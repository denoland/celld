// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The Queue cell's two host seams, shared by both engines.
//!
//! The JavaScript cell owns SQL and presentation. It sends row facts here so
//! alarm selection, concurrency admission, generation advancement, settlement
//! fencing, purge classification, retry precedence, and exhaustion have one
//! production implementation in `celld-logic` rather than a tested Rust copy
//! beside a different shipped JavaScript copy. The V8 arm reaches `run`
//! through a synchronous op and the workerd arm through a service binding
//! the arm answers itself.

use anyhow::anyhow;
use anyhow::Context;
use anyhow::Result;
use base64::Engine;

use crate::engine_api::QueueBatch;
use crate::engine_api::QueueContentType;
use crate::engine_api::QueueMessage;
use crate::engine_api::QueueMetrics;
use crate::generation::GenerationId;
use crate::host_channels::QueueDispatchReq;
use crate::host_channels::QueueLeaseRef;

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueDispatchEnvelope {
    lease_id: String,
    leases: Vec<QueueLeaseRef>,
    messages: Vec<QueueWireMessage>,
    metrics: QueueWireMetrics,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueWireMessage {
    id: String,
    timestamp_ms: i64,
    body_base64: String,
    content_type: String,
    attempts: u16,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueueWireMetrics {
    backlog_count: f64,
    backlog_bytes: f64,
    oldest_message_timestamp_ms: Option<i64>,
}

/// The broker's dispatch envelope as the host request that carries it: the
/// Queue cell has already installed each lease, and the batch travels to
/// the consumer script with those leases for its settlement.
pub(crate) fn dispatch_request(
    generation: GenerationId,
    scope: String,
    script: String,
    queue: String,
    envelope: &str,
) -> Result<QueueDispatchReq> {
    let envelope: QueueDispatchEnvelope =
        serde_json::from_str(envelope).context("invalid Queue dispatch envelope")?;
    let mut messages = Vec::with_capacity(envelope.messages.len());
    for message in envelope.messages {
        let content_type = match message.content_type.as_str() {
            "text" => QueueContentType::Text,
            "bytes" => QueueContentType::Bytes,
            "json" => QueueContentType::Json,
            "v8" => QueueContentType::V8,
            other => return Err(anyhow!("invalid Queue content type {other:?}")),
        };
        let body = base64::engine::general_purpose::STANDARD
            .decode(&message.body_base64)
            .context("invalid Queue message body")?;
        messages.push(QueueMessage {
            id: message.id,
            timestamp_ms: message.timestamp_ms,
            body,
            content_type,
            attempts: message.attempts,
        });
    }
    anyhow::ensure!(
        messages.len() == envelope.leases.len()
            && messages
                .iter()
                .zip(&envelope.leases)
                .all(|(message, lease)| message.id == lease.message_id),
        "a Queue dispatch must carry one matching lease per message"
    );
    Ok(QueueDispatchReq {
        generation,
        scope,
        script,
        lease_id: envelope.lease_id,
        leases: envelope.leases,
        batch: QueueBatch {
            queue,
            messages,
            metrics: QueueMetrics {
                backlog_count: envelope.metrics.backlog_count,
                backlog_bytes: envelope.metrics.backlog_bytes,
                oldest_message_timestamp_ms: envelope.metrics.oldest_message_timestamp_ms,
            },
        },
    })
}

/// One policy request from the cell, by its `op`, answered from
/// `celld_logic::queue`.
pub(crate) fn run(request: &serde_json::Value) -> Result<serde_json::Value> {
    let integer = |object: &serde_json::Value, name: &str| -> Result<i64> {
        object
            .get(name)
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| anyhow!("Queue policy input has no integer {name}"))
    };
    let optional_integer = |object: &serde_json::Value, name: &str| -> Result<Option<i64>> {
        match object.get(name) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(value) => value
                .as_i64()
                .map(Some)
                .ok_or_else(|| anyhow!("Queue policy input {name} is not an integer")),
        }
    };

    match request.get("op").and_then(serde_json::Value::as_str) {
        Some("rearm") => Ok(serde_json::json!(celld_logic::queue::rearm(
            integer(request, "now")?,
            optional_integer(request, "batchDeadline")?,
            optional_integer(request, "earliestVisible")?,
            optional_integer(request, "earliestLeaseExpiry")?,
            optional_integer(request, "nextSweep")?,
        ))),
        Some("capacity") => {
            let active = usize::try_from(integer(request, "active")?)
                .map_err(|_| anyhow!("Queue active concurrency is out of range"))?;
            let maximum = u16::try_from(integer(request, "maximum")?)
                .map_err(|_| anyhow!("Queue max concurrency is out of range"))?;
            Ok(serde_json::json!(celld_logic::queue::can_install_lease(
                active, maximum,
            )))
        }
        Some("retries") => {
            let now = integer(request, "now")?;
            let entries = request
                .get("entries")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| anyhow!("Queue retry policy has no entries"))?;
            let mut results = Vec::with_capacity(entries.len());
            for entry in entries {
                let seconds = |name: &str| -> Result<Option<u32>> {
                    optional_integer(entry, name)?
                        .map(|value| {
                            u32::try_from(value)
                                .map_err(|_| anyhow!("Queue retry {name} is out of range"))
                        })
                        .transpose()
                };
                let attempt = u16::try_from(integer(entry, "attempt")?)
                    .map_err(|_| anyhow!("Queue retry attempt is out of range"))?;
                let max_retries = u16::try_from(integer(entry, "maxRetries")?)
                    .map_err(|_| anyhow!("Queue maxRetries is out of range"))?;
                results.push(serde_json::json!({
                    "at": celld_logic::queue::retry_at(
                        now,
                        seconds("explicitSeconds")?,
                        seconds("configuredSeconds")?,
                    ),
                    "exhausted": celld_logic::queue::exhausted(attempt, max_retries),
                }));
            }
            Ok(serde_json::Value::Array(results))
        }
        Some("expiry") => {
            let now = integer(request, "now")?;
            let entries = request
                .get("entries")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| anyhow!("Queue expiry policy has no entries"))?;
            let mut results = Vec::with_capacity(entries.len());
            for entry in entries {
                let prior_failures = u16::try_from(integer(entry, "priorFailures")?)
                    .map_err(|_| anyhow!("Queue priorFailures is out of range"))?;
                let max_retries = u16::try_from(integer(entry, "maxRetries")?)
                    .map_err(|_| anyhow!("Queue maxRetries is out of range"))?;
                let configured = optional_integer(entry, "configuredSeconds")?
                    .map(|value| {
                        u32::try_from(value)
                            .map_err(|_| anyhow!("Queue retry delay is out of range"))
                    })
                    .transpose()?;
                let purge = entry
                    .get("purgeOnSettle")
                    .and_then(serde_json::Value::as_bool)
                    .ok_or_else(|| anyhow!("Queue expiry has no purgeOnSettle"))?;
                let expired = celld_logic::queue::expire_lease(
                    now,
                    prior_failures,
                    max_retries,
                    configured,
                    purge,
                );
                let action = match expired.action {
                    celld_logic::queue::ExpiredLeaseAction::RetryAt(at) => {
                        serde_json::json!({ "kind": "retry", "at": at })
                    }
                    celld_logic::queue::ExpiredLeaseAction::Exhausted => {
                        serde_json::json!({ "kind": "exhausted" })
                    }
                    celld_logic::queue::ExpiredLeaseAction::DeletePurged => {
                        serde_json::json!({ "kind": "delete-purged" })
                    }
                };
                results.push(serde_json::json!({
                    "attempt": expired.attempt,
                    "action": action,
                }));
            }
            Ok(serde_json::Value::Array(results))
        }
        Some("batch") => {
            let now = integer(request, "now")?;
            let max_batch_size = usize::try_from(integer(request, "maxBatchSize")?)
                .map_err(|_| anyhow!("Queue maxBatchSize is out of range"))?;
            let rows = request
                .get("rows")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| anyhow!("Queue batch policy has no rows"))?;
            let rows = rows
                .iter()
                .map(|row| {
                    let generation = row
                        .get("leaseGeneration")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| anyhow!("Queue row has no leaseGeneration"))?
                        .parse::<u64>()
                        .context("Queue leaseGeneration is invalid")?;
                    Ok(celld_logic::queue::BatchRow {
                        seq: integer(row, "seq")?,
                        visible_at: integer(row, "visibleAt")?,
                        lease_generation: generation,
                        leased_until: optional_integer(row, "leasedUntil")?,
                        purge_on_settle: row
                            .get("purgeOnSettle")
                            .and_then(serde_json::Value::as_bool)
                            .ok_or_else(|| anyhow!("Queue row has no purgeOnSettle"))?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let plan = celld_logic::queue::batch_plan(now, &rows, max_batch_size)?;
            Ok(serde_json::json!({
                "leases": plan.leases.into_iter().map(|lease| serde_json::json!({
                    "seq": lease.seq,
                    "generation": lease.generation.to_string(),
                    "reclaimed": lease.reclaimed,
                })).collect::<Vec<_>>(),
                "deletePurged": plan.delete_purged,
            }))
        }
        Some("settlement") => {
            let members = |name: &str| -> Result<Vec<celld_logic::queue::LeaseMember<'_>>> {
                request
                    .get(name)
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| anyhow!("Queue settlement policy has no {name}"))?
                    .iter()
                    .map(|member| {
                        let string = |field: &str| -> Result<&str> {
                            member
                                .get(field)
                                .and_then(serde_json::Value::as_str)
                                .ok_or_else(|| anyhow!("Queue settlement member has no {field}"))
                        };
                        Ok(celld_logic::queue::LeaseMember {
                            seq: string("seq")?
                                .parse::<i64>()
                                .context("Queue settlement sequence is invalid")?,
                            message_id: string("messageId")?,
                            generation: string("generation")?
                                .parse::<u64>()
                                .context("Queue settlement generation is invalid")?,
                        })
                    })
                    .collect()
            };
            let current = members("current")?;
            let submitted = members("submitted")?;
            Ok(serde_json::json!(celld_logic::queue::settlement_matches(
                &current, &submitted,
            )))
        }
        Some("purge") => {
            let now = integer(request, "now")?;
            let rows = request
                .get("rows")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| anyhow!("Queue purge policy has no rows"))?
                .iter()
                .map(|row| {
                    Ok(celld_logic::queue::PurgeRow {
                        seq: row
                            .get("seq")
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| anyhow!("Queue purge row has no sequence"))?
                            .parse::<i64>()
                            .context("Queue purge sequence is invalid")?,
                        lease_id_present: row
                            .get("leaseIdPresent")
                            .and_then(serde_json::Value::as_bool)
                            .ok_or_else(|| anyhow!("Queue purge row has no lease state"))?,
                        leased_until: optional_integer(row, "leasedUntil")?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let plan = celld_logic::queue::purge_plan(now, &rows);
            Ok(serde_json::json!({
                "delete": plan.delete.into_iter().map(|seq| seq.to_string()).collect::<Vec<_>>(),
                "markForSettle": plan.mark_for_settle.into_iter().map(|seq| seq.to_string()).collect::<Vec<_>>(),
            }))
        }
        Some(other) => Err(anyhow!("unknown Queue policy operation {other:?}")),
        None => Err(anyhow!("Queue policy input has no op")),
    }
}

#[cfg(celld_internal_tests)]
thread_local! {
    static QUEUE_LEASE_DURATION_FOR_TEST: std::cell::Cell<Option<i64>> = const {
        std::cell::Cell::new(None)
    };
    static QUEUE_BATCH_TIMEOUT_FOR_TEST: std::cell::Cell<Option<i64>> = const {
        std::cell::Cell::new(None)
    };
    static QUEUE_RETENTION_FOR_TEST: std::cell::Cell<Option<i64>> = const {
        std::cell::Cell::new(None)
    };
    static QUEUE_SWEEP_BATCH_FOR_TEST: std::cell::Cell<Option<usize>> = const {
        std::cell::Cell::new(None)
    };
}

/// Measure lease expiry without waiting through the production handler budget.
/// The override is thread-local because runtime tests build
/// unrelated Workers in parallel, and a process environment variable would
/// silently shorten their leases too.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn set_queue_lease_duration_for_test(duration: Option<i64>) {
    QUEUE_LEASE_DURATION_FOR_TEST.set(duration);
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn set_queue_batch_timeout_for_test(duration: Option<i64>) {
    QUEUE_BATCH_TIMEOUT_FOR_TEST.set(duration);
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn set_queue_retention_for_test(duration: Option<i64>) {
    QUEUE_RETENTION_FOR_TEST.set(duration);
}

#[cfg(celld_internal_tests)]
pub fn set_queue_sweep_batch_for_test(rows: Option<usize>) {
    QUEUE_SWEEP_BATCH_FOR_TEST.set(rows);
}

#[cfg(celld_internal_tests)]
fn effective_queue_lease_duration(duration: i64) -> i64 {
    QUEUE_LEASE_DURATION_FOR_TEST.get().unwrap_or(duration)
}

#[cfg(not(celld_internal_tests))]
fn effective_queue_lease_duration(duration: i64) -> i64 {
    duration
}

#[cfg(celld_internal_tests)]
fn effective_queue_batch_timeout(duration: i64) -> i64 {
    if duration == 0 {
        0
    } else {
        QUEUE_BATCH_TIMEOUT_FOR_TEST.get().unwrap_or(duration)
    }
}

#[cfg(celld_internal_tests)]
fn effective_queue_retention(duration: i64) -> i64 {
    QUEUE_RETENTION_FOR_TEST.get().unwrap_or(duration)
}

#[cfg(not(celld_internal_tests))]
fn effective_queue_retention(duration: i64) -> i64 {
    duration
}

#[cfg(celld_internal_tests)]
fn effective_queue_sweep_batch(rows: usize) -> usize {
    QUEUE_SWEEP_BATCH_FOR_TEST.get().unwrap_or(rows)
}

#[cfg(not(celld_internal_tests))]
fn effective_queue_sweep_batch(rows: usize) -> usize {
    rows
}

#[cfg(not(celld_internal_tests))]
fn effective_queue_batch_timeout(duration: i64) -> i64 {
    duration
}

/// The Queue cell's deployment-wide settings, as the cell reads them: the
/// consumer catalog by queue, the lease duration, and the limits.
pub(crate) fn consumer_config(config: &crate::js::WorkerConfig) -> Result<serde_json::Value> {
    let consumers: serde_json::Map<String, serde_json::Value> = config
        .queue_consumers
        .iter()
        .map(|registration| {
            let consumer = &registration.config;
            let mut value = serde_json::json!({
                "script": registration.script,
                "maxBatchSize": consumer.max_batch_size,
                "maxBatchTimeoutMs":
                    effective_queue_batch_timeout(i64::from(consumer.max_batch_timeout) * 1000),
                "maxRetries": consumer.max_retries,
            });
            if let Some(queue) = &consumer.dead_letter_queue {
                value["deadLetterQueue"] = serde_json::json!(queue);
            }
            if let Some(limit) = consumer.max_concurrency {
                value["maxConcurrency"] = serde_json::json!(limit);
            }
            if let Some(delay) = consumer.retry_delay {
                value["retryDelaySeconds"] = serde_json::json!(delay);
            }
            (consumer.queue.clone(), value)
        })
        .collect();
    let ms = |duration: std::time::Duration| duration.as_millis().min(i64::MAX as u128) as i64;
    let admission = ms(crate::engine_api::admission_wait());
    let handler = ms(crate::engine_api::handler_budget());
    let settlement = i64::try_from(crate::actor::operation_deadline_ms()?).unwrap_or(i64::MAX);
    let lease = effective_queue_lease_duration(celld_logic::queue::lease_duration_ms(
        admission, handler, settlement,
    ));
    Ok(serde_json::json!({
        "consumers": consumers,
        "leaseDurationMs": lease,
        "limits": {
            "maxMessageBytes": celld_logic::queue::MAX_MESSAGE_BYTES,
            "maxBatchBytes": celld_logic::queue::MAX_SEND_BATCH_BYTES,
            "maxBatchMessages": celld_logic::queue::MAX_BATCH_MESSAGES,
            "producerGroupMs": crate::queue_batching::timing().producer_ms,
            "maxConcurrency": celld_logic::queue::MAX_CONCURRENCY,
            "maxDelaySeconds": celld_logic::queue::MAX_DELAY_SECONDS,
            "retentionMs": effective_queue_retention(celld_logic::queue::RETENTION_MS),
            "sweepBatchRows": effective_queue_sweep_batch(celld_logic::sweep::BATCH_ROWS),
        },
    }))
}
