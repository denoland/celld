// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Compatibility garbage collection for dead celld process generations.
//!
//! Wake entries and lazy ownership takeover provide serving correctness.
//! This adapter retires the historical `node-cells/` index debris and expired
//! node-session records left in fleet buckets shared with celld.

use crate::bucket::Bucket;
use anyhow::Context as _;
use futures_util::stream::{self, StreamExt as _};
use serde::Deserialize;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tracing::{info, warn};

const MARKER_GC_CONCURRENCY: usize = 64;

#[derive(Deserialize, Serialize)]
struct NodeWire {
    node: String,
    expires_ms: u64,
    #[serde(default, rename = "ownership_index_generation")]
    generation: String,
    #[serde(default)]
    probe_public_key: String,
    /// The folded node log: this record IS
    /// the fleet log's root of truth, so retirement must respect its
    /// state and the tombstone must carry it through unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    log: Option<serde_json::Value>,
    /// Every other lease field, preserved verbatim: a crash between the
    /// tombstone and the delete must never publish a record poorer than
    /// the one it fences.
    #[serde(flatten)]
    rest: serde_json::Map<String, serde_json::Value>,
}

impl NodeWire {
    /// The process generation, which a record written after part 2 of
    /// the `ownership_index_generation` retirement (see
    /// `ownership_store::NodeLeaseWire`) carries only as the probe key.
    fn generation(&self) -> &str {
        if self.generation.is_empty() {
            &self.probe_public_key
        } else {
            &self.generation
        }
    }
}

#[derive(Clone, Debug)]
struct DeadNode {
    node: String,
    generation: String,
}

#[derive(Default)]
struct IndexedOwnership {
    markers: Vec<String>,
}

#[derive(Default)]
struct MarkerGcSummary {
    markers: usize,
    retired: usize,
    failures: usize,
}

/// Process-local retry state for compatibility GC.
///
/// Durable marker deletions survive cancellation. `swept` avoids repeating a
/// fleet-wide marker scan when only conditional retirement of the node record
/// remains, while `retries` bounds a persistently failing marker store.
#[derive(Default)]
pub struct DeadNodeGc {
    swept: BTreeSet<String>,
    retries: BTreeMap<String, (String, u32, u64)>,
}

impl DeadNodeGc {
    /// Run one pass while renewing the advisory fleet-waker role. No task is
    /// spawned: the caller's existing wake-loop future polls both work and
    /// renewal, and dropping a lost-role pass cancels remaining I/O.
    ///
    /// Returns the node sessions whose leases were live when the pass began,
    /// and only when this node held the role for the whole pass. The caller
    /// uses that as its election result: the fleet-wide wake probe that
    /// follows runs on the same tick, against the same leases, and only on
    /// the node that holds the role. `None` means either that another node
    /// holds the role or that this node lost it mid-pass; both mean "do not
    /// probe this tick".
    pub async fn run_elected_pass(
        &mut self,
        bucket: &Bucket,
        node: &str,
        tick_ms: u64,
    ) -> Option<BTreeSet<String>> {
        let lease_ttl_ms = tick_ms.saturating_mul(3).min(i64::MAX as u64);
        if !crate::wake::try_hold_waker(
            bucket,
            node,
            crate::ownership_store::now_ms() as i64,
            lease_ttl_ms as i64,
        )
        .await
        {
            return None;
        }

        let renew_ms = (lease_ttl_ms / 3).max(1);
        let mut renewal = crate::asyncrt::interval(Duration::from_millis(renew_ms));
        renewal.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
        renewal.tick().await;
        let pass = self.run_pass(bucket, tick_ms);
        tokio::pin!(pass);
        loop {
            crate::asyncrt::select_biased! {
                "a due waker-lease renewal wins a tie with completion of the GC pass";
                _ = renewal.tick() => {
                    if !crate::wake::try_hold_waker(
                        bucket,
                        node,
                        crate::ownership_store::now_ms() as i64,
                        lease_ttl_ms as i64,
                    ).await {
                        warn!("waker lease renewal failed; cancelling dead-node GC");
                        return None;
                    }
                },
                live = &mut pass => return Some(live),
            }
        }
    }

    /// One GC pass. Returns the live node sessions the pass observed.
    async fn run_pass(&mut self, bucket: &Bucket, tick_ms: u64) -> BTreeSet<String> {
        let observed_ms = crate::ownership_store::now_ms();
        let NodeScan { dead, live } = scan_nodes(bucket, observed_ms).await;
        let dead_names: BTreeSet<&str> = dead.iter().map(|record| record.node.as_str()).collect();
        self.swept.retain(|node| dead_names.contains(node.as_str()));
        self.retries
            .retain(|node, _| dead_names.contains(node.as_str()));

        let retry_now = crate::asyncrt::mono_ms();
        let indexed: BTreeMap<String, String> = dead
            .iter()
            .filter(|record| {
                !self.swept.contains(&record.node)
                    && self
                        .retries
                        .get(&record.node)
                        .is_none_or(|(generation, _, retry_at)| {
                            generation != &record.generation || *retry_at <= retry_now
                        })
                    && !record.generation.is_empty()
            })
            .map(|record| (record.node.clone(), record.generation.clone()))
            .collect();

        let indexed = if indexed.is_empty() {
            Some(BTreeMap::new())
        } else {
            match cells_indexed_by_nodes(bucket, &indexed).await {
                Ok(indexed) => Some(indexed),
                Err(error) => {
                    warn!(%error, nodes = indexed.len(), "dead-node marker scan failed");
                    None
                }
            }
        };
        let mut summaries = match indexed {
            Some(indexed) => gc_markers(bucket, indexed).await,
            None => BTreeMap::new(),
        };

        for record in dead {
            let node = record.node;
            let generation = record.generation;
            if !self.swept.contains(&node) {
                let (markers, retired, failures, complete) = if generation.is_empty() {
                    (0, 0, 0, true)
                } else {
                    let Some(summary) = summaries.remove(&node) else {
                        continue;
                    };
                    (
                        summary.markers,
                        summary.retired,
                        summary.failures,
                        summary.failures == 0,
                    )
                };
                info!(
                    event = "dead_node_reconciliation",
                    %node,
                    markers,
                    retired,
                    failures,
                    complete,
                    "dead-node marker GC complete"
                );
                if !complete {
                    let failure_count = self
                        .retries
                        .get(&node)
                        .filter(|(retry_generation, _, _)| retry_generation == &generation)
                        .map_or(1, |(_, count, _)| count.saturating_add(1));
                    let retry_ms = celld_logic::dead_node_reconciliation::retry_delay_ms(
                        tick_ms,
                        failure_count,
                    );
                    self.retries.insert(
                        node,
                        (
                            generation,
                            failure_count,
                            crate::asyncrt::mono_ms().saturating_add(retry_ms),
                        ),
                    );
                    continue;
                }
                self.retries.remove(&node);
                self.swept.insert(node.clone());
            }
            match retire_dead_node(bucket, &node, observed_ms).await {
                Ok(true) => {
                    self.swept.remove(&node);
                }
                Ok(false) => {}
                Err(error) => warn!(%node, %error, "dead-node lease retirement failed"),
            }
        }
        live
    }
}

/// The node sessions with a live lease, for a caller that has no GC pass to
/// take them from (the boot-time wake scan).
pub async fn live_nodes(bucket: &Bucket, now_ms: u64) -> BTreeSet<String> {
    scan_nodes(bucket, now_ms).await.live
}

/// The fleet's node records, split by lease state at one instant.
struct NodeScan {
    dead: Vec<DeadNode>,
    /// Sessions whose record matches its key and whose lease has not
    /// expired. A record that fails to read is in neither set, so an owner
    /// named by such a record counts as not live and the wake probe hints
    /// its cells; the core's own lease read then decides.
    live: BTreeSet<String>,
}

async fn scan_nodes(bucket: &Bucket, now_ms: u64) -> NodeScan {
    let mut dead = Vec::new();
    let mut live = BTreeSet::new();
    let objects = match bucket.list("nodes/").await {
        Ok(objects) => objects,
        Err(error) => {
            warn!(%error, "dead-node scan list failed");
            return NodeScan { dead, live };
        }
    };
    for object in objects {
        let key = object.location.as_ref();
        let Some(node) = key
            .strip_prefix("nodes/")
            .and_then(|value| value.strip_suffix(".json"))
        else {
            continue;
        };
        match read_node(bucket, key).await {
            Ok(Some((record, _)))
                if celld_logic::dead_node_reconciliation::node_record_is_dead(
                    node,
                    &record.node,
                    record.expires_ms,
                    now_ms,
                ) =>
            {
                dead.push(DeadNode {
                    node: node.to_string(),
                    generation: record.generation().to_string(),
                });
            }
            Ok(Some((record, _))) if record.node == node => {
                live.insert(node.to_string());
            }
            Ok(_) => {}
            Err(error) => warn!(%node, %error, "dead-node scan read failed"),
        }
    }
    NodeScan { dead, live }
}

async fn cells_indexed_by_nodes(
    bucket: &Bucket,
    nodes: &BTreeMap<String, String>,
) -> anyhow::Result<BTreeMap<String, IndexedOwnership>> {
    let mut indexed: BTreeMap<String, IndexedOwnership> = nodes
        .keys()
        .cloned()
        .map(|node| (node, IndexedOwnership::default()))
        .collect();
    for object in bucket.list("node-cells/").await? {
        let key = object.location.as_ref();
        let Some((node, _)) = celld_logic::dead_node_reconciliation::parse_marker_key(key) else {
            continue;
        };
        if let Some(entry) = indexed.get_mut(node) {
            entry.markers.push(key.to_string());
        }
    }
    Ok(indexed)
}

async fn gc_markers(
    bucket: &Bucket,
    indexed: BTreeMap<String, IndexedOwnership>,
) -> BTreeMap<String, MarkerGcSummary> {
    let mut summaries = BTreeMap::new();
    let mut work = Vec::new();
    for (node, indexed) in indexed {
        summaries
            .entry(node.clone())
            .or_insert_with(|| MarkerGcSummary {
                markers: indexed.markers.len(),
                ..MarkerGcSummary::default()
            });
        for marker in indexed.markers {
            work.push((node.clone(), marker));
        }
    }
    let mut results = stream::iter(work)
        .map(|(node, marker)| async move {
            let result = bucket.delete(&marker).await;
            (node, result)
        })
        .buffer_unordered(MARKER_GC_CONCURRENCY);
    while let Some((node, result)) = results.next().await {
        let summary = summaries.get_mut(&node).expect("work has a summary");
        match result {
            Ok(_) => summary.retired += 1,
            Err(error) => {
                if summary.failures == 0 {
                    warn!(%node, %error, "dead-node marker delete failed");
                }
                summary.failures += 1;
            }
        }
    }
    summaries
}

async fn retire_dead_node(bucket: &Bucket, node: &str, now_ms: u64) -> anyhow::Result<bool> {
    let key = format!("nodes/{node}.json");
    let Some((record, etag)) = read_node(bucket, &key).await? else {
        return Ok(true);
    };
    if !celld_logic::dead_node_reconciliation::node_record_is_dead(
        node,
        &record.node,
        record.expires_ms,
        now_ms,
    ) {
        return Ok(false);
    }
    // A folded record is never deleted. Two reasons compose. An unsealed log
    // is the fleet's only pointer to an unrecovered tail, so the record must outlive
    // recovery — the dead-leader sweep seals it, and a later pass
    // retires it here. And object_store has no conditional delete, so
    // an unconditional delete can land arbitrarily late — after the
    // node's successor generation reinstalled the key and activated its
    // log — and erase an acked tail. So a dead folded record's terminal
    // state is the tombstone: still dead, still sealed, expiry zero.
    // Node keys are stable across restarts, so retained tombstones
    // number the decommissioned nodes, not the restarts, and the
    // dead-leader sweep can always find a sealed session to GC its
    // bundles. `Ok(false)` keeps the marker latch held: the record
    // remains listed, and only true deletion clears the latch.
    if let Some(log) = record.log.as_ref() {
        if log.get("state").and_then(|state| state.as_str()) != Some("sealed") {
            return Ok(false);
        }
        if record.expires_ms == 0 {
            return Ok(false);
        }
        let tombstone = serde_json::to_vec(&NodeWire {
            expires_ms: 0,
            ..record
        })?;
        bucket.put_cas(&key, tombstone, Some(&etag)).await?;
        return Ok(false);
    }
    // No conditional delete in object_store: fence with a CAS tombstone
    // (`expires_ms: 0`, still a dead record), then delete. A crash between
    // the two leaves a record that still reads as dead and retires on the
    // next pass.
    let tombstone = serde_json::to_vec(&NodeWire {
        expires_ms: 0,
        ..record
    })?;
    match bucket.put_cas(&key, tombstone, Some(&etag)).await? {
        Some(_) => {
            bucket.delete(&key).await?;
            Ok(true)
        }
        None => Ok(false),
    }
}

async fn read_node(bucket: &Bucket, key: &str) -> anyhow::Result<Option<(NodeWire, String)>> {
    let Some((bytes, etag)) = bucket.get(key).await? else {
        return Ok(None);
    };
    let record = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode {}://{}/{key}", bucket.scheme(), bucket.name))?;
    Ok(Some((record, etag)))
}
