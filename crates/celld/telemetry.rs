// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Telemetry is observational and does not affect Actor decisions.
#![allow(clippy::disallowed_methods)]

//! Telemetry: wide events out of the shell, Parquet into the bucket.
//!
//! This is v1's substrate. Off by default, and off is structural: `init`
//! never runs, `start_trace` finds no globals and answers `None`, and no
//! event is ever built.
//! Sampling is decided at trace creation for the same reason — an
//! unsampled request builds nothing.
//!
//! The hot path never blocks. `record` is a `try_send`; a full channel
//! increments a drop counter. Under saturation celld sheds telemetry,
//! counted, never requests. The pipeline is ordinary tasks on the shared
//! runtime — no dedicated thread (the Deno pattern needs one because its
//! main loop is a current-thread runtime; celld's is not) — and encoding
//! runs on `spawn_blocking`, the `prune_local_cache` treatment.

use crate::bucket::Bucket;
use anyhow::anyhow;
use anyhow::bail;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Where telemetry objects live, under the sink bucket.
pub const TRACES_PREFIX: &str = "telemetry/traces";
pub const LOGS_PREFIX: &str = "telemetry/logs";

/// A `console.log` body larger than this is truncated: log records are
/// telemetry, not blob storage, and one runaway line must not dominate a
/// batch.
#[doc(hidden)]
pub const LOG_BODY_CAP: usize = 8 * 1024;

/// The schema is not frozen. Stamped on every object so a reader knows
/// what contract the columns follow; the DuckDB page documents it.
const SCHEMA_VERSION: &str = "v0-unstable";

/// Queue between the hot path and the pump; a full queue sheds, counted.
/// The pump drains continuously and the size trigger bounds its batch, so
/// this only needs to cover a scheduling hiccup, not a flush interval.
const CHANNEL_CAPACITY: usize = 8_192;

/// One batch remains exporter-owned while it retries. The channel stays
/// bounded behind it, so an outage can consume this batch and at most
/// `CHANNEL_CAPACITY` newer events. Five attempts cover a short collector
/// restart without turning telemetry into an unbounded delivery queue.
const OTLP_MAX_ATTEMPTS: u32 = 5;
const OTLP_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const OTLP_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// The provider page and the S3 bulk-delete limit are both 1,000. Keeping
/// them equal lets a sweep delete one page without retaining a second key
/// collection or splitting one provider page across storage requests.
const SWEEP_PAGE_SIZE: usize = 1_000;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Retention {
    Days(u32),
    /// "Never delete, my lifecycle rules own this."
    None,
}

impl Retention {
    fn label(self) -> String {
        match self {
            Retention::Days(days) => format!("{days}d"),
            Retention::None => "none".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SinkChoice {
    /// Parquet into the operator's bucket: zero configuration beyond the flag.
    /// DuckDB reads the result as described in `docs/telemetry.md`.
    Bucket,
    /// OTLP/http-protobuf to a collector — the ClickHouse-class path.
    Otlp,
}

#[derive(Debug)]
pub struct Config {
    pub sink: SinkChoice,
    /// `CELLD_OTEL_BUCKET`, same endpoint/region/credentials as the
    /// fleet bucket. `None` means the fleet bucket itself.
    pub bucket_override: Option<String>,
    /// Base OTLP endpoint; `/v1/traces` and `/v1/logs` are appended,
    /// per the spec's default-port convention.
    pub otlp_endpoint: String,
    pub otlp_headers: Vec<(String, String)>,
    pub otlp_timeout: Duration,
    pub service: String,
    pub sample_ratio: f64,
    /// `parentbased_*`: a caller's sampled flag decides for its children;
    /// the ratio applies to roots only.
    pub parent_based: bool,
    pub retention: Retention,
    pub flush: Duration,
    /// Estimated buffered bytes that force a flush before `flush` elapses —
    /// Firehose semantics: time or size, whichever first. Keeps a busy
    /// node's Parquet objects near this size instead of thousands of
    /// slivers per hour.
    pub flush_bytes: usize,
    /// How often the retention sweep runs.
    pub sweep: Duration,
}

impl Config {
    /// `None` when `CELLD_OTEL` is unset or `0` — the default, and the
    /// only zero-cost state.
    pub fn from_env() -> anyhow::Result<Option<Config>> {
        let enabled = crate::env_vars::flag("CELLD_OTEL", false)?;
        Self::from_lookup(enabled, crate::env_vars::value)
    }

    #[doc(hidden)]
    pub fn from_lookup(
        enabled: bool,
        get: impl Fn(&str) -> anyhow::Result<Option<String>>,
    ) -> anyhow::Result<Option<Config>> {
        let sink = match get("CELLD_OTEL_SINK")?.as_deref() {
            None | Some("bucket") => SinkChoice::Bucket,
            Some("otlp") => SinkChoice::Otlp,
            Some(other) => bail!("unknown CELLD_OTEL_SINK {other:?}"),
        };
        let otlp_endpoint = get("OTEL_EXPORTER_OTLP_ENDPOINT")?
            .unwrap_or_else(|| "http://localhost:4318".to_string())
            .trim_end_matches('/')
            .to_string();
        let mut otlp_headers = Vec::new();
        if let Some(list) = get("OTEL_EXPORTER_OTLP_HEADERS")? {
            for pair in list.split(',').filter(|pair| !pair.trim().is_empty()) {
                let (name, value) = pair
                    .split_once('=')
                    .ok_or_else(|| anyhow!("OTEL_EXPORTER_OTLP_HEADERS: {pair:?}"))?;
                otlp_headers.push((name.trim().to_string(), value.trim().to_string()));
            }
        }
        let otlp_timeout = crate::env_vars::parse_positive::<u64>(
            "OTEL_EXPORTER_OTLP_TIMEOUT",
            get("OTEL_EXPORTER_OTLP_TIMEOUT")?,
        )?
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(10));
        let service = get("OTEL_SERVICE_NAME")?.unwrap_or_else(|| "celld".to_string());
        // Per the otel spec: a `parentbased_*` sampler defers to the
        // caller's sampled flag and decides itself only for roots. That
        // is an explicit statement of upstream trust — appropriate here,
        // because celld's ingress sits behind the operator's own edge.
        let sampler = get("OTEL_TRACES_SAMPLER")?;
        let sampler = sampler.as_deref().unwrap_or("parentbased_always_on");
        let parent_based = sampler.starts_with("parentbased_");
        let sample_ratio = match sampler {
            "parentbased_always_on" | "always_on" => 1.0,
            "parentbased_always_off" | "always_off" => 0.0,
            "traceidratio" | "parentbased_traceidratio" => {
                let arg = get("OTEL_TRACES_SAMPLER_ARG")?
                    .ok_or_else(|| anyhow!("OTEL_TRACES_SAMPLER_ARG is required"))?;
                let ratio: f64 = arg
                    .parse()
                    .map_err(|_| anyhow!("OTEL_TRACES_SAMPLER_ARG: {arg:?}"))?;
                if !(0.0..=1.0).contains(&ratio) {
                    bail!("OTEL_TRACES_SAMPLER_ARG must be in [0, 1]: {arg}");
                }
                ratio
            }
            other => bail!("unsupported OTEL_TRACES_SAMPLER {other:?}"),
        };
        let retention = match get("CELLD_OTEL_RETENTION")?.as_deref() {
            None => Retention::Days(30),
            Some("none") => Retention::None,
            Some(value) => {
                let days = value
                    .strip_suffix('d')
                    .and_then(|days| days.parse::<u32>().ok())
                    .filter(|days| *days > 0)
                    .ok_or_else(|| {
                        anyhow!("CELLD_OTEL_RETENTION must be <n>d or none: {value:?}")
                    })?;
                Retention::Days(days)
            }
        };
        // The production cadence is policy, not operator configuration. A
        // short override supports controlled retention validation.
        let sweep = crate::env_vars::parse_positive::<u64>(
            "CELLD_TEST_OTEL_SWEEP_MS",
            get("CELLD_TEST_OTEL_SWEEP_MS")?,
        )?
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(6 * 3600));
        let flush = crate::env_vars::parse_positive::<u64>(
            "CELLD_OTEL_FLUSH_MS",
            get("CELLD_OTEL_FLUSH_MS")?,
        )?
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(300));
        let flush_bytes = crate::env_vars::parse_positive::<usize>(
            "CELLD_OTEL_FLUSH_BYTES",
            get("CELLD_OTEL_FLUSH_BYTES")?,
        )?
        .unwrap_or(5 * 1024 * 1024);
        Ok(enabled.then_some(Config {
            sink,
            bucket_override: get("CELLD_OTEL_BUCKET")?,
            otlp_endpoint,
            otlp_headers,
            otlp_timeout,
            service,
            sample_ratio,
            parent_based,
            retention,
            flush,
            flush_bytes,
            sweep,
        }))
    }
}

/// Span kinds, numbered as the OTLP proto numbers them so the later OTLP
/// sink is a transcription, not a mapping.
pub const KIND_INTERNAL: u8 = 1;
pub const KIND_SERVER: u8 = 2;
pub const KIND_CLIENT: u8 = 3;
pub const KIND_CONSUMER: u8 = 4;

/// A recorded span's identity.
#[derive(Clone, Copy, Debug)]
pub struct TraceIds {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
}

/// The W3C context that crosses request boundaries. This keeps the recording
/// decision with the ids, so an unsampled context can propagate without any
/// caller mistaking its existence for permission to record an event.
#[derive(Clone, Copy, Debug)]
pub struct TraceContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub sampled: bool,
}

impl TraceContext {
    pub fn recording_ids(self) -> Option<TraceIds> {
        self.sampled.then_some(TraceIds {
            trace_id: self.trace_id,
            span_id: self.span_id,
        })
    }
}

/// A caller's trace context, extracted from a W3C `traceparent` header.
/// celld does not terminate TLS; whoever can reach its ingress is the
/// operator's own edge, so honoring this trusts the same infrastructure.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ParentContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub sampled: bool,
}

/// Parse a W3C `traceparent` value. Anything malformed is `None`, and the
/// request becomes an ordinary root, exactly as if the header were absent.
pub fn parse_traceparent(value: &str) -> Option<ParentContext> {
    let value = value.trim().as_bytes();
    if value.len() < 55 || value[2] != b'-' || value[35] != b'-' || value[52] != b'-' {
        return None;
    }

    let version = unhex::<1>(&value[..2])?[0];
    if version == 0xff
        || (version == 0 && value.len() != 55)
        || (version != 0 && value.len() > 55 && value[55] != b'-')
    {
        return None;
    }

    let trace_id = unhex(&value[3..35])?;
    let span_id = unhex(&value[36..52])?;
    let flags = unhex::<1>(&value[53..55])?[0];
    if trace_id == [0; 16] || span_id == [0; 8] {
        return None;
    }
    Some(ParentContext {
        trace_id,
        span_id,
        sampled: flags & 1 == 1,
    })
}

/// The header for an outbound request made inside a propagated context.
pub fn traceparent(context: &TraceContext) -> String {
    let flags = if context.sampled { "01" } else { "00" };
    format!(
        "00-{}-{}-{flags}",
        hex(&context.trace_id),
        hex(&context.span_id)
    )
}

fn unhex<const N: usize>(text: &[u8]) -> Option<[u8; N]> {
    if text.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (index, byte) in out.iter_mut().enumerate() {
        let high = hex_nibble(text[index * 2])?;
        let low = hex_nibble(text[index * 2 + 1])?;
        *byte = high << 4 | low;
    }
    Some(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// One wide event. Flat on purpose: every field is a Parquet column, and the
/// column names are the schema contract.
pub struct Span {
    pub ids: TraceIds,
    pub parent_span_id: Option<[u8; 8]>,
    pub name: &'static str,
    pub kind: u8,
    pub start_unix_us: i64,
    pub duration_us: i64,
    pub ok: bool,
    pub error: Option<String>,
    pub request_id: Option<String>,
    pub cell: Option<String>,
    pub epoch: Option<u64>,
    pub isolate: Option<u64>,
    pub queue_wait_us: Option<i64>,
    pub url: Option<String>,
    pub http_status: Option<u16>,
    /// `Some(true)` for a parent extracted from a caller's traceparent,
    /// `Some(false)` for an in-process parent, `None` for a root.
    pub parent_remote: Option<bool>,
}

/// One captured console line, correlated to the trace whose turn (or
/// whose promise continuation — that is what CPED buys) produced it.
pub struct Log {
    pub trace_id: Option<[u8; 16]>,
    pub span_id: Option<[u8; 8]>,
    pub time_unix_us: i64,
    pub body: String,
}

impl Span {
    pub fn new(ids: TraceIds, name: &'static str, kind: u8) -> Span {
        Span {
            ids,
            parent_span_id: None,
            name,
            kind,
            start_unix_us: 0,
            duration_us: 0,
            ok: true,
            error: None,
            request_id: None,
            cell: None,
            epoch: None,
            isolate: None,
            queue_wait_us: None,
            url: None,
            http_status: None,
            parent_remote: None,
        }
    }
}

enum Event {
    Span(Span),
    Log(Log),
}

impl Event {
    /// A cheap pre-encode size estimate for the flush trigger: fixed
    /// columns plus the owned strings. Compression shrinks the object
    /// afterwards, the same way Firehose's buffer counts raw bytes.
    fn approx_bytes(&self) -> usize {
        const LEN: fn(&Option<String>) -> usize = |s| s.as_deref().map_or(0, str::len);
        match self {
            Event::Span(span) => {
                96 + span.name.len()
                    + LEN(&span.error)
                    + LEN(&span.request_id)
                    + LEN(&span.cell)
                    + LEN(&span.url)
            }
            Event::Log(log) => 40 + log.body.len(),
        }
    }
}

struct Telemetry {
    tx: tokio::sync::mpsc::Sender<Event>,
    dropped: AtomicU64,
    sampler: Sampler,
    parent_based: bool,
}

#[derive(Clone, Copy)]
enum Sampler {
    AlwaysOff,
    /// The number of `u64` decision values that record, starting at zero.
    TraceIdRatio(u64),
    AlwaysOn,
}

impl Sampler {
    fn from_ratio(ratio: f64) -> Sampler {
        if ratio == 0.0 {
            return Sampler::AlwaysOff;
        }
        if ratio == 1.0 {
            return Sampler::AlwaysOn;
        }
        // A u64 has 2^64 possible values, which cannot fit in a u64. Keep
        // the full-range endpoint as a distinct policy and store a count for
        // every intermediate ratio. An inclusive maximum would add one
        // sampled value and would make the zero endpoint nonempty again.
        Sampler::TraceIdRatio((ratio * (u64::MAX as f64 + 1.0)) as u64)
    }

    fn samples(self, decision: u64) -> bool {
        match self {
            Sampler::AlwaysOff => false,
            Sampler::TraceIdRatio(upper_bound) => decision < upper_bound,
            Sampler::AlwaysOn => true,
        }
    }
}

static TELEMETRY: OnceLock<Telemetry> = OnceLock::new();

pub fn active() -> bool {
    TELEMETRY.get().is_some()
}

/// Decide a new root trace at its creation. `None` means off or unsampled,
/// and the caller builds nothing at all.
pub fn start_trace() -> Option<TraceContext> {
    start_trace_with_parent(None)
}

/// Decide a trace at its creation, under a foreign parent when the
/// caller sent one. A rejected foreign parent remains present for W3C
/// propagation, but a rejected root remains absent to preserve the cheap path.
pub fn start_trace_with_parent(parent: Option<&ParentContext>) -> Option<TraceContext> {
    let telemetry = TELEMETRY.get()?;
    let (trace_id, sampled) = decide_with_sampler(
        telemetry.parent_based,
        telemetry.sampler,
        parent,
        rand::random(),
    );
    if !sampled && parent.is_none() {
        return None;
    }
    Some(TraceContext {
        trace_id,
        span_id: rand::random(),
        sampled,
    })
}

/// The sampling decision, pure so the semantics are testable: adopt the
/// parent's trace id either way; `parent_based` defers to its sampled
/// flag, everything else applies the sampler to the trace id itself.
fn decide_with_sampler(
    parent_based: bool,
    sampler: Sampler,
    parent: Option<&ParentContext>,
    fresh: [u8; 16],
) -> ([u8; 16], bool) {
    let (trace_id, parent_sampled) = match parent {
        Some(parent) => (parent.trace_id, Some(parent.sampled)),
        None => (fresh, None),
    };
    let sampled = match (parent_sampled, parent_based) {
        (Some(sampled), true) => sampled,
        _ => {
            let head = u64::from_be_bytes(trace_id[..8].try_into().unwrap());
            sampler.samples(head)
        }
    };
    (trace_id, sampled)
}

/// The sampler is private, so the private suite reaches the decision
/// through the ratio the configuration carries.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn decide(
    parent_based: bool,
    sample_ratio: f64,
    parent: Option<&ParentContext>,
    fresh: [u8; 16],
) -> ([u8; 16], bool) {
    decide_with_sampler(
        parent_based,
        Sampler::from_ratio(sample_ratio),
        parent,
        fresh,
    )
}

/// Join an in-process parent: same trace and decision, with a fresh span id.
pub fn child_of(parent: &TraceContext) -> Option<TraceContext> {
    TELEMETRY.get()?;
    Some(TraceContext {
        trace_id: parent.trace_id,
        span_id: rand::random(),
        sampled: parent.sampled,
    })
}

/// A child context: same trace and decision, with a fresh span id.
pub fn child_context(parent: &TraceContext) -> TraceContext {
    TraceContext {
        trace_id: parent.trace_id,
        span_id: rand::random(),
        sampled: parent.sampled,
    }
}

/// Queue a finished span. Never blocks: a full channel counts a drop —
/// telemetry sheds, requests do not.
pub fn record(span: Span) {
    send(Event::Span(span));
}

/// Queue a captured console line, truncated to the cap.
pub fn record_log(mut log: Log) {
    cap_body(&mut log.body);
    send(Event::Log(log));
}

/// Bound a captured error the way a log body is bound, so one runaway
/// exception message cannot dominate a batch or a span row.
pub(crate) fn cap_error(mut error: String) -> String {
    cap_body(&mut error);
    error
}

#[doc(hidden)]
pub fn cap_body(body: &mut String) {
    if body.len() <= LOG_BODY_CAP {
        return;
    }
    let mut end = LOG_BODY_CAP;
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    body.truncate(end);
}

fn send(event: Event) {
    let Some(telemetry) = TELEMETRY.get() else {
        return;
    };
    if telemetry.tx.try_send(event).is_err() {
        telemetry.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn now_unix_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// Start the pipeline. Called at most once, from startup, only when the
/// operator turned telemetry on and a bucket exists.
pub fn init(
    config: &Config,
    bucket: Option<Bucket>,
    node: String,
    region: String,
) -> anyhow::Result<()> {
    let sink = match config.sink {
        SinkChoice::Bucket => {
            let bucket = bucket.expect("the bucket sink needs a bucket");
            tracing::info!(
                retention = %config.retention.label(),
                sample_ratio = config.sample_ratio,
                bucket = %bucket.name,
                "telemetry on; Parquet to the bucket"
            );
            if let Retention::Days(days) = config.retention {
                tokio::spawn(sweep_loop(bucket.clone(), days, config.sweep));
            }
            SinkRuntime::Bucket {
                bucket,
                retention: config.retention.label(),
            }
        }
        SinkChoice::Otlp => {
            tracing::info!(
                endpoint = %config.otlp_endpoint,
                sample_ratio = config.sample_ratio,
                "telemetry on; OTLP to the collector"
            );
            SinkRuntime::Otlp {
                client: reqwest::Client::builder()
                    .timeout(config.otlp_timeout)
                    .build()
                    .map_err(|error| anyhow!("otlp client: {error}"))?,
                traces_url: format!("{}/v1/traces", config.otlp_endpoint),
                logs_url: format!("{}/v1/logs", config.otlp_endpoint),
                headers: config.otlp_headers.clone(),
            }
        }
    };
    let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
    let telemetry = Telemetry {
        tx,
        dropped: AtomicU64::new(0),
        sampler: Sampler::from_ratio(config.sample_ratio),
        parent_based: config.parent_based,
    };
    if TELEMETRY.set(telemetry).is_err() {
        panic!("telemetry initialized twice");
    }
    tokio::spawn(pump(
        rx,
        sink,
        node,
        region,
        config.service.clone(),
        config.flush,
        config.flush_bytes,
    ));
    Ok(())
}

enum SinkRuntime {
    Bucket {
        bucket: Bucket,
        retention: String,
    },
    Otlp {
        client: reqwest::Client,
        traces_url: String,
        logs_url: String,
        headers: Vec<(String, String)>,
    },
}

impl SinkRuntime {
    /// Ship one flush interval's events. Encoding runs on the blocking
    /// pool either way. For OTLP, the return value counts records discarded
    /// after a permanent refusal or retry exhaustion.
    async fn flush(
        &self,
        spans: Vec<Span>,
        logs: Vec<Log>,
        node: &str,
        region: &str,
        service: &str,
    ) -> u64 {
        let span_count = spans.len() as u64;
        let log_count = logs.len() as u64;
        match self {
            SinkRuntime::Bucket { bucket, retention } => {
                let node_ = node.to_string();
                let region_ = region.to_string();
                let encoded = tokio::task::spawn_blocking(move || {
                    let spans = (!spans.is_empty())
                        .then(|| encode_spans(&spans, &node_, &region_))
                        .transpose()?;
                    let logs = (!logs.is_empty())
                        .then(|| encode_logs(&logs, &node_, &region_))
                        .transpose()?;
                    anyhow::Ok((spans, logs))
                })
                .await;
                let (span_bytes, log_bytes) = match flatten(encoded) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::warn!(%error, "telemetry batch lost: encode failed");
                        return 0;
                    }
                };
                let meta = [
                    ("celld-retention", retention.as_str()),
                    ("celld-schema", SCHEMA_VERSION),
                ];
                for (prefix, bytes) in [(TRACES_PREFIX, span_bytes), (LOGS_PREFIX, log_bytes)] {
                    let Some(bytes) = bytes else { continue };
                    let key = object_key(prefix, node, now_unix_us());
                    if let Err(error) = bucket.put_with_meta(&key, bytes, &meta).await {
                        tracing::warn!(%error, key, "telemetry batch lost: put failed");
                    }
                }
                0
            }
            SinkRuntime::Otlp {
                client,
                traces_url,
                logs_url,
                headers,
            } => {
                let node_ = node.to_string();
                let region_ = region.to_string();
                let service_ = service.to_string();
                let encoded = tokio::task::spawn_blocking(move || {
                    let spans = (!spans.is_empty())
                        .then(|| crate::otlp::traces_request(&spans, &node_, &region_, &service_));
                    let logs = (!logs.is_empty())
                        .then(|| crate::otlp::logs_request(&logs, &node_, &region_, &service_));
                    anyhow::Ok((spans, logs))
                })
                .await;
                let (span_bytes, log_bytes) = match flatten(encoded) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        tracing::warn!(%error, "telemetry batch lost: encode failed");
                        return span_count + log_count;
                    }
                };
                let mut dropped = 0;
                for (url, bytes, count) in [
                    (traces_url, span_bytes, span_count),
                    (logs_url, log_bytes, log_count),
                ] {
                    let Some(bytes) = bytes else { continue };
                    if !post_otlp(client, url, headers, bytes.into()).await {
                        dropped += count;
                    }
                }
                dropped
            }
        }
    }
}

/// Send the same encoded request until the collector accepts it, refuses it
/// permanently, or the retry budget expires. This future owns only one batch.
/// While it sleeps, request handling continues and the bounded channel sheds
/// excess telemetry instead of turning an outage into application backpressure.
async fn post_otlp(
    client: &reqwest::Client,
    url: &str,
    headers: &[(String, String)],
    bytes: bytes::Bytes,
) -> bool {
    for attempt in 1..=OTLP_MAX_ATTEMPTS {
        let mut request = client
            .post(url)
            .header("content-type", "application/x-protobuf")
            .body(bytes.clone());
        for (name, value) in headers {
            request = request.header(name.as_str(), value.as_str());
        }
        let (error, delay) = match request.send().await {
            Ok(response) if response.status().is_success() => return true,
            Ok(response) if retryable_otlp_status(response.status()) => {
                let status = response.status();
                let delay =
                    otlp_response_delay(status, retry_after(&response), otlp_backoff(attempt))
                        .expect("the retryable status guard supplies a retry delay");
                (format!("collector returned {status}"), delay)
            }
            Ok(response) => {
                tracing::warn!(
                    status = %response.status(),
                    url,
                    "telemetry batch lost: collector refused permanently"
                );
                return false;
            }
            Err(error) => (error.to_string(), otlp_backoff(attempt)),
        };
        if attempt == OTLP_MAX_ATTEMPTS {
            tracing::warn!(
                %error,
                url,
                attempts = OTLP_MAX_ATTEMPTS,
                "telemetry batch lost: OTLP retry budget exhausted"
            );
            return false;
        }
        tracing::warn!(
            %error,
            url,
            attempt,
            delay_ms = delay.as_millis(),
            "telemetry export failed transiently; retrying"
        );
        tokio::time::sleep(delay).await;
    }
    unreachable!("the nonzero OTLP attempt range always returns")
}

fn retryable_otlp_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 502 | 503 | 504)
}

fn otlp_response_delay(
    status: reqwest::StatusCode,
    retry_after: Option<Duration>,
    fallback: Duration,
) -> Option<Duration> {
    retryable_otlp_status(status).then(|| retry_after.unwrap_or(fallback).min(OTLP_BACKOFF_MAX))
}

fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    if !matches!(response.status().as_u16(), 429 | 503) {
        return None;
    }
    let value = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let retry_at = httpdate::parse_http_date(value).ok()?;
    Some(
        retry_at
            .duration_since(SystemTime::now())
            .unwrap_or_default(),
    )
}

fn otlp_backoff(attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    let ceiling = OTLP_BACKOFF_INITIAL
        .saturating_mul(1_u32 << exponent)
        .min(OTLP_BACKOFF_MAX);
    let ceiling_ms = ceiling.as_millis() as u64;
    Duration::from_millis(rand::random::<u64>() % (ceiling_ms + 1))
}

#[cfg(all(test, celld_internal_tests))]
mod telemetry_contract {
    use super::*;

    include!(env!("CELLD_INTERNAL_TELEMETRY_TESTS"));
}

/// A `spawn_blocking` result unwrapped honestly: the panic and the
/// encode error both lose the batch with a reason.
fn flatten<T>(joined: Result<anyhow::Result<T>, tokio::task::JoinError>) -> anyhow::Result<T> {
    match joined {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error),
        Err(error) => Err(anyhow!("encoder panicked: {error}")),
    }
}

/// Drain the channel into Parquet objects — one per signal per flush
/// interval with traffic. Encode on the blocking pool; PUT as ordinary
/// async I/O.
async fn pump(
    mut rx: tokio::sync::mpsc::Receiver<Event>,
    sink: SinkRuntime,
    node: String,
    region: String,
    service: String,
    flush: Duration,
    flush_bytes: usize,
) {
    let mut batch: Vec<Event> = Vec::new();
    loop {
        let deadline = tokio::time::Instant::now() + flush;
        let mut bytes = 0usize;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(event)) => {
                    bytes += event.approx_bytes();
                    batch.push(event);
                    // Firehose semantics: the size trigger cuts a batch
                    // early so a busy node ships few, fat objects.
                    if bytes >= flush_bytes {
                        break;
                    }
                }
                Ok(None) => return, // sender is static; process is ending
                Err(_) => break,    // interval elapsed
            }
        }
        if batch.is_empty() {
            continue;
        }
        let mut spans = Vec::new();
        let mut logs = Vec::new();
        for event in batch.drain(..) {
            match event {
                Event::Span(span) => spans.push(span),
                Event::Log(log) => logs.push(log),
            }
        }
        let sink_dropped = sink.flush(spans, logs, &node, &region, &service).await;
        let dropped = sink_dropped.saturating_add(
            TELEMETRY
                .get()
                .map(|telemetry| telemetry.dropped.swap(0, Ordering::Relaxed))
                .unwrap_or(0),
        );
        if dropped > 0 {
            tracing::warn!(dropped, "telemetry shed");
        }
    }
}

/// `<prefix>/<node>/<yyyy/mm/dd/hh>/<flush_us>-<rand>.parquet`.
/// Partitioned by arrival at the flush, which is what retention prunes
/// by; event timestamps stay exact inside the file.
#[doc(hidden)]
pub fn object_key(prefix: &str, node: &str, unix_us: i64) -> String {
    let seconds = unix_us / 1_000_000;
    let (y, m, d) = civil_from_days(seconds.div_euclid(86_400));
    let hour = seconds.rem_euclid(86_400) / 3_600;
    let tag: u32 = rand::random();
    format!("{prefix}/{node}/{y:04}/{m:02}/{d:02}/{hour:02}/{unix_us}-{tag:08x}.parquet")
}

/// The retention sweep: celld deletes its own old telemetry, because
/// an S3 lifecycle rule needs bucket-level permissions the deliberately
/// object-scoped credentials must not require. Prefix
/// deletes over the day-partitioned layout, idempotent and safe to
/// race between nodes — every node sweeps the whole telemetry prefix,
/// so a dead node's data expires too. The sweep enforces the *current*
/// configuration; the per-object retention stamp is forensic.
async fn sweep_loop(bucket: Bucket, retention_days: u32, every: Duration) {
    loop {
        let cutoff = cutoff_date(now_unix_us(), retention_days);
        let deleted = sweep_once(&bucket, cutoff).await;
        if deleted > 0 {
            tracing::info!(deleted, retention_days, "telemetry swept");
        }
        tokio::time::sleep(every).await;
    }
}

/// One complete retention pass. A page failure ends only that signal's pass,
/// while deletions completed from its earlier pages remain effective.
#[doc(hidden)]
pub async fn sweep_once(bucket: &Bucket, cutoff: (i64, u32, u32)) -> u64 {
    let mut deleted = 0u64;
    for prefix in [TRACES_PREFIX, LOGS_PREFIX] {
        let mut page_token = None;
        loop {
            let page = match bucket
                .objects_page(prefix, page_token, SWEEP_PAGE_SIZE)
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!(%error, prefix, "telemetry sweep could not list");
                    break;
                }
            };
            let next_page = page.page_token;
            // Retain only the keys this page proves are expired. Fresh and
            // unparseable descriptions are released before deletion starts.
            let expired_keys: Vec<String> = page
                .objects
                .into_iter()
                .filter_map(|object| {
                    let key = object.location.as_ref();
                    expired(key, prefix, cutoff).then(|| key.to_string())
                })
                .collect();
            if !expired_keys.is_empty() {
                deleted += bucket.delete_many(&expired_keys).await.len() as u64;
            }
            match next_page {
                Some(token) => page_token = Some(token),
                None => break,
            }
        }
    }
    deleted
}

/// The newest civil date old enough to delete: strictly before
/// `retention_days` whole days ago.
#[doc(hidden)]
pub fn cutoff_date(now_unix_us: i64, retention_days: u32) -> (i64, u32, u32) {
    let today = (now_unix_us / 1_000_000).div_euclid(86_400);
    civil_from_days(today - retention_days as i64)
}

/// Whether a telemetry object key's day partition is older than the
/// cutoff. Keys that do not parse as
/// `<prefix>/<node>/<yyyy>/<mm>/<dd>/...` are never touched: the sweep
/// deletes only what the layout proves is telemetry with a date.
#[doc(hidden)]
pub fn expired(key: &str, prefix: &str, cutoff: (i64, u32, u32)) -> bool {
    let Some(rest) = key
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix('/'))
    else {
        return false;
    };
    let mut parts = rest.split('/');
    let _node = parts.next();
    let (Some(y), Some(m), Some(d)) = (parts.next(), parts.next(), parts.next()) else {
        return false;
    };
    let (Ok(y), Ok(m), Ok(d)) = (y.parse::<i64>(), m.parse::<u32>(), d.parse::<u32>()) else {
        return false;
    };
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return false;
    }
    (y, m, d) < cutoff
}

/// Days since the Unix epoch to a civil date (Howard Hinnant's
/// `civil_from_days`, public domain construction).
#[doc(hidden)]
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// The Parquet schema, in message-type form because the text is the
/// documentation. These column names are the contract in `docs/telemetry.md`,
/// so a schema change requires a documentation change.
const MESSAGE_TYPE: &str = "
message celld_span {
  required binary node (STRING);
  required binary region (STRING);
  required binary trace_id (STRING);
  required binary span_id (STRING);
  optional binary parent_span_id (STRING);
  required binary name (STRING);
  required int32 kind (INTEGER(8,false));
  required int64 start_unix_us;
  required int64 duration_us;
  required boolean ok;
  optional binary error (STRING);
  optional binary request_id (STRING);
  optional binary cell (STRING);
  optional int64 epoch;
  optional int64 isolate;
  optional int64 queue_wait_us;
  optional binary url (STRING);
  optional int32 http_status (INTEGER(16,false));
  optional boolean parent_remote;
}";

/// The logs signal: one row per captured console line, joined to traces
/// on `trace_id`.
const LOG_MESSAGE_TYPE: &str = "
message celld_log {
  required binary node (STRING);
  required binary region (STRING);
  optional binary trace_id (STRING);
  optional binary span_id (STRING);
  required int64 time_unix_us;
  required binary body (STRING);
}";

/// The native column-writer API rather than the arrow one: the schema is
/// flat primitives, and skipping the `arrow` feature keeps the whole
/// arrow crate stack out of the binary.
#[doc(hidden)]
pub fn encode_spans(spans: &[Span], node: &str, region: &str) -> anyhow::Result<Vec<u8>> {
    use parquet::basic::Compression;
    use parquet::basic::ZstdLevel;
    use parquet::data_type::BoolType;
    use parquet::data_type::ByteArray;
    use parquet::data_type::ByteArrayType;
    use parquet::data_type::Int32Type;
    use parquet::data_type::Int64Type;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::ColumnPath;
    use std::sync::Arc;

    let schema = Arc::new(parse_message_type(MESSAGE_TYPE)?);
    let properties = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            // Point lookups by trace id are the one query a
            // time-partitioned scan is bad at; the bloom filter is what
            // makes them cheap.
            .set_column_bloom_filter_enabled(ColumnPath::from("trace_id"), true)
            .build(),
    );
    let mut writer = SerializedFileWriter::new(Vec::new(), schema, properties)?;
    let mut group = writer.next_row_group()?;

    // Columns are written in schema order; each macro arm consumes the
    // next one. Optionals carry definition levels (1 present, 0 null)
    // and pack only the present values, as the format requires.
    macro_rules! column {
        ($type:ty, req $values:expr) => {{
            let mut column = group.next_column()?.expect("schema column");
            let values = $values;
            column.typed::<$type>().write_batch(&values, None, None)?;
            column.close()?;
        }};
        ($type:ty, opt $values:expr) => {{
            let mut column = group.next_column()?.expect("schema column");
            let options = $values;
            let levels: Vec<i16> = options.iter().map(|value| value.is_some() as i16).collect();
            let values: Vec<_> = options.into_iter().flatten().collect();
            column
                .typed::<$type>()
                .write_batch(&values, Some(&levels), None)?;
            column.close()?;
        }};
    }
    let text = |value: &str| ByteArray::from(value.as_bytes().to_vec());
    let each = spans.iter();

    column!(ByteArrayType, req each.clone().map(|_| text(node)).collect::<Vec<_>>());
    column!(ByteArrayType, req each.clone().map(|_| text(region)).collect::<Vec<_>>());
    column!(ByteArrayType, req each.clone().map(|s| text(&hex(&s.ids.trace_id))).collect::<Vec<_>>());
    column!(ByteArrayType, req each.clone().map(|s| text(&hex(&s.ids.span_id))).collect::<Vec<_>>());
    column!(ByteArrayType, opt each.clone().map(|s| s.parent_span_id.map(|id| text(&hex(&id)))).collect::<Vec<_>>());
    column!(ByteArrayType, req each.clone().map(|s| text(s.name)).collect::<Vec<_>>());
    column!(Int32Type, req each.clone().map(|s| s.kind as i32).collect::<Vec<_>>());
    column!(Int64Type, req each.clone().map(|s| s.start_unix_us).collect::<Vec<_>>());
    column!(Int64Type, req each.clone().map(|s| s.duration_us).collect::<Vec<_>>());
    column!(BoolType, req each.clone().map(|s| s.ok).collect::<Vec<_>>());
    column!(ByteArrayType, opt each.clone().map(|s| s.error.as_deref().map(text)).collect::<Vec<_>>());
    column!(ByteArrayType, opt each.clone().map(|s| s.request_id.as_deref().map(text)).collect::<Vec<_>>());
    column!(ByteArrayType, opt each.clone().map(|s| s.cell.as_deref().map(text)).collect::<Vec<_>>());
    column!(Int64Type, opt each.clone().map(|s| s.epoch.map(|v| v as i64)).collect::<Vec<_>>());
    column!(Int64Type, opt each.clone().map(|s| s.isolate.map(|v| v as i64)).collect::<Vec<_>>());
    column!(Int64Type, opt each.clone().map(|s| s.queue_wait_us).collect::<Vec<_>>());
    column!(ByteArrayType, opt each.clone().map(|s| s.url.as_deref().map(text)).collect::<Vec<_>>());
    column!(Int32Type, opt each.clone().map(|s| s.http_status.map(|v| v as i32)).collect::<Vec<_>>());
    column!(BoolType, opt each.clone().map(|s| s.parent_remote).collect::<Vec<_>>());

    group.close()?;
    Ok(writer.into_inner()?)
}

#[doc(hidden)]
pub fn encode_logs(logs: &[Log], node: &str, region: &str) -> anyhow::Result<Vec<u8>> {
    use parquet::basic::Compression;
    use parquet::basic::ZstdLevel;
    use parquet::data_type::ByteArray;
    use parquet::data_type::ByteArrayType;
    use parquet::data_type::Int64Type;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::ColumnPath;
    use std::sync::Arc;

    let schema = Arc::new(parse_message_type(LOG_MESSAGE_TYPE)?);
    let properties = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_column_bloom_filter_enabled(ColumnPath::from("trace_id"), true)
            .build(),
    );
    let mut writer = SerializedFileWriter::new(Vec::new(), schema, properties)?;
    let mut group = writer.next_row_group()?;

    macro_rules! column {
        ($type:ty, req $values:expr) => {{
            let mut column = group.next_column()?.expect("schema column");
            let values = $values;
            column.typed::<$type>().write_batch(&values, None, None)?;
            column.close()?;
        }};
        ($type:ty, opt $values:expr) => {{
            let mut column = group.next_column()?.expect("schema column");
            let options = $values;
            let levels: Vec<i16> = options.iter().map(|value| value.is_some() as i16).collect();
            let values: Vec<_> = options.into_iter().flatten().collect();
            column
                .typed::<$type>()
                .write_batch(&values, Some(&levels), None)?;
            column.close()?;
        }};
    }
    let text = |value: &str| ByteArray::from(value.as_bytes().to_vec());
    let each = logs.iter();

    column!(ByteArrayType, req each.clone().map(|_| text(node)).collect::<Vec<_>>());
    column!(ByteArrayType, req each.clone().map(|_| text(region)).collect::<Vec<_>>());
    column!(ByteArrayType, opt each.clone().map(|l| l.trace_id.map(|id| text(&hex(&id)))).collect::<Vec<_>>());
    column!(ByteArrayType, opt each.clone().map(|l| l.span_id.map(|id| text(&hex(&id)))).collect::<Vec<_>>());
    column!(Int64Type, req each.clone().map(|l| l.time_unix_us).collect::<Vec<_>>());
    column!(ByteArrayType, req each.clone().map(|l| text(&l.body)).collect::<Vec<_>>());

    group.close()?;
    Ok(writer.into_inner()?)
}
