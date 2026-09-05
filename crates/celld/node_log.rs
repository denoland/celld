// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! v0 of the in-fleet replicated log tier (`CELLD_DURABILITY=fleet`).
//!
//! Each node streams the per-cell L0 LTX segments it has captured but not
//! yet uploaded to a small follower ensemble over the signed peer
//! transport. A write acknowledges when every member holds its segment on
//! disk — write-all, ack-all — or when the ordinary bucket upload proves
//! it first, whichever wins. The bucket upload path is unchanged and
//! remains the tiering mechanism, so node-log recovery re-creates exactly
//! the objects the dead leader would have uploaded and every per-cell
//! restore and compaction mechanism stays byte-for-byte as it is.
//!
//! `log/<node>.json` is the CAS-guarded root of truth for the ensemble and
//! the log epoch. It is created before the node's first fleet-durable ack
//! and never deleted, so a takeover that finds no record may treat the
//! bucket as complete. The decisions are `celld_logic::log_tier`; this
//! module is their executor.
//!
//! v0 limits, deliberate: entries travel as base64 JSON; a follower
//! failure degrades the node to bucket-proof acks until a periodic
//! re-recruit CASes a fresh ensemble; recovery gathers from every
//! reachable sealed member and requires at least one.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::anyhow;
use anyhow::Context;
use celld_logic::log_tier;
use celld_logic::log_tier::LogState;
use tracing::info;
use tracing::warn;

use futures_util::FutureExt;
use futures_util::StreamExt;

use crate::bucket::Bucket;
use crate::ltx_repl::ShipEntry;
use crate::peer_auth::PeerAuth;

mod recovery_progress;

/// The one peer-POST boundary used by the node log.
///
/// Production installs the signed `reqwest` implementation below. A
/// scheduler-controlled implementation can replace the transport while all
/// codecs and follower handlers remain the shipping ones.
pub trait LogTransport: Send + Sync + 'static {
    fn post<'a>(
        &'a self,
        node: &'a str,
        addr: &'a str,
        path: &'a str,
        body: Vec<u8>,
        deadline: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    >;

    /// Open one long-lived ordered byte stream to a member
    /// (the ordered-transport design). The default refuses, so
    /// every test transport keeps its request-shaped world unless a test
    /// opts in.
    fn open_stream<'a>(
        &'a self,
        node: &'a str,
        addr: &'a str,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<LogStreamIo>> + Send + 'a>>
    {
        let _ = (node, addr, path);
        Box::pin(async { anyhow::bail!("transport does not support ordered streams") })
    }
}

/// The duplex an ordered stream rides. Anything tokio-readable and
/// -writable serves: a reqwest upgrade in production, a `tokio::io::duplex`
/// in the twins.
pub trait LogStreamDuplex: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}
impl<T> LogStreamDuplex for T where T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}
pub type LogStreamIo = Box<dyn LogStreamDuplex>;

/// One stream frame: a little-endian length prefix and the payload.
/// Leader-to-follower payloads are `encode_append` bodies; follower-to-
/// leader payloads are JSON `AppendResp`s, answered strictly in arrival
/// order. The cap bounds a corrupt or hostile length prefix.
const STREAM_FRAME_CAP: usize = 64 * 1024 * 1024;

#[doc(hidden)]
pub async fn read_frame<R>(io: &mut R) -> anyhow::Result<Option<Vec<u8>>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut len = [0_u8; 4];
    match io.read_exact(&mut len).await {
        Ok(_) => {}
        // EOF on a frame boundary is the peer closing; mid-frame EOF below
        // is an error, exactly like a torn append body.
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let len = u32::from_le_bytes(len) as usize;
    if len > STREAM_FRAME_CAP {
        anyhow::bail!("stream frame of {len} bytes exceeds the cap");
    }
    let mut payload = vec![0_u8; len];
    io.read_exact(&mut payload).await?;
    Ok(Some(payload))
}

#[doc(hidden)]
pub async fn write_frame<W>(io: &mut W, payload: &[u8]) -> anyhow::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    io.write_all(&u32::try_from(payload.len())?.to_le_bytes())
        .await?;
    io.write_all(payload).await?;
    io.flush().await?;
    Ok(())
}

/// Frames one commit may carry from one stream. The leader's window holds
/// eight frames per lane, so an honest burst is eight; the bound only keeps
/// a misbehaving peer from building an unbounded commit.
const SERVE_BATCH_FRAMES: usize = 64;

/// The bytes one serve read asks for: enough that a burst of small frames
/// arrives in one read rather than one read per frame.
const SERVE_READ_CHUNK: usize = 64 * 1024;

/// One complete frame from the front of `buf`, when it holds one. A partial
/// frame stays put for the next read; a header past the cap is the same
/// error `read_frame` reports.
fn take_buffered_frame(buf: &mut Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len > STREAM_FRAME_CAP {
        anyhow::bail!("stream frame of {len} bytes exceeds the cap");
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let frame = buf[4..4 + len].to_vec();
    buf.drain(..4 + len);
    Ok(Some(frame))
}

/// Serve one ordered append stream as a group commit: block for the next
/// frame, take every further frame the socket has ALREADY delivered, commit
/// them as one batch, and answer each in order. Apply order equals arrival
/// order because this loop is the stream's only reader — that single fact
/// is what lets the leader keep several frames in flight without the
/// follower needing reorder state. Any error or EOF ends the stream with no
/// cleanup: the contiguity rule already treats a half-received stream
/// exactly like a half-delivered round, and the seal marks fence a zombie
/// leader's frames here just as they fence its one-shot appends.
///
/// Why the commit is grouped (the 2026-09-01 write-latency ledger, 131k
/// served frames): the leader pipelines up to eight frames per lane, this
/// loop served them one at a time, and each `store.append` paid its own
/// fsync chain — ~4 ms of a ~5.8 ms serial service interval. A burst of
/// eight rounds therefore drained at eight chains, and the leader billed
/// every frame behind the head its queue position times that interval.
/// The frames a burst leaves on the socket are exactly the ones that can
/// share a chain. Merging at the leader instead was tried and refuted: the
/// lane drains its queue into the socket immediately, so there is never a
/// backlog on that side to merge.
pub async fn serve_log_stream<IO>(mut io: IO, store: Arc<FollowerStore>) -> anyhow::Result<()>
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut buf: Vec<u8> = Vec::new();
    // A delivered frame that belongs to another fragment: it could not ride
    // the batch it arrived with, so it opens the next one.
    let mut carried: Option<AppendReq> = None;
    let mut cycle_started = mono_us();
    loop {
        let head = match carried.take() {
            Some(req) => req,
            None => {
                let frame = loop {
                    if let Some(frame) = take_buffered_frame(&mut buf)? {
                        break frame;
                    }
                    buf.reserve(SERVE_READ_CHUNK);
                    if io.read_buf(&mut buf).await? == 0 {
                        // EOF on a frame boundary is the peer closing;
                        // mid-frame EOF is an error, exactly like a torn
                        // append body.
                        if buf.is_empty() {
                            return Ok(());
                        }
                        anyhow::bail!("stream closed mid-frame");
                    }
                };
                decode_append(&frame)?
            }
        };
        // The cycle split is the follower-side latency ledger: `read_ms`
        // is time waiting for (and reading) the batch head, and the rest
        // is this loop's serial service — the lane's throughput ceiling.
        let read_done = mono_us();
        // The reader-idle ledger: a long read wait is either no traffic or a
        // starved reader task; paired with the leader's head-stall event it
        // tells which. One second is far past any honest inter-frame gap on
        // a loaded lane.
        let idle_ms = read_done.saturating_sub(cycle_started) / 1000;
        if idle_ms >= 1_000 {
            warn!(
                event = "log_serve_idle",
                idle_ms, "stream reader idle past one second"
            );
        }
        let mut batch = AppendBatch::new(head);
        // The drain takes only what has already arrived. A single poll of
        // `read_buf` is cancel-safe — a pending read consumes nothing — so
        // the drain never waits on the wire and never tears a frame, which
        // `read_frame` (a `read_exact` of the header, then the body) could
        // not promise if it were polled once and dropped.
        while batch.frame_count() < SERVE_BATCH_FRAMES {
            let frame = match take_buffered_frame(&mut buf)? {
                Some(frame) => frame,
                None => {
                    buf.reserve(SERVE_READ_CHUNK);
                    match io.read_buf(&mut buf).now_or_never() {
                        // Nothing more delivered, or EOF: commit what is
                        // here. The blocking read above reports the EOF.
                        None | Some(Ok(0)) => break,
                        Some(Ok(_)) => continue,
                        Some(Err(error)) => return Err(error.into()),
                    }
                }
            };
            if let Err(other) = batch.try_push(decode_append(&frame)?) {
                carried = Some(other);
                break;
            }
        }
        let decode_done = mono_us();
        let frames = batch.frame_count();
        let entries = batch.entry_count();
        let answers = store.append_batch(batch).await;
        let append_done = mono_us();
        for resp in &answers {
            write_frame(&mut io, &serde_json::to_vec(resp)?).await?;
        }
        let ack_done = mono_us();
        info!(
            event = "log_serve_cycle",
            frames,
            entries,
            read_ms = read_done.saturating_sub(cycle_started) / 1000,
            decode_ms = decode_done.saturating_sub(read_done) / 1000,
            append_ms = append_done.saturating_sub(decode_done) / 1000,
            ack_ms = ack_done.saturating_sub(append_done) / 1000,
            "served one stream append commit"
        );
        cycle_started = ack_done;
    }
}

/// Concurrent per-cell upload lanes during node-log recovery. The
/// sequential version cost ~47 s for 180 entries on the lab fleet; more
/// than a few lanes multiplied across RACING recoverers (self-recovery
/// plus every survivor's eager sweep) tripped R2's same-object 429 rate
/// limit into a livelock, so the lanes are modest and each upload retries
/// 429-class refusals with backoff.
/// Recovery uploads one merged segment per cell epoch after one watermark
/// lookup each. A dead session on a loaded fleet holds thousands of cells,
/// and at eight in flight the 2026-09-03 restart spent 388 s of its 403 s
/// recovery in this phase; the bucket takes far more in flight than that.
const RECOVERY_UPLOAD_CONCURRENCY: usize = 32;
/// Per-cell coverage reads of the graceful seal and the uncovered-row scan
/// stay below a small fleet's cell count, so a close does not fan out one
/// listing per cell at once.
const COVERAGE_READ_CONCURRENCY: usize = 16;

/// A contender observes a live recovery before it tries to replace it. The
/// node-log tail is bounded to the flush window, so thirty seconds is enough
/// for the elected reader in normal operation and still leaves ninety seconds
/// inside the default rollout readiness bound after a crashed recoverer.
/// A recovery claim whose heartbeat is older than this is stale and may be
/// taken over. The claimant beats every `RECOVERY_HEARTBEAT`, so a live
/// claimant is never mistaken for a dead one by a slow bucket round trip.
pub(crate) const RECOVERY_CLAIM_TTL: std::time::Duration = std::time::Duration::from_secs(30);
const RECOVERY_CLAIM_POLL: std::time::Duration = std::time::Duration::from_millis(250);
const RECOVERY_HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(5);
/// How long a boot waits behind another node's live claim before it says
/// so again in the log.
const RECOVERY_WAIT_REPORT: std::time::Duration = std::time::Duration::from_secs(30);

/// What a recovery does when another node holds a live claim on the log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecoverMode {
    /// A maintenance sweep: another node is on it, so there is nothing to
    /// do here. Every peer used to take the claim over after a fixed wait
    /// and repeat the same gather.
    Sweep,
    /// A boot, or a cold route that needs the seal: this node cannot go on
    /// until the log is sealed, so it waits behind the claimant and takes
    /// the claim over only once its heartbeat is stale.
    Boot,
}

/// The claimant's heartbeat while it gathers, beaten cooperatively at each
/// step of the gather and the upload. A cooperative beat cannot race the
/// seal, and it is deterministic under the simulated clock.
struct ClaimBeat {
    last_mono_ms: u64,
}

impl ClaimBeat {
    fn new() -> Self {
        Self {
            last_mono_ms: mono_ms(),
        }
    }
}

/// Process-monotonic milliseconds for follower-health bookkeeping: latency
/// windows and quarantine arithmetic must not jump with the wall clock.
fn mono_ms() -> u64 {
    crate::asyncrt::mono_ms()
}

fn mono_us() -> u64 {
    crate::asyncrt::mono_us()
}

/// The eviction policy, defaults per the design doc, every constant an E8
/// target the lab can override.
pub fn eviction_policy_from_env() -> anyhow::Result<celld_logic::log_evict::EvictionPolicy> {
    let default = celld_logic::log_evict::EvictionPolicy::default();
    Ok(celld_logic::log_evict::EvictionPolicy {
        budget_ms: crate::env_vars::with_default("CELLD_LOG_EVICT_BUDGET_MS", default.budget_ms)?,
        sibling_factor: default.sibling_factor,
        sustain_ms: crate::env_vars::with_default(
            "CELLD_LOG_EVICT_SUSTAIN_MS",
            default.sustain_ms,
        )?,
        backstop_ms: crate::env_vars::with_default(
            "CELLD_LOG_EVICT_BACKSTOP_MS",
            default.backstop_ms,
        )?,
        quarantine_ms: crate::env_vars::with_default(
            "CELLD_LOG_EVICT_QUARANTINE_MS",
            default.quarantine_ms,
        )?,
        min_swap_interval_ms: default.min_swap_interval_ms,
        window_ms: default.window_ms,
        min_samples: default.min_samples,
        hedge_floor_ms: default.hedge_floor_ms,
        hedge_factor: default.hedge_factor,
    })
}

// ── The record ──────────────────────────────────────────────────────────────
//
// Since the lease-fold, the log record LIVES in the node lease record:
// nodes/<node>.json carries a folded `log` object
// beside its authority fields, and a session's identity is the record's
// generation. Reading "session X/G" means reading X's lease and answering
// None unless it still carries generation G — a replaced record is a
// recovered-then-superseded session, and absence keeps meaning complete.
// Writes here are for DEAD sessions only (recovery's fence and seal): they
// CAS the full wire record, carrying every authority field through
// unchanged and never touching expiry. A LIVE session's own writes go
// through the core's lease chain instead (write_own_log below), because
// the lease has exactly one writer per process and a second one would
// race the renewal guard.

fn lease_key(node: &str) -> String {
    format!("nodes/{node}.json")
}

pub(crate) struct FoldedRead {
    pub(crate) record: log_tier::LogRecord,
    pub(crate) active: bool,
    pub(crate) token: String,
    pub(crate) wire: crate::ownership_store::NodeLeaseWire,
}

fn log_from_wire(log: &crate::ownership_store::NodeLogWire) -> anyhow::Result<log_tier::LogRecord> {
    Ok(log_tier::LogRecord {
        epoch: log.epoch,
        ensemble: log.ensemble.iter().cloned().collect(),
        tiered: log.tiered,
        state: match log.state.as_str() {
            "open" => LogState::Open,
            "recovering" => LogState::Recovering,
            "sealed" => LogState::Sealed,
            other => return Err(anyhow!("unknown log record state {other:?}")),
        },
        claimant: log.claimant.clone(),
        claimed_ms: log.claimed_ms,
    })
}

pub(crate) fn log_to_wire(
    record: &log_tier::LogRecord,
    active: bool,
) -> crate::ownership_store::NodeLogWire {
    crate::ownership_store::NodeLogWire {
        state: match record.state {
            LogState::Open => "open",
            LogState::Recovering => "recovering",
            LogState::Sealed => "sealed",
        }
        .to_string(),
        epoch: record.epoch,
        ensemble: record.ensemble.iter().cloned().collect(),
        tiered: record.tiered,
        active,
        claimant: record.claimant.clone(),
        claimed_ms: record.claimed_ms,
    }
}

pub(crate) async fn read_record(
    bucket: &Bucket,
    session: &str,
) -> anyhow::Result<Option<FoldedRead>> {
    let (node, generation) = session.split_once('/').unwrap_or((session, ""));
    let Some((bytes, token)) = bucket.get(&lease_key(node)).await? else {
        return Ok(None);
    };
    let wire: crate::ownership_store::NodeLeaseWire = serde_json::from_slice(&bytes)?;
    // A bare node name reads whatever session the record carries; a full
    // <node>/<generation> pins it, and a superseded generation is a
    // recovered-then-replaced session whose absence means complete.
    if !generation.is_empty() && wire.generation != generation {
        return Ok(None);
    }
    let Some(log) = wire.log.as_ref() else {
        return Ok(None);
    };
    Ok(Some(FoldedRead {
        record: log_from_wire(log)?,
        active: log.active,
        token,
        wire,
    }))
}

/// CAS a DEAD session's folded log fields. Authority fields ride through
/// from the wire the caller read; expiry is never extended, so this write
/// can only fence, never revive.
pub(crate) async fn write_dead_record(
    bucket: &Bucket,
    session: &str,
    prior: &crate::ownership_store::NodeLeaseWire,
    record: &log_tier::LogRecord,
    active: bool,
    token: &str,
) -> anyhow::Result<Option<String>> {
    let (node, _) = session.split_once('/').unwrap_or((session, ""));
    let mut wire = prior.clone();
    wire.log = Some(log_to_wire(record, active));
    let body = serde_json::to_vec(&wire)?;
    bucket.put_cas(&lease_key(node), body, Some(token)).await
}

/// The LIVE session's writer for its own folded log: publish the desired
/// object to the ownership store, nudge the core into an immediate
/// renewal, and wait until an APPLIED lease write carries it. The lease
/// chain stays single-writer — this is how open, activation, and the
/// graceful seal become durable without racing the renewal guard. A
/// fenced node's renewals stop applying, so the wait times out and the
/// caller refuses, which is the fence doing its job.
pub struct OwnLog {
    pub ownership: Arc<crate::ownership_store::BucketOwnership>,
    pub nudge: Box<dyn Fn() + Send + Sync>,
    /// One publish outstanding at a time: with the seq-tagged applied
    /// notification, "applied seq >= mine" then implies the applied body
    /// IS my object. The live transition capability holds its wider guard
    /// across this publication and the decision that produced it.
    pub write_lock: tokio::sync::Mutex<()>,
}

impl OwnLog {
    async fn write(&self, log: Option<crate::ownership_store::NodeLogWire>) -> anyhow::Result<()> {
        let _serialized = self.write_lock.lock().await;
        let mut rx = self.ownership.applied_log();
        let seq = self.ownership.set_own_log(log);
        (self.nudge)();
        let deadline = crate::asyncrt::mono_ms().saturating_add(10_000);
        loop {
            if rx.borrow_and_update().1 >= seq {
                return Ok(());
            }
            crate::asyncrt::select_biased! {
                "an applied-log change wins a tie so a completed write does not time out";
                changed = rx.changed() => {
                    anyhow::ensure!(changed.is_ok(), "the lease writer is gone");
                },
                _ = crate::asyncrt::sleep_until(deadline) => {
                    anyhow::bail!(
                        "no lease write carried the folded log within 10s;                          treating this session as fenced"
                    );
                }
            }
        }
    }

    fn current(&self) -> Option<crate::ownership_store::NodeLogWire> {
        self.ownership.own_log()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeLogLifecycle {
    Running,
    Quiescing,
}

/// The one capability for a live session's record transitions. The guard
/// covers the current-record read, the transition decision, the durable
/// publication, and any in-memory value coupled to that publication. A
/// separate check before this guard cannot enforce shutdown because close
/// can start between that check and the record write.
struct LiveLogTransitions {
    own_log: Arc<OwnLog>,
    lifecycle: tokio::sync::Mutex<NodeLogLifecycle>,
}

struct LiveLogTransition<'a> {
    transitions: &'a LiveLogTransitions,
    lifecycle: tokio::sync::MutexGuard<'a, NodeLogLifecycle>,
}

impl LiveLogTransitions {
    fn new(own_log: Arc<OwnLog>) -> Self {
        Self {
            own_log,
            lifecycle: tokio::sync::Mutex::new(NodeLogLifecycle::Running),
        }
    }

    async fn lock(&self) -> LiveLogTransition<'_> {
        LiveLogTransition {
            transitions: self,
            lifecycle: self.lifecycle.lock().await,
        }
    }

    async fn activate(&self, expected: &log_tier::LogRecord) -> anyhow::Result<bool> {
        let transition = self.lock().await;
        if !transition.is_running() {
            return Ok(false);
        }
        let Some(current) = transition.current() else {
            return Ok(false);
        };
        let current_record = log_from_wire(&current)?;
        if current_record != *expected {
            return Ok(false);
        }
        transition.write(Some(log_to_wire(expected, true))).await?;
        Ok(true)
    }
}

impl LiveLogTransition<'_> {
    fn is_running(&self) -> bool {
        *self.lifecycle == NodeLogLifecycle::Running
    }

    fn begin_quiescing(&mut self) -> bool {
        if !self.is_running() {
            return false;
        }
        *self.lifecycle = NodeLogLifecycle::Quiescing;
        true
    }

    fn current(&self) -> Option<crate::ownership_store::NodeLogWire> {
        self.transitions.own_log.current()
    }

    async fn write(&self, log: Option<crate::ownership_store::NodeLogWire>) -> anyhow::Result<()> {
        self.transitions.own_log.write(log).await
    }
}

// ── Wire types for the peer endpoints ───────────────────────────────────────
//
// The two byte-dominated messages — the append request and the tail
// response — travel as a small binary framing; every control message
// (append response, seal, tail request) stays JSON. The same entry
// encoding is the follower's on-disk `<seq>.entry` format, so one decoder
// serves the wire and the disk. All integers little-endian:
//
//   append body:   "CLA1" u16 leader_len leader u64 epoch u64 truncate_to
//                  u32 count entry*
//   tail response: "CLT1" u32 count entry*
//   entry:         "CLE1" u64 seq u16 cell_len cell u64 cell_epoch
//                  u64 txid u32 len bytes

const APPEND_MAGIC: &[u8; 4] = b"CLA1";
const TAIL_MAGIC: &[u8; 4] = b"CLT1";
const ENTRY_MAGIC: &[u8; 4] = b"CLE1";

pub struct Entry {
    pub seq: u64,
    pub cell: String,
    pub cell_epoch: u64,
    pub txid: u64,
    pub bytes: Vec<u8>,
}

pub struct AppendReq {
    pub leader: String,
    pub epoch: u64,
    /// Follower may drop entries at or below this sequence; they are in the
    /// bucket.
    pub truncate_to: u64,
    pub entries: Vec<Entry>,
}

fn validate_log_leader(leader: &str) -> anyhow::Result<()> {
    let validate = |name| crate::machine::validate_node_name(name);
    match leader.split_once('/') {
        Some((node, generation)) => {
            validate(node)?;
            validate(generation)?;
        }
        None => validate(leader)?,
    }
    Ok(())
}

fn deserialize_log_leader<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let leader = <String as serde::Deserialize>::deserialize(deserializer)?;
    validate_log_leader(&leader).map_err(|error| {
        serde::de::Error::custom(format!("invalid log leader identity: {error}"))
    })?;
    Ok(leader)
}

fn take<'a>(buf: &mut &'a [u8], n: usize, what: &str) -> anyhow::Result<&'a [u8]> {
    if buf.len() < n {
        return Err(anyhow!("log wire: truncated {what}"));
    }
    let (head, rest) = buf.split_at(n);
    *buf = rest;
    Ok(head)
}

fn take_u16(buf: &mut &[u8], what: &str) -> anyhow::Result<u16> {
    Ok(u16::from_le_bytes(take(buf, 2, what)?.try_into().unwrap()))
}

fn take_u32(buf: &mut &[u8], what: &str) -> anyhow::Result<u32> {
    Ok(u32::from_le_bytes(take(buf, 4, what)?.try_into().unwrap()))
}

fn take_u64(buf: &mut &[u8], what: &str) -> anyhow::Result<u64> {
    Ok(u64::from_le_bytes(take(buf, 8, what)?.try_into().unwrap()))
}

fn take_string(buf: &mut &[u8], what: &str) -> anyhow::Result<String> {
    let len = take_u16(buf, what)? as usize;
    Ok(std::str::from_utf8(take(buf, len, what)?)
        .map_err(|_| anyhow!("log wire: {what} not utf-8"))?
        .to_string())
}

#[doc(hidden)]
pub fn encode_entry(entry: &Entry, out: &mut Vec<u8>) {
    out.extend_from_slice(ENTRY_MAGIC);
    out.extend_from_slice(&entry.seq.to_le_bytes());
    out.extend_from_slice(&(entry.cell.len() as u16).to_le_bytes());
    out.extend_from_slice(entry.cell.as_bytes());
    out.extend_from_slice(&entry.cell_epoch.to_le_bytes());
    out.extend_from_slice(&entry.txid.to_le_bytes());
    out.extend_from_slice(&(entry.bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&entry.bytes);
}

fn decode_entry(buf: &mut &[u8]) -> anyhow::Result<Entry> {
    if take(buf, 4, "entry magic")? != ENTRY_MAGIC {
        return Err(anyhow!("log wire: bad entry magic"));
    }
    let seq = take_u64(buf, "entry seq")?;
    let cell = take_string(buf, "entry cell")?;
    let cell_epoch = take_u64(buf, "entry cell_epoch")?;
    let txid = take_u64(buf, "entry txid")?;
    let len = take_u32(buf, "entry len")? as usize;
    let bytes = take(buf, len, "entry bytes")?.to_vec();
    Ok(Entry {
        seq,
        cell,
        cell_epoch,
        txid,
        bytes,
    })
}

pub fn encode_append(req: &AppendReq) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(APPEND_MAGIC);
    out.extend_from_slice(&(req.leader.len() as u16).to_le_bytes());
    out.extend_from_slice(req.leader.as_bytes());
    out.extend_from_slice(&req.epoch.to_le_bytes());
    out.extend_from_slice(&req.truncate_to.to_le_bytes());
    out.extend_from_slice(&(req.entries.len() as u32).to_le_bytes());
    for entry in &req.entries {
        encode_entry(entry, &mut out);
    }
    out
}

pub fn decode_append(mut body: &[u8]) -> anyhow::Result<AppendReq> {
    let buf = &mut body;
    if take(buf, 4, "append magic")? != APPEND_MAGIC {
        return Err(anyhow!("log wire: bad append magic"));
    }
    let leader = take_string(buf, "append leader")?;
    validate_log_leader(&leader).context("invalid log leader identity")?;
    let epoch = take_u64(buf, "append epoch")?;
    let truncate_to = take_u64(buf, "append truncate_to")?;
    let entries = decode_entries(buf, "append")?;
    Ok(AppendReq {
        leader,
        epoch,
        truncate_to,
        entries,
    })
}

/// The shared tail of both framed bodies: a count, that many entries,
/// and nothing after them.
fn decode_entries(buf: &mut &[u8], what: &str) -> anyhow::Result<Vec<Entry>> {
    let count = take_u32(buf, what)? as usize;
    let mut entries = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        entries.push(decode_entry(buf)?);
    }
    if !buf.is_empty() {
        return Err(anyhow!("log wire: trailing bytes after {what}"));
    }
    Ok(entries)
}

pub fn encode_tail_resp(resp: &TailResp) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(TAIL_MAGIC);
    out.extend_from_slice(&(resp.entries.len() as u32).to_le_bytes());
    for entry in &resp.entries {
        encode_entry(entry, &mut out);
    }
    out
}

pub fn decode_tail_resp(mut body: &[u8]) -> anyhow::Result<TailResp> {
    let buf = &mut body;
    if take(buf, 4, "tail magic")? != TAIL_MAGIC {
        return Err(anyhow!("log wire: bad tail magic"));
    }
    Ok(TailResp {
        entries: decode_entries(buf, "tail")?,
    })
}

/// The frames one commit may carry: consecutive stream frames of ONE
/// fragment. Growing a batch is the only way to build one, and it hands
/// back a frame for another leader or epoch, so `append_batch` never has to
/// decide what a mixed batch means — the caller keeps the refused frame and
/// opens the next batch with it.
pub struct AppendBatch {
    frames: Vec<AppendReq>,
}

impl AppendBatch {
    pub fn new(first: AppendReq) -> Self {
        Self {
            frames: vec![first],
        }
    }

    /// Add a frame of the same fragment, or hand it back untouched.
    pub fn try_push(&mut self, req: AppendReq) -> Result<(), AppendReq> {
        let head = &self.frames[0];
        if req.leader != head.leader || req.epoch != head.epoch {
            return Err(req);
        }
        self.frames.push(req);
        Ok(())
    }

    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    pub fn entry_count(&self) -> usize {
        self.frames.iter().map(|frame| frame.entries.len()).sum()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AppendResp {
    pub ok: bool,
    pub end: u64,
    /// The fragment epoch the reported `end` belongs to. A refusal whose
    /// echo matches the request epoch and whose end covers the batch still
    /// confirms durability — the retransmission case — while a refusal
    /// from another fragment says nothing about these sequences. Absent
    /// from binaries that predate hedging, which reads as unconfirmable.
    #[serde(default)]
    pub epoch: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SealReq {
    #[serde(deserialize_with = "deserialize_log_leader")]
    pub leader: String,
    pub epoch: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SealResp {
    pub end: u64,
    /// The highest sequence that this follower already removed after the
    /// bucket covered it. Recovery must receive every sequence in
    /// `(base, end]` before this follower is complete evidence. An older
    /// follower omits this field, so it cannot certify a recovery pass.
    #[serde(default)]
    pub base: Option<u64>,
    /// The fragment epoch that this response can certify. A name-reused
    /// machine with a fresh disk answers 0. A follower with an incomplete
    /// retained range also answers 0, so an older recovery caller cannot
    /// count a successful-looking response with a gap.
    #[serde(default)]
    pub fragment_epoch: u64,
    /// The fragment epoch that this follower actually holds. Recovery uses
    /// this field to distinguish an incomplete current fragment from a
    /// different fragment. An older follower omits it.
    #[serde(default)]
    pub held_fragment_epoch: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TailReq {
    #[serde(deserialize_with = "deserialize_log_leader")]
    pub leader: String,
}

pub struct TailResp {
    pub entries: Vec<Entry>,
}

// ── The follower side ───────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct FollowerState {
    pub fragment_epoch: u64,
    pub base: u64,
    pub end: u64,
    pub sealed_to: u64,
}

// A committed follower batch carries the state transition that its entries
// make durable. The SHA-256 footer distinguishes a complete fsynced batch from
// a crash-torn direct write, so the batch itself can be the recovery commit
// point without a second state-file fsync and rename.
//
//   batch: "CLB1" fragment_epoch base end sealed_to count entry* sha256
const COMMITTED_BATCH_MAGIC: &[u8; 4] = b"CLB1";
const COMMITTED_BATCH_DIGEST_LEN: usize = 32;

struct CommittedBatch {
    state: FollowerState,
    entries: Vec<Entry>,
}

fn encode_committed_batch(state: FollowerState, entries: &[Entry]) -> Vec<u8> {
    use sha2::Digest as _;

    let mut out = Vec::new();
    out.extend_from_slice(COMMITTED_BATCH_MAGIC);
    out.extend_from_slice(&state.fragment_epoch.to_le_bytes());
    out.extend_from_slice(&state.base.to_le_bytes());
    out.extend_from_slice(&state.end.to_le_bytes());
    out.extend_from_slice(&state.sealed_to.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        encode_entry(entry, &mut out);
    }
    let digest = sha2::Sha256::digest(&out);
    out.extend_from_slice(&digest);
    out
}

fn decode_committed_batch(bytes: &[u8]) -> anyhow::Result<CommittedBatch> {
    use sha2::Digest as _;

    let payload_len = bytes
        .len()
        .checked_sub(COMMITTED_BATCH_DIGEST_LEN)
        .ok_or_else(|| anyhow!("follower batch is shorter than its digest"))?;
    let (payload, stored_digest) = bytes.split_at(payload_len);
    let actual_digest = sha2::Sha256::digest(payload);
    if actual_digest.as_slice() != stored_digest {
        anyhow::bail!("follower batch digest mismatch");
    }

    let mut buf = payload;
    if take(&mut buf, 4, "follower batch magic")? != COMMITTED_BATCH_MAGIC {
        anyhow::bail!("bad follower batch magic");
    }
    let state = FollowerState {
        fragment_epoch: take_u64(&mut buf, "follower batch epoch")?,
        base: take_u64(&mut buf, "follower batch base")?,
        end: take_u64(&mut buf, "follower batch end")?,
        sealed_to: take_u64(&mut buf, "follower batch seal")?,
    };
    let entries = decode_entries(&mut buf, "follower batch")?;
    let Some(first) = entries.first() else {
        anyhow::bail!("follower batch has no entries");
    };
    let last = entries.last().expect("a non-empty batch has a last entry");
    if state.fragment_epoch == 0 || state.base > state.end || state.end != last.seq {
        anyhow::bail!("follower batch state does not cover its entries");
    }
    if entries
        .windows(2)
        .any(|pair| pair[1].seq != pair[0].seq.saturating_add(1))
    {
        anyhow::bail!("follower batch entries are not contiguous");
    }
    if first.seq <= state.base {
        anyhow::bail!("follower batch starts at or below its base");
    }
    Ok(CommittedBatch { state, entries })
}

fn follower_batch_range(name: &str, suffix: &str) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(suffix)?;
    let (first, last) = stem.split_once('-')?;
    Some((first.parse().ok()?, last.parse().ok()?))
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn sync_directory(path: &Path) -> std::io::Result<()> {
    let filesystem = celld_ltx::DirectFileSystem;
    celld_ltx::FileSystem::sync_all(&filesystem, path)
}

#[cfg(celld_internal_tests)]
type DirectorySyncForTest = dyn Fn(&Path) -> std::io::Result<()> + Send + Sync;

#[derive(Default)]
struct FollowerPersistTiming {
    write_us: u64,
    fsync_us: u64,
    rename_us: u64,
    directory_us: u64,
}

/// One node's store of the log fragments that it follows.
///
/// Each follower session uses `<root>/peerlog/<node>/<generation>/`. The store
/// persists the seal mark before it sends the response, so the mark survives a
/// follower restart. The store also reads the former flat layout during an
/// upgrade.
pub struct FollowerStore {
    root: PathBuf,
    filesystem: Arc<dyn celld_ltx::FileSystem>,
    bucket: Option<Arc<Bucket>>,
    node: String,
    logs: Mutex<HashMap<String, FollowerState>>,
    /// Per-leader mutual exclusion over the whole read-modify-write of a
    /// fragment. Without it, an append that loaded state before a seal
    /// persists writes the stale `sealed_to` back afterwards — the seal
    /// mark is atomic in the model and must be atomic here.
    guards: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Leaders whose directory chain this process has already fsynced to
    /// the data root. Ancestor directory entries only change when the
    /// leader directory is created, so re-syncing them on every state
    /// persist was one serial fsync chain per append — the write-latency
    /// floor the 2026-08-25 gate decomposition landed on. A restart
    /// starts empty and conservatively re-syncs each chain once.
    synced_namespaces: Mutex<std::collections::BTreeSet<String>>,
    #[cfg(celld_internal_tests)]
    directory_sync_for_test: Arc<DirectorySyncForTest>,
}

impl FollowerStore {
    pub fn new(root: &Path, bucket: Option<Arc<Bucket>>, node: &str) -> Self {
        let filesystem = crate::asyncrt::fs();
        #[cfg(celld_internal_tests)]
        let directory_filesystem = filesystem.clone();
        Self {
            root: root.join("peerlog"),
            filesystem,
            bucket,
            node: node.to_string(),
            logs: Mutex::new(HashMap::new()),
            guards: Mutex::new(HashMap::new()),
            synced_namespaces: Mutex::new(std::collections::BTreeSet::new()),
            #[cfg(celld_internal_tests)]
            directory_sync_for_test: Arc::new(move |path| directory_filesystem.sync_all(path)),
        }
    }

    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub fn new_with_directory_sync_for_test(
        root: &Path,
        bucket: Option<Arc<Bucket>>,
        node: &str,
        directory_sync: impl Fn(&Path) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> Self {
        let mut store = Self::new(root, bucket, node);
        store.directory_sync_for_test = Arc::new(directory_sync);
        store
    }

    /// A store over an injected filesystem, so a test can count every
    /// fsync the serve path issues — the directory sync included, which is
    /// why the directory seam is wired to the same filesystem here.
    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub fn new_with_filesystem_for_test(
        root: &Path,
        bucket: Option<Arc<Bucket>>,
        node: &str,
        filesystem: Arc<dyn celld_ltx::FileSystem>,
    ) -> Self {
        let mut store = Self::new(root, bucket, node);
        let directory_filesystem = filesystem.clone();
        store.filesystem = filesystem;
        store.directory_sync_for_test = Arc::new(move |path| directory_filesystem.sync_all(path));
        store
    }

    #[cfg(celld_internal_tests)]
    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        (self.directory_sync_for_test)(path)
    }

    #[cfg(not(celld_internal_tests))]
    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.filesystem.sync_all(path)
    }

    /// Barrier every directory entry from the session leaf through the data
    /// root. A follower session is `<node>/<generation>`, so the chain is
    /// four directories deep, and a leaf-only fsync leaves the two
    /// intermediate entries outside the barrier the acknowledgment claims.
    ///
    /// The chain is walked on every persist, not only when `create_dir_all`
    /// reports a fresh directory. A predecessor process can create the
    /// chain and die before its own barrier completes; this process would
    /// then find the directories present, skip the fsync, and acknowledge
    /// over a chain that was never durable. The price is four directory
    /// fsyncs for each persist — nine for a steady-state append, which
    /// persists twice for the truncate and syncs the leaf once more for the
    /// entry files.
    fn sync_namespace_to_data_root(&self, leaf: &Path) -> anyhow::Result<()> {
        let data_root = self
            .root
            .parent()
            .ok_or_else(|| anyhow!("peerlog root has no data-root parent"))?;
        for directory in leaf.ancestors() {
            self.sync_directory(directory)?;
            if directory == data_root {
                return Ok(());
            }
        }
        Err(anyhow!(
            "follower directory {} is outside data root {}",
            leaf.display(),
            data_root.display()
        ))
    }

    /// Make the leader directory durable, and the first time this process
    /// commits anything for `leader`, its ancestor chain as well. The leaf
    /// sync makes a rename or a new batch file durable on every commit; the
    /// ancestor chain only holds the directory's creation, so it is synced
    /// once per (process, leader) rather than on every append serve cycle,
    /// and a failed chain sync re-arms so the next commit retries it. A
    /// predecessor can create the chain and die before its own barrier
    /// completes, so a directory that already exists proves nothing. Both
    /// the state chain and the batch commit go through here: a batch
    /// acknowledged over a chain this process never synced would claim a
    /// durability its directory entries do not have.
    fn sync_leader_directory(&self, leader: &str, dir: &Path) -> anyhow::Result<()> {
        let first_commit = self
            .synced_namespaces
            .lock()
            .unwrap()
            .insert(leader.to_string());
        if first_commit {
            if let Err(error) = self.sync_namespace_to_data_root(dir) {
                self.synced_namespaces.lock().unwrap().remove(leader);
                return Err(error);
            }
            return Ok(());
        }
        self.sync_directory(dir)?;
        Ok(())
    }

    fn guard(&self, leader: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.guards
            .lock()
            .unwrap()
            .entry(leader.to_string())
            .or_default()
            .clone()
    }

    fn dir(&self, leader: &str) -> PathBuf {
        self.root.join(leader)
    }

    fn followed_sessions(&self) -> Vec<String> {
        let mut sessions = Vec::new();
        let Ok(nodes) = self.filesystem.read_dir(&self.root) else {
            return sessions;
        };
        for node in nodes.into_iter().filter(|item| item.is_dir) {
            let Some(node_name) = node.file_name.to_str().map(str::to_string) else {
                continue;
            };
            if self
                .filesystem
                .metadata(&node.path.join("state.json"))
                .is_ok_and(|metadata| metadata.is_file)
            {
                // Keep fragments written by the former flat leader identity
                // reachable during an upgrade.
                sessions.push(node_name.clone());
            }
            let Ok(generations) = self.filesystem.read_dir(&node.path) else {
                continue;
            };
            for generation in generations.into_iter().filter(|item| item.is_dir) {
                let Some(generation_name) = generation.file_name.to_str().map(str::to_string)
                else {
                    continue;
                };
                if self
                    .filesystem
                    .metadata(&generation.path.join("state.json"))
                    .is_ok_and(|metadata| metadata.is_file)
                {
                    sessions.push(format!("{node_name}/{generation_name}"));
                }
            }
        }
        sessions.sort();
        sessions.dedup();
        sessions
    }

    fn committed_batches(&self, leader: &str) -> Vec<CommittedBatch> {
        let dir = self.dir(leader);
        let Ok(read) = self.filesystem.read_dir(&dir) else {
            return Vec::new();
        };
        let mut batches = Vec::new();
        for item in read {
            let Some(name) = item.file_name.to_str() else {
                continue;
            };
            let Some((named_first, named_last)) = follower_batch_range(name, ".batch") else {
                continue;
            };
            let decoded = self
                .filesystem
                .read(&item.path)
                .map_err(anyhow::Error::from)
                .and_then(|bytes| decode_committed_batch(&bytes));
            match decoded {
                Ok(batch)
                    if batch.entries.first().map(|entry| entry.seq) == Some(named_first)
                        && batch.entries.last().map(|entry| entry.seq) == Some(named_last) =>
                {
                    batches.push(batch);
                }
                Ok(_) => warn!(
                    leader,
                    path = %item.path.display(),
                    "follower batch filename does not match its committed range"
                ),
                Err(error) => warn!(
                    leader,
                    path = %item.path.display(),
                    %error,
                    "invalid committed follower batch ignored"
                ),
            }
        }
        batches.sort_by_key(|batch| batch.entries.first().map_or(0, |entry| entry.seq));
        batches
    }

    /// Recover the newest checksum-valid state whose live suffix is complete.
    /// The durable state file proves its prefix. A later batch can advance the
    /// base past a missing prefix because its `base` is a bucket watermark;
    /// every sequence above that base must still be present without a gap.
    fn recover_committed_state(&self, leader: &str, seed: FollowerState) -> FollowerState {
        if seed.fragment_epoch == 0 {
            return seed;
        }
        let batches = self.committed_batches(leader);
        let mut candidates: Vec<&CommittedBatch> = batches
            .iter()
            .filter(|batch| {
                batch.state.fragment_epoch == seed.fragment_epoch && batch.state.end > seed.end
            })
            .collect();
        candidates.sort_by_key(|batch| std::cmp::Reverse(batch.state.end));
        for candidate in candidates {
            let mut covered = seed.end.max(candidate.state.base);
            for batch in batches
                .iter()
                .filter(|batch| batch.state.fragment_epoch == seed.fragment_epoch)
            {
                let first = batch
                    .entries
                    .first()
                    .expect("committed batches are non-empty")
                    .seq;
                let last = batch
                    .entries
                    .last()
                    .expect("committed batches are non-empty")
                    .seq;
                if last <= covered {
                    continue;
                }
                if first > covered.saturating_add(1) {
                    break;
                }
                covered = covered.max(last);
            }
            if covered >= candidate.state.end {
                return FollowerState {
                    fragment_epoch: seed.fragment_epoch,
                    base: seed.base.max(candidate.state.base),
                    end: candidate.state.end,
                    sealed_to: seed.sealed_to.max(candidate.state.sealed_to),
                };
            }
        }
        seed
    }

    #[doc(hidden)]
    pub fn load(&self, leader: &str) -> FollowerState {
        if let Some(state) = self.logs.lock().unwrap().get(leader) {
            return *state;
        }
        let seed = self
            .filesystem
            .read(&self.dir(leader).join("state.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        let state = self.recover_committed_state(leader, seed);
        self.logs.lock().unwrap().insert(leader.to_string(), state);
        state
    }

    #[doc(hidden)]
    pub fn persist(&self, leader: &str, state: FollowerState) -> anyhow::Result<()> {
        self.persist_timed(leader, state).map(|_| ())
    }

    fn persist_timed(
        &self,
        leader: &str,
        state: FollowerState,
    ) -> anyhow::Result<FollowerPersistTiming> {
        let dir = self.dir(leader);
        self.filesystem.create_dir_all(&dir)?;
        let path = dir.join("state.json");
        let tmp = dir.join("state.json.tmp");
        let write_started = mono_us();
        self.filesystem.write(&tmp, &serde_json::to_vec(&state)?)?;
        let write_us = mono_us().saturating_sub(write_started);
        let fsync_started = mono_us();
        self.filesystem.sync_all(&tmp)?;
        let fsync_us = mono_us().saturating_sub(fsync_started);
        let rename_started = mono_us();
        self.filesystem.rename(&tmp, &path)?;
        let rename_us = mono_us().saturating_sub(rename_started);
        let dir_started = mono_us();
        self.sync_leader_directory(leader, &dir)?;
        let directory_us = mono_us().saturating_sub(dir_started);
        self.logs.lock().unwrap().insert(leader.to_string(), state);
        Ok(FollowerPersistTiming {
            write_us,
            fsync_us,
            rename_us,
            directory_us,
        })
    }

    /// Adopt a new fragment epoch — only against the record. A leader's
    /// stream announcing an epoch is not authority: a fenced leader could
    /// invent one above our seal mark and talk past the fence, so the view
    /// change is verified against `log/<leader>.json` before any entry at
    /// the new epoch is accepted.
    async fn adopt(&self, leader: &str, epoch: u64, first_seq: u64) -> bool {
        let Some(bucket) = &self.bucket else {
            return false;
        };
        let record = match read_record(bucket, leader).await {
            Ok(Some(folded)) => folded.record,
            _ => return false,
        };
        let member = record.ensemble.contains(&self.node);
        let state = self.load(leader);
        if record.epoch != epoch
            || record.state != LogState::Open
            || !member
            || state.sealed_to >= epoch
            || first_seq != 1
        {
            return false;
        }
        // A new fragment has no durable records before its first append.
        // Starting above sequence 1 would let its seal response certify an
        // epoch whose missing prefix cannot be gathered during recovery.
        // Old-fragment entries are garbage the record no longer references.
        let _ = self.remove_entries_below(leader, u64::MAX);
        self.persist(
            leader,
            FollowerState {
                fragment_epoch: epoch,
                base: first_seq.saturating_sub(1),
                end: first_seq.saturating_sub(1),
                sealed_to: state.sealed_to,
            },
        )
        .is_ok()
    }

    fn remove_entries_below(&self, leader: &str, seq: u64) -> anyhow::Result<()> {
        let dir = self.dir(leader);
        let Ok(read) = self.filesystem.read_dir(&dir) else {
            return Ok(());
        };
        for item in read {
            let name = item.file_name;
            let Some(name) = name.to_str() else { continue };
            if let Some(stem) = name.strip_suffix(".entry") {
                if stem.parse::<u64>().is_ok_and(|s| s <= seq) {
                    let _ = self.filesystem.remove_file(&item.path);
                }
            } else if let Some(stem) = name.strip_suffix(".entries") {
                // A batch file goes only when its WHOLE range is behind the
                // watermark. A straddling batch keeps its covered entries:
                // truncation is space reclamation, not a correctness
                // boundary, so retaining them is free.
                if stem
                    .split_once('-')
                    .and_then(|(_, last)| last.parse::<u64>().ok())
                    .is_some_and(|last| last <= seq)
                {
                    let _ = self.filesystem.remove_file(&item.path);
                }
            } else if follower_batch_range(name, ".batch").is_some_and(|(_, last)| last <= seq) {
                let _ = self.filesystem.remove_file(&item.path);
            }
        }
        Ok(())
    }

    /// One frame through the batch commit: the stream and the one-shot
    /// `/peer/log/append` share a single implementation, so an answer
    /// means the same thing on both transports.
    pub async fn append(&self, req: AppendReq) -> AppendResp {
        let mut answers = self.append_batch(AppendBatch::new(req)).await;
        debug_assert_eq!(answers.len(), 1, "one frame in, one answer out");
        answers.pop().expect("append_batch answers every frame")
    }

    /// Commit a batch of consecutive frames of one fragment as ONE
    /// durability chain and answer every frame from the committed end.
    ///
    /// Each frame's answer is its own: confirmed when the committed end
    /// reaches its last sequence, refused otherwise — exactly what it
    /// would have received alone, because the contiguity rule accepts the
    /// batch's entries in the order the frames arrived and a refusal stops
    /// the batch where a lone frame would have been refused. The cost this
    /// removes is one fsync chain per frame; see `serve_log_stream`.
    pub async fn append_batch(&self, batch: AppendBatch) -> Vec<AppendResp> {
        fn refusals(count: usize, state: FollowerState) -> Vec<AppendResp> {
            (0..count)
                .map(|_| AppendResp {
                    ok: false,
                    end: state.end,
                    epoch: Some(state.fragment_epoch),
                })
                .collect()
        }
        let frames = batch.frames;
        let leader = frames[0].leader.clone();
        let epoch = frames[0].epoch;
        let total_entries: usize = frames.iter().map(|frame| frame.entries.len()).sum();
        let handled = mono_us();
        let guard = self.guard(&leader);
        let guard_started = mono_us();
        let _held = guard.lock().await;
        let guard_us = mono_us().saturating_sub(guard_started);
        let _emit = HandleTiming {
            handled,
            guard_us,
            entries: total_entries,
        };
        let mut state = self.load(&leader);
        if state.fragment_epoch != epoch {
            if total_entries == 0 {
                // An idle probe at an epoch we do not hold: adopting here
                // would fix the fragment base at zero and poison the next
                // real append, so refuse — the probe measured the guard
                // lock and nothing else.
                return refusals(frames.len(), state);
            }
            let first = frames
                .iter()
                .flat_map(|frame| frame.entries.first())
                .map(|entry| entry.seq)
                .next()
                .unwrap_or(0);
            if !self.adopt(&leader, epoch, first).await {
                // A refusal here degrades the leader to bucket acks; the
                // silent version cost the lab an unattributed 85 s window.
                warn!(
                    leader,
                    epoch,
                    held_epoch = state.fragment_epoch,
                    sealed_to = state.sealed_to,
                    "append refused: fragment adoption failed against the record"
                );
                return refusals(frames.len(), state);
            }
            state = self.load(&leader);
        }
        // The shipping decision is celld_logic::log_tier::FollowerLog; drive
        // it entry by entry so the seal and contiguity refusals are exactly
        // the modeled ones.
        let mut log = log_tier::FollowerLog {
            fragment_epoch: state.fragment_epoch,
            base: state.base,
            end: state.end,
            sealed_to: state.sealed_to,
        };
        let dir = self.dir(&leader);
        if self.filesystem.create_dir_all(&dir).is_err() {
            return refusals(frames.len(), state);
        }
        // The accepted prefix of every frame in the burst persists as ONE
        // checksum-protected commit file. It carries the entries and their
        // resulting follower state, so one file fsync plus the directory
        // barrier makes the whole transition recoverable — per burst, not
        // per frame. A crash-torn direct write has no valid digest and is
        // ignored; a later state-file rewrite is not on the hot append path.
        let persist_started = mono_ms();
        let mut entry_write_us = 0_u64;
        let mut entry_fsync_us = 0_u64;
        let mut entry_directory_us = 0_u64;
        // Each frame's answer needs its own last sequence after the frames
        // have given up their entries to the commit below.
        let frame_count = frames.len();
        let lasts: Vec<Option<u64>> = frames
            .iter()
            .map(|frame| frame.entries.last().map(|entry| entry.seq))
            .collect();
        let truncate_to = frames
            .iter()
            .map(|frame| frame.truncate_to)
            .max()
            .unwrap_or(0);
        let mut accepted: Vec<Entry> = Vec::new();
        'accept: for frame in frames {
            // A frame the fragment already covers confirms from the end
            // without reapplying — the resume rule's follower half. The
            // model refuses a covered sequence as a contiguity break, and
            // alone that refusal is harmless because the frame's answer is
            // read off the end anyway; in a batch it must not stop the
            // frames behind it, which a retransmission delivers in the same
            // burst as the duplicate. Only a real gap or seal stops the
            // batch, where it would have refused the lone frame too.
            if frame
                .entries
                .last()
                .is_none_or(|entry| entry.seq <= log.end)
            {
                continue;
            }
            for entry in frame.entries {
                if !log.accept_append(epoch, entry.seq) {
                    warn!(
                        leader,
                        epoch,
                        seq = entry.seq,
                        end = log.end,
                        sealed_to = log.sealed_to,
                        "append refused: seal or contiguity"
                    );
                    break 'accept;
                }
                accepted.push(entry);
            }
        }
        let mut new_state = FollowerState {
            fragment_epoch: log.fragment_epoch,
            base: log.base,
            end: log.end,
            sealed_to: log.sealed_to,
        };
        // The truncate folds into the commit file's state. The watermark is
        // monotone per lane, so the burst's newest value is what applying
        // each frame's in turn would leave — capped below the burst's first
        // accepted sequence, because `decode_committed_batch` rejects a
        // file whose first entry is at or below its base, and a later
        // frame's watermark can cover an earlier frame's range once the
        // bucket proves it mid-burst. The rest of the truncation arrives
        // with the next round's watermark.
        let truncate_to = match accepted.first() {
            Some(first) => truncate_to.min(first.seq.saturating_sub(1)),
            None => truncate_to,
        };
        if truncate_to > 0 {
            let mut truncated = log_tier::FollowerLog {
                fragment_epoch: new_state.fragment_epoch,
                base: new_state.base,
                end: new_state.end,
                sealed_to: new_state.sealed_to,
            };
            truncated.truncate(truncate_to.min(new_state.end));
            new_state.base = truncated.base;
        }

        let mut commit_ok = true;
        if !accepted.is_empty() {
            let first = accepted
                .first()
                .expect("an accepted batch has a first entry")
                .seq;
            let last = accepted
                .last()
                .expect("an accepted batch has a last entry")
                .seq;
            let path = dir.join(format!("{first}-{last}.batch"));
            let encoded = encode_committed_batch(new_state, &accepted);
            let write_started = mono_us();
            let write = self.filesystem.write(&path, &encoded);
            entry_write_us = mono_us().saturating_sub(write_started);
            let write = write.and_then(|()| {
                let fsync_started = mono_us();
                let result = self.filesystem.sync_all(&path);
                entry_fsync_us = mono_us().saturating_sub(fsync_started);
                result
            });
            if write.is_err() {
                commit_ok = false;
            } else {
                let directory_started = mono_us();
                if let Err(error) = self.sync_leader_directory(&leader, &dir) {
                    // The complete file can survive despite a failed barrier.
                    // Recovering it is safe because its own fsync finished; if
                    // its name disappears, the failed append was never acked.
                    warn!(
                        leader,
                        epoch,
                        end = state.end,
                        %error,
                        "append refused: batch directory sync failed"
                    );
                    commit_ok = false;
                }
                entry_directory_us = mono_us().saturating_sub(directory_started);
            }
        }
        let handle_ms = mono_us().saturating_sub(_emit.handled) / 1000;
        let persist_ms = mono_ms().saturating_sub(persist_started);
        let state_started = mono_ms();
        let state_persist = if !commit_ok {
            Err(anyhow!("the follower batch did not reach its commit point"))
        } else if !accepted.is_empty() {
            self.logs.lock().unwrap().insert(leader.clone(), new_state);
            // Reclaim committed ranges only after the new base is durable.
            // The deletion is best-effort and needs no second directory sync:
            // a crash can retain covered files, which `tail` already hides.
            let _ = self.remove_entries_below(&leader, new_state.base);
            Ok(FollowerPersistTiming::default())
        } else if new_state != state {
            // A duplicate append can still advance only the bucket watermark.
            // With no new batch to carry it, retain the ordinary state chain.
            self.persist_timed(&leader, new_state)
        } else {
            Ok(FollowerPersistTiming::default())
        };
        let state_ok = state_persist.is_ok();
        let state_timing = state_persist.unwrap_or_default();
        info!(
            event = "log_append_serve",
            leader,
            frames = frame_count,
            entries = total_entries,
            guard_ms = _emit.guard_us / 1000,
            handle_ms,
            persist_ms,
            state_ms = mono_ms().saturating_sub(state_started),
            entry_write_us,
            entry_fsync_us,
            entry_directory_us,
            state_write_us = state_timing.write_us,
            state_fsync_us = state_timing.fsync_us,
            state_rename_us = state_timing.rename_us,
            state_directory_us = state_timing.directory_us,
            "follower persisted an append batch"
        );
        if !state_ok {
            return refusals(frame_count, state);
        }
        lasts
            .into_iter()
            .map(|last| {
                let last = last.unwrap_or(new_state.end);
                AppendResp {
                    ok: new_state.end >= last,
                    end: new_state.end,
                    epoch: Some(new_state.fragment_epoch),
                }
            })
            .collect()
    }

    /// Persist the seal mark BEFORE answering: once the response leaves,
    /// this follower must refuse the sealed epoch forever, including across
    /// a restart.
    pub async fn seal(&self, req: &SealReq) -> anyhow::Result<SealResp> {
        let guard = self.guard(&req.leader);
        let _held = guard.lock().await;
        let state = self.load(&req.leader);
        let mut log = log_tier::FollowerLog {
            fragment_epoch: state.fragment_epoch,
            base: state.base,
            end: state.end,
            sealed_to: state.sealed_to,
        };
        let end = log.seal(req.epoch);
        self.persist(
            &req.leader,
            FollowerState {
                sealed_to: log.sealed_to,
                ..state
            },
        )?;
        // `fragment_epoch` is the field that older recovery callers already
        // use as their certificate. Inspect the frozen tail before returning
        // it, so a torn current fragment fails closed even when the caller
        // ignores the newer range fields. The actual epoch travels
        // separately so a new caller can still gather the readable prefix
        // and classify the incomplete fragment conclusively.
        let tail = self.tail(&TailReq {
            leader: req.leader.clone(),
        });
        let retained_complete = tail_covers_sealed_range(state.base, end, &tail.entries);
        if !retained_complete {
            warn!(
                leader = req.leader,
                fragment_epoch = state.fragment_epoch,
                base = state.base,
                end,
                "sealed follower cannot certify its incomplete retained range"
            );
        }
        Ok(SealResp {
            end,
            base: Some(state.base),
            fragment_epoch: if retained_complete {
                state.fragment_epoch
            } else {
                0
            },
            held_fragment_epoch: Some(state.fragment_epoch),
        })
    }

    /// One fragment-GC pass over every leader this node follows. A
    /// fragment is garbage when its epoch is closed: the record moved past
    /// it (reconfiguration or a reopened incarnation force-tiered or
    /// recovered it away), or the record at its epoch is Sealed (recovery
    /// certified and uploaded the tail, so this copy is redundant by
    /// write-all). The deletion runs under the per-leader guard, keeps the
    /// state file, and extends the seal mark over the closed epoch, so a
    /// straggling append at it is refused rather than resurrected.
    pub async fn gc_fragments(&self) {
        let Some(bucket) = &self.bucket else { return };
        let leaders = self.followed_sessions();
        for leader in leaders {
            let Ok(Some(folded)) = read_record(bucket, &leader).await else {
                continue;
            };
            let record = folded.record;
            let guard = self.guard(&leader);
            let _held = guard.lock().await;
            let state = self.load(&leader);
            if state.fragment_epoch == 0 {
                continue;
            }
            let closed = log_tier::fragment_closed(&record, state.fragment_epoch);
            if !closed {
                continue;
            }
            if state.base == state.end
                && self
                    .filesystem
                    .metadata(&self.dir(&leader).join("state.json"))
                    .is_ok_and(|metadata| metadata.is_file)
            {
                // Already empty; nothing to remove, and the state file
                // stays as the seal-mark carrier.
                let _ = self.persist(
                    &leader,
                    FollowerState {
                        sealed_to: state.sealed_to.max(state.fragment_epoch),
                        ..state
                    },
                );
                continue;
            }
            let _ = self.remove_entries_below(&leader, u64::MAX);
            if self
                .persist(
                    &leader,
                    FollowerState {
                        fragment_epoch: state.fragment_epoch,
                        base: state.end,
                        end: state.end,
                        sealed_to: state.sealed_to.max(state.fragment_epoch),
                    },
                )
                .is_ok()
            {
                info!(
                    leader,
                    epoch = state.fragment_epoch,
                    "fragment GC: closed epoch's fragments removed"
                );
            }
        }
    }

    pub fn tail(&self, req: &TailReq) -> TailResp {
        // Entries above the persisted end are unacked debris a crash may
        // legitimately tear (the entry syncs before the state does), and
        // including or losing an unacked frame is free. A torn entry ABOVE
        // the base and AT OR BELOW the end would be an acked frame's only
        // local copy, so it is skipped LOUDLY — write-all means another
        // member still has it, and the line attributes the anomaly.
        let state = self.load(&req.leader);
        let base = state.base;
        let end = state.end;
        let mut entries: Vec<Entry> = self
            .committed_batches(&req.leader)
            .into_iter()
            .filter(|batch| batch.state.fragment_epoch == state.fragment_epoch)
            .flat_map(|batch| batch.entries)
            .filter(|entry| entry.seq > base)
            .collect();
        let dir = self.dir(&req.leader);
        if let Ok(read) = self.filesystem.read_dir(&dir) {
            for item in read {
                let name = item.file_name;
                let Some(name) = name.to_str() else { continue };
                if let Some(stem) = name.strip_suffix(".entry") {
                    if let Ok(bytes) = self.filesystem.read(&item.path) {
                        match decode_entry(&mut bytes.as_slice()) {
                            Ok(entry) => {
                                if entry.seq > base {
                                    entries.push(entry);
                                }
                            }
                            Err(error) => {
                                let torn_acked = stem
                                    .parse::<u64>()
                                    .is_ok_and(|seq| seq > base && seq <= end);
                                if torn_acked {
                                    warn!(
                                        leader = req.leader,
                                        path = %item.path.display(),
                                        %error,
                                        "torn entry at or below the fragment end skipped in tail"
                                    );
                                }
                            }
                        }
                    }
                } else if let Some(stem) = name.strip_suffix(".entries") {
                    let Ok(bytes) = self.filesystem.read(&item.path) else {
                        continue;
                    };
                    // Decode the batch until the tear, keeping every intact
                    // entry. The first undecoded sequence starts at the
                    // filename's range and follows the last decoded entry,
                    // so the acked-tear classification is exact.
                    let mut next = stem
                        .split_once('-')
                        .and_then(|(first, _)| first.parse::<u64>().ok());
                    let mut buf = bytes.as_slice();
                    while !buf.is_empty() {
                        match decode_entry(&mut buf) {
                            Ok(entry) => {
                                next = Some(entry.seq + 1);
                                if entry.seq > base {
                                    entries.push(entry);
                                }
                            }
                            Err(error) => {
                                if next.is_some_and(|seq| seq > base && seq <= end) {
                                    warn!(
                                        leader = req.leader,
                                        path = %item.path.display(),
                                        %error,
                                        "torn batch at or below the fragment end skipped in tail"
                                    );
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
        entries.sort_by_key(|entry| entry.seq);
        // Batch files can overlap: a torn batch leaves its file above the
        // persisted end, and the retransmission of its unanswered frames
        // commits again under whatever batch boundaries the socket had
        // then. One fragment sequence is one set of bytes — a leader
        // assigns each sequence once per epoch and adoption clears an old
        // epoch's files — so a repeated sequence is the same entry twice,
        // never a conflict.
        entries.dedup_by_key(|entry| entry.seq);
        TailResp { entries }
    }
}

fn tail_covers_sealed_range(base: u64, end: u64, entries: &[Entry]) -> bool {
    if base > end {
        return false;
    }
    // `(base, end]` is empty here. `tail` can still expose an entry above
    // `end` because an entry-file sync precedes the state commit, but that
    // unacknowledged debris says nothing about this retained range.
    if base == end {
        return true;
    }
    let mut covered = base;
    for entry in entries {
        if entry.seq <= covered {
            continue;
        }
        if covered.checked_add(1) != Some(entry.seq) {
            return false;
        }
        covered = entry.seq;
        if covered == end {
            return true;
        }
    }
    covered == end
}

/// The signed production implementation. It preserves the previous request
/// construction, authentication, response validation, and timeout behavior.
struct SignedPeerTransport {
    http: reqwest::Client,
    auth: Arc<PeerAuth>,
}

impl SignedPeerTransport {
    fn new(auth: Arc<PeerAuth>) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .tcp_nodelay(true)
                .build()
                .expect("build log peer client"),
            auth,
        }
    }
}

impl LogTransport for SignedPeerTransport {
    fn post<'a>(
        &'a self,
        node: &'a str,
        addr: &'a str,
        path: &'a str,
        body: Vec<u8>,
        deadline: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut builder = self.http.post(format!("http://{addr}{path}"));
            if let Some(deadline) = deadline {
                builder = builder.timeout(deadline);
            }
            let request = self.auth.sign(builder, "POST", path, &body, node)?;
            let response = request.body(body).send().await?;
            // Status before response-auth: an older peer can answer an
            // unsigned route error, and callers need that status to classify
            // a protocol-incapable member separately from transport failure.
            if !response.status().is_success() {
                let status = response.status();
                return Err(anyhow::Error::new(PeerHttpError { status })
                    .context(format!("peer {node} answered {status}")));
            }
            crate::peer_auth::validate_response(response.headers())?;
            Ok(response.bytes().await?)
        })
    }

    fn open_stream<'a>(
        &'a self,
        node: &'a str,
        addr: &'a str,
        path: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<LogStreamIo>> + Send + 'a>>
    {
        Box::pin(async move {
            // A dedicated client: the shared one carries a 10 s whole-request
            // timeout that would sever a healthy long-lived stream. The
            // handshake is authenticated both ways — the request by the
            // peer HMAC, the 101 by the response validation — and the
            // upgraded byte channel inherits that establishment, which is
            // the same trust shape as every other authenticated connection.
            let client = reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .tcp_nodelay(true)
                .build()?;
            let builder = client
                .post(format!("http://{addr}{path}"))
                .header(hyper::header::CONNECTION, "upgrade")
                .header(hyper::header::UPGRADE, "celld-log-stream");
            let request = self.auth.sign(builder, "POST", path, &[], node)?;
            let response = request.send().await?;
            if response.status() != reqwest::StatusCode::SWITCHING_PROTOCOLS {
                let status = response.status();
                return Err(anyhow::Error::new(PeerHttpError { status })
                    .context(format!("peer {node} refused the stream: {status}")));
            }
            crate::peer_auth::validate_response(response.headers())?;
            Ok(Box::new(response.upgrade().await?) as LogStreamIo)
        })
    }
}

/// A peer answered with an HTTP error status: typed so callers can tell
/// "the route is not there" from transport silence.
#[derive(Debug)]
struct PeerHttpError {
    status: reqwest::StatusCode,
}

impl std::fmt::Display for PeerHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "peer answered {}", self.status)
    }
}

impl std::error::Error for PeerHttpError {}

// ── The leader side ─────────────────────────────────────────────────────────

#[derive(Clone)]
#[doc(hidden)]
pub struct Member {
    pub node: String,
    pub addr: String,
}

/// One round's work for one member lane: the shared request and the slot
/// its answer lands in.
#[doc(hidden)]
pub struct LaneJob {
    pub req: Arc<AppendReq>,
    /// Submission time, for the lane-wait split in the rtt event.
    pub enqueued: u64,
    /// The request's entry bytes, released from the stream window when the
    /// member answers.
    pub bytes: u64,
    pub resp: tokio::sync::oneshot::Sender<Option<(String, u64)>>,
}

/// One outstanding round, counted from submission until its completion is
/// applied or discarded. The completion owns this guard after the network
/// future resolves, so cancellation releases every reservation exactly once.
/// Follower-side handler timing carrier for the serve event.
struct HandleTiming {
    handled: u64,
    guard_us: u64,
    #[allow(dead_code)]
    entries: usize,
}

struct OutstandingRound(Arc<std::sync::atomic::AtomicU64>);

impl Drop for OutstandingRound {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The fleet shipper for ONE ensemble at one epoch: assigns sequence
/// numbers, POSTs pipelined batches to every member, and reports all-acked.
/// Each member's ordered lane keeps its fragment contiguous. Membership and
/// epoch are immutable, so an ensemble change builds and swaps a new shipper.
/// The stream window (the stream-window design): per-member
/// append accounting and the fleet watermark. Each ordered lane keeps the
/// contiguous acked end its follower reports, the watermark is the minimum
/// across members, and admission is bounded per lane by appends and bytes,
/// so a slow member becomes bounded backpressure instead of unbounded
/// queueing. `CELLD_LOG_WINDOW=0` (the default) disables the stream and
/// the ship loop keeps today's round bound.
#[doc(hidden)]
pub struct StreamWindow {
    /// Appends one lane may hold in flight.
    window: u64,
    /// Request bytes one lane may hold in flight.
    byte_cap: u64,
    lanes: Vec<StreamLane>,
    /// The fleet watermark: the minimum contiguous acked end across
    /// members. Monotone by `fetch_max`; a concurrent stale minimum can
    /// only be conservative, never ahead.
    watermark: std::sync::atomic::AtomicU64,
}

struct StreamLane {
    inflight: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
    /// This member's contiguous acked end. Monotone because the lane is
    /// one ordered worker and `append_confirms` only accepts an end that
    /// covers the batch at the leader's fragment epoch.
    acked: std::sync::atomic::AtomicU64,
}

impl StreamWindow {
    #[doc(hidden)]
    pub fn new(window: u64, byte_cap: u64, members: usize) -> Self {
        Self {
            window,
            byte_cap,
            lanes: (0..members)
                .map(|_| StreamLane {
                    inflight: std::sync::atomic::AtomicU64::new(0),
                    bytes: std::sync::atomic::AtomicU64::new(0),
                    acked: std::sync::atomic::AtomicU64::new(0),
                })
                .collect(),
            watermark: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// True while every lane has room for one more append. The slowest
    /// member closes the window; its lane refilling is what reopens it.
    #[doc(hidden)]
    pub fn admit(&self) -> bool {
        self.lanes.iter().all(|lane| {
            lane.inflight.load(Ordering::SeqCst) < self.window
                && lane.bytes.load(Ordering::SeqCst) < self.byte_cap
        })
    }

    #[doc(hidden)]
    pub fn submitted(&self, lane: usize, bytes: u64) {
        self.lanes[lane].inflight.fetch_add(1, Ordering::SeqCst);
        self.lanes[lane].bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    #[doc(hidden)]
    pub fn answered(&self, lane: usize, bytes: u64) {
        self.lanes[lane].inflight.fetch_sub(1, Ordering::SeqCst);
        self.lanes[lane].bytes.fetch_sub(bytes, Ordering::SeqCst);
    }

    /// A confirmed append: advance the lane's acked end to what its
    /// follower reported and recompute the watermark. Returns the new
    /// watermark when it moved. A refusal or failure never reaches here,
    /// so a lagging or refusing member pins the watermark in place.
    #[doc(hidden)]
    pub fn confirmed(&self, lane: usize, end: u64) -> Option<u64> {
        self.lanes[lane].acked.fetch_max(end, Ordering::SeqCst);
        let minimum = self
            .lanes
            .iter()
            .map(|lane| lane.acked.load(Ordering::SeqCst))
            .min()
            .unwrap_or(0);
        let prior = self.watermark.fetch_max(minimum, Ordering::SeqCst);
        (minimum > prior).then_some(minimum)
    }

    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub fn watermark(&self) -> u64 {
        self.watermark.load(Ordering::SeqCst)
    }
}

/// The fleet shipper for ONE ensemble at one epoch: assigns sequence
/// numbers, POSTs one in-flight batch to every member, and reports
/// all-acked. One batch at a time is what keeps each follower's fragment
/// contiguous. Membership and epoch are immutable — an ensemble change
/// builds a new shipper and the manager swaps it.
pub struct FleetShipper {
    node: String,
    transport: Arc<dyn LogTransport>,
    live_log: Arc<LiveLogTransitions>,
    record: log_tier::LogRecord,
    /// True only after the authoritative record applies `active` for this
    /// ensemble. The first eligible batch publishes `active` before its
    /// acknowledgements are credited; a failed publication permanently
    /// degrades the shipper. Recovery uses `active` to tell a never-adopted
    /// fragment (safe to seal empty) from an all-amnesiac ensemble (refuse the
    /// silent seal).
    activated: Arc<std::sync::atomic::AtomicBool>,
    epoch: u64,
    members: Vec<Member>,
    /// One ordered lane per member: a single worker serves that member's
    /// rounds strictly in submission order, so pipelining across rounds
    /// can never reorder appends at a follower — contiguity holds by
    /// construction and the follower needs no reorder state.
    lanes: Vec<tokio::sync::mpsc::UnboundedSender<LaneJob>>,
    /// Rounds the ship loop may keep in flight at once.
    pipeline: usize,
    seq: std::sync::atomic::AtomicU64,
    /// A failed member degrades the shipper permanently: fleet proofs stop
    /// and every ack rides the bucket, which is always safe, until the
    /// maintenance loop CASes a fresh ensemble.
    degraded: Arc<std::sync::atomic::AtomicBool>,
    /// Batches between capture and credit, one count per round. The
    /// reconfigure barrier cannot see such a batch in the shipped/tiered
    /// counters — its frames credit only after the round completes — so
    /// `maintain` must refuse to step epochs while any is out, or a frame
    /// covered only by the old ensemble could ack under a record that no
    /// longer names its holders.
    outstanding: Arc<std::sync::atomic::AtomicU64>,
    /// The gray-follower ledger lives with the member lanes now: append
    /// timings feed it from each lane worker, and it outlives this shipper
    /// so quarantine survives the swap.
    policy: Arc<celld_logic::log_evict::EvictionPolicy>,
    /// The stream window, when `CELLD_LOG_WINDOW` enables it. `None` keeps
    /// the round-bounded ship loop unchanged.
    stream: Option<Arc<StreamWindow>>,
}

impl crate::ltx_repl::Shipper for FleetShipper {
    fn ship(
        &self,
        batch: Vec<ShipEntry>,
        covered_seq: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::ltx_repl::ShipCompletion> + Send + 'static>,
    > {
        self.ship_batch(batch, covered_seq)
    }

    fn active(&self) -> bool {
        self.is_active()
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn pipeline(&self) -> usize {
        self.pipeline
    }

    fn admit(&self) -> bool {
        self.stream.as_ref().is_none_or(|stream| stream.admit())
    }
}

impl FleetShipper {
    fn degrade(&self, why: &str) {
        degrade_shared(&self.degraded, self.epoch, why);
    }

    async fn post_append(&self, member: &Member, req: &AppendReq) -> AppendSend {
        post_append_to(&self.transport, &self.policy, member, req).await
    }

    fn is_active(&self) -> bool {
        !self.degraded.load(Ordering::SeqCst)
    }
}

/// One member's answer to one append POST, classified for the health
/// ledger: a parsed response, a protocol-level incapability (route
/// missing or body unparseable — quarantine), or a transient transport
/// failure (retry).
enum AppendSend {
    Answered(AppendResp),
    Incapable(anyhow::Error),
    Failed(#[allow(dead_code)] anyhow::Error),
}

/// Whether one member's answer confirms this append. Direct acceptance
/// confirms; a refusal still confirms when the follower echoes the same
/// fragment epoch and its contiguous end covers the batch — the entries are
/// durable there, which is all write-all needs, and it is exactly the shape
/// a hedged retransmission produces when the original landed first. The
/// epoch echo is what makes the refusal reading sound: a follower on
/// another fragment reports an end that says nothing about these sequences.
#[doc(hidden)]
pub fn append_confirms(req_epoch: u64, last: u64, resp: &AppendResp) -> bool {
    resp.ok || (resp.epoch == Some(req_epoch) && resp.end >= last)
}

/// Whether one ATTEMPT confirms this append. Only a well-formed answer can:
/// a transport failure and a protocol-level incapability report on the
/// attempt, not on what the follower holds.
fn send_confirms(req_epoch: u64, last: u64, send: &AppendSend) -> bool {
    matches!(send, AppendSend::Answered(resp) if append_confirms(req_epoch, last, resp))
}

fn degrade_shared(degraded: &std::sync::atomic::AtomicBool, epoch: u64, why: &str) {
    if !degraded.swap(true, Ordering::SeqCst) {
        warn!(epoch, why, "log ensemble degraded; acks ride the bucket");
    }
}

/// The append POST: a binary body (the entries dominate it), a JSON
/// response. Bounded by the eviction backstop, not the generic client
/// timeout: an append slower than the backstop triggers the evict
/// regardless, and a gray follower must not pin the in-flight batch
/// (and with it the reconfigure barrier) for ten seconds.
async fn post_append_to(
    transport: &Arc<dyn LogTransport>,
    policy: &celld_logic::log_evict::EvictionPolicy,
    member: &Member,
    req: &AppendReq,
) -> AppendSend {
    let deadline = std::time::Duration::from_millis(policy.backstop_ms + 100);
    let encode_started = mono_us();
    let body = encode_append(req);
    let encode_us = mono_us().saturating_sub(encode_started);
    let body_len = body.len();
    let bytes = match transport
        .post(
            &member.node,
            &member.addr,
            "/peer/log/append",
            body,
            Some(deadline),
        )
        .await
    {
        Ok(bytes) => bytes,
        // A missing or unimplemented route is a binary that does not
        // speak the log tier — the mixed-version seam. Everything
        // else (timeouts, resets, 5xx) stays a transient failure.
        Err(error) => {
            let incapable = error
                .downcast_ref::<PeerHttpError>()
                .is_some_and(|http| matches!(http.status.as_u16(), 404 | 405 | 501));
            return if incapable {
                AppendSend::Incapable(error)
            } else {
                AppendSend::Failed(error)
            };
        }
    };
    tracing::debug!(
        target: "timing",
        event = "log_append_encode",
        encode_us,
        body_len,
        "append body encoded"
    );
    match serde_json::from_slice(&bytes) {
        Ok(resp) => AppendSend::Answered(resp),
        // A 200 whose body is not an AppendResp is no celld follower.
        Err(error) => AppendSend::Incapable(error.into()),
    }
}

/// How the lane arms its hedge deadline. `Fixed` is the lab override
/// (`CELLD_LOG_HEDGE_MS`; 0 disables hedging); `Adaptive` derives the
/// deadline from the follower-health ledger per arming, so the deadline
/// tracks the fleet's measured honest tail instead of a constant that a
/// loaded fleet's tail can cross (the log-hedge-deadline design).
#[derive(Clone, Copy)]
#[doc(hidden)]
pub enum HedgeMode {
    Fixed(u64),
    Adaptive,
}

fn resolve_hedge_ms(
    tuning: &LaneTuning,
    policy: &celld_logic::log_evict::EvictionPolicy,
    health: &Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
) -> u64 {
    match tuning.hedge {
        HedgeMode::Fixed(ms) => ms,
        HedgeMode::Adaptive => health.lock().unwrap().hedge_deadline_ms(policy, mono_ms()),
    }
}

/// The per-lane knobs that ride beside the transport handles: the hedge
/// deadline, this lane's slot in the stream window when the window is on,
/// and whether the lane speaks the ordered stream transport.
#[doc(hidden)]
pub struct LaneTuning {
    pub hedge: HedgeMode,
    pub stream: Option<(Arc<StreamWindow>, usize)>,
    pub stream_transport: bool,
}

/// Resolve one append's answer: the health sample, the rtt event, the
/// confirmation reading, the window accounting, and the caller's oneshot.
/// Shared by the serial lane, the stream lane, and the stream lane's
/// hedge and fallback paths, so every transport reads an answer the same
/// way.
#[allow(clippy::too_many_arguments)]
fn resolve_append(
    member: &Member,
    policy: &celld_logic::log_evict::EvictionPolicy,
    health: &Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
    tuning: &LaneTuning,
    job: LaneJob,
    started: u64,
    write_ms: u64,
    resp: AppendSend,
) {
    let done = mono_ms();
    health
        .lock()
        .unwrap()
        .append_completed(&member.node, done, done.saturating_sub(started));
    info!(
        event = "log_append_rtt",
        member = member.node,
        entries = job.req.entries.len(),
        lane_wait_ms = started.saturating_sub(job.enqueued),
        rtt_ms = done.saturating_sub(started),
        write_ms,
        "append round trip completed"
    );
    let last = job.req.entries.last().map_or(0, |entry| entry.seq);
    let outcome = match resp {
        AppendSend::Answered(resp) if append_confirms(job.req.epoch, last, &resp) => {
            Some((member.node.clone(), resp.end))
        }
        AppendSend::Incapable(error) => {
            // Fast rejections read as healthy latency samples, so
            // the gray verdicts never fire on this member and the
            // rebuild re-picks it forever (#95): quarantine it
            // here and the next rebuild recruits around it.
            warn!(
                member = member.node,
                %error,
                "follower cannot serve log appends; quarantined from recruitment"
            );
            health
                .lock()
                .unwrap()
                .append_incapable(policy, &member.node, done);
            None
        }
        AppendSend::Answered(_) | AppendSend::Failed(_) => None,
    };
    // The window accounting settles BEFORE the round future can
    // resolve: by the time the ship loop sees a completed round, every
    // one of its lane slots is already released.
    if let Some((window, index)) = &tuning.stream {
        window.answered(*index, job.bytes);
        if let Some((_, end)) = &outcome {
            if let Some(watermark) = window.confirmed(*index, *end) {
                info!(
                    event = "log_watermark_advance",
                    watermark,
                    member = member.node,
                    "fleet watermark advanced"
                );
            }
        }
    }
    let _ = job.resp.send(outcome);
}

/// One job through the one-shot HTTP path, hedged: the serial lane's whole
/// step, also the stream lane's fallback for an old follower.
async fn http_append_job(
    member: &Member,
    transport: &Arc<dyn LogTransport>,
    policy: &celld_logic::log_evict::EvictionPolicy,
    health: &Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
    tuning: &LaneTuning,
    job: LaneJob,
) {
    let started = mono_ms();
    health.lock().unwrap().append_started(&member.node, started);
    let last = job.req.entries.last().map_or(0, |entry| entry.seq);
    // The hedge: appends are idempotent per sequence — a duplicate of a
    // persisted range refuses with an end that covers it, which
    // `append_confirms` reads as the ack — so a round trip stuck in a
    // platform tail is raced by a second copy. This is what keeps one
    // slow append from stalling its round and every round behind it
    // (#140).
    //
    // The race is for the first CONFIRMING answer, never for the first
    // answer: a duplicate may only ever turn a non-confirmation into a
    // confirmation, so in every other case the round's verdict and the
    // health-ledger classification are the ORIGINAL attempt's, exactly
    // as if no duplicate had been sent. Returning whichever attempt
    // merely resolved first instead lets a hedge that fails fast cancel
    // a healthy original — failing a round the original was about to
    // confirm, degrading the whole shipper onto bucket acks for a tail
    // it had already absorbed, and quarantining a live member for
    // `quarantine_ms` when the duplicate's answer is an `Incapable` one.
    // The lane therefore waits the loser out whenever the winner does
    // not confirm. That wait is bounded and small: both attempts carry
    // the same eviction-backstop deadline, so the lane holds a job for
    // at most `hedge_ms` longer than the un-hedged path did.
    let hedge_ms = resolve_hedge_ms(tuning, policy, health);
    let resp = if hedge_ms == 0 {
        post_append_to(transport, policy, member, &job.req).await
    } else {
        let mut original = std::pin::pin!(post_append_to(transport, policy, member, &job.req));
        let inside_deadline = crate::asyncrt::select_biased! {
            "the original response wins a tie with the hedge deadline so no duplicate is sent";
            resp = &mut original => Some(resp),
            _ = crate::asyncrt::sleep(std::time::Duration::from_millis(hedge_ms)) => None,
        };
        match inside_deadline {
            // Answered before the deadline: no duplicate is sent, and
            // the answer stands whether or not it confirms.
            Some(resp) => resp,
            None => {
                info!(
                    event = "log_append_hedge",
                    member = member.node,
                    entries = job.req.entries.len(),
                    hedge_ms,
                    "append hedged after the deadline"
                );
                let mut hedged =
                    std::pin::pin!(post_append_to(transport, policy, member, &job.req));
                let (winner, winner_is_original) = crate::asyncrt::select_biased! {
                    "the original attempt wins simultaneous replies so it retains the verdict";
                    resp = &mut original => (resp, true),
                    resp = &mut hedged => (resp, false),
                };
                if send_confirms(job.req.epoch, last, &winner) {
                    winner
                } else if winner_is_original {
                    // Give the duplicate the rest of its deadline, and
                    // take it only to upgrade a non-confirmation.
                    let late = hedged.await;
                    if send_confirms(job.req.epoch, last, &late) {
                        late
                    } else {
                        winner
                    }
                } else {
                    // The duplicate answered first and did not confirm:
                    // whatever it said, the original owns the verdict.
                    original.await
                }
            }
        }
    };
    resolve_append(member, policy, health, tuning, job, started, 0, resp);
}

/// One member's ordered append lane. A single worker serves the member's
/// rounds strictly in submission order, so several rounds in flight can
/// never reorder appends at this follower; the worker exits when its
/// shipper is dropped and the channel closes. With the stream transport
/// the same worker pumps one long-lived duplex instead of awaiting each
/// request round trip.
#[doc(hidden)]
pub async fn member_lane(
    member: Member,
    transport: Arc<dyn LogTransport>,
    policy: Arc<celld_logic::log_evict::EvictionPolicy>,
    health: Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
    tuning: LaneTuning,
    mut jobs: tokio::sync::mpsc::UnboundedReceiver<LaneJob>,
    stop: crate::ltx_repl::StopToken,
) {
    if tuning.stream_transport {
        stream_lane(
            &member, &transport, &policy, &health, &tuning, &mut jobs, &stop,
        )
        .await;
        return;
    }
    loop {
        let job = crate::asyncrt::select_biased! {
            "a stop signal that ties queued work prevents the lane from taking another job";
            _ = stop.stopped() => break,
            job = jobs.recv() => job,
        };
        let Some(job) = job else { break };
        let work = http_append_job(&member, &transport, &policy, &health, &tuning, job);
        tokio::pin!(work);
        crate::asyncrt::select_biased! {
            "a stop signal that ties one-shot completion ends the lane before more work";
            _ = stop.stopped() => break,
            _ = &mut work => {},
        }
    }
}

/// One frame the stream lane has written and not yet matched to a
/// response. `job` empties when the hedge answers first; the entry then
/// only holds the FIFO slot for the stream's late duplicate answer.
struct StreamInFlight {
    job: Option<LaneJob>,
    frame: Vec<u8>,
    /// When the frame was written — queue residency starts here.
    started: u64,
    /// How long the socket write itself blocked the lane worker. Nonzero
    /// values mean TCP backpressure paces the lane; ~zero means the lane
    /// waits on the follower's serve cycle, not on the wire.
    write_ms: u64,
    /// The head-stall event fired for this frame (once per frame).
    stall_reported: bool,
    /// When the frame became the pipeline head — the follower's service
    /// clock. The health ledger and the hedge run on THIS, not on the
    /// write time: with W frames queued, write-to-resolve includes up to
    /// W round trips of residency, and a ledger built for one-in-flight
    /// lanes reads that as grayness and evicts a healthy member (the
    /// 2026-08-21 stall attribution).
    head_started: Option<u64>,
    hedged: bool,
}

#[derive(Clone, Copy)]
enum StreamProgress {
    AwaitingFirstResponse,
    ReceivedResponse,
}

impl StreamProgress {
    fn after_response(self) -> Self {
        match self {
            Self::AwaitingFirstResponse => Self::ReceivedResponse,
            Self::ReceivedResponse => self,
        }
    }

    fn permits_replay(self) -> bool {
        matches!(self, Self::ReceivedResponse)
    }
}

/// The progress recovered by consuming and fully retiring one connection.
/// Replay code cannot carry progress away from a live connection.
struct RetiredStream {
    progress: StreamProgress,
}

impl RetiredStream {
    fn permits_replay(&self) -> bool {
        self.progress.permits_replay()
    }
}

/// One physical append stream, its reader, and the progress observed on that
/// stream. A new connection always starts without progress. Only a response
/// matched to a frame advances its state, so a one-shot answer cannot permit a
/// replay on a silent stream. Dropping `responses` closes the reader's sender;
/// `retire` then joins the reader before a replacement stream can start.
struct StreamConnection {
    write: tokio::io::WriteHalf<LogStreamIo>,
    responses: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    reader: crate::asyncrt::TaskHandle<()>,
    progress: StreamProgress,
}

impl StreamConnection {
    fn new(
        write: tokio::io::WriteHalf<LogStreamIo>,
        responses: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        reader: crate::asyncrt::TaskHandle<()>,
    ) -> Self {
        Self {
            write,
            responses,
            reader,
            progress: StreamProgress::AwaitingFirstResponse,
        }
    }

    fn record_matched_response(&mut self) {
        self.progress = self.progress.after_response();
    }

    async fn retire(self) -> RetiredStream {
        let Self {
            write,
            responses,
            reader,
            progress,
        } = self;
        drop(write);
        drop(responses);
        let _ = reader.await;
        RetiredStream { progress }
    }
}

/// Arm the service clock on the current head — the first unresolved
/// frame — and give the health ledger its start sample.
fn arm_head(
    member: &Member,
    health: &Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
    inflight: &mut std::collections::VecDeque<StreamInFlight>,
) {
    if let Some(head) = inflight.iter_mut().find(|entry| entry.job.is_some()) {
        if head.head_started.is_none() {
            let now = mono_ms();
            head.head_started = Some(now);
            health.lock().unwrap().append_started(&member.node, now);
        }
    }
}

/// The stream lane (the ordered-transport design): pump queued
/// jobs down one ordered duplex and match responses strictly FIFO, so W
/// frames ride the wire while the follower's single reader keeps apply
/// order equal to submission order. The resume rule: on a broken stream,
/// redial once and retransmit every unanswered frame in order — the
/// follower's covering-end reading turns duplicates into acks, so the
/// follower's own state decides what was persisted, never the leader's
/// sent count (BrokenStreamResumeBlind is the model tooth). A break with
/// no progress since the last dial fails every outstanding job instead,
/// which degrades the shipper exactly as a failed append does today. A
/// head frame stalled past the hedge deadline is raced by a one-shot
/// duplicate; an old follower without the route drops the lane to the
/// one-shot path for its lifetime.
async fn stream_lane(
    member: &Member,
    transport: &Arc<dyn LogTransport>,
    policy: &celld_logic::log_evict::EvictionPolicy,
    health: &Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
    tuning: &LaneTuning,
    jobs: &mut tokio::sync::mpsc::UnboundedReceiver<LaneJob>,
    stop: &crate::ltx_repl::StopToken,
) {
    let mut conn: Option<StreamConnection> = None;
    let mut inflight: std::collections::VecDeque<StreamInFlight> =
        std::collections::VecDeque::new();
    let mut hedge_rx: Option<tokio::sync::oneshot::Receiver<AppendSend>> = None;

    fn spawn_reader(
        read: tokio::io::ReadHalf<LogStreamIo>,
    ) -> (
        tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        crate::asyncrt::TaskHandle<()>,
    ) {
        let (send, receive) = tokio::sync::mpsc::unbounded_channel();
        let reader = crate::asyncrt::spawn(async move {
            let mut read = read;
            loop {
                let frame = crate::asyncrt::select_biased! {
                    "reader cancellation wins a tie with a frame from the retired connection";
                    _ = send.closed() => return,
                    frame = read_frame(&mut read) => frame,
                };
                match frame {
                    Ok(Some(frame)) => {
                        if send.send(frame).is_err() {
                            return;
                        }
                    }
                    Ok(None) | Err(_) => return,
                }
            }
        });
        (receive, reader)
    }

    async fn dial(
        member: &Member,
        transport: &Arc<dyn LogTransport>,
    ) -> anyhow::Result<StreamConnection> {
        let io = transport
            .open_stream(&member.node, &member.addr, "/peer/log/stream")
            .await?;
        let (read, write) = tokio::io::split(io);
        let (responses, reader) = spawn_reader(read);
        Ok(StreamConnection::new(write, responses, reader))
    }

    async fn retire_connection(conn: &mut Option<StreamConnection>) {
        if let Some(conn) = conn.take() {
            let _ = conn.retire().await;
        }
    }

    /// Fail every unanswered frame; the round futures then fail and the
    /// shipper degrades through the existing path.
    fn fail_all(
        member: &Member,
        policy: &celld_logic::log_evict::EvictionPolicy,
        health: &Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
        tuning: &LaneTuning,
        inflight: &mut std::collections::VecDeque<StreamInFlight>,
    ) {
        for entry in inflight.drain(..) {
            if let Some(job) = entry.job {
                resolve_append(
                    member,
                    policy,
                    health,
                    tuning,
                    job,
                    entry.started,
                    entry.write_ms,
                    AppendSend::Failed(anyhow::anyhow!("append stream failed")),
                );
            }
        }
    }

    loop {
        // Disconnected: wait for work, then establish.
        if conn.is_none() {
            let job = crate::asyncrt::select_biased! {
                "a stop signal that ties queued work prevents a new stream connection";
                _ = stop.stopped() => None,
                job = jobs.recv() => job,
            };
            let Some(job) = job else { return };
            match dial(member, transport).await {
                Ok(established) => conn = Some(established),
                Err(error) => {
                    let incapable = error
                        .downcast_ref::<PeerHttpError>()
                        .is_some_and(|http| matches!(http.status.as_u16(), 404 | 405 | 501));
                    if incapable {
                        // An old follower: this lane speaks one-shot HTTP for
                        // its lifetime. The mixed fleet is safe and merely
                        // slow on this member.
                        info!(
                            member = member.node,
                            "follower has no stream route; lane falls back to one-shot appends"
                        );
                        http_append_job(member, transport, policy, health, tuning, job).await;
                        while let Some(job) = jobs.recv().await {
                            http_append_job(member, transport, policy, health, tuning, job).await;
                        }
                        return;
                    }
                    let started = mono_ms();
                    health.lock().unwrap().append_started(&member.node, started);
                    resolve_append(
                        member,
                        policy,
                        health,
                        tuning,
                        job,
                        started,
                        0,
                        AppendSend::Failed(error),
                    );
                    continue;
                }
            }
            // Fall through with the job still to write.
            let write = &mut conn.as_mut().expect("just established").write;
            let frame = encode_append(&job.req);
            let started = mono_ms();
            if write_frame(write, &frame).await.is_err() {
                retire_connection(&mut conn).await;
                health.lock().unwrap().append_started(&member.node, started);
                resolve_append(
                    member,
                    policy,
                    health,
                    tuning,
                    job,
                    started,
                    mono_ms().saturating_sub(started),
                    AppendSend::Failed(anyhow::anyhow!("append stream write failed")),
                );
                continue;
            }
            inflight.push_back(StreamInFlight {
                job: Some(job),
                frame,
                started,
                write_ms: mono_ms().saturating_sub(started),
                stall_reported: false,
                head_started: None,
                hedged: false,
            });
            arm_head(member, health, &mut inflight);
            continue;
        }

        // The hedge deadline for the head frame, when one is eligible.
        let armed_hedge_ms = if hedge_rx.is_none() {
            resolve_hedge_ms(tuning, policy, health)
        } else {
            0
        };
        let hedge_deadline = if armed_hedge_ms > 0 {
            inflight
                .iter()
                .find(|entry| entry.job.is_some())
                .filter(|entry| !entry.hedged)
                .and_then(|entry| Some(entry.head_started? + armed_hedge_ms))
        } else {
            None
        };
        // The head-stall ledger (the overload-stability design): a head
        // outstanding past half the backstop is on its way to evicting a
        // member, and this is the leader's only view of WHERE the frame
        // sat — write_ms says whether the socket write drained; a frame
        // that wrote in ~0 ms yet goes unanswered was not read.
        {
            let now = mono_ms();
            let queued = inflight.len();
            if let Some(head) = inflight.iter_mut().find(|entry| entry.job.is_some()) {
                if let Some(head_started) = head.head_started {
                    let age_ms = now.saturating_sub(head_started);
                    if !head.stall_reported && age_ms >= policy.backstop_ms / 2 {
                        head.stall_reported = true;
                        warn!(
                            event = "log_head_stall",
                            member = member.node,
                            age_ms,
                            queue_residency_ms = head_started.saturating_sub(head.started),
                            write_ms = head.write_ms,
                            entries = head.job.as_ref().map_or(0, |job| job.req.entries.len()),
                            bytes = head.frame.len(),
                            queued,
                            "stream head outstanding past half the backstop"
                        );
                    }
                }
            }
        }

        let responses = &mut conn.as_mut().expect("connected").responses;
        enum LaneEvent {
            Job(Option<LaneJob>),
            Response(Option<Vec<u8>>),
            HedgeFire,
            HedgeAnswer(AppendSend),
            Stopped,
        }
        let hedge_side = async {
            crate::asyncrt::select_biased! {
                "an armed hedge deadline wins a tie with the preceding hedge answer";
                fired = async {
                    match hedge_deadline {
                        Some(deadline) => {
                            let wait = deadline.saturating_sub(mono_ms());
                            crate::asyncrt::sleep(std::time::Duration::from_millis(wait)).await
                        }
                        None => std::future::pending().await,
                    }
                } => {
                    let () = fired;
                    LaneEvent::HedgeFire
                },
                answer = async {
                    match &mut hedge_rx {
                        Some(receiver) => match receiver.await {
                            Ok(answer) => answer,
                            Err(_) => AppendSend::Failed(anyhow::anyhow!("hedge task dropped")),
                        },
                        None => std::future::pending().await,
                    }
                } => LaneEvent::HedgeAnswer(answer),
            }
        };
        let event = crate::asyncrt::select_biased! {
            "a lane event wins a tie with hedge work so ordered stream progress runs first";
            event = async {
                crate::asyncrt::select_biased! {
                    "a stop signal that ties stream activity closes the lane first";
                    stopped = async {
                        stop.stopped().await;
                        LaneEvent::Stopped
                    } => stopped,
                    event = async {
                        crate::asyncrt::select_biased! {
                            "a queued append wins a tie with a response to preserve legacy lane order";
                            job = jobs.recv() => LaneEvent::Job(job),
                            response = responses.recv() => LaneEvent::Response(response),
                        }
                    } => event,
                }
            } => event,
            event = hedge_side => event,
        };

        match event {
            LaneEvent::Stopped => {
                fail_all(member, policy, health, tuning, &mut inflight);
                retire_connection(&mut conn).await;
                return;
            }
            LaneEvent::Job(None) => {
                retire_connection(&mut conn).await;
                return;
            }
            LaneEvent::Job(Some(job)) => {
                let write = &mut conn.as_mut().expect("connected").write;
                let frame = encode_append(&job.req);
                let started = mono_ms();
                let written = write_frame(write, &frame).await;
                let write_ms = mono_ms().saturating_sub(started);
                match written {
                    Ok(()) => {
                        inflight.push_back(StreamInFlight {
                            job: Some(job),
                            frame,
                            started,
                            write_ms,
                            stall_reported: false,
                            head_started: None,
                            hedged: false,
                        });
                        arm_head(member, health, &mut inflight);
                    }
                    Err(_) => {
                        inflight.push_back(StreamInFlight {
                            job: Some(job),
                            frame,
                            started,
                            write_ms,
                            stall_reported: false,
                            head_started: None,
                            hedged: false,
                        });
                        conn = reconnect(
                            conn.take().expect("connected"),
                            member,
                            transport,
                            policy,
                            health,
                            tuning,
                            &mut inflight,
                        )
                        .await;
                    }
                }
            }
            LaneEvent::Response(Some(payload)) => {
                let Some(mut entry) = inflight.pop_front() else {
                    // A response with no frame outstanding is a protocol
                    // violation; drop the stream and let redial sort it out.
                    conn = reconnect(
                        conn.take().expect("connected"),
                        member,
                        transport,
                        policy,
                        health,
                        tuning,
                        &mut inflight,
                    )
                    .await;
                    continue;
                };
                conn.as_mut().expect("connected").record_matched_response();
                let Some(job) = entry.job.take() else {
                    // The hedge already answered this frame; the stream's
                    // duplicate only frees the FIFO slot.
                    arm_head(member, health, &mut inflight);
                    continue;
                };
                let service_started = entry.head_started.unwrap_or(entry.started);
                match serde_json::from_slice::<AppendResp>(&payload) {
                    Ok(resp) => {
                        resolve_append(
                            member,
                            policy,
                            health,
                            tuning,
                            job,
                            service_started,
                            entry.write_ms,
                            AppendSend::Answered(resp),
                        );
                        arm_head(member, health, &mut inflight);
                    }
                    Err(error) => {
                        resolve_append(
                            member,
                            policy,
                            health,
                            tuning,
                            job,
                            service_started,
                            entry.write_ms,
                            AppendSend::Failed(error.into()),
                        );
                        conn = reconnect(
                            conn.take().expect("connected"),
                            member,
                            transport,
                            policy,
                            health,
                            tuning,
                            &mut inflight,
                        )
                        .await;
                    }
                }
            }
            LaneEvent::Response(None) => {
                conn = reconnect(
                    conn.take().expect("connected"),
                    member,
                    transport,
                    policy,
                    health,
                    tuning,
                    &mut inflight,
                )
                .await;
            }
            LaneEvent::HedgeFire => {
                if let Some(entry) = inflight
                    .iter_mut()
                    .find(|entry| entry.job.is_some())
                    .filter(|entry| !entry.hedged)
                {
                    entry.hedged = true;
                    info!(
                        event = "log_append_hedge",
                        member = member.node,
                        hedge_ms = armed_hedge_ms,
                        "stream head hedged after the deadline"
                    );
                    let (send, receive) = tokio::sync::oneshot::channel();
                    hedge_rx = Some(receive);
                    let transport = transport.clone();
                    let policy_owned = policy.clone();
                    let member_owned = member.clone();
                    let req = entry
                        .job
                        .as_ref()
                        .map(|job| job.req.clone())
                        .expect("checked job.is_some");
                    crate::asyncrt::spawn(async move {
                        let resp =
                            post_append_to(&transport, &policy_owned, &member_owned, &req).await;
                        let _ = send.send(resp);
                    })
                    .detach();
                }
            }
            LaneEvent::HedgeAnswer(resp) => {
                hedge_rx = None;
                let mut confirmed = None;
                if let Some(entry) = inflight
                    .iter_mut()
                    .find(|entry| entry.hedged && entry.job.is_some())
                {
                    // The one-shot duplicate may only ever turn a
                    // non-confirmation into a confirmation (the #199 race
                    // rule): a duplicate that failed fast or answered
                    // Incapable proves nothing about what the follower
                    // holds, and the stream's own answer — bounded by the
                    // reconnect deadline — owns the verdict.
                    let last = entry
                        .job
                        .as_ref()
                        .map(|job| job.req.entries.last().map_or(0, |entry| entry.seq))
                        .unwrap_or(0);
                    let epoch = entry.job.as_ref().map(|job| job.req.epoch).unwrap_or(0);
                    if send_confirms(epoch, last, &resp) {
                        if let Some(job) = entry.job.take() {
                            let service_started = entry.head_started.unwrap_or(entry.started);
                            confirmed = Some((job, service_started, entry.write_ms));
                        }
                    }
                }
                if let Some((job, service_started, write_ms)) = confirmed {
                    // A one-shot answer proves that the follower is reachable,
                    // but it is not progress on this stream connection. Retire
                    // the stalled connection now, so its complete unresolved
                    // queue shares one deadline. `reconnect` permits one ordered
                    // replay only when this stream previously returned a frame.
                    let retired = conn.take().expect("connected").retire().await;
                    resolve_append(
                        member,
                        policy,
                        health,
                        tuning,
                        job,
                        service_started,
                        write_ms,
                        resp,
                    );
                    conn = redial_after_retire(
                        retired,
                        member,
                        transport,
                        policy,
                        health,
                        tuning,
                        &mut inflight,
                    )
                    .await;
                } else {
                    arm_head(member, health, &mut inflight);
                }
            }
        }
    }

    /// Redial once and retransmit every unanswered frame in order. The
    /// follower's reported state decides what those retransmissions mean:
    /// a persisted range refuses with a covering end, which reads as the
    /// ack. A break with no answer since the last dial fails everything
    /// instead of looping.
    async fn reconnect(
        established: StreamConnection,
        member: &Member,
        transport: &Arc<dyn LogTransport>,
        policy: &celld_logic::log_evict::EvictionPolicy,
        health: &Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
        tuning: &LaneTuning,
        inflight: &mut std::collections::VecDeque<StreamInFlight>,
    ) -> Option<StreamConnection> {
        let retired = established.retire().await;
        redial_after_retire(retired, member, transport, policy, health, tuning, inflight).await
    }

    async fn redial_after_retire(
        retired: RetiredStream,
        member: &Member,
        transport: &Arc<dyn LogTransport>,
        policy: &celld_logic::log_evict::EvictionPolicy,
        health: &Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
        tuning: &LaneTuning,
        inflight: &mut std::collections::VecDeque<StreamInFlight>,
    ) -> Option<StreamConnection> {
        if !retired.permits_replay() {
            fail_all(member, policy, health, tuning, inflight);
            return None;
        }
        // Hedge-resolved slots have no response coming on the new stream.
        inflight.retain(|entry| entry.job.is_some());
        // The retransmission restarts every frame's service; a stale head
        // clock would bill the outage to the follower's health.
        for entry in inflight.iter_mut() {
            entry.head_started = None;
        }
        match dial(member, transport).await {
            Ok(mut conn) => {
                for entry in inflight.iter() {
                    if write_frame(&mut conn.write, &entry.frame).await.is_err() {
                        conn.retire().await;
                        fail_all(member, policy, health, tuning, inflight);
                        return None;
                    }
                }
                arm_head(member, health, inflight);
                Some(conn)
            }
            Err(_) => {
                fail_all(member, policy, health, tuning, inflight);
                None
            }
        }
    }
}

impl FleetShipper {
    /// Ship one batch to every member through its ordered lane.
    ///
    /// The synchronous prefix allocates the sequence range and enqueues the
    /// round on every member lane BEFORE returning the future, so submission
    /// order — not poll order — fixes the per-member append order. The
    /// returned future only collects the answers: `Some(last_seq)` when
    /// every member confirmed every entry fsync'd — the ack-all rule. Any
    /// failure degrades the shipper: fleet proofs stop and the gate rides
    /// the bucket upload, which is always safe. `covered_seq` rides along
    /// as the followers' truncate_to.
    fn ship_batch(
        &self,
        batch: Vec<ShipEntry>,
        covered_seq: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::ltx_repl::ShipCompletion> + Send + 'static>,
    > {
        if self.members.is_empty() || batch.is_empty() {
            return Box::pin(std::future::ready(
                crate::ltx_repl::ShipCompletion::unreserved(None),
            ));
        }
        // The count rises BEFORE the degraded check:
        // maintain sets degraded and then reads the outstanding count as
        // its drain barrier, so the old order let a batch slip past a
        // reconfiguration's decision and fleet-ack frames on the retired
        // ensemble that the barrier never counted.
        self.outstanding.fetch_add(1, Ordering::SeqCst);
        let outstanding = OutstandingRound(self.outstanding.clone());
        if self.degraded.load(Ordering::SeqCst) {
            return Box::pin(async move {
                crate::ltx_repl::ShipCompletion::reserved(None, outstanding)
            });
        }
        let first = self.seq.fetch_add(batch.len() as u64, Ordering::SeqCst) + 1;
        let entries: Vec<Entry> = batch
            .into_iter()
            .enumerate()
            .map(|(index, entry)| Entry {
                seq: first + index as u64,
                cell: entry.cell,
                cell_epoch: entry.epoch,
                txid: entry.txid,
                bytes: entry.bytes,
            })
            .collect();
        let last = first + entries.len() as u64 - 1;
        let req = Arc::new(AppendReq {
            leader: self.node.clone(),
            epoch: self.epoch,
            truncate_to: covered_seq,
            entries,
        });
        let req_bytes = req
            .entries
            .iter()
            .map(|entry| entry.bytes.len() as u64)
            .sum::<u64>();
        let receivers: Vec<tokio::sync::oneshot::Receiver<Option<(String, u64)>>> = self
            .lanes
            .iter()
            .enumerate()
            .map(|(index, lane)| {
                if let Some(stream) = &self.stream {
                    stream.submitted(index, req_bytes);
                }
                let (resp, receive) = tokio::sync::oneshot::channel();
                let _ = lane.send(LaneJob {
                    req: req.clone(),
                    enqueued: mono_ms(),
                    bytes: req_bytes,
                    resp,
                });
                receive
            })
            .collect();
        let view = log_tier::LeaderView {
            epoch: self.epoch,
            ensemble: self.record.ensemble.clone(),
        };
        let activated = self.activated.clone();
        let degraded = self.degraded.clone();
        let live_log = self.live_log.clone();
        let record = self.record.clone();
        let epoch = self.epoch;
        Box::pin(async move {
            // Write-all, ack-all is the core's decision: every ensemble
            // member must confirm a contiguous end at or past the batch — a
            // member that refused, errored, or answered short is a failed
            // batch.
            let ends: BTreeMap<String, u64> = futures_util::future::join_all(receivers)
                .await
                .into_iter()
                .flatten()
                .flatten()
                .collect();
            if log_tier::ack_fleet_allowed(&view, &ends, last) {
                // The activation fence: before the epoch's first fleet ack
                // is credited, the record must say `active`, or a later
                // recovery meeting only amnesiac members would seal an
                // empty gather as if nothing had ever been acked.
                if !activated.load(Ordering::SeqCst) {
                    let active = live_log.activate(&record).await.unwrap_or(false);
                    // Through the core's lease chain (lease-fold): a fenced
                    // session's renewals stop applying, so the wait fails
                    // and the ack must not credit.
                    if active {
                        activated.store(true, Ordering::SeqCst);
                    } else {
                        degrade_shared(&degraded, epoch, "activation transition not applied");
                        return crate::ltx_repl::ShipCompletion::reserved(None, outstanding);
                    }
                }
                return crate::ltx_repl::ShipCompletion::reserved(Some(last), outstanding);
            }
            degrade_shared(&degraded, epoch, "member append failed");
            crate::ltx_repl::ShipCompletion::reserved(None, outstanding)
        })
    }
}

// ── Recovery and the takeover interlock ─────────────────────────────────────

/// Everything node-log recovery needs from the node: the bucket, the signed
/// peer client, address resolution, and the raw per-cell upload.
pub struct NodeLogManager {
    node: String,
    /// The single-writer path for our own folded log state: publishes to
    /// the ownership store and rides the core's immediate renewal.
    live_log: Arc<LiveLogTransitions>,
    /// This process session's log identity: `<node>/<generation>`, the
    /// generation from the node lease record. Every self record, bundle,
    /// fragment, and loss key hangs off it.
    session: String,
    bucket: Arc<Bucket>,
    ownership: Arc<crate::ownership_store::BucketOwnership>,
    ltx: Arc<crate::ltx_repl::LtxRepl>,
    transport: Arc<dyn LogTransport>,
    /// The current ensemble's shipper, swapped whole by the maintenance
    /// loop. The manager itself is the installed `Shipper`, delegating here.
    inner: Mutex<Option<Arc<FleetShipper>>>,
    /// The record epoch THIS incarnation CASed open (0 = none). An open
    /// record at any other epoch belongs to a previous incarnation and must
    /// be recovered — its acked tail may exist only on the old followers —
    /// before the maintenance loop may step past it.
    /// Bundle the paced tiering (`CELLD_LOG_BUNDLE`): one PUT per node per
    /// flush interval instead of one per cell-transaction.
    bundle_mode: bool,
    bundle_seq: std::sync::atomic::AtomicU64,
    /// The leader's own index of the bundles it wrote this run:
    /// (object key, rows). Bounded; a restart loses it safely, because
    /// self-recovery folds the previous incarnation's bundles anyway.
    bundle_index: Mutex<std::collections::VecDeque<(String, Vec<celld_ltx::bundle::BundleRow>)>>,
    /// Sessions whose bundle subtree one sweep pass confirmed empty:
    /// a permanent tombstone (dead-lease GC never deletes a folded
    /// record) must not cost a bundle LIST on every sweep tick forever
    /// Process-local; a restart re-confirms once.
    gc_confirmed_empty: Mutex<std::collections::HashSet<String>>,
    /// Retained bundles this process has already declared unreadable. The
    /// record is the durable one; this only keeps one process from
    /// rewriting the same declaration on every scan.
    declared_bundle_losses: Mutex<BTreeSet<String>>,
    /// One-slot cache for the compactor's fetches: bundles are read many
    /// rows at a time, and re-GETting per row would refund the savings.
    bundle_cache: tokio::sync::Mutex<Option<(String, Arc<Vec<u8>>)>>,
    /// The gray-follower ledger: append timings in, eviction verdicts and
    /// the quarantine out. Outlives every shipper swap.
    health: Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
    policy: Arc<celld_logic::log_evict::EvictionPolicy>,
    /// A correlated member stall latched self-suspicion: every ensemble
    /// member stuck at once means our own connectivity is the suspect,
    /// so recruitment parks — no reopen CAS churn into a partition —
    /// until any peer answers anything successfully. Cleared by a probe
    /// or append Ok; 37 doomed epochs in one 45 s partition taught the
    /// alternative.
    suspect_self: Arc<std::sync::atomic::AtomicBool>,
    /// When we last said that the fleet posture is requested and not
    /// achieved. `maintain` runs on a ticker and used to return silently
    /// whenever it found no peer to recruit, so a fleet that never formed an
    /// ensemble logged nothing at all — only a fleet that formed one and lost
    /// it did. A single node then paid the bucket for every write with no
    /// line anywhere saying why, or that a second node changes it.
    shortfall_logged_ms: Arc<std::sync::atomic::AtomicU64>,
    /// The shutdown latch: once set, the bundle sink refuses new flushes
    /// so the graceful seal's uncovered-scan cannot go stale between the
    /// LIST and the seal CAS.
    closing: std::sync::atomic::AtomicBool,
    /// A bundle flush between its PUT and its credit; the graceful seal
    /// waits this out after latching `closing`.
    flush_in_flight: std::sync::atomic::AtomicBool,
    #[cfg(all(test, celld_internal_tests))]
    close_transition_pause: Mutex<Option<Arc<NodeLogTransitionPause>>>,
    #[cfg(all(test, celld_internal_tests))]
    maintenance_install_pause: Mutex<Option<Arc<NodeLogTransitionPause>>>,
    #[cfg(all(test, celld_internal_tests))]
    maintenance_publish_pause: Mutex<Option<Arc<NodeLogTransitionPause>>>,
    #[cfg(all(test, celld_internal_tests))]
    shipper_injection_calls: std::sync::atomic::AtomicU64,
    /// Every predecessor session's log is proven recovered; see
    /// `ensure_predecessors_recovered` for why this can latch.
    predecessors_clean: std::sync::atomic::AtomicBool,
    /// Process-local single-flight locks for dead-session recovery. The map
    /// stores weak values, so observing many historical sessions does not
    /// retain one allocation per session for the process lifetime.
    recovery_locks: Mutex<BTreeMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    /// Sessions for which this process won the Open -> Recovering CAS. A
    /// failed elected pass can retry immediately; other processes wait for
    /// the bounded claim before they compete to replace it.
    task_stop: crate::ltx_repl::StopToken,
    child_tasks: crate::ltx_repl::TaskGroup,
}

struct NodeLogTaskOwner {
    stop: crate::ltx_repl::StopToken,
    roots: crate::ltx_repl::TaskGroup,
    children: crate::ltx_repl::TaskGroup,
}

struct FollowerTaskOwner {
    store: Option<Arc<FollowerStore>>,
    follower_stop: crate::ltx_repl::StopToken,
    follower_tasks: crate::ltx_repl::TaskGroup,
}

struct OwnedDurabilityStack {
    ltx: Arc<crate::ltx_repl::LtxRepl>,
    manager: Arc<NodeLogManager>,
    registration: Option<crate::ltx_repl::DurabilityRegistration>,
    ltx_tasks: Option<crate::ltx_repl::LtxTaskOwner>,
    replica_close_task: Option<crate::asyncrt::TaskHandle<()>>,
    node_log_tasks: NodeLogTaskOwner,
    fleet: bool,
}

/// The daemon's unique owner for one local durability stack.
///
/// Cloneable runtime handles can borrow the replicator and the manager, but
/// only this value controls their coupled registration and background tasks.
/// A drop requests the local fallback but does not join tasks. Call
/// [`Self::shutdown_local`] to prove that all admitted tasks completed.
pub struct DurabilityOwner {
    stack: Option<OwnedDurabilityStack>,
    follower_tasks: FollowerTaskOwner,
}

impl DurabilityOwner {
    /// Creates the unique owner for a runtime durability stack.
    ///
    /// Set `fleet` to install the coupled node-log registration.
    /// `bundle_mode` enables the LTX bundle sink when `fleet` is `true`.
    ///
    /// # Panics
    ///
    /// The function panics if another owner controls the LTX task set. It also
    /// panics if a fleet registration targets a stopped LTX service.
    pub fn new(manager: Arc<NodeLogManager>, fleet: bool, bundle_mode: bool) -> Self {
        let shipper = manager.clone();
        Self::new_with_shipper(manager, fleet, bundle_mode, shipper)
    }

    fn new_with_shipper(
        manager: Arc<NodeLogManager>,
        fleet: bool,
        bundle_mode: bool,
        shipper: Arc<dyn crate::ltx_repl::Shipper>,
    ) -> Self {
        let ltx = manager.ltx.clone();
        // Claim the unique lifecycle capability before changing the coupled
        // registration. A duplicate construction fails here, so its unwind
        // cannot supersede and then clear the live owner's generation.
        let ltx_tasks = ltx.take_task_owner();
        let registration = fleet.then(|| {
            ltx.register_durability(
                shipper,
                bundle_mode.then(|| manager.clone() as Arc<dyn crate::ltx_repl::BundleSink>),
            )
            .expect("a new durability owner cannot install on a stopped LTX service")
        });
        Self::from_claimed_registration(manager, fleet, ltx, ltx_tasks, registration)
    }

    /// Installs a gated shipper decorator at the shipping registration seam.
    /// The lifecycle owner still retains the real manager, so the decorator
    /// cannot replace its maintenance, shutdown, or bundle ownership.
    #[cfg(all(test, celld_internal_tests))]
    pub(crate) fn new_with_shipper_for_world(
        manager: Arc<NodeLogManager>,
        fleet: bool,
        bundle_mode: bool,
        shipper: Arc<dyn crate::ltx_repl::Shipper>,
    ) -> Self {
        manager
            .shipper_injection_calls
            .fetch_add(1, Ordering::SeqCst);
        Self::new_with_shipper(manager, fleet, bundle_mode, shipper)
    }

    fn from_claimed_registration(
        manager: Arc<NodeLogManager>,
        fleet: bool,
        ltx: Arc<crate::ltx_repl::LtxRepl>,
        ltx_tasks: crate::ltx_repl::LtxTaskOwner,
        registration: Option<crate::ltx_repl::DurabilityRegistration>,
    ) -> Self {
        let follower_stop = crate::ltx_repl::StopToken::new();
        let node_log_stop = manager.task_stop.clone();
        let child_tasks = manager.child_tasks.clone();
        Self {
            stack: Some(OwnedDurabilityStack {
                ltx,
                manager,
                registration,
                ltx_tasks: Some(ltx_tasks),
                replica_close_task: None,
                node_log_tasks: NodeLogTaskOwner {
                    stop: node_log_stop.clone(),
                    roots: crate::ltx_repl::TaskGroup::new(node_log_stop),
                    children: child_tasks,
                },
                fleet,
            }),
            follower_tasks: FollowerTaskOwner {
                store: None,
                follower_stop: follower_stop.clone(),
                follower_tasks: crate::ltx_repl::TaskGroup::new(follower_stop),
            },
        }
    }

    /// Creates an owner for a follower store without a runtime durability stack.
    ///
    /// The owner starts and retains the follower fragment collector.
    ///
    /// # Panics
    ///
    /// The function panics outside the asynchronous runtime.
    pub fn new_follower(follower: Arc<FollowerStore>) -> Self {
        let follower_stop = crate::ltx_repl::StopToken::new();
        let follower_tasks = crate::ltx_repl::TaskGroup::new(follower_stop.clone());
        spawn_fragment_gc(follower.clone(), follower_stop.clone(), &follower_tasks);
        Self {
            stack: None,
            follower_tasks: FollowerTaskOwner {
                store: Some(follower),
                follower_stop,
                follower_tasks,
            },
        }
    }

    /// Starts the background tasks for a runtime durability stack.
    ///
    /// The optional follower store adds the follower fragment collector.
    ///
    /// # Panics
    ///
    /// The function panics for a follower-only owner or outside the asynchronous
    /// runtime. It also panics while node-log roots are active or when a
    /// follower store is already installed.
    pub fn start_background(&mut self, follower: Option<Arc<FollowerStore>>) {
        let stack = self
            .stack
            .as_ref()
            .expect("the durability stack must be installed");
        assert!(
            stack.node_log_tasks.roots.is_empty(),
            "node-log background tasks were already started"
        );
        if stack.fleet {
            spawn_maintenance(
                stack.manager.clone(),
                stack.node_log_tasks.stop.clone(),
                &stack.node_log_tasks.roots,
            );
        }
        if let Some(follower) = follower {
            assert!(
                self.follower_tasks.store.is_none(),
                "follower background tasks were already started"
            );
            self.follower_tasks.store = Some(follower.clone());
            spawn_fragment_gc(
                follower,
                self.follower_tasks.follower_stop.clone(),
                &self.follower_tasks.follower_tasks,
            );
        }
    }

    /// Stops the node-log roots, seals covered data, and joins the roots.
    ///
    /// This method has no deadline. Use [`Self::quiesce_and_seal_within`] during
    /// a bounded process shutdown.
    pub async fn quiesce_and_seal(&mut self) {
        let Some(stack) = &self.stack else {
            return;
        };
        stack.node_log_tasks.stop.request_stop();
        stack.manager.close_gracefully().await;
        stack.node_log_tasks.roots.join().await;
    }

    /// Quiesces the durability stack within the specified remaining time.
    ///
    /// The method returns `true` after a complete quiesce. It runs the local
    /// fallback and returns `false` after a timeout.
    #[must_use]
    pub async fn quiesce_and_seal_within(&mut self, remaining: std::time::Duration) -> bool {
        if crate::asyncrt::timeout(remaining, self.quiesce_and_seal())
            .await
            .is_ok()
        {
            true
        } else {
            // Cancellation can drop a join while it holds an admitted task
            // handle. Publish the local stop state immediately, so no task or
            // registration can recreate a durability resource after the
            // process deadline. The process exits without requiring a join.
            self.stop_local_now();
            false
        }
    }

    /// Stops local admission and joins all admitted durability tasks.
    ///
    /// The caller must await this method to prove that all tasks completed and
    /// all registered managed replicas are closed. The caller must first stop
    /// and join the runtime operations that can open a replica. This method has
    /// no deadline. Use [`Self::shutdown_local_within`] during a bounded process
    /// shutdown.
    pub async fn shutdown_local(&mut self) {
        self.follower_tasks.follower_stop.request_stop();
        if let Some(stack) = &mut self.stack {
            stack.node_log_tasks.stop.request_stop();
            if let Some(tasks) = &stack.ltx_tasks {
                tasks.request_stop();
            }
            stack.registration.take();
            stack.manager.shutdown_local_fallback();
            stack.ltx.shutdown_local_fallback();
            stack.node_log_tasks.roots.join().await;
            if let Some(tasks) = &stack.ltx_tasks {
                tasks.join().await;
            }
            // Keep the owner installed until its join completes. A deadline
            // can cancel this future while one handle is claimed, and the
            // task group returns that handle for a later shutdown attempt.
            stack.ltx_tasks.take();
            stack.node_log_tasks.children.join().await;
            // A direct durability pass is not in an owned task group and can
            // still hold a replica mutex. Every admitted release close has
            // joined now. Snapshot the remaining registered replicas, run the
            // close loop off the executor, and retain its handle before await.
            if stack.replica_close_task.is_none() {
                stack.replica_close_task = stack.ltx.start_close_local_replicas();
            }
            if let Some(close) = &mut stack.replica_close_task {
                if let Err(error) = close.await {
                    warn!(%error, "managed replica close task stopped with an error");
                }
            }
            stack.replica_close_task.take();
        }
        self.follower_tasks.follower_tasks.join().await;
        self.follower_tasks.store.take();
    }

    /// Stops local admission and joins tasks within the specified remaining time.
    ///
    /// The method returns `true` after all admitted tasks complete and all
    /// registered managed replicas are closed. The caller must first stop and
    /// join the runtime operations that can open a replica. It runs the local
    /// fallback and returns `false` after a timeout.
    #[must_use]
    pub async fn shutdown_local_within(&mut self, remaining: std::time::Duration) -> bool {
        if crate::asyncrt::timeout(remaining, self.shutdown_local())
            .await
            .is_ok()
        {
            true
        } else {
            // A cancelled group join returns its claimed handle, and the owner
            // retains every admitted release close plus the final snapshot
            // close. Publish fallback without taking a replica mutex, so a
            // blocked capture cannot extend the process deadline a second time.
            self.stop_local_now();
            false
        }
    }

    /// Stops local admission and breaks component cycles without waiting.
    ///
    /// The daemon uses this fallback after its shutdown deadline. A completed
    /// [`Self::shutdown_local`] call proves that all admitted tasks completed
    /// and all registered managed replicas are closed. A `true` result from
    /// [`Self::shutdown_local_within`] gives the same proof.
    pub fn stop_local_now(&mut self) {
        self.shutdown_local_fallback();
    }

    fn shutdown_local_fallback(&mut self) {
        self.follower_tasks.follower_stop.request_stop();
        self.follower_tasks.store.take();
        if let Some(stack) = &mut self.stack {
            stack.node_log_tasks.stop.request_stop();
            stack.registration.take();
            stack.manager.shutdown_local_fallback();
            stack.ltx.shutdown_local_fallback();
        }
    }
}

impl Drop for DurabilityOwner {
    fn drop(&mut self) {
        self.shutdown_local_fallback();
    }
}

impl crate::ltx_repl::Shipper for NodeLogManager {
    fn ship(
        &self,
        batch: Vec<ShipEntry>,
        covered_seq: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::ltx_repl::ShipCompletion> + Send + 'static>,
    > {
        let epoch = self.epoch();
        self.ship_at_epoch(epoch, batch, covered_seq)
    }

    fn ship_at_epoch(
        &self,
        expected_epoch: u64,
        batch: Vec<ShipEntry>,
        covered_seq: u64,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = crate::ltx_repl::ShipCompletion> + Send + 'static>,
    > {
        // Select the delegate and reserve its round under the same manager
        // lock that maintenance uses to replace it. A capture that finishes
        // after an epoch swap cannot reach the new followers with the old
        // epoch's truncation watermark.
        let round = self
            .inner
            .lock()
            .unwrap()
            .as_ref()
            .filter(|shipper| shipper.epoch == expected_epoch)
            .map(|shipper| shipper.ship_batch(batch, covered_seq));
        let suspect_self = self.suspect_self.clone();
        Box::pin(async move {
            let shipped = match round {
                Some(round) => round.await,
                None => crate::ltx_repl::ShipCompletion::unreserved(None),
            };
            if shipped.last_seq().is_some() {
                suspect_self.store(false, Ordering::SeqCst);
            }
            shipped
        })
    }

    fn pipeline(&self) -> usize {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map_or(1, |shipper| shipper.pipeline)
    }

    fn active(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|shipper| shipper.is_active())
    }

    fn epoch(&self) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |shipper| shipper.epoch)
    }

    fn admit(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|shipper| shipper.stream.as_ref().is_none_or(|stream| stream.admit()))
    }
}

impl NodeLogManager {
    #[cfg(all(test, celld_internal_tests))]
    pub fn reset_shipper_injection_calls_for_world(&self) {
        self.shipper_injection_calls.store(0, Ordering::SeqCst);
    }

    #[cfg(all(test, celld_internal_tests))]
    pub fn shipper_injection_calls_for_world(&self) -> u64 {
        self.shipper_injection_calls.load(Ordering::SeqCst)
    }

    /// `session` is the process's full log identity, `<node>/<generation>`,
    /// with the generation taken from the node lease record.
    pub fn new(
        session: &str,
        bucket: Arc<Bucket>,
        own_log: Arc<OwnLog>,
        ltx: Arc<crate::ltx_repl::LtxRepl>,
        auth: Arc<PeerAuth>,
        bundle_mode: bool,
        policy: celld_logic::log_evict::EvictionPolicy,
    ) -> Self {
        Self::new_with_log_transport(
            session,
            bucket,
            own_log,
            ltx,
            Arc::new(SignedPeerTransport::new(auth)),
            bundle_mode,
            policy,
        )
    }

    #[cfg(all(test, celld_internal_tests))]
    pub(crate) fn new_with_transport(
        session: &str,
        bucket: Arc<Bucket>,
        own_log: Arc<OwnLog>,
        ltx: Arc<crate::ltx_repl::LtxRepl>,
        transport: Arc<dyn LogTransport>,
        bundle_mode: bool,
        policy: celld_logic::log_evict::EvictionPolicy,
    ) -> Self {
        Self::new_with_log_transport(
            session,
            bucket,
            own_log,
            ltx,
            transport,
            bundle_mode,
            policy,
        )
    }

    fn new_with_log_transport(
        session: &str,
        bucket: Arc<Bucket>,
        own_log: Arc<OwnLog>,
        ltx: Arc<crate::ltx_repl::LtxRepl>,
        transport: Arc<dyn LogTransport>,
        bundle_mode: bool,
        policy: celld_logic::log_evict::EvictionPolicy,
    ) -> Self {
        let ownership = own_log.ownership.clone();
        let live_log = Arc::new(LiveLogTransitions::new(own_log));
        let task_stop = crate::ltx_repl::StopToken::new();
        let child_tasks = crate::ltx_repl::TaskGroup::new(task_stop.clone());
        Self {
            node: session.split('/').next().unwrap_or(session).to_string(),
            session: session.to_string(),
            live_log,
            bucket,
            ownership,
            ltx,
            transport,
            inner: Mutex::new(None),
            bundle_mode,
            bundle_seq: std::sync::atomic::AtomicU64::new(0),
            bundle_index: Mutex::new(std::collections::VecDeque::new()),
            bundle_cache: tokio::sync::Mutex::new(None),
            health: Arc::new(Mutex::new(celld_logic::log_evict::FollowerHealth::default())),
            policy: Arc::new(policy),
            suspect_self: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            shortfall_logged_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            closing: std::sync::atomic::AtomicBool::new(false),
            flush_in_flight: std::sync::atomic::AtomicBool::new(false),
            #[cfg(all(test, celld_internal_tests))]
            close_transition_pause: Mutex::new(None),
            #[cfg(all(test, celld_internal_tests))]
            maintenance_install_pause: Mutex::new(None),
            #[cfg(all(test, celld_internal_tests))]
            maintenance_publish_pause: Mutex::new(None),
            #[cfg(all(test, celld_internal_tests))]
            shipper_injection_calls: std::sync::atomic::AtomicU64::new(0),
            predecessors_clean: std::sync::atomic::AtomicBool::new(false),
            recovery_locks: Mutex::new(BTreeMap::new()),
            gc_confirmed_empty: Mutex::new(std::collections::HashSet::new()),
            declared_bundle_losses: Mutex::new(BTreeSet::new()),
            task_stop,
            child_tasks,
        }
    }

    /// Recover the predecessor session's log, read from this NODE's own
    /// lease record: until our install replaces it, the record carries the
    /// predecessor's generation and folded state, and recovery-before-
    /// install is the invariant that lets a successor write log: None.
    /// DONE-ONCE per process: after one clean pass the latch holds — a
    /// predecessor state can only reappear if another process supersedes
    /// our lease, and then we are fenced regardless. The latch sets only
    /// on success; a failed pass retries on the next cold path.
    async fn ensure_predecessors_recovered(&self) -> anyhow::Result<()> {
        if self.predecessors_clean.load(Ordering::SeqCst) {
            return Ok(());
        }
        if let Some(folded) = read_record(&self.bucket, &self.node).await? {
            let session = format!("{}/{}", self.node, folded.wire.generation);
            if session != self.session
                && log_tier::takeover_gate(Some(&folded.record))
                    == log_tier::TakeoverGate::RecoverFirst
            {
                info!(session, "recovering a predecessor session's node log");
                self.recover_as(&session, RecoverMode::Boot).await?;
            }
        }
        // The install about to follow erases the record's only pointer to
        // the predecessor generation, and no GC path can rediscover a
        // sealed subtree with no pointer: the boot is
        // the one moment that knows every stale generation, so it sweeps
        // them here. Non-fatal — the leak is storage cost, not safety,
        // and the next restart retries.
        if let Err(error) = self.gc_stale_generation_bundles().await {
            warn!(%error, "stale-generation bundle GC failed; retried at the next restart");
        }
        self.predecessors_clean.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Delete every bundle and recovery checkpoint under this node from a
    /// generation other than the running session's. Recovery-before-
    /// install has already sealed the predecessor by the time this runs,
    /// and a sealed session's bundles are garbage (recovery folded every
    /// acked row per-cell first). Its recovery checkpoints are obsolete too.
    /// Loss declarations and all other keys stay untouched.
    async fn gc_stale_generation_bundles(&self) -> anyhow::Result<()> {
        let prefix = format!("log/{}/", self.node);
        let mut stale: Vec<String> = Vec::new();
        for meta in self.bucket.list(&prefix).await? {
            let key = meta.location.as_ref().to_string();
            let Some(rest) = key.strip_prefix(&prefix) else {
                continue;
            };
            let Some((generation, tail)) = rest.split_once('/') else {
                continue;
            };
            if !tail.starts_with("bundle/") && !tail.starts_with("recovered/") {
                continue;
            }
            if format!("{}/{generation}", self.node) == self.session {
                continue;
            }
            stale.push(key);
        }
        if stale.is_empty() {
            return Ok(());
        }
        let count = stale.len();
        let gone = self.bucket.delete_many(&stale).await;
        if gone.len() != count {
            anyhow::bail!(
                "{} of {count} stale-generation recovery objects survived the delete",
                count - gone.len()
            );
        }
        info!(
            objects = count,
            "stale predecessor generations' recovery objects retired"
        );
        Ok(())
    }

    /// The fast eviction watch, one poll: any member the policy says to
    /// evict degrades the shipper now (acks fall to the bucket, which is
    /// always safe) and joins the quarantine; the caller runs `maintain`
    /// immediately so the swap costs detection plus flush plus CAS, not a
    /// maintenance tick. Returns whether anything was evicted.
    pub fn evict_gray_followers(&self) -> bool {
        let inner = self.inner.lock().unwrap().clone();
        let Some(shipper) = inner else { return false };
        if !shipper.is_active() {
            return false;
        }
        let now = mono_ms();
        let members: Vec<String> = shipper
            .members
            .iter()
            .map(|member| member.node.clone())
            .collect();
        let mut health = self.health.lock().unwrap();
        // Every member stuck at once is OUR fault, not theirs: degrade
        // without quarantining anyone, so the pool is intact the moment
        // connectivity returns.
        if health.correlated_stall(&self.policy, &members, now) {
            drop(health);
            self.suspect_self.store(true, Ordering::SeqCst);
            shipper.degrade("correlated member stall; suspecting ourselves");
            return true;
        }
        if !health.swap_allowed(&self.policy, now) {
            return false;
        }
        for member in &members {
            if health.verdict(&self.policy, member, &members, now)
                == celld_logic::log_evict::Verdict::Evict
            {
                // The evidence is a second, idempotent read of the same
                // ledger at the same instant: the eviction path is rare,
                // and the decision entry stays the one the DST drives.
                let (_, detail) = health.verdict_detailed(&self.policy, member, &members, now);
                health.evicted(&self.policy, member, now);
                drop(health);
                // The evidence, not just the sentence: which rule, and the
                // numbers it saw. Reconstructing this after the fact from
                // completed appends is impossible for the backstop, whose
                // evidence is the append that never completed.
                warn!(
                    event = "log_evict_verdict",
                    member,
                    rule = detail.rule,
                    outstanding_ms = detail.outstanding_ms,
                    own_median_ms = detail.own_median_ms,
                    samples = detail.samples,
                    sibling_median_ms = detail.sibling_median_ms,
                    threshold_ms = detail.threshold_ms,
                    suspect_ms = detail.suspect_ms,
                    backstop_ms = self.policy.backstop_ms,
                    budget_ms = self.policy.budget_ms,
                    "gray follower evicted"
                );
                shipper.degrade(&format!("gray follower {member} evicted"));
                return true;
            }
        }
        false
    }

    /// Idle disk probes: an empty append still persists (and fsyncs) the
    /// follower's state file, so a quiet fleet finds a dying follower disk
    /// before load does. One owned probe runs per quiet member per interval.
    /// A hanging probe marks the member outstanding and the backstop acts.
    pub fn probe_followers(self: &Arc<Self>) {
        const PROBE_QUIET_MS: u64 = 2_000;
        let inner = self.inner.lock().unwrap().clone();
        let Some(shipper) = inner else { return };
        // Probes run DEGRADED too — a degraded shipper is exactly when
        // connectivity evidence matters: the probe's signed 200 is what
        // lifts self-suspicion after a partition heals, and gating probes
        // on health once parked recruitment forever.
        let now = mono_ms();
        for member in &shipper.members {
            if !self
                .health
                .lock()
                .unwrap()
                .probe_due(&member.node, now, PROBE_QUIET_MS)
            {
                continue;
            }
            let shipper = shipper.clone();
            let node = member.node.clone();
            let health = self.health.clone();
            let suspect_self = self.suspect_self.clone();
            let stop = self.task_stop.clone();
            self.child_tasks
                .spawn_owned("node_log_follower_probe", async move {
                    let member = shipper
                        .members
                        .iter()
                        .find(|member| member.node == node)
                        .expect("probed member is in the ensemble");
                    let req = AppendReq {
                        leader: shipper.node.clone(),
                        epoch: shipper.epoch,
                        truncate_to: 0,
                        entries: Vec::new(),
                    };
                    let started = mono_ms();
                    health.lock().unwrap().append_started(&node, started);
                    let outcome = crate::asyncrt::select_biased! {
                        "a stop signal that ties a probe response prevents recording stale health";
                        _ = stop.stopped() => return,
                        outcome = shipper.post_append(member, &req) => outcome,
                    };
                    let done = mono_ms();
                    health.lock().unwrap().append_completed(
                        &node,
                        done,
                        done.saturating_sub(started),
                    );
                    // ANY well-formed peer response — even an append refusal,
                    // which is still a signed HTTP 200 — proves connectivity
                    // and lifts self-suspicion. An incapable answer proves
                    // the peer is the wrong binary, not that we are cut off,
                    // and it quarantines here exactly as a shipped batch
                    // would — an idle ensemble must not keep a 0.2.x member
                    // recruit-eligible just because no writes arrive.
                    match outcome {
                        AppendSend::Answered(_) => {
                            suspect_self.store(false, Ordering::SeqCst);
                        }
                        AppendSend::Incapable(error) => {
                            warn!(
                                member = node,
                                %error,
                                "follower cannot serve log appends; quarantined from recruitment"
                            );
                            health
                                .lock()
                                .unwrap()
                                .append_incapable(&shipper.policy, &node, done);
                        }
                        AppendSend::Failed(_) => {}
                    }
                });
        }
    }

    async fn post<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        node: &str,
        addr: &str,
        path: &str,
        req: &Req,
    ) -> anyhow::Result<Resp> {
        let bytes = self
            .transport
            .post(node, addr, path, serde_json::to_vec(req)?, None)
            .await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// The tail POST: a JSON request, a binary response (the entries
    /// dominate it).
    async fn post_tail(&self, node: &str, addr: &str, req: &TailReq) -> anyhow::Result<TailResp> {
        let bytes = self
            .transport
            .post(node, addr, "/peer/log/tail", serde_json::to_vec(req)?, None)
            .await?;
        decode_tail_resp(&bytes)
    }

    /// Node-log recovery, the model's StartRecovery/SealFollower/
    /// FinishRecovery in one pass: fence via CAS, seal every reachable
    /// member, gather their tails, require one matching member to return its
    /// complete retained range, upload each entry to the exact per-cell key
    /// the dead leader would have used, and CAS the record sealed. Every step
    /// is a CAS or an idempotent PUT, so racing recoverers re-run harmlessly.
    /// Upload gathered rows into the per-cell layout: grouped per
    /// (cell, epoch), each group's contiguous tail merged into ONE L0
    /// segment (per-row fallback for non-contiguous chains), skipping
    /// rows the per-cell watermark already covers. Shared by recovery's
    /// gather and the reopen healing pass. Any failure propagates —
    /// callers must not seal past an incomplete fold.
    async fn upload_gathered(
        &self,
        gathered: BTreeMap<(String, u64, u64), Vec<u8>>,
        mut claim: Option<(&str, &mut ClaimBeat)>,
    ) -> anyhow::Result<usize> {
        if gathered.is_empty() {
            return Ok(0);
        }
        let mut progress = match claim.as_mut() {
            Some((dead, beat)) => Some(recovery_progress::Progress::load(self, dead, beat).await?),
            None => None,
        };
        type CellRows = Vec<(u64, Vec<u8>)>;
        let mut groups: BTreeMap<(String, u64), CellRows> = BTreeMap::new();
        for ((cell, cell_epoch, txid), bytes) in gathered {
            groups
                .entry((cell, cell_epoch))
                .or_default()
                .push((txid, bytes));
        }
        if let Some(progress) = &progress {
            groups.retain(|(cell, epoch), rows| {
                if let Some(through) = progress.through(cell, *epoch) {
                    rows.retain(|(txid, _)| *txid > through);
                }
                !rows.is_empty()
            });
        }
        let uploads = groups.into_iter().map(|((cell, cell_epoch), rows)| {
            let ltx = self.ltx.clone();
            async move {
                let through = rows.last().expect("a gathered cell has at least one row").0;
                let watermark = ltx.covered_txid(&cell, cell_epoch).await;
                let rows: Vec<(u64, Vec<u8>)> = rows
                    .into_iter()
                    .filter(|(txid, _)| *txid > watermark)
                    .collect();
                if rows.is_empty() {
                    return anyhow::Ok((recovery_progress::CoveredCell { cell, epoch: cell_epoch, through }, 0_usize));
                }
                let uploaded = rows.len();
                let puts: Vec<(u64, u64, Vec<u8>)> =
                    match crate::ltx_repl::LtxRepl::merge_l0_rows(&rows) {
                        Some(merged) => {
                            vec![(rows[0].0, rows[uploaded - 1].0, merged)]
                        }
                        None => rows
                            .into_iter()
                            .map(|(txid, bytes)| (txid, txid, bytes))
                            .collect(),
                    };
                for (min_txid, max_txid, bytes) in puts {
                    // Racing recoverers PUT the same idempotent keys, and
                    // the object store answers the collision with a
                    // retryable refusal, not corruption — one recoverer
                    // backing off is all it takes to converge.
                    let mut attempt = 0_u32;
                    loop {
                        match ltx
                            .upload_raw_l0(&cell, cell_epoch, min_txid, max_txid, &bytes)
                            .await
                        {
                            Ok(()) => break,
                            Err(error) if attempt < 4 => {
                                attempt += 1;
                                let jitter = (min_txid.wrapping_mul(2654435761) >> 27) % 97;
                                let wait = 150_u64 * (1 << attempt) + jitter;
                                warn!(%error, cell, min_txid, attempt, "recovery upload refused; backing off");
                                crate::asyncrt::sleep(std::time::Duration::from_millis(wait)).await;
                            }
                            Err(error) => {
                                return Err(error).with_context(|| {
                                    format!(
                                        "recovery upload {cell} e{cell_epoch} \
                                         t{min_txid}-{max_txid}"
                                    )
                                });
                            }
                        }
                    }
                }
                anyhow::Ok((recovery_progress::CoveredCell { cell, epoch: cell_epoch, through }, uploaded))
            }
        });
        let mut count = 0_usize;
        let mut uploads =
            futures_util::stream::iter(uploads).buffer_unordered(RECOVERY_UPLOAD_CONCURRENCY);
        while let Some(uploaded) = futures_util::StreamExt::next(&mut uploads).await {
            let (cell, uploaded) = uploaded?;
            count += uploaded;
            if let Some(progress) = progress.as_mut() {
                progress.completed(&self.bucket, cell).await;
            }
            if let Some((dead, beat)) = claim.as_mut() {
                self.beat_claim(dead, beat).await?;
            }
        }
        Ok(count)
    }

    /// Recover one dead SESSION's log: `dead` is `<node>/<generation>`.
    pub async fn recover(&self, dead: &str) -> anyhow::Result<()> {
        self.recover_as(dead, RecoverMode::Sweep).await
    }

    pub(crate) async fn recover_as(&self, dead: &str, mode: RecoverMode) -> anyhow::Result<()> {
        let recovery_lock = {
            let mut locks = self.recovery_locks.lock().unwrap();
            locks.retain(|_, lock| lock.strong_count() > 0);
            match locks.get(dead).and_then(std::sync::Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(tokio::sync::Mutex::new(()));
                    locks.insert(dead.to_string(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        // A sweep that finds this process already recovering the session,
        // for a boot or a cold route, has nothing to add; waiting here held
        // the maintenance ticker for the length of the recovery.
        let _single_flight = match mode {
            RecoverMode::Boot => recovery_lock.lock().await,
            RecoverMode::Sweep => match recovery_lock.try_lock() {
                Ok(guard) => guard,
                Err(_) => {
                    info!(dead, "this node is already recovering the log");
                    return Ok(());
                }
            },
        };
        self.recover_serial(dead, mode).await
    }

    /// Refresh the claim's heartbeat once `RECOVERY_HEARTBEAT` has passed.
    /// An error means another node took the claim over because this one
    /// looked stale; the gather stops and the caller re-resolves.
    async fn beat_claim(&self, dead: &str, beat: &mut ClaimBeat) -> anyhow::Result<()> {
        if mono_ms().saturating_sub(beat.last_mono_ms) < RECOVERY_HEARTBEAT.as_millis() as u64 {
            return Ok(());
        }
        let Some(folded) = read_record(&self.bucket, dead).await? else {
            return Ok(());
        };
        let now = crate::ownership_store::now_ms();
        let Some(refreshed) = log_tier::refresh_recovery(&folded.record, &self.node, now) else {
            anyhow::bail!("node-log recovery for {dead} was taken over by another node");
        };
        // A lost CAS is a concurrent record write, not a lost claim; the
        // next beat re-reads and refreshes again.
        let _ = write_dead_record(
            &self.bucket,
            dead,
            &folded.wire,
            &refreshed,
            folded.active,
            &folded.token,
        )
        .await?;
        beat.last_mono_ms = mono_ms();
        Ok(())
    }

    async fn recover_serial(&self, dead: &str, mode: RecoverMode) -> anyhow::Result<()> {
        let stale_after_ms = RECOVERY_CLAIM_TTL.as_millis() as u64;
        for _attempt in 0..5 {
            let Some(folded) = read_record(&self.bucket, dead).await? else {
                return Ok(());
            };
            let FoldedRead {
                record,
                active,
                token,
                wire,
            } = folded;
            // Re-judge deadness on the record actually being fenced: the
            // caller's verdict may be stale — an owner
            // can restart between the read that justified this call and
            // now, and the spec's RecoverLog is enabled only past expiry
            // AT the step. A live lease is an error, not a skip: the
            // caller's claim must refuse and re-resolve to routing.
            let now = crate::ownership_store::now_ms();
            anyhow::ensure!(
                wire.expires_ms <= now,
                "refusing to fence {dead}: its lease is live again (expires in {}ms)",
                wire.expires_ms.saturating_sub(now)
            );
            // No justification pin: the record is keyed by session, so a
            // revived process writes a NEW key and can never step this one.
            // The only concurrent writer is a rival recoverer, and every
            // recovery step is a CAS or an idempotent upload — a lost CAS
            // re-reads and converges on the rival's outcome.
            match record.state {
                LogState::Sealed => return Ok(()),
                LogState::Open => {
                    let Some(recovering) = log_tier::start_recovery(&record, &self.node, now)
                    else {
                        continue;
                    };
                    if write_dead_record(&self.bucket, dead, &wire, &recovering, active, &token)
                        .await?
                        .is_none()
                    {
                        continue; // lost the CAS; re-read
                    }
                }
                LogState::Recovering if log_tier::recovery_claimed_by(&record, &self.node) => {}
                LogState::Recovering
                    if log_tier::recovery_claim_live(&record, now, stale_after_ms) =>
                {
                    // The Open -> Recovering CAS elects one reader, and its
                    // heartbeat is the proof it is still reading. A sweep
                    // leaves it alone. A boot cannot go on without the seal,
                    // so it waits; the wait ends when the log seals or the
                    // heartbeat goes stale, and the next pass takes over.
                    let claimant = record.claimant.clone().unwrap_or_default();
                    if mode == RecoverMode::Sweep {
                        info!(dead, claimant, "another node is recovering this log");
                        return Ok(());
                    }
                    let waited_from = mono_ms();
                    let mut reported = waited_from;
                    loop {
                        crate::asyncrt::sleep(RECOVERY_CLAIM_POLL).await;
                        let Some(current) = read_record(&self.bucket, dead).await? else {
                            return Ok(());
                        };
                        let now = crate::ownership_store::now_ms();
                        if current.record.state == LogState::Sealed {
                            return Ok(());
                        }
                        if !log_tier::recovery_claim_live(&current.record, now, stale_after_ms) {
                            break;
                        }
                        if mono_ms().saturating_sub(reported)
                            >= RECOVERY_WAIT_REPORT.as_millis() as u64
                        {
                            reported = mono_ms();
                            info!(
                                dead,
                                claimant,
                                waited_ms = mono_ms().saturating_sub(waited_from),
                                "waiting behind another node's live recovery"
                            );
                        }
                    }
                    continue;
                }
                LogState::Recovering => {
                    let Some(taken) =
                        log_tier::take_over_recovery(&record, &self.node, now, stale_after_ms)
                    else {
                        continue;
                    };
                    warn!(
                        dead,
                        stale_claimant = record.claimant.as_deref().unwrap_or("none"),
                        "taking over a stale recovery claim"
                    );
                    if write_dead_record(&self.bucket, dead, &wire, &taken, active, &token)
                        .await?
                        .is_none()
                    {
                        continue; // lost the CAS; re-read
                    }
                }
            }
            let mut beat = ClaimBeat::new();
            let Some(folded) = read_record(&self.bucket, dead).await? else {
                return Ok(());
            };
            let FoldedRead {
                record,
                active,
                token: _,
                wire: _,
            } = folded;
            if record.state == LogState::Sealed {
                return Ok(());
            }

            let now = crate::ownership_store::now_ms();
            let pass_started = mono_ms();
            let mut complete_witnesses = 0_usize;
            // A member is CONCLUSIVE when its fate is known: lease provably
            // expired and unreachable, reachable with a different fragment
            // epoch, or reachable with an explicitly incomplete retained
            // range. A failed tail or an old response without that range is
            // inconclusive. Only a fully conclusive, witness-free, active log
            // may declare bounded loss.
            let mut inconclusive = 0_usize;
            let mut gathered: BTreeMap<(String, u64, u64), Vec<u8>> = BTreeMap::new();
            // "A blink is not death" applies to loss declaration too: a
            // member whose lease merely lapsed (a restart, a fleet-wide
            // power cycle) is NOT conclusively gone — its fsync'd fragments
            // boot back in seconds, and declaring loss against boot order
            // would discard acked writes sitting intact on disk — so a
            // 3x-TTL grace applies to MEMBER fate here, unrelated to the
            // sweep (which, under the fold, judges the record's own
            // published expiry with no grace).
            let grace_ms = (self.ownership.lease_ttl_ms() * 3).max(20_000);
            for member in &record.ensemble {
                self.beat_claim(dead, &mut beat).await?;
                let lease = self.ownership.read_node_lease(member).await;
                let lease_live = matches!(&lease, Ok(Some(lease)) if lease.expires_ms > now);
                let lease_long_dead = matches!(
                    &lease,
                    Ok(Some(lease)) if lease.expires_ms.saturating_add(grace_ms) < now
                );
                let addr = match lease {
                    Ok(Some(lease)) => Some(lease.addr),
                    _ => None,
                };
                let Some(addr) = addr else {
                    if lease_live || !lease_long_dead {
                        inconclusive += 1;
                    }
                    continue;
                };
                let seal = SealReq {
                    leader: dead.to_string(),
                    epoch: record.epoch,
                };
                let Ok::<SealResp, _>(sealed) =
                    self.post(member, &addr, "/peer/log/seal", &seal).await
                else {
                    if lease_live || !lease_long_dead {
                        inconclusive += 1;
                    }
                    continue;
                };
                let held_fragment_epoch =
                    sealed.held_fragment_epoch.unwrap_or(sealed.fragment_epoch);
                let tail = TailReq {
                    leader: dead.to_string(),
                };
                let mut tail = match self.post_tail(member, &addr, &tail).await {
                    Ok(tail) => tail,
                    Err(error) => {
                        // The seal answer and the fragment are TWO requests,
                        // and only the second carries the data. Counting the
                        // witness on the first let a fragment this pass never
                        // read satisfy the certify guard: recovery sealed the
                        // record as "the bucket is complete" while the acked
                        // frames sat fsync'd on a member that had just
                        // answered, and the fragment GC then deleted the last
                        // copy. A member that sealed has a live disk, so its
                        // fate is open, not conclusive — it joins the loud
                        // retry instead.
                        warn!(
                            member,
                            dead,
                            epoch = record.epoch,
                            %error,
                            "recovery could not read a sealed member's tail"
                        );
                        if held_fragment_epoch == record.epoch {
                            inconclusive += 1;
                        }
                        continue;
                    }
                };
                tail.entries.sort_by_key(|entry| entry.seq);
                // A matching fragment epoch identifies the right fragment,
                // but it does not prove that every persisted sequence is
                // still readable. The seal returns the retained range, and
                // the tail must cover that complete range before recovery can
                // use this member as its evidence. A response from an older
                // follower has no base, so a rolling upgrade retries instead
                // of silently accepting an unverifiable tail.
                if held_fragment_epoch == record.epoch {
                    match sealed.base {
                        Some(base) if tail_covers_sealed_range(base, sealed.end, &tail.entries) => {
                            complete_witnesses += 1;
                        }
                        Some(base) => {
                            warn!(
                                dead,
                                member,
                                base,
                                end = sealed.end,
                                "sealed follower returned an incomplete tail"
                            );
                        }
                        None => {
                            inconclusive += 1;
                            warn!(
                                dead,
                                member, "sealed follower did not report its retained range"
                            );
                        }
                    }
                }
                for entry in tail.entries {
                    gathered.insert((entry.cell, entry.cell_epoch, entry.txid), entry.bytes);
                }
            }
            let members_ms = mono_ms().saturating_sub(pass_started);
            // The dead leader's un-drained bundles are bucket-durable
            // coverage that recovery folds into the per-cell prefixes —
            // one GET per bundle, sliced locally, the same idempotent
            // per-cell PUTs as the follower gather. This runs regardless
            // of the witness outcome: even a declared loss drains what
            // the bucket already holds. EVERY retained bundle, not only
            // the record epoch's: a live reconfiguration steps the epoch
            // behind a barrier that counts bundle coverage as tiered, so
            // rows can be durable only in a prior epoch's bundle — an
            // epoch filter here sealed them out of the per-cell layout
            // forever (the RecoveryEpochFilter tooth). What bounds this
            // gather to the true un-drained window is bundle GC deleting
            // covered bundles, not a filter that can orphan acked rows;
            // the covered-txid check below still bounds the uploads.
            // Concurrent GETs: the profiling round measured the serial
            // gather at 89-112 s over ~1,300 bundles — the whole outage.
            // Order does not matter: a row duplicated across bundles
            // carries identical bytes (same cell, epoch, txid), and
            // or_insert keeps follower-gathered bytes authoritative.
            let bundle_metas = self
                .bucket
                .list(&format!("log/{dead}/bundle/"))
                .await
                .with_context(|| format!("list retained recovery bundles for {dead}"))?;
            let bundles_read = bundle_metas.len();
            let fetches = bundle_metas.into_iter().map(|meta| {
                let bucket = self.bucket.clone();
                async move {
                    let key = meta.location.as_ref().to_string();
                    let fetched = bucket
                        .get(&key)
                        .await
                        .with_context(|| format!("read retained recovery bundle {key}"))?;
                    let (bytes, _) = fetched
                        .ok_or_else(|| anyhow!("listed recovery bundle {key} disappeared"))?;
                    anyhow::Ok((key, bytes))
                }
            });
            let mut fetches =
                futures_util::stream::iter(fetches).buffer_unordered(RECOVERY_UPLOAD_CONCURRENCY);
            while let Some(fetched) = futures_util::StreamExt::next(&mut fetches).await {
                self.beat_claim(dead, &mut beat).await?;
                let (key, bytes) = fetched?;
                let rows = celld_ltx::bundle::decode_rows(&bytes)
                    .with_context(|| format!("decode retained recovery bundle {key}"))?;
                for row in rows {
                    let payload = celld_ltx::bundle::slice(&bytes, &row)
                        .with_context(|| format!("read a row from recovery bundle {key}"))?;
                    gathered
                        .entry((row.cell.clone(), row.cell_epoch, row.txid))
                        .or_insert_with(|| payload.to_vec());
                }
            }
            drop(fetches);

            let bundles_ms = mono_ms()
                .saturating_sub(pass_started)
                .saturating_sub(members_ms);
            // `active` was CASed before the first fleet ack of this epoch was
            // credited, so no complete witness plus active means that acked
            // frames can be missing. If any member's state remains
            // inconclusive, keep failing loudly because its data can still
            // become readable. If every member is conclusive, declare the
            // bounded loss AS A RECORD: a permanent object beside the log
            // record stating what was unrecoverable, then seal and proceed.
            // "Loss is a record, never a prompt."
            if complete_witnesses == 0 && active && !record.ensemble.is_empty() {
                anyhow::ensure!(
                    inconclusive == 0,
                    "node-log recovery for {dead}: no complete true witness among {:?} and \
                     {inconclusive} member(s) undecided; refusing to seal while \
                     member state remains unverified",
                    record.ensemble
                );
                let loss = serde_json::json!({
                    "leader": dead,
                    "epoch": record.epoch,
                    "declared_at_ms": now,
                    "declared_by": self.node,
                    "ensemble": record.ensemble.iter().collect::<Vec<_>>(),
                    "note": "no complete true witness survived; acked writes within the \
                             final flush window may be unrecovered",
                });
                self.bucket
                    .put(
                        &format!("log/{dead}.e{}.loss.json", record.epoch),
                        serde_json::to_vec(&loss)?,
                    )
                    .await?;
                warn!(
                    dead,
                    epoch = record.epoch,
                    "declared bounded loss: no complete true witness for an active log; \
                     recovery record written"
                );
            }
            // Skip rows the drain points already folded into the per-cell
            // prefix: one listing per level and cell bounds uploads to the true
            // un-drained tail, so recovery cost tracks the flush window,
            // not the epoch's age. LTX TXIDs are contiguous per epoch, so
            // coverage up to the listed maximum is coverage of everything
            // at or below it. Cells drive concurrently — the sequential
            // version cost the lab ~47 s for 180 entries — while rows
            // within a cell stay ordered; any failure aborts the pass
            // before the record can seal.
            let upload_started = mono_ms();
            let count = self
                .upload_gathered(gathered, Some((dead, &mut beat)))
                .await?;
            let upload_ms = mono_ms().saturating_sub(upload_started);
            // The record is re-read for the token the beats moved.
            let mut sealed = false;
            for _ in 0..3 {
                let Some(current) = read_record(&self.bucket, dead).await? else {
                    return Ok(());
                };
                if current.record.state == LogState::Sealed {
                    return Ok(());
                }
                anyhow::ensure!(
                    log_tier::recovery_claimed_by(&current.record, &self.node),
                    "node-log recovery for {dead} was taken over by another node"
                );
                let Some(done) = log_tier::finish_recovery(&current.record, record.tiered) else {
                    break;
                };
                if write_dead_record(
                    &self.bucket,
                    dead,
                    &current.wire,
                    &done,
                    current.active,
                    &current.token,
                )
                .await?
                .is_some()
                {
                    sealed = true;
                    break;
                }
            }
            if sealed {
                info!(
                    dead,
                    entries = count,
                    pass = _attempt,
                    members_ms,
                    bundles_read,
                    bundles_ms,
                    upload_ms,
                    total_ms = mono_ms().saturating_sub(pass_started),
                    "node log recovered and sealed"
                );
                return Ok(());
            }
        }
        Err(anyhow!("node-log recovery for {dead} lost every CAS race"))
    }

    /// The takeover interlock: may this cell's takeover treat the bucket as
    /// complete? Runs before the restore reads or seals anything. `prior`
    /// arrives through the decision core's Claim, so an acquire confirmed by
    /// reconciliation names the displaced owner exactly like one confirmed
    /// by the CAS response — the v0 ambiguous-CAS window is closed. `None`
    /// means the consumed record was released or absent, which the release
    /// path already proved durable. Absence of a log record is a proof (the
    /// node never acked past the bucket); a sealed record means recovery
    /// already ran; anything else runs it now.
    pub async fn ensure_recovered(&self, prior: Option<&str>) -> anyhow::Result<()> {
        self.ensure_predecessors_recovered().await?;
        let Some(prior) = prior else {
            return Ok(());
        };
        if prior == self.node {
            return Ok(());
        }
        // ONE rule for every cold path, at the cost of ONE read the core
        // usually already performed: before restoring a cell last owned
        // by node X, X's folded log state must be sealed or absent. The
        // lease record holds at most one session's state — recovery-
        // before-install is what keeps a predecessor's Open state from
        // being replaced unrecovered — so a single GET decides.
        let folded = read_record(&self.bucket, prior).await?;
        let session = folded
            .as_ref()
            .map(|folded| format!("{prior}/{}", folded.wire.generation));
        match log_tier::takeover_gate(folded.as_ref().map(|folded| &folded.record)) {
            log_tier::TakeoverGate::BucketComplete => {}
            log_tier::TakeoverGate::RecoverFirst => {
                self.recover_as(
                    &session.expect("a record implies a session"),
                    RecoverMode::Boot,
                )
                .await?
            }
        }
        Ok(())
    }

    fn shipper_batch_in_flight(shipper: &FleetShipper) -> bool {
        shipper.outstanding.load(Ordering::SeqCst) > 0
    }

    /// The graceful-shutdown drain point: stop fleet acks, wait for the
    /// ticking bundle loop to tier what was shipped, then seal our own
    /// record. The next incarnation finds Sealed and opens a fresh epoch
    /// with no gather at all — without this, a routine restart hands
    /// recovery a whole epoch of already-drained bundles to re-fold.
    /// Best-effort: any failure leaves the record Open, and recovery
    /// does what it always does.
    pub async fn close_gracefully(&self) {
        let shipper = {
            let mut transition = self.live_log.lock().await;
            if !transition.begin_quiescing() {
                return;
            }
            // Quiesce the sink before degrading the shipper. The lifecycle
            // guard makes both decisions visible before any maintenance or
            // first-ack transition can enter its publication section.
            self.closing.store(true, Ordering::SeqCst);
            self.inner.lock().unwrap().clone()
        };
        let Some(shipper) = shipper else { return };
        shipper.degrade("graceful shutdown");
        for _ in 0..50 {
            if self.ltx.all_shipped_tiered() {
                break;
            }
            crate::asyncrt::sleep(std::time::Duration::from_millis(200)).await;
        }
        for _ in 0..30 {
            if !self.flush_in_flight.load(Ordering::SeqCst) {
                break;
            }
            crate::asyncrt::sleep(std::time::Duration::from_millis(100)).await;
        }
        #[cfg(all(test, celld_internal_tests))]
        {
            // Capture the real folded record before the test pause. The
            // final transition deliberately re-reads it under a fresh guard.
            let transition = self.live_log.lock().await;
            let _captured = transition.current();
            drop(transition);
            self.pause_close_transition_for_world().await;
        }
        let transition = self.live_log.lock().await;
        debug_assert!(!transition.is_running());
        // eprintln, not tracing: this runs on the way out of the process,
        // and buffered stdout may never flush before exit.
        let Some(current) = transition.current() else {
            eprintln!("node-log close: no folded log; nothing to seal");
            return;
        };
        let Ok(record) = log_from_wire(&current) else {
            eprintln!("node-log close: folded log unreadable; left as is");
            return;
        };
        let active = current.active;
        // In bundle mode "tiered" includes bundle coverage, but a sealed
        // record tells every future recovery there is nothing to gather —
        // so the seal requires every acked row as a per-cell object. The
        // barrier is the RETAINED BUNDLE SCAN, not a cell counter: the
        // gate's synced_seq is advanced by bundle credits (it must be —
        // it is the ack counter), so the old all_synced_per_cell check
        // never actually demanded the per-cell layout, and the class-A
        // hour sealed 1,300 acked rows into orphanhood behind epoch 156.
        // An Open record is always safe: the next incarnation's recovery
        // drains the bundles.
        let per_cell_complete = self.ltx.all_tails_ready_for_graceful_seal()
            && (!self.bundle_mode
                || match self.uncovered_bundle_rows().await {
                    Ok(uncovered) => uncovered.is_empty(),
                    Err(_) => false,
                });
        if !log_tier::graceful_seal_allowed(
            &record,
            shipper.epoch,
            Self::shipper_batch_in_flight(&shipper),
            per_cell_complete,
        ) {
            eprintln!(
                "node-log close: not sealable (record epoch {} state {:?}, shipper epoch {}); record left open",
                record.epoch, record.state, shipper.epoch
            );
            return;
        }
        let sealed = log_tier::LogRecord {
            state: LogState::Sealed,
            ..record
        };
        match transition.write(Some(log_to_wire(&sealed, active))).await {
            Ok(()) => eprintln!("node-log close: sealed epoch {}", sealed.epoch),
            Err(error) => eprintln!("node-log close: seal not durable: {error:#}"),
        }
    }

    /// Startup: recover every predecessor session's open log — their
    /// acked tails may sit on our old followers or our own staged files,
    /// and nothing may ack against a fresh ensemble until those tails are
    /// in the bucket. Our own session's record cannot exist yet, so this
    /// is ordinary dead-session recovery under other keys.
    pub async fn recover_self(&self) -> anyhow::Result<()> {
        self.ensure_predecessors_recovered().await
    }

    /// Is the fleet tier serving acknowledgements?
    ///
    /// One follower is the floor, not two. Recruitment still takes two, so a
    /// healthy fleet holds three copies; the floor is what the ensemble may
    /// shrink to and keep acknowledging, which is the shape an in-sync replica
    /// set has: Kafka recruits a replication factor and serves while at least
    /// `min.insync.replicas` are in sync, canonically three and two.
    ///
    /// Two copies is where surviving the loss of the leader begins, and the
    /// bucket upload races every fleet acknowledgement regardless. Requiring
    /// two followers put the fast tier one node past the guarantee that
    /// matters, and made a single eviction on a three-node fleet drop every
    /// acknowledgement to the bucket, because the ensemble fell under the
    /// floor with nothing left to recruit.
    /// Say that the fleet posture is requested and not achieved, at most
    /// once every five minutes.
    ///
    /// Silence is the failure that costs the most here. `log ensemble
    /// degraded` fires only when an ensemble that existed was lost, so a node
    /// that never formed one said nothing, and a single-node fleet ran the
    /// default fleet posture on bucket acknowledgements forever with no line
    /// explaining the latency or naming the fix.
    ///
    /// The two cases differ. No live peer is an ordinary single-node fleet and
    /// the message is an invitation: a second node acknowledges from a
    /// follower's disk instead of a storage round trip. Live peers that are
    /// all ineligible is a fault the operator can act on.
    fn report_fleet_shortfall(&self, live_peers: usize, now_ms: u64) {
        const REPORT_INTERVAL_MS: u64 = 300_000;
        let last = match self.shortfall_logged_ms.load(Ordering::SeqCst) {
            0 => None,
            stamp => Some(stamp),
        };
        let Some(shortfall) =
            log_tier::fleet_shortfall(live_peers, now_ms, last, REPORT_INTERVAL_MS)
        else {
            return;
        };
        self.shortfall_logged_ms.store(now_ms, Ordering::SeqCst);
        match shortfall {
            log_tier::Shortfall::NoPeer => info!(
                live_peers,
                "fleet durability requested and no peer is available; writes \
                 wait for the bucket. A second node acknowledges a write when \
                 its follower holds it on disk, which is much faster"
            ),
            log_tier::Shortfall::NoEligiblePeer => warn!(
                live_peers,
                "fleet durability requested and no peer is eligible; writes \
                 wait for the bucket. Every live peer is quarantined for \
                 incapability or has not been recruited yet"
            ),
        }
    }

    pub fn healthy(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|shipper| shipper.is_active() && !shipper.members.is_empty())
    }

    fn shutdown_local_fallback(&self) {
        self.task_stop.request_stop();
        self.closing.store(true, Ordering::SeqCst);
        if let Some(shipper) = self.inner.lock().unwrap().take() {
            shipper.degrade("local durability shutdown");
        }
    }

    /// One ensemble-maintenance pass: if the shipper is absent, degraded,
    /// or under-strength while better peers exist, rebuild it. The order is
    /// the model's reconfiguration discipline, enforced against live
    /// uploads: deactivate (acks fall to the bucket), drain until every
    /// shipped frame is tiered (the force-tier barrier — old fragments
    /// become garbage), CAS the record at the next epoch, then install the
    /// new shipper. A lost CAS leaves us at bucket posture; the next pass
    /// re-reads and retries.
    pub async fn maintain(&self) -> anyhow::Result<()> {
        // Avoid every remote read after local ownership has ended. Shutdown
        // can still race a pass after this check, so publication has a second
        // check under the lifecycle transition below.
        if self.task_stop.is_stopped() {
            return Ok(());
        }
        if self.healthy() {
            return Ok(());
        }
        // Self-suspicion parks recruitment: opening epochs into our own
        // partition churns records and proves nothing. Any successful
        // peer response lifts it.
        if self.suspect_self.load(Ordering::SeqCst) {
            return Ok(());
        }
        let peers = self.ownership.read_capacity_peers().await?;
        let now = crate::ownership_store::now_ms();
        let mono = mono_ms();
        // Kept before the filters consume the vector: a fleet with no peer at
        // all is a different message from a fleet whose peers are all
        // ineligible, and an operator needs to be told which one they have.
        let live_peers = peers
            .iter()
            .filter(|peer| peer.node != self.node && peer.expires_ms > now)
            .count();
        let members: Vec<Member> = peers
            .into_iter()
            .filter(|peer| peer.node != self.node && peer.expires_ms > now)
            // Only a follower that cannot serve appends at all sits out a
            // term. A gray one is recruitable again at once, because the
            // latency rule can judge it again and the swap rate cap is what
            // bounds flapping.
            .filter(|peer| !self.health.lock().unwrap().quarantined(&peer.node, mono))
            // Recruit up to two followers — three copies — while the ensemble
            // may serve on one. Replication factor and the in-sync floor are
            // separate numbers.
            .take(2)
            .map(|peer| Member {
                node: peer.node,
                addr: peer.addr,
            })
            .collect();
        // Peer discovery is intentionally outside the transition guard.
        // Recheck the lifecycle after that await, then keep the snapshot,
        // publication, and successor installation in one critical section.
        let transition = self.live_log.lock().await;
        if !transition.is_running() {
            return Ok(());
        }
        {
            let inner = self.inner.lock().unwrap().clone();
            if let Some(current) = inner {
                // Same strength available: nothing to improve.
                if current.is_active() && members.len() <= current.members.len() {
                    return Ok(());
                }
                // Deactivate first: acks fall to the bucket and pacing
                // stops, so the drain below converges.
                current.degraded.store(true, Ordering::SeqCst);
                // The reconfiguration barrier is a core decision: a batch
                // between capture and credit is invisible to the coverage
                // counters, and every fleet-shipped frame must be
                // bucket-covered before the old fragments become
                // abandonable. Wait it out; the next tick retries.
                if !log_tier::may_reconfigure(
                    current.outstanding.load(Ordering::SeqCst) > 0,
                    self.ltx.all_shipped_tiered(),
                ) {
                    return Ok(());
                }
            }
        }
        if members.is_empty() {
            self.report_fleet_shortfall(live_peers, mono);
            return Ok(());
        }
        if !self.ltx.all_shipped_tiered() {
            return Ok(()); // re-checked: the drain may regress between locks
        }
        let ensemble: BTreeSet<String> = members.iter().map(|member| member.node.clone()).collect();
        // The in-process folded state IS the record (single lease writer):
        // no bucket read, and no CAS-lost re-read loop — the only
        // concurrent mutation is a peer's recovery of our EXPIRED lease,
        // and then our renewals stop applying and write_own_log fails.
        let prior = transition
            .current()
            .as_ref()
            .map(log_from_wire)
            .transpose()?;
        let step = log_tier::maintain_step(prior.as_ref());
        let record = match step {
            log_tier::MaintainStep::CreateFresh => log_tier::create_record(ensemble.clone(), 0)
                .ok_or_else(|| anyhow!("empty ensemble"))?,
            log_tier::MaintainStep::Wait => return Ok(()),
            // v0 tiers per cell, so the record's tiered offset stays 0
            // and the drain barrier above is what makes the old
            // fragments abandonable — the same precondition
            // plan_reconfigure encodes for the bundle tier. The reopen
            // healing pass is deleted with the per-session key: a
            // within-session reopen sits behind the drain barrier (every
            // shipped row tiered), and a new session recovers its
            // predecessors before its first open, so no reopen can strand
            // bundle rows behind a record it never owned.
            log_tier::MaintainStep::Reopen(epoch) => log_tier::LogRecord {
                epoch,
                ensemble: ensemble.clone(),
                tiered: 0,
                state: LogState::Open,
                claimant: None,
                claimed_ms: None,
            },
        };
        // A fresh epoch always opens inactive: `active` flips — through
        // the lease chain — before the first fleet ack of the epoch is
        // credited. The open itself rides an immediate renewal; a failure
        // means our lease is not applying and the posture stays bucket.
        #[cfg(all(test, celld_internal_tests))]
        self.pause_maintenance_publish_for_world().await;
        // Keep this check under the transition guard and immediately before
        // the remote write. A pass stopped before this point cannot publish.
        // A write admitted here can finish after fallback, so dead-session
        // recovery remains responsible for that remote Open record.
        if self.task_stop.is_stopped() {
            return Ok(());
        }
        transition.write(Some(log_to_wire(&record, false))).await?;
        #[cfg(all(test, celld_internal_tests))]
        self.pause_maintenance_install_for_world().await;
        info!(
            epoch = record.epoch,
            members = ?record.ensemble,
            "log ensemble open; fleet acks enabled"
        );
        self.health.lock().unwrap().reset();
        // An unset override selects the adaptive deadline; a set value is
        // the lab's fixed override, and 0 disables hedging entirely.
        let hedge = match crate::env_vars::optional::<u64>("CELLD_LOG_HEDGE_MS")
            .ok()
            .flatten()
        {
            Some(ms) => HedgeMode::Fixed(ms),
            None => HedgeMode::Adaptive,
        };
        // Local shutdown publishes stop before it takes `inner`. Keep the
        // stop recheck and successor installation under that same mutex: an
        // install that wins is removed by shutdown, and an install that loses
        // cannot resurrect a shipper or its member lanes afterward.
        let mut installed = self.inner.lock().unwrap();
        if self.task_stop.is_stopped() {
            return Ok(());
        }
        // The stream window (the stream-window design): a nonzero
        // CELLD_LOG_WINDOW is the appends-per-lane bound and takes over the
        // ship loop's depth; zero keeps the round pipeline unchanged.
        let window = crate::env_vars::optional::<u64>("CELLD_LOG_WINDOW")
            .ok()
            .flatten()
            .unwrap_or(0);
        let byte_cap = crate::env_vars::optional::<u64>("CELLD_LOG_WINDOW_BYTES")
            .ok()
            .flatten()
            .unwrap_or(8 * 1024 * 1024);
        let stream = (window > 0)
            .then(|| Arc::new(StreamWindow::new(window, byte_cap.max(1), members.len())));
        // The ordered stream transport (the ordered-transport design)
        // ships dark: an unset or "http" value keeps the one-shot lanes.
        let stream_transport = crate::env_vars::value("CELLD_LOG_TRANSPORT")
            .ok()
            .flatten()
            .is_some_and(|value| value == "stream");
        let pipeline = if window > 0 {
            window as usize
        } else {
            crate::env_vars::positive_or("CELLD_LOG_PIPELINE", 4_usize).unwrap_or(4)
        };
        let mut lanes = Vec::with_capacity(members.len());
        for (index, member) in members.iter().enumerate() {
            let (send, receive) = tokio::sync::mpsc::unbounded_channel();
            self.child_tasks.spawn_owned(
                "node_log_member_lane",
                member_lane(
                    member.clone(),
                    self.transport.clone(),
                    self.policy.clone(),
                    self.health.clone(),
                    LaneTuning {
                        hedge,
                        stream: stream.clone().map(|window| (window, index)),
                        stream_transport,
                    },
                    receive,
                    self.task_stop.clone(),
                ),
            );
            lanes.push(send);
        }
        *installed = Some(Arc::new(FleetShipper {
            // The wire leader identity IS the session string: followers key
            // fragments and seal marks by it, so a restarted process's
            // appends can never collide with its predecessor's fragments.
            node: self.session.clone(),
            transport: self.transport.clone(),
            record: record.clone(),
            live_log: self.live_log.clone(),
            activated: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            epoch: record.epoch,
            members,
            lanes,
            pipeline,
            seq: std::sync::atomic::AtomicU64::new(0),
            degraded: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            outstanding: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            policy: self.policy.clone(),
            stream,
        }));
        // The successor record and its in-memory shipper form one state
        // transition. Keep shutdown excluded until both values are visible.
        drop(transition);
        Ok(())
    }
}

impl NodeLogManager {
    /// Bundle GC: delete a bundle object once every row it carries is
    /// covered by the per-cell layout. The watermark is what keeps
    /// recovery's whole-prefix gather bounded to the true un-drained
    /// window without an epoch filter that could orphan acked rows — a
    /// bundle is deletable exactly when nothing could ever need to gather
    /// it. Bounded per tick; rows come from the in-memory index when this
    /// incarnation wrote the bundle, one GET otherwise. An unreadable
    /// bundle is never deleted on a guess: it stays, and stays cheap.
    /// Every retained bundle row above its (cell, epoch) per-cell
    /// covered watermark — the rows a sealed record would ORPHAN. The
    /// graceful seal refuses while any exist, and the reopen path folds
    /// them per-cell first (the healing pass). The previous barrier sealed
    /// over exactly this set: `all_synced_per_cell` counted
    /// bundle credits (the gate's counter must), so the old barrier
    /// never actually demanded the per-cell layout.
    pub async fn uncovered_bundle_rows(
        &self,
    ) -> anyhow::Result<BTreeMap<(String, u64, u64), Vec<u8>>> {
        self.uncovered_bundle_rows_for(None).await
    }

    /// The same scan bounded to one cell. The reactivation fold uses it:
    /// the whole-session listing still runs (the in-memory index caps at
    /// 512 bundles, below a churn-heavy tail), but watermark lookups and
    /// payload GETs are paid only for the named cell's rows.
    async fn uncovered_bundle_rows_for(
        &self,
        only_cell: Option<&str>,
    ) -> anyhow::Result<BTreeMap<(String, u64, u64), Vec<u8>>> {
        let prefix = format!("log/{}/bundle/", self.session);
        let mut uncovered: BTreeMap<(String, u64, u64), Vec<u8>> = BTreeMap::new();
        // The index first, GETs only for the misses, and those
        // concurrently: the first cut GET every retained bundle serially
        // and blew systemd's stop budget — the graceful close timed out
        // into SIGKILL and the seal never happened at all.
        // Where one bundle's rows come from: its payload, or the bounded
        // index that already holds them.
        enum Fetched {
            Payload(bytes::Bytes),
            Indexed(Vec<celld_ltx::bundle::BundleRow>),
        }
        let listed = self.bucket.list(&prefix).await?;
        let fetches = listed.into_iter().map(|meta| {
            let key = meta.location.as_ref().to_string();
            let indexed = {
                let index = self.bundle_index.lock().unwrap();
                index
                    .iter()
                    .find(|(indexed, _)| *indexed == key)
                    .map(|(_, rows)| rows.clone())
            };
            let bucket = self.bucket.clone();
            async move {
                // The rows travel WITH the index hit. Looking them up again
                // after the fetch let a rotation out of the bounded index
                // drop the bundle from the scan between the two lookups,
                // and a dropped bundle reads as a bundle with no rows --
                // the same silent gap a failed read used to leave.
                if let Some(rows) = indexed {
                    return anyhow::Ok(Some((key, Fetched::Indexed(rows))));
                }
                match bucket.get(&key).await {
                    Ok(Some((bytes, _))) => Ok(Some((key, Fetched::Payload(bytes)))),
                    // Absent IS an answer: bundle GC deletes a bundle only
                    // once the per-cell layout covers every row it carries,
                    // so a listed-then-absent bundle carries nothing this
                    // scan must report. A read FAILURE is not an answer.
                    // Reporting one as a bundle with no rows is what let the
                    // graceful seal call an unread bundle drained and orphan
                    // its acked rows behind a record that says the bucket is
                    // complete; the callers both fail closed on an error, so
                    // the error has to reach them.
                    Ok(None) => Ok(None),
                    Err(error) => Err(error.context(format!(
                        "read retained bundle {key} for the uncovered-row scan"
                    ))),
                }
            }
        });
        let mut fetched = Vec::new();
        let mut fetches =
            futures_util::stream::iter(fetches).buffer_unordered(COVERAGE_READ_CONCURRENCY);
        while let Some(item) = futures_util::StreamExt::next(&mut fetches).await {
            if let Some(item) = item? {
                fetched.push(item);
            }
        }
        drop(fetches);
        // Bundles whose bytes this pass read and could not use. They are
        // declared together below, before the scan answers, so no caller
        // proceeds past an undeclared loss.
        let mut unreadable: BTreeMap<String, (String, Vec<celld_ltx::bundle::BundleRow>)> =
            BTreeMap::new();
        let mut bundles = Vec::new();
        for (key, item) in fetched {
            let (rows, bytes) = match item {
                Fetched::Payload(bytes) => match celld_ltx::bundle::decode_rows(&bytes) {
                    Ok(rows) => (rows, Some(bytes)),
                    // The envelope is unreadable, so the object no longer
                    // says what it held and the declaration cannot name a
                    // row. Skipping it instead reported a bundle with no
                    // rows, which seals over whatever it carried.
                    Err(error) => {
                        unreadable
                            .entry(key)
                            .or_insert_with(|| (error.to_string(), Vec::new()));
                        continue;
                    }
                },
                Fetched::Indexed(rows) => (rows, None),
            };
            bundles.push((key, rows, bytes));
        }

        // A coverage watermark costs one object-store listing for each LTX
        // level. Awaiting every distinct cell here serialized hundreds of
        // pairs on the process-exit path, after the handoff itself had
        // completed. Collect the exact unique set first and overlap it under
        // the same bound recovery uses. An unbounded fan-out would trade the
        // shutdown delay for a store burst and recreate the recovery storm
        // this barrier exists to avoid.
        let cells = bundles
            .iter()
            .flat_map(|(_, rows, _)| rows.iter().map(|row| (row.cell.clone(), row.cell_epoch)))
            .filter(|(cell, _)| only_cell.is_none_or(|only| only == cell))
            .collect::<BTreeSet<_>>();
        let ltx = &self.ltx;
        let lookups = cells.into_iter().map(|(cell, epoch)| async move {
            let watermark = ltx.covered_txid(&cell, epoch).await;
            ((cell, epoch), watermark)
        });
        let mut lookups =
            futures_util::stream::iter(lookups).buffer_unordered(COVERAGE_READ_CONCURRENCY);
        let mut covered: HashMap<(String, u64), u64> = HashMap::new();
        while let Some((cell, watermark)) = lookups.next().await {
            covered.insert(cell, watermark);
        }
        drop(lookups);

        for (key, rows, mut bytes) in bundles {
            for row in rows {
                if only_cell.is_some_and(|only| only != row.cell) {
                    continue;
                }
                let cache_key = (row.cell.clone(), row.cell_epoch);
                let watermark = covered.get(&cache_key).copied().unwrap_or_default();
                // The twin-gated decision IS the predicate: a row the
                // per-cell layout covers is deletable; anything else is
                // exactly what a seal would orphan. Routing through
                // bundle_deletable keeps this barrier inside the ratchet
                // instead of an inline comparison free to drift again.
                if !log_tier::bundle_deletable([(row.txid, watermark)]) {
                    // Index-hit bundles were not fetched; an uncovered row
                    // forces the one lazy GET its payload needs.
                    if bytes.is_none() {
                        // Same rule as the first pass: absence is an answer,
                        // a failed read is not.
                        bytes = self
                            .bucket
                            .get(&key)
                            .await
                            .with_context(|| {
                                format!("read retained bundle {key} for the uncovered-row scan")
                            })?
                            .map(|(bytes, _)| bytes);
                    }
                    let Some(bytes) = bytes.as_ref() else {
                        continue;
                    };
                    match celld_ltx::bundle::slice(bytes, &row) {
                        Ok(payload) => {
                            uncovered
                                .entry((row.cell.clone(), row.cell_epoch, row.txid))
                                .or_insert_with(|| payload.to_vec());
                        }
                        // The envelope decoded, and this row does not lie
                        // inside the object. The row reached here only
                        // because the per-cell layout does not cover it, so
                        // this is one acknowledged row with no remaining
                        // bucket copy, and the declaration can name it.
                        Err(error) => {
                            let entry = unreadable
                                .entry(key.clone())
                                .or_insert_with(|| (error.to_string(), Vec::new()));
                            entry.1.push(row);
                        }
                    }
                }
            }
        }
        // Declare before answering. A caller that seals or restores on this
        // answer must do it against a durable record of what was lost.
        for (key, (reason, rows)) in unreadable {
            self.declare_bundle_loss(&key, &rows, &reason).await?;
        }
        Ok(uncovered)
    }

    /// Record a retained bundle that this session wrote and cannot read
    /// back.
    ///
    /// The scan reads `log/<session>/bundle/` only, so the writer is this
    /// process running this binary. Bytes that do not decode are therefore
    /// corruption, not a newer envelope this reader does not know, and no
    /// retry and no other reader can do better. That leaves a permanent
    /// refusal or a record. A refusal wedges every activation waiting on
    /// the fold, so the loss becomes a record and the scan continues --
    /// the shape the amnesiac ensemble already uses, and the reason "loss
    /// is a record, never a prompt" is a rule here.
    ///
    /// `rows` names what was lost when one row failed to slice out of an
    /// envelope that decoded. It is empty when the envelope itself was
    /// unreadable, because the object then no longer says what it held.
    ///
    /// The record sits beside `log/<session>/`, never inside its `bundle/`
    /// subtree, so no bundle listing tries to decode a declaration.
    async fn declare_bundle_loss(
        &self,
        key: &str,
        rows: &[celld_ltx::bundle::BundleRow],
        reason: &str,
    ) -> anyhow::Result<()> {
        if !self
            .declared_bundle_losses
            .lock()
            .unwrap()
            .insert(key.to_string())
        {
            return Ok(());
        }
        let name = key.rsplit('/').next().unwrap_or(key);
        let record = format!("log/{}.bundle-{name}.loss.json", self.session);
        let body = serde_json::json!({
            "session": self.session,
            "bundle": key,
            "declared_at_ms": crate::ownership_store::now_ms(),
            "declared_by": self.node,
            "reason": reason,
            "rows": rows
                .iter()
                .map(|row| {
                    serde_json::json!({
                        "cell": row.cell,
                        "cell_epoch": row.cell_epoch,
                        "txid": row.txid,
                    })
                })
                .collect::<Vec<_>>(),
            "note": "a retained bundle this session wrote could not be read \
                     back; an acknowledged row it carried that the per-cell \
                     layout does not cover is unrecovered",
        });
        if let Err(error) = self.bucket.put(&record, serde_json::to_vec(&body)?).await {
            // The scan must not answer until the record is durable, so a
            // failed declaration un-latches and the next pass retries it.
            self.declared_bundle_losses.lock().unwrap().remove(key);
            return Err(error.context(format!("declare the unreadable bundle {key}")));
        }
        warn!(
            bundle = key,
            reason,
            rows = rows.len(),
            "declared a retained bundle this session cannot read back"
        );
        Ok(())
    }

    pub async fn gc_bundles(&self) -> anyhow::Result<()> {
        // 512, not 32: the lab's profiling round found 1,300+ retained
        // bundles — at ~1 bundle/s produced and 32 examined per 30 s tick
        // the backlog only ever grew, and recovery's whole-prefix gather
        // paid for it (89-112 s of a 97-116 s outage). The examined set
        // costs one LIST plus mostly index-hits; the covered_txid cache
        // bounds the per-tick LIST fan-out to the cell count. The TIME
        // budget is the other half: un-indexed bundles cost a GET each,
        // and an unbounded drain pass competed with serving hard enough
        // to gray followers and trigger eviction churn (the ~300 ms
        // bucket-riding window the faceted latency lanes exposed). The
        // pass stops at the budget; the next tick continues where the
        // listing puts it.
        const EXAMINED_PER_TICK: usize = 512;
        const TICK_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);
        let started = mono_ms();
        let prefix = format!("log/{}/bundle/", self.session);
        let mut covered: HashMap<(String, u64), u64> = HashMap::new();
        let mut deletable: Vec<(String, usize)> = Vec::new();
        for meta in self
            .bucket
            .list(&prefix)
            .await?
            .into_iter()
            .take(EXAMINED_PER_TICK)
        {
            if mono_ms().saturating_sub(started) > TICK_BUDGET.as_millis() as u64 {
                break;
            }
            let key = meta.location.as_ref().to_string();
            let indexed = {
                let index = self.bundle_index.lock().unwrap();
                index
                    .iter()
                    .find(|(indexed, _)| *indexed == key)
                    .map(|(_, rows)| rows.clone())
            };
            let rows = match indexed {
                Some(rows) => rows,
                None => {
                    let Ok(Some((bytes, _))) = self.bucket.get(&key).await else {
                        continue;
                    };
                    match celld_ltx::bundle::decode_rows(&bytes) {
                        Ok(rows) => rows,
                        Err(_) => continue,
                    }
                }
            };
            let mut paired = Vec::with_capacity(rows.len());
            for row in &rows {
                let cache_key = (row.cell.clone(), row.cell_epoch);
                let watermark = match covered.get(&cache_key) {
                    Some(watermark) => *watermark,
                    None => {
                        let watermark = self.ltx.covered_txid(&row.cell, row.cell_epoch).await;
                        covered.insert(cache_key, watermark);
                        watermark
                    }
                };
                paired.push((row.txid, watermark));
            }
            if !log_tier::bundle_deletable(paired) {
                continue;
            }
            deletable.push((key, rows.len()));
        }
        if deletable.is_empty() {
            return Ok(());
        }
        // One DeleteObjects request instead of one DELETE per bundle: the
        // lab priced the per-key path at 9k class A operations an hour.
        let keys: Vec<String> = deletable.iter().map(|(key, _)| key.clone()).collect();
        let gone = self.bucket.delete_many(&keys).await;
        if !gone.is_empty() {
            let rows: usize = deletable
                .iter()
                .filter(|(key, _)| gone.contains(key))
                .map(|(_, rows)| rows)
                .sum();
            self.bundle_index
                .lock()
                .unwrap()
                .retain(|(indexed, _)| !gone.contains(indexed));
            info!(
                bundles = gone.len(),
                rows, "bundle GC: drained bundles deleted in one batch"
            );
        }
        Ok(())
    }

    /// Eager recovery ("Recovery is one verb"): every maintenance tick,
    /// sweep `log/` for a foreign, unsealed record whose owner's lease has
    /// expired, and recover it — traffic or none. Lazy-only recovery left
    /// an idle dead owner's un-tiered tail on two follower disks for an
    /// unbounded time; the sweep bounds that exposure at roughly one tick
    /// past lease expiry. Every survivor sweeps; racing recoverers collapse
    /// onto the CAS like every other recovery race.
    pub async fn sweep_dead_leaders(&self) -> anyhow::Result<()> {
        let now = crate::ownership_store::now_ms();
        for meta in self.bucket.list("nodes/").await? {
            let Some(node) = meta
                .location
                .as_ref()
                .strip_prefix("nodes/")
                .and_then(|name| name.strip_suffix(".json"))
                .filter(|name| !name.contains('/'))
                .map(str::to_string)
            else {
                continue;
            };
            if node == self.node {
                continue;
            }
            // One unreadable record must not end the sweep for every node
            // sorted after it.
            let Ok(Some(folded)) = read_record(&self.bucket, &node).await else {
                continue;
            };
            let session = format!("{node}/{}", folded.wire.generation);
            let record = folded.record;
            // Under the fold, the lease we just read IS the record: a
            // session is dead the moment its published expiry passed. A
            // restarted node replaces the record (generation and all)
            // through recovery-before-install, so the sweep never
            // contends with a returning leader; an expired same-
            // generation lease is a process that self-fenced or is about
            // to, and recovery's CAS fences its remaining renewals.
            let dead = folded.wire.expires_ms <= now;
            if record.state == LogState::Sealed {
                // A dead session's sealed subtree is one GC unit: recovery
                // folded every acked row per-cell before the seal, so the
                // retained bundles are garbage, and deleting the record
                // afterwards keeps the invariant — absence means complete,
                // exactly as sealed did. Bundles go FIRST so a session
                // record never vanishes while its subtree still holds
                // objects; loss records are declarations and stay forever.
                // Racing sweepers double-delete idempotently. Without this,
                // log/ grows one record per restart for the fleet's
                // lifetime, and every sweep and takeover LIST scales with
                // restart history.
                if dead && !self.gc_confirmed_empty.lock().unwrap().contains(&session) {
                    match self.gc_sealed_session(&session).await {
                        Ok(empty) => {
                            if empty {
                                self.gc_confirmed_empty.lock().unwrap().insert(session);
                            }
                        }
                        Err(error) => warn!(session, %error, "sealed-session GC failed"),
                    }
                }
                continue;
            }
            if !dead {
                continue;
            }
            info!(session, "eager recovery: dead session with an open log");
            if let Err(error) = self.recover(&session).await {
                warn!(session, %error, "eager node-log recovery failed");
            }
        }
        Ok(())
    }

    /// Delete a dead, sealed session's retained bundles. Under the fold
    /// the record itself lives in the lease and is retired by dead-lease
    /// GC; a sealed session leaves bundles and recovery checkpoints behind.
    /// Recovery folded every acked row per-cell before the seal, so both
    /// subtrees are garbage. Batched: a session can retain hundreds, and
    /// the lab priced one-key-at-a-time GC at 9k class A operations an
    /// hour. A key that fails stays for the next sweep tick.
    async fn gc_sealed_session(&self, session: &str) -> anyhow::Result<bool> {
        let prefix = format!("log/{session}/");
        let keys: Vec<String> = self
            .bucket
            .list(&prefix)
            .await?
            .into_iter()
            .map(|meta| meta.location.as_ref().to_string())
            .filter(|key| {
                key.strip_prefix(&prefix).is_some_and(|tail| {
                    tail.starts_with("bundle/") || tail.starts_with("recovered/")
                })
            })
            .collect();
        if keys.is_empty() {
            return Ok(true);
        }
        let count = keys.len();
        let gone = self.bucket.delete_many(&keys).await;
        if gone.len() != count {
            anyhow::bail!(
                "{} of {count} recovery objects survived the delete; retrying next tick",
                count - gone.len()
            );
        }
        info!(
            session,
            objects = count,
            "sealed session's recovery objects retired"
        );
        Ok(true)
    }
}

/// The maintenance cadence: recruit at startup, repair forever after, and
/// sweep for dead leaders' open logs. Beside it, the fast eviction watch:
/// gray-follower detection cannot wait thirty seconds when one slow fsync
/// tail is every ack's tail, so verdicts poll at a sub-second cadence and
/// an eviction repairs the ensemble immediately.
fn spawn_maintenance(
    manager: Arc<NodeLogManager>,
    stop: crate::ltx_repl::StopToken,
    roots: &crate::ltx_repl::TaskGroup,
) {
    let maintenance_manager = Arc::downgrade(&manager);
    let watcher = Arc::downgrade(&manager);
    let maintenance_stop = stop.clone();
    roots.spawn_owned("node_log_maintenance", async move {
        let mut tick = crate::asyncrt::interval(std::time::Duration::from_secs(30));
        tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
        loop {
            crate::asyncrt::select_biased! {
                "a stop signal that ties the maintenance tick prevents another maintenance pass";
                _ = maintenance_stop.stopped() => break,
                _ = tick.tick() => {},
            }
            let Some(manager) = maintenance_manager.upgrade() else {
                break;
            };
            // The watchdog for #490: on the 2026-08-28 fleet this loop's
            // heartbeat stopped mid-pass and never returned — no error, no
            // panic, no recovery line — and the silence cost the diagnosis.
            // Every phase must end; a phase that outlives the warning
            // budget names itself while it is still stuck, so the next
            // occurrence is a one-line diagnosis plus a stack, not an
            // archaeology session. The pass still runs to completion —
            // this observes, it does not cancel: an aborted maintenance
            // phase could orphan a half-installed ensemble.
            for (phase, work) in [
                ("maintain", manager.maintain().boxed()),
                ("dead-leader sweep", manager.sweep_dead_leaders().boxed()),
                ("bundle GC", manager.gc_bundles().boxed()),
            ] {
                let started = mono_ms();
                let mut work = work;
                loop {
                    match crate::asyncrt::timeout(std::time::Duration::from_secs(60), &mut work)
                        .await
                    {
                        Ok(Ok(())) => break,
                        Ok(Err(error)) => {
                            warn!(%error, phase, "log maintenance phase failed");
                            break;
                        }
                        Err(_) => {
                            warn!(
                                phase,
                                stuck_ms = mono_ms().saturating_sub(started),
                                "log maintenance phase has not returned; \
                                 the ticker is blocked behind it (#490)"
                            );
                        }
                    }
                }
            }
        }
    });
    roots.spawn_owned("node_log_posture_watch", async move {
        let mut tick = crate::asyncrt::interval(std::time::Duration::from_millis(250));
        tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
        let mut repair_since: Option<u64> = None;
        let mut last_repair_try = mono_ms().saturating_sub(2_000);
        let mut repair_interval = std::time::Duration::from_secs(1);
        loop {
            crate::asyncrt::select_biased! {
                "a stop signal that ties the posture tick prevents another repair attempt";
                _ = stop.stopped() => break,
                _ = tick.tick() => {},
            }
            let Some(watcher) = watcher.upgrade() else {
                break;
            };
            watcher.probe_followers();
            watcher.evict_gray_followers();
            // Posture repair does not wait for the 30 s maintenance tick:
            // while the shipper is absent or degraded, retry about once a
            // second — the first attempt after an eviction usually loses
            // to the drain barrier, and E8's first light measured a 22 s
            // hole where nothing retried until the tick. The repair
            // latency line is the E8 recruit-repair metric.
            if watcher.healthy() {
                if let Some(since) = repair_since.take() {
                    info!(
                        event = "posture_repair",
                        degraded_ms = mono_ms().saturating_sub(since),
                        "fleet posture repaired"
                    );
                }
                repair_interval = std::time::Duration::from_secs(1);
                continue;
            }
            repair_since.get_or_insert_with(mono_ms);
            // The first repair after a degrade is immediate — that is the
            // 0.7s swap — but successive failed repairs back off to the
            // swap rate cap: a full peer partition once opened seventeen
            // doomed epochs in twenty seconds at the flat retry rate.
            if mono_ms().saturating_sub(last_repair_try) >= repair_interval.as_millis() as u64 {
                last_repair_try = mono_ms();
                let epoch_before = crate::ltx_repl::Shipper::epoch(&*watcher);
                if let Err(error) = watcher.maintain().await {
                    warn!(%error, "posture repair failed");
                }
                let stepped = crate::ltx_repl::Shipper::epoch(&*watcher) != epoch_before;
                repair_interval = if watcher.healthy() {
                    std::time::Duration::from_secs(1)
                } else if stepped {
                    (repair_interval * 2).min(std::time::Duration::from_secs(10))
                } else {
                    std::time::Duration::from_secs(1)
                };
            }
        }
    });
}

/// The follower-side fragment GC: a fragment whose epoch the record has
/// moved past (a reconfiguration or a reopened incarnation), or whose
/// epoch's record is Sealed (recovery certified and uploaded the tail), is
/// garbage no gather will ever consult — without this sweep a follower
/// that is never re-recruited keeps one closed epoch's fragments per
/// leader forever. The seal mark is preserved and extended: a closed
/// epoch is refused from here on, which is also what makes the deletion
/// safe against any straggling append.
fn spawn_fragment_gc(
    store: Arc<FollowerStore>,
    stop: crate::ltx_repl::StopToken,
    tasks: &crate::ltx_repl::TaskGroup,
) {
    tasks.spawn_owned("follower_fragment_gc", async move {
        let mut tick = crate::asyncrt::interval(std::time::Duration::from_secs(600));
        tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
        loop {
            crate::asyncrt::select_biased! {
                "a stop signal that ties the fragment-GC tick prevents another deletion pass";
                _ = stop.stopped() => break,
                _ = tick.tick() => {},
            }
            store.gc_fragments().await;
        }
    });
}

impl crate::ltx_repl::BundleSink for NodeLogManager {
    /// One object per node-flush: `log/<node>/bundle/e<epoch>-<seq>.ltxb`,
    /// verbatim L0 segments plus the footer (`crate::bundle`). Keys are
    /// unique per (epoch, seq), so the PUT needs no condition; the epoch in
    /// the key scopes recovery's gather and the eventual GC sweep.
    fn put_bundle<'a>(
        &'a self,
        entries: Vec<celld_ltx::bundle::BundleEntry>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            struct FlushGuard<'a>(&'a std::sync::atomic::AtomicBool);
            impl Drop for FlushGuard<'_> {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::SeqCst);
                }
            }
            self.flush_in_flight.store(true, Ordering::SeqCst);
            let _flush = FlushGuard(&self.flush_in_flight);
            let (epoch, flusher) = {
                let inner = self.inner.lock().unwrap();
                match inner.as_ref() {
                    Some(shipper) => (shipper.epoch, shipper.clone()),
                    None => return false,
                }
            };
            let seq = self.bundle_seq.fetch_add(1, Ordering::SeqCst);
            let body = match celld_ltx::bundle::encode(&entries) {
                Ok(body) => body,
                Err(error) => {
                    warn!(%error, "bundle encode failed");
                    return false;
                }
            };
            let rows = match celld_ltx::bundle::decode_rows(&body) {
                Ok(rows) => rows,
                Err(error) => {
                    warn!(%error, "bundle self-decode failed");
                    return false;
                }
            };
            let key = format!("log/{}/bundle/e{epoch}-{seq:08}.ltxb", self.session);
            match self.bucket.put(&key, body).await {
                Ok(()) => {
                    // The credit check: the PUT is unconditional, so by
                    // itself it proves nothing to the ack path — a healed
                    // zombie whose record a recoverer already sealed can
                    // still land bundle objects, and crediting them would
                    // ack rows no future takeover reads (the sealed record
                    // says the bucket is complete, and takeovers read
                    // per-cell prefixes). Durability credits only if the
                    // record is still Open at this shipper's epoch AFTER
                    // the PUT: any recovery that fences later must list
                    // after this PUT completed and therefore gathers it.
                    // A BUCKET read on purpose: the hazard is a peer's
                    // recovery CAS fencing this record, which the
                    // in-process copy cannot see.
                    let credit = log_tier::bundle_credit_allowed(
                        read_record(&self.bucket, &self.session)
                            .await
                            .ok()
                            .flatten()
                            .as_ref()
                            .map(|folded| &folded.record),
                        epoch,
                    );
                    if !credit {
                        // Degrade the shipper that OWNED this flush's
                        // epoch, not whichever is installed now: a flush
                        // racing a legitimate reconfiguration must not
                        // poison the successor ensemble it knows nothing
                        // about. For a true zombie the flusher IS the
                        // installed shipper and stops exactly as before;
                        // for a swap race this degrades a retired object,
                        // which is the correct no-op — the lab measured
                        // the alternative as an epoch-churn loop.
                        flusher.degrade("record moved under a bundle flush");
                        warn!(key, "bundle flush not credited: record moved");
                        return false;
                    }
                    let mut index = self.bundle_index.lock().unwrap();
                    index.push_back((key, rows));
                    // Bounded: older bundles are compacted past or folded
                    // by drains; the cap only limits the overlay's view.
                    while index.len() > 512 {
                        index.pop_front();
                    }
                    true
                }
                Err(error) => {
                    warn!(%error, key, "bundle put failed");
                    false
                }
            }
        })
    }

    fn active(&self) -> bool {
        // Draining rides the bundle path even while the shipper is
        // DEGRADED: degrade stops fleet proofs, not tiering. The credit
        // check against the record is the safety gate (a sealed or
        // stepped record refuses the credit), and demoting the
        // post-eviction drain to sequential per-cell PUTs was measured at
        // 57 s of bucket-posture acks under load — where the design
        // promised one flush. The one exception is the shutdown latch:
        // once the graceful close begins its seal scan, a new flush could
        // credit rows the scan never saw, so `closing` quiesces the sink
        // and late writes ride per-cell acks instead.
        self.bundle_mode
            && !self.closing.load(Ordering::SeqCst)
            && self.inner.lock().unwrap().is_some()
    }

    fn rows_for(&self, cell: &str, epoch: u64) -> Vec<celld_ltx::LocatedRow> {
        let index = self.bundle_index.lock().unwrap();
        index
            .iter()
            .flat_map(|(key, rows)| {
                rows.iter()
                    .filter(|row| row.cell == cell && row.cell_epoch == epoch)
                    .map(|row| celld_ltx::LocatedRow {
                        source: key.clone(),
                        row: row.clone(),
                    })
            })
            .collect()
    }

    fn fetch_bundle<'a>(
        &'a self,
        source: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<u8>>> + Send + 'a>>
    {
        Box::pin(async move {
            {
                let cache = self.bundle_cache.lock().await;
                if let Some((key, bytes)) = cache.as_ref() {
                    if key == source {
                        return Ok(bytes.as_ref().clone());
                    }
                }
            }
            let Some((bytes, _)) = self.bucket.get(source).await? else {
                anyhow::bail!("bundle {source} vanished");
            };
            let bytes: Vec<u8> = bytes.to_vec();
            *self.bundle_cache.lock().await = Some((source.to_string(), Arc::new(bytes.clone())));
            Ok(bytes)
        })
    }

    /// Recovery's gather for one cell, run by the successor of a quiet
    /// ending before its restore. The tail can live in two places: this
    /// node's own retained bundles, and the live members' fragments — a
    /// fleet ack proves follower fsync, not a bundle flush, so the member
    /// gather is the one the field incident needed. No seal: the leader
    /// is alive, the cell's epoch is closed, and rows at or below the ack
    /// are immutable, so this reads exactly what a recovery of this
    /// session would gather. A member that cannot answer fails the fold,
    /// and the caller then fails the activation — restoring past a
    /// partial gather would serve a truncated database as read-write.
    /// `upload_gathered` skips rows the per-cell watermark covers and
    /// merges the contiguous tail into one object, so a re-run after a
    /// partial failure repeats no upload.
    fn fold_cell<'a>(
        &'a self,
        cell: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let mut gathered = self.uncovered_bundle_rows_for(Some(cell)).await?;
            let record = read_record(&self.bucket, &self.session)
                .await?
                .map(|folded| folded.record);
            if let Some(record) = record {
                for member in &record.ensemble {
                    let lease = self.ownership.read_node_lease(member).await?;
                    let addr = lease
                        .map(|lease| lease.addr)
                        .ok_or_else(|| anyhow!("fold member {member} has no lease"))?;
                    let tail = self
                        .post_tail(
                            member,
                            &addr,
                            &TailReq {
                                leader: self.session.clone(),
                            },
                        )
                        .await
                        .map_err(|error| anyhow!("fold tail from {member}: {error}"))?;
                    for entry in tail.entries {
                        if entry.cell != cell {
                            continue;
                        }
                        gathered
                            .entry((entry.cell, entry.cell_epoch, entry.txid))
                            .or_insert(entry.bytes);
                    }
                }
            }
            if gathered.is_empty() {
                return Ok(());
            }
            let uploaded = self.upload_gathered(gathered, None).await?;
            info!(
                cell,
                uploaded, "folded a quietly stranded tail before reactivation"
            );
            Ok(())
        })
    }
}

#[cfg(all(test, celld_internal_tests))]
include!(env!("CELLD_INTERNAL_NODE_LOG_OBSERVERS"));
