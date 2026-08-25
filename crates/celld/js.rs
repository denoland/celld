// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The raw V8 adapter and its child modules remain outside the World. The
// Actor-reachable wake and WebSocket state is injected through HostServices.
#![allow(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]

//! The JS engine: rusty_v8 directly. One isolate per cell.
//!
//! This slice runs actual Durable Objects: the worker's default-export `fetch`
//! receives an `env` whose bindings are DO namespaces; `env.NS.get(id).fetch()`
//! instantiates the exported DO class (once per id) with a `state` whose
//! `storage` is backed by the cell's SQLite (crate::storage). DO storage is
//! async in JS, synchronous underneath — the ops are sync Rust, wrapped in
//! `async` by the JS harness.
use crate::asyncrt;
use crate::storage;
use anyhow::{anyhow, Context as _, Result};
use futures_util::StreamExt as _;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use v8::{ValueDeserializerHelper, ValueSerializerHelper};

/// Bare Node builtin specifiers that the bundler leaves for the runtime.
/// Root entries also match subpaths in esbuild and in module resolution.
pub(crate) const BARE_NODE_BUILTINS: &[&str] = &[
    "assert",
    "async_hooks",
    "buffer",
    "child_process",
    "cluster",
    "constants",
    "crypto",
    "dgram",
    "diagnostics_channel",
    "dns",
    "events",
    "fs",
    "fs/promises",
    "http",
    "http2",
    "https",
    "inspector",
    "module",
    "net",
    "os",
    "path",
    "perf_hooks",
    "process",
    "punycode",
    "querystring",
    "readline",
    "sqlite",
    "stream",
    "string_decoder",
    "timers",
    "tls",
    "tty",
    "url",
    "util",
    "util/types",
    "v8",
    "vm",
    "worker_threads",
    "zlib",
];

/// The pending exception text from a TryCatch scope (a macro so it needs no
/// bound on the crate-private scope traits).
macro_rules! exc {
    ($tc:expr) => {
        $tc.exception()
            .map(|e| e.to_rust_string_lossy(&*$tc))
            .unwrap_or_else(|| "<none>".into())
    };
}

/// A cross-node dispatch: the isolate hit `env.NS.get(id)` for a cell this node
/// does not own. The op hands this to the tokio runtime, which resolves the
/// owner and HTTP-proxies the fetch, replying on `reply` (an async oneshot the
/// async-op future awaits — the JS thread is never blocked).
pub struct DoCallReq {
    pub request_id: Option<RequestId>,
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    /// An explicit JavaScript AbortSignal must reach the target handler, even
    /// when it fires before cold routing completes. Transport cancellation has
    /// no handler contract and can stop the cold route immediately.
    pub deliver_abort_to_handler: bool,
    pub scope: String,
    pub name: Option<String>,
    pub url: String,
    pub method: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    /// Where this call sits in its caller's order for this cell.
    pub order: Option<CallOrder>,
    /// The dispatching handler's trace context, read from CPED at the
    /// call site, so the cell's span joins the caller's trace.
    pub parent: Option<crate::telemetry::TraceIds>,
}
static DO_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<DoCallReq>> = OnceLock::new();

/// Delivery order, per caller and per cell.
///
/// Two calls a script makes back-to-back on one Durable Object stub reach
/// the cell in that order. Workerd guarantees it — one pipe per stub — and
/// celld did too, while a cell's events came off one channel. Nothing
/// between the call and the cell keeps it now: the caller's two ops race to
/// the proxy channel, the proxy polls them in a `FuturesUnordered`, and two
/// drives race for the isolate. So the order is taken where it is still
/// true, synchronously in `op_do_call_impl`, and carried to the one place
/// that decides when an event is delivered.
///
/// It is a chain, not a counter: each call leaves behind the receiver its
/// successor waits on, and takes its predecessor's.
///
/// **The chain lives on the caller.** It was a process-wide map keyed by
/// `(context, cell)`, which made every Durable Object call in the node take
/// one global lock to record something only its own caller could ever read.
/// A shared mutex on a per-request path is a scalability cliff that a small
/// machine cannot show you, and the state was never shared to begin with.
///
/// **Local delivery only.** A call routed to another node crosses HTTP,
/// where this order has nowhere to ride, so it is dropped there. Workerd
/// has the same seam in a different place, and a cell that moves mid-flight
/// reorders under both.
pub struct CallOrder {
    /// The call before this one, or `None` if this is the first.
    ahead: Option<tokio::sync::oneshot::Receiver<Place>>,
    /// What the call behind this one waits on.
    release: Option<tokio::sync::oneshot::Sender<Place>>,
    /// The caller that owns the chain, and this call's place in it.
    caller: Arc<IoContext>,
    cell: String,
    seq: u64,
    delivered: bool,
}

/// What a call hands to the one behind it.
///
/// `None` once this call has been delivered: the queue has moved on. `Some`
/// when it died on the way to a cell and never took its turn, and then it
/// is this call's own unfinished place — so the call behind waits for what
/// this one was waiting for instead of jumping the queue.
///
/// Without the handoff a failed call releases its successor immediately,
/// and a third call can be delivered ahead of a first that is still in
/// flight. Rare, since it needs a call to fail between two that do not, and
/// silent, since every call involved still gets an answer.
struct Place(Option<Box<tokio::sync::oneshot::Receiver<Place>>>);

impl CallOrder {
    /// Wait for every call before this one to be delivered.
    ///
    /// A loop rather than one await because a call that died hands back
    /// what *it* was waiting for. `Err` is the chain ahead going away
    /// entirely, which is nothing left to wait for.
    pub async fn wait(&mut self) {
        let mut ahead = self.ahead.take();
        while let Some(place) = ahead {
            ahead = match place.await {
                Ok(Place(forward)) => forward.map(|boxed| *boxed),
                Err(_) => None,
            };
        }
    }

    /// This call has been delivered; the one behind it may go.
    pub fn delivered(&mut self) {
        self.delivered = true;
    }
}

impl Drop for CallOrder {
    fn drop(&mut self) {
        // Nothing followed this call, so the chain is empty and its tail is
        // only a leak. A caller's chains outlive its calls otherwise, and a
        // cell isolate's default caller outlives everything.
        let mut chains = self.caller.call_chains.lock().unwrap();
        if chains
            .tails
            .get(&self.cell)
            .is_some_and(|(seq, _)| *seq == self.seq)
        {
            chains.tails.remove(&self.cell);
        }
        drop(chains);
        if let Some(release) = self.release.take() {
            let forward = (!self.delivered).then(|| self.ahead.take()).flatten();
            let _ = release.send(Place(forward.map(Box::new)));
        }
    }
}

/// One caller's chains, one per cell it has called.
#[derive(Default)]
struct CallChains {
    next_seq: u64,
    tails: HashMap<String, (u64, tokio::sync::oneshot::Receiver<Place>)>,
}

/// Take this call's place in its caller's chain for `cell`. Synchronous, so
/// the places are taken in the order the script made the calls.
fn enter_call_order(caller: Arc<IoContext>, cell: &str) -> CallOrder {
    let (release, next) = tokio::sync::oneshot::channel();
    let (seq, ahead) = {
        let mut chains = caller.call_chains.lock().unwrap();
        chains.next_seq += 1;
        let seq = chains.next_seq;
        let ahead = chains
            .tails
            .insert(cell.to_string(), (seq, next))
            .map(|(_, ahead)| ahead);
        (seq, ahead)
    };
    CallOrder {
        ahead,
        release: Some(release),
        caller,
        cell: cell.to_string(),
        seq,
        delivered: false,
    }
}

/// An output-gate request for the co-hosted (same-isolate) Durable Object fast
/// path. That path runs a resident DO's `fetch` inline, bypassing
/// `dispatch_do_call` where the gate lives; the inline handler samples the
/// cell's committed-write position and asks the actor whether the response can
/// leave. `position` is present for a write and absent for a read.
pub struct GateReq {
    pub scope: String,
    pub position: Option<u64>,
    pub reply: tokio::sync::oneshot::Sender<Result<(), celld_logic::RequestError>>,
}
static GATE_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<GateReq>> = OnceLock::new();
pub fn set_gate_tx(tx: tokio::sync::mpsc::UnboundedSender<GateReq>) {
    let _ = GATE_TX.set(tx);
}

/// A service-binding call: `env.NAME.fetch()`. Unlike a Durable Object call
/// there is no identity to resolve — any isolate running `script` will do — so
/// the runtime hands this straight to that script's stateless isolate pool.
pub struct SvcCallReq {
    /// Fires when the caller's request signal aborts, so the router stops
    /// waiting on the target instead of leaving the call outstanding.
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    pub script: String,
    pub url: String,
    pub method: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
}
static SVC_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<SvcCallReq>> = OnceLock::new();

/// A Worker assets-binding call. The script name selects the immutable asset
/// index loaded for that Worker; unlike ingress this never falls back into the
/// Worker and therefore cannot recurse.
pub struct AssetCallReq {
    pub script: String,
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
}
static ASSET_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<AssetCallReq>> = OnceLock::new();

/// An RPC call on a named `WorkerEntrypoint` of another script. Arguments and
/// the result cross as V8 structured-clone bytes.
pub struct SvcRpcReq {
    pub script: String,
    pub entrypoint: String,
    pub method: String,
    pub args: Vec<u8>,
    pub reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
}
static SVC_RPC_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<SvcRpcReq>> = OnceLock::new();
pub fn set_svc_rpc_tx(tx: tokio::sync::mpsc::UnboundedSender<SvcRpcReq>) {
    let _ = SVC_RPC_TX.set(tx);
}
pub fn set_svc_call_tx(tx: tokio::sync::mpsc::UnboundedSender<SvcCallReq>) {
    let _ = SVC_CALL_TX.set(tx);
}
pub fn set_asset_call_tx(tx: tokio::sync::mpsc::UnboundedSender<AssetCallReq>) {
    let _ = ASSET_CALL_TX.set(tx);
}
pub fn set_do_call_tx(tx: tokio::sync::mpsc::UnboundedSender<DoCallReq>) {
    let _ = DO_CALL_TX.set(tx);
}

/// Hand a Durable Object call to the dispatcher. Public HTTP ingress uses the
/// same queue a Worker's `env.NAME.get(id).fetch()` does, so both resolve
/// ownership, forward, and redispatch by one policy rather than two.
#[must_use]
pub fn submit_do_call(call: DoCallReq) -> bool {
    DO_CALL_TX.get().is_some_and(|tx| tx.send(call).is_ok())
}

/// The arm-time wake-entry gate, output-gate style (matching Durable
/// Objects): `setAlarm()` resolves optimistically on the committed local
/// write — it never yields the cell's event scheduling to remote I/O — and
/// the cell's response edge is withheld until every pending wake-entry PUT
/// has landed. Invariant: an arm the caller has OBSERVED acknowledged
/// (received the response) is covered by a durable entry.
pub struct ArmGate {
    pub bucket: crate::bucket::Bucket,
    pub flusher: Arc<crate::wake::WakeFlusher>,
}

type ArmGateRx = tokio::sync::oneshot::Receiver<Result<(), String>>;

#[derive(Default)]
pub(crate) struct WakeEntryService {
    gate: OnceLock<ArmGate>,
    pending: Mutex<HashMap<String, Vec<ArmGateRx>>>,
}

pub fn set_arm_gate(gate: ArmGate) {
    let _ = asyncrt::services().wake_entry().gate.set(gate);
}

/// Whether this node still holds a wake entry for `cell`.
pub fn wake_entry_tracked(cell: &str) -> bool {
    asyncrt::services()
        .wake_entry()
        .gate
        .get()
        .is_some_and(|gate| gate.flusher.tracks(cell))
}

/// Drop this node's belief about `cell`'s wake entry — the cell fenced or
/// resolved to a remote owner, so the entry is no longer ours to manage.
/// Defined since the wake audit and now actually called (it was dead code,
/// which meant a node that lost a cell kept believing in its entry).
pub fn forget_wake_entry(cell: &str) {
    let services = asyncrt::services();
    if let Some(gate) = services.wake_entry().gate.get() {
        gate.flusher.forget(cell);
    }
}

/// Adopt the wake entry a restored alarm implies, so consuming that alarm
/// deletes it rather than orphaning it.
pub fn adopt_wake_entry(cell: &str, at_ms: i64) {
    let services = asyncrt::services();
    if let Some(gate) = services.wake_entry().gate.get() {
        gate.flusher.adopt(cell, at_ms);
    }
}

/// Bring the bucket's wake entry for `cell` into line with its alarm.
///
/// Arming writes an entry; something has to take it away again once the alarm
/// has been consumed, or the entry outlives its alarm and every later due scan
/// finds it and wakes a cell with nothing to do. `consume_durable` gates that
/// final delete on the consuming commit being replicated -- removing the hint
/// while the commit that consumed the alarm is still only local would lose
/// both the alarm and the record that could have revived it.
pub async fn reconcile_wake_entry(cell: &str, next_alarm_ms: i64, consume_durable: bool) {
    let services = asyncrt::services();
    let Some(gate) = services.wake_entry().gate.get() else {
        return;
    };
    gate.flusher
        .reconcile(&gate.bucket, cell, next_alarm_ms, consume_durable)
        .await;
}

/// A committed alarm tightened the durable wake bound: launch the entry PUT
/// and register it against the cell's output gate. No-op when the bound
/// already covers it or no gate is configured (unit tests).
fn spawn_arm_gate(cell: &str, at_ms: i64) {
    #[cfg(all(test, celld_internal_tests))]
    if asyncrt::sabotage_active(crate::host_services::EngineSabotage::SkipArmTimeWakePut) {
        return;
    }
    let services = asyncrt::services();
    let Some(gate) = services.wake_entry().gate.get() else {
        return;
    };
    let Some(celld_logic::wake::Op::Put { key, due_ms }) = gate.flusher.arm_op(cell, at_ms) else {
        return;
    };
    let cell_ = cell.to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let mut pending = services.wake_entry().pending.lock().unwrap();
        let gates = pending.entry(cell_.clone()).or_default();
        // Arms with no response edge to drain them (alarm handlers,
        // WebSocket events) must not accumulate: drop already-settled gates.
        gates.retain_mut(|gate| {
            matches!(
                gate.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            )
        });
        gates.push(rx);
    }
    asyncrt::spawn(async move {
        let gate = services.wake_entry().gate.get().unwrap();
        // A delete of this exact key may already be on the wire — the tracked
        // entry's consume-delete, or the move-delete of a key this arm is
        // about to re-PUT. Either way the PUT must land after that delete,
        // not race it (S3 orders concurrent same-key writes arbitrarily).
        // Deletes of the cell's OTHER keys cannot touch this PUT, and waiting
        // on them would hold the response behind unrelated store latency.
        gate.flusher.await_key_deletable(&cell_, &key).await;
        let body = format!("{{\"cell\":{cell_:?},\"due_ms\":{due_ms}}}");
        let result = gate
            .bucket
            .put(&key, body.into_bytes())
            .await
            .map(|_| gate.flusher.confirm_arm(&cell_, due_ms, key))
            .map_err(|e| format!("setAlarm wake entry: {e}"));
        let _ = tx.send(result);
    })
    .detach();
}

#[cfg(all(test, celld_internal_tests))]
pub(crate) fn spawn_arm_gate_for_test(cell: &str, at_ms: i64) {
    spawn_arm_gate(cell, at_ms);
}

/// Await the cell's pending wake-entry PUTs — the output-gate drain a
/// response edge performs before releasing its reply. An error means an arm
/// the response would acknowledge is not durably covered; the caller must
/// fail the response rather than lie (the flusher retries the entry).
pub async fn drain_arm_gates(cell: &str) -> Result<(), String> {
    let services = asyncrt::services();
    let gates = services.wake_entry().pending.lock().unwrap().remove(cell);
    let Some(gates) = gates else { return Ok(()) };
    for gate in gates {
        match gate.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err("wake-entry gate task dropped".into()),
        }
    }
    Ok(())
}

pub type RequestId = u128;

#[derive(Clone, Copy)]
pub struct FetchRequest<'a> {
    pub url: &'a str,
    pub method: &'a str,
    pub body: &'a [u8],
    pub headers: &'a [(String, String)],
    pub request_id: Option<RequestId>,
}

/// Allocate an id for an ingress or service-binding request so it can be
/// aborted mid-flight.
pub fn next_request_id() -> RequestId {
    next_do_request_id()
}

static NEXT_DO_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static DO_REQUEST_PROCESS_PREFIX: OnceLock<u64> = OnceLock::new();
static DO_CALL_CANCELS: OnceLock<
    std::sync::Mutex<HashMap<RequestId, tokio::sync::oneshot::Sender<()>>>,
> = OnceLock::new();
fn do_call_cancels(
) -> &'static std::sync::Mutex<HashMap<RequestId, tokio::sync::oneshot::Sender<()>>> {
    DO_CALL_CANCELS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn next_do_request_id() -> RequestId {
    let prefix = *DO_REQUEST_PROCESS_PREFIX.get_or_init(|| {
        let mut bytes = [0; 8];
        getrandom::fill(&mut bytes).expect("OS random source unavailable");
        u64::from_ne_bytes(bytes)
    });
    let sequence = NEXT_DO_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    (u128::from(prefix) << 64) | u128::from(sequence)
}

pub fn request_id_string(request_id: RequestId) -> String {
    format!("{request_id:032x}")
}

pub fn parse_request_id(value: &str) -> Option<RequestId> {
    u128::from_str_radix(value, 16).ok()
}

struct DoCallCancelGuard(Option<RequestId>);

impl DoCallCancelGuard {
    fn new(request: RequestId) -> Self {
        Self(Some(request))
    }

    fn disarm(&mut self) {
        if let Some(request) = self.0.take() {
            do_call_cancels().lock().unwrap().remove(&request);
        }
    }
}

impl Drop for DoCallCancelGuard {
    fn drop(&mut self) {
        let Some(request) = self.0.take() else {
            return;
        };
        if let Some(cancel) = do_call_cancels().lock().unwrap().remove(&request) {
            let _ = cancel.send(());
        }
    }
}

/// An RPC payload crossing the host boundary. JS stubs marshal by V8
/// structured clone (`V8`); the legacy cross-node JSON envelope and the test
/// harness use `Json`. `__dispatchRpc` answers in the flavor it was asked in.
pub enum RpcData {
    Json(String),
    V8(Vec<u8>),
}

/// A native Durable Object RPC call (`stub.someMethod(...args)`).
/// Routing/activation is identical to fetch calls.
pub struct RpcCallReq {
    pub scope: String,
    pub name: Option<String>,
    pub method: String,
    pub args: RpcData,
    pub reply: tokio::sync::oneshot::Sender<Result<RpcData>>,
}
static RPC_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<RpcCallReq>> = OnceLock::new();
pub fn set_rpc_call_tx(tx: tokio::sync::mpsc::UnboundedSender<RpcCallReq>) {
    let _ = RPC_CALL_TX.set(tx);
}

pub struct OutboundWsReq {
    pub scope: String,
    pub id: u64,
    pub url: String,
    pub protocols: Vec<String>,
    /// Present for an isolate-polled (Worker) socket. Created and registered
    /// on the JS thread before the request is sent, so `__ws_next` can never
    /// run ahead of its own queue.
    pub pull: Option<WsPullSender>,
    /// Extra request headers, for the `fetch()` upgrade form.
    pub headers: Vec<(String, String)>,
    /// A `fetch()` upgrade wants the whole handshake outcome, including the
    /// ordinary response a server that declines to upgrade sent instead.
    pub want_response: bool,
    /// A socket already upgraded in this process, which this request joins
    /// instead of dialing `url`. It is the cell end of a Durable Object
    /// subrequest whose caller kept the client end, so there is no handshake
    /// to run and no connection to open: the host only has to carry frames
    /// between two isolates.
    pub target: Option<WsTarget>,
    pub reply: tokio::sync::oneshot::Sender<Result<OutboundWsOpen>>,
}

/// The ordinary HTTP response a server sent instead of upgrading. `fetch()`
/// returns it verbatim rather than turning it into a connection error.
pub struct DeclinedUpgrade {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// What an outbound handshake produced.
pub struct OutboundWsOpen {
    pub protocol: Option<String>,
    pub declined: Option<DeclinedUpgrade>,
}
static OUTBOUND_WS_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<OutboundWsReq>> =
    OnceLock::new();
pub fn set_outbound_ws_tx(tx: tokio::sync::mpsc::UnboundedSender<OutboundWsReq>) {
    let _ = OUTBOUND_WS_TX.set(tx);
}

static NEXT_TIMER_ID: AtomicU64 = AtomicU64::new(1);
static TIMER_CANCELS: OnceLock<std::sync::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<()>>>> =
    OnceLock::new();
fn timer_cancels() -> &'static std::sync::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<()>>> {
    TIMER_CANCELS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}
static NEXT_HTTP_STREAM_ID: AtomicU64 = AtomicU64::new(1);
/// What `__http_stream_read` resolves with at end of stream.
///
/// The reader identifies the end by type, not by value. A chunk always
/// resolves as a `Uint8Array`, therefore body bytes can never look like
/// this marker. The value stays distinctive, so a reader that does compare
/// the value cannot match a plausible body.
const HTTP_STREAM_DONE: &str = "__celld_http_stream_end__";
enum HttpStreamSource {
    Response(reqwest::Response),
    Receiver(tokio::sync::mpsc::Receiver<Result<Vec<u8>, String>>),
    Stream(HttpChunkStream),
}
struct HttpStreamEntry {
    created: Instant,
    source: Option<HttpStreamSource>,
    cancelled: tokio::sync::watch::Sender<bool>,
}
static HTTP_STREAMS: OnceLock<std::sync::Mutex<HashMap<u64, HttpStreamEntry>>> = OnceLock::new();
fn http_streams() -> &'static std::sync::Mutex<HashMap<u64, HttpStreamEntry>> {
    HTTP_STREAMS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}
fn register_http_stream(stream_id: u64, source: HttpStreamSource) {
    let (cancelled, _) = tokio::sync::watch::channel(false);
    let mut streams = http_streams().lock().unwrap();
    streams.retain(|_, stream| stream.created.elapsed() < Duration::from_secs(60));
    streams.insert(
        stream_id,
        HttpStreamEntry {
            created: Instant::now(),
            source: Some(source),
            cancelled,
        },
    );
}
struct ResponseStreamWriter {
    created: Instant,
    writer: tokio::sync::mpsc::Sender<Result<Vec<u8>, String>>,
    finished: tokio::sync::watch::Sender<bool>,
}
static RESPONSE_STREAM_WRITERS: OnceLock<std::sync::Mutex<HashMap<u64, ResponseStreamWriter>>> =
    OnceLock::new();
fn response_stream_writers() -> &'static std::sync::Mutex<HashMap<u64, ResponseStreamWriter>> {
    RESPONSE_STREAM_WRITERS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

pub type HttpChunkStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<Vec<u8>, String>> + Send>>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WsTarget {
    pub id: u64,
    pub scope: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_node: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_epoch: Option<u64>,
}

/// Encode a host response for the JS side. A text body crosses as a
/// JS string (cheap, lossless), binary as a byte array, and a streaming body
/// by id — serializing a Vec<u8> as a JSON number array is the dominant cost
/// for real DO responses. `ws_target` is carried by the paths that can answer
/// with a WebSocket upgrade: a Durable Object call and a service-binding call.
fn encode_http_response(mut response: HttpResponse, ws_target: bool) -> String {
    let mut obj = serde_json::json!({
        "status": response.status,
        "headers": response.headers,
    });
    if ws_target {
        obj["wsTarget"] = serde_json::json!(response.ws);
    }
    if let Some(stream) = response.stream.take() {
        let stream_id = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
        register_http_stream(stream_id, HttpStreamSource::Stream(stream));
        obj["streamId"] = serde_json::json!(stream_id);
    } else {
        match std::str::from_utf8(&response.body) {
            Ok(text) => obj["body"] = serde_json::Value::String(text.into()),
            Err(_) => obj["bodyBytes"] = serde_json::json!(response.body),
        }
    }
    obj.to_string()
}

pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    /// A response body forwarded without materializing it in memory.
    pub stream: Option<HttpChunkStream>,
    pub headers: Vec<(String, String)>,
    pub ws: Option<WsTarget>,
    /// The cell's committed-write position after the handler ran, for a local
    /// Durable Object request. The shell gates the response on durability when
    /// this advanced past the cell's last seen position. `None` for responses
    /// with no cell storage (Worker, asset, or proxied remote).
    pub write_position: Option<u64>,
}

/// Which alarm a dispatch runs, and who owns its bookkeeping.
pub enum AlarmDispatch {
    /// Claim whatever is due now, and record the outcome. Production only
    /// ever asks for this.
    Due,
    /// Claim whatever is armed, however far ahead its deadline. Test-only:
    /// the corpus asserts on retry accounting without waiting out real
    /// deadlines.
    Armed,
    /// The caller has already claimed it. Dispatch with this retry count and
    /// leave the bookkeeping to them. Test-only, so the corpus can assert on
    /// the claim and the handler separately.
    Claimed(i64),
}

/// One event a cell receives.
///
/// Every variant is something the outside world did to the cell, and each
/// becomes an `InFlight` entry driven by its own tokio task. Lifecycle —
/// taking a cell in, giving it back, cancelling a request — is not here: it
/// is a direct call on the isolate, because it is not an event and has no
/// handler to run.
pub enum CellJob {
    Fetch {
        request_id: Option<RequestId>,
        scope: String,
        name: Option<String>,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
        /// Where this call sits in its caller's order for this cell, if it
        /// came from a script rather than from a peer or the ingress.
        order: Option<CallOrder>,
    },
    Rpc {
        scope: String,
        name: Option<String>,
        method: String,
        args: RpcData,
        reply: tokio::sync::oneshot::Sender<Result<RpcOutcome>>,
    },
    WsOpen {
        scope: String,
        ws_id: u64,
        protocol: String,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    WsMessage {
        scope: String,
        ws_id: u64,
        data: WsIn,
        reply: tokio::sync::oneshot::Sender<Result<WsDispatch>>,
    },
    WsClosed {
        scope: String,
        ws_id: u64,
        code: u16,
        reason: String,
        was_clean: bool,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Alarm {
        scope: String,
        scheduled_ms: i64,
        /// Which alarm to run, and who owns the bookkeeping.
        claim: AlarmDispatch,
        /// Replies with (next alarm, the handler's write delta). The delta is
        /// sampled inside the turn — cell storage must never be reached from
        /// the shell — and the shell uses it to prove the consuming commit
        /// durable before the core settles. Every answer shape samples it the
        /// same way now, in `InFlight::answer_settled`.
        reply: tokio::sync::oneshot::Sender<Result<(Option<i64>, Option<u64>)>>,
    },
}

impl CellJob {
    /// The cell this event addresses, which is also the realm it runs in.
    pub fn scope(&self) -> &str {
        match self {
            CellJob::Fetch { scope, .. }
            | CellJob::Rpc { scope, .. }
            | CellJob::WsOpen { scope, .. }
            | CellJob::WsMessage { scope, .. }
            | CellJob::WsClosed { scope, .. }
            | CellJob::Alarm { scope, .. } => scope,
        }
    }

    /// This event's place in its caller's order, taken out so the drive can
    /// wait on it and then release the call behind it.
    pub fn take_order(&mut self) -> Option<CallOrder> {
        match self {
            CellJob::Fetch { order, .. } => order.take(),
            _ => None,
        }
    }

    /// Fail this event without running it.
    pub fn fail(self, error: anyhow::Error) {
        match self {
            CellJob::Fetch { reply, .. } => drop(reply.send(Err(error))),
            CellJob::Rpc { reply, .. } => drop(reply.send(Err(error))),
            CellJob::WsOpen { reply, .. } => drop(reply.send(Err(error))),
            CellJob::WsMessage { reply, .. } => drop(reply.send(Err(error))),
            CellJob::WsClosed { reply, .. } => drop(reply.send(Err(error))),
            CellJob::Alarm { reply, .. } => drop(reply.send(Err(error))),
        }
    }
}

thread_local! {
    // One outbound HTTP client per JS thread — building it per fetch rebuilds
    // the TLS stack every call (async-op-hazards.md).
    static HTTP: reqwest::Client = reqwest::Client::new();
    static HTTP_MANUAL: reqwest::Client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none()).build().unwrap();
    static DO_ID_KEYS: RefCell<HashMap<String, [u8; 32]>> = RefCell::new(HashMap::new());
}

mod websocket;
pub(crate) use websocket::WebSocketService;
pub use websocket::*;
use websocket::{ws_capture_begin, ws_capture_take};

/// Held for the duration of one cell event; pops on every exit path.
pub struct EgressGateScope;

impl Drop for EgressGateScope {
    fn drop(&mut self) {
        current_context().egress.lock().unwrap().pop();
    }
}

pub fn egress_gate_enter(cell: &str, position: Option<u64>) -> EgressGateScope {
    current_context()
        .egress
        .lock()
        .unwrap()
        .push((cell.to_string(), position.unwrap_or(0)));
    EgressGateScope
}

/// What an outbound effect must trail before it leaves the process.
///
/// The three cases are named rather than nested inside one another because
/// two of them once shared a representation, and that is what issue #144 was:
/// a reader that starts after another request committed samples the new
/// position as its own baseline and advances nothing, so a two-state gate read
/// it as code that owns no cell at all and let its side effect out ungated.
/// A variant per case makes the distinction one the compiler keeps.
enum EgressGate {
    /// No cell event is running. Stateless Worker code owns no cell state, so
    /// its egress reveals nothing that can still be lost and leaves directly.
    NoCell,
    /// This event wrote through the position. The effect waits for that write.
    Wrote(String, u64),
    /// A read-only output. It reveals whatever the cell holds, so it trails
    /// the newest write barrier still open on the cell. Only the core knows
    /// which writes are outstanding, so this asks rather than guesses.
    ReadOnly(String),
}

impl EgressGate {
    /// Whether the effect must consult the gate before it leaves. True for a
    /// read as well as a write: a read whose cell has no barrier is released
    /// at once, but only the core can say so.
    fn is_gated(&self) -> bool {
        !matches!(self, EgressGate::NoCell)
    }
}

/// Sample what the running handler's outbound effects must trail. Answers for
/// every active cell event, including a read whose core lookup can settle
/// immediately.
fn egress_gate_request() -> EgressGate {
    let context = current_context();
    let stack = context.egress.lock().unwrap();
    let Some((cell, before)) = stack.last() else {
        return EgressGate::NoCell;
    };
    match storage::write_position(cell).filter(|position| position > before) {
        Some(position) => EgressGate::Wrote(cell.clone(), position),
        // A read-only output in a process that configured no output gate has
        // nothing to trail. Such a process cannot acknowledge a write either
        // -- `op_gate_write` refuses one for want of the same channel -- so no
        // committed-but-unproven write exists for a read to reveal. Answer as
        // if no cell event were running, which keeps the effect on the direct,
        // synchronous path it has always taken; a WebSocket frame in
        // particular must not join a deferred queue whose flush needs a gate
        // to release it. A write still takes a ticket and still fails closed.
        None if GATE_TX.get().is_none() => EgressGate::NoCell,
        None => EgressGate::ReadOnly(cell.clone()),
    }
}

/// Wait for the writes an outbound effect can reveal to be proven durable
/// before it leaves the process.
///
/// This is the output gate applied to egress rather than to the response.
/// It cannot deadlock the handler that is awaiting it: the ticket is served by
/// `dispatch_gate` on the host's own loop and resolved by the replicator's
/// independent task, neither of which needs the isolate's event loop to run.
/// A read-only ticket carries `None` and the core releases it at once when the
/// cell has no barrier open, so an ordinary read pays one actor hop and no
/// replica write.
async fn await_egress_gate(gate: EgressGate) -> std::result::Result<(), String> {
    let (cell, position) = match gate {
        EgressGate::NoCell => return Ok(()),
        EgressGate::Wrote(cell, position) => (cell, Some(position)),
        EgressGate::ReadOnly(cell) => (cell, None),
    };
    let (tx, receive) = tokio::sync::oneshot::channel();
    let sent = GATE_TX
        .get()
        .map(|gate| {
            gate.send(GateReq {
                scope: cell,
                position,
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    if !sent {
        return Err("no output-gate channel".into());
    }
    match receive.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(format!(
            "refusing to send: the write this request follows is not durable ({error:?})"
        )),
        Err(_) => Err("output gate dropped".into()),
    }
}

/// Hold an outbound request until the write it follows is durable, then hand
/// it to the host.
///
/// The send moves inside the future deliberately. These channels dispatch as
/// soon as `send` is called, so gating anywhere after it would let the effect
/// leave first -- the request would already be on its way while the caller
/// waited for a durability answer that no longer decides anything.
async fn gated_channel_send<T>(
    gate: EgressGate,
    channel: &'static OnceLock<tokio::sync::mpsc::UnboundedSender<T>>,
    request: T,
    missing: &'static str,
) -> std::result::Result<(), String> {
    await_egress_gate(gate).await?;
    match channel.get() {
        Some(tx) if tx.send(request).is_ok() => Ok(()),
        _ => Err(missing.to_string()),
    }
}

/// One `celld_logic::gate::InputGate` per cell.
///
/// The logic decides; this is only the map and the ids. It replaces the
/// promise chain that used to live in `harness.js`, which serialised blocks
/// against each other but did nothing about *delivery* — so a second event
/// could arrive mid-block, and under real concurrency that wedged the cell.
///
/// Keyed by cell scope rather than held on the isolate, because a gate
/// belongs to a cell and cells share isolates.
static CELL_GATES: OnceLock<Mutex<HashMap<String, celld_logic::gate::InputGate>>> = OnceLock::new();
static NEXT_GATE_EVENT: AtomicU64 = AtomicU64::new(1);

fn cell_gates() -> &'static Mutex<HashMap<String, celld_logic::gate::InputGate>> {
    CELL_GATES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// What a waiting event is told when the gate finally opens: nothing, or
/// the reason the holder failed.
type GateWake = tokio::sync::oneshot::Sender<Result<(), String>>;

static GATE_WAITERS: OnceLock<Mutex<HashMap<String, Vec<GateWake>>>> = OnceLock::new();

/// Events waiting for a cell's input gate to open, in arrival order.
///
/// Step 6 deleted this queue, correctly at the time: an event the gate
/// refused stayed on the cell's job channel until a later delivery point
/// took it, so the channel *was* the queue and a second one could disagree
/// with it. Drives have no channel, so a refused event needs somewhere to
/// wait, and workerd keeps the same structure for the same reason
/// (`io-gate.h`, `kj::List<Waiter, &Waiter::link> waiters`).
///
/// In the shell rather than in `celld_logic::gate`, because what waits is a
/// tokio task and the core is sans-IO. The core still owns the decision —
/// `is_open` — and this owns only the waking.
fn gate_waiters() -> &'static Mutex<HashMap<String, Vec<GateWake>>> {
    GATE_WAITERS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Take a ticket to be woken when `cell`'s gate opens, or `None` if it is
/// open now.
///
/// The gate's lock is held across the check and the enqueue, so a release
/// that lands between the two cannot leave an event waiting on a gate that
/// is already open.
pub fn cell_gate_wait(cell: &str) -> Option<tokio::sync::oneshot::Receiver<Result<(), String>>> {
    let gates = cell_gates().lock().unwrap();
    if gates.get(cell).is_none_or(|gate| gate.is_open()) {
        return None;
    }
    let (wake, waiter) = tokio::sync::oneshot::channel();
    gate_waiters()
        .lock()
        .unwrap()
        .entry(cell.to_string())
        .or_default()
        .push(wake);
    drop(gates);
    Some(waiter)
}

/// Wake everything waiting on `cell`'s gate. Each re-checks and re-queues if
/// another block took the gate first, so waking all of them is correct and
/// the order they resume in is theirs to lose, not this function's to keep.
fn wake_gate_waiters(cell: &str, outcome: Result<(), String>) {
    let waiters = gate_waiters().lock().unwrap().remove(cell);
    for wake in waiters.into_iter().flatten() {
        let _ = wake.send(outcome.clone());
    }
}

/// The event holding this cell's gate died without releasing it.
///
/// Only execution termination reaches here: every other exit from a block
/// runs the `finally` in `harness.js`. Without it a cell whose blocking
/// request was killed would refuse every later event forever.
fn cell_gate_abandon(scope: &str) {
    if let Some(gate) = cell_gates().lock().unwrap().get_mut(scope) {
        gate.abandon();
    }
    // The holder died without running its `finally`, so nobody can say why.
    // Refuse the waiters anyway: the cell it queued behind is gone.
    wake_gate_waiters(
        scope,
        Err("the cell's critical section ended without releasing".to_string()),
    );
}

/// JS promise resolvers awaiting an async op, keyed by the op's id.
///
/// **Per isolate, and shared across threads.** Under D1 a request's turns run
/// on whichever tokio worker picks them up, so a resolver registered on one
/// thread is resolved from another; a thread-local map loses it, and
/// `resolve_res` fails to find the resolver and returns silently, leaving the
/// handler awaiting a promise nothing will ever settle. The `Mutex` is what
/// makes that safe, not the map's location.
///
/// It lives on `ActorRuntimeState` — one per isolate, reached from a scope —
/// so a resolver is only ever looked up in the heap that owns it. One map for
/// the whole process would also work, because op ids come from a single
/// counter (`asyncrt::NEXT_ID`) and an id names exactly one resolver. But that
/// makes an id-allocation bug into cross-isolate handle confusion, where this
/// makes it a miss.
type PromiseMap = HashMap<u64, v8::Global<v8::PromiseResolver>>;

/// Requests cancelled by a reentrant `AbortFetch`. Global for the same reason
/// as [`promises`]: the turn that observes a cancellation need not be on the
/// thread that recorded it.
static CANCELLED_REQUESTS: OnceLock<Mutex<HashSet<RequestId>>> = OnceLock::new();

fn cancelled_requests() -> &'static Mutex<HashSet<RequestId>> {
    CANCELLED_REQUESTS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn promise_store(scope: &mut v8::PinScope, id: u64, r: v8::Global<v8::PromiseResolver>) {
    actor_runtime_state(scope)
        .promises
        .lock()
        .unwrap()
        .insert(id, r);
}

/// Resolve or reject op `id`'s promise with its outcome.
fn resolve_res(tc: &mut v8::PinScope, id: u64, res: Result<asyncrt::OpOut, String>) {
    let g = match actor_runtime_state(tc).promises.lock().unwrap().remove(&id) {
        Some(g) => g,
        None => return,
    };
    let r = v8::Local::new(tc, g);
    match res {
        Ok(asyncrt::OpOut::Str(v)) => {
            let s = v8::String::new(tc, &v).unwrap();
            r.resolve(tc, s.into());
        }
        Ok(asyncrt::OpOut::Bytes(b)) => {
            let v = bytes_value(tc, b);
            r.resolve(tc, v);
        }
        Err(e) => {
            let s = v8::String::new(tc, &e).unwrap();
            let ex = v8::Exception::error(tc, s);
            r.reject(tc, ex);
        }
    }
}

/// Move `bytes` into a `Uint8Array` without copying.
fn bytes_value<'s>(scope: &mut v8::PinScope<'s, '_>, bytes: Vec<u8>) -> v8::Local<'s, v8::Value> {
    let len = bytes.len();
    let store = v8::ArrayBuffer::new_backing_store_from_vec(bytes).make_shared();
    let buffer = v8::ArrayBuffer::with_backing_store(scope, &store);
    v8::Uint8Array::new(scope, buffer, 0, len).unwrap().into()
}

pub fn handler_budget() -> Duration {
    static BUDGET: OnceLock<Duration> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        Duration::from_secs(
            crate::env_vars::positive_or("CELLD_HANDLER_BUDGET_S", 300)
                .expect("validated CELLD_HANDLER_BUDGET_S"),
        )
    })
}

/// Aborts raised by an HTTP or service-binding caller disconnecting while the
/// target runs in the stateless isolate pool. Durable Object aborts can also
/// arrive as a reentrant `CellJob::AbortFetch`, so the abort has to be visible
/// from whichever turn observes it.
///
/// The counter keeps the common path to one relaxed atomic load per loop turn:
/// with nothing pending, the mutex is never taken.
static PENDING_ABORTS: OnceLock<std::sync::Mutex<std::collections::HashSet<RequestId>>> =
    OnceLock::new();
static PENDING_ABORT_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn pending_aborts() -> &'static std::sync::Mutex<std::collections::HashSet<RequestId>> {
    PENDING_ABORTS.get_or_init(Default::default)
}

/// Mark an in-flight request cancelled from any thread.
pub fn abort_request(request_id: RequestId) {
    if pending_aborts().lock().unwrap().insert(request_id) {
        PENDING_ABORT_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}

fn clear_request_cancellation(request_id: RequestId) {
    let _ = take_pending_abort(request_id);
}

fn take_pending_abort(request_id: RequestId) -> bool {
    if PENDING_ABORT_COUNT.load(Ordering::Relaxed) == 0 {
        return false;
    }
    if pending_aborts().lock().unwrap().remove(&request_id) {
        PENDING_ABORT_COUNT.fetch_sub(1, Ordering::Relaxed);
        return true;
    }
    false
}

pub fn take_request_cancellation(request_id: Option<RequestId>) -> bool {
    request_id.is_some_and(|request_id| {
        cancelled_requests().lock().unwrap().remove(&request_id) || take_pending_abort(request_id)
    })
}

fn resolved_promise<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
) -> Result<v8::Local<'s, v8::Promise>> {
    let resolver =
        v8::PromiseResolver::new(tc).ok_or_else(|| anyhow!("could not create event promise"))?;
    resolver.resolve(tc, value);
    Ok(resolver.get_promise(tc))
}

fn abort_incoming_request(tc: &mut v8::PinScope, request_id: RequestId) -> Result<bool> {
    let global = tc.get_current_context().global(tc);
    let key = v8::String::new(tc, "__abortIncomingRequest").unwrap();
    let function: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no __abortIncomingRequest"))?
        .try_into()
        .map_err(|_| anyhow!("__abortIncomingRequest is not a function"))?;
    let request_id = v8::String::new(tc, &request_id_string(request_id)).unwrap();
    let recv = v8::undefined(tc).into();
    let result = function
        .call(tc, recv, &[request_id.into()])
        .ok_or_else(|| anyhow!("__abortIncomingRequest threw"))?;
    Ok(result.boolean_value(tc))
}

/// The rejection reason of a settled-rejected promise, with a little stack.
fn reject_reason(tc: &mut v8::PinScope, p: v8::Local<v8::Promise>) -> String {
    let r = p.result(tc);
    let msg = r.to_rust_string_lossy(tc);
    let stk = r
        .to_object(tc)
        .and_then(|o| {
            let k = v8::String::new(tc, "stack")?;
            o.get(tc, k.into())
        })
        .map(|s| s.to_rust_string_lossy(tc))
        .unwrap_or_default();
    let tail = stk
        .lines()
        .skip(1)
        .take(3)
        .map(|l| l.trim())
        .collect::<Vec<_>>()
        .join(" <- ");
    if tail.is_empty() {
        msg
    } else {
        format!("{msg} [{tail}]")
    }
}

pub struct Engine;
impl Engine {
    pub fn init() {
        // No `v8::icu::set_common_data_78` call: the rusty_v8 152 prebuilt
        // statically links the complete ICU data (full locale tables and
        // regex property-of-strings, e.g. /\p{RGI_Emoji}/v — verified
        // empirically), so overriding it with an embedded icudtl.dat only
        // duplicated 10.8 MB in the binary. A test pins that coverage, so a
        // future v8 bump that reduces the builtin data fails rather than
        // silently narrowing it; the fix would be to re-embed rusty_v8's
        // `third_party/icu/common/icudtl.dat`, 16-byte-aligned.
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform);
        v8::V8::initialize();
    }
}

/// Per-worker compatibility switches, derived from the manifest's
/// compatibility date and flags (Workerd compatibility-date.capnp). `Default`
/// is every switch off; production derives real values in `main`.
#[derive(Clone, Copy, Default)]
pub struct Compat {
    pub delete_all_deletes_alarm: bool,
    /// `js_rpc`: RPC on a Durable Object class that does not extend
    /// `DurableObject` (Workerd worker-rpc.c++ getTargetInfo()).
    pub js_rpc: bool,
    /// `fetcher_has_get_put_delete` (off = `fetcher_no_get_put_delete`,
    /// default on for dates >= 2024-03-26): the deprecated `get()`/`put()`/
    /// `delete()` HTTP helpers on stubs (Workerd http.c++ Fetcher).
    pub fetcher_get_put_delete: bool,
    /// `sqlite_vec`: expose the pre-v1 sqlite-vec extension to SQLite-backed
    /// Durable Objects. This switch is explicit and has no date default.
    pub sqlite_vec: bool,
    /// `websocket_standard_binary_type`: `binaryType` defaults to `"blob"` and
    /// a binary message arrives as a `Blob`, per the WHATWG default. Without
    /// it celld keeps the historical `"arraybuffer"`.
    pub websocket_standard_binary_type: bool,
}

/// A non-main module the worker's main module may import, tagged by how the
/// runtime materializes it.
pub enum ModuleSource {
    /// UTF-8 content served as `export default "<content>"` (wrangler's Text
    /// rule), registered under the given specifier verbatim.
    Text(String),
    /// JS source compiled as a sibling ES module (Worker Loader multi-module
    /// bundles), registered under both `name` and `./name`.
    EsModule(String),
    /// Wasm bytes served as a module whose default export is the compiled
    /// `WebAssembly.Module` (Wrangler's `CompiledWasm` rule), registered
    /// under both `name` and `./name`.
    Wasm(bytes::Bytes),
}

pub struct WorkerConfig {
    src: String,
    pub script_name: String,
    do_classes: Vec<String>,
    bindings: Vec<(String, String)>,
    r2_bindings: Vec<String>,
    /// `d1_databases`: (environment name, stable database identity). The
    /// identity addresses the cell that holds the database; see [[d1]].
    d1_bindings: Vec<(String, String)>,
    ai_binding: Option<String>,
    vars: Vec<(String, String)>,
    node: String,
    /// The worker's non-main modules, so the main module can import siblings.
    modules: Vec<(String, ModuleSource)>,
    compat: Compat,
    /// `[[services]]`: (binding name, target script, optional entrypoint).
    /// The target runs in this process; see [[service-bindings]].
    services: Vec<(String, String, Option<String>)>,
    asset_binding: Option<String>,
    /// `env` name of the Worker Loader binding, if this Worker may spawn
    /// dynamic isolates.
    loader_binding: Option<String>,
    /// Ambient outbound authority. Loaded workers may be denied.
    egress: EgressPolicy,
    /// Extra `env` values a loaded worker was handed, as a JSON object string
    /// merged onto its `env`. Loader-only; empty for normal workers.
    loader_env: Option<String>,
    /// `triggers.crons` from the deployment. Empty for a loaded worker and for
    /// any script without cron triggers.
    pub crons: Vec<String>,
}

pub struct WorkerConfigOptions {
    pub src: String,
    pub script_name: String,
    pub do_classes: Vec<String>,
    pub bindings: Vec<(String, String)>,
    pub r2_bindings: Vec<String>,
    pub d1_bindings: Vec<(String, String)>,
    pub ai_binding: Option<String>,
    pub vars: Vec<(String, String)>,
    pub node: String,
    pub modules: Vec<(String, ModuleSource)>,
    pub compat: Compat,
}

impl WorkerConfig {
    pub fn new(options: WorkerConfigOptions) -> Self {
        let WorkerConfigOptions {
            src,
            script_name,
            do_classes,
            bindings,
            r2_bindings,
            d1_bindings,
            ai_binding,
            vars,
            node,
            modules,
            compat,
        } = options;
        Self {
            src,
            script_name,
            do_classes,
            bindings,
            r2_bindings,
            d1_bindings,
            ai_binding,
            vars,
            node,
            modules,
            compat,
            services: Vec::new(),
            asset_binding: None,
            loader_binding: None,
            egress: EgressPolicy::Allow,
            loader_env: None,
            crons: Vec::new(),
        }
    }

    /// Give this Worker the deployment's cron trigger expressions.
    pub fn with_crons(mut self, crons: Vec<String>) -> Self {
        self.crons = crons;
        self
    }

    /// Grant this Worker a Worker Loader binding at `env` name `binding`.
    pub fn with_loader(mut self, binding: Option<String>) -> Self {
        self.loader_binding = binding;
        self
    }

    /// Set this Worker's ambient outbound authority (loaded workers only).
    fn with_egress(mut self, egress: EgressPolicy) -> Self {
        self.egress = egress;
        self
    }

    /// Merge `env` (a JSON object string) onto a loaded worker's `env`.
    fn with_loader_env(mut self, env: Option<String>) -> Self {
        self.loader_env = env;
        self
    }

    /// Declare the service bindings this Worker may call.
    pub fn with_services(mut self, services: Vec<(String, String, Option<String>)>) -> Self {
        self.services = services;
        self
    }

    pub fn with_asset_binding(mut self, binding: Option<String>) -> Self {
        self.asset_binding = binding;
        self
    }
}

pub struct Worker {
    inner: Option<WorkerIsolate>,
}

pub struct WorkerIsolate {
    /// Shared rather than owned: under D1 an isolate belongs to no thread,
    /// and a worker enters it by taking its `v8::Locker`.
    ///
    /// Every `lock()` below blocks until the current holder releases. That is
    /// only safe because the pool takes an async permit for this isolate
    /// first, so the lock is uncontended by construction; a `lock()` reached
    /// without that permit would be a blocking call on a tokio worker.
    isolate: v8::SharedIsolate,
    /// The one realm this isolate has: its context, and the entry `fetch`
    /// that lives in it.
    ///
    /// It was a `HashMap` keyed by cell, on the premise that a cell needs a
    /// realm of its own. It does not, and nothing ever put a second one in.
    /// The harness already keys instances by scope (`__cell.instances`), and
    /// Durable Objects **share** a context per script rather than getting one
    /// each — a context per cell would also duplicate the compiled module per
    /// cell, which is most of what sharing an isolate saves.
    realm: Realm,
    original_heap_limit: usize,
    compat: Compat,
    /// The storage of the cells this isolate hosts.
    ///
    /// It lives here, and not in a thread-local, because a driven cell's
    /// turns run on whatever tokio worker holds the isolate. See
    /// `storage::Cells`.
    cells: storage::Cells,
}

/// An isolate's context and the entry `fetch` that lives in it.
///
/// `fetch` is here rather than beside the isolate because a function is a
/// value in the realm that created it.
struct Realm {
    context: v8::Global<v8::Context>,
    fetch: v8::Global<v8::Function>,
}

impl WorkerIsolate {
    /// Take the isolate for one turn, and make the cells it hosts reachable
    /// while it is held.
    ///
    /// The two belong together. The lock is what makes this thread the only
    /// one that can touch the isolate, and a cell's SQLite handles are part
    /// of what it may touch — so a locker taken without installing them
    /// would let a turn reach no storage at all, or another isolate's.
    /// Pairing them here is why no call site has to remember.
    fn lock(&self) -> (v8::Locker<'_>, storage::Installed) {
        (self.isolate.lock(), self.cells.install())
    }

    /// Lift condemnation from an isolate whose heap has drained.
    ///
    /// `near_heap_limit` latches a flag that nothing used to clear, so a cell
    /// that reached its limit once stayed condemned until the process
    /// restarted. The flag is re-read here, between turns rather than inside
    /// one, because a handler must not see the isolate recover halfway
    /// through.
    ///
    /// A heap still over the line buys one `low_memory_notification` and a
    /// second reading. V8 stops collecting once it is past the limit, so a
    /// drained isolate holds the dead heap until something allocates again —
    /// without the forced collection the reading that decides recovery is a
    /// reading of garbage. `HEAP_GC_NUDGE_INTERVAL` bounds the cost.
    ///
    /// Removing the callback with the original limit puts back the limit
    /// `near_heap_limit` raised; re-adding it re-arms the guard.
    fn recover_heap(&self, locker: &mut v8::Locker<'_>) {
        let Some(state) = locker.get_slot::<Arc<HeapLimitState>>().cloned() else {
            return;
        };
        if !state.excessively_exceeded.load(Ordering::Relaxed) {
            return;
        }
        if heap_share(locker, state.limit) >= HEAP_RECOVERY_SHARE {
            if !state.due_for_gc_nudge() {
                return;
            }
            locker.low_memory_notification();
            if heap_share(locker, state.limit) >= HEAP_RECOVERY_SHARE {
                return;
            }
        }
        state.excessively_exceeded.store(false, Ordering::Relaxed);
        let data = Arc::as_ptr(&state) as *mut HeapLimitState as *mut std::ffi::c_void;
        locker.remove_near_heap_limit_callback(near_heap_limit, state.limit);
        locker.add_near_heap_limit_callback(near_heap_limit, data);
        tracing::info!(
            event = "isolate_heap_recovered",
            limit_bytes = state.limit,
            "isolate heap fell back under its limit, so it serves again"
        );
    }

    /// Localise the realm for one turn.
    ///
    /// **Taking the scope is the point.** Reaching a realm means touching
    /// `Global` handles, which on a shared isolate is only legal while its
    /// `Locker` is held — and there is no scope to pass until the isolate is
    /// locked. So the requirement is structural rather than a comment asking
    /// callers to remember it. It was a comment, and the one call site that
    /// forgot cloned two `Global`s a line too early; the panic happened
    /// inside a spawned request task, where tokio swallowed it and it
    /// surfaced only as a poisoned mutex on every later request.
    fn realm<'s>(&self, hs: &mut v8::PinScope<'s, '_, ()>) -> Entered<'s> {
        Entered {
            context: v8::Local::new(hs, &self.realm.context),
            fetch: v8::Local::new(hs, &self.realm.fetch),
        }
    }
}

/// One realm, entered. Valid only for the scope that produced it, which is
/// what ties it to the isolate being locked.
struct Entered<'s> {
    context: v8::Local<'s, v8::Context>,
    fetch: v8::Local<'s, v8::Function>,
}

impl std::ops::Deref for Worker {
    type Target = WorkerIsolate;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("Worker isolate unavailable")
    }
}

impl std::ops::DerefMut for Worker {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().expect("Worker isolate unavailable")
    }
}

const DEFAULT_V8_HEAP_LIMIT_BYTES: usize = 128 * 1024 * 1024;
const V8_HEAP_EMERGENCY_BYTES: usize = 16 * 1024 * 1024;

/// Share of the heap limit past which an isolate takes on no more retained
/// state. It sits below the near-limit callback on purpose: this refuses one
/// hibernatable socket while the isolate still works, where the callback
/// fires once it no longer does.
const HEAP_ADMISSION_SHARE: f64 = 0.9;

/// Share of the heap limit under which a condemned isolate is condemned no
/// more. Under `HEAP_ADMISSION_SHARE` by enough that recovery does not
/// immediately re-admit into a heap that is about to fail again.
const HEAP_RECOVERY_SHARE: f64 = 0.75;

/// The shortest gap between two collections forced by `recover_heap`. A full
/// collection of a 128 MB heap costs tens of milliseconds and a loaded cell
/// begins many turns each second, so an unbounded nudge would spend more of a
/// condemned isolate on collecting than on serving.
const HEAP_GC_NUDGE_INTERVAL: Duration = Duration::from_secs(1);

struct HeapLimitState {
    excessively_exceeded: AtomicBool,
    /// The limit V8 gave this isolate, before any emergency extension.
    /// Recovery measures against this one because `near_heap_limit` raises
    /// the current one.
    limit: usize,
    last_gc_nudge: Mutex<Option<Instant>>,
    /// Test-only: a real heap over the admission share is unreachable from a
    /// test, and the condemnation flag cannot stand in for it because
    /// recovery clears that one at the next turn.
    #[cfg(all(test, celld_internal_tests))]
    forced_admission_refusal: AtomicBool,
}

impl HeapLimitState {
    /// Whether a condemned isolate can pay for another forced collection.
    ///
    /// Rate-limited rather than once for each condemnation: the load can
    /// still be live at the first reading and gone by the second, so a
    /// one-shot nudge would leave the cell condemned exactly as before.
    fn due_for_gc_nudge(&self) -> bool {
        let Ok(mut last) = self.last_gc_nudge.lock() else {
            return false;
        };
        let now = Instant::now();
        if last.is_some_and(|at| now.duration_since(at) < HEAP_GC_NUDGE_INTERVAL) {
            return false;
        }
        *last = Some(now);
        true
    }
}

#[cfg(all(test, celld_internal_tests))]
fn admission_refusal_forced(state: &HeapLimitState) -> bool {
    state.forced_admission_refusal.load(Ordering::Relaxed)
}

#[cfg(not(all(test, celld_internal_tests)))]
fn admission_refusal_forced(_state: &HeapLimitState) -> bool {
    false
}

type SerializedPut = (String, Vec<u8>);
type PendingPuts = HashMap<String, Vec<SerializedPut>>;

/// Ambient outbound authority for an isolate. Normal workers keep `Allow`; a
/// Worker Loader can hand a loaded worker `Deny` (globalOutbound: null) so its
/// global `fetch()` throws and it must reach the world through `env`
/// capabilities.
#[derive(Clone, Copy, Default, PartialEq)]
enum EgressPolicy {
    #[default]
    Allow,
    Deny,
}

#[derive(Default)]
struct ActorRuntimeState {
    promises: std::sync::Mutex<PromiseMap>,
    termination: std::sync::Mutex<Option<ExecutionTermination>>,
    pending_puts: std::sync::Mutex<PendingPuts>,
    egress: EgressPolicy,
}

struct ExecutionTermination {
    error: String,
    actor_scope: Option<String>,
}

fn finish_terminated_actor_event(scope: &mut v8::PinScope, actor_scope: &str) {
    // The one exit from a block that runs no JS, so the `finally` in
    // harness.js never releases. Leave it and the cell refuses everything
    // from here on.
    cell_gate_abandon(actor_scope);
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, "__endTerminatedActorEvent").unwrap();
    let Some(value) = global.get(scope, key.into()) else {
        return;
    };
    let Ok(function) = v8::Local::<v8::Function>::try_from(value) else {
        return;
    };
    let actor_scope = v8::String::new(scope, actor_scope).unwrap();
    let recv = v8::undefined(scope).into();
    let _ = function.call(scope, recv, &[actor_scope.into()]);
}

/// Compile and run a JS expression that evaluates to a function.
fn compile_fn<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    src: &str,
) -> Result<v8::Local<'s, v8::Function>> {
    let code = v8::String::new(scope, src).unwrap();
    let script =
        v8::Script::compile(scope, code, None).ok_or_else(|| anyhow!("compile shim: {src}"))?;
    let value = script
        .run(scope)
        .ok_or_else(|| anyhow!("run shim: {src}"))?;
    value
        .try_into()
        .map_err(|_| anyhow!("shim is not a function: {src}"))
}

/// Whether `register_entrypoints` put `name` in the `__cell.<registry>`
/// object (e.g. `entrypoints`, `doExports`).
fn cell_registry_has(scope: &mut v8::PinScope, registry: &str, name: &str) -> Result<bool> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell_key = v8::String::new(scope, "__cell").unwrap();
    let registry_key = v8::String::new(scope, registry).unwrap();
    let registry_obj = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .and_then(|cell| cell.get(scope, registry_key.into()))
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell.{registry} registry"))?;
    let name_key = v8::String::new(scope, name).unwrap();
    Ok(registry_obj
        .has_own_property(scope, name_key.into())
        .unwrap_or(false))
}

fn take_execution_termination(scope: &mut v8::PinScope) -> Option<anyhow::Error> {
    let state = scope.get_slot::<Arc<ActorRuntimeState>>().cloned();
    let termination = state.and_then(|state| state.termination.lock().ok()?.take());
    let is_terminating = scope.is_execution_terminating();
    if termination.is_some() || is_terminating {
        scope.cancel_terminate_execution();
    }
    if let Some(termination) = termination {
        if let Some(actor_scope) = termination.actor_scope {
            finish_terminated_actor_event(scope, &actor_scope);
        }
        return Some(anyhow!(termination.error));
    }
    is_terminating.then(|| anyhow!("JavaScript execution was terminated"))
}

extern "C" fn near_heap_limit(
    data: *mut std::ffi::c_void,
    current_heap_limit: usize,
    _initial_heap_limit: usize,
) -> usize {
    // SAFETY: `data` points into the Arc stored in the isolate slot. Worker::drop
    // removes this callback before the isolate (and its slots) are destroyed.
    let state = unsafe { &*(data as *const HeapLimitState) };
    state.excessively_exceeded.store(true, Ordering::Relaxed);
    // V8 fatally aborts the process if a near-limit callback does not extend
    // the limit. This reserve lets JS observe condemnation and unwind.
    //
    // The extension must also cover one allocation as large as the limit
    // itself: flattening a 128 MiB cons string asks V8 for a single 128 MiB
    // block, and V8 re-invokes this callback across last-resort GC rounds
    // only unreliably (under parallel load it often stops after one round
    // and aborts the process). Doubling the original limit makes the first
    // invocation sufficient on its own; `recover_heap` and `Drop` still
    // restore the original limit, so nothing else changes.
    current_heap_limit
        .saturating_add(V8_HEAP_EMERGENCY_BYTES)
        .max(state.limit.saturating_mul(2))
}

/// Live heap use, as a share of `limit`.
fn heap_share(isolate: &mut v8::Isolate, limit: usize) -> f64 {
    if limit == 0 {
        return 0.0;
    }
    isolate.get_heap_statistics().used_heap_size() as f64 / limit as f64
}

fn v8_heap_limit_bytes() -> usize {
    crate::env_vars::positive::<usize>("CELLD_V8_HEAP_LIMIT_MB")
        .expect("validated CELLD_V8_HEAP_LIMIT_MB")
        .map(|megabytes| {
            megabytes
                .checked_mul(1024 * 1024)
                .expect("validated CELLD_V8_HEAP_LIMIT_MB range")
        })
        .unwrap_or(DEFAULT_V8_HEAP_LIMIT_BYTES)
}

impl Drop for WorkerIsolate {
    fn drop(&mut self) {
        let limit = self.original_heap_limit;
        let (mut locker, _cells) = self.lock();
        locker.remove_near_heap_limit_callback(near_heap_limit, limit);
    }
}

/// One in-flight request inside a shared isolate.
///
/// Owned by the request's own tokio task, which is why every field is
/// `Send`: the task suspends between turns and can resume on any worker,
/// then re-enters the isolate it is affiliated with.
/// Where a finished handler's result goes, and in what shape.
///
/// A fetch answers an `HttpResponse` and an entrypoint RPC answers bytes.
/// Everything between — the turn loop, the op region, cancellation, the
/// budget — is identical, so the difference lives here rather than in two
/// copies of `drive`.
pub enum Answer {
    Fetch(tokio::sync::oneshot::Sender<Result<HttpResponse>>),
    Rpc(tokio::sync::oneshot::Sender<Result<Vec<u8>>>),
    /// A DO method call, which answers a value rather than a response.
    CellRpc(tokio::sync::oneshot::Sender<Result<RpcOutcome>>),
    /// A `webSocketMessage`, which answers the frames the output gate held
    /// and the write they are gated on.
    WsMessage(tokio::sync::oneshot::Sender<Result<WsDispatch>>),
    /// An event whose result is that it finished: `webSocketOpen`,
    /// `webSocketClose`.
    Ack(tokio::sync::oneshot::Sender<Result<()>>),
    /// An alarm, which answers whatever alarm the handler left armed.
    Alarm(tokio::sync::oneshot::Sender<Result<(Option<i64>, Option<u64>)>>),
}

impl Answer {
    /// Send an error, whichever shape the caller is waiting for.
    fn fail(self, error: anyhow::Error) {
        match self {
            Answer::Fetch(reply) => drop(reply.send(Err(error))),
            Answer::Rpc(reply) => drop(reply.send(Err(error))),
            Answer::CellRpc(reply) => drop(reply.send(Err(error))),
            Answer::WsMessage(reply) => drop(reply.send(Err(error))),
            Answer::Ack(reply) => drop(reply.send(Err(error))),
            Answer::Alarm(reply) => drop(reply.send(Err(error))),
        }
    }
}

pub struct InFlight {
    /// The handler's promise. A `Global` because it outlives the turn that
    /// created it: Locals never cross a turn, exactly as workerd's
    /// `Worker::Lock` never does.
    promise: v8::Global<v8::Promise>,
    context: Arc<IoContext>,
    /// The cell this event belongs to, and `None` for stateless work.
    ///
    /// It is what the storage-shaped parts of settling — the durability
    /// position, a fatal SQL error — are read against.
    scope: Option<String>,
    /// The cell's committed-write position before the handler ran.
    ///
    /// The answer carries a write position only if the handler advanced it,
    /// so the output gate sees a write this event made and ignores celld's
    /// own activation writes (the actor name, alarm bookkeeping).
    writes_before: Option<u64>,
    request_id: Option<RequestId>,
    active_request_id: Option<RequestId>,
    reply: Option<Answer>,
    /// `waitUntil` work still running after the response was sent. The
    /// request stays in flight until it settles, as the single-request loop
    /// keeps driving it.
    background: Option<v8::Global<v8::Promise>>,
    /// Ops this request is waiting on, so a completion can be attributed to
    /// the request whose context must be current while its continuation runs.
    ops: std::collections::HashSet<u64>,
    /// The alarm bookkeeping this entry still owes, if it is one.
    alarm: Option<AlarmClaim>,
    started: Instant,
    /// The sampled trace this entry records into, `None` when telemetry
    /// is off or the trace was not sampled. Installed into the isolate's
    /// CPED slot for every turn, so promise continuations carry it.
    trace: Option<crate::telemetry::TraceIds>,
    /// Why a handler that did not finish failed, captured for the span's
    /// `error` so a query sees the reason, not only that it failed. Set
    /// only when a trace is sampled.
    failure: Option<String>,
    /// The isolate this entry's ops belong to, so `abandon` can drop their
    /// resolvers without a scope to reach the isolate through.
    runtime_state: Arc<ActorRuntimeState>,
}

impl InFlight {
    /// Read the handler's settled value in the shape this entry answers, end
    /// the event, and reply.
    ///
    /// Every shape ends the event exactly once, whether it produced a value
    /// or an error — ending it is what yields the `waitUntil` work the entry
    /// keeps driving afterwards, and a shape that skipped it on the error
    /// path would leave the context open.
    fn answer_settled<'s>(
        &mut self,
        tc: &mut v8::PinScope<'s, '_>,
        value: v8::Local<'s, v8::Value>,
    ) {
        let Some(reply) = self.reply.take() else {
            return;
        };
        // A cell whose SQL failed fatally cannot answer whatever the handler
        // returned: the storage the handler read may not be what the cell
        // has.
        if let Some(error) = self.scope.as_deref().and_then(storage::sql_critical_error) {
            let _ = end_event_context(tc);
            if self.trace.is_some() {
                self.failure = Some(crate::telemetry::cap_error(error.to_string()));
            }
            reply.fail(anyhow!(error));
            return;
        }
        self.background = match reply {
            Answer::Fetch(reply) => {
                let read = read_response(tc, value).map(|mut response| {
                    response.write_position = self.write_delta();
                    response
                });
                send_and_end(tc, reply, read)
            }
            Answer::Rpc(reply) => {
                let read =
                    view_bytes(value).ok_or_else(|| anyhow!("entrypoint RPC answered non-bytes"));
                send_and_end(tc, reply, read)
            }
            Answer::CellRpc(reply) => {
                let outcome = RpcOutcome {
                    data: rpc_data_ret(tc, value),
                    write_position: self.write_delta(),
                };
                send_and_end(tc, reply, Ok(outcome))
            }
            Answer::WsMessage(reply) => {
                let dispatch = WsDispatch {
                    frames: ws_capture_take(),
                    write_position: self.write_delta(),
                };
                send_and_end(tc, reply, Ok(dispatch))
            }
            Answer::Ack(reply) => send_and_end(tc, reply, Ok(())),
            Answer::Alarm(reply) => {
                // The handler ran and returned, so close the claim as a
                // success. This is the only path that does: every other
                // `settle_alarm` call site records a failure, and without
                // this one a *successful* alarm was recorded as one to
                // retry — which re-armed it forever, kept the cell busy,
                // and meant an eviction waiting for the cell to go quiet
                // never got its turn.
                self.settle_alarm(true, false);
                // Read what stands *after* that cleanup, and take the delta
                // last so it covers the cleanup's own commit — which is the
                // write the core must prove durable before it settles the
                // alarm.
                let alarm = self
                    .scope
                    .as_deref()
                    .map(storage::get_alarm)
                    .unwrap_or(None);
                send_and_end(tc, reply, Ok((alarm, self.write_delta())))
            }
        };
    }

    /// The write position to gate this answer on: `None` unless the handler
    /// advanced the cell's committed writes past where they were.
    fn write_delta(&self) -> Option<u64> {
        write_delta(
            self.writes_before,
            self.scope.as_deref().and_then(storage::write_position),
        )
    }

    /// Fail whichever shape is waiting, without knowing which.
    ///
    /// This must not touch cell storage: a budget overrun and a stuck
    /// handler are found on the driving task, between turns, where the
    /// storage thread-local is null — settling the alarm claim here
    /// panicked the node (denoland/celld#170). The claim stays put, and
    /// the drive loop's `owes_alarm` turn records it as not counting
    /// against the retry limit, because a failure here is not the
    /// handler's. A handler that threw is recorded by `settle`, which
    /// knows that it did, before it reaches this.
    fn fail(&mut self, error: anyhow::Error) {
        if let Some(reply) = self.reply.take() {
            if self.trace.is_some() {
                self.failure = Some(crate::telemetry::cap_error(error.to_string()));
            }
            reply.fail(error);
        }
    }

    /// Record how a claimed alarm ended. Runs once; later calls do nothing.
    fn settle_alarm(&mut self, ok: bool, counts_against_limit: bool) {
        let (Some(scope), Some(claim)) = (self.scope.as_deref(), self.alarm.take()) else {
            return;
        };
        if ok {
            storage::finish_alarm_handler(scope, true, claim.now_ms);
        } else {
            storage::finish_alarm_handler_with_retry_policy(
                scope,
                false,
                claim.now_ms,
                counts_against_limit,
            );
        }
    }

    /// Whether a claimed alarm's outcome is still unrecorded. True only
    /// where the event ended without ever entering the isolate again.
    pub fn owes_alarm(&self) -> bool {
        self.alarm.is_some()
    }

    /// Done when the response has been sent and nothing is left running.
    pub fn finished(&self) -> bool {
        self.reply.is_none() && self.background.is_none() && self.ops.is_empty()
    }

    /// Why the handler failed, when a sampled trace captured it.
    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    /// How long this handler may still run, or `None` once it has answered.
    ///
    /// The budget bounds the *response*, not the request: `waitUntil` work
    /// continues after the client has been served and is not charged for the
    /// time the handler already spent.
    pub fn remaining(&self, budget: Duration) -> Option<Duration> {
        self.reply
            .is_some()
            .then(|| budget.saturating_sub(self.started.elapsed()))
    }

    /// Give up on a handler that will not settle.
    pub fn time_out(&mut self, budget: Duration) {
        self.fail(anyhow!("handler exceeded {}s budget", budget.as_secs()));
    }

    /// Nothing this request awaits can move it, so it will never settle on
    /// its own. Reachable when a handler awaits a promise only some *other*
    /// request could resolve — which the pump concealed, because it settled
    /// every entry on every turn whoever the turn belonged to.
    pub fn stuck(&mut self) {
        self.fail(anyhow!("handler is waiting on nothing"));
        self.background = None;
    }

    /// Has the client been answered? `waitUntil` work can still be running.
    pub fn answered(&self) -> bool {
        self.reply.is_none()
    }

    /// Whether a client can still disconnect from this request. A request
    /// with no id is internal and has no client to hang up.
    pub fn cancellable(&self) -> bool {
        self.reply.is_some() && self.request_id.is_some()
    }

    pub fn request_id(&self) -> Option<RequestId> {
        self.request_id
    }

    /// The request is over and its remaining ops are being dropped. Purge
    /// their resolvers.
    ///
    /// An op that never completes would otherwise leave its
    /// `Global<PromiseResolver>` in the isolate's map for as long as the
    /// isolate lives. A request that drives itself has to purge them on its
    /// own way out.
    pub fn abandon(&mut self) {
        if self.ops.is_empty() {
            return;
        }
        let mut promises = self.runtime_state.promises.lock().unwrap();
        for id in self.ops.drain() {
            promises.remove(&id);
        }
    }
}

/// The CPED slot carries two riders in one immutable record: the
/// harness's async-context frame (`__als_get`/`__als_set` — ALS and
/// request-context confinement) and telemetry's trace context. Each
/// write builds a fresh two-element array preserving the other rider,
/// so V8's per-reaction snapshots restore both atomically and neither
/// feature can stomp the other — the collision the trace-join test
/// caught when telemetry first took the whole slot.
///
/// The empty state stays *empty*: when both riders are absent the slot
/// is undefined, which V8 fast-paths, and "off is structural" holds.
fn cped_parts<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> (v8::Local<'s, v8::Value>, v8::Local<'s, v8::Value>) {
    let data = scope.get_continuation_preserved_embedder_data();
    if let Ok(record) = v8::Local::<v8::Array>::try_from(data) {
        if record.length() == 2 {
            let undefined = v8::undefined(scope).into();
            return (
                record.get_index(scope, 0).unwrap_or(undefined),
                record.get_index(scope, 1).unwrap_or(undefined),
            );
        }
    }
    // Any non-record value is a bare frame from before this scheme, or
    // the empty slot.
    (data, v8::undefined(scope).into())
}

fn cped_frame<'s>(scope: &mut v8::PinScope<'s, '_>) -> v8::Local<'s, v8::Value> {
    cped_parts(scope).0
}

fn cped_trace<'s>(scope: &mut v8::PinScope<'s, '_>) -> v8::Local<'s, v8::Value> {
    cped_parts(scope).1
}

fn set_cped(scope: &mut v8::PinScope, frame: v8::Local<v8::Value>, trace: v8::Local<v8::Value>) {
    if frame.is_undefined() && trace.is_undefined() {
        let undefined = v8::undefined(scope).into();
        scope.set_continuation_preserved_embedder_data(undefined);
        return;
    }
    let record = v8::Array::new(scope, 2);
    record.set_index(scope, 0, frame);
    record.set_index(scope, 1, trace);
    scope.set_continuation_preserved_embedder_data(record.into());
}

/// Install a trace context into the isolate's CPED slot for one turn.
///
/// V8 snapshots continuation-preserved embedder data when a promise
/// reaction is registered and restores it while the reaction runs. That
/// is the exactness `console.log` correlation needs: a continuation
/// belonging to a *different* entry that runs during this turn's
/// microtask checkpoint carries its own context, not this turn's. The
/// layout is 16 trace-id bytes then 8 span-id bytes in one 24-byte
/// ArrayBuffer, fresh per turn — cheap at sampled rates, and nothing
/// outlives V8's own snapshots.
///
/// Returns the previous slot value *only when something was installed*;
/// the caller restores it before releasing the isolate. An untraced
/// turn touches CPED not at all — every traced turn restores what it
/// found, so the ambient slot between turns is always empty, and "off
/// is structural" stays true at the V8 boundary too (the -13% the
/// always-read version cost at 1% sampling was measured, not guessed).
fn install_trace<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    trace: Option<&crate::telemetry::TraceIds>,
) -> Option<v8::Local<'s, v8::Value>> {
    let ids = trace?;
    let previous = scope.get_continuation_preserved_embedder_data();
    let buffer = v8::ArrayBuffer::new(scope, 24);
    let data = buffer.get_backing_store().data()?;
    let bytes = data.as_ptr() as *mut u8;
    // SAFETY: a freshly created 24-byte buffer, written before any JS
    // can see it.
    unsafe {
        std::ptr::copy_nonoverlapping(ids.trace_id.as_ptr(), bytes, 16);
        std::ptr::copy_nonoverlapping(ids.span_id.as_ptr(), bytes.add(16), 8);
    }
    let frame = cped_frame(scope);
    set_cped(scope, frame, buffer.into());
    Some(previous)
}

fn restore_trace(scope: &mut v8::PinScope, previous: Option<v8::Local<v8::Value>>) {
    if let Some(previous) = previous {
        scope.set_continuation_preserved_embedder_data(previous);
    }
}

/// The trace context current at this exact point of JS execution, read
/// from CPED — the running turn's, or the running continuation's if V8
/// restored one. `None` when telemetry is off, the trace was unsampled,
/// or execution is outside any entry.
pub(crate) fn current_trace_ids(scope: &mut v8::PinScope) -> Option<crate::telemetry::TraceIds> {
    if !crate::telemetry::active() {
        return None;
    }
    let data = cped_trace(scope);
    let buffer = v8::Local::<v8::ArrayBuffer>::try_from(data).ok()?;
    if buffer.byte_length() != 24 {
        return None;
    }
    let data = buffer.get_backing_store().data()?;
    let bytes = data.as_ptr() as *const u8;
    let mut trace_id = [0u8; 16];
    let mut span_id = [0u8; 8];
    // SAFETY: length checked; the buffer is alive for this scope.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes, trace_id.as_mut_ptr(), 16);
        std::ptr::copy_nonoverlapping(bytes.add(16), span_id.as_mut_ptr(), 8);
    }
    Some(crate::telemetry::TraceIds { trace_id, span_id })
}

/// The isolate, entered for one turn. Everything a request does to a shared
/// isolate goes through one of these, and none of them may be held across an
/// await: the caller takes the pool's async permit first, runs a turn, and
/// leaves.
impl Worker {
    /// Run a request's first turn.
    ///
    /// Returns what is now in flight — `None` when nothing is, the reply
    /// already carrying the error — and the ops the handler enqueued, which
    /// the caller awaits with no isolate held.
    pub fn turn_begin(
        &mut self,
        job: crate::WorkerJob,
        trace: Option<crate::telemetry::TraceIds>,
    ) -> (Option<InFlight>, Vec<Op>) {
        let Some(inner) = self.inner.as_mut() else {
            return (None, Vec::new());
        };
        let (mut locker, _cells) = inner.lock();
        inner.recover_heap(&mut locker);
        v8::scope!(let hs, &mut *locker);
        let realm = inner.realm(hs);
        let context = realm.context;
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();

        let previous = install_trace(tc, trace.as_ref());
        let out = match begin(tc, realm.fetch, job) {
            Begun::Running(mut entry) => {
                entry.trace = trace;
                // A handler that returned an already-resolved promise is
                // finished before it ever suspends, and must not wait for an
                // op it will never start.
                let ops = finish_turn(tc, &mut entry);
                (Some(*entry), ops)
            }
            Begun::Threw(answer) => {
                answer.fail(anyhow!("fetch threw: {}", exc!(tc)));
                (None, Vec::new())
            }
            Begun::Nothing => (None, Vec::new()),
        };
        restore_trace(tc, previous);
        out
    }

    /// Enter the isolate solely to end a request whose client has hung up.
    ///
    /// A suspended request notices the disconnect without any isolate — the
    /// flag is host state — so this is reached only when it has actually
    /// fired, rather than on a timer as the blocking run loop did.
    pub fn turn_cancel(&mut self, entry: &mut InFlight) -> Vec<Op> {
        let Some(inner) = self.inner.as_mut() else {
            return Vec::new();
        };
        let (mut locker, _cells) = inner.lock();
        v8::scope!(let hs, &mut *locker);
        let realm = inner.realm(hs);
        let context = realm.context;
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();

        let previous = install_trace(tc, entry.trace.as_ref());
        cancel(tc, entry);
        let ops = finish_turn(tc, entry);
        restore_trace(tc, previous);
        ops
    }

    /// Run a later turn: resolve one of this request's ops, drain the
    /// microtasks that follow, and answer if the handler settled.
    pub fn turn_deliver(
        &mut self,
        entry: &mut InFlight,
        op: u64,
        res: Result<asyncrt::OpOut, String>,
    ) -> Vec<Op> {
        let Some(inner) = self.inner.as_mut() else {
            return Vec::new();
        };
        let (mut locker, _cells) = inner.lock();
        v8::scope!(let hs, &mut *locker);
        let realm = inner.realm(hs);
        let context = realm.context;
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();

        let previous = install_trace(tc, entry.trace.as_ref());
        deliver(tc, entry, op, res);
        cancelled(tc, entry);
        let ops = finish_turn(tc, entry);
        restore_trace(tc, previous);
        ops
    }
}

/// Close a turn, whatever the turn did.
///
/// Every step earns its place and the order is the whole point:
///
/// 1. **settle** — the handler's promise may have resolved, so marshal the
///    response and answer. Marshalling can start a JS-to-host body pump and
///    register it with `waitUntil`.
/// 2. **checkpoint** — that pump's native ops exist only once the microtasks
///    which create them run.
/// 3. **settle again** — the checkpoint may itself have settled the promise,
///    or the `waitUntil` aggregate.
/// 4. **adopt** — only now is the set of ops this request waits on complete.
///
/// Draining before step 2 is the bug that made a streaming handler answer
/// with nothing outstanding and conclude it was waiting on nothing.
/// The checkpoint before adoption is what makes response-body pumps visible.
fn finish_turn(tc: &mut v8::PinScope, entry: &mut InFlight) -> Vec<Op> {
    settle(tc, entry);
    tc.perform_microtask_checkpoint();
    // `abort()` and `process.exit()` terminate execution without settling
    // the handler's promise, so an entry that only watched the promise would
    // wait on it forever and then report that it was waiting on nothing.
    // The blocking loop broke out of its loop here; an entry fails here.
    if let Some(error) = take_execution_termination(tc) {
        entry.fail(error);
        entry.background = None;
        entry.abandon();
        return Vec::new();
    }
    settle(tc, entry);
    adopt(entry)
}

/// An op the JS enqueued, and the id whose promise it resolves.
pub type Op = (u64, asyncrt::OpFuture);

/// Take the ops this turn enqueued, recording them as the request's own.
///
/// Drained after `settle`, not before: ending an event runs JS, and anything
/// that starts there belongs to this request too. The pump drained first and
/// so could attribute those to whichever entry it settled next.
fn adopt(entry: &mut InFlight) -> Vec<Op> {
    let spawns = asyncrt::drain_spawns();
    for (id, _) in &spawns {
        entry.ops.insert(*id);
    }
    spawns
}

/// What starting a request produced.
enum Begun {
    /// In flight. Its ops must be adopted and its promise driven.
    Running(Box<InFlight>),
    /// The handler threw before it could suspend, so nothing is in flight.
    /// The exception belongs to the caller's `TryCatch` — an unnameable type
    /// no signature here can take — so the caller reads it and answers.
    Threw(Answer),
    /// Nothing started: the job was not a fetch, or the reply already
    /// carries the error.
    Nothing,
}

/// Start a request: build it, call the Worker's `fetch`, and hand back what
/// is now in flight.
///
/// The half of a turn that runs *before* anything can suspend.
fn begin<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    fetch: v8::Local<'s, v8::Function>,
    job: crate::WorkerJob,
) -> Begun {
    let job = match job {
        crate::WorkerJob::Rpc {
            entrypoint,
            method,
            args,
            reply,
        } => return begin_entrypoint_rpc(tc, &entrypoint, &method, args, reply),
        job => job,
    };
    let crate::WorkerJob::Fetch {
        url,
        method,
        body,
        headers,
        request_id,
        reply,
        ..
    } = job
    else {
        return Begun::Nothing;
    };

    let context = IoContext::new();
    let guard = CurrentGuard::enter(context.clone());
    let started = start_fetch(tc, fetch, &url, &method, &body, &headers, request_id);
    let promise = match started {
        Ok(Started::Running(ret, active)) => match ret.try_cast::<v8::Promise>() {
            Ok(promise) => Ok((promise, active)),
            Err(_) => resolved_promise(tc, ret).map(|promise| (promise, active)),
        },
        Ok(Started::Threw) => {
            drop(guard);
            return Begun::Threw(Answer::Fetch(reply));
        }
        Err(error) => Err(error),
    };
    match promise {
        Ok((promise, active_request_id)) => {
            tc.perform_microtask_checkpoint();
            let entry = InFlight {
                runtime_state: actor_runtime_state(tc),
                promise: v8::Global::new(tc, promise),
                context,
                scope: None,
                writes_before: None,
                request_id,
                active_request_id,
                reply: Some(Answer::Fetch(reply)),
                background: None,
                ops: std::collections::HashSet::new(),
                alarm: None,
                started: Instant::now(),
                trace: None,
                failure: None,
            };
            drop(guard);
            Begun::Running(Box::new(entry))
        }
        Err(error) => {
            drop(guard);
            let _ = reply.send(Err(error));
            Begun::Nothing
        }
    }
}

/// Start an entrypoint RPC's first turn.
///
/// The same shape as a fetch and deliberately so: call the handler, keep the
/// promise, and let `drive` pump it with no isolate held across the awaits.
/// The old dispatcher blocked until the promise settled, which is why RPC
/// needed a thread of its own and why `WorkerPool` outlived the fetch path.
fn begin_entrypoint_rpc(
    tc: &mut v8::PinScope,
    entrypoint: &str,
    method: &str,
    args: Vec<u8>,
    reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
) -> Begun {
    let context = IoContext::new();
    let guard = CurrentGuard::enter(context.clone());
    let global = tc.get_current_context().global(tc);
    let started = (|| {
        let key = v8::String::new(tc, "__dispatchEntrypointRpc").unwrap();
        let f: v8::Local<v8::Function> = global
            .get(tc, key.into())
            .ok_or_else(|| anyhow!("no __dispatchEntrypointRpc"))?
            .try_into()
            .map_err(|_| anyhow!("__dispatchEntrypointRpc is not a function"))?;
        let entrypoint = v8::String::new(tc, entrypoint).unwrap();
        let method = v8::String::new(tc, method).unwrap();
        let args = bytes_value(tc, args);
        let recv = v8::undefined(tc).into();
        begin_event_context(tc)?;
        let ret = f
            .call(tc, recv, &[entrypoint.into(), method.into(), args])
            .ok_or_else(|| anyhow!("entrypoint RPC threw"))?;
        match ret.try_cast::<v8::Promise>() {
            Ok(promise) => Ok(promise),
            Err(_) => resolved_promise(tc, ret),
        }
    })();
    match started {
        Ok(promise) => {
            tc.perform_microtask_checkpoint();
            let entry = InFlight {
                runtime_state: actor_runtime_state(tc),
                promise: v8::Global::new(tc, promise),
                context,
                scope: None,
                writes_before: None,
                request_id: None,
                active_request_id: None,
                reply: Some(Answer::Rpc(reply)),
                background: None,
                ops: std::collections::HashSet::new(),
                alarm: None,
                started: Instant::now(),
                trace: None,
                failure: None,
            };
            drop(guard);
            Begun::Running(Box::new(entry))
        }
        Err(error) => {
            let _ = end_event_context(tc);
            drop(guard);
            let _ = reply.send(Err(error));
            Begun::Nothing
        }
    }
}

/// for the JSON flavor, a `Uint8Array` for structured clone.
fn rpc_data_value<'s>(scope: &mut v8::PinScope<'s, '_>, data: RpcData) -> v8::Local<'s, v8::Value> {
    match data {
        RpcData::Json(json) => v8::String::new(scope, &json).unwrap().into(),
        RpcData::V8(bytes) => bytes_value(scope, bytes),
    }
}

/// The inverse: `__dispatchRpc` answers in the flavor it was asked in.
fn rpc_data_ret(scope: &mut v8::PinScope, ret: v8::Local<v8::Value>) -> RpcData {
    match view_bytes(ret) {
        Some(bytes) => RpcData::V8(bytes),
        None => RpcData::Json(ret.to_rust_string_lossy(scope)),
    }
}

fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// End the event, then answer with what the handler produced.
///
/// Ending it is what yields the `waitUntil` aggregate the entry keeps
/// driving after the caller has been served, so it happens for every answer
/// shape and on the error path too.
fn send_and_end<T>(
    tc: &mut v8::PinScope,
    reply: tokio::sync::oneshot::Sender<Result<T>>,
    value: Result<T>,
) -> Option<v8::Global<v8::Promise>> {
    let background = end_event_context(tc).ok().flatten();
    let _ = reply.send(value);
    background.map(|promise| v8::Global::new(tc, promise))
}

/// A committed-write position only counts when the handler advanced it; celld's
/// own activation writes are already below `before`.
pub(crate) fn write_delta(before: Option<u64>, after: Option<u64>) -> Option<u64> {
    match (before, after) {
        (Some(before), Some(after)) if after > before => Some(after),
        _ => None,
    }
}

/// What a Durable Object RPC method returned, plus the position its writes
/// reached, so the caller can hold the reply behind durability.
pub struct RpcOutcome {
    pub data: RpcData,
    pub write_position: Option<u64>,
}

/// What a claimed alarm still owes its bookkeeping.
///
/// `finish_alarm_handler` must run exactly once however the event ends, and
/// an event can now end without running JS at all — a budget overrun, a
/// handler waiting on nothing — so the entry carries the claim rather than
/// the caller that made it.
struct AlarmClaim {
    /// The instant the dispatch was judged due against, so the outcome is
    /// recorded against the same one.
    now_ms: i64,
}

/// Start one cell event: the half that runs before the handler can suspend.
///
/// Every cell event does the same four things — name the cell it belongs to,
/// sample the writes its answer will be gated on, open an event context, and
/// keep the promise the dispatcher returned. Only the call and the shape of
/// the answer differ, which is what the arguments say.
fn start_cell_event<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    scope: &str,
    answer: Answer,
    request_id: Option<RequestId>,
    capture_frames: bool,
    call: impl FnOnce(&mut v8::PinScope<'s, '_>) -> Result<v8::Local<'s, v8::Value>>,
) -> Begun {
    let context = IoContext::new();
    let guard = CurrentGuard::enter(context.clone());
    // Sampled before the handler runs so the output gate can tell a write
    // this event made from celld's own activation writes.
    let writes_before = storage::write_position(scope);
    // No guard: this frame is the context's outermost one and the context
    // ends with the event, so there is nothing to pop it before.
    context
        .egress
        .lock()
        .unwrap()
        .push((scope.to_string(), writes_before.unwrap_or(0)));
    if capture_frames {
        ws_capture_begin();
    }
    let started = (|| {
        begin_event_context(tc)?;
        let ret = call(tc)?;
        match ret.try_cast::<v8::Promise>() {
            Ok(promise) => Ok(promise),
            Err(_) => resolved_promise(tc, ret),
        }
    })();
    match started {
        Ok(promise) => {
            tc.perform_microtask_checkpoint();
            let entry = InFlight {
                runtime_state: actor_runtime_state(tc),
                promise: v8::Global::new(tc, promise),
                context,
                scope: Some(scope.to_string()),
                writes_before,
                request_id,
                // A cell's dispatcher registers the incoming request itself,
                // so there is nothing for the shell to finish; `cancel`
                // aborts by the job's own id.
                active_request_id: None,
                reply: Some(answer),
                background: None,
                ops: std::collections::HashSet::new(),
                alarm: None,
                started: Instant::now(),
                trace: None,
                failure: None,
            };
            drop(guard);
            Begun::Running(Box::new(entry))
        }
        Err(error) => {
            let _ = end_event_context(tc);
            drop(guard);
            answer.fail(error);
            Begun::Nothing
        }
    }
}

/// Look one of the harness dispatchers up on the current realm's global.
fn dispatcher<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    name: &str,
) -> Result<v8::Local<'s, v8::Function>> {
    let global = tc.get_current_context().global(tc);
    let key = v8::String::new(tc, name).unwrap();
    global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no {name}"))?
        .try_into()
        .map_err(|_| anyhow!("{name} is not a function"))
}

/// Start a cell event's first turn.
///
/// The counterpart of `begin` for the events a cell receives. Where the
/// blocking loop ran each of these to completion inside one call — and
/// serviced other cells' events inside *that* — each is now an entry a tokio
/// task drives, so two events of one cell interleave by suspending rather
/// than by nesting.
fn begin_cell(tc: &mut v8::PinScope, job: CellJob) -> Begun {
    match job {
        CellJob::Fetch {
            request_id,
            scope,
            name,
            url,
            method,
            body,
            headers,
            reply,
            order: _,
        } => {
            if let Err(error) = register_actor_name(tc, &scope, name.as_deref()) {
                let _ = reply.send(Err(error));
                return Begun::Nothing;
            }
            start_cell_event(tc, &scope, Answer::Fetch(reply), request_id, false, |tc| {
                let f = dispatcher(tc, "__dispatchTo")?;
                let arguments = [
                    v8::String::new(tc, &scope).unwrap().into(),
                    v8::String::new(tc, &url).unwrap().into(),
                    v8::String::new(tc, &method).unwrap().into(),
                    bytes_value(tc, body),
                    v8::String::new(tc, &serde_json::to_string(&headers)?)
                        .unwrap()
                        .into(),
                    match request_id {
                        Some(id) => v8::String::new(tc, &request_id_string(id)).unwrap().into(),
                        None => v8::null(tc).into(),
                    },
                ];
                let recv = v8::undefined(tc).into();
                f.call(tc, recv, &arguments)
                    .ok_or_else(|| anyhow!("dispatchTo threw"))
            })
        }
        CellJob::Rpc {
            scope,
            name,
            method,
            args,
            reply,
        } => {
            if let Err(error) = register_actor_name(tc, &scope, name.as_deref()) {
                let _ = reply.send(Err(error));
                return Begun::Nothing;
            }
            start_cell_event(tc, &scope, Answer::CellRpc(reply), None, false, |tc| {
                let f = dispatcher(tc, "__dispatchRpc")?;
                let arguments = [
                    v8::String::new(tc, &scope).unwrap().into(),
                    v8::String::new(tc, &method).unwrap().into(),
                    rpc_data_value(tc, args),
                ];
                let recv = v8::undefined(tc).into();
                f.call(tc, recv, &arguments)
                    .ok_or_else(|| anyhow!("dispatchRpc threw"))
            })
        }
        CellJob::WsOpen {
            scope,
            ws_id,
            protocol,
            reply,
        } => start_cell_event(tc, &scope, Answer::Ack(reply), None, false, |tc| {
            let f = dispatcher(tc, "__wsOpen")?;
            let arguments = [
                v8::String::new(tc, &scope).unwrap().into(),
                v8::Number::new(tc, ws_id as f64).into(),
                v8::String::new(tc, &protocol).unwrap().into(),
            ];
            let recv = v8::undefined(tc).into();
            f.call(tc, recv, &arguments)
                .ok_or_else(|| anyhow!("wsOpen threw"))
        }),
        CellJob::WsMessage {
            scope,
            ws_id,
            data,
            reply,
        } => start_cell_event(tc, &scope, Answer::WsMessage(reply), None, true, |tc| {
            let (name, data) = match data {
                WsIn::Text(text) => ("__wsMessage", v8::String::new(tc, &text).unwrap().into()),
                WsIn::Binary(bytes) => ("__wsBinary", bytes_value(tc, bytes)),
            };
            let f = dispatcher(tc, name)?;
            let arguments = [
                v8::String::new(tc, &scope).unwrap().into(),
                v8::Number::new(tc, ws_id as f64).into(),
                data,
            ];
            let recv = v8::undefined(tc).into();
            f.call(tc, recv, &arguments)
                .ok_or_else(|| anyhow!("WebSocket message dispatch threw"))
        }),
        CellJob::WsClosed {
            scope,
            ws_id,
            code,
            reason,
            was_clean,
            reply,
        } => start_cell_event(tc, &scope, Answer::Ack(reply), None, false, |tc| {
            let f = dispatcher(tc, "__wsClosed")?;
            let arguments = [
                v8::String::new(tc, &scope).unwrap().into(),
                v8::Number::new(tc, ws_id as f64).into(),
                v8::Number::new(tc, f64::from(code)).into(),
                v8::String::new(tc, &reason).unwrap().into(),
                v8::Boolean::new(tc, was_clean).into(),
            ];
            let recv = v8::undefined(tc).into();
            f.call(tc, recv, &arguments)
                .ok_or_else(|| anyhow!("wsClosed threw"))
        }),
        CellJob::Alarm {
            scope,
            scheduled_ms,
            claim,
            reply,
        } => begin_alarm(tc, &scope, scheduled_ms, claim, reply),
    }
}

/// Start an alarm, claiming the due entry so nothing else fires it.
///
/// The claim is recorded on the entry rather than closed here, because the
/// handler has not run yet: the outcome is known only when the event ends,
/// which is now many turns away.
fn begin_alarm(
    tc: &mut v8::PinScope,
    scope: &str,
    scheduled_ms: i64,
    claim: AlarmDispatch,
    reply: tokio::sync::oneshot::Sender<Result<(Option<i64>, Option<u64>)>>,
) -> Begun {
    let now = unix_now_ms();
    if now < scheduled_ms {
        let _ = reply.send(Err(anyhow!("alarm dispatched before its deadline")));
        return Begun::Nothing;
    }
    if let AlarmDispatch::Claimed(retry) = claim {
        let Some(scheduled_at) = storage::active_alarm_scheduled_time(scope) else {
            let _ = reply.send(Err(anyhow!("alarm dispatched without a claim")));
            return Begun::Nothing;
        };
        return fire_alarm_handler(tc, scope, scheduled_at, retry, None, reply);
    }
    let due_by = match claim {
        AlarmDispatch::Armed => i64::MAX,
        _ => now,
    };
    let Some((scheduled_at, retry)) = storage::due_alarm_entry(scope, due_by) else {
        // Nothing is due: another dispatch already ran it, or the handler
        // that armed it cleared it. Answer what stands now, with no delta —
        // no handler ran, so there is nothing written to prove.
        let _ = reply.send(Ok((storage::get_alarm(scope), None)));
        return Begun::Nothing;
    };
    storage::begin_alarm_handler(scope, scheduled_at);
    fire_alarm_handler(
        tc,
        scope,
        scheduled_at,
        retry,
        Some(AlarmClaim { now_ms: now }),
        reply,
    )
}

/// Call `alarm()`, carrying whatever claim its outcome must close.
fn fire_alarm_handler(
    tc: &mut v8::PinScope,
    scope: &str,
    scheduled_at: i64,
    retry: i64,
    claim: Option<AlarmClaim>,
    reply: tokio::sync::oneshot::Sender<Result<(Option<i64>, Option<u64>)>>,
) -> Begun {
    let begun = start_cell_event(tc, scope, Answer::Alarm(reply), None, false, |tc| {
        let f = dispatcher(tc, "__fireAlarm")?;
        let arguments = [
            v8::String::new(tc, scope).unwrap().into(),
            v8::Number::new(tc, scheduled_at as f64).into(),
            v8::Number::new(tc, retry as f64).into(),
        ];
        let recv = v8::undefined(tc).into();
        f.call(tc, recv, &arguments)
            .ok_or_else(|| anyhow!("alarm threw"))
    });
    match begun {
        Begun::Running(mut entry) => {
            entry.alarm = claim;
            Begun::Running(entry)
        }
        // The dispatcher never ran, so nothing can have changed the alarm.
        // Give the claim back as a failure that does not count against the
        // retry limit: the handler is not what failed.
        begun => {
            if let Some(claim) = claim {
                storage::finish_alarm_handler_with_retry_policy(scope, false, claim.now_ms, false);
            }
            begun
        }
    }
}

/// Resolve one completed op and run the microtasks that follow, with the
/// owning request's context current.
///
/// The half of a turn that resumes a suspended request. It knows nothing
/// about any other request: the caller has already established that this op
/// is `entry`'s.
fn deliver(
    tc: &mut v8::PinScope,
    entry: &mut InFlight,
    op: u64,
    res: Result<asyncrt::OpOut, String>,
) {
    entry.ops.remove(&op);
    let guard = CurrentGuard::enter(entry.context.clone());
    resolve_res(tc, op, res);
    tc.perform_microtask_checkpoint();
    drop(guard);
}

/// End a request whose client has hung up, so its handler stops running.
fn cancelled(tc: &mut v8::PinScope, entry: &mut InFlight) {
    if entry.reply.is_none() || !take_request_cancellation(entry.request_id) {
        return;
    }
    cancel(tc, entry);
}

/// End a request whose cancellation has already been taken.
///
/// Split from [`cancelled`] because a suspended request observes the
/// cancellation *outside* the isolate — the flag is host state and costs no
/// V8 — and only then enters to act on it.
fn cancel(tc: &mut v8::PinScope, entry: &mut InFlight) {
    let guard = CurrentGuard::enter(entry.context.clone());
    // The id that names this request to the JS side. A stateless handler is
    // registered by the shell, and only once it suspends, so the shell holds
    // the id; a cell's dispatcher registers the request itself, so the job's
    // own id is the one to abort. Aborting the wrong one — or neither —
    // leaves the target's `request.signal` unfired while the caller is told
    // the client has gone.
    if let Some(id) = entry.active_request_id.or(entry.request_id) {
        let _ = abort_incoming_request(tc, id);
    }
    // Finishing is the shell's to undo only where the shell registered.
    if let Some(id) = entry.active_request_id {
        finish_incoming_request(tc, id);
    }
    let background = end_event_context(tc).ok().flatten();
    entry.background = background.map(|promise| v8::Global::new(tc, promise));
    drop(guard);
    entry.fail(anyhow!("The client has disconnected"));
    // A cancelled handler with no waitUntil work has nothing left to drive.
    // Drop its host ops now, so their guards cancel routed work. Explicit
    // waitUntil work keeps its ops and continues after the client disconnects.
    if entry.background.is_none() {
        entry.abandon();
    }
}

/// Answer a request whose handler promise has settled, and retire its
/// `waitUntil` work once that settles too.
fn settle(tc: &mut v8::PinScope, entry: &mut InFlight) {
    if entry.reply.is_some() {
        let promise = v8::Local::new(tc, &entry.promise);
        match promise.state() {
            v8::PromiseState::Pending => {}
            v8::PromiseState::Fulfilled => {
                let guard = CurrentGuard::enter(entry.context.clone());
                let value = promise.result(tc);
                entry.answer_settled(tc, value);
                if let Some(request_id) = entry.active_request_id {
                    finish_incoming_request(tc, request_id);
                }
                drop(guard);
            }
            v8::PromiseState::Rejected => {
                let guard = CurrentGuard::enter(entry.context.clone());
                let reason = reject_reason(tc, promise);
                // The handler is what failed, so an alarm's failure here
                // counts against its retry limit — unlike a budget overrun
                // or a disconnect, which `fail` records as not counting.
                entry.settle_alarm(false, true);
                let _ = end_event_context(tc);
                if let Some(request_id) = entry.active_request_id {
                    finish_incoming_request(tc, request_id);
                }
                drop(guard);
                entry.fail(anyhow!("rejected: {reason}"));
            }
        }
    }
    if let Some(background) = &entry.background {
        let promise = v8::Local::new(tc, background);
        if !matches!(promise.state(), v8::PromiseState::Pending) {
            entry.background = None;
        }
    }
}

/// Begin a stateless request: build it, call the Worker's `fetch`, and hand
/// back the promise it returned.
///
/// The half of a request that runs *before* it can suspend. Split out from
/// driving it to completion so a caller can start several and drive them
/// together; the single-request path starts one and drives it immediately,
/// which is what it always did.
///
/// The caller owns the `IoContext` and must have it current: `__beginEvent`
/// runs here, and the frame it pushes belongs to this request.
fn start_fetch<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    fetch: v8::Local<'s, v8::Function>,
    url: &str,
    method: &str,
    body: &RequestBody,
    headers: &[(String, String)],
    request_id: Option<RequestId>,
) -> Result<Started<'s>> {
    let req = match request_id {
        Some(_) => make_incoming_request(tc, url, method, body, headers),
        None => make_request(tc, url, method, body.bytes(), headers),
    }?;
    let env = harness_env(tc)?;
    let recv = v8::undefined(tc).into();
    let f = fetch;
    let execution_ctx = begin_event_context(tc)?;
    let Some(ret) = f.call(tc, recv, &[req, env, execution_ctx]) else {
        // The handler threw synchronously. Termination carries its own error;
        // otherwise the pending exception is on the caller's TryCatch, which
        // is the only scope that can read it, so it formats the message.
        let error = take_execution_termination(tc);
        let _ = end_event_context(tc);
        return match error {
            Some(error) => Err(error),
            None => Ok(Started::Threw),
        };
    };
    // A handler that suspends must be reachable by an abort: register it as an
    // incoming request so a client disconnect can cancel it. One that already
    // settled cannot be cancelled, so its registration is cleared instead.
    let active_request_id = match request_id {
        Some(request_id)
            if ret
                .try_cast::<v8::Promise>()
                .is_ok_and(|promise| promise.state() == v8::PromiseState::Pending) =>
        {
            if let Err(error) = register_incoming_request(tc, request_id, req) {
                clear_request_cancellation(request_id);
                let _ = end_event_context(tc);
                return Err(error);
            }
            Some(request_id)
        }
        Some(request_id) => {
            clear_request_cancellation(request_id);
            None
        }
        None => None,
    };
    Ok(Started::Running(ret, active_request_id))
}

/// What `start_fetch` produced: a running handler, or a synchronous throw
/// whose exception only the caller's `TryCatch` can read.
enum Started<'s> {
    Running(v8::Local<'s, v8::Value>, Option<RequestId>),
    Threw,
}

impl Worker {
    /// Compile the worker module, wire the DO harness, and extract the entry
    /// `fetch`. `do_classes` come from the manifest; `bindings` maps a binding
    /// name to a DO class name (from wrangler metadata).
    ///
    /// The runtime builds a `WorkerConfig` directly; this helper is
    /// test-only.
    #[cfg(test)]
    pub fn load(options: WorkerConfigOptions, owned: &[String]) -> Result<Worker> {
        let config = Arc::new(WorkerConfig::new(options));
        Self::load_config(config, owned)
    }

    pub fn load_config(config: Arc<WorkerConfig>, owned: &[String]) -> Result<Worker> {
        Self::load_config_owned(config, Arc::from(owned.to_vec()))
    }

    fn load_config_owned(config: Arc<WorkerConfig>, owned: Arc<[String]>) -> Result<Worker> {
        let src = config.src.as_str();
        let script_name = config.script_name.as_str();
        let do_classes = config.do_classes.as_slice();
        let node = config.node.as_str();
        let compat = config.compat;
        let params = v8::CreateParams::default().heap_limits(0, v8_heap_limit_bytes());
        let mut isolate = v8::Isolate::new(params);
        // Dynamic `import()` of builtin specifiers; per-import()-call only.
        isolate.set_host_import_module_dynamically_callback(host_import_module_dynamically);
        let original_heap_limit = isolate.get_heap_statistics().heap_size_limit();
        let heap_limit_state = Arc::new(HeapLimitState {
            excessively_exceeded: AtomicBool::new(false),
            limit: original_heap_limit,
            last_gc_nudge: Mutex::new(None),
            #[cfg(all(test, celld_internal_tests))]
            forced_admission_refusal: AtomicBool::new(false),
        });
        let runtime_state = Arc::new(ActorRuntimeState {
            promises: std::sync::Mutex::new(PromiseMap::new()),
            egress: config.egress,
            ..Default::default()
        });
        let heap_limit_state_ptr =
            Arc::as_ptr(&heap_limit_state) as *mut HeapLimitState as *mut std::ffi::c_void;
        isolate.set_slot(heap_limit_state);
        isolate.set_slot(runtime_state.clone());
        isolate.set_slot(Arc::new(ModuleRegistry::default()));
        isolate.add_near_heap_limit_callback(near_heap_limit, heap_limit_state_ptr);
        let (context, fetch) = {
            v8::scope!(let hs, &mut isolate);
            let context = v8::Context::new(hs, Default::default());
            let cs = &mut v8::ContextScope::new(hs, context);
            let tc = std::pin::pin!(v8::TryCatch::new(cs));
            let scope = &mut tc.init();

            install_ops(scope, context);
            install_prelude(scope)?; // Web Platform APIs
            install_harness(scope)?; // DO object model + minimal Response
            install_lazy_globals(scope)?;
            // A global, so it must exist before the module evaluates: bundles
            // read Cloudflare.compatibilityFlags at module scope.
            inject_compatibility_flags(scope, compat)?;
            inject_storage_compatibility(scope, compat)?;

            let module = match compile_module(scope, "worker.js", src) {
                Some(m) => m,
                None => return Err(anyhow!("compile: {}", exc!(scope))),
            };
            register_stubs(scope, src, &config.modules); // cloudflare:*/node:* + text modules
            register_wasm_modules(scope, &config.modules);
            register_loader_modules(scope, &config.modules);
            module
                .instantiate_module(scope, resolve_external)
                .ok_or_else(|| anyhow!("instantiate: {}", exc!(scope)))?;
            let ev = match module.evaluate(scope) {
                Some(value) => value,
                None => {
                    if let Some(error) = take_execution_termination(scope) {
                        return Err(error);
                    }
                    return Err(anyhow!("evaluate: {}", exc!(scope)));
                }
            };
            if let Some(error) = take_execution_termination(scope) {
                return Err(error);
            }
            if let Ok(p) = ev.try_cast::<v8::Promise>() {
                if p.state() == v8::PromiseState::Rejected {
                    let r = p.result(scope);
                    let stk = r
                        .to_object(scope)
                        .and_then(|o| {
                            let k = v8::String::new(scope, "stack")?;
                            o.get(scope, k.into())
                        })
                        .map(|s| s.to_rust_string_lossy(scope))
                        .unwrap_or_default();
                    return Err(anyhow!(
                        "top-level rejected: {} | {}",
                        r.to_rust_string_lossy(scope),
                        stk.lines().take(4).collect::<Vec<_>>().join(" <- ")
                    ));
                }
            }

            let ns = module
                .get_module_namespace()
                .to_object(scope)
                .ok_or_else(|| anyhow!("ns"))?;

            // register each exported DO class into the harness registry
            for cn in do_classes {
                // The D1 class is the runtime's own and is never a worker
                // export. It is in `do_classes` so that it gets a namespace
                // key; reading it off the module namespace would find
                // `undefined` and overwrite the harness's registration.
                if cn == crate::deploy::D1_CLASS {
                    continue;
                }
                let key = v8::String::new(scope, cn).unwrap();
                let cls = ns
                    .get(scope, key.into())
                    .ok_or_else(|| anyhow!("DO class {cn} not exported"))?;
                register_class(scope, cn, cls)?;
            }
            inject_namespace_keys(scope, script_name, do_classes)?;
            inject_crons(scope, &config.crons)?;
            populate_cf_exports(scope, ns, do_classes)?;
            register_entrypoints(scope, ns)?;
            // build env from bindings and stash it in the harness
            build_env(scope, &config)?;
            // tell the harness which cells are local (route the rest cross-node)
            inject_routing(scope, owned.as_ref(), node)?;

            // entry fetch
            let dk = v8::String::new(scope, "default").unwrap();
            let default = ns
                .get(scope, dk.into())
                .ok_or_else(|| anyhow!("no default export"))?
                .to_object(scope)
                .ok_or_else(|| anyhow!("default not object"))?;
            let fk = v8::String::new(scope, "fetch").unwrap();
            let fetch_value = default
                .get(scope, fk.into())
                .ok_or_else(|| anyhow!("no fetch"))?;
            let default_is_entrypoint =
                default.is_function() && cell_registry_has(scope, "entrypoints", "default")?;
            let f: v8::Local<v8::Function> = if fetch_value.is_function() {
                fetch_value.try_into().expect("function casts to Function")
            } else if default_is_entrypoint {
                // A class-based default entrypoint (extends WorkerEntrypoint)
                // keeps fetch on the prototype, so route through the
                // harness's cached instance like a named entrypoint. Only a
                // registered entrypoint dispatches that way — any other
                // callable would load fine and then 500 on every request.
                compile_fn(
                    scope,
                    "(req) => globalThis.__dispatchEntrypointFetch('default', req)",
                )?
            } else if default.is_function() && cell_registry_has(scope, "doExports", "default")? {
                return Err(anyhow!(
                    "the default export is a Durable Object class; export a fetch \
                     handler or a WorkerEntrypoint class as the default"
                ));
            } else {
                return Err(anyhow!("fetch not fn"));
            };

            // Lets a self-targeted service binding invoke the handler in
            // this isolate instead of crossing to a pool thread.
            {
                let cell_key = v8::String::new(scope, "__cell").unwrap();
                if let Some(cell) = context
                    .global(scope)
                    .get(scope, cell_key.into())
                    .and_then(|value| value.to_object(scope))
                {
                    let key = v8::String::new(scope, "selfFetch").unwrap();
                    cell.set(scope, key.into(), f.into());
                    // Optional scheduled handler, reached by a self-targeted
                    // service binding's scheduled().
                    let sk = v8::String::new(scope, "scheduled").unwrap();
                    let key_ = v8::String::new(scope, "selfScheduled").unwrap();
                    let own = default
                        .get(scope, sk.into())
                        .filter(|handler| handler.is_function());
                    if let Some(handler) = own {
                        cell.set(scope, key_.into(), handler);
                    } else if default_is_entrypoint {
                        // A class-based default entrypoint keeps scheduled on
                        // the prototype; dispatch through the cached instance
                        // like fetch above.
                        let pk = v8::String::new(scope, "prototype").unwrap();
                        let proto_scheduled = default
                            .get(scope, pk.into())
                            .and_then(|proto| proto.to_object(scope))
                            .and_then(|proto| proto.get(scope, sk.into()));
                        if proto_scheduled.is_some_and(|handler| handler.is_function()) {
                            let shim = compile_fn(
                                scope,
                                "(ctrl) => globalThis.__dispatchEntrypointScheduled('default', ctrl)",
                            )?;
                            cell.set(scope, key_.into(), shim.into());
                        }
                    }
                }
            }
            (v8::Global::new(scope, context), v8::Global::new(scope, f))
        };
        Ok(Worker {
            inner: Some(WorkerIsolate {
                // Every setup scope above has closed, so nothing is entered
                // on top of this isolate and it can be handed over.
                // SAFETY: `into_shared` requires every piece of embedder
                // state hanging off this isolate to be `Send`, because it
                // migrates between threads and is dropped on whichever one
                // holds the lock last. This isolate carries exactly:
                //
                // - three slots, whose types the assertion below pins as
                //   `Send + Sync`;
                // - `near_heap_limit`, a bare fn pointer whose only captured
                //   state is a raw pointer into the `HeapLimitState` above;
                // - `host_import_module_dynamically`, a bare fn pointer that
                //   captures nothing;
                // - a default `CreateParams` allocator, owned by V8.
                //
                // Nothing else is attached, and the assertion fails the build
                // if a slot type ever stops being thread-safe.
                //
                // 152.1.0 made this fallible. None of the four refusals can
                // hold here — this isolate is entered, is not a snapshot
                // creator, has no C++ heap, and has taken no weak handles —
                // so a refusal is a bug in the setup above, not a condition
                // to recover from. It panics with the reason named.
                isolate: unsafe { isolate.try_into_shared() }
                    .unwrap_or_else(|error| panic!("cell isolate cannot be shared: {error}")),
                realm: Realm { context, fetch },
                original_heap_limit,
                compat,
                cells: storage::Cells::default(),
            }),
        })
    }

    /// Restore an idFromName() actor's human-readable identity before its
    /// constructor runs. The host calls this on activation and before a named
    /// request is dispatched.
    /// Record whether this isolate hosts `cell`, so the harness can dispatch
    /// to it in-isolate instead of going back out through the host.
    ///
    /// Baked in at load time until cells shared isolates, when an isolate
    /// served exactly one cell for its whole life and the list could not
    /// change. Now a cell arrives at an isolate that already exists, so the
    /// list has to move with placement.
    ///
    /// Getting this wrong is not symmetric. A cell missing from the list
    /// still works: the dispatch goes out to the host, which routes it
    /// straight back here, costing a round trip. A cell wrongly *in* the
    /// list is a cell this isolate would try to serve without hosting it, so
    /// the shell sets it when it takes a residency and clears it when the
    /// residency ends.
    pub fn own_cell(
        &mut self,
        cell: &str,
        db_path: Option<&str>,
        owned: bool,
    ) -> Result<Option<i64>> {
        let compat = self.inner.as_ref().expect("live worker isolate").compat;
        let (mut locker, _cells) = self.lock();
        v8::scope!(let hs, &mut *locker);
        let realm = self.realm(hs);
        let context = realm.context;
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        adopt_cell(&mut tc.init(), cell, db_path, owned, compat)
    }

    /// Drain the alarm moves the last turn committed in this isolate.
    ///
    /// An alarm move is a turn output, exactly like the ops a turn starts:
    /// the drive that ran the turn reports it to the host, so a handler
    /// that arms an alarm and then awaits it is schedulable immediately,
    /// not when the request ends. A separate call rather than part of the
    /// turn methods' return because stateless turns cannot move an alarm
    /// and never pay for it.
    pub fn take_alarm_moves(&mut self) -> Vec<(String, i64)> {
        let Some(inner) = self.inner.as_mut() else {
            return Vec::new();
        };
        let (_locker, _cells) = inner.lock();
        storage::take_alarm_moves()
    }

    pub fn set_id_name(&mut self, scope: &str, name: &str) -> Result<()> {
        let (mut locker, _cells) = self.lock();
        v8::scope!(let hs, &mut *locker);
        let realm = self.realm(hs);
        let context = realm.context;
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        register_actor_name(&mut tc.init(), scope, Some(name))
    }

    /// Start a cell event's first turn.
    ///
    /// The cell counterpart of `turn_begin`: it hands back what is now in
    /// flight for `runtime::drive_cell` to pump.
    pub fn turn_begin_cell(
        &mut self,
        job: CellJob,
        trace: Option<crate::telemetry::TraceIds>,
    ) -> (Option<InFlight>, Vec<Op>) {
        let Some(inner) = self.inner.as_mut() else {
            return (None, Vec::new());
        };
        let (mut locker, _cells) = inner.lock();
        inner.recover_heap(&mut locker);
        v8::scope!(let hs, &mut *locker);
        let realm = inner.realm(hs);
        let context = realm.context;
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();

        let previous = install_trace(tc, trace.as_ref());
        let out = match begin_cell(tc, job) {
            Begun::Running(mut entry) => {
                entry.trace = trace;
                let ops = finish_turn(tc, &mut entry);
                (Some(*entry), ops)
            }
            Begun::Threw(answer) => {
                answer.fail(anyhow!("cell event threw: {}", exc!(tc)));
                (None, Vec::new())
            }
            Begun::Nothing => (None, Vec::new()),
        };
        restore_trace(tc, previous);
        out
    }

    /// Enter the isolate solely to see whether another event settled this
    /// one's promise.
    ///
    /// A promise resolved by a different entry has no waker pointing here,
    /// so the driving task cannot be told and has to look. That is why this
    /// is a poll and not an event, and it is the same reason the blocking
    /// loop woke every 10 ms.
    pub fn turn_poll(&mut self, entry: &mut InFlight) -> Vec<Op> {
        let Some(inner) = self.inner.as_mut() else {
            return Vec::new();
        };
        let (mut locker, _cells) = inner.lock();
        v8::scope!(let hs, &mut *locker);
        let realm = inner.realm(hs);
        let context = realm.context;
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();

        let previous = install_trace(tc, entry.trace.as_ref());
        let ops = finish_turn(tc, entry);
        restore_trace(tc, previous);
        ops
    }

    /// Enter the isolate solely to record how a claimed alarm ended.
    ///
    /// Reached only when the event ended without running JS again — a budget
    /// overrun, or a handler waiting on nothing. The bookkeeping is storage
    /// the isolate owns, so it cannot be done from the driving task.
    pub fn turn_finish_alarm(&mut self, entry: &mut InFlight) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };
        let (_locker, _cells) = inner.lock();
        entry.settle_alarm(false, false);
    }
}

// ---- native ops exposed to JS ----

/// Host ops, defined non-enumerable. They are runtime internals: a bundle
/// walking `globalThis` must not find them, let alone `new` one — `for (const
/// k in globalThis) new globalThis[k]()` used to reach `__actor_abort` and
/// kill the actor.
macro_rules! ops {
    ($scope:expr, $global:expr, $($name:literal => $op:path),* $(,)?) => {
        $({
            let f = v8::Function::new($scope, $op).unwrap();
            let k = v8::String::new($scope, $name).unwrap();
            $global.define_own_property(
                $scope, k.into(), f.into(), v8::PropertyAttribute::DONT_ENUM);
        })*
    };
}

fn install_ops(scope: &mut v8::PinScope, context: v8::Local<v8::Context>) {
    let global = context.global(scope);
    ops! { scope, global,
        "__heap_limit_excessively_exceeded" =>
            op_heap_limit_excessively_exceeded,
        "__heap_over_admission_share" => op_heap_over_admission_share,
        "__ws_send" => websocket::op_ws_send,
        "__ws_send_binary" => websocket::op_ws_send_binary,
        "__ws_close" => websocket::op_ws_close,
        "__ws_alloc" => websocket::op_ws_alloc,
        "__ws_accept" => websocket::op_ws_accept,
        "__ws_accept_regular" => websocket::op_ws_accept_regular,
        "__ws_list" => websocket::op_ws_list,
        "__ws_attachment_set" => websocket::op_ws_attachment_set,
        "__ws_auto_response_set" => websocket::op_ws_auto_response_set,
        "__ws_auto_response_get" => websocket::op_ws_auto_response_get,
        "__ws_auto_response_ts" => websocket::op_ws_auto_response_ts,
        "__ws_connect" => websocket::op_ws_connect,
        "__ws_bind_target" => websocket::op_ws_bind_target,
        "__ws_next" => websocket::op_ws_next,
        "__ws_upgrade" => websocket::op_ws_upgrade,
        "__storage_get" => storage_ops::op_storage_get,
        "__storage_get_many" => storage_ops::op_storage_get_many,
        "__sql_exec" => storage_ops::op_sql_exec,
        "__sql_ingest" => storage_ops::op_sql_ingest,
        "__sql_cursor_start" => storage_ops::op_sql_cursor_start,
        "__sql_cursor_next" => storage_ops::op_sql_cursor_next,
        "__sql_cursor_close" => storage_ops::op_sql_cursor_close,
        "__sql_database_size" => storage_ops::op_sql_database_size,
        "__d1_run" => storage_ops::op_d1_run,
        "__storage_transaction_control" => storage_ops::op_storage_transaction_control,
        "__log" => op_log,
        "__storage_put" => storage_ops::op_storage_put,
        "__storage_put_many" => storage_ops::op_storage_put_many,
        "__storage_queue_put" => storage_ops::op_storage_queue_put,
        "__storage_queue_put_many" => storage_ops::op_storage_queue_put_many,
        "__storage_put_serialized" => storage_ops::op_storage_put_serialized,
        "__storage_queue_put_serialized" => storage_ops::op_storage_queue_put_serialized,
        "__storage_flush_pending_puts" => storage_ops::op_storage_flush_pending_puts,
        "__storage_cancel_pending_puts" => storage_ops::op_storage_cancel_pending_puts,
        "__actor_abort" => op_actor_abort,
        "__cron_plan" => op_cron_plan,
        "__process_exit" => op_process_exit,
        "__storage_delete" => storage_ops::op_storage_delete,
        "__storage_delete_many" => storage_ops::op_storage_delete_many,
        "__storage_list" => storage_ops::op_storage_list,
        "__storage_sync_list_start" => storage_ops::op_storage_sync_list_start,
        "__storage_sync_list_next" => storage_ops::op_storage_sync_list_next,
        "__storage_delete_all" => storage_ops::op_storage_delete_all,
        "$$urlParse" => op_url_parse,
        "$$urlPatternParse" => op_urlpattern_parse,
        "$$urlPatternMatchInput" => op_urlpattern_match_input,
        "$$atob" => op_atob,
        "$$btoa" => op_btoa,
        "$$textDecoderLabel" => op_text_decoder_label,
        "$$textDecoderNew" => op_text_decoder_new,
        "$$textDecoderDecode" => op_text_decoder_decode,
        "$$textDecoderDecodeOnce" => op_text_decoder_decode_once,
        "$$textDecoderFree" => op_text_decoder_free,
        "__alarm_set" => op_alarm_set,
        "__alarm_get" => op_alarm_get,
        "__alarm_delete" => op_alarm_delete,
        "__loader_load" => op_loader_load,
        "__loader_fetch" => op_loader_fetch,
        "__loader_rpc" => op_loader_rpc,
        "__loader_drop" => op_loader_drop,
        "__do_call" => op_do_call,
        "__writePosition" => op_write_position,
        "__gateWrite" => op_gate_write,
        "__svc_call" => op_svc_call,
        "__svc_call_cancellable" => op_svc_call_cancellable,
        "__svc_rpc" => op_svc_rpc,
        "__do_call_cancellable" => op_do_call_cancellable,
        "__do_call_cancel" => op_do_call_cancel,
        "__do_id" => op_do_id,
        "__rpc_call" => op_rpc_call,
        "__sc_encode" => storage_ops::op_sc_encode,
        "__sc_decode" => storage_ops::op_sc_decode,
        "__op_fetch" => op_fetch,
        "__asset_fetch" => op_asset_fetch,
        "__http_stream_read" => op_http_stream_read,
        "__http_stream_cancel" => op_http_stream_cancel,
        "__http_stream_tee" => op_http_stream_tee,
        "__response_stream_create" => op_response_stream_create,
        "__response_stream_write" => op_response_stream_write,
        "__response_stream_closed" => op_response_stream_closed,
        "__response_stream_close" => op_response_stream_close,
        "__op_timer" => op_timer,
        "__timer_alloc" => op_timer_alloc,
        "__gate_acquire" => op_gate_acquire,
        "__gate_wait" => op_gate_wait,
        "__gate_release" => op_gate_release,
        "__timer_cancel" => op_timer_cancel,
        "__crypto_operation" => crypto::op_crypto_operation,
        "$$randomValues" => crypto::op_webcrypto_random,
        "$$digest" => crypto::op_webcrypto_digest,
        "$$hmacSign" => crypto::op_webcrypto_hmac_sign,
        "$$hmacVerify" => crypto::op_webcrypto_hmac_verify,
        "$$aesEncrypt" => crypto::op_webcrypto_aes_encrypt,
        "$$aesDecrypt" => crypto::op_webcrypto_aes_decrypt,
        "$$pbkdf2" => crypto::op_node_pbkdf2,
        "$$hkdf" => crypto::op_node_hkdf,
        "$$timingSafeEqual" => crypto::op_timing_safe_equal,
        "__event_begin" => op_event_begin,
        "__event_end" => op_event_end,
        "__wait_until" => op_wait_until,
        "__event_depth" => op_event_depth,
        "__als_get" => op_als_get,
        "__als_set" => op_als_set,
        "__util_type_flags" => op_util_type_flags,
        "__util_constructor_name" => op_util_constructor_name,
        "__util_proxy_details" => op_util_proxy_details,
        "__util_promise_details" => op_util_promise_details,
        "__util_preview_entries" => op_util_preview_entries,
        "__builtin_module" => op_builtin_module,
        "__zlib" => zlib::op_zlib,
        "__zlib_stream_new" => zlib::op_zlib_stream_new,
        "__zlib_stream_push" => zlib::op_zlib_stream_push,
        "__zlib_stream_end" => zlib::op_zlib_stream_end,
        "__zlib_stream_drop" => zlib::op_zlib_stream_drop,
    }
    #[cfg(all(test, celld_internal_tests))]
    ops! { scope, global,
        "__test_gc" => op_test_gc,
        "__loader_count" => op_loader_count,
        "__test_set_heap_limit_excessively_exceeded" =>
            op_test_set_heap_limit_excessively_exceeded,
        "__test_force_heap_admission_refusal" =>
            op_test_force_heap_admission_refusal,
        "__test_heap_share" => op_test_heap_share,
        "__sql_set_max_page_count_for_test" =>
            storage_ops::op_sql_set_max_page_count_for_test,
        "__sql_set_write_fault_for_test" => storage_ops::op_sql_set_write_fault_for_test,
        "__sql_set_cache_size_for_test" => storage_ops::op_sql_set_cache_size_for_test,
        "__sql_set_interrupt_fault_for_test" =>
            storage_ops::op_sql_set_interrupt_fault_for_test,
        "__sql_register_nomem_function_for_test" =>
            storage_ops::op_sql_register_nomem_function_for_test,
    }
}

/// Create a JS promise for an async op `id` and return it to JS.
fn promise_for<'s>(scope: &mut v8::PinScope<'s, '_>, id: u64) -> v8::Local<'s, v8::Value> {
    let resolver = v8::PromiseResolver::new(scope).unwrap();
    let promise = resolver.get_promise(scope);
    promise_store(scope, id, v8::Global::new(scope, resolver));
    promise.into()
}

/// Async cross-node dispatch: hand the fetch to the tokio proxy task and await
/// its reply off-thread — the JS thread is never blocked. Resolves to a JSON
/// `{status, body, headers}` string the harness turns back into a Response.
/// `__svc_rpc(script, entrypoint, method, argsSc)` -> Promise<Uint8Array>;
/// arguments and result are V8 structured-clone bytes.
fn op_svc_rpc(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let script = args.get(0).to_rust_string_lossy(scope);
    let entrypoint = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let call_args = view_bytes(args.get(3)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let gate = egress_gate_request();
    let request = SvcRpcReq {
        script,
        entrypoint,
        method,
        args: call_args,
        reply: tx,
    };
    let id = asyncrt::enqueue(async move {
        gated_channel_send(gate, &SVC_RPC_TX, request, "no service binding channel").await?;
        match rx.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(format!("{error}")),
            Err(error) => Err(format!("service dropped: {error}")),
        }
    });
    rv.set(promise_for(scope, id));
}

/// `__svc_call(script, url, method, body, headersJson)` -> Promise<json>.
/// The service-binding equivalent of `__do_call`: no scope to resolve and no
/// cancellation token, just a handoff to the target script's pool.
fn op_svc_call(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    op_svc_call_impl(scope, args, &mut rv, false);
}

fn op_svc_call_cancellable(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    op_svc_call_impl(scope, args, &mut rv, true);
}

fn op_svc_call_impl(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: &mut v8::ReturnValue<v8::Value>,
    cancellable: bool,
) {
    let script = args.get(0).to_rust_string_lossy(scope);
    let url = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let body = view_bytes(args.get(3)).unwrap_or_default();
    let headers =
        serde_json::from_str(&args.get(4).to_rust_string_lossy(scope)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    // Shares the Durable Object cancel registry, so `__do_call_cancel` works
    // unchanged for a service call.
    let (request_id, cancel, cancel_guard) = if cancellable {
        let request_id = next_do_request_id();
        let (cancel_sender, cancel_receiver) = tokio::sync::oneshot::channel();
        do_call_cancels()
            .lock()
            .unwrap()
            .insert(request_id, cancel_sender);
        (
            Some(request_id),
            Some(cancel_receiver),
            Some(DoCallCancelGuard::new(request_id)),
        )
    } else {
        (None, None, None)
    };
    let gate = egress_gate_request();
    let request = SvcCallReq {
        cancel,
        script,
        url,
        method,
        body,
        headers,
        reply: tx,
    };
    let id = asyncrt::enqueue(async move {
        let mut cancel_guard = cancel_guard;
        gated_channel_send(gate, &SVC_CALL_TX, request, "no service binding channel").await?;
        let result = match rx.await {
            Ok(Ok(response)) => Ok(encode_http_response(response, true)),
            Ok(Err(error)) => Err(format!("{error}")),
            Err(error) => Err(format!("service dropped: {error}")),
        };
        if let Some(cancel_guard) = cancel_guard.as_mut() {
            cancel_guard.disarm();
        }
        result
    });
    let promise = promise_for(scope, id);
    if let Some(request_id) = request_id {
        if let Some(object) = promise.to_object(scope) {
            let key = v8::String::new(scope, "__celldCancelId").unwrap();
            let value = v8::String::new(scope, &request_id.to_string()).unwrap();
            object.set(scope, key.into(), value.into());
        }
    }
    rv.set(promise);
}

// --- Worker Loader: dynamic isolates for Code Mode ---
// A running Worker creates a fresh isolate from code it supplies at runtime
// and invokes it. The loaded isolate uses the same turn driver as every
// stateless Worker, so an awaited operation holds no thread or isolate.

// Mirror the workerd dynamic-worker limits (worker-loader.c++): 64 MiB total
// module bytes, 1 MiB env. Messages match so the conformance cases pass.
const MAX_DYNAMIC_WORKER_CODE_SIZE: usize = 64 * 1024 * 1024;
const MAX_DYNAMIC_WORKER_ENV_SIZE: usize = 1024 * 1024;

#[derive(Clone)]
enum LoaderState {
    Loading,
    Ready(Arc<crate::pool::Slot>),
    Failed(Arc<str>),
}

type LoaderRegistry = HashMap<u64, tokio::sync::watch::Receiver<LoaderState>>;
static LOADER_REGISTRY: OnceLock<std::sync::Mutex<LoaderRegistry>> = OnceLock::new();
static LOADER_NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn loader_registry() -> &'static std::sync::Mutex<LoaderRegistry> {
    LOADER_REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn loader_throw(scope: &mut v8::PinScope, message: &str) {
    let message = v8::String::new(scope, message).unwrap();
    let exception = v8::Exception::error(scope, message);
    scope.throw_exception(exception);
}

async fn loaded_worker_slot(
    mut state: tokio::sync::watch::Receiver<LoaderState>,
) -> Result<Arc<crate::pool::Slot>, String> {
    loop {
        match state.borrow().clone() {
            LoaderState::Ready(slot) => return Ok(slot),
            LoaderState::Failed(error) => return Err(error.to_string()),
            LoaderState::Loading => {}
        }
        state
            .changed()
            .await
            .map_err(|_| "worker loader: load task dropped".to_string())?;
    }
}

/// `__loader_load(codeJson)` -> stub id. Builds a WorkerConfig from the
/// supplied modules and registers its asynchronous load state. Compilation
/// runs on Tokio's blocking pool. Calls wait for that result and then use the
/// normal stateless turn driver.
fn op_loader_load(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let code_json = args.get(0).to_rust_string_lossy(scope);
    let code: serde_json::Value = match serde_json::from_str(&code_json) {
        Ok(code) => code,
        Err(e) => return loader_throw(scope, &format!("worker loader: {e}")),
    };
    let Some(main) = code.get("mainModule").and_then(|v| v.as_str()) else {
        return loader_throw(scope, "worker loader: missing mainModule");
    };
    let Some(src) = code
        .get("modules")
        .and_then(|m| m.get(main))
        .and_then(|v| v.as_str())
        .map(str::to_string)
    else {
        return loader_throw(
            scope,
            &format!("worker loader: module {main:?} missing or not a string"),
        );
    };
    // Every module other than the main one is a sibling the main module may
    // import. JS modules arrive in the JSON as strings; anything else there is
    // rejected — JSON.stringify would already have mangled it, and dropping it
    // silently leaves the loaded worker failing instantiation with an
    // unresolved specifier that never names the cause. Wasm modules arrive
    // out of band as the second argument, an array of `[name, Uint8Array]`
    // pairs, which keeps the blobs out of the JSON payload entirely.
    let mut modules: Vec<(String, ModuleSource)> = Vec::new();
    if let Some(map) = code.get("modules").and_then(|m| m.as_object()) {
        for (name, value) in map.iter().filter(|(name, _)| name.as_str() != main) {
            let Some(source) = value.as_str() else {
                return loader_throw(
                    scope,
                    &format!("worker loader: module {name:?} must be a string or wasm bytes"),
                );
            };
            modules.push((name.clone(), ModuleSource::EsModule(source.to_string())));
        }
    }
    let sideband = args.get(1);
    if !sideband.is_undefined() {
        let Ok(entries) = v8::Local::<v8::Array>::try_from(sideband) else {
            return loader_throw(scope, "worker loader: wasm modules must be an array");
        };
        for index in 0..entries.length() {
            let entry = entries
                .get_index(scope, index)
                .and_then(|entry| v8::Local::<v8::Array>::try_from(entry).ok())
                .and_then(|pair| Some((pair.get_index(scope, 0)?, pair.get_index(scope, 1)?)))
                .filter(|(name, _)| name.is_string())
                .and_then(|(name, value)| {
                    let view = v8::Local::<v8::ArrayBufferView>::try_from(value).ok()?;
                    Some((name.to_rust_string_lossy(scope), view))
                });
            let Some((name, view)) = entry else {
                return loader_throw(scope, "worker loader: malformed wasm module entry");
            };
            // A name carried by both the JSON map and the side-band would
            // silently shadow one module with the other in the registry;
            // refuse it the way a non-string JSON module is refused.
            if name == main || modules.iter().any(|(existing, _)| *existing == name) {
                return loader_throw(
                    scope,
                    &format!("worker loader: duplicate module name {name:?}"),
                );
            }
            let mut bytes = vec![0u8; view.byte_length()];
            view.copy_contents(&mut bytes);
            modules.push((name, ModuleSource::Wasm(bytes.into())));
        }
    }
    // Total module bytes, checked before compiling anything (the oversized
    // module is never parsed) — the extra modules are not yet loaded but do
    // count against the ceiling, as upstream.
    let code_size: usize = src.len()
        + modules
            .iter()
            .map(|(_, source)| match source {
                ModuleSource::Text(source) | ModuleSource::EsModule(source) => source.len(),
                ModuleSource::Wasm(bytes) => bytes.len(),
            })
            .sum::<usize>();
    if code_size > MAX_DYNAMIC_WORKER_CODE_SIZE {
        return loader_throw(
            scope,
            &format!(
                "Dynamic Worker code size ({code_size} bytes) exceeds the \
                 maximum allowed size of {MAX_DYNAMIC_WORKER_CODE_SIZE} bytes."
            ),
        );
    }
    // Plain JSON `env` values merge onto the loaded worker's env; capability
    // stubs are not yet supported and would fail to serialize upstream in JS.
    let loader_env = code
        .get("env")
        .filter(|v| !v.is_null())
        .map(|v| v.to_string());
    if let Some(env) = &loader_env {
        if env.len() > MAX_DYNAMIC_WORKER_ENV_SIZE {
            return loader_throw(
                scope,
                &format!(
                    "Dynamic Worker env size ({} bytes) exceeds the maximum \
                     allowed size of {MAX_DYNAMIC_WORKER_ENV_SIZE} bytes.",
                    env.len()
                ),
            );
        }
    }
    // globalOutbound: absent inherits the caller's authority, null denies
    // ambient egress, a Fetcher (broker) is not implemented yet.
    let egress = match code.get("globalOutbound") {
        None => actor_runtime_state(scope).egress,
        Some(v) if v.is_null() => EgressPolicy::Deny,
        Some(_) => {
            return loader_throw(
                scope,
                "worker loader: globalOutbound broker is not implemented yet",
            );
        }
    };
    // Bound live loaded workers so a runaway agent loop cannot exhaust
    // isolates. Evicted workers (dropped stubs) free their slot.
    let max = crate::env_vars::positive_or("CELLD_MAX_LOADED_WORKERS", 256)
        .expect("validated CELLD_MAX_LOADED_WORKERS");
    if loader_registry().lock().unwrap().len() >= max {
        return loader_throw(
            scope,
            &format!("worker loader: too many loaded workers (limit {max})"),
        );
    }
    // Honor the WorkerCode's declared compatibility (workerd worker_compat
    // reads snake_case keys); Code Mode workers keep RPC on regardless.
    let mut compat = crate::worker_compat(&serde_json::json!({
        "compatibility_date": code.get("compatibilityDate"),
        "compatibility_flags": code.get("compatibilityFlags"),
    }));
    compat.js_rpc = true;
    let id = LOADER_NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let config = Arc::new(
        WorkerConfig::new(WorkerConfigOptions {
            src,
            script_name: format!("__loader:{id}"),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            r2_bindings: Vec::new(),
            // A loaded worker reaches D1 only if its parent injects a stub;
            // ambient bindings are exactly what Code Mode withholds.
            d1_bindings: Vec::new(),
            ai_binding: None,
            vars: Vec::new(),
            node: String::new(),
            modules,
            compat,
        })
        .with_egress(egress)
        .with_loader_env(loader_env),
    );
    let handle = match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle,
        Err(error) => return loader_throw(scope, &format!("worker loader: {error}")),
    };
    let (loaded, state) = tokio::sync::watch::channel(LoaderState::Loading);
    loader_registry().lock().unwrap().insert(id, state);
    handle.spawn(async move {
        let state =
            match tokio::task::spawn_blocking(move || Worker::load_config(config, &[])).await {
                Ok(Ok(worker)) => LoaderState::Ready(crate::pool::Slot::standalone(worker)),
                Ok(Err(error)) => LoaderState::Failed(Arc::from(format!("{error}"))),
                Err(error) => LoaderState::Failed(Arc::from(format!(
                    "worker loader: load task failed: {error}"
                ))),
            };
        loaded.send_replace(state);
    });
    rv.set(v8::Number::new(scope, id as f64).into());
}

/// `__loader_fetch(id, url, method, body, headersJson)` -> Promise<json>. The
/// loaded-worker analog of `__svc_call`: encodes the response the same way.
fn op_loader_fetch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let url = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let body = view_bytes(args.get(3)).unwrap_or_default();
    let headers =
        serde_json::from_str(&args.get(4).to_rust_string_lossy(scope)).unwrap_or_default();
    let loaded = loader_registry().lock().unwrap().get(&id).cloned();
    let async_id = asyncrt::enqueue(async move {
        let state = loaded.ok_or_else(|| "worker loader: unknown worker".to_string())?;
        let slot = loaded_worker_slot(state).await?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Fetch {
            queued_at: Instant::now(),
            url,
            method,
            body: body.into(),
            headers,
            request_id: None,
            reply,
        };
        let driving = tokio::spawn(crate::runtime::drive(slot, job, None));
        match receive.await {
            Ok(Ok(response)) => Ok(encode_http_response(response, false)),
            Ok(Err(error)) => Err(format!("{error}")),
            Err(_) => match driving.await {
                Err(error) => Err(format!("loaded worker task died: {error}")),
                Ok(()) => Err("loaded worker dropped response".to_string()),
            },
        }
    });
    rv.set(promise_for(scope, async_id));
}

/// `__loader_rpc(id, entrypoint, method, argsSc)` -> Promise<Uint8Array>. The
/// loaded-worker analog of `__svc_rpc`: a named-entrypoint method call whose
/// args and result are V8 structured-clone bytes.
fn op_loader_rpc(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let entrypoint = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let call_args = view_bytes(args.get(3)).unwrap_or_default();
    let loaded = loader_registry().lock().unwrap().get(&id).cloned();
    let async_id = asyncrt::enqueue(async move {
        let state = loaded.ok_or_else(|| "worker loader: unknown worker".to_string())?;
        let slot = loaded_worker_slot(state).await?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Rpc {
            entrypoint,
            method,
            args: call_args,
            reply,
        };
        let driving = tokio::spawn(crate::runtime::drive(slot, job, None));
        match receive.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(format!("{error}")),
            Err(_) => match driving.await {
                Err(error) => Err(format!("loaded worker task died: {error}")),
                Ok(()) => Err("loaded worker dropped RPC result".to_string()),
            },
        }
    });
    rv.set(promise_for(scope, async_id));
}

/// `__loader_drop(id)` — evict a loaded worker. Called from a
/// FinalizationRegistry when its stub is GC'd. Removing the registry entry
/// drops the isolate after any calls that already cloned its load state end.
fn op_loader_drop(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    if let Some(state) = loader_registry().lock().unwrap().remove(&id) {
        // This op runs while the parent isolate is entered. The load state can
        // own the loaded isolate, and V8 forbids dropping one isolate while a
        // different isolate is entered on the same thread. Hand the final
        // reference to the host scheduler so destruction happens after this
        // turn has left V8.
        asyncrt::op_handle().spawn(async move { drop(state) });
    }
}

/// `__writePosition(scope)` -> the cell's committed-write position (a number),
/// or `null` when the scope has no open connection. The co-hosted DO fast path
/// samples this around an inline `fetch` to tell a write from a read.
fn op_write_position(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    match storage::write_position(&cell) {
        Some(position) => rv.set(v8::Number::new(scope, position as f64).into()),
        None => rv.set(v8::null(scope).into()),
    }
}

/// `__gateWrite(scope, positionOrNull)` -> a promise that resolves once every
/// write the response can reveal is durable. A null position identifies a
/// read-only response, which trails any barrier already open for the cell.
fn op_gate_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let value = args.get(1);
    let position = (!value.is_null_or_undefined())
        .then(|| value.to_integer(scope).map(|n| n.value() as u64))
        .flatten();
    let read_only = position.is_none();
    let (tx, receive) = tokio::sync::oneshot::channel();
    let sent = GATE_TX
        .get()
        .map(|gate| {
            gate.send(GateReq {
                scope: cell,
                position,
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    let id = asyncrt::enqueue(async move {
        if !sent {
            if read_only {
                return Ok(String::new());
            }
            return Err("no output-gate channel".into());
        }
        match receive.await {
            Ok(Ok(())) => Ok(String::new()),
            Ok(Err(error)) => Err(format!("{error:?}")),
            Err(_) => Err("output gate dropped".into()),
        }
    });
    rv.set(promise_for(scope, id));
}

fn op_do_call(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    op_do_call_impl(scope, args, &mut rv, false);
}

fn op_do_call_cancellable(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    op_do_call_impl(scope, args, &mut rv, true);
}

fn op_do_call_impl(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: &mut v8::ReturnValue<v8::Value>,
    cancellable: bool,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let name_value = args.get(1);
    let name = (!name_value.is_null_or_undefined()).then(|| name_value.to_rust_string_lossy(scope));
    let url = args.get(2).to_rust_string_lossy(scope);
    let method = args.get(3).to_rust_string_lossy(scope);
    let body = view_bytes(args.get(4)).unwrap_or_default();
    let headers =
        serde_json::from_str(&args.get(5).to_rust_string_lossy(scope)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    // Taken here and nowhere later: this is the last point that still runs
    // in the order the script made the calls.
    let order = Some(enter_call_order(current_context(), &cell));
    // Every host op needs an internal cancellation channel. A caller without
    // an AbortSignal cannot cancel from JavaScript, but its enclosing Worker
    // can still disappear and drop this op before routing completes.
    let request_id = next_do_request_id();
    let (cancel_sender, cancel) = tokio::sync::oneshot::channel();
    do_call_cancels()
        .lock()
        .unwrap()
        .insert(request_id, cancel_sender);
    let mut cancel_guard = DoCallCancelGuard::new(request_id);
    let gate = egress_gate_request();
    let request = DoCallReq {
        request_id: Some(request_id),
        cancel: Some(cancel),
        deliver_abort_to_handler: cancellable,
        scope: cell,
        name,
        url,
        method,
        body,
        headers,
        reply: tx,
        order,
        parent: current_trace_ids(scope),
    };
    let id = asyncrt::enqueue(async move {
        gated_channel_send(gate, &DO_CALL_TX, request, "no proxy channel").await?;
        let result = match rx.await {
            Ok(Ok(response)) => Ok(encode_http_response(response, true)),
            Ok(Err(e)) => Err(format!("{e}")),
            Err(e) => Err(format!("proxy dropped: {e}")),
        };
        cancel_guard.disarm();
        result
    });
    let p = promise_for(scope, id);
    if cancellable {
        if let Some(promise) = p.to_object(scope) {
            let key = v8::String::new(scope, "__celldCancelId").unwrap();
            let request_id = v8::String::new(scope, &request_id_string(request_id)).unwrap();
            promise.set(scope, key.into(), request_id.into());
        }
    }
    rv.set(p);
}

fn op_do_call_cancel(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let request_id = args.get(0).to_rust_string_lossy(scope);
    let Some(request_id) = parse_request_id(&request_id) else {
        return;
    };
    if let Some(cancel) = do_call_cancels().lock().unwrap().remove(&request_id) {
        let _ = cancel.send(());
    }
}

/// `__rpc_call(scope, name, method, argsSc)` -> Promise<Uint8Array>;
/// arguments and result are V8 structured-clone bytes.
fn op_rpc_call(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let name_value = args.get(1);
    let name = (!name_value.is_null_or_undefined()).then(|| name_value.to_rust_string_lossy(scope));
    let method = args.get(2).to_rust_string_lossy(scope);
    let args = RpcData::V8(view_bytes(args.get(3)).unwrap_or_default());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let gate = egress_gate_request();
    let request = RpcCallReq {
        scope: cell,
        name,
        method,
        args,
        reply: tx,
    };
    let id = asyncrt::enqueue(async move {
        gated_channel_send(gate, &RPC_CALL_TX, request, "no RPC channel").await?;
        match rx.await {
            Ok(Ok(RpcData::V8(bytes))) => Ok(bytes),
            Ok(Ok(RpcData::Json(_))) => Err("RPC answered JSON to a structured-clone call".into()),
            Ok(Err(e)) => Err(format!("{e}")),
            Err(e) => Err(format!("RPC proxy dropped: {e}")),
        }
    });
    let p = promise_for(scope, id);
    rv.set(p);
}

/// Outbound `fetch` — the op behind the harness's `fetch()`. Resolves to a JSON
/// `{status, body, headers}` string. Streaming is a later layer.
fn op_fetch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    // A loaded worker with globalOutbound: null has no ambient egress: it must
    // reach the world through `env` capabilities. Message matches workerd.
    if actor_runtime_state(scope).egress == EgressPolicy::Deny {
        return loader_throw(
            scope,
            "This worker is not permitted to access the internet via global \
             functions like fetch(). It must use capabilities (such as \
             bindings in 'env') to talk to the outside world.",
        );
    }
    let method = args.get(0).to_rust_string_lossy(scope);
    let url = args.get(1).to_rust_string_lossy(scope);
    let b = args.get(2);
    let body: Option<Vec<u8>> = if b.is_undefined() || b.is_null() {
        None
    } else {
        serde_json::from_str(&b.to_rust_string_lossy(scope)).ok()
    };
    let mut headers: Vec<(String, String)> =
        serde_json::from_str(&args.get(3).to_rust_string_lossy(scope)).unwrap_or_default();
    let redirect = args.get(4).to_rust_string_lossy(scope);
    let client = if redirect == "manual" {
        HTTP_MANUAL.with(|client| client.clone())
    } else {
        HTTP.with(|client| client.clone())
    };
    // The creating context, read here while JS is still running: the op
    // future resolves on whatever worker polls it, far from any CPED.
    let trace = current_trace_ids(scope);
    // The child's ids are minted before the request leaves so the
    // traceparent header carries them: whatever this fetch reaches can
    // join the trace celld is part of.
    let child = trace.as_ref().map(crate::telemetry::child_ids);
    if let Some(child) = child.as_ref() {
        if !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("traceparent"))
        {
            headers.push(("traceparent".into(), crate::telemetry::traceparent(child)));
        }
    }
    let span_url = trace.as_ref().map(|_| url.clone());
    // Whatever this request can reveal must be durable before it leaves: a
    // third party that has acted on a write celld then loses cannot be told to
    // un-act. That covers a write this handler made and a value it only read,
    // because the third party cannot tell the two apart.
    let gate = egress_gate_request();
    let id = asyncrt::enqueue(async move {
        let span_started = trace.as_ref().map(|_| crate::telemetry::now_unix_us());
        let mut span = trace.as_ref().zip(child).map(|(parent, child)| {
            let mut span =
                crate::telemetry::Span::new(child, "fetch", crate::telemetry::KIND_CLIENT);
            span.parent_span_id = Some(parent.span_id);
            span.parent_remote = Some(false);
            span.url = span_url;
            span
        });
        let mut finish = |ok: bool, status: Option<u16>, error: Option<String>| {
            if let Some(mut span) = span.take() {
                span.start_unix_us = span_started.unwrap_or_default();
                span.duration_us = crate::telemetry::now_unix_us() - span.start_unix_us;
                span.ok = ok;
                span.http_status = status;
                span.error = error;
                crate::telemetry::record(span);
            }
        };
        await_egress_gate(gate).await.inspect_err(|error| {
            finish(false, None, Some(error.clone()));
        })?;
        let m = reqwest::Method::from_bytes(method.as_bytes()).unwrap_or(reqwest::Method::GET);
        // a timeout bounds a black-hole host: the future settles (Err) instead
        // of parking run_loop forever — the hang the Drop guard can't catch.
        let fetch_timeout = crate::env_vars::positive_or("CELLD_FETCH_TIMEOUT_S", 120)
            .expect("validated CELLD_FETCH_TIMEOUT_S");
        let mut rb = client
            .request(m, &url)
            .timeout(std::time::Duration::from_secs(fetch_timeout));
        for (name, value) in headers {
            rb = rb.header(name, value);
        }
        if let Some(b) = body {
            rb = rb.body(b);
        }
        match rb.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                finish(true, Some(status), None);
                let headers = resp
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            value.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect::<Vec<_>>();
                let stream_id = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
                register_http_stream(stream_id, HttpStreamSource::Response(resp));
                Ok(serde_json::json!({
                    "status": status, "streamId": stream_id, "headers": headers,
                })
                .to_string())
            }
            Err(e) => {
                finish(false, None, Some(format!("fetch: {e}")));
                Err(format!("fetch: {e}"))
            }
        }
    });
    let p = promise_for(scope, id);
    rv.set(p);
}

fn op_asset_fetch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let script = args.get(0).to_rust_string_lossy(scope);
    let method = args.get(1).to_rust_string_lossy(scope);
    let url = args.get(2).to_rust_string_lossy(scope);
    let headers: Vec<(String, String)> =
        serde_json::from_str(&args.get(3).to_rust_string_lossy(scope)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = ASSET_CALL_TX.get().is_some_and(|sender| {
        sender
            .send(AssetCallReq {
                script,
                url,
                method,
                headers,
                reply: tx,
            })
            .is_ok()
    });
    let id = asyncrt::enqueue(async move {
        if !sent {
            return Err("no asset resolver channel".to_string());
        }
        match rx.await {
            Ok(Ok(response)) => Ok(encode_http_response(response, false)),
            Ok(Err(error)) => Err(format!("asset fetch: {error}")),
            Err(error) => Err(format!("asset resolver dropped: {error}")),
        }
    });
    rv.set(promise_for(scope, id));
}

async fn next_http_stream_chunk(source: &mut HttpStreamSource) -> Result<Option<Vec<u8>>, String> {
    match source {
        HttpStreamSource::Response(response) => response
            .chunk()
            .await
            .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
            .map_err(|error| format!("response stream: {error}")),
        HttpStreamSource::Receiver(receiver) => match receiver.recv().await {
            Some(Ok(bytes)) => Ok(Some(bytes)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        },
        HttpStreamSource::Stream(stream) => stream.next().await.transpose(),
    }
}

/// Move a host response source into a directly-polled stream. No pump task is
/// needed: the eventual HTTP or JS consumer supplies the backpressure.
fn http_chunk_stream(source: HttpStreamSource) -> HttpChunkStream {
    match source {
        HttpStreamSource::Response(response) => Box::pin(response.bytes_stream().map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(|error| format!("response stream: {error}"))
        })),
        HttpStreamSource::Receiver(receiver) => {
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(receiver))
        }
        HttpStreamSource::Stream(stream) => stream,
    }
}

/// Host-native tee for an outbound response. Both branches are represented by
/// stream IDs, so one can be returned through Axum while JS independently
/// scans the other for observability or usage accounting.
fn tee_http_stream(mut source: HttpStreamSource) -> (u64, u64) {
    let (tx1, rx1) = tokio::sync::mpsc::channel(16);
    let (tx2, rx2) = tokio::sync::mpsc::channel(16);
    let id1 = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    let id2 = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    register_http_stream(id1, HttpStreamSource::Receiver(rx1));
    register_http_stream(id2, HttpStreamSource::Receiver(rx2));
    asyncrt::op_handle().spawn(async move {
        loop {
            match next_http_stream_chunk(&mut source).await {
                Ok(Some(bytes)) => {
                    let first = tx1.send(Ok(bytes.clone())).await.is_ok();
                    let second = tx2.send(Ok(bytes)).await.is_ok();
                    if !first && !second {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = tx1.send(Err(error.clone())).await;
                    let _ = tx2.send(Err(error)).await;
                    break;
                }
            }
        }
    });
    (id1, id2)
}

fn op_http_stream_read(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let source = http_streams()
        .lock()
        .unwrap()
        .get_mut(&stream_id)
        .and_then(|stream| {
            stream
                .source
                .take()
                .map(|source| (source, stream.cancelled.subscribe()))
        });
    let id = asyncrt::enqueue(async move {
        let Some((mut source, mut cancelled)) = source else {
            return Ok(asyncrt::OpOut::Str(HTTP_STREAM_DONE.into()));
        };
        let next = tokio::select! {
            result = next_http_stream_chunk(&mut source) => Some(result),
            _ = cancelled.changed() => None,
        };
        match next {
            None => Ok(asyncrt::OpOut::Str(HTTP_STREAM_DONE.into())),
            Some(Ok(Some(bytes))) => {
                if let Some(stream) = http_streams().lock().unwrap().get_mut(&stream_id) {
                    stream.created = Instant::now();
                    stream.source = Some(source);
                }
                Ok(asyncrt::OpOut::Bytes(bytes))
            }
            Some(Ok(None)) => {
                http_streams().lock().unwrap().remove(&stream_id);
                Ok(asyncrt::OpOut::Str(HTTP_STREAM_DONE.into()))
            }
            Some(Err(error)) => {
                http_streams().lock().unwrap().remove(&stream_id);
                Err(error)
            }
        }
    });
    rv.set(promise_for(scope, id));
}

fn op_http_stream_tee(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let source = http_streams()
        .lock()
        .unwrap()
        .remove(&stream_id)
        .and_then(|stream| stream.source);
    let Some(source) = source else {
        let message = v8::String::new(scope, "response stream is no longer available").unwrap();
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
        return;
    };
    let ids = tee_http_stream(source);
    let json = serde_json::to_string(&ids).unwrap();
    rv.set(v8::String::new(scope, &json).unwrap().into());
}

fn op_http_stream_cancel(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    if let Some(stream) = http_streams().lock().unwrap().remove(&stream_id) {
        let _ = stream.cancelled.send(true);
    }
}

fn op_response_stream_create(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    let (writer, receiver) = tokio::sync::mpsc::channel(1);
    let (finished, _) = tokio::sync::watch::channel(false);
    let now = Instant::now();
    register_http_stream(stream_id, HttpStreamSource::Receiver(receiver));
    let mut writers = response_stream_writers().lock().unwrap();
    writers.retain(|_, stream| stream.created.elapsed() < Duration::from_secs(60));
    writers.insert(
        stream_id,
        ResponseStreamWriter {
            created: now,
            writer,
            finished,
        },
    );
    rv.set(v8::Number::new(scope, stream_id as f64).into());
}

fn op_response_stream_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let bytes = args
        .get(1)
        .try_cast::<v8::ArrayBufferView>()
        .ok()
        .map(|view| {
            let mut bytes = vec![0; view.byte_length()];
            view.copy_contents(&mut bytes);
            bytes
        });
    let writer = response_stream_writers()
        .lock()
        .unwrap()
        .get_mut(&stream_id)
        .map(|stream| {
            stream.created = Instant::now();
            stream.writer.clone()
        });
    let id = asyncrt::enqueue(async move {
        let Some(bytes) = bytes else {
            return Err("response stream chunks must be ArrayBuffer views".into());
        };
        let Some(writer) = writer else {
            return Err("response stream consumer canceled".into());
        };
        if writer.send(Ok(bytes)).await.is_err() {
            response_stream_writers().lock().unwrap().remove(&stream_id);
            return Err("response stream consumer canceled".into());
        }
        Ok(String::new())
    });
    rv.set(promise_for(scope, id));
}

fn op_response_stream_closed(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let (writer, mut finished) = response_stream_writers()
        .lock()
        .unwrap()
        .get(&stream_id)
        .map(|stream| (stream.writer.clone(), stream.finished.subscribe()))
        .unzip();
    let id = asyncrt::enqueue(async move {
        let cancelled = match (writer, finished.as_mut()) {
            (Some(writer), Some(finished)) => tokio::select! {
                biased;
                result = finished.changed() => result.is_err(),
                _ = writer.closed() => true,
            },
            _ => false,
        };
        Ok(String::from(if cancelled {
            "cancelled"
        } else {
            "finished"
        }))
    });
    rv.set(promise_for(scope, id));
}

fn op_response_stream_close(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let error = args.get(1).to_rust_string_lossy(scope);
    let stream = response_stream_writers().lock().unwrap().remove(&stream_id);
    let id = asyncrt::enqueue(async move {
        if let Some(stream) = stream {
            let _ = stream.finished.send(true);
            if !error.is_empty() {
                let _ = stream.writer.send(Err(error)).await;
            }
        }
        Ok(String::new())
    });
    rv.set(promise_for(scope, id));
}

pub fn reqwest_response_stream(response: reqwest::Response) -> HttpChunkStream {
    http_chunk_stream(HttpStreamSource::Response(response))
}

/// Hand a request body to the isolate as a stream rather than as bytes.
/// The returned id names the stream for `__http_stream_read`, so the
/// Worker pulls each chunk off the socket as it asks for it and the host
/// never holds the whole body.
pub fn register_body_stream(stream: HttpChunkStream) -> u64 {
    let stream_id = NEXT_HTTP_STREAM_ID.fetch_add(1, Ordering::Relaxed);
    register_http_stream(stream_id, HttpStreamSource::Stream(stream));
    stream_id
}

/// How an incoming request body reaches the isolate.
///
/// A small body crosses as bytes. This costs one copy and no asynchronous
/// operations, so a common request pays nothing for a stream that it does
/// not need. A large body, or a body of unknown length, crosses as a
/// stream id. The peak cost of that body is one chunk, not its length.
pub enum RequestBody {
    Bytes(Vec<u8>),
    Stream(u64),
}

impl RequestBody {
    /// The bytes already in hand, for the paths that hold a whole body.
    pub fn bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Stream(_) => &[],
        }
    }
}

impl From<Vec<u8>> for RequestBody {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Bytes(bytes)
    }
}

/// Timer op behind `setTimeout`: a promise resolving after `ms`.
fn op_timer(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let timer_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let ms = args.get(1).number_value(scope).unwrap_or(0.0).max(0.0) as u64;
    let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel();
    timer_cancels().lock().unwrap().insert(timer_id, cancel_tx);
    let id = asyncrt::enqueue(async move {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(ms)) => {}
            _ = &mut cancel_rx => {}
        }
        timer_cancels().lock().unwrap().remove(&timer_id);
        Ok(String::new())
    });
    let p = promise_for(scope, id);
    rv.set(p);
}

/// `__gate_acquire(scope)` — take the cell's input gate for a
/// `blockConcurrencyWhile`, waiting if another block holds it.
///
/// Answers a promise of the event id, not the id itself. It was synchronous,
/// and that was right while a cell's events came off one channel: only one
/// ran at a time, a delivery point had already found the gate open, and
/// yielding even one microtask reopened a window in which a nested delivery
/// could wait on a gate nothing would release. Neither half holds now —
/// events are independent tasks, several run at once, and nothing nests —
/// so two blocks can meet and the second must queue.
fn op_gate_acquire(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let event = NEXT_GATE_EVENT.fetch_add(1, Ordering::Relaxed);
    // Taken under the gates lock so the test and the take are one step: a
    // release landing between them would leave this block queued behind a
    // gate that is already open.
    let taken = cell_gates()
        .lock()
        .unwrap()
        .entry(cell.clone())
        .or_default()
        .acquire(event);
    if taken {
        let id = asyncrt::enqueue(async move { Ok(event.to_string()) });
        rv.set(promise_for(scope, id));
        return;
    }
    let id = asyncrt::enqueue(async move {
        loop {
            // Ask for a ticket first: `cell_gate_wait` tests and enqueues
            // under the gates lock, so a release landing between the two
            // cannot leave this waiter behind an open gate.
            let waiting = cell_gate_wait(&cell);
            let taken = cell_gates()
                .lock()
                .unwrap()
                .entry(cell.clone())
                .or_default()
                .acquire(event);
            if taken {
                return Ok(event.to_string());
            }
            match waiting {
                Some(open) => match open.await {
                    Ok(Ok(())) => {}
                    // The block ahead of this one failed and reset the cell,
                    // so this block has nothing left to guard.
                    Ok(Err(failure)) => return Err(failure),
                    Err(_) => {
                        return Err("cell stopped while waiting for its input gate".to_string())
                    }
                },
                // The gate was open when we asked and shut before we took
                // it: another block won the race. Yield rather than spin —
                // this runs on a tokio worker, and a bare `continue` here
                // starved the runtime.
                None => tokio::task::yield_now().await,
            }
        }
    });
    rv.set(promise_for(scope, id));
}

/// `__gate_wait(scope)` — settle when the cell's input gate is open.
///
/// The gate's other side. `op_gate_acquire` is for something that wants to
/// *hold* the gate; this is for something that only has to arrive after it,
/// which is every event the gate exists to hold back. A drive does this in
/// Rust before it begins a turn; an RPC stub op does it here, because it is
/// dispatched inside the isolate and never becomes a drive.
///
/// Rejecting matters as much as resolving: a critical section that failed
/// reset the cell, so what waited behind it is refused with the reason
/// rather than run against state that no longer exists.
fn op_gate_wait(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let id = asyncrt::enqueue(async move {
        loop {
            match cell_gate_wait(&cell) {
                None => return Ok(String::new()),
                Some(open) => match open.await {
                    Ok(Ok(())) => {}
                    Ok(Err(failure)) => return Err(failure),
                    Err(_) => {
                        return Err("cell stopped while waiting for its input gate".to_string())
                    }
                },
            }
        }
    });
    rv.set(promise_for(scope, id));
}

/// `__gate_release(scope, event)` — the block is over. The next delivery
/// point to run takes whatever queued behind it.
fn op_gate_release(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let event = args.get(1).number_value(scope).unwrap_or_default() as celld_logic::gate::EventId;
    // A third argument means the block failed and the actor was reset, so
    // what queued behind it is refused with that reason rather than
    // delivered to a cell whose state is gone.
    let failure = args.get(2);
    let outcome = if failure.is_null_or_undefined() {
        Ok(())
    } else {
        Err(failure.to_rust_string_lossy(scope))
    };
    if let Some(gate) = cell_gates().lock().unwrap().get_mut(&cell) {
        gate.release(event);
    }
    wake_gate_waiters(&cell, outcome);
}

fn op_timer_alloc(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = v8::Number::new(scope, NEXT_TIMER_ID.fetch_add(1, Ordering::Relaxed) as f64);
    rv.set(id.into());
}

fn op_timer_cancel(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let timer_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    if let Some(cancel) = timer_cancels().lock().unwrap().remove(&timer_id) {
        let _ = cancel.send(());
    }
}

fn durable_object_id_key(namespace_key: &str) -> [u8; 32] {
    DO_ID_KEYS.with(|keys| {
        if let Some(key) = keys.borrow().get(namespace_key) {
            return *key;
        }
        use sha2::Digest;
        let key: [u8; 32] = sha2::Sha256::digest(namespace_key.as_bytes()).into();
        keys.borrow_mut().insert(namespace_key.to_string(), key);
        key
    })
}

fn durable_object_id_hmac(key: &[u8; 32], input: &[u8]) -> hmac::Hmac<sha2::Sha256> {
    use hmac::Mac;
    let mut mac = <hmac::Hmac<sha2::Sha256> as hmac::Mac>::new_from_slice(key)
        .expect("SHA-256 accepts any HMAC key");
    mac.update(input);
    mac
}

/// Ids already derived on this thread, keyed by namespace and name.
///
/// `getByName` costs two HMAC-SHA256 rounds, and a profile of `/c/hello`
/// found them among the largest terms the stateless path does not have —
/// the same name resolving to the same id, recomputed on every request.
///
/// Per thread and unsynchronised, which is sound because the derivation is
/// a pure function of its inputs: a worker that has not seen a name pays
/// for it once, and no answer can differ between threads. Bounded because
/// names come from the application, and cleared wholesale at the cap rather
/// than evicted one at a time — a cache this cheap to refill does not earn
/// an LRU.
const DO_ID_CACHE_MAX: usize = 4096;

fn durable_object_id_for_name(namespace_key: &str, name: &str) -> [u8; 32] {
    use hmac::Mac;

    thread_local! {
        static IDS: RefCell<HashMap<(String, String), [u8; 32]>> =
            RefCell::new(HashMap::new());
    }
    let cached = IDS.with(|ids| {
        ids.borrow()
            .get(&(namespace_key.to_string(), name.to_string()))
            .copied()
    });
    if let Some(id) = cached {
        return id;
    }

    let key = durable_object_id_key(namespace_key);
    let mut id = [0_u8; 32];
    let digest = durable_object_id_hmac(&key, name.as_bytes())
        .finalize()
        .into_bytes();
    id[..16].copy_from_slice(&digest[..16]);
    let digest = durable_object_id_hmac(&key, &id[..16])
        .finalize()
        .into_bytes();
    id[16..].copy_from_slice(&digest[..16]);

    IDS.with(|ids| {
        let mut ids = ids.borrow_mut();
        if ids.len() >= DO_ID_CACHE_MAX {
            ids.clear();
        }
        ids.insert((namespace_key.to_string(), name.to_string()), id);
    });
    id
}

/// The key a Durable Object namespace derives its IDs from. D1 uses one
/// fleet-wide namespace because the database is a resource that several
/// Workers can bind, and a Worker rename must not rename that database.
const D1_NAMESPACE_KEY: &str = "cells:v1:d1:__D1Database";

pub(crate) fn namespace_key(script_name: &str, class_name: &str) -> String {
    if class_name == crate::deploy::D1_CLASS {
        D1_NAMESPACE_KEY.to_string()
    } else {
        format!("cells:v1:{}:{script_name}:{class_name}", script_name.len())
    }
}

/// The cell scope a D1 database lives at, for a caller outside any isolate.
/// `celld d1` addresses a database over the operator route, which takes a
/// scope, so this derives what `getByName` derives in the harness, from the
/// same key and the same HMAC.
pub fn d1_cell_scope(database_identity: &str) -> String {
    let id = durable_object_id_for_name(D1_NAMESPACE_KEY, database_identity);
    format!("{}:{}", crate::deploy::D1_CLASS, durable_object_id_hex(&id))
}

fn durable_object_id_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_durable_object_id(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let nibble = |byte| match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        };
        output[index] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(output)
}

fn throw_durable_object_id_error(scope: &mut v8::PinScope, message: &str) {
    let message = v8::String::new(scope, message).unwrap();
    let exception = v8::Exception::type_error(scope, message);
    scope.throw_exception(exception);
}

fn register_actor_name(
    scope: &mut v8::PinScope,
    actor_scope: &str,
    name: Option<&str>,
) -> Result<()> {
    let Some(name) = name else {
        return Ok(());
    };
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell_key = v8::String::new(scope, "__cell").unwrap();
    let cell = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let names_key = v8::String::new(scope, "idNames").unwrap();
    let names = cell
        .get(scope, names_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing Durable Object name registry"))?;
    let actor_scope_key = v8::String::new(scope, actor_scope).unwrap();
    if let Some(existing) = names.get(scope, actor_scope_key.into()) {
        if !existing.is_undefined() {
            if existing.to_rust_string_lossy(scope) == name {
                return Ok(());
            }
            anyhow::bail!("actor name conflicts with active identity for {actor_scope}");
        }
    }

    let (class_name, id) = actor_scope
        .split_once(':')
        .ok_or_else(|| anyhow!("named Durable Object scope has no class separator"))?;
    let namespace_keys_key = v8::String::new(scope, "namespaceKeys").unwrap();
    let namespace_keys = cell
        .get(scope, namespace_keys_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing Durable Object namespace registry"))?;
    let class_name_key = v8::String::new(scope, class_name).unwrap();
    let namespace_key = namespace_keys
        .get(scope, class_name_key.into())
        .filter(|value| value.is_string())
        .ok_or_else(|| anyhow!("missing namespace key for Durable Object class {class_name}"))?
        .to_rust_string_lossy(scope);
    let expected = durable_object_id_hex(&durable_object_id_for_name(&namespace_key, name));
    if id != expected {
        anyhow::bail!("actor name does not match Durable Object ID for {actor_scope}");
    }
    storage::set_actor_name(actor_scope, name)?;

    let name_value = v8::String::new(scope, name).unwrap();
    if !names
        .set(scope, actor_scope_key.into(), name_value.into())
        .unwrap_or(false)
    {
        anyhow::bail!("could not register Durable Object name");
    }
    Ok(())
}

fn op_do_id(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use hmac::Mac;

    let namespace_key = args.get(0).to_rust_string_lossy(scope);
    let operation = args.get(1).to_rust_string_lossy(scope);
    let input = args.get(2).to_rust_string_lossy(scope);
    let key = durable_object_id_key(&namespace_key);
    let mut id = [0_u8; 32];

    match operation.as_str() {
        "name" => {
            id = durable_object_id_for_name(&namespace_key, &input);
            rv.set(
                v8::String::new(scope, &durable_object_id_hex(&id))
                    .unwrap()
                    .into(),
            );
            return;
        }
        "unique" => {
            if getrandom::fill(&mut id[..16]).is_err() {
                throw_durable_object_id_error(scope, "secure random generation failed");
                return;
            }
        }
        "validate" => {
            let Some(decoded) = decode_durable_object_id(&input) else {
                throw_durable_object_id_error(
                    scope,
                    "Invalid Durable Object ID: must be 64 hex digits",
                );
                return;
            };
            if durable_object_id_hmac(&key, &decoded[..16])
                .verify_truncated_left(&decoded[16..])
                .is_err()
            {
                throw_durable_object_id_error(
                    scope,
                    "Durable Object ID is not valid for this namespace",
                );
                return;
            }
            rv.set(
                v8::String::new(scope, &input.to_ascii_lowercase())
                    .unwrap()
                    .into(),
            );
            return;
        }
        _ => {
            throw_durable_object_id_error(scope, "unknown Durable Object ID operation");
            return;
        }
    }

    let digest = durable_object_id_hmac(&key, &id[..16])
        .finalize()
        .into_bytes();
    id[16..].copy_from_slice(&digest[..16]);
    rv.set(
        v8::String::new(scope, &durable_object_id_hex(&id))
            .unwrap()
            .into(),
    );
}

fn view_bytes(value: v8::Local<v8::Value>) -> Option<Vec<u8>> {
    let view = v8::Local::<v8::ArrayBufferView>::try_from(value).ok()?;
    let mut bytes = vec![0_u8; view.byte_length()];
    view.copy_contents(&mut bytes);
    Some(bytes)
}

fn webcrypto_return_bytes(
    scope: &mut v8::PinScope,
    mut rv: v8::ReturnValue<v8::Value>,
    bytes: &[u8],
) {
    let buffer = v8::ArrayBuffer::new(scope, bytes.len());
    if !bytes.is_empty() {
        let store = buffer.get_backing_store();
        let destination = unsafe {
            std::slice::from_raw_parts_mut(store.data().unwrap().as_ptr() as *mut u8, bytes.len())
        };
        destination.copy_from_slice(bytes);
    }
    let view = v8::Uint8Array::new(scope, buffer, 0, bytes.len()).unwrap();
    rv.set(view.into());
}

mod crypto;
mod zlib;

fn op_actor_abort(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let reason = args.get(1).to_rust_string_lossy(scope);
    let state = actor_runtime_state(scope);
    state
        .pending_puts
        .lock()
        .expect("pending puts lock poisoned")
        .remove(&cell);
    *state.termination.lock().expect("termination lock poisoned") = Some(ExecutionTermination {
        error: format!("__CELLD_ACTOR_ABORT__:{reason}"),
        actor_scope: Some(cell),
    });
    scope.terminate_execution();
}

fn op_process_exit(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let code = args.get(0).integer_value(scope).unwrap_or(0);
    if current_context().depth() == 0 {
        tracing::warn!(
            code,
            "process.exit called without an active request; ignoring"
        );
        return;
    }
    let actor_scope = args.get(1).to_rust_string_lossy(scope);
    let actor_scope = (!actor_scope.is_empty()).then_some(actor_scope);
    let state = actor_runtime_state(scope);
    if let Some(actor_scope) = actor_scope.as_deref() {
        state
            .pending_puts
            .lock()
            .expect("pending puts lock poisoned")
            .remove(actor_scope);
    }
    *state.termination.lock().expect("termination lock poisoned") = Some(ExecutionTermination {
        error: format!("__CELLD_PROCESS_EXIT__:The Node.js process.exit({code}) API was called."),
        actor_scope,
    });
    scope.terminate_execution();
}

fn op_log(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let msg: Vec<String> = (0..args.length())
        .map(|i| args.get(i).to_rust_string_lossy(scope))
        .collect();
    let body = msg.join(" ");
    // Correlated by CPED, so a continuation logging after an await — or
    // another entry's continuation running in this turn's checkpoint —
    // lands on the trace that owns it, not on whoever holds the isolate.
    if crate::telemetry::active() {
        let ids = current_trace_ids(scope);
        crate::telemetry::record_log(crate::telemetry::Log {
            trace_id: ids.as_ref().map(|ids| ids.trace_id),
            span_id: ids.as_ref().map(|ids| ids.span_id),
            time_unix_us: crate::telemetry::now_unix_us(),
            body: body.clone(),
        });
    }
    tracing::info!(target: "cell_console", "{}", body);
}
fn op_heap_limit_excessively_exceeded(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let exceeded = scope
        .get_slot::<Arc<HeapLimitState>>()
        .is_some_and(|state| state.excessively_exceeded.load(Ordering::Relaxed));
    rv.set(v8::Boolean::new(scope, exceeded).into());
}
/// Whether this isolate is too close to its heap limit to retain more state.
fn op_heap_over_admission_share(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let state = scope.get_slot::<Arc<HeapLimitState>>().cloned();
    let over = match state {
        // A condemned isolate admits nothing, whatever the heap now reads.
        Some(state)
            if state.excessively_exceeded.load(Ordering::Relaxed)
                || admission_refusal_forced(&state) =>
        {
            true
        }
        Some(state) => heap_share(scope, state.limit) >= HEAP_ADMISSION_SHARE,
        None => false,
    };
    rv.set(v8::Boolean::new(scope, over).into());
}
#[cfg(all(test, celld_internal_tests))]
fn op_test_set_heap_limit_excessively_exceeded(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    if let Some(state) = scope.get_slot::<Arc<HeapLimitState>>() {
        state
            .excessively_exceeded
            .store(args.get(0).boolean_value(scope), Ordering::Relaxed);
    }
}
#[cfg(all(test, celld_internal_tests))]
fn op_test_force_heap_admission_refusal(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    if let Some(state) = scope.get_slot::<Arc<HeapLimitState>>() {
        state
            .forced_admission_refusal
            .store(args.get(0).boolean_value(scope), Ordering::Relaxed);
    }
}
#[cfg(all(test, celld_internal_tests))]
fn op_test_gc(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    scope.request_garbage_collection_for_testing(v8::GarbageCollectionType::Full);
}
mod storage_ops;
use storage_ops::{actor_runtime_state, throw_storage_error};

/// $$urlParse(input, base?) -> {protocol,username,password,host,port,pathname,search,hash,href}
/// Backed by the WHATWG-conformant `url` crate.
fn op_url_parse(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let input = args.get(0).to_rust_string_lossy(scope);
    let base = args.get(1);
    let parsed = if base.is_undefined() {
        url::Url::parse(&input)
    } else {
        url::Url::options()
            .base_url(
                url::Url::parse(&base.to_rust_string_lossy(scope))
                    .ok()
                    .as_ref(),
            )
            .parse(&input)
    };
    let u = match parsed {
        Ok(u) => u,
        Err(e) => {
            let msg = v8::String::new(scope, &format!("Invalid URL: {e}")).unwrap();
            let exc = v8::Exception::type_error(scope, msg);
            scope.throw_exception(exc);
            return;
        }
    };
    let o = v8::Object::new(scope);
    let set = |k: &str, v: &str| {
        let key = v8::String::new(scope, k).unwrap();
        let val = v8::String::new(scope, v).unwrap();
        o.set(scope, key.into(), val.into());
    };
    set("protocol", u.scheme());
    set("username", u.username());
    set("password", u.password().unwrap_or(""));
    set("host", u.host_str().unwrap_or(""));
    set("port", &u.port().map(|p| p.to_string()).unwrap_or_default());
    set("pathname", u.path());
    set("search", u.query().unwrap_or(""));
    set("hash", u.fragment().unwrap_or(""));
    set("href", u.as_str());
    rv.set(o.into());
}

// URLPattern host seam, split like Deno's: pattern parsing and match-input
// canonicalization run in Rust via the `urlpattern` crate; per-match regex
// execution stays in JS `RegExp` (src/js/url_pattern.js). JSON is the
// boundary — both ops run at construct/match-canonicalize time only.

/// `$$urlPatternParse(inputJson, baseURL?, ignoreCase)` -> json. Input is a
/// pattern string or an init object of string components. Returns
/// per-component `{ patternString, regexpString, groupNameList }` plus
/// `hasRegexpGroups`. Throws TypeError on an invalid pattern.
fn op_urlpattern_parse(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use urlpattern::quirks;
    let input = args.get(0).to_rust_string_lossy(scope);
    let input: quirks::StringOrInit = serde_json::from_str(&input).unwrap();
    let base = args.get(1);
    let base = (!base.is_undefined()).then(|| base.to_rust_string_lossy(scope));
    let options = urlpattern::UrlPatternOptions {
        ignore_case: args.get(2).boolean_value(scope),
    };
    let pattern = quirks::process_construct_pattern_input(input, base.as_deref())
        .and_then(|init| quirks::parse_pattern(init, options));
    let p = match pattern {
        Ok(p) => p,
        Err(e) => {
            let msg = v8::String::new(scope, &e.to_string()).unwrap();
            let exc = v8::Exception::type_error(scope, msg);
            scope.throw_exception(exc);
            return;
        }
    };
    let component = |c: &quirks::UrlPatternComponent| {
        serde_json::json!({
            "patternString": c.pattern_string,
            "regexpString": c.regexp_string,
            "groupNameList": c.group_name_list,
        })
    };
    let out = serde_json::json!({
        "protocol": component(&p.protocol),
        "username": component(&p.username),
        "password": component(&p.password),
        "hostname": component(&p.hostname),
        "port": component(&p.port),
        "pathname": component(&p.pathname),
        "search": component(&p.search),
        "hash": component(&p.hash),
        "hasRegexpGroups": p.has_regexp_groups,
    });
    rv.set(v8::String::new(scope, &out.to_string()).unwrap().into());
}

/// `$$urlPatternMatchInput(inputJson, baseURL?)` -> json `[8 strings]` in
/// component order (protocol..hash), or `null` when the input does not parse
/// as a URL. Throws TypeError for an init combined with a baseURL argument.
fn op_urlpattern_match_input(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use urlpattern::quirks;
    let input = args.get(0).to_rust_string_lossy(scope);
    let input: quirks::StringOrInit = serde_json::from_str(&input).unwrap();
    let base = args.get(1);
    let base = (!base.is_undefined()).then(|| base.to_rust_string_lossy(scope));
    let null = v8::null(scope).into();
    let m = match quirks::process_match_input(input, base.as_deref()) {
        Ok(Some((input_, _inputs))) => match quirks::parse_match_input(input_) {
            Some(m) => m,
            None => return rv.set(null),
        },
        Ok(None) => return rv.set(null),
        Err(e) => {
            let msg = v8::String::new(scope, &e.to_string()).unwrap();
            let exc = v8::Exception::type_error(scope, msg);
            scope.throw_exception(exc);
            return;
        }
    };
    let out = serde_json::json!([
        m.protocol, m.username, m.password, m.hostname, m.port, m.pathname, m.search, m.hash,
    ]);
    rv.set(v8::String::new(scope, &out.to_string()).unwrap().into());
}

fn op_atob(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use base64::Engine;
    let input = args.get(0).to_rust_string_lossy(scope);
    let cleaned: String = input.chars().filter(|c| !c.is_whitespace()).collect();
    match base64::engine::general_purpose::STANDARD.decode(cleaned) {
        Ok(bytes) => {
            // atob returns a binary string (latin1)
            let s: String = bytes.iter().map(|&b| b as char).collect();
            rv.set(v8::String::new(scope, &s).unwrap().into());
        }
        Err(_) => {
            let msg = v8::String::new(scope, "Invalid base64").unwrap();
            let exc = v8::Exception::error(scope, msg);
            scope.throw_exception(exc);
        }
    }
}

// node:util introspection seam: V8-level queries that JS cannot perform,
// mirroring Workerd's C++ `node-internal:util` builtin. Only the lazy
// node:async_hooks prelude calls these. The value is V8's
// continuation-preserved embedder data — the primitive Workerd's
// AsyncContextFrame rides — which V8 itself captures and restores
// around every promise reaction. No promise hooks are installed, and an
// isolate that never touches AsyncLocalStorage never sets the value, so
// a bundle that does not import async_hooks pays nothing per promise.

/// The per-request context: everything a request owns while it is in flight.
///
/// Modelled on workerd's `IoContext`. The host owns it, not JS: a request's
/// event frames and the `waitUntil` promises inside them live here, and JS
/// reaches them only through ops that ask for the *current* context. That
/// inversion is the point. While an isolate serves one request at a time,
/// JS-side per-request state is indistinguishable from isolate-side state,
/// and the two silently diverge the moment requests interleave — one
/// request's `__endEvent` popping another's event, its `waitUntil` work
/// attributed to a request that never asked for it.
///
/// `current()` is a thread-local the host installs for the duration of a
/// turn and restores on the way out, exactly as `threadLocalRequest` is set
/// in `IoContext::runInContextScope` and restored by `SuppressIoContextScope`.
/// An op that needs a context and finds none is a bug in the host, not in
/// the script, so it says so rather than inventing one.
///
/// **It is `Send + Sync`, and that is load-bearing.** Under D1 a request is
/// a tokio task that suspends between turns and can resume on any worker, so
/// the context it carries has to cross threads with it. `Rc` and `RefCell`
/// would forbid that, and the alternative — parking every live context in a
/// map inside the isolate, keyed by request id — rebuilds the very
/// demultiplexing table (`idmap`) that deleting the pump exists to remove.
/// So the interiors are `Mutex` instead. The locks are uncontended by
/// construction, because only the isolate's holder can reach a context, and
/// they are taken about twice per request.
pub struct IoContext {
    /// This caller's delivery order for each cell it has called. See
    /// `CallOrder` — it is here rather than in a process-wide map because
    /// nothing outside this caller ever reads it.
    call_chains: Mutex<CallChains>,
    /// Event frames, innermost last. A frame collects the `waitUntil`
    /// promises registered while it is the innermost one.
    ///
    /// Nesting is per-request and strictly LIFO — a service-binding
    /// dispatch, an RPC entry, or a DO construction pushes its own frame so
    /// its `waitUntil` binds to that dispatch, matching workerd.
    events: Mutex<Vec<Vec<v8::Global<v8::Promise>>>>,
    /// Isolate-polled sockets opened by each nested run loop of this
    /// request, innermost last. `asyncrt` aborts a region's pending ops when
    /// it exits; these are host-side resources abort cannot reach, so the
    /// region closes them itself.
    sockets: Mutex<Vec<Vec<u64>>>,
    /// Sockets opened before any run loop exists. A handler runs
    /// synchronously to its first await before its loop is entered, so a
    /// socket built at the top of a handler has no frame yet; the loop
    /// adopts these when it starts.
    pending_sockets: Mutex<Vec<u64>>,
    /// Output-gate capture. While a `webSocketMessage` runs, the frames it
    /// sends are collected here instead of reaching the wire, so the shell
    /// can hold them until the message's write is durable. A stack, so a
    /// nested dispatch keeps its frames apart from the outer one's.
    ///
    /// On the event and not on the thread, because an event outlives the
    /// turn that began it: capture starts in one turn and is taken in a
    /// later one, which tokio may run on a different worker.
    ws_capture: Mutex<Vec<Vec<(u64, WsOut)>>>,
    /// Which cell an event belongs to, and the committed-write position it
    /// started at, innermost last. An outbound effect raised during the
    /// event consults this: if the handler has advanced the position, the
    /// effect waits for the output gate before it leaves the process.
    ///
    /// Empty for stateless Worker code, which owns no cell and gates
    /// nothing.
    egress: Mutex<Vec<(String, u64)>>,
}

impl IoContext {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            call_chains: Mutex::new(CallChains::default()),
            events: Mutex::new(Vec::new()),
            sockets: Mutex::new(Vec::new()),
            pending_sockets: Mutex::new(Vec::new()),
            ws_capture: Mutex::new(Vec::new()),
            egress: Mutex::new(Vec::new()),
        })
    }

    fn begin_event(&self) {
        self.events.lock().unwrap().push(Vec::new());
    }

    /// Pop the innermost frame and hand back what it collected. `None` when
    /// there is no frame, which is a script calling `waitUntil` outside any
    /// event; the harness turns that into workerd's global-scope error.
    fn end_event(&self) -> Option<Vec<v8::Global<v8::Promise>>> {
        self.events.lock().unwrap().pop()
    }

    fn register_wait_until(&self, promise: v8::Global<v8::Promise>) {
        if let Some(frame) = self.events.lock().unwrap().last_mut() {
            frame.push(promise);
        }
    }

    fn depth(&self) -> usize {
        self.events.lock().unwrap().len()
    }
}

/// What `OwnedIsolate::into_shared` demands of us: every slot value on a
/// shared isolate is reachable, and droppable, from whichever thread holds
/// the lock. The type system cannot check it at the call site, so it is
/// checked here instead.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<HeapLimitState>();
    assert_send_sync::<ActorRuntimeState>();
    assert_send_sync::<ModuleRegistry>();
};

/// A request must be able to carry its own context across a suspension. A
/// compile-time check, because losing it would surface as an unrelated
/// `Send` error wherever the request task is spawned.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Arc<IoContext>>();
};

thread_local! {
    /// The request whose turn is running on this thread, or none between
    /// turns. Never set by JS.
    static CURRENT: RefCell<Option<Arc<IoContext>>> =
        const { RefCell::new(None) };
}

/// Install `context` for the duration of a turn, restoring whatever was
/// current when the guard drops.
///
/// Restoring rather than clearing is what makes nested host dispatch safe: a
/// service binding runs a second handler inside the first one's turn, and the
/// outer request must still be current when it returns.
pub struct CurrentGuard(Option<Arc<IoContext>>);

impl CurrentGuard {
    pub fn enter(context: Arc<IoContext>) -> Self {
        CurrentGuard(CURRENT.with(|current| current.borrow_mut().replace(context)))
    }
}

impl Drop for CurrentGuard {
    fn drop(&mut self) {
        CURRENT.with(|current| *current.borrow_mut() = self.0.take());
    }
}

/// The context this turn belongs to.
///
/// A thread that has not had one installed gets a default: every path that
/// runs JS without an explicit request context — a cell isolate, which the
/// DO contract already serializes, an RPC entry, a DO constructor — keeps
/// exactly the semantics it had when the stack lived in JS, one per isolate.
/// Isolation is a property of *installing* a context, so it arrives with the
/// paths that need it rather than being retrofitted to every caller at once.
fn current_context() -> Arc<IoContext> {
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        current.get_or_insert_with(IoContext::new).clone()
    })
}

fn op_event_begin(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let _ = scope;
    current_context().begin_event();
}

/// Pop the innermost event and return the aggregate of its `waitUntil`
/// promises, or null when it collected none. The aggregate is built here
/// rather than in JS because the promises live here: the host holds them for
/// the request, and the request is what the frame belongs to.
fn op_event_end<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Some(frame) = current_context().end_event() else {
        rv.set_null();
        return;
    };
    if frame.is_empty() {
        rv.set_null();
        return;
    }
    let promises: Vec<v8::Local<v8::Value>> = frame
        .iter()
        .map(|promise| v8::Local::new(scope, promise).into())
        .collect();
    let array = v8::Array::new_with_elements(scope, &promises);
    match all_settled(scope, array.into()) {
        Some(aggregate) => rv.set(aggregate),
        None => rv.set_null(),
    }
}

fn op_wait_until<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    args: v8::FunctionCallbackArguments<'s>,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let value = args.get(0);
    let promise = match value.try_cast::<v8::Promise>() {
        Ok(promise) => promise,
        Err(_) => match resolved_promise(scope, value) {
            Ok(promise) => promise,
            Err(_) => return,
        },
    };
    current_context().register_wait_until(v8::Global::new(scope, promise));
}

/// How many events are open on the current request. The harness uses it for
/// workerd's global-scope error, which fires when `waitUntil` is imported and
/// called with no event in progress.
fn op_event_depth(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let depth = current_context().depth();
    rv.set(v8::Integer::new(scope, depth as i32).into());
}

/// `Promise.allSettled(values)`, from the context's own realm.
fn all_settled<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    values: v8::Local<'s, v8::Value>,
) -> Option<v8::Local<'s, v8::Value>> {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, "Promise").unwrap();
    let promise_ctor: v8::Local<v8::Object> = global.get(scope, key.into())?.try_into().ok()?;
    let key = v8::String::new(scope, "allSettled").unwrap();
    let all_settled: v8::Local<v8::Function> =
        promise_ctor.get(scope, key.into())?.try_into().ok()?;
    all_settled.call(scope, promise_ctor.into(), &[values])
}

fn op_als_get(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    rv.set(cped_frame(scope));
}

fn op_als_set(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let trace = cped_trace(scope);
    set_cped(scope, args.get(0), trace);
}

// node:util prelude calls these; registration is the sole per-isolate cost.

/// Type-check bitmask. Bit order must match `T` in src/js/node_util.js.
fn op_util_type_flags(
    _scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let v = args.get(0);
    let checks: [bool; 27] = [
        v.is_external(),
        v.is_date(),
        v.is_arguments_object(),
        v.is_big_int_object(),
        v.is_boolean_object(),
        v.is_number_object(),
        v.is_string_object(),
        v.is_symbol_object(),
        v.is_native_error(),
        v.is_reg_exp(),
        v.is_async_function(),
        v.is_generator_function(),
        v.is_generator_object(),
        v.is_promise(),
        v.is_map(),
        v.is_set(),
        v.is_map_iterator(),
        v.is_set_iterator(),
        v.is_weak_map(),
        v.is_weak_set(),
        v.is_array_buffer(),
        v.is_data_view(),
        v.is_shared_array_buffer(),
        v.is_proxy(),
        v.is_module_namespace_object(),
        v.is_typed_array(),
        v.is_array_buffer_view(),
    ];
    let mut flags: u32 = 0;
    for (i, hit) in checks.iter().enumerate() {
        if *hit {
            flags |= 1 << i;
        }
    }
    rv.set_uint32(flags);
}

fn op_util_constructor_name(
    _scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(obj) = v8::Local::<v8::Object>::try_from(args.get(0)) else {
        return; // undefined
    };
    rv.set(obj.get_constructor_name().into());
}

/// `[target, handler]` for a proxy, undefined otherwise.
fn op_util_proxy_details(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(proxy) = v8::Local::<v8::Proxy>::try_from(args.get(0)) else {
        return;
    };
    let target = proxy.get_target(scope);
    let handler = proxy.get_handler(scope);
    rv.set(v8::Array::new_with_elements(scope, &[target, handler]).into());
}

/// `[state]` for a pending promise, `[state, result]` otherwise,
/// undefined for a non-promise. States: 0 pending, 1 fulfilled, 2 rejected.
fn op_util_promise_details(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(promise) = v8::Local::<v8::Promise>::try_from(args.get(0)) else {
        return;
    };
    let state = match promise.state() {
        v8::PromiseState::Pending => 0,
        v8::PromiseState::Fulfilled => 1,
        v8::PromiseState::Rejected => 2,
    };
    let pending = state == 0;
    let state: v8::Local<v8::Value> = v8::Integer::new(scope, state).into();
    let elements = if pending {
        vec![state]
    } else {
        vec![state, promise.result(scope)]
    };
    rv.set(v8::Array::new_with_elements(scope, &elements).into());
}

/// `[entries, isKeyValue]` for collections and their iterators (V8's
/// PreviewEntries), undefined when V8 has no preview.
fn op_util_preview_entries(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(obj) = v8::Local::<v8::Object>::try_from(args.get(0)) else {
        return;
    };
    let (entries, is_key_value) = obj.preview_entries(scope);
    let Some(entries) = entries else { return };
    let flag: v8::Local<v8::Value> = v8::Boolean::new(scope, is_key_value).into();
    rv.set(v8::Array::new_with_elements(scope, &[entries.into(), flag]).into());
}

fn op_alarm_set(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let s = args.get(0).to_rust_string_lossy(scope);
    let at = args.get(1).number_value(scope).unwrap_or(0.0) as i64;
    match storage::set_alarm(&s, at) {
        Err(error) => throw_storage_error(scope, "setAlarm", error),
        // Committed immediately: register the wake-entry PUT against the
        // cell's output gate. Inside an explicit transaction (`Ok(None)`)
        // the gate registers at that transaction's commit instead.
        Ok(Some(committed)) if committed >= 0 => spawn_arm_gate(&s, committed),
        Ok(_) => {}
    }
}
fn op_alarm_get(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let s = args.get(0).to_rust_string_lossy(scope);
    match storage::get_alarm(&s) {
        Some(at) => rv.set(v8::Number::new(scope, at as f64).into()),
        None => rv.set(v8::null(scope).into()),
    }
}
fn op_alarm_delete(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let s = args.get(0).to_rust_string_lossy(scope);
    if let Err(error) = storage::delete_alarm(&s) {
        throw_storage_error(scope, "deleteAlarm", error);
    }
}

/// The reserved cron cell's whole schedule decision, in one call so the policy
/// has one home in `celld_logic::cron` rather than a JavaScript copy that can
/// drift from it.
///
/// `firedMs` is the occurrence being handled, or negative when the cell is
/// only arming. Returns `{ matching, armAt, armIsRetry }`: which expressions
/// the occurrence belongs to, by index into the list passed in, when to arm
/// next — `null` when the schedule is exhausted and the cell should retire —
/// and whether that deadline is the failure backoff rather than the next
/// occurrence.
fn op_cron_plan(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(list) = v8::Local::<v8::Array>::try_from(args.get(0)) else {
        return;
    };
    let mut crons = Vec::with_capacity(list.length() as usize);
    // Where each parsed expression sits in the list the caller passed. A
    // malformed expression is already refused by `celld deploy`, but the
    // control plane checks only the field count, so one can still arrive here.
    // Skipping it keeps one bad entry from silencing a script's other crons,
    // and this map is what keeps the skip from renumbering them: the caller
    // reports `controller.cron` by index, so a shifted index names the wrong
    // expression for every entry after the bad one.
    let mut positions = Vec::with_capacity(list.length() as usize);
    for index in 0..list.length() {
        let Some(value) = list.get_index(scope, index) else {
            continue;
        };
        if let Ok(cron) = celld_logic::cron::parse(&value.to_rust_string_lossy(scope)) {
            crons.push(cron);
            positions.push(index as usize);
        }
    }
    let fired_ms = args.get(1).number_value(scope).unwrap_or(-1.0) as i64;
    let now_ms = args.get(2).number_value(scope).unwrap_or(0.0) as i64;
    let retry = args.get(3).number_value(scope).unwrap_or(0.0) as i64;
    let failed = args.get(4).boolean_value(scope);

    let matching = if fired_ms >= 0 {
        celld_logic::cron::matching(&crons, fired_ms)
    } else {
        Vec::new()
    };
    let next = celld_logic::cron::next_across(&crons, now_ms);
    // The retry backoff is `alarm::alarm_retry`'s, not a second schedule:
    // a cron that fails behaves like any other failing alarm, except that
    // `cron_rearm` never lets the backoff outlast the next occurrence.
    let retry_at = failed
        .then(|| celld_logic::alarm::alarm_retry(now_ms, retry, retry, true))
        .flatten();
    let arm_at = celld_logic::cron::cron_rearm(next, retry_at);
    // Which of the two the deadline belongs to. `cron_rearm` takes the earlier
    // and gives a tie to the occurrence, so `armAt` alone cannot say — and the
    // caller has to know, because a retry owes the expressions of the
    // occurrence that failed, while a deadline that is an occurrence owes the
    // expressions that match it.
    let arm_is_retry = match (arm_at, next) {
        (Some(at), Some(occurrence)) => at < occurrence,
        (Some(_), None) => true,
        (None, _) => false,
    };

    let indices = v8::Array::new(scope, matching.len() as i32);
    for (slot, index) in matching.iter().enumerate() {
        let value = v8::Number::new(scope, positions[*index] as f64);
        indices.set_index(scope, slot as u32, value.into());
    }
    let result = v8::Object::new(scope);
    let key = v8::String::new(scope, "matching").unwrap();
    result.set(scope, key.into(), indices.into());
    let key = v8::String::new(scope, "armAt").unwrap();
    let value: v8::Local<v8::Value> = match arm_at {
        Some(at) => v8::Number::new(scope, at as f64).into(),
        None => v8::null(scope).into(),
    };
    result.set(scope, key.into(), value);
    let key = v8::String::new(scope, "armIsRetry").unwrap();
    let value = v8::Boolean::new(scope, arm_is_retry);
    result.set(scope, key.into(), value.into());
    rv.set(result.into());
}

fn op_btoa(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use base64::Engine;
    let input = args.get(0).to_rust_string_lossy(scope);
    let bytes: Vec<u8> = input.chars().map(|c| c as u8).collect();
    let s = base64::engine::general_purpose::STANDARD.encode(bytes);
    rv.set(v8::String::new(scope, &s).unwrap().into());
}

// ---- WHATWG text decoders (encoding_rs) ----
//
// These ops back every TextDecoder decode. src/js/text_encoding.js
// resolves the label and owns the decoder's lifetime, and encoding_rs
// does the decoding — for utf-8 and utf-16 as much as for every other
// WHATWG label (windows-1252, Big5, GBK/GB18030, ISO-2022-JP,
// x-user-defined, …). Previous utf-8 and utf-16 JS decoders were slower
// than these ops at every measured size.
// Streaming decoders live in a per-isolate table keyed by id; JS frees
// one on the final decode (last=true), on a fatal error (encoding_rs
// poisons an errored decoder), or via FinalizationRegistry when a
// mid-stream decoder is abandoned. Ids are never reused, so a late
// finalizer free of an already-closed id is a no-op.

/// Live streaming decoders for one isolate, keyed by an id JS holds across
/// awaits.
///
/// An isolate slot rather than a process-wide `Mutex<HashMap<..>>` like
/// [`zlib_streams`]. The reason that one is process-wide holds here too — a
/// `TextDecoder` in streaming mode outlives the turn that made it, and the
/// next turn can run on a different tokio worker — but it argues against a
/// *thread-local*, not against this. A `TextDecoder` is a JS object, so it
/// never leaves the isolate that made it, and every op that touches its
/// decoder runs under that isolate's `v8::Locker`. The lock the embedder
/// already holds is what makes the access exclusive, which `&mut Isolate`
/// on the slot then proves. A second lock inside it buys nothing.
///
/// This removes a ceiling rather than a measured regression, and the
/// difference matters. In isolation the pattern does collapse: replayed on
/// its own — remove, decode outside the lock, insert — 256-byte chunks
/// peaked at two threads and then went *backwards*, 8 threads serving
/// 0.46x what 1 thread served against 6.7x for the same decoding with no
/// table. But celld end to end shows no difference between the two, at any
/// concurrency this hardware can drive, because a chunk costs about 42us
/// of stream and turn machinery around a lock held for about 100ns. So the
/// mutex was not what any cell was waiting on. It was a process-wide
/// serialisation point on a path that every stream now takes, and it went
/// because it does not need to exist, not because it was costing anything
/// yet.
///
/// Dropping the isolate drops the table, so a decoder abandoned by a cell
/// that goes away needs no finalizer to run.
#[derive(Default)]
struct TextDecoders(HashMap<u64, encoding_rs::Decoder>);

static TEXT_DECODER_NEXT: AtomicU64 = AtomicU64::new(1);

/// The calling isolate's decoder table, created on first use.
fn text_decoders<'a>(scope: &'a mut v8::PinScope) -> &'a mut HashMap<u64, encoding_rs::Decoder> {
    if scope.get_slot::<TextDecoders>().is_none() {
        scope.set_slot(TextDecoders::default());
    }
    &mut scope.get_slot_mut::<TextDecoders>().expect("just set").0
}

/// `$$textDecoderLabel(label)` -> canonical lowercase name, or undefined
/// for unknown labels and the replacement encoding (RangeError in JS).
fn op_text_decoder_label(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let label = args.get(0).to_rust_string_lossy(scope);
    if let Some(enc) = encoding_rs::Encoding::for_label_no_replacement(label.as_bytes()) {
        let name = enc.name().to_ascii_lowercase();
        rv.set(v8::String::new(scope, &name).unwrap().into());
    }
}

/// `$$textDecoderNew(name, ignoreBOM)` -> id.
fn op_text_decoder_new(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let name = args.get(0).to_rust_string_lossy(scope);
    let ignore_bom = args.get(1).boolean_value(scope);
    let enc = encoding_rs::Encoding::for_label(name.as_bytes()).expect("JS resolved the label");
    let dec = if ignore_bom {
        enc.new_decoder_without_bom_handling()
    } else {
        enc.new_decoder_with_bom_removal()
    };
    let id = TEXT_DECODER_NEXT.fetch_add(1, Ordering::Relaxed);
    text_decoders(scope).insert(id, dec);
    rv.set(v8::Number::new(scope, id as f64).into());
}

/// `$$textDecoderDecode(id, view, fatal, last)` -> string. Frees the
/// decoder when `last` is true or on a fatal malformed sequence
/// (TypeError).
fn op_text_decoder_decode(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let bytes = view_bytes(args.get(1)).unwrap_or_default();
    let fatal = args.get(2).boolean_value(scope);
    let last = args.get(3).boolean_value(scope);
    // Per spec an empty streaming chunk is a no-op, but encoding_rs
    // 0.8 flushes a pending multibyte lead when fed an empty non-last
    // slice (e.g. Shift_JIS 0x82, "", 0xA0 would yield U+FFFD instead
    // of あ). Skip the decoder entirely.
    if bytes.is_empty() && !last {
        rv.set(v8::String::empty(scope).into());
        return;
    }
    let mut dec = text_decoders(scope)
        .remove(&id)
        .expect("JS holds the only live id");
    let Ok(out) = run_decoder(&mut dec, &bytes, fatal, last) else {
        throw_invalid_encoded_data(scope);
        return;
    };
    if !last {
        text_decoders(scope).insert(id, dec);
    }
    rv.set(v8::String::new(scope, &out).unwrap().into());
}

/// `$$textDecoderDecodeOnce(name, view, fatal, ignoreBOM)` -> string, for
/// a complete buffer.
///
/// The whole decode is one call, so the decoder never outlives it and
/// never reaches [`text_decoders`]. That saves an insert and a remove,
/// and it keeps every non-streaming decode on the node off a table that
/// is one mutex for the whole process — which is most decodes, because
/// `request.text()` and `response.text()` are not streams. A stream
/// still takes that lock twice per chunk, which is a cost per chunk
/// rather than per byte, and it has not been measured under load;
/// sharding the table is the answer if it ever shows.
fn op_text_decoder_decode_once(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let name = args.get(0).to_rust_string_lossy(scope);
    let bytes = view_bytes(args.get(1)).unwrap_or_default();
    let fatal = args.get(2).boolean_value(scope);
    let ignore_bom = args.get(3).boolean_value(scope);
    let enc = encoding_rs::Encoding::for_label(name.as_bytes()).expect("JS resolved the label");
    let mut dec = if ignore_bom {
        enc.new_decoder_without_bom_handling()
    } else {
        enc.new_decoder_with_bom_removal()
    };
    match run_decoder(&mut dec, &bytes, fatal, true) {
        Ok(out) => rv.set(v8::String::new(scope, &out).unwrap().into()),
        Err(()) => throw_invalid_encoded_data(scope),
    }
}

/// Feeds `bytes` to `dec` and answers the text. `Err` means `fatal` was
/// set and the input was malformed.
fn run_decoder(
    dec: &mut encoding_rs::Decoder,
    bytes: &[u8],
    fatal: bool,
    last: bool,
) -> Result<String, ()> {
    let mut out = String::new();
    if fatal {
        let cap = dec
            .max_utf8_buffer_length_without_replacement(bytes.len())
            .unwrap();
        out.reserve(cap);
        if !matches!(
            dec.decode_to_string_without_replacement(bytes, &mut out, last),
            (encoding_rs::DecoderResult::InputEmpty, _),
        ) {
            return Err(());
        }
    } else {
        let cap = dec.max_utf8_buffer_length(bytes.len()).unwrap();
        out.reserve(cap);
        let _ = dec.decode_to_string(bytes, &mut out, last);
    }
    Ok(out)
}

fn throw_invalid_encoded_data(scope: &mut v8::PinScope) {
    let msg = v8::String::new(scope, "The encoded data was not valid.").unwrap();
    let exc = v8::Exception::type_error(scope, msg);
    scope.throw_exception(exc);
}

/// `$$textDecoderFree(id)` — FinalizationRegistry cleanup for a decoder
/// abandoned mid-stream.
fn op_text_decoder_free(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    text_decoders(scope).remove(&id);
}

// ---- the JS harness: Web API + the DO object model ----

mod bootstrap;
mod modules;
use bootstrap::{
    adopt_cell, begin_event_context, build_env, end_event_context, harness_env,
    inject_compatibility_flags, inject_crons, inject_namespace_keys, inject_routing,
    inject_storage_compatibility, install_harness, install_prelude, populate_cf_exports,
    register_class, register_entrypoints,
};
use modules::{
    compile_module, host_import_module_dynamically, install_lazy_globals, op_builtin_module,
    register_loader_modules, register_stubs, register_wasm_modules, resolve_external,
    ModuleRegistry,
};
/// Like `make_request`, but marks the request as incoming. Its signal is
/// registered only if the handler actually suspends, preserving the
/// synchronous request path.
fn make_incoming_request<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    url: &str,
    method: &str,
    body: &RequestBody,
    headers: &[(String, String)],
) -> Result<v8::Local<'s, v8::Value>> {
    let global = tc.get_current_context().global(tc);
    let key = v8::String::new(tc, "__makeIncomingRequest").unwrap();
    let f: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no __makeIncomingRequest"))?
        .try_into()
        .map_err(|_| anyhow!("not fn"))?;
    let url = v8::String::new(tc, url).unwrap();
    let method = v8::String::new(tc, method).unwrap();
    let headers = v8::String::new(
        tc,
        &serde_json::to_string(headers).unwrap_or_else(|_| "[]".into()),
    )
    .unwrap();
    // A streamed body passes its id, and the harness builds the body
    // stream around that id. A held body passes the bytes.
    let (body, stream_id) = match body {
        RequestBody::Bytes(bytes) => (bytes_value(tc, bytes.clone()), v8::undefined(tc).into()),
        RequestBody::Stream(id) => (
            v8::undefined(tc).into(),
            v8::Number::new(tc, *id as f64).into(),
        ),
    };
    let recv = v8::undefined(tc).into();
    f.call(
        tc,
        recv,
        &[url.into(), method.into(), body, headers.into(), stream_id],
    )
    .ok_or_else(|| anyhow!("__makeIncomingRequest threw"))
}

fn register_incoming_request(
    tc: &mut v8::PinScope,
    request_id: RequestId,
    request: v8::Local<v8::Value>,
) -> Result<()> {
    let global = tc.get_current_context().global(tc);
    let key = v8::String::new(tc, "__registerIncomingRequest").unwrap();
    let function: v8::Local<v8::Function> = global
        .get(tc, key.into())
        .ok_or_else(|| anyhow!("no __registerIncomingRequest"))?
        .try_into()
        .map_err(|_| anyhow!("__registerIncomingRequest is not a function"))?;
    let id = v8::String::new(tc, &request_id_string(request_id)).unwrap();
    let recv = v8::undefined(tc).into();
    function
        .call(tc, recv, &[id.into(), request])
        .ok_or_else(|| anyhow!("__registerIncomingRequest threw"))?;
    Ok(())
}

fn finish_incoming_request(tc: &mut v8::PinScope, request_id: RequestId) {
    let global = tc.get_current_context().global(tc);
    let key = v8::String::new(tc, "__finishIncomingRequest").unwrap();
    let Some(value) = global.get(tc, key.into()) else {
        return;
    };
    let Ok(function) = value.try_cast::<v8::Function>() else {
        return;
    };
    let id = v8::String::new(tc, &request_id_string(request_id)).unwrap();
    let recv = v8::undefined(tc).into();
    let _ = function.call(tc, recv, &[id.into()]);
}

fn make_request<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    url: &str,
    method: &str,
    body: &[u8],
    headers: &[(String, String)],
) -> Result<v8::Local<'s, v8::Value>> {
    let g = tc.get_current_context().global(tc);
    let k = v8::String::new(tc, "__makeRequest").unwrap();
    let f: v8::Local<v8::Function> = g.get(tc, k.into()).unwrap().try_into().unwrap();
    let u = v8::String::new(tc, url).unwrap();
    let m = v8::String::new(tc, method).unwrap();
    let b = bytes_value(tc, body.to_vec());
    let h = v8::String::new(tc, &serde_json::to_string(headers)?).unwrap();
    let recv = v8::undefined(tc).into();
    f.call(tc, recv, &[u.into(), m.into(), b, h.into()])
        .ok_or_else(|| anyhow!("makeRequest threw"))
}

fn read_response(scope: &mut v8::PinScope, ret: v8::Local<v8::Value>) -> Result<HttpResponse> {
    let ctx = scope.get_current_context();
    let global = ctx.global(scope);
    let key = v8::String::new(scope, "__readResponse").unwrap();
    let f: v8::Local<v8::Function> = global
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("no __readResponse"))?
        .try_into()
        .map_err(|_| anyhow!("not fn"))?;
    let recv = v8::undefined(scope).into();
    let out = f
        .call(scope, recv, &[ret])
        .ok_or_else(|| anyhow!("readResponse threw"))?;
    // The harness response reader is synchronous. Keeping this assertion at
    // the native boundary prevents a future harness change from quietly
    // reintroducing a nested async runtime while an isolate is entered.
    if out.is_promise() {
        return Err(anyhow!("__readResponse returned a promise"));
    }
    let out = out.to_object(scope).ok_or_else(|| anyhow!("not object"))?;
    let sk = v8::String::new(scope, "status").unwrap();
    let bk = v8::String::new(scope, "bodyBytes").unwrap();
    let tk = v8::String::new(scope, "bodyStreamId").unwrap();
    let hk = v8::String::new(scope, "headersJson").unwrap();
    let wk = v8::String::new(scope, "wsTargetJson").unwrap();
    let status = out
        .get(scope, sk.into())
        .and_then(|v| v.uint32_value(scope))
        .unwrap_or(200) as u16;
    // Body bytes cross as a Uint8Array copied directly out of V8 — no JSON
    // number array (which was ~4x the bytes to serialize + parse per response).
    let body = out
        .get(scope, bk.into())
        .and_then(|v| v8::Local::<v8::ArrayBufferView>::try_from(v).ok())
        .map(|view| {
            let mut buf = vec![0u8; view.byte_length()];
            view.copy_contents(&mut buf);
            buf
        })
        .unwrap_or_default();
    let stream_id = out
        .get(scope, tk.into())
        .and_then(|value| value.integer_value(scope))
        .unwrap_or(0)
        .max(0) as u64;
    let stream = if stream_id == 0 {
        None
    } else {
        http_streams()
            .lock()
            .unwrap()
            .remove(&stream_id)
            .and_then(|stream| stream.source)
            .map(http_chunk_stream)
    };
    if stream_id != 0 && stream.is_none() {
        return Err(anyhow!(
            "response stream {stream_id} is no longer available"
        ));
    }
    let headers = out
        .get(scope, hk.into())
        .and_then(|v| serde_json::from_str(&v.to_rust_string_lossy(scope)).ok())
        .unwrap_or_default();
    let ws_json = out
        .get(scope, wk.into())
        .map(|v| v.to_rust_string_lossy(scope))
        .unwrap_or_else(|| "null".into());
    let ws = serde_json::from_str(&ws_json).ok();
    if status == 101 {
        tracing::info!(%ws_json, has_target = ws.is_some(), "JS WebSocket response");
    }
    Ok(HttpResponse {
        status,
        body,
        stream,
        headers,
        ws,
        write_position: None,
    })
}

#[cfg(all(test, celld_internal_tests))]
fn init_engine_for_tests() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        v8::V8::set_flags_from_string("--expose-gc");
        Engine::init();
    });
}

#[cfg(all(test, celld_internal_tests))]
include!(env!("CELLD_INTERNAL_JS_OBSERVERS"));

/// Externally injected Web-platform tests.
#[cfg(all(test, celld_internal_tests))]
pub(crate) mod conformance_runtime_tests {
    include!(env!("CELLD_CONFORMANCE_JS_RUNTIME_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_web_platform_tests {
    include!(env!("CELLD_CONFORMANCE_WEB_PLATFORM_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
mod call_order_private {
    include!(env!("CELLD_CONFORMANCE_CALL_ORDER_TESTS"));
}
