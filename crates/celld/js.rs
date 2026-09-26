// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The raw V8 adapter and its child modules remain outside the Actor execution
// domain. The Actor-reachable wake and WebSocket state is injected through
// HostServices.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

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
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use v8::{ValueDeserializerHelper, ValueSerializerHelper};

pub(crate) mod input_gate_lifecycle;
use input_gate_lifecycle::{CrossEntryGateClaim, CrossEntryGateClaims};

pub use crate::engine_api::answer_ticket;
pub use crate::engine_api::d1_cell_scope;
pub(crate) use crate::engine_api::decode_durable_object_id;
pub(crate) use crate::engine_api::durable_object_id_for_name;
pub(crate) use crate::engine_api::durable_object_id_hex;
pub(crate) use crate::engine_api::durable_object_id_hmac;
pub(crate) use crate::engine_api::durable_object_id_key;
pub(crate) use crate::engine_api::fail_in_turn_error;
pub use crate::engine_api::failed_write_position;
pub use crate::engine_api::handler_budget;
pub use crate::engine_api::kv_cell_scope;
pub(crate) use crate::engine_api::namespace_key;
pub use crate::engine_api::next_do_request_id;
pub use crate::engine_api::next_request_id;
pub use crate::engine_api::parse_request_id;
pub use crate::engine_api::queue_cell_scope;
pub use crate::engine_api::request_id_string;
#[cfg(celld_internal_tests)]
pub use crate::engine_api::workflow_cell_scope_for_test;
pub use crate::engine_api::CellOverloaded;
pub use crate::engine_api::Compat;
pub use crate::engine_api::FailedInTurn;
pub use crate::engine_api::GatedAnswer;
pub use crate::engine_api::HttpChunkStream;
pub use crate::engine_api::HttpResponse;
pub use crate::engine_api::HttpResponseWebSocket;
pub use crate::engine_api::ModuleSource;
pub use crate::engine_api::QueueBatch;
pub use crate::engine_api::QueueBinding;
pub use crate::engine_api::QueueContentType;
pub use crate::engine_api::QueueDispatchResult;
pub use crate::engine_api::QueueMessage;
pub use crate::engine_api::QueueMetrics;
pub use crate::engine_api::QueueOutcome;
pub use crate::engine_api::QueueRetryBatch;
pub use crate::engine_api::QueueRetryMessage;
pub use crate::engine_api::RequestBody;
pub use crate::engine_api::RequestId;
pub use crate::engine_api::RpcData;
pub use crate::engine_api::RpcOutcome;
pub use crate::engine_api::WorkerConfigOptions;
pub use crate::engine_api::WorkflowBinding;
pub use crate::engine_api::WsTarget;
pub(crate) use crate::engine_api::BARE_NODE_BUILTINS;
pub use crate::engine_api::CELL_OVERLOAD_ERROR_MARKER;
#[cfg(celld_internal_tests)]
pub use crate::host_channels::install_outbound_ws_connector_for_test;
pub(crate) use crate::host_channels::outbound_ws_tx;
pub use crate::host_channels::set_asset_call_tx;
pub use crate::host_channels::set_facet_tx;
pub use crate::host_channels::set_gate_tx;
pub use crate::host_channels::set_kv_blob_store;
pub use crate::host_channels::set_outbound_ws_tx;
pub use crate::host_channels::set_queue_dispatch_tx;
pub use crate::host_channels::set_rpc_call_tx;
pub use crate::host_channels::set_svc_call_tx;
pub use crate::host_channels::set_svc_rpc_tx;
pub use crate::host_channels::AssetCallReq;
pub use crate::host_channels::FacetReq;
pub use crate::host_channels::GateReq;
pub use crate::host_channels::QueueDispatchReq;
pub use crate::host_channels::QueueLeaseRef;
pub use crate::host_channels::RpcCallReq;
pub use crate::host_channels::SvcCallReq;
pub use crate::host_channels::SvcRpcReq;
#[cfg(celld_internal_tests)]
pub use crate::host_channels::TestOutboundWsConnector;
pub(crate) use crate::host_channels::ASSET_CALL_TX;
pub(crate) use crate::host_channels::FACET_TX;
pub(crate) use crate::host_channels::GATE_TX;
pub(crate) use crate::host_channels::QUEUE_DISPATCH_TX;
pub(crate) use crate::host_channels::RPC_CALL_TX;
pub(crate) use crate::host_channels::SVC_CALL_TX;
pub(crate) use crate::host_channels::SVC_RPC_TX;
pub(crate) use crate::http_streams::claim_http_stream;
pub(crate) use crate::http_streams::encode_http_response;
pub(crate) use crate::http_streams::http_stream_read;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::http_stream_read_source_first_for_test;
pub(crate) use crate::http_streams::http_stream_service;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::next_http_stream_chunk;
pub(crate) use crate::http_streams::prepare_http_stream_tee;
pub use crate::http_streams::register_body_stream;
pub(crate) use crate::http_streams::response_stream_close;
pub(crate) use crate::http_streams::response_stream_write;
pub(crate) use crate::http_streams::retain_first_http_cleanup_panic;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::settle_http_stream_read;
pub use crate::http_streams::take_body_stream;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::HttpStreamActivityError;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::HttpStreamActivityKind;
pub(crate) use crate::http_streams::HttpStreamClaim;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::HttpStreamEntry;
pub(crate) use crate::http_streams::HttpStreamService;
pub(crate) use crate::http_streams::HttpStreamSource;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::HttpStreamTermination;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::HttpStreamTerminationReason;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::HttpSweeperExitGate;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::HttpTeePump;
pub(crate) use crate::http_streams::HttpTransferredStream;
pub use crate::http_streams::RequestBodyGuard;
pub(crate) use crate::http_streams::ResponseStreamWriter;
pub(crate) use crate::http_streams::HTTP_STREAM_DONE;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::HTTP_STREAM_IDLE_TIMEOUT_MS;
pub(crate) use crate::http_streams::HTTP_STREAM_REGISTRATION_CLOSED;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::HTTP_TEE_BRANCH_CAPACITY;
#[cfg(all(test, celld_internal_tests))]
pub(crate) use crate::http_streams::NEXT_HTTP_STREAM_ID;
pub use crate::wake_entry::forget_wake_entry;
pub(crate) use crate::wake_entry::launch_arm_gate;
pub(crate) use crate::wake_entry::maintain_wake_entry;
pub(crate) use crate::wake_entry::publish_observed_wake_entry;
pub use crate::wake_entry::set_arm_gate;
pub use crate::wake_entry::ArmGate;
pub(crate) use crate::wake_entry::ArmGateRx;
pub(crate) use crate::wake_entry::WakeEntryService;

#[cfg(all(test, celld_internal_tests))]
fn test_host_services() -> Arc<crate::host_services::HostServices> {
    asyncrt::services()
}

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
    pub body: RequestBody,
    /// Owns a streamed body while the call waits for routing or admission.
    pub body_guard: RequestBodyGuard,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    /// Where this call sits in its caller's order for this cell.
    pub order: Option<CallOrder>,
    /// The dispatching handler's trace context, read from CPED at the
    /// call site, so the cell's span joins the caller's trace.
    pub parent: Option<crate::telemetry::TraceContext>,
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
#[doc(hidden)]
pub struct Place(Option<Box<tokio::sync::oneshot::Receiver<Place>>>);

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
#[doc(hidden)]
pub struct CallChains {
    next_seq: u64,
    pub tails: HashMap<String, (u64, tokio::sync::oneshot::Receiver<Place>)>,
}

/// Take this call's place in its caller's chain for `cell`. Synchronous, so
/// the places are taken in the order the script made the calls.
#[doc(hidden)]
pub fn enter_call_order(caller: Arc<IoContext>, cell: &str) -> CallOrder {
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

pub use r2_ops::set_r2_store;

/// Sample the source under its SQLite lock. A missing source cannot grant
/// publication or cleanup authority; the output gate fails closed for an arm.
pub(crate) fn observe_alarm(cell: &str, at_ms: Option<i64>) -> celld_logic::wake::AlarmSnapshot {
    storage::alarm_snapshot(cell).unwrap_or_else(|error| {
        tracing::warn!(%cell, %error, "alarm observation has no committed wake source");
        celld_logic::wake::AlarmSnapshot::without_wake(at_ms)
    })
}

/// A committed installation needs discovery coverage: launch the entry PUT
/// and register it against the current event's output gate. No-op when the
/// bound already covers it or no gate is configured.
fn spawn_arm_gate(cell: &str, at_ms: i64, context: Option<Arc<IoContext>>) {
    if let Some(rx) = launch_arm_gate(cell, observe_alarm(cell, Some(at_ms))) {
        register_arm_gate_with_current_event(rx, context);
    }
}

fn register_arm_gate_with_current_event(gate: ArmGateRx, context: Option<Arc<IoContext>>) {
    let Some(context) = context else {
        drop(gate);
        return;
    };
    if let Err(gate) = context.register_arm_gate(gate) {
        // The response boundary sealed this context. The PUT itself
        // continues, but its receiver cannot migrate into a later event's
        // reply batch.
        drop(gate);
    }
}

#[cfg(celld_internal_tests)]
fn register_test_pending_arm_gate(cell: &str, gate: ArmGateRx) {
    let services = asyncrt::services();
    let mut pending = services.wake_entry().test_pending.lock().unwrap();
    let gates = pending.entry(cell.to_string()).or_default();
    gates.retain_mut(|gate| {
        matches!(
            gate.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        )
    });
    gates.push(gate);
}

#[cfg(all(test, celld_internal_tests))]
pub(crate) fn spawn_observed_arm_gate_for_test(
    cell: &str,
    alarm: celld_logic::wake::AlarmSnapshot,
) {
    let Some(gate) = launch_arm_gate(cell, alarm) else {
        return;
    };
    match installed_context() {
        Some(context) => register_arm_gate_with_current_event(gate, Some(context)),
        None => register_test_pending_arm_gate(cell, gate),
    }
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn pause_next_arm_gate_for_test() -> tokio::sync::oneshot::Sender<Result<(), String>> {
    let (resume, paused) = tokio::sync::oneshot::channel();
    asyncrt::services()
        .wake_entry()
        .scripted
        .lock()
        .unwrap()
        .push_back(paused);
    resume
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn pause_next_arm_gate_for_cell_for_test(
    cell: &str,
) -> tokio::sync::oneshot::Sender<Result<(), String>> {
    let (resume, paused) = tokio::sync::oneshot::channel();
    asyncrt::services()
        .wake_entry()
        .scripted_by_cell
        .lock()
        .unwrap()
        .entry(cell.to_string())
        .or_default()
        .push_back(paused);
    resume
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn next_arm_gate_is_paused_for_test() -> bool {
    !asyncrt::services()
        .wake_entry()
        .scripted
        .lock()
        .unwrap()
        .is_empty()
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn next_arm_gate_for_cell_is_paused_for_test(cell: &str) -> bool {
    asyncrt::services()
        .wake_entry()
        .scripted_by_cell
        .lock()
        .unwrap()
        .get(cell)
        .is_some_and(|gates| !gates.is_empty())
}

/// Drain the compatibility wake-entry PUT gates for a simulated S1 cell.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub async fn drain_arm_gates(cell: &str) -> Result<(), String> {
    let services = asyncrt::services();
    let gates = services
        .wake_entry()
        .test_pending
        .lock()
        .unwrap()
        .remove(cell);
    let Some(gates) = gates else {
        return Ok(());
    };
    await_arm_gates(gates).await
}

#[derive(Clone, Copy)]
pub struct FetchRequest<'a> {
    pub url: &'a str,
    pub method: &'a str,
    pub body: &'a [u8],
    pub headers: &'a [(String, String)],
    pub request_id: Option<RequestId>,
}

static DO_CALL_CANCELS: OnceLock<
    std::sync::Mutex<HashMap<RequestId, tokio::sync::oneshot::Sender<()>>>,
> = OnceLock::new();
fn do_call_cancels(
) -> &'static std::sync::Mutex<HashMap<RequestId, tokio::sync::oneshot::Sender<()>>> {
    DO_CALL_CANCELS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

enum RpcSignalState {
    Live(HashMap<RequestId, tokio::sync::oneshot::Sender<Option<Vec<u8>>>>),
    Aborted(Vec<u8>),
}

static RPC_SIGNALS: OnceLock<std::sync::Mutex<HashMap<RequestId, RpcSignalState>>> =
    OnceLock::new();

fn rpc_signals() -> &'static std::sync::Mutex<HashMap<RequestId, RpcSignalState>> {
    RPC_SIGNALS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

struct RpcSignalSubscriptionGuard {
    signal: RequestId,
    subscription: RequestId,
}

impl Drop for RpcSignalSubscriptionGuard {
    fn drop(&mut self) {
        let mut signals = rpc_signals().lock().unwrap();
        if let Some(RpcSignalState::Live(subscribers)) = signals.get_mut(&self.signal) {
            subscribers.remove(&self.subscription);
        }
    }
}

/// Publish `request_id` on a pending call's promise as the token the JS
/// harness hands back to `__do_call_cancel`.
///
/// The only writer of `__celldCancelId`, and the counterpart of
/// [`parse_request_id`]. Each cancellable op used to spell the property name
/// and the encoding out for itself, and `op_svc_call_impl` wrote decimal while
/// `op_do_call_cancel` reads hexadecimal. Every decimal digit is also a
/// hexadecimal digit, so the parse succeeded on a different number, the
/// registry lookup missed, and the cancel was dropped with no error — a
/// service-binding abort rejected the caller and left the target running. One
/// writer paired with one parser removes the chance to disagree.
fn attach_cancel_id(
    scope: &mut v8::PinScope,
    promise: v8::Local<v8::Value>,
    request_id: RequestId,
) {
    let Some(object) = promise.to_object(scope) else {
        return;
    };
    let key = v8::String::new(scope, "__celldCancelId").unwrap();
    let value = v8::String::new(scope, &request_id_string(request_id)).unwrap();
    object.set(scope, key.into(), value.into());
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

static NEXT_TIMER_ID: AtomicU64 = AtomicU64::new(1);
static TIMER_CANCELS: OnceLock<std::sync::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<()>>>> =
    OnceLock::new();
fn timer_cancels() -> &'static std::sync::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<()>>> {
    TIMER_CANCELS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Which alarm a dispatch runs, and who owns its bookkeeping.
pub enum AlarmDispatch {
    /// Claim whatever is due now, and record the outcome.
    Due,
    #[cfg(celld_internal_tests)]
    Armed,
    #[cfg(celld_internal_tests)]
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
        body: RequestBody,
        headers: Vec<(String, String)>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
        /// Where this call sits in its caller's order for this cell, if it
        /// came from a script rather than from a peer or the ingress.
        order: Option<CallOrder>,
    },
    Rpc {
        request_id: Option<RequestId>,
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
        reply: tokio::sync::oneshot::Sender<Result<Option<u64>>>,
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
        reply: tokio::sync::oneshot::Sender<Result<Option<u64>>>,
    },
    Alarm {
        /// The shell task identity used to interrupt a firing during drain.
        request_id: Option<RequestId>,
        scope: String,
        scheduled_ms: i64,
        /// Which alarm to run, and who owns the bookkeeping.
        claim: AlarmDispatch,
        /// Replies with (next alarm, the handler's write delta). The delta is
        /// sampled inside the turn — cell storage must never be reached from
        /// the shell — and the shell uses it to prove the consuming commit
        /// durable before the core settles. Every answer shape samples it the
        /// same way now, in `InFlight::answer_settled`.
        reply:
            tokio::sync::oneshot::Sender<Result<(celld_logic::wake::AlarmSnapshot, Option<u64>)>>,
    },
    #[cfg(celld_internal_tests)]
    SyncErrorForTest {
        scope: String,
        gate: ArmGateRx,
        socket_id: Option<u64>,
        terminate: bool,
        reply: tokio::sync::oneshot::Sender<Result<Option<u64>>>,
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
            #[cfg(celld_internal_tests)]
            CellJob::SyncErrorForTest { scope, .. } => scope,
        }
    }

    /// The client request that can cancel this event, when it is a fetch.
    pub(crate) fn request_id(&self) -> Option<RequestId> {
        match self {
            CellJob::Fetch { request_id, .. } => *request_id,
            _ => None,
        }
    }

    /// Queue sends use native RPC, so the method is the narrow seam where the
    /// owner can bound producer events without limiting an ordinary Durable
    /// Object RPC or a Queue alarm and settlement.
    pub(crate) fn is_queue_producer(&self) -> bool {
        matches!(self, CellJob::Rpc { scope, method, .. }
            if scope.split_once(':').is_some_and(|(class, _)|
                class == crate::deploy::QUEUE_CLASS) && method == "__queueSend")
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
            #[cfg(celld_internal_tests)]
            CellJob::SyncErrorForTest { reply, .. } => drop(reply.send(Err(error))),
        }
    }
}

thread_local! {
    // One outbound HTTP client per JS thread — building it per fetch rebuilds
    // the TLS stack every call (async-op-hazards.md).
    static HTTP: reqwest::Client = reqwest::Client::new();
    static HTTP_MANUAL: reqwest::Client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none()).build().unwrap();
    // A separate policy stops an `error` request before reqwest can replay it
    // at the destination. Inspecting the final response would be too late:
    // the method, body, and credentials could already have left the process.
    static HTTP_ERROR: reqwest::Client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            attempt.error("fetch redirect mode is error")
        })).build().unwrap();
}

#[derive(Debug)]
struct RedirectSubrequestLimit(String);

impl std::fmt::Display for RedirectSubrequestLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RedirectSubrequestLimit {}

fn fetch_request_error(error: &reqwest::Error) -> String {
    let mut source: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(limit) = source.downcast_ref::<RedirectSubrequestLimit>() {
            return limit.0.clone();
        }
        let Some(next) = source.source() else {
            return format!("fetch: {error}");
        };
        source = next;
    }
}

#[doc(hidden)]
pub mod websocket;
pub(crate) use websocket::WebSocketService;
pub use websocket::*;
use websocket::{
    ws_capture_begin, ws_capture_discard, ws_capture_flush_touched, ws_capture_set_promise,
    ws_capture_take, ws_close_request_sockets,
};

/// What an outbound effect must trail before it leaves the process.
///
/// The three cases are named rather than nested inside one another because
/// two of them once shared a representation and lost this distinction:
/// a reader that starts after another request committed samples the new
/// position as its own baseline and advances nothing, so a two-state gate read
/// it as code that owns no cell at all and let its side effect out ungated.
/// A variant per case makes the distinction one the compiler keeps.
enum EgressGate {
    /// No cell event is running. Stateless Worker code owns no cell state, so
    /// its egress reveals nothing that can still be lost and leaves directly.
    NoCell,
    /// This event wrote through the position. The effect waits for that
    /// write. The last field is the activation epoch the position belongs to.
    Wrote(String, celld_logic::Channel, u64, Option<u64>),
    /// A read-only output. It reveals whatever the cell holds, so it carries
    /// the position it observed above the cell's published baseline, for the
    /// core to hold it behind the proof of a commit whose handler has not
    /// taken a ticket yet, and it trails the newest write barrier still open
    /// on the cell otherwise. Only the core knows which writes are
    /// outstanding, so this asks rather than guesses. The last field is the
    /// activation epoch the sample was taken at.
    ReadOnly(String, celld_logic::Channel, Option<u64>, Option<u64>),
    /// A facet's storage failed. No proof can cover its write, so the effect
    /// fails closed. The field is the storage failure.
    Unpersisted(String),
    /// An effect of a facet that wrote: the facet's own stream proves the
    /// write before the root cell's gate verdicts the rest, as workerd's
    /// facet output gate holds the effect. Fields: the root cell's gate, the
    /// facet's stream, and the activation epoch.
    Facet(Box<EgressGate>, String, u64),
}

impl EgressGate {
    /// Whether the effect must consult the gate before it leaves. True for a
    /// read as well as a write: a read whose cell has no barrier is released
    /// at once, but only the core can say so.
    fn is_gated(&self) -> bool {
        !matches!(self, EgressGate::NoCell)
    }

    /// The cell whose event raised the effect. The host derives this from the
    /// active event, so JavaScript cannot claim another cell's authority.
    fn cell_scope(&self) -> Option<&str> {
        match self {
            EgressGate::NoCell | EgressGate::Unpersisted(_) => None,
            EgressGate::Wrote(cell, ..) | EgressGate::ReadOnly(cell, ..) => Some(cell),
            EgressGate::Facet(root, ..) => root.cell_scope(),
        }
    }
}

/// The cell event the executing JavaScript belongs to.
///
/// The continuation's own context first, from CPED: V8 runs a promise
/// reaction of cell A inside another event's microtask checkpoint, and the
/// thread-local turn owner then names that other event. An effect raised by
/// A's reaction would otherwise be gated against the wrong cell's position, or
/// refused as belonging to the wrong cell. The turn owner remains the answer
/// for a context that installs no continuation token.
fn event_context(scope: &mut v8::PinScope) -> Arc<IoContext> {
    current_reaction_io_context(scope).unwrap_or_else(current_context)
}

/// What one cell event's outbound effects gate against.
#[derive(Clone)]
struct EgressFrame {
    /// The scope whose connection records this event's writes: the cell for an
    /// ordinary event, and the facet for an event of an embedded facet.
    storage: String,
    /// The committed-write position of `storage` when the event started.
    before: u64,
    /// The root cell's gate, when `storage` names an embedded facet. Kept
    /// beside the scope it stores under, so an effect cannot take one without
    /// the other and charge a facet's egress to the facet.
    root: Option<storage::RootGate>,
}

/// The frame of the running event, if a cell event is running at all.
fn current_cell_event(context: &IoContext) -> Option<EgressFrame> {
    context.egress.lock().unwrap().last().cloned()
}

/// Return and clear the number of output-gate samples for one test cell.
#[cfg(all(test, celld_internal_tests))]
pub(crate) fn take_egress_gate_samples_for_test(scope: &str, channel: celld_logic::Channel) -> u64 {
    test_host_services()
        .wake_entry()
        .egress_gate_samples
        .lock()
        .unwrap()
        .remove(&(scope.to_string(), channel))
        .unwrap_or(0)
}

/// Sample what the running handler's outbound effects must trail. Answers for
/// every active cell event, including a read whose core lookup can settle
/// immediately. Every sample here happens in the calling turn; the ticket
/// carries them to the host loop, which must not reach cell storage itself.
fn egress_gate_request(context: &IoContext, channel: celld_logic::Channel) -> EgressGate {
    let Some(frame) = current_cell_event(context) else {
        return EgressGate::NoCell;
    };
    #[cfg(all(test, celld_internal_tests))]
    {
        let cell = frame
            .root
            .as_ref()
            .map(|root| root.cell.clone())
            .unwrap_or_else(|| frame.storage.clone());
        *test_host_services()
            .wake_entry()
            .egress_gate_samples
            .lock()
            .unwrap()
            .entry((cell, channel))
            .or_default() += 1;
    }
    let (cell, before) = (frame.storage, frame.before);
    let sample = match storage::write_position(&cell) {
        Ok(sample) => sample,
        Err(error) => return EgressGate::Unpersisted(error.to_string()),
    };
    if let Some(root) = frame.root {
        return facet_egress_gate(&cell, root, channel);
    }
    let epoch = storage::activation_epoch(&cell);
    match sample.filter(|position| *position > before) {
        Some(position) => EgressGate::Wrote(cell, channel, position, epoch),
        // A read-only output in a process that configured no output gate has
        // nothing to trail. Such a process cannot acknowledge a write either
        // -- `await_egress_gate` fails a write closed for want of the same
        // channel -- so no committed-but-unproven write exists for a read to
        // reveal. Answer as
        // if no cell event were running, which keeps the effect on the direct,
        // synchronous path it has always taken; a WebSocket frame in
        // particular must not join a deferred queue whose flush needs a gate
        // to release it. A write still takes a ticket and still fails closed.
        None if GATE_TX.get().is_none() => EgressGate::NoCell,
        None => {
            let observed = storage::observed_position(&cell, sample);
            EgressGate::ReadOnly(cell, channel, observed, epoch)
        }
    }
}

/// Sample an effect raised by an event of an embedded facet.
///
/// A facet is not a cell: no Worker exports its class under the id
/// `facet_scope` builds, so a ticket in the facet's name activates a cell
/// that cannot start. The effect can reveal what the root cell showed the
/// facet, so it trails the root cell's reader gate with the sample the parent
/// took for the call; a write of the facet's own is proved on its stream.
fn facet_egress_gate(
    facet: &str,
    root: storage::RootGate,
    channel: celld_logic::Channel,
) -> EgressGate {
    if let Some(error) = storage::sql_critical_error(facet) {
        return EgressGate::Unpersisted(error);
    }
    // A process with no output gate has nothing to trail, and a WebSocket
    // frame must keep its direct path (see `egress_gate_request`).
    if GATE_TX.get().is_none() {
        return EgressGate::NoCell;
    }
    let gate = EgressGate::ReadOnly(root.cell, channel, root.observed, Some(root.epoch));
    // Any effect can reveal a facet write, this event's or an earlier one's,
    // so every effect waits for the facet's stream, as workerd's facet
    // output gate holds it.
    EgressGate::Facet(Box::new(gate), root.stream, root.epoch)
}

/// Prove a facet's stream through the host loop, which owns the replicator.
async fn prove_facet(stream: String, epoch: u64) -> std::result::Result<(), GateRefusal> {
    // No host loop means no replicator, so nothing can prove the write and
    // nothing can lose it to a later proof either.
    let Some(sender) = FACET_TX.get() else {
        return Ok(());
    };
    let (reply, receive) = tokio::sync::oneshot::channel();
    let request = FacetReq::Prove {
        stream,
        epoch,
        reply,
    };
    sender.send(request).map_err(|_| GateRefusal::Dropped)?;
    match receive.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(GateRefusal::Unpersisted(format!("{error:#}"))),
        Err(_) => Err(GateRefusal::Dropped),
    }
}

/// Why the output gate did not release a ticket.
enum GateRefusal {
    /// The process installed no gate channel. A write must still fail closed
    /// here: an acknowledgement nobody can prove is the loss this gate exists
    /// to prevent.
    NoChannel,
    /// The core answered, and the answer is that the write is not durable.
    Unproven(celld_logic::RequestError),
    /// The shell dropped the ticket before the core answered.
    Dropped,
    /// A facet's write never reached a database a proof can cover.
    Unpersisted(String),
}

impl std::fmt::Display for GateRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GateRefusal::NoChannel => f.write_str("no output-gate channel"),
            GateRefusal::Unproven(error @ celld_logic::RequestError::DurabilityUnproven) => {
                write!(
                    f,
                    "the write this request follows is not durable ({error:?})"
                )
            }
            GateRefusal::Unproven(error) => {
                write!(f, "the output gate refused the ticket ({error:?})")
            }
            GateRefusal::Dropped => f.write_str("output gate dropped"),
            GateRefusal::Unpersisted(error) => {
                write!(f, "the facet's write did not reach its database ({error})")
            }
        }
    }
}

/// Take one ticket on the output gate and wait for the core's verdict.
///
/// This is the output gate applied to an in-handler effect rather than to the
/// response. It cannot deadlock the handler that is awaiting it: the ticket is
/// served by `dispatch_gate` on the host's own loop and resolved by the
/// replicator's independent task, neither of which needs the isolate's event
/// loop to run. A read-only ticket carries `None` and the core releases it at
/// once when the cell has no barrier open, so an ordinary read pays one actor
/// hop and no replica write.
async fn egress_gate_verdict(gate: EgressGate) -> std::result::Result<(), GateRefusal> {
    let gate = match gate {
        EgressGate::Facet(root, stream, epoch) => {
            prove_facet(stream, epoch).await?;
            *root
        }
        gate => gate,
    };
    let (cell, channel, position, observed, epoch) = match gate {
        EgressGate::Facet(..) => unreachable!("a facet gate nests no facet gate"),
        EgressGate::NoCell => return Ok(()),
        EgressGate::Unpersisted(error) => return Err(GateRefusal::Unpersisted(error)),
        EgressGate::Wrote(cell, channel, position, epoch) => {
            (cell, channel, Some(position), None, epoch)
        }
        EgressGate::ReadOnly(cell, channel, observed, epoch) => {
            (cell, channel, None, observed, epoch)
        }
    };
    let (tx, receive) = tokio::sync::oneshot::channel();
    let sent = GATE_TX
        .get()
        .map(|gate| {
            gate.send(GateReq {
                scope: cell,
                ticket: crate::actor::GateTicket {
                    channel,
                    position,
                    observed,
                    epoch,
                },
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    if !sent {
        return Err(GateRefusal::NoChannel);
    }
    match receive.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(GateRefusal::Unproven(error)),
        Err(_) => Err(GateRefusal::Dropped),
    }
}

/// Wait for the writes an outbound effect can reveal to be proven durable
/// before it leaves the process. See `egress_gate_verdict`.
async fn await_egress_gate(gate: EgressGate) -> std::result::Result<(), String> {
    egress_gate_verdict(gate)
        .await
        .map_err(|refusal| match refusal {
            GateRefusal::Unproven(_) | GateRefusal::Unpersisted(_) => {
                format!("refusing to send: {refusal}")
            }
            GateRefusal::NoChannel | GateRefusal::Dropped => refusal.to_string(),
        })
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
static CELL_GATES: OnceLock<Mutex<HashMap<String, CellGate>>> = OnceLock::new();
static NEXT_GATE_EVENT: AtomicU64 = AtomicU64::new(1);
// Cell gates are process-wide, so their owner ids must not repeat in another
// isolate. A per-isolate sequence can make one event appear to own another
// isolate's gate and keep the wrong event alive.
static NEXT_IO_CONTEXT_ID: AtomicU64 = AtomicU64::new(0);

fn allocate_io_context_id() -> u64 {
    NEXT_IO_CONTEXT_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .unwrap_or_else(|_| panic!("the process exhausted its IoContext ids"))
        + 1
}

/// One cell's input gate, with the event that holds it and the events queued
/// to take it, named by the continuation id of their `IoContext`.
///
/// The critical section binds this owner to its continuation's operation
/// driver before acquisition. An ordinary continuation belongs to its origin;
/// a critical section retains the driver that started it, even when a foreign
/// checkpoint resumes its callback. The origin claim preserves the separate
/// resource context. Recording only the ambient turn here lets that event
/// retire while another driver polls the acquisition, rejecting a live block.
#[derive(Default)]
struct CellGate {
    gate: celld_logic::gate::InputGate,
    holder: Option<u64>,
    /// A reaction can belong to an event other than the turn owner. The claim
    /// keeps that originating event and its resources active until the block
    /// ends. A same-event block needs no claim because its `InFlight` owns the
    /// gate itself.
    origin: Option<CrossEntryGateClaim>,
    /// Counted, because one event can start several blocks at once.
    queued: HashMap<u64, u32>,
}

/// How many holds and queued takes exist across every gate, so the checks
/// every turn makes can skip the map while no event blocks at all, which is
/// nearly always.
static GATE_ENGAGEMENTS: AtomicUsize = AtomicUsize::new(0);

fn cell_gates() -> &'static Mutex<HashMap<String, CellGate>> {
    CELL_GATES.get_or_init(|| Mutex::new(HashMap::new()))
}

impl CellGate {
    fn take(
        &mut self,
        event: celld_logic::gate::EventId,
        owner: Option<u64>,
        origin: &mut Option<CrossEntryGateClaim>,
    ) -> bool {
        let was_open = self.gate.is_open();
        let taken = self.gate.acquire(event);
        if taken && was_open {
            self.set_holder(owner, origin.take());
        }
        taken
    }

    fn set_holder(&mut self, owner: Option<u64>, origin: Option<CrossEntryGateClaim>) {
        if self.holder.is_none() && owner.is_some() {
            GATE_ENGAGEMENTS.fetch_add(1, Ordering::Relaxed);
        }
        self.holder = owner;
        self.origin = origin;
    }

    fn clear_holder(&mut self) -> Option<CrossEntryGateClaim> {
        if self.holder.take().is_some() {
            GATE_ENGAGEMENTS.fetch_sub(1, Ordering::Relaxed);
        }
        self.origin.take()
    }

    fn release(
        &mut self,
        event: celld_logic::gate::EventId,
    ) -> (bool, Option<CrossEntryGateClaim>) {
        if !self.gate.release(event) {
            return (false, None);
        }
        (true, self.clear_holder())
    }

    fn abandon(
        &mut self,
        event: celld_logic::gate::EventId,
    ) -> (bool, Option<CrossEntryGateClaim>) {
        if !self.gate.abandon(event) {
            return (false, None);
        }
        (true, self.clear_holder())
    }

    fn is_unused(&self) -> bool {
        self.gate.is_open() && self.queued.is_empty()
    }
}

/// How the event running under `context` is engaged with the cells' gates:
/// it holds one, it is queued to take one, or neither.
///
/// Every gate is searched, not only the gate of the event's own cell: a
/// reaction of cell A's event can run inside cell B's checkpoint, and the
/// block it starts then belongs to A's gate while its ops belong to B's
/// entry. B's entry is the one that must keep them, and it finds itself
/// through its own id whichever gate names it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum GateEngagement {
    Holds,
    Queued,
    None,
}

fn gate_engagement(cell: Option<&str>, context: &Arc<IoContext>) -> GateEngagement {
    if GATE_ENGAGEMENTS.load(Ordering::Relaxed) == 0 {
        return GateEngagement::None;
    }
    let Some(id) = context.continuation_id() else {
        return GateEngagement::None;
    };
    fn of(gate: &CellGate, id: u64) -> GateEngagement {
        if !gate.gate.is_open() && gate.holder == Some(id) {
            GateEngagement::Holds
        } else if gate.queued.contains_key(&id) {
            GateEngagement::Queued
        } else {
            GateEngagement::None
        }
    }
    let gates = cell_gates().lock().unwrap();
    // The event's own cell first: nearly every block is on it, and the walk
    // below is the price of the cross-cell case alone.
    if let Some(gate) = cell.and_then(|cell| gates.get(cell)) {
        let engagement = of(gate, id);
        if engagement != GateEngagement::None {
            return engagement;
        }
    }
    let mut engagement = GateEngagement::None;
    for gate in gates.values() {
        match of(gate, id) {
            GateEngagement::Holds => return GateEngagement::Holds,
            GateEngagement::Queued => engagement = GateEngagement::Queued,
            GateEngagement::None => {}
        }
    }
    engagement
}

/// What a waiting event is told when the gate finally opens: nothing, or
/// the reason the holder failed.
type GateWake = tokio::sync::oneshot::Sender<Result<(), String>>;

static GATE_WAITERS: OnceLock<Mutex<HashMap<String, Vec<GateWake>>>> = OnceLock::new();

/// Events waiting for a cell's input gate to open, in arrival order.
///
/// An earlier implementation deleted this queue because an event the gate
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
    if gates.get(cell).is_none_or(|gate| gate.gate.is_open()) {
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

enum CellGateAcquisition {
    Acquired,
    Waiting {
        gate: tokio::sync::oneshot::Receiver<Result<(), String>>,
        retirement: tokio::sync::watch::Receiver<bool>,
    },
    Retired,
}

/// Acquire `cell` for `event`, or enqueue its next attempt before the gate can
/// change state.
///
/// A failed holder wakes its existing waiters with an error. Separating the
/// failed acquisition from waiter registration lets that failure land between
/// the two, so the event can miss the error and run against the reset actor.
/// The context lock also covers the successful host claim, so event retirement
/// cannot land between acquiring the process gate and recording its owner.
fn acquire_cell_gate(
    context: &IoContext,
    cell: &str,
    event: celld_logic::gate::EventId,
    owner: Option<u64>,
    origin: &mut Option<CrossEntryGateClaim>,
) -> CellGateAcquisition {
    let mut claims = context.input_gates.lock().unwrap();
    if claims.retired {
        return CellGateAcquisition::Retired;
    }
    let mut gates = cell_gates().lock().unwrap();
    if gates
        .entry(cell.to_string())
        .or_default()
        .take(event, owner, origin)
    {
        let previous = claims.held.insert(event, cell.to_string());
        assert!(previous.is_none(), "an input-gate event id was reused");
        return CellGateAcquisition::Acquired;
    }
    let (wake, waiter) = tokio::sync::oneshot::channel();
    gate_waiters()
        .lock()
        .unwrap()
        .entry(cell.to_string())
        .or_default()
        .push(wake);
    let retirement = claims.retirement.subscribe();
    drop(gates);
    CellGateAcquisition::Waiting {
        gate: waiter,
        retirement,
    }
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

const ABANDONED_INPUT_GATE: &str = "the cell's critical section ended without releasing";
const RETIRED_INPUT_GATE: &str = "the cell event ended before it could acquire an input gate";

/// Release one completed block and remove the open gate from the process map.
fn release_cell_gate(cell: &str, event: celld_logic::gate::EventId, outcome: Result<(), String>) {
    let origin = {
        let mut gates = cell_gates().lock().unwrap();
        let Some(gate) = gates.get_mut(cell) else {
            return;
        };
        let (opened, origin) = gate.release(event);
        // A stale release or a remaining nested hold cannot wake the waiters.
        // In particular, a stale error must not fail a replacement holder's
        // queued events.
        if !opened {
            return;
        }
        if gate.is_unused() {
            gates.remove(cell);
        }
        origin
    };
    // Releasing the origin claim can wake its drive and eventually run
    // `IoContext::drop`, which also locks the gate map. Keep that work outside
    // the map's lock.
    drop(origin);
    wake_gate_waiters(cell, outcome);
}

/// Retire `context`'s waiters and abandon every input gate that it still owns.
///
/// The event id is part of each claim. A concurrent event can already be
/// running when this one dies, so a cell name alone cannot safely decide
/// which holder to release. Removing the open map entry also prevents an old
/// cell incarnation from leaving process-wide state for a later one.
fn abandon_context_input_gates(context: &IoContext) -> Vec<(String, celld_logic::gate::EventId)> {
    let claims = context.retire_input_gates();
    for (cell, event) in &claims {
        let (abandoned, origin) = {
            let mut gates = cell_gates().lock().unwrap();
            let (abandoned, origin) = gates
                .get_mut(cell)
                .map_or((false, None), |gate| gate.abandon(*event));
            if abandoned {
                gates.remove(cell);
            }
            (abandoned, origin)
        };
        // See `release_cell_gate`: releasing an origin can re-enter this map
        // from its eventual context destructor.
        drop(origin);
        if abandoned {
            wake_gate_waiters(cell, Err(ABANDONED_INPUT_GATE.to_string()));
        }
    }
    claims
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
    #[cfg(celld_internal_tests)]
    let input = bytes.as_ptr() as usize;
    let len = bytes.len();
    let store = v8::ArrayBuffer::new_backing_store_from_vec(bytes).make_shared();
    let buffer = v8::ArrayBuffer::with_backing_store(scope, &store);
    #[cfg(celld_internal_tests)]
    record_bytes_value_allocation_for_test(
        input,
        buffer
            .get_backing_store()
            .data()
            .map(|data| data.as_ptr() as usize),
    );
    v8::Uint8Array::new(scope, buffer, 0, len).unwrap().into()
}

#[cfg(celld_internal_tests)]
static BYTES_VALUE_ALLOCATION_PROBE: Mutex<Option<(usize, Option<usize>)>> = Mutex::new(None);

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn capture_bytes_value_allocation_for_test(expected: *const u8) {
    *BYTES_VALUE_ALLOCATION_PROBE.lock().unwrap() = Some((expected as usize, None));
}

#[cfg(celld_internal_tests)]
fn record_bytes_value_allocation_for_test(input: usize, output: Option<usize>) {
    let mut probe = BYTES_VALUE_ALLOCATION_PROBE.lock().unwrap();
    if probe
        .as_ref()
        .is_some_and(|(expected, _)| *expected == input)
    {
        *probe = Some((input, output));
    }
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn take_bytes_value_allocation_for_test() -> Option<usize> {
    BYTES_VALUE_ALLOCATION_PROBE
        .lock()
        .unwrap()
        .take()
        .and_then(|(_, output)| output)
}

#[cfg(unix)]
fn clock_nanos(clock: libc::clockid_t) -> Option<u64> {
    // SAFETY: `clock_gettime` initializes the complete `timespec` on success,
    // and the pointer remains valid for the call.
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    if unsafe { libc::clock_gettime(clock, time.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: The successful call above initialized `time`.
    let time = unsafe { time.assume_init() };
    let seconds = u64::try_from(time.tv_sec).ok()?;
    let nanos = u64::try_from(time.tv_nsec).ok()?;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

#[cfg(unix)]
fn platform_thread_cpu_nanos() -> Option<u64> {
    clock_nanos(libc::CLOCK_THREAD_CPUTIME_ID)
}

#[cfg(not(unix))]
fn platform_thread_cpu_nanos() -> Option<u64> {
    None
}

fn thread_cpu_nanos() -> Option<u64> {
    #[cfg(all(test, celld_internal_tests))]
    if let Some(sample) =
        THREAD_CPU_NANOS_SAMPLES_FOR_TEST.with(|samples| samples.borrow_mut().pop_front())
    {
        return sample;
    }
    platform_thread_cpu_nanos()
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
struct TargetThreadCpuClock(libc::clockid_t);

#[cfg(target_os = "linux")]
impl TargetThreadCpuClock {
    fn current() -> Option<Self> {
        unsafe extern "C" {
            fn pthread_getcpuclockid(
                thread: libc::pthread_t,
                clock_id: *mut libc::clockid_t,
            ) -> libc::c_int;
        }
        let mut clock_id = std::mem::MaybeUninit::<libc::clockid_t>::uninit();
        // SAFETY: `pthread_self` returns the calling thread, and the output
        // pointer remains valid for the call.
        if unsafe { pthread_getcpuclockid(libc::pthread_self(), clock_id.as_mut_ptr()) } != 0 {
            return None;
        }
        // SAFETY: The successful call above initialized `clock_id`.
        Some(Self(unsafe { clock_id.assume_init() }))
    }

    fn nanos(self) -> Option<u64> {
        clock_nanos(self.0)
    }
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
struct TargetThreadCpuClock(libc::mach_port_t);

#[cfg(target_os = "macos")]
impl TargetThreadCpuClock {
    fn current() -> Option<Self> {
        // SAFETY: Both calls inspect the calling thread and transfer no
        // ownership. The returned Mach thread port remains valid while this
        // turn holds the execution thread.
        Some(Self(unsafe {
            libc::pthread_mach_thread_np(libc::pthread_self())
        }))
    }

    fn nanos(self) -> Option<u64> {
        let mut info = std::mem::MaybeUninit::<libc::thread_basic_info>::uninit();
        let mut count = libc::THREAD_BASIC_INFO_COUNT;
        // SAFETY: `info` has the size described by `count`, and `thread_info`
        // initializes it completely when the call succeeds.
        let status = unsafe {
            libc::thread_info(
                self.0,
                u32::try_from(libc::THREAD_BASIC_INFO).expect("thread info flavor is positive"),
                info.as_mut_ptr().cast::<libc::integer_t>(),
                &mut count,
            )
        };
        if status != libc::KERN_SUCCESS {
            return None;
        }
        // SAFETY: The successful call above initialized `info`.
        let info = unsafe { info.assume_init() };
        let user_seconds = u64::try_from(info.user_time.seconds).ok()?;
        let user_micros = u64::try_from(info.user_time.microseconds).ok()?;
        let system_seconds = u64::try_from(info.system_time.seconds).ok()?;
        let system_micros = u64::try_from(info.system_time.microseconds).ok()?;
        user_seconds
            .checked_add(system_seconds)?
            .checked_mul(1_000_000_000)?
            .checked_add(user_micros.checked_add(system_micros)?.checked_mul(1_000)?)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
#[derive(Clone, Copy)]
struct TargetThreadCpuClock;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl TargetThreadCpuClock {
    fn current() -> Option<Self> {
        None
    }

    fn nanos(self) -> Option<u64> {
        None
    }
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
static SHUTDOWN_ABORTS: OnceLock<std::sync::Mutex<std::collections::HashSet<RequestId>>> =
    OnceLock::new();
static PENDING_ABORT_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn pending_aborts() -> &'static std::sync::Mutex<std::collections::HashSet<RequestId>> {
    PENDING_ABORTS.get_or_init(Default::default)
}

fn shutdown_aborts() -> &'static std::sync::Mutex<std::collections::HashSet<RequestId>> {
    SHUTDOWN_ABORTS.get_or_init(Default::default)
}

#[cfg(all(test, celld_internal_tests))]
type AbortRequestPauseForTest = (
    RequestId,
    std::sync::mpsc::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

#[cfg(all(test, celld_internal_tests))]
static ABORT_REQUEST_PAUSE_FOR_TEST: Mutex<Option<AbortRequestPauseForTest>> = Mutex::new(None);

#[cfg(all(test, celld_internal_tests))]
pub(crate) struct AbortRequestPauseHandleForTest {
    entered: std::sync::mpsc::Receiver<()>,
    release: Option<std::sync::mpsc::Sender<()>>,
}

#[cfg(all(test, celld_internal_tests))]
impl AbortRequestPauseHandleForTest {
    pub(crate) fn wait_until_entered(&self) {
        self.entered
            .recv()
            .expect("the abort publisher dropped before its publication seam");
    }

    pub(crate) fn release(mut self) {
        self.release
            .take()
            .expect("the abort publication pause was released twice")
            .send(())
            .expect("the abort publisher dropped while paused");
    }
}

#[cfg(all(test, celld_internal_tests))]
pub(crate) fn pause_abort_request_for_test(
    request_id: RequestId,
) -> AbortRequestPauseHandleForTest {
    let (entered_tx, entered) = std::sync::mpsc::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let previous = ABORT_REQUEST_PAUSE_FOR_TEST
        .lock()
        .unwrap()
        .replace((request_id, entered_tx, release_rx));
    assert!(
        previous.is_none(),
        "an abort publication pause is already armed"
    );
    AbortRequestPauseHandleForTest {
        entered,
        release: Some(release),
    }
}

#[cfg(all(test, celld_internal_tests))]
pub(crate) fn pause_abort_request_if_armed_for_test(request_id: RequestId) {
    let pause = {
        let mut pause = ABORT_REQUEST_PAUSE_FOR_TEST.lock().unwrap();
        if pause
            .as_ref()
            .is_some_and(|(expected, _, _)| *expected == request_id)
        {
            pause.take()
        } else {
            None
        }
    };
    if let Some((_, entered, release)) = pause {
        entered
            .send(())
            .expect("the abort publication test dropped its observer");
        release
            .recv()
            .expect("the abort publication test dropped its release");
    }
}

#[cfg(all(test, celld_internal_tests))]
pub(crate) fn request_cancellation_state_for_test(request_id: RequestId) -> (bool, bool) {
    (
        pending_aborts().lock().unwrap().contains(&request_id),
        cancelled_requests().lock().unwrap().contains(&request_id),
    )
}

/// Mark an in-flight request cancelled from any thread.
pub fn abort_request(request_id: RequestId) {
    #[cfg(all(test, celld_internal_tests))]
    pause_abort_request_if_armed_for_test(request_id);
    if pending_aborts().lock().unwrap().insert(request_id) {
        PENDING_ABORT_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}

/// Force a cell event to retire for a lifecycle transition. Unlike a client
/// disconnect, shutdown cannot preserve `waitUntil` work because the runtime
/// and its ownership are about to leave this process.
pub fn abort_request_for_shutdown(request_id: RequestId) {
    shutdown_aborts().lock().unwrap().insert(request_id);
    abort_request(request_id);
}

pub(crate) fn clear_request_cancellation(request_id: RequestId) {
    let _ = take_pending_abort(request_id);
    cancelled_requests().lock().unwrap().remove(&request_id);
    shutdown_aborts().lock().unwrap().remove(&request_id);
}

pub fn take_shutdown_cancellation(request_id: Option<RequestId>) -> bool {
    request_id.is_some_and(|request_id| shutdown_aborts().lock().unwrap().remove(&request_id))
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn clear_request_cancellation_for_test(request_id: RequestId) {
    clear_request_cancellation(request_id);
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
    let function = event_hook(tc, |hooks| &hooks.abort_incoming_request)?;
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
        // V8 is process-global, so the guard belongs to the initializer. A
        // guard at each caller allowed two callers to each initialize V8 once,
        // which poisoned the global state when both ran.
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            #[cfg(celld_internal_tests)]
            v8::V8::set_flags_from_string("--expose-gc");
            // No `v8::icu::set_common_data_78` call: the rusty_v8 152 prebuilt
            // statically links the complete ICU data (full locale tables and
            // regex property-of-strings, e.g. /\p{RGI_Emoji}/v — verified
            // empirically), so overriding it with an embedded icudtl.dat only
            // duplicated 10.8 MB in the binary. The required coverage is
            // explicit, so a future v8 bump that reduces the builtin data
            // fails rather than silently narrowing it; the fix would be to
            // re-embed rusty_v8's `third_party/icu/common/icudtl.dat`,
            // 16-byte-aligned.
            let platform = v8::new_default_platform(0, false).make_shared();
            v8::V8::initialize_platform(platform);
            v8::V8::initialize();
        });
    }
}

/// The resource name the entry module compiles under. It is also the name the
/// entry module is projected under in `/bundle`, so a stack frame and a bundle
/// path name the same file.
const ENTRY_MODULE_NAME: &str = "worker.js";

/// One queue's push consumer after the owning script has been resolved.
#[derive(Clone)]
pub struct QueueConsumerRegistration {
    pub script: String,
    pub config: crate::protocol::QueueConsumerConfig,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WorkerOrigin {
    Deployment,
    Dynamic,
}

type ResourceLimits = crate::WorkerInvocationLimits;

fn effective_resource_limits(
    configured: Option<ResourceLimits>,
    invocation: Option<ResourceLimits>,
) -> Option<ResourceLimits> {
    let lower = |configured: Option<u32>, invocation: Option<u32>| match (configured, invocation) {
        (Some(configured), Some(invocation)) => Some(configured.min(invocation)),
        (configured, invocation) => configured.or(invocation),
    };
    let limits = ResourceLimits {
        cpu_ms: lower(
            configured.and_then(|limits| limits.cpu_ms),
            invocation.and_then(|limits| limits.cpu_ms),
        ),
        sub_requests: lower(
            configured.and_then(|limits| limits.sub_requests),
            invocation.and_then(|limits| limits.sub_requests),
        ),
    };
    (limits.cpu_ms.is_some() || limits.sub_requests.is_some()).then_some(limits)
}

struct CpuTurnStart {
    thread_nanos: Option<u64>,
    wall: Instant,
}

#[derive(Clone)]
struct LoaderEnvRoute {
    script: String,
    entrypoint: Option<String>,
    props: Vec<u8>,
}

#[derive(Clone)]
struct LoaderEnv {
    bytes: Vec<u8>,
    routes: Vec<LoaderEnvRoute>,
}

pub struct WorkerConfig {
    src: String,
    /// The canonical module name for `src`. Dynamic Workers preserve the
    /// caller's `mainModule` path so relative imports use its directory.
    main_module_name: String,
    pub script_name: String,
    /// The construction path that grants Dynamic Worker load semantics.
    /// A script name is caller-controlled, so it cannot prove this origin.
    origin: WorkerOrigin,
    do_classes: Vec<String>,
    bindings: Vec<(String, String)>,
    /// `r2_buckets`: (environment name, bucket name). The bucket name is
    /// the key space the binding owns inside the fleet bucket; see [[r2]].
    r2_bindings: Vec<(String, String)>,
    /// `d1_databases`: (environment name, stable database identity). The
    /// identity addresses the cell that holds the database; see [[d1]].
    d1_bindings: Vec<(String, String)>,
    /// `kv_namespaces`: (environment name, namespace identity). The identity is
    /// the config's `id` verbatim, and it addresses the namespace's cells; see
    /// [[kv]].
    kv_bindings: Vec<(String, String)>,
    queue_bindings: Vec<QueueBinding>,
    pub(crate) queue_consumers: Vec<QueueConsumerRegistration>,
    /// This script declared a consumer in its own manifest. The deployment-
    /// wide catalog later replaces `queue_consumers` in every isolate, so the
    /// catalog cannot answer whether this particular default export can omit
    /// `fetch` and provide only `queue`.
    declares_queue_consumer: bool,
    workflow_bindings: Vec<WorkflowBinding>,
    vars: Vec<(String, String)>,
    node: String,
    /// The worker's non-main modules, so the main module can import siblings.
    modules: Vec<(String, ModuleSource)>,
    compat: Compat,
    /// `[[services]]`: (binding name, target script, optional entrypoint).
    /// The target runs in this process; see [[service-bindings]].
    services: Vec<(String, String, Option<String>)>,
    asset_binding: Option<String>,
    /// `env` names of the Worker Loader bindings, if this Worker can spawn
    /// dynamic isolates. A config can declare more than one loader, and each
    /// binding gets a cache of its own, so a name loaded through one loader
    /// is a different Worker from the same name loaded through another.
    loader_bindings: Vec<String>,
    /// Ambient outbound authority. Loaded workers may be denied.
    egress: EgressPolicy,
    /// Structured-clone values and host-minted service routes that a loaded
    /// worker receives. Loader-only; empty for a normal worker.
    loader_env: Option<LoaderEnv>,
    /// Per-invocation limits supplied with a dynamically loaded Worker.
    /// Deployment Workers leave this empty.
    resource_limits: Option<ResourceLimits>,
    /// Whether a Dynamic Worker can receive a tail report sender on a fetch.
    tail_reporting: bool,
    /// `triggers.crons` from the deployment. Empty for a loaded worker and for
    /// any script without cron triggers.
    pub crons: Vec<String>,
    /// `containers` from the deployment: the classes whose `ctx.container`
    /// exists. Empty for a loaded worker.
    pub containers: Vec<crate::container::ContainerSpec>,
    /// The application generation this configuration belongs to. Every
    /// isolate built from it carries the value as a slot, so its host calls
    /// resolve against the deployment graph it was built with.
    pub generation: crate::generation::GenerationId,
    /// The external (`node:*`/`cloudflare:*`) imports of `src`.
    ///
    /// The scan walks the whole bundle, which an esbuild artifact makes
    /// megabytes, and its answer depends only on `src` — a field nothing
    /// mutates after construction. It used to run inside `load_config`,
    /// so a deployment paid it again on every cell wake and on every
    /// stateless pool thread. One `Arc<WorkerConfig>` backs all of those
    /// isolates, so scanning here pays it once instead.
    main_imports: modules::ExternalImports,
    /// The same scan for each `ModuleSource::EsModule` sibling, in the order
    /// `es_module_sources` yields them. Read through
    /// [`WorkerConfig::es_modules`], which hands a module and its scan out
    /// together so no caller can pair a module with another module's scan.
    module_imports: Vec<modules::ExternalImports>,
}

/// The `ModuleSource::EsModule` siblings of a worker, as (name, source).
///
/// The single definition of that order: `WorkerConfig::new` scans in it and
/// `WorkerConfig::es_modules` zips against it, so the two cannot disagree.
fn es_module_sources(modules: &[(String, ModuleSource)]) -> impl Iterator<Item = (&str, &str)> {
    modules.iter().filter_map(|(name, source)| match source {
        ModuleSource::EsModule(source) => Some((name.as_str(), source.as_str())),
        _ => None,
    })
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TailLog {
    timestamp: i64,
    level: String,
    message: Vec<String>,
}

const TAIL_LOG_LIMIT_BYTES: usize = 256 * 1024;

#[derive(Default)]
struct TailLogCapture {
    records: Vec<TailLog>,
    /// The serialized record bytes, including separators but excluding the
    /// two array delimiters. The records and this measure share one lock, so
    /// no caller can retain a record without charging it to the same budget.
    serialized_bytes: usize,
    /// A record that crosses the limit ends capture for this invocation.
    /// Continuing after that record would expose later context that Workerd
    /// omits, even when a smaller record could fit in the remaining budget.
    full: bool,
}

#[derive(Default)]
struct JsonByteCounter(usize);

impl std::io::Write for JsonByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialized_json_bytes(value: &impl serde::Serialize) -> usize {
    let mut counter = JsonByteCounter::default();
    serde_json::to_writer(&mut counter, value).expect("TailLog serialization is infallible");
    counter.0
}

#[cfg(all(test, celld_internal_tests))]
fn tail_log_limit_bytes_for_test() -> usize {
    TAIL_LOG_LIMIT_BYTES
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
            kv_bindings,
            queue_bindings,
            queue_consumers,
            workflow_bindings,
            vars,
            node,
            modules,
            compat,
        } = options;
        let main_imports = modules::scan_external_imports(&src);
        let module_imports = es_module_sources(&modules)
            .map(|(_name, source)| modules::scan_external_imports(source))
            .collect();
        let declares_queue_consumer = !queue_consumers.is_empty();
        let queue_consumers = queue_consumers
            .into_iter()
            .map(|config| QueueConsumerRegistration {
                script: script_name.clone(),
                config,
            })
            .collect();
        Self {
            src,
            main_module_name: ENTRY_MODULE_NAME.to_string(),
            script_name,
            origin: WorkerOrigin::Deployment,
            do_classes,
            bindings,
            r2_bindings,
            d1_bindings,
            kv_bindings,
            queue_bindings,
            queue_consumers,
            declares_queue_consumer,
            workflow_bindings,
            vars,
            node,
            modules,
            compat,
            services: Vec::new(),
            asset_binding: None,
            loader_bindings: Vec::new(),
            egress: EgressPolicy::Allow,
            loader_env: None,
            resource_limits: None,
            tail_reporting: false,
            crons: Vec::new(),
            containers: Vec::new(),
            generation: 0,
            main_imports,
            module_imports,
        }
    }

    pub fn with_containers(mut self, containers: Vec<crate::container::ContainerSpec>) -> Self {
        self.containers = containers;
        self
    }

    /// Whether cells of `class` supervise a container.
    pub fn container_class(&self, class: &str) -> bool {
        self.containers.iter().any(|spec| spec.class_name == class)
    }

    /// Stamp this Worker with the application generation it serves.
    pub fn with_generation(mut self, generation: crate::generation::GenerationId) -> Self {
        self.generation = generation;
        self
    }

    pub fn with_queue_consumers(mut self, consumers: Vec<QueueConsumerRegistration>) -> Self {
        self.queue_consumers = consumers;
        self
    }

    /// Grant the relaxed load contract used only by a Worker Loader isolate.
    fn into_dynamic_worker(mut self) -> Self {
        self.origin = WorkerOrigin::Dynamic;
        self
    }

    fn with_main_module_name(mut self, name: String) -> Self {
        self.main_module_name = name;
        self
    }

    fn with_tail_reporting(mut self, enabled: bool) -> Self {
        self.tail_reporting = enabled;
        self
    }
    /// Every `ModuleSource::EsModule` sibling with its name, its source and
    /// its scanned external imports.
    fn es_modules(&self) -> impl Iterator<Item = (&str, &str, &modules::ExternalImports)> {
        // `zip` would truncate silently if the two ever disagreed, and a
        // truncated scan means a sibling links against a stub missing its
        // names. Both are filled from `es_module_sources` in `new`, so a
        // disagreement is a bug in a future mutator, not a reachable state.
        debug_assert_eq!(
            es_module_sources(&self.modules).count(),
            self.module_imports.len(),
            "an ES sibling lost its scanned imports",
        );
        es_module_sources(&self.modules)
            .zip(&self.module_imports)
            .map(|((name, source), imports)| (name, source, imports))
    }

    /// Give this Worker the deployment's cron trigger expressions.
    pub fn with_crons(mut self, crons: Vec<String>) -> Self {
        self.crons = crons;
        self
    }

    /// Grant this Worker a Worker Loader binding at each `env` name.
    pub fn with_loaders(mut self, bindings: Vec<String>) -> Self {
        self.loader_bindings = bindings;
        self
    }

    /// Set this Worker's ambient outbound authority (loaded workers only).
    fn with_egress(mut self, egress: EgressPolicy) -> Self {
        self.egress = egress;
        self
    }

    /// Install the limits that every invocation of this Dynamic Worker owns.
    fn with_resource_limits(mut self, limits: Option<ResourceLimits>) -> Self {
        self.resource_limits = limits;
        self
    }

    /// Merge the caller's structured-clone `env` onto a loaded worker's env.
    fn with_loader_env(mut self, env: Option<LoaderEnv>) -> Self {
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

/// The storage authority installed when a cell enters an isolate.
///
/// The path and epoch form one value because opening either one without the
/// other would let later asynchronous work use the wrong ownership epoch.
#[doc(hidden)]
pub struct CellStorage<'a> {
    pub path: &'a str,
    pub epoch: u64,
    /// Fleet ownership supplies persistent writer epochs. Standalone ownership
    /// resets on restart and never publishes discovery objects.
    pub replicated_wake: bool,
    /// The activation's paged VFS, when its restore paged. The file at `path`
    /// is then sparse; opening it without this VFS reads holes as data.
    pub vfs: Option<&'a str>,
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
    runtime_state: Arc<ActorRuntimeState>,
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
    /// Identity used to reclaim every dynamic Worker this isolate created.
    /// Named Worker Loader identity lives in this isolate's JavaScript heap,
    /// so no child can remain charged to the registry after its owner is gone.
    loader_owner: LoaderOwner,
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
    fn cpu_watchdog(
        &self,
        remaining: Option<Duration>,
        declared_ms: Option<u32>,
    ) -> Option<CpuWatchdog> {
        CpuWatchdog::start(
            remaining,
            declared_ms,
            self.isolate.thread_safe_handle(),
            self.runtime_state.clone(),
        )
    }

    fn initial_cpu_budget(&self) -> Option<Duration> {
        self.runtime_state
            .resource_limits
            .and_then(|limits| limits.cpu_ms)
            .map(|milliseconds| Duration::from_millis(u64::from(milliseconds)))
    }

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
    /// An explicit admission refusal cannot use the condemnation flag because
    /// recovery clears that flag at the next turn.
    #[cfg(celld_internal_tests)]
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

#[cfg(celld_internal_tests)]
fn admission_refusal_forced(state: &HeapLimitState) -> bool {
    state.forced_admission_refusal.load(Ordering::Relaxed)
}

#[cfg(not(celld_internal_tests))]
fn admission_refusal_forced(_state: &HeapLimitState) -> bool {
    false
}

type SerializedPut = (String, Vec<u8>);
type PendingPuts = HashMap<String, Vec<SerializedPut>>;

/// One Fetcher route transferred from a Worker Loader parent into its child.
/// The generation and the complete target travel together, so a loaded Worker
/// cannot accidentally resolve the route against a later deployment.
#[derive(Clone, Debug, PartialEq)]
struct OutboundBroker {
    generation: crate::generation::GenerationId,
    script: String,
    entrypoint: Option<crate::WorkerFetchEntrypoint>,
}

/// Ambient outbound authority for an isolate. Normal workers keep `Allow`; a
/// Worker Loader can deny egress or route it through a transferred Fetcher.
#[derive(Clone, Default, PartialEq)]
enum EgressPolicy {
    #[default]
    Allow,
    Deny,
    Broker(OutboundBroker),
}

#[derive(Default)]
struct ActorRuntimeState {
    promises: std::sync::Mutex<PromiseMap>,
    termination: std::sync::Mutex<Option<ExecutionTermination>>,
    pending_puts: std::sync::Mutex<PendingPuts>,
    io_contexts: std::sync::Mutex<HashMap<u64, Weak<IoContext>>>,
    /// WebSocket captures that received a frame in the current isolate turn.
    /// Weak references record every continuation context without extending
    /// an event's lifetime merely to flush its output.
    ws_capture_touched: Mutex<Vec<Weak<IoContext>>>,
    egress: EgressPolicy,
    resource_limits: Option<ResourceLimits>,
    tail_reporting: bool,
    script_name: String,
    event_hooks: OnceLock<EventHooks>,
    /// See [`internals`].
    internals: OnceLock<v8::Global<v8::Object>>,
    /// Set once a patch is installed, so an unpatched isolate pays one
    /// relaxed load per op and not a lock: the patch check runs on every op
    /// call of every test, and the lock cost 18% on an op-bound loop.
    #[cfg(celld_internal_tests)]
    op_patched: AtomicBool,
    #[cfg(celld_internal_tests)]
    op_patches: Mutex<HashMap<&'static str, v8::Global<v8::Function>>>,
    #[cfg(celld_internal_tests)]
    op_patches_active: Mutex<HashSet<&'static str>>,
}

/// The harness functions the host calls on the boundary of every cell event.
///
/// `harness.js` installs `__beginEvent`, `__endEvent`, `__advanceIoTime`, and
/// `__abortIncomingRequest` on the internals object once per isolate and
/// never replaces them. Reading each one back by name per event costs a fresh
/// `v8::String` plus a lookup for a result that cannot change; holding the
/// functions removes the string and the lookup together.
///
/// The four are resolved together on purpose. A partial resolution would
/// leave one hook still reached by name, so `install_harness` builds all four
/// or the isolate fails to load, and no caller has to remember which of them
/// is cached.
///
/// A `v8::Global` is only valid in the isolate that created it, which is why
/// this hangs off `ActorRuntimeState` — an isolate slot — and not off a
/// process-wide `static`. `ModuleRegistry` documents the same constraint.
struct EventHooks {
    begin_event: v8::Global<v8::Function>,
    end_event: v8::Global<v8::Function>,
    advance_io_time: v8::Global<v8::Function>,
    abort_incoming_request: v8::Global<v8::Function>,
}

/// One cached hook, opened into `scope`.
///
/// `pick` names the hook rather than a getter per hook, so the four call
/// sites stay one line each.
fn event_hook<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    pick: fn(&EventHooks) -> &v8::Global<v8::Function>,
) -> Result<v8::Local<'s, v8::Function>> {
    let state = actor_runtime_state(scope);
    let hooks = state
        .event_hooks
        .get()
        .ok_or_else(|| anyhow!("event hooks are not installed"))?;
    Ok(v8::Local::new(scope, pick(hooks)))
}

/// Advance the isolate clock at an I/O boundary before JavaScript resumes.
///
/// The cached hook is installed before user code loads, so a script cannot
/// replace the function that owns this invariant. Each caller is a complete
/// JavaScript turn caused by external input or by a completed native op.
/// The return value is the sample that an alarm turn must use for its due
/// comparison, so the host and JavaScript observe one event time.
fn advance_io_time(scope: &mut v8::PinScope) -> i64 {
    let timestamp_ms = unix_now_ms();
    let hook = event_hook(scope, |hooks| &hooks.advance_io_time)
        .expect("the isolate clock hook is installed");
    let timestamp = v8::Number::new(scope, timestamp_ms as f64);
    let recv = v8::undefined(scope).into();
    hook.call(scope, recv, &[timestamp.into()])
        .expect("the isolate clock hook cannot throw");
    timestamp_ms
}

impl ActorRuntimeState {
    fn io_context(&self, id: u64) -> Option<Arc<IoContext>> {
        let mut contexts = self.io_contexts.lock().unwrap();
        let context = contexts.get(&id)?.upgrade();
        if context.is_none() {
            contexts.remove(&id);
        }
        context
    }
}

struct ExecutionTermination {
    error: String,
    actor_scope: Option<String>,
    context_id: Option<u64>,
}

#[cfg(all(test, celld_internal_tests))]
static CPU_FINISH_TERMINATION_FOR_TEST: Mutex<Option<(usize, usize)>> = Mutex::new(None);

#[cfg(all(test, celld_internal_tests))]
fn terminate_on_cpu_finish_for_test(runtime_state: &Arc<ActorRuntimeState>, finish: usize) {
    let previous = CPU_FINISH_TERMINATION_FOR_TEST
        .lock()
        .unwrap()
        .replace((Arc::as_ptr(runtime_state) as usize, finish));
    assert!(
        previous.is_none(),
        "a CPU finish termination is already armed"
    );
}

#[cfg(all(test, celld_internal_tests))]
fn inject_cpu_finish_termination_for_test(
    tc: &mut v8::PinScope,
    entry: &InFlight,
) -> Option<String> {
    let mut armed = CPU_FINISH_TERMINATION_FOR_TEST.lock().unwrap();
    let (runtime_state, remaining) = armed.as_mut()?;
    if *runtime_state != Arc::as_ptr(&entry.runtime_state) as usize {
        return None;
    }
    *remaining = remaining.saturating_sub(1);
    if *remaining != 0 {
        return None;
    }
    armed.take();
    let error = "Worker exceeded CPU limit of 10 ms".to_string();
    *entry
        .runtime_state
        .termination
        .lock()
        .expect("termination lock poisoned") = Some(ExecutionTermination {
        error: error.clone(),
        actor_scope: None,
        context_id: None,
    });
    tc.terminate_execution();
    Some(error)
}

fn finish_cpu_turn(_tc: &mut v8::PinScope, entry: &InFlight) -> Result<(), String> {
    #[cfg(all(test, celld_internal_tests))]
    if let Some(error) = inject_cpu_finish_termination_for_test(_tc, entry) {
        return Err(error);
    }
    entry.context.finish_cpu_turn()
}

/// A backstop for JavaScript that never returns to the host's exact CPU
/// sample. Exact accounting rejects finite turns in `finish_cpu_turn`; this
/// scheduler exists because an infinite loop has no later boundary at which
/// that sample can run. One process-wide thread watches all isolates. A thread
/// per turn consumed a PID under cgroup `pids.max` and paid an OS thread spawn
/// and join for every JavaScript continuation.
struct CpuWatchdog {
    id: u64,
}

struct CpuWatch {
    limit: Duration,
    declared_ms: u32,
    target_clock: Option<TargetThreadCpuClock>,
    target_started: Option<u64>,
    next_check: Instant,
    isolate: v8::IsolateHandle,
    runtime_state: Arc<ActorRuntimeState>,
}

#[derive(Default)]
struct CpuWatchdogState {
    next_id: u64,
    watches: HashMap<u64, CpuWatch>,
}

struct CpuWatchdogService {
    shared: Arc<(Mutex<CpuWatchdogState>, std::sync::Condvar)>,
}

static CPU_WATCHDOG_SERVICE: OnceLock<CpuWatchdogService> = OnceLock::new();

fn cpu_watchdog_remaining(
    limit: Duration,
    started_nanos: Option<u64>,
    current_nanos: Option<u64>,
) -> Option<Duration> {
    let elapsed = started_nanos
        .zip(current_nanos)
        .map(|(started, current)| current.saturating_sub(started))?;
    let limit_nanos = u64::try_from(limit.as_nanos()).unwrap_or(u64::MAX);
    (elapsed < limit_nanos).then(|| Duration::from_nanos(limit_nanos - elapsed))
}

impl CpuWatchdog {
    fn start(
        limit: Option<Duration>,
        declared_ms: Option<u32>,
        isolate: v8::IsolateHandle,
        runtime_state: Arc<ActorRuntimeState>,
    ) -> Option<Self> {
        let limit = limit?;
        let declared_ms = declared_ms?;
        let service = cpu_watchdog_service();
        let target_clock = TargetThreadCpuClock::current();
        let watch = CpuWatch {
            limit,
            declared_ms,
            target_clock,
            target_started: target_clock.and_then(TargetThreadCpuClock::nanos),
            next_check: Instant::now() + limit,
            isolate,
            runtime_state,
        };
        let (state, wake) = &*service.shared;
        let mut state = state.lock().unwrap();
        let id = state.next_id;
        state.next_id = state.next_id.wrapping_add(1);
        state.watches.insert(id, watch);
        drop(state);
        wake.notify_one();
        Some(Self { id })
    }

    #[cfg(all(test, celld_internal_tests))]
    fn is_registered_for_test(&self) -> bool {
        cpu_watchdog_service()
            .shared
            .0
            .lock()
            .unwrap()
            .watches
            .contains_key(&self.id)
    }
}

impl Drop for CpuWatchdog {
    fn drop(&mut self) {
        let (state, wake) = &*cpu_watchdog_service().shared;
        state.lock().unwrap().watches.remove(&self.id);
        wake.notify_one();
    }
}

fn cpu_watchdog_service() -> &'static CpuWatchdogService {
    CPU_WATCHDOG_SERVICE.get_or_init(|| {
        let shared = Arc::new((
            Mutex::new(CpuWatchdogState::default()),
            std::sync::Condvar::new(),
        ));
        let scheduler = shared.clone();
        std::thread::Builder::new()
            .name("celld-cpu-watchdog".to_string())
            .spawn(move || run_cpu_watchdog_service(&scheduler))
            .expect("spawn the process CPU watchdog");
        CpuWatchdogService { shared }
    })
}

fn run_cpu_watchdog_service(shared: &Arc<(Mutex<CpuWatchdogState>, std::sync::Condvar)>) -> ! {
    let (state, wake) = &**shared;
    let mut state = state.lock().unwrap();
    loop {
        let Some(next_check) = state.watches.values().map(|watch| watch.next_check).min() else {
            state = wake.wait(state).unwrap();
            continue;
        };
        let now = Instant::now();
        if now < next_check {
            let (next_state, _) = wake.wait_timeout(state, next_check - now).unwrap();
            state = next_state;
            continue;
        }

        let due = state
            .watches
            .iter()
            .filter_map(|(id, watch)| (watch.next_check <= now).then_some(*id))
            .collect::<Vec<_>>();
        for id in due {
            let Some(watch) = state.watches.get_mut(&id) else {
                continue;
            };
            if let Some(remaining) = cpu_watchdog_remaining(
                watch.limit,
                watch.target_started,
                watch.target_clock.and_then(TargetThreadCpuClock::nanos),
            ) {
                watch.next_check = Instant::now() + remaining;
                continue;
            }
            let watch = state.watches.remove(&id).unwrap();
            let mut termination = watch
                .runtime_state
                .termination
                .lock()
                .expect("termination lock poisoned");
            if termination.is_none() {
                *termination = Some(ExecutionTermination {
                    error: format!("Worker exceeded CPU limit of {} ms", watch.declared_ms),
                    actor_scope: None,
                    context_id: None,
                });
                drop(termination);
                watch.isolate.terminate_execution();
            }
        }
    }
}

fn cpu_watchdog_for_context(scope: &mut v8::PinScope, context: &IoContext) -> Option<CpuWatchdog> {
    let runtime_state = actor_runtime_state(scope);
    let declared_ms = context
        .cpu_limit_nanos
        .and_then(|limit| u32::try_from(limit / 1_000_000).ok());
    CpuWatchdog::start(
        context.remaining_cpu_budget(),
        declared_ms,
        scope.thread_safe_handle(),
        runtime_state,
    )
}

fn finish_terminated_actor_event(scope: &mut v8::PinScope, context: &IoContext) {
    finish_retired_input_gate_context(scope, context);
}

fn finish_retired_input_gate_context(scope: &mut v8::PinScope, context: &IoContext) {
    context.force_retire_cross_entry_gates();
    let _ = abandon_context_input_gates(context);
    let context_id = context.continuation_id().unwrap_or_default();
    retire_input_gate_js_context(scope, context_id);
}

fn retire_input_gate_js_context(scope: &mut v8::PinScope, context_id: u64) {
    let Ok(function) = internal_function(scope, "__retireInputGateContext") else {
        return;
    };
    let context_id = v8::String::new(scope, &context_id.to_string()).unwrap();
    let recv = v8::undefined(scope).into();
    let _ = function.call(scope, recv, &[context_id.into()]);
}

/// The fetch handler of a loaded Worker that exports none: workerd's text,
/// thrown when a request reaches it, as on Cloudflare.
const NO_FETCH_HANDLER: &str =
    "() => { throw new Error('Handler does not export a fetch() function.'); }";

/// Evaluate a host-written JS expression, with `__celld` bound, to a function.
fn compile_fn<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    src: &str,
) -> Result<v8::Local<'s, v8::Function>> {
    let value = run_internal_snippet(scope, &format!("return {src};"))
        .ok_or_else(|| anyhow!("compile shim: {src}"))?;
    value
        .try_into()
        .map_err(|_| anyhow!("shim is not a function: {src}"))
}

/// Whether `register_entrypoints` put `name` in the `__cell.<registry>`
/// object (e.g. `entrypoints`, `doExports`).
fn cell_registry_has(scope: &mut v8::PinScope, registry: &str, name: &str) -> Result<bool> {
    let cell = cell_state(scope)?;
    let registry_key = v8::String::new(scope, registry).unwrap();
    let registry_obj = cell
        .get(scope, registry_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell.{registry} registry"))?;
    let name_key = v8::String::new(scope, name).unwrap();
    Ok(registry_obj
        .has_own_property(scope, name_key.into())
        .unwrap_or(false))
}

fn take_execution_termination_in_context(
    scope: &mut v8::PinScope,
    context: Option<&IoContext>,
) -> Option<anyhow::Error> {
    let state = scope.get_slot::<Arc<ActorRuntimeState>>().cloned();
    let termination = state
        .as_ref()
        .and_then(|state| state.termination.lock().ok()?.take());
    let is_terminating = scope.is_execution_terminating();
    if termination.is_some() || is_terminating {
        scope.cancel_terminate_execution();
    }
    if let Some(termination) = termination {
        if termination.actor_scope.is_some() {
            if let Some(context_id) = termination.context_id {
                if let Some(context) = state
                    .as_ref()
                    .and_then(|state| state.io_context(context_id))
                {
                    finish_terminated_actor_event(scope, &context);
                }
            } else if let Some(context) = context {
                finish_terminated_actor_event(scope, context);
            } else {
                finish_terminated_actor_event(scope, &current_context());
            }
        }
        return Some(anyhow!(termination.error));
    }
    is_terminating.then(|| anyhow!("JavaScript execution was terminated"))
}

fn take_execution_termination(scope: &mut v8::PinScope) -> Option<anyhow::Error> {
    take_execution_termination_in_context(scope, None)
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
        // Detach children while their IDs are still attributable to this
        // parent, but do not drop their V8 isolates while this one is entered.
        // In-flight child calls hold their own receiver clones and finish
        // normally; this removes only the registry's ownership reference.
        let loaded_children = take_loader_owner(&self.loader_owner);
        let limit = self.original_heap_limit;
        {
            let (mut locker, _cells) = self.lock();
            locker.remove_near_heap_limit_callback(near_heap_limit, limit);
        }
        drop(loaded_children);
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
    Queue(tokio::sync::oneshot::Sender<Result<QueueDispatchResult>>),
    /// A DO method call, which answers a value rather than a response.
    CellRpc(tokio::sync::oneshot::Sender<Result<RpcOutcome>>),
    /// A `webSocketMessage`, which answers the frames the output gate held
    /// and the write they are gated on.
    WsMessage(tokio::sync::oneshot::Sender<Result<WsDispatch>>),
    /// An event whose result is that it finished: `webSocketOpen`,
    /// `webSocketClose`. It answers the position its writes reached, so the
    /// shell can open the barrier they need.
    Ack(tokio::sync::oneshot::Sender<Result<Option<u64>>>),
    /// An alarm, which answers whatever alarm the handler left armed.
    Alarm(tokio::sync::oneshot::Sender<Result<(celld_logic::wake::AlarmSnapshot, Option<u64>)>>),
}

/// The invocation data that becomes one Tail Worker event after all referenced
/// work finishes. The response sender stays independent, so tail delivery
/// cannot delay or replace the Dynamic Worker response.
struct TailReportState {
    reply: tokio::sync::oneshot::Sender<String>,
    script_name: String,
    event_timestamp: i64,
    url: String,
    method: String,
    headers: serde_json::Map<String, serde_json::Value>,
    response_status: Option<u16>,
    failure: Option<TailException>,
}

/// A failure and the instant it occurred. Tail delivery can wait for
/// `waitUntil` work, so sampling the clock when the report finishes would
/// move the exception forward by the duration of that work.
#[derive(serde::Serialize)]
struct TailException {
    timestamp: i64,
    name: &'static str,
    message: String,
}

impl TailException {
    fn new(message: String) -> Self {
        Self {
            timestamp: crate::telemetry::now_unix_us() / 1_000,
            name: "Error",
            message,
        }
    }
}

fn tail_request_headers(
    headers: &[(String, String)],
) -> serde_json::Map<String, serde_json::Value> {
    let mut normalized = serde_json::Map::new();
    for (name, value) in headers {
        match normalized.entry(name.to_ascii_lowercase()) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(serde_json::Value::String(value.clone()));
            }
            serde_json::map::Entry::Occupied(mut entry) => {
                let serde_json::Value::String(joined) = entry.get_mut() else {
                    unreachable!("tail request headers contain only strings");
                };
                joined.push_str(", ");
                joined.push_str(value);
            }
        }
    }
    normalized
}

impl TailReportState {
    fn record_failure(&mut self, message: String) {
        self.failure = Some(TailException::new(message));
    }

    fn finish(self, context: &IoContext) {
        let outcome = if self.failure.is_some() {
            "exception"
        } else {
            "ok"
        };
        let exceptions: Vec<_> = self.failure.into_iter().collect();
        let event = serde_json::json!([{
            "scriptName": self.script_name,
            "event": {
                "request": {
                    "cf": {},
                    "headers": self.headers,
                    "method": self.method,
                    "url": self.url,
                },
                "response": self.response_status.map(|status| serde_json::json!({
                    "status": status,
                })),
            },
            "eventTimestamp": self.event_timestamp,
            "logs": context.take_tail_logs(),
            "exceptions": exceptions,
            "outcome": outcome,
        }]);
        let _ = self.reply.send(event.to_string());
    }
}

impl Answer {
    /// Send an error, whichever shape the caller is waiting for.
    fn fail(self, error: anyhow::Error) {
        let _ = self.fail_with_arm_gates(error, Vec::new());
    }

    fn fail_with_arm_gates(
        self,
        error: anyhow::Error,
        gates: Vec<ArmGateRx>,
    ) -> Option<GatedReplyRx> {
        match self {
            Answer::Fetch(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::Rpc(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::Queue(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::CellRpc(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::WsMessage(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::Ack(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::Alarm(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
        }
    }
}

async fn await_arm_gates(gates: Vec<ArmGateRx>) -> Result<(), String> {
    let mut first_failure = None;
    for gate in gates {
        let failure = match gate.await {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(_) => Some("wake-entry gate task dropped".into()),
        };
        if first_failure.is_none() {
            first_failure = failure;
        }
    }
    match first_failure {
        Some(failure) => Err(failure),
        None => Ok(()),
    }
}

fn send_answer_after_arm_gates<T: Send + 'static>(
    mut reply: tokio::sync::oneshot::Sender<Result<T>>,
    value: Result<T>,
    gates: Vec<ArmGateRx>,
) -> Option<GatedReplyRx> {
    if gates.is_empty() {
        drop(reply.send(value));
        return None;
    }
    let (completed, completion) = tokio::sync::oneshot::channel();
    let (cancel, mut cancelled) = tokio::sync::oneshot::channel();
    #[cfg(celld_internal_tests)]
    let drop_task = asyncrt::services()
        .wake_entry()
        .drop_next_gated_reply_task
        .swap(false, Ordering::AcqRel);
    asyncrt::spawn(async move {
        #[cfg(celld_internal_tests)]
        if drop_task {
            return;
        }
        enum GateWait {
            Completed(Result<(), String>),
            CallerClosed,
            Cancelled,
            DriverDropped,
        }
        let gate_wait = {
            let mut waiting = Box::pin(await_arm_gates(gates));
            let result = asyncrt::select_biased! {
                "a completed gate aggregate wins a tie with caller cancellation";
                result = &mut waiting => GateWait::Completed(result),
                cancellation = async {
                    asyncrt::select_biased! {
                        "a closed caller wins a tie with an explicit cancellation signal";
                        _ = reply.closed() => GateWait::CallerClosed,
                        cancelled = &mut cancelled => match cancelled {
                            Ok(()) => GateWait::Cancelled,
                            Err(_) => GateWait::DriverDropped,
                        },
                    }
                } => cancellation,
            };
            // On either cancellation path this drops every unobserved gate
            // receiver before the completion becomes visible to the driver.
            drop(waiting);
            result
        };
        let value = match gate_wait {
            GateWait::Completed(gate_result) => match (value, gate_result) {
                (value, Ok(())) => value,
                (Ok(_), Err(error)) => Err(anyhow!("wake-entry gate: {error}")),
                (Err(primary), Err(error)) => {
                    tracing::error!(
                        event = "wake_entry_gate_failed_after_handler_error",
                        handler_error = %primary,
                        gate_error = %error,
                    );
                    Err(primary.context(format!("wake-entry gate also failed: {error}")))
                }
            },
            GateWait::CallerClosed => {
                let failure = value
                    .as_ref()
                    .err()
                    .map(|error| crate::telemetry::cap_error(format!("{error:#}")));
                drop(value);
                let _ = completed.send(GatedReplyCompletion::CallerClosed { failure });
                return;
            }
            // The driver observed a request cancellation after the handler
            // fixed its reply. Do not enter JavaScript again: the handler is
            // already over. A successful answer becomes a disconnect error,
            // while an existing handler error remains the primary failure.
            GateWait::Cancelled => match value {
                Ok(_) => Err(anyhow!("The client has disconnected")),
                Err(primary) => Err(primary),
            },
            // Dropping the driver drops both halves of its completion handle.
            // Release every detached resource, but do not let an abandoned
            // driver manufacture a reply that nothing owns any longer.
            GateWait::DriverDropped => {
                drop(value);
                return;
            }
        };
        let failure = value
            .as_ref()
            .err()
            .map(|error| crate::telemetry::cap_error(format!("{error:#}")));
        let completion = match reply.send(value) {
            Ok(()) => GatedReplyCompletion::Sent { failure },
            Err(value) => {
                drop(value);
                GatedReplyCompletion::CallerClosed { failure }
            }
        };
        let _ = completed.send(completion);
    })
    .detach();
    Some(GatedReplyRx {
        completion,
        cancel: Some(cancel),
    })
}

pub(crate) enum GatedReplyCompletion {
    /// The final reply value is now visible to the receiver.
    Sent { failure: Option<String> },
    /// The receiver disappeared, so no reply remains to deliver.
    CallerClosed { failure: Option<String> },
}

pub(crate) struct GatedReplyRx {
    completion: tokio::sync::oneshot::Receiver<GatedReplyCompletion>,
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
}

impl GatedReplyRx {
    /// Stop waiting on event gates after a request cancellation. The detached
    /// task owns the reply value and gate receivers, so it performs their
    /// ordered cleanup and reports completion back to the driver.
    pub(crate) fn cancel(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

impl std::future::Future for GatedReplyRx {
    type Output = Result<GatedReplyCompletion, tokio::sync::oneshot::error::RecvError>;

    fn poll(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.get_mut().completion).poll(context)
    }
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub async fn await_arm_gates_for_test(
    gates: Vec<tokio::sync::oneshot::Receiver<Result<(), String>>>,
) -> Result<(), String> {
    await_arm_gates(gates).await
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub enum GatedReplyCompletionForTest {
    Sent { failure: Option<String> },
    CallerClosed { failure: Option<String> },
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub struct GatedReplyForTest(GatedReplyRx);

#[cfg(celld_internal_tests)]
impl std::future::Future for GatedReplyForTest {
    type Output = Result<GatedReplyCompletionForTest, tokio::sync::oneshot::error::RecvError>;

    fn poll(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(context).map(|result| {
            result.map(|completion| match completion {
                GatedReplyCompletion::Sent { failure } => {
                    GatedReplyCompletionForTest::Sent { failure }
                }
                GatedReplyCompletion::CallerClosed { failure } => {
                    GatedReplyCompletionForTest::CallerClosed { failure }
                }
            })
        })
    }
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn send_answer_after_arm_gates_for_test<T: Send + 'static>(
    reply: tokio::sync::oneshot::Sender<Result<T>>,
    value: Result<T>,
    gates: Vec<tokio::sync::oneshot::Receiver<Result<(), String>>>,
) -> Option<GatedReplyForTest> {
    send_answer_after_arm_gates(reply, value, gates).map(GatedReplyForTest)
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn drop_next_gated_reply_task_for_test() {
    asyncrt::services()
        .wake_entry()
        .drop_next_gated_reply_task
        .store(true, Ordering::Release);
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
    gated_reply: Option<GatedReplyRx>,
    /// `waitUntil` work still running after the response was sent. The
    /// request stays in flight until it settles, as the single-request loop
    /// keeps driving it.
    background: Option<v8::Global<v8::Promise>>,
    /// A successfully completed Durable Object event keeps every referenced
    /// native operation alive. Workerd does not require `waitUntil` for this
    /// I/O, so dropping these operations loses detached timers and subrequests.
    completed_cell_event: bool,
    /// Ops this request is waiting on, so a completion can be attributed to
    /// the request whose context must be current while its continuation runs.
    ops: std::collections::HashSet<u64>,
    /// The subset of `ops` whose resource owns this event's `IoContext`.
    /// These operations continue after the handler and `waitUntil` settle.
    io_context_ops: std::collections::HashSet<u64>,
    /// The subset of `ops` that can run while referenced work is live but
    /// cannot keep this event alive by themselves.
    unrefed_ops: std::collections::HashSet<u64>,
    /// The alarm bookkeeping this entry still owes, if it is one.
    alarm: Option<AlarmClaim>,
    started: Instant,
    /// The propagation context for this entry. A rejected foreign context
    /// stays here with `sampled` clear, so continuations can forward it
    /// without treating it as permission to record spans or logs.
    trace: Option<crate::telemetry::TraceContext>,
    /// Why the event failed, captured for the span's `error` so a query sees
    /// the reason, not only that it failed. This also records an output-gate
    /// failure after a successful handler.
    failure: Option<String>,
    /// The report owed to the loader isolate after this invocation retires.
    tail_report: Option<TailReportState>,
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
            self.context.seal_wait_until();
            if self.trace.is_some_and(|trace| trace.sampled) {
                self.failure = Some(crate::telemetry::cap_error(error.to_string()));
            }
            self.gated_reply =
                reply.fail_with_arm_gates(anyhow!(error), self.context.take_arm_gates());
            return;
        }
        self.completed_cell_event = self.scope.is_some();
        let (background, gated_reply) = match reply {
            Answer::Fetch(reply) => {
                // A value the decoder refuses is a failure in the turn like a
                // throw is, so it carries the same positions.
                let read = self.gate_positions().and_then(|positions| {
                    let (write_position, observed_position) = positions;
                    read_response(tc, value)
                        .map(|mut response| {
                            response.write_position = write_position;
                            response.observed_position = observed_position;
                            response
                        })
                        .map_err(|error| fail_in_turn_error(error, positions))
                });
                if let Some(report) = &mut self.tail_report {
                    match &read {
                        Ok(response) => report.response_status = Some(response.status),
                        Err(error) => report.record_failure(format!("{error:#}")),
                    }
                }
                send_and_end(tc, &self.context, reply, read)
            }
            Answer::Rpc(reply) => {
                let read =
                    view_bytes(value).ok_or_else(|| anyhow!("entrypoint RPC answered non-bytes"));
                send_and_end(tc, &self.context, reply, read)
            }
            Answer::Queue(reply) => {
                let read = read_queue_result(tc, value);
                if let Ok(result) = &read {
                    if result.outcome == QueueOutcome::Exception
                        && self.trace.is_some_and(|trace| trace.sampled)
                    {
                        self.failure = Some(crate::telemetry::cap_error(
                            result
                                .error
                                .clone()
                                .unwrap_or_else(|| "queue handler rejected".to_string()),
                        ));
                    }
                }
                send_and_end(tc, &self.context, reply, read)
            }
            Answer::CellRpc(reply) => {
                let outcome = self
                    .gate_positions()
                    .map(|(write_position, observed_position)| {
                        #[cfg(celld_internal_tests)]
                        if self.scope.as_deref().is_some_and(|scope| {
                            scope
                                .split_once(':')
                                .is_some_and(|(class, _)| class == crate::deploy::QUEUE_CLASS)
                        }) {
                            QUEUE_PRODUCER_WRITE_POSITIONS
                                .with(|positions| positions.borrow_mut().push(write_position));
                        }
                        RpcOutcome {
                            data: rpc_data_ret(tc, value),
                            write_position,
                            observed_position,
                        }
                    });
                send_and_end(tc, &self.context, reply, outcome)
            }
            Answer::WsMessage(reply) => {
                let dispatch = self
                    .gate_positions()
                    .map(|(write_position, observed_position)| WsDispatch {
                        frames: ws_capture_take(),
                        write_position,
                        observed_position,
                    });
                send_and_end(tc, &self.context, reply, dispatch)
            }
            Answer::Ack(reply) => send_and_end(tc, &self.context, reply, self.write_delta()),
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
                    .map(|cell| observe_alarm(cell, storage::get_alarm(cell)))
                    .unwrap_or_else(|| celld_logic::wake::AlarmSnapshot::without_wake(None));
                send_and_end(
                    tc,
                    &self.context,
                    reply,
                    self.write_delta().map(|write| (alarm, write)),
                )
            }
        };
        self.background = background;
        self.gated_reply = gated_reply;
    }

    /// The write position to gate this answer on: `None` unless the handler
    /// advanced the cell's committed writes past where they were.
    fn write_delta(&self) -> Result<Option<u64>> {
        self.gate_positions().map(|(write, _)| write)
    }

    /// The positions this answer's ticket carries: the write position when
    /// the handler advanced the cell's committed writes past where they were,
    /// and the position the answer observed above the cell's published
    /// baseline. One sample serves both, so they cannot disagree about what
    /// the cell holds.
    fn gate_positions(&self) -> Result<(Option<u64>, Option<u64>)> {
        match self.scope.as_deref() {
            Some(scope) => gate_positions(scope, self.writes_before),
            None => Ok((None, None)),
        }
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
    ///
    /// For the same reason the error leaves here without the position the
    /// handler's commits reached: `fail_in_turn` is the half that can sample
    /// it. A write a handler made before it ran out of budget is therefore
    /// still an unproven commit with no barrier of its own.
    fn fail(&mut self, error: anyhow::Error) {
        if let Some(report) = &mut self.tail_report {
            report.record_failure(format!("{error:#}"));
        }
        if let Some(reply) = self.reply.take() {
            if matches!(&reply, Answer::WsMessage(_)) {
                // A failed message has no successful dispatch whose barrier
                // can authorize its output. Discard the still-active capture
                // here so rejection, cancellation, and turn failures cannot
                // leave frames for the turn-end flush to publish.
                ws_capture_discard(&self.context);
            }
            if self.trace.is_some_and(|trace| trace.sampled) {
                self.failure = Some(crate::telemetry::cap_error(error.to_string()));
            }
            self.gated_reply = reply.fail_with_arm_gates(error, self.context.take_arm_gates());
        }
    }

    /// Fail the event from inside its isolate turn, where the cell's storage
    /// is at hand. The error carries the positions the turn sampled, so the
    /// shell gates it as it gates a success. A commit a handler made before it
    /// threw is as unproven as one it answered with, and without a ticket it
    /// opened no barrier, so a read-only request that followed revealed it
    /// while a crash could still lose it. A handler that only read carries the
    /// observed position for the same reason: the message it throws with can
    /// quote what it read, and the error answer reveals that state as a
    /// response body does. An alarm that rejected settled its claim before
    /// this, so the delta covers its retry record too; one that failed any
    /// other way still owes that record, and `turn_finish_alarm` samples again
    /// after writing it.
    fn fail_in_turn(&mut self, error: anyhow::Error) {
        let error = match self.gate_positions() {
            Ok(positions) => fail_in_turn_error(error, positions),
            Err(sample) => sample,
        };
        self.fail(error);
    }

    /// Fail the event of a client that has hung up. The write half of
    /// `fail_in_turn` applies — a commit the handler made still needs its
    /// barrier — but the read-only half does not: there is no client left to
    /// tell, so no message carries what the handler read, and a read-only
    /// ticket would only hold the request's pin, and a shutdown, for a
    /// durability round trip that proves nothing.
    fn fail_cancelled(&mut self, error: anyhow::Error) {
        let error = match self.write_delta() {
            Ok(write_position) => fail_in_turn_error(error, (write_position, None)),
            Err(sample) => sample,
        };
        self.fail(error);
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

    /// Done when the response is sent and no referenced work remains.
    pub fn finished(&self) -> bool {
        self.retired() && !self.has_refed_ops()
    }

    /// Whether an operation can keep this event alive.
    pub(crate) fn has_refed_ops(&self) -> bool {
        self.ops != self.unrefed_ops
    }

    /// Borrow the reply gate and operation mailbox together. The wake loop
    /// must listen to both while the detached gate owns the response, and a
    /// split borrow avoids cloning the context on every operation pass.
    pub(crate) fn gated_reply_and_io_context(
        &mut self,
    ) -> (Option<&mut GatedReplyRx>, &Arc<IoContext>) {
        (self.gated_reply.as_mut(), &self.context)
    }

    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub fn has_gated_reply_for_test(&self) -> bool {
        self.gated_reply.is_some()
    }

    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub async fn finish_gated_reply_for_test(&mut self) -> bool {
        let Some(gated_reply) = self.gated_reply.as_mut() else {
            return false;
        };
        let completion = gated_reply.await;
        self.finish_gated_reply(completion);
        true
    }

    /// Ask the detached gate owner to release every event gate and finish the
    /// reply edge. The driver waits for its completion before it retires the
    /// event, so response resources cannot outlive that cleanup.
    pub(crate) fn cancel_gated_reply(&mut self) {
        if let Some(gated_reply) = &mut self.gated_reply {
            gated_reply.cancel();
        }
    }

    /// Retire a gate waiter after it sent the reply or observed caller-close.
    ///
    /// This method also closes request sockets when the completion makes the
    /// entry retire. `finish_turn` cannot do that earlier because the reply
    /// value can still own response resources while the gate is pending.
    pub(crate) fn finish_gated_reply(
        &mut self,
        completion: Result<GatedReplyCompletion, tokio::sync::oneshot::error::RecvError>,
    ) {
        self.gated_reply = None;
        if self.trace.is_some_and(|trace| trace.sampled) {
            match completion {
                Ok(GatedReplyCompletion::Sent {
                    failure: Some(failure),
                })
                | Ok(GatedReplyCompletion::CallerClosed {
                    failure: Some(failure),
                }) => self.failure = Some(failure),
                Ok(GatedReplyCompletion::Sent { failure: None }) => {}
                Ok(GatedReplyCompletion::CallerClosed { failure: None }) => {
                    if self.failure.is_none() {
                        self.failure = Some("The client has disconnected".to_string());
                    }
                }
                Err(_) => {
                    if self.failure.is_none() {
                        self.failure = Some("wake-entry reply gate task dropped".to_string());
                    }
                }
            }
        }
        if self.retired() {
            self.context.close_sockets();
        }
    }

    /// The request has answered and its `waitUntil` work has settled.
    ///
    /// An isolate-polled WebSocket can still own an op after this point, and
    /// that op must not defer retirement. Retiring closes the sockets the
    /// request opened, and the pump of an outbound socket ends only with a
    /// close, so a retirement that waited for the pump waited for itself and
    /// the request never left the drive loop. The op keeps the entry's ops
    /// alive instead, through `keeps_native_ops`, which is what a socket the
    /// response took over needs: that socket left the request's set at the
    /// handoff, so retiring does not close it, and its pump runs until the
    /// client closes it.
    fn retired(&self) -> bool {
        self.reply.is_none()
            && self.gated_reply.is_none()
            && self.background.is_none()
            // A block that holds the gate is still this event's work:
            // retiring would close the sockets its callback may still read.
            && !self.holds_gate()
            // A reaction from this event can run its block during another
            // event's turn. Retain this event until that block releases its
            // claim, or retirement closes resources the callback still uses.
            && self.context.retire_without_cross_entry_gate()
    }

    /// Why the event failed, when a sampled trace captured it.
    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    pub(crate) fn finish_tail_report(&mut self) {
        if let Some(report) = self.tail_report.take() {
            report.finish(&self.context);
        }
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
        self.context.seal_wait_until();
    }

    /// Whether a nested `WorkerEntrypoint` call is still pending. When no
    /// native operation can resume JavaScript, the call itself must reject;
    /// failing the enclosing event would prevent its caller from catching the
    /// Workerd cancellation.
    pub(crate) fn has_pending_events(&self) -> bool {
        self.context.has_pending_events()
    }

    /// Has the client been answered? `waitUntil` work can still be running.
    pub fn answered(&self) -> bool {
        self.reply.is_none() && self.gated_reply.is_none()
    }

    /// Whether a native operation can still resume JavaScript for this event.
    ///
    /// A detached reply gate is host work. It cannot keep handler operations
    /// alive after the handler ends. A successfully completed Durable Object
    /// event keeps its pending I/O without `waitUntil`, as Workerd does. An
    /// operation that owns the event's `IoContext` and a block that holds or
    /// is queued to take a cell's input gate can also continue. The block
    /// belongs to the object, not to the client: it runs to completion as
    /// workerd's critical section does, and its `finally` opens the gate.
    /// Dropping its ops with the reply dropped the timer or subrequest it
    /// awaited, and the gate stayed shut for every later event (#733).
    pub(crate) fn keeps_native_ops(&self) -> bool {
        self.reply.is_some()
            || self.completed_cell_event
            || !self.io_context_ops.is_empty()
            || self.keeps_native_ops_after_disconnect()
    }

    /// Whether work that survives a normal client disconnect still needs
    /// this event's native operations.
    ///
    /// An origin claim covers operations started before a cross-entry block.
    /// Its callback can already hold their promises even though another
    /// event owns the block's turn and therefore owns the gate engagement.
    fn keeps_native_ops_after_disconnect(&self) -> bool {
        self.background.is_some()
            || self.engages_gate()
            || self.context.has_cross_entry_gate_claim()
    }

    /// Complete the resource-retirement edge that a claim release woke.
    pub(crate) fn finish_cross_entry_gates(&self) {
        if self.retired() {
            self.context.close_sockets();
        }
    }

    /// Whether this event holds a cell's input gate now, or is queued to
    /// take one.
    fn engages_gate(&self) -> bool {
        gate_engagement(self.scope.as_deref(), &self.context) != GateEngagement::None
    }

    /// Whether this event holds a cell's input gate now. A queued block has
    /// not started, so only a held one keeps the event's sockets: a stale
    /// queue count from a future that was never adopted must not keep the
    /// event from retiring.
    fn holds_gate(&self) -> bool {
        gate_engagement(self.scope.as_deref(), &self.context) == GateEngagement::Holds
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
        self.context.seal_wait_until();
        self.io_context_ops.clear();
        self.unrefed_ops.clear();
        // An op a foreign turn handed this event after its driver's last pass
        // never reached `ops`, so the drain below cannot reach its resolver.
        // Closing the mailbox both yields those ops and refuses every later
        // hand-off, which is what bounds this to one place instead of a race
        // the driver would have to keep re-checking on its way out.
        let handed = self.context.close_handed_ops();
        if self.ops.is_empty() && handed.is_empty() {
            return;
        }
        let mut promises = self.runtime_state.promises.lock().unwrap();
        for id in self.ops.drain() {
            promises.remove(&id);
        }
        for (id, _, _) in handed {
            promises.remove(&id);
        }
    }

    /// The context whose mailbox carries the ops other turns enqueued for
    /// this event.
    ///
    /// Borrowed, not cloned. The driver reads this once per pass of a loop
    /// that turns for every operation the event completes, and it holds the
    /// entry alive for longer than it holds this.
    pub(crate) fn io_context(&self) -> &Arc<IoContext> {
        &self.context
    }

    /// Give an op to the event whose continuation enqueued it.
    ///
    /// The owner's driver polls it, so the owner's retirement decides when it
    /// is cancelled and this event's retirement cannot take it away. An owner
    /// that is already gone can never run the continuation again, so its
    /// resolver goes with the op, exactly as `abandon` drops this event's own.
    fn hand_op(
        &mut self,
        owner: u64,
        id: u64,
        future: asyncrt::OpFuture,
        lifetime: asyncrt::OpLifetime,
    ) {
        // One branch for both refusals — an owner that is already gone, and
        // one whose mailbox closed while this turn ran. Splitting them let the
        // second forget to drop the resolver, which is the whole defect.
        let delivered = self
            .runtime_state
            .io_context(owner)
            .is_some_and(|context| context.hand_op(id, future, lifetime));
        if !delivered {
            self.runtime_state.promises.lock().unwrap().remove(&id);
        }
    }

    /// Take the ops another turn enqueued for this event, and record them as
    /// this event's own so that `deliver` and `abandon` treat them alike.
    pub(crate) fn take_handed_ops(&mut self) -> Vec<Op> {
        // The common answer is "none", and reaching it without the queue's
        // lock is why the count exists.
        if !self.context.has_handed_ops() {
            return Vec::new();
        }
        let handed = self.context.take_handed_ops();
        let mut ops = Vec::with_capacity(handed.len());
        for (id, future, lifetime) in handed {
            self.ops.insert(id);
            if lifetime == asyncrt::OpLifetime::IoContext {
                self.io_context_ops.insert(id);
            }
            if lifetime == asyncrt::OpLifetime::Unrefed {
                self.unrefed_ops.insert(id);
            }
            ops.push((id, future));
        }
        ops
    }

    fn retire_background_for_shutdown(&mut self) {
        self.background = None;
        self.context.seal_wait_until();
        self.context.close_sockets();
    }
}

/// The CPED slot carries three riders in one immutable record: the
/// harness's async-context frame (`__als_get`/`__als_set` — ALS and
/// request-context confinement), telemetry's trace context, and the native
/// `IoContext` token. Each write builds a fresh three-element array preserving
/// the other riders, so V8's per-reaction snapshots restore them atomically.
/// Telemetry once took the whole slot and exposed this collision.
///
/// When all three riders are absent, the slot stays undefined and V8 can use
/// its empty-state fast path. Stateless code can retain that path. A cell
/// event intentionally installs the native token even when telemetry is off.
///
/// This reads one rider and leaves the other two unmaterialized. Reading a
/// single rider is what every caller wants, so the layout rule lives here
/// once and each accessor names its own index. Returning all three instead
/// made a caller that wanted one rider pay three `get_index` calls, and a
/// caller that wanted two pay six across two reads of the slot. Op
/// attribution wants only the token and runs for every operation a
/// JavaScript call enqueues, so that difference sits on a hot path.
fn cped_rider<'s>(scope: &mut v8::PinScope<'s, '_>, index: u32) -> v8::Local<'s, v8::Value> {
    let data = scope.get_continuation_preserved_embedder_data();
    let record = match v8::Local::<v8::Array>::try_from(data) {
        // Length 2 is the former layout. Its snapshots can still be live
        // while an isolate upgrades across this code boundary, and they
        // carry no token, so the token rider reads undefined from them.
        Ok(record) if record.length() == 3 || record.length() == 2 => record,
        // Any non-record value is a bare frame from before this scheme, or
        // the empty slot. Only the frame rider can read anything from it,
        // and that path returns without asking V8 for anything further.
        _ => {
            return if index == 0 {
                data
            } else {
                v8::undefined(scope).into()
            };
        }
    };
    if index >= record.length() {
        return v8::undefined(scope).into();
    }
    let undefined = v8::undefined(scope).into();
    record.get_index(scope, index).unwrap_or(undefined)
}

fn cped_frame<'s>(scope: &mut v8::PinScope<'s, '_>) -> v8::Local<'s, v8::Value> {
    cped_rider(scope, 0)
}

fn cped_trace<'s>(scope: &mut v8::PinScope<'s, '_>) -> v8::Local<'s, v8::Value> {
    cped_rider(scope, 1)
}

fn cped_io_context<'s>(scope: &mut v8::PinScope<'s, '_>) -> v8::Local<'s, v8::Value> {
    cped_rider(scope, 2)
}

fn set_cped(
    scope: &mut v8::PinScope,
    frame: v8::Local<v8::Value>,
    trace: v8::Local<v8::Value>,
    io_context: v8::Local<v8::Value>,
) {
    if frame.is_undefined() && trace.is_undefined() && io_context.is_undefined() {
        let undefined = v8::undefined(scope).into();
        scope.set_continuation_preserved_embedder_data(undefined);
        return;
    }
    let record = v8::Array::new(scope, 3);
    record.set_index(scope, 0, frame);
    record.set_index(scope, 1, trace);
    record.set_index(scope, 2, io_context);
    scope.set_continuation_preserved_embedder_data(record.into());
}

/// Install a trace context into the isolate's CPED slot for one turn.
///
/// V8 snapshots continuation-preserved embedder data when a promise
/// reaction is registered and restores it while the reaction runs. That
/// is the exactness `console.log` correlation needs: a continuation
/// belonging to a *different* entry that runs during this turn's
/// microtask checkpoint carries its own context, not this turn's. The
/// layout is 16 trace-id bytes, 8 span-id bytes, and one sampling byte
/// in one ArrayBuffer. Sampled contexts pay the existing cost, and only a
/// rejected foreign context adds this cost to an unsampled request.
///
/// Returns the previous slot value *only when a trace was installed*; the
/// caller restores it before releasing the isolate. An untraced turn does not
/// install the trace rider. A cell event separately installs its `IoContext`
/// token, so V8 can attribute a continuation that runs during another event's
/// checkpoint.
fn install_trace<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    trace: Option<&crate::telemetry::TraceContext>,
) -> Option<v8::Local<'s, v8::Value>> {
    let context = trace?;
    let previous = scope.get_continuation_preserved_embedder_data();
    let buffer = v8::ArrayBuffer::new(scope, 25);
    let data = buffer.get_backing_store().data()?;
    let bytes = data.as_ptr() as *mut u8;
    // SAFETY: a freshly created 25-byte buffer, written before any JS
    // can see it.
    unsafe {
        std::ptr::copy_nonoverlapping(context.trace_id.as_ptr(), bytes, 16);
        std::ptr::copy_nonoverlapping(context.span_id.as_ptr(), bytes.add(16), 8);
        bytes.add(24).write(u8::from(context.sampled));
    }
    let frame = cped_frame(scope);
    let io_context = cped_io_context(scope);
    set_cped(scope, frame, buffer.into(), io_context);
    Some(previous)
}

fn restore_trace(scope: &mut v8::PinScope, previous: Option<v8::Local<v8::Value>>) {
    if let Some(previous) = previous {
        scope.set_continuation_preserved_embedder_data(previous);
    }
}

/// Install the current cell event's native context in CPED. V8 restores this
/// token for each reaction, including a reaction that runs during another
/// event's microtask checkpoint.
fn install_io_context<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    context: &IoContext,
) -> Option<v8::Local<'s, v8::Value>> {
    let (id, _) = context.continuation.as_ref()?;
    let previous = scope.get_continuation_preserved_embedder_data();
    let frame = cped_frame(scope);
    let trace = cped_trace(scope);
    let token: v8::Local<v8::Value> = v8::BigInt::new_from_u64(scope, *id).into();
    set_cped(scope, frame, trace, token);
    Some(previous)
}

fn restore_io_context(scope: &mut v8::PinScope, previous: Option<v8::Local<v8::Value>>) {
    if let Some(previous) = previous {
        scope.set_continuation_preserved_embedder_data(previous);
    }
}

fn current_reaction_io_context(scope: &mut v8::PinScope) -> Option<Arc<IoContext>> {
    let id = reaction_continuation_id(scope)?;
    actor_runtime_state(scope).io_context(id)
}

/// The `IoContext` id of the code running at this exact point, read from CPED.
///
/// It is the running turn's id, or the id of a continuation V8 restored — the
/// resource identity of the code, even when a critical section has a separate
/// driver for its native operations.
fn reaction_continuation_id(scope: &mut v8::PinScope) -> Option<u64> {
    continuation_context_id(scope, 0)
}

/// A plain token names both the resource context and the operation driver.
/// A critical section carries [origin, driver] instead: its driver owns the
/// gate and must also poll its operations, while storage and resource access
/// must still resolve to the origin. Keeping both in the same CPED rider
/// preserves this relationship across foreign microtask checkpoints and ALS
/// frame changes without charging ordinary continuations for another rider.
fn continuation_context_id(scope: &mut v8::PinScope, index: u32) -> Option<u64> {
    let token = cped_io_context(scope);
    let token = if let Ok(pair) = v8::Local::<v8::Array>::try_from(token) {
        pair.get_index(scope, index)?
    } else {
        token
    };
    let token = v8::Local::<v8::BigInt>::try_from(token).ok()?;
    let (id, lossless) = token.u64_value();
    lossless.then_some(id)
}

fn operation_continuation_id(scope: &mut v8::PinScope) -> Option<u64> {
    continuation_context_id(scope, 1)
}

/// Enter a critical section with its gate owner and operation driver bound
/// together. Only the callback's continuations inherit the pair; restoring
/// the previous slot leaves the caller's subsequent work with its own event.
fn op_with_input_gate_context(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(callback) = v8::Local::<v8::Function>::try_from(args.get(0)) else {
        return;
    };
    let previous = scope.get_continuation_preserved_embedder_data();
    let token = cped_io_context(scope);
    // A nested block inherits its driver's identity, even if a third event
    // runs the checkpoint. A new block uses the entry driving this turn.
    if !token.is_array() {
        if let Some(origin) = reaction_continuation_id(scope) {
            let driver = current_context().continuation_id().unwrap_or(origin);
            let origin = v8::BigInt::new_from_u64(scope, origin);
            let driver = v8::BigInt::new_from_u64(scope, driver);
            let pair = v8::Array::new_with_elements(scope, &[origin.into(), driver.into()]);
            let frame = cped_frame(scope);
            let trace = cped_trace(scope);
            set_cped(scope, frame, trace, pair.into());
        }
    }
    let receiver = v8::undefined(scope).into();
    let result = callback.call(scope, receiver, &[]);
    scope.set_continuation_preserved_embedder_data(previous);
    if let Some(result) = result {
        rv.set(result);
    }
}

/// Resolve the exact tracked reaction, or use the active context when this
/// isolate does not track reactions at all.
///
/// Cell events always install a token. A token that no longer resolves names
/// a retired event and must not borrow another event's ambient context.
/// Stateless requests install no token, so their current turn remains the
/// only context that can own a synchronous operation such as `process.exit`.
fn current_reaction_or_untracked_io_context(scope: &mut v8::PinScope) -> Option<Arc<IoContext>> {
    if cped_io_context(scope).is_undefined() {
        return Some(current_context());
    }
    current_reaction_io_context(scope)
}

/// The trace context current at this exact point of JS execution, read
/// from CPED — the running turn's, or the running continuation's if V8
/// restored one. `None` when telemetry is off or execution is outside an
/// entry. An unsampled foreign context remains present with its flag clear.
pub(crate) fn current_trace_context(
    scope: &mut v8::PinScope,
) -> Option<crate::telemetry::TraceContext> {
    if !crate::telemetry::active() {
        return None;
    }
    let data = cped_trace(scope);
    let buffer = v8::Local::<v8::ArrayBuffer>::try_from(data).ok()?;
    if buffer.byte_length() != 25 {
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
    let sampled = unsafe { bytes.add(24).read() != 0 };
    Some(crate::telemetry::TraceContext {
        trace_id,
        span_id,
        sampled,
    })
}

/// The isolate, entered for one turn. Everything a request does to a shared
/// isolate goes through one of these, and none of them may be held across an
/// await: the caller takes the pool's async permit first, runs a turn, and
/// leaves.
/// An isolate's heap as V8 accounts for it, for `/state`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HeapBytes {
    /// Physical memory committed to the heap.
    pub physical: u64,
    /// External memory the isolate tracks: array buffer stores and the like.
    pub external: u64,
}

impl Worker {
    /// The heap V8 reports for this isolate. Takes the isolate lock, so the
    /// caller must hold the pool's permit for this worker, exactly as a turn
    /// does; `None` once the isolate has been freed.
    pub fn heap_bytes(&self) -> Option<HeapBytes> {
        let inner = self.inner.as_ref()?;
        let (mut locker, _cells) = inner.lock();
        let statistics = locker.get_heap_statistics();
        Some(HeapBytes {
            physical: statistics.total_physical_size() as u64,
            external: statistics.external_memory() as u64,
        })
    }

    /// Run a request's first turn.
    ///
    /// Returns what is now in flight — `None` when nothing is, the reply
    /// already carrying the error — and the ops the handler enqueued, which
    /// the caller awaits with no isolate held.
    pub fn turn_begin(
        &mut self,
        job: crate::WorkerJob,
        trace: Option<crate::telemetry::TraceContext>,
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

        advance_io_time(tc);
        let previous = install_trace(tc, trace.as_ref());
        let invocation_limits = match &job {
            crate::WorkerJob::Fetch {
                invocation_limits, ..
            }
            | crate::WorkerJob::Rpc {
                invocation_limits, ..
            } => *invocation_limits,
            crate::WorkerJob::Queue { .. } => None,
        };
        let initial_limits =
            effective_resource_limits(inner.runtime_state.resource_limits, invocation_limits);
        let declared_ms = initial_limits.and_then(|limits| limits.cpu_ms);
        let cpu_watchdog = inner.cpu_watchdog(
            declared_ms.map(|milliseconds| Duration::from_millis(u64::from(milliseconds))),
            declared_ms,
        );
        let begun = begin(tc, realm.fetch, job);
        drop(cpu_watchdog);
        let out = match begun {
            Begun::Running(mut entry) => {
                entry.trace = trace;
                // A handler that returned an already-resolved promise is
                // finished before it ever suspends, and must not wait for an
                // op it will never start.
                let ops = finish_turn(tc, &mut entry);
                (Some(*entry), ops)
            }
            Begun::Threw(failure) => {
                let BegunFailure {
                    answer,
                    cpu_error,
                    tail_report,
                } = *failure;
                let error = take_execution_termination(tc)
                    .or_else(|| cpu_error.map(anyhow::Error::msg))
                    .unwrap_or_else(|| anyhow!("fetch threw: {}", exc!(tc)));
                if let Some((mut report, context)) = tail_report {
                    report.record_failure(format!("{error:#}"));
                    report.finish(&context);
                }
                answer.fail(error);
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
        self.cancel_turn(entry, false)
    }

    /// The runtime is stopping: end the event as a client hang-up does, but
    /// keep nothing running for it, a critical section included. The block
    /// would not reach its end before the process exits, so its gate is
    /// abandoned and the events queued behind it are refused now rather
    /// than left waiting through the shutdown.
    pub fn turn_cancel_for_shutdown(&mut self, entry: &mut InFlight) -> Vec<Op> {
        self.cancel_turn(entry, true)
    }

    fn cancel_turn(&mut self, entry: &mut InFlight, shutdown: bool) -> Vec<Op> {
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

        advance_io_time(tc);
        let previous_io_context = install_io_context(tc, &entry.context);
        let previous = install_trace(tc, entry.trace.as_ref());
        entry.context.begin_cpu_turn();
        let cpu_watchdog = inner.cpu_watchdog(
            entry.context.remaining_cpu_budget(),
            entry.context.cpu_limit_ms(),
        );
        cancel(tc, entry, shutdown);
        drop(cpu_watchdog);
        let ops = finish_turn(tc, entry);
        restore_trace(tc, previous);
        restore_io_context(tc, previous_io_context);
        ops
    }

    /// Retire input-gate work for an event that cannot run JavaScript again.
    ///
    /// The host claims are taken before the isolate lock because the caller
    /// already owns this slot. A newly woken event can queue for the slot, but
    /// it cannot enter the isolate until the stale actor state is removed.
    pub(crate) fn turn_retire_input_gates(&mut self, entry: &InFlight) {
        entry.context.force_retire_cross_entry_gates();
        let _ = abandon_context_input_gates(&entry.context);
        self.turn_retire_input_gate_context(entry);
    }

    /// Enforce the host-side invariant when an ordinarily completed event
    /// drops its final owner. The JavaScript `finally` normally released every
    /// gate, so entering the isolate is necessary only when a hold remains.
    pub(crate) fn turn_abandon_input_gates(&mut self, entry: &InFlight) {
        let abandoned = abandon_context_input_gates(&entry.context);
        if abandoned.is_empty() {
            return;
        }
        self.turn_retire_input_gate_context(entry);
    }

    fn turn_retire_input_gate_context(&mut self, entry: &InFlight) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };
        let (mut locker, _cells) = inner.lock();
        v8::scope!(let hs, &mut *locker);
        let realm = inner.realm(hs);
        let context = realm.context;
        let cs = &mut v8::ContextScope::new(hs, context);
        let context_id = entry.context.continuation_id().unwrap_or_default();
        retire_input_gate_js_context(cs, context_id);
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

        advance_io_time(tc);
        let previous_io_context = install_io_context(tc, &entry.context);
        let previous = install_trace(tc, entry.trace.as_ref());
        entry.context.begin_cpu_turn();
        let cpu_watchdog = inner.cpu_watchdog(
            entry.context.remaining_cpu_budget(),
            entry.context.cpu_limit_ms(),
        );
        deliver(tc, entry, op, res);
        cancelled(tc, entry);
        drop(cpu_watchdog);
        let ops = finish_turn(tc, entry);
        restore_trace(tc, previous);
        restore_io_context(tc, previous_io_context);
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
/// 3. **release the facet gate** — the second facet flush can now cover every
///    write exposed by the checkpoint, or reject the held response.
/// 4. **settle again** — the checkpoint may itself have settled the promise,
///    or the `waitUntil` aggregate.
/// 5. **flush captured frames** — a `webSocketMessage` handler that has not
///    answered is suspended, so the frames it has sent so far leave now
///    rather than at the end of the handler.
/// 6. **adopt** — only now is the set of ops this request waits on complete.
/// 7. **close** — close the sockets the request opened, once it has retired.
///    A socket the response took over left that set at the handoff.
///
/// Draining before step 2 is the bug that made a streaming handler answer
/// with nothing outstanding and conclude it was waiting on nothing.
/// The checkpoint before adoption is what makes response-body pumps visible.
///
/// An accepted Worker socket starts its next `__ws_next` before the fetch
/// response retires. The driver used to drop that receive future with the
/// handler's other ops once the handler answered, so later frames hung. An op
/// that owns the `IoContext` therefore keeps the entry's ops alive after the
/// answer, through `keeps_native_ops`. The lifetime travels in the spawn
/// record rather than in a separate op-id tag: a tag can outlive a failed
/// turn, and the spawn and its classification drain together. Such an op
/// does not defer retirement: see `retired`.
fn finish_turn(tc: &mut v8::PinScope, entry: &mut InFlight) -> Vec<Op> {
    if let Err(error) = finish_cpu_turn(tc, entry) {
        let error = take_execution_termination_in_context(tc, Some(&entry.context))
            .unwrap_or_else(|| anyhow!(error));
        entry.fail_in_turn(error);
        entry.background = None;
        entry.abandon();
        ws_capture_flush_touched(tc, &entry.runtime_state);
        return Vec::new();
    }
    entry.context.begin_cpu_turn();
    {
        let _cpu_watchdog = cpu_watchdog_for_context(tc, &entry.context);
        settle(tc, entry);
        tc.perform_microtask_checkpoint();
    }
    if let Err(error) = finish_cpu_turn(tc, entry) {
        let error = take_execution_termination_in_context(tc, Some(&entry.context))
            .unwrap_or_else(|| anyhow!(error));
        entry.fail_in_turn(error);
        entry.background = None;
        entry.abandon();
        ws_capture_flush_touched(tc, &entry.runtime_state);
        return Vec::new();
    }
    // `abort()` and `process.exit()` terminate execution without settling
    // the handler's promise, so an entry that only watched the promise would
    // wait on it forever and then report that it was waiting on nothing.
    // The blocking loop broke out of its loop here; an entry fails here.
    if let Some(error) = take_execution_termination_in_context(tc, Some(&entry.context)) {
        entry.fail_in_turn(error);
        entry.background = None;
        entry.abandon();
        ws_capture_flush_touched(tc, &entry.runtime_state);
        return Vec::new();
    }
    settle(tc, entry);
    // 5. **release what suspended WebSocket handlers have sent** — after the
    //    settles above, so a handler that answered in this turn keeps the
    //    whole-batch path its answer carries. The set comes from each capture
    //    write rather than the turn owner, because the checkpoint can run a
    //    continuation from another event.
    ws_capture_flush_touched(tc, &entry.runtime_state);
    let ops = adopt(entry);
    // 7. **close the request's sockets** — see above.
    if entry.retired() {
        entry.context.close_sockets();
    }
    ops
}

/// An op the JS enqueued, and the id whose promise it resolves.
pub type Op = (u64, asyncrt::OpFuture);

/// Take the ops this turn enqueued that belong to this event, and hand every
/// other event its own.
///
/// Drained after `settle`, not before: ending an event runs JS, and anything
/// that starts there belongs to this request too. The pump drained first and
/// so could attribute those to whichever entry it settled next.
///
/// A turn is not proof of ownership. V8 runs a *foreign* continuation during
/// this turn's microtask checkpoint — a cell event resumed by the promise this
/// event just settled — and an op that continuation enqueues arrives here
/// exactly like this event's own. Adopting it made the wrong driver poll it,
/// and retiring then cancelled it together with the resolver, so the event
/// really waiting never settled and every later event queued behind it. The op
/// layer names the enqueueing continuation,
/// so an op with a foreign name goes to that event's driver instead. Critical
/// sections carry an explicit driver alongside their resource context; the
/// op wrapper uses that driver so gate retirement and op cancellation agree.
fn adopt(entry: &mut InFlight) -> Vec<Op> {
    let spawns = asyncrt::drain_spawns();
    let mine = entry.context.continuation_id();
    let mut ops = Vec::with_capacity(spawns.len());
    for (id, future, lifetime, owner) in spawns {
        if let Some(owner) = owner.filter(|owner| Some(*owner) != mine) {
            entry.hand_op(owner, id, future, lifetime);
            continue;
        }
        entry.ops.insert(id);
        if lifetime == asyncrt::OpLifetime::IoContext {
            entry.io_context_ops.insert(id);
        }
        if lifetime == asyncrt::OpLifetime::Unrefed {
            entry.unrefed_ops.insert(id);
        }
        ops.push((id, future));
    }
    ops
}

/// What starting a request produced.
enum Begun {
    /// In flight. Its ops must be adopted and its promise driven.
    Running(Box<InFlight>),
    /// The handler threw before it could suspend, so nothing is in flight.
    /// The exception belongs to the caller's `TryCatch` — an unnameable type
    /// no signature here can take — so the caller reads it and answers.
    Threw(Box<BegunFailure>),
    /// Nothing started: the job was not a fetch, or the reply already
    /// carries the error.
    Nothing,
}

struct BegunFailure {
    answer: Answer,
    cpu_error: Option<String>,
    tail_report: Option<(TailReportState, Arc<IoContext>)>,
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
            operation,
            props,
            invocation_limits,
            reply,
        } => {
            return begin_entrypoint_rpc(
                tc,
                &entrypoint,
                operation,
                props,
                invocation_limits,
                reply,
            );
        }
        crate::WorkerJob::Queue { batch, reply, .. } => {
            return begin_queue(tc, batch, reply);
        }
        job => job,
    };
    let crate::WorkerJob::Fetch {
        entrypoint,
        invocation_limits,
        url,
        method,
        body,
        headers,
        request_id,
        tail_report,
        reply,
        ..
    } = job
    else {
        return Begun::Nothing;
    };
    let runtime_state = actor_runtime_state(tc);
    let tail_reporting = tail_report.is_some();
    debug_assert!(
        !tail_reporting || runtime_state.tail_reporting,
        "a tail report reached a Worker that was loaded without tails",
    );
    // The report sender is the invocation-level authority. The isolate-level
    // option also applies to RPC and cell events, but those events have no
    // report to consume retained logs, so enabling capture for them only
    // holds records until their IoContext retires.
    let context = IoContext::with_options(
        effective_resource_limits(runtime_state.resource_limits, invocation_limits),
        tail_reporting,
    );
    if let Some(stream_id) = body.stream_id() {
        context.own_body_stream(stream_id);
    }
    let guard = CurrentGuard::enter(context.clone());
    context.begin_cpu_turn();
    let target = match entrypoint {
        Some(entrypoint) => FetchTarget::Entrypoint(entrypoint),
        None => FetchTarget::Default(fetch),
    };
    let started = start_fetch(tc, target, &url, &method, body, &headers, request_id);
    let tail_headers = tail_request_headers(&headers);
    let mut tail_report = tail_report.map(|reply| TailReportState {
        reply,
        script_name: runtime_state.script_name.clone(),
        event_timestamp: unix_now_ms(),
        url,
        method,
        headers: tail_headers,
        response_status: None,
        failure: None,
    });
    let promise = match started {
        Ok(Started::Running(ret, active)) => match ret.try_cast::<v8::Promise>() {
            Ok(promise) => Ok((promise, active)),
            Err(_) => resolved_promise(tc, ret).map(|promise| (promise, active)),
        },
        Ok(Started::Threw) => {
            let cpu_error = context.finish_cpu_turn().err();
            drop(guard);
            return Begun::Threw(Box::new(BegunFailure {
                answer: Answer::Fetch(reply),
                cpu_error,
                tail_report: tail_report.take().map(|report| (report, context)),
            }));
        }
        Err(error) => Err(error),
    };
    match promise {
        Ok((promise, active_request_id)) => {
            tc.perform_microtask_checkpoint();
            let entry = InFlight {
                runtime_state,
                promise: v8::Global::new(tc, promise),
                context,
                scope: None,
                writes_before: None,
                request_id,
                active_request_id,
                reply: Some(Answer::Fetch(reply)),
                gated_reply: None,
                background: None,
                completed_cell_event: false,
                ops: std::collections::HashSet::new(),
                io_context_ops: std::collections::HashSet::new(),
                unrefed_ops: std::collections::HashSet::new(),
                alarm: None,
                started: Instant::now(),
                trace: None,
                failure: None,
                tail_report,
            };
            drop(guard);
            Begun::Running(Box::new(entry))
        }
        Err(error) => {
            let cpu_error = context.finish_cpu_turn().err().map(anyhow::Error::msg);
            let error = take_execution_termination(tc)
                .or(cpu_error)
                .unwrap_or(error);
            if let Some(mut report) = tail_report.take() {
                report.record_failure(format!("{error:#}"));
                report.finish(&context);
            }
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
    operation: crate::WorkerRpcOperation,
    props: Vec<u8>,
    invocation_limits: Option<ResourceLimits>,
    reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
) -> Begun {
    let context = IoContext::with_resource_limits(effective_resource_limits(
        actor_runtime_state(tc).resource_limits,
        invocation_limits,
    ));
    let guard = CurrentGuard::enter(context.clone());
    context.begin_cpu_turn();
    let started = (|| {
        let f = internal_function(tc, "__dispatchEntrypointRpc")?;
        let entrypoint = v8::String::new(tc, entrypoint).unwrap();
        let (path, args) = match operation {
            crate::WorkerRpcOperation::Get { path } => (path, v8::null(tc).into()),
            crate::WorkerRpcOperation::Call { path, args } => (path, bytes_value(tc, args)),
        };
        let path = path
            .iter()
            .map(|part| v8::String::new(tc, part).unwrap().into())
            .collect::<Vec<v8::Local<v8::Value>>>();
        let path = v8::Array::new_with_elements(tc, &path);
        // The caller's props, structured-clone bytes like `args`. `local` stays
        // false: the call crossed an isolate boundary.
        let local = v8::Boolean::new(tc, false);
        let props = bytes_value(tc, props);
        let recv = v8::undefined(tc).into();
        begin_event_context(tc)?;
        let ret = f
            .call(
                tc,
                recv,
                &[entrypoint.into(), path.into(), args, local.into(), props],
            )
            .ok_or_else(|| anyhow!("entrypoint RPC threw"))?;
        match ret.try_cast::<v8::Promise>() {
            Ok(promise) => Ok(promise),
            Err(_) => resolved_promise(tc, ret),
        }
    })();
    let event_started = Instant::now();
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
                gated_reply: None,
                background: None,
                completed_cell_event: false,
                ops: std::collections::HashSet::new(),
                io_context_ops: std::collections::HashSet::new(),
                unrefed_ops: std::collections::HashSet::new(),
                alarm: None,
                started: event_started,
                trace: None,
                failure: None,
                tail_report: None,
            };
            drop(guard);
            Begun::Running(Box::new(entry))
        }
        Err(error) => {
            let _ = end_event_context(tc);
            let cpu_error = context.finish_cpu_turn().err().map(anyhow::Error::msg);
            let error = take_execution_termination(tc)
                .or(cpu_error)
                .unwrap_or(error);
            drop(guard);
            let _ = reply.send(Err(error));
            Begun::Nothing
        }
    }
}

fn queue_property<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    name: &str,
) -> Result<v8::Local<'s, v8::Value>> {
    let key = v8::String::new(scope, name).unwrap();
    object
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("queue result has no {name}"))
}

fn queue_bool<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    name: &str,
) -> Result<bool> {
    let value = queue_property(scope, object, name)?;
    anyhow::ensure!(value.is_boolean(), "queue result {name} is not a boolean");
    Ok(value.boolean_value(scope))
}

fn queue_delay<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
) -> Result<Option<i32>> {
    let value = queue_property(scope, object, "delaySeconds")?;
    if value.is_undefined() {
        return Ok(None);
    }
    value
        .int32_value(scope)
        .map(Some)
        .ok_or_else(|| anyhow!("queue retry delaySeconds is not a 32-bit integer"))
}

fn queue_string_array<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    name: &str,
) -> Result<Vec<String>> {
    let value = queue_property(scope, object, name)?;
    let array: v8::Local<v8::Array> = value
        .try_into()
        .map_err(|_| anyhow!("queue result {name} is not an array"))?;
    (0..array.length())
        .map(|index| {
            let value = array
                .get_index(scope, index)
                .ok_or_else(|| anyhow!("queue result {name}[{index}] is missing"))?;
            anyhow::ensure!(
                value.is_string(),
                "queue result {name}[{index}] is not a string"
            );
            Ok(value.to_rust_string_lossy(scope))
        })
        .collect()
}

fn read_queue_result<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: v8::Local<'s, v8::Value>,
) -> Result<QueueDispatchResult> {
    let object: v8::Local<v8::Object> = value
        .try_into()
        .map_err(|_| anyhow!("queue handler answered a non-object"))?;
    let outcome = queue_property(scope, object, "outcome")?;
    anyhow::ensure!(outcome.is_string(), "queue result outcome is not a string");
    let outcome = match outcome.to_rust_string_lossy(scope).as_str() {
        "ok" => QueueOutcome::Ok,
        "exception" => QueueOutcome::Exception,
        outcome => return Err(anyhow!("unknown queue result outcome {outcome}")),
    };
    let error = queue_property(scope, object, "error")?;
    let error = if error.is_undefined() {
        None
    } else {
        anyhow::ensure!(error.is_string(), "queue result error is not a string");
        Some(error.to_rust_string_lossy(scope))
    };
    let retry_batch: v8::Local<v8::Object> = queue_property(scope, object, "retryBatch")?
        .try_into()
        .map_err(|_| anyhow!("queue result retryBatch is not an object"))?;
    let retries = queue_property(scope, object, "retryMessages")?;
    let retries: v8::Local<v8::Array> = retries
        .try_into()
        .map_err(|_| anyhow!("queue result retryMessages is not an array"))?;
    let mut retry_messages = Vec::with_capacity(retries.length() as usize);
    for index in 0..retries.length() {
        let retry: v8::Local<v8::Object> = retries
            .get_index(scope, index)
            .ok_or_else(|| anyhow!("queue result retryMessages[{index}] is missing"))?
            .try_into()
            .map_err(|_| anyhow!("queue result retryMessages[{index}] is not an object"))?;
        let msg_id = queue_property(scope, retry, "msgId")?;
        anyhow::ensure!(
            msg_id.is_string(),
            "queue result retryMessages[{index}].msgId is not a string"
        );
        retry_messages.push(QueueRetryMessage {
            msg_id: msg_id.to_rust_string_lossy(scope),
            delay_seconds: queue_delay(scope, retry)?,
        });
    }
    Ok(QueueDispatchResult {
        outcome,
        error,
        ack_all: queue_bool(scope, object, "ackAll")?,
        retry_batch: QueueRetryBatch {
            retry: queue_bool(scope, retry_batch, "retry")?,
            delay_seconds: queue_delay(scope, retry_batch)?,
        },
        explicit_acks: queue_string_array(scope, object, "explicitAcks")?,
        retry_messages,
    })
}

fn set_queue_property<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    object: v8::Local<'s, v8::Object>,
    name: &str,
    value: v8::Local<'s, v8::Value>,
) -> Result<()> {
    let key = v8::String::new(scope, name).unwrap();
    anyhow::ensure!(
        object.create_data_property(scope, key.into(), value) == Some(true),
        "could not construct queue batch field {name}"
    );
    Ok(())
}

fn queue_batch_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    batch: QueueBatch,
) -> Result<v8::Local<'s, v8::Value>> {
    let QueueBatch {
        queue,
        messages,
        metrics,
    } = batch;
    let mut message_values = Vec::with_capacity(messages.len());
    for message in messages {
        let object = v8::Object::new(scope);
        let id = v8::String::new(scope, &message.id).unwrap();
        set_queue_property(scope, object, "id", id.into())?;
        let timestamp = v8::Number::new(scope, message.timestamp_ms as f64);
        set_queue_property(scope, object, "timestamp", timestamp.into())?;
        let body = bytes_value(scope, message.body);
        set_queue_property(scope, object, "body", body)?;
        let content_type = v8::String::new(scope, message.content_type.as_str()).unwrap();
        set_queue_property(scope, object, "contentType", content_type.into())?;
        let attempts = v8::Integer::new_from_unsigned(scope, u32::from(message.attempts));
        set_queue_property(scope, object, "attempts", attempts.into())?;
        message_values.push(object.into());
    }

    let metrics_value = v8::Object::new(scope);
    let count = v8::Number::new(scope, metrics.backlog_count);
    set_queue_property(scope, metrics_value, "backlogCount", count.into())?;
    let bytes = v8::Number::new(scope, metrics.backlog_bytes);
    set_queue_property(scope, metrics_value, "backlogBytes", bytes.into())?;
    let oldest = match metrics.oldest_message_timestamp_ms {
        Some(timestamp) => v8::Number::new(scope, timestamp as f64).into(),
        None => v8::undefined(scope).into(),
    };
    set_queue_property(scope, metrics_value, "oldestMessageTimestamp", oldest)?;

    let value = v8::Object::new(scope);
    let queue = v8::String::new(scope, &queue).unwrap();
    set_queue_property(scope, value, "queue", queue.into())?;
    let messages = v8::Array::new_with_elements(scope, &message_values);
    set_queue_property(scope, value, "messages", messages.into())?;
    set_queue_property(scope, value, "metrics", metrics_value.into())?;
    Ok(value.into())
}

/// Start a queue consumer's first turn. The batch body bytes move directly
/// into V8, and only the small settlement record crosses back out.
fn begin_queue(
    tc: &mut v8::PinScope,
    batch: QueueBatch,
    reply: tokio::sync::oneshot::Sender<Result<QueueDispatchResult>>,
) -> Begun {
    let context = IoContext::with_resource_limits(actor_runtime_state(tc).resource_limits);
    let guard = CurrentGuard::enter(context.clone());
    context.begin_cpu_turn();
    let started = (|| {
        let dispatch = internal_function(tc, "__dispatchEntrypointQueue")?;
        let batch = queue_batch_value(tc, batch)?;
        let entrypoint = v8::String::new(tc, "default").unwrap();
        let recv = v8::undefined(tc).into();
        begin_event_context(tc)?;
        let ret = dispatch
            .call(tc, recv, &[entrypoint.into(), batch])
            .ok_or_else(|| anyhow!("queue dispatch threw"))?;
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
                reply: Some(Answer::Queue(reply)),
                gated_reply: None,
                background: None,
                completed_cell_event: false,
                ops: std::collections::HashSet::new(),
                io_context_ops: std::collections::HashSet::new(),
                unrefed_ops: std::collections::HashSet::new(),
                alarm: None,
                started: Instant::now(),
                trace: None,
                failure: None,
                tail_report: None,
            };
            drop(guard);
            Begun::Running(Box::new(entry))
        }
        Err(error) => {
            let _ = end_event_context(tc);
            let cpu_error = context.finish_cpu_turn().err().map(anyhow::Error::msg);
            let error = take_execution_termination(tc)
                .or(cpu_error)
                .unwrap_or(error);
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
        RpcData::V8(bytes) => bytes_value(scope, bytes.into()),
    }
}

/// The inverse: `__dispatchRpc` answers in the flavor it was asked in.
fn rpc_data_ret(scope: &mut v8::PinScope, ret: v8::Local<v8::Value>) -> RpcData {
    match view_bytes(ret) {
        Some(bytes) => RpcData::V8(bytes.into()),
        None => RpcData::Json(ret.to_rust_string_lossy(scope)),
    }
}

fn unix_now_ms() -> i64 {
    #[cfg(all(test, celld_internal_tests))]
    if let Some(timestamp_ms) = unix_now_ms_for_test() {
        return timestamp_ms;
    }
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
    context: &IoContext,
    reply: tokio::sync::oneshot::Sender<Result<T>>,
    value: Result<T>,
) -> (Option<v8::Global<v8::Promise>>, Option<GatedReplyRx>)
where
    T: Send + 'static,
{
    let background = end_event_context(tc).ok().flatten();
    let gated_reply = send_answer_after_arm_gates(reply, value, context.take_arm_gates());
    (
        background.map(|promise| v8::Global::new(tc, promise)),
        gated_reply,
    )
}

/// The positions an answer's ticket carries, sampled from a cell's storage:
/// the write position when the handler advanced the cell's committed writes
/// past `writes_before`, and otherwise the position the answer observed above
/// the cell's published baseline. One sample serves both, so they cannot
/// disagree about what the cell holds, and they are returned together because
/// a site that gates on one alone releases the output the other covers.
fn gate_positions(scope: &str, writes_before: Option<u64>) -> Result<(Option<u64>, Option<u64>)> {
    let sample = storage::write_position(scope)?;
    let write = write_delta(writes_before, sample);
    let observed = if write.is_none() {
        storage::observed_position(scope, sample)
    } else {
        None
    };
    Ok((write, observed))
}

/// A committed-write position only counts when the handler advanced it; celld's
/// own activation writes are already below `before`.
pub(crate) fn write_delta(before: Option<u64>, after: Option<u64>) -> Option<u64> {
    match (before, after) {
        (Some(before), Some(after)) if after > before => Some(after),
        _ => None,
    }
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
    body_stream_id: Option<u64>,
    capture_frames: bool,
    call: impl FnOnce(&mut v8::PinScope<'s, '_>) -> Result<v8::Local<'s, v8::Value>>,
) -> Begun {
    let runtime_state = actor_runtime_state(tc);
    let context = IoContext::tracked(&runtime_state);
    if let Some(stream_id) = body_stream_id {
        context.own_body_stream(stream_id);
    }
    let guard = CurrentGuard::enter(context.clone());
    let previous_io_context = install_io_context(tc, &context);
    context.begin_cpu_turn();
    // Sampled before the handler runs so the output gate can tell a write
    // this event made from celld's own activation writes. The first sample of
    // an activation is also the baseline a read-only answer observes above.
    let writes_before = storage::event_start_position(scope);
    // No guard: this frame is the context's outermost one and the context
    // ends with the event, so there is nothing to pop it before.
    context.egress.lock().unwrap().push(EgressFrame {
        storage: scope.to_string(),
        before: writes_before.unwrap_or(0),
        // A facet event stores here and gates on the cell that owns the root
        // database. Read once, with the position, so no later effect can take
        // one without the other.
        root: storage::embedded_root_gate(scope),
    });
    if capture_frames {
        ws_capture_begin();
    }
    let event_started = Instant::now();
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
            if capture_frames {
                ws_capture_set_promise(&context, v8::Global::new(tc, promise));
            }
            let entry = InFlight {
                runtime_state: runtime_state.clone(),
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
                gated_reply: None,
                background: None,
                completed_cell_event: false,
                ops: std::collections::HashSet::new(),
                io_context_ops: std::collections::HashSet::new(),
                unrefed_ops: std::collections::HashSet::new(),
                alarm: None,
                started: event_started,
                trace: None,
                failure: None,
                tail_report: None,
            };
            restore_io_context(tc, previous_io_context);
            drop(guard);
            Begun::Running(Box::new(entry))
        }
        Err(error) => {
            // A V8 termination has a concrete stored cause. The dispatcher
            // wrapper can only report that its call returned no value, so do
            // not replace process.exit or actor-abort with that generic seam.
            let cpu_error = context.finish_cpu_turn().err().map(anyhow::Error::msg);
            let error = take_execution_termination(tc)
                .or(cpu_error)
                .unwrap_or(error);
            // The handler ran synchronously before it threw, and a
            // synchronous storage call both commits and reads, so the error
            // carries the positions it reached like a rejection does.
            let error = match gate_positions(scope, writes_before) {
                Ok(positions) => fail_in_turn_error(error, positions),
                Err(sample) => sample,
            };
            let background = end_event_context(tc)
                .ok()
                .flatten()
                .map(|promise| v8::Global::new(tc, promise));
            let failure = crate::telemetry::cap_error(format!("{error:#}"));
            if matches!(&answer, Answer::WsMessage(_)) {
                ws_capture_discard(&context);
            }
            let gated_reply = answer.fail_with_arm_gates(error, context.take_arm_gates());
            let begun = if gated_reply.is_none() && background.is_none() {
                Begun::Nothing
            } else {
                let undefined = v8::undefined(tc).into();
                let promise = resolved_promise(tc, undefined)
                    .expect("a finishing cell event can create a resolved promise");
                Begun::Running(Box::new(InFlight {
                    runtime_state,
                    promise: v8::Global::new(tc, promise),
                    context,
                    scope: Some(scope.to_string()),
                    writes_before,
                    request_id,
                    active_request_id: None,
                    reply: None,
                    gated_reply,
                    background,
                    completed_cell_event: false,
                    ops: std::collections::HashSet::new(),
                    io_context_ops: std::collections::HashSet::new(),
                    unrefed_ops: std::collections::HashSet::new(),
                    alarm: None,
                    started: event_started,
                    trace: None,
                    failure: Some(failure),
                    tail_report: None,
                }))
            };
            restore_io_context(tc, previous_io_context);
            drop(guard);
            begun
        }
    }
}

/// Start a cell event's first turn.
///
/// The counterpart of `begin` for the events a cell receives. Where the
/// blocking loop ran each of these to completion inside one call — and
/// serviced other cells' events inside *that* — each is now an entry a tokio
/// task drives, so two events of one cell interleave by suspending rather
/// than by nesting.
fn begin_cell(tc: &mut v8::PinScope, job: CellJob, event_time: i64) -> Begun {
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
            let body_stream_id = body.stream_id();
            start_cell_event(
                tc,
                &scope,
                Answer::Fetch(reply),
                request_id,
                body_stream_id,
                false,
                |tc| {
                    let f = internal_function(tc, "__dispatchTo")?;
                    // A held body crosses as its bytes; a streamed body crosses as
                    // its host stream id, so the handler reads it in parts instead
                    // of the routing seam collecting it first.
                    let (body_value, stream_id) = match body {
                        RequestBody::Bytes(bytes) => {
                            (bytes_value(tc, bytes.into()), v8::null(tc).into())
                        }
                        RequestBody::Stream(id) => (
                            v8::undefined(tc).into(),
                            v8::Number::new(tc, id as f64).into(),
                        ),
                    };
                    let arguments = [
                        v8::String::new(tc, &scope).unwrap().into(),
                        v8::String::new(tc, &url).unwrap().into(),
                        v8::String::new(tc, &method).unwrap().into(),
                        body_value,
                        v8::String::new(tc, &serde_json::to_string(&headers)?)
                            .unwrap()
                            .into(),
                        match request_id {
                            Some(id) => v8::String::new(tc, &request_id_string(id)).unwrap().into(),
                            None => v8::null(tc).into(),
                        },
                        stream_id,
                    ];
                    let recv = v8::undefined(tc).into();
                    f.call(tc, recv, &arguments)
                        .ok_or_else(|| anyhow!("dispatchTo threw"))
                },
            )
        }
        CellJob::Rpc {
            request_id,
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
            start_cell_event(
                tc,
                &scope,
                Answer::CellRpc(reply),
                request_id,
                None,
                false,
                |tc| {
                    let f = internal_function(tc, "__dispatchRpc")?;
                    let arguments = [
                        v8::String::new(tc, &scope).unwrap().into(),
                        v8::String::new(tc, &method).unwrap().into(),
                        rpc_data_value(tc, args),
                    ];
                    let recv = v8::undefined(tc).into();
                    f.call(tc, recv, &arguments)
                        .ok_or_else(|| anyhow!("dispatchRpc threw"))
                },
            )
        }
        CellJob::WsOpen {
            scope,
            ws_id,
            protocol,
            reply,
        } => start_cell_event(tc, &scope, Answer::Ack(reply), None, None, false, |tc| {
            let f = internal_function(tc, "__wsOpen")?;
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
        } => start_cell_event(
            tc,
            &scope,
            Answer::WsMessage(reply),
            None,
            None,
            true,
            |tc| {
                let (name, data) = match data {
                    WsIn::Text(text) => ("__wsMessage", v8::String::new(tc, &text).unwrap().into()),
                    WsIn::Binary(bytes) => ("__wsBinary", bytes_value(tc, bytes)),
                };
                let f = internal_function(tc, name)?;
                let arguments = [
                    v8::String::new(tc, &scope).unwrap().into(),
                    v8::Number::new(tc, ws_id as f64).into(),
                    data,
                ];
                let recv = v8::undefined(tc).into();
                f.call(tc, recv, &arguments)
                    .ok_or_else(|| anyhow!("WebSocket message dispatch threw"))
            },
        ),
        CellJob::WsClosed {
            scope,
            ws_id,
            code,
            reason,
            was_clean,
            reply,
        } => start_cell_event(tc, &scope, Answer::Ack(reply), None, None, false, |tc| {
            let f = internal_function(tc, "__wsClosed")?;
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
            request_id,
            scope,
            scheduled_ms,
            claim,
            reply,
        } => begin_alarm(
            tc,
            &scope,
            scheduled_ms,
            claim,
            request_id,
            reply,
            event_time,
        ),
        #[cfg(celld_internal_tests)]
        CellJob::SyncErrorForTest {
            scope,
            gate,
            socket_id,
            terminate,
            reply,
        } => start_cell_event(tc, &scope, Answer::Ack(reply), None, None, false, |tc| {
            register_arm_gate_with_current_event(gate, installed_context());
            if let Some(socket_id) = socket_id {
                current_context().sockets.lock().unwrap().push(socket_id);
            }
            if terminate {
                let state = actor_runtime_state(tc);
                *state.termination.lock().expect("termination lock poisoned") =
                    Some(ExecutionTermination {
                        error: "synchronous V8 termination sentinel".to_string(),
                        actor_scope: None,
                        context_id: None,
                    });
                tc.terminate_execution();
                Err(anyhow!("generic synchronous dispatch failure"))
            } else {
                Err(anyhow!("synchronous cell dispatch sentinel"))
            }
        }),
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
    request_id: Option<RequestId>,
    reply: tokio::sync::oneshot::Sender<Result<(celld_logic::wake::AlarmSnapshot, Option<u64>)>>,
    event_time: i64,
) -> Begun {
    if event_time < scheduled_ms {
        let _ = reply.send(Err(anyhow!("alarm dispatched before its deadline")));
        return Begun::Nothing;
    }
    #[cfg(celld_internal_tests)]
    if let AlarmDispatch::Claimed(retry) = claim {
        let Some(scheduled_at) = storage::active_alarm_scheduled_time(scope) else {
            let _ = reply.send(Err(anyhow!("alarm dispatched without a claim")));
            return Begun::Nothing;
        };
        return fire_alarm_handler(tc, scope, scheduled_at, retry, None, request_id, reply);
    }
    let due_by = match claim {
        #[cfg(celld_internal_tests)]
        AlarmDispatch::Armed => i64::MAX,
        AlarmDispatch::Due => event_time,
        #[cfg(celld_internal_tests)]
        AlarmDispatch::Claimed(_) => unreachable!("claimed alarms return above"),
    };
    let Some((scheduled_at, retry)) = storage::due_alarm_entry(scope, due_by) else {
        // Nothing is due: another dispatch already ran it, or the handler
        // that armed it cleared it. Answer what stands now, with no delta —
        // no handler ran, so there is nothing written to prove.
        let _ = reply.send(Ok((observe_alarm(scope, storage::get_alarm(scope)), None)));
        return Begun::Nothing;
    };
    storage::begin_alarm_handler(scope, scheduled_at);
    fire_alarm_handler(
        tc,
        scope,
        scheduled_at,
        retry,
        Some(AlarmClaim { now_ms: event_time }),
        request_id,
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
    request_id: Option<RequestId>,
    reply: tokio::sync::oneshot::Sender<Result<(celld_logic::wake::AlarmSnapshot, Option<u64>)>>,
) -> Begun {
    let begun = start_cell_event(
        tc,
        scope,
        Answer::Alarm(reply),
        request_id,
        None,
        false,
        |tc| {
            let f = internal_function(tc, "__fireAlarm")?;
            let arguments = [
                v8::String::new(tc, scope).unwrap().into(),
                v8::Number::new(tc, scheduled_at as f64).into(),
                v8::Number::new(tc, retry as f64).into(),
            ];
            let recv = v8::undefined(tc).into();
            f.call(tc, recv, &arguments)
                .ok_or_else(|| anyhow!("alarm threw"))
        },
    );
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
    entry.io_context_ops.remove(&op);
    entry.unrefed_ops.remove(&op);
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
    let shutdown = take_shutdown_cancellation(entry.request_id);
    cancel(tc, entry, shutdown);
}

/// End a request whose cancellation has already been taken.
///
/// Split from [`cancelled`] because a suspended request observes the
/// cancellation *outside* the isolate — the flag is host state and costs no
/// V8 — and only then enters to act on it.
fn cancel(tc: &mut v8::PinScope, entry: &mut InFlight, shutdown: bool) {
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
    if shutdown {
        finish_retired_input_gate_context(tc, &entry.context);
    }
    drop(guard);
    entry.fail_cancelled(anyhow!("The client has disconnected"));
    if shutdown {
        // A lifecycle cancellation can be consumed either between turns or
        // during an operation turn. Keep the background cleanup here so both
        // paths retire the same work.
        entry.retire_background_for_shutdown();
    }
    // A cancelled handler with no waitUntil work has nothing left to drive.
    // Drop its host ops now, so their guards cancel routed work. Explicit
    // waitUntil work keeps its ops and continues after the client disconnects.
    //
    // So does a handler that holds or awaits the cell's input gate, or whose
    // cross-entry block can still use an operation started by this event.
    // Not at shutdown: nothing will run the block to its end, so its gate is
    // given up instead.
    if shutdown || !entry.keeps_native_ops_after_disconnect() {
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
                entry.context.seal_wait_until();
                if let Some(request_id) = entry.active_request_id {
                    finish_incoming_request(tc, request_id);
                }
                drop(guard);
                entry.fail_in_turn(anyhow!("rejected: {reason}"));
            }
        }
    }
    if let Some(background) = &entry.background {
        let promise = v8::Local::new(tc, background);
        if !matches!(promise.state(), v8::PromiseState::Pending) {
            let late = entry.context.take_late_wait_until();
            entry.background =
                wait_until_aggregate(tc, &late).map(|promise| v8::Global::new(tc, promise));
            if !late.is_empty() && entry.background.is_none() {
                entry.context.seal_wait_until();
            }
        }
    }
}

fn wait_until_aggregate<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    promises: &[v8::Global<v8::Promise>],
) -> Option<v8::Local<'s, v8::Promise>> {
    if promises.is_empty() {
        return None;
    }
    let values = promises
        .iter()
        .map(|promise| v8::Local::new(scope, promise).into())
        .collect::<Vec<v8::Local<v8::Value>>>();
    let array = v8::Array::new_with_elements(scope, &values);
    all_settled(scope, array.into())?.try_cast().ok()
}

enum FetchTarget<'s> {
    Default(v8::Local<'s, v8::Function>),
    Entrypoint(crate::WorkerFetchEntrypoint),
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
    target: FetchTarget<'s>,
    url: &str,
    method: &str,
    body: RequestBody,
    headers: &[(String, String)],
    request_id: Option<RequestId>,
) -> Result<Started<'s>> {
    let req = match request_id {
        Some(_) => make_incoming_request(tc, url, method, body, headers),
        None => make_request(tc, url, method, body, headers),
    }?;
    let recv = v8::undefined(tc).into();
    let ret = match target {
        FetchTarget::Entrypoint(entrypoint) => {
            let dispatch = internal_function(tc, "__dispatchEntrypointFetch")?;
            let name = v8::String::new(tc, &entrypoint.name).unwrap();
            let props = bytes_value(tc, entrypoint.props);
            // Settlement and synchronous-throw cleanup both close this host
            // frame. The dispatcher supplies its own handler context, but
            // omitting the outer frame would leave those paths unbalanced.
            begin_event_context(tc)?;
            dispatch.call(tc, recv, &[name.into(), req, props])
        }
        FetchTarget::Default(fetch) => {
            let env = harness_env(tc)?;
            let execution_ctx = begin_event_context(tc)?;
            fetch.call(tc, recv, &[req, env, execution_ctx])
        }
    };
    let Some(ret) = ret else {
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

fn settle_isolate_heap(isolate: &mut v8::Isolate) {
    #[cfg(all(test, celld_internal_tests))]
    let _observation = HeapSettlementObservationForTest::begin();
    isolate.low_memory_notification();
}

impl Worker {
    /// Compile the worker module, wire the DO harness, and extract the entry
    /// `fetch`. `do_classes` come from the manifest; `bindings` maps a binding
    /// name to a DO class name (from wrangler metadata).
    ///
    /// The runtime builds a `WorkerConfig` directly. This cfg-gated helper
    /// constructs the same value from individual options.
    #[cfg(celld_internal_tests)]
    pub fn load(options: WorkerConfigOptions) -> Result<Worker> {
        Self::load_config(Arc::new(WorkerConfig::new(options)))
    }

    pub fn load_config(config: Arc<WorkerConfig>) -> Result<Worker> {
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
            #[cfg(celld_internal_tests)]
            forced_admission_refusal: AtomicBool::new(false),
        });
        let runtime_state = Arc::new(ActorRuntimeState {
            promises: std::sync::Mutex::new(PromiseMap::new()),
            egress: config.egress.clone(),
            resource_limits: config.resource_limits,
            tail_reporting: config.tail_reporting,
            script_name: script_name.to_string(),
            ..Default::default()
        });
        let loader_owner = LoaderOwner::fresh(script_name, config.generation);
        let heap_limit_state_ptr =
            Arc::as_ptr(&heap_limit_state) as *mut HeapLimitState as *mut std::ffi::c_void;
        isolate.set_slot(heap_limit_state);
        isolate.set_slot(runtime_state.clone());
        isolate.set_slot(Arc::new(ModuleRegistry::default()));
        isolate.set_slot(loader_owner.clone());
        isolate.set_slot(crate::generation::GenerationTag(config.generation));
        // Retains the config so `/bundle` can project it later. Nothing is
        // walked or copied here: a worker that never reads its own bundle pays
        // only this `Arc` clone.
        isolate.set_slot(Arc::new(BundleFs {
            config: config.clone(),
            tree: OnceLock::new(),
        }));
        isolate.add_near_heap_limit_callback(near_heap_limit, heap_limit_state_ptr);
        let (context, fetch) = {
            v8::scope!(let hs, &mut isolate);
            let context = v8::Context::new(hs, Default::default());
            let cs = &mut v8::ContextScope::new(hs, context);
            let tc = std::pin::pin!(v8::TryCatch::new(cs));
            let scope = &mut tc.init();

            let arguments = install_ops(scope);
            install_prelude(scope, &arguments)?; // Web Platform APIs
            install_harness(scope, &arguments)?; // DO object model + minimal Response
            install_lazy_globals(scope)?;
            // A global, so it must exist before the module evaluates: bundles
            // read Cloudflare.compatibilityFlags at module scope.
            inject_compatibility_flags(scope, compat)?;
            inject_storage_compatibility(scope, compat)?;

            let module = match compile_module(scope, &config.main_module_name, src) {
                Some(m) => m,
                None => return Err(anyhow!("compile: {}", exc!(scope))),
            };
            register_stubs(scope, &config); // cloudflare:*/node:* + text modules
            register_wasm_modules(scope, &config.modules);
            register_loader_modules(scope, &config, module);
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
                        "top-level rejected: {}\n{}",
                        r.to_rust_string_lossy(scope),
                        stk
                    ));
                }
            }

            let ns = module
                .get_module_namespace()
                .to_object(scope)
                .ok_or_else(|| anyhow!("ns"))?;
            // workerd reads every export of the main module as a handler or
            // a class, so a primitive export fails the Worker's start.
            let names = ns
                .get_own_property_names(scope, Default::default())
                .ok_or_else(|| anyhow!("ns names"))?;
            for index in 0..names.length() {
                let name = names.get_index(scope, index).unwrap();
                let value = ns.get(scope, name).unwrap();
                if !value.is_object() {
                    return Err(anyhow!(
                        "Uncaught TypeError: Incorrect type for map entry '{}': the \
                         provided value is not of type 'function or ExportedHandler'.",
                        name.to_rust_string_lossy(scope)
                    ));
                }
            }

            // register each exported DO class into the harness registry
            for cn in do_classes {
                // The D1 class is the runtime's own and is never a worker
                // export. It is in `do_classes` so that it gets a namespace
                // key; reading it off the module namespace would find
                // `undefined` and overwrite the harness's registration.
                if crate::deploy::is_reserved_class(cn) {
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
            inject_workflows(scope, script_name, &config.workflow_bindings)?;
            inject_containers(scope, &config.containers)?;
            inject_kv_limits(scope)?;
            inject_queue_config(scope, &config)?;
            populate_cf_exports(scope, ns, do_classes)?;
            register_entrypoints(scope, ns)?;
            validate_workflow_classes(scope, ns, &config.workflow_bindings)?;
            // build env from bindings and stash it in the harness
            build_env(scope, &config)?;
            // tell the harness which cells are local (route the rest cross-node)
            inject_routing(scope, node)?;

            // entry fetch. A loaded Worker often exists for its Durable
            // Object class alone, and workerd loads such a module without a
            // default export, so one reads as an empty object here. A
            // deployment's own script keeps the refusal: a bundle that lost
            // its handler must fail adoption, so the serving generation stays
            // up instead of answering 500 to every request.
            let loaded = config.origin == WorkerOrigin::Dynamic;
            let dk = v8::String::new(scope, "default").unwrap();
            let default_value = ns
                .get(scope, dk.into())
                .ok_or_else(|| anyhow!("no default export"))?;
            let default = if default_value.is_object() {
                default_value.to_object(scope).expect("an object")
            } else if loaded && default_value.is_undefined() {
                v8::Object::new(scope)
            } else {
                return Err(anyhow!("default not object"));
            };
            let fk = v8::String::new(scope, "fetch").unwrap();
            let fetch_value = default
                .get(scope, fk.into())
                .ok_or_else(|| anyhow!("no fetch"))?;
            let default_is_entrypoint =
                default.is_function() && cell_registry_has(scope, "entrypoints", "default")?;
            let class_fetch = default_is_entrypoint
                && default
                    .get(scope, v8::String::new(scope, "prototype").unwrap().into())
                    .and_then(|prototype| prototype.to_object(scope))
                    .and_then(|prototype| prototype.get(scope, fk.into()))
                    .is_some_and(|handler| handler.is_function());
            let qk = v8::String::new(scope, "queue").unwrap();
            let own_queue = default
                .get(scope, qk.into())
                .is_some_and(|handler| handler.is_function());
            let class_queue = default_is_entrypoint
                && default
                    .get(scope, v8::String::new(scope, "prototype").unwrap().into())
                    .and_then(|prototype| prototype.to_object(scope))
                    .and_then(|prototype| prototype.get(scope, qk.into()))
                    .is_some_and(|handler| handler.is_function());
            let has_queue_handler = own_queue || class_queue;
            anyhow::ensure!(
                !config.declares_queue_consumer || has_queue_handler,
                "queue consumer has no queue handler"
            );
            let f: v8::Local<v8::Function> = if fetch_value.is_function() {
                fetch_value.try_into().expect("function casts to Function")
            } else if default_is_entrypoint && class_fetch {
                // A class-based default entrypoint (extends WorkerEntrypoint)
                // keeps fetch on the prototype, so route through the
                // harness's invocation instance like a named entrypoint. Only a
                // registered entrypoint dispatches that way — any other
                // callable would load fine and then 500 on every request.
                compile_fn(
                    scope,
                    "(req) => __celld.__dispatchEntrypointFetch('default', req)",
                )?
            } else if default.is_function() && cell_registry_has(scope, "doExports", "default")? {
                return Err(anyhow!(
                    "the default export is a Durable Object class; export a fetch \
                     handler or a WorkerEntrypoint class as the default"
                ));
            } else if config.declares_queue_consumer {
                // A push-consumer Worker needs no HTTP handler. Keep the
                // stateless pool's fetch slot total, but fail closed if this
                // deployment becomes the fleet's HTTP entry point. Test the
                // prototype above before the entrypoint fallback below: a
                // queue-only class is still a registered entrypoint, but
                // routing fetch through it throws instead of returning 404.
                compile_fn(scope, "() => new Response('Not found', { status: 404 })")?
            } else if default_is_entrypoint {
                // Preserve an RPC-only default entrypoint. Its HTTP path
                // reports the harness's specific missing-fetch error, while
                // its named methods remain callable through RPC.
                compile_fn(
                    scope,
                    "(req) => __celld.__dispatchEntrypointFetch('default', req)",
                )?
            } else if loaded {
                compile_fn(scope, NO_FETCH_HANDLER)?
            } else {
                return Err(anyhow!("fetch not fn"));
            };

            // Lets a self-targeted service binding invoke the handler in
            // this isolate instead of crossing to a pool thread.
            {
                if let Ok(cell) = cell_state(scope) {
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
                        // the prototype; dispatch through an invocation instance
                        // like fetch above.
                        let pk = v8::String::new(scope, "prototype").unwrap();
                        let proto_scheduled = default
                            .get(scope, pk.into())
                            .and_then(|proto| proto.to_object(scope))
                            .and_then(|proto| proto.get(scope, sk.into()));
                        if proto_scheduled.is_some_and(|handler| handler.is_function()) {
                            let shim = compile_fn(
                                scope,
                                "(ctrl) => __celld.__dispatchEntrypointScheduled('default', ctrl)",
                            )?;
                            cell.set(scope, key_.into(), shim.into());
                        }
                    }
                }
            }
            (v8::Global::new(scope, context), v8::Global::new(scope, f))
        };
        // Module evaluation and harness installation allocate temporary V8
        // objects. No request can reuse them after these scopes close, and a
        // new cell isolate can otherwise retain their pages indefinitely.
        // Observe only post-setup collection, so bootstrap GC cannot make
        // a missing settlement notification pass the regression.
        #[cfg(all(test, celld_internal_tests))]
        install_heap_collection_observer_for_test(&mut isolate);
        settle_isolate_heap(&mut isolate);
        Ok(Worker {
            inner: Some(WorkerIsolate {
                // Every setup scope above has closed, so nothing is entered
                // on top of this isolate and it can be handed over.
                // SAFETY: `into_shared` requires every piece of embedder
                // state hanging off this isolate to be `Send`, because it
                // migrates between threads and is dropped on whichever one
                // holds the lock last. This isolate carries exactly:
                //
                // - four slots, whose types the assertion below pins as
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
                runtime_state,
                realm: Realm { context, fetch },
                original_heap_limit,
                compat,
                loader_owner,
                cells: storage::Cells::default(),
            }),
        })
    }

    /// Restore an idFromName() actor's human-readable identity before its
    /// constructor runs. The host calls this on activation and before a named
    /// request is dispatched.
    /// Take a cell into this isolate or give it back.
    ///
    /// Taking it opens the cell's SQLite -- which the isolate owns, not the
    /// caller -- and restores the persisted id name. Giving it back releases
    /// what the isolate holds for the residency and closes the database, so
    /// state cannot span two epochs.
    ///
    /// Dispatch does not depend on this: every cell call goes out through the
    /// host whichever isolate hosts the target.
    pub fn own_cell(
        &mut self,
        cell: &str,
        storage: Option<CellStorage<'_>>,
    ) -> Result<celld_logic::wake::AlarmSnapshot> {
        let compat = self.inner.as_ref().expect("live worker isolate").compat;
        let (mut locker, _cells) = self.lock();
        v8::scope!(let hs, &mut *locker);
        let realm = self.realm(hs);
        let context = realm.context;
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let alarm = adopt_cell(&mut tc.init(), cell, storage, compat)?;
        // The source identity belongs to this installed SQLite turn. Reading
        // it from the pool after this guard drops reaches no cell connection.
        Ok(observe_alarm(cell, alarm))
    }

    /// Collect objects left by cell adoption after an isolate becomes full.
    /// The pool calls this when completed adoptions reach the resident limit,
    /// because collecting after every adoption slows activation.
    pub(crate) fn settle_heap(&mut self) {
        let (mut locker, _cells) = self.lock();
        settle_isolate_heap(&mut locker);
    }

    fn own_embedded_cell(
        &mut self,
        cell: &str,
        parent: &storage::StorageIdentity,
        name: &str,
        id: &str,
        props_sc: Vec<u8>,
        file: (std::path::PathBuf, bool),
    ) -> Result<Option<i64>> {
        let compat = self.inner.as_ref().expect("live worker isolate").compat;
        let (mut locker, _cells) = self.lock();
        if storage::activation_epoch(cell).is_some() {
            // The facet is already open, but this call carries a newer sample
            // of the root cell. Take it: the egress of this call must wait for
            // what the root cell had committed when the call left it.
            storage::refresh_embedded_root(cell, parent);
            return Ok(None);
        }
        v8::scope!(let hs, &mut *locker);
        let realm = self.realm(hs);
        let context = realm.context;
        let cs = &mut v8::ContextScope::new(hs, context);
        let tc = std::pin::pin!(v8::TryCatch::new(cs));
        let tc = &mut tc.init();
        let (path, restored) = file;
        adopt_embedded_cell(
            tc,
            cell,
            parent,
            name,
            EmbeddedStartup {
                id,
                props_sc,
                path,
                restored,
            },
            compat,
        )
    }

    /// Drain the alarm moves the last turn committed in this isolate.
    ///
    /// An alarm move is a turn output, exactly like the ops a turn starts:
    /// the drive that ran the turn reports it to the host, so a handler
    /// that arms an alarm and then awaits it is schedulable immediately,
    /// not when the request ends. A separate call rather than part of the
    /// turn methods' return because stateless turns cannot move an alarm
    /// and never pay for it.
    /// Bind the revision under the isolate lock too. A later reporter callback
    /// can run after another turn arms the cell; sampling there would combine
    /// the old value with the new arm's authority to change its wake entry.
    pub fn take_alarm_moves(&mut self) -> Vec<(String, celld_logic::wake::AlarmSnapshot)> {
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
        trace: Option<crate::telemetry::TraceContext>,
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

        let event_time = advance_io_time(tc);
        let previous = install_trace(tc, trace.as_ref());
        let cpu_watchdog = inner.cpu_watchdog(
            inner.initial_cpu_budget(),
            inner
                .runtime_state
                .resource_limits
                .and_then(|limits| limits.cpu_ms),
        );
        let begun = begin_cell(tc, job, event_time);
        drop(cpu_watchdog);
        let out = match begun {
            Begun::Running(mut entry) => {
                entry.trace = trace;
                let previous_io_context = install_io_context(tc, &entry.context);
                let ops = finish_turn(tc, &mut entry);
                restore_io_context(tc, previous_io_context);
                (Some(*entry), ops)
            }
            Begun::Threw(failure) => {
                let BegunFailure {
                    answer, cpu_error, ..
                } = *failure;
                answer.fail(
                    take_execution_termination(tc)
                        .or_else(|| cpu_error.map(anyhow::Error::msg))
                        .unwrap_or_else(|| anyhow!("cell event threw: {}", exc!(tc))),
                );
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

        let previous_io_context = install_io_context(tc, &entry.context);
        let previous = install_trace(tc, entry.trace.as_ref());
        entry.context.begin_cpu_turn();
        let ops = finish_turn(tc, entry);
        restore_trace(tc, previous);
        restore_io_context(tc, previous_io_context);
        ops
    }

    /// Reject each pending `WorkerEntrypoint` call that has no native work.
    /// The rejection runs inside the owning request, so the enclosing handler
    /// can catch it and continue exactly as it can in Workerd.
    pub(crate) fn turn_cancel_pending_events(&mut self, entry: &mut InFlight) -> Vec<Op> {
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

        let previous_io_context = install_io_context(tc, &entry.context);
        let previous = install_trace(tc, entry.trace.as_ref());
        entry.context.begin_cpu_turn();
        let cpu_watchdog = inner.cpu_watchdog(
            entry.context.remaining_cpu_budget(),
            entry.context.cpu_limit_ms(),
        );
        let ids = entry.context.take_pending_events();
        let called = (|| {
            let function = internal_function(tc, "__cancelPendingEvents").ok()?;
            let values = ids
                .iter()
                .map(|id| v8::Number::new(tc, *id as f64).into())
                .collect::<Vec<v8::Local<v8::Value>>>();
            let values = v8::Array::new_with_elements(tc, &values);
            let receiver = v8::undefined(tc).into();
            function.call(tc, receiver, &[values.into()])
        })();
        if called.is_none() {
            entry.fail_in_turn(anyhow!("pending-event cancellation threw"));
        }
        drop(cpu_watchdog);
        let ops = finish_turn(tc, entry);
        restore_trace(tc, previous);
        restore_io_context(tc, previous_io_context);
        ops
    }

    /// Enter the isolate solely to record how a claimed alarm ended.
    ///
    /// Reached only when the event ended without running JS again — a budget
    /// overrun, or a handler waiting on nothing — or threw before it could
    /// suspend. The bookkeeping is storage the isolate owns, so it cannot be
    /// done from the driving task.
    ///
    /// Answers the position the event's commits reached, sampled after the
    /// retry record is written: that record and whatever the handler wrote
    /// before it failed are unproven, and the error that already left carried
    /// at most the handler's part, so `fire_alarm` gates on this instead.
    pub fn turn_finish_alarm(&mut self, entry: &mut InFlight) -> Result<Option<u64>> {
        debug_assert!(
            self.inner.is_some(),
            "the final alarm turn runs on a live worker"
        );
        let Some(inner) = self.inner.as_mut() else {
            return Ok(None);
        };
        let (_locker, _cells) = inner.lock();
        entry.settle_alarm(false, false);
        entry.write_delta()
    }
}

// ---- native ops exposed to JS ----

/// One host op per entry. The macro names the table and the installer
/// together, so the parameter list every internal script is compiled with
/// and the functions it is called with come from one source and cannot
/// drift apart.
///
/// The ops are runtime internals and never globals: `for (const k in
/// globalThis) new globalThis[k]()` once reached `__actor_abort` and killed
/// the actor, and a loaded worker once reached `__do_call` around its
/// `globalOutbound: null` (denoland/celld#192).
macro_rules! ops {
    ($names:ident, $install:ident, $($name:literal => $op:path),* $(,)?) => {
        const $names: &[&str] = &[$($name),*];
        fn $install<'s>(
            scope: &mut v8::PinScope<'s, '_>,
            functions: &mut Vec<v8::Local<'s, v8::Function>>,
        ) {
            $({
                // Each expansion defines its own `attributed`, so every op
                // keeps a distinct callback. It records which continuation
                // enqueued the op, and only when the call enqueued one: the
                // count is a thread-local read, while the name costs a CPED
                // lookup.
                fn attributed<'s>(
                    scope: &mut v8::PinScope<'s, '_>,
                    args: v8::FunctionCallbackArguments<'s>,
                    rv: v8::ReturnValue<'s, v8::Value>,
                ) {
                    #[cfg(celld_internal_tests)]
                    let mut rv = rv;
                    #[cfg(celld_internal_tests)]
                    if call_op_patch(scope, $name, &args, &mut rv) {
                        return;
                    }
                    let before = asyncrt::spawn_count();
                    $op(scope, args, rv);
                    if asyncrt::spawn_count() != before {
                        if let Some(owner) = operation_continuation_id(scope) {
                            asyncrt::attribute_spawns(before, owner);
                        }
                    }
                }
                functions.push(v8::Function::new(scope, attributed).unwrap());
            })*
        }
    };
}

/// The parameters every internal script is compiled with: the internals
/// object, then one binding per host op in table order. The values are what
/// the script is called with, in the same order.
///
/// Ops reach the harness as parameters and not as globals, so a bundle cannot
/// name `__do_call` and address a cell it holds no stub for, and a loaded
/// worker with `globalOutbound: null` cannot reach the host at all except
/// through the capabilities in its `env`. A parameter is also a binding user
/// code cannot replace, which is what the event hooks already required.
///
/// Parameters rather than `__celld.__op(...)` at every call site: the
/// internal scripts name an op in several hundred places, a bare identifier
/// keeps each of those lines as it was, and a closure binding costs a
/// context-slot load where a property costs a lookup. The same op is also a
/// property of `__celld`, read-only, for the host and for the test hatch.
pub(super) struct InternalArguments<'s> {
    names: Vec<v8::Local<'s, v8::String>>,
    values: Vec<v8::Local<'s, v8::Value>>,
}

fn op_names() -> impl Iterator<Item = &'static str> {
    #[cfg(celld_internal_tests)]
    let test = TEST_OP_NAMES.iter().copied();
    #[cfg(not(celld_internal_tests))]
    let test = std::iter::empty();
    OP_NAMES.iter().copied().chain(test)
}

impl<'s> InternalArguments<'s> {
    /// `__celld` first, then `ops` in `op_names()` order.
    fn new(
        scope: &mut v8::PinScope<'s, '_>,
        internals: v8::Local<'s, v8::Object>,
        ops: impl FnMut(
            &mut v8::PinScope<'s, '_>,
            v8::Local<'s, v8::String>,
        ) -> v8::Local<'s, v8::Value>,
    ) -> Self {
        let mut ops = ops;
        let mut names = vec![v8_strings::key(scope, &v8_strings::INTERNALS)];
        let mut values = vec![internals.into()];
        for name in op_names() {
            let key = v8::String::new(scope, name).unwrap();
            values.push(ops(scope, key));
            names.push(key);
        }
        Self { names, values }
    }
}

/// Build the ops, hold them on the internals object, and return them as the
/// arguments of the bootstrap scripts. Nothing here touches the global.
fn install_ops<'s>(scope: &mut v8::PinScope<'s, '_>) -> InternalArguments<'s> {
    let internals = v8::Object::new(scope);
    let mut functions = Vec::new();
    install_op_functions(scope, &mut functions);
    #[cfg(celld_internal_tests)]
    install_test_op_functions(scope, &mut functions);
    let mut functions = functions.into_iter();
    // Read-only: a test that assigns `__celld.__op = fn`, the idiom that
    // patching a global once allowed, throws instead of binding a script that
    // compiles later to a function the bootstrap harness never saw.
    let arguments = InternalArguments::new(scope, internals, |scope, key| {
        let function = functions.next().expect("one function per op name");
        internals.define_own_property(
            scope,
            key.into(),
            function.into(),
            v8::PropertyAttribute::READ_ONLY | v8::PropertyAttribute::DONT_DELETE,
        );
        function.into()
    });
    actor_runtime_state(scope)
        .internals
        .set(v8::Global::new(scope, internals))
        .unwrap_or_else(|_| panic!("internals were already installed"));
    // A test build exposes the internals object, so an engine test can reach
    // ops and harness state by name and patch an op through
    // `__test_patch_op`. A shipping isolate has no `__`-prefixed global.
    #[cfg(celld_internal_tests)]
    {
        let global = scope.get_current_context().global(scope);
        let key = v8_strings::key(scope, &v8_strings::INTERNALS);
        global.define_own_property(
            scope,
            key.into(),
            internals.into(),
            v8::PropertyAttribute::DONT_ENUM,
        );
    }
    arguments
}

/// The same arguments, rebuilt from the internals object for a script that
/// compiles after bootstrap: a lazy global or a lazy builtin module.
pub(super) fn internal_arguments<'s>(scope: &mut v8::PinScope<'s, '_>) -> InternalArguments<'s> {
    let internals = internals(scope);
    InternalArguments::new(scope, internals, |scope, key| {
        internals.get(scope, key.into()).unwrap()
    })
}

/// The internals object of this isolate: every host op and every harness
/// function the host calls, reachable from Rust and from internal scripts
/// but from no user code.
pub(super) fn internals<'s>(scope: &mut v8::PinScope<'s, '_>) -> v8::Local<'s, v8::Object> {
    let state = actor_runtime_state(scope);
    let internals = state
        .internals
        .get()
        .expect("internals are installed before any script runs");
    v8::Local::new(scope, internals)
}

pub(super) fn internal_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    name: &str,
) -> Option<v8::Local<'s, v8::Value>> {
    let internals = internals(scope);
    let key = v8::String::new(scope, name).unwrap();
    internals
        .get(scope, key.into())
        .filter(|value| !value.is_undefined())
}

pub(super) fn internal_function<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    name: &str,
) -> Result<v8::Local<'s, v8::Function>> {
    internal_value(scope, name)
        .ok_or_else(|| anyhow!("no {name}"))?
        .try_into()
        .map_err(|_| anyhow!("{name} is not a function"))
}

pub(super) fn internal_object<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    name: &str,
) -> Result<v8::Local<'s, v8::Object>> {
    internal_value(scope, name)
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing {name} runtime state"))
}

/// `__cell`, the harness's runtime-state object.
pub(super) fn cell_state<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<v8::Local<'s, v8::Object>> {
    let internals = internals(scope);
    let key = v8_strings::key(scope, &v8_strings::CELL);
    internals
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))
}

/// Run a small host-generated snippet with `__celld` bound. The snippet
/// differs per load, so it is compiled without the bootstrap cache.
pub(super) fn run_internal_snippet<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    source: &str,
) -> Option<v8::Local<'s, v8::Value>> {
    let code = v8::String::new(scope, source)?;
    let arguments = InternalArguments {
        names: vec![v8_strings::key(scope, &v8_strings::INTERNALS)],
        values: vec![internals(scope).into()],
    };
    bootstrap::run_internal_script(scope, None, code, &arguments).ok()
}

/// `__test_patch_op(name, fn | null)`: route the named op through `fn` for
/// the rest of the isolate's life, or restore it. The patch receives the op's
/// arguments and `this`; an op it calls itself runs the real op, so a patch
/// can observe or clamp arguments and then forward them.
///
/// The forward must happen before the patch returns. The re-entrancy guard
/// is held for that synchronous extent only, so a forward after an `await`
/// meets the patch again and recurses.
#[cfg(celld_internal_tests)]
fn op_test_patch_op(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let requested = args.get(0).to_rust_string_lossy(scope);
    let Some(name) = op_names()
        .filter(|name| *name != "__test_patch_op")
        .find(|name| *name == requested)
    else {
        return loader_throw(
            scope,
            &format!("__test_patch_op: {requested} is not a patchable op"),
        );
    };
    let state = actor_runtime_state(scope);
    let mut patches = state.op_patches.lock().unwrap();
    match v8::Local::<v8::Function>::try_from(args.get(1)) {
        Ok(patch) => {
            state.op_patched.store(true, Ordering::Relaxed);
            patches.insert(name, v8::Global::new(scope, patch));
        }
        Err(_) => {
            patches.remove(name);
        }
    }
}

#[cfg(celld_internal_tests)]
fn call_op_patch(
    scope: &mut v8::PinScope,
    name: &'static str,
    args: &v8::FunctionCallbackArguments,
    rv: &mut v8::ReturnValue<v8::Value>,
) -> bool {
    let state = actor_runtime_state(scope);
    if !state.op_patched.load(Ordering::Relaxed) {
        return false;
    }
    let patch = state
        .op_patches
        .lock()
        .unwrap()
        .get(name)
        .map(|patch| v8::Local::new(scope, patch));
    let Some(patch) = patch else {
        return false;
    };
    // Re-entered from inside the patch: this call is the forward to the real
    // op.
    if !state.op_patches_active.lock().unwrap().insert(name) {
        return false;
    }
    let values: Vec<v8::Local<v8::Value>> = (0..args.length()).map(|i| args.get(i)).collect();
    let result = patch.call(scope, args.this().into(), &values);
    state.op_patches_active.lock().unwrap().remove(name);
    if let Some(result) = result {
        rv.set(result);
    }
    true
}

ops! { OP_NAMES, install_op_functions,
        "__heap_limit_excessively_exceeded" =>
            op_heap_limit_excessively_exceeded,
        "__heap_over_admission_share" => op_heap_over_admission_share,
        "__ws_send" => websocket::op_ws_send,
        "__ws_send_binary" => websocket::op_ws_send_binary,
        "__ws_close" => websocket::op_ws_close,
        "__ws_alloc" => websocket::op_ws_alloc,
        "__ws_prepare_worker_handoff" => websocket::op_ws_prepare_worker_handoff,
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
        "__hr_create" => html_rewriter::op_hr_create,
        "__hr_write" => html_rewriter::op_hr_write,
        "__hr_end" => html_rewriter::op_hr_end,
        "__hr_cmd" => html_rewriter::op_hr_cmd,
        "__hr_event" => html_rewriter::op_hr_event,
        "__hr_take" => html_rewriter::op_hr_take,
        "__hr_free" => html_rewriter::op_hr_free,
        "__tcp_connect" => tcp::op_tcp_connect,
        "__tcp_read" => tcp::op_tcp_read,
        "__tcp_write" => tcp::op_tcp_write,
        "__tcp_shutdown" => tcp::op_tcp_shutdown,
        "__tcp_close" => tcp::op_tcp_close,
        "__tcp_starttls" => tcp::op_tcp_starttls,
        "__container_running" => container::op_container_running,
        "__container_address" => container::op_container_address,
        "__container_start" => container::op_container_start,
        "__container_monitor" => container::op_container_monitor,
        "__container_destroy" => container::op_container_destroy,
        "__container_signal" => container::op_container_signal,
        "__container_inactivity" => container::op_container_inactivity,
        "__container_exec" => container::op_container_exec,
        "__container_exec_read" => container::op_container_exec_read,
        "__container_exec_write" => container::op_container_exec_write,
        "__container_exec_close" => container::op_container_exec_close,
        "__container_exec_wait" => container::op_container_exec_wait,
        "__container_exec_kill" => container::op_container_exec_kill,
        "__container_exec_drop" => container::op_container_exec_drop,
        "__storage_get" => storage_ops::op_storage_get,
        "__storage_get_many" => storage_ops::op_storage_get_many,
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
        "__storage_sync" => storage_ops::op_storage_sync,
        "__storage_cancel_pending_puts" => storage_ops::op_storage_cancel_pending_puts,
        "__actor_abort" => op_actor_abort,
        "__cron_plan" => op_cron_plan,
        "__kv_blob" => op_kv_blob,
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
        "__queue_alarm_set_wait" => op_queue_alarm_set_wait,
        "__alarm_get" => op_alarm_get,
        "__alarm_delete" => op_alarm_delete,
        "__loader_load" => op_loader_load,
        "__loader_fetch" => op_loader_fetch,
        "__loader_rpc" => op_loader_rpc,
        "__loader_drop" => op_loader_drop,
        "__facet_fetch" => op_facet_fetch,
        "__facet_rpc" => op_facet_rpc,
        "__facet_abort" => op_facet_abort,
        "__facet_delete" => op_facet_delete,
        "__do_call" => op_do_call,
        "__svc_call" => op_svc_call,
        "__svc_call_cancellable" => op_svc_call_cancellable,
        "__svc_rpc" => op_svc_rpc,
        "__queue_dispatch" => op_queue_dispatch,
        "__queue_policy" => op_queue_policy,
        "__do_call_cancellable" => op_do_call_cancellable,
        "__do_call_cancel" => op_do_call_cancel,
        "__rpc_signal_new" => op_rpc_signal_new,
        "__rpc_signal_abort" => op_rpc_signal_abort,
        "__rpc_signal_release" => op_rpc_signal_release,
        "__rpc_signal_poll" => op_rpc_signal_poll,
        "__rpc_signal_wait" => op_rpc_signal_wait,
        "__rpc_signal_unsubscribe" => op_rpc_signal_unsubscribe,
        "__do_id" => op_do_id,
        "__rpc_call" => op_rpc_call,
        "__sc_encode" => storage_ops::op_sc_encode,
        "__sc_decode" => storage_ops::op_sc_decode,
        "__structured_clone" => storage_ops::op_structured_clone,
        "__op_fetch" => op_fetch,
        "__r2_head" => r2_ops::op_r2_head,
        "__r2_get" => r2_ops::op_r2_get,
        "__r2_put" => r2_ops::op_r2_put,
        "__r2_put_begin" => r2_ops::op_r2_put_begin,
        "__r2_put_chunk" => r2_ops::op_r2_put_chunk,
        "__r2_put_end" => r2_ops::op_r2_put_end,
        "__r2_delete" => r2_ops::op_r2_delete,
        "__r2_list" => r2_ops::op_r2_list,
        "__r2_mp_begin" => r2_ops::op_r2_mp_begin,
        "__r2_mp_resume" => r2_ops::op_r2_mp_resume,
        "__r2_mp_part" => r2_ops::op_r2_mp_part,
        "__r2_mp_complete" => r2_ops::op_r2_mp_complete,
        "__r2_mp_abort" => r2_ops::op_r2_mp_abort,
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
        "__io_context_id" => op_io_context_id,
        "__with_input_gate_context" => op_with_input_gate_context,
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
        "__pending_event_begin" => op_pending_event_begin,
        "__pending_event_end" => op_pending_event_end,
        "__vfs_mkdir" => op_vfs_mkdir,
        "__vfs_read_file" => op_vfs_read_file,
        "__vfs_stat" => op_vfs_stat,
        "__wait_until" => op_wait_until,
        "__wait_until_active" => op_wait_until_active,
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

#[cfg(celld_internal_tests)]
ops! { TEST_OP_NAMES, install_test_op_functions,
    "__test_patch_op" => op_test_patch_op,
    "__test_gc" => op_test_gc,
        "__loader_count" => op_loader_count,
        "__test_set_heap_limit_excessively_exceeded" =>
            op_test_set_heap_limit_excessively_exceeded,
        "__test_force_heap_admission_refusal" =>
            op_test_force_heap_admission_refusal,
        "__test_heap_share" => op_test_heap_share,
        "__test_external_memory" => op_test_external_memory,
        "__test_workflow_event_consumed" => op_test_workflow_event_consumed,
        "__test_workflow_meta_created" => op_test_workflow_meta_created,
        "__test_workflow_terminal_alarm_set" => op_test_workflow_terminal_alarm_set,
        "__test_queue_dlq_accepted" => op_test_queue_dlq_accepted,
        "__test_queue_metrics_materialized" => op_test_queue_metrics_materialized,
        "__test_queue_producer_group" => op_test_queue_producer_group,
        "__test_queue_rearm_bounded" => op_test_queue_rearm_bounded,
        "__test_queue_lease_lookup_plan" => op_test_queue_lease_lookup_plan,
        "__test_kv_list_plan" => op_test_kv_list_plan,
        "__sql_set_max_page_count_for_test" =>
            storage_ops::op_sql_set_max_page_count_for_test,
        "__sql_set_write_fault_for_test" => storage_ops::op_sql_set_write_fault_for_test,
        "__sql_set_cache_size_for_test" => storage_ops::op_sql_set_cache_size_for_test,
        "__sql_set_interrupt_fault_for_test" =>
            storage_ops::op_sql_set_interrupt_fault_for_test,
    "__sql_register_nomem_function_for_test" =>
        storage_ops::op_sql_register_nomem_function_for_test,
}

/// `__kv_blob(requestJson, bytes?)` -> Promise<string | Uint8Array>.
///
/// One op for the whole large-value path, following `__d1_run` rather than
/// exposing get, put and sweep as three. A put copies one typed view into Rust,
/// and a successful get resolves directly to a `Uint8Array`. JSON arrays would
/// turn a valid 25 MiB value into millions of heap objects and crash the
/// isolate before the bucket I/O began.
///
/// JSON remains the control envelope for an absent get and a swept count. The
/// cell scope is host-derived, so JavaScript cannot read or collect another
/// namespace's objects by changing the request.
fn op_kv_blob(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let request = args.get(0).to_rust_string_lossy(scope);
    let value = view_bytes(args.get(1));
    // Bucket I/O is egress, so it waits on the same gate a service call does
    // rather than escaping the shed the node applies under pressure.
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Service);
    // Blob authority follows the storage installed for this exact cell
    // activation. Capturing it before enqueue prevents a later ownership
    // lookup from lending a deposed collector the new owner's epoch.
    let authority = gate
        .cell_scope()
        .and_then(|cell| storage::activation_epoch(cell).map(|epoch| (cell.to_string(), epoch)));
    let id = asyncrt::enqueue(async move {
        let (cell, activation_epoch) =
            authority.ok_or_else(|| "KV blob I/O requires active cell storage".to_string())?;
        await_egress_gate(gate).await?;
        match crate::kv_blob::run(&cell, activation_epoch, &request, value).await? {
            crate::kv_blob::BlobReply::Bytes(bytes) => Ok(asyncrt::OpOut::Bytes(bytes)),
            crate::kv_blob::BlobReply::Json(reply) => Ok(asyncrt::OpOut::Str(reply.to_string())),
        }
    });
    rv.set(promise_for(scope, id));
}

/// Create a JS promise for an async op `id` and return it to JS.
fn promise_for<'s>(scope: &mut v8::PinScope<'s, '_>, id: u64) -> v8::Local<'s, v8::Value> {
    let resolver = v8::PromiseResolver::new(scope).unwrap();
    let promise = resolver.get_promise(scope);
    promise_store(scope, id, v8::Global::new(scope, resolver));
    promise.into()
}

fn rejected_promise<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    message: &str,
) -> v8::Local<'s, v8::Value> {
    let resolver = v8::PromiseResolver::new(scope).unwrap();
    let promise = resolver.get_promise(scope);
    let message = v8::String::new(scope, message).unwrap();
    let exception = v8::Exception::error(scope, message);
    resolver.reject(scope, exception);
    promise.into()
}

fn op_queue_dispatch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let script = args.get(0).to_rust_string_lossy(scope);
    let queue = args.get(1).to_rust_string_lossy(scope);
    let envelope = args.get(2).to_rust_string_lossy(scope);
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Queue);
    let cell = gate.cell_scope().map(str::to_string).unwrap_or_default();
    let request = match crate::queue_policy::dispatch_request(
        current_generation(scope),
        cell,
        script,
        queue,
        &envelope,
    ) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("{error:#}")),
    };
    let id = asyncrt::enqueue(async move {
        if request.scope.is_empty() {
            return Err("Queue dispatch requires a cell event".to_string());
        }
        gated_channel_send(
            gate,
            &QUEUE_DISPATCH_TX,
            request,
            "no Queue dispatch channel",
        )
        .await?;
        Ok(asyncrt::OpOut::Str(String::new()))
    });
    rv.set(promise_for(scope, id));
}

/// The Queue policy boundary, `crate::queue_policy::run` behind one
/// synchronous op.
fn op_queue_policy(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let request: serde_json::Value =
        match serde_json::from_str(&args.get(0).to_rust_string_lossy(scope)) {
            Ok(request) => request,
            Err(error) => {
                return loader_throw(scope, &format!("invalid Queue policy input: {error}"))
            }
        };
    #[cfg(celld_internal_tests)]
    if request.get("op").and_then(serde_json::Value::as_str) == Some("settlement") {
        QUEUE_SETTLEMENT_POLICY_OBSERVED.with(|observed| observed.set(true));
    }
    match crate::queue_policy::run(&request) {
        Ok(value) => {
            let value = v8::String::new(scope, &value.to_string()).unwrap();
            rv.set(value.into());
        }
        Err(error) => loader_throw(scope, &error.to_string()),
    }
}

/// Async cross-node dispatch: hand the fetch to the tokio proxy task and await
/// its reply off-thread — the JS thread is never blocked. Resolves to a JSON
/// `{status, body, headers}` string the harness turns back into a Response.
/// `__svc_rpc(script, entrypoint, pathJson, argsSc, propsSc)` ->
/// Promise<Uint8Array>; arguments, props, and the result are structured-clone
/// bytes.
/// The application generation the calling isolate was built for, from the
/// slot `load_config` installs. Zero for an isolate built outside any
/// generation, which the runtime resolves as the current one.
fn current_generation(scope: &mut v8::PinScope) -> crate::generation::GenerationId {
    scope
        .get_slot::<crate::generation::GenerationTag>()
        .map(|tag| tag.0)
        .unwrap_or(0)
}

fn op_svc_rpc(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let context = event_context(scope);
    if let Err(error) = context.charge_subrequest() {
        return loader_throw(scope, &error);
    }
    let script = args.get(0).to_rust_string_lossy(scope);
    let entrypoint = args.get(1).to_rust_string_lossy(scope);
    let path = match serde_json::from_str(&args.get(2).to_rust_string_lossy(scope)) {
        Ok(path) => path,
        Err(error) => return loader_throw(scope, &format!("service RPC path is invalid: {error}")),
    };
    let call_args = args.get(3);
    let operation = if call_args.is_null() {
        crate::WorkerRpcOperation::Get { path }
    } else {
        crate::WorkerRpcOperation::Call {
            path,
            args: view_bytes(call_args).unwrap_or_default(),
        }
    };
    let props = view_bytes(args.get(4)).unwrap_or_default();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Service);
    let request = SvcRpcReq {
        generation: current_generation(scope),
        script,
        entrypoint,
        props,
        operation,
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

/// `__svc_call(script, url, method, body, headersJson, streamId, entrypoint,
/// propsSc)` -> Promise<json>.
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
    if let Err(error) = event_context(scope).charge_subrequest() {
        return loader_throw(scope, &error);
    }
    let script = args.get(0).to_rust_string_lossy(scope);
    let url = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let stream_arg = args.get(5);
    let entrypoint_arg = args.get(6);
    let props = view_bytes(args.get(7)).unwrap_or_default();
    let entrypoint = entrypoint_arg
        .is_string()
        .then(|| crate::WorkerFetchEntrypoint {
            name: entrypoint_arg.to_rust_string_lossy(scope),
            props,
        });
    let body = if stream_arg.is_number() {
        RequestBody::Stream(stream_arg.number_value(scope).unwrap_or(0.0) as u64)
    } else {
        let Some(bytes) = view_bytes(args.get(3)) else {
            return loader_throw(
                scope,
                "service binding: the request body is not a typed array",
            );
        };
        RequestBody::Bytes(bytes.into())
    };
    let headers: Vec<(String, String)> =
        match serde_json::from_str(&args.get(4).to_rust_string_lossy(scope)) {
            Ok(headers) => headers,
            Err(error) => {
                return loader_throw(
                    scope,
                    &format!(
                        "service binding: the request headers are not a name/value list: {error}"
                    ),
                )
            }
        };
    let body_guard = match body.stream_id() {
        Some(stream_id) => match current_context().transfer_body_stream(stream_id) {
            Some(guard) => guard,
            None => return loader_throw(scope, "service binding: the body stream is not owned"),
        },
        None => RequestBodyGuard::of(&body),
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    // Shares the Durable Object cancel registry and the id encoding
    // `attach_cancel_id` writes, so `__do_call_cancel` works unchanged for a
    // service call. Only a cancellable call registers: a caller with no
    // AbortSignal has nothing that can cancel it, and the router already ends
    // the call when the reply sender drops.
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
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Service);
    let request = SvcCallReq {
        cancel,
        generation: current_generation(scope),
        script,
        entrypoint,
        url,
        method,
        body,
        body_guard,
        headers,
        reply: tx,
    };
    let stream_service = http_stream_service();
    let id = asyncrt::enqueue(async move {
        let mut cancel_guard = cancel_guard;
        gated_channel_send(gate, &SVC_CALL_TX, request, "no service binding channel").await?;
        let result = match rx.await {
            Ok(Ok(response)) => encode_http_response(response, true, &stream_service),
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
        attach_cancel_id(scope, promise, request_id);
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
/// The largest `props` value one loaded entrypoint or Durable Object class can
/// carry, as structured-clone bytes. Workerd does not impose this bound, but
/// celld holds the value on a host job for the whole call. The harness shares
/// the caller's isolate and cannot prevent that unbounded host allocation, so
/// every op that accepts call props must enforce the limit.
const MAX_DYNAMIC_WORKER_PROPS_SIZE: usize = 1024 * 1024;

fn loader_props_bytes(value: v8::Local<v8::Value>) -> Result<Vec<u8>, String> {
    // RPC and facet calls use undefined for absent props. A different
    // non-typed value is a malformed transport argument, not empty props.
    let props = if value.is_undefined() {
        Vec::new()
    } else {
        view_bytes(value)
            .ok_or_else(|| "worker loader: the props are not a typed array".to_string())?
    };
    if props.len() > MAX_DYNAMIC_WORKER_PROPS_SIZE {
        return Err(format!(
            "Dynamic Worker props size ({} bytes) exceeds the maximum \
             allowed size of {MAX_DYNAMIC_WORKER_PROPS_SIZE} bytes.",
            props.len()
        ));
    }
    Ok(props)
}

#[derive(Clone)]
enum LoaderState {
    Loading,
    Ready(Arc<crate::pool::Slot>),
    Failed(Arc<str>),
}

#[derive(Clone, Eq, PartialEq)]
struct LoaderPrincipal {
    script: Arc<str>,
    generation: crate::generation::GenerationId,
}

#[derive(Clone, Eq, PartialEq)]
struct LoaderOwner {
    id: u64,
    principal: LoaderPrincipal,
}

impl LoaderOwner {
    fn fresh(script: &str, generation: crate::generation::GenerationId) -> Self {
        Self {
            id: LOADER_NEXT_OWNER.fetch_add(1, Ordering::Relaxed),
            principal: LoaderPrincipal {
                script: Arc::from(script),
                generation,
            },
        }
    }
}

#[doc(hidden)]
pub struct LoaderEntry {
    owner: LoaderOwner,
    state: tokio::sync::watch::Receiver<LoaderState>,
}

type LoaderRegistry = HashMap<u64, LoaderEntry>;
static LOADER_REGISTRY: OnceLock<std::sync::Mutex<LoaderRegistry>> = OnceLock::new();
static LOADER_NEXT_ID: AtomicU64 = AtomicU64::new(1);
static LOADER_NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

#[doc(hidden)]
pub fn loader_registry() -> &'static std::sync::Mutex<LoaderRegistry> {
    LOADER_REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Remove the registry references for every child of `owner`. The caller
/// chooses when to drop the returned states, because they can own V8 isolates
/// and no other isolate can be entered on that thread at destruction time.
fn take_loader_owner(owner: &LoaderOwner) -> Vec<tokio::sync::watch::Receiver<LoaderState>> {
    let mut registry = loader_registry().lock().unwrap();
    let ids: Vec<u64> = registry
        .iter()
        .filter_map(|(id, entry)| (&entry.owner == owner).then_some(*id))
        .collect();
    ids.into_iter()
        .filter_map(|id| registry.remove(&id).map(|entry| entry.state))
        .collect()
}

// Bound live Dynamic Workers so a runaway loader cannot exhaust isolates.
// Keep admission policy inside the registry: a caller-selected bound would let
// different entry points disagree about how much process capacity remains.
const MAX_LOADED_WORKERS: usize = 256;
// Reserve only the final process slot. A fixed fraction needlessly reduces
// single-script capacity without giving every competing principal a share.
const MAX_LOADED_WORKERS_PER_PRINCIPAL: usize = MAX_LOADED_WORKERS - 1;

/// Check both limits and install the live state under one registry lock. A
/// separate check and insert lets concurrent isolates exceed either limit.
fn admit_loaded_worker(
    owner: LoaderOwner,
    state: tokio::sync::watch::Receiver<LoaderState>,
) -> Result<u64, String> {
    let mut registry = loader_registry().lock().unwrap();
    let principal_limit = MAX_LOADED_WORKERS_PER_PRINCIPAL;
    let principal_count = registry
        .values()
        .filter(|entry| entry.owner.principal == owner.principal)
        .count();
    if principal_count >= principal_limit {
        return Err(format!(
            "worker loader: too many loaded workers for script \"{}\" in generation {} (limit {principal_limit})",
            owner.principal.script, owner.principal.generation
        ));
    }
    if registry.len() >= MAX_LOADED_WORKERS {
        return Err(format!(
            "worker loader: too many loaded workers (limit {MAX_LOADED_WORKERS})"
        ));
    }

    let id = LOADER_NEXT_ID.fetch_add(1, Ordering::Relaxed);
    registry.insert(id, LoaderEntry { owner, state });
    Ok(id)
}

fn loader_throw(scope: &mut v8::PinScope, message: &str) {
    let message = v8::String::new(scope, message).unwrap();
    let exception = v8::Exception::error(scope, message);
    scope.throw_exception(exception);
}

fn loader_invocation_limits(
    scope: &mut v8::PinScope,
    value: v8::Local<v8::Value>,
) -> Result<Option<ResourceLimits>, String> {
    let json = value.to_rust_string_lossy(scope);
    serde_json::from_str(&json)
        .map_err(|error| format!("worker loader: invalid invocation limits: {error}"))
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

/// `__loader_load(codeJson, wasm, outbound, outboundProps, envSc, envRoutes,
/// tailReporting)` -> stub id. Builds a WorkerConfig from the supplied modules
/// and registers its asynchronous load state. Compilation runs on Tokio's
/// blocking pool. Calls wait for that result and use the normal turn driver.
fn op_loader_load(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let generation = current_generation(scope);
    let code_json = args.get(0).to_rust_string_lossy(scope);
    let code: serde_json::Value = match serde_json::from_str(&code_json) {
        Ok(code) => code,
        Err(e) => return loader_throw(scope, &format!("worker loader: {e}")),
    };
    // These fields change the security or observability contract of the
    // loaded Worker. Ignoring one makes a Worker run without the contract its
    // caller requested, which is more dangerous than refusing the load. Keep
    // this check in the host op so both load() and the lazy get() path cross
    // the same enforcement boundary.
    if code.get("allowExperimental").is_some() {
        return loader_throw(
            scope,
            "worker loader: WorkerCode field `allowExperimental` is not supported",
        );
    }
    let limits = match code.get("limits") {
        Some(value) => match serde_json::from_value::<ResourceLimits>(value.clone()) {
            Ok(limits) => Some(limits),
            Err(error) => {
                return loader_throw(scope, &format!("worker loader: invalid limits: {error}"));
            }
        },
        None => None,
    };
    let tail_reporting = args.get(6).boolean_value(scope);
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
    // The environment crosses as structured-clone bytes. Service Binding
    // capabilities use a separate host-minted route table, so application
    // data can select only a route that the parent already supplied.
    let env_arg = args.get(4);
    let loader_env = if env_arg.is_undefined() {
        None
    } else {
        let Some(bytes) = view_bytes(env_arg) else {
            return loader_throw(scope, "worker loader: env is not a typed array");
        };
        let routes_arg = args.get(5);
        let Ok(route_entries) = v8::Local::<v8::Array>::try_from(routes_arg) else {
            return loader_throw(scope, "worker loader: env routes are not an array");
        };
        let mut routes = Vec::with_capacity(route_entries.length() as usize);
        let mut total_size = bytes.len();
        for index in 0..route_entries.length() {
            let Some(entry) = route_entries
                .get_index(scope, index)
                .and_then(|value| v8::Local::<v8::Array>::try_from(value).ok())
            else {
                return loader_throw(scope, "worker loader: malformed env capability route");
            };
            let Some(script_value) = entry.get_index(scope, 0).filter(|value| value.is_string())
            else {
                return loader_throw(scope, "worker loader: malformed env capability script");
            };
            let Some(entrypoint_value) = entry.get_index(scope, 1) else {
                return loader_throw(scope, "worker loader: malformed env capability entrypoint");
            };
            let entrypoint = if entrypoint_value.is_null_or_undefined() {
                None
            } else if entrypoint_value.is_string() {
                Some(entrypoint_value.to_rust_string_lossy(scope))
            } else {
                return loader_throw(scope, "worker loader: malformed env capability entrypoint");
            };
            let Some(props_value) = entry.get_index(scope, 2) else {
                return loader_throw(scope, "worker loader: malformed env capability props");
            };
            let Some(props) = view_bytes(props_value) else {
                return loader_throw(scope, "worker loader: env capability props are not bytes");
            };
            total_size = total_size.saturating_add(props.len());
            routes.push(LoaderEnvRoute {
                script: script_value.to_rust_string_lossy(scope),
                entrypoint,
                props,
            });
        }
        if total_size > MAX_DYNAMIC_WORKER_ENV_SIZE {
            return loader_throw(
                scope,
                &format!(
                    "Dynamic Worker env size ({total_size} bytes) exceeds the maximum \
                     allowed size of {MAX_DYNAMIC_WORKER_ENV_SIZE} bytes.",
                ),
            );
        }
        Some(LoaderEnv { bytes, routes })
    };
    // The harness removes the live Fetcher from the JSON and sends only the
    // route that its private WeakMap minted. This op supplies the generation,
    // so neither application data nor a stale child can select another graph.
    #[derive(serde::Deserialize)]
    struct BrokerRoute {
        script: String,
        entrypoint: Option<String>,
    }
    let outbound = args.get(2);
    let outbound_props = args.get(3);
    let egress = if outbound.is_undefined() {
        actor_runtime_state(scope).egress.clone()
    } else if outbound.is_null() {
        EgressPolicy::Deny
    } else {
        let route: BrokerRoute = match serde_json::from_str(&outbound.to_rust_string_lossy(scope)) {
            Ok(route) => route,
            Err(error) => {
                return loader_throw(
                    scope,
                    &format!("worker loader: malformed globalOutbound route: {error}"),
                )
            }
        };
        let props = match view_bytes(outbound_props) {
            Some(props) => props,
            None => {
                return loader_throw(
                    scope,
                    "worker loader: globalOutbound props are not a typed array",
                )
            }
        };
        let entrypoint = match route.entrypoint {
            Some(name) => Some(crate::WorkerFetchEntrypoint { name, props }),
            None if props.is_empty() => None,
            None => {
                return loader_throw(
                    scope,
                    "worker loader: globalOutbound props require an entrypoint",
                )
            }
        };
        EgressPolicy::Broker(OutboundBroker {
            generation,
            script: route.script,
            entrypoint,
        })
    };
    let owner = scope
        .get_slot::<LoaderOwner>()
        .expect("Worker isolate has a Loader owner")
        .clone();
    // Honor the WorkerCode's declared compatibility (workerd worker_compat
    // reads snake_case keys); Code Mode workers keep RPC on regardless.
    let mut compat = crate::worker_compat(&serde_json::json!({
        "compatibility_date": code.get("compatibilityDate"),
        "compatibility_flags": code.get("compatibilityFlags"),
    }));
    compat.js_rpc = true;
    let handle = match tokio::runtime::Handle::try_current() {
        Ok(handle) => handle,
        Err(error) => return loader_throw(scope, &format!("worker loader: {error}")),
    };
    let (loaded, state) = tokio::sync::watch::channel(LoaderState::Loading);
    let id = match admit_loaded_worker(owner, state) {
        Ok(id) => id,
        Err(error) => return loader_throw(scope, &error),
    };
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
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: String::new(),
            modules,
            compat,
        })
        .into_dynamic_worker()
        .with_main_module_name(main.to_string())
        .with_generation(generation)
        .with_egress(egress)
        .with_resource_limits(limits)
        .with_tail_reporting(tail_reporting)
        .with_loader_env(loader_env),
    );
    handle.spawn(async move {
        let state = match tokio::task::spawn_blocking(move || Worker::load_config(config)).await {
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

/// `__loader_fetch(id, url, method, body, headersJson, streamId, entrypoint,
/// propsSc, limitsJson, tailReporting)` -> Promise<json> or
/// [Promise<json>, Promise<json>].
/// The loaded-worker analog of `__svc_call`: it encodes the response the same
/// way and can return a separate report after the invocation finishes.
fn op_loader_fetch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let url = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let entrypoint = args.get(6);
    if !entrypoint.is_string() {
        return loader_throw(scope, "worker loader: the entrypoint is not a string");
    }
    let entrypoint = entrypoint.to_rust_string_lossy(scope);
    let props = match loader_props_bytes(args.get(7)) {
        Ok(props) => props,
        Err(error) => return loader_throw(scope, &error),
    };
    let invocation_limits = match loader_invocation_limits(scope, args.get(8)) {
        Ok(limits) => limits,
        Err(error) => return loader_throw(scope, &error),
    };
    // The Worker already stores its default handler, including the synthetic
    // missing-handler function for a class-only module. A named entrypoint
    // must instead use the harness dispatcher, so an absent name fails rather
    // than falling back to the default export. A props-carrying default fetch
    // also needs that dispatcher because the direct path cannot install props.
    let entrypoint =
        (entrypoint != "default" || !props.is_empty()).then_some(crate::WorkerFetchEntrypoint {
            name: entrypoint,
            props,
        });
    let stream_arg = args.get(5);
    let body = if stream_arg.is_number() {
        RequestBody::Stream(stream_arg.number_value(scope).unwrap_or(0.0) as u64)
    } else {
        let Some(bytes) = view_bytes(args.get(3)) else {
            return loader_throw(
                scope,
                "worker loader: the request body is not a typed array",
            );
        };
        RequestBody::Bytes(bytes.into())
    };
    let headers: Vec<(String, String)> =
        match serde_json::from_str(&args.get(4).to_rust_string_lossy(scope)) {
            Ok(headers) => headers,
            Err(error) => {
                return loader_throw(
                    scope,
                    &format!(
                        "worker loader: the request headers are not a name/value list: {error}"
                    ),
                )
            }
        };
    let want_tail_report = args.get(9).boolean_value(scope);
    let (tail_report, tail_receive) = if want_tail_report {
        let (send, receive) = tokio::sync::oneshot::channel();
        (Some(send), Some(receive))
    } else {
        (None, None)
    };
    let mut body_guard = match body.stream_id() {
        Some(stream_id) => match current_context().transfer_body_stream(stream_id) {
            Some(guard) => guard,
            None => return loader_throw(scope, "worker loader: the body stream is not owned"),
        },
        None => RequestBodyGuard::of(&body),
    };
    let loaded = loader_registry()
        .lock()
        .unwrap()
        .get(&id)
        .map(|entry| entry.state.clone());
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Service);
    let stream_service = http_stream_service();
    let async_id = asyncrt::enqueue(async move {
        // The child's own `IoContext` has an empty egress stack, so nothing
        // inside it gates what it sends onward. Holding the call until the
        // caller's writes are proven makes every effect of the loaded worker
        // trail that proof, as a service-binding call already does.
        await_egress_gate(gate).await?;
        let state = loaded.ok_or_else(|| "worker loader: unknown worker".to_string())?;
        let slot = loaded_worker_slot(state).await?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Fetch {
            queued_at: Instant::now(),
            entrypoint,
            invocation_limits,
            url,
            method,
            body,
            headers,
            // A loaded Worker receives an incoming request like a service
            // target does. The id selects the stream-aware construction path
            // and gives an abandoned request a lifecycle owner.
            request_id: Some(next_request_id()),
            tail_report,
            reply,
        };
        let driving = tokio::spawn(crate::runtime::drive(slot, job, None));
        match receive.await {
            Ok(Ok(response)) => {
                // The response proves that the loaded Worker installed its
                // request context. That context owns an unread body tail
                // through its waitUntil work.
                body_guard.disarm();
                encode_http_response(response, false, &stream_service)
            }
            Ok(Err(error)) => Err(format!("{error}")),
            Err(_) => match driving.await {
                Err(error) => Err(format!("loaded worker task died: {error}")),
                Ok(()) => Err("loaded worker dropped response".to_string()),
            },
        }
    });
    let response = promise_for(scope, async_id);
    let Some(tail_receive) = tail_receive else {
        rv.set(response);
        return;
    };
    let report_id = asyncrt::enqueue(async move {
        tail_receive
            .await
            .map_err(|error| format!("loaded worker dropped tail report: {error}"))
    });
    let report = promise_for(scope, report_id);
    let pair = v8::Array::new_with_elements(scope, &[response, report]);
    rv.set(pair.into());
}

/// `__loader_rpc(id, entrypoint, method, argsSc, propsSc, limitsJson)` ->
/// Promise<Uint8Array>. The loaded-worker analog of `__svc_rpc`: a
/// named-entrypoint method call whose args and result are V8 structured-clone
/// bytes. `propsSc` carries `getEntrypoint(name, {props})` to the callee's
/// `ctx.props` in the same encoding; it is empty for no props.
fn op_loader_rpc(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let entrypoint = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let call_args = view_bytes(args.get(3)).unwrap_or_default();
    let props = match loader_props_bytes(args.get(4)) {
        Ok(props) => props,
        Err(error) => return loader_throw(scope, &error),
    };
    let invocation_limits = match loader_invocation_limits(scope, args.get(5)) {
        Ok(limits) => limits,
        Err(error) => return loader_throw(scope, &error),
    };
    // The props ride on `WorkerJob::Rpc` for the whole call, so an unbounded
    // value is an unbounded per-call allocation in the host. The harness checks
    // nothing here: it runs in the calling worker's isolate beside the
    // application's code, so only this op can bound the value.
    let loaded = loader_registry()
        .lock()
        .unwrap()
        .get(&id)
        .map(|entry| entry.state.clone());
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Service);
    let async_id = asyncrt::enqueue(async move {
        // The same rule as `op_loader_fetch`: the call carries cell state.
        await_egress_gate(gate).await?;
        let state = loaded.ok_or_else(|| "worker loader: unknown worker".to_string())?;
        let slot = loaded_worker_slot(state).await?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Rpc {
            entrypoint,
            operation: crate::WorkerRpcOperation::Call {
                path: vec![method],
                args: call_args,
            },
            props,
            invocation_limits,
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

fn facet_scope(class_name: &str, parent_scope: &str, owner: &str, name: &str) -> String {
    use sha2::Digest as _;
    let mut digest = sha2::Sha256::new();
    digest.update(parent_scope.as_bytes());
    digest.update([0]);
    digest.update(owner.as_bytes());
    digest.update([0]);
    digest.update(name.as_bytes());
    let digest: [u8; 32] = digest.finalize().into();
    format!("{class_name}:{}", durable_object_id_hex(&digest))
}

/// Activate a facet's stream through the host loop, which owns the
/// replicator, and answer its database file and whether it was restored.
async fn open_facet_file(
    parent: &storage::StorageIdentity,
    name: &str,
) -> Result<(std::path::PathBuf, bool), String> {
    let mut names = parent.facet_path.clone();
    names.push(name.to_string());
    let Some(sender) = FACET_TX.get() else {
        // No host loop runs the replicator, so the file sits beside the
        // root's database and is named after it, as workerd names a facet
        // `<id>.<facet>.sqlite`.
        let stream = crate::engine_api::facet_cell(&parent.root_scope, &names);
        let facet = stream[parent.root_scope.len()..]
            .trim_start_matches('/')
            .replace('/', "-");
        let root = std::path::Path::new(&parent.root_path);
        let file = root.file_name().unwrap().to_string_lossy();
        return Ok((root.with_file_name(format!("{file}.{facet}.sqlite")), false));
    };
    let (reply, receive) = tokio::sync::oneshot::channel();
    let request = FacetReq::Open {
        root: parent.root_scope.clone(),
        epoch: parent.epoch,
        names,
        reply,
    };
    sender
        .send(request)
        .map_err(|_| "the facet channel closed".to_string())?;
    receive
        .await
        .map_err(|_| "the facet open was dropped".to_string())?
        .map_err(|error| format!("open the facet database: {error:#}"))
}

struct FacetStart {
    class_name: String,
    parent_scope: String,
    owner: String,
    name: String,
    id: String,
    props_sc: Vec<u8>,
    parent: storage::StorageIdentity,
}

/// A facet ready for one call, with what its reply must wait for.
struct LoadedFacet {
    slot: Arc<crate::pool::Slot>,
    scope: String,
    stream: String,
    epoch: u64,
}

impl LoadedFacet {
    /// A facet's reply leaves as workerd's facet output gate lets it: after
    /// every write the facet committed is durable on its own stream. That
    /// covers an earlier call's write this reply can reveal as well as the
    /// call's own, and an error reply as well as a result.
    async fn gate_reply(&self) -> Result<(), String> {
        prove_facet(self.stream.clone(), self.epoch)
            .await
            .map_err(|refusal| format!("facet reply: {refusal}"))
    }
}

/// The loader id of a class from the script's own `ctx.exports`. Loaded
/// Workers' ids start at 1.
const OWN_SCRIPT_LOADER: u64 = 0;

/// The isolate a facet's class runs in: a loaded Worker's, or, for a class
/// of the root's own script, the root's own, as workerd runs it.
enum FacetHost {
    Loaded(tokio::sync::watch::Receiver<LoaderState>),
    Own(Arc<crate::pool::Slot>),
}

impl FacetHost {
    /// Resolved in the op, while the root's turn names its slot.
    fn of(loader: u64) -> Result<Self, String> {
        if loader == OWN_SCRIPT_LOADER {
            return crate::pool::current_slot()
                .map(Self::Own)
                .ok_or_else(|| "facet: no isolate runs this turn".to_string());
        }
        loader_registry()
            .lock()
            .unwrap()
            .get(&loader)
            .map(|entry| Self::Loaded(entry.state.clone()))
            .ok_or_else(|| "worker loader: unknown worker".to_string())
    }

    async fn slot(self) -> Result<Arc<crate::pool::Slot>, String> {
        match self {
            Self::Loaded(loaded) => loaded_worker_slot(loaded).await,
            Self::Own(slot) => Ok(slot),
        }
    }
}

async fn prepare_facet(host: FacetHost, start: FacetStart) -> Result<LoadedFacet, String> {
    let slot = host.slot().await?;
    let file = open_facet_file(&start.parent, &start.name).await?;
    let mut names = start.parent.facet_path.clone();
    names.push(start.name.clone());
    let stream = crate::engine_api::facet_cell(&start.parent.root_scope, &names);
    let epoch = start.parent.epoch;
    let scope = facet_scope(
        &start.class_name,
        &start.parent_scope,
        &start.owner,
        &start.name,
    );
    slot.turn(|worker| {
        worker.own_embedded_cell(
            &scope,
            &start.parent,
            &start.name,
            &start.id,
            start.props_sc,
            file,
        )
    })
    .await
    .map_err(|error| format!("worker loader facet: {error}"))?;
    Ok(LoadedFacet {
        slot,
        scope,
        stream,
        epoch,
    })
}

fn op_facet_rpc(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let loader = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let class_name = args.get(1).to_rust_string_lossy(scope);
    let parent_scope = args.get(2).to_rust_string_lossy(scope);
    let owner = args.get(3).to_rust_string_lossy(scope);
    let name = args.get(4).to_rust_string_lossy(scope);
    let facet_id = args.get(5).to_rust_string_lossy(scope);
    let props_sc = match loader_props_bytes(args.get(6)) {
        Ok(props) => props,
        Err(error) => return loader_throw(scope, &error),
    };
    let method = args.get(7).to_rust_string_lossy(scope);
    let call_args = view_bytes(args.get(8)).unwrap_or_default();
    let parent = match storage::storage_identity(&parent_scope) {
        Ok(Some(parent)) => parent,
        Ok(None) => {
            return loader_throw(
                scope,
                "facets are available only inside a Durable Object event",
            )
        }
        Err(error) => return loader_throw(scope, &error.to_string()),
    };
    let host = FacetHost::of(loader);
    let trace = current_trace_context(scope);
    let async_id = asyncrt::enqueue(async move {
        let facet = prepare_facet(
            host?,
            FacetStart {
                class_name,
                parent_scope,
                owner,
                name,
                id: facet_id,
                props_sc,
                parent,
            },
        )
        .await?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::Rpc {
            request_id: None,
            scope: facet.scope.clone(),
            name: None,
            method,
            args: RpcData::V8(call_args.into()),
            reply,
        };
        let driving = tokio::spawn(crate::runtime::drive_cell(
            facet.slot.affiliate(),
            job,
            None,
            trace,
        ));
        let replied = receive.await;
        if replied.is_ok() {
            facet.gate_reply().await?;
        }
        match replied {
            Ok(Ok(outcome)) => match outcome.data {
                RpcData::V8(bytes) => {
                    facet.gate_reply().await?;
                    Ok(Vec::<u8>::from(bytes))
                }
                RpcData::Json(_) => Err("facet RPC answered JSON".to_string()),
            },
            Ok(Err(error)) => Err(format!("{error}")),
            Err(_) => match driving.await {
                Err(error) => Err(format!("facet RPC task died: {error}")),
                Ok(()) => Err("facet dropped RPC result".to_string()),
            },
        }
    });
    rv.set(promise_for(scope, async_id));
}

fn op_facet_fetch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let loader = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let class_name = args.get(1).to_rust_string_lossy(scope);
    let parent_scope = args.get(2).to_rust_string_lossy(scope);
    let owner = args.get(3).to_rust_string_lossy(scope);
    let name = args.get(4).to_rust_string_lossy(scope);
    let facet_id = args.get(5).to_rust_string_lossy(scope);
    let props_sc = match loader_props_bytes(args.get(6)) {
        Ok(props) => props,
        Err(error) => return loader_throw(scope, &error),
    };
    let url = args.get(7).to_rust_string_lossy(scope);
    let method = args.get(8).to_rust_string_lossy(scope);
    let stream_arg = args.get(11);
    let body = if stream_arg.is_number() {
        RequestBody::Stream(stream_arg.number_value(scope).unwrap_or(0.0) as u64)
    } else {
        let Some(bytes) = view_bytes(args.get(9)) else {
            return loader_throw(scope, "facet: the request body is not a typed array");
        };
        RequestBody::Bytes(bytes.into())
    };
    let headers: Vec<(String, String)> =
        match serde_json::from_str(&args.get(10).to_rust_string_lossy(scope)) {
            Ok(headers) => headers,
            Err(error) => return loader_throw(scope, &format!("facet headers: {error}")),
        };
    let mut body_guard = match body.stream_id() {
        Some(stream_id) => match current_context().transfer_body_stream(stream_id) {
            Some(guard) => guard,
            None => return loader_throw(scope, "facet: the body stream is not owned"),
        },
        None => RequestBodyGuard::of(&body),
    };
    let parent = match storage::storage_identity(&parent_scope) {
        Ok(Some(parent)) => parent,
        Ok(None) => {
            return loader_throw(
                scope,
                "facets are available only inside a Durable Object event",
            )
        }
        Err(error) => return loader_throw(scope, &error.to_string()),
    };
    let host = FacetHost::of(loader);
    let trace = current_trace_context(scope);
    let stream_service = http_stream_service();
    let async_id = asyncrt::enqueue(async move {
        let facet = prepare_facet(
            host?,
            FacetStart {
                class_name,
                parent_scope,
                owner,
                name,
                id: facet_id,
                props_sc,
                parent,
            },
        )
        .await?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::Fetch {
            request_id: None,
            scope: facet.scope.clone(),
            name: None,
            url,
            method,
            body,
            headers,
            reply,
            order: None,
        };
        let driving = tokio::spawn(crate::runtime::drive_cell(
            facet.slot.affiliate(),
            job,
            None,
            trace,
        ));
        let replied = receive.await;
        if replied.is_ok() {
            facet.gate_reply().await?;
        }
        match replied {
            Ok(Ok(response)) => {
                body_guard.disarm();
                facet.gate_reply().await?;
                encode_http_response(response, false, &stream_service)
            }
            Ok(Err(error)) => Err(format!("{error}")),
            Err(_) => match driving.await {
                Err(error) => Err(format!("facet fetch task died: {error}")),
                Ok(()) => Err("facet dropped fetch response".to_string()),
            },
        }
    });
    rv.set(promise_for(scope, async_id));
}

fn op_facet_abort(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let loader = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let class_name = args.get(1).to_rust_string_lossy(scope);
    let parent_scope = args.get(2).to_rust_string_lossy(scope);
    let owner = args.get(3).to_rust_string_lossy(scope);
    let name = args.get(4).to_rust_string_lossy(scope);
    let host = FacetHost::of(loader);
    let facet_scope = facet_scope(&class_name, &parent_scope, &owner, &name);
    let async_id = asyncrt::enqueue(async move {
        let slot = host?.slot().await?;
        slot.turn(|worker| worker.own_cell(&facet_scope, None))
            .await
            .map_err(|error| format!("abort facet: {error}"))?;
        Ok(Vec::new())
    });
    rv.set(promise_for(scope, async_id));
}

fn op_facet_delete(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let parent_scope = args.get(0).to_rust_string_lossy(scope);
    let name = args.get(1).to_rust_string_lossy(scope);
    let parent = match storage::storage_identity(&parent_scope) {
        Ok(Some(parent)) => parent,
        Ok(None) => {
            return loader_throw(
                scope,
                "facets are available only inside a Durable Object event",
            )
        }
        Err(error) => return loader_throw(scope, &error.to_string()),
    };
    if let Err(error) = storage::delete_legacy_facet(&parent, &name) {
        return loader_throw(scope, &format!("delete facet storage: {error}"));
    }
    let mut names = parent.facet_path.clone();
    names.push(name);
    let async_id = asyncrt::enqueue(async move {
        let Some(sender) = FACET_TX.get() else {
            let (path, _) = open_facet_file(&parent, names.last().unwrap()).await?;
            for suffix in ["", "-wal", "-shm"] {
                let mut file = path.clone().into_os_string();
                file.push(suffix);
                let _ = asyncrt::fs().remove_file(std::path::Path::new(&file));
            }
            return Ok(Vec::new());
        };
        let (reply, receive) = tokio::sync::oneshot::channel();
        let request = FacetReq::Delete {
            root: parent.root_scope,
            epoch: parent.epoch,
            names,
            reply,
        };
        sender
            .send(request)
            .map_err(|_| "the facet channel closed".to_string())?;
        receive
            .await
            .map_err(|_| "the facet delete was dropped".to_string())?
            .map_err(|error| format!("delete facet storage: {error:#}"))?;
        Ok(Vec::new())
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
    if let Some(entry) = loader_registry().lock().unwrap().remove(&id) {
        // This op runs while the parent isolate is entered. The load state can
        // own the loaded isolate, and V8 forbids dropping one isolate while a
        // different isolate is entered on the same thread. Hand the final
        // reference to the host scheduler so destruction happens after this
        // turn has left V8.
        asyncrt::op_handle().spawn(async move { drop(entry.state) });
    }
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
    // arg 4 is the held bytes; arg 6, when a number, is a host stream id for a
    // body that must not be collected here (a large or unbounded upload). The
    // owning cell reads that stream directly; only a cross-node hop collects it.
    let stream_arg = args.get(6);
    let body = if stream_arg.is_number() {
        RequestBody::Stream(stream_arg.number_value(scope).unwrap_or(0.0) as u64)
    } else {
        let Some(bytes) = view_bytes(args.get(4)) else {
            return loader_throw(
                scope,
                "durable object: the request body is not a typed array",
            );
        };
        RequestBody::Bytes(bytes.into())
    };
    let headers: Vec<(String, String)> =
        match serde_json::from_str(&args.get(5).to_rust_string_lossy(scope)) {
            Ok(headers) => headers,
            Err(error) => {
                return loader_throw(
                    scope,
                    &format!(
                        "durable object: the request headers are not a name/value list: {error}"
                    ),
                )
            }
        };
    let body_guard = match body.stream_id() {
        Some(stream_id) => match current_context().transfer_body_stream(stream_id) {
            Some(guard) => guard,
            None => return loader_throw(scope, "durable object: the body stream is not owned"),
        },
        None => RequestBodyGuard::of(&body),
    };
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
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::CellRpc);
    let request = DoCallReq {
        request_id: Some(request_id),
        cancel: Some(cancel),
        deliver_abort_to_handler: cancellable,
        scope: cell,
        name,
        url,
        method,
        body,
        body_guard,
        headers,
        reply: tx,
        order,
        parent: current_trace_context(scope),
    };
    let stream_service = http_stream_service();
    let id = asyncrt::enqueue(async move {
        gated_channel_send(gate, &DO_CALL_TX, request, "no proxy channel").await?;
        let result = match rx.await {
            Ok(Ok(response)) => encode_http_response(response, true, &stream_service),
            Ok(Err(e)) => Err(format!("{e}")),
            Err(e) => Err(format!("proxy dropped: {e}")),
        };
        cancel_guard.disarm();
        result
    });
    let p = promise_for(scope, id);
    if cancellable {
        attach_cancel_id(scope, p, request_id);
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

/// Allocate a process-wide signal identity. The structured-clone RPC envelope
/// carries this identity between isolates, and the registry carries the one
/// state transition that a snapshot cannot represent.
fn op_rpc_signal_new(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let signal = next_request_id();
    rpc_signals()
        .lock()
        .unwrap()
        .insert(signal, RpcSignalState::Live(HashMap::new()));
    let signal = v8::String::new(scope, &request_id_string(signal)).unwrap();
    rv.set(signal.into());
}

fn op_rpc_signal_abort(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let Some(signal) = parse_request_id(&args.get(0).to_rust_string_lossy(scope)) else {
        return;
    };
    let reason = view_bytes(args.get(1)).unwrap_or_default();
    let subscribers = {
        let mut signals = rpc_signals().lock().unwrap();
        let Some(state) = signals.get_mut(&signal) else {
            return;
        };
        match std::mem::replace(state, RpcSignalState::Aborted(reason.clone())) {
            RpcSignalState::Live(subscribers) => subscribers,
            RpcSignalState::Aborted(prior) => {
                *state = RpcSignalState::Aborted(prior);
                return;
            }
        }
    };
    for subscriber in subscribers.into_values() {
        let _ = subscriber.send(Some(reason.clone()));
    }
}

fn op_rpc_signal_release(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let Some(signal) = parse_request_id(&args.get(0).to_rust_string_lossy(scope)) else {
        return;
    };
    let state = rpc_signals().lock().unwrap().remove(&signal);
    if let Some(RpcSignalState::Live(subscribers)) = state {
        for subscriber in subscribers.into_values() {
            let _ = subscriber.send(None);
        }
    }
}

fn op_rpc_signal_unsubscribe(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let Some(signal) = parse_request_id(&args.get(0).to_rust_string_lossy(scope)) else {
        return;
    };
    let Some(subscription) = parse_request_id(&args.get(1).to_rust_string_lossy(scope)) else {
        return;
    };
    if let Some(RpcSignalState::Live(subscribers)) = rpc_signals().lock().unwrap().get_mut(&signal)
    {
        subscribers.remove(&subscription);
    }
}

fn op_rpc_signal_poll(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Some(signal) = parse_request_id(&args.get(0).to_rust_string_lossy(scope)) else {
        return;
    };
    let signals = rpc_signals().lock().unwrap();
    match signals.get(&signal) {
        Some(RpcSignalState::Aborted(reason)) => {
            rv.set(bytes_value(scope, reason.clone()));
        }
        Some(RpcSignalState::Live(_)) => {}
        None => rv.set(v8::null(scope).into()),
    }
}

fn op_rpc_signal_wait(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let signal_text = args.get(0).to_rust_string_lossy(scope);
    let Some(signal) = parse_request_id(&signal_text) else {
        return loader_throw(scope, "invalid RPC AbortSignal id");
    };
    let subscription = next_request_id();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let immediate = {
        let mut signals = rpc_signals().lock().unwrap();
        match signals.get_mut(&signal) {
            Some(RpcSignalState::Live(subscribers)) => {
                subscribers.insert(subscription, tx);
                None
            }
            Some(RpcSignalState::Aborted(reason)) => Some(Some(reason.clone())),
            None => Some(None),
        }
    };
    // The subscription must receive aborts while the handler or `waitUntil`
    // work is live, but a listener alone is not pending application work. An
    // ordinary handler op pins every successful Durable Object event until
    // the source aborts, including events unrelated to the retained signal.
    let id = asyncrt::enqueue_unrefed(async move {
        let _guard = RpcSignalSubscriptionGuard {
            signal,
            subscription,
        };
        let reason = match immediate {
            Some(reason) => reason,
            None => rx.await.unwrap_or(None),
        };
        Ok(reason.unwrap_or_default())
    });
    let promise = promise_for(scope, id);
    if let Some(object) = promise.to_object(scope) {
        let key = v8::String::new(scope, "__celldRpcSignalSubscription").unwrap();
        let value = v8::String::new(scope, &request_id_string(subscription)).unwrap();
        object.set(scope, key.into(), value.into());
    }
    rv.set(promise);
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
    let args = RpcData::V8(view_bytes(args.get(3)).unwrap_or_default().into());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::CellRpc);
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
            Ok(Ok(RpcData::V8(bytes))) => Ok(Vec::<u8>::from(bytes)),
            Ok(Ok(RpcData::Json(_))) => Err("RPC answered JSON to a structured-clone call".into()),
            Ok(Err(e)) => Err(format!("{e}")),
            Err(e) => Err(format!("RPC proxy dropped: {e}")),
        }
    });
    let p = promise_for(scope, id);
    rv.set(p);
}

/// Outbound `fetch` — the op behind the harness's `fetch()`. Resolves to a JSON
/// `{status, body, headers}` string.
fn op_fetch(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let context = event_context(scope);
    if let Err(error) = context.charge_subrequest() {
        return loader_throw(scope, &error);
    }
    let egress = actor_runtime_state(scope).egress.clone();
    // A loaded worker with globalOutbound: null has no ambient egress: it must
    // reach the world through `env` capabilities. Message matches workerd.
    if egress == EgressPolicy::Deny {
        return loader_throw(
            scope,
            "This worker is not permitted to access the internet via global \
             functions like fetch(). It must use capabilities (such as \
             bindings in 'env') to talk to the outside world.",
        );
    }
    let method = args.get(0).to_rust_string_lossy(scope);
    let url = args.get(1).to_rust_string_lossy(scope);
    // A body is a typed array, as it is for `__svc_call`, `__do_call` and
    // `__loader_fetch`. `None` and an empty body are different requests — one
    // carries no `Content-Length` — so the absent case stays distinct.
    let body_arg = args.get(2);
    let stream_arg = args.get(5);
    let body: Option<RequestBody> = if body_arg.is_undefined() || body_arg.is_null() {
        None
    } else if stream_arg.is_number() {
        Some(RequestBody::Stream(
            stream_arg.number_value(scope).unwrap_or(0.0) as u64,
        ))
    } else {
        match view_bytes(body_arg) {
            Some(bytes) => Some(RequestBody::Bytes(bytes.into())),
            // Answering an unreadable argument with `None` sent the request
            // with no body at all: the peer saw something the Worker never
            // asked for, and nothing threw and nothing was logged.
            None => return loader_throw(scope, "fetch: the request body is not a typed array"),
        }
    };
    let raw_headers = args.get(3).to_rust_string_lossy(scope);
    let mut headers: Vec<(String, String)> = match serde_json::from_str(&raw_headers) {
        Ok(headers) => headers,
        // Same failure as the body: dropping the headers silently sent an
        // unauthenticated, unrouted request in place of the real one.
        Err(error) => {
            return loader_throw(
                scope,
                &format!("fetch: the request headers are not a name/value list: {error}"),
            )
        }
    };
    let redirect = args.get(4).to_rust_string_lossy(scope);
    // The harness passes `true` only when the caller supplied an AbortSignal.
    // An absent argument is `false`, so a direct caller gets the uncancellable
    // request it asked for.
    let cancellable = args.get(6).boolean_value(scope);
    let mut body_guard = match body.as_ref().and_then(RequestBody::stream_id) {
        Some(stream_id) => match current_context().transfer_body_stream(stream_id) {
            Some(guard) => guard,
            None => return loader_throw(scope, "fetch: the body stream is not owned"),
        },
        None => RequestBodyGuard(None),
    };
    // Request validates this value in the harness, but the native op is a
    // separate trust boundary. Keep this match exhaustive so a future direct
    // caller cannot turn an unknown mode into redirect-following behavior.
    match redirect.as_str() {
        "follow" | "manual" | "error" => {}
        other => return loader_throw(scope, &format!("fetch: unknown redirect mode {other}")),
    }
    if let EgressPolicy::Broker(broker) = egress {
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
        let (tx, rx) = tokio::sync::oneshot::channel();
        let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Fetch);
        let request = SvcCallReq {
            cancel,
            generation: broker.generation,
            script: broker.script,
            entrypoint: broker.entrypoint,
            url,
            method,
            body: body.unwrap_or_else(|| RequestBody::Bytes(Vec::new().into())),
            body_guard,
            headers,
            reply: tx,
        };
        let stream_service = http_stream_service();
        let id = asyncrt::enqueue(async move {
            let mut cancel_guard = cancel_guard;
            gated_channel_send(gate, &SVC_CALL_TX, request, "no service binding channel").await?;
            let result = match rx.await {
                Ok(Ok(response)) => encode_http_response(response, true, &stream_service),
                Ok(Err(error)) => Err(format!("{error}")),
                Err(error) => Err(format!("globalOutbound Fetcher dropped: {error}")),
            };
            if let Some(cancel_guard) = cancel_guard.as_mut() {
                cancel_guard.disarm();
            }
            result
        });
        let promise = promise_for(scope, id);
        if let Some(request_id) = request_id {
            attach_cancel_id(scope, promise, request_id);
        }
        rv.set(promise);
        return;
    }
    // Select the client only for direct egress: a broker must not initialize
    // an unused network client and its TLS stack.
    let client = match redirect.as_str() {
        "follow" if context.subrequest_limit.is_some() => context.redirect_counting_client(),
        "follow" => HTTP.with(|client| client.clone()),
        "manual" => HTTP_MANUAL.with(|client| client.clone()),
        "error" => HTTP_ERROR.with(|client| client.clone()),
        _ => unreachable!("the redirect mode was validated above"),
    };
    // The creating context, read here while JS is still running: the op
    // future resolves on whatever worker polls it, far from any CPED.
    let trace = current_trace_context(scope);
    // The child's ids are minted before the request leaves so the
    // traceparent header carries them: whatever this fetch reaches can
    // join the trace celld is part of.
    let child = trace.as_ref().map(crate::telemetry::child_context);
    if let Some(child) = child.as_ref() {
        if !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("traceparent"))
        {
            headers.push(("traceparent".into(), crate::telemetry::traceparent(child)));
        }
    }
    let recording = trace.and_then(crate::telemetry::TraceContext::recording_ids);
    let child_recording = child.and_then(crate::telemetry::TraceContext::recording_ids);
    let span_url = recording.map(|_| url.clone());
    // Whatever this request can reveal must be durable before it leaves: a
    // third party that has acted on a write celld then loses cannot be told to
    // un-act. That covers a write this handler made and a value it only read,
    // because the third party cannot tell the two apart.
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Fetch);
    // Shares the cancel registry, the id encoding and the `__do_call_cancel`
    // op that a Durable Object call and a service binding call already use.
    // Only a caller that supplied an AbortSignal registers: without one there
    // is nothing that can cancel, and the op then costs no registry entry.
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
    let stream_service = http_stream_service();
    let id = asyncrt::enqueue(async move {
        // Held, not disarmed: this op has several early returns, and the guard
        // removes its registry entry on every one of them when it drops. The
        // cancel it sends then reaches a receiver this block already consumed,
        // which is the same no-op as a disarm.
        let _cancel_guard = cancel_guard;
        let span_started = recording.map(|_| crate::telemetry::now_unix_us());
        let mut span = recording.zip(child_recording).map(|(parent, child)| {
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
        // reqwest omits Content-Length for an empty Vec, which collapses the
        // wire representation of `Some([])` into the representation of
        // `None`. Install the zero length here unless the Worker supplied a
        // framing header, so the body distinction survives the HTTP client.
        let empty_body_needs_length = body
            .as_ref()
            .is_some_and(|body| matches!(body, RequestBody::Bytes(bytes) if bytes.is_empty()))
            && !headers.iter().any(|(name, _)| {
                name.eq_ignore_ascii_case("content-length")
                    || name.eq_ignore_ascii_case("transfer-encoding")
            });
        for (name, value) in headers {
            rb = rb.header(name, value);
        }
        if let Some(body) = body {
            rb = match body {
                RequestBody::Bytes(bytes) => rb.body(bytes),
                RequestBody::Stream(stream_id) => match body_guard.take_stream(stream_id) {
                    Ok(stream) => rb.body(reqwest::Body::wrap_stream(stream)),
                    Err(error) => {
                        finish(false, None, Some(error.clone()));
                        return Err(error);
                    }
                },
            };
        }
        if empty_body_needs_length {
            rb = rb.header(reqwest::header::CONTENT_LENGTH, 0);
        }
        // The cancellable window is the request itself: connect, send, and wait
        // for the response head. Dropping the send future here closes the
        // connection, which is what tells the upstream to stop producing. After
        // this point the response body is a registered stream, and
        // `__http_stream_cancel` is what ends it.
        let sent = match cancel {
            Some(cancel) => crate::asyncrt::select_biased! {
                "a cancel that is ready wins a tie, so an aborted request never \
                 hands back a response and never leaves a body stream nobody reads";
                _ = cancel => None,
                result = rb.send() => Some(result),
            },
            None => Some(rb.send().await),
        };
        let Some(sent) = sent else {
            let error = "fetch: the request was aborted".to_string();
            finish(false, None, Some(error.clone()));
            return Err(error);
        };
        match sent {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let headers = resp
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            // A header value is decoded as UTF-8 with
                            // replacement, as workerd decodes it.
                            String::from_utf8_lossy(value.as_bytes()).into_owned(),
                        )
                    })
                    .collect::<Vec<_>>();
                let Some(stream_id) =
                    stream_service.register_source(HttpStreamSource::Response(resp))
                else {
                    let error = format!("fetch: {HTTP_STREAM_REGISTRATION_CLOSED}");
                    finish(false, Some(status), Some(error.clone()));
                    return Err(error);
                };
                finish(true, Some(status), None);
                Ok(serde_json::json!({
                    "status": status, "streamId": stream_id, "headers": headers,
                })
                .to_string())
            }
            Err(e) => {
                let error = fetch_request_error(&e);
                finish(false, None, Some(error.clone()));
                Err(error)
            }
        }
    });
    let p = promise_for(scope, id);
    if let Some(request_id) = request_id {
        attach_cancel_id(scope, p, request_id);
    }
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
        match serde_json::from_str(&args.get(3).to_rust_string_lossy(scope)) {
            Ok(headers) => headers,
            Err(error) => {
                return loader_throw(
                    scope,
                    &format!("assets: the request headers are not a name/value list: {error}"),
                )
            }
        };
    let generation = current_generation(scope);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let sent = ASSET_CALL_TX.get().is_some_and(|sender| {
        sender
            .send(AssetCallReq {
                generation,
                script,
                url,
                method,
                headers,
                reply: tx,
            })
            .is_ok()
    });
    let stream_service = http_stream_service();
    let id = asyncrt::enqueue(async move {
        if !sent {
            return Err("no asset resolver channel".to_string());
        }
        match rx.await {
            Ok(Ok(response)) => encode_http_response(response, false, &stream_service),
            Ok(Err(error)) => Err(format!("asset fetch: {error}")),
            Err(error) => Err(format!("asset resolver dropped: {error}")),
        }
    });
    rv.set(promise_for(scope, id));
}

fn tee_http_stream(
    service: &Arc<HttpStreamService>,
    source: HttpTransferredStream,
) -> Result<(u64, u64), &'static str> {
    let (ids, pump) = prepare_http_stream_tee(service, source)?;
    asyncrt::op_handle().spawn(pump);
    Ok(ids)
}

fn op_http_stream_read(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let service = http_stream_service();
    let read = http_stream_read(service, stream_id);
    let id = asyncrt::enqueue(async move {
        match read.await? {
            Some(bytes) => Ok(asyncrt::OpOut::Bytes(bytes)),
            None => Ok(asyncrt::OpOut::Str(HTTP_STREAM_DONE.into())),
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
    let service = http_stream_service();
    let source = service.checkout_transfer(stream_id, None).ok();
    let Some(source) = source else {
        let message = v8::String::new(scope, "response stream is no longer available").unwrap();
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
        return;
    };
    let ids = match tee_http_stream(&service, source) {
        Ok(ids) => ids,
        Err(error) => {
            let message = v8::String::new(scope, error).unwrap();
            let exception = v8::Exception::type_error(scope, message);
            scope.throw_exception(exception);
            return;
        }
    };
    current_context().replace_body_stream(stream_id, &service, ids);
    let json = serde_json::to_string(&ids).unwrap();
    rv.set(v8::String::new(scope, &json).unwrap().into());
}

fn op_http_stream_cancel(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let stream_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    http_stream_service().cancel_source(stream_id);
}

fn op_response_stream_create(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let (writer, receiver) = tokio::sync::mpsc::channel(1);
    let (finished, _) = tokio::sync::watch::channel(false);
    let Some(stream_id) = http_stream_service()
        .register_response_pair(receiver, ResponseStreamWriter::new(writer, finished))
    else {
        let message = v8::String::new(scope, HTTP_STREAM_REGISTRATION_CLOSED).unwrap();
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
        return;
    };
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
    // A response is gated once, when it is released, but a body that streams
    // keeps producing after that. A later chunk can carry a commit that
    // another event made after the release, and that chunk leaves the process
    // as surely as the response head did. So each chunk takes its own ticket,
    // sampled here while JS still runs, exactly as `fetch` does.
    //
    // The pump in `__bridgeResponseStream` awaits each write before it asks
    // the producer for the next chunk, so one chunk is in flight at a time and
    // a held chunk cannot overtake a released one. The ticket is taken before
    // `response_stream_write` reserves the stream's write activity, so a long
    // wait for a proof holds no lease on the stream. The stream's inactivity
    // expiry still runs during the wait, so a proof that never lands ends the
    // body with an error rather than releasing the chunk, which is the
    // direction this gate exists to fail in.
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::Response);
    let service = http_stream_service();
    let id = asyncrt::enqueue(async move {
        await_egress_gate(gate).await?;
        response_stream_write(service, stream_id, bytes).await?;
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
    let (writer, mut finished) = http_stream_service()
        .writer_close_watch(stream_id)
        .map(|(writer, finished)| (Some(writer), Some(finished)))
        .unwrap_or((None, None));
    let id = asyncrt::enqueue(async move {
        let cancelled = match (writer, finished.as_mut()) {
            (Some(writer), Some(finished)) => crate::asyncrt::select_biased! {
                "stream completion wins a tie so a completed response is not reported as cancelled";
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
    let close = response_stream_close(http_stream_service(), stream_id, error);
    let id = asyncrt::enqueue(async move {
        close.await?;
        Ok(String::new())
    });
    rv.set(promise_for(scope, id));
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
        crate::asyncrt::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(ms)) => {}
            _ = &mut cancel_rx => {}
        }
        timer_cancels().lock().unwrap().remove(&timer_id);
        Ok(String::new())
    });
    let p = promise_for(scope, id);
    rv.set(p);
}

/// `__io_context_id(operationDriver = false)` — return the live context id.
///
/// The default returns the resource origin. A true argument returns the
/// operation driver, so a nested block can refuse a retained stale context.
fn op_io_context_id(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let driver = args.get(0).boolean_value(scope);
    let id = if driver {
        operation_continuation_id(scope).filter(|id| {
            actor_runtime_state(scope)
                .io_context(*id)
                .is_some_and(|context| context.accepts_handed_ops())
        })
    } else {
        current_reaction_io_context(scope).and_then(|context| context.continuation_id())
    };
    let id = id.map(|id| id.to_string()).unwrap_or_default();
    rv.set(v8::String::new(scope, &id).unwrap().into());
}

fn return_gate_acquisition(
    scope: &mut v8::PinScope,
    mut rv: v8::ReturnValue<v8::Value>,
    event: celld_logic::gate::EventId,
    owner: Option<u64>,
    promise: v8::Local<v8::Value>,
) {
    let event = v8::String::new(scope, &event.to_string()).unwrap();
    let owner =
        v8::String::new(scope, &owner.map(|id| id.to_string()).unwrap_or_default()).unwrap();
    let values = [event.into(), owner.into(), promise];
    rv.set(v8::Array::new_with_elements(scope, &values).into());
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
    rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let event = NEXT_GATE_EVENT.fetch_add(1, Ordering::Relaxed);
    let contexts = current_reaction_io_context(scope).zip(
        operation_continuation_id(scope).and_then(|id| actor_runtime_state(scope).io_context(id)),
    );
    let Some((context, owner_context)) = contexts else {
        // A promise can retain user code after its event has retired. It must
        // not borrow the context of whichever event happens to run the stale
        // reaction's checkpoint.
        let promise = rejected_promise(scope, RETIRED_INPUT_GATE);
        return_gate_acquisition(scope, rv, event, None, promise);
        return;
    };
    // The harness installs the driver before acquisition. A missing driver
    // is rejected above instead of transferring a stale block to its origin.
    let owner = owner_context.continuation_id();
    // Queued until taken or refused, either way once: the guard counts the
    // event out when the future ends, however it ends.
    struct Queued {
        cell: String,
        owner: Option<u64>,
    }
    impl Queued {
        fn new(cell: String, owner: Option<u64>) -> Self {
            if let Some(owner) = owner {
                *cell_gates()
                    .lock()
                    .unwrap()
                    .entry(cell.clone())
                    .or_default()
                    .queued
                    .entry(owner)
                    .or_default() += 1;
                GATE_ENGAGEMENTS.fetch_add(1, Ordering::Relaxed);
            }
            Self { cell, owner }
        }
    }
    impl Drop for Queued {
        fn drop(&mut self) {
            let Some(owner) = self.owner else {
                return;
            };
            let mut gates = cell_gates().lock().unwrap();
            let remove = if let Some(gate) = gates.get_mut(&self.cell) {
                if let Some(count) = gate.queued.get_mut(&owner) {
                    *count -= 1;
                    if *count == 0 {
                        gate.queued.remove(&owner);
                    }
                }
                gate.is_unused()
            } else {
                false
            };
            if remove {
                gates.remove(&self.cell);
            }
            drop(gates);
            GATE_ENGAGEMENTS.fetch_sub(1, Ordering::Relaxed);
        }
    }
    let mut origin = if Arc::ptr_eq(&context, &owner_context) {
        None
    } else {
        let Some(claim) = context.claim_cross_entry_gate() else {
            let promise = rejected_promise(scope, RETIRED_INPUT_GATE);
            return_gate_acquisition(scope, rv, event, None, promise);
            return;
        };
        Some(claim)
    };
    let id = match acquire_cell_gate(&owner_context, &cell, event, owner, &mut origin) {
        CellGateAcquisition::Acquired => {
            asyncrt::enqueue_io_context(async move { Ok(String::new()) })
        }
        CellGateAcquisition::Waiting {
            mut gate,
            mut retirement,
        } => {
            let queued = Queued::new(cell.clone(), owner);
            let context = Arc::downgrade(&owner_context);
            asyncrt::enqueue_io_context(async move {
                let _queued = queued;
                loop {
                    let outcome = crate::asyncrt::select_biased! {
                        "a settled gate failure remains visible when its event retires at the same instant";
                        outcome = &mut gate => Some(outcome),
                        retired = retirement.wait_for(|retired| *retired) => {
                            let _ = retired;
                            None
                        }
                    };
                    let Some(outcome) = outcome else {
                        return Err(RETIRED_INPUT_GATE.to_string());
                    };
                    match outcome {
                        Ok(Ok(())) => {}
                        // The block ahead failed and reset the actor. This event
                        // was sent to that old actor, so it cannot compete for the
                        // fresh gate.
                        Ok(Err(failure)) => return Err(failure),
                        Err(_) => {
                            return Err("cell stopped while waiting for its input gate".to_string())
                        }
                    }
                    let Some(context) = context.upgrade() else {
                        return Err(RETIRED_INPUT_GATE.to_string());
                    };
                    match acquire_cell_gate(&context, &cell, event, owner, &mut origin) {
                        CellGateAcquisition::Acquired => {
                            return Ok(String::new());
                        }
                        CellGateAcquisition::Waiting {
                            gate: next_gate,
                            retirement: next_retirement,
                        } => {
                            gate = next_gate;
                            retirement = next_retirement;
                        }
                        CellGateAcquisition::Retired => return Err(RETIRED_INPUT_GATE.to_string()),
                    }
                }
            })
        }
        CellGateAcquisition::Retired => {
            let promise = rejected_promise(scope, RETIRED_INPUT_GATE);
            return_gate_acquisition(scope, rv, event, None, promise);
            return;
        }
    };
    let promise = promise_for(scope, id);
    return_gate_acquisition(scope, rv, event, owner, promise);
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

/// `__gate_release(scope, event, owner)` — the block is over. The next
/// delivery point to run takes whatever queued behind it.
fn op_gate_release(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let event = args.get(1).number_value(scope).unwrap_or_default() as celld_logic::gate::EventId;
    let owner = args.get(2).to_rust_string_lossy(scope).parse::<u64>().ok();
    // A fourth argument means the block failed and the actor was reset, so
    // what queued behind it is refused with that reason rather than
    // delivered to a cell whose state is gone.
    let failure = args.get(3);
    let outcome = if failure.is_null_or_undefined() {
        Ok(())
    } else {
        Err(failure.to_rust_string_lossy(scope))
    };
    let Some(owner) = owner else {
        return;
    };
    let active = current_context();
    let context = if active.continuation_id() == Some(owner) {
        Some(active)
    } else {
        actor_runtime_state(scope).io_context(owner)
    };
    let Some(context) = context else {
        return;
    };
    if !context.release_input_gate(&cell, event) {
        return;
    }
    release_cell_gate(&cell, event, outcome);
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
    let cell = cell_state(scope)?;
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

mod container;
mod crypto;
mod html_rewriter;
mod tcp;
#[cfg(celld_internal_tests)]
pub use tcp::test_extra_tls_root;
mod zlib;

fn op_actor_abort(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let Some(context) = current_reaction_io_context(scope) else {
        return;
    };
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
        context_id: context.continuation_id(),
    });
    scope.terminate_execution();
}

fn op_process_exit(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let code = args.get(0).integer_value(scope).unwrap_or(0);
    let Some(context) = current_reaction_or_untracked_io_context(scope) else {
        tracing::warn!(
            code,
            "process.exit called without an active request; ignoring"
        );
        return;
    };
    if context.depth() == 0 {
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
        context_id: context.continuation_id(),
    });
    scope.terminate_execution();
}

fn op_log(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let mut msg: Vec<String> = (0..args.length())
        .map(|i| args.get(i).to_rust_string_lossy(scope))
        .collect();
    let level = if msg.len() >= 2
        && matches!(
            msg.first().map(String::as_str),
            Some("debug" | "error" | "info" | "log" | "warn")
        ) {
        msg.remove(0)
    } else {
        "log".to_string()
    };
    let body = msg.join(" ");
    if let Some(context) = current_reaction_or_untracked_io_context(scope) {
        context.record_tail_log(&level, &body);
    }
    let displayed = match level.as_str() {
        "error" => format!("ERROR {body}"),
        "warn" => format!("WARN {body}"),
        _ => body,
    };
    // Correlated by CPED, so a continuation logging after an await — or
    // another entry's continuation running in this turn's checkpoint —
    // lands on the trace that owns it, not on whoever holds the isolate.
    if crate::telemetry::active() {
        if let Some(ids) =
            current_trace_context(scope).and_then(crate::telemetry::TraceContext::recording_ids)
        {
            crate::telemetry::record_log(crate::telemetry::Log {
                trace_id: Some(ids.trace_id),
                span_id: Some(ids.span_id),
                time_unix_us: crate::telemetry::now_unix_us(),
                body: displayed.clone(),
            });
        }
    }
    tracing::info!(target: "cell_console", "{}", displayed);
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
#[cfg(celld_internal_tests)]
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
#[cfg(celld_internal_tests)]
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
#[cfg(celld_internal_tests)]
fn op_test_gc(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    scope.request_garbage_collection_for_testing(v8::GarbageCollectionType::Full);
}

#[cfg(celld_internal_tests)]
thread_local! {
    static FAIL_NEXT_WORKFLOW_EVENT_CONSUMPTION: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static FAIL_NEXT_WORKFLOW_META_CREATION: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static FAIL_NEXT_WORKFLOW_TERMINAL_ALARM_SET: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static FAIL_NEXT_QUEUE_DLQ_ACCEPT: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static QUEUE_DLQ_ACCEPT_CALLS: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
    static QUEUE_METRICS_MATERIALIZED: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static QUEUE_PRODUCER_GROUPS: std::cell::RefCell<Vec<(usize, usize)>> = const {
        std::cell::RefCell::new(Vec::new())
    };
    static QUEUE_PRODUCER_WRITE_POSITIONS: std::cell::RefCell<Vec<Option<u64>>> = const {
        std::cell::RefCell::new(Vec::new())
    };
    static QUEUE_REARM_OBSERVED: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static QUEUE_REARM_POSITIVE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static QUEUE_REARM_BOUND_VIOLATED: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static QUEUE_SETTLEMENT_POLICY_OBSERVED: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static QUEUE_LEASE_LOOKUP_PLANS: std::cell::RefCell<Vec<String>> = const {
        std::cell::RefCell::new(Vec::new())
    };
    static KV_LIST_PLANS: std::cell::RefCell<Vec<String>> = const {
        std::cell::RefCell::new(Vec::new())
    };
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn fail_next_workflow_event_consumption_for_test() {
    FAIL_NEXT_WORKFLOW_EVENT_CONSUMPTION.with(|fail| fail.set(true));
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn fail_next_workflow_meta_creation_for_test() {
    FAIL_NEXT_WORKFLOW_META_CREATION.with(|fail| fail.set(true));
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn fail_next_workflow_terminal_alarm_set_for_test() {
    FAIL_NEXT_WORKFLOW_TERMINAL_ALARM_SET.with(|fail| fail.set(true));
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn fail_next_queue_dlq_accept_for_test() {
    FAIL_NEXT_QUEUE_DLQ_ACCEPT.with(|fail| fail.set(true));
}

#[cfg(celld_internal_tests)]
pub fn reset_queue_dlq_accept_calls_for_test() {
    QUEUE_DLQ_ACCEPT_CALLS.with(|calls| calls.set(0));
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn queue_dlq_accept_calls_for_test() -> usize {
    QUEUE_DLQ_ACCEPT_CALLS.with(std::cell::Cell::get)
}

#[cfg(celld_internal_tests)]
pub fn reset_queue_hot_path_observations_for_test() {
    QUEUE_METRICS_MATERIALIZED.with(|observed| observed.set(false));
    QUEUE_REARM_OBSERVED.with(|observed| observed.set(false));
    QUEUE_REARM_POSITIVE.with(|observed| observed.set(false));
    QUEUE_REARM_BOUND_VIOLATED.with(|violated| violated.set(false));
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn queue_hot_path_observations_for_test() -> (bool, bool, bool) {
    let materialized = QUEUE_METRICS_MATERIALIZED.with(std::cell::Cell::get);
    let bounded = QUEUE_REARM_OBSERVED.with(std::cell::Cell::get)
        && QUEUE_REARM_POSITIVE.with(std::cell::Cell::get)
        && !QUEUE_REARM_BOUND_VIOLATED.with(std::cell::Cell::get);
    let violated = QUEUE_REARM_BOUND_VIOLATED.with(std::cell::Cell::get);
    (materialized, bounded, violated)
}

#[cfg(celld_internal_tests)]
pub fn reset_queue_producer_groups_for_test() {
    QUEUE_PRODUCER_GROUPS.with(|groups| groups.borrow_mut().clear());
    QUEUE_PRODUCER_WRITE_POSITIONS.with(|positions| positions.borrow_mut().clear());
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn queue_producer_groups_for_test() -> Vec<(usize, usize)> {
    QUEUE_PRODUCER_GROUPS.with(|groups| groups.borrow().clone())
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn queue_producer_write_positions_for_test() -> Vec<Option<u64>> {
    QUEUE_PRODUCER_WRITE_POSITIONS.with(|positions| positions.borrow().clone())
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn reset_queue_lease_lookup_plans_for_test() {
    QUEUE_LEASE_LOOKUP_PLANS.with(|plans| plans.borrow_mut().clear());
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn queue_lease_lookup_plans_for_test() -> Vec<String> {
    QUEUE_LEASE_LOOKUP_PLANS.with(|plans| plans.borrow().clone())
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn reset_kv_list_plans_for_test() {
    KV_LIST_PLANS.with(|plans| plans.borrow_mut().clear());
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn kv_list_plans_for_test() -> Vec<String> {
    KV_LIST_PLANS.with(|plans| plans.borrow().clone())
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn reset_queue_settlement_policy_observation_for_test() {
    QUEUE_SETTLEMENT_POLICY_OBSERVED.with(|observed| observed.set(false));
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn queue_settlement_policy_observed_for_test() -> bool {
    QUEUE_SETTLEMENT_POLICY_OBSERVED.with(std::cell::Cell::get)
}

#[cfg(celld_internal_tests)]
fn op_test_workflow_event_consumed(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let fail = FAIL_NEXT_WORKFLOW_EVENT_CONSUMPTION.with(|fail| fail.replace(false));
    if fail {
        throw_storage_error(
            scope,
            "workflow event consumption",
            "injected failure after workflow event delete",
        );
    }
}

#[cfg(celld_internal_tests)]
fn op_test_workflow_meta_created(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let fail = FAIL_NEXT_WORKFLOW_META_CREATION.with(|fail| fail.replace(false));
    if fail {
        throw_storage_error(
            scope,
            "workflow creation",
            "injected failure after workflow metadata write",
        );
    }
}

#[cfg(celld_internal_tests)]
fn op_test_workflow_terminal_alarm_set(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let fail = FAIL_NEXT_WORKFLOW_TERMINAL_ALARM_SET.with(|fail| fail.replace(false));
    if fail {
        throw_storage_error(
            scope,
            "workflow terminal alarm installation",
            "injected failure after workflow terminal alarm installation",
        );
    }
}

#[cfg(celld_internal_tests)]
fn op_test_queue_dlq_accepted(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    QUEUE_DLQ_ACCEPT_CALLS.with(|calls| calls.set(calls.get() + 1));
    let fail = FAIL_NEXT_QUEUE_DLQ_ACCEPT.with(|fail| fail.replace(false));
    if fail {
        throw_storage_error(
            scope,
            "Queue DLQ transfer",
            "injected failure after the target accepted the message",
        );
    }
}

#[cfg(celld_internal_tests)]
fn op_test_queue_metrics_materialized(
    _scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    QUEUE_METRICS_MATERIALIZED.with(|observed| observed.set(true));
}

#[cfg(celld_internal_tests)]
fn op_test_queue_producer_group(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let calls = args.get(0).integer_value(scope).unwrap_or(0).max(0) as usize;
    let messages = args.get(1).integer_value(scope).unwrap_or(0).max(0) as usize;
    QUEUE_PRODUCER_GROUPS.with(|groups| groups.borrow_mut().push((calls, messages)));
}

#[cfg(celld_internal_tests)]
fn op_test_queue_lease_lookup_plan(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let plan = args.get(0).to_rust_string_lossy(scope);
    QUEUE_LEASE_LOOKUP_PLANS.with(|plans| plans.borrow_mut().push(plan));
}

#[cfg(celld_internal_tests)]
fn op_test_kv_list_plan(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let plan = args.get(0).to_rust_string_lossy(scope);
    KV_LIST_PLANS.with(|plans| plans.borrow_mut().push(plan));
}

#[cfg(celld_internal_tests)]
fn op_test_queue_rearm_bounded(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    QUEUE_REARM_OBSERVED.with(|observed| observed.set(true));
    QUEUE_REARM_POSITIVE.with(|observed| observed.set(observed.get() || args.get(1).is_true()));
    if !args.get(0).boolean_value(scope) {
        QUEUE_REARM_BOUND_VIOLATED.with(|violated| violated.set(true));
    }
}
// `celld r2` writes an object a binding then reads, so the operator command
// calls the very functions the ops call rather than a second implementation
// of the object record.
pub(crate) mod r2_ops;
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
    // The nine keys are fixed, so they come from the constant table rather
    // than from nine fresh `v8::String`s per `new URL(...)`.
    //
    // A set can still fail — a throwing setter on `Object.prototype` reaches
    // this plain object — and a URL record missing a component is worse than
    // no record at all, so the first refusal stops the build.
    let port = u.port().map(|p| p.to_string()).unwrap_or_default();
    let fields: [(&'static v8::OneByteConst, &str); 9] = [
        (&v8_strings::URL_PROTOCOL, u.scheme()),
        (&v8_strings::URL_USERNAME, u.username()),
        (&v8_strings::URL_PASSWORD, u.password().unwrap_or("")),
        (&v8_strings::URL_HOST, u.host_str().unwrap_or("")),
        (&v8_strings::URL_PORT, &port),
        (&v8_strings::URL_PATHNAME, u.path()),
        (&v8_strings::URL_SEARCH, u.query().unwrap_or("")),
        (&v8_strings::URL_HASH, u.fragment().unwrap_or("")),
        (&v8_strings::URL_HREF, u.as_str()),
    ];
    for (k, v) in fields {
        let key = static_key(scope, k);
        let val = v8::String::new(scope, v).unwrap();
        match o.set(scope, key.into(), val.into()) {
            Some(true) => {}
            // The setter threw. Its exception is already pending, and it names
            // the real cause, so it propagates unchanged. Throwing a generic
            // TypeError here would overwrite it and hide that cause.
            None => return,
            // A refusal that does not throw — a non-writable inherited data
            // property, in sloppy mode — leaves no pending exception, so this
            // path has to raise one of its own.
            Some(false) => {
                let name = key.to_rust_string_lossy(scope);
                let msg = v8::String::new(
                    scope,
                    &format!("Invalid URL: the {name} property of the URL record was refused"),
                )
                .unwrap();
                let exc = v8::Exception::type_error(scope, msg);
                scope.throw_exception(exc);
                return;
            }
        }
    }
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
    use base64::engine::{GeneralPurpose, GeneralPurposeConfig};
    use base64::Engine;
    let input = args.get(0).to_rust_string_lossy(scope);
    // https://infra.spec.whatwg.org/#forgiving-base64-decode
    //
    // Only the five ASCII whitespace characters are ignored. `is_whitespace`
    // covers the Unicode White_Space property, so it also stripped U+00A0 and
    // U+3000 and made `atob("YQ==\u{a0}")` decode, which Workerd rejects.
    let cleaned: Vec<u8> = input
        .bytes()
        .filter(|b| !matches!(b, b'\t' | b'\n' | b'\x0c' | b'\r' | b' '))
        .collect();
    let mut data = cleaned.as_slice();
    if data.len().is_multiple_of(4) {
        data = data
            .strip_suffix(b"==")
            .or_else(|| data.strip_suffix(b"="))
            .unwrap_or(data);
    }
    // Padding is removed above, so the decoder must now reject every `=` that
    // is left. `RequireNone` is the mode that does this; `Indifferent` would
    // accept the malformed partial padding of `atob("YQ=")`, which Workerd
    // rejects. `allow_trailing_bits` is what makes the decode forgiving: the
    // spec discards the unused trailing bits of an unpadded group even when
    // they are not zero, so `atob("YR==")` returns "a" instead of throwing.
    const FORGIVING: GeneralPurpose = GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        GeneralPurposeConfig::new()
            .with_decode_padding_mode(base64::engine::DecodePaddingMode::RequireNone)
            .with_decode_allow_trailing_bits(true),
    );
    match FORGIVING.decode(data) {
        Ok(bytes) => {
            // The public `atob()` wrapper omits this argument and keeps the
            // required binary-string result. The KV operator protocol asks
            // for a typed view, so a 25 MiB value does not become 25 million
            // JavaScript numbers before the cell can store it.
            if args.get(1).boolean_value(scope) {
                rv.set(bytes_value(scope, bytes));
                return;
            }
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
// around every promise reaction. No promise hooks are installed. A stateless
// isolate that never touches AsyncLocalStorage leaves the frame absent; a cell
// event still carries the separate native context token described below.

#[derive(Default)]
struct IoEventFrame {
    wait_until: Vec<v8::Global<v8::Promise>>,
    arm_gates: Vec<ArmGateRx>,
}

#[derive(Default)]
struct IoEventState {
    frames: Vec<IoEventFrame>,
    /// Registrations made by active `waitUntil` work after the handler's
    /// frame ended. The driver folds each batch into a new aggregate before
    /// it retires the event, so one callback can extend the same lifetime.
    late_wait_until: Vec<v8::Global<v8::Promise>>,
    /// True from the top-level frame's first non-empty drain until the driver
    /// observes that drain settled with no later registrations. Keeping this
    /// state on the request prevents a foreign event in the same isolate from
    /// lending or taking away registration authority.
    wait_until_active: bool,
    ended_arm_gates: Vec<ArmGateRx>,
    arm_gates_sealed: bool,
}

/// The writable directory subset of Workerd's per-request virtual filesystem.
///
/// The first compatibility slice stores directories only. Keeping the tree on
/// `IoContext` is the isolation mechanism: async continuations for one request
/// share it, while a concurrent or later request receives a different tree.
/// No path can escape into the host filesystem.
struct RequestVfs {
    nodes: HashMap<String, VfsNode>,
}

/// Keep the node kind in the tree, even while the first slice can only create
/// directories. Deno's VFS uses the same entry-typed shape; a path-only set
/// would make a later file node indistinguishable from its parent directory.
enum VfsNode {
    Directory { writable: bool },
}

/// The read-only `/bundle` projection of a worker's own modules.
///
/// This is isolate state, not request state. The bytes are immutable and
/// identical for every request the isolate serves, so building the tree per
/// request would repeat the work and re-copy every module body. `BundleFs`
/// below keeps one `Arc` and hands a clone to each reader.
struct BundleTree {
    nodes: HashMap<String, BundleNode>,
}

/// A projected node. A file owns `Bytes` so a read is a refcount bump rather
/// than a copy, and so a wasm module's bytes are shared with the config
/// instead of duplicated.
enum BundleNode {
    Directory,
    File(bytes::Bytes),
}

/// The bundle tree and the config it projects, materialized on first use.
///
/// Workerd builds `/bundle` through a lazy directory for the same reason (see
/// `getBundleDirectory` in `src/workerd/io/bundle-fs.c++`): a worker that never
/// calls `node:fs` must not pay to walk its module list or to copy a module
/// body into the filesystem. Holding the `Arc<WorkerConfig>` keeps the source
/// alive, so materialization does not need its own copy of a module name.
struct BundleFs {
    config: Arc<WorkerConfig>,
    tree: OnceLock<Arc<BundleTree>>,
}

impl BundleFs {
    /// Materialize once, then share. Returns an `Arc` clone so a caller can
    /// read without holding an isolate borrow across the read.
    fn tree(&self) -> Arc<BundleTree> {
        self.tree
            .get_or_init(|| Arc::new(BundleTree::project(&self.config)))
            .clone()
    }
}

impl BundleTree {
    /// Project every bundle module into `/bundle`, keyed by its name as a path.
    ///
    /// A module name is a path, not a flat key: workerd parses it against
    /// `file:///`, so `a/esModule` becomes `/bundle/a/esModule` and creates the
    /// intervening directory. The entry module is projected under the name it
    /// compiles as, matching workerd's `mainScriptName` entry.
    fn project(config: &WorkerConfig) -> Self {
        let mut nodes = HashMap::new();
        nodes.insert("/bundle".to_string(), BundleNode::Directory);
        let entry = std::iter::once((
            ENTRY_MODULE_NAME,
            bytes::Bytes::copy_from_slice(config.src.as_bytes()),
        ));
        let modules = config.modules.iter().map(|(name, source)| {
            let bytes = match source {
                // A text or ES module body is a `String` in the config, so the
                // projection copies it once per isolate. A wasm body is already
                // `Bytes`, so it is shared rather than copied.
                ModuleSource::Text(source) | ModuleSource::EsModule(source) => {
                    bytes::Bytes::copy_from_slice(source.as_bytes())
                }
                ModuleSource::Wasm(bytes) => bytes.clone(),
            };
            (name.as_str(), bytes)
        });
        for (name, bytes) in entry.chain(modules) {
            // A leading or doubled separator would otherwise create an empty
            // segment, which no path can address.
            let segments: Vec<&str> = name.split('/').filter(|part| !part.is_empty()).collect();
            let Some((file, directories)) = segments.split_last() else {
                continue;
            };
            let mut path = String::from("/bundle");
            for directory in directories {
                path.push('/');
                path.push_str(directory);
                nodes.entry(path.clone()).or_insert(BundleNode::Directory);
            }
            path.push('/');
            path.push_str(file);
            nodes.insert(path, BundleNode::File(bytes));
        }
        Self { nodes }
    }
}

enum VfsMkdirResult {
    Created(Option<String>),
    Error(&'static str),
}

impl Default for RequestVfs {
    fn default() -> Self {
        Self {
            nodes: [
                ("/", false),
                ("/bundle", false),
                ("/dev", false),
                ("/tmp", true),
            ]
            .into_iter()
            .map(|(path, writable)| (path.to_string(), VfsNode::Directory { writable }))
            .collect(),
        }
    }
}

struct InputGateClaims {
    /// Set before the turn owner releases its final `IoContext` owner. The
    /// watch wakes its pending gate acquisition even when the promise
    /// reaction that requested the gate belongs to another event.
    retired: bool,
    held: HashMap<celld_logic::gate::EventId, String>,
    retirement: tokio::sync::watch::Sender<bool>,
}

impl Default for InputGateClaims {
    fn default() -> Self {
        let (retirement, _) = tokio::sync::watch::channel(false);
        Self {
            retired: false,
            held: HashMap::new(),
            retirement,
        }
    }
}

impl RequestVfs {
    fn stat(&self, path: &str) -> Option<bool> {
        self.nodes.get(path).map(|node| match node {
            VfsNode::Directory { writable } => *writable,
        })
    }

    fn mkdir(&mut self, path: &str, recursive: bool) -> VfsMkdirResult {
        if path != "/tmp" && !path.starts_with("/tmp/") {
            return VfsMkdirResult::Error("EPERM");
        }
        if self.stat(path).is_some() {
            return VfsMkdirResult::Created(None);
        }
        if recursive {
            let mut current = "/tmp".to_string();
            let mut first_created = None;
            for segment in path.trim_start_matches("/tmp/").split('/') {
                current.push('/');
                current.push_str(segment);
                if self
                    .nodes
                    .insert(current.clone(), VfsNode::Directory { writable: true })
                    .is_none()
                    && first_created.is_none()
                {
                    first_created = Some(current.clone());
                }
            }
            return VfsMkdirResult::Created(first_created);
        }
        let parent =
            path.rsplit_once('/').map_or(
                "/",
                |(parent, _)| {
                    if parent.is_empty() {
                        "/"
                    } else {
                        parent
                    }
                },
            );
        if self.stat(parent).is_none() {
            return VfsMkdirResult::Error("ENOENT");
        }
        self.nodes
            .insert(path.to_string(), VfsNode::Directory { writable: true });
        VfsMkdirResult::Created(None)
    }
}

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
/// would forbid that. The isolate keeps one narrow weak lookup from a CPED
/// continuation token to this context, because a reaction can run during a
/// different event's checkpoint. That lookup owns no context, operation, or
/// resolver, and `IoContext::drop` removes it. It therefore attributes the
/// running reaction without rebuilding the pump's lifetime-owning `idmap`.
/// A cross-entry input gate is the narrow exception: it claims the originating
/// context so the event and its resources stay active until the callback ends.
/// The gate releases that claim when the block ends. The interiors are
/// `Mutex`; only the isolate's holder reaches them.
pub struct IoContext {
    /// A process-wide identity captured by V8 with each promise reaction.
    /// The process-wide cell-gate map also uses it, so another isolate must
    /// not reuse it. The isolate registry holds only a `Weak`, so a
    /// continuation does not extend the request lifetime by itself. A
    /// cross-entry input gate adds an explicit claim that prevents this
    /// context's `InFlight` from retiring while its callback still runs.
    continuation: Option<(u64, Weak<ActorRuntimeState>)>,
    /// This caller's delivery order for each cell it has called. See
    /// `CallOrder` — it is here rather than in a process-wide map because
    /// nothing outside this caller ever reads it.
    pub call_chains: Mutex<CallChains>,
    /// Event frames, innermost last. A frame collects the `waitUntil`
    /// promises registered while it is the innermost one.
    ///
    /// Nesting is per-request and strictly LIFO — a service-binding
    /// dispatch, an RPC entry, or a DO construction pushes its own frame so
    /// its `waitUntil` binds to that dispatch, matching workerd.
    events: Mutex<IoEventState>,
    /// Workerd gives every request an empty, memory-backed `/tmp`. Directory
    /// state belongs here so it follows the request across promise turns and
    /// disappears with the request instead of leaking across isolate reuse.
    /// The tree stays unallocated when the application does not use `node:fs`.
    vfs: OnceLock<Mutex<RequestVfs>>,
    /// Isolate-polled sockets this request opened. Dropping the request's
    /// pending ops aborts everything the isolate is waiting on, but a socket
    /// is a host-side resource that abort cannot reach: its connector task
    /// exits only when the isolate stops reading. So the request closes its
    /// own sockets, and [`IoContext::drop`] is what makes that unforgettable.
    ///
    /// Flat rather than a stack. Only sockets that belong to no cell land
    /// here, and one `IoContext` is one request, so there is nothing to nest:
    /// a service-binding dispatch or a Durable Object call gets a context of
    /// its own rather than a frame inside this one.
    sockets: Mutex<Vec<u64>>,
    /// HTMLRewriter instances this request created. Each holds a parked
    /// parser thread; freeing them at retirement is what reclaims the
    /// thread when a transform is abandoned unread — nothing else can,
    /// because the pending event promise roots the output stream.
    rewriters: Mutex<Vec<u64>>,
    /// Outbound TCP sockets this event opened. A socket cannot outlive
    /// its event: retirement drops the registry entries, which closes
    /// the connections.
    tcp_sockets: Mutex<Vec<u64>>,
    /// Input-gate holds and pending acquisitions for this event.
    ///
    /// The event owns the cell and host id together, and retirement wakes a
    /// pending acquisition. A timeout or cancellation can therefore abandon
    /// only its own hold and cannot resume after its event has gone away.
    input_gates: Mutex<InputGateClaims>,
    /// Blocks created by this event but run during another event's turn.
    /// These claims keep this event's native operations and resources alive;
    /// their watch wakes the drive when the final block ends.
    cross_entry_gates: CrossEntryGateClaims,
    /// Host-backed request bodies this handler owns. A subrequest transfers
    /// an id out before its target takes ownership, so only one request can
    /// reclaim an unread tail.
    body_streams: Mutex<HashMap<u64, HttpStreamClaim>>,
    /// Output-gate capture. While a `webSocketMessage` runs, the frames it
    /// sends are collected here instead of reaching the wire, so the shell
    /// can hold them until the message's write is durable. A stack, so a
    /// nested dispatch keeps its frames apart from the outer one's.
    ///
    /// On the event and not on the thread, because an event outlives the
    /// turn that began it: capture starts in one turn and is taken in a
    /// later one, which tokio may run on a different worker.
    ws_capture: Mutex<Vec<websocket::WsCapture>>,
    /// What each running event's outbound effects gate against, innermost
    /// last. An outbound effect raised during the event consults this: if the
    /// handler has advanced the position, the effect waits for the output gate
    /// before it leaves the process.
    ///
    /// Empty for stateless Worker code, which owns no cell and gates
    /// nothing.
    egress: Mutex<Vec<EgressFrame>>,
    /// Ops another event's turn enqueued for this event, waiting for this
    /// event's driver to take them. See `adopt`: a continuation that resumes
    /// inside a foreign turn still enqueues through that turn, and only the
    /// driver of the event it belongs to may poll and cancel its work.
    ///
    /// `None` once this event retired: the mailbox is closed, and a hand-off
    /// that arrives after that is refused rather than parked. A parked op no
    /// driver will ever take is a `Global<PromiseResolver>` held for the life
    /// of the isolate, which is the leak `InFlight::abandon` exists to
    /// prevent; refusing lets the handing turn drop the resolver instead,
    /// while it still owns the isolate.
    handed: Mutex<Option<Vec<HandedOp>>>,
    /// How many ops `handed` holds, readable without taking its lock.
    ///
    /// Every pass of a driver's wake loop asks whether a foreign turn left it
    /// an op, and for all but a few passes in a cell's life the answer is no.
    /// The lock is uncontended, so this saves only the lock itself, but it
    /// pays that back on a loop that turns once per operation the event
    /// completes.
    handed_len: AtomicUsize,
    /// Wakes this event's driver when `handed` grows or a pending entrypoint
    /// starts. Both changes can make the driver's current wait obsolete.
    handed_wake: tokio::sync::Notify,
    /// IDs of nested `WorkerEntrypoint` calls that have not settled. The
    /// driver consumes these IDs only when the request has no native work, so
    /// it can reject the calls instead of timing out the enclosing event.
    pending_events: Mutex<HashSet<u64>>,
    /// The invocation-local subrequest budget. The count lives with the
    /// request context, so two concurrent calls into one loaded isolate never
    /// spend one another's allowance.
    subrequest_limit: Option<u32>,
    subrequests: AtomicUsize,
    /// A request-scoped client counts redirects against this request. A weak
    /// capture avoids a client/context cycle, and one client retains the
    /// connection pool across all fetches in the invocation.
    redirect_client: OnceLock<reqwest::Client>,
    cpu_limit_nanos: Option<u64>,
    cpu_used_nanos: AtomicU64,
    cpu_turn: Mutex<Option<CpuTurnStart>>,
    /// Whether this event records console calls for a Dynamic Worker tail.
    tail_reporting: bool,
    tail_logs: Mutex<TailLogCapture>,
}

/// An op enqueued by one event's continuation during another event's turn: the
/// promise id, the work, and its complete lifetime classification.
type HandedOp = (u64, asyncrt::OpFuture, asyncrt::OpLifetime);

impl IoContext {
    fn record_tail_log(&self, level: &str, body: &str) {
        if !self.tail_reporting {
            return;
        }
        let mut capture = self.tail_logs.lock().unwrap();
        if capture.full {
            return;
        }
        let separator = usize::from(!capture.records.is_empty());
        // A serialized string cannot be shorter than its UTF-8 input. This
        // rejects an obviously oversized call without allocating another
        // buffer or scanning it for JSON escapes.
        let minimum_bytes = capture
            .serialized_bytes
            .saturating_add(separator)
            .saturating_add(body.len())
            .saturating_add(2);
        if minimum_bytes > TAIL_LOG_LIMIT_BYTES {
            capture.full = true;
            return;
        }
        let record = TailLog {
            timestamp: crate::telemetry::now_unix_us() / 1_000,
            level: level.to_string(),
            message: vec![body.to_string()],
        };
        let record_bytes = serialized_json_bytes(&record);
        let next_bytes = capture
            .serialized_bytes
            .saturating_add(separator)
            .saturating_add(record_bytes);
        if next_bytes.saturating_add(2) > TAIL_LOG_LIMIT_BYTES {
            capture.full = true;
            return;
        }
        capture.serialized_bytes = next_bytes;
        capture.records.push(record);
    }

    fn take_tail_logs(&self) -> Vec<TailLog> {
        std::mem::take(&mut *self.tail_logs.lock().unwrap()).records
    }

    /// The continuation id this context is registered under, which names
    /// the event to the cell's gate. `None` for a context V8 never captures.
    fn continuation_id(&self) -> Option<u64> {
        self.continuation.as_ref().map(|(id, _)| *id)
    }

    fn cpu_limit_ms(&self) -> Option<u32> {
        self.cpu_limit_nanos
            .and_then(|limit| u32::try_from(limit / 1_000_000).ok())
    }

    fn redirect_counting_client(self: &Arc<Self>) -> reqwest::Client {
        self.redirect_client
            .get_or_init(|| {
                let context = Arc::downgrade(self);
                reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                        if attempt.previous().len() >= 10 {
                            return attempt.error("too many redirects");
                        }
                        if let Some(context) = context.upgrade() {
                            if let Err(error) = context.charge_subrequest() {
                                return attempt.error(RedirectSubrequestLimit(error));
                            }
                        }
                        attempt.follow()
                    }))
                    .build()
                    .expect("build the redirect-counting HTTP client")
            })
            .clone()
    }

    /// Spend one subrequest from this invocation before the operation leaves
    /// the isolate. The failed attempt is counted too, so repeated catches
    /// cannot retry an operation that already exhausted the budget.
    fn charge_subrequest(&self) -> Result<(), String> {
        let Some(limit) = self.subrequest_limit else {
            return Ok(());
        };
        let used = self.subrequests.fetch_add(1, Ordering::Relaxed) + 1;
        if used > limit as usize {
            return Err(format!("Worker exceeded subrequest limit of {limit}"));
        }
        Ok(())
    }

    fn begin_cpu_turn(&self) {
        if self.cpu_limit_nanos.is_none() {
            return;
        }
        *self.cpu_turn.lock().unwrap() = Some(CpuTurnStart {
            thread_nanos: thread_cpu_nanos(),
            wall: Instant::now(),
        });
    }

    fn remaining_cpu_budget(&self) -> Option<Duration> {
        self.cpu_limit_nanos.map(|limit| {
            Duration::from_nanos(limit.saturating_sub(self.cpu_used_nanos.load(Ordering::Relaxed)))
        })
    }

    fn finish_cpu_turn(&self) -> Result<(), String> {
        let Some(limit) = self.cpu_limit_nanos else {
            return Ok(());
        };
        let Some(started) = self.cpu_turn.lock().unwrap().take() else {
            return Ok(());
        };
        let elapsed = started
            .thread_nanos
            .zip(thread_cpu_nanos())
            .map(|(started, finished)| finished.saturating_sub(started))
            .unwrap_or_else(|| started.wall.elapsed().as_nanos() as u64);
        let used = self.cpu_used_nanos.fetch_add(elapsed, Ordering::Relaxed) + elapsed;
        if used > limit {
            return Err(format!(
                "Worker exceeded CPU limit of {} ms",
                limit / 1_000_000
            ));
        }
        Ok(())
    }

    #[allow(clippy::new_ret_no_self, clippy::new_without_default)]
    #[doc(hidden)]
    pub fn new() -> Arc<Self> {
        Self::with_resource_limits(None)
    }

    fn with_resource_limits(limits: Option<ResourceLimits>) -> Arc<Self> {
        Self::with_options(limits, false)
    }

    #[cfg(all(test, celld_internal_tests))]
    fn with_tail_reporting(tail_reporting: bool) -> Arc<Self> {
        Self::with_options(None, tail_reporting)
    }

    fn with_options(limits: Option<ResourceLimits>, tail_reporting: bool) -> Arc<Self> {
        Arc::new(Self {
            continuation: None,
            call_chains: Mutex::new(CallChains::default()),
            events: Mutex::new(IoEventState::default()),
            vfs: OnceLock::new(),
            sockets: Mutex::new(Vec::new()),
            rewriters: Mutex::new(Vec::new()),
            tcp_sockets: Mutex::new(Vec::new()),
            input_gates: Mutex::new(InputGateClaims::default()),
            cross_entry_gates: CrossEntryGateClaims::default(),
            body_streams: Mutex::new(HashMap::new()),
            ws_capture: Mutex::new(Vec::new()),
            egress: Mutex::new(Vec::new()),
            handed: Mutex::new(Some(Vec::new())),
            handed_len: AtomicUsize::new(0),
            handed_wake: tokio::sync::Notify::new(),
            pending_events: Mutex::new(HashSet::new()),
            subrequest_limit: limits.and_then(|limits| limits.sub_requests),
            subrequests: AtomicUsize::new(0),
            redirect_client: OnceLock::new(),
            cpu_limit_nanos: limits
                .and_then(|limits| limits.cpu_ms)
                .map(|milliseconds| u64::from(milliseconds).saturating_mul(1_000_000)),
            cpu_used_nanos: AtomicU64::new(0),
            cpu_turn: Mutex::new(None),
            tail_reporting,
            tail_logs: Mutex::new(TailLogCapture::default()),
        })
    }

    fn tracked(runtime_state: &Arc<ActorRuntimeState>) -> Arc<Self> {
        let id = allocate_io_context_id();
        let context = Arc::new(Self {
            continuation: Some((id, Arc::downgrade(runtime_state))),
            call_chains: Mutex::new(CallChains::default()),
            events: Mutex::new(IoEventState::default()),
            vfs: OnceLock::new(),
            sockets: Mutex::new(Vec::new()),
            rewriters: Mutex::new(Vec::new()),
            tcp_sockets: Mutex::new(Vec::new()),
            input_gates: Mutex::new(InputGateClaims::default()),
            cross_entry_gates: CrossEntryGateClaims::default(),
            body_streams: Mutex::new(HashMap::new()),
            ws_capture: Mutex::new(Vec::new()),
            egress: Mutex::new(Vec::new()),
            handed: Mutex::new(Some(Vec::new())),
            handed_len: AtomicUsize::new(0),
            handed_wake: tokio::sync::Notify::new(),
            pending_events: Mutex::new(HashSet::new()),
            subrequest_limit: runtime_state
                .resource_limits
                .and_then(|limits| limits.sub_requests),
            subrequests: AtomicUsize::new(0),
            redirect_client: OnceLock::new(),
            cpu_limit_nanos: runtime_state
                .resource_limits
                .and_then(|limits| limits.cpu_ms)
                .map(|milliseconds| u64::from(milliseconds).saturating_mul(1_000_000)),
            cpu_used_nanos: AtomicU64::new(0),
            cpu_turn: Mutex::new(None),
            // Tracked contexts drive cell and RPC events. Dynamic Worker tail
            // reports currently belong only to fetch jobs, whose untracked
            // context is constructed from the report sender in `begin`.
            tail_reporting: false,
            tail_logs: Mutex::new(TailLogCapture::default()),
        });
        runtime_state
            .io_contexts
            .lock()
            .unwrap()
            .insert(id, Arc::downgrade(&context));
        context
    }

    /// Take an op that another event's turn enqueued for this event.
    ///
    /// Reports whether the mailbox accepted it. A closed mailbox refuses, and
    /// the caller must then drop the op's resolver: this event will never run
    /// the continuation waiting on it.
    #[must_use]
    fn hand_op(&self, id: u64, future: asyncrt::OpFuture, lifetime: asyncrt::OpLifetime) -> bool {
        let mut handed = self.handed.lock().unwrap();
        let Some(queue) = handed.as_mut() else {
            return false;
        };
        queue.push((id, future, lifetime));
        // Published under the lock, so a reader that sees this count can take
        // the lock and find the op there.
        self.handed_len.store(queue.len(), Ordering::Release);
        drop(handed);
        self.handed_wake.notify_one();
        true
    }

    /// Whether a foreign turn has left this event an op, without locking.
    ///
    /// A `false` answer is authoritative for the driver that asks it. Only
    /// that driver empties the queue, so nothing it must see can appear and
    /// vanish behind this read; a hand-off that lands immediately after it
    /// wakes the driver through `handed_ready` instead.
    fn has_handed_ops(&self) -> bool {
        self.handed_len.load(Ordering::Acquire) != 0
    }

    fn accepts_handed_ops(&self) -> bool {
        self.handed.lock().unwrap().is_some()
    }

    fn register_pending_event(&self, id: u64) {
        self.pending_events.lock().unwrap().insert(id);
        // The call can start during another event's microtask checkpoint,
        // after this context's driver has already parked. Wake that driver so
        // it can reject the call when no native operation can resume it.
        self.handed_wake.notify_one();
    }

    fn finish_pending_event(&self, id: u64) {
        self.pending_events.lock().unwrap().remove(&id);
    }

    fn has_pending_events(&self) -> bool {
        !self.pending_events.lock().unwrap().is_empty()
    }

    fn take_pending_events(&self) -> Vec<u64> {
        let mut pending = self.pending_events.lock().unwrap();
        let mut ids = pending.drain().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// Resolve once another turn has handed this event an op or started a
    /// pending entrypoint call.
    ///
    /// This can also resolve with nothing to take, and a caller must treat
    /// an empty queue as ordinary. `notify_one` stores a permit when no
    /// driver is parked, and the driver takes the op on its next pass
    /// through `has_handed_ops` before it waits again, so the permit
    /// outlives the op it announced and releases one later wait early. That
    /// wait costs one atomic read and parks again, which is why the permit
    /// is left alone: cancelling it needs a second piece of state, and that
    /// state has to stay consistent with the queue.
    pub(crate) async fn handed_ready(&self) {
        self.handed_wake.notified().await;
    }

    /// Hand this event's driver everything a foreign turn left for it.
    fn take_handed_ops(&self) -> Vec<HandedOp> {
        let mut handed = self.handed.lock().unwrap();
        self.handed_len.store(0, Ordering::Release);
        handed.as_mut().map(std::mem::take).unwrap_or_default()
    }

    /// Close the mailbox and return whatever no driver took.
    ///
    /// Called when the event retires. The caller drops the resolvers of the
    /// ops that come back, and every later hand-off is refused at the source,
    /// so no op can sit here waiting for a driver that will never run again.
    #[must_use]
    fn close_handed_ops(&self) -> Vec<HandedOp> {
        let mut handed = self.handed.lock().unwrap();
        self.handed_len.store(0, Ordering::Release);
        handed.take().unwrap_or_default()
    }

    fn begin_event(&self) {
        self.events
            .lock()
            .unwrap()
            .frames
            .push(IoEventFrame::default());
    }

    fn release_input_gate(&self, cell: &str, event: celld_logic::gate::EventId) -> bool {
        let mut claims = self.input_gates.lock().unwrap();
        if !claims.held.get(&event).is_some_and(|owned| owned == cell) {
            return false;
        }
        claims.held.remove(&event);
        true
    }

    fn retire_input_gates(&self) -> Vec<(String, celld_logic::gate::EventId)> {
        let mut claims = self.input_gates.lock().unwrap();
        claims.retired = true;
        claims.retirement.send_replace(true);
        let mut held = claims
            .held
            .drain()
            .map(|(event, cell)| (cell, event))
            .collect::<Vec<_>>();
        held.sort_unstable();
        held
    }

    /// Pop the innermost frame and hand back what it collected. `None` when
    /// there is no frame, which is a script calling `waitUntil` outside any
    /// event; the harness turns that into workerd's global-scope error.
    fn end_event(&self) -> Option<Vec<v8::Global<v8::Promise>>> {
        let mut events = self.events.lock().unwrap();
        let mut frame = events.frames.pop()?;
        if events.arm_gates_sealed {
            drop(frame.arm_gates);
        } else if let Some(parent) = events.frames.last_mut() {
            parent.arm_gates.append(&mut frame.arm_gates);
        } else {
            events.ended_arm_gates.append(&mut frame.arm_gates);
        }
        if events.frames.is_empty() && !frame.wait_until.is_empty() {
            events.wait_until_active = true;
        }
        Some(frame.wait_until)
    }

    fn register_wait_until(&self, promise: v8::Global<v8::Promise>) {
        let mut events = self.events.lock().unwrap();
        if let Some(frame) = events.frames.last_mut() {
            frame.wait_until.push(promise);
        } else if events.wait_until_active {
            events.late_wait_until.push(promise);
        }
    }

    /// Whether imported `waitUntil()` has request-associated work to extend.
    fn accepts_wait_until(&self) -> bool {
        let events = self.events.lock().unwrap();
        !events.frames.is_empty() || events.wait_until_active
    }

    /// Take registrations made while the current background aggregate ran.
    /// An empty batch seals the registration window together with the
    /// driver's decision to retire that aggregate.
    fn take_late_wait_until(&self) -> Vec<v8::Global<v8::Promise>> {
        let mut events = self.events.lock().unwrap();
        let late = std::mem::take(&mut events.late_wait_until);
        if late.is_empty() {
            events.wait_until_active = false;
        }
        late
    }

    fn seal_wait_until(&self) {
        let mut events = self.events.lock().unwrap();
        events.wait_until_active = false;
        events.late_wait_until.clear();
    }

    fn register_arm_gate(&self, gate: ArmGateRx) -> Result<(), ArmGateRx> {
        let mut events = self.events.lock().unwrap();
        if events.arm_gates_sealed {
            return Err(gate);
        }
        if let Some(frame) = events.frames.last_mut() {
            frame.arm_gates.push(gate);
        } else {
            // `op_event_end` pops the final frame before `Promise.allSettled`
            // can invoke a user-defined `then`. A gate created during that
            // call still belongs to this event until `take_arm_gates` seals
            // the response boundary.
            events.ended_arm_gates.push(gate);
        }
        Ok(())
    }

    /// Take every response gate that this event owns.
    ///
    /// The normal path takes gates moved out by `end_event`. Active frames
    /// cover failures that cannot run JavaScript again, such as a timeout or
    /// an isolate termination.
    fn take_arm_gates(&self) -> Vec<ArmGateRx> {
        let mut events = self.events.lock().unwrap();
        if events.arm_gates_sealed {
            return Vec::new();
        }
        events.arm_gates_sealed = true;
        let mut gates = std::mem::take(&mut events.ended_arm_gates);
        for frame in &mut events.frames {
            gates.append(&mut frame.arm_gates);
        }
        gates
    }

    fn depth(&self) -> usize {
        self.events.lock().unwrap().frames.len()
    }

    /// Close every isolate-polled socket this request opened.
    ///
    /// Draining makes the operation idempotent. The drive loop calls it when
    /// the request retires, and `drop` covers every earlier failure path.
    fn close_sockets(&self) {
        let opened = self.sockets.lock().unwrap().drain(..).collect();
        ws_close_request_sockets(opened);
        let rewriters: Vec<u64> = self.rewriters.lock().unwrap().drain(..).collect();
        for id in rewriters {
            html_rewriter::free(id);
        }
        let tcp: Vec<u64> = self.tcp_sockets.lock().unwrap().drain(..).collect();
        for id in tcp {
            tcp::free(id);
        }
    }

    #[doc(hidden)]
    pub fn own_body_stream(&self, stream_id: u64) {
        let Some(claim) = claim_http_stream(stream_id) else {
            return;
        };
        let replaced = self.body_streams.lock().unwrap().insert(stream_id, claim);
        // Releasing a claim can drop an arbitrary source. Keep that work out
        // of the request-ownership lock.
        drop(replaced);
    }

    fn transfer_body_stream(&self, stream_id: u64) -> Option<RequestBodyGuard> {
        let claim = self.body_streams.lock().unwrap().remove(&stream_id)?;
        // Move the exact service claim into the guard. A later asynchronous
        // drop cannot resolve through another ambient Domain.
        Some(RequestBodyGuard::transferred(claim))
    }

    /// Replace one request-owned source with the two sources created by a
    /// native tee. Response streams are not request-owned, so they keep the
    /// registry's ordinary unowned lifecycle.
    fn replace_body_stream(
        &self,
        stream_id: u64,
        service: &Arc<HttpStreamService>,
        branches: (u64, u64),
    ) {
        let original = self.body_streams.lock().unwrap().remove(&stream_id);
        let Some(original) = original else {
            return;
        };
        debug_assert!(Arc::ptr_eq(&original.service, service));
        let mut branch_claims = Vec::new();
        for branch in [branches.0, branches.1] {
            if let Some(claim) = service.claim(branch) {
                branch_claims.push((branch, claim));
            }
        }
        let replaced = {
            let mut owned = self.body_streams.lock().unwrap();
            branch_claims
                .into_iter()
                .filter_map(|(branch, claim)| owned.insert(branch, claim))
                .collect::<Vec<_>>()
        };
        // The original entry was consumed by tee, so releasing this claim is
        // a no-op. Replaced claims still release through their bound service.
        drop(original);
        drop(replaced);
    }

    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub fn begin_event_for_test(&self) {
        self.begin_event();
    }

    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub fn end_event_for_test(&self) -> Option<()> {
        self.end_event().map(drop)
    }

    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub fn register_arm_gate_for_test(
        &self,
        gate: tokio::sync::oneshot::Receiver<Result<(), String>>,
    ) -> Result<(), tokio::sync::oneshot::Receiver<Result<(), String>>> {
        self.register_arm_gate(gate)
    }

    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub fn take_arm_gates_for_test(
        &self,
    ) -> Vec<tokio::sync::oneshot::Receiver<Result<(), String>>> {
        self.take_arm_gates()
    }
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
#[derive(Default)]
pub struct IoContextRegistryForTest(Arc<ActorRuntimeState>);

#[cfg(celld_internal_tests)]
impl IoContextRegistryForTest {
    #[doc(hidden)]
    pub fn track(&self) -> (u64, Arc<IoContext>) {
        let context = IoContext::tracked(&self.0);
        let id = context
            .continuation
            .as_ref()
            .map(|(id, _)| *id)
            .expect("a tracked IoContext has a continuation id");
        (id, context)
    }

    #[doc(hidden)]
    pub fn resolves_to(&self, id: u64, expected: &Arc<IoContext>) -> bool {
        self.0
            .io_context(id)
            .is_some_and(|context| Arc::ptr_eq(&context, expected))
    }

    #[doc(hidden)]
    pub fn contains(&self, id: u64) -> bool {
        self.0.io_contexts.lock().unwrap().contains_key(&id)
    }

    #[doc(hidden)]
    pub fn is_empty(&self) -> bool {
        self.0.io_contexts.lock().unwrap().is_empty()
    }

    /// Hand `context` an op that never completes, and report whether its
    /// mailbox accepted it. The future stands in for real work: the mailbox
    /// only stores it, so nothing here polls it.
    #[doc(hidden)]
    pub fn hand_pending_op(&self, context: &Arc<IoContext>, id: u64) -> bool {
        context.hand_op(
            id,
            Box::pin(std::future::pending()),
            asyncrt::OpLifetime::Handler,
        )
    }

    /// The op ids `context` still holds for a driver that has not taken them.
    #[doc(hidden)]
    pub fn handed_op_ids(&self, context: &Arc<IoContext>) -> Vec<u64> {
        context
            .handed
            .lock()
            .unwrap()
            .as_ref()
            .map(|queue| queue.iter().map(|(id, _, _)| *id).collect())
            .unwrap_or_default()
    }

    /// Run the mailbox half of an event's retirement: close it, and report
    /// the ops whose resolvers `InFlight::abandon` must then drop.
    #[doc(hidden)]
    pub fn close_handed_ops(&self, context: &Arc<IoContext>) -> Vec<u64> {
        context
            .close_handed_ops()
            .into_iter()
            .map(|(id, _, _)| id)
            .collect()
    }
}

/// A Worker socket lives and dies with its request, exactly as it does on
/// Cloudflare. Closing it here rather than at each of the drive loops' exits
/// is the difference between a rule every exit path has to remember and one
/// it cannot get wrong: a request that times out, that is abandoned as stuck,
/// or whose client disconnects drops its context on a path of its own.
impl Drop for IoContext {
    fn drop(&mut self) {
        let already_panicking = std::thread::panicking();
        // The drive loop also abandons these while it still owns the isolate,
        // because that path resets the JavaScript actor state. Drop is the
        // final host-side invariant for an earlier exit: no request context
        // can leave a process-wide input gate active after it is gone.
        let _ = abandon_context_input_gates(self);
        self.close_sockets();
        let mut claims = self
            .body_streams
            .get_mut()
            .unwrap()
            .drain()
            .collect::<Vec<_>>();
        // A claim can drop an arbitrary source. Fix the first-panic order even
        // when the request map uses a different randomized hash seed.
        claims.sort_unstable_by_key(|(stream_id, _)| *stream_id);
        let mut first_panic = None;
        for (_, claim) in claims {
            retain_first_http_cleanup_panic(
                &mut first_panic,
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(claim))),
            );
        }
        if let Some((id, runtime_state)) = &self.continuation {
            if let Some(runtime_state) = runtime_state.upgrade() {
                runtime_state.io_contexts.lock().unwrap().remove(id);
            }
        }
        if let Some(payload) = first_panic {
            if already_panicking {
                std::mem::forget(payload);
            } else {
                std::panic::resume_unwind(payload);
            }
        }
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
    assert_send_sync::<LoaderOwner>();
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

#[cfg(celld_internal_tests)]
fn installed_context() -> Option<Arc<IoContext>> {
    CURRENT.with(|current| current.borrow().clone())
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
    match wait_until_aggregate(scope, &frame) {
        Some(aggregate) => rv.set(aggregate.into()),
        None => rv.set_null(),
    }
}

/// Resolve the request whose code is running, even when its reaction executes
/// during another request's microtask checkpoint. A missing captured context
/// is retired, so it must not fall back to the request driving this turn.
fn reaction_or_current_context(scope: &mut v8::PinScope) -> Option<Arc<IoContext>> {
    match reaction_continuation_id(scope) {
        Some(id) => actor_runtime_state(scope).io_context(id),
        None => Some(current_context()),
    }
}

fn op_pending_event_begin(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let Some(id) = args.get(0).integer_value(scope) else {
        return;
    };
    if let Some(context) = reaction_or_current_context(scope) {
        context.register_pending_event(id as u64);
    }
}

fn op_pending_event_end(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let Some(id) = args.get(0).integer_value(scope) else {
        return;
    };
    if let Some(context) = reaction_or_current_context(scope) {
        context.finish_pending_event(id as u64);
    }
}

/// Create one directory in the current request's virtual `/tmp` tree.
/// A created path starts with `/`, an error is a Node code, and an empty string
/// means that no path must be returned. The disjoint representation keeps the
/// result and its meaning together without allocating a JS object for each
/// directory segment.
fn op_vfs_mkdir(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.get(0).to_rust_string_lossy(scope);
    let recursive = args.get(1).boolean_value(scope);
    let result = current_context()
        .vfs
        .get_or_init(|| Mutex::new(RequestVfs::default()))
        .lock()
        .unwrap()
        .mkdir(&path, recursive);
    let result = match result {
        VfsMkdirResult::Created(Some(path)) => path,
        VfsMkdirResult::Created(None) => String::new(),
        VfsMkdirResult::Error(code) => code.to_string(),
    };
    rv.set(v8::String::new(scope, &result).unwrap().into());
}

/// Return `[kind, size]`, where kind is 0 for a missing path, 1 for a
/// read-only directory, 2 for a writable directory, and 3 for a read-only
/// file. Only a file has a size; a directory reports 0, matching workerd.
///
/// The pair travels together because a caller that knows a path is a file
/// always needs its size to build `Stats`, and a second lookup could observe a
/// different tree.
fn op_vfs_stat(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.get(0).to_rust_string_lossy(scope);
    let (kind, size) = match current_context()
        .vfs
        .get_or_init(|| Mutex::new(RequestVfs::default()))
        .lock()
        .unwrap()
        .stat(&path)
    {
        Some(writable) => (if writable { 2 } else { 1 }, 0),
        // Only a path the writable tree does not claim can be a bundle path,
        // so the request tree stays authoritative for `/tmp`.
        None => match bundle_node(scope, &path) {
            Some(BundleStat::Directory) => (1, 0),
            Some(BundleStat::File(size)) => (3, size),
            None => (0, 0),
        },
    };
    let pair = v8::Array::new(scope, 2);
    let kind = v8::Integer::new(scope, kind).into();
    let size = v8::Number::new(scope, size as f64).into();
    pair.set_index(scope, 0, kind);
    pair.set_index(scope, 1, size);
    rv.set(pair.into());
}

/// What a bundle path is, without copying the bytes a stat does not need.
enum BundleStat {
    Directory,
    File(usize),
}

/// Materializes `/bundle` on the first call that reaches it.
fn bundle_node(scope: &mut v8::PinScope, path: &str) -> Option<BundleStat> {
    let bundle = scope.get_slot::<Arc<BundleFs>>().cloned()?;
    match bundle.tree().nodes.get(path)? {
        BundleNode::Directory => Some(BundleStat::Directory),
        BundleNode::File(bytes) => Some(BundleStat::File(bytes.len())),
    }
}

/// Read a whole bundle file. Returns an `ArrayBuffer` of the module's bytes,
/// or `null` when the path is not a bundle file, so the caller raises the
/// `ENOENT` that names the syscall it was performing.
fn op_vfs_read_file(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let path = args.get(0).to_rust_string_lossy(scope);
    let Some(bundle) = scope.get_slot::<Arc<BundleFs>>().cloned() else {
        return;
    };
    let tree = bundle.tree();
    let Some(BundleNode::File(bytes)) = tree.nodes.get(&path) else {
        return;
    };
    // One copy into the V8 heap. The projected `Bytes` stays shared, so a
    // second read of the same module does not re-copy the config's source.
    let store = v8::ArrayBuffer::new_backing_store_from_vec(bytes.to_vec()).make_shared();
    let buffer = v8::ArrayBuffer::with_backing_store(scope, &store);
    rv.set(buffer.into());
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
    if let Some(context) = reaction_or_current_context(scope) {
        context.register_wait_until(v8::Global::new(scope, promise));
    }
}

fn op_wait_until_active(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let active =
        reaction_or_current_context(scope).is_some_and(|context| context.accepts_wait_until());
    rv.set(v8::Boolean::new(scope, active).into());
}

/// How many event frames are open on the current request.
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
    let io_context = cped_io_context(scope);
    set_cped(scope, args.get(0), trace, io_context);
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
    let timing_started = (tracing::enabled!(target: "queue_timing", tracing::Level::DEBUG)
        && s.split_once(':')
            .is_some_and(|(class, _)| class == crate::deploy::QUEUE_CLASS))
    .then(crate::asyncrt::mono_us);
    let result = storage::set_alarm(&s, at);
    if let Some(timing_started) = timing_started {
        tracing::debug!(
            target: "queue_timing",
            event = "queue_alarm_write_timing",
            total_us = crate::asyncrt::mono_us().saturating_sub(timing_started),
            ok = result.is_ok(),
            "Queue alarm write completed"
        );
    }
    match result {
        Err(error) => throw_storage_error(scope, "setAlarm", error),
        // Committed immediately: register the wake-entry PUT against the
        // current event's output gate. Inside an explicit transaction (`Ok(None)`)
        // the gate registers at that transaction's commit instead.
        Ok(Some(committed)) if committed >= 0 => {
            spawn_arm_gate(&s, committed, current_reaction_io_context(scope))
        }
        Ok(_) => {}
    }
}

/// Queue producer groups resolve promises from several cell events after one
/// event rearms the broker. The ordinary event-local arm gate cannot migrate
/// to those other events, so this internal variant awaits the wake-entry PUT
/// before the shared promise resolves. The later output gate still proves the
/// SQLite/LTX write; together they preserve the same acknowledgement boundary
/// as an ungrouped `setAlarm` call.
fn op_queue_alarm_set_wait(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let at_ms = args.get(1).number_value(scope).unwrap_or(0.0) as i64;
    let committed = match storage::set_alarm(&cell, at_ms) {
        Ok(committed) => committed,
        Err(error) => {
            throw_storage_error(scope, "Queue setAlarm", error);
            return;
        }
    };
    let Some(committed) = committed.filter(|committed| *committed >= 0) else {
        return;
    };
    let Some(gate) = launch_arm_gate(&cell, observe_alarm(&cell, Some(committed))) else {
        return;
    };
    let id = asyncrt::enqueue(async move {
        match gate.await {
            Ok(Ok(())) => Ok(String::new()),
            Ok(Err(error)) => Err(format!("Queue wake-entry gate: {error}")),
            Err(_) => Err("Queue wake-entry gate task dropped".to_string()),
        }
    });
    rv.set(promise_for(scope, id));
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

    let celld_logic::cron::Plan {
        matching,
        arm_at,
        arm_is_retry,
    } = celld_logic::cron::plan(
        &crons,
        (fired_ms >= 0).then_some(fired_ms),
        now_ms,
        retry,
        failed,
    );

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
    // The public `btoa()` wrapper always supplies a string. The KV operator
    // protocol supplies a typed view directly, which avoids constructing a
    // second 25 MiB binary string only to encode it here.
    let bytes = view_bytes(args.get(0)).unwrap_or_else(|| {
        args.get(0)
            .to_rust_string_lossy(scope)
            .chars()
            .map(|c| c as u8)
            .collect()
    });
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

#[doc(hidden)]
pub mod bootstrap;
pub mod modules;
mod v8_strings;
use bootstrap::{
    adopt_cell, adopt_embedded_cell, begin_event_context, build_env, end_event_context,
    harness_env, inject_compatibility_flags, inject_containers, inject_crons, inject_kv_limits,
    inject_namespace_keys, inject_queue_config, inject_routing, inject_storage_compatibility,
    inject_workflows, install_harness, install_prelude, populate_cf_exports, register_class,
    register_entrypoints, validate_workflow_classes, EmbeddedStartup,
};
use modules::{
    compile_module, host_import_module_dynamically, install_lazy_globals, op_builtin_module,
    register_loader_modules, register_stubs, register_wasm_modules, resolve_external,
    ModuleRegistry,
};
use v8_strings::key as static_key;
/// Like `make_request`, but marks the request as incoming. Its signal is
/// registered only if the handler actually suspends, preserving the
/// synchronous request path.
fn make_incoming_request<'s>(
    tc: &mut v8::PinScope<'s, '_>,
    url: &str,
    method: &str,
    body: RequestBody,
    headers: &[(String, String)],
) -> Result<v8::Local<'s, v8::Value>> {
    let f = internal_function(tc, "__makeIncomingRequest")?;
    let url = v8::String::new(tc, url).unwrap();
    let method = v8::String::new(tc, method).unwrap();
    let headers = v8::String::new(
        tc,
        &serde_json::to_string(headers).unwrap_or_else(|_| "[]".into()),
    )
    .unwrap();
    // A streamed body passes its id, and the harness builds the body
    // stream around that id. A held body passes the bytes.
    let stream_id = body.stream_id();
    let (body, stream_id) = match stream_id {
        None => (
            bytes_value(tc, body.into_held_bytes().unwrap()),
            v8::undefined(tc).into(),
        ),
        Some(id) => (
            v8::undefined(tc).into(),
            v8::Number::new(tc, id as f64).into(),
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
    let function = internal_function(tc, "__registerIncomingRequest")?;
    let id = v8::String::new(tc, &request_id_string(request_id)).unwrap();
    let recv = v8::undefined(tc).into();
    function
        .call(tc, recv, &[id.into(), request])
        .ok_or_else(|| anyhow!("__registerIncomingRequest threw"))?;
    Ok(())
}

fn finish_incoming_request(tc: &mut v8::PinScope, request_id: RequestId) {
    let Ok(function) = internal_function(tc, "__finishIncomingRequest") else {
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
    body: RequestBody,
    headers: &[(String, String)],
) -> Result<v8::Local<'s, v8::Value>> {
    let f = internal_function(tc, "__makeRequest")?;
    let u = v8::String::new(tc, url).unwrap();
    let m = v8::String::new(tc, method).unwrap();
    let bytes = body.into_held_bytes().unwrap_or_default();
    let b = bytes_value(tc, bytes);
    let h = v8::String::new(tc, &serde_json::to_string(headers)?).unwrap();
    let recv = v8::undefined(tc).into();
    f.call(tc, recv, &[u.into(), m.into(), b, h.into()])
        .ok_or_else(|| anyhow!("makeRequest threw"))
}

fn read_response(scope: &mut v8::PinScope, ret: v8::Local<v8::Value>) -> Result<HttpResponse> {
    let f = internal_function(scope, "__readResponse")?;
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
    let ek = v8::String::new(scope, "error").unwrap();
    if let Some(error) = out.get(scope, ek.into()).filter(|value| value.is_string()) {
        return Err(anyhow!(error.to_rust_string_lossy(scope)));
    }
    let sk = v8::String::new(scope, "status").unwrap();
    let bk = v8::String::new(scope, "bodyBytes").unwrap();
    let tk = v8::String::new(scope, "bodyStreamId").unwrap();
    let hk = v8::String::new(scope, "headersJson").unwrap();
    let wk = v8::String::new(scope, "wsTargetJson").unwrap();
    let worker_wk = v8::String::new(scope, "workerSocketId").unwrap();
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
        http_stream_service()
            .checkout_transfer(stream_id, None)
            .ok()
            .map(|source| Box::pin(source) as HttpChunkStream)
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
    let worker_id = out
        .get(scope, worker_wk.into())
        .and_then(|value| value.integer_value(scope))
        .unwrap_or(0)
        .max(0) as u64;
    let worker_ws = if worker_id == 0 {
        None
    } else {
        Some(
            websocket::transfer_worker_websocket_handoff(worker_id)
                .ok_or_else(|| anyhow!("Worker WebSocket {worker_id} has no frame handoff"))?,
        )
    };
    if status == 101 {
        tracing::info!(%ws_json, has_target = ws.is_some(), "JS WebSocket response");
    }
    let websocket = match (ws, worker_ws) {
        (Some(target), None) => Some(HttpResponseWebSocket::Cell(target)),
        (None, Some(worker)) => Some(HttpResponseWebSocket::Worker(worker)),
        (None, None) => None,
        (Some(_), Some(_)) => return Err(anyhow!("Worker response has two WebSocket targets")),
    };
    Ok(HttpResponse {
        status,
        body,
        stream,
        headers,
        websocket,
        write_position: None,
        observed_position: None,
    })
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn init_engine_for_tests() {
    Engine::init();
}

#[cfg(celld_internal_tests)]
include!(env!("CELLD_INTERNAL_JS_OBSERVERS"));
