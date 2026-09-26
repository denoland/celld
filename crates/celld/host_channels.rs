// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The channels a cell runtime uses to hand work to the host: one request
//! type and one sender per host service. Engine-neutral; `crate::js` and
//! `crate::js::r2_ops` re-export every item at its old path.
use crate::bucket::Bucket;
use crate::engine_api::HttpResponse;
use crate::engine_api::QueueBatch;
use crate::engine_api::RequestBody;
use crate::engine_api::RpcData;
use crate::http_streams::RequestBodyGuard;
use crate::ws_registry::OutboundWsReq;
use anyhow::Result;
#[cfg(celld_internal_tests)]
use std::cell::RefCell;
use std::sync::OnceLock;

/// A ticket asking the actor whether an outbound effect may leave the process.
///
/// Every in-handler channel takes one: `fetch`, a service binding, a call to
/// another cell, and a frame on a socket the isolate opened. `position` is
/// present when the running event wrote through it, absent when the event only
/// read and the effect must trail whatever the cell already has outstanding.
pub struct GateReq {
    pub scope: String,
    /// The route the held effect would leave by, the position it must see
    /// proven, and the epoch that position was sampled at. The sample happens
    /// in the handler's turn, before `dispatch_gate` acquires the request that
    /// pins the cell; a reset in between discards the sampled write and the
    /// request would activate the next epoch, so the core refuses a ticket
    /// whose epoch is not the resident one. The core stores the route and
    /// hands it back, so the shell can route the release to the adapter
    /// holding the effect.
    pub ticket: crate::actor::GateTicket,
    pub reply: tokio::sync::oneshot::Sender<Result<(), celld_logic::RequestError>>,
}
pub(crate) static GATE_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<GateReq>> = OnceLock::new();

/// A facet's stream, asked for by the isolate that runs the facet: a facet
/// is a database and a replication stream of its own
/// (`crate::facet_streams`), which only the runtime can activate, delete, or
/// prove.
pub enum FacetReq {
    /// Activate the stream of the facet at `names` below `root`, and answer
    /// its database file and whether replication restored it.
    Open {
        root: String,
        epoch: u64,
        names: Vec<String>,
        reply: tokio::sync::oneshot::Sender<Result<(std::path::PathBuf, bool)>>,
    },
    /// Delete the facet's stream and every stream below it.
    Delete {
        root: String,
        epoch: u64,
        names: Vec<String>,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// Prove every committed write of the stream durable.
    Prove {
        stream: String,
        epoch: u64,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
}
pub(crate) static FACET_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<FacetReq>> =
    OnceLock::new();
pub fn set_facet_tx(tx: tokio::sync::mpsc::UnboundedSender<FacetReq>) {
    let _ = FACET_TX.set(tx);
}
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
    /// The application generation of the calling isolate. The target is
    /// resolved in that generation's service graph, so a caller built for
    /// one deployment never reaches a target from another.
    pub generation: crate::generation::GenerationId,
    pub script: String,
    /// A named Worker entrypoint and its props, or the target's default export
    /// when absent. Keeping both values together prevents a route from losing
    /// the props that select its authority boundary.
    pub entrypoint: Option<crate::WorkerFetchEntrypoint>,
    pub url: String,
    pub method: String,
    pub body: RequestBody,
    /// Owns a streamed body until the target installs its request context.
    pub body_guard: RequestBodyGuard,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
}
pub(crate) static SVC_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<SvcCallReq>> =
    OnceLock::new();

/// A Worker assets-binding call. The script name selects the immutable asset
/// index loaded for that Worker; unlike ingress this never falls back into the
/// Worker and therefore cannot recurse.
pub struct AssetCallReq {
    /// The calling isolate's application generation; see `SvcCallReq`.
    pub generation: crate::generation::GenerationId,
    pub script: String,
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
}
pub(crate) static ASSET_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<AssetCallReq>> =
    OnceLock::new();

/// An RPC operation on a named `WorkerEntrypoint` of another script. A call's
/// arguments and every result cross as V8 structured-clone bytes.
pub struct SvcRpcReq {
    /// The calling isolate's application generation; see `SvcCallReq`.
    pub generation: crate::generation::GenerationId,
    pub script: String,
    pub entrypoint: String,
    /// Structured-clone bytes for the target entrypoint's `ctx.props`.
    pub props: Vec<u8>,
    pub operation: crate::WorkerRpcOperation,
    pub reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
}
pub(crate) static SVC_RPC_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<SvcRpcReq>> =
    OnceLock::new();

/// The persisted identity a consumer settlement must match for one message.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct QueueLeaseRef {
    pub message_id: String,
    pub seq: i64,
    pub generation: u64,
}

/// A durable broker batch released through its output gate.
pub struct QueueDispatchReq {
    pub scope: String,
    /// The dispatching queue cell's application generation; see
    /// `SvcCallReq`.
    pub generation: crate::generation::GenerationId,
    pub script: String,
    pub lease_id: String,
    pub leases: Vec<QueueLeaseRef>,
    pub batch: QueueBatch,
}
pub(crate) static QUEUE_DISPATCH_TX: OnceLock<
    tokio::sync::mpsc::UnboundedSender<QueueDispatchReq>,
> = OnceLock::new();
/// The fleet bucket used by KV values that are too large for a namespace cell.
///
/// The landed R2 binding established the safe pattern: a cloneable bucket
/// handle can live beside the asynchronous ops, so independent requests do not
/// queue behind one task that awaits every object-store operation in order.
static KV_BLOB_STORE: OnceLock<crate::bucket::Bucket> = OnceLock::new();

pub fn set_kv_blob_store(store: crate::bucket::Bucket) {
    let _ = KV_BLOB_STORE.set(store);
}

pub(crate) fn kv_blob_store() -> std::result::Result<&'static crate::bucket::Bucket, String> {
    KV_BLOB_STORE
        .get()
        .ok_or_else(|| "KV large values need a fleet bucket".to_string())
}

pub fn set_svc_rpc_tx(tx: tokio::sync::mpsc::UnboundedSender<SvcRpcReq>) {
    let _ = SVC_RPC_TX.set(tx);
}
pub fn set_svc_call_tx(tx: tokio::sync::mpsc::UnboundedSender<SvcCallReq>) {
    let _ = SVC_CALL_TX.set(tx);
}
pub fn set_queue_dispatch_tx(tx: tokio::sync::mpsc::UnboundedSender<QueueDispatchReq>) {
    let _ = QUEUE_DISPATCH_TX.set(tx);
}
pub fn set_asset_call_tx(tx: tokio::sync::mpsc::UnboundedSender<AssetCallReq>) {
    let _ = ASSET_CALL_TX.set(tx);
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
pub(crate) static RPC_CALL_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<RpcCallReq>> =
    OnceLock::new();
pub fn set_rpc_call_tx(tx: tokio::sync::mpsc::UnboundedSender<RpcCallReq>) {
    let _ = RPC_CALL_TX.set(tx);
}

static OUTBOUND_WS_TX: OnceLock<tokio::sync::mpsc::UnboundedSender<OutboundWsReq>> =
    OnceLock::new();
pub fn set_outbound_ws_tx(tx: tokio::sync::mpsc::UnboundedSender<OutboundWsReq>) {
    let _ = OUTBOUND_WS_TX.set(tx);
}

#[cfg(celld_internal_tests)]
/// An outbound connector scoped to the current internal-test JS thread.
///
/// The production connector is process-wide. Installing a test sender there
/// captures requests from unrelated suites, and those suites have no receiver
/// that can answer them. The thread-local sender follows the current-thread V8
/// harnesses, while this handle owns its receiver and removes the sender when
/// the case ends.
#[doc(hidden)]
pub struct TestOutboundWsConnector {
    requests: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<OutboundWsReq>>,
    /// A scoped connector must drop on the thread where it installed its sender.
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(celld_internal_tests)]
impl TestOutboundWsConnector {
    #[doc(hidden)]
    pub fn requests(
        &self,
    ) -> &tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<OutboundWsReq>> {
        &self.requests
    }
}

#[cfg(celld_internal_tests)]
thread_local! {
    static TEST_OUTBOUND_WS_TX: RefCell<Option<tokio::sync::mpsc::UnboundedSender<OutboundWsReq>>> =
        const { RefCell::new(None) };
}

#[cfg(celld_internal_tests)]
impl Drop for TestOutboundWsConnector {
    fn drop(&mut self) {
        TEST_OUTBOUND_WS_TX.with(|slot| {
            assert!(
                slot.borrow_mut().take().is_some(),
                "test outbound WebSocket connector was not installed",
            );
        });
    }
}

#[cfg(celld_internal_tests)]
/// Install the internal-test connector for the current JS thread.
#[doc(hidden)]
pub fn install_outbound_ws_connector_for_test() -> TestOutboundWsConnector {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    TEST_OUTBOUND_WS_TX.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "test outbound WebSocket connector is already installed"
        );
        *slot = Some(tx);
    });
    TestOutboundWsConnector {
        requests: tokio::sync::Mutex::new(rx),
        _not_send: std::marker::PhantomData,
    }
}

/// Select the scoped internal-test connector before the production connector.
pub(crate) fn outbound_ws_tx() -> Option<tokio::sync::mpsc::UnboundedSender<OutboundWsReq>> {
    #[cfg(celld_internal_tests)]
    if let Some(tx) = TEST_OUTBOUND_WS_TX.with(|slot| slot.borrow().clone()) {
        return Some(tx);
    }
    OUTBOUND_WS_TX.get().cloned()
}
/// The fleet bucket, once a node has opened one. Installed beside the
/// wake-entry gate at startup; absent for a bucketless run.
pub(crate) static R2_STORE: OnceLock<Bucket> = OnceLock::new();

/// Give the R2 bindings the fleet bucket to live in.
pub fn set_r2_store(bucket: Bucket) {
    let _ = R2_STORE.set(bucket);
}
