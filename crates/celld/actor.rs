// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The serial lifecycle executor and its shell-side request drivers.

use crate::js::{HttpResponse, WsOut};
use crate::machine::{
    ownership_on_evict_from_environment, pressure_config_from_environment,
    random_process_generation, DEFAULT_MAX_OUTBOUND_WEBSOCKETS,
    DEFAULT_OWNER_LOG_RECOVERY_ATTEMPTS, DEFAULT_OWNER_LOG_RECOVERY_BACKOFF_MS,
};
use crate::ownership_store::{now_ms, BucketOwnership, LeaseCasError};
use crate::peer_auth::{self, PeerAuth};
use crate::runtime::{CellHost, RuntimeManager};
use anyhow::Context as _;
use celld_logic::{
    on_event, AdoptedCell, CapacityPeer, CasGuard, CasOutcome, Channel, Config, Effect, Event,
    Failure, LeaseCasOutcome, NodeLeaseRecord, NodeLeaseSpec, OpId, OwnerRecord, OwnershipOnEvict,
    Phase, RequestError, Route, State, StopCause, Timer, WebSocketKind, WorkerRoute,
};
use futures_util::stream::FuturesUnordered;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio_util::time::{delay_queue, DelayQueue};

mod production;

const DEFAULT_OPERATION_DEADLINE_MS: u64 = 15_000;

/// The shell's deadline for one actor operation. Queue leases include this
/// exact budget for their settlement call, so the actor and broker must read
/// one accessor rather than duplicate the environment default.
#[doc(hidden)]
pub fn operation_deadline_ms() -> anyhow::Result<u64> {
    crate::env_vars::positive_or("CELLD_OPERATION_DEADLINE_MS", DEFAULT_OPERATION_DEADLINE_MS)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimerSlot {
    NodeLeaseRenew,
    NodeLeaseFence,
    CellAlarm(String),
    /// Keyed by operation, deliberately. Every other timer here coalesces
    /// because only its newest arming matters; a deadline is the opposite --
    /// each watches a different outstanding operation, and a shared slot
    /// would let arming one silently cancel another, leaving every activation
    /// but the most recent with nothing watching it.
    OperationDeadline(OpId),
    /// Keyed by cell and generation for the same reason as the deadline
    /// above: a node parks many cells at once, and one slot per cell would
    /// let a later parking disarm an earlier one.
    QueuedActivation(String, u64),
    /// Keyed like `QueuedActivation`: one pending retry per cell, and a
    /// newer episode's arming supersedes a stale one.
    OwnerLogRecoveryRetry(String, u64),
}

impl TimerSlot {
    pub fn of(timer: &Timer) -> Self {
        match timer {
            Timer::NodeLeaseRenew { .. } => Self::NodeLeaseRenew,
            Timer::NodeLeaseFence { .. } => Self::NodeLeaseFence,
            Timer::CellAlarm { cell, .. } => Self::CellAlarm(cell.clone()),
            Timer::OperationDeadline { op } => Self::OperationDeadline(*op),
            Timer::QueuedActivation { cell, generation } => {
                Self::QueuedActivation(cell.clone(), *generation)
            }
            Timer::OwnerLogRecoveryRetry { cell, generation } => {
                Self::OwnerLogRecoveryRetry(cell.clone(), *generation)
            }
        }
    }
}

/// One versioned timer arm emitted by an Actor step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerArm {
    pub slot: TimerSlot,
    pub ordinal: u64,
    pub timer: Timer,
    pub at_mono_ms: u64,
}

/// The shared displacement discipline for production and deterministic timers.
pub struct TimerSlots<V> {
    armed: BTreeMap<TimerSlot, (u64, V)>,
    ordinals: BTreeMap<TimerSlot, u64>,
}

impl<V> Default for TimerSlots<V> {
    fn default() -> Self {
        Self {
            armed: BTreeMap::new(),
            ordinals: BTreeMap::new(),
        }
    }
}

impl<V> TimerSlots<V> {
    /// Creates one arm, assigns its stable per-slot ordinal, and applies it.
    pub fn arm(&mut self, timer: Timer, at_mono_ms: u64, value: V) -> (TimerArm, Option<V>) {
        let slot = TimerSlot::of(&timer);
        let ordinal = self.ordinals.entry(slot.clone()).or_default();
        let arm = TimerArm {
            slot: slot.clone(),
            ordinal: *ordinal,
            timer,
            at_mono_ms,
        };
        *ordinal = ordinal.saturating_add(1);
        let displaced = self.replace(&slot, arm.ordinal, value);
        (arm, displaced)
    }

    /// Applies an arm whose ordinal the Actor has already assigned.
    pub fn install(&mut self, arm: &TimerArm, value: V) -> Option<V> {
        self.replace(&arm.slot, arm.ordinal, value)
    }

    fn replace(&mut self, slot: &TimerSlot, ordinal: u64, value: V) -> Option<V> {
        let displaced = self
            .armed
            .insert(slot.clone(), (ordinal, value))
            .map(|(_, value)| value);
        debug_assert!(
            displaced.is_none() || !matches!(slot, TimerSlot::OperationDeadline(_)),
            "an operation deadline displaced another: {slot:?}"
        );
        displaced
    }

    /// Removes the current arm only when both parts of its identity match.
    pub fn fire(&mut self, slot: &TimerSlot, ordinal: u64) -> Option<V> {
        if self
            .armed
            .get(slot)
            .is_some_and(|(armed, _)| *armed == ordinal)
        {
            self.armed.remove(slot).map(|(_, value)| value)
        } else {
            None
        }
    }

    fn clear_slot(&mut self, slot: &TimerSlot) {
        self.armed.remove(slot);
    }
}

pub struct MemoryOwnership {
    node: String,
    owners: BTreeMap<String, OwnerRecord>,
    leases: BTreeMap<String, NodeLeaseRecord>,
    next_etag: u64,
}

#[derive(Clone)]
pub enum Ownership {
    Memory(Arc<Mutex<MemoryOwnership>>),
    Bucket(Arc<BucketOwnership>),
}

impl Ownership {
    async fn read_owner(&self, cell: &str) -> Result<Option<OwnerRecord>, Failure> {
        match self {
            Self::Memory(memory) => Ok(memory.lock().await.owners.get(cell).cloned()),
            Self::Bucket(bucket) => bucket.read_owner(cell).await.map_err(|error| {
                eprintln!("celld ownership read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn read_node_lease(&self, owner: &str) -> Result<Option<NodeLeaseRecord>, Failure> {
        match self {
            Self::Memory(memory) => Ok(memory.lock().await.leases.get(owner).cloned()),
            Self::Bucket(bucket) => bucket.read_node_lease(owner).await.map_err(|error| {
                eprintln!("celld node lease read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn read_capacity_peers(&self) -> Result<Vec<celld_logic::CapacityPeer>, Failure> {
        match self {
            // The in-memory adapter is a single-node development mode. It has
            // no external membership enumeration to offer.
            Self::Memory(_) => Ok(Vec::new()),
            Self::Bucket(bucket) => bucket.read_capacity_peers().await.map_err(|error| {
                eprintln!("celld capacity peer read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    /// The shared fleet sample and the instant it was taken, for placement.
    async fn read_capacity_view(&self) -> Result<(u64, Vec<celld_logic::CapacityPeer>), Failure> {
        match self {
            Self::Memory(_) => Ok((now_ms(), Vec::new())),
            Self::Bucket(bucket) => bucket.read_shared_capacity_view().await.map_err(|error| {
                eprintln!("celld capacity view read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn read_self_node_lease(&self, node: &str) -> Result<Option<NodeLeaseRecord>, Failure> {
        match self {
            Self::Memory(memory) => Ok(memory.lock().await.leases.get(node).cloned()),
            Self::Bucket(bucket) => bucket.read_self_node_lease(node).await.map_err(|error| {
                eprintln!("celld self node lease read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn cas_owner(
        &self,
        cell: &str,
        guard: CasGuard,
        epoch: u64,
    ) -> Result<CasOutcome, Failure> {
        match self {
            Self::Memory(memory) => {
                let mut memory = memory.lock().await;
                let allowed = match guard {
                    CasGuard::Absent => !memory.owners.contains_key(cell),
                    CasGuard::Match(expected) => memory
                        .owners
                        .get(cell)
                        .is_some_and(|owner| owner.etag == expected),
                };
                if allowed {
                    let etag = format!("e{}", memory.next_etag);
                    memory.next_etag += 1;
                    let node = memory.node.clone();
                    memory.owners.insert(
                        cell.into(),
                        OwnerRecord {
                            node: Some(node),
                            epoch,
                            etag,
                        },
                    );
                }
                Ok(if allowed {
                    CasOutcome::Applied
                } else {
                    CasOutcome::Rejected
                })
            }
            Self::Bucket(bucket) => bucket.cas_owner(cell, guard, epoch).await.map_err(|error| {
                // Any transport or 5xx failure may have happened after the
                // store committed. The core reconciles by reading the owner
                // again.
                eprintln!("celld ownership CAS ambiguous: {error:#}");
                Failure::Ambiguous
            }),
        }
    }

    async fn release_owner(&self, cell: &str, epoch: u64) -> Result<CasOutcome, Failure> {
        match self {
            Self::Memory(memory) => {
                let mut memory = memory.lock().await;
                let node = memory.node.clone();
                let releasable = memory.owners.get(cell).is_some_and(|owner| {
                    owner.node.as_deref() == Some(node.as_str()) && owner.epoch == epoch
                });
                if releasable {
                    let etag = format!("e{}", memory.next_etag);
                    memory.next_etag += 1;
                    memory.owners.insert(
                        cell.into(),
                        OwnerRecord {
                            node: None,
                            epoch,
                            etag,
                        },
                    );
                }
                Ok(if releasable {
                    CasOutcome::Applied
                } else {
                    CasOutcome::Rejected
                })
            }
            // A release that may or may not have committed needs no
            // reconciliation: the record either still names this node, and the
            // next eviction releases it again, or it does not, and the cell is
            // already free. Either way nothing is owed.
            Self::Bucket(bucket) => bucket.release_owner(cell, epoch).await.map_err(|error| {
                eprintln!("celld ownership release failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn cas_node_lease(
        &self,
        guard: CasGuard,
        mut record: NodeLeaseRecord,
        stamped: &mut Option<celld_logic::log_tier::LogState>,
    ) -> Result<LeaseCasOutcome, Failure> {
        match self {
            Self::Memory(memory) => {
                let mut memory = memory.lock().await;
                let current = memory.leases.get(&record.node);
                let allowed = match guard {
                    CasGuard::Absent => current.is_none(),
                    CasGuard::Match(expected) => {
                        current.is_some_and(|lease| lease.etag == expected)
                    }
                };
                if !allowed {
                    return Ok(LeaseCasOutcome::Rejected);
                }
                let etag = format!("e{}", memory.next_etag);
                memory.next_etag += 1;
                record.etag = etag.clone();
                *stamped = record.log_state;
                memory.leases.insert(record.node.clone(), record);
                Ok(LeaseCasOutcome::Applied { etag })
            }
            Self::Bucket(bucket) => bucket
                .cas_node_lease(guard, &record, stamped)
                .await
                .map_err(|error| match error {
                    // The write provably did not commit, so the record
                    // still holds what the prior attempt left. A readback
                    // would only spend authority to re-learn that, and the
                    // renewal window is exactly what a store blip is
                    // eating.
                    LeaseCasError::NotCommitted(error) => {
                        eprintln!("celld node-lease CAS did not commit: {error:#}");
                        Failure::Definite
                    }
                    LeaseCasError::Ambiguous(error) => {
                        eprintln!("celld node-lease CAS ambiguous: {error:#}");
                        Failure::Ambiguous
                    }
                }),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::Bucket(bucket) => bucket.storage_scheme(),
        }
    }
}

pub enum Message {
    /// A periodic resource sample. The measuring is the shell's job; every
    /// decision that follows belongs to the core.
    SampleLoad,
    /// The log tier changed the folded log object the next lease write
    /// must carry (lease-fold): renew now so the change becomes durable.
    NudgeNodeLease,
    /// Stop new internal routing while a clean same-node reload drains. The
    /// request shell has already stopped admission; this closes wake and
    /// service paths that do not pass through the HTTP gate.
    BeginPreserve,
    /// The node adopted a new application generation. Resident cells on an
    /// older one move to it at a safe point, or by force after `max_age_ms`;
    /// cells of an eager class move at once.
    GenerationChanged {
        generation: crate::generation::GenerationId,
        max_age_ms: u64,
        eager_classes: Vec<String>,
    },
    /// Graceful shutdown: release every owned cell's record so peers take
    /// over immediately. The release writes run as effects inside the
    /// shutdown drain window, and the drain waits for them to complete —
    /// bounded by the drain deadline, so a wedged store still cannot hold
    /// the exit hostage.
    ReleaseAll,
    /// Give up to `cells` idle cells to the fleet. The balancing loop sends
    /// this after it has confirmed from the leases that this node is the
    /// densest and a peer has room.
    Rebalance {
        cells: usize,
    },
    /// Has the shutdown handoff finished? True once no cell occupies
    /// capacity: nothing resident, restoring, or mid-eviction. The drain
    /// loop polls this to exit as soon as the handoff completes instead of
    /// sitting out the full drain window.
    Drained {
        reply: oneshot::Sender<DrainStatus>,
    },
    Request {
        request: u64,
        cell: String,
        capacity_handoff: bool,
        /// Present only for the signed shutdown-handoff endpoint. The actor
        /// settles this channel as soon as the ownership CAS is authoritative;
        /// the ordinary route reply remains pending until it is cancelled.
        handoff_accept: Option<HandoffAcceptWaiter>,
        /// An event from this exact registered WebSocket can finish on a
        /// quiescing resident. Ordinary requests carry no identity and wait
        /// for the successor route.
        websocket: Option<u64>,
        reply: oneshot::Sender<Result<Routed, RequestError>>,
    },
    /// A caller disappeared before routing finished. The request identity is
    /// allocated by the shell, so the actor can remove it from either cold
    /// admission queue immediately.
    CancelRoute {
        request: u64,
    },
    WorkerRequest {
        reply: oneshot::Sender<WorkerRouted>,
    },
    ActivityFinished {
        request: u64,
        // The activity drop also observes the cell's alarm. Folded in here so a
        // request's completion costs the serial core one message, not two — the
        // hot path for every routed DO call.
        cell: String,
        alarm_at_ms: Option<i64>,
        alarm_covered: bool,
    },
    /// An outbound effect is ready to leave on `channel`. A write opens a
    /// durability barrier, and a read trails the newest barrier already open on
    /// its cell. The channel rides along so the release can be routed back to
    /// whichever adapter is holding the effect.
    Output {
        request: u64,
        ticket: GateTicket,
        reply: oneshot::Sender<Result<(), RequestError>>,
    },
    /// A `webSocketMessage` finished; hand its captured outbound frames to the
    /// cell's output gate. `write_position` present means the handler wrote, so
    /// the frames are held behind that write until it is durable; absent means
    /// flush them behind the proof of what the handler observed, if a proof is
    /// still owed for that (`observed`, as on [`GateTicket`]), else behind any
    /// earlier still-pending write, else immediately.
    WsOutput {
        request: u64,
        scope: String,
        frames: Vec<(u64, WsOut)>,
        write_position: Option<u64>,
        observed: Option<u64>,
        reply: oneshot::Sender<()>,
    },
    WebSocketOpened {
        cell: String,
        websocket: u64,
        kind: WebSocketKind,
        reply: oneshot::Sender<bool>,
    },
    WebSocketClosed {
        cell: String,
        websocket: u64,
    },
    AlarmObserved {
        cell: String,
        at_ms: Option<i64>,
        covered: bool,
    },
    WakeHint {
        cell: String,
        entry_ms: i64,
        scope: celld_logic::WakeHintScope,
    },
    Evict {
        cell: String,
        reply: oneshot::Sender<()>,
    },
    InvalidateRemote {
        cell: String,
        node: String,
        epoch: u64,
        reply: oneshot::Sender<()>,
    },
    Snapshot {
        reply: oneshot::Sender<(String, Vec<celld_logic::DrainPin>, SwapCensus)>,
    },
    Health {
        reply: oneshot::Sender<bool>,
    },
    Presence {
        reply: oneshot::Sender<celld_logic::PresenceSnapshot>,
    },
    ResidentEpoch {
        cell: String,
        reply: oneshot::Sender<Option<u64>>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShutdownMode {
    /// The logical node is leaving. Release every owner record so another
    /// node can take over without waiting for the node lease to expire.
    Handoff,
    /// The same logical node will start again at the same address. Drain the
    /// request shell, but keep ownership and the local replica cache intact.
    Preserve,
}

pub struct Routed {
    pub request: u64,
    pub route: Route,
}

/// A test-only receipt for three request-path outcomes. `AppHandle::request`
/// deliberately maps both channel failures to `NodeFenced`, so its public
/// result cannot identify which asynchronous seam failed.
#[cfg(all(test, celld_internal_tests))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RequestPathReceiptForTest {
    SubmissionFailure {
        request: u64,
    },
    ReplyChannelClosed {
        request: u64,
    },
    Returned {
        allocated_request: u64,
        result: Result<(u64, Route), RequestError>,
    },
}

/// A test-only receipt for three output-gate outcomes. The public gate maps
/// both channel failures to `NodeFenced`, so its result cannot identify which
/// asynchronous seam failed.
#[cfg(all(test, celld_internal_tests))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OutputGateReceiptForTest {
    SubmissionFailure {
        request: u64,
    },
    ReplyChannelClosed {
        request: u64,
    },
    Returned {
        request: u64,
        result: Result<(), RequestError>,
    },
}

enum RequestPathOutcome {
    SubmissionFailure,
    ReplyChannelClosed,
    Returned(Result<Routed, RequestError>),
}

enum OutputGateOutcome {
    SubmissionFailure,
    ReplyChannelClosed,
    Returned(Result<(), RequestError>),
}

#[derive(Clone, Copy, Default)]
pub struct DrainStatus {
    pub occupied: usize,
    pub activating: usize,
    /// Resident cells reserved for shutdown handoff. New requests wait for
    /// their successor routes while admitted work finishes.
    pub quiescing: usize,
    pub evicting: usize,
    /// Ownership releases still in flight. A handoff drain waits for zero:
    /// a released cell leaves `occupied` before its record write commits,
    /// so exiting on occupancy alone can abort the write mid-flight and
    /// leave a record the successor waits out the node lease for.
    pub releasing: usize,
    /// Released cells waiting for a successor's ownership acknowledgement.
    /// These hold the shutdown pacing permits.
    pub adopting: usize,
    /// Monotonic ownership acknowledgements. A progressing handoff can
    /// take longer than one stall interval when the node owns many cells.
    pub handed_off: u64,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoffRequest {
    pub cell: String,
    pub released_epoch: u64,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandoffResponse {
    pub node: String,
    pub addr: String,
    pub epoch: u64,
    pub peer_protocol: u16,
    /// Whether the successor adopted the cell. It repeats what the status code
    /// reports, and the donor accepts the reply only when the two agree: `200`
    /// with `true` is an adoption, and `409` with `false` names the current
    /// owner for the donor to contact instead. Every other pairing is refused,
    /// so a disagreement between the two cannot be read as a handoff.
    pub published: bool,
}

pub struct HandoffAcceptWaiter {
    released_epoch: u64,
    reply: oneshot::Sender<AdoptedCell>,
}

// Read only by the orphaned `worker_route`, and removed with the rest of the
// landing-cell machinery when that goes.
#[allow(dead_code)]
pub struct WorkerRouted {
    pub request: u64,
    pub route: Option<WorkerRoute>,
}

/// Worker requests owned by one HTTP connection. Hyper can finish a failed
/// connection without dropping its service future, so the connection also
/// aborts these requests explicitly when its transport ends.
#[derive(Clone, Default)]
pub struct ConnectionWorkerRequests(
    Arc<std::sync::Mutex<Vec<Arc<crate::runtime::RequestCancellationLifetime>>>>,
);

impl ConnectionWorkerRequests {
    pub fn register(&self, cancellation: Arc<crate::runtime::RequestCancellationLifetime>) {
        self.0.lock().unwrap().push(cancellation);
    }

    pub fn complete(&self, cancellation: &Arc<crate::runtime::RequestCancellationLifetime>) {
        self.0
            .lock()
            .unwrap()
            .retain(|active| !Arc::ptr_eq(active, cancellation));
    }

    pub fn abort_all(&self) {
        let cancellations = std::mem::take(&mut *self.0.lock().unwrap());
        for cancellation in cancellations {
            cancellation.publish_abort();
        }
    }
}

pub struct IngressAbortGuard {
    cancellation: Option<Arc<crate::runtime::RequestCancellationLifetime>>,
    connection: ConnectionWorkerRequests,
}

impl IngressAbortGuard {
    pub fn new(
        cancellation: Arc<crate::runtime::RequestCancellationLifetime>,
        connection: ConnectionWorkerRequests,
    ) -> Self {
        connection.register(cancellation.clone());
        Self {
            cancellation: Some(cancellation),
            connection,
        }
    }

    pub fn disarm(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            self.connection.complete(&cancellation);
        }
    }
}

/// Cancels a core route when the future awaiting it is dropped. A normal
/// route disarms the guard before returning its result.
pub struct RouteCancelGuard {
    tx: mpsc::UnboundedSender<Message>,
    request: Option<u64>,
}

impl RouteCancelGuard {
    pub fn new(tx: mpsc::UnboundedSender<Message>, request: u64) -> Self {
        Self {
            tx,
            request: Some(request),
        }
    }

    pub fn disarm(&mut self) {
        self.request = None;
    }
}

impl Drop for RouteCancelGuard {
    fn drop(&mut self) {
        if let Some(request) = self.request {
            let _ = self.tx.send(Message::CancelRoute { request });
        }
    }
}

impl Drop for IngressAbortGuard {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            self.connection.complete(&cancellation);
            cancellation.publish_abort();
        }
    }
}

/// One public HTTP request admitted before shutdown closed the gate. Peer
/// requests do not hold this guard because the handoff must keep serving them.
pub struct PublicRequestGuard {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for PublicRequestGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Default)]
pub struct DrainPinRegistry(Arc<std::sync::Mutex<DrainPinState>>);

#[derive(Default)]
struct DrainPinState {
    pins: BTreeMap<u64, DrainPinTrace>,
    requests: HashMap<crate::js::RequestId, u64>,
}

struct DrainPinTrace {
    request_id: Option<crate::js::RequestId>,
    origin: &'static str,
    started_mono_ms: u64,
    phase: &'static str,
    response_headers_sent: bool,
    response_body_active: bool,
    durable_txid: Option<u64>,
    output_gate_result: &'static str,
    ack_point: &'static str,
    cancellation_state: &'static str,
    cancel: watch::Sender<bool>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct DrainPinView {
    cell: String,
    core_request_id: Option<u64>,
    request_id: Option<String>,
    origin: &'static str,
    age_ms: u64,
    phase: &'static str,
    response_headers_sent: bool,
    response_body_active: bool,
    durable_txid: Option<u64>,
    output_gate_result: &'static str,
    ack_point: &'static str,
    active_request_count: usize,
    output_gate_count: usize,
    cancellation_state: &'static str,
    regular_websocket_count: usize,
    outbound_websocket_count: usize,
}

impl DrainPinRegistry {
    fn begin(
        &self,
        core_request: u64,
        request_id: Option<crate::js::RequestId>,
        origin: &'static str,
    ) -> watch::Receiver<bool> {
        let (cancel, receive) = watch::channel(false);
        let mut state = self.0.lock().unwrap();
        state.pins.insert(
            core_request,
            DrainPinTrace {
                request_id,
                origin,
                started_mono_ms: crate::asyncrt::mono_ms(),
                phase: "handler",
                response_headers_sent: false,
                response_body_active: false,
                durable_txid: None,
                output_gate_result: "not_applicable",
                ack_point: "not_acknowledged",
                cancellation_state: "not_cancelled",
                cancel,
            },
        );
        if let Some(request_id) = request_id {
            state.requests.insert(request_id, core_request);
        }
        receive
    }

    fn update(
        &self,
        core_request: u64,
        phase: &'static str,
        response_headers_sent: bool,
        response_body_active: bool,
    ) {
        if let Some(pin) = self.0.lock().unwrap().pins.get_mut(&core_request) {
            pin.phase = phase;
            pin.response_headers_sent = response_headers_sent;
            pin.response_body_active = response_body_active;
            if response_headers_sent {
                pin.ack_point = "response_headers_sent";
            }
        }
    }

    fn gate_started(&self, core_request: u64, durable_txid: u64) {
        if let Some(pin) = self.0.lock().unwrap().pins.get_mut(&core_request) {
            pin.durable_txid = Some(durable_txid);
            pin.output_gate_result = "pending";
        }
    }

    fn gate_finished(&self, core_request: u64, proven: bool) {
        if let Some(pin) = self.0.lock().unwrap().pins.get_mut(&core_request) {
            pin.output_gate_result = if proven { "proven" } else { "failed" };
        }
    }

    pub fn cancel_request(
        &self,
        request_id: crate::js::RequestId,
        cancellation_state: &'static str,
    ) -> Option<&'static str> {
        let mut state = self.0.lock().unwrap();
        let core_request = state.requests.get(&request_id).copied()?;
        let pin = state.pins.get_mut(&core_request)?;
        pin.cancellation_state = cancellation_state;
        let _ = pin.cancel.send(true);
        Some(pin.phase)
    }

    fn cancel_core_request(
        &self,
        core_request: u64,
        cancellation_state: &'static str,
    ) -> Option<(Option<crate::js::RequestId>, &'static str)> {
        let mut state = self.0.lock().unwrap();
        let pin = state.pins.get_mut(&core_request)?;
        pin.cancellation_state = cancellation_state;
        let _ = pin.cancel.send(true);
        Some((pin.request_id, pin.phase))
    }

    fn finish(&self, core_request: u64) -> Option<DrainPinTrace> {
        let mut state = self.0.lock().unwrap();
        let pin = state.pins.remove(&core_request)?;
        if let Some(request_id) = pin.request_id {
            state.requests.remove(&request_id);
        }
        Some(pin)
    }

    fn snapshot(&self, pins: Vec<celld_logic::DrainPin>) -> Vec<DrainPinView> {
        let state = self.0.lock().unwrap();
        pins.into_iter()
            .map(|pin| {
                let trace = pin.request.and_then(|request| state.pins.get(&request));
                DrainPinView {
                    cell: pin.cell,
                    core_request_id: pin.request,
                    request_id: trace
                        .and_then(|trace| trace.request_id)
                        .map(crate::js::request_id_string),
                    origin: trace.map_or("unknown", |trace| trace.origin),
                    age_ms: trace.map_or(0, |trace| {
                        crate::asyncrt::mono_ms().saturating_sub(trace.started_mono_ms)
                    }),
                    phase: trace.map_or("unknown", |trace| trace.phase),
                    response_headers_sent: trace.is_some_and(|trace| trace.response_headers_sent),
                    response_body_active: trace.is_some_and(|trace| trace.response_body_active),
                    durable_txid: trace.and_then(|trace| trace.durable_txid),
                    output_gate_result: trace.map_or("unknown", |trace| trace.output_gate_result),
                    ack_point: trace.map_or("unknown", |trace| trace.ack_point),
                    active_request_count: pin.active_request_count,
                    output_gate_count: pin.output_gate_count,
                    cancellation_state: trace.map_or("unknown", |trace| trace.cancellation_state),
                    regular_websocket_count: pin.regular_websocket_count,
                    outbound_websocket_count: pin.outbound_websocket_count,
                }
            })
            .collect()
    }
}

pub struct ActivityGuard {
    tx: mpsc::UnboundedSender<Message>,
    request: u64,
    runtime: Option<RuntimeManager>,
    cell: String,
    drain_pins: DrainPinRegistry,
    cancellation: watch::Receiver<bool>,
}

impl ActivityGuard {
    pub fn set_phase(
        &self,
        phase: &'static str,
        response_headers_sent: bool,
        response_body_active: bool,
    ) {
        self.drain_pins.update(
            self.request,
            phase,
            response_headers_sent,
            response_body_active,
        );
    }

    pub fn cancellation(&self) -> watch::Receiver<bool> {
        self.cancellation.clone()
    }

    pub fn gate_started(&self, durable_txid: u64) {
        self.drain_pins.gate_started(self.request, durable_txid);
    }

    pub fn gate_finished(&self, proven: bool) {
        self.drain_pins.gate_finished(self.request, proven);
    }
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        if let Some(pin) = self.drain_pins.finish(self.request) {
            if let Some(request_id) = pin
                .request_id
                .filter(|_| tracing::enabled!(target: "timing", tracing::Level::DEBUG))
            {
                tracing::debug!(
                    target: "timing",
                    event = "cell_request_lifecycle",
                    core_request_id = self.request,
                    request_id = %crate::js::request_id_string(request_id),
                    scope = %self.cell,
                    origin = pin.origin,
                    age_ms = crate::asyncrt::mono_ms().saturating_sub(pin.started_mono_ms),
                    phase = pin.phase,
                    response_headers_sent = pin.response_headers_sent,
                    response_body_active = pin.response_body_active,
                    durable_txid = pin.durable_txid,
                    output_gate_result = pin.output_gate_result,
                    ack_point = pin.ack_point,
                    cancellation_state = pin.cancellation_state,
                    "cell request lifetime ended"
                );
            }
        }
        // Read and send under the registry lock (`with_alarm`), so this
        // report cannot carry an alarm older than one the reporter already
        // sent — a stale fold arriving later would unarm the core and
        // delete the wake entry (`alarm_reporter` documents the ordering).
        let send = |tx: &mpsc::UnboundedSender<Message>, at_ms, covered| {
            let _ = tx.send(Message::ActivityFinished {
                request: self.request,
                cell: self.cell.clone(),
                alarm_at_ms: at_ms,
                alarm_covered: covered,
            });
        };
        match &self.runtime {
            Some(runtime) => runtime.with_alarm(&self.cell, |at_ms| {
                let covered = runtime.alarm_covered(&self.cell, at_ms);
                send(&self.tx, at_ms, covered);
            }),
            None => send(&self.tx, None, false),
        }
    }
}

#[derive(Clone, Copy)]
enum RouteStage {
    OwnershipRead,
    NodeLeaseLookup,
    CapacityLookup,
    OwnershipAcquire,
    Restore,
    IsolateStartup,
    RegistryInsert,
}

struct EffectTiming {
    cell: String,
    stage: RouteStage,
    elapsed_us: u64,
}

pub struct CompletedEffect {
    event: Event,
    timing: Option<EffectTiming>,
}

impl CompletedEffect {
    fn plain(event: Event) -> Self {
        Self {
            event,
            timing: None,
        }
    }

    fn timed(event: Event, cell: String, stage: RouteStage, started_mono_ms: u64) -> Self {
        Self {
            event,
            timing: Some(EffectTiming {
                cell,
                stage,
                elapsed_us: mono_elapsed_us(started_mono_ms),
            }),
        }
    }
}

struct CellRouteTiming {
    started_mono_ms: u64,
    activation_started: bool,
    capacity_wait_started_mono_ms: Option<u64>,
    latch_wait_us: u64,
    ownership_read_us: u64,
    node_lease_lookup_us: u64,
    capacity_lookup_us: u64,
    capacity_wait_us: u64,
    activation_slot_wait_us: u64,
    lease_permit_us: u64,
    ownership_acquire_us: u64,
    replica_discovery_us: u64,
    restore_us: u64,
    isolate_startup_us: u64,
    registry_insert_us: u64,
    fresh: Option<bool>,
}

impl CellRouteTiming {
    fn new() -> Self {
        Self {
            started_mono_ms: crate::asyncrt::mono_ms(),
            activation_started: false,
            capacity_wait_started_mono_ms: None,
            latch_wait_us: 0,
            ownership_read_us: 0,
            node_lease_lookup_us: 0,
            capacity_lookup_us: 0,
            capacity_wait_us: 0,
            activation_slot_wait_us: 0,
            lease_permit_us: 0,
            ownership_acquire_us: 0,
            replica_discovery_us: 0,
            restore_us: 0,
            isolate_startup_us: 0,
            registry_insert_us: 0,
            fresh: None,
        }
    }

    fn effect_started(&mut self) {
        if !self.activation_started {
            self.activation_started = true;
            self.activation_slot_wait_us = mono_elapsed_us(self.started_mono_ms);
        }
        if let Some(started) = self.capacity_wait_started_mono_ms.take() {
            self.capacity_wait_us = self
                .capacity_wait_us
                .saturating_add(mono_elapsed_us(started));
        }
    }

    fn record(&mut self, stage: RouteStage, elapsed_us: u64) {
        let field = match stage {
            RouteStage::OwnershipRead => &mut self.ownership_read_us,
            RouteStage::NodeLeaseLookup => &mut self.node_lease_lookup_us,
            RouteStage::CapacityLookup => &mut self.capacity_lookup_us,
            RouteStage::OwnershipAcquire => &mut self.ownership_acquire_us,
            RouteStage::Restore => &mut self.restore_us,
            RouteStage::IsolateStartup => &mut self.isolate_startup_us,
            RouteStage::RegistryInsert => &mut self.registry_insert_us,
        };
        *field = field.saturating_add(elapsed_us);
    }
}

/// Report a cell's runtime stopped, but only once the host has actually let
/// the cell go.
///
/// A stop that failed can leave the cell's realm holding its open database,
/// so reporting it stopped is what makes that dangerous: the core answers a
/// stop by starting the cell again — on the current application generation
/// for a swap, or on another node after a handoff — against storage the old
/// realm may still have. Both stop paths therefore retry rather than report,
/// and they share this loop so neither can drift into reporting first.
///
/// The retry is unbounded on purpose. The core arms an operation deadline on
/// the `Cleaning` phase this answers, so a host that never lets go is bounded
/// there rather than by a count guessed here.
async fn report_stopped_when_released<F, Fut>(
    op: OpId,
    cell: String,
    epoch: u64,
    event: &'static str,
    deadline_mono_ms: Option<u64>,
    mut release: F,
) -> CompletedEffect
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    while let Err(error) = release().await {
        if deadline_mono_ms.is_some_and(|deadline| crate::asyncrt::mono_ms() >= deadline) {
            tracing::warn!(
                event,
                %cell,
                epoch,
                %error,
                "the host did not release the cell before the deadline; restarting it in place"
            );
            return CompletedEffect::plain(Event::RuntimeStopFailed { op });
        }
        tracing::warn!(
            event,
            %cell,
            epoch,
            %error,
            "the host did not release the cell; retrying before reporting it stopped"
        );
        crate::asyncrt::sleep(std::time::Duration::from_millis(
            crate::replication::CELL_RELEASE_RETRY_BASE_MS,
        ))
        .await;
    }
    CompletedEffect::plain(Event::RuntimeStopped { op })
}

fn mono_elapsed_us(started_mono_ms: u64) -> u64 {
    crate::asyncrt::mono_ms()
        .saturating_sub(started_mono_ms)
        .saturating_mul(1_000)
}

pub type EffectFuture = Pin<Box<dyn Future<Output = CompletedEffect> + Send>>;
pub type ConnectionFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
pub type DoCallFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
pub type AssetCallFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
pub type WebSocketFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
pub type CachePruneFuture = Pin<
    Box<
        dyn Future<
                Output = (
                    u64,
                    Result<std::io::Result<(usize, usize, u64)>, crate::asyncrt::TaskPanic>,
                ),
            > + Send,
    >,
>;

/// One ready item selected for the serial Actor.
pub enum ActorInput {
    Message(Message),
    Completed(CompletedEffect),
    TimerFired(Timer),
}

/// Futures and timer arms emitted by one Actor transition.
#[derive(Default)]
pub struct StepOutput {
    pub effects: Vec<EffectFuture>,
    pub timers: Vec<TimerArm>,
}

fn drain_step_output(
    out: &mut StepOutput,
    effects: &mut FuturesUnordered<EffectFuture>,
    delays: &mut DelayQueue<TimerArm>,
    timers: &mut TimerSlots<delay_queue::Key>,
) {
    for effect in out.effects.drain(..) {
        effects.push(effect);
    }
    for arm in out.timers.drain(..) {
        let delay = std::time::Duration::from_millis(
            arm.at_mono_ms.saturating_sub(crate::asyncrt::mono_ms()),
        );
        let key = delays.insert(arm.clone(), delay);
        if let Some(displaced) = timers.install(&arm, key) {
            delays.remove(&displaced);
        }
    }
}

/// The largest request body that a node accepts unless the operator lowers
/// the limit. The value keeps the large git-pack ingress use case supported.
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 1 << 30;

/// The generation census `/state` reports beside the serving deployment.
pub struct SwapCensus {
    /// Resident cells moving to the current generation right now.
    pub swapping: usize,
    /// Each resident cell and the generation its runtime started on.
    pub generations: Vec<(String, u64)>,
}

#[derive(Clone)]
pub struct AppHandle {
    pub tx: mpsc::UnboundedSender<Message>,
    pub runtime: Option<RuntimeManager>,
    /// Asks the pointer watcher to adopt the current deployment now. Sending
    /// fails on a node with no pointer to watch, such as a local-script node.
    pub reload: crate::generation::ReloadSender,
    pub peer_http: reqwest::Client,
    pub peer_auth: Arc<PeerAuth>,
    pub advertise: String,
    pub websockets: mpsc::UnboundedSender<WebSocketFuture>,
    /// Whether the RPO=0 output gate is armed: hold a local write's response
    /// until its cell is proven durable. On by default; set `CELLD_OUTPUT_GATE=0`
    /// to acknowledge writes without proving them durable. The core and its DST
    /// are unconditional.
    pub output_gate: bool,
    /// Concurrent outbound WebSockets one cell may hold, for the refusal
    /// message; the core enforces it.
    pub max_outbound_websockets: usize,
    /// Set the instant a graceful shutdown begins, so `/.well-known/celld/health` reports
    /// unhealthy and a load balancer stops routing here before teardown.
    pub draining: Arc<std::sync::atomic::AtomicBool>,
    /// The one-shot fleet gate on the first health 200. A fresh process
    /// reports ready only after no peer donor is mid-handoff and the fleet has
    /// successor capacity, so a rolling update cannot advance faster than the
    /// fleet recovers. Fleet state never demotes readiness after the gate
    /// opens; peers reach owned cells directly regardless.
    pub fleet_ready: Arc<std::sync::atomic::AtomicBool>,
    /// Public requests which passed the shutdown gate and have not finished.
    /// This includes the complete top-level Worker call, so a cell write it
    /// goes on to make cannot outlive the pre-handoff durability boundary.
    pub public_in_flight: Arc<AtomicUsize>,
    /// Shell-side phases joined to the decision core's exact eviction pins.
    pub drain_pins: DrainPinRegistry,
    /// The log tier's follower store: this node holds peers' log fragments
    /// whatever its own durability posture. `None` without a bucket.
    pub follower: Option<Arc<crate::node_log::FollowerStore>>,
    /// Whether forwarded scheme and host headers can set `request.url`.
    /// The default is false because a direct client controls both headers.
    pub trust_forwarded_headers: bool,
    /// The maximum body size for public Worker ingress and direct Durable
    /// Object ingress. The same value must govern both entry points because a
    /// caller can choose either route for an ordinary Durable Object.
    pub max_request_body_bytes: usize,
    /// The complete retry budget for a contradicted remote ownership
    /// generation. This is the same operation deadline that bounds the
    /// decision core, so a shell retry cannot outlive its core operation.
    pub operation_deadline_ms: u64,
}

impl AppHandle {
    pub async fn request(&self, cell: String) -> Result<Routed, RequestError> {
        self.request_with_mode(cell, false, None).await
    }

    /// Route one test request and retain which asynchronous seam produced the
    /// result. The shipping method keeps its compact public result, while this
    /// receipt prevents an inference from `NodeFenced` or an error string.
    #[cfg(all(test, celld_internal_tests))]
    pub(crate) async fn request_with_receipt_for_test(
        &self,
        cell: String,
    ) -> RequestPathReceiptForTest {
        let (request, outcome) = self.request_path(cell, false, None).await;
        match outcome {
            RequestPathOutcome::SubmissionFailure => {
                RequestPathReceiptForTest::SubmissionFailure { request }
            }
            RequestPathOutcome::ReplyChannelClosed => {
                RequestPathReceiptForTest::ReplyChannelClosed { request }
            }
            RequestPathOutcome::Returned(result) => RequestPathReceiptForTest::Returned {
                allocated_request: request,
                result: result.map(|routed| (routed.request, routed.route)),
            },
        }
    }

    pub async fn capacity_request(&self, cell: String) -> Result<Routed, RequestError> {
        self.request_with_mode(cell, true, None).await
    }

    pub async fn websocket_request(
        &self,
        cell: String,
        websocket: u64,
    ) -> Result<Routed, RequestError> {
        self.request_with_mode(cell, false, Some(websocket)).await
    }

    async fn request_with_mode(
        &self,
        cell: String,
        capacity_handoff: bool,
        websocket: Option<u64>,
    ) -> Result<Routed, RequestError> {
        match self.request_path(cell, capacity_handoff, websocket).await.1 {
            RequestPathOutcome::Returned(result) => result,
            RequestPathOutcome::SubmissionFailure | RequestPathOutcome::ReplyChannelClosed => {
                Err(RequestError::NodeFenced)
            }
        }
    }

    async fn request_path(
        &self,
        cell: String,
        capacity_handoff: bool,
        websocket: Option<u64>,
    ) -> (u64, RequestPathOutcome) {
        let request = crate::asyncrt::next_core_request();
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::Request {
                request,
                cell,
                capacity_handoff,
                handoff_accept: None,
                websocket,
                reply,
            })
            .is_err()
        {
            return (request, RequestPathOutcome::SubmissionFailure);
        }
        let mut cancel = RouteCancelGuard::new(self.tx.clone(), request);
        let outcome = match receive.await {
            Ok(result) => RequestPathOutcome::Returned(result),
            Err(_) => RequestPathOutcome::ReplyChannelClosed,
        };
        cancel.disarm();
        (request, outcome)
    }

    /// Acquire a released cell without materializing its runtime. The donor's
    /// final snapshot makes a later demand-driven restore authoritative, and
    /// avoiding an eager restore keeps a fleet restart from becoming a cold
    /// start storm.
    pub async fn accept_handoff(
        &self,
        cell: String,
        released_epoch: u64,
    ) -> Result<HandoffAttempt, RequestError> {
        let request = crate::asyncrt::next_core_request();
        let (route_reply, mut route_receive) = oneshot::channel();
        let (accept_reply, mut accept_receive) = oneshot::channel();
        if self
            .tx
            .send(Message::Request {
                request,
                cell: cell.clone(),
                capacity_handoff: true,
                handoff_accept: Some(HandoffAcceptWaiter {
                    released_epoch,
                    reply: accept_reply,
                }),
                websocket: None,
                reply: route_reply,
            })
            .is_err()
        {
            return Err(RequestError::NodeFenced);
        }
        let mut cancel = RouteCancelGuard::new(self.tx.clone(), request);
        let result = crate::asyncrt::select_biased! {
            "a completed route wins a tie so current ownership precedes the accept acknowledgement";
            routed = &mut route_receive => match routed
                .unwrap_or(Err(RequestError::NodeFenced))?
            {
                Routed {
                    request,
                    route: Route::Local,
                } => {
                    // A repeated request can find the cell published already.
                    drop(self.activity(request, cell.clone()));
                    self.resident_epoch(cell)
                        .await
                        .filter(|epoch| *epoch > released_epoch)
                        .map(|epoch| HandoffAttempt::Accepted(AdoptedCell {
                            node: self.peer_auth.source().to_string(),
                            addr: self.advertise.clone(),
                            epoch,
                            peer_protocol: peer_auth::PROTOCOL_VERSION,
                        }))
                        .ok_or(RequestError::AcquireFailed)
                }
                Routed {
                    route: Route::Remote {
                        node,
                        addr,
                        epoch,
                        peer_protocol,
                    },
                    ..
                } => Ok(HandoffAttempt::CurrentOwner(AdoptedCell {
                    node,
                    addr,
                    epoch,
                    peer_protocol,
                })),
            },
            accepted = &mut accept_receive => accepted
                .map(HandoffAttempt::Accepted)
                .map_err(|_| RequestError::NodeFenced),
        };
        // The ownership acknowledgement deliberately leaves no route waiter:
        // real traffic, not this synthetic request, starts materialization.
        cancel.disarm();
        let _ = self.tx.send(Message::CancelRoute { request });
        result
    }

    // Orphaned by the always-pool Worker entry below. What remains of the
    // landing-cell machinery -- this, `Message::WorkerRequest`, `WorkerRouted`,
    // `fetch_worker_on_cell`, `CellJob::WorkerFetch`, and the logic
    // `worker_request` -- is kept together so it can be removed in one piece
    // rather than unpicked here. The in-isolate dispatch it was paired with is
    // already gone, and with it the gate that path needed.
    #[allow(dead_code)]
    async fn worker_route(&self) -> anyhow::Result<WorkerRouted> {
        let (reply, receive) = oneshot::channel();
        self.tx
            .send(Message::WorkerRequest { reply })
            .map_err(|_| anyhow::anyhow!("core stopped before Worker routing"))?;
        receive
            .await
            .context("core stopped while routing Worker request")
    }

    pub async fn fetch_worker(
        &self,
        url: String,
        method: String,
        body: crate::js::RequestBody,
        headers: Vec<(String, String)>,
        connection: ConnectionWorkerRequests,
    ) -> anyhow::Result<HttpResponse> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Worker runtime unavailable"))?;
        // Admission happens in the runtime, against the pool that actually
        // holds the requests. It used to be duplicated here against an empty
        // `PoolLoad` — a snapshot with no isolates and no affiliations, which
        // could only ever answer "yes" once pressure stopped refusing
        // outright. A decision made against a fake reading is not a fast path.
        let cancellation = crate::runtime::RequestCancellationLifetime::stateless();
        // The Worker entry is stateless — run it in the pool, always. Routing it
        // to a "landing cell" existed only to make an `env.NS.get(id)` call
        // resolve inline; but a cell runs one fetch at a time, so under any real
        // concurrency the entry can't be on the cell it calls, and the routing
        // round-trip is paid for nothing — on top of the DO call's own routing.
        // A DO call reaches its owning cell (with fencing and the output gate) on
        // the host path, so one routing round-trip per request, never two.
        let fetching = runtime.fetch_worker_pool(url, method, body, headers, cancellation.clone());
        let mut abort = IngressAbortGuard::new(cancellation, connection);
        let result = fetching.await;
        abort.disarm();
        result
    }

    pub fn activity(&self, request: u64, cell: String) -> ActivityGuard {
        self.activity_for(request, cell, None, "local")
    }

    pub fn activity_for(
        &self,
        request: u64,
        cell: String,
        request_id: Option<crate::js::RequestId>,
        origin: &'static str,
    ) -> ActivityGuard {
        let cancellation = self.drain_pins.begin(request, request_id, origin);
        ActivityGuard {
            tx: self.tx.clone(),
            request,
            runtime: self.runtime.clone(),
            cell,
            drain_pins: self.drain_pins.clone(),
            cancellation,
        }
    }

    /// Hold a response until every write it can reveal is durable. A write
    /// supplies its ending position. A read supplies `None` and trails the
    /// newest outstanding write on the same cell, if one exists. A ticket
    /// whose position was sampled before `request` pinned the cell supplies
    /// the epoch it sampled at, and the core refuses it when the cell has
    /// since moved to another epoch.
    pub async fn gate_output(&self, request: u64, ticket: GateTicket) -> Result<(), RequestError> {
        match self.gate_output_path(request, ticket).await {
            OutputGateOutcome::Returned(result) => result,
            OutputGateOutcome::SubmissionFailure | OutputGateOutcome::ReplyChannelClosed => {
                Err(RequestError::NodeFenced)
            }
        }
    }

    async fn gate_output_path(&self, request: u64, ticket: GateTicket) -> OutputGateOutcome {
        let started_mono_ms = crate::asyncrt::mono_ms();
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::Output {
                request,
                ticket,
                reply,
            })
            .is_err()
        {
            return OutputGateOutcome::SubmissionFailure;
        }
        let outcome = match receive.await {
            Ok(result) => OutputGateOutcome::Returned(result),
            Err(_) => OutputGateOutcome::ReplyChannelClosed,
        };
        tracing::debug!(
            target: "timing",
            event = "gate_write_timing",
            request,
            total_us = mono_elapsed_us(started_mono_ms),
            "output gate resolved"
        );
        outcome
    }

    /// Run one test output gate without collapsing its two channel failures
    /// into a returned engine rejection. The request travels in every arm, so
    /// a consumer cannot attach the receipt to another caller.
    #[cfg(all(test, celld_internal_tests))]
    pub(crate) async fn gate_output_with_receipt_for_test(
        &self,
        request: u64,
        ticket: GateTicket,
    ) -> OutputGateReceiptForTest {
        match self.gate_output_path(request, ticket).await {
            OutputGateOutcome::SubmissionFailure => {
                OutputGateReceiptForTest::SubmissionFailure { request }
            }
            OutputGateOutcome::ReplyChannelClosed => {
                OutputGateReceiptForTest::ReplyChannelClosed { request }
            }
            OutputGateOutcome::Returned(result) => {
                OutputGateReceiptForTest::Returned { request, result }
            }
        }
    }

    pub async fn gate_write(&self, request: u64, position: u64) -> Result<(), RequestError> {
        self.gate_output(request, GateTicket::response(Some(position), None))
            .await
    }

    /// Hand a finished `webSocketMessage`'s frames to the cell's output gate.
    /// Awaited so the request stays pinned until the actor has registered the
    /// gate (the core reads the still-active request when it opens); frames flush
    /// or fail asynchronously as durability resolves, not here.
    ///
    /// An error means the actor has stopped and registered nothing: no
    /// barrier opened, so a caller that relied on one for an unproven write
    /// must not treat the write as covered.
    pub async fn ws_output(
        &self,
        request: u64,
        scope: String,
        frames: Vec<(u64, WsOut)>,
        write_position: Option<u64>,
        observed: Option<u64>,
    ) -> Result<(), ActorStopped> {
        let (reply, receive) = oneshot::channel();
        self.tx
            .send(Message::WsOutput {
                request,
                scope,
                frames,
                write_position,
                observed,
                reply,
            })
            .map_err(|_| ActorStopped)?;
        receive.await.map_err(|_| ActorStopped)
    }

    pub async fn websocket_opened(
        &self,
        cell: String,
        websocket: u64,
        kind: WebSocketKind,
    ) -> anyhow::Result<()> {
        let (reply, receive) = oneshot::channel();
        self.tx
            .send(Message::WebSocketOpened {
                cell,
                websocket,
                kind,
                reply,
            })
            .map_err(|_| anyhow::anyhow!("core stopped before WebSocket opened"))?;
        let held = receive
            .await
            .context("core stopped while opening WebSocket")?;
        anyhow::ensure!(
            held,
            "outbound WebSocket refused: a cell may hold at most {}, and a node \
             may pin at most {}% of its residency ceiling",
            self.max_outbound_websockets,
            celld_logic::pressure::MAX_OUTBOUND_PIN_PERCENT,
        );
        Ok(())
    }

    pub fn websocket_closed(&self, cell: String, websocket: u64) {
        let _ = self.tx.send(Message::WebSocketClosed { cell, websocket });
    }

    pub async fn evict(&self, cell: String) {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Evict { cell, reply }).is_ok() {
            let _ = receive.await;
        }
    }

    pub async fn invalidate_remote(&self, cell: String, node: String, epoch: u64) {
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::InvalidateRemote {
                cell,
                node,
                epoch,
                reply,
            })
            .is_ok()
        {
            let _ = receive.await;
        }
    }

    pub async fn snapshot(&self) -> String {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Snapshot { reply }).is_err() {
            return "{\"error\":\"actor_stopped\"}".into();
        }
        let Ok((state, pins, census)) = receive.await else {
            return "{\"error\":\"actor_stopped\"}".into();
        };
        let pins = self.drain_pins.snapshot(pins);
        let Ok(mut state) = serde_json::from_str::<serde_json::Value>(&state) else {
            return state;
        };
        if !pins.is_empty() {
            state["drainPins"] =
                serde_json::to_value(pins).unwrap_or_else(|_| serde_json::json!([]));
        }
        // Which deployment this node serves, and which ones it is still
        // draining. Nothing else in the process records the version, so
        // this is how an operator learns whether a deploy took.
        if let Some(runtime) = &self.runtime {
            let generation = runtime.generation();
            state["deployment"] = serde_json::json!({
                "version": generation.version(),
                "prefix": generation.prefix(),
                "generation": generation.id(),
                "draining": runtime
                    .draining_generations()
                    .into_iter()
                    .map(|(id, version)| serde_json::json!({"generation": id, "version": version}))
                    .collect::<Vec<_>>(),
                // Resident cells moving to the current generation right now,
                // and the generation each resident cell runs: the two numbers
                // an operator watches while a deployment converges.
                "swapping": census.swapping,
                "cells": census
                    .generations
                    .into_iter()
                    .map(|(cell, generation)| (cell, serde_json::Value::from(generation)))
                    .collect::<serde_json::Map<_, _>>(),
            });
        }
        state.to_string()
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn fleet_ready(&self) -> bool {
        self.fleet_ready.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Atomically cross the public shutdown gate. The second draining read
    /// closes the race where shutdown stores the flag between the first read
    /// and the counter increment: that request backs out before user code runs.
    pub fn admit_public(&self) -> Option<PublicRequestGuard> {
        if self.draining.load(Ordering::SeqCst) {
            return None;
        }
        self.public_in_flight.fetch_add(1, Ordering::SeqCst);
        if self.draining.load(Ordering::SeqCst) {
            self.public_in_flight.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(PublicRequestGuard {
            in_flight: self.public_in_flight.clone(),
        })
    }

    pub fn public_in_flight(&self) -> usize {
        self.public_in_flight.load(Ordering::SeqCst)
    }

    /// A dead actor has nothing left to hand off, so channel failure
    /// reports drained rather than wedging shutdown.
    pub async fn drain_status(&self) -> DrainStatus {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Drained { reply }).is_err() {
            return DrainStatus::default();
        }
        receive.await.unwrap_or_default()
    }

    pub async fn healthy(&self) -> bool {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Health { reply }).is_err() {
            return false;
        }
        receive.await.unwrap_or(false)
    }

    pub async fn presence(&self) -> Option<celld_logic::PresenceSnapshot> {
        let (reply, receive) = oneshot::channel();
        self.tx.send(Message::Presence { reply }).ok()?;
        receive.await.ok()
    }

    async fn resident_epoch(&self, cell: String) -> Option<u64> {
        let (reply, receive) = oneshot::channel();
        self.tx.send(Message::ResidentEpoch { cell, reply }).ok()?;
        receive.await.ok().flatten()
    }
}

/// One cell's WebSocket output gate: an ordered queue of write barriers.
#[derive(Default)]
struct WsGate {
    barriers: VecDeque<WsBarrier>,
}

/// One `webSocketMessage` batch and the outbound frames held behind it.
/// `settled` is `None` while the verdict is outstanding, `Some(true)` once the
/// core proved every write the frames can reveal (they may flush when this
/// barrier reaches the front), `Some(false)` on failure (the gate breaks).
///
/// Every batch gets its own barrier, including one that wrote nothing: the core
/// decides what it trails, because only the core knows the writes other
/// channels have outstanding on this cell. The queue keeps them in arrival
/// order and drains only from the front, so a barrier settled early cannot
/// overtake one still waiting.
struct WsBarrier {
    request: u64,
    settled: Option<bool>,
    frames: Vec<(u64, WsOut)>,
}

/// The actor stopped before it answered: its channel is closed, and nothing
/// it was asked to register exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorStopped;

impl std::fmt::Display for ActorStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the cell actor stopped before it registered the output")
    }
}

impl std::error::Error for ActorStopped {}

/// What one output-gate ticket asks: the route it holds, the committed-write
/// position it must see proven, and the activation epoch that position was
/// sampled at.
///
/// One value rather than three arguments, because a position without its
/// epoch is a ticket a reset can satisfy at the wrong epoch (see
/// `Event::Output`): the routed path samples in the handler's turn and only
/// then acquires the request that pins the cell, and a caller that could pass
/// the position alone would drop the epoch without the compiler noticing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GateTicket {
    pub channel: Channel,
    pub position: Option<u64>,
    /// The committed-write position a read-only output observed above the
    /// cell's published baseline, when a handler had advanced it. The core
    /// holds the output until a verified proof covers it, whether or not the
    /// handler that wrote has taken a ticket yet. Read only when `position`
    /// is `None`.
    pub observed: Option<u64>,
    pub epoch: Option<u64>,
}

impl GateTicket {
    /// The handler's own response. Its request was active on the cell before
    /// the positions were sampled, and a reset deactivates every request of
    /// the cell it discards, so the core already refuses a stale one: no
    /// epoch needs to travel. Both positions come from one sample, so a
    /// read-only response cannot leave its observed position behind.
    pub const fn response(position: Option<u64>, observed: Option<u64>) -> Self {
        Self {
            channel: Channel::Response,
            position,
            observed,
            epoch: None,
        }
    }
}

/// The shell identity for one output held by the core.
///
/// One request can hold output on multiple channels. Keeping both values in
/// the map key prevents a later channel from replacing the first channel's
/// waiter before the core releases it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HeldOutputKey {
    request: u64,
    channel: Channel,
}

impl HeldOutputKey {
    const fn websocket(request: u64) -> Self {
        Self {
            request,
            channel: Channel::WsHibernatable,
        }
    }
}

pub struct Actor {
    state: ActorState,
    pub ownership: Ownership,
    peer_http: reqwest::Client,
    peer_auth: Arc<PeerAuth>,
    paced_handoff: bool,
    operation_deadline_ms: u64,
    handoff_directory: Arc<Mutex<HandoffDirectory>>,
    host: Option<CellHost>,
    drain_pins: DrainPinRegistry,
    /// Per-cell handoff pipeline timings, one entry per quiesce nomination.
    /// Each phase is measured from effect dispatch to its answering event,
    /// so queue time counts because that is the serial cost of the drain.
    handoff_timings: HashMap<String, HandoffCellTiming>,
    /// The in-flight pipeline step for each timed handoff operation.
    handoff_phase_ops: HashMap<OpId, (String, HandoffPhase, u64)>,
    /// The node-log manager, filled at boot once the log tier exists:
    /// the executor's RecoverNodeLog effect runs through it directly.
    pub node_log: Arc<std::sync::Mutex<Option<Arc<crate::node_log::NodeLogManager>>>>,
    region: String,
    pending: BTreeMap<u64, oneshot::Sender<Result<Routed, RequestError>>>,
    request_cells: BTreeMap<u64, String>,
    handoff_accepts: BTreeMap<u64, (String, HandoffAcceptWaiter)>,
    route_timings: BTreeMap<String, CellRouteTiming>,
    pending_workers: BTreeMap<u64, oneshot::Sender<WorkerRouted>>,
    eviction_waiters: BTreeMap<String, Vec<oneshot::Sender<()>>>,
    durability_waiters: BTreeMap<u64, (String, Vec<oneshot::Sender<()>>)>,
    eviction_stops: BTreeMap<u64, Vec<oneshot::Sender<()>>>,
    /// Outputs held by the gate, keyed by request and channel. Every output but
    /// a captured `WsHibernatable` frame waits here for the core's `Release`.
    gated_responses: BTreeMap<HeldOutputKey, oneshot::Sender<Result<(), RequestError>>>,
    /// Per-cell WebSocket output gate: a FIFO of write barriers, each holding
    /// the outbound frames produced up to it. The front drains in write order as
    /// durability proves, so no client ever sees a frame that trails an
    /// unproven write (the Cloudflare per-object output gate).
    ws_gates: BTreeMap<String, WsGate>,
    /// Gated `webSocketMessage` batches map each request-channel pair to its
    /// cell, so `Release` routes to the barrier queue rather than a oneshot.
    ws_gated: BTreeMap<HeldOutputKey, String>,
    published: BTreeSet<String>,
    fail_publish_once: bool,
    publishes: u64,
    stops: u64,
    pub lease_spec: NodeLeaseSpec,
    /// Whether to re-check every core invariant after every event.
    ///
    /// The check is a full scan of the cell table, so its cost grows with
    /// residency: roughly 800us per event at ten thousand resident cells,
    /// which would cap a busy node at about a thousand events a second in
    /// assertion code alone. It is a model-checking assertion, and the place
    /// it earns its keep is simulation, which can afford to run it after
    /// every event over far more schedules than production will ever see.
    /// Debug builds keep it. Release builds rely on the deterministic model,
    /// because this full-table scan is too expensive for production traffic.
    validate_invariants: bool,
    /// The counters peers rank this node by, when there is a bucket to
    /// publish them to.
    live_load: Option<Arc<crate::ownership_store::LiveLoad>>,
    /// The shed reason last reported to the log, so a latch that holds for
    /// minutes is reported once rather than on every sample.
    logged_shed_reason: Option<&'static str>,
    fence: mpsc::UnboundedSender<i32>,
    timer_slots: TimerSlots<()>,
    started: bool,
    preserving: bool,
}

/// One handoff pipeline step whose dispatch-to-completion time is measured.
#[derive(Clone, Copy, PartialEq)]
enum HandoffPhase {
    Durability,
    Stop,
    Release,
    Adopt,
}

/// The accumulating phase durations for one quiescing cell's handoff.
struct HandoffCellTiming {
    nominated_mono_ms: u64,
    quiesce_us: Option<u64>,
    durability_us: Option<u64>,
    stop_us: Option<u64>,
    release_us: Option<u64>,
}

#[derive(Default)]
struct HandoffDirectory {
    sampled_at_mono_ms: Option<u64>,
    peers: Vec<CapacityPeer>,
}

const HANDOFF_DIRECTORY_MAX_AGE_MS: u64 = 500;
const HANDOFF_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(250);

/// A deterministic final tie-break spreads peers whose projected load is equal.
fn handoff_tie_break(cell: &str, node: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in cell
        .as_bytes()
        .iter()
        .copied()
        .chain(std::iter::once(0xff))
        .chain(node.as_bytes().iter().copied())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

async fn handoff_candidates(
    ownership: &Ownership,
    directory: &Arc<Mutex<HandoffDirectory>>,
    source: &str,
    cell: &str,
    rebalance: bool,
) -> Result<Vec<CapacityPeer>, Failure> {
    let mut directory = directory.lock().await;
    let now_mono_ms = crate::asyncrt::mono_ms();
    let stale = directory
        .sampled_at_mono_ms
        .is_none_or(|sampled| now_mono_ms.saturating_sub(sampled) >= HANDOFF_DIRECTORY_MAX_AGE_MS);
    if stale {
        directory.peers = ownership.read_capacity_peers().await?;
        directory.sampled_at_mono_ms = Some(now_mono_ms);
    }
    let peers = directory.peers.clone();
    let mut ranked = rank_handoff_candidates(peers, source, cell, now_ms());
    if rebalance {
        // A drain takes any peer with room. A balancing move takes only a
        // peer below its ownership target, least dense first; a fuller peer
        // would hand the cell straight back on its own next sample.
        let receivers = celld_logic::rebalance::receivers(
            &directory.peers,
            source,
            now_ms(),
            crate::machine::lease_ttl_ms_from_environment(),
        );
        ranked.retain(|peer| receivers.contains(&peer.node));
        ranked.sort_by_key(|peer| receivers.iter().position(|node| *node == peer.node));
        if ranked.is_empty() {
            // The plan saw room that this sample does not. The adoption
            // retries until its deadline, so this repeats; without it a
            // failed balancing move left no trace of why.
            tracing::debug!(
                event = "rebalance_no_receiver",
                %cell,
                below_target = receivers.len(),
                "no peer below its target can take the cell"
            );
        }
    }
    Ok(ranked)
}

fn rank_handoff_candidates(
    peers: Vec<CapacityPeer>,
    source: &str,
    cell: &str,
    current_ms: u64,
) -> Vec<CapacityPeer> {
    let mut peers: Vec<_> = peers
        .into_iter()
        .filter(|peer| {
            peer.node != source
                && peer.expires_ms > current_ms
                && !peer.addr.is_empty()
                && peer.peer_protocol == peer_auth::PROTOCOL_VERSION
                && peer.sampled_ms != 0
                && !peer.pressured
                && peer.memory_headroom != Some(false)
                && peer.paced_handoff
        })
        .collect();
    peers.sort_by_key(|peer| {
        (
            peer.resident_cells,
            peer.host_websockets,
            peer.in_use_bytes.unwrap_or(peer.rss_bytes),
            handoff_tie_break(cell, &peer.node),
            peer.node.clone(),
        )
    });
    peers
}

pub enum HandoffAttempt {
    Accepted(AdoptedCell),
    CurrentOwner(AdoptedCell),
    Refused,
}

async fn request_peer_handoff(
    http: &reqwest::Client,
    auth: &PeerAuth,
    peer: &AdoptedCell,
    request: &HandoffRequest,
) -> anyhow::Result<HandoffAttempt> {
    let path = "/peer/handoff";
    let body = serde_json::to_vec(request)?;
    let outbound = auth.sign(
        http.post(format!("http://{}{path}", peer.addr)),
        "POST",
        path,
        &body,
        &peer.node,
    )?;
    let response = outbound.body(body).send().await?;
    peer_auth::validate_response(response.headers())?;
    let status = response.status();
    let response: HandoffResponse = match response.json().await {
        Ok(response) => response,
        Err(_) => return Ok(HandoffAttempt::Refused),
    };
    let owner = AdoptedCell {
        node: response.node,
        addr: response.addr,
        epoch: response.epoch,
        peer_protocol: response.peer_protocol,
    };
    if status == reqwest::StatusCode::OK && response.published {
        Ok(HandoffAttempt::Accepted(owner))
    } else if status == reqwest::StatusCode::CONFLICT && !response.published {
        Ok(HandoffAttempt::CurrentOwner(owner))
    } else {
        Ok(HandoffAttempt::Refused)
    }
}

pub struct AdmissionLimits {
    pub resident: usize,
    pub activations: usize,
    pub evictions: usize,
    pub releases: usize,
    /// The ownership share this node publishes for rebalancing.
    pub placement_weight: u64,
}

pub struct ActorIdentity {
    pub node: String,
    pub advertise: String,
    pub region: String,
}

/// The test-only decision state that must be installed as one unit.
///
/// A scripted Actor cannot read process-wide configuration while concurrent
/// Worlds construct other nodes. The resume generation and the core
/// configuration therefore travel together to the private constructor.
#[cfg(all(test, celld_internal_tests))]
pub(crate) struct ScriptedActorDecisionState {
    pub(crate) resume_generation: Option<String>,
    pub(crate) placement_weight: u64,
    pub(crate) config: Config,
}

pub struct ActorServices {
    pub runtime: Option<RuntimeManager>,
    pub drain_pins: DrainPinRegistry,
    pub ownership: Option<Ownership>,
    pub peer_http: reqwest::Client,
    pub peer_auth: Arc<PeerAuth>,
    pub paced_handoff: bool,
}

struct ActorHostServices {
    host: Option<CellHost>,
    drain_pins: DrainPinRegistry,
    ownership: Option<Ownership>,
    peer_http: reqwest::Client,
    peer_auth: Arc<PeerAuth>,
    paced_handoff: bool,
}

impl Actor {
    pub async fn from_environment(
        limits: AdmissionLimits,
        fail_publish_once: bool,
        fence: mpsc::UnboundedSender<i32>,
        runtime: Option<RuntimeManager>,
        ownership: Option<Ownership>,
        identity: ActorIdentity,
        resume_generation: Option<String>,
    ) -> anyhow::Result<Self> {
        let peer_auth = Arc::new(PeerAuth::new([0; 32], identity.node.clone())?);
        Self::from_environment_with_cell_host(
            limits,
            fail_publish_once,
            fence,
            ActorHostServices {
                host: runtime.map(CellHost::V8),
                drain_pins: DrainPinRegistry::default(),
                ownership,
                peer_http: reqwest::Client::new(),
                peer_auth,
                paced_handoff: false,
            },
            identity,
            resume_generation,
        )
        .await
    }

    pub async fn from_environment_with_services(
        limits: AdmissionLimits,
        fail_publish_once: bool,
        fence: mpsc::UnboundedSender<i32>,
        services: ActorServices,
        identity: ActorIdentity,
        resume_generation: Option<String>,
    ) -> anyhow::Result<Self> {
        let ActorServices {
            runtime,
            drain_pins,
            ownership,
            peer_http,
            peer_auth,
            paced_handoff,
        } = services;
        Self::from_environment_with_cell_host(
            limits,
            fail_publish_once,
            fence,
            ActorHostServices {
                host: runtime.map(CellHost::V8),
                drain_pins,
                ownership,
                peer_http,
                peer_auth,
                paced_handoff,
            },
            identity,
            resume_generation,
        )
        .await
    }

    #[cfg(all(test, celld_internal_tests))]
    pub(crate) async fn from_environment_with_scripted_host(
        limits: AdmissionLimits,
        fail_publish_once: bool,
        fence: mpsc::UnboundedSender<i32>,
        host: crate::conformance_sim_cell_host::SimCellHost,
        ownership: Option<Ownership>,
        identity: ActorIdentity,
        resume_generation: Option<String>,
    ) -> anyhow::Result<Self> {
        let peer_auth = Arc::new(PeerAuth::new([0; 32], identity.node.clone())?);
        Self::from_environment_with_cell_host(
            limits,
            fail_publish_once,
            fence,
            ActorHostServices {
                host: Some(CellHost::Scripted(host)),
                drain_pins: DrainPinRegistry::default(),
                ownership,
                peer_http: reqwest::Client::new(),
                peer_auth,
                paced_handoff: false,
            },
            identity,
            resume_generation,
        )
        .await
    }

    /// Build the scripted Actor with one recorded core configuration.
    ///
    /// Concurrent node construction cannot safely mutate process-wide
    /// environment variables. Build the ordinary scripted shell first, then
    /// install the supplied configuration in both the unstarted decision core
    /// and the shell fields that consume it. The shell services, ownership,
    /// and host boundary stay identical to the ordinary constructor.
    #[cfg(all(test, celld_internal_tests))]
    pub(crate) async fn from_environment_with_scripted_host_and_config(
        fail_publish_once: bool,
        fence: mpsc::UnboundedSender<i32>,
        host: crate::conformance_sim_cell_host::SimCellHost,
        ownership: Option<Ownership>,
        identity: ActorIdentity,
        decision_state: ScriptedActorDecisionState,
    ) -> anyhow::Result<Self> {
        let ScriptedActorDecisionState {
            resume_generation,
            placement_weight,
            config,
        } = decision_state;
        let operation_deadline_ms = config
            .operation_deadline_ms
            .filter(|deadline| *deadline > 0)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "the scripted Actor configuration requires a positive operation deadline"
                )
            })?;
        let limits = AdmissionLimits {
            resident: config.max_resident,
            activations: config.max_activations,
            evictions: config.max_evictions,
            releases: config.max_releases,
            placement_weight,
        };
        let node = identity.node.clone();
        let mut actor = Self::from_environment_with_scripted_host(
            limits,
            fail_publish_once,
            fence,
            host,
            ownership,
            identity,
            resume_generation,
        )
        .await?;
        // The shell uses this deadline for handoff and stop retries while the
        // core uses the same value to arm operation timers. Installing only
        // the core configuration creates two different clocks in one Actor.
        actor.operation_deadline_ms = operation_deadline_ms;
        actor.state = ActorState::from(State::new(node, config));
        Ok(actor)
    }

    async fn from_environment_with_cell_host(
        limits: AdmissionLimits,
        fail_publish_once: bool,
        fence: mpsc::UnboundedSender<i32>,
        services: ActorHostServices,
        identity: ActorIdentity,
        resume_generation: Option<String>,
    ) -> anyhow::Result<Self> {
        let ActorHostServices {
            host,
            drain_pins,
            ownership,
            peer_http,
            peer_auth,
            paced_handoff,
        } = services;
        let ActorIdentity {
            node,
            advertise,
            region,
        } = identity;
        let ownership = if let Some(ownership) = ownership {
            ownership
        } else {
            Ownership::Memory(Arc::new(Mutex::new(MemoryOwnership {
                node: node.clone(),
                owners: BTreeMap::new(),
                leases: BTreeMap::new(),
                next_etag: 1,
            })))
        };
        let live_load = match &ownership {
            Ownership::Bucket(bucket) => Some(bucket.live()),
            Ownership::Memory(_) => None,
        };
        if let Some(live) = &live_load {
            live.placement_weight
                .store(limits.placement_weight.max(1), Ordering::Relaxed);
            crate::ownership_store::set_node_load(live.clone());
        }
        let process_generation = match &ownership {
            Ownership::Bucket(bucket) => bucket
                .process_generation()
                .map(str::to_owned)
                .unwrap_or_else(random_process_generation),
            Ownership::Memory(_) => random_process_generation(),
        };
        let ttl_ms = crate::env_vars::positive_or("CELLD_TTL_MS", 10_000)?;
        let operation_deadline_ms = operation_deadline_ms()?;
        let lease_spec = NodeLeaseSpec {
            // Startup has already resolved the environment and CLI settings
            // into this validated internal address. Reading the environment
            // again would let CELLD_ADVERTISE override a later CLI option and
            // publish an address that the bound listener did not validate.
            addr: advertise,
            peer_protocol: peer_auth::PROTOCOL_VERSION,
            generation: std::env::var("CELLD_TEST_GENERATION").unwrap_or(process_generation),
            resume_generation,
            ttl_ms,
        };
        Ok(Self {
            node_log: Arc::new(std::sync::Mutex::new(None)),
            state: ActorState::from(State::new(
                node,
                Config {
                    max_resident: limits.resident,
                    max_activations: limits.activations,
                    max_evictions: limits.evictions,
                    max_releases: limits.releases,
                    alarm_resident_ms: crate::wake::resident_ms().max(0) as u64,
                    require_node_lease: true,
                    peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
                    operation_deadline_ms: Some(operation_deadline_ms),
                    owner_log_recovery_backoff_ms: crate::env_vars::positive_or(
                        "CELLD_RECOVERY_RETRY_MS",
                        DEFAULT_OWNER_LOG_RECOVERY_BACKOFF_MS,
                    )?,
                    owner_log_recovery_attempts: crate::env_vars::positive_or(
                        "CELLD_RECOVERY_RETRIES",
                        DEFAULT_OWNER_LOG_RECOVERY_ATTEMPTS,
                    )?,
                    idle_evict_ms: crate::env_vars::positive::<u64>("CELLD_IDLE_EVICT_S")?
                        .map(|seconds| seconds.saturating_mul(1_000)),
                    pressure: pressure_config_from_environment()?,
                    max_outbound_websockets: crate::env_vars::positive_or(
                        "CELLD_MAX_OUTBOUND_WEBSOCKETS",
                        DEFAULT_MAX_OUTBOUND_WEBSOCKETS,
                    )?,
                    ownership_on_evict: match &ownership {
                        // Releasing a cell says "any node may take this
                        // one", which is only true if some other node could
                        // restore it. Without a bucket the sole copy is this
                        // node's disk, keyed to an epoch a re-acquire will
                        // step past, so releasing would lose the cell.
                        Ownership::Memory(_) => OwnershipOnEvict::Sticky,
                        Ownership::Bucket(_) => ownership_on_evict_from_environment()?,
                    },
                },
            )),
            ownership,
            peer_http,
            peer_auth,
            paced_handoff,
            operation_deadline_ms,
            handoff_directory: Arc::new(Mutex::new(HandoffDirectory::default())),
            host,
            drain_pins,
            handoff_timings: HashMap::new(),
            handoff_phase_ops: HashMap::new(),
            region,
            pending: BTreeMap::new(),
            request_cells: BTreeMap::new(),
            handoff_accepts: BTreeMap::new(),
            route_timings: BTreeMap::new(),
            pending_workers: BTreeMap::new(),
            eviction_waiters: BTreeMap::new(),
            durability_waiters: BTreeMap::new(),
            gated_responses: BTreeMap::new(),
            ws_gates: BTreeMap::new(),
            ws_gated: BTreeMap::new(),
            eviction_stops: BTreeMap::new(),
            published: BTreeSet::new(),
            fail_publish_once,
            publishes: 0,
            stops: 0,
            lease_spec,
            live_load,
            logged_shed_reason: None,
            validate_invariants: cfg!(debug_assertions),
            fence,
            timer_slots: TimerSlots::default(),
            started: false,
            preserving: false,
        })
    }

    /// Emits the exactly-once pre-loop node-lease transition.
    pub fn start(&mut self, out: &mut StepOutput) {
        assert!(!self.started, "Actor::start was called more than once");
        self.started = true;
        self.drive(
            Event::StartNodeLease {
                now_ms: now_ms(),
                now_mono_ms: crate::asyncrt::mono_ms(),
                spec: self.lease_spec.clone(),
            },
            out,
        );
    }

    /// Handles exactly one ready mailbox item, completion, or timer.
    pub fn step(&mut self, input: ActorInput, out: &mut StepOutput) {
        assert!(self.started, "Actor::step was called before Actor::start");
        match input {
            ActorInput::Message(message) => self.handle_message(message, out),
            ActorInput::Completed(completed) => {
                let route_cell = completed.timing.as_ref().map(|timing| timing.cell.clone());
                if let Some(timing) = completed.timing {
                    self.record_effect_timing(timing);
                }
                self.drive(completed.event, out);
                if let Some(cell) = route_cell {
                    self.observe_capacity_wait(&cell);
                }
            }
            ActorInput::TimerFired(timer) => {
                self.timer_slots.clear_slot(&TimerSlot::of(&timer));
                if self.preserving && matches!(timer, Timer::CellAlarm { .. }) {
                    return;
                }
                self.drive(
                    Event::TimerFired {
                        timer,
                        now_ms: now_ms(),
                        now_mono_ms: crate::asyncrt::mono_ms(),
                    },
                    out,
                );
            }
        }
    }

    fn handle_message(&mut self, message: Message, out: &mut StepOutput) {
        match message {
            Message::BeginPreserve => {
                self.preserving = true;
                self.drive(Event::BeginPreserve, out);
            }
            Message::Request { reply, .. } if self.preserving => {
                let _ = reply.send(Err(RequestError::NodeFenced));
            }
            Message::Request {
                request,
                cell,
                capacity_handoff,
                handoff_accept,
                websocket,
                reply,
            } => {
                let retired_durability = match self.state.phase(&cell) {
                    Some(Phase::EnsuringDurability { op, .. }) => Some(*op),
                    _ => None,
                };
                self.pending.insert(request, reply);
                let ownership_handoff = handoff_accept.is_some();
                if let Some(waiter) = handoff_accept {
                    self.handoff_accepts.insert(request, (cell.clone(), waiter));
                }
                self.begin_route_if_cold(&cell);
                self.request_cells.insert(request, cell.clone());
                self.drive(
                    if let Some(websocket) = websocket {
                        Event::WebSocketRequestAt {
                            request,
                            cell: cell.clone(),
                            websocket,
                            now_ms: now_ms(),
                            now_mono_ms: crate::asyncrt::mono_ms(),
                        }
                    } else if ownership_handoff {
                        Event::HandoffRequestAt {
                            request,
                            cell: cell.clone(),
                            now_ms: now_ms(),
                            now_mono_ms: crate::asyncrt::mono_ms(),
                        }
                    } else if capacity_handoff {
                        Event::CapacityRequestAt {
                            request,
                            cell: cell.clone(),
                            now_mono_ms: crate::asyncrt::mono_ms(),
                        }
                    } else {
                        Event::RequestAt {
                            request,
                            cell: cell.clone(),
                            now_mono_ms: crate::asyncrt::mono_ms(),
                        }
                    },
                    out,
                );
                self.observe_capacity_wait(&cell);
                if let Some(op) = retired_durability {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
            }
            Message::CancelRoute { request } => {
                self.pending.remove(&request);
                self.handoff_accepts.remove(&request);
                if let Some(cell) = self.request_cells.remove(&request) {
                    self.finish_route(&cell, "cancelled", "caller_disconnected", None);
                }
                self.drive(Event::Cancel { request }, out);
            }
            Message::WorkerRequest { reply } if self.preserving => {
                let _ = reply.send(WorkerRouted {
                    request: crate::asyncrt::next_core_request(),
                    route: None,
                });
            }
            Message::WorkerRequest { reply } => {
                let request = crate::asyncrt::next_core_request();
                self.pending_workers.insert(request, reply);
                self.drive(Event::WorkerRequest { request }, out);
            }
            Message::ReleaseAll => self.drive(Event::ReleaseAll, out),
            Message::Rebalance { .. } if self.preserving => {}
            Message::Rebalance { cells } => self.drive(Event::Rebalance { cells }, out),
            Message::GenerationChanged {
                generation,
                max_age_ms,
                eager_classes,
            } => self.drive(
                Event::GenerationChanged {
                    generation,
                    now_mono_ms: crate::asyncrt::mono_ms(),
                    max_age_ms,
                    eager_classes,
                },
                out,
            ),
            Message::Drained { reply } => {
                let _ = reply.send(DrainStatus {
                    occupied: self.state.occupied(),
                    activating: self.state.activating(),
                    quiescing: self.state.quiescing(),
                    evicting: self.state.evicting(),
                    releasing: self.state.releasing(),
                    adopting: self.state.adopting(),
                    handed_off: self.state.handed_off(),
                });
            }
            Message::ResidentEpoch { cell, reply } => {
                let epoch = match self.state.phase(&cell) {
                    Some(Phase::Resident { epoch })
                    | Some(Phase::EnsuringDurability { epoch, .. }) => Some(*epoch),
                    _ => None,
                };
                let _ = reply.send(epoch);
            }
            Message::SampleLoad if self.preserving => {}
            Message::SampleLoad => {
                // Counted once: it is a walk of every cell, and the latch and
                // the number peers rank this node by must agree anyway.
                let occupied = self.state.occupied();
                let metrics = crate::asyncrt::services().sample_metrics();
                let cpu = metrics.cpu_percent_x100;
                let load = celld_logic::pressure::Load {
                    resident_cells: occupied,
                    rss_bytes: metrics.rss_bytes,
                    in_use_bytes: metrics.in_use_bytes,
                    cgroup_working_set_bytes: metrics.cgroup_working_set_bytes,
                    cgroup_current_bytes: metrics.cgroup_current_bytes,
                };
                let now_mono_ms = crate::asyncrt::mono_ms();
                self.drive(Event::LoadSampled { load, now_mono_ms }, out);
                // Report a change of shed reason once. `rss-hard` retains its
                // established name, but in a Linux cgroup it says
                // `memory.current` crossed the absolute cap.
                let reason = self.state.shed_reason();
                if reason != self.logged_shed_reason {
                    self.logged_shed_reason = reason;
                    // How many V8 heaps the resident cells hold open. Most of
                    // what a cell costs is its heap, and a heap comes back
                    // only when its last cell goes, so cells alone do not say
                    // how much a walk down can still return.
                    let heaps = self
                        .state
                        .resident_isolates()
                        .into_iter()
                        .collect::<BTreeSet<_>>()
                        .len();
                    match reason {
                        Some(celld_logic::pressure::SHED_RSS_HARD) => tracing::warn!(
                            rss_bytes = load.rss_bytes,
                            in_use_bytes = load.in_use_bytes,
                            cgroup_working_set_bytes = load.cgroup_working_set_bytes,
                            cgroup_current_bytes = load.cgroup_current_bytes,
                            heaps,
                            "shedding on the absolute memory cap"
                        ),
                        Some(reason) => tracing::info!(
                            reason,
                            rss_bytes = load.rss_bytes,
                            in_use_bytes = load.in_use_bytes,
                            cgroup_working_set_bytes = load.cgroup_working_set_bytes,
                            cgroup_current_bytes = load.cgroup_current_bytes,
                            heaps,
                            "shedding"
                        ),
                        None => tracing::info!("no longer shedding"),
                    }
                }
                // Republish what peers rank this node by. The same numbers the
                // latch just saw: a node that reports last tick's residency
                // attracts work it has already refused.
                if let Some(live) = &self.live_load {
                    live.owned_cells
                        .store(self.state.owned_cells(), Ordering::Relaxed);
                    live.ownership_confirmed
                        .store(self.state.ownership_confirmed(), Ordering::Relaxed);
                    live.resident_cells.store(occupied, Ordering::Relaxed);
                    live.host_websockets
                        .store(self.state.host_websockets(), Ordering::Relaxed);
                    live.pressured
                        .store(self.state.shedding(), Ordering::Relaxed);
                    live.memory_headroom
                        .store(self.state.memory_headroom(), Ordering::Relaxed);
                    live.cpu_percent_x100.store(cpu, Ordering::Relaxed);
                    live.restoring
                        .store(self.state.activation_backlog() as u64, Ordering::Relaxed);
                    live.draining
                        .store(self.state.draining(), Ordering::Relaxed);
                }
            }
            Message::Output {
                request,
                ticket,
                reply,
            } => {
                self.gated_responses.insert(
                    HeldOutputKey {
                        request,
                        channel: ticket.channel,
                    },
                    reply,
                );
                self.drive(
                    Event::OutputAt {
                        request,
                        channel: ticket.channel,
                        position: ticket.position,
                        observed: ticket.observed,
                        epoch: ticket.epoch,
                        now_mono_ms: crate::asyncrt::mono_ms(),
                    },
                    out,
                );
            }
            Message::WsOutput {
                request,
                scope,
                frames,
                write_position,
                observed,
                reply,
            } => {
                // Open a barrier for every batch, then let the core decide
                // what it trails: its own write when the handler wrote, and
                // otherwise the newest write still outstanding on this cell,
                // whichever channel made it. The queue consulted here before
                // was filled in one place -- the arm below that writes -- so it
                // held only writes a `webSocketMessage` handler made itself,
                // and an HTTP, RPC, or peer write on the same cell was
                // invisible to a batch that only read.
                //
                // Both registrations happen before the drive, and must: `drive`
                // runs the effects it produces synchronously, so a read with no
                // barrier open on its cell is released inside the call below.
                // Moved after it, that `Effect::Release` would find no
                // `ws_gated` entry, fall through to the `gated_responses` map,
                // match nothing, and strand these frames behind a barrier that
                // nothing will ever settle.
                self.ws_gates
                    .entry(scope.clone())
                    .or_default()
                    .barriers
                    .push_back(WsBarrier {
                        request,
                        settled: None,
                        frames,
                    });
                self.ws_gated
                    .insert(HeldOutputKey::websocket(request), scope);
                // The core answers for every writer on the cell, including an
                // alarm: its consuming commit opens a barrier of its own, so a
                // read-only batch trails that too.
                self.drive(
                    Event::OutputAt {
                        request,
                        channel: Channel::WsHibernatable,
                        position: write_position,
                        observed,
                        epoch: None,
                        now_mono_ms: crate::asyncrt::mono_ms(),
                    },
                    out,
                );
                let _ = reply.send(());
            }
            Message::ActivityFinished {
                request,
                cell,
                alarm_at_ms,
                alarm_covered,
            } => {
                // Folded from the old AlarmObserved-then-ActivityFinished pair the
                // activity drop sent: observe the alarm and release any retired
                // durability waiters, then finish the activity — same events, same
                // order, one message instead of two.
                let retired_durability = match self.state.phase(&cell) {
                    Some(Phase::EnsuringDurability { op, .. }) => Some(*op),
                    _ => None,
                };
                self.drive(
                    Event::AlarmObserved {
                        cell,
                        at_ms: alarm_at_ms,
                        covered: alarm_covered,
                        now_ms: now_ms(),
                        now_mono_ms: crate::asyncrt::mono_ms(),
                    },
                    out,
                );
                if let Some(op) = retired_durability {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
                self.drive(Event::ActivityFinished { request }, out);
            }
            Message::WebSocketOpened {
                cell,
                websocket,
                kind,
                reply,
            } => {
                self.drive(
                    Event::WebSocketOpened {
                        cell: cell.clone(),
                        websocket,
                        kind,
                    },
                    out,
                );
                // The core may decline to hold the transport. Say so, rather
                // than acknowledging an open and closing the socket a moment
                // later: an application that hit a ceiling deserves to be
                // told which one, not left watching a socket disappear.
                // Only an outbound socket can be declined, and only for a
                // cell the core knows: an inbound transport is never refused,
                // and reporting one as refused fails opens that succeeded.
                let refused = kind == WebSocketKind::Outbound
                    && !self.state.holds_websocket(&cell, websocket);
                let _ = reply.send(!refused);
            }
            Message::WebSocketClosed { cell, websocket } => {
                self.drive(Event::WebSocketClosed { cell, websocket }, out);
            }
            Message::AlarmObserved {
                cell,
                at_ms,
                covered,
            } => {
                let retired_durability = match self.state.phase(&cell) {
                    Some(Phase::EnsuringDurability { op, .. }) => Some(*op),
                    _ => None,
                };
                self.drive(
                    Event::AlarmObserved {
                        cell,
                        at_ms,
                        covered,
                        now_ms: now_ms(),
                        now_mono_ms: crate::asyncrt::mono_ms(),
                    },
                    out,
                );
                if let Some(op) = retired_durability {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
            }
            Message::NudgeNodeLease => {
                self.drive(
                    Event::NudgeNodeLease {
                        now_ms: now_ms(),
                        now_mono_ms: crate::asyncrt::mono_ms(),
                    },
                    out,
                );
            }
            Message::WakeHint { .. } if self.preserving => {}
            Message::WakeHint {
                cell,
                entry_ms,
                scope,
            } => {
                self.drive(
                    Event::WakeHintAt {
                        cell,
                        entry_ms,
                        scope,
                        now_mono_ms: crate::asyncrt::mono_ms(),
                    },
                    out,
                );
            }
            Message::Evict { reply, .. } if self.preserving => {
                let _ = reply.send(());
            }
            Message::Evict { cell, reply } if self.state.is_active(&cell) => {
                let _ = reply.send(());
            }
            Message::Evict { cell, reply } => match self.state.phase(&cell) {
                Some(Phase::Resident { .. }) => {
                    self.eviction_waiters
                        .entry(cell.clone())
                        .or_default()
                        .push(reply);
                    self.drive(Event::Evict { cell }, out);
                }
                Some(Phase::EnsuringDurability { op, .. }) => {
                    self.durability_waiters
                        .entry(*op)
                        .or_insert_with(|| (cell, Vec::new()))
                        .1
                        .push(reply);
                }
                Some(Phase::Cleaning {
                    op,
                    cause: StopCause::Evict { .. },
                    ..
                }) => {
                    self.eviction_stops.entry(*op).or_default().push(reply);
                }
                _ => {
                    let _ = reply.send(());
                }
            },
            Message::InvalidateRemote {
                cell,
                node,
                epoch,
                reply,
            } => {
                self.drive(Event::InvalidateRemote { cell, node, epoch }, out);
                let _ = reply.send(());
            }
            Message::Snapshot { reply } => {
                let generations = self
                    .state
                    .residents()
                    .into_iter()
                    .map(|cell| {
                        let generation = self.state.cell_generation(&cell).unwrap_or(0);
                        (cell, generation)
                    })
                    .collect();
                let census = SwapCensus {
                    swapping: self.state.swapping(),
                    generations,
                };
                let _ = reply.send((self.state_json(), self.state.drain_pins(), census));
            }
            Message::Health { reply } => {
                let _ = reply.send(self.state.ready_to_serve());
            }
            Message::Presence { reply } => {
                let _ = reply.send(self.state.presence_snapshot());
            }
        }
    }

    /// Settle a gated `webSocketMessage`'s durability. On success mark its
    /// barrier durable and flush the durable prefix in write order; on failure
    /// break the whole cell gate — drop every held frame and reset its sockets,
    /// since an unproven write must never leave an acknowledged trace.
    fn ws_release(&mut self, held: HeldOutputKey, ok: bool) {
        let Some(scope) = self.ws_gated.remove(&held) else {
            return;
        };
        let mut flush = Vec::new();
        let broke = {
            let Some(gate) = self.ws_gates.get_mut(&scope) else {
                return;
            };
            if let Some(barrier) = gate.barriers.iter_mut().find(|b| b.request == held.request) {
                barrier.settled = Some(ok);
            }
            if gate.barriers.iter().any(|b| b.settled == Some(false)) {
                true
            } else {
                while gate
                    .barriers
                    .front()
                    .is_some_and(|b| b.settled == Some(true))
                {
                    flush.push(gate.barriers.pop_front().unwrap().frames);
                }
                false
            }
        };
        if broke {
            self.ws_gates.remove(&scope);
            self.ws_gated.retain(|_, s| *s != scope);
            crate::js::ws_close_scope(&scope, 1011, "durability unproven");
            return;
        }
        for frames in flush {
            crate::js::ws_emit_batch(frames);
        }
        if self
            .ws_gates
            .get(&scope)
            .is_some_and(|g| g.barriers.is_empty())
        {
            self.ws_gates.remove(&scope);
        }
    }

    /// Close a timed handoff pipeline step when its answering event arrives,
    /// and emit the cell's aggregate timing once adoption succeeds. A failed
    /// adoption keeps the cell entry so its retry accumulates on the same
    /// record; an expired operation abandons its pipeline entirely.
    fn close_handoff_phase(&mut self, event: &Event) {
        let (op, adopted) = match event {
            Event::DurabilityChecked { op, .. }
            | Event::RuntimeStopped { op }
            | Event::OwnerReleased { op, .. } => (*op, None),
            Event::SuccessorAdopted { op, result } => (*op, Some(result.is_ok())),
            Event::TimerFired {
                timer: Timer::OperationDeadline { op },
                ..
            } => {
                if let Some((cell, _, _)) = self.handoff_phase_ops.remove(op) {
                    self.handoff_timings.remove(&cell);
                }
                return;
            }
            _ => return,
        };
        let Some((cell, phase, started_mono_ms)) = self.handoff_phase_ops.remove(&op) else {
            return;
        };
        let elapsed = mono_elapsed_us(started_mono_ms);
        if phase == HandoffPhase::Adopt {
            if adopted == Some(true) {
                if let Some(timing) = self.handoff_timings.remove(&cell) {
                    tracing::info!(
                        event = "cell_handoff_timing",
                        %cell,
                        quiesce_us = timing.quiesce_us.unwrap_or(0),
                        durability_us = timing.durability_us.unwrap_or(0),
                        stop_us = timing.stop_us.unwrap_or(0),
                        release_us = timing.release_us.unwrap_or(0),
                        adopt_us = elapsed,
                        total_us = mono_elapsed_us(timing.nominated_mono_ms),
                        "handoff pipeline completed"
                    );
                }
            }
            return;
        }
        let Some(timing) = self.handoff_timings.get_mut(&cell) else {
            return;
        };
        match phase {
            HandoffPhase::Durability => timing.durability_us = Some(elapsed),
            HandoffPhase::Stop => timing.stop_us = Some(elapsed),
            HandoffPhase::Release => timing.release_us = Some(elapsed),
            HandoffPhase::Adopt => {}
        }
    }

    fn drive(&mut self, first: Event, out: &mut StepOutput) {
        let mut events = VecDeque::from([first]);
        while let Some(event) = events.pop_front() {
            self.close_handoff_phase(&event);
            let durability = match &event {
                Event::DurabilityChecked { op, .. } => Some(*op),
                // A deadline resolves the same operation the proof would
                // have, so it has to release the same waiters. Without this
                // the core abandons the eviction and the caller that asked
                // for it stays blocked on a proof that is no longer coming.
                Event::TimerFired {
                    timer: Timer::OperationDeadline { op },
                    ..
                } => Some(*op),
                _ => None,
            };
            // A bounded stop that gave up also ends the eviction the
            // waiters asked about; without this branch `/evict/` waited
            // forever on a cell that had already restarted in place.
            let stopped = match &event {
                Event::RuntimeStopped { op } | Event::RuntimeStopFailed { op } => Some(*op),
                _ => None,
            };
            let effects = apply_core_event(&mut self.state, event);
            if let Some(op) = durability {
                if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                    let stop = effects.iter().find_map(|effect| match effect {
                        Effect::StopRuntime {
                            op,
                            cause: StopCause::Evict { .. },
                            ..
                        } => Some(*op),
                        _ => None,
                    });
                    if let Some(stop) = stop {
                        self.eviction_stops.entry(stop).or_default().extend(waiters);
                    } else {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
            }
            for effect in effects {
                self.execute(effect, &mut events, out);
            }
            self.settle_handoff_accepts();
            if let Some(op) = stopped {
                if let Some(waiters) = self.eviction_stops.remove(&op) {
                    for waiter in waiters {
                        let _ = waiter.send(());
                    }
                }
            }
            if self.validate_invariants {
                self.state.validate().expect("celld core invariant");
            }
        }
    }

    fn settle_handoff_accepts(&mut self) {
        let accepted: Vec<_> = self
            .handoff_accepts
            .iter()
            .filter_map(|(request, (cell, waiter))| {
                self.state
                    .accepted_epoch(cell)
                    .filter(|epoch| *epoch > waiter.released_epoch)
                    .map(|epoch| (*request, epoch))
            })
            .collect();
        for (request, epoch) in accepted {
            let Some((_, waiter)) = self.handoff_accepts.remove(&request) else {
                continue;
            };
            let _ = waiter.reply.send(AdoptedCell {
                node: self.state.node().to_string(),
                addr: self.lease_spec.addr.clone(),
                epoch,
                peer_protocol: peer_auth::PROTOCOL_VERSION,
            });
        }
    }

    fn execute(&mut self, effect: Effect, immediate: &mut VecDeque<Event>, out: &mut StepOutput) {
        match effect {
            Effect::ScheduleTimer { timer, at_mono_ms } => {
                let (arm, _) = self.timer_slots.arm(timer, at_mono_ms, ());
                out.timers.push(arm);
            }
            Effect::ReadSelfNodeLease { op } => {
                let ownership = self.ownership.clone();
                let node = self.state.node().to_string();
                out.effects.push(Box::pin(async move {
                    let result = ownership.read_self_node_lease(&node).await;
                    CompletedEffect::plain(Event::SelfNodeLeaseRead {
                        op,
                        now_ms: now_ms(),
                        now_mono_ms: crate::asyncrt::mono_ms(),
                        result,
                    })
                }));
            }
            Effect::CasNodeLease {
                op,
                guard,
                record,
                authority_expires_ms,
            } => {
                let ownership = self.ownership.clone();
                out.effects.push(Box::pin(async move {
                    let attempt_started_mono_ms = crate::asyncrt::mono_ms();
                    let node = record.node.clone();
                    let candidate_expires_ms = record.expires_ms;
                    // Logged before the CAS: with only the completion line, a
                    // renewal hung on a storage tail is indistinguishable from
                    // a timer that never fired (the n6 fence, 2026-08-11).
                    tracing::info!(
                        event = "node_lease_attempt_started",
                        %node,
                        attempt = if authority_expires_ms.is_some() {
                            "renew"
                        } else {
                            "acquire"
                        },
                        prior_authority_headroom_ms = authority_expires_ms
                            .map(|expires_ms| expires_ms.saturating_sub(now_ms()))
                            .unwrap_or(0),
                        "node lease attempt started"
                    );
                    // A renewal must return while proven authority remains,
                    // because only a returned attempt lets the ambiguity
                    // read-back run before the watchdog. The 10:15Z R2
                    // brownout fenced 9 nodes whose sole hung attempt was
                    // still inside the transport's 15 s timeout when the
                    // 10 s TTL expired. Bound the attempt to half the
                    // remaining authority (capped, floored) and map timeout
                    // to Ambiguous — the same conservative outcome a lost
                    // response already produces, so safety is unchanged.
                    // The stamp survives a timed-out attempt: the backend
                    // writes it through the out-parameter synchronously at
                    // serialization, before the transport await, so even a
                    // dropped future has reported what the possibly-landed
                    // body carried.
                    let mut stamped_log_state = None;
                    let result = match authority_expires_ms {
                        Some(expires_ms) => {
                            let remaining = expires_ms.saturating_sub(now_ms());
                            let bound =
                                std::time::Duration::from_millis((remaining / 2).clamp(250, 2_500));
                            match crate::asyncrt::timeout(
                                bound,
                                ownership.cas_node_lease(guard, record, &mut stamped_log_state),
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(_) => Err(Failure::Ambiguous),
                            }
                        }
                        None => {
                            ownership
                                .cas_node_lease(guard, record, &mut stamped_log_state)
                                .await
                        }
                    };
                    let completed_ms = now_ms();
                    let elapsed_ms =
                        crate::asyncrt::mono_ms().saturating_sub(attempt_started_mono_ms);
                    let prior_authority_headroom_ms = authority_expires_ms
                        .map(|expires_ms| expires_ms.saturating_sub(completed_ms))
                        .unwrap_or(0);
                    let candidate_headroom_ms = candidate_expires_ms.saturating_sub(completed_ms);
                    let attempt = if authority_expires_ms.is_some() {
                        "renew"
                    } else {
                        "acquire"
                    };
                    let outcome = match &result {
                        Ok(LeaseCasOutcome::Applied { .. }) => "applied",
                        Ok(LeaseCasOutcome::Rejected) => "rejected",
                        Err(Failure::Ambiguous) => "ambiguous",
                        Err(Failure::Definite) => "definite_failure",
                    };
                    if matches!(&result, Ok(LeaseCasOutcome::Applied { .. })) {
                        tracing::info!(
                            event = "node_lease_attempt",
                            %node,
                            attempt,
                            outcome,
                            elapsed_ms,
                            prior_authority_headroom_ms,
                            candidate_headroom_ms,
                            "node lease attempt completed"
                        );
                    } else {
                        tracing::warn!(
                            event = "node_lease_attempt",
                            %node,
                            attempt,
                            outcome,
                            elapsed_ms,
                            prior_authority_headroom_ms,
                            candidate_headroom_ms,
                            "node lease attempt did not apply"
                        );
                    }
                    CompletedEffect::plain(Event::NodeLeaseCasCompleted {
                        op,
                        now_mono_ms: crate::asyncrt::mono_ms(),
                        result,
                        stamped_log_state,
                    })
                }));
            }
            Effect::ReadLocalCells => {
                let runtime = self.host.clone();
                out.effects.push(Box::pin(async move {
                    let result = match runtime {
                        // The host runtime's blocking pool, not a second
                        // pool on the core's current-thread runtime.
                        Some(runtime) => crate::asyncrt::blocking(move || {
                            runtime.local_reload_cells().map_err(|error| {
                                eprintln!("celld local reload scan failed: {error:#}");
                                Failure::Definite
                            })
                        })
                        .await
                        .unwrap_or(Err(Failure::Definite)),
                        None => Err(Failure::Definite),
                    };
                    CompletedEffect::plain(Event::LocalCellsRead { result })
                }));
            }
            Effect::ReadOwner { op, cell } => {
                self.route_effect_started(&cell);
                let ownership = self.ownership.clone();
                let timing_cell = cell.clone();
                out.effects.push(Box::pin(async move {
                    let started = crate::asyncrt::mono_ms();
                    let result = ownership.read_owner(&cell).await;
                    CompletedEffect::timed(
                        Event::OwnerRead {
                            op,
                            now_ms: now_ms(),
                            result,
                        },
                        timing_cell,
                        RouteStage::OwnershipRead,
                        started,
                    )
                }));
            }
            Effect::ReadNodeLease { op, cell, owner } => {
                self.route_effect_started(&cell);
                let ownership = self.ownership.clone();
                let timing_cell = cell;
                out.effects.push(Box::pin(async move {
                    let started = crate::asyncrt::mono_ms();
                    let result = ownership.read_node_lease(&owner).await;
                    CompletedEffect::timed(
                        Event::NodeLeaseRead {
                            op,
                            now_ms: now_ms(),
                            result,
                        },
                        timing_cell,
                        RouteStage::NodeLeaseLookup,
                        started,
                    )
                }));
            }
            Effect::RecoverNodeLog { op, cell, owner } => {
                self.route_effect_started(&cell);
                let interlock = self.node_log.lock().unwrap().clone();
                let timing_cell = cell.clone();
                out.effects.push(Box::pin(async move {
                    let started = crate::asyncrt::mono_ms();
                    let result = match interlock {
                        // No log tier on this node (no bucket): nothing to
                        // recover, and the claim proceeds as before.
                        None => Ok(()),
                        Some(interlock) => interlock
                            .ensure_recovered(Some(&owner))
                            .await
                            .map_err(|error| {
                                eprintln!(
                                    "celld node-log recovery for takeover of {cell} failed: {error:#}"
                                );
                                Failure::Ambiguous
                            }),
                    };
                    CompletedEffect::timed(
                        Event::NodeLogRecovered { op, result },
                        timing_cell,
                        RouteStage::NodeLeaseLookup,
                        started,
                    )
                }));
            }
            Effect::ReadCapacityPeers { op, cell } => {
                self.route_effect_started(&cell);
                let ownership = self.ownership.clone();
                let timing_cell = cell;
                out.effects.push(Box::pin(async move {
                    let started = crate::asyncrt::mono_ms();
                    // The shared sample, judged at the instant it was taken:
                    // its leases are copies that age while the nodes renew.
                    let (now_ms, result) = match ownership.read_capacity_view().await {
                        Ok((view_ms, peers)) => (view_ms, Ok(peers)),
                        Err(error) => (now_ms(), Err(error)),
                    };
                    CompletedEffect::timed(
                        Event::CapacityPeersRead { op, now_ms, result },
                        timing_cell,
                        RouteStage::CapacityLookup,
                        started,
                    )
                }));
            }
            Effect::CasOwner {
                op,
                cell,
                guard,
                epoch,
                takeover,
            } => {
                self.route_effect_started(&cell);
                if let Some(timing) = self.route_timings.get_mut(&cell) {
                    timing
                        .fresh
                        .get_or_insert(!takeover && matches!(guard, CasGuard::Absent));
                }
                let ownership = self.ownership.clone();
                let timing_cell = cell.clone();
                out.effects.push(Box::pin(async move {
                    let started = crate::asyncrt::mono_ms();
                    let result = ownership.cas_owner(&cell, guard, epoch).await;
                    CompletedEffect::timed(
                        Event::OwnerCasCompleted { op, result },
                        timing_cell,
                        RouteStage::OwnershipAcquire,
                        started,
                    )
                }));
            }
            Effect::AdoptWakeEntry { cell, entry_ms } => {
                crate::js::adopt_wake_entry(&cell, entry_ms);
            }
            Effect::ReconcileWakeEntry {
                cell,
                next_alarm_ms,
            } => {
                if self.host.is_some() {
                    // An ARMED alarm must always be reconcilable, tracked or
                    // not: belief can be lost while the alarm stands (a
                    // failed arm-time PUT, an entry retired under a racing
                    // arm), and skipping here would mean it is never
                    // re-asserted — entryless until the alarm fires or the
                    // cell moves. Gating the CONSUME side on tracking is
                    // what keeps alarm-less cells op-quiescent: untracked
                    // with no alarm, there is nothing to delete.
                    if next_alarm_ms >= 0 || crate::js::wake_entry_tracked(&cell) {
                        // On the host runtime: this arm runs on the core
                        // thread, and a bare spawn would schedule the S3
                        // round trip there too.
                        crate::asyncrt::spawn(async move {
                            // A consume-delete only ever follows a firing, and
                            // the core orders it from the far side of the
                            // output gate: `alarm_finished` gates the
                            // consuming commit, and only a proven
                            // DurableReached — with an ownership read behind a
                            // bucket proof — lets the alarm settle
                            // into this reconcile. So by the time this delete
                            // is ordered, the proof already happened, and
                            // nothing here decides anything. The old
                            // sync_refused probe asked the same question a
                            // second way and was wrong more often: it refused
                            // for any database the replicator never
                            // registered, leaving the entry to outlive its
                            // alarm forever.
                            crate::js::reconcile_wake_entry(&cell, next_alarm_ms, true).await;
                        })
                        .detach();
                    }
                }
            }
            Effect::ReleaseOwner { op, cell, epoch } => {
                if self.handoff_timings.contains_key(&cell) {
                    self.handoff_phase_ops.insert(
                        op,
                        (
                            cell.clone(),
                            HandoffPhase::Release,
                            crate::asyncrt::mono_ms(),
                        ),
                    );
                }
                let ownership = self.ownership.clone();
                out.effects.push(Box::pin(async move {
                    let result = ownership.release_owner(&cell, epoch).await;
                    CompletedEffect::plain(Event::OwnerReleased { op, result })
                }));
            }
            Effect::AdoptReleased {
                op,
                cell,
                released_epoch,
                rebalance,
            } => {
                // The in-memory adapter is one process, so it has no successor
                // to acknowledge a handoff. The feature switch is an escape
                // hatch for mixed operational use, not the production default.
                if !self.paced_handoff || matches!(&self.ownership, Ownership::Memory(_)) {
                    immediate.push_back(Event::SuccessorAdopted {
                        op,
                        result: Err(Failure::Definite),
                    });
                    return;
                }
                if self.handoff_timings.contains_key(&cell) {
                    self.handoff_phase_ops.insert(
                        op,
                        (cell.clone(), HandoffPhase::Adopt, crate::asyncrt::mono_ms()),
                    );
                }
                let ownership = self.ownership.clone();
                let directory = self.handoff_directory.clone();
                let http = self.peer_http.clone();
                let auth = self.peer_auth.clone();
                let source = self.state.node().to_string();
                // A drain retries until the process deadline ends it. A
                // balancing move has no such end, so it fails after the
                // operation deadline and the cell stays unowned for the
                // next request to claim.
                let deadline =
                    rebalance.then(|| std::time::Duration::from_millis(self.operation_deadline_ms));
                out.effects.push(Box::pin(async move {
                    let started_mono_ms = crate::asyncrt::mono_ms();
                    let request = HandoffRequest {
                        cell: cell.clone(),
                        released_epoch,
                    };
                    let mut attempts = 0_u64;
                    let adoption = async { loop {
                        let peers =
                            handoff_candidates(&ownership, &directory, &source, &cell, rebalance)
                                .await
                                .unwrap_or_default();
                        for peer in peers {
                            let mut target = AdoptedCell {
                                node: peer.node,
                                addr: peer.addr,
                                epoch: 0,
                                peer_protocol: peer.peer_protocol,
                            };
                            let mut visited = BTreeSet::new();
                            for _ in 0..4 {
                                if target.node == source
                                    || target.addr.is_empty()
                                    || target.peer_protocol != peer_auth::PROTOCOL_VERSION
                                    || !visited.insert(target.node.clone())
                                {
                                    break;
                                }
                                attempts = attempts.saturating_add(1);
                                match request_peer_handoff(&http, &auth, &target, &request).await {
                                    Ok(HandoffAttempt::Accepted(adopted))
                                        if adopted.epoch > released_epoch =>
                                    {
                                        tracing::info!(
                                            event = "cell_handoff_accepted",
                                            %cell,
                                            released_epoch,
                                            successor = %adopted.node,
                                            successor_epoch = adopted.epoch,
                                            attempts,
                                            elapsed_ms = crate::asyncrt::mono_ms()
                                                .saturating_sub(started_mono_ms),
                                            "successor acquired a released cell for demand-driven restore"
                                        );
                                        return CompletedEffect::plain(Event::SuccessorAdopted {
                                            op,
                                            result: Ok(adopted),
                                        });
                                    }
                                    Ok(HandoffAttempt::CurrentOwner(owner)) => {
                                        target = owner;
                                    }
                                    Ok(HandoffAttempt::Accepted(_))
                                    | Ok(HandoffAttempt::Refused)
                                    | Err(_) => break,
                                }
                            }
                        }
                        // No peer can accept yet. Retain the core permit and
                        // retry until the process-wide shutdown deadline.
                        crate::asyncrt::sleep(HANDOFF_RETRY_DELAY).await;
                    } };
                    match deadline {
                        Some(deadline) => crate::asyncrt::timeout(deadline, adoption)
                            .await
                            .unwrap_or_else(|_| {
                                // The cell is left unowned and its next request
                                // re-reads ownership; the donor's counter shows
                                // it, and this names the cell and the wait.
                                tracing::warn!(
                                    event = "rebalance_adoption_timed_out",
                                    %cell,
                                    released_epoch,
                                    deadline_ms = deadline.as_millis() as u64,
                                    "no peer acquired a balanced cell before the deadline"
                                );
                                CompletedEffect::plain(Event::SuccessorAdopted {
                                    op,
                                    result: Err(Failure::Definite),
                                })
                            }),
                        None => adoption.await,
                    }
                }));
            }
            Effect::CancelCellActivity {
                cell,
                requests,
                websockets,
                alarm,
                keep_hibernatable,
            } => {
                self.handoff_timings
                    .entry(cell.clone())
                    .or_insert(HandoffCellTiming {
                        nominated_mono_ms: crate::asyncrt::mono_ms(),
                        quiesce_us: None,
                        durability_us: None,
                        stop_us: None,
                        release_us: None,
                    });
                for request in requests {
                    if let Some((request_id, _)) = self
                        .drain_pins
                        .cancel_core_request(request, "shutdown_quiesce")
                    {
                        // The handler can answer before its output gate or
                        // waitUntil work settles. Cancel every pinned phase,
                        // or that background work can outlive the donor.
                        if let (Some(runtime), Some(request_id)) = (&self.host, request_id) {
                            runtime.abort_activity(request_id);
                        }
                    }
                }
                if let (Some(runtime), Some(op)) = (&self.host, alarm) {
                    runtime.abort_alarm(&cell, op);
                }
                if !websockets.is_empty() {
                    if keep_hibernatable {
                        for websocket in &websockets {
                            crate::js::ws_close(*websocket, 1012, "service restart");
                        }
                    } else {
                        crate::js::ws_close_scope(&cell, 1012, "service restart");
                    }
                    for websocket in websockets {
                        crate::js::ws_unregister(websocket);
                        immediate.push_back(Event::WebSocketClosed {
                            cell: cell.clone(),
                            websocket,
                        });
                    }
                }
                // The arm gate can finish after the core's earlier alarm
                // observation. Refresh once when the cell enters the handoff
                // cohort, under the same registry ordering as the reporter.
                // An uncovered alarm needs more than another cache read: the
                // successor stays dormant after adoption, so await the wake
                // entry PUT before telling the core this cell can leave.
                if let Some(runtime) = self.host.clone() {
                    match runtime.alarm_observation(&cell) {
                        Some((Some(at_ms), false)) => {
                            out.effects.push(Box::pin(async move {
                                let started = crate::asyncrt::mono_ms();
                                let (observed, covered) = runtime
                                    .refresh_handoff_alarm_coverage(&cell, at_ms)
                                    .await
                                    .unwrap_or((Some(at_ms), false));
                                tracing::info!(
                                    event = "handoff_alarm_coverage",
                                    %cell,
                                    at_ms,
                                    covered,
                                    elapsed_ms = crate::asyncrt::mono_ms().saturating_sub(started),
                                    "refreshed alarm coverage for graceful handoff"
                                );
                                CompletedEffect::plain(Event::AlarmObserved {
                                    cell,
                                    at_ms: observed,
                                    covered,
                                    now_ms: now_ms(),
                                    now_mono_ms: crate::asyncrt::mono_ms(),
                                })
                            }));
                        }
                        Some((at_ms, covered)) => immediate.push_back(Event::AlarmObserved {
                            cell,
                            at_ms,
                            covered,
                            now_ms: now_ms(),
                            now_mono_ms: crate::asyncrt::mono_ms(),
                        }),
                        None => {}
                    }
                }
            }
            Effect::Restore { op, cell, spec } => {
                self.route_effect_started(&cell);
                if let Some(runtime) = self.host.clone() {
                    let timing_cell = cell.clone();
                    // A restore downloads, merges, and fsyncs a whole
                    // database. Poll it on the host runtime: this future
                    // lives in `out`, which the core thread drives,
                    // and the core owns the node lease timer.
                    let task = crate::asyncrt::spawn(async move {
                        let started = crate::asyncrt::mono_ms();
                        let result = runtime.restore_cell(&cell, &spec).await.map_err(|error| {
                            eprintln!("celld restore failed for {cell}: {error:#}");
                            Failure::Definite
                        });
                        CompletedEffect::timed(
                            Event::RestoreCompleted { op, result },
                            timing_cell,
                            RouteStage::Restore,
                            started,
                        )
                    });
                    out.effects.push(Box::pin(async move {
                        task.await.expect("restore task panicked")
                    }));
                } else {
                    self.record_effect_timing(EffectTiming {
                        cell,
                        stage: RouteStage::Restore,
                        elapsed_us: 0,
                    });
                    immediate.push_back(Event::RestoreCompleted {
                        op,
                        result: Ok(celld_logic::RestoreOutcome {
                            restored: false,
                            alarm: None,
                        }),
                    });
                }
            }
            Effect::StartRuntime { op, cell, epoch } => {
                self.route_effect_started(&cell);
                if let Some(runtime) = self.host.clone() {
                    let fresh = self
                        .route_timings
                        .get(&cell)
                        .and_then(|timing| timing.fresh)
                        .unwrap_or(false);
                    let timing_cell = cell.clone();
                    out.effects.push(Box::pin(async move {
                        let started = crate::asyncrt::mono_ms();
                        let placed = runtime
                            .start_cell(cell.clone(), epoch, fresh)
                            .await
                            .map_err(|error| {
                                eprintln!("celld runtime start failed for {cell}: {error:#}");
                                Failure::Definite
                            });
                        let isolate = placed.as_ref().ok().map(|(isolate, _)| *isolate);
                        let generation = placed
                            .as_ref()
                            .ok()
                            .map_or(0, |(_, generation)| *generation);
                        CompletedEffect::timed(
                            Event::RuntimeStarted {
                                op,
                                isolate,
                                generation,
                                result: placed.map(|_| ()),
                            },
                            timing_cell,
                            RouteStage::IsolateStartup,
                            started,
                        )
                    }));
                } else {
                    self.record_effect_timing(EffectTiming {
                        cell,
                        stage: RouteStage::IsolateStartup,
                        elapsed_us: 0,
                    });
                    immediate.push_back(Event::RuntimeStarted {
                        op,
                        isolate: None,
                        generation: 0,
                        result: Ok(()),
                    });
                }
            }
            Effect::Publish { op, cell, epoch } => {
                self.route_effect_started(&cell);
                let publish_started = crate::asyncrt::mono_ms();
                self.publishes += 1;
                let result = if self.fail_publish_once {
                    self.fail_publish_once = false;
                    Err(Failure::Ambiguous)
                } else {
                    self.host
                        .as_ref()
                        .map_or(Ok(()), |runtime| runtime.publish_cell(&cell, epoch))
                        .map_err(|error| {
                            eprintln!("celld runtime publication failed for {cell}: {error:#}");
                            Failure::Definite
                        })
                };
                if result.is_ok() {
                    self.published.insert(cell.clone());
                }
                self.record_effect_timing(EffectTiming {
                    cell: cell.clone(),
                    stage: RouteStage::RegistryInsert,
                    elapsed_us: mono_elapsed_us(publish_started),
                });
                if result.is_ok() {
                    let node = self.state.node().to_string();
                    self.finish_route(&cell, "activated", "", Some((&node, epoch)));
                }
                immediate.push_back(Event::Published { op, result });
            }
            Effect::EnsureDurable { op, cell, epoch } => {
                if let Some(timing) = self.handoff_timings.get_mut(&cell) {
                    if timing.quiesce_us.is_none() {
                        timing.quiesce_us = Some(mono_elapsed_us(timing.nominated_mono_ms));
                    }
                    self.handoff_phase_ops.insert(
                        op,
                        (
                            cell.clone(),
                            HandoffPhase::Durability,
                            crate::asyncrt::mono_ms(),
                        ),
                    );
                }
                let waiters = self.eviction_waiters.remove(&cell).unwrap_or_default();
                self.durability_waiters.insert(op, (cell.clone(), waiters));
                if let Some(runtime) = self.host.clone() {
                    // A hot cell can merge thousands of staged LTX rows while
                    // this future is between awaits. Keep that synchronous
                    // merge on the host runtime: `out` is polled by the
                    // dedicated core thread, which also owns the node-lease
                    // renew and fence timers. Polling the proof inline can
                    // therefore turn a graceful handoff into a self-fence.
                    let task = crate::asyncrt::spawn(async move {
                        let result = runtime.ensure_durable(&cell, epoch).await.map_err(|error| {
                            eprintln!(
                                "celld durability proof failed for {cell} epoch {epoch}: {error:#}"
                            );
                            Failure::Ambiguous
                        });
                        CompletedEffect::plain(Event::DurabilityChecked { op, result })
                    });
                    out.effects.push(Box::pin(async move {
                        task.await.expect("durability proof task panicked")
                    }));
                } else {
                    immediate.push_back(Event::DurabilityChecked { op, result: Ok(()) });
                }
            }
            // The output gate: prove the cell durable, then let the core release
            // the held write response. Reuses the same recency-proving primitive
            // as EnsureDurable — a proof issued after the write covers it.
            Effect::AwaitDurable {
                op,
                cell,
                epoch,
                position,
            } => {
                if let Some(runtime) = self.host.clone() {
                    out.effects.push(Box::pin(async move {
                        // The replicator reports the position it actually
                        // proved durable and which mechanism proved it; the
                        // core acks only if the position covers this write,
                        // and decides per source whether ownership
                        // verification must run first (Effect::VerifyOwnership).
                        let (result, source) =
                            match runtime.await_durable(&cell, epoch, position).await {
                                Ok((durable, source)) => (Ok(durable), source),
                                Err(error) => {
                                    eprintln!(
                                        "celld output-gate durability proof failed for {cell} \
                                         epoch {epoch}: {error:#}"
                                    );
                                    (Err(Failure::Ambiguous), celld_logic::ProofSource::Bucket)
                                }
                            };
                        CompletedEffect::plain(Event::DurableReached { op, result, source })
                    }));
                } else {
                    immediate.push_back(Event::DurableReached {
                        op,
                        result: Ok(position),
                        source: celld_logic::ProofSource::Fleet,
                    });
                }
            }
            Effect::VerifyOwnership { op, cell, epoch } => {
                // Verify ownership for bucket-proof acks. Durable in
                // `e<epoch>/` is not the same as durable: if the cell has been taken over, that
                // prefix is orphaned — the next owner restores a higher
                // epoch and this write is gone. A read is enough, and is why
                // this is one GET rather than a compare-and-swap: if the
                // record still names us, no takeover linearised before this
                // read; the LTX went up before it; so any later takeover
                // restores from a lineage that already contains the write.
                let ownership = self.ownership.clone();
                let node = self.state.node().to_string();
                out.effects.push(Box::pin(async move {
                    let result = match ownership.read_owner(&cell).await {
                        Ok(Some(record))
                            if record.node.as_deref() == Some(node.as_str())
                                && record.epoch == epoch =>
                        {
                            Ok(())
                        }
                        Ok(record) => {
                            eprintln!(
                                "celld output gate: {cell} epoch {epoch} is no longer ours \
                                 (record: {record:?}); refusing to acknowledge a write in an \
                                 orphaned epoch"
                            );
                            Err(Failure::Definite)
                        }
                        Err(failure) => Err(failure),
                    };
                    CompletedEffect::plain(Event::OwnershipVerified { op, result })
                }));
            }
            // The shell's only per-channel code: how the effect was held, and
            // how it is released. Every channel but one takes a durability
            // ticket and waits on a oneshot, so they collapse onto one adapter;
            // `Sync` holds nothing but the ticket itself, because the effect it
            // releases is the handler's promise. Only `WsHibernatable` needs
            // another, and only because a socket is an ordered stream whose
            // frames must drain in barrier order.
            //
            // The channel says which way the effect leaves; `ws_gated` says how
            // this one is being held. They agree except in one case, and there
            // the holding mechanism decides: a frame for a hibernatable socket
            // raised OUTSIDE a `webSocketMessage` handler -- from `fetch`, an
            // alarm, or `webSocketClose` -- is never captured, so it waits on a
            // ticket like every other channel rather than in the cell's barrier
            // queue. Routing it to the barrier queue would find no entry and
            // strand the task waiting on the ticket.
            Effect::Release {
                request,
                channel,
                result,
            } => {
                let held = HeldOutputKey { request, channel };
                match channel {
                    Channel::WsHibernatable if self.ws_gated.contains_key(&held) => {
                        self.ws_release(held, result.is_ok())
                    }
                    Channel::Response
                    | Channel::Fetch
                    | Channel::WsHibernatable
                    | Channel::WsSelf
                    | Channel::Service
                    | Channel::CellRpc
                    | Channel::Queue
                    | Channel::Sync => {
                        if let Some(reply) = self.gated_responses.remove(&held) {
                            let _ = reply.send(result);
                        }
                    }
                }
            }
            Effect::StopRuntime {
                op,
                cell,
                epoch,
                cause,
                bounded,
            } => {
                if self.handoff_timings.contains_key(&cell) {
                    self.handoff_phase_ops.insert(
                        op,
                        (cell.clone(), HandoffPhase::Stop, crate::asyncrt::mono_ms()),
                    );
                }
                self.stops += 1;
                if matches!(cause, StopCause::Swap) {
                    // A generation swap: the cell leaves its isolate and
                    // nothing else changes. No durability barrier, no
                    // replica release, no local unlink -- the same epoch
                    // starts again on the current generation as soon as the
                    // core hears the stop.
                    self.published.remove(&cell);
                    if let Some(runtime) = self.host.clone() {
                        let released = cell.clone();
                        let task = crate::asyncrt::spawn(report_stopped_when_released(
                            op,
                            cell,
                            epoch,
                            "generation_swap_out_retry",
                            None,
                            move || {
                                let runtime = runtime.clone();
                                let cell = released.clone();
                                async move { runtime.swap_out_cell(&cell, epoch).await }
                            },
                        ));
                        out.effects.push(Box::pin(async move {
                            task.await.expect("swap out task panicked")
                        }));
                    } else {
                        immediate.push_back(Event::RuntimeStopped { op });
                    }
                } else {
                    let evicting = matches!(cause, StopCause::Evict { .. });
                    if matches!(cause, StopCause::Fence) {
                        // A fenced cell's wake entry belongs to whoever takes the
                        // cell over; a retained local belief would collide with
                        // the new owner's arm/consume traffic on the same key.
                        crate::js::forget_wake_entry(&cell);
                    }
                    // Keep the persistence decision whole. A cleanup still has
                    // authority, so it makes the closed image a proved local
                    // base. A fence can only close in place, and a reset must
                    // discard the database that failed its durability proof.
                    let stop_mode = match cause {
                        StopCause::Evict { rebalance } => crate::runtime::StopMode::Evict {
                            preserve_local: !rebalance,
                        },
                        StopCause::Cleanup => crate::runtime::StopMode::Rebase,
                        StopCause::Fence => crate::runtime::StopMode::CloseInPlace,
                        StopCause::Reset => crate::runtime::StopMode::Discard,
                        StopCause::Swap => unreachable!("generation swaps return above"),
                    };
                    if evicting {
                        if let Some(live) = &self.live_load {
                            live.shed_cells.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    self.published.remove(&cell);
                    if let Some(runtime) = self.host.clone() {
                        // A stop closes a whole database. It fsyncs the directory
                        // and the file, then unlinks the local copy, and it does
                        // all of that inside one synchronous call. Poll it on the
                        // host runtime: this future lives in `out`, which the core
                        // thread drives in the same select as the timer queue, and
                        // the core owns the node lease timer. A slow fsync on the
                        // core thread starves the lease renew and the lease fence,
                        // and the node then fences itself.
                        let released = cell.clone();
                        let deadline_mono_ms = bounded.then(|| {
                            crate::asyncrt::mono_ms().saturating_add(self.operation_deadline_ms)
                        });
                        let task = crate::asyncrt::spawn(report_stopped_when_released(
                            op,
                            cell,
                            epoch,
                            "stop_runtime_retry",
                            deadline_mono_ms,
                            move || {
                                let runtime = runtime.clone();
                                let cell = released.clone();
                                async move { runtime.stop_cell(&cell, epoch, stop_mode).await }
                            },
                        ));
                        out.effects.push(Box::pin(async move {
                            task.await.expect("stop runtime task panicked")
                        }));
                    } else {
                        immediate.push_back(Event::RuntimeStopped { op });
                    }
                }
            }
            Effect::FireAlarm {
                op,
                cell,
                epoch,
                scheduled_ms,
            } => {
                if let Some(runtime) = self.host.clone() {
                    out.effects.push(Box::pin(async move {
                        // The shell reports the firing raw: the deadline that
                        // stands after the handler, whether a wake entry
                        // covers it, and the position of the consuming
                        // commit. Proving that commit durable — and this node
                        // still the owner — is the core's job: it opens the
                        // same output gate a request's write gets, so one
                        // question answers for every egress on the cell, and
                        // the consume-side wake-entry delete leaves only from
                        // the far side of the proof. On failure the core
                        // re-arms or resets; either way the entry stays
                        // discoverable and at-least-once holds.
                        let result = runtime
                            .fire_alarm(op, cell.clone(), scheduled_ms)
                            .await
                            .map_err(|error| {
                                eprintln!("celld alarm dispatch failed: {error:#}");
                                Failure::Definite
                            });
                        CompletedEffect::plain(Event::AlarmFinished {
                            op,
                            cell,
                            epoch,
                            now_ms: now_ms(),
                            now_mono_ms: crate::asyncrt::mono_ms(),
                            result,
                        })
                    }));
                } else {
                    immediate.push_back(Event::AlarmFinished {
                        op,
                        cell,
                        epoch,
                        now_ms: now_ms(),
                        now_mono_ms: crate::asyncrt::mono_ms(),
                        result: Ok((None, true, None)),
                    });
                }
            }
            Effect::Complete { request, result } => {
                if let Some(cell) = self.request_cells.remove(&request) {
                    match &result {
                        Ok(Route::Remote { node, epoch, .. }) => {
                            self.finish_route(&cell, "remote_owner", "", Some((node, *epoch)));
                            // The cell lives elsewhere: its wake entry is no
                            // longer this node's to track, and a stale local
                            // belief would collide with the owner's own
                            // arm/consume traffic on the same key.
                            crate::js::forget_wake_entry(&cell);
                        }
                        Ok(Route::Local) => {
                            self.finish_route(&cell, "resident_after_wait", "", None);
                        }
                        Err(error) => {
                            self.finish_route(
                                &cell,
                                "route_error",
                                request_error_phase(*error),
                                None,
                            );
                        }
                    }
                }
                if let Some(reply) = self.pending.remove(&request) {
                    let local = result == Ok(Route::Local);
                    if reply
                        .send(result.map(|route| Routed { request, route }))
                        .is_err()
                        && local
                    {
                        immediate.push_back(Event::ActivityFinished { request });
                    }
                }
            }
            Effect::CompleteWorker { request, route } => {
                if let Some(op) = route.as_ref().and_then(|route| route.retired_durability) {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
                if let Some(reply) = self.pending_workers.remove(&request) {
                    let reserved = route.is_some();
                    if reply.send(WorkerRouted { request, route }).is_err() && reserved {
                        immediate.push_back(Event::ActivityFinished { request });
                    }
                }
            }
            Effect::CloseWebSocket { cell, websocket } => {
                // The core declined to hold this transport. Drop it and tell
                // the core it is gone, so the cell is not left believing it
                // has a socket the node already closed.
                eprintln!(
                    "celld refused an outbound WebSocket for {cell}: the node's \
                     outbound pin budget is spent"
                );
                crate::js::ws_unregister(websocket);
                immediate.push_back(Event::WebSocketClosed { cell, websocket });
            }
            Effect::Halt { code, reason } => {
                // Say why before going. Self-fencing is the most drastic thing
                // this process does, and an exit code on its own leaves an
                // operator to guess between a lease it could not renew, a
                // replicator that died, and a crash.
                match reason {
                    celld_logic::HaltReason::NodeLeaseExpired => tracing::warn!(
                        event = "node_lease_watchdog_fence",
                        code,
                        "SELF-FENCE: node lease not renewed within TTL — halting"
                    ),
                    celld_logic::HaltReason::NodeLeaseMissing => tracing::warn!(
                        event = "node_lease_record_missing_fence",
                        code,
                        "SELF-FENCE: node lease record is missing — halting"
                    ),
                    celld_logic::HaltReason::NodeLeaseMismatch => tracing::warn!(
                        event = "node_lease_record_mismatch_fence",
                        code,
                        "SELF-FENCE: node lease record no longer matches this process — halting"
                    ),
                }
                let _ = self.fence.send(code);
            }
        }
    }

    fn state_json(&self) -> String {
        // The lease sample reads counts the load tick last stored. Store them
        // now, so the sample below agrees with the state beside it.
        let node_load = self.live_load.as_ref().map(|live| {
            live.owned_cells
                .store(self.state.owned_cells(), Ordering::Relaxed);
            live.ownership_confirmed
                .store(self.state.ownership_confirmed(), Ordering::Relaxed);
            live.resident_cells
                .store(self.state.occupied(), Ordering::Relaxed);
            live.host_websockets
                .store(self.state.host_websockets(), Ordering::Relaxed);
            serde_json::to_string(&crate::ownership_store::process_load(live))
                .expect("node load serializes")
        });
        let activity = self.state.presence_snapshot().activity;
        let residents = self
            .state
            .residents()
            .into_iter()
            .map(|cell| format!("{cell:?}"))
            .collect::<Vec<_>>()
            .join(",");
        let published = self
            .published
            .iter()
            .map(|cell| format!("{cell:?}"))
            .collect::<Vec<_>>()
            .join(",");
        // Both numbers: a gap between them is memory the allocator kept, which
        // no eviction returns. One sample, so they cannot disagree.
        let memory = crate::memory::sample();
        // `restoring` is a sum, and a node that refuses cells for a quarter of
        // an hour needs its parts. `activating` holds a permit and is doing
        // work; `activation_waiting` is queued behind the activation ceiling;
        // `capacity_waiting` is queued behind residency. The census says where
        // every cell is, which `occupied` cannot: it counts residency, so a
        // node part-way through thousands of cold starts reports almost none.
        // Issue #50 is open because none of this was recorded at the time.
        let phases = self
            .state
            .phase_census()
            .into_iter()
            .map(|(phase, count)| format!("{phase:?}:{count}"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"ownership\":{:?},\"owned_cells\":{},\"occupied\":{},\"quiescing\":{},\"evicting\":{},\"releasing\":{},\"adopting\":{},\"restoring\":{},\"activating\":{},\"activation_waiting\":{},\"capacity_waiting\":{},\"phases\":{{{}}},\"handed_off\":{},\"handoff_failed\":{},\"rebalanced\":{},\"rebalance_failed\":{},\"shedding\":{},\"rss_bytes\":{},\"in_use_bytes\":{},\"cgroup_working_set_bytes\":{},\"cgroup_current_bytes\":{},\"node_load\":{},\"residents\":[{}],\"published\":[{}],\"publishes\":{},\"stops\":{}}}",
            self.ownership.name(),
            self.state.owned_cells(),
            self.state.occupied(),
            self.state.quiescing(),
            self.state.evicting(),
            self.state.releasing(),
            self.state.adopting(),
            self.state.activation_backlog(),
            self.state.activating(),
            self.state.activation_waiting().len(),
            self.state.waiting().len(),
            phases,
            activity.handed_off,
            activity.handoff_failed,
            activity.rebalanced,
            activity.rebalance_failed,
            self.state
                .shed_reason()
                .map_or_else(|| "null".to_string(), |reason| format!("{reason:?}")),
            memory.rss_bytes,
            memory.in_use_bytes,
            memory
                .cgroup_working_set_bytes
                .map_or_else(|| "null".to_string(), |bytes| bytes.to_string()),
            memory
                .cgroup_current_bytes
                .map_or_else(|| "null".to_string(), |bytes| bytes.to_string()),
            node_load.unwrap_or_else(|| "null".to_string()),
            residents,
            published,
            self.publishes,
            self.stops
        )
    }

    fn begin_route_if_cold(&mut self, cell: &str) {
        if !matches!(
            self.state.phase(cell),
            Some(Phase::Resident { .. } | Phase::EnsuringDurability { .. } | Phase::Remote { .. })
        ) {
            self.route_timings
                .entry(cell.to_string())
                .or_insert_with(CellRouteTiming::new);
        }
    }

    fn route_effect_started(&mut self, cell: &str) {
        if let Some(timing) = self.route_timings.get_mut(cell) {
            timing.effect_started();
        }
    }

    fn observe_capacity_wait(&mut self, cell: &str) {
        if matches!(self.state.phase(cell), Some(Phase::WaitingCapacity)) {
            if let Some(timing) = self.route_timings.get_mut(cell) {
                timing
                    .capacity_wait_started_mono_ms
                    .get_or_insert_with(crate::asyncrt::mono_ms);
            }
        }
    }

    fn record_effect_timing(&mut self, completed: EffectTiming) {
        if let Some(timing) = self.route_timings.get_mut(&completed.cell) {
            timing.record(completed.stage, completed.elapsed_us);
        }
    }

    fn finish_route(
        &mut self,
        cell: &str,
        outcome: &str,
        failure_phase: &str,
        owner: Option<(&str, u64)>,
    ) {
        let Some(mut timing) = self.route_timings.remove(cell) else {
            return;
        };
        if let Some(started) = timing.capacity_wait_started_mono_ms.take() {
            timing.capacity_wait_us = timing
                .capacity_wait_us
                .saturating_add(mono_elapsed_us(started));
        }
        let (owner_node, epoch) = owner.unwrap_or(("", 0));
        tracing::debug!(
            target: "timing",
            event = "cell_route_timing",
            outcome,
            failure_phase,
            scope = %cell,
            node = %self.state.node(),
            region = %self.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            owner_node,
            epoch,
            fresh = timing.fresh.unwrap_or(false),
            total_us = mono_elapsed_us(timing.started_mono_ms),
            latch_wait_us = timing.latch_wait_us,
            ownership_read_us = timing.ownership_read_us,
            node_lease_lookup_us = timing.node_lease_lookup_us,
            capacity_lookup_us = timing.capacity_lookup_us,
            capacity_wait_us = timing.capacity_wait_us,
            activation_slot_wait_us = timing.activation_slot_wait_us,
            lease_permit_us = timing.lease_permit_us,
            ownership_acquire_us = timing.ownership_acquire_us,
            replica_discovery_us = timing.replica_discovery_us,
            restore_us = timing.restore_us,
            isolate_startup_us = timing.isolate_startup_us,
            registry_insert_us = timing.registry_insert_us,
            "cell route resolved"
        );
    }
}

fn request_error_phase(error: RequestError) -> &'static str {
    match error {
        RequestError::NodeUnavailable | RequestError::NodeFenced => "node_authority",
        RequestError::ResolveFailed | RequestError::PeerIncompatible => "ownership_lookup",
        RequestError::CapacityExhausted => "capacity_wait",
        RequestError::AcquireFailed => "ownership_acquire",
        RequestError::RestoreFailed => "restore",
        RequestError::RuntimeFailed => "isolate_startup",
        RequestError::PublishFailed => "registry_insert",
        RequestError::DurabilityUnproven => "output_gate",
    }
}

#[cfg(not(all(test, celld_internal_tests)))]
type ActorState = State;

#[cfg(not(all(test, celld_internal_tests)))]
fn apply_core_event(state: &mut ActorState, event: Event) -> Vec<Effect> {
    on_event(state, event)
}

#[cfg(all(test, celld_internal_tests))]
include!(env!("CELLD_INTERNAL_ACTOR_OBSERVERS"));
