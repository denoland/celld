// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Clean-sheet celld decision core.
//!
//! [`on_event`] is the only way behavioral state advances. The production
//! executor and deterministic simulator both feed it events and perform the
//! returned effects. No adapter may mutate [`State`] directly.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub mod alarm;
pub mod cache;
pub mod cell;
pub mod cron;
pub mod dead_node_reconciliation;
pub mod drain;
pub mod durability;
pub mod format;
pub mod gate;
pub mod http;
pub mod isolate;
pub mod kv;
pub mod log_evict;
pub mod log_tier;
mod output_gate;
pub mod peer;
pub mod pressure;
pub mod queue;
pub mod rebalance;
pub mod restore;
pub mod routing;
pub mod schedule;
pub mod sqlite;
pub mod sweep;
pub mod wake;

mod types;
pub use types::*;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Activation {
    Claim(Claim),
    Restore(RestoreSpec),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ColdStart {
    ReadOwner,
    Restore(RestoreSpec),
}

/// Facts already decided by ownership resolution that select a safe restore
/// source. The effect adapter must not rediscover or guess these.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreSpec {
    pub epoch: Epoch,
    /// This activation conditionally created epoch one, so no replica can
    /// precede it.
    pub fresh: bool,
    /// Ownership was seized from a different node (or a released record), so
    /// a previous local eviction cache is not authoritative.
    pub took_over: bool,
    /// The node-level lease handoff proved that this exact local epoch remains
    /// authoritative. The adapter must not consult the remote replica.
    pub resume_local: bool,
    /// The owner the takeover displaced — the node named by the record
    /// version the acquire consumed — for the node-log takeover interlock.
    /// `None` when the record was released or absent, which the release
    /// path already proved durable. Carried here rather than memoized in
    /// the executor so an acquire confirmed by reconciliation names its
    /// prior exactly like one confirmed by the CAS response.
    pub prior: Option<NodeId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    pub guard: CasGuard,
    pub epoch: Epoch,
    pub takeover: bool,
    /// The node named by the record version `guard` matches — the prior
    /// owner this claim displaces if it applies. `None` for a released or
    /// absent record.
    pub prior: Option<NodeId>,
    /// Ambiguous acquires already reconciled for this claim.
    ///
    /// An ambiguous compare-and-swap is re-read rather than retried blindly,
    /// which is correct — it may have applied. But the re-read leads to
    /// another acquire, and if that is ambiguous too the cycle repeats. With
    /// no bound a persistently unanswered store turns a request that used to
    /// hang into one that spins, which is worse: it burns a slot and an
    /// object-store budget forever instead of merely waiting.
    pub reconciles: u32,
}

/// How many times one claim may reconcile an ambiguous acquire before the
/// request is failed and the caller left to decide. Small: each pass is a full
/// read plus a write, and a store answering ambiguously three times running is
/// not about to start answering.
pub const MAX_ACQUIRE_RECONCILES: u32 = 3;

/// Phases map onto the Durable Objects lifecycle states where Cloudflare has
/// a name for them:
///
/// - `Resident` is active or idle in memory (states 1-3).
/// - `Dormant` is out of memory but still owned by this node, so the cell has
///   not been moved off its host. A dormant cell with surviving hibernatable
///   sockets is *hibernated* (state 4); see [`Core::is_hibernated`].
/// - `Inactive` is out of memory and owned by nobody: removed from the host,
///   needing a cold start (state 5, and the initial state of every cell).
///
/// Note that celld and Cloudflare use "evict" differently. Cloudflare evicts a
/// cell *off its host*, which produces `Inactive`. celld evicts a cell *out of
/// memory*, which produces `Dormant`, and shedding is what then publishes it
/// unowned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Inactive,
    /// Cold demand queued behind `max_activations`, before any I/O begins.
    WaitingActivation,
    ReadingOwner {
        op: OpId,
    },
    ReadingNodeLease {
        op: OpId,
        owner: OwnerRecord,
        /// The wall instant when the read effect was issued. A returned
        /// snapshot can prove expiry only relative to its invocation; using
        /// response time lets transport delay age a previously live snapshot
        /// into false takeover evidence after its owner has renewed.
        issued_ms: u64,
    },
    /// The takeover interlock (lease-fold): the dead owner's folded log
    /// state was not sealed, so the executor is recovering its sessions
    /// before this cell may claim.
    RecoveringOwnerLog {
        op: OpId,
        owner: OwnerRecord,
    },
    /// A dead-owner log recovery failed; the cell holds its requests and
    /// re-enters resolution when the retry timer fires. It holds no
    /// activation permit while it waits, so the backoff of one dead
    /// owner's cells cannot starve unrelated activations.
    WaitingOwnerLogRecovery {
        generation: u64,
    },
    ReadingCapacity {
        op: OpId,
        claim: Claim,
    },
    WaitingCapacity,
    Acquiring {
        op: OpId,
        claim: Claim,
        /// The claim lands `Dormant` (every request on the cell is an
        /// ownership-only handoff), so it holds no residency slot and a
        /// node at its cap can still accept it.
        dormant: bool,
    },
    ReconcilingAcquire {
        op: OpId,
        claim: Claim,
        dormant: bool,
    },
    Restoring {
        op: OpId,
        spec: RestoreSpec,
    },
    Starting {
        op: OpId,
        epoch: Epoch,
    },
    Publishing {
        op: OpId,
        epoch: Epoch,
    },
    /// The runtime remains published and routable while replica durability is
    /// being proved. A failed or ambiguous proof returns to `Resident`.
    EnsuringDurability {
        op: OpId,
        epoch: Epoch,
    },
    Cleaning {
        op: OpId,
        epoch: Epoch,
        cause: StopCause,
    },
    /// Out of memory, still owned here. Hibernated when sockets survived.
    Dormant {
        epoch: Epoch,
    },
    /// The owner record is no longer this node's, and a signed peer request is
    /// waiting for a successor to restore and publish the cell.
    Adopting {
        op: OpId,
        released_epoch: Epoch,
    },
    Resident {
        epoch: Epoch,
    },
    Remote {
        node: NodeId,
        addr: String,
        epoch: Epoch,
        peer_protocol: u16,
        /// Present only for epoch-zero capacity candidates. It identifies the
        /// exact advisory sample a refusal disproved.
        capacity_sampled_ms: Option<u64>,
    },
    Fenced,
}

/// One authority observation from one state-machine boundary.
#[cfg(celld_internal_tests)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityObserverSnapshot {
    pub node_authoritative: bool,
    pub fenced: bool,
    pub now_mono_ms: u64,
    pub phase: Option<Phase>,
}

/// The independent inputs for one StepSafety census.
#[cfg(celld_internal_tests)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepSafetyObserverSnapshot {
    pub occupied_counter: usize,
    pub capacity_holder_ids: BTreeSet<CellId>,
    pub activation_permit_ids: BTreeSet<CellId>,
    pub activation_holder_ids: BTreeSet<CellId>,
}

/// The reported name of a phase. Stable across internal renames, because
/// `/state` and `celld diagnose` publish it.
fn phase_name(phase: &Phase) -> &'static str {
    match phase {
        Phase::Inactive => "inactive",
        Phase::WaitingActivation => "waiting_activation",
        Phase::ReadingOwner { .. } => "reading_owner",
        Phase::ReadingNodeLease { .. } => "reading_node_lease",
        Phase::RecoveringOwnerLog { .. } => "recovering_owner_log",
        Phase::WaitingOwnerLogRecovery { .. } => "waiting_owner_log_recovery",
        Phase::ReadingCapacity { .. } => "reading_capacity",
        Phase::WaitingCapacity => "waiting_capacity",
        Phase::Acquiring { .. } => "acquiring",
        Phase::ReconcilingAcquire { .. } => "reconciling_acquire",
        Phase::Restoring { .. } => "restoring",
        Phase::Starting { .. } => "starting",
        Phase::Publishing { .. } => "publishing",
        Phase::EnsuringDurability { .. } => "ensuring_durability",
        Phase::Cleaning {
            cause: StopCause::Swap,
            ..
        } => "swapping",
        Phase::Cleaning { .. } => "cleaning",
        Phase::Dormant { .. } => "dormant",
        Phase::Adopting { .. } => "adopting",
        Phase::Resident { .. } => "resident",
        Phase::Remote { .. } => "remote",
        Phase::Fenced => "fenced",
    }
}

/// What waits behind an output gate.
///
/// An output waits to leave on its channel. A fired alarm waits for its own
/// settlement, because the core orders the consume-side wake-entry delete from
/// the far side of the gate: deleting the entry while the consuming commit is
/// still local would lose both the alarm and the record that could revive it.
///
/// The two settle differently and that is the whole reason this is an enum: an
/// output is released to the shell, an alarm is replayed back into the core. A
/// channel cannot express the difference -- an alarm has no request to route
/// by and no adapter holding anything for it -- so it is a second kind of
/// owner rather than a seventh [`Channel`].
#[derive(Clone, Debug, PartialEq, Eq)]
enum GateOwner {
    /// An output withheld until every write it can reveal is proven.
    Output(Held),
    /// The firing alarm's op, and the observation `fire_alarm` made. Both are
    /// replayed into `alarm_finished` when the gate settles.
    Alarm {
        alarm: OpId,
        at_ms: Option<i64>,
        covered: bool,
    },
}

/// One local write held open by the output gate.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Barrier {
    /// What opened it. An output's `channel` is returned verbatim in the
    /// release effect; nothing in the core reads it.
    owner: GateOwner,
    cell: CellId,
    epoch: Epoch,
    position: u64,
    /// Read-only outputs that finished after this write committed but before
    /// its durability proof completed. They reveal the same state, so they
    /// share the write's verdict -- one proof, one verdict, however many
    /// channels are waiting on it.
    followers: Vec<Held>,
}

/// One withheld output: which request raised it, and which way it leaves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Held {
    request: RequestId,
    channel: Channel,
}

/// One reason that a quiescing cell cannot enter its handoff yet.
///
/// This is a read-only management projection. The core remains the authority
/// for the request and WebSocket indexes that produce it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DrainPin {
    pub cell: CellId,
    pub request: Option<RequestId>,
    pub active_request_count: usize,
    pub output_gate_count: usize,
    pub regular_websocket_count: usize,
    pub outbound_websocket_count: usize,
}

#[derive(Clone, Debug)]
struct Cell {
    phase: Phase,
    /// The highest committed-write position a verified proof has covered in
    /// this residency, and the epoch it belongs to. A read-only output that
    /// observed no more than this reveals nothing a crash can still lose, so
    /// it needs no proof of its own. `set_phase` clears it when a residency
    /// begins, because positions belong to one runtime's connection, and a
    /// proof at another epoch covers nothing here.
    proven_position: Option<(Epoch, u64)>,
    /// Failed dead-owner log recovery cycles in the current episode.
    /// Reset when a recovery succeeds or the episode fails its cap, so
    /// a later episode starts with the full budget.
    owner_log_recovery_attempts: u32,
    /// The shutdown drain has reserved this resident cell for its next
    /// handoff batch. New requests wait for the successor route while work
    /// which was already admitted finishes on this runtime.
    quiescing: bool,
    requests: BTreeSet<RequestId>,
    websockets: BTreeMap<WebSocketId, WebSocketKind>,
    waiting_for: Option<Activation>,
    waiting_activation: Option<ColdStart>,
    /// The in-flight release of this cell's ownership record, if any.
    releasing: Option<OpId>,
    /// Whether the eviction now under way hands this cell to the fleet.
    evict_rebalance: bool,
    /// The epoch this node holds the record at, kept across the transient
    /// wake phases. A dormant cell that waits for an activation permit still
    /// routes every request here, and a fleet-wide wake wave otherwise hides
    /// hundreds of owned cells per node from the published count for a
    /// minute; the 2026-09-03 fleet moved cells on that dip.
    held_epoch: Option<Epoch>,
    /// A balancing move is in progress on a node that is not draining. It
    /// carries the drain's handoff semantics for this one dormant cell: a
    /// request waits for the successor, and the release continues into
    /// adoption.
    handoff: bool,
    alarm: Option<AlarmState>,
    alarm_wake: bool,
    /// Startup, rather than a request or alarm, demands this activation.
    resume_demand: bool,
    /// A restarted process is reading this cell's record to confirm what it
    /// owns. Like startup demand it keeps the read queued, but it wakes
    /// nothing: the read settles the cell to dormant or inactive.
    confirming: bool,
    /// When this cell's eviction was last refused, if it has been.
    ///
    /// A cell that cannot prove its replica durable goes back to residency,
    /// and being cold is exactly why nobody registered it -- so it settles at
    /// the head of the eviction order and is chosen again on the next pass,
    /// and the one after. Enough of those and the node has no shed candidate
    /// it will ever succeed with. Recording the refusal lets the order prefer
    /// cells that have not just failed, while still coming back to them when
    /// they are all that is left.
    eviction_refused_mono_ms: Option<u64>,
    /// The node-wide identity of the V8 heap holding this cell's realm.
    /// Eviction order reads it: taking a heap's last cell returns the heap,
    /// while taking one of thirty-two returns only a cell record.
    isolate: Option<isolate::HeapId>,
    /// The application generation whose isolate holds this cell's realm.
    /// Zero until the shell reports a start, and the swap pump reads zero as
    /// stale: a cell it cannot place is moved rather than trusted.
    generation: u64,
    /// This cell is moving to the current generation: quiescing for the
    /// swap, stopped, or starting again. Cleared when it publishes, and by
    /// any phase change that leaves the swap path.
    swapping: bool,
    /// A forced swap already cancelled this cell's activity. It asks once.
    swap_cancelled: bool,
    /// The queue deadline now watching this cell, if it is parked. A timer
    /// carrying any other generation has been outlived and expires nothing.
    queued_generation: Option<u64>,
    /// When this cell last did something, on the remembered monotonic clock.
    ///
    /// Eviction order is the whole reason this exists. Without it the shed
    /// candidate is whichever cell sorts first by id, so a node under
    /// sustained pressure evicts its alphabetically-first cell over and over
    /// -- restoring and shedding the busiest cell on the node while an idle
    /// one further down the alphabet is never touched.
    last_used_mono_ms: u64,
    /// The newest local request routed to this cell.
    ///
    /// A load sample updates the monotonic clock less often than requests
    /// arrive, so many busy cells can share one `last_used_mono_ms` and fall
    /// back to lexical drain order. Request IDs are monotonically increasing
    /// core inputs, so this field preserves their exact replayable order.
    last_used_request: RequestId,
    /// How many local requests this process routed to the cell.
    ///
    /// A bounded handoff protects a shared singleton before a newer one-shot
    /// cell. The count is process-local because the drain orders only this
    /// process's resident set.
    local_request_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AlarmState {
    Armed {
        at_ms: i64,
        generation: u64,
        covered: bool,
    },
    Firing {
        op: OpId,
        at_ms: i64,
        generation: u64,
        covered: bool,
        /// The newest runtime observation that arrived before the firing's
        /// durability result. It cannot settle the alarm by itself.
        observed: Option<(Option<i64>, bool)>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AlarmObservation {
    at_ms: Option<i64>,
    covered: bool,
    firing_settled: bool,
}

#[derive(Clone, Debug)]
struct HeldNodeLease {
    spec: NodeLeaseSpec,
    record: NodeLeaseRecord,
    last_ok_mono_ms: u64,
    last_attempt_mono_ms: u64,
    timer_generation: u64,
}

#[derive(Clone, Debug)]
struct PendingNodeLease {
    spec: NodeLeaseSpec,
    desired: NodeLeaseRecord,
    prior: Option<HeldNodeLease>,
    /// This read follows a write whose result was ambiguous. It may prove the
    /// desired record landed, but must not turn a failed create into an
    /// unbounded read/write loop.
    readback_only: bool,
    /// The monotonic instant `desired.expires_ms` was computed from.
    ///
    /// Authority is bounded by what the *bucket* says, and the bucket says
    /// `expires_ms`, which is sampled here rather than when the write lands.
    /// Anchoring the local fence deadline to the write's completion instead
    /// would let this node serve for the whole duration of the round trip
    /// after every peer is entitled to declare it dead -- the store is under
    /// no obligation to answer quickly, so that window is unbounded.
    anchor_mono_ms: u64,
    /// This initial CAS replaces the exact live generation certified by the
    /// clean local shutdown. Renewals never set this bit.
    resume_local: bool,
}

#[derive(Clone, Debug)]
enum NodeAuthority {
    Unstarted,
    Reading {
        op: OpId,
        pending: PendingNodeLease,
    },
    Writing {
        op: OpId,
        pending: PendingNodeLease,
    },
    Held(HeldNodeLease),
    /// A continuous node whose initial acquisition did not succeed, waiting to
    /// try again.
    ///
    /// `StartNodeLease` is emitted exactly once, at process start, so without
    /// this the first failed acquisition is permanent: the process stays up,
    /// holds no authority, and answers every request `NodeUnavailable` for as
    /// long as it runs. One slow or rate-limited bucket response at startup
    /// should not cost a node its whole lifetime.
    Retrying {
        spec: NodeLeaseSpec,
        generation: u64,
    },
    Failed,
    Fenced,
}

impl Default for Cell {
    fn default() -> Self {
        Self {
            phase: Phase::Inactive,
            proven_position: None,
            owner_log_recovery_attempts: 0,
            quiescing: false,
            requests: BTreeSet::new(),
            websockets: BTreeMap::new(),
            waiting_for: None,
            waiting_activation: None,
            releasing: None,
            evict_rebalance: false,
            held_epoch: None,
            handoff: false,
            alarm: None,
            alarm_wake: false,
            resume_demand: false,
            confirming: false,
            isolate: None,
            generation: 0,
            swapping: false,
            swap_cancelled: false,
            queued_generation: None,
            eviction_refused_mono_ms: None,
            last_used_mono_ms: 0,
            last_used_request: 0,
            local_request_count: 0,
        }
    }
}

/// The last cut the walk down took, and what the node looked like when it did.
///
/// The measurement travels with the value because the two are not comparable.
/// When the latches change, so does the number the walk down reads, and
/// comparing a fresh resident set size against a stale in-use figure finds them
/// flat by coincidence.
#[derive(Clone, Copy, Debug)]
struct ShedCut {
    /// The measurement `bytes` was read from.
    metric: pressure::Metric,
    /// What that measurement said at the cut.
    bytes: u64,
    /// The residency the cut was taken at.
    cells: usize,
}

/// All authoritative coordination state for the first vertical slice.
pub struct State {
    node: NodeId,
    config: Config,
    fenced: bool,
    /// A shutdown handoff is under way: every resident cell is to be
    /// released, pumped at most `max_releases` at a time. Sticky -- a
    /// draining node never goes back to serving.
    draining: bool,
    /// Number of cell-table entries inspected by the shutdown release pump.
    /// Instrumentation uses this to enforce a structural work bound instead
    /// of timing a host whose speed is unrelated to the decision.
    #[cfg(celld_internal_tests)]
    drain_table_visits: u64,
    /// Cells reserved at the shutdown or generation-swap quiesce boundary.
    /// The set is bounded by `max_releases`, so an actor event can promote an
    /// idle reservation without walking the complete resident table.
    quiescing_cells: BTreeSet<CellId>,
    /// Cells whose owner-record release is in flight. This index makes both
    /// drain pacing and the 50 ms shell status poll independent of fleet size.
    releasing_cells: BTreeSet<CellId>,
    /// Released cells waiting for a successor acknowledgement. Like the other
    /// handoff indexes, this set is the exact bounded work queue.
    adopting_cells: BTreeSet<CellId>,
    /// An exact clean predecessor was replaced and its local inventory is
    /// being scanned or materialized. Readiness remains false until every
    /// discovered cell either publishes or fails closed.
    resuming: bool,
    resuming_cells: BTreeSet<CellId>,
    /// A restarted process is reading the ownership record of every cell it
    /// finds on its own disk, to count what it still owns without waking
    /// any of it. Until this finishes the published owned count is absent,
    /// so no peer balances against a node that only looks empty.
    confirming: bool,
    confirming_cells: BTreeSet<CellId>,
    next_op: OpId,
    next_timer_generation: u64,
    cells: BTreeMap<CellId, Cell>,
    request_cells: BTreeMap<RequestId, CellId>,
    /// Local routes handed to the executor but not yet reported complete.
    /// These are eviction pins: the adapter may still be running user code.
    active_requests: BTreeMap<RequestId, CellId>,
    /// Reverse index of `active_requests`: how many active requests target
    /// each cell. `is_active` reads it in O(1) where scanning the values was
    /// O(active_requests), and `worker_request` calls `is_active` once per
    /// resident cell — the product wedged cold-cell activation, the rate
    /// falling with the cell count (engine/pathological-load.md).
    active_cells: BTreeMap<CellId, usize>,
    /// Local write responses withheld by the output gate until their cell is
    /// proven durable to the written position, keyed by the durability op. An
    /// open gate makes its cell active, so the cell cannot be evicted
    /// underneath it; a fence drains these to a failed response so a write is
    /// never acknowledged after the node loses authority.
    barriers: BTreeMap<OpId, Barrier>,
    /// Requests whose activity ended while a write of theirs was still on the
    /// output gate. The pin outlives the activity: it keeps the cell from being
    /// evicted under the gate, and keeps a later write of the same request able
    /// to open one. See `activity_finished`.
    gate_pinned: BTreeSet<RequestId>,
    /// Last resident selected for top-level Worker execution. Cell IDs are
    /// ordered, so this is enough to replay a fair round-robin without shell
    /// atomics or registry iteration order leaking into behavior.
    worker_cursor: Option<CellId>,
    activity: ActivitySnapshot,
    activation_waiters: VecDeque<CellId>,
    /// Cells holding a complete cold-route admission. Keeping this explicit,
    /// instead of inferring it from executor tasks, makes the concurrency bound
    /// part of the replayable state machine.
    activation_permits: BTreeSet<CellId>,
    /// Cells with an eviction in flight -- from the durability proof through
    /// the runtime stop. Explicit for the same reason as the activation
    /// permits: the bound belongs in the replayable state machine rather than
    /// being implied by how many executor tasks happen to exist.
    eviction_permits: BTreeSet<CellId>,
    /// Cells waiting for a residency slot, in arrival order. FIFO is the
    /// whole admission policy: waking every waiter on a release and letting
    /// them race is unfair by construction — under sustained eviction a
    /// waiter can time out while thousands of slots are freed around it.
    /// The queue converts "eventually, probably" into a bound: a waiter with
    /// `k` waiters ahead of it is admitted within `k` releases, so no
    /// arrival pattern can starve it.
    capacity_waiters: VecDeque<CellId>,
    /// Requests received as epoch-zero capacity handoffs. Keeping the mode on
    /// the request rather than in the HTTP adapter makes the admit/refuse race
    /// atomic with every other lifecycle transition.
    capacity_requests: BTreeSet<RequestId>,
    /// Signed shutdown handoffs that need ownership but not a warm runtime.
    /// A real request joining the same acquisition still causes the normal
    /// restore, so demand always outranks the lazy handoff optimization.
    handoff_requests: BTreeSet<RequestId>,
    /// Reservations made against each unchanged advisory sample. Concurrent
    /// lookups are applied one event at a time, so projected load cannot pick
    /// the same advertised final slot twice by accident.
    capacity_reservations: BTreeMap<NodeId, usize>,
    capacity_samples: BTreeMap<NodeId, u64>,
    /// A refusal disproves exactly one load sample. The node becomes eligible
    /// again only after its lease advertises a newer sample.
    capacity_rejections: BTreeMap<NodeId, u64>,
    /// Live routing authority is shared by every cell owned by the same node.
    /// Keeping that cache here makes expiry and invalidation deterministic,
    /// rather than an invisible executor optimization.
    node_lease_cache: BTreeMap<NodeId, NodeLeaseRecord>,
    /// Start/publish effects invalidated by fencing may still commit. Their
    /// late completion must trigger compensating cleanup, not be ignored.
    retired_runtime_ops: BTreeMap<OpId, (CellId, Epoch)>,
    /// Runtime starts which exceeded their request deadline but still own a
    /// live executor task. A start cannot be cancelled safely, so the core
    /// keeps its phase and operation until the real completion arrives. New
    /// callers fail fast instead of starting a duplicate runtime for the same
    /// cell and epoch.
    timed_out_runtime_starts: BTreeSet<OpId>,
    /// The cell each in-flight cell-scoped operation belongs to. Completion
    /// events name only the op; this index is what makes resolving one a
    /// lookup instead of a walk of every cell.
    cell_ops: BTreeMap<OpId, CellId>,
    /// How many cells currently occupy capacity, maintained at every phase
    /// transition. `has_capacity` asks on every admission and `validate` on
    /// every debug event; counting by walking the map priced both by the
    /// total cell count rather than the answer.
    occupied: usize,
    node_authority: NodeAuthority,
    /// A NudgeNodeLease arrived while a lease write was already in flight.
    /// That write's body was serialized BEFORE the
    /// nudge's publish, so it cannot carry it. Drain the flag by renewing
    /// again the moment the in-flight write settles into Held.
    nudge_pending: bool,
    /// The most recent wall-clock instant any event carried. Alarms are wall
    /// clock, so judging whether one is imminent needs this rather than the
    /// monotonic reading next to it.
    now_ms: u64,
    /// The most recent monotonic instant any event carried. Not a clock read:
    /// the core never asks what time it is, it remembers what it was told, so
    /// a handler that was not handed a timestamp can still arm a deadline
    /// relative to the event being processed.
    now_mono_ms: u64,
    /// How far down the current sample asked the node to shed.
    ///
    /// The walk down runs at eviction speed rather than sample speed: each
    /// completed eviction starts the next while residency is still above this
    /// floor. Without one it is a cell per sample, so a node fifteen over its
    /// watermark refuses work for fifteen sampling periods. Recomputed every
    /// sample, so a resource trigger comes down by a proportion of what was
    /// last measured rather than aiming at a cell count that means nothing to
    /// it.
    shed_floor: usize,
    /// The cut the current shed floor was set from. A later latched sample
    /// compares against this: a completed cut that left the number flat makes
    /// another cut futile.
    shed_cut: Option<ShedCut>,
    /// Which ceilings are latched. Held apart from `shed_reason`, which names
    /// only the more serious crossing.
    latches: pressure::Latches,
    /// The application generation the node serves, as last announced by
    /// `Event::GenerationChanged`. Zero before any announcement, when no cell
    /// is stale.
    current_generation: u64,
    /// When the current generation was announced, on the monotonic clock.
    /// The maximum age counts from here.
    generation_changed_mono_ms: u64,
    /// How long a resident cell may stay on an older generation before its
    /// swap is forced. Zero forces every stale cell at the announcement.
    swap_max_age_ms: u64,
    /// The classes whose cells are forced without waiting for the maximum
    /// age, from the last announcement.
    ///
    /// Held against the announcement rather than marked on each cell,
    /// because only a resident cell is ever nominated: a mark left a dormant
    /// reserved cell -- which is what a reserved cell is whenever it is idle
    /// -- carrying a forced flag that nothing could act on, and the core
    /// refuses that pair. Eagerness is a property of the class and the
    /// adoption, so the pump reads it where it decides, and no cell can hold
    /// a swap flag it is not swapping under.
    swap_eager_classes: BTreeSet<String>,
    /// The shedding latch. celld kept this in the executor, which meant the
    /// hysteresis -- the part with actual behaviour -- was the one piece the
    /// simulation could not reach. It is carried here so a sample
    /// sequence is replayable.
    shedding: bool,
    /// Whether the last load sample is below every configured low watermark.
    /// This is stricter than `!shedding` and reserves room for a fleet handoff.
    memory_headroom: bool,
    /// Which resource is holding the latch, for the effect the shell logs.
    shed_reason: Option<&'static str>,
}

impl State {
    pub fn new(node: impl Into<NodeId>, config: Config) -> Self {
        assert!(config.max_evictions > 0, "max_evictions must be positive");
        assert!(config.max_releases > 0, "max_releases must be positive");
        assert!(
            config.max_activations > 0,
            "max_activations must be positive"
        );
        Self {
            node: node.into(),
            config,
            fenced: false,
            draining: false,
            #[cfg(celld_internal_tests)]
            drain_table_visits: 0,
            quiescing_cells: BTreeSet::new(),
            releasing_cells: BTreeSet::new(),
            adopting_cells: BTreeSet::new(),
            current_generation: 0,
            generation_changed_mono_ms: 0,
            swap_max_age_ms: 0,
            swap_eager_classes: BTreeSet::new(),
            resuming: false,
            resuming_cells: BTreeSet::new(),
            confirming: false,
            confirming_cells: BTreeSet::new(),
            next_op: 1,
            next_timer_generation: 1,
            cells: BTreeMap::new(),
            request_cells: BTreeMap::new(),
            active_requests: BTreeMap::new(),
            active_cells: BTreeMap::new(),
            barriers: BTreeMap::new(),
            gate_pinned: BTreeSet::new(),
            worker_cursor: None,
            activity: ActivitySnapshot::default(),
            activation_waiters: VecDeque::new(),
            activation_permits: BTreeSet::new(),
            eviction_permits: BTreeSet::new(),
            capacity_waiters: VecDeque::new(),
            capacity_requests: BTreeSet::new(),
            handoff_requests: BTreeSet::new(),
            capacity_reservations: BTreeMap::new(),
            capacity_samples: BTreeMap::new(),
            capacity_rejections: BTreeMap::new(),
            node_lease_cache: BTreeMap::new(),
            retired_runtime_ops: BTreeMap::new(),
            timed_out_runtime_starts: BTreeSet::new(),
            cell_ops: BTreeMap::new(),
            occupied: 0,
            node_authority: NodeAuthority::Unstarted,
            nudge_pending: false,
            now_ms: 0,
            now_mono_ms: 0,
            shed_floor: 0,
            shed_cut: None,
            latches: pressure::Latches::default(),
            shedding: false,
            memory_headroom: false,
            shed_reason: None,
        }
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced
    }

    /// Authority is the lease's published validity evaluated at ask time on
    /// the monotonic clock — not the `Held` variant. A peer treats the record
    /// as dead the instant `now_ms` reaches `expires_ms`, so a holder whose
    /// lease lapsed must answer `false` here even if `Timer::NodeLeaseFence`
    /// has not fired yet: the timer is the liveness backstop that halts the
    /// process, never the safety mechanism. A suspended VM's timer fires
    /// late, but a request evaluated after resume still refuses. The `<` /
    /// `>=` polarity matches the fence timer at `handle_timer`, so the
    /// predicate and the timer cannot disagree.
    pub fn node_authoritative(&self) -> bool {
        if !self.config.require_node_lease {
            return !self.fenced;
        }
        let fresh = |held: &HeldNodeLease| {
            self.now_mono_ms.saturating_sub(held.last_ok_mono_ms) < held.spec.ttl_ms
        };
        match &self.node_authority {
            NodeAuthority::Held(held) => fresh(held),
            NodeAuthority::Reading { pending, .. } | NodeAuthority::Writing { pending, .. } => {
                pending.prior.as_ref().is_some_and(fresh)
            }
            _ => false,
        }
    }

    /// Whether the owned-cell count describes every record this node holds.
    /// False while a restarted process confirms its local inventory.
    pub fn ownership_confirmed(&self) -> bool {
        !self.confirming
    }

    /// Whether this node is handing its cells away. Published in the lease,
    /// where it keeps a draining node out of the balancer's receivers.
    pub fn draining(&self) -> bool {
        self.draining
    }

    /// Whether the process can accept new traffic.
    pub fn ready_to_serve(&self) -> bool {
        if self.fenced || self.resuming {
            return false;
        }
        self.node_authoritative()
    }

    pub fn phase(&self, cell: &str) -> Option<&Phase> {
        self.cells.get(cell).map(|cell| &cell.phase)
    }

    /// Return every cell phase for an opt-in transition observer.
    ///
    /// An observer joins these snapshots by cell before it discards each cell
    /// identifier. A node-wide census cannot preserve that association.
    #[cfg(celld_internal_tests)]
    pub fn phase_snapshot_for_test(&self) -> BTreeMap<CellId, Phase> {
        self.cells
            .iter()
            .map(|(id, cell)| (id.clone(), cell.phase.clone()))
            .collect()
    }

    /// Return the authority inputs from one observation boundary.
    #[cfg(celld_internal_tests)]
    pub fn authority_observer_snapshot(&self, cell: &str) -> AuthorityObserverSnapshot {
        AuthorityObserverSnapshot {
            node_authoritative: self.node_authoritative(),
            fenced: self.fenced,
            now_mono_ms: self.now_mono_ms,
            phase: self.phase(cell).cloned(),
        }
    }

    /// Return both sides of each structural count from the decision core.
    #[cfg(celld_internal_tests)]
    pub fn step_safety_observer_snapshot(&self) -> StepSafetyObserverSnapshot {
        StepSafetyObserverSnapshot {
            occupied_counter: self.occupied,
            capacity_holder_ids: self
                .cells
                .iter()
                .filter(|(_, cell)| phase_occupies_capacity(&cell.phase))
                .map(|(id, _)| id.clone())
                .collect(),
            activation_permit_ids: self.activation_permits.clone(),
            activation_holder_ids: self
                .cells
                .iter()
                .filter(|(_, cell)| phase_holds_activation(&cell.phase))
                .map(|(id, _)| id.clone())
                .collect(),
        }
    }

    /// Which resource is currently holding the shedding latch, if any.
    ///
    /// A node that is refusing work should be able to say why; without this
    /// the operator sees only that admissions stopped.
    pub fn shed_reason(&self) -> Option<&'static str> {
        self.shed_reason
    }

    /// Whether this node is currently shedding, for peers ranking it as a
    /// placement target. A node that says no while walking down invites the
    /// work it is trying to get rid of.
    pub fn shedding(&self) -> bool {
        self.shedding
    }

    /// Whether the last load sample reserves memory below every pressure
    /// resume line. False before the first sample.
    pub fn memory_headroom(&self) -> bool {
        self.memory_headroom
    }

    /// Host-side sockets open across every cell. Each one pins its cell
    /// against eviction, so a node holding many is a poor landing place
    /// even when its residency looks unremarkable.
    pub fn host_websockets(&self) -> usize {
        self.cells.values().map(|cell| cell.websockets.len()).sum()
    }

    /// Does this cell still hold that socket? False when the core declined
    /// it, so the shell can answer the opener instead of closing underneath.
    pub fn holds_websocket(&self, id: &str, websocket: WebSocketId) -> bool {
        self.cells
            .get(id)
            .is_some_and(|cell| cell.websockets.contains_key(&websocket))
    }

    pub fn occupied(&self) -> usize {
        self.occupied
    }

    /// Cells whose ownership acquisition this state machine has confirmed.
    ///
    /// This includes dormant cells, which consume no residency capacity but
    /// still route every later request to this node, and a confirmed cell
    /// that is waking through the transient phases. A first acquisition is
    /// excluded until reconciliation proves that its CAS landed.
    pub fn owned_cells(&self) -> usize {
        self.cells
            .values()
            .filter(|cell| cell.held_epoch.is_some())
            .count()
    }

    /// Are fewer than `ceiling` cells occupying capacity?
    fn occupied_below(&self, ceiling: usize) -> bool {
        self.occupied < ceiling
    }

    pub fn residents(&self) -> Vec<CellId> {
        self.cells
            .iter()
            .filter(|(_, cell)| {
                matches!(
                    cell.phase,
                    Phase::Resident { .. } | Phase::EnsuringDurability { .. }
                )
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Resident cells which have stopped admitting new local requests so the
    /// shutdown drain can reach a final durability boundary.
    pub fn quiescing(&self) -> usize {
        self.quiescing_cells.len()
    }

    /// Return the release pump's exact cell-table work for a private bounded
    /// work assertion. This is not elapsed time, so the result is stable on
    /// every host and under every scheduler.
    #[cfg(celld_internal_tests)]
    pub fn drain_table_visits_for_test(&self) -> u64 {
        self.drain_table_visits
    }

    #[cfg(celld_internal_tests)]
    fn record_drain_table_visits(&mut self, visits: usize) {
        self.drain_table_visits = self.drain_table_visits.saturating_add(visits as u64);
    }

    /// Return every pin on every resident reserved for a shutdown handoff.
    /// A cell without an active request still gets one row, so a regular or
    /// outbound WebSocket cannot leave an otherwise empty stall invisible.
    pub fn drain_pins(&self) -> Vec<DrainPin> {
        let mut pins = Vec::new();
        for (cell, state) in self.cells.iter().filter(|(_, cell)| cell.quiescing) {
            let active: Vec<RequestId> = self
                .active_requests
                .iter()
                .filter_map(|(request, request_cell)| (request_cell == cell).then_some(*request))
                .collect();
            let regular_websocket_count = state
                .websockets
                .values()
                .filter(|kind| matches!(kind, WebSocketKind::Regular))
                .count();
            let outbound_websocket_count = state
                .websockets
                .values()
                .filter(|kind| matches!(kind, WebSocketKind::Outbound))
                .count();
            if active.is_empty() {
                pins.push(DrainPin {
                    cell: cell.clone(),
                    request: None,
                    active_request_count: 0,
                    output_gate_count: 0,
                    regular_websocket_count,
                    outbound_websocket_count,
                });
                continue;
            }
            let active_request_count = active.len();
            for request in active {
                pins.push(DrainPin {
                    cell: cell.clone(),
                    request: Some(request),
                    active_request_count,
                    output_gate_count: self
                        .barriers
                        .values()
                        .filter(|barrier| {
                            matches!(&barrier.owner, GateOwner::Output(held) if held.request == request)
                                || barrier.followers.iter().any(|held| held.request == request)
                        })
                        .count(),
                    regular_websocket_count,
                    outbound_websocket_count,
                });
            }
        }
        pins
    }

    /// Return the exact management projection for the current event boundary.
    /// Activations are not visible until publication; a runtime being held for
    /// durability remains visible because it is still published and routable.
    pub fn presence_snapshot(&self) -> PresenceSnapshot {
        let cells = self
            .cells
            .iter()
            .filter_map(|(id, cell)| match cell.phase {
                Phase::Resident { epoch } | Phase::EnsuringDurability { epoch, .. } => {
                    Some(PresenceCell {
                        id: id.clone(),
                        epoch,
                    })
                }
                _ => None,
            })
            .collect();
        PresenceSnapshot {
            serving: self.ready_to_serve(),
            cells,
            activity: self.activity,
        }
    }

    /// The isolate each resident cell sits in. A node holds one V8 heap per
    /// distinct value here, so the spread of this is the memory a walk down
    /// can still return.
    pub fn resident_isolates(&self) -> Vec<isolate::HeapId> {
        self.cells
            .values()
            .filter(|cell| matches!(cell.phase, Phase::Resident { .. }))
            .filter_map(|cell| cell.isolate)
            .collect()
    }

    pub fn waiting(&self) -> Vec<CellId> {
        self.capacity_waiters.iter().cloned().collect()
    }

    pub fn activation_waiting(&self) -> Vec<CellId> {
        self.activation_waiters.iter().cloned().collect()
    }

    pub fn activating(&self) -> usize {
        self.activation_permits.len()
    }

    /// Evictions in flight, from the durability proof through the runtime
    /// stop. During a drain this saturates at `max_releases`.
    pub fn evicting(&self) -> usize {
        self.eviction_permits.len()
    }

    /// How many cells sit in each phase, by a stable short name, omitting the
    /// phases with no cells.
    ///
    /// A node that refuses cells is diagnosed by where its cells are, and
    /// `occupied` cannot say: it counts residency, so a node holding thousands
    /// of cells part-way through a cold start reports almost none. A fleet held
    /// that state for fifteen minutes and the record could not say what the
    /// cells were doing (issue #50).
    ///
    /// The names are part of the operator interface. A chart and a human read
    /// them, so they do not change with an internal rename.
    pub fn phase_census(&self) -> Vec<(&'static str, usize)> {
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
        for cell in self.cells.values() {
            *counts.entry(phase_name(&cell.phase)).or_default() += 1;
        }
        counts.into_iter().collect()
    }

    /// Cold routes that have not finished: cells that hold an activation
    /// permit plus cells queued behind the activation ceiling. A capacity
    /// waiter already holds a permit, so this counts every cell once. This is
    /// capacity telemetry; a graceful handoff is paced by successor ownership
    /// acknowledgements instead.
    pub fn activation_backlog(&self) -> usize {
        self.activation_permits.len() + self.activation_waiters.len()
    }

    /// Ownership releases still in flight. The shutdown drain waits for
    /// zero: a released cell stops occupying capacity before its record
    /// write commits, so an exit gated on occupancy alone can outrun its
    /// own releases and leave records a successor must wait out the node
    /// lease for -- the takeover-at-once promise broken on every real
    /// store, where the write loses a race against a 50ms drain poll that
    /// a local fixture always wins.
    pub fn releasing(&self) -> usize {
        self.releasing_cells.len()
    }

    /// Released cells still waiting for a successor ownership acknowledgement.
    ///
    /// This count holds the shutdown handoff permit after the owner write has
    /// completed. Without it the release pump can blank the complete resident
    /// set before any successor reserves restoration capacity.
    pub fn adopting(&self) -> usize {
        self.adopting_cells.len()
    }

    /// Cell movements past nomination and before a successor acknowledgement.
    pub fn handoffs_in_flight(&self) -> usize {
        self.quiescing()
            .saturating_add(self.eviction_permits.len())
            .saturating_add(self.releasing())
            .saturating_add(self.adopting())
    }

    /// Completed successor ownership acknowledgements during this process lifetime.
    /// Shutdown uses this monotonic value to distinguish a slow, advancing
    /// handoff from one that has stopped making progress.
    pub fn handed_off(&self) -> u64 {
        self.activity.handed_off
    }

    /// Return an epoch this node has acquired for a dormant, warming, or
    /// published cell. A shutdown handoff acknowledges this authority without
    /// forcing the cell runtime into memory.
    pub fn accepted_epoch(&self, id: &str) -> Option<Epoch> {
        self.cells
            .get(id)
            .and_then(|cell| phase_owned_epoch(&cell.phase))
    }

    /// Cheap internal consistency gate run by both executors after every event.
    /// Check one withheld output: pinned on the barrier's cell, and the only
    /// output this request holds on that channel.
    fn validate_held(
        held: &Held,
        op: &OpId,
        barrier: &Barrier,
        active_requests: &BTreeMap<RequestId, CellId>,
        seen: &mut BTreeSet<(RequestId, &'static str)>,
    ) -> Result<(), String> {
        if active_requests.get(&held.request) != Some(&barrier.cell) {
            return Err(format!(
                "held output {} on {} behind op {op} is not pinned on {:?}",
                held.request,
                held.channel.as_str(),
                barrier.cell
            ));
        }
        if !seen.insert((held.request, held.channel.as_str())) {
            return Err(format!(
                "request {} has two outputs held on {}",
                held.request,
                held.channel.as_str()
            ));
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), String> {
        if !self.occupied_below(self.config.max_resident.saturating_add(1)) {
            return Err(format!(
                "occupied {} exceeds ceiling {}",
                self.occupied(),
                self.config.max_resident
            ));
        }
        let eviction_ceiling = self.eviction_ceiling();
        if self.eviction_permits.len() > eviction_ceiling {
            return Err(format!(
                "evicting {} exceeds ceiling {}",
                self.eviction_permits.len(),
                eviction_ceiling
            ));
        }
        let handoffs = self.handoffs_in_flight();
        if self.draining && handoffs > self.config.max_releases {
            return Err(format!(
                "handoffs {handoffs} exceeds release ceiling {}",
                self.config.max_releases
            ));
        }
        if self.activation_permits.len() > self.config.max_activations {
            return Err(format!(
                "activating {} exceeds ceiling {}",
                self.activation_permits.len(),
                self.config.max_activations
            ));
        }

        let mut activation_queued = BTreeSet::new();
        for id in &self.activation_waiters {
            if !activation_queued.insert(id) {
                return Err(format!("activation waiter {id:?} is queued twice"));
            }
            let Some(cell) = self.cells.get(id) else {
                return Err(format!("activation waiter {id:?} has no cell state"));
            };
            if cell.phase != Phase::WaitingActivation || cell.waiting_activation.is_none() {
                return Err(format!("activation waiter {id:?} is not waiting"));
            }
            if self.activation_permits.contains(id) {
                return Err(format!("activation waiter {id:?} also holds a permit"));
            }
        }
        for id in &self.activation_permits {
            let Some(cell) = self.cells.get(id) else {
                return Err(format!("activation permit {id:?} has no cell state"));
            };
            if !phase_holds_activation(&cell.phase) {
                return Err(format!(
                    "activation permit {id:?} is held in terminal phase {:?}",
                    cell.phase
                ));
            }
        }

        for (id, cell) in &self.cells {
            let in_pipeline = match cell.phase {
                Phase::Dormant { .. } => cell.releasing.is_some(),
                Phase::Adopting { .. } => true,
                _ => false,
            };
            if cell.handoff && !in_pipeline {
                return Err(format!(
                    "handoff cell {id:?} is outside the pipeline in {:?}",
                    cell.phase
                ));
            }
        }
        let actual_quiescing: BTreeSet<_> = self
            .cells
            .iter()
            .filter(|(_, cell)| cell.quiescing)
            .map(|(id, _)| id.clone())
            .collect();
        if actual_quiescing != self.quiescing_cells {
            return Err(format!(
                "quiescing index {:?} differs from cells {:?}",
                self.quiescing_cells, actual_quiescing
            ));
        }
        let actual_releasing: BTreeSet<_> = self
            .cells
            .iter()
            .filter(|(_, cell)| cell.releasing.is_some())
            .map(|(id, _)| id.clone())
            .collect();
        if actual_releasing != self.releasing_cells {
            return Err(format!(
                "releasing index {:?} differs from cells {:?}",
                self.releasing_cells, actual_releasing
            ));
        }
        let actual_adopting: BTreeSet<_> = self
            .cells
            .iter()
            .filter(|(_, cell)| matches!(cell.phase, Phase::Adopting { .. }))
            .map(|(id, _)| id.clone())
            .collect();
        if actual_adopting != self.adopting_cells {
            return Err(format!(
                "adopting index {:?} differs from cells {:?}",
                self.adopting_cells, actual_adopting
            ));
        }
        let actual_confirming: BTreeSet<_> = self
            .cells
            .iter()
            .filter(|(_, cell)| cell.confirming)
            .map(|(id, _)| id.clone())
            .collect();
        if actual_confirming != self.confirming_cells {
            return Err(format!(
                "confirming index {:?} differs from cells {:?}",
                self.confirming_cells, actual_confirming
            ));
        }
        for id in &self.confirming_cells {
            let cell = &self.cells[id];
            if !matches!(
                cell.phase,
                Phase::ReadingOwner { .. } | Phase::WaitingActivation
            ) {
                return Err(format!(
                    "confirming cell {id:?} left the owner read in {:?}",
                    cell.phase
                ));
            }
        }
        if !self.confirming && !self.confirming_cells.is_empty() {
            return Err("confirmation finished with cells still confirming".into());
        }
        let actual_resuming: BTreeSet<_> = self
            .cells
            .iter()
            .filter(|(_, cell)| cell.resume_demand)
            .map(|(id, _)| id.clone())
            .collect();
        if actual_resuming != self.resuming_cells {
            return Err(format!(
                "resuming index {:?} differs from cells {:?}",
                self.resuming_cells, actual_resuming
            ));
        }
        if !self.resuming && !actual_resuming.is_empty() {
            return Err(format!(
                "settled node retains resume demand for {actual_resuming:?}"
            ));
        }

        let mut queued = BTreeSet::new();
        for id in &self.capacity_waiters {
            if !queued.insert(id) {
                return Err(format!("capacity waiter {id:?} is queued twice"));
            }
            let Some(cell) = self.cells.get(id) else {
                return Err(format!("capacity waiter {id:?} has no cell state"));
            };
            if cell.phase != Phase::WaitingCapacity || cell.waiting_for.is_none() {
                return Err(format!("capacity waiter {id:?} is not waiting"));
            }
        }
        for (id, cell) in &self.cells {
            if cell.resume_demand
                && !matches!(
                    cell.phase,
                    Phase::WaitingActivation
                        | Phase::WaitingCapacity
                        | Phase::Restoring { .. }
                        | Phase::Starting { .. }
                        | Phase::Publishing { .. }
                        | Phase::Fenced
                )
            {
                return Err(format!(
                    "cell {id:?} retains resume demand in terminal phase {:?}",
                    cell.phase
                ));
            }
            if cell.quiescing && !matches!(cell.phase, Phase::Resident { .. }) {
                return Err(format!(
                    "cell {id:?} is quiescing in non-resident phase {:?}",
                    cell.phase
                ));
            }
            if cell.swapping
                && !matches!(
                    cell.phase,
                    Phase::Resident { .. }
                        | Phase::Cleaning {
                            cause: StopCause::Swap,
                            ..
                        }
                        | Phase::Starting { .. }
                        | Phase::Publishing { .. }
                )
            {
                return Err(format!(
                    "cell {id:?} is swapping in phase {:?}, outside the swap path",
                    cell.phase
                ));
            }
            if cell.swap_cancelled && !cell.swapping {
                return Err(format!("cell {id:?} carries a swap flag without swapping"));
            }
            let is_activation_queued = activation_queued.contains(id);
            if (cell.phase == Phase::WaitingActivation) != is_activation_queued {
                return Err(format!("cell {id:?} activation queue and phase disagree"));
            }
            if phase_requires_activation_permit(&cell.phase, cell.swapping)
                && !self.activation_permits.contains(id)
            {
                return Err(format!(
                    "cell {id:?} is in activation phase {:?} without a permit",
                    cell.phase
                ));
            }
            let is_queued = queued.contains(id);
            if (cell.phase == Phase::WaitingCapacity) != is_queued {
                return Err(format!("cell {id:?} queue and phase disagree"));
            }
            for request in &cell.requests {
                if self.request_cells.get(request) != Some(id) {
                    return Err(format!(
                        "request {request} index disagrees with cell {id:?}"
                    ));
                }
            }
            // Every in-flight cell op must be resolvable through the index,
            // or its completion event will never find this cell.
            let indexed = |op: OpId| self.cell_ops.get(&op) == Some(id);
            if let Some(op) = phase_op(&cell.phase) {
                if !indexed(op) {
                    return Err(format!("cell {id:?} phase op {op} is not indexed"));
                }
            }
            if let Some(AlarmState::Firing { op, .. }) = cell.alarm {
                if !indexed(op) {
                    return Err(format!("cell {id:?} firing alarm op {op} is not indexed"));
                }
            }
            if let Some(op) = cell.releasing {
                if !indexed(op) {
                    return Err(format!("cell {id:?} release op {op} is not indexed"));
                }
            }
        }
        let walked = self
            .cells
            .values()
            .filter(|cell| phase_occupies_capacity(&cell.phase))
            .count();
        if walked != self.occupied {
            return Err(format!(
                "occupied counter {} disagrees with the walk {walked}",
                self.occupied
            ));
        }
        for (request, id) in &self.request_cells {
            if !self
                .cells
                .get(id)
                .is_some_and(|cell| cell.requests.contains(request))
            {
                return Err(format!("request index {request} has no matching waiter"));
            }
        }
        for (op, id) in &self.cell_ops {
            let Some(cell) = self.cells.get(id) else {
                return Err(format!("cell op {op} points to missing cell {id:?}"));
            };
            if cell.phase == Phase::Fenced && cell.releasing != Some(*op) {
                return Err(format!("fenced cell {id:?} retains stale operation {op}"));
            }
        }
        for op in &self.timed_out_runtime_starts {
            let Some(id) = self.cell_ops.get(op) else {
                return Err(format!("timed-out runtime start op {op} is not indexed"));
            };
            if !self.cells.get(id).is_some_and(
                |cell| matches!(cell.phase, Phase::Starting { op: current, .. } if current == *op),
            ) {
                return Err(format!(
                    "timed-out runtime start op {op} has no matching phase"
                ));
            }
        }
        // The reverse index must agree with the map it summarizes, exactly.
        let mut recount: BTreeMap<CellId, usize> = BTreeMap::new();
        for id in self.active_requests.values() {
            *recount.entry(id.clone()).or_insert(0) += 1;
        }
        if recount != self.active_cells {
            return Err(format!(
                "active_cells index {:?} disagrees with active_requests {:?}",
                self.active_cells, recount
            ));
        }
        for (request, id) in &self.active_requests {
            if self.request_cells.contains_key(request) {
                return Err(format!("request {request} is both pending and active"));
            }
            if !self.cells.get(id).is_some_and(|cell| {
                matches!(
                    cell.phase,
                    Phase::Resident { .. } | Phase::EnsuringDurability { .. }
                )
            }) {
                return Err(format!(
                    "active request {request} has no resident cell {id:?}"
                ));
            }
        }
        // `(request, channel)` identifies a withheld output. The shell keys its
        // adapters on that pair, so a second output for the same pair would be
        // released once and stranded once.
        let mut held_outputs: BTreeSet<(RequestId, &'static str)> = BTreeSet::new();
        for (op, barrier) in &self.barriers {
            match &barrier.owner {
                // A held output keeps its request pinned, so the cell cannot be
                // evicted underneath the barrier and a later write of the same
                // request can still find it.
                GateOwner::Output(held) => {
                    Self::validate_held(
                        held,
                        op,
                        barrier,
                        &self.active_requests,
                        &mut held_outputs,
                    )?;
                }
                // An alarm pins nothing: it is not a request, so it has no
                // activity to hold. What keeps the cell resident is the firing
                // state itself, which `shed_candidate` refuses to evict. The
                // phase is deliberately not asserted here — an activity fold
                // can retire the firing state while this proof is still in
                // flight, and the barrier that outlives it is then settled by
                // whichever teardown takes the cell.
                GateOwner::Alarm { .. } => {
                    if !self.cells.contains_key(&barrier.cell) {
                        return Err(format!(
                            "gated alarm write op {op} names a cell that is gone: {:?}",
                            barrier.cell
                        ));
                    }
                }
            }
            // A follower is always an output: an alarm opens a barrier and
            // never trails one.
            for held in &barrier.followers {
                Self::validate_held(held, op, barrier, &self.active_requests, &mut held_outputs)?;
            }
        }
        for (id, cell) in &self.cells {
            if matches!(cell.alarm, Some(AlarmState::Firing { .. }))
                && !matches!(cell.phase, Phase::Resident { .. })
            {
                return Err(format!("cell {id:?} is firing an alarm while not resident"));
            }
            if cell
                .websockets
                .values()
                .any(|kind| matches!(kind, WebSocketKind::Regular | WebSocketKind::Outbound))
                && !matches!(
                    cell.phase,
                    Phase::Resident { .. } | Phase::EnsuringDurability { .. }
                )
            {
                return Err(format!(
                    "cell {id:?} has a live transport while not resident"
                ));
            }
        }
        Ok(())
    }

    fn op(&mut self) -> OpId {
        let op = self.next_op;
        self.next_op = self.next_op.checked_add(1).expect("operation id exhausted");
        op
    }

    /// Allocate an op that will live in `cell`'s phase, alarm, or release
    /// slot, and index it.
    fn cell_op(&mut self, cell: &str) -> OpId {
        let op = self.op();
        self.cell_ops.insert(op, cell.to_string());
        op
    }

    /// Resolve a completion's op to its cell, consuming the index entry —
    /// but only when `predicate` confirms the cell still holds the op. On a
    /// mismatch the entry stays: the op's real completion has not arrived
    /// yet (an expiry can probe the wrong handler), and it must still find
    /// its way here later.
    fn take_cell_op(&mut self, op: OpId, predicate: impl Fn(&Cell) -> bool) -> Option<CellId> {
        let id = self.cell_ops.get(&op)?.clone();
        if !self.cells.get(&id).is_some_and(predicate) {
            return None;
        }
        self.cell_ops.remove(&op);
        Some(id)
    }

    fn has_capacity(&self) -> bool {
        // Residency is a hard cap, known exactly and counted -- never sampled.
        // A node at its cell cap is at capacity, not overloaded: it refuses
        // more and holds what it has, rather than shedding a live cell it must
        // then place again elsewhere. The only sampled fact that refuses
        // admission is genuine memory pressure, which a cell count
        // cannot see.
        self.occupied_below(self.config.max_resident) && !self.shedding
    }

    pub fn is_active(&self, id: &str) -> bool {
        self.active_cells.contains_key(id)
            || self.cells.get(id).is_some_and(|cell| {
                cell.websockets
                    .values()
                    .any(|kind| matches!(kind, WebSocketKind::Regular | WebSocketKind::Outbound))
            })
    }

    /// Record `request` as active on `cell`, keeping the `active_cells`
    /// reverse index in step with `active_requests`. Every insert into
    /// `active_requests` goes through here so the two never disagree.
    fn activate_request(&mut self, request: RequestId, cell: CellId) {
        *self.active_cells.entry(cell.clone()).or_insert(0) += 1;
        self.active_requests.insert(request, cell);
    }

    /// Drop `request` from the active set, decrementing its cell's index and
    /// forgetting the cell once no request targets it. Returns the cell the
    /// request held, as `active_requests.remove` did.
    fn deactivate_request(&mut self, request: RequestId) -> Option<CellId> {
        let cell = self.active_requests.remove(&request)?;
        if let Some(count) = self.active_cells.get_mut(&cell) {
            *count -= 1;
            if *count == 0 {
                self.active_cells.remove(&cell);
            }
        }
        Some(cell)
    }

    /// Hibernated (Durable Objects state 4): out of memory, still on this
    /// host, with WebSocket clients parked at the network layer. Nothing in
    /// memory survives -- the next event runs the constructor again, exactly
    /// as a cold start does -- so the surviving sockets are the whole of the
    /// difference from `Inactive`.
    pub fn is_hibernated(&self, id: &str) -> bool {
        self.cells.get(id).is_some_and(|cell| {
            matches!(cell.phase, Phase::Dormant { .. })
                && cell
                    .websockets
                    .values()
                    .any(|kind| matches!(kind, WebSocketKind::Hibernatable))
        })
    }

    /// Hibernatable (Durable Objects state 2): resident, doing no work, and
    /// holding nothing that must survive in memory. State 3 is the negation --
    /// resident and idle, but pinned by a `Regular` or `Outbound` socket that
    /// cannot outlive the isolate.
    ///
    /// This is the cell's own criteria, not a decision to evict. celld also
    /// refuses eviction for an imminent or uncovered alarm and for state the
    /// bucket cannot restore; those are policy and are checked separately.
    pub fn is_hibernatable(&self, id: &str) -> bool {
        self.cells
            .get(id)
            .is_some_and(|cell| matches!(cell.phase, Phase::Resident { .. }))
            && !self.is_active(id)
    }

    pub fn websocket_count(&self, id: &str) -> usize {
        self.cells.get(id).map_or(0, |cell| cell.websockets.len())
    }

    fn record_local_request(cell: &mut Cell, request: RequestId, now_mono_ms: u64) {
        cell.last_used_mono_ms = now_mono_ms;
        cell.last_used_request = cell.last_used_request.max(request);
        cell.local_request_count = cell
            .local_request_count
            .checked_add(1)
            .expect("local request count exhausted");
    }

    fn complete_request(
        &mut self,
        id: &str,
        request: RequestId,
        result: Result<Route, RequestError>,
        effects: &mut Vec<Effect>,
    ) {
        self.capacity_requests.remove(&request);
        self.handoff_requests.remove(&request);
        match &result {
            Ok(Route::Local) => {
                self.activate_request(request, id.to_string());
                let now = self.now_mono_ms;
                if let Some(cell) = self.cells.get_mut(id) {
                    Self::record_local_request(cell, request, now);
                }
            }
            Ok(Route::Remote { .. }) => {
                self.activity.proxied = self.activity.proxied.saturating_add(1);
            }
            Err(_) => {}
        }
        effects.push(Effect::Complete { request, result });
    }

    fn record_acquisition(&mut self, spec: &RestoreSpec) {
        self.activity.acquired = self.activity.acquired.saturating_add(1);
        if spec.took_over {
            self.activity.expired_owner_leases =
                self.activity.expired_owner_leases.saturating_add(1);
        }
        if spec.epoch > 1 {
            self.activity.advanced_epochs = self.activity.advanced_epochs.saturating_add(1);
        }
    }

    /// Whether a cell takes the handoff path: the node is draining, or a
    /// balancing move reserved this one cell.
    fn hands_off(&self, cell: &Cell) -> bool {
        self.draining || cell.handoff
    }

    /// Return a voluntary eviction to service as one transition. Every
    /// rescue returns the permit with the phase, or the cell looks resident
    /// while a permit stays spent.
    fn cancel_eviction(&mut self, id: &str, cell: &mut Cell, epoch: Epoch) {
        if let Some(op) = phase_op(&cell.phase) {
            self.cell_ops.remove(&op);
        }
        set_phase(&mut self.occupied, cell, Phase::Resident { epoch });
        self.eviction_permits.remove(id);
    }

    fn worker_request(&mut self, request: RequestId, effects: &mut Vec<Effect>) {
        let route = self
            .cells
            .iter()
            .filter_map(|(id, cell)| {
                let (epoch, retired_durability) = match cell.phase {
                    Phase::Resident { epoch } => (epoch, None),
                    Phase::EnsuringDurability { op, epoch } => (epoch, Some(op)),
                    _ => return None,
                };
                if cell.quiescing
                    || self.is_active(id)
                    || matches!(cell.alarm, Some(AlarmState::Firing { .. }))
                {
                    return None;
                }
                Some((id.clone(), epoch, retired_durability))
            })
            .find(|(id, _, _)| self.worker_cursor.as_ref().is_none_or(|cursor| id > cursor))
            .or_else(|| {
                self.cells.iter().find_map(|(id, cell)| {
                    let (epoch, retired_durability) = match cell.phase {
                        Phase::Resident { epoch } => (epoch, None),
                        Phase::EnsuringDurability { op, epoch } => (epoch, Some(op)),
                        _ => return None,
                    };
                    (!cell.quiescing
                        && !self.is_active(id)
                        && !matches!(cell.alarm, Some(AlarmState::Firing { .. })))
                    .then(|| (id.clone(), epoch, retired_durability))
                })
            })
            .map(|(cell, epoch, retired_durability)| WorkerRoute {
                cell,
                epoch,
                retired_durability,
            });

        if let Some(route) = &route {
            self.worker_cursor = Some(route.cell.clone());
            self.activate_request(request, route.cell.clone());
            if let Some(mut cell) = self.cells.remove(&route.cell) {
                if matches!(cell.phase, Phase::EnsuringDurability { .. }) {
                    self.cancel_eviction(&route.cell, &mut cell, route.epoch);
                }
                Self::record_local_request(&mut cell, request, self.now_mono_ms);
                self.cells.insert(route.cell.clone(), cell);
            }
        }
        effects.push(Effect::CompleteWorker { request, route });
    }

    fn finish_requests(
        &mut self,
        id: &str,
        cell: &mut Cell,
        result: Result<Route, RequestError>,
        effects: &mut Vec<Effect>,
    ) {
        for request in std::mem::take(&mut cell.requests) {
            self.request_cells.remove(&request);
            if result == Ok(Route::Local) {
                Self::record_local_request(cell, request, self.now_mono_ms);
            }
            self.complete_request(id, request, result.clone(), effects);
        }
    }

    fn begin_cold_route(
        &mut self,
        id: &str,
        cell: &mut Cell,
        start: ColdStart,
        effects: &mut Vec<Effect>,
    ) {
        debug_assert!(self.activation_permits.contains(id));
        cell.waiting_activation = None;
        match start {
            ColdStart::ReadOwner => {
                let op = self.cell_op(id);
                set_phase(&mut self.occupied, cell, Phase::ReadingOwner { op });
                effects.push(Effect::ReadOwner {
                    op,
                    cell: id.to_string(),
                });
            }
            ColdStart::Restore(spec) => {
                self.activate_or_wait(id, cell, Activation::Restore(spec), effects)
            }
        }
    }

    fn admit_or_queue_activation(
        &mut self,
        id: &str,
        cell: &mut Cell,
        start: ColdStart,
        effects: &mut Vec<Effect>,
    ) {
        if self.activation_permits.contains(id) {
            self.begin_cold_route(id, cell, start, effects);
        } else if self.activation_permits.len() < self.config.max_activations {
            self.activation_permits.insert(id.to_string());
            self.begin_cold_route(id, cell, start, effects);
        } else {
            set_phase(&mut self.occupied, cell, Phase::WaitingActivation);
            cell.waiting_activation = Some(start);
            self.activation_waiters.push_back(id.to_string());
            self.watch_queued(id, cell, effects);
        }
    }

    fn pump_activations(&mut self, effects: &mut Vec<Effect>) {
        self.activation_permits.retain(|id| {
            self.cells
                .get(id)
                .is_some_and(|cell| phase_holds_activation(&cell.phase))
        });

        while self.activation_permits.len() < self.config.max_activations {
            let Some(id) = self.activation_waiters.pop_front() else {
                break;
            };
            let Some(mut cell) = self.cells.remove(&id) else {
                continue;
            };
            if cell.phase != Phase::WaitingActivation {
                self.cells.insert(id, cell);
                continue;
            }
            let Some(start) = cell.waiting_activation.take() else {
                self.cells.insert(id, cell);
                continue;
            };
            if cell.requests.is_empty()
                && !cell.alarm_wake
                && !cell.resume_demand
                && !cell.confirming
            {
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    match start {
                        ColdStart::ReadOwner => Phase::Inactive,
                        ColdStart::Restore(spec) => Phase::Dormant { epoch: spec.epoch },
                    },
                );
            } else {
                self.activation_permits.insert(id.clone());
                self.begin_cold_route(&id, &mut cell, start, effects);
            }
            self.cells.insert(id, cell);
        }
    }

    /// Adopt the exact epochs encoded by a clean predecessor's local paths.
    /// The node lease CAS that caused this event is the ownership proof, so
    /// this path deliberately emits no per-cell ownership effects.
    fn local_cells_read(
        &mut self,
        result: Result<Vec<LocalCell>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        if self.confirming {
            self.confirm_local_cells(result, effects);
            return;
        }
        if !self.resuming {
            return;
        }
        let Ok(mut cells) = result else {
            self.resuming = false;
            return;
        };
        // A preserved eviction copy is not a resumable epoch.
        cells.retain(|cell| cell.live);
        cells.sort();
        cells.dedup();
        if cells.len() > self.config.max_resident || !self.cells.is_empty() {
            self.resuming = false;
            return;
        }
        for local in cells {
            let mut cell = Cell {
                resume_demand: true,
                ..Cell::default()
            };
            self.resuming_cells.insert(local.id.clone());
            self.admit_or_queue_activation(
                &local.id,
                &mut cell,
                ColdStart::Restore(RestoreSpec {
                    epoch: local.epoch,
                    fresh: false,
                    took_over: false,
                    resume_local: true,
                    prior: None,
                }),
                effects,
            );
            self.cells.insert(local.id, cell);
        }
        if self.resuming_cells.is_empty() {
            self.resuming = false;
        }
    }

    /// Read the ownership record of every cell on this node's disk that the
    /// process does not know yet, and settle each one to dormant if the
    /// record still names this node, without waking it. A crash restart
    /// otherwise publishes only the cells that traffic has touched, and its
    /// peers balance against a node that looks empty. The startup demand
    /// marker keeps the read queued behind the activation ceiling; a request
    /// that arrives first turns the read into an ordinary wake.
    fn confirm_local_cells(
        &mut self,
        result: Result<Vec<LocalCell>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Ok(mut cells) = result else {
            self.confirming = false;
            return;
        };
        cells.sort();
        cells.dedup();
        for local in cells {
            if self.cells.contains_key(&local.id) || self.confirming_cells.contains(&local.id) {
                continue;
            }
            let mut cell = Cell {
                confirming: true,
                ..Cell::default()
            };
            self.confirming_cells.insert(local.id.clone());
            self.admit_or_queue_activation(&local.id, &mut cell, ColdStart::ReadOwner, effects);
            self.cells.insert(local.id, cell);
        }
        if self.confirming_cells.is_empty() {
            self.confirming = false;
        }
    }

    fn settle_confirmation(&mut self, id: &str) {
        if self.confirming_cells.remove(id) && self.confirming_cells.is_empty() {
            self.confirming = false;
        }
    }

    fn settle_local_resume(&mut self, id: &str) {
        if self.resuming_cells.remove(id) && self.resuming_cells.is_empty() {
            self.resuming = false;
        }
        if let Some(cell) = self.cells.get_mut(id) {
            cell.resume_demand = false;
        }
    }

    fn begin_activation(
        &mut self,
        id: &str,
        cell: &mut Cell,
        activation: Activation,
        effects: &mut Vec<Effect>,
    ) {
        // An ownership-only handoff claim lands `Dormant`: it takes no
        // residency slot, so place_unowned admits it above the cap and
        // the phase carries the exemption.
        let dormant = !cell.requests.is_empty()
            && cell
                .requests
                .iter()
                .all(|request| self.handoff_requests.contains(request));
        debug_assert!(self.has_capacity() || dormant);
        cell.waiting_for = None;
        let op = self.cell_op(id);
        match activation {
            Activation::Claim(claim) => {
                set_phase(
                    &mut self.occupied,
                    cell,
                    Phase::Acquiring {
                        op,
                        claim: claim.clone(),
                        dormant,
                    },
                );
                effects.push(Effect::CasOwner {
                    op,
                    cell: id.to_string(),
                    guard: claim.guard,
                    epoch: claim.epoch,
                    takeover: claim.takeover,
                });
            }
            Activation::Restore(spec) => {
                set_phase(
                    &mut self.occupied,
                    cell,
                    Phase::Restoring {
                        op,
                        spec: spec.clone(),
                    },
                );
                effects.push(Effect::Restore {
                    op,
                    cell: id.to_string(),
                    spec,
                });
            }
        }
    }

    fn activate_or_wait(
        &mut self,
        id: &str,
        cell: &mut Cell,
        activation: Activation,
        effects: &mut Vec<Effect>,
    ) {
        if cell.requests.is_empty() && !cell.alarm_wake && !cell.resume_demand && !cell.confirming {
            set_phase(
                &mut self.occupied,
                cell,
                match activation {
                    Activation::Claim(_) => Phase::Inactive,
                    Activation::Restore(spec) => Phase::Dormant { epoch: spec.epoch },
                },
            );
        } else if self.has_capacity() {
            self.begin_activation(id, cell, activation, effects);
        } else {
            set_phase(&mut self.occupied, cell, Phase::WaitingCapacity);
            cell.waiting_for = Some(activation);
            self.capacity_waiters.push_back(id.to_string());
            self.watch_queued(id, cell, effects);
            self.shed_one(effects);
        }
    }

    fn pump_capacity(&mut self, effects: &mut Vec<Effect>) {
        while self.has_capacity() {
            let Some(id) = self.capacity_waiters.pop_front() else {
                break;
            };
            let Some(mut cell) = self.cells.remove(&id) else {
                continue;
            };
            if cell.phase != Phase::WaitingCapacity {
                self.cells.insert(id, cell);
                continue;
            }
            let Some(activation) = cell.waiting_for.take() else {
                self.cells.insert(id, cell);
                continue;
            };
            if cell.requests.is_empty() && !cell.alarm_wake && !cell.resume_demand {
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    match activation {
                        Activation::Claim(_) => Phase::Inactive,
                        Activation::Restore(spec) => Phase::Dormant { epoch: spec.epoch },
                    },
                );
            } else {
                self.begin_activation(&id, &mut cell, activation, effects);
            }
            self.cells.insert(id, cell);
        }
        self.shed_one(effects);
    }

    fn start_node_lease(&mut self, now_ms: u64, spec: NodeLeaseSpec, effects: &mut Vec<Effect>) {
        if !matches!(
            self.node_authority,
            NodeAuthority::Unstarted | NodeAuthority::Failed
        ) {
            return;
        }
        self.begin_node_lease_acquisition(now_ms, spec, effects);
    }

    fn begin_node_lease_acquisition(
        &mut self,
        now_ms: u64,
        spec: NodeLeaseSpec,
        effects: &mut Vec<Effect>,
    ) {
        let desired = NodeLeaseRecord {
            node: self.node.clone(),
            addr: spec.addr.clone(),
            expires_ms: now_ms.saturating_add(spec.ttl_ms),
            peer_protocol: spec.peer_protocol,
            generation: spec.generation.clone(),
            // A fresh session has not opened a log; the predecessor's
            // state was recovered before this install (the engine's
            // startup order), so None is the truth, not a reset.
            log_state: None,
            etag: String::new(),
        };
        let pending = PendingNodeLease {
            spec,
            desired,
            prior: None,
            readback_only: false,
            anchor_mono_ms: self.now_mono_ms,
            resume_local: false,
        };
        let op = self.op();
        self.node_authority = NodeAuthority::Reading { op, pending };
        effects.push(Effect::ReadSelfNodeLease { op });
    }

    fn fail_initial_node_lease(&mut self, spec: NodeLeaseSpec, effects: &mut Vec<Effect>) {
        // Nothing else will ever ask: `StartNodeLease` is a one-shot at
        // process start. Arm the retry here or this node is done serving.
        // The renewal cadence is the right interval -- it is already the
        // rate this fleet has decided its bucket can carry.
        let generation = self.next_timer_generation;
        self.next_timer_generation = self
            .next_timer_generation
            .checked_add(1)
            .expect("timer generation exhausted");
        let retry_after = (spec.ttl_ms / 3).max(1);
        effects.push(Effect::ScheduleTimer {
            timer: Timer::NodeLeaseRenew { generation },
            at_mono_ms: self.now_mono_ms.saturating_add(retry_after),
        });
        self.node_authority = NodeAuthority::Retrying { spec, generation };
    }

    /// A lease write landed. Decide whether it actually preserved authority,
    /// and if so install it anchored to the instant its `expires_ms` was
    /// sampled.
    ///
    /// A renewal that lands at or after the PRIOR lease's expiry has not
    /// extended anything: every peer was already entitled to read that record,
    /// find it dead, and seize this node's cells, and one of them may have
    /// begun doing exactly that. Rewriting `nodes/<node>.json` does not
    /// retract a takeover already in flight -- the ownership compare-and-swap
    /// a peer is about to issue is guarded on the ownership record's ETag,
    /// which a resident owner never touches -- so resurrecting the lease here
    /// is precisely how two nodes end up serving one cell. The gap is a fence,
    /// not a hiccup.
    fn complete_node_lease_write(
        &mut self,
        pending: PendingNodeLease,
        record: NodeLeaseRecord,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        // The record just published expires at `anchor + ttl`. A round trip
        // that outlived that window landed a lease which is already dead to
        // every peer reading the bucket, so this process must not act on it.
        let expired = now_mono_ms.saturating_sub(pending.anchor_mono_ms) >= pending.spec.ttl_ms;
        match &pending.prior {
            // A renewal only preserves authority if it lands while the PRIOR
            // lease is still valid. Losing that continuity means peers were
            // already entitled to seize this node's cells.
            Some(prior) => {
                if expired || now_mono_ms.saturating_sub(prior.last_ok_mono_ms) >= prior.spec.ttl_ms
                {
                    self.fence_node(HaltReason::NodeLeaseExpired, effects);
                    return;
                }
            }
            // An initial acquisition that lands expired never became
            // authoritative: this node owns nothing and is serving nothing, so
            // there is no runtime to fence and no reason to halt. Fail the
            // waiters and let a later request retry from a fresh sample.
            None => {
                if expired {
                    self.fail_initial_node_lease(pending.spec, effects);
                    return;
                }
            }
        }
        let resume_local = pending.resume_local;
        // The first hold of a process that is not resuming a clean reload
        // starts the inventory confirmation; a renewal never does.
        let confirm_local = pending.prior.is_none() && !resume_local;
        self.hold_node_lease(
            pending.spec,
            record,
            pending.anchor_mono_ms,
            resume_local,
            confirm_local,
            effects,
        );
    }

    /// Install a held lease. `anchor_mono_ms` is the monotonic instant the
    /// record's `expires_ms` was computed from -- NOT the instant the write
    /// completed. See [`PendingNodeLease::anchor_mono_ms`].
    fn hold_node_lease(
        &mut self,
        spec: NodeLeaseSpec,
        record: NodeLeaseRecord,
        anchor_mono_ms: u64,
        resume_local: bool,
        confirm_local: bool,
        effects: &mut Vec<Effect>,
    ) {
        let generation = self.next_timer_generation;
        self.next_timer_generation = self
            .next_timer_generation
            .checked_add(1)
            .expect("timer generation exhausted");
        // A nudge that arrived mid-write could not ride the write that
        // just landed (its body predates the publish): renew again NOW.
        let renew_after = if std::mem::take(&mut self.nudge_pending) {
            1
        } else {
            (spec.ttl_ms / 3).max(1)
        };
        effects.push(Effect::ScheduleTimer {
            timer: Timer::NodeLeaseRenew { generation },
            at_mono_ms: anchor_mono_ms.saturating_add(renew_after),
        });
        // Exactly the published deadline. A peer treats the record as live
        // while `expires_ms > now_ms`, so this node must have stopped serving
        // by the time `now_ms` reaches `expires_ms` -- not one millisecond
        // after it.
        effects.push(Effect::ScheduleTimer {
            timer: Timer::NodeLeaseFence { generation },
            at_mono_ms: anchor_mono_ms.saturating_add(spec.ttl_ms),
        });
        self.node_authority = NodeAuthority::Held(HeldNodeLease {
            spec,
            record,
            last_ok_mono_ms: anchor_mono_ms,
            last_attempt_mono_ms: anchor_mono_ms,
            timer_generation: generation,
        });
        if resume_local {
            self.resuming = true;
            effects.push(Effect::ReadLocalCells);
        } else if confirm_local {
            self.confirming = true;
            effects.push(Effect::ReadLocalCells);
        }
    }

    fn resume_node_lease_after_failure(
        &mut self,
        prior: HeldNodeLease,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        // A publish that arrived mid-write must not wait a full ttl/3
        // behind a transient failure: the
        // OwnLog waiter's deadline is shorter than that.
        let retry_after = if std::mem::take(&mut self.nudge_pending) {
            1
        } else {
            // A failed CAS and its readback consume authority without moving
            // the published expiry. Waiting another full cadence can put the
            // retry at or beyond that expiry, so a recovered store gets no
            // further attempt and the node fences. Keep the normal cadence
            // only while it leaves at least half of the remaining window
            // for the retry's CAS and readback; the attempt that just
            // failed shows how long an attempt can take. Otherwise retry
            // after a quarter of the window, so a store that answers as
            // slowly as it just did still lands the retry before the
            // expiry. On 2026-09-03 a fleet kept the full cadence with
            // 4.4 s left after a 2.5 s ambiguous write and every node
            // fenced in the same second.
            let authority_deadline = prior.last_ok_mono_ms.saturating_add(prior.spec.ttl_ms);
            let remaining = authority_deadline.saturating_sub(now_mono_ms);
            let cadence = (prior.spec.ttl_ms / 3).max(1);
            if cadence <= remaining / 2 {
                cadence
            } else {
                (remaining / 4).max(1)
            }
        };
        effects.push(Effect::ScheduleTimer {
            timer: Timer::NodeLeaseRenew {
                generation: prior.timer_generation,
            },
            at_mono_ms: now_mono_ms.saturating_add(retry_after),
        });
        self.node_authority = NodeAuthority::Held(prior);
    }

    fn begin_node_lease_write(
        &mut self,
        pending: PendingNodeLease,
        guard: CasGuard,
        effects: &mut Vec<Effect>,
    ) {
        let op = self.op();
        let record = pending.desired.clone();
        let authority_expires_ms = pending.prior.as_ref().map(|prior| prior.record.expires_ms);
        self.node_authority = NodeAuthority::Writing { op, pending };
        effects.push(Effect::CasNodeLease {
            op,
            guard,
            record,
            authority_expires_ms,
        });
    }

    fn read_self_node_lease(
        &mut self,
        op: OpId,
        now_ms: u64,
        now_mono_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let authority = std::mem::replace(&mut self.node_authority, NodeAuthority::Failed);
        let NodeAuthority::Reading {
            op: current,
            pending,
        } = authority
        else {
            self.node_authority = authority;
            return;
        };
        if current != op {
            self.node_authority = NodeAuthority::Reading {
                op: current,
                pending,
            };
            return;
        }
        match result {
            Err(_) => {
                if let Some(prior) = pending.prior {
                    self.resume_node_lease_after_failure(prior, now_mono_ms, effects);
                } else {
                    self.fail_initial_node_lease(pending.spec, effects);
                }
            }
            Ok(Some(record)) if same_node_lease(&record, &pending.desired) => {
                self.complete_node_lease_write(pending, record, now_mono_ms, effects);
            }
            Ok(Some(record))
                if pending
                    .prior
                    .as_ref()
                    .is_some_and(|prior| same_node_lease(&record, &prior.record)) =>
            {
                let mut prior = pending.prior.expect("checked above");
                prior.record.etag = record.etag;
                self.resume_node_lease_after_failure(prior, now_mono_ms, effects);
            }
            Ok(Some(record))
                if pending.prior.as_ref().is_some_and(|prior| {
                    record.node == pending.desired.node
                        && record.addr == pending.desired.addr
                        && record.peer_protocol == pending.desired.peer_protocol
                        && record.generation == pending.desired.generation
                        && record.log_state == pending.desired.log_state
                        && record.expires_ms > prior.record.expires_ms
                        && record.expires_ms <= pending.desired.expires_ms
                }) =>
            {
                // Our own zombie: every identity field ours, including the
                // folded log state a peer's recovery stamp would have
                // changed, and only the expiry moved — no further than an
                // attempt of ours could have written. An earlier ambiguous
                // renewal landed after its readback missed it (#482). Adopt
                // the record. Bound the adopted monotonic anchor by both the
                // expiry delta and the expiry observed at this readback. A
                // wall-clock step between the write attempts is present in
                // one bound but not the other, so taking the earlier anchor
                // prevents that step from becoming local lease authority.
                let mut prior = pending.prior.expect("checked above");
                let delta = record.expires_ms.saturating_sub(prior.record.expires_ms);
                let delta_anchor = prior.last_ok_mono_ms.saturating_add(delta);
                let observed_remaining = record.expires_ms.saturating_sub(now_ms);
                if observed_remaining == 0 {
                    self.fence_node(HaltReason::NodeLeaseExpired, effects);
                    return;
                }
                // The anchor names the deadline as `anchor + ttl_ms`, so it
                // cannot name a translated expiry that falls less than one
                // TTL after the monotonic epoch. A large forward step early
                // in uptime produces exactly that. Saturating the anchor to
                // zero there would read as a full TTL of authority — past
                // the published deadline, and overlapping a successor — so
                // an unrepresentable deadline fences instead.
                let Some(observed_anchor) = now_mono_ms
                    .saturating_add(observed_remaining)
                    .checked_sub(prior.spec.ttl_ms)
                else {
                    self.fence_node(HaltReason::NodeLeaseExpired, effects);
                    return;
                };
                prior.last_ok_mono_ms = delta_anchor.min(observed_anchor);
                prior.record = record;
                // An adoption happens only after an ambiguous attempt, its
                // readback, a cadence wait, a rejection, and a second
                // readback have eaten most of the TTL, so the adopted
                // headroom is always thin. The 2026-08-28T07:43Z field
                // fence landed one millisecond past an adopted expiry with
                // the cadence-scheduled renewal 54 ms from completion.
                // Renew NOW, on the nudge lane.
                self.nudge_pending = true;
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::NodeLeaseFence {
                        generation: prior.timer_generation,
                    },
                    at_mono_ms: prior.last_ok_mono_ms.saturating_add(prior.spec.ttl_ms),
                });
                self.resume_node_lease_after_failure(prior, now_mono_ms, effects);
            }
            Ok(None) if pending.prior.is_some() => {
                // The record vanished after this process held it. Report the
                // observed loss instead of making the shell call it a TTL
                // expiry while published authority still had time remaining.
                self.fence_node(HaltReason::NodeLeaseMissing, effects);
            }
            Ok(Some(_)) if pending.prior.is_some() => {
                // Every record that this process can safely retain or adopt
                // matched an earlier arm, so authority is lost. The record is
                // NOT proof of a foreign writer: an ambiguous renewal of this
                // process can land late, carrying the folded log stamp it was
                // serialized with, after OwnLog published a newer one. The
                // zombie arm above compares that stamp, so it misses, and the
                // record arrives here self-authored. Report the observation.
                self.fence_node(HaltReason::NodeLeaseMismatch, effects);
            }
            Ok(_) if pending.readback_only => {
                // The ambiguous initial write did not publish the exact
                // desired generation. Fail this activation; a later request
                // may retry from a fresh read, but this one cannot spin.
                self.fail_initial_node_lease(pending.spec, effects);
            }
            Ok(Some(record)) => {
                // The record we are
                // about to replace carries the predecessor's folded log
                // state, and our install writes a fresh one — so a
                // not-sealed predecessor log here means recovery has NOT
                // run, and installing would erase the only evidence it
                // was needed. The engine's boot order makes this
                // unreachable; the guard keeps it structurally so.
                let predecessor_unrecovered = pending.prior.is_none()
                    && record.generation != pending.desired.generation
                    && !matches!(
                        record.log_state,
                        None | Some(crate::log_tier::LogState::Sealed)
                    );
                if predecessor_unrecovered {
                    self.fail_initial_node_lease(pending.spec, effects);
                    return;
                }
                // A configured node id is a singleton key: restarting that
                // node replaces its prior process generation immediately. The
                // ETag still serializes competing replacements, and a process
                // which loses that CAS never becomes authoritative.
                let mut pending = pending;
                pending.resume_local = pending.prior.is_none()
                    && !pending.readback_only
                    && record.expires_ms > now_ms
                    && pending.spec.resume_generation.as_deref()
                        == Some(record.generation.as_str());
                self.begin_node_lease_write(pending, CasGuard::Match(record.etag), effects);
            }
            Ok(None) => self.begin_node_lease_write(pending, CasGuard::Absent, effects),
        }
    }

    fn node_lease_cas_completed(
        &mut self,
        op: OpId,
        now_mono_ms: u64,
        result: Result<LeaseCasOutcome, Failure>,
        stamped_log_state: Option<crate::log_tier::LogState>,
        effects: &mut Vec<Effect>,
    ) {
        let authority = std::mem::replace(&mut self.node_authority, NodeAuthority::Failed);
        let NodeAuthority::Writing {
            op: current,
            pending,
        } = authority
        else {
            self.node_authority = authority;
            return;
        };
        if current != op {
            self.node_authority = NodeAuthority::Writing {
                op: current,
                pending,
            };
            return;
        }
        match result {
            Ok(LeaseCasOutcome::Applied { etag }) => {
                let mut record = pending.desired.clone();
                record.etag = etag;
                // The shell stamps the folded log into the body at
                // serialization, after the core chose `desired`: the held
                // record must track the body that actually landed, or the next ambiguous
                // readback compares the bucket's truth against a stale
                // belief and self-fences a healthy node.
                record.log_state = stamped_log_state;
                self.complete_node_lease_write(pending, record, now_mono_ms, effects);
            }
            Ok(LeaseCasOutcome::Rejected) => {
                // A rejected renewal is NOT proof of usurpation: an earlier
                // AMBIGUOUS renewal can land after its own readback missed
                // it, and then the ETag moved under our hand (#482 — R2
                // throttling manufactured exactly this, and fencing here
                // halted a healthy follower with 549ms of real authority
                // left). Read back and decide from the record; the readback
                // compares against what the shell actually serialized, for
                // the same reason the ambiguous arm does.
                let read = self.op();
                let mut pending = pending;
                pending.desired.log_state = stamped_log_state;
                self.node_authority = NodeAuthority::Reading { op: read, pending };
                effects.push(Effect::ReadSelfNodeLease { op: read });
            }
            Err(Failure::Ambiguous) => {
                let read = self.op();
                let mut pending = pending;
                pending.readback_only = true;
                // The readback compares against what the shell actually
                // serialized into this attempt, never the core's
                // pre-attempt belief.
                pending.desired.log_state = stamped_log_state;
                self.node_authority = NodeAuthority::Reading { op: read, pending };
                effects.push(Effect::ReadSelfNodeLease { op: read });
            }
            Err(Failure::Definite) => {
                if let Some(prior) = pending.prior {
                    self.resume_node_lease_after_failure(prior, now_mono_ms, effects);
                } else {
                    self.fail_initial_node_lease(pending.spec, effects);
                }
            }
        }
    }

    /// Arm a deadline for every activation effect this event produced.
    ///
    /// Done once, centrally, rather than at each site that emits one: every
    /// separate site is a chance to forget, and a forgotten deadline is
    /// invisible until something hangs. The effect list already names every
    /// operation the executor is about to start, so it is the natural place to
    /// decide which of them are worth watching.
    fn arm_operation_deadlines(&mut self, effects: &mut Vec<Effect>) {
        let Some(deadline_ms) = self.config.operation_deadline_ms else {
            return;
        };
        let watched: Vec<OpId> = effects
            .iter()
            .filter_map(|effect| match effect {
                // Every operation that holds something back. The activation
                // stages hold a caller; a durability proof holds an eviction;
                // a firing alarm holds the cell out of dormancy. All three
                // keep the ownership record claimed while they wait.
                //
                // `StopRuntime` is the deliberate exception. It has no failure
                // handling to reuse -- a stop cannot fail, it can only not
                // finish -- so abandoning one would mean declaring a runtime
                // gone while it may still be running. That needs a decision,
                // not a timer.
                // A restore is a sequence of individually bounded object-store
                // requests. Its total duration scales with the replica. The
                // shell cannot cancel the task, so retiring the core op would
                // discard late success and let another request duplicate it.
                Effect::Restore { .. } => None,
                Effect::ReadOwner { op, .. }
                | Effect::ReadNodeLease { op, .. }
                | Effect::RecoverNodeLog { op, .. }
                | Effect::ReadCapacityPeers { op, .. }
                | Effect::CasOwner { op, .. }
                | Effect::StartRuntime { op, .. }
                | Effect::Publish { op, .. }
                | Effect::EnsureDurable { op, .. }
                // A stuck owner write must retry while the drain is open.
                // A stuck successor request must release its cohort permit;
                // the owner record is already blank, so later demand can
                // discover whichever peer acquired it.
                | Effect::ReleaseOwner { op, .. }
                | Effect::AdoptReleased { op, .. }
                // A held write response: a swallowed durability proof must not
                // hang a client forever.
                | Effect::AwaitDurable { op, .. }
                | Effect::FireAlarm { op, .. } => Some(*op),
                _ => None,
            })
            .collect();
        let at_mono_ms = self.now_mono_ms.saturating_add(deadline_ms);
        for op in watched {
            effects.push(Effect::ScheduleTimer {
                timer: Timer::OperationDeadline { op },
                at_mono_ms,
            });
        }
    }

    /// An activation effect outlived its deadline.
    ///
    /// Expiry deliberately reuses each stage's own failure handling rather
    /// than introducing a second way to abandon work: the core already knows
    /// how to reconcile an ambiguous acquire and how to fail a read, and a
    /// deadline is only a different reason for reaching those paths.
    ///
    /// The classification is the whole substance. A read cannot have committed
    /// anything, so it is definite. Everything past it may have taken effect
    /// on the far side while the answer was lost, so it is ambiguous — the
    /// same distinction that decides whether a retry is safe. Calling a
    /// timed-out compare-and-swap definite would let a second attempt
    /// overwrite an epoch that had in fact been applied.
    fn expire_operation(&mut self, op: OpId, now_ms: u64, effects: &mut Vec<Effect>) {
        // A gated write is tracked in `barriers`, not the op index.
        // Ambiguous is the only safe class: the write may or may not be
        // durable, so the client must not be told it succeeded.
        if self.barriers.contains_key(&op) {
            // Source is irrelevant on a failed proof; Bucket is the
            // conservative label.
            self.durable_reached(op, Err(Failure::Ambiguous), ProofSource::Bucket, effects);
            return;
        }
        // A peek, not a take: each arm below re-enters the op's own
        // completion handler, and that handler consumes the index entry.
        let Some(id) = self.cell_ops.get(&op).cloned() else {
            // Already answered, superseded, or fenced. A stale deadline has
            // nothing to expire, which is the ordinary case.
            return;
        };
        let Some(cell) = self.cells.get(&id) else {
            return;
        };
        // A release can have committed before its response disappeared. The
        // ordinary failure path is safe in both cases: a retry either blanks
        // our still-owned record or reads the already-blank record and
        // settles as rejected.
        if cell.releasing == Some(op) {
            self.owner_released(op, Err(Failure::Definite), effects);
            return;
        }
        // Ownership is already blank before adoption starts. Retiring a stuck
        // acknowledgement cannot restore this donor's authority. Future
        // demand reads the record and claims or follows the real owner.
        if matches!(cell.phase, Phase::Adopting { op: current, .. } if current == op) {
            self.successor_adopted(op, Err(Failure::Definite), effects);
            return;
        }
        // A firing alarm is tracked on the cell rather than in its phase, so
        // it is looked for first. Expiry re-arms it exactly as a failed
        // handler would, which keeps alarms at-least-once instead of turning
        // a stuck handler into a lost one.
        if matches!(cell.alarm, Some(AlarmState::Firing { op: current, .. }) if current == op) {
            // The cell comes from the index here, unlike the event, which
            // carries its own: an expiry that cannot find the op has nothing
            // to time out, so depending on the index is safe, while the event
            // cannot depend on it because a fold may retire the firing state
            // while the write it reports is still unproven. The epoch is read
            // for completeness and never used — a failure opens no gate, and
            // the gate is the only thing `alarm_finished` reads it for.
            let epoch = match cell.phase {
                Phase::Resident { epoch } => epoch,
                _ => 0,
            };
            let (id, now_mono_ms) = (id.clone(), self.now_mono_ms);
            self.alarm_finished(
                op,
                (id, epoch),
                now_ms,
                now_mono_ms,
                Err(Failure::Ambiguous),
                effects,
            );
            return;
        }
        if phase_op(&cell.phase) != Some(op) {
            return;
        }
        let phase = Some(cell.phase.clone());
        match phase {
            Some(Phase::ReadingOwner { .. }) => {
                self.owner_read(op, 0, Err(Failure::Definite), effects)
            }
            Some(Phase::ReadingNodeLease { .. }) => {
                self.node_lease_read(op, now_ms, Err(Failure::Definite), effects)
            }
            // Recovery can upload data or seal the remote session before its
            // completion is lost, so a timeout cannot prove that nothing
            // changed on the far side.
            Some(Phase::RecoveringOwnerLog { .. }) => {
                self.node_log_recovered(op, Err(Failure::Ambiguous), effects)
            }
            Some(Phase::ReadingCapacity { .. }) => {
                self.capacity_peers_read(op, now_ms, Err(Failure::Definite), effects)
            }
            Some(Phase::Acquiring { .. }) => {
                self.owner_cas_completed(op, Err(Failure::Ambiguous), effects)
            }
            // ReconcilingAcquire waits on the read that follows an ambiguous
            // CAS. The CAS handler accepts only Acquiring, so dispatching the
            // deadline there leaves the read operation and request attached.
            Some(Phase::ReconcilingAcquire { .. }) => {
                self.owner_read(op, now_ms, Err(Failure::Definite), effects)
            }
            // A restore has no aggregate deadline. Each object-store request
            // is bounded in the shell, and the complete task must retain its
            // op so a late success cannot be discarded and duplicated.
            Some(Phase::Restoring { .. }) => {}
            Some(Phase::Starting { .. }) => self.runtime_start_timed_out(op, effects),
            Some(Phase::Publishing { .. }) => self.published(op, Err(Failure::Ambiguous), effects),
            // An unprovable snapshot leaves the cell resident. Evicting on a
            // proof that never arrived is the one outcome that loses data.
            Some(Phase::EnsuringDurability { .. }) => {
                self.durability_checked(op, Err(Failure::Ambiguous), effects)
            }
            _ => {}
        }
    }

    fn timer_fired(
        &mut self,
        timer: Timer,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        let timer = match timer {
            Timer::CellAlarm { cell, generation } => {
                self.cell_alarm_timer(&cell, generation, now_ms, now_mono_ms, effects);
                return;
            }
            // An activation deadline is not conditional on node authority: a
            // request stalled behind a swallowed effect must be released
            // whatever the lease is doing.
            Timer::OperationDeadline { op } => {
                self.expire_operation(op, now_ms, effects);
                return;
            }
            // Also unconditional on node authority: a cell parked behind the
            // gate is waiting on this node's own capacity, not on its lease.
            Timer::QueuedActivation { cell, generation } => {
                self.expire_queued(&cell, generation, effects);
                self.pump_activations(effects);
                self.pump_capacity(effects);
                return;
            }
            // Unconditional as well: the retry re-enters resolution, and
            // resolution itself decides what the lease permits.
            Timer::OwnerLogRecoveryRetry { cell, generation } => {
                self.retry_owner_log_recovery(&cell, generation, effects);
                return;
            }
            timer => timer,
        };
        // A retry has no held lease to validate against, so it is resolved
        // before the authority lookup below discards it.
        if let NodeAuthority::Retrying { spec, generation } = &self.node_authority {
            if matches!(timer, Timer::NodeLeaseRenew { generation: fired } if fired == *generation)
            {
                let spec = spec.clone();
                self.begin_node_lease_acquisition(now_ms, spec, effects);
            }
            return;
        }
        let active = match &self.node_authority {
            NodeAuthority::Held(held) => Some(held.clone()),
            NodeAuthority::Reading { pending, .. } | NodeAuthority::Writing { pending, .. } => {
                pending.prior.clone()
            }
            _ => None,
        };
        let Some(held) = active else {
            return;
        };
        // A VM suspension can advance wall time while its monotonic clock is
        // stopped. The published wall deadline and the local monotonic
        // deadline are independent upper bounds, so reaching either one ends
        // authority. Checking only monotonic elapsed time lets a resumed node
        // retain or renew a record that every peer already considers dead.
        let lease_expired = now_ms >= held.record.expires_ms
            || now_mono_ms.saturating_sub(held.last_ok_mono_ms) >= held.spec.ttl_ms;
        match timer {
            // `>=`, not `>`: a peer stops treating the record as live the
            // instant `now_ms` reaches `expires_ms`, so authority must end
            // there too. Serving through that one millisecond overlaps a
            // takeover that is already entitled to proceed.
            Timer::NodeLeaseFence { generation }
                if generation == held.timer_generation && lease_expired =>
            {
                self.fence_node(HaltReason::NodeLeaseExpired, effects);
            }
            // A renew timer that arrives after the lease it belongs to has
            // already expired is not a renewal opportunity; authority is
            // already gone. Fence rather than issue a write that could
            // resurrect it behind a peer's in-flight takeover.
            Timer::NodeLeaseRenew { generation }
                if generation == held.timer_generation && lease_expired =>
            {
                self.fence_node(HaltReason::NodeLeaseExpired, effects);
            }
            Timer::NodeLeaseRenew { generation }
                if generation == held.timer_generation
                    && matches!(self.node_authority, NodeAuthority::Held(_)) =>
            {
                let NodeAuthority::Held(mut prior) =
                    std::mem::replace(&mut self.node_authority, NodeAuthority::Failed)
                else {
                    unreachable!("held checked above")
                };
                prior.last_attempt_mono_ms = now_mono_ms;
                let spec = prior.spec.clone();
                let desired = NodeLeaseRecord {
                    node: self.node.clone(),
                    addr: spec.addr.clone(),
                    expires_ms: now_ms.saturating_add(spec.ttl_ms),
                    peer_protocol: spec.peer_protocol,
                    generation: spec.generation.clone(),
                    // The write discipline the fold's safety rests on
                    // (BrokenLeaseRenewalDropsLog is the spec tooth): a
                    // renewal carries the folded log state through
                    // unchanged.
                    log_state: prior.record.log_state,
                    etag: String::new(),
                };
                let guard = CasGuard::Match(prior.record.etag.clone());
                self.begin_node_lease_write(
                    PendingNodeLease {
                        spec,
                        desired,
                        prior: Some(prior),
                        readback_only: false,
                        anchor_mono_ms: now_mono_ms,
                        resume_local: false,
                    },
                    guard,
                    effects,
                );
            }
            Timer::CellAlarm { .. } => unreachable!("cell timers returned above"),
            _ => {}
        }
    }

    fn schedule_alarm_timer(
        &self,
        cell: &str,
        generation: u64,
        at_ms: i64,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        let at_ms = u64::try_from(at_ms).unwrap_or(0);
        effects.push(Effect::ScheduleTimer {
            timer: Timer::CellAlarm {
                cell: cell.to_string(),
                generation,
            },
            at_mono_ms: now_mono_ms.saturating_add(at_ms.saturating_sub(now_ms)),
        });
    }

    fn alarm_observed(
        &mut self,
        id: &str,
        observed: AlarmObservation,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        let AlarmObservation {
            at_ms,
            covered,
            firing_settled,
        } = observed;
        let Some(mut cell) = self.cells.remove(id) else {
            return;
        };
        if matches!(cell.phase, Phase::Fenced) {
            self.cells.insert(id.to_string(), cell);
            return;
        }
        // A failed output proof has already chosen the durable recovery
        // state and is stopping the unproven runtime. Its alarm cache can
        // still report the locally consumed row while the stop is in flight.
        // That observation belongs to the runtime being discarded, so it
        // cannot replace the re-armed alarm or delete its wake entry.
        if matches!(
            cell.phase,
            Phase::Cleaning {
                cause: StopCause::Reset,
                ..
            }
        ) {
            self.cells.insert(id.to_string(), cell);
            return;
        }
        if let Phase::EnsuringDurability { epoch, .. } = cell.phase {
            // An ordinary alarm change rescues a cell from eviction. A
            // handoff already stopped new routes and deliberately refreshed
            // this observation after nomination. A covered alarm (or no
            // alarm) is therefore confirmation, not new demand. Cancelling
            // here re-emits EnsureDurable forever while the cell has no pin.
            let safe_drain_refresh = self.draining && (at_ms.is_none() || covered);
            if !safe_drain_refresh {
                self.cancel_eviction(id, &mut cell, epoch);
            }
        }
        // Runtime observations can arrive before `AlarmFinished`. The alarm
        // handler removes its row before its consuming write is durable, so a
        // direct `None` must not replace `Firing` and delete the wake entry.
        // Stage a real move for the completion path. The completion carries
        // the durability verdict that decides whether the observation is safe
        // to apply. An unchanged report only refreshes wake coverage unless
        // it supersedes an earlier staged move.
        if !firing_settled {
            if let Some(AlarmState::Firing {
                at_ms: firing_at_ms,
                covered: firing_covered,
                observed,
                ..
            }) = &mut cell.alarm
            {
                if at_ms == Some(*firing_at_ms) && observed.is_none() {
                    *firing_covered = covered;
                } else {
                    *observed = Some((at_ms, covered));
                }
                self.cells.insert(id.to_string(), cell);
                return;
            }
        }
        let observed_before = match cell.alarm {
            Some(AlarmState::Armed { at_ms, .. }) | Some(AlarmState::Firing { at_ms, .. }) => at_ms,
            None => -1,
        };
        // The assignment below replaces a firing alarm unconditionally. Its
        // op entry is usually consumed already (`alarm_finished` routes
        // through here), but a direct observation can land mid-fire, and the
        // replaced op will then never match a lookup again.
        if let Some(AlarmState::Firing { op, .. }) = cell.alarm {
            self.cell_ops.remove(&op);
        }
        cell.alarm = at_ms.filter(|at_ms| *at_ms >= 0).map(|at_ms| {
            let generation = self.next_timer_generation;
            self.next_timer_generation = self
                .next_timer_generation
                .checked_add(1)
                .expect("timer generation exhausted");
            self.schedule_alarm_timer(id, generation, at_ms, now_ms, now_mono_ms, effects);
            AlarmState::Armed {
                at_ms,
                generation,
                covered,
            }
        });
        if cell.alarm.is_none() {
            cell.alarm_wake = false;
        }
        // Both ways an alarm settles arrive here -- a request that changed it
        // and a firing that consumed it (`alarm_finished` routes through this
        // function). Saying so once, here, is what keeps the bucket entry
        // from depending on the shell noticing each path separately. Only on
        // a change: an activity that left the alarm alone has nothing to
        // mirror, and re-stating it would put a bucket round trip on the end
        // of every request.
        let settled = at_ms.filter(|at_ms| *at_ms >= 0).unwrap_or(-1);
        if settled != observed_before {
            effects.push(Effect::ReconcileWakeEntry {
                cell: id.to_string(),
                next_alarm_ms: settled,
            });
        }
        self.cells.insert(id.to_string(), cell);
    }

    fn cell_alarm_timer(
        &mut self,
        id: &str,
        generation: u64,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        if !self.node_authoritative() {
            return;
        }
        let Some(mut cell) = self.cells.remove(id) else {
            return;
        };
        let Some(AlarmState::Armed {
            at_ms,
            generation: current,
            covered,
        }) = cell.alarm
        else {
            self.cells.insert(id.to_string(), cell);
            return;
        };
        if current != generation {
            self.cells.insert(id.to_string(), cell);
            return;
        }
        if i64::try_from(now_ms).unwrap_or(i64::MAX) < at_ms {
            self.schedule_alarm_timer(id, generation, at_ms, now_ms, now_mono_ms, effects);
            self.cells.insert(id.to_string(), cell);
            return;
        }
        let epoch = match cell.phase {
            Phase::Resident { epoch } => epoch,
            Phase::EnsuringDurability { epoch, .. } => {
                self.cancel_eviction(id, &mut cell, epoch);
                epoch
            }
            Phase::Fenced => {
                cell.alarm = None;
                self.cells.insert(id.to_string(), cell);
                return;
            }
            Phase::Dormant { .. } => {
                cell.alarm_wake = true;
                // Every activation takes a fresh epoch. The LTX metadata does
                // not survive an eviction, so a same-epoch wake would restart
                // at TXID 1 inside a populated prefix and mix two writer
                // lineages (CelldWriterGeneration.tla, #158). The CAS also
                // settles the older resume-vs-release race deterministically.
                self.admit_or_queue_activation(id, &mut cell, ColdStart::ReadOwner, effects);
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
                self.cells.insert(id.to_string(), cell);
                return;
            }
            Phase::Inactive => {
                cell.alarm_wake = true;
                self.admit_or_queue_activation(id, &mut cell, ColdStart::ReadOwner, effects);
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
                self.cells.insert(id.to_string(), cell);
                return;
            }
            _ => {
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
                self.cells.insert(id.to_string(), cell);
                return;
            }
        };
        let op = self.cell_op(id);
        cell.alarm = Some(AlarmState::Firing {
            op,
            at_ms,
            generation,
            covered,
            observed: None,
        });
        cell.alarm_wake = false;
        self.cells.insert(id.to_string(), cell);
        effects.push(Effect::FireAlarm {
            op,
            cell: id.to_string(),
            epoch,
            scheduled_ms: at_ms,
        });
    }

    /// Settle a firing alarm, once the commit that consumed it is durable.
    ///
    /// The consuming commit is a write like any other, so it takes the same
    /// output gate a request's write takes. Holding the alarm behind that gate
    /// is what lets the core order the wake-entry delete safely: the delete
    /// leaves only from the far side of a proven `DurableReached`, and every
    /// response that can reveal the same commit trails the same gate through
    /// `read_output`. The gate is keyed by the cell and epoch the firing was
    /// dispatched against, because an activity fold can supersede the firing
    /// state while the write is still unproven.
    ///
    /// The gate calls back here with `position: None` once it settles, so the
    /// second pass runs the ordinary settlement below and cannot re-gate.
    ///
    /// `(fired, epoch)` is where the firing was dispatched, carried as a pair
    /// because neither half means anything without the other.
    fn alarm_finished(
        &mut self,
        op: OpId,
        (fired, epoch): (CellId, Epoch),
        now_ms: u64,
        now_mono_ms: u64,
        result: Result<(Option<i64>, bool, Option<u64>), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        if let Ok((at_ms, covered, Some(position))) = result {
            if matches!(
                self.cells.get(&fired).map(|cell| &cell.phase),
                Some(Phase::Resident { epoch: current }) if *current == epoch
            ) {
                let gate = self.op();
                self.barriers.insert(
                    gate,
                    Barrier {
                        owner: GateOwner::Alarm {
                            alarm: op,
                            at_ms,
                            covered,
                        },
                        cell: fired.clone(),
                        epoch,
                        position,
                        followers: Vec::new(),
                    },
                );
                effects.push(Effect::AwaitDurable {
                    op: gate,
                    cell: fired,
                    epoch,
                    position,
                });
                return;
            }
            // The cell no longer runs the epoch this write belongs to, so the
            // reset or fence that took it has already refused to acknowledge
            // the write. Settle the alarm below; a superseded op falls out of
            // `take_cell_op`.
        }
        let result = result.map(|(at_ms, covered, _)| (at_ms, covered));
        let Some(id) = self.take_cell_op(op, |cell| {
            matches!(cell.alarm, Some(AlarmState::Firing { op: current, .. }) if current == op)
        }) else {
            return;
        };
        let Some(mut cell) = self.cells.remove(&id) else {
            return;
        };
        let Some(AlarmState::Firing {
            at_ms,
            generation,
            covered,
            observed,
            ..
        }) = cell.alarm
        else {
            self.cells.insert(id, cell);
            return;
        };
        match result {
            Ok((at_ms, covered)) => {
                // Leave the fired alarm in place for `alarm_observed` to
                // replace: it assigns unconditionally, and it compares against
                // what was there to decide whether the bucket entry has to
                // follow. Clearing it here first makes a consumed alarm look
                // like it was never armed, and the entry is never deleted.
                self.cells.insert(id.clone(), cell);
                let (at_ms, covered) = observed.unwrap_or((at_ms, covered));
                self.alarm_observed(
                    &id,
                    AlarmObservation {
                        at_ms,
                        covered,
                        firing_settled: true,
                    },
                    now_ms,
                    now_mono_ms,
                    effects,
                );
            }
            Err(_) => {
                cell.alarm = Some(AlarmState::Armed {
                    at_ms,
                    generation,
                    covered,
                });
                self.cells.insert(id.clone(), cell);
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id,
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(500),
                });
            }
        }
    }

    fn fence_node(&mut self, reason: HaltReason, effects: &mut Vec<Effect>) {
        self.node_authority = NodeAuthority::Fenced;
        self.fence(effects);
        effects.push(Effect::Halt { code: 3, reason });
    }

    fn request(
        &mut self,
        request: RequestId,
        id: CellId,
        capacity_handoff: bool,
        ownership_handoff: bool,
        effects: &mut Vec<Effect>,
    ) {
        if self.request_cells.contains_key(&request) {
            return;
        }
        if self.fenced {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeFenced),
            });
            return;
        }
        if capacity_handoff {
            self.capacity_requests.insert(request);
        }
        if ownership_handoff {
            self.handoff_requests.insert(request);
        }
        if self.node_authoritative() {
            self.request_authorized(request, id, effects);
            return;
        }
        effects.push(Effect::Complete {
            request,
            result: Err(RequestError::NodeUnavailable),
        });
    }

    /// Route one event from an already-registered WebSocket. A regular or
    /// outbound socket keeps its resident runtime alive, so a shutdown drain
    /// must let that socket finish its message and close handlers locally.
    /// Ordinary requests still wait for the successor once the cell quiesces.
    fn websocket_request(
        &mut self,
        request: RequestId,
        id: CellId,
        websocket: WebSocketId,
        effects: &mut Vec<Effect>,
    ) {
        let held_resident = self.node_authoritative()
            && self.cells.get(&id).is_some_and(|cell| {
                matches!(cell.phase, Phase::Resident { .. })
                    && cell.websockets.contains_key(&websocket)
            });
        if held_resident {
            self.complete_request(&id, request, Ok(Route::Local), effects);
        } else {
            // A hibernatable socket can outlive residency. It follows the
            // ordinary route so it can reactivate here or reach a successor.
            self.request(request, id, false, false, effects);
        }
    }

    fn request_authorized(&mut self, request: RequestId, id: CellId, effects: &mut Vec<Effect>) {
        let mut cell = self.cells.remove(&id).unwrap_or_default();
        match &cell.phase {
            Phase::Resident { .. } if !cell.quiescing => {
                Self::record_local_request(&mut cell, request, self.now_mono_ms);
                self.complete_request(&id, request, Ok(Route::Local), effects)
            }
            Phase::EnsuringDurability { epoch, .. } if !self.draining => {
                // The runtime is still published, so a new request wins the
                // race with voluntary eviction. Retiring the operation makes
                // its eventual durability completion harmless. The permit
                // taken at nomination comes back with the rescue: leaked, it
                // counts against `max_evictions` forever and eventually
                // stands every future eviction down.
                let epoch = *epoch;
                self.cancel_eviction(&id, &mut cell, epoch);
                Self::record_local_request(&mut cell, request, self.now_mono_ms);
                self.complete_request(&id, request, Ok(Route::Local), effects);
            }
            Phase::Remote {
                node,
                addr,
                epoch,
                peer_protocol,
                ..
            } => effects.push(Effect::Complete {
                request,
                result: Ok(Route::Remote {
                    node: node.clone(),
                    addr: addr.clone(),
                    epoch: *epoch,
                    peer_protocol: *peer_protocol,
                }),
            }),
            Phase::Fenced => effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeFenced),
            }),
            Phase::Starting { op, .. } if self.timed_out_runtime_starts.contains(op) => effects
                .push(Effect::Complete {
                    request,
                    result: Err(RequestError::RuntimeFailed),
                }),
            Phase::Inactive if self.draining => effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeUnavailable),
            }),
            Phase::Dormant { .. } if self.handoff_requests.contains(&request) => {
                // The ownership-only handoff is complete already. Keep the
                // synthetic request pending just long enough for the executor
                // to observe `accepted_epoch`; its cancellation removes it.
                cell.requests.insert(request);
                self.request_cells.insert(request, id.clone());
            }
            phase => {
                cell.requests.insert(request);
                self.request_cells.insert(request, id.clone());
                match phase {
                    Phase::Inactive => {
                        self.admit_or_queue_activation(
                            &id,
                            &mut cell,
                            ColdStart::ReadOwner,
                            effects,
                        );
                    }
                    Phase::Dormant { .. } if !self.hands_off(&cell) => {
                        // A wake always claims a fresh epoch. The preserved
                        // snapshot remains reusable as the previous epoch's
                        // baseline after the claim succeeds.
                        self.admit_or_queue_activation(
                            &id,
                            &mut cell,
                            ColdStart::ReadOwner,
                            effects,
                        );
                    }
                    _ => {}
                }
            }
        }
        self.cells.insert(id, cell);
    }

    fn wake_hint(
        &mut self,
        id: CellId,
        entry_ms: i64,
        scope: WakeHintScope,
        effects: &mut Vec<Effect>,
    ) {
        if self.fenced {
            return;
        }
        if self.node_authoritative() {
            self.wake_hint_authorized(id, entry_ms, scope, effects);
        }
    }

    fn wake_hint_authorized(
        &mut self,
        id: CellId,
        entry_ms: i64,
        scope: WakeHintScope,
        effects: &mut Vec<Effect>,
    ) {
        if self.draining {
            return;
        }
        // An `Owned` hint is decided before the map is touched.
        // `unwrap_or_default` below creates an `Inactive` entry for a cell
        // this node has never seen, which is what a `Fleet` probe needs and
        // exactly wrong here: a non-elected node sees the whole fleet's due
        // set on every tick, and each entry would leave a cell behind.
        if scope == WakeHintScope::Owned
            && !matches!(
                self.cells.get(&id).map(|cell| &cell.phase),
                Some(Phase::Dormant { .. })
            )
        {
            return;
        }
        let mut cell = self.cells.remove(&id).unwrap_or_default();
        cell.alarm_wake = true;
        match cell.phase {
            Phase::Inactive | Phase::Remote { .. } | Phase::Dormant { .. } => {
                // Adopt before the activation is admitted: the activation
                // restores the alarm, and the restore-time latch adopts only
                // when nothing is tracked yet. The entry's own minute must
                // win that race, because a postponed alarm keeps its
                // stale-early entry, and adopting the restored due time
                // instead would track a key that does not exist and leave
                // the real entry to wake the cell on every later tick.
                effects.push(Effect::AdoptWakeEntry {
                    cell: id.clone(),
                    entry_ms,
                });
                self.admit_or_queue_activation(&id, &mut cell, ColdStart::ReadOwner, effects);
            }
            _ => {}
        }
        self.cells.insert(id, cell);
    }

    fn cancel(&mut self, request: RequestId, effects: &mut Vec<Effect>) {
        // A cancelled route can already be Local-complete: the reply raced
        // the caller's death and was never consumed, so no activity guard
        // exists and no `ActivityFinished` will ever arrive. Release the
        // activity here, or the leaked entry pins its cell forever and a
        // later quiesce can never reach eviction. Every consumed route
        // disarms its cancel guard before this event can be sent, and a
        // guard that did report finished has already left `active_requests`,
        // so this release cannot double-fire.
        if self.active_requests.contains_key(&request) {
            self.activity_finished(request, effects);
        }
        self.capacity_requests.remove(&request);
        self.handoff_requests.remove(&request);
        let Some(id) = self.request_cells.remove(&request) else {
            return;
        };
        let Some(cell) = self.cells.get_mut(&id) else {
            return;
        };
        cell.requests.remove(&request);
        // A clean-generation resume is node demand, not caller demand. If a
        // joined caller cancels, keep that resume queued so readiness can
        // still observe its terminal outcome.
        if cell.requests.is_empty()
            && !cell.alarm_wake
            && !cell.resume_demand
            && cell.phase == Phase::WaitingActivation
        {
            let start = cell.waiting_activation.take();
            set_phase(
                &mut self.occupied,
                cell,
                match start {
                    Some(ColdStart::Restore(spec)) => Phase::Dormant { epoch: spec.epoch },
                    _ => Phase::Inactive,
                },
            );
            self.activation_waiters.retain(|queued| queued != &id);
        } else if cell.requests.is_empty()
            && !cell.alarm_wake
            && !cell.resume_demand
            && cell.phase == Phase::WaitingCapacity
        {
            let activation = cell.waiting_for.take();
            set_phase(
                &mut self.occupied,
                cell,
                match activation {
                    Some(Activation::Restore(spec)) => Phase::Dormant { epoch: spec.epoch },
                    _ => Phase::Inactive,
                },
            );
            self.capacity_waiters.retain(|queued| queued != &id);
        }
    }

    /// Retire cold work that cannot contribute to a clean local reload.
    ///
    /// Published runtimes remain untouched. The shell drains their active
    /// turns before it snapshots the resident inventory. Cold restore and
    /// recovery phases have no runtime to preserve, and they can legitimately
    /// outlive every caller, so waiting for it would make one cold cell disable
    /// local reuse for the complete node. Retire those phases and fail their
    /// callers. A later completion is ignored because its operation is no
    /// longer current.
    fn begin_preserve(&mut self, effects: &mut Vec<Effect>) {
        self.activation_waiters.clear();
        self.capacity_waiters.clear();

        let ids: Vec<CellId> = self.cells.keys().cloned().collect();
        for id in ids {
            let mut cell = self.cells.remove(&id).expect("id came from map");
            let fallback = match &cell.phase {
                Phase::WaitingActivation => match cell.waiting_activation.as_ref() {
                    Some(ColdStart::Restore(spec)) => Some(Phase::Dormant { epoch: spec.epoch }),
                    _ => Some(Phase::Inactive),
                },
                Phase::ReadingOwner { .. }
                | Phase::ReadingNodeLease { .. }
                | Phase::RecoveringOwnerLog { .. }
                | Phase::ReadingCapacity { .. }
                | Phase::Acquiring { .. }
                | Phase::ReconcilingAcquire { .. } => Some(Phase::Inactive),
                Phase::WaitingCapacity => match cell.waiting_for.as_ref() {
                    Some(Activation::Restore(spec)) => Some(Phase::Dormant { epoch: spec.epoch }),
                    _ => Some(Phase::Inactive),
                },
                Phase::Restoring { spec, .. } => Some(Phase::Dormant { epoch: spec.epoch }),
                _ => None,
            };
            if let Some(fallback) = fallback {
                if let Some(stale) = phase_op(&cell.phase) {
                    self.cell_ops.remove(&stale);
                }
                self.finish_requests(&id, &mut cell, Err(RequestError::NodeFenced), effects);
                set_phase(&mut self.occupied, &mut cell, fallback);
                cell.waiting_for = None;
                cell.waiting_activation = None;
                cell.alarm_wake = false;
                cell.resume_demand = false;
                self.resuming_cells.remove(&id);
            }
            self.cells.insert(id, cell);
        }
        if self.resuming_cells.is_empty() {
            self.resuming = false;
        }
        self.activation_permits.retain(|id| {
            self.cells
                .get(id)
                .is_some_and(|cell| phase_holds_activation(&cell.phase))
        });
        self.capacity_requests
            .retain(|request| self.request_cells.contains_key(request));
        self.handoff_requests
            .retain(|request| self.request_cells.contains_key(request));
    }

    fn begin_capacity_lookup(
        &mut self,
        id: &str,
        cell: &mut Cell,
        claim: Claim,
        effects: &mut Vec<Effect>,
    ) {
        let op = self.cell_op(id);
        set_phase(
            &mut self.occupied,
            cell,
            Phase::ReadingCapacity {
                op,
                claim: claim.clone(),
            },
        );
        effects.push(Effect::ReadCapacityPeers {
            op,
            cell: id.to_string(),
        });
    }

    /// Place a genuinely unowned cell. Ordinary ingress may look for fleet
    /// capacity before waiting locally; a capacity handoff must either reserve
    /// a real local slot now or explicitly refuse so its caller can traverse.
    fn place_unowned(
        &mut self,
        id: &str,
        cell: &mut Cell,
        claim: Claim,
        effects: &mut Vec<Effect>,
    ) {
        if self.has_capacity() {
            self.activate_or_wait(id, cell, Activation::Claim(claim), effects);
            return;
        }

        // An ownership-only handoff parks dormant after its claim and
        // holds no residency slot, so the cap is no reason to refuse it.
        // Refusing here starved a whole drain when every peer sat at its
        // cap: each stuck cell held a donor release permit, the donor
        // retried forever, its no-progress deadline fired, and SIGKILL
        // left the fleet's records naming a dead node. The claim's
        // Acquiring phase carries a dormant mark and occupies no
        // residency slot, so the ceiling
        // invariant holds throughout. A cell that live demand also
        // reached still refuses below, because its claim would restore a
        // runtime this node has no slot for.
        if !cell.requests.is_empty()
            && cell
                .requests
                .iter()
                .all(|request| self.handoff_requests.contains(request))
        {
            self.begin_activation(id, cell, Activation::Claim(claim), effects);
            return;
        }

        let handoffs: Vec<RequestId> = cell
            .requests
            .iter()
            .copied()
            .filter(|request| self.capacity_requests.contains(request))
            .collect();
        for request in handoffs {
            cell.requests.remove(&request);
            self.request_cells.remove(&request);
            self.complete_request(id, request, Err(RequestError::CapacityExhausted), effects);
        }

        if cell.requests.is_empty() || !self.config.require_node_lease {
            // Alarm wakes cannot be proxied as an HTTP capacity handoff. They
            // retain the old local wait semantics until alarm dispatch itself
            // has a fleet transport effect. Lease-disabled mode likewise has
            // no authoritative fleet membership to enumerate.
            self.activate_or_wait(id, cell, Activation::Claim(claim), effects);
        } else {
            self.begin_capacity_lookup(id, cell, claim, effects);
        }
    }

    fn owner_read(
        &mut self,
        op: OpId,
        now_ms: u64,
        result: Result<Option<OwnerRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(op, |cell| {
            matches!(
                cell.phase,
                Phase::ReadingOwner { op: current }
                    | Phase::ReconcilingAcquire { op: current, .. }
                    if current == op
            )
        }) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        if std::mem::take(&mut cell.confirming) {
            self.settle_confirmation(&id);
            if cell.requests.is_empty() && !cell.alarm_wake {
                // Nobody wants the cell: record what the store says and
                // stop. A record that still names this node leaves the cell
                // dormant at that epoch, exactly as an idle eviction does,
                // and the next request wakes it through the usual claim.
                let phase = match result {
                    Ok(Some(record)) if record.node.as_deref() == Some(self.node.as_str()) => {
                        Phase::Dormant {
                            epoch: record.epoch,
                        }
                    }
                    _ => Phase::Inactive,
                };
                set_phase(&mut self.occupied, &mut cell, phase);
                self.cells.insert(id, cell);
                return;
            }
        }
        let reconciling_claim = match &cell.phase {
            Phase::ReconcilingAcquire { claim, .. } => Some(claim.clone()),
            _ => None,
        };
        // A reconcile re-reads and then acquires again: one attempt
        // continuing, not a new one. The count has to survive the round trip
        // or the bound below can never be reached.
        let reconciles = reconciling_claim
            .as_ref()
            .map_or(0, |claim| claim.reconciles);
        match result {
            Ok(Some(record))
                if reconciling_claim.as_ref().is_some_and(|claim| {
                    record.node.as_deref() == Some(self.node.as_str())
                        && record.epoch == claim.epoch
                }) =>
            {
                let spec = RestoreSpec {
                    epoch: record.epoch,
                    fresh: matches!(
                        reconciling_claim.as_ref().map(|claim| &claim.guard),
                        Some(CasGuard::Absent)
                    ),
                    took_over: reconciling_claim
                        .as_ref()
                        .is_some_and(|claim| claim.takeover),
                    resume_local: false,
                    // The ambiguous acquire applied after all, so the prior
                    // owner it displaced is the one the claim carried — the
                    // takeover interlock must see it exactly as if the CAS
                    // response had confirmed directly.
                    prior: reconciling_claim
                        .as_ref()
                        .and_then(|claim| claim.prior.clone()),
                };
                self.record_acquisition(&spec);
                let ownership_only = !cell.requests.is_empty()
                    && cell
                        .requests
                        .iter()
                        .all(|request| self.handoff_requests.contains(request));
                if ownership_only {
                    set_phase(
                        &mut self.occupied,
                        &mut cell,
                        Phase::Dormant { epoch: spec.epoch },
                    );
                } else {
                    self.activate_or_wait(&id, &mut cell, Activation::Restore(spec), effects);
                }
            }
            Ok(Some(record)) if record.node.as_deref() == Some(self.node.as_str()) => {
                let epoch = record.epoch.saturating_add(1);
                let prior = record.node.clone();
                self.activate_or_wait(
                    &id,
                    &mut cell,
                    Activation::Claim(Claim {
                        guard: CasGuard::Match(record.etag),
                        epoch,
                        takeover: false,
                        prior,
                        reconciles,
                    }),
                    effects,
                );
            }
            Ok(Some(record)) if record.node.is_none() => {
                let epoch = record.epoch.saturating_add(1);
                self.place_unowned(
                    &id,
                    &mut cell,
                    Claim {
                        guard: CasGuard::Match(record.etag),
                        epoch,
                        takeover: true,
                        prior: None,
                        reconciles,
                    },
                    effects,
                );
            }
            Ok(Some(record)) => {
                let owner = record.node.clone().expect("foreign owner checked above");
                let cached = self
                    .node_lease_cache
                    .get(&owner)
                    .filter(|lease| lease.expires_ms > now_ms && !lease.addr.is_empty())
                    .cloned();
                if let Some(lease) = cached {
                    self.apply_node_lease_result(
                        &id,
                        &mut cell,
                        record,
                        now_ms,
                        Ok(Some(lease)),
                        effects,
                    );
                } else {
                    self.node_lease_cache.remove(&owner);
                    let next = self.cell_op(&id);
                    set_phase(
                        &mut self.occupied,
                        &mut cell,
                        Phase::ReadingNodeLease {
                            op: next,
                            owner: record,
                            issued_ms: now_ms,
                        },
                    );
                    effects.push(Effect::ReadNodeLease {
                        op: next,
                        cell: id.clone(),
                        owner,
                    });
                }
            }
            Ok(None) => {
                self.place_unowned(
                    &id,
                    &mut cell,
                    Claim {
                        guard: CasGuard::Absent,
                        epoch: 1,
                        takeover: false,
                        prior: None,
                        reconciles,
                    },
                    effects,
                );
            }
            Err(_) => {
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                self.finish_requests(&id, &mut cell, Err(RequestError::ResolveFailed), effects);
            }
        }
        self.cells.insert(id, cell);
        if reconciling_claim.is_some() {
            self.pump_capacity(effects);
        }
    }

    fn node_lease_read(
        &mut self,
        op: OpId,
        _now_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(op, |cell| {
            matches!(cell.phase, Phase::ReadingNodeLease { op: current, .. } if current == op)
        }) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::ReadingNodeLease {
            owner: record,
            issued_ms,
            ..
        } = &cell.phase
        else {
            unreachable!()
        };
        let record = record.clone();
        let issued_ms = *issued_ms;
        if let Ok(Some(lease)) = &result {
            if record.node.as_deref() == Some(lease.node.as_str())
                && lease.expires_ms > issued_ms
                && !lease.addr.is_empty()
            {
                self.node_lease_cache
                    .insert(lease.node.clone(), lease.clone());
            }
        }
        self.apply_node_lease_result(&id, &mut cell, record, issued_ms, result, effects);
        self.cells.insert(id, cell);
    }

    fn capacity_peers_read(
        &mut self,
        op: OpId,
        now_ms: u64,
        result: Result<Vec<CapacityPeer>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(op, |cell| {
            matches!(cell.phase, Phase::ReadingCapacity { op: current, .. } if current == op)
        }) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::ReadingCapacity { claim, .. } = &cell.phase else {
            unreachable!()
        };
        let claim = claim.clone();
        let peers = match result {
            Ok(peers) => peers,
            Err(_) => {
                self.activate_or_wait(&id, &mut cell, Activation::Claim(claim), effects);
                self.cells.insert(id, cell);
                return;
            }
        };

        // A newer load sample supersedes both reservations made against and
        // refusals of the prior sample. Equal samples deliberately retain
        // both, which is what makes concurrent read completions compose.
        for peer in &peers {
            let prior = self.capacity_samples.get(&peer.node).copied().unwrap_or(0);
            if peer.sampled_ms > prior {
                self.capacity_samples
                    .insert(peer.node.clone(), peer.sampled_ms);
                self.capacity_reservations.remove(&peer.node);
                self.capacity_rejections.remove(&peer.node);
            }
        }

        let selected = peers
            .into_iter()
            .filter(|peer| {
                peer.node != self.node
                    && peer.expires_ms > now_ms
                    && !peer.addr.is_empty()
                    && peer.peer_protocol == self.config.peer_protocol
                    && peer.sampled_ms != 0
                    && !peer.pressured
                    && self
                        .capacity_rejections
                        .get(&peer.node)
                        .is_none_or(|sample| peer.sampled_ms > *sample)
            })
            .map(|peer| {
                let projected = peer.resident_cells.saturating_add(
                    self.capacity_reservations
                        .get(&peer.node)
                        .copied()
                        .unwrap_or(0),
                );
                (peer, projected)
            })
            .min_by_key(|(peer, projected)| {
                // The third key is the memory the peer holds, which is what
                // the peer decides its own pressure on. A peer from before that
                // field existed reports nothing, and its resident set size is
                // the only number available -- which reads as more loaded than
                // it is, and is the conservative direction for a tiebreak.
                (
                    *projected,
                    peer.host_websockets,
                    peer.in_use_bytes.unwrap_or(peer.rss_bytes),
                    peer.node.clone(),
                )
            });

        if let Some((peer, _)) = selected {
            *self
                .capacity_reservations
                .entry(peer.node.clone())
                .or_default() += 1;
            set_phase(
                &mut self.occupied,
                &mut cell,
                Phase::Remote {
                    node: peer.node.clone(),
                    addr: peer.addr.clone(),
                    epoch: 0,
                    peer_protocol: peer.peer_protocol,
                    capacity_sampled_ms: Some(peer.sampled_ms),
                },
            );
            self.finish_requests(
                &id,
                &mut cell,
                Ok(Route::Remote {
                    node: peer.node,
                    addr: peer.addr,
                    epoch: 0,
                    peer_protocol: peer.peer_protocol,
                }),
                effects,
            );
        } else {
            self.activate_or_wait(&id, &mut cell, Activation::Claim(claim), effects);
        }
        self.cells.insert(id, cell);
    }

    fn apply_node_lease_result(
        &mut self,
        id: &str,
        cell: &mut Cell,
        record: OwnerRecord,
        issued_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        match result {
            Ok(Some(lease))
                if lease.expires_ms > issued_ms
                    && !lease.addr.is_empty()
                    && record.node.as_deref() == Some(lease.node.as_str())
                    && lease.peer_protocol != self.config.peer_protocol =>
            {
                set_phase(&mut self.occupied, cell, Phase::Inactive);
                self.finish_requests(id, cell, Err(RequestError::PeerIncompatible), effects);
            }
            Ok(Some(lease))
                if lease.expires_ms > issued_ms
                    && !lease.addr.is_empty()
                    && record.node.as_deref() == Some(lease.node.as_str())
                    && lease.peer_protocol == self.config.peer_protocol =>
            {
                let node = lease.node;
                let addr = lease.addr;
                let epoch = record.epoch;
                set_phase(
                    &mut self.occupied,
                    cell,
                    Phase::Remote {
                        node: node.clone(),
                        addr: addr.clone(),
                        epoch,
                        peer_protocol: lease.peer_protocol,
                        capacity_sampled_ms: None,
                    },
                );
                self.finish_requests(
                    id,
                    cell,
                    Ok(Route::Remote {
                        node,
                        addr,
                        epoch,
                        peer_protocol: lease.peer_protocol,
                    }),
                    effects,
                );
            }
            Ok(lease) => {
                // The takeover interlock, decided HERE on the lease the
                // core just read (lease-fold): a dead owner whose folded
                // log state is not sealed may hold acked writes only on
                // its followers, and claiming before recovery fixes a
                // bucket state that excludes them. Absence of a lease —
                // or a lease that never opened a log — keeps its meaning
                // as a proof: nothing was acked past the bucket.
                let unrecovered = lease.as_ref().is_some_and(|lease| {
                    !matches!(
                        lease.log_state,
                        None | Some(crate::log_tier::LogState::Sealed)
                    )
                });
                if unrecovered {
                    let owner = record.node.clone().unwrap_or_default();
                    let next = self.cell_op(id);
                    set_phase(
                        &mut self.occupied,
                        cell,
                        Phase::RecoveringOwnerLog {
                            op: next,
                            owner: record,
                        },
                    );
                    effects.push(Effect::RecoverNodeLog {
                        op: next,
                        cell: id.to_string(),
                        owner,
                    });
                    return;
                }
                let epoch = record.epoch.saturating_add(1);
                let prior = record.node.clone();
                self.activate_or_wait(
                    id,
                    cell,
                    Activation::Claim(Claim {
                        guard: CasGuard::Match(record.etag),
                        epoch,
                        takeover: true,
                        prior,
                        reconciles: 0,
                    }),
                    effects,
                );
            }
            Err(_) => {
                set_phase(&mut self.occupied, cell, Phase::Inactive);
                self.finish_requests(id, cell, Err(RequestError::ResolveFailed), effects);
            }
        }
    }

    /// The interlock's completion: recovery sealed (or proved absent)
    /// every session of the dead owner, so the claim the gate deferred
    /// proceeds against the SAME owner record the decision was made on —
    /// the CAS guard still carries its etag, so a record that moved in
    /// the meantime rejects the claim and resolution restarts.
    fn node_log_recovered(
        &mut self,
        op: OpId,
        result: Result<(), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::RecoveringOwnerLog { op: p, .. } if p == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::RecoveringOwnerLog { owner, .. } = cell.phase.clone() else {
            unreachable!()
        };
        match result {
            Ok(()) => {
                let epoch = owner.epoch.saturating_add(1);
                let prior = owner.node.clone();
                self.activate_or_wait(
                    &id,
                    &mut cell,
                    Activation::Claim(Claim {
                        guard: CasGuard::Match(owner.etag),
                        epoch,
                        takeover: true,
                        prior,
                        reconciles: 0,
                    }),
                    effects,
                );
                cell.owner_log_recovery_attempts = 0;
            }
            Err(_) => {
                // A recovery fails precisely while the survivors sweep the
                // dead owner's deep log — minutes of work for a large
                // owner. The request waits the sweep out
                // behind a backoff instead of failing, and the permit
                // returns so one dead owner's cells cannot starve the node.
                cell.owner_log_recovery_attempts =
                    cell.owner_log_recovery_attempts.saturating_add(1);
                if cell.owner_log_recovery_attempts >= self.config.owner_log_recovery_attempts {
                    cell.owner_log_recovery_attempts = 0;
                    set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                    self.finish_requests(&id, &mut cell, Err(RequestError::ResolveFailed), effects);
                } else {
                    let generation = self.next_timer_generation;
                    self.next_timer_generation = self
                        .next_timer_generation
                        .checked_add(1)
                        .expect("timer generation exhausted");
                    set_phase(
                        &mut self.occupied,
                        &mut cell,
                        Phase::WaitingOwnerLogRecovery { generation },
                    );
                    effects.push(Effect::ScheduleTimer {
                        timer: Timer::OwnerLogRecoveryRetry {
                            cell: id.clone(),
                            generation,
                        },
                        at_mono_ms: self
                            .now_mono_ms
                            .saturating_add(self.config.owner_log_recovery_backoff_ms),
                    });
                }
            }
        }
        self.cells.insert(id, cell);
        self.pump_activations(effects);
    }

    /// A backed-off dead-owner recovery re-enters ownership resolution
    /// from the top: the owner record may have moved while the cell
    /// waited, and the admission queue shares permits fairly with every
    /// other cold route.
    fn retry_owner_log_recovery(&mut self, id: &str, generation: u64, effects: &mut Vec<Effect>) {
        let Some(mut cell) = self.cells.remove(id) else {
            return;
        };
        if cell.phase != (Phase::WaitingOwnerLogRecovery { generation }) {
            self.cells.insert(id.to_string(), cell);
            return;
        }
        if cell.requests.is_empty() && !cell.alarm_wake && !cell.resume_demand {
            // Every waiting request was cancelled during the backoff;
            // nothing demands the cell any more.
            cell.owner_log_recovery_attempts = 0;
            set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
        } else {
            self.admit_or_queue_activation(id, &mut cell, ColdStart::ReadOwner, effects);
        }
        self.cells.insert(id.to_string(), cell);
    }

    fn owner_cas_completed(
        &mut self,
        op: OpId,
        result: Result<CasOutcome, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Acquiring { op: current, .. } if current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Acquiring { claim, dormant, .. } = &cell.phase else {
            unreachable!()
        };
        let dormant = *dormant;
        let claim = claim.clone();
        match result {
            Ok(CasOutcome::Applied) => {
                let spec = RestoreSpec {
                    epoch: claim.epoch,
                    fresh: matches!(claim.guard, CasGuard::Absent),
                    took_over: claim.takeover,
                    resume_local: false,
                    prior: claim.prior.clone(),
                };
                self.record_acquisition(&spec);
                let ownership_only = !cell.requests.is_empty()
                    && cell
                        .requests
                        .iter()
                        .all(|request| self.handoff_requests.contains(request));
                if ownership_only {
                    set_phase(
                        &mut self.occupied,
                        &mut cell,
                        Phase::Dormant { epoch: spec.epoch },
                    );
                } else if dormant {
                    // Live demand joined a dormant-bound claim before the CAS
                    // landed. The claim held no residency slot, so the restore
                    // must go through the capacity queue rather than start
                    // inline on a node that has none.
                    self.activate_or_wait(&id, &mut cell, Activation::Restore(spec), effects);
                } else {
                    // An ordinary claim already occupies the slot its restore
                    // needs, so the transition stays inline; re-checking
                    // capacity here would count the cell against itself.
                    let next = self.cell_op(&id);
                    set_phase(
                        &mut self.occupied,
                        &mut cell,
                        Phase::Restoring {
                            op: next,
                            spec: spec.clone(),
                        },
                    );
                    effects.push(Effect::Restore {
                        op: next,
                        cell: id.clone(),
                        spec,
                    });
                }
            }
            Ok(CasOutcome::Rejected) => {
                let next = self.cell_op(&id);
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::ReadingOwner { op: next },
                );
                effects.push(Effect::ReadOwner {
                    op: next,
                    cell: id.clone(),
                });
            }
            Err(Failure::Ambiguous) if claim.reconciles < MAX_ACQUIRE_RECONCILES => {
                let next = self.cell_op(&id);
                let claim = Claim {
                    reconciles: claim.reconciles + 1,
                    ..claim
                };
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::ReconcilingAcquire {
                        op: next,
                        claim,
                        dormant,
                    },
                );
                effects.push(Effect::ReadOwner {
                    op: next,
                    cell: id.clone(),
                });
            }
            Err(Failure::Ambiguous) => {
                // Out of reconciles. The claim may or may not have applied, so
                // this cell is left dormant for a later request to resolve
                // from the record rather than guessed at here.
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                self.finish_requests(&id, &mut cell, Err(RequestError::AcquireFailed), effects);
            }
            Err(Failure::Definite) => {
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                self.finish_requests(&id, &mut cell, Err(RequestError::AcquireFailed), effects);
            }
        }
        self.cells.insert(id, cell);
        self.pump_capacity(effects);
    }

    fn restore_completed(
        &mut self,
        op: OpId,
        result: Result<RestoreOutcome, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Restoring { op: current, .. } if current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Restoring { spec, .. } = &cell.phase else {
            unreachable!()
        };
        let epoch = spec.epoch;
        let mut resume_failed = false;
        match result {
            Ok(outcome) => {
                if outcome.restored {
                    self.activity.restored = self.activity.restored.saturating_add(1);
                }
                // Seed the mirror from the durable truth the restore loaded,
                // before the isolate opens the same database and long before
                // it would re-arm anything.
                if let Some(alarm) = outcome.alarm.filter(|alarm| alarm.at_ms >= 0) {
                    let generation = self.next_timer_generation;
                    self.next_timer_generation = self
                        .next_timer_generation
                        .checked_add(1)
                        .expect("timer generation exhausted");
                    let (now_ms, now_mono_ms) = (self.now_ms, self.now_mono_ms);
                    self.schedule_alarm_timer(
                        &id,
                        generation,
                        alarm.at_ms,
                        now_ms,
                        now_mono_ms,
                        effects,
                    );
                    cell.alarm = Some(AlarmState::Armed {
                        at_ms: alarm.at_ms,
                        generation,
                        covered: alarm.covered,
                    });
                }
                let next = self.cell_op(&id);
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::Starting { op: next, epoch },
                );
                effects.push(Effect::StartRuntime {
                    op: next,
                    cell: id.clone(),
                    epoch,
                });
            }
            Err(_) => {
                set_phase(&mut self.occupied, &mut cell, Phase::Dormant { epoch });
                self.finish_requests(&id, &mut cell, Err(RequestError::RestoreFailed), effects);
                resume_failed = cell.resume_demand;
            }
        }
        self.cells.insert(id.clone(), cell);
        if resume_failed {
            self.settle_local_resume(&id);
        }
        self.pump_capacity(effects);
    }

    fn runtime_started(
        &mut self,
        op: OpId,
        isolate: Option<isolate::HeapId>,
        generation: u64,
        result: Result<(), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Starting { op: current, .. } if current == op),
        ) else {
            self.compensate_retired_runtime(op, result, effects);
            return;
        };
        self.timed_out_runtime_starts.remove(&op);
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Starting { epoch, .. } = cell.phase else {
            unreachable!()
        };
        let mut resume_failed = false;
        match result {
            Ok(()) => {
                cell.isolate = isolate;
                cell.generation = generation;
                let next = self.cell_op(&id);
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::Publishing { op: next, epoch },
                );
                effects.push(Effect::Publish {
                    op: next,
                    cell: id.clone(),
                    epoch,
                });
            }
            Err(_) => {
                set_phase(&mut self.occupied, &mut cell, Phase::Dormant { epoch });
                self.finish_requests(&id, &mut cell, Err(RequestError::RuntimeFailed), effects);
                resume_failed = cell.resume_demand;
            }
        }
        self.cells.insert(id.clone(), cell);
        if resume_failed {
            self.settle_local_resume(&id);
        }
        self.pump_capacity(effects);
    }

    /// Fail the callers waiting on a runtime start without retiring the start.
    ///
    /// The shell cannot cancel a V8 startup turn. Treating this deadline as a
    /// definite failure lets the next request start a second runtime while the
    /// first task can still insert its handle. The late first completion then
    /// becomes an untracked runtime which every later request reports as an
    /// existing duplicate. Keep the operation indexed instead. Its real
    /// completion publishes the runtime or returns the cell to `Dormant`.
    fn runtime_start_timed_out(&mut self, op: OpId, effects: &mut Vec<Effect>) {
        let Some(id) = self.cell_ops.get(&op).cloned() else {
            return;
        };
        let Some(mut cell) = self.cells.remove(&id) else {
            return;
        };
        if !matches!(cell.phase, Phase::Starting { op: current, .. } if current == op) {
            self.cells.insert(id, cell);
            return;
        }
        self.timed_out_runtime_starts.insert(op);
        self.finish_requests(&id, &mut cell, Err(RequestError::RuntimeFailed), effects);
        self.cells.insert(id, cell);
    }

    fn published(&mut self, op: OpId, result: Result<(), Failure>, effects: &mut Vec<Effect>) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Publishing { op: current, .. } if current == op),
        ) else {
            self.compensate_retired_runtime(op, result, effects);
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Publishing { epoch, .. } = cell.phase else {
            unreachable!()
        };
        let completing_resume = cell.resume_demand;
        match result {
            Ok(()) => {
                set_phase(&mut self.occupied, &mut cell, Phase::Resident { epoch });
                // A swap ends here: the cell is resident again, on the
                // generation `runtime_started` recorded.
                cell.swapping = false;
                cell.swap_cancelled = false;
                self.finish_requests(&id, &mut cell, Ok(Route::Local), effects);
            }
            Err(_) => {
                let next = self.cell_op(&id);
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::Cleaning {
                        op: next,
                        epoch,
                        cause: StopCause::Cleanup,
                    },
                );
                effects.push(Effect::StopRuntime {
                    op: next,
                    cell: id.clone(),
                    epoch,
                    cause: StopCause::Cleanup,
                    bounded: false,
                });
            }
        }
        self.cells.insert(id.clone(), cell);
        if completing_resume {
            self.settle_local_resume(&id);
        }
    }

    /// A release either published the cell as unowned or did not. Ownership
    /// is the bucket's answer, not this node's, so a rejected or failed write
    /// simply leaves the record naming this node -- correct, if less useful,
    /// and the next eviction gets another chance.
    fn owner_released(
        &mut self,
        op: OpId,
        result: Result<CasOutcome, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(op, |cell| cell.releasing == Some(op)) else {
            return;
        };
        let released_epoch = match self.cells.get(&id).map(|cell| &cell.phase) {
            Some(Phase::Dormant { epoch }) => Some(*epoch),
            _ => None,
        };
        // Only a cell still sitting where the eviction left it may be
        // forgotten. Anything else means it was wanted again while the write
        // was in flight, and that claim outranks a release decided earlier.
        // On a draining node a rejection settles the cell too: the record no
        // longer names this node at this epoch, so nothing here blocks a
        // successor, and the pump must not re-read a record it can never
        // blank. An indefinite failure stays `Dormant`, which is what makes
        // the pump retry it while the drain window is open.
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let hands_off = self.hands_off(&cell);
        let settled = matches!(result, Ok(CasOutcome::Applied))
            || (hands_off && matches!(result, Ok(CasOutcome::Rejected)));
        let adoption =
            (settled && hands_off && released_epoch.is_some()).then(|| self.cell_op(&id));
        self.releasing_cells.remove(&id);
        if adoption.is_some() {
            self.adopting_cells.insert(id.clone());
        }
        cell.releasing = None;
        if let (Some(released_epoch), Some(adopt)) = (released_epoch, adoption) {
            set_phase(
                &mut self.occupied,
                &mut cell,
                Phase::Adopting {
                    op: adopt,
                    released_epoch,
                },
            );
            effects.push(Effect::AdoptReleased {
                op: adopt,
                cell: id.clone(),
                released_epoch,
                rebalance: cell.handoff,
            });
        } else {
            if settled && matches!(cell.phase, Phase::Dormant { .. }) {
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
            }
            // A balancing move that did not reach adoption is over. The
            // requests it parked take the ordinary path: a wake if the
            // record still names this node, an owner read if it does not.
            if std::mem::take(&mut cell.handoff) && !cell.requests.is_empty() {
                self.admit_or_queue_activation(&id, &mut cell, ColdStart::ReadOwner, effects);
            }
        }
        self.cells.insert(id, cell);
    }

    /// Finish one shutdown handoff after a successor acquired the cell. The
    /// successor keeps the cell dormant until real traffic needs a runtime,
    /// so a fleet restart does not eagerly materialize every resident cell.
    fn successor_adopted(
        &mut self,
        op: OpId,
        result: Result<AdoptedCell, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Adopting { op: current, .. } if current == op),
        ) else {
            return;
        };
        self.adopting_cells.remove(&id);
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Adopting { released_epoch, .. } = cell.phase else {
            unreachable!()
        };
        let handoff = std::mem::take(&mut cell.handoff);
        match result {
            Ok(adopted)
                if adopted.node != self.node
                    && !adopted.addr.is_empty()
                    && adopted.epoch > released_epoch
                    && adopted.peer_protocol == self.config.peer_protocol =>
            {
                self.activity.handed_off = self.activity.handed_off.saturating_add(1);
                if handoff {
                    self.activity.rebalanced = self.activity.rebalanced.saturating_add(1);
                }
                let route = Route::Remote {
                    node: adopted.node.clone(),
                    addr: adopted.addr.clone(),
                    epoch: adopted.epoch,
                    peer_protocol: adopted.peer_protocol,
                };
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::Remote {
                        node: adopted.node,
                        addr: adopted.addr,
                        epoch: adopted.epoch,
                        peer_protocol: adopted.peer_protocol,
                        capacity_sampled_ms: None,
                    },
                );
                self.finish_requests(&id, &mut cell, Ok(route), effects);
            }
            // A drain has no future here, so its parked requests fail. A
            // balancing move only released the record: the cell is unowned,
            // and its requests re-read ownership like any cold request.
            _ if handoff => {
                self.activity.handoff_failed = self.activity.handoff_failed.saturating_add(1);
                self.activity.rebalance_failed = self.activity.rebalance_failed.saturating_add(1);
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                if !cell.requests.is_empty() {
                    self.admit_or_queue_activation(&id, &mut cell, ColdStart::ReadOwner, effects);
                }
            }
            _ => {
                self.activity.handoff_failed = self.activity.handoff_failed.saturating_add(1);
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                self.finish_requests(&id, &mut cell, Err(RequestError::NodeUnavailable), effects);
            }
        }
        self.cells.insert(id, cell);
    }

    fn runtime_stopped(&mut self, op: OpId, effects: &mut Vec<Effect>) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Cleaning { op: current, .. } if current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        // The realm went with the runtime. Leaving the isolate recorded here
        // would keep counting this cell against that heap, so the walk down
        // would never see an isolate empty and would never aim at one.
        cell.isolate = None;
        let Phase::Cleaning { epoch, cause, .. } = cell.phase else {
            unreachable!()
        };
        match cause {
            StopCause::Cleanup => {
                set_phase(&mut self.occupied, &mut cell, Phase::Dormant { epoch });
                self.finish_requests(&id, &mut cell, Err(RequestError::PublishFailed), effects);
            }
            // `Inactive`, not `Dormant`: the next request re-reads the
            // ownership record. A proof that could not be completed is exactly
            // the moment to stop assuming this node still holds the cell, and
            // the re-read costs one GET on a path that was already failing.
            StopCause::Reset => {
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                if let Some(AlarmState::Firing { op, .. }) = cell.alarm {
                    self.cell_ops.remove(&op);
                }
                cell.alarm = None;
                cell.alarm_wake = false;
                cell.websockets.clear();
                self.finish_requests(
                    &id,
                    &mut cell,
                    Err(RequestError::DurabilityUnproven),
                    effects,
                );
            }
            StopCause::Evict { rebalance }
                if cell.requests.is_empty() || (self.draining && rebalance) =>
            {
                set_phase(&mut self.occupied, &mut cell, Phase::Dormant { epoch });
                self.eviction_permits.remove(&id);
                // The record still names this node, which is the whole cost of
                // stopping here: every later request for the cell routes to a
                // node that has already decided it has no room. Publishing it
                // as unowned is what turns an eviction into shed load.
                if rebalance {
                    let op = self.cell_op(&id);
                    cell.releasing = Some(op);
                    self.releasing_cells.insert(id.clone());
                    effects.push(Effect::ReleaseOwner {
                        op,
                        cell: id.clone(),
                        epoch,
                    });
                }
            }
            StopCause::Evict { .. } => {
                // A request arrived mid-eviction, so the cell turns straight
                // back around. The eviction is over either way.
                self.eviction_permits.remove(&id);
                // The stop discarded the LTX metadata. Re-read ownership and
                // claim epoch+1 before reusing the preserved SQLite image.
                self.admit_or_queue_activation(&id, &mut cell, ColdStart::ReadOwner, effects);
            }
            // The node began leaving while the cell was between isolates.
            // Started again it would only be one more resident for the
            // drain to evict; dormant, the drain releases its record like
            // any other.
            StopCause::Swap if self.draining => {
                set_phase(&mut self.occupied, &mut cell, Phase::Dormant { epoch });
                self.finish_requests(&id, &mut cell, Err(RequestError::PublishFailed), effects);
            }
            // The swap's second half: the same epoch, the same owner record,
            // the same replica, a new isolate. `Starting` without `Restore`
            // is deliberate — the database is open on disk and the LTX
            // handle never closed — and without `CasOwner`, because nothing
            // about ownership changed. Requests queued while the cell
            // quiesced release when it publishes.
            StopCause::Swap => {
                let op = self.cell_op(&id);
                set_phase(&mut self.occupied, &mut cell, Phase::Starting { op, epoch });
                effects.push(Effect::StartRuntime {
                    op,
                    cell: id.clone(),
                    epoch,
                });
            }
            StopCause::Fence => unreachable!("fenced cells do not wait for runtime shutdown"),
        }
        self.cells.insert(id, cell);
        self.pump_capacity(effects);
        self.shed_toward_floor(effects);
    }

    /// A bounded stop gave up. The host dropped the runtime and kept the
    /// database open, which is the state a generation swap leaves between
    /// its halves, so the cell restarts on the open handle at the same
    /// epoch through the swap's own path. The eviction is recorded as
    /// refused, so idle eviction waits another idle period before it tries
    /// the cell again instead of stopping it on every sample. Without this a
    /// cell whose final proof stalled retried every 10 s without end and
    /// answered nobody (the 2026-09-03 whale runs).
    fn runtime_stop_failed(&mut self, op: OpId, effects: &mut Vec<Effect>) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Cleaning { op: current, .. } if current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Cleaning { epoch, cause, .. } = cell.phase else {
            unreachable!()
        };
        assert!(
            matches!(cause, StopCause::Evict { .. }),
            "only an eviction stop is bounded"
        );
        cell.isolate = None;
        cell.evict_rebalance = false;
        cell.handoff = false;
        cell.eviction_refused_mono_ms = Some(self.now_mono_ms);
        self.eviction_permits.remove(&id);
        let next = self.cell_op(&id);
        cell.swapping = true;
        set_phase(
            &mut self.occupied,
            &mut cell,
            Phase::Starting { op: next, epoch },
        );
        effects.push(Effect::StartRuntime {
            op: next,
            cell: id.clone(),
            epoch,
        });
        self.cells.insert(id, cell);
    }

    /// Announce the generation the node now serves.
    ///
    /// Nothing moves here, and nothing is marked; the pump moves cells after
    /// this and every later event, so the bound, the safe point, and
    /// eagerness are decided by the same code whether a cell became stale
    /// now or lands stale later. A resident cell of an eager class is forced
    /// at once: the engine's reserved cells hold no application state worth
    /// waiting for, and the cron cell must run the new schedule before the
    /// adoption arms it.
    fn generation_changed(&mut self, generation: u64, max_age_ms: u64, eager_classes: &[String]) {
        if generation <= self.current_generation {
            return;
        }
        self.current_generation = generation;
        self.generation_changed_mono_ms = self.now_mono_ms;
        self.swap_max_age_ms = max_age_ms;
        self.swap_eager_classes = eager_classes.iter().cloned().collect();
    }

    /// Whether this cell's class was named eager by the last announcement, so
    /// its swap does not wait for the maximum age.
    ///
    /// A scope is `<class>:<instance>` and the instance may itself contain a
    /// `:`, so the class is everything before the first one -- the same split
    /// the shell makes when it resolves a cell to its Worker config.
    fn swap_is_eager(&self, id: &str) -> bool {
        let class = id.split_once(':').map_or(id, |(class, _)| class);
        self.swap_eager_classes.contains(class)
    }

    /// Whether a swap can stop this cell's runtime now without cutting work
    /// short: no request running, no output withheld by the gate, no alarm
    /// handler in flight, and no regular or outbound socket, which cannot
    /// outlive the isolate. The input gate is not consulted; a
    /// `blockConcurrencyWhile` holder is a running request or alarm, so it is
    /// covered by those.
    fn swap_safe_point(&self, id: &str) -> bool {
        let Some(cell) = self.cells.get(id) else {
            return false;
        };
        !self.active_cells.contains_key(id)
            && !cell
                .websockets
                .values()
                .any(|kind| matches!(kind, WebSocketKind::Regular | WebSocketKind::Outbound))
            && !matches!(cell.alarm, Some(AlarmState::Firing { .. }))
            && !self.barriers.values().any(|barrier| barrier.cell == id)
    }

    /// The swap pump, run after every event once a generation change has
    /// been announced: move every resident cell on an older generation to
    /// the current one, at most `max_releases` at a time.
    ///
    /// A nominated cell quiesces first, exactly as a shutdown handoff does:
    /// new requests queue for the restarted runtime instead of routing to the
    /// old one, so continuous traffic cannot keep the cell from ever reaching
    /// a safe point. Work already admitted finishes. At the safe point the
    /// cell stops with `StopCause::Swap`, and `runtime_stopped` starts it
    /// again on the current generation at the same epoch.
    ///
    /// A cell that never reaches a safe point — a regular WebSocket, an
    /// endless request — is forced once the maximum age has passed, or at
    /// once when its class is eager: its activity is cancelled, which is what
    /// a Cloudflare deployment does to every object unconditionally. The
    /// hibernatable sockets survive either way.
    fn pump_swap(&mut self, effects: &mut Vec<Effect>) {
        if self.current_generation == 0
            || self.draining
            || self.fenced
            || !self.node_authoritative()
        {
            return;
        }
        let current = self.current_generation;
        let forced_by_age = self
            .now_mono_ms
            .saturating_sub(self.generation_changed_mono_ms)
            >= self.swap_max_age_ms;
        let in_flight = self.cells.values().filter(|cell| cell.swapping).count();
        let room = self.config.max_releases.saturating_sub(in_flight);
        let stale: Vec<CellId> = self
            .cells
            .iter()
            .filter(|(_, cell)| {
                matches!(cell.phase, Phase::Resident { .. })
                    && !cell.swapping
                    && !cell.quiescing
                    && cell.releasing.is_none()
                    && cell.generation != current
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in stale.into_iter().take(room) {
            let cell = self.cells.get_mut(&id).expect("stale cell listed above");
            cell.swapping = true;
            cell.quiescing = true;
            self.quiescing_cells.insert(id);
        }
        let nominated: Vec<CellId> = self
            .cells
            .iter()
            .filter(|(_, cell)| {
                cell.swapping && cell.quiescing && matches!(cell.phase, Phase::Resident { .. })
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in nominated {
            if !self.swap_safe_point(&id) {
                let eager = self.swap_is_eager(&id);
                let cell = self.cells.get(&id).expect("nominated cell listed above");
                let alarm = match cell.alarm {
                    Some(AlarmState::Firing { op, .. }) => Some(op),
                    _ => None,
                };
                if !(forced_by_age || eager) || cell.swap_cancelled {
                    continue;
                }
                let requests = self
                    .active_requests
                    .iter()
                    .filter_map(|(request, cell)| (cell == &id).then_some(*request))
                    .collect();
                let cell = self
                    .cells
                    .get_mut(&id)
                    .expect("nominated cell listed above");
                cell.swap_cancelled = true;
                let websockets = cell
                    .websockets
                    .iter()
                    .filter(|(_, kind)| !matches!(kind, WebSocketKind::Hibernatable))
                    .map(|(websocket, _)| *websocket)
                    .collect();
                effects.push(Effect::CancelCellActivity {
                    cell: id,
                    requests,
                    websockets,
                    alarm,
                    keep_hibernatable: true,
                });
                continue;
            }
            let op = self.cell_op(&id);
            let cell = self
                .cells
                .get_mut(&id)
                .expect("nominated cell listed above");
            let Phase::Resident { epoch } = cell.phase else {
                unreachable!("nominated cells are resident")
            };
            set_phase(
                &mut self.occupied,
                cell,
                Phase::Cleaning {
                    op,
                    epoch,
                    cause: StopCause::Swap,
                },
            );
            self.quiescing_cells.remove(&id);
            effects.push(Effect::StopRuntime {
                op,
                cell: id,
                epoch,
                cause: StopCause::Swap,
                bounded: false,
            });
        }
    }

    /// The generation a cell's runtime was started on, if it has one.
    pub fn cell_generation(&self, id: &str) -> Option<u64> {
        self.cells.get(id).map(|cell| cell.generation)
    }

    /// The generation the node serves, as last announced.
    pub fn current_generation(&self) -> u64 {
        self.current_generation
    }

    /// Cells moving to the current generation right now.
    pub fn swapping(&self) -> usize {
        self.cells.values().filter(|cell| cell.swapping).count()
    }

    fn durability_checked(
        &mut self,
        op: OpId,
        result: Result<(), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let now = self.now_mono_ms;
        let Some(id) = self.take_cell_op(op, |cell| {
            matches!(cell.phase, Phase::EnsuringDurability { op: current, .. } if current == op)
        }) else {
            return;
        };
        let cell = self.cells.get_mut(&id).expect("cell found above");
        let Phase::EnsuringDurability { epoch, .. } = cell.phase else {
            unreachable!()
        };
        match result {
            Ok(()) => {
                // Minted inline: `cell` still borrows the map, and field
                // accesses split where a method call cannot.
                let stop = self.next_op;
                self.next_op = self.next_op.checked_add(1).expect("operation id exhausted");
                self.cell_ops.insert(stop, id.clone());
                let rebalance = cell.evict_rebalance;
                set_phase(
                    &mut self.occupied,
                    cell,
                    Phase::Cleaning {
                        op: stop,
                        epoch,
                        cause: StopCause::Evict { rebalance },
                    },
                );
                effects.push(Effect::StopRuntime {
                    op: stop,
                    cell: id,
                    epoch,
                    cause: StopCause::Evict { rebalance },
                    bounded: !self.draining,
                });
            }
            Err(_) => {
                set_phase(&mut self.occupied, cell, Phase::Resident { epoch });
                cell.eviction_refused_mono_ms = Some(now);
                self.eviction_permits.remove(&id);
            }
        }
    }

    /// Evict on demand. Local, like an idle eviction: the caller asked
    /// this node to drop the cell, not to give it away.
    fn evict(&mut self, id: &str, effects: &mut Vec<Effect>) {
        self.begin_eviction(id, false, effects);
    }

    /// Shutdown handoff: give every resident cell away by releasing its
    /// owner record, so peers take over at once instead of waiting out the
    /// node lease. `rebalance` is forced true, so this releases even when
    /// `ownership_on_evict` keeps ownership on an ordinary evict.
    ///
    /// Only marks the node draining; `pump_release` does the work, at most
    /// `max_releases` at a time. An unbounded walk here would start a
    /// durability proof per resident cell at once -- a node holding
    /// thousands turns its own shutdown into the restore storm the eviction
    /// bound exists to prevent, and trips the `validate` ceiling besides.
    fn release_all(&mut self, effects: &mut Vec<Effect>) {
        self.draining = true;
        self.pump_release(effects);
    }

    /// Fail every unacknowledged output held on `id` without discarding the
    /// runtime. A shutdown handoff has already quiesced the cell, and its
    /// mandatory final durability proof follows before ownership release. The
    /// caller therefore learns that its write was not acknowledged, while the
    /// handoff can still prove and preserve that write for a later retry.
    fn cancel_cell_outputs(&mut self, id: &str, effects: &mut Vec<Effect>) {
        let outputs: Vec<OpId> = self
            .barriers
            .iter()
            .filter(|(_, barrier)| {
                barrier.cell == id && matches!(barrier.owner, GateOwner::Output(_))
            })
            .map(|(op, _)| *op)
            .collect();
        for op in outputs {
            let Some(gate) = self.barriers.remove(&op) else {
                continue;
            };
            let GateOwner::Output(owner) = gate.owner else {
                unreachable!("only output gates were selected")
            };
            self.release_held(owner, Err(RequestError::DurabilityUnproven), effects);
            for follower in gate.followers {
                self.release_held(follower, Err(RequestError::DurabilityUnproven), effects);
            }
        }
    }

    /// The drain pump, run after every event once draining: keep
    /// `max_releases` handoffs in flight while any owned cell remains.
    /// The pump first marks a bounded batch as quiescing. A quiescing cell
    /// queues new requests for its successor, so continuous traffic cannot
    /// keep finding a fresh activity between eviction attempts. Work already
    /// admitted on the cell finishes before its durability proof starts.
    /// Releases in flight count against the same ceiling as the proofs: the
    /// bound exists to keep shutdown from flooding the store, and a release
    /// is one more write in that flood.
    fn pump_release(&mut self, effects: &mut Vec<Effect>) {
        if !self.draining {
            return;
        }
        let mut in_flight = self.handoffs_in_flight();

        // A reserved cell can become idle on any event. Promote it into the
        // ordinary eviction state without spending another handoff permit.
        let quiescing: Vec<CellId> = self.quiescing_cells.iter().cloned().collect();
        for id in quiescing {
            if self.begin_eviction(&id, true, effects) {
                self.quiescing_cells.remove(&id);
            }
        }

        // A full batch cannot nominate another cell. Stop before rebuilding
        // and sorting the resident inventory: peer traffic still reaches the
        // draining actor, and repeating that complete scan for every queued
        // request can starve the node-lease timer until the donor self-fences.
        if in_flight >= self.config.max_releases {
            return;
        }

        // A dormant Queue broker has no runtime to prove or stop, but it still
        // owns delayed deliveries and retry alarms. Move those owner records
        // before resident work can spend the complete bounded batch. The
        // general dormant walk below remains after residents because an idle
        // application cell carries no comparable background obligation.
        #[cfg(celld_internal_tests)]
        self.record_drain_table_visits(self.cells.len());
        let dormant_queues: Vec<(CellId, Epoch)> = self
            .cells
            .iter()
            .filter_map(|(id, cell)| match cell.phase {
                Phase::Dormant { epoch } if cell.releasing.is_none() && is_queue_broker(id) => {
                    Some((id.clone(), epoch))
                }
                _ => None,
            })
            .collect();
        for (id, epoch) in dormant_queues {
            if in_flight >= self.config.max_releases {
                return;
            }
            let op = self.cell_op(&id);
            let cell = self.cells.get_mut(&id).expect("dormant Queue listed above");
            cell.releasing = Some(op);
            self.releasing_cells.insert(id.clone());
            effects.push(Effect::ReleaseOwner {
                op,
                cell: id,
                epoch,
            });
            in_flight += 1;
        }

        // Reserve the complete batch even when its cells are active. Future
        // requests wait in `cell.requests`; the already-running requests keep
        // the runtime pinned until their activity guards finish.
        #[cfg(celld_internal_tests)]
        self.record_drain_table_visits(self.cells.len());
        let mut residents: Vec<(CellId, bool, u64, RequestId, u64)> = self
            .cells
            .iter()
            .filter(|(_, cell)| !cell.quiescing && matches!(cell.phase, Phase::Resident { .. }))
            .map(|(id, cell)| {
                (
                    id.clone(),
                    is_queue_broker(id),
                    cell.local_request_count,
                    cell.last_used_request,
                    cell.last_used_mono_ms,
                )
            })
            .collect();
        // A process deadline can cut this walk before it reaches every cell.
        // A Queue broker owns delayed delivery and retry alarms, so its work
        // does not always arrive as a routed request that increments the use
        // count. Protect those persistent brokers first. Then protect cells
        // that current traffic used repeatedly, followed by the newest use.
        // The old BTreeMap order handed off thousands of one-shot cells before
        // shared storage, so complete API surfaces waited for dead-node
        // recovery even though the donor made bounded handoff progress.
        residents.sort_by(
            |(left_id, left_queue, left_count, left_request, left_used),
             (right_id, right_queue, right_count, right_request, right_used)| {
                right_queue
                    .cmp(left_queue)
                    .then_with(|| right_count.cmp(left_count))
                    .then_with(|| right_request.cmp(left_request))
                    .then_with(|| right_used.cmp(left_used))
                    .then_with(|| left_id.cmp(right_id))
            },
        );
        for (id, _, _, _, _) in residents {
            if in_flight >= self.config.max_releases {
                return;
            }
            let requests = self
                .active_requests
                .iter()
                .filter_map(|(request, cell)| (cell == &id).then_some(*request))
                .collect();
            let cell = self.cells.get_mut(&id).expect("resident cell listed above");
            let alarm = match cell.alarm {
                Some(AlarmState::Firing { op, .. }) => Some(op),
                _ => None,
            };
            cell.quiescing = true;
            self.quiescing_cells.insert(id.clone());
            let websockets = cell.websockets.keys().copied().collect();
            self.cancel_cell_outputs(&id, effects);
            effects.push(Effect::CancelCellActivity {
                cell: id.clone(),
                requests,
                websockets,
                alarm,
                keep_hibernatable: false,
            });
            in_flight += 1;
            if self.begin_eviction(&id, true, effects) {
                self.quiescing_cells.remove(&id);
            }
        }
        // A cell evicted before the drain kept its record on this node --
        // a sticky eviction, hibernation, or a release whose write failed
        // -- so a later request could resume locally. On a leaving node
        // that record only makes a successor wait out the node lease. The
        // runtime is already stopped, so the drain releases the record
        // directly.
        #[cfg(celld_internal_tests)]
        self.record_drain_table_visits(self.cells.len());
        let dormant: Vec<(CellId, Epoch)> = self
            .cells
            .iter()
            .filter_map(|(id, cell)| match cell.phase {
                Phase::Dormant { epoch } if cell.releasing.is_none() => Some((id.clone(), epoch)),
                _ => None,
            })
            .collect();
        for (id, epoch) in dormant {
            if in_flight >= self.config.max_releases {
                return;
            }
            self.release_dormant(&id, epoch, effects);
            in_flight += 1;
        }
    }

    /// Release one dormant cell's record for a successor. The runtime is
    /// already stopped, so this is the record write alone.
    fn release_dormant(&mut self, id: &str, epoch: Epoch, effects: &mut Vec<Effect>) {
        let op = self.cell_op(id);
        let handoff = !self.draining;
        let cell = self.cells.get_mut(id).expect("dormant cell listed above");
        cell.releasing = Some(op);
        cell.handoff = handoff;
        self.releasing_cells.insert(id.to_string());
        // A hibernated cell parks its sockets past the runtime, and
        // nothing else on the handoff path visits them: without this
        // close the process exit tears them down as an abnormal
        // 1006, while a resident cell's clients get the clean 1012
        // above.
        if !cell.websockets.is_empty() {
            let websockets = cell.websockets.keys().copied().collect();
            effects.push(Effect::CancelCellActivity {
                cell: id.to_string(),
                requests: Vec::new(),
                websockets,
                alarm: None,
                keep_hibernatable: false,
            });
        }
        effects.push(Effect::ReleaseOwner {
            op,
            cell: id.to_string(),
            epoch,
        });
    }

    /// Give up to `count` hibernated cells to the fleet without draining.
    ///
    /// Only a dormant cell moves: its move is one record write and one
    /// signed acquire, it cannot park a request behind a durability proof,
    /// and the receiver restores it when traffic returns. A resident cell
    /// hibernates through the ordinary idle eviction first and moves after.
    /// The drain's eviction pipeline is not used here: a drain cannot
    /// abandon a cell whose final proof stalls, and a balancing move must.
    /// On the 2026-09-03 fleet that pipeline parked whales without end. A
    /// cell with a pending request or an alarm about to fire stays. Every
    /// move shares the drain's release ceiling, and a node that is draining,
    /// fenced, or shedding moves nothing because those paths own its cells.
    fn rebalance(&mut self, count: usize, effects: &mut Vec<Effect>) {
        if self.draining || self.fenced || self.shedding || !self.node_authoritative() {
            return;
        }
        let budget = count.min(
            self.config
                .max_releases
                .saturating_sub(self.handoffs_in_flight()),
        );
        let mut dormant: Vec<(CellId, Epoch, u64)> = self
            .cells
            .iter()
            .filter_map(|(id, cell)| match cell.phase {
                Phase::Dormant { epoch }
                    if cell.releasing.is_none()
                        && cell.requests.is_empty()
                        && !self.alarm_is_imminent(cell) =>
                {
                    Some((id.clone(), epoch, cell.last_used_mono_ms))
                }
                _ => None,
            })
            .collect();
        dormant.sort_by(|left, right| (left.2, &left.0).cmp(&(right.2, &right.0)));
        for (id, epoch, _) in dormant.into_iter().take(budget) {
            self.release_dormant(&id, epoch, effects);
        }
    }

    /// Does an eviction made for room hand the cell away?
    fn rebalances(&self) -> bool {
        self.config.ownership_on_evict == OwnershipOnEvict::Release
    }

    /// A drain may hold as many evictions as releases, because every
    /// resident cell leaves through one.
    fn eviction_ceiling(&self) -> usize {
        if self.draining {
            self.config.max_releases.max(self.config.max_evictions)
        } else {
            self.config.max_evictions
        }
    }

    /// An alarm about to fire is not worth an eviction: the wake costs
    /// more than the residency it would save, so the cell is held even
    /// though its entry is perfectly durable. Coverage says the alarm can
    /// survive the eviction; this says it is not worth surviving it.
    fn alarm_is_imminent(&self, cell: &Cell) -> bool {
        match cell.alarm {
            Some(AlarmState::Armed { at_ms, .. }) | Some(AlarmState::Firing { at_ms, .. }) => {
                at_ms >= 0
                    && (at_ms as u64).saturating_sub(self.now_ms) <= self.config.alarm_resident_ms
            }
            None => false,
        }
    }

    fn begin_eviction(&mut self, id: &str, rebalance: bool, effects: &mut Vec<Effect>) -> bool {
        let alarm_is_imminent = self
            .cells
            .get(id)
            .is_some_and(|cell| self.alarm_is_imminent(cell));
        // Only while there is room to spare. Holding a cell to save a wake is
        // worth it on an idle node and indefensible on a full one: the window
        // defaults to an hour, so on an alarm-driven workload this would hold
        // most of the node and pin it at its ceiling -- trading a real
        // admission failure for a saved activation. Under pressure the node
        // takes the wake.
        if alarm_is_imminent && !self.shedding && !self.draining {
            return false;
        }
        let alarm_is_safe = self.cells.get(id).is_some_and(|cell| {
            cell.alarm
                .as_ref()
                .is_none_or(|alarm| matches!(alarm, AlarmState::Armed { covered: true, .. }))
        });
        if !self.is_hibernatable(id) || !alarm_is_safe {
            return false;
        }
        // The permit is taken here, so the ceiling holds here. A balancing
        // batch reserves more cells than permits, and the pump promotes
        // them as permits return.
        if self.eviction_permits.len() >= self.eviction_ceiling() {
            return false;
        }
        let Some(Phase::Resident { epoch }) = self.cells.get(id).map(|cell| &cell.phase) else {
            return false;
        };
        let epoch = *epoch;
        let op = self.cell_op(id);
        let Some(cell) = self.cells.get_mut(id) else {
            return false;
        };
        cell.evict_rebalance = rebalance;
        set_phase(
            &mut self.occupied,
            cell,
            Phase::EnsuringDurability { op, epoch },
        );
        self.eviction_permits.insert(id.to_string());
        effects.push(Effect::EnsureDurable {
            op,
            cell: id.to_string(),
            epoch,
        });
        true
    }

    fn shed_one(&mut self, effects: &mut Vec<Effect>) {
        // Only when the blocker is headroom. A latched node is already walking
        // down on its own schedule, and admission stays closed until it
        // reaches the low watermark -- so shedding to make room for a waiter
        // that cannot be admitted has no stopping condition, and every
        // completed eviction re-enters here through `pump_capacity` and starts
        // another. That empties the node.
        //
        // Count the evictions already in flight against the waiters: this is
        // reachable from every activity finish and websocket close, and a
        // waiter whose eviction is already under way must not turn each of
        // those triggers into another victim. Spend the cut on commit, never
        // on nomination.
        if self.shedding
            || self.capacity_waiters.len() <= self.eviction_permits.len()
            || self.eviction_permits.len() >= self.config.max_evictions
        {
            return;
        }
        if let Some(victim) = self.shed_candidate() {
            self.begin_eviction(&victim, self.rebalances(), effects);
        }
    }

    /// The cell to shed right now: resident, idle, not holding an alarm the
    /// node still owes, and the least recently used of those. Demand shedding
    /// and pressure shedding both come through here, so they cannot disagree
    /// about what is safe to take or which one to take first.
    /// Bound the time a cell can sit behind the admission gate.
    ///
    /// Every other stall in a cold route is bounded by `operation_deadline_ms`
    /// through `watch_operations`, which arms per emitted effect. A parked
    /// cell has emitted none, so it is the one stall nothing expires, and a
    /// gate that closes on memory the queue itself holds can never reopen.
    fn watch_queued(&mut self, id: &str, cell: &mut Cell, effects: &mut Vec<Effect>) {
        let Some(deadline_ms) = self.config.operation_deadline_ms else {
            return;
        };
        let generation = self.next_timer_generation;
        self.next_timer_generation = self
            .next_timer_generation
            .checked_add(1)
            .expect("timer generation exhausted");
        cell.queued_generation = Some(generation);
        effects.push(Effect::ScheduleTimer {
            timer: Timer::QueuedActivation {
                cell: id.to_string(),
                generation,
            },
            at_mono_ms: self.now_mono_ms.saturating_add(deadline_ms),
        });
    }

    /// A cell that has waited out its deadline behind the gate.
    ///
    /// Answering `CapacityExhausted` is the existing refusal for "not here,
    /// not now": it already carries the stale-route header, so the caller
    /// re-resolves instead of retrying a node that cannot take it.
    fn expire_queued(&mut self, cell: &str, generation: u64, effects: &mut Vec<Effect>) {
        let Some(queued) = self.cells.get(cell) else {
            return;
        };
        if queued.queued_generation != Some(generation) {
            return;
        }
        if !matches!(
            queued.phase,
            Phase::WaitingActivation | Phase::WaitingCapacity
        ) {
            return;
        }
        for request in queued.requests.iter().copied().collect::<Vec<_>>() {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::CapacityExhausted),
            });
            self.request_cells.remove(&request);
            self.capacity_requests.remove(&request);
            if let Some(queued) = self.cells.get_mut(cell) {
                queued.requests.remove(&request);
            }
        }
        let Some(queued) = self.cells.get_mut(cell) else {
            return;
        };
        queued.queued_generation = None;
        // An alarm wake or a record confirmation is not a client waiting, so
        // it keeps the cell queued rather than being refused with the
        // requests. A clean-reload resume is retired here instead, so a slow
        // resume does not hold readiness.
        if queued.alarm_wake || queued.confirming {
            return;
        }
        self.activation_waiters.retain(|queued| queued != cell);
        self.capacity_waiters.retain(|queued| queued != cell);
        let Some(queued) = self.cells.get_mut(cell) else {
            return;
        };
        let start = queued.waiting_activation.take();
        let activation = queued.waiting_for.take();
        let dormant = match (start, activation) {
            (Some(ColdStart::Restore(spec)), _) => Some(spec.epoch),
            (_, Some(Activation::Restore(spec))) => Some(spec.epoch),
            _ => None,
        };
        let settles_resume = queued.resume_demand;
        set_phase(
            &mut self.occupied,
            queued,
            match dormant {
                Some(epoch) => Phase::Dormant { epoch },
                None => Phase::Inactive,
            },
        );
        // A saved restore has no caller that can retire its resume demand.
        // Leaving that demand indexed after this terminal timeout keeps the
        // complete node unready with no operation or timer that can clear it.
        if settles_resume {
            self.settle_local_resume(cell);
        }
    }

    fn shed_candidate(&self) -> Option<CellId> {
        self.shed_candidate_where(|_| true)
    }

    /// The shed victim among the cells `keep` accepts. The idle path
    /// passes a coldness filter here: without it, one warm cell in the
    /// emptiest isolate is the single candidate every sample, and idle
    /// eviction stalls while cold cells sit in fuller isolates.
    fn shed_candidate_where(&self, keep: impl Fn(&Cell) -> bool) -> Option<CellId> {
        let evictable = |id: &CellId, cell: &Cell| {
            self.is_hibernatable(id)
                && keep(cell)
                && cell
                    .alarm
                    .as_ref()
                    .is_none_or(|alarm| matches!(alarm, AlarmState::Armed { covered: true, .. }))
        };
        // How many cells each isolate still holds. The victim comes from the
        // one closest to empty, because only the cut that takes an isolate's
        // last cell gives the heap back -- and the heap is where the memory
        // is. Evicting by recency alone spreads the cuts over every isolate,
        // so the node gives up its working set and frees nothing.
        let mut occupancy: BTreeMap<isolate::HeapId, usize> = BTreeMap::new();
        for cell in self.cells.values() {
            if let Some(isolate) = cell.isolate {
                *occupancy.entry(isolate).or_default() += 1;
            }
        }
        self.cells
            .iter()
            .filter(|(id, cell)| evictable(id, cell))
            // Within an isolate the order is unchanged: never-refused first,
            // then by how long ago the refusal was, then least recently used,
            // with the id as a tiebreak so the choice is a function of the
            // state and not of map iteration order.
            //
            // A cell the shell never placed sorts last rather than first. It
            // has no heap to give back, so preferring it would reintroduce the
            // scatter this ordering exists to remove.
            .min_by_key(|(id, cell)| {
                let emptiest = cell
                    .isolate
                    .and_then(|isolate| occupancy.get(&isolate).copied())
                    .unwrap_or(usize::MAX);
                (
                    emptiest,
                    cell.eviction_refused_mono_ms,
                    cell.last_used_mono_ms,
                    (*id).clone(),
                )
            })
            .map(|(id, _)| id.clone())
    }

    /// Release one cell that has gone cold, with nothing asking for the room.
    ///
    /// Shedding answers "this node is in trouble"; this answers the ordinary
    /// question of a cell nobody has touched in a long time. Without it a node
    /// only ever gives a cell back under pressure or when another cell wants
    /// the slot, so a quiet node holds every cell it ever served until it
    /// reaches a watermark -- paying to keep runtimes alive for traffic that
    /// stopped hours ago.
    fn evict_idle(&mut self, now_mono_ms: u64, effects: &mut Vec<Effect>) {
        let Some(idle_ms) = self.config.idle_evict_ms else {
            return;
        };
        // Fill the permits, exactly as `shed_toward_floor` does. One
        // candidate per sample drains an idle backlog at the sampling
        // period: a few thousand parked hibernatable cells — the shape
        // of a game or chat fleet after activity stops — then keep
        // their isolates pinned for the better part of an hour, and an
        // autoscaler reading occupancy never sees the node go quiet.
        while self.eviction_permits.len() < self.config.max_evictions {
            // A refused eviction waits out another idle period. Retrying it
            // on every sample stops and restarts a cell whose proof cannot
            // complete for as long as that lasts.
            let Some(candidate) = self.shed_candidate_where(|cell| {
                now_mono_ms.saturating_sub(cell.last_used_mono_ms) >= idle_ms
                    && cell
                        .eviction_refused_mono_ms
                        .is_none_or(|refused| now_mono_ms.saturating_sub(refused) >= idle_ms)
            }) else {
                return;
            };
            // Idle eviction is a local residency decision, not a handoff.
            if !self.begin_eviction(&candidate, false, effects) {
                return;
            }
        }
    }

    /// Fold a resource sample into the shedding latch, then act on it.
    ///
    /// The latch is the whole point. Shedding on the instantaneous crossing
    /// alone flaps: the eviction relieves the pressure, admission resumes, the
    /// ceiling is crossed again. `PressureConfig` holds the node in shedding
    /// until every configured low watermark clears, so the node walks down to
    /// its target instead of oscillating around its ceiling.
    fn load_sampled(&mut self, load: pressure::Load, now_mono_ms: u64, effects: &mut Vec<Effect>) {
        self.memory_headroom = self.config.pressure.has_headroom(load);
        // Both latches are folded, and the reason names the more serious of the
        // two. The latches are state of their own: derived from the reason
        // instead, one crossing holds the node against the other's watermark.
        let (latches, reason) = self.config.pressure.classify(load, self.latches);
        self.latches = latches;
        self.shed_reason = reason;
        self.shedding = reason.is_some();
        if !self.shedding {
            // Relieved. Whatever was queued for capacity may proceed.
            self.shed_cut = None;
            self.pump_capacity(effects);
            self.evict_idle(now_mono_ms, effects);
            return;
        }
        // Evicting only helps if it can finish the job. Without a stopping
        // condition, a ceiling below the process's memory floor cuts a
        // proportion of whatever remains on every sample and walks the node to
        // zero. So the latch holds -- admission stays closed, the node is over
        // its ceiling -- while the walk down stops spending the working set.
        // `walk_metric` documents which measurement it reads and why. Changing
        // that measurement discards the baseline: the two numbers are not
        // comparable, and they sit close enough to find each other flat by
        // coincidence.
        let metric = pressure::PressureConfig::walk_metric(latches);
        let sample_bytes = load.metric_bytes(metric);
        if self.shed_cut.is_some_and(|cut| cut.metric != metric) {
            self.shed_cut = None;
        }
        if let Some(cut) = self.shed_cut {
            let cut_landed = self.occupied() <= self.shed_floor && self.eviction_permits.is_empty();
            // A rising sample re-arms the walk down: the projection was built
            // from a measurement that no longer describes the node, so take a
            // fresh baseline. The band absorbs the jitter of a live sample and
            // decides nothing about what a cut must achieve.
            let rose = sample_bytes > cut.bytes.saturating_add(cut.bytes / 100);
            if cut_landed && !rose && !self.shedding_can_reach_resume_line(cut, sample_bytes) {
                return;
            }
        }
        // How far this resource sample asks the node to come down: a proportion
        // of what was just measured, because the effect of an eviction on the
        // sample is not visible until the next one.
        self.shed_floor = pressure::PressureConfig::release_target(load.resident_cells);
        self.shed_cut = Some(ShedCut {
            metric,
            bytes: sample_bytes,
            cells: load.resident_cells,
        });
        self.shed_toward_floor(effects);
    }

    /// Is there a heap this walk down can still take back whole?
    ///
    /// True when some isolate holds only evictable cells and it is not the
    /// last one. The last one is excluded deliberately: emptying it means
    /// walking the node to zero, which is the case the stopping condition
    /// exists to prevent, and the node keeps one heap's worth of working set
    /// instead.
    ///
    /// A cell the shell never placed counts against nothing here, so a node
    /// with no placement reported falls through to the projection unchanged.
    fn can_empty_an_isolate(&self) -> bool {
        let mut evictable: BTreeMap<isolate::HeapId, bool> = BTreeMap::new();
        for (id, cell) in &self.cells {
            let Some(isolate) = cell.isolate else {
                continue;
            };
            let can_go = self.is_hibernatable(id)
                && cell
                    .alarm
                    .as_ref()
                    .is_none_or(|alarm| matches!(alarm, AlarmState::Armed { covered: true, .. }));
            *evictable.entry(isolate).or_insert(true) &= can_go;
        }
        evictable.len() > 1 && evictable.values().any(|all_evictable| *all_evictable)
    }

    /// Can shedding still get the sample down to the resume line?
    ///
    /// Do not replace this with a band on the last two samples, at any width.
    /// A cut is a proportion of the cells while the sample also holds memory no
    /// cell owns, so once that fixed part dominates, a cut that helps moves the
    /// sample by less than the band and the walk down stops above a line it
    /// could reach. That is #50, and 5% and 1% both had it.
    ///
    /// The yield is wanted honest, not corrected: a node that keeps part of a
    /// stopped cell really does return less per eviction, and holding the
    /// working set is the right answer there. A lagging sample under-reports
    /// the yield for a sample or two, and the walk down survives that because
    /// a stop takes no fresh baseline: the sample keeps falling against the
    /// bytes of the last cut until the memory that came back late has landed
    /// against it.
    fn shedding_can_reach_resume_line(&self, cut: ShedCut, sample_bytes: u64) -> bool {
        let Some(resume_line) = self.config.pressure.resume_line(cut.metric) else {
            // Unreachable: `walk_metric` only names a measurement whose ceiling
            // is configured. With no line, the safe answer is that shedding
            // buys nothing, so the caller keeps the working set.
            return false;
        };
        // The projection below prices the next cut from the last one, which
        // only holds while every cut is worth the same. It is not: a cut that
        // takes an isolate's last cell returns that heap, and the heap is
        // most of what a cell costs. So while a heap can still be emptied,
        // the last cut does not describe the next one and the projection has
        // nothing to say.
        if self.can_empty_an_isolate() {
            return true;
        }
        let removed = cut.cells.saturating_sub(self.occupied());
        if removed == 0 {
            // Only a node holding nothing arrives here. Reaching this call
            // means `occupied` is at or below `release_target(cut.cells)`,
            // which sits strictly below `cut.cells` for any residency but
            // zero, so zero is the one residency where a cut can vacate
            // nothing and still be measured. It vacated nothing because there
            // was nothing to vacate, so there is no yield to project and the
            // division below would be by zero.
            //
            // Such a node is over its ceiling on memory no cell owns, and it
            // holds no cell to spend on the crossing. The two answers here
            // therefore differ only in which sample the baseline remembers,
            // and neither changes what the node does. This one lets the walk
            // down carry on, the way the isolate arm above does.
            return true;
        }
        let returned = cut.bytes.saturating_sub(sample_bytes);
        // One division, at the end: a per-cell yield rounded to a whole number
        // first is zero for any cell worth less than a byte more than the
        // rounding, which reports every walk down on a large node as futile.
        let projected = u128::from(returned).saturating_mul(self.occupied() as u128)
            / u128::from(removed as u64);
        let floor = u128::from(sample_bytes).saturating_sub(projected);
        // Inclusive: `classify` holds the latch only *above* the low watermark,
        // so a projection landing exactly on the line reaches a node that
        // releases. A strict `<` here wedges that node.
        floor <= u128::from(resume_line)
    }

    /// Continue a latched walk down as each eviction lands.
    ///
    /// `shed_one` stands down while the latch is hot, because its stopping
    /// condition -- the waiter got in -- cannot be met until the node is
    /// relieved. This is the other half: a stopping condition that can be met,
    /// so the node reaches its floor at the speed evictions complete rather
    /// than one per sampling period. Serialized like every other eviction
    /// path, so a walk down never puts the whole working set in flight.
    fn shed_toward_floor(&mut self, effects: &mut Vec<Effect>) {
        if !self.shedding || self.occupied() <= self.shed_floor {
            return;
        }
        // Fill the permits rather than starting one and waiting for it. Each
        // proof is a round trip, so a serialized drain costs the number of
        // cells times that latency; running the bound's worth at once is the
        // difference between a walk down measured in seconds and one measured
        // in minutes.
        // Count what is already leaving against the target. `occupied` still
        // includes a cell whose proof is in flight, so comparing it directly
        // nominates cells the evictions already under way will account for,
        // and the node settles below its floor by up to the whole bound --
        // the mistake celld's eviction budget documents: spend the cut on
        // commit, never on nomination.
        while self.eviction_permits.len() < self.config.max_evictions
            && self.occupied().saturating_sub(self.eviction_permits.len()) > self.shed_floor
        {
            let Some(victim) = self.shed_candidate() else {
                return;
            };
            if !self.begin_eviction(&victim, self.rebalances(), effects) {
                return;
            }
        }
    }

    fn activity_finished(&mut self, request: RequestId, effects: &mut Vec<Effect>) {
        // A write still on the output gate keeps its request pinned, so the
        // cell cannot be evicted before the write is proven durable. The unpin
        // moves to whichever path drains the last gate for this request.
        if self.barriers.values().any(|barrier| {
            matches!(&barrier.owner, GateOwner::Output(held) if held.request == request)
                || barrier.followers.iter().any(|held| held.request == request)
        }) {
            self.gate_pinned.insert(request);
            return;
        }
        if let Some(id) = self.deactivate_request(request) {
            let now = self.now_mono_ms;
            if let Some(cell) = self.cells.get_mut(&id) {
                cell.last_used_mono_ms = now;
                cell.last_used_request = cell.last_used_request.max(request);
            }
        }
        self.shed_one(effects);
    }

    /// Discard a runtime whose durability could not be proved.
    ///
    /// The caller has been told its write failed. If the cell stays resident,
    /// the very next read is served from the same unreplicated memory and
    /// returns that write anyway -- the node disagreeing with what it just
    /// told a client. Stopping with [`StopCause::Reset`] keeps no local
    /// snapshot, so the next activation restores from the bucket.
    ///
    /// Note this is a stop, not a truncation. The restored database is not
    /// invalidated by a failed proof; only this node's unreplicated additions
    /// to it are in doubt, and re-reading durable state is how they are
    /// discarded.
    fn reset_cell(&mut self, id: &str, epoch: Epoch, effects: &mut Vec<Effect>) {
        if !matches!(
            self.cells.get(id).map(|cell| &cell.phase),
            Some(Phase::Resident { epoch: current }) if *current == epoch
        ) {
            return;
        }
        // Every other write still on the gate for this cell loses its runtime
        // here, so none of them may be acknowledged either.
        let doomed: Vec<OpId> = self
            .barriers
            .iter()
            .filter(|(_, barrier)| barrier.cell == id && barrier.epoch == epoch)
            .map(|(op, _)| *op)
            .collect();
        for op in doomed {
            let Some(gate) = self.barriers.remove(&op) else {
                continue;
            };
            match gate.owner {
                GateOwner::Output(held) => {
                    self.gate_pinned.remove(&held.request);
                    effects.push(Effect::Release {
                        request: held.request,
                        channel: held.channel,
                        result: Err(RequestError::DurabilityUnproven),
                    });
                }
                // This proof can no longer succeed against a runtime being
                // discarded, so the alarm settles as a failed handler's does:
                // re-armed, with the wake entry left in place for the next
                // activation to find.
                GateOwner::Alarm { alarm, .. } => {
                    let (now_ms, now_mono_ms) = (self.now_ms, self.now_mono_ms);
                    self.alarm_finished(
                        alarm,
                        (gate.cell.clone(), gate.epoch),
                        now_ms,
                        now_mono_ms,
                        Err(Failure::Ambiguous),
                        effects,
                    );
                }
            }
            for held in gate.followers {
                effects.push(Effect::Release {
                    request: held.request,
                    channel: held.channel,
                    result: Err(RequestError::DurabilityUnproven),
                });
            }
        }
        // Requests already running against this runtime lose it here, exactly
        // as a fence does. Leaving them in `active_requests` would leave the
        // core believing a stopped cell is still serving, which its own
        // invariant catches.
        let running: Vec<RequestId> = self
            .active_requests
            .iter()
            .filter(|(_, cell)| cell.as_str() == id)
            .map(|(request, _)| *request)
            .collect();
        for request in running {
            self.deactivate_request(request);
            self.gate_pinned.remove(&request);
        }
        let op = self.cell_op(id);
        if let Some(cell) = self.cells.get_mut(id) {
            // The phase being replaced may hold an op that will now never
            // complete against it; its index entry must not outlive it.
            if let Some(stale) = phase_op(&cell.phase) {
                self.cell_ops.remove(&stale);
            }
            set_phase(
                &mut self.occupied,
                cell,
                Phase::Cleaning {
                    op,
                    epoch,
                    cause: StopCause::Reset,
                },
            );
        }
        effects.push(Effect::StopRuntime {
            op,
            cell: id.to_string(),
            epoch,
            cause: StopCause::Reset,
            bounded: false,
        });
    }

    /// How many cells are currently held resident by a non-hibernatable
    /// transport.
    fn outbound_pinned(&self) -> usize {
        self.cells
            .values()
            .filter(|cell| {
                cell.websockets
                    .values()
                    .any(|kind| *kind == WebSocketKind::Outbound)
            })
            .count()
    }

    fn websocket_opened(
        &mut self,
        id: &str,
        websocket: WebSocketId,
        kind: WebSocketKind,
        effects: &mut Vec<Effect>,
    ) {
        if self.fenced {
            return;
        }
        // A non-hibernatable transport holds its cell resident for as long as
        // it is open, so every one of them is a cell the node can never shed.
        // Pin the whole ceiling and there is nothing left to nominate:
        // residency cannot fall, admission waits on capacity that will never
        // be freed, and the node is wedged by its own applications. Cells that
        // already hold one are not counted again -- the budget is on how much
        // of the node is held, not on how many sockets exist.
        let already_pinned = self.cells.get(id).is_some_and(|cell| {
            cell.websockets
                .values()
                .any(|kind| *kind == WebSocketKind::Outbound)
        });
        let cell_outbound = self.cells.get(id).map_or(0, |cell| {
            cell.websockets
                .values()
                .filter(|kind| **kind == WebSocketKind::Outbound)
                .count()
        });
        if kind == WebSocketKind::Outbound
            && (cell_outbound >= self.config.max_outbound_websockets
                || (!already_pinned
                    && !pressure::may_pin_outbound(
                        self.outbound_pinned(),
                        Some(self.config.max_resident),
                    )))
        {
            effects.push(Effect::CloseWebSocket {
                cell: id.to_string(),
                websocket,
            });
            return;
        }
        let Some(cell) = self.cells.get_mut(id) else {
            return;
        };
        cell.websockets.insert(websocket, kind);
    }

    fn websocket_closed(&mut self, id: &str, websocket: WebSocketId, effects: &mut Vec<Effect>) {
        if let Some(cell) = self.cells.get_mut(id) {
            cell.websockets.remove(&websocket);
        }
        self.shed_one(effects);
    }

    fn invalidate_remote(&mut self, id: &str, node: &str, epoch: Epoch) {
        self.node_lease_cache.remove(node);
        let rejected_sample = self.cells.get(id).and_then(|cell| match &cell.phase {
            Phase::Remote {
                node: current,
                epoch: current_epoch,
                capacity_sampled_ms,
                ..
            } if current == node && *current_epoch == epoch => *capacity_sampled_ms,
            _ => None,
        });
        if let Some(sample) = rejected_sample {
            self.capacity_rejections.insert(node.to_string(), sample);
            self.capacity_reservations.remove(node);
        }
        let Some(cell) = self.cells.get_mut(id) else {
            return;
        };
        if matches!(
            &cell.phase,
            Phase::Remote {
                node: current,
                epoch: current_epoch,
                ..
            } if current == node && *current_epoch == epoch
        ) {
            set_phase(&mut self.occupied, cell, Phase::Inactive);
        }
    }

    fn fence(&mut self, effects: &mut Vec<Effect>) {
        if self.fenced {
            return;
        }
        self.fenced = true;
        self.activation_waiters.clear();
        self.activation_permits.clear();
        self.capacity_waiters.clear();
        self.quiescing_cells.clear();
        self.adopting_cells.clear();
        self.active_requests.clear();
        self.active_cells.clear();
        // Any write still waiting on the output gate loses its cell here, so it
        // must fail rather than be acknowledged — the fence and the fail are
        // atomic. A late DurableReached for a drained op is ignored.
        self.gate_pinned.clear();
        for (_, gate) in std::mem::take(&mut self.barriers) {
            // An alarm has no output to fail, and unlike `reset_cell` it is not
            // replayed either. The per-cell teardown below retires its firing
            // op and clears the alarm, and the wake entry is left where it is,
            // so the node that takes the cell next discovers it and fires
            // again.
            if let GateOwner::Output(held) = gate.owner {
                effects.push(Effect::Release {
                    request: held.request,
                    channel: held.channel,
                    result: Err(RequestError::NodeFenced),
                });
            }
            for held in gate.followers {
                effects.push(Effect::Release {
                    request: held.request,
                    channel: held.channel,
                    result: Err(RequestError::NodeFenced),
                });
            }
        }
        let ids: Vec<CellId> = self.cells.keys().cloned().collect();
        for id in ids {
            let mut cell = self.cells.remove(&id).expect("id came from map");
            match &cell.phase {
                Phase::Starting { op, epoch } | Phase::Publishing { op, epoch } => {
                    self.timed_out_runtime_starts.remove(op);
                    self.retired_runtime_ops.insert(*op, (id.clone(), *epoch));
                }
                _ => {}
            }
            if let Some(epoch) = runtime_epoch(&cell.phase) {
                let op = self.op();
                effects.push(Effect::StopRuntime {
                    op,
                    cell: id.clone(),
                    epoch,
                    cause: StopCause::Fence,
                    bounded: false,
                });
            }
            self.finish_requests(&id, &mut cell, Err(RequestError::NodeFenced), effects);
            // The phase and alarm die here without completing; their ops can
            // never match a lookup again. The release op survives on purpose:
            // `owner_released` still consumes it after a fence, as it did
            // when resolution was a scan.
            if let Some(stale) = phase_op(&cell.phase) {
                self.cell_ops.remove(&stale);
            }
            if let Some(AlarmState::Firing { op, .. }) = cell.alarm {
                self.cell_ops.remove(&op);
            }
            set_phase(&mut self.occupied, &mut cell, Phase::Fenced);
            cell.waiting_for = None;
            cell.waiting_activation = None;
            cell.alarm = None;
            cell.alarm_wake = false;
            cell.websockets.clear();
            self.cells.insert(id, cell);
        }
    }

    fn compensate_retired_runtime(
        &mut self,
        op: OpId,
        result: Result<(), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some((cell, epoch)) = self.retired_runtime_ops.remove(&op) else {
            return;
        };
        // Definite failure created nothing. Success or ambiguity may have
        // created/published a runtime after authority was revoked, so cleanup
        // is mandatory and idempotent.
        if result != Err(Failure::Definite) {
            let cleanup = self.op();
            effects.push(Effect::StopRuntime {
                op: cleanup,
                cell,
                epoch,
                // This completion lost node authority before it became
                // visible. It can close the database, but it cannot run the
                // durability pass that an ordinary live cleanup uses to make
                // the image a successor-epoch base.
                cause: StopCause::Fence,
                bounded: false,
            });
        }
    }
}

/// The sole state transition entry point.
/// The monotonic instant an event carries, if it carries one. Events without
/// a timestamp leave the remembered instant alone rather than resetting it.
/// The wall-clock reading an event carried, if it carried one.
fn event_now_ms(event: &Event) -> Option<u64> {
    match event {
        Event::StartNodeLease { now_ms, .. }
        | Event::SelfNodeLeaseRead { now_ms, .. }
        | Event::OwnerRead { now_ms, .. }
        | Event::NodeLeaseRead { now_ms, .. }
        | Event::TimerFired { now_ms, .. }
        | Event::AlarmObserved { now_ms, .. }
        | Event::AlarmFinished { now_ms, .. } => Some(*now_ms),
        _ => None,
    }
}

fn event_mono_ms(event: &Event) -> Option<u64> {
    match event {
        Event::StartNodeLease { now_mono_ms, .. }
        | Event::SelfNodeLeaseRead { now_mono_ms, .. }
        | Event::NodeLeaseCasCompleted { now_mono_ms, .. }
        | Event::RequestAt { now_mono_ms, .. }
        | Event::CapacityRequestAt { now_mono_ms, .. }
        | Event::HandoffRequestAt { now_mono_ms, .. }
        | Event::WebSocketRequestAt { now_mono_ms, .. }
        | Event::OutputAt { now_mono_ms, .. }
        | Event::WakeHintAt { now_mono_ms, .. }
        | Event::TimerFired { now_mono_ms, .. }
        | Event::AlarmObserved { now_mono_ms, .. }
        | Event::AlarmFinished { now_mono_ms, .. }
        | Event::LoadSampled { now_mono_ms, .. }
        | Event::GenerationChanged { now_mono_ms, .. } => Some(*now_mono_ms),
        _ => None,
    }
}

pub fn on_event(state: &mut State, event: Event) -> Vec<Effect> {
    let mut effects = Vec::new();
    if let Some(now_mono_ms) = event_mono_ms(&event) {
        state.now_mono_ms = state.now_mono_ms.max(now_mono_ms);
    }
    if let Some(now_ms) = event_now_ms(&event) {
        state.now_ms = state.now_ms.max(now_ms);
    }
    match event {
        Event::StartNodeLease { now_ms, spec, .. } => {
            state.start_node_lease(now_ms, spec, &mut effects)
        }
        Event::NodeLogRecovered { op, result } => {
            state.node_log_recovered(op, result, &mut effects)
        }
        Event::NudgeNodeLease {
            now_ms,
            now_mono_ms,
        } => match &state.node_authority {
            NodeAuthority::Held(held) => {
                let generation = held.timer_generation;
                state.timer_fired(
                    Timer::NodeLeaseRenew { generation },
                    now_ms,
                    now_mono_ms,
                    &mut effects,
                );
            }
            // A write is in flight (or a read preceding one): its body was
            // serialized before this nudge's publish, so it cannot confirm
            // it. Remember, and hold_node_lease renews again on arrival.
            NodeAuthority::Writing { .. } | NodeAuthority::Reading { .. } => {
                state.nudge_pending = true;
            }
            _ => {}
        },
        Event::SelfNodeLeaseRead {
            op,
            now_ms,
            now_mono_ms,
            result,
        } => state.read_self_node_lease(op, now_ms, now_mono_ms, result, &mut effects),
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms,
            result,
            stamped_log_state,
        } => {
            state.node_lease_cas_completed(op, now_mono_ms, result, stamped_log_state, &mut effects)
        }
        Event::LocalCellsRead { result } => state.local_cells_read(result, &mut effects),
        Event::TimerFired {
            timer,
            now_ms,
            now_mono_ms,
        } => state.timer_fired(timer, now_ms, now_mono_ms, &mut effects),
        Event::Request { request, cell } => {
            state.request(request, cell, false, false, &mut effects)
        }
        Event::RequestAt { request, cell, .. } => {
            state.request(request, cell, false, false, &mut effects)
        }
        Event::CapacityRequestAt { request, cell, .. } => {
            state.request(request, cell, true, false, &mut effects)
        }
        Event::HandoffRequestAt { request, cell, .. } => {
            state.request(request, cell, true, true, &mut effects)
        }
        Event::WebSocketRequestAt {
            request,
            cell,
            websocket,
            ..
        } => state.websocket_request(request, cell, websocket, &mut effects),
        Event::WorkerRequest { request } => state.worker_request(request, &mut effects),
        Event::BeginPreserve => state.begin_preserve(&mut effects),
        Event::Cancel { request } => state.cancel(request, &mut effects),
        Event::ActivityFinished { request } => state.activity_finished(request, &mut effects),
        Event::Output {
            request,
            channel,
            position,
            observed,
            epoch,
        } => state.output(request, channel, position, observed, epoch, &mut effects),
        Event::OutputAt {
            request,
            channel,
            position,
            observed,
            epoch,
            ..
        } => state.output(request, channel, position, observed, epoch, &mut effects),
        Event::DurableReached { op, result, source } => {
            state.durable_reached(op, result, source, &mut effects)
        }
        Event::OwnershipVerified { op, result } => {
            state.ownership_verified(op, result, &mut effects)
        }
        Event::WebSocketOpened {
            cell,
            websocket,
            kind,
        } => state.websocket_opened(&cell, websocket, kind, &mut effects),
        Event::WebSocketClosed { cell, websocket } => {
            state.websocket_closed(&cell, websocket, &mut effects)
        }
        Event::AlarmObserved {
            cell,
            at_ms,
            covered,
            now_ms,
            now_mono_ms,
        } => state.alarm_observed(
            &cell,
            AlarmObservation {
                at_ms,
                covered,
                firing_settled: false,
            },
            now_ms,
            now_mono_ms,
            &mut effects,
        ),
        Event::AlarmFinished {
            op,
            cell,
            epoch,
            now_ms,
            now_mono_ms,
            result,
        } => state.alarm_finished(op, (cell, epoch), now_ms, now_mono_ms, result, &mut effects),
        Event::WakeHint {
            cell,
            entry_ms,
            scope,
        } => state.wake_hint(cell, entry_ms, scope, &mut effects),
        Event::WakeHintAt {
            cell,
            entry_ms,
            scope,
            ..
        } => state.wake_hint(cell, entry_ms, scope, &mut effects),
        Event::OwnerRead { op, now_ms, result } => {
            state.owner_read(op, now_ms, result, &mut effects)
        }
        Event::NodeLeaseRead { op, now_ms, result } => {
            state.node_lease_read(op, now_ms, result, &mut effects)
        }
        Event::CapacityPeersRead { op, now_ms, result } => {
            state.capacity_peers_read(op, now_ms, result, &mut effects)
        }
        Event::OwnerCasCompleted { op, result } => {
            state.owner_cas_completed(op, result, &mut effects)
        }
        Event::OwnerReleased { op, result } => state.owner_released(op, result, &mut effects),
        Event::SuccessorAdopted { op, result } => state.successor_adopted(op, result, &mut effects),
        Event::RestoreCompleted { op, result } => state.restore_completed(op, result, &mut effects),
        Event::RuntimeStarted {
            op,
            isolate,
            generation,
            result,
        } => state.runtime_started(op, isolate, generation, result, &mut effects),
        Event::GenerationChanged {
            generation,
            max_age_ms,
            eager_classes,
            ..
        } => state.generation_changed(generation, max_age_ms, &eager_classes),
        Event::Published { op, result } => state.published(op, result, &mut effects),
        Event::DurabilityChecked { op, result } => {
            state.durability_checked(op, result, &mut effects)
        }
        Event::RuntimeStopped { op } => state.runtime_stopped(op, &mut effects),
        Event::RuntimeStopFailed { op } => state.runtime_stop_failed(op, &mut effects),
        Event::Evict { cell } => state.evict(&cell, &mut effects),
        Event::LoadSampled { load, now_mono_ms } => {
            state.load_sampled(load, now_mono_ms, &mut effects)
        }
        Event::InvalidateRemote { cell, node, epoch } => {
            state.invalidate_remote(&cell, &node, epoch)
        }
        Event::NodeFenced => state.fence_node(HaltReason::NodeLeaseExpired, &mut effects),
        Event::ReleaseAll => state.release_all(&mut effects),
        Event::Rebalance { cells } => state.rebalance(cells, &mut effects),
    }
    state.pump_activations(&mut effects);
    state.pump_release(&mut effects);
    state.pump_swap(&mut effects);
    if cfg!(debug_assertions) {
        state.validate().expect("state invariant");
    }
    state.arm_operation_deadlines(&mut effects);
    effects
}

fn is_queue_broker(id: &str) -> bool {
    id.split_once(':')
        .is_some_and(|(class, _)| class == queue::RESERVED_CLASS)
}

fn same_node_lease(left: &NodeLeaseRecord, right: &NodeLeaseRecord) -> bool {
    left.node == right.node
        && left.addr == right.addr
        && left.expires_ms == right.expires_ms
        && left.peer_protocol == right.peer_protocol
        && left.generation == right.generation
        // The folded log state distinguishes "our record, untouched" from
        // "our record, fenced to recovering by a peer":
        // ignoring it let an ambiguous renewal's readback adopt recovery's
        // etag and the NEXT renewal un-fenced the recovery. Any difference
        // here is authority loss.
        && left.log_state == right.log_state
}

fn phase_occupies_capacity(phase: &Phase) -> bool {
    matches!(
        phase,
        Phase::Acquiring { dormant: false, .. }
            | Phase::ReconcilingAcquire { dormant: false, .. }
            | Phase::Restoring { .. }
            | Phase::Starting { .. }
            | Phase::Publishing { .. }
            | Phase::EnsuringDurability { .. }
            | Phase::Cleaning { .. }
            | Phase::Resident { .. }
    )
}

fn phase_holds_activation(phase: &Phase) -> bool {
    matches!(
        phase,
        Phase::ReadingOwner { .. }
            | Phase::ReadingNodeLease { .. }
            | Phase::RecoveringOwnerLog { .. }
            | Phase::ReadingCapacity { .. }
            | Phase::WaitingCapacity
            | Phase::Acquiring { .. }
            | Phase::ReconcilingAcquire { .. }
            | Phase::Restoring { .. }
            | Phase::Starting { .. }
            | Phase::Publishing { .. }
            | Phase::Cleaning { .. }
    )
}

/// Identify phases that cannot exist outside one admitted cold route.
///
/// `Starting` and `Publishing` also serve a live generation swap, so the swap
/// flag disambiguates them. `Cleaning` has cold and resident entry points and
/// is therefore not a safe converse even though it retains an existing permit.
fn phase_requires_activation_permit(phase: &Phase, swapping: bool) -> bool {
    matches!(
        phase,
        Phase::ReadingOwner { .. }
            | Phase::ReadingNodeLease { .. }
            | Phase::RecoveringOwnerLog { .. }
            | Phase::ReadingCapacity { .. }
            | Phase::WaitingCapacity
            | Phase::Acquiring { .. }
            | Phase::ReconcilingAcquire { .. }
            | Phase::Restoring { .. }
    ) || (!swapping && matches!(phase, Phase::Starting { .. } | Phase::Publishing { .. }))
}

/// The one gate every phase change passes through: it keeps `occupied`
/// equal to the number of cells in a capacity-occupying phase, so nothing
/// ever has to count them. `validate` asserts the equality by walking.
fn set_phase(occupied: &mut usize, cell: &mut Cell, phase: Phase) {
    if !matches!(&phase, Phase::Resident { .. }) {
        cell.quiescing = false;
    }
    // Ownership follows the confirmed phases and survives the transient
    // wake phases between them; a phase that names another owner, no owner,
    // or a released record ends it.
    match (&phase, phase_owned_epoch(&phase)) {
        (_, Some(epoch)) => cell.held_epoch = Some(epoch),
        (Phase::Inactive | Phase::Remote { .. } | Phase::Fenced | Phase::Adopting { .. }, None) => {
            cell.held_epoch = None
        }
        _ => {}
    }
    // A proof covers positions of one runtime: the next runtime opens a new
    // connection whose positions start over, at the same epoch after a
    // generation swap. Every residency starts with no proof on record, so a
    // stale one cannot cover a new runtime's commit; a residency that kept
    // its runtime, as one a failed eviction returns to, proves once more.
    if matches!(&phase, Phase::Resident { .. }) {
        cell.proven_position = None;
    }
    // A swap lives on exactly these phases. Any other change — an eviction
    // that won the race, a fence, a failed start — ends it, and the cell
    // picks the current generation on its next activation instead.
    if !matches!(
        &phase,
        Phase::Resident { .. }
            | Phase::Cleaning {
                cause: StopCause::Swap,
                ..
            }
            | Phase::Starting { .. }
            | Phase::Publishing { .. }
    ) {
        cell.swapping = false;
        cell.swap_cancelled = false;
    }
    match (
        phase_occupies_capacity(&cell.phase),
        phase_occupies_capacity(&phase),
    ) {
        (false, true) => *occupied += 1,
        (true, false) => *occupied -= 1,
        _ => {}
    }
    cell.phase = phase;
}

/// The in-flight operation a phase is waiting on, if it is waiting on one.
fn phase_op(phase: &Phase) -> Option<OpId> {
    match phase {
        Phase::ReadingOwner { op }
        | Phase::ReadingNodeLease { op, .. }
        | Phase::RecoveringOwnerLog { op, .. }
        | Phase::ReadingCapacity { op, .. }
        | Phase::Acquiring { op, .. }
        | Phase::ReconcilingAcquire { op, .. }
        | Phase::Restoring { op, .. }
        | Phase::Starting { op, .. }
        | Phase::Publishing { op, .. }
        | Phase::EnsuringDurability { op, .. }
        | Phase::Cleaning { op, .. }
        | Phase::Adopting { op, .. } => Some(*op),
        _ => None,
    }
}

/// Return the epoch only after this node has confirmed its ownership CAS.
/// Keep the census and handoff acknowledgement on this one classification so
/// a new owned phase cannot silently affect routing without affecting load.
fn phase_owned_epoch(phase: &Phase) -> Option<Epoch> {
    match phase {
        Phase::Restoring { spec, .. } => Some(spec.epoch),
        Phase::Starting { epoch, .. }
        | Phase::Publishing { epoch, .. }
        | Phase::EnsuringDurability { epoch, .. }
        | Phase::Cleaning { epoch, .. }
        | Phase::Dormant { epoch }
        | Phase::Resident { epoch } => Some(*epoch),
        _ => None,
    }
}

fn runtime_epoch(phase: &Phase) -> Option<Epoch> {
    match phase {
        Phase::Publishing { epoch, .. }
        | Phase::EnsuringDurability { epoch, .. }
        | Phase::Cleaning { epoch, .. }
        | Phase::Resident { epoch } => Some(*epoch),
        _ => None,
    }
}
