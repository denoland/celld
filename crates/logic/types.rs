// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The vocabulary the core is written in: what it is told, what it
//! answers with, and the records it reads and writes.
//!
//! Everything here is data. `Event` is the only way in, `Effect` the only
//! way out, and the rest are the shapes those two carry. No decision is
//! made in this file — that is `State` in the crate root.
use super::*;

pub type Ms = i64;

pub type CellId = String;
pub type NodeId = String;
pub type RequestId = u64;
pub type WebSocketId = u64;
pub type OpId = u64;
pub type Epoch = u64;

/// One runtime that this node can currently serve locally.
///
/// Presence is a read-only projection of the decision core, not a second
/// inventory maintained by an adapter. Keeping the epoch beside the cell ID
/// lets management and inspection traffic identify the exact fenced runtime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresenceCell {
    pub id: CellId,
    pub epoch: Epoch,
}

/// Cumulative lifecycle decisions exposed to management from the same state
/// machine that made them. These are advisory counters, but they are not a
/// second shell-owned model and replay to the same values for the same events.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ActivitySnapshot {
    pub acquired: u64,
    pub proxied: u64,
    pub expired_owner_leases: u64,
    pub restored: u64,
    pub advanced_epochs: u64,
    /// Shutdown handoffs acknowledged after a successor acquired authority.
    pub handed_off: u64,
    /// Handoff operations that the executor explicitly rejected. A process
    /// deadline can terminate before this counter changes.
    pub handoff_failed: u64,
    /// Balancing moves a peer acquired. A subset of `handed_off`.
    pub rebalanced: u64,
    /// Balancing moves no peer acquired. A subset of `handoff_failed`; the
    /// cell stayed unowned and its next request re-reads ownership.
    pub rebalance_failed: u64,
}

/// Management-facing lifecycle state derived atomically from [`State`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresenceSnapshot {
    pub serving: bool,
    pub cells: Vec<PresenceCell>,
    pub activity: ActivitySnapshot,
}

impl PresenceSnapshot {
    pub fn owned_cells(&self) -> usize {
        self.cells.len()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Resident cells plus activation reservations may never exceed this.
    pub max_resident: usize,
    /// Complete nonresident routes which may be in flight at once.
    ///
    /// This is deliberately independent of the stateless Worker pool. A warm
    /// request consumes no activation slot, while a cold request holds one
    /// across ownership resolution, capacity waiting, restore, and publish.
    pub max_activations: usize,
    /// Evictions that may hold a durability proof in flight at once.
    ///
    /// A proof is a round trip to the bucket, so draining a node one cell at a
    /// time takes the number of cells times that latency -- a node shedding
    /// five hundred cells against a two hundred millisecond proof refuses
    /// admission for a minute and a half while it walks down. Bounded
    /// concurrency is what makes the walk down finish in a time anyone can
    /// reason about.
    pub max_evictions: usize,
    /// Complete cell handoffs in flight at once during shutdown.
    ///
    /// `max_evictions` is tuned for a background walk down beside live
    /// traffic. This permit remains held through durability, release, and
    /// successor ownership acceptance. The successor's activation ceiling
    /// bounds the asynchronous restore wave after acceptance.
    pub max_releases: usize,
    /// Concurrent outbound WebSockets one cell may hold.
    ///
    /// Distinct from the node-wide pin budget, which counts *cells* held
    /// resident: one socket is enough to pin a cell, so that budget says
    /// nothing about how many a single cell may open. This bounds what one
    /// application can consume on its own behalf.
    pub max_outbound_websockets: usize,
    /// What an evicted cell's ownership record should say.
    ///
    /// Releasing it lets any node take the cell next, which is what makes a
    /// loaded node shed load rather than merely stop hosting it: keeping the
    /// record means every later request for that cell still routes here, to a
    /// node that already decided it has no room. Keeping it is right when the
    /// local eviction snapshot is the point, because a same-node wake is a
    /// rename instead of a restore.
    pub ownership_on_evict: OwnershipOnEvict,
    /// Production executors require a live self-node lease before serving.
    /// Deterministic unit slices which do not exercise node authority can
    /// disable this explicitly.
    pub require_node_lease: bool,
    /// Exact cross-node request protocol this process can authenticate and
    /// understand. A live owner speaking another version is unavailable, not
    /// stale: incompatibility never authorizes takeover.
    pub peer_protocol: u16,
    /// How long an activation effect may remain outstanding before the core
    /// stops waiting for it.
    ///
    /// Without this a swallowed effect is invisible: no event ever arrives, no
    /// timer is watching, and the request waits forever while every piece of
    /// state remains perfectly consistent. celld shipped that and parked
    /// requests past ninety seconds. `None` restores the old behaviour, which
    /// callers use only when they need to observe an indefinite wait.
    pub operation_deadline_ms: Option<u64>,
    /// How long a cell waits after a failed dead-owner log recovery
    /// before it re-enters ownership resolution.
    ///
    /// A recovery fails precisely when the owner just died with a deep
    /// open log: the survivors' sweep of that log is what the request
    /// is really waiting for, and a thousand-cell owner measured
    /// ~155 s of it. Failing the request instead turns that window
    /// into minutes of hard errors.
    pub owner_log_recovery_backoff_ms: u64,
    /// How many failed recovery cycles a cell may retry before its
    /// requests fail with `ResolveFailed`.
    ///
    /// A bound, not a target: the shell's own request lifetimes cut a
    /// waiting client far earlier. At the default backoff this covers
    /// the measured sweep above with a wide margin, so the cap only
    /// fires on a recovery that is truly wedged.
    pub owner_log_recovery_attempts: u32,
    /// How close an armed alarm may be before the cell stops being worth
    /// evicting. Inside this window the wake would cost more than the
    /// residency it saves, so the cell is held.
    pub alarm_resident_ms: u64,
    /// How long a cell may sit unused before the node gives it back, with no
    /// pressure involved. `None` keeps every cell resident until something
    /// needs the room.
    pub idle_evict_ms: Option<u64>,
    /// Ceilings and low watermarks for load shedding.
    ///
    /// `max_resident` is a hard cap on reservations; this is the softer,
    /// resource-aware question of whether the node is overloaded at all.
    /// Without it a node has only a cell count to reason about, so it meets
    /// memory exhaustion by running into it rather than shedding.
    pub pressure: pressure::PressureConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnerRecord {
    /// `None` is a deliberately released, fenced record. Epochs never reset.
    pub node: Option<NodeId>,
    pub epoch: Epoch,
    pub etag: String,
}

/// The routing and authority fields read from `nodes/<node>.json`.
///
/// The executor samples wall time and returns the verbatim record. The core,
/// not the storage adapter, decides whether it is live and routable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLeaseRecord {
    pub node: NodeId,
    pub addr: String,
    pub expires_ms: u64,
    pub peer_protocol: u16,
    /// Per-process generation; production stores this in
    /// `ownership_index_generation` and, as the same value, in
    /// `probe_public_key`.
    pub generation: String,
    /// The folded node-log state: `None` means this session never opened
    /// a fleet log — it never acked past the bucket. Anything not sealed
    /// gates a takeover of this node's cells behind node-log recovery.
    pub log_state: Option<crate::log_tier::LogState>,
    /// Object version observed by the read. Empty only in synthetic events.
    pub etag: String,
}

/// One advisory fleet-capacity observation returned by the storage shell.
///
/// Membership and expiry remain authoritative node-lease facts. Load is only
/// used to choose where an unowned cell should try to land; the chosen peer
/// must still atomically admit the handoff before it acquires ownership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapacityPeer {
    pub node: NodeId,
    pub addr: String,
    pub expires_ms: u64,
    pub peer_protocol: u16,
    pub sampled_ms: u64,
    /// Confirmed ownership held by this peer, including dormant cells.
    /// `None` from a peer that predates this field.
    pub owned_cells: Option<usize>,
    /// The share of fleet ownership this peer wants, relative to the other
    /// weights. `None` from a peer that predates rebalancing.
    pub placement_weight: Option<u64>,
    /// The newest bucket format this peer reads (`crate::format`). `None`
    /// from a peer that predates the field, which reads format 1.
    pub bucket_format: Option<u16>,
    pub resident_cells: usize,
    pub host_websockets: usize,
    pub rss_bytes: u64,
    /// The allocator-adjusted RSS fallback for ordinary memory pressure.
    /// `None` from a peer that predates the field.
    pub in_use_bytes: Option<u64>,
    pub pressured: bool,
    /// Whether every configured memory measurement is below its low
    /// watermark. `None` from a peer that predates this field.
    pub memory_headroom: Option<bool>,
    /// Cold routes that have not finished on this peer.
    pub restoring: u64,
    /// This process accepts a signed adoption request and acknowledges it
    /// after it acquires the cell's next ownership epoch.
    pub paced_handoff: bool,
    /// An operator paused balancing on this node. One paused lease stops
    /// every move in the fleet, so a pause from any node is fleet-wide.
    pub rebalance_paused: bool,
    /// This node is handing its cells away. It is not a receiver: for one
    /// lease lifetime after it empties it would otherwise look like the
    /// emptiest node in the fleet, and every cell sent to it fails its
    /// adoption or leaves with the drain.
    pub draining: bool,
}

/// A successor that has acquired a released cell.
///
/// The executor obtains this only from a signed peer acknowledgement. The
/// successor can still be restoring the durable replica when it responds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdoptedCell {
    pub node: NodeId,
    pub addr: String,
    pub epoch: Epoch,
    pub peer_protocol: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeLeaseSpec {
    pub addr: String,
    pub peer_protocol: u16,
    pub generation: String,
    /// The generation named by a clean local reload certificate. An initial
    /// lease CAS can resume local cells only when it replaces this exact,
    /// still-live generation.
    pub resume_generation: Option<String>,
    pub ttl_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasGuard {
    Absent,
    Match(String),
}

/// See [`Config::ownership_on_evict`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OwnershipOnEvict {
    /// Publish the cell as unowned so any node may take it next.
    #[default]
    Release,
    /// Keep the record, so a same-node wake can reuse the local snapshot.
    Sticky,
}

/// Why the node is halting. The shell writes this down: a process that
/// self-fences and exits without saying why leaves an operator with an exit
/// code and nothing else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HaltReason {
    /// The node lease was not renewed inside its TTL, so this node can no
    /// longer prove it owns anything it is serving.
    NodeLeaseExpired,
    /// A renewal readback found no node lease record, so the record that
    /// published the authority of this process is gone.
    NodeLeaseMissing,
    /// A renewal readback found a node lease record that this process can
    /// neither recognize nor adopt, so it can no longer prove its authority.
    /// The record can be a foreign replacement, or a late self-authored write
    /// that carries a stale folded log stamp; the readback cannot tell them
    /// apart, so the reason names the observation, not an author.
    NodeLeaseMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CasOutcome {
    Applied,
    Rejected,
}

/// Observable result of a successful restore effect. A non-fresh activation
/// may still discover that no local or replicated database exists; the
/// adapter reports what happened instead of making the core infer I/O truth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreOutcome {
    pub restored: bool,
    /// The alarm the restored database already had armed.
    ///
    /// A cell carries its alarm in its own SQLite, so a cell that arrives
    /// here from another node — or wakes cold — has one the isolate has not
    /// re-armed yet. Nothing else tells this node about it: the observer
    /// fires when a *running* isolate calls `setAlarm`, which is exactly the
    /// case this is not. Without it the mirror reads "no alarm" and every
    /// residency decision that consults it is wrong in the direction of
    /// shedding a cell that is about to fire.
    pub alarm: Option<RestoredAlarm>,
}

/// See [`RestoreOutcome::alarm`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoredAlarm {
    pub at_ms: i64,
    /// Whether a durable wake entry already covers it. Read from the same
    /// flusher the observer consults, not assumed: claiming coverage this
    /// node cannot prove is how an alarm gets lost across an eviction.
    pub covered: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseCasOutcome {
    Applied { etag: String },
    Rejected,
}

/// Deterministic timers are versioned effects, not implicit clock reads. A
/// stale firing from a replaced lease generation is harmless.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Timer {
    NodeLeaseRenew {
        generation: u64,
    },
    NodeLeaseFence {
        generation: u64,
    },
    CellAlarm {
        cell: CellId,
        generation: u64,
    },
    /// Fires if `op` is still outstanding. Identified by operation rather than
    /// by cell, so a completion that lands first simply leaves a stale timer
    /// that finds nothing to expire.
    OperationDeadline {
        op: OpId,
    },
    /// Fires if `cell` is still parked behind the admission gate. Keyed by
    /// cell because a parked cell has no operation: `OperationDeadline` is
    /// armed per emitted effect, and parking is the state of having emitted
    /// none. Without this the queue is the one stall with no bound.
    QueuedActivation {
        cell: CellId,
        generation: u64,
    },
    /// Fires to re-enter ownership resolution after a failed dead-owner
    /// log recovery. Keyed by cell like `QueuedActivation`, because the
    /// backing-off cell holds no operation; the generation discards a
    /// stale retry after the cell moved on.
    OwnerLogRecoveryRetry {
        cell: CellId,
        generation: u64,
    },
}

/// Every way a cell's state can leave this process, and the one promise the
/// application treats as if it had.
///
/// This enum lists every external route, plus `Sync`. It is deliberately not
/// `#[non_exhaustive]`, so every exhaustive shell match stops compiling until
/// somebody decides how a new channel is held and how it is released.
///
/// The core stores a channel and hands it back. It never reads it. A `match`
/// on `Channel` inside `celld-logic` would split one output invariant into a
/// separate rule for each channel.
///
/// An alarm is missing here on purpose, and stays missing. Its consuming
/// commit does open a barrier -- as `GateOwner::Alarm`, keyed by the cell and
/// epoch the firing was dispatched against -- but a barrier is not a channel.
/// The alarm's settlement is not an egress: it is replayed back into the core
/// rather than released to the shell, it has no request to route by, and the
/// output gate does not release it to the shell, and it has no request to route
/// by. A variant here would force the core to match on `Channel` to tell a
/// release from a replay, which is the one thing this enum exists to prevent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Channel {
    /// The answer to the cell event itself: an HTTP response, a cell RPC
    /// reply, or a peer-served reply. A response whose body streams takes one
    /// ticket for its head and one more for each chunk, because a chunk the
    /// producer makes after the release can reveal a later write.
    Response,
    /// `fetch()` from a handler to a third party.
    Fetch,
    /// A frame from a `webSocketMessage` handler, captured by the host and
    /// released from the cell's barrier queue.
    WsHibernatable,
    /// A frame on a socket the isolate opened and polls itself. It cannot be
    /// captured, because the handler may be awaiting the reply to the very
    /// frame being held, so it waits on a durability ticket instead.
    WsSelf,
    /// A service-binding call, `env.NAME.fetch()` or its RPC form.
    Service,
    /// A call to another cell: its `fetch` or one of its RPC methods.
    CellRpc,
    /// A leased Queue batch leaving its broker for a consumer Worker.
    Queue,
    /// The promise `storage.sync()` returns to the handler. Nothing leaves
    /// the process on this route, but the application treats the settled
    /// promise as a durability statement and acts on it through routes the
    /// gate cannot hold: a log line, local work, or the next checkpoint. A
    /// promise that settles before the proof lets the application discard a
    /// recovery path it still needs. So the promise takes the same ticket an
    /// egress takes, and its release is the promise settling.
    Sync,
}

impl Channel {
    /// Return the diagnostic name for this channel.
    pub fn as_str(self) -> &'static str {
        match self {
            Channel::Response => "response",
            Channel::Fetch => "fetch",
            Channel::WsHibernatable => "ws_hibernatable",
            Channel::WsSelf => "ws_self",
            Channel::Service => "service",
            Channel::CellRpc => "cellrpc",
            Channel::Queue => "queue",
            Channel::Sync => "sync",
        }
    }
}

/// Which mechanism proved a gated write durable. The fences differ: the
/// fleet's follower ensemble arbitrates (a takeover seals a member first),
/// the bucket only stores — so bucket proofs verify ownership before any
/// reveal and fleet proofs do not need that extra read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProofSource {
    Fleet,
    Bucket,
}

/// Whether a failed operation definitely did not commit or may have committed
/// before its caller lost the response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Definite,
    Ambiguous,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WebSocketKind {
    Hibernatable,
    Regular,
    /// A transport the cell opened itself with `new WebSocket(url)`. It pins
    /// its cell exactly as a regular one does -- a live transport cannot move
    /// with ownership -- but unlike an inbound client socket it is created by
    /// application code at a rate the application chooses, so how much of the
    /// node it may hold is budgeted.
    Outbound,
}

/// How far a wake hint reaches.
///
/// Every node lists the fleet's due wake entries on its tick, but only the
/// node that holds the advisory waker lease may spend I/O on cells it does not
/// own. `Owned` is every other node's hint: it wakes a cell this node holds
/// dormant and does nothing else. `Fleet` is the elected waker's hint (and the
/// boot scan's): it may probe a cell nobody here owns and take over a dead
/// owner's cell.
///
/// The distinction exists because a hint costs an activation permit and an
/// owner read before the core can learn the cell is someone else's. The
/// fleet's due set is O(fleet) while a node's own is O(node), so a node that
/// probed every fleet entry queued its own alarm wakes behind the fleet's:
/// measured as a 60 s tail against 32 s on seven nodes (2026-09-01).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WakeHintScope {
    /// Wake a dormant cell this node owns; ignore every other cell.
    Owned,
    /// Probe ownership and take over a cell whose owner is gone.
    Fleet,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    StartNodeLease {
        now_ms: u64,
        /// The monotonic instant `now_ms` was sampled. The initial lease's
        /// TTL is anchored here; without it the anchor is 0 and every
        /// millisecond of pre-actor startup counts against the first
        /// acquisition (a slow boot then discards a perfectly good lease).
        now_mono_ms: u64,
        spec: NodeLeaseSpec,
    },
    SelfNodeLeaseRead {
        op: OpId,
        now_ms: u64,
        now_mono_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
    },
    NodeLeaseCasCompleted {
        op: OpId,
        now_mono_ms: u64,
        result: Result<LeaseCasOutcome, Failure>,
        /// The folded log state the shell serialized into THIS attempt's
        /// body. The shell stamps the body from its own slot at
        /// serialization time — after the core chose `desired` — so the
        /// core's readback comparisons must use this value, never its
        /// pre-attempt belief. `None` means the body carried no log
        /// object (bucket posture, or a backend that does not fold).
        stamped_log_state: Option<crate::log_tier::LogState>,
    },
    /// The executor scanned the local paths after an exact node-generation
    /// handoff. These cells retain their existing ownership epochs, so the
    /// core can materialize them without reading or changing each owner.
    LocalCellsRead {
        result: Result<Vec<LocalCell>, Failure>,
    },
    TimerFired {
        timer: Timer,
        now_ms: u64,
        now_mono_ms: u64,
    },
    Request {
        request: RequestId,
        cell: CellId,
    },
    /// Production form of [`Event::Request`] with the sampled clocks. Untimed
    /// model slices retain the compact variant above and use the state's last
    /// observed clocks.
    RequestAt {
        request: RequestId,
        cell: CellId,
        now_mono_ms: u64,
    },
    /// A peer selected this node as a possible landing place for an unowned
    /// cell. Unlike ordinary ingress, this request must refuse immediately if
    /// its advertised capacity has gone stale; waiting here would strand the
    /// forwarding node instead of letting it traverse another candidate.
    CapacityRequestAt {
        request: RequestId,
        cell: CellId,
        now_mono_ms: u64,
    },
    /// A terminating peer asks this node to acquire an unowned cell. The
    /// ownership record must move now, but the runtime stays dormant until
    /// real traffic needs it. This avoids an eager fleet-wide restore wave.
    HandoffRequestAt {
        request: RequestId,
        cell: CellId,
        now_ms: u64,
        now_mono_ms: u64,
    },
    /// An event from a WebSocket that this cell already owns. A regular or
    /// outbound WebSocket pins its resident runtime, so its final events must
    /// still run locally after a shutdown batch marks the cell as quiescing.
    /// The exact WebSocket identity prevents new traffic from using this path.
    WebSocketRequestAt {
        request: RequestId,
        cell: CellId,
        websocket: WebSocketId,
        now_ms: u64,
        now_mono_ms: u64,
    },
    /// Reserve an idle resident isolate for a top-level Worker request. The
    /// shell falls back to the stateless pool when no resident is available;
    /// choosing and pinning a resident is lifecycle policy and therefore
    /// belongs in the replayable core.
    WorkerRequest {
        request: RequestId,
    },
    /// Stop cold activation work for a planned same-node replacement while
    /// leaving published runtimes and ownership intact. Late shell completions
    /// are stale, so a long restore cannot consume the complete shutdown grace
    /// and disable the certified local-reload path for every resident cell.
    BeginPreserve,
    Cancel {
        request: RequestId,
    },
    ActivityFinished {
        request: RequestId,
    },
    /// An effect is ready to leave the process on `channel`, and it can reveal
    /// this cell's state. The core withholds it until every write it can
    /// reveal is proven durable. This is the output gate, and the whole of it:
    /// every channel arrives here and the core does not branch on which.
    ///
    /// `position` is `Some` when the event that produced this output advanced
    /// the cell's committed WAL to it — the output opens its own barrier and
    /// waits for a proof that covers that position. `position` is `None` when
    /// the event only read: the output trails the newest barrier already open
    /// on the cell, because a reader can start after a write commits and
    /// before its proof lands. Its own start and end positions therefore
    /// decide nothing. A cell with nothing outstanding releases it at once,
    /// so an ordinary read pays no durability latency.
    ///
    /// `observed` is the committed-write position a read-only output saw when
    /// it answered, above the cell's published baseline, when a handler had
    /// advanced it: `None` when the output carries a `position` of its own or
    /// the cell holds no handler write. The core holds the output until a
    /// verified proof covers it, so a reader that answers between another
    /// handler's commit and that handler's ticket waits for the proof the
    /// writer has not asked for yet.
    ///
    /// `epoch` is the activation epoch the shell sampled `position` at, when
    /// it sampled before the request pinned the cell: an in-handler ticket
    /// takes its position in the handler's turn and only then acquires a
    /// request. A reset between the two discards that epoch's unproven writes,
    /// and the request then activates the next epoch, whose proof cannot cover
    /// what the ticket asks about. The core refuses a ticket whose epoch is
    /// not the resident one. `None` when the request was already active on
    /// the cell at the sample, as the response is: a reset deactivates every
    /// request of the cell it discards, so a stale one is refused as not
    /// active rather than by its epoch.
    Output {
        request: RequestId,
        channel: Channel,
        position: Option<u64>,
        observed: Option<u64>,
        epoch: Option<Epoch>,
    },
    /// The production output event. The monotonic instant keeps the authority
    /// decision and the release inseparable when a handler outlives the node
    /// lease under which it started. Untimed simulations can use [`Event::Output`].
    ///
    /// `epoch` and `observed` carry the same values as on [`Event::Output`],
    /// and the core applies the same rules to them. The production shell sends
    /// only this variant, so both must travel here too; without them, every
    /// released ticket in a production build would skip those checks.
    OutputAt {
        request: RequestId,
        channel: Channel,
        position: Option<u64>,
        observed: Option<u64>,
        epoch: Option<Epoch>,
        now_mono_ms: u64,
    },
    /// The shell finished proving a gated write durable. `Ok(position)` reports
    /// the committed-write position the replica has *actually* proved durable;
    /// the core acknowledges only when it covers the gated write's position, so
    /// a replicator that proves less than it was asked to cannot force an early
    /// ack. `Err` failed the proof outright.
    ///
    /// `source` says which mechanism proved it, because the two carry different
    /// acknowledgement fences. A fleet proof is already arbitrated: a takeover
    /// seals a follower before restoring, so a stale owner's next ack-all fails
    /// closed. A bucket proof needs ownership verification before anything
    /// is revealed.
    DurableReached {
        op: OpId,
        result: Result<u64, Failure>,
        source: ProofSource,
    },
    /// The ownership record still names this node at this epoch
    /// (`Ok`), or does not, or could not be read.
    OwnershipVerified {
        op: OpId,
        result: Result<(), Failure>,
    },
    WebSocketOpened {
        cell: CellId,
        websocket: WebSocketId,
        kind: WebSocketKind,
    },
    WebSocketClosed {
        cell: CellId,
        websocket: WebSocketId,
    },
    AlarmObserved {
        cell: CellId,
        at_ms: Option<i64>,
        covered: bool,
        now_ms: u64,
        now_mono_ms: u64,
    },
    AlarmFinished {
        op: OpId,
        /// The cell and epoch the firing was dispatched against. The gate the
        /// core opens for the consuming commit is keyed by these, and not by
        /// the alarm's own op: an activity fold can supersede the firing state
        /// while the handler's write is still unproven, and that write exists
        /// — and must hold every reader of the cell — regardless.
        cell: CellId,
        epoch: Epoch,
        now_ms: u64,
        now_mono_ms: u64,
        /// The deadline standing after the handler ran, whether a wake entry
        /// covers it, and the position of the consuming commit if the firing
        /// wrote. The position is sampled last, so it covers the consume
        /// itself, and the core proves it durable before the alarm settles.
        result: Result<(Option<i64>, bool, Option<u64>), Failure>,
    },
    /// A due wake entry was found for `cell`. `entry_ms` is the minute the
    /// entry is filed under, so the node that acts on the hint can adopt
    /// that exact entry; `scope` says how far the hint reaches.
    WakeHint {
        cell: CellId,
        entry_ms: i64,
        scope: WakeHintScope,
    },
    WakeHintAt {
        cell: CellId,
        entry_ms: i64,
        scope: WakeHintScope,
        now_mono_ms: u64,
    },
    OwnerRead {
        op: OpId,
        /// Wall-clock observation made when the ownership read completed. It
        /// bounds reuse of a shared owner-node lease without letting the core
        /// read a clock itself.
        now_ms: u64,
        result: Result<Option<OwnerRecord>, Failure>,
    },
    NodeLogRecovered {
        op: OpId,
        result: Result<(), Failure>,
    },
    /// The log tier changed the folded log object the next lease write
    /// must carry (lease-fold): renew NOW so the change is durable before
    /// the caller proceeds. A no-op unless the lease is held.
    NudgeNodeLease {
        now_ms: u64,
        now_mono_ms: u64,
    },
    NodeLeaseRead {
        op: OpId,
        now_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
    },
    CapacityPeersRead {
        op: OpId,
        now_ms: u64,
        result: Result<Vec<CapacityPeer>, Failure>,
    },
    OwnerCasCompleted {
        op: OpId,
        result: Result<CasOutcome, Failure>,
    },
    OwnerReleased {
        op: OpId,
        result: Result<CasOutcome, Failure>,
    },
    /// A released cell became authoritative on a successor. Its runtime can
    /// still be warming when this event arrives.
    SuccessorAdopted {
        op: OpId,
        result: Result<AdoptedCell, Failure>,
    },
    RestoreCompleted {
        op: OpId,
        result: Result<RestoreOutcome, Failure>,
    },
    RuntimeStarted {
        op: OpId,
        /// The node-wide identity of the V8 heap now holding this cell's
        /// realm. Placement happens in the shell, so the core has to be told;
        /// the walk down groups on it to empty a heap rather than scatter its
        /// cuts across every one. `None` when no runtime placed the cell.
        isolate: Option<crate::isolate::HeapId>,
        /// The application generation whose isolate took the cell. The shell
        /// resolves the generation at placement, and a placement that
        /// straddled an adoption lands on the previous one; the swap pump
        /// compares this with the current generation. Zero means the cell
        /// was never stamped, which the pump treats as stale.
        generation: u64,
        result: Result<(), Failure>,
    },
    /// The node adopted a new application generation. Every resident cell on
    /// an older generation moves to it at a safe point, or by force once
    /// `max_age_ms` has passed since this event. A resident cell whose class
    /// is in `eager_classes` — the engine's reserved classes, which hold no
    /// application state worth waiting for — is forced at once.
    GenerationChanged {
        generation: u64,
        now_mono_ms: u64,
        max_age_ms: u64,
        eager_classes: Vec<String>,
    },
    Published {
        op: OpId,
        result: Result<(), Failure>,
    },
    DurabilityChecked {
        op: OpId,
        result: Result<(), Failure>,
    },
    RuntimeStopped {
        op: OpId,
    },
    /// A bounded stop did not complete before its deadline. The runtime is
    /// gone, the database stays open, and the cell restarts on it at the
    /// same epoch, as a generation swap does.
    RuntimeStopFailed {
        op: OpId,
    },
    /// Policy input for this first slice. Later eviction selection emits this
    /// from the same core rather than an external caller choosing a victim.
    Evict {
        cell: CellId,
    },
    /// A periodic resource sample from the edge.
    ///
    /// The core never reads a clock or a proc file; the shell measures and
    /// hands the numbers over, and every decision that follows -- whether the
    /// node is overloaded, whether the latch stays hot, how far to shed --
    /// happens here where a schedule can replay it.
    LoadSampled {
        load: pressure::Load,
        now_mono_ms: u64,
    },
    /// Retire an exact cached remote route after a dispatch failure which is
    /// known not to have executed the request. Newer route generations are
    /// unaffected by delayed failure reports.
    InvalidateRemote {
        cell: CellId,
        node: NodeId,
        epoch: Epoch,
    },
    NodeFenced,
    /// The node is shutting down: release every resident cell and wait for a
    /// successor to publish it. Unlike an on-demand evict this always
    /// releases, whatever `ownership_on_evict` is set to. Complete handoffs
    /// run at most `max_releases` at a time, and a cell that is active when
    /// the drain begins is picked up once its activity finishes.
    ReleaseAll,
    /// Give up to `cells` idle cells to the fleet through the shutdown
    /// handoff pipeline without draining. The core picks dormant cells
    /// first, then hibernatable residents, and skips any cell with work.
    Rebalance {
        cells: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    Local,
    Remote {
        node: NodeId,
        addr: String,
        epoch: Epoch,
        peer_protocol: u16,
    },
}

/// Exact resident isolate reserved for a top-level Worker request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerRoute {
    pub cell: CellId,
    pub epoch: Epoch,
    /// A pending durability proof made stale by selecting this still-routable
    /// resident. The executor uses the ID only to release its effect waiter;
    /// a late completion is ignored by the core's phase check.
    pub retired_durability: Option<OpId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestError {
    NodeUnavailable,
    ResolveFailed,
    AcquireFailed,
    RestoreFailed,
    RuntimeFailed,
    PublishFailed,
    NodeFenced,
    PeerIncompatible,
    CapacityExhausted,
    /// A local write ran but its durability could not be proven, so the
    /// response must fail rather than falsely acknowledge the write.
    DurabilityUnproven,
}

/// Work performed outside the core. Every asynchronous effect is versioned;
/// completion events with an obsolete `op` are ignored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    ScheduleTimer {
        timer: Timer,
        at_mono_ms: u64,
    },
    ReadSelfNodeLease {
        op: OpId,
    },
    CasNodeLease {
        op: OpId,
        guard: CasGuard,
        record: NodeLeaseRecord,
        /// The prior record's peer-visible expiry for a renewal. The shell
        /// uses it to report how much authority remained when the attempt
        /// completed. An initial acquisition has no prior authority.
        authority_expires_ms: Option<u64>,
    },
    /// Read the local cell inventory. This is emitted only after the initial
    /// node lease CAS replaced the exact certified predecessor generation.
    ReadLocalCells,
    ReadOwner {
        op: OpId,
        cell: CellId,
    },
    ReadNodeLease {
        op: OpId,
        cell: CellId,
        owner: NodeId,
    },
    /// The takeover interlock, as a core decision (lease-fold): the dead
    /// owner's folded log state was not sealed, so its acked tail may
    /// exist only on its followers. The executor recovers every
    /// non-sealed session of `owner` into the bucket and reports
    /// `NodeLogRecovered`; only then does the claim proceed.
    RecoverNodeLog {
        op: OpId,
        cell: CellId,
        owner: NodeId,
    },
    /// Enumerate recent node leases and return their advisory load records.
    /// Listing and bounded parallel reads are adapter mechanics; selection,
    /// reservations, and exclusions are deterministic core policy.
    ReadCapacityPeers {
        op: OpId,
        cell: CellId,
    },
    CasOwner {
        op: OpId,
        cell: CellId,
        guard: CasGuard,
        epoch: Epoch,
        takeover: bool,
    },
    /// Bring the bucket's wake entry for this cell into line with its alarm.
    ///
    /// Emitted wherever the alarm settles, which is the only place that
    /// knows. An arm needs an entry; a consumed alarm needs its entry gone,
    /// or every later due scan finds a hint for an alarm that already fired
    /// and wakes a cell with nothing to do. `next_alarm_ms` is -1 when no
    /// alarm remains.
    ReconcileWakeEntry {
        cell: CellId,
        next_alarm_ms: i64,
    },
    /// Take responsibility for the wake entry a hint came from, filed under
    /// `entry_ms`. Emitted only when the core acts on the hint: the node that
    /// activates the cell must know the entry so its consume deletes it, and
    /// a node that does not act must not learn about a cell it never
    /// resolves, or the fleet's whole due set accumulates in its flusher.
    AdoptWakeEntry {
        cell: CellId,
        entry_ms: i64,
    },
    /// Publish an evicted cell as unowned, keeping its epoch, so the next
    /// node to want it can take it without waiting for this one to notice.
    ReleaseOwner {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
    },
    /// Ask a compatible peer to acquire a cell whose owner record was just
    /// released. The result acknowledges authority acquisition. Replica
    /// restore and runtime publication continue outside the donor's shutdown
    /// critical path.
    AdoptReleased {
        op: OpId,
        cell: CellId,
        released_epoch: Epoch,
        /// A balancing move rather than a drain: the successor must be a
        /// peer below its ownership target, and the search is bounded by
        /// the operation deadline instead of the process exit.
        rebalance: bool,
    },
    /// Interrupt the activity which existed when a cell entered its shutdown
    /// handoff batch. The shell maps core request IDs to handler/body aborts
    /// and closes sockets with a restart status.
    CancelCellActivity {
        cell: CellId,
        requests: Vec<RequestId>,
        websockets: Vec<WebSocketId>,
        /// A firing alarm is a handler without a routed request id. Its exact
        /// operation identifies the shell task to cancel, so a late cancel
        /// cannot affect the next alarm firing for this cell.
        alarm: Option<OpId>,
        /// A forced generation swap lists only the regular and outbound
        /// sockets, and the cell stays on this node: its hibernatable sockets
        /// and auto-response pair survive. A shutdown handoff lists every
        /// socket and forgets the pair, because the cell is leaving.
        keep_hibernatable: bool,
    },
    Restore {
        op: OpId,
        cell: CellId,
        spec: RestoreSpec,
    },
    StartRuntime {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
    },
    Publish {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
    },
    /// Prove that every commit made before this effect is recoverable from
    /// replica authority. Voluntary eviction cannot begin until this succeeds.
    EnsureDurable {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
    },
    /// The output gate: prove the cell's committed `position` is replicated so
    /// a withheld local write response can be released. Unlike
    /// [`Effect::EnsureDurable`] this is per-request and changes no cell phase,
    /// so the cell keeps serving co-resident requests while one response waits.
    AwaitDurable {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
        position: u64,
    },
    /// For bucket-proof acknowledgements, read `own.json` and answer whether it
    /// still names this node at this epoch. The core holds the gated write
    /// until `Event::OwnershipVerified` returns. Fleet-proof acks never
    /// emit this — the ensemble seal is their fence.
    VerifyOwnership {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
    },
    StopRuntime {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
        cause: StopCause,
        /// The shell gives up after its operation deadline and reports
        /// `RuntimeStopFailed`. A drain and a fence retry until the process
        /// ends, because their node is leaving; an eviction on a serving
        /// node is not worth a cell that never answers again.
        bounded: bool,
    },
    FireAlarm {
        op: OpId,
        cell: CellId,
        epoch: Epoch,
        scheduled_ms: i64,
    },
    Complete {
        request: RequestId,
        result: Result<Route, RequestError>,
    },
    /// Release a withheld output now that the writes it can reveal are
    /// decided: `Ok` lets it leave, `Err` refuses it. Emitted only for an
    /// output the shell held open via [`Event::OutputAt`].
    ///
    /// `channel` is the one the shell supplied, returned unchanged. It selects
    /// which adapter holds this effect and is the only thing the shell needs
    /// in order to route the release. No operation id rides along: the shell
    /// never learns the one the core assigned, and `(request, channel)` is
    /// unique by construction — `State::validate` enforces it.
    Release {
        request: RequestId,
        channel: Channel,
        result: Result<(), RequestError>,
    },
    /// Complete the synchronous resident-selection decision. `None` means
    /// the executor must use the ordinary stateless Worker pool.
    CompleteWorker {
        request: RequestId,
        route: Option<WorkerRoute>,
    },
    /// Refuse a transport the node cannot afford to hold. The shell closes it;
    /// the cell carries on without it.
    CloseWebSocket {
        cell: CellId,
        websocket: WebSocketId,
    },
    Halt {
        code: i32,
        reason: HaltReason,
    },
}

/// One database encoded by the local cell path.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalCell {
    pub id: CellId,
    pub epoch: Epoch,
    /// The epoch's database is open-able as it stands, so a clean reload
    /// resumes it in place. False for the copy an eviction preserved: it
    /// proves the node held the cell, which is what the ownership
    /// confirmation after a restart needs, but it is not a resumable
    /// epoch. Without it a restarted node confirmed only the cells that
    /// were resident when it died and hid every hibernated one.
    pub live: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopCause {
    Cleanup,
    /// `rebalance` means this eviction hands the cell to the fleet: its
    /// ownership record is released and the local replica is not worth
    /// keeping. An idle eviction is the opposite on both counts -- it
    /// keeps the record so the next activation here renames the file into
    /// place instead of paying a full remote restore.
    Evict {
        rebalance: bool,
    },
    Fence,
    /// A durability proof came back negative, so this runtime's memory and its
    /// local database may hold writes the bucket does not. celld has just told
    /// the caller those writes failed; continuing to serve them is the
    /// divergence, not the failure. Stop, keep no local snapshot, and let the
    /// next activation restore from the bucket.
    Reset,
    /// The cell moves to the current application generation. The runtime
    /// leaves its isolate and is started again at the same epoch on an
    /// isolate of the new generation: no ownership change, no restore, no
    /// replication release. The stop reaches the shell as `Cleaning`, and the
    /// core answers the stop with `StartRuntime` instead of a terminal phase.
    Swap,
}
