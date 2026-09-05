// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The isolate pool, reified sans-IO.
//!
//! celld runs JS on a set of isolates that grow and shrink with demand. An
//! isolate is not bound to a thread: `v8::Locker` installs the entering
//! thread's per-isolate state, so any worker can take an isolate and run a
//! turn in it. What an isolate *is* bound to is the JS state it holds, and
//! that is what every decision here is about.
//!
//! Two quantities, because they answer different questions:
//!
//! - **turns** — turns in flight on this isolate: queued for it plus the
//!   one running. CPU demand. A request awaiting I/O contributes nothing,
//!   because the isolate is released across the await, so this is what
//!   placement balances. It must count the queue, not just the runner: an
//!   isolate serves one turn at a time, so "executing" never exceeds one
//!   and could never signal that another isolate would help.
//! - **requests** — requests affiliated with this isolate. A request runs
//!   its first turn somewhere and its promise then lives in that isolate's
//!   heap, so every later turn must come back. Affiliation is memory, not
//!   CPU, and is what admission and shedding care about.
//!
//! Everything here is a predicate over observed load. The shell reports what
//! it sees and performs what it is told; it never decides.

/// Which isolate. An index the shell keeps, stable for that isolate's life.
/// Never a thread id — an isolate has no thread.
pub type IsolateId = usize;

/// One constructed V8 heap in this node process.
///
/// Unlike [`IsolateId`], this value is not a pool index. The shell allocates a
/// fresh value for every worker that it installs, so two script pools and two
/// application generations cannot make distinct heaps look identical to
/// node-wide pressure policy.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HeapId(u64);

impl HeapId {
    /// Construct an opaque heap identity at a shell or simulator boundary.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for HeapId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// What the shell observes about one isolate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IsolateLoad {
    /// Turns in flight: waiting for this isolate plus the one running in it.
    /// Released across an await, so this is CPU demand rather than total
    /// outstanding work. Counting only the runner would cap this at one,
    /// since an isolate serves one turn at a time.
    pub turns: usize,
    /// Requests whose JS state lives here, running or awaiting. Each one
    /// pins a promise and its closures in this isolate's heap.
    pub requests: usize,
    /// Cells hosted here: realms this isolate holds, one per cell.
    ///
    /// The third quantity, and unlike the other two it does not come back on
    /// its own. A request ends; a cell stays until it is evicted or handed
    /// to another node. So this measures what the isolate is *committed* to,
    /// and it is what cell placement balances.
    pub cells: usize,
    /// The shell has stopped affiliating new work and is draining this
    /// isolate.
    pub retiring: bool,
}

/// Pool-wide limits, built once from configuration by the caller. The core
/// never reads the environment itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolLimits {
    /// Turns in flight at which an isolate is considered busy, so the pool
    /// prefers to grow rather than queue another turn behind those. Small:
    /// a couple of turns already waiting means another isolate would help.
    pub grow_at: usize,
    /// Per-isolate turn level the pool must stay under for a retirement to be
    /// worth starting. Below `grow_at`, and the gap is the hysteresis that
    /// stops growth and retirement chasing each other.
    pub shrink_under: usize,
    /// Ceiling on live isolates. Past it the pool queues rather than grows.
    pub max_stateless: usize,
    /// Affiliated stateless requests the node may hold at once, across every
    /// isolate. `None` leaves admission unbounded, which is only correct
    /// while something else caps concurrency.
    ///
    /// A node-wide figure rather than a per-isolate one, because the memory
    /// it bounds is the node's: a suspended request pins its promise and
    /// buffers wherever it landed, and spreading the same requests over more
    /// isolates does not make them cheaper.
    pub max_requests: Option<usize>,
    /// Cells one isolate may host. Past it, cell placement builds another
    /// isolate rather than adding a realm.
    ///
    /// A per-isolate figure where `max_requests` is node-wide, and the
    /// difference is not an inconsistency. A suspended request costs the
    /// same wherever it landed, so spreading requests buys nothing and the
    /// node-wide total is the real bound. Cells are not like that: they
    /// share one V8 heap, and a heap that holds too many of them is a
    /// single OOM taking every cell in it down together. Spreading is the
    /// whole point, so the bound has to be per-isolate.
    pub max_cells: usize,
}

/// The pool as the shell sees it. Indexed by [`IsolateId`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolLoad {
    pub isolates: Vec<IsolateLoad>,
    pub limits: PoolLimits,
}

/// Why a request was refused. The shell maps this to a status; a refusal is
/// a 503, because the node is the thing that is unavailable, not the request
/// that is wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// The node already holds all the stateless requests it may.
    NodeFull,
    /// The node is shedding: it is over a resource ceiling and recovering.
    NodePressured,
}

/// Where a new request runs its first turn.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Placement {
    Existing(IsolateId),
    /// Build an isolate and use that. Growth is a placement outcome rather
    /// than a separate command, because an isolate cannot be addressed
    /// before the shell has built it.
    Grow,
}

impl PoolLoad {
    fn live(&self) -> impl Iterator<Item = (IsolateId, &IsolateLoad)> {
        self.isolates
            .iter()
            .enumerate()
            .filter(|(_, l)| !l.retiring)
    }

    fn live_count(&self) -> usize {
        self.live().count()
    }
}

/// Take a stateless request, or refuse it.
///
/// Admission and placement are one decision — *may this run, and where* —
/// so they are one call against one snapshot. Split apart they were two
/// views of the same node that could disagree, and the shell could admit a
/// request and then fail to place it, or place one it had never admitted.
///
/// Called after the connection is accepted, not before. A node that stops
/// accepting leaves connections in its kernel backlog, where they wait
/// without an answer until the client's own deadline — the failure this
/// bound exists to prevent, arriving one layer lower. Accepting and refusing
/// costs a syscall and gives the caller a 503 it can act on.
///
/// **Pressure does not close stateless admission.** It used to, and that was
/// wrong twice over.
///
/// Transient work gives memory back on its own, so a request cheap enough to
/// drain by itself is not the thing to refuse.
///
/// And it wedged the node's clients. The stateless Worker entry is also the
/// ingress that routes a cell to whichever node owns it, so refusing it does
/// not move load off a latched node; it removes that node's ability to move
/// load anywhere. A latched node must keep answering and place its cells on a
/// spare.
///
/// What pressure does instead is **stop the pool growing**: a node over its
/// memory ceiling may keep serving on the isolates it already has, but must
/// not build another. Growth is still allowed when there is nothing to place
/// onto, because an isolate the node cannot build is a request it cannot
/// answer at all.
///
/// Stateless memory is bounded by `max_requests`, which is the figure that
/// actually measures it.
pub fn admit(load: &PoolLoad, shedding: bool) -> Result<Placement, Refusal> {
    if let Some(max) = load.limits.max_requests {
        let held: usize = load.isolates.iter().map(|l| l.requests).sum();
        if held >= max {
            return Err(if shedding {
                // Reported ahead of fullness: a full node drains on its own,
                // a pressured one does not, so pressure is the more
                // actionable thing to tell the caller.
                Refusal::NodePressured
            } else {
                Refusal::NodeFull
            });
        }
    }
    Ok(if shedding {
        place_without_growing(load)
    } else {
        place(load)
    })
}

/// [`place`], except that it will not build an isolate while one exists to
/// use. The node is over a memory ceiling; another heap is the last thing it
/// needs.
fn place_without_growing(load: &PoolLoad) -> Placement {
    match load.live().min_by_key(|(_, l)| l.turns) {
        Some((id, _)) => Placement::Existing(id),
        None => Placement::Grow,
    }
}

/// Choose the isolate for a request's first turn.
///
/// This is the only free choice a stateless request gets. Every later turn of
/// that request is addressed to the isolate chosen here, because its promise
/// lives in that heap — so the shell must remember the affiliation, and this
/// function is not consulted again for that request.
///
/// Balanced on turns in flight rather than affiliations, because an awaiting
/// request occupies nothing: an isolate holding a thousand suspended
/// requests and running no JS is idle and should be given work. Affiliation
/// count is a memory signal and belongs to admission, not here.
///
/// Retiring isolates are skipped: the point of retiring one is that it stops
/// taking new affiliations.
pub fn place(load: &PoolLoad) -> Placement {
    match load.live().min_by_key(|(_, l)| l.turns) {
        // No isolate, or every one of them is draining.
        None => Placement::Grow,
        Some((id, l)) => {
            if l.turns >= load.limits.grow_at && load.live_count() < load.limits.max_stateless {
                Placement::Grow
            } else {
                // At the ceiling the pool queues. Refusing work is
                // admission's job: see `worker::may_admit`.
                Placement::Existing(id)
            }
        }
    }
}

/// Where a cell's realm goes, for the whole life of that cell on this node.
///
/// **Not the same decision as [`place`], and the difference is permanence.**
/// A stateless request picks an isolate for its first turn and is done: the
/// next request picks again, so a bad choice costs one request. A cell's
/// realm holds its JS state, so every event it ever receives comes back
/// here. The shell records the assignment and never asks again — this is
/// consulted once per cell, not once per event.
///
/// That makes the balance different too. `place` balances `turns`, because
/// an awaiting request occupies nothing and CPU is what a new turn contends
/// for. Here the cost is a realm that stays, so this reads `cells` and leaves
/// turns alone: an isolate running hot right now may still be the right home
/// for a cell that will outlive the burst.
///
/// Growth happens at `max_cells` rather than at a busy-ness threshold, and
/// the ceiling is per-isolate for the reason in [`PoolLimits::max_cells`] —
/// too many cells in one heap is one OOM that takes all of them.
///
/// There is no separate cell-isolate ceiling. The node-wide resident and RSS
/// limits decide whether this node can host another cell. Once ownership has
/// admitted the cell, placement always provides enough isolates to preserve
/// the per-isolate limit.
pub fn place_cell(load: &PoolLoad) -> Placement {
    // The fullest isolate that still has room, not the emptiest.
    //
    // Packing is what makes a heap reclaimable. The walk down evicts from the
    // isolate closest to empty, because only the cut that takes an isolate's
    // last cell returns its heap. Spreading aimed new cells at that same
    // isolate, so under any load that keeps admitting, eviction drained a heap
    // and placement refilled it: the isolate never reached zero, `retire`
    // refused it, and the node paid an eviction and a cold start per cell for
    // no memory back.
    //
    // Packing costs nothing that `max_cells` was not already bounding. That
    // ceiling is what limits how many cells one OOM can take, and it applies
    // either way.
    match load
        .live()
        .filter(|(_, l)| l.cells < load.limits.max_cells)
        .max_by_key(|(id, l)| (l.cells, std::cmp::Reverse(*id)))
    {
        // No isolate, or every one of them is draining.
        None => Placement::Grow,
        Some((id, _)) => Placement::Existing(id),
    }
}

/// Which isolate to retire, if any.
///
/// Worth starting only when the rest of the pool can absorb the current turns
/// without immediately wanting to grow again.
///
/// The victim is cheapest to empty: fewer affiliated requests, then the
/// newest, so a pool that grew for a burst gives back what the burst added.
/// An isolate empties itself as its requests finish, so the shell only has
/// to stop sending it work and wait.
///
/// This returns a candidate, not a command. The shell still stops affiliating
/// new work and lets outstanding requests finish before the isolate goes
/// away — `Pool::reap`.
///
/// **An isolate hosting a cell is never a candidate.** Retiring rests on the
/// victim emptying itself, and requests do that because they answer. A cell
/// does not: it stays until it is evicted or handed to another node, neither
/// of which retirement can cause. Choosing one would mark it retiring
/// forever — refused new work, never drained, never freed — which is a
/// smaller pool and a leak at the same time.
///
/// Cell and stateless pools both run this maintenance decision. The cell pool
/// uses this filter to reclaim empty heaps after eviction while preserving
/// every isolate that still holds a realm.
/// May a RETIRING isolate's worker and heap be freed? Only when it is
/// provably drained: no queued or running turn, and no live affiliation.
/// An affiliation is any dispatched event's claim on the isolate, held
/// across suspensions — a suspended event holds no turn, so `turns`
/// alone is not drained-ness, and freeing on it re-enters a freed heap
/// when the event's host I/O completes (denoland/celld#147). `cells` is
/// consulted too: `retire` never selects a housed isolate, but a
/// generation-wide retirement marks every isolate of a superseded
/// deployment, housed or not, and freeing a housed one would drop every
/// resident cell in it. A cell whose residency dropped mid-event is still
/// the affiliation's case, not this one.
pub fn may_free(load: &IsolateLoad) -> bool {
    load.retiring && load.turns == 0 && load.requests == 0 && load.cells == 0
}

pub fn retire(load: &PoolLoad) -> Option<IsolateId> {
    let live = load.live().count();
    if live == 0 {
        return None;
    }

    let turns: usize = load.live().map(|(_, l)| l.turns).sum();
    let remaining = live - 1;
    if turns > remaining.saturating_mul(load.limits.shrink_under) {
        return None;
    }

    load.live()
        .filter(|(_, l)| l.cells == 0)
        .min_by_key(|(id, l)| (l.requests, std::cmp::Reverse(*id)))
        .map(|(id, _)| id)
}
