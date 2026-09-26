// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// RuntimeManager is the V8 cell-host arm, so its executor and observability
// clocks remain ambient.
#![allow(clippy::disallowed_methods)]

//! V8 runtime materialization behind core-authorized lifecycle effects.
//!
//! The manager owns handles and filesystem paths, never lifecycle policy.
//! StartRuntime, Publish, and StopRuntime decide when a cell handle moves from
//! starting to externally dispatchable to closed.

use crate::asyncrt;
pub(crate) use crate::engine_api::StopMode;
use crate::generation::{Generation, GenerationId};
use crate::js::{self, CellJob, CellStorage, HttpResponse, Worker, WorkerConfig};

pub use crate::clean_reload::take_clean_reload_generation;
pub use crate::clean_reload::write_clean_reload_marker;
pub use crate::engine_api::admission_wait;
pub use crate::engine_api::AlarmObserver;
pub use crate::engine_api::RuntimeFetch;
pub use crate::engine_api::RuntimeOptions;
pub use crate::engine_api::ServiceFetch;
pub use crate::ltx_replication::Replication;
use anyhow::{anyhow, Context};
use futures_util::StreamExt as _;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const REMOTE_ABORT_TTL: Duration = Duration::from_secs(600);
const REMOTE_COMPLETION_TTL: Duration = Duration::from_secs(60);
const MAX_REMOTE_PENDING_ABORTS: usize = 65_536;
#[doc(hidden)]
pub const MAX_REMOTE_COMPLETIONS: usize = 65_536;
const MAX_ALARM_COMPLETIONS: usize = 65_536;

/// How often the isolate pool gives back what it no longer needs.
const REAP_INTERVAL: Duration = Duration::from_secs(30);

/// How often a suspended request re-reads its cancellation flag. Matches the
/// blocking run loop's own cap, which exists for the same reason: a client
/// disconnect is raised on another thread and has nothing to wake this one.
const CANCELLATION_TICK: Duration = Duration::from_millis(10);
/// Cell fetches that one target can hold before celld refuses excess work.
///
/// The last nonsaturated Queue step in the AWS partition staircase held
/// approximately 40 requests by Little's Law. A limit of 64 keeps that step
/// below the gate and prevents one target from consuming hundreds of client
/// slots after its throughput stops increasing.
pub const DEFAULT_MAX_CELL_REQUESTS: usize = 64;

/// Serialize each publisher that owns this lifetime with the target request
/// driver's final cancellation cleanup. Without this shared lifetime, the
/// driver can clear and exit between a publisher observing a pending reply and
/// publishing its abort. This race leaves a tombstone with no remaining driver.
///
/// This hidden public type is the embedder boundary for a stateless request.
/// An embedder can give clones to abort publishers, but it must give the same
/// lifetime to the driver through [`Self::drive_stateless_fetch`].
#[doc(hidden)]
pub struct RequestCancellationLifetime {
    request_id: js::RequestId,
    finished: Mutex<bool>,
}

impl RequestCancellationLifetime {
    #[doc(hidden)]
    pub fn stateless() -> Arc<Self> {
        Self::from_request_id(js::next_request_id())
    }

    fn from_request_id(request_id: js::RequestId) -> Arc<Self> {
        Arc::new(Self {
            request_id,
            finished: Mutex::new(false),
        })
    }

    #[doc(hidden)]
    pub fn request_id(&self) -> js::RequestId {
        self.request_id
    }

    /// Publish an abort from an embedder that owns this lifetime.
    #[doc(hidden)]
    pub fn publish_abort(&self) {
        #[cfg(all(test, celld_internal_tests))]
        // Pause before taking `finished`: the test must let retirement win
        // this ordering without deadlocking on the very lock under test.
        js::pause_abort_request_if_armed_for_test(self.request_id);
        let finished = self
            .finished
            .lock()
            .expect("request cancellation lifetime poisoned");
        if !*finished {
            js::abort_request(self.request_id);
        }
    }

    pub(crate) fn finish(&self) {
        let mut finished = self
            .finished
            .lock()
            .expect("request cancellation lifetime poisoned");
        js::clear_request_cancellation(self.request_id);
        *finished = true;
    }

    /// Drive one cancellable stateless fetch with this same lifetime.
    ///
    /// This function constructs the driver future and its retirement guard
    /// before it returns. Thus, dropping the future before its first poll also
    /// retires the request. The request ID is derived here, so a publisher
    /// cannot name one request while the driver retires another request.
    #[doc(hidden)]
    pub fn drive_stateless_fetch(
        self: Arc<Self>,
        slot: Arc<crate::pool::Slot>,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<HttpResponse>>,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let job = stateless_fetch_job_factory(None, url, method, body, headers, Some(self))(reply);
        drive_affiliated(slot.affiliate(), job, None)
    }
}

/// A stateless job and the only cancellation lifetime that can name it.
/// Cancellable constructors derive the copied JavaScript request id from the
/// lifetime, so a caller cannot pair one id with another lifetime.
struct StatelessWorkerJob {
    job: crate::WorkerJob,
    cancellation: RequestCancellationGuard,
}

impl StatelessWorkerJob {
    fn fetch(
        entrypoint: Option<crate::WorkerFetchEntrypoint>,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        cancellation: RequestCancellationGuard,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<HttpResponse>>,
    ) -> Self {
        let request_id = cancellation
            .lifetime()
            .map(|lifetime| lifetime.request_id());
        Self {
            job: crate::WorkerJob::Fetch {
                queued_at: Instant::now(),
                entrypoint,
                invocation_limits: None,
                url,
                method,
                body,
                headers,
                request_id,
                tail_report: None,
                reply,
            },
            cancellation,
        }
    }

    fn driver_owned(job: crate::WorkerJob) -> Self {
        // This compatibility constructor is for a job with no publisher. A
        // publisher must give its lifetime to the fetch factory because a
        // copied request id cannot prove that the publisher and driver match.
        let lifetime = match &job {
            crate::WorkerJob::Fetch { request_id, .. } => {
                request_id.map(RequestCancellationLifetime::from_request_id)
            }
            crate::WorkerJob::Rpc { .. } | crate::WorkerJob::Queue { .. } => None,
        };
        let cancellation = RequestCancellationGuard::new(lifetime);
        Self { job, cancellation }
    }
}

fn stateless_fetch_job_factory(
    entrypoint: Option<crate::WorkerFetchEntrypoint>,
    url: String,
    method: String,
    body: js::RequestBody,
    headers: Vec<(String, String)>,
    cancellation: Option<Arc<RequestCancellationLifetime>>,
) -> impl FnOnce(tokio::sync::oneshot::Sender<anyhow::Result<HttpResponse>>) -> StatelessWorkerJob {
    // The factory crosses the admission await, so it owns retirement until it
    // transfers the same guard into the spawned driver's job.
    let cancellation = RequestCancellationGuard::new(cancellation);
    move |reply| {
        StatelessWorkerJob::fetch(entrypoint, url, method, body, headers, cancellation, reply)
    }
}

/// Remove a request's cancellation state when its driver leaves. Each
/// publisher for this request must share this lifetime, so publication
/// serializes with cleanup.
struct RequestCancellationGuard(Option<Arc<RequestCancellationLifetime>>);

impl RequestCancellationGuard {
    fn new(lifetime: Option<Arc<RequestCancellationLifetime>>) -> Self {
        Self(lifetime)
    }

    fn shared(lifetime: Arc<RequestCancellationLifetime>) -> Self {
        Self::new(Some(lifetime))
    }

    fn lifetime(&self) -> Option<&Arc<RequestCancellationLifetime>> {
        self.0.as_ref()
    }
}

impl Drop for RequestCancellationGuard {
    fn drop(&mut self) {
        if let Some(lifetime) = &self.0 {
            lifetime.finish();
        }
    }
}

async fn drive_with_request_cancellation(
    driving: impl std::future::Future<Output = ()> + Send + 'static,
    cancellation: RequestCancellationGuard,
) {
    // The caller constructs the guard before it creates this future. The
    // future therefore owns retirement before its first poll, and this local
    // keeps that ownership until the complete driver future returns.
    let _request_cancellation = cancellation;
    driving.await;
}

#[derive(Clone)]
#[doc(hidden)]
pub enum RemoteRequestState {
    PendingAbort(Instant),
    Active(Arc<RequestCancellationLifetime>),
    Completed(Instant),
}

#[derive(Default)]
#[doc(hidden)]
pub struct RemoteRequestRegistry {
    pub states: HashMap<js::RequestId, RemoteRequestState>,
    pending: VecDeque<(Instant, js::RequestId)>,
    completed: VecDeque<(Instant, js::RequestId)>,
}

impl RemoteRequestRegistry {
    fn prune(&mut self) {
        while self.pending.len() > MAX_REMOTE_PENDING_ABORTS
            || self
                .pending
                .front()
                .is_some_and(|(created, _)| created.elapsed() >= REMOTE_ABORT_TTL)
        {
            let (created, request) = self.pending.pop_front().expect("checked pending abort");
            if matches!(
                self.states.get(&request),
                Some(RemoteRequestState::PendingAbort(current)) if *current == created
            ) {
                self.states.remove(&request);
            }
        }
        while self.completed.len() > MAX_REMOTE_COMPLETIONS
            || self
                .completed
                .front()
                .is_some_and(|(created, _)| created.elapsed() >= REMOTE_COMPLETION_TTL)
        {
            let (created, request) = self.completed.pop_front().expect("checked completion");
            if matches!(
                self.states.get(&request),
                Some(RemoteRequestState::Completed(current)) if *current == created
            ) {
                self.states.remove(&request);
            }
        }
    }

    fn pending_abort(&mut self, request: js::RequestId) {
        let created = Instant::now();
        self.states
            .insert(request, RemoteRequestState::PendingAbort(created));
        self.pending.push_back((created, request));
    }

    #[doc(hidden)]
    pub fn completed(&mut self, request: js::RequestId) {
        let created = Instant::now();
        self.states
            .insert(request, RemoteRequestState::Completed(created));
        self.completed.push_back((created, request));
        self.prune();
    }

    /// Record that `request` has been handed to a cell isolate.
    #[doc(hidden)]
    pub fn active(&mut self, request: js::RequestId) -> Arc<RequestCancellationLifetime> {
        let lifetime = RequestCancellationLifetime::from_request_id(request);
        self.states
            .insert(request, RemoteRequestState::Active(lifetime.clone()));
        lifetime
    }

    /// Record and publish a hang-up for `request`, and report whether it
    /// reached an active fetch.
    ///
    /// An `Active` fetch is tombstoned here instead of being left in place.
    /// `fetch_cell` is what normally retires an id, but a caller that
    /// disconnects mid-fetch has that future dropped underneath it, so the
    /// retiring `completed()` never runs. `prune` reclaims only the ids it can
    /// reach through `pending` or `completed`, so without this tombstone the
    /// entry stays in `states` for the life of the process, and one routine
    /// disconnect leaks one entry. A `fetch_cell` that does outlive the abort
    /// enqueues a fresher tombstone, and `prune`'s generation guard discards
    /// the stale enqueue without disturbing the live entry.
    #[doc(hidden)]
    pub fn abort(&mut self, request: js::RequestId) -> bool {
        match self.states.get(&request).cloned() {
            Some(RemoteRequestState::Active(lifetime)) => {
                self.completed(request);
                // Publish while the registry still owns the state transition,
                // so every direct abort reaches the same lifetime handshake.
                lifetime.publish_abort();
                true
            }
            Some(RemoteRequestState::Completed(_) | RemoteRequestState::PendingAbort(_)) => false,
            None => {
                self.pending_abort(request);
                false
            }
        }
    }
}

#[derive(Default)]
struct AlarmRequestRegistry {
    states: BTreeMap<(String, celld_logic::OpId), AlarmRequestState>,
    completed: VecDeque<(String, celld_logic::OpId)>,
}

#[derive(Clone, Copy)]
enum AlarmRequestState {
    PendingAbort,
    Active(js::RequestId),
    Completed,
}

impl AlarmRequestRegistry {
    fn begin(&mut self, cell: String, op: celld_logic::OpId, request: js::RequestId) -> bool {
        let key = (cell, op);
        let cancel = matches!(
            self.states.remove(&key),
            Some(AlarmRequestState::PendingAbort)
        );
        assert!(
            self.states
                .insert(key, AlarmRequestState::Active(request))
                .is_none(),
            "one shell alarm task per core operation"
        );
        cancel
    }

    fn finish(&mut self, cell: &str, op: celld_logic::OpId, request: js::RequestId) {
        let key = (cell.to_string(), op);
        if matches!(
            self.states.get(&key),
            Some(AlarmRequestState::Active(active)) if *active == request
        ) {
            self.states
                .insert(key.clone(), AlarmRequestState::Completed);
            self.completed.push_back(key);
        }
        while self.completed.len() > MAX_ALARM_COMPLETIONS {
            let completed = self.completed.pop_front().expect("checked completion");
            if matches!(
                self.states.get(&completed),
                Some(AlarmRequestState::Completed)
            ) {
                self.states.remove(&completed);
            }
        }
    }

    fn aborting(&mut self, cell: &str, op: celld_logic::OpId) -> Option<js::RequestId> {
        let key = (cell.to_string(), op);
        match self.states.get(&key).copied() {
            Some(AlarmRequestState::Active(request)) => Some(request),
            Some(AlarmRequestState::PendingAbort | AlarmRequestState::Completed) => None,
            None => {
                self.states.insert(key, AlarmRequestState::PendingAbort);
                None
            }
        }
    }
}

struct AlarmRequestGuard {
    registry: Arc<Mutex<AlarmRequestRegistry>>,
    cell: String,
    op: celld_logic::OpId,
    request: js::RequestId,
}

impl Drop for AlarmRequestGuard {
    fn drop(&mut self) {
        self.registry
            .lock()
            .expect("alarm registry poisoned")
            .finish(&self.cell, self.op, self.request);
    }
}

/// The isolate pool's limits, built from the environment here because the
/// decision core never reads it.
///
/// `max_requests` is the node's only bound on stateless memory, and it is
/// live: `Slot::affiliate` counts an affiliation for a request's whole life,
/// `observe` reports it, and `isolate::admit` refuses against it. Unset
/// means unbounded, not unwired — `engine/load-under-pressure.md` measures
/// `CELLD_MAX_REQUESTS=32` admitting 641 rps against a theoretical 640.
pub fn pool_limits() -> celld_logic::isolate::PoolLimits {
    const GROW_AT: usize = 2;
    const SHRINK_UNDER: usize = 1;
    const MAX_CELLS_PER_ISOLATE: usize = 32;
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    celld_logic::isolate::PoolLimits {
        // These thresholds form one hysteresis policy, so they are constants
        // rather than two independently configurable values.
        grow_at: GROW_AT,
        shrink_under: SHRINK_UNDER,
        max_stateless: env_usize("CELLD_MAX_STATELESS_ISOLATES").unwrap_or(cores),
        max_requests: env_usize("CELLD_MAX_REQUESTS"),
        // This is an engine blast-radius policy. The resident-cell and RSS
        // limits are the operator controls for node memory.
        max_cells: MAX_CELLS_PER_ISOLATE,
    }
}

fn env_usize(name: &str) -> Option<usize> {
    crate::env_vars::positive(name).expect("validated positive runtime limit")
}

#[derive(Clone)]
#[doc(hidden)]
pub struct StatelessRuntime {
    pub node: Arc<str>,
    pub region: Arc<str>,
    /// The isolates fetch runs on, entered one turn at a time from whichever
    /// tokio worker is driving the request.
    pub isolates: Arc<crate::pool::Pool>,
}

#[derive(Clone, Copy)]
#[doc(hidden)]
pub enum StatelessVerb {
    Fetch,
    Rpc,
    Queue,
}

impl StatelessVerb {
    fn task_died(self, error: tokio::task::JoinError) -> anyhow::Error {
        match self {
            Self::Fetch => anyhow!("stateless request task died: {error}"),
            Self::Rpc => anyhow!("stateless RPC task died: {error}"),
            Self::Queue => anyhow!("stateless queue task died: {error}"),
        }
    }

    fn dropped_result(self) -> anyhow::Error {
        match self {
            Self::Fetch => anyhow!("stateless Worker dropped response"),
            Self::Rpc => anyhow!("stateless Worker dropped RPC result"),
            Self::Queue => anyhow!("stateless Worker dropped queue result"),
        }
    }
}

struct CellHandle {
    epoch: u64,
    /// The application generation whose isolate holds this cell, reported
    /// when the cell leaves for a swap.
    generation: GenerationId,
    startup_us: u64,
    /// The cell's claim on the isolate holding its realm. An event knows
    /// where to run from it, and dropping it gives the placement back.
    residency: crate::pool::Residency,
    /// The source value and wake revision stay together in the registry.
    /// Reading a newer flusher revision beside an older cached value would
    /// authorize that old value to delete a later arm's entry.
    alarm: celld_logic::wake::AlarmSnapshot,
    requests: Arc<CellRequestAdmission>,
}

struct CellRequestAdmission {
    in_flight: AtomicUsize,
    saturated: AtomicBool,
    limit: usize,
}

impl CellRequestAdmission {
    fn acquire(self: &Arc<Self>) -> Option<CellRequestPermit> {
        let admitted = self
            .in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                (held < self.limit).then_some(held + 1)
            })
            .is_ok();
        if admitted {
            // A successful admission proves that the target left its prior
            // saturated interval. Reset here as well as on release because a
            // refusal and a release can race between the failed count check
            // and the saturation transition.
            self.saturated.store(false, Ordering::Release);
        }
        admitted.then(|| CellRequestPermit(self.clone()))
    }
}

struct CellRequestPermit(Arc<CellRequestAdmission>);

impl Drop for CellRequestPermit {
    fn drop(&mut self) {
        let held = self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
        if held == self.0.limit {
            self.0.saturated.store(false, Ordering::Release);
        }
    }
}

struct AdmittedCellRequest {
    affiliation: crate::pool::Affiliation,
    permit: CellRequestPermit,
}

/// How a turn's alarm move reaches the host. `drive_cell` calls it with
/// what `take_alarm_moves` drained; it caches the value on the cell's
/// handle and forwards a real change to the observer.
#[doc(hidden)]
pub type AlarmReporter = Arc<dyn Fn(String, celld_logic::wake::AlarmSnapshot) + Send + Sync>;

/// Each committed installation reaches the host, including same-time re-arms.
/// Duplicate observations of that installation are deduplicated. A
/// scope without a handle was stopped mid-flight; its ActivityFinished
/// report is gone with it, so a move for it says nothing and is dropped.
///
/// The observer is called under the registry lock, and `with_alarm` reads
/// and reports under the same lock, so what reaches the core is monotone
/// with the cache: a stale end-of-request read cannot land *after* a
/// fresher report and unarm an alarm the core just learned about — the
/// core overwrites on observation and deletes the wake entry on `None`,
/// so that ordering loses the alarm outright. The observer only sends on
/// an unbounded channel, so holding the lock across it cannot block.
fn alarm_reporter(cells: &Arc<Mutex<CellRegistry>>, observe: &AlarmObserver) -> AlarmReporter {
    let cells_ = cells.clone();
    let observe_ = observe.clone();
    Arc::new(
        move |scope: String, alarm: celld_logic::wake::AlarmSnapshot| {
            let mut registry = cells_.lock().expect("cell registry poisoned");
            let handle = if let Some(handle) = registry.published.get_mut(&scope) {
                Some(handle)
            } else {
                registry.starting.get_mut(&scope)
            };
            if let Some(handle) = handle {
                let changed = handle.alarm.at_ms() != alarm.at_ms();
                // Refresh the revision even when a same-time rearm needs no timer
                // change. An ActivityFinished must use that source's snapshot.
                handle.alarm = alarm;
                if changed {
                    observe_(scope, alarm);
                }
            }
        },
    )
}

#[derive(Default)]
struct CellRegistry {
    starting: HashMap<String, CellHandle>,
    published: HashMap<String, CellHandle>,
}

#[derive(Clone)]
pub struct RuntimeManager {
    /// What the node's cell runtime holds whichever engine runs the cells.
    core: crate::cell_runtime::CellRuntime,
    cells: Arc<Mutex<CellRegistry>>,
    alarm_reporter: AlarmReporter,
    /// A peer abort can arrive before the forwarded fetch. The tombstone and
    /// cell enqueue share this lock so neither ordering can lose cancellation.
    remote_requests: Arc<Mutex<RemoteRequestRegistry>>,
    /// Shell alarm tasks keyed by the core firing operation. A shutdown
    /// cancellation must target the firing it observed, not a later retry.
    alarm_requests: Arc<Mutex<AlarmRequestRegistry>>,
    max_cell_requests: usize,
}

/// The engine-neutral half, so a method of either reads as the manager's own.
impl std::ops::Deref for RuntimeManager {
    type Target = crate::cell_runtime::CellRuntime;
    fn deref(&self) -> &crate::cell_runtime::CellRuntime {
        &self.core
    }
}

/// The engine's part of `Generation::build`: V8 starts once per process.
pub(crate) fn init_engine() {
    init_v8();
}

/// The isolates a script's cells live in, built from the script's config.
pub(crate) fn cell_pool(config: Arc<WorkerConfig>) -> Arc<crate::pool::Pool> {
    Arc::new(crate::pool::Pool::new(
        pool_limits(),
        admission_wait(),
        Box::new(move || load_cell_isolate(config.clone())),
    ))
}

/// Wait for one cell fetch reply without retaining the internal receiver after
/// its external caller disconnects. Dropping that receiver is the direct wake
/// for a reply that already belongs to a detached wake-entry gate task.
async fn receive_cell_fetch_reply(
    mut receive: tokio::sync::oneshot::Receiver<anyhow::Result<HttpResponse>>,
    cancellation: Option<Arc<RequestCancellationLifetime>>,
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
) -> anyhow::Result<HttpResponse> {
    let received = match (cancellation, cancel) {
        (Some(cancellation), Some(mut cancel)) => asyncrt::select_biased! {
            "a completed cell reply wins a tie with an external disconnect";
            result = &mut receive => result,
            cancelled = &mut cancel => match cancelled {
                Ok(()) => {
                    // The driver reads this between turns. Dropping the
                    // receiver also wakes a reply that has already left V8
                    // and is waiting only on its event's durability gates.
                    cancellation.publish_abort();
                    drop(receive);
                    return Err(anyhow!("The client has disconnected"));
                }
                Err(_) => receive.await,
            }
        },
        _ => receive.await,
    };
    received.context("cell isolate dropped response")?
}

async fn receive_service_fetch_response(
    response: impl std::future::Future<Output = anyhow::Result<HttpResponse>>,
    cancellation: Arc<RequestCancellationLifetime>,
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
) -> anyhow::Result<HttpResponse> {
    match cancel {
        Some(mut cancel) => crate::asyncrt::select_biased! {
            "a completed service response wins a tie with an external disconnect";
            response = response => response,
            _ = &mut cancel => {
                cancellation.publish_abort();
                Err(anyhow!("service-binding caller disconnected"))
            }
        },
        None => response.await,
    }
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub async fn receive_cell_fetch_reply_for_test(
    receive: tokio::sync::oneshot::Receiver<anyhow::Result<HttpResponse>>,
    request_id: Option<js::RequestId>,
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
) -> anyhow::Result<HttpResponse> {
    let cancellation = request_id.map(RequestCancellationLifetime::from_request_id);
    receive_cell_fetch_reply(receive, cancellation, cancel).await
}

#[cfg(celld_internal_tests)]
#[derive(Clone)]
#[doc(hidden)]
pub struct RequestCancellationLifetimeForTest(Arc<RequestCancellationLifetime>);

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn request_cancellation_lifetime_for_test(
    request_id: js::RequestId,
) -> RequestCancellationLifetimeForTest {
    RequestCancellationLifetimeForTest(RequestCancellationLifetime::from_request_id(request_id))
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn finish_request_cancellation_for_test(lifetime: &RequestCancellationLifetimeForTest) {
    lifetime.0.finish();
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn finish_active_request_cancellation_for_test(lifetime: &Arc<RequestCancellationLifetime>) {
    lifetime.finish();
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub async fn receive_cell_fetch_reply_with_lifetime_for_test(
    receive: tokio::sync::oneshot::Receiver<anyhow::Result<HttpResponse>>,
    lifetime: RequestCancellationLifetimeForTest,
    cancel: tokio::sync::oneshot::Receiver<()>,
    entered: tokio::sync::oneshot::Sender<()>,
) -> anyhow::Result<HttpResponse> {
    let _ = entered.send(());
    receive_cell_fetch_reply(receive, Some(lifetime.0), Some(cancel)).await
}

#[cfg(all(test, celld_internal_tests))]
pub(crate) async fn receive_service_fetch_response_for_test(
    response: impl std::future::Future<Output = anyhow::Result<HttpResponse>>,
    lifetime: Arc<RequestCancellationLifetime>,
    cancel: tokio::sync::oneshot::Receiver<()>,
) -> anyhow::Result<HttpResponse> {
    receive_service_fetch_response(response, lifetime, Some(cancel)).await
}

impl RuntimeManager {
    /// A deployment with no Durable Object classes can never land a Worker fetch
    /// on a cell, so the core's round-robin routing always returns `None`. Lets
    /// the request path skip the core round-trip entirely for stateless workers.
    pub fn has_cell_classes(&self) -> bool {
        self.generation().has_cell_classes()
    }

    /// The reserved cell carrying the current deployment's cron schedule, or
    /// `None` when it declares no `triggers.crons`. Asked after every
    /// adoption so the schedule is armed without a request.
    pub fn cron_cell(&self) -> Option<String> {
        self.generation().cron_cell()
    }

    /// Start the node's cell runtime around its first generation.
    ///
    /// The generation is built before this is called, by `Generation::build`,
    /// which is also what a reload calls: a deployment reaches a running
    /// node through exactly one function whether the node is booting or
    /// adopting. What starts here is the node-level state that outlives any
    /// generation — the cell registry, replication, the wake flusher.
    pub fn start(generation: Generation, options: RuntimeOptions) -> anyhow::Result<Self> {
        let RuntimeOptions {
            data_dir,
            replication,
            wake,
            alarm_observer,
            node,
            region,
            bucket,
        } = options;
        let max_cell_requests =
            crate::env_vars::positive_or("CELLD_MAX_CELL_REQUESTS", DEFAULT_MAX_CELL_REQUESTS)?;
        let generation = Arc::new(generation);
        let core = crate::cell_runtime::CellRuntime::new(
            generation.clone(),
            data_dir,
            replication,
            wake,
            alarm_observer,
            node,
            region,
        )?;
        let cells = Arc::new(Mutex::new(CellRegistry::default()));
        let alarm_reporter = alarm_reporter(&cells, &core.alarm_observer);
        let manager = Self {
            core,
            cells,
            alarm_reporter,
            remote_requests: Arc::new(Mutex::new(RemoteRequestRegistry::default())),
            alarm_requests: Arc::new(Mutex::new(AlarmRequestRegistry::default())),
            max_cell_requests,
        };
        manager.watch_generation(&generation);
        crate::container::configure(
            manager.node.to_string(),
            bucket,
            manager.data_dir.as_ref().clone(),
        );
        install_container_specs(&generation);
        Ok(manager)
    }

    /// Make `generation` current: the flip.
    ///
    /// From the moment this returns, new stateless requests, new cell
    /// activations, ingress asset lookups, service-binding resolution, and
    /// queue dispatch use the new generation. The previous one stops taking
    /// new work and drains; it is dropped once every isolate it built has
    /// been freed. Requests and cells already on it finish there.
    pub fn adopt(&self, generation: Generation) -> Arc<Generation> {
        let next = Arc::new(generation);
        self.watch_generation(&next);
        install_container_specs(&next);
        let previous = {
            let mut generations = self.generations.write().expect("generation lock poisoned");
            let previous = std::mem::replace(&mut generations.current, next.clone());
            // Installed in the same instant the new one becomes current. A
            // call from an isolate of the previous generation must never
            // find neither, because `generation_by_id` answers "neither"
            // with the current generation and the call would cross.
            generations.draining.push(previous.clone());
            previous
        };
        tracing::info!(
            event = "deployment_generation_adopted",
            generation = next.id,
            version = %next.version,
            prefix = %next.prefix,
            previous_generation = previous.id,
            previous_version = %previous.version,
            "application generation adopted"
        );
        // After the install, never before: retiring walks every pool of the
        // previous generation and this must not hold the generation lock,
        // which every request reads.
        previous.retire();
        next
    }

    /// Spawn the maintenance loop for one generation's cell pools.
    ///
    /// The loop holds a weak reference. A strong one would keep a superseded
    /// generation — its compiled scripts and every pool — alive for the life
    /// of the process, so the loop ends when the generation is dropped. The
    /// draining list is what keeps a superseded generation alive until then,
    /// and this loop is what removes it from that list once every isolate it
    /// built has been freed.
    fn watch_generation(&self, generation: &Arc<Generation>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let reaping = Arc::downgrade(generation);
        let generations = self.generations.clone();
        handle.spawn(async move {
            let mut tick = tokio::time::interval_at(
                tokio::time::Instant::now() + REAP_INTERVAL,
                REAP_INTERVAL,
            );
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some(generation) = reaping.upgrade() else {
                    return;
                };
                generation.reap_cell_pools();
                if !generation.is_drained() {
                    continue;
                }
                let mut generations = generations.write().expect("generation lock poisoned");
                if let Some(index) = generations
                    .draining
                    .iter()
                    .position(|candidate| Arc::ptr_eq(candidate, &generation))
                {
                    generations.draining.remove(index);
                    tracing::info!(
                        event = "deployment_generation_freed",
                        generation = generation.id,
                        version = %generation.version,
                        "application generation freed"
                    );
                }
            }
        });
    }

    pub async fn fetch_worker(
        &self,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> anyhow::Result<HttpResponse> {
        // The snapshot is held for the whole request, so the generation it
        // started on outlives it even after the node adopts another.
        let generation = self.generation();
        generation
            .stateless
            .fetch(url, method, body.into(), headers, None)
            .await
    }

    /// Dispatch a cancellable top-level Worker request to the stateless pool.
    pub fn fetch_worker_pool(
        &self,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        cancellation: Arc<RequestCancellationLifetime>,
    ) -> impl std::future::Future<Output = anyhow::Result<HttpResponse>> + Send + 'static {
        let generation = self.generation();
        let fetching = generation
            .stateless
            .fetch(url, method, body, headers, Some(cancellation));
        async move {
            let _generation = generation;
            fetching.await
        }
    }

    /// Dispatch a top-level Worker request on the exact resident runtime the
    /// decision core reserved. The activity token pins that lifecycle choice
    /// until the queued event has completely left the isolate loop.
    pub async fn fetch_worker_on_cell(
        &self,
        cell: String,
        epoch: u64,
        request: RuntimeFetch,
        inline_activity: crate::CellActivityGuard,
    ) -> anyhow::Result<HttpResponse> {
        let RuntimeFetch {
            url,
            method,
            body,
            headers,
            request_id,
            // A resident Worker fetch is not a cell event; its inbound
            // traceparent is honored by the drive, from the headers.
            order: _,
            parent: _,
        } = request;
        let request_id = request_id.context("resident Worker fetch requires a request id")?;
        let isolate = self
            .cells
            .lock()
            .expect("cell registry poisoned")
            .published
            .get(&cell)
            .filter(|handle| handle.epoch == epoch)
            // Affiliated under the registry lock for the driver's whole
            // lifetime, exactly like cell_isolate (denoland/celld#147).
            .map(|handle| handle.residency.slot().affiliate())
            .ok_or_else(|| anyhow!("cell runtime is not published at epoch {epoch}: {cell}"))?;
        // The Worker entry, run in the isolate that hosts the cell it will
        // route to. The call still goes out through the host -- every cell
        // dispatch does -- but it comes back to an isolate that is already
        // warm for this cell, with its storage open and its instance live.
        //
        // It needs no rescheduling any more. A Worker fetch could not be
        // nested inside an actor event, and delivery used to nest, so a job
        // that arrived mid-event had to be handed back to the stateless
        // pool. Events no longer nest: this is an entry like any other, and
        // it waits for its turn rather than for the isolate to go idle.
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Fetch {
            queued_at: Instant::now(),
            entrypoint: None,
            invocation_limits: None,
            url,
            method,
            body,
            headers,
            request_id: Some(request_id),
            tail_report: None,
            reply,
        };
        tokio::spawn(async move {
            let _inline_activity = inline_activity;
            drive_worker_on_cell(isolate, job).await;
        });
        receive
            .await
            .context("cell isolate dropped Worker response")?
    }

    /// Call a service binding. `generation` is the caller's, so the target
    /// comes from the deployment graph the caller was built with.
    pub async fn fetch_service(&self, call: ServiceFetch) -> anyhow::Result<HttpResponse> {
        let ServiceFetch {
            generation,
            script,
            entrypoint,
            url,
            method,
            body,
            headers,
            cancel,
        } = call;
        let generation = self.generation_by_id(generation);
        let pool = generation
            .service(&script)
            .ok_or_else(|| anyhow!("no service Worker for script {script}"))?;
        let cancellation = RequestCancellationLifetime::stateless();
        let response = pool.fetch_target(
            entrypoint,
            url,
            method,
            body,
            headers,
            Some(cancellation.clone()),
        );
        receive_service_fetch_response(response, cancellation, cancel).await
    }

    pub async fn rpc_service(
        &self,
        generation: GenerationId,
        script: &str,
        entrypoint: String,
        props: Vec<u8>,
        operation: crate::WorkerRpcOperation,
    ) -> anyhow::Result<Vec<u8>> {
        let generation = self.generation_by_id(generation);
        generation
            .service(script)
            .ok_or_else(|| anyhow!("no service Worker for script {script}"))?
            .rpc(entrypoint, props, operation)
            .await
    }

    /// Dispatch one broker-leased batch to its attached consumer script.
    pub async fn queue_service(
        &self,
        generation: GenerationId,
        script: &str,
        batch: js::QueueBatch,
    ) -> anyhow::Result<js::QueueDispatchResult> {
        let generation = self.generation_by_id(generation);
        generation
            .service(script)
            .ok_or_else(|| anyhow!("no Queue consumer Worker for script {script}"))?
            .queue(batch)
            .await
    }

    pub async fn restore_cell(
        &self,
        cell: &str,
        spec: &celld_logic::RestoreSpec,
    ) -> anyhow::Result<celld_logic::RestoreOutcome> {
        let path = self.db_path(cell, spec.epoch);
        self.facets.register(cell, spec);
        if let Some(replication) = &self.replication {
            let (restored_path, restored, vfs) = replication.restore(cell, spec, true).await?;
            if restored_path != path {
                return Err(anyhow!(
                    "replication restored {} instead of {}",
                    restored_path.display(),
                    path.display()
                ));
            }
            return Ok(celld_logic::RestoreOutcome {
                restored,
                alarm: self.restored_alarm(cell, &path, vfs.as_deref()).await,
            });
        }
        let parent = path.parent().context("cell database has no parent")?;
        let parent = parent.to_path_buf();
        let parent_display = parent.display().to_string();
        let filesystem = asyncrt::fs();
        asyncrt::blocking(move || filesystem.create_dir_all(&parent))
            .await?
            .with_context(|| format!("create cell data directory {parent_display}"))?;
        Ok(celld_logic::RestoreOutcome {
            restored: false,
            alarm: self.restored_alarm(cell, &path, None).await,
        })
    }

    /// The alarm the restored database already had armed, read directly by
    /// path. Read-only, and the connection is dropped here -- the isolate
    /// opens the same file moments later through `spawn_cell`.
    async fn restored_alarm(
        &self,
        cell: &str,
        path: &std::path::Path,
        vfs: Option<&str>,
    ) -> Option<celld_logic::RestoredAlarm> {
        // A paged cell's alarm read faults pages in on the reading thread, so
        // it runs on a blocking thread rather than on this runtime worker.
        let path_ = path.to_string_lossy().into_owned();
        let cell_ = cell.to_string();
        let vfs_ = vfs.map(str::to_string);
        let persisted = asyncrt::blocking(move || {
            crate::storage::persisted_alarm(&path_, &cell_, vfs_.as_deref())
        })
        .await
        .ok()
        .flatten();
        restored_alarm_from_persisted(cell, persisted, |at_ms| {
            self.alarm_covered(
                cell,
                celld_logic::wake::AlarmSnapshot::without_wake(Some(at_ms)),
            )
        })
    }

    /// Detach every resident runtime, prove and retain its exact database path,
    /// and remove stale live-named epochs. The caller has already stopped
    /// admission and drained request effects.
    pub async fn prepare_clean_reload(
        &self,
        cells: &[celld_logic::PresenceCell],
    ) -> anyhow::Result<usize> {
        let replication = self
            .replication
            .as_ref()
            .context("clean reload requires replication")?;
        let keep: BTreeSet<_> = cells
            .iter()
            .map(|cell| (cell.id.clone(), cell.epoch))
            .collect();
        anyhow::ensure!(
            keep.len() == cells.len(),
            "clean reload resident inventory contains duplicates"
        );
        let mut closes = futures_util::stream::iter(cells.iter().cloned())
            .map(|cell| {
                let runtime = self.clone();
                let replication = replication.clone();
                async move {
                    // A normal stop releases the replica before returning.
                    // Keep it registered until the final capture proves the
                    // stopped database position present in the bucket.
                    runtime.swap_out_cell(&cell.id, cell.epoch).await?;
                    for facet in runtime.facets.stopping(&cell.id, cell.epoch) {
                        replication.close_for_reload(&facet, cell.epoch).await?;
                        runtime.facets.stopped(&cell.id, cell.epoch, &facet);
                    }
                    replication.close_for_reload(&cell.id, cell.epoch).await?;
                    runtime.facets.forget(&cell.id, cell.epoch);
                    anyhow::Ok(())
                }
            })
            .buffer_unordered(128);
        while let Some(result) = closes.next().await {
            result?;
        }
        let pruned = replication.prune_stale_live(&keep)?;
        Ok(pruned)
    }

    /// Take a cell out of its isolate for a generation swap.
    ///
    /// The first half of `stop_cell` and nothing more: close the application
    /// database in the isolate that holds it, and drop the residency so the
    /// isolate can be reclaimed once its last cell leaves. The replica keeps
    /// its handle and its file, the owner record keeps its epoch, and the
    /// core starts the cell again on the current generation as soon as it
    /// hears the stop. A cell already gone is not an error: the swap and an
    /// eviction can race, and the eviction's stop did this work.
    pub async fn swap_out_cell(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        // The isolates to release, taken by reference rather than by handle.
        // A release that fails can leave this cell's database open in that
        // realm, and the handle owns both the residency that keeps the realm
        // from being reclaimed and the registry entry a retry finds it
        // through -- so only a release that succeeded gives them up.
        let slots: Vec<(Arc<crate::pool::Slot>, GenerationId)> = {
            let cells = self.cells.lock().expect("cell registry poisoned");
            [&cells.starting, &cells.published]
                .into_iter()
                .filter_map(|residents| residents.get(cell))
                .filter(|handle| handle.epoch == epoch)
                .map(|handle| (handle.residency.slot().clone(), handle.generation))
                .collect()
        };
        for (slot, generation) in slots {
            // Taking the isolate for this turn is the barrier: an event of
            // this cell either finished its turn before it or has not
            // started one, so closing its SQLite cannot land mid-handler.
            slot.turn(|worker| {
                #[cfg(debug_assertions)]
                if let Some(error) = crate::engine_api::injected_swap_release_failure() {
                    return Err(error);
                }
                worker.own_cell(cell, None)
            })
            .await
            .with_context(|| format!("release {cell} from its isolate for a generation swap"))?;
            tracing::info!(
                event = "generation_swap_out",
                scope = %cell,
                epoch,
                generation,
                "cell left its isolate for a generation swap"
            );
        }
        // Released, so the placements can go back. Dropping each handle drops
        // its residency, which is what lets the isolate be reclaimed once its
        // last cell has left.
        let mut cells = self.cells.lock().expect("cell registry poisoned");
        let CellRegistry {
            starting,
            published,
        } = &mut *cells;
        for residents in [starting, published] {
            if residents
                .get(cell)
                .is_some_and(|handle| handle.epoch == epoch)
            {
                residents.remove(cell);
            }
        }
        Ok(())
    }

    /// Materialize an isolate and retain it as non-routable until publication.
    /// Start a cell's runtime and report which isolate took its realm and the
    /// generation it belongs to. The core groups eviction on the isolate and
    /// the swap pump keys on the generation, so both come back from here.
    pub async fn start_cell(
        &self,
        cell: String,
        epoch: u64,
        fresh: bool,
    ) -> anyhow::Result<(celld_logic::isolate::HeapId, GenerationId)> {
        let db_path = self.db_path(&cell, epoch);
        let class = cell
            .split_once(':')
            .map(|(class, _)| class)
            .ok_or_else(|| anyhow!("cell scope has no class: {cell}"))?;
        let generation = self.generation();
        let config = generation
            .cell_config(class)
            .ok_or_else(|| anyhow!("no Worker exports Durable Object class {class}"))?;
        // The object's `running` is read synchronously, so the engine
        // answers it here, before the object exists. A container a previous
        // activation on this node left running is adopted by name.
        if config.container_class(class) {
            let engine = crate::container::engine().await?;
            engine.attach(&cell, class).await?;
        }
        let startup_timing = CellIsolateStartupTiming {
            started: Instant::now(),
            scope: cell.clone(),
            node: self.node.clone(),
            region: self.region.clone(),
            epoch,
            fresh,
        };

        let isolates = generation
            .cell_isolates(&config.script_name)
            .ok_or_else(|| anyhow!("no cell isolates for script {}", config.script_name))?;
        // Building an isolate compiles the script, so it runs on a blocking
        // thread: the pool builds before taking its lock, but the caller is a
        // tokio worker either way.
        let placed = {
            let isolates = isolates.clone();
            tokio::task::spawn_blocking(move || isolates.place_cell())
                .await
                .context("cell placement panicked")?
        };
        let residency = match placed {
            Ok(residency) => residency,
            Err(error) => {
                startup_timing.emit("error", "worker_load");
                return Err(error);
            }
        };
        let placed_in = residency.slot().heap_id();

        // Everything the cell needs that the isolate must do: open its
        // SQLite — which the isolate owns, not the caller — and restore its
        // persisted id name. A paged restore leaves the file sparse behind
        // the activation's VFS, so the actor's connection must open through
        // it too.
        //
        // A direct call rather than a job: adoption is not an event, it runs
        // no handler, and it needs one turn.
        // The activation's own answer, not a guess from an absent handle: a
        // cell removed between restore and adoption must not be opened
        // plainly over a sparse or missing file.
        let paged_vfs = match self.replication.as_ref() {
            Some(replication) => match replication.ltx().activation_vfs(&cell, epoch) {
                Ok(vfs) => vfs,
                Err(error) => {
                    startup_timing.emit("error", "storage_open");
                    return Err(error);
                }
            },
            None => None,
        };
        let adopted = residency
            .adopt(
                &cell,
                CellStorage {
                    path: path_text(&db_path),
                    epoch,
                    replicated_wake: self.wake.is_some(),
                    vfs: paged_vfs.as_deref(),
                },
            )
            .await;
        let (residency, alarm) = match adopted {
            Ok(adopted) => adopted,
            Err(error) => {
                startup_timing.emit("error", "storage_open");
                return Err(error);
            }
        };
        (self.alarm_observer)(cell.clone(), alarm);
        let startup_us = startup_timing.emit("ready", "");

        {
            let mut cells = self.cells.lock().expect("cell registry poisoned");
            if cells.starting.contains_key(&cell) || cells.published.contains_key(&cell) {
                return Err(anyhow!("cell runtime already exists: {cell}"));
            }
            cells.starting.insert(
                cell.clone(),
                CellHandle {
                    epoch,
                    generation: generation.id,
                    startup_us,
                    residency,
                    alarm,
                    requests: Arc::new(CellRequestAdmission {
                        in_flight: AtomicUsize::new(0),
                        saturated: AtomicBool::new(false),
                        limit: self.max_cell_requests,
                    }),
                },
            );
            Ok((placed_in, generation.id))
        }
    }

    /// Make the exact started generation visible to request dispatch.
    pub fn publish_cell(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        let mut cells = self.cells.lock().expect("cell registry poisoned");
        if cells
            .starting
            .get(cell)
            .is_none_or(|handle| handle.epoch != epoch)
        {
            return Err(anyhow!("no started cell runtime for {cell} epoch {epoch}"));
        }
        let handle = cells
            .starting
            .remove(cell)
            .expect("checked started runtime");
        let startup_us = handle.startup_us;
        if let Some(replaced) = cells.published.insert(cell.to_string(), handle) {
            // Nothing to shut down: the isolate serves other cells, and
            // dropping the handle drops the residency that held its place.
            drop(replaced);
            return Err(anyhow!("replaced published cell runtime for {cell}"));
        }
        drop(cells);
        tracing::info!(
            event = "cell_runtime_publication",
            outcome = "published",
            scope = %cell,
            node = %self.node,
            region = %self.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            epoch,
            isolate_startup_us = startup_us,
            "cell runtime published"
        );
        Ok(())
    }

    pub(crate) async fn stop_cell(
        &self,
        cell: &str,
        epoch: u64,
        mode: StopMode,
    ) -> anyhow::Result<()> {
        // No facet opens once the stop begins; the facet loop below stops
        // the streams this answers again, with any that opened before.
        self.facets.stopping(cell, epoch);
        if let Some(engine) = crate::container::engine_if_ready() {
            // Only an idle eviction keeps the cell on this node, so only it
            // keeps the container: the next activation here reconnects.
            let release = match &mode {
                StopMode::Evict {
                    preserve_local: true,
                    ..
                } => crate::container::Release::Keep,
                _ => crate::container::Release::Destroy,
            };
            engine.release(cell, release).await;
        }
        let mut stopped = Vec::new();
        {
            let mut cells = self.cells.lock().expect("cell registry poisoned");
            if cells
                .starting
                .get(cell)
                .is_some_and(|handle| handle.epoch == epoch)
            {
                if let Some(handle) = cells.starting.remove(cell) {
                    stopped.push(handle);
                }
            }
            if cells
                .published
                .get(cell)
                .is_some_and(|handle| handle.epoch == epoch)
            {
                if let Some(handle) = cells.published.remove(cell) {
                    stopped.push(handle);
                }
            }
        }
        for handle in stopped {
            // Give the cell back rather than shutting the isolate down: it
            // serves other cells. Taking the isolate for this turn is the
            // barrier — an event of this cell either finished its turn
            // before it, or has not started one — so closing its SQLite
            // cannot land under a handler that is mid-turn.
            // The cell's own lane, not the shared stateless one: this stop
            // follows the turns already admitted for this cell, and it does
            // not wait behind every stateless turn queued on the isolate. On
            // a loaded node that queue held a handoff batch for minutes.
            let _ = handle
                .residency
                .slot()
                .turn_cell(cell, |worker| worker.own_cell(cell, None))
                .await;
            // Dropping the handle drops its residency, which is what gives
            // the isolate its place back — and what lets `retire` reclaim
            // the isolate once no cell is left in it.
            drop(handle);
        }
        let Some(replication) = &self.replication else {
            self.facets.forget(cell, epoch);
            return Ok(());
        };
        // The root and its facets stop in the order `FacetStreams::stop_root`
        // owns, which a test drives through its failures.
        let stop_root = || async {
            // Every stop releases the handle, and this does not consult
            // `stopped_runtime`. The replication entry is created by
            // `Effect::Restore` and the registry entry by
            // `Effect::StartRuntime`, so the entry outlives a start that fails
            // between the two and there is nothing else that would ever remove
            // it. Its lifetime is the activation's, not the registry's.
            //
            // The mode pairs the durability authority with the file outcome.
            // Passing these as independent booleans let cleanup close a proved
            // remote restore in place, where no later activation could use it.
            match &mode {
                // A failed final sync leaves the handle and files in place.
                // This call is therefore intentionally retryable after the
                // runtime itself has already stopped. An abandoned one is not:
                // it returns `EvictionAbandoned`, and the caller ends the stop
                // instead of trying again.
                StopMode::Evict {
                    preserve_local,
                    abandon,
                } => replication
                    .evict(cell, epoch, *preserve_local, abandon.as_deref())
                    .await
                    .map(|_| ()),
                StopMode::Rebase => replication.release(cell, epoch).await,
                StopMode::CloseInPlace => replication.close_in_place(cell, epoch).await,
                StopMode::Discard => {
                    replication.ltx().discard(cell, epoch);
                    Ok(())
                }
            }
        };
        let stop_facet = |facet: String| {
            let mode = &mode;
            async move {
                // A retry after the facet's stream stopped, or after shutdown
                // took the handle, has nothing left to stop.
                if !replication.ltx().is_resident(&facet, epoch) {
                    return Ok(());
                }
                match mode {
                    // A facet keeps no local file past an eviction: its handler
                    // writes through a connection of its own, which can outlive
                    // the root's stop, and a preserved file would become the
                    // next activation's baseline. The root is gone by now, so
                    // the eviction is not abandoned.
                    StopMode::Evict { .. } => replication
                        .evict(&facet, epoch, false, None)
                        .await
                        .map(|_| ()),
                    StopMode::Rebase => replication.release(&facet, epoch).await,
                    StopMode::CloseInPlace => replication.close_in_place(&facet, epoch).await,
                    StopMode::Discard => {
                        replication.ltx().discard(&facet, epoch);
                        Ok(())
                    }
                }
            }
        };
        self.facets
            .stop_root(
                cell,
                epoch,
                replication.ltx().is_resident(cell, epoch),
                stop_root,
                stop_facet,
            )
            .await
    }

    pub async fn fetch_cell(
        &self,
        cell: String,
        name: Option<String>,
        request: RuntimeFetch,
        cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> anyhow::Result<HttpResponse> {
        let RuntimeFetch {
            url,
            method,
            body,
            headers,
            request_id,
            order,
            parent,
        } = request;
        let admitted = match self.admit_cell_request(&cell)? {
            Some(admitted) => admitted,
            None => {
                return Ok(HttpResponse {
                    status: 503,
                    headers: vec![
                        ("retry-after".to_string(), "1".to_string()),
                        ("x-celld-overload".to_string(), "cell".to_string()),
                    ],
                    body: b"cell request limit reached".to_vec(),
                    websocket: None,
                    stream: None,
                    write_position: None,
                    observed_position: None,
                });
            }
        };
        let AdmittedCellRequest {
            affiliation,
            permit,
        } = admitted;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::Fetch {
            request_id,
            scope: cell,
            name,
            url,
            method,
            body,
            headers,
            reply,
            order,
        };
        let cancellation = if let Some(request_id) = request_id {
            let mut requests = self
                .remote_requests
                .lock()
                .expect("request registry poisoned");
            requests.prune();
            if matches!(
                requests.states.remove(&request_id),
                Some(RemoteRequestState::PendingAbort(_))
            ) {
                requests.completed(request_id);
                return Err(anyhow!("the client disconnected before dispatch"));
            }
            Some(requests.active(request_id))
        } else {
            None
        };
        let alarm_reporter = self.alarm_reporter.clone();
        let drive_cancellation = cancellation.clone();
        tokio::spawn(async move {
            let _permit = permit;
            drive_cell_with_request_cancellation(
                affiliation,
                job,
                Some(alarm_reporter),
                parent,
                drive_cancellation,
            )
            .await;
        });
        let result = receive_cell_fetch_reply(receive, cancellation, cancel).await;
        if let Some(request_id) = request_id {
            let mut requests = self
                .remote_requests
                .lock()
                .expect("request registry poisoned");
            requests.completed(request_id);
        }
        result
    }

    /// Tell a cell to abandon a fetch, by name.
    ///
    /// `fetch_cell` drops its reply receiver and returns when its explicit
    /// cancellation signal arrives. A caller that learns about a hang-up only
    /// in a destructor has no future left to receive that signal, so it uses
    /// this direct path instead.
    pub fn abort_fetch(&self, cell: &str, request_id: js::RequestId) {
        let mut requests = self
            .remote_requests
            .lock()
            .expect("request registry poisoned");
        requests.prune();
        requests.abort(request_id);
        drop(requests);
        let _ = cell;
    }

    pub fn published_epoch(&self, cell: &str) -> Option<u64> {
        self.cells
            .lock()
            .expect("cell registry poisoned")
            .published
            .get(cell)
            .map(|handle| handle.epoch)
    }

    pub fn alarm(&self, cell: &str) -> Option<i64> {
        self.with_alarm(cell, |at_ms| at_ms)
    }

    /// Read the cell's alarm cache and call `f` before the registry lock is
    /// released. The reporter sends under the same lock, so whatever `f`
    /// sends is ordered with the reporter's sends: a read taken here cannot
    /// reach the core after a fresher report (see `alarm_reporter`).
    pub fn with_alarm<T>(&self, cell: &str, f: impl FnOnce(Option<i64>) -> T) -> T {
        self.with_alarm_snapshot(cell, |alarm| f(alarm.at_ms()))
    }

    pub(crate) fn with_alarm_snapshot<T>(
        &self,
        cell: &str,
        f: impl FnOnce(celld_logic::wake::AlarmSnapshot) -> T,
    ) -> T {
        let cells = self.cells.lock().expect("cell registry poisoned");
        let alarm = cells
            .published
            .get(cell)
            .or_else(|| cells.starting.get(cell))
            .map(|handle| handle.alarm)
            .unwrap_or_else(|| celld_logic::wake::AlarmSnapshot::without_wake(None));
        f(alarm)
    }

    pub async fn fire_alarm(
        &self,
        op: celld_logic::OpId,
        cell: String,
        scheduled_ms: i64,
    ) -> anyhow::Result<(celld_logic::wake::AlarmSnapshot, bool, Option<u64>)> {
        let request_id = js::next_request_id();
        let cancel = self
            .alarm_requests
            .lock()
            .expect("alarm registry poisoned")
            .begin(cell.clone(), op, request_id);
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::Alarm {
            request_id: Some(request_id),
            scope: cell.clone(),
            scheduled_ms,
            claim: js::AlarmDispatch::Due,
            reply,
        };
        let guard = AlarmRequestGuard {
            registry: self.alarm_requests.clone(),
            cell: cell.clone(),
            op,
            request: request_id,
        };
        let isolate = self.cell_isolate(&cell)?;
        let alarm_reporter = self.alarm_reporter.clone();
        let drive = crate::asyncrt::spawn(async move {
            let _guard = guard;
            drive_alarm(isolate, job, Some(alarm_reporter)).await
        });
        if cancel {
            js::abort_request_for_shutdown(request_id);
        }
        let result = receive.await.context("cell isolate dropped alarm result")?;
        match result {
            // A handler that returned settled its claim inside its completion
            // turn (`Answer::Alarm`), so the snapshot and the delta it replied
            // with are already final and nothing below reads the drive's
            // position. The drive itself runs on, because a completed cell
            // event keeps its pending I/O (`completed_cell_event`), exactly as
            // it does after a fetch answers. Waiting for it here held the
            // core in `AlarmState::Firing` until the leftover I/O ended or the
            // operation deadline expired the firing, so an `alarm()` that left
            // one pending timer — what `AbortSignal.timeout` leaves behind
            // after its fetch finished — delayed every re-arm by up to
            // `CELLD_OPERATION_DEADLINE_MS`, and the core booked a handler
            // that succeeded as a stuck one (denoland/celld#228).
            Ok((alarm, wrote)) => {
                watch_detached_alarm_drive(drive, &cell, op);
                Ok((alarm, self.alarm_covered(&cell, alarm), wrote))
            }
            Err(error) => {
                // A cancelled or failed alarm settles its claim in the drive's
                // final isolate turn. The event reply is itself held behind
                // every wake-entry arm, so waiting for the drive makes that
                // final cache authoritative before the core sees completion.
                let final_write = drive.await.expect("cell alarm drive task panicked")?;
                // The drive completed its final isolate turn before this
                // branch. Its alarm cache is therefore authoritative even
                // when the handler failed: `Some` is the automatic retry or
                // an explicit re-arm, and `None` is an explicit change which
                // wins over retry. Returning the old firing as an error would
                // resurrect an alarm which storage no longer contains and
                // leave a draining cell permanently uncovered.
                //
                // The position travels as it does for a success. What the
                // handler committed before it failed, and the retry record
                // the final turn wrote, are unproven writes the core must
                // prove before a reader can reveal them; without a position
                // the alarm settled at once and opened no barrier (#715).
                // A handler that rejected settled its claim before its error
                // left, so the error carries the whole delta; one that
                // failed before or between turns had its record written by
                // the final turn, whose sample is the later one.
                let alarm = self.with_alarm_snapshot(&cell, |alarm| alarm);
                Ok((
                    alarm,
                    self.alarm_covered(&cell, alarm),
                    final_write.max(js::failed_write_position(&error)),
                ))
            }
        }
    }

    pub fn abort_alarm(&self, cell: &str, op: celld_logic::OpId) {
        let request = self
            .alarm_requests
            .lock()
            .expect("alarm registry poisoned")
            .aborting(cell, op);
        if let Some(request) = request {
            js::abort_request_for_shutdown(request);
        }
    }

    /// Run `webSocketOpen`. The answer is the position the handler's writes
    /// reached, or the error carries it, so the caller can open their barrier.
    pub async fn ws_open(
        &self,
        cell: String,
        ws_id: u64,
        protocol: String,
    ) -> anyhow::Result<Option<u64>> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::WsOpen {
            scope: cell.clone(),
            ws_id,
            protocol,
            reply,
        };
        self.cell_event(&cell, job, receive, "cell isolate dropped WebSocket open")
            .await
    }

    pub async fn rpc(
        &self,
        cell: String,
        name: Option<String>,
        method: String,
        args: js::RpcData,
        request_id: Option<js::RequestId>,
    ) -> anyhow::Result<js::RpcOutcome> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::Rpc {
            request_id,
            scope: cell.clone(),
            name,
            method,
            args,
            reply,
        };
        self.cell_event(&cell, job, receive, "cell isolate dropped RPC result")
            .await
    }

    pub async fn ws_message(
        &self,
        cell: String,
        ws_id: u64,
        data: js::WsIn,
    ) -> anyhow::Result<js::WsDispatch> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::WsMessage {
            scope: cell.clone(),
            ws_id,
            data,
            reply,
        };
        self.cell_event(
            &cell,
            job,
            receive,
            "cell isolate dropped WebSocket message",
        )
        .await
    }

    /// Run `webSocketClose`. The answer is the position the handler's writes
    /// reached, or the error carries it, so the caller can open their barrier.
    pub async fn ws_closed(
        &self,
        cell: String,
        ws_id: u64,
        code: u16,
        reason: String,
        was_clean: bool,
    ) -> anyhow::Result<Option<u64>> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::WsClosed {
            scope: cell.clone(),
            ws_id,
            code,
            reason,
            was_clean,
            reply,
        };
        self.cell_event(&cell, job, receive, "cell isolate dropped WebSocket close")
            .await
    }

    /// The isolate a published cell's events run in.
    fn cell_isolate(&self, cell: &str) -> anyhow::Result<crate::pool::Affiliation> {
        // The affiliation is taken UNDER the registry lock, while the
        // cell's Residency provably pins the slot, and it is held by the
        // driver for the event's entire async lifetime — including while
        // the event is suspended awaiting host I/O. Without it, a
        // suspended event holds only a bare Arc<Slot>: stop_cell() drops
        // the Residency, the pool reaps the "drained" isolate, and the
        // resumed event enters a freed worker and aborts the process
        // (denoland/celld#147).
        self.cells
            .lock()
            .expect("cell registry poisoned")
            .published
            .get(cell)
            .map(|handle| handle.residency.slot().affiliate())
            .ok_or_else(|| anyhow!("cell runtime is not published: {cell}"))
    }

    /// Reserve one fetch against a published cell and return its isolate
    /// affiliation with the reservation. Returning either value alone would
    /// let a caller run work that the target did not admit or release the
    /// admission before the event's asynchronous lifetime ended.
    fn admit_cell_request(&self, cell: &str) -> anyhow::Result<Option<AdmittedCellRequest>> {
        let cells = self.cells.lock().expect("cell registry poisoned");
        let handle = cells
            .published
            .get(cell)
            .ok_or_else(|| anyhow!("cell runtime is not published: {cell}"))?;
        let Some(permit) = handle.requests.acquire() else {
            let in_flight = handle.requests.in_flight.load(Ordering::Acquire);
            if !handle.requests.saturated.swap(true, Ordering::AcqRel) {
                tracing::warn!(
                    event = "cell_overload_refused",
                    scope = %cell,
                    node = %self.node,
                    region = %self.region,
                    in_flight,
                    limit = handle.requests.limit,
                    "refused work for a saturated cell"
                );
            }
            return Ok(None);
        };
        Ok(Some(AdmittedCellRequest {
            affiliation: handle.residency.slot().affiliate(),
            permit,
        }))
    }

    /// Start one cell event and wait for its answer.
    ///
    /// The event is driven by its own task, so this future holds nothing
    /// while it waits: the isolate is taken and given back one turn at a
    /// time by `drive_cell`.
    async fn cell_event<T>(
        &self,
        cell: &str,
        job: CellJob,
        receive: tokio::sync::oneshot::Receiver<anyhow::Result<T>>,
        dropped: &'static str,
    ) -> anyhow::Result<T> {
        let isolate = self.cell_isolate(cell)?;
        tokio::spawn(drive_cell(
            isolate,
            job,
            Some(self.alarm_reporter.clone()),
            None,
        ));
        receive.await.context(dropped)?
    }
}

fn restored_alarm_from_persisted(
    _cell: &str,
    persisted: Option<(i64, i64, u32, u32)>,
    covered: impl FnOnce(i64) -> bool,
) -> Option<celld_logic::RestoredAlarm> {
    let at_ms = match persisted {
        Some((at_ms, ..)) => at_ms,
        None => -1,
    };
    if at_ms < 0 {
        return None;
    }
    // The restore hint grants no publication or retirement authority. The
    // opened SQLite connection installs the new writer identity first.
    Some(celld_logic::RestoredAlarm {
        at_ms,
        covered: covered(at_ms),
    })
}

#[cfg(all(test, celld_internal_tests))]
pub(crate) fn restored_alarm_for_test(
    cell: &str,
    path: &std::path::Path,
    covered: bool,
) -> Option<celld_logic::RestoredAlarm> {
    let persisted = crate::storage::persisted_alarm(&path.to_string_lossy(), cell, None);
    restored_alarm_from_persisted(cell, persisted, |_| covered)
}

/// The caller's trace context, when the ingress request carried one.
/// Malformed headers are ignored; nothing here is trusted for anything
/// but correlation and (under parentbased samplers, deliberately) the
/// sampling decision.
fn inbound_parent(job: &crate::WorkerJob) -> Option<crate::telemetry::ParentContext> {
    if !crate::telemetry::active() {
        return None;
    }
    let crate::WorkerJob::Fetch { headers, .. } = job else {
        return None;
    };
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("traceparent"))
        .and_then(|(_, value)| crate::telemetry::parse_traceparent(value))
}

/// An op in flight, carrying the id of the promise it will resolve.
type PendingOp = std::pin::Pin<
    Box<dyn std::future::Future<Output = (u64, Result<asyncrt::OpOut, String>)> + Send>,
>;

/// The ops one request is waiting on. Per request, not per isolate: that is
/// what deleting the pump means, and it is why no attribution table exists.
type Ops = futures_util::stream::FuturesUnordered<PendingOp>;

fn adopt(ops: &mut Ops, started: Vec<js::Op>) {
    for (id, future) in started {
        ops.push(Box::pin(async move { (id, future.await) }));
    }
}

/// Cancel native futures and remove their JavaScript resolvers together.
///
/// A failed handler can still own a gated reply, but none of its handler ops
/// can re-enter the isolate while that detached gate waiter finishes.
fn abort_ops(ops: &mut Ops, entry: &mut js::InFlight) {
    ops.clear();
    entry.abandon();
}

/// Drive one stateless request to completion, one turn at a time.
///
/// The loop the pump used to run, owned by the request instead. Between turns
/// it holds no isolate — only its affiliation, which is memory rather than
/// CPU — so a handler awaiting I/O stops nothing else in that isolate.
/// This compatibility entry owns any cancellation lifetime that it creates.
/// A caller with an abort publisher must use
/// `RequestCancellationLifetime::drive_stateless_fetch` instead.
#[doc(hidden)]
pub fn drive(
    slot: Arc<crate::pool::Slot>,
    job: crate::WorkerJob,
    telemetry: Option<(Arc<str>, Arc<str>)>,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    drive_affiliated(
        slot.affiliate(),
        StatelessWorkerJob::driver_owned(job),
        telemetry,
    )
}

fn drive_affiliated(
    affiliation: crate::pool::Affiliation,
    job: StatelessWorkerJob,
    telemetry: Option<(Arc<str>, Arc<str>)>,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    drive_affiliated_with_budget(affiliation, job, telemetry, js::handler_budget())
}

fn drive_affiliated_with_budget(
    affiliation: crate::pool::Affiliation,
    job: StatelessWorkerJob,
    telemetry: Option<(Arc<str>, Arc<str>)>,
    budget: Duration,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    let StatelessWorkerJob { job, cancellation } = job;
    let driving = drive_affiliated_inner(affiliation, job, telemetry, budget);
    drive_with_request_cancellation(driving, cancellation)
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn drive_affiliated_with_budget_for_test(
    affiliation: crate::pool::Affiliation,
    job: crate::WorkerJob,
    telemetry: Option<(Arc<str>, Arc<str>)>,
    budget: Duration,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    drive_affiliated_with_budget(
        affiliation,
        StatelessWorkerJob::driver_owned(job),
        telemetry,
        budget,
    )
}

async fn drive_affiliated_inner(
    affiliation: crate::pool::Affiliation,
    job: crate::WorkerJob,
    telemetry: Option<(Arc<str>, Arc<str>)>,
    budget: Duration,
) {
    let slot = affiliation.slot().clone();
    // One sampling decision per request, shared by the SERVER span and
    // the turn context the handler's console/fetch children inherit. A
    // caller's traceparent is honored per the spec: ids adopted either
    // way, its sampled flag deciding only under a parentbased sampler.
    let remote = telemetry.as_ref().and_then(|_| inbound_parent(&job));
    let trace = telemetry
        .as_ref()
        .and_then(|_| crate::telemetry::start_trace_with_parent(remote.as_ref()));
    let recording = trace.and_then(crate::telemetry::TraceContext::recording_ids);
    let mut timing = telemetry.map(|(node, region)| {
        StatelessTiming::start(&job, slot.id, node, region, recording, remote)
    });
    // Admission created `affiliation` before returning the isolate. It stays
    // here for the request's whole life, so maintenance cannot free the heap
    // between placement and this first turn or while a promise is suspended.
    let _affiliation = affiliation;
    drive_worker(
        &slot,
        job,
        trace,
        budget,
        |entry| {
            if let Some(timing) = &mut timing {
                timing.answered(entry);
            }
        },
        |_| {},
    )
    .await;
}

// Both worker placements use the same execution loop. Keep observation at
// each turn separate from completion: response timing excludes waitUntil,
// while the worker-on-cell span includes it. Neither observer owns cleanup.
async fn drive_worker(
    slot: &crate::pool::Slot,
    job: crate::WorkerJob,
    trace: Option<crate::telemetry::TraceContext>,
    budget: Duration,
    mut after_turn: impl FnMut(&js::InFlight),
    on_finish: impl FnOnce(&js::InFlight),
) {
    let mut ops = Ops::new();

    let (begun, started) = slot.turn(|worker| worker.turn_begin(job, trace)).await;
    // Nothing is in flight; the reply already carries the error.
    let Some(mut entry) = begun else {
        drop(started);
        return;
    };
    if entry.keeps_native_ops() {
        adopt(&mut ops, started);
    } else {
        drop(started);
        abort_ops(&mut ops, &mut entry);
    }
    after_turn(&entry);

    while !entry.finished() {
        let started = match wake_with_cross_entry_gate(&mut ops, &mut entry, budget).await {
            Wake::Op(op, result) => {
                slot.turn(|worker| worker.turn_deliver(&mut entry, op, result))
                    .await
            }
            Wake::GatedReply(completion) => {
                entry.finish_gated_reply(completion);
                Vec::new()
            }
            Wake::CancelGatedReply => {
                entry.cancel_gated_reply();
                Vec::new()
            }
            Wake::CrossEntryGateChanged => {
                entry.finish_cross_entry_gates();
                Vec::new()
            }
            Wake::Cancelled { shutdown } => {
                let started = slot
                    .turn(|worker| {
                        if shutdown {
                            worker.turn_cancel_for_shutdown(&mut entry)
                        } else {
                            worker.turn_cancel(&mut entry)
                        }
                    })
                    .await;
                entry.cancel_gated_reply();
                started
            }
            Wake::Expired => {
                entry.time_out(budget);
                Vec::new()
            }
            Wake::Idle => {
                entry.stuck();
                Vec::new()
            }
            Wake::PendingEventIdle => {
                slot.turn(|worker| worker.turn_cancel_pending_events(&mut entry))
                    .await
            }
            Wake::Poll => slot.turn(|worker| worker.turn_poll(&mut entry)).await,
        };
        if entry.keeps_native_ops() {
            adopt(&mut ops, started);
        } else {
            drop(started);
            abort_ops(&mut ops, &mut entry);
        }
        after_turn(&entry);
    }

    on_finish(&entry);
    // Dropping `ops` aborts whatever is still pending, which is what a region
    // does on every exit path; their resolvers have to go with them.
    entry.finish_tail_report();
    entry.abandon();
}

/// What next moves a suspended request.
enum Wake {
    /// One of its own ops finished.
    Op(u64, Result<asyncrt::OpOut, String>),
    /// The detached output-gate waiter sent, or abandoned, the final reply.
    GatedReply(Result<js::GatedReplyCompletion, tokio::sync::oneshot::error::RecvError>),
    /// The handler has answered, so cancel its detached reply gate without
    /// entering JavaScript again.
    CancelGatedReply,
    /// A cross-entry claim changed, or subscribing closed a retirement gap.
    CrossEntryGateChanged,
    /// Its client hung up, or shutdown forced the complete event to retire.
    Cancelled { shutdown: bool },
    /// It ran past the handler budget without answering.
    Expired,
    /// Nothing outstanding could ever move it.
    Idle,
    /// A nested Worker entrypoint is pending without native work. Reject that
    /// call inside JavaScript so the enclosing event can catch the failure.
    PendingEventIdle,
    /// Nothing of its own is outstanding, but another event of the same cell
    /// still could settle it. Look in and see.
    Poll,
}

fn take_cancellation_wake(request_id: Option<js::RequestId>) -> Option<Wake> {
    js::take_request_cancellation(request_id).then(|| Wake::Cancelled {
        shutdown: js::take_shutdown_cancellation(request_id),
    })
}

/// Wait for whichever comes first, holding no isolate.
///
/// This is the whole of what a request does between turns, and it is
/// deliberately the only place that waits: everything else in `drive` either
/// holds the isolate or is arithmetic.
async fn wake_with_cross_entry_gate(
    ops: &mut Ops,
    entry: &mut js::InFlight,
    budget: Duration,
) -> Wake {
    let wait = entry.prepare_cross_entry_gate_wait();
    match wait
        .wait(wake(ops, entry, budget), |wake| matches!(wake, Wake::Idle))
        .await
    {
        js::input_gate_lifecycle::WaitOutcome::StateChanged => Wake::CrossEntryGateChanged,
        js::input_gate_lifecycle::WaitOutcome::Ordinary(wake) => wake,
    }
}

async fn wake(ops: &mut Ops, entry: &mut js::InFlight, budget: Duration) -> Wake {
    loop {
        // An op this event enqueued from inside another event's turn reaches
        // this driver here rather than through `adopt`, because the turn that
        // took it belongs to another entry. See `js::adopt`.
        adopt(ops, entry.take_handed_ops());
        // A WorkerEntrypoint call is its own PendingEvent. When no referenced
        // native operation can move it, reject that call inside JavaScript.
        // An unreferenced signal listener cannot make progress and must not
        // hide a hung call. This check precedes the answered branch because
        // the enclosing handler can keep the call alive through waitUntil
        // after its response.
        if !entry.has_refed_ops() && entry.has_pending_events() {
            return Wake::PendingEventIdle;
        }
        let Some(left) = entry.remaining(budget) else {
            // The handler settled, so neither its reply gate nor waitUntil
            // work is charged to the handler budget. Poll both because the
            // background can progress while the gate owns the reply.
            let request_id = entry.request_id();
            let (gated_reply, context) = entry.gated_reply_and_io_context();
            if let Some(gated_reply) = gated_reply {
                let Some(request_id) = request_id else {
                    let next = if ops.is_empty() {
                        asyncrt::select_biased! {
                            "a completed gated reply wins a tie with an op another turn hands over";
                            completion = gated_reply => Some(Wake::GatedReply(completion)),
                            _ = context.handed_ready() => None,
                        }
                    } else {
                        asyncrt::select_biased! {
                            "a completed gated reply wins a tie with another operation wake";
                            completion = gated_reply => Some(Wake::GatedReply(completion)),
                            result = ops.next() => result.map(|(op, result)| Wake::Op(op, result)),
                            _ = context.handed_ready() => None,
                        }
                    };
                    match next {
                        Some(wake) => return wake,
                        // A hand-off, or an exhausted stream after its final
                        // completion; take the mailbox before waiting again.
                        None => continue,
                    }
                };
                // The domain select is declaration-order biased. A reply that
                // completed wins over an op, a mailbox hand-off wins over a
                // cancellation tick, and cancellation is sampled immediately
                // after a mailbox or timer wake so continuous hand-offs cannot
                // starve it.
                let next = if ops.is_empty() {
                    asyncrt::select_biased! {
                        "a completed gated reply wins a tie with another request wake";
                        completion = gated_reply => Some(Wake::GatedReply(completion)),
                        _ = context.handed_ready() => None,
                        _ = asyncrt::sleep(CANCELLATION_TICK) => None,
                    }
                } else {
                    asyncrt::select_biased! {
                        "a completed gated reply wins a tie with another request wake";
                        completion = gated_reply => Some(Wake::GatedReply(completion)),
                        next = async {
                            asyncrt::select_biased! {
                                "an operation result wins a tie with another request wake";
                                result = ops.next() => Some(match result {
                                    Some((op, result)) => Wake::Op(op, result),
                                    None => Wake::Idle,
                                }),
                                _ = context.handed_ready() => None,
                                _ = asyncrt::sleep(CANCELLATION_TICK) => None,
                            }
                        } => next,
                    }
                };
                if let Some(next) = next {
                    return next;
                }
                if let Some(cancelled) = take_cancellation_wake(Some(request_id)) {
                    return if matches!(cancelled, Wake::Cancelled { shutdown: true }) {
                        cancelled
                    } else {
                        Wake::CancelGatedReply
                    };
                }
                continue;
            }
            // The reply arrived, so only waitUntil work remains. A client
            // disconnect no longer matters, but a lifecycle cancellation
            // must still retire the background work before the runtime stops.
            let Some(request_id) = request_id else {
                let context = entry.io_context();
                let next = asyncrt::select_biased! {
                    "an operation result wins a tie with an op another turn hands over";
                    result = ops.next() => Some(result),
                    _ = context.handed_ready() => None,
                };
                match next {
                    Some(Some((op, result))) => return Wake::Op(op, result),
                    Some(None) => return Wake::Idle,
                    // Handed over; the next pass takes it.
                    None => continue,
                }
            };
            let next = if ops.is_empty() {
                asyncrt::sleep(CANCELLATION_TICK).await;
                None
            } else {
                asyncrt::select_biased! {
                    "completed waitUntil work wins a tie with periodic lifecycle cancellation sampling";
                    result = ops.next() => Some(match result {
                        Some((op, result)) => Wake::Op(op, result),
                        None => Wake::Idle,
                    }),
                    _ = asyncrt::sleep(CANCELLATION_TICK) => None,
                }
            };
            if let Some(next) = next {
                return next;
            }
            if let Some(cancelled) = take_cancellation_wake(Some(request_id)) {
                if matches!(cancelled, Wake::Cancelled { shutdown: true }) {
                    return cancelled;
                }
            }
            continue;
        };
        // A disconnect is raised on another thread with nothing to wake this
        // one, so the wait is capped and the flag re-read — as the blocking
        // run loop capped its own. The difference is that reading it costs no
        // isolate, so a request enters V8 only once the client has really gone.
        // An unrefed op can wake a handler that awaits it, but it cannot be
        // the reason this driver stops polling the isolate. A retained signal
        // listener otherwise hides the empty-op path that lets a loopback
        // entry make progress, and both events wait on each other forever.
        let only_unrefed = !ops.is_empty() && !entry.has_refed_ops();
        let capped = if entry.cancellable() || only_unrefed {
            left.min(CANCELLATION_TICK)
        } else {
            left
        };
        // Nothing of this entry's own is outstanding, so there is no future
        // to wait on — only the chance that some *other* entry in this
        // isolate has settled it since the last look. That is an ordinary
        // thing rather than a stall: a cell awaits the alarm it armed, and a
        // Worker awaits a Durable Object it dispatched to. Both used to
        // resolve inside the caller's own run loop, so there was nothing to
        // wait for; both are separate entries now.
        //
        // So "waiting on nothing" is a verdict the budget reaches, not one
        // an empty op set proves.
        let context = entry.io_context();
        if ops.is_empty() {
            if left.is_zero() {
                return Wake::Idle;
            }
            // The handed wake belongs on this path too. This entry holds no op
            // of its own, so without it a handed op waits out the whole
            // cancellation tick before the next pass can take it — the delay
            // this branch has no reason to add.
            asyncrt::select_biased! {
                "an op another turn hands over wins a tie with the cancellation tick";
                _ = context.handed_ready() => {},
                _ = tokio::time::sleep(capped.min(CANCELLATION_TICK)) => {},
            }
            // Re-read the flag on this path too. A request with nothing
            // outstanding can still have its client hang up, and only the
            // branch below used to look.
            if let Some(cancelled) = take_cancellation_wake(entry.request_id()) {
                return cancelled;
            }
            return Wake::Poll;
        }
        let next = tokio::time::timeout(capped, async {
            asyncrt::select_biased! {
                "an operation result wins a tie with an op another turn hands over";
                result = ops.next() => Some(result),
                _ = context.handed_ready() => None,
            }
        })
        .await;
        match next {
            Ok(Some(Some((op, result)))) => return Wake::Op(op, result),
            Ok(Some(None)) => return Wake::Idle,
            // Another turn handed this event an op. The next pass takes it.
            Ok(None) => continue,
            Err(_) => {
                if let Some(cancelled) = take_cancellation_wake(entry.request_id()) {
                    return cancelled;
                }
                if capped == left {
                    return Wake::Expired;
                }
                if only_unrefed {
                    return Wake::Poll;
                }
                continue;
            }
        }
    }
}

/// One stateless request's canonical timing event.
///
/// The phases still mean what they always did, but what they measure has
/// moved: `queue_wait_us` was the wait for a free worker thread and is now
/// the wait for admission and the isolate's async gate, and `execution_us`
/// spans every turn the request took rather than one uninterrupted run.
struct StatelessTiming {
    queued_at: Instant,
    request_id: Option<js::RequestId>,
    node: Arc<str>,
    region: Arc<str>,
    isolate: usize,
    admitted: Instant,
    emitted: bool,
    /// Sampled at creation: `None` is off or unsampled, and nothing more
    /// is ever built for this request.
    trace: Option<crate::telemetry::TraceIds>,
    /// The caller's span, when the request arrived with a traceparent.
    remote_parent: Option<crate::telemetry::ParentContext>,
    /// Why the handler failed, read from the entry when it was answered.
    failure: Option<String>,
    span_name: &'static str,
    span_kind: u8,
}

impl StatelessTiming {
    fn start(
        job: &crate::WorkerJob,
        isolate: usize,
        node: Arc<str>,
        region: Arc<str>,
        trace: Option<crate::telemetry::TraceIds>,
        remote_parent: Option<crate::telemetry::ParentContext>,
    ) -> Self {
        let (queued_at, request_id, span_name, span_kind) = match job {
            crate::WorkerJob::Fetch {
                queued_at,
                request_id,
                ..
            } => (
                *queued_at,
                *request_id,
                "celld.fetch",
                crate::telemetry::KIND_SERVER,
            ),
            crate::WorkerJob::Queue { queued_at, .. } => (
                *queued_at,
                None,
                "celld.queue",
                crate::telemetry::KIND_CONSUMER,
            ),
            crate::WorkerJob::Rpc { .. } => (
                Instant::now(),
                None,
                "celld.fetch",
                crate::telemetry::KIND_SERVER,
            ),
        };
        StatelessTiming {
            queued_at,
            request_id,
            node,
            region,
            isolate,
            admitted: Instant::now(),
            emitted: false,
            trace,
            remote_parent,
            failure: None,
            span_name,
            span_kind,
        }
    }

    /// Emit once, the first time the client has been answered. `waitUntil`
    /// work continues afterwards and is not part of the response's timing.
    fn answered(&mut self, entry: &js::InFlight) {
        if self.emitted || !entry.answered() {
            return;
        }
        self.emitted = true;
        self.failure = entry.failure().map(str::to_string);
        self.emit();
    }

    fn emit(&self) {
        if let Some(ids) = self.trace {
            let total_us = self.queued_at.elapsed().as_micros() as i64;
            let mut span = crate::telemetry::Span::new(ids, self.span_name, self.span_kind);
            span.start_unix_us = crate::telemetry::now_unix_us() - total_us;
            span.duration_us = total_us;
            span.ok = self.failure.is_none();
            span.error = self.failure.clone();
            span.request_id = self.request_id.map(js::request_id_string);
            span.isolate = Some(self.isolate as u64);
            span.parent_span_id = self.remote_parent.map(|parent| parent.span_id);
            span.parent_remote = self.remote_parent.map(|_| true);
            span.queue_wait_us =
                Some(self.admitted.duration_since(self.queued_at).as_micros() as i64);
            crate::telemetry::record(span);
        }
        // An info!-per-request costs real throughput on the hot path, so the
        // `enabled!` guard skips the elapsed math and the formatting when the
        // target is off. The lab turns it on with RUST_LOG=info,timing=debug.
        let Some(request_id) = self
            .request_id
            .filter(|_| tracing::enabled!(target: "timing", tracing::Level::DEBUG))
        else {
            return;
        };
        tracing::debug!(
            target: "timing",
            event = "worker_fetch_timing",
            outcome = "completed",
            request_id = %js::request_id_string(request_id),
            node = %self.node,
            region = %self.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            total_us = self.queued_at.elapsed().as_micros() as u64,
            queue_wait_us = self.admitted.duration_since(self.queued_at).as_micros() as u64,
            execution_us = self.admitted.elapsed().as_micros() as u64,
            isolate = self.isolate,
            "stateless Worker fetch completed"
        );
    }
}

/// Initialise V8, at most once.
///
/// **Must run before any thread that will enter an isolate is created.**
/// V8 protects its pointer tables with a memory protection key, and the
/// `PKRU` register granting access to that key is per-thread and inherited
/// at thread creation. A thread created before this runs never receives
/// access, so its first read of a dispatch table traps with `SEGV_PKUERR`
/// on any CPU that supports protection keys.
pub fn init_v8() {
    js::Engine::init();
}

impl StatelessRuntime {
    #[doc(hidden)]
    pub fn start(
        config: Arc<WorkerConfig>,
        node: Arc<str>,
        region: Arc<str>,
    ) -> anyhow::Result<Self> {
        let build = {
            let config = config.clone();
            move || Worker::load_config(config.clone())
        };
        let isolates = Arc::new(crate::pool::Pool::new(
            pool_limits(),
            admission_wait(),
            Box::new(build),
        ));
        // Eagerly, so a script that does not load fails here rather than on
        // every request, and so the first request does not pay for compiling
        // it. Growth past this one stays lazy.
        isolates.warm().context("stateless Worker failed to load")?;
        // Give isolates back when the burst that grew them is over. Without
        // this the pool only grows, and every heap a burst created is held
        // for the life of the process. Long relative to a request, because
        // retiring is not urgent and a short period would thrash a pool that
        // is about to be busy again.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Weak, so this loop cannot keep a superseded generation's pool
            // alive: it ends when the pool is dropped.
            let reaping = Arc::downgrade(&isolates);
            handle.spawn(async move {
                // `interval` fires its first tick immediately, which
                // reaped the isolate `warm` had just built and handed the
                // first request the compile cost warming exists to avoid.
                let mut tick = tokio::time::interval_at(
                    tokio::time::Instant::now() + REAP_INTERVAL,
                    REAP_INTERVAL,
                );
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let Some(pool) = reaping.upgrade() else {
                        return;
                    };
                    pool.reap();
                }
            });
        }
        Ok(Self {
            isolates,
            node,
            region,
        })
    }

    /// Admit and drive one stateless event. A verb supplies only its job and
    /// result type, so admission, shedding, pressure, and task failure cannot
    /// drift between fetch, RPC, and queue dispatch.
    async fn dispatch<T: Send + 'static>(
        &self,
        verb: StatelessVerb,
        make_job: impl FnOnce(tokio::sync::oneshot::Sender<anyhow::Result<T>>) -> crate::WorkerJob,
    ) -> anyhow::Result<T> {
        let shedding = crate::ownership_store::node_is_shedding();
        self.dispatch_stateless(verb, shedding, move |reply| {
            StatelessWorkerJob::driver_owned(make_job(reply))
        })
        .await
    }

    #[doc(hidden)]
    pub async fn dispatch_with_shedding<T: Send + 'static>(
        &self,
        verb: StatelessVerb,
        shedding: bool,
        make_job: impl FnOnce(tokio::sync::oneshot::Sender<anyhow::Result<T>>) -> crate::WorkerJob,
    ) -> anyhow::Result<T> {
        self.dispatch_stateless(verb, shedding, move |reply| {
            StatelessWorkerJob::driver_owned(make_job(reply))
        })
        .await
    }

    async fn dispatch_stateless<T: Send + 'static>(
        &self,
        verb: StatelessVerb,
        shedding: bool,
        make_job: impl FnOnce(tokio::sync::oneshot::Sender<anyhow::Result<T>>) -> StatelessWorkerJob,
    ) -> anyhow::Result<T> {
        let affiliation = self.isolates.admit_or_wait(shedding).await?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = make_job(reply);
        // Spawned rather than awaited inline, because the response and the
        // event are not the same lifetime: `waitUntil` work outlives the
        // answer, and the driver keeps turning until it settles.
        let driving = tokio::spawn(drive_affiliated(
            affiliation,
            job,
            Some((self.node.clone(), self.region.clone())),
        ));
        match receive.await {
            Ok(result) => result,
            // The driver dropped the reply without sending. Joining it here —
            // and only here — turns a bare "channel closed" into the panic
            // that actually caused it, at no cost on the path that works.
            Err(_) => match driving.await {
                Err(error) => Err(verb.task_died(error)),
                Ok(()) => Err(verb.dropped_result()),
            },
        }
    }

    /// Serve one stateless request, entering an isolate once per turn.
    ///
    /// The request drives itself: it is admitted and placed, runs its first
    /// turn, then awaits its *own* ops with no isolate held, re-entering for
    /// each completion. Nothing multiplexes and nothing demultiplexes, which
    /// is what deleting the pump buys.
    #[doc(hidden)]
    pub fn fetch(
        &self,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        cancellation: Option<Arc<RequestCancellationLifetime>>,
    ) -> impl std::future::Future<Output = anyhow::Result<HttpResponse>> + Send + 'static {
        self.fetch_target(None, url, method, body, headers, cancellation)
    }

    /// Serve one default or named stateless Worker entrypoint.
    fn fetch_target(
        &self,
        entrypoint: Option<crate::WorkerFetchEntrypoint>,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        cancellation: Option<Arc<RequestCancellationLifetime>>,
    ) -> impl std::future::Future<Output = anyhow::Result<HttpResponse>> + Send + 'static {
        let runtime = self.clone();
        let make_job =
            stateless_fetch_job_factory(entrypoint, url, method, body, headers, cancellation);
        async move {
            let shedding = crate::ownership_store::node_is_shedding();
            runtime
                .dispatch_stateless(StatelessVerb::Fetch, shedding, make_job)
                .await
        }
    }

    /// An entrypoint RPC, on the isolate pool like fetch.
    ///
    /// It used to go to the worker threads, because the old RPC dispatcher
    /// blocked while the handler awaited — and a
    /// blocking call cannot hold a pool slot without parking a tokio worker
    /// on V8. Turning it into `begin`/`drive` removes the blocking, and with
    /// it the last reason `WorkerPool` existed.
    async fn rpc(
        &self,
        entrypoint: String,
        props: Vec<u8>,
        operation: crate::WorkerRpcOperation,
    ) -> anyhow::Result<Vec<u8>> {
        self.dispatch(StatelessVerb::Rpc, move |reply| crate::WorkerJob::Rpc {
            entrypoint,
            operation,
            props,
            invocation_limits: None,
            reply,
        })
        .await
    }

    /// Dispatch one leased queue batch through the stateless isolate pool.
    #[doc(hidden)]
    pub async fn queue(&self, batch: js::QueueBatch) -> anyhow::Result<js::QueueDispatchResult> {
        let queued_at = Instant::now();
        self.dispatch(StatelessVerb::Queue, move |reply| crate::WorkerJob::Queue {
            queued_at,
            batch,
            reply,
        })
        .await
    }
}

struct CellIsolateStartupTiming {
    started: Instant,
    scope: String,
    node: Arc<str>,
    region: Arc<str>,
    epoch: u64,
    fresh: bool,
}

impl CellIsolateStartupTiming {
    fn emit(&self, outcome: &str, failure_phase: &str) -> u64 {
        let total_us = self.started.elapsed().as_micros() as u64;
        if let Some(ids) =
            crate::telemetry::start_trace().and_then(crate::telemetry::TraceContext::recording_ids)
        {
            let mut span = crate::telemetry::Span::new(
                ids,
                "celld.cell_startup",
                crate::telemetry::KIND_INTERNAL,
            );
            span.start_unix_us = crate::telemetry::now_unix_us() - total_us as i64;
            span.duration_us = total_us as i64;
            span.ok = outcome == "ready";
            span.error = (!span.ok).then(|| failure_phase.to_string());
            span.cell = Some(self.scope.clone());
            span.epoch = Some(self.epoch);
            crate::telemetry::record(span);
        }
        tracing::info!(
            event = "cell_isolate_startup_timing",
            outcome,
            failure_phase,
            scope = %self.scope,
            node = %self.node,
            region = %self.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            epoch = self.epoch,
            fresh = self.fresh,
            total_us,
            "cell isolate startup completed"
        );
        total_us
    }
}

/// Build one isolate for a Worker script's cells.
///
/// No cells yet: `Worker::own_cell` opens each cell's storage as it is
/// placed here, because this isolate outlives any of them.
fn load_cell_isolate(config: Arc<WorkerConfig>) -> anyhow::Result<Worker> {
    #[cfg(debug_assertions)]
    if let Ok(barrier) = std::env::var("CELLD_TEST_CELL_STARTUP_BARRIER") {
        while !Path::new(&barrier).exists() {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    #[cfg(debug_assertions)]
    if std::env::var("CELLD_TEST_CELL_STARTUP_FAILURE").as_deref() == Ok("1") {
        return Err(anyhow!("injected cell isolate startup failure"));
    }
    Worker::load_config(config).map_err(|error| error.context("cell isolate load failed"))
}

/// Drive the Worker entry in a cell's isolate.
///
/// The stateless `drive` loop, with the isolate named rather than admitted:
/// this request must run *here*, because the cell it will route to lives here
/// and arriving in a warm isolate is the point.
async fn drive_worker_on_cell(affiliation: crate::pool::Affiliation, job: crate::WorkerJob) {
    // Held to the end of this function: the request's claim on the isolate
    // outlives every suspension, so the pool cannot free the worker under
    // a parked event (denoland/celld#147).
    let slot = affiliation.slot().clone();
    let remote = inbound_parent(&job);
    let trace = crate::telemetry::start_trace_with_parent(remote.as_ref());
    let recording = trace.and_then(crate::telemetry::TraceContext::recording_ids);
    let span_started = recording.map(|_| (Instant::now(), crate::telemetry::now_unix_us()));
    drive_worker(
        &slot,
        job,
        trace,
        js::handler_budget(),
        |_| {},
        |entry| {
            if let (Some(ids), Some((started, start_unix))) = (recording, span_started) {
                let mut span =
                    crate::telemetry::Span::new(ids, "celld.fetch", crate::telemetry::KIND_SERVER);
                span.start_unix_us = start_unix;
                span.duration_us = started.elapsed().as_micros() as i64;
                span.ok = entry.finished() && entry.failure().is_none();
                span.error = entry.failure().map(str::to_string);
                span.parent_span_id = remote.map(|parent| parent.span_id);
                span.parent_remote = remote.map(|_| true);
                crate::telemetry::record(span);
            }
        },
    )
    .await;
}

/// Report a turn's alarm moves to the host.
///
/// The host otherwise hears about a cell's alarm only when the request
/// finishes, because `ActivityGuard` reports it on drop. That is too late
/// for a handler that arms an alarm and then *awaits* it: the timer would
/// not be scheduled until the request ended, and the request cannot end
/// until the alarm fires. The blocking run loop hid this by polling
/// `get_alarm` between turns and firing a due alarm inline; with events as
/// entries, the host has to be told as soon as the arming turn returns.
fn report_alarm_moves(
    report: &Option<AlarmReporter>,
    moves: Vec<(String, celld_logic::wake::AlarmSnapshot)>,
) {
    if let Some(report) = report {
        for (scope, at_ms) in moves {
            report(scope, at_ms);
        }
    }
}

/// Drive one cell event to completion, one turn at a time.
///
/// The same loop as `drive`, and deliberately so: what makes a cell event
/// different is which realm its turns enter and that it waits for the input
/// gate first — not how it is pumped. Between turns it holds no isolate, so
/// a handler awaiting I/O stops neither its own cell's next event nor any
/// other cell sharing the isolate.
#[doc(hidden)]
pub async fn drive_cell(
    affiliation: crate::pool::Affiliation,
    job: CellJob,
    report: Option<AlarmReporter>,
    parent: Option<crate::telemetry::TraceContext>,
) {
    let cancellation = job
        .request_id()
        .map(RequestCancellationLifetime::from_request_id);
    #[cfg(celld_internal_tests)]
    let _ = drive_cell_inner(
        affiliation,
        job,
        report,
        parent,
        js::handler_budget(),
        cancellation,
        DriveCellTestObservers::default(),
    )
    .await;
    #[cfg(not(celld_internal_tests))]
    let _ = drive_cell_inner(
        affiliation,
        job,
        report,
        parent,
        js::handler_budget(),
        cancellation,
    )
    .await;
}

/// Let an answered alarm's leftover I/O finish on its own, and report how it
/// ended.
///
/// `fire_alarm` stops waiting for the drive as soon as the handler returns, so
/// nothing joins the task after that. Without this watcher a panic in the
/// leftover work, which the former `drive.await` turned into a loud process
/// panic, would end the task in silence.
fn watch_detached_alarm_drive(
    drive: crate::asyncrt::TaskHandle<anyhow::Result<Option<u64>>>,
    cell: &str,
    op: celld_logic::OpId,
) {
    let cell = cell.to_string();
    crate::asyncrt::spawn(async move {
        let failure = match drive.await {
            Ok(Ok(_)) => return,
            Ok(Err(error)) => format!("{error:#}"),
            Err(panic) => panic.to_string(),
        };
        tracing::warn!(
            event = "alarm_leftover_work_failed",
            scope = %cell,
            op,
            failure = %failure,
            "work an alarm handler left pending ended in a failure"
        );
    })
    .detach();
}

/// Drive an alarm event and answer the position its final turn reached.
///
/// A handler that failed between turns, or threw before it suspended, has
/// its retry record written by the final alarm turn, after its error left.
/// That commit is as unproven as the handler's own, so `fire_alarm` gates on
/// the position from here. `None` when the event settled its claim itself;
/// its error then carries the position.
pub(crate) async fn drive_alarm(
    affiliation: crate::pool::Affiliation,
    job: CellJob,
    report: Option<AlarmReporter>,
) -> anyhow::Result<Option<u64>> {
    let cancellation = job
        .request_id()
        .map(RequestCancellationLifetime::from_request_id);
    drive_cell_inner(
        affiliation,
        job,
        report,
        None,
        js::handler_budget(),
        cancellation,
        #[cfg(celld_internal_tests)]
        DriveCellTestObservers::default(),
    )
    .await
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub async fn drive_cell_with_budget_for_test(
    affiliation: crate::pool::Affiliation,
    job: CellJob,
    report: Option<AlarmReporter>,
    parent: Option<crate::telemetry::TraceContext>,
    budget: Duration,
) {
    let cancellation = job
        .request_id()
        .map(RequestCancellationLifetime::from_request_id);
    let _ = drive_cell_inner(
        affiliation,
        job,
        report,
        parent,
        budget,
        cancellation,
        DriveCellTestObservers::default(),
    )
    .await;
}

async fn drive_cell_with_request_cancellation(
    affiliation: crate::pool::Affiliation,
    job: CellJob,
    report: Option<AlarmReporter>,
    parent: Option<crate::telemetry::TraceContext>,
    cancellation: Option<Arc<RequestCancellationLifetime>>,
) {
    assert_eq!(
        job.request_id(),
        cancellation.as_ref().map(|lifetime| lifetime.request_id),
        "a cell fetch and its cancellation lifetime must have the same request id"
    );
    #[cfg(celld_internal_tests)]
    let _ = drive_cell_inner(
        affiliation,
        job,
        report,
        parent,
        js::handler_budget(),
        cancellation,
        DriveCellTestObservers::default(),
    )
    .await;
    #[cfg(not(celld_internal_tests))]
    let _ = drive_cell_inner(
        affiliation,
        job,
        report,
        parent,
        js::handler_budget(),
        cancellation,
    )
    .await;
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub async fn drive_cell_observing_gated_failure_for_test(
    affiliation: crate::pool::Affiliation,
    job: CellJob,
    gated_failure: tokio::sync::oneshot::Sender<bool>,
) {
    let cancellation = job
        .request_id()
        .map(RequestCancellationLifetime::from_request_id);
    let _ = drive_cell_inner(
        affiliation,
        job,
        None,
        None,
        js::handler_budget(),
        cancellation,
        DriveCellTestObservers {
            gated_failure: Some(gated_failure),
            ..DriveCellTestObservers::default()
        },
    )
    .await;
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub async fn drive_cell_observing_gated_op_drop_for_test(
    affiliation: crate::pool::Affiliation,
    job: CellJob,
    gated_failure: tokio::sync::oneshot::Sender<bool>,
    native_op_dropped: tokio::sync::oneshot::Sender<()>,
) {
    let cancellation = job
        .request_id()
        .map(RequestCancellationLifetime::from_request_id);
    let _ = drive_cell_inner(
        affiliation,
        job,
        None,
        None,
        js::handler_budget(),
        cancellation,
        DriveCellTestObservers {
            gated_failure: Some(gated_failure),
            native_op_dropped: Some(native_op_dropped),
        },
    )
    .await;
}

#[cfg(celld_internal_tests)]
#[derive(Default)]
struct DriveCellTestObservers {
    gated_failure: Option<tokio::sync::oneshot::Sender<bool>>,
    native_op_dropped: Option<tokio::sync::oneshot::Sender<()>>,
}

#[cfg(celld_internal_tests)]
struct NativeOpDropProbe(Option<tokio::sync::oneshot::Sender<()>>);

#[cfg(celld_internal_tests)]
impl Drop for NativeOpDropProbe {
    fn drop(&mut self) {
        if let Some(dropped) = self.0.take() {
            let _ = dropped.send(());
        }
    }
}

#[cfg(celld_internal_tests)]
fn adopt_cell_ops_for_test(
    ops: &mut Ops,
    started: Vec<js::Op>,
    observer: &mut Option<tokio::sync::oneshot::Sender<()>>,
) {
    for (id, future) in started {
        let probe = observer
            .take()
            .map(|observer| NativeOpDropProbe(Some(observer)));
        ops.push(Box::pin(async move {
            let _probe = probe;
            (id, future.await)
        }));
    }
}

#[cfg(celld_internal_tests)]
fn notify_gated_failure_for_test(
    entry: &mut js::InFlight,
    observer: &mut Option<tokio::sync::oneshot::Sender<bool>>,
) {
    let gated = entry.gated_reply_and_io_context().0.is_some();
    if let Some(observer) = observer.take() {
        let _ = observer.send(gated);
    }
}

async fn drive_cell_inner(
    affiliation: crate::pool::Affiliation,
    mut job: CellJob,
    report: Option<AlarmReporter>,
    parent: Option<crate::telemetry::TraceContext>,
    budget: Duration,
    request_cancellation: Option<Arc<RequestCancellationLifetime>>,
    #[cfg(celld_internal_tests)] mut test_observers: DriveCellTestObservers,
) -> anyhow::Result<Option<u64>> {
    let _request_cancellation = request_cancellation.map(RequestCancellationGuard::shared);
    // Held to the end of this function: the event's claim on the isolate
    // outlives every suspension, so the pool cannot free the worker under
    // a parked event (denoland/celld#147).
    let slot = affiliation.slot().clone();
    let scope = job.scope().to_string();
    let _queue_producer = if job.is_queue_producer() {
        match slot.queue_producer(&scope) {
            Some(permit) => Some(permit),
            None => {
                job.fail(anyhow!(js::CellOverloaded));
                return Ok(None);
            }
        }
    } else {
        None
    };
    // Two calls a caller made back-to-back reach the cell in that order.
    // This is the only place that can hold it: everything upstream is a
    // race, and everything downstream has already been delivered. Held
    // until the event *begins*, not until it finishes — a handler that
    // waits must not stop the next call arriving, or cell events would
    // stop interleaving and the DO contract with them.
    let mut order = job.take_order();
    if let Some(order) = order.as_mut() {
        order.wait().await;
    }
    // Join the dispatching Worker's trace when there is one — the root
    // already made the sampling decision — else decide a fresh root.
    // The seed is captured before the job moves, recorded once the entry
    // settles.
    let trace = match parent.as_ref() {
        Some(parent) => crate::telemetry::child_of(parent),
        None => crate::telemetry::start_trace(),
    };
    let recording = trace.and_then(crate::telemetry::TraceContext::recording_ids);
    let span_seed = recording.map(|_| {
        let name = match &job {
            CellJob::Fetch { .. } => "celld.cell_fetch",
            CellJob::Alarm { .. } => "celld.alarm",
            CellJob::Rpc { .. } => "celld.rpc",
            CellJob::WsOpen { .. } => "celld.ws_open",
            CellJob::WsMessage { .. } => "celld.ws_message",
            CellJob::WsClosed { .. } => "celld.ws_close",
            #[cfg(celld_internal_tests)]
            CellJob::SyncErrorForTest { .. } => "celld.sync_error_test",
        };
        (
            name,
            job.scope().to_string(),
            Instant::now(),
            crate::telemetry::now_unix_us(),
        )
    });
    let mut ops = Ops::new();
    // `blockConcurrencyWhile` shuts the cell's gate, and a shut gate means no
    // event reaches that cell until it opens. The blocking loop left a
    // refused job on the channel; there is no channel now, so the event waits
    // here.
    //
    // Asked *inside* the turn, which is the whole of it. A handler shuts the
    // gate while holding the isolate, so a check made before taking the
    // isolate can pass and then queue behind the very block it should have
    // waited for. On an idle machine the blocking event always won that race
    // and the bug was invisible; under load it is not.
    //
    // `cell_gate_wait` checks and enqueues under the gate's own lock, so the
    // ticket cannot be missed by a release landing between the two. Only the
    // waiting happens out here, because a turn may not await.
    let mut pending = Some(job);
    let (begun, started, moves) = loop {
        let mut waiting = None;
        let taken = slot
            .turn_cell(&scope, |worker| {
                let job = pending.take().expect("one job per attempt");
                if let Some(open) = js::cell_gate_wait(job.scope()) {
                    waiting = Some(open);
                    pending = Some(job);
                    return None;
                }
                let (begun, started) = worker.turn_begin_cell(job, trace);
                Some((begun, started, worker.take_alarm_moves()))
            })
            .await;
        match taken {
            Some(taken) => break taken,
            None => match waiting {
                None => {}
                Some(open) => match open.await {
                    // The gate opened normally; try for the isolate again.
                    Ok(Ok(())) => {}
                    // The critical section this event queued behind failed,
                    // which reset the cell. Delivering now would run against
                    // state that no longer exists, so refuse instead and say
                    // why the caller is being refused.
                    Ok(Err(failure)) => {
                        if let Some(job) = pending.take() {
                            job.fail(anyhow!(failure));
                        }
                        return Ok(None);
                    }
                    // The cell stopped while this event waited.
                    Err(_) => return Ok(None),
                },
            },
        }
    };
    // Delivered. Whatever the caller sent next may go.
    if let Some(order) = order.as_mut() {
        order.delivered();
    }
    drop(order);
    report_alarm_moves(&report, moves);
    // Nothing is in flight; the reply already carries the error.
    let Some(mut entry) = begun else {
        drop(started);
        return Ok(None);
    };
    if entry.keeps_native_ops() {
        #[cfg(celld_internal_tests)]
        adopt_cell_ops_for_test(&mut ops, started, &mut test_observers.native_op_dropped);
        #[cfg(not(celld_internal_tests))]
        adopt(&mut ops, started);
    } else {
        #[cfg(celld_internal_tests)]
        adopt_cell_ops_for_test(&mut ops, started, &mut test_observers.native_op_dropped);
        #[cfg(not(celld_internal_tests))]
        drop(started);
        abort_ops(&mut ops, &mut entry);
    }
    #[cfg(celld_internal_tests)]
    if entry.gated_reply_and_io_context().0.is_some() {
        notify_gated_failure_for_test(&mut entry, &mut test_observers.gated_failure);
    }

    while !entry.finished() {
        let (started, moves) = match wake_with_cross_entry_gate(&mut ops, &mut entry, budget).await
        {
            Wake::Op(op, result) => {
                slot.turn(|worker| {
                    let started = worker.turn_deliver(&mut entry, op, result);
                    (started, worker.take_alarm_moves())
                })
                .await
            }
            Wake::GatedReply(completion) => {
                entry.finish_gated_reply(completion);
                (Vec::new(), Vec::new())
            }
            Wake::CancelGatedReply => {
                entry.cancel_gated_reply();
                (Vec::new(), Vec::new())
            }
            Wake::CrossEntryGateChanged => {
                entry.finish_cross_entry_gates();
                (Vec::new(), Vec::new())
            }
            Wake::Cancelled { shutdown } => {
                let cancelled = slot
                    .turn(|worker| {
                        let started = if shutdown {
                            worker.turn_cancel_for_shutdown(&mut entry)
                        } else {
                            worker.turn_cancel(&mut entry)
                        };
                        (started, worker.take_alarm_moves())
                    })
                    .await;
                entry.cancel_gated_reply();
                #[cfg(celld_internal_tests)]
                notify_gated_failure_for_test(&mut entry, &mut test_observers.gated_failure);
                cancelled
            }
            Wake::Expired => {
                entry.time_out(budget);
                slot.turn(|worker| worker.turn_retire_input_gates(&entry))
                    .await;
                #[cfg(celld_internal_tests)]
                notify_gated_failure_for_test(&mut entry, &mut test_observers.gated_failure);
                (Vec::new(), Vec::new())
            }
            Wake::Idle => {
                entry.stuck();
                slot.turn(|worker| worker.turn_retire_input_gates(&entry))
                    .await;
                #[cfg(celld_internal_tests)]
                notify_gated_failure_for_test(&mut entry, &mut test_observers.gated_failure);
                (Vec::new(), Vec::new())
            }
            Wake::PendingEventIdle => {
                slot.turn(|worker| {
                    let started = worker.turn_cancel_pending_events(&mut entry);
                    (started, worker.take_alarm_moves())
                })
                .await
            }
            Wake::Poll => {
                slot.turn(|worker| {
                    let started = worker.turn_poll(&mut entry);
                    (started, worker.take_alarm_moves())
                })
                .await
            }
        };
        #[cfg(celld_internal_tests)]
        if entry.gated_reply_and_io_context().0.is_some() {
            notify_gated_failure_for_test(&mut entry, &mut test_observers.gated_failure);
        }
        if entry.keeps_native_ops() {
            #[cfg(celld_internal_tests)]
            adopt_cell_ops_for_test(&mut ops, started, &mut test_observers.native_op_dropped);
            #[cfg(not(celld_internal_tests))]
            adopt(&mut ops, started);
        } else {
            drop(started);
            abort_ops(&mut ops, &mut entry);
        }
        // A turn that ran JS may have armed an alarm this very request is
        // waiting on, so the host hears about it now rather than when the
        // request ends.
        report_alarm_moves(&report, moves);
    }

    // An alarm that ended without running JS again still owes its outcome —
    // `fail` deliberately leaves the claim, because it runs between turns
    // where cell storage is unreachable (denoland/celld#170) — and
    // recording it is storage only the isolate can reach.
    let mut alarm_write = Ok(None);
    if entry.owes_alarm() {
        let (moves, write) = slot
            .turn(|worker| {
                let write = worker.turn_finish_alarm(&mut entry);
                (worker.take_alarm_moves(), write)
            })
            .await;
        report_alarm_moves(&report, moves);
        alarm_write = write;
    }
    if let (Some(ids), Some((name, cell, started, start_unix))) = (recording, span_seed) {
        let kind = if name == "celld.cell_fetch" {
            crate::telemetry::KIND_SERVER
        } else {
            crate::telemetry::KIND_INTERNAL
        };
        let mut span = crate::telemetry::Span::new(ids, name, kind);
        span.start_unix_us = start_unix;
        span.duration_us = started.elapsed().as_micros() as i64;
        span.ok = entry.finished() && entry.failure().is_none();
        span.error = entry.failure().map(str::to_string);
        span.cell = Some(cell);
        span.parent_span_id = parent.map(|parent| parent.span_id);
        span.parent_remote = parent.map(|_| false);
        crate::telemetry::record(span);
    }
    slot.turn(|worker| worker.turn_abandon_input_gates(&entry))
        .await;
    entry.abandon();
    alarm_write
}

fn path_text(path: &Path) -> &str {
    path.to_str().expect("celld data path must be UTF-8")
}

/// Tell the container module which classes the current generation
/// attaches a container to, and warm their images off the request path.
/// A generation with no container class installs an empty list, so a
/// class dropped by a redeploy cannot start a container.
fn install_container_specs(generation: &Generation) {
    crate::container::install_specs(
        generation.containers.clone(),
        generation.fence_image.clone(),
    );
    if generation.containers.is_empty() {
        return;
    }
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(crate::container::prewarm());
    }
}
