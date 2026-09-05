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
use crate::generation::{DeploymentGraph, Generation, GenerationId, GenerationOptions};
use crate::js::{self, CellJob, CellStorage, HttpResponse, Worker, WorkerConfig};
use crate::ltx_repl::LtxRepl;
use crate::replication::{ActivationOptions, StorageCredentials};
use crate::wake::WakeFlusher;
use anyhow::{anyhow, Context};
use futures_util::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
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
const CLEAN_RELOAD_MARKER: &str = ".clean-reload.json";
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
        let job = stateless_fetch_job_factory(url, method, body, headers, Some(self))(reply);
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
                url,
                method,
                body,
                headers,
                request_id,
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
    url: String,
    method: String,
    body: js::RequestBody,
    headers: Vec<(String, String)>,
    cancellation: Option<Arc<RequestCancellationLifetime>>,
) -> impl FnOnce(tokio::sync::oneshot::Sender<anyhow::Result<HttpResponse>>) -> StatelessWorkerJob {
    // The factory crosses the admission await, so it owns retirement until it
    // transfers the same guard into the spawned driver's job.
    let cancellation = RequestCancellationGuard::new(cancellation);
    move |reply| StatelessWorkerJob::fetch(url, method, body, headers, cancellation, reply)
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

#[derive(Deserialize, Serialize)]
struct CleanReloadMarker<'a> {
    node: &'a str,
    generation: &'a str,
}

#[derive(Deserialize)]
struct OwnedCleanReloadMarker {
    node: String,
    generation: String,
}

/// Read the fixed-size certificate left by a clean local shutdown. The node
/// lease state machine still decides whether the named generation is live and
/// can be replaced; this local value grants no authority by itself.
pub fn take_clean_reload_generation(data_dir: &Path, node: &str) -> Option<String> {
    let path = data_dir.join(CLEAN_RELOAD_MARKER);
    let filesystem = asyncrt::fs();
    let bytes = filesystem.read(&path).ok()?;
    let _ = filesystem.remove_file(&path);
    let marker: OwnedCleanReloadMarker = serde_json::from_slice(&bytes).ok()?;
    (marker.node == node).then_some(marker.generation)
}

pub fn write_clean_reload_marker(
    data_dir: &Path,
    node: &str,
    generation: &str,
) -> anyhow::Result<()> {
    let filesystem = asyncrt::fs();
    filesystem.create_dir_all(data_dir)?;
    let marker = data_dir.join(CLEAN_RELOAD_MARKER);
    let temporary = data_dir.join(".clean-reload.tmp");
    let body = serde_json::to_vec(&CleanReloadMarker { node, generation })?;
    filesystem.write(&temporary, &body)?;
    filesystem.rename(&temporary, &marker)?;
    Ok(())
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

fn require_cell_scope_capacity(data_dir: &Path) -> anyhow::Result<()> {
    asyncrt::fs()
        .create_dir_all(data_dir)
        .with_context(|| format!("create data directory {}", data_dir.display()))?;
    let reported = asyncrt::filesystem_name_max(data_dir)
        .with_context(|| format!("read NAME_MAX for {}", data_dir.display()))?
        .with_context(|| {
            format!(
                "the filesystem does not report NAME_MAX for {}",
                data_dir.display()
            )
        })?;
    let name_max = usize::try_from(reported).context("NAME_MAX does not fit usize")?;
    require_cell_scope_name_max(name_max)
}

pub(crate) fn require_cell_scope_name_max(name_max: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        name_max >= celld_logic::cell::MAX_CELL_SCOPE,
        "the data filesystem supports {name_max}-byte names, but celld requires {}",
        celld_logic::cell::MAX_CELL_SCOPE
    );
    Ok(())
}

/// The isolate pool's limits, built from the environment here because the
/// decision core never reads it.
///
/// `max_requests` is the node's only bound on stateless memory, and it is
/// live: `Slot::affiliate` counts an affiliation for a request's whole life,
/// `observe` reports it, and `isolate::admit` refuses against it. Unset
/// means unbounded, not unwired — `engine/load-under-pressure.md` measures
/// `CELLD_MAX_REQUESTS=32` admitting 641 rps against a theoretical 640.
/// How long a stateless request may wait for a free `max_requests` slot
/// before it is refused. Zero restores the old refuse-at-once behaviour.
/// The default is one second: long enough that a saturated node converts
/// its refusal storm into kernel-buffer queueing, short enough that a
/// caller learns the truth before it matters.
pub fn admission_wait() -> std::time::Duration {
    // Not `env_usize`, which filters zero out — zero is meaningful here.
    let ms = crate::env_vars::with_default("CELLD_ADMISSION_WAIT_MS", 1000u64)
        .expect("validated CELLD_ADMISSION_WAIT_MS");
    std::time::Duration::from_millis(ms)
}

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
    /// The last alarm the reporter saw, `-1` for none: the cache behind
    /// `alarm()`'s point query. Written only by the effect path — a turn
    /// moves an alarm, its drive reports it — never by storage directly.
    next_alarm_ms: AtomicI64,
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

pub type AlarmObserver = Arc<dyn Fn(String, Option<i64>) + Send + Sync>;

/// How a turn's alarm move reaches the host. `drive_cell` calls it with
/// what `take_alarm_moves` drained; it caches the value on the cell's
/// handle and forwards a real change to the observer.
#[doc(hidden)]
pub type AlarmReporter = Arc<dyn Fn(String, i64) + Send + Sync>;

/// Re-arming the same time is not a change the host needs to hear twice —
/// the dedupe the old watcher's diffing provided, kept by the cache. A
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
    Arc::new(move |scope: String, at_ms: i64| {
        let registry = cells_.lock().expect("cell registry poisoned");
        let changed = registry
            .published
            .get(&scope)
            .or_else(|| registry.starting.get(&scope))
            .is_some_and(|handle| handle.next_alarm_ms.swap(at_ms, Ordering::AcqRel) != at_ms);
        if changed {
            observe_(scope, (at_ms >= 0).then_some(at_ms));
        }
    })
}

#[derive(Default)]
struct CellRegistry {
    starting: HashMap<String, CellHandle>,
    published: HashMap<String, CellHandle>,
}

/// The generations a node can resolve a call against: the one it serves and
/// the superseded ones whose isolates are still draining.
///
/// One value under one lock, because installing the new generation and
/// keeping the previous one resolvable are the same instant. Held apart,
/// the window between them resolved an in-flight call from the previous
/// generation against the new deployment graph.
struct Generations {
    current: Arc<Generation>,
    draining: Vec<Arc<Generation>>,
}

#[derive(Clone)]
pub struct RuntimeManager {
    /// The generations a call can resolve against. A reader takes one
    /// snapshot and never holds the lock across an await.
    generations: Arc<std::sync::RwLock<Generations>>,
    cells: Arc<Mutex<CellRegistry>>,
    alarm_reporter: AlarmReporter,
    /// A peer abort can arrive before the forwarded fetch. The tombstone and
    /// cell enqueue share this lock so neither ordering can lose cancellation.
    remote_requests: Arc<Mutex<RemoteRequestRegistry>>,
    /// Shell alarm tasks keyed by the core firing operation. A shutdown
    /// cancellation must target the firing it observed, not a later retry.
    alarm_requests: Arc<Mutex<AlarmRequestRegistry>>,
    data_dir: Arc<PathBuf>,
    replication: Option<Replication>,
    wake: Option<Arc<WakeFlusher>>,
    alarm_observer: AlarmObserver,
    node: Arc<str>,
    region: Arc<str>,
    max_cell_requests: usize,
}

/// The Actor's cell-runtime boundary. Ordinary builds contain only the V8 arm.
#[derive(Clone)]
pub(crate) enum CellHost {
    V8(RuntimeManager),
    #[cfg(all(test, celld_internal_tests))]
    Scripted(crate::conformance_sim_cell_host::SimCellHost),
}

#[derive(Clone, Copy)]
pub(crate) enum StopMode {
    /// Prove the final position, then optionally retain a local base.
    Evict { preserve_local: bool },
    /// Prove the final position and retain it for the successor epoch.
    Rebase,
    /// Close without a remote write because the caller lacks authority.
    CloseInPlace,
    /// Remove an image whose durability proof failed.
    Discard,
}

impl CellHost {
    pub(crate) fn local_reload_cells(&self) -> anyhow::Result<Vec<celld_logic::LocalCell>> {
        match self {
            Self::V8(runtime) => runtime.local_reload_cells(),
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.local_reload_cells(),
        }
    }

    pub(crate) async fn restore_cell(
        &self,
        cell: &str,
        spec: &celld_logic::RestoreSpec,
    ) -> anyhow::Result<celld_logic::RestoreOutcome> {
        match self {
            Self::V8(runtime) => runtime.restore_cell(cell, spec).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.restore_cell(cell, spec).await,
        }
    }

    /// Start a cell's runtime, reporting the isolate that took its realm and
    /// the application generation that isolate belongs to.
    pub(crate) async fn start_cell(
        &self,
        cell: String,
        epoch: u64,
        fresh: bool,
    ) -> anyhow::Result<(celld_logic::isolate::HeapId, GenerationId)> {
        match self {
            Self::V8(runtime) => runtime.start_cell(cell, epoch, fresh).await,
            // The scripted host maps its deterministic isolate pool to heap
            // identities on the generation a node boots with; no scripted
            // world adopts another.
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime
                .start_cell(cell, epoch, fresh)
                .await
                .map(|isolate| (isolate, crate::generation::FIRST_GENERATION)),
        }
    }

    /// Take a cell out of its isolate for a generation swap: close its
    /// application database and give the placement back, and touch nothing
    /// else. Ownership, the epoch, the replica, and the wake entry all stay.
    pub(crate) async fn swap_out_cell(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        match self {
            Self::V8(runtime) => runtime.swap_out_cell(cell, epoch).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.swap_out_cell(cell, epoch).await,
        }
    }

    pub(crate) fn publish_cell(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        match self {
            Self::V8(runtime) => runtime.publish_cell(cell, epoch),
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.publish_cell(cell, epoch),
        }
    }

    pub(crate) async fn ensure_durable(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        match self {
            Self::V8(runtime) => runtime.ensure_durable(cell, epoch).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.ensure_durable(cell, epoch).await,
        }
    }

    pub(crate) async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<(u64, celld_logic::ProofSource)> {
        match self {
            Self::V8(runtime) => runtime.await_durable(cell, epoch, position).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.await_durable(cell, epoch, position).await,
        }
    }

    pub(crate) async fn stop_cell(
        &self,
        cell: &str,
        epoch: u64,
        mode: StopMode,
    ) -> anyhow::Result<()> {
        match self {
            Self::V8(runtime) => runtime.stop_cell(cell, epoch, mode).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.stop_cell(cell, epoch, mode).await,
        }
    }

    pub(crate) fn abort_activity(&self, request_id: js::RequestId) {
        match self {
            Self::V8(_) => js::abort_request_for_shutdown(request_id),
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(_) => {}
        }
    }

    /// Read one V8 alarm-cache observation in reporter order. A scripted host
    /// owns its alarm outcome in the simulation, so the actor must not invent
    /// a second observation for it.
    pub(crate) fn alarm_observation(&self, cell: &str) -> Option<(Option<i64>, bool)> {
        match self {
            Self::V8(runtime) => {
                Some(runtime.with_alarm(cell, |at_ms| (at_ms, runtime.alarm_covered(cell, at_ms))))
            }
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(_) => None,
        }
    }

    /// Make an armed alarm discoverable before a graceful handoff continues.
    /// The runtime cache and the wake flusher share the reporter ordering, so
    /// re-reading after the awaited reconcile gives the core one observation
    /// that cannot overtake the bucket operation it describes.
    pub(crate) async fn refresh_handoff_alarm_coverage(
        &self,
        cell: &str,
        at_ms: i64,
    ) -> Option<(Option<i64>, bool)> {
        match self {
            Self::V8(_) => {
                js::reconcile_wake_entry(cell, at_ms, true).await;
                self.alarm_observation(cell)
            }
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(_) => None,
        }
    }

    pub(crate) async fn fire_alarm(
        &self,
        op: celld_logic::OpId,
        cell: String,
        scheduled_ms: i64,
    ) -> anyhow::Result<(Option<i64>, bool, Option<u64>)> {
        match self {
            Self::V8(runtime) => runtime.fire_alarm(op, cell, scheduled_ms).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.fire_alarm(op, cell, scheduled_ms).await,
        }
    }

    pub(crate) fn abort_alarm(&self, cell: &str, op: celld_logic::OpId) {
        match self {
            Self::V8(runtime) => runtime.abort_alarm(cell, op),
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(_) => {}
        }
    }
}

/// Node-level inputs to the cell runtime: everything that outlives an
/// application generation. What a deployment implies goes through
/// `Generation::build` instead.
pub struct RuntimeOptions {
    pub data_dir: PathBuf,
    pub replication: Option<Replication>,
    pub wake: Option<Arc<WakeFlusher>>,
    pub alarm_observer: AlarmObserver,
    pub node: String,
    pub region: String,
}

/// A service-binding fetch crossing from a calling isolate into the router.
pub struct ServiceFetch {
    /// The caller's application generation; the target is resolved in its
    /// deployment graph.
    pub generation: GenerationId,
    pub script: String,
    pub url: String,
    pub method: String,
    pub body: js::RequestBody,
    pub headers: Vec<(String, String)>,
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
}

/// Owned HTTP request crossing from the async shell into a V8 executor.
pub struct RuntimeFetch {
    pub url: String,
    pub method: String,
    pub body: js::RequestBody,
    pub headers: Vec<(String, String)>,
    pub request_id: Option<js::RequestId>,
    /// Where this call sits in its caller's order for this cell.
    pub order: Option<js::CallOrder>,
    /// The dispatching Worker's trace context, so the cell's span joins
    /// the caller's trace instead of rooting a disconnected one.
    pub parent: Option<crate::telemetry::TraceContext>,
}

/// The node's replication engine: the in-process `celld-ltx` replicator,
/// hidden behind this wrapper so nothing else touches the backend directly.
#[derive(Clone)]
pub struct Replication {
    ltx: Arc<LtxRepl>,
}

impl Replication {
    /// See `LtxRepl::set_paged_fleet`: the fleet sampler's answer to whether
    /// every live lease reads a paged epoch.
    pub fn set_paged_fleet(&self, ready: bool) -> bool {
        self.ltx.set_paged_fleet(ready)
    }

    /// Whether this node would page a large takeover now.
    pub fn paged_fleet(&self) -> bool {
        self.ltx.paged_fleet()
    }

    pub fn start(
        bucket: crate::bucket::Bucket,
        watch: &Path,
        endpoint: Option<String>,
        region: String,
        credentials: Option<StorageCredentials>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            ltx: Arc::new(LtxRepl::start(
                watch,
                bucket.backend(),
                bucket.name,
                bucket.prefix,
                endpoint,
                region,
                credentials,
            )?),
        })
    }

    /// The log tier installs its shipper and takeover interlock here.
    pub fn ltx(&self) -> Arc<LtxRepl> {
        self.ltx.clone()
    }

    async fn restore(
        &self,
        cell: &str,
        spec: &celld_logic::RestoreSpec,
    ) -> anyhow::Result<(PathBuf, bool, Option<String>)> {
        let options = ActivationOptions {
            cell,
            epoch: spec.epoch,
            fresh: spec.fresh,
            took_over: spec.took_over,
            resume_local: spec.resume_local,
            prior: spec.prior.clone(),
        };
        let activated = self.ltx.activate(options).await?;
        Ok((activated.path, activated.restored, activated.vfs))
    }

    pub fn process_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.ltx.process_status()
    }

    /// Enforce the byte ceiling on preserved eviction snapshots.
    ///
    /// The directory walk is synchronous, so callers must run this on a
    /// blocking executor rather than the runtime's serving thread.
    pub fn prune_local_cache(&self, max_bytes: u64) -> std::io::Result<(usize, usize, u64)> {
        self.ltx.prune_local_cache(max_bytes)
    }

    pub fn close_for_reload(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        self.ltx.close_for_reload(cell, epoch)
    }

    pub fn local_cells(&self) -> Vec<celld_logic::LocalCell> {
        self.ltx.local_cells()
    }

    pub fn prune_stale_live(&self, keep: &BTreeSet<(String, u64)>) -> anyhow::Result<usize> {
        self.ltx.prune_stale_live(keep)
    }

    /// Copy the exact published epoch into a private read-only snapshot.
    pub fn snapshot_active(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<crate::replication::RestoredSnapshot>> {
        self.ltx.snapshot_active(cell, epoch)
    }

    /// Restore the newest completed replica without claiming or activating it.
    pub async fn restore_snapshot(
        &self,
        cell: &str,
    ) -> anyhow::Result<Option<crate::replication::RestoredSnapshot>> {
        self.ltx.restore_snapshot(cell).await
    }

    async fn ensure_durable(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        self.ltx.handoff_wait(cell, epoch).await?;
        Ok(())
    }

    /// The output-gate durability wait: return the committed-write position the
    /// replica has proved durable, at least covering `position`, and which
    /// mechanism proved it (the fences differ; see `celld_logic::ProofSource`).
    /// The replicator batches concurrent writes to one cell behind a
    /// background sync and reports the real durable position.
    async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<(u64, celld_logic::ProofSource)> {
        self.ltx.await_durable(cell, epoch, position).await
    }

    async fn evict(&self, cell: &str, epoch: u64, preserve_local: bool) -> anyhow::Result<()> {
        self.ltx
            .evict(cell, epoch, preserve_local)
            .await
            .map(|_| ())
    }

    async fn release(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        self.ltx.release(cell, epoch).await
    }

    async fn close_in_place(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        self.ltx.close_in_place(cell, epoch).await
    }
}

impl Generation {
    /// Build a deployment into a generation that can serve.
    ///
    /// Every script in the graph is compiled and its primary pool warmed, so
    /// a bundle that does not load, or declares a Durable Object class it
    /// does not export, fails here — before the generation can take traffic
    /// — rather than on the first request. The Durable Object classes of
    /// every script form one flat registry, because `start_cell` resolves a
    /// cell by the class its scope names and nothing else.
    ///
    /// Boot and reload both come through here. There is no other function
    /// that turns a deployment into runtime state.
    pub fn build(
        id: GenerationId,
        graph: DeploymentGraph,
        options: GenerationOptions,
    ) -> anyhow::Result<Self> {
        let GenerationOptions {
            loader_binding,
            node,
            region,
        } = options;
        let DeploymentGraph { primary, cohosted } = graph;
        init_v8();

        // A Queue class is shared by every co-hosted script, but its broker
        // needs the consumer script rather than whichever script happened to
        // register `__Queue` first. Resolve one deployment-wide catalog before
        // constructing any WorkerConfig, then install the same catalog into
        // every isolate that could host a queue cell.
        let mut queue_catalog = BTreeMap::new();
        for (script, consumers) in
            std::iter::once((&primary.script_name, &primary.options.queue_consumers)).chain(
                cohosted
                    .iter()
                    .map(|target| (&target.script_name, &target.options.queue_consumers)),
            )
        {
            for consumer in consumers {
                let registration = js::QueueConsumerRegistration {
                    script: script.clone(),
                    config: consumer.clone(),
                };
                if let Some(previous) = queue_catalog.insert(consumer.queue.clone(), registration) {
                    return Err(anyhow!(
                        "queue {:?} is consumed by both {} and {}",
                        consumer.queue,
                        previous.script,
                        script
                    ));
                }
            }
        }
        let queue_catalog = queue_catalog.into_values().collect::<Vec<_>>();

        let node: Arc<str> = Arc::from(node);
        let region: Arc<str> = Arc::from(region);
        let crate::fleet::LoadedDeployment {
            options: worker,
            script_name: primary_script,
            version,
            prefix,
            asset_binding,
            assets: primary_assets,
            services,
            crons,
        } = primary;
        let mut assets = HashMap::new();
        if let Some(resolver) = primary_assets {
            assets.insert(primary_script.clone(), resolver);
        }
        let primary_classes = worker.do_classes.clone();
        // Only classes the user declared can be a bare-id default. Every
        // runtime-supplied class rides in `do_classes` so that its namespace
        // key is minted, and counting one here made adding any D1 binding flip
        // a one-class project past the `len == 1` condition — every
        // `/do/<bare-id>` request then failed with "requires exactly one
        // configured Durable Object class" for a config that still declared
        // exactly one.
        //
        // This asks `deploy::is_reserved_class` rather than naming one class,
        // because the first version of this filter named `__D1Database` alone
        // and `__Workflow` walked straight back into the same bug when it
        // shipped. A fourth reserved class must not be able to do it a third
        // time.
        let user_classes: Vec<&String> = worker
            .do_classes
            .iter()
            .filter(|class| !crate::deploy::is_reserved_class(class))
            .collect();
        let default_do_class =
            (user_classes.len() == 1).then(|| Arc::from(user_classes[0].as_str()));
        let config = Arc::new(
            WorkerConfig::new(worker)
                .with_services(services)
                .with_asset_binding(asset_binding)
                .with_loader(loader_binding)
                .with_queue_consumers(queue_catalog.clone())
                .with_crons(crons.clone())
                .with_generation(id),
        );
        let stateless = StatelessRuntime::start(config.clone(), node.clone(), region.clone())?;
        let mut service_pools = HashMap::from([(primary_script.clone(), stateless.clone())]);
        // Two scripts exporting one class name is genuinely ambiguous and is
        // refused — but a *reserved* class is not an export, and the two kinds
        // behave differently:
        //
        // `__D1Database` is deliberately shared. Its namespace is fleet-wide so
        // that several Workers can bind one database and a rename cannot rename
        // it, so two scripts declaring `d1_databases` address the same cells on
        // purpose. A D1 cell runs SQL and reads no user code, binding or var,
        // so whichever config serves it is immaterial and the first wins.
        // Refusing the second was bug F3: the node exited rather than start.
        //
        // A workflow class is script-scoped (`deploy::workflow_class`), so two
        // scripts produce two distinct names and never reach this branch. If
        // one ever did, it would be a real collision and must still be refused.
        let mut cell_configs: HashMap<String, Arc<WorkerConfig>> = HashMap::new();
        for class in primary_classes {
            register_cell_class(&mut cell_configs, class, config.clone(), &|class| {
                anyhow!("duplicate Durable Object class {class}")
            })?;
        }
        // The reserved cron cell is not a user class, so it is registered here
        // rather than arriving in the manifest's `do_classes`. It shares the
        // primary Worker's config because its alarm's only job is to call that
        // script's `scheduled` handler.
        if !config.crons.is_empty() {
            cell_configs.insert(
                celld_logic::cron::RESERVED_CLASS.to_string(),
                config.clone(),
            );
        }
        for target in cohosted {
            let crate::fleet::LoadedDeployment {
                options,
                script_name: script,
                asset_binding,
                assets: target_assets,
                services,
                ..
            } = target;
            if let Some(resolver) = target_assets {
                assets.insert(script.clone(), resolver);
            }
            let target_classes = options.do_classes.clone();
            let config = Arc::new(
                WorkerConfig::new(options)
                    .with_services(services)
                    .with_asset_binding(asset_binding)
                    .with_queue_consumers(queue_catalog.clone())
                    .with_generation(id),
            );
            let pool = StatelessRuntime::start(config.clone(), node.clone(), region.clone())?;
            if service_pools.insert(script.clone(), pool).is_some() {
                return Err(anyhow!("duplicate co-hosted Worker script {script}"));
            }
            for class in target_classes {
                register_cell_class(&mut cell_configs, class, config.clone(), &|class| {
                    anyhow!(
                        "Durable Object class {class} is exported by more than one co-hosted script"
                    )
                })?;
            }
        }
        let cell_isolates: HashMap<String, Arc<crate::pool::Pool>> = cell_configs
            .values()
            .map(|config| {
                let build = config.clone();
                (
                    config.script_name.clone(),
                    Arc::new(crate::pool::Pool::new(
                        pool_limits(),
                        admission_wait(),
                        Box::new(move || load_cell_isolate(build.clone())),
                    )),
                )
            })
            .collect();
        Ok(Generation {
            id,
            version,
            prefix,
            script_name: primary_script,
            stateless,
            services: service_pools,
            cell_configs,
            cell_isolates,
            default_do_class,
            assets,
            crons,
        })
    }
}
/// Claim one class name in a deployment's cell registry.
///
/// A duplicate is an error for a user class, because `start_cell` resolves a
/// cell from the class its scope names and two exports of one name are
/// genuinely ambiguous. A duplicate of a *shared* reserved class is not: see
/// `deploy::is_shared_reserved_class`. The caller supplies the error so the
/// message can say whether the collision was inside one script or across two.
fn register_cell_class(
    configs: &mut HashMap<String, Arc<WorkerConfig>>,
    class: String,
    config: Arc<WorkerConfig>,
    duplicate: &dyn Fn(&str) -> anyhow::Error,
) -> anyhow::Result<()> {
    match configs.entry(class) {
        Entry::Vacant(slot) => {
            slot.insert(config);
            Ok(())
        }
        Entry::Occupied(slot) if crate::deploy::is_shared_reserved_class(slot.key()) => Ok(()),
        Entry::Occupied(slot) => Err(duplicate(slot.key())),
    }
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
    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn region(&self) -> &str {
        &self.region
    }

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
        } = options;
        let max_cell_requests =
            crate::env_vars::positive_or("CELLD_MAX_CELL_REQUESTS", DEFAULT_MAX_CELL_REQUESTS)?;
        require_cell_scope_capacity(&data_dir)?;
        let node: Arc<str> = Arc::from(node);
        let region: Arc<str> = Arc::from(region);
        let cells = Arc::new(Mutex::new(CellRegistry::default()));
        let alarm_reporter = alarm_reporter(&cells, &alarm_observer);
        let generation = Arc::new(generation);
        let manager = Self {
            generations: Arc::new(std::sync::RwLock::new(Generations {
                current: generation.clone(),
                draining: Vec::new(),
            })),
            cells,
            alarm_reporter,
            remote_requests: Arc::new(Mutex::new(RemoteRequestRegistry::default())),
            alarm_requests: Arc::new(Mutex::new(AlarmRequestRegistry::default())),
            data_dir: Arc::new(data_dir),
            replication,
            wake,
            alarm_observer,
            node,
            region,
            max_cell_requests,
        };
        manager.watch_generation(&generation);
        Ok(manager)
    }

    /// The generation this node serves now.
    ///
    /// A caller that makes more than one decision from it takes the snapshot
    /// once and uses it throughout: the current generation can change between
    /// two reads, and a request must see one deployment graph, not two.
    pub fn generation(&self) -> Arc<Generation> {
        self.generations
            .read()
            .expect("generation lock poisoned")
            .current
            .clone()
    }

    /// The generation an isolate was built for: the current one when the id
    /// matches, otherwise a superseded generation still draining. Zero — the
    /// tag of an isolate built outside any generation — and an id whose
    /// generation has finished draining both resolve to the current one.
    pub fn generation_by_id(&self, id: GenerationId) -> Arc<Generation> {
        // One lock over both halves: read separately, a caller from the
        // previous generation could observe the new one as current before
        // the previous one was resolvable, find neither, and fall through to
        // the new graph -- the cross-generation call this exists to prevent.
        let generations = self.generations.read().expect("generation lock poisoned");
        if id == 0 || generations.current.id == id {
            return generations.current.clone();
        }
        generations
            .draining
            .iter()
            .find(|generation| generation.id == id)
            .cloned()
            .unwrap_or_else(|| generations.current.clone())
    }

    /// The id the next generation takes: one past the newest this node has
    /// built, draining generations included, so an id is never reused while
    /// an isolate still carries it.
    pub fn next_generation_id(&self) -> GenerationId {
        let generations = self.generations.read().expect("generation lock poisoned");
        let draining = generations
            .draining
            .iter()
            .map(|generation| generation.id)
            .max()
            .unwrap_or(0);
        generations.current.id.max(draining) + 1
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

    /// The generations still draining: id and version, for `/state`.
    pub fn draining_generations(&self) -> Vec<(GenerationId, String)> {
        self.generations
            .read()
            .expect("generation lock poisoned")
            .draining
            .iter()
            .map(|generation| (generation.id, generation.version.clone()))
            .collect()
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

    /// Resolve a client-supplied cell id to a scope.
    ///
    /// The id arrives from the network, and the scope it becomes is used as a
    /// path component and as an object-store key, so the charset gate runs
    /// first. Without it a scope carries its own path segments and `db_path`
    /// walks out of the data directory through them.
    ///
    /// The fleet-wide storage gate runs a second time on the composed scope. A
    /// bare id takes a class prefix, so the scope that reaches storage is the
    /// value that must fit.
    pub fn cell_scope(&self, id: &str) -> anyhow::Result<String> {
        if !celld_logic::cell::valid_cell_scope(id) {
            return Err(anyhow!("cell id is not a well-formed scope"));
        }
        if id.contains(':') {
            return Ok(id.to_string());
        }
        let generation = self.generation();
        let class = generation.default_do_class().ok_or_else(|| {
            anyhow!("a bare cell id requires exactly one configured Durable Object class")
        })?;
        let scope = format!("{class}:{id}");
        if !celld_logic::cell::valid_cell_scope(&scope) {
            return Err(anyhow!("cell id is not a well-formed scope"));
        }
        Ok(scope)
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
            url,
            method,
            body,
            headers,
            request_id: Some(request_id),
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
        let response = pool.fetch(url, method, body, headers, Some(cancellation.clone()));
        receive_service_fetch_response(response, cancellation, cancel).await
    }

    pub async fn rpc_service(
        &self,
        generation: GenerationId,
        script: &str,
        entrypoint: String,
        method: String,
        args: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        let generation = self.generation_by_id(generation);
        generation
            .service(script)
            .ok_or_else(|| anyhow!("no service Worker for script {script}"))?
            .rpc(entrypoint, method, args)
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
        if let Some(replication) = &self.replication {
            let (restored_path, restored, vfs) = replication.restore(cell, spec).await?;
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
            self.alarm_covered(cell, Some(at_ms))
        })
    }

    pub fn replication(&self) -> Option<Replication> {
        self.replication.clone()
    }

    /// Read the filesystem inventory after the core has replaced the exact
    /// clean predecessor lease generation.
    pub fn local_reload_cells(&self) -> anyhow::Result<Vec<celld_logic::LocalCell>> {
        let replication = self
            .replication
            .as_ref()
            .context("local reload requires replication")?;
        Ok(replication.local_cells())
    }

    /// Close every resident runtime, retain its exact database path, remove
    /// stale live-named epochs, and publish one node-level local certificate.
    /// The caller has already stopped admission and drained request effects.
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
                    runtime
                        .stop_cell(&cell.id, cell.epoch, StopMode::CloseInPlace)
                        .await?;
                    replication.close_for_reload(&cell.id, cell.epoch)
                }
            })
            .buffer_unordered(128);
        while let Some(result) = closes.next().await {
            result?;
        }
        let pruned = replication.prune_stale_live(&keep)?;
        Ok(pruned)
    }

    pub fn replication_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        match &self.replication {
            Some(replication) => replication.process_status(),
            None => Ok(None),
        }
    }

    pub async fn ensure_durable(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        match &self.replication {
            Some(replication) => replication.ensure_durable(cell, epoch).await,
            None => Ok(()),
        }
    }

    /// The output-gate durability wait (see `Replication::await_durable`).
    /// Returns the proved durable position and its proof source; with no
    /// replicator every position is trivially durable, and the fleet source
    /// keeps the gate read-free exactly like the old immediate release.
    pub async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<(u64, celld_logic::ProofSource)> {
        match &self.replication {
            Some(replication) => replication.await_durable(cell, epoch, position).await,
            None => Ok((position, celld_logic::ProofSource::Fleet)),
        }
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
                if let Some(error) = injected_swap_release_failure() {
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
        let isolate = residency.slot().clone();
        let placed_in = isolate.heap_id();

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
        let adopted = isolate
            .turn(|worker| {
                worker.own_cell(
                    &cell,
                    Some(CellStorage {
                        path: path_text(&db_path),
                        epoch,
                        vfs: paged_vfs.as_deref(),
                    }),
                )
            })
            .await;
        let alarm = match adopted {
            Ok(alarm) => alarm,
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
                    next_alarm_ms: AtomicI64::new(alarm.unwrap_or(-1)),
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
        if let Some(replication) = &self.replication {
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
            match mode {
                StopMode::Evict { preserve_local } => {
                    // A failed final sync leaves the handle and files in place.
                    // This call is therefore intentionally retryable after the
                    // runtime itself has already stopped.
                    replication.evict(cell, epoch, preserve_local).await?;
                }
                StopMode::Rebase => replication.release(cell, epoch).await?,
                StopMode::CloseInPlace => replication.close_in_place(cell, epoch).await?,
                StopMode::Discard => replication.ltx.discard(cell, epoch),
            }
        }
        Ok(())
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
        let cells = self.cells.lock().expect("cell registry poisoned");
        let at_ms = cells
            .published
            .get(cell)
            .or_else(|| cells.starting.get(cell))
            .map(|handle| handle.next_alarm_ms.load(Ordering::Acquire))
            .filter(|at_ms| *at_ms >= 0);
        f(at_ms)
    }

    pub fn alarm_covered(&self, cell: &str, at_ms: Option<i64>) -> bool {
        match (at_ms, &self.wake) {
            (None, _) => true,
            (Some(at_ms), Some(wake)) if self.replication.is_some() => wake.covered(cell, at_ms),
            (Some(_), None) => false,
            (Some(_), Some(_)) => false,
        }
    }

    pub async fn fire_alarm(
        &self,
        op: celld_logic::OpId,
        cell: String,
        scheduled_ms: i64,
    ) -> anyhow::Result<(Option<i64>, bool, Option<u64>)> {
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
        let final_write = drive.await.expect("cell alarm drive task panicked");
        // A cancelled or failed alarm settles its claim in the drive's final
        // isolate turn. The event reply is itself held behind every wake-entry
        // arm, so waiting for the drive makes that final cache authoritative
        // before the core sees completion.
        match result {
            Ok((at_ms, wrote)) => Ok((at_ms, self.alarm_covered(&cell, at_ms), wrote)),
            Err(error) => {
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
                let at_ms = self.alarm(&cell);
                Ok((
                    at_ms,
                    self.alarm_covered(&cell, at_ms),
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

    fn db_path(&self, cell: &str, epoch: u64) -> PathBuf {
        // A runtime without replication has no remote epoch namespace, so it
        // keeps its only SQLite family at the existing e1 path across logical
        // ownership epochs. The stable path survives a restart, needs no
        // multi-file SQLite family move, and remains compatible with older
        // releases.
        let epoch = if self.replication.is_some() { epoch } else { 1 };
        self.data_dir
            .join(cell)
            .join("ltx")
            .join(format!("e{epoch}"))
            .join("db.sqlite")
    }
}

fn restored_alarm_from_persisted(
    cell: &str,
    persisted: Option<(i64, i64, u32, u32)>,
    covered: impl FnOnce(i64) -> bool,
) -> Option<celld_logic::RestoredAlarm> {
    let at_ms = match persisted {
        Some((at_ms, ..)) => at_ms,
        None => -1,
    };
    if at_ms < 0 {
        // The durable truth this activation just restored has NO alarm —
        // but a wake entry may still be tracked (the due scan adopts the
        // entry that woke the cell). That entry disagrees with durable
        // truth: an arm whose commit never replicated, or a consume whose
        // delete was lost. Left alone it is immortal — one spurious
        // activation per waker tick, forever.
        // Reconciling against the empty truth deletes it; `take_delete`
        // re-checks at execution time, so an arm racing this activation
        // cancels the delete.
        if crate::js::wake_entry_tracked(cell) {
            let cell_ = cell.to_string();
            crate::asyncrt::spawn(async move {
                crate::js::reconcile_wake_entry(&cell_, -1, true).await;
            })
            .detach();
        }
        return None;
    }
    // The entry this alarm already has in the bucket was written by whoever
    // armed it, which is not this process once the cell went inactive. Claim
    // it now, while the alarm is in hand.
    crate::js::adopt_wake_entry(cell, at_ms);
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
    if let Some(timing) = &mut timing {
        timing.answered(&entry);
    }

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
            Wake::Poll => slot.turn(|worker| worker.turn_poll(&mut entry)).await,
        };
        if entry.keeps_native_ops() {
            adopt(&mut ops, started);
        } else {
            drop(started);
            abort_ops(&mut ops, &mut entry);
        }
        if let Some(timing) = &mut timing {
            timing.answered(&entry);
        }
    }

    // Dropping `ops` aborts whatever is still pending, which is what a region
    // does on every exit path; their resolvers have to go with them.
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
        let Some(left) = entry.remaining(budget) else {
            // The handler settled, so neither its reply gate nor waitUntil
            // work is charged to the handler budget. Poll both because the
            // background can progress while the gate owns the reply.
            let request_id = entry.request_id();
            if let Some(gated_reply) = entry.gated_reply() {
                let Some(request_id) = request_id else {
                    if ops.is_empty() {
                        return Wake::GatedReply(gated_reply.await);
                    }
                    return asyncrt::select_biased! {
                        "a completed gated reply wins a tie with an operation result";
                        completion = gated_reply => Wake::GatedReply(completion),
                        result = ops.next() => match result {
                            Some((op, result)) => Wake::Op(op, result),
                            None => Wake::Idle,
                        },
                    };
                };
                // The domain select is declaration-order biased. A reply that
                // completed wins over an op, and an op wins over a cancellation
                // tick, matching every other request wake boundary.
                let next = if ops.is_empty() {
                    asyncrt::select_biased! {
                        "a completed gated reply wins a tie with a cancellation tick";
                        completion = gated_reply => Some(Wake::GatedReply(completion)),
                        _ = asyncrt::sleep(CANCELLATION_TICK) => None,
                    }
                } else {
                    asyncrt::select_biased! {
                        "a completed gated reply wins a tie with another request wake";
                        completion = gated_reply => Some(Wake::GatedReply(completion)),
                        next = async {
                            asyncrt::select_biased! {
                                "an operation result wins a tie with a cancellation tick";
                                result = ops.next() => Some(match result {
                                    Some((op, result)) => Wake::Op(op, result),
                                    None => Wake::Idle,
                                }),
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
                return match ops.next().await {
                    Some((op, result)) => Wake::Op(op, result),
                    None => Wake::Idle,
                };
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
        let capped = if entry.cancellable() {
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
        if ops.is_empty() {
            if left.is_zero() {
                return Wake::Idle;
            }
            tokio::time::sleep(capped.min(CANCELLATION_TICK)).await;
            // Re-read the flag on this path too. A request with nothing
            // outstanding can still have its client hang up, and only the
            // branch below used to look.
            if let Some(cancelled) = take_cancellation_wake(entry.request_id()) {
                return cancelled;
            }
            return Wake::Poll;
        }
        match tokio::time::timeout(capped, ops.next()).await {
            Ok(Some((op, result))) => return Wake::Op(op, result),
            Ok(None) => return Wake::Idle,
            Err(_) => {
                if let Some(cancelled) = take_cancellation_wake(entry.request_id()) {
                    return cancelled;
                }
                if capped == left {
                    return Wake::Expired;
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
        let runtime = self.clone();
        let make_job = stateless_fetch_job_factory(url, method, body, headers, cancellation);
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
        method: String,
        args: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        self.dispatch(StatelessVerb::Rpc, move |reply| crate::WorkerJob::Rpc {
            entrypoint,
            method,
            args,
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
/// Fail the next release of a generation swap, for the black-box matrix.
///
/// A count rather than a latch, so a test can prove the retry reaches
/// success instead of only proving that it never proceeds. `debug_assertions`
/// is the gate `CELLD_TEST_CELL_STARTUP_FAILURE` beside it already uses: the
/// runtime matrix drives the shipped debug binary, which no private cfg
/// reaches, and a release build compiles neither.
#[cfg(debug_assertions)]
fn injected_swap_release_failure() -> Option<anyhow::Error> {
    static REMAINING: std::sync::OnceLock<AtomicI64> = std::sync::OnceLock::new();
    let remaining = REMAINING.get_or_init(|| {
        AtomicI64::new(
            std::env::var("CELLD_TEST_SWAP_RELEASE_FAILURES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
        )
    });
    (remaining.fetch_sub(1, Ordering::Relaxed) > 0)
        .then(|| anyhow!("injected generation swap release failure"))
}

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
    let budget = js::handler_budget();
    let mut ops = Ops::new();
    let (begun, started) = slot.turn(|worker| worker.turn_begin(job, trace)).await;
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
            Wake::Poll => slot.turn(|worker| worker.turn_poll(&mut entry)).await,
        };
        if entry.keeps_native_ops() {
            adopt(&mut ops, started);
        } else {
            drop(started);
            abort_ops(&mut ops, &mut entry);
        }
    }
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
    entry.abandon();
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
fn report_alarm_moves(report: &Option<AlarmReporter>, moves: Vec<(String, i64)>) {
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
    drive_cell_inner(
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
    drive_cell_inner(
        affiliation,
        job,
        report,
        parent,
        js::handler_budget(),
        cancellation,
    )
    .await;
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
) -> Option<u64> {
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
    drive_cell_inner(
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
    drive_cell_inner(
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
    drive_cell_inner(
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
    drive_cell_inner(
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
    drive_cell_inner(
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
    let gated = entry.gated_reply().is_some();
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
) -> Option<u64> {
    let _request_cancellation = request_cancellation.map(RequestCancellationGuard::shared);
    // Held to the end of this function: the event's claim on the isolate
    // outlives every suspension, so the pool cannot free the worker under
    // a parked event (denoland/celld#147).
    let slot = affiliation.slot().clone();
    let scope = job.scope().to_string();
    let mut queue_producer = if job.is_queue_producer() {
        match slot.queue_producer(&scope).await {
            Some(permit) => Some(permit),
            None => {
                job.fail(anyhow!(js::CellOverloaded));
                return None;
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
                        return None;
                    }
                    // The cell stopped while this event waited.
                    Err(_) => return None,
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
        return None;
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
    if entry.gated_reply().is_some() {
        if let Some(producer) = queue_producer.as_mut() {
            producer.reached_reply_gate();
        }
    }
    #[cfg(celld_internal_tests)]
    if entry.gated_reply().is_some() {
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
            Wake::Poll => {
                slot.turn(|worker| {
                    let started = worker.turn_poll(&mut entry);
                    (started, worker.take_alarm_moves())
                })
                .await
            }
        };
        if entry.gated_reply().is_some() {
            if let Some(producer) = queue_producer.as_mut() {
                producer.reached_reply_gate();
            }
        }
        #[cfg(celld_internal_tests)]
        if entry.gated_reply().is_some() {
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
    let mut alarm_write = None;
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
