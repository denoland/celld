// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// RuntimeManager is the V8 cell-host arm. A later scripted cell host replaces
// it in the World, so its executor and observability clocks remain ambient.
#![allow(clippy::disallowed_macros, clippy::disallowed_methods)]

//! V8 runtime materialization behind core-authorized lifecycle effects.
//!
//! The manager owns handles and filesystem paths, never lifecycle policy.
//! StartRuntime, Publish, and StopRuntime decide when a cell handle moves from
//! starting to externally dispatchable to closed.

use crate::asyncrt;
use crate::js::{self, CellJob, HttpResponse, Worker, WorkerConfig, WorkerConfigOptions};
use crate::ltx_repl::LtxRepl;
use crate::replication::{ActivationOptions, StorageCredentials, SyncWait};
use crate::wake::WakeFlusher;
use anyhow::{anyhow, Context};
use futures_util::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant};

const REMOTE_ABORT_TTL: Duration = Duration::from_secs(600);

/// How often the isolate pool gives back what it no longer needs.
const REAP_INTERVAL: Duration = Duration::from_secs(30);

/// How often a suspended request re-reads its cancellation flag. Matches the
/// blocking run loop's own cap, which exists for the same reason: a client
/// disconnect is raised on another thread and has nothing to wake this one.
const CANCELLATION_TICK: Duration = Duration::from_millis(10);
const CLEAN_RELOAD_MARKER: &str = ".clean-reload.json";

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

fn prune_remote_aborts(aborts: &mut HashMap<js::RequestId, Instant>) {
    aborts.retain(|_, created| created.elapsed() < REMOTE_ABORT_TTL);
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
struct StatelessRuntime {
    node: Arc<str>,
    region: Arc<str>,
    /// The isolates fetch runs on, entered one turn at a time from whichever
    /// tokio worker is driving the request.
    isolates: Arc<crate::pool::Pool>,
}

struct CellHandle {
    epoch: u64,
    startup_us: u64,
    /// The cell's claim on the isolate holding its realm. An event knows
    /// where to run from it, and dropping it gives the placement back.
    residency: crate::pool::Residency,
    /// The last alarm the reporter saw, `-1` for none: the cache behind
    /// `alarm()`'s point query. Written only by the effect path — a turn
    /// moves an alarm, its drive reports it — never by storage directly.
    next_alarm_ms: AtomicI64,
}

pub type AlarmObserver = Arc<dyn Fn(String, Option<i64>) + Send + Sync>;

/// How a turn's alarm move reaches the host. `drive_cell` calls it with
/// what `take_alarm_moves` drained; it caches the value on the cell's
/// handle and forwards a real change to the observer.
pub(crate) type AlarmReporter = Arc<dyn Fn(String, i64) + Send + Sync>;

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

#[derive(Clone)]
pub struct RuntimeManager {
    stateless: StatelessRuntime,
    services: Arc<HashMap<String, StatelessRuntime>>,
    cell_configs: Arc<HashMap<String, Arc<WorkerConfig>>>,
    /// The isolates a Worker script's cells live in — the same `Pool` the
    /// stateless path admits into, because an isolate is an isolate. Cells
    /// of one script share them, so cells of one class share module scope
    /// exactly when they are colocated, which is what Durable Objects do.
    cell_isolates: Arc<HashMap<String, Arc<crate::pool::Pool>>>,
    cells: Arc<Mutex<CellRegistry>>,
    alarm_reporter: AlarmReporter,
    /// A peer abort can arrive before the forwarded fetch. The tombstone and
    /// cell enqueue share this lock so neither ordering can lose cancellation.
    remote_aborts: Arc<Mutex<HashMap<js::RequestId, Instant>>>,
    data_dir: Arc<PathBuf>,
    default_do_class: Option<Arc<str>>,
    replication: Option<Replication>,
    wake: Option<Arc<WakeFlusher>>,
    alarm_observer: AlarmObserver,
    node: Arc<str>,
    region: Arc<str>,
}

/// The Actor's cell-runtime boundary. Ordinary builds contain only the V8
/// arm; an internal test build adds the deterministic scripted arm.
#[derive(Clone)]
pub(crate) enum CellHost {
    V8(RuntimeManager),
    #[cfg(all(test, celld_internal_tests))]
    Scripted(crate::conformance_sim_cell_host::SimCellHost),
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

    pub(crate) async fn start_cell(
        &self,
        cell: String,
        epoch: u64,
        fresh: bool,
    ) -> anyhow::Result<celld_logic::isolate::IsolateId> {
        match self {
            Self::V8(runtime) => runtime.start_cell(cell, epoch, fresh).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.start_cell(cell, epoch, fresh).await,
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
        evict: bool,
        preserve_local: bool,
    ) {
        match self {
            Self::V8(runtime) => runtime.stop_cell(cell, epoch, evict, preserve_local).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => {
                runtime.stop_cell(cell, epoch, evict, preserve_local).await;
            }
        }
    }

    pub(crate) async fn fire_alarm(
        &self,
        cell: String,
        scheduled_ms: i64,
    ) -> anyhow::Result<(Option<i64>, bool, Option<u64>)> {
        match self {
            Self::V8(runtime) => runtime.fire_alarm(cell, scheduled_ms).await,
            #[cfg(all(test, celld_internal_tests))]
            Self::Scripted(runtime) => runtime.fire_alarm(cell, scheduled_ms).await,
        }
    }
}

pub struct CohostedWorker {
    pub options: WorkerConfigOptions,
    pub services: Vec<(String, String, Option<String>)>,
    pub asset_binding: Option<String>,
}

pub struct RuntimeOptions {
    pub worker: WorkerConfigOptions,
    pub services: Vec<(String, String, Option<String>)>,
    pub asset_binding: Option<String>,
    pub loader_binding: Option<String>,
    pub cohosted: Vec<CohostedWorker>,
    pub data_dir: PathBuf,
    pub replication: Option<Replication>,
    pub wake: Option<Arc<WakeFlusher>>,
    pub alarm_observer: AlarmObserver,
    pub node: String,
    pub region: String,
    /// `triggers.crons` from the deployment, driving the reserved cron cell.
    pub crons: Vec<String>,
}

/// Owned HTTP request crossing from the async shell into a V8 executor.
pub struct RuntimeFetch {
    pub url: String,
    pub method: String,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub request_id: Option<js::RequestId>,
    /// Where this call sits in its caller's order for this cell.
    pub order: Option<js::CallOrder>,
    /// The dispatching Worker's trace context, so the cell's span joins
    /// the caller's trace instead of rooting a disconnected one.
    pub parent: Option<crate::telemetry::TraceIds>,
}

/// The node's replication engine: the in-process `celld-ltx` replicator,
/// hidden behind this wrapper so nothing else touches the backend directly.
#[derive(Clone)]
pub struct Replication {
    ltx: Arc<LtxRepl>,
}

impl Replication {
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
    ) -> anyhow::Result<(PathBuf, bool)> {
        let options = ActivationOptions {
            cell,
            epoch: spec.epoch,
            fresh: spec.fresh,
            took_over: spec.took_over,
            resume_local: spec.resume_local,
            prior: spec.prior.clone(),
        };
        let activated = self.ltx.activate(options).await?;
        Ok((activated.path, activated.restored))
    }

    /// Drive/observe this cell's durability, the primitive shared by the two
    /// durability gates and the refusal check.
    async fn sync_wait(&self, cell: &str, epoch: u64) -> SyncWait {
        self.ltx
            .sync_wait(cell, epoch, Duration::from_secs(10))
            .await
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
        match self.sync_wait(cell, epoch).await {
            SyncWait::Durable => {}
            SyncWait::Unsupported | SyncWait::Failed => {
                return Err(anyhow!(
                    "replica durability could not be proved for {cell} epoch {epoch}"
                ))
            }
        }
        // Then ask the bucket, because these are not the same question.
        // `sync_wait` asks the replicator about a path it must have registered;
        // registration is not guaranteed to cover every published cell.
        // Hibernation deletes the only local copy, so the last thing checked
        // before it goes has to be the artifact itself rather than a report.
        let replicated = self.ltx.epoch_replicated(cell, epoch).await;
        if !replicated {
            return Err(anyhow!(
                "no replica objects for {cell} epoch {epoch}; refusing to \
                 evict state the bucket cannot restore"
            ));
        }
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

    async fn evict(&self, cell: &str, epoch: u64, preserve_local: bool) {
        self.ltx.evict(cell, epoch, preserve_local).await
    }

    fn release(&self, cell: &str, epoch: u64) {
        self.ltx.release(cell, epoch)
    }
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
        !self.cell_configs.is_empty()
    }

    /// The reserved cell carrying this deployment's cron schedule, or `None`
    /// when the deployment declares no `triggers.crons`. Asked at boot so the
    /// schedule is armed without a request; the answer is derived from the
    /// registered class rather than plumbed separately, so it cannot disagree
    /// with what `start_cell` will accept.
    pub fn cron_cell(&self) -> Option<String> {
        self.cell_configs
            .get(celld_logic::cron::RESERVED_CLASS)
            .map(|config| celld_logic::cron::reserved_cell(&config.script_name))
    }

    pub fn start(options: RuntimeOptions) -> anyhow::Result<Self> {
        let RuntimeOptions {
            worker,
            services,
            asset_binding,
            loader_binding,
            cohosted,
            data_dir,
            replication,
            wake,
            alarm_observer,
            node,
            region,
            crons,
        } = options;
        require_cell_scope_capacity(&data_dir)?;
        init_v8();

        let node: Arc<str> = Arc::from(node);
        let region: Arc<str> = Arc::from(region);
        let primary_script = worker.script_name.clone();
        let primary_classes = worker.do_classes.clone();
        // Only classes the user declared can be a bare-id default. The
        // runtime-supplied `__D1Database` rides in `do_classes` so that its
        // namespace key is minted, and counting it here made adding any D1
        // binding flip a one-class project past the `len == 1` test — every
        // `/do/<bare-id>` request then failed with "requires exactly one
        // configured Durable Object class" for a config that still declared
        // exactly one.
        let user_classes: Vec<&String> = worker
            .do_classes
            .iter()
            .filter(|class| class.as_str() != crate::deploy::D1_CLASS)
            .collect();
        let default_do_class =
            (user_classes.len() == 1).then(|| Arc::from(user_classes[0].as_str()));
        let config = Arc::new(
            WorkerConfig::new(worker)
                .with_services(services)
                .with_asset_binding(asset_binding)
                .with_loader(loader_binding)
                .with_crons(crons),
        );
        let stateless = StatelessRuntime::start(config.clone(), node.clone(), region.clone())?;
        let mut service_pools = HashMap::from([(primary_script, stateless.clone())]);
        let mut cell_configs = HashMap::new();
        for class in primary_classes {
            if cell_configs.insert(class.clone(), config.clone()).is_some() {
                return Err(anyhow!("duplicate Durable Object class {class}"));
            }
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
            let script = target.options.script_name.clone();
            let target_classes = target.options.do_classes.clone();
            let config = Arc::new(
                WorkerConfig::new(target.options)
                    .with_services(target.services)
                    .with_asset_binding(target.asset_binding),
            );
            let pool = StatelessRuntime::start(config.clone(), node.clone(), region.clone())?;
            if service_pools.insert(script.clone(), pool).is_some() {
                return Err(anyhow!("duplicate co-hosted Worker script {script}"));
            }
            for class in target_classes {
                if cell_configs.insert(class.clone(), config.clone()).is_some() {
                    return Err(anyhow!(
                        "Durable Object class {class} is exported by more than one co-hosted script"
                    ));
                }
            }
        }
        let cells = Arc::new(Mutex::new(CellRegistry::default()));
        let alarm_reporter = alarm_reporter(&cells, &alarm_observer);
        let cell_isolates: Arc<HashMap<String, Arc<crate::pool::Pool>>> = Arc::new(
            cell_configs
                .values()
                .map(|config| {
                    let config = config.clone();
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
                .collect(),
        );
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let reaping = cell_isolates.clone();
            handle.spawn(async move {
                let mut tick = tokio::time::interval_at(
                    tokio::time::Instant::now() + REAP_INTERVAL,
                    REAP_INTERVAL,
                );
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    for pool in reaping.values() {
                        pool.reap_empty();
                    }
                }
            });
        }
        Ok(Self {
            stateless,
            services: Arc::new(service_pools),
            cell_isolates,
            cell_configs: Arc::new(cell_configs),
            cells,
            alarm_reporter,
            remote_aborts: Arc::new(Mutex::new(HashMap::new())),
            data_dir: Arc::new(data_dir),
            default_do_class,
            replication,
            wake,
            alarm_observer,
            node,
            region,
        })
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
        let class = self.default_do_class.as_deref().ok_or_else(|| {
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
        self.stateless
            .fetch(url, method, body.into(), headers, None)
            .await
    }

    /// Dispatch a cancellable top-level Worker request to the stateless pool.
    pub async fn fetch_worker_pool(
        &self,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        request_id: js::RequestId,
    ) -> anyhow::Result<HttpResponse> {
        self.stateless
            .fetch(url, method, body, headers, Some(request_id))
            .await
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
        // route to, so `env.NS.get(ownScope)` resolves in-isolate instead of
        // going back out through the host.
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
            body: body.into(),
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

    pub async fn fetch_service(
        &self,
        script: &str,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
        cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> anyhow::Result<HttpResponse> {
        let pool = self
            .services
            .get(script)
            .cloned()
            .ok_or_else(|| anyhow!("no service Worker for script {script}"))?;
        let request_id = js::next_request_id();
        let response = pool.fetch(url, method, body.into(), headers, Some(request_id));
        match cancel {
            Some(mut cancel) => tokio::select! {
                response = response => response,
                _ = &mut cancel => {
                    js::abort_request(request_id);
                    Err(anyhow!("service-binding caller disconnected"))
                }
            },
            None => response.await,
        }
    }

    pub async fn rpc_service(
        &self,
        script: &str,
        entrypoint: String,
        method: String,
        args: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        self.services
            .get(script)
            .cloned()
            .ok_or_else(|| anyhow!("no service Worker for script {script}"))?
            .rpc(entrypoint, method, args)
            .await
    }

    pub async fn restore_cell(
        &self,
        cell: &str,
        spec: &celld_logic::RestoreSpec,
    ) -> anyhow::Result<celld_logic::RestoreOutcome> {
        let path = self.db_path(cell, spec.epoch);
        if let Some(replication) = &self.replication {
            let (restored_path, restored) = replication.restore(cell, spec).await?;
            if restored_path != path {
                return Err(anyhow!(
                    "replication restored {} instead of {}",
                    restored_path.display(),
                    path.display()
                ));
            }
            return Ok(celld_logic::RestoreOutcome {
                restored,
                alarm: self.restored_alarm(cell, &path),
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
            alarm: self.restored_alarm(cell, &path),
        })
    }

    /// The alarm the restored database already had armed, read directly by
    /// path. Read-only, and the connection is dropped here -- the isolate
    /// opens the same file moments later through `spawn_cell`.
    fn restored_alarm(
        &self,
        cell: &str,
        path: &std::path::Path,
    ) -> Option<celld_logic::RestoredAlarm> {
        restored_alarm_from_path(cell, path, |at_ms| self.alarm_covered(cell, Some(at_ms)))
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
                    runtime.stop_cell(&cell.id, cell.epoch, false, true).await;
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

    /// Materialize an isolate and retain it as non-routable until publication.
    /// Start a cell's runtime and report which isolate took its realm. The
    /// core groups eviction on that, so it has to come back from here.
    pub async fn start_cell(
        &self,
        cell: String,
        epoch: u64,
        fresh: bool,
    ) -> anyhow::Result<celld_logic::isolate::IsolateId> {
        let db_path = self.db_path(&cell, epoch);
        let class = cell
            .split_once(':')
            .map(|(class, _)| class)
            .ok_or_else(|| anyhow!("cell scope has no class: {cell}"))?;
        let config = self
            .cell_configs
            .get(class)
            .cloned()
            .ok_or_else(|| anyhow!("no Worker exports Durable Object class {class}"))?;
        let startup_timing = CellIsolateStartupTiming {
            started: Instant::now(),
            scope: cell.clone(),
            node: self.node.clone(),
            region: self.region.clone(),
            epoch,
            fresh,
        };

        let isolates = self
            .cell_isolates
            .get(&config.script_name)
            .cloned()
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
        let placed_in = isolate.id;

        // Everything the cell needs that the isolate must do: open its
        // SQLite — which the isolate owns, not the caller — restore its
        // persisted id name, and record `__cell.owned` so the harness can
        // dispatch to it without a host round trip.
        //
        // A direct call rather than a job: adoption is not an event, it runs
        // no handler, and it needs one turn.
        let adopted = isolate
            .turn(|worker| worker.own_cell(&cell, Some(path_text(&db_path)), true))
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
                    startup_us,
                    residency,
                    next_alarm_ms: AtomicI64::new(alarm.unwrap_or(-1)),
                },
            );
            Ok(placed_in)
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

    pub async fn stop_cell(&self, cell: &str, epoch: u64, evict: bool, preserve_local: bool) {
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
            let _ = handle
                .residency
                .slot()
                .turn(|worker| worker.own_cell(cell, None, false))
                .await;
            // Dropping the handle drops its residency, which is what gives
            // the isolate its place back — and what lets `retire` reclaim
            // the isolate once no cell is left in it.
            drop(handle);
        }
        if let Some(replication) = &self.replication {
            // Every stop releases the handle, and this does not test
            // `stopped_runtime`. The replication entry is created by
            // `Effect::Restore` and the registry entry by
            // `Effect::StartRuntime`, so the entry outlives a start that fails
            // between the two and there is nothing else that would ever remove
            // it. Its lifetime is the activation's, not the registry's.
            //
            // Eviction takes the orderly file path. A reset discards the local
            // database without another durability attempt, while every other
            // stop releases the handle and keeps its files for reactivation.
            if evict {
                replication.evict(cell, epoch, preserve_local).await;
            } else if preserve_local {
                replication.release(cell, epoch);
            } else {
                replication.ltx.discard(cell, epoch);
            }
        }
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
        let isolate = self.cell_isolate(&cell)?;
        let (reply, mut receive) = tokio::sync::oneshot::channel();
        let scope = cell.clone();
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
        if let Some(request_id) = request_id {
            let mut aborts = self.remote_aborts.lock().expect("abort registry poisoned");
            prune_remote_aborts(&mut aborts);
            if aborts.remove(&request_id).is_some() {
                return Err(anyhow!("the client disconnected before dispatch"));
            }
        }
        tokio::spawn(drive_cell(
            isolate,
            job,
            Some(self.alarm_reporter.clone()),
            parent,
        ));
        let result = match (request_id, cancel) {
            (Some(request_id), Some(mut cancel)) => tokio::select! {
                result = &mut receive => result,
                cancelled = &mut cancel => {
                    if cancelled.is_ok() {
                        // The drive re-reads this between turns and enters
                        // the isolate only once it has really fired.
                        js::abort_request(request_id);
                    }
                    receive.await
                }
            },
            _ => receive.await,
        }
        .context("cell isolate dropped response")?;
        if let Some(request_id) = request_id {
            self.remote_aborts
                .lock()
                .expect("abort registry poisoned")
                .remove(&request_id);
        }
        js::drain_arm_gates(&scope)
            .await
            .map_err(|error| anyhow!(error))?;
        result
    }

    /// Tell a cell to abandon a fetch, by name.
    ///
    /// `fetch_cell`'s own cancellation needs someone still awaiting it, which
    /// is exactly what a dropped connection does not leave behind: the future
    /// carrying the `select!` dies in the same instant as the signal. A caller
    /// that learns about the hang-up in a destructor has to say so directly.
    pub fn abort_fetch(&self, cell: &str, request_id: js::RequestId) {
        let mut aborts = self.remote_aborts.lock().expect("abort registry poisoned");
        prune_remote_aborts(&mut aborts);
        aborts.insert(request_id, Instant::now());
        drop(aborts);
        js::abort_request(request_id);
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
        cell: String,
        scheduled_ms: i64,
    ) -> anyhow::Result<(Option<i64>, bool, Option<u64>)> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::Alarm {
            scope: cell.clone(),
            scheduled_ms,
            claim: js::AlarmDispatch::Due,
            reply,
        };
        let (at_ms, wrote) = self
            .cell_event(&cell, job, receive, "cell isolate dropped alarm result")
            .await?;
        js::drain_arm_gates(&cell)
            .await
            .map_err(|error| anyhow!(error))?;
        Ok((at_ms, self.alarm_covered(&cell, at_ms), wrote))
    }

    pub async fn ws_open(&self, cell: String, ws_id: u64, protocol: String) -> anyhow::Result<()> {
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
    ) -> anyhow::Result<js::RpcOutcome> {
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = CellJob::Rpc {
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

    pub async fn ws_closed(
        &self,
        cell: String,
        ws_id: u64,
        code: u16,
        reason: String,
        was_clean: bool,
    ) -> anyhow::Result<()> {
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

fn restored_alarm_from_path(
    cell: &str,
    path: &std::path::Path,
    covered: impl FnOnce(i64) -> bool,
) -> Option<celld_logic::RestoredAlarm> {
    let persisted = crate::storage::persisted_alarm(&path.to_string_lossy(), cell);
    let at_ms = match persisted {
        Some((at_ms, ..)) => at_ms,
        None => -1,
    };
    if at_ms < 0 {
        #[cfg(all(test, celld_internal_tests))]
        if crate::asyncrt::sabotage_active(
            crate::host_services::EngineSabotage::IgnoreAlarmConsumeOnRestore,
        ) {
            if let Some(stale_at_ms) = crate::js::tracked_wake_due_ms(cell) {
                crate::js::adopt_wake_entry(cell, stale_at_ms);
                return Some(celld_logic::RestoredAlarm {
                    at_ms: stale_at_ms,
                    covered: covered(stale_at_ms),
                });
            }
        }
        // The durable truth this activation just restored has NO alarm —
        // but a wake entry may still be tracked (the due scan adopts the
        // entry that woke the cell). That entry disagrees with durable
        // truth: an arm whose commit never replicated, or a consume whose
        // delete was lost. Left alone it is immortal — one spurious
        // activation per waker tick, forever (the item-6 audit's cost leak).
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
    restored_alarm_from_path(cell, path, |_| covered)
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

/// Drive one stateless request to completion, one turn at a time.
///
/// The loop the pump used to run, owned by the request instead. Between turns
/// it holds no isolate — only its affiliation, which is memory rather than
/// CPU — so a handler awaiting I/O stops nothing else in that isolate.
pub(crate) async fn drive(
    slot: Arc<crate::pool::Slot>,
    job: crate::WorkerJob,
    telemetry: Option<(Arc<str>, Arc<str>)>,
) {
    drive_affiliated(slot.affiliate(), job, telemetry).await;
}

async fn drive_affiliated(
    affiliation: crate::pool::Affiliation,
    job: crate::WorkerJob,
    telemetry: Option<(Arc<str>, Arc<str>)>,
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
    let mut timing = telemetry
        .map(|(node, region)| FetchTiming::start(&job, slot.id, node, region, trace, remote));
    // Admission created `affiliation` before returning the isolate. It stays
    // here for the request's whole life, so maintenance cannot free the heap
    // between placement and this first turn or while a promise is suspended.
    let _affiliation = affiliation;
    let budget = js::handler_budget();
    let mut ops = Ops::new();

    let (begun, started) = slot.turn(|worker| worker.turn_begin(job, trace)).await;
    adopt(&mut ops, started);
    // Nothing is in flight; the reply already carries the error.
    let Some(mut entry) = begun else { return };
    if let Some(timing) = &mut timing {
        timing.answered(&entry);
    }

    while !entry.finished() {
        let started = match wake(&mut ops, &entry, budget).await {
            Wake::Op(op, result) => {
                slot.turn(|worker| worker.turn_deliver(&mut entry, op, result))
                    .await
            }
            Wake::Cancelled => slot.turn(|worker| worker.turn_cancel(&mut entry)).await,
            Wake::Expired => {
                entry.time_out(budget);
                break;
            }
            Wake::Idle => {
                entry.stuck();
                break;
            }
            Wake::Poll => slot.turn(|worker| worker.turn_poll(&mut entry)).await,
        };
        adopt(&mut ops, started);
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
    /// Its client hung up.
    Cancelled,
    /// It ran past the handler budget without answering.
    Expired,
    /// Nothing outstanding could ever move it.
    Idle,
    /// Nothing of its own is outstanding, but another event of the same cell
    /// still could settle it. Look in and see.
    Poll,
}

/// Wait for whichever comes first, holding no isolate.
///
/// This is the whole of what a request does between turns, and it is
/// deliberately the only place that waits: everything else in `drive` either
/// holds the isolate or is arithmetic.
async fn wake(ops: &mut Ops, entry: &js::InFlight, budget: Duration) -> Wake {
    loop {
        let Some(left) = entry.remaining(budget) else {
            // Answered already, so this is `waitUntil` work: not charged the
            // handler budget, and with no client left to hang up.
            return match ops.next().await {
                Some((op, result)) => Wake::Op(op, result),
                None => Wake::Idle,
            };
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
        // Worker awaits a Durable Object it dispatched to in-isolate. Both
        // used to resolve inside the caller's own run loop, so there was
        // nothing to wait for; both are separate entries now.
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
            if js::take_request_cancellation(entry.request_id()) {
                return Wake::Cancelled;
            }
            return Wake::Poll;
        }
        match tokio::time::timeout(capped, ops.next()).await {
            Ok(Some((op, result))) => return Wake::Op(op, result),
            Ok(None) => return Wake::Idle,
            Err(_) if js::take_request_cancellation(entry.request_id()) => return Wake::Cancelled,
            // The wait was the whole remaining budget, so it is spent.
            Err(_) if capped == left => return Wake::Expired,
            // Only a cancellation tick elapsed; keep waiting.
            Err(_) => continue,
        }
    }
}

/// One stateless request's canonical timing event.
///
/// The phases still mean what they always did, but what they measure has
/// moved: `queue_wait_us` was the wait for a free worker thread and is now
/// the wait for admission and the isolate's async gate, and `execution_us`
/// spans every turn the request took rather than one uninterrupted run.
struct FetchTiming {
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
}

impl FetchTiming {
    fn start(
        job: &crate::WorkerJob,
        isolate: usize,
        node: Arc<str>,
        region: Arc<str>,
        trace: Option<crate::telemetry::TraceIds>,
        remote_parent: Option<crate::telemetry::ParentContext>,
    ) -> Self {
        let (queued_at, request_id) = match job {
            crate::WorkerJob::Fetch {
                queued_at,
                request_id,
                ..
            } => (*queued_at, *request_id),
            _ => (Instant::now(), None),
        };
        FetchTiming {
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
            let mut span =
                crate::telemetry::Span::new(ids, "celld.fetch", crate::telemetry::KIND_SERVER);
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
    static V8_INIT: Once = Once::new();
    V8_INIT.call_once(js::Engine::init);
}

impl StatelessRuntime {
    fn start(config: Arc<WorkerConfig>, node: Arc<str>, region: Arc<str>) -> anyhow::Result<Self> {
        let build = {
            let config = config.clone();
            move || Worker::load_config(config.clone(), &[])
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
            let reaping = isolates.clone();
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
                    reaping.reap();
                }
            });
        }
        Ok(Self {
            isolates,
            node,
            region,
        })
    }

    /// Serve one stateless request, entering an isolate once per turn.
    ///
    /// The request drives itself: it is admitted and placed, runs its first
    /// turn, then awaits its *own* ops with no isolate held, re-entering for
    /// each completion. Nothing multiplexes and nothing demultiplexes, which
    /// is what deleting the pump buys.
    async fn fetch(
        &self,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        request_id: Option<js::RequestId>,
    ) -> anyhow::Result<HttpResponse> {
        let shedding = crate::ownership_store::node_is_shedding();
        let affiliation = self.isolates.admit_or_wait(shedding).await?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Fetch {
            queued_at: Instant::now(),
            url,
            method,
            body,
            headers,
            request_id,
            reply,
        };
        // Spawned rather than awaited inline, because the response and the
        // request are not the same event: `waitUntil` work outlives the
        // answer, and the driver keeps turning until it settles.
        let driving = tokio::spawn(drive_affiliated(
            affiliation,
            job,
            Some((self.node.clone(), self.region.clone())),
        ));
        match receive.await {
            Ok(response) => response,
            // The driver dropped the reply without sending. Joining it here —
            // and only here — turns a bare "channel closed" into the panic
            // that actually caused it, at no cost on the path that works.
            Err(_) => match driving.await {
                Err(error) => Err(anyhow!("stateless request task died: {error}")),
                Ok(()) => Err(anyhow!("stateless Worker dropped response")),
            },
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
        let shedding = crate::ownership_store::node_is_shedding();
        let affiliation = self.isolates.admit_or_wait(shedding).await?;
        let (reply, receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Rpc {
            entrypoint,
            method,
            args,
            reply,
        };
        let driving = tokio::spawn(drive_affiliated(
            affiliation,
            job,
            Some((self.node.clone(), self.region.clone())),
        ));
        match receive.await {
            Ok(result) => result,
            Err(_) => match driving.await {
                Err(error) => Err(anyhow!("stateless RPC task died: {error}")),
                Ok(()) => Err(anyhow!("stateless Worker dropped RPC result")),
            },
        }
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
        if let Some(ids) = crate::telemetry::start_trace() {
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
/// No cells yet: `Worker::own_cell` fills in `__cell.owned` as each is
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
    Worker::load_config(config, &[]).map_err(|error| error.context("cell isolate load failed"))
}

/// Drive the Worker entry in a cell's isolate.
///
/// The stateless `drive` loop, with the isolate named rather than admitted:
/// this request must run *here*, because the cell it will route to lives
/// here and routing to it in-isolate is the point.
async fn drive_worker_on_cell(affiliation: crate::pool::Affiliation, job: crate::WorkerJob) {
    // Held to the end of this function: the request's claim on the isolate
    // outlives every suspension, so the pool cannot free the worker under
    // a parked event (denoland/celld#147).
    let slot = affiliation.slot().clone();
    let remote = inbound_parent(&job);
    let trace = crate::telemetry::start_trace_with_parent(remote.as_ref());
    let span_started = trace.map(|_| (Instant::now(), crate::telemetry::now_unix_us()));
    let budget = js::handler_budget();
    let mut ops = Ops::new();
    let (begun, started) = slot.turn(|worker| worker.turn_begin(job, trace)).await;
    adopt(&mut ops, started);
    let Some(mut entry) = begun else {
        return;
    };
    while !entry.finished() {
        let started = match wake(&mut ops, &entry, budget).await {
            Wake::Op(op, result) => {
                slot.turn(|worker| worker.turn_deliver(&mut entry, op, result))
                    .await
            }
            Wake::Cancelled => slot.turn(|worker| worker.turn_cancel(&mut entry)).await,
            Wake::Expired => {
                entry.time_out(budget);
                break;
            }
            Wake::Idle => {
                entry.stuck();
                break;
            }
            Wake::Poll => slot.turn(|worker| worker.turn_poll(&mut entry)).await,
        };
        adopt(&mut ops, started);
    }
    if let (Some(ids), Some((started, start_unix))) = (trace, span_started) {
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
pub(crate) async fn drive_cell(
    affiliation: crate::pool::Affiliation,
    mut job: CellJob,
    report: Option<AlarmReporter>,
    parent: Option<crate::telemetry::TraceIds>,
) {
    // Held to the end of this function: the event's claim on the isolate
    // outlives every suspension, so the pool cannot free the worker under
    // a parked event (denoland/celld#147).
    let slot = affiliation.slot().clone();
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
    let span_seed = trace.map(|_| {
        let name = match &job {
            CellJob::Fetch { .. } => "celld.cell_fetch",
            CellJob::Alarm { .. } => "celld.alarm",
            CellJob::Rpc { .. } => "celld.rpc",
            CellJob::WsOpen { .. } => "celld.ws_open",
            CellJob::WsMessage { .. } => "celld.ws_message",
            CellJob::WsClosed { .. } => "celld.ws_close",
        };
        (
            name,
            job.scope().to_string(),
            Instant::now(),
            crate::telemetry::now_unix_us(),
        )
    });
    let budget = js::handler_budget();
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
    // `cell_gate_wait` tests and enqueues under the gate's own lock, so the
    // ticket cannot be missed by a release landing between the two. Only the
    // waiting happens out here, because a turn may not await.
    let mut pending = Some(job);
    let (begun, started, moves) = loop {
        let mut waiting = None;
        let taken = slot
            .turn(|worker| {
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
                        return;
                    }
                    // The cell stopped while this event waited.
                    Err(_) => return,
                },
            },
        }
    };
    // Delivered. Whatever the caller sent next may go.
    if let Some(order) = order.as_mut() {
        order.delivered();
    }
    drop(order);
    adopt(&mut ops, started);
    report_alarm_moves(&report, moves);
    // Nothing is in flight; the reply already carries the error.
    let Some(mut entry) = begun else {
        return;
    };

    while !entry.finished() {
        let (started, moves) = match wake(&mut ops, &entry, budget).await {
            Wake::Op(op, result) => {
                slot.turn(|worker| {
                    let started = worker.turn_deliver(&mut entry, op, result);
                    (started, worker.take_alarm_moves())
                })
                .await
            }
            Wake::Cancelled => {
                slot.turn(|worker| {
                    let started = worker.turn_cancel(&mut entry);
                    (started, worker.take_alarm_moves())
                })
                .await
            }
            Wake::Expired => {
                entry.time_out(budget);
                break;
            }
            Wake::Idle => {
                entry.stuck();
                break;
            }
            Wake::Poll => {
                slot.turn(|worker| {
                    let started = worker.turn_poll(&mut entry);
                    (started, worker.take_alarm_moves())
                })
                .await
            }
        };
        adopt(&mut ops, started);
        // A turn that ran JS may have armed an alarm this very request is
        // waiting on, so the host hears about it now rather than when the
        // request ends.
        report_alarm_moves(&report, moves);
    }

    // An alarm that ended without running JS again still owes its outcome —
    // `fail` deliberately leaves the claim, because it runs between turns
    // where cell storage is unreachable (denoland/celld#170) — and
    // recording it is storage only the isolate can reach.
    if entry.owes_alarm() {
        let moves = slot
            .turn(|worker| {
                worker.turn_finish_alarm(&mut entry);
                worker.take_alarm_moves()
            })
            .await;
        report_alarm_moves(&report, moves);
    }
    if let (Some(ids), Some((name, cell, started, start_unix))) = (trace, span_seed) {
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
    entry.abandon();
}

fn path_text(path: &Path) -> &str {
    path.to_str().expect("celld data path must be UTF-8")
}
