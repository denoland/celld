// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! One application deployment as the running process holds it.
//!
//! A [`Generation`] is everything a node derives from a deployment: the
//! compiled Worker configurations, the isolate pools they run in, the
//! Durable Object class registry, the service-binding graph, the asset
//! resolvers, and the cron schedule. A node serves exactly one current
//! generation and reaches it through a snapshot, so a request that started
//! on one generation finishes on it even after the node adopts another.
//!
//! Boot and reload construct a generation through the same two functions,
//! [`DeploymentGraph::load`] and `Generation::build`. Nothing else reads a
//! deployment manifest into runtime state. A value a deployment implies
//! therefore has one place it can be computed, and a reload cannot miss what
//! a boot did, because there is no second path for it to miss.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use anyhow::{anyhow, Context};

use crate::assets::AssetResolver;
use crate::bucket::Bucket;
use crate::fleet::{self, LoadedDeployment};
use crate::js::{WorkerConfig, WorkerConfigOptions};

/// Which generation, within this process. Monotonic from one at boot; never
/// reused, never persisted, and never compared across nodes — the fleet-wide
/// identity of a deployment is its version string.
pub type GenerationId = u64;

/// The generation a node boots on. Later generations count up from it.
pub const FIRST_GENERATION: GenerationId = 1;

/// Ask the node to adopt the deployment `deploy/current.json` names now.
pub struct ReloadRequest {
    /// Rebuild even when the pointer names the current deployment, so a
    /// memoised failure is retried and fresh isolates replace the serving
    /// ones. `POST /reload` sets it; a poll tick and a managed nudge do not.
    pub force: bool,
    /// Where to report the outcome. A poll tick has nobody to tell.
    pub reply: Option<tokio::sync::oneshot::Sender<ReloadOutcome>>,
}

/// What one adoption attempt concluded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReloadOutcome {
    /// A new generation serves. Requests admitted from now on use it.
    Adopted {
        generation: GenerationId,
        version: String,
        prefix: String,
    },
    /// The pointer names the deployment already serving and the request did
    /// not force a rebuild.
    Unchanged {
        generation: GenerationId,
        version: String,
    },
    /// The deployment did not build. The current generation is untouched.
    Failed {
        version: String,
        prefix: String,
        error: String,
    },
}

pub type ReloadSender = tokio::sync::mpsc::UnboundedSender<ReloadRequest>;
pub type ReloadReceiver = tokio::sync::mpsc::UnboundedReceiver<ReloadRequest>;

pub fn reload_channel() -> (ReloadSender, ReloadReceiver) {
    tokio::sync::mpsc::unbounded_channel()
}

/// Ask for a poll now, without waiting for the outcome. A managed
/// `deployment_current` message and a successful apply both end here: the
/// pointer is the authority, and the nudge only shortens the wait for it.
pub fn nudge(reload: &ReloadSender) {
    let _ = reload.send(ReloadRequest {
        force: false,
        reply: None,
    });
}

/// How often a node re-reads the pointer without a nudge. For a standalone
/// node this is the deploy latency; for a managed node it is the backstop
/// behind the push. `CELLD_DEPLOY_POLL_S`, default 30.
pub fn poll_interval() -> std::time::Duration {
    let seconds = crate::env_vars::positive_or("CELLD_DEPLOY_POLL_S", 30u64)
        .expect("validated CELLD_DEPLOY_POLL_S");
    std::time::Duration::from_secs(seconds)
}

/// How long a resident cell may keep running a superseded generation after
/// the node adopts a new one, before its swap is forced: its activity
/// cancelled and its regular WebSockets closed with 1012. Zero forces every
/// resident cell at the flip, which is what a Cloudflare deployment does.
/// `CELLD_DEPLOY_MAX_AGE_S`, default 60.
pub fn max_age() -> std::time::Duration {
    // Not `positive_or`: zero is meaningful here.
    let seconds = crate::env_vars::with_default("CELLD_DEPLOY_MAX_AGE_S", 60u64)
        .expect("validated CELLD_DEPLOY_MAX_AGE_S");
    std::time::Duration::from_secs(seconds)
}

/// The scripts one deployment reaches: the primary script plus every
/// service-binding target and queue consumer it declares, transitively.
///
/// [`DeploymentGraph::load`] is the only bucket walk, so the boot path and
/// the reload path cannot resolve a deployment's dependencies differently.
pub struct DeploymentGraph {
    pub primary: LoadedDeployment,
    pub cohosted: Vec<LoadedDeployment>,
}

impl DeploymentGraph {
    /// One script and nothing else: the local-script mode a runtime test
    /// starts from a file, which declares no services and has no bucket to
    /// resolve them from.
    pub fn single(primary: LoadedDeployment) -> Self {
        Self {
            primary,
            cohosted: Vec::new(),
        }
    }

    /// Resolve the fleet-wide pointer and every script it depends on.
    ///
    /// A service binding names a script, and that script's own pointer names
    /// its deployment. A queue dependency names a queue, and the queue's
    /// consumer attachment names an exact deployment. The walk refuses a
    /// script that resolves twice and a queue attached to a deployment other
    /// than the one already loaded, because either would give one script two
    /// bodies in one process.
    pub async fn load(bucket: &Bucket, node: String) -> anyhow::Result<Self> {
        let primary = fleet::load_current_worker(bucket, node.clone()).await?;
        let primary_script = primary.script_name.clone();
        let mut loaded_scripts = BTreeMap::from([(primary_script.clone(), primary.prefix.clone())]);
        let mut loaded_consumers =
            BTreeMap::from([(primary_script.clone(), consumed_queues(&primary.options))]);
        let mut visited_queues = BTreeSet::new();
        let mut dependencies = dependencies_of(&primary);
        let mut cohosted = Vec::new();
        while let Some(dependency) = dependencies.pop_front() {
            let loaded = match dependency {
                Dependency::Service(target) => {
                    if target == primary_script || loaded_scripts.contains_key(&target) {
                        continue;
                    }
                    let loaded = fleet::load_named_worker(bucket, &target, node.clone())
                        .await
                        .with_context(|| format!("load service binding target {target}"))?;
                    if loaded.script_name != target {
                        anyhow::bail!(
                            "service pointer {target} resolved script {}",
                            loaded.script_name
                        );
                    }
                    loaded
                }
                Dependency::Queue(queue) => {
                    if !visited_queues.insert(queue.clone()) {
                        continue;
                    }
                    let declared_by = loaded_consumers
                        .iter()
                        .find_map(|(script, queues)| queues.contains(&queue).then_some(script));
                    let Some(consumer) =
                        fleet::load_queue_consumer_attachment(bucket, &queue).await?
                    else {
                        if let Some(script) = declared_by {
                            anyhow::bail!(
                                "script {script:?} consumes queue {queue:?}, but the queue has no active consumer attachment; re-run `celld deploy`"
                            );
                        }
                        continue;
                    };
                    if let Some(script) = declared_by {
                        anyhow::ensure!(
                            script == &consumer.script_name,
                            "queue {queue:?} is attached to script {:?}, but loaded script {script:?} also consumes it",
                            consumer.script_name
                        );
                    }
                    if let Some(prefix) = loaded_scripts.get(&consumer.script_name) {
                        anyhow::ensure!(
                            prefix == &consumer.prefix,
                            "queue {queue:?} is attached to deployment {} of script {:?}, but deployment {prefix} is already loaded; re-run `celld deploy`",
                            consumer.version,
                            consumer.script_name
                        );
                        anyhow::ensure!(
                            loaded_consumers
                                .get(&consumer.script_name)
                                .is_some_and(|queues| queues.contains(&queue)),
                            "queue {queue:?} is attached to script {:?}, but its loaded deployment does not consume that queue",
                            consumer.script_name
                        );
                        continue;
                    }
                    fleet::load_queue_consumer_worker(bucket, &queue, &consumer, node.clone())
                        .await
                        .with_context(|| format!("load consumer for queue {queue:?}"))?
                }
            };
            let target = loaded.script_name.clone();
            anyhow::ensure!(
                loaded_scripts
                    .insert(target.clone(), loaded.prefix.clone())
                    .is_none(),
                "script {target:?} was loaded twice"
            );
            loaded_consumers.insert(target.clone(), consumed_queues(&loaded.options));
            dependencies.extend(dependencies_of(&loaded));
            // A node runs the schedule of the deployment it was given and of
            // no other. The reserved class is one key, so a second script's
            // cron cell would resolve to the first script's config and run
            // the wrong `scheduled` handler. Dropping the schedule is the
            // safe half of that trade and this says so out loud, because a
            // trigger that never fires and says nothing is the failure the
            // whole feature is built to avoid. Deploy the script as a node's
            // own deployment to run its crons.
            if !loaded.crons.is_empty() {
                tracing::warn!(
                    script = %target,
                    crons = %loaded.crons.join(", "),
                    "a service binding target declares cron triggers; a node fires only its own deployment's schedule, so these never run here"
                );
            }
            cohosted.push(loaded);
        }
        Ok(Self { primary, cohosted })
    }
}

enum Dependency {
    Service(String),
    Queue(String),
}

fn consumed_queues(options: &WorkerConfigOptions) -> BTreeSet<String> {
    options
        .queue_consumers
        .iter()
        .map(|consumer| consumer.queue.clone())
        .collect()
}

fn dependencies_of(loaded: &LoadedDeployment) -> VecDeque<Dependency> {
    let queues = loaded
        .options
        .queue_bindings
        .iter()
        .map(|binding| binding.queue.clone())
        .chain(loaded.options.queue_consumers.iter().flat_map(|consumer| {
            std::iter::once(consumer.queue.clone()).chain(consumer.dead_letter_queue.clone())
        }))
        .collect::<BTreeSet<_>>();
    loaded
        .services
        .iter()
        .map(|(_, script, _)| Dependency::Service(script.clone()))
        .chain(queues.into_iter().map(Dependency::Queue))
        .collect()
}

/// Node-level inputs `Generation::build` needs beside the deployment itself.
pub struct GenerationOptions {
    pub node: String,
    pub region: String,
}

/// A deployment, built and ready to serve.
///
/// The fields are the four maps `RuntimeManager` once held for the life of
/// the process, plus the asset resolvers and the cron schedule that lived on
/// the application handle. They are private and reached through
/// `RuntimeManager`, which hands out this struct only as a snapshot.
pub struct Generation {
    pub(crate) id: GenerationId,
    pub(crate) version: String,
    pub(crate) prefix: String,
    pub(crate) script_name: String,
    pub(crate) stateless: crate::runtime::StatelessRuntime,
    pub(crate) services: HashMap<String, crate::runtime::StatelessRuntime>,
    pub(crate) cell_configs: HashMap<String, Arc<WorkerConfig>>,
    /// The isolates a Worker script's cells live in — the same `Pool` the
    /// stateless path admits into, because an isolate is an isolate. Cells
    /// of one script share them, so cells of one class share module scope
    /// exactly when they are colocated, which is what Durable Objects do.
    pub(crate) cell_isolates: HashMap<String, Arc<crate::pool::Pool>>,
    pub(crate) default_do_class: Option<Arc<str>>,
    pub(crate) assets: HashMap<String, AssetResolver>,
    /// Every container class of every script in the graph.
    pub(crate) containers: Vec<crate::container::ContainerSpec>,
    /// See `Manifest::fence_image`; the first script's, they are all the
    /// same image when one celld deployed them.
    pub(crate) fence_image: Option<String>,
}

impl Generation {
    pub fn id(&self) -> GenerationId {
        self.id
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The primary script: the one ingress serves and whose assets ingress
    /// consults before running the Worker.
    pub fn script_name(&self) -> &str {
        &self.script_name
    }

    /// The asset resolver of the named script, if that script deployed
    /// assets.
    pub fn assets(&self, script: &str) -> Option<&AssetResolver> {
        self.assets.get(script)
    }

    /// The primary script's asset resolver, which ingress consults.
    pub fn ingress_assets(&self) -> Option<&AssetResolver> {
        self.assets.get(&self.script_name)
    }

    pub fn has_cell_classes(&self) -> bool {
        !self.cell_configs.is_empty()
    }

    /// The reserved cell carrying this deployment's cron schedule, or `None`
    /// when the deployment declares no `triggers.crons`. Derived from the
    /// registered class rather than plumbed separately, so it cannot
    /// disagree with what `start_cell` will accept.
    pub fn cron_cell(&self) -> Option<String> {
        self.cell_configs
            .get(celld_logic::cron::RESERVED_CLASS)
            .map(|config| celld_logic::cron::reserved_cell(&config.script_name))
    }

    pub(crate) fn cell_config(&self, class: &str) -> Option<Arc<WorkerConfig>> {
        self.cell_configs.get(class).cloned()
    }

    /// The engine's reserved Durable Object classes this generation
    /// registers: cron, queue, workflow, D1, KV. Their cells hold no
    /// application state worth waiting for, so an adoption moves them at
    /// once — and the cron cell must run the new schedule before the
    /// adoption arms it.
    ///
    /// The cron class is named beside `deploy::is_reserved_class` rather
    /// than added to it. That predicate also decides which classes refuse an
    /// unauthenticated operator route, and the cron cell is not one of them,
    /// so widening it to reach this list would widen that refusal too.
    pub fn reserved_classes(&self) -> Vec<String> {
        self.cell_configs
            .keys()
            .filter(|class| {
                crate::deploy::is_reserved_class(class)
                    || class.as_str() == celld_logic::cron::RESERVED_CLASS
            })
            .cloned()
            .collect()
    }

    pub(crate) fn cell_isolates(&self, script: &str) -> Option<Arc<crate::pool::Pool>> {
        self.cell_isolates.get(script).cloned()
    }

    pub(crate) fn service(&self, script: &str) -> Option<crate::runtime::StatelessRuntime> {
        self.services.get(script).cloned()
    }

    pub(crate) fn default_do_class(&self) -> Option<&str> {
        self.default_do_class.as_deref()
    }

    /// Stop every isolate of this generation from taking new work. Stateless
    /// isolates free as their affiliations drop; cell isolates free as their
    /// cells move to a newer generation.
    pub(crate) fn retire(&self) {
        for service in self.services.values() {
            service.isolates.retire_all();
        }
        for pool in self.cell_isolates.values() {
            pool.retire_all();
        }
    }

    /// Whether every isolate of this generation has been freed, so the
    /// generation itself can be dropped.
    pub(crate) fn is_drained(&self) -> bool {
        self.services
            .values()
            .all(|service| service.isolates.is_drained())
            && self.cell_isolates.values().all(|pool| pool.is_drained())
    }

    /// One maintenance pass over the cell pools: retire and free every empty
    /// isolate. An empty cell heap carries no warm request capacity worth
    /// preserving, unlike a stateless one.
    pub(crate) fn reap_cell_pools(&self) {
        for pool in self.cell_isolates.values() {
            pool.reap_empty();
        }
    }

    /// The isolates this generation holds right now, for `/state`.
    pub(crate) fn isolate_census(&self) -> IsolateCensus {
        IsolateCensus {
            stateless: self.stateless.isolates.census(),
            services: self
                .services
                .iter()
                .map(|(name, service)| (name.clone(), service.isolates.census()))
                .collect(),
            cells: self
                .cell_isolates
                .iter()
                .map(|(script, pool)| (script.clone(), pool.census()))
                .collect(),
        }
    }
}

/// The isolates one generation holds, by the pool they belong to, as
/// `/state` reports them.
#[derive(Debug, serde::Serialize)]
pub struct IsolateCensus {
    /// The pool that serves the primary script's stateless handlers.
    pub stateless: crate::pool::PoolCensus,
    /// The pools of the named services, by service name.
    pub services: BTreeMap<String, crate::pool::PoolCensus>,
    /// The pools the cells live in, by Worker script.
    pub cells: BTreeMap<String, crate::pool::PoolCensus>,
}

/// The generation an isolate was built for, installed as an isolate slot by
/// `Worker::load_config` so a call the isolate makes into the host — a
/// service binding, an assets binding, a queue dispatch — resolves against
/// the graph the caller was built with rather than whichever generation is
/// current when the call lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerationTag(pub GenerationId);
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
        let GenerationOptions { node, region } = options;
        let DeploymentGraph { primary, cohosted } = graph;
        crate::runtime::init_engine();

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
                let registration = crate::js::QueueConsumerRegistration {
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
        let primary_script = primary.script_name.clone();
        let version = primary.version.clone();
        let prefix = primary.prefix.clone();
        let mut all_containers = Vec::new();
        let mut fence_image = None;
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
        let user_classes: Vec<&String> = primary
            .options
            .do_classes
            .iter()
            .filter(|class| !crate::deploy::is_reserved_class(class))
            .collect();
        let default_do_class =
            (user_classes.len() == 1).then(|| Arc::from(user_classes[0].as_str()));
        // Two scripts exporting one class name is genuinely ambiguous and is
        // refused — but a *reserved* class is not an export, and the two kinds
        // behave differently:
        //
        // `__D1Database` is deliberately shared. Its namespace is fleet-wide so
        // that several Workers can bind one database and a rename cannot rename
        // it, so two scripts declaring `d1_databases` address the same cells on
        // purpose. A D1 cell runs SQL and reads no user code, binding or var,
        // so whichever config serves it is immaterial and the first wins.
        // Refusing the second made the node exit instead of start.
        //
        // A workflow class is script-scoped (`deploy::workflow_class`), so two
        // scripts produce two distinct names and never reach this branch. If
        // one ever did, it would be a real collision and must still be refused.
        let mut cell_configs: HashMap<String, Arc<WorkerConfig>> = HashMap::new();
        let mut service_pools = HashMap::new();
        let mut assets = HashMap::new();
        // Primary first: shared reserved classes retain the first script's
        // config. Only this script owns ingress and the node's cron schedule;
        // bindings and runtime construction follow the same path for all scripts.
        for (index, deployment) in std::iter::once(primary).chain(cohosted).enumerate() {
            let is_primary = index == 0;
            let crate::fleet::LoadedDeployment {
                options,
                script_name: script,
                asset_binding,
                loader_bindings,
                assets: resolver,
                services,
                crons,
                containers,
                fence_image: fence,
                ..
            } = deployment;
            all_containers.extend(containers.iter().cloned());
            fence_image = fence_image.or(fence);
            if let Some(resolver) = resolver {
                assets.insert(script.clone(), resolver);
            }
            let classes = options.do_classes.clone();
            let config = Arc::new(
                WorkerConfig::new(options)
                    .with_services(services)
                    .with_asset_binding(asset_binding)
                    .with_loaders(loader_bindings)
                    .with_queue_consumers(queue_catalog.clone())
                    .with_crons(if is_primary { crons } else { Vec::new() })
                    .with_containers(containers)
                    .with_generation(id),
            );
            let pool = crate::runtime::StatelessRuntime::start(
                config.clone(),
                node.clone(),
                region.clone(),
            )?;
            if service_pools.insert(script.clone(), pool).is_some() {
                return Err(anyhow!("duplicate co-hosted Worker script {script}"));
            }
            for class in classes {
                register_cell_class(&mut cell_configs, class, config.clone(), &|class| {
                    if is_primary {
                        anyhow!("duplicate Durable Object class {class}")
                    } else {
                        anyhow!(
                            "Durable Object class {class} is exported by more than one co-hosted script"
                        )
                    }
                })?;
            }
            // Cron is runtime-supplied, not a manifest class. Its alarm calls
            // the primary script's scheduled handler with that script's bindings.
            if !config.crons.is_empty() {
                cell_configs.insert(celld_logic::cron::RESERVED_CLASS.to_string(), config);
            }
        }
        let stateless = service_pools[&primary_script].clone();
        let cell_isolates: HashMap<String, Arc<crate::pool::Pool>> = cell_configs
            .values()
            .map(|config| {
                (
                    config.script_name.clone(),
                    crate::runtime::cell_pool(config.clone()),
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
            containers: all_containers,
            fence_image,
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
