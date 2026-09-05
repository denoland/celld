// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Deployment is an operator control-plane path outside the Actor execution
// domain.
#![allow(clippy::disallowed_methods)]
// Deploy narrates progress; every line of it is for a person.

//! `celld deploy` — build a Wrangler project and write it to the fleet bucket.
//!
//! Bundling is esbuild's job; this module does config, identity, and durable
//! bucket publication. Nothing here shells out to wrangler or speaks a
//! Cloudflare-shaped API. Config keys are an allowlist: anything we do not
//! model is refused, never silently dropped.
use crate::bucket::Bucket;
use crate::note;
use crate::protocol::{
    asset_blob_key, AssetConfig, AssetEntry, AssetIndex, AssetManifestRef, DeployPointer, Manifest,
    ModuleKind, ModuleRef, QueueConsumerAttachment, QueueConsumerConfig, QueueConsumerDeployment,
    Rollout, RunWorkerFirst, FEATURE_ASSETS_V1, FEATURE_CRON_V1, FEATURE_D1_V1, FEATURE_KV_V1,
    FEATURE_QUEUES_V1, FEATURE_R2_V1, FEATURE_SQLITE_VEC_V1, FEATURE_WASM_V1, FEATURE_WORKFLOWS_V1,
    QUEUE_CONSUMER_ATTACHMENT_SCHEMA_VERSION,
};
use anyhow::{anyhow, bail, Context};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::{stream, StreamExt, TryStreamExt};
use serde_json::{json, Map, Value};
use sha2::Digest;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

/// Config keys we understand. Anything else is an error: refusing is
/// compat-safe, guessing produces confusing activation failures later.
const SUPPORTED_KEYS: &[&str] = &[
    "$schema",
    "name",
    "main",
    "compatibility_date",
    "compatibility_flags",
    "durable_objects",
    "migrations",
    "assets",
    "services",
    "triggers",
    "vars",
    "d1_databases",
    "kv_namespaces",
    "queues",
    "workflows",
    "r2_buckets",
    "no_bundle",
];

/// The Durable Object class every D1 database runs as. It is supplied by the
/// runtime, not by the worker, so a config naming it in `durable_objects` or
/// `migrations` is refused: the harness registers this class in every isolate,
/// and a user binding onto it would silently reach a D1 database cell instead
/// of the user's class. A bare module export of this name is not refused —
/// the loader skips it rather than let it shadow the built-in.
pub const D1_CLASS: &str = "__D1Database";

/// The Durable Object class every workflow instance runs as. Like `D1_CLASS`,
/// it is supplied by the runtime and refused as a user class name.
pub const WORKFLOW_CLASS: &str = "__Workflow";

/// The Durable Object class every KV namespace runs as.
///
/// Fleet-wide, like `D1_CLASS` and unlike `workflow_class`: a namespace is a
/// resource several Workers bind, so two scripts naming one namespace mean to
/// reach one set of cells.
pub const KV_CLASS: &str = celld_logic::kv::RESERVED_CLASS;

/// The Durable Object class every Queue broker runs as.
pub const QUEUE_CLASS: &str = celld_logic::queue::RESERVED_CLASS;

/// Every runtime-supplied Durable Object class.
pub const RESERVED_CLASSES: &[&str] = &[D1_CLASS, WORKFLOW_CLASS, KV_CLASS, QUEUE_CLASS];

/// The Durable Object class a script's workflow instances run as.
///
/// Script-scoped, because a workflow instance already is. Its namespace key is
/// `cells:v1:<len>:<script>:__Workflow`, so two co-hosted scripts address
/// different cells — but both used to register the *same* class name, and a
/// deployment's class registry is one flat map, so the second script collided
/// with the first and the node refused to start at all. The name now says what
/// the namespace key always said.
///
/// D1 deliberately does not get this treatment. Its namespace is fleet-wide so
/// that several Workers can bind one database and a Worker rename cannot rename
/// it, which means a D1 cell genuinely is shared and one entry is correct.
///
/// `.` is the separator for three reasons, and all three are constraints
/// rather than taste. A JavaScript class name cannot contain one, so no user
/// export can collide. It is not `:`, which is what the cell-scope parser
/// splits a class from an id on. And it is inside
/// `celld_logic::cell::valid_cell_scope`'s charset, which is a security fence
/// on a name that becomes a path component and an object-store key — `@` is
/// not, and a scope carrying one is refused before it reaches storage. Cron
/// picked `.` for the first of those reasons already.
pub fn workflow_class(script_name: &str) -> String {
    format!("{WORKFLOW_CLASS}.{script_name}")
}

/// Whether a reserved class names cells that several scripts share on purpose.
///
/// Only D1. Its namespace key is fleet-wide precisely so that a database
/// outlives the name of any one Worker that binds it, so two scripts declaring
/// `d1_databases` mean to reach the same cells, and one registry entry serving
/// both is correct rather than merely tolerated. A workflow class carries its
/// script, so it is never shared and a collision there is a real one.
pub fn is_shared_reserved_class(class: &str) -> bool {
    class == D1_CLASS || class == KV_CLASS || class == QUEUE_CLASS
}

/// Whether a class name is a script-scoped workflow class.
///
/// One reading of the shape, because three call sites wanted it and three
/// copies of `strip_prefix(WORKFLOW_CLASS)` is how the separator comes to mean
/// two things.
pub fn is_workflow_class(class: &str) -> bool {
    class
        .strip_prefix(WORKFLOW_CLASS)
        .is_some_and(|rest| rest.starts_with('.'))
}

pub fn is_reserved_class(class: &str) -> bool {
    RESERVED_CLASSES.contains(&class) || is_workflow_class(class)
}

/// The reserved class a cell scope names, or `None` for an ordinary Durable
/// Object.
///
/// A scope is `<class>:<instance>` and the instance may itself contain a `:`,
/// so the class is everything before the first one — the same split
/// `Runtime::start_cell` makes when it resolves a cell to its Worker config.
pub fn reserved_class_of(scope: &str) -> Option<&str> {
    let (class, _) = scope.split_once(':')?;
    is_reserved_class(class).then_some(class)
}

/// Whether a cell scope names any runtime-supplied class.
///
/// This is what the unauthenticated `/do/` route refuses, and it is deliberately
/// one question rather than one question per class. `/do/` used to ask
/// one predicate per class, so every reserved class added a
/// third, a fourth, and a place to forget one — and a forgotten refusal is not
/// a missing feature, it is an unauthenticated route onto a cell whose whole
/// surface is an operator protocol. Adding a class to `RESERVED_CLASSES` now
/// closes that door with no new code.
pub fn is_reserved_scope(scope: &str) -> bool {
    reserved_class_of(scope).is_some()
}

/// What to tell an operator who reached a reserved class on the wrong route.
///
/// A hint, never a gate. The refusal is `is_reserved_scope`, which cannot be
/// forgotten; this only decides how helpful the message is, so a class with no
/// entry here is still refused and simply gets the general answer.
pub fn operator_hint(class: &str) -> &'static str {
    if class == D1_CLASS {
        "use `celld d1`"
    } else if class == KV_CLASS {
        "use `celld kv`"
    } else if class == QUEUE_CLASS {
        "use `celld queue`"
    } else if is_workflow_class(class) {
        "use the workflow binding"
    } else {
        "reach it through its binding"
    }
}

const MAX_ASSET_FILES: usize = 20_000;
const MAX_ASSET_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_ASSET_FILE_BYTES: u64 = 25 * 1024 * 1024;
const MAX_ASSET_DIRECTIVE_BYTES: u64 = 100 * 1024;
const ASSET_UPLOAD_CONCURRENCY: usize = 16;

/// Wrangler's Worker-name bound. The charset below is ASCII, so the byte and
/// character counts are identical. This also keeps a generated Workflow class
/// far below the cell-scope limit.
const MAX_SCRIPT_NAME_BYTES: usize = 63;
const _: () =
    assert!(WORKFLOW_CLASS.len() + 1 + MAX_SCRIPT_NAME_BYTES <= celld_logic::cell::MAX_CELL_SCOPE);

pub struct Options {
    pub config: Option<PathBuf>,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub dry_run: bool,
    pub json: bool,
}

pub fn print_help() {
    let text = "celld deploy — build a Worker with esbuild and write it to the fleet bucket\n\n\
USAGE:\n  celld deploy [PROJECT] --bucket [s3://|gs://|az://]NAME[/PREFIX] [OPTIONS]\n\n\
PROJECT is a directory or a Wrangler config; it defaults to the working\n\
directory, where celld looks for wrangler.jsonc or wrangler.json.\n\n\
OPTIONS:\n  --config PATH          Same as passing PROJECT positionally\n  --bucket [s3://|gs://|az://]NAME[/PREFIX]\n                         Fleet bucket and prefix; defaults to CELLD_BUCKET.\n                         gs:// selects a Google Cloud Storage bucket, az://\n                         an Azure Blob Storage container with its account in\n                         AZURE_STORAGE_ACCOUNT_NAME; celld then rejects\n                         --endpoint and ignores --region\n  --endpoint URL         S3-compatible endpoint; defaults to S3_ENDPOINT\n  --region REGION        Storage region; defaults to AWS_REGION\n  --dry-run              Bundle and print the version without writing\n  --json                 Print the deployment as one JSON object\n  -h, --help             Show this help\n\n\
Credentials come from the standard AWS credential chain, from Google\n\
Application Default Credentials for a gs:// bucket, or from an Azure storage\n\
account key, managed identity, or workload identity for an az:// bucket.\n\n\
Worker projects require `esbuild` on PATH; asset-only projects do not. Static\n\
assets, service bindings, and string vars are supported. Routes are not; use\n\
Wrangler for route configuration.\n\
A running node polls the deployment pointer and adopts the new version in\n\
place; nothing restarts."
    ;
    let text = format!(
        "{text}\nThe config `name` uses lowercase ASCII letters, digits, and internal hyphens. \
         It is at most {MAX_SCRIPT_NAME_BYTES} bytes."
    );
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text).help(&text);
}

pub fn options_from_arguments(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<Option<Options>> {
    let mut options = Options {
        config: None,
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        json: false,
    };
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--dry-run" => options.dry_run = true,
            "--json" => options.json = true,
            "--config" => {
                options.config = Some(PathBuf::from(
                    arguments.next().context("--config requires a value")?,
                ));
            }
            "--bucket" => {
                let value = arguments.next().context("--bucket requires a value")?;
                options.bucket = Some(value);
            }
            "--endpoint" => {
                options.endpoint = Some(arguments.next().context("--endpoint requires a value")?);
            }
            "--region" => {
                options.region = Some(arguments.next().context("--region requires a value")?);
            }
            other if other.starts_with('-') => {
                bail!("unknown argument for `celld deploy`: {other}")
            }
            other if options.config.is_none() => options.config = Some(PathBuf::from(other)),
            other => bail!("`celld deploy` takes one project path, and already has one: {other}"),
        }
    }
    Ok(Some(options))
}

/// What a config resolves to once the allowlist has been applied.
struct Project {
    script_name: String,
    /// `no_bundle: true` uploads the entry file as written instead of running
    /// esbuild. A Vite build has already bundled the Worker, and bundling it
    /// twice is what breaks that output.
    no_bundle: bool,
    /// Entry relative to the project root. esbuild stamps this path into the
    /// bundle, so it must not depend on the working directory celld was
    /// invoked from — identical source would otherwise hash two ways.
    entry: Option<String>,
    assets: Option<ProjectAssets>,
    metadata: Value,
    do_classes: Vec<String>,
    sqlite_classes: Vec<String>,
    crons: Vec<String>,
    has_workflows: bool,
    has_kv: bool,
    queue_consumers: Vec<QueueConsumerConfig>,
    has_queues: bool,
    has_r2: bool,
}

struct ProjectAssets {
    directory: PathBuf,
    config: AssetConfig,
    raw_metadata: Value,
}

pub struct BuiltAssets {
    pub index: Vec<u8>,
    pub blobs: BTreeMap<String, Vec<u8>>,
    pub file_count: u32,
    pub total_bytes: u64,
}

/// The built deployment, before anything is written.
pub struct Built {
    pub script_name: String,
    pub version: String,
    pub prefix: String,
    pub manifest: Manifest,
    pub modules: Vec<(String, Vec<u8>)>,
    pub assets: Option<BuiltAssets>,
    pub bundled_in: Duration,
}

impl Built {
    pub fn bytes(&self) -> usize {
        self.modules.iter().map(|(_, body)| body.len()).sum()
    }

    /// What the deployment weighs, the bindings it will have, and nothing we
    /// cannot stand behind: no URL, because celld routes nothing, and no
    /// startup time, because deploying does not start an isolate.
    pub fn report(&self) {
        note!(" celld {}", env!("CARGO_PKG_VERSION"));
        note!("{}", "─".repeat(47));
        note!(
            "Total Upload: {} / gzip: {}",
            kib(self.bytes()),
            kib(gzipped(&self.modules)),
        );
        if let Some(assets) = &self.assets {
            note!(
                "Static Assets: {} files / {} ({} unique bodies)",
                assets.file_count,
                kib(assets.total_bytes as usize),
                assets.blobs.len(),
            );
        }
        let bindings = self.bindings();
        if bindings.is_empty() {
            note!("Your Worker has no bindings.");
        } else {
            let width = bindings
                .iter()
                .map(|(binding, _)| binding.len())
                .chain(std::iter::once("Binding".len()))
                .max()
                .unwrap_or_default()
                + 6;
            note!("Your Worker has access to the following bindings:");
            note!("{:width$}Resource", "Binding");
            for (binding, resource) in bindings {
                note!("{binding:width$}{resource}");
            }
        }
        note!(
            "Bundled {} ({})",
            self.script_name,
            seconds(self.bundled_in)
        );
    }

    /// `env.NAME (Class)` against the resource it resolves to, the way
    /// Wrangler renders it.
    fn bindings(&self) -> Vec<(String, String)> {
        self.manifest
            .raw_metadata
            .get("bindings")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .filter_map(|binding| {
                let name = binding.get("name").and_then(Value::as_str)?;
                match binding.get("type").and_then(Value::as_str) {
                    Some("assets") => {
                        Some((format!("env.{name} (Assets)"), "Static Assets".to_string()))
                    }
                    Some("durable_object_namespace") => {
                        let class = binding.get("class_name").and_then(Value::as_str)?;
                        let sqlite = self.manifest.sqlite_classes.iter().any(|c| c == class);
                        Some((
                            format!("env.{name} ({class})"),
                            match sqlite {
                                true => "Durable Object (SQLite)".to_string(),
                                false => "Durable Object".to_string(),
                            },
                        ))
                    }
                    Some("service") => {
                        let service = binding.get("service").and_then(Value::as_str)?;
                        let target = binding
                            .get("entrypoint")
                            .and_then(Value::as_str)
                            .map_or_else(
                                || service.to_string(),
                                |entrypoint| format!("{service}#{entrypoint}"),
                            );
                        Some((format!("env.{name} (Service)"), target))
                    }
                    Some("d1") => {
                        let database = binding.get("database_name").and_then(Value::as_str)?;
                        Some((format!("env.{name} (D1)"), database.to_string()))
                    }
                    Some("workflow") => {
                        let workflow = binding.get("workflow_name").and_then(Value::as_str)?;
                        Some((format!("env.{name} (Workflow)"), workflow.to_string()))
                    }
                    Some("kv") => {
                        let id = binding.get("id").and_then(Value::as_str)?;
                        Some((format!("env.{name} (KV)"), id.to_string()))
                    }
                    Some("queue") => {
                        let queue = binding.get("queue").and_then(Value::as_str)?;
                        Some((format!("env.{name} (Queue)"), queue.to_string()))
                    }
                    Some("r2_bucket") => {
                        let bucket = binding.get("bucket_name").and_then(Value::as_str)?;
                        Some((format!("env.{name} (R2)"), bucket.to_string()))
                    }
                    Some("plain_text") => Some((
                        format!("env.{name} (Text)"),
                        "Environment Variable".to_string(),
                    )),
                    _ => None,
                }
            })
            .collect()
    }
}

fn kib(bytes: usize) -> String {
    format!("{:.2} KiB", bytes as f64 / 1024.0)
}

fn seconds(elapsed: Duration) -> String {
    format!("{:.2} sec", elapsed.as_secs_f64())
}

fn gzipped(modules: &[(String, Vec<u8>)]) -> usize {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for (_, body) in modules {
        encoder.write_all(body).ok();
    }
    encoder.finish().map(|out| out.len()).unwrap_or_default()
}

pub fn build(options: &Options) -> anyhow::Result<Built> {
    let config_path = resolve_config(options.config.clone())?;
    let root = config_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let project = read_project(&config_path, &root)?;
    let started = Instant::now();
    let built_assets = project.assets.as_ref().map(build_assets).transpose()?;
    let bundle = project
        .entry
        .as_deref()
        .map(|entry| {
            if project.no_bundle {
                // Already bundled by the caller's toolchain. Read it as it is;
                // running esbuild over a Vite build is what corrupts it. A
                // pre-bundled entry carries no sibling wasm: esbuild's copy
                // loader is what would have produced them.
                let path = root.join(entry);
                std::fs::read(&path)
                    .with_context(|| format!("read entry point {}", path.display()))
                    .map(|bundle| BundleOutput {
                        bundle,
                        wasm: Vec::new(),
                    })
            } else {
                run_esbuild(&root, entry)
            }
        })
        .transpose()?;
    let bundled_in = started.elapsed();

    // esbuild emits one JS module plus a copy of every wasm file the bundle
    // imports; the copies ship as sibling modules.
    let module_name = "index.js".to_string();
    let (mut modules, wasm_modules) = match bundle {
        Some(output) => (vec![(module_name.clone(), output.bundle)], output.wasm),
        None => (Vec::new(), Vec::new()),
    };
    let wasm_names: BTreeSet<String> = wasm_modules.iter().map(|(name, _)| name.clone()).collect();
    modules.extend(wasm_modules);
    // Identity is over the exact metadata bytes the manifest retains, so the
    // serialization happens once and is reused for both.
    let metadata_json = serde_json::to_vec(&project.metadata)?;
    let version = crate::protocol::deployment_version(
        &modules,
        &metadata_json,
        built_assets.as_ref().map(|assets| assets.index.as_slice()),
    );
    let prefix = format!("deploy/{}/{}", project.script_name, version);
    let asset_reference = built_assets.as_ref().map(|assets| AssetManifestRef {
        index: "assets.json".to_string(),
        sha256: format!("{:x}", Sha256::digest(&assets.index)),
        file_count: assets.file_count,
        total_bytes: assets.total_bytes,
    });
    let sqlite_vec = project
        .metadata
        .get("compatibility_flags")
        .and_then(Value::as_array)
        .is_some_and(|flags| flags.iter().any(|flag| flag.as_str() == Some("sqlite_vec")));
    let uses_d1 = project.do_classes.iter().any(|class| class == D1_CLASS);
    let manifest = Manifest {
        schema_version: if asset_reference.is_some() { 2 } else { 1 },
        version: version.clone(),
        script_name: project.script_name.clone(),
        main_module: project.entry.as_ref().map(|_| module_name.clone()),
        do_classes: project.do_classes,
        sqlite_classes: project.sqlite_classes,
        modules: modules
            .iter()
            .map(|(name, bytes)| ModuleRef {
                name: name.clone(),
                bytes: bytes.len(),
                sha256: format!("{:x}", Sha256::digest(bytes)),
                kind: wasm_names.contains(name).then_some(ModuleKind::Wasm),
            })
            .collect(),
        assets: asset_reference,
        crons: project.crons.clone(),
        queue_consumers: project.queue_consumers,
        // Each capability the manifest depends on is named here, so a node
        // that predates it rejects the deployment up front instead of
        // partially deserializing the manifest and failing at worker load.
        required_features: {
            let mut features = Vec::new();
            if built_assets.is_some() {
                features.push(FEATURE_ASSETS_V1.to_string());
            }
            if !project.crons.is_empty() {
                features.push(FEATURE_CRON_V1.to_string());
            }
            if uses_d1 {
                features.push(FEATURE_D1_V1.to_string());
            }
            if project.has_workflows {
                features.push(FEATURE_WORKFLOWS_V1.to_string());
            }
            if project.has_kv {
                features.push(FEATURE_KV_V1.to_string());
            }
            if project.has_queues {
                features.push(FEATURE_QUEUES_V1.to_string());
            }
            if project.has_r2 {
                features.push(FEATURE_R2_V1.to_string());
            }
            if sqlite_vec {
                features.push(FEATURE_SQLITE_VEC_V1.to_string());
            }
            if !wasm_names.is_empty() {
                features.push(FEATURE_WASM_V1.to_string());
            }
            features
        },
        raw_metadata: project.metadata,
    };
    Ok(Built {
        script_name: project.script_name,
        version,
        prefix,
        manifest,
        modules,
        assets: built_assets,
        bundled_in,
    })
}

pub async fn write(bucket: &Bucket, built: &Built) -> anyhow::Result<()> {
    // Read every attachment before uploading immutable deployment objects.
    // A competing consumer is a deploy refusal, so it must not leave a new
    // version in the bucket that an operator can mistake for a published one.
    let queue_attachments = prepare_queue_attachments(bucket, &built.manifest).await?;

    // Asset bodies are fleet-wide and content-addressed. Finish every body
    // before publishing the deployment-local index or manifest so a reader
    // can never observe a pointer whose assets are incomplete.
    if let Some(assets) = &built.assets {
        stream::iter(&assets.blobs)
            .map(|(sha256, body)| ensure_asset_blob(bucket, sha256, body))
            .buffer_unordered(ASSET_UPLOAD_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
    }
    for (name, bytes) in &built.modules {
        bucket
            .put(&format!("{}/{name}", built.prefix), bytes.clone())
            .await?;
    }
    if let Some(assets) = &built.assets {
        bucket
            .put(
                &format!("{}/assets.json", built.prefix),
                assets.index.clone(),
            )
            .await?;
    }
    bucket
        .put(
            &format!("{}/manifest.json", built.prefix),
            serde_json::to_vec_pretty(&built.manifest)?,
        )
        .await?;

    // An attachment names this exact immutable prefix. Publish it only after
    // the prefix is complete, so a node can never resolve a consumer to a
    // half-uploaded deployment. The named and fleet pointers can move later
    // without tearing this queue-to-consumer relationship.
    publish_queue_attachments(bucket, &built.manifest, &built.prefix, queue_attachments).await?;

    let pointer = DeployPointer {
        script_name: Some(built.script_name.clone()),
        version: built.version.clone(),
        prefix: built.prefix.clone(),
        rollout: Rollout { percent: 100 },
    };
    let encoded = serde_json::to_vec_pretty(&pointer)?;
    // The named pointer resolves service-binding components; the fleet-wide
    // one is the sole application selector, so it moves last. A concurrent
    // deploy must produce a loser, not a lost write.
    put_pointer(
        bucket,
        &format!("deploy/{}/current.json", built.script_name),
        encoded.clone(),
    )
    .await?;
    put_pointer(bucket, "deploy/current.json", encoded).await?;
    Ok(())
}

pub(crate) struct QueueAttachmentState {
    queue: String,
    key: String,
    current: Option<QueueConsumerAttachment>,
    token: Option<String>,
    desired: bool,
}

pub(crate) async fn prepare_queue_attachments(
    bucket: &Bucket,
    manifest: &Manifest,
) -> anyhow::Result<Vec<QueueAttachmentState>> {
    let desired = manifest
        .queue_consumers
        .iter()
        .map(|consumer| consumer.queue.clone())
        .collect::<BTreeSet<_>>();
    let previous = current_queue_consumers(bucket, &manifest.script_name).await?;
    let queues = desired.union(&previous).cloned().collect::<Vec<_>>();
    let mut states = Vec::with_capacity(queues.len());

    for queue in queues {
        let key = crate::protocol::queue_consumer_attachment_key(&queue)
            .expect("deploy validated the queue name");
        let (current, token) = match bucket.get(&key).await? {
            Some((body, token)) => {
                let attachment: QueueConsumerAttachment = serde_json::from_slice(&body)
                    .with_context(|| format!("decode queue consumer attachment {key}"))?;
                crate::protocol::validate_queue_consumer_attachment(&queue, &attachment)?;
                (Some(attachment), Some(token))
            }
            None => (None, None),
        };
        let is_desired = desired.contains(&queue);
        if is_desired {
            if let Some(owner) = current
                .as_ref()
                .and_then(|attachment| attachment.consumer.as_ref())
                .filter(|consumer| consumer.script_name != manifest.script_name)
            {
                bail!(
                    "queue {:?} already has consumer script {:?}; remove that consumer before deploying {}",
                    queue,
                    owner.script_name,
                    manifest.script_name
                );
            }
        }
        states.push(QueueAttachmentState {
            queue,
            key,
            current,
            token,
            desired: is_desired,
        });
    }
    Ok(states)
}

async fn current_queue_consumers(
    bucket: &Bucket,
    script_name: &str,
) -> anyhow::Result<BTreeSet<String>> {
    let pointer_key = format!("deploy/{script_name}/current.json");
    let Some((pointer_body, _)) = bucket.get(&pointer_key).await? else {
        return Ok(BTreeSet::new());
    };
    let pointer: DeployPointer =
        serde_json::from_slice(&pointer_body).with_context(|| format!("decode {pointer_key}"))?;
    anyhow::ensure!(
        pointer.script_name.as_deref() == Some(script_name),
        "named deployment pointer {pointer_key} does not identify script {script_name:?}"
    );
    let manifest_key = format!("{}/manifest.json", pointer.prefix);
    let (manifest_body, _) = bucket
        .get(&manifest_key)
        .await?
        .with_context(|| format!("read {manifest_key}"))?;
    let manifest: Manifest =
        serde_json::from_slice(&manifest_body).with_context(|| format!("decode {manifest_key}"))?;
    anyhow::ensure!(
        manifest.script_name == script_name,
        "named deployment pointer {pointer_key} resolved script {:?}",
        manifest.script_name
    );
    Ok(manifest
        .queue_consumers
        .into_iter()
        .map(|consumer| consumer.queue)
        .collect())
}

pub(crate) async fn publish_queue_attachments(
    bucket: &Bucket,
    manifest: &Manifest,
    prefix: &str,
    states: Vec<QueueAttachmentState>,
) -> anyhow::Result<()> {
    let consumer = QueueConsumerDeployment {
        script_name: manifest.script_name.clone(),
        version: manifest.version.clone(),
        prefix: prefix.to_string(),
    };
    for state in states {
        let next_consumer = if state.desired {
            Some(consumer.clone())
        } else if state
            .current
            .as_ref()
            .and_then(|attachment| attachment.consumer.as_ref())
            .is_some_and(|owner| owner.script_name == manifest.script_name)
        {
            None
        } else {
            continue;
        };
        let next = QueueConsumerAttachment {
            schema_version: QUEUE_CONSUMER_ATTACHMENT_SCHEMA_VERSION,
            queue: state.queue,
            consumer: next_consumer,
        };
        if state.current.as_ref() == Some(&next) {
            continue;
        }
        let body = serde_json::to_vec_pretty(&next)?;
        match bucket
            .put_cas(&state.key, body, state.token.as_deref())
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                bail!(
                    "queue consumer attachment {} lost a race; another deploy may have landed first; re-run `celld deploy`",
                    state.key
                )
            }
            Err(error) => {
                return Err(error.context(
                    "a queue consumer attachment write may have committed; re-run `celld deploy`",
                ));
            }
        }
    }
    Ok(())
}

async fn ensure_asset_blob(bucket: &Bucket, sha256: &str, body: &[u8]) -> anyhow::Result<()> {
    let key = asset_blob_key(sha256).expect("built asset digest is valid");
    if let Ok(Some((size, meta))) = bucket.head_with_meta(&key, "sha256").await {
        if size == body.len() as u64 && meta.as_deref() == Some(sha256) {
            return Ok(());
        }
    }
    bucket
        .put_with_meta(&key, body.to_vec(), &[("sha256", sha256)])
        .await
}

/// Compare-and-swap on a pointer: create it if absent, otherwise replace
/// exactly the value we read.
async fn put_pointer(bucket: &Bucket, key: &str, body: Vec<u8>) -> anyhow::Result<()> {
    let etag = bucket.head(key).await.ok().flatten().map(|(_, etag)| etag);
    match bucket.put_cas(key, body, etag.as_deref()).await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(anyhow!(
            "write {}://{}/{key} lost a race\n\
             Another deploy may have landed first; re-run `celld deploy`.",
            bucket.scheme(),
            bucket.name
        )),
        Err(error) => {
            Err(error.context("Another deploy may have landed first; re-run `celld deploy`."))
        }
    }
}

/// What `celld d1` needs out of a project: the declared databases, so a name
/// the project never declared fails here instead of creating an empty one.
pub struct D1Project {
    /// One entry per declaration, in config order.
    pub databases: Vec<D1Declaration>,
}

pub struct D1Declaration {
    pub database_name: String,
    /// Cloudflare's stable resource ID when present, otherwise the database
    /// name for a celld-only project.
    pub database_identity: String,
    /// Wrangler scopes `migrations_dir` to the binding, so this is per
    /// database and never shared. A single shared directory would apply one
    /// database's migrations to another.
    pub migrations_dir: PathBuf,
    /// The bookkeeping table, per binding as on wrangler
    /// (`migrations_table`). Hard-coding `d1_migrations` here made celld
    /// read an empty table on a project that had renamed it, and every
    /// already-applied migration re-ran.
    pub migrations_table: String,
}

/// Read a project's D1 declarations. This reads the same config `build` reads,
/// but it does not bundle: `celld d1` acts on a database that is already
/// deployed, and a project that fails to build must still be migratable.
pub fn read_d1_project(given: Option<PathBuf>) -> anyhow::Result<D1Project> {
    let path = resolve_config(given)?;
    let root = path.parent().unwrap_or(Path::new(".")).to_path_buf();
    let source =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let config: Value = serde_json::from_str(&strip_jsonc(&source))
        .with_context(|| format!("parse {}", path.display()))?;
    let object = config
        .as_object()
        .ok_or_else(|| anyhow!("{} is not a JSON object", path.display()))?;
    object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{} has no `name`", path.display()))?;
    let mut databases = Vec::new();
    if let Some(Value::Array(entries)) = object.get("d1_databases") {
        for entry in entries {
            let Some(name) = entry.get("database_name").and_then(Value::as_str) else {
                continue;
            };
            // Wrangler discovers migrations through more knobs than the two
            // celld reads. An unread knob must be a refusal, not a silent
            // default: honoring half of a project's migration config applies
            // the wrong files in the wrong order.
            if entry.get("migrations_pattern").is_some() {
                bail!(
                    "d1 database {name:?} sets `migrations_pattern`, which celld \
                     does not support; celld reads `*.sql` from `migrations_dir`"
                );
            }
            let migrations_table = match entry.get("migrations_table") {
                None => "d1_migrations".to_string(),
                Some(Value::String(table)) => table.clone(),
                Some(_) => bail!("d1 database {name:?} has a non-string `migrations_table`"),
            };
            // The table name is joined into SQL as an identifier, both here
            // and in the cell, so anything but a plain identifier is refused
            // before it can reach either.
            let mut characters = migrations_table.chars();
            let plain = characters
                .next()
                .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
                && characters.all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !plain {
                bail!(
                    "d1 database {name:?} has a `migrations_table` that is not a \
                     plain identifier: {migrations_table:?}"
                );
            }
            let database_identity = match entry.get("database_id") {
                None => name.to_string(),
                Some(Value::String(identity)) if !identity.is_empty() => identity.clone(),
                Some(_) => bail!("d1 database {name:?} has an invalid `database_id`"),
            };
            let migrations_dir = match entry.get("migrations_dir") {
                None => root.join("migrations"),
                Some(Value::String(directory)) => {
                    root.join(project_relative_path(directory, "migrations_dir")?)
                }
                Some(_) => bail!("d1 database {name:?} has a non-string `migrations_dir`"),
            };
            databases.push(D1Declaration {
                database_name: name.to_string(),
                database_identity,
                migrations_dir,
                migrations_table,
            });
        }
    }
    let mut unique = Vec::<D1Declaration>::new();
    for declaration in databases {
        if let Some(previous) = unique
            .iter()
            .find(|candidate| candidate.database_name == declaration.database_name)
        {
            if previous.database_identity != declaration.database_identity
                || previous.migrations_dir != declaration.migrations_dir
                || previous.migrations_table != declaration.migrations_table
            {
                bail!(
                    "d1 database {:?} has ambiguous aliases with different database_id, \
                     migrations_dir, or migrations_table values",
                    declaration.database_name
                );
            }
            continue;
        }
        if let Some(previous) = unique
            .iter()
            .find(|candidate| candidate.database_identity == declaration.database_identity)
        {
            if previous.migrations_dir != declaration.migrations_dir
                || previous.migrations_table != declaration.migrations_table
            {
                bail!(
                    "d1 database identity {:?} has ambiguous aliases {:?} and {:?} with \
                     different migrations_dir or migrations_table values",
                    declaration.database_identity,
                    previous.database_name,
                    declaration.database_name
                );
            }
        }
        unique.push(declaration);
    }
    Ok(D1Project { databases: unique })
}

/// A path may name the config itself or the directory holding it; with no
/// path at all, the working directory.
pub(crate) fn resolve_config(given: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    let directory = match given {
        Some(path) if path.is_dir() => path,
        Some(path) if path.exists() => return Ok(path),
        Some(path) => bail!(
            "no Wrangler config or project directory at {}",
            path.display()
        ),
        None => PathBuf::from("."),
    };
    for candidate in ["wrangler.jsonc", "wrangler.json"] {
        let path = directory.join(candidate);
        if path.exists() {
            return Ok(path);
        }
    }
    if directory.join("wrangler.toml").exists() {
        bail!("wrangler.toml is not supported; convert it to wrangler.jsonc");
    }
    bail!(
        "no wrangler.jsonc or wrangler.json in {}",
        directory.display()
    )
}

fn read_project(path: &Path, root: &Path) -> anyhow::Result<Project> {
    let source =
        std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let config: Value = serde_json::from_str(&strip_jsonc(&source))
        .with_context(|| format!("parse {}", path.display()))?;
    let object = config
        .as_object()
        .ok_or_else(|| anyhow!("{} is not a JSON object", path.display()))?;

    let unsupported = object
        .keys()
        .filter(|key| !SUPPORTED_KEYS.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        bail!(
            "`celld deploy` does not support these config keys: {}.\n\
             Deploy this project with Wrangler instead, or remove them.",
            unsupported.join(", ")
        );
    }

    let script_name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("config has no `name`"))?
        .to_string();
    if !valid_script_name(&script_name) {
        bail!(
            "config `name` must contain 1 to {MAX_SCRIPT_NAME_BYTES} bytes of lowercase \
             ASCII letters, digits, or internal hyphens: {script_name:?}"
        );
    }
    let main = object
        .get("main")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| anyhow!("config `main` must be a string"))
                .and_then(|value| project_relative_path(value, "main"))
        })
        .transpose()?;
    if let Some(main) = &main {
        let entry = root.join(main);
        let metadata = std::fs::symlink_metadata(&entry)
            .with_context(|| format!("inspect entry point {}", entry.display()))?;
        if !metadata.file_type().is_file() {
            bail!("entry point {} is not a regular file", entry.display());
        }
    }
    let no_bundle = match object.get("no_bundle") {
        None => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => bail!("config `no_bundle` must be a boolean"),
    };
    if no_bundle && main.is_none() {
        bail!("config sets `no_bundle` without `main`");
    }
    let assets = object
        .get("assets")
        .map(|value| read_asset_project(value, root, object))
        .transpose()?;
    if main.is_none() && assets.is_none() {
        bail!("config has neither `main` nor `assets`");
    }
    if main.is_none()
        && assets.as_ref().is_some_and(|assets| {
            matches!(
                &assets.config.run_worker_first,
                RunWorkerFirst::Bool(true) | RunWorkerFirst::Routes(_)
            )
        })
    {
        bail!("an asset-only project cannot set `assets.run_worker_first`");
    }

    let mut sqlite_classes = read_sqlite_classes(object)?;
    if let Some(class) = sqlite_classes.iter().find(|class| is_reserved_class(class)) {
        bail!("`{class}` is a runtime class; remove it from `migrations`");
    }
    let crons = read_crons(object)?;
    if !crons.is_empty() && main.is_none() {
        bail!("config sets `triggers.crons` without `main`; a cron trigger needs a `scheduled` handler to call");
    }

    // Wrangler-shaped upload metadata, so a manifest written here and one
    // written by the control plane describe a deployment the same way.
    let mut bindings = Vec::new();
    let mut do_classes = Vec::new();
    for binding in object
        .get("durable_objects")
        .and_then(|value| value.get("bindings"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
    {
        let name = binding
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("durable object binding has no `name`"))?;
        let class_name = binding
            .get("class_name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("durable object binding {name} has no `class_name`"))?;
        do_classes.push(class_name.to_string());
        bindings.push(json!({
            "type": "durable_object_namespace",
            "name": name,
            "class_name": class_name,
        }));
    }
    let services = match object.get("services") {
        None => &[][..],
        Some(Value::Array(services)) => services.as_slice(),
        Some(_) => bail!("config `services` must be an array"),
    };
    let mut service_count = 0_usize;
    for service in services {
        let binding = service
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("service binding has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid service binding name: {binding:?}");
        }
        let target = service
            .get("service")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("service binding {binding} has no `service`"))?;
        let entrypoint = service
            .get("entrypoint")
            .map(|value| {
                value.as_str().ok_or_else(|| {
                    anyhow!("service binding {binding} `entrypoint` must be a string")
                })
            })
            .transpose()?;
        let mut encoded = json!({
            "type": "service",
            "name": binding,
            "service": target,
        });
        if let Some(entrypoint) = entrypoint {
            encoded["entrypoint"] = json!(entrypoint);
        }
        bindings.push(encoded);
        service_count += 1;
    }
    let d1_databases = match object.get("d1_databases") {
        None => &[][..],
        Some(Value::Array(databases)) => databases.as_slice(),
        Some(_) => bail!("config `d1_databases` must be an array"),
    };
    // The reserved class must be refused whether or not the project declares
    // any `d1_databases`: the harness registers its own `__D1Database` class
    // in every isolate, so a durable_objects binding naming it would silently
    // resolve to the D1 database cell instead of the user's class.
    if let Some(class) = do_classes.iter().find(|class| is_reserved_class(class)) {
        bail!("`{class}` is a runtime class; rename the Durable Object class");
    }
    for database in d1_databases {
        let binding = database
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("d1 database has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid d1 binding name: {binding:?}");
        }
        let database_name = database
            .get("database_name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow!("d1 binding {binding} has no `database_name`; celld uses the name to select the database from the CLI")
            })?;
        if database_name.is_empty() {
            bail!("d1 binding {binding} has an empty `database_name`");
        }
        let mut encoded = json!({
            "type": "d1",
            "name": binding,
            "database_name": database_name,
        });
        if let Some(database_id) = database.get("database_id") {
            let database_id = database_id
                .as_str()
                .filter(|database_id| !database_id.is_empty())
                .ok_or_else(|| anyhow!("d1 binding {binding} has an invalid `database_id`"))?;
            encoded["database_id"] = json!(database_id);
        }
        bindings.push(encoded);
    }
    if !d1_databases.is_empty() {
        // A D1 database is a cell of a runtime-supplied class. Declaring it
        // here is what registers its namespace key and marks it SQLite-backed;
        // it is never a worker export, so it stays out of `ctx.exports`.
        do_classes.push(D1_CLASS.to_string());
        sqlite_classes.push(D1_CLASS.to_string());
    }
    let kv_namespaces = match object.get("kv_namespaces") {
        None => &[][..],
        Some(Value::Array(namespaces)) => namespaces.as_slice(),
        Some(_) => bail!("config `kv_namespaces` must be an array"),
    };
    let mut kv_ids = BTreeSet::new();
    for namespace in kv_namespaces {
        // Every key celld does not model stops the deploy, per the loud-gap
        // rule. `preview_id` is the exception and is accepted below: it selects
        // a different namespace under `wrangler dev`, which celld does not run,
        // so ignoring it costs a developer nothing at deploy time.
        let unsupported = namespace
            .as_object()
            .map(|object| {
                object
                    .keys()
                    .filter(|key| !matches!(key.as_str(), "binding" | "id" | "preview_id"))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(key) = unsupported.first() {
            bail!(
                "kv namespace declares `{key}`, which celld does not model; \
                 celld models `binding`, `id`, and `preview_id`"
            );
        }
        let binding = namespace
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("kv namespace has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid kv binding name: {binding:?}");
        }
        // `id` is the namespace identity, verbatim, and celld invents no
        // `namespace_name` key beside it. Upstream's `kv_namespaces` carries no
        // human-readable name at all -- only `binding`, `id` and `preview_id` --
        // and celld's accepted configuration is a strict subset of Wrangler's:
        // a key celld accepts and Wrangler diagnoses would make the file
        // unportable, which is the one property the deploy story rests on. A
        // project ported from Cloudflare therefore keeps its hex id and works;
        // a project that was never there writes a readable string.
        let id = namespace.get("id").and_then(Value::as_str).ok_or_else(|| {
            anyhow!(
                "kv binding {binding} has no `id`; celld uses the id as the \
                     namespace identity, so any stable string serves"
            )
        })?;
        if id.is_empty() {
            bail!("kv binding {binding} has an empty `id`");
        }
        // The id becomes a cell name and therefore a path component and an
        // object-store key, so it answers to the same charset fence every cell
        // scope does rather than to a looser one invented here.
        if !celld_logic::cell::valid_cell_scope(id) {
            bail!(
                "kv binding {binding} has an `id` that cannot name a cell: {id:?}; \
                 use ASCII letters, digits, and `_ - . : $`"
            );
        }
        if !kv_ids.insert(id.to_string()) {
            bail!("duplicate kv namespace id: {id:?}");
        }
        bindings.push(json!({
            "type": "kv",
            "name": binding,
            "id": id,
        }));
    }
    if !kv_namespaces.is_empty() {
        // A KV namespace is a cell of a runtime-supplied class, declared here
        // so its namespace key is minted and its cells are SQLite-backed. Like
        // D1's, it is never a worker export and stays out of `ctx.exports`.
        do_classes.push(KV_CLASS.to_string());
        sqlite_classes.push(KV_CLASS.to_string());
    }
    let queues = match object.get("queues") {
        None => None,
        Some(Value::Object(queues)) => Some(queues),
        Some(_) => bail!("config `queues` must be an object"),
    };
    if let Some(key) = queues
        .into_iter()
        .flat_map(|queues| queues.keys())
        .find(|key| !matches!(key.as_str(), "producers" | "consumers"))
    {
        bail!("config `queues` declares `{key}`, which celld does not model");
    }
    let queue_producers = match queues.and_then(|queues| queues.get("producers")) {
        None => &[][..],
        Some(Value::Array(producers)) => producers.as_slice(),
        Some(_) => bail!("config `queues.producers` must be an array"),
    };
    for producer in queue_producers {
        reject_queue_keys(
            producer,
            &["binding", "queue", "delivery_delay"],
            "producer",
        )?;
        let binding = producer
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("queue producer has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid queue binding name: {binding:?}");
        }
        let queue = queue_name(producer, "producer")?;
        let delivery_delay = optional_queue_u32(producer, "delivery_delay")?.unwrap_or(0);
        if delivery_delay > celld_logic::queue::MAX_DELAY_SECONDS {
            bail!("queue producer delivery_delay must be between 0 and 86400 seconds");
        }
        let mut encoded = json!({
            "type": "queue",
            "name": binding,
            "queue": queue,
        });
        if producer.get("delivery_delay").is_some() {
            encoded["delivery_delay"] = json!(delivery_delay);
        }
        bindings.push(encoded);
    }
    let queue_consumers_raw = match queues.and_then(|queues| queues.get("consumers")) {
        None => &[][..],
        Some(Value::Array(consumers)) => consumers.as_slice(),
        Some(_) => bail!("config `queues.consumers` must be an array"),
    };
    let mut queue_consumers = Vec::new();
    let mut consumed_queues = BTreeSet::new();
    for consumer in queue_consumers_raw {
        if consumer.get("script_name").is_some() {
            bail!("queue consumer declares `script_name`; celld attaches it to the current script");
        }
        reject_queue_keys(
            consumer,
            &[
                "queue",
                "max_batch_size",
                "max_batch_timeout",
                "max_retries",
                "dead_letter_queue",
                "max_concurrency",
                "retry_delay",
            ],
            "consumer",
        )?;
        let queue = queue_name(consumer, "consumer")?.to_string();
        if !consumed_queues.insert(queue.clone()) {
            bail!("duplicate queue consumer: {queue:?}");
        }
        let dead_letter_queue = consumer
            .get("dead_letter_queue")
            .map(|value| {
                let name = value
                    .as_str()
                    .ok_or_else(|| anyhow!("queue consumer dead_letter_queue must be a string"))?;
                validate_queue_name(name, "dead-letter queue")?;
                anyhow::ensure!(
                    name != queue,
                    "a queue cannot use itself as its dead-letter queue"
                );
                Ok::<_, anyhow::Error>(name.to_string())
            })
            .transpose()?;
        let config = QueueConsumerConfig {
            queue,
            max_batch_size: optional_queue_u16(consumer, "max_batch_size")?
                .unwrap_or(celld_logic::queue::DEFAULT_MAX_BATCH_SIZE),
            max_batch_timeout: optional_queue_u16(consumer, "max_batch_timeout")?
                .unwrap_or(celld_logic::queue::DEFAULT_MAX_BATCH_TIMEOUT_SECONDS),
            max_retries: optional_queue_u16(consumer, "max_retries")?
                .unwrap_or(celld_logic::queue::DEFAULT_MAX_RETRIES),
            dead_letter_queue,
            max_concurrency: optional_queue_u16(consumer, "max_concurrency")?,
            retry_delay: optional_queue_u32(consumer, "retry_delay")?,
        };
        celld_logic::queue::validate_config(&celld_logic::queue::QueueConfig {
            max_batch_size: config.max_batch_size,
            max_batch_timeout_seconds: config.max_batch_timeout,
            max_retries: config.max_retries,
            max_concurrency: config.max_concurrency,
            delivery_delay_seconds: 0,
            retry_delay_seconds: config.retry_delay,
        })
        .map_err(anyhow::Error::new)?;
        queue_consumers.push(config);
    }
    if !queue_producers.is_empty() || !queue_consumers.is_empty() {
        if !queue_consumers.is_empty() && main.is_none() {
            bail!("config declares a queue consumer without `main`; a consumer needs a Worker module with a queue handler");
        }
        do_classes.push(QUEUE_CLASS.to_string());
        sqlite_classes.push(QUEUE_CLASS.to_string());
    }
    let workflows = match object.get("workflows") {
        None => &[][..],
        Some(Value::Array(workflows)) => workflows.as_slice(),
        Some(_) => bail!("config `workflows` must be an array"),
    };
    let mut workflow_names = BTreeSet::new();
    for workflow in workflows {
        // Any key celld does not model stops the deploy, per the loud-gap
        // rule: an unread knob (`schedules` is the notable one) must be a
        // refusal, never a workflow that silently lacks the behavior it
        // declares.
        let unsupported = workflow
            .as_object()
            .map(|object| {
                object
                    .keys()
                    .filter(|key| {
                        !matches!(
                            key.as_str(),
                            "name" | "binding" | "class_name" | "script_name"
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !unsupported.is_empty() {
            bail!(
                "`celld deploy` does not support these workflow keys: {}.\n\
                 celld models `name`, `binding`, `class_name`, and `script_name`; \
                 remove the rest (`schedules` included).",
                unsupported.join(", ")
            );
        }
        let binding = workflow
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("workflow has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid workflow binding name: {binding:?}");
        }
        let workflow_name = workflow
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("workflow binding {binding} has no `name`"))?;
        if !valid_resource_name(workflow_name) {
            bail!(
                "invalid workflow name {workflow_name:?}: use letters, digits, `-` and `_` \
                 (not starting with `-`), at most 64 characters"
            );
        }
        if !workflow_names.insert(workflow_name.to_string()) {
            bail!("duplicate workflow name: {workflow_name:?}");
        }
        let class_name = workflow
            .get("class_name")
            .and_then(Value::as_str)
            .filter(|class| !class.is_empty())
            .ok_or_else(|| anyhow!("workflow binding {binding} has no `class_name`"))?;
        if is_reserved_class(class_name) {
            bail!(
                "workflow {workflow_name:?} names the reserved class {class_name:?} as its \
                 `class_name`; export a WorkflowEntrypoint subclass of your own"
            );
        }
        if let Some(script) = workflow.get("script_name").and_then(Value::as_str) {
            if script != script_name {
                bail!(
                    "workflow {workflow_name:?} sets `script_name` {script:?}; celld runs a \
                     workflow only in the script that declares it"
                );
            }
        }
        bindings.push(json!({
            "type": "workflow",
            "name": binding,
            "workflow_name": workflow_name,
            "class_name": class_name,
        }));
    }
    if !workflows.is_empty() {
        if main.is_none() {
            bail!("config declares `workflows` without `main`; a workflow needs a Worker module to export its class");
        }
        do_classes.push(workflow_class(&script_name));
        sqlite_classes.push(workflow_class(&script_name));
    }
    let r2_buckets = match object.get("r2_buckets") {
        None => &[][..],
        Some(Value::Array(buckets)) => buckets.as_slice(),
        Some(_) => bail!("config `r2_buckets` must be an array"),
    };
    let mut r2_binding_names = BTreeSet::new();
    let mut r2_bucket_names = BTreeSet::new();
    for bucket in r2_buckets {
        let binding = bucket
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("r2 bucket has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid r2 binding name: {binding:?}");
        }
        if !r2_binding_names.insert(binding.to_string()) {
            bail!("duplicate r2 binding name: {binding:?}");
        }
        let bucket_name = bucket
            .get("bucket_name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow!("r2 binding {binding} has no `bucket_name`; celld uses the name to place the bucket's keys in the fleet bucket")
            })?;
        if !valid_resource_name(bucket_name) {
            bail!(
                "invalid r2 bucket name {bucket_name:?}: use letters, digits, `-` and `_` \
                 (not starting with `-`), at most 64 characters"
            );
        }
        if !r2_bucket_names.insert(bucket_name.to_string()) {
            bail!("two r2 bindings name the same bucket: {bucket_name:?}");
        }
        // Cloudflare scopes a jurisdiction to an account's R2; celld has
        // neither, and silently ignoring it would put the keys somewhere
        // the operator did not ask for.
        if bucket.get("jurisdiction").is_some() {
            bail!("r2 binding {binding} sets `jurisdiction`, which celld does not have");
        }
        bindings.push(json!({
            "type": "r2_bucket",
            "name": binding,
            "bucket_name": bucket_name,
        }));
    }
    let vars = match object.get("vars") {
        None => None,
        Some(Value::Object(vars)) => Some(vars),
        Some(_) => bail!("config `vars` must be an object"),
    };
    let mut var_count = 0_usize;
    for (name, value) in vars.into_iter().flatten() {
        if !valid_binding(name) {
            bail!("invalid var binding name: {name:?}");
        }
        let value = value
            .as_str()
            .ok_or_else(|| anyhow!("var binding {name} must be a string"))?;
        bindings.push(json!({
            "type": "plain_text",
            "name": name,
            "text": value,
        }));
        var_count += 1;
    }
    if main.is_none()
        && (!do_classes.is_empty()
            || !sqlite_classes.is_empty()
            || service_count > 0
            || var_count > 0
            || !r2_binding_names.is_empty())
    {
        bail!("an asset-only project cannot declare Worker bindings");
    }
    if let Some(binding) = assets
        .as_ref()
        .and_then(|assets| assets.config.binding.as_ref())
    {
        bindings.push(json!({
            "type": "assets",
            "name": binding,
        }));
    }
    // build_env installs every binding into one JavaScript object. Validate
    // that structural invariant once here, after every binding source has
    // contributed, so a new binding kind cannot silently shadow an old one.
    let mut binding_names = BTreeMap::new();
    for binding in &bindings {
        let name = binding.get("name").and_then(Value::as_str).unwrap_or("");
        let kind = binding.get("type").and_then(Value::as_str).unwrap_or("");
        if let Some(previous) = binding_names.insert(name, kind) {
            bail!(
                "binding name {name:?} is declared by both a {previous} and a {kind} \
                 binding; every name in `env` must be unique"
            );
        }
    }
    let mut metadata = Map::new();
    if main.is_some() {
        metadata.insert("main_module".into(), json!("index.js"));
    }
    if let Some(assets) = &assets {
        metadata.insert("assets".into(), assets.raw_metadata.clone());
    }
    if let Some(date) = object.get("compatibility_date") {
        metadata.insert("compatibility_date".into(), date.clone());
    }
    if let Some(flags) = object.get("compatibility_flags") {
        metadata.insert("compatibility_flags".into(), flags.clone());
    }
    metadata.insert("bindings".into(), Value::Array(bindings));
    if !queue_consumers.is_empty() {
        // Include consumer policy in deployment identity. The typed manifest
        // field below is the runtime contract; this copy keeps a settings-only
        // deploy from reusing the previous code-and-bindings version.
        metadata.insert("queue_consumers".into(), json!(queue_consumers));
    }
    if !sqlite_classes.is_empty() {
        metadata.insert(
            "migrations".into(),
            json!({ "new_sqlite_classes": sqlite_classes }),
        );
    }

    Ok(Project {
        script_name,
        no_bundle,
        entry: main,
        assets,
        metadata: Value::Object(metadata),
        do_classes,
        sqlite_classes,
        crons,
        has_workflows: !workflows.is_empty(),
        has_kv: !kv_namespaces.is_empty(),
        has_queues: !queue_producers.is_empty() || !queue_consumers.is_empty(),
        queue_consumers,
        has_r2: !r2_buckets.is_empty(),
    })
}

fn reject_queue_keys(value: &Value, accepted: &[&str], kind: &str) -> anyhow::Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("queue {kind} must be an object"))?;
    if let Some(key) = object.keys().find(|key| !accepted.contains(&key.as_str())) {
        bail!("queue {kind} declares `{key}`, which celld does not model");
    }
    Ok(())
}

fn queue_name<'a>(value: &'a Value, kind: &str) -> anyhow::Result<&'a str> {
    let name = value
        .get("queue")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("queue {kind} has no `queue` name"))?;
    validate_queue_name(name, kind)?;
    Ok(name)
}

fn validate_queue_name(name: &str, kind: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!name.is_empty(), "queue {kind} has an empty queue name");
    anyhow::ensure!(
        celld_logic::cell::valid_cell_scope(name),
        "queue {kind} has a name that cannot name a cell: {name:?}; use ASCII letters, digits, and `_ - . : $`"
    );
    Ok(())
}

fn optional_queue_u64(value: &Value, field: &str) -> anyhow::Result<Option<u64>> {
    value
        .get(field)
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| anyhow!("queue {field} must be a non-negative integer"))
        })
        .transpose()
}

fn optional_queue_u16(value: &Value, field: &str) -> anyhow::Result<Option<u16>> {
    optional_queue_u64(value, field)?
        .map(|value| {
            u16::try_from(value).map_err(|_| anyhow!("queue {field} is too large: {value}"))
        })
        .transpose()
}

fn optional_queue_u32(value: &Value, field: &str) -> anyhow::Result<Option<u32>> {
    optional_queue_u64(value, field)?
        .map(|value| {
            u32::try_from(value).map_err(|_| anyhow!("queue {field} is too large: {value}"))
        })
        .transpose()
}

/// The upstream workflow-name rule (`^[a-zA-Z0-9_][a-zA-Z0-9-_]*$`, at most 64
/// characters). Instance ids follow the same rule; the harness validates those
/// because they arrive at run time, not at deploy. An R2 `bucket_name` takes
/// the same rule for a second reason: the name becomes a key prefix inside the
/// fleet bucket, and this rule is what keeps it a single path segment, so a
/// binding cannot address the fleet's own deployment, cell, or lease keys.
fn valid_resource_name(name: &str) -> bool {
    name.len() <= 64
        && name
            .chars()
            .next()
            .is_some_and(|first| first == '_' || first.is_ascii_alphanumeric())
        && name
            .chars()
            .skip(1)
            .all(|value| value == '_' || value == '-' || value.is_ascii_alphanumeric())
}

/// The Wrangler-compatible Worker name that is safe in every place where
/// celld reuses it. Checking only the generated Workflow class would leave
/// deployment keys and operator output with a different accepted language.
fn valid_script_name(name: &str) -> bool {
    name.len() <= MAX_SCRIPT_NAME_BYTES
        && name
            .bytes()
            .next()
            .is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
        && name
            .bytes()
            .last()
            .is_some_and(|last| last.is_ascii_lowercase() || last.is_ascii_digit())
        && name
            .bytes()
            .all(|value| value.is_ascii_lowercase() || value.is_ascii_digit() || value == b'-')
}

/// `triggers.crons`, validated here so a malformed expression stops the deploy
/// the developer is watching instead of an activation an hour later. Wrangler
/// accepts `triggers` with other keys we do not model; only `crons` is read.
fn read_crons(project: &Map<String, Value>) -> anyhow::Result<Vec<String>> {
    let Some(value) = project.get("triggers") else {
        return Ok(Vec::new());
    };
    let triggers = value
        .as_object()
        .ok_or_else(|| anyhow!("config `triggers` must be an object"))?;
    let Some(value) = triggers.get("crons") else {
        return Ok(Vec::new());
    };
    let entries = value
        .as_array()
        .ok_or_else(|| anyhow!("config `triggers.crons` must be an array of strings"))?;
    let mut crons = Vec::new();
    for entry in entries {
        let expression = entry
            .as_str()
            .ok_or_else(|| anyhow!("config `triggers.crons` must be an array of strings"))?;
        celld_logic::cron::parse(expression).map_err(|error| anyhow!("{error}"))?;
        crons.push(expression.trim().to_string());
    }
    Ok(crons)
}

fn read_sqlite_classes(project: &Map<String, Value>) -> anyhow::Result<Vec<String>> {
    let Some(value) = project.get("migrations") else {
        return Ok(Vec::new());
    };
    let migrations = value
        .as_array()
        .ok_or_else(|| anyhow!("config `migrations` must be an array"))?;
    let mut tags = BTreeSet::new();
    let mut classes = BTreeSet::new();
    let mut result = Vec::new();
    for (index, migration) in migrations.iter().enumerate() {
        let migration = migration
            .as_object()
            .ok_or_else(|| anyhow!("config `migrations[{index}]` must be an object"))?;
        let unsupported = migration
            .keys()
            .filter(|key| !matches!(key.as_str(), "tag" | "new_sqlite_classes"))
            .cloned()
            .collect::<Vec<_>>();
        if !unsupported.is_empty() {
            bail!(
                "`celld deploy` does not support these migration keys: {}.\n\
                 Class rename, delete, transfer, and non-SQLite migration semantics need an explicit persisted-state contract before deployment.",
                unsupported.join(", ")
            );
        }
        let tag = migration
            .get("tag")
            .and_then(Value::as_str)
            .filter(|tag| !tag.is_empty())
            .ok_or_else(|| {
                anyhow!("config `migrations[{index}].tag` must be a non-empty string")
            })?;
        if !tags.insert(tag.to_string()) {
            bail!("config has duplicate migration tag {tag:?}");
        }
        let Some(value) = migration.get("new_sqlite_classes") else {
            continue;
        };
        let new_classes = value.as_array().ok_or_else(|| {
            anyhow!("config `migrations[{index}].new_sqlite_classes` must be an array")
        })?;
        for (class_index, class) in new_classes.iter().enumerate() {
            let class = class.as_str().filter(|class| !class.is_empty()).ok_or_else(|| {
                anyhow!(
                    "config `migrations[{index}].new_sqlite_classes[{class_index}]` must be a non-empty string"
                )
            })?;
            if !classes.insert(class.to_string()) {
                bail!("SQLite class {class:?} is introduced by more than one migration");
            }
            result.push(class.to_string());
        }
    }
    Ok(result)
}

fn read_asset_project(
    value: &Value,
    root: &Path,
    project: &Map<String, Value>,
) -> anyhow::Result<ProjectAssets> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("config `assets` must be an object"))?;
    let supported = [
        "directory",
        "binding",
        "html_handling",
        "not_found_handling",
        "run_worker_first",
    ];
    let unsupported = object
        .keys()
        .filter(|key| !supported.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        bail!(
            "`celld deploy` does not support these assets keys: {}",
            unsupported.join(", ")
        );
    }
    let directory = object
        .get("directory")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("config `assets.directory` must be a string"))?;
    let directory = project_relative_path(directory, "assets.directory")?;
    let directory = root.join(directory);
    let metadata = std::fs::symlink_metadata(&directory)
        .with_context(|| format!("inspect asset directory {}", directory.display()))?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        bail!(
            "asset directory {} is not a regular directory",
            directory.display()
        );
    }

    let binding = object
        .get("binding")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| anyhow!("config `assets.binding` must be a string"))
                .and_then(|binding| {
                    if valid_binding(binding) {
                        Ok(binding.to_string())
                    } else {
                        bail!("invalid asset binding name: {binding:?}")
                    }
                })
        })
        .transpose()?;
    let html_handling = optional_asset_mode(
        object,
        "html_handling",
        "auto-trailing-slash",
        &[
            "auto-trailing-slash",
            "force-trailing-slash",
            "drop-trailing-slash",
            "none",
        ],
    )?;
    let not_found_handling = optional_asset_mode(
        object,
        "not_found_handling",
        "none",
        &["none", "single-page-application", "404-page"],
    )?;
    let run_worker_first = object
        .get("run_worker_first")
        .map(|value| {
            serde_json::from_value::<RunWorkerFirst>(value.clone())
                .context("config `assets.run_worker_first` must be a boolean or route list")
        })
        .transpose()?
        .unwrap_or_default();
    validate_worker_first(&run_worker_first)?;

    let headers = read_asset_directive(&directory, "_headers")?;
    let redirects = read_asset_directive(&directory, "_redirects")?;
    let compatibility_date = project
        .get("compatibility_date")
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| anyhow!("config `compatibility_date` must be a string"))
        })
        .transpose()?;
    let compatibility_flags = project
        .get("compatibility_flags")
        .map(|value| {
            value
                .as_array()
                .ok_or_else(|| anyhow!("config `compatibility_flags` must be an array"))?
                .iter()
                .map(|flag| {
                    flag.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| anyhow!("compatibility flags must be strings"))
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();

    // Retain the Wrangler-shaped upload metadata as well as the normalized
    // index config. Defaults and compatibility settings belong in the index;
    // upload metadata includes only the values Wrangler would send.
    let mut upload_config = Map::new();
    for key in ["html_handling", "not_found_handling", "run_worker_first"] {
        if let Some(value) = object.get(key) {
            upload_config.insert(key.to_string(), value.clone());
        }
    }
    if let Some(headers) = &headers {
        upload_config.insert("_headers".to_string(), Value::String(headers.clone()));
    }
    if let Some(redirects) = &redirects {
        upload_config.insert("_redirects".to_string(), Value::String(redirects.clone()));
    }

    Ok(ProjectAssets {
        directory,
        config: AssetConfig {
            binding,
            html_handling: Some(html_handling),
            not_found_handling: Some(not_found_handling),
            run_worker_first,
            headers,
            redirects,
            compatibility_date,
            compatibility_flags,
        },
        raw_metadata: json!({ "config": Value::Object(upload_config) }),
    })
}

fn project_relative_path(value: &str, key: &str) -> anyhow::Result<String> {
    let value = value.strip_prefix("./").unwrap_or(value);
    let path = Path::new(value);
    if value.is_empty()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        bail!("config `{key}` must be a path inside the project");
    }
    Ok(value.to_string())
}

fn optional_asset_mode(
    object: &Map<String, Value>,
    key: &str,
    default: &str,
    supported: &[&str],
) -> anyhow::Result<String> {
    let value = match object.get(key) {
        Some(value) => value
            .as_str()
            .ok_or_else(|| anyhow!("config `assets.{key}` must be a string"))?,
        None => default,
    };
    if !supported.contains(&value) {
        bail!("unsupported assets.{key} value: {value:?}");
    }
    Ok(value.to_string())
}

fn valid_binding(binding: &str) -> bool {
    binding.len() <= 128
        && binding
            .chars()
            .next()
            .is_some_and(|value| value == '_' || value == '$' || value.is_ascii_alphabetic())
        && binding
            .chars()
            .skip(1)
            .all(|value| value == '_' || value == '$' || value.is_ascii_alphanumeric())
}

fn validate_worker_first(value: &RunWorkerFirst) -> anyhow::Result<()> {
    let RunWorkerFirst::Routes(routes) = value else {
        return Ok(());
    };
    if routes.is_empty() || routes.len() > 100 {
        bail!("asset worker-first routes must contain between 1 and 100 rules");
    }
    let mut positive = false;
    let mut seen = std::collections::HashSet::new();
    for route in routes {
        if route.len() <= 1
            || route.len() > 100
            || route.contains(['\\', '\0'])
            || (!route.starts_with('/') && !route.starts_with("!/"))
            || !seen.insert(route)
        {
            bail!("invalid asset worker-first route: {route:?}");
        }
        positive |= route.starts_with('/');
    }
    if !positive {
        bail!("asset worker-first routes require a positive rule");
    }
    Ok(())
}

fn read_asset_directive(directory: &Path, name: &str) -> anyhow::Result<Option<String>> {
    let path = directory.join(name);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
    };
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        bail!("asset directive {} is not a regular file", path.display());
    }
    if metadata.len() > MAX_ASSET_DIRECTIVE_BYTES {
        bail!("asset directive {} exceeds 100 KiB", path.display());
    }
    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    if contents.len() as u64 > MAX_ASSET_DIRECTIVE_BYTES {
        bail!("asset directive {} exceeds 100 KiB", path.display());
    }
    Ok(Some(contents))
}

fn build_assets(project: &ProjectAssets) -> anyhow::Result<BuiltAssets> {
    let mut files = Vec::new();
    collect_asset_files(&project.directory, "", &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    if files.len() > MAX_ASSET_FILES {
        bail!("asset directory exceeds the {MAX_ASSET_FILES}-file limit");
    }

    let mut entries = BTreeMap::new();
    let mut blobs = BTreeMap::new();
    let mut total_bytes = 0_u64;
    for (relative, path) in files {
        let metadata =
            std::fs::metadata(&path).with_context(|| format!("inspect {}", path.display()))?;
        if metadata.len() > MAX_ASSET_FILE_BYTES {
            bail!("asset /{relative} exceeds the 25 MiB file limit");
        }
        total_bytes = total_bytes
            .checked_add(metadata.len())
            .context("asset byte count overflow")?;
        if total_bytes > MAX_ASSET_BYTES {
            bail!("asset directory exceeds the 1 GiB deployment limit");
        }
        let body = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        if body.len() as u64 != metadata.len() {
            bail!("asset changed while being read: {}", path.display());
        }
        let sha256 = format!("{:x}", Sha256::digest(&body));
        blobs.entry(sha256.clone()).or_insert(body);
        entries.insert(
            format!("/{relative}"),
            AssetEntry {
                sha256,
                bytes: metadata.len(),
                content_type: asset_content_type(&path).map(str::to_string),
            },
        );
    }
    let file_count = u32::try_from(entries.len()).context("asset file count overflow")?;
    let index = serde_json::to_vec(&AssetIndex {
        schema_version: 1,
        entries,
        config: project.config.clone(),
    })?;
    Ok(BuiltAssets {
        index,
        blobs,
        file_count,
        total_bytes,
    })
}

fn collect_asset_files(
    directory: &Path,
    relative: &str,
    files: &mut Vec<(String, PathBuf)>,
) -> anyhow::Result<()> {
    let mut entries = std::fs::read_dir(directory)
        .with_context(|| format!("read asset directory {}", directory.display()))?
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name().into_string().map_err(|_| {
            anyhow!(
                "asset path contains a non-UTF-8 name under {}",
                directory.display()
            )
        })?;
        if relative.is_empty() && name == ".assetsignore" {
            bail!(
                "{} is not supported by `celld deploy`; remove it or deploy with Wrangler",
                entry.path().display()
            );
        }
        if relative.is_empty() && (name == "_worker.js" || name.starts_with("_worker.js/")) {
            bail!(
                "refusing to publish reserved Worker source as an asset: {}",
                entry.path().display()
            );
        }
        if relative.is_empty() && matches!(name.as_str(), "_headers" | "_redirects") {
            continue;
        }
        if name.contains(['\\', '\0']) {
            bail!("invalid asset path component: {name:?}");
        }
        let child_relative = if relative.is_empty() {
            name
        } else {
            format!("{relative}/{name}")
        };
        if child_relative.len() + 1 > 1024 {
            bail!("asset path exceeds 1024 bytes: /{child_relative}");
        }
        let asset_path = format!("/{child_relative}");
        if !crate::assets::is_canonical_asset_path(&asset_path) {
            bail!("asset path is not canonical: {asset_path:?}");
        }
        let file_type = entry
            .file_type()
            .with_context(|| format!("inspect asset {}", entry.path().display()))?;
        if file_type.is_symlink() {
            bail!(
                "asset tree contains a symbolic link: {}",
                entry.path().display()
            );
        } else if file_type.is_dir() {
            collect_asset_files(&entry.path(), &child_relative, files)?;
        } else if file_type.is_file() {
            files.push((child_relative, entry.path()));
        } else {
            bail!(
                "asset tree contains a special file: {}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

fn asset_content_type(path: &Path) -> Option<&'static str> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    Some(match extension.as_str() {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" | "cjs" => "text/javascript; charset=utf-8",
        "json" | "map" => "application/json; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "xml" => "application/xml; charset=utf-8",
        "svg" => "image/svg+xml",
        "webmanifest" => "application/manifest+json; charset=utf-8",
        "wasm" => "application/wasm",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "ico" => "image/x-icon",
        "pdf" => "application/pdf",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "br" => "application/octet-stream",
        _ => return None,
    })
}

/// esbuild's outputs: the bundled entry module, and the wasm files its
/// imports were resolved to (each under the name the rewritten import uses).
struct BundleOutput {
    bundle: Vec<u8>,
    wasm: Vec<(String, Vec<u8>)>,
}

fn run_esbuild(root: &Path, entry: &str) -> anyhow::Result<BundleOutput> {
    // node: builtins stay external. Wrangler polyfills them with unenv; celld
    // implements the workerd `nodejs_compat` subset itself, so the runtime
    // provides them.
    let binary = std::env::var("CELLD_ESBUILD").unwrap_or_else(|_| "esbuild".to_string());
    let outdir = tempfile::tempdir().context("create esbuild output directory")?;
    // esbuild leaves an external builtin from a CommonJS dependency behind
    // its synchronous __require helper. Workerd's Wrangler plugin rewrites
    // that call to an ESM import, but the esbuild CLI has no equivalent
    // plugin hook. Give only this generated bundle a lexical bridge to the
    // same builtin table. A runtime global would make require visible to raw
    // ESM Workers, which Workerd does not do.
    let commonjs_builtin_bridge = r#"var require = (id) => {
  const name = String(id);
  const builtin = process.getBuiltinModule(name);
  if (builtin === undefined) {
    const error = new Error(`Cannot find module '${name}'`);
    error.code = "MODULE_NOT_FOUND";
    throw error;
  }
  return builtin;
};"#;
    let output = Command::new(&binary)
        .current_dir(root)
        .arg(entry)
        .arg("--bundle")
        .arg("--format=esm")
        .arg("--platform=browser")
        .arg("--target=es2024")
        .arg("--conditions=workerd,worker,browser")
        .arg(format!("--banner:js={commonjs_builtin_bridge}"))
        .arg("--external:node:*")
        .arg("--external:cloudflare:*")
        .args(
            crate::js::BARE_NODE_BUILTINS
                .iter()
                .map(|specifier| format!("--external:{specifier}")),
        )
        // Wasm becomes a sibling module (Wrangler's CompiledWasm rule). The
        // `copy` loader makes esbuild resolve each wasm import like any other
        // import (importer-relative, node_modules, deduplicated) and rewrite
        // the specifier to the copied file, so the bundle and the emitted
        // files agree on names; the runtime serves each file as a compiled
        // WebAssembly.Module default export.
        .arg("--loader:.wasm=copy")
        .arg(format!("--outdir={}", outdir.path().display()))
        .arg("--entry-names=index")
        .output()
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                anyhow!(
                    "esbuild not found ({binary}).\n\
                     `celld deploy` bundles with esbuild; install it and retry,\n\
                     or set CELLD_ESBUILD to its path."
                )
            } else {
                anyhow!("run esbuild: {error}")
            }
        })?;
    if !output.status.success() {
        bail!(
            "esbuild failed:\n{}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let bundle =
        std::fs::read(outdir.path().join("index.js")).context("read esbuild output bundle")?;
    // The copied wasm files land beside the bundle; each becomes its own
    // deployed module under the name the rewritten imports use.
    let mut wasm = Vec::new();
    for dirent in std::fs::read_dir(outdir.path()).context("read esbuild output directory")? {
        let path = dirent?.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("wasm") {
            let name = path
                .file_name()
                .expect("read_dir entries have a file name")
                .to_string_lossy()
                .into_owned();
            let bytes = std::fs::read(&path)
                .with_context(|| format!("read wasm module {}", path.display()))?;
            wasm.push((name, bytes));
        }
    }
    // read_dir order is platform-defined; the deployment version hashes the
    // module list, so keep it stable.
    wasm.sort();
    Ok(BundleOutput { bundle, wasm })
}

/// Minimal JSONC support: line and block comments, and trailing commas.
/// String contents are preserved verbatim.
fn strip_jsonc(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    let mut in_string = false;
    let mut escaped = false;
    while let Some(c) = chars.next() {
        if in_string {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => {
                in_string = true;
                out.push(c);
            }
            '/' if chars.peek() == Some(&'/') => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut previous = '\0';
                for c in chars.by_ref() {
                    if previous == '*' && c == '/' {
                        break;
                    }
                    previous = c;
                }
            }
            '}' | ']' => {
                // The comma this closes is trailing; drop it, keep the layout.
                let trimmed = out.trim_end().len();
                if out[..trimmed].ends_with(',') {
                    out.remove(trimmed - 1);
                }
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}
