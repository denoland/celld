// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Fleet CLI and peer-diagnostic work executes outside the Actor execution
// domain.
#![allow(clippy::disallowed_methods)]
// Diagnose used to split by verdict rather than by kind, so a redirect kept
// the passes and lost the failures. Every check now goes through `Output`.

//! Production bucket deployment adapters reused by the clean-sheet host.

use crate::bucket::{Bucket, StorageProbeVerdict};
use crate::deploy;
use crate::js::{ModuleSource, WorkerConfigOptions};
use crate::ownership_store::NodeLeaseWire;
use crate::protocol::{DeployPointer, Manifest, ModuleKind, Rollout};
use anyhow::{bail, Context};
use std::borrow::Cow;

use crate::cli_output::Format;
use crate::cli_output::Output;
use crate::cli_output::Record;
use crate::note;

/// What a deployment produced.
///
/// Deploy is a narrative, so its progress belongs on stderr — but the
/// version is the datum a release script reads back, so it stays a row on
/// stdout rather than a line to grep out of the prose.
pub(crate) struct Deployed {
    worker: String,
    version: String,
    location: String,
    dry_run: bool,
}

impl Record for Deployed {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "worker": self.worker,
            "version": self.version,
            "location": self.location,
            "dry_run": self.dry_run,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(if self.dry_run {
            format!(
                "Current Version ID: {} (dry run; nothing written)",
                self.version
            )
        } else {
            format!("Current Version ID: {}", self.version)
        })
    }
}

/// One diagnostic verdict.
///
/// Every check is a row, whatever its verdict. The old shape sent `ok` and
/// `skip` to stdout and `fail` to stderr, so `celld diagnose > report.txt`
/// saved the passes and lost the failures. The exit code still carries the
/// summary, so a redirect and `$?` together say everything.
pub struct Check {
    verdict: &'static str,
    subject: String,
    detail: String,
}

impl Check {
    fn new(verdict: &'static str, subject: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            verdict,
            subject: subject.into(),
            detail: detail.into(),
        }
    }

    pub fn ok(subject: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new("ok", subject, detail)
    }

    pub fn fail(subject: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new("fail", subject, detail)
    }

    pub fn skip(subject: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new("skip", subject, detail)
    }
}

impl Record for Check {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "verdict": self.verdict,
            "check": self.subject,
            "detail": self.detail,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        // The long-standing shape: `ok bucket ...`, `fail peer X: ...`.
        Cow::Owned(if self.detail.is_empty() {
            format!("{} {}", self.verdict, self.subject)
        } else {
            format!("{} {}: {}", self.verdict, self.subject, self.detail)
        })
    }
}
use std::collections::BTreeMap;
use std::time::Duration;
use tracing::info;

pub fn bucket_client(bucket: &str, endpoint: Option<&str>, region: &str) -> anyhow::Result<Bucket> {
    bucket_client_with_credentials(bucket, endpoint, region, None)
}

/// Build the authority-heartbeat client on its own HTTP connection pool.
///
/// Node lease traffic must not queue behind ordinary ownership, deployment,
/// or replica requests. Every `Bucket::open` builds its own transport, so a
/// dedicated instance keeps the safety lane isolated, and the `celld-lease`
/// app tag labels it in black-box storage traces.
pub fn lease_bucket_client_with_credentials(
    bucket: &str,
    endpoint: Option<&str>,
    region: &str,
    managed: Option<&crate::control_plane::ManagedStorageConfig>,
) -> anyhow::Result<Bucket> {
    open(bucket, endpoint, region, managed, Some("celld-lease"))
}

pub fn bucket_client_with_credentials(
    bucket: &str,
    endpoint: Option<&str>,
    region: &str,
    managed: Option<&crate::control_plane::ManagedStorageConfig>,
) -> anyhow::Result<Bucket> {
    open(bucket, endpoint, region, managed, None)
}

fn open(
    bucket: &str,
    endpoint: Option<&str>,
    region: &str,
    managed: Option<&crate::control_plane::ManagedStorageConfig>,
    app: Option<&str>,
) -> anyhow::Result<Bucket> {
    let credentials = managed.map(|managed| crate::bucket::StaticCredentials {
        access_key_id: managed.access_key_id.clone(),
        secret_access_key: managed.secret_access_key.clone(),
        session_token: managed.session_token.clone(),
    });
    Bucket::open(bucket, endpoint, region, credentials, app)
}

pub async fn validate_bucket(bucket: &Bucket) -> anyhow::Result<()> {
    bucket.validate().await.with_context(|| {
        format!(
            "bucket unavailable or inaccessible: {}://{}",
            bucket.scheme(),
            bucket.name
        )
    })
}

/// Validate storage issued by the Managed Control Plane and preserve the
/// operator-visible failure vocabulary. Newly issued provider credentials can
/// take a moment to propagate, so only the final rejection is authoritative.
pub async fn validate_managed_bucket(bucket: &Bucket) -> anyhow::Result<()> {
    const RETRIES: u32 = 5;
    for attempt in 1..=RETRIES {
        match validate_managed_bucket_once(bucket, attempt == RETRIES).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt == RETRIES => return Err(error),
            Err(_) => {
                info!(
                    bucket = %bucket.name,
                    attempt,
                    "storage credential not accepted yet; retrying"
                );
                tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
            }
        }
    }
    unreachable!("loop returns on the final attempt")
}

async fn validate_managed_bucket_once(bucket: &Bucket, report: bool) -> anyhow::Result<()> {
    match bucket.validate().await {
        Ok(()) => Ok(()),
        Err(error) if crate::bucket::is_unauthorized(&error) => {
            if report {
                crate::control_plane::report_managed_runtime_state(
                    crate::control_plane::ManagedRuntimeState::CredentialRevoked,
                );
                bail!(
                    "managed storage credential was rejected or revoked for {}://{}",
                    bucket.scheme(),
                    bucket.name
                );
            }
            bail!(
                "managed storage credential was not accepted yet for {}://{}",
                bucket.scheme(),
                bucket.name
            );
        }
        Err(error) => {
            if report {
                crate::control_plane::report_managed_runtime_state(
                    crate::control_plane::ManagedRuntimeState::BucketUnavailable,
                );
            }
            Err(error).with_context(|| {
                format!(
                    "bucket unavailable or inaccessible: {}://{}",
                    bucket.scheme(),
                    bucket.name
                )
            })
        }
    }
}

/// Test the one bucket capability a fleet cannot run without.
///
/// A list proves the bucket answers a request. It does not prove the
/// store keeps the conditional write, and some stores accept the
/// precondition header and then ignore it. A fleet on such a store loses
/// a cell to two owners, or dies in a self-fence loop, minutes after an
/// operator read `ok bucket`. So diagnose provokes the rejections a
/// conforming store must produce.
///
/// The probe writes and deletes one small object, which a read-only
/// credential cannot do; `--read-only` skips it for that operator.
async fn probe_storage(out: &mut Output, bucket: &Bucket, read_only: bool) -> anyhow::Result<()> {
    if read_only {
        out.row(&Check::skip("bucket write probe", "--read-only"))?;
        return Ok(());
    }
    match bucket.probe_cas().await {
        Ok(()) => {
            out.row(&Check::ok(
                "bucket conditional write",
                "create, reject-create, update, reject-stale",
            ))?;
            Ok(())
        }
        Err(error) => {
            let detail = if crate::bucket::is_unauthorized(&error) {
                "the credential cannot write to the bucket, and a celld node requires write \
                 access; pass --read-only to diagnose with a read-only credential"
                    .to_string()
            } else {
                format!("{error:#}")
            };
            out.row(&Check::fail("bucket conditional write", detail))?;
            bail!("bucket failed the storage conformance probe")
        }
    }
}

/// Test the required conditional writes and ranged reads before a node serves,
/// and refuse to start when the store proves it cannot support celld.
///
/// The two probe outcomes get different answers on purpose. A violation
/// is a property of the store: it never clears. A node that serves anyway can
/// share a cell with another owner or fail when it reads stored cell data.
/// Refusing to start reports the permanent problem before either failure.
///
/// An ambiguous error is not that. `put_cas` runs with transport retries off,
/// so a slow or dropped connection answers `Err`. The complete probe retries
/// with a fresh key twice, then preserves the existing warn-and-serve result.
///
/// `managed` reports the refusal to the Managed Control Plane, the way
/// [`validate_managed_bucket`] reports its own failures. Without it an
/// enrolled installation that refuses to serve leaves only local stderr
/// behind, so the operator who most needs the reason cannot read it.
pub async fn probe_storage_before_serving(bucket: &Bucket, managed: bool) -> anyhow::Result<()> {
    const ATTEMPTS: usize = 3;
    for attempt in 1..=ATTEMPTS {
        match bucket.probe_startup_storage_steps().await {
            Ok(StorageProbeVerdict::Conformant) => return Ok(()),
            Ok(StorageProbeVerdict::Violation(reason)) => {
                if managed {
                    crate::control_plane::report_managed_runtime_state(
                        crate::control_plane::ManagedRuntimeState::StorageContractViolated,
                    );
                }
                bail!(
                    "the bucket does not keep the storage contract, so celld cannot serve cells \
                     safely on it: {reason}. Set CELLD_STORAGE_PROBE=0 to start without this test"
                )
            }
            Err(error) if attempt < ATTEMPTS => {
                tracing::warn!(
                    attempt,
                    attempts = ATTEMPTS,
                    error = format!("{error:#}"),
                    "could not verify the bucket storage contract; retrying"
                );
            }
            Err(error) => {
                tracing::warn!(
                    attempts = ATTEMPTS,
                    error = format!("{error:#}"),
                    "could not verify the bucket storage contract; starting anyway"
                );
                return Ok(());
            }
        }
    }
    unreachable!("the storage probe loop always returns on its last attempt")
}

pub async fn diagnose(
    bucket: &Bucket,
    peers: Vec<String>,
    unsafe_public_advertise: bool,
    read_only: bool,
    json: bool,
    // Checks the caller already made, printed through this command's one
    // `Output`. The listener binds happen before a bucket is even opened,
    // and printing them from there put three text lines in front of the
    // JSON, which broke `celld diagnose --json | jq` at its first line.
    preamble: Vec<Check>,
) -> anyhow::Result<()> {
    let mut out = Output::new(if json { Format::Json } else { Format::Text });
    for check in &preamble {
        out.row(check)?;
    }
    // Every verdict is flushed before the exit status is decided, so a
    // failing diagnosis still leaves the operator the checks that passed.
    let verdict =
        diagnose_checks(&mut out, bucket, peers, unsafe_public_advertise, read_only).await;
    out.finish()?;
    verdict
}

async fn diagnose_checks(
    out: &mut Output,
    bucket: &Bucket,
    peers: Vec<String>,
    unsafe_public_advertise: bool,
    read_only: bool,
) -> anyhow::Result<()> {
    validate_bucket(bucket).await?;
    out.row(&Check::ok(
        format!("bucket {}://{}", bucket.scheme(), bucket.name),
        "",
    ))?;
    // A store that cannot fence makes every peer result moot, so the
    // storage verdict comes before the fleet walk.
    probe_storage(out, bucket, read_only).await?;

    let enumerated = peers.is_empty();
    let peers = if enumerated {
        let peers = node_lease_ids(bucket).await?;
        out.row(&Check::ok(
            "fleet",
            format!("{} node lease(s) enumerated", peers.len()),
        ))?;
        peers
    } else {
        peers
    };
    if peers.is_empty() {
        return Ok(());
    }
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build peer diagnostic client")?;
    let auth = crate::peer_auth::PeerAuth::new(
        crate::peer_auth::load_existing(bucket).await?,
        "diagnostic",
    )?;
    let mut failures = 0_usize;
    let mut expired = 0_usize;
    for peer in peers {
        let node = match live_node_lease(bucket, &peer).await {
            Ok(Some(node)) => node,
            Ok(None) if enumerated => {
                expired += 1;
                out.row(&Check::skip(format!("peer {peer}"), "lease is expired"))?;
                continue;
            }
            Ok(None) => {
                failures += 1;
                out.row(&Check::fail(
                    format!("peer {peer}"),
                    format!("node {peer} lease is expired"),
                ))?;
                continue;
            }
            Err(error) => {
                failures += 1;
                out.row(&Check::fail(format!("peer {peer}"), format!("{error}")))?;
                continue;
            }
        };
        let advertise = match crate::startup::parse_advertise(&node.addr) {
            Ok(advertise) => advertise,
            Err(error) => {
                failures += 1;
                out.row(&Check::fail(
                    format!("peer {peer}"),
                    format!("malformed advertise address {:?}: {error}", node.addr),
                ))?;
                continue;
            }
        };
        if advertise.is_public_ip() && !unsafe_public_advertise {
            failures += 1;
            out.row(&Check::fail(
                format!("peer {peer}"),
                format!(
                    "unsafe public advertise address {}; use a private overlay or \
                     --unsafe-public-advertise",
                    node.addr
                ),
            ))?;
            continue;
        }
        if let Err(error) = crate::peer_probe::probe(&http, &node, &auth).await {
            failures += 1;
            out.row(&Check::fail(
                format!("peer {peer} at {}", node.addr),
                format!("{error}"),
            ))?;
            continue;
        }
        let load_age_ms = if node.load.sampled_ms == 0 {
            "unknown".to_string()
        } else {
            crate::ownership_store::now_ms()
                .saturating_sub(node.load.sampled_ms)
                .to_string()
        };
        // A 1-byte RSS is the sentinel a platform without /proc leaves behind,
        // not a measurement. Report it the way the load age already reports a
        // missing sample, so no operator reads it as a real number.
        let rss_bytes = if node.load.rss_bytes <= 1 {
            "unknown".to_string()
        } else {
            node.load.rss_bytes.to_string()
        };
        // The shedding decision reads the in-use figure, not the resident set
        // size, so a diagnosis that prints only the latter cannot explain why
        // a node sheds. A node from before this field reports nothing, which
        // is not the same as zero.
        let in_use_bytes = node
            .load
            .in_use_bytes
            .map_or_else(|| "unknown".to_string(), |bytes| bytes.to_string());
        let owned_cells = node
            .load
            .owned_cells
            .map_or_else(|| "unknown".to_string(), |cells| cells.to_string());
        out.row(&Check::ok(
            format!("peer {} at {}", node.node, node.addr),
            format!(
                "(signed direct probe) protocol={} owned_cells={} resident_cells={} \
                 websockets={} rss_bytes={} in_use_bytes={} cpu_percent={:.2} fds={}/{} \
                 pressured={} shed_cells={} restoring={} load_age_ms={}",
                node.peer_protocol,
                owned_cells,
                node.load.resident_cells,
                node.load.host_websockets,
                rss_bytes,
                in_use_bytes,
                node.load.cpu_percent_x100 as f64 / 100.0,
                node.load.open_fds,
                node.load.fd_limit,
                node.load.pressured,
                node.load.shed_cells,
                node.load.restoring,
                load_age_ms,
            ),
        ))?;
    }
    if expired > 0 {
        out.row(&Check::ok(
            "fleet",
            format!("skipped {expired} expired node lease(s)"),
        ))?;
    }
    if failures > 0 {
        bail!("fleet diagnostics failed for {failures} peer(s)");
    }
    Ok(())
}

pub(crate) async fn live_node_lease(
    bucket: &Bucket,
    peer: &str,
) -> anyhow::Result<Option<NodeLeaseWire>> {
    let key = format!("nodes/{peer}.json");
    let node: NodeLeaseWire = serde_json::from_str(&get_string(bucket, &key).await?)
        .with_context(|| format!("decode {}://{}/{key}", bucket.scheme(), bucket.name))?;
    if node.node != peer {
        bail!(
            "node lease {key} identifies unexpected node {:?}",
            node.node
        );
    }
    if node.expires_ms <= crate::ownership_store::now_ms() {
        return Ok(None);
    }
    if node.addr.is_empty() {
        bail!("node {peer} lease has no advertised address");
    }
    Ok(Some(node))
}

pub(crate) async fn node_lease_ids(bucket: &Bucket) -> anyhow::Result<Vec<String>> {
    let mut nodes = Vec::new();
    for object in bucket
        .list("nodes/")
        .await
        .context("enumerate node leases")?
    {
        let Some(node) = object
            .location
            .as_ref()
            .strip_prefix("nodes/")
            .and_then(|key| key.strip_suffix(".json"))
        else {
            continue;
        };
        if !node.is_empty() {
            nodes.push(node.to_string());
        }
    }
    nodes.sort();
    nodes.dedup();
    Ok(nodes)
}

pub(crate) fn resolve_storage_location(
    bucket: &mut Option<String>,
    endpoint: &mut Option<String>,
    region: Option<&str>,
) -> String {
    let env = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };
    if bucket.is_none() {
        *bucket = crate::cli_options::bucket_from_environment();
    }
    if endpoint.is_none() {
        *endpoint = env("S3_ENDPOINT");
    }
    region
        .map(str::to_string)
        .or_else(|| env("AWS_REGION"))
        .or_else(|| env("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|| "us-east-1".to_string())
}

#[cfg(all(test, celld_internal_tests))]
mod internal_tests {
    include!(env!("CELLD_INTERNAL_FLEET_TESTS"));
}

pub async fn run_deploy(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(mut options) = deploy::options_from_arguments(arguments)? else {
        deploy::print_help();
        return Ok(());
    };
    let region = resolve_storage_location(
        &mut options.bucket,
        &mut options.endpoint,
        options.region.as_deref(),
    );
    if !options.dry_run && options.bucket.is_none() {
        bail!(
            "celld deploy requires --bucket s3://NAME, gs://NAME or az://CONTAINER \
             (or CELLD_BUCKET)"
        );
    }
    let built = deploy::build(&options)?;
    built.report();
    let mut out = Output::new(if options.json {
        Format::Json
    } else {
        Format::Text
    });
    if options.dry_run {
        out.row(&Deployed {
            worker: built.script_name.clone(),
            version: built.version.clone(),
            location: String::new(),
            dry_run: true,
        })?;
        return out.finish();
    }

    let bucket = options.bucket.expect("validated deployment bucket");
    let store = bucket_client(&bucket, options.endpoint.as_deref(), &region)?;
    validate_bucket(&store).await?;
    let started = std::time::Instant::now();
    deploy::write(&store, &built).await?;
    let location = format!(
        "{}://{}/{}{}",
        store.scheme(),
        store.name,
        store.prefix,
        built.prefix
    );
    note!(
        "Uploaded {} ({:.2} sec)",
        built.script_name,
        started.elapsed().as_secs_f64()
    );
    note!("  {location}");
    out.row(&Deployed {
        worker: built.script_name.clone(),
        version: built.version.clone(),
        location,
        dry_run: false,
    })?;
    note!("Nodes adopt this version at their next pointer poll, without a restart.");
    out.finish()
}

async fn get_string(bucket: &Bucket, key: &str) -> anyhow::Result<String> {
    String::from_utf8(get_bytes(bucket, key).await?.into())
        .context("deployment module is not UTF-8")
}

async fn get_bytes(bucket: &Bucket, key: &str) -> anyhow::Result<bytes::Bytes> {
    let (bytes, _) = bucket.get(key).await?.with_context(|| {
        format!(
            "read {}://{}/{key}: no such key",
            bucket.scheme(),
            bucket.name
        )
    })?;
    Ok(bytes)
}

/// The fleet-wide pointer: the bucket's only application commit point.
pub const CURRENT_POINTER_KEY: &str = "deploy/current.json";

pub async fn load_current_worker(
    bucket: &Bucket,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    load_worker_from_pointer(bucket, CURRENT_POINTER_KEY, node).await
}

/// Read the fleet-wide pointer without loading what it names. The watcher
/// compares it with the serving generation before it loads anything.
pub async fn read_current_pointer(bucket: &Bucket) -> anyhow::Result<DeployPointer> {
    serde_json::from_str(&get_string(bucket, CURRENT_POINTER_KEY).await?)
        .with_context(|| format!("decode {CURRENT_POINTER_KEY}"))
}

pub async fn load_named_worker(
    bucket: &Bucket,
    script: &str,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    load_worker_from_pointer(bucket, &format!("deploy/{script}/current.json"), node).await
}

pub async fn load_queue_consumer_attachment(
    bucket: &Bucket,
    queue: &str,
) -> anyhow::Result<Option<crate::protocol::QueueConsumerDeployment>> {
    let key = crate::protocol::queue_consumer_attachment_key(queue)
        .with_context(|| format!("invalid queue name {queue:?} in deployment manifest"))?;
    let Some((body, _)) = bucket.get(&key).await? else {
        return Ok(None);
    };
    let attachment: crate::protocol::QueueConsumerAttachment = serde_json::from_slice(&body)
        .with_context(|| format!("decode queue consumer attachment {key}"))?;
    crate::protocol::validate_queue_consumer_attachment(queue, &attachment)?;
    Ok(attachment.consumer)
}

pub async fn load_queue_consumer_worker(
    bucket: &Bucket,
    queue: &str,
    consumer: &crate::protocol::QueueConsumerDeployment,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    let pointer = DeployPointer {
        script_name: Some(consumer.script_name.clone()),
        version: consumer.version.clone(),
        prefix: consumer.prefix.clone(),
        rollout: Rollout { percent: 100 },
    };
    let pointer_key = format!("deploy/{}/current.json", consumer.script_name);
    let loaded = load_worker_at_pointer(bucket, pointer, &pointer_key, node).await?;
    anyhow::ensure!(
        loaded
            .options
            .queue_consumers
            .iter()
            .any(|config| config.queue == queue),
        "queue {queue:?} attachment resolved deployment {} of script {:?}, which does not consume that queue",
        consumer.version,
        consumer.script_name
    );
    Ok(loaded)
}

async fn load_worker_from_pointer(
    bucket: &Bucket,
    pointer_key: &str,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    let pointer: DeployPointer = serde_json::from_str(&get_string(bucket, pointer_key).await?)
        .with_context(|| format!("decode {pointer_key}"))?;
    load_worker_at_pointer(bucket, pointer, pointer_key, node).await
}

async fn load_worker_at_pointer(
    bucket: &Bucket,
    pointer: DeployPointer,
    pointer_key: &str,
    node: String,
) -> anyhow::Result<LoadedDeployment> {
    let manifest: Manifest = serde_json::from_str(
        &get_string(bucket, &format!("{}/manifest.json", pointer.prefix)).await?,
    )
    .context("decode deployment manifest")?;
    anyhow::ensure!(
        manifest.version == pointer.version,
        "deployment pointer for version {:?} resolved manifest version {:?}",
        pointer.version,
        manifest.version
    );
    if let Some(script_name) = &pointer.script_name {
        anyhow::ensure!(
            manifest.script_name == *script_name,
            "deployment pointer for script {script_name:?} resolved script {:?}",
            manifest.script_name
        );
    }
    crate::protocol::validate_required_features(&manifest.required_features)?;
    crate::protocol::validate_queue_manifest(&manifest)?;
    let prefix = &pointer.prefix;
    let mut fetched =
        futures_util::future::try_join_all(manifest.modules.iter().map(|module| async move {
            let key = format!("{prefix}/{}", module.name);
            let bytes = get_bytes(bucket, &key).await?;
            module.verify(&bytes)?;
            anyhow::Ok((module, bytes))
        }))
        .await?;
    let src = match manifest.main_module.as_deref() {
        Some(main) => {
            let position = fetched
                .iter()
                .position(|(module, _)| module.name == main)
                .with_context(|| {
                    format!("main module {main:?} is absent from the manifest module list")
                })?;
            let (_, bytes) = fetched.remove(position);
            String::from_utf8(bytes.into()).context("deployment main module is not UTF-8")?
        }
        None if manifest.assets.is_some() => {
            // Ingress is handled by the immutable asset resolver. Keeping a
            // synthetic Worker makes the runtime construction path uniform
            // and is a fail-closed guard if an asset-only request escapes it.
            "export default { fetch() { return new Response('Not found', { status: 404 }); } };"
                .to_string()
        }
        None => bail!("deployment has neither a main module nor assets"),
    };
    let mut modules = Vec::new();
    for (module, bytes) in fetched {
        let entry = match module.kind {
            Some(ModuleKind::Wasm) => (module.name.clone(), ModuleSource::Wasm(bytes)),
            None => (
                format!("./{}", module.name),
                ModuleSource::Text(
                    String::from_utf8(bytes.into()).context("deployment module is not UTF-8")?,
                ),
            ),
        };
        modules.push(entry);
    }
    let do_bindings = bindings(&manifest, "durable_object_namespace")
        .filter_map(|binding| {
            Some((
                binding.get("name")?.as_str()?.to_string(),
                binding.get("class_name")?.as_str()?.to_string(),
            ))
        })
        .collect();
    // (env name, bucket name). The bucket name is the key space the
    // binding owns inside the fleet bucket; see [[r2]].
    let r2_bindings = bindings(&manifest, "r2_bucket")
        .filter_map(|binding| {
            Some((
                binding.get("name")?.as_str()?.to_string(),
                binding.get("bucket_name")?.as_str()?.to_string(),
            ))
        })
        .collect();
    // (env name, stable database identity). A Cloudflare database_id survives
    // Worker renames and lets several Workers share the resource. A celld-only
    // config without one falls back to the database name.
    let d1_bindings = bindings(&manifest, "d1")
        .filter_map(|binding| {
            Some((
                binding.get("name")?.as_str()?.to_string(),
                binding
                    .get("database_id")
                    .or_else(|| binding.get("database_name"))?
                    .as_str()?
                    .to_string(),
            ))
        })
        .collect();
    // (env name, namespace identity). The identity is the config's `id`
    // verbatim: upstream's `kv_namespaces` has no human-readable name field, so
    // there is no second key to fall back to and none is invented.
    let kv_bindings = bindings(&manifest, "kv")
        .filter_map(|binding| {
            Some((
                binding.get("name")?.as_str()?.to_string(),
                binding.get("id")?.as_str()?.to_string(),
            ))
        })
        .collect();
    let queue_bindings = bindings(&manifest, "queue")
        .filter_map(|binding| {
            Some(crate::js::QueueBinding {
                environment: binding.get("name")?.as_str()?.to_string(),
                queue: binding.get("queue")?.as_str()?.to_string(),
                delivery_delay: binding
                    .get("delivery_delay")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                    .unwrap_or(0),
            })
        })
        .collect();
    let workflow_bindings = bindings(&manifest, "workflow")
        .filter_map(|binding| {
            Some(crate::js::WorkflowBinding {
                environment: binding.get("name")?.as_str()?.to_string(),
                workflow: binding.get("workflow_name")?.as_str()?.to_string(),
                class: binding.get("class_name")?.as_str()?.to_string(),
            })
        })
        .collect();
    let ai_binding = configured_ai_binding(
        bindings(&manifest, "ai")
            .find_map(|binding| binding.get("name")?.as_str().map(str::to_string)),
    );
    let services = service_bindings(&manifest);
    let vars = worker_vars(&manifest)?;
    let compat = crate::worker_compat(&manifest.raw_metadata);
    let assets = match &manifest.assets {
        Some(reference) => Some(
            crate::assets::AssetResolver::load(
                bucket,
                &pointer.prefix,
                reference,
                manifest.main_module.is_none(),
                // The resolver can consult this pointer again later: a
                // rolling restart serves new HTML from upgraded nodes while
                // this process still holds the boot index, and the fallback
                // is what keeps the new content-hashed assets servable
                // everywhere (denoland/celld#161).
                Some(crate::assets::FallbackSource {
                    pointer_key: pointer_key.to_string(),
                    boot_prefix: pointer.prefix.clone(),
                }),
            )
            .await?,
        ),
        None => None,
    };
    let asset_binding = assets
        .as_ref()
        .and_then(crate::assets::AssetResolver::binding_name)
        .map(str::to_string);
    let script_name = manifest.script_name.clone();
    let version = manifest.version.clone();
    let prefix = pointer.prefix.clone();
    let crons = manifest.crons.clone();
    Ok(LoadedDeployment {
        options: WorkerConfigOptions {
            src,
            script_name: script_name.clone(),
            do_classes: manifest.do_classes,
            bindings: do_bindings,
            r2_bindings,
            d1_bindings,
            kv_bindings,
            queue_bindings,
            queue_consumers: manifest.queue_consumers,
            workflow_bindings,
            ai_binding,
            vars,
            node,
            modules,
            compat,
        },
        script_name,
        version,
        prefix,
        asset_binding,
        assets,
        services,
        crons,
    })
}

/// Apply celld's manifest-first precedence for the optional AI binding.
pub fn configured_ai_binding(manifest_binding: Option<String>) -> Option<String> {
    manifest_binding
        .or_else(|| std::env::var("CELLD_AI_BINDING").ok())
        .or_else(|| std::env::var_os("CELLD_AI_URL").map(|_| "AI".to_string()))
}

pub struct LoadedDeployment {
    pub options: WorkerConfigOptions,
    pub script_name: String,
    pub version: String,
    pub prefix: String,
    pub asset_binding: Option<String>,
    pub assets: Option<crate::assets::AssetResolver>,
    pub services: Vec<(String, String, Option<String>)>,
    /// `triggers.crons` from the manifest, driving the reserved cron cell.
    pub crons: Vec<String>,
}

fn bindings<'a>(
    manifest: &'a Manifest,
    kind: &'a str,
) -> impl Iterator<Item = &'a serde_json::Value> {
    manifest
        .raw_metadata
        .get("bindings")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(move |binding| {
            binding.get("type").and_then(serde_json::Value::as_str) == Some(kind)
        })
}

fn service_bindings(manifest: &Manifest) -> Vec<(String, String, Option<String>)> {
    bindings(manifest, "service")
        .filter_map(|binding| {
            Some((
                binding.get("name")?.as_str()?.to_string(),
                binding.get("service")?.as_str()?.to_string(),
                binding
                    .get("entrypoint")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            ))
        })
        .collect()
}

fn worker_vars(manifest: &Manifest) -> anyhow::Result<Vec<(String, String)>> {
    let mut vars = BTreeMap::new();
    for binding in bindings(manifest, "plain_text") {
        if let (Some(name), Some(value)) = (
            binding.get("name").and_then(serde_json::Value::as_str),
            binding.get("text").and_then(serde_json::Value::as_str),
        ) {
            vars.insert(name.to_string(), value.to_string());
        }
    }
    if let Ok(path) = std::env::var("CELLD_VARS_FILE") {
        let contents = std::fs::read_to_string(&path)
            .with_context(|| format!("read Worker vars file {path}"))?;
        for line in contents.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((name, raw)) = line.split_once('=') else {
                continue;
            };
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            let raw = raw.trim();
            let value = raw
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .or_else(|| {
                    raw.strip_prefix('\'')
                        .and_then(|value| value.strip_suffix('\''))
                })
                .unwrap_or(raw);
            vars.insert(name.to_string(), value.to_string());
        }
    }
    for (name, value) in std::env::vars() {
        if let Some(name) = name
            .strip_prefix("CELLD_VAR_")
            .filter(|name| !name.is_empty())
        {
            vars.insert(name.to_string(), value);
        }
    }
    Ok(vars.into_iter().collect())
}
