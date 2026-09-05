// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Managed control-plane sessions execute outside the Actor execution domain.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::bucket::Bucket;
use crate::note;
use crate::protocol::{asset_blob_key, AssetIndex, DeployPointer, Manifest};
use anyhow::{anyhow, Context};
use celld_logic::PresenceSnapshot;
use fastwebsockets::{Frame, OpCode};
use hyper::header::HeaderMap;
use rand::RngCore;
use rusqlite::{types::ValueRef, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tracing::{info, warn};

const DEFAULT_CONTROL_URL: &str = "https://celld.dev";
const DEFAULT_ENVIRONMENT: &str = "prod";
const MAX_PRESENCE_CELLS: usize = 50;
const MAX_PRESENCE_CELL_ID_BYTES: usize = 200;
const MAX_EXPLORER_CELL_PAGE: usize = 100;
const MAX_EXPLORER_TABLES: usize = 100;
const MAX_EXPLORER_COLUMNS: usize = 32;
const MAX_EXPLORER_ROWS: usize = 25;
const MAX_EXPLORER_VALUE_BYTES: usize = 2 * 1024;
const MAX_EXPLORER_RESPONSE_BYTES: usize = 96 * 1024;
const MAX_MANAGED_MODULES: usize = 64;
const MAX_MANAGED_MODULE_BYTES: usize = 25 * 1024 * 1024;

/// Whether a managed deployment still replaces the process image, as it did
/// before nodes adopted deployments in place. Off by default: the exec cut
/// in-flight requests and cold-restored every resident cell. Kept for one
/// release as an escape hatch, then removed.
fn restart_on_deployment_enabled() -> bool {
    crate::env_vars::flag("CELLD_CLOUD_RESTART_ON_DEPLOY", false)
        .expect("validated CELLD_CLOUD_RESTART_ON_DEPLOY")
}

/// React to a deployment the control plane says is current: nudge the
/// pointer watcher, or under the escape hatch replace the process image.
fn adopt_or_restart(reload: &crate::generation::ReloadSender) {
    if restart_on_deployment_enabled() {
        restart_for_deployment();
    }
    crate::generation::nudge(reload);
}

#[cfg(unix)]
fn restart_process(trigger: &'static str) -> ! {
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => {
            warn!(
                event = "control_plane_restart_exec_failed",
                trigger,
                %error,
                "could not locate celld for process reload; exiting for an external supervisor"
            );
            std::process::exit(75);
        }
    };
    let signer = match crate::peer_probe::reexec_signer_secret() {
        Ok(signer) => signer,
        Err(error) => {
            warn!(
                event = "control_plane_restart_exec_failed",
                trigger,
                %error,
                "could not preserve process probe identity; exiting for an external supervisor"
            );
            std::process::exit(75);
        }
    };
    let mut command = Command::new(executable);
    command
        .args(std::env::args_os().skip(1))
        .env(crate::peer_probe::REEXEC_SIGNER_ENV, signer);
    if let Some(node) = REEXEC_NODE_SESSION_ID.get() {
        command.env("CELLD_NODE", node);
    }
    let error = command.exec();
    warn!(
        event = "control_plane_restart_exec_failed",
        trigger,
        %error,
        "could not re-exec celld for process reload; exiting for an external supervisor"
    );
    std::process::exit(75);
}

#[cfg(not(unix))]
fn restart_process(_trigger: &'static str) -> ! {
    std::process::exit(75);
}

fn restart_for_deployment() -> ! {
    restart_process("deployment")
}

static REEXEC_NODE_SESSION_ID: OnceLock<String> = OnceLock::new();

pub fn install_reexec_node_session_id(node: &str) -> anyhow::Result<()> {
    REEXEC_NODE_SESSION_ID
        .set(node.to_string())
        .map_err(|_| anyhow!("re-exec node-session identity is already installed"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagedRuntimeState {
    Connected,
    ControlPlaneUnavailable,
    BucketUnavailable,
    CredentialRevoked,
    /// The bucket answers, and the credential works, but the store does not
    /// support a required conditional write or ranged read. Distinct from
    /// `BucketUnavailable`, which a retry can clear: this one never does.
    StorageContractViolated,
}

impl ManagedRuntimeState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::ControlPlaneUnavailable => "control_plane_unavailable",
            Self::BucketUnavailable => "bucket_unavailable",
            Self::CredentialRevoked => "credential_revoked",
            Self::StorageContractViolated => "storage_contract_violated",
        }
    }
}

static MANAGED_RUNTIME_STATE: OnceLock<Mutex<Option<ManagedRuntimeState>>> = OnceLock::new();

pub fn report_managed_runtime_state(state: ManagedRuntimeState) {
    let mut current = MANAGED_RUNTIME_STATE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap();
    if *current == Some(state) {
        return;
    }
    *current = Some(state);
    drop(current);
    let managed_state = state.as_str();
    match state {
        ManagedRuntimeState::Connected => info!(
            event = "managed_runtime_state",
            managed_state, "Managed Control Plane connected"
        ),
        ManagedRuntimeState::ControlPlaneUnavailable => info!(
            event = "managed_runtime_state",
            managed_state, "Managed Control Plane unavailable; bucket-backed serving continues"
        ),
        ManagedRuntimeState::BucketUnavailable => warn!(
            event = "managed_runtime_state",
            managed_state, "fleet bucket unavailable; serving cannot start"
        ),
        // Name the remedy: this condition can be cleared by a restart or an
        // explicit credential refresh.
        ManagedRuntimeState::CredentialRevoked => warn!(
            event = "managed_runtime_state",
            managed_state,
            "managed storage credential was rejected; restart celld to fetch a \
             fresh one, or run `celld credentials refresh` if a rotation is pending"
        ),
        // No remedy on this node: the store lacks a required operation, so a
        // restart repeats the refusal. Name the two exits an operator has.
        ManagedRuntimeState::StorageContractViolated => warn!(
            event = "managed_runtime_state",
            managed_state,
            "the fleet bucket does not support a storage operation celld requires, \
             so serving cannot start; move the fleet to a compatible store, or set \
             CELLD_STORAGE_PROBE=0 to start without this test"
        ),
    }
}

#[derive(Debug)]
struct ManagedControlCredentialRevoked;

impl std::fmt::Display for ManagedControlCredentialRevoked {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "Managed Control Plane credential was rejected or revoked.\n\
             Restart celld to fetch a fresh credential. If that does not help, \
             run `celld credentials refresh`, and `celld diagnose` to check \
             bucket access.",
        )
    }
}

impl std::error::Error for ManagedControlCredentialRevoked {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ConnectOptions {
    control_url: String,
    environment: String,
    force: bool,
    byo_storage: Option<ByoStorageConfig>,
}

#[derive(Clone, Deserialize, Serialize)]
struct CloudConfig {
    #[serde(default = "installation_schema_version")]
    version: u32,
    #[serde(alias = "instance_id")]
    installation_id: String,
    #[serde(alias = "instance_token")]
    installation_token: Option<String>,
    #[serde(default = "initial_credential_version")]
    credential_version: u64,
    fleet_id: Option<String>,
    environment_id: Option<String>,
    control_url: String,
    environment: String,
    #[serde(default)]
    storage: Option<ManagedStorageConfig>,
    #[serde(default)]
    byo_storage: Option<ByoStorageConfig>,
    #[serde(default)]
    credential_handoff_id: Option<String>,
    #[serde(default)]
    previous_credential: Option<PreviousCredential>,
    #[serde(default)]
    pending_claim: Option<PendingClaim>,
}

#[derive(Clone, Deserialize, Serialize)]
struct PreviousCredential {
    installation_token: String,
    credential_version: u64,
    storage: ManagedStorageConfig,
}

#[derive(Clone, Deserialize, Serialize)]
struct PendingClaim {
    verification_uri_complete: String,
    expires_at_ms: u64,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ManagedStorageConfig {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ByoStorageConfig {
    pub bucket: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    pub region: String,
}

#[derive(Clone)]
pub enum InstallationStorageConfig {
    Managed(ManagedStorageConfig),
    Byo(ByoStorageConfig),
}

#[derive(Serialize)]
struct CreateClaimRequest<'a> {
    instance_id: &'a str,
    instance_name: &'a str,
    environment: &'a str,
    version: &'static str,
    platform: String,
    storage_origin: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    bucket: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<&'a str>,
}

#[derive(Deserialize)]
struct CreateClaimResponse {
    device_code: String,
    verification_uri_complete: String,
    expires_in: u64,
    interval: u64,
}

#[derive(Serialize)]
struct PollClaimRequest<'a> {
    device_code: &'a str,
}

#[derive(Deserialize)]
struct PollClaimResponse {
    status: String,
    #[serde(default = "managed_storage_origin")]
    storage_origin: String,
    credential_handoff_id: Option<String>,
    credential_version: Option<u64>,
    instance_token: Option<String>,
    fleet_id: Option<String>,
    environment_id: Option<String>,
    bucket: Option<String>,
    endpoint: Option<String>,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    interval: Option<u64>,
}

#[derive(Deserialize)]
struct CredentialRotationResponse {
    status: String,
    credential_handoff_id: String,
    credential_version: u64,
    instance_token: String,
    bucket: String,
    endpoint: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[derive(Deserialize)]
struct AgentCommandEnvelope {
    command: AgentCommand,
}

#[derive(Deserialize)]
struct AgentCommand {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    deployment: AgentDeployment,
}

#[derive(Deserialize)]
#[doc(hidden)]
pub struct AgentDeployment {
    pub script_name: String,
    pub version: String,
    pub manifest: Manifest,
    pub pointer: DeployPointer,
    pub modules: Vec<AgentModule>,
    #[serde(default)]
    pub assets: Option<AgentAssets>,
}

#[derive(Deserialize)]
#[doc(hidden)]
pub struct AgentModule {
    pub name: String,
    pub sha256: String,
    pub download_url: String,
}

#[derive(Deserialize)]
pub struct AgentAssets {
    pub index_download_url: String,
    pub blob_download_base_url: String,
    pub sha256: String,
    pub file_count: u32,
    pub total_bytes: u64,
}

#[derive(Serialize)]
struct CompleteCommandRequest<'a> {
    success: bool,
    error: Option<&'a str>,
}

struct AppliedDeployment {
    id: String,
    script_name: String,
}

pub async fn handle_connect_command(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<()> {
    let mut options = options_from_env()?;
    let mut args = arguments.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--env" => {
                options.environment = args.next().context("--env requires a value")?;
            }
            "--control-url" => {
                options.control_url = args.next().context("--control-url requires a value")?;
            }
            "--force" => options.force = true,
            "--help" | "-h" => {
                if let Some(argument) = args.next() {
                    return Err(anyhow!(
                        "unexpected argument after connect --help: {argument}"
                    ));
                }
                print_connect_help();
                return Ok(());
            }
            _ => return Err(anyhow!("unknown connect option: {argument}")),
        }
    }
    validate_environment(&options.environment)?;
    connect(options, true).await
}

/// `celld token` — print the fleet's deploy token and the exact commands to
/// use it.
///
/// Wrangler expects deployment credentials in environment variables, so this
/// command exposes the connected fleet's short-lived token in a shell-friendly
/// form.
pub async fn handle_token_command(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<()> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    if matches!(arguments.as_slice(), [a] if a == "--help" || a == "-h") {
        // Help is prose an operator redirects on purpose, so it is stdout.
        return crate::cli_output::Output::new(crate::cli_output::Format::Text).help(
            "Print this fleet's deploy token, and how to deploy with it.\n\n\
             USAGE:\n  celld token [--format shell]\n\n\
             OPTIONS:\n  --format shell  Print only shell exports, suitable for `eval`.\n",
        );
    }
    let shell_only = match arguments.as_slice() {
        [] => false,
        [format, value] if format == "--format" && value == "shell" => true,
        _ => {
            return Err(anyhow!(
                "expected `celld token` or `celld token --format shell`; \
                 run `celld token --help` for usage"
            ))
        }
    };
    let path = config_path()?;
    let contents = std::fs::read_to_string(&path).with_context(|| {
        format!(
            "read {}. This celld installation is not enrolled yet — run `celld` \
             and approve the link it prints.",
            path.display()
        )
    })?;
    let config: CloudConfig =
        serde_json::from_str(&contents).with_context(|| format!("decode {}", path.display()))?;
    let token = config.installation_token.as_deref().context(
        "this celld installation is not enrolled in the Managed Control Plane — \
         run `celld` and approve the link it prints",
    )?;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/agent/deploy-token", config.control_url))
        .bearer_auth(token)
        .send()
        .await
        .context("request a deploy token")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "could not get a deploy token ({status}): {body}\n\
             If this installation was disconnected, run `celld` to re-enrol."
        ));
    }
    #[derive(serde::Deserialize)]
    struct DeployTokenResponse {
        token: String,
        account_id: String,
        api_base_url: String,
    }
    let issued: DeployTokenResponse = response.json().await.context("decode deploy token")?;
    let exports = render_deploy_token(&issued.token, &issued.account_id, &issued.api_base_url);
    if !shell_only {
        // Say what to do next, not merely what happened -- on stderr, so
        // that `eval "$(celld token)"` sees only the exports.
        note!("Deploy token for this fleet:\n\n  {}\n", issued.token);
        note!("Deploy your app with these exports, then `npx wrangler@4 deploy`:\n");
    }
    crate::cli_output::Output::new(crate::cli_output::Format::Text).bytes(exports.as_bytes())
}

fn render_deploy_token(token: &str, account_id: &str, api_base_url: &str) -> String {
    format!(
        "export CLOUDFLARE_API_TOKEN={}\n\
         export CLOUDFLARE_ACCOUNT_ID={}\n\
         export CLOUDFLARE_API_BASE_URL={}\n",
        shell_quote(token),
        shell_quote(account_id),
        shell_quote(api_base_url),
    )
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

pub async fn handle_credentials_command(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<()> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    if matches!(arguments.as_slice(), [argument] if argument == "--help" || argument == "-h") {
        print_credentials_help();
        return Ok(());
    }
    if !matches!(arguments.as_slice(), [command] if command == "refresh") {
        return Err(anyhow!(
            "expected `celld credentials refresh`; run `celld credentials --help` for usage"
        ));
    }
    refresh_credentials().await
}

pub async fn handle_disconnect_command(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<()> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    if matches!(arguments.as_slice(), [argument] if argument == "--help" || argument == "-h") {
        print_disconnect_help();
        return Ok(());
    }
    if !matches!(arguments.as_slice(), [confirmation] if confirmation == "--yes") {
        return Err(anyhow!(
            "disconnect revokes every process sharing this installation; rerun `celld disconnect --yes` to confirm"
        ));
    }
    let path = config_path()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let archive = disconnect_at_path(path, &client).await?;
    note!(
        "Disconnected this installation from the Managed Control Plane.\nArchived its revoked local record at {}.",
        archive.display()
    );
    Ok(())
}

async fn disconnect_at_path(path: PathBuf, client: &reqwest::Client) -> anyhow::Result<PathBuf> {
    let _lock = acquire_enrollment_lock(&path).await?;
    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let config: CloudConfig =
        serde_json::from_str(&contents).with_context(|| format!("decode {}", path.display()))?;
    let token = config
        .installation_token
        .as_deref()
        .context("this celld installation is not enrolled in the Managed Control Plane")?;
    let response = client
        .post(format!(
            "{}/api/agent/credentials/revoke",
            config.control_url
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({ "installation_id": config.installation_id }))
        .send()
        .await
        .context("revoke managed installation")?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "managed installation revocation returned {}",
            response.status()
        ));
    }

    // Keep a recoverable, permission-preserving record rather than deleting
    // secrets in place. The provider credential is already revoked, and bare
    // `celld` will create a fresh installation record on its next start.
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .context("managed installation path has no UTF-8 file name")?;
    let archive = path.with_file_name(format!("{file_name}.disconnected-{}", current_time_ms()));
    std::fs::rename(&path, &archive).with_context(|| {
        format!(
            "archive revoked installation record as {}",
            archive.display()
        )
    })?;
    Ok(archive)
}

async fn refresh_credentials() -> anyhow::Result<()> {
    let path = config_path()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    refresh_credentials_at_path(path, &client).await
}

async fn refresh_credentials_at_path(
    path: PathBuf,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    let _lock = acquire_enrollment_lock(&path).await?;
    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let mut config: CloudConfig =
        serde_json::from_str(&contents).with_context(|| format!("decode {}", path.display()))?;
    let token = config
        .installation_token
        .as_deref()
        .context("this celld installation is not enrolled in the Managed Control Plane")?;
    let current_storage = config
        .storage
        .as_ref()
        .context("managed installation record has no storage credentials")?;

    if let Some(handoff_id) = config.credential_handoff_id.clone() {
        acknowledge_credential_handoff(client, &config, &handoff_id)
            .await
            .context("finish previously saved credential refresh")?;
        config.credential_handoff_id = None;
        config.previous_credential = None;
        save_config(&path, &config)?;
        note!(
            "Managed credential refresh completed (version {}).",
            config.credential_version
        );
        return Ok(());
    }

    let response = client
        .post(format!("{}/api/agent/credentials/next", config.control_url))
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("request staged managed credential")?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        note!("No managed credential rotation is pending.");
        return Ok(());
    }
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        return Err(anyhow!(
            "this installation credential is revoked; enroll a new installation"
        ));
    }
    if !response.status().is_success() {
        return Err(anyhow!(
            "credential refresh endpoint returned {}",
            response.status()
        ));
    }
    let replacement: CredentialRotationResponse = response
        .json()
        .await
        .context("decode staged managed credential")?;
    validate_credential_rotation(&replacement, &config, current_storage)?;

    config.previous_credential = Some(PreviousCredential {
        installation_token: token.to_string(),
        credential_version: config.credential_version,
        storage: current_storage.clone(),
    });
    config.installation_token = Some(replacement.instance_token);
    config.credential_version = replacement.credential_version;
    config.storage = Some(ManagedStorageConfig {
        bucket: replacement.bucket,
        endpoint: replacement.endpoint,
        region: replacement.region,
        access_key_id: replacement.access_key_id,
        secret_access_key: replacement.secret_access_key,
        session_token: replacement.session_token,
    });
    config.credential_handoff_id = Some(replacement.credential_handoff_id.clone());

    // The replacement is persisted before acknowledgement. If this process is
    // killed after the rename, the marker makes the acknowledgement retryable.
    // The managed control plane keeps both token hashes valid until this
    // acknowledgement succeeds.
    save_config(&path, &config)?;
    acknowledge_credential_handoff(client, &config, &replacement.credential_handoff_id)
        .await
        .context(
            "replacement credential is saved, but acknowledgement is pending; rerun `celld credentials refresh`",
        )?;
    config.credential_handoff_id = None;
    config.previous_credential = None;
    save_config(&path, &config)?;
    note!(
        "Managed credential refresh completed (version {}).",
        config.credential_version
    );
    Ok(())
}

fn validate_credential_rotation(
    replacement: &CredentialRotationResponse,
    config: &CloudConfig,
    current: &ManagedStorageConfig,
) -> anyhow::Result<()> {
    if replacement.status != "rotation_pending" {
        return Err(anyhow!(
            "unexpected credential refresh status: {}",
            replacement.status
        ));
    }
    if replacement.credential_version != config.credential_version.saturating_add(1) {
        return Err(anyhow!(
            "credential refresh version is not the next installation version"
        ));
    }
    if replacement.bucket != current.bucket
        || replacement.endpoint != current.endpoint
        || replacement.region != current.region
    {
        return Err(anyhow!(
            "credential refresh attempted to change the installation storage target"
        ));
    }
    for (name, value) in [
        (
            "credential handoff",
            replacement.credential_handoff_id.as_str(),
        ),
        ("installation token", replacement.instance_token.as_str()),
        ("storage access key", replacement.access_key_id.as_str()),
        ("storage secret key", replacement.secret_access_key.as_str()),
    ] {
        if value.is_empty() || value.len() > 500 {
            return Err(anyhow!("credential refresh returned an invalid {name}"));
        }
    }
    Ok(())
}

pub async fn connect_on_startup_with_storage(
    byo_storage: Option<ByoStorageConfig>,
) -> anyhow::Result<()> {
    let mut options = options_from_env()?;
    options.byo_storage = byo_storage;
    validate_environment(&options.environment)?;
    connect(options, true).await
}

pub fn installation_storage() -> anyhow::Result<InstallationStorageConfig> {
    installation_storage_with_version().map(|(storage, _)| storage)
}

pub fn installation_storage_with_version() -> anyhow::Result<(InstallationStorageConfig, u64)> {
    let config = connected_config()?.context("managed installation is not connected")?;
    let credential_version = config.credential_version;
    let storage = match (config.storage, config.byo_storage) {
        (Some(storage), None) => InstallationStorageConfig::Managed(storage),
        (None, Some(storage)) => InstallationStorageConfig::Byo(storage),
        (None, None) => {
            return Err(anyhow!("managed installation record has no storage target"));
        }
        (Some(_), Some(_)) => {
            return Err(anyhow!(
                "managed installation record contains conflicting storage targets"
            ));
        }
    };
    Ok((storage, credential_version))
}

pub type PresenceSnapshotFuture = Pin<Box<dyn Future<Output = Option<PresenceSnapshot>> + Send>>;
pub type PresenceSnapshotSource = Arc<dyn Fn() -> PresenceSnapshotFuture + Send + Sync>;

/// Production WebSocket transport around a read-only projection supplied by
/// celld-logic. The control-plane adapter cannot mutate lifecycle state or
/// maintain its own resident inventory.
pub struct PresenceRuntime {
    pub s3: Bucket,
    pub replication: Option<crate::runtime::Replication>,
    pub node_session_id: String,
    pub advertise: String,
    pub listen: String,
    /// Credential version used to construct S3, lease, replication, explorer,
    /// and deployment adapters. This intentionally comes from the same config
    /// snapshot as those credentials, not from a later presence-agent read.
    pub credential_version: u64,
    pub snapshot: PresenceSnapshotSource,
    /// The pointer watcher a deployment notification nudges.
    pub reload: crate::generation::ReloadSender,
}

pub fn start_presence_agent(runtime: PresenceRuntime) -> bool {
    let config = match connected_config() {
        Ok(Some(config)) => config,
        Ok(None) => return false,
        Err(error) => {
            warn!(event = "control_plane_presence_configuration_error", %error);
            return false;
        }
    };
    let hostname = machine_hostname();
    tokio::spawn(async move {
        let mut consecutive_failures = 0_u32;
        loop {
            let started = Instant::now();
            let result = crate::asyncrt::select! {
                result = presence_session(&config, &runtime, &hostname) => result,
                credential_version = wait_for_credential_rotation(runtime.credential_version) => {
                    restart_for_credential_rotation(runtime.credential_version, credential_version);
                }
            };
            if started.elapsed() >= Duration::from_secs(30) {
                consecutive_failures = 0;
            }
            match result {
                Ok(()) => {}
                Err(error) if consecutive_failures == 0 => {
                    report_managed_runtime_state(
                        if error.is::<ManagedControlCredentialRevoked>() {
                            ManagedRuntimeState::CredentialRevoked
                        } else {
                            ManagedRuntimeState::ControlPlaneUnavailable
                        },
                    );
                    info!(
                        event = "control_plane_presence_unavailable",
                        %error,
                        "Managed Control Plane presence unavailable; bucket-backed serving continues"
                    );
                }
                Err(_) => {}
            }
            consecutive_failures = consecutive_failures.saturating_add(1);
            let exponent = consecutive_failures.saturating_sub(1).min(5);
            let seconds = 2_u64.saturating_mul(1_u64 << exponent).min(60);
            let jitter_ms = (rand::random::<u16>() as u64) % 1000;
            crate::asyncrt::select! {
                _ = tokio::time::sleep(Duration::from_millis(seconds * 1000 + jitter_ms)) => {}
                credential_version = wait_for_credential_rotation(runtime.credential_version) => {
                    restart_for_credential_rotation(runtime.credential_version, credential_version);
                }
            }
        }
    });
    true
}

fn restart_for_credential_rotation(previous_version: u64, credential_version: u64) -> ! {
    info!(
        event = "control_plane_restart_for_credentials",
        previous_credential_version = previous_version,
        credential_version,
        "managed credential changed; restarting celld to rebuild every credentialed adapter"
    );
    restart_process("credential_rotation")
}

/// Watch the durable installation record for the lifetime of the advisory
/// presence agent. `celld credentials refresh` persists the replacement before
/// acknowledging its handoff; seeing that version is therefore enough to
/// rebuild the complete adapter graph. Replacing only one S3 client here would
/// leave the lease pool, replication, explorer, or deploy agent on revoked
/// credentials.
async fn wait_for_credential_rotation(current_version: u64) -> u64 {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut unavailable = false;
    loop {
        tick.tick().await;
        match connected_config() {
            Ok(Some(config)) => {
                if unavailable {
                    info!(
                        event = "control_plane_credential_watch_recovered",
                        "managed installation record is readable again"
                    );
                    unavailable = false;
                }
                if config.credential_version != current_version {
                    return config.credential_version;
                }
            }
            Ok(None) => {
                if !unavailable {
                    warn!(
                        event = "control_plane_credential_watch_unavailable",
                        reason = "installation_not_connected",
                        "cannot observe managed credential rotation"
                    );
                    unavailable = true;
                }
            }
            Err(error) => {
                if !unavailable {
                    warn!(
                        event = "control_plane_credential_watch_unavailable",
                        reason = "installation_read_failed",
                        %error,
                        "cannot observe managed credential rotation"
                    );
                    unavailable = true;
                }
            }
        }
    }
}

async fn presence_session(
    config: &CloudConfig,
    runtime: &PresenceRuntime,
    hostname: &str,
) -> anyhow::Result<()> {
    let (url, headers) = presence_request(
        config,
        &runtime.node_session_id,
        &runtime.advertise,
        &runtime.listen,
        hostname,
    )?;
    let socket = match crate::ws_client::connect(&url, headers).await {
        Ok(connection) => connection.socket,
        Err(crate::ws_client::Error::Declined(declined))
            if matches!(declined.status.as_u16(), 401 | 403) =>
        {
            return Err(ManagedControlCredentialRevoked.into());
        }
        Err(error) => {
            return Err(anyhow!("{error}")).context("connect Managed Control Plane presence");
        }
    };
    report_managed_runtime_state(ManagedRuntimeState::Connected);
    info!(
        event = "control_plane_presence_connected",
        node_session_id = %runtime.node_session_id,
        advertise = %runtime.advertise,
        "node session connected to the Managed Control Plane"
    );
    let heartbeat_period = Duration::from_millis(
        crate::env_vars::with_default("CELLD_PRESENCE_HEARTBEAT_MS", 30_000)
            .expect("validated CELLD_PRESENCE_HEARTBEAT_MS"),
    );
    let shadow_presence = crate::env_vars::flag("CELLD_PRESENCE_SHADOW", false)
        .expect("validated CELLD_PRESENCE_SHADOW");
    pump_presence(
        socket,
        heartbeat_period,
        move || async move {
            let snapshot = (runtime.snapshot)()
                .await
                .context("celld core stopped before presence snapshot")?;
            let lease_shadow = if shadow_presence {
                Some(lease_shadow_observation(runtime).await)
            } else {
                None
            };
            let mut cells = snapshot
                .cells
                .iter()
                .filter(|cell| {
                    !cell.id.is_empty()
                        && cell.id.len() <= MAX_PRESENCE_CELL_ID_BYTES
                        && !cell.id.chars().any(char::is_control)
                })
                .map(|cell| {
                    serde_json::json!({
                        "id": cell.id,
                        "epoch": cell.epoch,
                    })
                })
                .collect::<Vec<_>>();
            let cells_truncated =
                cells.len() < snapshot.cells.len() || cells.len() > MAX_PRESENCE_CELLS;
            cells.truncate(MAX_PRESENCE_CELLS);
            let mut message = serde_json::json!({
                "serving": snapshot.serving,
                "owned_cells": snapshot.owned_cells(),
                "cells": cells,
                "cells_truncated": cells_truncated,
                "activity": {
                    "acquired": snapshot.activity.acquired,
                    "proxied": snapshot.activity.proxied,
                    "expired_owner_leases": snapshot.activity.expired_owner_leases,
                    "restored": snapshot.activity.restored,
                    "advanced_epochs": snapshot.activity.advanced_epochs,
                    "handed_off": snapshot.activity.handed_off,
                    "handoff_failed": snapshot.activity.handoff_failed,
                },
            });
            if let Some(observation) = lease_shadow {
                message["lease_shadow"] = observation;
            }
            Ok(message.to_string())
        },
        move |text| async move {
            let message = serde_json::from_str::<serde_json::Value>(&text).ok();
            let message_type = message
                .as_ref()
                .and_then(|value| value.get("type"))
                .and_then(|value| value.as_str())
                .map(str::to_string);
            match message_type.as_deref() {
                Some("explorer_request") => {
                    let response =
                        handle_explorer_request(message.as_ref().unwrap(), runtime).await;
                    return Ok(Some(response.to_string()));
                }
                Some("deployment") => {
                    let client = reqwest::Client::builder()
                        .timeout(Duration::from_secs(30))
                        .build()?;
                    if poll_and_apply(&client, config, &runtime.s3)
                        .await?
                        .is_some()
                    {
                        adopt_or_restart(&runtime.reload);
                    }
                }
                Some("deployment_current") => adopt_or_restart(&runtime.reload),
                _ => {}
            }
            Ok(None)
        },
    )
    .await
}

/// Run a presence session until the heartbeat or the socket stops it.
///
/// The read is never cancelled. `fastwebsockets::read_frame` consumes header
/// bytes from its buffer and then awaits for the payload, holding what it parsed
/// in local variables, so dropping that future keeps the buffer advanced and
/// loses the header. The next read then treats a payload byte as a frame header
/// and the stream never realigns. An earlier version of this session read the
/// socket in the same select as the heartbeat timer, so every tick dropped a read
/// in progress. Cancelling a read mid-payload fails against that shape.
///
/// One async block therefore owns each direction, and the socket is split so
/// that the heartbeat can write while the reader reads. The select at the end
/// drops the direction that did not finish, which is safe because the session is
/// over and the caller reconnects with a new socket.
///
/// The two directions are not independent. Both write to the one socket, so they
/// share the writer behind a mutex, and `write_frame` is awaited while that mutex
/// is held. A control plane that stops reading therefore blocks the heartbeat and
/// the automatic replies of the reader alike. The cancelling `select!` had the
/// same head-of-line property, so this is not a regression, but the split shape
/// must not be read as two isolated pipes.
///
/// `heartbeat` returns the message for one tick. `text` handles one text message
/// and returns an optional reply. Ping and Close keep the automatic reply of the
/// reader, so only Text reaches `text`.
#[doc(hidden)]
pub async fn pump_presence<S, H, HFut, T, TFut>(
    socket: fastwebsockets::WebSocket<S>,
    heartbeat_period: Duration,
    mut heartbeat: H,
    mut text: T,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    H: FnMut() -> HFut,
    HFut: Future<Output = anyhow::Result<String>>,
    T: FnMut(String) -> TFut,
    TFut: Future<Output = anyhow::Result<Option<String>>>,
{
    use fastwebsockets::{FragmentCollectorRead, Payload};

    let (reader, writer) = socket.split(tokio::io::split);
    let mut reader = FragmentCollectorRead::new(reader);
    let writer = Arc::new(tokio::sync::Mutex::new(writer));

    let mut ticker = tokio::time::interval(heartbeat_period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let beat_writer = writer.clone();
    let beat = async {
        loop {
            ticker.tick().await;
            let message = heartbeat().await?;
            beat_writer
                .lock()
                .await
                .write_frame(Frame::text(message.into_bytes().into()))
                .await?;
        }
    };

    let obligated_writer = writer.clone();
    let mut obligated = move |frame: Frame<'_>| {
        let writer = obligated_writer.clone();
        // The reply borrows the reader's buffer, and the write outlives the
        // callback, so the payload is copied out. The reader builds a pong or a
        // close echo unmasked, exactly as the unsplit collector did.
        let reply = Frame::new(
            frame.fin,
            frame.opcode,
            None,
            Payload::Owned(frame.payload.to_vec()),
        );
        async move { writer.lock().await.write_frame(reply).await }
    };
    let read = async {
        loop {
            let frame = reader
                .read_frame(&mut obligated)
                .await
                .map_err(|error| anyhow!("{error}"))
                .context("read Managed Control Plane presence")?;
            match frame.opcode {
                OpCode::Text => {
                    let message = String::from_utf8_lossy(&frame.payload).into_owned();
                    if let Some(reply) = text(message).await? {
                        writer
                            .lock()
                            .await
                            .write_frame(Frame::text(reply.into_bytes().into()))
                            .await?;
                    }
                }
                OpCode::Close => return Err(anyhow!("Managed Control Plane closed presence")),
                _ => {}
            }
        }
    };

    let mut beat = std::pin::pin!(beat);
    let mut read = std::pin::pin!(read);
    crate::asyncrt::select! {
        result = &mut beat => result,
        result = &mut read => result,
    }
}

/// Independently observe bucket truth for rollout comparison. This is an
/// off-by-default management diagnostic: it never feeds the lease record back
/// into the core or changes whether the node serves.
async fn lease_shadow_observation(runtime: &PresenceRuntime) -> serde_json::Value {
    let checked_at_ms = crate::ownership_store::now_ms();
    match crate::ownership_store::load_node_lease(&runtime.s3, &runtime.node_session_id).await {
        Ok(Some(record)) => serde_json::json!({
            "bucket_status": if record.expires_ms > checked_at_ms { "live" } else { "expired" },
            "node": record.node,
            "advertise": record.addr,
            "expires_ms": record.expires_ms,
            "checked_at_ms": checked_at_ms,
        }),
        Ok(None) => serde_json::json!({
            "bucket_status": "missing",
            "node": null,
            "advertise": null,
            "expires_ms": null,
            "checked_at_ms": checked_at_ms,
        }),
        Err(_) => serde_json::json!({
            "bucket_status": "unavailable",
            "node": null,
            "advertise": null,
            "expires_ms": null,
            "checked_at_ms": checked_at_ms,
        }),
    }
}

/// How long a read-only exploration can spend in object storage.
///
/// A person waits for this answer in the Managed Control Plane, and the
/// presence read loop runs the handler inline, so an exploration that never
/// finishes holds every later presence message behind it. The bounded replica
/// retry policy already keeps an ordinary failure well under this, therefore
/// reaching this deadline means the store stopped answering rather than
/// answering with an error.
const EXPLORER_STORAGE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

async fn handle_explorer_request(
    message: &serde_json::Value,
    runtime: &PresenceRuntime,
) -> serde_json::Value {
    let request_id = message
        .get("request_id")
        .and_then(|value| value.as_str())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
        .unwrap_or("");
    let operation = message.get("operation").and_then(|value| value.as_str());
    let result = match operation {
        Some("list_cells") => {
            let cursor = match message.get("cursor") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(cursor)) if cursor.len() <= 2_048 => {
                    Some(cursor.as_str())
                }
                _ => return explorer_error(request_id, "invalid_request"),
            };
            match tokio::time::timeout(
                EXPLORER_STORAGE_DEADLINE,
                list_durable_cells(&runtime.s3, cursor),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    warn!(
                        event = "control_plane_explorer_deadline",
                        request_id,
                        operation = "list_cells",
                        "read-only exploration passed its storage deadline"
                    );
                    return explorer_error(request_id, "inspection_timed_out");
                }
            }
        }
        Some("inspect_cell") => {
            let Some(cell) = message
                .get("cell_id")
                .and_then(|value| value.as_str())
                // The engine's storage fence, the same one the listing
                // applies. An inbound id is spliced into a bucket key, so
                // the charset that decides what a node could have written
                // also decides what this can ask for.
                .filter(|cell| celld_logic::cell::valid_cell_scope(cell))
            else {
                return explorer_error(request_id, "invalid_request");
            };
            let table = match message.get("table") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(table))
                    if !table.is_empty() && table.len() <= 200 =>
                {
                    Some(table.clone())
                }
                _ => return explorer_error(request_id, "invalid_request"),
            };
            let Some(replication) = runtime.replication.clone() else {
                return explorer_error(request_id, "snapshot_not_found");
            };
            let Some(snapshot) = (runtime.snapshot)().await else {
                return explorer_error(request_id, "node_transport_unavailable");
            };
            let active_epoch = snapshot
                .cells
                .iter()
                .find(|candidate| candidate.id == cell)
                .map(|candidate| candidate.epoch);
            if let Some(epoch) = active_epoch {
                let cell = cell.to_string();
                match tokio::task::spawn_blocking(move || {
                    let Some(snapshot) = replication.snapshot_active(&cell, epoch)? else {
                        return Ok(None);
                    };
                    inspect_snapshot(snapshot, &cell, table.as_deref(), "active").map(Some)
                })
                .await
                {
                    Ok(Ok(Some(result))) => Ok(result),
                    Ok(Ok(None)) => {
                        return explorer_error(request_id, "active_snapshot_unavailable");
                    }
                    Ok(Err(error)) => Err(error.context("inspect active cell snapshot")),
                    Err(error) => Err(anyhow!("active inspection task failed: {error}")),
                }
            } else {
                let restored = match tokio::time::timeout(
                    EXPLORER_STORAGE_DEADLINE,
                    replication.restore_snapshot(cell),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        warn!(
                            event = "control_plane_explorer_deadline",
                            request_id,
                            operation = "inspect_cell",
                            cell,
                            "read-only exploration passed its storage deadline"
                        );
                        return explorer_error(request_id, "inspection_timed_out");
                    }
                };
                match restored {
                    Ok(Some(snapshot)) => {
                        let cell = cell.to_string();
                        match tokio::task::spawn_blocking(move || {
                            inspect_snapshot(snapshot, &cell, table.as_deref(), "replicated")
                        })
                        .await
                        {
                            Ok(result) => result,
                            Err(error) => Err(anyhow!("inspection task failed: {error}")),
                        }
                    }
                    Ok(None) => return explorer_error(request_id, "snapshot_not_found"),
                    Err(error) => Err(error.context("restore replicated snapshot")),
                }
            }
        }
        _ => return explorer_error(request_id, "invalid_request"),
    };
    match result {
        Ok(result) => serde_json::json!({
            "type": "explorer_response",
            "request_id": request_id,
            "ok": true,
            "result": result,
        }),
        Err(error) => {
            let stable_error = if error
                .chain()
                .any(|cause| cause.to_string() == "table_not_found")
            {
                "table_not_found"
            } else if error
                .chain()
                .any(|cause| cause.to_string() == "table_not_previewable")
            {
                "table_not_previewable"
            } else {
                "inspection_failed"
            };
            warn!(
                event = "control_plane_explorer_request_failed",
                request_id,
                %error,
                "read-only cell inspection failed"
            );
            explorer_error(request_id, stable_error)
        }
    }
}

fn explorer_error(request_id: &str, error: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "explorer_response",
        "request_id": request_id,
        "ok": false,
        "error": error,
    })
}

async fn list_durable_cells(
    bucket: &Bucket,
    cursor: Option<&str>,
) -> anyhow::Result<serde_json::Value> {
    // Paging is client-side over the full delimiter listing, bounded by the
    // fleet's cell count. `Bucket::common_prefixes_page` would bound the
    // request too, but the explorer's cursor is a sorted position and the
    // store lists in key order, which are not the same sequence.
    let mut cells = bucket
        .common_prefixes("cells/")
        .await?
        .into_iter()
        .filter_map(|prefix| prefix.strip_prefix("cells/").map(str::to_string))
        // The same fence `celld cell list` applies: the engine's storage
        // charset decides what a celld node could have written. A second
        // opinion here would show foreign bucket content as a cell in one
        // surface and hide it in the other.
        .filter(|cell| celld_logic::cell::valid_cell_scope(cell))
        .collect::<Vec<_>>();
    cells.sort();
    if let Some(cursor) = cursor {
        cells.retain(|cell| cell.as_str() > cursor);
    }
    let mut next_cursor = None;
    if cells.len() > MAX_EXPLORER_CELL_PAGE {
        cells.truncate(MAX_EXPLORER_CELL_PAGE);
        next_cursor = cells.last().cloned();
    }
    Ok(serde_json::json!({
        "cells": cells,
        "next_cursor": next_cursor,
    }))
}

fn inspect_snapshot(
    snapshot: crate::replication::RestoredSnapshot,
    cell: &str,
    selected_table: Option<&str>,
    source: &str,
) -> anyhow::Result<serde_json::Value> {
    let connection = Connection::open_with_flags(
        snapshot.path(),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let mut schema = connection.prepare(
        "SELECT name, type, sql
           FROM sqlite_schema
          WHERE type IN ('table', 'view')
            AND name NOT LIKE 'sqlite_%'
          ORDER BY name
          LIMIT ?1",
    )?;
    let mut rows = schema.query([i64::try_from(MAX_EXPLORER_TABLES + 1)?])?;
    let mut tables = Vec::new();
    let mut schema_bytes = 0_usize;
    let mut tables_truncated = false;
    while let Some(row) = rows.next()? {
        if tables.len() >= MAX_EXPLORER_TABLES {
            tables_truncated = true;
            break;
        }
        let name: String = row.get(0)?;
        let kind: String = row.get(1)?;
        let sql: Option<String> = row.get(2)?;
        let entry = serde_json::json!({
            "name": name,
            "kind": kind,
            "sql": sql.map(|sql| truncate_utf8(&sql, 4 * 1024).0),
        });
        let entry_bytes = serde_json::to_vec(&entry)?.len();
        if schema_bytes + entry_bytes > MAX_EXPLORER_RESPONSE_BYTES / 2 {
            tables_truncated = true;
            break;
        }
        schema_bytes += entry_bytes;
        tables.push(entry);
    }
    drop(rows);
    drop(schema);
    let preview = match selected_table {
        Some(table) => Some(preview_table(&connection, table)?),
        None => None,
    };
    Ok(serde_json::json!({
        "cell_id": cell,
        "epoch": snapshot.epoch,
        "source": source,
        "tables": tables,
        "tables_truncated": tables_truncated,
        "preview": preview,
    }))
}

fn preview_table(connection: &Connection, table: &str) -> anyhow::Result<serde_json::Value> {
    let kind = connection
        .query_row(
            "SELECT type FROM sqlite_schema WHERE name = ?1 AND type IN ('table', 'view')",
            [table],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .context("table_not_found")?;
    if kind != "table" {
        anyhow::bail!("table_not_previewable");
    }
    let quoted = table.replace('"', "\"\"");
    let mut statement = connection.prepare(&format!(
        "SELECT * FROM \"{quoted}\" LIMIT {}",
        MAX_EXPLORER_ROWS + 1,
    ))?;
    let columns = statement
        .column_names()
        .iter()
        .take(MAX_EXPLORER_COLUMNS)
        .map(|name| truncate_utf8(name, 200).0)
        .collect::<Vec<_>>();
    let column_count = columns.len();
    let all_column_count = statement.column_count();
    let mut query = statement.query([])?;
    let mut rows = Vec::new();
    let mut response_bytes = 0_usize;
    let mut truncated = all_column_count > MAX_EXPLORER_COLUMNS;
    while let Some(row) = query.next()? {
        if rows.len() >= MAX_EXPLORER_ROWS {
            truncated = true;
            break;
        }
        let mut values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            values.push(explorer_value(row.get_ref(index)?));
        }
        let row_bytes = serde_json::to_vec(&values)?.len();
        if response_bytes + row_bytes > MAX_EXPLORER_RESPONSE_BYTES / 2 {
            truncated = true;
            break;
        }
        response_bytes += row_bytes;
        rows.push(values);
    }
    Ok(serde_json::json!({
        "table": table,
        "columns": columns,
        "rows": rows,
        "truncated": truncated,
    }))
}

fn explorer_value(value: ValueRef<'_>) -> serde_json::Value {
    match value {
        ValueRef::Null => serde_json::json!({ "type": "null", "value": null }),
        ValueRef::Integer(value) => {
            serde_json::json!({ "type": "integer", "value": value.to_string() })
        }
        ValueRef::Real(value) => {
            serde_json::json!({ "type": "real", "value": value.to_string() })
        }
        ValueRef::Text(value) => {
            let text = String::from_utf8_lossy(value);
            let (text, truncated) = truncate_utf8(&text, MAX_EXPLORER_VALUE_BYTES);
            serde_json::json!({
                "type": "text",
                "value": text,
                "bytes": value.len(),
                "truncated": truncated,
            })
        }
        ValueRef::Blob(value) => {
            let shown = &value[..value.len().min(MAX_EXPLORER_VALUE_BYTES)];
            serde_json::json!({
                "type": "blob",
                "value": hex(shown),
                "bytes": value.len(),
                "truncated": shown.len() < value.len(),
            })
        }
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_string(), false);
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_string(), true)
}

fn hex(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn presence_request(
    config: &CloudConfig,
    node_session_id: &str,
    advertise: &str,
    listen: &str,
    hostname: &str,
) -> anyhow::Result<(String, HeaderMap)> {
    use hyper::header::{HeaderName, HeaderValue, AUTHORIZATION};

    let token = config
        .installation_token
        .as_deref()
        .context("managed installation has no token")?;
    let mut url = url::Url::parse(&config.control_url)?;
    url.set_scheme(if url.scheme() == "https" { "wss" } else { "ws" })
        .map_err(|_| anyhow!("invalid Managed Control Plane WebSocket scheme"))?;
    url.set_path("/api/agent/presence");
    url.query_pairs_mut()
        .append_pair("node_session_id", node_session_id)
        .append_pair("advertise", advertise)
        .append_pair("listen", listen);
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}"))?,
    );
    headers.insert(
        HeaderName::from_static("x-cells-version"),
        HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
    );
    // Deployment features come from the shared list so the advertised
    // capabilities can never drift from what apply_deployment accepts.
    let capabilities = crate::protocol::SUPPORTED_DEPLOYMENT_FEATURES
        .iter()
        .copied()
        .chain(["sqlite-explorer-v1", "sqlite-explorer-v2"])
        .collect::<Vec<_>>()
        .join(",");
    headers.insert(
        HeaderName::from_static("x-cells-capabilities"),
        HeaderValue::from_str(&capabilities)?,
    );
    headers.insert(
        HeaderName::from_static("x-cells-hostname"),
        HeaderValue::from_str(hostname)?,
    );
    headers.insert(
        HeaderName::from_static("x-cells-os"),
        HeaderValue::from_static(std::env::consts::OS),
    );
    headers.insert(
        HeaderName::from_static("x-cells-arch"),
        HeaderValue::from_static(std::env::consts::ARCH),
    );
    Ok((url.into(), headers))
}

pub fn start_deploy_agent(bucket: Bucket, reload: crate::generation::ReloadSender) -> bool {
    let config = match connected_config() {
        Ok(Some(config)) => config,
        Ok(None) => return false,
        Err(error) => {
            warn!(event = "control_plane_agent_configuration_error", %error);
            return false;
        }
    };
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                warn!(event = "control_plane_agent_client_error", %error);
                return;
            }
        };
        let mut consecutive_failures = 0_u32;
        loop {
            let mut delay = Duration::from_secs(60);
            match poll_and_apply(&client, &config, &bucket).await {
                Ok(Some(applied)) => {
                    if consecutive_failures > 0 {
                        info!(
                            event = "control_plane_agent_reconnected",
                            failures = consecutive_failures,
                            "Managed Control Plane connection restored"
                        );
                    }
                    consecutive_failures = 0;
                    info!(
                        event = "control_plane_deployment_applied",
                        deployment_id = %applied.id,
                        script_name = %applied.script_name,
                        "deployment applied to the fleet bucket; adopting it"
                    );
                    adopt_or_restart(&reload);
                }
                Ok(None) => {
                    if consecutive_failures > 0 {
                        info!(
                            event = "control_plane_agent_reconnected",
                            failures = consecutive_failures,
                            "Managed Control Plane connection restored"
                        );
                    }
                    consecutive_failures = 0;
                }
                Err(error) => {
                    if error.is::<ManagedControlCredentialRevoked>() {
                        report_managed_runtime_state(ManagedRuntimeState::CredentialRevoked);
                    }
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if consecutive_failures == 1 {
                        info!(
                            event = "control_plane_agent_unavailable",
                            %error,
                            "Managed Control Plane unavailable; bucket-backed serving continues"
                        );
                    }
                    let exponent = consecutive_failures.saturating_sub(1).min(5);
                    let seconds = 2_u64.saturating_mul(1_u64 << exponent).min(60);
                    let jitter_ms = (rand::random::<u16>() as u64) % 1000;
                    delay = Duration::from_millis(seconds * 1000 + jitter_ms);
                }
            }
            tokio::time::sleep(delay).await;
        }
    });
    true
}

pub async fn wait_for_initial_deployment(bucket: &Bucket) -> anyhow::Result<()> {
    let config = connected_config()?.context("managed installation is not connected")?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    note!("Waiting for the first Managed Control Plane deployment...");
    note!(
        "Deploy from {}/control; this process will start automatically.",
        config.control_url.trim_end_matches('/')
    );
    loop {
        if deployment_exists(bucket).await? {
            return Ok(());
        }
        if poll_and_apply(&client, &config, bucket).await?.is_some() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn deployment_exists(bucket: &Bucket) -> anyhow::Result<bool> {
    let objects = bucket
        .list("deploy/")
        .await
        .context("discover managed deployment")?;
    Ok(objects
        .iter()
        .map(|object| object.location.as_ref())
        .any(|key| {
            key == "deploy/current.json"
                || key
                    .strip_prefix("deploy/")
                    .is_some_and(is_named_current_pointer)
        }))
}

fn is_named_current_pointer(key: &str) -> bool {
    let mut parts = key.split('/');
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(_), Some("current.json"), None)
    )
}

fn connected_config() -> anyhow::Result<Option<CloudConfig>> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let config: CloudConfig =
        serde_json::from_str(&contents).with_context(|| format!("decode {}", path.display()))?;
    if config.installation_token.is_some() {
        Ok(Some(config))
    } else {
        Ok(None)
    }
}

async fn poll_and_apply(
    client: &reqwest::Client,
    config: &CloudConfig,
    bucket: &Bucket,
) -> anyhow::Result<Option<AppliedDeployment>> {
    let token = config
        .installation_token
        .as_deref()
        .context("cloud config has no instance token")?;
    let response = client
        .post(format!("{}/api/agent/commands/next", config.control_url))
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .context("poll deployment command")?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        return Ok(None);
    }
    if matches!(response.status().as_u16(), 401 | 403) {
        return Err(ManagedControlCredentialRevoked.into());
    }
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("deployment poll returned {status}: {body}"));
    }
    let envelope: AgentCommandEnvelope =
        response.json().await.context("decode deployment command")?;
    let command = envelope.command;
    if command.kind != "deploy" {
        return Err(anyhow!(
            "unsupported control-plane command: {}",
            command.kind
        ));
    }

    let result = apply_deployment(client, token, bucket, &command.deployment).await;
    let failure = result.as_ref().err().map(|error| format!("{error:#}"));
    let completion = client
        .post(format!(
            "{}/api/agent/commands/{}/complete",
            config.control_url, command.id
        ))
        .bearer_auth(token)
        .json(&CompleteCommandRequest {
            success: result.is_ok(),
            error: failure.as_deref(),
        })
        .send()
        .await
        .context("report deployment completion")?;
    if !completion.status().is_success() {
        let status = completion.status();
        let body = completion.text().await.unwrap_or_default();
        return Err(anyhow!("deployment completion returned {status}: {body}"));
    }
    result?;
    Ok(Some(AppliedDeployment {
        id: command.id,
        script_name: command.deployment.script_name,
    }))
}

#[doc(hidden)]
pub async fn apply_deployment(
    client: &reqwest::Client,
    token: &str,
    bucket: &Bucket,
    deployment: &AgentDeployment,
) -> anyhow::Result<()> {
    if deployment.pointer.version != deployment.version
        || deployment.manifest.version != deployment.version
        || deployment.manifest.script_name != deployment.script_name
    {
        return Err(anyhow!("control-plane deployment metadata is inconsistent"));
    }
    validate_managed_module_envelope(deployment)?;
    validate_managed_class_migrations(&deployment.manifest)?;
    crate::protocol::validate_required_features(&deployment.manifest.required_features)?;
    crate::protocol::validate_queue_manifest(&deployment.manifest)?;
    // Read the current owner before writing this deployment's immutable
    // objects. A competing Queue consumer must refuse the deployment without
    // leaving a prefix that looks publishable.
    let queue_attachments =
        crate::deploy::prepare_queue_attachments(bucket, &deployment.manifest).await?;

    let mut asset_files = 0_u32;
    let mut asset_bytes = 0_u64;
    let mut asset_blobs = 0_usize;
    if let Some(reference) = &deployment.manifest.assets {
        let assets = deployment
            .assets
            .as_ref()
            .context("asset manifest has no control-plane transport")?;
        if reference.index != "assets.json"
            || reference.sha256 != assets.sha256
            || reference.file_count != assets.file_count
            || reference.total_bytes != assets.total_bytes
        {
            return Err(anyhow!("control-plane asset metadata is inconsistent"));
        }
        let response = client
            .get(&assets.index_download_url)
            .bearer_auth(token)
            .send()
            .await
            .context("download asset index")?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "asset index download returned {}",
                response.status()
            ));
        }
        let index_bytes = response.bytes().await?;
        let index_hash = format!("{:x}", Sha256::digest(&index_bytes));
        if index_hash != assets.sha256 {
            return Err(anyhow!("asset index checksum mismatch"));
        }
        let index: AssetIndex =
            serde_json::from_slice(&index_bytes).context("decode asset index")?;
        if index.schema_version != 1
            || index.entries.len() != assets.file_count as usize
            || index.entries.values().map(|entry| entry.bytes).sum::<u64>() != assets.total_bytes
        {
            return Err(anyhow!("asset index counts do not match deployment"));
        }

        let mut unique = std::collections::BTreeMap::<String, u64>::new();
        for (path, entry) in &index.entries {
            validate_asset_index_entry(path, &entry.sha256, entry.bytes)?;
            if let Some(previous) = unique.insert(entry.sha256.clone(), entry.bytes) {
                if previous != entry.bytes {
                    return Err(anyhow!(
                        "asset digest has conflicting sizes: {}",
                        entry.sha256
                    ));
                }
            }
        }
        for (sha256, bytes) in &unique {
            let key =
                asset_blob_key(sha256).ok_or_else(|| anyhow!("invalid asset digest: {sha256}"))?;
            if let Ok(Some((size, meta))) = bucket.head_with_meta(&key, "sha256").await {
                if size == *bytes && meta.as_deref() == Some(sha256.as_str()) {
                    continue;
                }
                tracing::warn!(
                    event = "asset_blob_repair_required",
                    %sha256,
                    "asset blob exists with an invalid size or checksum metadata"
                );
            }

            let response = client
                .get(format!(
                    "{}/{}",
                    assets.blob_download_base_url.trim_end_matches('/'),
                    sha256
                ))
                .bearer_auth(token)
                .send()
                .await
                .with_context(|| format!("download asset blob {sha256}"))?;
            if !response.status().is_success() {
                return Err(anyhow!(
                    "asset blob {sha256} download returned {}",
                    response.status()
                ));
            }
            let body = response.bytes().await?;
            if body.len() as u64 != *bytes {
                return Err(anyhow!("asset blob {sha256} size mismatch"));
            }
            let hash = format!("{:x}", Sha256::digest(&body));
            if hash != *sha256 {
                return Err(anyhow!("asset blob {sha256} checksum mismatch"));
            }
            bucket
                .put_with_meta(&key, body.to_vec(), &[("sha256", sha256)])
                .await?;
        }
        bucket
            .put(
                &format!("{}/assets.json", deployment.pointer.prefix),
                index_bytes.to_vec(),
            )
            .await?;
        asset_files = assets.file_count;
        asset_bytes = assets.total_bytes;
        asset_blobs = unique.len();
    } else if deployment.assets.is_some() {
        return Err(anyhow!(
            "control-plane sent asset transport for a manifest without assets"
        ));
    }

    for module in &deployment.modules {
        let response = client
            .get(&module.download_url)
            .bearer_auth(token)
            .send()
            .await
            .with_context(|| format!("download module {}", module.name))?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "module {} download returned {}",
                module.name,
                response.status()
            ));
        }
        let bytes = response.bytes().await?;
        let expected_bytes = deployment
            .manifest
            .modules
            .iter()
            .find(|reference| reference.name == module.name)
            .expect("managed module envelope validated above")
            .bytes;
        if bytes.len() != expected_bytes {
            return Err(anyhow!("module {} size mismatch", module.name));
        }
        let hash = format!("{:x}", Sha256::digest(&bytes));
        if hash != module.sha256 {
            return Err(anyhow!("module {} checksum mismatch", module.name));
        }
        bucket
            .put(
                &format!("{}/{}", deployment.pointer.prefix, module.name),
                bytes.to_vec(),
            )
            .await?;
    }
    bucket
        .put(
            &format!("{}/manifest.json", deployment.pointer.prefix),
            serde_json::to_vec_pretty(&deployment.manifest)?,
        )
        .await?;
    // A producer can run in a different script, so the manifest is not a
    // discoverable consumer index. Publish the same fleet-wide attachment as
    // `celld deploy`, after its exact deployment prefix is complete.
    crate::deploy::publish_queue_attachments(
        bucket,
        &deployment.manifest,
        &deployment.pointer.prefix,
        queue_attachments,
    )
    .await?;
    bucket
        .put(
            &format!("deploy/{}/current.json", deployment.script_name),
            serde_json::to_vec_pretty(&deployment.pointer)?,
        )
        .await?;
    // This fleet-wide pointer is the sole application selector. The named
    // pointer above remains only for resolving service-binding components and
    // for migrating buckets written by older celld releases.
    bucket
        .put(
            "deploy/current.json",
            serde_json::to_vec_pretty(&deployment.pointer)?,
        )
        .await?;
    info!(
        event = "control_plane_deployment_applied",
        script_name = %deployment.script_name,
        version = %deployment.version,
        modules = deployment.modules.len(),
        asset_files,
        asset_bytes,
        asset_blobs,
        bucket = %bucket.name,
        "deployment artifacts written to fleet bucket"
    );
    Ok(())
}

fn validate_managed_module_envelope(deployment: &AgentDeployment) -> anyhow::Result<()> {
    let expected_prefix = format!("deploy/{}/{}", deployment.script_name, deployment.version);
    if deployment.pointer.script_name.as_deref() != Some(deployment.script_name.as_str())
        || deployment.pointer.prefix != expected_prefix
        || deployment.pointer.rollout.percent != 100
    {
        return Err(anyhow!(
            "control-plane deployment pointer is inconsistent with its identity"
        ));
    }
    let expected_schema = if deployment.manifest.assets.is_some() {
        2
    } else {
        1
    };
    if deployment.manifest.schema_version != expected_schema {
        return Err(anyhow!(
            "control-plane manifest schema does not match its features"
        ));
    }
    if deployment.manifest.modules.len() > MAX_MANAGED_MODULES
        || deployment.modules.len() > MAX_MANAGED_MODULES
    {
        return Err(anyhow!("control-plane deployment has too many modules"));
    }

    let mut manifest_modules = std::collections::BTreeMap::new();
    let mut total_bytes = 0_usize;
    for module in &deployment.manifest.modules {
        if !valid_managed_module_name(&module.name)
            || !(valid_lower_hex(&module.sha256, 16) || valid_lower_hex(&module.sha256, 64))
        {
            return Err(anyhow!(
                "control-plane manifest has an invalid module reference: {:?}",
                module.name
            ));
        }
        total_bytes = total_bytes
            .checked_add(module.bytes)
            .filter(|bytes| *bytes <= MAX_MANAGED_MODULE_BYTES)
            .ok_or_else(|| anyhow!("control-plane deployment modules are too large"))?;
        if manifest_modules
            .insert(module.name.as_str(), module)
            .is_some()
        {
            return Err(anyhow!(
                "control-plane manifest repeats module {:?}",
                module.name
            ));
        }
    }
    match deployment.manifest.main_module.as_deref() {
        Some(main) if !manifest_modules.contains_key(main) => {
            return Err(anyhow!(
                "control-plane manifest main module is not in its module list"
            ));
        }
        None if deployment.manifest.assets.is_none() => {
            return Err(anyhow!(
                "control-plane manifest has neither a main module nor assets"
            ));
        }
        _ => {}
    }

    let mut transport_modules = std::collections::BTreeMap::new();
    for module in &deployment.modules {
        if !valid_managed_module_name(&module.name) || !valid_lower_hex(&module.sha256, 64) {
            return Err(anyhow!(
                "control-plane transport has an invalid module reference: {:?}",
                module.name
            ));
        }
        if transport_modules
            .insert(module.name.as_str(), module)
            .is_some()
        {
            return Err(anyhow!(
                "control-plane transport repeats module {:?}",
                module.name
            ));
        }
    }
    if manifest_modules.keys().ne(transport_modules.keys()) {
        return Err(anyhow!(
            "control-plane transport modules do not match the manifest"
        ));
    }
    for (name, manifest) in manifest_modules {
        let transport = transport_modules
            .get(name)
            .expect("module name sets compared above");
        if !transport.sha256.starts_with(&manifest.sha256) {
            return Err(anyhow!(
                "control-plane transport checksum does not match manifest module {name:?}"
            ));
        }
    }
    Ok(())
}

fn valid_managed_module_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 240
        && !name.starts_with('/')
        && !name.contains('\\')
        && name
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

fn valid_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_managed_class_migrations(manifest: &Manifest) -> anyhow::Result<()> {
    let do_classes = manifest
        .do_classes
        .iter()
        .map(|class| {
            if class.is_empty() {
                Err(anyhow!(
                    "managed manifest has an empty Durable Object class"
                ))
            } else {
                Ok(class.as_str())
            }
        })
        .collect::<anyhow::Result<std::collections::BTreeSet<_>>>()?;
    let mut declared_sqlite = std::collections::BTreeSet::new();
    for class in &manifest.sqlite_classes {
        if class.is_empty() {
            return Err(anyhow!("managed manifest has an empty SQLite class"));
        }
        if !do_classes.contains(class.as_str()) {
            return Err(anyhow!(
                "managed manifest SQLite class {class:?} is not a Durable Object class"
            ));
        }
        if !declared_sqlite.insert(class.as_str()) {
            return Err(anyhow!(
                "managed manifest declares SQLite class {class:?} more than once"
            ));
        }
    }

    let metadata = manifest
        .raw_metadata
        .as_object()
        .context("managed manifest raw_metadata must be an object")?;
    let Some(migrations) = metadata.get("migrations") else {
        if declared_sqlite.is_empty() {
            return Ok(());
        }
        return Err(anyhow!(
            "managed manifest SQLite classes do not match its migration metadata"
        ));
    };
    let migrations = migrations
        .as_object()
        .context("managed manifest migrations must be an object")?;
    let unsupported = migrations
        .keys()
        .filter(|key| !matches!(key.as_str(), "old_tag" | "new_tag" | "steps"))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        return Err(anyhow!(
            "unsupported managed migration keys: {}. Class rename, delete, transfer, and non-SQLite migration semantics need an explicit persisted-state contract before deployment",
            unsupported.join(", ")
        ));
    }
    for tag in ["old_tag", "new_tag"] {
        if let Some(value) = migrations.get(tag) {
            if !value.is_null() && value.as_str().is_none_or(str::is_empty) {
                return Err(anyhow!(
                    "managed manifest migrations.{tag} must be null or a non-empty string"
                ));
            }
        }
    }
    let steps = migrations
        .get("steps")
        .and_then(serde_json::Value::as_array)
        .context("managed manifest migrations.steps must be an array")?;
    let mut migrated_sqlite = std::collections::BTreeSet::new();
    for (index, step) in steps.iter().enumerate() {
        let step = step.as_object().with_context(|| {
            format!("managed manifest migrations.steps[{index}] must be an object")
        })?;
        let unsupported = step
            .keys()
            .filter(|key| key.as_str() != "new_sqlite_classes")
            .cloned()
            .collect::<Vec<_>>();
        if !unsupported.is_empty() {
            return Err(anyhow!(
                "unsupported managed migration keys: {}. Class rename, delete, transfer, and non-SQLite migration semantics need an explicit persisted-state contract before deployment",
                unsupported.join(", ")
            ));
        }
        let classes = step
            .get("new_sqlite_classes")
            .and_then(serde_json::Value::as_array)
            .with_context(|| {
                format!(
                    "managed manifest migrations.steps[{index}].new_sqlite_classes must be an array"
                )
            })?;
        for (class_index, class) in classes.iter().enumerate() {
            let class = class.as_str().filter(|class| !class.is_empty()).with_context(|| {
                format!(
                    "managed manifest migrations.steps[{index}].new_sqlite_classes[{class_index}] must be a non-empty string"
                )
            })?;
            if !migrated_sqlite.insert(class) {
                return Err(anyhow!(
                    "managed migration introduces SQLite class {class:?} more than once"
                ));
            }
        }
    }
    if migrated_sqlite != declared_sqlite {
        return Err(anyhow!(
            "managed manifest SQLite classes do not match its migration metadata"
        ));
    }
    Ok(())
}

fn validate_asset_index_entry(path: &str, sha256: &str, bytes: u64) -> anyhow::Result<()> {
    if !path.starts_with('/')
        || path.len() > 1024
        || path.contains('\\')
        || path.contains('\0')
        || path
            .split('/')
            .skip(1)
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(anyhow!("invalid asset path in index: {path:?}"));
    }
    if asset_blob_key(sha256).is_none() {
        return Err(anyhow!("invalid asset digest in index: {sha256:?}"));
    }
    if bytes > 25 * 1024 * 1024 {
        return Err(anyhow!("asset exceeds runtime size limit: {path:?}"));
    }
    Ok(())
}

async fn connect(options: ConnectOptions, wait: bool) -> anyhow::Result<()> {
    let path = config_path()?;
    connect_at_path(path, options, wait).await
}

async fn connect_at_path(path: PathBuf, options: ConnectOptions, wait: bool) -> anyhow::Result<()> {
    let _lock = match try_acquire_enrollment_lock(&path).await? {
        Some(lock) => lock,
        None => wait_for_shared_enrollment(&path).await?,
    };
    let mut config = load_or_create_config(&path, &options)?;
    let enrollment_byo = match (
        config.storage.as_ref(),
        config.byo_storage.as_ref(),
        options.byo_storage.as_ref(),
    ) {
        (Some(_), None, Some(_)) => {
            return Err(anyhow!(
                "this installation is enrolled with managed storage; disconnect before changing storage origin"
            ));
        }
        (None, Some(current), Some(requested)) if current != requested => {
            return Err(anyhow!(
                "the requested BYO bucket does not match this installation enrollment"
            ));
        }
        (None, Some(current), _) => Some(current.clone()),
        (None, None, requested) => requested.cloned(),
        (Some(_), None, None) => None,
        _ => {
            return Err(anyhow!(
                "managed installation record contains an invalid storage target"
            ));
        }
    };
    if config.installation_token.is_some()
        && (config.storage.is_some() || config.byo_storage.is_some())
        && !options.force
    {
        validate_requested_storage(&config, options.byo_storage.as_ref())?;
        retry_credential_handoff_ack(path.clone(), config.clone());
        info!(
            event = "control_plane_already_connected",
            installation_id = %config.installation_id,
            environment = %config.environment,
            control_url = %config.control_url,
            "celld is already connected to the Managed Control Plane"
        );
        return Ok(());
    }

    config.control_url = normalized_control_url(&options.control_url)?;
    config.environment = options.environment.clone();
    config.installation_token = None;
    config.fleet_id = None;
    config.environment_id = None;
    config.credential_handoff_id = None;
    config.pending_claim = None;
    config.storage = None;
    config.byo_storage = enrollment_byo;
    save_config(&path, &config)?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(if wait { 15 } else { 3 }))
        .build()?;
    let claim_url = format!("{}/api/instances/claims", config.control_url);
    let instance_name = instance_name();
    let response = client
        .post(claim_url)
        .json(&CreateClaimRequest {
            instance_id: &config.installation_id,
            instance_name: &instance_name,
            environment: &config.environment,
            version: env!("CARGO_PKG_VERSION"),
            platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
            storage_origin: if config.byo_storage.is_some() {
                "byo"
            } else {
                "managed_r2"
            },
            bucket: config
                .byo_storage
                .as_ref()
                .map(|storage| storage.bucket.as_str()),
            endpoint: config
                .byo_storage
                .as_ref()
                .and_then(|storage| storage.endpoint.as_deref()),
            region: config
                .byo_storage
                .as_ref()
                .map(|storage| storage.region.as_str()),
        })
        .send()
        .await
        .context("request activation link")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("activation endpoint returned {status}: {body}"));
    }
    let claim: CreateClaimResponse = response.json().await.context("decode activation link")?;
    config.pending_claim = Some(PendingClaim {
        verification_uri_complete: claim.verification_uri_complete.clone(),
        expires_at_ms: current_time_ms().saturating_add(claim.expires_in.saturating_mul(1000)),
    });
    save_config(&path, &config)?;

    info!(
        event = "control_plane_claim_pending",
        installation_id = %config.installation_id,
        environment = %config.environment,
        verification_url = %claim.verification_uri_complete,
        expires_in_seconds = claim.expires_in,
        "authenticate to connect this celld instance"
    );
    note!(
        "\nConnect this celld instance to the Managed Control Plane:\n\n  {}\n\nEnvironment: {}\n",
        claim.verification_uri_complete,
        config.environment
    );

    if wait {
        wait_for_approval(client, path, config, claim).await
    } else {
        tokio::spawn(async move {
            if let Err(error) = wait_for_approval(client, path, config, claim).await {
                warn!(event = "control_plane_claim_failed", %error);
            }
        });
        Ok(())
    }
}

async fn wait_for_approval(
    client: reqwest::Client,
    path: PathBuf,
    mut config: CloudConfig,
    claim: CreateClaimResponse,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(claim.expires_in);
    let poll_url = format!("{}/api/instances/claims/token", config.control_url);
    let mut interval = claim.interval.max(1);
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(interval)).await;
        let response = client
            .post(&poll_url)
            .json(&PollClaimRequest {
                device_code: &claim.device_code,
            })
            .send()
            .await
            .context("poll activation")?;
        if response.status() == reqwest::StatusCode::GONE {
            return Err(anyhow!("activation link expired"));
        }
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("activation poll returned {status}: {body}"));
        }
        let result: PollClaimResponse =
            response.json().await.context("decode activation status")?;
        if result.status == "pending" {
            interval = result.interval.unwrap_or(interval).max(1);
            continue;
        }
        if result.status != "connected" {
            return Err(anyhow!("unexpected activation status: {}", result.status));
        }
        config.installation_token = Some(
            result
                .instance_token
                .context("activation response omitted instance token")?,
        );
        config.credential_version = result.credential_version.unwrap_or(1);
        config.fleet_id = Some(
            result
                .fleet_id
                .context("activation response omitted fleet")?,
        );
        config.environment_id = Some(
            result
                .environment_id
                .context("activation response omitted environment")?,
        );
        let bucket = result
            .bucket
            .context("activation response omitted bucket")?;
        let region = result
            .region
            .context("activation response omitted bucket region")?;
        match result.storage_origin.as_str() {
            "managed_r2" => {
                if config.byo_storage.is_some() {
                    return Err(anyhow!(
                        "activation changed the requested BYO bucket to managed storage"
                    ));
                }
                config.storage = Some(ManagedStorageConfig {
                    bucket,
                    endpoint: result
                        .endpoint
                        .context("activation response omitted bucket endpoint")?,
                    region,
                    access_key_id: result
                        .access_key_id
                        .context("activation response omitted storage access key")?,
                    secret_access_key: result
                        .secret_access_key
                        .context("activation response omitted storage secret key")?,
                    session_token: result.session_token,
                });
                config.credential_handoff_id = Some(
                    result
                        .credential_handoff_id
                        .context("activation response omitted credential handoff")?,
                );
            }
            "byo" => {
                let storage = ByoStorageConfig {
                    bucket,
                    endpoint: result.endpoint,
                    region,
                };
                if config.byo_storage.as_ref() != Some(&storage) {
                    return Err(anyhow!(
                        "activation returned a different BYO storage target"
                    ));
                }
                if result.credential_handoff_id.is_some()
                    || result.access_key_id.is_some()
                    || result.secret_access_key.is_some()
                    || result.session_token.is_some()
                {
                    return Err(anyhow!(
                        "activation returned managed credentials for a BYO bucket"
                    ));
                }
                config.storage = None;
                config.byo_storage = Some(storage);
                config.credential_handoff_id = None;
            }
            origin => {
                return Err(anyhow!(
                    "activation returned unknown storage origin: {origin}"
                ))
            }
        }
        config.pending_claim = None;
        save_config(&path, &config)?;
        if let Some(handoff_id) = config.credential_handoff_id.clone() {
            match acknowledge_credential_handoff(&client, &config, &handoff_id).await {
                Ok(()) => {
                    config.credential_handoff_id = None;
                    save_config(&path, &config)?;
                }
                Err(error) => {
                    info!(
                        event = "control_plane_credential_ack_deferred",
                        %error,
                        "managed credential is saved; delivery acknowledgement will retry in the background"
                    );
                    retry_credential_handoff_ack(path.clone(), config.clone());
                }
            }
        }
        info!(
            event = "control_plane_connected",
            installation_id = %config.installation_id,
            fleet_id = %config.fleet_id.as_deref().unwrap_or_default(),
            environment_id = %config.environment_id.as_deref().unwrap_or_default(),
            environment = %config.environment,
            "celld connected to the Managed Control Plane"
        );
        note!(
            "Connected to the Managed Control Plane ({}).\n\
             Keep celld running. In your app directory, run `celld token`, apply the \
             exports it prints, then run `npx wrangler@4 deploy`.",
            config.environment
        );
        return Ok(());
    }
    // Make expiry visible so an unattended process cannot keep displaying a
    // dead approval URL.
    note!(
        "\nThe activation link expired before it was approved.\n\
         Restart celld to get a new one.\n"
    );
    Err(anyhow!(
        "activation link expired before approval; restart celld for a new link"
    ))
}

fn options_from_env() -> anyhow::Result<ConnectOptions> {
    Ok(ConnectOptions {
        control_url: std::env::var("CELLD_CONTROL_URL")
            .unwrap_or_else(|_| DEFAULT_CONTROL_URL.to_string()),
        environment: std::env::var("CELLD_ENV").unwrap_or_else(|_| DEFAULT_ENVIRONMENT.to_string()),
        force: false,
        byo_storage: None,
    })
}

fn config_path() -> anyhow::Result<PathBuf> {
    let base = if let Some(path) = std::env::var_os("CELLD_CONFIG_DIR") {
        PathBuf::from(path)
    } else if let Some(path) = std::env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(path).join("celld")
    } else if let Some(path) = std::env::var_os("HOME") {
        PathBuf::from(path).join(".config").join("celld")
    } else {
        std::env::current_dir()?.join(".celld")
    };
    Ok(base.join("cloud.json"))
}

fn load_or_create_config(path: &Path, options: &ConnectOptions) -> anyhow::Result<CloudConfig> {
    if path.exists() {
        let contents =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        return serde_json::from_str(&contents)
            .with_context(|| format!("decode {}", path.display()));
    }
    let config = CloudConfig {
        version: installation_schema_version(),
        installation_id: random_instance_id(),
        installation_token: None,
        credential_version: initial_credential_version(),
        fleet_id: None,
        environment_id: None,
        control_url: normalized_control_url(&options.control_url)?,
        environment: options.environment.clone(),
        storage: None,
        byo_storage: None,
        credential_handoff_id: None,
        previous_credential: None,
        pending_claim: None,
    };
    save_config(path, &config)?;
    Ok(config)
}

fn retry_credential_handoff_ack(path: PathBuf, config: CloudConfig) {
    let Some(handoff_id) = config.credential_handoff_id.clone() else {
        return;
    };
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
        {
            Ok(client) => client,
            Err(_) => return,
        };
        if acknowledge_credential_handoff(&client, &config, &handoff_id)
            .await
            .is_err()
        {
            return;
        }
        let Ok(_lock) = acquire_enrollment_lock(&path).await else {
            return;
        };
        let Ok(contents) = std::fs::read_to_string(&path) else {
            return;
        };
        let Ok(mut current) = serde_json::from_str::<CloudConfig>(&contents) else {
            return;
        };
        if current.credential_handoff_id.as_deref() != Some(&handoff_id) {
            return;
        }
        current.credential_handoff_id = None;
        current.previous_credential = None;
        if let Err(error) = save_config(&path, &current) {
            warn!(
                event = "control_plane_credential_ack_save_failed",
                %error,
                "credential delivery was acknowledged but the local marker could not be cleared"
            );
        }
    });
}

async fn acknowledge_credential_handoff(
    client: &reqwest::Client,
    config: &CloudConfig,
    handoff_id: &str,
) -> anyhow::Result<()> {
    let token = config
        .installation_token
        .as_deref()
        .context("managed installation has no token")?;
    let response = client
        .post(format!("{}/api/agent/credentials/ack", config.control_url))
        .bearer_auth(token)
        .json(&serde_json::json!({ "handoff_id": handoff_id }))
        .send()
        .await
        .context("acknowledge managed credential delivery")?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "credential acknowledgement returned {}",
            response.status()
        ));
    }
    Ok(())
}

fn save_config(path: &Path, config: &CloudConfig) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("cloud config has no parent directory")?;
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(config)?)
        .with_context(|| format!("write {}", temporary.display()))?;
    restrict_permissions(&temporary)?;
    std::fs::rename(&temporary, path).with_context(|| format!("replace {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

fn random_instance_id() -> String {
    let mut bytes = [0_u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    let suffix = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("inst_{suffix}")
}

fn installation_schema_version() -> u32 {
    1
}

fn initial_credential_version() -> u64 {
    1
}

fn managed_storage_origin() -> String {
    "managed_r2".to_string()
}

fn validate_requested_storage(
    config: &CloudConfig,
    requested: Option<&ByoStorageConfig>,
) -> anyhow::Result<()> {
    match (config.storage.as_ref(), config.byo_storage.as_ref(), requested) {
        (Some(_), None, None) | (None, Some(_), None) => Ok(()),
        (Some(_), None, Some(_)) => Err(anyhow!(
            "this installation is enrolled with managed storage; disconnect before changing storage origin"
        )),
        (None, Some(current), Some(requested)) if current == requested => Ok(()),
        (None, Some(_), Some(_)) => Err(anyhow!(
            "the requested BYO bucket does not match this installation enrollment"
        )),
        _ => Err(anyhow!(
            "managed installation record contains an invalid storage target"
        )),
    }
}

struct EnrollmentLock {
    _file: File,
}

async fn try_acquire_enrollment_lock(config: &Path) -> anyhow::Result<Option<EnrollmentLock>> {
    let (file, lock_path) = open_enrollment_lock(config).await?;
    tokio::task::spawn_blocking(move || match file.try_lock() {
        Ok(()) => Ok(Some(EnrollmentLock { _file: file })),
        Err(std::fs::TryLockError::WouldBlock) => Ok(None),
        Err(std::fs::TryLockError::Error(error)) => {
            Err(error).with_context(|| format!("lock {}", lock_path.display()))
        }
    })
    .await
    .context("join enrollment try-lock task")?
}

async fn acquire_enrollment_lock(config: &Path) -> anyhow::Result<EnrollmentLock> {
    let (file, lock_path) = open_enrollment_lock(config).await?;
    tokio::task::spawn_blocking(move || {
        file.lock()
            .with_context(|| format!("lock {}", lock_path.display()))?;
        Ok(EnrollmentLock { _file: file })
    })
    .await
    .context("join enrollment lock task")?
}

async fn open_enrollment_lock(config: &Path) -> anyhow::Result<(File, PathBuf)> {
    let lock_path = config.with_extension("lock");
    let parent = lock_path
        .parent()
        .context("managed installation lock has no parent directory")?
        .to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&parent).with_context(|| format!("create {}", parent.display()))?;
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("open {}", lock_path.display()))?;
        Ok((file, lock_path))
    })
    .await
    .context("join enrollment lock open task")?
}

async fn wait_for_shared_enrollment(path: &Path) -> anyhow::Result<EnrollmentLock> {
    loop {
        if let Ok(contents) = std::fs::read_to_string(path) {
            if let Ok(config) = serde_json::from_str::<CloudConfig>(&contents) {
                if config.installation_token.is_some()
                    && (config.storage.is_some() || config.byo_storage.is_some())
                {
                    return acquire_enrollment_lock(path).await;
                }
                if let Some(pending) = config.pending_claim {
                    if pending.expires_at_ms > current_time_ms() {
                        note!(
                            "\nConnect this celld installation to the Managed Control Plane:\n\n  {}\n\nEnvironment: {}\n",
                            pending.verification_uri_complete, config.environment
                        );
                        return acquire_enrollment_lock(path).await;
                    }
                }
            }
        }
        if let Some(lock) = try_acquire_enrollment_lock(path).await? {
            return Ok(lock);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn normalized_control_url(value: &str) -> anyhow::Result<String> {
    let value = value.trim_end_matches('/');
    let url = url::Url::parse(value).context("invalid control plane URL")?;
    if url.scheme() != "https"
        && !(url.scheme() == "http" && matches!(url.host_str(), Some("localhost" | "127.0.0.1")))
    {
        return Err(anyhow!("control plane URL must use HTTPS"));
    }
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(anyhow!(
            "control plane URL must not contain a path, query, or fragment"
        ));
    }
    Ok(value.to_string())
}

fn validate_environment(value: &str) -> anyhow::Result<()> {
    if value.is_empty() || value.len() > 32 {
        return Err(anyhow!("environment must be 1–32 characters"));
    }
    let mut characters = value.chars();
    if !characters
        .next()
        .is_some_and(|character| character.is_ascii_lowercase())
        || !characters.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
    {
        return Err(anyhow!(
            "environment must start with a lowercase letter and contain only lowercase letters, numbers, and hyphens"
        ));
    }
    Ok(())
}

fn instance_name() -> String {
    std::env::var("CELLD_INSTANCE_NAME").unwrap_or_else(|_| machine_hostname())
}

fn machine_hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
        })
        .map(|value| value.trim().to_string())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 100
                && value.is_ascii()
                && !value.chars().any(char::is_control)
        })
        .unwrap_or_else(|| "celld".to_string())
}

// The presence session read the socket in the same select! as the heartbeat
// timer, so every tick dropped a read in progress. The fault is invisible from
// outside: the session reconnects and only the messages in the window are gone,
// so frame-level observation is required.

fn print_connect_help() {
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text)
        .help("Connect this celld installation to the Managed Control Plane.\n\nUSAGE:\n  celld connect [--env prod] [--control-url https://celld.dev] [--force]");
}

fn print_credentials_help() {
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text)
        .help("Refresh a staged Managed Control Plane credential.\n\nUSAGE:\n  celld credentials refresh\n\nRunning celld processes sharing this installation automatically reload after the replacement is saved. Unix processes re-exec; other platforms exit 75 for their supervisor.");
}

fn print_disconnect_help() {
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text)
        .help("Revoke and disconnect this Managed Control Plane installation.\n\nUSAGE:\n  celld disconnect --yes\n\nStop every celld process sharing this installation first. Other installations in the fleet are unaffected.");
}
