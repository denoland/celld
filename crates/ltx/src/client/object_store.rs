//! client::object_store — S3/R2/MinIO `ReplicaClient` via the `object_store` crate.
//!
//! Ported (behavior only) from litestream@v0.5.11 `s3/replica_client.go`. We do
//! **not** use the Go AWS SDK; instead the five `ReplicaClient` operations are
//! mapped onto `object_store::ObjectStore` (the `s3` cargo feature wires the
//! `object_store::aws::AmazonS3Builder` backend). The behavioral invariants from
//! the upstream conformance suite are kept:
//!
//!   * key/path scheme `{path}/{level:04x}/{min}-{max}.ltx`
//!     (s3/replica_client.go:629, 677, 1040-1042);
//!   * the 5 MiB single-PUT vs multipart threshold
//!     (s3/replica_client.go:99 + the Go uploader default);
//!   * list + seek-skip on `min_txid < seek`, ascending TXID order
//!     (s3/replica_client.go:1530-1533);
//!   * `NoSuchKey` → `os.ErrNotExist` error mapping
//!     (s3/replica_client.go:647-649, 1662-1668);
//!   * batch DELETE up to 1000 keys per call with per-key error surfacing
//!     (s3/replica_client.go:1028-1101).
//!
//! The provider-defaults table (`ParseHost`, path-style flags, endpoint env
//! var) is a faithful port of `NewReplicaClientFromURL`
//! (s3/replica_client.go:133-314) so a `s3://…` URL configures the same way.
//!
//! This whole module is gated behind `#[cfg(feature = "s3")]` because it needs
//! `object_store`'s AWS backend.

#![cfg(feature = "s3")]

use std::sync::Arc;

use chrono::{DateTime, SecondsFormat};
use futures_util::stream::{StreamExt, TryStreamExt};
use object_store::aws::{
    AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, AwsAuthorizer, AwsCredential,
};
use object_store::client::HttpRequestBody;
use object_store::path::Path as ObjPath;
use object_store::{
    Attribute, AttributeValue, Attributes, ClientOptions, GetOptions, GetRange, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, RetryConfig,
};

use crate::paged::RangeReader;

use crate::error::{Error, Result};
use crate::ltx::{self, FileInfo};
use crate::replica_url::{
    self, bool_query_value, ensure_endpoint_scheme, region_from_s3_arn, ParsedReplicaUrl,
};
use crate::TXID;

use super::ReplicaClient;

/// The standard Litestream S3 metadata key for an LTX header timestamp.
const METADATA_KEY_TIMESTAMP: &str = "litestream-timestamp";

/// The retry policy every replica store uses.
///
/// object_store defaults to 10 retries under a 180-second ceiling. That suits
/// a client whose caller has nowhere else to go. A replica store always has
/// somewhere else to go: a failed segment read is retried by the restore that
/// asked for it, and a failed upload is retried by the next replication turn,
/// so a long ladder inside one request buys nothing and delays the answer.
///
/// The ceiling matters most on a request a person is waiting for. A read-only
/// cell inspection reaches this store, and a definite failure under the
/// default policy takes seconds of pure backoff before the caller learns
/// anything. Three attempts still absorb an ordinary transient error.
pub fn replica_retry_config() -> RetryConfig {
    RetryConfig {
        max_retries: 3,
        retry_timeout: std::time::Duration::from_secs(30),
        ..RetryConfig::default()
    }
}

/// The same key with an underscore, for Azure Blob Storage. An Azure blob
/// metadata name must be a C# identifier, so it cannot hold a hyphen, and
/// the standard key is refused there.
const METADATA_KEY_TIMESTAMP_UNDERSCORE: &str = "litestream_timestamp";

/// Which object-metadata name carries the LTX header timestamp.
///
/// The name is per backend, not per deployment: Azure Blob Storage
/// refuses a hyphen in a metadata name, and every other supported store
/// accepts the standard Litestream key. The value and its format do not
/// change.
///
/// The key exists for external Litestream tooling, which reads
/// `litestream-timestamp` to do a timestamp restore without downloading
/// every candidate file. A renamed key therefore ends that interop on
/// Azure. celld itself never reads the key back.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TimestampMetadataKey {
    /// `litestream-timestamp`, the standard Litestream key.
    #[default]
    Litestream,
    /// `litestream_timestamp`, for a store that refuses a hyphen.
    Underscore,
}

impl TimestampMetadataKey {
    /// The metadata name this variant writes.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Litestream => METADATA_KEY_TIMESTAMP,
            Self::Underscore => METADATA_KEY_TIMESTAMP_UNDERSCORE,
        }
    }
}

/// Max keys S3 operates on per batch DELETE. `MaxKeys` (s3/replica_client.go:56).
pub const MAX_KEYS: usize = 1000;

/// Region used when none is specified. `DefaultRegion` (s3/replica_client.go:59).
pub const DEFAULT_REGION: &str = "us-east-1";

/// Multipart upload threshold: data at or above this size is uploaded with
/// `put_multipart`; below it, a single `put`. Matches the Go uploader's 5 MiB
/// `PartSize` default (s3/replica_client.go:99).
pub const MULTIPART_THRESHOLD: usize = 5 * 1024 * 1024;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for the S3/R2/MinIO backend.
///
/// Maps to the public fields of Go's `ReplicaClient` struct
/// (s3/replica_client.go:78-116). Zero/`None` values mean "use the backend
/// default".
#[derive(Debug, Clone, Default)]
pub struct ObjectStoreConfig {
    /// Bucket name (required).
    pub bucket: String,
    /// Key prefix within the bucket.
    pub path: String,
    /// AWS region.
    pub region: String,
    /// Custom endpoint (MinIO, R2, …); empty = native AWS.
    pub endpoint: String,
    /// Static access key id; empty = ambient credential chain.
    pub access_key_id: String,
    /// Static secret access key; empty = ambient credential chain.
    pub secret_access_key: String,
    /// Session token for temporary/scoped credentials (STS, R2 API tokens);
    /// empty = none. Required alongside temporary keys or signing fails.
    pub session_token: String,
    /// Force path-style addressing (required for MinIO/Backblaze/Supabase/Filebase).
    pub force_path_style: bool,
    /// Skip TLS verification (allows self-signed endpoints).
    pub skip_verify: bool,
    /// Multipart part size in bytes; 0 = default (5 MiB).
    pub part_size: u64,
    /// The metadata name for the LTX header timestamp. The default is the
    /// standard Litestream key; a host that injects an Azure store must
    /// select [`TimestampMetadataKey::Underscore`], because Azure refuses
    /// a hyphen in a metadata name.
    pub timestamp_metadata_key: TimestampMetadataKey,
}

impl ObjectStoreConfig {
    /// Construct from a parsed `s3://` URL, mirroring `NewReplicaClientFromURL`
    /// (s3/replica_client.go:133-314): host → bucket/region/endpoint/path-style
    /// (or ARN), query-param overrides (camelCase ↔ hyphenated aliases), the
    /// `AWS_*`/`LITESTREAM_*` env credentials, the `LITESTREAM_S3_ENDPOINT` env
    /// fallback, and the provider-specific path-style defaults for
    /// MinIO/Backblaze/Filebase/Supabase.
    pub fn from_url(parsed: &ParsedReplicaUrl) -> Result<Self> {
        let host = &parsed.host;
        let query = &parsed.query;

        // Host → bucket/region/endpoint/forcePathStyle (or ARN access point).
        let (bucket, mut region, mut endpoint, mut force_path_style) = if host.starts_with("arn:") {
            (host.clone(), region_from_s3_arn(host), String::new(), false)
        } else {
            parse_host(host)
        };

        let q = Some(query);

        // endpoint query param: ensure scheme, default to path-style for custom
        // endpoints unless force-path-style is explicitly set to false.
        let q_endpoint = query.get("endpoint");
        if !q_endpoint.is_empty() {
            let (ep, _) = ensure_endpoint_scheme(q_endpoint);
            endpoint = ep;
            match bool_query_value(q, &["forcePathStyle", "force-path-style"]) {
                Some(false) => {}
                _ => force_path_style = true,
            }
        }
        let q_region = query.get("region");
        if !q_region.is_empty() {
            region = q_region.to_string();
        }
        if let Some(v) = bool_query_value(q, &["forcePathStyle", "force-path-style"]) {
            force_path_style = v;
        }
        let mut skip_verify = false;
        if let Some(v) = bool_query_value(q, &["skipVerify", "skip-verify"]) {
            skip_verify = v;
        }

        let mut part_size: u64 = 0;
        let v = query.get("partSize");
        let v2 = query.get("part-size");
        if !v.is_empty() {
            if let Ok(n) = v.parse::<u64>() {
                if n > 0 {
                    part_size = n;
                }
            }
        } else if !v2.is_empty() {
            if let Ok(n) = v2.parse::<u64>() {
                if n > 0 {
                    part_size = n;
                }
            }
        }

        if bucket.is_empty() {
            return Err(Error::Other("bucket required for s3 replica URL".into()));
        }

        // Track whether forcePathStyle was explicitly set via query param
        // (s3/replica_client.go:208) — this gates the env-var/provider defaults.
        let force_path_style_set =
            !query.get("forcePathStyle").is_empty() || !query.get("force-path-style").is_empty();

        // Static credentials from env (AWS_* preferred, then LITESTREAM_*).
        let mut access_key_id = String::new();
        let mut secret_access_key = String::new();
        if let Some(v) = nonempty_env("AWS_ACCESS_KEY_ID") {
            access_key_id = v;
        } else if let Some(v) = nonempty_env("LITESTREAM_ACCESS_KEY_ID") {
            access_key_id = v;
        }
        if let Some(v) = nonempty_env("AWS_SECRET_ACCESS_KEY") {
            secret_access_key = v;
        } else if let Some(v) = nonempty_env("LITESTREAM_SECRET_ACCESS_KEY") {
            secret_access_key = v;
        }
        let session_token = nonempty_env("AWS_SESSION_TOKEN")
            .or_else(|| nonempty_env("LITESTREAM_SESSION_TOKEN"))
            .unwrap_or_default();

        // LITESTREAM_S3_ENDPOINT env fallback (only when no endpoint yet).
        if endpoint.is_empty() {
            if let Some(v) = nonempty_env("LITESTREAM_S3_ENDPOINT") {
                let (ep, _) = ensure_endpoint_scheme(&v);
                endpoint = ep;
                if !force_path_style_set {
                    force_path_style = true;
                }
            }
        }

        // Provider detection for applying defaults.
        let is_filebase = replica_url::is_filebase_endpoint(&endpoint);
        let is_backblaze = replica_url::is_backblaze_endpoint(&endpoint);
        let is_minio = replica_url::is_minio_endpoint(&endpoint);
        let is_supabase = replica_url::is_supabase_endpoint(&endpoint);
        if !force_path_style_set && (is_filebase || is_backblaze || is_minio || is_supabase) {
            force_path_style = true;
        }

        Ok(ObjectStoreConfig {
            bucket,
            path: parsed.path.clone(),
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
            force_path_style,
            skip_verify,
            part_size,
            timestamp_metadata_key: TimestampMetadataKey::default(),
        })
    }

    /// Build the backing `Arc<dyn ObjectStore>` for this config. Public so a host
    /// can build one store for a bucket and share it across many
    /// [`ObjectStoreClient::with_store`] clients that differ only by key prefix
    /// — one connection pool for every cell on a node.
    ///
    /// An empty `access_key_id` and `secret_access_key` select the ambient AWS
    /// credential chain, which reads web identity, ECS task credentials, EKS
    /// Pod Identity, and instance metadata from the process environment. Set
    /// both fields to keep a caller's own credentials authoritative.
    pub fn build_store(&self) -> Result<Arc<dyn ObjectStore>> {
        Ok(self.build_s3()?)
    }

    /// The concrete S3 store, kept beside the erased one so the paged fault
    /// path can borrow its credential.
    fn build_s3(&self) -> Result<Arc<AmazonS3>> {
        if self.bucket.is_empty() {
            return Err(Error::Other("s3: bucket name is required".into()));
        }

        let region = if self.region.is_empty() {
            DEFAULT_REGION.to_string()
        } else {
            self.region.clone()
        };

        // Forward the credential inputs that object_store reads. A bare
        // `new()` reads no environment, so it falls through to IMDS on EKS,
        // and `from_env()` would also import endpoint and request settings
        // that belong to this explicit ObjectStoreConfig. So the allowlist
        // stops at the credential boundary, and the config below stays
        // authoritative.
        //
        // object_store 0.12.5 takes AWS_WEB_IDENTITY_TOKEN_FILE and
        // AWS_ROLE_ARN from the process environment rather than from these
        // builder fields, so that pair changes nothing today. It stays because
        // the session name and the STS endpoint beside it *are* read from the
        // builder, and a release that moves the pair to the builder must not
        // drop this client back to IMDS.
        let mut builder = AmazonS3Builder::new();
        for (name, key) in [
            (
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
            ),
            (
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                AmazonS3ConfigKey::ContainerCredentialsFullUri,
            ),
            (
                "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
                AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
            ),
            (
                "AWS_WEB_IDENTITY_TOKEN_FILE",
                AmazonS3ConfigKey::WebIdentityTokenFile,
            ),
            ("AWS_ROLE_ARN", AmazonS3ConfigKey::RoleArn),
            ("AWS_ROLE_SESSION_NAME", AmazonS3ConfigKey::RoleSessionName),
            ("AWS_ENDPOINT_URL_STS", AmazonS3ConfigKey::StsEndpoint),
            ("AWS_METADATA_ENDPOINT", AmazonS3ConfigKey::MetadataEndpoint),
        ] {
            if let Some(value) = nonempty_env(name) {
                builder = builder.with_config(key, value);
            }
        }
        let mut builder = builder
            .with_bucket_name(&self.bucket)
            .with_region(region)
            .with_retry(replica_retry_config())
            // Path-style ⇔ NOT virtual-hosted-style (s3/replica_client.go:258-263).
            .with_virtual_hosted_style_request(!self.force_path_style)
            // object_store disables S3 conditional puts by default. Keep the
            // established ETagMatch behavior, so a user of the returned store
            // can send PutMode::Create or PutMode::Update. Ordinary replica
            // PUTs remain unconditional.
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch);

        if !self.endpoint.is_empty() {
            // A plaintext or local endpoint must allow HTTP. skip_verify applies
            // only to TLS certificate validation.
            let allow_http = self.endpoint.starts_with("http://")
                || replica_url::is_local_endpoint(&self.endpoint);
            let client_options = ClientOptions::new()
                .with_allow_http(allow_http)
                .with_allow_invalid_certificates(self.skip_verify);
            builder = builder
                .with_endpoint(&self.endpoint)
                .with_client_options(client_options);
        }

        if !self.access_key_id.is_empty() {
            builder = builder.with_access_key_id(&self.access_key_id);
        }
        if !self.secret_access_key.is_empty() {
            builder = builder.with_secret_access_key(&self.secret_access_key);
        }
        if !self.session_token.is_empty() {
            builder = builder.with_token(&self.session_token);
        }

        let store = builder
            .build()
            .map_err(|e| Error::Other(format!("s3: build store: {e}").into()))?;
        Ok(Arc::new(store))
    }

    /// Effective multipart part size (`part_size`, or the 5 MiB default).
    fn effective_part_size(&self) -> usize {
        if self.part_size > 0 {
            self.part_size as usize
        } else {
            MULTIPART_THRESHOLD
        }
    }
}

/// Returns `Some(value)` for a non-empty env var, else `None`. Mirrors Go's
/// `if v := os.Getenv(k); v != ""` pattern (s3/replica_client.go:211-224).
fn nonempty_env(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => None,
    }
}

// ── ParseHost ─────────────────────────────────────────────────────────────────

/// Parse an S3 host into `(bucket, region, endpoint, force_path_style)`.
///
/// Direct port of `ParseHost` (s3/replica_client.go:1608-1652): MinIO-style
/// `bucket.host:port`, then the AWS / DigitalOcean / Backblaze / Filebase /
/// Scaleway provider patterns, falling back to "host *is* the bucket".
pub fn parse_host(host: &str) -> (String, String, String, bool) {
    // MinIO-style hosts: `bucket.host:port` (a colon and not a ".com").
    if host.contains(':') && !host.contains(".com") {
        // SplitN(host, ".", 2)
        if let Some((bucket, rest)) = host.split_once('.') {
            return (
                bucket.to_string(),
                DEFAULT_REGION.to_string(),
                format!("http://{rest}"),
                true,
            );
        }
        // No bucket in host, just host:port.
        return (String::new(), String::new(), format!("http://{host}"), true);
    }

    // AWS S3: `^(.+)\.s3(?:\.([^.]+))?\.amazonaws\.com$`
    if let Some((bucket, region)) = match_aws_s3(host) {
        return (bucket, region, String::new(), false);
    }
    // DigitalOcean: `^(?:(.+)\.)?([^.]+)\.digitaloceanspaces.com$`
    if let Some((bucket, region)) = match_two_label_suffix(host, ".digitaloceanspaces.com") {
        return (
            bucket,
            region.clone(),
            format!("https://{region}.digitaloceanspaces.com"),
            false,
        );
    }
    // Backblaze: `^(?:(.+)\.)?s3.([^.]+)\.backblazeb2.com$`
    if let Some((bucket, region)) = match_s3_region_suffix(host, ".backblazeb2.com") {
        return (
            bucket,
            region.clone(),
            format!("https://s3.{region}.backblazeb2.com"),
            true,
        );
    }
    // Filebase: `^(?:(.+)\.)?s3.filebase.com$`
    if let Some(bucket) = match_filebase(host) {
        return (bucket, String::new(), "s3.filebase.com".to_string(), false);
    }
    // Scaleway: `^(?:(.+)\.)?s3.([^.]+)\.scw\.cloud$`
    if let Some((bucket, region)) = match_s3_region_suffix(host, ".scw.cloud") {
        return (
            bucket,
            region.clone(),
            format!("s3.{region}.scw.cloud"),
            false,
        );
    }

    // Standard S3: the host is the bucket name.
    (host.to_string(), String::new(), String::new(), false)
}

/// `^(.+)\.s3(?:\.([^.]+))?\.amazonaws\.com$` → (bucket, region).
fn match_aws_s3(host: &str) -> Option<(String, String)> {
    let rest = host.strip_suffix(".amazonaws.com")?;
    // rest = "<bucket>.s3" or "<bucket>.s3.<region>"
    if let Some(bucket) = rest.strip_suffix(".s3") {
        if bucket.is_empty() {
            return None;
        }
        return Some((bucket.to_string(), String::new()));
    }
    // "<bucket>.s3.<region>": find the ".s3." separator; region is a single
    // label ([^.]+) — i.e. the remainder after ".s3." must contain no dot.
    let idx = rest.find(".s3.")?;
    let bucket = &rest[..idx];
    let region = &rest[idx + 4..];
    if bucket.is_empty() || region.is_empty() || region.contains('.') {
        return None;
    }
    Some((bucket.to_string(), region.to_string()))
}

/// `^(?:(.+)\.)?([^.]+)\.<suffix>$` → (bucket, region). `suffix` starts with '.'.
fn match_two_label_suffix(host: &str, suffix: &str) -> Option<(String, String)> {
    let rest = host.strip_suffix(suffix)?;
    if rest.is_empty() {
        return None;
    }
    // The last label before the suffix is the region; anything before it
    // (optionally) is the bucket.
    match rest.rfind('.') {
        Some(i) => {
            let bucket = &rest[..i];
            let region = &rest[i + 1..];
            if region.is_empty() {
                return None;
            }
            Some((bucket.to_string(), region.to_string()))
        }
        None => Some((String::new(), rest.to_string())),
    }
}

/// `^(?:(.+)\.)?s3.([^.]+)\.<suffix>$` → (bucket, region). `suffix` starts with '.'.
fn match_s3_region_suffix(host: &str, suffix: &str) -> Option<(String, String)> {
    let rest = host.strip_suffix(suffix)?;
    // rest = "[bucket.]s3.<region>"; region is one label ([^.]+).
    // Bucket-less form: rest == "s3.<region>".
    if let Some(region) = rest.strip_prefix("s3.") {
        if region.is_empty() || region.contains('.') {
            return None;
        }
        return Some((String::new(), region.to_string()));
    }
    // Bucketed form: rest == "<bucket>.s3.<region>".
    let sep = rest.find(".s3.")?;
    let bucket = &rest[..sep];
    let region = &rest[sep + 4..];
    if bucket.is_empty() || region.is_empty() || region.contains('.') {
        return None;
    }
    Some((bucket.to_string(), region.to_string()))
}

/// `^(?:(.+)\.)?s3.filebase.com$` → bucket.
fn match_filebase(host: &str) -> Option<String> {
    if host == "s3.filebase.com" {
        return Some(String::new());
    }
    let bucket = host.strip_suffix(".s3.filebase.com")?;
    if bucket.is_empty() {
        None
    } else {
        Some(bucket.to_string())
    }
}

// ── Client ────────────────────────────────────────────────────────────────────

/// Concrete S3/R2/MinIO backend, wrapping a lazily-initialised
/// `Arc<dyn ObjectStore>`. The config-driven path builds an S3 store, but
/// [`Self::with_store`] accepts any prebuilt `ObjectStore` — the five
/// replica operations are provider-neutral, and a host can inject e.g. a
/// GCS or in-memory store.
///
/// Mirrors Go `ReplicaClient` (s3/replica_client.go:78-116). The inner store is
/// created on the first call that needs it (`OnceCell`, mirroring `Init`,
/// s3/replica_client.go:322-477), so construction is infallible and race-free.
pub struct ObjectStoreClient {
    store: tokio::sync::OnceCell<Arc<dyn ObjectStore>>,
    /// The concrete store when this client built its own; `with_store`
    /// leaves it empty, and the paged reader is then unavailable.
    s3: tokio::sync::OnceCell<Arc<AmazonS3>>,
    config: ObjectStoreConfig,
}

impl std::fmt::Debug for ObjectStoreClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreClient")
            .field("config", &self.config)
            .field("initialized", &self.store.initialized())
            .finish()
    }
}

impl ObjectStoreClient {
    /// Create a client from config (no I/O; the store is built on first use).
    pub fn new(config: ObjectStoreConfig) -> Self {
        ObjectStoreClient {
            store: tokio::sync::OnceCell::new(),
            s3: tokio::sync::OnceCell::new(),
            config,
        }
    }

    /// Create a client directly from an already-built `ObjectStore`, such as an
    /// in-memory store or a pre-configured backend.
    pub fn with_store(config: ObjectStoreConfig, store: Arc<dyn ObjectStore>) -> Self {
        let cell = tokio::sync::OnceCell::new();
        cell.set(store).ok();
        ObjectStoreClient {
            store: cell,
            s3: tokio::sync::OnceCell::new(),
            config,
        }
    }

    /// Get-or-build the inner store, once.
    async fn store(&self) -> Result<&Arc<dyn ObjectStore>> {
        self.store
            .get_or_try_init(|| async {
                let s3 = self.config.build_s3()?;
                self.s3.set(s3.clone()).ok();
                Ok(s3 as Arc<dyn ObjectStore>)
            })
            .await
    }

    /// A blocking ranged reader over this client's objects for the paged
    /// VFS. It snapshots the store's credential now (an async fetch on the
    /// caller's runtime) so each fault can sign its own request on whatever
    /// thread SQLite is on, with no runtime in the loop.
    pub async fn blocking_range_reader(&self) -> Result<Box<dyn RangeReader>> {
        let config = &self.config;
        // Only native S3 is read by a signed request of our own; any other
        // store — the deterministic simulation's, a directory, memory —
        // is read through the store on a helper thread.
        let store = self.store().await?;
        if !store.to_string().starts_with("AmazonS3") {
            return Ok(Box::new(StoreRangeReader::new(
                store.clone(),
                config.path.clone(),
            )));
        }
        // celld's per-cell clients share one pre-built store (`with_store`),
        // so there is no concrete store to borrow a provider from; the static
        // keys the config carries are the credential. Only an ambient
        // credential chain needs the provider, and that needs a store this
        // client built itself.
        let mut provider = None;
        let credential = if !config.access_key_id.is_empty() {
            Arc::new(AwsCredential {
                key_id: config.access_key_id.clone(),
                secret_key: config.secret_access_key.clone(),
                token: (!config.session_token.is_empty()).then(|| config.session_token.clone()),
            })
        } else {
            self.store().await?;
            let s3 = self.s3.get().ok_or_else(|| {
                Error::Other(
                    "paged reads over an ambient credential chain need a store this client built"
                        .into(),
                )
            })?;
            provider = Some(s3.credentials().clone());
            s3.credentials()
                .get_credential()
                .await
                .map_err(map_os_error)?
        };
        let region = if config.region.is_empty() {
            DEFAULT_REGION.to_string()
        } else {
            config.region.clone()
        };
        // Mirror the store's own addressing so the signed host matches what
        // the bucket expects: virtual-hosted unless path-style was forced.
        let base = if config.endpoint.is_empty() {
            format!("https://{}.s3.{region}.amazonaws.com", config.bucket)
        } else if config.force_path_style {
            format!(
                "{}/{}",
                config.endpoint.trim_end_matches('/'),
                config.bucket
            )
        } else {
            let (scheme, host) = config
                .endpoint
                .split_once("://")
                .ok_or_else(|| Error::Other("s3: endpoint has no scheme".into()))?;
            format!(
                "{scheme}://{}.{}",
                config.bucket,
                host.trim_end_matches('/')
            )
        };
        let tls = ureq::tls::TlsConfig::builder()
            .disable_verification(config.skip_verify)
            .build();
        // A fault runs on the caller's thread — for the actor's queries the
        // node's core — so this budget is how long one slow object read can
        // stall every cell on the node. A normal fault is 65-200 ms; 10 s is
        // fifty times that, and the single retry below bounds a stall at
        // about 20 s instead of the 90 s three 30 s attempts allowed.
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(FAULT_TIMEOUT))
            .http_status_as_error(false)
            .tls_config(tls)
            .build()
            .new_agent();
        Ok(Box::new(S3RangeReader {
            credential: std::sync::RwLock::new(credential),
            provider,
            region,
            base,
            prefix: config.path.clone(),
            agent,
        }))
    }

    /// Returns whether the replica epoch prefix contains an object.
    ///
    /// The caller needs only an existence proof, so it stops the recursive,
    /// delimiter-less listing after the first object. An empty or non-empty
    /// prefix therefore costs one list request instead of one request for each
    /// compaction level.
    pub async fn has_any_object(&self) -> Result<bool> {
        let store = self.store().await?;
        let prefix = ObjPath::from(self.root_prefix());
        store
            .list(Some(&prefix))
            .next()
            .await
            .transpose()
            .map(|object| object.is_some())
            .map_err(map_os_error)
    }

    /// Build the S3 key for an LTX file: `{path}/{level:04x}/{min}-{max}.ltx`.
    /// Ported from s3/replica_client.go:629, 677, 1040-1042.
    fn ltx_key(&self, level: i32, min_txid: TXID, max_txid: TXID) -> String {
        let filename = ltx::format_filename(min_txid, max_txid);
        format!("{}/{:04x}/{}", self.config.path, level, filename)
    }

    /// Prefix for listing a level: `{path}/{level:04x}/`.
    /// Ported from s3/replica_client.go:1363.
    fn level_prefix(&self, level: i32) -> String {
        format!("{}/{:04x}/", self.config.path, level)
    }

    /// Root prefix for delete-all: `{path}/`. (s3/replica_client.go:1114).
    fn root_prefix(&self) -> String {
        format!("{}/", self.config.path)
    }

    /// The highest transaction any level holds, from one listing of the
    /// whole path. A caller that only needs the watermark, such as a log
    /// recovery deciding which gathered rows the bucket already covers, pays
    /// one round trip here instead of one per level.
    pub async fn max_txid_all_levels(&self) -> Result<TXID> {
        let store = self.store().await?;
        let prefix = ObjPath::from(self.root_prefix());
        let mut listed = store.list(Some(&prefix));
        let mut max = TXID(0);
        while let Some(meta) = listed.try_next().await.map_err(map_os_error)? {
            let name = meta.location.filename().unwrap_or("");
            if let Ok((_, max_txid)) = ltx::parse_filename(name) {
                max = max.max(max_txid);
            }
        }
        Ok(max)
    }
}

/// Map an `object_store::Error` to `crate::Error`, preserving NotFound as
/// `io::ErrorKind::NotFound` so callers keep working with the std error kind.
/// Mirrors `isNotExists` → `os.ErrNotExist` (s3/replica_client.go:647-649,
/// 1662-1668).
fn map_os_error(e: object_store::Error) -> Error {
    match e {
        object_store::Error::NotFound { .. } => {
            Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, e))
        }
        other => Error::Other(Box::new(other)),
    }
}

#[async_trait::async_trait]
impl ReplicaClient for ObjectStoreClient {
    async fn ltx_files(&self, level: i32, seek: TXID) -> Result<Vec<FileInfo>> {
        self.ltx_files_bounded(level, seek, usize::MAX).await
    }

    async fn ltx_files_bounded(
        &self,
        level: i32,
        seek: TXID,
        limit: usize,
    ) -> Result<Vec<FileInfo>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let store = self.store().await?;
        let prefix = ObjPath::from(self.level_prefix(level));
        // The offset ends immediately before every filename with this minimum
        // TXID. S3 and GCS can push it into the listing request, so an additive
        // compaction does not scan the complete retained L0 history. The
        // supported production stores return each page in lexical order. The
        // compactor validates continuity and fails closed for an unordered
        // custom store.
        let offset = ObjPath::from(format!("{}{:016x}", self.level_prefix(level), seek.0));
        let mut listed = store.list_with_offset(Some(&prefix), &offset);
        let mut infos = Vec::with_capacity(limit.min(256));
        while let Some(meta) = listed.try_next().await.map_err(map_os_error)? {
            let name = meta.location.filename().unwrap_or("");
            let (min_txid, max_txid) = match ltx::parse_filename(name) {
                Ok(t) => t,
                Err(_) => continue, // skip non-LTX keys
            };
            if min_txid < seek {
                continue;
            }
            infos.push(FileInfo {
                level,
                min_txid,
                max_txid,
                size: meta.size as i64,
                created_at: Some(std::time::SystemTime::from(meta.last_modified)),
                ..Default::default()
            });
            if infos.len() == limit {
                break;
            }
        }

        // Iterator contract: ascending by (level, min_txid, max_txid).
        infos.sort_by(|a, b| {
            (a.level, a.min_txid.0, a.max_txid.0).cmp(&(b.level, b.min_txid.0, b.max_txid.0))
        });
        Ok(infos)
    }

    async fn open_ltx_file(&self, level: i32, min_txid: TXID, max_txid: TXID) -> Result<Vec<u8>> {
        let store = self.store().await?;
        let key = ObjPath::from(self.ltx_key(level, min_txid, max_txid));

        let result = match store.get(&key).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("replica: get object {key}: not found"),
                )));
            }
            Err(e) => return Err(map_os_error(e)),
        };

        let bytes = result.bytes().await.map_err(map_os_error)?;
        Ok(bytes.to_vec())
    }

    async fn read_range(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        let store = self.store().await?;
        let key = ObjPath::from(self.ltx_key(level, min_txid, max_txid));
        let options = GetOptions {
            range: Some(GetRange::Bounded(offset..offset.saturating_add(len))),
            ..Default::default()
        };
        let result = match store.get_opts(&key, options).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("replica: get range {key}: not found"),
                )));
            }
            Err(e) => return Err(map_os_error(e)),
        };
        let bytes = result.bytes().await.map_err(map_os_error)?;
        Ok(bytes.to_vec())
    }

    async fn write_ltx_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        data: &[u8],
    ) -> Result<FileInfo> {
        let store = self.store().await?;

        // Preserve the LTX header timestamp in the standard Litestream object
        // metadata. This costs no extra request and lets Litestream perform an
        // accurate timestamp restore without downloading every candidate file.
        // The name is per backend, because Azure refuses a hyphen in it.
        let header = ltx::Header::parse(data)?;
        let created_at = std::time::UNIX_EPOCH
            + std::time::Duration::from_millis(header.timestamp.max(0) as u64);
        let mut attributes = Attributes::new();
        attributes.insert(
            Attribute::Metadata(self.config.timestamp_metadata_key.as_str().into()),
            AttributeValue::from(format_rfc3339_nano(header.timestamp)?),
        );
        let key = ObjPath::from(self.ltx_key(level, min_txid, max_txid));

        // Multipart threshold: < 5 MiB → single PUT; ≥ 5 MiB → multipart with
        // fixed-size parts. Ported from the Go uploader's 5 MiB PartSize default
        // (s3/replica_client.go:99, brief §5.1).
        let part_size = self.config.effective_part_size();
        if data.len() < MULTIPART_THRESHOLD {
            let payload = PutPayload::from(data.to_vec());
            let options = PutOptions {
                attributes,
                ..Default::default()
            };
            store
                .put_opts(&key, payload, options)
                .await
                .map_err(|e| Error::Other(format!("replica: upload to {key}: {e}").into()))?;
        } else {
            let options = PutMultipartOptions {
                attributes,
                ..Default::default()
            };
            let mut upload = store
                .put_multipart_opts(&key, options)
                .await
                .map_err(|e| Error::Other(format!("replica: upload to {key}: {e}").into()))?;
            // Keep every fallible step inside one result. Otherwise, a new
            // early return can leave completed parts in the storage service.
            let upload_result: Result<()> = async {
                // Upload in fixed-size parts (each ≥ 5 MiB except possibly the
                // last, matching object_store's part-size requirement).
                for chunk in data.chunks(part_size.max(MULTIPART_THRESHOLD)) {
                    upload
                        .put_part(PutPayload::from(chunk.to_vec()))
                        .await
                        .map_err(|e| {
                            Error::Other(format!("replica: upload part to {key}: {e}").into())
                        })?;
                }
                upload.complete().await.map_err(|e| {
                    Error::Other(format!("replica: complete upload to {key}: {e}").into())
                })?;
                Ok(())
            }
            .await;
            if let Err(upload_error) = upload_result {
                if let Err(abort_error) = upload.abort().await {
                    tracing::warn!(
                        %abort_error,
                        key = %key,
                        "failed LTX multipart upload could not be aborted"
                    );
                }
                return Err(upload_error);
            }
        }

        Ok(FileInfo {
            level,
            min_txid,
            max_txid,
            size: data.len() as i64,
            created_at: Some(created_at),
            ..Default::default()
        })
    }

    async fn delete_ltx_files(&self, files: &[FileInfo]) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        let store = self.store().await?;

        // Build the key list, then delete in batches of MAX_KEYS via the
        // store's delete_stream, surfacing per-key errors (brief §5.5).
        let keys: Vec<ObjPath> = files
            .iter()
            .map(|info| ObjPath::from(self.ltx_key(info.level, info.min_txid, info.max_txid)))
            .collect();

        for batch in keys.chunks(MAX_KEYS) {
            delete_batch(store.as_ref(), batch, /*ignore_missing=*/ true).await?;
        }
        Ok(())
    }

    async fn delete_all(&self) -> Result<()> {
        let store = self.store().await?;
        let prefix = ObjPath::from(self.root_prefix());

        // List everything under the path prefix, then batch-delete.
        // (s3/replica_client.go:1104-1148).
        let keys: Vec<ObjPath> = store
            .list(Some(&prefix))
            .map_ok(|m| m.location)
            .map_err(map_os_error)
            .try_collect()
            .await?;

        for batch in keys.chunks(MAX_KEYS) {
            delete_batch(store.as_ref(), batch, /*ignore_missing=*/ true).await?;
        }
        Ok(())
    }
}

/// Delete a batch of keys via `delete_stream`, surfacing per-key errors.
///
/// When `ignore_missing` is set, `NotFound` is tolerated (delete is idempotent —
/// the file client swallows ENOENT the same way), but every other per-key error
/// is returned (brief §5.5: do not silently swallow partial failures).
async fn delete_batch(
    store: &dyn ObjectStore,
    keys: &[ObjPath],
    ignore_missing: bool,
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let owned: Vec<ObjPath> = keys.to_vec();
    let stream = futures_util::stream::iter(owned.into_iter().map(Ok));
    let mut results = store.delete_stream(stream.boxed());
    while let Some(res) = results.next().await {
        match res {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) if ignore_missing => {}
            Err(e) => return Err(map_os_error(e)),
        }
    }
    Ok(())
}

/// Format a Unix-millisecond timestamp like Go's `time.RFC3339Nano`.
fn format_rfc3339_nano(unix_millis: i64) -> Result<String> {
    let mut timestamp = DateTime::from_timestamp_millis(unix_millis.max(0))
        .ok_or_else(|| Error::Other("LTX timestamp is outside the RFC3339 range".into()))?
        .to_rfc3339_opts(SecondsFormat::AutoSi, true);

    // Chrono keeps millisecond precision as three digits. Go removes trailing
    // zeros, so `.500Z` becomes `.5Z`.
    if timestamp.contains('.') {
        // The pop must stay outside debug_assert!, which vanishes in release
        // builds and would leave the Z in place to be doubled below.
        let suffix = timestamp.pop();
        debug_assert_eq!(suffix, Some('Z'));
        while timestamp.ends_with('0') {
            timestamp.pop();
        }
        timestamp.push('Z');
    }
    Ok(timestamp)
}

#[doc(hidden)]
pub mod internal {
    use super::*;

    pub const METADATA_KEY_TIMESTAMP: &str = super::METADATA_KEY_TIMESTAMP;
    pub const METADATA_KEY_TIMESTAMP_UNDERSCORE: &str = super::METADATA_KEY_TIMESTAMP_UNDERSCORE;

    pub fn format_rfc3339_nano(unix_millis: i64) -> Result<String> {
        super::format_rfc3339_nano(unix_millis)
    }

    pub fn ltx_key(client: &ObjectStoreClient, level: i32, min: TXID, max: TXID) -> String {
        client.ltx_key(level, min, max)
    }

    pub fn level_prefix(client: &ObjectStoreClient, level: i32) -> String {
        client.level_prefix(level)
    }

    pub fn root_prefix(client: &ObjectStoreClient) -> String {
        client.root_prefix()
    }

    pub fn map_os_error(error: object_store::Error) -> Error {
        super::map_os_error(error)
    }
}

/// Bytes the SigV4 canonical URI leaves unencoded, plus the segment
/// delimiter: A-Z a-z 0-9 `-` `.` `_` `~` and `/`.
const KEY_ENCODE_SET: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// One ranged read's budget on the fault path (see the agent below).
const FAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The paged VFS's fault-path reader: a SigV4-signed ranged GET over a
/// blocking socket. See [`RangeReader`] for why it must not touch a runtime.
///
/// The credential is the snapshot taken at activation; a credential that
/// rotates during a long residency makes later faults fail with 403, which
/// surfaces as an I/O error on that read. Refreshing it needs the async
/// provider and belongs to a re-activation, not to a fault.
/// The fault path over any [`ObjectStore`] that is not native S3: the
/// deterministic simulation's store, a directory, a memory store. Reads run
/// on one helper thread with its own runtime; the calling thread blocks on
/// the reply, which is the fault path's contract. The helper is a thread,
/// not a task on any caller's runtime, so the same-runtime deadlocks that
/// sank every bridge on the fleet do not apply (`refresh_aws_credential`
/// takes the same shape).
pub struct StoreRangeReader {
    requests: std::sync::mpsc::Sender<RangeRequest>,
    prefix: String,
}

struct RangeRequest {
    key: String,
    offset: u64,
    len: u64,
    reply: std::sync::mpsc::Sender<Result<Vec<u8>>>,
}

impl StoreRangeReader {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        let (requests, inbox) = std::sync::mpsc::channel::<RangeRequest>();
        std::thread::Builder::new()
            .name("paged-store-reader".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("paged store reader runtime");
                for request in inbox {
                    let path = ObjPath::from(request.key);
                    let range = request.offset..request.offset + request.len;
                    let out = runtime
                        .block_on(store.get_range(&path, range))
                        .map(|bytes| bytes.to_vec())
                        .map_err(map_os_error);
                    let _ = request.reply.send(out);
                }
            })
            .expect("spawn the paged store reader");
        Self { requests, prefix }
    }
}

impl RangeReader for StoreRangeReader {
    fn read_range(&self, info: &FileInfo, offset: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let filename = ltx::format_filename(info.min_txid, info.max_txid);
        let key = format!("{}/{:04x}/{filename}", self.prefix, info.level);
        let (reply, inbox) = std::sync::mpsc::channel();
        let stopped = || Error::Other("the paged store reader stopped".into());
        self.requests
            .send(RangeRequest {
                key,
                offset,
                len,
                reply,
            })
            .map_err(|_| stopped())?;
        inbox.recv().map_err(|_| stopped())?
    }
}

pub struct S3RangeReader {
    credential: std::sync::RwLock<Arc<AwsCredential>>,
    /// The store's provider when the credential came from an ambient chain
    /// (STS, instance roles), which rotates; `None` for static keys.
    provider: Option<object_store::aws::AwsCredentialProvider>,
    region: String,
    base: String,
    prefix: String,
    agent: ureq::Agent,
}

impl S3RangeReader {
    fn get(&self, url: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut signed = http::Request::builder()
            .method("GET")
            .uri(url)
            .header("range", format!("bytes={offset}-{}", offset + len - 1))
            .body(HttpRequestBody::empty())
            .map_err(|e| Error::Other(format!("paged: build request: {e}").into()))?;
        let credential = self.credential.read().unwrap().clone();
        AwsAuthorizer::new(&credential, "s3", &self.region).authorize(&mut signed, None);
        let mut request = self.agent.get(url);
        for (name, value) in signed.headers() {
            // ureq sets Host from the URL itself, identically to the signed
            // value; setting it again is refused.
            if name != http::header::HOST {
                let value = value
                    .to_str()
                    .map_err(|e| Error::Other(format!("paged: header: {e}").into()))?;
                request = request.header(name.as_str(), value);
            }
        }
        let mut response = request
            .call()
            .map_err(|e| Error::Io(std::io::Error::other(format!("paged get: {e}"))))?;
        if response.status() == http::StatusCode::FORBIDDEN {
            return Err(Error::Forbidden);
        }
        if response.status() != http::StatusCode::PARTIAL_CONTENT {
            // S3's error body names the code and, for a signature mismatch,
            // the canonical request it computed — the only way to see it.
            let status = response.status();
            let detail = response
                .body_mut()
                .with_config()
                .limit(8192)
                .read_to_string()
                .unwrap_or_default();
            // Not transient: the retry below is for transport errors only.
            return Err(Error::Other(
                format!("paged get: {status} for range {offset}+{len} of {url}: {detail}").into(),
            ));
        }
        let body = response
            .body_mut()
            .with_config()
            // ureq's limit is exclusive; the length check below is the real
            // bound.
            .limit(len + 1)
            .read_to_vec()
            .map_err(|e| Error::Io(std::io::Error::other(format!("paged body: {e}"))))?;
        if body.len() as u64 != len {
            return Err(Error::Io(std::io::Error::other(format!(
                "paged get: short range body {} of {len}",
                body.len()
            ))));
        }
        Ok(body)
    }
}

impl RangeReader for S3RangeReader {
    fn read_range(&self, info: &FileInfo, offset: u64, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let filename = ltx::format_filename(info.min_txid, info.max_txid);
        // SigV4's canonical URI percent-encodes every byte outside the
        // unreserved set — a `:` in a cell name is `%3A` — and R2 canonicalizes
        // what it receives. Signing the raw path therefore signs a different
        // string than the bucket checks (every fault a 403), so the path is
        // encoded before it is signed, and sent encoded.
        let key = format!("{}/{:04x}/{filename}", self.prefix, info.level);
        let key = percent_encoding::utf8_percent_encode(&key, KEY_ENCODE_SET);
        let url = format!("{}/{key}", self.base);
        // A transport hiccup on a 4KiB read is cheap to retry; a status
        // failure is not transient and returns at once.
        let mut attempt = 0;
        let mut refreshed = false;
        loop {
            match self.get(&url, offset, len) {
                Ok(body) => return Ok(body),
                // A rotated ambient credential fails with 403; take a fresh
                // one from the provider once, then treat a second 403 as
                // final.
                Err(Error::Forbidden) if !refreshed && self.provider.is_some() => {
                    refreshed = true;
                    match refresh_aws_credential(self.provider.as_ref().unwrap()) {
                        Some(fresh) => *self.credential.write().unwrap() = fresh,
                        None => return Err(Error::Forbidden),
                    }
                }
                Err(error) if attempt < 1 && matches!(error, Error::Io(_)) => {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_millis(100 * attempt));
                }
                Err(error) => return Err(error),
            }
        }
    }
}

/// Fetches a fresh credential from an ambient provider, from a thread that
/// is not on any runtime: the fault path runs on whatever thread SQLite is
/// on, which may be a runtime worker where `block_on` panics, so the async
/// provider is driven on a helper thread with its own runtime.
pub fn refresh_aws_credential(
    provider: &object_store::aws::AwsCredentialProvider,
) -> Option<Arc<AwsCredential>> {
    let provider = provider.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()?;
        runtime.block_on(provider.get_credential()).ok()
    })
    .join()
    .ok()
    .flatten()
}
