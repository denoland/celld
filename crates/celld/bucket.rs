// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The engine's single object-store client: the `object_store` crate
//! `celld-ltx` already links, bound to one bucket. Replaces aws-sdk-s3.
//! No call site streamed a body, so everything is in-memory `Bytes`.
//!
//! Two conditional-write dialects share the surface. An `s3://` (or
//! bare) spec speaks the S3 dialect: the CAS token is the etag, sent as
//! If-Match / If-None-Match with SigV4 credentials. An `az://` spec
//! speaks the same etag dialect on Azure Blob Storage: Put Blob honors
//! both headers, so only the client and the credentials differ. A
//! `gs://` spec speaks the Cloud Storage XML API dialect: the CAS token
//! is the object generation, sent as x-goog-if-generation-match with
//! OAuth credentials. The distinction is the dialect, not the endpoint —
//! GCS accepts S3-style requests on the same host but does not apply
//! If-Match to a PUT, so only the generation dialect can fence there.
//! Callers never see the difference: the token is an opaque `String` a
//! read answers and a conditional write consumes.
//!
//! Error contract, relied on by the self-fence: `put_cas` answers
//! `Ok(None)` only for a clean 412/409 rejection; every other failure
//! surfaces as `Err`. An `Err` is ambiguous — the write may have
//! committed — unless `cas_write_did_not_commit` recognizes an answer
//! that proves the store wrote no object. In the same spirit a response
//! that carries no CAS token is an error, never an empty token a later
//! conditional write would trust.

use anyhow::anyhow;
use anyhow::Context;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::aws::S3ConditionalPut;
use object_store::azure::authority_hosts;
use object_store::azure::AzureConfigKey;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::list::PaginatedListOptions;
use object_store::list::PaginatedListStore;
use object_store::path::Path;
use object_store::Attribute;
use object_store::Attributes;
use object_store::ClientOptions;
use object_store::Error;
use object_store::GetOptions;
use object_store::GetRange;
use object_store::MultipartUpload;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::PutMode;
use object_store::PutMultipartOptions;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::RetryConfig;
use object_store::UpdateVersion;
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

/// Explicit credentials for a managed installation; everything else comes
/// from the standard `AWS_*` environment.
pub struct StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// Which conditional-write dialect the bucket speaks, and therefore
/// what the opaque CAS token holds: the etag on S3 and on Azure Blob
/// Storage, the object generation on GCS. GCS ignores etags on writes,
/// so its tokens must come from the generation everywhere — reads,
/// heads, and put results. Azure needs no third dialect: Put Blob
/// applies If-None-Match and If-Match to the etag, exactly as S3 does,
/// so the two share [`Self::token`] and [`Self::update`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum StorageBackend {
    S3,
    Gcs,
    Azure,
    Local,
}

impl StorageBackend {
    fn scheme(self) -> &'static str {
        match self {
            StorageBackend::S3 => "s3",
            StorageBackend::Gcs => "gs",
            StorageBackend::Azure => "az",
            StorageBackend::Local => "dev",
        }
    }

    /// The CAS token a read or applied write answers: etag or generation,
    /// per dialect. A response without one cannot be fenced against, so a
    /// missing or empty token is an error — never an empty string a later
    /// conditional write would send as a real precondition.
    #[doc(hidden)]
    pub fn token(self, e_tag: Option<String>, version: Option<String>) -> anyhow::Result<String> {
        let (token, header) = match self {
            StorageBackend::S3 | StorageBackend::Azure | StorageBackend::Local => (e_tag, "ETag"),
            StorageBackend::Gcs => (version, "x-goog-generation"),
        };
        match token {
            Some(token) if !token.is_empty() => Ok(token),
            _ => Err(anyhow!(
                "response carries no {header}, so there is no CAS token"
            )),
        }
    }

    /// The precondition a conditional update sends for a held token.
    #[doc(hidden)]
    pub fn update(self, token: &str) -> UpdateVersion {
        match self {
            StorageBackend::S3 | StorageBackend::Azure | StorageBackend::Local => UpdateVersion {
                e_tag: Some(token.to_string()),
                version: None,
            },
            StorageBackend::Gcs => UpdateVersion {
                e_tag: None,
                version: Some(token.to_string()),
            },
        }
    }

    /// What a conditional write sends on the wire, named for an operator
    /// reading a probe failure. The dialects use different headers, so a
    /// message that names only one sends half the fleet looking in the
    /// wrong place.
    fn precondition(self) -> &'static str {
        match self {
            StorageBackend::S3 | StorageBackend::Azure | StorageBackend::Local => {
                "If-Match / If-None-Match"
            }
            StorageBackend::Gcs => "x-goog-if-generation-match",
        }
    }
}

/// One object-store bucket, optionally scoped to a key prefix. Cheap to
/// clone; each `open` builds its own HTTP transport, so a dedicated
/// instance also isolates its traffic.
#[derive(Clone)]
pub struct Bucket {
    pub store: Arc<dyn ObjectStore>,
    /// The same client as `store`, reached through the paginated listing
    /// trait. `ObjectStore::list_with_delimiter` drains every continuation
    /// page into one buffer before it returns, so it cannot answer "the
    /// first N" without paying for all of them. `PaginatedListStore` takes
    /// a page size and returns a resumption token, which is what bounds a
    /// listing's cost rather than only its output. It is a separate trait,
    /// so the concrete client has to be kept here as it is built —
    /// `Arc<dyn ObjectStore>` cannot be widened to it later.
    paginated: Arc<dyn PaginatedListStore>,
    /// Conditional writes only, built with retries OFF: a retried CAS put
    /// can land on the first attempt's own token change and report a clean
    /// 412 — converting "may have committed" into a false rejection. The
    /// ambiguity must surface as `Err` so the caller reconciles.
    pub cas_store: Arc<dyn ObjectStore>,
    pub backend: StorageBackend,
    /// Bucket name, for messages — the store is already bound to it.
    pub name: String,
    /// Empty, or a slash-terminated key prefix every operation is scoped
    /// to. Call sites keep forming unprefixed keys; this type is the one
    /// place that knows where in the bucket a fleet lives.
    pub prefix: String,
}

/// One page of [`Bucket::common_prefixes_page`].
pub struct CommonPrefixPage {
    /// The children this page listed, in the store's key order.
    pub prefixes: Vec<String>,
    /// Present exactly when the store truncated the page, so a caller
    /// learns that more children exist without a second request.
    pub page_token: Option<String>,
}

/// One bounded page of object descriptions from a recursive listing.
pub(crate) struct ObjectPage {
    pub objects: Vec<ObjectMeta>,
    pub page_token: Option<String>,
}

/// Whether a conditional write reached a provider-enforced conflict.
/// Azure reports a failed `If-None-Match` as `Precondition`, while some
/// stores report the same create conflict as `AlreadyExists`. Both are a
/// clean lost race. Every other error remains ambiguous.
#[doc(hidden)]
pub fn is_clean_cas_rejection(error: &Error) -> bool {
    matches!(
        error,
        Error::Precondition { .. } | Error::AlreadyExists { .. }
    )
}

/// Whether a failed conditional write proves that the store holds no new
/// object. A caller that renews a lease uses this to skip the readback
/// that resolves an ambiguous write, because the readback costs a round
/// trip out of the authority a store blip is already eating.
///
/// Every variant below is either a rejection the client made before it
/// sent a request, or a store answer that precedes the object write:
/// `object_store` maps 401 to `Unauthenticated` and 403 to
/// `PermissionDenied`, and each of the four object stores this engine
/// speaks to authorizes a `PUT` before it applies one. A `PUT` that
/// answers 404 reached no bucket, so it wrote nothing.
///
/// `Error::Generic` is absent on purpose, and it carries every 5xx, every
/// timeout and every connection failure. A connection that never opened
/// also wrote nothing, but `object_store` 0.12 keeps `RetryError` (and so
/// the `Connect` error kind) private, so no public API separates it from
/// a 500 that can have committed. Matching the message text would decide
/// a lease fence on a string, therefore the whole variant stays ambiguous
/// until the upstream type is reachable.
#[doc(hidden)]
pub fn cas_write_did_not_commit(error: &anyhow::Error) -> bool {
    // `put_cas` wraps the store error in context, so read the chain rather
    // than the outermost error. A chain that carries no `object_store`
    // error at all -- an applied write whose response had no CAS token --
    // does not match, which leaves it ambiguous. That is the safe answer.
    error.downcast_ref::<Error>().is_some_and(|error| {
        matches!(
            error,
            Error::Unauthenticated { .. }
                | Error::PermissionDenied { .. }
                | Error::NotFound { .. }
                | Error::InvalidPath { .. }
                | Error::NotSupported { .. }
                | Error::NotImplemented
                | Error::UnknownConfigurationKey { .. }
        )
    })
}

/// Split a `[s3://|gs://|az://]NAME[/PREFIX]` bucket spec into the
/// backend, the bucket name and a normalized key prefix: empty, or
/// slash-terminated. A spec without a scheme stays S3-compatible, and a
/// spec without a PREFIX keeps every key at the bucket root, so a fleet
/// provisioned before either existed never moves its objects.
/// Scheme names are ASCII case-insensitive, as URI schemes require.
///
/// On `az://` the NAME is the container, and the storage account comes
/// from `AZURE_STORAGE_ACCOUNT_NAME`. The second path segment is the key
/// prefix on all three schemes, so the account cannot live there without
/// making `az://` parse differently from the other two.
#[doc(hidden)]
pub fn split_spec(spec: &str) -> (StorageBackend, &str, String) {
    let strip_scheme = |scheme: &str| {
        spec.get(..scheme.len())
            .filter(|candidate| candidate.eq_ignore_ascii_case(scheme))
            .map(|_| &spec[scheme.len()..])
    };
    let (backend, spec) = if let Some(rest) = strip_scheme("gs://") {
        (StorageBackend::Gcs, rest)
    } else if let Some(rest) = strip_scheme("az://") {
        (StorageBackend::Azure, rest)
    } else if let Some(rest) = strip_scheme("s3://") {
        (StorageBackend::S3, rest)
    } else {
        (StorageBackend::S3, spec)
    };
    let (name, prefix) = spec.split_once('/').unwrap_or((spec, ""));
    let parts = prefix.split('/').filter(|part| !part.is_empty());
    (
        backend,
        name,
        parts.map(|part| format!("{part}/")).collect(),
    )
}

/// The S3 endpoint the environment supplies when no explicit one was
/// given: the AWS SDK's standard `AWS_ENDPOINT_URL`, then the older
/// `AWS_ENDPOINT` — the same variables `AmazonS3Builder::from_env`
/// injects into the client. Resolved here so the path-style decision and
/// the client always agree about whether an endpoint is in play.
#[doc(hidden)]
pub fn resolve_env_endpoint() -> Option<String> {
    ["AWS_ENDPOINT_URL", "AWS_ENDPOINT"]
        .iter()
        .find_map(|var| std::env::var(var).ok().filter(|value| !value.is_empty()))
}

/// The byte range a `get` asked for. R2 spells a range four ways and so
/// does `object_store`, so the request reaches the backend as the caller
/// wrote it rather than widened to a whole-object read.
#[derive(Clone, Copy)]
pub enum BlobRange {
    /// The whole object.
    Whole,
    /// `offset` onwards, to the end of the object.
    From(u64),
    /// `length` bytes starting at `offset`.
    Bounded { offset: u64, length: u64 },
    /// The last `length` bytes.
    Suffix(u64),
}

/// R2's `onlyIf`, split by who can answer it. The etag halves ride down
/// to the store as If-Match / If-None-Match, so no concurrent write slips
/// between the check and the read. The upload-time halves are checked
/// here: R2 compares milliseconds and an HTTP date carries only seconds,
/// so If-Modified-Since would answer a different question for every
/// object written in the same second as the bound.
#[derive(Default, Clone)]
pub struct BlobConditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub uploaded_before_ms: Option<i64>,
    pub uploaded_after_ms: Option<i64>,
}

impl BlobConditions {
    /// Whether an etag satisfies the halves the store checks. Used on the
    /// write path, which evaluates the whole condition itself against a
    /// head rather than sending it, because a write's precondition has to
    /// be paired with the CAS token of the very version it inspected.
    fn etag_met(&self, etag: Option<&str>) -> bool {
        let matches = |list: &str| match list.trim() {
            "*" => etag.is_some(),
            list => list
                .split(',')
                .any(|candidate| etag_eq(candidate, etag.unwrap_or_default())),
        };
        self.if_match.as_deref().is_none_or(&matches)
            && !self.if_none_match.as_deref().is_some_and(&matches)
    }

    /// Whether an upload time satisfies the halves this module checks.
    fn time_met(&self, uploaded_ms: i64) -> bool {
        self.uploaded_before_ms
            .is_none_or(|before| uploaded_ms < before)
            && self
                .uploaded_after_ms
                .is_none_or(|after| uploaded_ms > after)
    }

    fn is_empty(&self) -> bool {
        self.if_match.is_none()
            && self.if_none_match.is_none()
            && self.uploaded_before_ms.is_none()
            && self.uploaded_after_ms.is_none()
    }
}

/// Etag comparison, weak-marker and quote insensitive. R2 hands a Worker
/// the unquoted etag and takes either spelling back.
fn etag_eq(left: &str, right: &str) -> bool {
    let bare = |value: &str| {
        value
            .trim()
            .trim_start_matches("W/")
            .trim_matches('"')
            .to_string()
    };
    bare(left) == bare(right)
}

/// The HTTP headers and the user metadata an object carries. The five
/// headers are the ones every backend stores as headers, and are R2's
/// `httpMetadata` minus `cacheExpiry`, which no backend has a header for.
/// `metadata` is the backend's user metadata, kept under its own
/// `x-amz-meta-` / `x-goog-meta-` / `x-ms-meta-` prefix; what an R2
/// binding puts in there is the binding's business, not this module's.
#[derive(Default, Clone)]
pub struct BlobAttributes {
    pub content_type: Option<String>,
    pub content_language: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub cache_control: Option<String>,
    pub metadata: Vec<(String, String)>,
}

impl BlobAttributes {
    /// The headers and user metadata on a response. Every other attribute
    /// `object_store` parses is a transport detail, not the object's.
    fn read(attributes: &Attributes) -> Self {
        let value = |attribute: &Attribute| {
            attributes
                .get(attribute)
                .map(|value| value.as_ref().to_string())
        };
        Self {
            content_type: value(&Attribute::ContentType),
            content_language: value(&Attribute::ContentLanguage),
            content_disposition: value(&Attribute::ContentDisposition),
            content_encoding: value(&Attribute::ContentEncoding),
            cache_control: value(&Attribute::CacheControl),
            metadata: user_metadata(attributes),
        }
    }

    /// The same, as the store's write-side attribute set.
    fn write(&self) -> Attributes {
        let mut attributes = Attributes::new();
        let mut set = |attribute: Attribute, value: &Option<String>| {
            if let Some(value) = value {
                attributes.insert(attribute, value.clone().into());
            }
        };
        set(Attribute::ContentType, &self.content_type);
        set(Attribute::ContentLanguage, &self.content_language);
        set(Attribute::ContentDisposition, &self.content_disposition);
        set(Attribute::ContentEncoding, &self.content_encoding);
        set(Attribute::CacheControl, &self.cache_control);
        for (name, value) in &self.metadata {
            attributes.insert(
                Attribute::Metadata(Cow::Owned(name.clone())),
                value.clone().into(),
            );
        }
        attributes
    }
}

/// What every answer about an object knows, whatever asked for it.
pub struct BlobMeta {
    pub size: u64,
    pub etag: Option<String>,
    /// The backend's version id, where the bucket has versioning on.
    pub version: Option<String>,
    /// The conditional-write token for exactly this version, in the
    /// backend's dialect — the etag on S3 and Azure, the generation on
    /// GCS. A conditional write pairs the check it made on this metadata
    /// with the token, so a racing write cannot land in between.
    pub cas: Option<String>,
    pub uploaded_ms: i64,
    pub attributes: BlobAttributes,
}

/// A blob a `get` answered, with its body still on the wire.
pub struct Blob {
    pub meta: BlobMeta,
    /// The slice of the object this response carries, `(offset, length)`.
    /// A bounded read that runs past the end is clamped by the store, so
    /// this is what was served rather than what was asked for.
    pub range: (u64, u64),
    pub body: BoxStream<'static, Result<Vec<u8>, String>>,
}

/// What a conditional read found.
pub enum BlobRead {
    /// No such key.
    Missing,
    /// The key is there and the condition refused it. R2 answers the
    /// object's metadata with no body, so the caller sees what it has.
    Unmet(BlobMeta),
    Hit(Blob),
}

/// One object in a listing page.
pub struct BlobEntry {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub version: Option<String>,
    pub uploaded_ms: i64,
}

/// One page of a listing.
pub struct BlobPage {
    pub objects: Vec<BlobEntry>,
    /// The keys a delimiter rolled up, in key order and deduplicated.
    pub prefixes: Vec<String>,
    pub truncated: bool,
    /// The last key this page consumed, whether it was listed or rolled
    /// up. The next page resumes strictly after it, which is what R2's
    /// opaque cursor does.
    pub cursor: Option<String>,
}

/// The user metadata on a response, as name/value pairs. `object_store`
/// parses the backend's `x-amz-meta-` / `x-goog-meta-` / `x-ms-meta-`
/// headers into `Attribute::Metadata`; every other attribute is a standard
/// HTTP header and not part of R2's `customMetadata`.
fn user_metadata(attributes: &Attributes) -> Vec<(String, String)> {
    attributes
        .iter()
        .filter_map(|(attribute, value)| match attribute {
            Attribute::Metadata(name) => {
                Some((name.as_ref().to_string(), value.as_ref().to_string()))
            }
            _ => None,
        })
        .collect()
}

/// The paginated listing an injected store cannot serve.
///
/// A test build constructs a bucket over plain injected stores, and a
/// paginated listing serves an operator command rather than a node
/// decision, so no injected store reaches one. Refusing is louder than an
/// empty page, which a caller would read as a fleet that holds no cells.
#[cfg(celld_internal_tests)]
#[derive(Debug)]
struct UnpaginatedStore;

#[cfg(celld_internal_tests)]
#[async_trait::async_trait]
impl PaginatedListStore for UnpaginatedStore {
    async fn list_paginated(
        &self,
        _prefix: Option<&str>,
        _options: PaginatedListOptions,
    ) -> object_store::Result<object_store::list::PaginatedListResult> {
        Err(object_store::Error::NotSupported {
            source: "an injected store serves no paginated listing".into(),
        })
    }
}

impl Bucket {
    /// Builds a bucket from separate ordinary and paginated stores.
    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub fn with_paginated_store_for_test(
        store: Arc<dyn ObjectStore>,
        paginated: Arc<dyn PaginatedListStore>,
        prefix: String,
    ) -> Self {
        Self {
            cas_store: store.clone(),
            store,
            paginated,
            backend: StorageBackend::S3,
            name: "telemetry-test".to_string(),
            prefix,
        }
    }

    /// Builds a bucket over injected ordinary and conditional-write stores.
    #[cfg(celld_internal_tests)]
    #[doc(hidden)]
    pub fn with_stores(
        store: Arc<dyn ObjectStore>,
        cas_store: Arc<dyn ObjectStore>,
        backend: StorageBackend,
        name: String,
        prefix: String,
    ) -> Self {
        Self {
            store,
            paginated: Arc::new(UnpaginatedStore),
            cas_store,
            backend,
            name,
            prefix,
        }
    }

    /// `bucket` is `[s3://|gs://|az://]NAME[/PREFIX]`. With a PREFIX every
    /// key this client reads or writes lives under `PREFIX/`, so several
    /// fleets can share one bucket without colliding.
    ///
    /// A `gs://` bucket authenticates through Google Application Default
    /// Credentials (or the `GOOGLE_*` service-account environment) and
    /// takes no S3 endpoint, static credentials, or region — the bucket
    /// carries its own location.
    ///
    /// An `az://` bucket names an Azure Blob Storage container, takes its
    /// account from `AZURE_STORAGE_ACCOUNT_NAME`, and authenticates with a
    /// storage account key, a managed identity, or a workload identity. It
    /// takes no S3 endpoint, static credentials, or region either.
    ///
    /// `app` labels this client's traffic in the User-Agent (the aws
    /// AppName format, `app/<name>`), keeping e.g. the lease safety lane
    /// observable in black-box storage traces.
    pub fn open(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
        credentials: Option<StaticCredentials>,
        app: Option<&str>,
    ) -> anyhow::Result<Bucket> {
        anyhow::ensure!(
            !bucket.starts_with("dev://"),
            "dev:// storage is internal to `celld dev` and is not a fleet bucket"
        );
        Self::open_with_sources(
            bucket,
            endpoint,
            region,
            credentials,
            app,
            CloudSources::from_process(),
        )
    }

    /// Open the machine-local development store. Only the `celld dev`
    /// supervisor and its child node receive this constructor; fleet flags
    /// continue to resolve exclusively through the cloud backend parser.
    pub(crate) fn open_dev(database: &std::path::Path) -> anyhow::Result<Bucket> {
        let store = Arc::new(crate::local_store::LocalStore::open(database)?);
        Ok(Bucket {
            store: store.clone(),
            paginated: store.clone(),
            cas_store: store,
            backend: StorageBackend::Local,
            name: database.display().to_string(),
            prefix: String::new(),
        })
    }

    /// The body of [`Self::open`], taking the cloud configuration instead
    /// of deriving it from the `GOOGLE_*` and `AZURE_*` environments. A
    /// caller that passes explicit sources is independent of that
    /// environment.
    #[doc(hidden)]
    pub fn open_with_sources(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
        credentials: Option<StaticCredentials>,
        app: Option<&str>,
        sources: CloudSources,
    ) -> anyhow::Result<Bucket> {
        let CloudSources {
            gcs: gcs_builder,
            azure: azure_env,
        } = sources;
        let (backend, bucket, prefix) = split_spec(bucket);
        // The prefix is spliced into keys as plain text and stripped off
        // listed keys the same way. A character `object_store` would
        // percent-encode would make the two disagree, so refuse it here
        // rather than mis-parse every listing later.
        let illegal = |c: char| !c.is_ascii_alphanumeric() && !"-_./".contains(c);
        if prefix.contains(illegal) {
            anyhow::bail!("bucket prefix accepts only letters, digits and -_./: {prefix:?}");
        }
        // These bounds mirror the aws-sdk TimeoutConfig they replace
        // (connect 3 s / attempt 15 s / operation 30 s) — a correctness
        // condition for the node self-fence, not tuning. The read-timeout
        // knob collapses into the per-request bound.
        let mut options = ClientOptions::new()
            .with_timeout(Duration::from_secs(15))
            .with_connect_timeout(Duration::from_secs(3))
            .with_allow_http(true);
        if let Some(app) = app {
            options = options.with_user_agent(
                hyper::header::HeaderValue::from_str(&format!("celld app/{app}"))
                    .context("app user agent")?,
            );
        }
        let retry = RetryConfig {
            max_retries: 2,
            retry_timeout: Duration::from_secs(30),
            ..RetryConfig::default()
        };
        // A conditional write retries zero times, and that is a correctness
        // bound rather than a conservative default. A retry inside the
        // transport repeats a `PUT` that can already have committed, and it
        // repeats it with the same If-Match token, so the second attempt
        // answers 412 for a write the first one applied. The caller then
        // reads a clean lost race where it in fact still holds the record.
        // A transport retry also spends the renewal attempt's own deadline:
        // the actor bounds a renewal CAS to half of the remaining authority,
        // and a hidden second attempt inside that bound removes the readback
        // the core needs to resolve the first. Ownership of the retry belongs
        // to the core, which schedules it against the authority that remains.
        let cas_retry = RetryConfig {
            max_retries: 0,
            retry_timeout: Duration::from_secs(30),
            ..RetryConfig::default()
        };
        let (store, paginated, cas_store): (
            Arc<dyn ObjectStore>,
            Arc<dyn PaginatedListStore>,
            Arc<dyn ObjectStore>,
        ) = match backend {
            StorageBackend::S3 => {
                // An endpoint can also arrive through the environment:
                // `AmazonS3Builder::from_env` reads the AWS SDK's standard
                // AWS_ENDPOINT_URL / AWS_ENDPOINT into the client. The
                // path-style decision below must see that endpoint too —
                // an env-supplied endpoint with virtual-hosted-style
                // requests half-configures the client, and against an
                // S3-compatible store every operation then fails with a
                // NoSuchBucket that names the key prefix, pointing well
                // away from the cause. An explicit argument (--endpoint /
                // S3_ENDPOINT, resolved by the caller) still wins.
                let endpoint = endpoint.map(str::to_string).or_else(resolve_env_endpoint);
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .with_retry(retry)
                    .with_client_options(options);
                if let Some(endpoint) = endpoint.as_deref() {
                    // Path-style against explicit S3-compatible endpoints, exactly
                    // as the aws client's force_path_style(endpoint.is_some()).
                    builder = builder
                        .with_endpoint(endpoint)
                        .with_virtual_hosted_style_request(false);
                } else {
                    builder = builder.with_virtual_hosted_style_request(true);
                }
                if let Some(credentials) = credentials {
                    builder = builder
                        .with_access_key_id(credentials.access_key_id)
                        .with_secret_access_key(credentials.secret_access_key);
                    if let Some(token) = credentials.session_token {
                        builder = builder.with_token(token);
                    }
                }
                let cas_builder = builder.clone().with_retry(cas_retry);
                let client = Arc::new(builder.build().context("build s3 client")?);
                (
                    client.clone(),
                    client,
                    Arc::new(cas_builder.build().context("build s3 cas client")?),
                )
            }
            StorageBackend::Gcs => {
                // The generation dialect only. celld's S3 client fences
                // with If-Match, which GCS does not apply to a PUT; and
                // this GCS client authenticates with OAuth, not HMAC keys.
                // So an S3 endpoint or S3 static credentials with gs:// is
                // a configuration error, not something to quietly
                // reinterpret.
                if endpoint.is_some() {
                    anyhow::bail!(
                        "a gs:// bucket takes no S3 endpoint; unset --endpoint / S3_ENDPOINT"
                    );
                }
                if credentials.is_some() {
                    anyhow::bail!(
                        "a gs:// bucket cannot use S3 static credentials; it authenticates \
                         with Google Application Default Credentials"
                    );
                }
                let builder = gcs_builder
                    .with_bucket_name(bucket)
                    .with_retry(retry)
                    .with_client_options(options);
                let cas_builder = builder.clone().with_retry(cas_retry);
                let client = Arc::new(builder.build().context("build gcs client")?);
                (
                    client.clone(),
                    client,
                    Arc::new(cas_builder.build().context("build gcs cas client")?),
                )
            }
            StorageBackend::Azure => {
                // The etag dialect again — Put Blob applies If-None-Match
                // and If-Match — but a different client and different
                // credentials. So an S3 endpoint or S3 static credentials
                // with az:// is a configuration error, exactly as it is
                // with gs://, and not something to quietly reinterpret.
                if endpoint.is_some() {
                    anyhow::bail!(
                        "an az:// bucket takes no S3 endpoint; unset --endpoint / S3_ENDPOINT"
                    );
                }
                if credentials.is_some() {
                    anyhow::bail!(
                        "an az:// bucket cannot use S3 static credentials; it authenticates \
                         with an Azure storage account key, a managed identity, or a \
                         workload identity"
                    );
                }
                let builder = azure_builder_for(&azure_env, bucket)?
                    .with_retry(retry)
                    .with_client_options(options);
                let cas_builder = builder.clone().with_retry(cas_retry);
                let client = Arc::new(builder.build().context("build azure client")?);
                (
                    client.clone(),
                    client,
                    Arc::new(cas_builder.build().context("build azure cas client")?),
                )
            }
            StorageBackend::Local => {
                unreachable!("the local development store has its own constructor")
            }
        };
        Ok(Bucket {
            store,
            paginated,
            cas_store,
            backend,
            name: bucket.to_string(),
            prefix,
        })
    }

    /// The bucket's URL scheme, `s3`, `gs` or `az`, for operator-facing
    /// messages.
    pub fn scheme(&self) -> &'static str {
        self.backend.scheme()
    }

    /// The dialect this bucket speaks, for choosing the matching
    /// replication store.
    #[doc(hidden)]
    pub fn backend(&self) -> StorageBackend {
        self.backend
    }

    /// Scope a caller's key to this client's prefix.
    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    /// The inverse of [`Self::key`]: a listing answers with full keys, and
    /// every caller parses back the key it asked for, not the prefix.
    fn unkey<'a>(&self, key: &'a str) -> &'a str {
        key.strip_prefix(self.prefix.as_str()).unwrap_or(key)
    }

    /// Body and CAS token, or `None` when the key does not exist.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<(Bytes, String)>> {
        let key = self.key(key);
        match self.store.get(&Path::from(key.as_str())).await {
            Ok(result) => {
                let token = self
                    .backend
                    .token(result.meta.e_tag.clone(), result.meta.version.clone())
                    .with_context(|| format!("read {}://{}/{key}", self.scheme(), self.name))?;
                let bytes = result.bytes().await.with_context(|| {
                    format!("read body {}://{}/{key}", self.scheme(), self.name)
                })?;
                Ok(Some((bytes, token)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => {
                Err(anyhow!(error).context(format!("read {}://{}/{key}", self.scheme(), self.name)))
            }
        }
    }

    /// Size and CAS token, or `None` when the key does not exist.
    pub async fn head(&self, key: &str) -> anyhow::Result<Option<(u64, String)>> {
        let key = self.key(key);
        match self.store.head(&Path::from(key.as_str())).await {
            Ok(meta) => {
                let token = self
                    .backend
                    .token(meta.e_tag, meta.version)
                    .with_context(|| format!("head {}://{}/{key}", self.scheme(), self.name))?;
                Ok(Some((meta.size, token)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => {
                Err(anyhow!(error).context(format!("head {}://{}/{key}", self.scheme(), self.name)))
            }
        }
    }

    pub async fn put(&self, key: &str, body: impl Into<PutPayload>) -> anyhow::Result<()> {
        let key = self.key(key);
        self.store
            .put(&Path::from(key.as_str()), body.into())
            .await
            .with_context(|| format!("write {}://{}/{key}", self.scheme(), self.name))?;
        Ok(())
    }

    /// Size plus one user-metadata value (`x-amz-meta-*` / `x-goog-meta-*`),
    /// or `None` when the key does not exist. A plain `head` cannot see
    /// user metadata; this one can.
    pub async fn head_with_meta(
        &self,
        key: &str,
        name: &str,
    ) -> anyhow::Result<Option<(u64, Option<String>)>> {
        let key = self.key(key);
        let options = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        match self
            .store
            .get_opts(&Path::from(key.as_str()), options)
            .await
        {
            Ok(result) => {
                let value = result
                    .attributes
                    .get(&Attribute::Metadata(name.to_string().into()))
                    .map(|value| value.as_ref().to_string());
                Ok(Some((result.meta.size, value)))
            }
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => {
                Err(anyhow!(error).context(format!("head {}://{}/{key}", self.scheme(), self.name)))
            }
        }
    }

    /// Plain write carrying user metadata (`x-amz-meta-*` / `x-goog-meta-*`).
    pub async fn put_with_meta(
        &self,
        key: &str,
        body: impl Into<PutPayload>,
        meta: &[(&'static str, &str)],
    ) -> anyhow::Result<()> {
        let key = self.key(key);
        let mut attributes = Attributes::new();
        for (name, value) in meta {
            attributes.insert(
                Attribute::Metadata(Cow::Borrowed(name)),
                value.to_string().into(),
            );
        }
        let options = PutOptions {
            attributes,
            ..PutOptions::default()
        };
        self.store
            .put_opts(&Path::from(key.as_str()), body.into(), options)
            .await
            .with_context(|| format!("write {}://{}/{key}", self.scheme(), self.name))?;
        Ok(())
    }

    /// Conditional write. `token: None` requires the key to be absent;
    /// `Some` requires the current CAS token — the etag on S3 and on
    /// Azure Blob Storage (If-Match), the generation on GCS
    /// (x-goog-if-generation-match).
    /// `Ok(Some(new_token))` applied, `Ok(None)` cleanly rejected; any
    /// other failure is ambiguous and stays an error.
    pub async fn put_cas(
        &self,
        key: &str,
        body: impl Into<PutPayload>,
        token: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        let key = self.key(key);
        let mode = match token {
            None => PutMode::Create,
            Some(token) => PutMode::Update(self.backend.update(token)),
        };
        match self
            .cas_store
            .put_opts(
                &Path::from(key.as_str()),
                body.into(),
                PutOptions::from(mode),
            )
            .await
        {
            Ok(result) => {
                // The write applied; a result without a usable token still
                // surfaces as `Err`, which callers already treat as "may
                // have committed" and reconcile.
                let token = self
                    .backend
                    .token(result.e_tag, result.version)
                    .with_context(|| {
                        format!(
                            "conditional write {}://{}/{key} applied without a CAS token",
                            self.scheme(),
                            self.name
                        )
                    })?;
                Ok(Some(token))
            }
            Err(error) if is_clean_cas_rejection(&error) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!(
                "conditional write {}://{}/{key} may have committed",
                self.scheme(),
                self.name
            ))),
        }
    }

    /// Idempotent: deleting an absent key succeeds, as S3's DELETE does.
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let key = self.key(key);
        match self.store.delete(&Path::from(key.as_str())).await {
            Ok(()) | Err(Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(anyhow!(error).context(format!(
                "delete {}://{}/{key}",
                self.scheme(),
                self.name
            ))),
        }
    }

    /// Batched delete: the S3-family backends fold this into DeleteObjects
    /// requests (up to 1,000 keys per class A operation) — the lab priced
    /// bundle GC's one-key-at-a-time deletes at 9k operations an hour.
    /// Returns the keys that are now gone; an absent key counts as gone,
    /// and a key that fails stays listed for the next pass.
    pub async fn delete_many(&self, keys: &[String]) -> Vec<String> {
        let locations = futures_util::stream::iter(
            keys.iter()
                .map(|key| Ok(Path::from(self.key(key).as_str())))
                .collect::<Vec<_>>(),
        )
        .boxed();
        let mut gone = Vec::with_capacity(keys.len());
        let mut results = self.store.delete_stream(locations);
        while let Some(result) = results.next().await {
            match result {
                Ok(path) => gone.push(self.unkey(path.as_ref()).to_string()),
                Err(Error::NotFound { path, .. }) => {
                    gone.push(self.unkey(&path).to_string());
                }
                Err(error) => {
                    tracing::warn!(%error, "batched delete left a key for the next pass");
                }
            }
        }
        gone
    }

    /// Every object under `prefix/`; the client paginates internally.
    /// Listed keys come back the way the caller wrote them, because the
    /// caller parses them and knows nothing of the fleet's prefix.
    pub async fn list(&self, prefix: &str) -> anyhow::Result<Vec<ObjectMeta>> {
        let path = Path::from(self.key(prefix.trim_end_matches('/')));
        let mut stream = self.store.list(Some(&path));
        let mut objects = Vec::new();
        while let Some(meta) = stream.next().await {
            let mut meta =
                meta.with_context(|| format!("list {}://{}/{path}", self.scheme(), self.name))?;
            if !self.prefix.is_empty() {
                // The listing gave object_store a valid key, so re-parsing
                // the tail of it cannot fail.
                meta.location = Path::parse(self.unkey(meta.location.as_ref())).unwrap();
            }
            objects.push(meta);
        }
        Ok(objects)
    }

    /// Does anything exist under `prefix/`? One page at most.
    pub async fn list_any(&self, prefix: &str) -> anyhow::Result<bool> {
        let path = Path::from(self.key(prefix.trim_end_matches('/')));
        match self.store.list(Some(&path)).next().await {
            None => Ok(false),
            Some(Ok(_)) => Ok(true),
            Some(Err(error)) => Err(anyhow!(error).context(format!(
                "list {}://{}/{path}",
                self.scheme(),
                self.name
            ))),
        }
    }

    /// One provider page of objects under `prefix/`.
    ///
    /// The opaque token resumes the same listing with the same prefix and
    /// limit. A maintenance caller must finish and release this page before
    /// it asks for the next one, or pagination would bound requests without
    /// bounding retained object descriptions.
    pub(crate) async fn objects_page(
        &self,
        prefix: &str,
        page_token: Option<String>,
        max_keys: usize,
    ) -> anyhow::Result<ObjectPage> {
        // `PaginatedListStore` does not append the separator that
        // `ObjectStore::list` appends to a path-segment prefix.
        let path = self.key(&format!("{}/", prefix.trim_end_matches('/')));
        let mut result = self
            .paginated
            .list_paginated(
                Some(path.as_str()),
                PaginatedListOptions {
                    max_keys: Some(max_keys),
                    page_token,
                    ..PaginatedListOptions::default()
                },
            )
            .await
            .with_context(|| format!("list {}://{}/{path}", self.scheme(), self.name))?;
        if !self.prefix.is_empty() {
            for object in &mut result.result.objects {
                object.location = Path::parse(self.unkey(object.location.as_ref())).unwrap();
            }
        }
        Ok(ObjectPage {
            objects: result.result.objects,
            page_token: result.page_token,
        })
    }

    /// One page of the immediate child "directories" that start with `prefix`.
    ///
    /// `start_after` resumes from a child a previous page returned, and
    /// `page_token` continues the same walk. Pass one or the other: a
    /// token is exact and works on every backend, while `start_after`
    /// survives between separate processes, so an operator can resume a
    /// listing by name.
    ///
    /// The store compares `start_after` against object keys, not against
    /// children, and every key below `prefix/child/` sorts after that
    /// prefix itself. So the page that resumes a walk repeats the child it
    /// resumed from, and the caller drops it. The reverse error would skip
    /// a cell, so the boundary is deliberately inclusive.
    pub async fn common_prefixes_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page_token: Option<String>,
        max_keys: usize,
    ) -> anyhow::Result<CommonPrefixPage> {
        // Keep the caller's exact prefix. Most callers use a directory prefix
        // such as `cells/`, but a class-filtered cell walk uses `cells/Class:`
        // so the store excludes every other class before it applies the bound.
        let path = self.key(prefix);
        if start_after.is_some() && self.backend == StorageBackend::Azure {
            anyhow::bail!(
                "az:// cannot resume a listing by name, because Azure listing has no \
                 start-after; drop --after and list the whole container"
            );
        }
        let result = self
            .paginated
            .list_paginated(
                Some(path.as_str()),
                PaginatedListOptions {
                    offset: start_after.map(|child| self.key(&format!("{child}/"))),
                    delimiter: Some(std::borrow::Cow::Borrowed("/")),
                    max_keys: Some(max_keys),
                    page_token,
                    ..PaginatedListOptions::default()
                },
            )
            .await
            .with_context(|| format!("list {}://{}/{path}", self.scheme(), self.name))?;
        Ok(CommonPrefixPage {
            prefixes: result
                .result
                .common_prefixes
                .into_iter()
                .map(|child| self.unkey(child.as_ref()).to_string())
                .collect(),
            page_token: result.page_token,
        })
    }

    /// Immediate child "directories" under `prefix/` (delimiter listing),
    /// as full prefixes with the trailing slash stripped.
    ///
    /// Every page is drained into one buffer, so the cost is the whole
    /// listing. A caller that only needs a bounded answer must use
    /// [`Self::common_prefixes_page`] instead.
    pub async fn common_prefixes(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let path = Path::from(self.key(prefix.trim_end_matches('/')));
        let result = self
            .store
            .list_with_delimiter(Some(&path))
            .await
            .with_context(|| format!("list {}://{}/{path}", self.scheme(), self.name))?;
        Ok(result
            .common_prefixes
            .into_iter()
            .map(|p| self.unkey(p.as_ref()).to_string())
            .collect())
    }

    // ---- R2 binding primitives ------------------------------------------
    //
    // An `r2_buckets` binding is served out of the fleet bucket under the
    // reserved `r2/<bucket_name>/` prefix, so a Worker's blobs share the
    // durability and the credentials of everything else celld stores and
    // need no second bucket. These are the only operations that reach past
    // the in-memory `Bytes` contract the rest of this module keeps: a blob
    // is a Worker's payload and can be far larger than a fleet object, so
    // it is read as a stream and a large write goes through multipart.

    /// Everything about an object except its bytes, or `None` when the key
    /// does not exist. This is R2's `head`, and the read half of every
    /// conditional write.
    pub async fn head_blob(&self, key: &str) -> anyhow::Result<Option<BlobMeta>> {
        let key = self.key(key);
        let options = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        match self
            .store
            .get_opts(&Path::from(key.as_str()), options)
            .await
        {
            Ok(result) => Ok(Some(self.blob_meta(&result.meta, &result.attributes))),
            Err(Error::NotFound { .. }) => Ok(None),
            Err(error) => {
                Err(anyhow!(error).context(format!("head {}://{}/{key}", self.scheme(), self.name)))
            }
        }
    }

    /// One blob a `get` answered, with its body still on the wire.
    pub async fn get_blob(
        &self,
        key: &str,
        range: BlobRange,
        conditions: &BlobConditions,
    ) -> anyhow::Result<BlobRead> {
        let scoped = self.key(key);
        let options = GetOptions {
            range: match range {
                BlobRange::Whole => None,
                BlobRange::From(offset) => Some(GetRange::Offset(offset)),
                BlobRange::Bounded { offset, length } => {
                    Some(GetRange::Bounded(offset..offset.saturating_add(length)))
                }
                BlobRange::Suffix(length) => Some(GetRange::Suffix(length)),
            },
            if_match: conditions.if_match.clone(),
            if_none_match: conditions.if_none_match.clone(),
            ..GetOptions::default()
        };
        let result = match self
            .store
            .get_opts(&Path::from(scoped.as_str()), options)
            .await
        {
            Ok(result) => result,
            // Only an absent key is a miss. A range that overshoots the end
            // of the object is served short, as R2 serves it; a range that
            // *starts* past the end is a 416, and it stays an error rather
            // than being turned into a miss, because that is what R2 does
            // with one too.
            Err(Error::NotFound { .. }) => return Ok(BlobRead::Missing),
            // The store says only that the condition failed. R2 answers a
            // refused `onlyIf` with the object itself, minus the body, so
            // one more head fills in what the caller is entitled to see.
            Err(Error::Precondition { .. } | Error::NotModified { .. }) => {
                return Ok(match self.head_blob(key).await? {
                    Some(meta) => BlobRead::Unmet(meta),
                    None => BlobRead::Missing,
                })
            }
            Err(error) => {
                return Err(anyhow!(error).context(format!(
                    "read {}://{}/{scoped}",
                    self.scheme(),
                    self.name
                )))
            }
        };
        let meta = self.blob_meta(&result.meta, &result.attributes);
        // The upload-time half of `onlyIf` is answered here, against the
        // millisecond the object carries. The body is dropped unread.
        if !conditions.time_met(meta.uploaded_ms) {
            return Ok(BlobRead::Unmet(meta));
        }
        // `range` is the slice this response actually carries, which is
        // what the caller reads; a bounded request past the end is clamped
        // here rather than by the store.
        let served = result.range.clone();
        let label = format!("read body {}://{}/{scoped}", self.scheme(), self.name);
        let body = result
            .into_stream()
            .map(move |chunk| match chunk {
                Ok(bytes) => Ok(bytes.to_vec()),
                Err(error) => Err(format!("{label}: {error}")),
            })
            .boxed();
        Ok(BlobRead::Hit(Blob {
            meta,
            range: (served.start, served.end.saturating_sub(served.start)),
            body,
        }))
    }

    /// A plain (non-multipart) write, carrying R2's `httpMetadata` and
    /// `customMetadata`.
    ///
    /// `conditions` is R2's `onlyIf`, and is answered by reading the
    /// object first and pairing the verdict with that version's CAS token:
    /// the check and the write then apply to the same version, and a racing
    /// write between them is refused rather than overwritten. `Ok(None)` is
    /// a refused precondition — R2 reports one as a `null` return, not a
    /// throw — and every other failure stays an error.
    pub async fn put_blob(
        &self,
        key: &str,
        body: PutPayload,
        attributes: &BlobAttributes,
        conditions: &BlobConditions,
    ) -> anyhow::Result<Option<BlobMeta>> {
        let size = body.content_length() as u64;
        let mode = match conditions.is_empty() {
            true => PutMode::Overwrite,
            false => match self.head_blob(key).await? {
                Some(current) => {
                    if !conditions.etag_met(current.etag.as_deref())
                        || !conditions.time_met(current.uploaded_ms)
                    {
                        return Ok(None);
                    }
                    let Some(cas) = current.cas else {
                        return Err(anyhow!(
                            "conditional R2 write of {}://{}/{key} cannot be fenced: \
                             the store answered no version token",
                            self.scheme(),
                            self.name
                        ));
                    };
                    PutMode::Update(self.backend.update(&cas))
                }
                // Nothing there: only a condition that an absent object can
                // satisfy may write, and it writes as a create so that a
                // racing create loses.
                None => match conditions.etag_met(None) {
                    true => PutMode::Create,
                    false => return Ok(None),
                },
            },
        };
        let conditional = !matches!(mode, PutMode::Overwrite);
        let store = match conditional {
            true => &self.cas_store,
            false => &self.store,
        };
        let scoped = self.key(key);
        let options = PutOptions {
            mode,
            attributes: attributes.write(),
            ..PutOptions::default()
        };
        match store
            .put_opts(&Path::from(scoped.as_str()), body, options)
            .await
        {
            Ok(result) => Ok(Some(BlobMeta {
                size,
                cas: self
                    .backend
                    .token(result.e_tag.clone(), result.version.clone())
                    .ok(),
                etag: result.e_tag,
                version: result.version,
                uploaded_ms: crate::asyncrt::wall_ms(),
                attributes: attributes.clone(),
            })),
            // A racing write took the version this one checked. The
            // caller's precondition is no longer true, which is the answer
            // R2 would have given had the race gone the other way.
            Err(error) if conditional && is_clean_cas_rejection(&error) => Ok(None),
            Err(error) => Err(anyhow!(error).context(format!(
                "write {}://{}/{scoped}",
                self.scheme(),
                self.name
            ))),
        }
    }

    /// One listing page: every object whose full key starts with `prefix`,
    /// in key order, resuming strictly after `after`. `limit` bounds the
    /// page, counting a rolled-up prefix as R2 counts one — against the
    /// same limit as an object.
    ///
    /// `prefix` is a raw string prefix, as R2's is, not the directory
    /// prefix the rest of this module lists by: the store is asked for the
    /// enclosing directory and the tail is matched here.
    ///
    /// `delimiter` is R2's: the run of a key between the end of `prefix`
    /// and the delimiter's first occurrence after it stands in for every
    /// key sharing it, and those keys are not listed individually.
    pub async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
        delimiter: Option<&str>,
    ) -> anyhow::Result<BlobPage> {
        let directory = prefix.rsplit_once('/').map_or("", |(head, _)| head);
        let path = Path::from(self.key(directory).as_str());
        let mut stream = match after {
            Some(after) => self
                .store
                .list_with_offset(Some(&path), &Path::from(self.key(after).as_str())),
            None => self.store.list(Some(&path)),
        };
        let delimiter = delimiter.filter(|delimiter| !delimiter.is_empty());
        let mut page = BlobPage {
            objects: Vec::with_capacity(limit.min(1024)),
            prefixes: Vec::new(),
            truncated: false,
            cursor: None,
        };
        // The rolled-up prefixes seen so far. A listing is in key order, so
        // a run of keys under one prefix is contiguous and only the newest
        // prefix can repeat — but a delimiter is any string, not only `/`,
        // so `prefix` can be re-entered later and the set is kept whole.
        // Ordered, because nothing in the engine may hold state a replay
        // would walk in a different order.
        let mut rolled = std::collections::BTreeSet::new();
        while let Some(meta) = stream.next().await {
            let meta =
                meta.with_context(|| format!("list {}://{}/{path}", self.scheme(), self.name))?;
            let key = self.unkey(meta.location.as_ref());
            if !key.starts_with(prefix) {
                continue;
            }
            let group = delimiter.and_then(|delimiter| {
                key[prefix.len()..]
                    .find(delimiter)
                    .map(|at| key[..prefix.len() + at + delimiter.len()].to_string())
            });
            // A prefix already rolled up costs nothing and does not end the
            // page: it is the same entry the caller has already been given.
            if let Some(group) = &group {
                if rolled.contains(group) {
                    page.cursor = Some(key.to_string());
                    continue;
                }
            }
            if page.objects.len() + page.prefixes.len() == limit {
                page.truncated = true;
                break;
            }
            match group {
                Some(group) => {
                    rolled.insert(group.clone());
                    page.prefixes.push(group);
                }
                None => page.objects.push(BlobEntry {
                    key: key.to_string(),
                    size: meta.size,
                    etag: meta.e_tag,
                    version: meta.version,
                    uploaded_ms: meta.last_modified.timestamp_millis(),
                }),
            }
            page.cursor = Some(key.to_string());
        }
        if !page.truncated {
            page.cursor = None;
        }
        Ok(page)
    }

    /// Open a multipart upload carrying R2's `httpMetadata` and
    /// `customMetadata`. The parts and the completion ride on the returned
    /// handle.
    pub async fn begin_multipart(
        &self,
        key: &str,
        attributes: &BlobAttributes,
    ) -> anyhow::Result<Box<dyn MultipartUpload>> {
        let key = self.key(key);
        let options = PutMultipartOptions {
            attributes: attributes.write(),
            ..PutMultipartOptions::default()
        };
        self.store
            .put_multipart_opts(&Path::from(key.as_str()), options)
            .await
            .with_context(|| format!("begin multipart {}://{}/{key}", self.scheme(), self.name))
    }

    /// One object's metadata, in the shape every R2 answer carries.
    fn blob_meta(&self, meta: &ObjectMeta, attributes: &Attributes) -> BlobMeta {
        BlobMeta {
            size: meta.size,
            etag: meta.e_tag.clone(),
            version: meta.version.clone(),
            cas: self
                .backend
                .token(meta.e_tag.clone(), meta.version.clone())
                .ok(),
            uploaded_ms: meta.last_modified.timestamp_millis(),
            attributes: BlobAttributes::read(attributes),
        }
    }

    /// The head_bucket replacement: prove the bucket is reachable and the
    /// credential is accepted with one list page. Scoped to the prefix, so
    /// a credential scoped to it validates too.
    pub async fn validate(&self) -> anyhow::Result<()> {
        let scope = (!self.prefix.is_empty()).then(|| Path::from(self.prefix.as_str()));
        match self.store.list(scope.as_ref()).next().await {
            None | Some(Ok(_)) => Ok(()),
            Some(Err(error)) => {
                Err(anyhow!(error).context(format!("validate {}://{}", self.scheme(), self.name)))
            }
        }
    }

    /// Run the conditional-write contract against the live bucket.
    ///
    /// A store can accept a precondition header and then ignore it, and no
    /// capability API answers whether it does. So the probe provokes the
    /// two rejections a conforming store must produce, and checks it
    /// produced them. celld decides which node owns a cell with a
    /// conditional write, so a store that applies a write it must reject
    /// lets two nodes own one cell (denoland/celld#137).
    ///
    /// The two outcomes are separated because they need different
    /// responses. A `Violation` is a property of the store and never
    /// clears, so it can stop a node. An `Err` is ambiguous — a network
    /// fault, a rejected credential — and a retry can clear it, so a
    /// caller that must not fail on a transient blip keeps serving.
    pub(crate) async fn probe_cas_steps(&self) -> anyhow::Result<StorageProbeVerdict> {
        let key = Self::probe_key("cas");
        let verdict = self.cas_contract(&key).await;
        // The object is debris on every path, so retire it before the
        // verdict. A delete that fails leaves one tiny object under
        // `probe/`, which nothing lists and nothing reads — but a
        // credential that cannot delete accrues one per boot, so say so.
        if let Err(error) = self.delete(&key).await {
            tracing::warn!(%error, "the conditional-write probe could not delete its object");
        }
        verdict
    }

    /// Run every storage check that a node needs before it serves.
    ///
    /// A clear unsupported-operation response is permanent, so it becomes a
    /// contract violation. Other errors remain ambiguous so the startup
    /// caller can retry the complete probe with fresh keys.
    pub(crate) async fn probe_startup_storage_steps(&self) -> anyhow::Result<StorageProbeVerdict> {
        match self.probe_cas_steps().await {
            Ok(StorageProbeVerdict::Conformant) => {}
            Ok(violation) => return Ok(violation),
            Err(error) if is_unsupported_operation(&error) => {
                return Ok(StorageProbeVerdict::Violation(
                    "the store does not support conditional writes, so celld cannot acquire a cell"
                        .to_string(),
                ));
            }
            Err(error) => return Err(error),
        }

        match self.probe_range_steps().await {
            Err(error) if is_unsupported_operation(&error) || is_invalid_range_response(&error) => {
                Ok(StorageProbeVerdict::Violation(
                    "the store does not honor ranged reads, so celld cannot read stored cell data"
                        .to_string(),
                ))
            }
            result => result,
        }
    }

    fn probe_key(kind: &str) -> String {
        let nanos = crate::asyncrt::wall_ms().max(0) as u128 * 1_000_000;
        // Unique per probe, so several nodes probing at once touch
        // disjoint keys and no probe reads another one's object as the
        // store misbehaving — a collision surfaces as a false `Violation`,
        // which stops a node. The random half carries that alone, because
        // a container fleet shares pid 1 and a clock before the epoch
        // leaves `nanos` at zero.
        format!(
            "probe/{kind}-{nanos}-{}-{:016x}",
            crate::asyncrt::process_tag(),
            rand::RngCore::next_u64(&mut crate::asyncrt::rng("cas_probe"))
        )
    }

    /// [`Self::probe_cas_steps`] collapsed to one answer, where any wrong
    /// answer fails the check.
    pub async fn probe_cas(&self) -> anyhow::Result<()> {
        match self.probe_cas_steps().await? {
            StorageProbeVerdict::Conformant => Ok(()),
            StorageProbeVerdict::Violation(reason) => Err(anyhow!(reason)),
        }
    }

    /// The four steps, against one key. Steps 2 and 4 must be rejected;
    /// a store that applies either one cannot fence.
    async fn cas_contract(&self, key: &str) -> anyhow::Result<StorageProbeVerdict> {
        let precondition = self.backend.precondition();
        let ambiguous = || {
            format!(
                "the store answered a conditional write with an error where celld requires a \
                 clean rejection, so celld cannot tell a lost race from a failed write and \
                 reconciles forever; the store must answer {precondition} with a rejection"
            )
        };

        // 1. A create on an absent key applies, and answers the token that
        //    steps 3 and 4 need.
        let Some(token) = self
            .put_cas(key, b"probe-create".to_vec(), None)
            .await
            .context("the conditional-write probe could not create its object")?
        else {
            return Ok(StorageProbeVerdict::Violation(
                "the store rejected a conditional create of an object that does not exist"
                    .to_string(),
            ));
        };

        // 2. A create over the object step 1 wrote must be rejected.
        if self
            .put_cas(key, b"probe-recreate".to_vec(), None)
            .await
            .with_context(ambiguous)?
            .is_some()
        {
            return Ok(StorageProbeVerdict::Violation(format!(
                "the store overwrote an object although the write was conditional on that object \
                 being absent; the store accepts {precondition} and does not enforce it, so two \
                 nodes can own one cell"
            )));
        }

        // 3. An update that carries the current token applies, and that
        //    retires the token step 4 reuses.
        if self
            .put_cas(key, b"probe-update".to_vec(), Some(&token))
            .await
            .context("the conditional-write probe could not update its object")?
            .is_none()
        {
            return Ok(StorageProbeVerdict::Violation(
                "the store rejected a conditional update that carried the current token"
                    .to_string(),
            ));
        }

        // 4. The token is stale now, so the update must be rejected. This
        //    step is the fencing contract itself.
        if self
            .put_cas(key, b"probe-stale".to_vec(), Some(&token))
            .await
            .with_context(ambiguous)?
            .is_some()
        {
            return Ok(StorageProbeVerdict::Violation(format!(
                "the store applied a conditional write that carried a stale token; the store \
                 accepts {precondition} and does not enforce it, so two nodes can own one cell"
            )));
        }

        Ok(StorageProbeVerdict::Conformant)
    }

    async fn probe_range_steps(&self) -> anyhow::Result<StorageProbeVerdict> {
        let key = Self::probe_key("range");
        let verdict = self.range_contract(&key).await;
        if let Err(error) = self.delete(&key).await {
            tracing::warn!(%error, "the ranged-read probe could not delete its object");
        }
        verdict
    }

    async fn range_contract(&self, key: &str) -> anyhow::Result<StorageProbeVerdict> {
        const BODY: &[u8] = b"celld-range-probe";
        const START: u64 = 6;
        const END: u64 = 11;
        let requested = START..END;

        self.put(key, BODY.to_vec())
            .await
            .context("the ranged-read probe could not create its object")?;
        let scoped = self.key(key);
        let result = self
            .store
            .get_opts(
                &Path::from(scoped.as_str()),
                GetOptions {
                    range: Some(GetRange::Bounded(requested.clone())),
                    ..GetOptions::default()
                },
            )
            .await
            .with_context(|| {
                format!(
                    "the ranged-read probe could not read {}://{}/{scoped}",
                    self.scheme(),
                    self.name
                )
            })?;
        let served = result.range.clone();
        let bytes = result
            .bytes()
            .await
            .context("the ranged-read probe could not read its response body")?;
        if served != requested || bytes.as_ref() != &BODY[START as usize..END as usize] {
            return Ok(StorageProbeVerdict::Violation(
                "the store does not honor ranged reads because it returned a different range or different bytes"
                    .to_string(),
            ));
        }
        Ok(StorageProbeVerdict::Conformant)
    }
}

/// What one storage-contract check found.
pub(crate) enum StorageProbeVerdict {
    /// The store returned every result that this check requires.
    Conformant,
    /// The store answered wrongly, and the string says how. This never
    /// clears on a retry, so a caller can act on it.
    Violation(String),
}

/// Whether an operation failed because the store clearly does not implement
/// it. The public object-store errors preserve that result structurally, but
/// its HTTP adapters keep a 405 or 501 status only in a private source error.
fn is_unsupported_operation(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if matches!(
            cause.downcast_ref::<Error>(),
            Some(Error::NotSupported { .. } | Error::NotImplemented)
        ) {
            return true;
        }
        let text = cause.to_string();
        text.contains("status 405")
            || text.contains("status 501")
            || text.contains("405 Method Not Allowed")
            || text.contains("501 Not Implemented")
    })
}

/// Whether the object-store client rejected a successful HTTP response because
/// it did not describe the requested byte range. The locked object-store
/// library keeps these parser types private, so their messages are the only
/// available boundary. A dependency update must verify these messages.
fn is_invalid_range_response(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let text = cause.to_string();
        text == "Received non-partial response when range requested"
            || text == "Content-Range header not present in partial response"
            || text.starts_with("Failed to parse value for CONTENT_RANGE header:")
            || text == "Content-Range header contained non UTF-8 characters"
            || (text.starts_with("Requested ") && text.contains(", got "))
    })
}

/// The replica-lane store for a `gs://` fleet bucket: its own transport
/// and connection pool, authenticated like [`Bucket::open`]'s gs:// path
/// (OAuth via Application Default Credentials or the `GOOGLE_*` env),
/// with the same bounded retry policy the S3 replica lane uses. Replica
/// writes are plain puts, so retries stay on.
#[doc(hidden)]
pub fn gcs_replica_store(bucket: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
    gcs_replica_store_with_builder(GoogleCloudStorageBuilder::from_env(), bucket)
}

/// The body of [`gcs_replica_store`], taking the base builder for the same
/// reason [`Bucket::open_with_sources`] does.
#[doc(hidden)]
pub fn gcs_replica_store_with_builder(
    builder: GoogleCloudStorageBuilder,
    bucket: &str,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(
        builder
            .with_bucket_name(bucket)
            .with_retry(celld_ltx::client::object_store::replica_retry_config())
            .build()
            .context("build gcs replica store")?,
    ))
}

/// The replica-lane store for an `az://` fleet bucket: its own transport
/// and connection pool, authenticated like [`Bucket::open`]'s az:// path,
/// with the same bounded retry policy the S3 replica lane uses. Replica
/// writes are plain puts, so retries stay on.
#[doc(hidden)]
pub fn azure_replica_store(container: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
    azure_replica_store_with_env(&AzureEnv::from_process(), container)
}

/// The body of [`azure_replica_store`], taking the environment for the
/// same reason [`Bucket::open_with_sources`] does.
#[doc(hidden)]
pub fn azure_replica_store_with_env(
    env: &AzureEnv,
    container: &str,
) -> anyhow::Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(
        azure_builder_for(env, container)?
            .with_retry(celld_ltx::client::object_store::replica_retry_config())
            .build()
            .context("build azure replica store")?,
    ))
}

/// The `AZURE_*` variables an `az://` bucket can inspect. celld captures
/// them instead of letting `MicrosoftAzureBuilder::from_env` read the
/// process environment, because the fleet client must decide which
/// recognized settings it honors, and a caller can supply an explicit set.
/// Names that `AzureConfigKey` does not parse are inert
/// and stay ignored, as they are in `from_env`.
#[derive(Clone, Default)]
#[doc(hidden)]
pub struct AzureEnv {
    variables: Vec<(String, String)>,
}

impl AzureEnv {
    /// Every `AZURE_*` variable in the process environment.
    fn from_process() -> AzureEnv {
        AzureEnv::from_pairs(std::env::vars().filter(|(name, _)| name.starts_with("AZURE_")))
    }

    #[doc(hidden)]
    pub fn from_pairs<K, V>(pairs: impl IntoIterator<Item = (K, V)>) -> AzureEnv
    where
        K: Into<String>,
        V: Into<String>,
    {
        AzureEnv {
            variables: pairs
                .into_iter()
                .map(|(name, value)| (name.into(), value.into()))
                .filter(|(_, value)| !value.is_empty())
                .collect(),
        }
    }
}

/// The cloud configuration [`Bucket::open`] derives from the process
/// environment: the GCS builder, and the `AZURE_*` variables. Bundled so
/// the seam that makes construction environment-independent stays one
/// parameter as backends are added.
#[doc(hidden)]
pub struct CloudSources {
    pub gcs: GoogleCloudStorageBuilder,
    pub azure: AzureEnv,
}

impl CloudSources {
    fn from_process() -> CloudSources {
        CloudSources {
            gcs: GoogleCloudStorageBuilder::from_env(),
            azure: AzureEnv::from_process(),
        }
    }
}

/// Is this a setting an `az://` bucket accepts? celld supports three
/// credential families — a storage account key, a managed identity, and
/// a workload identity — on the public Azure cloud, and no other
/// recognized Azure setting.
///
/// This is an allowlist, and the wildcard arm is the point of it.
/// `AzureConfigKey` is `#[non_exhaustive]`, so a key that a later
/// `object_store` adds falls to `false` and refuses, instead of reaching
/// the client unexamined. A denylist gave the opposite default and let
/// three settings through: `AZURE_USE_FABRIC_ENDPOINT` retargets the
/// client at OneLake, the four `AZURE_FABRIC_*` variables form a fourth
/// credential that wins ahead of the account key, and
/// `AZURE_SKIP_SIGNATURE` makes every request anonymous.
fn accepts_azure_config_key(key: &AzureConfigKey) -> bool {
    matches!(
        key,
        // The account, and the account-key credential.
        AzureConfigKey::AccountName
            | AzureConfigKey::AccessKey
            // A managed identity: the defaults reach IMDS, and any one
            // of these three selects a user-assigned identity.
            | AzureConfigKey::ClientId
            | AzureConfigKey::ObjectId
            | AzureConfigKey::MsiResourceId
            // A workload identity, with ClientId above.
            | AzureConfigKey::AuthorityId
            | AzureConfigKey::AuthorityHost
            | AzureConfigKey::FederatedTokenFile
            // Azurite, handled separately below.
            | AzureConfigKey::UseEmulator
    )
}

/// Build the Azure client configuration for `container` from the accepted
/// part of `env`, and refuse every other recognized Azure setting.
///
/// `object_store`'s builder accepts every source the Azure chain offers.
/// celld accepts three, which mirrors the deliberate narrowness of the S3
/// path, where celld reads the `AWS_*` environment but no `~/.aws`
/// profile and no SSO login. Both the fleet client and the replica store
/// come through here, so they cannot narrow differently.
///
/// A refused variable fails at startup with a message that names it. A
/// silently ignored credential surfaces much later as a permission
/// error, and it points at the container instead of the configuration.
fn azure_builder_for(env: &AzureEnv, container: &str) -> anyhow::Result<MicrosoftAzureBuilder> {
    let mut parsed: Vec<(AzureConfigKey, &str, &str)> = Vec::new();
    let mut seen: Vec<(AzureConfigKey, &str)> = Vec::new();
    for (name, value) in &env.variables {
        // `from_env` parses each AZURE_* name into a config key and drops
        // the ones that do not parse, so a name this parse rejects is a
        // name object_store would have ignored anyway.
        let Ok(key) = name.to_ascii_lowercase().parse::<AzureConfigKey>() else {
            continue;
        };
        if !accepts_azure_config_key(&key) {
            anyhow::bail!(
                "an az:// bucket does not accept {name}; celld authenticates with an Azure \
                 storage account key, a managed identity, or a workload identity on the \
                public Azure cloud, and it refuses every other Azure setting"
            );
        }
        let value = if key == AzureConfigKey::AuthorityHost {
            let public = authority_hosts::AZURE_PUBLIC_CLOUD;
            if value != public && value != &format!("{public}/") {
                anyhow::bail!(
                    "an az:// bucket accepts {name} only for the public Azure authority \
                     {public}; sovereign and custom authority hosts are not supported"
                );
            }
            // The webhook value has a trailing slash, but object_store
            // inserts its own separator. Pass one canonical spelling.
            public
        } else {
            value.as_str()
        };
        if let Some((_, first)) = seen.iter().find(|(candidate, _)| *candidate == key) {
            anyhow::bail!(
                "an az:// bucket does not accept both {first} and {name}; they are aliases for \
                 the same Azure setting"
            );
        }
        seen.push((key, name));
        parsed.push((key, name.as_str(), value));
    }

    let has = |wanted| seen.iter().any(|(key, _)| *key == wanted);
    let account_key = has(AzureConfigKey::AccessKey);
    let client_id = has(AzureConfigKey::ClientId);
    let workload_specific = has(AzureConfigKey::AuthorityId)
        || has(AzureConfigKey::AuthorityHost)
        || has(AzureConfigKey::FederatedTokenFile);
    let managed_specific = has(AzureConfigKey::ObjectId) || has(AzureConfigKey::MsiResourceId);
    let managed_selectors: Vec<&str> = seen
        .iter()
        .filter(|(key, _)| {
            matches!(
                key,
                AzureConfigKey::ClientId | AzureConfigKey::ObjectId | AzureConfigKey::MsiResourceId
            )
        })
        .map(|(_, name)| *name)
        .collect();

    if (account_key && (client_id || workload_specific || managed_specific))
        || (workload_specific && managed_specific)
    {
        anyhow::bail!(
            "an az:// bucket does not accept mixed Azure credential families; select exactly \
             one storage account key, workload identity, or managed identity"
        );
    }
    // Two selectors inside the managed-identity family are not an alias
    // pair, so the duplicate check above does not see them. object_store
    // resolves them by precedence instead — client_id, then object_id,
    // then msi_res_id (`azure/credential.rs`) — so the node authenticates
    // as an identity the operator did not choose, and the mistake
    // surfaces as a permission error against the container. That is the
    // late, misdirected failure this whole seam exists to prevent.
    if !workload_specific && managed_selectors.len() > 1 {
        anyhow::bail!(
            "an az:// bucket accepts one managed-identity selector, but {} name different \
             identities; set exactly one of AZURE_CLIENT_ID, AZURE_OBJECT_ID, or \
             AZURE_MSI_RESOURCE_ID",
            managed_selectors.join(" and ")
        );
    }
    if workload_specific
        && !(client_id
            && has(AzureConfigKey::AuthorityId)
            && has(AzureConfigKey::FederatedTokenFile))
    {
        anyhow::bail!(
            "an Azure workload identity requires AZURE_CLIENT_ID, AZURE_TENANT_ID, and \
             AZURE_FEDERATED_TOKEN_FILE"
        );
    }

    let mut builder = MicrosoftAzureBuilder::new();
    let mut account = false;
    let mut emulator = None;
    for (key, name, value) in parsed {
        match key {
            // Never handed on as a string. object_store would parse it
            // itself, and its parse accepts y/n as well as true/false, so
            // a second parse here could disagree with it — and a
            // disagreement over this key means celld validates a
            // production configuration while the client talks to a local
            // Azurite. Two such nodes each own every cell. The name
            // travels with the value so the refusal below names what the
            // operator set. Today one AZURE_ spelling parses to this key,
            // so the two can not differ; carrying the name keeps that
            // true if object_store adds an alias.
            AzureConfigKey::UseEmulator => emulator = Some((name, value)),
            AzureConfigKey::AccountName => {
                account = true;
                builder = builder.with_config(key, value);
            }
            _ => builder = builder.with_config(key, value),
        }
    }
    // Azurite is the one endpoint override celld allows, and it arrives
    // as a parsed bool, so object_store re-parses nothing. Its
    // conditional-write behavior is not qualified for a production fleet.
    if let Some((name, value)) = emulator {
        let on = match value.to_ascii_lowercase().as_str() {
            "true" | "1" => true,
            "false" | "0" => false,
            other => anyhow::bail!("{name} accepts true or false, not {other:?}"),
        };
        builder = builder.with_use_emulator(on);
        if on {
            return Ok(builder.with_container_name(container));
        }
    }
    if !account {
        anyhow::bail!(
            "an az:// bucket names a container, so the storage account must come from \
             AZURE_STORAGE_ACCOUNT_NAME"
        );
    }
    Ok(builder.with_container_name(container))
}

/// Was this a 401/403 — the credential itself rejected? Used by the managed
/// path to report a revoked credential rather than a flaky bucket.
pub fn is_unauthorized(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if matches!(
            cause.downcast_ref::<Error>(),
            Some(Error::PermissionDenied { .. } | Error::Unauthenticated { .. })
        ) {
            return true;
        }
        // The S3 list path wraps its private retry error as Generic, so the
        // status survives only in Display. object_store 0.11 used `status
        // 403`, and 0.12 uses the canonical HTTP phrase below. A revoked
        // managed credential must keep the same operator-visible state.
        let text = cause.to_string();
        text.contains("status 403")
            || text.contains("status 401")
            || text.contains("status code: 403 Forbidden")
            || text.contains("status code: 401 Unauthorized")
    })
}
