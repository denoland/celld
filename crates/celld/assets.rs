// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Asset download and cache maintenance execute outside the Actor execution
// domain.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use crate::bucket::Bucket;
use crate::js;
use crate::protocol::{
    asset_blob_key, AssetEntry, AssetIndex, AssetManifestRef, DeployPointer, Manifest,
    RunWorkerFirst,
};
use anyhow::{anyhow, Context};
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use futures_util::StreamExt;
use percent_encoding::percent_decode_str;
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;

const DEFAULT_ASSET_CACHE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone)]
pub struct AssetResolver {
    inner: Arc<AssetResolverInner>,
}

/// Where a resolver can look for a NEWER deployment's asset index than the
/// one this process booted with. During a rolling restart an upgraded node
/// serves HTML whose content-hashed asset paths an un-restarted node has
/// never heard of; the blobs are content-addressed and fleet-visible, so
/// only the index is stale. denoland/celld#161.
pub struct FallbackSource {
    /// The pointer this deployment was loaded from (`deploy/current.json`,
    /// or the script-scoped pointer for a service).
    pub pointer_key: String,
    /// The deployment prefix loaded at boot; a pointer naming another
    /// prefix names a newer deployment.
    pub boot_prefix: String,
}

/// The newest foreign index this resolver has adopted, plus the throttle
/// that keeps a miss storm from becoming a pointer-GET storm. Guarded by a
/// std mutex that is NEVER held across I/O: the refresh runs in a spawned
/// task, so a degraded bucket cannot stall the miss path — before this
/// rule, one slow pointer GET head-of-line-blocked every Worker-bound
/// request whose path missed the index.
/// What one refresh pass concluded about the pointer.
enum Adoption {
    /// The pointer names the booted deployment: any adoption is withdrawn.
    Booted,
    /// The pointer names the deployment already adopted.
    Unchanged,
    /// The pointer names a deployment this resolver now adopts. Boxed:
    /// an index can be large, and the other variants carry nothing.
    Newer {
        prefix: String,
        index: Box<AssetIndex>,
    },
}

#[derive(Default)]
struct FallbackState {
    checked: Option<std::time::Instant>,
    refreshing: bool,
    prefix: Option<String>,
    index: Option<Arc<AssetIndex>>,
}

/// How long a pointer observation stays fresh. A newer deployment's assets
/// become servable within this bound after its pointer lands, which is far
/// inside the window a rolling restart leaves a node un-upgraded.
const FALLBACK_CHECK_MS: u64 = 5_000;

struct AssetResolverInner {
    bucket: Bucket,
    index: AssetIndex,
    asset_only: bool,
    fallback: Option<FallbackSource>,
    fallback_state: std::sync::Mutex<FallbackState>,
    cache_root: PathBuf,
    cache_max_bytes: u64,
    download_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    cleanup_lock: Mutex<()>,
    header_rules: Vec<HeaderRule>,
    redirect_rules: Vec<RedirectRule>,
}

enum Route {
    Asset {
        path: String,
        entry: AssetEntry,
        status: StatusCode,
    },
    Redirect {
        location: String,
        status: StatusCode,
    },
    Missing,
}

#[derive(Clone, Copy)]
enum RequestedRange {
    Full,
    Partial { start: u64, end: u64 },
    Unsatisfiable,
}

struct RulePattern {
    host: Option<regex::Regex>,
    path: regex::Regex,
}

struct HeaderRule {
    pattern: RulePattern,
    operations: Vec<HeaderOperation>,
}

enum HeaderOperation {
    Set(String, String),
    Remove(String),
}

struct RedirectRule {
    pattern: RulePattern,
    destination: String,
    status: u16,
}

impl AssetResolver {
    pub async fn load(
        bucket: &Bucket,
        prefix: &str,
        reference: &AssetManifestRef,
        asset_only: bool,
        fallback: Option<FallbackSource>,
    ) -> anyhow::Result<Self> {
        if reference.index != "assets.json" {
            return Err(anyhow!("unsupported asset index name: {}", reference.index));
        }
        let key = format!("{prefix}/{}", reference.index);
        let (bytes, _) = bucket.get(&key).await?.with_context(|| {
            format!(
                "read {}://{}/{key}: no such key",
                bucket.scheme(),
                bucket.name
            )
        })?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        if sha256 != reference.sha256 {
            return Err(anyhow!("asset index checksum mismatch"));
        }
        let index: AssetIndex = serde_json::from_slice(&bytes).context("decode asset index")?;
        validate_index(&index, reference)?;
        let header_rules = parse_header_rules(index.config.headers.as_deref().unwrap_or(""))?;
        let redirect_rules = parse_redirect_rules(index.config.redirects.as_deref().unwrap_or(""))?;
        let cache_root = std::env::var_os("CELLD_ASSET_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("celld").join("asset-cache"));
        std::fs::create_dir_all(&cache_root)
            .with_context(|| format!("create asset cache directory {}", cache_root.display()))?;
        let cache_max_bytes =
            crate::env_vars::with_default("CELLD_ASSET_CACHE_BYTES", DEFAULT_ASSET_CACHE_BYTES)?;
        Ok(Self {
            inner: Arc::new(AssetResolverInner {
                bucket: bucket.clone(),
                index,
                asset_only,
                fallback,
                fallback_state: std::sync::Mutex::new(FallbackState::default()),
                cache_root,
                cache_max_bytes,
                download_locks: Mutex::new(HashMap::new()),
                cleanup_lock: Mutex::new(()),
                header_rules,
                redirect_rules,
            }),
        })
    }

    pub fn binding_name(&self) -> Option<&str> {
        self.inner.index.config.binding.as_deref()
    }

    pub fn asset_only(&self) -> bool {
        self.inner.asset_only
    }

    pub fn should_run_worker_first(&self, encoded_path: &str) -> bool {
        let Some(path) = decode_path(encoded_path) else {
            return false;
        };
        match &self.inner.index.config.run_worker_first {
            RunWorkerFirst::Bool(value) => *value,
            RunWorkerFirst::Routes(rules) => {
                let excluded = rules
                    .iter()
                    .filter_map(|rule| rule.strip_prefix('!'))
                    .any(|rule| route_pattern_matches(rule, &path));
                !excluded
                    && rules
                        .iter()
                        .filter(|rule| !rule.starts_with('!'))
                        .any(|rule| route_pattern_matches(rule, &path))
            }
        }
    }

    pub async fn ingress_response(
        &self,
        encoded_path: &str,
        query: Option<&str>,
        head: bool,
        request_headers: &HeaderMap,
    ) -> anyhow::Result<Option<Response>> {
        let include_not_found =
            self.inner.asset_only || self.navigation_prefers_asset_serving(request_headers);
        let host = request_headers
            .get("host")
            .and_then(|value| value.to_str().ok())
            .map(strip_port);
        self.response(
            encoded_path,
            query,
            host,
            head,
            request_headers,
            include_not_found,
        )
        .await
    }

    async fn response(
        &self,
        encoded_path: &str,
        query: Option<&str>,
        host: Option<&str>,
        head: bool,
        request_headers: &HeaderMap,
        include_not_found: bool,
    ) -> anyhow::Result<Option<Response>> {
        let Some(path) = decode_path(encoded_path) else {
            return Ok(None);
        };
        let mut selected = None;
        for rule in &self.inner.redirect_rules {
            let Some(captures) = rule.pattern.matches(host, &path) else {
                continue;
            };
            let destination = substitute(&rule.destination, &captures);
            if rule.status == 200 {
                let boundary = destination.find(['?', '#']).unwrap_or(destination.len());
                let destination_path = &destination[..boundary];
                if !destination_path.starts_with('/') {
                    return Err(anyhow!("asset proxy destination must be a local path"));
                }
                selected = Some(route(&self.inner.index, destination_path, true));
            } else {
                let status = StatusCode::from_u16(rule.status)
                    .context("invalid static asset redirect status")?;
                return Ok(Some(
                    Response::builder()
                        .status(status)
                        .header("location", destination)
                        .body(Body::empty())?,
                ));
            }
            break;
        }
        let routed = match selected {
            Some(routed) => routed,
            None => {
                let mut routed = route(&self.inner.index, &path, false);
                if matches!(routed, Route::Missing) {
                    if let Some(fresh) = self.newer_deployment_route(&path) {
                        routed = fresh;
                    }
                }
                if matches!(routed, Route::Missing) && include_not_found {
                    routed = not_found_route(&self.inner.index, &path);
                }
                routed
            }
        };
        match routed {
            Route::Missing => Ok(None),
            Route::Redirect {
                mut location,
                status,
            } => {
                if let Some(query) = query.filter(|value| !value.is_empty()) {
                    location.push('?');
                    location.push_str(query);
                }
                Ok(Some(
                    Response::builder()
                        .status(status)
                        .header("location", location)
                        .body(Body::empty())?,
                ))
            }
            Route::Asset {
                path: resolved_path,
                entry,
                status,
            } => {
                let etag = format!("\"{}\"", entry.sha256);
                if request_headers
                    .get("if-none-match")
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| etag_matches(value, &etag))
                {
                    let mut response = Response::builder()
                        .status(StatusCode::NOT_MODIFIED)
                        .header("etag", etag)
                        .body(Body::empty())?;
                    self.apply_headers(response.headers_mut(), host, &path)?;
                    return Ok(Some(response));
                }
                let requested_range = if status == StatusCode::OK {
                    requested_range(request_headers, entry.bytes, &etag)
                } else {
                    RequestedRange::Full
                };
                if matches!(requested_range, RequestedRange::Unsatisfiable) {
                    let mut response = Response::builder()
                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                        .header("content-range", format!("bytes */{}", entry.bytes))
                        .header("accept-ranges", "bytes")
                        .header("etag", etag)
                        .body(Body::empty())?;
                    self.apply_headers(response.headers_mut(), host, &path)?;
                    return Ok(Some(response));
                }
                let (response_status, start, content_length) = match requested_range {
                    RequestedRange::Partial { start, end } => {
                        (StatusCode::PARTIAL_CONTENT, start, end - start + 1)
                    }
                    RequestedRange::Full | RequestedRange::Unsatisfiable => {
                        (status, 0, entry.bytes)
                    }
                };
                let mut builder = Response::builder()
                    .status(response_status)
                    .header("content-length", content_length)
                    .header("accept-ranges", "bytes")
                    .header("etag", &etag);
                if let RequestedRange::Partial { start, end } = requested_range {
                    builder = builder.header(
                        "content-range",
                        format!("bytes {start}-{end}/{}", entry.bytes),
                    );
                }
                if !request_headers.contains_key("authorization")
                    && !request_headers.contains_key("range")
                {
                    builder = builder.header("cache-control", "public, max-age=0, must-revalidate");
                }
                if let Some(content_type) = &entry.content_type {
                    builder = builder.header(
                        "content-type",
                        HeaderValue::from_str(content_type)
                            .context("invalid asset content type")?,
                    );
                }
                let body = if head {
                    Body::empty()
                } else {
                    let mut file = self.open_asset(&resolved_path, &entry).await?;
                    if start > 0 {
                        file.seek(std::io::SeekFrom::Start(start))
                            .await
                            .context("seek asset cache file")?;
                    }
                    let stream = ReaderStream::new(file.take(content_length))
                        .map(|result| result.map_err(std::io::Error::other));
                    Body::from_stream(stream)
                };
                let mut response = builder.body(body)?;
                self.apply_headers(response.headers_mut(), host, &path)?;
                Ok(Some(response))
            }
        }
    }

    pub async fn binding_response(
        &self,
        url: &str,
        method: &str,
        headers: &[(String, String)],
    ) -> anyhow::Result<js::HttpResponse> {
        if !matches!(method, "GET" | "HEAD") {
            return Ok(js::HttpResponse {
                status: StatusCode::METHOD_NOT_ALLOWED.as_u16(),
                body: b"Method not allowed".to_vec(),
                stream: None,
                headers: vec![("content-type".into(), "text/plain; charset=utf-8".into())],
                websocket: None,
                write_position: None,
                observed_position: None,
            });
        }
        let path = url::Url::parse(url).context("invalid assets binding URL")?;
        let mut request_headers = HeaderMap::new();
        for (name, value) in headers {
            let Ok(name) = name.parse::<axum::http::HeaderName>() else {
                continue;
            };
            let Ok(value) = HeaderValue::from_str(value) else {
                continue;
            };
            request_headers.append(name, value);
        }
        let response = match self
            .response(
                path.path(),
                path.query(),
                path.host_str(),
                method == "HEAD",
                &request_headers,
                true,
            )
            .await?
        {
            Some(response) => response,
            None => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("content-type", "text/plain; charset=utf-8")
                .body(Body::from("Not found"))?,
        };
        let (parts, body) = response.into_parts();
        Ok(js::HttpResponse {
            status: parts.status.as_u16(),
            body: Vec::new(),
            stream: Some(Box::pin(body.into_data_stream().map(|chunk| {
                chunk
                    .map(|bytes| bytes.to_vec())
                    .map_err(|error| format!("asset response stream: {error}"))
            }))),
            headers: parts
                .headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_string(),
                        value.to_str().unwrap_or_default().to_string(),
                    )
                })
                .collect(),
            websocket: None,
            write_position: None,
            observed_position: None,
        })
    }

    fn apply_headers(
        &self,
        headers: &mut HeaderMap,
        host: Option<&str>,
        path: &str,
    ) -> anyhow::Result<()> {
        let mut custom = std::collections::HashSet::new();
        for rule in &self.inner.header_rules {
            let Some(captures) = rule.pattern.matches(host, path) else {
                continue;
            };
            for operation in &rule.operations {
                match operation {
                    HeaderOperation::Remove(name) => {
                        headers.remove(name);
                        custom.insert(name.clone());
                    }
                    HeaderOperation::Set(name, value) => {
                        let value = substitute(value, &captures);
                        if custom.insert(name.clone()) {
                            headers.remove(name);
                        }
                        let combined = match headers.get(name).and_then(|value| value.to_str().ok())
                        {
                            Some(previous) => format!("{previous}, {value}"),
                            None => value,
                        };
                        headers.insert(
                            name.parse::<axum::http::HeaderName>()
                                .context("invalid custom asset header name")?,
                            HeaderValue::from_str(&combined)
                                .context("invalid custom asset header value")?,
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// A route from a NEWER deployment's index, when the fleet pointer has
    /// moved past the one this process booted with. Best-effort: every
    /// failure keeps the booted behavior, so the fallback can make an asset
    /// appear but never disappear. The lock never crosses I/O — the refresh
    /// runs in a spawned task on the host runtime and the triggering miss
    /// routes against the last adopted snapshot, so a degraded bucket
    /// degrades only the fallback, never the miss path. One refresh per
    /// `FALLBACK_CHECK_MS`, and the `refreshing` latch keeps a refresh that
    /// outlives the interval from stacking a second one behind it
    /// (denoland/celld#161).
    fn newer_deployment_route(&self, path: &str) -> Option<Route> {
        self.inner.fallback.as_ref()?;
        let (snapshot, refresh_due) = {
            let mut state = self.inner.fallback_state.lock().unwrap();
            let due = !state.refreshing
                && state.checked.is_none_or(|at| {
                    at.elapsed() >= std::time::Duration::from_millis(FALLBACK_CHECK_MS)
                });
            if due {
                state.refreshing = true;
                state.checked = Some(std::time::Instant::now());
            }
            (state.index.clone(), due)
        };
        if refresh_due {
            let resolver = self.clone();
            crate::asyncrt::op_handle().spawn(async move {
                resolver.refresh_fallback().await;
            });
        }
        let index = snapshot?;
        match route(&index, path, false) {
            Route::Missing => None,
            found => Some(found),
        }
    }

    /// One refresh pass: read the pointer, adopt a newer deployment's
    /// checksum-validated index, clear the adoption when the pointer moved
    /// BACK to the booted deployment — a rollback must withdraw the rolled-
    /// back assets, not pin them until restart.
    async fn refresh_fallback(&self) {
        let Some(source) = self.inner.fallback.as_ref() else {
            return;
        };
        let adopted = self.inner.fallback_state.lock().unwrap().prefix.clone();
        let outcome = self.adopt_newer_index(source, adopted.as_deref()).await;
        let mut state = self.inner.fallback_state.lock().unwrap();
        state.refreshing = false;
        match outcome {
            Ok(Adoption::Newer { prefix, index }) => {
                state.prefix = Some(prefix);
                state.index = Some(Arc::new(*index));
            }
            Ok(Adoption::Booted) => {
                state.prefix = None;
                state.index = None;
            }
            Ok(Adoption::Unchanged) => {}
            Err(error) => {
                tracing::warn!(%error, "asset version-skew fallback refresh failed");
            }
        }
    }

    /// Read the pointer and, when it names a deployment this resolver has
    /// not adopted, load and validate that deployment's asset index with
    /// the same gates the boot path applies.
    async fn adopt_newer_index(
        &self,
        source: &FallbackSource,
        adopted: Option<&str>,
    ) -> anyhow::Result<Adoption> {
        let bucket = &self.inner.bucket;
        let (bytes, _) = bucket
            .get(&source.pointer_key)
            .await?
            .with_context(|| format!("read {}", source.pointer_key))?;
        let pointer: DeployPointer =
            serde_json::from_slice(&bytes).context("decode deploy pointer")?;
        if pointer.prefix == source.boot_prefix {
            return Ok(Adoption::Booted);
        }
        if Some(pointer.prefix.as_str()) == adopted {
            return Ok(Adoption::Unchanged);
        }
        let manifest_key = format!("{}/manifest.json", pointer.prefix);
        let (bytes, _) = bucket
            .get(&manifest_key)
            .await?
            .with_context(|| format!("read {manifest_key}"))?;
        let manifest: Manifest =
            serde_json::from_slice(&bytes).context("decode deployment manifest")?;
        crate::protocol::validate_required_features(&manifest.required_features)?;
        let Some(reference) = manifest.assets else {
            // The newer deployment has no assets: adopt an empty index so
            // the pointer is not re-fetched for every miss until it moves.
            return Ok(Adoption::Newer {
                prefix: pointer.prefix,
                index: Box::default(),
            });
        };
        let index_key = format!("{}/{}", pointer.prefix, reference.index);
        let (bytes, _) = bucket
            .get(&index_key)
            .await?
            .with_context(|| format!("read {index_key}"))?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        if sha256 != reference.sha256 {
            return Err(anyhow!("fallback asset index checksum mismatch"));
        }
        let index: AssetIndex = serde_json::from_slice(&bytes).context("decode asset index")?;
        validate_index(&index, &reference)?;
        Ok(Adoption::Newer {
            prefix: pointer.prefix,
            index: Box::new(index),
        })
    }

    fn navigation_prefers_asset_serving(&self, headers: &HeaderMap) -> bool {
        if headers
            .get("sec-fetch-mode")
            .and_then(|value| value.to_str().ok())
            != Some("navigate")
        {
            return false;
        }
        let config = &self.inner.index.config;
        if config
            .compatibility_flags
            .iter()
            .any(|flag| flag == "assets_navigation_has_no_effect")
        {
            return false;
        }
        config
            .compatibility_flags
            .iter()
            .any(|flag| flag == "assets_navigation_prefers_asset_serving")
            || config
                .compatibility_date
                .as_deref()
                .is_some_and(|date| date >= "2025-04-01")
    }

    async fn open_asset(
        &self,
        resolved_path: &str,
        entry: &AssetEntry,
    ) -> anyhow::Result<tokio::fs::File> {
        let cache_path = cache_path(&self.inner.cache_root, &entry.sha256)?;
        if let Some(file) = valid_cached_file(&cache_path, entry.bytes).await? {
            touch_cache_file(cache_path.clone());
            return Ok(file);
        }

        let lock = {
            let mut locks = self.inner.download_locks.lock().await;
            locks
                .entry(entry.sha256.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = lock.lock().await;
        if let Some(file) = valid_cached_file(&cache_path, entry.bytes).await? {
            touch_cache_file(cache_path.clone());
            return Ok(file);
        }

        let parent = cache_path
            .parent()
            .context("asset cache path has no parent")?;
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create asset cache shard {}", parent.display()))?;
        let mut random = rand::rngs::OsRng;
        let temporary = parent.join(format!(".{}.{}.tmp", std::process::id(), random.next_u64()));
        let key = asset_blob_key(&entry.sha256)
            .ok_or_else(|| anyhow!("invalid asset digest for {resolved_path}"))?;
        let (body, _) = self
            .inner
            .bucket
            .get(&key)
            .await
            .with_context(|| format!("read asset {resolved_path}"))?
            .with_context(|| {
                format!(
                    "asset {resolved_path} missing from {}://{}/{key}",
                    self.inner.bucket.scheme(),
                    self.inner.bucket.name
                )
            })?;
        if body.len() as u64 != entry.bytes {
            return Err(anyhow!("asset {resolved_path} has the wrong bucket size"));
        }
        let mut file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .with_context(|| format!("create asset cache file {}", temporary.display()))?;
        let write = async {
            file.write_all(&body).await?;
            file.sync_all().await?;
            anyhow::Ok(())
        }
        .await;
        if let Err(error) = write {
            drop(file);
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error);
        }
        drop(file);
        let actual = format!("{:x}", Sha256::digest(&body));
        if actual != entry.sha256 {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(anyhow!(
                "asset {resolved_path} checksum mismatch: expected {}, got {actual}",
                entry.sha256
            ));
        }
        if valid_cached_file(&cache_path, entry.bytes).await?.is_some() {
            let _ = tokio::fs::remove_file(&temporary).await;
        } else {
            tokio::fs::rename(&temporary, &cache_path)
                .await
                .with_context(|| {
                    format!(
                        "publish asset cache file {} -> {}",
                        temporary.display(),
                        cache_path.display()
                    )
                })?;
        }
        let file = tokio::fs::File::open(&cache_path)
            .await
            .with_context(|| format!("open asset cache file {}", cache_path.display()))?;
        touch_cache_file(cache_path);
        let resolver = self.clone();
        tokio::spawn(async move {
            if let Err(error) = resolver.cleanup_cache().await {
                tracing::warn!(
                    event = "asset_cache_cleanup_failed",
                    %error,
                    "could not enforce the local asset cache bound"
                );
            }
        });
        Ok(file)
    }

    async fn cleanup_cache(&self) -> anyhow::Result<()> {
        let _guard = self.inner.cleanup_lock.lock().await;
        let root = self.inner.cache_root.clone();
        let limit = self.inner.cache_max_bytes;
        tokio::task::spawn_blocking(move || cleanup_cache_files(&root, limit))
            .await
            .context("join asset cache cleanup")?
    }
}

impl RulePattern {
    fn parse(value: &str, allow_absolute: bool) -> anyhow::Result<Self> {
        let (host, path) = if value.starts_with("https://") {
            if !allow_absolute {
                return Err(anyhow!(
                    "absolute URLs are not supported in _redirects sources"
                ));
            }
            let url = url::Url::parse(value).context("invalid absolute asset rule URL")?;
            if url.scheme() != "https" || url.port().is_some() || url.query().is_some() {
                return Err(anyhow!(
                    "absolute asset rules require an HTTPS host without a port"
                ));
            }
            (
                Some(compile_rule_component(
                    url.host_str().context("asset rule URL has no host")?,
                    '.',
                )?),
                url.path().to_string(),
            )
        } else {
            if !value.starts_with('/') {
                return Err(anyhow!("asset rule paths must begin with '/'"));
            }
            (None, value.to_string())
        };
        Ok(Self {
            host,
            path: compile_rule_component(&path, '/')?,
        })
    }

    fn matches(&self, host: Option<&str>, path: &str) -> Option<HashMap<String, String>> {
        let mut values = HashMap::new();
        if let Some(pattern) = &self.host {
            let captures = pattern.captures(host?)?;
            collect_captures(pattern, &captures, &mut values);
        }
        let captures = self.path.captures(path)?;
        collect_captures(&self.path, &captures, &mut values);
        Some(values)
    }
}

fn compile_rule_component(value: &str, delimiter: char) -> anyhow::Result<regex::Regex> {
    let mut expression = String::from("^");
    let mut characters = value.chars().peekable();
    let mut captures = std::collections::HashSet::new();
    while let Some(character) = characters.next() {
        if character == '*' {
            if !captures.insert("splat".to_string()) {
                return Err(anyhow!("asset rules may contain only one splat"));
            }
            expression.push_str("(?P<splat>.*)");
            continue;
        }
        if character == ':'
            && characters
                .peek()
                .is_some_and(|next| next.is_ascii_alphabetic())
        {
            let mut name = String::new();
            while characters
                .peek()
                .is_some_and(|next| next.is_ascii_alphanumeric() || *next == '_')
            {
                name.push(characters.next().unwrap());
            }
            if !captures.insert(name.clone()) {
                return Err(anyhow!("asset rule placeholder {name:?} is duplicated"));
            }
            expression.push_str(&format!(
                "(?P<{name}>[^{delimiter}]+)",
                delimiter = regex::escape(&delimiter.to_string())
            ));
            continue;
        }
        expression.push_str(&regex::escape(&character.to_string()));
    }
    expression.push('$');
    regex::Regex::new(&expression).context("compile asset routing rule")
}

fn collect_captures(
    pattern: &regex::Regex,
    captures: &regex::Captures<'_>,
    values: &mut HashMap<String, String>,
) {
    for name in pattern.capture_names().flatten() {
        if let Some(value) = captures.name(name) {
            values.insert(name.to_string(), value.as_str().to_string());
        }
    }
}

fn substitute(template: &str, captures: &HashMap<String, String>) -> String {
    let mut result = String::with_capacity(template.len());
    let mut characters = template.chars().peekable();
    while let Some(character) = characters.next() {
        if character != ':'
            || !characters
                .peek()
                .is_some_and(|next| next.is_ascii_alphabetic())
        {
            result.push(character);
            continue;
        }
        let mut name = String::new();
        while characters
            .peek()
            .is_some_and(|next| next.is_ascii_alphanumeric() || *next == '_')
        {
            name.push(characters.next().unwrap());
        }
        if let Some(value) = captures.get(&name) {
            result.push_str(value);
        } else {
            result.push(':');
            result.push_str(&name);
        }
    }
    result
}

fn parse_header_rules(value: &str) -> anyhow::Result<Vec<HeaderRule>> {
    let mut rules: Vec<HeaderRule> = Vec::new();
    let mut current: Option<usize> = None;
    for (line_number, line) in value.lines().enumerate() {
        if line.len() > 2_000 {
            return Err(anyhow!(
                "_headers line {} exceeds 2,000 characters",
                line_number + 1
            ));
        }
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if line.starts_with(char::is_whitespace) {
            let index = current.context("_headers contains an operation before its URL pattern")?;
            if let Some(name) = trimmed.strip_prefix("! ") {
                let name = parse_custom_header_name(name)?;
                rules[index].operations.push(HeaderOperation::Remove(name));
            } else {
                let (name, value) = trimmed
                    .split_once(':')
                    .context("_headers operation is missing ':'")?;
                let name = parse_custom_header_name(name)?;
                HeaderValue::from_str(value.trim()).context("invalid _headers value")?;
                rules[index]
                    .operations
                    .push(HeaderOperation::Set(name, value.trim().to_string()));
            }
            continue;
        }
        if rules.len() >= 100 {
            return Err(anyhow!("_headers exceeds 100 rules"));
        }
        rules.push(HeaderRule {
            pattern: RulePattern::parse(trimmed, true)
                .with_context(|| format!("invalid _headers rule on line {}", line_number + 1))?,
            operations: Vec::new(),
        });
        current = Some(rules.len() - 1);
    }
    Ok(rules)
}

fn parse_custom_header_name(value: &str) -> anyhow::Result<String> {
    let name = value
        .trim()
        .parse::<axum::http::HeaderName>()
        .context("invalid _headers name")?
        .as_str()
        .to_string();
    if matches!(
        name.as_str(),
        "connection" | "content-length" | "transfer-encoding"
    ) {
        return Err(anyhow!("_headers cannot override {name}"));
    }
    Ok(name)
}

fn parse_redirect_rules(value: &str) -> anyhow::Result<Vec<RedirectRule>> {
    let mut rules = Vec::new();
    let mut static_rules = 0_usize;
    let mut dynamic_rules = 0_usize;
    for (line_number, line) in value.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.len() > 1_000 {
            return Err(anyhow!(
                "_redirects line {} exceeds 1,000 characters",
                line_number + 1
            ));
        }
        let parts = line.split_whitespace().collect::<Vec<_>>();
        if !(2..=3).contains(&parts.len()) {
            return Err(anyhow!(
                "invalid _redirects rule on line {}",
                line_number + 1
            ));
        }
        let status = match parts.get(2) {
            Some(value) => value.parse::<u16>().context("invalid _redirects status")?,
            None => 302,
        };
        if !matches!(status, 200 | 301 | 302 | 303 | 307 | 308) {
            return Err(anyhow!("unsupported _redirects status {status}"));
        }
        if status == 200 && !parts[1].starts_with('/') {
            return Err(anyhow!("_redirects proxy destinations must be local paths"));
        }
        if status != 200
            && !parts[1].starts_with('/')
            && url::Url::parse(parts[1])
                .ok()
                .is_none_or(|url| !matches!(url.scheme(), "http" | "https"))
        {
            return Err(anyhow!("invalid _redirects destination"));
        }
        let dynamic = parts[0].contains('*') || parts[0].contains(':');
        if dynamic {
            dynamic_rules += 1;
        } else {
            static_rules += 1;
        }
        if static_rules > 2_000 || dynamic_rules > 100 || rules.len() >= 2_100 {
            return Err(anyhow!("_redirects exceeds Cloudflare's rule limits"));
        }
        rules.push(RedirectRule {
            pattern: RulePattern::parse(parts[0], false).with_context(|| {
                format!("invalid _redirects source on line {}", line_number + 1)
            })?,
            destination: parts[1].to_string(),
            status,
        });
    }
    Ok(rules)
}

fn strip_port(host: &str) -> &str {
    if host.starts_with('[') {
        return host.find(']').map_or(host, |end| &host[1..end]);
    }
    host.rsplit_once(':')
        .filter(|(_, port)| port.bytes().all(|byte| byte.is_ascii_digit()))
        .map_or(host, |(host, _)| host)
}

fn route(index: &AssetIndex, path: &str, include_not_found: bool) -> Route {
    let entries = &index.entries;
    let html = index
        .config
        .html_handling
        .as_deref()
        .unwrap_or("auto-trailing-slash");

    // Non-HTML assets are never affected by trailing-slash canonicalization.
    if !path.ends_with(".html") {
        if let Some(entry) = entries.get(path) {
            return asset_route(path, entry, StatusCode::OK);
        }
    }

    if html != "none" {
        if let Some((asset_path, entry)) = html_asset(entries, path) {
            let canonical = canonical_html_path(&asset_path, html);
            if path != canonical {
                return Route::Redirect {
                    location: canonical,
                    status: StatusCode::TEMPORARY_REDIRECT,
                };
            }
            return asset_route(&asset_path, entry, StatusCode::OK);
        }
    } else if let Some(entry) = entries.get(path) {
        return asset_route(path, entry, StatusCode::OK);
    }

    if !include_not_found {
        return Route::Missing;
    }
    not_found_route(index, path)
}

/// The `not_found_handling` tail of `route`, separated so the version-skew
/// fallback can run between an exact miss and the not-found fabrication: a
/// single-page application must serve a NEW deployment's hashed asset, not
/// its own stale `/index.html`, when both are possible.
fn not_found_route(index: &AssetIndex, path: &str) -> Route {
    let entries = &index.entries;
    match index.config.not_found_handling.as_deref() {
        Some("single-page-application") => entries
            .get("/index.html")
            .map(|entry| asset_route("/index.html", entry, StatusCode::OK))
            .unwrap_or(Route::Missing),
        Some("404-page") => nearest_404(entries, path)
            .map(|(path, entry)| asset_route(&path, entry, StatusCode::NOT_FOUND))
            .unwrap_or(Route::Missing),
        _ => Route::Missing,
    }
}

fn asset_route(path: &str, entry: &AssetEntry, status: StatusCode) -> Route {
    Route::Asset {
        path: path.to_string(),
        entry: entry.clone(),
        status,
    }
}

fn html_asset<'a>(
    entries: &'a std::collections::BTreeMap<String, AssetEntry>,
    path: &str,
) -> Option<(String, &'a AssetEntry)> {
    let mut candidates = Vec::with_capacity(3);
    if path.ends_with(".html") {
        candidates.push(path.to_string());
    } else if path.ends_with("/index") {
        candidates.push(format!("{path}.html"));
    } else if path.ends_with('/') {
        candidates.push(format!("{path}index.html"));
        if path != "/" {
            candidates.push(format!("{}.html", path.trim_end_matches('/')));
        }
    } else {
        candidates.push(format!("{path}.html"));
        candidates.push(format!("{path}/index.html"));
    }
    candidates
        .into_iter()
        .find_map(|candidate| entries.get(&candidate).map(|entry| (candidate, entry)))
}

fn canonical_html_path(asset_path: &str, html_handling: &str) -> String {
    if asset_path == "/index.html" {
        return "/".to_string();
    }
    let base = if let Some(parent) = asset_path.strip_suffix("/index.html") {
        parent
    } else {
        asset_path.strip_suffix(".html").unwrap_or(asset_path)
    };
    match html_handling {
        "force-trailing-slash" => format!("{}/", base.trim_end_matches('/')),
        "drop-trailing-slash" => {
            let value = base.trim_end_matches('/');
            if value.is_empty() {
                "/".to_string()
            } else {
                value.to_string()
            }
        }
        _ if asset_path.ends_with("/index.html") => format!("{}/", base.trim_end_matches('/')),
        _ => base.trim_end_matches('/').to_string(),
    }
}

fn nearest_404<'a>(
    entries: &'a std::collections::BTreeMap<String, AssetEntry>,
    path: &str,
) -> Option<(String, &'a AssetEntry)> {
    let mut directory = if path.ends_with('/') {
        path.trim_end_matches('/').to_string()
    } else {
        path.rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
            .unwrap_or_default()
    };
    loop {
        let candidate = if directory.is_empty() {
            "/404.html".to_string()
        } else {
            format!("{directory}/404.html")
        };
        if let Some(entry) = entries.get(&candidate) {
            return Some((candidate, entry));
        }
        if directory.is_empty() {
            return None;
        }
        directory = directory
            .rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
            .unwrap_or_default();
    }
}

fn etag_matches(value: &str, etag: &str) -> bool {
    value.split(',').any(|candidate| {
        let candidate = candidate.trim();
        candidate == "*" || candidate == etag || candidate.strip_prefix("W/") == Some(etag)
    })
}

fn requested_range(headers: &HeaderMap, bytes: u64, etag: &str) -> RequestedRange {
    let Some(value) = headers.get("range").and_then(|value| value.to_str().ok()) else {
        return RequestedRange::Full;
    };
    if headers
        .get("if-range")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() != etag)
    {
        return RequestedRange::Full;
    }
    let Some(value) = value.strip_prefix("bytes=") else {
        return RequestedRange::Unsatisfiable;
    };
    if value.contains(',') || bytes == 0 {
        return RequestedRange::Unsatisfiable;
    }
    let Some((start, end)) = value.split_once('-') else {
        return RequestedRange::Unsatisfiable;
    };
    if start.is_empty() {
        let Ok(suffix) = end.parse::<u64>() else {
            return RequestedRange::Unsatisfiable;
        };
        if suffix == 0 {
            return RequestedRange::Unsatisfiable;
        }
        let length = suffix.min(bytes);
        return RequestedRange::Partial {
            start: bytes - length,
            end: bytes - 1,
        };
    }
    let Ok(start) = start.parse::<u64>() else {
        return RequestedRange::Unsatisfiable;
    };
    if start >= bytes {
        return RequestedRange::Unsatisfiable;
    }
    let end = if end.is_empty() {
        bytes - 1
    } else {
        let Ok(end) = end.parse::<u64>() else {
            return RequestedRange::Unsatisfiable;
        };
        end.min(bytes - 1)
    };
    if end < start {
        return RequestedRange::Unsatisfiable;
    }
    RequestedRange::Partial { start, end }
}

fn cache_path(root: &Path, sha256: &str) -> anyhow::Result<PathBuf> {
    asset_blob_key(sha256)
        .context("invalid asset cache digest")
        .map(|_| root.join(&sha256[..2]).join(sha256))
}

async fn valid_cached_file(
    path: &Path,
    expected_bytes: u64,
) -> anyhow::Result<Option<tokio::fs::File>> {
    match tokio::fs::File::open(path).await {
        Ok(file) => {
            let metadata = file
                .metadata()
                .await
                .with_context(|| format!("stat asset cache file {}", path.display()))?;
            if metadata.len() == expected_bytes {
                Ok(Some(file))
            } else {
                drop(file);
                let _ = tokio::fs::remove_file(path).await;
                Ok(None)
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("open asset cache file {}", path.display()))
        }
    }
}

fn touch_cache_file(path: PathBuf) {
    tokio::task::spawn_blocking(move || {
        let times = std::fs::FileTimes::new().set_modified(std::time::SystemTime::now());
        let _ = std::fs::File::options()
            .write(true)
            .open(path)
            .and_then(|file| file.set_times(times));
    });
}

fn cleanup_cache_files(root: &Path, limit: u64) -> anyhow::Result<()> {
    let mut files = Vec::new();
    let mut total = 0_u64;
    for shard in
        std::fs::read_dir(root).with_context(|| format!("scan asset cache {}", root.display()))?
    {
        let shard = shard?;
        if !shard.file_type()?.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(shard.path())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.len() != 64
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                continue;
            }
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                continue;
            }
            total = total.saturating_add(metadata.len());
            files.push((
                metadata.modified().unwrap_or(std::time::UNIX_EPOCH),
                metadata.len(),
                entry.path(),
            ));
        }
    }
    if total <= limit {
        return Ok(());
    }
    files.sort_by_key(|(modified, _, _)| *modified);
    for (_, bytes, path) in files {
        if total <= limit {
            break;
        }
        if std::fs::remove_file(&path).is_ok() {
            total = total.saturating_sub(bytes);
        }
    }
    Ok(())
}

fn validate_index(index: &AssetIndex, reference: &AssetManifestRef) -> anyhow::Result<()> {
    if index.schema_version != 1 {
        return Err(anyhow!(
            "unsupported asset index schema: {}",
            index.schema_version
        ));
    }
    if index.entries.len() != reference.file_count as usize {
        return Err(anyhow!("asset index file count mismatch"));
    }
    if index.entries.len() > 20_000 {
        return Err(anyhow!("asset index exceeds the file limit"));
    }
    let config = &index.config;
    if !matches!(
        config
            .html_handling
            .as_deref()
            .unwrap_or("auto-trailing-slash"),
        "auto-trailing-slash" | "force-trailing-slash" | "drop-trailing-slash" | "none"
    ) {
        return Err(anyhow!("unsupported asset HTML handling"));
    }
    if !matches!(
        config.not_found_handling.as_deref().unwrap_or("none"),
        "none" | "single-page-application" | "404-page"
    ) {
        return Err(anyhow!("unsupported asset not-found handling"));
    }
    if let Some(binding) = &config.binding {
        let valid = binding.len() <= 128
            && binding
                .chars()
                .next()
                .is_some_and(|value| value == '_' || value == '$' || value.is_ascii_alphabetic())
            && binding
                .chars()
                .skip(1)
                .all(|value| value == '_' || value == '$' || value.is_ascii_alphanumeric());
        if !valid {
            return Err(anyhow!("invalid asset binding name"));
        }
    }
    if let RunWorkerFirst::Routes(routes) = &config.run_worker_first {
        if routes.is_empty() || routes.len() > 100 {
            return Err(anyhow!("invalid asset worker-first route count"));
        }
        let mut positive = false;
        let mut seen = std::collections::HashSet::new();
        for route in routes {
            if route.starts_with('/') {
                positive = true;
            } else if !route.starts_with("!/") {
                return Err(anyhow!("invalid asset worker-first route: {route:?}"));
            }
            if route.len() > 100 || !seen.insert(route) {
                return Err(anyhow!("invalid asset worker-first route: {route:?}"));
            }
        }
        if !positive {
            return Err(anyhow!("asset worker-first routes require a positive rule"));
        }
    }
    let mut total = 0_u64;
    for (path, entry) in &index.entries {
        if !is_canonical_asset_path(path)
            || path == "/"
            || asset_blob_key(&entry.sha256).is_none()
            || entry.bytes > 25 * 1024 * 1024
        {
            return Err(anyhow!("invalid asset index entry: {path:?}"));
        }
        if entry
            .content_type
            .as_deref()
            .is_some_and(|value| value.len() > 200 || value.contains(['\r', '\n']))
        {
            return Err(anyhow!("invalid asset content type: {path:?}"));
        }
        total = total
            .checked_add(entry.bytes)
            .context("asset index byte count overflow")?;
    }
    if total != reference.total_bytes {
        return Err(anyhow!("asset index total byte count mismatch"));
    }
    if total > 1024 * 1024 * 1024 {
        return Err(anyhow!("asset index exceeds the deployment byte limit"));
    }
    Ok(())
}

fn decode_path(path: &str) -> Option<String> {
    if !path.starts_with('/') || path.contains('\\') || path.contains('\0') {
        return None;
    }
    let decoded = percent_decode_str(path).decode_utf8().ok()?.into_owned();
    if decoded.contains('\\')
        || decoded.contains('\0')
        || decoded.split('/').skip(1).any(|segment| {
            segment.is_empty() && decoded != "/" || segment == "." || segment == ".."
        })
    {
        return None;
    }
    Some(decoded)
}

/// A stored path must survive request-path decoding unchanged. The deployer
/// uses this same predicate because publishing an index with a second decoded
/// spelling would make every node reject the complete deployment.
pub(crate) fn is_canonical_asset_path(path: &str) -> bool {
    decode_path(path).as_deref() == Some(path)
}

fn route_pattern_matches(pattern: &str, path: &str) -> bool {
    let mut expression = String::from("^");
    for part in pattern.split('*') {
        expression.push_str(&regex::escape(part));
        expression.push_str(".*");
    }
    if !pattern.ends_with('*') {
        expression.truncate(expression.len().saturating_sub(2));
        expression.push('$');
    }
    regex::Regex::new(&expression).is_ok_and(|pattern| pattern.is_match(path))
}
