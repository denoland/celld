//! replica_url.rs — parse `s3://` and `file://` replica URLs + S3 endpoint
//! detection helpers.
//!
//! Ported from litestream@v0.5.11 `replica_url.go`. Scope is the KEEP set (D-7):
//! the pure URL-parsing and endpoint-detection functions for the **s3** and
//! **file** schemes. The `RegisterReplicaClientFactory` registry and the
//! `NewReplicaClientFromURL` *client construction* path are deferred to the
//! client implementations. The crate does not include the gs, abs, oss, sftp,
//! or webdav schemes.

use crate::error::{Error, Result};

fn url_err(msg: impl Into<String>) -> Error {
    Error::Other(msg.into().into())
}

// ── Query (a tiny ordered multimap, like Go's url.Values) ─────────────────────

/// Parsed query parameters. Mirrors `url.Values`: ordered, allows duplicate
/// keys, and `get` returns the first value for a key.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Query(Vec<(String, String)>);

impl Query {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// First value for `key`, or `""` if absent (matches Go `url.Values.Get`).
    pub fn get(&self, key: &str) -> &str {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    }

    pub fn contains(&self, key: &str) -> bool {
        self.0.iter().any(|(k, _)| k == key)
    }
}

/// Parses an `a=b&c=d` query string with percent-decoding, like
/// `url.ParseQuery`. `+` decodes to a space (form semantics).
fn parse_query(s: &str) -> Query {
    let mut out = Vec::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        out.push((percent_decode(k), percent_decode(v)));
    }
    Query(out)
}

/// Minimal `application/x-www-form-urlencoded` decode: `+` → space and
/// `%XX` → byte. Invalid escapes are left verbatim (best effort).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ── Scheme + URL parsing ──────────────────────────────────────────────────────

/// The fields parsed from a replica URL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedReplicaUrl {
    pub scheme: String,
    pub host: String,
    pub path: String,
    pub query: Query,
    /// `user[:password]` userinfo, if present.
    pub userinfo: Option<String>,
}

/// Extracts and lowercases the URL scheme (matching Go `net/url`, which
/// lowercases schemes). Returns `("", s)` when there is no valid leading
/// `scheme:` (e.g. `"invalid"`, `"relative/path"`).
fn parse_scheme(s: &str) -> (String, &str) {
    let bytes = s.as_bytes();
    for (i, &c) in bytes.iter().enumerate() {
        match c {
            b'a'..=b'z' | b'A'..=b'Z' => {}
            b'0'..=b'9' | b'+' | b'-' | b'.' if i > 0 => {}
            b':' if i > 0 => return (s[..i].to_ascii_lowercase(), &s[i + 1..]),
            _ => return (String::new(), s),
        }
    }
    (String::new(), s)
}

/// Parses a replica URL into scheme/host/path/query/userinfo.
///
/// Ported from `ParseReplicaURLWithQuery` (replica_url.go:79-104). The
/// `s3://arn:` Access-Point form is handled specially because a raw ARN is not
/// a parseable authority.
pub fn parse_replica_url_with_query(s: &str) -> Result<ParsedReplicaUrl> {
    // A fragment is not part of the replica location. Remove it before the
    // custom ARN, path, and query parsers can treat it as stored data.
    let s = s.split_once('#').map_or(s, |(before, _)| before);

    if s.to_ascii_lowercase().starts_with("s3://arn:") {
        let (scheme, host, path, query) = parse_s3_access_point_url(s)?;
        return Ok(ParsedReplicaUrl {
            scheme,
            host,
            path,
            query,
            userinfo: None,
        });
    }

    let (scheme, rest) = parse_scheme(s);
    if scheme.is_empty() {
        return Err(url_err(format!("replica url scheme required: {s}")));
    }

    if scheme == "file" {
        // Go blanks the scheme + query and returns path.Clean(u.String()).
        let no_query = rest.split('?').next().unwrap_or(rest);
        let (_, host, path, _) = split_authority(no_query);
        let reconstructed = if host.is_empty() {
            path
        } else {
            format!("//{host}{path}")
        };
        return Ok(ParsedReplicaUrl {
            scheme,
            host: String::new(),
            path: crate::path_clean(&reconstructed),
            query: Query::default(),
            userinfo: None,
        });
    }

    // Default (s3, …): scheme://[userinfo@]host[/path][?query].
    let (q_str, before_q) = match rest.split_once('?') {
        Some((a, b)) => (Some(b), a),
        None => (None, rest),
    };
    let (userinfo, host, path, _) = split_authority(before_q);
    let query = q_str.map(parse_query).unwrap_or_default();
    Ok(ParsedReplicaUrl {
        scheme,
        host,
        // strings.TrimPrefix(path.Clean(u.Path), "/")
        path: crate::path_clean(&path)
            .strip_prefix('/')
            .map(str::to_string)
            .unwrap_or_else(|| crate::path_clean(&path)),
        query,
        userinfo,
    })
}

/// Splits a post-scheme remainder of the form `//[user@]host[/path]` into
/// `(userinfo, host, path)`. The leading `//` is optional/tolerated; `path`
/// keeps its leading slash. `query` is always returned empty here (callers
/// split it off first).
fn split_authority(rest: &str) -> (Option<String>, String, String, Query) {
    let r = rest.strip_prefix("//").unwrap_or(rest);
    let (authority, path) = match r.find('/') {
        Some(i) => (&r[..i], &r[i..]),
        None => (r, ""),
    };
    let (userinfo, host) = match authority.rfind('@') {
        Some(i) => (Some(authority[..i].to_string()), &authority[i + 1..]),
        None => (None, authority),
    };
    (
        userinfo,
        host.to_string(),
        path.to_string(),
        Query::default(),
    )
}

/// Parses a replica URL into scheme/host/path (no query/userinfo).
///
/// Ported from `ParseReplicaURL` (replica_url.go:68-76).
pub fn parse_replica_url(s: &str) -> Result<(String, String, String)> {
    let p = parse_replica_url_with_query(s)?;
    Ok((p.scheme, p.host, p.path))
}

/// Returns the replica backend type from a URL (`""` if no scheme).
/// `webdavs` normalizes to `webdav`. Ported from replica_url.go:56-65.
pub fn replica_type_from_url(s: &str) -> String {
    let (scheme, _) = parse_scheme(s);
    if scheme.is_empty() {
        return String::new();
    }
    if scheme == "webdavs" {
        return "webdav".to_string();
    }
    scheme
}

/// `true` if `s` looks like a URL (`^\w+://`). Ported from replica_url.go:364-369.
pub fn is_url(s: &str) -> bool {
    if let Some(idx) = s.find("://") {
        idx > 0
            && s[..idx]
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    } else {
        false
    }
}

// ── S3 Access Point ARN parsing ───────────────────────────────────────────────

/// Parses an `s3://arn:…:accesspoint/<name>[/key]` URL.
/// Ported from `parseS3AccessPointURL` (replica_url.go:107-136).
fn parse_s3_access_point_url(s: &str) -> Result<(String, String, String, Query)> {
    const PREFIX: &str = "s3://";
    if !s.to_ascii_lowercase().starts_with(PREFIX) {
        return Err(url_err(format!("invalid s3 access point url: {s}")));
    }
    let arn_with_path = &s[PREFIX.len()..];

    let (arn_with_path, query_str) = match arn_with_path.find('?') {
        Some(idx) => (&arn_with_path[..idx], Some(&arn_with_path[idx + 1..])),
        None => (arn_with_path, None),
    };

    let (bucket, key) = split_s3_access_point_arn(arn_with_path)?;
    let query = query_str.map(parse_query).unwrap_or_default();
    Ok((
        "s3".to_string(),
        bucket,
        clean_replica_url_path(&key),
        query,
    ))
}

/// Splits an S3 Access Point ARN into `(bucket, key)`.
/// Ported from `splitS3AccessPointARN` (replica_url.go:139-162).
fn split_s3_access_point_arn(s: &str) -> Result<(String, String)> {
    const MARKER: &str = ":accesspoint/";
    let lower = s.to_ascii_lowercase();
    let idx = lower
        .find(MARKER)
        .ok_or_else(|| url_err(format!("invalid s3 access point arn: {s}")))?;
    let name_start = idx + MARKER.len();
    if name_start >= s.len() {
        return Err(url_err(format!("invalid s3 access point arn: {s}")));
    }
    let remainder = &s[name_start..];
    match remainder.find('/') {
        None => Ok((s.to_string(), String::new())),
        Some(slash_idx) => {
            let bucket_end = name_start + slash_idx;
            Ok((
                s[..bucket_end].to_string(),
                remainder[slash_idx + 1..].to_string(),
            ))
        }
    }
}

/// Cleans a URL path for replica storage. Ported from replica_url.go:165-175.
pub fn clean_replica_url_path(p: &str) -> String {
    if p.is_empty() {
        return String::new();
    }
    let cleaned = crate::path_clean(&format!("/{p}"));
    let cleaned = cleaned.strip_prefix('/').unwrap_or(&cleaned);
    if cleaned == "." {
        return String::new();
    }
    cleaned.to_string()
}

/// Extracts the region from an S3 ARN (`arn:aws:s3:<region>:…`).
/// Ported from replica_url.go:178-184.
pub fn region_from_s3_arn(arn: &str) -> String {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() >= 4 {
        parts[3].to_string()
    } else {
        String::new()
    }
}

/// Reads a boolean from query params, checking `keys` in order. Returns `None`
/// when no key is set (Go's `ok == false`); an unrecognized value yields
/// `Some(false)`. Ported from `BoolQueryValue` (replica_url.go:188-205).
pub fn bool_query_value(query: Option<&Query>, keys: &[&str]) -> Option<bool> {
    let query = query?;
    for key in keys {
        let raw = query.get(key);
        if !raw.is_empty() {
            return Some(matches!(
                raw.to_ascii_lowercase().as_str(),
                "true" | "1" | "t" | "yes"
            ));
        }
    }
    None
}

// ── Endpoint detection (S3 provider/local heuristics) ─────────────────────────

/// Extracts the host from an endpoint URL, or returns it as-is.
/// Ported from `extractEndpointHost` (replica_url.go:351-362).
fn extract_endpoint_host(endpoint: &str) -> String {
    let endpoint = endpoint.trim().to_ascii_lowercase();
    if endpoint.is_empty() {
        return String::new();
    }
    for scheme in ["http://", "https://"] {
        if let Some(rest) = endpoint.strip_prefix(scheme) {
            let host = rest.split('/').next().unwrap_or(rest);
            if !host.is_empty() {
                return host.to_string();
            }
            return endpoint;
        }
    }
    endpoint
}

fn host_has_suffix(endpoint: &str, suffix: &str) -> bool {
    let host = extract_endpoint_host(endpoint);
    !host.is_empty() && host.ends_with(suffix)
}

pub fn is_tigris_endpoint(endpoint: &str) -> bool {
    let host = extract_endpoint_host(endpoint);
    host == "fly.storage.tigris.dev" || host == "t3.storage.dev"
}
pub fn is_hetzner_endpoint(endpoint: &str) -> bool {
    host_has_suffix(endpoint, ".your-objectstorage.com")
}
pub fn is_digitalocean_endpoint(endpoint: &str) -> bool {
    host_has_suffix(endpoint, ".digitaloceanspaces.com")
}
pub fn is_backblaze_endpoint(endpoint: &str) -> bool {
    host_has_suffix(endpoint, ".backblazeb2.com")
}
pub fn is_filebase_endpoint(endpoint: &str) -> bool {
    extract_endpoint_host(endpoint) == "s3.filebase.com"
}
pub fn is_scaleway_endpoint(endpoint: &str) -> bool {
    host_has_suffix(endpoint, ".scw.cloud")
}
pub fn is_cloudflare_r2_endpoint(endpoint: &str) -> bool {
    host_has_suffix(endpoint, ".r2.cloudflarestorage.com")
}
pub fn is_supabase_endpoint(endpoint: &str) -> bool {
    host_has_suffix(endpoint, ".supabase.co")
}

/// `true` if the endpoint looks like MinIO (a custom `host:port` that is not a
/// known cloud). Ported from `IsMinIOEndpoint` (replica_url.go:278-301).
pub fn is_minio_endpoint(endpoint: &str) -> bool {
    let host = extract_endpoint_host(endpoint);
    if host.is_empty() || !host.contains(':') {
        return false;
    }
    const KNOWN: [&str; 9] = [
        ".amazonaws.com",
        ".digitaloceanspaces.com",
        ".backblazeb2.com",
        ".filebase.com",
        ".scw.cloud",
        ".r2.cloudflarestorage.com",
        "tigris.dev",
        "t3.storage.dev",
        ".supabase.co",
    ];
    !KNOWN.iter().any(|k| host.contains(k))
}

/// `true` for localhost / loopback / RFC1918 / `.local` endpoints.
/// Ported from `IsLocalEndpoint` (replica_url.go:306-329).
pub fn is_local_endpoint(endpoint: &str) -> bool {
    let mut host = extract_endpoint_host(endpoint);
    if host.is_empty() {
        return false;
    }
    if let Some(idx) = host.rfind(':') {
        host = host[..idx].to_string();
    }
    let is_private_ipv4 = host
        .parse::<std::net::Ipv4Addr>()
        .is_ok_and(|address| address.is_private());
    host == "localhost"
        || host == "127.0.0.1"
        || is_private_ipv4
        || host.ends_with(".local")
        || host.ends_with(".localhost")
}

/// Ensures an endpoint has an http(s) scheme: local → `http://`, else
/// `https://`. Returns `(endpoint, scheme_was_added)`.
/// Ported from `EnsureEndpointScheme` (replica_url.go:335-347).
pub fn ensure_endpoint_scheme(endpoint: &str) -> (String, bool) {
    if endpoint.is_empty() {
        return (String::new(), false);
    }
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return (endpoint.to_string(), false);
    }
    if is_local_endpoint(endpoint) {
        (format!("http://{endpoint}"), true)
    } else {
        (format!("https://{endpoint}"), true)
    }
}
