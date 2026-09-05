// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The V8 surface over an `r2_buckets` binding.
//!
//! celld does not run a blob service; it runs *on* one. A binding is
//! therefore served out of the fleet bucket the node already holds
//! credentials for, under the reserved `r2/<bucket_name>/` prefix — the
//! same durability, the same store, no second set of credentials. A node
//! with no bucket (a local run without `--bucket`) has nowhere to put a
//! blob, and every op says so rather than pretending.
//!
//! Each op converts V8 values to Rust, calls `crate::bucket`, and converts
//! the answer back. The one thing decided here is how an R2 object's
//! record — `httpMetadata`, `customMetadata`, `checksums`, `storageClass`
//! — is spelled in a store that has none of those concepts; see
//! [`Envelope`]. What the binding covers, and where it diverges from R2,
//! is documented on the harness side in `__makeR2Bucket`.

use super::*;
use crate::bucket::BlobAttributes;
use crate::bucket::BlobConditions;
use crate::bucket::BlobMeta;
use crate::bucket::BlobRange;
use crate::bucket::BlobRead;
use crate::bucket::Bucket;
use object_store::MultipartUpload;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

/// The fleet bucket, once a node has opened one. Installed beside the
/// wake-entry gate at startup; absent for a bucketless run.
static R2_STORE: OnceLock<Bucket> = OnceLock::new();

/// Give the R2 bindings the fleet bucket to live in.
pub fn set_r2_store(bucket: Bucket) {
    let _ = R2_STORE.set(bucket);
}

/// A page of an R2 listing. R2's own default and maximum is 1,000.
const LIST_LIMIT: usize = 1000;

/// How many of a listing page's heads are in flight at once when the
/// caller asked for `include`. A page can hold a thousand objects, and
/// reading their metadata one round trip after another would make the
/// option unusable.
const LIST_HEADS: usize = 16;

/// How long an untouched multipart upload or streaming write is kept
/// before the host abandons it. A push that streams a multi-gigabyte pack
/// part by part refreshes its entry on every part, so only an abandoned
/// one ages out.
const IDLE: Duration = Duration::from_secs(3600);

/// The part size a streaming `put` cuts at once it is too big to write in
/// one request. Above S3's 5 MiB floor, and a round number of pages.
const STREAM_PART: usize = 8 << 20;

/// How far ahead of the object store a caller may run with out-of-order
/// multipart parts before the host stops holding them. Parts are handed
/// to the store in ascending order, so a part that arrives before its
/// predecessor waits in memory; this bounds that wait.
const PART_BACKLOG: usize = 256 << 20;

/// The user-metadata name the R2 object record lives under. See
/// [`Envelope`].
const ENVELOPE: &str = "celld-r2";

/// The key space one binding owns inside the fleet bucket. `bucket_name`
/// comes from the deployment manifest, which validates it, so the prefix
/// cannot escape into the fleet's own keys.
fn blob_key(bucket_name: &str, key: &str) -> String {
    format!("r2/{bucket_name}/{key}")
}

/// The fleet bucket, or the error every op answers without one.
fn store() -> Result<&'static Bucket, String> {
    R2_STORE.get().ok_or_else(|| {
        "R2 bindings need a fleet bucket: start celld with --bucket (or CELLD_BUCKET)".to_string()
    })
}

// ---- the object record ---------------------------------------------------

/// R2's `httpMetadata`. Five of the six are ordinary HTTP headers that
/// every backend stores as headers, and travel as headers. `cacheExpiry`
/// is not a header anywhere, so it travels in the [`Envelope`] with the
/// rest of the record.
#[derive(Default, Clone, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
struct HttpMeta {
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_disposition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<String>,
    /// Milliseconds since the epoch, as R2 spells it.
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_expiry: Option<i64>,
}

/// Everything an R2 object carries that a blob store has no place for.
///
/// A store keeps bytes, five content headers, and a flat map of ASCII
/// user metadata whose key case it is free to fold. R2 keeps that plus
/// case-sensitive `customMetadata` with arbitrary text in it, a
/// `cacheExpiry`, a set of checksums, and a storage class. So the record
/// is written as one JSON value under a single reserved user-metadata
/// name, escaped to ASCII, and the five content headers are *also*
/// written as headers so the stored object is a well-formed object rather
/// than a celld-private encoding.
///
/// An object with no envelope — one this runtime wrote before the record
/// existed, or one another tool put in the bucket — still reads: its user
/// metadata is its `customMetadata`, and its headers are its
/// `httpMetadata`. That is the whole reason the envelope is additive.
#[derive(Default, Clone, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
struct Envelope {
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    custom: BTreeMap<String, String>,
    http: HttpMeta,
    /// Lowercase algorithm name (`md5`, `sha1`, `sha256`, `sha384`,
    /// `sha512`) to lowercase hex.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    checksums: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_class: Option<String>,
}

impl Envelope {
    /// The record on an object the store answered. The five content
    /// headers come off the response, because they are the object's own
    /// headers and stay right even for an object written by another tool.
    fn read(attributes: &BlobAttributes) -> Self {
        let mut envelope = attributes
            .metadata
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(ENVELOPE))
            .and_then(|(_, value)| serde_json::from_str::<Self>(value).ok())
            .unwrap_or_else(|| Self {
                custom: attributes
                    .metadata
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
                ..Self::default()
            });
        envelope.http.content_type = attributes.content_type.clone();
        envelope.http.content_language = attributes.content_language.clone();
        envelope.http.content_disposition = attributes.content_disposition.clone();
        envelope.http.content_encoding = attributes.content_encoding.clone();
        envelope.http.cache_control = attributes.cache_control.clone();
        envelope
    }

    /// The store-side attributes that carry this record.
    fn write(&self) -> BlobAttributes {
        BlobAttributes {
            content_type: self.http.content_type.clone(),
            content_language: self.http.content_language.clone(),
            content_disposition: self.http.content_disposition.clone(),
            content_encoding: self.http.content_encoding.clone(),
            cache_control: self.http.cache_control.clone(),
            metadata: vec![(ENVELOPE.to_string(), ascii_json(self))],
        }
    }
}

/// A JSON value with every non-ASCII character escaped. User metadata is
/// an HTTP header on all three backends and only US-ASCII survives the
/// trip; JSON's `\u` escapes make that lossless rather than lossy.
fn ascii_json<T: Serialize>(value: &T) -> String {
    let json = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string());
    if json.is_ascii() {
        return json;
    }
    // Only string contents can be non-ASCII in JSON, so escaping any such
    // character in place stays valid JSON.
    let mut out = String::with_capacity(json.len());
    for character in json.chars() {
        match character.is_ascii() {
            true => out.push(character),
            false => {
                let mut units = [0u16; 2];
                for unit in character.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    out
}

/// One R2 object, in the shape `__makeR2Bucket` turns into an `R2Object`.
/// `range` is present only on the answer to a `get`.
fn object_json(key: &str, meta: &BlobMeta, range: Option<(u64, u64)>) -> serde_json::Value {
    let envelope = Envelope::read(&meta.attributes);
    let mut json = serde_json::json!({
        "key": key,
        "size": meta.size,
        // R2 gives every write a version id. A versioned bucket has one;
        // everywhere else the etag names the same thing — the bytes this
        // key held at this moment.
        "version": meta.version.clone().or_else(|| meta.etag.clone()),
        "etag": meta.etag,
        "uploaded": meta.uploaded_ms,
        "http": envelope.http,
        "custom": envelope.custom,
        "checksums": envelope.checksums,
        "storageClass": envelope.storage_class.unwrap_or_else(|| "Standard".to_string()),
    });
    if let Some((offset, length)) = range {
        json["range"] = serde_json::json!({ "offset": offset, "length": length });
    }
    json
}

// ---- requests ------------------------------------------------------------

/// R2's `onlyIf`, already normalized by the harness: the `Headers` form
/// and the `R2Conditional` form arrive here the same way.
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Conditional {
    etag_matches: Option<String>,
    etag_does_not_match: Option<String>,
    /// Milliseconds since the epoch.
    uploaded_before: Option<i64>,
    uploaded_after: Option<i64>,
}

impl From<Conditional> for BlobConditions {
    fn from(conditional: Conditional) -> Self {
        Self {
            if_match: conditional.etag_matches,
            if_none_match: conditional.etag_does_not_match,
            uploaded_before_ms: conditional.uploaded_before,
            uploaded_after_ms: conditional.uploaded_after,
        }
    }
}

/// R2's `R2Range`, normalized by the harness to the three fields R2
/// documents. `suffix` wins over the other two, as it does on R2.
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct Range {
    offset: Option<u64>,
    length: Option<u64>,
    suffix: Option<u64>,
}

impl From<Range> for BlobRange {
    fn from(range: Range) -> Self {
        match (range.suffix, range.offset, range.length) {
            (Some(suffix), _, _) => Self::Suffix(suffix),
            (None, offset, Some(length)) => Self::Bounded {
                offset: offset.unwrap_or(0),
                length,
            },
            (None, Some(offset), None) => Self::From(offset),
            (None, None, None) => Self::Whole,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct GetRequest {
    range: Option<Range>,
    only_if: Option<Conditional>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct PutRequest {
    http: HttpMeta,
    custom: BTreeMap<String, String>,
    storage_class: Option<String>,
    only_if: Option<Conditional>,
    /// Algorithm name to the lowercase hex digest the caller asserts.
    /// A mismatch is a refused write, as it is on R2.
    verify: BTreeMap<String, String>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct ListRequest {
    prefix: String,
    cursor: Option<String>,
    start_after: Option<String>,
    limit: Option<i64>,
    delimiter: Option<String>,
    /// `true` when the caller asked for `httpMetadata` or
    /// `customMetadata`, which a listing does not carry and a head does.
    include: bool,
}

// ---- checksums -----------------------------------------------------------

/// The digests an R2 write computes over its own bytes. R2 always records
/// an md5 for an object written in one request, and records whichever
/// other digests the caller asserted; nothing else is computed, because
/// nothing else would ever be read back.
#[derive(Default)]
struct Digests {
    md5: Option<md5::Md5>,
    sha1: Option<sha1::Sha1>,
    sha256: Option<sha2::Sha256>,
    sha384: Option<sha2::Sha384>,
    sha512: Option<sha2::Sha512>,
}

impl Digests {
    /// `md5` unless this write cannot honestly claim one, plus every
    /// algorithm the caller asserted.
    fn wanted(asserted: &BTreeMap<String, String>, md5: bool) -> Self {
        let has = |name: &str| asserted.contains_key(name);
        Self {
            md5: (md5 || has("md5")).then(md5::Md5::default),
            sha1: has("sha1").then(sha1::Sha1::default),
            sha256: has("sha256").then(sha2::Sha256::default),
            sha384: has("sha384").then(sha2::Sha384::default),
            sha512: has("sha512").then(sha2::Sha512::default),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        use sha2::Digest as _;
        if let Some(digest) = &mut self.md5 {
            digest.update(bytes);
        }
        if let Some(digest) = &mut self.sha1 {
            digest.update(bytes);
        }
        if let Some(digest) = &mut self.sha256 {
            digest.update(bytes);
        }
        if let Some(digest) = &mut self.sha384 {
            digest.update(bytes);
        }
        if let Some(digest) = &mut self.sha512 {
            digest.update(bytes);
        }
    }

    fn finish(self) -> BTreeMap<String, String> {
        use sha2::Digest as _;
        let mut out = BTreeMap::new();
        let mut take = |name: &str, digest: Option<Vec<u8>>| {
            if let Some(digest) = digest {
                out.insert(name.to_string(), hex(&digest));
            }
        };
        take("md5", self.md5.map(|digest| digest.finalize().to_vec()));
        take("sha1", self.sha1.map(|digest| digest.finalize().to_vec()));
        take(
            "sha256",
            self.sha256.map(|digest| digest.finalize().to_vec()),
        );
        take(
            "sha384",
            self.sha384.map(|digest| digest.finalize().to_vec()),
        );
        take(
            "sha512",
            self.sha512.map(|digest| digest.finalize().to_vec()),
        );
        out
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, byte| {
        out.push_str(&format!("{byte:02x}"));
        out
    })
}

/// Check the caller's asserted digests against the computed ones. R2
/// refuses a write whose checksum does not match what arrived, which is
/// the entire point of sending one.
fn verify(
    asserted: &BTreeMap<String, String>,
    computed: &BTreeMap<String, String>,
) -> Result<(), String> {
    for (name, claimed) in asserted {
        let claimed = claimed.trim().to_ascii_lowercase();
        match computed.get(name) {
            Some(actual) if *actual == claimed => {}
            Some(actual) => {
                return Err(format!(
                    "R2 put: the {name} checksum of the body is {actual}, not the {claimed} the \
                     caller asserted"
                ))
            }
            None => return Err(format!("R2 put: celld cannot check a {name} checksum")),
        }
    }
    Ok(())
}

// ---- reads ---------------------------------------------------------------

/// `__r2_head(bucketName, key)`. Resolves to the object's record, or to
/// `{"state":"miss"}` when there is no such key.
pub(super) fn op_r2_head(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let bucket_name = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let id = asyncrt::enqueue(async move {
        let meta = store()?
            .head_blob(&blob_key(&bucket_name, &key))
            .await
            .map_err(|error| error.to_string())?;
        Ok(match meta {
            None => serde_json::json!({ "state": "miss" }).to_string(),
            Some(meta) => serde_json::json!({
                "state": "hit",
                "object": object_json(&key, &meta, None),
            })
            .to_string(),
        })
    });
    rv.set(promise_for(scope, id));
}

/// `__r2_get(bucketName, key, requestJson)`. Resolves to a JSON envelope;
/// a hit's body is a host stream the caller drains as a `ReadableStream`,
/// so a blob never has to fit in the isolate's heap. A read whose
/// `onlyIf` was refused answers `unmet` and the record with no body, as
/// R2 does.
pub(super) fn op_r2_get(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let bucket_name = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let request = serde_json::from_str::<GetRequest>(&args.get(2).to_rust_string_lossy(scope))
        .map_err(|error| format!("invalid R2 get options: {error}"));
    let stream_service = http_stream_service();
    let id = asyncrt::enqueue(async move {
        let request = request?;
        let range = request
            .range
            .map(BlobRange::from)
            .unwrap_or(BlobRange::Whole);
        let conditions = BlobConditions::from(request.only_if.unwrap_or_default());
        let read = store()?
            .get_blob(&blob_key(&bucket_name, &key), range, &conditions)
            .await
            .map_err(|error| error.to_string())?;
        Ok(match read {
            BlobRead::Missing => serde_json::json!({ "state": "miss" }).to_string(),
            BlobRead::Unmet(meta) => serde_json::json!({
                "state": "unmet",
                "object": object_json(&key, &meta, None),
            })
            .to_string(),
            BlobRead::Hit(blob) => {
                let stream_id = stream_service
                    .register_source(HttpStreamSource::Stream(blob.body))
                    .ok_or_else(|| format!("R2 get: {HTTP_STREAM_REGISTRATION_CLOSED}"))?;
                serde_json::json!({
                    "state": "hit",
                    "object": object_json(&key, &blob.meta, Some(blob.range)),
                    "streamId": stream_id,
                })
                .to_string()
            }
        })
    });
    rv.set(promise_for(scope, id));
}

/// `__r2_delete(bucketName, keysJson)`. Deleting an absent key succeeds,
/// as it does on R2.
pub(super) fn op_r2_delete(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let bucket_name = args.get(0).to_rust_string_lossy(scope);
    let keys = serde_json::from_str::<Vec<String>>(&args.get(1).to_rust_string_lossy(scope))
        .map_err(|error| format!("invalid R2 delete key list: {error}"));
    let id = asyncrt::enqueue(async move {
        let keys = keys?;
        let store = store()?;
        let keys = keys
            .iter()
            .map(|key| blob_key(&bucket_name, key))
            .collect::<Vec<_>>();
        // `delete_many` reports what went; anything left is a failure the
        // caller must see, because R2's delete either applies or throws.
        let gone = store.delete_many(&keys).await.len();
        if gone != keys.len() {
            return Err(format!(
                "R2 delete removed {gone} of {} keys; the rest failed",
                keys.len()
            ));
        }
        Ok(String::new())
    });
    rv.set(promise_for(scope, id));
}

/// `__r2_list(bucketName, requestJson)`. An absent cursor starts at the
/// first key. The cursor a truncated page answers with is the last key it
/// consumed, so the next page resumes strictly after it.
pub(super) fn op_r2_list(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let bucket_name = args.get(0).to_rust_string_lossy(scope);
    let request = serde_json::from_str::<ListRequest>(&args.get(1).to_rust_string_lossy(scope))
        .map_err(|error| format!("invalid R2 list options: {error}"));
    let id = asyncrt::enqueue(async move {
        let request = request?;
        let store = store()?;
        let limit = match request.limit.unwrap_or(0) {
            limit if limit > 0 => (limit as usize).min(LIST_LIMIT),
            _ => LIST_LIMIT,
        };
        let scoped = blob_key(&bucket_name, &request.prefix);
        // R2 resumes from the cursor when it has one and ignores
        // `startAfter`, which is the same knob for a first page.
        let after = request
            .cursor
            .filter(|cursor| !cursor.is_empty())
            .or(request.start_after)
            .filter(|after| !after.is_empty())
            .map(|after| blob_key(&bucket_name, &after));
        let page = store
            .list_page(
                &scoped,
                after.as_deref(),
                limit,
                request.delimiter.as_deref(),
            )
            .await
            .map_err(|error| error.to_string())?;
        // The binding's key space is the caller's: strip the reserved
        // prefix back off, so a listed key is one the caller can `get`.
        let strip = blob_key(&bucket_name, "");
        let unscope = |key: &str| key.strip_prefix(&strip).unwrap_or(key).to_string();
        // A listing carries no metadata on any object store; `include` is
        // R2 saying it is worth one head per object to have it, and those
        // heads go out together rather than one after another.
        let objects = match request.include {
            false => page
                .objects
                .iter()
                .map(|entry| {
                    let meta = BlobMeta {
                        size: entry.size,
                        etag: entry.etag.clone(),
                        version: entry.version.clone(),
                        cas: None,
                        uploaded_ms: entry.uploaded_ms,
                        attributes: BlobAttributes::default(),
                    };
                    object_json(&unscope(&entry.key), &meta, None)
                })
                .collect::<Vec<_>>(),
            true => {
                let keys = page
                    .objects
                    .iter()
                    .map(|entry| entry.key.clone())
                    .collect::<Vec<_>>();
                let heads = futures_util::stream::iter(keys)
                    .map(|key| async move {
                        store
                            .head_blob(&key)
                            .await
                            .map_err(|error| error.to_string())
                    })
                    .buffered(LIST_HEADS)
                    .collect::<Vec<_>>()
                    .await;
                let mut objects = Vec::with_capacity(heads.len());
                for (entry, head) in page.objects.iter().zip(heads) {
                    // A key deleted between the listing and the head is one
                    // R2 would not have listed either.
                    if let Some(meta) = head? {
                        objects.push(object_json(&unscope(&entry.key), &meta, None));
                    }
                }
                objects
            }
        };
        Ok(serde_json::json!({
            "objects": objects,
            "prefixes": page.prefixes.iter().map(|prefix| unscope(prefix)).collect::<Vec<_>>(),
            "truncated": page.truncated,
            "cursor": page.cursor.as_deref().map(unscope),
        })
        .to_string())
    });
    rv.set(promise_for(scope, id));
}

// ---- writes --------------------------------------------------------------

/// The bytes of an `ArrayBuffer` view argument, or `None` when the
/// argument is not one.
fn view_bytes(value: v8::Local<v8::Value>) -> Option<Vec<u8>> {
    let view = value.try_cast::<v8::ArrayBufferView>().ok()?;
    let mut bytes = vec![0; view.byte_length()];
    view.copy_contents(&mut bytes);
    Some(bytes)
}

/// Write `body` under `key`, honoring the request's conditions and
/// checksums. The single-request path: a caller that handed R2 a buffer
/// gets one PUT, and one md5 R2 would also have computed.
async fn put_once(
    bucket_name: String,
    key: String,
    body: Vec<u8>,
    request: PutRequest,
) -> Result<String, String> {
    let mut digests = Digests::wanted(&request.verify, true);
    digests.update(&body);
    let checksums = digests.finish();
    verify(&request.verify, &checksums)?;
    let envelope = Envelope {
        custom: request.custom,
        http: request.http,
        checksums,
        storage_class: request.storage_class,
    };
    let conditions = BlobConditions::from(request.only_if.unwrap_or_default());
    let meta = store()?
        .put_blob(
            &blob_key(&bucket_name, &key),
            body.into(),
            &envelope.write(),
            &conditions,
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(match meta {
        // R2 answers a refused precondition with `null`, not a throw.
        None => serde_json::json!({ "stored": false }).to_string(),
        Some(meta) => serde_json::json!({
            "stored": true,
            "object": object_json(&key, &meta, None),
        })
        .to_string(),
    })
}

/// `__r2_put(bucketName, key, bytes, requestJson)`. The whole body is in
/// the isolate already, so it goes in one request.
pub(super) fn op_r2_put(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let bucket_name = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let body = view_bytes(args.get(2));
    let request = serde_json::from_str::<PutRequest>(&args.get(3).to_rust_string_lossy(scope))
        .map_err(|error| format!("invalid R2 put options: {error}"));
    let id = asyncrt::enqueue(async move {
        let request = request?;
        let Some(body) = body else {
            return Err("R2 put: the body must be an ArrayBuffer view".to_string());
        };
        put_once(bucket_name, key, body, request).await
    });
    rv.set(promise_for(scope, id));
}

/// A `put` whose body is a `ReadableStream`: the isolate hands over one
/// chunk at a time and the host decides, at the first part boundary,
/// whether this is one request or a multipart upload.
struct PutStream {
    bucket_name: String,
    key: String,
    request: PutRequest,
    digests: Digests,
    /// Bytes not yet handed to the store.
    buffered: Vec<u8>,
    size: u64,
    /// Open once the body outgrew a single request.
    upload: Option<Box<dyn MultipartUpload>>,
    touched: Instant,
}

impl PutStream {
    /// Take in one chunk, handing the store every whole part it makes.
    async fn push(&mut self, chunk: Vec<u8>) -> Result<(), String> {
        self.digests.update(&chunk);
        self.size += chunk.len() as u64;
        self.buffered.extend_from_slice(&chunk);
        while self.buffered.len() > STREAM_PART {
            let part = self.buffered.drain(..STREAM_PART).collect::<Vec<_>>();
            self.part(part).await?;
        }
        Ok(())
    }

    /// Hand one whole part to the store, opening the upload if this is the
    /// first.
    async fn part(&mut self, bytes: Vec<u8>) -> Result<(), String> {
        if self.upload.is_none() {
            // A precondition is checked against the version a write is
            // about to replace, and a multipart completion takes none. A
            // small streamed body still goes out as one conditional
            // request; this one has outgrown that.
            if self.request.only_if.is_some() {
                return Err(format!(
                    "R2 put of {}: celld cannot apply `onlyIf` to a streamed body larger than \
                     {STREAM_PART} bytes, because it is written as a multipart upload and a \
                     multipart completion takes no precondition",
                    self.key
                ));
            }
            // The record is fixed when the upload opens, so it can only
            // carry checksums already known: the ones the caller asserted,
            // which the completion refuses to apply if the bytes disagree.
            // A computed md5 is not one of them, and R2's multipart
            // objects do not carry one either.
            if !self.request.verify.contains_key("md5") {
                self.digests.md5 = None;
            }
            self.upload = Some(
                store()?
                    .begin_multipart(
                        &blob_key(&self.bucket_name, &self.key),
                        &self.envelope().write(),
                    )
                    .await
                    .map_err(|error| error.to_string())?,
            );
        }
        let upload = self.upload.as_mut().expect("just opened");
        upload
            .put_part(bytes.into())
            .await
            .map_err(|error| format!("R2 put of {}: {error}", self.key))
    }

    /// Close the stream out: one request if nothing was ever parted off,
    /// the multipart completion otherwise.
    async fn finish(&mut self) -> Result<String, String> {
        let Some(mut upload) = self.upload.take() else {
            return put_once(
                std::mem::take(&mut self.bucket_name),
                std::mem::take(&mut self.key),
                std::mem::take(&mut self.buffered),
                std::mem::take(&mut self.request),
            )
            .await;
        };
        let mut failed = None;
        if !self.buffered.is_empty() {
            let last = std::mem::take(&mut self.buffered);
            if let Err(error) = upload.put_part(last.into()).await {
                failed = Some(format!("R2 put of {}: {error}", self.key));
            }
        }
        let computed = std::mem::take(&mut self.digests).finish();
        if failed.is_none() {
            failed = verify(&self.request.verify, &computed).err();
        }
        // A body that did not match what the caller asserted is not
        // written at all, so the parts already on the store go away.
        if let Some(error) = failed {
            if let Err(error) = upload.abort().await {
                tracing::warn!(%error, key = self.key, "refused R2 write could not be aborted");
            }
            return Err(error);
        }
        let result = upload
            .complete()
            .await
            .map_err(|error| format!("R2 put of {}: {error}", self.key))?;
        let meta = BlobMeta {
            size: self.size,
            etag: result.e_tag.clone(),
            version: result.version,
            cas: None,
            uploaded_ms: asyncrt::wall_ms(),
            // The record the object actually carries, which is the one
            // written when the upload opened.
            attributes: self.envelope().write(),
        };
        Ok(serde_json::json!({
            "stored": true,
            "object": object_json(&self.key, &meta, None),
        })
        .to_string())
    }

    /// The record a multipart-sized body is stored with.
    fn envelope(&self) -> Envelope {
        Envelope {
            custom: self.request.custom.clone(),
            http: self.request.http.clone(),
            checksums: self.request.verify.clone(),
            storage_class: self.request.storage_class.clone(),
        }
    }
}

/// The open writes, by host id.
///
/// The inner lock is asynchronous because the work it guards is: a Worker
/// may have several calls against one write in flight, and they have to
/// queue behind each other rather than find the entry missing. The outer
/// lock only ever guards the map.
type Open<T> = std::sync::Mutex<HashMap<u64, Arc<tokio::sync::Mutex<T>>>>;

static PUTS: OnceLock<Open<PutStream>> = OnceLock::new();

fn puts() -> &'static Open<PutStream> {
    PUTS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Abandon every streaming write nothing has touched for [`IDLE`], and
/// abort whatever parts it left on the store.
fn reap_puts() {
    let stale = {
        let mut puts = puts().lock().unwrap();
        let ids = puts
            .iter()
            .filter(|(_, put)| {
                // A write with a call in flight is busy, not abandoned.
                put.try_lock()
                    .is_ok_and(|put| put.touched.elapsed() >= IDLE)
            })
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| puts.remove(&id))
            .collect::<Vec<_>>()
    };
    for put in stale {
        asyncrt::op_handle().spawn(async move {
            let Some(mut upload) = put.lock().await.upload.take() else {
                return;
            };
            if let Err(error) = upload.abort().await {
                tracing::warn!(%error, "abandoned R2 streaming write could not be aborted");
            }
        });
    }
}

/// `__r2_put_begin(bucketName, key, requestJson)`. Resolves to the host id
/// of the open write.
pub(super) fn op_r2_put_begin(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let bucket_name = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let request = serde_json::from_str::<PutRequest>(&args.get(2).to_rust_string_lossy(scope))
        .map_err(|error| format!("invalid R2 put options: {error}"));
    let id = asyncrt::enqueue(async move {
        let request = request?;
        reap_puts();
        // Fail before a byte moves if there is no bucket at all.
        store()?;
        let digests = Digests::wanted(&request.verify, true);
        let put_id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        puts().lock().unwrap().insert(
            put_id,
            Arc::new(tokio::sync::Mutex::new(PutStream {
                bucket_name,
                key,
                request,
                digests,
                buffered: Vec::new(),
                size: 0,
                upload: None,
                touched: Instant::now(),
            })),
        );
        Ok(put_id.to_string())
    });
    rv.set(promise_for(scope, id));
}

/// `__r2_put_chunk(putId, bytes)`. One chunk of the caller's
/// `ReadableStream`. The isolate awaits each one, so the backpressure the
/// stream needs is the promise this returns.
pub(super) fn op_r2_put_chunk(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let put_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let chunk = view_bytes(args.get(1));
    let id = asyncrt::enqueue(async move {
        let Some(chunk) = chunk else {
            return Err("R2 put: a body chunk must be an ArrayBuffer view".to_string());
        };
        let put = puts()
            .lock()
            .unwrap()
            .get(&put_id)
            .cloned()
            .ok_or_else(|| format!("R2 streaming write {put_id} is not open"))?;
        // A failed chunk leaves the write open, so `__r2_put_end` can
        // abort the parts already on the store.
        let mut put = put.lock().await;
        put.touched = Instant::now();
        put.push(chunk).await.map(|()| String::new())
    });
    rv.set(promise_for(scope, id));
}

/// `__r2_put_end(putId, abort)`. Completes the write, or throws away
/// whatever it had when the caller's stream errored.
pub(super) fn op_r2_put_end(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let put_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let abort = args.get(1).boolean_value(scope);
    let put = puts().lock().unwrap().remove(&put_id);
    let id = asyncrt::enqueue(async move {
        let Some(put) = put else {
            return Err(format!("R2 streaming write {put_id} is not open"));
        };
        let mut put = put.lock().await;
        if abort {
            if let Some(mut upload) = put.upload.take() {
                let _ = upload.abort().await;
            }
            return Ok(serde_json::json!({ "stored": false }).to_string());
        }
        put.finish().await
    });
    rv.set(promise_for(scope, id));
}

// ---- multipart -----------------------------------------------------------

struct UploadEntry {
    bucket_name: String,
    key: String,
    upload: Box<dyn MultipartUpload>,
    /// Part numbers already handed to the store, in that order. The store
    /// owns the part bookkeeping and concatenates the parts in the order
    /// it received them, so this is the object's real shape.
    flushed: Vec<u32>,
    /// Parts that arrived before the one in front of them, held until
    /// their turn. A caller uploading parts sequentially never fills this.
    pending: BTreeMap<u32, Vec<u8>>,
    /// Bytes in `pending`, against [`PART_BACKLOG`].
    held: usize,
    /// The record the object was opened with, answered again by the
    /// completion the way R2 answers one.
    envelope: Envelope,
    /// Bytes handed to the store, which is the completed object's size.
    written: u64,
    touched: Instant,
}

impl UploadEntry {
    /// The next part number the store can take. Nothing may go before
    /// part 1, because a part that arrived first is not necessarily the
    /// first part; everything after that is one past what last went.
    fn next(&self) -> u32 {
        self.flushed.last().map_or(1, |last| last + 1)
    }

    /// Hand the store every held part that is now in turn.
    async fn drain(&mut self) -> Result<(), String> {
        loop {
            let next = self.next();
            let Some(bytes) = self.pending.remove(&next) else {
                return Ok(());
            };
            self.held -= bytes.len();
            self.written += bytes.len() as u64;
            self.upload
                .put_part(bytes.into())
                .await
                .map_err(|error| format!("R2 multipart part {next} of {}: {error}", self.key))?;
            self.flushed.push(next);
        }
    }
}

static UPLOADS: OnceLock<Open<UploadEntry>> = OnceLock::new();

/// celld runs one deployment per node, so open writes deliberately share
/// one deployment-global id space across that node's isolates.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn uploads() -> &'static Open<UploadEntry> {
    UPLOADS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Abort every upload nothing has touched for [`IDLE`]. A dropped handle
/// leaves the parts on the store, so the abort is issued rather than left
/// to the bucket's lifecycle rules.
fn reap_uploads() {
    let stale = {
        let mut uploads = uploads().lock().unwrap();
        let ids = uploads
            .iter()
            .filter(|(_, entry)| {
                // An upload with a part in flight is busy, not abandoned.
                entry
                    .try_lock()
                    .is_ok_and(|entry| entry.touched.elapsed() >= IDLE)
            })
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| uploads.remove(&id))
            .collect::<Vec<_>>()
    };
    for entry in stale {
        asyncrt::op_handle().spawn(async move {
            if let Err(error) = entry.lock().await.upload.abort().await {
                tracing::warn!(%error, "abandoned R2 multipart upload could not be aborted");
            }
        });
    }
}

/// `__r2_mp_begin(bucketName, key, requestJson)`. Resolves to the host id
/// of the open upload.
pub(super) fn op_r2_mp_begin(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let bucket_name = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let request = serde_json::from_str::<PutRequest>(&args.get(2).to_rust_string_lossy(scope))
        .map_err(|error| format!("invalid R2 multipart options: {error}"));
    let id = asyncrt::enqueue(async move {
        let request = request?;
        reap_uploads();
        // A multipart object carries no md5, on R2 or here, so nothing is
        // computed over parts that were never seen whole.
        let envelope = Envelope {
            custom: request.custom,
            http: request.http,
            checksums: BTreeMap::new(),
            storage_class: request.storage_class,
        };
        let upload = store()?
            .begin_multipart(&blob_key(&bucket_name, &key), &envelope.write())
            .await
            .map_err(|error| error.to_string())?;
        let upload_id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        uploads().lock().unwrap().insert(
            upload_id,
            Arc::new(tokio::sync::Mutex::new(UploadEntry {
                bucket_name,
                key,
                upload,
                flushed: Vec::new(),
                pending: BTreeMap::new(),
                held: 0,
                envelope,
                written: 0,
                touched: Instant::now(),
            })),
        );
        Ok(upload_id.to_string())
    });
    rv.set(promise_for(scope, id));
}

/// `__r2_mp_resume(bucketName, key, uploadId)`. Answers the host id when
/// the upload is one this node still holds open.
pub(super) fn op_r2_mp_resume(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let bucket_name = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let upload_id = args.get(2).to_rust_string_lossy(scope);
    let id = asyncrt::enqueue(async move {
        let parsed = upload_id.parse::<u64>().ok();
        let entry = parsed.and_then(|id| uploads().lock().unwrap().get(&id).cloned());
        let entry = match entry {
            Some(entry) => Some(entry.lock_owned().await),
            None => None,
        };
        match entry.as_deref() {
            Some(entry) if entry.bucket_name == bucket_name && entry.key == key => {
                Ok(upload_id.clone())
            }
            Some(_) => Err(format!(
                "R2 multipart upload {upload_id} was opened for another key or binding"
            )),
            // The handle the object store hands out cannot be re-derived
            // from an id, so an upload outlives only the node that opened
            // it — and only until that node restarts.
            None => Err(format!(
                "R2 multipart upload {upload_id} is not open on this node: celld can resume an \
                 upload within the node that created it, not across nodes or restarts"
            )),
        }
    });
    rv.set(promise_for(scope, id));
}

/// `__r2_mp_part(uploadId, partNumber, bytes)`. A part that arrives before
/// the one in front of it waits in memory: the object store concatenates
/// parts in the order it is given them, so the order is restored here
/// rather than left to the store. Uploading a part number again replaces
/// it while it is still waiting; once it has gone to the store the append
/// cannot be taken back, so the replacement is refused rather than
/// silently dropped.
pub(super) fn op_r2_mp_part(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let upload_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let part_number = args.get(1).integer_value(scope).unwrap_or(0).max(0) as u32;
    let bytes = view_bytes(args.get(2));
    let id = asyncrt::enqueue(async move {
        let Some(bytes) = bytes else {
            return Err(format!(
                "R2 multipart part {part_number} of upload {upload_id} must be an ArrayBuffer view"
            ));
        };
        let entry = uploads()
            .lock()
            .unwrap()
            .get(&upload_id)
            .cloned()
            .ok_or_else(|| format!("R2 multipart upload {upload_id} is not open"))?;
        let mut entry = entry.lock().await;
        entry.touched = Instant::now();
        let result = async {
            if entry.flushed.contains(&part_number) {
                return Err(format!(
                    "R2 multipart part {part_number} of upload {upload_id} was already handed to \
                     the object store, which appends parts and cannot rewrite one: celld \
                     replaces a part that is still waiting its turn, not one already written. \
                     Send the replacement before the part in front of it, or open the upload \
                     again"
                ));
            }
            // R2 replaces a part when its number is uploaded again. A part
            // still waiting its turn is replaced in place, so the bytes it
            // displaces leave the backlog with it: counting the replacement
            // without discounting the replaced would grow `held` past what
            // is actually held and refuse an upload well inside the limit.
            let replaced = entry.pending.get(&part_number).map_or(0, Vec::len);
            let held = entry.held - replaced + bytes.len();
            if held > PART_BACKLOG {
                return Err(format!(
                    "R2 multipart upload {upload_id} is holding {} bytes of parts waiting for \
                     part {}: celld hands parts to the object store in ascending order, so \
                     upload the parts in front of the ones running ahead",
                    entry.held,
                    entry.next(),
                ));
            }
            entry.held = held;
            entry.pending.insert(part_number, bytes);
            entry.drain().await
        }
        .await;
        result?;
        Ok(serde_json::json!({ "partNumber": part_number }).to_string())
    });
    rv.set(promise_for(scope, id));
}

/// `__r2_mp_complete(uploadId, partNumbersJson)`. The object the store
/// assembles is the parts in the order it received them, so the caller's
/// list decides the order of anything still held and is checked against
/// what already went.
pub(super) fn op_r2_mp_complete(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let upload_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let claimed = serde_json::from_str::<Vec<u32>>(&args.get(1).to_rust_string_lossy(scope))
        .map_err(|error| format!("invalid R2 multipart part list: {error}"));
    let id = asyncrt::enqueue(async move {
        let claimed = claimed?;
        let Some(entry) = uploads().lock().unwrap().remove(&upload_id) else {
            return Err(format!("R2 multipart upload {upload_id} is not open"));
        };
        let mut entry = entry.lock().await;
        let result = complete(&mut entry, upload_id, claimed).await;
        if result.is_err() {
            if let Err(error) = entry.upload.abort().await {
                tracing::warn!(
                    %error,
                    upload_id,
                    "mismatched R2 multipart upload could not be aborted"
                );
            }
        }
        result
    });
    rv.set(promise_for(scope, id));
}

/// The body of [`op_r2_mp_complete`], split out so a failure can abort the
/// upload rather than leave its parts on the store.
async fn complete(
    entry: &mut UploadEntry,
    upload_id: u64,
    claimed: Vec<u32>,
) -> Result<String, String> {
    if claimed.len() < entry.flushed.len() || claimed[..entry.flushed.len()] != entry.flushed {
        return Err(format!(
            "R2 multipart complete of upload {upload_id} names {claimed:?}, which does not start \
             with the parts already written in order ({:?}); celld hands parts to the object \
             store as they arrive, so a completion may add to that order but not rewrite it",
            entry.flushed
        ));
    }
    for number in &claimed[entry.flushed.len()..] {
        let Some(bytes) = entry.pending.remove(number) else {
            return Err(format!(
                "R2 multipart complete of upload {upload_id} names part {number}, which was never \
                 uploaded"
            ));
        };
        entry.held -= bytes.len();
        entry.written += bytes.len() as u64;
        entry.upload.put_part(bytes.into()).await.map_err(|error| {
            format!("R2 multipart part {number} of upload {upload_id}: {error}")
        })?;
        entry.flushed.push(*number);
    }
    let result = entry
        .upload
        .complete()
        .await
        .map_err(|error| format!("R2 multipart complete of upload {upload_id}: {error}"))?;
    let meta = BlobMeta {
        size: entry.written,
        etag: result.e_tag.clone(),
        version: result.version,
        cas: None,
        uploaded_ms: asyncrt::wall_ms(),
        attributes: entry.envelope.write(),
    };
    Ok(object_json(&entry.key, &meta, None).to_string())
}

/// `__r2_mp_abort(uploadId)`. Aborting an upload the host no longer holds
/// succeeds: the caller's intent is already the state of the world.
pub(super) fn op_r2_mp_abort(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let upload_id = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    let entry = uploads().lock().unwrap().remove(&upload_id);
    let id =
        asyncrt::enqueue(async move {
            if let Some(entry) = entry {
                entry.lock().await.upload.abort().await.map_err(|error| {
                    format!("R2 multipart abort of upload {upload_id}: {error}")
                })?;
            }
            Ok(String::new())
        });
    rv.set(promise_for(scope, id));
}
