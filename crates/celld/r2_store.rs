// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The object-store side of an `r2_buckets` binding: the R2 object record,
//! the request shapes, and the reads and writes that `celld r2` and the cell
//! runtime share. Every engine reaches the fleet bucket through this module,
//! so an operator and a Worker cannot disagree about the key an object lives
//! under or the record it carries. The V8 ops live in `js/r2_ops.rs`.

use crate::asyncrt;
use crate::bucket::BlobAttributes;
use crate::bucket::BlobConditions;
use crate::bucket::BlobMeta;
use crate::bucket::BlobRange;
use crate::bucket::BlobRead;
use crate::bucket::Bucket;
use crate::host_channels::R2_STORE;
use futures_util::StreamExt as _;
use object_store::MultipartUpload;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;

/// A page of an R2 listing. R2's own default and maximum is 1,000.
const LIST_LIMIT: usize = 1000;

/// How many of a listing page's heads are in flight at once when the
/// caller asked for `include`. A page can hold a thousand objects, and
/// reading their metadata one round trip after another would make the
/// option unusable.
const LIST_HEADS: usize = 16;

/// The part size a streaming `put` cuts at once it is too big to write in
/// one request. Above S3's 5 MiB floor, and a round number of pages.
const STREAM_PART: usize = 8 << 20;

/// The user-metadata name the R2 object record lives under. See
/// [`Envelope`]. The bucket stores it as `celld_r2` on Azure, which
/// refuses a hyphen in a metadata name.
const ENVELOPE: &str = "celld-r2";

/// The key space one binding owns inside the fleet bucket. `bucket_name`
/// comes from the deployment manifest, which validates it, so the prefix
/// cannot escape into the fleet's own keys.
pub(crate) fn blob_key(bucket_name: &str, key: &str) -> String {
    format!("r2/{bucket_name}/{key}")
}

/// The fleet bucket, or the error every op answers without one.
pub(crate) fn store() -> Result<&'static Bucket, String> {
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
pub(crate) struct HttpMeta {
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
pub(crate) struct Envelope {
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) custom: BTreeMap<String, String>,
    pub(crate) http: HttpMeta,
    /// Lowercase algorithm name (`md5`, `sha1`, `sha256`, `sha384`,
    /// `sha512`) to lowercase hex.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) checksums: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) storage_class: Option<String>,
}

impl Envelope {
    /// The record on an object the store answered. The five content
    /// headers come off the response, because they are the object's own
    /// headers and stay right even for an object written by another tool.
    fn read(attributes: &BlobAttributes) -> Self {
        let mut envelope = attributes
            .metadata_value(ENVELOPE)
            .and_then(|value| serde_json::from_str::<Self>(value).ok())
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
    pub(crate) fn write(&self) -> BlobAttributes {
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
pub(crate) fn object_json(
    key: &str,
    meta: &BlobMeta,
    range: Option<(u64, u64)>,
) -> serde_json::Value {
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
pub(crate) struct Conditional {
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
pub(crate) struct Range {
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
pub(crate) struct GetRequest {
    pub(crate) range: Option<Range>,
    pub(crate) only_if: Option<Conditional>,
}

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct PutRequest {
    pub(crate) http: HttpMeta,
    pub(crate) custom: BTreeMap<String, String>,
    pub(crate) storage_class: Option<String>,
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

/// One object's record, or `None` when the key does not exist.
///
/// Shared with `celld r2 head`: an operator and a Worker must not be able
/// to disagree about which key an object lives under, and the answer is
/// only right because both sides scope the key the same way.
pub(crate) async fn head(bucket_name: &str, key: &str) -> Result<Option<BlobMeta>, String> {
    store()?
        .head_blob(&blob_key(bucket_name, key))
        .await
        .map_err(|error| error.to_string())
}

/// One conditional read, with the body still on the wire.
///
/// Shared with `celld r2 get`, which drains the body to stdout instead of
/// handing it to an isolate.
pub(crate) async fn read(
    bucket_name: &str,
    key: &str,
    range: BlobRange,
    conditions: &BlobConditions,
) -> Result<BlobRead, String> {
    store()?
        .get_blob(&blob_key(bucket_name, key), range, conditions)
        .await
        .map_err(|error| error.to_string())
}

/// Remove every named key. Shared with `celld r2 delete`.
pub(crate) async fn delete(bucket_name: &str, keys: &[String]) -> Result<(), String> {
    let store = store()?;
    let keys = keys
        .iter()
        .map(|key| blob_key(bucket_name, key))
        .collect::<Vec<_>>();
    // `delete_blobs` reports what went; anything left is a failure the
    // caller must see, because R2's delete either applies or throws.
    let gone = store.delete_blobs(&keys).await;
    if gone != keys.len() {
        return Err(format!(
            "R2 delete removed {gone} of {} keys; the rest failed",
            keys.len()
        ));
    }
    Ok(())
}

/// One listing page, in the shape `__makeR2Bucket` turns into an
/// `R2Objects`. `request_json` is R2's `R2ListOptions`, normalized by the
/// harness.
///
/// Shared with `celld r2 list`, so an operator's listing applies the same
/// prefix scoping, the same cursor rule, and the same page bound as a
/// Worker's.
pub(crate) async fn list(
    bucket_name: &str,
    request_json: &str,
) -> Result<serde_json::Value, String> {
    let request = serde_json::from_str::<ListRequest>(request_json)
        .map_err(|error| format!("invalid R2 list options: {error}"))?;
    let store = store()?;
    let limit = match request.limit.unwrap_or(0) {
        limit if limit > 0 => (limit as usize).min(LIST_LIMIT),
        _ => LIST_LIMIT,
    };
    let scoped = blob_key(bucket_name, &request.prefix);
    // R2 resumes from the cursor when it has one and ignores
    // `startAfter`, which is the same knob for a first page.
    let after = request
        .cursor
        .filter(|cursor| !cursor.is_empty())
        .or(request.start_after)
        .filter(|after| !after.is_empty())
        .map(|after| blob_key(bucket_name, &after));
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
    let strip = blob_key(bucket_name, "");
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
    }))
}

/// Write `body` under `key`, honoring the request's conditions and
/// checksums. The single-request path: a caller that handed R2 a buffer
/// gets one PUT, and one md5 R2 would also have computed.
pub(crate) async fn put_once(
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

/// A `put` whose body is a `ReadableStream`: the isolate hands over one
/// chunk at a time and the host decides, at the first part boundary,
/// whether this is one request or a multipart upload.
pub(crate) struct PutStream {
    bucket_name: String,
    key: String,
    request: PutRequest,
    digests: Digests,
    /// Bytes not yet handed to the store.
    buffered: Vec<u8>,
    size: u64,
    /// Open once the body outgrew a single request.
    pub(crate) upload: Option<Box<dyn MultipartUpload>>,
    pub(crate) touched: Instant,
}

/// Open a streaming write. `request_json` is R2's `R2PutOptions`,
/// normalized by the harness.
///
/// This is the one constructor, so `celld r2 put` and a Worker's
/// `env.BUCKET.put()` cannot store a different record for the same input:
/// the checksum set, the envelope, and the single-request/multipart choice
/// are all decided here and in [`PutStream::finish`].
// The idle stamp only ages an abandoned write out; it never orders host
// work, so it stays on the ambient clock outside the execution domain.
// `allow`, not `expect`: Linux clippy does not report this call, so an
// expectation that macOS fulfills fails the public release build.
#[allow(
    clippy::disallowed_methods,
    reason = "the idle stamp ages an abandoned write out and orders no host work"
)]
pub(crate) fn open_put(
    bucket_name: String,
    key: String,
    request_json: &str,
) -> Result<PutStream, String> {
    let request = serde_json::from_str::<PutRequest>(request_json)
        .map_err(|error| format!("invalid R2 put options: {error}"))?;
    // Fail before a byte moves if there is no bucket at all.
    store()?;
    let digests = Digests::wanted(&request.verify, true);
    Ok(PutStream {
        bucket_name,
        key,
        request,
        digests,
        buffered: Vec::new(),
        size: 0,
        upload: None,
        touched: Instant::now(),
    })
}

impl PutStream {
    /// Take in one chunk, handing the store every whole part it makes.
    pub(crate) async fn push(&mut self, chunk: Vec<u8>) -> Result<(), String> {
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
    pub(crate) async fn finish(&mut self) -> Result<String, String> {
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
pub(crate) type Open<T> = std::sync::Mutex<HashMap<u64, Arc<tokio::sync::Mutex<T>>>>;

static PUTS: OnceLock<Open<PutStream>> = OnceLock::new();

pub(crate) fn puts() -> &'static Open<PutStream> {
    PUTS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}
