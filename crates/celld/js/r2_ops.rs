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
//! Each op converts V8 values to Rust, calls `crate::r2_store`, and converts
//! the answer back. How an R2 object's record — `httpMetadata`,
//! `customMetadata`, `checksums`, `storageClass` — is spelled in a store
//! that has none of those concepts is decided there; see [`Envelope`]. What
//! the binding covers, and where it diverges from R2, is documented on the
//! harness side in `__makeR2Bucket`.

use super::*;
use crate::bucket::BlobConditions;
use crate::bucket::BlobMeta;
use crate::bucket::BlobRange;
use crate::bucket::BlobRead;
use object_store::MultipartUpload;
use std::collections::BTreeMap;

pub use crate::host_channels::set_r2_store;
pub(crate) use crate::r2_store::blob_key;
pub(crate) use crate::r2_store::delete;
pub(crate) use crate::r2_store::head;
pub(crate) use crate::r2_store::list;
pub(crate) use crate::r2_store::object_json;
pub(crate) use crate::r2_store::open_put;
pub(crate) use crate::r2_store::put_once;
pub(crate) use crate::r2_store::puts;
pub(crate) use crate::r2_store::read;
pub(crate) use crate::r2_store::store;
pub(crate) use crate::r2_store::Envelope;
pub(crate) use crate::r2_store::GetRequest;
pub(crate) use crate::r2_store::Open;
pub(crate) use crate::r2_store::PutRequest;

/// How long an untouched multipart upload or streaming write is kept
/// before the host abandons it. A push that streams a multi-gigabyte pack
/// part by part refreshes its entry on every part, so only an abandoned
/// one ages out.
const IDLE: Duration = Duration::from_secs(3600);

/// How far ahead of the object store a caller may run with out-of-order
/// multipart parts before the host stops holding them. Parts are handed
/// to the store in ascending order, so a part that arrives before its
/// predecessor waits in memory; this bounds that wait.
const PART_BACKLOG: usize = 256 << 20;

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
        let meta = head(&bucket_name, &key).await?;
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
        let read = read(&bucket_name, &key, range, &conditions).await?;
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
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::R2);
    let id = asyncrt::enqueue(async move {
        let keys = keys?;
        await_egress_gate(gate).await?;
        delete(&bucket_name, &keys).await?;
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
    let request_json = args.get(1).to_rust_string_lossy(scope);
    let id =
        asyncrt::enqueue(async move { Ok(list(&bucket_name, &request_json).await?.to_string()) });
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
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::R2);
    let id = asyncrt::enqueue(async move {
        let request = request?;
        let Some(body) = body else {
            return Err("R2 put: the body must be an ArrayBuffer view".to_string());
        };
        await_egress_gate(gate).await?;
        put_once(bucket_name, key, body, request).await
    });
    rv.set(promise_for(scope, id));
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
    let request_json = args.get(2).to_rust_string_lossy(scope);
    let id = asyncrt::enqueue(async move {
        let put = open_put(bucket_name, key, &request_json)?;
        reap_puts();
        let put_id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        puts()
            .lock()
            .unwrap()
            .insert(put_id, Arc::new(tokio::sync::Mutex::new(put)));
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
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::R2);
    let id = asyncrt::enqueue(async move {
        let Some(chunk) = chunk else {
            return Err("R2 put: a body chunk must be an ArrayBuffer view".to_string());
        };
        await_egress_gate(gate).await?;
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
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::R2);
    let id = asyncrt::enqueue(async move {
        // Keep the entry registered until the proof succeeds. If the gate
        // refuses the effect, the normal cleanup path can abort a live multipart
        // upload instead of dropping its handle and orphaning its parts.
        await_egress_gate(gate).await?;
        let put = puts().lock().unwrap().remove(&put_id);
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
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::R2);
    let id = asyncrt::enqueue(async move {
        let request = request?;
        await_egress_gate(gate).await?;
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
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::R2);
    let id = asyncrt::enqueue(async move {
        let Some(bytes) = bytes else {
            return Err(format!(
                "R2 multipart part {part_number} of upload {upload_id} must be an ArrayBuffer view"
            ));
        };
        await_egress_gate(gate).await?;
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
    let gate = egress_gate_request(&event_context(scope), celld_logic::Channel::R2);
    let id = asyncrt::enqueue(async move {
        let claimed = claimed?;
        // Keep the upload reachable until the proof succeeds, so a refused
        // completion leaves a handle that `reap_uploads` can abort.
        await_egress_gate(gate).await?;
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
    let gate = uploads()
        .lock()
        .unwrap()
        .contains_key(&upload_id)
        .then(|| egress_gate_request(&event_context(scope), celld_logic::Channel::R2));
    let id =
        asyncrt::enqueue(async move {
            // An absent upload changes nothing. A live upload stays in the
            // registry until the proof succeeds, so a refusal cannot orphan
            // parts by dropping its only handle.
            let Some(gate) = gate else {
                return Ok(String::new());
            };
            await_egress_gate(gate).await?;
            let entry = uploads().lock().unwrap().remove(&upload_id);
            if let Some(entry) = entry {
                entry.lock().await.upload.abort().await.map_err(|error| {
                    format!("R2 multipart abort of upload {upload_id}: {error}")
                })?;
            }
            Ok(String::new())
        });
    rv.set(promise_for(scope, id));
}
