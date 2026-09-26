// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The KV namespace's host side, shared by both engine arms: the
//! large-value path to the fleet bucket, and the limits the cell and the
//! binding read as data.
//!
//! The V8 arm reaches this through the `__kv_blob` op; the workerd arm
//! through a service binding it answers itself. The cell scope and the
//! activation epoch are the caller's authority: one activation cannot prove
//! another activation's live set, so it collects only from its own prefix.

use std::collections::HashSet;

use crate::host_channels::kv_blob_store;

pub(crate) enum BlobReply {
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

/// One request of the protocol: `{mode: prepare|get|put|sweep, ...}`, with
/// the put's bytes beside it.
pub(crate) async fn run(
    cell: &str,
    activation_epoch: u64,
    request: &str,
    value: Option<Vec<u8>>,
) -> Result<BlobReply, String> {
    use celld_logic::kv::BlobRef;
    let request: serde_json::Value =
        serde_json::from_str(request).map_err(|error| format!("invalid request: {error}"))?;
    let field = |name: &str| -> Result<String, String> {
        request
            .get(name)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| format!("request has no {name}"))
    };
    let reply = match field("mode")?.as_str() {
        "prepare" => {
            let digest = field("digest")?;
            let reference = BlobRef::v2(activation_epoch, &digest)
                .map_err(|error| error.to_string())?
                .encode();
            serde_json::json!({ "reference": reference })
        }
        "get" => {
            let reference = field("reference")?;
            let reference = BlobRef::parse(&reference).map_err(|error| error.to_string())?;
            if !reference.readable_by(activation_epoch) {
                return Err("a KV row references a later ownership epoch".to_string());
            }
            let key = reference.object_key(cell);
            match kv_blob_store()?
                .get(&key)
                .await
                .map_err(|error| error.to_string())?
            {
                Some((bytes, _etag)) => return Ok(BlobReply::Bytes(bytes.to_vec())),
                None => serde_json::json!({ "found": false }),
            }
        }
        "put" => {
            let bytes = value.ok_or_else(|| "request has no byte view".to_string())?;
            let reference = field("reference")?;
            let reference = BlobRef::parse(&reference).map_err(|error| error.to_string())?;
            if !reference.writable_by(activation_epoch) {
                return Err("a new KV blob must use the active ownership epoch".to_string());
            }
            let key = reference.object_key(cell);
            kv_blob_store()?
                .put(&key, bytes)
                .await
                .map_err(|error| error.to_string())?;
            serde_json::json!({ "ok": true })
        }
        "sweep" => {
            let values = request
                .get("live")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "request has no live blob reference list".to_string())?;
            let mut live = HashSet::new();
            for value in values {
                let reference = value.as_str().ok_or_else(|| {
                    "the live blob reference list contains a non-string".to_string()
                })?;
                let reference = BlobRef::parse(reference).map_err(|error| error.to_string())?;
                if !reference.readable_by(activation_epoch) {
                    return Err("the live blob reference list contains a later epoch".to_string());
                }
                if matches!(reference, BlobRef::V2 { .. }) {
                    live.insert(reference.encode());
                }
            }
            // Mark and sweep, not a refcount: a crash between the blob write
            // and the row commit leaves bytes no count ever counted. The
            // caller includes its pending references. Legacy blobs live
            // outside this prefix and stay, as the safe migration cost.
            let prefix = BlobRef::v2_object_prefix(cell);
            let listed = kv_blob_store()?
                .list(&prefix)
                .await
                .map_err(|error| error.to_string())?;
            // Validate the complete listing before issuing one delete, so a
            // malformed key fails closed instead of collecting partially.
            let mut doomed = Vec::new();
            for object in listed {
                let key = object.location.as_ref().to_string();
                let suffix = key.strip_prefix(&prefix).ok_or_else(|| {
                    "the KV blob listing returned a key outside its prefix".to_string()
                })?;
                let reference =
                    BlobRef::parse_object_suffix(suffix).map_err(|error| error.to_string())?;
                if reference.collectable_by(activation_epoch) && !live.contains(&reference.encode())
                {
                    doomed.push(key);
                }
            }
            let gone = kv_blob_store()?.delete_many(&doomed).await;
            if gone.len() != doomed.len() {
                return Err(format!(
                    "{} blob(s) refused deletion",
                    doomed.len().saturating_sub(gone.len())
                ));
            }
            serde_json::json!({ "removed": gone.len() })
        }
        other => return Err(format!("unknown mode {other}")),
    };
    Ok(BlobReply::Json(reply))
}

/// Every bound `celld_logic::kv` declares, as data, plus the test knobs.
///
/// One variable for every KV test knob, not one variable each:
///
///   CELLD_TEST_KV=no-sweep,fail-after-blob,min-ttl-ms=1000
///
/// These are test-only and read the production environment rather than
/// sitting behind `cfg(celld_internal_tests)`, because the code they steer
/// is JavaScript and a cfg cannot reach it. `min-ttl-ms` shortens the
/// sixty-second expiry floor; `no-sweep` stops reclamation so a test can pin
/// the read filter; `fail-after-blob` fails a put between the blob write and
/// the row commit; `race-sweep-put` holds a sweep after its mark snapshot
/// until a new large put starts; `blob-sweep-ms` shortens the collector
/// delay; `legacy-schema` creates the first release's inline-only table.
pub(crate) fn limits() -> serde_json::Value {
    use celld_logic::kv;
    let knobs = std::env::var("CELLD_TEST_KV").unwrap_or_default();
    let knob = |name: &str| -> Option<String> {
        knobs.split(',').map(str::trim).find_map(|entry| {
            let rest = entry.strip_prefix(name)?;
            match rest {
                "" => Some(String::new()),
                rest => rest.strip_prefix('=').map(str::to_string),
            }
        })
    };
    let ms = |name: &str, default: i64| {
        knob(name)
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|ms| *ms > 0)
            .unwrap_or(default)
    };
    serde_json::json!({
        "maxKeyBytes": kv::MAX_KEY_BYTES,
        "maxValueBytes": kv::MAX_VALUE_BYTES,
        "maxInlineValueBytes": kv::MAX_INLINE_VALUE_BYTES,
        "maxMetadataBytes": kv::MAX_METADATA_BYTES,
        "maxBulkKeys": kv::MAX_BULK_KEYS,
        "maxListLimit": kv::MAX_LIST_LIMIT,
        "sweepBatchRows": celld_logic::sweep::BATCH_ROWS,
        "minExpirationTtlMs": ms("min-ttl-ms", kv::MIN_EXPIRATION_TTL_MS),
        "blobSweepMs": ms("blob-sweep-ms", 60_000),
        "sweepDisabled": knob("no-sweep").is_some(),
        "failAfterBlobWrite": knob("fail-after-blob").is_some(),
        "raceSweepPut": knob("race-sweep-put").is_some(),
        "legacySchema": knob("legacy-schema").is_some(),
    })
}
