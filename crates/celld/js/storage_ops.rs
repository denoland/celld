// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The V8 surface over `crate::storage`: key-value, SQL, and the value
//! encoding shared by both.
//!
//! Nothing here decides anything. Each op converts V8 values to Rust,
//! calls into `storage`, and converts the answer back — so the storage
//! semantics live in that module and the serialization format lives here,
//! because it is what JS can see.
use super::*;

pub(super) fn op_storage_get(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let scope_s = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    match storage::get_stored(&scope_s, &key) {
        Ok(Some(value)) => {
            if let Some((value, _)) = deserialize_stored(scope, value, args.get(2)) {
                rv.set(value);
            }
        }
        Ok(None) => rv.set(v8::undefined(scope).into()),
        Err(error) => throw_storage_error(scope, "get", error),
    }
}
pub(super) fn op_storage_get_many(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let keys = match storage_string_array(scope, args.get(1)) {
        Ok(keys) => keys,
        Err(error) => {
            throw_storage_error(scope, "get", error);
            return;
        }
    };
    match storage::get_many_stored(&cell, &keys) {
        Ok(entries) => {
            let sentinel = args.get(2);
            if let Some((map, tagged)) = storage_entries_map(scope, entries, sentinel) {
                rv.set(if tagged {
                    wrap_stored(scope, sentinel, map.into())
                } else {
                    map.into()
                });
            }
        }
        Err(error) => throw_storage_error(scope, "get", error),
    }
}
pub(super) fn op_storage_put(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let s = args.get(0).to_rust_string_lossy(scope);
    let k = args.get(1).to_rust_string_lossy(scope);
    let Some(value) = serialize_storage_value(scope, args.get(2)) else {
        return;
    };
    if let Err(error) = storage::put_serialized(&s, &k, &value) {
        throw_storage_error(scope, "put", error);
    }
}
pub(super) fn op_storage_put_many(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let Ok(entries) = v8::Local::<v8::Array>::try_from(args.get(1)) else {
        throw_storage_error(scope, "put", "entries must be an array");
        return;
    };
    let mut serialized = Vec::with_capacity(entries.length() as usize);
    for index in 0..entries.length() {
        let Some(entry) = entries.get_index(scope, index) else {
            return;
        };
        let Ok(entry) = v8::Local::<v8::Array>::try_from(entry) else {
            throw_storage_error(scope, "put", "entry must be a key/value pair");
            return;
        };
        let Some(key) = entry.get_index(scope, 0) else {
            return;
        };
        let Some(value) = entry.get_index(scope, 1) else {
            return;
        };
        let Some(value) = serialize_storage_value(scope, value) else {
            return;
        };
        serialized.push((key.to_rust_string_lossy(scope), value));
    }
    if let Err(error) = storage::put_many_serialized(&cell, &serialized) {
        throw_storage_error(scope, "put", error);
    }
}

/// `__storage_put_serialized(cell, key, bytes)`: write pre-encoded row
/// bytes verbatim — the stored-stub envelope (or a plain clone) built by
/// the JS storage wrapper after a plain clone failed. Never on the fast
/// path.
pub(super) fn op_storage_put_serialized(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let Some(bytes) = view_bytes(args.get(2)) else {
        throw_storage_error(scope, "put", "serialized row must be bytes");
        return;
    };
    if let Err(error) = storage::put_serialized(&cell, &key, &bytes) {
        throw_storage_error(scope, "put", error);
    }
}

/// The queued flavor of [`op_storage_put_serialized`], joining the same
/// pending-puts batch the plain queue ops feed.
pub(super) fn op_storage_queue_put_serialized(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let Some(bytes) = view_bytes(args.get(2)) else {
        throw_storage_error(scope, "put", "serialized row must be bytes");
        return;
    };
    queue_serialized_puts(scope, cell, vec![(key, bytes)]);
}

pub(super) fn actor_runtime_state(scope: &mut v8::PinScope) -> Arc<ActorRuntimeState> {
    scope
        .get_slot::<Arc<ActorRuntimeState>>()
        .expect("actor runtime state slot")
        .clone()
}

fn queue_serialized_puts(scope: &mut v8::PinScope, cell: String, entries: Vec<(String, Vec<u8>)>) {
    actor_runtime_state(scope)
        .pending_puts
        .lock()
        .expect("pending puts lock poisoned")
        .entry(cell)
        .or_default()
        .extend(entries);
}

pub(super) fn op_storage_queue_put(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let key = args.get(1).to_rust_string_lossy(scope);
    let Some(value) = serialize_storage_value(scope, args.get(2)) else {
        return;
    };
    queue_serialized_puts(scope, cell, vec![(key, value)]);
}

pub(super) fn op_storage_queue_put_many(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let Ok(entries) = v8::Local::<v8::Array>::try_from(args.get(1)) else {
        throw_storage_error(scope, "put", "entries must be an array");
        return;
    };
    let mut serialized = Vec::with_capacity(entries.length() as usize);
    for index in 0..entries.length() {
        let Some(entry) = entries.get_index(scope, index) else {
            return;
        };
        let Ok(entry) = v8::Local::<v8::Array>::try_from(entry) else {
            throw_storage_error(scope, "put", "entry must be a key/value pair");
            return;
        };
        let Some(key) = entry.get_index(scope, 0) else {
            return;
        };
        let Some(value) = entry.get_index(scope, 1) else {
            return;
        };
        let Some(value) = serialize_storage_value(scope, value) else {
            return;
        };
        serialized.push((key.to_rust_string_lossy(scope), value));
    }
    queue_serialized_puts(scope, cell, serialized);
}

pub(super) fn op_storage_flush_pending_puts(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    if let Err(error) = flush_pending_puts(scope, &cell) {
        throw_storage_error(scope, "put", error);
    }
}

fn flush_pending_puts(scope: &mut v8::PinScope, cell: &str) -> Result<(), String> {
    let entries = actor_runtime_state(scope)
        .pending_puts
        .lock()
        .expect("pending puts lock poisoned")
        .remove(cell)
        .unwrap_or_default();
    if entries.is_empty() {
        return Ok(());
    }
    storage::put_many_serialized(cell, &entries).map_err(|error| error.to_string())
}

/// `storage.sync()`: resolve once every write the cell committed before the
/// call is proven durable, by the same proof the output gate accepts before
/// it releases an egress.
///
/// The promise takes an ordinary gate ticket on its own channel rather than
/// calling the replicator: the core checks that the proven position covers
/// the requested one and, for a bucket proof, verifies ownership before it
/// acks, and a direct replicator call would skip both. With
/// `CELLD_OUTPUT_GATE=0` the shell releases every ticket without a proof, so
/// the promise resolves after the local commit: the operator opted out of
/// proofs for the whole process, and `sync()` follows the same rule as the
/// response.
///
/// The ticket is always a write ticket at the cell's current committed
/// position, whether or not this event wrote. A read-only ticket trails the
/// newest barrier open on the cell, and a commit opens a barrier only when
/// an output of its own event takes a ticket; an event that committed and
/// then awaited other work has none open yet, so a read-only ticket from a
/// concurrent event would release at once with that commit unproven. The
/// contract is "every write the cell committed before the call", so the
/// position decides, not the caller's share of it. A cell with nothing new
/// pays one local capture pass and uploads nothing.
///
/// Every storage sample happens in this turn. The completion runs on the host
/// loop after the turn ends, and cell storage must not be reached from there
/// (denoland/celld#170).
pub(super) fn op_storage_sync(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    // A promise about durability fails closed. Without a gate channel no
    // ticket can be answered, and without a running cell event nothing says
    // which cell's writes this turn made. An egress in either state leaves
    // ungated because it reveals no cell, but a durability statement with no
    // proof behind it is not one this engine can make. The channel check
    // repeats the verdict's `NoChannel` refusal on purpose: it runs before
    // any storage is read, so a process without a gate never turns `sync()`
    // into a silent local-only pass by a later refactor of the verdict.
    if GATE_TX.get().is_none() {
        throw_storage_error(scope, "sync", "this process has no output-gate channel");
        return;
    }
    let context = event_context(scope);
    let Some(frame) = current_cell_event(&context) else {
        throw_storage_error(scope, "sync", format!("{cell} has no running cell event"));
        return;
    };
    let event_cell = frame.storage;
    // The ticket names the cell of the running event, never the scope the
    // caller passed. They can only differ when JavaScript holds another
    // cell's storage handle, and then the proof would be for the wrong cell.
    // Checked before the sample below, so the op never reads another cell's
    // connection state and never reports that state under this scope's name.
    if event_cell != cell {
        throw_storage_error(
            scope,
            "sync",
            format!("{cell} is not the running event's cell"),
        );
        return;
    }
    let Some(sample) = storage::sync_sample(&cell) else {
        throw_storage_error(scope, "sync", format!("{cell} is not resident"));
        return;
    };
    // An open transaction advances the write position before it commits,
    // and the replicator ships only committed frames. A ticket taken here
    // would be released by a proof that cannot include the transaction, so
    // the promise would settle as a statement about writes the proof does
    // not cover. It rejects instead. The flag is the connection's, so this
    // also rejects a caller that another event's open `transaction()` let
    // in; that caller's own writes joined that transaction and are just as
    // uncommitted, and a wait for the commit would still be a false
    // statement if the transaction rolled back.
    if sample.in_transaction {
        throw_storage_error(
            scope,
            "sync",
            format!(
                "{cell} has an open transaction; its writes are not committed, so no \
                 durability proof can cover them. Call sync() after the transaction commits"
            ),
        );
        return;
    }
    // A facet's `sync()` is a statement about the root cell's database, which
    // is where its writes live. It names that cell and carries that cell's
    // sample, because a ticket in the facet's name would activate a cell no
    // Worker exports. The image is copied first, so the proof the ticket waits
    // for is a proof of a database that contains this facet's writes.
    let gate = match frame.root {
        Some(root) => {
            storage::flush_embedded(&cell);
            match storage::sql_critical_error(&cell) {
                Some(error) => EgressGate::Unpersisted(error),
                None => EgressGate::Wrote(
                    root.cell,
                    celld_logic::Channel::Sync,
                    root.position,
                    Some(root.epoch),
                ),
            }
        }
        None => EgressGate::Wrote(
            cell.clone(),
            celld_logic::Channel::Sync,
            sample.position,
            Some(sample.epoch),
        ),
    };
    let id = asyncrt::enqueue(async move {
        egress_gate_verdict(gate)
            .await
            .map_err(|refusal| format!("storage.sync: {cell}: {refusal}"))?;
        Ok(String::new())
    });
    rv.set(promise_for(scope, id));
}

pub(super) fn op_storage_cancel_pending_puts(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    actor_runtime_state(scope)
        .pending_puts
        .lock()
        .expect("pending puts lock poisoned")
        .remove(&cell);
}

pub(super) fn op_sql_ingest(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let input = args.get(1).to_rust_string_lossy(scope);
    let out = match storage::sql_ingest(&cell, &input) {
        Ok((remainder, rows_written, statement_count)) => serde_json::json!({
            "remainder": remainder,
            "rowsWritten": rows_written,
            "statementCount": statement_count,
        }),
        Err(error) => serde_json::json!({ "error": error }),
    };
    rv.set(v8::String::new(scope, &out.to_string()).unwrap().into());
}
/// `__sql_cursor_start(cell, query, bindsJson)` — open a native SQL cursor and
/// hand back its first row.
///
/// Returns a `v8::Object` with `cursorId`, `columns`, `row`, `rowsWritten` and
/// `reusedCachedQuery`. A failure throws.
///
/// An object and not a fixed-position array, because this op answers once per
/// result set rather than once per row, so the per-call cost of five string
/// keys is paid once and buys a named payload that `SqlCursor` reads field by
/// field. `op_sql_cursor_next` runs per row and therefore tells its two
/// answers apart by type instead; there is only one answer shape here, so
/// there is nothing to tell apart.
///
/// `row` and every value inside it are V8 values, as in `op_sql_cursor_next`.
/// One result set is therefore carried by one encoding, so row 1 and row 2 of
/// the same `SELECT` cannot disagree about a value.
pub(super) fn op_sql_cursor_start(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let query = args.get(1).to_rust_string_lossy(scope);
    let timing_started = (tracing::enabled!(target: "queue_timing", tracing::Level::DEBUG)
        && cell
            .split_once(':')
            .is_some_and(|(class, _)| class == crate::deploy::QUEUE_CLASS))
    .then(crate::asyncrt::mono_us);
    // An undecodable bind payload used to become an empty bind list, and the
    // query ran anyway. `sql_cursor_start` executes every parameter-free
    // prefix statement before it compares the final statement's parameter
    // count against the binds, so an empty default committed a prefix write
    // and then reported a parameter-count mismatch that named nothing the
    // caller had done. Refuse the call before SQLite sees any of it.
    let binds: Vec<serde_json::Value> =
        match serde_json::from_str(&args.get(2).to_rust_string_lossy(scope)) {
            Ok(binds) => binds,
            Err(error) => {
                return loader_throw(
                    scope,
                    &format!("sql: the bind payload is not a JSON array: {error}"),
                )
            }
        };
    let result = storage::sql_cursor_start(&cell, &query, &binds);
    if let Some(timing_started) = timing_started {
        let statement = query
            .split_ascii_whitespace()
            .next()
            .unwrap_or("empty")
            .to_ascii_lowercase();
        tracing::debug!(
            target: "queue_timing",
            event = "queue_sql_timing",
            statement,
            total_us = crate::asyncrt::mono_us().saturating_sub(timing_started),
            ok = result.is_ok(),
            "Queue SQL statement completed"
        );
    }
    match result {
        Ok((cursor, columns, row, rows_written, reused_cached_query)) => {
            let result = v8::Object::new(scope);
            let set = |scope: &mut v8::PinScope, name: &str, value: v8::Local<v8::Value>| {
                let key = v8::String::new(scope, name).unwrap();
                result.set(scope, key.into(), value);
            };
            let cursor_id = v8::Number::new(scope, cursor as f64).into();
            set(scope, "cursorId", cursor_id);
            let names = v8::Array::new(scope, columns.len() as i32);
            for (index, name) in columns.iter().enumerate() {
                let name = v8::String::new(scope, name).unwrap();
                names.set_index(scope, index as u32, name.into());
            }
            set(scope, "columns", names.into());
            let first: v8::Local<v8::Value> = match row {
                Some(row) => sql_row_to_v8(scope, row).into(),
                None => v8::null(scope).into(),
            };
            set(scope, "row", first);
            let written = v8::Number::new(scope, rows_written as f64).into();
            set(scope, "rowsWritten", written);
            let reused = v8::Boolean::new(scope, reused_cached_query).into();
            set(scope, "reusedCachedQuery", reused);
            rv.set(result.into());
        }
        // `SQL error: ` and not `storage.`, because that is the prefix a Worker
        // already sees from a failing `exec()`. The message stays byte for
        // byte what `SqlCursor`'s constructor used to build from the in-band
        // `{"error": ...}` field, following 229da324.
        Err(error) => throw_sql_error(scope, error),
    }
}
/// `__sql_cursor_next(cursorId)` — one step of a native SQL cursor.
///
/// Returns a `v8::Array` of column values for a row, and a `v8::Number` with
/// the cursor's final `rowsWritten` once SQLite reports DONE. The two answers
/// are told apart by type, as `op_storage_sync_list_next` tells a pair from
/// null. A failure throws.
///
/// This op used to serialise the row to JSON and let JS `JSON.parse` it back,
/// once per row. A 100k-row `SELECT` therefore paid 100k serialise/parse
/// round-trips, and a BLOB column paid about four text bytes per data byte,
/// because JSON carries bytes as an array of decimal numbers. Its `start`
/// sibling above carries row 1 through the same converter.
pub(super) fn op_sql_cursor_next(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cursor = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    match storage::sql_cursor_next(cursor) {
        Ok((Some(row), _)) => rv.set(sql_row_to_v8(scope, row).into()),
        Ok((None, rows_written)) => rv.set(v8::Number::new(scope, rows_written as f64).into()),
        Err(error) => throw_sql_error(scope, error),
    }
}

/// A SQL failure as the Worker sees it. Both cursor ops raise through here, so
/// the two halves of one result set cannot report the same fault differently.
///
/// The prefix is `SQL error: ` and not `storage.`, because that is the prefix
/// `SqlCursor` built in JS from the in-band `{"error": ...}` field that both
/// ops used to answer with, and a Worker already matches on it.
fn throw_sql_error(scope: &mut v8::PinScope, error: impl std::fmt::Display) {
    let message = v8::String::new(scope, &format!("SQL error: {error}")).unwrap();
    let exception = v8::Exception::error(scope, message);
    scope.throw_exception(exception);
}

/// One SQL result row as a `v8::Array` of column values.
fn sql_row_to_v8<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    row: Vec<storage::SqlValue>,
) -> v8::Local<'s, v8::Array> {
    let array = v8::Array::new(scope, row.len() as i32);
    for (index, value) in row.into_iter().enumerate() {
        let value = sql_value_to_v8(scope, value);
        array.set_index(scope, index as u32, value);
    }
    array
}

/// One SQL column value as a V8 value, matching what `JSON.parse` plus the
/// removed `_decode` closure in `harness.js` used to produce, except for a
/// non-finite REAL, which JSON could not carry at all.
fn sql_value_to_v8<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: storage::SqlValue,
) -> v8::Local<'s, v8::Value> {
    match value {
        storage::SqlValue::Null => v8::null(scope).into(),
        // A SQLite integer is an i64 and a JS number is a double, so an
        // integer above 2^53 loses its low bits. `JSON.parse` rounded the
        // decimal literal the same way, so this keeps the old value exactly
        // rather than introducing a new loss. A Smi is used where it fits.
        storage::SqlValue::Integer(i) => match i32::try_from(i) {
            Ok(small) => v8::Integer::new(scope, small).into(),
            Err(_) => v8::Number::new(scope, i as f64).into(),
        },
        // A REAL crosses as a double, including a non-finite one: SQLite reads
        // `9e999` as REAL infinity, and a JS number holds infinity.
        // The JSON path had no such literal and answered null, which lost a
        // value SQLite had stored. Both cursor ops read this arm, so row 1 and
        // row 2 of one result set report the same value.
        storage::SqlValue::Real(f) => v8::Number::new(scope, f).into(),
        storage::SqlValue::Text(text) => v8::String::new(scope, &text).unwrap().into(),
        // An ArrayBuffer, because that is what the `_decode` closure produced
        // from `{__celld_bytes: [...]}`. The bytes move into the backing store
        // without a copy, as `bytes_value` in `js.rs` does.
        storage::SqlValue::Blob(bytes) => {
            if bytes.is_empty() {
                return v8::ArrayBuffer::new(scope, 0).into();
            }
            let store = v8::ArrayBuffer::new_backing_store_from_vec(bytes).make_shared();
            v8::ArrayBuffer::with_backing_store(scope, &store).into()
        }
    }
}
pub(super) fn op_sql_cursor_close(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cursor = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    storage::sql_cursor_close(cursor);
}
pub(super) fn op_sql_database_size(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    match storage::sql_database_size(&cell) {
        Ok(bytes) => rv.set(v8::Number::new(scope, bytes as f64).into()),
        Err(error) => throw_storage_error(scope, "sql.databaseSize", error),
    }
}
/// `__d1_run(cell, request)` — the D1 adapter's one op. The request and the
/// reply are the typed structures in `storage::d1_run`; nothing D1 crosses
/// this boundary through the general SQL ops, so the D1 contract cannot
/// drift apart from itself one op at a time.
pub(super) fn op_d1_run(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let request = args.get(1).to_rust_string_lossy(scope);
    let out = storage::d1_run_json(&cell, &request);
    rv.set(v8::String::new(scope, &out).unwrap().into());
}
#[cfg(celld_internal_tests)]
pub(super) fn op_sql_set_max_page_count_for_test(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let pages = args.get(1).uint32_value(scope).unwrap_or(0);
    if let Err(error) = storage::set_max_page_count_for_test(&cell, pages) {
        throw_storage_error(scope, "sql.setMaxPageCountForTest", error);
    }
}
#[cfg(celld_internal_tests)]
pub(super) fn op_sql_set_write_fault_for_test(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    storage::set_write_fault_for_test(args.get(0).boolean_value(scope));
}
#[cfg(celld_internal_tests)]
pub(super) fn op_sql_set_cache_size_for_test(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let pages = args.get(1).int32_value(scope).unwrap_or(0);
    if let Err(error) = storage::set_cache_size_for_test(&cell, pages) {
        throw_storage_error(scope, "sql.setCacheSizeForTest", error);
    }
}
#[cfg(celld_internal_tests)]
pub(super) fn op_sql_set_interrupt_fault_for_test(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let enabled = args.get(1).boolean_value(scope);
    if let Err(error) = storage::set_interrupt_fault_for_test(&cell, enabled) {
        throw_storage_error(scope, "sql.setInterruptFaultForTest", error);
    }
}
#[cfg(celld_internal_tests)]
pub(super) fn op_sql_register_nomem_function_for_test(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    if let Err(error) = storage::register_nomem_function_for_test(&cell) {
        throw_storage_error(scope, "sql.registerNomemFunctionForTest", error);
    }
}
pub(super) fn op_storage_transaction_control(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let action = args.get(1).to_rust_string_lossy(scope);
    let nested = args.get(2).boolean_value(scope);
    let savepoint = args.get(3).to_rust_string_lossy(scope);
    let timing_started = (tracing::enabled!(target: "queue_timing", tracing::Level::DEBUG)
        && cell
            .split_once(':')
            .is_some_and(|(class, _)| class == crate::deploy::QUEUE_CLASS))
    .then(crate::asyncrt::mono_us);
    // A put queues before its first await. Move that queue into the current
    // SQL scope before every boundary: before BEGIN/SAVEPOINT it belongs to
    // the parent, and before COMMIT/ROLLBACK it belongs to the ending scope.
    // Clearing the cell queue on rollback would also discard parent writes;
    // flushing after rollback would commit the canceled writes outside it.
    let result = flush_pending_puts(scope, &cell)
        .and_then(|()| storage::transaction_control(&cell, &action, nested, &savepoint));
    if let Some(timing_started) = timing_started {
        tracing::debug!(
            target: "queue_timing",
            event = "queue_transaction_timing",
            action,
            nested,
            total_us = crate::asyncrt::mono_us().saturating_sub(timing_started),
            ok = result.is_ok(),
            "Queue SQL transaction control completed"
        );
    }
    match result {
        Err(error) => throw_storage_error(scope, "transaction", error),
        // An outermost commit published a dirty alarm: register its
        // wake-entry PUT against the current event's output gate.
        Ok(Some(at)) if at >= 0 => spawn_arm_gate(&cell, at, current_reaction_io_context(scope)),
        Ok(_) => {}
    }
}
pub(super) fn op_storage_delete(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let s = args.get(0).to_rust_string_lossy(scope);
    let k = args.get(1).to_rust_string_lossy(scope);
    match storage::delete(&s, &k) {
        Ok(deleted) => rv.set(v8::Boolean::new(scope, deleted).into()),
        Err(error) => throw_storage_error(scope, "delete", error),
    }
}
pub(super) fn op_storage_delete_many(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let keys = match storage_string_array(scope, args.get(1)) {
        Ok(keys) => keys,
        Err(error) => {
            throw_storage_error(scope, "delete", error);
            return;
        }
    };
    match storage::delete_many(&cell, &keys) {
        Ok(deleted) => rv.set(v8::Number::new(scope, deleted as f64).into()),
        Err(error) => throw_storage_error(scope, "delete", error),
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct StorageListOptions {
    start: Option<String>,
    end: Option<String>,
    start_after: Option<String>,
    prefix: Option<String>,
    limit: Option<usize>,
    reverse: bool,
}

pub(super) fn op_storage_list(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let options = match serde_json::from_str::<StorageListOptions>(
        &args.get(1).to_rust_string_lossy(scope),
    ) {
        Ok(options) => options,
        Err(error) => {
            throw_storage_error(scope, "list", error);
            return;
        }
    };
    match storage::list_stored_with_options(
        &cell,
        options.start.as_deref(),
        options.end.as_deref(),
        options.start_after.as_deref(),
        options.prefix.as_deref(),
        options.limit,
        options.reverse,
    ) {
        Ok(entries) => {
            let sentinel = args.get(2);
            if let Some((map, tagged)) = storage_entries_map(scope, entries, sentinel) {
                rv.set(if tagged {
                    wrap_stored(scope, sentinel, map.into())
                } else {
                    map.into()
                });
            }
        }
        Err(error) => throw_storage_error(scope, "list", error),
    }
}

pub(super) fn op_storage_sync_list_start(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let options = match serde_json::from_str::<StorageListOptions>(
        &args.get(1).to_rust_string_lossy(scope),
    ) {
        Ok(options) => options,
        Err(error) => {
            throw_storage_error(scope, "kv.list", error);
            return;
        }
    };
    match storage::sync_list_start(
        &cell,
        options.start.as_deref(),
        options.end.as_deref(),
        options.start_after.as_deref(),
        options.prefix.as_deref(),
        options.limit,
        options.reverse,
    ) {
        Ok(cursor) => rv.set(v8::Number::new(scope, cursor as f64).into()),
        Err(error) => throw_storage_error(scope, "kv.list", error),
    }
}

pub(super) fn op_storage_sync_list_next(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cursor = args.get(0).integer_value(scope).unwrap_or(0).max(0) as u64;
    match storage::sync_list_next(cursor) {
        Ok(Some((key, value))) => {
            let Some((decoded, _)) = deserialize_stored(scope, value, args.get(1)) else {
                throw_storage_error(
                    scope,
                    "kv.list",
                    format!("deserialization failed for key {key}"),
                );
                return;
            };
            let pair = v8::Array::new(scope, 2);
            let key = v8::String::new(scope, &key).unwrap();
            pair.set_index(scope, 0, key.into());
            pair.set_index(scope, 1, decoded);
            rv.set(pair.into());
        }
        Ok(None) => rv.set(v8::null(scope).into()),
        Err(error) => throw_storage_error(scope, "kv.list", error),
    }
}

struct StorageValueDelegate;

impl v8::ValueSerializerImpl for StorageValueDelegate {
    fn throw_data_clone_error(&self, scope: &mut v8::PinScope, message: v8::Local<v8::String>) {
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
    }
}

impl v8::ValueDeserializerImpl for StorageValueDelegate {}

#[derive(Clone, Default)]
struct TransientCloneDelegate {
    wasm_modules: std::rc::Rc<RefCell<Vec<v8::CompiledWasmModule>>>,
    shared_buffers: std::rc::Rc<RefCell<Vec<v8::SharedRef<v8::BackingStore>>>>,
}

impl v8::ValueSerializerImpl for TransientCloneDelegate {
    fn throw_data_clone_error(&self, scope: &mut v8::PinScope, message: v8::Local<v8::String>) {
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
    }

    fn get_shared_array_buffer_id<'s>(
        &self,
        _scope: &mut v8::PinScope<'s, '_>,
        buffer: v8::Local<'s, v8::SharedArrayBuffer>,
    ) -> Option<u32> {
        let mut buffers = self.shared_buffers.borrow_mut();
        let id = u32::try_from(buffers.len()).ok()?;
        buffers.push(v8::SharedArrayBuffer::get_backing_store(&buffer));
        Some(id)
    }

    fn get_wasm_module_transfer_id(
        &self,
        _scope: &mut v8::PinScope<'_, '_>,
        module: v8::Local<v8::WasmModuleObject>,
    ) -> Option<u32> {
        let mut modules = self.wasm_modules.borrow_mut();
        let id = u32::try_from(modules.len()).ok()?;
        modules.push(module.get_compiled_module());
        Some(id)
    }
}

impl v8::ValueDeserializerImpl for TransientCloneDelegate {
    fn get_shared_array_buffer_from_id<'s>(
        &self,
        scope: &mut v8::PinScope<'s, '_>,
        id: u32,
    ) -> Option<v8::Local<'s, v8::SharedArrayBuffer>> {
        let buffers = self.shared_buffers.borrow();
        let backing_store = buffers.get(id as usize)?.clone();
        Some(v8::SharedArrayBuffer::with_backing_store(
            scope,
            &backing_store,
        ))
    }

    fn get_wasm_module_from_id<'s>(
        &self,
        scope: &mut v8::PinScope<'s, '_>,
        id: u32,
    ) -> Option<v8::Local<'s, v8::WasmModuleObject>> {
        let modules = self.wasm_modules.borrow();
        v8::WasmModuleObject::from_compiled_module(scope, modules.get(id as usize)?)
    }
}

fn serialize_storage_value(
    scope: &mut v8::PinScope,
    value: v8::Local<v8::Value>,
) -> Option<Vec<u8>> {
    let context = scope.get_current_context();
    let serializer = v8::ValueSerializer::new(scope, Box::new(StorageValueDelegate));
    serializer.write_header();
    if !serializer.write_value(context, value).unwrap_or(false) {
        return None;
    }
    let mut bytes = serializer.release();
    // Workerd pins persisted actor values to V8 wire version 15 so rolling
    // upgrades remain readable by older processes. V8 150 writes version 16;
    // for Cells' supported (sub-4GiB) ArrayBuffers its varint encoding is
    // byte-compatible, so retain the Workerd on-disk version tag.
    if bytes.starts_with(&[0xff, 0x10]) {
        bytes[1] = 0x0f;
    }
    Some(bytes)
}

fn deserialize_storage_value<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: storage::StoredValue,
) -> Option<v8::Local<'s, v8::Value>> {
    match value {
        storage::StoredValue::LegacyJson(json) => {
            let json = v8::String::new(scope, &json)?;
            v8::json::parse(scope, json)
        }
        storage::StoredValue::V8(bytes) => {
            let context = scope.get_current_context();
            let deserializer =
                v8::ValueDeserializer::new(scope, Box::new(StorageValueDelegate), &bytes);
            deserializer.set_supports_legacy_wire_format(true);
            if !deserializer.read_header(context).unwrap_or(false) {
                return None;
            }
            deserializer.read_value(context)
        }
    }
}

/// First byte of a stored-stub row: a durable stub marker tree follows
/// as an ordinary V8 clone. Plain rows keep V8's own 0xff prefix, so
/// the tag branch never runs for them.
const STORED_STUB_TAG: u8 = 0x01;

fn wrap_stored<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    sentinel: v8::Local<v8::Value>,
    value: v8::Local<v8::Value>,
) -> v8::Local<'s, v8::Value> {
    let pair = v8::Array::new(scope, 2);
    pair.set_index(scope, 0, sentinel);
    pair.set_index(scope, 1, value);
    pair.into()
}

/// Decode one stored row for JS. A stub-tagged row comes back as
/// `[sentinel, tree]` (plus `true`), which only the storage wrapper —
/// holder of the closure-private sentinel — turns back into live stubs.
fn deserialize_stored<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    value: storage::StoredValue,
    sentinel: v8::Local<v8::Value>,
) -> Option<(v8::Local<'s, v8::Value>, bool)> {
    match value {
        storage::StoredValue::V8(bytes) if bytes.first() == Some(&STORED_STUB_TAG) => {
            let tree =
                deserialize_storage_value(scope, storage::StoredValue::V8(bytes[1..].to_vec()))?;
            Some((wrap_stored(scope, sentinel, tree), true))
        }
        value => deserialize_storage_value(scope, value).map(|value| (value, false)),
    }
}

/// `__sc_encode(value)` -> Uint8Array: V8 structured clone, sharing the
/// storage serializer (same delegate, same pinned wire version). The RPC
/// marshalling seam — a failed clone leaves the delegate's throw pending.
pub(super) fn op_sc_encode(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if let Some(bytes) = serialize_storage_value(scope, args.get(0)) {
        rv.set(bytes_value(scope, bytes));
    }
}

/// `__sc_decode(bytes)` -> value: inverse of `__sc_encode`.
pub(super) fn op_sc_decode(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let Some(bytes) = view_bytes(args.get(0)) else {
        let message = v8::String::new(scope, "__sc_decode expects bytes").unwrap();
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
        return;
    };
    match deserialize_storage_value(scope, storage::StoredValue::V8(bytes)) {
        Some(value) => rv.set(value),
        None => throw_storage_error(scope, "rpc", "corrupt clone payload"),
    }
}

/// Clone one value inside the current isolate. Unlike the persisted storage
/// codec, this transient path can retain compiled WebAssembly modules and
/// shared backing stores between its serializer and deserializer.
pub(super) fn op_structured_clone(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let scope = &mut tc.init();
    let context = scope.get_current_context();
    let delegate = TransientCloneDelegate::default();
    let bytes = {
        let serializer = v8::ValueSerializer::new(scope, Box::new(delegate.clone()));
        serializer.write_header();
        if !serializer
            .write_value(context, args.get(0))
            .unwrap_or(false)
        {
            if !scope.has_caught() {
                let message =
                    v8::String::new(scope, "structured clone serialization failed").unwrap();
                let exception = v8::Exception::type_error(scope, message);
                scope.throw_exception(exception);
            }
            scope.rethrow();
            return;
        }
        serializer.release()
    };
    let deserializer = v8::ValueDeserializer::new(scope, Box::new(delegate), &bytes);
    if !deserializer.read_header(context).unwrap_or(false) {
        if !scope.has_caught() {
            let message = v8::String::new(scope, "structured clone header is invalid").unwrap();
            let exception = v8::Exception::type_error(scope, message);
            scope.throw_exception(exception);
        }
        scope.rethrow();
        return;
    }
    let Some(value) = deserializer.read_value(context) else {
        if !scope.has_caught() {
            let message =
                v8::String::new(scope, "structured clone deserialization failed").unwrap();
            let exception = v8::Exception::type_error(scope, message);
            scope.throw_exception(exception);
        }
        scope.rethrow();
        return;
    };
    rv.set(value);
}

fn storage_string_array(
    scope: &mut v8::PinScope,
    value: v8::Local<v8::Value>,
) -> std::result::Result<Vec<String>, &'static str> {
    let array = v8::Local::<v8::Array>::try_from(value).map_err(|_| "keys must be an array")?;
    let mut keys = Vec::with_capacity(array.length() as usize);
    for index in 0..array.length() {
        let value = array.get_index(scope, index).ok_or("missing key")?;
        keys.push(value.to_rust_string_lossy(scope));
    }
    Ok(keys)
}

/// The `bool` reports whether any entry was a stored-stub row, so the
/// caller wraps the whole map only then — plain maps cross unwrapped and
/// the JS side never walks them.
fn storage_entries_map<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    entries: Vec<(String, storage::StoredValue)>,
    sentinel: v8::Local<v8::Value>,
) -> Option<(v8::Local<'s, v8::Map>, bool)> {
    let map = v8::Map::new(scope);
    let mut any_tagged = false;
    for (key, value) in entries {
        let key = v8::String::new(scope, &key)?;
        let (value, tagged) = deserialize_stored(scope, value, sentinel)?;
        any_tagged |= tagged;
        map.set(scope, key.into(), value)?;
    }
    Some((map, any_tagged))
}

pub(super) fn throw_storage_error(
    scope: &mut v8::PinScope,
    operation: &str,
    error: impl std::fmt::Display,
) {
    let message = v8::String::new(scope, &format!("storage.{operation}: {error}")).unwrap();
    let exception = v8::Exception::error(scope, message);
    scope.throw_exception(exception);
}
pub(super) fn op_storage_delete_all(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let cell = args.get(0).to_rust_string_lossy(scope);
    let delete_alarm = args.get(1).boolean_value(scope);
    if let Err(error) = storage::delete_all_with_alarm(&cell, delete_alarm) {
        let message = v8::String::new(scope, &format!("deleteAll: {error}")).unwrap();
        let exception = v8::Exception::error(scope, message);
        scope.throw_exception(exception);
    }
}
