// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Isolate bootstrap: what a fresh context needs before user code runs.
//!
//! The prelude and the harness compile once per process and are cached, so a
//! new isolate pays for execution but not for compilation. On top of those go
//! the bindings — `env`, the routing table, the compatibility flags — which
//! differ per worker and are therefore built every time.
use super::*;

/// Per-process compiled-bytecode cache for the fixed bootstrap scripts.
/// Every cell wake pays a fresh isolate, and without this each one re-parses
/// and re-compiles the same ~23k lines of prelude + harness — the single
/// largest slice of cold-wake latency. The first isolate compiles
/// eagerly and publishes the cache; every later isolate consumes it and only
/// executes.
type BootstrapCache = std::collections::HashMap<&'static str, std::sync::Arc<Vec<u8>>>;

fn bootstrap_code_cache() -> &'static std::sync::Mutex<BootstrapCache> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<BootstrapCache>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(Default::default)
}

fn run_bootstrap_script(scope: &mut v8::PinScope, name: &'static str, src: &str) -> Result<()> {
    use v8::script_compiler::CompileOptions;
    use v8::script_compiler::NoCacheReason;
    let code = v8::String::new(scope, src).unwrap();
    let cached = bootstrap_code_cache().lock().unwrap().get(name).cloned();
    let unbound = match cached {
        // `CachedData` borrows `bytes`; the Arc binding outlives the Source.
        Some(bytes) => {
            let data = v8::script_compiler::CachedData::new(&bytes);
            let mut source = v8::script_compiler::Source::new_with_cached_data(code, None, data);
            let unbound = v8::script_compiler::compile_unbound_script(
                scope,
                &mut source,
                CompileOptions::ConsumeCodeCache,
                NoCacheReason::NoReason,
            )
            .ok_or_else(|| anyhow!("bootstrap compile {name}"))?;
            debug_assert!(
                !source.get_cached_data().is_some_and(|data| data.rejected()),
                "code cache rejected for {name}"
            );
            unbound
        }
        None => {
            let mut source = v8::script_compiler::Source::new(code, None);
            // Eager: a lazily-compiled cache would push function-body compile
            // cost back into every consumer's first request.
            let unbound = v8::script_compiler::compile_unbound_script(
                scope,
                &mut source,
                CompileOptions::EagerCompile,
                NoCacheReason::NoReason,
            )
            .ok_or_else(|| anyhow!("bootstrap compile {name}"))?;
            if let Some(data) = unbound.create_code_cache() {
                bootstrap_code_cache()
                    .lock()
                    .unwrap()
                    .insert(name, std::sync::Arc::new(data.to_vec()));
            }
            unbound
        }
    };
    unbound
        .bind_to_current_context(scope)
        .run(scope)
        .ok_or_else(|| anyhow!("bootstrap run {name}"))?;
    Ok(())
}

/// WPT-conformant Web APIs: TextEncoder, URL/URLSearchParams, atob/btoa, and
/// Headers. Run once per isolate before the Cells/Cloudflare-specific harness.
pub(super) fn install_prelude(scope: &mut v8::PinScope) -> Result<()> {
    const PRELUDE: &[(&str, &str)] = &[
        ("text_encoding.js", include_str!("text_encoding.js")),
        ("url_search_params.js", include_str!("url_search_params.js")),
        ("url.js", include_str!("url.js")),
        ("atob_btoa.js", include_str!("atob_btoa.js")),
        ("headers.js", include_str!("headers.js")),
        ("streams.js", include_str!("streams.js")),
        ("writable_stream.js", include_str!("writable_stream.js")),
        (
            "text_encoding_streams.js",
            include_str!("text_encoding_streams.js"),
        ),
    ];
    for (name, src) in PRELUDE {
        run_bootstrap_script(scope, name, src)?;
    }
    Ok(())
}

pub(super) fn install_harness(scope: &mut v8::PinScope) -> Result<()> {
    #[cfg(celld_internal_tests)]
    let harness = include_str!("harness.js")
        .replace(
            "/*__CELLD_TEST_WORKFLOW_EVENT_CONSUMED__*/",
            "__test_workflow_event_consumed();",
        )
        .replace(
            "/*__CELLD_TEST_WORKFLOW_META_CREATED__*/",
            "__test_workflow_meta_created();",
        )
        .replace(
            "/*__CELLD_TEST_WORKFLOW_ALARM_DELETED__*/",
            "__test_workflow_alarm_deleted();",
        )
        .replace(
            "/*__CELLD_TEST_WORKFLOW_BEFORE_PAUSE_SETTLE__*/",
            concat!(
                "if (globalThis.__test_workflow_pause_settle_race === true) { ",
                "await this.__wfResume(); ",
                "globalThis.__test_workflow_pause_settle_race = false; }"
            ),
        )
        .replace(
            "/*__CELLD_TEST_WORKFLOW_AFTER_RUN_STARTED__*/",
            concat!(
                "if (globalThis.__test_workflow_pause_settle_race === true) { ",
                "await globalThis.__test_workflow_callback_started(); ",
                "await this.__wfPause(); globalThis.__test_workflow_release(); }"
            ),
        );
    #[cfg(celld_internal_tests)]
    let harness = harness.as_str();
    #[cfg(not(celld_internal_tests))]
    let harness = include_str!("harness.js");
    run_bootstrap_script(scope, "harness.js", harness)?;
    // After the harness: both build on its EventTarget, and EventSource
    // uses its fetch and timers.
    run_bootstrap_script(
        scope,
        "message_channel.js",
        include_str!("message_channel.js"),
    )?;
    run_bootstrap_script(scope, "event_source.js", include_str!("event_source.js"))?;
    run_bootstrap_script(scope, "cache.js", include_str!("cache.js"))?;
    run_bootstrap_script(scope, "sockets.js", include_str!("sockets.js"))?;
    run_bootstrap_script(scope, "html_rewriter.js", include_str!("html_rewriter.js"))?;
    run_bootstrap_script(scope, "crypto.js", include_str!("crypto.js"))?;
    // Resolving the event hooks belongs to installing the harness, not to the
    // first event: the harness is what defines them, and a caller that
    // installed the harness without them would silently fall back to nothing
    // at all. `harness.js` has just run, so all four globals exist here.
    install_event_hooks(scope)
}

/// Resolve the per-event harness hooks once and hold them for the isolate.
/// See [`EventHooks`] for why all four resolve together.
fn install_event_hooks(scope: &mut v8::PinScope) -> Result<()> {
    let hooks = EventHooks {
        begin_event: global_function(scope, "__beginEvent")?,
        end_event: global_function(scope, "__endEvent")?,
        advance_io_time: global_function(scope, "__advanceIoTime")?,
        abort_incoming_request: global_function(scope, "__abortIncomingRequest")?,
    };
    actor_runtime_state(scope)
        .event_hooks
        .set(hooks)
        .map_err(|_| anyhow!("event hooks were already installed"))
}

fn global_function(scope: &mut v8::PinScope, name: &str) -> Result<v8::Global<v8::Function>> {
    let global = scope.get_current_context().global(scope);
    let key = v8::String::new(scope, name).unwrap();
    let function: v8::Local<v8::Function> = global
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("no {name}"))?
        .try_into()
        .map_err(|_| anyhow!("{name} is not a function"))?;
    Ok(v8::Global::new(scope, function))
}

pub(super) fn register_class(
    scope: &mut v8::PinScope,
    name: &str,
    cls: v8::Local<v8::Value>,
) -> Result<()> {
    let ctx = scope.get_current_context();
    let global = ctx.global(scope);
    let ck = static_key(scope, &v8_strings::CELL);
    let cell = global
        .get(scope, ck.into())
        .unwrap()
        .to_object(scope)
        .unwrap();
    let clk = v8::String::new(scope, "classes").unwrap();
    let classes = cell
        .get(scope, clk.into())
        .unwrap()
        .to_object(scope)
        .unwrap();
    let nk = v8::String::new(scope, name).unwrap();
    classes.set(scope, nk.into(), cls);
    Ok(())
}

/// Populate `cloudflare:workers` `exports` with the worker module's own exports:
/// a Durable Object class is exposed as its namespace (idFromName/get/RPC), any
/// other export by value. `default` is omitted (it is the fetch entrypoint, not
/// an importable export). Mirrors Workerd's `import { exports }` surface.
pub(super) fn populate_cf_exports(
    scope: &mut v8::PinScope,
    ns: v8::Local<v8::Object>,
    do_classes: &[String],
) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cf = global
        .get(scope, v8::String::new(scope, "__cf").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cf runtime state"))?;
    let exports = cf
        .get(scope, v8::String::new(scope, "exports").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cf.exports"))?;
    let cell_key = static_key(scope, &v8_strings::CELL);
    let cell = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let make_ns: v8::Local<v8::Function> = cell
        .get(
            scope,
            v8::String::new(scope, "makeNamespace").unwrap().into(),
        )
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| anyhow!("missing __cell.makeNamespace"))?;
    let do_set: std::collections::HashSet<&str> = do_classes.iter().map(String::as_str).collect();
    let names = ns
        .get_own_property_names(scope, Default::default())
        .ok_or_else(|| anyhow!("no module export names"))?;
    for index in 0..names.length() {
        let name_value = names.get_index(scope, index).unwrap();
        let name = name_value.to_rust_string_lossy(scope);
        if name == "default" {
            continue;
        }
        let export_value = if do_set.contains(name.as_str()) {
            let arg = v8::String::new(scope, &name).unwrap().into();
            make_ns
                .call(scope, cell.into(), &[arg])
                .ok_or_else(|| anyhow!("makeNamespace({name}) failed"))?
        } else {
            ns.get(scope, name_value)
                .ok_or_else(|| anyhow!("export {name}"))?
        };
        let key = v8::String::new(scope, &name).unwrap();
        exports.set(scope, key.into(), export_value);
    }
    Ok(())
}

pub(super) fn inject_namespace_keys(
    scope: &mut v8::PinScope,
    script_name: &str,
    do_classes: &[String],
) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell_key = static_key(scope, &v8_strings::CELL);
    let cell = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let keys_key = v8::String::new(scope, "namespaceKeys").unwrap();
    let keys = cell
        .get(scope, keys_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing Durable Object namespace registry"))?;
    let script_key = v8::String::new(scope, "script").unwrap();
    let script_value = v8::String::new(scope, script_name).unwrap();
    cell.set(scope, script_key.into(), script_value.into());
    for class_name in do_classes {
        let class_key = v8::String::new(scope, class_name).unwrap();
        let namespace_key = super::namespace_key(script_name, class_name);
        let namespace_key = v8::String::new(scope, &namespace_key).unwrap();
        if !keys
            .set(scope, class_key.into(), namespace_key.into())
            .unwrap_or(false)
        {
            anyhow::bail!("could not register namespace key for {class_name}");
        }
    }
    Ok(())
}

/// `__cell.crons`: the deployment's cron trigger expressions, verbatim as the
/// developer wrote them. The reserved cron cell reads them at each activation,
/// so a schedule change arrives with the deployment and needs no migration of
/// an alarm the previous deployment armed. Absent `triggers.crons`, the list
/// is empty and the cron cell retires itself.
pub(super) fn inject_crons(scope: &mut v8::PinScope, crons: &[String]) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell_key = static_key(scope, &v8_strings::CELL);
    let cell = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let list = v8::Array::new(scope, crons.len() as i32);
    for (index, cron) in crons.iter().enumerate() {
        let value = v8::String::new(scope, cron).ok_or_else(|| anyhow!("cron expression"))?;
        list.set_index(scope, index as u32, value.into());
    }
    let key = v8::String::new(scope, "crons").unwrap();
    cell.set(scope, key.into(), list.into());
    Ok(())
}

/// `__cell.workflows`: the deployment's workflow-name → class-name map. The
/// reserved workflow cell resolves the user class from it at each replay, so
/// a redeploy that renames the class takes effect on the next activation and
/// nothing about a running instance's ledger has to move.
pub(super) fn inject_workflows(
    scope: &mut v8::PinScope,
    script_name: &str,
    workflow_bindings: &[WorkflowBinding],
) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell = global
        .get(scope, v8::String::new(scope, "__cell").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let map = v8::Object::new(scope);
    for binding in workflow_bindings {
        let key =
            v8::String::new(scope, &binding.workflow).ok_or_else(|| anyhow!("workflow name"))?;
        let value =
            v8::String::new(scope, &binding.class).ok_or_else(|| anyhow!("workflow class"))?;
        map.set(scope, key.into(), value.into());
    }
    let key = v8::String::new(scope, "workflows").unwrap();
    cell.set(scope, key.into(), map.into());
    // Only when the deployment declares a workflow. The harness registers
    // `__WorkflowCell` in every isolate, so aliasing unconditionally would put
    // a class in the registry that this deployment never declared. It would be
    // inert -- `cell_configs` is built from the manifest's `do_classes` and
    // would not name it -- but "the harness registers what the deployment
    // declares" is worth keeping true rather than nearly true.
    if !workflow_bindings.is_empty() {
        alias_workflow_class(scope, cell, script_name)?;
    }
    Ok(())
}

/// Register the workflow cell class under its script-scoped name.
///
/// `harness.js` registers `__WorkflowCell` under the bare `__Workflow` at
/// install time, which is before `__cell.script` exists, so the script-scoped
/// name cannot be built there. This copies the registration across once the
/// script is known.
///
/// The bare entry stays behind and is inert: `cell_configs` is built from the
/// manifest's `do_classes`, which carries only the scoped name, so no cell can
/// start under it. Removing it would buy nothing and would leave the harness
/// and this function disagreeing about who owns the registration. Done through the V8 object API rather than a compiled
/// snippet, because this runs on every isolate load and a `Script::compile`
/// here cost ~100us per worker.
fn alias_workflow_class(
    scope: &mut v8::PinScope,
    cell: v8::Local<v8::Object>,
    script_name: &str,
) -> Result<()> {
    let alias = crate::deploy::workflow_class(script_name);
    let alias = v8::String::new(scope, &alias).ok_or_else(|| anyhow!("workflow class name"))?;
    let base = v8::String::new(scope, crate::deploy::WORKFLOW_CLASS).unwrap();
    for registry in ["classes", "doExports"] {
        let key = v8::String::new(scope, registry).unwrap();
        let table = cell
            .get(scope, key.into())
            .and_then(|value| value.to_object(scope))
            .ok_or_else(|| anyhow!("missing __cell.{registry}"))?;
        let value = table
            .get(scope, base.into())
            .ok_or_else(|| anyhow!("harness registered no {registry} entry for a workflow"))?;
        table.set(scope, alias.into(), value);
    }
    Ok(())
}

/// `__cell.kvLimits`: every bound `celld_logic::kv` declares, as data.
///
/// Injected rather than written in `harness.js` so the binding, `celld kv` and
/// deploy-time validation cannot disagree about what a valid key is. No host
/// op: a limit is data rather than a decision, so shipping the values is
/// enough, and D1 set the bar at one op with Workflows coming in under it.
///
/// Built through the V8 object API rather than by compiling a snippet, because
/// this runs on every isolate load.
pub(super) fn inject_kv_limits(scope: &mut v8::PinScope) -> Result<()> {
    use celld_logic::kv;
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell = global
        .get(scope, v8::String::new(scope, "__cell").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let limits = v8::Object::new(scope);
    let set = |scope: &mut v8::PinScope, name: &str, value: f64| {
        let key = v8::String::new(scope, name).unwrap();
        let value = v8::Number::new(scope, value);
        limits.set(scope, key.into(), value.into());
    };
    set(scope, "maxKeyBytes", kv::MAX_KEY_BYTES as f64);
    set(scope, "maxValueBytes", kv::MAX_VALUE_BYTES as f64);
    set(
        scope,
        "maxInlineValueBytes",
        kv::MAX_INLINE_VALUE_BYTES as f64,
    );
    set(scope, "maxMetadataBytes", kv::MAX_METADATA_BYTES as f64);
    set(scope, "maxBulkKeys", kv::MAX_BULK_KEYS as f64);
    set(scope, "maxListLimit", kv::MAX_LIST_LIMIT as f64);
    set(
        scope,
        "sweepBatchRows",
        celld_logic::sweep::BATCH_ROWS as f64,
    );
    // One variable for every KV test knob, not one variable each.
    //
    //   CELLD_TEST_KV=no-sweep,fail-after-blob,min-ttl-ms=1000
    //
    // These are test-only and they read the production environment rather than
    // sitting behind `cfg(celld_internal_tests)`, because the code they steer
    // is JavaScript in the harness and a cfg cannot reach it.
    // `CELLD_TEST_OTEL_SWEEP_MS` is the existing precedent for a
    // controlled-timing override living in production code. Collapsing them
    // into one name keeps that surface at a single documented variable however
    // many knobs the feature grows, rather than a new `CELLD_TEST_KV_*` for
    // each -- which is what this had already become.
    //
    // What each is for, and why a test cannot do without it:
    //
    // - `min-ttl-ms` shortens upstream's sixty-second expiry floor, because a
    //   test cannot wait out a minute of wall clock. Nothing else about the
    //   deadline arithmetic changes.
    // - `no-sweep` stops reclamation, so a test that means to pin the *read
    //   filter* can show the filter hid an expired key rather than the sweep
    //   having deleted the row underneath it. The first expiry test passed
    //   with the filter deleted until this existed.
    // - `fail-after-blob` fails a put between the blob write and the row
    //   commit. That window is one await wide, and a SIGKILL there would take
    //   the assertion with it.
    // - `race-sweep-put` holds a blob sweep after its mark snapshot until a
    //   new large put starts. The put and the sweep then overlap, so a test
    //   proves that collection authority is serialized with the put protocol.
    // - `blob-sweep-ms` shortens the durable collector delay. A crash-orphan
    //   test must observe the wake without waiting one production minute.
    // - `legacy-schema` creates the inline-only table from the first KV
    //   release, so the migration test proves an existing row survives.
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
    let min_ttl = knob("min-ttl-ms")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(kv::MIN_EXPIRATION_TTL_MS);
    set(scope, "minExpirationTtlMs", min_ttl as f64);
    let blob_sweep_ms = knob("blob-sweep-ms")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(60_000);
    set(scope, "blobSweepMs", blob_sweep_ms as f64);
    for (name, present) in [
        ("sweepDisabled", knob("no-sweep").is_some()),
        ("failAfterBlobWrite", knob("fail-after-blob").is_some()),
        ("raceSweepPut", knob("race-sweep-put").is_some()),
        ("legacySchema", knob("legacy-schema").is_some()),
    ] {
        let key = v8::String::new(scope, name).unwrap();
        let value = v8::Boolean::new(scope, present);
        limits.set(scope, key.into(), value.into());
    }
    let key = v8::String::new(scope, "kvLimits").unwrap();
    cell.set(scope, key.into(), limits.into());
    Ok(())
}

/// `globalThis.Cloudflare.compatibilityFlags`. Only flags Cells actually
/// honours are listed; a flag Cells does not model is absent (falsy) rather
/// than reported as enabled.
///
/// Built through the V8 object API rather than by compiling a snippet: this
/// runs on every isolate load, and a separate `Script::compile` there cost
/// ~100us per worker.
pub(super) fn inject_compatibility_flags(scope: &mut v8::PinScope, compat: Compat) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let flags = v8::Object::new(scope);
    let set_flag = |scope: &mut v8::PinScope, name: &str, value: bool| {
        let key = v8::String::new(scope, name).unwrap();
        let value = v8::Boolean::new(scope, value);
        flags.set(scope, key.into(), value.into());
    };
    set_flag(scope, "upper_case_all_http_methods", true);
    // Cells' async stream/pipe methods return rejected promises
    // rather than throwing synchronously, which is exactly the
    // behavior this flag names.
    set_flag(scope, "capture_async_api_throws", true);
    set_flag(
        scope,
        "delete_all_deletes_alarm",
        compat.delete_all_deletes_alarm,
    );
    set_flag(scope, "js_rpc", compat.js_rpc);
    set_flag(scope, "sqlite_vec", compat.sqlite_vec);
    set_flag(
        scope,
        "fetcher_no_get_put_delete",
        !compat.fetcher_get_put_delete,
    );
    let cloudflare = v8::Object::new(scope);
    let key = v8::String::new(scope, "compatibilityFlags").unwrap();
    cloudflare.set(scope, key.into(), flags.into());
    let key = v8::String::new(scope, "Cloudflare").unwrap();
    global.set(scope, key.into(), cloudflare.into());
    Ok(())
}

pub(super) fn build_env(scope: &mut v8::PinScope, config: &WorkerConfig) -> Result<()> {
    let script_name = config.script_name.as_str();
    let bindings = config.bindings.as_slice();
    let r2_bindings = config.r2_bindings.as_slice();
    let d1_bindings = config.d1_bindings.as_slice();
    let kv_bindings = config.kv_bindings.as_slice();
    let queue_bindings = config.queue_bindings.as_slice();
    let workflow_bindings = config.workflow_bindings.as_slice();
    let ai_binding = config.ai_binding.as_deref();
    let vars = config.vars.as_slice();
    let services = config.services.as_slice();
    let asset_binding = config.asset_binding.as_deref();
    // call the harness: for each binding, env[bindingName] = makeNamespace(className)
    let src = {
        let mut lines = String::from("(() => { const e = __cell.env;\n");
        for (bname, cname) in bindings {
            lines.push_str(&format!(
                "e[{:?}] = __cell.makeNamespace({:?});\n",
                bname, cname
            ));
        }
        for (name, bucket) in r2_bindings {
            lines.push_str(&format!(
                "e[{:?}] = __makeR2Bucket({:?}, {:?});\n",
                name, name, bucket
            ));
        }
        for (binding, id) in kv_bindings {
            // The cell name comes from `celld_logic::kv::cell_name`, never from
            // the harness. It carries the shard, and while there is one shard
            // that is a constant the binding must not be trusted to reproduce:
            // the name is hashed into the cell scope, so a formatting
            // disagreement would silently address a second, empty namespace
            // rather than fail. When shards outnumber one, this becomes a name
            // per shard and the choice moves here too.
            lines.push_str(&format!(
                "e[{:?}] = __makeKvNamespace({:?}, {:?});\n",
                binding,
                id,
                celld_logic::kv::cell_name(id, 0),
            ));
        }
        for binding in queue_bindings {
            lines.push_str(&format!(
                "e[{:?}] = __makeQueue({:?}, {:?}, {});\n",
                binding.environment,
                binding.queue,
                celld_logic::queue::cell_name(&binding.queue),
                binding.delivery_delay,
            ));
        }
        for (name, database) in d1_bindings {
            lines.push_str(&format!(
                "e[{:?}] = __makeD1Database({:?});\n",
                name, database
            ));
        }
        for binding in workflow_bindings {
            lines.push_str(&format!(
                "e[{:?}] = __makeWorkflow({:?});\n",
                binding.environment, binding.workflow
            ));
        }
        if let (Some(name), Ok(url)) = (ai_binding, std::env::var("CELLD_AI_URL")) {
            lines.push_str(&format!("e[{:?}] = __makeAiBinding({:?});\n", name, url));
        }
        for (binding, script, entrypoint) in services {
            let entrypoint = match entrypoint {
                Some(name) => format!("{name:?}"),
                None => "null".to_string(),
            };
            lines.push_str(&format!(
                "e[{:?}] = __makeServiceBinding({:?}, {});\n",
                binding, script, entrypoint
            ));
        }
        for (name, value) in vars {
            lines.push_str(&format!("e[{:?}] = {:?};\n", name, value));
        }
        if let Some(name) = asset_binding {
            lines.push_str(&format!(
                "e[{:?}] = __makeAssetsBinding({:?});\n",
                name, script_name
            ));
        }
        if let Some(name) = config.loader_binding.as_deref() {
            lines.push_str(&format!("e[{:?}] = __makeLoader();\n", name));
        }
        // A loaded worker's caller-supplied `env` (plain JSON values only in
        // the walking skeleton) merges last, over the declared bindings.
        if let Some(env) = config.loader_env.as_deref() {
            lines.push_str(&format!("Object.assign(e, {});\n", env));
        }
        lines.push_str("})();");
        lines
    };
    let code = v8::String::new(scope, &src).unwrap();
    let s = v8::Script::compile(scope, code, None).ok_or_else(|| anyhow!("env compile"))?;
    s.run(scope).ok_or_else(|| anyhow!("env run"))?;
    Ok(())
}

#[cfg(celld_internal_tests)]
thread_local! {
    static QUEUE_LEASE_DURATION_FOR_TEST: std::cell::Cell<Option<i64>> = const {
        std::cell::Cell::new(None)
    };
    static QUEUE_BATCH_TIMEOUT_FOR_TEST: std::cell::Cell<Option<i64>> = const {
        std::cell::Cell::new(None)
    };
    static QUEUE_RETENTION_FOR_TEST: std::cell::Cell<Option<i64>> = const {
        std::cell::Cell::new(None)
    };
    static QUEUE_SWEEP_BATCH_FOR_TEST: std::cell::Cell<Option<usize>> = const {
        std::cell::Cell::new(None)
    };
}

/// Measure lease expiry without waiting through the production handler budget.
/// The override is thread-local because the private runtime corpus builds
/// unrelated Workers in parallel, and a process environment variable would
/// silently shorten their leases too.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn set_queue_lease_duration_for_test(duration: Option<i64>) {
    QUEUE_LEASE_DURATION_FOR_TEST.set(duration);
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn set_queue_batch_timeout_for_test(duration: Option<i64>) {
    QUEUE_BATCH_TIMEOUT_FOR_TEST.set(duration);
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn set_queue_retention_for_test(duration: Option<i64>) {
    QUEUE_RETENTION_FOR_TEST.set(duration);
}

#[cfg(celld_internal_tests)]
pub fn set_queue_sweep_batch_for_test(rows: Option<usize>) {
    QUEUE_SWEEP_BATCH_FOR_TEST.set(rows);
}

#[cfg(celld_internal_tests)]
fn effective_queue_lease_duration(duration: i64) -> i64 {
    QUEUE_LEASE_DURATION_FOR_TEST.get().unwrap_or(duration)
}

#[cfg(not(celld_internal_tests))]
fn effective_queue_lease_duration(duration: i64) -> i64 {
    duration
}

#[cfg(celld_internal_tests)]
fn effective_queue_batch_timeout(duration: i64) -> i64 {
    if duration == 0 {
        0
    } else {
        QUEUE_BATCH_TIMEOUT_FOR_TEST.get().unwrap_or(duration)
    }
}

#[cfg(celld_internal_tests)]
fn effective_queue_retention(duration: i64) -> i64 {
    QUEUE_RETENTION_FOR_TEST.get().unwrap_or(duration)
}

#[cfg(not(celld_internal_tests))]
fn effective_queue_retention(duration: i64) -> i64 {
    duration
}

#[cfg(celld_internal_tests)]
fn effective_queue_sweep_batch(rows: usize) -> usize {
    QUEUE_SWEEP_BATCH_FOR_TEST.get().unwrap_or(rows)
}

#[cfg(not(celld_internal_tests))]
fn effective_queue_sweep_batch(rows: usize) -> usize {
    rows
}

#[cfg(not(celld_internal_tests))]
fn effective_queue_batch_timeout(duration: i64) -> i64 {
    duration
}

/// Install the deployment-wide Queue consumer catalog and lease budget.
///
/// Every co-hosted config gets the same catalog before an isolate starts, so a
/// shared `__Queue` class cannot dispatch according to whichever producer
/// registered the class first.
pub(super) fn inject_queue_config(scope: &mut v8::PinScope, config: &WorkerConfig) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell = global
        .get(scope, v8::String::new(scope, "__cell").unwrap().into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let consumers = v8::Object::new(scope);
    for registration in &config.queue_consumers {
        let value = v8::Object::new(scope);
        let set_string = |scope: &mut v8::PinScope, name: &str, text: &str| {
            let key = v8::String::new(scope, name).unwrap();
            let text = v8::String::new(scope, text).unwrap();
            value.set(scope, key.into(), text.into());
        };
        let set_number = |scope: &mut v8::PinScope, name: &str, number: f64| {
            let key = v8::String::new(scope, name).unwrap();
            let number = v8::Number::new(scope, number);
            value.set(scope, key.into(), number.into());
        };
        let config = &registration.config;
        set_string(scope, "script", &registration.script);
        set_number(scope, "maxBatchSize", f64::from(config.max_batch_size));
        set_number(
            scope,
            "maxBatchTimeoutMs",
            effective_queue_batch_timeout(i64::from(config.max_batch_timeout) * 1000) as f64,
        );
        set_number(scope, "maxRetries", f64::from(config.max_retries));
        if let Some(dead_letter_queue) = &config.dead_letter_queue {
            set_string(scope, "deadLetterQueue", dead_letter_queue);
        }
        if let Some(max_concurrency) = config.max_concurrency {
            set_number(scope, "maxConcurrency", f64::from(max_concurrency));
        }
        if let Some(retry_delay) = config.retry_delay {
            set_number(scope, "retryDelaySeconds", f64::from(retry_delay));
        }
        let key = v8::String::new(scope, &config.queue).unwrap();
        consumers.set(scope, key.into(), value.into());
    }
    let key = v8::String::new(scope, "queueConsumers").unwrap();
    cell.set(scope, key.into(), consumers.into());

    let admission = crate::runtime::admission_wait()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    let handler = super::handler_budget().as_millis().min(i64::MAX as u128) as i64;
    let settlement = i64::try_from(crate::actor::operation_deadline_ms()?).unwrap_or(i64::MAX);
    let duration = effective_queue_lease_duration(celld_logic::queue::lease_duration_ms(
        admission, handler, settlement,
    ));
    let key = v8::String::new(scope, "queueLeaseDurationMs").unwrap();
    let duration = v8::Number::new(scope, duration as f64);
    cell.set(scope, key.into(), duration.into());
    let limits = v8::Object::new(scope);
    let set_limit = |scope: &mut v8::PinScope, name: &str, number: f64| {
        let key = v8::String::new(scope, name).unwrap();
        let number = v8::Number::new(scope, number);
        limits.set(scope, key.into(), number.into());
    };
    set_limit(
        scope,
        "maxMessageBytes",
        celld_logic::queue::MAX_MESSAGE_BYTES as f64,
    );
    set_limit(
        scope,
        "maxBatchBytes",
        celld_logic::queue::MAX_SEND_BATCH_BYTES as f64,
    );
    set_limit(
        scope,
        "maxBatchMessages",
        celld_logic::queue::MAX_BATCH_MESSAGES as f64,
    );
    set_limit(
        scope,
        "producerGroupMs",
        crate::env_vars::optional::<u64>("CELLD_QUEUE_PRODUCER_GROUP_MS")?.unwrap_or(4) as f64,
    );
    set_limit(
        scope,
        "maxConcurrency",
        celld_logic::queue::MAX_CONCURRENCY as f64,
    );
    set_limit(
        scope,
        "maxDelaySeconds",
        celld_logic::queue::MAX_DELAY_SECONDS as f64,
    );
    set_limit(
        scope,
        "retentionMs",
        effective_queue_retention(celld_logic::queue::RETENTION_MS) as f64,
    );
    set_limit(
        scope,
        "sweepBatchRows",
        effective_queue_sweep_batch(celld_logic::sweep::BATCH_ROWS) as f64,
    );
    let key = v8::String::new(scope, "queueLimits").unwrap();
    cell.set(scope, key.into(), limits.into());
    Ok(())
}

/// Record every exported class deriving from `WorkerEntrypoint` in
/// `__cell.entrypoints`, so a service binding with `entrypoint = "Name"` can
/// resolve it, and every export deriving from `DurableObject` in
/// `__cell.doExports`, so misrouting a DO class as a stateless entrypoint is
/// reported as such (Workerd getExportedHandler()). Runs once per load; the
/// check is a prototype walk, not a call into user code.
pub(super) fn register_entrypoints(
    scope: &mut v8::PinScope,
    ns: v8::Local<v8::Object>,
) -> Result<()> {
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cell_key = static_key(scope, &v8_strings::CELL);
    let cell = global
        .get(scope, cell_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cell runtime state"))?;
    let key = v8::String::new(scope, "entrypoints").unwrap();
    let entrypoints = cell
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing entrypoint registry"))?;
    let key = v8::String::new(scope, "objectEntrypoints").unwrap();
    let object_entrypoints = cell
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing object-entrypoint registry"))?;
    let key = v8::String::new(scope, "doExports").unwrap();
    let do_exports = cell
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing doExports registry"))?;
    let key = v8::String::new(scope, "classes").unwrap();
    let classes = cell
        .get(scope, key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing Durable Object class registry"))?;
    let cf_key = v8::String::new(scope, "__cf").unwrap();
    let cf = global
        .get(scope, cf_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cf"))?;
    let key = v8::String::new(scope, "WorkerEntrypoint").unwrap();
    let entrypoint_base = cf
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("missing WorkerEntrypoint base"))?;
    let key = v8::String::new(scope, "DurableObject").unwrap();
    let durable_base = cf
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("missing DurableObject base"))?;
    let Some(names) = ns.get_own_property_names(scope, Default::default()) else {
        return Ok(());
    };
    let yes = v8::Boolean::new(scope, true);
    let extends = compile_fn(scope, EXTENDS_SRC)?;
    for index in 0..names.length() {
        let Some(name) = names.get_index(scope, index) else {
            continue;
        };
        let Some(value) = ns.get(scope, name) else {
            continue;
        };
        if !value.is_function() {
            // A plain-object export is Workerd's non-class entrypoint; its
            // handler functions dispatch as fn(arg, env, ctx).
            if value.is_object() {
                object_entrypoints.set(scope, name, value);
            }
            continue;
        }
        // Walk the prototype chain rather than calling anything. The walk
        // runs in JS: Reflect.getPrototypeOf honors Proxy exports (SDK shims
        // like workers-rs wrap their classes), which the host-side
        // Object::GetPrototype does not.
        if call_extends(scope, extends, value, entrypoint_base)? {
            entrypoints.set(scope, name, value);
        } else if call_extends(scope, extends, value, durable_base)? {
            do_exports.set(scope, name, yes.into());
            classes.set(scope, name, value);
        }
    }
    Ok(())
}

/// Validate the deployment's workflow classes at load: each configured
/// `class_name` must be a module export that extends `WorkflowEntrypoint`
/// from `cloudflare:workers`, so a wrong class fails the load loudly,
/// exactly as a Durable Object export does. The per-replay resolution off
/// `__cell.workflows` and `__cf.exports` is unchanged; this checks the same
/// names once, up front, instead of erroring the first instance at run time.
pub(super) fn validate_workflow_classes(
    scope: &mut v8::PinScope,
    ns: v8::Local<v8::Object>,
    workflow_bindings: &[WorkflowBinding],
) -> Result<()> {
    if workflow_bindings.is_empty() {
        return Ok(());
    }
    let context = scope.get_current_context();
    let global = context.global(scope);
    let cf_key = v8::String::new(scope, "__cf").unwrap();
    let cf = global
        .get(scope, cf_key.into())
        .and_then(|value| value.to_object(scope))
        .ok_or_else(|| anyhow!("missing __cf"))?;
    let key = v8::String::new(scope, "WorkflowEntrypoint").unwrap();
    let base = cf
        .get(scope, key.into())
        .ok_or_else(|| anyhow!("missing WorkflowEntrypoint base"))?;
    let extends = compile_fn(scope, EXTENDS_SRC)?;
    for binding in workflow_bindings {
        let class_name = binding.class.as_str();
        let key = v8::String::new(scope, class_name).unwrap();
        let class = ns
            .get(scope, key.into())
            .filter(|value| !value.is_undefined())
            .ok_or_else(|| {
                anyhow!("workflow class {class_name} is not exported by the Worker module")
            })?;
        if !class.is_function() || !call_extends(scope, extends, class, base)? {
            anyhow::bail!(
                "workflow class {class_name} must extend WorkflowEntrypoint \
                 from cloudflare:workers"
            );
        }
    }
    Ok(())
}

/// `(cls, base) => base is on cls's prototype chain`, via Reflect so Proxy
/// wrappers report the prototype their handler exposes. A getPrototypeOf trap
/// also makes the chain user-controlled (a cycle, or a fresh Proxy per hop,
/// is spec-legal on an extensible target), so track visited links and cap the
/// walk rather than let it hang the load.
const EXTENDS_SRC: &str = "((cls, base) => { \
    const seen = new Set(); \
    for (let p = Reflect.getPrototypeOf(cls); p; p = Reflect.getPrototypeOf(p)) { \
        if (p === base) return true; \
        if (seen.has(p) || seen.size >= 1000) return false; \
        seen.add(p); \
    } \
    return false; })";

/// The walk crosses into user code when an export is a Proxy, so it can throw
/// (a revoked Proxy, a throwing getPrototypeOf trap). Pin a TryCatch and
/// surface that as a load error rather than misclassifying the export.
fn call_extends(
    scope: &mut v8::PinScope,
    extends: v8::Local<v8::Function>,
    class: v8::Local<v8::Value>,
    base: v8::Local<v8::Value>,
) -> Result<bool> {
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let scope = &mut tc.init();
    let recv = v8::undefined(scope).into();
    match extends.call(scope, recv, &[class, base]) {
        Some(result) => Ok(result.is_true()),
        None => Err(anyhow!("inspect export prototype chain: {}", exc!(scope))),
    }
}

/// Record the node id. Every cell scope routes through the host, whichever node
/// owns it.
pub(super) fn inject_routing(scope: &mut v8::PinScope, node: &str) -> Result<()> {
    let src = format!("(() => {{ __cell.node = {node:?}; }})();");
    let code = v8::String::new(scope, &src).unwrap();
    let s = v8::Script::compile(scope, code, None).ok_or_else(|| anyhow!("routing compile"))?;
    s.run(scope).ok_or_else(|| anyhow!("routing run"))?;
    Ok(())
}

/// Release a cell's instance and restore its id name.
///
/// The isolate-side half of taking a cell in or giving it back: taking it opens
/// the cell's storage, giving it back releases what the isolate holds and closes
/// the database, so state cannot span two epochs.
pub(super) fn adopt_cell(
    tc: &mut v8::PinScope,
    cell: &str,
    cell_storage: Option<CellStorage<'_>>,
    compat: Compat,
) -> Result<Option<i64>> {
    let owned = cell_storage.is_some();
    if let Some(cell_storage) = cell_storage {
        storage::open_at_epoch(
            cell,
            cell_storage.path,
            cell_storage.epoch,
            cell_storage.vfs,
            compat.sqlite_vec,
        )
        .context("cell storage open failed")?;
    }
    finish_cell_adoption(tc, cell, owned)
}

pub(super) fn adopt_embedded_cell(
    tc: &mut v8::PinScope,
    cell: &str,
    parent: &storage::StorageIdentity,
    name: &str,
    id: &str,
    props_json: &str,
    compat: Compat,
) -> Result<Option<i64>> {
    storage::open_embedded(cell, parent, name, compat.sqlite_vec)
        .context("facet storage open failed")?;
    let alarm = finish_cell_adoption(tc, cell, true)?;
    let depth = parent.facet_path.len() + 1;
    let source = format!(
        "__cell.facetConfigs[{cell:?}] = {{ id: {id:?}, props: JSON.parse({props_json:?}), depth: {depth} }};"
    );
    let code = v8::String::new(tc, &source).unwrap();
    let script =
        v8::Script::compile(tc, code, None).ok_or_else(|| anyhow!("facet config compile"))?;
    script
        .run(tc)
        .ok_or_else(|| anyhow!("facet config install"))?;
    Ok(alarm)
}

fn finish_cell_adoption(tc: &mut v8::PinScope, cell: &str, owned: bool) -> Result<Option<i64>> {
    // Every cell dispatch goes out through the host and arrives as an ordinary
    // reentrant `CellJob::Fetch`. There is no same-isolate shortcut, and there
    // will not be one.
    //
    // A dispatch that ran the callee's handler inside the caller's execution
    // would skip the callee's IoContext, its input gate, and its request
    // accounting: a `blockConcurrencyWhile` held by the callee would not hold
    // off such a call. That is a different semantic, not an optimisation. The
    // nesting deadlock that first disabled the shortcut -- A calls B, B calls
    // back into A, and the stack never unwinds -- was the symptom of crossing
    // that boundary rather than the reason to avoid it.
    //
    // The script releases everything the isolate holds for this cell's
    // residency, on both edges: the give-back frees what the application left
    // behind — memory the node cannot shed, because the cell has left
    // residency — and the take-in stops state spanning two epochs, which
    // corrupts rather than leaks. `__cell.release` lives in the harness
    // because what a residency owns is decided by the maps that hold it; this
    // side only says when.
    //
    // The release loses nothing the cell needs back: `register_actor_name`
    // persists the id name before writing it here, so the take-in below reads
    // it back.
    let source = format!("(() => {{ const s = {cell:?}; __cell.release(s); }})();");
    let code = v8::String::new(tc, &source).unwrap();
    let script = v8::Script::compile(tc, code, None).ok_or_else(|| anyhow!("adopt compile"))?;
    script.run(tc).ok_or_else(|| anyhow!("adopt run"))?;
    if !owned {
        storage::close(cell);
        return Ok(None);
    }
    let name = storage::get_actor_name(cell).context("read actor name")?;
    register_actor_name(tc, cell, name.as_deref())?;
    Ok(storage::get_alarm(cell))
}

pub(super) fn inject_storage_compatibility(scope: &mut v8::PinScope, compat: Compat) -> Result<()> {
    let source = format!(
        "__cell.deleteAllDeletesAlarm = {};\n\
         __cell.compat.jsRpc = {};\n\
         __cell.compat.fetcherGetPutDelete = {};\n\
         __cell.compat.websocketStandardBinaryType = {};\n\
         __cell.compat.queueJsonMessages = {};",
        compat.delete_all_deletes_alarm,
        compat.js_rpc,
        compat.fetcher_get_put_delete,
        compat.websocket_standard_binary_type,
        compat.queue_json_messages,
    );
    let code = v8::String::new(scope, &source).unwrap();
    let script =
        v8::Script::compile(scope, code, None).ok_or_else(|| anyhow!("compatibility compile"))?;
    script
        .run(scope)
        .ok_or_else(|| anyhow!("compatibility run"))?;
    Ok(())
}

/// `__cell.env`, the bindings object every handler receives as its second
/// argument.
///
/// This runs per request, so both keys come from the constant table. The
/// *value* stays a live lookup: unlike the event hooks, `env` is an ordinary
/// data property that a worker's own code can replace, and holding the object
/// this returns would make the host ignore that. The saving there is one
/// property read, so the correctness question decides it.
pub(super) fn harness_env<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<v8::Local<'s, v8::Value>> {
    let ctx = scope.get_current_context();
    let global = ctx.global(scope);
    let ck = static_key(scope, &v8_strings::CELL);
    let cell = global
        .get(scope, ck.into())
        .unwrap()
        .to_object(scope)
        .unwrap();
    let ek = static_key(scope, &v8_strings::ENV);
    Ok(cell.get(scope, ek.into()).unwrap())
}

pub(super) fn begin_event_context<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<v8::Local<'s, v8::Value>> {
    let function = event_hook(scope, |hooks| &hooks.begin_event)?;
    let recv = v8::undefined(scope).into();
    function
        .call(scope, recv, &[])
        .ok_or_else(|| anyhow!("__beginEvent threw"))
}

pub(super) fn end_event_context<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Result<Option<v8::Local<'s, v8::Promise>>> {
    let function = event_hook(scope, |hooks| &hooks.end_event)?;
    let recv = v8::undefined(scope).into();
    let value = function
        .call(scope, recv, &[])
        .ok_or_else(|| anyhow!("__endEvent threw"))?;
    if value.is_null_or_undefined() {
        Ok(None)
    } else {
        Ok(Some(value.try_into().map_err(|_| {
            anyhow!("__endEvent did not return a promise")
        })?))
    }
}

// ---- module compilation + request/response marshalling ----
