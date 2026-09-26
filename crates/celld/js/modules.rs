// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! ES module resolution and compilation.
//!
//! A specifier is either a builtin, whose source this module holds, or an
//! import of another worker, which resolves through the loader. Most builtins
//! are lazy: a module compiles the first time something reads its global, so
//! a worker that never touches `node:zlib` never compiles it.
use super::*;

macro_rules! static_source {
    ($name:ident, $file:literal) => {
        static $name: v8::OneByteConst =
            v8::String::create_external_onebyte_const(include_bytes!($file));
    };
}

static_source!(NODE_ASSERT_SOURCE, "node_assert.js");
static_source!(NODE_TIMERS_SOURCE, "node_timers.js");
static_source!(NODE_TEST_SOURCE, "node_test.js");
static_source!(NODE_UTIL_SOURCE, "node_util.js");
static_source!(NODE_EVENTS_SOURCE, "node_events.js");
static_source!(NODE_OS_SOURCE, "node_os.js");
static_source!(NODE_PATH_SOURCE, "node_path.js");
static_source!(NODE_BUFFER_SOURCE, "node_buffer.js");
static_source!(NODE_CRYPTO_SOURCE, "node_crypto.js");
static_source!(NODE_ASYNC_HOOKS_SOURCE, "node_async_hooks.js");
static_source!(
    NODE_DIAGNOSTICS_CHANNEL_SOURCE,
    "node_diagnostics_channel.js"
);
static_source!(NODE_STREAM_SOURCE, "node_stream.js");
static_source!(
    IDENTITY_TRANSFORM_STREAM_SOURCE,
    "identity_transform_stream.js"
);
static_source!(COMPRESSION_STREAMS_SOURCE, "compression_streams.js");
static_source!(BYTE_STREAMS_SOURCE, "byte_streams.js");
static_source!(SET_IMMEDIATE_SOURCE, "set_immediate.js");
static_source!(URL_PATTERN_SOURCE, "url_pattern.js");

#[cfg(all(test, celld_internal_tests))]
static INTERNAL_MODULE_SOURCES: &[(&str, &v8::OneByteConst)] = &[
    ("node_assert.js", &NODE_ASSERT_SOURCE),
    ("node_timers.js", &NODE_TIMERS_SOURCE),
    ("node_test.js", &NODE_TEST_SOURCE),
    ("node_util.js", &NODE_UTIL_SOURCE),
    ("node_events.js", &NODE_EVENTS_SOURCE),
    ("node_os.js", &NODE_OS_SOURCE),
    ("node_path.js", &NODE_PATH_SOURCE),
    ("node_buffer.js", &NODE_BUFFER_SOURCE),
    ("node_crypto.js", &NODE_CRYPTO_SOURCE),
    ("node_async_hooks.js", &NODE_ASYNC_HOOKS_SOURCE),
    (
        "node_diagnostics_channel.js",
        &NODE_DIAGNOSTICS_CHANNEL_SOURCE,
    ),
    ("node_stream.js", &NODE_STREAM_SOURCE),
    (
        "identity_transform_stream.js",
        &IDENTITY_TRANSFORM_STREAM_SOURCE,
    ),
    ("compression_streams.js", &COMPRESSION_STREAMS_SOURCE),
    ("byte_streams.js", &BYTE_STREAMS_SOURCE),
    ("set_immediate.js", &SET_IMMEDIATE_SOURCE),
    ("url_pattern.js", &URL_PATTERN_SOURCE),
];

#[cfg(all(test, celld_internal_tests))]
pub(super) fn internal_sources_for_test<'s>(
    scope: &mut v8::PinScope<'s, '_>,
) -> Vec<(&'static str, v8::Local<'s, v8::String>)> {
    let mut sources = Vec::with_capacity(INTERNAL_MODULE_SOURCES.len());
    for &(name, source) in INTERNAL_MODULE_SOURCES {
        sources.push((name, v8_strings::source(scope, source)));
    }
    sources
}

pub(super) fn compile_module<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    name: &str,
    src: &str,
) -> Option<v8::Local<'s, v8::Module>> {
    let name = v8::String::new(scope, name)?;
    let source = v8::String::new(scope, src)?;
    let origin = v8::ScriptOrigin::new(
        scope,
        name.into(),
        0,
        0,
        false,
        0,
        None,
        false,
        false,
        true,
        None,
    );
    let mut src = v8::script_compiler::Source::new(source, Some(&origin));
    v8::script_compiler::compile_module(scope, &mut src)
}

/// specifier -> pre-compiled module. Plain keys serve static resolution, and
/// `dyn:` keys serve dynamic resolution and cache full-surface builtins.
///
/// **An isolate slot, not a thread-local.** These are `Global<Module>`
/// handles into *one* isolate's heap. Under D1 several isolates are built and
/// entered from the same tokio worker, so a thread-local made them share one
/// table: `register_stubs` clearing it for a new isolate wiped a live one's
/// stubs, and `dynamic_namespace` could localise a handle belonging to a
/// different isolate — not a wrong answer but an invalid one. The registry
/// belongs to the isolate because its contents do.
#[derive(Default)]
pub(super) struct ModuleRegistry {
    modules: Mutex<HashMap<String, v8::Global<v8::Module>>>,
    /// Canonical names for source modules. V8 passes only the referrer module
    /// to the static resolve callback, so the registry must retain the name
    /// that relative imports use as their base.
    source_names: Mutex<Vec<(v8::Global<v8::Module>, String)>>,
    /// The host-generated stubs: the only modules whose `celld:internals`
    /// import resolves. A user module that names the specifier is refused
    /// by [`resolve_external`], so the internals object stays out of reach
    /// of every bundle and every loaded worker.
    trusted: Mutex<Vec<v8::Global<v8::Module>>>,
    internals: Mutex<Option<v8::Global<v8::Module>>>,
}

impl ModuleRegistry {
    /// Register `module` under every spec in `specs`. `trusted` admits it to
    /// `celld:internals`, and is the only way in: every host-generated stub
    /// registers here, and a user module never passes `true`.
    fn register(
        &self,
        scope: &mut v8::PinScope,
        specs: impl IntoIterator<Item = String>,
        module: v8::Local<v8::Module>,
        trusted: bool,
        source_name: Option<&str>,
    ) {
        let global = v8::Global::new(scope, module);
        if trusted {
            self.trusted.lock().unwrap().push(global.clone());
        }
        if let Some(source_name) = source_name {
            self.source_names
                .lock()
                .unwrap()
                .push((global.clone(), source_name.to_string()));
        }
        let mut modules = self.modules.lock().unwrap();
        for spec in specs {
            modules.insert(spec, global.clone());
        }
    }

    fn source_name(
        &self,
        scope: &mut v8::PinScope,
        module: v8::Local<v8::Module>,
    ) -> Option<String> {
        self.source_names
            .lock()
            .unwrap()
            .iter()
            .find(|(registered, _)| v8::Local::new(scope, registered) == module)
            .map(|(_, name)| name.clone())
    }

    fn trusts(&self, scope: &mut v8::PinScope, module: v8::Local<v8::Module>) -> bool {
        self.trusted
            .lock()
            .unwrap()
            .iter()
            .any(|trusted| v8::Local::new(scope, trusted) == module)
    }
}

/// The specifier the stubs import the internals object from. It is not a
/// builtin: `is_external` does not know it, so a dynamic import() of it
/// rejects, and a static import from a user module fails to link.
const INTERNALS_SPECIFIER: &str = "celld:internals";

/// One synthetic module per isolate whose default export is the internals
/// object, built the first time a stub links.
fn internals_module<'s>(scope: &mut v8::PinScope<'s, '_>) -> v8::Local<'s, v8::Module> {
    let registry = modreg(scope);
    let cached = registry
        .internals
        .lock()
        .unwrap()
        .as_ref()
        .map(|module| v8::Local::new(scope, module));
    if let Some(module) = cached {
        return module;
    }
    let name = v8::String::new(scope, INTERNALS_SPECIFIER).unwrap();
    let default = v8::String::new(scope, "default").unwrap();
    let module =
        v8::Module::create_synthetic_module(scope, name, &[default], internals_evaluation_steps);
    *registry.internals.lock().unwrap() = Some(v8::Global::new(scope, module));
    module
}

fn internals_evaluation_steps<'s>(
    context: v8::Local<'s, v8::Context>,
    module: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Value>> {
    v8::callback_scope!(unsafe scope, context);
    let internals = internals(scope);
    let default = v8::String::new(scope, "default").unwrap();
    module.set_synthetic_module_export(scope, default, internals.into())?;
    Some(v8::undefined(scope).into())
}

/// Compile a host-generated stub that imports `celld:internals` and register
/// it under `spec` as a trusted referrer.
fn register_internal_stub(scope: &mut v8::PinScope, spec: &str, source: &str) {
    let Some(module) = compile_module(scope, spec, source) else {
        tracing::warn!(%spec, "module stub failed to compile");
        return;
    };
    modreg(scope).register(scope, [spec.to_string()], module, true, None);
}

fn modreg(scope: &mut v8::PinScope) -> Arc<ModuleRegistry> {
    scope
        .get_slot::<Arc<ModuleRegistry>>()
        .cloned()
        .expect("isolate has no module registry")
}

/// Is `spec` an external builtin (`node:*`, `cloudflare:*`, or a bare node
/// builtin)? esbuild bundles every npm dep inline and leaves only builtins
/// external, so a remaining bare specifier is a node builtin.
fn is_external(spec: &str) -> bool {
    if spec.starts_with("node:") || spec.starts_with("cloudflare:") {
        return true;
    }
    // bare builtin, or a builtin submodule like `stream/promises`
    BARE_NODE_BUILTINS.contains(&spec)
        || BARE_NODE_BUILTINS.contains(&spec.split('/').next().unwrap_or(""))
}

/// The import statement, and the brace group inside its clause.
///
/// `Regex::new` compiles the pattern to a program and builds the matcher
/// scaffolding around it, so it is far more expensive than the scan itself on
/// a small bundle. Both patterns were function locals, so an isolate paid that
/// cost once for the main module and once more for each sibling module of a
/// Worker Loader bundle — on every isolate build.
static IMPORT_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    // A minifier can join `import` to `{` or `*`, but a default binding must
    // keep whitespace. Preserve that token boundary so an identifier that
    // starts with `import` cannot look like an import statement.
    regex::Regex::new(r#"\bimport(\s+[^;]*?|\s*[{*][^;]*?)\s*from\s*["']([^"']+)["']"#).unwrap()
});
static IMPORT_BRACES_RE: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"\{([^}]*)\}").unwrap());

/// Counts scans of a source carrying [`SCAN_PROBE_MARKER`].
///
/// The counter is filtered by the marker rather than counting every scan, so a
/// bare global counter cannot move under an unrelated worker build.
#[cfg(celld_internal_tests)]
static IMPORT_SCANS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// A source puts this in a comment to have its scans counted.
#[cfg(celld_internal_tests)]
pub(super) const SCAN_PROBE_MARKER: &str = "celld-import-scan-probe";

/// How many times a source carrying [`SCAN_PROBE_MARKER`] has been scanned.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn import_scans() -> usize {
    IMPORT_SCANS.load(std::sync::atomic::Ordering::Relaxed)
}

/// External specifier -> the bindings a bundle pulls from it.
pub(super) type ExternalImports =
    std::collections::BTreeMap<String, std::collections::BTreeSet<String>>;

/// Scan a bundle for its external (`cloudflare:*` / `node:*`) imports and the
/// named/default bindings pulled from each. V8 static-links these, so a stub
/// must provide exactly these names; namespace imports (`* as x`) need none.
pub(super) fn scan_external_imports(src: &str) -> ExternalImports {
    #[cfg(celld_internal_tests)]
    if src.contains(SCAN_PROBE_MARKER) {
        IMPORT_SCANS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let mut map = ExternalImports::default();
    for cap in IMPORT_RE.captures_iter(src) {
        if !is_external(&cap[2]) {
            continue;
        }
        let clause = cap[1].trim();
        let set = map.entry(cap[2].to_string()).or_default();
        if let Some(b) = IMPORT_BRACES_RE.captures(clause) {
            for part in b[1].split(',') {
                let name = part.split_whitespace().next().unwrap_or("");
                // A commented-out binding inside the braces is not a name.
                if !name.is_empty() && !name.starts_with("//") {
                    set.insert(name.to_string());
                }
            }
        }
        let head = clause
            .split(['{', '*'])
            .next()
            .unwrap_or("")
            .trim()
            .trim_end_matches(',')
            .trim();
        if !head.is_empty() {
            set.insert("default".into());
        } // default import
          // `import * as x` consumes the whole namespace; the stub must then
          // export the module's full surface, not just the scanned names.
        if clause.contains('*') {
            set.insert("*".into());
        }
    }
    map
}

/// A stub ES module for `spec`. `cloudflare:workers` binds the real DO base
/// class from the harness; every other builtin routes to a pass-through proxy
/// so evaluation never crashes on an unsupported node/cf API.
/// A module Cells really implements in JS. Its source is injected into the
/// generated stub module, and `register_stubs` only builds stubs for
/// specifiers a bundle actually imports — so an isolate that never imports it
/// pays nothing at load. `global` is the internals property the source
/// defines and doubles as the redefinition guard when a bundle imports
/// several aliases.
struct LazyModule {
    specs: &'static [&'static str],
    global: &'static str,
    source: &'static v8::OneByteConst,
}

const LAZY_MODULES: &[LazyModule] = &[
    LazyModule {
        specs: &["assert", "node:assert"],
        global: "__assertModule",
        source: &NODE_ASSERT_SOURCE,
    },
    LazyModule {
        specs: &["assert/strict", "node:assert/strict"],
        global: "__assertStrictModule",
        source: &NODE_ASSERT_SOURCE,
    },
    LazyModule {
        specs: &["timers/promises", "node:timers/promises"],
        global: "__timersPromises",
        source: &NODE_TIMERS_SOURCE,
    },
    LazyModule {
        specs: &["test", "node:test", "node:test/reporters"],
        global: "__nodeTest",
        source: &NODE_TEST_SOURCE,
    },
    LazyModule {
        specs: &["util", "node:util"],
        global: "__utilModule",
        source: &NODE_UTIL_SOURCE,
    },
    // Same source, different namespace: node:util/types has the type
    // predicates as its named exports. The shared source defines both
    // globals, so either entry's guard covers the other.
    LazyModule {
        specs: &["util/types", "node:util/types"],
        global: "__utilTypesModule",
        source: &NODE_UTIL_SOURCE,
    },
    LazyModule {
        specs: &["events", "node:events"],
        global: "__eventsModule",
        source: &NODE_EVENTS_SOURCE,
    },
    LazyModule {
        specs: &["os", "node:os"],
        global: "__osModule",
        source: &NODE_OS_SOURCE,
    },
    LazyModule {
        specs: &["path", "node:path", "path/posix", "node:path/posix"],
        global: "__pathModule",
        source: &NODE_PATH_SOURCE,
    },
    // Same source; either entry's guard covers the other.
    LazyModule {
        specs: &["path/win32", "node:path/win32"],
        global: "__pathWin32Module",
        source: &NODE_PATH_SOURCE,
    },
    // Shares its source with the `Buffer` LAZY_GLOBALS entry; the script
    // is self-guarded, so whichever seam runs first wins and the other
    // reuses the same classes.
    LazyModule {
        specs: &["buffer", "node:buffer"],
        global: "__buffer",
        source: &NODE_BUFFER_SOURCE,
    },
    LazyModule {
        specs: &["crypto", "node:crypto"],
        global: "__cryptoModule",
        source: &NODE_CRYPTO_SOURCE,
    },
    LazyModule {
        specs: &["async_hooks", "node:async_hooks"],
        global: "__asyncHooksModule",
        source: &NODE_ASYNC_HOOKS_SOURCE,
    },
    LazyModule {
        specs: &["diagnostics_channel", "node:diagnostics_channel"],
        global: "__diagnosticsChannelModule",
        source: &NODE_DIAGNOSTICS_CHANNEL_SOURCE,
    },
    // One source defines the whole stream family; whichever entry runs
    // first, its guard covers the others.
    LazyModule {
        specs: &["stream", "node:stream"],
        global: "__streamModule",
        source: &NODE_STREAM_SOURCE,
    },
    LazyModule {
        specs: &["stream/promises", "node:stream/promises"],
        global: "__streamPromises",
        source: &NODE_STREAM_SOURCE,
    },
    LazyModule {
        specs: &["stream/web", "node:stream/web"],
        global: "__streamWeb",
        source: &NODE_STREAM_SOURCE,
    },
    LazyModule {
        specs: &["stream/consumers", "node:stream/consumers"],
        global: "__streamConsumers",
        source: &NODE_STREAM_SOURCE,
    },
];

/// A global whose definition only compiles when something reads the name.
/// One script may define several names; touching any of them installs them
/// all. A bundle that names none pays only the `SetLazyDataProperty` calls
/// at boot, so this is where a new global belongs unless the prelude has to
/// see it.
struct LazyGlobal {
    names: &'static [&'static str],
    source: &'static v8::OneByteConst,
}

const LAZY_GLOBALS: &[LazyGlobal] = &[
    LazyGlobal {
        names: &["IdentityTransformStream", "FixedLengthStream"],
        source: &IDENTITY_TRANSFORM_STREAM_SOURCE,
    },
    LazyGlobal {
        names: &["CompressionStream", "DecompressionStream"],
        source: &COMPRESSION_STREAMS_SOURCE,
    },
    LazyGlobal {
        names: &[
            "ReadableByteStreamController",
            "ReadableStreamBYOBReader",
            "ReadableStreamBYOBRequest",
        ],
        source: &BYTE_STREAMS_SOURCE,
    },
    LazyGlobal {
        names: &["Buffer"],
        source: &NODE_BUFFER_SOURCE,
    },
    LazyGlobal {
        names: &["setImmediate", "clearImmediate"],
        source: &SET_IMMEDIATE_SOURCE,
    },
    LazyGlobal {
        names: &["URLPattern"],
        source: &URL_PATTERN_SOURCE,
    },
];

/// Runs once per group, the first time one of its names is read. V8 then
/// replaces the accessor with the returned value, so the script never
/// compiles twice.
fn lazy_global_getter(
    scope: &mut v8::PinScope,
    name: v8::Local<v8::Name>,
    args: v8::PropertyCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let index = args.data().integer_value(scope).unwrap();
    let group = &LAZY_GLOBALS[index as usize];
    let code = v8_strings::source(scope, group.source);
    let arguments = internal_arguments(scope);
    let exports = super::bootstrap::run_internal_script(scope, None, code, &arguments).unwrap();
    let exports: v8::Local<v8::Object> = exports.try_into().unwrap();
    // The group's other names are still lazy; define them now or reading one
    // would compile the same script again. Not this one — V8 asserts the
    // property is still an accessor when the getter returns.
    let global = scope.get_current_context().global(scope);
    for other in group.names {
        let key = v8::String::new(scope, other).unwrap();
        if name.strict_equals(key.into()) {
            continue;
        }
        let value = exports.get(scope, key.into()).unwrap();
        global.create_data_property(scope, key.into(), value);
    }
    rv.set(exports.get(scope, name.into()).unwrap());
}

pub(super) fn install_lazy_globals(scope: &mut v8::PinScope) -> Result<()> {
    let global = scope.get_current_context().global(scope);
    for (i, group) in LAZY_GLOBALS.iter().enumerate() {
        let data = v8::Integer::new(scope, i as i32);
        for name in group.names {
            let key = v8::String::new(scope, name).unwrap();
            global.set_lazy_data_property_with_data(
                scope,
                key.into(),
                lazy_global_getter,
                data.into(),
                v8::PropertyAttribute::NONE,
                v8::SideEffectType::HasSideEffect,
                v8::SideEffectType::HasSideEffect,
            );
        }
    }
    Ok(())
}

/// Globals already installed by the src/js/harness for partially supported
/// builtins. These cost every isolate, so new modules belong in
/// `LAZY_MODULES` instead.
fn eager_module_global(spec: &str) -> Option<&'static str> {
    Some(match spec {
        "fs" | "node:fs" => "__fs",
        "fs/promises" | "node:fs/promises" => "__fsPromises",
        "zlib" | "node:zlib" => "__zlibModule",
        _ => return None,
    })
}

/// Materialize an imported builtin from its external source before its small
/// ES module wrapper links. The old wrapper concatenated the full setup text,
/// which turned a shared static source back into one owned string per isolate.
fn install_lazy_module(scope: &mut v8::PinScope, spec: &str) -> Option<&'static LazyModule> {
    let module = LAZY_MODULES
        .iter()
        .find(|module| module.specs.contains(&spec))?;
    if internal_value(scope, module.global).is_some() {
        return Some(module);
    }
    let code = v8_strings::source(scope, module.source);
    let arguments = internal_arguments(scope);
    super::bootstrap::run_internal_script(scope, None, code, &arguments).ok()?;
    Some(module)
}

fn stub_source(spec: &str, names: &std::collections::BTreeSet<String>) -> String {
    let cf = spec == "cloudflare:workers";
    let cf_sockets = spec == "cloudflare:sockets";
    // `cloudflare:workflows` is real, not a pass-through proxy: its one
    // export is `NonRetryableError`, and a proxy standing in for an Error
    // subclass would make `throw new NonRetryableError(...)` throw a value
    // the retry policy cannot recognize — the step would retry, silently.
    let cf_workflows = spec == "cloudflare:workflows";
    let lazy = LAZY_MODULES.iter().find(|m| m.specs.contains(&spec));
    let eager = eager_module_global(spec);
    let base = if cf {
        "__celld.__cf".to_string()
    } else if cf_sockets {
        "__celld.__cfSockets".to_string()
    } else if cf_workflows {
        "__celld.__cfWorkflows".to_string()
    } else if let Some(m) = lazy {
        format!("__celld.{}", m.global)
    } else if let Some(global) = eager {
        format!("__celld.{global}")
    } else {
        // Unsupported: a path-carrying stub whose property walks stay
        // inert but whose calls throw, so first use fails loudly with the
        // specifier in the message instead of silently passing through.
        format!(
            "__celld.__nodeStubFor({})",
            serde_json::to_string(spec).unwrap()
        )
    };
    let mut out = internals_import();
    for n in names {
        if n == "*" {
            continue; // namespace imports take the full-surface path
        } else if n == "default" {
            out.push_str(&format!("export default {base};\n"));
        } else {
            out.push_str(&format!("export const {n} = {base}.{n};\n"));
        }
    }
    out
}

/// The first line of every host-generated stub.
fn internals_import() -> String {
    format!("import __celld from {INTERNALS_SPECIFIER:?};\n")
}

/// Compile one stub module per external specifier into the isolate's module
/// registry. Run before
/// instantiating a real bundle; `resolve_external` then serves them.
pub(super) fn register_stubs(scope: &mut v8::PinScope, config: &WorkerConfig) {
    let registry = modreg(scope);
    registry.modules.lock().unwrap().clear();
    registry.trusted.lock().unwrap().clear();
    for (spec, names) in &config.main_imports {
        install_lazy_module(scope, spec);
        // `import * as x` binds the whole namespace, so the stub's exports
        // must be the module's full surface — probed from the backing
        // object, like dynamic import() — not just the scanned names.
        let s = if names.contains("*") {
            full_surface_source(scope, spec, names).unwrap_or_else(|| stub_source(spec, names))
        } else {
            stub_source(spec, names)
        };
        register_internal_stub(scope, spec, &s);
    }
    // sibling text modules (wrangler Text rule: `import md from './x.md'`)
    for (spec, source) in &config.modules {
        let ModuleSource::Text(content) = source else {
            continue;
        };
        let s = format!(
            "export default {};",
            serde_json::to_string(content).unwrap()
        );
        match compile_module(scope, spec, &s) {
            Some(m) => modreg(scope).register(scope, [spec.clone()], m, false, None),
            None => tracing::warn!(%spec, "text module failed to compile"),
        }
    }
    tracing::debug!(
        mods = modreg(scope).modules.lock().unwrap().len(),
        "registered import stubs + text modules"
    );
}

/// Compile `source` and insert it into the isolate's module registry under
/// both `name` and `./name`, so bare and relative sibling imports resolve to
/// one module.
fn register_sibling_module(scope: &mut v8::PinScope, name: &str, source: &str, trusted: bool) {
    let Some(m) = compile_module(scope, name, source) else {
        tracing::warn!(%name, "sibling module failed to compile");
        return;
    };
    register_named_module(scope, name, m, trusted);
}

fn register_named_module(
    scope: &mut v8::PinScope,
    name: &str,
    module: v8::Local<v8::Module>,
    trusted: bool,
) {
    // The dynamic key makes the same module available to import() without
    // making a narrow static builtin stub look like a full namespace.
    let specs = [name.to_string(), format!("./{name}")]
        .into_iter()
        .flat_map(|spec| [format!("dyn:{spec}"), spec]);
    modreg(scope).register(scope, specs, module, trusted, Some(name));
}

/// Compiled-wasm modules shared process-wide: the first isolate to see a blob
/// compiles it and caches the `CompiledWasmModule`; every later isolate (each
/// pool thread, each DO cell activation) rehydrates that without recompiling,
/// as workerd does via `FromCompiledModule`. Bounded LRU: deployed modules
/// are fixed at startup (a redeploy restarts the process), but the Worker
/// Loader can stream in distinct wasm blobs at runtime, so modules that
/// evicted dynamic workers leave behind age out instead of accumulating
/// for the life of the daemon.
#[derive(Default)]
struct CompiledWasmCache {
    entries: HashMap<[u8; 32], (v8::CompiledWasmModule, u64)>,
    tick: u64,
}

const MAX_COMPILED_WASM_MODULES: usize = 32;

impl CompiledWasmCache {
    fn get(&mut self, hash: &[u8; 32]) -> Option<&v8::CompiledWasmModule> {
        self.tick += 1;
        let tick = self.tick;
        self.entries.get_mut(hash).map(|(module, used)| {
            *used = tick;
            &*module
        })
    }

    fn insert(&mut self, hash: [u8; 32], module: v8::CompiledWasmModule) {
        if self.entries.len() >= MAX_COMPILED_WASM_MODULES {
            let lru = self
                .entries
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(hash, _)| *hash);
            if let Some(lru) = lru {
                self.entries.remove(&lru);
            }
        }
        self.tick += 1;
        self.entries.insert(hash, (module, self.tick));
    }
}

/// Compile wasm bytes behind a TryCatch (so invalid bytes cannot leave a
/// pending exception dangling over the rest of the load) and record the
/// compiled module in the shared cache for later isolates. The compile runs
/// outside the cache lock; only the insert takes it.
fn compile_wasm<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    bytes: &[u8],
    hash: [u8; 32],
    cache: &Mutex<CompiledWasmCache>,
) -> Result<v8::Local<'s, v8::WasmModuleObject>, String> {
    let compiled = {
        let tc = std::pin::pin!(v8::TryCatch::new(scope));
        let tcs = &mut tc.init();
        match v8::WasmModuleObject::compile(tcs, bytes) {
            Some(module) => {
                cache
                    .lock()
                    .unwrap()
                    .insert(hash, module.get_compiled_module());
                Ok(v8::Global::new(tcs, module))
            }
            None => Err(exc!(tcs)),
        }
    };
    compiled.map(|module| v8::Local::new(scope, &module))
}

/// Register each sibling wasm module under its name and `./name` as a stub
/// whose default export is the compiled `WebAssembly.Module` — the shape
/// workerd gives a `CompiledWasm` module import, which is what
/// wasm-bindgen/workers-rs bundles expect from `import x from "./x.wasm"`.
/// Each stub reads its compiled module from `__celld.__wasmModules`, and
/// its one-time evaluation consumes that entry.
pub(super) fn register_wasm_modules(scope: &mut v8::PinScope, modules: &[(String, ModuleSource)]) {
    static COMPILED: OnceLock<Mutex<CompiledWasmCache>> = OnceLock::new();
    let wasm = modules.iter().filter_map(|(name, source)| match source {
        ModuleSource::Wasm(bytes) => Some((name, bytes)),
        _ => None,
    });
    let mut table = None;
    let cache = COMPILED.get_or_init(Default::default);
    for (name, bytes) in wasm {
        let table = *table.get_or_insert_with(|| {
            let internals = internals(scope);
            let table = v8::Object::new(scope);
            let table_key = v8::String::new(scope, "__wasmModules").unwrap();
            internals.set(scope, table_key.into(), table.into());
            table
        });
        use sha2::Digest;
        let hash: [u8; 32] = sha2::Sha256::digest(bytes).into();
        // `CompiledWasmModule` is not Clone, so the rehydrate reads the cache
        // entry in place; `from_compiled_module` shares the compiled code
        // rather than recompiling, so the lock is held only briefly.
        let cached = {
            let mut cache = cache.lock().unwrap();
            cache
                .get(&hash)
                .and_then(|compiled| v8::WasmModuleObject::from_compiled_module(scope, compiled))
        };
        let module = match cached {
            Some(module) => Ok(module),
            None => compile_wasm(scope, bytes, hash, cache),
        };
        let quoted = serde_json::to_string(name).unwrap();
        let source = match module {
            Ok(module) => {
                let key = v8::String::new(scope, name).unwrap();
                table.set(scope, key.into(), module.into());
                let mut source = internals_import();
                source.push_str(&format!(
                    "const m = __celld.__wasmModules[{quoted}];\n\
                     delete __celld.__wasmModules[{quoted}];\n\
                     export default m;"
                ));
                source
            }
            // The importing module reports the failure, matching the eager
            // `new WebAssembly.Module(bytes)` stub this replaces.
            Err(error) => {
                tracing::warn!(%name, %error, "wasm module failed to compile");
                let message =
                    serde_json::to_string(&format!("{name} failed to compile: {error}")).unwrap();
                format!("throw new WebAssembly.CompileError({message});")
            }
        };
        register_sibling_module(scope, name, &source, true);
    }
}

/// Register a loaded worker's sibling JS modules (Worker Loader multi-module
/// bundles), in addition to the builtins `register_stubs` already added for the
/// main module. Each module is registered under its own name and `./name` so
/// both bare and relative sibling imports resolve; the whole graph links when
/// the main module instantiates. Any builtin a sibling imports (and the main
/// module did not) is stubbed too.
pub(super) fn register_loader_modules(
    scope: &mut v8::PinScope,
    config: &WorkerConfig,
    main: v8::Local<v8::Module>,
) {
    for (_name, _source, imports) in config.es_modules() {
        for (spec, names) in imports {
            if modreg(scope).modules.lock().unwrap().contains_key(spec) {
                continue;
            }
            install_lazy_module(scope, spec);
            let s = if names.contains("*") {
                full_surface_source(scope, spec, names).unwrap_or_else(|| stub_source(spec, names))
            } else {
                stub_source(spec, names)
            };
            register_internal_stub(scope, spec, &s);
        }
    }
    register_named_module(scope, &config.main_module_name, main, false);
    for (name, source, _imports) in config.es_modules() {
        register_sibling_module(scope, name, source, false);
    }
}

/// Module resolve callback: serve a registered stub for `cloudflare:*`/`node:*`.
/// celld bundles are single-file, so anything else is genuinely unresolvable.
pub(super) fn resolve_external<'s>(
    context: v8::Local<'s, v8::Context>,
    specifier: v8::Local<'s, v8::String>,
    _a: v8::Local<'s, v8::FixedArray>,
    referrer: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Module>> {
    v8::callback_scope!(unsafe scope, context);
    let spec = specifier.to_rust_string_lossy(scope);
    let registry = modreg(scope);
    if spec == INTERNALS_SPECIFIER {
        if registry.trusts(scope, referrer) {
            return Some(internals_module(scope));
        }
        // A user module named the stubs' private specifier. Refuse with a
        // message: the bare miss below reports no exception, and a link
        // error that says nothing sends the author looking for a typo.
        let message = v8::String::new(scope, "celld:internals is not importable").unwrap();
        let exception = v8::Exception::type_error(scope, message);
        scope.throw_exception(exception);
        return None;
    }
    let resolved = registry
        .source_name(scope, referrer)
        .and_then(|referrer| resolve_relative_specifier(&referrer, &spec));
    let key = resolved.as_deref().unwrap_or(&spec);
    let m = registry
        .modules
        .lock()
        .unwrap()
        .get(key)
        .map(|module| v8::Local::new(scope, module));
    if m.is_none() {
        tracing::warn!(%spec, resolved = key, "resolve: no stub for specifier");
    }
    m
}

/// Resolve a relative module specifier against the importing module's name.
/// Module names use URL-style `/` separators even on Windows, so filesystem
/// path utilities would give platform-dependent answers here.
fn resolve_relative_specifier(referrer: &str, spec: &str) -> Option<String> {
    if !spec.starts_with("./") && !spec.starts_with("../") {
        return None;
    }
    let absolute = referrer.starts_with('/');
    let mut segments: Vec<&str> = referrer
        .rsplit_once('/')
        .map_or("", |(parent, _)| parent)
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    for segment in spec.split('/') {
        match segment {
            "" | "." => {}
            ".." if segments.last().is_some_and(|segment| *segment != "..") => {
                segments.pop();
            }
            ".." if !absolute => segments.push(segment),
            ".." => {}
            _ => segments.push(segment),
        }
    }
    let path = segments.join("/");
    Some(if absolute { format!("/{path}") } else { path })
}

/// The backing-object expression for a builtin specifier, or `None` if `spec`
/// is not one. A lazy module is materialized from its external source first.
fn builtin_source(scope: &mut v8::PinScope, spec: &str) -> Option<String> {
    if spec == "cloudflare:workers" {
        return Some("__celld.__cf".into());
    }
    if spec == "cloudflare:sockets" {
        return Some("__celld.__cfSockets".into());
    }
    if spec == "cloudflare:workflows" {
        return Some("__celld.__cfWorkflows".into());
    }
    if let Some(module) = install_lazy_module(scope, spec) {
        return Some(format!("__celld.{}", module.global));
    }
    if let Some(global) = eager_module_global(spec) {
        return Some(format!("__celld.{global}"));
    }
    is_external(spec).then(|| {
        format!(
            "__celld.__nodeStubFor({})",
            serde_json::to_string(spec).unwrap()
        )
    })
}

/// Full-surface module source for a builtin: run the (guarded) setup, probe
/// the backing object's enumerable keys, and re-export each one. Serves both
/// `import * as x` stubs and dynamic import(). `extra` adds statically
/// scanned names the probe may have missed, so mixed named + namespace
/// imports still link. String export names sidestep identifier restrictions.
fn full_surface_source(
    scope: &mut v8::PinScope,
    spec: &str,
    extra: &std::collections::BTreeSet<String>,
) -> Option<String> {
    let expr = builtin_source(scope, spec)?;
    let probe = format!("return JSON.stringify(Object.keys(Object({expr})));");
    let keys = run_internal_snippet(scope, &probe)?.to_rust_string_lossy(scope);
    let mut names: Vec<String> = serde_json::from_str(&keys).unwrap_or_default();
    for name in extra {
        if name != "*" && !names.contains(name) {
            names.push(name.clone());
        }
    }
    let mut src = internals_import();
    src.push_str(&format!("const __b = {expr};\nexport default __b;\n"));
    for (i, name) in names.iter().enumerate() {
        if name == "default" {
            continue;
        }
        let name = serde_json::to_string(name).unwrap();
        src.push_str(&format!(
            "const __e{i} = __b[{name}]; export {{ __e{i} as {name} }};\n"
        ));
    }
    Some(src)
}

/// `process.getBuiltinModule(id)`: the builtin's backing object, or
/// undefined for anything that is not a node builtin.
pub(super) fn op_builtin_module(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let spec = args.get(0).to_rust_string_lossy(scope);
    if spec.starts_with("cloudflare:") {
        return; // not a node builtin
    }
    let Some(expr) = builtin_source(scope, &spec) else {
        return;
    };
    if let Some(value) = run_internal_snippet(scope, &format!("return {expr};")) {
        rv.set(value);
    }
}

/// Return a registered sibling, or compile a full-namespace builtin once per
/// isolate. A `dyn:` key distinguishes both from a narrow static builtin stub.
fn dynamic_module<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    spec: &str,
) -> Result<v8::Local<'s, v8::Module>> {
    let key = format!("dyn:{spec}");
    let registry = modreg(scope);
    let cached = registry
        .modules
        .lock()
        .unwrap()
        .get(&key)
        .map(|module| v8::Local::new(scope, module));
    let module = match cached {
        Some(module) => module,
        None => {
            let src = full_surface_source(scope, spec, &Default::default())
                .ok_or_else(|| anyhow!("dynamic import of \"{spec}\" is not supported"))?;
            let module = compile_module(scope, spec, &src)
                .ok_or_else(|| anyhow!("dynamic module for {spec} did not compile"))?;
            modreg(scope).register(scope, [key], module, true, None);
            module
        }
    };
    if module.get_status() == v8::ModuleStatus::Uninstantiated {
        module
            .instantiate_module(scope, resolve_external)
            .filter(|linked| *linked)
            .ok_or_else(|| anyhow!("dynamic module for {spec} did not link"))?;
    }
    Ok(module)
}

fn return_callback_data(
    _scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    rv.set(args.data());
}

/// Evaluate a dynamic module and translate V8's evaluation promise into the
/// namespace promise that import() requires. The chain preserves a top-level
/// await delay and forwards its rejection.
fn evaluate_dynamic_module<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    spec: &str,
) -> Result<v8::Local<'s, v8::Promise>> {
    let module = dynamic_module(scope, spec)?;
    let evaluated = module
        .evaluate(scope)
        .ok_or_else(|| anyhow!("dynamic module for {spec} threw"))?
        .try_cast::<v8::Promise>()
        .map_err(|_| anyhow!("dynamic module for {spec} returned no promise"))?;
    let namespace = module.get_module_namespace();
    let return_namespace = v8::Function::builder(return_callback_data)
        .data(namespace)
        .build(scope)
        .ok_or_else(|| anyhow!("dynamic module for {spec} created no namespace callback"))?;
    evaluated
        .then(scope, return_namespace)
        .ok_or_else(|| anyhow!("dynamic module for {spec} created no namespace promise"))
}

/// Dynamic `import()` host hook. A loaded Worker's registered siblings resolve
/// before builtins, so the lookup cannot escape into another Worker registry.
/// Anything absent from both sources rejects.
pub(super) fn host_import_module_dynamically<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _host_defined_options: v8::Local<'s, v8::Data>,
    resource_name: v8::Local<'s, v8::Value>,
    specifier: v8::Local<'s, v8::String>,
    _import_attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<v8::Local<'s, v8::Promise>> {
    let resolver = v8::PromiseResolver::new(scope)?;
    let promise = resolver.get_promise(scope);
    let spec = specifier.to_rust_string_lossy(scope);
    let referrer = resource_name.to_rust_string_lossy(scope);
    let resolved = resolve_relative_specifier(&referrer, &spec);
    let spec = resolved.as_deref().unwrap_or(&spec);
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let tc = &mut tc.init();
    match evaluate_dynamic_module(tc, spec) {
        Ok(import) => return Some(import),
        Err(error) => {
            let caught = tc.exception();
            tc.reset();
            let exception = caught.unwrap_or_else(|| {
                let message = v8::String::new(tc, &error.to_string()).unwrap();
                v8::Exception::error(tc, message)
            });
            resolver.reject(tc, exception);
        }
    }
    Some(promise)
}
