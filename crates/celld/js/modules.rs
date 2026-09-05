// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! ES module resolution and compilation.
//!
//! A specifier is either a builtin, whose source this module holds, or an
//! import of another worker, which resolves through the loader. Most builtins
//! are lazy: a module compiles the first time something reads its global, so
//! a worker that never touches `node:zlib` never compiles it.
use super::*;

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

/// specifier -> pre-compiled stub module, consulted by `resolve_external`
/// while V8 statically links a real worker bundle's imports, and by
/// `dynamic_namespace` as its once-per-isolate cache.
///
/// **An isolate slot, not a thread-local.** These are `Global<Module>`
/// handles into *one* isolate's heap. Under D1 several isolates are built and
/// entered from the same tokio worker, so a thread-local made them share one
/// table: `register_stubs` clearing it for a new isolate wiped a live one's
/// stubs, and `dynamic_namespace` could localise a handle belonging to a
/// different isolate — not a wrong answer but an invalid one. The registry
/// belongs to the isolate because its contents do.
#[derive(Default)]
pub(super) struct ModuleRegistry(Mutex<HashMap<String, v8::Global<v8::Module>>>);

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
    regex::Regex::new(r#"import\s+([^;]*?)\s*from\s*["']([^"']+)["']"#).unwrap()
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
/// pays nothing at load. `global` is what the source defines and doubles as
/// the redefinition guard when a bundle imports several aliases.
struct LazyModule {
    specs: &'static [&'static str],
    global: &'static str,
    source: &'static str,
}

const LAZY_MODULES: &[LazyModule] = &[
    LazyModule {
        specs: &["assert", "node:assert"],
        global: "__assertModule",
        source: include_str!("node_assert.js"),
    },
    LazyModule {
        specs: &["assert/strict", "node:assert/strict"],
        global: "__assertStrictModule",
        source: include_str!("node_assert.js"),
    },
    LazyModule {
        specs: &["timers/promises", "node:timers/promises"],
        global: "__timersPromises",
        source: include_str!("node_timers.js"),
    },
    LazyModule {
        specs: &["test", "node:test", "node:test/reporters"],
        global: "__nodeTest",
        source: include_str!("node_test.js"),
    },
    LazyModule {
        specs: &["util", "node:util"],
        global: "__utilModule",
        source: include_str!("node_util.js"),
    },
    // Same source, different namespace: node:util/types has the type
    // predicates as its named exports. The shared source defines both
    // globals, so either entry's guard covers the other.
    LazyModule {
        specs: &["util/types", "node:util/types"],
        global: "__utilTypesModule",
        source: include_str!("node_util.js"),
    },
    LazyModule {
        specs: &["events", "node:events"],
        global: "__eventsModule",
        source: include_str!("node_events.js"),
    },
    LazyModule {
        specs: &["os", "node:os"],
        global: "__osModule",
        source: include_str!("node_os.js"),
    },
    LazyModule {
        specs: &["path", "node:path", "path/posix", "node:path/posix"],
        global: "__pathModule",
        source: include_str!("node_path.js"),
    },
    // Same source; either entry's guard covers the other.
    LazyModule {
        specs: &["path/win32", "node:path/win32"],
        global: "__pathWin32Module",
        source: include_str!("node_path.js"),
    },
    // Shares its source with the `Buffer` LAZY_GLOBALS entry; the script
    // is self-guarded, so whichever seam runs first wins and the other
    // reuses the same classes.
    LazyModule {
        specs: &["buffer", "node:buffer"],
        global: "__buffer",
        source: include_str!("node_buffer.js"),
    },
    LazyModule {
        specs: &["crypto", "node:crypto"],
        global: "__cryptoModule",
        source: include_str!("node_crypto.js"),
    },
    LazyModule {
        specs: &["async_hooks", "node:async_hooks"],
        global: "__asyncHooksModule",
        source: include_str!("node_async_hooks.js"),
    },
    LazyModule {
        specs: &["diagnostics_channel", "node:diagnostics_channel"],
        global: "__diagnosticsChannelModule",
        source: include_str!("node_diagnostics_channel.js"),
    },
    // One source defines the whole stream family; whichever entry runs
    // first, its guard covers the others.
    LazyModule {
        specs: &["stream", "node:stream"],
        global: "__streamModule",
        source: include_str!("node_stream.js"),
    },
    LazyModule {
        specs: &["stream/promises", "node:stream/promises"],
        global: "__streamPromises",
        source: include_str!("node_stream.js"),
    },
    LazyModule {
        specs: &["stream/web", "node:stream/web"],
        global: "__streamWeb",
        source: include_str!("node_stream.js"),
    },
    LazyModule {
        specs: &["stream/consumers", "node:stream/consumers"],
        global: "__streamConsumers",
        source: include_str!("node_stream.js"),
    },
];

/// A global whose definition only compiles when something reads the name.
/// One script may define several names; touching any of them installs them
/// all. A bundle that names none pays only the `SetLazyDataProperty` calls
/// at boot, so this is where a new global belongs unless the prelude has to
/// see it.
struct LazyGlobal {
    names: &'static [&'static str],
    source: &'static str,
}

const LAZY_GLOBALS: &[LazyGlobal] = &[
    LazyGlobal {
        names: &["IdentityTransformStream", "FixedLengthStream"],
        source: include_str!("identity_transform_stream.js"),
    },
    LazyGlobal {
        names: &["CompressionStream", "DecompressionStream"],
        source: include_str!("compression_streams.js"),
    },
    LazyGlobal {
        names: &[
            "ReadableByteStreamController",
            "ReadableStreamBYOBReader",
            "ReadableStreamBYOBRequest",
        ],
        source: include_str!("byte_streams.js"),
    },
    LazyGlobal {
        names: &["Buffer"],
        source: include_str!("node_buffer.js"),
    },
    LazyGlobal {
        names: &["setImmediate", "clearImmediate"],
        source: include_str!("set_immediate.js"),
    },
    LazyGlobal {
        names: &["URLPattern"],
        source: include_str!("url_pattern.js"),
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
    let code = v8::String::new(scope, group.source).unwrap();
    let script = v8::Script::compile(scope, code, None).unwrap();
    let exports = script.run(scope).unwrap();
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
        "fs" | "node:fs" => "globalThis.__fs",
        "fs/promises" | "node:fs/promises" => "globalThis.__fsPromises",
        "zlib" | "node:zlib" => "globalThis.__zlibModule",
        _ => return None,
    })
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
        "globalThis.__cf".to_string()
    } else if cf_sockets {
        "globalThis.__cfSockets".to_string()
    } else if cf_workflows {
        "globalThis.__cfWorkflows".to_string()
    } else if let Some(m) = lazy {
        format!("globalThis.{}", m.global)
    } else if let Some(global) = eager {
        global.to_string()
    } else {
        // Unsupported: a path-carrying stub whose property walks stay
        // inert but whose calls throw, so first use fails loudly with the
        // specifier in the message instead of silently passing through.
        format!(
            "globalThis.__nodeStubFor({})",
            serde_json::to_string(spec).unwrap()
        )
    };
    let mut out = String::new();
    if let Some(m) = lazy {
        out.push_str(&format!("if (!globalThis.{}) {{\n", m.global));
        out.push_str(m.source);
        out.push_str("}\n");
    }
    for n in names {
        if n == "*" {
            continue; // namespace imports take the full-surface path
        } else if n == "default" {
            out.push_str(&format!("export default {base};\n"));
        } else if cf
            && matches!(
                n.as_str(),
                "DurableObject" | "RpcTarget" | "WorkerEntrypoint" | "env" | "exports"
            )
        {
            out.push_str(&format!("export const {n} = globalThis.__cf.{n};\n"));
        } else {
            out.push_str(&format!("export const {n} = {base}.{n};\n"));
        }
    }
    out
}

/// Compile one stub module per external specifier into the isolate's module
/// registry. Run before
/// instantiating a real bundle; `resolve_external` then serves them.
pub(super) fn register_stubs(scope: &mut v8::PinScope, config: &WorkerConfig) {
    modreg(scope).0.lock().unwrap().clear();
    let reg = |spec: String, source: String, scope: &mut v8::PinScope| {
        if let Some(m) = compile_module(scope, &spec, &source) {
            let g = v8::Global::new(scope, m);
            modreg(scope).0.lock().unwrap().insert(spec, g);
        } else {
            tracing::warn!(%spec, "module stub failed to compile");
        }
    };
    for (spec, names) in &config.main_imports {
        // `import * as x` binds the whole namespace, so the stub's exports
        // must be the module's full surface — probed from the backing
        // object, like dynamic import() — not just the scanned names.
        let s = if names.contains("*") {
            full_surface_source(scope, spec, names).unwrap_or_else(|| stub_source(spec, names))
        } else {
            stub_source(spec, names)
        };
        reg(spec.clone(), s, scope);
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
        reg(spec.clone(), s, scope);
    }
    tracing::debug!(
        mods = modreg(scope).0.lock().unwrap().len(),
        "registered import stubs + text modules"
    );
}

/// Compile `source` and insert it into the isolate's module registry under
/// both `name` and `./name`, so bare and relative sibling imports resolve to
/// one module.
fn register_sibling_module(scope: &mut v8::PinScope, name: &str, source: &str) {
    let Some(m) = compile_module(scope, name, source) else {
        tracing::warn!(%name, "sibling module failed to compile");
        return;
    };
    let g = v8::Global::new(scope, m);
    let registry = modreg(scope);
    let mut reg = registry.0.lock().unwrap();
    reg.insert(name.to_string(), g.clone());
    reg.insert(format!("./{name}"), g);
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
/// Each stub reads its compiled module from `globalThis.__wasmModules`, and
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
            // Non-enumerable, like every host-injected global (see the
            // `ops!` macro): a bundle walking `globalThis` must not find
            // runtime internals.
            let global = scope.get_current_context().global(scope);
            let table = v8::Object::new(scope);
            let table_key = v8::String::new(scope, "__wasmModules").unwrap();
            global.define_own_property(
                scope,
                table_key.into(),
                table.into(),
                v8::PropertyAttribute::DONT_ENUM,
            );
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
                format!(
                    "const m = globalThis.__wasmModules[{quoted}];\n\
                     delete globalThis.__wasmModules[{quoted}];\n\
                     export default m;"
                )
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
        register_sibling_module(scope, name, &source);
    }
}

/// Register a loaded worker's sibling JS modules (Worker Loader multi-module
/// bundles), in addition to the builtins `register_stubs` already added for the
/// main module. Each module is registered under its own name and `./name` so
/// both bare and relative sibling imports resolve; the whole graph links when
/// the main module instantiates. Any builtin a sibling imports (and the main
/// module did not) is stubbed too.
pub(super) fn register_loader_modules(scope: &mut v8::PinScope, config: &WorkerConfig) {
    for (_name, _source, imports) in config.es_modules() {
        for (spec, names) in imports {
            if modreg(scope).0.lock().unwrap().contains_key(spec) {
                continue;
            }
            let s = if names.contains("*") {
                full_surface_source(scope, spec, names).unwrap_or_else(|| stub_source(spec, names))
            } else {
                stub_source(spec, names)
            };
            if let Some(m) = compile_module(scope, spec, &s) {
                let g = v8::Global::new(scope, m);
                modreg(scope).0.lock().unwrap().insert(spec.clone(), g);
            }
        }
    }
    for (name, source, _imports) in config.es_modules() {
        register_sibling_module(scope, name, source);
    }
}

/// Module resolve callback: serve a registered stub for `cloudflare:*`/`node:*`.
/// celld bundles are single-file, so anything else is genuinely unresolvable.
pub(super) fn resolve_external<'s>(
    context: v8::Local<'s, v8::Context>,
    specifier: v8::Local<'s, v8::String>,
    _a: v8::Local<'s, v8::FixedArray>,
    _r: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Module>> {
    v8::callback_scope!(unsafe scope, context);
    let spec = specifier.to_rust_string_lossy(scope);
    let registry = modreg(scope);
    let m = registry
        .0
        .lock()
        .unwrap()
        .get(&spec)
        .map(|g| v8::Local::new(scope, g));
    if m.is_none() {
        tracing::warn!(%spec, "resolve: no stub for specifier");
    }
    m
}

/// The (guarded setup script, backing-object expression) pair for a builtin
/// specifier, or None if `spec` is not one. The setup materializes a lazy
/// module's source at most once; the expression then names its global.
fn builtin_source(spec: &str) -> Option<(String, String)> {
    if spec == "cloudflare:workers" {
        return Some((String::new(), "globalThis.__cf".into()));
    }
    if spec == "cloudflare:sockets" {
        return Some((String::new(), "globalThis.__cfSockets".into()));
    }
    if spec == "cloudflare:workflows" {
        return Some((String::new(), "globalThis.__cfWorkflows".into()));
    }
    if let Some(m) = LAZY_MODULES.iter().find(|m| m.specs.contains(&spec)) {
        return Some((
            format!("if (!globalThis.{}) {{\n{}}}\n", m.global, m.source),
            format!("globalThis.{}", m.global),
        ));
    }
    if let Some(global) = eager_module_global(spec) {
        return Some((String::new(), global.to_string()));
    }
    is_external(spec).then(|| {
        let stub = format!(
            "globalThis.__nodeStubFor({})",
            serde_json::to_string(spec).unwrap()
        );
        (String::new(), stub)
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
    let (setup, expr) = builtin_source(spec)?;
    let probe = format!("{setup}JSON.stringify(Object.keys(Object({expr})))");
    let code = v8::String::new(scope, &probe)?;
    let script = v8::Script::compile(scope, code, None)?;
    let keys = script.run(scope)?.to_rust_string_lossy(scope);
    let mut names: Vec<String> = serde_json::from_str(&keys).unwrap_or_default();
    for name in extra {
        if name != "*" && !names.contains(name) {
            names.push(name.clone());
        }
    }
    let mut src = format!("const __b = {expr};\nexport default __b;\n");
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
    let Some((setup, expr)) = builtin_source(&spec) else {
        return;
    };
    let code = v8::String::new(scope, &format!("{setup}{expr}")).unwrap();
    if let Some(script) = v8::Script::compile(scope, code, None) {
        if let Some(value) = script.run(scope) {
            rv.set(value);
        }
    }
}

/// Evaluate (once per isolate) a full-namespace module for a builtin
/// specifier. Unlike the static stubs, whose exports are the statically
/// scanned import names, this re-exports every enumerable key of the
/// backing object — a dynamic import's consumer can reach for any of them.
/// Cached in the isolate's module registry under a `dyn:` key so later
/// import() calls are a
/// table lookup.
fn dynamic_namespace<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    spec: &str,
) -> Result<v8::Local<'s, v8::Value>> {
    let key = format!("dyn:{spec}");
    let registry = modreg(scope);
    let cached = registry
        .0
        .lock()
        .unwrap()
        .get(&key)
        .map(|g| v8::Local::new(scope, g));
    if let Some(module) = cached {
        return Ok(module.get_module_namespace());
    }
    let src = full_surface_source(scope, spec, &Default::default())
        .ok_or_else(|| anyhow!("dynamic import of \"{spec}\" is not supported"))?;
    let module = compile_module(scope, spec, &src)
        .ok_or_else(|| anyhow!("dynamic module for {spec} did not compile"))?;
    module
        .instantiate_module(scope, resolve_external)
        .ok_or_else(|| anyhow!("dynamic module for {spec} did not link"))?;
    module
        .evaluate(scope)
        .ok_or_else(|| anyhow!("dynamic module for {spec} threw"))?;
    if module.get_status() != v8::ModuleStatus::Evaluated {
        return Err(anyhow!("dynamic module for {spec} failed to evaluate"));
    }
    let g = v8::Global::new(scope, module);
    modreg(scope).0.lock().unwrap().insert(key, g);
    Ok(module.get_module_namespace())
}

/// Dynamic `import()` host hook. Builtin specifiers resolve through the same
/// table static imports use; anything else rejects — celld bundles are
/// single-file, so there is nothing else to load.
pub(super) fn host_import_module_dynamically<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    _host_defined_options: v8::Local<'s, v8::Data>,
    _resource_name: v8::Local<'s, v8::Value>,
    specifier: v8::Local<'s, v8::String>,
    _import_attributes: v8::Local<'s, v8::FixedArray>,
) -> Option<v8::Local<'s, v8::Promise>> {
    let resolver = v8::PromiseResolver::new(scope)?;
    let promise = resolver.get_promise(scope);
    let spec = specifier.to_rust_string_lossy(scope);
    let tc = std::pin::pin!(v8::TryCatch::new(scope));
    let tc = &mut tc.init();
    match dynamic_namespace(tc, &spec) {
        Ok(namespace) => {
            resolver.resolve(tc, namespace);
        }
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
