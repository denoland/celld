# WebAssembly

A Worker bundle can import a `.wasm` file. The import gives the
compiled module, not the bytes. This is the same rule that Wrangler
applies, so a bundle that runs on Cloudflare runs on celld without a
change.

```js
import addModule from "./add.wasm";

const { exports } = new WebAssembly.Instance(addModule);

export default {
  fetch() {
    return new Response(String(exports.add(2, 3)));
  },
};
```

`celld deploy` finds each wasm import, uploads the file beside the
bundle, and marks the deployment with the `wasm-v1` feature. A node
that predates this feature refuses the deployment with a clear
message, so a mixed fleet fails at deploy time and not at request
time.

celld compiles each wasm module once for the whole process. Every
isolate after the first one reuses the compiled module, so a cell
activation does not pay the compilation again.

## Example

The [WebAssembly example](../examples/wasm) compiles a Rust Durable Object and
imports its WebAssembly module.

<!-- celld-example: wasm -->

## Prebuilt Workers

With `no_bundle: true`, celld preserves the entry JavaScript byte for byte.
It applies Wrangler's default `**/*.wasm` and `**/*.wasm?module` patterns below
the directory that contains `main`. For example, `main: "./dist/shim.mjs"` can
import `"./add.wasm"` or `"./lib/add.wasm"` from `dist`. The module names keep
these relative paths. The files use the same WASM metadata and feature gate as
a bundled deploy.

The default scan includes WASM files that the JavaScript does not import. Use
a dedicated build output directory, so celld does not upload unrelated WASM.
The scan excludes `.git`, `.celld`, `.wrangler`, and symbolic links to
directories. It refuses a symbolic link whose name matches a WASM pattern.
Copy the WASM file into the build output instead of linking to it or importing
it from a parent directory.

This mode matches Wrangler's default WASM discovery, but it does not implement
all [Wrangler module discovery settings](https://developers.cloudflare.com/workers/wrangler/configuration/#find-additional-modules).
celld does not discover additional JavaScript modules and does not accept
`rules`, `base_dir`, or `find_additional_modules`. The JavaScript must already
be bundled, and its WASM imports must be relative to the entry directory. This
mode does not require an esbuild executable.

## Rust with workers-rs

[workers-rs](https://github.com/cloudflare/workers-rs) compiles a Rust
crate to a Worker. The tool `worker-build` produces a JavaScript shim
and a wasm file, and the shim is a normal entry point for
`celld deploy`.

1. Install the build tool: `cargo install worker-build`.
2. Build the crate: `worker-build --release`.
3. Point the config at the shim:

```jsonc
{
  "name": "my-app",
  "main": "./build/worker/shim.mjs",
  "compatibility_date": "2026-01-01",
}
```

4. Deploy: `celld deploy`.

The shim wraps its exports in a JavaScript Proxy, and celld resolves
entrypoint classes and Durable Object classes through that wrapper.
The runtime surface that the application can use is the surface in
[Cloudflare compatibility](cloudflare-compat.md); a workers-rs API
that maps to a missing runtime feature does not work.

## Dynamic Workers

A dynamically loaded worker can also carry wasm. Pass the bytes in the
`modules` map as `{ wasm: bytes }`, and the loaded worker imports a compiled
module. celld refuses bare bytes, as workerd does.

```js
const worker = env.loader.load({
  compatibilityDate: "2025-01-01",
  mainModule: "main.js",
  modules: {
    "main.js": `import m from "./add.wasm"; ...`,
    "add.wasm": { wasm: wasmBytes },
  },
});
```

## Limits

The wasm bytes count against the deployment size limits, exactly as
JavaScript modules do. A wasm module that does not compile fails the
importing module with a `WebAssembly.CompileError` that names the
file.
