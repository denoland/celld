# Cloudflare compatibility

celld implements the Cloudflare Workers APIs that this page links to. The notes
under each API list only the celld gaps and differences.

- **Yes** means that an application that uses the API runs on celld. The notes
  list each difference from Cloudflare and each method that is not available.
- **Partial** means that a large part of the API is missing, so an application
  that depends on that part does not run.
- **Experimental** means that the API needs an opt-in setting, and it can change
  without notice.
- **No** means that celld does not implement the API.

celld must reject an unsupported configuration or API at deployment or first
use. An unsupported feature that does not cause an error is a defect. This page
identifies each known exception.

## Services

| service | status |
| --- | --- |
| [Workers](#workers) | **Yes** |
| [Durable Objects](#durable-objects) | **Yes** |
| [Durable Object Facets](#durable-object-facets) | **Experimental** |
| [Static assets](#static-assets) | **Yes** |
| [Cron Triggers](#cron-triggers) | **Yes** |
| [Dynamic Workers](#dynamic-workers) | **Experimental** |
| [KV](#kv) | **Yes** |
| [Queues](#queues) | **Yes** |
| [D1](#d1) | **Yes** |
| [Workflows](#workflows) | **Yes** |
| [R2](#r2) | **Yes** |
| Workers AI | **No** |
| Vectorize | **No** |
| Hyperdrive | **No** |
| Browser Rendering | **No** |
| Email Workers | **No** |
| Python Workers | **No** |

### [Workers](https://developers.cloudflare.com/workers/runtime-apis/)

- The [runtime API table](#runtime-apis) lists the Worker runtime gaps.
- celld does not manage a custom domain or terminate TLS. Terminate TLS in the
  ingress proxy.
- An outbound `fetch` accepts an `AbortSignal` in its options. An abort rejects
  the call with the reason of the signal, and it cancels the request that is in
  flight, so the destination gets a disconnect. celld reads only a signal that
  the caller supplies. An incoming request carries its own signal for the
  disconnect of the client, and a subrequest does not inherit it.
- celld does not host a model, so Workers AI is not available. An experimental
  HTTP adapter lets an application that calls `env.AI.run()` deploy unchanged:
  with `CELLD_AI_URL` set, the `run` method posts the model and the input as
  JSON to that URL. The `run` method takes a third options argument.
  `returnRawResponse: true` gives back the unconsumed upstream `Response`, so
  an application can read a streaming completion chunk by chunk. The raw option gives back the `Response`
  for an error status too, so the application must test `ok` or `status`
  itself. The default parses the upstream body as JSON, and it throws
  an `Error` for a status that is not 2xx. The message of that `Error` gives
  the status and the first 512 characters of the upstream body, so an
  application can see the cause. A `signal` option cancels the request
  through the same `fetch` mechanism, and it also cancels a response body that
  the application is reading.

### [Durable Objects](https://developers.cloudflare.com/durable-objects/)

- An RPC stub cannot cross an isolate boundary. See [RPC](#rpc).
- An outbound WebSocket does not continue after the object moves to another
  node.
- celld refuses invalid UTF-8 from a SQLite `TEXT` value. Store arbitrary
  bytes in a `BLOB`.
- `SqlStorage.Cursor.toArray()` gives a celld-specific error when the isolate
  is near its V8 heap limit.
- `storage.sync()` resolves only after the object store or the fleet ensemble
  holds every write that the object committed before the call. The promise
  rejects when celld cannot prove the writes durable before the shorter of
  the durability budget (`CELLD_LTX_DURABILITY_TIMEOUT_SECS`, default: 10
  seconds, counted from the start of the write's upload) and
  `CELLD_OPERATION_DEADLINE_MS` (default: 15000 milliseconds). celld then
  resets the object and restores the proven history. The reset can stop the
  object before the handler observes the rejection, so an application must not
  depend on the rejection to recover.
- `storage.sync()` rejects while a `transaction()` is open on the object,
  because the transaction is not committed and no proof can cover its writes.
  Call `sync()` after the transaction commits. A `sync()` promise that a
  `transactionSync()` callback creates settles after the callback returns, so
  it covers the committed transaction. If the callback rolls back, an existing
  `sync()` promise resolves after the rollback without preserving canceled writes.
- `storage.sync()` rejects after `ctx.abort()` or a failed
  `blockConcurrencyWhile()` aborted the object.
- A transaction handle rejects new storage operations after the transaction
  commits or rolls back. Calling `rollback()` again after a rollback has no
  effect. A rollback also cancels writes submitted without an `await` inside
  that transaction. A nested rollback preserves the outer transaction's writes.
- A callback that finishes after an object abort cannot release a newer
  critical section's input gate or stop the node with a stale gate release.
- A `transaction()` callback runs under the input gate, as in Workerd: no
  other event starts in the object until the callback returns. celld does not
  hold an event that was already waiting on a subrequest or a timer when the
  callback started. That event resumes during the callback, and its writes
  join the open transaction. A callback that runs for more than 30 seconds
  resets the object and rolls the transaction back, because the limit of
  `blockConcurrencyWhile()` applies to it. A callback that throws rolls the
  transaction back and rejects, and the object continues. A block that
  starts nested blocks and does not await them holds the gate until they
  settle, each within its own 30 seconds, as nested critical sections do in
  Workerd.
- A `blockConcurrencyWhile()` callback continues after the client of its
  request disconnects. The object serves the next event when the callback
  settles.
- With `CELLD_OUTPUT_GATE=0`, or with no object store configured,
  `storage.sync()` resolves after the local commit and does not wait for a
  durability proof.
- A handler that fails after it writes answers only after the write is
  durable. celld holds the error behind the same output gate as a success,
  because the write is committed and a later request can read it. This rule
  applies to a `fetch` or RPC handler that throws or returns an invalid
  response, to a `webSocketMessage`, `webSocketOpen`, or `webSocketClose`
  handler, and to an `alarm` handler. When celld cannot prove the write durable
  before the shorter of `CELLD_LTX_DURABILITY_TIMEOUT_SECS` and
  `CELLD_OPERATION_DEADLINE_MS`, the request fails with a durability error and
  celld resets the object. With `CELLD_OUTPUT_GATE=0`, the error leaves after
  the local commit.
- A handler that fails without a write of its own answers only after every
  write that the object holds is durable. The message of a handler that throws
  can carry a value that the handler read, so celld holds the error as it holds
  a read-only response. A failure that celld raises, and not the handler — a
  handler over its time budget, or a handler that waits on nothing — carries no
  value from the object, so it leaves at once.

### [Static assets](https://developers.cloudflare.com/workers/static-assets/)

- celld does not compress an asset response. Put a compressing proxy in front
  of the fleet when a client needs gzip or brotli.
- celld has no edge cache. A node downloads an asset from the bucket on the
  first request, and it keeps a local disk cache of 512 MiB
  (`CELLD_ASSET_CACHE_BYTES`). Each response carries an `etag` and
  `cache-control: public, max-age=0, must-revalidate`, so a browser
  revalidates with `If-None-Match` and gets `304`.
- A node that has not restarted after a new deployment serves the new
  deployment's content-hashed assets. celld checks the deployment pointer
  after an asset miss, at most one time in 5 seconds.
- A `_headers` file cannot set or remove `connection`, `content-length`, or
  `transfer-encoding`.
- celld enforces the Cloudflare rule limits at deployment, so a `_headers`
  file with more than 100 rules or a `_redirects` file with more than 2,000
  static or 100 dynamic rules stops the deploy. Each directive file has a
  100 KiB limit.
- `celld deploy` does not read `.assetsignore`, and a project that contains
  one must deploy with Wrangler. The command refuses a symbolic link, a
  special file, a non-UTF-8 name, a path above 1024 bytes, a path that
  changes under percent-decoding, and a `_worker.js` entry.
- A deployment can contain at most 20,000 assets, 25 MiB for each asset, and
  1 GiB in total.

### [Cron Triggers](https://developers.cloudflare.com/workers/configuration/cron-triggers/)

- celld rejects a descending range such as `SAT-SUN` or `NOV-FEB`.
- celld rejects `*` inside a list such as `1,*`.
- celld runs one handler for each occurrence across the complete fleet.
- After fleet downtime, celld runs the most recent missed occurrence one time.
- celld runs one handler at a time for each script. It retries a failed handler
  until the next occurrence unless the handler calls `noRetry()`.
- A service-binding target cannot run its own Cron Triggers.

### [Dynamic Workers](https://developers.cloudflare.com/dynamic-workers/)

- Dynamic Workers are experimental. `CELLD_WORKER_LOADER=LOADER` exposes the
  Worker Loader binding at `env.LOADER`.
- `getDurableObjectClass()` returns a class for a Durable Object facet.
- A call into a loaded Worker from a Durable Object waits for a durability proof
  of the object's writes, because the call carries the object's state. A
  service-binding call waits in the same way.
- A `globalOutbound` Fetcher is not available.
- A loaded Worker cannot receive a capability stub in `env`.
- Awaitable and pipelined properties are not available.

### [Durable Object Facets](https://developers.cloudflare.com/dynamic-workers/usage/durable-object-facets/)

- Facets are experimental, and they require Dynamic Workers. `ctx.facets`
  supports `get()`, `abort()`, and `delete()` for a class from the Worker
  Loader binding.
- A facet has an isolated SQLite database, and celld replicates this database
  with the root Durable Object.
- A facet writes into the database of the root Durable Object, so an outbound
  effect from inside a facet waits for the durability proof of that object.
  `storage.sync()` inside a facet makes the same statement about that object.
- A facet class from `ctx.exports` or a Durable Object binding is not
  available. The `clone()` method is also not available.
- A `props` value for `getDurableObjectClass()` must be JSON-serializable.

### [KV](https://developers.cloudflare.com/kv/api/)

- celld has no edge cache. `cacheTtl` has no effect, and `cacheStatus` is
  `null`.
- A value above 1 MiB requires a fleet bucket.
- celld stores a separate object when an application writes an identical large
  value after a namespace changes owners. The separate object prevents an old
  owner from deleting the current value.
- celld reads a large-value row from an older release, but it does not reclaim
  the legacy object after the row is removed.
- A namespace has one writer. Use more namespaces to increase write capacity.
- A namespace ID can use the Cloudflare hexadecimal form or another stable
  string.

### [Queues](https://developers.cloudflare.com/queues/configuration/javascript-apis/)

- A queue has one writer. Use more queues to increase write capacity.
- A Queue owner admits at most 256 concurrent producer calls. It commits
  concurrent calls in shared transactions, and each reply still waits for its
  own durability proof. This bound lets alarms and settlements run, and it
  limits the message bodies that wait for durability. The owner refuses an
  additional call, and the producer can retry it.
- A queue can have one consumer script. The consumer cannot also export a
  `fetch()` handler.
- A message id is a UUID version 7. The first 48 bits hold the enqueue time
  in milliseconds, so the ids of one Queue owner sort in enqueue order.
- celld retains a message for four days. You cannot configure this period.
- Pull consumers and the Queues HTTP API are not available.
- Dashboard controls, manual consumer attachment, R2 event notifications, and
  Queue event subscriptions are not available.

### [D1](https://developers.cloudflare.com/d1/worker-api/d1-database/)

- A binding result can contain at most 100,000 rows or 32 MiB.
- celld refuses invalid UTF-8 from a SQLite `TEXT` value. Store arbitrary
  bytes in a `BLOB`.

### [Workflows](https://developers.cloudflare.com/workflows/build/workers-api/)

- `create()` replaces a terminal instance that has the same ID. Cloudflare
  refuses each duplicate ID.
- celld replays `run()` from the start, so code outside a step runs again.
- A crash after a step side effect can run the step callback again.
- `retention` and `locationHint` are not available.
- Non-step work cannot remain pending for more than 60 seconds.
- A step result, an event payload, and the workflow parameters each have a
  1 MiB limit.
- `pause()` stops a queued or waiting instance immediately. It lets an active
  step finish, and the status changes from `waitingForPause` to `paused` before
  the next step starts.
- `resume()` starts a paused instance and cancels a pending pause. A paused
  retry, sleep, or event wait keeps its remaining wait duration.
- `restart()` starts a new generation from the beginning by default. Its
  `from` option can select a step by its `name`, `count`, and `type`. The
  `count` value defaults to `1`, and the `type` value defaults to `do`.
- A selected restart reuses each result before the selected step and reruns
  that step and each later step. The runtime rejects a selector that does not
  match the execution history.
- `delete()`, `deleteBatch()`, and rollback are not available.
- A sensitive step result and a `ReadableStream` step result are not
  available.

### [R2](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/)

- An R2 binding uses the fleet bucket under `r2/<bucket_name>/`.
- The `version` of an object equals its content ETag, so two uploads of
  identical bytes to one key return one version.
- `ssecKey` is not available.
- A conditional write cannot use a streamed body larger than 8 MiB.
- `createMultipartUpload()` does not accept a checksum.
- A multipart upload cannot resume on another node or after a restart.
- celld cannot replace a multipart part that the object store already holds.
- Out-of-order parts can use at most 256 MiB of memory, and completion cannot
  change the order of stored parts.
- `jurisdiction` is not available.

## Runtime APIs

| API | status |
| --- | --- |
| [Fetch, Request, Response, and Headers](#fetch-request-response-and-headers) | **Yes** |
| [Bindings](#bindings) | **Yes** |
| [Context](#context) | **Yes** |
| [Handlers](#handlers) | **Yes** |
| [RPC](#rpc) | **Yes** |
| [Streams](#streams) | **Yes** |
| Encoding | **Yes** |
| [WebSockets](#websockets) | **Yes** |
| [Web Crypto](#web-crypto) | **Yes** |
| [Web standards](#web-standards) | **Yes** |
| WebAssembly | **Yes** |
| [Performance and timers](#performance-and-timers) | **Yes** |
| [Console](#console) | **Yes** |
| [Node.js compatibility](#nodejs-compatibility) | **Partial** |
| [Cache](#cache) | **Partial** |
| HTMLRewriter | **Yes** |
| [TCP sockets](#tcp-sockets) | **Yes** |
| EventSource | **Yes** |
| MessageChannel | **Yes** |
| BroadcastChannel | **No** |

### [Fetch, Request, Response, and Headers](https://developers.cloudflare.com/workers/runtime-apis/fetch/)

- The `cache` request option is not available.
- The `redirect` option accepts `follow`, `manual`, and `error`. The `error`
  value rejects a redirect with a `TypeError` before celld sends a request to
  the destination. celld rejects each other value.
- celld removes `Content-Length` from a Worker response. It preserves the
  header for a `HEAD` response.
- A remote Durable Object call streams the request body to the owner. The call
  cannot retry after body transmission starts because celld keeps no replay
  copy.
- A remote Durable Object call waits for a rejected owner generation to change.
  The wait cannot exceed `CELLD_OPERATION_DEADLINE_MS`.

### [Bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/)

- The [services table](#services) lists the available binding types.
- celld supports Durable Objects, services, variables, assets, D1, KV, Queues,
  Workflows, and R2 bindings. Each other binding type is not available.

### [Context](https://developers.cloudflare.com/workers/runtime-apis/context/)

- `passThroughOnException()` has no effect because celld has no CDN fallback.
- `ctx.facets` is available only inside a Durable Object.

### [Handlers](https://developers.cloudflare.com/workers/runtime-apis/handlers/)

- The `tail` and `email` handlers are not available.

### [RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)

- A cross-isolate named service binding supports only a single method call. It
  does not support `fetch()`, awaitable properties, or pipelined paths.
- An RPC stub cannot cross an isolate boundary.
- `ctx.exports` contains only the entrypoints that the configuration declares.
- A remote RPC retries only when the failed peer attempt did not start the
  method. An application must use a stable operation ID for another retry.

### [Streams](https://developers.cloudflare.com/workers/runtime-apis/streams/)

- celld expires an unclaimed and inactive HTTP stream after 60 seconds. A
  successful stream operation starts a new 60-second period.
- A natural stream end or the explicit cancellation of an active read produces
  EOF. A read from an expired or unknown stream produces an error.
- A response body tee uses one bounded buffer for each branch, so the slowest
  live branch controls the source backpressure. celld cancels the source after
  both branches close.

### [WebSockets](https://developers.cloudflare.com/workers/runtime-apis/websockets/)

- A caller must call `accept()` on the socket from a subrequest upgrade.
- An outbound Worker socket closes after the response and `waitUntil` work
  end.
- A Worker socket that the response returns stays open after the response.
  The response takes ownership of the socket, so the end of the request
  cannot close it.
- celld applies a 1 MiB byte budget to the non-terminal frames in each
  isolate-polled WebSocket input queue. The budget includes the message data and
  a fixed charge for each frame.
- The queue reserves one terminal close outside this budget. Therefore, a full
  queue cannot prevent celld from releasing the host socket task.
- If the isolate does not continue to poll, celld discards its unread frames
  during socket cleanup. A later pull reports an abnormal close.
- A message larger than 1 MiB uses the complete queue budget. Therefore, celld
  waits for the Worker to consume that message before it reads another message.
- celld rejects an upgrade when the response status is not 101.
- celld removes Worker-supplied protocol and connection headers from an upgrade
  response.
- An outbound upgrade combines repeated values for one header name.
- celld rejects a close frame from a client when its status code is not valid
  for the WebSocket protocol. The close event has code `1002`, and `wasClean`
  is `false`.
- A WebSocket transport cannot move to a new cell owner. A client must reconnect
  and keep the same application operation ID.
- `acceptWebSocket()` throws when the isolate uses more than 90 percent of its
  V8 heap limit.

### [Web Crypto](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/)

- HMAC signing and verification accept MD5, SHA-1, SHA-224, SHA-256,
  SHA-384, and SHA-512.
- ECDSA signatures support only the P-256 curve and the SHA-256 hash.
- AES-GCM supports authentication tags from 96 through 128 bits in 8-bit
  increments. It returns an `OperationError` for every other tag length.
- RSA-OAEP supports SHA-1, SHA-256, SHA-384, and SHA-512. A nonempty label must
  contain valid UTF-8 bytes.
- A secret key cannot use the `jwk` format in `exportKey()` or
  `wrapKey()`.

### [Web standards](https://developers.cloudflare.com/workers/runtime-apis/web-standards/)

- An `AbortSignal` does not abort an RPC call.
- `signal.onabort` has no effect. Use `addEventListener("abort", ...)`.
- `structuredClone()` does not clone an `AbortSignal`.

### [Performance and timers](https://developers.cloudflare.com/workers/runtime-apis/performance/)

`performance.timeOrigin` is `0`, and `performance.now()` matches `Date.now()`.
Both clocks advance at an I/O boundary and stay fixed during JavaScript
execution.

### [Console](https://developers.cloudflare.com/workers/runtime-apis/console/)

### [Node.js compatibility](https://developers.cloudflare.com/workers/runtime-apis/nodejs/)

- celld implements `node:assert`, `node:async_hooks`, `node:buffer`,
  `node:diagnostics_channel`, `node:events`, `node:fs`, `node:os`,
  `node:path`, `node:stream`, `node:timers/promises`, and `node:util`.
- `node:diagnostics_channel` implements named channels, subscriptions,
  `AsyncLocalStorage` binding, and tracing helpers. It does not export channel
  messages to a tail Worker.
- `node:os` returns the deterministic operating-system values that Workerd
  returns. Its `tmpdir()` and `homedir()` functions return `/tmp/`.
- `node:crypto` does not implement Diffie-Hellman, streaming signatures,
  ciphers, RSA-PSS, or DSA signatures and key generation.
- `node:zlib` implements only the synchronous gzip and deflate functions.
- `node:fs` provides an empty, memory-backed `/tmp` for each request. The
  promise, callback, and synchronous APIs implement `access`, `mkdir`,
  `realpath`, `stat`, `lstat`, and `readFile`.
- A callback runs after the current turn, therefore an application cannot
  observe a result before its call returns. celld reports a filesystem error
  through the callback, but throws an argument error, because a bad argument
  can make the callback unusable.
- These APIs accept a string, a `Buffer`, or a file `URL`. The `stat` and
  `lstat` APIs support bigint metadata.
- `fs.constants` gives the four access modes. The `access` API reports what the
  filesystem permits and not a POSIX permission, so a check for `X_OK` always
  fails, because no file is executable. A check for `W_OK` on a read-only path
  fails with the `ENOENT` error that a missing path also gives, therefore an
  application cannot tell the two apart.
- `node:fs` also provides a read-only `/bundle` directory. It contains one file
  for each module in the Worker bundle, so an application can read its own
  source. The entry module is `/bundle/worker.js`.
- A module name is a path, so a module named `a/b.js` becomes the file
  `/bundle/a/b.js` and celld creates the directory `/bundle/a`.
- A `/bundle` file contains the raw bytes of the module, therefore a read
  returns the source text and not a compiled module. Each attempt to change
  `/bundle` throws `EPERM`.
- The temporary filesystem does not contain file data. A read of a `/tmp` path
  returns `ENOENT`, and each unsupported filesystem call throws an error.
- The celld bundler supports a synchronous CommonJS `require()` of a Node.js
  built-in module. A raw ESM Worker does not receive a global `require()`.
- An import of each other Node.js module succeeds, and the first call into
  the module throws an error.

### [Cache](https://developers.cloudflare.com/workers/runtime-apis/cache/)

- celld has no shared edge cache, so the Cache API cannot serve a stored
  response. `caches.default` and `caches.open()` give a cache with
  always-miss semantics.
- `put()` validates its arguments and reads the response body to
  completion, and it stores nothing.
- `match()` gives `undefined`, and `delete()` gives `false`.

### [TCP sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/)

- A socket cannot outlive the event that created it. celld closes the
  socket when the event ends, so a Durable Object must reconnect in a
  later event.
- celld verifies a TLS server against the bundled Mozilla root store.
- Cloudflare blocks some destination ports, and celld does not block a
  port. The operator controls the egress policy of the fleet network.

### BroadcastChannel

- Cloudflare Workers does not provide BroadcastChannel, and celld does not
  provide it. The class is defined, so a bundle can reference it at load
  time, and the constructor throws an error.

## Compatibility flags

celld honors these compatibility switches:

- `delete_all_deletes_alarm`
- `js_rpc`
- `fetcher_no_get_put_delete`
- `sqlite_vec`
- `websocket_standard_binary_type`
- The static-assets navigation flags

celld accepts each other compatibility flag without effect.
`Cloudflare.compatibilityFlags` reports only the flags that celld honors.

## Wrangler configuration

`celld deploy` accepts `wrangler.jsonc` or `wrangler.json`. It does not
accept `wrangler.toml`.

The `name` value must contain 1 to 63 lowercase ASCII letters, digits, or
internal hyphens. The value must not start or end with a hyphen.

The deployment accepts these top-level keys:

- `$schema`, `name`, `main`, and `no_bundle`
- `compatibility_date` and `compatibility_flags`
- `durable_objects` and `migrations`
- `assets`, `services`, `triggers`, and `vars`
- `d1_databases`, `kv_namespaces`, `queues`, `workflows`, and
  `r2_buckets`

Each other top-level key, including `routes`, stops the deployment.
An asset-only project can omit `main`. celld refuses a symlink or special file
in an asset directory. The native deploy command refuses an asset path that
does not remain unchanged after UTF-8 percent-decoding, and `.assetsignore`
requires Wrangler.

See [Limitations](limitations.md) for the operating-system, networking,
security, pressure, and update boundaries.
