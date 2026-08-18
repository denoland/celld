// The DO object model and the Cloudflare-compatible runtime surface, run
// once per isolate by `js.rs::install_harness` after the Web-API prelude.
// Lived as a 4,000-line raw string inside js.rs until 2026-07-29; it is a
// JavaScript program and belongs in a .js file, like the rest of src/js/.
function __bodyBytes(body) {
  if (body == null) return new Uint8Array();
  if (body && body.__celldBodyBytes instanceof Uint8Array)
    return body.__celldBodyBytes.slice();
  if (body instanceof ArrayBuffer) return new Uint8Array(body.slice(0));
  if (ArrayBuffer.isView(body))
    return new Uint8Array(body.buffer, body.byteOffset, body.byteLength).slice();
  return new TextEncoder().encode(String(body));
}
// Bodies that carry their own Content-Type. A Blob's is its `type`, a
// FormData serializes to multipart with a generated boundary, and a
// URLSearchParams to urlencoded. Returns null for every other body so the
// caller falls through to __bodyBytes. Blob, File, FormData and
// URLSearchParams are all installed later in this file; this runs per
// request, long after that.
function __typedBody(body) {
  if (body == null || typeof body !== "object") return null;
  if (globalThis.Blob && body instanceof Blob)
    return { bytes: body._bytes.slice(), type: body.type };
  if (globalThis.FormData && body instanceof FormData)
    return __multipartBody(body);
  if (globalThis.URLSearchParams && body instanceof URLSearchParams)
    return {
      bytes: new TextEncoder().encode(String(body)),
      type: "application/x-www-form-urlencoded;charset=UTF-8",
    };
  return null;
}
// The WHATWG escape for a multipart field name or filename. It is one-way:
// a parser does not undo it, so a name containing a quote round-trips as
// `%22` rather than the original character.
function __mimeEscape(value) {
  value = String(value);
  // A trailing backslash would escape the closing quote of the parameter and
  // run the header into the part body, so it is refused rather than encoded.
  // Workerd refuses it with this exact message.
  if (value.endsWith("\\"))
    throw new TypeError("Name or filename can't end with backslash");
  return value
    .replace(/\n/g, "%0A").replace(/\r/g, "%0D").replace(/"/g, "%22");
}
// Serialize a FormData into a multipart/form-data body. The boundary must
// not occur in any part; 16 random bytes make that collision unreachable,
// and the delimiter stays unquoted so a `boundary=` parameter needs no
// quoting.
function __multipartBody(form) {
  const random = new Uint8Array(16);
  if (globalThis.crypto && crypto.getRandomValues) crypto.getRandomValues(random);
  else for (let i = 0; i < random.length; i++)
    random[i] = Math.floor(Math.random() * 256);
  const boundary = "----celldFormBoundary" +
    Array.from(random, (byte) => byte.toString(16).padStart(2, "0")).join("");
  const encoder = new TextEncoder();
  const chunks = [];
  for (const [name, value] of form) {
    let header = `--${boundary}\r\nContent-Disposition: form-data; name="${
      __mimeEscape(name)
    }"`;
    if (value instanceof Blob) {
      header += `; filename="${__mimeEscape(value.name)}"`;
      header += `\r\nContent-Type: ${value.type || "application/octet-stream"}`;
    }
    chunks.push(encoder.encode(`${header}\r\n\r\n`));
    chunks.push(value instanceof Blob ? value._bytes : encoder.encode(value));
    chunks.push(encoder.encode("\r\n"));
  }
  chunks.push(encoder.encode(`--${boundary}--\r\n`));
  let total = 0;
  for (const chunk of chunks) total += chunk.byteLength;
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return { bytes, type: `multipart/form-data; boundary=${boundary}` };
}
function __chunkBytes(chunk) {
  if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
  if (ArrayBuffer.isView(chunk))
    return new Uint8Array(
      chunk.buffer, chunk.byteOffset, chunk.byteLength);
  throw new TypeError(
    "Iterable bodies must produce ArrayBuffer or " +
    "ArrayBufferView chunks");
}
// Workerd's "gen" bodies: async iterables become a body stream;
// sync iterables of buffers/views concatenate eagerly. Called only
// for object bodies that are not streams or Blobs — an object with
// a custom toString/@@toPrimitive stringifies instead, unless it is
// an array or async-iterable (Workerd's precedence). Returns null
// for the string-coercion path.
function __iterableBody(body) {
  if (body instanceof ArrayBuffer || ArrayBuffer.isView(body))
    return null;
  const asyncIter = body[Symbol.asyncIterator];
  if (typeof asyncIter === "function") {
    const iterator = asyncIter.call(body);
    return new ReadableStream({
      async pull(controller) {
        const result = await iterator.next();
        if (result.done) { controller.close(); return; }
        controller.enqueue(__chunkBytes(result.value));
      },
      cancel(reason) {
        if (typeof iterator.return === "function")
          iterator.return(reason);
      },
    }, { highWaterMark: 0 });
  }
  if (!Array.isArray(body) &&
      (body.toString !== Object.prototype.toString ||
       body[Symbol.toPrimitive] !== undefined))
    return null;
  if (typeof body[Symbol.iterator] !== "function") return null;
  const chunks = [];
  let length = 0;
  for (const chunk of body) {
    if (!(chunk instanceof ArrayBuffer) && !ArrayBuffer.isView(chunk))
      return null;
    const view = __chunkBytes(chunk);
    chunks.push(view);
    length += view.byteLength;
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const view of chunks) {
    bytes.set(view, offset);
    offset += view.byteLength;
  }
  return bytes;
}
// Drain a Request/Response body stream, caching the result on the
// instance so later consumers see the materialized bytes.
async function __drainBody(target) {
  const chunks = [];
  let length = 0;
  const reader = target.body.getReader();
  for (;;) {
    const result = await reader.read();
    if (result.done) break;
    chunks.push(result.value);
    length += result.value.byteLength;
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  target._bodyBytes = bytes;
  // text()/json() decode lazily; draining a binary body must not build the
  // string form (it can exceed V8's string length for multi-hundred-MB
  // payloads, which turns arrayBuffer() of a large body into a throw).
  return bytes;
}
class CelldBodyStream extends ReadableStream {
  constructor(body) {
    const bytes = __bodyBytes(body);
    const st = { bytes, off: 0 };
    super({
      pull(controller) {
        if (st.off >= st.bytes.byteLength) {
          controller.close();
          return;
        }
        const end = Math.min(st.off + 64 * 1024, st.bytes.byteLength);
        controller.enqueue(st.bytes.subarray(st.off, end));
        st.off = end;
      },
    }, { highWaterMark: 0 });
    this._st = st;
    this.__celldBodyBytes = bytes;
    // Known length, so re-using this stream as a subrequest body
    // still advertises Content-Length.
    this._expectedLength = bytes.byteLength;
  }
  get __celldBodyText() { return new TextDecoder().decode(this.__celldBodyBytes); }
  // Workerd body streams are internal streams, so BYOB readers
  // work on them. Naming ReadableStreamBYOBReader compiles the
  // lazy byte-stream prelude; a worker that never asks for byob
  // mode never pays for it.
  getReader(options) {
    if (options !== undefined && options !== null &&
        options.mode !== undefined) {
      if (String(options.mode) !== "byob")
        throw new TypeError(`Invalid reader mode '${options.mode}'`);
      if (this._ictl === undefined)
        this._ictl = ReadableStreamBYOBReader._buffered(this._st);
      return new ReadableStreamBYOBReader(this);
    }
    return super.getReader(options);
  }
  // Cancelling a body stream poisons later consumption: text()
  // and friends reject, as they would over a real socket.
  _cancel(reason) {
    this._cancelled = true;
    return super._cancel(reason);
  }
}
class CelldHttpBodyStream extends ReadableStream {
  constructor(streamId) {
    const id = Number(streamId);
    super({
      async pull(controller) {
        const chunk = JSON.parse(await __http_stream_read(id));
        if (chunk.done) controller.close();
        else controller.enqueue(Uint8Array.from(chunk.bytes || []));
      },
      cancel() { __http_stream_cancel(id); },
    }, { highWaterMark: 0 });
    this.__celldStreamId = id;
  }
  tee() {
    if (this.locked) throw new TypeError("ReadableStream is locked");
    this._locked = true;
    this._disturbed = true;
    const [left, right] = JSON.parse(
      __http_stream_tee(this.__celldStreamId),
    );
    return [
      new CelldHttpBodyStream(left),
      new CelldHttpBodyStream(right),
    ];
  }
}
globalThis.Response = class Response {
  constructor(body, init = {}) {
    // Streaming/iterable detection runs only for object bodies, so
    // the common string path skips every instanceof below.
    let stream = null;
    let typed = null;
    if (body !== null && typeof body === "object") {
      if (body instanceof ReadableStream) stream = body;
      else if ((typed = __typedBody(body)) !== null) { /* bytes and type */ }
      else {
        const iterable = __iterableBody(body);
        if (iterable instanceof ReadableStream) stream = iterable;
        else if (iterable !== null) body = iterable;
      }
    }
    this._bodyBytes = stream !== null
      ? null
      : typed ? typed.bytes : __bodyBytes(body);
    // Never decoded eagerly: a response whose text is never read must not
    // pay a whole-body UTF-8 decode — for a large binary body (an archive,
    // a git pack) that costs seconds and can exceed V8's string length.
    this._body = undefined;
    // Hono rebuilds a response after middleware with
    // `new Response(response.body, response)`; the held stream (or a
    // fresh CelldBodyStream) preserves the payload across that
    // standard clone shape.
    this.body = body == null
      ? null
      : stream !== null
        ? stream
        : new CelldBodyStream(this._bodyBytes);
    this.status = init.status === undefined ? 200 : Number(init.status);
    this.statusText = init.statusText === undefined ? "" : String(init.statusText);
    this.headers = new Headers(init.headers);
    if (typed && typed.type && !this.headers.has("content-type"))
      this.headers.set("content-type", typed.type);
    this.webSocket = init.webSocket;
    this._wsTarget = init.__wsTarget || init._wsTarget || null;
    this.ok = this.status >= 200 && this.status <= 299;
    this.redirected = false;
    this.type = "default";
    this.url = "";
    this.bodyUsed = false;
    if (init.cf !== undefined) this.cf = init.cf;
  }
  static json(data, init = {}) {
    const headers = new Headers(init.headers || {});
    if (!headers.has("content-type"))
      headers.set("content-type", "application/json");
    return new Response(JSON.stringify(data), { ...init, headers });
  }
  async _consume() {
    if (this._bodyBytes !== null) {
      if (this.body !== null && this.body._cancelled)
        throw new TypeError(
          "Body has already been used. It can only be used once. " +
          "Use tee() first if you need to read it multiple times.");
      return this._bodyBytes;
    }
    return __drainBody(this);
  }
  async text() {
    this.bodyUsed = true;
    return new TextDecoder().decode(await this._consume());
  }
  async json() { return JSON.parse(await this.text()); }
  async formData() {
    return __parseFormData(
      await this.text(), this.headers.get('content-type'));
  }
  async arrayBuffer() {
    this.bodyUsed = true;
    return (await this._consume()).slice().buffer;
  }
  async blob() {
    return new Blob([await this.arrayBuffer()],
      { type: this.headers.get("content-type") || "" });
  }
  clone() {
    if (this.bodyUsed) throw new TypeError("Body has already been consumed");
    if (this._bodyBytes === null)
      throw new TypeError("Cannot clone a streaming response before consumption");
    return new Response(this._bodyBytes, {
      status: this.status, statusText: this.statusText, headers: this.headers,
      webSocket: this.webSocket, __wsTarget: this._wsTarget, cf: this.cf,
    });
  }
};
// Hoisted: this would otherwise allocate an array and scan it on
// every Request construction, which is the HTTP hot path.
const __HTTP_METHODS = new Set(
  ["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]);
globalThis.Request = class Request {
  constructor(input, init = {}) {
    const prior = input instanceof Request ? input : null;
    const suppliedSignal = init.signal !== undefined;
    // The cache option needs the cache_option_enabled compat flag,
    // which Cells does not implement (Workerd's cache-disabled path).
    if (init.cache !== undefined)
      throw new Error("The 'cache' field on 'RequestInitializerDict' " +
        "is not implemented.");
    this.url = prior ? prior.url : String(input);
    {
      // Workers upper-cases every method (the
      // upper_case_all_http_methods flag) but rejects anything outside
      // the known set, reporting the original casing.
      const raw = String(init.method === undefined
        ? (prior ? prior.method : "GET") : init.method);
      const upper = raw.toUpperCase();
      if (!__HTTP_METHODS.has(upper))
        throw new TypeError(`Invalid HTTP method string: ${raw}`);
      this.method = upper;
    }
    const hasBody =
      Object.prototype.hasOwnProperty.call(init, "body");
    let body = hasBody ? init.body : null;
    let stream = null;
    let typed = null;
    if (hasBody && body !== null && typeof body === "object") {
      if (body instanceof ReadableStream) stream = body;
      else if ((typed = __typedBody(body)) !== null) { /* bytes and type */ }
      else {
        const iterable = __iterableBody(body);
        if (iterable instanceof ReadableStream) stream = iterable;
        else if (iterable !== null) body = iterable;
      }
    }
    if (!hasBody && prior && prior._bodyBytes === null) {
      // Adopt the prior request's stream; per spec the prior body
      // is disturbed by the new request.
      stream = prior.body;
      prior.bodyUsed = true;
    }
    this._bodyBytes = stream !== null
      ? null
      : hasBody
        ? (typed ? typed.bytes : __bodyBytes(body))
        : (prior ? prior._bodyBytes.slice() : new Uint8Array());
    // Body text is decoded on the first text()/json(), not here: a request
    // whose body is never read never pays the decode.
    this._body = undefined;
    // Headers are stored raw and built on the first `.headers` access. The
    // hot paths -- a hello world, a Worker that only forwards to a cell --
    // read none, so `new Headers` and the JSON.parse behind it never run.
    if (init.__headersJson !== undefined) {
      this._headersJson = init.__headersJson;
      this._headersInit = undefined;
    } else if (init.headers === undefined && prior) {
      this._headersJson = prior._headersJson;
      this._headersInit = prior._headers ?? prior._headersInit;
    } else {
      this._headersJson = undefined;
      this._headersInit = init.headers;
    }
    this._headers = null;
    // A typed body's default content-type must land now; only user-built
    // typed bodies reach this, never the incoming request path.
    if (typed && typed.type && !this.headers.has("content-type")) {
      this.headers.set("content-type", typed.type);
    }
    this.bodyUsed = false;
    // The body stream and the signal stay eager, own properties: the RPC and
    // storage serializers fall back to the lift-into-marker path only when a
    // value is not directly structured-cloneable, and a Request's un-cloneable
    // body stream (or AbortSignal) is what triggers that. Making them lazy
    // getters let `__sc_encode` clone the Request into a broken plain object
    // and skip the lift -- the serializeHttpTypes conformance failure.
    this.body = stream !== null
      ? stream
      : ["GET", "HEAD"].includes(this.method)
        ? null : new CelldBodyStream(this._bodyBytes);
    this.redirect = init.redirect || (prior ? prior.redirect : "follow");
    const cf = init.cf === undefined
      ? (prior ? prior.cf : undefined) : init.cf;
    if (cf !== undefined) this.cf = cf;
    this.signal = suppliedSignal
      ? init.signal
      : (prior ? prior.signal : new AbortController().signal);
    this._signalForSubrequests = init.__celldIncomingSignal
      ? null
      : suppliedSignal
        ? this.signal
        : (prior ? prior._signalForSubrequests : null);
  }
  get headers() {
    if (this._headers === null) {
      const src = this._headersJson !== undefined
        ? JSON.parse(this._headersJson)
        : this._headersInit;
      this._headers = new Headers(src);
      this._headersJson = undefined;
      this._headersInit = undefined;
    }
    return this._headers;
  }
  set headers(value) {
    this._headers = value instanceof Headers ? value : new Headers(value);
    this._headersJson = undefined;
    this._headersInit = undefined;
  }
  async _consume() {
    if (this._bodyBytes !== null) return this._bodyBytes;
    return __drainBody(this);
  }
  async text() {
    this.bodyUsed = true;
    const bytes = await this._consume();
    return this._body ??= new TextDecoder().decode(bytes);
  }
  async json() { return JSON.parse(await this.text()); }
  async formData() {
    return __parseFormData(
      await this.text(), this.headers.get('content-type'));
  }
  async arrayBuffer() {
    this.bodyUsed = true;
    return (await this._consume()).slice().buffer;
  }
  async blob() {
    return new Blob([await this.arrayBuffer()],
      { type: this.headers.get("content-type") || "" });
  }
  clone() {
    if (this.bodyUsed) throw new TypeError("Body has already been consumed");
    if (this._bodyBytes === null) {
      // Streaming body: tee, keep one branch, clone the other.
      const [left, right] = this.body.tee();
      this.body = left;
      return new Request(this, { body: right });
    }
    return new Request(this);
  }
};
globalThis.__makeRequest = (
  url, method, body, headersJson = "[]", signal = undefined,
  incomingSignal = false,
) => new Request(url, {
  // The raw header JSON, not a parsed object: an incoming request whose
  // handler never reads `.headers` (a hello world, or a Worker that only
  // routes to a cell) then never parses it. See `get headers()`.
  method, body, __headersJson: headersJson, signal,
  __celldIncomingSignal: incomingSignal,
});
const __fmt = (a) => a.map((x) => {
  if (typeof x === "string") return x;
  if (x instanceof Error) return x.stack || (x.name + ": " + x.message);
  try { return JSON.stringify(x); } catch { return String(x); }
}).join(" ");
globalThis.console = {
  log: (...a) => __log(__fmt(a)), info: (...a) => __log(__fmt(a)),
  error: (...a) => __log("ERROR " + __fmt(a)), warn: (...a) => __log("WARN " + __fmt(a)),
  debug() {}, trace() {}, group() {}, groupEnd() {}, table() {},
};

// async-op shims: outbound fetch + timers, driven by the host event loop.
globalThis.fetch = async (input, init) => {
  const req = new Request(input, init);
  // `fetch(url, { headers: { Upgrade: "websocket" } })` is the other way
  // Cloudflare opens an outbound socket, and the one most examples use.
  if ((req.headers.get("upgrade") ?? "").toLowerCase() === "websocket") {
    return await __fetchWebSocketUpgrade(req);
  }
  const bytes = ["GET", "HEAD"].includes(req.method)
    ? null : await req._consume();
  const body = bytes === null
    ? undefined : JSON.stringify(Array.from(bytes));
  const r = JSON.parse(await __op_fetch(
    req.method, req.url, body, JSON.stringify(Array.from(req.headers)), req.redirect,
  ));
  const response = new Response(
    new CelldHttpBodyStream(r.streamId),
    { status: r.status, headers: r.headers },
  );
  response.url = req.url;
  return response;
};
globalThis.__fetchWebSocketUpgrade = async (req) => {
  const scope = __actorEventStack[__actorEventStack.length - 1] || "";
  const id = __ws_alloc();
  const socket = __makeSocket(id);
  socket._outbound = true;
  socket._polled = !scope;
  socket.url = req.url;
  socket.readyState = WebSocket.READY_STATE_CONNECTING;
  __sockets.set(id, socket);
  let raw;
  try {
    raw = JSON.parse(await __ws_upgrade(
      id,
      scope,
      req.url,
      JSON.stringify(Array.from(req.headers)),
    ));
  } catch (error) {
    __sockets.delete(id);
    throw error;
  }
  if (!raw.upgraded) {
    // The server answered without upgrading. That is an ordinary response,
    // not a connection error, and it is returned unchanged.
    __sockets.delete(id);
    const response = new Response(new Uint8Array(raw.body), {
      status: raw.status,
      headers: raw.headers,
    });
    response.url = req.url;
    return response;
  }
  socket.protocol = raw.protocol ?? "";
  const response = new Response(null, { status: 101 });
  response.webSocket = socket;
  response.url = req.url;
  return response;
};
globalThis.__makeAssetsBinding = (script) => ({
  async fetch(input, init) {
    const req = input instanceof Request ? new Request(input, init) : new Request(input, init);
    const r = JSON.parse(await __asset_fetch(
      script,
      req.method,
      req.url,
      JSON.stringify(Array.from(req.headers)),
    ));
    const response = new Response(
      new CelldHttpBodyStream(r.streamId),
      { status: r.status, headers: r.headers },
    );
    response.url = req.url;
    return response;
  },
});
// R2 is deliberately not implemented. The binding exists so a worker
// that declares r2_buckets still loads, but every method fails loudly
// instead of pretending to be a bucket.
globalThis.__makeR2Bucket = (name) => {
  const fail = (method) => () => {
    throw new Error(
      `R2 is not implemented in celld (binding ${name}, ` +
        `method ${method})`,
    );
  };
  const bucket = {};
  for (const method of ["head", "get", "put", "delete", "list",
    "createMultipartUpload", "resumeMultipartUpload"]) {
    bucket[method] = fail(method);
  }
  return bucket;
};
globalThis.__makeAiBinding = (url) => ({
  async run(model, input) {
    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model, input }),
    });
    if (!response.ok) throw new Error(`AI binding returned ${response.status}`);
    return response.json();
  },
});
const __timers = new Map();
globalThis.setTimeout = (cb, ms, ...a) => {
  const id = __timer_alloc();
  __timers.set(id, true);
  __op_timer(id, ms | 0).then(() => {
    if (!__timers.delete(id)) return;
    cb(...a);
  });
  return id;
};
globalThis.clearTimeout = (id) => {
  if (!__timers.delete(id)) return;
  __timer_cancel(id);
};
globalThis.setInterval = () => {
  throw new Error("setInterval is not implemented in celld");
};
globalThis.clearInterval = () => {};

const __sqlCursorFinalizer = typeof FinalizationRegistry === "function"
  ? new FinalizationRegistry((cursorId) => __sql_cursor_close(cursorId))
  : null;

// A `SqlStorageCursor` over one exec's result (Cloudflare DO SQL API).
class SqlCursor {
  constructor(res) {
    if (res.error) throw new Error("SQL error: " + res.error);
    this._decode = (value) => value && typeof value === "object" &&
      Array.isArray(value.__celld_bytes)
      ? Uint8Array.from(value.__celld_bytes).buffer
      : value;
    this.columns = res.columns;
    this.rowsWritten = Number(res.rowsWritten || 0);
    this.reusedCachedQueryForTest = Boolean(res.reusedCachedQuery);
    this._deferredError = null;
    if (res.native) {
      this._native = true;
      this._cursorId = Number(res.cursorId || 0);
      this._prefetched = res.row === null
        ? null
        : res.row.map(this._decode);
      this.rowsRead = this._prefetched === null ? 0 : 1;
      this._finalizerToken = null;
      if (this._cursorId && __sqlCursorFinalizer) {
        this._finalizerToken = {};
        __sqlCursorFinalizer.register(
          this, this._cursorId, this._finalizerToken,
        );
      }
    } else {
      this._native = false;
      this._rows = res.rows.map((row) => row.map(this._decode));
      this._rowCount = this._rows.length;
      this._index = 0;
      this.rowsRead = Math.min(1, this._rowCount);
    }
  }
  _obj(r) { const o = {}; for (let i = 0; i < this.columns.length; i++) o[this.columns[i]] = r[i]; return o; }
  _finishNative() {
    if (this._finalizerToken && __sqlCursorFinalizer)
      __sqlCursorFinalizer.unregister(this._finalizerToken);
    this._finalizerToken = null;
    this._cursorId = 0;
  }
  _advanceNative() {
    if (!this._cursorId) {
      this._prefetched = null;
      return;
    }
    const result = JSON.parse(__sql_cursor_next(this._cursorId));
    if (result.error) {
      this._deferredError = new Error("SQL error: " + result.error);
      this._prefetched = null;
      this._finishNative();
      return;
    }
    this.rowsWritten = Number(result.rowsWritten || this.rowsWritten);
    if (result.row === null) {
      this._prefetched = null;
      this._finishNative();
    } else {
      this._prefetched = result.row.map(this._decode);
      this.rowsRead++;
    }
  }
  _nextRaw() {
    if (this._native) {
      if (this._deferredError) {
        const error = this._deferredError;
        this._deferredError = null;
        throw error;
      }
      if (this._prefetched === null) return { done: true };
      const value = this._prefetched;
      this._advanceNative();
      return { done: false, value };
    }
    if (this._index >= this._rowCount) return { done: true };
    const index = this._index++;
    const value = this._rows[index];
    // Release consumed rows even if the cursor itself remains reachable.
    this._rows[index] = null;
    this.rowsRead = Math.min(this._index + 1, this._rowCount);
    return { done: false, value };
  }
  toArray() {
    const rows = [];
    while (true) {
      const result = this.next();
      if (result.done) return rows;
      rows.push(result.value);
      if (__heap_limit_excessively_exceeded())
        throw new Error("result set is too large to fit in memory");
    }
  }
  one() {
    const first = this.next();
    if (first.done)
      throw new Error("Expected exactly one result from SQL query, but got no results");
    if (!this.next().done)
      throw new Error("Expected exactly one result from SQL query, but got multiple results");
    return first.value;
  }
  *raw() {
    while (true) {
      const result = this._nextRaw();
      if (result.done) return;
      yield result.value;
    }
  }
  get columnNames() { return this.columns; }
  next() {
    const result = this._nextRaw();
    return result.done ? result : { done: false, value: this._obj(result.value) };
  }
  [Symbol.iterator]() { return this; }
}
class SqlStorage {
  constructor(scope) { this._scope = scope; }
  prepare(query) {
    const storage = this;
    const source = String(query);
    return (...binds) => storage.exec(source, ...binds);
  }
  exec(query, ...binds) {
    const encode = (value) => {
      if (value instanceof ArrayBuffer)
        return { __celld_bytes: Array.from(new Uint8Array(value)) };
      if (ArrayBuffer.isView(value))
        return { __celld_bytes: Array.from(
          new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
        ) };
      return value;
    };
    return new SqlCursor(JSON.parse(__sql_cursor_start(
      this._scope, query, JSON.stringify(binds.map(encode)),
    )));
  }
  ingest(input) {
    const result = JSON.parse(__sql_ingest(this._scope, String(input)));
    if (result.error) throw new Error("SQL error: " + result.error);
    return result;
  }
  get databaseSize() { return __sql_database_size(this._scope); }
  setMaxPageCountForTest(pages) {
    if (typeof __sql_set_max_page_count_for_test !== "function")
      throw new Error("setMaxPageCountForTest is only available in tests");
    __sql_set_max_page_count_for_test(this._scope, Number(pages));
  }
  setWriteFaultForTest(enabled) {
    if (typeof __sql_set_write_fault_for_test !== "function")
      throw new Error("setWriteFaultForTest is only available in tests");
    __sql_set_write_fault_for_test(!!enabled);
  }
  setCacheSizeForTest(pages) {
    if (typeof __sql_set_cache_size_for_test !== "function")
      throw new Error("setCacheSizeForTest is only available in tests");
    __sql_set_cache_size_for_test(this._scope, Number(pages));
  }
  setInterruptFaultForTest(enabled) {
    if (typeof __sql_set_interrupt_fault_for_test !== "function")
      throw new Error(
        "setInterruptFaultForTest is only available in tests");
    __sql_set_interrupt_fault_for_test(this._scope, !!enabled);
  }
  registerNomemFunctionForTest() {
    if (typeof __sql_register_nomem_function_for_test !== "function")
      throw new Error(
        "registerNomemFunctionForTest is only available in tests");
    __sql_register_nomem_function_for_test(this._scope);
  }
}
function __readStoredValue(scope, key, read) {
  try {
    return read();
  } catch (error) {
    const message = String(error && error.message || error);
    if (!message.toLowerCase().includes("deserialize cloned data"))
      throw error;
    const contextual = new Error(
      "actor storage deserialization failed; actorId = " +
      scope + "; key = " + key + "; " + message,
    );
    contextual.cause = error;
    throw contextual;
  }
}
class SyncKvListIterator {
  constructor(root, generation, cursor) {
    this._root = root;
    this._generation = generation;
    this._cursor = cursor;
  }
  [Symbol.iterator]() { return this; }
  next() {
    if (this._generation !== this._root._syncKvListGeneration) {
      throw new Error(
        "kv.list() iterator was invalidated because a new call to kv.list() was started. " +
        "Only one kv.list() iterator can exist at a time.",
      );
    }
    const value =
      __storage_sync_list_next(this._cursor, __storedSentinel);
    if (value === null) return { done: true, value: undefined };
    value[1] = __unwrapStored(value[1]);
    return { done: false, value };
  }
}
class SyncKvStorage {
  constructor(storage) {
    this._storage = storage;
    this._root = storage._transactionRoot;
  }
  get(key) {
    key = String(key);
    return __unwrapStored(__readStoredValue(
      this._storage._scope, key,
      () => __storage_get(this._storage._scope, key,
                          __storedSentinel),
    ));
  }
  put(key, value) {
    key = String(key);
    try {
      __storage_put(this._storage._scope, key, value);
    } catch (error) {
      __storage_put_serialized(this._storage._scope, key,
                               __storedBytes(value, error));
    }
  }
  delete(key) {
    return __storage_delete(this._storage._scope, String(key));
  }
  list(options = {}) {
    const generation = ++this._root._syncKvListGeneration;
    const cursor = __storage_sync_list_start(
      this._storage._scope, JSON.stringify(options),
    );
    return new SyncKvListIterator(
      this._root, generation, cursor,
    );
  }
}
class DurableObjectStorage {
  constructor(
    scope,
    state = null,
    transactionRoot = null,
    transactionDepth = 0,
    transactionControl = null,
  ) {
    this._scope = scope;
    this._state = state;
    this.sql = new SqlStorage(scope);
    this._transactionRoot = transactionRoot || this;
    this._transactionDepth = transactionDepth;
    this._transactionControl = transactionControl;
    if (!transactionRoot) {
      this._transactionSerial = 0;
      this._transactionTail = Promise.resolve();
      this._syncKvListGeneration = 0;
    }
    this._kv = new SyncKvStorage(this);
  }
  get kv() { return this._kv; }
  _flushPendingPuts() {
    if (this._state && this._state._aborted) return false;
    __storage_flush_pending_puts(this._scope);
    return true;
  }
  async get(k) {
    await Promise.resolve();
    if (!this._flushPendingPuts()) return;
    const key = Array.isArray(k) ? JSON.stringify(k) : String(k);
    return __readStoredValue(this._scope, key, () => {
      if (Array.isArray(k))
        return __unwrapStoredMap(
          __storage_get_many(this._scope, k, __storedSentinel));
      return __unwrapStored(
        __storage_get(this._scope, k, __storedSentinel));
    });
  }
  async put(k, val) {
    if (typeof k === "string") {
      try {
        __storage_queue_put(this._scope, k, val);
      } catch (error) {
        __storage_queue_put_serialized(this._scope, k,
                                       __storedBytes(val, error));
      }
      await Promise.resolve();
      this._flushPendingPuts();
      return;
    }
    const source = k instanceof Map ? Array.from(k) : Object.entries(k);
    if (source.length === 0) return;
    const entries =
      source.map(([key, value]) => [String(key), value]);
    try {
      __storage_queue_put_many(this._scope, entries);
    } catch (error) {
      // A batch with a stub entry: encode every entry first (plain
      // entries keep their native clone bytes, stub entries take
      // the stored-stub envelope), then queue — so a genuinely
      // uncloneable entry still queues nothing, like the batch op,
      // which serializes fully before queueing.
      const encoded = entries.map(([key, value]) => {
        try {
          return [key, __sc_encode(value)];
        } catch (error_) {
          return [key, __storedBytes(value, error_)];
        }
      });
      for (const [key, bytes] of encoded)
        __storage_queue_put_serialized(this._scope, key, bytes);
    }
    await Promise.resolve();
    this._flushPendingPuts();
  }
  async delete(k) {
    await Promise.resolve();
    if (!this._flushPendingPuts()) return;
    if (Array.isArray(k)) {
      if (k.length === 0) return 0;
      return __storage_delete_many(this._scope, k);
    }
    return __storage_delete(this._scope, k);
  }
  async list(options = {}) {
    await Promise.resolve();
    if (!this._flushPendingPuts()) return new Map();
    return __unwrapStoredMap(__storage_list(
      this._scope, JSON.stringify(options), __storedSentinel));
  }
  async deleteAll() {
    await Promise.resolve();
    if (!this._flushPendingPuts()) return;
    __storage_delete_all(this._scope, __cell.deleteAllDeletesAlarm);
  }
  async sync() {
    await Promise.resolve();
    this._flushPendingPuts();
  }
  async setAlarm(t) {
    await Promise.resolve();
    if (!this._flushPendingPuts()) return;
    __alarm_set(this._scope, t instanceof Date ? t.getTime() : Number(t));
  }
  async getAlarm() {
    await Promise.resolve();
    if (!this._flushPendingPuts()) return null;
    const v = __alarm_get(this._scope);
    return v === null ? null : v;
  }
  async deleteAlarm() {
    await Promise.resolve();
    if (!this._flushPendingPuts()) return;
    __alarm_delete(this._scope);
  }
  _transactionStart() {
    const root = this._transactionRoot;
    const savepoint = "cells_tx_" + (++root._transactionSerial);
    __storage_transaction_control(
      this._scope, "start", this._transactionDepth > 0, savepoint,
    );
    return savepoint;
  }
  _transactionCommit(savepoint) {
    __storage_transaction_control(
      this._scope, "commit", this._transactionDepth > 0, savepoint,
    );
  }
  _transactionRollback(savepoint, explicit = false) {
    __storage_transaction_control(
      this._scope,
      explicit ? "rollback_explicit" : "rollback",
      this._transactionDepth > 0,
      savepoint,
    );
  }
  _transactionView(transactionControl) {
    return new DurableObjectStorage(
      this._scope,
      this._state,
      this._transactionRoot,
      this._transactionDepth + 1,
      transactionControl,
    );
  }
  rollback() {
    const control = this._transactionControl;
    if (!control)
      throw new TypeError("rollback() must be called on a transaction");
    if (!control.rolledBack) {
      control.rollback();
      control.rolledBack = true;
    }
  }
  transactionSync(f) {
    const savepoint = this._transactionStart();
    const control = {
      rolledBack: false,
      rollback: () => this._transactionRollback(savepoint, true),
    };
    try {
      const value = f(this._transactionView(control));
      if (!control.rolledBack) this._transactionCommit(savepoint);
      return value;
    } catch (error) {
      if (!control.rolledBack) {
        try { this._transactionRollback(savepoint); } catch {}
      }
      throw error;
    }
  }
  async _runTransaction(f) {
    const savepoint = this._transactionStart();
    const control = {
      rolledBack: false,
      rollback: () => this._transactionRollback(savepoint, true),
    };
    try {
      const value = await f(this._transactionView(control));
      if (!control.rolledBack) this._transactionCommit(savepoint);
      return value;
    } catch (error) {
      if (!control.rolledBack) {
        try { this._transactionRollback(savepoint); } catch {}
      }
      throw error;
    }
  }
  async transaction(f) {
    if (this._transactionDepth > 0) return this._runTransaction(f);
    const root = this._transactionRoot;
    const previous = root._transactionTail;
    let release;
    root._transactionTail = new Promise((resolve) => { release = resolve; });
    await previous;
    try {
      return await this._runTransaction(f);
    } finally {
      release();
    }
  }
}
class DurableObjectId {
  constructor(className, value, name = undefined) {
    Object.defineProperties(this, {
      _className: { value: className },
      _value: { value },
    });
    this.name = name;
    this.jurisdiction = undefined;
  }
  toString() { return this._value; }
  equals(other) {
    return other instanceof DurableObjectId && other._value === this._value;
  }
  _scope() { return this._className + ":" + this._value; }
}
globalThis.DurableObjectRoutingError = class DurableObjectRoutingError
  extends Error {
  constructor(detail = {}) {
    super("The Durable Object owner is currently unreachable");
    this.name = "DurableObjectRoutingError";
    this.code = "owner_unreachable";
    this.retryable = true;
    if (typeof detail.scope === "string") this.scope = detail.scope;
    if (typeof detail.owner === "string") this.owner = detail.owner;
  }
};
const __durableObjectRoutingError = (error) => {
  const marker = "__CELLD_DO_ROUTING_ERROR__:";
  const message = String(error && error.message || error);
  const offset = message.indexOf(marker);
  if (offset < 0) return null;
  try {
    return new DurableObjectRoutingError(
      JSON.parse(message.slice(offset + marker.length)),
    );
  } catch {
    return new DurableObjectRoutingError();
  }
};
// A cell's input gate as the isolate sees it, for the one delivery
// point that is inside the isolate rather than in front of it: an
// RPC stub op. `shut` counts the blocks that have asked for the gate
// and is raised synchronously, so no window exists in which a block
// has started and the isolate still thinks the cell is open.
// `holder` names the block actually running — a second block that is
// still queued has raised `shut` but holds nothing.
//
// A stub minted while a block runs carries that block's token and
// re-enters it instead of queueing behind it. That is Workerd's
// `IoContext::makeReentryCallback`, and without it a callback the
// block itself sent out could never come back.
const __cellBlocks = new Map();
const __blockEnter = (scope) => {
  const block = __cellBlocks.get(scope);
  if (block !== undefined) return (block.shut++, block);
  const fresh = { shut: 1, holder: null };
  __cellBlocks.set(scope, fresh);
  return fresh;
};
const __blockLeave = (scope, block) => {
  block.holder = null;
  if (--block.shut === 0) __cellBlocks.delete(scope);
};
class DurableObjectState {
  constructor(scope) {
    this._scope = scope;
    this._aborted = false;
    this.storage = new DurableObjectStorage(scope, this);
    this._gate = Promise.resolve();
    this._blockDepth = 0;
    const separator = scope.indexOf(":");
    const className = separator < 0 ? scope : scope.slice(0, separator);
    const value = separator < 0 ? scope : scope.slice(separator + 1);
    this.id = new DurableObjectId(
      className, value, __cell.idNames[scope],
    );
  }
  // Workerd's DurableObjectState.exports (actor-state.h): the same
  // loopback surface as ctx.exports on stateless entrypoints.
  get exports() { return __ctxExports(); }
  blockConcurrencyWhile(f) {
    if (typeof f !== "function")
      throw new TypeError("blockConcurrencyWhile() requires a function");
    if (this._blockDepth >= 64)
      throw new Error(
        "blockConcurrencyWhile() calls are nested too deeply.",
      );
    if (this._blockDepth > 0) return this._runConcurrencyBlock(f);
    // The host owns the gate (celld_logic::gate::InputGate). It replaces a
    // promise chain that could only order blocks against each other; the
    // gate sits at the delivery points, so nothing else is delivered to this
    // cell at all while the block runs.
    //
    // The acquire waits. It used to be synchronous, and had to be: a cell's
    // events came off one channel, so yielding even one microtask reopened a
    // window in which this cell delivered an event that then waited on a
    // gate nothing would release. Events are independent tasks now, so two
    // can reach a block together and the second has to queue — and nothing
    // nests, so waiting cannot deadlock on an event suspended beneath it.
    // Asked for synchronously, awaited asynchronously, and the split is the
    // whole point. The op shuts the gate in this very call, so no event can
    // be delivered to this cell from here on — that immediacy is what makes
    // `failCriticalSection()` reject a `ping()` issued right after it.
    // Waiting for the *ticket* is separate: events are independent tasks
    // now, so two of them can reach a block together and the second has to
    // queue behind the first. Calling this inside the async body instead
    // costs one microtask, and an event delivered in that window walks
    // straight into the critical section.
    const block = __blockEnter(this._scope);
    const acquired = __gate_acquire(this._scope);
    const next = (async () => {
      try {
        // The op answers a string; the gate keys events by number.
        const event = Number(await acquired);
        block.holder = event;
        // A critical section that fails resets the actor, so whatever queued
        // behind its gate must be refused rather than handed to the reset
        // one — it was sent to a cell whose state no longer exists. The
        // failure rides with the release for that: waiters are woken with it
        // instead of merely woken.
        let failure = null;
        try {
          return await this._runConcurrencyBlock(f);
        } catch (error) {
          failure = String((error && error.message) || error);
          throw error;
        } finally {
          __gate_release(this._scope, event, failure);
        }
      } finally {
        // Also on a rejected `acquired`: the block ahead failed, this one
        // never ran, and the cell must not stay shut on its behalf.
        __blockLeave(this._scope, block);
      }
    })();
    // `_ready()` awaits this. The host gate stops *other* events, but a
    // block taken in the constructor runs inside the very event that is
    // being delivered, so nothing external can hold that event back — the
    // promise does. Concurrent blocks within one event go through
    // `_blockDepth` above and never reach here.
    this._gate = next;
    return next;
  }
  _runConcurrencyBlock(f) {
    this._blockDepth++;
    let result;
    try {
      result = f();
    } catch (error) {
      this._blockDepth--;
      this._resetAfterConcurrencyFailure(error);
      throw error;
    }
    const timerId = __timer_alloc();
    let settled = false;
    const timeout = __op_timer(
      timerId,
      30_000,
    ).then(() => {
      if (!settled) {
        throw new Error(
          "A call to blockConcurrencyWhile() in a Durable Object waited for " +
          "too long. The call was canceled and the Durable Object was reset.",
        );
      }
    });
    return Promise.race([Promise.resolve(result), timeout]).then(
      (value) => {
        settled = true;
        __timer_cancel(timerId);
        this._blockDepth--;
        return value;
      },
      (error) => {
        settled = true;
        __timer_cancel(timerId);
        this._blockDepth--;
        this._resetAfterConcurrencyFailure(error);
        throw error;
      },
    );
  }
  _resetAfterConcurrencyFailure(error) {
    this._aborted = true;
    __storage_cancel_pending_puts(this._scope);
    const instance = __cell.instances[this._scope];
    if (instance && instance.__celldState === this)
      delete __cell.instances[this._scope];
    // Under a stub-mediated caller (this scope is not the current
    // event) the failure breaks the actor, as Workerd joins it
    // into the on-abort promise; a direct event keeps reset-only
    // semantics — its caller sees the rejection itself.
    if (__actorEventStack[__actorEventStack.length - 1] !==
        this._scope)
      __actorBreak(this._scope, error);
  }
  abort(reason) {
    this._aborted = true;
    __storage_cancel_pending_puts(this._scope);
    const instance = __cell.instances[this._scope];
    if (instance && instance.__celldState === this)
      delete __cell.instances[this._scope];
    const message = reason instanceof Error
      ? reason.message
      : String(reason);
    // Direct host-dispatched event on this actor: the uncatchable
    // terminate_execution path. Under a same-isolate stub caller,
    // termination would unwind the caller too, so break in JS.
    if (__actorEventStack[__actorEventStack.length - 1] ===
        this._scope) {
      __actor_abort(this._scope, message);
      return;
    }
    __actorBreak(this._scope,
      reason instanceof Error ? reason : new Error(message));
  }
  _ready() { return this._gate; }
  getWebSockets(tag) {
    return JSON.parse(__ws_list(this._scope, tag)).map((row) => __socketFromRow(row));
  }
  acceptWebSocket(ws, tags = []) {
    ws._target = { id: ws._id, scope: this._scope };
    if (ws._peer) ws._peer._target = ws._target;
    ws._hibernatable = true;
    if (ws._peer) ws._peer._hibernatable = true;
    ws._tags = Array.from(tags, String);
    __ws_accept(ws._id, this._scope, JSON.stringify(ws._tags));
    __sockets.set(ws._id, ws);
  }
  _socket(id) {
    return __sockets.get(Number(id)) ||
      this.getWebSockets().find((ws) => ws._id === id) ||
      __wsStub(id);
  }
  // Workerd actor-state.c++ setWebSocketAutoResponse: no pair unsets, and
  // each side is capped at 2048 UTF-8 bytes. The pair itself lives in the
  // shell — a matched message is answered without waking this cell.
  setWebSocketAutoResponse(pair) {
    if (pair === undefined || pair === null) {
      __ws_auto_response_set(this._scope, null, null);
      return;
    }
    if (!(pair instanceof WebSocketRequestResponsePair))
      throw new TypeError(
        "Failed to execute 'setWebSocketAutoResponse' on " +
        "'DurableObjectState': parameter 1 is not of type " +
        "'WebSocketRequestResponsePair'.",
      );
    const max = 2048;
    const bytes = (s) => new TextEncoder().encode(s).length;
    const requestSize = bytes(pair.request);
    if (requestSize > max)
      throw new RangeError(
        `Request cannot be larger than ${max} bytes. ` +
        `A request of size ${requestSize} was provided.`,
      );
    const responseSize = bytes(pair.response);
    if (responseSize > max)
      throw new RangeError(
        `Response cannot be larger than ${max} bytes. ` +
        `A response of size ${responseSize} was provided.`,
      );
    __ws_auto_response_set(this._scope, pair.request, pair.response);
  }
  getWebSocketAutoResponse() {
    const pair = JSON.parse(__ws_auto_response_get(this._scope));
    if (pair === null) return null;
    return new WebSocketRequestResponsePair(pair[0], pair[1]);
  }
  getWebSocketAutoResponseTimestamp(ws) {
    const ms = __ws_auto_response_ts(ws._id);
    return ms === null ? null : new Date(ms);
  }
  waitUntil(promise) {
    globalThis.__registerWaitUntil(promise);
  }
}
function _instance(scope) {
  let inst = __cell.instances[scope];
  if (!inst) {
    const className = scope.split(":")[0];
    const cls = __cell.classes[className];
    if (!cls) throw new Error("no DO class " + className);
    const state = new DurableObjectState(scope);
    inst = new cls(state, __cell.env);
    Object.defineProperty(inst, "__celldState", { value: state });
    // Workerd worker-rpc.c++ getTargetInfo(): RPC needs `extends
    // DurableObject` unless the js_rpc compat flag is on. Decided
    // once here; dispatch reads a boolean.
    state._rpcOk = __cell.compat.jsRpc ||
      __cell.doExports[className] === true;
    __cell.instances[scope] = inst;
  }
  return inst;
}
async function _readyInstance(scope) {
  const inst = _instance(scope);
  await inst.__celldState._ready();
  return inst;
}
const __actorEventStack = [];
const __incomingRequestSignals = new Map();
const __abortSignal = (signal, reason) => {
  if (signal.aborted) return;
  signal.aborted = true;
  signal.reason = reason;
  signal.dispatchEvent(new Event("abort"));
};
globalThis.__abortIncomingRequest = (requestId) => {
  const signal = __incomingRequestSignals.get(String(requestId));
  if (signal && !signal.aborted) {
    __abortSignal(signal, new Error("The client has disconnected"));
    return true;
  }
  return false;
};
globalThis.__registerIncomingRequest = (requestId, request) => {
  __incomingRequestSignals.set(String(requestId), request.signal);
};
globalThis.__finishIncomingRequest = (requestId) => {
  __incomingRequestSignals.delete(String(requestId));
};
globalThis.__endTerminatedActorEvent = (scope) => {
  for (let index = __actorEventStack.length - 1; index >= 0; index--) {
    if (__actorEventStack[index] === scope) {
      __actorEventStack.splice(index, 1);
      return;
    }
  }
};
// Race a pending dispatch against the caller's abort signal: on abort,
// run `abandon` (which reaches the target's side) and reject with the
// caller's reason. The dispatch keeps its settle handlers, so a late
// settlement after abandonment cannot trip the unhandled-rejection
// signal.
const __raceCallerAbort = (dispatch, signal, abandon) =>
  new Promise((resolve, reject) => {
    let settled = false;
    const onAbort = () => {
      if (settled) return;
      settled = true;
      abandon();
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    dispatch.then(
      (value) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
const __awaitCancellableDoCall = (operation, signal) => {
  if (signal.aborted) {
    __do_call_cancel(operation.__celldCancelId);
    return Promise.reject(signal.reason);
  }
  return __raceCallerAbort(operation, signal,
    () => __do_call_cancel(operation.__celldCancelId));
};
// Workerd semantics: a Response crossing a service binding is a fresh
// fetch-shaped Response — immutable headers, default reason phrase,
// the request URL, and a real body stream — never the callee's own
// mutable object.
const __STATUS_TEXT = {
  100: "Continue", 101: "Switching Protocols", 102: "Processing",
  103: "Early Hints", 200: "OK", 201: "Created", 202: "Accepted",
  203: "Non-Authoritative Information", 204: "No Content",
  205: "Reset Content", 206: "Partial Content", 207: "Multi-Status",
  208: "Already Reported", 226: "IM Used", 300: "Multiple Choices",
  301: "Moved Permanently", 302: "Found", 303: "See Other",
  304: "Not Modified", 305: "Use Proxy", 307: "Temporary Redirect",
  308: "Permanent Redirect", 400: "Bad Request", 401: "Unauthorized",
  402: "Payment Required", 403: "Forbidden", 404: "Not Found",
  405: "Method Not Allowed", 406: "Not Acceptable",
  407: "Proxy Authentication Required", 408: "Request Timeout",
  409: "Conflict", 410: "Gone", 411: "Length Required",
  412: "Precondition Failed", 413: "Payload Too Large",
  414: "URI Too Long", 415: "Unsupported Media Type",
  416: "Range Not Satisfiable", 417: "Expectation Failed",
  418: "I'm a teapot", 421: "Misdirected Request",
  422: "Unprocessable Entity", 423: "Locked", 424: "Failed Dependency",
  425: "Too Early", 426: "Upgrade Required",
  428: "Precondition Required", 429: "Too Many Requests",
  431: "Request Header Fields Too Large",
  451: "Unavailable For Legal Reasons", 500: "Internal Server Error",
  501: "Not Implemented", 502: "Bad Gateway",
  503: "Service Unavailable", 504: "Gateway Timeout",
  505: "HTTP Version Not Supported", 506: "Variant Also Negotiates",
  507: "Insufficient Storage", 508: "Loop Detected",
  510: "Not Extended", 511: "Network Authentication Required",
};
const __wrapServiceResponse = (res, url) => {
  const wrapped = new Response(
    // Buffered bodies re-stream (an empty buffer still yields a
    // stream, as over real HTTP); a streaming body passes through.
    res._bodyBytes !== null ? res._bodyBytes : res.body,
    {
      status: res.status,
      statusText:
        res.statusText || __STATUS_TEXT[res.status] || "",
      headers: res.headers,
      webSocket: res.webSocket,
      __wsTarget: res._wsTarget,
    },
  );
  // The constructor copies headers, so mark the copy.
  Object.defineProperty(wrapped.headers, "_immutable", { value: true });
  wrapped.url = url;
  // An upgraded pair whose both ends live in this isolate: link them
  // directly — the host connection seam only reaches external
  // clients. Frames the handler queued before linking flush first.
  const client = wrapped.webSocket;
  if (wrapped.status === 101 && client && client._peer) {
    const server = client._peer;
    client._loopback = server;
    server._loopback = client;
    for (const frame of server._pending.splice(0)) {
      // "send-binary" joined this queue with outbound sockets; without a case
      // of its own it fell through to the close branch and tore the pair down
      // instead of delivering the frame.
      if (frame[0] === "send")
        queueMicrotask(() => client._dispatchMessage(frame[1]));
      else if (frame[0] === "send-binary")
        queueMicrotask(() => {
          const data = frame[1];
          client._dispatchMessage(
            data instanceof ArrayBuffer ? data : data.buffer.slice(
              data.byteOffset,
              data.byteOffset + data.byteLength,
            ),
          );
        });
      else
        queueMicrotask(
          () => client._dispatchClose(frame[1], frame[2], true));
    }
  }
  return wrapped;
};
// `[[services]]` binding: a Fetcher pointed at another Worker in this
// process. No identity to resolve, so it goes straight to __svc_call.
// Worker Loader (Code Mode): spawn a fresh isolate from supplied code and
// invoke it. Walking skeleton — only `load(code)` and a default-entrypoint
// `fetch()` are wired, mirroring the cross-isolate service-binding path below.
globalThis.__makeLoader = () => {
  // `get(name, …)` is memoized by name to one isolate; `load()` is anonymous.
  // A stub holds a Promise<id> so `getCode` may be async and load lazily.
  const byName = new Map();
  const makeEntrypoint = (idPromise, entrypoint) => {
    const target = {
      async fetch(input, init) {
        const id = await idPromise;
        const req = new Request(input, init);
        const headers = JSON.stringify(Array.from(req.headers));
        const body_ = req._bodyBytes === null
          ? await req._consume() : req._bodyBytes;
        const r = JSON.parse(
          await __loader_fetch(id, req.url, req.method, body_, headers));
        const body = r.streamId !== undefined
          ? new CelldHttpBodyStream(r.streamId)
          : r.body !== undefined ? r.body : Uint8Array.from(r.bodyBytes || []);
        return __wrapServiceResponse(
          new Response(body, { status: r.status, headers: r.headers }),
          req.url);
      },
    };
    // Both the default and named entrypoints expose RPC: property access is a
    // callable pipeline node dispatched via __loader_rpc to that entrypoint in
    // the loaded worker (default -> the "default" export, which
    // register_entrypoints registers like any other). fetch stays on the
    // target. Only single-method calls are supported for now.
    const session = {
      get: () => Promise.reject(new Error(
        "Awaitable properties on loaded workers are not supported yet.")),
      call: (path, args) => (async () => {
        if (path.length !== 1)
          throw new Error(
            "Pipelined property paths on loaded workers are not supported " +
            "yet.");
        const id = await idPromise;
        return __rpcDes(
          await __loader_rpc(id, entrypoint, path[0], __rpcOut(args, false)));
      })(),
    };
    return new Proxy(target, {
      get: (base, prop) => {
        if (prop === "then") return undefined;
        if (Reflect.has(base, prop)) return Reflect.get(base, prop);
        if (typeof prop !== "string") return undefined;
        return __makeNode(session, [prop], null);
      },
    });
  };
  // Anonymous load() workers are evicted when their only stub is GC'd: the
  // finalizer drops the worker's isolate so it does not leak. Named get()
  // workers are retained by `byName` (memoized) and so are not registered.
  const finalizer = typeof FinalizationRegistry === "function"
    ? new FinalizationRegistry((id) => __loader_drop(id))
    : null;
  const makeStub = (idPromise, evictable) => {
    // Explicit disposal evicts the worker deterministically; the finalizer is
    // a GC backstop for anonymous stubs that are dropped without disposing.
    // __loader_drop is idempotent, so the two paths cannot double-free.
    const drop = () => { idPromise.then((id) => __loader_drop(id), () => {}); };
    const stub = {
      getEntrypoint(name = null, _options = {}) {
        return makeEntrypoint(idPromise, name === null ? "default" : name);
      },
      dispose: drop,
    };
    if (typeof Symbol.dispose === "symbol") stub[Symbol.dispose] = drop;
    if (evictable && finalizer)
      idPromise.then((id) => finalizer.register(stub, id), () => {});
    return stub;
  };
  // JSON.stringify silently drops binary values, so each non-string module —
  // wasm bytes as a BufferSource, or workerd's `{ wasm }` / `{ esModule }`
  // module shapes — is normalized first: ES modules to plain strings, wasm
  // pulled out into a side-band `[name, Uint8Array]` list the op reads
  // directly, so multi-MB blobs never take a base64/JSON round-trip.
  const toBytes = (v) => v instanceof ArrayBuffer ? new Uint8Array(v)
    : ArrayBuffer.isView(v)
      ? new Uint8Array(v.buffer, v.byteOffset, v.byteLength) : null;
  const encodeModules = (c) => {
    if (c === null || typeof c !== "object" || c.modules === null
        || typeof c.modules !== "object") return { config: c, wasm: [] };
    const modules = {};
    const wasm = [];
    for (const [name, value] of Object.entries(c.modules)) {
      const wrapped = value !== null && typeof value === "object" ? value : {};
      const bytes = toBytes(value) ?? toBytes(wrapped.wasm);
      if (bytes !== null) wasm.push([name, bytes]);
      else if (typeof wrapped.esModule === "string") modules[name] = wrapped.esModule;
      else modules[name] = value;
    }
    return { config: { ...c, modules }, wasm };
  };
  // getCode is deferred into a microtask so a throw (or async getCode)
  // surfaces as a rejection when the worker is first used, not at get()/load().
  const loadFrom = (getCode) =>
    Promise.resolve().then(getCode)
      .then((c) => {
        const { config, wasm } = encodeModules(c);
        return __loader_load(JSON.stringify(config), wasm);
      });
  return {
    load(code) { return makeStub(loadFrom(() => code), true); },
    get(name, getCode) {
      let idPromise = byName.get(name);
      if (idPromise === undefined) {
        idPromise = loadFrom(getCode);
        byName.set(name, idPromise);
      }
      return makeStub(idPromise, false);
    },
  };
};

globalThis.__makeServiceBinding = (script, entrypoint = null) => {
  const target = {
  async fetch(input, init) {
    const req = new Request(input, init);
    const signal = req._signalForSubrequests;
    if (signal?.aborted) throw signal.reason;
    // A stream body advertises its length the way the HTTP layer
    // would: known length → Content-Length, unknown → chunked.
    if (req._bodyBytes === null &&
        !req.headers.has("content-length") &&
        !req.headers.has("transfer-encoding")) {
      const length = req.body._expectedLength;
      if (length === undefined)
        req.headers.set("transfer-encoding", "chunked");
      else req.headers.set("content-length", String(length));
    }
    // Fast path: the target is this same script, so its handler lives in
    // this isolate. Skip the op + pool-thread hop and call it directly,
    // inside its own event so the target's waitUntil does not attach to
    // the caller's. Cross-script targets still need their own isolate.
    if (script === __cell.script && entrypoint !== null) {
      // Mirror __dispatchTo: the target sees a fresh incoming signal,
      // aborted with "The client has disconnected" when the caller
      // abandons the call or cancels the response body; the caller's
      // own signal races the dispatch so an abandoned call rejects
      // immediately instead of pinning on a hung target.
      const requestController = new AbortController();
      const dispatch = __dispatchEntrypointFetch(
        entrypoint,
        new Request(req, {
          signal: requestController.signal,
          __celldIncomingSignal: true,
        }));
      const response = signal
        ? await __raceCallerAbort(dispatch, signal, () =>
            requestController.abort(
              new Error("The client has disconnected")))
        : await dispatch;
      __attachResponseRequestCancellation(
        response, requestController, true);
      return __wrapServiceResponse(response, req.url);
    }
    if (entrypoint !== null)
      throw new Error(
        "Cross-script service bindings with an entrypoint do not " +
        "support fetch() yet; only same-script targets do.");
    if (script === __cell.script && typeof __cell.selfFetch === "function") {
      if (__cell.svcDepth >= 8)
        throw new Error(
          "Service binding recursion limit exceeded (8)");
      __cell.svcDepth = (__cell.svcDepth || 0) + 1;
      const ctx = __beginEvent();
      try {
        return __wrapServiceResponse(
          await __ctxRun(undefined,
            () => __cell.selfFetch(req, __cell.env, ctx)),
          req.url);
      } finally {
        __cell.svcDepth--;
        __endEvent();
      }
    }
    const headers = JSON.stringify(Array.from(req.headers));
    // Cross-isolate calls carry exact bytes; a stream body is drained
    // (and per spec disturbed) first.
    const body_ = req._bodyBytes === null
      ? await req._consume() : req._bodyBytes;
    const r = JSON.parse(await (signal
      ? __awaitCancellableDoCall(
        __svc_call_cancellable(
          script, req.url, req.method, body_, headers),
        signal)
      : __svc_call(
        script, req.url, req.method, body_, headers)));
    const body = r.streamId !== undefined
      ? new CelldHttpBodyStream(r.streamId)
      : r.body !== undefined
        ? r.body
        : Uint8Array.from(r.bodyBytes || []);
    return __wrapServiceResponse(
      new Response(body, { status: r.status, headers: r.headers }),
      req.url);
  },
  // Workerd's test-visible Fetcher.scheduled(): invoke the target's
  // scheduled handler and report the outcome.
  async scheduled(options = {}) {
    if (script !== __cell.script || entrypoint !== null ||
        typeof __cell.selfScheduled !== "function")
      throw new Error(
        "scheduled() is only implemented for same-script service " +
        "bindings whose target has a scheduled handler");
    let noRetry = false;
    const ctrl = {
      scheduledTime: options.scheduledTime === undefined
        ? Date.now() : Number(options.scheduledTime),
      cron: options.cron === undefined ? "" : String(options.cron),
      noRetry() { noRetry = true; },
    };
    const ctx = __beginEvent();
    try {
      await __cell.selfScheduled(ctrl, __cell.env, ctx);
      return { outcome: "ok", noRetry };
    } finally {
      __endEvent();
    }
  },
  };
  if (entrypoint === null) return target;
  // With `entrypoint = "Name"`, any property other than fetch is an
  // awaitable/callable pipeline node rooted at that class: awaiting
  // resolves the property remotely (a property-GET wire op), calling
  // invokes it, and deeper access extends the path, resolved on the
  // receiver side in one op. Same-script dispatch stays in this
  // isolate, so stub-able values may cross. A property path rooted
  // directly at the binding is context-free (ctx null): awaiting it
  // starts a fresh session, as in Workerd.
  const session =
    __entrypointSession(entrypoint, script === __cell.script, script);
  return new Proxy(target, {
    getPrototypeOf: () => __cf.ServiceStub.prototype,
    get: (base, prop) => {
    if (prop === "then") return undefined; // a stub is not a thenable
    if (Reflect.has(base, prop)) return Reflect.get(base, prop);
    if (typeof prop !== "string") return undefined;
    // Workerd's test hook: the named method as a callable handle,
    // resolved — and refused — on the receiver side.
    if (prop === "getRpcMethodForTestOnly")
      return (name) => __makeNode(session, [String(name)], null);
    return __makeNode(session, [prop], null);
  }});
};
// RPC marshalling: V8 structured clone (Workerd js-rpc semantics), so
// undefined, Date, Map, Set, BigInt, typed arrays, and cycles survive.
// A value V8 cannot clone throws DataCloneError, as in Workerd.
//
// The envelope's first byte tags the payload. 0xff (the clone version
// header) = plain clone, decoded as-is — the common case pays one
// byte-compare and nothing else. 0x01 = clone of a lifted tree in
// which RpcTarget instances, functions, and Durable Object stubs were
// replaced by stub markers; the lift runs only after a plain clone
// already threw, so plain-data calls never walk. 0x02 = a lifted
// tree carrying only by-value host types (Blob/File, Headers,
// Request/Response) and no capabilities — decoded like 0x01, but a
// reply so tagged does not root its callee context. 0x00 = clone of
// [error, ownProps] — a callee exception crossing as a real Error.
const __dataCloneError = (error) => new DOMException(
  String(error && error.message || error), "DataCloneError");
const __tagged = (tag, sc) => {
  const out = new Uint8Array(sc.length + 1);
  out[0] = tag;
  out.set(sc, 1);
  return out;
};
// ---- request-context confinement -------------------------------
// Workerd ties I/O objects to the IoContext of the event that made
// them. Cells' equivalent: each dispatched event enters an
// async-context frame (the same CPED the ALS rides, so it survives
// awaits) carrying a context id. Stubs remember their owning
// context and refuse to serialize elsewhere; pipeline nodes refuse
// foreign awaits and calls — each with Workerd's exact error.
let __nextCtxId = 1;
const __ctxKey = Symbol("celld.ctx");
const __ctxNow = () => {
  const frame = __als_get();
  return frame === undefined ? undefined : frame.get(__ctxKey);
};
// Run `fn` under context `id` (undefined = a fresh one). An async
// fn started inside keeps the frame across its awaits. The prior
// frame is cloned so user AsyncLocalStorage stores still flow into
// same-isolate callees, as they did before contexts existed.
const __ctxRun = (id, fn) => {
  const prior = __als_get();
  const frame = new Map(prior);
  frame.set(__ctxKey, id ?? __nextCtxId++);
  __als_set(frame);
  try {
    return fn();
  } finally {
    __als_set(prior);
  }
};
const __ctxError = (kind) => new Error(
  "Cannot perform I/O on behalf of a different request. I/O " +
  "objects (such as streams, request/response bodies, and others) " +
  "created in the context of one request handler cannot be " +
  "accessed from a different request's handler. This is a " +
  "limitation of Cloudflare Workers which allows us to improve " +
  "overall performance. (I/O type: " + kind + ")");
// ---- abortable request contexts --------------------------------
// Workerd's ctx.abort(reason) on an entrypoint aborts the request's
// IoContext: the in-flight call rejects with the reason, and stubs
// the context holds are disposed (their disposal callbacks fire).
// State is allocated only when a context first holds a stub handle
// or aborts, so plain-data dispatch pays nothing; call paths gate
// every abort lookup on `__abortedCtxs.size` (one field read).
// Aborted ids stay recorded: a stub rooted in a dead context must
// keep rejecting with the reason.
const __abortedCtxs = new Map();
// An actor-flavored abort reason: in-flight ops reject with the
// wrapped reason, later ops with Workerd's post-abort message
// (upstream TODO(bug): should propagate the reason, but doesn't).
class __ActorAbort { constructor(reason) { this.reason = reason; } }
const __postAbortError = (aborted) => aborted instanceof __ActorAbort
  ? new Error("The execution context which hosts this callback " +
      "is no longer running.")
  : aborted;
// ctx id -> Set of live stub handles (metas) the context holds.
const __ctxStubs = new Map();
// ctx id -> Set of reject callbacks for stub ops in flight against
// that context. A hung callee never settles, so a context abort
// must reject its pending callers directly.
const __ctxPendingOps = new Map();
const __opTrack = (ctx, reject) => {
  let set = __ctxPendingOps.get(ctx);
  if (set === undefined) __ctxPendingOps.set(ctx, set = new Set());
  set.add(reject);
  return () => {
    set.delete(reject);
    if (set.size === 0) __ctxPendingOps.delete(ctx);
  };
};
const __ctxRegister = (meta) => {
  if (meta.ctx === undefined) return;
  let set = __ctxStubs.get(meta.ctx);
  if (set === undefined) __ctxStubs.set(meta.ctx, set = new Set());
  set.add(meta);
};
const __ctxUnregister = (meta) => {
  const set = __ctxStubs.get(meta.ctx);
  if (set === undefined) return;
  set.delete(meta);
  if (set.size === 0) __ctxStubs.delete(meta.ctx);
};
// Tear down a context: dispose every handle it still holds, so a
// dup() leaked into it reaches the sender's disposal callback.
const __ctxEnd = (id) => {
  const set = __ctxStubs.get(id);
  if (set === undefined) return;
  __ctxStubs.delete(id);
  for (const meta of set) __disposeStub(meta);
};
const __ctxAbort = (id, reason) => {
  if (id === undefined || __abortedCtxs.has(id)) return;
  const stored = reason === undefined
    ? new Error("The execution context has been aborted.")
    : reason;
  __abortedCtxs.set(id, stored);
  __ctxEnd(id);
  const pending = __ctxPendingOps.get(id);
  if (pending === undefined) return;
  __ctxPendingOps.delete(id);
  const live =
    stored instanceof __ActorAbort ? stored.reason : stored;
  for (const reject of pending) reject(live);
};
// The entrypoint ctx.abort. Instances are cached across calls, so
// the construction-time ctx cannot pin a request id (Workerd
// constructs per request); the current frame's id at abort() time
// is the request being served, which matches Workerd observably.
const __ctxAbortCurrent =
  (reason) => __ctxAbort(__ctxNow(), reason);
// ---- same-process RPC stubs ------------------------------------
// A stub entry owns a local target; `refs` counts live handles
// across dup()s. When the last handle is disposed the target's own
// Symbol.dispose runs (async, matching Workerd's disposal callback).
// Stubs cross same-isolate transports only (the DO owned fast path
// and same-script entrypoint RPC); a marker revived elsewhere fails
// loudly on use instead of aliasing an unrelated local entry.
// `ctx` records the owning request context: the entry's for
// running its target, the handle's for the serialize-elsewhere
// check.
const __stubEntries = new Map();
const __stubMeta = new WeakMap();
let __nextStubId = 1;
const __stubIsolate = Math.random().toString(36).slice(2);
const __newEntry = (target) => {
  // `scope` records the actor event that minted the entry (top of
  // the event stack at lift time), so an actor breakage can find
  // and abort the contexts hosting its exported stubs, and an op on
  // the stub can queue on that cell's input gate. `section` is the
  // critical section running at lift time, if any — an op on this
  // stub re-enters it rather than queueing behind it.
  const scope = __actorEventStack[__actorEventStack.length - 1];
  const entry = {
    id: __nextStubId++, target, refs: 1, ctx: __ctxNow(), scope,
    section: __cellBlocks.get(scope)?.holder ?? null,
  };
  __stubEntries.set(entry.id, entry);
  return entry;
};
// Actors broken in JS: scope -> reason. Workerd's DO ctx.abort
// rejects in-flight and future calls; when the abort fires under a
// same-isolate stub caller, terminate_execution would unwind that
// caller too (the routed dispatch re-enters the isolate), so the
// breakage is recorded here instead: contexts hosting the actor's
// exported stubs abort (in-flight ops reject with the reason,
// later ops with the post-abort message) and RPC dispatch to the
// scope rejects with the reason. Direct host-dispatched DO events
// keep the uncatchable terminate_execution path.
const __brokenActors = new Map();
const __actorBreak = (scope, reason) => {
  if (__brokenActors.has(scope)) return;
  __brokenActors.set(scope, reason);
  for (const entry of __stubEntries.values())
    if (entry.scope === scope)
      __ctxAbort(entry.ctx, new __ActorAbort(reason));
};
const __disposeStub = (meta) => {
  if (meta.disposed) return;
  meta.disposed = true;
  __ctxUnregister(meta);
  const entry = meta.entry;
  if (--entry.refs > 0) return;
  __stubEntries.delete(entry.id);
  const disposer = entry.target?.[Symbol.dispose];
  if (typeof disposer === "function")
    Promise.resolve().then(() => disposer.call(entry.target));
};
const __stubDisposedError = () =>
  new Error("RPC stub used after being disposed.");
// Shared brand value for RpcTarget instances; see the RpcTarget
// constructor.
const __rpcNoClone = () => {};
// Workerd method-visibility rules by target kind: an RpcTarget
// exposes inherited methods/accessors (never own instance state,
// never Object.prototype); a plain object or function exposes own
// properties only. Property reads go through normal [[Get]] so
// Proxy handlers participate, as in Workerd.
const __rpcNoSuchMethod = (prop) => new TypeError(
  'The RPC receiver does not implement the method "' + prop + '".');
const __stubResolve = (target, prop) => {
  if (target instanceof __cf.RpcTarget) {
    if (Object.hasOwn(target, prop) || !(prop in target) ||
        prop in Object.prototype) throw __rpcNoSuchMethod(prop);
  } else if (!Object.hasOwn(target, prop)) {
    throw __rpcNoSuchMethod(prop);
  }
  const value = target[prop];
  // Bind plain methods to their receiver, but never touch `.bind` on
  // a stub or pipeline node — property access on those is remote.
  return typeof value === "function" && !__stubMeta.has(value) &&
      !(value instanceof __cf.RpcPromise) &&
      !(value instanceof __cf.RpcProperty)
    ? value.bind(target) : value;
};
// ---- streams over RPC ------------------------------------------
// A live stream crosses as a handle in the caps table: the sender
// locks the origin (reader/writer acquired at lift) behind a
// bridge entry, and the receiver's endpoint is an ordinary
// ReadableStream/WritableStream whose pulls and writes are stub
// ops against the bridge — one bounded reverse call per chunk,
// the clone being the one wire copy. Backpressure is the op in
// flight: hwm 0 pulls only on demand, and the writable carries
// one chunk per op. EOF, close, and errors cross as op results;
// teardown (param disposal, context end, context abort) runs the
// bridge disposer, which cancels or aborts an unfinished origin
// with Workerd's generic disconnect errors — reasons do not
// propagate, matching Workerd's own TODOs (and its verbatim
// "endeded" typo).
const __wsDisconnect = () => new Error(
  "WritableStream received over RPC was disconnected because " +
  "the remote execution context has endeded.");
const __rsDisconnect = () => new Error(
  "ReadableStream received over RPC disconnected prematurely.");
// Receiver wrapper stream -> its handle meta, for forwarding.
const __rpcStreamMeta = new WeakMap();
const __readableBridge = (reader) => {
  const bridge = {
    done: false,
    async read() {
      let result;
      try {
        result = await reader.read();
      } catch (error) {
        bridge.done = true;
        throw error;
      }
      if (result.done) bridge.done = true;
      return result;
    },
    cancel() {
      if (bridge.done) return;
      bridge.done = true;
      reader.cancel(__rsDisconnect()).catch(() => {});
    },
    [Symbol.dispose]() { bridge.cancel(); },
  };
  return bridge;
};
const __writableBridge = (writer) => {
  const bridge = {
    done: false,
    write: (chunk) => writer.write(chunk),
    close() {
      bridge.done = true;
      return writer.close();
    },
    abort() {
      bridge.done = true;
      return writer.abort(__wsDisconnect());
    },
    [Symbol.dispose]() {
      if (!bridge.done) bridge.abort().catch(() => {});
    },
  };
  return bridge;
};
const __liftStream = (v) => {
  const readable = v instanceof ReadableStream;
  const key = readable ? "__celld$rs" : "__celld$ws";
  const meta = __rpcStreamMeta.get(v);
  if (meta !== undefined && !meta.disposed && !v.locked) {
    // Re-serializing a received, untouched stream forwards the
    // original handle: the reference moves (like a stub) and
    // the local wrapper is dead — a round trip stays one hop.
    meta.disposed = true;
    __ctxUnregister(meta);
    return { [key]: meta.entry.id, t: __stubIsolate };
  }
  if (v.locked)
    throw new TypeError(readable
      ? "The ReadableStream has been locked to a reader."
      : "The WritableStream has been locked to a writer.");
  const bridge = readable
    ? __readableBridge(v.getReader())
    : __writableBridge(v.getWriter());
  return { [key]: __newEntry(bridge).id, t: __stubIsolate };
};
// Replace stub-able values with wire markers. Runs only after a
// plain clone failed, so plain-data serialization never pays for
// it. Passing an existing stub transfers its reference: the
// sender's handle is disposed (dup() first to keep one) and the
// receiver adopts it. Returns null when nothing was liftable.
const __stubLift = (value) => {
  let lifted = false;
  // Capabilities (stubs, disposers) root the callee context;
  // by-value host types do not — they pick the 0x02 envelope.
  let caps = false;
  const seen = new Map();
  const ctx = __ctxNow();
  // A buffered body crosses as its bytes; a live-stream body
  // crosses as a stream handle (a capability, so the reply
  // roots its callee context and the pulls keep working).
  const liftBody = (v) => {
    if (v._bodyBytes !== null) return v._bodyBytes;
    caps = true;
    return __liftStream(v.body);
  };
  const lift = (v) => {
    if (v === null ||
        (typeof v !== "object" && typeof v !== "function")) return v;
    const cached = seen.get(v);
    if (cached !== undefined) return cached;
    const meta = __stubMeta.get(v);
    if (meta) {
      lifted = true;
      caps = true;
      if (meta.disposed) throw __stubDisposedError();
      // A stub belongs to the request that received it; another
      // request cannot serialize it (Workerd's IoContext rule).
      if (meta.ctx !== ctx) throw __ctxError("Client");
      meta.disposed = true; // the ref moves to the receiver
      __ctxUnregister(meta);
      const marker = { "__celld$stub": meta.entry.id,
                       t: __stubIsolate, c: meta.callable };
      seen.set(v, marker);
      return marker;
    }
    const svc = __svcMeta.get(v);
    if (svc !== undefined) {
      // A loopback service stub (ctx.exports): name + props cross
      // as plain data and revive as a fresh loopback stub. Props
      // are lifted too — they may nest further stubs (Workerd's
      // nested channel tokens).
      lifted = true;
      caps = true;
      const marker = { "__celld$svc": svc.name, t: __stubIsolate };
      seen.set(v, marker);
      if (svc.props !== undefined) marker.p = lift(svc.props);
      return marker;
    }
    // Workerd refuses to serialize its promise/property handles.
    if (v instanceof __cf.RpcPromise || v instanceof __cf.RpcProperty)
      throw new DOMException(
        'Could not serialize object of type "' +
        (v instanceof __cf.RpcPromise ? "RpcPromise" : "RpcProperty") +
        '". This type does not support serialization.',
        "DataCloneError");
    if (typeof v === "function" || v instanceof __cf.RpcTarget) {
      lifted = true;
      caps = true;
      const marker = { "__celld$stub": __newEntry(v).id,
                       t: __stubIsolate,
                       c: typeof v === "function" };
      seen.set(v, marker);
      return marker;
    }
    const doId = v.__celldDo;
    if (doId !== undefined) {
      lifted = true;
      caps = true;
      const marker = { "__celld$do": doId._className,
                       v: doId._value, n: v.name ?? null };
      seen.set(v, marker);
      return marker;
    }
    // HTTP host types cross by value, as in Workerd's RPC
    // serialization: entry lists for Headers, the buffered bytes
    // for bodies (the marker aliases the live buffer; the clone
    // is the one wire copy), and no signal — the receiver mints
    // a fresh one. A live stream body cannot cross yet.
    let marker;
    if (v instanceof Headers) {
      marker = { "__celld$hdr": [...v] };
    } else if (v instanceof Blob) {
      marker = { "__celld$blob": v._bytes, y: v.type };
      if (v instanceof File) {
        marker.n = v.name;
        marker.m = v.lastModified;
      }
    } else if (v instanceof Request) {
      marker = { "__celld$req": v.url, m: v.method,
                 h: [...v.headers], r: v.redirect, c: v.cf,
                 b: liftBody(v) };
    } else if (v instanceof Response) {
      marker = { "__celld$res": v.status,
                 t: v.statusText ||
                    __STATUS_TEXT[v.status] || "",
                 h: [...v.headers], c: v.cf,
                 b: v.body === null ? null : liftBody(v) };
    } else if (v instanceof ReadableStream ||
               v instanceof WritableStream) {
      caps = true;
      marker = __liftStream(v);
    } else if (v instanceof AbortSignal) {
      // A live signal handle: the receiver mints a fresh signal
      // wired to this one (same isolate); foreign bytes revive
      // a snapshot of the aborted flag.
      caps = true;
      marker = { "__celld$sig": __newEntry(v).id,
                 t: __stubIsolate, a: v.aborted };
    }
    if (marker !== undefined) {
      lifted = true;
      seen.set(v, marker);
      return marker;
    }
    const proto = Object.getPrototypeOf(v);
    if (!Array.isArray(v) &&
        proto !== Object.prototype && proto !== null) {
      // A Proxy emulating neither a plain object nor an RpcTarget
      // is Workerd's canonical proxy serialization error.
      if (__util_proxy_details(v) !== undefined)
        throw new DOMException(
          "Proxy could not be serialized because it is not a " +
          "valid RPC receiver type. The Proxy must emulate either " +
          "a plain object or an RpcTarget, as indicated by the " +
          "Proxy's prototype chain.", "DataCloneError");
      return v; // host/other: leave to the clone
    }
    const out = Array.isArray(v) ? [] : {};
    seen.set(v, out);
    for (const key of Object.keys(v)) out[key] = lift(v[key]);
    const disposer = v[Symbol.dispose];
    if (!Array.isArray(v) && typeof disposer === "function") {
      lifted = true;
      caps = true;
      out["__celld$disp"] = __newEntry(disposer.bind(v)).id;
    }
    return out;
  };
  const tree = lift(value);
  return lifted ? { tree, caps } : null;
};
// Revive host-type markers into real instances, adopting the wire
// bytes directly — no copy beyond the clone's own.
const __reviveBlob = (marker, bytes) => {
  const blob = marker.n !== undefined
    ? new File([], marker.n,
        { type: marker.y, lastModified: marker.m })
    : new Blob([], { type: marker.y });
  blob._bytes = bytes;
  blob.size = bytes.byteLength;
  return blob;
};
const __adoptBody = (target, bytes) => {
  target._bodyBytes = bytes;
  target._body = undefined; // decoded lazily on the first text()/json()
  const body = target.body;
  if (body !== null) {
    body._st.bytes = bytes;
    body.__celldBodyBytes = bytes;
    body._expectedLength = bytes.byteLength;
  }
};
// A non-bytes `b` is a live-stream body marker: revive it and
// let the constructor adopt it as a streaming body.
const __reviveRequest = (marker, url, revive) => {
  if (!(marker.b instanceof Uint8Array))
    return new Request(url, { method: marker.m,
      headers: marker.h, redirect: marker.r, cf: marker.c,
      body: revive(marker.b) });
  const req = new Request(url, { method: marker.m,
    headers: marker.h, redirect: marker.r, cf: marker.c });
  __adoptBody(req, marker.b);
  return req;
};
const __reviveResponse = (marker, revive) => {
  const bytes = marker.b instanceof Uint8Array;
  const res = new Response(
    marker.b === null ? null : bytes ? "" : revive(marker.b), {
      status: marker["__celld$res"], statusText: marker.t,
      headers: marker.h, cf: marker.c });
  if (bytes) __adoptBody(res, marker.b);
  return res;
};
// Wire a received stream handle to a local endpoint built with
// the ordinary constructors — nothing new threads through the
// stream hot paths. Read errors surface as Workerd's generic
// premature-disconnect; write errors propagate (Workerd sends
// real errors back through the write loop).
const __foreignStreamOp = () => Promise.reject(new Error(
  "RPC streams cannot cross isolate boundaries yet."));
const __reviveStream = (marker, id, readable, handles) => {
  const entry = marker.t === __stubIsolate
    ? __stubEntries.get(id) : undefined;
  if (entry === undefined)
    return readable
      ? new ReadableStream({ pull: __foreignStreamOp })
      : new WritableStream({ write: __foreignStreamOp,
          close: __foreignStreamOp, abort: __foreignStreamOp });
  const meta = { entry, disposed: false, ctx: __ctxNow() };
  __ctxRegister(meta);
  handles.push(meta);
  const stream = readable
    ? new ReadableStream({
        async pull(controller) {
          let result;
          try {
            result = await __stubOp(meta, ["read"], []);
          } catch {
            throw __rsDisconnect();
          }
          if (result.done) controller.close();
          else controller.enqueue(result.value);
        },
        cancel: () =>
          __stubOp(meta, ["cancel"], []).catch(() => {}),
      }, { highWaterMark: 0 })
    : new WritableStream({
        write: (chunk) => __stubOp(meta, ["write"], [chunk]),
        close: () => __stubOp(meta, ["close"], []),
        abort: () =>
          __stubOp(meta, ["abort"], []).catch(() => {}),
      });
  __rpcStreamMeta.set(stream, meta);
  return stream;
};
const __reviveSignal = (marker, id) => {
  const entry = marker.t === __stubIsolate
    ? __stubEntries.get(id) : undefined;
  const controller = new AbortController();
  if (entry === undefined) {
    if (marker.a) controller.abort();
    return controller.signal;
  }
  __stubEntries.delete(id);
  const signal = entry.target;
  if (signal.aborted) controller.abort(signal.reason);
  else signal.addEventListener("abort",
    () => controller.abort(signal.reason), { once: true });
  return controller.signal;
};
// The inverse: markers become live handles. `handles` collects the
// revived stubs (for call-end param disposal or result-tree
// disposal); `disposers` collects remote Symbol.dispose entries.
const __stubRevive = (value) => {
  const handles = [];
  const disposers = [];
  const seen = new Set();
  const revive = (v) => {
    if (v === null || typeof v !== "object") return v;
    const stubId = v["__celld$stub"];
    if (stubId !== undefined) {
      const entry = v.t === __stubIsolate
        ? __stubEntries.get(stubId) : undefined;
      const stub = entry === undefined
        ? __foreignStub()
        : __makeStub(entry, v.c);
      const meta = __stubMeta.get(stub);
      if (meta) handles.push(meta);
      return stub;
    }
    const svcName = v["__celld$svc"];
    if (svcName !== undefined)
      return v.t === __stubIsolate
        ? __entrypointStub(svcName, revive(v.p))
        : __foreignStub();
    const doClass = v["__celld$do"];
    if (doClass !== undefined) {
      const namespace = __cell.makeNamespace(doClass);
      return namespace.get(
        new DurableObjectId(doClass, v.v, v.n ?? undefined));
    }
    const hdr = v["__celld$hdr"];
    if (hdr !== undefined) return new Headers(hdr);
    const blobBytes = v["__celld$blob"];
    if (blobBytes !== undefined) return __reviveBlob(v, blobBytes);
    const reqUrl = v["__celld$req"];
    if (reqUrl !== undefined)
      return __reviveRequest(v, reqUrl, revive);
    if (v["__celld$res"] !== undefined)
      return __reviveResponse(v, revive);
    const rsId = v["__celld$rs"];
    if (rsId !== undefined)
      return __reviveStream(v, rsId, true, handles);
    const wsId = v["__celld$ws"];
    if (wsId !== undefined)
      return __reviveStream(v, wsId, false, handles);
    const sigId = v["__celld$sig"];
    if (sigId !== undefined) return __reviveSignal(v, sigId);
    if (seen.has(v)) return v;
    seen.add(v);
    if (Array.isArray(v)) {
      for (let i = 0; i < v.length; i++) v[i] = revive(v[i]);
      return v;
    }
    const proto = Object.getPrototypeOf(v);
    if (proto !== Object.prototype && proto !== null) return v;
    for (const key of Object.keys(v)) {
      if (key === "__celld$disp") {
        const entry = __stubEntries.get(v[key]);
        if (entry) disposers.push(entry);
        delete v[key];
        continue;
      }
      v[key] = revive(v[key]);
    }
    return v;
  };
  return { value: revive(value), handles, disposers };
};
// A marker that crossed an isolate boundary: fail on use, loudly.
const __foreignStub = () => new Proxy(function () {}, {
  get: (_b, prop) => {
    if (prop === "then" || typeof prop !== "string") return undefined;
    return () => Promise.reject(new Error(
      "RPC stubs cannot cross isolate boundaries yet."));
  },
  apply: () => Promise.reject(new Error(
    "RPC stubs cannot cross isolate boundaries yet.")),
});
// Workerd's entrypoint method-visibility rules (worker-rpc.c++):
// reserved lifecycle names are refused outright; only prototype
// methods and accessors are visible — never own instance state
// (env/ctx live there) and never Object.prototype.
const __entrypointReserved = new Set([
  "constructor", "fetch", "connect", "alarm", "scheduled",
  "webSocketMessage", "webSocketClose", "webSocketError", "dup",
]);
const __entrypointResolve = (inst, prop) => {
  if (__entrypointReserved.has(prop))
    throw new TypeError("'" + prop +
      "' is a reserved method and cannot be called over RPC.");
  if (Object.hasOwn(inst, prop) || !(prop in inst) ||
      prop in Object.prototype)
    throw __rpcNoSuchMethod(prop);
  const value = inst[prop];
  return typeof value === "function" && !__stubMeta.has(value) &&
      !(value instanceof __cf.RpcPromise) &&
      !(value instanceof __cf.RpcProperty)
    ? value.bind(inst) : value;
};
// A pipeline hop may continue only through plain data, functions,
// RpcTargets, and stubs — never through an RPC promise/property
// handle or a class instance (Workerd's receiver rules).
const __walkable = (v) =>
  v instanceof __cf.RpcPromise || v instanceof __cf.RpcProperty
    ? false
    : typeof v === "function" || v instanceof __cf.RpcTarget ||
      (typeof v === "object" && v !== null &&
        (Object.getPrototypeOf(v) === Object.prototype ||
          Array.isArray(v)));
// Receiver-side pipeline walk: resolve `path` from `root`, then
// GET the final member (`args` null) or CALL it. Errors name the
// path walked so far, as Workerd's do. A stub mid-walk continues
// against its own target, in the stub's owning context.
const __rpcWalk = async (root, path, args, entrypointRoot) => {
  let value = root;
  for (let i = 0; i < path.length; i++) {
    const meta = __stubMeta.get(value);
    if (meta) {
      if (meta.disposed) throw __stubDisposedError();
      const rest = path.slice(i);
      return await __ctxRun(meta.entry.ctx,
        () => __rpcWalk(meta.entry.target, rest, args, false));
    }
    if (i > 0 && !__walkable(value))
      throw __rpcNoSuchMethod(path[i - 1]);
    const prop = path[i];
    const next = entrypointRoot && i === 0
      ? __entrypointResolve(value, prop)
      : __stubResolve(value, prop);
    if (i === path.length - 1) {
      if (args === null) return next;
      if (typeof next !== "function")
        throw new TypeError(
          '"' + path.join(".") + '" is not a function.');
      return next(...args);
    }
    if (next instanceof __cf.RpcPromise ||
        next instanceof __cf.RpcProperty)
      throw __rpcNoSuchMethod(prop);
    value = next;
  }
  // An empty path applies the root itself (a callable stub).
  if (args === null) return value;
  if (typeof value !== "function")
    throw new TypeError("The RPC stub is not callable.");
  return value(...args);
};
// One stub-mediated op: clone through the RPC envelope both ways,
// so nested stubs, errors, and uncloneables behave exactly as a
// dispatched call. Args serialize in the caller's context (its
// stubs must be its own to transfer); the decode, the walk, and
// the reply serialization run in the stub's owning context, so
// stubs minted by the target belong to the target's context.
// Params received by the target are disposed when the op ends
// (Workerd's param-disposal rule).
const __stubOp = (meta, path, args) => {
  if (meta.disposed) return Promise.reject(__stubDisposedError());
  const entry = meta.entry;
  // A stub rooted in an aborted context is broken: reject with the
  // abort reason, before and after the dispatch (the target may
  // abort its own context mid-call). Gated on one field read.
  if (__abortedCtxs.size !== 0) {
    const aborted = __abortedCtxs.get(entry.ctx);
    if (aborted !== undefined)
      return Promise.reject(__postAbortError(aborted));
  }
  const argsSc = args === null ? null : __rpcOut(args, true);
  const dispatch = (async () => {
    // Workerd delivers RPC asynchronously: the callee must not run
    // before the caller's synchronous code. A call made just after
    // a not-yet-delivered ctx.abort counts as in flight (rejects
    // with the reason, not the post-abort message). Bodies queue
    // in call order, so e-order holds.
    await null;
    // The cell that minted this stub gates it like any other event.
    // The op is dispatched inside the isolate and never becomes a
    // drive, so it is the one delivery point that has to ask here
    // rather than in Rust. A stub carrying the running critical
    // section re-enters it; everything else queues, and a section
    // that failed refuses what queued.
    const cell = entry.scope;
    if (cell !== undefined) {
      const block = __cellBlocks.get(cell);
      if (block !== undefined && (entry.section === null ||
          entry.section !== block.holder))
        await __gate_wait(cell);
    }
    const reply = await __ctxRun(entry.ctx,
      () => __rpcRun(async () => {
        const decoded =
          argsSc === null ? null : __rpcDesArgs(argsSc);
        try {
          return await __rpcWalk(entry.target, path,
            decoded === null ? null : decoded.args, false);
        } finally {
          if (decoded !== null)
            for (const handle of decoded.received)
              __disposeStub(handle);
        }
      }, true));
    if (__abortedCtxs.size !== 0) {
      const aborted = __abortedCtxs.get(entry.ctx);
      if (aborted !== undefined)
        throw aborted instanceof __ActorAbort
          ? aborted.reason : aborted;
    }
    return __rpcDes(reply);
  })();
  if (entry.ctx === undefined) return dispatch;
  // Race the op against its hosting context's abort: a hung callee
  // never settles, so the settlement check above cannot reach it.
  // A late settlement lands on an already-settled promise (no-op).
  return new Promise((resolve, reject) => {
    const untrack = __opTrack(entry.ctx, reject);
    dispatch.then(
      (value) => { untrack(); resolve(value); },
      (error) => { untrack(); reject(error); });
  });
};
// Resolve a path against a local, already-revived value. A hop
// landing on a same-isolate stub delegates the rest of the path
// to the stub's target; everything else is a plain [[Get]] so
// Durable Object stubs and Proxies participate.
const __walkLocal = (parent, path, args) => {
  for (let i = 0; i < path.length; i++) {
    const meta = __stubMeta.get(parent);
    if (meta) return __stubOp(meta, path.slice(i), args);
    const prop = path[i];
    const member = parent == null ? undefined : parent[prop];
    if (i < path.length - 1) {
      parent = member;
      continue;
    }
    if (args === null) return member;
    if (typeof member !== "function") {
      if (member === undefined) throw __rpcNoSuchMethod(prop);
      throw new TypeError('"' + prop + '" is not a function.');
    }
    // [[Call]] directly: `.apply` on a stub proxy would be a
    // remote property access, not an invocation.
    return Reflect.apply(member, parent, args);
  }
  if (args === null) return parent;
  const meta = __stubMeta.get(parent);
  if (meta) return __stubOp(meta, [], args);
  if (typeof parent !== "function")
    throw new TypeError("The RPC value is not callable.");
  return parent(...args);
};
// A session is one place pipeline ops resolve: the local value a
// call returned, a same-isolate stub's target, or a named
// entrypoint (whose paths resolve receiver-side in one op).
const __valueSession = (promise) => ({
  root: () => promise,
  get: (path) => promise.then((v) => __walkLocal(v, path, null)),
  call: (path, args) =>
    promise.then((v) => __walkLocal(v, path, args)),
});
const __stubSession = (meta) => ({
  get: (path) => __stubOp(meta, path, null),
  call: (path, args) => __stubOp(meta, path, args),
});
const __entrypointSession = (name, local, script, makeInst) => ({
  get: (path) => local
    ? (async () => __rpcDes(
        await __entrypointOp(name, path, null, true, makeInst)))()
    : Promise.reject(new Error(
        "Awaitable properties on cross-script service bindings " +
        "are not supported yet.")),
  call: (path, args) => (async () => {
    const argsSc = __rpcOut(args, local);
    if (local)
      return __rpcDes(await __entrypointOp(
        name, path, argsSc, true, makeInst));
    if (path.length !== 1)
      throw new Error(
        "Pipelined property paths on cross-script service " +
        "bindings are not supported yet.");
    return __rpcDes(
      await __svc_rpc(script, name, path[0], argsSc));
  })(),
});
// Workerd's JsRpcPromise/JsRpcProperty: awaitable, callable, and
// property access extends a path resolved at the far end, so
// intermediates obey Workerd's receiver rules. `ctx` is the node's
// owning request context — null marks a context-free node (a
// property path rooted directly at a service binding, which
// starts a fresh session per await); a foreign context awaiting a
// property or calling through the node gets Workerd's
// cross-context error. Depth is capped like Workerd's
// MAX_PROPERTY_DEPTH.
const __makeNode = (session, path, ctx) => {
  let promise;
  const value = () => promise ??= (() => {
    if (path.length === 0) return session.root();
    if (ctx !== null && ctx !== __ctxNow())
      return Promise.reject(__ctxError("Pipeline"));
    return session.get(path);
  })();
  const brand =
    path.length === 0 ? __cf.RpcPromise : __cf.RpcProperty;
  return new Proxy(function () {}, {
    getPrototypeOf: () => brand.prototype,
    get: (_b, p) => {
      if (p === "then")
        return (onOk, onErr) => value().then(onOk, onErr);
      if (p === "catch") return (onErr) => value().catch(onErr);
      if (p === "finally")
        return (onDone) => value().finally(onDone);
      if (typeof p !== "string") return undefined;
      if (path.length >= 5120)
        throw new TypeError(
          "RPC pipelined property chain is too deep.");
      return __makeNode(session, [...path, p], ctx);
    },
    apply: (_b, _this, args) => {
      let call;
      if (ctx !== null && ctx !== __ctxNow()) {
        call = Promise.reject(__ctxError("JsRpcPromise"));
        call.catch(() => {});
      } else {
        // Eager, so concurrent calls keep e-order.
        call = session.call(path, args);
      }
      return __makeNode(
        __valueSession(call), [], ctx ?? __ctxNow());
    },
  });
};
const __makeStub = (entry, callable) => {
  const meta =
    { entry, callable, disposed: false, ctx: __ctxNow() };
  __ctxRegister(meta);
  const stub = new Proxy(function () {}, {
    getPrototypeOf: () => __cf.RpcStub.prototype,
    get: (_b, prop) => {
      if (prop === "then") return undefined;
      if (prop === Symbol.dispose) return () => __disposeStub(meta);
      if (prop === "dup") return () => {
        if (meta.disposed) throw __stubDisposedError();
        entry.refs++;
        return __makeStub(entry, callable);
      };
      if (typeof prop !== "string") return undefined;
      return __makeNode(__stubSession(meta), [prop], __ctxNow());
    },
    apply: (_b, _this, args) => __makeNode(
      __valueSession(__stubOp(meta, [], args)), [], __ctxNow()),
  });
  __stubMeta.set(stub, meta);
  return stub;
};
// A loopback service stub for one of this worker's own
// entrypoints — the ctx.exports surface. Calling the stub itself
// returns a new stub carrying per-instance props, delivered to
// the class constructor as ctx.props (Workerd's
// ctx.exports.Name({ props })).
const __svcMeta = new WeakMap();
const __entrypointStub = (name, props) => {
  let inst;
  const makeInst = props === undefined ? undefined : () => {
    if (inst !== undefined) return inst;
    const cls = __cell.entrypoints[name];
    if (typeof cls !== "function")
      throw new TypeError(
        "The entrypoint " + name + " cannot carry props.");
    // Construction gets its own event, like __entrypointInstance.
    const ctx = __beginEvent(props);
    try {
      inst = new cls(ctx, __cell.env);
    } finally {
      __endEvent();
    }
    return inst;
  };
  const session = __entrypointSession(name, true, null, makeInst);
  const stub = new Proxy(function () {}, {
    getPrototypeOf: () => __cf.ServiceStub.prototype,
    get: (_b, prop) => {
      if (prop === "then") return undefined;
      if (typeof prop !== "string") return undefined;
      if (prop === "getRpcMethodForTestOnly")
        return (n) => __makeNode(session, [String(n)], null);
      return __makeNode(session, [prop], null);
    },
    apply: (_b, _this, args) =>
      __entrypointStub(name, args[0]?.props),
  });
  __svcMeta.set(stub, { name, props });
  return stub;
};
// ---- stored stubs ----------------------------------------------
// Durable Object storage accepts only stubs with durable identity:
// loopback service stubs (entrypoint name + props re-mint a fresh
// stub on read) and Durable Object stubs (HMAC'd id + class revive
// in any isolate). A transient handle — a received RPC stub, a
// function, an RpcTarget — dies with its isolate; persisting its
// entry id would revive garbage after a restart, so it is refused.
// A stub-bearing row is written as 0x01 + clone(marker tree), off
// the plain-clone fast path (the lift runs only after the plain
// clone already threw, exactly like the RPC envelope). Reads hand
// tagged rows to JS as [sentinel, tree]; the sentinel never leaves
// this script and no user value can decode to it, so revival
// cannot be spoofed by data shaped like a marker.
const __storedSentinel = {};
const __storedLift = (value) => {
  let lifted = false;
  const seen = new Map();
  const lift = (v) => {
    if (v === null ||
        (typeof v !== "object" && typeof v !== "function")) return v;
    const cached = seen.get(v);
    if (cached !== undefined) return cached;
    // Ordering mirrors __stubLift: identify proxies by their
    // side tables and brands before touching any property — a
    // stub or pipeline proxy answers every property read with a
    // fresh RpcProperty node.
    if (__stubMeta.has(v) || v instanceof __cf.RpcTarget)
      throw new DOMException(
        "Durable Object storage can only store stubs with " +
        "durable identity: service stubs from ctx.exports and " +
        "Durable Object stubs. This value is a transient RPC " +
        "handle that would not survive a restart.",
        "DataCloneError");
    const svc = __svcMeta.get(v);
    if (svc !== undefined) {
      lifted = true;
      const marker = { "__celld$svc": svc.name };
      seen.set(v, marker);
      if (svc.props !== undefined) marker.p = lift(svc.props);
      return marker;
    }
    if (v instanceof __cf.RpcPromise ||
        v instanceof __cf.RpcProperty)
      throw new DOMException(
        'Could not serialize object of type "' +
        (v instanceof __cf.RpcPromise ? "RpcPromise" : "RpcProperty") +
        '". This type does not support serialization.',
        "DataCloneError");
    if (typeof v === "function") return v; // leave to the clone
    const doId = v.__celldDo;
    if (doId !== undefined) {
      lifted = true;
      const marker = { "__celld$do": doId._className,
                       v: doId._value, n: v.name ?? null };
      seen.set(v, marker);
      return marker;
    }
    if (Array.isArray(v)) {
      const out = [];
      seen.set(v, out);
      for (let i = 0; i < v.length; i++) out[i] = lift(v[i]);
      return out;
    }
    const proto = Object.getPrototypeOf(v);
    if (proto !== Object.prototype && proto !== null)
      return v; // host/other: leave to the clone
    const out = {};
    seen.set(v, out);
    for (const key of Object.keys(v)) out[key] = lift(v[key]);
    return out;
  };
  const tree = lift(value);
  return lifted ? tree : null;
};
// Encode one stub-bearing value for storage. Rethrows the original
// clone error when nothing was liftable, so plain uncloneables
// fail exactly as they always did.
const __storedBytes = (value, error) => {
  const tree = __storedLift(value);
  if (tree === null) throw error;
  return __tagged(1, __sc_encode(tree));
};
const __storedRevive = (value) => {
  const seen = new Set();
  const revive = (v) => {
    if (v === null || typeof v !== "object") return v;
    const svcName = v["__celld$svc"];
    if (svcName !== undefined)
      return __entrypointStub(svcName, revive(v.p));
    const doClass = v["__celld$do"];
    if (doClass !== undefined)
      return __cell.makeNamespace(doClass).get(
        new DurableObjectId(doClass, v.v, v.n ?? undefined));
    if (seen.has(v)) return v;
    seen.add(v);
    if (Array.isArray(v)) {
      for (let i = 0; i < v.length; i++) v[i] = revive(v[i]);
      return v;
    }
    const proto = Object.getPrototypeOf(v);
    if (proto !== Object.prototype && proto !== null) return v;
    for (const key of Object.keys(v)) v[key] = revive(v[key]);
    return v;
  };
  return revive(value);
};
const __unwrapStored = (v) =>
  Array.isArray(v) && v[0] === __storedSentinel
    ? __storedRevive(v[1]) : v;
// A map result is wrapped as a whole only when it holds at least
// one tagged row, so the per-entry walk never runs on plain data.
const __unwrapStoredMap = (v) => {
  if (!Array.isArray(v) || v[0] !== __storedSentinel) return v;
  const map = v[1];
  for (const [key, value] of map)
    map.set(key, __unwrapStored(value));
  return map;
};
// ctx.exports: loopback stubs for every exported entrypoint plus
// this worker's Durable Object namespaces. Built once, on first
// access — ctx construction itself only carries the getter.
let __ctxExportsCache;
const __ctxExports = () => __ctxExportsCache ??= (() => {
  const out = {};
  for (const name of Object.keys(__cell.entrypoints))
    out[name] = __entrypointStub(name, undefined);
  for (const name of Object.keys(__cell.objectEntrypoints))
    if (name !== "default")
      out[name] = __entrypointStub(name, undefined);
  for (const name of Object.keys(__cell.namespaceKeys))
    out[name] = __cell.makeNamespace(name);
  return out;
})();
// ---- RPC envelope ----------------------------------------------
// Serialize one payload. `lift` marks a same-isolate transport,
// where stub-able values may cross as markers; elsewhere they stay
// a DataCloneError, exactly as before stubs existed.
const __rpcOut = (value, lift) => {
  try {
    return __sc_encode(value);
  } catch (error) {
    const lifted = lift ? __stubLift(value) : null;
    if (lifted === null) throw __dataCloneError(error);
    try {
      return __tagged(
        lifted.caps ? 1 : 2, __sc_encode(lifted.tree));
    } catch (error_) {
      throw __dataCloneError(error_);
    }
  }
};
// A callee exception as tagged bytes: the Error crosses by value
// (V8 serializes Error natively), custom own properties beside it.
const __rpcErrOut = (error) => {
  // `name` rides in the props: V8 only round-trips the standard
  // Error subclass names, and e.g. DataCloneError must survive.
  const props = error instanceof Error
    ? { ...error, name: error.name } : {};
  let sc;
  try {
    sc = __sc_encode([error, props]);
  } catch {
    const error_ = new Error(String(error?.message ?? error));
    sc = __sc_encode([error_, { name: String(error?.name ?? "Error") }]);
  }
  return __tagged(0, sc);
};
// The callee half of one RPC: run `body`, answer tagged bytes.
const __rpcRun = async (body, lift) => {
  try {
    return __rpcOut(await body(), lift);
  } catch (error) {
    return __rpcErrOut(error);
  }
};
const __rpcDesArgs = (bytes) => {
  if (bytes[0] === 0xff)
    return { args: __sc_decode(bytes), received: [] };
  const revived = __stubRevive(__sc_decode(bytes.subarray(1)));
  return { args: revived.value, received: revived.handles };
};
// The caller half: decode a reply, rebuilding stubs and rethrowing
// callee exceptions as real Errors with the callee's own
// properties, `.remote`, and a local (caller-side) stack.
const __rpcDes = (bytes) => {
  if (bytes[0] === 0xff) return __sc_decode(bytes);
  if (bytes[0] === 1 || bytes[0] === 2) {
    const { value, handles, disposers } =
      __stubRevive(__sc_decode(bytes.subarray(1)));
    if (value !== null && typeof value === "object" &&
        !__stubMeta.has(value) &&
        (handles.length > 0 || disposers.length > 0)) {
      Object.defineProperty(value, Symbol.dispose, {
        configurable: true,
        value: () => {
          for (const handle of handles) __disposeStub(handle);
          for (const entry of disposers) {
            __stubEntries.delete(entry.id);
            Promise.resolve().then(() => entry.target());
          }
        },
      });
    }
    return value;
  }
  const [error, props] = __sc_decode(bytes.subarray(1));
  if (error instanceof Error) {
    Object.assign(error, props);
    error.remote = true;
    const local = new Error().stack;
    error.stack = error.name + ": " + error.message +
      local.slice(local.indexOf("\n"));
  }
  throw error;
};
// Deprecated Fetcher `get()`/`put()`/`delete()` HTTP helpers, kept by
// the fetcher_has_get_put_delete compat flag (Workerd http.c++):
// shortcuts for fetch() with the corresponding method.
const __fetcherStatus = (res, method) => {
  if (res.status >= 200 && res.status < 300) return;
  throw new Error("HTTP " + method + " request failed: " + res.status +
    " " + (res.statusText || __STATUS_TEXT[res.status] || ""));
};
const __fetcherHelper = (fetch, prop) => {
  if (prop === "get") return async (url, type) => {
    const res = await fetch(url, { method: "GET" });
    if (res.status === 404 || res.status === 410) return null;
    __fetcherStatus(res, "GET");
    if (type === "stream")
      return res.body ?? new ReadableStream({ start(c) { c.close(); } });
    if (type === "arrayBuffer") return res.arrayBuffer();
    if (type === "json") return res.json();
    return res.text();
  };
  if (prop === "put") return async (url, body, options) => {
    const { expiration, expirationTtl } = options ?? {};
    if (expiration !== undefined || expirationTtl !== undefined) {
      const url_ = new URL(url);
      if (expiration !== undefined)
        url_.searchParams.append("expiration", expiration);
      if (expirationTtl !== undefined)
        url_.searchParams.append("expiration_ttl", expirationTtl);
      url = url_.toString();
    }
    __fetcherStatus(await fetch(url, { method: "PUT", body }), "PUT");
  };
  return async (url) => {
    __fetcherStatus(await fetch(url, { method: "DELETE" }), "DELETE");
  };
};
function makeNamespace(className) {
  const namespaceKey = __cell.namespaceKeys[className];
  if (typeof namespaceKey !== "string")
    throw new Error("no Durable Object namespace key for " + className);
  return new DurableObjectNamespace(className, namespaceKey);
}
// A named class: SDKs sniff bindings by constructor name (workers-rs
// EnvBinding requires `constructor.name === "DurableObjectNamespace"`).
class DurableObjectNamespace {
  constructor(className, namespaceKey) {
    Object.defineProperty(this, "_className", { value: className });
    Object.defineProperty(this, "_namespaceKey", { value: namespaceKey });
  }
  idFromName(name) {
    name = String(name);
    return new DurableObjectId(
      this._className, __do_id(this._namespaceKey, "name", name), name,
    );
  }
  idFromString(value) {
    return new DurableObjectId(
      this._className, __do_id(this._namespaceKey, "validate", String(value)),
    );
  }
  newUniqueId(options = {}) {
    const jurisdiction = options == null ? undefined : options.jurisdiction;
    if (jurisdiction != null)
      throw new Error("Jurisdiction restrictions are not implemented");
    return new DurableObjectId(
      this._className, __do_id(this._namespaceKey, "unique", ""),
    );
  }
  jurisdiction(value) {
    if (value == null) return this;
    throw new Error("Jurisdiction restrictions are not implemented");
  }
  getByName(name, options) {
    return this.get(this.idFromName(name), options);
  }
  get(id) {
    const className = this._className;
    if (!(id instanceof DurableObjectId) || id._className !== className)
      throw new TypeError("Durable Object ID is not valid for this namespace");
    const scope = id._scope();
    // Emulate production: the actor recovers its name only when it is
    // <= 1024 UTF-8 bytes; longer names are dropped so ctx.id.name is
    // undefined. The full name still seeds the routing hash, so
    // dispatch is unchanged. Short names skip the byte count (< 256
    // chars is always <= 1020 bytes) to keep the hot path alloc-free.
    const nm = id.name;
    const dispatchName = nm === undefined ? undefined
      : nm.length < 256 || new TextEncoder().encode(nm).length <= 1024
        ? nm : undefined;
    if (dispatchName !== undefined) __cell.idNames[scope] = dispatchName;
    // Fetch and native RPC use the same host routing/activation seam.
    // Never expose `.then`: a DO stub is not itself a promise.
    // `__celldDo` brands the stub so the RPC lift can send it as a
    // revivable marker rather than failing the clone; non-enumerable
    // so Object.keys(stub) stays Workerd's [id, name].
    const target = { id, name: dispatchName };
    Object.defineProperty(target, "__celldDo", { value: id });
    const abortMarker = "__CELLD_ACTOR_ABORT__:";
    const processExitMarker = "__CELLD_PROCESS_EXIT__:";
    let brokenReason = null;
    const invoke = async (operation) => {
      if (brokenReason !== null) throw new Error(brokenReason);
      try {
        return await operation();
      } catch (error) {
        const routingError = __durableObjectRoutingError(error);
        if (routingError !== null) throw routingError;
        const message = String(error && error.message || error);
        const marker = [abortMarker, processExitMarker]
          .find((candidate) => message.includes(candidate));
        if (!marker) throw error;
        brokenReason = message.slice(message.indexOf(marker) + marker.length);
        throw new Error(brokenReason);
      }
    };
    const doFetch = async (input, init) => {
        const req = new Request(input, init);
        const signal = req._signalForSubrequests;
        if (signal?.aborted) throw signal.reason;
        // The DO seam carries exact bytes; a stream body is drained
        // (and per spec disturbed) first.
        const body_ = req._bodyBytes === null
          ? await req._consume() : req._bodyBytes;
        // Forward the request's headers to the cell as JSON. When they were
        // never materialized -- a Worker that only routes to a cell reads
        // none -- pass the raw header string straight through, so the whole
        // dispatch never builds a Headers object on either side.
        const headersJson = req._headersJson !== undefined
          ? req._headersJson
          : JSON.stringify(Array.from(req.headers));
        // Fast path: this isolate owns the target cell — run the DO
        // in-isolate, avoiding the __do_call host round trip.
        if (__cell.owned[scope]) {
          return await invoke(() => __dispatchTo(
            scope, req.url, req.method, body_,
            headersJson,
            null,
            true,
            signal,
          ));
        }
        const r = JSON.parse(await invoke(() => {
          if (!signal) return __do_call(
            scope, dispatchName ?? null, req.url, req.method, body_,
            headersJson,
          );
          return __awaitCancellableDoCall(
            __do_call_cancellable(
              scope, dispatchName ?? null, req.url, req.method, body_,
              headersJson,
            ),
            signal,
          );
        }));
        const body = r.streamId !== undefined
          ? new CelldHttpBodyStream(r.streamId)
          : r.body !== undefined
            ? r.body
            : Uint8Array.from(r.bodyBytes || []);
        return new Response(body, {
          status: r.status, headers: r.headers, __wsTarget: r.wsTarget,
        });
    };
    const stub = new Proxy(target, { get: (_target, prop) => {
      if (prop === "then") return undefined;
      if (Reflect.has(_target, prop)) return Reflect.get(_target, prop);
      if (prop === "fetch") return doFetch;
      if (typeof prop !== "string") return undefined;
      if (__cell.compat.fetcherGetPutDelete &&
          (prop === "get" || prop === "put" || prop === "delete"))
        return __fetcherHelper(doFetch, prop);
      // Fast path: this isolate owns the target cell — run the DO RPC
      // in-isolate, avoiding the __rpc_call host round trip. Still a
      // structured clone each way: Workerd extracts a copy even for a
      // same-isolate call, and JSON round-tripped here before. The
      // reply decodes inside invoke() so abort/exit markers rethrown
      // from the envelope still trip the broken-stub sniffing.
      if (__cell.owned[scope])
        return async (...args) => invoke(
          async () => __rpcDes(await __dispatchRpc(
            scope, prop, __rpcOut(args, true))),
        );
      // The routed channel also lifts: same-process dispatch
      // re-enters this isolate, where the markers revive; bytes
      // that land elsewhere revive as loud foreign stubs.
      return async (...args) => invoke(
        async () => __rpcDes(await __rpc_call(
          scope, dispatchName ?? null, prop, __rpcOut(args, true),
        )),
      );
    }});
    return stub;
  }
}
const __attachResponseRequestCancellation = (
  response,
  requestController,
  wrapBody,
) => {
  if (!(response instanceof Response) ||
      response._bodyBytes !== null ||
      response.body === null) {
    return;
  }
  const requestControllers = Array.isArray(
    response.__celldRequestControllers,
  )
    ? response.__celldRequestControllers
    : [requestController];
  if (requestControllers !== response.__celldRequestControllers) {
    Object.defineProperty(response, "__celldRequestControllers", {
      value: requestControllers,
    });
  } else {
    requestControllers.push(requestController);
  }
  if (!wrapBody || response.__celldCancellationWrapped) return;
  const reader = response.body.getReader();
  response.body = new ReadableStream({
    async pull(controller) {
      const result = await reader.read();
      if (result.done) controller.close();
      else controller.enqueue(result.value);
    },
    async cancel() {
      const reason = new Error("The client has disconnected");
      for (const controller of requestControllers) {
        if (!controller.signal.aborted) controller.abort(reason);
      }
      try {
        await reader.cancel(reason);
      } catch {}
    },
  }, { highWaterMark: 0 });
  Object.defineProperty(response, "__celldCancellationWrapped", {
    value: true,
  });
};
// called on the owner node by the /__do/<scope> endpoint: run one object.
// Returns the Response; Rust's read_response unwraps it (don't double-wrap).
// A top-level (non-actor) request whose signal can be aborted by id, so a
// disconnected HTTP or service-binding caller reaches the target's
// `request.signal`.
globalThis.__makeIncomingRequest = (
  url, method, body, headersJson,
) => __makeRequest(
  url, method, body, headersJson, undefined, true);
globalThis.__dispatchTo = async (
  scope, url, method, body, headersJson, requestId = null, inline = false,
  callerSignal = null,
) => {
  // Request already allocates a default controller for an absent signal.
  // Retain that same allocation so a streamed response can report reader
  // cancellation after the handler has returned.
  const requestController = new AbortController();
  if (requestId !== null)
    __incomingRequestSignals.set(
      String(requestId), requestController.signal);
  __actorEventStack.push(scope);
  try {
    // Output gate for the co-hosted fast path: the routed path is gated in the
    // host, but an owned in-isolate dispatch bypasses it, so sample the cell's
    // committed-write position here and hold the response below until durable.
    const gateBefore = inline ? __writePosition(scope) : null;
    const dispatch = (async () => {
      const inst = await _readyInstance(scope);
      return await __ctxRun(undefined, () => inst.fetch(
        __makeRequest(
          url,
          method,
          body,
          headersJson,
          requestController.signal,
          true,
        )));
    })();
    // The owned in-isolate fast path carries the caller's signal here
    // directly (the routed channel reaches this signal through
    // __do_call_cancel/__abortIncomingRequest instead). Mirror those
    // semantics: abandonment aborts the target's fresh incoming signal
    // with the canonical disconnect reason and rejects the caller with
    // its own reason; the abandoned dispatch keeps its settle handlers
    // and drains its waitUntil work.
    const response = callerSignal === null
      ? await dispatch
      : await __raceCallerAbort(dispatch, callerSignal, () =>
          requestController.abort(
            new Error("The client has disconnected")));
    __attachResponseRequestCancellation(
      response,
      requestController,
      inline,
    );
    if (response instanceof Response && response.status === 101 &&
        response.webSocket && !response.webSocket._target) {
      const socket = response.webSocket._peer;
      const target = { id: response.webSocket._id, scope };
      response.webSocket._target = target;
      if (socket) {
        socket._target = target;
        if (!socket._hibernatable)
          __ws_accept_regular(socket._id, scope);
        __sockets.set(socket._id, socket);
        socket._flushPending();
      }
    }
    // Hold the inline response until the write is durable (rejects if it can
    // not be proved, breaking the call as a routed gate failure would). A null
    // `before` means this thread opened the cell's connection during the
    // handler; a fresh connection's change counter starts at zero, so treat it
    // as zero — a write advances past it, a read leaves it there.
    if (inline) {
      const after = __writePosition(scope);
      if (after !== null && after > (gateBefore ?? 0)) await __gateWrite(scope, after);
    }
    return response;
  } finally {
    if (requestId !== null)
      __incomingRequestSignals.delete(String(requestId));
    if (__actorEventStack[__actorEventStack.length - 1] === scope)
      __actorEventStack.pop();
  }
};
const __rpcTargetMethod = async (scope, method) => {
  const inst = await _readyInstance(scope);
  if (!inst.__celldState._rpcOk)
    throw new TypeError(
      "The receiving Durable Object does not support RPC, because " +
      "its class was not declared with `extends DurableObject`. In " +
      "order to enable RPC, make sure your class extends the " +
      "special class `DurableObject`, which can be imported from " +
      "the module \"cloudflare:workers\".");
  const fn = inst[method];
  if (typeof fn !== "function")
    throw new TypeError(method + " is not a function");
  return [inst, fn];
};
// The byte path always lifts stub-able values into the reply: the
// markers carry the isolate token, so they revive only back in this
// isolate (the owned fast path and same-process routed dispatch)
// and fail loudly on use anywhere else. Callee exceptions cross in
// the error envelope on every flavor.
globalThis.__dispatchRpc = async (scope, method, args) => {
  __actorEventStack.push(scope);
  try {
    // A string is the legacy JSON flavor (test harness, old cross-node
    // envelope); bytes are V8 structured clone. Answer in kind.
    if (typeof args === "string") {
      const [inst, fn] = await __rpcTargetMethod(scope, method);
      const result = await fn.apply(inst, JSON.parse(args));
      return JSON.stringify(result) ?? "null";
    }
    return await __ctxRun(undefined, () => (async () => {
      const decoded = __rpcDesArgs(args);
      try {
        return await __rpcRun(async () => {
          // A broken actor (JS-flavored ctx.abort) rejects every
          // later call with the reason. One gated field read.
          if (__brokenActors.size !== 0) {
            const broken = __brokenActors.get(scope);
            if (broken !== undefined) throw broken;
          }
          const [inst, fn] = await __rpcTargetMethod(scope, method);
          return fn.apply(inst, decoded.args);
        }, true);
      } finally {
        for (const handle of decoded.received) __disposeStub(handle);
      }
    })());
  } finally {
    if (__actorEventStack[__actorEventStack.length - 1] === scope)
      __actorEventStack.pop();
  }
};
// Invoke a method on a named WorkerEntrypoint. Instances are cached per
// entrypoint: the class is stateless across calls the way a Worker is,
// so re-constructing per call would only add allocation.
const __entrypointInstances = new Map();
const __entrypointInstance = (name) => {
  const cls = __cell.entrypoints[name];
  if (typeof cls !== "function") {
    // Workerd getExportedHandler(): distinguish a Durable Object class
    // misused as a stateless entrypoint from a name that resolves to
    // nothing. Error path only; both lookups are O(1).
    if (__cell.classes[name] !== undefined || __cell.doExports[name])
      throw new TypeError(
        `The entrypoint name ${name} refers to a Durable Object ` +
        "class, but the incoming request is trying to invoke it as " +
        "a stateless worker.");
    throw new TypeError(
      `The entrypoint name ${name} was not found in this worker. ` +
      "Ensure the worker exports an entrypoint with that name.");
  }
  let inst = __entrypointInstances.get(name);
  if (inst === undefined) {
    // End the construction event immediately: ctx.waitUntil registers
    // into whichever event is current at call time, so leaving this
    // event on the stack would swallow every later registration in the
    // isolate.
    const ctx = __beginEvent();
    try {
      inst = new cls(ctx, __cell.env);
    } finally {
      __endEvent();
    }
    __entrypointInstances.set(name, inst);
  }
  return inst;
};
// `env.NAME.fetch()` where NAME is bound with `entrypoint = "..."` goes
// to that class's fetch, not the module's default export. A plain
// object export (Workerd's non-class entrypoint) dispatches its
// handler functions as fn(arg, env, ctx).
const __dispatchEntrypointMethod = async (name, method, arg) => {
  const handler = __cell.objectEntrypoints[name];
  if (handler !== undefined) {
    if (typeof handler[method] !== "function")
      throw new TypeError(
        `Entrypoint ${JSON.stringify(name)} has no ${method} handler`);
    const ctx = __beginEvent();
    try {
      return await __ctxRun(undefined,
        () => handler[method](arg, __cell.env, ctx));
    } finally {
      __endEvent();
    }
  }
  // A class entrypoint's methods get env and ctx from its constructor,
  // not as arguments.
  const inst = __entrypointInstance(name);
  if (typeof inst[method] !== "function")
    throw new TypeError(
      `Entrypoint ${JSON.stringify(name)} has no ${method} handler`);
  return await __ctxRun(undefined, () => inst[method](arg));
};
globalThis.__dispatchEntrypointFetch = (name, request) =>
  __dispatchEntrypointMethod(name, "fetch", request);
globalThis.__dispatchEntrypointScheduled = (name, ctrl) =>
  __dispatchEntrypointMethod(name, "scheduled", ctrl);
// Workerd's simple-handler RPC rules (worker-rpc.c++): a non-class
// handler method is called as fn(arg, env, ctx), the client must send
// exactly one argument, and the handler must not declare more than
// (arg, env, ctx). The messages are Workerd's, verbatim.
const __callObjectEntrypoint = (handler, method, args) => {
  const fn = handler[method];
  if (typeof fn !== "function")
    throw new TypeError(
      'The RPC receiver does not implement the method "' + method +
      '".');
  if (fn.length > 3)
    throw new TypeError(
      'Cannot call handler function "' + method + '" over RPC ' +
      "because it has the wrong number of arguments. A simple " +
      "function handler can only be called over RPC if it has " +
      "exactly the arguments (arg, env, ctx), where only the first " +
      "argument comes from the client. To support multi-argument " +
      "RPC functions, use class-based syntax (extending " +
      "WorkerEntrypoint) instead.");
  if (args.length !== 1)
    throw new TypeError(
      'Attempted to call RPC function "' + method + '" with the ' +
      "wrong number of arguments. When calling a top-level handler " +
      "function that is not declared as part of a class, you must " +
      "always send exactly one argument. In order to support " +
      "variable numbers of arguments, the server must use " +
      "class-based syntax (extending WorkerEntrypoint) instead.");
  const ctx = {
    waitUntil: globalThis.__registerWaitUntil,
    passThroughOnException() {},
    abort: __ctxAbortCurrent,
    props: __defaultProps,
    get exports() { return __ctxExports(); },
  };
  return fn.call(handler, args[0], __cell.env, ctx);
};
// One entrypoint op (a call, or a property GET when argsSc is
// null), inside a fresh request context — the callee owns stubs
// revived from its params, and stubs it mints belong to it.
const __entrypointOp = (name, path, argsSc, local, makeInst) => {
  const id = __nextCtxId++;
  return __ctxRun(id, () => (async () => {
  const decoded = argsSc === null ? null : __rpcDesArgs(argsSc);
  let drain = null;
  try {
    const reply = await __rpcRun(async () => {
      // The handler's synchronous part runs inside its own event so
      // ctx.waitUntil and the imported waitUntil have a target. The
      // event pops before the first await — the event stack is
      // strictly LIFO and an event held across an await would be
      // popped by whichever event settles next — and its registered
      // work drains before a plain reply (below).
      __beginEvent();
      let result;
      try {
        const handler = __cell.objectEntrypoints[name];
        if (handler !== undefined && makeInst === undefined) {
          // Simple-handler entrypoints expose single-method calls
          // only; property GETs and deeper paths are refused.
          if (argsSc === null || path.length !== 1)
            throw __rpcNoSuchMethod(path[0]);
          result = __callObjectEntrypoint(
            handler, path[0], decoded.args);
        } else {
          const inst = makeInst === undefined
            ? __entrypointInstance(name) : makeInst();
          result = __rpcWalk(inst, path,
            decoded === null ? null : decoded.args, true);
        }
      } finally {
        drain = __endEvent();
      }
      return await result;
    }, local);
    // Registered work drains before a plain reply. A
    // capability-bearing reply (tag 1) must not wait: a returned
    // stream's chunks may be produced by that very work, which
    // cannot finish until the caller pulls (returnReadableStream's
    // waitUntil writer would deadlock behind its own reply).
    if (reply[0] !== 1 && drain !== null) await drain;
    // ctx.abort() during the call supersedes its result; the raw
    // reason rejects the caller (same isolate — identity holds).
    if (__abortedCtxs.size !== 0) {
      const reason = __abortedCtxs.get(id);
      if (reason !== undefined) throw reason;
    }
    // A reply that exports no stubs (tag 0xff plain, tag 0 error)
    // leaves nothing rooting this context: tear it down now, so a
    // dup() the callee leaked reaches its disposal callback
    // promptly (Workerd tears the IoContext down at call end
    // unless returned capabilities hold it open).
    if (reply[0] !== 1) __ctxEnd(id);
    return reply;
  } finally {
    if (decoded !== null)
      for (const handle of decoded.received) __disposeStub(handle);
  }
})());
};
// Cross-isolate and host callers still pass a single method name.
globalThis.__dispatchEntrypointRpc =
  (name, path, argsSc, local = false) => __entrypointOp(
    name, typeof path === "string" ? [path] : path, argsSc, local,
    undefined);
// WebSocket: the host holds the socket; these deliver events into the DO.
// `ws` is a lightweight stub whose send/close route back to the host task
// by wsId — so the isolate can be hibernated between messages.
globalThis.__wsStub = (wsId) => ({
  _hibernatable: true,
  send: (data) => {
    if (data instanceof ArrayBuffer)
      __ws_send_binary(wsId, new Uint8Array(data));
    else if (ArrayBuffer.isView(data))
      __ws_send_binary(wsId,
        new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
    else
      __ws_send(wsId, String(data));
  },
  close: (code = 1000, reason = "") => __ws_close(wsId, code, reason),
});
globalThis.__wsOpen = async (scope, wsId, protocol) => {
  __actorEventStack.push(scope);
  try {
    const inst = await _readyInstance(scope);
    const socket = inst.__celldState._socket(wsId);
    if (socket.readyState !== WebSocket.READY_STATE_CONNECTING) return;
    socket.protocol = protocol;
    socket.readyState = WebSocket.READY_STATE_OPEN;
    socket.dispatchEvent(new Event("open"));
  } finally {
    if (__actorEventStack[__actorEventStack.length - 1] === scope)
      __actorEventStack.pop();
  }
};
globalThis.__wsMessage = async (scope, wsId, msg) => {
  __actorEventStack.push(scope);
  try {
    const inst = await _readyInstance(scope);
    const socket = inst.__celldState._socket(wsId);
    if (!socket._hibernatable && typeof socket._dispatchMessage === "function")
      socket._dispatchMessage(msg);
    else if (typeof inst.webSocketMessage === "function")
      await inst.webSocketMessage(socket, msg);
  } finally {
    if (__actorEventStack[__actorEventStack.length - 1] === scope)
      __actorEventStack.pop();
  }
};
globalThis.__wsBinary = async (scope, wsId, data) => {
  __actorEventStack.push(scope);
  try {
    const inst = await _readyInstance(scope);
    const socket = inst.__celldState._socket(wsId);
    const bytes = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
    if (!socket._hibernatable && typeof socket._dispatchMessage === "function")
      socket._dispatchMessage(bytes);
    else if (typeof inst.webSocketMessage === "function")
      await inst.webSocketMessage(socket, bytes);
  } finally {
    if (__actorEventStack[__actorEventStack.length - 1] === scope)
      __actorEventStack.pop();
  }
};
globalThis.__wsClosed = async (scope, wsId, code, reason, wasClean) => {
  __actorEventStack.push(scope);
  try {
    const inst = await _readyInstance(scope);
    const socket = inst.__celldState._socket(wsId);
    if (!socket._hibernatable && typeof socket._dispatchClose === "function")
      socket._dispatchClose(code, reason, wasClean);
    else if (!wasClean && typeof inst.webSocketError === "function")
      // A hibernatable socket reports an abnormal closure through
      // webSocketError, which celld listed as a handler name and never
      // called.
      await inst.webSocketError(
        socket,
        new Error(reason ? `WebSocket closed abnormally: ${reason}` : "WebSocket closed abnormally"),
      );
    else if (typeof inst.webSocketClose === "function")
      await inst.webSocketClose(socket, code, reason, wasClean);
  } finally {
    if (__actorEventStack[__actorEventStack.length - 1] === scope)
      __actorEventStack.pop();
  }
};
// called by celld's scheduler when an alarm is due. Returns a promise.
globalThis.__fireAlarm = async (scope, scheduledTime, retryCount) => {
  __actorEventStack.push(scope);
  try {
    const inst = await _readyInstance(scope);
    if (typeof inst.alarm !== "function") return;
    await inst.alarm({ scheduledTime, retryCount, isRetry: retryCount > 0 });
  } finally {
    if (__actorEventStack[__actorEventStack.length - 1] === scope)
      __actorEventStack.pop();
  }
};
globalThis.__cell = {
  entrypoints: {},
  objectEntrypoints: {},
  doExports: {},
  classes: {},
  instances: {},
  env: {},
  owned: {},
  idNames: {},
  namespaceKeys: {},
  node: "",
  deleteAllDeletesAlarm: false,
  compat: { jsRpc: false, fetcherGetPutDelete: false },
  makeNamespace,
};
// `cloudflare:workers` module surface. The DO base class sets ctx/env the
// way `class X extends DurableObject` expects; env aliases the cell env.
globalThis.__cf = {
  DurableObject: class DurableObject {
    constructor(ctx, env) { this.ctx = ctx; this.env = env; }
  },
  // The enumerable function-valued brand makes V8's structured clone
  // fail on every RpcTarget instance, forcing it off the silent
  // plain-clone path and into the stub lift (Workerd rejects these
  // from plain serialization outright).
  RpcTarget: class RpcTarget {
    constructor() {
      Object.defineProperty(this, "__celldRpcTarget", {
        value: __rpcNoClone, enumerable: true,
      });
    }
  },
  // Named entrypoint for `[[services]]` with `entrypoint = "Name"`.
  WorkerEntrypoint: class WorkerEntrypoint {
    constructor(ctx, env) { this.ctx = ctx; this.env = env; }
  },
  // `new RpcStub(target)` wraps any local object or function in a
  // loopback stub; the constructor returns the proxy, so instanceof
  // works through __makeStub's getPrototypeOf trap.
  RpcStub: class RpcStub {
    constructor(target) {
      if (target === null ||
          (typeof target !== "object" && typeof target !== "function"))
        throw new TypeError(
          "RpcStub requires an object or function.");
      return __makeStub(
        __newEntry(target), typeof target === "function");
    }
  },
  RpcPromise: class RpcPromise extends Promise {},
  RpcProperty: class RpcProperty {},
  ServiceStub: class ServiceStub {},
  // `import { waitUntil } from "cloudflare:workers"`: register into
  // the current event; outside any event this is Workerd's
  // global-scope error.
  waitUntil(promise) {
    if (__event_depth() === 0)
      throw new Error(
        "Disallowed operation called within global scope.");
    globalThis.__registerWaitUntil(promise);
  },
  exports: {},
  get env() { return globalThis.__cell.env; },
};
// Pass-through proxy standing in for unsupported node:* builtins: callable,
// constructable, and every property returns itself — so bundle evaluation
// never crashes on a builtin the fetch path doesn't actually exercise.
globalThis.__nodeStub = new Proxy(function () {}, {
  get: (_t, p) => {
    // Never masquerade as a thenable. `await` probes `.then`; returning
    // the callable proxy here creates a promise that can never settle.
    if (p === "then") return undefined;
    // coerce cleanly in string/number contexts so evaluation never throws
    if (p === Symbol.toPrimitive || p === "toString" || p === "valueOf") return () => "";
    if (p === Symbol.toStringTag) return "NodeStub";
    if (p === Symbol.iterator) return function* () {};
    return globalThis.__nodeStub;
  },
  apply: () => globalThis.__nodeStub,
  construct: () => globalThis.__nodeStub,
});
if (!globalThis.Event) {
  globalThis.Event = class Event {
    // Private fields: stored in the object itself, so an Event costs no
    // side allocation and no defineProperty call at construction. The
    // public members are accessors because the DOM standard makes them
    // read-only; EventTarget drives them through _begin/_end.
    #type; #bubbles; #cancelable; #composed;
    #defaultPrevented = false;
    #eventPhase = 0;
    #target; #currentTarget; #path;
    #stop = false; #stopImmediate = false; #dispatching = false;
    // Workerd: events the runtime delivers are trusted; events a
    // script constructs and dispatches itself are not.
    #trusted = false;
    constructor(type, init = {}) {
      if (arguments.length === 0)
        throw new TypeError(
          "Failed to construct 'Event': 1 argument required, but only " +
          "0 present.");
      if (init !== undefined && init !== null &&
          typeof init !== "object")
        throw new TypeError(
          "Failed to construct 'Event': The provided value is not of " +
          "type 'EventInit'.");
      const options = init || {};
      // Template interpolation, not String(): it must throw for a
      // Symbol, which String() would happily format.
      this.#type = `${type}`;
      this.#bubbles = !!options.bubbles;
      this.#cancelable = !!options.cancelable;
      this.#composed = !!options.composed;
    }
    get type() { return this.#type; }
    get bubbles() { return this.#bubbles; }
    get cancelable() { return this.#cancelable; }
    get composed() { return this.#composed; }
    get defaultPrevented() { return this.#defaultPrevented; }
    get eventPhase() { return this.#eventPhase; }
    get target() { return this.#target; }
    get currentTarget() { return this.#currentTarget; }
    get isTrusted() { return this.#trusted; }
    get timeStamp() { return 0; }
    get returnValue() { return !this.#defaultPrevented; }
    // The one writable member, per the DOM standard.
    get cancelBubble() { return this.#stop; }
    set cancelBubble(value) { if (value) this.#stop = true; }
    composedPath() { return this.#path ? this.#path.slice() : []; }
    preventDefault() {
      if (this.#cancelable) this.#defaultPrevented = true;
    }
    stopPropagation() { this.#stop = true; }
    stopImmediatePropagation() {
      this.#stop = true;
      this.#stopImmediate = true;
    }
    // Internal dispatch hooks used by EventTarget.
    get _dispatching() { return this.#dispatching; }
    get _stopImmediate() { return this.#stopImmediate; }
    _trust() { this.#trusted = true; }
    _begin(target) {
      this.#dispatching = true;
      this.#target = target;
      this.#currentTarget = target;
      this.#eventPhase = 2; // AT_TARGET
      this.#path = [target];
      this.#stop = false;
      this.#stopImmediate = false;
    }
    // Workerd leaves currentTarget set after dispatch (the DOM standard
    // nulls it); only the phase is reset.
    _end() {
      this.#eventPhase = 0;
      this.#dispatching = false;
    }
  };
  {
    const phases = {
      NONE: 0, CAPTURING_PHASE: 1, AT_TARGET: 2, BUBBLING_PHASE: 3,
    };
    for (const [name, value] of Object.entries(phases)) {
      const d = { value, writable: false, enumerable: true,
        configurable: false };
      Object.defineProperty(globalThis.Event, name, d);
      Object.defineProperty(globalThis.Event.prototype, name, d);
    }
  }
}
if (!globalThis.CustomEvent) {
  globalThis.CustomEvent = class CustomEvent extends Event {
    constructor(type, init = {}) {
      super(type, init);
      this.detail = init.detail === undefined ? null : init.detail;
    }
  };
}
if (!globalThis.ExtendableEvent) {
  // Exists as a type but is not constructable from user code.
  globalThis.ExtendableEvent = class ExtendableEvent extends Event {
    constructor() {
      throw new TypeError("Illegal constructor");
    }
  };
}
if (!globalThis.EventTarget) {
  globalThis.EventTarget = class EventTarget {
    constructor() { this._listeners = new Map(); }
    addEventListener(type, callback, options = {}) {
      if (callback === null || callback === undefined) return;
      if (typeof callback !== "function" &&
          typeof callback.handleEvent !== "function")
        throw new TypeError(
          "Failed to execute 'addEventListener' on 'EventTarget': " +
          "parameter 2 is not of type 'EventListener'.");
      const object = typeof options === "object" && options !== null;
      // Capture and passive are accepted for portability but must be
      // false: Cells dispatches only at the target, so honouring them
      // would silently change ordering.
      if (object ? options.capture || options.passive : !!options)
        throw new TypeError(
          "Cells does not support the 'capture' or 'passive' options " +
          "on addEventListener().");
      const signal = object ? options.signal : undefined;
      if (signal && signal.aborted) return;
      const key = String(type);
      const list = this._listeners.get(key) || [];
      if (list.some((e) => e.callback === callback)) return;
      list.push({ callback, once: object && !!options.once });
      this._listeners.set(key, list);
      if (signal)
        signal.addEventListener("abort", () =>
          this.removeEventListener(key, callback), { once: true });
    }
    removeEventListener(type, callback, options = {}) {
      const object = typeof options === "object" && options !== null;
      if (object ? options.capture : !!options)
        throw new TypeError(
          "Cells does not support the 'capture' option on " +
          "removeEventListener().");
      const list = this._listeners.get(String(type));
      if (!list) return;
      const index = list.findIndex((e) => e.callback === callback);
      if (index >= 0) list.splice(index, 1);
    }
    dispatchEvent(event) {
      if (!(event instanceof Event))
        throw new TypeError("argument is not an Event");
      if (event._dispatching)
        throw new DOMException(
          "The event is already being dispatched.",
          "InvalidStateError");
      event._begin(this);
      // Copy: a listener may add or remove listeners mid-dispatch.
      for (const item of (this._listeners.get(event.type) || []).slice()) {
        if (event._stopImmediate) break;
        if (item.once)
          this.removeEventListener(event.type, item.callback);
        if (typeof item.callback === "function")
          item.callback.call(this, event);
        else item.callback.handleEvent(event);
      }
      const handler = this["on" + event.type];
      if (!event._stopImmediate && typeof handler === "function")
        handler.call(this, event);
      event._end();
      return !event.defaultPrevented;
    }
  };
}
// The global scope is itself an EventTarget.
if (typeof globalThis.addEventListener !== "function") {
  const globalTarget = new EventTarget();
  for (const name of
    ["addEventListener", "removeEventListener", "dispatchEvent"])
    globalThis[name] = globalTarget[name].bind(globalTarget);
}
if (!globalThis.MessageEvent) {
  globalThis.MessageEvent = class MessageEvent extends Event {
    constructor(type, init = {}) {
      super(type, init);
      this.data = init.data;
      this.origin = String(init.origin || "");
      this.lastEventId = String(init.lastEventId || "");
    }
  };
}
if (!globalThis.CloseEvent) {
  globalThis.CloseEvent = class CloseEvent extends Event {
    constructor(type, init = {}) {
      super(type, init);
      this.code = Number(init.code || 0);
      // `reason` is a USVString: a lone surrogate becomes U+FFFD rather than
      // travelling as an unpaired code unit.
      this.reason = String(init.reason || "").toWellFormed
        ? String(init.reason || "").toWellFormed()
        : String(init.reason || "").replace(
          /[\uD800-\uDFFF]/g,
          (unit, index, whole) => {
            const code = unit.charCodeAt(0);
            const next = whole.charCodeAt(index + 1);
            const prev = whole.charCodeAt(index - 1);
            const paired = code <= 0xDBFF
              ? next >= 0xDC00 && next <= 0xDFFF
              : prev >= 0xD800 && prev <= 0xDBFF;
            return paired ? unit : "\uFFFD";
          },
        );
      this.wasClean = !!init.wasClean;
    }
  };
}
if (!globalThis.DOMException) {
  globalThis.DOMException = class DOMException extends Error {
    constructor(message = "", name = "Error") { super(message); this.name = name; }
  };
  // WebIDL legacy code constants, enumerable on both the interface
  // object and its prototype, in spec order.
  {
    const codes = [
      ["INDEX_SIZE_ERR", 1], ["DOMSTRING_SIZE_ERR", 2],
      ["HIERARCHY_REQUEST_ERR", 3], ["WRONG_DOCUMENT_ERR", 4],
      ["INVALID_CHARACTER_ERR", 5], ["NO_DATA_ALLOWED_ERR", 6],
      ["NO_MODIFICATION_ALLOWED_ERR", 7], ["NOT_FOUND_ERR", 8],
      ["NOT_SUPPORTED_ERR", 9], ["INUSE_ATTRIBUTE_ERR", 10],
      ["INVALID_STATE_ERR", 11], ["SYNTAX_ERR", 12],
      ["INVALID_MODIFICATION_ERR", 13], ["NAMESPACE_ERR", 14],
      ["INVALID_ACCESS_ERR", 15], ["VALIDATION_ERR", 16],
      ["TYPE_MISMATCH_ERR", 17], ["SECURITY_ERR", 18],
      ["NETWORK_ERR", 19], ["ABORT_ERR", 20],
      ["URL_MISMATCH_ERR", 21], ["QUOTA_EXCEEDED_ERR", 22],
      ["TIMEOUT_ERR", 23], ["INVALID_NODE_TYPE_ERR", 24],
      ["DATA_CLONE_ERR", 25],
    ];
    for (const [name, value] of codes) {
      const d = { value, writable: false, enumerable: true,
        configurable: false };
      Object.defineProperty(globalThis.DOMException, name, d);
      Object.defineProperty(globalThis.DOMException.prototype, name, d);
    }
  }
}
if (!globalThis.AbortSignal) {
  const __abortBrand = Symbol("abortSignal");
  globalThis.AbortSignal = class AbortSignal extends EventTarget {
    // The Headers/Blob clone-poisoning brand: V8's structured
    // clone throws on functions, so a live signal reaches the
    // RPC lift instead of silently flattening to plain data.
    constructor() {
      // Per spec AbortSignal has no constructor: it is only reachable
      // through AbortController, AbortSignal.abort/timeout/any. The internal
      // paths pass the brand to get past this.
      if (arguments[0] !== __abortBrand) {
        throw new TypeError("Illegal constructor");
      }
      super();
      this.aborted = false;
      this.reason = undefined;
      this.__celldHost = __rpcNoClone;
    }
    throwIfAborted() { if (this.aborted) throw this.reason; }
    static abort(reason = new DOMException("This operation was aborted", "AbortError")) {
      const c = new AbortController(); c.abort(reason); return c.signal;
    }
    static timeout(ms) {
      const c = new AbortController();
      setTimeout(() => c.abort(new DOMException(
        "The operation was aborted due to timeout", "TimeoutError",
      )), ms);
      return c.signal;
    }
    static any(signals) {
      const c = new AbortController();
      for (const signal of signals) {
        if (signal.aborted) { c.abort(signal.reason); break; }
        signal.addEventListener("abort", () => c.abort(signal.reason), { once: true });
      }
      return c.signal;
    }
  };
  globalThis.AbortController = class AbortController {
    constructor() { this.signal = new AbortSignal(__abortBrand); }
    abort(reason = new DOMException("This operation was aborted", "AbortError")) {
      __abortSignal(this.signal, reason);
    }
  };
}
if (!globalThis.Blob) {
  // Same clone-poisoning brand as Headers: an enumerable
  // function-valued property makes V8's structured clone throw, so
  // a Blob reaches the RPC lift instead of silently flattening.
  const __blobNoClone = () => {};
  globalThis.Blob = class Blob {
    constructor(parts = [], options = {}) {
      // size is assigned before type so both land in the order the
      // inspect output below reports them.
      if (options && options.endings !== undefined)
        throw new Error(
          "The 'endings' field on 'Options' is not implemented.");
      this.size = 0;
      // A type outside U+0020..U+007E is not a valid MIME type and is
      // dropped rather than stored.
      const rawType = String(options.type || "");
      this.type = /^[\u0020-\u007e]*$/.test(rawType)
        ? rawType.toLowerCase() : "";
      this.__celldHost = __blobNoClone;
      // Two passes. First convert every part in order — string
      // conversion runs user code (Symbol.toPrimitive), which may
      // resize a backing buffer. Buffers and views are carried through
      // as-is so a length-tracking view is measured *after* those side
      // effects, matching Workerd. Then allocate once and copy.
      //
      // The previous shape pushed every byte into a plain array with a
      // spread, costing roughly 8x the memory and exceeding the
      // argument limit on multi-megabyte parts.
      const sources = [];
      for (const part of parts) {
        if (part instanceof Blob) sources.push(part._bytes);
        else if (part instanceof ArrayBuffer ||
                 ArrayBuffer.isView(part)) sources.push(part);
        else sources.push(new TextEncoder().encode(String(part)));
      }
      const chunks = sources.map((source) =>
        source instanceof Uint8Array ? source
          : source instanceof ArrayBuffer ? new Uint8Array(source)
            : new Uint8Array(
              source.buffer, source.byteOffset, source.byteLength));
      let total = 0;
      for (const chunk of chunks) total += chunk.byteLength;
      const LIMIT = 134217728;
      if (total > LIMIT)
        throw new RangeError(
          `Blob size ${total} exceeds limit ${LIMIT}`);
      const bytes = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      // Non-enumerable: the backing store is not part of the object's
      // observable shape (inspect, JSON, spread).
      Object.defineProperty(this, "_bytes", {
        value: bytes, writable: true, configurable: true,
      });
      this.size = total;
    }
    async arrayBuffer() { return this._bytes.slice().buffer; }
    async bytes() { return this._bytes.slice(); }
    async text() { return new TextDecoder().decode(this._bytes); }
    stream() {
      const bytes = this._bytes;
      return new ReadableStream({
        start(controller) {
          if (bytes.byteLength) controller.enqueue(bytes.slice());
          controller.close();
        },
      });
    }
    [Symbol.for("nodejs.util.inspect.custom")]() {
      return `Blob { size: ${this.size}, type: '${this.type}' }`;
    }
    slice(start = 0, end = this.size, type = "") {
      const norm = (n, fallback) => {
        n = n === undefined ? fallback : Number(n);
        return n < 0 ? Math.max(this.size + n, 0) : Math.min(n, this.size);
      };
      const a = norm(start, 0), b = norm(end, this.size);
      return new Blob(
        [this._bytes.subarray(a, Math.max(a, b))], { type });
    }
    get [Symbol.toStringTag]() { return "Blob"; }
  };
}
if (!globalThis.File) {
  // WebIDL-ish per Workerd: name is required, name/lastModified are
  // brand-checked prototype getters (enumerable, matching the
  // workers_api_getters_setters_on_prototype flag), and lastModified
  // coerces via ToNumber (throws for BigInt), NaN becoming 0.
  globalThis.File = class File extends Blob {
    #name;
    #lastModified;
    constructor(parts, name, options = {}) {
      if (arguments.length < 2)
        throw new TypeError(
          "Failed to construct 'File': 2 arguments required, but " +
          `only ${arguments.length} present.`);
      super(parts, options);
      this.#name = String(name);
      const lm = options == null ? undefined : options.lastModified;
      if (lm === undefined) this.#lastModified = Date.now();
      else {
        const n = +lm; // ToNumber: throws for BigInt, like WebIDL
        this.#lastModified = Number.isFinite(n) ? Math.trunc(n) : 0;
      }
    }
    get name() { return this.#name; }
    get lastModified() { return this.#lastModified; }
    get [Symbol.toStringTag]() { return "File"; }
    [Symbol.for("nodejs.util.inspect.custom")]() {
      return `File { name: '${this.name}', ` +
        `lastModified: ${this.lastModified}, size: ${this.size}, ` +
        `type: '${this.type}' }`;
    }
  };
  for (const key of ["name", "lastModified"]) {
    const desc =
      Object.getOwnPropertyDescriptor(File.prototype, key);
    desc.enumerable = true;
    Object.defineProperty(File.prototype, key, desc);
  }
}
if (!globalThis.FormData) {
// Body -> FormData for multipart/form-data and
// application/x-www-form-urlencoded. Kept out of the class so V8 only
// pre-parses it; the body is compiled on first actual formData() call.
globalThis.__parseFormData = (text, contentType) => {
  const ct = String(contentType || "");
  if (/^\s*application\/x-www-form-urlencoded/i.test(ct)) {
    // The body has already been decoded as UTF-8, so a declared charset that
    // is not UTF-8 cannot be honoured. Refuse rather than mis-decode.
    const charset = /;\s*charset\s*=\s*"?([^";]+)/i.exec(ct);
    if (charset && !/^utf-?8$/i.test(charset[1].trim()))
      throw new TypeError(
        `Unsupported charset "${charset[1].trim()}". FormData can only ` +
        "parse UTF-8 encoded bodies.");
    const form = new FormData();
    for (const [key, value] of new URLSearchParams(text))
      form.append(key, value);
    return form;
  }
  if (!/^\s*multipart\/form-data/i.test(ct))
    throw new TypeError(
      "Unrecognized Content-Type header value. FormData can only " +
      "parse the following MIME types: multipart/form-data, " +
      "application/x-www-form-urlencoded");
  const found = /boundary\s*=\s*(?:"([^"]*)"|([^;]+))/i.exec(ct);
  if (!found)
    throw new TypeError(
      "No boundary was found in the multipart/form-data " +
      "Content-Type header.");
  const boundary = (found[1] !== undefined ? found[1] : found[2]).trim();
  const form = new FormData();
  const delimiter = "--" + boundary;
  // Walk the delimiters rather than splitting on them, so that a truncated or
  // corrupt body is refused instead of silently yielding the parts that
  // happened to parse. The preamble before the first delimiter and the
  // epilogue after the closing one are ignored.
  let cursor = text.indexOf(delimiter);
  if (cursor < 0)
    throw new TypeError(
      "No boundary delimiter was found in the multipart/form-data body.");
  cursor += delimiter.length;
  for (;;) {
    // What follows a delimiter decides everything: "--" closes the body, a
    // line break opens another part, and anything else is malformed.
    if (text.startsWith("--", cursor)) break;
    const lineBreak = text.startsWith("\r\n", cursor)
      ? 2
      : text.startsWith("\n", cursor) ? 1 : 0;
    if (lineBreak === 0)
      throw new TypeError(
        "A boundary delimiter must be followed by CRLF, LF, or \"--\".");
    cursor += lineBreak;
    const next = text.indexOf(delimiter, cursor);
    if (next < 0)
      throw new TypeError(
        "The multipart/form-data body ended without a closing boundary " +
        "delimiter.");
    // The line break before a delimiter belongs to the delimiter, not to the
    // part it terminates.
    const chunk = text.slice(cursor, next).replace(/\r?\n$/, "");
    cursor = next + delimiter.length;
    const split = /\r?\n\r?\n/.exec(chunk);
    if (!split)
      throw new TypeError(
        "A FormData part's headers must be terminated by a blank line.");
    const rawHeaders = chunk.slice(0, split.index);
    const body = chunk.slice(split.index + split[0].length);
    let disposition = null;
    let type = "";
    for (const line of rawHeaders.split(/\r?\n/)) {
      const colon = line.indexOf(":");
      if (colon < 0) continue;
      const name = line.slice(0, colon).trim().toLowerCase();
      const value = line.slice(colon + 1).trim();
      if (name === "content-disposition") disposition = value;
      else if (name === "content-type") type = value;
    }
    if (disposition === null)
      throw new TypeError(
        "Content-Disposition header is required for each FormData " +
        "part.");
    const kind = disposition.split(";")[0].trim();
    if (kind.toLowerCase() !== "form-data")
      throw new TypeError(
        "Content-Disposition header for FormData part must have the " +
        "value \"form-data\", possibly followed by parameters. Got: " +
        "\"" + kind + "\"");
    const param = (key) => {
      const m = new RegExp(
        key + "\\s*=\\s*(?:\"((?:[^\"\\\\]|\\\\.)*)\"|([^;]*))", "i",
      ).exec(disposition);
      if (!m) return undefined;
      const raw = m[1] !== undefined ? m[1] : (m[2] || "").trim();
      if (/\\$/.test(raw.replace(/\\\\/g, "")))
        throw new TypeError(
          "Name or filename can't end with backslash");
      return raw.replace(/\\(.)/g, "$1");
    };
    const name = param("name");
    if (name === undefined)
      throw new TypeError(
        "Content-Disposition header for FormData part must have a " +
        "name parameter.");
    const filename = param("filename");
    if (filename === undefined) form.append(name, body);
    else
      form.append(
        name, new File([body], filename, { type: type || "" }));
  }
  return form;
};
  // Spec entry conversion: strings stay strings; a Blob becomes a
  // File named "blob" (or `filename`); a File is renamed only when a
  // filename is supplied.
  const __formDataValue = (value, filename) => {
    if (!(value instanceof Blob)) return String(value);
    if (value instanceof File && filename === undefined) return value;
    return new File(
      [value],
      filename === undefined ? "blob" : String(filename),
      {
        type: value.type,
        lastModified:
          value instanceof File ? value.lastModified : undefined,
      },
    );
  };
  globalThis.FormData = class FormData {
    constructor() { this._entries = []; }
    append(name, value, filename) {
      this._entries.push(
        [String(name), __formDataValue(value, filename)]);
    }
    set(name, value, filename) {
      value = __formDataValue(value, filename);
      const key = String(name);
      // Spec: replace the first match in place, keeping its position,
      // and drop any later matches.
      const first = this._entries.findIndex(([k]) => k === key);
      if (first < 0) { this._entries.push([key, value]); return; }
      this._entries[first] = [key, value];
      for (let i = this._entries.length - 1; i > first; i--)
        if (this._entries[i][0] === key) this._entries.splice(i, 1);
    }
    get(name) {
      const row = this._entries.find(([key]) => key === String(name));
      return row ? row[1] : null;
    }
    getAll(name) {
      return this._entries.filter(([key]) => key === String(name)).map(([, value]) => value);
    }
    has(name) { return this._entries.some(([key]) => key === String(name)); }
    delete(name) {
      const key = String(name);
      // Splice in place: the iterators below read the live list, so the
      // array identity must survive a delete during iteration.
      for (let i = this._entries.length - 1; i >= 0; i--)
        if (this._entries[i][0] === key) this._entries.splice(i, 1);
    }
    // Iteration is live: the iterator holds an index and re-reads the
    // entry list, so entries appended during iteration are visited and
    // an exhausted iterator resumes if entries are added later. A
    // generator cannot do this — once it returns it is done forever.
    _iterate(pick) {
      const self = this;
      let index = 0;
      const iterator = {
        next() {
          if (index >= self._entries.length)
            return { value: undefined, done: true };
          return { value: pick(self._entries[index++]), done: false };
        },
        [Symbol.iterator]() { return iterator; },
      };
      return iterator;
    }
    entries() { return this._iterate((e) => [e[0], e[1]]); }
    keys() { return this._iterate((e) => e[0]); }
    values() { return this._iterate((e) => e[1]); }
    [Symbol.iterator]() { return this.entries(); }
    forEach(callback, thisArg) {
      if (typeof callback !== "function")
        throw new TypeError(
          "Failed to execute 'forEach' on 'FormData': parameter 1 is " +
          "not of type 'Function'.");
      for (const [key, value] of this.entries())
        callback.call(thisArg, value, key, this);
    }
  };
}
// Node's Buffer lives in src/js/node_buffer.js, compiled
// lazily (LAZY_GLOBALS / LAZY_MODULES) on first use.
if (!globalThis.ErrorEvent) {
  globalThis.ErrorEvent = class ErrorEvent extends Event {
    constructor(type, init = {}) {
      super(type, init);
      this.message = String(init.message ?? "");
      this.filename = String(init.filename ?? "");
      this.lineno = Number(init.lineno ?? 0);
      this.colno = Number(init.colno ?? 0);
      this.error = init.error;
    }
  };
}
// Web API globals a real bundle references at module scope but that the
// prelude doesn't provide. Stub as empty classes (guarded so we never
// clobber a real prelude implementation). Enough to LOAD; runtime use of
// an unimplemented one is a separate gap.
for (const n of ["WebSocket", "EventTarget", "Event", "MessageEvent",
  "CloseEvent", "ErrorEvent", "EventSource", "MessageChannel",
  "MessagePort", "BroadcastChannel", "SubtleCrypto", "TransformStream",
  "ReadableStream", "WritableStream", "ReadableStreamDefaultReader",
  "WritableStreamDefaultWriter",
  "ByteLengthQueuingStrategy", "CountQueuingStrategy",
  "TextEncoderStream", "TextDecoderStream", "FileReader"]) {
  if (!globalThis[n]) globalThis[n] = class {};
}
globalThis.__sockets = new Map();
globalThis.WebSocket = class WebSocket extends EventTarget {
  // The spec names, which workerd exposes and real bundles use. celld only
  // had the READY_STATE_* aliases, which no other runtime defines.
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static READY_STATE_CONNECTING = 0;
  static READY_STATE_OPEN = 1;
  static READY_STATE_CLOSING = 2;
  static READY_STATE_CLOSED = 3;
  constructor(id, protocols = []) {
    super();
    const outbound = typeof id === "string";
    this._outbound = outbound;
    this._pendingClose = null;
    this._id = outbound ? __ws_alloc() : Number(id);
    this._attachment = undefined;
    this._target = null;
    this._accepted = false;
    this._hibernatable = false;
    this._pending = [];
    // The other end of an in-isolate upgrade (a WebSocketPair served
    // over a same-script service binding); frames route to it
    // directly instead of through the host connection.
    this._loopback = null;
    this.readyState = outbound ? 0 : 1;
    this._binaryType = __cell.compat?.websocketStandardBinaryType
      ? "blob"
      : "arraybuffer";
    // A server-side socket has no URL, and reports null rather than an empty
    // string -- workerd's inspect output pins this.
    this.url = outbound ? id : null;
    this.protocol = "";
    this.extensions = "";
    if (outbound) {
      // workerd validates the URL in the constructor and throws
      // synchronously; letting the connector reject later would surface a
      // scheme mistake as a network failure instead of a TypeError.
      let parsed;
      try {
        parsed = new URL(id);
      } catch {
        throw new DOMException(
          "WebSocket Constructor: The url is invalid.",
          "SyntaxError");
      }
      if (parsed.protocol !== "ws:" && parsed.protocol !== "wss:") {
        throw new DOMException(
          "WebSocket Constructor: The url scheme must be ws or wss.",
          "SyntaxError");
      }
      // A Durable Object socket is pushed events by the host so it can revive
      // a hibernated cell. A Worker socket has no cell: the isolate polls it,
      // and it lives and dies with the request, which is the lifetime
      // Cloudflare gives one too.
      const scope = __actorEventStack[__actorEventStack.length - 1] || "";
      this._polled = !scope;
      const requested = typeof protocols === "string"
        ? [protocols]
        : Array.from(protocols, String);
      for (const value of requested) {
        // RFC 6455 subprotocols are HTTP tokens; a space or separator would
        // otherwise be smuggled into the Sec-WebSocket-Protocol header.
        if (!/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(value)) {
          throw new DOMException(
            `WebSocket Constructor: The subprotocol '${value}' is invalid.`,
            "SyntaxError");
        }
      }
      if (new Set(requested).size !== requested.length) {
        throw new DOMException(
          "WebSocket Constructor: The subprotocols must be unique.",
          "SyntaxError");
      }
      __sockets.set(this._id, this);
      const connection = __ws_connect(this._id, scope, id, JSON.stringify(requested));
      __registerWaitUntil(connection);
      if (this._polled) this._startPump();
      connection.then((protocol) => {
        if (this.readyState === WebSocket.READY_STATE_CLOSING) {
          if (this._pendingClose) {
            const [code, reason] = this._pendingClose;
            __ws_close(this._id, code, reason);
          }
          return;
        }
        if (this.readyState !== WebSocket.READY_STATE_CONNECTING) return;
        this.protocol = protocol;
        this.readyState = WebSocket.READY_STATE_OPEN;
        this.dispatchEvent(new Event("open"));
      }, (error) => {
        this.readyState = WebSocket.READY_STATE_CLOSED;
        __sockets.delete(this._id);
        this.dispatchEvent(new ErrorEvent("error", { message: String(error?.message || error), error }));
        this._dispatchClose(1006, "", false);
      });
    }
  }
  // The pump ends only when the socket closes, so it must NOT be registered
  // as waitUntil work: that would hold the request open for as long as the
  // socket lives, and the region can only reclaim an abandoned socket by
  // exiting. Its `__ws_next` ops still belong to the region and are driven
  // while the request runs, then aborted with it.
  _startPump() {
    if (this._pumping) return;
    this._pumping = true;
    this._pump().catch(() => {});
  }
  // Drain the host queue for an isolate-polled socket. Each `__ws_next` is an
  // ordinary async op, so the request's region owns it and aborts it if the
  // request ends first — the socket is closed by the region on the way out.
  async _pump() {
    for (;;) {
      const frame = await __ws_next(this._id);
      const tag = frame[0];
      const body = frame.subarray(1);
      if (tag === 0) {
        this._dispatchMessage(new TextDecoder().decode(body));
      } else if (tag === 1) {
        this._dispatchMessage(
          body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength),
        );
      } else if (tag === 2) {
        if (this.readyState === WebSocket.READY_STATE_CONNECTING) {
          this.protocol = new TextDecoder().decode(body);
          this.readyState = WebSocket.READY_STATE_OPEN;
          this.dispatchEvent(new Event("open"));
        }
      } else {
        const info = JSON.parse(new TextDecoder().decode(body));
        this._dispatchClose(info.code, info.reason, info.wasClean);
        return;
      }
    }
  }
  // Deliver whatever this end queued before it had somewhere to send.
  _flushToPeer() {
    const peer = this._loopback;
    if (!peer) return;
    for (const frame of this._pending.splice(0)) {
      if (frame[0] === "send") {
        queueMicrotask(() => peer._dispatchMessage(frame[1]));
      } else if (frame[0] === "send-binary") {
        const data = frame[1];
        const bytes = data instanceof ArrayBuffer
          ? data
          : data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
        queueMicrotask(() => peer._dispatchMessage(bytes));
      } else {
        queueMicrotask(() => peer._dispatchClose(frame[1], frame[2], true));
      }
    }
  }
  get binaryType() { return this._binaryType; }
  // The spec ignores an unrecognised value rather than throwing.
  set binaryType(value) {
    if (value === "blob" || value === "arraybuffer") this._binaryType = value;
  }
  accept() {
    this._accepted = true;
    // A pair used directly, without being returned through a 101 response,
    // still has to carry frames once both ends are accepted. celld only
    // linked a pair on the upgrade path, so such a pair queued forever.
    if (this._peer && this._peer._accepted && !this._loopback) {
      this._loopback = this._peer;
      this._peer._loopback = this;
      this._flushToPeer();
      this._peer._flushToPeer();
    }
    // A socket obtained from a `fetch()` upgrade is already connected and
    // delivers nothing until it is accepted; this is where it starts.
    if (this._outbound && this.readyState === WebSocket.READY_STATE_CONNECTING) {
      this.readyState = WebSocket.READY_STATE_OPEN;
      if (this._polled) this._startPump();
    }
  }
  send(data) {
    if (this.readyState !== WebSocket.READY_STATE_OPEN)
      throw new DOMException("WebSocket is not open", "InvalidStateError");
    const binary = data instanceof ArrayBuffer || ArrayBuffer.isView(data);
    const text = binary ? null : String(data);
    if (this._loopback) {
      const peer = this._loopback;
      const message = binary
        ? (data instanceof ArrayBuffer ? data.slice(0) : data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength))
        : text;
      queueMicrotask(() => peer._dispatchMessage(message));
      return;
    }
    // The pending queue belongs to an inbound pair socket that has been
    // accepted but not yet bound to a host transport. An outbound socket is
    // already bound -- queueing here would silently swallow its frames.
    if (!this._outbound && this._accepted && !this._target) {
      this._pending.push([binary ? "send-binary" : "send", binary ? data : text]);
      return;
    }
    if (binary) {
      const bytes = data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
      __ws_send_binary(this._id, bytes);
    } else {
      __ws_send(this._id, text);
    }
  }
  close(code = 1000, reason = "") {
    // WHATWG: an application may send 1000 or 3000-4999. Everything else is
    // reserved for the protocol itself.
    const wanted = Number(code);
    if (wanted !== 1000 && !(wanted >= 3000 && wanted <= 4999)) {
      throw new DOMException(
        `WebSocket close code ${code} is not permitted`,
        "InvalidAccessError");
    }
    // RFC 6455 5.5: the close frame body is at most 125 bytes, two of which
    // are the code. Measured in UTF-8, so a multibyte reason can exceed the
    // cap at well under 123 characters.
    if (new TextEncoder().encode(String(reason)).length > 123) {
      throw new DOMException(
        "WebSocket close reason must not exceed 123 bytes",
        "SyntaxError");
    }
    // Outbound sockets follow the WebSocket spec: closing a closed socket is a
    // no-op. Inbound sockets must NOT take that shortcut -- a Durable Object
    // answers a peer-initiated close from inside `webSocketClose`, by which
    // point `_dispatchClose` has already marked this side closed, and its
    // reply carries the reason the client is entitled to see.
    if (this._outbound && this.readyState === WebSocket.READY_STATE_CLOSED) {
      return;
    }
    const connecting = this.readyState === WebSocket.READY_STATE_CONNECTING;
    this.readyState = WebSocket.READY_STATE_CLOSING;
    if (this._outbound && connecting) {
      this._pendingClose = [code, reason];
      return;
    }
    if (this._outbound) {
      __ws_close(this._id, code, reason);
      return;
    }
    this.readyState = 3;
    if (this._loopback) {
      const peer = this._loopback;
      queueMicrotask(() => peer._dispatchClose(code, reason, true));
      return;
    }
    if (this._accepted && !this._target) {
      this._pending.push(["close", code, reason]);
      return;
    }
    __ws_close(this._id, code, reason);
  }
  _flushPending() {
    const pending = this._pending.splice(0);
    for (const frame of pending) {
      if (frame[0] === "send") __ws_send(this._id, frame[1]);
      else if (frame[0] === "send-binary") {
        const data = frame[1];
        const bytes = data instanceof ArrayBuffer
          ? new Uint8Array(data)
          : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
        __ws_send_binary(this._id, bytes);
      }
      else __ws_close(this._id, frame[1], frame[2]);
    }
  }
  serializeAttachment(value) {
    // Structured clone, not JSON: Cloudflare accepts anything cloneable
    // here, so a Date must come back a Date and a Map a Map.
    this._attachment = value;
    __ws_attachment_set(this._id, __sc_encode(value));
  }
  deserializeAttachment() { return this._attachment; }
  _dispatchMessage(data) {
    if (this._binaryType === "blob" && data instanceof ArrayBuffer) {
      data = new Blob([data]);
    }
    const event = new MessageEvent("message", { data });
    event._trust();
    this.dispatchEvent(event);
  }
  _dispatchClose(code, reason, wasClean) {
    if (this.readyState === WebSocket.READY_STATE_CLOSED) return;
    this.readyState = WebSocket.READY_STATE_CLOSED;
    __sockets.delete(this._id);
    // An abnormal closure fires `error` before `close`. Code written against
    // the standard onerror never ran otherwise, which matters most for an
    // outbound socket: its peer is a remote server that can simply vanish.
    // An abnormal closure fires `error` before `close`. Code written against
    // the standard onerror never ran otherwise, which matters most for an
    // outbound socket: its peer is a remote server that can simply vanish.
    // An abnormal closure fires `error` before `close`. Code written against
    // the standard onerror never ran otherwise, which matters most for an
    // outbound socket: its peer is a remote server that can simply vanish.
    if (!wasClean) {
      this.dispatchEvent(new ErrorEvent("error", {
        message: reason
          ? `WebSocket closed abnormally: ${reason}`
          : "WebSocket closed abnormally",
      }));
    }
    const event = new CloseEvent("close", { code, reason, wasClean });
    event._trust();
    this.dispatchEvent(event);
  }
};
globalThis.__makeSocket = (id) => new WebSocket(id);
globalThis.__socketFromRow = (row) => {
  let ws = __sockets.get(Number(row.id));
  if (!ws) {
    ws = __makeSocket(row.id);
    __sockets.set(Number(row.id), ws);
  }
  ws._tags = row.tags || [];
  ws._hibernatable = true;
  if (row.attachment != null) {
    try {
      ws._attachment = __sc_decode(new Uint8Array(row.attachment));
    } catch {
      ws._attachment = undefined;
    }
  }
  return ws;
};
// Workerd's api::WebSocketRequestResponsePair (websocket.h): an immutable
// request/response pair for state.setWebSocketAutoResponse.
globalThis.WebSocketRequestResponsePair =
  class WebSocketRequestResponsePair {
    #request;
    #response;
    constructor(request, response) {
      this.#request = String(request);
      this.#response = String(response);
    }
    get request() { return this.#request; }
    get response() { return this.#response; }
  };
globalThis.WebSocketPair = function WebSocketPair() {
  const id = __ws_alloc();
  const client = __makeSocket(id);
  const server = __makeSocket(id);
  client._peer = server;
  server._peer = client;
  // Indexed AND iterable: `const [client, server] = new WebSocketPair()` is
  // the form most Cloudflare code uses.
  return {
    0: client,
    1: server,
    length: 2,
    [Symbol.iterator]() {
      return [client, server][Symbol.iterator]();
    },
  };
};
if (!globalThis.performance)
  globalThis.performance = { now: () => Date.now(), timeOrigin: 0, mark() {}, measure() {} };
if (!globalThis.navigator) globalThis.navigator = {
  userAgent: "Cloudflare-Workers", hardwareConcurrency: 1,
  language: "en", languages: ["en"],
};
if (!globalThis.queueMicrotask)
  globalThis.queueMicrotask = (f) => Promise.resolve().then(f);
if (!globalThis.scheduler)
  globalThis.scheduler = {
    wait: (ms) => new Promise((resolve) => setTimeout(resolve, Number(ms) || 0)),
  };
if (!globalThis.structuredClone) {
  globalThis.structuredClone = (value, options) => {
    const seen = new Map();
    const clone = (input) => {
      if (input === null || typeof input !== "object") return input;
      if (seen.has(input)) return seen.get(input);
      if (input instanceof Date) return new Date(input.getTime());
      if (input instanceof ArrayBuffer) return input.slice(0);
      if (ArrayBuffer.isView(input))
        return new input.constructor(input.buffer.slice(
          input.byteOffset, input.byteOffset + input.byteLength,
        ));
      if (input instanceof Map) {
        const out = new Map(); seen.set(input, out);
        for (const [k, v] of input) out.set(clone(k), clone(v));
        return out;
      }
      if (input instanceof Set) {
        const out = new Set(); seen.set(input, out);
        for (const v of input) out.add(clone(v));
        return out;
      }
      const out = Array.isArray(input) ? [] : Object.create(Object.getPrototypeOf(input));
      seen.set(input, out);
      for (const key of Reflect.ownKeys(input)) out[key] = clone(input[key]);
      return out;
    };
    const result = clone(value);
    // Transfer semantics: the source buffer is detached. The clone
    // above already copied its bytes, which is what the spec's
    // "transferred" clone observes.
    for (const item of (options && options.transfer) || []) {
      if (item instanceof ArrayBuffer) item.transfer();
    }
    return result;
  };
}
// Web Crypto is installed after the rest of the harness so it can use
// DOMException, structuredClone, and Buffer.
// `Buffer` is read at call time, which materializes the lazy global.
const __zlibSync = (mode, data) =>
  Buffer.from(__zlib(mode, Buffer.from(data)));
globalThis.__zlibModule = {
  constants: {
    Z_NO_FLUSH: 0,
    Z_PARTIAL_FLUSH: 1,
    Z_SYNC_FLUSH: 2,
    Z_FULL_FLUSH: 3,
    Z_FINISH: 4,
    Z_BLOCK: 5,
  },
  gzipSync: (data, _options) => __zlibSync("gzip", data),
  gunzipSync: (data, _options) => __zlibSync("gunzip", data),
  deflateSync: (data, _options) => __zlibSync("deflate", data),
  inflateSync: (data, _options) => __zlibSync("inflate", data),
  deflateRawSync: (data, _options) => __zlibSync("deflateRaw", data),
  inflateRawSync: (data, _options) => __zlibSync("inflateRaw", data),
};
if (!globalThis.process) globalThis.process = {
  env: {}, platform: "linux", arch: "x64", version: "v20.0.0",
  versions: { node: "20.0.0" }, argv: [], cwd: () => "/",
  stdin: { fd: 0, isTTY: false },
  stdout: { fd: 1, isTTY: false, write: (s) => { __log(String(s)); return true; } },
  stderr: { fd: 2, isTTY: false, write: (s) => { __log(String(s)); return true; } },
  nextTick: (f, ...a) => queueMicrotask(() => f(...a)),
  on() {}, once() {}, off() {}, emit() {}, hrtime: () => [0, 0],
};
globalThis.process.exit = (code = 0) => {
  const actorScope = __actorEventStack[__actorEventStack.length - 1] || "";
  if (actorScope) {
    const instance = __cell.instances[actorScope];
    if (instance?.__celldState)
      instance.__celldState._resetAfterConcurrencyFailure();
  }
  __process_exit(Number(code) || 0, actorScope);
};
globalThis.process.getBuiltinModule =
  (id) => __builtin_module(String(id));
if (!globalThis.global) globalThis.global = globalThis;
// Events belong to the request, and the host owns the request. `__event_*`
// and `__wait_until` operate on whichever context the host has made current
// for this turn, so nothing here has to know which request is running — and
// two requests sharing an isolate cannot pop each other's events.
globalThis.__registerWaitUntil = (promise) => {
  const tracked = Promise.resolve(promise).catch((error) => {
    console.error("waitUntil rejected", error);
  });
  __wait_until(tracked);
};
// `props` are the per-stub props a loopback service stub carries
// (ctx.props); `exports` is built once, on first access.
const __defaultProps = {};
globalThis.__beginEvent = (props = __defaultProps) => {
  __event_begin();
  return {
    waitUntil: globalThis.__registerWaitUntil,
    passThroughOnException() {},
    abort: __ctxAbortCurrent,
    props,
    get exports() { return __ctxExports(); },
  };
};
globalThis.__endEvent = () => __event_end();
// fs stub that behaves like a no-filesystem env: reads throw ENOENT and
// existsSync is false, so the common `try { readFileSync } catch (ENOENT)`
// fallbacks in real deps (e.g. pi-agent config) take their default path.
const __enoent = () => { const e = new Error("ENOENT: no filesystem"); e.code = "ENOENT"; throw e; };
globalThis.__fs = new Proxy({}, { get: (_t, p) => {
  if (p === "existsSync") return () => false;
  if (["readFileSync", "readdirSync", "statSync", "lstatSync", "realpathSync", "readlinkSync"].includes(p)) return __enoent;
  return globalThis.__nodeStub;
}});
const __bridgeResponseStream = (body, requestControllers) => {
  const streamId = __response_stream_create();
  const pump = (async () => {
    const reader = body.getReader();
    const consumerClosed = __response_stream_closed(streamId)
      .then((status) => status === "cancelled"
        ? { consumerClosed: true }
        : new Promise(() => {}));
    const cancelProducer = async () => {
      const reason = new Error("The client has disconnected");
      for (const controller of requestControllers || []) {
        if (!controller.signal.aborted) controller.abort(reason);
      }
      try {
        await reader.cancel(reason);
      } catch {}
      await __response_stream_close(streamId, "");
    };
    try {
      for (;;) {
        const result = await Promise.race([
          reader.read().then((read) => ({ read })),
          consumerClosed,
        ]);
        if (result.consumerClosed) {
          await cancelProducer();
          return;
        }
        if (result.read.done) {
          await __response_stream_close(streamId, "");
          return;
        }
        const bytes = __bodyBytes(result.read.value);
        for (let offset = 0; offset < bytes.byteLength; offset += 64 * 1024) {
          await __response_stream_write(
            streamId,
            bytes.subarray(offset, offset + 64 * 1024),
          );
        }
      }
    } catch (error) {
      if (String(error).includes("response stream consumer canceled")) {
        await cancelProducer();
        return;
      }
      await __response_stream_close(streamId, String(error));
    }
  })();
  globalThis.__registerWaitUntil(pump);
  return streamId;
};
globalThis.__readResponse = (r) => {
  if (!(r instanceof Response)) {
    return {
      status: 200,
      bodyBytes: new TextEncoder().encode(String(r)),
      bodyStreamId: 0,
      headersJson: "[]",
      wsTargetJson: "null",
    };
  }
  const bodyStreamId = r._bodyBytes === null
    ? typeof r.body?.__celldStreamId === "number" &&
        !r.__celldRequestControllers
      ? r.body.__celldStreamId
      : __bridgeResponseStream(
        r.body,
        r.__celldRequestControllers,
      )
    : 0;
  if (bodyStreamId) {
    return {
      status: r.status,
      bodyBytes: new Uint8Array(),
      bodyStreamId,
      headersJson: JSON.stringify(Array.from(r.headers)),
      wsTargetJson: JSON.stringify(
        r._wsTarget || (r.webSocket && r.webSocket._target) || null,
      ),
    };
  }
  const bytes = r._bodyBytes;
  return {
    status: r.status,
    bodyBytes: bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes),
    bodyStreamId: 0,
    headersJson: JSON.stringify(Array.from(r.headers)),
    wsTargetJson: JSON.stringify(
      r._wsTarget || (r.webSocket && r.webSocket._target) || null,
    ),
  };
};
