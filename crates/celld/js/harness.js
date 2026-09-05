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
  // text() memoizes into _body on demand. Decoding here charges every
  // binary passthrough the cost of a string it never reads.
  target._body = undefined;
  return bytes;
}
class CelldBodyStream extends ReadableStream {
  constructor(owner) {
    // Couple the stream to its buffered owner, so _bodyBytes and the stream
    // cannot be installed from different arrays. Copying the owner's bytes
    // again here doubles the external memory held for every constructed body.
    if (!(owner instanceof globalThis.Request) &&
        !(owner instanceof globalThis.Response))
      throw new TypeError("CelldBodyStream requires a buffered body owner");
    const bytes = owner._bodyBytes;
    if (!(bytes instanceof Uint8Array))
      throw new TypeError("CelldBodyStream owner requires buffered body bytes");
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
        // A chunk arrives as a Uint8Array. The host moves it in without
        // a copy. The end of the stream arrives as a string.
        const chunk = await __http_stream_read(id);
        if (typeof chunk === "string") controller.close();
        else controller.enqueue(chunk);
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
    // Never decoded eagerly: a whole-body UTF-8 decode costs seconds for a
    // large binary body (an archive, a git pack) and can exceed V8's string
    // length. On Response the field is deliberately write-only — text()
    // decodes fresh from _bodyBytes and only Request.text() memoizes into
    // _body — but the assignment stays: __drainBody and __adoptBody assign
    // _body on both classes, so defining it here keeps the hidden-class
    // shape stable. Do not add a Response reader; do not "simplify" this
    // away.
    this._body = undefined;
    // Hono rebuilds a response after middleware with
    // `new Response(response.body, response)`; the held stream (or a
    // fresh CelldBodyStream) preserves the payload across that
    // standard clone shape.
    this.body = body == null
      ? null
      : stream !== null
        ? stream
        : new CelldBodyStream(this);
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
  static redirect(url, status = 302) {
    const input = String(url);
    const statusCode = Number(status) | 0;
    if (![301, 302, 303, 307, 308].includes(statusCode)) {
      throw new RangeError(
        `${statusCode} is not a redirect status code. ` +
        "It must be one of: 301, 302, 303, 307, or 308.");
    }
    const response = new Response(null, {
      status: statusCode,
      headers: { Location: new URL(input).href },
    });
    // A redirect response owns a guarded header list. Without the guard, an
    // application can rewrite Location after construction, which differs
    // from both Fetch and Workerd even though the initial response is valid.
    Object.defineProperty(response.headers, "_immutable", { value: true });
    return response;
  }
  static error() {
    const response = new Response(null);
    response.status = 0;
    response.ok = false;
    response.type = "error";
    return response;
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
    // The undecoded bytes, not `text()`: a UTF-8 decode replaces each invalid
    // sequence in a binary file part with one U+FFFD, so both the bytes and
    // the length change.
    this.bodyUsed = true;
    return __parseFormData(
      await this._consume(), this.headers.get('content-type'));
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
    const response = new Response(
      this.body === null ? null : this._bodyBytes,
      {
        status: this.status, statusText: this.statusText, headers: this.headers,
        webSocket: this.webSocket, __wsTarget: this._wsTarget, cf: this.cf,
      },
    );
    response.type = this.type;
    return response;
  }
};
// Hoisted: this would otherwise allocate an array and scan it on
// every Request construction, which is the HTTP hot path.
const __HTTP_METHODS = new Set(
  ["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]);
const __FETCH_REDIRECT_MODES = new Set(["follow", "manual", "error"]);
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
      // Per spec `new Request(request)` copies the header list. The prior
      // request's `Headers` object cannot stand in for it: `new Headers(h)`
      // reads `h` through its iterator, which sorts, lower-cases and
      // combines, so forwarding a request whose headers were read would
      // undo the wire shape the outbound paths preserve.
      this._headersInit = prior._headers
        ? prior._headers.__celldHeaderList
        : prior._headersInit;
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
        ? null : new CelldBodyStream(this);
    this.redirect = String(init.redirect === undefined
      ? (prior ? prior.redirect : "follow")
      : init.redirect);
    if (!__FETCH_REDIRECT_MODES.has(this.redirect)) {
      throw new TypeError(`Invalid redirect mode: ${this.redirect}`);
    }
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
    // The undecoded bytes, not `text()`: a UTF-8 decode replaces each invalid
    // sequence in a binary file part with one U+FFFD, so both the bytes and
    // the length change.
    this.bodyUsed = true;
    return __parseFormData(
      await this._consume(), this.headers.get('content-type'));
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
const __consoleNoop = () => {};
globalThis.console = {
  debug: (...a) => __log(__fmt(a)),
  error: (...a) => __log("ERROR " + __fmt(a)),
  info: (...a) => __log(__fmt(a)),
  log: (...a) => __log(__fmt(a)),
  warn: (...a) => __log("WARN " + __fmt(a)),
  clear: __consoleNoop,
  count: __consoleNoop,
  group: __consoleNoop,
  table: __consoleNoop,
  trace: __consoleNoop,
  assert: __consoleNoop,
  countReset: __consoleNoop,
  dir: __consoleNoop,
  dirxml: __consoleNoop,
  groupCollapsed: __consoleNoop,
  groupEnd: __consoleNoop,
  profile: __consoleNoop,
  profileEnd: __consoleNoop,
  time: __consoleNoop,
  timeEnd: __consoleNoop,
  timeLog: __consoleNoop,
  timeStamp: __consoleNoop,
  createTask: () => {
    throw new Error("console.createTask() is not implemented");
  },
};

// async-op shims: outbound fetch + timers, driven by the host event loop.
// Move one request body through a host subrequest seam. A body that already
// names a host stream keeps that stream incremental. An ordinary JavaScript
// stream has no host owner, so it uses the existing byte fallback.
const __subrequestBody = async (req, absent = false) => {
  if (absent) return { body: undefined, streamId: undefined };
  if (req._bodyBytes !== null)
    return { body: req._bodyBytes, streamId: undefined };
  const streamId = req.body?.__celldStreamId;
  if (streamId === undefined) {
    req.bodyUsed = true;
    return { body: await req._consume(), streamId: undefined };
  }
  // The receiver now owns the one host source. Lock the wrapper so a later
  // read fails instead of racing the receiver, and expose the transfer through
  // the standard Body flag.
  req.bodyUsed = true;
  req.body.getReader();
  return { body: new Uint8Array(), streamId };
};
// A data: URL carries its response inline; RFC 2397 percent-decoding
// happens byte-wise, so a payload that is deliberately invalid UTF-8
// survives to the consumer (HTMLRewriter's own suite depends on it).
const __dataUrlResponse = (url) => {
  // The URL parser strips leading and trailing C0 controls and spaces
  // before doing anything else; a literal trailing space in a data: URL
  // is therefore not part of the payload, while an encoded %20 is.
  url = url.replace(/^[\x00-\x20]+/, "").replace(/[\x00-\x20]+$/, "");
  const match = /^data:([^,]*),([\s\S]*)$/.exec(url);
  if (!match) throw new TypeError("Invalid data: URL");
  let [, meta, payload] = match;
  const base64 = /;base64$/i.test(meta);
  if (base64) meta = meta.replace(/;base64$/i, "");
  let bytes;
  if (base64) {
    const bin = atob(payload);
    bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
  } else {
    const out = [];
    for (let i = 0; i < payload.length; i++) {
      const c = payload[i];
      if (c === "%" && /^[0-9A-Fa-f]{2}$/.test(payload.slice(i + 1, i + 3))) {
        out.push(parseInt(payload.slice(i + 1, i + 3), 16));
        i += 2;
      } else {
        out.push(payload.charCodeAt(i) & 0xff);
      }
    }
    bytes = Uint8Array.from(out);
  }
  return new Response(bytes, {
    headers: { "content-type": meta || "text/plain;charset=US-ASCII" },
  });
};
globalThis.fetch = async (input, init) => {
  const rawUrl = typeof input === "string" ? input : input?.url;
  if (typeof rawUrl === "string" && rawUrl.startsWith("data:")) {
    return __dataUrlResponse(rawUrl);
  }
  const req = new Request(input, init);
  // `fetch(url, { headers: { Upgrade: "websocket" } })` is the other way
  // Cloudflare opens an outbound socket, and the one most examples use.
  if ((req.headers.get("upgrade") ?? "").toLowerCase() === "websocket") {
    return await __fetchWebSocketUpgrade(req);
  }
  // The caller's signal, never the incoming request's own.
  const signal = req._signalForSubrequests;
  if (signal?.aborted) throw signal.reason;
  // The bytes go to the host as a typed array, the same way every other
  // subrequest op takes a body. Encoding them as a JSON number array cost
  // roughly ten times the body in transient allocation, and it corrupted
  // nothing only because the host decoded the same shape back.
  const { body, streamId } = await __subrequestBody(
    req, ["GET", "HEAD"].includes(req.method));
  // The header list, not the iterator: the iterator sorts, lower-cases and
  // combines, so `Array.from(req.headers)` would send one `X-Trace: a, b`
  // for two appends and put celld's casing on the wire in place of the
  // author's. `op_fetch` builds the request with `RequestBuilder::header`,
  // which appends, so repeats survive.
  let raw;
  try {
    const dispatch = __op_fetch(
      req.method, req.url, body,
      JSON.stringify(req.headers.__celldHeaderList), req.redirect,
      streamId, signal != null,
    );
    if (signal) {
      // An abort and the response can cross, leaving a body stream nobody
      // reads holding its upstream connection until the sweeper expires it.
      // Every arm is silenced because the caller already owns both outcomes.
      dispatch.then((late) => {
        if (!signal.aborted) return;
        const abandoned = JSON.parse(late).streamId;
        if (abandoned !== undefined) __http_stream_cancel(abandoned);
      }, () => {}).catch(() => {});
    }
    raw = await (signal ? __awaitCancellableDoCall(dispatch, signal) : dispatch);
  } catch (error) {
    // An abort is the caller's reason, not a network error, so it skips
    // every translation below.
    if (signal?.aborted && error === signal.reason) throw error;
    // The Fetch API represents a redirect refusal as a network error, which
    // rejects with TypeError. Keep the host message for diagnostics.
    if (req.redirect === "error") {
      throw new TypeError(error?.message ?? String(error));
    }
    throw error;
  }
  const r = JSON.parse(raw);
  const response = new Response(
    new CelldHttpBodyStream(r.streamId),
    { status: r.status, headers: r.headers },
  );
  response.url = req.url;
  return response;
};
globalThis.__fetchWebSocketUpgrade = async (req) => {
  const scope = __currentActorScope();
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
      // The iterator, not the header list, unlike every other outbound
      // path. The handshake builder on the host side puts these into a
      // `HeaderMap` with `insert`, which replaces, and it reads the
      // offered subprotocols with a first-match `find`. Both would drop
      // every repeat but one. Sending the combined form keeps
      // `Sec-WebSocket-Protocol: a` plus `: b` meaning `a, b`, which is
      // what the peer must see. Moving this to the header list needs the
      // host to append instead, so it is a separate change.
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
    const responseBody = r.streamId !== undefined
      ? new CelldHttpBodyStream(r.streamId)
      : r.body !== undefined ? r.body : Uint8Array.from(r.bodyBytes || []);
    const response = new Response(
      responseBody,
      { status: r.status, headers: r.headers },
    );
    response.url = req.url;
    return response;
  },
});
// An `r2_buckets` binding, served out of the fleet bucket under the
// reserved `r2/<bucketName>/` prefix. celld runs on blob storage rather
// than providing it, so a binding gets the store the node already holds
// credentials for; a node started without a bucket has nowhere to put a
// blob and every method says so.
//
// The whole `R2Bucket` surface is here: `head`, `get`, `put`, `delete`,
// `list`, `createMultipartUpload` and `resumeMultipartUpload`, with
// `httpMetadata`, `customMetadata`, `checksums`, `storageClass`, `onlyIf`
// and every range spelling. What celld cannot honor still fails loudly
// rather than pretending — a silent gap is the failure mode the binding
// is written to avoid. Those are `ssecKey` (celld has no customer-key
// encryption), a conditional `put` of a body too big for one request, and
// a multipart upload resumed on a node other than the one that opened it.
const __r2Reject = (bucket, method, detail) => {
  throw new Error(
    `R2 ${method} is not implemented in celld (binding ${bucket})` +
      (detail ? `: ${detail}` : ""),
  );
};
// R2's key rules, checked here so a key the real thing refuses is refused
// here too rather than quietly written.
const __r2Key = (binding, method, key) => {
  const text = String(key);
  const length = new TextEncoder().encode(text).length;
  if (length === 0 || length > 1024) {
    throw new Error(
      `R2 ${method} (binding ${binding}): a key is 1 to 1024 bytes, not ${length}`,
    );
  }
  return text;
};
const __r2Hex = (value) => {
  if (typeof value === "string") return value.trim().toLowerCase();
  const bytes = value instanceof ArrayBuffer
    ? new Uint8Array(value)
    : ArrayBuffer.isView(value)
    ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
    : null;
  if (!bytes) return null;
  let out = "";
  for (const byte of bytes) out += byte.toString(16).padStart(2, "0");
  return out;
};
const __r2Bytes = (hex) => {
  const out = new Uint8Array(hex.length >> 1);
  for (let at = 0; at < out.length; at++) {
    out[at] = parseInt(hex.substr(at * 2, 2), 16);
  }
  return out.buffer;
};
// Read a `ReadableStream` to one `Uint8Array`. Bodies drained this way
// are the ones the caller asked to have whole.
const __r2Drain = async (stream) => {
  const reader = stream.getReader();
  const chunks = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    total += value.length;
  }
  const out = new Uint8Array(total);
  let at = 0;
  for (const chunk of chunks) {
    out.set(chunk, at);
    at += chunk.length;
  }
  return out;
};
const __r2Ms = (value) => {
  if (value === undefined || value === null) return undefined;
  const ms = value instanceof Date ? value.getTime() : Number(value);
  return Number.isFinite(ms) ? ms : undefined;
};
// `httpMetadata` is either an `R2HTTPMetadata` or a `Headers`, and R2
// takes both on the way in.
const __r2Http = (value) => {
  if (value === undefined || value === null) return {};
  const from = (name, key) =>
    typeof value.get === "function" ? value.get(name) ?? undefined : value[key];
  const http = {
    contentType: from("content-type", "contentType"),
    contentLanguage: from("content-language", "contentLanguage"),
    contentDisposition: from("content-disposition", "contentDisposition"),
    contentEncoding: from("content-encoding", "contentEncoding"),
    cacheControl: from("cache-control", "cacheControl"),
    cacheExpiry: __r2Ms(
      typeof value.get === "function" ? value.get("expires") : value.cacheExpiry,
    ),
  };
  for (const [name, entry] of Object.entries(http)) {
    if (entry === undefined || entry === null) delete http[name];
    else if (name !== "cacheExpiry") http[name] = String(entry);
  }
  return http;
};
// `onlyIf` is either an `R2Conditional` or a `Headers`; the header
// spelling of "only if it has not changed since" is If-Unmodified-Since,
// which is `uploadedBefore`.
const __r2OnlyIf = (value) => {
  if (value === undefined || value === null) return undefined;
  const only = typeof value.get === "function"
    ? {
      etagMatches: value.get("if-match") ?? undefined,
      etagDoesNotMatch: value.get("if-none-match") ?? undefined,
      uploadedAfter: __r2Ms(Date.parse(value.get("if-modified-since") ?? "")),
      uploadedBefore: __r2Ms(Date.parse(value.get("if-unmodified-since") ?? "")),
    }
    : {
      etagMatches: value.etagMatches,
      etagDoesNotMatch: value.etagDoesNotMatch,
      uploadedAfter: __r2Ms(value.uploadedAfter),
      uploadedBefore: __r2Ms(value.uploadedBefore),
    };
  for (const [name, entry] of Object.entries(only)) {
    if (entry === undefined || entry === null || Number.isNaN(entry)) {
      delete only[name];
    } else if (name.startsWith("etag")) only[name] = String(entry);
  }
  return Object.keys(only).length ? only : undefined;
};
// A range is an `R2Range` or a `Headers` carrying a Range header. R2
// takes `offset` alone, `length` alone, both, or `suffix`.
const __r2Range = (binding, value) => {
  if (value === undefined || value === null) return undefined;
  if (typeof value.get === "function") {
    const header = value.get("range");
    if (!header) return undefined;
    const parsed = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
    if (!parsed) {
      __r2Reject(binding, "get(range)", `celld cannot parse ${header}`);
    }
    const [, first, last] = parsed;
    if (first === "") return { suffix: Number(last) };
    if (last === "") return { offset: Number(first) };
    return { offset: Number(first), length: Number(last) - Number(first) + 1 };
  }
  const range = {};
  if (value.suffix !== undefined && value.suffix !== null) {
    range.suffix = Number(value.suffix);
  }
  if (value.offset !== undefined && value.offset !== null) {
    range.offset = Number(value.offset);
  }
  if (value.length !== undefined && value.length !== null) {
    range.length = Number(value.length);
  }
  for (const [name, entry] of Object.entries(range)) {
    if (!Number.isFinite(entry) || entry < 0) {
      __r2Reject(binding, "get(range)", `\`${name}\` must be a whole number`);
    }
  }
  return Object.keys(range).length ? range : undefined;
};
// R2 hashes an object's bytes and hands the digests back as
// `ArrayBuffer`s, with a `toJSON()` that spells them in hex.
const __r2Checksums = (hex) => {
  const checksums = {
    toJSON() {
      const json = {};
      for (const [name, digest] of Object.entries(hex)) json[name] = digest;
      return json;
    },
  };
  for (const [name, digest] of Object.entries(hex)) {
    checksums[name] = __r2Bytes(digest);
  }
  return checksums;
};
// One object as R2 describes it. `object` is the host's record; `body`,
// when there is one, makes this an `R2ObjectBody`.
const __r2Object = (object, body) => {
  const http = { ...object.http };
  if (http.cacheExpiry !== undefined) {
    http.cacheExpiry = new Date(http.cacheExpiry);
  }
  const etag = object.etag ? String(object.etag).replace(/^"|"$/g, "") : "";
  const r2 = {
    key: object.key,
    version: object.version ?? etag,
    size: object.size,
    etag,
    httpEtag: etag ? `"${etag}"` : "",
    uploaded: new Date(object.uploaded),
    httpMetadata: http,
    customMetadata: object.custom ?? {},
    checksums: __r2Checksums(object.checksums ?? {}),
    storageClass: object.storageClass ?? "Standard",
    // R2 fills the response headers from the object's own metadata, which
    // is what makes `return new Response(object.body, { headers })` serve
    // a stored file correctly.
    writeHttpMetadata(headers) {
      const write = (name, value) => {
        if (value !== undefined && value !== null) headers.set(name, value);
      };
      write("content-type", http.contentType);
      write("content-language", http.contentLanguage);
      write("content-disposition", http.contentDisposition);
      write("content-encoding", http.contentEncoding);
      write("cache-control", http.cacheControl);
      if (http.cacheExpiry !== undefined) {
        headers.set("expires", http.cacheExpiry.toUTCString());
      }
    },
  };
  if (object.range) r2.range = { ...object.range };
  if (!body) return r2;
  const drain = () => __r2Drain(body);
  r2.body = body;
  Object.defineProperty(r2, "bodyUsed", {
    get: () => body.locked || body._disturbed === true,
  });
  r2.arrayBuffer = async () => (await drain()).buffer;
  r2.bytes = drain;
  r2.text = async () => new TextDecoder().decode(await drain());
  r2.json = async () => JSON.parse(new TextDecoder().decode(await drain()));
  r2.blob = async () => new Blob([await drain()]);
  return r2;
};
// The write-side options every writing method shares, in the shape the
// host ops read.
const __r2WriteOptions = (binding, method, options) => {
  const o = options ?? {};
  if (o.ssecKey !== undefined) {
    __r2Reject(
      binding,
      `${method}(ssecKey)`,
      "celld has no customer-key encryption",
    );
  }
  const custom = {};
  for (const [name, value] of Object.entries(o.customMetadata ?? {})) {
    custom[name] = String(value);
  }
  const verify = {};
  for (const name of ["md5", "sha1", "sha256", "sha384", "sha512"]) {
    if (o[name] === undefined || o[name] === null) continue;
    const hex = __r2Hex(o[name]);
    if (hex === null) {
      __r2Reject(
        binding,
        `${method}(${name})`,
        "a checksum is hex or an ArrayBuffer",
      );
    }
    verify[name] = hex;
  }
  const storageClass = o.storageClass === undefined || o.storageClass === null
    ? undefined
    : String(o.storageClass);
  if (storageClass !== undefined &&
    !["Standard", "InfrequentAccess"].includes(storageClass)
  ) {
    __r2Reject(
      binding,
      `${method}(storageClass)`,
      `${storageClass} is not an R2 storage class`,
    );
  }
  return {
    http: __r2Http(o.httpMetadata),
    custom,
    storageClass,
    onlyIf: __r2OnlyIf(o.onlyIf),
    verify,
  };
};
// A `put` body, as either the bytes to send in one request or the stream
// to feed the host chunk by chunk. R2 takes all of these.
const __r2Value = async (binding, value) => {
  if (value === undefined || value === null) return new Uint8Array(0);
  if (typeof value === "string") return new TextEncoder().encode(value);
  if (value instanceof Uint8Array) return value;
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (typeof Blob !== "undefined" && value instanceof Blob) {
    return new Uint8Array(await value.arrayBuffer());
  }
  if (value && typeof value.getReader === "function") return value;
  return __r2Reject(
    binding,
    "put",
    "a body is a string, an ArrayBuffer, a view, a Blob, a ReadableStream, or null",
  );
};
// Feed a `ReadableStream` body to the host one chunk at a time. The host
// writes one request when the whole body fits in a part and a multipart
// upload when it does not, so a Worker can stream a body far larger than
// its heap.
const __r2PutStream = async (bucketName, key, stream, options) => {
  const id = Number(await __r2_put_begin(bucketName, key, JSON.stringify(options)));
  const reader = stream.getReader();
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value || value.length === 0) continue;
      await __r2_put_chunk(
        id,
        value instanceof Uint8Array
          ? value
          : new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
      );
    }
  } catch (error) {
    await __r2_put_end(id, true);
    throw error;
  }
  return JSON.parse(await __r2_put_end(id, false));
};
// One open multipart upload. `id` resolves to the host's upload id, so
// `resumeMultipartUpload` can hand back a handle without awaiting — as R2
// does — and surface a bad id on the first method that uses it.
const __r2Multipart = (binding, bucketName, key, id) => ({
  key,
  get uploadId() {
    return String(id.value ?? "");
  },
  async uploadPart(partNumber, value, options) {
    if (options?.ssecKey !== undefined) {
      __r2Reject(binding, "uploadPart(ssecKey)", "celld has no customer-key encryption");
    }
    const body = await __r2Value(binding, value);
    const bytes = body instanceof Uint8Array ? body : await __r2Drain(body);
    const r = JSON.parse(await __r2_mp_part(await id.host, partNumber | 0, bytes));
    // The object store owns the part list this completion acts on, so it
    // never hands back a part etag to name one with.
    return { partNumber: r.partNumber, etag: "" };
  },
  async complete(parts) {
    const numbers = (parts ?? []).map((part) => part.partNumber | 0);
    return __r2Object(JSON.parse(
      await __r2_mp_complete(await id.host, JSON.stringify(numbers)),
    ));
  },
  async abort() {
    await __r2_mp_abort(await id.host);
  },
});
globalThis.__makeR2Bucket = (binding, bucketName) => ({
  async head(key) {
    const name = __r2Key(binding, "head", key);
    const r = JSON.parse(await __r2_head(bucketName, name));
    return r.state === "miss" ? null : __r2Object(r.object);
  },
  async get(key, options) {
    const name = __r2Key(binding, "get", key);
    const o = options ?? {};
    if (o.ssecKey !== undefined) {
      __r2Reject(binding, "get(ssecKey)", "celld has no customer-key encryption");
    }
    const request = {
      range: __r2Range(binding, o.range),
      onlyIf: __r2OnlyIf(o.onlyIf),
    };
    const r = JSON.parse(await __r2_get(bucketName, name, JSON.stringify(request)));
    if (r.state === "miss") return null;
    // A refused `onlyIf` answers the object without its body, which is how
    // a caller tells "not modified" from "not there".
    if (r.state === "unmet") return __r2Object(r.object);
    return __r2Object(r.object, new CelldHttpBodyStream(r.streamId));
  },
  async put(key, value, options) {
    const name = __r2Key(binding, "put", key);
    const write = __r2WriteOptions(binding, "put", options);
    const body = await __r2Value(binding, value);
    const r = body instanceof Uint8Array
      ? JSON.parse(await __r2_put(bucketName, name, body, JSON.stringify(write)))
      : await __r2PutStream(bucketName, name, body, write);
    // R2 answers a refused `onlyIf` with `null` rather than throwing.
    return r.stored ? __r2Object(r.object) : null;
  },
  async delete(keys) {
    const list = (Array.isArray(keys) ? keys : [keys]).map((key) =>
      __r2Key(binding, "delete", key)
    );
    if (list.length > 1000) {
      throw new Error(
        `R2 delete (binding ${binding}) takes at most 1000 keys, not ${list.length}`,
      );
    }
    if (list.length) await __r2_delete(bucketName, JSON.stringify(list));
  },
  async list(options) {
    const o = options ?? {};
    const include = Array.isArray(o.include) ? o.include : [];
    for (const entry of include) {
      if (!["httpMetadata", "customMetadata"].includes(entry)) {
        __r2Reject(binding, "list(include)", `${entry} is not an R2 listing extra`);
      }
    }
    const request = {
      prefix: o.prefix === undefined || o.prefix === null ? "" : String(o.prefix),
      cursor: o.cursor === undefined || o.cursor === null ? null : String(o.cursor),
      startAfter: o.startAfter === undefined || o.startAfter === null
        ? null
        : String(o.startAfter),
      limit: o.limit === undefined || o.limit === null ? 0 : o.limit | 0,
      delimiter: o.delimiter === undefined || o.delimiter === null
        ? null
        : String(o.delimiter),
      include: include.length > 0,
    };
    const r = JSON.parse(await __r2_list(bucketName, JSON.stringify(request)));
    const objects = r.objects.map((object) => {
      // A listing carries the extras it was asked for and nothing else,
      // so a caller cannot mistake an empty map for an empty object.
      if (!include.includes("httpMetadata")) object.http = {};
      if (!include.includes("customMetadata")) object.custom = {};
      return __r2Object(object);
    });
    // R2 omits `cursor` entirely when the listing is complete, and callers
    // test `truncated` before reading it.
    const page = { objects, truncated: r.truncated, delimitedPrefixes: r.prefixes };
    if (r.truncated && r.cursor) page.cursor = r.cursor;
    return page;
  },
  async createMultipartUpload(key, options) {
    const name = __r2Key(binding, "createMultipartUpload", key);
    const write = __r2WriteOptions(binding, "createMultipartUpload", options);
    if (write.onlyIf) {
      __r2Reject(
        binding,
        "createMultipartUpload(onlyIf)",
        "a multipart completion takes no precondition",
      );
    }
    // R2 takes a checksum on `put`, which sees the whole body, and not on a
    // multipart upload, which never does.
    for (const name of Object.keys(write.verify)) {
      __r2Reject(binding, `createMultipartUpload(${name})`, "R2 takes a checksum on `put`");
    }
    const uploadId = await __r2_mp_begin(bucketName, name, JSON.stringify(write));
    return __r2Multipart(binding, bucketName, name, {
      value: uploadId,
      host: Promise.resolve(Number(uploadId)),
    });
  },
  // R2 hands back a handle without checking it; so does this, and the
  // first method that uses it says whether the upload is still open.
  resumeMultipartUpload(key, uploadId) {
    const name = __r2Key(binding, "resumeMultipartUpload", key);
    const id = String(uploadId);
    const host = __r2_mp_resume(bucketName, name, id).then(Number);
    // A handle nobody uses must not become an unhandled rejection; the
    // first method that awaits `host` still sees the failure.
    host.catch(() => {});
    return __r2Multipart(binding, bucketName, name, { value: id, host });
  },
});
// Tear down an upstream body the caller has given up on. The host source goes
// first, because that drops the upstream connection and `ReadableStream.cancel`
// rejects on the locked stream a reader holds. Erroring the stream then gives
// the reader the caller's reason instead of the registry's "expired or is not
// registered", which reads as an engine fault.
const __aiCancelBody = (response, reason) => {
  const body = response?.body;
  if (body?.__celldStreamId === undefined) return;
  __http_stream_cancel(body.__celldStreamId);
  body._controller?.error(reason);
};
// Workers AI passes a third argument to `run()`. `returnRawResponse` hands the
// caller an unconsumed Response for a streaming completion, whatever its
// status; parsing stays the default, and only that default rejects a non-2xx
// status. `fetch` covers the signal up to the response head, after which the
// body is a host stream, so the raw path installs its own abort listener.
globalThis.__makeAiBinding = (url) => ({
  async run(model, input, options) {
    const signal = options?.signal;
    const response = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model, input }),
      signal,
    });
    // The raw path returns ahead of every status test, so no arrangement of
    // this function can convert a non-2xx response into a throw. Workers AI
    // hands the error Response back as well, and the status is the only thing
    // an OpenAI-compatible client has to classify the failure with. A throw
    // reaches such a client as a failed fetch, which it reports as a connection
    // error and retries, so a permanent 403 becomes an unbounded retry loop.
    // The body stays unconsumed here, because the caller owns it.
    if (options?.returnRawResponse) {
      if (signal) {
        signal.addEventListener(
          "abort", () => __aiCancelBody(response, signal.reason), { once: true });
      }
      return response;
    }
    if (!response.ok) {
      // Draining the body is the work `__aiCancelBody` did here before, so the
      // cancel stays only for a read that fails and leaves the source live.
      // `fetch` covers the signal to the response head only, so the read is
      // raced: an upstream that stalls its error body must not hang a caller.
      const read = response.text().catch((error) => {
        __aiCancelBody(response, error);
        return "";
      });
      const detail = signal
        ? await __raceCallerAbort(
          read, signal, () => __aiCancelBody(response, signal.reason))
        : await read;
      // An upstream body is unbounded and this message reaches a log.
      const suffix = detail ? `: ${detail.slice(0, 512)}` : "";
      throw new Error(`AI binding returned ${response.status}${suffix}`);
    }
    const parse = response.json();
    return signal
      ? await __raceCallerAbort(
        parse, signal, () => __aiCancelBody(response, signal.reason))
      : await parse;
  },
});
// A host timer op resolves once, so an interval arms a new one after every
// round. `__timers` maps the id the caller holds to the op that is armed
// right now: for a timeout that is the same id, and for an interval it is a
// fresh id each round. Clearing therefore cancels the armed op, not the id.
const __timers = new Map();
const __clearTimer = (id) => {
  const armed = __timers.get(id);
  if (armed === undefined) return;
  __timers.delete(id);
  __timer_cancel(armed);
};
globalThis.setTimeout = (cb, ms, ...a) => {
  const id = __timer_alloc();
  __timers.set(id, id);
  __op_timer(id, ms | 0).then(() => {
    if (!__timers.delete(id)) return;
    cb(...a);
  });
  return id;
};
globalThis.setInterval = (cb, ms, ...a) => {
  const id = __timer_alloc();
  const delay = ms | 0;
  const arm = (armed) => {
    __timers.set(id, armed);
    __op_timer(armed, delay).then(() => {
      // Cancelling drops the entry and cancelling then re-arming replaces
      // it, so a round whose op is no longer the armed one is dead and
      // stops here. A cancelled op still resolves, which is why this test
      // is on identity and not on the op alone.
      if (__timers.get(id) !== armed) return;
      // Arm the next round before the callback runs. A `clearInterval`
      // inside the callback must end the interval, and arming after the
      // callback would undo it.
      arm(__timer_alloc());
      cb(...a);
    });
  };
  arm(id);
  return id;
};
// One id space and one cancel, because a caller can cross the two.
globalThis.clearTimeout = __clearTimer;
globalThis.clearInterval = __clearTimer;

const __sqlCursorFinalizer = typeof FinalizationRegistry === "function"
  ? new FinalizationRegistry((cursorId) => __sql_cursor_close(cursorId))
  : null;

// A `SqlStorageCursor` over one exec's result (Cloudflare DO SQL API).
class SqlCursor {
  // `res` is what `__sql_cursor_start` returns: V8 values throughout, with the
  // first row already stepped. A failing query throws out of the op, so there
  // is no in-band error field to test here.
  constructor(res) {
    this.columns = res.columns;
    this.rowsWritten = Number(res.rowsWritten || 0);
    this.reusedCachedQueryForTest = Boolean(res.reusedCachedQuery);
    this._deferredError = null;
    this._cursorId = Number(res.cursorId || 0);
    this._prefetched = res.row;
    this.rowsRead = this._prefetched === null ? 0 : 1;
    this._finalizerToken = null;
    if (this._cursorId && __sqlCursorFinalizer) {
      this._finalizerToken = {};
      __sqlCursorFinalizer.register(
        this, this._cursorId, this._finalizerToken,
      );
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
    // The op answers with V8 values, not JSON: an array is a row, a number is
    // the cursor's final rowsWritten, and a failure throws. The throw is
    // caught and deferred, because a consumer must still receive the last good
    // row -- the error belongs to the call after it, not to this one.
    let result;
    try {
      result = __sql_cursor_next(this._cursorId);
    } catch (error) {
      this._deferredError = error;
      this._prefetched = null;
      this._finishNative();
      return;
    }
    if (Array.isArray(result)) {
      this._prefetched = result;
      this.rowsRead++;
      return;
    }
    this.rowsWritten = Number(result || this.rowsWritten);
    this._prefetched = null;
    this._finishNative();
  }
  _nextRaw() {
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
  toArray() {
    const rows = [];
    while (true) {
      const result = this.next();
      if (result.done) return rows;
      rows.push(result.value);
      if (__heap_limit_excessively_exceeded())
        throw new Error(
          "the isolate is over its V8 heap limit, so it stopped " +
          "materializing a result set (see CELLD_V8_HEAP_LIMIT_MB)");
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
  // The state is what the abort guard in `exec` reads; a storage built
  // without one would bypass the guard silently.
  constructor(scope, state, storage) {
    if (!state) throw new TypeError("SqlStorage requires its DurableObjectState");
    this._scope = scope;
    this._state = state;
    this._storage = storage;
  }
  prepare(query) {
    this._storage._assertTransactionActive("sql.prepare");
    const storage = this;
    const source = String(query);
    return (...binds) => storage.exec(source, ...binds);
  }
  exec(query, ...binds) {
    this._storage._assertTransactionActive("sql.exec");
    // A callback that outlived its block runs on an object that was reset,
    // whose replacement uses the same connection: a statement from it
    // would land in the replacement's state. The KV surface drops such
    // writes silently; SQL refuses aloud, since a cursor can carry reads.
    if (this._state._aborted) {
      throw new Error("the Durable Object was reset; this event's storage is closed");
    }
    const encode = (value) => {
      if (value instanceof ArrayBuffer)
        return { __celld_bytes: Array.from(new Uint8Array(value)) };
      if (ArrayBuffer.isView(value))
        return { __celld_bytes: Array.from(
          new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
        ) };
      return value;
    };
    return new SqlCursor(__sql_cursor_start(
      this._scope, query, JSON.stringify(binds.map(encode)),
    ));
  }
  ingest(input) {
    this._storage._assertTransactionActive("sql.ingest");
    const result = JSON.parse(__sql_ingest(this._scope, String(input)));
    if (result.error) throw new Error("SQL error: " + result.error);
    return result;
  }
  get databaseSize() {
    this._storage._assertTransactionActive("sql.databaseSize");
    return __sql_database_size(this._scope);
  }
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
    this._storage._assertTransactionActive("kv.get");
    key = String(key);
    return __unwrapStored(__readStoredValue(
      this._storage._scope, key,
      () => __storage_get(this._storage._scope, key,
                          __storedSentinel),
    ));
  }
  put(key, value) {
    this._storage._assertTransactionActive("kv.put");
    key = String(key);
    try {
      __storage_put(this._storage._scope, key, value);
    } catch (error) {
      __storage_put_serialized(this._storage._scope, key,
                               __storedBytes(value, error));
    }
  }
  delete(key) {
    this._storage._assertTransactionActive("kv.delete");
    return __storage_delete(this._storage._scope, String(key));
  }
  list(options = {}) {
    this._storage._assertTransactionActive("kv.list");
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
    this.sql = new SqlStorage(scope, state, this);
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
  _transactionStatus() {
    let status = null;
    for (let control = this._transactionControl; control; control = control.parent) {
      if (control.rolledBack) return "rolled back";
      if (control.committed) status = "committed";
    }
    return status;
  }
  _assertTransactionActive(operation) {
    const status = this._transactionStatus();
    if (status) throw new Error("Cannot " + operation + "() on " + status + " transaction");
  }
  _flushPendingPuts() {
    // An admitted put can resume after its boundary flushed and ended the
    // transaction. Its promise completes without flushing a later scope.
    if (this._transactionStatus()) return false;
    if (this._state && this._state._aborted) return false;
    __storage_flush_pending_puts(this._scope);
    return true;
  }
  async get(k) {
    this._assertTransactionActive("get");
    // Transaction operations reach SQLite before their caller can roll back.
    if (!this._transactionControl) await Promise.resolve();
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
    this._assertTransactionActive("put");
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
    this._assertTransactionActive("delete");
    if (!this._transactionControl) await Promise.resolve();
    if (!this._flushPendingPuts()) return;
    if (Array.isArray(k)) {
      if (k.length === 0) return 0;
      return __storage_delete_many(this._scope, k);
    }
    return __storage_delete(this._scope, k);
  }
  async list(options = {}) {
    this._assertTransactionActive("list");
    if (!this._transactionControl) await Promise.resolve();
    if (!this._flushPendingPuts()) return new Map();
    return __unwrapStoredMap(__storage_list(
      this._scope, JSON.stringify(options), __storedSentinel));
  }
  async deleteAll() {
    this._assertTransactionActive("deleteAll");
    if (!this._transactionControl) await Promise.resolve();
    if (!this._flushPendingPuts()) return;
    __storage_delete_all(this._scope, __cell.deleteAllDeletesAlarm);
  }
  // Resolves once every write this cell committed before the call is
  // proven durable, by the proof the output gate requires before it releases
  // an egress. Workerd resolves when the pending writes have completed; on
  // celld the local file is a cache, so "completed" is "proven". The host op
  // rejects when that proof fails or times out, so an application never
  // treats a lost write as safe.
  async sync() {
    this._assertTransactionActive("sync");
    await Promise.resolve();
    const status = this._transactionStatus();
    // An admitted sync can resume after transactionSync commits or rolls
    // back. Its boundary settled the queued puts, so prove the surviving
    // state without flushing a later transaction through this handle. A
    // new call after that boundary still fails the entry guard above.
    if (this._state?._aborted || (status === null && !this._flushPendingPuts()))
      throw new Error(
        "storage.sync: " + this._scope + " was aborted; the object is no " +
        "longer running");
    await __storage_sync(this._scope);
  }
  async setAlarm(t) {
    this._assertTransactionActive("setAlarm");
    if (!this._transactionControl) await Promise.resolve();
    if (!this._flushPendingPuts()) return;
    if (this._state?._facetDepth > 0)
      throw new Error("Facets currently cannot set alarms.");
    __alarm_set(this._scope, t instanceof Date ? t.getTime() : Number(t));
  }
  async getAlarm() {
    this._assertTransactionActive("getAlarm");
    if (!this._transactionControl) await Promise.resolve();
    if (!this._flushPendingPuts()) return null;
    const v = __alarm_get(this._scope);
    return v === null ? null : v;
  }
  async deleteAlarm() {
    this._assertTransactionActive("deleteAlarm");
    if (!this._transactionControl) await Promise.resolve();
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
    if (control.rolledBack) return;
    this._assertTransactionActive("rollback");
    control.rollback();
    control.rolledBack = true;
  }
  transactionSync(f) {
    this._assertTransactionActive("transactionSync");
    const savepoint = this._transactionStart();
    const control = this._newTransactionControl(savepoint);
    try {
      const value = f(this._transactionView(control));
      if (!control.rolledBack) {
        this._transactionCommit(savepoint);
        control.committed = true;
      }
      return value;
    } catch (error) {
      if (!control.rolledBack) {
        try {
          this._transactionRollback(savepoint);
          control.rolledBack = true;
        } catch (rollbackError) {
          this._abortAfterFailedRollback(rollbackError, error);
        }
      }
      throw error;
    }
  }
  _newTransactionControl(savepoint) {
    return {
      rolledBack: false,
      committed: false,
      parent: this._transactionControl,
      rollback: () => this._transactionRollback(savepoint, true),
    };
  }
  async _runTransactionWith(f, savepoint, control) {
    try {
      const value = await f(this._transactionView(control));
      if (!control.rolledBack) {
        this._transactionCommit(savepoint);
        control.committed = true;
      }
      return value;
    } catch (error) {
      if (!control.rolledBack) {
        // The flag records a rollback that happened, not one that was
        // attempted: a rollback that fails leaves the savepoint open, and a
        // later transaction on the same connection would commit its writes.
        // Nothing can be said about the connection then, so the object is
        // aborted rather than left to serve from it.
        try {
          this._transactionRollback(savepoint);
          control.rolledBack = true;
        } catch (rollbackError) {
          this._abortAfterFailedRollback(rollbackError, error);
        }
      }
      throw error;
    }
  }
  // `cause` is the failure that led to the rollback; the abort's error keeps
  // it, so a debugger can walk back to the callback's own throw. The
  // rollback failure is reported even when the object is already aborted,
  // as it is after a block's limit: the abort then changes nothing, and the
  // log line is the only trace the failure leaves.
  _abortAfterFailedRollback(rollbackError, cause) {
    // Inside a catch, so the log must not become the throw that leaves it:
    // the slot release after the caller's rethrow depends on that.
    try { console.error("transaction rollback failed", rollbackError); } catch {}
    const state = this._transactionRoot._state;
    if (state && !state._aborted) {
      state.abort(
        new Error(
          "a transaction rollback failed: " + __describeFailure(rollbackError),
          { cause },
        ),
      );
    }
  }
  async _runTransaction(f) {
    const savepoint = this._transactionStart();
    return this._runTransactionWith(f, savepoint, this._newTransactionControl(savepoint));
  }
  async transaction(f) {
    this._assertTransactionActive("transaction");
    if (this._transactionDepth > 0) return this._runTransaction(f);
    // Workerd runs the callback under blockConcurrencyWhile: the input gate
    // shuts, so no other event starts in the object while the transaction is
    // open. Without it another event's reads saw uncommitted rows, its
    // writes joined the open transaction on the same connection, and a
    // rollback discarded a write celld had already acknowledged (#714). The
    // callback's failure is caught inside the block: a failed transaction
    // rolls back and rejects, and must not reset the object the way a failed
    // critical section does.
    //
    // The tail orders transactions one event starts concurrently, which the
    // gate does not separate. Its slot is taken here, in call order: taken
    // inside the block it would be taken in the order the gate woke the
    // callers, which is not theirs to keep. It is released inside the block,
    // when the callback has settled, and not when the block has: a second
    // transaction the same event starts while the first runs rides the
    // outer hold as a nested block, and the outer block waits for nested
    // ones before it releases, so a slot held until the block's end would
    // wait on itself.
    const root = this._transactionRoot;
    const previous = root._transactionTail;
    let release;
    root._transactionTail = new Promise((resolve) => { release = resolve; });
    let control = null;
    let started = false;
    let abandoned = null;
    const guarded = async () => {
      started = true;
      try {
        await previous;
        // The block ended while this waited for its slot, or the object was
        // reset under it by an enclosing block that failed: the caller has
        // its rejection and the gate is open again, so starting the
        // transaction now would run it beside whatever the object serves.
        if (abandoned !== null) return { error: abandoned };
        if (root._state._aborted) {
          return { error: new Error("the Durable Object was reset before the transaction started") };
        }
        // The start is a host op that can fail like any other statement, and
        // a failure there is the transaction's to report, not the block's:
        // a throw that escaped here would reset the object.
        try {
          const savepoint = this._transactionStart();
          control = this._newTransactionControl(savepoint);
          return { value: await this._runTransactionWith(f, savepoint, control) };
        } catch (error) {
          return { error };
        }
      } finally {
        release();
      }
    };
    let outcome;
    try {
      outcome = await root._state.blockConcurrencyWhile(guarded);
    } catch (error) {
      // The block itself failed: its limit ended the callback while it still
      // ran, or the block ahead of it failed and this one never started. A
      // transaction the callback still holds would otherwise stay open on
      // the connection the object's replacement uses, and the callback's
      // late commit would carry the replacement's writes with it. Roll it
      // back now, unless the callback's continuation committed in the same
      // tick the limit won; the callback's own commit is skipped from here
      // on. The slot stays with the callback until it settles: the callback
      // still runs on the connection, and a transaction started beside it
      // would interleave with its remaining statements. A block that failed
      // before the callback started holds nothing, so its slot is released
      // here or nothing ever would.
      abandoned = error;
      if (control && !control.rolledBack && !control.committed) {
        try {
          control.rollback();
          control.rolledBack = true;
        } catch (rollbackError) {
          this._abortAfterFailedRollback(rollbackError, error);
        }
      }
      if (!started) release();
      throw error;
    }
    if ("error" in outcome) throw outcome.error;
    return outcome.value;
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
// RPC stub op. `events` records every block that has asked for the
// gate, so no window exists in which a block has started and the
// isolate still thinks the cell is open. `holder` names the block
// actually running; another event can still be queued.
//
// A stub minted while a block runs carries that block's token and
// re-enters it instead of queueing behind it. That is Workerd's
// `IoContext::makeReentryCallback`, and without it a callback the
// block itself sent out could never come back.
const __cellBlocks = new Map();
const __blockEnter = (scope, event, owner) => {
  let settleReady;
  let rejectReady;
  const ready = new Promise((resolve, reject) => {
    settleReady = resolve;
    rejectReady = reject;
  });
  const record = {
    owner,
    reaction: String(__io_context_id()),
    actorEvent: __currentActorEvent(),
    retired: false,
    ready,
    settleReady,
    rejectReady,
  };
  const block = __cellBlocks.get(scope);
  if (block !== undefined) {
    block.events.set(event, record);
    return { block, record };
  }
  const fresh = {
    holder: null,
    events: new Map([[event, record]]),
  };
  __cellBlocks.set(scope, fresh);
  return { block: fresh, record };
};
const __blockLeave = (scope, block, event) => {
  if (!block.events.delete(event)) return;
  if (block.holder === event) block.holder = null;
  if (block.events.size === 0 && __cellBlocks.get(scope) === block)
    __cellBlocks.delete(scope);
};
const __durableClassMeta = new WeakMap();
let __nextFacetOwner = 1;
const __makeDurableObjectClass = (idPromise, name, options = {}) => {
  const value = {};
  __durableClassMeta.set(value, {
    idPromise,
    name: name === null || name === undefined ? "default" : String(name),
    props: options?.props,
  });
  return value;
};
class DurableObjectFacets {
  constructor(state) {
    this._state = state;
    this._owner = String(__nextFacetOwner++);
    this._next = 1;
    this._running = new Map();
    this._barriers = new Map();
  }
  _validName(name) {
    name = String(name);
    if (new TextEncoder().encode(name).length > 256)
      throw new TypeError("Facet name is too long (max 256 characters).");
    return name;
  }
  get(name, getStartupOptions) {
    name = this._validName(name);
    if (this._state._facetDepth >= 3)
      throw new Error(
        "Facet nesting depth limit exceeded. The maximum depth including the root Durable Object is 4.");
    if (typeof getStartupOptions !== "function")
      throw new TypeError("The facet startup callback must be a function.");
    let record = this._running.get(name);
    if (record !== undefined) return record.stub;
    // Each run gets a distinct host scope. An aborted request can still have
    // a pending continuation, so reusing its scope would let that continuation
    // enter the replacement instance after the abort barrier opens.
    record = {
      aborted: false,
      error: undefined,
      owner: this._owner + ":" + this._next++,
      started: null,
      stub: null,
    };
    const barrier = this._barriers.get(name);
    const start = () => record.started ??= Promise.resolve(barrier)
      .then(getStartupOptions)
      .then(async (options) => {
        const meta = __durableClassMeta.get(options?.class);
        if (meta === undefined)
          throw new TypeError(
            "FacetStartupOptions.class must be a DurableObjectClass.");
        const loader = await meta.idPromise;
        const id = options.id instanceof DurableObjectId
          ? options.id.toString()
          : options.id === undefined ? this._state.id.toString() : String(options.id);
        return [loader, meta.name, id, meta.props];
      });
    const invoke = async (operation) => {
      if (record.aborted) throw record.error;
      return operation(await start());
    };
    const target = {};
    // Methods share the loaded-worker RPC transport. A facet stub is not
    // awaitable, and a property is a single callable pipeline node.
    const session = {
      get: () => Promise.reject(new Error(
        "Awaitable properties on facets are not supported yet.")),
      call: (path, args) => invoke(async ([loader, className, id, props]) => {
        if (path.length !== 1)
          throw new Error(
            "Pipelined property paths on facets are not supported yet.");
        return __rpcDes(await __facet_rpc(
          loader, className, this._state._scope, record.owner, name, id,
          JSON.stringify(props ?? null), path[0], __rpcOut(args, false)));
      }),
    };
    // Arrow closures retain the manager because `target.fetch`'s method
    // receiver is the target object, not the DurableObjectFacets instance.
    const manager = this;
    target.fetch = async function(input, init) {
      return invoke(async ([loader, className, id, props]) => {
        const req = new Request(input, init);
        const headers = JSON.stringify(req.headers.__celldHeaderList);
        const { body, streamId } = await __subrequestBody(req);
        const response = JSON.parse(await __facet_fetch(
          loader, className, manager._state._scope, record.owner, name, id,
          JSON.stringify(props ?? null), req.url, req.method, body, headers, streamId));
        const responseBody = response.streamId !== undefined
          ? new CelldHttpBodyStream(response.streamId)
          : response.body !== undefined ? response.body
          : Uint8Array.from(response.bodyBytes || []);
        return __wrapServiceResponse(new Response(responseBody, {
          status: response.status,
          headers: response.headers,
        }), req.url);
      });
    };
    record.stub = new Proxy(target, {
      get(base, prop) {
        if (prop === "then") return undefined;
        if (Reflect.has(base, prop)) return Reflect.get(base, prop);
        if (typeof prop !== "string") return undefined;
        return __makeNode(session, [prop], null);
      },
    });
    this._running.set(name, record);
    return record.stub;
  }
  _abort(name, reason) {
    name = this._validName(name);
    const record = this._running.get(name);
    if (record === undefined) return Promise.resolve();
    record.aborted = true;
    record.error = reason;
    this._running.delete(name);
    const barrier = record.started === null ? Promise.resolve() : record.started
      .then(([loader, className]) =>
        __facet_abort(
          loader, className, this._state._scope, record.owner, name), () => {});
    this._barriers.set(name, barrier);
    const clear = () => {
      if (this._barriers.get(name) === barrier) this._barriers.delete(name);
    };
    barrier.then(clear, clear);
    return barrier;
  }
  abort(name, reason) {
    this._abort(name, reason);
  }
  delete(name) {
    name = this._validName(name);
    const barrier = this._abort(name, new Error("Facet was deleted.")).then(() =>
      __facet_delete(this._state._scope, name));
    this._barriers.set(name, barrier);
    const clear = () => {
      if (this._barriers.get(name) === barrier) this._barriers.delete(name);
    };
    // Keep a failed delete as the name's barrier. Clearing it would let the
    // next get reload the image which the failed operation did not delete.
    barrier.then(clear, () => {});
  }
  _release() {
    for (const name of Array.from(this._running.keys()))
      this._abort(name, new Error("The parent Durable Object was released."));
  }
}
// The reason a failed critical section gives its waiters. It must not throw:
// it runs where a throw would skip the gate's release, and a value with no
// string form, such as `Object.create(null)`, is a legal thing to throw.
function __describeFailure(error) {
  try {
    return String((error && error.message) || error);
  } catch {
    return "critical section failed";
  }
}
class DurableObjectState {
  constructor(scope) {
    this._scope = scope;
    this._aborted = false;
    this.storage = new DurableObjectStorage(scope, this);
    this.facets = new DurableObjectFacets(this);
    this._gate = Promise.resolve();
    this._blockDepth = 0;
    const facet = __cell.facetConfigs[scope];
    this._facetDepth = facet?.depth ?? 0;
    const separator = scope.indexOf(":");
    const className = separator < 0 ? scope : scope.slice(0, separator);
    const value = facet?.id ?? (separator < 0 ? scope : scope.slice(separator + 1));
    this.id = new DurableObjectId(
      className, value, __cell.idNames[scope],
    );
    this.props = facet?.props;
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
    if (this._blockDepth > 0) {
      // A nested block rides the outer hold. One the outer body starts and
      // does not await would outlive that hold, and a `transaction()`
      // started that way is the shape that mattered: the outer block
      // returned, the gate opened, and another event wrote into the still
      // open transaction (#714). The outer block drains these before it
      // releases, so the hold lasts as long as any block under it. A nested
      // block that fails resets the object as any block does, and the outer
      // release must carry that failure, or the waiters would be admitted to
      // an object that no longer exists: the drain records it.
      const nested = this._runConcurrencyBlock(f);
      (this._nestedBlocks ||= []).push(nested.then(() => null, __describeFailure));
      return nested;
    }
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
    const [eventText, owner, acquired] = __gate_acquire(this._scope);
    const event = Number(eventText);
    const { block, record } = __blockEnter(this._scope, event, owner);
    const next = (async () => {
      try {
        await acquired;
        // A shutdown can retire the promise reaction while another event
        // owns this queued acquisition. The owner still has to consume its
        // ticket, but the retired callback must not run.
        if (record.retired) {
          __gate_release(this._scope, event, owner, null);
          return;
        }
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
          failure = __describeFailure(error);
          throw error;
        } finally {
          // Nested blocks are drained only after a body that returned: a
          // body that failed has reset the object, its waiters are refused
          // whatever the nested blocks do, and waiting for them would only
          // delay the refusal and a transaction's rollback behind it. Each
          // nested block has its own limit, so a drain is bounded by the
          // last one started, as workerd's nested critical sections are.
          while (failure === null && this._nestedBlocks && this._nestedBlocks.length > 0) {
            for (const nestedFailure of await Promise.all(this._nestedBlocks.splice(0))) {
              if (nestedFailure !== null && failure === null) failure = nestedFailure;
            }
          }
          this._nestedBlocks = undefined;
          __gate_release(this._scope, event, owner, failure);
        }
      } finally {
        // Also on a rejected `acquired`: the block ahead failed, this one
        // never ran, and the cell must not stay shut on its behalf.
        __blockLeave(this._scope, block, event);
      }
    })();
    // `_ready()` awaits this. The host gate stops *other* events, but a
    // block taken in the constructor runs inside the very event that is
    // being delivered, so nothing external can hold that event back — the
    // promise does. Concurrent blocks within one event go through
    // `_blockDepth` above and never reach here.
    this._gate = Promise.all([this._gate, record.ready]).then(() => undefined);
    // `_ready()` observes the original promise. This extra handler prevents
    // a constructor-time rejection from becoming unhandled before an event
    // reaches `_ready()`.
    this._gate.catch(() => {});
    next.then(record.settleReady, record.rejectReady);
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
    if (this._aborted) return;
    this._aborted = true;
    __storage_cancel_pending_puts(this._scope);
    const instance = __cell.instances[this._scope];
    if (instance && instance.__celldState === this)
      delete __cell.instances[this._scope];
    // Under a stub-mediated caller (this scope is not the current
    // event) the failure breaks the actor, as Workerd joins it
    // into the on-abort promise; a direct event keeps reset-only
    // semantics — its caller sees the rejection itself.
    if (__currentActorScope() !== this._scope)
      __actorBreak(this._scope, error);
  }
  abort(reason) {
    if (this._aborted) return;
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
    if (__currentActorScope() === this._scope) {
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
  getTags(ws) {
    if (!(ws instanceof WebSocket) || !ws._hibernatable) {
      throw new Error(
        "you must call 'acceptWebSocket()' before attempting to access " +
        "the tags of a WebSocket.");
    }
    return Array.from(ws._tags);
  }
  acceptWebSocket(ws, tags = []) {
    if (__heap_over_admission_share())
      throw new Error(
        "the isolate is near its V8 heap limit, so it refused a " +
        "WebSocket (see CELLD_V8_HEAP_LIMIT_MB)");
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
const __currentActorEvent = () => {
  const context = String(__io_context_id());
  if (context === "") return undefined;
  for (let index = __actorEventStack.length - 1; index >= 0; index--) {
    const event = __actorEventStack[index];
    if (event.context === context) return event;
  }
  return undefined;
};
const __currentActorScope = () => __currentActorEvent()?.scope ?? "";
const __beginActorEvent = (scope) => {
  const event = { scope, context: String(__io_context_id()) };
  __actorEventStack.push(event);
  return event;
};
const __endActorEvent = (event) => {
  const index = __actorEventStack.lastIndexOf(event);
  if (index >= 0) __actorEventStack.splice(index, 1);
};
const __endActorEventsForContext = (context) => {
  for (let index = __actorEventStack.length - 1; index >= 0; index--) {
    if (__actorEventStack[index].context === context)
      __actorEventStack.splice(index, 1);
  }
};
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
globalThis.__retireInputGateContext = (context) => {
  for (const [scope, block] of __cellBlocks) {
    const retired = [...block.events].filter(([, record]) =>
      record.owner === context || record.reaction === context
    );
    if (retired.length === 0) continue;
    const holder = retired.find(([event]) => block.holder === event);
    if (holder === undefined) {
      // A queued callback has not changed the actor. Remove only that
      // callback, and let an unrelated holder continue with the same state.
      for (const [event, record] of retired) {
        record.retired = true;
        record.settleReady();
        block.events.delete(event);
        if (record.actorEvent !== undefined)
          __endActorEvent(record.actorEvent);
      }
      if (block.events.size === 0 && __cellBlocks.get(scope) === block)
        __cellBlocks.delete(scope);
      continue;
    }
    // A running callback changed this actor and cannot reach its own
    // `finally`. Reset the actor, and release the host gate when the context
    // that started the callback differs from the turn that owns its work.
    const [holderEvent, holderRecord] = holder;
    __gate_release(
      scope,
      holderEvent,
      holderRecord.owner,
      "the cell's critical section ended without releasing",
    );
    const instance = __cell.instances[scope];
    if (instance !== undefined) instance.__celldState._aborted = true;
    __storage_cancel_pending_puts(scope);
    if (instance !== undefined && __cell.instances[scope] === instance)
      delete __cell.instances[scope];
    for (const record of block.events.values()) {
      record.retired = true;
      record.settleReady();
      if (record.actorEvent !== undefined)
        __endActorEvent(record.actorEvent);
    }
    // The old actor cannot run any block `finally`. A later event creates an
    // instance whose ready promise contains none of this actor's work.
    if (__cellBlocks.get(scope) === block) __cellBlocks.delete(scope);
  }
  __endActorEventsForContext(context);
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
const __ERROR_RESPONSE_MESSAGE =
  "Return value from serve handler must not be an error " +
  "response (like Response.error())";
const __wrapServiceResponse = (res, url) => {
  // Response.error() represents a Fetch network error, not an HTTP response.
  // Letting status 0 cross this seam turns it into an ordinary response and
  // makes callers continue after the target explicitly reported failure.
  if (res.type === "error" || res.status === 0) {
    throw new TypeError(__ERROR_RESPONSE_MESSAGE);
  }
  // A 101 that crossed isolates carries a target rather than a socket: the
  // pair's other end is in the isolate that answered, and the two cannot be
  // linked the way the loopback below links them. Give the caller a client
  // end the host can join to that target, on the same terms as a Durable
  // Object subrequest -- nothing binds until `accept()`, so a response
  // passed straight back out keeps the direct route to the real client.
  const bound = !res.webSocket && res._wsTarget && res.status === 101
    ? new WebSocket(undefined, [], res._wsTarget)
    : undefined;
  const wrapped = new Response(
    // Buffered bodies re-stream (an empty buffer still yields a
    // stream, as over real HTTP); a streaming body passes through.
    res._bodyBytes !== null ? res._bodyBytes : res.body,
    {
      status: res.status,
      statusText:
        res.statusText || __STATUS_TEXT[res.status] || "",
      headers: res.headers,
      webSocket: res.webSocket || bound,
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
        // The verbatim header list; see the note on the outbound `fetch`.
        // The loaded worker rebuilds a `Headers` from these pairs, so a
        // repeat and its casing survive the isolate boundary.
        const headers = JSON.stringify(req.headers.__celldHeaderList);
        const { body, streamId } = await __subrequestBody(req);
        const r = JSON.parse(
          await __loader_fetch(id, req.url, req.method, body, headers, streamId));
        const responseBody = r.streamId !== undefined
          ? new CelldHttpBodyStream(r.streamId)
          : r.body !== undefined ? r.body : Uint8Array.from(r.bodyBytes || []);
        return __wrapServiceResponse(
          new Response(responseBody, { status: r.status, headers: r.headers }),
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
      getDurableObjectClass(name = null, options = {}) {
        return __makeDurableObjectClass(idPromise, name, options);
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
    // The verbatim header list; see the note on the outbound `fetch`. The
    // target isolate rebuilds a `Headers` from these pairs, so a repeat and
    // its casing survive the crossing.
    const headers = JSON.stringify(req.headers.__celldHeaderList);
    // Cross-isolate calls carry exact bytes. A host-backed stream transfers
    // its one source to the target, and another stream uses the byte fallback.
    const { body, streamId } = await __subrequestBody(req);
    const r = JSON.parse(await (signal
      ? __awaitCancellableDoCall(
        __svc_call_cancellable(
          script, req.url, req.method, body, headers, streamId),
        signal)
      : __svc_call(
        script, req.url, req.method, body, headers, streamId)));
    const responseBody = r.streamId !== undefined
      ? new CelldHttpBodyStream(r.streamId)
      : r.body !== undefined
        ? r.body
        : Uint8Array.from(r.bodyBytes || []);
    return __wrapServiceResponse(
      new Response(responseBody, {
        status: r.status, headers: r.headers, __wsTarget: r.wsTarget,
      }),
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
// Stubs cross same-isolate transports only (same-script entrypoint
// RPC and same-process routed dispatch); a marker revived elsewhere fails
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
  const scope = __currentActorScope() || undefined;
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
// Everything this isolate holds for one cell scope that belongs to the
// cell's residency rather than to the isolate, so a fresh instance
// inherits none of it. `adopt_cell` calls this on both edges: the
// give-back frees it, the take-in guarantees that no epoch spans.
// Storage is not here — `stop_cell` closes it host-side — and sockets
// live in the host registry, so a hibernated cell keeps them.
const __cellRelease = (scope) => {
  __cell.instances[scope]?.ctx?.facets?._release();
  delete __cell.instances[scope];
  delete __cell.idNames[scope];
  delete __cell.facetConfigs[scope];
  __brokenActors.delete(scope);
  // An abandoned block can never run `__blockLeave`. A normal eviction
  // cannot abandon a block because its event keeps the cell in flight. A
  // stop that waits for nobody can, so the release drops every block record
  // together with the other state for this residency.
  __cellBlocks.delete(scope);
  // An entry holds `target`, which can be an object the released
  // instance owns. Dropping it ends that reachability; `released` is
  // what an already-revived handle reads, since it holds its entry
  // directly and never looks in this map again.
  for (const entry of __stubEntries.values()) {
    if (entry.scope !== scope) continue;
    entry.released = true;
    __stubEntries.delete(entry.id);
    // Break in-flight ops as an abort would, but not through
    // `__actorBreak`: that records the scope broken for the life of the
    // isolate, which is the state this function drops.
    __ctxAbort(entry.ctx, new __ActorAbort(__releasedStubError()));
  }
};
const __releasedStubError = () => new Error(
  "The Durable Object that returned this RPC stub no longer runs on " +
  "this node.");
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
// Bind through the intrinsic because a callable proxy can implement `.bind`
// as a remote property. Reading that property can turn a valid named RPC
// method into a non-function. Keep this rule shared by every RPC binding site.
const __rpcBindMethod = (value, receiver) =>
  Reflect.apply(Function.prototype.bind, value, [receiver]);
const __stubResolve = (target, prop) => {
  if (target instanceof __cf.RpcTarget) {
    if (Object.hasOwn(target, prop) || !(prop in target) ||
        prop in Object.prototype) throw __rpcNoSuchMethod(prop);
  } else if (!Object.hasOwn(target, prop)) {
    throw __rpcNoSuchMethod(prop);
  }
  const value = target[prop];
  // Keep celld's own stubs and pipeline nodes unwrapped because the wrapper
  // would lose their metadata.
  return typeof value === "function" && !__stubMeta.has(value) &&
      !(value instanceof __cf.RpcPromise) &&
      !(value instanceof __cf.RpcProperty)
    ? __rpcBindMethod(value, target) : value;
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
                 e: v.type === "error",
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
      out["__celld$disp"] = __newEntry(__rpcBindMethod(disposer, v)).id;
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
  if (marker.e) res.type = "error";
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
    ? __rpcBindMethod(value, inst) : value;
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
  // The cell that minted this stub left residency, so `entry.target`
  // belongs to an instance this node released. The stub fails here
  // rather than calling into it.
  if (entry.released) return Promise.reject(__releasedStubError());
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
        // A body already backed by a host stream forwards to the cell as its
        // stream id, so a proxied upload to a cell on this node costs a chunk
        // rather than the whole body (#156). The host keeps it a stream for a
        // local cell and collects it only to sign a cross-node frame. A held
        // body -- or a stream with no host id -- crosses as its bytes.
        const bodyStreamId =
          req._bodyBytes === null && req.body && req.body.__celldStreamId !== undefined
            ? req.body.__celldStreamId
            : undefined;
        const body_ = bodyStreamId !== undefined
          ? new Uint8Array()
          : req._bodyBytes === null
            ? await req._consume()
            : req._bodyBytes;
        if (bodyStreamId !== undefined) {
          // The cell reads this upload through the same host stream id, so the
          // Worker's wrapper must not also read it: lock it (a late read throws
          // instead of racing the cell over one host stream) and mark it used,
          // which is what awaiting the old _consume() did and what workerd
          // reports as request.bodyUsed after the forward.
          req.bodyUsed = true;
          req.body.getReader();
        }
        // Forward the request's headers to the cell as JSON. When they were
        // never materialized -- a Worker that only routes to a cell reads
        // none -- pass the raw header string straight through, so the whole
        // dispatch never builds a Headers object on either side.
        //
        // Both branches now carry the same shape. The raw string is the
        // wire list the host handed in, so the materialized branch must send
        // the header list too, not the iterator; see the note on the
        // outbound `fetch`. Reading `.headers` on a forwarded request used
        // to change what the cell received.
        const headersJson = req._headersJson !== undefined
          ? req._headersJson
          : JSON.stringify(req.headers.__celldHeaderList);
        const r = JSON.parse(await invoke(() => {
          if (!signal) return __do_call(
            scope, dispatchName ?? null, req.url, req.method, body_,
            headersJson, bodyStreamId,
          );
          return __awaitCancellableDoCall(
            __do_call_cancellable(
              scope, dispatchName ?? null, req.url, req.method, body_,
              headersJson, bodyStreamId,
            ),
            signal,
          );
        }));
        const body = r.streamId !== undefined
          ? new CelldHttpBodyStream(r.streamId)
          : r.body !== undefined
            ? r.body
            : Uint8Array.from(r.bodyBytes || []);
        // An upgrade the cell answered with. The pair straddles two isolates,
        // so the client end cannot be the peer object itself -- it is a new
        // socket the host joins to the cell's on `accept()`. A caller that
        // returns this response instead of accepting keeps the direct route
        // `__wsTarget` already describes, and binds nothing.
        return new Response(body, {
          status: r.status,
          headers: r.headers,
          __wsTarget: r.wsTarget,
          webSocket: r.status === 101 && r.wsTarget
            ? new WebSocket(undefined, [], r.wsTarget)
            : undefined,
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
      // Every cell RPC goes out through the host, whichever node owns the
      // target. Same-process dispatch re-enters this isolate, where the
      // abort and exit markers revive; bytes that land elsewhere revive as
      // loud foreign stubs.
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
// A large body, or a body of unknown length, arrives as a stream id and
// not as bytes. The handler then pulls the body off the socket as it
// reads, so the whole body is never resident. `request.body` is a stream
// in both cases.
globalThis.__makeIncomingRequest = (
  url, method, body, headersJson, streamId,
) => __makeRequest(
  url, method,
  streamId === undefined ? body : new CelldHttpBodyStream(streamId),
  headersJson, undefined, true);
globalThis.__dispatchTo = async (
  scope, url, method, body, headersJson, requestId = null, bodyStreamId = null,
) => {
  // A routed body that must not be collected crosses as a host stream id; the
  // handler here reads it in parts. A held body arrives as its bytes.
  if (bodyStreamId !== null) body = new CelldHttpBodyStream(bodyStreamId);
  // Request already allocates a default controller for an absent signal.
  // Retain that same allocation so a streamed response can report reader
  // cancellation after the handler has returned.
  const requestController = new AbortController();
  if (requestId !== null)
    __incomingRequestSignals.set(
      String(requestId), requestController.signal);
  const actorEvent = __beginActorEvent(scope);
  try {
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
    // The caller's abandonment reaches the target through
    // __do_call_cancel/__abortIncomingRequest, which aborts the fresh incoming
    // signal made above; the host dispatcher passes no caller signal here.
    const response = await dispatch;
    // Not wrapped: the host holds this response and gates it, and the caller
    // that would cancel the body reaches it through the routed channel's own
    // controller rather than this one.
    __attachResponseRequestCancellation(response, requestController, false);
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
    return response;
  } finally {
    if (requestId !== null)
      __incomingRequestSignals.delete(String(requestId));
    __endActorEvent(actorEvent);
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
// isolate (same-process routed dispatch re-enters it) and fail loudly
// on use anywhere else. Callee exceptions cross in
// the error envelope on every flavor.
globalThis.__dispatchRpc = async (scope, method, args) => {
  const actorEvent = __beginActorEvent(scope);
  try {
    // A string is the legacy JSON flavor; bytes are V8 structured clone.
    // Answer in kind.
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
    __endActorEvent(actorEvent);
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
// Invoke a handler inside the event frame that the host already opened. A
// Queue dispatch uses this form because its host driver must receive and keep
// driving the handler's waitUntil aggregate after the settlement is returned.
const __dispatchEntrypointMethodInCurrentEvent = async (name, method, arg) => {
  const handler = __cell.objectEntrypoints[name];
  if (handler !== undefined) {
    if (typeof handler[method] !== "function")
      throw new TypeError(
        `Entrypoint ${JSON.stringify(name)} has no ${method} handler`);
    return await __ctxRun(undefined,
      () => handler[method](arg, __cell.env, __entrypointContext()));
  }
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
// A queue batch owns mutable settlement state only while its handler runs.
// Keeping a batch alive after dispatch must not keep authority to settle its
// lease, because that lease can have expired and been handed out again.
globalThis.__dispatchEntrypointQueue = async (name, incoming) => {
  let active = true;
  let ackAll = false;
  const explicitAcks = new Set();
  const retries = new Map();
  const retryBatch = { retry: false, delaySeconds: undefined };
  const checkActive = () => {
    if (!active)
      throw new Error("Queue event methods cannot be called after the handler returns.");
  };
  const retryDelay = (options) => {
    const value = options?.delaySeconds;
    if (value === undefined) return undefined;
    if (!Number.isInteger(value) || value < -0x8000_0000 || value > 0x7fff_ffff)
      throw new TypeError("delaySeconds must be a 32-bit integer");
    return value;
  };
  const decode = (message) => {
    switch (message.contentType) {
      case "text": return new TextDecoder().decode(message.body);
      case "bytes": return message.body;
      case "json": return JSON.parse(new TextDecoder().decode(message.body));
      case "v8": return __sc_decode(message.body);
      default: throw new TypeError(`Unknown queue content type: ${message.contentType}`);
    }
  };
  const messages = incoming.messages.map((message) => ({
    id: message.id,
    timestamp: new Date(message.timestamp),
    body: decode(message),
    attempts: message.attempts,
    ack() {
      checkActive();
      if (ackAll || retryBatch.retry || retries.has(message.id)) return;
      explicitAcks.add(message.id);
    },
    retry(options) {
      checkActive();
      if (ackAll || explicitAcks.has(message.id)) return;
      const delaySeconds = retryDelay(options);
      if (delaySeconds !== undefined || !retries.has(message.id))
        retries.set(message.id, delaySeconds);
    },
  }));
  const oldest = incoming.metrics.oldestMessageTimestamp;
  const batch = {
    queue: incoming.queue,
    messages,
    metadata: {
      metrics: {
        backlogCount: incoming.metrics.backlogCount,
        backlogBytes: incoming.metrics.backlogBytes,
        oldestMessageTimestamp: oldest === undefined ? undefined : new Date(oldest),
      },
    },
    ackAll() {
      checkActive();
      if (!retryBatch.retry) ackAll = true;
    },
    retryAll(options) {
      checkActive();
      if (ackAll) return;
      retryBatch.retry = true;
      const delaySeconds = retryDelay(options);
      if (delaySeconds !== undefined) retryBatch.delaySeconds = delaySeconds;
    },
  };
  let outcome = "ok";
  let errorText;
  try {
    await __dispatchEntrypointMethodInCurrentEvent(name, "queue", batch);
  } catch (error) {
    // A handler exception retries the batch but keeps explicit acks that the
    // handler made before it threw. The caller needs both facts together.
    outcome = "exception";
    try {
      errorText = String(error);
    } catch {
      errorText = "queue handler rejected";
    }
  } finally {
    active = false;
  }
  return {
    outcome,
    error: errorText,
    ackAll,
    retryBatch,
    explicitAcks: [...explicitAcks],
    retryMessages: [...retries].map(([msgId, delaySeconds]) => ({
      msgId,
      delaySeconds,
    })),
  };
};
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
  const actorEvent = __beginActorEvent(scope);
  try {
    const inst = await _readyInstance(scope);
    const socket = inst.__celldState._socket(wsId);
    if (socket.readyState !== WebSocket.READY_STATE_CONNECTING) return;
    socket.protocol = protocol;
    socket.readyState = WebSocket.READY_STATE_OPEN;
    socket.dispatchEvent(new Event("open"));
  } finally {
    __endActorEvent(actorEvent);
  }
};
globalThis.__wsMessage = async (scope, wsId, msg) => {
  const actorEvent = __beginActorEvent(scope);
  try {
    const inst = await _readyInstance(scope);
    const socket = inst.__celldState._socket(wsId);
    if (!socket._hibernatable && typeof socket._dispatchMessage === "function")
      socket._dispatchMessage(msg);
    else if (typeof inst.webSocketMessage === "function")
      await inst.webSocketMessage(socket, msg);
  } finally {
    __endActorEvent(actorEvent);
  }
};
globalThis.__wsBinary = async (scope, wsId, data) => {
  const actorEvent = __beginActorEvent(scope);
  try {
    const inst = await _readyInstance(scope);
    const socket = inst.__celldState._socket(wsId);
    const bytes = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
    if (!socket._hibernatable && typeof socket._dispatchMessage === "function")
      socket._dispatchMessage(bytes);
    else if (typeof inst.webSocketMessage === "function")
      await inst.webSocketMessage(socket, bytes);
  } finally {
    __endActorEvent(actorEvent);
  }
};
globalThis.__wsClosed = async (scope, wsId, code, reason, wasClean) => {
  const actorEvent = __beginActorEvent(scope);
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
    __endActorEvent(actorEvent);
  }
};
// called by celld's scheduler when an alarm is due. Returns a promise.
globalThis.__fireAlarm = async (scope, scheduledTime, retryCount) => {
  const actorEvent = __beginActorEvent(scope);
  try {
    const inst = await _readyInstance(scope);
    if (typeof inst.alarm !== "function") return;
    await inst.alarm({ scheduledTime, retryCount, isRetry: retryCount > 0 });
  } finally {
    __endActorEvent(actorEvent);
  }
};
// The reserved cron cell. It is not a user class: celld registers it under
// `.cron` for every deployment that declares `triggers.crons`, and one cell
// per script carries that script's whole schedule. Ownership CAS on that one
// name is what makes a cron trigger fire once per fleet rather than once per
// node -- the same arbiter an alarm already relies on, with nothing added.
//
// The schedule itself is never stored here. `__cell.crons` comes from the
// deployment the cell is running under, so changing a cron expression takes
// effect on the next activation and needs no migration of an armed alarm.
class CelldCronSchedule {
  constructor(state) {
    this._state = state;
    // Failures within one occurrence. Held in memory on purpose: losing the
    // count to an eviction only makes the next retry sooner, and every retry
    // is capped at the next occurrence anyway, so it need not be durable.
    this._retry = 0;
    // What a pending retry owes: the occurrence, the expressions of it that
    // failed, and the deadline the retry was armed for. A retry deadline is a
    // backoff instant and not an occurrence, so nothing about it can be
    // recovered from the deadline itself -- without this record the wake
    // matches no expression, runs nothing, and drops the failed tick in
    // silence. Held in memory beside `_retry` and for the same reason: an
    // eviction costs the retry, never the next occurrence.
    this._owed = null;
  }
  // celld calls this once per node after a deployment loads, because a cron
  // cell has no client to wake it. Arming is idempotent -- the same schedule
  // and the same clock give the same answer -- so it does not matter which
  // node wins the cell.
  async fetch() {
    // A pending retry is a deadline this cell already owes, and re-arming
    // would move it to the next occurrence and lose the tick it owes with it.
    if (this._owed !== null) return new Response(null, { status: 204 });
    const armed = await this._state.storage.getAlarm();
    // An alarm already due is a tick that is late, not one that is wrong.
    // Leaving it alone is what makes a fleet that was down fire the missed
    // occurrence once, instead of skipping it by re-arming into the future.
    if (armed === null || armed > Date.now()) await this._arm(null, [], false);
    return new Response(null, { status: 204 });
  }
  async alarm(info) {
    const crons = __cell.crons || [];
    // A retry wake carries the backoff instant as its deadline, so what to run
    // and what time to report both come from the record the failure left. Only
    // a deadline that is not a retry is an occurrence to match against.
    const owed = this._owed !== null && this._owed.armedFor === info.scheduledTime
      ? this._owed
      : null;
    const occurrence = owed === null ? info.scheduledTime : owed.at;
    // One deadline can belong to several expressions, and each is its own
    // invocation with its own controller.cron, as on Cloudflare. A retry
    // repeats only the expressions that failed, because the ones that
    // succeeded already ran for this occurrence.
    const due = owed === null
      ? __cron_plan(crons, info.scheduledTime, Date.now(), 0, false).matching
      : owed.indices;
    const failed = [];
    for (const index of due) {
      let noRetry = false;
      const controller = {
        // The occurrence, never the instant this attempt started, so a late
        // run and a retry both report the minute they were scheduled for.
        scheduledTime: occurrence,
        cron: crons[index],
        noRetry() { noRetry = true; },
      };
      const ctx = __beginEvent();
      try {
        if (typeof __cell.selfScheduled !== "function")
          throw new Error(
            "a cron trigger needs the Worker to export a `scheduled` handler");
        await __cell.selfScheduled(controller, __cell.env, ctx);
      } catch (error) {
        // Swallowed on purpose: this handler owns the re-arm below, so
        // throwing would hand the deadline to the generic alarm retry and
        // lose the next occurrence with it.
        console.error(
          `scheduled handler for cron ${JSON.stringify(crons[index])} failed:`,
          error);
        if (!noRetry) failed.push(index);
      } finally {
        __endEvent();
      }
    }
    await this._arm(occurrence, failed, owed !== null);
  }
  // `occurrence` is the tick just handled, or null when the cell is only
  // arming, `failed` holds the expressions of it still owed, and `retried`
  // says the deadline just handled was a backoff rather than an occurrence.
  async _arm(occurrence, failed, retried) {
    const crons = __cell.crons || [];
    // The count is failures within one occurrence, so a fresh occurrence
    // starts at one however the last one ended. Carrying it across would
    // spend `alarm_retry`'s ceiling once and leave every later occurrence
    // with no retry at all, which is a budget for the schedule and not for
    // the tick.
    this._retry = failed.length ? (retried ? this._retry + 1 : 1) : 0;
    // A negative first argument means "arming only, nothing fired".
    const plan = __cron_plan(crons, -1, Date.now(), this._retry, failed.length > 0);
    // The next occurrence beats the backoff whenever it is sooner, and it then
    // cancels the retry rather than queueing behind it, so the record the
    // retry needed goes when the retry does.
    this._owed = plan.armIsRetry
      ? { at: occurrence, indices: failed, armedFor: plan.armAt }
      : null;
    // No expression matches again and nothing is owed: the deployment dropped
    // its crons, so the cell retires rather than waking forever.
    if (plan.armAt === null) await this._state.storage.deleteAlarm();
    else await this._state.storage.setAlarm(plan.armAt);
  }
}
globalThis.__cell = {
  entrypoints: {},
  objectEntrypoints: {},
  doExports: {},
  classes: { ".cron": CelldCronSchedule },
  crons: [],
  workflows: {},
  instances: {},
  facetConfigs: {},
  env: {},
  idNames: {},
  namespaceKeys: {},
  node: "",
  deleteAllDeletesAlarm: false,
  compat: { jsRpc: false, fetcherGetPutDelete: false, queueJsonMessages: false },
  makeNamespace,
  release: __cellRelease,
};
// ---- KV ------------------------------------------------------------------
// A KV namespace is a cell of a runtime-supplied Durable Object class, so it
// inherits ownership, fencing, LTX replication and durable acknowledgement
// from the cell it already is. This half is the server; `__makeKvNamespace`
// below is the client the binding hands to a Worker.
//
// Reads here are strongly consistent and the published contract is upstream's:
// a value can be up to 60 seconds old. That gap is deliberate. A node-local
// read cache is the obvious next optimisation and it would make reads
// genuinely stale, and a freshness promise made now could not be withdrawn
// then. Implement strong, promise weak.
//
// Storage is ordinary cell SQL rather than the KV surface, because a namespace
// is read by ordered prefix scan and `list` is the operation that decides the
// schema. `celld_logic::kv` owns every decision about a key, a deadline or a
// limit; nothing here re-derives one.

// Run one bounded reclamation transaction. A cell supplies only its ordered
// candidate query and its per-row transition, so KV expiry and Queue retention
// share the turn bound and the transaction shape without pretending their SQL
// state machines are the same.
const __cellSweepBatch = (storage, select, reclaim, limit) => {
  let processed = 0;
  storage.transactionSync(() => {
    const rows = select(limit);
    if (rows.length > limit) throw new Error("a cell sweep exceeded its row bound");
    for (const row of rows) {
      reclaim(row);
      processed += 1;
    }
  });
  return processed;
};

const __KV_TABLE = "__kv";
const __KV_META_TABLE = "__kv_meta";

// Upstream's four content types. The wire form is what `get` returns for
// `type: "text"`; the binding converts from there, so the cell stores bytes
// and a tag and never a parsed value.
const __kvError = (message) => new Error("KV_ERROR: " + String(message));

// One op for the whole large-value path, following `__d1_run`. Bytes cross as
// a typed view in both directions. Encoding a 25 MiB value as a JSON number
// array creates millions of heap objects and can crash an otherwise valid put.
const __kvBlob = async (request, value) => {
  const reply = await __kv_blob(JSON.stringify(request), value);
  return reply instanceof Uint8Array
    ? { found: true, value: reply }
    : JSON.parse(reply);
};

// The authenticated operator route still uses JSON for its control envelope.
// A byte array is acceptable for an inline value, but a 25 MiB array creates
// 25 million JavaScript numbers and exhausts the isolate. The raw base64 ops
// accept typed views for this internal wire form; the public atob() and btoa()
// wrappers keep their standard string-only behaviour.
const __kvOperatorValue = (value) => {
  const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
  return bytes.byteLength > __kvLimits().maxInlineValueBytes
    ? { value: $$btoa(bytes), valueEncoding: "base64" }
    : { value: [...bytes] };
};

// Content addressing, so the same value written twice in one ownership epoch
// stores once and a retry after an uncertain failure costs nothing. A later
// epoch deliberately uses a different object. SHA-256 makes an accidental
// collision infeasible. The host adds the active cell scope and epoch to the
// object key, because one activation cannot prove another activation's live
// set and therefore cannot safely collect from a shared digest prefix.
const __kvDigest = async (bytes) => {
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  let out = "";
  for (const byte of hash) out += byte.toString(16).padStart(2, "0");
  return out;
};

class __KvNamespaceCell {
  constructor(ctx) {
    this.ctx = ctx;
    this._ready = false;
    // Blob references written to the bucket whose row has not committed yet. A sweep
    // must spare these, or it collects bytes a put is about to reference.
    this._pending = new Set();
    // Blob writes and the mark-and-sweep protocol share one per-cell queue.
    // The pending set is live state, not a snapshot that stays valid across an
    // await, so collection cannot overlap a put from its announcement through
    // its row commit. Other cells and ordinary reads stay independent.
    this._blobProtocolTail = Promise.resolve();
  }

  async _withBlobProtocol(operation) {
    const previous = this._blobProtocolTail;
    let release;
    this._blobProtocolTail = new Promise((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }

  // Schedule collection before a blob can leave the process or a row can drop
  // its reference. `setAlarm()` writes cell state, and the following bucket
  // egress waits on that write's output gate. A crash can therefore leave an
  // orphan only together with the durable wake that will collect it.
  async _armBlobSweep() {
    this._open().exec(
      `INSERT INTO ${__KV_META_TABLE} (name, value) VALUES ('blob-sweep-due', 1)
       ON CONFLICT(name) DO UPDATE SET value = excluded.value`,
    );
    const now = Date.now();
    const deadline = now + __kvLimits().blobSweepMs;
    const armed = await this.ctx.storage.getAlarm();
    // During an alarm event, `getAlarm()` can still report the timestamp that
    // caused the current delivery. It is not a future wake and cannot cover a
    // reference removed by an event that interleaves with this one.
    if (armed === null || armed <= now || armed > deadline) {
      await this.ctx.storage.setAlarm(deadline);
    }
  }

  _blobSweepDue() {
    return this._open().exec(
      `SELECT 1 AS due FROM ${__KV_META_TABLE}
        WHERE name = 'blob-sweep-due' AND value = 1`,
    ).toArray().length > 0;
  }

  _clearBlobSweepDue() {
    this._open().exec(
      `DELETE FROM ${__KV_META_TABLE} WHERE name = 'blob-sweep-due'`,
    );
  }

  // Created on first touch rather than at construction, so a namespace that is
  // only ever read costs no write, and an eviction that drops the isolate does
  // not need the table rebuilt before the cell can answer.
  _open() {
    if (this._ready) return this.ctx.storage.sql;
    const sql = this.ctx.storage.sql;
    // Recreate the first release's table before the normal open path. The
    // migration test then reaches the same `CREATE IF NOT EXISTS` boundary as
    // an upgraded cell and starts with a row it must preserve.
    if (__kvLimits().legacySchema) {
      sql.exec(
        `CREATE TABLE IF NOT EXISTS ${__KV_TABLE} (
           name TEXT PRIMARY KEY,
           value BLOB,
           tag TEXT NOT NULL,
           metadata TEXT,
           expires_at INTEGER
         ) WITHOUT ROWID`,
      );
      sql.exec(
        `INSERT OR IGNORE INTO ${__KV_TABLE}
           (name, value, tag, metadata, expires_at)
         VALUES ('before-upgrade', CAST('legacy' AS BLOB), 'text', NULL, NULL)`,
      );
    }
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${__KV_TABLE} (
         name TEXT PRIMARY KEY,
         value BLOB,
         blob_id TEXT,
         size INTEGER NOT NULL,
         tag TEXT NOT NULL,
         metadata TEXT,
         expires_at INTEGER
       ) WITHOUT ROWID`,
    );
    // Inline KV shipped before `blob_id` and `size`. `CREATE IF NOT EXISTS`
    // does not change that table, so migrate each column independently. A
    // crash between the ALTER statements is safe because the next open reads
    // the surviving shape and resumes at the missing column.
    const columns = new Set(
      sql.exec(`PRAGMA table_info(${__KV_TABLE})`).toArray().map((row) => row.name),
    );
    if (!columns.has("blob_id")) {
      sql.exec(`ALTER TABLE ${__KV_TABLE} ADD COLUMN blob_id TEXT`);
    }
    if (!columns.has("size")) {
      sql.exec(
        `ALTER TABLE ${__KV_TABLE}
           ADD COLUMN size INTEGER NOT NULL DEFAULT 0`,
      );
      sql.exec(`UPDATE ${__KV_TABLE} SET size = LENGTH(value)`);
    }
    // `list` walks the primary key in order, so the only index worth carrying
    // is the sweeper's. Without it the sweep is a full scan of a namespace
    // whose whole point is being large.
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__KV_TABLE}_expires
         ON ${__KV_TABLE} (expires_at) WHERE expires_at IS NOT NULL`,
    );
    // The due bit is durable because the isolate that wrote a blob can die
    // before its row commit. In-memory state cannot schedule that orphan's
    // collector on the next owner.
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${__KV_META_TABLE} (
         name TEXT PRIMARY KEY,
         value INTEGER NOT NULL
       ) WITHOUT ROWID`,
    );
    this._ready = true;
    return sql;
  }

  // A key past its deadline is invisible from the instant it expires, not from
  // whenever the sweeper next runs. The read path filters and the sweep only
  // reclaims space, so the two can never disagree about which side of the
  // boundary a key is on.
  async __kvGet({ keys, withMetadata, withExpiration = false, now }) {
    const sql = this._open();
    const out = [];
    for (const key of keys) {
      const row = sql.exec(
        `SELECT value, blob_id, tag, metadata, expires_at FROM ${__KV_TABLE}
          WHERE name = ? AND (expires_at IS NULL OR expires_at > ?)`,
        key,
        now,
      ).toArray()[0];
      if (row === undefined) {
        out.push({ key, found: false });
        continue;
      }
      let value = row.value;
      if (row.blob_id !== null && row.blob_id !== undefined) {
        const blob = await __kvBlob({ mode: "get", reference: row.blob_id });
        // A row naming a blob the bucket does not hold is the failure the
        // commit order exists to prevent, so it is reported as itself rather
        // than as a missing key: an absent key and a broken one need different
        // answers from an operator.
        if (!blob.found) {
          throw __kvError(
            `the value for ${JSON.stringify(key)} is missing from the fleet ` +
              `bucket (blob ${row.blob_id})`,
          );
        }
        value = new Uint8Array(blob.value);
      }
      const entry = { key, found: true, value, tag: row.tag };
      if (withMetadata) entry.metadata = row.metadata ?? null;
      // These fields describe one row snapshot. The operator bulk export must
      // not combine a value from this read with an expiration from an earlier
      // listing when a concurrent put replaces the key between both calls.
      if (withExpiration) entry.expiration = row.expires_at ?? null;
      out.push(entry);
    }
    return out;
  }

  // One transaction for the whole batch, so a bulk put is all-or-nothing
  // rather than a prefix of itself. Upstream gives no such guarantee, and
  // giving a stronger one here costs nothing and cannot surprise a caller.
  // Blob first, row second. An orphan blob is bytes the collector reclaims; a
  // row naming a blob that was never written is a read that fails forever, and
  // no sweep repairs it. The opposite commit order can leave that dangling
  // row when a process stops between the two writes.
  //
  // The blob reference travels in `_pending` until the row commits, so a sweep that
  // runs mid-put does not collect bytes that are about to be referenced. That
  // costs the put and not the read -- the commit requires the blob, so a
  // collected blob means the commit cannot fire -- which is why the model's
  // tooth for it is an action property and not an invariant.
  async _store(value) {
    if (value.byteLength <= __kvLimits().maxInlineValueBytes) {
      return { value, blobId: null };
    }
    const digest = await __kvDigest(value);
    await this._armBlobSweep();
    // The host mints the reference from the epoch installed with this cell.
    // JavaScript never guesses that authority or recovers it after an await.
    const { reference } = await __kvBlob({ mode: "prepare", digest });
    this._pending.add(reference);
    try {
      await __kvBlob({ mode: "put", reference }, value);
      // Force the collector race at its real seam. The alarm took its mark
      // snapshot before this put started and waits briefly after the put
      // announces itself. Without blob-protocol serialization, this write
      // lands during that wait and must stay here until the stale sweep runs.
      if (
        __kvLimits().raceSweepPut && this._testSweepMarked &&
        !this._testSweepFinished
      ) {
        await new Promise((resolve) => {
          this._testResumePut = resolve;
        });
      }
      // The crash window the commit order exists for: the blob is in the
      // bucket and the row is not written yet. The failed operation no longer
      // protects the digest in memory, so the durable wake can reclaim it even
      // when this isolate remains resident.
      if (__kvLimits().failAfterBlobWrite) {
        throw __kvError("CELLD_TEST_KV_FAIL_AFTER_BLOB_WRITE");
      }
      return { value: null, blobId: reference };
    } catch (error) {
      this._pending.delete(reference);
      throw error;
    }
  }

  async __kvPut({ entries }) {
    if (
      __kvLimits().raceSweepPut &&
      entries.some((entry) =>
        entry.value.byteLength > __kvLimits().maxInlineValueBytes
      )
    ) {
      if (!this._testSweepMarked || this._testPutAttempted === undefined) {
        throw __kvError("the test blob sweep did not reach its mark snapshot");
      }
      this._testPutAttempted();
      this._testPutAttempted = undefined;
    }
    return this._withBlobProtocol(() => this._putLocked(entries));
  }

  async _putLocked(entries) {
    const sql = this._open();
    // An inline replacement can remove the final row reference to an old
    // blob, so arm before changing the row. A new large value also arms in
    // `_store`, before its own bucket write.
    const replacesBlob = entries.some((entry) =>
      sql.exec(
        `SELECT 1 AS found FROM ${__KV_TABLE}
          WHERE name = ? AND blob_id IS NOT NULL`,
        entry.key,
      ).toArray().length > 0
    );
    if (replacesBlob) await this._armBlobSweep();
    const stored = [];
    try {
      for (const entry of entries) {
        stored.push({ entry, ...(await this._store(entry.value)) });
      }
      this.ctx.storage.transactionSync(() => {
        for (const { entry, value, blobId } of stored) {
          sql.exec(
            `INSERT INTO ${__KV_TABLE}
               (name, value, blob_id, size, tag, metadata, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(name) DO UPDATE SET
               value = excluded.value,
               blob_id = excluded.blob_id,
               size = excluded.size,
               tag = excluded.tag,
               metadata = excluded.metadata,
               expires_at = excluded.expires_at`,
            entry.key,
            value,
            blobId,
            entry.value.byteLength,
            entry.tag,
            entry.metadata ?? null,
            entry.expiresAt ?? null,
          );
        }
      });
    } finally {
      // A committed row protects the blob durably. A failed put protects
      // nothing, so it must not pin an orphan until this isolate is evicted.
      for (const { blobId } of stored) {
        if (blobId !== null) this._pending.delete(blobId);
      }
    }
    // Only when something in this batch can expire. A namespace of permanent
    // keys arms no alarm and wakes for nothing.
    if (entries.some((entry) => entry.expiresAt !== null && entry.expiresAt !== undefined)) {
      await this._rearm();
    }
  }

  async __kvDelete({ keys }) {
    return this._withBlobProtocol(() => this._deleteLocked(keys));
  }

  async _deleteLocked(keys) {
    const sql = this._open();
    const removesBlob = keys.some((key) =>
      sql.exec(
        `SELECT 1 AS found FROM ${__KV_TABLE}
          WHERE name = ? AND blob_id IS NOT NULL`,
        key,
      ).toArray().length > 0
    );
    if (removesBlob) await this._armBlobSweep();
    this.ctx.storage.transactionSync(() => {
      for (const key of keys) {
        sql.exec(`DELETE FROM ${__KV_TABLE} WHERE name = ?`, key);
      }
    });
  }

  // Arm for the earliest deadline the table holds, or disarm when it holds
  // none. Asked from the row rather than held in memory, so the answer does
  // not depend on what this activation happens to remember -- an evicted cell
  // that wakes for an alarm re-derives the same deadline from the same rows.
  //
  // What carries the deadline across an eviction is the alarm itself, which is
  // durable cell state. This is *not* recomputed when a cell is merely opened,
  // so the one gap is a `_rearm` that throws after its put committed: the rows
  // are written, the caller sees an error, and nothing is armed until the next
  // put with a deadline. That costs space and never correctness, because the
  // read path filters an expired key whether or not it was reclaimed.
  async _rearm() {
    // A test that means to pin the read filter must be able to stop the sweep,
    // or it cannot tell which mechanism hid the key -- and a test that cannot
    // tell will pass when the filter is broken. This seam exists because that
    // is exactly what happened: the first version of the expiry test stayed
    // green with the filter removed.
    if (__kvLimits().sweepDisabled) return;
    const sql = this._open();
    const row = sql.exec(
      `SELECT MIN(expires_at) AS next FROM ${__KV_TABLE} WHERE expires_at IS NOT NULL`,
    ).toArray()[0];
    const next = row === undefined ? null : row.next;
    const blobDue = this._blobSweepDue();
    if ((next === null || next === undefined) && !blobDue) {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    const now = Date.now();
    const deadlines = [];
    // Never in the past: a deadline already due is swept on this wake, and
    // arming behind `now` would spin.
    if (next !== null && next !== undefined) {
      deadlines.push(Math.max(next, now + 1));
    }
    if (blobDue) deadlines.push(now + __kvLimits().blobSweepMs);
    const deadline = Math.min(...deadlines);
    const armed = await this.ctx.storage.getAlarm();
    if (armed === null || armed <= now || armed > deadline) {
      await this.ctx.storage.setAlarm(deadline);
    }
  }

  // Reclaiming space, never deciding visibility. A key is invisible from the
  // instant it expires because the read path filters, so a late sweep costs
  // storage and never correctness — which is what lets this be bounded and
  // re-armed rather than obliged to finish.
  // Every reference a row names, plus every reference a put has written and
  // not yet committed. The two go in one list because the bucket end cannot
  // tell them apart and does not need to: it only needs "do not delete these".
  _liveBlobs() {
    const rows = this._open().exec(
      `SELECT blob_id FROM ${__KV_TABLE} WHERE blob_id IS NOT NULL`,
    ).toArray();
    return [...new Set([...rows.map((row) => row.blob_id), ...this._pending])];
  }

  async alarm() {
    const removed = await this._withBlobProtocol(async () => {
      const now = Date.now();
      // If expiry will remove a blob reference, persist the due bit and a
      // future wake before deleting the row. A crash later in this alarm then
      // leaves the next owner enough durable state to finish collection.
      const expiresBlob = this._open().exec(
        `SELECT 1 AS found FROM ${__KV_TABLE}
          WHERE expires_at IS NOT NULL AND expires_at <= ?
            AND blob_id IS NOT NULL
          LIMIT ?`,
        now,
        __kvLimits().sweepBatchRows,
      ).toArray().length > 0;
      if (expiresBlob) await this._armBlobSweep();

      const { removed } = this.__kvSweep({
        now,
        limit: __kvLimits().sweepBatchRows,
      });
      // The blob sweep runs after the row sweep, on the same wake, so a row
      // reclaimed above has already dropped its reference by the time the live
      // set is read. A durable due bit distinguishes a GC wake from an inline
      // expiry wake and survives isolate eviction or a failed bucket request.
      if (this._blobSweepDue() || __kvLimits().raceSweepPut) {
        const live = this._liveBlobs();
        if (__kvLimits().raceSweepPut && !this._testSweepFinished) {
          const putAttempted = new Promise((resolve) => {
            this._testPutAttempted = resolve;
          });
          this._testSweepMarked = true;
          await putAttempted;
          // The fixture bucket is local. This bounded pause lets an
          // unprotected put finish its blob write; a protected put is waiting
          // for this protocol lock and therefore cannot enter the window.
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
        let swept = false;
        try {
          await __kvBlob({ mode: "sweep", live });
          swept = true;
        } catch (error) {
          // Reclaiming space is not deciding visibility, so a bucket that
          // refuses a delete costs storage and never correctness. The next
          // wake tries again.
          console.error("kv blob sweep failed:", error);
        } finally {
          if (__kvLimits().raceSweepPut) {
            this._testSweepFinished = true;
            this._testResumePut?.();
            this._testResumePut = undefined;
          }
        }
        if (swept) this._clearBlobSweepDue();
      }
      return removed;
    });
    // A full batch means there is more to reclaim, so come back promptly
    // rather than waiting for the next natural deadline.
    if (removed >= __kvLimits().sweepBatchRows) {
      await this.ctx.storage.setAlarm(Date.now() + 1);
      return;
    }
    await this._rearm();
  }

  // Pagination resumes at `name > after`, never at an offset. A caller holds
  // no transaction across pages, so an offset would skip or repeat keys the
  // moment a concurrent writer inserted or deleted inside the prefix.
  //
  // One row beyond the limit is read to decide `list_complete`, and discarded.
  // Asking the database is the only honest answer: a page that happens to fill
  // exactly is indistinguishable from a page that ended.
  __kvList({ prefix, limit, after, now }) {
    const sql = this._open();
    const pattern = `${prefix.replace(/([%_\\])/g, "\\$1")}%`;
    const rows = sql.exec(
      `SELECT name, metadata, expires_at FROM ${__KV_TABLE}
        WHERE name LIKE ? ESCAPE '\\'
          AND name > ?
          AND (expires_at IS NULL OR expires_at > ?)
        ORDER BY name
        LIMIT ?`,
      pattern,
      after ?? "",
      now,
      limit + 1,
    ).toArray();
    const complete = rows.length <= limit;
    const page = complete ? rows : rows.slice(0, limit);
    return {
      keys: page.map((row) => ({
        name: row.name,
        metadata: row.metadata ?? null,
        expiration: row.expires_at ?? null,
      })),
      complete,
    };
  }

  // The operator's live key and byte counts. `size` stays in the row because
  // a bucket-backed value has a NULL inline `value`; `LENGTH(value)` would
  // report zero bytes for exactly the values whose size matters most.
  __kvMetrics({ now }) {
    const sql = this._open();
    const row = sql.exec(
      `SELECT COUNT(*) AS count,
              COALESCE(SUM(size), 0) AS bytes
         FROM ${__KV_TABLE}
        WHERE expires_at IS NULL OR expires_at > ?`,
      now,
    ).toArray()[0];
    return { count: row.count, bytes: row.bytes };
  }

  // Reclaim what the read path already treats as gone. Bounded per call so a
  // namespace with a large expired population cannot hold the cell for an
  // unbounded time; the caller re-arms while there is more.
  __kvSweep({ now, limit }) {
    const sql = this._open();
    const removed = __cellSweepBatch(
      this.ctx.storage,
      (bound) => sql.exec(
        `SELECT name FROM ${__KV_TABLE}
          WHERE expires_at IS NOT NULL AND expires_at <= ?
          ORDER BY expires_at
          LIMIT ?`,
        now,
        bound,
      ).toArray(),
      (row) => {
        sql.exec(`DELETE FROM ${__KV_TABLE} WHERE name = ?`, row.name);
      },
      limit,
    );
    return { removed };
  }

  // The operator surface, reached only over `/runtime/`, which authenticates
  // with the fleet secret. `/do/` refuses every reserved class structurally,
  // so adding this handler does not put a namespace on an unauthenticated
  // route -- the trap d1.md decision 4 records paying for once, and the reason
  // that refusal is one question rather than one per class.
  //
  // Values cross this boundary as arrays of bytes rather than as text, because
  // a namespace holds bytes: a value written from a Worker as an ArrayBuffer
  // has no faithful string form, and `celld kv` must be able to read back
  // exactly what was written.
  async fetch(request) {
    let body;
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "KV_ERROR: invalid request body" }, { status: 400 });
    }
    const now = Date.now();
    try {
      let result;
      switch (body.op) {
        case "get": {
          // Awaited: `__kvGet` resolves a bucket-backed value, so it is
          // async. Destructuring the promise instead threw "object is not
          // iterable" on every operator read, and only here -- the binding
          // reaches the same method over RPC, which awaits for it.
          const [row] = await this.__kvGet({
            keys: [String(body.key)],
            withMetadata: true,
            withExpiration: true,
            now,
          });
          result = row.found
            ? {
              found: true,
              ...__kvOperatorValue(row.value),
              tag: row.tag,
              metadata: row.metadata ?? null,
              expiration: row.expiration,
            }
            : { found: false };
          break;
        }
        case "put":
        case "put-base64": {
          // Validated here, through the same helpers the binding calls, so a
          // key written by `celld kv` is a key the binding could have written.
          // The operator route used to arrive pre-validated by the CLI, which
          // put the same bound in two processes -- and they disagreed the first
          // time a test shortened one of them.
          const key = String(body.key);
          __kvCheckKey(key);
          const value = body.op === "put-base64"
            ? $$atob(String(body.value), true)
            : new Uint8Array(body.value);
          __kvCheckValue(value.byteLength);
          const metadata = body.metadata === undefined ? null : body.metadata;
          if (metadata !== null) __kvCheckMetadata(metadata);
          // Resolved from the cell's clock, not the caller's, because that is
          // the clock the read filter and the sweeper compare against.
          const expiresAt = __kvExpiryAt(now, body.expiration, body.expirationTtl);
          await this.__kvPut({
            entries: [{
              key,
              value,
              tag: String(body.tag ?? __KV_TAG_BYTES),
              metadata,
              expiresAt,
            }],
          });
          result = { ok: true };
          break;
        }
        case "delete":
          for (const key of body.keys) __kvCheckKey(String(key));
          await this.__kvDelete({ keys: body.keys.map(String) });
          result = { ok: true };
          break;
        case "list":
          result = this.__kvList({
            prefix: String(body.prefix ?? ""),
            limit: Number(body.limit),
            after: String(body.after ?? ""),
            now,
          });
          break;
        // What `celld kv info` reports. `stored` counts every row and `live`
        // counts only what a read would return, so the difference is exactly
        // the population the sweeper still owes -- which is the one number
        // that makes reclamation observable from outside the cell.
        case "info": {
          const live = this.__kvMetrics({ now });
          const total = this._open().exec(
            `SELECT COUNT(*) AS count FROM ${__KV_TABLE}`,
          ).toArray()[0];
          result = { live: live.count, bytes: live.bytes, stored: total.count };
          break;
        }
        default:
          return Response.json(
            { error: `KV_ERROR: unknown operation ${JSON.stringify(body.op)}` },
            { status: 400 },
          );
      }
      return Response.json({ result });
    } catch (error) {
      return Response.json(
        { error: String(error && error.message || error) },
        { status: 400 },
      );
    }
  }
}

__cell.classes.__KvNamespace = __KvNamespaceCell;
// RPC on a stub needs `extends DurableObject` or the js_rpc flag. This class is
// the runtime's own, so grant it here rather than making every KV user set a
// compatibility flag to reach a namespace.
__cell.doExports.__KvNamespace = true;

// The client half.
//
// Every number below is injected from `celld_logic::kv` as `__cell.kvLimits`
// rather than written here, so the binding, `celld kv` and deploy-time
// validation cannot disagree about what a valid key is. No new host op: D1 set
// the bar at one and Workflows came in under it, and a limit is data rather
// than a decision, so shipping the values is enough. What stays in JS is the
// presentation -- the error text is the binding's published contract, in
// upstream's `KV <OP> failed:` shape, and belongs beside the calls that raise
// it.
//
// The cursor codec is the one thing that must byte-match Rust's, and it is
// plain hex: a total function with a single right answer, which two correct
// implementations cannot disagree about. A policy would be a different matter
// and none is duplicated here.
const __kvLimits = () => __cell.kvLimits;

const __kvCheckKey = (key) => {
  if (key.length === 0) throw __kvError("a key must not be empty");
  const bytes = __kvEncoder.encode(key).byteLength;
  if (bytes > __kvLimits().maxKeyBytes) {
    throw __kvError(
      `a key is at most ${__kvLimits().maxKeyBytes} bytes, got ${bytes}`,
    );
  }
};

// A TTL becomes an absolute deadline here, once, at the call. Resolving it
// again later against a newer clock would extend the life of the key every
// time anything re-read the row -- the reason a workflow persists a sleep
// deadline and not its duration.
const __kvCheckValue = (size) => {
  // Upstream's bound, and now the only one. A value above the inline bound is
  // no longer refused: it goes to the fleet bucket, which is the split
  // Cloudflare's own KV rearchitecture made and for the same reason — a cell
  // replicates every write as LTX, so an inline value is paid for twice.
  if (size > __kvLimits().maxValueBytes) {
    throw __kvError(`a value is at most ${__kvLimits().maxValueBytes} bytes, got ${size}`);
  }
};

const __kvCheckMetadata = (metadata) => {
  const bytes = __kvEncoder.encode(metadata).byteLength;
  if (bytes > __kvLimits().maxMetadataBytes) {
    throw __kvError(
      `metadata is at most ${__kvLimits().maxMetadataBytes} bytes, got ${bytes}`,
    );
  }
};

const __kvExpiryAt = (now, expiration, expirationTtl) => {
  if (expirationTtl !== undefined && expirationTtl !== null) {
    const ms = Math.floor(Number(expirationTtl) * 1000);
    if (!Number.isFinite(ms) || ms < __kvLimits().minExpirationTtlMs) {
      throw __kvError(
        `expirationTtl is at least ${__kvLimits().minExpirationTtlMs / 1000} seconds`,
      );
    }
    return now + ms;
  }
  if (expiration !== undefined && expiration !== null) {
    const at = Math.floor(Number(expiration) * 1000);
    if (!Number.isFinite(at) || at <= now) {
      throw __kvError(`expiration ${expiration} is in the past`);
    }
    return at;
  }
  return null;
};

const __kvCursor = {
  encode(key) {
    let out = "";
    for (const byte of __kvEncoder.encode(key)) {
      out += byte.toString(16).padStart(2, "0");
    }
    return out;
  },
  decode(cursor) {
    // A malformed cursor is refused, never read as "start from the
    // beginning": that would hand a paginating caller its first page a second
    // time and call it progress.
    if (cursor.length % 2 !== 0 || /[^0-9a-f]/.test(cursor)) {
      throw __kvError("the list cursor is malformed");
    }
    const bytes = new Uint8Array(cursor.length / 2);
    for (let at = 0; at < bytes.length; at += 1) {
      bytes[at] = Number.parseInt(cursor.slice(at * 2, at * 2 + 2), 16);
    }
    return __kvDecoder.decode(bytes);
  },
};

const __kvListLimit = (requested) => {
  const max = __kvLimits().maxListLimit;
  if (requested === undefined || requested === null) return max;
  const limit = Math.floor(Number(requested));
  // Zero would be a page that never ends.
  if (!Number.isFinite(limit) || limit <= 0) return max;
  return Math.min(limit, max);
};

// Upstream's four content types, and what each one means on the way in and out.
const __KV_TAG_TEXT = "text";
const __KV_TAG_BYTES = "bytes";

const __kvEncoder = new TextEncoder();
const __kvDecoder = new TextDecoder();

// A value becomes bytes plus a tag, once, at the public boundary. Storing the
// tag is what lets `get(key, "text")` and `get(key)` answer differently from
// the same row without the cell parsing anything.
const __kvEncodeValue = (value) => {
  if (typeof value === "string") {
    return { value: __kvEncoder.encode(value), tag: __KV_TAG_TEXT };
  }
  if (value instanceof ArrayBuffer) {
    return { value: new Uint8Array(value.slice(0)), tag: __KV_TAG_BYTES };
  }
  if (ArrayBuffer.isView(value)) {
    // Copied, not referenced. `sendBatch`'s resizable-ArrayBuffer regression
    // upstream is the same hazard: a caller may resize or reuse the buffer
    // between this call and the write, and a shallow view would then read
    // decommitted pages.
    return {
      value: new Uint8Array(
        value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
      ),
      tag: __KV_TAG_BYTES,
    };
  }
  throw __kvError(
    "a KV value must be a string, an ArrayBuffer, or a typed array",
  );
};

const __kvDecodeValue = (bytes, tag, type) => {
  const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  switch (type) {
    case "arrayBuffer":
      return view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength);
    case "json":
      return JSON.parse(__kvDecoder.decode(view));
    case "stream":
      return new Response(view).body;
    case "text":
    default:
      // A value written as bytes still decodes as text when asked for text,
      // which is upstream's behaviour and the reason the tag is advisory
      // rather than a type check.
      return __kvDecoder.decode(view);
  }
};

const __kvReadOptions = (options) => {
  if (typeof options === "string") return { type: options };
  if (options === null || options === undefined) return { type: "text" };
  return { type: options.type ?? "text", cacheTtl: options.cacheTtl };
};

class KvNamespace {
  constructor(id, cellName) {
    Object.defineProperty(this, "_id", { value: id });
    Object.defineProperty(this, "_cellName", { value: cellName });
  }

  // Resolved per call rather than cached: a cell can move between calls, and
  // `getByName` costs what the Durable Object path already pays.
  get _stub() {
    return __cell.makeNamespace("__KvNamespace").getByName(this._cellName);
  }

  async _read(keys, options, withMetadata) {
    const { type } = __kvReadOptions(options);
    for (const key of keys) __kvCheckKey(key);
    const rows = await this._stub.__kvGet({
      keys,
      withMetadata,
      now: Date.now(),
    });
    return rows.map((row) => {
      if (!row.found) return { key: row.key, value: null, metadata: null };
      return {
        key: row.key,
        value: __kvDecodeValue(row.value, row.tag, type),
        metadata: row.metadata === null || row.metadata === undefined
          ? null
          : JSON.parse(row.metadata),
      };
    });
  }

  // `get(key)` answers a value; `get([keys])` answers a Map with a null hole
  // for a key that is not there. Upstream's bulk form is a Map and not an
  // object, which matters: an object would collide a key named `__proto__`
  // with the prototype chain.
  async get(key, options) {
    if (Array.isArray(key)) {
      const rows = await this._read(__kvBulkKeys(key), options, false);
      return new Map(rows.map((row) => [row.key, row.value]));
    }
    const [row] = await this._read([String(key)], options, false);
    return row.value;
  }

  async getWithMetadata(key, options) {
    if (Array.isArray(key)) {
      const rows = await this._read(__kvBulkKeys(key), options, true);
      return new Map(
        rows.map((row) => [row.key, { value: row.value, metadata: row.metadata }]),
      );
    }
    const [row] = await this._read([String(key)], options, true);
    return {
      value: row.value,
      metadata: row.metadata,
      // Null, and honestly so: celld has no read cache, and reporting a HIT
      // from a runtime that never cached anything would be a lie in a field
      // applications read.
      cacheStatus: null,
    };
  }

  async put(key, value, options) {
    const name = String(key);
    __kvCheckKey(name);
    const encoded = __kvEncodeValue(value);
    __kvCheckValue(encoded.value.byteLength);
    const metadata = options && options.metadata !== undefined
      ? JSON.stringify(options.metadata)
      : null;
    if (metadata !== null) __kvCheckMetadata(metadata);
    const expiresAt = __kvExpiryAt(
      Date.now(),
      options && options.expiration,
      options && options.expirationTtl,
    );
    await this._stub.__kvPut({
      entries: [{ key: name, value: encoded.value, tag: encoded.tag, metadata, expiresAt }],
    });
  }

  async delete(key) {
    const name = String(key);
    __kvCheckKey(name);
    await this._stub.__kvDelete({ keys: [name] });
  }

  // Upstream takes one key or an array, and caps the array at the same 100 a
  // bulk get takes.
  async deleteBulk(keys) {
    const names = Array.isArray(keys) ? __kvBulkKeys(keys) : [String(keys)];
    for (const name of names) __kvCheckKey(name);
    await this._stub.__kvDelete({ keys: names });
  }

  async list(options) {
    const prefix = options && options.prefix ? String(options.prefix) : "";
    const limit = __kvListLimit(options && options.limit);
    const after = options && options.cursor ? __kvCursor.decode(String(options.cursor)) : "";
    const page = await this._stub.__kvList({ prefix, limit, after, now: Date.now() });
    const keys = page.keys.map((entry) => {
      const key = { name: entry.name };
      if (entry.metadata !== null) key.metadata = JSON.parse(entry.metadata);
      // Upstream reports an expiration in seconds.
      if (entry.expiration !== null) key.expiration = Math.floor(entry.expiration / 1000);
      return key;
    });
    if (page.complete) return { keys, list_complete: true, cacheStatus: null };
    return {
      keys,
      list_complete: false,
      cursor: __kvCursor.encode(keys[keys.length - 1].name),
      cacheStatus: null,
    };
  }
}

// The bulk ceiling belongs to the binding, and a CLI chunks beneath it rather
// than inheriting it. An empty array is refused because upstream refuses it:
// answering an empty Map would look like "none of these keys exist".
const __kvBulkKeys = (keys) => {
  if (keys.length === 0) throw __kvError("a bulk get needs at least one key");
  if (keys.length > __kvLimits().maxBulkKeys) {
    throw __kvError(
      `a bulk get takes at most ${__kvLimits().maxBulkKeys} keys, got ${keys.length}`,
    );
  }
  return keys.map(String);
};

globalThis.__makeKvNamespace = (id, cellName) => new KvNamespace(id, cellName);

// ---- Queues --------------------------------------------------------------
// A Queue is one runtime-supplied Durable Object. The producer binding writes
// bytes and a content tag through RPC, and the cell owns the durable order,
// deadlines, leases, and alarms. `celld_logic::queue` owns every transition
// that can invalidate a lease or choose a deadline; this code supplies SQL and
// the Cloudflare-compatible presentation.

const __QUEUE_MESSAGES = "__queue_messages";
const __QUEUE_META = "__queue_meta";
const __QUEUE_STATS = "__queue_stats";
const __QUEUE_TRANSFER_RECEIPTS = "__queue_transfer_receipts";
const __queueEncoder = new TextEncoder();

// Independent send() calls share the same economics as sendBatch() without
// changing the public promise boundary. The first call opens a four-millisecond
// owner-local group by default; later cell events append their already-copied
// entries, and park on their own promise. One event commits the bounded group,
// and every response still trails that commit through the ordinary output
// gate. The three caps bound heap retention and keep the persisted transaction
// inside the public sendBatch limits.
const __QUEUE_PRODUCER_GROUP_CALLS = 64;
const __QUEUE_PRODUCER_GROUP_MESSAGES = 100;
const __QUEUE_PRODUCER_GROUP_BYTES = 256_000;

const __queueError = (message) => new Error("QUEUE_ERROR: " + String(message));
// A Queue message id is a UUIDv7 (RFC 9562): 48 bits of enqueue time, a
// 12-bit sequence inside that millisecond, and 62 random bits. It keeps the
// UUID shape a consumer can parse, and it sorts in enqueue order, so the
// UNIQUE index on `id` appends. A random v4 id made every grouped send
// transaction dirty one index leaf per message (about 1.6 WAL frames per
// 64-byte message) and turned every passive checkpoint into a random write
// across the whole backlog, which is what made a deep Queue's checkpoint
// cost grow with its backlog. The sequence never moves backwards inside one
// process, so a clock step only widens the gap between consecutive ids.
let __queueIdMs = 0;
let __queueIdSeq = 0;
function __queueMessageId(now) {
  if (now > __queueIdMs) {
    __queueIdMs = now;
    __queueIdSeq = 0;
  } else {
    __queueIdSeq += 1;
    if (__queueIdSeq > 0xfff) {
      __queueIdMs += 1;
      __queueIdSeq = 0;
    }
  }
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  const ms = __queueIdMs;
  bytes[0] = Math.floor(ms / 0x10000000000) & 0xff;
  bytes[1] = Math.floor(ms / 0x100000000) & 0xff;
  bytes[2] = (ms >>> 24) & 0xff;
  bytes[3] = (ms >>> 16) & 0xff;
  bytes[4] = (ms >>> 8) & 0xff;
  bytes[5] = ms & 0xff;
  bytes[6] = 0x70 | (__queueIdSeq >> 8);
  bytes[7] = __queueIdSeq & 0xff;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  let hex = "";
  for (let i = 0; i < 16; i++) hex += bytes[i].toString(16).padStart(2, "0");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${
    hex.slice(16, 20)
  }-${hex.slice(20)}`;
}
const __queueLimits = () => __cell.queueLimits;
const __queuePolicy = (request) =>
  JSON.parse(__queue_policy(JSON.stringify(request)));

const __queueDelay = (value, fallback = 0) => {
  if (value === undefined || value === null) return fallback;
  const seconds = Number(value);
  if (!Number.isInteger(seconds) || seconds < 0 ||
      seconds > __queueLimits().maxDelaySeconds) {
    throw __queueError(
      `delaySeconds must be an integer from 0 to ${__queueLimits().maxDelaySeconds}`,
    );
  }
  return seconds;
};

const __queueBytes = (value) => {
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value.slice(0));
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(
      value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
    );
  }
  throw __queueError("a bytes message must be an ArrayBuffer or a typed array");
};

// Encode before entering the cell. The copy makes `sendBatch()` immune to a
// caller resizing or reusing a backing buffer while the durable write awaits.
const __queueEncode = (body, requestedType) => {
  const contentType = requestedType ??
    (__cell.compat.queueJsonMessages ? "json" : "v8");
  let bytes;
  switch (contentType) {
    case "text":
      bytes = __queueEncoder.encode(String(body));
      break;
    case "bytes":
      bytes = __queueBytes(body);
      break;
    case "json": {
      let json;
      try {
        json = JSON.stringify(body);
      } catch (error) {
        throw __queueError("the JSON message body cannot be serialized", error);
      }
      if (json === undefined) {
        throw __queueError("the JSON message body cannot be undefined");
      }
      bytes = __queueEncoder.encode(json);
      break;
    }
    case "v8":
      try {
        bytes = __sc_encode(body);
      } catch (error) {
        throw __queueError("the message body cannot be cloned", error);
      }
      break;
    default:
      throw __queueError(
        `contentType must be "text", "bytes", "json", or "v8", got ${JSON.stringify(contentType)}`,
      );
  }
  if (bytes.byteLength > __queueLimits().maxMessageBytes) {
    throw __queueError(
      `a message is at most ${__queueLimits().maxMessageBytes} bytes, got ${bytes.byteLength}`,
    );
  }
  return { body: bytes, contentType, size: bytes.byteLength };
};

// A lease-scoped read must seek, not walk the backlog. The plan is the only
// evidence that says which, and it is cheap to ask for, so a gated test seam
// reads it from the shipped query rather than from a copy that can drift.
const __queueReportLeaseLookupPlan = (sql, query, leaseId) => {
  if (typeof __test_queue_lease_lookup_plan !== "function") return;
  const plan = sql.exec(`EXPLAIN QUERY PLAN ${query}`, leaseId)
    .toArray()
    .map((row) => row.detail)
    .join("; ");
  __test_queue_lease_lookup_plan(plan);
};

const __queueMetricsView = (metrics) => ({
  backlogCount: metrics.backlogCount,
  backlogBytes: metrics.backlogBytes,
  oldestMessageTimestamp: metrics.oldestMessageTimestamp === null ||
      metrics.oldestMessageTimestamp === undefined
    ? undefined
    : new Date(metrics.oldestMessageTimestamp),
});

class __QueueCell {
  constructor(ctx) {
    this.ctx = ctx;
    this.queue = ctx.id.name;
    this._ready = false;
    this._producerGroup = null;
  }

  _open() {
    if (this._ready) return this.ctx.storage.sql;
    const sql = this.ctx.storage.sql;
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${__QUEUE_MESSAGES} (
         seq INTEGER PRIMARY KEY AUTOINCREMENT,
         id TEXT NOT NULL UNIQUE,
         body BLOB NOT NULL,
         content_type TEXT NOT NULL,
         size INTEGER NOT NULL,
         enqueued_at INTEGER NOT NULL,
         visible_at INTEGER NOT NULL,
         attempts INTEGER NOT NULL DEFAULT 0,
         lease_id TEXT,
         lease_generation INTEGER NOT NULL DEFAULT 0,
         leased_until INTEGER,
         purge_on_settle INTEGER NOT NULL DEFAULT 0,
         dlq_target TEXT,
         dlq_transfer_id TEXT,
         transfer_kind TEXT,
         dead_letter_source TEXT,
         CHECK ((dlq_target IS NULL AND transfer_kind IS NULL)
             OR (dlq_target IS NOT NULL
                 AND transfer_kind IN ('dead-letter', 'redrive')))
       )`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_visible
         ON ${__QUEUE_MESSAGES} (visible_at, seq)`,
    );
    // Rearming is on every producer acknowledgement and settlement. Each
    // scheduling lookup must therefore stop at one deadline or one batch;
    // an aggregate over the backlog makes a large Queue quadratic to fill.
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_ready
         ON ${__QUEUE_MESSAGES} (visible_at, seq)
         WHERE purge_on_settle = 0 AND dlq_target IS NULL
           AND lease_id IS NULL`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_enqueued
         ON ${__QUEUE_MESSAGES} (enqueued_at, seq)`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_retained
         ON ${__QUEUE_MESSAGES} (enqueued_at, seq)
         WHERE purge_on_settle = 0 AND dlq_target IS NULL`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_lease
         ON ${__QUEUE_MESSAGES} (leased_until, seq)
         WHERE lease_id IS NOT NULL`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_moving
         ON ${__QUEUE_MESSAGES} (seq)
         WHERE dlq_target IS NOT NULL`,
    );
    // Dispatch and settlement both select a whole batch by its lease. Without
    // this index SQLite answers them by walking every row: the dispatch
    // readback scans `_visible` and the settlement scan walks the table, so
    // each delivered batch costs two passes over the backlog. The `_lease`
    // index cannot serve them because it is keyed on `leased_until`.
    // The partial predicate keeps only leased rows, which `max_concurrency`
    // times `max_batch_size` bounds, and an inserted message has a null
    // `lease_id`, so `send` pays nothing for it.
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_lease_batch
         ON ${__QUEUE_MESSAGES} (lease_id, visible_at, seq)
         WHERE lease_id IS NOT NULL`,
    );
    // The live-row index catches an impossible duplicate while a received
    // message remains queued. The receipt below is the durable deduplication
    // record after a consumer or purge removes that message.
    sql.exec(
      `CREATE UNIQUE INDEX IF NOT EXISTS ${__QUEUE_MESSAGES}_dlq_transfer
         ON ${__QUEUE_MESSAGES} (dlq_transfer_id)
         WHERE dlq_transfer_id IS NOT NULL AND dlq_target IS NULL`,
    );
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${__QUEUE_META} (
         name TEXT PRIMARY KEY,
         value TEXT NOT NULL
       ) WITHOUT ROWID`,
    );
    // The producer result and operator metrics are on hot paths. SQLite
    // triggers keep the three values in the same transaction as every row
    // transition, so a caller cannot observe a new message with old totals.
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${__QUEUE_STATS} (
         id INTEGER PRIMARY KEY CHECK (id = 1),
         stored_count INTEGER NOT NULL,
         backlog_count INTEGER NOT NULL,
         backlog_bytes INTEGER NOT NULL,
         oldest_message_timestamp INTEGER
       )`,
    );
    sql.exec(
      `INSERT INTO ${__QUEUE_STATS}
         (id, stored_count, backlog_count, backlog_bytes,
          oldest_message_timestamp)
       SELECT 1,
              COUNT(*),
              COALESCE(SUM(purge_on_settle = 0), 0),
              COALESCE(SUM(CASE WHEN purge_on_settle = 0 THEN size ELSE 0 END), 0),
              MIN(CASE WHEN purge_on_settle = 0 THEN enqueued_at END)
         FROM ${__QUEUE_MESSAGES}
        WHERE true
       ON CONFLICT(id) DO NOTHING`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stored_insert
       AFTER INSERT ON ${__QUEUE_MESSAGES}
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET stored_count = stored_count + 1
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stored_delete
       AFTER DELETE ON ${__QUEUE_MESSAGES}
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET stored_count = stored_count - 1
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stats_insert
       AFTER INSERT ON ${__QUEUE_MESSAGES}
       WHEN new.purge_on_settle = 0
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET backlog_count = backlog_count + 1,
                backlog_bytes = backlog_bytes + new.size,
                oldest_message_timestamp = CASE
                  WHEN oldest_message_timestamp IS NULL
                    OR new.enqueued_at < oldest_message_timestamp
                  THEN new.enqueued_at ELSE oldest_message_timestamp END
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stats_delete
       AFTER DELETE ON ${__QUEUE_MESSAGES}
       WHEN old.purge_on_settle = 0
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET backlog_count = backlog_count - 1,
                backlog_bytes = backlog_bytes - old.size,
                oldest_message_timestamp = CASE
                  WHEN backlog_count <= 1 THEN NULL
                  WHEN oldest_message_timestamp = old.enqueued_at
                  THEN (SELECT MIN(enqueued_at) FROM ${__QUEUE_MESSAGES}
                         WHERE purge_on_settle = 0)
                  ELSE oldest_message_timestamp END
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stats_exclude
       AFTER UPDATE OF purge_on_settle ON ${__QUEUE_MESSAGES}
       WHEN old.purge_on_settle = 0 AND new.purge_on_settle != 0
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET backlog_count = backlog_count - 1,
                backlog_bytes = backlog_bytes - old.size,
                oldest_message_timestamp = CASE
                  WHEN backlog_count <= 1 THEN NULL
                  WHEN oldest_message_timestamp = old.enqueued_at
                  THEN (SELECT MIN(enqueued_at) FROM ${__QUEUE_MESSAGES}
                         WHERE purge_on_settle = 0)
                  ELSE oldest_message_timestamp END
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TRIGGER IF NOT EXISTS ${__QUEUE_MESSAGES}_stats_include
       AFTER UPDATE OF purge_on_settle ON ${__QUEUE_MESSAGES}
       WHEN old.purge_on_settle != 0 AND new.purge_on_settle = 0
       BEGIN
         UPDATE ${__QUEUE_STATS}
            SET backlog_count = backlog_count + 1,
                backlog_bytes = backlog_bytes + new.size,
                oldest_message_timestamp = CASE
                  WHEN oldest_message_timestamp IS NULL
                    OR new.enqueued_at < oldest_message_timestamp
                  THEN new.enqueued_at ELSE oldest_message_timestamp END
          WHERE id = 1;
       END`,
    );
    sql.exec(
      `CREATE TABLE IF NOT EXISTS ${__QUEUE_TRANSFER_RECEIPTS} (
         transfer_id TEXT PRIMARY KEY,
         message_id TEXT NOT NULL,
         dead_letter_source TEXT,
         expires_at INTEGER NOT NULL
       ) WITHOUT ROWID`,
    );
    sql.exec(
      `CREATE INDEX IF NOT EXISTS ${__QUEUE_TRANSFER_RECEIPTS}_expires
         ON ${__QUEUE_TRANSFER_RECEIPTS} (expires_at, transfer_id)`,
    );
    this._ready = true;
    return sql;
  }

  _meta(name) {
    return this._open().exec(
      `SELECT value FROM ${__QUEUE_META} WHERE name = ?`, name,
    ).toArray()[0]?.value;
  }

  _setMeta(name, value) {
    if (value === null || value === undefined) {
      this._open().exec(`DELETE FROM ${__QUEUE_META} WHERE name = ?`, name);
      return;
    }
    this._open().exec(
      `INSERT INTO ${__QUEUE_META} (name, value) VALUES (?, ?)
       ON CONFLICT(name) DO UPDATE SET value = excluded.value`,
      name,
      String(value),
    );
  }

  _paused() {
    return this._meta("paused") === "1";
  }

  _metrics(now = Date.now(), waitingOnly = false) {
    const expiresBefore = now - __queueLimits().retentionMs;
    const sql = this._open();
    const stats = sql.exec(
      `SELECT backlog_count, backlog_bytes, oldest_message_timestamp
         FROM ${__QUEUE_STATS} WHERE id = 1`,
    ).toArray()[0];
    // Retention is a clock transition, not a row transition. Until the
    // bounded sweep catches up, use the exact query only at that boundary;
    // the normal hot path remains one materialized row at every depth.
    if (stats.oldest_message_timestamp !== null &&
        stats.oldest_message_timestamp <= expiresBefore) {
      const row = sql.exec(
        `SELECT COUNT(*) AS backlog_count,
                COALESCE(SUM(size), 0) AS backlog_bytes,
                MIN(enqueued_at) AS oldest_message_timestamp
           FROM ${__QUEUE_MESSAGES}
          WHERE purge_on_settle = 0 AND enqueued_at > ?
            AND (? = 0 OR (lease_id IS NULL AND dlq_target IS NULL
                           AND visible_at <= ?))`,
        expiresBefore,
        waitingOnly ? 1 : 0,
        now,
      ).toArray()[0];
      return {
        backlogCount: row.backlog_count,
        backlogBytes: row.backlog_bytes,
        oldestMessageTimestamp: row.oldest_message_timestamp ?? null,
      };
    }
    if (!waitingOnly) {
      if (typeof __test_queue_metrics_materialized === "function") {
        __test_queue_metrics_materialized();
      }
      return {
        backlogCount: stats.backlog_count,
        backlogBytes: stats.backlog_bytes,
        oldestMessageTimestamp: stats.oldest_message_timestamp ?? null,
      };
    }
    const blocked = sql.exec(
      `SELECT COALESCE(SUM(blocked_count), 0) AS blocked_count,
              COALESCE(SUM(blocked_bytes), 0) AS blocked_bytes
         FROM (
           SELECT COUNT(*) AS blocked_count,
                  COALESCE(SUM(size), 0) AS blocked_bytes
             FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_lease
            WHERE purge_on_settle = 0 AND lease_id IS NOT NULL
           UNION ALL
           SELECT COUNT(*) AS blocked_count,
                  COALESCE(SUM(size), 0) AS blocked_bytes
             FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_moving
            WHERE purge_on_settle = 0 AND dlq_target IS NOT NULL
           UNION ALL
           SELECT COUNT(*) AS blocked_count,
                  COALESCE(SUM(size), 0) AS blocked_bytes
             FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_ready
            WHERE purge_on_settle = 0 AND dlq_target IS NULL
              AND lease_id IS NULL AND visible_at > ?
         )`,
      now,
    ).toArray()[0];
    const unleasedOldest = sql.exec(
      `SELECT enqueued_at
         FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_retained
        WHERE purge_on_settle = 0 AND dlq_target IS NULL
          AND lease_id IS NULL AND visible_at <= ?
        ORDER BY enqueued_at, seq
        LIMIT 1`,
      now,
    ).toArray()[0]?.enqueued_at ?? null;
    return {
      backlogCount: stats.backlog_count - blocked.blocked_count,
      backlogBytes: stats.backlog_bytes - blocked.blocked_bytes,
      oldestMessageTimestamp: unleasedOldest,
    };
  }

  async _rearm(now = Date.now(), waitForWake = false) {
    const sql = this._open();
    const consumer = __cell.queueConsumers?.[this.queue];
    const paused = this._paused();
    const active = sql.exec(
      `SELECT COUNT(DISTINCT lease_id) AS count
         FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_lease
        WHERE lease_id IS NOT NULL AND leased_until > ?`,
      now,
    ).toArray()[0].count;
    const concurrency = consumer
      ? consumer.maxConcurrency ?? __queueLimits().maxConcurrency
      : 0;
    const hasCapacity = !paused && consumer !== undefined && __queuePolicy({
      op: "capacity",
      active,
      maximum: concurrency,
    });
    // We need only enough ready rows to distinguish a partial batch from a
    // full one. The two indexed arms cover a new row and an expired lease,
    // and the outer LIMIT keeps the work independent of backlog depth.
    let ready = [];
    if (hasCapacity) {
      const visibleReady = sql.exec(
        `SELECT visible_at AS ready_at, seq
           FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_ready
          WHERE purge_on_settle = 0 AND dlq_target IS NULL
            AND lease_id IS NULL AND visible_at <= ?
          ORDER BY visible_at, seq
          LIMIT ?`,
        now,
        consumer.maxBatchSize,
      ).toArray();
      const expiredReady = sql.exec(
        `SELECT leased_until AS ready_at, seq
           FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_lease
          WHERE purge_on_settle = 0 AND dlq_target IS NULL
            AND lease_id IS NOT NULL AND leased_until <= ?
          ORDER BY leased_until, seq
          LIMIT ?`,
        now,
        consumer.maxBatchSize,
      ).toArray();
      ready = [...visibleReady, ...expiredReady]
        .sort((left, right) => left.ready_at - right.ready_at || left.seq - right.seq)
        .slice(0, consumer.maxBatchSize);
    }
    if (typeof __test_queue_rearm_bounded === "function") {
      __test_queue_rearm_bounded(
        ready.length <= (consumer?.maxBatchSize ?? 0),
        ready.length > 0,
      );
    }
    const earliestVisible = hasCapacity
      ? sql.exec(
        `SELECT visible_at
           FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_ready
          WHERE purge_on_settle = 0 AND dlq_target IS NULL
            AND lease_id IS NULL AND visible_at > ?
          ORDER BY visible_at, seq
          LIMIT 1`,
        now,
      ).toArray()[0]?.visible_at ?? null
      : null;
    const earliestLeaseExpiry = paused
      ? null
      : sql.exec(
        `SELECT leased_until
           FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_lease
          WHERE lease_id IS NOT NULL
          ORDER BY leased_until
          LIMIT 1`,
      ).toArray()[0]?.leased_until ?? null;
    const nextSweep = sql.exec(
      `SELECT enqueued_at + ? AS next_sweep
         FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_retained
        WHERE dlq_target IS NULL AND purge_on_settle = 0
        ORDER BY enqueued_at, seq
        LIMIT 1`,
      __queueLimits().retentionMs,
    ).toArray()[0]?.next_sweep ?? null;
    const dlqPending = sql.exec(
      `SELECT 1 AS pending
         FROM ${__QUEUE_MESSAGES} INDEXED BY ${__QUEUE_MESSAGES}_moving
        WHERE dlq_target IS NOT NULL
        LIMIT 1`,
    ).toArray().length !== 0;
    const receiptSweep = sql.exec(
      `SELECT expires_at AS next_sweep
         FROM ${__QUEUE_TRANSFER_RECEIPTS}
        ORDER BY expires_at, transfer_id
        LIMIT 1`,
    ).toArray()[0]?.next_sweep ?? null;
    // A queue without an attached consumer only wakes to reclaim retention.
    // Consumer attachment will nudge the cell when the fleet catalog gains a
    // registration; spinning on an already-visible row cannot make progress.
    let batchDeadline = null;
    if (ready.length > 0) {
      batchDeadline = ready.length >= consumer.maxBatchSize
        ? now
        : ready[0].ready_at + consumer.maxBatchTimeoutMs;
    }
    if (dlqPending) {
      batchDeadline = batchDeadline === null ? now : Math.min(batchDeadline, now);
    }
    const sweepDeadlines = [nextSweep, receiptSweep]
      .filter((deadline) => deadline !== null && deadline !== undefined);
    const armAt = __queuePolicy({
      op: "rearm",
      now,
      batchDeadline,
      earliestVisible,
      // A live handler can settle after delivery pauses. Do not reclaim its
      // lease while paused; resume re-arms the expired deadline immediately
      // and only then can a new generation replace it.
      earliestLeaseExpiry,
      nextSweep: sweepDeadlines.length === 0 ? null : Math.min(...sweepDeadlines),
    });
    if (armAt !== null && !Number.isFinite(armAt)) {
      throw __queueError("the Queue sweep deadline is invalid");
    }
    if (armAt === null) await this.ctx.storage.deleteAlarm();
    else if (waitForWake) {
      await __queue_alarm_set_wait(this.ctx.storage._scope, armAt);
    } else await this.ctx.storage.setAlarm(armAt);
  }

  _newProducerGroup() {
    const group = {
      calls: [],
      entries: [],
      bytes: 0,
      flushing: false,
      timer: null,
    };
    this._producerGroup = group;
    group.timer = setTimeout(() => {
      void this._flushProducerGroup(group);
    }, __queueLimits().producerGroupMs);
    return group;
  }

  async _flushProducerGroup(group) {
    if (group.flushing) return;
    group.flushing = true;
    if (this._producerGroup === group) this._producerGroup = null;
    if (group.timer !== null) {
      clearTimeout(group.timer);
      group.timer = null;
    }

    const now = Date.now();
    try {
      const sql = this._open();
      this.ctx.storage.transactionSync(() => {
        for (const entry of group.entries) {
          sql.exec(
            `INSERT INTO ${__QUEUE_MESSAGES}
               (id, body, content_type, size, enqueued_at, visible_at)
             VALUES (?, ?, ?, ?, ?, ?)`,
            __queueMessageId(now),
            entry.body,
            entry.contentType,
            entry.size,
            now,
            now + entry.delaySeconds * 1000,
          );
        }
      });
      await this._rearm(now, true);
      const metrics = this._metrics(now);
      if (typeof __test_queue_producer_group === "function") {
        __test_queue_producer_group(group.calls.length, group.entries.length);
      }
      for (const call of group.calls) call.resolve(metrics);
    } catch (error) {
      for (const call of group.calls) call.reject(error);
    }
  }

  __queueSend({ entries }) {
    const bytes = entries.reduce((total, entry) => total + entry.size, 0);
    return new Promise((resolve, reject) => {
      let group = this._producerGroup;
      if (group !== null &&
          (group.calls.length + 1 > __QUEUE_PRODUCER_GROUP_CALLS ||
            group.entries.length + entries.length > __QUEUE_PRODUCER_GROUP_MESSAGES ||
            group.bytes + bytes > __QUEUE_PRODUCER_GROUP_BYTES)) {
        void this._flushProducerGroup(group);
        group = null;
      }
      if (group === null) group = this._newProducerGroup();
      group.calls.push({ resolve, reject });
      group.entries.push(...entries);
      group.bytes += bytes;
      if (group.calls.length === __QUEUE_PRODUCER_GROUP_CALLS ||
          group.entries.length === __QUEUE_PRODUCER_GROUP_MESSAGES ||
          group.bytes === __QUEUE_PRODUCER_GROUP_BYTES) {
        void this._flushProducerGroup(group);
      }
    });
  }

  __queueMetrics() {
    return this._metrics();
  }

  // Retention decides visibility through `_metrics` immediately, while this
  // bounded sweep only reclaims space. A live lease is marked instead of
  // deleted, so its handler can finish but settlement or expiry removes it
  // without a redelivery.
  _sweep(now) {
    const sql = this._open();
    const cutoff = now - __queueLimits().retentionMs;
    const messages = __cellSweepBatch(
      this.ctx.storage,
      (limit) => sql.exec(
        `SELECT seq, lease_id, leased_until FROM ${__QUEUE_MESSAGES}
          WHERE enqueued_at <= ?
            AND (purge_on_settle = 0
              OR lease_id IS NULL OR leased_until <= ?)
          ORDER BY enqueued_at, seq
          LIMIT ?`,
        cutoff,
        now,
        limit,
      ).toArray(),
      (row) => {
        if (row.lease_id !== null && row.leased_until > now) {
          sql.exec(
            `UPDATE ${__QUEUE_MESSAGES} SET purge_on_settle = 1 WHERE seq = ?`,
            row.seq,
          );
        } else {
          sql.exec(`DELETE FROM ${__QUEUE_MESSAGES} WHERE seq = ?`, row.seq);
        }
      },
      __queueLimits().sweepBatchRows,
    );
    const remaining = __queueLimits().sweepBatchRows - messages;
    if (remaining === 0) return messages;
    const receipts = __cellSweepBatch(
      this.ctx.storage,
      (limit) => sql.exec(
        `SELECT transfer_id FROM ${__QUEUE_TRANSFER_RECEIPTS}
          WHERE expires_at <= ?
          ORDER BY expires_at, transfer_id
          LIMIT ?`,
        now,
        limit,
      ).toArray(),
      (row) => {
        sql.exec(
          `DELETE FROM ${__QUEUE_TRANSFER_RECEIPTS} WHERE transfer_id = ?`,
          row.transfer_id,
        );
      },
      remaining,
    );
    return messages + receipts;
  }

  _candidateRows(now, limit) {
    return this._open().exec(
      `SELECT seq, id, body, content_type, size, enqueued_at, visible_at,
              attempts, lease_id, lease_generation, leased_until,
              purge_on_settle
         FROM ${__QUEUE_MESSAGES}
        WHERE dlq_target IS NULL AND ((purge_on_settle = 1
                 AND (lease_id IS NULL OR leased_until <= ?))
           OR (purge_on_settle = 0 AND visible_at <= ?
                 AND (lease_id IS NULL OR leased_until <= ?)))
        ORDER BY visible_at, seq
        LIMIT ?`,
      now,
      now,
      now,
      limit,
    ).toArray();
  }

  async _leaseBatch(now, consumer) {
    const sql = this._open();
    const active = sql.exec(
      `SELECT COUNT(DISTINCT lease_id) AS count
         FROM ${__QUEUE_MESSAGES}
        WHERE lease_id IS NOT NULL AND leased_until > ?`,
      now,
    ).toArray()[0].count;
    const concurrency = consumer.maxConcurrency ?? __queueLimits().maxConcurrency;
    if (!__queuePolicy({ op: "capacity", active, maximum: concurrency })) return null;

    // Read extra rows so expired purge markers cannot hide a deliverable
    // batch. Policy chooses at most `maxBatchSize` rows and returns every
    // purge deletion that precedes them in this bounded scan.
    const rows = this._candidateRows(now, consumer.maxBatchSize + 256);
    const deliverable = rows.filter((row) => row.purge_on_settle === 0);
    if (deliverable.length === 0) {
      if (rows.length > 0) {
        this.ctx.storage.transactionSync(() => {
          for (const row of rows) {
            sql.exec(`DELETE FROM ${__QUEUE_MESSAGES} WHERE seq = ?`, row.seq);
          }
        });
      }
      return null;
    }
    const readySince = Math.min(...deliverable.map((row) =>
      row.lease_id === null ? row.visible_at : row.leased_until
    ));
    if (deliverable.length < consumer.maxBatchSize &&
        readySince + consumer.maxBatchTimeoutMs > now) {
      return null;
    }

    const plan = __queuePolicy({
      op: "batch",
      now,
      maxBatchSize: consumer.maxBatchSize,
      rows: rows.map((row) => ({
        seq: row.seq,
        visibleAt: row.visible_at,
        leaseGeneration: String(row.lease_generation),
        leasedUntil: row.leased_until,
        purgeOnSettle: row.purge_on_settle !== 0,
      })),
    });
    if (plan.leases.length === 0 && plan.deletePurged.length === 0) return null;
    const rowBySeq = new Map(rows.map((row) => [row.seq, row]));
    const reclaimed = plan.leases.filter((lease) => lease.reclaimed);
    const expiryPolicy = __queuePolicy({
      op: "expiry",
      now,
      entries: reclaimed.map((lease) => {
        const row = rowBySeq.get(lease.seq);
        return {
          priorFailures: row.attempts,
          maxRetries: consumer.maxRetries,
          configuredSeconds: consumer.retryDelaySeconds ?? null,
          purgeOnSettle: row.purge_on_settle !== 0,
        };
      }),
    });
    const expiryBySeq = new Map(
      reclaimed.map((lease, index) => [lease.seq, expiryPolicy[index]]),
    );
    const installs = plan.leases.filter((lease) => {
      const expiry = expiryBySeq.get(lease.seq);
      return expiry === undefined ||
        (expiry.action.kind === "retry" && expiry.action.at <= now);
    });
    const exhaustedTransfers = new Map(
      reclaimed
        .filter((lease) =>
          expiryBySeq.get(lease.seq).action.kind === "exhausted" &&
          consumer.deadLetterQueue !== undefined
        )
        .map((lease) => [lease.seq, crypto.randomUUID()]),
    );
    const leaseId = crypto.randomUUID();
    const leasedUntil = Math.min(
      Number.MAX_SAFE_INTEGER,
      now + __cell.queueLeaseDurationMs,
    );
    this.ctx.storage.transactionSync(() => {
      for (const seq of plan.deletePurged) {
        sql.exec(
          `DELETE FROM ${__QUEUE_MESSAGES}
            WHERE seq = ? AND purge_on_settle = 1
              AND (lease_id IS NULL OR leased_until <= ?)`,
          seq,
          now,
        );
      }
      for (const lease of reclaimed) {
        const expiry = expiryBySeq.get(lease.seq);
        if (expiry.action.kind === "delete-purged") {
          sql.exec(
            `DELETE FROM ${__QUEUE_MESSAGES}
              WHERE seq = ? AND leased_until <= ?`,
            lease.seq,
            now,
          );
        } else if (expiry.action.kind === "exhausted") {
          if (consumer.deadLetterQueue === undefined) {
            sql.exec(
              `DELETE FROM ${__QUEUE_MESSAGES}
                WHERE seq = ? AND leased_until <= ?`,
              lease.seq,
              now,
            );
          } else {
            sql.exec(
              `UPDATE ${__QUEUE_MESSAGES}
                  SET attempts = ?, lease_id = NULL, leased_until = NULL,
                      dlq_target = ?, dlq_transfer_id = ?,
                      transfer_kind = 'dead-letter'
                WHERE seq = ? AND leased_until <= ?`,
              expiry.attempt,
              consumer.deadLetterQueue,
              exhaustedTransfers.get(lease.seq),
              lease.seq,
              now,
            );
          }
        } else if (expiry.action.at > now) {
          sql.exec(
            `UPDATE ${__QUEUE_MESSAGES}
                SET attempts = ?, visible_at = ?, lease_id = NULL,
                    leased_until = NULL
              WHERE seq = ? AND leased_until <= ?`,
            expiry.attempt,
            expiry.action.at,
            lease.seq,
            now,
          );
        }
      }
      for (const lease of installs) {
        const generation = Number(lease.generation);
        if (!Number.isSafeInteger(generation)) {
          throw __queueError(`message ${lease.seq} exhausted its safe lease generation`);
        }
        sql.exec(
          `UPDATE ${__QUEUE_MESSAGES}
              SET lease_id = ?, lease_generation = ?, leased_until = ?,
                  attempts = attempts + ?
            WHERE seq = ? AND lease_generation = ?
              AND (lease_id IS NULL OR leased_until <= ?)`,
          leaseId,
          generation,
          leasedUntil,
          lease.reclaimed ? 1 : 0,
          lease.seq,
          generation - 1,
          now,
        );
      }
    });
    if (installs.length === 0) return null;
    const leasedQuery =
      `SELECT seq, id, body, content_type, enqueued_at, attempts,
              lease_generation
         FROM ${__QUEUE_MESSAGES}
        WHERE lease_id = ?
        ORDER BY visible_at, seq`;
    __queueReportLeaseLookupPlan(sql, leasedQuery, leaseId);
    const leased = sql.exec(leasedQuery, leaseId).toArray();
    if (leased.length !== installs.length) {
      throw __queueError("the Queue lease transaction changed fewer rows than its plan");
    }
    return {
      leaseId,
      leases: leased.map((row) => ({
        message_id: row.id,
        seq: row.seq,
        generation: row.lease_generation,
      })),
      messages: leased.map((row) => ({
        id: row.id,
        timestampMs: row.enqueued_at,
        // SQLite answers a BLOB as an ArrayBuffer. The raw base64 op accepts
        // typed views, so normalize here; otherwise it encodes the text
        // "[object ArrayBuffer]" and every consumer decoder sees corrupt data.
        bodyBase64: $$btoa(
          row.body instanceof Uint8Array ? row.body : new Uint8Array(row.body),
        ),
        contentType: row.content_type,
        attempts: row.attempts + 1,
      })),
      // Miniflare removes every dispatched batch from its backlog before it
      // invokes the consumer. Exclude every lease here for the same contract;
      // the operator aggregate still includes durable in-flight messages.
      metrics: this._metrics(now, true),
    };
  }

  // The target deduplicates the stable transfer identity. The source can
  // therefore repeat this call after a stop between the target commit and the
  // source delete without creating a second message.
  async __queueDlqAcceptBatch({ transfers }) {
    if (!Array.isArray(transfers) || transfers.length === 0 ||
        transfers.length > __queueLimits().maxBatchMessages) {
      throw __queueError("a Queue transfer batch has an invalid size");
    }
    for (const transfer of transfers) {
      if (transfer.deadLetterSource !== null &&
          typeof transfer.deadLetterSource !== "string") {
        throw __queueError("a Queue transfer has an invalid dead-letter source");
      }
    }
    const now = Date.now();
    const sql = this._open();
    this.ctx.storage.transactionSync(() => {
      for (const { transferId, entry, deadLetterSource } of transfers) {
        const accepted = sql.exec(
          `SELECT message_id, dead_letter_source
             FROM ${__QUEUE_TRANSFER_RECEIPTS}
            WHERE transfer_id = ?`,
          transferId,
        ).toArray()[0];
        if (accepted !== undefined && (accepted.message_id !== entry.id ||
            accepted.dead_letter_source !== deadLetterSource)) {
          throw __queueError("a DLQ transfer collided with another message identity");
        }
        if (accepted !== undefined) continue;
        sql.exec(
          `INSERT INTO ${__QUEUE_TRANSFER_RECEIPTS}
             (transfer_id, message_id, dead_letter_source, expires_at)
           VALUES (?, ?, ?, ?)`,
          transferId,
          entry.id,
          deadLetterSource,
          now + __queueLimits().retentionMs,
        );
        sql.exec(
          `INSERT INTO ${__QUEUE_MESSAGES}
             (id, body, content_type, size, enqueued_at, visible_at,
              attempts, dlq_transfer_id, dead_letter_source)
           VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)`,
          entry.id,
          entry.body,
          entry.contentType,
          entry.size,
          now,
          now,
          transferId,
          deadLetterSource,
        );
      }
    });
    await this._rearm(now);
    return this._metrics(now);
  }

  async _driveDlq(limit = 100) {
    const now = Date.now();
    const pending = this._open().exec(
      `SELECT seq, id, body, content_type, size, dlq_target, dlq_transfer_id,
              transfer_kind
         FROM ${__QUEUE_MESSAGES}
        WHERE dlq_target IS NOT NULL
          AND enqueued_at > ?
        ORDER BY seq
        LIMIT ?`,
      now - __queueLimits().retentionMs,
      limit,
    ).toArray();
    const groups = new Map();
    for (const row of pending) {
      let group = groups.get(row.dlq_target);
      if (group === undefined) groups.set(row.dlq_target, group = []);
      group.push(row);
    }
    for (const [targetName, rows] of groups) {
      const target = __cell.makeNamespace("__Queue").getByName(targetName);
      await target.__queueDlqAcceptBatch({
        transfers: rows.map((row) => ({
          transferId: row.dlq_transfer_id,
          // A dead-letter transfer records where redrive returns the message.
          // A redrive returns it to ordinary queue state, so the source marker
          // must not follow it and make a later redrive bounce it backwards.
          deadLetterSource: row.transfer_kind === "dead-letter" ? this.queue : null,
          entry: {
            id: row.id,
            body: row.body,
            contentType: row.content_type,
            size: row.size,
          },
        })),
      });
      if (typeof __test_queue_dlq_accepted === "function") {
        __test_queue_dlq_accepted();
      }
      // The target committed every receipt before this reply. Delete the
      // corresponding source marks together; a stop before this transaction
      // leaves the whole group replayable through those stable receipts.
      this.ctx.storage.transactionSync(() => {
        for (const row of rows) {
          this._open().exec(
            `DELETE FROM ${__QUEUE_MESSAGES}
              WHERE seq = ? AND dlq_target = ? AND dlq_transfer_id = ?
                AND transfer_kind = ?`,
            row.seq,
            row.dlq_target,
            row.dlq_transfer_id,
            row.transfer_kind,
          );
        }
      });
    }
    return pending.length;
  }

  async alarm() {
    const now = Date.now();
    try {
      await this._driveDlq();
    } catch (error) {
      // The durable source mark is the retry record. An alarm stays armed at
      // `now`, and the target's transfer identity makes a replay idempotent.
      console.error("Queue DLQ transfer failed:", error);
    }
    const swept = this._sweep(now);
    const consumer = this._paused()
      ? undefined
      : __cell.queueConsumers?.[this.queue];
    // Install every lease that the configured concurrency admits before the
    // first consumer can settle. One lease per alarm serialized full batches
    // because the next alarm depended on a later scheduler turn, so a queue
    // with maxConcurrency > 1 still used only one handler under backlog.
    // `_leaseBatch` counts the durable lease IDs on every iteration, which
    // makes the configured concurrency the structural bound for this loop.
    const batches = [];
    if (consumer !== undefined) {
      while (true) {
        const batch = await this._leaseBatch(now, consumer);
        if (batch === null) break;
        batches.push(batch);
      }
    }
    try {
      // `_leaseBatch` can exhaust an expired lease and create a new durable
      // DLQ mark. Drive that mark in the same wake; the first call above is
      // for a mark recovered from an earlier stopped process.
      await this._driveDlq();
    } catch (error) {
      console.error("Queue DLQ transfer failed:", error);
    }
    await this._rearm(now);
    if (swept >= __queueLimits().sweepBatchRows && batches.length === 0) {
      await this.ctx.storage.setAlarm(now + 1);
      return;
    }
    for (const batch of batches) {
      // This await covers only the output-gated handoff to the host. The
      // consumer runs after this alarm returns, and settlement comes back as
      // an independent cell event. All admitted leases are durable before the
      // first handoff, so a fast settlement cannot make this turn exceed the
      // configured concurrency.
      await __queue_dispatch(
        consumer.script,
        this.queue,
        JSON.stringify(batch),
      );
    }
  }

  _matchingLease(body) {
    if (typeof body.leaseId !== "string" || !Array.isArray(body.leases)) return null;
    const matchQuery =
      `SELECT seq, id, attempts, lease_generation, purge_on_settle
         FROM ${__QUEUE_MESSAGES}
        WHERE lease_id = ?
        ORDER BY seq`;
    __queueReportLeaseLookupPlan(this._open(), matchQuery, body.leaseId);
    const rows = this._open().exec(matchQuery, body.leaseId).toArray();
    for (const lease of body.leases) {
      if (typeof lease?.message_id !== "string" ||
          !Number.isSafeInteger(lease.seq) ||
          !Number.isSafeInteger(lease.generation)) return null;
    }
    const matches = __queuePolicy({
      op: "settlement",
      current: rows.map((row) => ({
        seq: String(row.seq),
        messageId: row.id,
        generation: String(row.lease_generation),
      })),
      submitted: body.leases.map((lease) => ({
        seq: String(lease.seq),
        messageId: lease.message_id,
        generation: String(lease.generation),
      })),
    });
    return matches ? rows : null;
  }

  async _settle(body) {
    const rows = this._matchingLease(body);
    if (rows === null) return false;
    const consumer = __cell.queueConsumers?.[this.queue];
    if (consumer === undefined) return false;
    const explicitAcks = new Set(body.explicitAcks ?? []);
    const retryMessages = new Map(
      (body.retryMessages ?? []).map((retry) => [retry.msgId, retry.delaySeconds]),
    );
    const retryBatch = body.retryBatch?.retry === true;
    const batchDelay = body.retryBatch?.delaySeconds;
    const exception = body.outcome === "exception";
    // An explicit ack-all survives a later handler exception, so it must
    // override every batch-level and message-level retry signal.
    const ackAll = body.ackAll === true;
    const decisions = rows.map((row) => {
      const retry = !ackAll && !explicitAcks.has(row.id) && (
        retryMessages.has(row.id) || retryBatch || exception
      );
      return { row, retry };
    });
    const retrying = decisions.filter((decision) =>
      decision.retry && decision.row.purge_on_settle === 0
    );
    const retryPolicy = __queuePolicy({
      op: "retries",
      now: Date.now(),
      entries: retrying.map(({ row }) => ({
        attempt: row.attempts + 1,
        maxRetries: consumer.maxRetries,
        explicitSeconds: retryMessages.has(row.id)
          ? retryMessages.get(row.id)
          : retryBatch ? batchDelay : null,
        configuredSeconds: consumer.retryDelaySeconds ?? null,
      })),
    });
    const retryById = new Map(
      retrying.map(({ row }, index) => [row.id, retryPolicy[index]]),
    );
    const dlqTransferById = new Map(
      retrying
        .filter(({ row }) =>
          retryById.get(row.id).exhausted && consumer.deadLetterQueue !== undefined
        )
        .map(({ row }) => [row.id, crypto.randomUUID()]),
    );
    const sql = this._open();
    this.ctx.storage.transactionSync(() => {
      for (const { row, retry } of decisions) {
        const policy = retryById.get(row.id);
        if (row.purge_on_settle !== 0 || !retry ||
            (policy.exhausted && consumer.deadLetterQueue === undefined)) {
          sql.exec(
            `DELETE FROM ${__QUEUE_MESSAGES}
              WHERE seq = ? AND lease_id = ? AND lease_generation = ?`,
            row.seq,
            body.leaseId,
            row.lease_generation,
          );
          continue;
        }
        if (policy.exhausted) {
          sql.exec(
            `UPDATE ${__QUEUE_MESSAGES}
                SET attempts = attempts + 1, lease_id = NULL,
                    leased_until = NULL, dlq_target = ?, dlq_transfer_id = ?,
                    transfer_kind = 'dead-letter'
              WHERE seq = ? AND lease_id = ? AND lease_generation = ?`,
            consumer.deadLetterQueue,
            dlqTransferById.get(row.id),
            row.seq,
            body.leaseId,
            row.lease_generation,
          );
          continue;
        }
        sql.exec(
          `UPDATE ${__QUEUE_MESSAGES}
              SET attempts = attempts + 1, visible_at = ?,
                  lease_id = NULL, leased_until = NULL
            WHERE seq = ? AND lease_id = ? AND lease_generation = ?`,
          policy.at,
          row.seq,
          body.leaseId,
          row.lease_generation,
        );
      }
    });
    try {
      await this._driveDlq();
    } catch (error) {
      // Settlement is durable once the source mark commits. A failed transfer
      // is alarm work now, not a reason to put the message back in delivery.
      console.error("Queue DLQ transfer deferred:", error);
    }
    await this._rearm();
    return true;
  }

  async _purge() {
    const now = Date.now();
    const sql = this._open();
    let deleted = 0;
    let leased = 0;
    this.ctx.storage.transactionSync(() => {
      let after = 0;
      while (true) {
        // Purge is necessarily proportional to the stored backlog, but its
        // policy envelope stays bounded instead of materializing the Queue in
        // V8 or Rust. One SQLite transaction keeps the visibility change
        // atomic across all chunks.
        const rows = sql.exec(
          `SELECT seq, lease_id, leased_until FROM ${__QUEUE_MESSAGES}
            WHERE seq > ?
            ORDER BY seq
            LIMIT 256`,
          after,
        ).toArray();
        if (rows.length === 0) break;
        after = rows[rows.length - 1].seq;
        const plan = __queuePolicy({
          op: "purge",
          now,
          rows: rows.map((row) => ({
            seq: String(row.seq),
            leaseIdPresent: row.lease_id !== null,
            leasedUntil: row.leased_until,
          })),
        });
        for (const seq of plan.delete) {
          sql.exec(
            `DELETE FROM ${__QUEUE_MESSAGES}
              WHERE seq = ?
                AND (lease_id IS NULL OR leased_until IS NULL OR leased_until <= ?)`,
            Number(seq),
            now,
          );
          deleted += 1;
        }
        for (const seq of plan.markForSettle) {
          sql.exec(
            `UPDATE ${__QUEUE_MESSAGES} SET purge_on_settle = 1
              WHERE seq = ? AND lease_id IS NOT NULL AND leased_until > ?`,
            Number(seq),
            now,
          );
          leased += 1;
        }
      }
    });
    await this._rearm(now);
    return { deleted, leased, metrics: this._metrics(now) };
  }

  async _setPaused(paused) {
    this._setMeta("paused", paused ? "1" : null);
    await this._rearm();
    return { paused, ...this._metrics() };
  }

  _operatorLimit(value, fallback) {
    const limit = value ?? fallback;
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
      throw __queueError("limit must be an integer from 1 through 100");
    }
    return limit;
  }

  _peek(limit) {
    const now = Date.now();
    const cutoff = now - __queueLimits().retentionMs;
    const rows = this._open().exec(
      `SELECT id, body, content_type, size, enqueued_at, visible_at, attempts,
              lease_id, leased_until, purge_on_settle, dlq_target,
              dead_letter_source
         FROM ${__QUEUE_MESSAGES}
        WHERE enqueued_at > ?
        ORDER BY visible_at, seq
        LIMIT ?`,
      cutoff,
      limit,
    ).toArray();
    return {
      messages: rows.map((row) => ({
        id: row.id,
        bodyBase64: $$btoa(
          row.body instanceof Uint8Array ? row.body : new Uint8Array(row.body),
        ),
        contentType: row.content_type,
        size: row.size,
        enqueuedAt: row.enqueued_at,
        visibleAt: row.visible_at,
        attempts: row.attempts,
        state: row.dlq_target !== null
          ? "moving"
          : row.purge_on_settle !== 0
          ? "purging"
          : row.lease_id !== null && row.leased_until > now
          ? "leased"
          : row.visible_at <= now
          ? "visible"
          : "delayed",
        leasedUntil: row.lease_id !== null && row.leased_until > now
          ? row.leased_until
          : null,
        transferTarget: row.dlq_target,
        deadLetterSource: row.dead_letter_source,
      })),
    };
  }

  async _redrive(limit) {
    // Finish an interrupted move before selecting more work. A transfer mark
    // is durable, so selecting it a second time would invent another identity
    // where replaying the existing identity is sufficient.
    await this._driveDlq(100);
    const now = Date.now();
    const cutoff = now - __queueLimits().retentionMs;
    const candidates = this._open().exec(
      `SELECT seq, dead_letter_source
         FROM ${__QUEUE_MESSAGES}
        WHERE dead_letter_source IS NOT NULL
          AND dlq_target IS NULL
          AND purge_on_settle = 0
          AND enqueued_at > ?
          AND (lease_id IS NULL OR leased_until <= ?)
        ORDER BY visible_at, seq
        LIMIT ?`,
      cutoff,
      now,
      limit,
    ).toArray();
    const transfers = candidates.map((row) => ({
      ...row,
      transferId: crypto.randomUUID(),
    }));
    this.ctx.storage.transactionSync(() => {
      for (const transfer of transfers) {
        this._open().exec(
          `UPDATE ${__QUEUE_MESSAGES}
              SET dlq_target = ?, dlq_transfer_id = ?, transfer_kind = 'redrive',
                  lease_id = NULL, leased_until = NULL
            WHERE seq = ? AND dead_letter_source = ? AND dlq_target IS NULL
              AND purge_on_settle = 0
              AND (lease_id IS NULL OR leased_until <= ?)`,
          transfer.dead_letter_source,
          transfer.transferId,
          transfer.seq,
          transfer.dead_letter_source,
          now,
        );
      }
    });
    // Arm before the cross-cell call. If the process stops after the durable
    // mark, the alarm replays the same transfer identity.
    await this._rearm(now);
    await this._driveDlq(100);
    await this._rearm();
    return { redriven: transfers.length, metrics: this._metrics() };
  }

  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (request.method !== "POST") {
      return new Response("Queue route not found", { status: 404 });
    }
    let body;
    try {
      body = await request.json();
    } catch {
      return new Response("invalid Queue request", { status: 400 });
    }
    try {
      if (path === "/__qSettle") {
        return await this._settle(body)
          ? new Response(null, { status: 204 })
          : new Response("stale Queue settlement", { status: 409 });
      }
      if (body.op === "info") {
        const metrics = this._metrics();
        const stored = this._open().exec(
          `SELECT stored_count FROM ${__QUEUE_STATS} WHERE id = 1`,
        ).toArray()[0].stored_count;
        return Response.json({ result: { ...metrics, stored, paused: this._paused() } });
      }
      if (body.op === "purge") {
        return Response.json({ result: await this._purge() });
      }
      if (body.op === "pause") {
        return Response.json({ result: await this._setPaused(true) });
      }
      if (body.op === "resume") {
        return Response.json({ result: await this._setPaused(false) });
      }
      if (body.op === "peek") {
        return Response.json({
          result: this._peek(this._operatorLimit(body.limit, 10)),
        });
      }
      if (body.op === "redrive") {
        return Response.json({
          result: await this._redrive(this._operatorLimit(body.limit, 100)),
        });
      }
      return Response.json(
        { error: `QUEUE_ERROR: unknown operation ${JSON.stringify(body.op)}` },
        { status: 400 },
      );
    } catch (error) {
      return Response.json(
        { error: String(error?.message ?? error) },
        { status: 400 },
      );
    }
  }
}

__cell.classes.__Queue = __QueueCell;
__cell.doExports.__Queue = true;

class Queue {
  constructor(queue, cellName, deliveryDelay) {
    Object.defineProperties(this, {
      _queue: { value: queue },
      _cellName: { value: cellName },
      _deliveryDelay: { value: deliveryDelay },
    });
  }

  get _stub() {
    return __cell.makeNamespace("__Queue").getByName(this._cellName);
  }

  async _send(entries) {
    const metrics = await this._stub.__queueSend({ entries });
    return { metadata: { metrics: __queueMetricsView(metrics) } };
  }

  send(body, options = {}) {
    const encoded = __queueEncode(body, options.contentType);
    return this._send([{
      ...encoded,
      delaySeconds: __queueDelay(options.delaySeconds, this._deliveryDelay),
    }]);
  }

  sendBatch(messages, options = {}) {
    if (messages === null || messages === undefined ||
        typeof messages[Symbol.iterator] !== "function") {
      throw __queueError("sendBatch() needs an iterable of messages");
    }
    const entries = [];
    let bytes = 0;
    const batchDelay = __queueDelay(options.delaySeconds, this._deliveryDelay);
    for (const message of messages) {
      if (entries.length >= __queueLimits().maxBatchMessages) {
        throw __queueError(
          `a batch has at most ${__queueLimits().maxBatchMessages} messages`,
        );
      }
      if (message === null || typeof message !== "object" || !("body" in message)) {
        throw __queueError("each batch entry needs a body");
      }
      const encoded = __queueEncode(message.body, message.contentType);
      bytes += encoded.size;
      if (bytes > __queueLimits().maxBatchBytes) {
        throw __queueError(
          `a batch is at most ${__queueLimits().maxBatchBytes} bytes, got ${bytes}`,
        );
      }
      entries.push({
        ...encoded,
        delaySeconds: __queueDelay(message.delaySeconds, batchDelay),
      });
    }
    if (entries.length === 0) {
      throw __queueError("sendBatch() needs at least one message");
    }
    return this._send(entries);
  }

  async metrics() {
    return __queueMetricsView(await this._stub.__queueMetrics());
  }
}

globalThis.__makeQueue = (queue, cellName, deliveryDelay) =>
  new Queue(queue, cellName, deliveryDelay);

// ---- D1 -----------------------------------------------------------------
// A D1 database is a cell of a runtime-supplied Durable Object class, so it
// inherits ownership, fencing, LTX replication, RPO=0 acknowledgement and
// pressure shedding from the cell it already is. This half is the server;
// `__makeD1Database` below is the client the binding hands to a Worker.
//
// Everything SQL goes through the one `__d1_run` op. The engine owns the
// statement walk, the row and byte caps, and the meta snapshot, all inside
// one execution — the previous shape assembled these from the general
// SqlStorage ops, and every seam between them was a contract only convention
// enforced.

// The error families applications match on. A message that already carries
// one must pass through unwrapped: wrapping again would bury the family the
// application is matching under a second D1_ERROR prefix.
const __d1IsFamilyError = (message) =>
  /^D1_([A-Z]+_)*(ERROR|NOTFOUND): /.test(String(message));

const __d1Error = (message, cause) => {
  const error = new Error("D1_ERROR: " + String(message));
  error.cause = cause !== undefined ? cause : new Error(String(message));
  return error;
};

// A typed refusal from the engine: `{family, message}` becomes the Error the
// application sees, with `.cause` carrying the bare message as upstream does.
const __d1Failure = (failure) => {
  const error = new Error(failure.family + ": " + failure.message);
  error.cause = new Error(failure.message);
  return error;
};

const __d1Run = (scope, request) => {
  const response = JSON.parse(__d1_run(scope, JSON.stringify(request)));
  if (response.error) throw __d1Failure(response.error);
  return response.ok;
};

class __D1DatabaseCell {
  constructor(ctx) {
    this.ctx = ctx;
  }
  // Takes an array so that `batch()` can ride this same method, and the
  // same turn, when it lands.
  __d1Query(statements) {
    const scope = this.ctx.storage.sql._scope;
    return statements.map((statement) =>
      __d1Run(scope, {
        mode: "prepared",
        sql: String(statement.sql),
        params: statement.params || [],
        first: !!statement.first,
      })
    );
  }
  __d1Batch(statements) {
    return __d1Run(this.ctx.storage.sql._scope, {
      mode: "batch",
      statements: statements.map((statement) => ({
        sql: String(statement.sql),
        params: statement.params || [],
      })),
    });
  }
  // The CLI's way in. `celld d1` signs each request with the fleet secret
  // and sends it to a live node's `/runtime/<scope>` route, which verifies the
  // signature and then forwards to the owner over the same dispatch `/do/`
  // uses — so the CLI needs no ownership logic and the database is reached
  // the way a Worker reaches it. The unauthenticated `/do/` route refuses a
  // D1 scope outright: this cell answers arbitrary SQL, and its scope is an
  // HMAC over names that sit in the project's config, so the scope itself
  // is no secret.
  async fetch(request) {
    let body;
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "D1_ERROR: invalid request body" }, { status: 400 });
    }
    try {
      let result;
      if (body.migrate !== undefined) {
        result = this.__d1Migrate(
          String(body.migrate.name),
          String(body.migrate.sql),
          String(body.migrate.table || "d1_migrations"),
        );
      } else if (body.exec !== undefined) {
        // The CLI asks for rows; the Worker binding's exec() never does.
        result = __d1Run(this.ctx.storage.sql._scope, {
          mode: "exec",
          sql: String(body.exec.sql),
          rows: !!body.exec.rows,
        });
      } else {
        result = this.__d1Query(body.statements || []);
      }
      return Response.json({ result });
    } catch (error) {
      return Response.json({ error: String(error && error.message || error) }, { status: 400 });
    }
  }
  __d1Exec(source) {
    const result = __d1Run(this.ctx.storage.sql._scope, {
      mode: "exec",
      sql: String(source),
      rows: false,
    });
    return { count: result.count, duration: result.duration };
  }
  // Apply one migration and record it. The engine runs the file, the
  // bookkeeping insert (the name is a bound parameter, so a file name never
  // reaches SQL text) and the BEGIN IMMEDIATE .. COMMIT bracket in one
  // request, so a migration that fails part-way rolls back whole and the
  // operator fixes the file and re-runs. Without the transaction the
  // statements that DID land are exactly the ones a re-run trips over.
  __d1Migrate(name, source, table) {
    return __d1Run(this.ctx.storage.sql._scope, {
      mode: "migrate",
      name,
      sql: source,
      table,
    });
  }
}

__cell.classes.__D1Database = __D1DatabaseCell;
// RPC on a stub needs `extends DurableObject` or the js_rpc flag. This class
// is the runtime's own, so grant it here instead of making every D1 user set
// a compatibility flag to reach their database.
__cell.doExports.__D1Database = true;

// Validate and encode bind values once, at the public boundary, exactly as
// upstream does (workerd d1-api.ts). Byte-shaped values become byte arrays —
// the wire format for a BLOB bind — and anything unsupported throws
// D1_TYPE_ERROR here, before any SQL runs. The first version deferred this
// to a JSON round-trip whose fallthrough was SQL NULL, so a Uint8Array bind
// stored NULL and nothing said so.
const __d1BindValue = (value) => {
  const kind = typeof value;
  if (kind === "number") {
    // A non-finite number has no JSON form, so it crosses as null and
    // stores SQL NULL. Upstream does the same (bind(NaN) selects typeof
    // "null"); making it explicit here turns the conversion from an
    // accident of JSON.stringify into a pinned decision.
    return Number.isFinite(value) ? value : null;
  }
  if (kind === "string") return value;
  if (kind === "boolean") return value ? 1 : 0;
  if (kind === "object") {
    if (value === null) return value;
    if (
      Array.isArray(value) &&
      value.every((byte) => typeof byte === "number" && byte >= 0 && byte < 256)
    ) {
      return value;
    }
    if (value instanceof ArrayBuffer) {
      return Array.from(new Uint8Array(value));
    }
    // A typed view binds its ELEMENT values, each truncated to a byte —
    // Int8Array([-1]) stores 0xff, Float32Array([1.5]) stores 0x01 — not
    // its underlying byte window, so Uint16Array([65, 66]) stores 2 bytes
    // and not 4. Upstream behaves this way (verified against workerd's D1;
    // the differential suite pins it), and the bare Array.from that
    // preceded this produced elements the engine then refused as bytes.
    if (ArrayBuffer.isView(value)) return Array.from(Uint8Array.from(value));
  }
  const error = new Error(
    `D1_TYPE_ERROR: Type '${kind}' not supported for value '${value}'`,
  );
  error.cause = new Error(`Type '${kind}' not supported for value '${value}'`);
  throw error;
};

class D1PreparedStatement {
  constructor(database, sql, params) {
    Object.defineProperty(this, "_database", { value: database });
    Object.defineProperty(this, "_sql", { value: sql });
    Object.defineProperty(this, "_params", { value: params });
  }
  bind(...values) {
    return new D1PreparedStatement(
      this._database,
      this._sql,
      values.map(__d1BindValue),
    );
  }
  async _run(first) {
    const results = await this._database._query([
      { sql: this._sql, params: this._params, first },
    ]);
    return results[0];
  }
  async all() {
    const result = await this._run();
    return {
      success: true,
      meta: result.meta,
      results: result.rows.map((row) => __d1Shape(result.columns, row)),
    };
  }
  async run() {
    return await this.all();
  }
  async first(column) {
    // `first: true` keeps one row on the cell side, so only that row crosses
    // the isolate boundary and the row cap cannot fire for a first().
    const result = await this._run(true);
    if (result.rows.length === 0) return null;
    const row = __d1Shape(result.columns, result.rows[0]);
    if (column === undefined) return row;
    // The check runs on the shaped object, as upstream's does, so a column
    // named like an Object.prototype member behaves the same here as there.
    if (row[column] === undefined) {
      const error = new Error(
        `D1_COLUMN_NOTFOUND: Column not found (${column})`,
      );
      error.cause = new Error("Column not found");
      throw error;
    }
    return row[column];
  }
  async raw(options) {
    const result = await this._run();
    return options && options.columnNames
      ? [result.columns, ...result.rows]
      : result.rows;
  }
}

// Object.fromEntries, not property assignment: assigning to a column named
// `__proto__` would walk the prototype chain and silently drop the value,
// while fromEntries defines an own property whatever the name.
const __d1Shape = (columns, row) =>
  Object.fromEntries(row.map((value, index) => [columns[index], value]));

const __d1PublicResult = (result) => ({
  success: true,
  meta: result.meta,
  results: result.rows.map((row) => __d1Shape(result.columns, row)),
});

// celld has one primary and no replicas, so every completed query satisfies
// every earlier session constraint. One stable opaque token states that fact
// without pretending an isolate-local counter is a durable database position.
const __d1PrimaryBookmark = "celld:primary";

class D1DatabaseSession {
  constructor(database, constraintOrBookmark) {
    Object.defineProperty(this, "_database", { value: database });
    const value = constraintOrBookmark == null
      ? "first-unconstrained"
      : String(constraintOrBookmark).trim();
    if (!value) {
      throw new Error("D1_SESSION_ERROR: invalid bookmark or constraint");
    }
    Object.defineProperty(this, "_bookmark", {
      value: value === "first-primary" || value === "first-unconstrained" ? null : value,
      writable: true,
    });
  }
  _advanceBookmark() {
    this._bookmark = __d1PrimaryBookmark;
  }
  async _query(statements) {
    const results = await this._database._query(statements);
    this._advanceBookmark();
    return results;
  }
  prepare(sql) {
    return new D1PreparedStatement(this, String(sql), []);
  }
  async batch(statements) {
    const results = await this._database._batch(statements);
    this._advanceBookmark();
    return results;
  }
  getBookmark() {
    return this._bookmark;
  }
}

// Named so SDKs that sniff a binding by `constructor.name` recognise it.
class D1Database {
  constructor(databaseName) {
    Object.defineProperty(this, "_databaseName", { value: databaseName });
  }
  // Resolved per call rather than cached: a cell can move between calls, and
  // `getByName` is the same cost the Durable Object path already pays.
  get _stub() {
    return __cell.makeNamespace("__D1Database").getByName(this._databaseName);
  }
  async _query(statements) {
    try {
      return await this._stub.__d1Query(statements);
    } catch (error) {
      if (__d1IsFamilyError(error && error.message)) throw error;
      throw __d1Error(String(error && error.message || error), error);
    }
  }
  async _batch(statements) {
    try {
      const encoded = statements.map((statement) => ({
        sql: statement._sql,
        params: statement._params,
      }));
      const results = await this._stub.__d1Batch(encoded);
      return results.map(__d1PublicResult);
    } catch (error) {
      if (__d1IsFamilyError(error && error.message)) throw error;
      throw __d1Error(String(error && error.message || error), error);
    }
  }
  prepare(sql) {
    return new D1PreparedStatement(this, String(sql), []);
  }
  async exec(sql) {
    try {
      return await this._stub.__d1Exec(String(sql));
    } catch (error) {
      if (__d1IsFamilyError(error && error.message)) throw error;
      throw __d1Error(String(error && error.message || error), error);
    }
  }
  async batch(statements) {
    return await this._batch(statements);
  }
  withSession(constraintOrBookmark) {
    return new D1DatabaseSession(this, constraintOrBookmark);
  }
  async dump() {
    throw __d1Error(
      "dump() is not implemented in celld; the database is a SQLite file in " +
        "your own bucket",
    );
  }
}

globalThis.__makeD1Database = (databaseName) => new D1Database(databaseName);
// ---- Workflows ----------------------------------------------------------
// A workflow instance is one cell of the runtime-supplied `__Workflow` class,
// named `<workflow_name>/<instance_id>` through getByName, so it inherits
// ownership, fencing, LTX replication and the output gate from the cell it
// already is. Execution is replay: `run()` re-executes from the top on every
// resume and only steps are memoized, in `ctx.storage` KV under `__wf.*`.
// This half is the server; `__makeWorkflow` below is the client the binding
// hands to a Worker. The public contract is in docs/cloudflare-compat.md.

// Upstream's limit for a step return, an event payload, and the params. The
// error is distinct and names the step, because "too large" and "the isolate
// died" must not read the same (the D1 row-cap precedent).
const __WF_VALUE_CAP = 1 << 20;
// Workflow names, instance ids, and event types all share this charset.
// `/` is outside it, which is what makes the `<name>/<id>` cell-name join
// unambiguous.
const __WF_NAME_RE = /^[a-zA-Z0-9_][a-zA-Z0-9-_]*$/;
const __wfError = (message) => new Error("WORKFLOW_ERROR: " + message);
// The suspension primitive: a blocked step returns a promise that never
// settles in this invocation, so user code simply stops progressing. A
// sentinel thrown through user code was rejected because `Promise.all` in
// `run()` would observe it as a rejection and user try/catch would eat it.
const __wfNever = new Promise(() => {});
const __wfTerminal = (status) =>
  status === "complete" || status === "errored" || status === "terminated";
// Upstream's hard bounds: 10,000 retries per step, 365 days for any sleep or
// event wait, one second under a waitForEvent timeout. Enforced loudly at the
// call site -- a config outside them would otherwise misbehave only after
// the instance is minutes or days into its run.
const __WF_RETRY_LIMIT_CAP = 10000;
const __WF_STEP_NAME_LIMIT = 256;
const __WF_MAX_WAIT_MS = 365 * 86400000;
const __WF_MIN_EVENT_TIMEOUT_MS = 1000;
// How long run() can stay pending on non-step work while no step runs and
// none is blocked before the instance fails. Upstream permits un-stepped
// awaits between steps (they are merely non-durable), so erroring at the
// first quiescent macrotask would kill any run() whose external await
// outlives one turn of the loop. Sixty seconds is long enough for any
// sane un-stepped await and short enough that a truly never-settling
// promise fails the instance instead of wedging the alarm handler.
const __WF_STALL_GRACE_MS = 60000;
const __WF_UNITS = {
  second: 1000,
  minute: 60000,
  hour: 3600000,
  day: 86400000,
  week: 604800000,
  // Upstream does not document its month/year arithmetic; fixed 30/365-day
  // units are used so one expression cannot mean two durations depending on
  // when it runs.
  month: 30 * 86400000,
  year: 365 * 86400000,
};
const __wfDuration = (value, what) => {
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) return value;
  if (typeof value !== "string") {
    throw __wfError(
      `invalid duration for ${what}: ${JSON.stringify(value)}; use milliseconds ` +
        'or "<n> <second|minute|hour|day|week|month|year>[s]"',
    );
  }
  const match = /^\s*(\d+(?:\.\d+)?)\s+(second|minute|hour|day|week|month|year)s?\s*$/
    .exec(value);
  if (!match) {
    throw __wfError(
      `invalid duration for ${what}: ${JSON.stringify(value)}; use milliseconds ` +
        'or "<n> <second|minute|hour|day|week|month|year>[s]"',
    );
  }
  return Number(match[1]) * __WF_UNITS[match[2]];
};
const __wfOwnObject = (value) =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const __wfUnknownKey = (value, allowed) =>
  Object.keys(value).find((key) => !allowed.includes(key));
const __wfStepName = (name) => {
  // The pinned workers-sdk validator has no minimum length. An empty name is
  // therefore valid, although rejecting it looks safer at first glance. A
  // non-empty check made celld reject Worker code that the upstream runtime
  // accepts, so keep only the upstream length and control-character bounds.
  if (
    typeof name !== "string" || name.length > __WF_STEP_NAME_LIMIT ||
    /[\x00-\x1f]/.test(name)
  ) {
    throw __wfError(
      `invalid step name ${JSON.stringify(name)}: use at most ` +
        `${__WF_STEP_NAME_LIMIT} characters and no control characters`,
    );
  }
};
const __wfEventType = (type) => {
  if (typeof type !== "string" || type.length > 100 || !__WF_NAME_RE.test(type)) {
    throw __wfError(
      `invalid event type ${JSON.stringify(type)}: use at most 100 letters, ` +
        "digits, hyphens, or underscores",
    );
  }
};
const __wfSleepDuration = (duration, name) => {
  const durationMs = __wfDuration(
    duration,
    `the duration of sleep ${JSON.stringify(name)}`,
  );
  if (durationMs > __WF_MAX_WAIT_MS) {
    throw __wfError(
      `sleep ${JSON.stringify(name)} is ${durationMs} ms, above the upstream ` +
        "limit of 365 days",
    );
  }
  return durationMs;
};
const __wfSleepUntil = (timestamp, name) => {
  const at = timestamp instanceof Date ? timestamp.getTime() : timestamp;
  if (typeof at !== "number" || !Number.isFinite(at)) {
    throw __wfError(
      `invalid sleepUntil timestamp for ${JSON.stringify(name)}: use a Date ` +
        "or a UNIX timestamp in milliseconds",
    );
  }
  const delay = at - Date.now();
  if (delay > __WF_MAX_WAIT_MS) {
    throw __wfError(
      `sleepUntil ${JSON.stringify(name)} is ${delay} ms away, above the ` +
        "upstream limit of 365 days",
    );
  }
  return at;
};
const __wfWaitOptions = (options, name) => {
  if (!__wfOwnObject(options)) {
    throw __wfError(`invalid waitForEvent options for ${JSON.stringify(name)}`);
  }
  __wfEventType(options.type);
  const timeout = options.timeout ?? "24 hours";
  const timeoutMs = __wfDuration(
    timeout,
    `the timeout of waitForEvent ${JSON.stringify(name)}`,
  );
  if (timeoutMs < __WF_MIN_EVENT_TIMEOUT_MS || timeoutMs > __WF_MAX_WAIT_MS) {
    throw __wfError(
      `the timeout of waitForEvent ${JSON.stringify(name)} is ${timeoutMs} ms; ` +
        "upstream allows 1 second to 365 days",
    );
  }
  return { type: options.type, timeout, timeoutMs };
};
const __wfEventOptions = (options) => {
  if (!__wfOwnObject(options)) {
    throw __wfError("sendEvent() needs an event object");
  }
  __wfEventType(options.type);
  if (options.payload !== undefined) __wfCheckValue(options.payload, "the event payload");
  return { type: options.type, payload: options.payload };
};
const __wfRestartOptions = (options) => {
  if (options === undefined) return {};
  if (!__wfOwnObject(options)) {
    throw __wfError("restart() options must be an object");
  }
  const unknown = __wfUnknownKey(options, ["from"]);
  if (unknown !== undefined) {
    throw __wfError(`restart() has an unknown option ${JSON.stringify(unknown)}`);
  }
  if (options.from === undefined) return {};
  const from = options.from;
  if (!__wfOwnObject(from)) {
    throw __wfError("restart() option from must be an object");
  }
  const fromUnknown = __wfUnknownKey(from, ["name", "count", "type"]);
  if (fromUnknown !== undefined) {
    throw __wfError(
      `restart() option from has an unknown field ${JSON.stringify(fromUnknown)}`,
    );
  }
  __wfStepName(from.name);
  const count = from.count ?? 1;
  if (!Number.isInteger(count) || count < 1) {
    throw __wfError("restart() option from.count must be a positive integer");
  }
  const type = from.type ?? "do";
  if (!["do", "sleep", "waitForEvent"].includes(type)) {
    throw __wfError(
      'restart() option from.type must be "do", "sleep", or "waitForEvent"',
    );
  }
  return { from: { name: from.name, count, type } };
};
const __wfStepConfig = (config, name) => {
  if (!__wfOwnObject(config)) {
    throw __wfError(`invalid config for step ${JSON.stringify(name)}: use an object`);
  }
  let unknown = __wfUnknownKey(config, ["retries", "timeout", "sensitive"]);
  if (unknown !== undefined) {
    throw __wfError(
      `invalid config for step ${JSON.stringify(name)}: unknown option ` +
        JSON.stringify(unknown),
    );
  }
  const retries = config.retries;
  if (retries !== undefined) {
    if (!__wfOwnObject(retries)) {
      throw __wfError(`invalid retries config for step ${JSON.stringify(name)}`);
    }
    unknown = __wfUnknownKey(retries, ["limit", "delay", "backoff"]);
    if (unknown !== undefined) {
      throw __wfError(
        `invalid retries config for step ${JSON.stringify(name)}: unknown option ` +
          JSON.stringify(unknown),
      );
    }
    if (
      typeof retries.limit !== "number" || !Number.isFinite(retries.limit) ||
      retries.limit < 0
    ) {
      throw __wfError(
        `invalid retries config for step ${JSON.stringify(name)}: ` +
          "limit must be a non-negative number",
      );
    }
    if (retries.limit > __WF_RETRY_LIMIT_CAP) {
      throw __wfError(
        `invalid retries config for step ${JSON.stringify(name)}: limit is above ` +
          String(__WF_RETRY_LIMIT_CAP),
      );
    }
    if (typeof retries.delay !== "function") {
      __wfDuration(retries.delay, `the retries config delay of step ${JSON.stringify(name)}`);
    }
    if (
      retries.backoff !== undefined &&
      !["constant", "linear", "exponential"].includes(retries.backoff)
    ) {
      throw __wfError(
        `invalid retries config for step ${JSON.stringify(name)}: ` +
          "backoff must be constant, linear, or exponential",
      );
    }
  }
  if (config.timeout !== undefined) {
    const timeout = __wfDuration(
      config.timeout,
      `the config timeout of step ${JSON.stringify(name)}`,
    );
    if (timeout === 0) {
      throw __wfError(
        `invalid config timeout for step ${JSON.stringify(name)}: use a value above zero`,
      );
    }
  }
  if (config.sensitive !== undefined && config.sensitive !== "output") {
    throw __wfError(
      `invalid config sensitive value for step ${JSON.stringify(name)}: use "output"`,
    );
  }
  return {
    ...config,
    ...(retries === undefined ? {} : { retries: { ...retries } }),
  };
};
// Serialization is checked with the same encoder storage and RPC use, so a
// value that passes here cannot fail later at the isolate boundary. The
// thrown errors are permanent: retrying cannot make a value cloneable or
// shrink it.
const __wfEncodeValue = (value, what) => {
  try {
    return __sc_encode(value);
  } catch (error) {
    const failure = __wfError(
      `${what} is not serializable: ${String(error && error.message || error)}. ` +
        "It must survive structured clone; a ReadableStream return is not implemented in celld.",
    );
    failure.__wfPermanent = true;
    throw failure;
  }
};
const __wfCheckEncodedValue = (bytes, what) => {
  if (bytes.byteLength > __WF_VALUE_CAP) {
    const failure = __wfError(
      `${what} is ${bytes.byteLength} bytes, above the 1 MiB limit; ` +
        "store large output externally and return a reference",
    );
    failure.__wfPermanent = true;
    throw failure;
  }
};
const __wfCheckValue = (value, what) =>
  __wfCheckEncodedValue(__wfEncodeValue(value, what), what);
const __wfErrorRecord = (error) => ({
  name: String(error && error.name || "Error"),
  message: String(error && error.message || error),
});
// A permanently failed step rejects on every replay, so user try/catch
// around step.do keeps working after a resume, not only on the attempt that
// failed live.
const __wfReviveError = (record) => {
  const error = new Error(record.message);
  error.name = record.name;
  return error;
};
const __wfLedgerPrefix = (generation) => `__wf.${generation}.`;
const __wfEventPrefix = (generation) =>
  __wfLedgerPrefix(generation) + "event.";
const __wfEventKey = (generation, sequence) =>
  __wfEventPrefix(generation) + String(sequence).padStart(9, "0");
// A ledger record replays only through the kind of call that wrote it. A
// `do` renamed into a `sleep`, or two calls reordered under one name, would
// otherwise read fields the other kind never wrote and continue on
// undefined -- a deadline of NaN, a value of nothing -- instead of failing.
const __wfKindCheck = (record, kind, name) => {
  if (record === undefined || record.kind === kind) return record;
  const failure = __wfError(
    `step ${JSON.stringify(name)} replays as ${kind} but its ledger record ` +
      `was written by ${record.kind}; a step keeps one kind across replays`,
  );
  failure.__wfPermanent = true;
  throw failure;
};
// Each step callback runs under an async-context frame naming its step (the
// same CPED-backed frame __ctxRun uses, so it survives the callback's
// awaits). A step started inside another step's callback would wedge
// quiescence: the outer callback cannot settle while it awaits the blocked
// inner step, so `running` never reaches zero and the instance can neither
// suspend nor finish. The frame is what lets the inner call refuse loudly
// at the nesting site instead.
const __wfStepKey = Symbol("celld.wfStep");
const __wfEnclosingStep = () => {
  const frame = __als_get();
  return frame === undefined ? undefined : frame.get(__wfStepKey);
};
const __wfEnterStep = (name, fn) => {
  const prior = __als_get();
  const frame = new Map(prior);
  frame.set(__wfStepKey, name);
  __als_set(frame);
  try {
    return fn();
  } finally {
    __als_set(prior);
  }
};

// Race an attempt against its deadline. The abandoned callback keeps
// running -- there is no cancellation to send it -- but its result is
// discarded and the timeout counts as a failed attempt, as upstream.
const __wfTimeout = (promise, ms, name, attempt) =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(
      () =>
        reject(new Error(
          `step ${JSON.stringify(name)} timed out after ${ms} ms (attempt ${attempt})`,
        )),
      ms,
    );
    promise.then(
      (value) => { clearTimeout(timer); resolve(value); },
      (error) => { clearTimeout(timer); reject(error); },
    );
  });

// Storage failures belong to the engine even when workflow code catches the
// rejected step promise. Record them outside the Error object, so user code
// cannot suppress a retry or forge one by returning a marked exception.
const __wfTrackStorageFailures = (storage, driver) => {
  const tracked = Object.create(storage);
  for (const name of ["get", "put", "transactionSync"]) {
    Object.defineProperty(tracked, name, {
      value(...args) {
        try {
          const result = storage[name](...args);
          if (result !== null && typeof result === "object" &&
            typeof result.then === "function") {
            return result.catch((error) => {
              driver.storageFailure ??= error;
              throw error;
            });
          }
          return result;
        } catch (error) {
          driver.storageFailure ??= error;
          throw error;
        }
      },
    });
  }
  return tracked;
};

// The WorkflowStep the driver hands to run(). Each method resolves its step
// record by (name, occurrence count) -- the count is assigned synchronously
// at call time, so `Promise.all` of two same-named steps gets deterministic
// distinct keys in call order, and a repeated name in a loop cannot collide.
const __wfMakeStep = (driver) => {
  const storage = driver.storage;
  // The shared step frame. `body` resolves to {value} or {block: at, kind};
  // a blocked step leaves its promise unsettled forever in this invocation.
  // The running counter covers the whole body, ledger reads included, so
  // quiescence cannot fire between a step's read and its decision.
  const frame = (name, body) => {
    // A stale replay -- one whose invocation already suspended -- can wake
    // later if user code raced a step against an un-stepped await. It goes
    // quiet instead of executing an attempt beside the fresh replay.
    if (driver.finished) return __wfNever;
    const enclosing = __wfEnclosingStep();
    if (enclosing !== undefined) {
      const failure = __wfError(
        `step ${JSON.stringify(name)} cannot start inside the callback of ` +
          `step ${JSON.stringify(enclosing)}; a step callback must not use ` +
          "the step API",
      );
      failure.__wfPermanent = true;
      return Promise.reject(failure);
    }
    const count = (driver.counts.get(name) ?? 0) + 1;
    driver.counts.set(name, count);
    const ordinal = ++driver.ordinal;
    const key = driver.ledgerPrefix + `step.${count}.${name}`;
    // Step activity restarts the stall clock: a pending grace timer was
    // armed against a run() that looked stuck, and this call is progress.
    driver.clearStall();
    driver.running += 1;
    return new Promise((resolve, reject) => {
      (async () => {
        const meta = await storage.get("__wf.meta");
        if (
          meta === undefined || meta.generation !== driver.generation ||
          __wfTerminal(meta.status) || meta.status === "paused" ||
          meta.status === "waitingForPause"
        ) {
          return { pause: true };
        }
        return await body(key, count, ordinal);
      })().then(
        (result) => {
          driver.running -= 1;
          driver.clearStall();
          if (result.pause === true) {
            driver.pauseRequested = true;
            driver.check();
            return;
          }
          if (result.block !== undefined) {
            if (!driver.finished) {
              driver.blocked.push({ at: result.block, kind: result.kind });
            }
            driver.check();
            return;
          }
          driver.check();
          resolve(result.value);
        },
        (error) => {
          driver.running -= 1;
          driver.clearStall();
          driver.check();
          reject(error);
        },
      );
    });
  };
  const doStep = (name, configOrCallback, callbackOrRollback, maybeRollback) => {
    const hasConfig = typeof configOrCallback !== "function";
    let config = hasConfig ? (configOrCallback ?? {}) : {};
    const callback = hasConfig ? callbackOrRollback : configOrCallback;
    const rollback = hasConfig ? maybeRollback : callbackOrRollback;
    try {
      __wfStepName(name);
      config = __wfStepConfig(config, name);
    } catch (error) {
      return Promise.reject(error);
    }
    if (rollback !== undefined) {
      return Promise.reject(__wfError(
        "step rollbackOptions are not implemented in celld; remove the rollback handler",
      ));
    }
    if (config.sensitive !== undefined) {
      return Promise.reject(__wfError(
        "sensitive step output is not implemented in celld; remove `sensitive`",
      ));
    }
    if (typeof callback !== "function") {
      return Promise.reject(__wfError("step.do needs a callback function"));
    }
    return frame(name, async (key, count, ordinal) => {
      const record = __wfKindCheck(await storage.get(key), "do", name);
      if (record !== undefined && record.status === "completed") {
        return { value: record.value };
      }
      if (record !== undefined && record.status === "failed") {
        throw __wfReviveError(record.error);
      }
      // A pending retry whose backoff has not elapsed blocks without
      // executing; the deadline was persisted when the attempt failed, so a
      // replay cannot reset the backoff.
      if (record !== undefined && record.nextAt > Date.now()) {
        return { block: record.nextAt, kind: "retry" };
      }
      const attempt = (record === undefined ? 0 : record.attempt) + 1;
      const historyOrdinal = record?.ordinal ?? ordinal;
      const retries = config.retries ??
        { limit: 5, delay: 10000, backoff: "exponential" };
      const timeout = config.timeout ?? "10 minutes";
      const timeoutMs = __wfDuration(
        timeout,
        `the timeout of step ${JSON.stringify(name)}`,
      );
      // Upstream omits a dynamic delay from the step context. Copying the
      // config verbatim would expose the callback to itself as retry policy,
      // while callers expect only the serializable limit and backoff fields.
      const contextRetries = typeof retries.delay === "function"
        ? { limit: retries.limit, ...(retries.backoff === undefined
          ? {}
          : { backoff: retries.backoff }) }
        : retries;
      const context = {
        step: { name, count },
        attempt,
        config: { retries: contextRetries, timeout },
      };
      try {
        // Persist the history entry before the callback starts. A concurrent
        // selected restart can therefore name an in-flight occurrence, and
        // its new generation fences the callback's eventual old-ledger write.
        await storage.put(key, {
          kind: "do",
          status: "running",
          attempt: attempt - 1,
          ordinal: historyOrdinal,
        });
        const value = await __wfTimeout(
          __wfEnterStep(name, async () => callback(context)),
          timeoutMs,
          name,
          attempt,
        );
        if (value !== undefined) {
          __wfCheckValue(value, `the return value of step ${JSON.stringify(name)}`);
        }
        await storage.put(key, {
          kind: "do",
          status: "completed",
          value,
          ordinal: historyOrdinal,
        });
        return { value };
      } catch (error) {
        // NonRetryableError skips retries by contract; a value-cap or
        // serialization failure is marked permanent above because another
        // attempt returns the same value.
        const permanent = error instanceof NonRetryableError ||
          (error !== null && typeof error === "object" &&
            (error.name === "NonRetryableError" || error.__wfPermanent === true)) ||
          attempt > retries.limit;
        if (permanent) {
          await storage.put(key, {
            kind: "do",
            status: "failed",
            error: __wfErrorRecord(error),
            ordinal: historyOrdinal,
          });
          throw error;
        }
        // Upstream documents the backoff names, not the arithmetic; the
        // simplest reading is used and recorded in the design page. When
        // `retries` is given without `backoff`, celld uses constant -- the
        // documented exponential default is the whole-config-omitted case.
        const dynamic = typeof retries.delay === "function";
        const delay = dynamic
          ? await __wfEnterStep(name, () => retries.delay({ ctx: context, error }))
          : retries.delay;
        const base = __wfDuration(
          delay,
          `the retry delay of step ${JSON.stringify(name)}`,
        );
        // A delay callback replaces the fixed base duration, not the backoff
        // policy. Skipping this factor makes a dynamic exponential or linear
        // policy retry earlier than the same policy with a fixed base.
        const factor = retries.backoff === "exponential"
          ? 2 ** (attempt - 1)
          : retries.backoff === "linear"
          ? attempt
          : 1;
        const nextAt = Date.now() + base * factor;
        await storage.put(key, {
          kind: "do",
          status: "retrying",
          attempt,
          nextAt,
          ordinal: historyOrdinal,
        });
        return { block: nextAt, kind: "retry" };
      }
    });
  };
  const sleepUntilDeadline = (name, key, ordinal, deadline) => (async () => {
    const record = __wfKindCheck(await storage.get(key), "sleep", name);
    if (record !== undefined && record.status === "completed") {
      return { value: undefined };
    }
    const historyOrdinal = record?.ordinal ?? ordinal;
    let at = record === undefined ? undefined : record.deadline;
    if (at === undefined) {
      // The absolute deadline is computed once and persisted. Recomputing
      // from "now" on replay would extend the sleep by however long the
      // instance was down, so a crash could postpone a deadline forever.
      at = deadline();
      if (at - Date.now() > __WF_MAX_WAIT_MS) {
        throw __wfError(
          `sleep ${JSON.stringify(name)} ends ${at - Date.now()} ms from now, ` +
            "above the upstream limit of 365 days",
        );
      }
      await storage.put(key, {
        kind: "sleep",
        status: "sleeping",
        deadline: at,
        ordinal: historyOrdinal,
      });
    }
    if (at <= Date.now()) {
      await storage.put(key, {
        kind: "sleep",
        status: "completed",
        ordinal: historyOrdinal,
      });
      return { value: undefined };
    }
    return { block: at, kind: "sleep" };
  })();
  return {
    do: doStep,
    sleep: (name, duration) => {
      let durationMs;
      try {
        __wfStepName(name);
        durationMs = __wfSleepDuration(duration, name);
      } catch (error) {
        return Promise.reject(error);
      }
      return frame(name, (key, _count, ordinal) =>
        sleepUntilDeadline(
          name,
          key,
          ordinal,
          () => Date.now() + durationMs,
        ));
    },
    sleepUntil: (name, timestamp) => {
      let at;
      try {
        __wfStepName(name);
        at = __wfSleepUntil(timestamp, name);
      } catch (error) {
        return Promise.reject(error);
      }
      return frame(name, (key, _count, ordinal) =>
        sleepUntilDeadline(name, key, ordinal, () => {
          // A replay uses the persisted deadline after it has passed. Check
          // the moving future bound only while the deadline is first installed,
          // or every successful sleep would fail on the resume that completes it.
          if (at - Date.now() < 0) {
            throw __wfError(
              `sleepUntil ${JSON.stringify(name)} needs a future timestamp`,
            );
          }
          return at;
        }));
    },
    waitForEvent: (name, options) => {
      let checked;
      try {
        __wfStepName(name);
        checked = __wfWaitOptions(options, name);
      } catch (error) {
        return Promise.reject(error);
      }
      return frame(name, async (key, _count, ordinal) => {
        let record = __wfKindCheck(await storage.get(key), "event", name);
        if (record !== undefined && record.status === "completed") {
          return { value: record.value };
        }
        if (record !== undefined && record.status === "failed") {
          throw __wfReviveError(record.error);
        }
        if (record === undefined) {
          record = {
            kind: "event",
            status: "waiting",
            type: checked.type,
            deadline: Date.now() + checked.timeoutMs,
            ordinal,
          };
          await storage.put(key, record);
        }
        // The deadline is checked before the buffer: an event that arrived
        // after the persisted deadline must time the step out, not succeed
        // because the replay that noticed it ran late.
        if (record.deadline <= Date.now()) {
          const error = new Error(
            `waitForEvent ${JSON.stringify(name)} timed out waiting for an event ` +
              `of type ${JSON.stringify(record.type)}`,
          );
          await storage.put(key, {
            kind: "event",
            status: "failed",
            error: __wfErrorRecord(error),
            ordinal: record.ordinal,
          });
          throw error;
        }
        // Delivered events persist until a matching step consumes one, in
        // arrival order -- the zero-padded sequence key makes list order
        // arrival order -- so an event sent before the step is reached is
        // buffered, not lost.
        const consumed = storage.transactionSync((transaction) => {
          const current = __wfKindCheck(transaction.kv.get(key), "event", name);
          if (current !== undefined && current.status === "completed") {
            return { value: current.value };
          }
          if (current !== undefined && current.status === "failed") {
            throw __wfReviveError(current.error);
          }
          for (const [eventKey, event] of transaction.kv.list({
            prefix: __wfEventPrefix(driver.generation),
          })) {
            if (event.type !== current.type) continue;
            const value = {
              payload: event.payload,
              timestamp: new Date(event.timestampMs),
              type: event.type,
            };
            // The delete and completed ledger record are one SQLite commit.
            // A crash can therefore expose both or neither, so an
            // acknowledged event cannot disappear between them.
            transaction.kv.delete(eventKey);
            /*__CELLD_TEST_WORKFLOW_EVENT_CONSUMED__*/
            transaction.kv.put(key, {
              kind: "event",
              status: "completed",
              value,
              ordinal: current.ordinal,
            });
            return { value };
          }
        });
        if (consumed !== undefined) return consumed;
        return { block: record.deadline, kind: "event" };
      });
    },
  };
};

const __WorkflowCell = (() => {
  class StorageTransaction {
    constructor(storage, transaction) {
      this.kv = transaction.kv;
      this._scope = storage._scope;
    }
    setAlarm(t) {
      __alarm_set(this._scope, t instanceof Date ? t.getTime() : Number(t));
    }
    deleteAlarm() {
      __alarm_delete(this._scope);
    }
  }
  const transactionSync = (storage, callback) =>
    storage.transactionSync((transaction) =>
      callback(new StorageTransaction(storage, transaction))
    );

  // The adapter stays in this closure because Workflow needs synchronous
  // alarm writes inside transactionSync(). The public async alarm API runs
  // after that commit, while a global helper would let application code
  // split metadata from its wake or invoke the raw operation after commit.
  return class __WorkflowCell {
  constructor(state) {
    this._state = state;
  }
  async __wfCreate({ workflowName, instanceId, params, skipExisting = false }) {
    const storage = this._state.storage;
    if (params !== undefined) __wfCheckValue(params, "the workflow params");
    const result = transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta !== undefined && skipExisting) return { id: instanceId, created: false };
      if (meta !== undefined && !__wfTerminal(meta.status)) {
        throw __wfError(
          `instance ${JSON.stringify(instanceId)} already exists with status ` +
            JSON.stringify(meta.status),
        );
      }
      if (meta !== undefined) {
        for (const [key] of transaction.kv.list({
          prefix: __wfLedgerPrefix(meta.generation),
        })) {
          transaction.kv.delete(key);
        }
      }
      const generation = crypto.randomUUID();
      transaction.kv.put("__wf.meta", {
        workflowName,
        instanceId,
        params,
        generation,
        createdMs: Date.now(),
        status: "queued",
      });
      // The metadata and alarm are one SQLite commit. A committed creation
      // therefore always has the wake that can start or recover its drive.
      transaction.setAlarm(Date.now());
      /*__CELLD_TEST_WORKFLOW_META_CREATED__*/
      return { id: instanceId, created: true };
    });
    return result;
  }
  async __wfStatus() {
    const meta = await this._state.storage.get("__wf.meta");
    // A cell exists on first address like any cell, so an absent ledger is
    // the only honest "no such instance" signal. Inventing an instance here
    // would turn every typo into an empty workflow.
    if (meta === undefined) throw __wfError("instance does not exist");
    const status = { status: meta.status, rollback: null };
    if (meta.error !== undefined) status.error = meta.error;
    if (meta.output !== undefined) status.output = meta.output;
    return status;
  }
  async __wfPause() {
    const storage = this._state.storage;
    transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) throw __wfError("instance does not exist");
      if (__wfTerminal(meta.status) || meta.status === "paused" ||
          meta.status === "waitingForPause") return;
      if (meta.status === "running") {
        meta.status = "waitingForPause";
        // A drive lost after this commit must re-enter and finish the pause.
        transaction.setAlarm(Date.now());
      } else {
        meta.status = "paused";
        meta.pausedMs = Date.now();
        transaction.deleteAlarm();
      }
      transaction.kv.put("__wf.meta", meta);
    });
  }
  async __wfResume() {
    const storage = this._state.storage;
    transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) throw __wfError("instance does not exist");
      if (meta.status === "waitingForPause") {
        meta.status = "running";
        transaction.kv.put("__wf.meta", meta);
        transaction.setAlarm(Date.now());
        return;
      }
      if (meta.status !== "paused") {
        throw __wfError(
          `cannot resume an instance with status ${JSON.stringify(meta.status)}`,
        );
      }
      const offset = Math.max(0, Date.now() - (meta.pausedMs ?? Date.now()));
      for (const [key, record] of transaction.kv.list({
        prefix: __wfLedgerPrefix(meta.generation) + "step.",
      })) {
        if (record.status === "retrying") record.nextAt += offset;
        if (record.status === "sleeping" || record.status === "waiting") {
          record.deadline += offset;
        }
        transaction.kv.put(key, record);
      }
      delete meta.pausedMs;
      meta.status = "queued";
      transaction.kv.put("__wf.meta", meta);
      transaction.setAlarm(Date.now());
    });
  }
  async __wfRestart(options) {
    const checked = __wfRestartOptions(options);
    const storage = this._state.storage;
    transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) throw __wfError("instance does not exist");
      const oldPrefix = __wfLedgerPrefix(meta.generation);
      let targetOrdinal;
      if (checked.from !== undefined) {
        const expectedKind = checked.from.type === "waitForEvent"
          ? "event"
          : checked.from.type;
        let occurrence = 0;
        const history = [...transaction.kv.list({ prefix: oldPrefix + "step." })]
          .filter(([, record]) => Number.isInteger(record.ordinal))
          .sort((left, right) => left[1].ordinal - right[1].ordinal);
        // The ledger key is `<prefix>step.<per-name-count>.<name>`. Derive
        // the name from it because a record stores replay state, not identity.
        for (const [key, record] of history) {
          const marker = oldPrefix + "step.";
          const suffix = key.slice(marker.length);
          const dot = suffix.indexOf(".");
          const name = dot < 0 ? "" : suffix.slice(dot + 1);
          if (record.kind !== expectedKind || name !== checked.from.name) continue;
          occurrence += 1;
          if (occurrence === checked.from.count) {
            targetOrdinal = record.ordinal;
            break;
          }
        }
        if (targetOrdinal === undefined) {
          throw __wfError(
            `restart() could not find ${checked.from.type} step ` +
              `${JSON.stringify(checked.from.name)} occurrence ${checked.from.count}`,
          );
        }
      }
      const generation = crypto.randomUUID();
      const newPrefix = __wfLedgerPrefix(generation);
      if (targetOrdinal !== undefined) {
        for (const [key, record] of transaction.kv.list({ prefix: oldPrefix + "step." })) {
          if (Number.isInteger(record.ordinal) && record.ordinal < targetOrdinal) {
            transaction.kv.put(newPrefix + key.slice(oldPrefix.length), record);
          }
        }
      }
      for (const [key] of transaction.kv.list({ prefix: oldPrefix })) {
        transaction.kv.delete(key);
      }
      meta.generation = generation;
      meta.status = "queued";
      delete meta.pausedMs;
      delete meta.output;
      delete meta.error;
      transaction.kv.put("__wf.meta", meta);
      transaction.setAlarm(Date.now());
    });
  }
  async __wfTerminate() {
    const storage = this._state.storage;
    transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) throw __wfError("instance does not exist");
      if (__wfTerminal(meta.status)) {
        throw __wfError(
          `cannot terminate an instance with status ${JSON.stringify(meta.status)}`,
        );
      }
      meta.status = "terminated";
      transaction.kv.put("__wf.meta", meta);
      transaction.deleteAlarm();
      /*__CELLD_TEST_WORKFLOW_ALARM_DELETED__*/
    });
  }
  async __wfSendEvent(options) {
    const storage = this._state.storage;
    // The binding validates first to avoid an unnecessary cell call. Repeat
    // the check here because this RPC owns the durable event invariant: no
    // current or future internal caller can bypass validation before storage.
    const { type, payload } = __wfEventOptions(options);
    transactionSync(storage, (transaction) => {
      const meta = transaction.kv.get("__wf.meta");
      if (meta === undefined) throw __wfError("instance does not exist");
      if (__wfTerminal(meta.status)) {
        throw __wfError(
          `cannot send an event to an instance with status ${JSON.stringify(meta.status)}`,
        );
      }
      const sequenceKey = __wfLedgerPrefix(meta.generation) + "eventSeq";
      const sequence = transaction.kv.get(sequenceKey) ?? 0;
      transaction.kv.put(sequenceKey, sequence + 1);
      transaction.kv.put(__wfEventKey(meta.generation, sequence), {
        type,
        payload,
        timestampMs: Date.now(),
      });
      // Allocation, the event body, and the nudge are one commit. Parallel
      // writers cannot reuse a sequence and a crash cannot expose a partial
      // acknowledged send.
      if (meta.status !== "paused" && meta.status !== "waitingForPause") {
        transaction.setAlarm(Date.now());
      }
    });
  }
  async alarm() {
    const storage = this._state.storage;
    // One drive at a time. A sendEvent nudge (setAlarm(now)) landing
    // mid-fire replaces the firing alarm in the core, and the replacement
    // can dispatch a second alarm() into this isolate while the first drive
    // still awaits user code. Two concurrent replays would both see the
    // same incomplete steps and double-execute their callbacks. The guard
    // is isolate-local and that is sufficient: every drive for a cell runs
    // in its owner's isolate, and a takeover starts a fresh isolate with no
    // drive in flight. The nudge is not lost -- the in-flight drive folds
    // `_wfNudge` into the minimum it re-arms, and if it suspended before
    // seeing the flag, the re-arm below starts a fresh drive immediately.
    // A failed drive is not re-raised here: the engine already retries the
    // alarm that fired it.
    if (this._wfDriving !== undefined) {
      this._wfNudge = Date.now();
      await this._wfDriving.catch(() => {});
      if (this._wfNudge !== undefined) {
        this._wfNudge = undefined;
        const meta = await storage.get("__wf.meta");
        if (
          meta !== undefined && !__wfTerminal(meta.status) &&
          meta.status !== "paused" && meta.status !== "waitingForPause"
        ) {
          await storage.setAlarm(Date.now());
        }
      }
      return;
    }
    const meta = await storage.get("__wf.meta");
    // A terminal instance's pending alarm can still fire; seeing the status
    // and doing nothing is the whole terminate-race policy.
    if (meta === undefined || __wfTerminal(meta.status)) {
      await storage.deleteAlarm();
      return;
    }
    if (meta.status === "paused") {
      await storage.deleteAlarm();
      return;
    }
    if (meta.status === "waitingForPause") {
      transactionSync(storage, (transaction) => {
        const current = transaction.kv.get("__wf.meta");
        if (current === undefined || current.generation !== meta.generation ||
            current.status !== "waitingForPause") return;
        current.status = "paused";
        current.pausedMs = Date.now();
        transaction.kv.put("__wf.meta", current);
        transaction.deleteAlarm();
      });
      return;
    }
    // The fired alarm stays armed while the drive runs. The engine consumes
    // it only when this handler returns cleanly, and retries it when the
    // handler fails -- that retry is the crash safety net. Deleting it here
    // would commit the deletion durably mid-drive (durability is per SQLite
    // commit, and replication ships frames per commit), so a SIGKILL before
    // the suspend-time re-arm would strand a "running" instance with no
    // alarm and no wake entry. The suspend path re-arms explicitly, and a
    // terminal drive deletes.
    const driving = this._drive(meta);
    this._wfDriving = driving;
    try {
      await driving;
    } finally {
      this._wfDriving = undefined;
    }
  }
  async _drive(meta) {
    const storage = this._state.storage;
    // The drive owns `status`, `output`, and `error`; every other piece of
    // instance state belongs to the RPCs (sendEvent's sequence lives in its
    // own key for the same reason). The gate is open at every await, so a
    // terminate can land mid-drive: re-reading before each write, and
    // writing nothing once the status is terminal, is what makes "terminate
    // wins the race" true rather than asserted. Writing back the meta this
    // drive entered with would resurrect a terminated instance as "waiting"
    // and re-arm the alarm its terminate deleted.
    const settle = (mutate) => transactionSync(storage, (transaction) => {
      const current = transaction.kv.get("__wf.meta");
      if (current === undefined || __wfTerminal(current.status) ||
          current.generation !== meta.generation) return false;
      mutate(current, transaction);
      transaction.kv.put("__wf.meta", current);
      return true;
    });
    const className = __cell.workflows[meta.workflowName];
    const cls = className === undefined ? undefined : __cf.exports[className];
    if (typeof cls !== "function" ||
        typeof (cls.prototype && cls.prototype.run) !== "function") {
      // Deploy cannot see inside the bundle, so a missing export fails
      // here, loudly, with the class and the config key named.
      settle((current, transaction) => {
        current.status = "errored";
        current.error = {
          name: "Error",
          message: className === undefined
            ? `workflow ${JSON.stringify(meta.workflowName)} is not declared ` +
              "in this deployment's `workflows`"
            : `workflow class ${JSON.stringify(className)} is not a module ` +
              "export with a run() method; export it and extend " +
              "WorkflowEntrypoint",
        };
        transaction.deleteAlarm();
        /*__CELLD_TEST_WORKFLOW_ALARM_DELETED__*/
      });
      return;
    }
    const driver = {
      storage: undefined,
      storageFailure: undefined,
      generation: meta.generation,
      ledgerPrefix: __wfLedgerPrefix(meta.generation),
      counts: new Map(),
      ordinal: 0,
      running: 0,
      blocked: [],
      pauseRequested: false,
      finished: false,
      checkQueued: false,
      stallTimer: undefined,
      clearStall() {
        if (this.stallTimer !== undefined) {
          clearTimeout(this.stallTimer);
          this.stallTimer = undefined;
        }
      },
    };
    driver.storage = __wfTrackStorageFailures(storage, driver);
    const outcome = new Promise((resolve) => {
      driver.finish = (result) => {
        if (driver.finished) return;
        driver.finished = true;
        driver.clearStall();
        resolve(result);
      };
    });
    // Quiescence: when a step is blocked, no step callback is executing,
    // and a macrotask has passed, nothing can progress, so the invocation
    // suspends. The macrotask lets every microtask chain a completed step
    // unblocked drain first; checking on the spot would suspend an instance
    // whose next step is one `await` away from starting.
    driver.check = () => {
      if (driver.checkQueued || driver.finished) return;
      driver.checkQueued = true;
      setTimeout(() => {
        driver.checkQueued = false;
        if (driver.finished || driver.running !== 0) return;
        if (driver.pauseRequested) {
          driver.finish({ kind: "pause" });
          return;
        }
        if (driver.blocked.length > 0) {
          driver.finish({ kind: "suspend" });
          return;
        }
        // No step runs, none is blocked, and run() has not settled: it
        // awaits something that is not a step. Upstream permits that -- an
        // un-stepped `await fetch()` between steps is legal, merely
        // non-durable -- so this is not yet an error; but a promise that
        // never settles would wedge the alarm handler in silence, with the
        // instance "running" forever. A grace timer splits the two: if the
        // same nothing-can-progress state still holds when it fires, the
        // instance fails loudly. The timer is a plain in-isolate timeout,
        // not a durable alarm, on purpose: an eviction or a restart replays
        // run() from the top anyway, so losing the timer loses nothing --
        // the replay re-enters the same stall and re-arms its own grace.
        if (driver.stallTimer !== undefined) return;
        driver.stallTimer = setTimeout(() => {
          driver.stallTimer = undefined;
          if (driver.finished || driver.running !== 0 || driver.blocked.length > 0) {
            return;
          }
          driver.finish({
            kind: "error",
            error: __wfError(
              "run() has been awaiting something that is not a step for " +
                (__WF_STALL_GRACE_MS / 1000) + " seconds while no step is " +
                "running or blocked; a replay cannot resume this wait, so " +
                "wrap long async work in step.do",
            ),
          });
        }, __WF_STALL_GRACE_MS);
      }, 0);
    };
    if (
      meta.status !== "running" &&
      !(await settle((current) => {
        current.status = "running";
      }))
    ) {
      return;
    }
    const step = __wfMakeStep(driver);
    const event = {
      payload: meta.params,
      timestamp: new Date(meta.createdMs),
      instanceId: meta.instanceId,
      workflowName: meta.workflowName,
    };
    // ExecutionContext-shaped, as the WorkflowEntrypoint constructor
    // documents upstream.
    const ctx = {
      waitUntil: globalThis.__registerWaitUntil,
      passThroughOnException() {},
    };
    (async () => new cls(ctx, __cell.env).run(event, step))().then(
      (value) => driver.finish({ kind: "complete", value }),
      (error) => driver.finish({ kind: "error", error }),
    );
    /*__CELLD_TEST_WORKFLOW_AFTER_RUN_STARTED__*/
    let result = await outcome;
    // The workflow can catch a rejected step promise, but it cannot turn an
    // engine storage failure into a successful run. Preserve the alarm so
    // the runtime retries over the last committed ledger prefix.
    if (driver.storageFailure !== undefined) {
      throw driver.storageFailure;
    }
    const currentMeta = await storage.get("__wf.meta");
    if (
      result.kind === "pause" ||
      (currentMeta !== undefined &&
        currentMeta.generation === meta.generation &&
        currentMeta.status === "waitingForPause")
    ) {
      /*__CELLD_TEST_WORKFLOW_BEFORE_PAUSE_SETTLE__*/
      settle((current, transaction) => {
        // A resume can cancel waitingForPause after this drive selected its
        // pause result. Preserve that newer running state and its alarm, or
        // this stale settlement strands an acknowledged resume as paused.
        if (current.status !== "waitingForPause") return;
        current.status = "paused";
        current.pausedMs = Date.now();
        transaction.deleteAlarm();
      });
      return;
    }
    if (result.kind === "suspend") {
      let at = Math.min(...driver.blocked.map((entry) => entry.at));
      // A nudge that landed mid-drive must not wait out the pending
      // deadline. It is visible in one of two places: getAlarm(), when the
      // nudge re-armed and no second alarm dispatched yet, or `_wfNudge`,
      // when a second alarm() already entered and parked on the drive
      // guard -- its dispatch consumed the armed alarm, so getAlarm()
      // alone would miss it.
      const nudge = await storage.getAlarm();
      if (nudge !== null) at = Math.min(at, nudge);
      if (this._wfNudge !== undefined) {
        at = Math.min(at, this._wfNudge);
        this._wfNudge = undefined;
      }
      settle((current, transaction) => {
        current.status = "waiting";
        transaction.setAlarm(at);
      });
      // The explicit re-arm replaces the alarm that fired this drive.
      // Durability is per SQLite commit, not per turn: a crash can persist
      // any prefix of this invocation's ledger writes, and the unconsumed
      // fired alarm is what re-drives the replay over whatever prefix
      // survived. A terminated instance re-arms nothing; its leftover
      // alarm fires once, sees the terminal status, and retires itself.
      return;
    }
    if (result.kind === "complete") {
      try {
        if (result.value !== undefined) {
          __wfCheckValue(result.value, "the run() return value");
        }
      } catch (error) {
        result = { kind: "error", error };
      }
      if (result.kind === "complete") {
        // Keep settlement outside the value-validation catch. A storage
        // failure must escape the alarm handler so the engine retries the
        // still-armed drive; converting it to a user error would consume the
        // alarm and make a transient commit failure terminal.
        settle((current, transaction) => {
          current.status = "complete";
          if (result.value !== undefined) current.output = result.value;
          // A concurrent nudge may have re-armed mid-run. Settlement retires
          // it in the same commit that makes the instance terminal.
          transaction.deleteAlarm();
          /*__CELLD_TEST_WORKFLOW_ALARM_DELETED__*/
        });
        return;
      }
    }
    settle((current, transaction) => {
      current.status = "errored";
      current.error = __wfErrorRecord(result.error);
      transaction.deleteAlarm();
      /*__CELLD_TEST_WORKFLOW_ALARM_DELETED__*/
    });
  }
  };
})();
__cell.classes.__Workflow = __WorkflowCell;
// RPC on a stub needs `extends DurableObject` or the js_rpc flag. This class
// is the runtime's own, so grant it here, as __D1Database does.
__cell.doExports.__Workflow = true;

class NonRetryableError extends Error {
  constructor(message, name = "NonRetryableError") {
    super(message);
    this.name = name;
  }
}
// Backing object for the `cloudflare:workflows` builtin module.
globalThis.__cfWorkflows = { NonRetryableError };

const __wfValidInstanceId = (id) =>
  typeof id === "string" && id.length >= 1 && id.length <= 100 &&
  __WF_NAME_RE.test(id);
const __wfCreateOptions = (options, what, validateParams = true) => {
  if (!__wfOwnObject(options)) {
    throw __wfError(`${what} needs an object`);
  }
  // These are published options, not unknown extension fields. Reject them
  // until their semantics exist so celld cannot silently promise retention or
  // placement. The generated binding surfaces ignore other object fields.
  if (options.retention !== undefined) {
    throw __wfError(`${what} does not support option "retention"`);
  }
  if (options.locationHint !== undefined) {
    throw __wfError(`${what} does not support option "locationHint"`);
  }
  const id = options.id === undefined ? crypto.randomUUID() : options.id;
  if (!__wfValidInstanceId(id)) {
    throw __wfError(
      `invalid instance id ${JSON.stringify(id)}: use letters, digits, ` +
        "\"-\" and \"_\" (not starting with \"-\"), at most 100 characters",
    );
  }
  if (validateParams && options.params !== undefined) {
    // The cell repeats this serialization check because it owns the durable
    // create invariant. A single create rejects before it addresses the cell.
    __wfCheckValue(options.params, "the workflow params");
  }
  return { id, params: options.params };
};

// Named so SDKs that sniff a binding by `constructor.name` recognise it.
class WorkflowInstance {
  constructor(workflowName, id) {
    Object.defineProperty(this, "_workflowName", { value: workflowName });
    this.id = id;
  }
  // Resolved per call rather than cached: a cell can move between calls.
  get _stub() {
    // The reserved class is script-scoped (`deploy::workflow_class`), because
    // a workflow instance is: two co-hosted scripts must not share one entry
    // in the deployment's flat class registry. `__cell.script` is injected
    // before any binding runs, so this resolves at call time.
    return __cell.makeNamespace("__Workflow." + __cell.script)
      .getByName(this._workflowName + "/" + this.id);
  }
  async status() {
    return await this._stub.__wfStatus();
  }
  async terminate(options) {
    if (options && options.rollback) {
      throw __wfError(
        "terminate({rollback: true}) is not implemented in celld; step " +
          "rollbackOptions are not implemented, so no handler could run",
      );
    }
    await this._stub.__wfTerminate();
  }
  async sendEvent(options) {
    await this._stub.__wfSendEvent(__wfEventOptions(options));
  }
  async pause() {
    await this._stub.__wfPause();
  }
  async resume() {
    await this._stub.__wfResume();
  }
  async restart(options) {
    await this._stub.__wfRestart(__wfRestartOptions(options));
  }
  async delete() {
    throw __wfError(
      "delete() is not implemented in celld yet; the instance state stays in " +
        "the cell until a delete surface exists",
    );
  }
}

class Workflow {
  constructor(workflowName) {
    Object.defineProperty(this, "_workflowName", { value: workflowName });
  }
  async _create(options, skipExisting) {
    const { id, params } = options;
    const instance = new WorkflowInstance(this._workflowName, id);
    const result = await instance._stub.__wfCreate({
      workflowName: this._workflowName,
      instanceId: id,
      params,
      skipExisting,
    });
    return result.created ? instance : undefined;
  }
  async create(options = {}) {
    return await this._create(__wfCreateOptions(options, "create()"), false);
  }
  async createBatch(batch) {
    if (!Array.isArray(batch)) {
      throw __wfError("createBatch() needs an array of create options");
    }
    if (batch.length > 100) {
      throw __wfError(
        `createBatch() accepts at most 100 instances, got ${batch.length}`,
      );
    }
    if (batch.length === 0) {
      throw __wfError("createBatch() needs at least one instance");
    }
    // Cloudflare rejects a structural error in any item before it creates the
    // first instance, but it filters a params clone failure per item. Keep
    // these two gates separate so a bad ID stays atomic while a clone failure
    // does not suppress the valid items around it.
    const optionsList = batch.map((options, index) =>
      __wfCreateOptions(options, `createBatch() item ${index}`, false));
    const creatable = [];
    for (const options of optionsList) {
      let encoded;
      try {
        if (options.params !== undefined) {
          encoded = __wfEncodeValue(options.params, "the workflow params");
        }
      } catch {
        continue;
      }
      if (encoded !== undefined) {
        __wfCheckEncodedValue(encoded, "the workflow params");
      }
      creatable.push(options);
    }
    const created = [];
    for (const options of creatable) {
      const instance = await this._create(options, true);
      if (instance !== undefined) created.push(instance);
    }
    return created;
  }
  async get(id) {
    if (!__wfValidInstanceId(id)) {
      throw __wfError(`invalid instance id ${JSON.stringify(id)}`);
    }
    const instance = new WorkflowInstance(this._workflowName, id);
    // The cell exists on first address like any cell, but get() must not
    // mint an instance for a typo: an id with no ledger rejects here.
    await instance._stub.__wfStatus();
    return instance;
  }
  async deleteBatch() {
    throw __wfError("deleteBatch() is not implemented in celld yet");
  }
}

globalThis.__makeWorkflow = (workflowName) => new Workflow(workflowName);
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
  // Base class for `workflows` classes. The reserved workflow cell
  // instantiates a subclass on every replay with an ExecutionContext-shaped
  // ctx -- never the cell's own DurableObjectState, which would hand user
  // code the ledger that replays it.
  WorkflowEntrypoint: class WorkflowEntrypoint {
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
// Proxy standing in for unsupported node:*/cloudflare:* builtins. Property
// walks stay inert — real bundles reference these at module scope, and
// evaluation must not crash on a builtin the fetch path never exercises —
// but a call or construct throws: the compat contract is "reject at first
// use", and the old silent pass-through turned a missing builtin into a
// wrong result far from the cause. Memoized by dotted path so repeated
// reads keep identity (`mod.foo === mod.foo`).
const __stubCache = new Map();
globalThis.__nodeStubFor = (path) => {
  let stub = __stubCache.get(path);
  if (stub) return stub;
  stub = new Proxy(function () {}, {
    get: (_t, p) => {
      // Never masquerade as a thenable. `await` probes `.then`; returning
      // a callable here creates a promise that can never settle.
      if (p === "then") return undefined;
      // coerce cleanly in string/number contexts so evaluation never throws
      if (p === Symbol.toPrimitive || p === "toString" || p === "valueOf")
        return () => "";
      if (p === Symbol.toStringTag) return "NodeStub";
      if (p === Symbol.iterator) return function* () {};
      if (typeof p !== "string") return undefined;
      return globalThis.__nodeStubFor(path + "." + p);
    },
    apply() { throw new Error(path + " is not implemented in celld"); },
    construct() { throw new Error(path + " is not implemented in celld"); },
  });
  __stubCache.set(path, stub);
  return stub;
};
globalThis.__nodeStub = globalThis.__nodeStubFor("node builtin");
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
// An event-handler IDL attribute occupies one listener-list position. Replacing
// its callback must retain that position, or listeners added after `onabort`
// run before it and observe an order that differs from the web platform.
const __eventHandlerEntry = Symbol("eventHandlerEntry");
const __getEventHandler = (target, type) =>
  (target._listeners.get(type) || [])
    .find((entry) => entry[__eventHandlerEntry])?.callback ?? null;
const __setEventHandler = (target, type, callback) => {
  const list = target._listeners.get(type) || [];
  const entry = list.find((item) => item[__eventHandlerEntry]);
  if (typeof callback !== "function") {
    if (entry) list.splice(list.indexOf(entry), 1);
    return;
  }
  if (entry) {
    entry.callback = callback;
  } else {
    list.push({ callback, once: false, [__eventHandlerEntry]: true });
    target._listeners.set(type, list);
  }
};
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
      const hasEventHandler = (this._listeners.get(event.type) || [])
        .some((item) => item[__eventHandlerEntry]);
      if (!hasEventHandler && !event._stopImmediate &&
          typeof handler === "function")
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
    // The legacy `code` member maps the error name to its constant, per
    // WebIDL; names outside the table (and every modern name) are 0.
    const legacy = {
      IndexSizeError: 1, HierarchyRequestError: 3, WrongDocumentError: 4,
      InvalidCharacterError: 5, NoModificationAllowedError: 7,
      NotFoundError: 8, NotSupportedError: 9, InUseAttributeError: 10,
      InvalidStateError: 11, SyntaxError: 12, InvalidModificationError: 13,
      NamespaceError: 14, InvalidAccessError: 15, TypeMismatchError: 17,
      SecurityError: 18, NetworkError: 19, AbortError: 20,
      URLMismatchError: 21, QuotaExceededError: 22, TimeoutError: 23,
      InvalidNodeTypeError: 24, DataCloneError: 25,
    };
    Object.defineProperty(globalThis.DOMException.prototype, "code", {
      get() { return legacy[this.name] ?? 0; },
      enumerable: true, configurable: true,
    });
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
  Object.defineProperty(globalThis.AbortSignal.prototype, "onabort", {
    configurable: true,
    enumerable: true,
    get() {
      if (!(this instanceof globalThis.AbortSignal))
        throw new TypeError("Illegal invocation");
      return __getEventHandler(this, "abort");
    },
    set(value) {
      if (!(this instanceof globalThis.AbortSignal))
        throw new TypeError("Illegal invocation");
      __setEventHandler(this, "abort", value);
    },
  });
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
// Byte offsets the multipart scanner compares against. A part body is
// arbitrary bytes, so every framing decision below is made on bytes.
const __CR = 13, __LF = 10, __DASH = 45;
const __bytesIndexOf = (haystack, needle, from) => {
  const last = haystack.length - needle.length;
  outer: for (let i = from; i <= last; i++) {
    for (let j = 0; j < needle.length; j++)
      if (haystack[i + j] !== needle[j]) continue outer;
    return i;
  }
  return -1;
};
// A boundary delimiter is a delimiter only at the start of a line (RFC 2046
// section 5.1.1). Searching for the bytes anywhere would let a binary part
// that happens to contain `--<boundary>` split itself, which is a corrupt
// upload rather than a parse error, so the caller never learns of it.
const __delimiterIndex = (bytes, delimiter, from) => {
  for (let at = from;;) {
    const found = __bytesIndexOf(bytes, delimiter, at);
    if (found < 0) return -1;
    if (found === 0 || bytes[found - 1] === __LF) return found;
    at = found + 1;
  }
};
// A `Content-Disposition` parameter, anchored to the start of a parameter:
// the parameter name must follow the header name or a `;`. An unanchored
// pattern found the `name=` inside `filename=`, so a part whose header wrote
// the filename first landed under the filename. The grammar does not
// constrain the parameter order, and both orders occur in the wild. Hoisted
// because the previous shape compiled a regex per parameter per part.
const __CD_NAME =
  /(?:^|;)\s*name\s*=\s*(?:"((?:[^"\\]|\\.)*)"|([^;]*))/i;
const __CD_FILENAME =
  /(?:^|;)\s*filename\s*=\s*(?:"((?:[^"\\]|\\.)*)"|([^;]*))/i;
// Body -> FormData for multipart/form-data and
// application/x-www-form-urlencoded. Kept out of the class so V8 only
// pre-parses it; the body is compiled on first actual formData() call.
//
// `bytes` is the undecoded body. A file part must reach `File` as the bytes
// that arrived: decoding the body first and slicing part bodies out of the
// string replaced every non-UTF-8 sequence with U+FFFD, and the re-encode in
// the `Blob` constructor then wrote those replacement bytes into the file.
globalThis.__parseFormData = (bytes, contentType) => {
  const ct = String(contentType || "");
  if (/^\s*application\/x-www-form-urlencoded/i.test(ct)) {
    // This form is decoded as UTF-8, so a declared charset that is not UTF-8
    // cannot be honoured. Refuse rather than mis-decode.
    const charset = /;\s*charset\s*=\s*"?([^";]+)/i.exec(ct);
    if (charset && !/^utf-?8$/i.test(charset[1].trim()))
      throw new TypeError(
        `Unsupported charset "${charset[1].trim()}". FormData can only ` +
        "parse UTF-8 encoded bodies.");
    const form = new FormData();
    for (const [key, value] of new URLSearchParams(
      new TextDecoder().decode(bytes)))
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
  const delimiter = new TextEncoder().encode("--" + boundary);
  // Walk the delimiters rather than splitting on them, so that a body whose
  // framing is truncated or corrupt is refused instead of silently yielding
  // the parts that happened to parse. Only the framing is checked; a part's
  // own bytes are carried through unexamined. The preamble before the first
  // delimiter and the epilogue after the closing one are ignored.
  let cursor = __delimiterIndex(bytes, delimiter, 0);
  if (cursor < 0)
    throw new TypeError(
      "No boundary delimiter was found in the multipart/form-data body.");
  cursor += delimiter.length;
  for (;;) {
    // What follows a delimiter decides everything: "--" closes the body, a
    // line break opens another part, and anything else is malformed.
    if (bytes[cursor] === __DASH && bytes[cursor + 1] === __DASH) break;
    const lineBreak = bytes[cursor] === __CR && bytes[cursor + 1] === __LF
      ? 2
      : bytes[cursor] === __LF ? 1 : 0;
    if (lineBreak === 0)
      throw new TypeError(
        "A boundary delimiter must be followed by CRLF, LF, or \"--\".");
    cursor += lineBreak;
    const partStart = cursor;
    const next = __delimiterIndex(bytes, delimiter, partStart);
    if (next < 0)
      throw new TypeError(
        "The multipart/form-data body ended without a closing boundary " +
        "delimiter.");
    // The line break before a delimiter belongs to the delimiter, not to the
    // part it terminates.
    let chunkEnd = next;
    if (chunkEnd > partStart && bytes[chunkEnd - 1] === __LF) {
      chunkEnd -= 1;
      if (chunkEnd > partStart && bytes[chunkEnd - 1] === __CR) chunkEnd -= 1;
    }
    cursor = next + delimiter.length;
    // The headers end at the first blank line. Either line break can be CRLF
    // or a bare LF, which accepted inputs can contain.
    let headerEnd = -1, bodyStart = -1;
    for (let i = partStart; i < chunkEnd; i++) {
      const first = bytes[i] === __CR && bytes[i + 1] === __LF
        ? 2
        : bytes[i] === __LF ? 1 : 0;
      if (first === 0) continue;
      const after = i + first;
      const second = bytes[after] === __CR && bytes[after + 1] === __LF
        ? 2
        : bytes[after] === __LF ? 1 : 0;
      if (second === 0) continue;
      headerEnd = i;
      bodyStart = after + second;
      break;
    }
    if (headerEnd < 0)
      throw new TypeError(
        "A FormData part's headers must be terminated by a blank line.");
    // Part headers are text. Decoding this slice as UTF-8 keeps a non-ASCII
    // name or filename intact and cannot reach the part body.
    const rawHeaders = new TextDecoder().decode(
      bytes.subarray(partStart, headerEnd));
    const body = bytes.subarray(bodyStart, chunkEnd);
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
    const param = (pattern) => {
      const m = pattern.exec(disposition);
      if (!m) return undefined;
      const raw = m[1] !== undefined ? m[1] : (m[2] || "").trim();
      if (/\\$/.test(raw.replace(/\\\\/g, "")))
        throw new TypeError(
          "Name or filename can't end with backslash");
      return raw.replace(/\\(.)/g, "$1");
    };
    const name = param(__CD_NAME);
    if (name === undefined)
      throw new TypeError(
        "Content-Disposition header for FormData part must have a " +
        "name parameter.");
    const filename = param(__CD_FILENAME);
    // A part without a filename is an entry value, which the spec defines as a
    // string, so this one part is decoded as UTF-8. A file part keeps its
    // bytes.
    if (filename === undefined)
      form.append(name, new TextDecoder().decode(body));
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
// clobber a real prelude implementation). Enough to LOAD; every name in
// this first list has a real implementation elsewhere, so the empty class
// only exists for the load-order window.
for (const n of ["WebSocket", "EventTarget", "Event", "MessageEvent",
  "CloseEvent", "ErrorEvent", "SubtleCrypto", "TransformStream",
  "ReadableStream", "WritableStream", "ReadableStreamDefaultReader",
  "WritableStreamDefaultWriter",
  "ByteLengthQueuingStrategy", "CountQueuingStrategy",
  "TextEncoderStream", "TextDecoderStream"]) {
  if (!globalThis[n]) globalThis[n] = class {};
}
// These have no implementation anywhere in celld. An empty class would
// let `new BroadcastChannel(...)` hand back an inert object that fails
// far from the cause; the compat contract is "reject at first use", so
// the name loads but construction throws. (EventSource, MessageChannel,
// and MessagePort are real; their scripts run after the harness.)
for (const n of ["BroadcastChannel", "FileReader"]) {
  if (!globalThis[n]) globalThis[n] = class {
    constructor() {
      throw new Error(n + " is not implemented in celld");
    }
  };
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
  constructor(id, protocols = [], boundTarget = null) {
    super();
    const dialed = typeof id === "string";
    // A socket bound to a Durable Object's socket behaves like a dialed one
    // in every way that matters here: the host carries its frames, so every
    // operation goes out through `__ws_*` on this id rather than to a peer
    // in this heap.
    const outbound = dialed || boundTarget !== null;
    this._outbound = outbound;
    this._boundTarget = boundTarget;
    this._bound = false;
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
    // string -- workerd's inspect output pins this. A socket that came back
    // from an upgrade has none either: nothing in this isolate dialed it.
    this.url = dialed ? id : null;
    this.protocol = "";
    this.extensions = "";
    if (boundTarget !== null) {
      // The caller drives this socket itself, exactly as a Worker drives one
      // it opened. The host pipe is not built until `accept()`, so a
      // response passed straight back out is left alone.
      this._polled = true;
      this._target = boundTarget;
    }
    if (dialed) {
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
      const scope = __currentActorScope();
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
  // socket lives, and the host reclaims an abandoned socket only once the
  // request has retired. Its `__ws_next` ops are driven while the request
  // runs and normally resolve with the host's 1001 "request ended" close.
  // They resolve as 1006 only if the sender disappears without a close frame.
  _startPump() {
    if (this._pumping) return;
    this._pumping = true;
    this._pump().catch(() => {});
  }
  // Drain the host queue for an isolate-polled socket. Each `__ws_next` is an
  // ordinary async op the request owns; a request that retires with the
  // socket still open closes it, which ends this loop with a close frame.
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
      // Bind before the pump: the op registers this socket's queue on this
      // thread, so the first `__ws_next` cannot run ahead of it.
      if (this._boundTarget !== null && !this._bound) {
        this._bound = true;
        __ws_bind_target(
          this._id,
          JSON.stringify(this._boundTarget),
          __currentActorScope(),
        );
      }
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
// Cloudflare exposes the time of the last I/O, not a clock that advances
// while JavaScript runs. Keep Date and performance on one turn timestamp so
// neither becomes a timing side channel and the two APIs cannot disagree.
const __NativeDate = Date;
let __ioTimestamp = 0;
const __CloudflareDate = function Date(...args) {
  if (new.target) {
    return Reflect.construct(
      __NativeDate,
      args.length === 0 ? [__ioTimestamp] : args,
      new.target,
    );
  }
  return new __NativeDate(__ioTimestamp).toString();
};
Object.defineProperties(__CloudflareDate, {
  length: { value: 7 },
  prototype: { value: __NativeDate.prototype },
  now: {
    value: () => __ioTimestamp,
    writable: true,
    configurable: true,
  },
  parse: {
    value: __NativeDate.parse,
    writable: true,
    configurable: true,
  },
  UTC: {
    value: __NativeDate.UTC,
    writable: true,
    configurable: true,
  },
});
Object.defineProperty(__NativeDate.prototype, "constructor", {
  value: __CloudflareDate,
  writable: true,
  configurable: true,
});
globalThis.Date = __CloudflareDate;
globalThis.performance = {
  now: () => __ioTimestamp,
  timeOrigin: 0,
  mark() {},
  measure() {},
};
Object.defineProperty(globalThis, "__advanceIoTime", {
  value(timestamp) {
    __ioTimestamp = timestamp;
  },
});
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
  const structuredCloneRandom = $$randomValues;
  globalThis.structuredClone = (value, options) => {
    const transfer = options?.transfer === undefined
      ? [] : Array.from(options.transfer);
    const seen = new Set();
    for (let index = 0; index < transfer.length; index++) {
      const item = transfer[index];
      if (!(item instanceof ArrayBuffer))
        throw new DOMException(
          `Value at index ${index} is not transferable`, "DataCloneError");
      if (item.detached)
        throw new DOMException(
          `ArrayBuffer at index ${index} is already detached`,
          "DataCloneError");
      if (seen.has(item))
        throw new DOMException(
          `ArrayBuffer at index ${index} is a duplicate`, "DataCloneError");
      seen.add(item);
    }

    // Blob, File, CryptoKey, and DOMException are JavaScript-backed in Cells,
    // so V8 does not see the native host objects that Deno registers with its
    // serializer. Replace only those objects before the V8 pass, then restore
    // them after it. A per-call random token prevents an application object
    // from being mistaken for a temporary record after deserialization.
    const hostMarker = "__celldStructuredCloneHost";
    const hostTokenBytes = new Uint32Array(4);
    structuredCloneRandom(hostTokenBytes);
    const hostToken = Array.from(hostTokenBytes).join(":");
    const projected = new Map();
    const passesThroughV8 = (input) =>
      input instanceof Date || input instanceof RegExp ||
      input instanceof Error || input instanceof ArrayBuffer ||
      input instanceof WebAssembly.Module ||
      (typeof SharedArrayBuffer === "function" &&
       input instanceof SharedArrayBuffer) ||
      ArrayBuffer.isView(input);
    const project = (input) => {
      if (input === null || typeof input !== "object") return input;
      if (projected.has(input)) return projected.get(input);

      let record;
      if (typeof File === "function" && input instanceof File) {
        record = { [hostMarker]: hostToken, kind: "File" };
        projected.set(input, record);
        record.bytes = input._bytes;
        record.type = input.type;
        record.name = input.name;
        record.lastModified = input.lastModified;
        return record;
      }
      if (typeof Blob === "function" && input instanceof Blob) {
        record = { [hostMarker]: hostToken, kind: "Blob" };
        projected.set(input, record);
        record.bytes = input._bytes;
        record.type = input.type;
        return record;
      }
      if (typeof CryptoKey === "function" && input instanceof CryptoKey) {
        record = { [hostMarker]: hostToken, kind: "CryptoKey" };
        projected.set(input, record);
        record.type = input.type;
        record.algorithm = input.algorithm;
        record.extractable = input.extractable;
        record.usages = input.usages;
        record.material = input.__celldMaterial;
        return record;
      }
      if (input instanceof DOMException) {
        record = { [hostMarker]: hostToken, kind: "DOMException" };
        projected.set(input, record);
        record.message = input.message;
        record.name = input.name;
        return record;
      }
      // These types do not have the Web IDL Serializable marker. Reject them
      // instead of exposing their JavaScript implementation as a plain object.
      if ((typeof URL === "function" && input instanceof URL) ||
          (typeof URLSearchParams === "function" &&
           input instanceof URLSearchParams) ||
          (typeof Headers === "function" && input instanceof Headers) ||
          (typeof FormData === "function" && input instanceof FormData) ||
          (typeof Request === "function" && input instanceof Request) ||
          (typeof Response === "function" && input instanceof Response) ||
          (typeof MessagePort === "function" && input instanceof MessagePort))
        throw new DOMException(
          "Cannot clone object of unsupported type.", "DataCloneError");
      if (Array.isArray(input)) {
        record = new Array(input.length);
        projected.set(input, record);
        for (const key of Object.keys(input)) record[key] = project(input[key]);
        return record;
      }
      if (input instanceof Map) {
        record = new Map();
        projected.set(input, record);
        for (const [key, entry] of input)
          record.set(project(key), project(entry));
        return record;
      }
      if (input instanceof Set) {
        record = new Set();
        projected.set(input, record);
        for (const entry of input) record.add(project(entry));
        return record;
      }
      if (passesThroughV8(input)) return input;

      record = {};
      projected.set(input, record);
      for (const key of Object.keys(input)) record[key] = project(input[key]);
      return record;
    };

    // Deno delegates the general graph to V8 and normalizes its clone error.
    // Cells already exposes the V8 value serializer for storage and RPC, so
    // one encode/decode preserves native internal slots, cycles, and shared
    // backing stores without a second JavaScript object walker.
    let result;
    try {
      result = __structured_clone(project(value));
    } catch (error) {
      if (error instanceof TypeError)
        throw new DOMException(error.message, "DataCloneError");
      throw error;
    }

    const revived = new Map();
    const revive = (input) => {
      if (input === null || typeof input !== "object") return input;
      if (revived.has(input)) return revived.get(input);

      let output;
      switch (input[hostMarker] === hostToken ? input.kind : undefined) {
        case "Blob":
          output = new Blob([input.bytes], { type: input.type });
          revived.set(input, output);
          return output;
        case "File":
          output = new File([input.bytes], input.name, {
            type: input.type, lastModified: input.lastModified,
          });
          revived.set(input, output);
          return output;
        case "CryptoKey":
          output = new CryptoKey(
            input.type, input.algorithm, input.extractable,
            input.usages, input.material);
          revived.set(input, output);
          return output;
        case "DOMException":
          output = new DOMException(input.message, input.name);
          revived.set(input, output);
          return output;
      }
      revived.set(input, input);
      if (Array.isArray(input)) {
        for (const key of Object.keys(input)) input[key] = revive(input[key]);
      } else if (input instanceof Map) {
        const entries = Array.from(input);
        input.clear();
        for (const [key, entry] of entries)
          input.set(revive(key), revive(entry));
      } else if (input instanceof Set) {
        const entries = Array.from(input);
        input.clear();
        for (const entry of entries) input.add(revive(entry));
      } else if (!passesThroughV8(input)) {
        for (const key of Object.keys(input)) input[key] = revive(input[key]);
      }
      return input;
    };
    result = revive(result);
    // Validate the complete list and clone the value before detaching any
    // source. A later invalid entry therefore cannot leave an earlier buffer
    // detached after the operation fails.
    for (const item of transfer) item.transfer();
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
  const actorScope = __currentActorScope();
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
const __entrypointContext = (props = __defaultProps) => ({
  waitUntil: globalThis.__registerWaitUntil,
  passThroughOnException() {},
  abort: __ctxAbortCurrent,
  props,
  get exports() { return __ctxExports(); },
});
globalThis.__beginEvent = (props = __defaultProps) => {
  __event_begin();
  return __entrypointContext(props);
};
globalThis.__endEvent = () => __event_end();
// Workerd's writable filesystem is memory-backed and request-scoped. The
// native IoContext owns the directory tree; this facade supplies Node's path,
// error, Promise, and Stats shapes without exposing a host filesystem.
const __fsPath = (value) => {
  if (value instanceof URL) {
    if (value.protocol !== "file:") {
      const error = new TypeError('The URL must be of scheme file');
      error.code = "ERR_INVALID_URL_SCHEME";
      throw error;
    }
    value = decodeURIComponent(value.pathname);
  } else if (globalThis.Buffer?.isBuffer(value)) {
    value = value.toString();
  } else if (typeof value !== "string") {
    const error = new TypeError(
      'The "path" argument must be of type string or an instance of Buffer or URL',
    );
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  if (value.includes("\0")) {
    const error = new TypeError('The argument "path" must not contain null bytes');
    error.code = "ERR_INVALID_ARG_VALUE";
    throw error;
  }
  const absolute = value.startsWith("/") ? value : process.cwd() + "/" + value;
  const parts = [];
  for (const part of absolute.split("/")) {
    if (!part || part === ".") continue;
    if (part === "..") parts.pop();
    else parts.push(part);
  }
  return "/" + parts.join("/");
};
const __fsError = (code, syscall, path) => {
  const e = new Error(`${code}: ${syscall} '${path}'`);
  e.errno = { EPERM: -1, ENOENT: -2, EEXIST: -17, ENOTDIR: -20 }[code];
  e.code = code;
  e.syscall = syscall;
  e.path = path;
  return e;
};
const __enoent = (path = "") => { throw __fsError("ENOENT", "open", path); };
const __fsStatsBadge = Symbol("fs.Stats");
class __FsStats {
  constructor(badge, kind, size, bigint) {
    if (badge !== __fsStatsBadge) throw new TypeError("Illegal constructor");
    const file = kind === 3;
    this.dev = 0;
    this.ino = 0;
    // A bundle file is 0o100444 and a writable temporary directory is 0o40666,
    // matching the mode values workerd's fs-stat-test.js asserts.
    this.mode = (file ? 0o100000 : 0o40000) | 0o444 | (kind === 2 ? 0o222 : 0);
    this.nlink = 1;
    this.uid = 0;
    this.gid = 0;
    this.rdev = 0;
    this.size = size;
    this.blksize = 0;
    this.blocks = 0;
    this.atimeMs = this.mtimeMs = this.ctimeMs = this.birthtimeMs = 0;
    this.atime = new Date(0);
    this.mtime = new Date(0);
    this.ctime = new Date(0);
    this.birthtime = new Date(0);
    Object.defineProperty(this, "__kind", { value: file ? "file" : "directory" });
    if (bigint) {
      for (const name of [
        "dev", "ino", "mode", "nlink", "uid", "gid", "rdev", "size",
        "blksize", "blocks", "atimeMs", "mtimeMs", "ctimeMs", "birthtimeMs",
      ]) this[name] = BigInt(this[name]);
      this.atimeNs = 0n;
      this.mtimeNs = 0n;
      this.ctimeNs = 0n;
      this.birthtimeNs = 0n;
    }
  }
  isFile() { return this.__kind === "file"; }
  isDirectory() { return this.__kind === "directory"; }
  isBlockDevice() { return false; }
  isCharacterDevice() { return false; }
  isSymbolicLink() { return false; }
  isFIFO() { return false; }
  isSocket() { return false; }
}
const __fsOptions = (options, names) => {
  if (options === undefined) return {};
  if (options === null || typeof options !== "object") {
    const error = new TypeError('The "options" argument must be of type object');
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  for (const name of names) {
    if (options[name] !== undefined && typeof options[name] !== "boolean") {
      const error = new TypeError(`The "options.${name}" property must be of type boolean`);
      error.code = "ERR_INVALID_ARG_TYPE";
      throw error;
    }
  }
  return options;
};
const __fsMkdirSync = (value, options = {}) => {
  const path = __fsPath(value);
  if (typeof options === "number") options = { mode: options };
  options = __fsOptions(options, ["recursive"]);
  const result = __vfs_mkdir(path, options.recursive === true);
  if (result && !result.startsWith("/")) throw __fsError(result, "mkdir", path);
  return result || undefined;
};
const __fsStatSync = (value, options, syscall = "stat") => {
  const path = __fsPath(value);
  options = __fsOptions(options, ["bigint", "throwIfNoEntry"]);
  const [kind, size] = __vfs_stat(path);
  if (!kind) {
    if (options.throwIfNoEntry === false) return undefined;
    throw __fsError("ENOENT", syscall, path);
  }
  return new __FsStats(__fsStatsBadge, kind, size, options.bigint === true);
};
const __fsLstatSync = (value, options) => __fsStatSync(value, options, "lstat");
const __fsRealpathSync = (value) => {
  const path = __fsPath(value);
  if (!__vfs_stat(path)[0]) throw __fsError("ENOENT", "realpath", path);
  return path;
};
const __fsExistsSync = (value) => {
  try { return __vfs_stat(__fsPath(value))[0] !== 0; }
  catch { return false; }
};
// Node returns a Buffer unless an encoding is given, as a string or as
// `options.encoding`. Only `utf8` is decoded here; any other encoding falls
// through to Buffer's own decoder.
const __fsReadFileSync = (value, options) => {
  const path = __fsPath(value);
  const encoding = typeof options === "string" ? options : options?.encoding;
  const bytes = __vfs_read_file(path);
  if (bytes === undefined) {
    // A directory read fails differently from a missing path, so the stat
    // decides which error the caller sees.
    if (__vfs_stat(path)[0]) throw __fsError("EISDIR", "read", path);
    throw __fsError("ENOENT", "open", path);
  }
  const buffer = Buffer.from(bytes);
  return encoding ? buffer.toString(encoding) : buffer;
};
// `fs.constants`, with the four access modes from workerd's
// `src/node/internal/internal_fs_constants.ts`. The table carries only the
// modes `access` reads. Every other constant belongs to an operation the
// virtual filesystem does not implement, and that operation already fails
// loudly at the unimplemented-builtin stub, so publishing its constants would
// only make a caller build a mode for a call it cannot make.
const __fsConstants = Object.freeze({ F_OK: 0, X_OK: 1, W_OK: 2, R_OK: 4 });
// Node validates a mode before it reaches the filesystem, so a bad mode is an
// argument error and never a filesystem error. The range is the union of the
// four access modes, which is what workerd's `validateMode` allows.
const __fsMode = (mode) => {
  if (mode === undefined) return __fsConstants.F_OK;
  if (typeof mode !== "number") {
    const error = new TypeError('The "mode" argument must be of type number');
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  if (!Number.isInteger(mode) || mode < 0 || mode > 7) {
    const error = new RangeError(
      `The value of "mode" is out of range. It must be >= 0 && <= 7. Received ${mode}`,
    );
    error.code = "ERR_OUT_OF_RANGE";
    throw error;
  }
  return mode;
};
// `access` reports what the virtual filesystem can do and not a POSIX
// permission, so it follows workerd's `accessSyncImpl`: no file is executable,
// therefore `X_OK` always fails, and an unwritable path fails exactly like a
// missing one, so a caller cannot tell the two apart. Kind 2 is the writable
// tree; see `op_vfs_stat`.
const __fsAccessSync = (value, mode) => {
  const path = __fsPath(value);
  mode = __fsMode(mode);
  const [kind] = __vfs_stat(path);
  const denied = mode & __fsConstants.X_OK
    || !kind
    || (mode & __fsConstants.W_OK && kind !== 2);
  if (denied) throw __fsError("ENOENT", "access", path);
};
// The callback forms run the same synchronous core as `fs.promises`.
// Validation and the operation are separate arguments, not one function,
// because they fail differently: an argument error throws, and only a
// filesystem error reaches the callback. One combined body would route both to
// the callback and hide a caller's bug behind an error it did not expect.
//
// `validate()` runs before the callback check, which is workerd's order: each
// entry point in `internal_fs_callback.ts` validates its path and its options,
// and only then calls `callWithSingleArgCallback`. Node checks the callback
// first, so a call that has a bad path and no callback reports the path here
// and reports the callback in Node. Both are `ERR_INVALID_ARG_TYPE`, and the
// runtime follows workerd.
const __fsDeliver = (validate, run, callback, deliver) => {
  validate();
  if (typeof callback !== "function") {
    const error = new TypeError('The "cb" argument must be of type function');
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  }
  let result;
  try {
    result = run();
  } catch (error) {
    // A callback delivered in this turn would let a caller observe the result
    // before its own call returned, which Node never does.
    queueMicrotask(() => callback(error));
    return;
  }
  queueMicrotask(() => deliver(result));
};
// An operation with a result calls back with `(error, result)`, and one
// without calls back with `(error)` alone. Workerd splits the two the same way
// (`callWithSingleArgCallback` and `callWithErrorOnlyCallback`), because a
// caller can read `arguments.length` and see the difference.
const __fsCallback = (validate, run, callback) =>
  __fsDeliver(validate, run, callback, (result) => callback(null, result));
const __fsErrorOnlyCallback = (validate, run, callback) =>
  __fsDeliver(validate, run, callback, () => callback(null));
// Node accepts `(path, callback)` or `(path, options, callback)` everywhere.
const __fsCallbackArgs = (options, callback) =>
  typeof options === "function" ? [undefined, options] : [options, callback];
const __fsAccess = (value, mode, callback) => {
  [mode, callback] = __fsCallbackArgs(mode, callback);
  __fsErrorOnlyCallback(
    () => { __fsPath(value); __fsMode(mode); },
    () => __fsAccessSync(value, mode),
    callback,
  );
};
const __fsStat = (value, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => { __fsPath(value); __fsOptions(options, ["bigint", "throwIfNoEntry"]); },
    () => __fsStatSync(value, options),
    callback,
  );
};
const __fsLstat = (value, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => { __fsPath(value); __fsOptions(options, ["bigint", "throwIfNoEntry"]); },
    () => __fsLstatSync(value, options),
    callback,
  );
};
const __fsRealpath = (value, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => __fsPath(value),
    () => __fsRealpathSync(value),
    callback,
  );
};
const __fsReadFile = (value, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => __fsPath(value),
    () => __fsReadFileSync(value, options),
    callback,
  );
};
const __fsMkdir = (value, options, callback) => {
  [options, callback] = __fsCallbackArgs(options, callback);
  __fsCallback(
    () => {
      __fsPath(value);
      // A number is the mode, which `mkdirSync` validates but does not apply.
      if (typeof options !== "number") __fsOptions(options, ["recursive"]);
    },
    () => __fsMkdirSync(value, options),
    callback,
  );
};
globalThis.__fsPromises = {
  // The promise form validates inside the promise, so an argument error
  // rejects instead of throwing. Workerd makes the same split, because a
  // caller of a promise API has no synchronous frame to catch.
  async access(...args) { return __fsAccessSync(...args); },
  async mkdir(...args) { return __fsMkdirSync(...args); },
  async stat(...args) { return __fsStatSync(...args); },
  async lstat(...args) { return __fsLstatSync(...args); },
  async realpath(...args) { return __fsRealpathSync(...args); },
  async readFile(...args) { return __fsReadFileSync(...args); },
};
const __fsSurface = {
  Stats: __FsStats,
  constants: __fsConstants,
  promises: globalThis.__fsPromises,
  accessSync: __fsAccessSync,
  access: __fsAccess,
  existsSync: __fsExistsSync,
  mkdirSync: __fsMkdirSync,
  statSync: __fsStatSync,
  lstatSync: __fsLstatSync,
  realpathSync: __fsRealpathSync,
  readFileSync: __fsReadFileSync,
  stat: __fsStat,
  lstat: __fsLstat,
  realpath: __fsRealpath,
  readFile: __fsReadFile,
  mkdir: __fsMkdir,
};
globalThis.__fs = new Proxy(__fsSurface, { get: (target, p) => {
  if (Reflect.has(target, p)) return Reflect.get(target, p);
  if (["readdirSync", "readlinkSync"].includes(p)) return __enoent;
  // Writable file contents and every mutation remain explicit unsupported
  // surfaces until a failing application seam requires them.
  if (typeof p !== "string") return undefined;
  return globalThis.__nodeStubFor("node:fs." + p);
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
      workerSocketId: 0,
    };
  }
  // Keep the network-error marker out of the HTTP response fields. If status
  // zero reaches Rust as an ordinary response, the outer server can report
  // only a generic invalid status instead of the Worker contract failure.
  if (r.type === "error" || r.status === 0)
    return { error: __ERROR_RESPONSE_MESSAGE };
  let workerSocketId = 0;
  let wsTarget = r._wsTarget || (r.webSocket && r.webSocket._target) || null;
  if (r.status === 101 && r.webSocket && wsTarget === null) {
    const server = r.webSocket._peer;
    if (server && server._accepted) {
      workerSocketId = server._id;
      // Install the host queue before the pump asks for its first frame. The
      // target marker makes sends use that queue instead of remaining in the
      // pair's pre-upgrade buffer.
      __ws_prepare_worker_handoff(workerSocketId);
      server._target = { id: workerSocketId, scope: "" };
      server._polled = true;
      __sockets.set(workerSocketId, server);
      server._startPump();
      server._flushPending();
    }
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
      wsTargetJson: JSON.stringify(wsTarget),
      workerSocketId,
    };
  }
  const bytes = r._bodyBytes;
  return {
    status: r.status,
    bodyBytes: bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes),
    bodyStreamId: 0,
    headersJson: JSON.stringify(Array.from(r.headers)),
    wsTargetJson: JSON.stringify(wsTarget),
    workerSocketId,
  };
};
