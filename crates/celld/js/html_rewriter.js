// HTMLRewriter over the __hr_* host ops (lol_html on its own thread).
//
// The host parks the parser while JS services each content token, so a
// handler can be async and its mutations apply to the live token with
// lol_html's own validation errors. Content tokens are only valid
// during their handler — afterwards every member throws, as Workerd's
// do. Stream-valued content (a ReadableStream or Response passed to
// before/replace/...) is buffered after the handler settles and then
// applied, which keeps output order without pausing the parser twice.
(function () {
  const kDead =
    "This content token is no longer valid. Content tokens are only " +
    "valid during the execution of the relevant content handler.";
  const kIterInvalid =
    "The attributes of this element have been modified during iteration. " +
    "You must create a new iterator after modifying attributes.";

  const cmd = (id, payload) => {
    const raw = __hr_cmd(id, JSON.stringify(payload));
    const parsed = JSON.parse(raw);
    if (parsed.error !== undefined) throw new TypeError(parsed.error);
    return parsed.ok;
  };

  // The iterator's [[Prototype]] chain must end at %IteratorPrototype%,
  // which the Workerd suite checks via a plain array iterator.
  const iterProto = {
    next() { return this.__step(); },
  };
  Object.setPrototypeOf(
    iterProto,
    Object.getPrototypeOf(Object.getPrototypeOf([][Symbol.iterator]())),
  );

  class Element {
    constructor(state) {
      this.__state = state; // { id, live, attrEpoch, deferred, endTag }
    }
    __check() {
      if (!this.__state.live) throw new TypeError(kDead);
    }
    __content(op, content, options) {
      this.__check();
      const html = !!(options && options.html);
      if (content instanceof ReadableStream || content instanceof Response) {
        this.__state.deferred.push({ op, source: content, html });
        return;
      }
      cmd(this.__state.id, { op, content: String(content), html });
    }
    get tagName() {
      this.__check();
      return cmd(this.__state.id, { op: "tagName" });
    }
    set tagName(name) {
      this.__check();
      cmd(this.__state.id, { op: "setTagName", name: String(name) });
    }
    get namespaceURI() {
      this.__check();
      return cmd(this.__state.id, { op: "namespaceURI" });
    }
    get removed() {
      this.__check();
      return cmd(this.__state.id, { op: "removed" });
    }
    get attributes() {
      this.__check();
      const state = this.__state;
      const epoch = state.attrEpoch;
      const inner = cmd(state.id, { op: "attributes" })[Symbol.iterator]();
      const iterator = Object.create(iterProto);
      iterator.__step = () => {
        if (!state.live) throw new TypeError(kDead);
        if (state.attrEpoch !== epoch) throw new TypeError(kIterInvalid);
        return inner.next();
      };
      return iterator;
    }
    getAttribute(name) {
      this.__check();
      return cmd(this.__state.id, { op: "getAttribute", name: String(name) });
    }
    hasAttribute(name) {
      this.__check();
      return cmd(this.__state.id, { op: "hasAttribute", name: String(name) });
    }
    setAttribute(name, value) {
      this.__check();
      cmd(this.__state.id, {
        op: "setAttribute",
        name: String(name),
        value: String(value),
      });
      this.__state.attrEpoch += 1;
      return this;
    }
    removeAttribute(name) {
      this.__check();
      cmd(this.__state.id, { op: "removeAttribute", name: String(name) });
      this.__state.attrEpoch += 1;
      return this;
    }
    before(content, options) { this.__content("before", content, options); return this; }
    after(content, options) { this.__content("after", content, options); return this; }
    prepend(content, options) { this.__content("prepend", content, options); return this; }
    append(content, options) { this.__content("append", content, options); return this; }
    replace(content, options) { this.__content("replace", content, options); return this; }
    setInnerContent(content, options) {
      this.__content("setInnerContent", content, options);
      return this;
    }
    remove() {
      this.__check();
      cmd(this.__state.id, { op: "remove" });
      return this;
    }
    removeAndKeepContent() {
      this.__check();
      cmd(this.__state.id, { op: "removeAndKeepContent" });
      return this;
    }
    onEndTag(handler) {
      this.__check();
      this.__state.endTag = handler;
    }
  }

  class EndTag {
    constructor(state, name) {
      this.__state = state;
      this.__name = name;
    }
    __check() {
      if (!this.__state.live) throw new TypeError(kDead);
    }
    get name() {
      this.__check();
      return this.__name;
    }
    before(content, options) {
      this.__check();
      cmd(this.__state.id, {
        op: "before",
        content: String(content),
        html: !!(options && options.html),
      });
      return this;
    }
    after(content, options) {
      this.__check();
      cmd(this.__state.id, {
        op: "after",
        content: String(content),
        html: !!(options && options.html),
      });
      return this;
    }
    remove() {
      this.__check();
      cmd(this.__state.id, { op: "remove" });
      return this;
    }
  }

  class Comment {
    constructor(state, text) {
      this.__state = state;
      this.__text = text;
    }
    __check() {
      if (!this.__state.live) throw new TypeError(kDead);
    }
    get text() {
      this.__check();
      return this.__text;
    }
    set text(value) {
      this.__check();
      cmd(this.__state.id, { op: "setText", text: String(value) });
      this.__text = String(value);
    }
    get removed() {
      this.__check();
      return cmd(this.__state.id, { op: "removed" });
    }
    __content(op, content, options) {
      this.__check();
      cmd(this.__state.id, {
        op,
        content: String(content),
        html: !!(options && options.html),
      });
    }
    before(content, options) { this.__content("before", content, options); return this; }
    after(content, options) { this.__content("after", content, options); return this; }
    replace(content, options) { this.__content("replace", content, options); return this; }
    remove() {
      this.__check();
      cmd(this.__state.id, { op: "remove" });
      return this;
    }
  }

  class Text {
    constructor(state, text, lastInTextNode) {
      this.__state = state;
      this.__text = text;
      this.__last = lastInTextNode;
    }
    __check() {
      if (!this.__state.live) throw new TypeError(kDead);
    }
    get text() {
      this.__check();
      return this.__text;
    }
    get lastInTextNode() {
      this.__check();
      return this.__last;
    }
    get removed() {
      this.__check();
      return cmd(this.__state.id, { op: "removed" });
    }
    __content(op, content, options) {
      this.__check();
      cmd(this.__state.id, {
        op,
        content: String(content),
        html: !!(options && options.html),
      });
    }
    before(content, options) { this.__content("before", content, options); return this; }
    after(content, options) { this.__content("after", content, options); return this; }
    replace(content, options) { this.__content("replace", content, options); return this; }
    remove() {
      this.__check();
      cmd(this.__state.id, { op: "remove" });
      return this;
    }
  }

  class Doctype {
    constructor(state, name, publicId, systemId) {
      this.__state = state;
      this.__name = name;
      this.__publicId = publicId;
      this.__systemId = systemId;
    }
    __check() {
      if (!this.__state.live) throw new TypeError(kDead);
    }
    get name() { this.__check(); return this.__name; }
    get publicId() { this.__check(); return this.__publicId; }
    get systemId() { this.__check(); return this.__systemId; }
  }

  class DocumentEnd {
    constructor(state) { this.__state = state; }
    append(content, options) {
      if (!this.__state.live) throw new TypeError(kDead);
      cmd(this.__state.id, {
        op: "append",
        content: String(content),
        html: !!(options && options.html),
      });
      return this;
    }
  }

  // Stream-valued content buffers to text after the handler settles.
  // Bad UTF-8 is Workerd's parser error, thrown before the mutation.
  const collectContent = async (source) => {
    const buffer = source instanceof Response
      ? await source.arrayBuffer()
      : await new Response(source).arrayBuffer();
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(buffer);
    } catch {
      throw new Error("Parser error: Invalid UTF-8");
    }
  };

  class HTMLRewriter {
    constructor() {
      this.__selectors = [];
      this.__document = [];
    }

    on(selector, handlers) {
      this.__selectors.push({ selector: String(selector), handlers });
      return this;
    }

    onDocument(handlers) {
      this.__document.push(handlers);
      return this;
    }

    transform(response) {
      if (!(response instanceof Response)) {
        throw new TypeError(
          "HTMLRewriter.transform() requires a Response argument");
      }
      const contentType = response.headers.get("content-type") || "";
      const charset = /;\s*charset=([^;\s]+)/i.exec(contentType);
      const config = {
        selectors: this.__selectors.map(({ selector, handlers }) => ({
          selector,
          element: typeof handlers?.element === "function",
          comments: typeof handlers?.comments === "function",
          text: typeof handlers?.text === "function",
        })),
        document: this.__document.map((handlers) => ({
          doctype: typeof handlers?.doctype === "function",
          comments: typeof handlers?.comments === "function",
          text: typeof handlers?.text === "function",
          end: typeof handlers?.end === "function",
        })),
      };
      if (charset) config.encoding = charset[1].replace(/^"|"$/g, "");
      const id = __hr_create(JSON.stringify(config));

      const init = {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      };
      const source = response.body;
      if (!source) {
        __hr_free(id);
        return new Response(null, init);
      }

      const state = {
        id,
        source,
        controller: null,
        reader: null,
        cancelled: false,
        endTags: new Map(),
        nextEndTagToken: 1,
        selectors: this.__selectors,
        document: this.__document,
      };
      const readable = new ReadableStream({
        start: (controller) => {
          state.controller = controller;
          __hrPump(state);
        },
        cancel: (reason) => __hrCancel(state, reason),
      });
      return new Response(readable, init);
    }
  }

  const __hrCancel = (state, reason) => {
    if (state.cancelled) return;
    state.cancelled = true;
    state.cancelReason = reason;
    __hr_free(state.id);
    // The source is NOT cancelled here: Workerd's pump notices a
    // cancelled destination on its next pull, so a producer's write
    // that a pending read already accepted still resolves, and only
    // the write after it observes the cancellation reason.
  };

  const __hrFail = (state, error) => {
    if (state.cancelled) return;
    state.cancelled = true;
    state.controller.error(error);
    __hr_free(state.id);
    if (state.reader) state.reader.cancel(error).catch(() => {});
  };

  const __hrFlush = (state) => {
    const bytes = __hr_take(state.id);
    if (bytes.byteLength > 0) state.controller.enqueue(bytes);
  };

  async function __hrService(state, ev) {
    const tokenState = {
      id: state.id,
      live: true,
      attrEpoch: 0,
      deferred: [],
      endTag: null,
    };
    let facade = null;
    let handler = null;
    let owner = null;
    if (ev.kind === "element") {
      owner = state.selectors[ev.handler].handlers;
      handler = owner.element;
      facade = new Element(tokenState);
    } else if (ev.kind === "comment") {
      owner = ev.document
        ? state.document[ev.docHandler]
        : state.selectors[ev.handler].handlers;
      handler = owner.comments;
      facade = new Comment(tokenState, ev.text);
    } else if (ev.kind === "text") {
      owner = ev.document
        ? state.document[ev.docHandler]
        : state.selectors[ev.handler].handlers;
      handler = owner.text;
      facade = new Text(tokenState, ev.text, ev.lastInTextNode);
    } else if (ev.kind === "doctype") {
      owner = state.document[ev.docHandler];
      handler = owner.doctype;
      facade = new Doctype(tokenState, ev.name, ev.publicId, ev.systemId);
    } else if (ev.kind === "documentEnd") {
      owner = state.document[ev.docHandler];
      handler = owner.end;
      facade = new DocumentEnd(tokenState);
    } else if (ev.kind === "endTag") {
      handler = state.endTags.get(ev.token);
      state.endTags.delete(ev.token);
      facade = new EndTag(tokenState, ev.name);
    }
    try {
      if (typeof handler === "function") {
        await handler.call(owner, facade);
      }
      for (const deferred of tokenState.deferred) {
        const content = await collectContent(deferred.source);
        cmd(state.id, { op: deferred.op, content, html: deferred.html });
      }
    } catch (error) {
      tokenState.live = false;
      try { __hr_cmd(state.id, JSON.stringify({ op: "abort" })); } catch {}
      __hrFail(state, error);
      return;
    }
    tokenState.live = false;
    const done = { op: "done" };
    if (ev.kind === "element" && typeof tokenState.endTag === "function") {
      done.wantEndTag = true;
      done.endTagToken = state.nextEndTagToken++;
      state.endTags.set(done.endTagToken, tokenState.endTag);
    }
    try { __hr_cmd(state.id, JSON.stringify(done)); } catch {}
  }

  async function __hrPump(state) {
    // Feed the source into the parser as it arrives; the event loop
    // below is what actually paces handler work and output.
    (async () => {
      try {
        const reader = state.source.getReader();
        state.reader = reader;
        for (;;) {
          const { done, value } = await reader.read();
          if (state.cancelled) {
            reader.cancel(state.cancelReason).catch(() => {});
            return;
          }
          if (done) break;
          __hr_write(
            state.id,
            value instanceof Uint8Array ? value : new Uint8Array(value),
          );
        }
        __hr_end(state.id);
      } catch (error) {
        __hrFail(state, error);
      }
    })();
    try {
      for (;;) {
        const raw = await __hr_event(state.id);
        if (state.cancelled) return;
        const ev = JSON.parse(raw);
        if (ev.kind === "output") {
          __hrFlush(state);
        } else if (ev.kind === "end") {
          __hrFlush(state);
          state.cancelled = true;
          state.controller.close();
          __hr_free(state.id);
          return;
        } else if (ev.kind === "error") {
          __hrFail(state, new Error(ev.message));
          return;
        } else if (ev.kind === "closed") {
          return;
        } else {
          await __hrService(state, ev);
        }
      }
    } catch (error) {
      __hrFail(state, error);
    }
  }

  globalThis.HTMLRewriter = HTMLRewriter;
})();
