// EventSource over the streaming fetch path, following Workerd
// (src/workerd/api/eventsource.c++): same reconnection ladder (2 s
// default retry clamped to [1 s, 10 s], one no-body response retries and
// a second fails), same header set, and the same parser — a message's
// data lines join with "\n", an empty joined data dispatches nothing,
// and `id:` persists as the Last-Event-ID for reconnects.
(function () {
  const kFrom = Symbol("celld.EventSource.from");

  class EventSource extends EventTarget {
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSED = 2;

    constructor(url, init = undefined) {
      super();
      this._sourceState = 0; // CONNECTING
      this._sourceUrl = "";
      this._fetcher = null;
      this._lastId = "";
      this._retryMs = 2000;
      this._dataLines = [];
      this._eventType = "";
      this._lineBuf = "";
      this._fieldSeen = false;
      this._sourceClosed = false;
      this._retryTimer = null;
      this._reader = null;
      this._previousNoBody = false;
      this._openHandler = null;
      this._messageHandler = null;
      this._errorHandler = null;
      if (url === kFrom) return;
      let parsed;
      try {
        parsed = new URL(String(url));
      } catch {
        throw new DOMException(
          "Cannot open an EventSource to '" + String(url) +
            "'. The URL is invalid.",
          "SyntaxError");
      }
      if (init?.withCredentials) {
        throw new DOMException(
          "The init.withCredentials option is not supported. It must be " +
            "false or undefined.",
          "NotSupportedError");
      }
      this._sourceUrl = parsed.href;
      this._fetcher = init?.fetcher ?? null;
      this._connect();
    }

    // Workerd extension: parse an SSE stream directly, with no
    // connection and therefore no reconnection.
    static from(readable) {
      if (!(readable instanceof ReadableStream)) {
        throw new TypeError("argument is not a ReadableStream");
      }
      if (readable.locked) {
        throw new TypeError("This ReadableStream is locked.");
      }
      const source = new EventSource(kFrom);
      source._pump(readable, false);
      return source;
    }

    get [Symbol.toStringTag]() { return "EventSource"; }
    get readyState() { return this._sourceState; }
    get url() { return this._sourceUrl; }
    get withCredentials() { return false; }

    get onopen() { return this._openHandler; }
    set onopen(handler) {
      this._openHandler = typeof handler === "function" ? handler : null;
    }
    get onmessage() { return this._messageHandler; }
    set onmessage(handler) {
      this._messageHandler = typeof handler === "function" ? handler : null;
    }
    get onerror() { return this._errorHandler; }
    set onerror(handler) {
      this._errorHandler = typeof handler === "function" ? handler : null;
    }

    close() {
      if (this._sourceClosed) return;
      this._sourceClosed = true;
      this._sourceState = 2; // CLOSED
      if (this._retryTimer !== null) {
        clearTimeout(this._retryTimer);
        this._retryTimer = null;
      }
      const reader = this._reader;
      this._reader = null;
      if (reader) reader.cancel().catch(() => {});
    }

    [Symbol.dispose]() { this.close(); }

    _notifyError(message, reconnecting, error = undefined) {
      if (this._sourceState === 2) return;
      this._sourceState = reconnecting ? 0 : 2;
      const event = new ErrorEvent("error", { message, error });
      event._trust();
      this.dispatchEvent(event);
    }

    _notifyOpen() {
      if (this._sourceState === 2) return;
      this._sourceState = 1; // OPEN
      const event = new Event("open");
      event._trust();
      this.dispatchEvent(event);
    }

    _dispatchMessage() {
      const data = this._dataLines.join("\n");
      const type = this._eventType || "message";
      this._dataLines = [];
      this._eventType = "";
      if (data === "") return;
      let origin = "";
      if (this._sourceUrl !== "") origin = new URL(this._sourceUrl).origin;
      const event = new MessageEvent(type, {
        data,
        origin,
        lastEventId: this._lastId,
      });
      event._trust();
      this.dispatchEvent(event);
    }

    _feedLine(line) {
      if (line === "") {
        if (this._fieldSeen) {
          this._fieldSeen = false;
          this._dispatchMessage();
        }
        return;
      }
      if (line[0] === ":") return; // comment
      const colon = line.indexOf(":");
      let field, value;
      if (colon === -1) {
        field = line;
        value = "";
      } else {
        field = line.slice(0, colon);
        value = line.slice(colon + 1);
        // Exactly one space after the colon is trimmed.
        if (value[0] === " ") value = value.slice(1);
      }
      this._fieldSeen = true;
      if (field === "data") this._dataLines.push(value);
      else if (field === "event") this._eventType = value;
      else if (field === "id") this._lastId = value;
      else if (field === "retry" && /^\d+$/.test(value)) {
        this._retryMs = Math.max(1000, Math.min(Number(value), 10000));
      }
      // Any other field is ignored.
    }

    _feedText(text, eof) {
      this._lineBuf += text;
      for (;;) {
        const match = /[\r\n]/.exec(this._lineBuf);
        if (!match) break;
        const at = match.index;
        // A "\r" at the buffer's end may be half of "\r\n"; wait for the
        // next chunk unless the stream already ended.
        if (this._lineBuf[at] === "\r" &&
          at + 1 === this._lineBuf.length && !eof) {
          break;
        }
        const line = this._lineBuf.slice(0, at);
        const skip = this._lineBuf[at] === "\r" &&
            this._lineBuf[at + 1] === "\n"
          ? 2
          : 1;
        this._lineBuf = this._lineBuf.slice(at + skip);
        this._feedLine(line);
      }
      // A partial line with no end-of-line is dropped at end of stream,
      // as Workerd's sink does.
      if (eof) this._lineBuf = "";
    }

    _scheduleReconnect() {
      this._retryTimer = setTimeout(() => {
        this._retryTimer = null;
        if (this._sourceClosed) return;
        this._connect();
      }, this._retryMs);
    }

    async _pump(readable, withReconnection) {
      this._notifyOpen();
      try {
        const reader = readable.getReader();
        this._reader = reader;
        const decoder = new TextDecoder(); // strips a leading BOM
        for (;;) {
          const { done, value } = await reader.read();
          if (this._sourceClosed) return;
          if (done) break;
          this._feedText(decoder.decode(value, { stream: true }), false);
        }
        this._feedText(decoder.decode(), true);
        this._reader = null;
        this._notifyError("The server disconnected.", withReconnection);
        if (withReconnection && !this._sourceClosed) {
          this._scheduleReconnect();
        }
      } catch (error) {
        if (this._sourceClosed) return;
        this._reader = null;
        this._notifyError(String(error?.message || error), false, error);
      }
    }

    async _connect() {
      if (this._sourceClosed) return;
      const headers = {
        "accept": "text/event-stream",
        "cache-control": "no-cache",
      };
      if (this._lastId !== "") headers["last-event-id"] = this._lastId;
      try {
        const fetcher = this._fetcher;
        const response = fetcher
          ? await fetcher.fetch(this._sourceUrl, { headers })
          : await fetch(this._sourceUrl, { headers });
        if (this._sourceClosed) return;
        if (!response.ok) {
          this._notifyError(
            "The response status code was " + response.status + ".", false);
          return;
        }
        const contentType = response.headers.get("content-type");
        if (!contentType) {
          this._notifyError(
            "No content type header was present in the response.", false);
          return;
        }
        const essence = contentType.split(";")[0].trim().toLowerCase();
        if (essence !== "text/event-stream") {
          this._notifyError(
            "The content type '" + contentType + "' is invalid.", false);
          return;
        }
        if (response.redirected && response.url) {
          try {
            this._sourceUrl = new URL(response.url).href;
          } catch { /* keep the configured URL */ }
        }
        // One empty response reads as a server hiccup and retries; a
        // second reads as a broken server and fails the connection.
        const body = response.status === 204 ? null : response.body;
        if (!body) {
          if (this._previousNoBody) {
            this._notifyError("The server provided no content.", false);
          } else {
            this._previousNoBody = true;
            this._notifyError(
              "The server provided no content. Will try reconnecting.",
              true);
            this._scheduleReconnect();
          }
          return;
        }
        await this._pump(body, true);
      } catch (error) {
        if (this._sourceClosed) return;
        this._notifyError(String(error?.message || error), false, error);
      }
    }
  }

  // The WebIDL constants live on the prototype as well as the interface.
  for (const [name, value] of
    [["CONNECTING", 0], ["OPEN", 1], ["CLOSED", 2]]) {
    Object.defineProperty(EventSource.prototype, name, {
      value, writable: false, enumerable: true, configurable: false,
    });
  }

  globalThis.EventSource = EventSource;
})();
