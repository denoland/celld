// MessageChannel/MessagePort for one isolate, following Workerd
// (src/workerd/api/messagechannel.c++): ports entangle in pairs, a port
// buffers messages until it has a message listener, close() closes both
// ports and fires "close" on each, and transfer lists are refused.
(function () {
  const kCreate = Symbol("celld.MessagePort.create");

  class MessagePort extends EventTarget {
    constructor(token = undefined) {
      super();
      if (token !== kCreate) {
        throw new TypeError("Illegal constructor");
      }
      this._peer = null;
      this._started = false;
      this._portClosed = false;
      this._portBuffer = [];
      this._messageHandler = null;
    }

    get [Symbol.toStringTag]() { return "MessagePort"; }

    get onmessage() { return this._messageHandler; }
    set onmessage(handler) {
      this._messageHandler = typeof handler === "function" ? handler : null;
      // Assigning a message handler starts the port, per the spec.
      this._startPort();
    }

    postMessage(value, options = undefined) {
      const transfer = Array.isArray(options) ? options : options?.transfer;
      if (transfer != null && transfer.length > 0) {
        throw new TypeError("Transfer list is not supported");
      }
      // RPC objects are host objects the structured clone algorithm
      // refuses. Workerd serializes before it looks at the peer, so a
      // closed port still surfaces the DataCloneError; matching that
      // keeps postMessage's behavior independent of peer liveness.
      const cf = globalThis.__cf;
      if (cf && (value instanceof cf.RpcTarget ||
        value instanceof cf.ServiceStub)) {
        throw new DOMException(
          "Could not serialize an RPC object for postMessage().",
          "DataCloneError");
      }
      const cloned = structuredClone(value);
      const peer = this._peer;
      if (this._portClosed || !peer || peer._portClosed) return;
      peer._deliver(cloned);
    }

    _deliver(data) {
      if (this._portClosed) return;
      if (!this._started) {
        this._portBuffer.push(data);
        return;
      }
      // Deferred like Workerd's delivery task: a handler attached in the
      // same turn as postMessage still sees the message.
      queueMicrotask(() => {
        if (this._portClosed) return;
        const event = new MessageEvent("message", { data });
        event._trust();
        this.dispatchEvent(event);
      });
    }

    _startPort() {
      if (this._started || this._portClosed) return;
      this._started = true;
      const pending = this._portBuffer;
      this._portBuffer = [];
      for (const data of pending) this._deliver(data);
    }

    start() { this._startPort(); }

    addEventListener(type, ...rest) {
      super.addEventListener(type, ...rest);
      if (String(type) === "message") this._startPort();
    }

    close() {
      if (this._portClosed) return;
      this._portClosed = true;
      this._portBuffer.length = 0;
      const peer = this._peer;
      if (peer && !peer._portClosed) peer.close();
      const event = new Event("close");
      event._trust();
      this.dispatchEvent(event);
    }
  }

  class MessageChannel {
    constructor() {
      const port1 = new MessagePort(kCreate);
      const port2 = new MessagePort(kCreate);
      port1._peer = port2;
      port2._peer = port1;
      Object.defineProperties(this, {
        port1: { value: port1, enumerable: true },
        port2: { value: port2, enumerable: true },
      });
    }
    get [Symbol.toStringTag]() { return "MessageChannel"; }
  }

  globalThis.MessagePort = MessagePort;
  globalThis.MessageChannel = MessageChannel;
})();
