// The Workers Cache API with always-miss semantics.
//
// celld has no shared edge cache, and Cloudflare's Cache API is a view of
// exactly that cache. Refusing the API would break the large class of
// Workers that treat `caches.default` as a best-effort layer over their
// own origin logic, so celld accepts the surface, validates it with
// Workerd's rules and messages, and misses every match — the same
// posture as `passThroughOnException()`, which is a documented no-op
// because there is no CDN behind it. A per-node in-memory cache can
// replace the miss without changing any observable contract.
(function () {
  const kCreate = Symbol("celld.Cache.create");

  // A key is a Request or URL string, and it must parse as an absolute
  // http(s) URL — the same rule and message as Workerd's validateUrl.
  const asKey = (request) => {
    const url = request instanceof Request ? request.url : String(request);
    let parsed = null;
    try {
      parsed = new URL(url);
    } catch { /* rejected below */ }
    if (!parsed ||
      (parsed.protocol !== "http:" && parsed.protocol !== "https:")) {
      throw new TypeError(
        "Invalid URL. Cache API keys must be fully-qualified, valid URLs.");
    }
    return request instanceof Request ? request : new Request(url);
  };

  class Cache {
    constructor(token = undefined) {
      if (token !== kCreate) throw new TypeError("Illegal constructor");
    }

    get [Symbol.toStringTag]() { return "Cache"; }

    async put(request, response) {
      const key = asKey(request);
      if (!(response instanceof Response)) {
        throw new TypeError(
          "Cache.put() requires a Response as its second argument.");
      }
      if (key.method !== "GET") {
        throw new TypeError("Cannot cache response to non-GET request.");
      }
      if (response.status === 206) {
        throw new TypeError(
          "Cannot cache response to a range request (206 Partial Content).");
      }
      const vary = response.headers.get("vary");
      if (vary !== null && vary.includes("*")) {
        throw new TypeError("Cannot cache response with 'Vary: *' header.");
      }
      if (response.bodyUsed) {
        throw new TypeError(
          "Cannot cache a response whose body is already used.");
      }
      // Consume the body even though nothing is stored: put()'s contract
      // is that the response is disturbed when it returns, and a
      // streaming producer must see its stream pulled to completion
      // instead of hanging on a reader that never comes.
      if (response.body) await response.arrayBuffer();
    }

    async match(request, options = undefined) {
      const key = asKey(request);
      // A non-GET key cannot match without ignoreMethod; celld misses
      // either way, and the option is accepted for compatibility.
      void key;
      void options;
      return undefined;
    }

    async delete(request, options = undefined) {
      asKey(request);
      void options;
      return false;
    }
  }

  class CacheStorage {
    constructor(token = undefined) {
      if (token !== kCreate) throw new TypeError("Illegal constructor");
    }

    get [Symbol.toStringTag]() { return "CacheStorage"; }

    async open(cacheName) {
      if (String(cacheName).length >= 1024) {
        throw new TypeError("Cache name is too long.");
      }
      return new Cache(kCreate);
    }
  }

  const storage = new CacheStorage(kCreate);
  Object.defineProperty(storage, "default", {
    value: new Cache(kCreate),
    enumerable: true,
  });

  globalThis.Cache = Cache;
  globalThis.CacheStorage = CacheStorage;
  globalThis.caches = storage;
})();
