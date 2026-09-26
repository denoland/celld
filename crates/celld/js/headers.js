const _tokenRe = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
// Token validation is memoized by name: a Worker sends the same handful of
// header names on every subrequest, so the regex would run once per header
// per request forever. The cache is a null-prototype object because the key
// is an attacker-controlled header name -- see the note on `_newList` -- and it
// is emptied wholesale at a bound so a caller that invents a fresh name per
// request cannot grow it without limit.
let _nameCache = { __proto__: null };
let _nameCacheSize = 0;
const _nameCacheMax = 4096;
function _checkName(name) {
  name = String(name);
  let valid = _nameCache[name];
  if (valid === undefined) {
    valid = _tokenRe.test(name);
    if (_nameCacheSize > _nameCacheMax) {
      _nameCache = { __proto__: null };
      _nameCacheSize = 0;
    }
    _nameCacheSize++;
    _nameCache[name] = valid;
  }
  if (!valid)
    throw new TypeError(
      "Invalid header name: " + name);
  // The name is returned as written. Lower-casing here would put celld's
  // spelling on the wire instead of the author's, which anything that
  // canonicalizes over the received bytes can see -- an AWS SigV4 signer
  // over `SignedHeaders` being the case that breaks.
  return name;
}
// Only a validated token reaches this, and the token production is ASCII, so
// `toLowerCase` cannot fold a name onto a different one here.
function _lower(name) {
  return name.toLowerCase();
}
// Byte sequence, minus NUL/LF/CR. Strip HTTP
// whitespace (HT/LF/CR/SP) from both ends first per
// WHATWG fetch -- trailing LF/CR are permitted input.
function _checkValue(value) {
  value = String(value).replace(
    /^[\t\n\r ]+|[\t\n\r ]+$/g, '');
  // workerd's rule: NUL, LF and CR are invalid; any other code
  // point passes (a received value is UTF-8 with replacement).
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c === 0 || c === 0x0A || c === 0x0D)
      throw new TypeError(
        "Invalid header value");
  }
  return value;
}

// Enumerable function-valued brand, shared by
// every instance: V8's structured clone throws
// on functions, so a Headers can never silently
// flatten into a plain object -- RPC
// serialization lifts it natively instead.
const _noClone = () => {};

// The spec's header list, plus the sorted pair list derived from it.
//
// `list` is a flat array of `[name, value]` pairs in insertion order, with
// each name exactly as it was written. `lower` is a parallel array holding
// the lower-cased form of `list[i][0]`, and it exists only so a comparison
// never lower-cases a name again. Both arrays are always mutated together --
// an entry with no matching `lower` slot would silently stop being findable.
//
// Two things forced this shape over a name-keyed structure. First, the list
// is what goes on the wire, and only a list can carry the author's casing,
// insertion order, and repeats. Second, header names are attacker-controlled
// and the token production admits `_`, so `__proto__` and `constructor` are
// valid names: a plain object keyed by a name answered both out of
// `Object.prototype`, which made `has("constructor")` true for a header
// nobody sent and turned the first `request.headers` read of a request
// carrying a `__proto__:` header into a TypeError. An array holds no such
// key, so the casing fix and the prototype fix are the same change. The
// `Map` this replaces fixed only the second one.
//
// `pairs` is the memo the iterator reads. Every mutator clears it, so a step
// after a mutation re-derives the list and iteration stays live, while an
// unmutated iteration sorts once instead of once per step.
function _newList() {
  return { list: [], lower: [], pairs: null };
}

// The iteration view, which is not the list: lower-cased, combined, and
// sorted by name, with `set-cookie` split one entry per value.
function _pairs(state) {
  if (state.pairs !== null) return state.pairs;
  const pairs = [];
  // Combining by a single pass needs a name-keyed index. A null-prototype
  // object is the only name-keyed object allowed here, for the reason on
  // `_newList` -- `seen["constructor"]` must be undefined, not a function.
  const seen = { __proto__: null };
  for (let i = 0; i < state.list.length; i++) {
    const name = state.lower[i];
    const value = state.list[i][1];
    // Only `set-cookie` keeps its values apart; every other name joins.
    if (name === "set-cookie") {
      pairs.push([name, value]);
      continue;
    }
    const at = seen[name];
    if (at !== undefined) {
      pairs[at][1] += ", " + value;
    } else {
      seen[name] = pairs.length;
      pairs.push([name, value]);
    }
  }
  // Stable, so repeated `set-cookie` values stay in insertion order.
  pairs.sort((a, b) =>
    a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
  state.pairs = pairs;
  return pairs;
}

class Headers {
  #list = _newList();

  #checkMutable() {
    if (this._immutable)
      throw new TypeError("Can't modify immutable headers.");
  }

  #push(name, v) {
    const state = this.#list;
    const lower = _lower(name);
    // A repeated name takes the casing already in the list, per the WHATWG
    // "header list append" operation, which reuses the name of the first
    // matching header, so one header never reaches the wire under two
    // spellings.
    for (let i = 0; i < state.lower.length; i++) {
      if (state.lower[i] === lower) {
        name = state.list[i][0];
        break;
      }
    }
    state.list.push([name, v]);
    state.lower.push(lower);
    state.pairs = null;
  }

  constructor(init) {
    this.__celldHost = _noClone;
    if (init === undefined) return;
    if (init === null || typeof init !== "object")
      throw new TypeError("Invalid Headers init");
    // Sequence-like (includes Headers with its default
    // iterator or any user-overridden one) takes precedence
    // over record so custom iterators are honored.
    if (typeof init[Symbol.iterator] === "function") {
      for (const pair of init) {
        if (!pair || typeof pair !== "object"
            || pair.length !== 2)
          throw new TypeError(
            "Headers init entry must be a 2-tuple");
        this.append(pair[0], pair[1]);
      }
    } else {
      // Record: explicit Reflect walk so Proxy handlers
      // see the WebIDL es-to-record trace exactly --
      // ownKeys -> for each key, getOwnPropertyDescriptor;
      // if enumerable, coerce key to ByteString (throws
      // for Symbols), then [[Get]], then validate value.
      for (const k of Reflect.ownKeys(init)) {
        const desc = Reflect
          .getOwnPropertyDescriptor(init, k);
        if (!desc || !desc.enumerable) continue;
        if (typeof k === "symbol")
          throw new TypeError(
            "Headers init key must be a string");
        const ck = _checkName(k);
        const v = Reflect.get(init, k, init);
        const cv = _checkValue(v);
        this.#push(ck, cv);
      }
    }
  }

  append(name, value) {
    this.#checkMutable();
    const k = _checkName(name);
    const v = _checkValue(value);
    this.#push(k, v);
  }
  get(name) {
    const lower = _lower(_checkName(name));
    const state = this.#list;
    let value = null;
    for (let i = 0; i < state.lower.length; i++) {
      if (state.lower[i] !== lower) continue;
      value = value === null ? state.list[i][1]
        : value + ", " + state.list[i][1];
    }
    return value;
  }
  set(name, value) {
    this.#checkMutable();
    name = _checkName(name);
    const v = _checkValue(value);
    const lower = _lower(name);
    const state = this.#list;
    // Compact in place: the first match takes the new value and keeps its
    // position and its casing, every later match is dropped. Rebuilding the
    // list instead would move the header to the end.
    let writeIdx = 0;
    let added = false;
    for (let i = 0; i < state.lower.length; i++) {
      if (state.lower[i] === lower) {
        if (added) continue;
        const entry = state.list[i];
        entry[1] = v;
        state.list[writeIdx] = entry;
        added = true;
      } else {
        state.list[writeIdx] = state.list[i];
      }
      state.lower[writeIdx] = state.lower[i];
      writeIdx++;
    }
    if (!added) {
      state.list.push([name, v]);
      state.lower.push(lower);
    } else if (writeIdx !== state.list.length) {
      state.list.length = writeIdx;
      state.lower.length = writeIdx;
    }
    state.pairs = null;
  }
  has(name) {
    const lower = _lower(_checkName(name));
    return this.#list.lower.includes(lower);
  }
  delete(name) {
    this.#checkMutable();
    const lower = _lower(_checkName(name));
    const state = this.#list;
    let writeIdx = 0;
    for (let i = 0; i < state.lower.length; i++) {
      if (state.lower[i] === lower) continue;
      state.list[writeIdx] = state.list[i];
      state.lower[writeIdx] = state.lower[i];
      writeIdx++;
    }
    if (writeIdx === state.list.length) return;
    state.list.length = writeIdx;
    state.lower.length = writeIdx;
    state.pairs = null;
  }
  getSetCookie() {
    const state = this.#list;
    const out = [];
    for (let i = 0; i < state.lower.length; i++)
      if (state.lower[i] === "set-cookie")
        out.push(state.list[i][1]);
    return out;
  }
  // The verbatim header list every outbound path serializes: insertion
  // order, original casing, one entry per append. The iteration view above
  // is lossy for a wire request -- it lower-cases, sorts, and combines -- so
  // this is a second, deliberately different view, not a convenience.
  //
  // A class accessor, so it is non-enumerable and a record walk over a
  // `Headers` never picks it up. Fresh pairs, so a caller that writes into
  // what it received cannot rewrite the header list or desynchronize it
  // from the parallel lower-cased names.
  get __celldHeaderList() {
    const list = this.#list.list;
    const out = new Array(list.length);
    for (let i = 0; i < list.length; i++)
      out[i] = [list[i][0], list[i][1]];
    return out;
  }
  // Iteration: per WHATWG the header list stays live,
  // so each step reads the current list -- see the
  // `pairs` memo above. Uses a hand-rolled iterator so
  // `next` has enumerable=true per WebIDL (generators
  // would be enumerable=false).
  entries() { return _makeHeadersIter(this.#list, 2); }
  keys() { return _makeHeadersIter(this.#list, 0); }
  values() { return _makeHeadersIter(this.#list, 1); }
  [Symbol.iterator]() { return this.entries(); }
  forEach(cb, thisArg) {
    if (typeof cb !== "function")
      throw new TypeError(
        "forEach callback must be a function");
    for (const [k, v] of this.entries())
      cb.call(thisArg, v, k, this);
  }
}

// Headers iterator prototype -- chained to
// %IteratorPrototype% so prototype-chain checks pass.
// kind: 0=keys, 1=values, 2=entries.
const _headersIterProto = Object.create(
  Object.getPrototypeOf(
    Object.getPrototypeOf([][Symbol.iterator]())));
Object.defineProperty(_headersIterProto, "next", {
  configurable: true, enumerable: true, writable: true,
  value: function next() {
    // The index into the current pair list is all the
    // iterator carries, so a name added after the
    // cursor is emitted and a name deleted before it
    // shifts the rest back -- the WHATWG live-list
    // semantics the plain-object version also had.
    const pairs = _pairs(this._list);
    if (this._cnt >= pairs.length)
      return { value: undefined, done: true };
    const [k, v] = pairs[this._cnt++];
    // A fresh pair per step: `pairs` outlives the step
    // now, so handing the memo's array to a caller
    // would let the caller rewrite the header list.
    const value = this._kind === 0 ? k
      : this._kind === 1 ? v : [k, v];
    return { value, done: false };
  },
});
Object.defineProperty(_headersIterProto,
  Symbol.iterator, { value() { return this; } });
function _makeHeadersIter(list, kind) {
  const it = Object.create(_headersIterProto);
  it._list = list; it._cnt = 0; it._kind = kind;
  return it;
}

// Coerce a Request/Response body init to one of the
// `BodyInit` member types or null. Pre-fix
// `new Request(url, {body: 42})` stored 42 as the
// body and then `.text()` ran TextDecoder on it,
// producing 42 NUL bytes (V8 interprets a number
// passed to TextDecoder.decode as a BufferSource of
// that byte length). Per Web IDL union conversion,
// the BodyInit dictionary member's USVString branch
// accepts any non-BodyInit value via ToString. Match
// that here so primitives go through cleanly.

globalThis.Headers = Headers;
