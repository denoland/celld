// node:crypto for celld — a port of Workerd's implementation at commit
// 191a27f941300dd8956f2afeb66c10a651108c0b. Copyright (c) 2017-2022
// Cloudflare, Inc. (Apache-2.0), itself adapted from Node.js (Joyent and
// Node.js contributors, MIT). Ported files:
//
//   src/node/crypto.ts                      (module surface, constants)
//   src/node/internal/crypto_hash.ts        (Hash / Hmac / hash())
//   src/node/internal/crypto_keys.ts        (KeyObject family; secret keys)
//   src/node/internal/crypto_pbkdf2.ts
//   src/node/internal/crypto_hkdf.ts
//   src/node/internal/crypto_random.ts
//   src/node/internal/crypto_util.ts
//   src/node/internal/validators.ts, internal_errors.ts (subsets)
//
// Types are stripped; the C++ `node-internal:crypto` builtin maps onto the
// $$digest / $$hmacSign / $$pbkdf2 / $$hkdf / $$randomValues /
// $$timingSafeEqual host ops, with hash state buffered on the JS side (digest
// runs once, at finalization).
// checkPrime/generatePrime are pure-JS BigInt Miller–Rabin, mirroring
// Workerd's src/workerd/api/crypto/prime.c++ semantics (8192-bit cap, the
// {12,11}/{24,23}/{60,59} add/rem allowlist, top-two-bits-set candidates).
// Asymmetric key parsing/generation (PEM/DER/JWK) is not implemented; those
// entry points validate their arguments like Workerd, then throw.
//
// Injected lazily into the generated stub module, so an isolate that never
// imports node:crypto pays nothing.
(() => {
  "use strict";
  const Buffer = globalThis.Buffer; // lazy global; forces node_buffer
  const kMaxLength = 2147483647;

  // ---- errors / validators (Workerd's internal_errors.ts subset) -----------
  const kTypes = [
    "string", "function", "number", "object", "Function", "Object",
    "boolean", "bigint", "symbol",
  ];
  const classRe = /^([A-Z][a-z0-9]*)+$/;
  const fmt = (v) => {
    if (typeof v === "string") return `'${v}'`;
    if (typeof v === "bigint") return `${v}n`;
    if (typeof v === "symbol") return v.toString();
    if (Array.isArray(v)) return `[ ${v.map(fmt).join(", ")} ]`;
    return String(v);
  };
  const received = (v) => {
    if (v == null) return ` Received ${v}`;
    if (typeof v === "function" && v.name)
      return ` Received function ${v.name}`;
    if (typeof v === "object") {
      if (v.constructor?.name)
        return ` Received an instance of ${v.constructor.name}`;
      return " Received Object";
    }
    let s = fmt(v);
    if (s.length > 27) s = `${s.slice(0, 25)}...`;
    return ` Received type ${typeof v} (${s})`;
  };
  const numSep = (s) => {
    let out = "", i = s.length;
    const start = s[0] === "-" ? 1 : 0;
    for (; i >= start + 4; i -= 3) out = `_${s.slice(i - 3, i)}${out}`;
    return `${s.slice(0, i)}${out}`;
  };
  function invalidArgType(name, expected, actual) {
    expected = Array.isArray(expected) ? expected : [expected];
    let msg = "The ";
    if (name.endsWith(" argument")) msg += `${name} `;
    else msg += `"${name}" ${name.includes(".") ? "property" : "argument"} `;
    msg += "must be ";
    const types = [], instances = [], other = [];
    for (const value of expected) {
      if (kTypes.includes(value)) types.push(value.toLowerCase());
      else if (classRe.test(value)) instances.push(value);
      else other.push(value);
    }
    if (instances.length > 0) {
      const i = types.indexOf("object");
      if (i !== -1) { types.splice(i, 1); instances.push("Object"); }
    }
    if (types.length > 0) {
      msg += `${types.length > 1 ? "one of type" : "of type"} `;
      if (types.length > 2) {
        const last = types.pop();
        msg += `${types.join(", ")}, or ${last}`;
      } else msg += types.join(" or ");
      if (instances.length > 0 || other.length > 0) msg += " or ";
    }
    if (instances.length > 0) {
      if (instances.length > 2) {
        const last = instances.pop();
        msg += `an instance of ${instances.join(", ")}, or ${last}`;
      } else {
        msg += `an instance of ${instances[0]}`;
        if (instances.length === 2) msg += ` or ${instances[1]}`;
      }
      if (other.length > 0) msg += " or ";
    }
    if (other.length > 0) {
      if (other.length > 2) {
        const last = other.pop();
        msg += `one of ${other.join(", ")}, or ${last}`;
      } else if (other.length === 2) {
        msg += `one of ${other[0]} or ${other[1]}`;
      } else {
        if (other[0]?.toLowerCase() !== other[0]) msg += "an ";
        msg += `${other[0]}`;
      }
    }
    return `${msg}.${received(actual)}`;
  }
  const nodeErr = (Base, code, message) => {
    const error = new Base(message);
    error.code = code;
    // Node bakes the code into the stack by generating it under the
    // decorated name, then restoring the plain one.
    error.name = `${Base.prototype.name} [${code}]`;
    void error.stack;
    error.name = Base.prototype.name;
    return error;
  };
  const ERR_INVALID_ARG_TYPE = (name, expected, actual) =>
    nodeErr(TypeError, "ERR_INVALID_ARG_TYPE",
      invalidArgType(name, expected, actual));
  const ERR_OUT_OF_RANGE = (name, range, input) => {
    let recv;
    if (Number.isInteger(input) && Math.abs(input) > 2 ** 32)
      recv = numSep(String(input));
    else if (typeof input === "bigint") {
      recv = String(input);
      if (input > 2n ** 32n || input < -(2n ** 32n)) recv = numSep(recv);
      recv += "n";
    } else recv = fmt(input);
    return nodeErr(RangeError, "ERR_OUT_OF_RANGE",
      `The value of "${name}" is out of range. It must be ${range}. ` +
      `Received ${recv}`);
  };
  const ERR_INVALID_ARG_VALUE = (name, value, reason = "is invalid") =>
    nodeErr(TypeError, "ERR_INVALID_ARG_VALUE",
      `The ${name.includes(".") ? "property" : "argument"} '${name}' ` +
      `${reason}. Received ${fmt(value)}`);
  const ERR_METHOD_NOT_IMPLEMENTED = (name) =>
    nodeErr(Error, "ERR_METHOD_NOT_IMPLEMENTED",
      `The ${name} method is not implemented`);
  const ERR_MISSING_OPTION = (name) =>
    nodeErr(TypeError, "ERR_MISSING_OPTION", `${name} is required`);
  const ERR_INCOMPATIBLE_OPTION_PAIR = (a, b) =>
    nodeErr(TypeError, "ERR_INCOMPATIBLE_OPTION_PAIR",
      `Option "${a}" cannot be used in combination with option "${b}"`);
  const ERR_CRYPTO_HASH_FINALIZED = () =>
    nodeErr(Error, "ERR_CRYPTO_HASH_FINALIZED", "Digest already called");
  const ERR_CRYPTO_HASH_UPDATE_FAILED = () =>
    nodeErr(Error, "ERR_CRYPTO_HASH_UPDATE_FAILED", "Hash update failed");
  const ERR_CRYPTO_INVALID_KEY_OBJECT_TYPE = (actual, expected) =>
    nodeErr(TypeError, "ERR_CRYPTO_INVALID_KEY_OBJECT_TYPE",
      `Invalid key object type ${actual}, expected ${expected}.`);
  const ERR_CRYPTO_TIMING_SAFE_EQUAL_LENGTH = () =>
    nodeErr(RangeError, "ERR_CRYPTO_TIMING_SAFE_EQUAL_LENGTH",
      "Input buffers must have the same byte length");

  function validateString(value, name) {
    if (typeof value !== "string")
      throw ERR_INVALID_ARG_TYPE(name, "string", value);
  }
  function validateBoolean(value, name) {
    if (typeof value !== "boolean")
      throw ERR_INVALID_ARG_TYPE(name, "boolean", value);
  }
  function validateFunction(value, name) {
    if (typeof value !== "function")
      throw ERR_INVALID_ARG_TYPE(name, "Function", value);
  }
  function validateObject(value, name) {
    if (value === null || Array.isArray(value) || typeof value !== "object")
      throw ERR_INVALID_ARG_TYPE(name, "Object", value);
  }
  function validateInteger(value, name, min = Number.MIN_SAFE_INTEGER,
      max = Number.MAX_SAFE_INTEGER) {
    if (typeof value !== "number")
      throw ERR_INVALID_ARG_TYPE(name, "number", value);
    if (!Number.isInteger(value))
      throw ERR_OUT_OF_RANGE(name, "an integer", value);
    if (value < min || value > max)
      throw ERR_OUT_OF_RANGE(name, `>= ${min} && <= ${max}`, value);
  }
  function validateInt32(value, name, min = -2147483648, max = 2147483647) {
    if (typeof value !== "number")
      throw ERR_INVALID_ARG_TYPE(name, "number", value);
    if (!Number.isInteger(value))
      throw ERR_OUT_OF_RANGE(name, "an integer", value);
    if (value < min || value > max)
      throw ERR_OUT_OF_RANGE(name, `>= ${min} && <= ${max}`, value);
  }
  function validateUint32(value, name, positive) {
    if (typeof value !== "number")
      throw ERR_INVALID_ARG_TYPE(name, "number", value);
    if (!Number.isInteger(value))
      throw ERR_OUT_OF_RANGE(name, "an integer", value);
    if (value !== value >>> 0) {
      const min = positive ? 1 : 0;
      throw ERR_OUT_OF_RANGE(name, `>= ${min} && < 4294967296`, value);
    }
    if (positive && value === 0)
      throw ERR_OUT_OF_RANGE(name, ">= 1 && < 4294967296", value);
  }
  function validateOneOf(value, name, oneOf) {
    if (!oneOf.includes(value)) {
      const allowed = oneOf
        .map((v) => (typeof v === "string" ? `'${v}'` : String(v)))
        .join(", ");
      throw ERR_INVALID_ARG_VALUE(name, value, "must be one of: " + allowed);
    }
  }

  // ---- crypto_util ---------------------------------------------------------
  const kHandle = Symbol("kHandle");
  const kState = Symbol("kState");
  const kFinalized = Symbol("kFinalized");

  const isAnyArrayBuffer = (v) =>
    v instanceof ArrayBuffer ||
    (typeof SharedArrayBuffer === "function" &&
      v instanceof SharedArrayBuffer);
  const isArrayBufferView = ArrayBuffer.isView;
  const isDataView = (v) => v instanceof DataView;
  const isUint8Array = (v) => v instanceof Uint8Array;

  function getStringOption(options, key) {
    let value;
    if (options && (value = options[key]) != null)
      validateString(value, `options.${key}`);
    return value;
  }
  function getArrayBufferOrView(buffer, name, encoding) {
    if (isAnyArrayBuffer(buffer)) return buffer;
    if (typeof buffer === "string") {
      if (encoding === undefined || encoding === "buffer") encoding = "utf8";
      return Buffer.from(buffer, encoding);
    }
    if (!isArrayBufferView(buffer)) {
      throw ERR_INVALID_ARG_TYPE(
        name,
        ["string", "ArrayBuffer", "Buffer", "TypedArray", "DataView"],
        buffer,
      );
    }
    return buffer;
  }
  function toBuf(val, encoding) {
    if (typeof val === "string") {
      if (encoding === "buffer") encoding = "utf8";
      return Buffer.from(val, encoding);
    }
    return val;
  }
  function validateByteSource(val, name) {
    val = toBuf(val);
    if (isAnyArrayBuffer(val) || isArrayBufferView(val)) return val;
    throw ERR_INVALID_ARG_TYPE(
      name,
      ["string", "ArrayBuffer", "TypedArray", "DataView", "Buffer"],
      val,
    );
  }
  // Normalize any accepted byte source to a Uint8Array view (no copy).
  const asU8 = (v) => {
    if (v instanceof Uint8Array) return v;
    if (isAnyArrayBuffer(v)) return new Uint8Array(v);
    return new Uint8Array(v.buffer, v.byteOffset, v.byteLength);
  };
  // A detached copy. Never `.slice()` here: Buffer overrides it as a view.
  const copyU8 = (v) => new Uint8Array(asU8(v));

  // ---- digest core ---------------------------------------------------------
  // name -> [host-op algorithm, digest length in bytes]
  const kDigests = {
    __proto__: null,
    md5: ["MD5", 16], sha1: ["SHA-1", 20], sha224: ["SHA-224", 28],
    sha256: ["SHA-256", 32], sha384: ["SHA-384", 48], sha512: ["SHA-512", 64],
  };
  const digestInfo = (name) =>
    kDigests[String(name).toLowerCase().replace("-", "")];

  // Workerd's C++ HashHandle, with the incremental EVP context replaced by
  // buffered chunks and a one-shot host op at digest time. `copy()` clones
  // the buffered chunks; digest-after-digest returns the cached bytes, and
  // update/copy after digest throw like Workerd's finalized context.
  class HashHandle {
    constructor(algorithm, xofLen, _chunks) {
      const info = digestInfo(algorithm);
      if (!info) throw new Error("Digest method not supported");
      if (xofLen !== undefined && xofLen !== info[1])
        throw new Error("invalid digest size");
      this.alg = algorithm;
      this.info = info;
      this.chunks = _chunks ? _chunks.slice() : [];
      this.result = null;
    }
    update(data) {
      if (this.result)
        throw new Error("Hash context has already been finalized.");
      this.chunks.push(copyU8(data));
      return 1;
    }
    digest() {
      if (this.result) return this.result.slice();
      const out = $$digest(this.info[0], concatU8(this.chunks));
      this.chunks = null;
      this.result = out;
      return out.slice();
    }
    copy(xofLen) {
      if (this.result)
        throw new Error("Hash context has already been finalized.");
      return new HashHandle(this.alg, xofLen, this.chunks);
    }
  }
  class HmacHandle {
    constructor(algorithm, key) {
      const info = digestInfo(algorithm);
      if (!info) throw new Error("Digest method not supported");
      this.info = info;
      // key: bytes, or a CryptoKey (from a KeyObject or WebCrypto).
      this.key = copyU8(
        key instanceof CryptoKey ? key.__celldMaterial.bytes : key);
      this.chunks = [];
    }
    update(data) {
      if (!this.chunks)
        throw new Error("HMAC context has already been finalized.");
      this.chunks.push(copyU8(data));
      return 1;
    }
    digest() {
      const sig = $$hmacSign(this.info[0], this.key, concatU8(this.chunks));
      this.chunks = null;
      if (!sig) throw new Error("HMAC digest failed");
      return sig;
    }
  }
  function concatU8(chunks) {
    let total = 0;
    for (const c of chunks) total += c.byteLength;
    const out = new Uint8Array(total);
    let off = 0;
    for (const c of chunks) { out.set(c, off); off += c.byteLength; }
    return out;
  }

  // ---- Hash / Hmac (crypto_hash.ts) ---------------------------------------
  // Real Transform streams, as in Node and Workerd: hashes are pipe targets
  // and readable sources (crypto-hash's hash_pipe_test pipes a PassThrough
  // into one and reads the digest back out). Materializing node:stream here
  // is lazy-on-lazy: only isolates that import node:crypto pay for it.
  const { Transform } = process.getBuiltinModule("node:stream");
  const streamify = (klass) => {
    Object.setPrototypeOf(klass.prototype, Transform.prototype);
    Object.setPrototypeOf(klass, Transform);
  };

  function createHash(algorithm, options) {
    return new Hash(algorithm, options);
  }
  function Hash(algorithm, options) {
    if (!(this instanceof Hash)) return new Hash(algorithm, options);
    const xofLen = typeof options === "object" && options !== null
      ? options.outputLength : undefined;
    if (xofLen !== undefined) validateUint32(xofLen, "options.outputLength");
    if (algorithm instanceof HashHandle) {
      this[kHandle] = algorithm.copy(xofLen);
    } else {
      validateString(algorithm, "algorithm");
      this[kHandle] = new HashHandle(algorithm, xofLen);
    }
    Transform.call(this, options);
    this[kState] = { [kFinalized]: false };
    return this;
  }
  Hash.prototype.copy = function (options) {
    if (this[kState][kFinalized]) throw ERR_CRYPTO_HASH_FINALIZED();
    return new Hash(this[kHandle], options);
  };
  Hash.prototype._transform = function (chunk, encoding, callback) {
    if (typeof chunk === "string") chunk = Buffer.from(chunk, encoding);
    this[kHandle].update(chunk);
    callback();
  };
  Hash.prototype._flush = function (callback) {
    this.push(Buffer.from(this[kHandle].digest()));
    callback();
  };
  Hash.prototype.update = function (data, encoding) {
    encoding ??= "utf8";
    if (encoding === "buffer") encoding = undefined;
    if (this[kState][kFinalized]) throw ERR_CRYPTO_HASH_FINALIZED();
    if (typeof data === "string") {
      data = Buffer.from(data, encoding);
    } else if (!isArrayBufferView(data)) {
      throw ERR_INVALID_ARG_TYPE(
        "data", ["string", "Buffer", "TypedArray", "DataView"], data);
    }
    if (!this[kHandle].update(data)) throw ERR_CRYPTO_HASH_UPDATE_FAILED();
    return this;
  };
  Hash.prototype.digest = function (outputEncoding) {
    if (this[kState][kFinalized]) throw ERR_CRYPTO_HASH_FINALIZED();
    const ret = Buffer.from(this[kHandle].digest());
    this[kState][kFinalized] = true;
    if (outputEncoding !== undefined && outputEncoding !== "buffer")
      return ret.toString(outputEncoding);
    return ret;
  };
  streamify(Hash);

  function createHmac(hmac, key, options) {
    return new Hmac(hmac, key, options);
  }
  function Hmac(hmac, key, options) {
    if (!(this instanceof Hmac)) return new Hmac(hmac, key, options);
    validateString(hmac, "hmac");
    const encoding = getStringOption(options, "encoding");
    if (key instanceof KeyObject) {
      if (key.type !== "secret")
        throw ERR_CRYPTO_INVALID_KEY_OBJECT_TYPE(key.type, "secret");
      this[kHandle] = new HmacHandle(hmac, key[kHandle]);
    } else if (key instanceof CryptoKey) {
      if (key.type !== "secret")
        throw ERR_CRYPTO_INVALID_KEY_OBJECT_TYPE(key.type, "secret");
      this[kHandle] = new HmacHandle(hmac, key);
    } else if (
      typeof key !== "string" &&
      !isArrayBufferView(key) &&
      !isAnyArrayBuffer(key)
    ) {
      throw ERR_INVALID_ARG_TYPE(
        "key",
        ["ArrayBuffer", "Buffer", "ArrayBufferView", "string", "KeyObject",
          "CryptoKey"],
        key,
      );
    } else {
      this[kHandle] =
        new HmacHandle(hmac, getArrayBufferOrView(key, "key", encoding));
    }
    Transform.call(this, options);
    this[kState] = { [kFinalized]: false };
    return this;
  }
  Hmac.prototype.update = Hash.prototype.update;
  Hmac.prototype.digest = function (outputEncoding) {
    if (this[kState][kFinalized]) {
      return !outputEncoding || outputEncoding === "buffer"
        ? Buffer.from("") : "";
    }
    const ret = Buffer.from(this[kHandle].digest());
    this[kState][kFinalized] = true;
    if (outputEncoding !== undefined && outputEncoding !== "buffer")
      return ret.toString(outputEncoding);
    return ret;
  };
  Hmac.prototype._flush = Hash.prototype._flush;
  Hmac.prototype._transform = Hash.prototype._transform;
  streamify(Hmac);

  function hash(algorithm, data, outputEncoding = "hex") {
    validateString(algorithm, "algorithm");
    validateString(outputEncoding, "outputEncoding");
    if (typeof data === "string" || isArrayBufferView(data)) {
      const h = createHash(algorithm);
      h.update(data, "utf8");
      return h.digest(outputEncoding);
    }
    throw ERR_INVALID_ARG_TYPE(
      "data", ["string", "Buffer", "TypedArray", "DataView"], data);
  }

  // ---- keys (crypto_keys.ts; secret keys only) ----------------------------
  const kInspect = Symbol.for("nodejs.util.inspect.custom");
  const kPromisifyArgs = Symbol.for("nodejs.util.promisify.custom.args");

  const keyEquals = (a, b) => {
    if (a === b) return true;
    if (!a.extractable || !b.extractable) return false;
    const ab = a.__celldMaterial?.bytes, bb = b.__celldMaterial?.bytes;
    if (!ab || !bb || ab.byteLength !== bb.byteLength) return false;
    let diff = 0;
    for (let i = 0; i < ab.byteLength; i++) diff |= ab[i] ^ bb[i];
    return diff === 0;
  };
  const keyMaterial = (key) => {
    const bytes = key.__celldMaterial?.bytes;
    if (!bytes) {
      throw new Error(
        "key material is not exportable through node:crypto yet");
    }
    return asU8(bytes);
  };

  // The ciphers celld can seal a PKCS#8 export under. Node reaches every
  // cipher OpenSSL carries, so this list is the short one, and a name outside
  // it is refused by name -- a silent fall back to one of these would seal the
  // key under a cipher the caller never asked for.
  const PKCS8_CIPHERS = ["aes-128-cbc", "aes-192-cbc", "aes-256-cbc"];

  function validateExportOptions(options, type, name = "options") {
    validateObject(options, name);
    if (options.format !== undefined)
      validateString(options.format, `${name}.format`);
    else options.format = "buffer";
    if ("type" in options && options.type !== undefined)
      validateString(options.type, `${name}.type`);
    if (type === "private" && "cipher" in options &&
        options.cipher !== undefined) {
      validateString(options.cipher, `${name}.cipher`);
      // OpenSSL resolves a cipher name case-insensitively, so Node takes
      // "AES-256-CBC" as readily as the lower-case spelling. The host matches
      // the lower-case name only, so normalize here, once, for both callers.
      options.cipher = options.cipher.toLowerCase();
      if (!PKCS8_CIPHERS.includes(options.cipher))
        throw ERR_METHOD_NOT_IMPLEMENTED(`${name}.cipher ${options.cipher}`);
      // Encryption here is the PKCS#8 wrapper and nothing else. For sec1 and
      // pkcs1 Node writes a traditional PEM instead -- an "EC PRIVATE KEY"
      // body behind Proc-Type and DEK-Info headers -- which celld cannot
      // produce, so it is refused rather than answered with a PKCS#8 key
      // under a header the caller did not ask for.
      if (options.type !== undefined && options.type !== "pkcs8")
        throw ERR_METHOD_NOT_IMPLEMENTED(`an encrypted ${options.type} export`);
      if (typeof options.passphrase === "string")
        options.passphrase = Buffer.from(options.passphrase, options.encoding);
      if (!isUint8Array(options.passphrase)) {
        throw ERR_INVALID_ARG_TYPE(
          `${name}.passphrase`, ["string", "Uint8Array"], options.passphrase);
      }
    } else if (type === "private" && options.passphrase !== undefined) {
      // A passphrase with no cipher named nothing to encrypt under. Node
      // refuses it, and so must celld: the alternative is to pick a cipher
      // the caller never asked for, or to write the key out in the clear.
      throw ERR_INVALID_ARG_VALUE(`${name}.cipher`, options.cipher);
    }
  }

  class KeyObject {
    constructor() {
      throw new Error("Illegal constructor");
    }
    static from(key) {
      if (!(key instanceof CryptoKey))
        throw ERR_INVALID_ARG_TYPE("key", "CryptoKey", key);
      switch (key.type) {
        case "secret":
          return Reflect.construct(function () {
            this[kHandle] = key;
          }, [], SecretKeyObject);
        case "private":
          return Reflect.construct(function () {
            this[kHandle] = key;
          }, [], PrivateKeyObject);
        case "public":
          return Reflect.construct(function () {
            this[kHandle] = key;
          }, [], PublicKeyObject);
      }
    }
    export(options = {}) {
      validateObject(options, "options");
      validateExportOptions(options, this.type);
      const bytes = keyMaterial(this[kHandle]);
      if (options.format === "jwk" && this.type === "secret") {
        return {
          kty: "oct",
          k: Buffer.from(bytes).toString("base64url"),
          ext: true,
        };
      }
      if (options.format === "jwk") {
        return keyOp("asym-key-export", {
          der: Array.from(bytes),
          visibility: this.type,
        }).jwk;
      }
      // Asymmetric: the host re-encodes into the requested structure. Node's
      // shapes -- a pem export is a string, a der export a Buffer.
      if (this.type !== "secret") {
        const out = keyOp("asym-key-reencode", {
          der: Array.from(bytes),
          visibility: this.type,
          format: options.format,
          type: options.type,
          // A passphrase makes this an encrypted PKCS#8 export, and the
          // cipher names the scheme the host encrypts under.
          cipher: options.cipher,
          passphrase: options.passphrase === undefined ? null : Array.from(
            asU8(typeof options.passphrase === "string"
              ? Buffer.from(options.passphrase)
              : options.passphrase)),
        });
        return out.pem !== undefined ? out.pem : Buffer.from(out.der);
      }
      return Buffer.from(bytes);
    }
    equals(otherKeyObject) {
      if (this === otherKeyObject ||
          this[kHandle] === otherKeyObject[kHandle]) return true;
      if (this.type !== otherKeyObject.type) return false;
      if (!(otherKeyObject[kHandle] instanceof CryptoKey)) {
        throw ERR_INVALID_ARG_TYPE(
          "otherKeyObject", "KeyObject", otherKeyObject);
      }
      return keyEquals(this[kHandle], otherKeyObject[kHandle]);
    }
    get [Symbol.toStringTag]() {
      return "KeyObject";
    }
  }
  const isKeyObject = (obj) =>
    obj != null && typeof obj === "object" && kHandle in obj;

  // The host normalizes every accepted input to one DER — PKCS#8 for
  // private keys, SPKI for public — and reports the type and details beside
  // it. Everything below reads that one shape.
  const keyOp = (operation, input) => {
    try {
      return JSON.parse(__crypto_operation(operation, JSON.stringify(input)));
    } catch (error) {
      // The host namespaces every throw with "crypto: ". node:crypto callers
      // match on Node's own message text — `throws(fn, { message })` compares
      // it exactly — so the namespace is stripped at this boundary.
      throw new Error(
        String(error?.message ?? error).replace(/^crypto: /, ""));
    }
  };
  const asymmetricMaterial = (key) => key.__celldMaterial ?? {};
  // Node names the Web Crypto algorithm, not the key type, on the CryptoKey.
  // Node accepts several spellings of the same curve; celld generates P-256
  // only, so the table doubles as the supported-curve check.
  const EC_CURVE_ALIASES = {
    "prime256v1": "P-256",
    "secp256r1": "P-256",
    "P-256": "P-256",
  };
  const WEB_CRYPTO_ALGORITHM = {
    rsa: "RSASSA-PKCS1-v1_5",
    ec: "ECDSA",
    ed25519: "Ed25519",
    x25519: "X25519",
  };

  function asymmetricKeyObject(result, visibility) {
    const material = {
      bytes: Uint8Array.from(result.der),
      keyType: result.keyType,
      details: result.details,
    };
    // The same builder Web Crypto's own importKey uses, so a key that
    // arrives through node:crypto reports what the same key reports after
    // crypto.subtle.importKey(). There is no request dictionary here, so
    // an RSA key takes the SHA-256 that builder defaults to.
    const algorithm = $$keyAlgorithm(
      WEB_CRYPTO_ALGORITHM[result.keyType], null, result.keyType,
      result.details);
    return KeyObject.from(
      new CryptoKey(visibility, algorithm, true, [], material));
  }

  class AsymmetricKeyObject extends KeyObject {
    get asymmetricKeyDetails() {
      const details = { ...asymmetricMaterial(this[kHandle]).details };
      // Node reports the exponent as a BigInt. JSON cannot carry one, so the
      // host sends a decimal string and it is widened back here.
      if (details.publicExponent !== undefined)
        details.publicExponent = BigInt(details.publicExponent);
      return details;
    }
    get asymmetricKeyType() {
      return asymmetricMaterial(this[kHandle]).keyType;
    }
    // The handle already *is* a CryptoKey; Node's contract is only that the
    // result is usable with Web Crypto, and this key was built with the
    // algorithm Web Crypto expects.
    toCryptoKey() {
      return this[kHandle];
    }
  }
  class PublicKeyObject extends AsymmetricKeyObject {
    get type() { return "public"; }
  }
  class PrivateKeyObject extends AsymmetricKeyObject {
    get type() { return "private"; }
  }
  class SecretKeyObject extends KeyObject {
    get symmetricKeySize() {
      return keyMaterial(this[kHandle]).byteLength;
    }
    get type() { return "secret"; }
    [kInspect](depth, options) {
      if (depth < 0) return this;
      return `SecretKeyObject { size: ${this.symmetricKeySize} }`;
    }
  }

  function validateKeyData(key, name) {
    if (key == null ||
        (typeof key !== "string" && !isArrayBufferView(key) &&
          !isAnyArrayBuffer(key))) {
      throw ERR_INVALID_ARG_TYPE(
        name, ["string", "ArrayBuffer", "TypedArray", "DataView"], key);
    }
  }
  function createSecretKey(key, encoding) {
    validateKeyData(key, "key");
    // Always copy: Buffer.from(string) may live in the shared pool, and view
    // inputs stay owned by the caller — key material must be immutable.
    const bytes = copyU8(
      typeof key === "string" ? Buffer.from(key, encoding) : key);
    const handle = new CryptoKey(
      "secret", { name: "secret" }, true, [], { bytes });
    return KeyObject.from(handle);
  }

  const KeyContext = {
    kCreatePublic: "kCreatePublic",
    kCreatePrivate: "kCreatePrivate",
  };
  const isStringOrBuffer = (v) =>
    typeof v === "string" || isArrayBufferView(v) || isAnyArrayBuffer(v);
  function prepareAsymmetricKey(key, ctx) {
    if (key == null) {
      throw ERR_INVALID_ARG_TYPE(
        "key",
        ["ArrayBuffer", "Buffer", "TypedArray", "DataView", "string",
          "object"],
        key,
      );
    }
    const normalized = isStringOrBuffer(key) ? { key, format: "pem" } : key;
    const {
      key: data, encoding = "utf8", format = "pem", type, passphrase,
    } = normalized;
    if (data == null || isKeyObject(data) || data instanceof CryptoKey) {
      throw ERR_INVALID_ARG_TYPE(
        "options.key",
        ["ArrayBuffer", "Buffer", "TypedArray", "DataView", "string",
          "object"],
        data,
      );
    }
    if (isStringOrBuffer(data)) {
      validateOneOf(format, "format", ["pem", "der"]);
      if (type !== undefined) {
        if (ctx === KeyContext.kCreatePrivate)
          validateOneOf(type, "type", ["pkcs1", "pkcs8", "sec1"]);
        else if (ctx === KeyContext.kCreatePublic)
          validateOneOf(type, "type", ["pkcs1", "spki"]);
      }
      return {
        key: getArrayBufferOrView(data, "key", encoding),
        format, type,
        passphrase: passphrase != null
          ? getArrayBufferOrView(passphrase, "passphrase", encoding)
          : undefined,
      };
    }
    if (typeof data !== "object") {
      throw ERR_INVALID_ARG_TYPE(
        "key",
        ["ArrayBuffer", "Buffer", "TypedArray", "DataView", "string",
          "object"],
        key,
      );
    }
    return { key: data, format: "jwk", type: undefined, passphrase: undefined };
  }
  // A JWK arrives as an object and crosses as one; every other format is
  // bytes, and the host reads a plain array.
  function importAsymmetric(prepared, visibility) {
    return asymmetricKeyObject(
      keyOp("asym-key-import", {
        key: prepared.format === "jwk"
          ? prepared.key
          : Array.from(asU8(prepared.key)),
        format: prepared.format,
        type: prepared.type ?? null,
        visibility,
        passphrase: prepared.passphrase === undefined
          ? null
          : Array.from(asU8(prepared.passphrase)),
      }),
      visibility,
    );
  }
  function createPrivateKey(key) {
    return importAsymmetric(
      prepareAsymmetricKey(key, KeyContext.kCreatePrivate), "private");
  }
  function createPublicKey(key) {
    // Node also derives the public half of a private key it is handed.
    if (isKeyObject(key) || key instanceof CryptoKey) {
      if (key.type !== "private")
        throw ERR_INVALID_ARG_TYPE("key", "PrivateKeyObject", key);
      const handle = key instanceof CryptoKey ? key : key[kHandle];
      return asymmetricKeyObject(
        keyOp("asym-key-public-from-private", {
          der: Array.from(asU8(asymmetricMaterial(handle).bytes)),
        }),
        "public",
      );
    }
    return importAsymmetric(
      prepareAsymmetricKey(key, KeyContext.kCreatePublic), "public");
  }

  function generateKey(type, options, callback) {
    try {
      const result = generateKeySync(type, options);
      queueMicrotask(() => { callback(null, result); });
    } catch (err) {
      queueMicrotask(() => { callback(err); });
    }
  }
  function generateKeySync(type, options) {
    validateOneOf(type, "type", ["hmac", "aes"]);
    validateObject(options, "options");
    const { length } = options;
    switch (type) {
      case "hmac": {
        validateInteger(length, "options.length", 8, 65536);
        return createSecretKey(randomBytes(Math.floor(length / 8)));
      }
      case "aes": {
        validateOneOf(length, "options.length", [128, 192, 256]);
        return createSecretKey(randomBytes(length / 8));
      }
    }
  }
  function generateKeyPair(type, options, callback) {
    validateFunction(callback, "callback");
    try {
      const { publicKey, privateKey } = generateKeyPairSync(type, options);
      queueMicrotask(() => { callback(null, publicKey, privateKey); });
    } catch (err) {
      queueMicrotask(() => { callback(err); });
    }
  }
  Object.defineProperty(generateKeyPair, kPromisifyArgs, {
    value: ["publicKey", "privateKey"],
    enumerable: false,
  });
  // Workerd's full argument validation; the generation itself (RSA/EC/EdDSA/
  // DH key synthesis with DER/PEM export) is not implemented in Cells.
  function generateKeyPairSync(type, options = {}) {
    validateOneOf(type, "type", ["rsa", "ec", "ed25519", "x25519", "dh"]);
    validateObject(options, "options");
    const {
      modulusLength, publicExponent = 0x10001, namedCurve, prime,
      primeLength, generator, group, groupName, paramEncoding = "named",
      publicKeyEncoding, privateKeyEncoding,
    } = options;
    if (publicKeyEncoding !== undefined) {
      validateExportOptions(
        publicKeyEncoding, "public", "options.publicKeyEncoding");
    }
    if (privateKeyEncoding !== undefined) {
      validateExportOptions(
        privateKeyEncoding, "private", "options.privateKeyEncoding");
    }
    switch (type) {
      case "rsa":
        validateUint32(modulusLength, "options.modulusLength");
        validateUint32(publicExponent, "options.publicExponent");
        break;
      case "ec":
        validateString(namedCurve, "options.namedCurve");
        validateOneOf(paramEncoding, "options.paramEncoding",
          ["named", "explicit"]);
        break;
      case "ed25519":
      case "x25519":
        break;
      case "dh": {
        if (generator != null)
          validateInt32(generator, "options.generator", 0);
        if (group != null || groupName != null) {
          if (prime != null) throw ERR_INCOMPATIBLE_OPTION_PAIR("group", "prime");
          if (primeLength != null)
            throw ERR_INCOMPATIBLE_OPTION_PAIR("group", "primeLength");
          if (generator != null)
            throw ERR_INCOMPATIBLE_OPTION_PAIR("group", "generator");
          validateString(group || groupName, "options.group");
          break;
        }
        if (prime != null) {
          if (primeLength != null)
            throw ERR_INCOMPATIBLE_OPTION_PAIR("prime", "primeLength");
          if (!isArrayBufferView(prime) && !isAnyArrayBuffer(prime)) {
            throw ERR_INVALID_ARG_TYPE(
              "options.prime", ["Buffer", "TypedArray", "ArrayBuffer"], prime);
          }
        } else if (primeLength != null) {
          validateInt32(primeLength, "options.primeLength", 0);
        } else {
          throw ERR_MISSING_OPTION(
            "At least one of the group, prime, or primeLength options");
        }
        break;
      }
    }
    // Everything above is Workerd's argument validation, unchanged. Below is
    // the generation itself, for the algorithms celld carries a crate for.
    if (type === "dh")
      throw ERR_METHOD_NOT_IMPLEMENTED(`generateKeyPairSync ${type}`);
    if (type === "rsa") {
      // Node's ceiling. Beyond it the prime search is effectively unbounded,
      // so it is refused rather than left to run.
      if (modulusLength > 16384)
        throw new RangeError(`Invalid modulusLength: ${modulusLength}`);
      // The rsa crate fixes the exponent at 65537; anything else would be
      // silently ignored, so it is refused instead.
      if (publicExponent !== 0x10001) {
        throw ERR_METHOD_NOT_IMPLEMENTED(
          `generateKeyPairSync rsa with publicExponent ${publicExponent}`);
      }
    }
    if (type === "ec" && EC_CURVE_ALIASES[namedCurve] === undefined) {
      throw ERR_METHOD_NOT_IMPLEMENTED(
        `generateKeyPairSync ec ${namedCurve}`);
    }
    const pair = keyOp("asym-key-generate", { type, modulusLength });
    const half = (der, visibility, encoding) => {
      if (encoding === undefined) {
        return asymmetricKeyObject(
          { keyType: pair.keyType, der, details: pair.details }, visibility);
      }
      // With an encoding, Node returns the encoded key itself rather than a
      // KeyObject: a string for pem, a Buffer for der. A private encoding
      // can carry a cipher and a passphrase, which seal the key the same way
      // `KeyObject.export` does -- celld dropped them before, so a caller
      // that asked for an encrypted key got a plaintext one.
      const out = keyOp("asym-key-reencode", {
        der,
        visibility,
        format: encoding.format,
        type: encoding.type,
        cipher: encoding.cipher,
        passphrase: encoding.passphrase === undefined ? null : Array.from(
          asU8(encoding.passphrase)),
      });
      return out.pem !== undefined ? out.pem : Buffer.from(out.der);
    };
    return {
      publicKey: half(pair.publicDer, "public", publicKeyEncoding),
      privateKey: half(pair.privateDer, "private", privateKeyEncoding),
    };
  }

  // ---- one-shot sign / verify ----------------------------------------
  // Node's `sign(algorithm, data, key)`. For Ed25519 the algorithm must be
  // null -- the curve fixes its own hash. For RSA and ECDSA it names the
  // digest. ECDSA signatures are DER here, as Node's `dsaEncoding` default
  // says, not the raw r||s Web Crypto uses.
  const SIGN_HASHES = {
    sha256: "SHA-256", "sha-256": "SHA-256",
    sha384: "SHA-384", "sha-384": "SHA-384",
    sha512: "SHA-512", "sha-512": "SHA-512",
  };
  function signingKey(key, want, name) {
    const object = isKeyObject(key)
      ? key
      : (key instanceof CryptoKey ? KeyObject.from(key) : createPrivateKey(key));
    if (object.type !== want) {
      throw ERR_CRYPTO_INVALID_KEY_OBJECT_TYPE(object.type, want);
    }
    const material = asymmetricMaterial(object[kHandle]);
    if (material.keyType === undefined)
      throw ERR_INVALID_ARG_TYPE(name, "asymmetric KeyObject", key);
    return material;
  }
  function signHash(algorithm, keyType) {
    // Ed25519 fixes its own hash, so the algorithm must be null for it and
    // must be a digest name for everything else. Accepting a digest for
    // Ed25519 would silently ignore it.
    if (keyType === "ed25519") {
      if (algorithm != null)
        throw ERR_INVALID_ARG_VALUE("algorithm", algorithm);
      return null;
    }
    if (algorithm == null)
      throw ERR_INVALID_ARG_VALUE("algorithm", algorithm);
    validateString(algorithm, "algorithm");
    const hash = SIGN_HASHES[algorithm.toLowerCase()];
    if (hash === undefined)
      throw ERR_METHOD_NOT_IMPLEMENTED(`sign with ${algorithm}`);
    return hash;
  }
  function sign(algorithm, data, key) {
    const material = signingKey(key, "private", "key");
    const hash = signHash(algorithm, material.keyType);
    const input = Array.from(asU8(data));
    const der = Array.from(asU8(material.bytes));
    switch (material.keyType) {
      case "ed25519":
        return Buffer.from(
          keyOp("ed25519-sign", { key: der, data: input }).bytes);
      case "rsa":
        return Buffer.from(
          keyOp("rsa-pkcs1-sign", { key: der, data: input, hash }).bytes);
      case "ec":
        return Buffer.from(
          keyOp("ec-sign-der", { key: der, data: input, hash }).bytes);
      default:
        throw ERR_METHOD_NOT_IMPLEMENTED(`sign with ${material.keyType}`);
    }
  }
  function verify(algorithm, data, key, signature) {
    // Node accepts a private key here and verifies with its public half.
    let material = signingKey(
      isKeyObject(key) && key.type === "private" ? createPublicKey(key) : key,
      "public",
      "key",
    );
    const hash = signHash(algorithm, material.keyType);
    const input = Array.from(asU8(data));
    const der = Array.from(asU8(material.bytes));
    const sig = Array.from(asU8(signature));
    switch (material.keyType) {
      case "ed25519":
        return keyOp("ed25519-verify", { key: der, data: input, signature: sig }).ok;
      case "rsa":
        return keyOp(
          "rsa-pkcs1-verify", { key: der, data: input, signature: sig, hash }).ok;
      case "ec":
        return keyOp(
          "ec-verify-der", {
            key: der, data: input, signature: sig, hash,
          }).ok;
      default:
        throw ERR_METHOD_NOT_IMPLEMENTED(`verify with ${material.keyType}`);
    }
  }

  // ---- pbkdf2 --------------------------------------------------------------
  function pbkdf2Check(password, salt, iterations, keylen, digest) {
    validateString(digest, "digest");
    password = getArrayBufferOrView(password, "password");
    salt = getArrayBufferOrView(salt, "salt");
    validateInt32(iterations, "iterations", 1);
    validateInt32(keylen, "keylen", 0);
    return { password, salt, iterations, keylen, digest };
  }
  function getPbkdf(password, salt, iterations, keylen, digest) {
    const info = digestInfo(digest);
    if (!info) throw new TypeError(`Invalid Pbkdf2 digest: ${digest}`);
    if (keylen > 255 * info[1]) {
      throw new RangeError(
        "Pbkdf2 failed: derived key length exceeds maximum for this hash");
    }
    return $$pbkdf2(info[0], asU8(password), asU8(salt), iterations, keylen);
  }
  function pbkdf2Sync(password, salt, iterations, keylen, digest) {
    ({ password, salt, iterations, keylen, digest } =
      pbkdf2Check(password, salt, iterations, keylen, digest));
    return Buffer.from(getPbkdf(password, salt, iterations, keylen, digest));
  }
  function pbkdf2(password, salt, iterations, keylen, digest, callback) {
    if (typeof digest === "function") validateString(undefined, "digest");
    validateFunction(callback, "callback");
    ({ password, salt, iterations, keylen, digest } =
      pbkdf2Check(password, salt, iterations, keylen, digest));
    new Promise((resolve, reject) => {
      try {
        resolve(getPbkdf(password, salt, iterations, keylen, digest));
      } catch (err) { reject(err); }
    }).then(
      (val) => callback(null, Buffer.from(val)),
      (err) => callback(err),
    );
  }

  // ---- hkdf ----------------------------------------------------------------
  function hkdfPrepareKey(key) {
    key = toBuf(key);
    if (!isAnyArrayBuffer(key) && !isArrayBufferView(key)) {
      throw ERR_INVALID_ARG_TYPE(
        "ikm",
        ["string", "SecretKeyObject", "ArrayBuffer", "TypedArray", "DataView",
          "Buffer"],
        key,
      );
    }
    return key;
  }
  function hkdfValidate(hash, key, salt, info, length) {
    if (key instanceof KeyObject) key = key.export();
    validateString(hash, "digest");
    key = hkdfPrepareKey(key);
    salt = validateByteSource(salt, "salt");
    info = validateByteSource(info, "info");
    validateInteger(length, "length", 0, kMaxLength);
    if (info.byteLength > 1024) {
      throw ERR_OUT_OF_RANGE(
        "info", "must not contain more than 1024 bytes", info.byteLength);
    }
    return { hash, key, salt, info, length };
  }
  function getHkdf(hash, key, salt, info, length) {
    const md = digestInfo(hash);
    if (!md) throw new TypeError(`Invalid Hkdf digest: ${hash}`);
    if (length > 255 * md[1])
      throw new RangeError("Invalid Hkdf key length");
    const out = $$hkdf(md[0], asU8(key), asU8(salt), asU8(info), length);
    return out.buffer;
  }
  function hkdf(hash, key, salt, info, length, callback) {
    ({ hash, key, salt, info, length } =
      hkdfValidate(hash, key, salt, info, length));
    validateFunction(callback, "callback");
    new Promise((resolve, reject) => {
      try { resolve(getHkdf(hash, key, salt, info, length)); }
      catch (err) { reject(err); }
    }).then(
      (val) => callback(null, val),
      (err) => callback(err),
    );
  }
  function hkdfSync(hash, key, salt, info, length) {
    ({ hash, key, salt, info, length } =
      hkdfValidate(hash, key, salt, info, length));
    return getHkdf(hash, key, salt, info, length);
  }

  // ---- random (crypto_random.ts) ------------------------------------------
  // $$randomValues fills any view (no WebCrypto 65,536-byte quota).
  const fillRandom = (view) => { $$randomValues(view); return view; };

  function randomBytes(size, callback) {
    validateInteger(size, "size", 0, kMaxLength);
    const buf = Buffer.alloc(size);
    if (callback !== undefined) { randomFill(buf, callback); return; }
    randomFillSync(buf);
    return buf;
  }
  function randomFillSync(buffer, offset, size) {
    if (!isAnyArrayBuffer(buffer) && !isArrayBufferView(buffer)) {
      throw ERR_INVALID_ARG_TYPE(
        "buffer",
        ["TypedArray", "DataView", "ArrayBuffer", "SharedArrayBuffer"],
        buffer,
      );
    }
    const maxLength = buffer.byteLength;
    if (offset !== undefined) validateInteger(offset, "offset", 0, kMaxLength);
    else offset = 0;
    if (size !== undefined)
      validateInteger(size, "size", 0, maxLength - offset);
    else size = maxLength - offset;
    let view = buffer;
    if (isAnyArrayBuffer(view)) view = new Uint8Array(view);
    else if (isDataView(view) || !(view instanceof Uint8Array))
      view = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
    fillRandom(view.subarray(offset, offset + size));
    return buffer;
  }
  function randomFill(buffer, offsetOrCallback, sizeOrCallback, callback) {
    if (!isAnyArrayBuffer(buffer) && !isArrayBufferView(buffer)) {
      throw ERR_INVALID_ARG_TYPE(
        "buffer",
        ["TypedArray", "DataView", "ArrayBuffer", "SharedArrayBuffer"],
        buffer,
      );
    }
    let offset = 0, size = 0;
    const maxLength = buffer.byteLength;
    if (typeof callback === "function") {
      validateInteger(offsetOrCallback, "offset", 0, maxLength);
      offset = offsetOrCallback;
      validateInteger(sizeOrCallback, "size", 0, maxLength - offset);
      size = sizeOrCallback;
    } else if (typeof sizeOrCallback === "function") {
      validateInteger(offsetOrCallback, "offset", 0, maxLength);
      offset = offsetOrCallback;
      size = maxLength - offset;
      callback = sizeOrCallback;
    } else if (typeof offsetOrCallback === "function") {
      offset = 0;
      size = maxLength;
      callback = offsetOrCallback;
    }
    validateFunction(callback, "callback");
    new Promise((resolve) => {
      randomFillSync(buffer, offset, size);
      resolve();
    }).then(
      () => callback(null, buffer),
      (err) => callback(err),
    );
  }

  const RAND_MAX = 0xffff_ffff_ffff;
  const randomCache = Buffer.alloc(6 * 1024);
  let randomCacheOffset = randomCache.length;
  function getRandomInt(min, max) {
    const range = max - min;
    if (!(range <= RAND_MAX)) {
      throw ERR_OUT_OF_RANGE(
        `max${max ? "" : " - min"}`, `<= ${RAND_MAX}`, range);
    }
    const randLimit = RAND_MAX - (RAND_MAX % range);
    for (;;) {
      if (randomCacheOffset === randomCache.length) {
        randomFillSync(randomCache);
        randomCacheOffset = 0;
      }
      const x = randomCache.readUIntBE(randomCacheOffset, 6);
      randomCacheOffset += 6;
      if (x < randLimit) return (x % range) + min;
    }
  }
  function randomInt(minOrMax, maxOrCallback, callback) {
    let min = 0, max = 0;
    if (typeof callback === "function") {
      validateInteger(minOrMax, "min");
      validateInteger(maxOrCallback, "max");
      min = minOrMax;
      max = maxOrCallback;
    } else if (typeof maxOrCallback === "function") {
      validateInteger(minOrMax, "max");
      max = minOrMax;
      callback = maxOrCallback;
    } else if (arguments.length === 2) {
      validateInteger(minOrMax, "min");
      validateInteger(maxOrCallback, "max");
      min = minOrMax;
      max = maxOrCallback;
    } else {
      validateInteger(minOrMax, "max");
      max = minOrMax;
    }
    if (min >= max) throw ERR_OUT_OF_RANGE("min", "min < max", min);
    if (callback != null) {
      new Promise((resolve) => { resolve(getRandomInt(min, max)); }).then(
        (n) => callback(null, n),
        (err) => callback(err),
      );
      return;
    }
    return getRandomInt(min, max);
  }
  function randomUUID(options) {
    if (options !== undefined) {
      validateObject(options, "options");
      if (options.disableEntropyCache !== undefined) {
        validateBoolean(
          options.disableEntropyCache, "options.disableEntropyCache");
      }
    }
    return crypto.randomUUID();
  }

  // ---- primes (BigInt port of Workerd's prime.c++) ------------------------
  const kMaxPrimeBits = 8192;
  const smallPrimes = (() => {
    const N = 2048;
    const composite = new Uint8Array(N + 1);
    const out = [];
    for (let i = 2; i <= N; i++) {
      if (composite[i]) continue;
      out.push(BigInt(i));
      for (let j = i * i; j <= N; j += i) composite[j] = 1;
    }
    return out;
  })();
  const bitLength = (n) => (n === 0n ? 0 : n.toString(2).length);
  function bigFromBytes(u8) {
    let n = 0n;
    for (const b of u8) n = (n << 8n) | BigInt(b);
    return n;
  }
  function bigToBytes(n, len) {
    const out = new Uint8Array(len);
    for (let i = len - 1; i >= 0; i--) {
      out[i] = Number(n & 0xffn);
      n >>= 8n;
    }
    return out;
  }
  function modPow(base, exp, mod) {
    let result = 1n;
    base %= mod;
    while (exp > 0n) {
      if (exp & 1n) result = (result * base) % mod;
      base = (base * base) % mod;
      exp >>= 1n;
    }
    return result;
  }
  function millerRabinRound(n, d, r, a) {
    let x = modPow(a, d, n);
    if (x === 1n || x === n - 1n) return true;
    for (let i = 1n; i < r; i++) {
      x = (x * x) % n;
      if (x === n - 1n) return true;
    }
    return false;
  }
  function randomBase(n) {
    // uniform-enough random base in [2, n - 2]
    const bytes = Math.ceil(bitLength(n) / 8) + 8;
    const raw = bigFromBytes(fillRandom(new Uint8Array(bytes)));
    return 2n + (raw % (n - 3n));
  }
  function isPrimeBig(n, checks) {
    if (n < 2n) return false;
    for (const p of smallPrimes) {
      if (n === p) return true;
      if (n % p === 0n) return false;
    }
    if (bitLength(n) <= 11) return true; // trial-divided exhaustively
    let d = n - 1n, r = 0n;
    while ((d & 1n) === 0n) { d >>= 1n; r++; }
    const rounds = checks > 0 ? checks : 32;
    if (!millerRabinRound(n, d, r, 2n)) return false;
    for (let i = 1; i < rounds; i++) {
      if (!millerRabinRound(n, d, r, randomBase(n))) return false;
    }
    return true;
  }
  // Fast pre-screen for candidate search: trial division only.
  const passesTrialDivision = (n) => {
    for (const p of smallPrimes) {
      if (n === p) return true;
      if (n % p === 0n) return false;
    }
    return true;
  };
  function randomPrime(size, safe, addBuf, remBuf) {
    if (size > kMaxPrimeBits) {
      throw new RangeError(
        `generatePrime size exceeds maximum (${kMaxPrimeBits} bits)`);
    }
    let add, rem;
    if (addBuf !== undefined) {
      add = bigFromBytes(asU8(addBuf));
      rem = remBuf !== undefined ? bigFromBytes(asU8(remBuf)) : undefined;
      // Workerd restricts add/rem to the pairings BN_generate_prime_ex
      // cannot loop on: (12,11), (24,23), (60,59).
      const pair = (a, b) => add === a && rem === b;
      if (rem === undefined ||
          !(pair(12n, 11n) || pair(24n, 23n) || pair(60n, 59n)))
        throw new RangeError("Invalid values for add and rem");
      if (add <= rem)
        throw new RangeError("options.rem must be smaller than options.add");
      if (bitLength(add) > size) {
        throw new RangeError(
          "options.add must not be bigger than size of the requested prime");
      }
    }
    const bytes = Math.ceil(size / 8);
    const topBits = size >= 2
      ? (1n << BigInt(size - 1)) | (1n << BigInt(size - 2))
      : 1n << BigInt(size - 1);
    for (;;) {
      let n = bigFromBytes(fillRandom(new Uint8Array(bytes)));
      n &= (1n << BigInt(size)) - 1n;
      n |= topBits | 1n;
      let step = 2n;
      if (add !== undefined) {
        n = n - (n % add) + rem;
        step = add;
      } else if (safe) {
        if (n % 4n === 1n) n += 2n; // n is odd; force n ≡ 3 (mod 4)
        step = 4n;
      }
      for (let i = 0; i < 4096; i++, n += step) {
        if (bitLength(n) !== size) break; // walked out of the window
        if (!passesTrialDivision(n)) continue;
        if (safe && !isPrimeBig((n - 1n) >> 1n, 0)) continue;
        if (!isPrimeBig(n, 0)) continue;
        return bigToBytes(n, bytes).buffer;
      }
    }
  }
  function bufToUnsignedBigInt(buf) {
    return bigFromBytes(new Uint8Array(buf));
  }
  function unsignedBigIntToBuffer(bigint, name) {
    if (bigint < 0) throw ERR_OUT_OF_RANGE(name, ">= 0", bigint);
    const hex = bigint.toString(16);
    const padded = hex.padStart(hex.length + (hex.length % 2), "0");
    return Buffer.from(padded, "hex");
  }
  function processGeneratePrimeOptions(options) {
    validateObject(options, "options");
    const { safe = false, bigint = false } = options;
    let { add, rem } = options;
    validateBoolean(safe, "options.safe");
    validateBoolean(bigint, "options.bigint");
    if (add !== undefined) {
      if (typeof add === "bigint") {
        add = unsignedBigIntToBuffer(add, "options.add");
      } else if (!isAnyArrayBuffer(add) && !isArrayBufferView(add)) {
        throw ERR_INVALID_ARG_TYPE(
          "options.add",
          ["ArrayBuffer", "TypedArray", "Buffer", "DataView", "bigint"],
          add,
        );
      }
    }
    if (rem !== undefined) {
      if (typeof rem === "bigint") {
        rem = unsignedBigIntToBuffer(rem, "options.rem");
      } else if (!isAnyArrayBuffer(rem) && !isArrayBufferView(rem)) {
        throw ERR_INVALID_ARG_TYPE(
          "options.rem",
          ["ArrayBuffer", "TypedArray", "Buffer", "DataView", "bigint"],
          rem,
        );
      }
    }
    return { safe, bigint, add, rem };
  }
  function generatePrimeSync(size, options = {}) {
    validateInt32(size, "size", 1);
    const { safe, bigint, add, rem } = processGeneratePrimeOptions(options);
    const primeBuf = randomPrime(size, safe, add, rem);
    return bigint ? bufToUnsignedBigInt(primeBuf) : primeBuf;
  }
  function generatePrime(size, options, callback) {
    validateInt32(size, "size", 1);
    if (typeof options === "function") {
      callback = options;
      options = {};
    }
    validateFunction(callback, "callback");
    const { safe, bigint, add, rem } = processGeneratePrimeOptions(options);
    new Promise((resolve, reject) => {
      try {
        const primeBuf = randomPrime(size, safe, add, rem);
        resolve(bigint ? bufToUnsignedBigInt(primeBuf) : primeBuf);
      } catch (err) { reject(err); }
    }).then(
      (val) => callback(null, val),
      (err) => callback(err),
    );
  }
  function validateCandidate(candidate) {
    if (typeof candidate === "bigint")
      candidate = unsignedBigIntToBuffer(candidate, "candidate");
    if (!isAnyArrayBuffer(candidate) && !isArrayBufferView(candidate)) {
      throw ERR_INVALID_ARG_TYPE(
        "candidate",
        ["ArrayBuffer", "TypedArray", "Buffer", "DataView", "bigint"],
        candidate,
      );
    }
    return candidate;
  }
  function validateChecks(options) {
    const { checks = 0 } = options;
    validateInt32(checks, "options.checks", 0);
    return checks;
  }
  function checkPrimeImpl(candidate, checks) {
    if (checks > 64) throw new RangeError("Invalid number of checks");
    const u8 = asU8(candidate);
    if (u8.byteLength > kMaxPrimeBits / 8)
      throw new RangeError("checkPrime candidate exceeds maximum size");
    return isPrimeBig(bigFromBytes(u8), checks);
  }
  function checkPrimeSync(candidate, options = {}) {
    candidate = validateCandidate(candidate);
    validateObject(options, "options");
    return checkPrimeImpl(candidate, validateChecks(options));
  }
  function checkPrime(candidate, options, callback) {
    candidate = validateCandidate(candidate);
    if (typeof options === "function") {
      callback = options;
      options = {};
    }
    validateObject(options, "options");
    validateFunction(callback, "callback");
    const checks = validateChecks(options);
    new Promise((resolve, reject) => {
      try { resolve(checkPrimeImpl(candidate, checks)); }
      catch (err) { reject(err); }
    }).then(
      (val) => callback(null, val),
      (err) => callback(err),
    );
  }

  // ---- misc ----------------------------------------------------------------
  function timingSafeEqual(a, b) {
    if (!isArrayBufferView(a)) {
      throw ERR_INVALID_ARG_TYPE(
        "buf1", ["Buffer", "TypedArray", "DataView"], a);
    }
    if (!isArrayBufferView(b)) {
      throw ERR_INVALID_ARG_TYPE(
        "buf2", ["Buffer", "TypedArray", "DataView"], b);
    }
    const ua = asU8(a), ub = asU8(b);
    if (ua.byteLength !== ub.byteLength)
      throw ERR_CRYPTO_TIMING_SAFE_EQUAL_LENGTH();
    return $$timingSafeEqual(ua, ub);
  }
  // The digests Cells actually implements (Node returns its real list too);
  // hkdf/pbkdf2/hash/hmac accept exactly these.
  const getHashes = () =>
    ["md5", "sha1", "sha224", "sha256", "sha384", "sha512"];
  const getCurves = () =>
    ["secp224r1", "prime256v1", "secp384r1", "secp521r1"];
  const secureHeapUsed = () =>
    ({ total: 0, used: 0, utilization: 0, min: 0 });
  const setEngine = () => {
    throw ERR_METHOD_NOT_IMPLEMENTED("setEngine");
  };
  const setFips = () => {
    throw ERR_METHOD_NOT_IMPLEMENTED("setFips");
  };
  const getFips = () => true;
  const notImplemented = (name) =>
    function () {
      throw ERR_METHOD_NOT_IMPLEMENTED(name);
    };

  const constants = {
    __proto__: null,
    DH_CHECK_P_NOT_SAFE_PRIME: 2,
    DH_CHECK_P_NOT_PRIME: 1,
    DH_UNABLE_TO_CHECK_GENERATOR: 4,
    DH_NOT_SUITABLE_GENERATOR: 8,
    RSA_PKCS1_PADDING: 1,
    RSA_NO_PADDING: 3,
    RSA_PKCS1_OAEP_PADDING: 4,
    RSA_X931_PADDING: 5,
    RSA_PKCS1_PSS_PADDING: 6,
    RSA_PSS_SALTLEN_DIGEST: -1,
    RSA_PSS_SALTLEN_MAX_SIGN: -2,
    RSA_PSS_SALTLEN_AUTO: -2,
    POINT_CONVERSION_COMPRESSED: 2,
    POINT_CONVERSION_UNCOMPRESSED: 4,
    POINT_CONVERSION_HYBRID: 6,
    OPENSSL_VERSION_NUMBER: 0,
    defaultCoreCipherList: "",
    defaultCipherList: "",
  };

  const webcrypto = globalThis.crypto;
  const cryptoModule = {
    // Random
    getRandomValues: webcrypto.getRandomValues.bind(webcrypto),
    pseudoRandomBytes: randomBytes,
    randomBytes, randomFillSync, randomFill, randomInt, randomUUID,
    generatePrime, generatePrimeSync, checkPrime, checkPrimeSync,
    // Hash and Hmac
    Hash, Hmac, createHash, createHmac, getHashes, hash,
    // KDF
    hkdf, hkdfSync, pbkdf2, pbkdf2Sync,
    // Keys
    KeyObject, SecretKeyObject, PublicKeyObject, PrivateKeyObject,
    createSecretKey, createPrivateKey, createPublicKey,
    generateKey, generateKeySync, generateKeyPair, generateKeyPairSync,
    // Sign/Verify, ciphers, DH, certs: not implemented — loud, not silent.
    createSign: notImplemented("createSign"),
    createVerify: notImplemented("createVerify"),
    sign, verify,
    createCipheriv: notImplemented("createCipheriv"),
    createDecipheriv: notImplemented("createDecipheriv"),
    publicEncrypt: notImplemented("publicEncrypt"),
    publicDecrypt: notImplemented("publicDecrypt"),
    privateEncrypt: notImplemented("privateEncrypt"),
    privateDecrypt: notImplemented("privateDecrypt"),
    scrypt: notImplemented("scrypt"),
    scryptSync: notImplemented("scryptSync"),
    createDiffieHellman: notImplemented("createDiffieHellman"),
    createDiffieHellmanGroup: notImplemented("createDiffieHellmanGroup"),
    getDiffieHellman: notImplemented("getDiffieHellman"),
    diffieHellman: notImplemented("diffieHellman"),
    createECDH: notImplemented("createECDH"),
    X509Certificate: notImplemented("X509Certificate"),
    Certificate: notImplemented("Certificate"),
    // Misc
    getCiphers: () => [],
    getCurves, secureHeapUsed, setEngine, timingSafeEqual,
    getFips, setFips,
    get fips() { return getFips(); },
    set fips(_) { setFips(_); },
    constants,
    // WebCrypto
    subtle: webcrypto.subtle,
    webcrypto,
    CryptoKey: globalThis.CryptoKey,
  };
  cryptoModule.default = cryptoModule;

  Object.defineProperty(globalThis, "__cryptoModule", {
    value: cryptoModule, configurable: true, writable: true,
  });
  // node:util's types.isKeyObject checks against this class if it exists.
  Object.defineProperty(globalThis, "__nodeKeyObjectClass", {
    value: KeyObject, configurable: true, writable: true,
  });
})();
