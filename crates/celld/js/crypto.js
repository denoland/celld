// Web Crypto implementation for the embedded runtime.
//
// The common WebCrypto paths use typed-array host ops.
// Cells keeps a second, JSON-shaped op for Workers-compatible algorithms that
// require structured arguments (RSA-OAEP, ECDSA, and Ed25519).
(function () {
  const _randomValues = $$randomValues;
  const _digest = $$digest;
  const _hmacSign = $$hmacSign;
  const _hmacVerify = $$hmacVerify;
  const _aesEncrypt = $$aesEncrypt;
  const _aesDecrypt = $$aesDecrypt;

  // MD5 is not in the Web Crypto spec; Cloudflare accepts it for `digest`
  // and DigestStream, and the host op has always implemented it.
  const _CRC_ALGS = new Set(["CRC32", "CRC32C", "CRC64NVME"]);
  const _DIGEST_ALGS = new Set([
    "SHA-1", "SHA-256", "SHA-384", "SHA-512", "MD5",
  ]);
  const _SECRET_KEY_ALGS = new Set(
    ["HMAC", "AES-GCM", "AES-CBC", "AES-CTR"],
  );
  // Cloudflare accepts its own pre-standard curve spellings beside the
  // standard ones.
  const _curveName = (curve) =>
    curve === "NODE-ED25519" ? "Ed25519" : String(curve ?? "");
  // The curves celld carries, under every spelling Web Crypto and Node use.
  const _EC_CURVES = {
    "P-256": "P-256", "prime256v1": "P-256", "secp256r1": "P-256",
    "P-384": "P-384", "secp384r1": "P-384",
    "P-521": "P-521", "secp521r1": "P-521",
  };
  // The asymmetric algorithms, as the uppercased name `_algorithmName`
  // produces mapped to the spelling `key.algorithm.name` reports. The
  // uppercased form is for lookup only: echoing it back names an algorithm
  // no library recognizes, because `RSASSA-PKCS1-V1_5` is not the Web
  // Crypto spelling of `RSASSA-PKCS1-v1_5`.
  const _ASYM_NAMES = {
    "RSASSA-PKCS1-V1_5": "RSASSA-PKCS1-v1_5",
    "RSA-OAEP": "RSA-OAEP",
    "RSA-PSS": "RSA-PSS",
    "ECDSA": "ECDSA",
    "ECDH": "ECDH",
    "ED25519": "Ed25519",
    "X25519": "X25519",
  };
  // Algorithms whose keys are asymmetric, whatever celld can then *do* with
  // them: import validates the key, and an unsupported operation throws
  // later at sign/verify/encrypt rather than here. Derived from the table
  // above, not written twice: an algorithm that imports but has no reported
  // spelling would report the uppercased lookup key.
  const _ASYM_ALGS = new Set(Object.keys(_ASYM_NAMES));

  function _toBuf(data) {
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data)) {
      return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    throw new TypeError("expected an ArrayBuffer or ArrayBufferView");
  }

  function _algorithmName(algorithm) {
    return String(
      typeof algorithm === "string" ? algorithm : algorithm?.name || "",
    ).toUpperCase();
  }

  function _hashName(hash) {
    return _algorithmName(
      typeof hash === "string" ? hash : hash?.name || "SHA-256",
    );
  }

  function _notSupported(message) {
    return new DOMException(message, "NotSupportedError");
  }

  function _operationError(message) {
    return new DOMException(message, "OperationError");
  }

  function _rsaOaepLabel(algorithm) {
    const label = _toBuf(algorithm?.label ?? new Uint8Array());
    try {
      new TextDecoder("utf-8", { fatal: true }).decode(label);
    } catch {
      throw _notSupported("RSA-OAEP labels must contain UTF-8 bytes");
    }
    return Array.from(label);
  }

  class CryptoKey {
    constructor(type, algorithm, extractable, usages, material) {
      Object.defineProperties(this, {
        type: { value: type, enumerable: true },
        algorithm: { value: algorithm, enumerable: true },
        extractable: { value: Boolean(extractable), enumerable: true },
        usages: { value: Object.freeze(Array.from(usages || [])), enumerable: true },
        __celldMaterial: { value: material },
      });
    }
    get [Symbol.toStringTag]() { return "CryptoKey"; }
  }

  function _makeKey(type, algorithm, extractable, usages, material) {
    return new CryptoKey(type, algorithm, extractable, usages, material);
  }

  // Web Crypto reports a key's algorithm as its *KeyAlgorithm dictionary,
  // built from the parsed key, not as the request the caller passed. An
  // RSA key carries RsaHashedKeyAlgorithm: the modulus length, the public
  // exponent as bytes, and the hash as `{ name }`. An EC key carries its
  // curve. A JOSE library reads `algorithm.hash.name` and `modulusLength`
  // to validate an RS256 key before it ever calls verify(), so a key that
  // echoes the request's hash string fails with a TypeError. The host
  // parses every asymmetric key, so its details are the source here.
  //
  // `algorithm` is the caller's request, or null when there is none — a
  // node:crypto KeyObject crossing to Web Crypto has only the parsed key.
  // RSA is the one algorithm whose hash the key itself does not carry, so
  // the request is the only source for it and a missing one means SHA-256.
  function _keyAlgorithm(name, algorithm, keyType, details) {
    const reported = { name: _ASYM_NAMES[_algorithmName(name)] ?? name };
    if (keyType === "rsa") {
      // The exponent is reported as a Uint8Array, which is what
      // `crypto_preserve_public_exponent` fixed upstream -- an ArrayBuffer
      // here is the bug that flag names. The host sends a decimal string,
      // because JSON cannot carry the BigInt the exponent can reach.
      const exponent = [];
      for (let n = BigInt(details.publicExponent); n; n >>= 8n) {
        exponent.unshift(Number(n & 255n));
      }
      reported.modulusLength = details.modulusLength;
      reported.publicExponent = Uint8Array.from(exponent);
      reported.hash = { name: _hashName(algorithm?.hash) };
    } else if (keyType === "ec") {
      // The host names the curve as node:crypto does; Web Crypto spells it
      // as the caller did, so map it back.
      reported.namedCurve =
        _EC_CURVES[details.namedCurve] ?? details.namedCurve;
    } else if (algorithm?.namedCurve !== undefined) {
      // Ed25519 and X25519 have one curve each, so the parsed key carries
      // no curve to report. Cloudflare still puts `namedCurve` on such a
      // key, and a caller that asked with `NODE-ED25519` matches on the
      // spelling it used, so the request's curve passes through.
      reported.namedCurve = algorithm.namedCurve;
    }
    return reported;
  }

  function _extra(operation, input) {
    return JSON.parse(__crypto_operation(operation, JSON.stringify(input)));
  }

  // The AES modes the host ops accept. `encrypt` and `decrypt` both test
  // against this one set, so neither can grow a mode the other refuses.
  const _AES_MODES = new Set(["AES-GCM", "AES-CBC", "AES-CTR"]);
  const _AES_GCM_TAG_LENGTHS = new Set([96, 104, 112, 120, 128]);
  const _RSA_OAEP_HASHES = new Set([
    "SHA-1", "SHA-256", "SHA-384", "SHA-512",
  ]);

  // All three modes take the same typed-array host ops: the key, the IV or
  // counter block, and the data are all bytes, so nothing here crosses as
  // JSON. CBC is PKCS#7-padded and CTR is its own inverse, so `encrypting`
  // only matters for CBC.
  //
  // AES-CTR names its IV `counter`, and that block belongs to the caller.
  // The host copies every view it is given, so nothing on this path can
  // increment the caller's block in place.
  //
  // AES-GCM reports a failure by returning nothing, and this turns that into
  // the `OperationError` the Web Crypto specification names. The block modes
  // throw from the host with the cause, so they never reach that branch.
  function _aes(name, algorithm, key, data, encrypting) {
    if (name === "AES-GCM" && _toBuf(algorithm.iv).byteLength === 0) {
      throw new DOMException("AES-GCM IV must not be empty.", "OperationError");
    }
    const tagLength = name === "AES-GCM"
      ? Number(algorithm.tagLength ?? 128)
      : 128;
    if (name === "AES-GCM" && !_AES_GCM_TAG_LENGTHS.has(tagLength)) {
      throw _operationError("unsupported AES-GCM tagLength: " + tagLength);
    }
    const run = encrypting ? _aesEncrypt : _aesDecrypt;
    const out = run(
      name,
      key.__celldMaterial.bytes,
      _toBuf(name === "AES-CTR" ? algorithm.counter : algorithm.iv),
      _toBuf(data),
      name === "AES-GCM"
        ? _toBuf(algorithm.additionalData ?? new Uint8Array())
        : new Uint8Array(),
      tagLength / 8,
    );
    if (!out) {
      throw _operationError(
        name + (encrypting ? " encrypt failed" : " decrypt failed"),
      );
    }
    return out.buffer.slice(out.byteOffset, out.byteOffset + out.byteLength);
  }

  class SubtleCrypto {
    get [Symbol.toStringTag]() { return "SubtleCrypto"; }

    async digest(algorithm, data) {
      const name = _algorithmName(algorithm);
      if (!_DIGEST_ALGS.has(name)) {
        throw _notSupported("unsupported digest algorithm: " + name);
      }
      const out = _digest(name, _toBuf(data));
      return out.buffer.slice(out.byteOffset, out.byteOffset + out.byteLength);
    }

    async importKey(format, keyData, algorithm, extractable, usages) {
      const name = _algorithmName(algorithm);
      if (format === "raw" && (name === "HKDF" || name === "PBKDF2")) {
        // The spec makes KDF secrets non-extractable, so the password
        // material cannot come back out through exportKey.
        if (extractable) {
          throw new DOMException(
            name + " keys must not be extractable", "SyntaxError");
        }
        return _makeKey(
          "secret", { name }, false, usages,
          { bytes: _toBuf(keyData).slice() },
        );
      }
      if (format === "raw" && _SECRET_KEY_ALGS.has(name)) {
        const raw = _toBuf(keyData).slice();
        const normalized = name === "HMAC"
          ? { name, hash: { name: _hashName(algorithm?.hash) }, length: raw.byteLength * 8 }
          : { name, length: raw.byteLength * 8 };
        return _makeKey(
          "secret", normalized, extractable, usages, { bytes: raw },
        );
      }
      // A JWK's `alg` names the algorithm it was made for. Anything that is
      // not a string is not an algorithm name, and importing it would leave
      // a key claiming to be something it cannot be. Web Crypto ignores a
      // mismatched `alg` on EC keys, so only the type is checked here.
      if (format === "jwk" && keyData?.alg !== undefined &&
          typeof keyData.alg !== "string") {
        throw new DOMException(
          `Unrecognized or unimplemented algorithm "${String(keyData.alg)}"`,
          "NotSupportedError",
        );
      }
      // Asymmetric keys go through the same host import node:crypto uses, so
      // the CryptoKey carries the key type and details beside its bytes.
      // KeyObject.from() then sees a real asymmetric key rather than opaque
      // material, and the key is validated at import instead of at first use.
      if (
        (format === "spki" || format === "pkcs8" ||
          format === "jwk") &&
        _ASYM_ALGS.has(name)
      ) {
        const jwk = format === "jwk";
        const visibility =
          format === "pkcs8" || (jwk && keyData?.d !== undefined)
            ? "private"
            : "public";
        const imported = _extra("asym-key-import", {
          key: jwk ? keyData : Array.from(_toBuf(keyData)),
          format: jwk ? "jwk" : "der",
          type: jwk ? null : format,
          visibility,
          passphrase: null,
        });
        return _makeKey(
          visibility,
          _keyAlgorithm(name, algorithm, imported.keyType, imported.details),
          extractable,
          usages,
          {
            bytes: Uint8Array.from(imported.der),
            keyType: imported.keyType,
            details: imported.details,
          },
        );
      }
      throw _notSupported("unsupported key import");
    }

    async exportKey(format, key) {
      if (format === "raw" && key?.__celldMaterial?.bytes) {
        if (!key.extractable) {
          throw new DOMException("key is not extractable", "InvalidAccessError");
        }
        const raw = key.__celldMaterial.bytes;
        return raw.buffer.slice(raw.byteOffset, raw.byteOffset + raw.byteLength);
      }
      if (format === "jwk" && key?.__celldMaterial?.jwk) {
        if (!key.extractable) {
          throw new DOMException("key is not extractable", "InvalidAccessError");
        }
        return structuredClone(key.__celldMaterial.jwk);
      }
      // Asymmetric keys export from their normalized DER: spki and pkcs8 as
      // they are stored, jwk through the host.
      const material = key?.__celldMaterial;
      if (material?.keyType !== undefined) {
        if (!key.extractable) {
          throw new DOMException("key is not extractable", "InvalidAccessError");
        }
        const visibility = key.type;
        if (format === "jwk") {
          return _extra("asym-key-export", {
            der: Array.from(material.bytes),
            visibility,
          }).jwk;
        }
        if ((format === "spki" && visibility === "public") ||
            (format === "pkcs8" && visibility === "private")) {
          const der = material.bytes;
          return der.buffer.slice(der.byteOffset, der.byteOffset + der.byteLength);
        }
      }
      throw _notSupported("unsupported key export");
    }

    // Cloudflare's extension, not Web Crypto: a constant-time compare for
    // signatures and MACs, where `===` on a decoded string leaks by timing.
    // Equal lengths are required, so this cannot be used to probe length.
    timingSafeEqual(a, b) {
      const left = _toBuf(a), right = _toBuf(b);
      if (left.byteLength !== right.byteLength) {
        throw new TypeError(
          "Input buffers must have the same byte length");
      }
      return $$timingSafeEqual(left, right);
    }

    // ECDH. `length` may be null or undefined, a recent spec change: the
    // shared secret is the curve's field size, so there is a right answer
    // without being told one. A shorter length truncates, as the spec says.
    // HKDF and PBKDF2 have no natural output size, so for them a missing
    // or unaligned length is the OperationError the spec names.
    async deriveBits(algorithm, baseKey, length) {
      const name = _algorithmName(algorithm);
      if (name === "HKDF" || name === "PBKDF2") {
        const bits = Number(length);
        if (!Number.isInteger(bits) || bits <= 0 || bits % 8 !== 0) {
          throw _operationError(
            name + " length must be a positive multiple of 8 bits");
        }
        const hash = _hashName(algorithm?.hash);
        if (!_DIGEST_ALGS.has(hash)) {
          throw _notSupported("unsupported " + name + " hash: " + hash);
        }
        const ikm = baseKey.__celldMaterial.bytes;
        const salt = _toBuf(algorithm?.salt ?? new Uint8Array());
        let out;
        if (name === "HKDF") {
          out = $$hkdf(
            hash, ikm, salt,
            _toBuf(algorithm?.info ?? new Uint8Array()), bits / 8);
          if (!out) {
            throw _operationError("HKDF length is too large for the hash");
          }
        } else {
          const iterations = Number(algorithm?.iterations);
          if (!Number.isInteger(iterations) || iterations <= 0) {
            throw _operationError(
              "PBKDF2 iterations must be a positive integer");
          }
          out = $$pbkdf2(hash, ikm, salt, iterations, bits / 8);
        }
        return out.buffer.slice(
          out.byteOffset, out.byteOffset + out.byteLength);
      }
      if (name !== "ECDH") {
        throw _notSupported("unsupported derive algorithm: " + name);
      }
      const publicKey = algorithm?.public;
      if (!publicKey || publicKey.type !== "public") {
        throw new TypeError("ECDH requires a public key in algorithm.public");
      }
      const shared = Uint8Array.from(_extra("ecdh-derive", {
        private: Array.from(_toBuf(baseKey.__celldMaterial.bytes)),
        public: Array.from(_toBuf(publicKey.__celldMaterial.bytes)),
      }).bytes);
      if (length === null || length === undefined) return shared.buffer;
      const bytes = Number(length) / 8;
      if (!Number.isInteger(bytes) || bytes < 0 || bytes > shared.byteLength) {
        throw _operationError("requested length exceeds the derived secret");
      }
      return shared.slice(0, bytes).buffer;
    }

    async deriveKey(algorithm, baseKey, derived, extractable, usages) {
      const name = _algorithmName(derived);
      // An HMAC key with no stated length defaults to its hash's block
      // size, which is what the spec's getKeyLength returns.
      const length = derived?.length ??
        (name === "AES-GCM" || name === "AES-CBC" || name === "AES-CTR"
          ? 256
          : name === "HMAC"
            ? ({ "SHA-384": 1024, "SHA-512": 1024 }[
              _hashName(derived?.hash)] ?? 512)
            : null);
      const bits = await this.deriveBits(algorithm, baseKey, length);
      return this.importKey(
        "raw", bits, derived, extractable, usages);
    }

    async generateKey(algorithm, extractable, usages) {
      const name = _algorithmName(algorithm);
      if (_SECRET_KEY_ALGS.has(name)) {
        let byteLength;
        if (name === "HMAC") {
          const defaults = {
            "SHA-1": 20,
            "SHA-256": 32,
            "SHA-384": 48,
            "SHA-512": 64,
          };
          const hash = _hashName(algorithm?.hash);
          byteLength = algorithm?.length
            ? Number(algorithm.length) / 8
            : defaults[hash];
          if (!byteLength) throw _notSupported("unsupported HMAC hash: " + hash);
          const raw = new Uint8Array(byteLength);
          crypto.getRandomValues(raw);
          return _makeKey(
            "secret",
            { name, hash: { name: hash }, length: raw.byteLength * 8 },
            extractable,
            usages,
            { bytes: raw },
          );
        }
        byteLength = Number(algorithm?.length || 256) / 8;
        if (byteLength !== 16 && byteLength !== 32) {
          throw new DOMException("AES-GCM length must be 128 or 256", "OperationError");
        }
        const raw = new Uint8Array(byteLength);
        crypto.getRandomValues(raw);
        return _makeKey(
          "secret",
          { name, length: raw.byteLength * 8 },
          extractable,
          usages,
          { bytes: raw },
        );
      }
      // Asymmetric signing keys. `NODE-ED25519` is Cloudflare's pre-standard
      // spelling of Ed25519 and stays as the reported algorithm name, because
      // the caller matched on what it asked for.
      const ASYM_GENERATE = {
        "RSASSA-PKCS1-V1_5": "rsa",
        "RSA-OAEP": "rsa",
        "RSA-PSS": "rsa",
        "ED25519": "ed25519",
        "NODE-ED25519": "ed25519",
        "ECDSA": "ec",
        "ECDH": "ec",
      };
      const kind = ASYM_GENERATE[name];
      if (kind !== undefined) {
        const options = { type: kind };
        if (kind === "rsa") {
          // Only 3 and 65537 are legal exponents, and celld generates with
          // 65537. Rejecting the rest here is what stops a pathological
          // exponent reaching the prime search at all.
          const raw = algorithm?.publicExponent;
          const bytes = raw ? Array.from(_toBuf(raw)) : [1, 0, 1];
          let exponent = 0;
          for (const byte of bytes) exponent = exponent * 256 + byte;
          if (exponent !== 3 && exponent !== 65537) {
            throw new DOMException(
              `The "publicExponent" must be either 3 or 65537, but got ` +
                `${exponent}.`,
              "OperationError",
            );
          }
          if (exponent !== 65537) {
            throw _notSupported("publicExponent 3 is not implemented");
          }
          options.modulusLength = Number(algorithm?.modulusLength ?? 2048);
          if (
            !Number.isInteger(options.modulusLength) ||
            options.modulusLength < 1024 ||
            options.modulusLength > 16384 ||
            options.modulusLength % 8 !== 0
          ) {
            throw _operationError(
              "RSA modulusLength must be a multiple of 8 from 1024 to 16384",
            );
          }
          const hash = _hashName(algorithm?.hash);
          if (name === "RSA-OAEP" && !_RSA_OAEP_HASHES.has(hash)) {
            throw _notSupported("unsupported RSA-OAEP hash: " + hash);
          }
        }
        if (kind === "ec") {
          const curve = _EC_CURVES[_curveName(algorithm?.namedCurve)];
          if (curve === undefined)
            throw _notSupported("unsupported curve: " + algorithm?.namedCurve);
          options.namedCurve = curve;
        }
        const pair = _extra("asym-key-generate", options);
        // A dictionary per half, not one shared between them. The reported
        // hash is what sign() then hashes with, so a caller that mutates
        // `publicKey.algorithm.hash.name` on a shared object would change
        // what the private key signs.
        const half = (der, type, allowed) =>
          _makeKey(type,
            _keyAlgorithm(name, algorithm, pair.keyType, pair.details),
            type === "public" ? true : extractable,
            (usages || []).filter((usage) => allowed.includes(usage)), {
              bytes: Uint8Array.from(der),
              keyType: pair.keyType,
              details: pair.details,
            });
        return {
          publicKey: half(pair.publicDer, "public", ["verify", "encrypt"]),
          privateKey: half(pair.privateDer, "private",
            ["sign", "decrypt", "deriveKey", "deriveBits"]),
        };
      }
      throw _notSupported("unsupported key algorithm: " + name);
    }

    async sign(algorithm, key, data) {
      const name = _algorithmName(algorithm || key?.algorithm);
      const bytes = _toBuf(data);
      if (name === "HMAC") {
        const hash = _hashName(key?.algorithm?.hash);
        const sig = _hmacSign(hash, key.__celldMaterial.bytes, bytes);
        if (!sig) throw _operationError("HMAC sign failed");
        return sig.buffer.slice(sig.byteOffset, sig.byteOffset + sig.byteLength);
      }
      const operation = name === "ED25519"
        ? "ed25519-sign"
        : name === "ECDSA"
          ? "p256-sign"
          : name === "RSASSA-PKCS1-V1_5"
            ? "rsa-pkcs1-sign"
            : name === "RSA-PSS"
              ? "rsa-pss-sign"
              : null;
      if (!operation) throw _notSupported("unsupported sign algorithm: " + name);
      if (name === "ECDSA") {
        if (_EC_CURVES[_curveName(key?.algorithm?.namedCurve)] !== "P-256") {
          throw _notSupported("ECDSA signatures support only P-256");
        }
        if (_hashName(algorithm?.hash) !== "SHA-256") {
          throw _notSupported("ECDSA P-256 supports only SHA-256");
        }
      }
      const result = _extra(operation, {
        key: Array.from(key?.__celldMaterial?.bytes || []),
        data: Array.from(bytes),
        // RSA carries its hash on the key; the others ignore the field.
        hash: _hashName(key?.algorithm?.hash),
        saltLength: Number(algorithm?.saltLength ?? 0),
      });
      return Uint8Array.from(result.bytes).buffer;
    }

    async verify(algorithm, key, signature, data) {
      const name = _algorithmName(algorithm || key?.algorithm);
      if (name === "HMAC") {
        return _hmacVerify(
          _hashName(key?.algorithm?.hash),
          key.__celldMaterial.bytes,
          _toBuf(signature),
          _toBuf(data),
        );
      }
      const operation = name === "ECDSA"
        ? "p256-verify"
        : name === "RSASSA-PKCS1-V1_5"
        ? "rsa-pkcs1-verify"
        : name === "RSA-PSS"
        ? "rsa-pss-verify"
        : null;
      if (!operation) {
        throw _notSupported("unsupported verify algorithm: " + name);
      }
      const material = key?.__celldMaterial?.bytes;
      if (!material) throw _notSupported("verify needs an spki public key");
      if (name === "ECDSA") {
        if (_EC_CURVES[_curveName(key?.algorithm?.namedCurve)] !== "P-256") {
          throw _notSupported("ECDSA signatures support only P-256");
        }
        if (_hashName(algorithm?.hash) !== "SHA-256") {
          throw _notSupported("ECDSA P-256 supports only SHA-256");
        }
      }
      return _extra(operation, {
        key: Array.from(material),
        data: Array.from(_toBuf(data)),
        signature: Array.from(_toBuf(signature)),
        // ECDSA carries its hash on the call, RSASSA on the key.
        hash: _hashName(
          name === "ECDSA" ? algorithm?.hash : key?.algorithm?.hash,
        ),
      }).ok;
    }

    async encrypt(algorithm, key, data) {
      const name = _algorithmName(algorithm);
      if (_AES_MODES.has(name)) {
        return _aes(name, algorithm, key, data, true);
      }
      if (name === "RSA-OAEP") {
        const hash = _hashName(key?.algorithm?.hash);
        if (!_RSA_OAEP_HASHES.has(hash)) {
          throw _notSupported("unsupported RSA-OAEP hash: " + hash);
        }
        const result = _extra("rsa-oaep-encrypt", {
          key: Array.from(key?.__celldMaterial?.bytes || []),
          data: Array.from(_toBuf(data)),
          hash,
          label: _rsaOaepLabel(algorithm),
        });
        return Uint8Array.from(result.bytes).buffer;
      }
      throw _notSupported("unsupported encrypt algorithm: " + name);
    }

    // wrapKey and unwrapKey compose the primitives exactly the way the
    // spec defines them: export + encrypt, and decrypt + import. A
    // jwk-format key crosses as its JSON bytes.
    async wrapKey(format, key, wrappingKey, wrapAlgorithm) {
      const exported = await this.exportKey(format, key);
      const raw = format === "jwk"
        ? new TextEncoder().encode(JSON.stringify(exported))
        : exported;
      return this.encrypt(wrapAlgorithm, wrappingKey, raw);
    }

    async unwrapKey(
      format, wrapped, unwrappingKey, unwrapAlgorithm,
      algorithm, extractable, usages,
    ) {
      const raw = await this.decrypt(unwrapAlgorithm, unwrappingKey, wrapped);
      const keyData = format === "jwk"
        ? JSON.parse(new TextDecoder().decode(raw))
        : raw;
      return this.importKey(format, keyData, algorithm, extractable, usages);
    }

    async decrypt(algorithm, key, data) {
      const name = _algorithmName(algorithm);
      if (_AES_MODES.has(name)) {
        return _aes(name, algorithm, key, data, false);
      }
      if (name === "RSA-OAEP") {
        const hash = _hashName(key?.algorithm?.hash);
        if (!_RSA_OAEP_HASHES.has(hash)) {
          throw _notSupported("unsupported RSA-OAEP hash: " + hash);
        }
        const result = _extra("rsa-oaep-decrypt", {
          key: Array.from(key?.__celldMaterial?.bytes || []),
          data: Array.from(_toBuf(data)),
          hash,
          label: _rsaOaepLabel(algorithm),
        });
        return Uint8Array.from(result.bytes).buffer;
      }
      throw _notSupported("unsupported decrypt algorithm: " + name);
    }
  }

  const subtle = new SubtleCrypto();
  const crypto = {
    getRandomValues(array) {
      // Web IDL brand check, observable via node:crypto's webcrypto alias.
      if (this !== crypto) throw new TypeError("Illegal invocation");
      if (
        !(array instanceof Int8Array) &&
        !(array instanceof Uint8Array) &&
        !(array instanceof Uint8ClampedArray) &&
        !(array instanceof Int16Array) &&
        !(array instanceof Uint16Array) &&
        !(array instanceof Int32Array) &&
        !(array instanceof Uint32Array) &&
        !(array instanceof BigInt64Array) &&
        !(array instanceof BigUint64Array)
      ) {
        throw new DOMException(
          "Argument is not an integer-typed array",
          "TypeMismatchError",
        );
      }
      if (array.byteLength > 65536) {
        throw new DOMException(
          "getRandomValues byteLength must be at most 65536",
          "QuotaExceededError",
        );
      }
      _randomValues(array);
      return array;
    },

    randomUUID() {
      const bytes = new Uint8Array(16);
      _randomValues(bytes);
      bytes[6] = (bytes[6] & 0x0f) | 0x40;
      bytes[8] = (bytes[8] & 0x3f) | 0x80;
      const h = (index) => bytes[index].toString(16).padStart(2, "0");
      return h(0) + h(1) + h(2) + h(3) + "-" +
        h(4) + h(5) + "-" + h(6) + h(7) + "-" +
        h(8) + h(9) + "-" + h(10) + h(11) +
        h(12) + h(13) + h(14) + h(15);
    },

    subtle,
    get [Symbol.toStringTag]() { return "Crypto"; },
  };

  // Cloudflare's DigestStream: a WritableStream that hashes what is written
  // to it and resolves `digest` when the stream closes. Not Web Crypto, and
  // the reason a Worker can hash a body it never has to hold whole -- though
  // celld buffers here, as its Hash already does.
  class DigestStream extends WritableStream {
    constructor(algorithm) {
      const name = _algorithmName(algorithm);
      // DigestStream takes the CRC checksums too; `subtle.digest` does not.
      if (!_DIGEST_ALGS.has(name) && !_CRC_ALGS.has(name)) {
        throw _notSupported("unsupported digest algorithm: " + name);
      }
      const state = { chunks: [], written: 0, resolve: null, reject: null };
      const digest = new Promise((resolve, reject) => {
        state.resolve = resolve;
        state.reject = reject;
      });
      super({
        write(chunk) {
          // A string is written as its UTF-8 bytes, as workerd does; every
          // other chunk must already be binary.
          if (typeof chunk !== "string" && !ArrayBuffer.isView(chunk) &&
              !(chunk instanceof ArrayBuffer)) {
            throw new TypeError(
              "DigestStream is a byte stream but received an object of " +
              "non-ArrayBuffer/ArrayBufferView/string type on its " +
              "writable side.");
          }
          const bytes = typeof chunk === "string"
            ? new TextEncoder().encode(chunk)
            : _toBuf(chunk);
          state.chunks.push(bytes);
          state.written += bytes.byteLength;
        },
        close() {
          const joined = new Uint8Array(state.written);
          let offset = 0;
          for (const chunk of state.chunks) {
            joined.set(chunk, offset);
            offset += chunk.byteLength;
          }
          state.chunks.length = 0;
          const out = _digest(name, joined);
          state.resolve(
            out.buffer.slice(out.byteOffset, out.byteOffset + out.byteLength));
        },
        abort(reason) {
          state.chunks.length = 0;
          state.reject(reason);
        },
      });
      // A stream that is written to and never closed is legal, and so is one
      // that is disposed. Neither should raise an unhandled rejection just
      // because nobody awaited `digest`.
      digest.catch(() => {});
      Object.defineProperties(this, {
        digest: { value: digest, enumerable: true },
        bytesWritten: { get: () => BigInt(state.written), enumerable: true },
      });
      this.__celldDigestState = state;
    }
    get [Symbol.toStringTag]() { return "DigestStream"; }
    [Symbol.dispose]() {
      const state = this.__celldDigestState;
      if (state.disposed) return; // disposing twice is a no-op
      state.disposed = true;
      state.chunks.length = 0;
      const error = new Error("The DigestStream was disposed.");
      state.reject(error);
      // Error the stream itself, so a later write() rejects with the same
      // reason rather than succeeding into a digest nobody will resolve.
      this.abort(error).catch(() => {});
    }
  }

  crypto.DigestStream = DigestStream;
  globalThis.DigestStream = DigestStream;
  globalThis.CryptoKey = CryptoKey;
  globalThis.SubtleCrypto = SubtleCrypto;
  globalThis.crypto = crypto;
  // `node_crypto.js` hands a parsed key to Web Crypto through
  // `KeyObject.toCryptoKey()`, so it reports the same dictionary. It built
  // a second one by hand, which drifted: it named every EC curve P-256 and
  // left the exponent off an RSA key. The loop below hides this name, and
  // the module is lazy, so it always resolves after this script runs.
  globalThis.$$keyAlgorithm = _keyAlgorithm;
})();

// Last harness script, so this sees every internal the others declared.
// Runtime plumbing must not show up in `for (const k in globalThis)`: a
// bundle walking the globals should find the Web platform and nothing
// else. Host ops are already non-enumerable; these are the JS-side ones.
for (const n of Object.getOwnPropertyNames(globalThis))
  if (n.startsWith("__") || n.startsWith("$$"))
    // A top-level `function` declaration is non-configurable, so a
    // couple of harness helpers cannot be hidden. Harmless: a walker
    // sees a function either way.
    try { Object.defineProperty(globalThis, n, { enumerable: false }); }
    catch { /* non-configurable */ }
