// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Web Crypto and node:crypto: the key formats and the primitives.
//!
//! The JS layer in `crypto.js` and `node_crypto.js` validates the arguments,
//! so what arrives here is already well formed. This module holds the key
//! formats — SPKI, PKCS#8, JWK, raw — and the operations that use them. It
//! touches V8 only in its ops.
use super::*;

fn crypto_bytes(value: &serde_json::Value, name: &str) -> Result<Vec<u8>> {
    serde_json::from_value(value.get(name).cloned().unwrap_or_default())
        .map_err(|_| anyhow!("invalid {name} bytes"))
}

fn rsa_jwk_uint(value: &serde_json::Value, name: &str) -> Result<rsa::BigUint> {
    use base64::Engine;
    let encoded = value
        .get(name)
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("RSA JWK missing {name} parameter"))?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| anyhow!("invalid RSA JWK {name}"))?;
    Ok(rsa::BigUint::from_bytes_be(&bytes))
}

fn rsa_jwk_encode(value: &rsa::BigUint) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_bytes_be())
}

fn rsa_oaep_padding(hash: &str, label: Vec<u8>) -> Result<rsa::Oaep> {
    let label = if label.is_empty() {
        None
    } else {
        Some(
            String::from_utf8(label)
                .map_err(|_| anyhow!("RSA-OAEP labels must contain UTF-8 bytes"))?,
        )
    };
    macro_rules! padding {
        ($digest:ty) => {
            match label {
                Some(label) => rsa::Oaep::new_with_label::<$digest, _>(label),
                None => rsa::Oaep::new::<$digest>(),
            }
        };
    }
    Ok(match hash {
        "SHA-1" => padding!(sha1::Sha1),
        "SHA-256" => padding!(sha2::Sha256),
        "SHA-384" => padding!(sha2::Sha384),
        "SHA-512" => padding!(sha2::Sha512),
        other => return Err(anyhow!("unsupported RSA-OAEP hash {other}")),
    })
}

fn digest_bytes(hash: &str, data: &[u8]) -> Result<Vec<u8>> {
    use sha2::Digest as _;
    Ok(match hash {
        "SHA-256" => sha2::Sha256::digest(data).to_vec(),
        "SHA-384" => sha2::Sha384::digest(data).to_vec(),
        "SHA-512" => sha2::Sha512::digest(data).to_vec(),
        other => return Err(anyhow!("unsupported ECDSA hash {other}")),
    })
}

/// Algorithm OIDs, so a parsed key can say what it is. Same constants as
/// Node's, via deno's `ext/node_crypto/keys.rs`.
const RSA_ENCRYPTION_OID: rsa::pkcs8::ObjectIdentifier =
    rsa::pkcs8::ObjectIdentifier::new_unwrap("1.2.840.113549.1.1.1");
const EC_PUBLIC_KEY_OID: rsa::pkcs8::ObjectIdentifier =
    rsa::pkcs8::ObjectIdentifier::new_unwrap("1.2.840.10045.2.1");
const SECP256R1_OID: rsa::pkcs8::ObjectIdentifier =
    rsa::pkcs8::ObjectIdentifier::new_unwrap("1.2.840.10045.3.1.7");
const SECP384R1_OID: rsa::pkcs8::ObjectIdentifier =
    rsa::pkcs8::ObjectIdentifier::new_unwrap("1.3.132.0.34");
const SECP521R1_OID: rsa::pkcs8::ObjectIdentifier =
    rsa::pkcs8::ObjectIdentifier::new_unwrap("1.3.132.0.35");

/// The NIST curves celld reads, as (OID, Node's `namedCurve`, JWK `crv`).
const EC_CURVES: &[(rsa::pkcs8::ObjectIdentifier, &str, &str)] = &[
    (SECP256R1_OID, "prime256v1", "P-256"),
    (SECP384R1_OID, "secp384r1", "P-384"),
    (SECP521R1_OID, "secp521r1", "P-521"),
];

/// Run `$body!(crate)` against whichever curve crate `$crv` names. The three
/// crates share an API, so this is the alternative to writing every EC arm
/// out three times.
macro_rules! ec_curve {
    ($crv:expr, $body:ident) => {
        match $crv {
            "P-256" => $body!(p256),
            "P-384" => $body!(p384),
            "P-521" => $body!(p521),
            other => return Err(anyhow!("unsupported EC curve {other}")),
        }
    };
}
const ED25519_OID: rsa::pkcs8::ObjectIdentifier =
    rsa::pkcs8::ObjectIdentifier::new_unwrap("1.3.101.112");
const X25519_OID: rsa::pkcs8::ObjectIdentifier =
    rsa::pkcs8::ObjectIdentifier::new_unwrap("1.3.101.110");
const DSA_OID: rsa::pkcs8::ObjectIdentifier =
    rsa::pkcs8::ObjectIdentifier::new_unwrap("1.2.840.10040.4.1");

/// A key normalized to one DER encoding: PKCS#8 for private, SPKI for public.
/// Every consumer — the sign and verify ops, `toCryptoKey`, export — then
/// reads one shape rather than five.
struct ParsedKey {
    kind: &'static str,
    der: Vec<u8>,
    details: serde_json::Value,
}

/// X25519 has no DER codec in `x25519-dalek`, and both structures are a
/// fixed shape for a 32-byte key, so they are written out directly. Verified
/// against the `x25519_private.pem` / `x25519_public.pem` fixtures.
fn x25519_pkcs8(scalar: &[u8; 32]) -> Vec<u8> {
    let mut der = vec![
        0x30, 0x2e, // SEQUENCE, 46 bytes
        0x02, 0x01, 0x00, // version 0
        0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, // AlgorithmIdentifier, X25519
        0x04, 0x22, 0x04, 0x20, // OCTET STRING(34) wrapping OCTET STRING(32)
    ];
    der.extend_from_slice(scalar);
    der
}

fn x25519_spki(point: &[u8; 32]) -> Vec<u8> {
    let mut der = vec![
        0x30, 0x2a, // SEQUENCE, 42 bytes
        0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, // AlgorithmIdentifier, X25519
        0x03, 0x21, 0x00, // BIT STRING(33), no unused bits
    ];
    der.extend_from_slice(point);
    der
}

/// The JWK `crv` for a parsed EC key, from the `namedCurve` its details
/// already carry.
fn ec_jwk_crv(parsed: &ParsedKey) -> Result<&'static str> {
    let named = parsed.details["namedCurve"]
        .as_str()
        .ok_or_else(|| anyhow!("EC key has no named curve"))?;
    EC_CURVES
        .iter()
        .find(|(_, node, _)| *node == named)
        .map(|(.., crv)| *crv)
        .ok_or_else(|| anyhow!("unsupported EC curve {named}"))
}

/// PEM armour: 64-character base64 lines between the labelled delimiters.
fn pem_wrap(label: &str, bytes: &[u8]) -> String {
    use base64::Engine;
    let body = base64::engine::general_purpose::STANDARD.encode(bytes);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).unwrap());
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

fn base64url(value: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value)
}

fn jwk_bytes(value: &serde_json::Value, kty: &str, name: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    let encoded = value
        .get(name)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("{kty} JWK missing {name} parameter"))?;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| anyhow!("invalid JWK {name}"))
}

/// Salt length OpenSSL uses for a PBES2 export, which is what Node inherits.
const PBES2_SALT_LEN: usize = 8;
/// PBKDF2 iteration count OpenSSL uses for a PBES2 export. Node inherits it,
/// so an encrypted key celld writes costs a reader the same as a Node one.
///
/// This is far weaker than the scrypt cost `pkcs5` reaches for on its own, and
/// deliberately so: an encrypted key is only as strong as the reader that has
/// to open it, and every Node and OpenSSL reader expects these parameters. Do
/// not raise it to harden the KDF -- that hardens celld's keys against celld
/// alone and desyncs the format from the one the ecosystem writes.
const PBES2_ITERATIONS: u32 = 2048;

/// PBES2 parameters for an encrypted PKCS#8 export.
///
/// `pkcs8::PrivateKeyInfo::encrypt` picks scrypt at its recommended cost
/// (2^15 rounds over 32 MiB) and AES-256-CBC, and it ignores the cipher the
/// caller asked for. Node derives with PBKDF2-HMAC-SHA256 over 2048
/// iterations and honours the cipher, so celld builds the parameters itself.
/// The scrypt default also made an export cost seconds, which is why the two
/// crypto suites were slow.
///
/// The cipher name arrives lower-cased from `node_crypto.js`, which is also
/// where an unwritable cipher is refused. A name that reaches the fallback
/// arm here is a bug on that side, not caller input.
fn pbes2_parameters<'a>(
    cipher: &str,
    salt: &'a [u8],
    iv: &'a [u8; 16],
) -> Result<pkcs8::pkcs5::pbes2::Parameters<'a>> {
    use pkcs8::pkcs5::pbes2::{EncryptionScheme, Parameters, Pbkdf2Params};

    let encryption = match cipher {
        "aes-128-cbc" => EncryptionScheme::Aes128Cbc { iv },
        "aes-192-cbc" => EncryptionScheme::Aes192Cbc { iv },
        "aes-256-cbc" => EncryptionScheme::Aes256Cbc { iv },
        other => return Err(anyhow!("unsupported cipher {other}")),
    };
    let kdf = Pbkdf2Params::hmac_with_sha256(PBES2_ITERATIONS, salt)
        .map_err(|_| anyhow!("invalid PBKDF2 parameters"))?;
    Ok(Parameters {
        kdf: kdf.into(),
        encryption,
    })
}

/// Identify an already-normalized PKCS#8 or SPKI blob and describe it. Node
/// reports `asymmetricKeyType` and `asymmetricKeyDetails` from exactly this.
fn describe_key(der: &[u8], private: bool) -> Result<ParsedKey> {
    use rsa::pkcs8::der::Decode;
    use rsa::traits::PublicKeyParts;

    let (algorithm, curve) = if private {
        let info = rsa::pkcs8::PrivateKeyInfo::from_der(der)
            .map_err(|_| anyhow!("invalid PKCS#8 private key"))?;
        (info.algorithm.oid, info.algorithm.parameters_oid().ok())
    } else {
        let info = rsa::pkcs8::SubjectPublicKeyInfoRef::from_der(der)
            .map_err(|_| anyhow!("invalid SPKI public key"))?;
        (info.algorithm.oid, info.algorithm.parameters_oid().ok())
    };

    match algorithm {
        RSA_ENCRYPTION_OID => {
            // Node reports the modulus length and exponent, so the key has to
            // be parsed rather than merely recognized.
            let (modulus, exponent) = if private {
                use rsa::pkcs8::DecodePrivateKey;
                let key = rsa::RsaPrivateKey::from_pkcs8_der(der)
                    .map_err(|_| anyhow!("invalid RSA private key"))?;
                (key.n().bits(), key.e().clone())
            } else {
                use rsa::pkcs8::DecodePublicKey;
                let key = rsa::RsaPublicKey::from_public_key_der(der)
                    .map_err(|_| anyhow!("invalid RSA public key"))?;
                (key.n().bits(), key.e().clone())
            };
            Ok(ParsedKey {
                kind: "rsa",
                der: der.to_vec(),
                details: serde_json::json!({
                    "modulusLength": modulus,
                    "publicExponent": exponent.to_string(),
                }),
            })
        }
        EC_PUBLIC_KEY_OID => {
            // Resolve the curve from the key, so every later operation uses
            // the matching implementation instead of assuming P-256.
            let curve = curve.ok_or_else(|| anyhow!("EC key has no named curve"))?;
            let named = EC_CURVES
                .iter()
                .find(|(oid, ..)| *oid == curve)
                .ok_or_else(|| anyhow!("unsupported EC curve {curve}"))?
                .1;
            Ok(ParsedKey {
                kind: "ec",
                der: der.to_vec(),
                details: serde_json::json!({ "namedCurve": named }),
            })
        }
        ED25519_OID => Ok(ParsedKey {
            kind: "ed25519",
            der: der.to_vec(),
            details: serde_json::json!({}),
        }),
        DSA_OID => {
            use dsa::pkcs8::DecodePrivateKey;
            use dsa::pkcs8::DecodePublicKey;
            use dsa::Components;
            let sizes = |components: &Components| (components.p().bits(), components.q().bits());
            let (modulus, divisor) = if private {
                let key = dsa::SigningKey::from_pkcs8_der(der)
                    .map_err(|_| anyhow!("invalid DSA private key"))?;
                sizes(key.verifying_key().components())
            } else {
                let key = dsa::VerifyingKey::from_public_key_der(der)
                    .map_err(|_| anyhow!("invalid DSA public key"))?;
                sizes(key.components())
            };
            Ok(ParsedKey {
                kind: "dsa",
                der: der.to_vec(),
                details: serde_json::json!({
                    "modulusLength": modulus,
                    "divisorLength": divisor,
                }),
            })
        }
        X25519_OID => Ok(ParsedKey {
            kind: "x25519",
            der: der.to_vec(),
            details: serde_json::json!({}),
        }),
        other => Err(anyhow!(
            "unsupported key algorithm {other}; celld implements RSA, EC P-256 and Ed25519"
        )),
    }
}

/// PEM label -> the DER structure inside it. Node keys the whole import off
/// this, because the label is the only self-description a PEM file carries.
fn pem_structure(label: &str) -> Result<&'static str> {
    Ok(match label {
        "PRIVATE KEY" => "pkcs8",
        "RSA PRIVATE KEY" => "pkcs1",
        "EC PRIVATE KEY" => "sec1",
        "DSA PRIVATE KEY" => "dsa-traditional",
        "PUBLIC KEY" => "spki",
        "RSA PUBLIC KEY" => "pkcs1-public",
        // Handled before this point, where the passphrase is in scope.
        "ENCRYPTED PRIVATE KEY" => "pkcs8",
        other => return Err(anyhow!("unsupported PEM label \"{other}\"")),
    })
}

/// Re-encode any accepted private structure as PKCS#8, and any public one as
/// SPKI.
fn normalize_key(der: &[u8], structure: &str) -> Result<(Vec<u8>, bool)> {
    use rsa::pkcs8::EncodePrivateKey;
    use rsa::pkcs8::EncodePublicKey;
    match structure {
        "pkcs8" => Ok((der.to_vec(), true)),
        "spki" => Ok((der.to_vec(), false)),
        "pkcs1" => {
            use rsa::pkcs1::DecodeRsaPrivateKey;
            let key = rsa::RsaPrivateKey::from_pkcs1_der(der)
                .map_err(|_| anyhow!("invalid PKCS#1 RSA private key"))?;
            Ok((key.to_pkcs8_der()?.as_bytes().to_vec(), true))
        }
        "pkcs1-public" => {
            use rsa::pkcs1::DecodeRsaPublicKey;
            let key = rsa::RsaPublicKey::from_pkcs1_der(der)
                .map_err(|_| anyhow!("invalid PKCS#1 RSA public key"))?;
            Ok((key.to_public_key_der()?.as_bytes().to_vec(), false))
        }
        // OpenSSL's traditional DSA key: a bare SEQUENCE of six INTEGERs
        // with no algorithm identifier, so it has to be read field by field
        // and rebuilt as PKCS#8. Same shape deno parses.
        "dsa-traditional" => {
            use dsa::pkcs8::der::asn1::UintRef;
            use dsa::pkcs8::der::{Decode, Reader, SliceReader};
            use dsa::pkcs8::EncodePrivateKey;
            let mut reader =
                SliceReader::new(der).map_err(|_| anyhow!("invalid DSA private key"))?;
            let key = reader
                .sequence(|seq| {
                    let _version = UintRef::decode(seq)?;
                    let mut field = || -> Result<rsa::BigUint, dsa::pkcs8::der::Error> {
                        Ok(rsa::BigUint::from_bytes_be(
                            UintRef::decode(seq)?.as_bytes(),
                        ))
                    };
                    let (p, q, g, y, x) = (field()?, field()?, field()?, field()?, field()?);
                    let bad = || dsa::pkcs8::der::Tag::Sequence.value_error();
                    let components =
                        dsa::Components::from_components(p, q, g).map_err(|_| bad())?;
                    let verifying =
                        dsa::VerifyingKey::from_components(components, y).map_err(|_| bad())?;
                    dsa::SigningKey::from_components(verifying, x).map_err(|_| bad())
                })
                .map_err(|_| anyhow!("invalid DSA private key"))?;
            Ok((key.to_pkcs8_der()?.as_bytes().to_vec(), true))
        }
        "sec1" => {
            // SEC1 names no algorithm, so P-256 is the only reading celld has.
            let key = p256::SecretKey::from_sec1_der(der)
                .map_err(|_| anyhow!("invalid SEC1 EC private key (celld implements P-256)"))?;
            Ok((
                p256::pkcs8::EncodePrivateKey::to_pkcs8_der(&key)?
                    .as_bytes()
                    .to_vec(),
                true,
            ))
        }
        other => Err(anyhow!("unsupported key structure \"{other}\"")),
    }
}

/// Node does not trust the caller's `type` for DER input — it sniffs, so the
/// same PKCS#1 blob imports whether the caller says `pkcs1`, `pkcs8` or
/// `sec1`. `crypto-keys-test.js` asserts exactly that ("All three of these
/// variations should work, despite the type being different"), so the stated
/// structure is only a hint about which decoder to try first.
fn normalize_key_sniffing(der: &[u8], stated: Option<&str>, want_private: bool) -> Result<Vec<u8>> {
    // `pkcs8` is exact; every other hint falls back. The suite pins both
    // halves: a PKCS#8 blob imports whether it is called pkcs1, pkcs8 or
    // sec1, but a PKCS#1 blob called `pkcs8` must fail -- "just like with
    // Node.js ... tho oddly sec1 is ok. Silly software."
    let candidates: &[&str] = match (want_private, stated) {
        (true, Some("pkcs8")) => &["pkcs8"],
        (true, _) => &["pkcs8", "pkcs1", "sec1"],
        (false, _) => &["spki", "pkcs1-public"],
    };
    let mut order: Vec<&str> = stated
        .into_iter()
        .filter(|s| candidates.contains(s))
        .collect();
    order.extend(candidates.iter().filter(|s| Some(**s) != stated));

    let mut last = None;
    for structure in order {
        // A structure that decodes but yields the wrong visibility is not a
        // match either, so validate all the way through identification.
        if let Ok((der, is_private)) = normalize_key(der, structure) {
            if is_private == want_private && describe_key(&der, is_private).is_ok() {
                return Ok(der);
            }
        }
        last = Some(structure);
    }
    let _ = last;
    // Node's wording, which the suite matches on exactly.
    Err(anyhow!("Failed to parse private key"))
}

/// A JWK becomes the same normalized DER, so JWK input costs nothing extra
/// downstream.
fn key_from_jwk(jwk: &serde_json::Value, want_private: bool) -> Result<Vec<u8>> {
    use rsa::pkcs8::EncodePrivateKey;
    use rsa::pkcs8::EncodePublicKey;
    let kty = jwk
        .get("kty")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let curve = jwk.get("crv").and_then(serde_json::Value::as_str);
    // A curve-bearing kty with no `crv` is a malformed JWK, not an
    // unsupported one, and Node distinguishes the two by message.
    if matches!(kty, "EC" | "OKP") && curve.is_none() {
        return Err(anyhow!("{kty} JWK missing crv parameter"));
    }
    match (kty, curve) {
        ("RSA", _) => {
            let n = rsa_jwk_uint(jwk, "n")?;
            let e = rsa_jwk_uint(jwk, "e")?;
            if want_private {
                // The CRT primes are required: RSA private operations need
                // them, and a JWK carrying only `d` cannot be completed here.
                let d = rsa_jwk_uint(jwk, "d")?;
                let p = rsa_jwk_uint(jwk, "p")?;
                let q = rsa_jwk_uint(jwk, "q")?;
                let mut key = rsa::RsaPrivateKey::from_components(n, e, d, vec![p, q])?;
                key.precompute()?;
                Ok(key.to_pkcs8_der()?.as_bytes().to_vec())
            } else {
                let key = rsa::RsaPublicKey::new(n, e)?;
                Ok(key.to_public_key_der()?.as_bytes().to_vec())
            }
        }
        ("EC", Some(crv)) => {
            // x and y are checked even for a private key, and before d,
            // because that is the order Node reports missing parameters in.
            let x = jwk_bytes(jwk, "EC", "x")?;
            let y = jwk_bytes(jwk, "EC", "y")?;
            macro_rules! import {
                ($c:ident) => {{
                    if want_private {
                        let d = jwk_bytes(jwk, "EC", "d")?;
                        let key = $c::SecretKey::from_slice(&d)
                            .map_err(|_| anyhow!("invalid {} JWK private scalar", crv))?;
                        // x and y are not decoration: a JWK whose public
                        // point does not match its scalar describes two
                        // different keys, and signing with it would produce
                        // signatures nobody can verify against the x/y the
                        // caller believes they imported.
                        use $c::elliptic_curve::sec1::ToEncodedPoint;
                        let point = key.public_key().to_encoded_point(false);
                        if point.x().map(|v| v.as_slice()) != Some(x.as_slice())
                            || point.y().map(|v| v.as_slice()) != Some(y.as_slice())
                        {
                            return Err(anyhow!(
                                "{} JWK private key is inconsistent with its public point",
                                crv
                            ));
                        }
                        $c::pkcs8::EncodePrivateKey::to_pkcs8_der(&key)?
                            .as_bytes()
                            .to_vec()
                    } else {
                        let mut sec1 = Vec::with_capacity(1 + x.len() + y.len());
                        sec1.push(0x04); // uncompressed point
                        sec1.extend_from_slice(&x);
                        sec1.extend_from_slice(&y);
                        let key = $c::PublicKey::from_sec1_bytes(&sec1)
                            .map_err(|_| anyhow!("invalid {} JWK public point", crv))?;
                        $c::pkcs8::EncodePublicKey::to_public_key_der(&key)?
                            .as_bytes()
                            .to_vec()
                    }
                }};
            }
            Ok(ec_curve!(crv, import))
        }
        ("OKP", Some("X25519")) => {
            // x25519-dalek has no DER codec, so the 32 raw bytes are wrapped
            // by hand. Both structures are fixed-shape for this algorithm:
            // PKCS#8 carries the scalar in an OCTET STRING inside an OCTET
            // STRING, SPKI the public point as a BIT STRING.
            let _ = jwk_bytes(jwk, "OKP", "x")?;
            if want_private {
                let d = jwk_bytes(jwk, "OKP", "d")?;
                let seed: [u8; 32] = d
                    .as_slice()
                    .try_into()
                    .map_err(|_| anyhow!("X25519 JWK private key must be 32 bytes"))?;
                Ok(x25519_pkcs8(&seed))
            } else {
                let x = jwk_bytes(jwk, "OKP", "x")?;
                let point: [u8; 32] = x
                    .as_slice()
                    .try_into()
                    .map_err(|_| anyhow!("X25519 JWK public key must be 32 bytes"))?;
                Ok(x25519_spki(&point))
            }
        }
        ("OKP", Some("Ed25519")) => {
            let _ = jwk_bytes(jwk, "OKP", "x")?;
            if want_private {
                let d = jwk_bytes(jwk, "OKP", "d")?;
                let seed: [u8; 32] = d
                    .as_slice()
                    .try_into()
                    .map_err(|_| anyhow!("Ed25519 JWK private key must be 32 bytes"))?;
                let key = ed25519_dalek::SigningKey::from_bytes(&seed);
                use ed25519_dalek::pkcs8::EncodePrivateKey;
                Ok(key.to_pkcs8_der()?.as_bytes().to_vec())
            } else {
                let x = jwk_bytes(jwk, "OKP", "x")?;
                let point: [u8; 32] = x
                    .as_slice()
                    .try_into()
                    .map_err(|_| anyhow!("Ed25519 JWK public key must be 32 bytes"))?;
                let key = ed25519_dalek::VerifyingKey::from_bytes(&point)
                    .map_err(|_| anyhow!("invalid Ed25519 JWK public key"))?;
                use ed25519_dalek::pkcs8::EncodePublicKey;
                Ok(key.to_public_key_der()?.as_bytes().to_vec())
            }
        }
        _ => Err(anyhow!(
            "JWK {} key import is not implemented for this key type",
            if want_private { "private" } else { "public" }
        )),
    }
}

fn crypto_operation(operation: &str, args: &serde_json::Value) -> Result<serde_json::Value> {
    match operation {
        "hmac-sign" => {
            use hmac::Mac;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let hash = args
                .get("hash")
                .and_then(|value| value.as_str())
                .unwrap_or("SHA-256");
            if !hash.eq_ignore_ascii_case("SHA-256") && !hash.eq_ignore_ascii_case("SHA256") {
                return Err(anyhow!("unsupported HMAC hash"));
            }
            let mut mac = <hmac::Hmac<sha2::Sha256> as hmac::Mac>::new_from_slice(&key)
                .map_err(|_| anyhow!("invalid HMAC key"))?;
            mac.update(&data);
            Ok(serde_json::json!({ "bytes": mac.finalize().into_bytes().to_vec() }))
        }
        // ECDH. Web Crypto's deriveBits returns the raw shared secret -- the
        // x coordinate of the shared point -- with no KDF applied. Its length
        // is the curve's field size, which is why a null `length` is
        // answerable at all.
        "ecdh-derive" => {
            let private = crypto_bytes(args, "private")?;
            let public = crypto_bytes(args, "public")?;
            let crv = ec_jwk_crv(&describe_key(&private, true)?)?;
            if crv != ec_jwk_crv(&describe_key(&public, false)?)? {
                return Err(anyhow!("ECDH keys are on different curves"));
            }
            macro_rules! derive {
                ($c:ident) => {{
                    use $c::pkcs8::DecodePrivateKey;
                    use $c::pkcs8::DecodePublicKey;
                    let secret = $c::SecretKey::from_pkcs8_der(&private)
                        .map_err(|_| anyhow!("invalid ECDH private key"))?;
                    let point = $c::PublicKey::from_public_key_der(&public)
                        .map_err(|_| anyhow!("invalid ECDH public key"))?;
                    $c::elliptic_curve::ecdh::diffie_hellman(
                        secret.to_nonzero_scalar(),
                        point.as_affine(),
                    )
                    .raw_secret_bytes()
                    .to_vec()
                }};
            }
            Ok(serde_json::json!({ "bytes": ec_curve!(crv, derive) }))
        }
        "ed25519-sign" => {
            use ed25519_dalek::pkcs8::DecodePrivateKey;
            use ed25519_dalek::Signer;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let key = ed25519_dalek::SigningKey::from_pkcs8_der(&key)
                .map_err(|_| anyhow!("invalid Ed25519 PKCS#8 key"))?;
            Ok(serde_json::json!({ "bytes": key.sign(&data).to_bytes().to_vec() }))
        }
        "p256-sign" => {
            use p256::ecdsa::signature::Signer;
            use p256::pkcs8::DecodePrivateKey;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let key = p256::ecdsa::SigningKey::from_pkcs8_der(&key)
                .map_err(|_| anyhow!("invalid P-256 PKCS#8 key"))?;
            let signature: p256::ecdsa::Signature = key.sign(&data);
            Ok(serde_json::json!({ "bytes": signature.to_bytes().to_vec() }))
        }
        // JWT verification is the reason these exist: RS256 and ES256 are what
        // token issuers sign with (denoland/celld#124). Both take an SPKI
        // public key, matching `importKey("spki", ...)`.
        // node:crypto's one-shot sign()/verify(). Web Crypto's ops above take
        // raw r||s for ECDSA; Node defaults to DER, so these are separate
        // entry points rather than flags on those.
        "ed25519-verify" => {
            use ed25519_dalek::pkcs8::DecodePublicKey;
            use ed25519_dalek::Verifier;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let signature = crypto_bytes(args, "signature")?;
            let key = ed25519_dalek::VerifyingKey::from_public_key_der(&key)
                .map_err(|_| anyhow!("invalid Ed25519 SPKI key"))?;
            let ok = ed25519_dalek::Signature::from_slice(&signature)
                .map(|signature| key.verify(&data, &signature).is_ok())
                .unwrap_or(false);
            Ok(serde_json::json!({ "ok": ok }))
        }
        "rsa-pkcs1-sign" => {
            use rsa::pkcs8::DecodePrivateKey;
            use rsa::signature::{RandomizedSigner, SignatureEncoding};
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let hash = args
                .get("hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("SHA-256");
            let key = rsa::RsaPrivateKey::from_pkcs8_der(&key)
                .map_err(|_| anyhow!("invalid RSA private key"))?;
            macro_rules! sign {
                ($hash:ty) => {
                    rsa::pkcs1v15::SigningKey::<$hash>::new(key)
                        // PKCS#1 v1.5 produces the same signature with or
                        // without randomness. The RNG blinds the private-key
                        // operation, so its timing does not expose the key.
                        .try_sign_with_rng(&mut rand::rngs::OsRng, &data)
                        .map_err(|_| anyhow!("RSA signing failed"))?
                        .to_vec()
                };
            }
            let bytes = match hash {
                "SHA-256" => sign!(sha2::Sha256),
                "SHA-384" => sign!(sha2::Sha384),
                "SHA-512" => sign!(sha2::Sha512),
                other => return Err(anyhow!("unsupported RSA sign hash {other}")),
            };
            Ok(serde_json::json!({ "bytes": bytes }))
        }
        // Node's ECDSA signatures are DER by default (`dsaEncoding`), where
        // Web Crypto's are raw r||s.
        "ec-sign-der" => {
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let hash = args
                .get("hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("SHA-256");
            let digest = digest_bytes(hash, &data)?;
            let parsed = describe_key(&key, true)?;
            let crv = ec_jwk_crv(&parsed)?;
            macro_rules! sign {
                ($c:ident) => {{
                    use $c::ecdsa::signature::hazmat::PrehashSigner;
                    use $c::pkcs8::DecodePrivateKey;
                    let secret = $c::SecretKey::from_pkcs8_der(&key)
                        .map_err(|_| anyhow!("invalid {crv} PKCS#8 key"))?;
                    let key = $c::ecdsa::SigningKey::from_slice(secret.to_bytes().as_slice())
                        .map_err(|_| anyhow!("invalid {crv} signing key"))?;
                    let signature: $c::ecdsa::Signature = key
                        .sign_prehash(&digest)
                        .map_err(|_| anyhow!("could not sign with {crv}"))?;
                    signature.to_der().as_bytes().to_vec()
                }};
            }
            Ok(serde_json::json!({ "bytes": ec_curve!(crv, sign) }))
        }
        "ec-verify-der" => {
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let signature = crypto_bytes(args, "signature")?;
            let hash = args
                .get("hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("SHA-256");
            let digest = digest_bytes(hash, &data)?;
            let parsed = describe_key(&key, false)?;
            let crv = ec_jwk_crv(&parsed)?;
            macro_rules! verify {
                ($c:ident) => {{
                    use $c::ecdsa::signature::hazmat::PrehashVerifier;
                    use $c::elliptic_curve::sec1::ToEncodedPoint;
                    use $c::pkcs8::DecodePublicKey;
                    let public = $c::PublicKey::from_public_key_der(&key)
                        .map_err(|_| anyhow!("invalid {crv} SPKI key"))?;
                    let point = public.to_encoded_point(false);
                    let key = $c::ecdsa::VerifyingKey::from_sec1_bytes(point.as_bytes())
                        .map_err(|_| anyhow!("invalid {crv} verifying key"))?;
                    $c::ecdsa::DerSignature::try_from(signature.as_slice())
                        .ok()
                        .and_then(|signature| $c::ecdsa::Signature::try_from(signature).ok())
                        .map(|signature| key.verify_prehash(&digest, &signature).is_ok())
                        .unwrap_or(false)
                }};
            }
            let ok = ec_curve!(crv, verify);
            Ok(serde_json::json!({ "ok": ok }))
        }
        "p256-verify" => {
            use p256::ecdsa::signature::Verifier;
            use p256::pkcs8::DecodePublicKey;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let signature = crypto_bytes(args, "signature")?;
            let key = p256::ecdsa::VerifyingKey::from_public_key_der(&key)
                .map_err(|_| anyhow!("invalid P-256 SPKI key"))?;
            // WebCrypto passes ECDSA signatures as raw r||s, not DER.
            let ok = p256::ecdsa::Signature::from_slice(&signature)
                .map(|signature| key.verify(&data, &signature).is_ok())
                .unwrap_or(false);
            Ok(serde_json::json!({ "ok": ok }))
        }
        "rsa-pkcs1-verify" => {
            use rsa::pkcs8::DecodePublicKey;
            use rsa::signature::Verifier;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let signature = crypto_bytes(args, "signature")?;
            let hash = args
                .get("hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("SHA-256")
                .to_string();
            let key = rsa::RsaPublicKey::from_public_key_der(&key)
                .map_err(|_| anyhow!("invalid RSA SPKI key"))?;
            macro_rules! verify {
                ($hash:ty) => {{
                    let key = rsa::pkcs1v15::VerifyingKey::<$hash>::new(key);
                    rsa::pkcs1v15::Signature::try_from(signature.as_slice())
                        .map(|signature| key.verify(&data, &signature).is_ok())
                        .unwrap_or(false)
                }};
            }
            let ok = match hash.as_str() {
                "SHA-256" => verify!(sha2::Sha256),
                "SHA-384" => verify!(sha2::Sha384),
                "SHA-512" => verify!(sha2::Sha512),
                other => return Err(anyhow!("unsupported RSA verify hash {other}")),
            };
            Ok(serde_json::json!({ "ok": ok }))
        }
        // Web Crypto RSA-PSS. Signing takes the caller's salt length —
        // the spec makes it an explicit parameter — and verification
        // recovers the salt length from the signature, which accepts
        // every salt the signer could legally have used.
        "rsa-pss-sign" => {
            use rsa::pkcs8::DecodePrivateKey;
            use rsa::signature::RandomizedSigner;
            use rsa::signature::SignatureEncoding;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let salt_len = args
                .get("saltLength")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0) as usize;
            let hash = args
                .get("hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("SHA-256");
            let key = rsa::RsaPrivateKey::from_pkcs8_der(&key)
                .map_err(|_| anyhow!("invalid RSA private key"))?;
            macro_rules! sign {
                ($hash:ty) => {
                    rsa::pss::SigningKey::<$hash>::new_with_salt_len(key, salt_len)
                        .sign_with_rng(&mut rand::rngs::OsRng, &data)
                        .to_vec()
                };
            }
            let bytes = match hash {
                "SHA-256" => sign!(sha2::Sha256),
                "SHA-384" => sign!(sha2::Sha384),
                "SHA-512" => sign!(sha2::Sha512),
                other => return Err(anyhow!("unsupported RSA sign hash {other}")),
            };
            Ok(serde_json::json!({ "bytes": bytes }))
        }
        "rsa-pss-verify" => {
            use rsa::pkcs8::DecodePublicKey;
            use rsa::signature::Verifier;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let signature = crypto_bytes(args, "signature")?;
            let hash = args
                .get("hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("SHA-256")
                .to_string();
            let key = rsa::RsaPublicKey::from_public_key_der(&key)
                .map_err(|_| anyhow!("invalid RSA SPKI key"))?;
            macro_rules! verify {
                ($hash:ty) => {{
                    let key = rsa::pss::VerifyingKey::<$hash>::new(key);
                    rsa::pss::Signature::try_from(signature.as_slice())
                        .map(|signature| key.verify(&data, &signature).is_ok())
                        .unwrap_or(false)
                }};
            }
            let ok = match hash.as_str() {
                "SHA-256" => verify!(sha2::Sha256),
                "SHA-384" => verify!(sha2::Sha384),
                "SHA-512" => verify!(sha2::Sha512),
                other => return Err(anyhow!("unsupported RSA verify hash {other}")),
            };
            Ok(serde_json::json!({ "ok": ok }))
        }
        "rsa-oaep-encrypt" => {
            use rsa::pkcs8::DecodePublicKey;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let label = crypto_bytes(args, "label")?;
            let hash = args
                .get("hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("SHA-256");
            let key = rsa::RsaPublicKey::from_public_key_der(&key)
                .map_err(|_| anyhow!("invalid RSA public key"))?;
            let bytes = key.encrypt(
                &mut rand::rngs::OsRng,
                rsa_oaep_padding(hash, label)?,
                &data,
            )?;
            Ok(serde_json::json!({ "bytes": bytes }))
        }
        "rsa-oaep-decrypt" => {
            use rsa::pkcs8::DecodePrivateKey;
            let key = crypto_bytes(args, "key")?;
            let data = crypto_bytes(args, "data")?;
            let label = crypto_bytes(args, "label")?;
            let hash = args
                .get("hash")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("SHA-256");
            let key = rsa::RsaPrivateKey::from_pkcs8_der(&key)
                .map_err(|_| anyhow!("invalid RSA private key"))?;
            // The plain decrypt API omits RSA blinding. Use the randomized
            // API so repeated attacker-controlled ciphertexts do not expose
            // the private key through operation timing.
            let bytes = key.decrypt_blinded(
                &mut rand::rngs::OsRng,
                rsa_oaep_padding(hash, label)?,
                &data,
            )?;
            Ok(serde_json::json!({ "bytes": bytes }))
        }
        // `createPublicKey` / `createPrivateKey`. Accepts every input shape
        // Node does — PEM, bare DER with a stated structure, or a JWK — and
        // answers with one normalized DER plus what Node reports as
        // `asymmetricKeyType` and `asymmetricKeyDetails`.
        "asym-key-import" => {
            let format = args
                .get("format")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("pem");
            let want_private = args
                .get("visibility")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|visibility| visibility == "private");

            let (der, is_private) = if format == "jwk" {
                let jwk = args.get("key").ok_or_else(|| anyhow!("missing JWK"))?;
                (key_from_jwk(jwk, want_private)?, want_private)
            } else {
                let key = crypto_bytes(args, "key")?;
                let stated = args.get("type").and_then(serde_json::Value::as_str);
                let passphrase = args
                    .get("passphrase")
                    .filter(|value| !value.is_null())
                    .map(|_| crypto_bytes(args, "passphrase"))
                    .transpose()?;
                if format == "pem" {
                    let text = std::str::from_utf8(&key)
                        .map_err(|_| anyhow!("PEM key is not valid UTF-8"))?;
                    if text.contains("ENCRYPTED PRIVATE KEY") {
                        let passphrase = passphrase
                            .ok_or_else(|| anyhow!("Passphrase required for encrypted key"))?;
                        use pkcs8::DecodePrivateKey as _;
                        let document =
                            pkcs8::SecretDocument::from_pkcs8_encrypted_pem(text, &passphrase)
                                .map_err(|_| anyhow!("Failed to decrypt private key"))?;
                        (document.as_bytes().to_vec(), true)
                    } else {
                        let (label, document) = rsa::pkcs8::Document::from_pem(text)
                            .map_err(|_| anyhow!("invalid PEM key"))?;
                        normalize_key(document.as_bytes(), pem_structure(label)?)?
                    }
                } else if let Ok(sealed) = pkcs8::EncryptedPrivateKeyInfo::try_from(key.as_slice())
                {
                    // An encrypted PKCS#8 blob is its own structure, so it is
                    // recognized before the sniffer runs: the sniffer would
                    // read it as an unknown key and report a parse failure
                    // rather than a missing or wrong passphrase.
                    let passphrase = passphrase
                        .ok_or_else(|| anyhow!("Passphrase required for encrypted key"))?;
                    let document = sealed
                        .decrypt(&passphrase)
                        .map_err(|_| anyhow!("Failed to decrypt private key"))?;
                    (document.as_bytes().to_vec(), true)
                } else {
                    (
                        normalize_key_sniffing(&key, stated, want_private)?,
                        want_private,
                    )
                }
            };

            if is_private != want_private {
                return Err(anyhow!(
                    "expected a {} key, got a {} key",
                    if want_private { "private" } else { "public" },
                    if is_private { "private" } else { "public" }
                ));
            }
            let parsed = describe_key(&der, is_private)?;
            Ok(serde_json::json!({
                "keyType": parsed.kind,
                "der": parsed.der,
                "details": parsed.details,
            }))
        }
        // `createPublicKey(privateKeyObject)`: derive the public half from an
        // already-normalized PKCS#8 key, so the caller never re-parses.
        "asym-key-public-from-private" => {
            use rsa::pkcs8::EncodePublicKey;
            let der = crypto_bytes(args, "der")?;
            let parsed = describe_key(&der, true)?;
            let public = match parsed.kind {
                "rsa" => {
                    use rsa::pkcs8::DecodePrivateKey;
                    let key = rsa::RsaPrivateKey::from_pkcs8_der(&der)
                        .map_err(|_| anyhow!("invalid RSA private key"))?;
                    rsa::RsaPublicKey::from(&key)
                        .to_public_key_der()?
                        .as_bytes()
                        .to_vec()
                }
                "ec" => {
                    let crv = ec_jwk_crv(&parsed)?;
                    macro_rules! public_from_private {
                        ($c:ident) => {{
                            use $c::pkcs8::DecodePrivateKey;
                            let key = $c::SecretKey::from_pkcs8_der(&der)
                                .map_err(|_| anyhow!("invalid {crv} private key"))?;
                            key.public_key().to_public_key_der()?.as_bytes().to_vec()
                        }};
                    }
                    ec_curve!(crv, public_from_private)
                }
                "ed25519" => {
                    use ed25519_dalek::pkcs8::DecodePrivateKey;
                    use ed25519_dalek::pkcs8::EncodePublicKey;
                    let key = ed25519_dalek::SigningKey::from_pkcs8_der(&der)
                        .map_err(|_| anyhow!("invalid Ed25519 private key"))?;
                    key.verifying_key().to_public_key_der()?.as_bytes().to_vec()
                }
                "x25519" => {
                    let scalar: [u8; 32] = der[der.len() - 32..]
                        .try_into()
                        .expect("X25519 PKCS#8 ends with the 32-byte scalar");
                    let secret = x25519_dalek::StaticSecret::from(scalar);
                    x25519_spki(x25519_dalek::PublicKey::from(&secret).as_bytes())
                }
                other => return Err(anyhow!("cannot derive a public key for {other}")),
            };
            let parsed = describe_key(&public, false)?;
            Ok(serde_json::json!({
                "keyType": parsed.kind,
                "der": parsed.der,
                "details": parsed.details,
            }))
        }
        // `generateKeyPairSync`. Answers in the same normalized encodings as
        // import — PKCS#8 and SPKI — so the two paths share everything
        // downstream.
        "asym-key-generate" => {
            use ed25519_dalek::pkcs8::EncodePublicKey as _;
            use p256::pkcs8::EncodePrivateKey as _;
            let kind = args
                .get("type")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| anyhow!("missing key type"))?;
            let mut rng = rand::rngs::OsRng;
            let (private, public) = match kind {
                "rsa" => {
                    let bits = args
                        .get("modulusLength")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| anyhow!("missing modulusLength"))?
                        as usize;
                    let mut key = rsa::RsaPrivateKey::new(&mut rng, bits)?;
                    key.precompute()?;
                    let public = rsa::RsaPublicKey::from(&key);
                    (
                        key.to_pkcs8_der()?.as_bytes().to_vec(),
                        public.to_public_key_der()?.as_bytes().to_vec(),
                    )
                }
                "ec" => {
                    // The JS layer maps Node's curve aliases and rejects
                    // anything not in EC_CURVES before reaching here.
                    let crv = args
                        .get("namedCurve")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("P-256");
                    macro_rules! generate {
                        ($c:ident) => {{
                            let key = $c::SecretKey::random(&mut rng);
                            (
                                $c::pkcs8::EncodePrivateKey::to_pkcs8_der(&key)?
                                    .as_bytes()
                                    .to_vec(),
                                $c::pkcs8::EncodePublicKey::to_public_key_der(&key.public_key())?
                                    .as_bytes()
                                    .to_vec(),
                            )
                        }};
                    }
                    ec_curve!(crv, generate)
                }
                "ed25519" => {
                    use ed25519_dalek::pkcs8::EncodePrivateKey as _;
                    // No `generate` on this build of ed25519-dalek (its
                    // `rand_core` feature is off); the seed is 32 random
                    // bytes either way.
                    let mut seed = [0u8; 32];
                    rand::RngCore::fill_bytes(&mut rng, &mut seed);
                    let key = ed25519_dalek::SigningKey::from_bytes(&seed);
                    (
                        key.to_pkcs8_der()?.as_bytes().to_vec(),
                        key.verifying_key().to_public_key_der()?.as_bytes().to_vec(),
                    )
                }
                "x25519" => {
                    let secret = x25519_dalek::StaticSecret::random_from_rng(rng);
                    let public = x25519_dalek::PublicKey::from(&secret);
                    (
                        x25519_pkcs8(&secret.to_bytes()),
                        x25519_spki(public.as_bytes()),
                    )
                }
                other => return Err(anyhow!("cannot generate a {other} key pair")),
            };
            let described = describe_key(&private, true)?;
            Ok(serde_json::json!({
                "keyType": described.kind,
                "privateDer": private,
                "publicDer": public,
                "details": described.details,
            }))
        }
        // Re-encode a normalized key into one of Node's export structures.
        // Import converts every structure *to* PKCS#8/SPKI; this is the
        // inverse, and `generateKeyPairSync`'s encoding options need it.
        "asym-key-reencode" => {
            use rsa::pkcs1::EncodeRsaPrivateKey as _;
            use rsa::pkcs1::EncodeRsaPublicKey as _;
            let der = crypto_bytes(args, "der")?;
            let private = args
                .get("visibility")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|visibility| visibility == "private");
            let structure = args
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(if private { "pkcs8" } else { "spki" });
            let described = describe_key(&der, private)?;
            // A passphrase turns the export into encrypted PKCS#8, whatever
            // structure was asked for -- that is the only structure the
            // encryption wrapper defines.
            if let Some(passphrase) = args
                .get("passphrase")
                .filter(|value| !value.is_null())
                .map(|_| crypto_bytes(args, "passphrase"))
                .transpose()?
            {
                if !private {
                    return Err(anyhow!("only a private key can be encrypted"));
                }
                let info = pkcs8::PrivateKeyInfo::try_from(der.as_slice())
                    .map_err(|_| anyhow!("invalid PKCS#8 private key"))?;
                let cipher = args
                    .get("cipher")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| anyhow!("an encrypted export needs a cipher"))?;
                use rand::RngCore as _;
                let mut salt = [0u8; PBES2_SALT_LEN];
                let mut iv = [0u8; 16];
                rand::rngs::OsRng.fill_bytes(&mut salt);
                rand::rngs::OsRng.fill_bytes(&mut iv);
                let params = pbes2_parameters(cipher, &salt, &iv)?;
                let document = info
                    .encrypt_with_params(params, &passphrase)
                    .map_err(|_| anyhow!("could not encrypt the private key"))?;
                let pem = args
                    .get("format")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|format| format == "pem");
                return Ok(if pem {
                    serde_json::json!({
                        "pem": pem_wrap("ENCRYPTED PRIVATE KEY", document.as_bytes()),
                    })
                } else {
                    serde_json::json!({ "der": document.as_bytes() })
                });
            }
            let (label, bytes) = match (structure, described.kind) {
                ("pkcs8", _) => ("PRIVATE KEY", der.clone()),
                ("spki", _) => ("PUBLIC KEY", der.clone()),
                ("pkcs1", "rsa") if private => {
                    use rsa::pkcs8::DecodePrivateKey;
                    let key = rsa::RsaPrivateKey::from_pkcs8_der(&der)?;
                    ("RSA PRIVATE KEY", key.to_pkcs1_der()?.as_bytes().to_vec())
                }
                ("pkcs1", "rsa") => {
                    use rsa::pkcs8::DecodePublicKey;
                    let key = rsa::RsaPublicKey::from_public_key_der(&der)?;
                    ("RSA PUBLIC KEY", key.to_pkcs1_der()?.as_bytes().to_vec())
                }
                ("sec1", "ec") if private => {
                    use p256::elliptic_curve::sec1::ToEncodedPoint;
                    use p256::pkcs8::DecodePrivateKey;
                    let key = p256::SecretKey::from_pkcs8_der(&der)?;
                    let _ = key.public_key().to_encoded_point(false);
                    ("EC PRIVATE KEY", key.to_sec1_der()?.to_vec())
                }
                (structure, kind) => {
                    return Err(anyhow!("cannot export a {kind} key as {structure}"))
                }
            };
            let pem = args
                .get("format")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|format| format == "pem");
            if pem {
                Ok(serde_json::json!({ "pem": pem_wrap(label, &bytes) }))
            } else {
                Ok(serde_json::json!({ "der": bytes }))
            }
        }
        // Export a normalized key back out. `jwk` is the shape `toCryptoKey`
        // and Node's `export({format:"jwk"})` both want.
        "asym-key-export" => {
            let der = crypto_bytes(args, "der")?;
            let private = args
                .get("visibility")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|visibility| visibility == "private");
            let parsed = describe_key(&der, private)?;
            let jwk = match (parsed.kind, private) {
                ("rsa", false) => {
                    use rsa::pkcs8::DecodePublicKey;
                    use rsa::traits::PublicKeyParts;
                    let key = rsa::RsaPublicKey::from_public_key_der(&der)?;
                    serde_json::json!({
                        "kty": "RSA",
                        "n": rsa_jwk_encode(key.n()),
                        "e": rsa_jwk_encode(key.e()),
                    })
                }
                ("rsa", true) => {
                    use rsa::pkcs8::DecodePrivateKey;
                    use rsa::traits::{PrivateKeyParts, PublicKeyParts};
                    let key = rsa::RsaPrivateKey::from_pkcs8_der(&der)?;
                    // The CRT parameters are part of the JWK, not an
                    // optimization detail: Node emits all of dp, dq and qi.
                    let missing = || anyhow!("RSA key has no CRT parameters");
                    serde_json::json!({
                        "kty": "RSA",
                        "n": rsa_jwk_encode(key.n()),
                        "e": rsa_jwk_encode(key.e()),
                        "d": rsa_jwk_encode(key.d()),
                        "p": rsa_jwk_encode(&key.primes()[0]),
                        "q": rsa_jwk_encode(&key.primes()[1]),
                        "dp": rsa_jwk_encode(key.dp().ok_or_else(missing)?),
                        "dq": rsa_jwk_encode(key.dq().ok_or_else(missing)?),
                        "qi": rsa_jwk_encode(&key.crt_coefficient().ok_or_else(missing)?),
                    })
                }
                ("ec", false) => {
                    let crv = ec_jwk_crv(&parsed)?;
                    macro_rules! export {
                        ($c:ident) => {{
                            use $c::elliptic_curve::sec1::ToEncodedPoint;
                            use $c::pkcs8::DecodePublicKey;
                            let key = $c::PublicKey::from_public_key_der(&der)?;
                            let point = key.to_encoded_point(false);
                            serde_json::json!({
                                "kty": "EC",
                                "crv": crv,
                                "x": base64url(point.x().ok_or_else(|| anyhow!("no x"))?),
                                "y": base64url(point.y().ok_or_else(|| anyhow!("no y"))?),
                            })
                        }};
                    }
                    ec_curve!(crv, export)
                }
                ("ec", true) => {
                    let crv = ec_jwk_crv(&parsed)?;
                    macro_rules! export {
                        ($c:ident) => {{
                            use $c::elliptic_curve::sec1::ToEncodedPoint;
                            use $c::pkcs8::DecodePrivateKey;
                            let key = $c::SecretKey::from_pkcs8_der(&der)?;
                            let point = key.public_key().to_encoded_point(false);
                            serde_json::json!({
                                "kty": "EC",
                                "crv": crv,
                                "x": base64url(point.x().ok_or_else(|| anyhow!("no x"))?),
                                "y": base64url(point.y().ok_or_else(|| anyhow!("no y"))?),
                                "d": base64url(&key.to_bytes()),
                            })
                        }};
                    }
                    ec_curve!(crv, export)
                }
                ("x25519", _) => {
                    let raw = &der[der.len() - 32..];
                    let mut jwk = serde_json::json!({ "kty": "OKP", "crv": "X25519" });
                    if private {
                        let scalar: [u8; 32] = raw.try_into().unwrap();
                        let secret = x25519_dalek::StaticSecret::from(scalar);
                        jwk["x"] = serde_json::Value::String(base64url(
                            x25519_dalek::PublicKey::from(&secret).as_bytes(),
                        ));
                        jwk["d"] = serde_json::Value::String(base64url(raw));
                    } else {
                        jwk["x"] = serde_json::Value::String(base64url(raw));
                    }
                    jwk
                }
                ("ed25519", false) => {
                    use ed25519_dalek::pkcs8::DecodePublicKey;
                    let key = ed25519_dalek::VerifyingKey::from_public_key_der(&der)
                        .map_err(|_| anyhow!("invalid Ed25519 public key"))?;
                    serde_json::json!({
                        "alg": "EdDSA",
                        "kty": "OKP",
                        "crv": "Ed25519",
                        "x": base64url(key.as_bytes()),
                    })
                }
                ("ed25519", true) => {
                    use ed25519_dalek::pkcs8::DecodePrivateKey;
                    let key = ed25519_dalek::SigningKey::from_pkcs8_der(&der)
                        .map_err(|_| anyhow!("invalid Ed25519 private key"))?;
                    serde_json::json!({
                        "alg": "EdDSA",
                        "kty": "OKP",
                        "crv": "Ed25519",
                        "x": base64url(key.verifying_key().as_bytes()),
                        "d": base64url(&key.to_bytes()),
                    })
                }
                ("dsa", _) => return Err(anyhow!("Key type is invalid for JWK export")),
                (other, _) => return Err(anyhow!("cannot export {other} as JWK")),
            };
            Ok(serde_json::json!({
                "keyType": parsed.kind,
                "jwk": jwk,
                "details": parsed.details,
            }))
        }
        _ => Err(anyhow!("unsupported crypto operation")),
    }
}

pub(super) fn op_crypto_operation(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let operation = args.get(0).to_rust_string_lossy(scope);
    // An undecodable argument used to become `Value::Null`, and the operation
    // then ran against it. Every field lookup missed, so the caller was told
    // its key bytes were invalid when the real fault was the argument object
    // itself. Name the argument instead.
    let input: serde_json::Value =
        match serde_json::from_str(&args.get(1).to_rust_string_lossy(scope)) {
            Ok(input) => input,
            Err(error) => {
                return loader_throw(
                    scope,
                    &format!("crypto: the argument object is not JSON: {error}"),
                )
            }
        };
    match crypto_operation(&operation, &input) {
        Ok(value) => {
            let json = value.to_string();
            rv.set(v8::String::new(scope, &json).unwrap().into());
        }
        Err(error) => {
            let message = v8::String::new(scope, &format!("crypto: {error}")).unwrap();
            let exception = v8::Exception::error(scope, message);
            scope.throw_exception(exception);
        }
    }
}

/// Fill an existing integer typed array in-place. JS performs the WebCrypto
/// type and 65,536-byte quota checks before entering this host op.
pub(super) fn op_webcrypto_random(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let Ok(view) = v8::Local::<v8::ArrayBufferView>::try_from(args.get(0)) else {
        return;
    };
    let Some(buffer) = view.buffer(scope) else {
        return;
    };
    let store = buffer.get_backing_store();
    let offset = view.byte_offset();
    let len = view.byte_length();
    let bytes = unsafe {
        std::slice::from_raw_parts_mut(store[offset..offset + len].as_ptr() as *mut u8, len)
    };
    if getrandom::fill(bytes).is_err() {
        let message = v8::String::new(scope, "secure random generation failed").unwrap();
        let exception = v8::Exception::error(scope, message);
        scope.throw_exception(exception);
    }
}

// `crc::Crc::new` builds a 256-entry lookup table by value — 1 KiB for a
// `Crc<u32>`, 2 KiB for a `Crc<u64>`. Constructed inside `op_webcrypto_digest`,
// that table is rebuilt for every chunk an S3-uploading Worker checksums
// through `DigestStream`. `Crc::new` is a `const fn`, so a `static` initializer
// is a const context and the table is built at compile time instead. A
// `LazyLock` would also hoist the build out of the call, but it adds an atomic
// guard check to every use that a plain `static` does not need.
static CRC32: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
static CRC32C: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
static CRC64NVME: crc::Crc<u64> = crc::Crc::<u64>::new(&crc::CRC_64_NVME);

/// WebCrypto digest: (algorithm, ArrayBufferView) -> Uint8Array.
pub(super) fn op_webcrypto_digest(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    use sha2::Digest;
    let algorithm = args.get(0).to_rust_string_lossy(scope).to_ascii_uppercase();
    let Some(bytes) = view_bytes(args.get(1)) else {
        return;
    };
    let digest = match algorithm.as_str() {
        "MD5" => md5::Md5::digest(&bytes).to_vec(),
        // Not digests, but DigestStream accepts them: the AWS SDKs and S3
        // checksum with these, so a Worker computing an upload checksum
        // needs them. Big-endian, as the checksum headers carry them.
        "CRC32" => CRC32.checksum(&bytes).to_be_bytes().to_vec(),
        "CRC32C" => CRC32C.checksum(&bytes).to_be_bytes().to_vec(),
        "CRC64NVME" => CRC64NVME.checksum(&bytes).to_be_bytes().to_vec(),
        "SHA-1" => sha1::Sha1::digest(&bytes).to_vec(),
        "SHA-224" => sha2::Sha224::digest(&bytes).to_vec(),
        "SHA-256" => sha2::Sha256::digest(&bytes).to_vec(),
        "SHA-384" => sha2::Sha384::digest(&bytes).to_vec(),
        "SHA-512" => sha2::Sha512::digest(&bytes).to_vec(),
        _ => return,
    };
    webcrypto_return_bytes(scope, rv, &digest);
}

// Signing and verification must accept the same HMAC hashes. Separate match
// tables let MD5 and SHA-224 reach signing while verification returned no JS
// value, so keep the set in one dispatcher and supply only the operation.
macro_rules! dispatch_hmac_digest {
    ($name:expr, $operation:ident) => {
        match $name {
            "MD5" => $operation!(md5::Md5),
            "SHA-1" => $operation!(sha1::Sha1),
            "SHA-224" => $operation!(sha2::Sha224),
            "SHA-256" => $operation!(sha2::Sha256),
            "SHA-384" => $operation!(sha2::Sha384),
            "SHA-512" => $operation!(sha2::Sha512),
            _ => return,
        }
    };
}

pub(super) fn op_webcrypto_hmac_sign(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    use hmac::Mac;
    let algorithm = args.get(0).to_rust_string_lossy(scope).to_ascii_uppercase();
    let Some(key) = view_bytes(args.get(1)) else {
        return;
    };
    let Some(data) = view_bytes(args.get(2)) else {
        return;
    };
    macro_rules! sign {
        ($digest:ty) => {{
            let Ok(mut mac) = <hmac::Hmac<$digest> as hmac::Mac>::new_from_slice(&key) else {
                return;
            };
            mac.update(&data);
            mac.finalize().into_bytes().to_vec()
        }};
    }
    let signature = dispatch_hmac_digest!(algorithm.as_str(), sign);
    webcrypto_return_bytes(scope, rv, &signature);
}

/// The digest bound `hmac::Hmac` carries: a digest whose core exposes the
/// eager, block-level API. Naming it once keeps the two KDF signatures below
/// readable, because `pbkdf2::pbkdf2_hmac` and `hkdf::Hkdf` both spell out the
/// same eight lines. Every digest `dispatch_digest!` routes — MD5, SHA-1 and
/// the four SHA-2 sizes — satisfies it, and a digest that did not would fail
/// to compile at the macro rather than fall back to something slower.
///
/// `hmac::SimpleHmac` needs only `Digest + BlockSizeUser` and would let this
/// bound go away, but it drives the digest through the generic `Digest`
/// wrapper instead of the core, which is why it is the slower of the two.
/// `op_webcrypto_hmac_sign` and `op_webcrypto_hmac_verify` in this file
/// already use `Hmac`.
///
/// A bare `trait EagerDigest: CoreProxy where Self::Core: ...` does not work:
/// a where-clause on a trait definition constrains implementors but is not
/// elaborated for callers, so `D: EagerDigest` alone would not discharge the
/// bound at the `pbkdf2_hmac` and `Hkdf` call sites. Carrying the two KDF
/// entry points as trait methods discharges it inside the impl instead, which
/// is the only reason this is a trait with methods rather than an alias.
trait EagerDigest {
    fn pbkdf2_into(password: &[u8], salt: &[u8], iterations: u32, out: &mut [u8]);

    /// `None` only when `out` is longer than the 255×hashLen of RFC 5869.
    fn hkdf_into(ikm: &[u8], salt: &[u8], info: &[u8], out: &mut [u8]) -> Option<()>;
}

impl<D> EagerDigest for D
where
    D: hmac::digest::core_api::CoreProxy + hmac::digest::OutputSizeUser,
    D::Core: hmac::digest::HashMarker
        + hmac::digest::core_api::UpdateCore
        + hmac::digest::core_api::FixedOutputCore
        + hmac::digest::core_api::BufferKindUser<BufferKind = hmac::digest::block_buffer::Eager>
        + Default
        + Clone
        + Sync,
    <D::Core as hmac::digest::core_api::BlockSizeUser>::BlockSize:
        hmac::digest::typenum::IsLess<hmac::digest::typenum::U256>,
    hmac::digest::typenum::Le<
        <D::Core as hmac::digest::core_api::BlockSizeUser>::BlockSize,
        hmac::digest::typenum::U256,
    >: hmac::digest::typenum::NonZero,
{
    fn pbkdf2_into(password: &[u8], salt: &[u8], iterations: u32, out: &mut [u8]) {
        pbkdf2::pbkdf2_hmac::<D>(password, salt, iterations, out);
    }

    fn hkdf_into(ikm: &[u8], salt: &[u8], info: &[u8], out: &mut [u8]) -> Option<()> {
        hkdf::Hkdf::<D>::new(Some(salt), ikm).expand(info, out).ok()
    }
}

/// PBKDF2 (RFC 2898) for node:crypto, over the `pbkdf2` crate.
///
/// This was a hand-rolled loop, justified in its own comment as avoiding an
/// extra dependency. That justification was false: `pbkdf2` already reaches
/// this build through `pkcs5`, which this file uses for encrypted PKCS#8, so
/// the crate was compiled either way and only the `Cargo.toml` line was
/// missing. The loop re-derived the HMAC key schedule from the password and
/// allocated a `Vec` on every one of the 100,000-plus iterations a real
/// caller asks for; the crate primes one HMAC and clones the primed state per
/// iteration, which is about twice as fast.
fn pbkdf2_kdf<D: EagerDigest>(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
    keylen: usize,
) -> Vec<u8> {
    let mut out = vec![0u8; keylen];
    D::pbkdf2_into(password, salt, iterations, &mut out);
    out
}

/// HKDF (RFC 5869) for node:crypto, over the `hkdf` crate, which reaches this
/// build through `elliptic-curve` for the same reason.
///
/// `None` means the caller asked for more than the 255×hashLen RFC 5869
/// defines. The hand-rolled version counted output blocks in a `u8` starting
/// at 1 and trusted a comment saying the JS layer capped the length. The cap
/// is real — `getHkdf` in `node_crypto.js` throws a `RangeError` above
/// 255×hashLen — but it admits exactly 255×hashLen, and the old loop
/// incremented the counter once more after emitting block 255. So an ordinary
/// `hkdfSync(hash, ikm, salt, info, 255 * hashLen)` overflowed the counter: a
/// panic under `debug-assertions`, and a silent wrap without them. Returning
/// the length check from the KDF instead of asserting it in a doc comment
/// keeps the ops correct whatever JS does, which matters because the ops are
/// own properties of the user's `globalThis` and only non-enumerable.
fn hkdf_kdf<D: EagerDigest>(
    ikm: &[u8],
    salt: &[u8],
    info: &[u8],
    length: usize,
) -> Option<Vec<u8>> {
    let mut out = vec![0u8; length];
    D::hkdf_into(ikm, salt, info, &mut out)?;
    Some(out)
}

/// Routes a digest name to a KDF instantiated over that digest. `None` is an
/// unknown name, which leaves the calling op's return value unset. This list
/// must stay equal to `getHashes()` in `node_crypto.js`: that function is
/// what the JS layer validates a caller's digest name against, so a name it
/// advertises but this macro omits becomes an `undefined` return.
macro_rules! dispatch_digest {
    ($name:expr, $fn:ident, ($($arg:expr),*)) => {
        match $name {
            "MD5" => Some($fn::<md5::Md5>($($arg),*)),
            "SHA-1" => Some($fn::<sha1::Sha1>($($arg),*)),
            "SHA-224" => Some($fn::<sha2::Sha224>($($arg),*)),
            "SHA-256" => Some($fn::<sha2::Sha256>($($arg),*)),
            "SHA-384" => Some($fn::<sha2::Sha384>($($arg),*)),
            "SHA-512" => Some($fn::<sha2::Sha512>($($arg),*)),
            _ => None,
        }
    };
}

/// `$$pbkdf2(algorithm, password, salt, iterations, keylen)` -> Uint8Array
pub(super) fn op_node_pbkdf2(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let algorithm = args.get(0).to_rust_string_lossy(scope);
    let Some(password) = view_bytes(args.get(1)) else {
        return;
    };
    let Some(salt) = view_bytes(args.get(2)) else {
        return;
    };
    let iterations = args.get(3).uint32_value(scope).unwrap_or(0).max(1);
    let keylen = args.get(4).uint32_value(scope).unwrap_or(0) as usize;
    let out = dispatch_digest!(
        algorithm.as_str(),
        pbkdf2_kdf,
        (&password, &salt, iterations, keylen)
    );
    if let Some(out) = out {
        webcrypto_return_bytes(scope, rv, &out);
    }
}

/// `$$hkdf(algorithm, ikm, salt, info, length)` -> Uint8Array
pub(super) fn op_node_hkdf(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let algorithm = args.get(0).to_rust_string_lossy(scope);
    let Some(ikm) = view_bytes(args.get(1)) else {
        return;
    };
    let Some(salt) = view_bytes(args.get(2)) else {
        return;
    };
    let Some(info) = view_bytes(args.get(3)) else {
        return;
    };
    let length = args.get(4).uint32_value(scope).unwrap_or(0) as usize;
    // The outer `Option` is an unknown digest, the inner one is a length above
    // 255xhashLen. Both leave the return value unset, which is what every
    // other reject in these ops does.
    let out = dispatch_digest!(algorithm.as_str(), hkdf_kdf, (&ikm, &salt, &info, length));
    if let Some(Some(out)) = out {
        webcrypto_return_bytes(scope, rv, &out);
    }
}

/// Constant-time equality for equal-length byte views. The JS surfaces own
/// their distinct argument and length errors; one native primitive owns the
/// security property both APIs promise.
pub(super) fn op_timing_safe_equal(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use subtle::ConstantTimeEq;
    let (Some(left), Some(right)) = (view_bytes(args.get(0)), view_bytes(args.get(1))) else {
        return;
    };
    rv.set(v8::Boolean::new(scope, bool::from(left.ct_eq(&right))).into());
}

pub(super) fn op_webcrypto_hmac_verify(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    use hmac::Mac;
    let algorithm = args.get(0).to_rust_string_lossy(scope).to_ascii_uppercase();
    let Some(key) = view_bytes(args.get(1)) else {
        return;
    };
    let Some(signature) = view_bytes(args.get(2)) else {
        return;
    };
    let Some(data) = view_bytes(args.get(3)) else {
        return;
    };
    macro_rules! verify {
        ($digest:ty) => {{
            let Ok(mut mac) = <hmac::Hmac<$digest> as hmac::Mac>::new_from_slice(&key) else {
                return;
            };
            mac.update(&data);
            mac.verify_slice(&signature).is_ok()
        }};
    }
    let valid = dispatch_hmac_digest!(algorithm.as_str(), verify);
    rv.set(v8::Boolean::new(scope, valid).into());
}

/// AES-GCM for the Web Crypto ops. The key, IV, and tag sizes are instantiated
/// explicitly because the crate fixes all three in the type.
///
/// Web Crypto permits any IV length; 96 bits is the recommendation, not the
/// rule, so 128-bit IVs are accepted too. For anything but 96 bits the crate
/// derives J0 by GHASHing the IV, per NIST SP 800-38D. An empty IV is
/// rejected in the JS layer, by name, before reaching here.
fn webcrypto_aes_gcm(
    key: &[u8],
    iv: &[u8],
    additional_data: &[u8],
    data: &[u8],
    tag_size: usize,
    encrypting: bool,
) -> Option<Vec<u8>> {
    use aes_gcm::aead::consts::{U12, U13, U14, U15, U16};
    use aes_gcm::aead::{Aead, KeyInit, Payload};
    macro_rules! run {
        ($aes:ty, $nonce_size:ty, $tag_size:ty) => {{
            type Gcm = aes_gcm::AesGcm<$aes, $nonce_size, $tag_size>;
            let cipher = <Gcm as KeyInit>::new_from_slice(key).ok()?;
            let nonce = aes_gcm::Nonce::<$nonce_size>::from_slice(iv);
            let payload = Payload {
                msg: data,
                aad: additional_data,
            };
            if encrypting {
                cipher.encrypt(nonce, payload)
            } else {
                cipher.decrypt(nonce, payload)
            }
            .ok()?
        }};
    }
    macro_rules! by_tag {
        ($aes:ty, $nonce_size:ty) => {
            match tag_size {
                12 => run!($aes, $nonce_size, U12),
                13 => run!($aes, $nonce_size, U13),
                14 => run!($aes, $nonce_size, U14),
                15 => run!($aes, $nonce_size, U15),
                16 => run!($aes, $nonce_size, U16),
                _ => return None,
            }
        };
    }
    macro_rules! by_iv {
        ($aes:ty) => {
            match iv.len() {
                12 => by_tag!($aes, U12),
                16 => by_tag!($aes, U16),
                _ => return None,
            }
        };
    }
    Some(match key.len() {
        16 => by_iv!(aes::Aes128),
        32 => by_iv!(aes::Aes256),
        _ => return None,
    })
}

/// The AES modes the ops accept. The set is closed here, so a mode name is
/// checked once on the way in and every later `match` is exhaustive. The
/// earlier shape compared the name against a string literal at each branch,
/// which let the three places that knew the set drift apart.
#[derive(Clone, Copy)]
enum AesMode {
    Gcm,
    /// GCM reports a failure by returning nothing and the block modes report
    /// a cause, so the two answer to different callers and cannot share one
    /// branch.
    Block(AesBlockMode),
}

#[derive(Clone, Copy)]
enum AesBlockMode {
    Cbc,
    Ctr,
}

impl AesMode {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "AES-GCM" => Self::Gcm,
            "AES-CBC" => Self::Block(AesBlockMode::Cbc),
            "AES-CTR" => Self::Block(AesBlockMode::Ctr),
            _ => return None,
        })
    }
}

/// AES-CBC with PKCS#7 padding, and AES-CTR. Both take 128- or 256-bit keys,
/// and CTR is its own inverse, so `encrypting` only matters for CBC.
///
/// The counter block belongs to the caller, and incrementing it in place is
/// an easy and invisible mistake. This function cannot make it: `iv` is a
/// copy the op already took out of V8, so the caller's block is not reachable
/// from here.
///
/// `mode` cannot name AES-GCM, because `AesBlockMode` does not hold it. The
/// GCM path therefore cannot arrive here by mistake, and this function needs
/// no arm to refuse it.
fn webcrypto_aes_block_mode(
    mode: AesBlockMode,
    key: &[u8],
    iv: &[u8],
    data: &[u8],
    encrypting: bool,
) -> Result<Vec<u8>> {
    if let AesBlockMode::Ctr = mode {
        use ctr::cipher::{KeyIvInit, StreamCipher};
        if iv.len() != 16 {
            return Err(anyhow!("AES-CTR requires a 16-byte counter block"));
        }
        let mut out = data.to_vec();
        macro_rules! run {
            ($aes:ty) => {{
                let mut cipher = ctr::Ctr128BE::<$aes>::new_from_slices(key, iv)
                    .map_err(|_| anyhow!("invalid AES-CTR key or counter"))?;
                cipher.apply_keystream(&mut out);
            }};
        }
        match key.len() {
            16 => run!(aes::Aes128),
            32 => run!(aes::Aes256),
            other => {
                return Err(anyhow!(
                    "AES-CTR requires a 128- or 256-bit key, got {other} bytes"
                ))
            }
        }
        return Ok(out);
    }
    use cbc::cipher::block_padding::Pkcs7;
    use cbc::cipher::{BlockDecryptMut, BlockEncryptMut, KeyIvInit};
    if iv.len() != 16 {
        return Err(anyhow!("AES-CBC requires a 16-byte IV"));
    }
    macro_rules! run {
        ($aes:ty) => {{
            if encrypting {
                cbc::Encryptor::<$aes>::new_from_slices(key, iv)
                    .map_err(|_| anyhow!("invalid AES-CBC key or IV"))?
                    .encrypt_padded_vec_mut::<Pkcs7>(data)
            } else {
                cbc::Decryptor::<$aes>::new_from_slices(key, iv)
                    .map_err(|_| anyhow!("invalid AES-CBC key or IV"))?
                    .decrypt_padded_vec_mut::<Pkcs7>(data)
                    .map_err(|_| anyhow!("AES-CBC decryption failed"))?
            }
        }};
    }
    match key.len() {
        16 => Ok(run!(aes::Aes128)),
        32 => Ok(run!(aes::Aes256)),
        other => Err(anyhow!(
            "AES-CBC requires a 128- or 256-bit key, got {other} bytes"
        )),
    }
}

/// `$$aesEncrypt(mode, key, iv, data, additionalData, tagBytes)` and its
/// decrypt counterpart return a Uint8Array. The last two arguments apply only
/// to AES-GCM, so one typed-array op pair still serves all three AES modes.
fn op_webcrypto_aes(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
    encrypting: bool,
) {
    let mode = args.get(0).to_rust_string_lossy(scope);
    let (Some(key), Some(iv), Some(data), Some(additional_data)) = (
        view_bytes(args.get(1)),
        view_bytes(args.get(2)),
        view_bytes(args.get(3)),
        view_bytes(args.get(4)),
    ) else {
        return;
    };
    let tag_size = args.get(5).uint32_value(scope).unwrap_or(16) as usize;
    let throw = |scope: &mut v8::PinScope, error: anyhow::Error| {
        let message = v8::String::new(scope, &format!("crypto: {error}")).unwrap();
        let exception = v8::Exception::error(scope, message);
        scope.throw_exception(exception);
    };
    let Some(parsed) = AesMode::parse(&mode) else {
        throw(scope, anyhow!("unsupported AES mode: {mode}"));
        return;
    };
    match parsed {
        // AES-GCM keeps reporting a failure by returning nothing, because
        // `crypto.js` turns that into the `OperationError` the Web Crypto
        // specification names. A host `Error` thrown from here would replace a
        // specified DOMException with an unspecified one. The block modes have
        // no such JS-side error, so they report the cause instead of returning
        // `undefined` for the JS layer to misread.
        AesMode::Gcm => {
            if let Some(out) =
                webcrypto_aes_gcm(&key, &iv, &additional_data, &data, tag_size, encrypting)
            {
                webcrypto_return_bytes(scope, rv, &out);
            }
        }
        AesMode::Block(block) => {
            match webcrypto_aes_block_mode(block, &key, &iv, &data, encrypting) {
                Ok(out) => webcrypto_return_bytes(scope, rv, &out),
                Err(error) => throw(scope, error),
            }
        }
    }
}

pub(super) fn op_webcrypto_aes_encrypt(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    op_webcrypto_aes(scope, args, rv, true);
}

pub(super) fn op_webcrypto_aes_decrypt(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    op_webcrypto_aes(scope, args, rv, false);
}
