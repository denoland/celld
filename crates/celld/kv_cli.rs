// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The KV CLI reads operator input and value files outside node storage.
#![allow(clippy::disallowed_methods)]
// The KV listing is data; everything else it says is for a person.

//! `celld kv` — read and write a deployed KV namespace.
//!
//! KV needs this command more than D1 does. A developer can reach a database
//! through `env.DB` from a Worker they already wrote, so `celld d1` adds
//! migrations and convenience. Inspecting a namespace, seeding one, or clearing
//! a bad key are operator tasks, and writing a throwaway Worker to do them is
//! not an answer anybody accepts. This command is the operator surface for KV.
//!
//! It reaches a namespace the way `celld d1` reaches a database, over the
//! machinery in [`crate::operator_cell`]: the node leases in the bucket find
//! the fleet, the fleet secret signs the request, and `/runtime/<scope>`
//! forwards to the owner.
//!
//! **The CLI implements no storage.** It transports a request and the cell
//! decides — the same rule that keeps `celld d1` free of SQLite. The size split
//! for a large value, the expiry arithmetic and the visibility filter all live
//! where the binding's do, so a key written here is a key the binding could
//! have written.

use crate::cli_options::FLEET_HELP;
use crate::cli_options::LISTING_HELP;
use crate::cli_output::list;
use crate::cli_output::Bounds;
use crate::cli_output::Format;
use crate::cli_output::Output;
use crate::cli_output::Page;
use crate::cli_output::Record;
use crate::cli_output::Resumable;
use crate::cli_output::Resume;
use crate::note;
use std::borrow::Cow;

/// One key in a namespace listing.
struct Key {
    name: String,
    expiration: Option<i64>,
    metadata: Option<Value>,
}

/// Convert the cell's storage deadline to the Wrangler field's unit.
///
/// The cell compares millisecond deadlines with `Date.now()`, but every
/// operator input and output named `expiration` uses Unix seconds. Keeping the
/// conversion at the serialization boundary prevents an exported deadline
/// from becoming one thousand times larger when it is used in another put.
fn expiration_seconds(entry: &Value) -> Option<i64> {
    entry
        .get("expiration")
        .and_then(Value::as_i64)
        .map(|milliseconds| milliseconds / 1000)
}

impl Record for Key {
    fn json(&self) -> Value {
        json!({
            "name": self.name,
            "expiration": self.expiration,
            // The namespace stores metadata as a JSON *string*. Passing it
            // through verbatim double-encodes it, so `jq .metadata.seeded`
            // reads a string and answers null.
            "metadata": self.metadata.as_ref().map(|raw| match raw {
                Value::String(text) => {
                    serde_json::from_str(text).unwrap_or_else(|_| raw.clone())
                }
                other => other.clone(),
            }),
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }
}

impl Resumable for Key {
    fn cursor(&self) -> &str {
        &self.name
    }
}

/// A namespace's totals.
struct Totals {
    live: i64,
    bytes: i64,
    stored: i64,
}

impl Record for Totals {
    fn json(&self) -> Value {
        json!({ "keys": self.live, "bytes": self.bytes, "stored": self.stored })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "keys:    {}\nbytes:   {}\nstored:  {}",
            self.live, self.bytes, self.stored
        ))
    }
}
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use serde_json::{json, Value};

use crate::operator_cell::{Fleet, Reachable, Subject};

trait NamespaceCall {
    async fn call(&self, body: Value) -> anyhow::Result<Value>;
}

impl NamespaceCall for Reachable {
    async fn call(&self, body: Value) -> anyhow::Result<Value> {
        Reachable::call(self, body).await
    }
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    // Which shard holds this operation. A keyed operation asks the hash; a
    // namespace-wide one takes shard zero, and becomes a fan-out when there is
    // more than one shard to fan out over. The hash is asked rather than
    // assumed even though `SHARDS` is one today, so raising the count changes
    // a constant and not this call site.
    let shard = match command.action.key() {
        Some(key) => celld_logic::kv::shard_of(key, celld_logic::kv::SHARDS),
        None => 0,
    };
    let scope = crate::js::kv_cell_scope(&command.namespace, shard);
    let storage = command.fleet.clone().resolve("celld kv")?;
    let namespace = Reachable::open(
        Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise: command.unsafe_public_advertise,
        },
        Subject {
            noun: "namespace",
            source: "kv",
            // A bulk seed is not a diagnostic ping, and a namespace read is not
            // a migration: between the two.
            timeout: Duration::from_secs(60),
        },
        scope,
        None,
    )
    .await?;

    match command.action {
        Action::Get { key } => {
            let result = namespace.call(json!({ "op": "get", "key": key })).await?;
            if result.get("found").and_then(Value::as_bool) != Some(true) {
                bail!("no key {key:?} in namespace {:?}", command.namespace);
            }
            let bytes = decode_bytes(&result)?;
            // Written to stdout as bytes, not as text. A namespace holds bytes,
            // and a value written from a Worker as an ArrayBuffer has no
            // faithful string form — so `celld kv get` piped to a file gives
            // back exactly what was stored.
            Output::new(Format::Text).bytes(&bytes)?;
        }
        Action::Put {
            key,
            value,
            file,
            expiration_ttl,
            expiration,
            metadata,
        } => {
            let bytes = match (value, file) {
                (Some(value), None) => value.into_bytes(),
                (None, Some(path)) => {
                    std::fs::read(&path).with_context(|| format!("read {}", path.display()))?
                }
                _ => unreachable!("parse enforces exactly one"),
            };
            // The wire uses the existing byte array for an inline value and
            // base64 above that threshold. This choice controls only the
            // transport. The cell still checks the key, value, metadata and
            // deadline, so validation cannot drift between two processes.
            let large = bytes.len() > celld_logic::kv::MAX_INLINE_VALUE_BYTES;
            namespace
                .call(json!({
                    // The operation name makes the new wire form fail closed
                    // on an older node. That node reports an unknown operation
                    // instead of interpreting a base64 string as an empty byte
                    // array and acknowledging corrupt data.
                    "op": if large { "put-base64" } else { "put" },
                    "key": key,
                    "value": if large {
                        Value::String(encode_base64(&bytes))
                    } else {
                        json!(bytes)
                    },
                    "tag": "bytes",
                    "metadata": metadata,
                    "expiration": expiration,
                    "expirationTtl": expiration_ttl,
                }))
                .await?;
            note!("wrote {key:?} ({} bytes)", bytes.len());
        }
        Action::Delete { keys } => {
            namespace
                .call(json!({ "op": "delete", "keys": keys }))
                .await?;
            note!("deleted {} key(s)", keys.len());
        }
        Action::List {
            prefix,
            bounds,
            json: as_json,
        } => {
            // Bounded like every other listing: a namespace can hold far
            // more keys than an operator wants to read, and each request
            // returns one page, so an unbounded default makes the cost
            // follow the namespace's size rather than the question.
            let mut out = Output::new(if as_json { Format::Json } else { Format::Text });
            let namespace = &namespace;
            let prefix = prefix.clone();
            let listed = list(&mut out, &bounds, |resume, want| {
                let prefix = prefix.clone();
                // A namespace resumes from the last key, and its cursor is
                // the key itself, so a store token adds nothing here.
                let after = match resume {
                    Resume::From(after) => after.unwrap_or_default(),
                    Resume::Token(token) => token,
                };
                async move {
                    let page = namespace
                        .call(json!({
                            "op": "list",
                            "prefix": prefix,
                            "limit": want.min(celld_logic::kv::list_limit(None)),
                            "after": after,
                        }))
                        .await?;
                    let keys = page
                        .get("keys")
                        .and_then(Value::as_array)
                        .ok_or_else(|| anyhow!("the namespace reply carried no keys: {page}"))?;
                    let rows: Vec<Key> = keys
                        .iter()
                        .map(|entry| Key {
                            name: entry
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_string(),
                            expiration: expiration_seconds(entry),
                            metadata: entry.get("metadata").cloned(),
                        })
                        .collect();
                    // The namespace reports completion rather than handing
                    // back a token, so the continuation is the last key.
                    let complete = page.get("complete").and_then(Value::as_bool) == Some(true);
                    let next = match rows.last() {
                        Some(last) if !complete => Some(last.name.clone()),
                        _ => None,
                    };
                    Ok(Page { rows, next })
                }
            })
            .await?;
            out.finish()?;
            listed.report("key", "--after");
        }
        Action::BulkPut { file } => {
            let source = std::fs::read_to_string(&file)
                .with_context(|| format!("read {}", file.display()))?;
            let entries: Vec<Value> = serde_json::from_str(&source)
                .with_context(|| format!("parse {} as a Wrangler bulk file", file.display()))?;
            // One request per entry, and the cell validates each. The binding's
            // 100-key ceiling is the *binding's*: a CLI that inherited it would
            // fail a ten-thousand-key import at key 101, which is a bad bug
            // report to receive for a migration path.
            let mut written = 0_usize;
            for entry in &entries {
                let key = entry
                    .get("key")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("a bulk entry has no `key`: {entry}"))?;
                let raw = entry
                    .get("value")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("bulk entry {key:?} has no `value`"))?;
                // Wrangler marks a value that is not text with `base64: true`,
                // and a namespace holds bytes, so the flag decides how the
                // string becomes them.
                let bytes = if entry.get("base64").and_then(Value::as_bool) == Some(true) {
                    decode_base64(raw)
                        .ok_or_else(|| anyhow!("bulk entry {key:?} has invalid base64"))?
                } else {
                    raw.as_bytes().to_vec()
                };
                let large = bytes.len() > celld_logic::kv::MAX_INLINE_VALUE_BYTES;
                namespace
                    .call(json!({
                        "op": if large { "put-base64" } else { "put" },
                        "key": key,
                        "value": if large {
                            Value::String(encode_base64(&bytes))
                        } else {
                            json!(bytes)
                        },
                        "tag": "bytes",
                        // Stringified once, the way the binding does, because
                        // the cell stores metadata as text.
                        "metadata": entry
                            .get("metadata")
                            .filter(|metadata| !metadata.is_null())
                            .map(ToString::to_string),
                        "expiration": entry.get("expiration"),
                        "expirationTtl": entry.get("expiration_ttl"),
                    }))
                    .await
                    .with_context(|| format!("write {key:?}"))?;
                written += 1;
            }
            note!("wrote {written} key(s)");
        }
        Action::BulkDelete { file } => {
            let source = std::fs::read_to_string(&file)
                .with_context(|| format!("read {}", file.display()))?;
            // Wrangler's delete file is an array of key names.
            let keys: Vec<String> = serde_json::from_str(&source)
                .with_context(|| format!("parse {} as a key list", file.display()))?;
            for chunk in keys.chunks(celld_logic::kv::MAX_BULK_KEYS) {
                namespace
                    .call(json!({ "op": "delete", "keys": chunk }))
                    .await?;
            }
            note!("deleted {} key(s)", keys.len());
        }
        Action::BulkGet { file } => {
            match file {
                Some(path) => bulk_export_file(&namespace, &path).await?,
                None => {
                    // A failed stdout export stays an incomplete JSON array,
                    // so a consumer cannot mistake a prefix for a complete
                    // migration. A named file has a stronger atomic contract
                    // in `bulk_export_file` because it has a stable pathname.
                    let stdout = std::io::stdout();
                    let mut writer = std::io::BufWriter::new(stdout.lock());
                    if let Err(error) = bulk_export(&namespace, &mut writer).await {
                        if error
                            .downcast_ref::<std::io::Error>()
                            .is_some_and(|error| error.kind() == std::io::ErrorKind::BrokenPipe)
                        {
                            return Ok(());
                        }
                        return Err(error);
                    }
                }
            }
        }
        Action::Info => {
            let result = namespace.call(json!({ "op": "info" })).await?;
            let number = |name: &str| result.get(name).and_then(Value::as_i64).unwrap_or(-1);
            // `stored` above `live` is the population the sweeper still owes.
            // An operator seeing a large gap is looking at reclamation that has
            // not run, not at keys an application can still read.
            let mut out = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            out.row(&Totals {
                live: number("live"),
                bytes: number("bytes"),
                stored: number("stored"),
            })?;
            out.finish()?;
        }
    }
    Ok(())
}

async fn bulk_export(
    namespace: &impl NamespaceCall,
    writer: &mut impl std::io::Write,
) -> anyhow::Result<()> {
    // The whole namespace, in Wrangler's bulk-put shape, so the output of this
    // command is the input of `celld kv bulk put`.
    let mut after = String::new();
    let mut wrote_row = false;
    writer.write_all(b"[\n").context("write the export")?;
    loop {
        let page = namespace
            .call(json!({
                "op": "list",
                "prefix": "",
                "limit": celld_logic::kv::list_limit(None),
                "after": after,
            }))
            .await?;
        let keys = page
            .get("keys")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("the namespace reply carried no keys: {page}"))?;
        if keys.is_empty() {
            break;
        }
        for entry in keys {
            let name = entry
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            after = name.to_string();
            let found = namespace.call(json!({ "op": "get", "key": name })).await?;
            if found.get("found").and_then(Value::as_bool) != Some(true) {
                // Expired between the listing and the read. Skipping is right:
                // the export is what a reader would see.
                continue;
            }
            let bytes = decode_bytes(&found)?;
            let mut row = json!({
                "key": name,
                "value": encode_base64(&bytes),
                "base64": true
            });
            // The cell stores metadata as the JSON text the binding wrote, and
            // Wrangler's file carries it as an object. Emit the object: writing
            // the text would make a reimport encode it a second time.
            if let Some(text) = found.get("metadata").and_then(Value::as_str) {
                row["metadata"] = serde_json::from_str::<Value>(text)
                    .unwrap_or_else(|_| Value::String(text.to_string()));
            }
            if let Some(expiration) = expiration_seconds(&found) {
                row["expiration"] = Value::from(expiration);
            }
            let encoded = serde_json::to_vec(&row).context("encode an export row")?;
            if wrote_row {
                writer.write_all(b",\n").context("write the export")?;
            }
            writer.write_all(&encoded).context("write the export")?;
            writer.flush().context("write the export")?;
            wrote_row = true;
        }
        if page.get("complete").and_then(Value::as_bool) == Some(true) {
            break;
        }
    }
    writer.write_all(b"\n]\n").context("write the export")?;
    writer.flush().context("write the export")
}

async fn bulk_export_file(
    namespace: &impl NamespaceCall,
    path: &std::path::Path,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let mut temporary = tempfile::Builder::new()
        .prefix(".celld-kv-export-")
        .tempfile_in(parent)
        .with_context(|| format!("create a temporary export beside {}", path.display()))?;
    {
        let mut writer = std::io::BufWriter::new(temporary.as_file_mut());
        bulk_export(namespace, &mut writer).await?;
    }
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("install the complete export at {}", path.display()))?;
    Ok(())
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Wrangler marks a non-text bulk value with `base64: true`, so an export this
/// command writes and an export `wrangler kv bulk get` writes are the same
/// shape. Written out rather than taken as a dependency: the engine has no
/// base64 crate, and this is the only caller.
fn encode_base64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for shift in [18, 12, 6, 0] {
            out.push(B64[((n >> shift) & 0x3f) as usize] as char);
        }
    }
    // Pad to the original length rather than the padded triple.
    let pad = (3 - bytes.len() % 3) % 3;
    out.truncate(out.len() - pad);
    out.push_str(&"=".repeat(pad));
    out
}

fn decode_base64(text: &str) -> Option<Vec<u8>> {
    let mut bits = 0_u32;
    let mut have = 0_u32;
    let mut digits = 0_usize;
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    for byte in text.bytes() {
        if byte == b'=' || byte == b'\n' || byte == b'\r' {
            continue;
        }
        let value = B64.iter().position(|candidate| *candidate == byte)? as u32;
        bits = (bits << 6) | value;
        have += 6;
        digits += 1;
        if have >= 8 {
            have -= 8;
            out.push((bits >> have) as u8);
        }
    }
    // A single leftover digit carries six bits, which is not a byte and never
    // a valid encoding. Without this a truncated value decodes to a *shorter*
    // value instead of failing, so a corrupt export would import quietly and
    // the loss would surface as a wrong answer much later.
    if digits % 4 == 1 {
        return None;
    }
    Some(out)
}

fn decode_bytes(result: &Value) -> anyhow::Result<Vec<u8>> {
    if result.get("valueEncoding").and_then(Value::as_str) == Some("base64") {
        return result
            .get("value")
            .and_then(Value::as_str)
            .and_then(decode_base64)
            .ok_or_else(|| anyhow!("the namespace reply carried invalid base64"));
    }
    result
        .get("value")
        .and_then(Value::as_array)
        .map(|bytes| {
            bytes
                .iter()
                .filter_map(|byte| byte.as_u64().map(|byte| byte as u8))
                .collect()
        })
        .ok_or_else(|| anyhow!("the namespace reply carried no value: {result}"))
}

impl Action {
    /// The key this operation addresses, when it addresses one.
    ///
    /// `list` and `info` span a namespace rather than a key, so with more than
    /// one shard they become a fan-out and a merge; `delete` takes several keys
    /// and would split by shard. Both are why this returns an `Option` rather
    /// than a key: the shape that needs answering later is visible now.
    fn key(&self) -> Option<&str> {
        match self {
            Self::Get { key } | Self::Put { key, .. } => Some(key),
            Self::Delete { keys } => keys.first().map(String::as_str),
            Self::List { .. } | Self::Info => None,
            // A bulk operation spans a namespace, so with more than one shard
            // it splits by key rather than addressing one cell.
            Self::BulkGet { .. } | Self::BulkPut { .. } | Self::BulkDelete { .. } => None,
        }
    }
}

#[cfg(all(test, celld_internal_tests))]
mod kv_cli_contract {
    include!(env!("CELLD_INTERNAL_KV_CLI_TESTS"));
}

enum Action {
    Get {
        key: String,
    },
    Put {
        key: String,
        /// Exactly one of the two, enforced at parse.
        value: Option<String>,
        file: Option<PathBuf>,
        expiration_ttl: Option<i64>,
        expiration: Option<i64>,
        metadata: Option<String>,
    },
    Delete {
        keys: Vec<String>,
    },
    List {
        prefix: String,
        bounds: Bounds,
        json: bool,
    },
    Info,
    /// `bulk get` and `bulk put` move a whole namespace, which is what makes
    /// `wrangler kv bulk get` -> `celld kv bulk put` a migration path off
    /// Cloudflare. The file format is Wrangler's: an array of
    /// `{key, value, expiration?, expiration_ttl?, metadata?, base64?}`.
    BulkGet {
        file: Option<PathBuf>,
    },
    BulkPut {
        file: PathBuf,
    },
    BulkDelete {
        file: PathBuf,
    },
}

struct Command {
    action: Action,
    namespace: String,
    fleet: crate::cli_options::FleetFlags,
    json: bool,
    unsafe_public_advertise: bool,
}

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter().peekable();
        let Some(first) = arguments.next() else {
            return Ok(None);
        };
        let verb = match first.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            verb @ ("get" | "put" | "delete" | "list" | "info") => verb.to_string(),
            // `bulk get|put|delete`, mirroring `wrangler kv bulk`.
            "bulk" => match arguments.next().as_deref() {
                Some("get") => "bulk-get".to_string(),
                Some("put") => "bulk-put".to_string(),
                Some("delete") => "bulk-delete".to_string(),
                // An option in the subcommand slot means the subcommand is
                // missing, not that the option names one.
                Some(other) if !other.starts_with('-') => {
                    bail!("unknown `celld kv bulk` subcommand: {other}")
                }
                _ => bail!("`celld kv bulk` needs `get`, `put`, or `delete`"),
            },
            other => bail!("unknown `celld kv` subcommand: {other}"),
        };
        // The namespace is the `id` from `kv_namespaces`, verbatim. Upstream's
        // configuration carries no human-readable name beside it, so there is
        // nothing else to resolve and celld invents no second key.
        let namespace = arguments
            .next()
            .filter(|value| !value.starts_with('-'))
            .ok_or_else(|| anyhow!("celld kv needs a namespace id"))?;

        let mut positional = Vec::new();
        let mut file = None;
        let mut expiration_ttl = None;
        let mut expiration = None;
        let mut metadata = None;
        let mut prefix = String::new();
        let mut fleet = crate::cli_options::FleetFlags::default();
        let mut bounds = Bounds::default();
        let mut json = false;
        let mut unsafe_public_advertise = false;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            let number = |flag: &str, raw: String| {
                raw.parse::<i64>()
                    .with_context(|| format!("{flag} takes a number, got {raw:?}"))
            };
            match argument.as_str() {
                "--path" => file = Some(PathBuf::from(value("--path")?)),
                "--ttl" | "--expiration-ttl" => {
                    let raw = value("--ttl")?;
                    expiration_ttl = Some(number("--ttl", raw)?);
                }
                "--expiration" => {
                    let raw = value("--expiration")?;
                    expiration = Some(number("--expiration", raw)?);
                }
                "--metadata" => metadata = Some(value("--metadata")?),
                "--prefix" => prefix = value("--prefix")?,
                "--json" => json = true,
                "--unsafe-public-advertise" => unsafe_public_advertise = true,
                // celld has neither concept, and a flag that silently means
                // nothing is the gap the compatibility page forbids.
                // `--local` and `--remote` select between miniflare's on-disk
                // state and the account API; `--preview` selects a second
                // namespace under `wrangler dev`.
                flag @ ("--local" | "--remote" | "--preview") => bail!(
                    "celld kv does not take {flag}: celld runs neither a local \
                     miniflare store nor a Cloudflare account, and it has no \
                     preview namespace. A namespace id addresses one namespace \
                     in one fleet."
                ),
                "--help" | "-h" => return Ok(None),
                other => {
                    // The shared flags first, so `--bucket`, `--limit` and
                    // their siblings mean here exactly what they mean in
                    // every other command.
                    if fleet.consume(other, &mut value)? || bounds.consume(other, &mut value)? {
                        continue;
                    }
                    if other.starts_with('-') {
                        bail!("unknown option: {other}; run `celld kv --help` for usage")
                    }
                    positional.push(other.to_string());
                }
            }
        }
        // A flag the verb does not read must be a refusal. Accepting `--ttl` on
        // a `get` would tell an operator they set an expiry that nothing wrote.
        let refuse = |flag: &str, set: bool| -> anyhow::Result<()> {
            if set {
                bail!("`celld kv {verb}` takes no {flag}");
            }
            Ok(())
        };
        let action = match verb.as_str() {
            "get" => {
                refuse("--path", file.is_some())?;
                refuse("--ttl", expiration_ttl.is_some())?;
                refuse("--metadata", metadata.is_some())?;
                let [key] = positional.as_slice() else {
                    bail!("celld kv get needs exactly one key");
                };
                Action::Get { key: key.clone() }
            }
            "put" => {
                let key = positional
                    .first()
                    .ok_or_else(|| anyhow!("celld kv put needs a key"))?
                    .clone();
                let inline = positional.get(1).cloned();
                if inline.is_some() == file.is_some() {
                    bail!("celld kv put requires exactly one of a value or --path");
                }
                if positional.len() > 2 {
                    bail!("celld kv put takes one key and at most one value");
                }
                Action::Put {
                    key,
                    value: inline,
                    file,
                    expiration_ttl,
                    expiration,
                    metadata,
                }
            }
            "delete" => {
                refuse("--path", file.is_some())?;
                if positional.is_empty() {
                    bail!("celld kv delete needs at least one key");
                }
                Action::Delete { keys: positional }
            }
            "list" => {
                refuse("--path", file.is_some())?;
                if !positional.is_empty() {
                    bail!("celld kv list takes no positional arguments; use --prefix");
                }
                Action::List {
                    prefix,
                    bounds: bounds.clone(),
                    json,
                }
            }
            "info" => {
                if !positional.is_empty() {
                    bail!("celld kv info takes no positional arguments");
                }
                Action::Info
            }
            "bulk-get" => Action::BulkGet {
                file: file.or_else(|| positional.first().map(PathBuf::from)),
            },
            "bulk-put" | "bulk-delete" => {
                let path = file
                    .or_else(|| positional.first().map(PathBuf::from))
                    .ok_or_else(|| anyhow!("celld kv {verb} needs a file"))?;
                if verb == "bulk-put" {
                    Action::BulkPut { file: path }
                } else {
                    Action::BulkDelete { file: path }
                }
            }
            _ => unreachable!("the verb was matched above"),
        };
        bounds.validate()?;
        Ok(Some(Self {
            action,
            namespace,
            // Resolution is deferred to `run`, which also unifies the region
            // default. This command used to fall back to "auto" while every
            // other command and the documented default used "us-east-1".
            fleet: fleet.with_environment(),
            json,
            unsafe_public_advertise,
        }))
    }
}

pub fn print_help() {
    let text = format!(
        "celld kv — read and write a deployed KV namespace

USAGE
  celld kv get    <namespace-id> <key>          [fleet options]
  celld kv put    <namespace-id> <key> <value>  [put options] [fleet options]
  celld kv delete <namespace-id> <key>...       [fleet options]
  celld kv list   <namespace-id>                [--prefix P] [listing options]
  celld kv info   <namespace-id>                [fleet options]
  celld kv bulk get    <namespace-id> [FILE]    [fleet options]
  celld kv bulk put    <namespace-id> FILE      [fleet options]
  celld kv bulk delete <namespace-id> FILE      [fleet options]

The namespace id is the `id` from the project's `kv_namespaces` entry,
verbatim. `get` writes the value to stdout as bytes, so a value stored as
bytes comes back byte for byte.

PUT OPTIONS
  --path FILE           read the value from a file instead of the command line
  --ttl SECONDS         expire this many seconds from now (at least 60)
  --expiration SECONDS  expire at this absolute unix time
  --metadata JSON       store metadata beside the value (at most 1024 bytes)

LISTING OPTIONS (list)
  --prefix P          list only the keys under this prefix
{LISTING_HELP}

FLEET OPTIONS
{FLEET_HELP}
  --unsafe-public-advertise
                      Trust a node advertising a public address

BULK
  The file format is Wrangler's, so `wrangler kv bulk get` writes a file this
  command reads: an array of {{key, value, expiration?, expiration_ttl?,
  metadata?, base64?}}. `bulk get` writes that shape, to FILE or to stdout, so
  an export here imports there too. `bulk delete` takes an array of key names.

  A bulk import is chunked beneath the binding's 100-key ceiling rather than
  refused at it, because that ceiling belongs to `env.KV` and not to a
  migration.

`info` reports live keys, their bytes, and stored rows. Stored above live is
the expired population the sweeper has not reclaimed yet; those keys are
already invisible to a read."
    );
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text).help(&text);
}
