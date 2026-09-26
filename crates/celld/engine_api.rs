// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Engine-neutral types and helpers that the cell runtimes and the host
//! share. Nothing here touches V8; `crate::js`, `crate::runtime` and
//! `crate::pool` re-export every item at its old path.
use crate::ws_registry::WorkerWebSocket;
use anyhow::Result;
use celld_logic::isolate::Refusal;
use std::cell::RefCell;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;

/// Bare Node builtin specifiers that the bundler leaves for the runtime.
/// Root entries also match subpaths in esbuild and in module resolution.
pub(crate) const BARE_NODE_BUILTINS: &[&str] = &[
    "assert",
    "async_hooks",
    "buffer",
    "child_process",
    "cluster",
    "constants",
    "crypto",
    "dgram",
    "diagnostics_channel",
    "dns",
    "events",
    "fs",
    "fs/promises",
    "http",
    "http2",
    "https",
    "inspector",
    "module",
    "net",
    "os",
    "path",
    "perf_hooks",
    "process",
    "punycode",
    "querystring",
    "readline",
    "sqlite",
    "stream",
    "string_decoder",
    "timers",
    "tls",
    "tty",
    "url",
    "util",
    "util/types",
    "v8",
    "vm",
    "worker_threads",
    "zlib",
];

pub type RequestId = u128;

/// Allocate an id for an ingress or service-binding request so it can be
/// aborted mid-flight.
pub fn next_request_id() -> RequestId {
    next_do_request_id()
}

static NEXT_DO_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static DO_REQUEST_PROCESS_PREFIX: OnceLock<u64> = OnceLock::new();

#[doc(hidden)]
pub fn next_do_request_id() -> RequestId {
    let prefix = *DO_REQUEST_PROCESS_PREFIX.get_or_init(|| {
        let mut bytes = [0; 8];
        getrandom::fill(&mut bytes).expect("OS random source unavailable");
        u64::from_ne_bytes(bytes)
    });
    let sequence = NEXT_DO_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    (u128::from(prefix) << 64) | u128::from(sequence)
}

pub fn request_id_string(request_id: RequestId) -> String {
    format!("{request_id:032x}")
}

pub fn parse_request_id(value: &str) -> Option<RequestId> {
    u128::from_str_radix(value, 16).ok()
}

/// An RPC payload crossing the host boundary. JS stubs marshal by V8
/// structured clone (`V8`), and legacy callers use the JSON envelope (`Json`).
/// `__dispatchRpc` answers in the flavor it was asked in.
pub enum RpcData {
    Json(String),
    V8(bytes::Bytes),
}

pub type HttpChunkStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<Vec<u8>, String>> + Send>>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WsTarget {
    pub id: u64,
    pub scope: String,
    /// A parked tunneled 101 on the calling node (`peer_tunnel::splice`).
    /// The id is meaningful only in the process that parked it, but it must
    /// survive the isolate round trip, so it serializes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunnel: Option<u64>,
}

pub enum HttpResponseWebSocket {
    Cell(WsTarget),
    Worker(WorkerWebSocket),
}

pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    /// A response body forwarded without materializing it in memory.
    pub stream: Option<HttpChunkStream>,
    pub headers: Vec<(String, String)>,
    /// The one WebSocket target that owns this response, if it is an upgrade.
    pub websocket: Option<HttpResponseWebSocket>,
    /// The cell's committed-write position after the handler ran, for a local
    /// Durable Object request. The shell gates the response on durability when
    /// this advanced past the cell's last seen position. `None` for responses
    /// with no cell storage (Worker, asset, or proxied remote).
    pub write_position: Option<u64>,
    /// The position the answer observed above the cell's published baseline
    /// when the handler did not write, so the shell can hold a read-only
    /// response behind the proof of another handler's commit. `None` when the
    /// cell holds no handler write, or the response has no cell storage.
    pub observed_position: Option<u64>,
}

/// The encoding that the queue producer selected for one message body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueContentType {
    Text,
    Bytes,
    Json,
    V8,
}

impl QueueContentType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Bytes => "bytes",
            Self::Json => "json",
            Self::V8 => "v8",
        }
    }
}

/// One message handed from a queue cell to a consumer isolate.
pub struct QueueMessage {
    pub id: String,
    pub timestamp_ms: i64,
    pub body: Vec<u8>,
    pub content_type: QueueContentType,
    pub attempts: u16,
}

/// The queue state observed when a cell leases a batch.
pub struct QueueMetrics {
    pub backlog_count: f64,
    pub backlog_bytes: f64,
    pub oldest_message_timestamp_ms: Option<i64>,
}

/// One leased batch handed to the stateless isolate pool.
pub struct QueueBatch {
    pub queue: String,
    pub messages: Vec<QueueMessage>,
    pub metrics: QueueMetrics,
}

/// The batch-wide retry decision made by a queue handler.
#[derive(Debug, Eq, PartialEq)]
pub struct QueueRetryBatch {
    pub retry: bool,
    pub delay_seconds: Option<i32>,
}

/// An explicit retry decision made for one message.
#[derive(Debug, Eq, PartialEq)]
pub struct QueueRetryMessage {
    pub msg_id: String,
    pub delay_seconds: Option<i32>,
}

/// How the queue handler itself completed. Infrastructure failures still use
/// the outer `Result`, so a handler exception can preserve earlier acks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueOutcome {
    Ok,
    Exception,
}

/// The handler outcome and the settlement instructions it made before return.
#[derive(Debug, Eq, PartialEq)]
pub struct QueueDispatchResult {
    pub outcome: QueueOutcome,
    pub error: Option<String>,
    pub ack_all: bool,
    pub retry_batch: QueueRetryBatch,
    pub explicit_acks: Vec<String>,
    pub retry_messages: Vec<QueueRetryMessage>,
}

/// A cell event that the runtime refuses before application code starts.
///
/// The type survives the Rust RPC path. Its display text also survives the V8
/// promise boundary, so the public ingress can restore the HTTP overload
/// contract after a Worker forwards the refusal.
#[doc(hidden)]
#[derive(Debug)]
pub struct CellOverloaded;

/// The V8 promise boundary preserves only an error string. Keep the public
/// overload phrase so a Worker can classify a caught Queue producer refusal.
/// Also include an opaque marker so unrelated application text cannot restore
/// an HTTP overload response, and keep the producer and ingress checks on one
/// value.
#[doc(hidden)]
pub const CELL_OVERLOAD_ERROR_MARKER: &str =
    "celld-internal-cell-overload-7ec38c64-12d7-4ddc-9e77-b63f9dc14130";

impl std::fmt::Display for CellOverloaded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "cell overload: admission refused ({CELL_OVERLOAD_ERROR_MARKER})"
        )
    }
}

impl std::error::Error for CellOverloaded {}

/// Per-worker compatibility switches, derived from the manifest's
/// compatibility date and flags (Workerd compatibility-date.capnp). `Default`
/// is every switch off; production derives real values in `main`.
#[derive(Clone, Copy, Default)]
pub struct Compat {
    pub delete_all_deletes_alarm: bool,
    /// `js_rpc`: RPC on a Durable Object class that does not extend
    /// `DurableObject` (Workerd worker-rpc.c++ getTargetInfo()).
    pub js_rpc: bool,
    /// `fetcher_has_get_put_delete` (off = `fetcher_no_get_put_delete`,
    /// default on for dates >= 2024-03-26): the deprecated `get()`/`put()`/
    /// `delete()` HTTP helpers on stubs (Workerd http.c++ Fetcher).
    pub fetcher_get_put_delete: bool,
    /// `sqlite_vec`: expose the pre-v1 sqlite-vec extension to SQLite-backed
    /// Durable Objects. This switch is explicit and has no date default.
    pub sqlite_vec: bool,
    /// `websocket_standard_binary_type`: `binaryType` defaults to `"blob"` and
    /// a binary message arrives as a `Blob`, per the WHATWG default. Without
    /// it celld keeps the historical `"arraybuffer"`.
    pub websocket_standard_binary_type: bool,
    /// Queue bodies default to JSON on compatibility dates after 2024-03-18;
    /// older deployments retain the V8 structured-clone default.
    pub queue_json_messages: bool,
}

/// A non-main module the worker's main module may import, tagged by how the
/// runtime materializes it.
#[derive(Clone)]
pub enum ModuleSource {
    /// UTF-8 content served as `export default "<content>"` (wrangler's Text
    /// rule), registered under the given specifier verbatim.
    Text(String),
    /// JS source compiled as a sibling ES module (Worker Loader multi-module
    /// bundles), registered under both `name` and `./name`.
    EsModule(String),
    /// Wasm bytes served as a module whose default export is the compiled
    /// `WebAssembly.Module` (Wrangler's `CompiledWasm` rule), registered
    /// under both `name` and `./name`.
    Wasm(bytes::Bytes),
}

/// A Workflow binding keeps the three names distinct across deployment
/// loading, environment construction, and runtime class injection.
pub struct WorkflowBinding {
    pub environment: String,
    pub workflow: String,
    pub class: String,
}

/// One Queue producer binding in a Worker environment.
#[derive(Clone)]
pub struct QueueBinding {
    pub environment: String,
    pub queue: String,
    pub delivery_delay: u32,
}

pub struct WorkerConfigOptions {
    pub src: String,
    pub script_name: String,
    pub do_classes: Vec<String>,
    pub bindings: Vec<(String, String)>,
    pub r2_bindings: Vec<(String, String)>,
    pub d1_bindings: Vec<(String, String)>,
    pub kv_bindings: Vec<(String, String)>,
    pub queue_bindings: Vec<QueueBinding>,
    pub queue_consumers: Vec<crate::protocol::QueueConsumerConfig>,
    pub workflow_bindings: Vec<WorkflowBinding>,
    pub vars: Vec<(String, String)>,
    pub node: String,
    pub modules: Vec<(String, ModuleSource)>,
    pub compat: Compat,
}

/// A handler that failed inside its turn, with the positions that turn
/// sampled from the cell.
///
/// A commit the handler made before it failed is as real as one a successful
/// handler made: it stays in the local database, unproven, and a read-only
/// output that followed would trail no barrier and reveal it while a crash can
/// still lose it. The error answer therefore takes the same write ticket a
/// success takes.
///
/// A handler that committed nothing still reveals what it read, because the
/// message it throws with can quote it — "insufficient funds: balance is 90"
/// is an ordinary shape — and that value can be another event's commit that no
/// proof covers yet. An error answer reveals cell state exactly as a 200 does,
/// so a read-only failure carries the observed position and the gate holds it
/// behind the newest barrier, as it holds a read-only success.
///
/// The failure the handler reported is the source; this wrapper displays its
/// message and continues its chain, so a client and a log see the handler's
/// own words.
#[derive(Debug)]
pub struct FailedInTurn {
    pub(crate) write_position: Option<u64>,
    pub(crate) observed_position: Option<u64>,
    source: anyhow::Error,
}

impl std::fmt::Display for FailedInTurn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl std::error::Error for FailedInTurn {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // The wrapped error's own message is this wrapper's display, so the
        // chain continues below it rather than repeating it. The wrapped
        // error's own type is therefore not in the chain: a `downcast_ref`
        // for it, or a `chain()` walk that looks for it, sees this wrapper
        // instead. Every error that reaches `fail_in_turn_error` today is a
        // plain message, so nothing looks; a typed handler failure would have
        // to be matched through `failed_write_position` and its text.
        let source: &(dyn std::error::Error + 'static) = self.source.as_ref();
        source.source()
    }
}

/// Attach the positions a failing handler's turn sampled, when it has either.
/// The pair comes from one `gate_positions` call, so an error cannot report a
/// write and an observation that disagree.
pub(crate) fn fail_in_turn_error(
    error: anyhow::Error,
    positions: (Option<u64>, Option<u64>),
) -> anyhow::Error {
    let (write_position, observed_position) = positions;
    if write_position.is_none() && observed_position.is_none() {
        return error;
    }
    anyhow::Error::new(FailedInTurn {
        write_position,
        observed_position,
        source: error,
    })
}

/// The position a failed handler committed before it failed, when it did.
pub fn failed_write_position(error: &anyhow::Error) -> Option<u64> {
    error
        .downcast_ref::<FailedInTurn>()
        .and_then(|failed| failed.write_position)
}

/// How an incoming request body reaches the isolate.
///
/// A small body crosses as bytes. This costs one copy and no asynchronous
/// operations, so a common request pays nothing for a stream that it does
/// not need. A large body, or a body of unknown length, crosses as a
/// stream id. The peak cost of that body is one chunk, not its length.
pub enum RequestBody {
    Bytes(bytes::Bytes),
    Stream(u64),
}

impl RequestBody {
    /// The bytes already in hand, for the paths that hold a whole body.
    pub fn bytes(&self) -> &[u8] {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Stream(_) => &[],
        }
    }

    pub fn stream_id(&self) -> Option<u64> {
        match self {
            Self::Bytes(_) => None,
            Self::Stream(id) => Some(*id),
        }
    }

    pub(crate) fn into_held_bytes(self) -> Option<Vec<u8>> {
        match self {
            Self::Bytes(bytes) => Some(bytes.into()),
            Self::Stream(_) => None,
        }
    }
}

impl From<Vec<u8>> for RequestBody {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Bytes(bytes.into())
    }
}

thread_local! {
    static DO_ID_KEYS: RefCell<HashMap<String, [u8; 32]>> = RefCell::new(HashMap::new());
}

pub(crate) fn durable_object_id_key(namespace_key: &str) -> [u8; 32] {
    DO_ID_KEYS.with(|keys| {
        if let Some(key) = keys.borrow().get(namespace_key) {
            return *key;
        }
        use sha2::Digest;
        let key: [u8; 32] = sha2::Sha256::digest(namespace_key.as_bytes()).into();
        keys.borrow_mut().insert(namespace_key.to_string(), key);
        key
    })
}

pub(crate) fn durable_object_id_hmac(key: &[u8; 32], input: &[u8]) -> hmac::Hmac<sha2::Sha256> {
    use hmac::Mac;
    let mut mac = <hmac::Hmac<sha2::Sha256> as hmac::Mac>::new_from_slice(key)
        .expect("SHA-256 accepts any HMAC key");
    mac.update(input);
    mac
}

/// Ids already derived on this thread, keyed by namespace and name.
///
/// `getByName` costs two HMAC-SHA256 rounds, and a profile of `/c/hello`
/// found them among the largest terms the stateless path does not have —
/// the same name resolving to the same id, recomputed on every request.
///
/// Per thread and unsynchronised, which is sound because the derivation is
/// a pure function of its inputs: a worker that has not seen a name pays
/// for it once, and no answer can differ between threads. Bounded because
/// names come from the application, and cleared wholesale at the cap rather
/// than evicted one at a time — a cache this cheap to refill does not earn
/// an LRU.
const DO_ID_CACHE_MAX: usize = 4096;

pub(crate) fn durable_object_id_for_name(namespace_key: &str, name: &str) -> [u8; 32] {
    use hmac::Mac;

    thread_local! {
        static IDS: RefCell<HashMap<(String, String), [u8; 32]>> =
            RefCell::new(HashMap::new());
    }
    let cached = IDS.with(|ids| {
        ids.borrow()
            .get(&(namespace_key.to_string(), name.to_string()))
            .copied()
    });
    if let Some(id) = cached {
        return id;
    }

    let key = durable_object_id_key(namespace_key);
    let mut id = [0_u8; 32];
    let digest = durable_object_id_hmac(&key, name.as_bytes())
        .finalize()
        .into_bytes();
    id[..16].copy_from_slice(&digest[..16]);
    let digest = durable_object_id_hmac(&key, &id[..16])
        .finalize()
        .into_bytes();
    id[16..].copy_from_slice(&digest[..16]);

    IDS.with(|ids| {
        let mut ids = ids.borrow_mut();
        if ids.len() >= DO_ID_CACHE_MAX {
            ids.clear();
        }
        ids.insert((namespace_key.to_string(), name.to_string()), id);
    });
    id
}

/// The key a Durable Object namespace derives its IDs from. D1 uses one
/// fleet-wide namespace because the database is a resource that several
/// Workers can bind, and a Worker rename must not rename that database.
pub(crate) const D1_NAMESPACE_KEY: &str = "cells:v1:d1:__D1Database";

/// The same, for a KV namespace, and for the same reason: several Workers can
/// bind one namespace, and they must reach one set of cells.
///
/// Written out rather than derived from the class name, because these two
/// strings are addresses. A scheme that computed them would be free to change,
/// and changing one renames every cell it ever addressed.
pub(crate) const KV_NAMESPACE_KEY: &str = "cells:v1:kv:__KvNamespace";

/// The fleet-wide namespace for Queue broker cells. The queue name is the
/// durable resource identity, so a producer and a consumer in different
/// scripts must derive the same cell id.
pub(crate) const QUEUE_NAMESPACE_KEY: &str = "cells:v1:queue:__Queue";

/// A shared reserved class addresses one set of cells for the whole fleet; every
/// other class, reserved or not, is scoped to the script that exports it.
///
/// The question is asked once, through `deploy::is_shared_reserved_class`, and
/// not as a chain of `==` against class names. A reserved class declared shared
/// there and script-scoped here would silently give each script its own copy of
/// a resource the configuration says they share -- which is what happened to KV
/// between its manifest landing and this line being written.
fn shared_namespace_key(class_name: &str) -> Option<&'static str> {
    match class_name {
        crate::deploy::D1_CLASS => Some(D1_NAMESPACE_KEY),
        crate::deploy::KV_CLASS => Some(KV_NAMESPACE_KEY),
        crate::deploy::QUEUE_CLASS => Some(QUEUE_NAMESPACE_KEY),
        _ => {
            debug_assert!(
                !crate::deploy::is_shared_reserved_class(class_name),
                "a shared reserved class needs a fleet-wide namespace key: {class_name}"
            );
            None
        }
    }
}

pub(crate) fn namespace_key(script_name: &str, class_name: &str) -> String {
    match shared_namespace_key(class_name) {
        Some(shared) => shared.to_string(),
        None => format!("cells:v1:{}:{script_name}:{class_name}", script_name.len()),
    }
}

/// The cell scope a D1 database lives at, for a caller outside any isolate.
/// `celld d1` addresses a database over the operator route, which takes a
/// scope, so this derives what `getByName` derives in the harness, from the
/// same key and the same HMAC.
pub fn d1_cell_scope(database_identity: &str) -> String {
    let id = durable_object_id_for_name(D1_NAMESPACE_KEY, database_identity);
    format!("{}:{}", crate::deploy::D1_CLASS, durable_object_id_hex(&id))
}

/// The cell scope one shard of a KV namespace lives at, for a caller outside
/// any isolate. `celld kv` addresses a namespace over the operator route, and
/// this derives what `getByName` derives in the harness -- from the same key,
/// the same name, and the same HMAC.
///
/// The name comes from `celld_logic::kv::cell_name`, which is also what the
/// binding is handed at `build_env`. Neither side formats it, because a
/// formatting disagreement here does not fail: it silently addresses a second,
/// empty namespace.
pub fn kv_cell_scope(namespace_id: &str, shard: u32) -> String {
    let name = celld_logic::kv::cell_name(namespace_id, shard);
    let id = durable_object_id_for_name(KV_NAMESPACE_KEY, &name);
    format!("{}:{}", crate::deploy::KV_CLASS, durable_object_id_hex(&id))
}

/// The Queue broker scope for callers outside a Worker isolate.
pub fn queue_cell_scope(queue: &str) -> String {
    let name = celld_logic::queue::cell_name(queue);
    let id = durable_object_id_for_name(QUEUE_NAMESPACE_KEY, name);
    format!(
        "{}:{}",
        crate::deploy::QUEUE_CLASS,
        durable_object_id_hex(&id)
    )
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn workflow_cell_scope_for_test(
    script_name: &str,
    workflow_name: &str,
    instance_id: &str,
) -> String {
    let class = crate::deploy::workflow_class(script_name);
    let namespace = namespace_key(script_name, &class);
    let name = format!("{workflow_name}/{instance_id}");
    let id = durable_object_id_for_name(&namespace, &name);
    format!("{class}:{}", durable_object_id_hex(&id))
}

pub(crate) fn durable_object_id_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

pub(crate) fn decode_durable_object_id(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let nibble = |byte| match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        };
        output[index] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(output)
}
pub type AlarmObserver = Arc<dyn Fn(String, celld_logic::wake::AlarmSnapshot) + Send + Sync>;

#[derive(Clone)]
pub(crate) enum StopMode {
    /// Prove the final position, then optionally retain a local base.
    ///
    /// `abandon` travels with `preserve_local` because an eviction that could
    /// be abandoned and an eviction that keeps a local base are the same
    /// eviction: the one a live node can change its mind about. A drain and a
    /// fence carry neither.
    Evict {
        preserve_local: bool,
        abandon: Option<Arc<crate::replication::EvictionAbandon>>,
    },
    /// Prove the final position and retain it for the successor epoch.
    Rebase,
    /// Close without a remote write because the caller lacks authority.
    CloseInPlace,
    /// Remove an image whose durability proof failed.
    Discard,
}
/// One pool's isolates by state, as `/state` reports them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct PoolCensus {
    /// Isolates that accept placement and work.
    pub live: usize,
    /// Live isolates that house no cell. The next maintenance pass retires
    /// them, so a count that persists across passes means the pass is not
    /// running.
    pub live_empty: usize,
    /// Retiring isolates whose heap is still installed, because a turn, a
    /// request, or a cell holds it.
    pub retiring: usize,
    /// Slots whose heap has been freed.
    pub freed: usize,
    /// Cells housed across the pool.
    pub cells: usize,
    /// Request affiliations across the pool, running or suspended.
    pub requests: usize,
    /// Turns in flight across the pool.
    pub turns: usize,
    /// Physical memory V8 has committed to the heaps of the isolates a turn
    /// did not hold at the sample.
    pub heap_bytes: u64,
    /// External memory those isolates track: array buffer stores and the
    /// like, which live outside the V8 heap.
    pub external_bytes: u64,
}

/// Refused by policy, or the isolate could not be built. Distinct because the
/// first is a 503 the caller may retry elsewhere and the second is a fault.
#[derive(Debug)]
pub enum AdmitError {
    Refused(Refusal),
    Build(anyhow::Error),
}

impl From<Refusal> for AdmitError {
    fn from(refusal: Refusal) -> Self {
        AdmitError::Refused(refusal)
    }
}

impl std::fmt::Display for AdmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdmitError::Refused(Refusal::NodeFull) => {
                write!(f, "node is at its stateless request limit")
            }
            AdmitError::Refused(Refusal::NodePressured) => write!(f, "node is shedding load"),
            AdmitError::Build(error) => write!(f, "isolate could not be started: {error}"),
        }
    }
}

impl std::error::Error for AdmitError {}

/// Node-level inputs to the cell runtime: everything that outlives an
/// application generation. What a deployment implies goes through
/// `Generation::build` instead.
pub struct RuntimeOptions {
    pub data_dir: std::path::PathBuf,
    pub replication: Option<crate::ltx_replication::Replication>,
    pub wake: Option<Arc<crate::wake::WakeFlusher>>,
    pub alarm_observer: AlarmObserver,
    pub node: String,
    pub region: String,
    /// The fleet bucket, where container images live. `None` for a local
    /// script, which has no bucket and loads images from the engine alone.
    pub bucket: Option<crate::bucket::Bucket>,
}

/// A service-binding fetch crossing from a calling isolate into the router.
pub struct ServiceFetch {
    /// The caller's application generation; the target is resolved in its
    /// deployment graph.
    pub generation: crate::generation::GenerationId,
    pub script: String,
    pub entrypoint: Option<crate::WorkerFetchEntrypoint>,
    pub url: String,
    pub method: String,
    pub body: RequestBody,
    pub headers: Vec<(String, String)>,
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
}

/// Owned HTTP request crossing from the async shell into a cell executor.
pub struct RuntimeFetch {
    pub url: String,
    pub method: String,
    pub body: RequestBody,
    pub headers: Vec<(String, String)>,
    pub request_id: Option<RequestId>,
    /// Where this call sits in its caller's order for this cell.
    pub order: Option<crate::js::CallOrder>,
    /// The dispatching Worker's trace context, so the cell's span joins
    /// the caller's trace instead of rooting a disconnected one.
    pub parent: Option<crate::telemetry::TraceContext>,
}

/// What a Durable Object RPC method returned, plus the position its writes
/// reached, so the caller can hold the reply behind durability.
pub struct RpcOutcome {
    pub data: RpcData,
    pub write_position: Option<u64>,
    /// As on `HttpResponse`: what a read-only reply observed above the cell's
    /// published baseline.
    pub observed_position: Option<u64>,
}

/// An answer the shell gates: a success reports the position its writes
/// reached, and the position it observed when it wrote nothing, in its own
/// shape.
pub trait GatedAnswer {
    fn write_position(&self) -> Option<u64>;
    fn observed_position(&self) -> Option<u64>;
}

impl GatedAnswer for HttpResponse {
    fn write_position(&self) -> Option<u64> {
        self.write_position
    }

    fn observed_position(&self) -> Option<u64> {
        self.observed_position
    }
}

impl GatedAnswer for RpcOutcome {
    fn write_position(&self) -> Option<u64> {
        self.write_position
    }

    fn observed_position(&self) -> Option<u64> {
        self.observed_position
    }
}

/// The ticket an answer takes through the output gate, or `None` when it
/// takes none.
///
/// A success always takes one: a write ticket when the handler advanced the
/// position, and otherwise a read-only ticket that carries what the answer
/// observed and trails the newest barrier on the cell. A failure raised inside
/// the handler's turn takes the same two shapes, which [`FailedInTurn`]
/// reports, because an error message can carry the cell's state as a body can.
/// A failure raised outside a turn — a budget overrun, a handler waiting on
/// nothing — took no sample and reports the host's own words, so it reveals
/// nothing and takes no ticket. The shell reads both arms through this one
/// function so that no answer site can gate one and forget the other.
pub fn answer_ticket<T: GatedAnswer>(result: &Result<T>) -> Option<crate::actor::GateTicket> {
    match result {
        Ok(answer) => Some(crate::actor::GateTicket::response(
            answer.write_position(),
            answer.observed_position(),
        )),
        Err(error) => error.downcast_ref::<FailedInTurn>().map(|failed| {
            crate::actor::GateTicket::response(failed.write_position, failed.observed_position)
        }),
    }
}

/// Fail the next release of a generation swap, for the black-box matrix.
///
/// A count rather than a latch, so a test can prove the retry reaches
/// success instead of only proving that it never proceeds. `debug_assertions`
/// is the gate `CELLD_TEST_CELL_STARTUP_FAILURE` beside it already uses: the
/// runtime matrix drives the debug binary, which no internal test cfg
/// reaches, and a release build compiles neither.
#[cfg(debug_assertions)]
pub(crate) fn injected_swap_release_failure() -> Option<anyhow::Error> {
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering;

    static REMAINING: std::sync::OnceLock<AtomicI64> = std::sync::OnceLock::new();
    let remaining = REMAINING.get_or_init(|| {
        AtomicI64::new(
            std::env::var("CELLD_TEST_SWAP_RELEASE_FAILURES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
        )
    });
    (remaining.fetch_sub(1, Ordering::Relaxed) > 0)
        .then(|| anyhow::anyhow!("injected generation swap release failure"))
}

/// The replication stream of a facet: nested under its root's own
/// coordinates, `<root>/facets/<h>` locally and `cells/<root>/facets/<h>/`
/// in the bucket, one level per name of the facet's path, so no scan of
/// cells sees a facet and deleting a facet with everything below it is
/// deleting one prefix. `<h>` is the first 16 bytes of the name's SHA-256
/// in hex: a name can be any string of up to 256 bytes, and the charset a
/// path segment allows is narrower. Workerd's own server names a facet
/// file by a small id from a per-object index; a hash needs no index to
/// replicate beside the root.
pub(crate) fn facet_cell(root: &str, names: &[String]) -> String {
    use sha2::Digest;
    let mut cell = root.to_string();
    for name in names {
        let digest = sha2::Sha256::digest(name.as_bytes());
        cell.push_str("/facets/");
        for byte in &digest[..16] {
            cell.push_str(&format!("{byte:02x}"));
        }
    }
    cell
}

/// How long a stateless request may wait for a free `max_requests` slot
/// before it is refused. Zero restores the old refuse-at-once behaviour.
/// The default is one second: long enough that a saturated node converts
/// its refusal storm into kernel-buffer queueing, short enough that a
/// caller learns the truth before it matters.
pub fn admission_wait() -> std::time::Duration {
    // Not `env_usize`, which filters zero out — zero is meaningful here.
    let ms = crate::env_vars::with_default("CELLD_ADMISSION_WAIT_MS", 1000u64)
        .expect("validated CELLD_ADMISSION_WAIT_MS");
    std::time::Duration::from_millis(ms)
}

/// The JavaScript handler budget, `CELLD_HANDLER_BUDGET_S`.
pub fn handler_budget() -> std::time::Duration {
    static BUDGET: OnceLock<std::time::Duration> = OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::time::Duration::from_secs(
            crate::env_vars::positive_or("CELLD_HANDLER_BUDGET_S", 300)
                .expect("validated CELLD_HANDLER_BUDGET_S"),
        )
    })
}
