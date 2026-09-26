// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The WebSocket registry: the sockets an instance holds, the frames waiting
//! to leave, and the isolate-polled input queues. Engine-neutral; the V8 ops
//! stay in `crate::js::websocket`, which re-exports every item at its old
//! path.
use crate::asyncrt;
use crate::engine_api::WsTarget;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct OutboundWsReq {
    pub scope: String,
    pub id: u64,
    pub url: String,
    pub protocols: Vec<String>,
    /// Present for an isolate-polled (Worker) socket. Created and registered
    /// on the JS thread before the request is sent, so `__ws_next` can never
    /// run ahead of its own queue.
    pub pull: Option<WsPullSender>,
    /// Extra request headers, for the `fetch()` upgrade form.
    pub headers: Vec<(String, String)>,
    /// A `fetch()` upgrade wants the whole handshake outcome, including the
    /// ordinary response a server that declines to upgrade sent instead.
    pub want_response: bool,
    /// A socket already upgraded in this process, which this request joins
    /// instead of dialing `url`. It is the cell end of a Durable Object
    /// subrequest whose caller kept the client end, so there is no handshake
    /// to run and no connection to open: the host only has to carry frames
    /// between two isolates.
    pub target: Option<WsTarget>,
    pub reply: tokio::sync::oneshot::Sender<Result<OutboundWsOpen>>,
}

/// The ordinary HTTP response a server sent instead of upgrading. `fetch()`
/// returns it verbatim rather than turning it into a connection error.
pub struct DeclinedUpgrade {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// What an outbound handshake produced.
pub struct OutboundWsOpen {
    pub protocol: Option<String>,
    pub declined: Option<DeclinedUpgrade>,
}

/// Outbound WebSocket traffic from a DO's `ws.send`/`ws.close`. The host holds
/// the socket in a task decoupled from the isolate (so the cell can hibernate
/// while the socket lives); `ws.send` routes here by wsId.
pub enum WsOut {
    Text(String),
    Binary(Vec<u8>),
    Close(u16, String),
}

/// The result of delivering one `webSocketMessage`. `frames` are the outbound
/// frames of the turn the handler answered in, captured by the output gate; a
/// turn the handler suspended in released its own frames as it ended, so those
/// are already gone. `write_position` is the cell's committed-write count
/// after the handler when it advanced past where it stood before — i.e. the
/// handler wrote, and its frames must be held until that position is durable.
/// `None` means no write: flush the frames.
pub struct WsDispatch {
    pub frames: Vec<(u64, WsOut)>,
    pub write_position: Option<u64>,
    /// As on `HttpResponse`: what a handler that wrote nothing observed above
    /// the cell's published baseline, for the gate to hold its frames behind
    /// the proof of another handler's commit.
    pub observed_position: Option<u64>,
}

/// One inbound event on a socket the ISOLATE polls, rather than one the host
/// pushes into a cell.
///
/// A Durable Object socket must survive between events and wake a hibernated
/// cell, so its frames arrive as `CellJob`s. A Worker socket cannot work that
/// way: the stateless pool has no addressable isolate to push into. That is
/// not a limitation to route around — it is exactly the lifetime Cloudflare
/// gives a Worker socket, which lives and dies with its `IoContext`, and
/// `IoContext::close_sockets` is what enforces it here. So the isolate pulls,
/// the same way it already pulls a streamed response body.
pub enum WsPull {
    Open(String),
    Text(String),
    Binary(Vec<u8>),
    Close(u16, String, bool),
}

/// Maximum charged bytes for non-terminal frames in one isolate-polled
/// WebSocket input queue.
pub(crate) const WS_PULL_QUEUE_MAX_BYTES: usize = 1024 * 1024;

/// Tags for the byte frame `__ws_next` resolves with. A tagged buffer keeps a
/// binary message on its fast path instead of base64 through a JSON envelope.
const WS_PULL_TAG_TEXT: u8 = 0;
const WS_PULL_TAG_BINARY: u8 = 1;
const WS_PULL_TAG_OPEN: u8 = 2;
const WS_PULL_TAG_CLOSE: u8 = 3;

impl WsPull {
    pub(crate) fn encode(self) -> Vec<u8> {
        let (tag, mut body) = match self {
            WsPull::Text(text) => (WS_PULL_TAG_TEXT, text.into_bytes()),
            WsPull::Binary(bytes) => (WS_PULL_TAG_BINARY, bytes),
            WsPull::Open(protocol) => (WS_PULL_TAG_OPEN, protocol.into_bytes()),
            WsPull::Close(code, reason, was_clean) => (
                WS_PULL_TAG_CLOSE,
                serde_json::json!({
                    "code": code,
                    "reason": reason,
                    "wasClean": was_clean,
                })
                .to_string()
                .into_bytes(),
            ),
        };
        let mut framed = Vec::with_capacity(body.len() + 1);
        framed.push(tag);
        framed.append(&mut body);
        framed
    }
}

struct WsPullCharge {
    #[cfg(all(test, celld_internal_tests))]
    bytes: usize,
    #[cfg(all(test, celld_internal_tests))]
    queued_bytes: Arc<std::sync::atomic::AtomicUsize>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[cfg(all(test, celld_internal_tests))]
impl Drop for WsPullCharge {
    fn drop(&mut self) {
        self.queued_bytes
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

struct QueuedWsPull {
    frame: WsPull,
    // Capacity and each non-terminal frame move through the channel together.
    // Every path that consumes or drops that frame therefore returns its queue
    // budget. One terminal frame has no permit because it must release a full
    // queue, but the test observer still measures its retained bytes.
    _charge: WsPullCharge,
}

struct WsPullSendState {
    tx: tokio::sync::mpsc::UnboundedSender<QueuedWsPull>,
    terminal_sent: bool,
}

#[derive(Clone)]
pub struct WsPullSender {
    state: Arc<std::sync::Mutex<WsPullSendState>>,
    #[cfg(all(test, celld_internal_tests))]
    queued_bytes: Arc<std::sync::atomic::AtomicUsize>,
    capacity: Arc<tokio::sync::Semaphore>,
}

pub struct WsPullReceiver {
    rx: tokio::sync::mpsc::UnboundedReceiver<QueuedWsPull>,
    #[cfg(all(test, celld_internal_tests))]
    queued_bytes: Arc<std::sync::atomic::AtomicUsize>,
}

pub fn ws_pull_channel() -> (WsPullSender, WsPullReceiver) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    #[cfg(all(test, celld_internal_tests))]
    let queued_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let capacity = Arc::new(tokio::sync::Semaphore::new(WS_PULL_QUEUE_MAX_BYTES));
    (
        WsPullSender {
            state: Arc::new(std::sync::Mutex::new(WsPullSendState {
                tx,
                terminal_sent: false,
            })),
            #[cfg(all(test, celld_internal_tests))]
            queued_bytes: queued_bytes.clone(),
            capacity,
        },
        WsPullReceiver {
            rx,
            #[cfg(all(test, celld_internal_tests))]
            queued_bytes,
        },
    )
}

impl WsPull {
    fn queue_bytes(&self) -> usize {
        let payload = match self {
            Self::Open(protocol) | Self::Text(protocol) => protocol.len(),
            Self::Binary(bytes) => bytes.len(),
            Self::Close(_, reason, _) => reason.len(),
        };
        std::mem::size_of::<Self>().saturating_add(payload)
    }
}

impl WsPullSender {
    pub fn is_closed(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.terminal_sent || state.tx.is_closed()
    }

    pub async fn send(&self, frame: WsPull) -> Result<(), WsPull> {
        let frame = match frame {
            WsPull::Close(code, reason, was_clean) => {
                return self.send_close(code, reason, was_clean);
            }
            frame => frame,
        };
        let bytes = frame.queue_bytes();
        // One message can exceed this queue limit until the separate incoming
        // message limit rejects it. Reserving the complete budget admits that
        // message only into an empty queue and prevents a second message from
        // increasing its retained peak.
        let permit_bytes = bytes.min(WS_PULL_QUEUE_MAX_BYTES) as u32;
        let permit = match self.capacity.clone().acquire_many_owned(permit_bytes).await {
            Ok(permit) => permit,
            Err(_) => return Err(frame),
        };
        // The terminal sender closes the semaphore before it queues the close.
        // A data sender can already own a permit at that point, so this lock
        // either orders that data before the close or rejects it after the
        // close. Without the shared order, a data frame could enter behind the
        // only terminal frame and keep the receiver open.
        let state = self.state.lock().unwrap();
        if state.terminal_sent {
            return Err(frame);
        }
        #[cfg(all(test, celld_internal_tests))]
        self.queued_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        state
            .tx
            .send(QueuedWsPull {
                frame,
                _charge: WsPullCharge {
                    #[cfg(all(test, celld_internal_tests))]
                    bytes,
                    #[cfg(all(test, celld_internal_tests))]
                    queued_bytes: self.queued_bytes.clone(),
                    _permit: Some(permit),
                },
            })
            .map_err(|error| error.0.frame)
    }

    /// Queue the one terminal event without waiting for data capacity.
    ///
    /// The shared state orders the close against a data sender that already
    /// owns capacity. Closing the semaphore wakes every sender that still waits
    /// for capacity, so a stopped isolate cannot keep a host socket task alive.
    pub fn send_close(&self, code: u16, reason: String, was_clean: bool) -> Result<(), WsPull> {
        let frame = WsPull::Close(code, reason, was_clean);
        #[cfg(all(test, celld_internal_tests))]
        let bytes = frame.queue_bytes();
        let mut state = self.state.lock().unwrap();
        if state.terminal_sent {
            return Err(frame);
        }
        state.terminal_sent = true;
        self.capacity.close();
        #[cfg(all(test, celld_internal_tests))]
        self.queued_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        state
            .tx
            .send(QueuedWsPull {
                frame,
                _charge: WsPullCharge {
                    #[cfg(all(test, celld_internal_tests))]
                    bytes,
                    #[cfg(all(test, celld_internal_tests))]
                    queued_bytes: self.queued_bytes.clone(),
                    _permit: None,
                },
            })
            .map_err(|error| error.0.frame)
    }
}

impl WsPullReceiver {
    pub(crate) async fn recv(&mut self) -> Option<WsPull> {
        let queued = self.rx.recv().await?;
        let QueuedWsPull { frame, _charge } = queued;
        if matches!(&frame, WsPull::Close(..)) {
            // The sender state prevents a frame from entering behind this one.
            // Close the receiver as part of consuming the terminal event, so a
            // later pull observes the end instead of waiting for sender drops.
            self.rx.close();
        }
        drop(_charge);
        Some(frame)
    }

    #[cfg(all(test, celld_internal_tests))]
    fn queued_bytes(&self) -> usize {
        self.queued_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(all(test, celld_internal_tests))]
mod input_queue_private {
    include!(env!("CELLD_INTERNAL_WEBSOCKET_TESTS"));
}

/// One socket's inbound queue. Shared so an op can await it without holding
/// the registry lock; one isolate polls a given socket serially.
type WsPullQueue = Arc<tokio::sync::Mutex<WsPullReceiver>>;
type WsPullRegistry = std::sync::Mutex<HashMap<u64, WsPullQueue>>;

/// Inbound queues for isolate-polled sockets, keyed by wsId.
pub(crate) fn ws_pull() -> Arc<WsPullRegistry> {
    asyncrt::services().websockets().pull.clone()
}

pub fn ws_pull_register(id: u64, rx: WsPullReceiver) {
    ws_pull()
        .lock()
        .unwrap()
        .insert(id, Arc::new(tokio::sync::Mutex::new(rx)));
}

pub fn ws_pull_unregister(id: u64) {
    ws_pull().lock().unwrap().remove(&id);
}

/// The frame channel that a top-level Worker transfers with its 101 response.
///
/// The channel and the socket id move together. Dropping the response before
/// the HTTP upgrade, or ending the upgrade task, therefore removes every host
/// registration instead of leaving a socket that no isolate can reach.
pub struct WorkerWebSocket {
    pub(crate) id: u64,
    pub(crate) inbound: WsPullSender,
}

impl WorkerWebSocket {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn inbound(&self) -> WsPullSender {
        self.inbound.clone()
    }
}

impl Drop for WorkerWebSocket {
    fn drop(&mut self) {
        ws_pull_unregister(self.id);
        ws_unregister(self.id);
    }
}

/// Whether the host still holds an inbound queue for `id`.
///
/// A leak here is invisible from outside the process: the queue holds the
/// receiver whose survival keeps a connector task — and its TCP connection —
/// alive, and a request that dies before its socket is ever registered for
/// output leaves nothing else to observe.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn ws_pull_registered(id: u64) -> bool {
    ws_pull().lock().unwrap().contains_key(&id)
}

pub enum WsIn {
    Text(String),
    Binary(Vec<u8>),
}

impl From<WsIn> for WsPull {
    fn from(frame: WsIn) -> Self {
        match frame {
            WsIn::Text(text) => Self::Text(text),
            WsIn::Binary(bytes) => Self::Binary(bytes),
        }
    }
}

#[doc(hidden)]
pub struct WsMeta {
    pub scope: String,
    pub hibernatable: bool,
    pub tags: Vec<String>,
    /// Structured-clone bytes, not JSON: `serializeAttachment` accepts
    /// anything cloneable, so Date, Map and Set must survive a round trip.
    pub attachment: Option<Vec<u8>>,
    pub pending: Vec<WsOut>,
    /// When the shell last answered this socket with the cell's auto-response,
    /// unix ms. Lives here rather than in the isolate because the reply is
    /// sent while the cell may not be resident at all.
    pub auto_response_at: Option<f64>,
}
#[derive(Default)]
#[doc(hidden)]
pub struct WsRegistry {
    pub(crate) outputs: HashMap<u64, tokio::sync::mpsc::UnboundedSender<WsOut>>,
    pub metadata: HashMap<u64, WsMeta>,
    pub(crate) worker_handoffs: HashMap<u64, WsPullSender>,
}
impl WsRegistry {
    #[doc(hidden)]
    pub fn register(&mut self, id: u64, tx: tokio::sync::mpsc::UnboundedSender<WsOut>) {
        if let Some(meta) = self.metadata.get_mut(&id) {
            for pending in meta.pending.drain(..) {
                let _ = tx.send(pending);
            }
        }
        self.outputs.insert(id, tx);
    }

    fn unregister(&mut self, id: u64) -> Option<WsMeta> {
        self.outputs.remove(&id);
        self.metadata.remove(&id)
    }

    #[doc(hidden)]
    pub fn emit(&mut self, id: u64, out: WsOut) {
        if let Some(tx) = self.outputs.get(&id) {
            tracing::debug!(ws_id = id, "queued outbound WebSocket frame");
            let _ = tx.send(out);
        } else if let Some(meta) = self.metadata.get_mut(&id) {
            tracing::debug!(ws_id = id, "buffered pre-upgrade WebSocket frame");
            meta.pending.push(out);
        } else {
            // The socket is gone and the frame has nowhere to go. Silence here
            // is what made a held frame indistinguishable from a sent one.
            tracing::warn!(ws_id = id, "dropped a frame for a closed WebSocket");
        }
    }
}
/// Every piece of one instance's WebSocket state.
///
/// A socket's id is allocated by `next_id` here, so an id identifies a socket
/// only within one instance. Each map a socket reaches must therefore belong
/// to the same instance: a map that stayed process-wide would be shared by
/// the sockets that two instances both numbered, and a second instance is not
/// hypothetical: a test can run multiple instances, and a per-isolate or
/// per-generation instance in production would inherit the
/// collision.
pub(crate) struct WebSocketService {
    registry: Arc<std::sync::Mutex<WsRegistry>>,
    regular_counts: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    next_id: AtomicU64,
    auto_responses: Arc<std::sync::Mutex<HashMap<String, (String, String)>>>,
    pull: Arc<WsPullRegistry>,
    flush: Arc<WsFlushState>,
}

impl Default for WebSocketService {
    fn default() -> Self {
        Self {
            registry: Arc::new(std::sync::Mutex::new(WsRegistry::default())),
            regular_counts: Arc::new(std::sync::Mutex::new(HashMap::new())),
            next_id: AtomicU64::new(1),
            auto_responses: Arc::new(std::sync::Mutex::new(HashMap::new())),
            pull: Arc::new(std::sync::Mutex::new(HashMap::new())),
            flush: Arc::new(WsFlushState::default()),
        }
    }
}

pub(crate) fn ws_registry() -> Arc<std::sync::Mutex<WsRegistry>> {
    asyncrt::services().websockets().registry.clone()
}

fn regular_ws_counts() -> Arc<std::sync::Mutex<HashMap<String, usize>>> {
    asyncrt::services().websockets().regular_counts.clone()
}
pub(crate) fn increment_regular_ws(scope: &str) {
    *regular_ws_counts()
        .lock()
        .unwrap()
        .entry(scope.to_string())
        .or_default() += 1;
}
pub(crate) fn decrement_regular_ws(scope: &str) {
    let counts = regular_ws_counts();
    let mut counts = counts.lock().unwrap();
    let Some(count) = counts.get_mut(scope) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counts.remove(scope);
    }
}
pub fn has_regular_websocket(scope: &str) -> bool {
    regular_ws_counts()
        .lock()
        .unwrap()
        .get(scope)
        .is_some_and(|count| *count > 0)
}
/// The auto-response pair per cell scope, set by
/// `state.setWebSocketAutoResponse`. Shell state, like the socket registry:
/// the whole point of the feature is answering a matched message while the
/// cell is not resident, so the isolate cannot hold it.
pub(crate) fn ws_auto_responses() -> Arc<std::sync::Mutex<HashMap<String, (String, String)>>> {
    asyncrt::services().websockets().auto_responses.clone()
}

/// The shell's read path asks here before dispatching a text frame. A match
/// returns the response to send on the same socket and stamps the socket's
/// timestamp; the frame then never reaches the cell — no dispatch, no wake.
/// Only hibernatable sockets participate, as in workerd, where matching
/// lives in the hibernation manager's read loop.
pub fn ws_auto_response(scope: &str, id: u64, text: &str) -> Option<String> {
    let response = {
        let pairs = ws_auto_responses();
        let pairs = pairs.lock().unwrap();
        let (request, response) = pairs.get(scope)?;
        if request != text {
            return None;
        }
        response.clone()
    };
    let registry = ws_registry();
    let mut registry = registry.lock().unwrap();
    let meta = registry.metadata.get_mut(&id)?;
    if !meta.hibernatable {
        return None;
    }
    meta.auto_response_at = Some(unix_ms());
    Some(response)
}

fn unix_ms() -> f64 {
    asyncrt::wall_ms() as f64
}

pub fn ws_hibernatable(id: u64) -> Option<bool> {
    ws_registry()
        .lock()
        .unwrap()
        .metadata
        .get(&id)
        .map(|meta| meta.hibernatable)
}
pub fn ws_next_id() -> u64 {
    asyncrt::services()
        .websockets()
        .next_id
        .fetch_add(1, Ordering::Relaxed)
}
pub fn ws_register(id: u64, tx: tokio::sync::mpsc::UnboundedSender<WsOut>) {
    ws_registry().lock().unwrap().register(id, tx);
}

/// Install one hibernatable socket through a cfg-gated execution backend.
#[cfg(celld_internal_tests)]
pub(crate) fn ws_register_hibernatable_for_test(
    id: u64,
    scope: &str,
    tx: tokio::sync::mpsc::UnboundedSender<WsOut>,
) {
    let registry = ws_registry();
    let mut registry = registry.lock().unwrap();
    registry.register(id, tx);
    registry.metadata.insert(
        id,
        WsMeta {
            scope: scope.to_string(),
            hibernatable: true,
            tags: Vec::new(),
            attachment: None,
            pending: Vec::new(),
            auto_response_at: None,
        },
    );
}
pub fn ws_register_outbound(id: u64, scope: &str) {
    let inserted = {
        let registry = ws_registry();
        let mut registry = registry.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) = registry.metadata.entry(id) {
            entry.insert(WsMeta {
                scope: scope.to_string(),
                hibernatable: false,
                tags: Vec::new(),
                attachment: None,
                pending: Vec::new(),
                auto_response_at: None,
            });
            true
        } else {
            false
        }
    };
    if inserted {
        increment_regular_ws(scope);
    }
}
pub fn ws_unregister(id: u64) {
    let meta = ws_registry().lock().unwrap().unregister(id);
    if let Some(meta) = meta.filter(|meta| !meta.hibernatable) {
        decrement_regular_ws(&meta.scope);
    }
}

/// Which channel a frame on this socket leaves by.
///
/// A hibernatable transport is `WsHibernatable`: the host pushes its messages
/// into the cell and captures what the handler emits. A turn the handler
/// suspends in releases its own batch on a ticket of its own, and the turn the
/// handler answers in releases its batch from the cell's barrier queue.
///
/// A socket the isolate opened and polls itself is `WsSelf`, and the two are
/// separate channels in the model for a concrete reason. That handler runs
/// inside the isolate's event loop, and that loop is what a captured frame
/// would be waiting on: the reply to a frame the gate is withholding never
/// arrives, so the loop never finishes, so the frame is never released. It
/// takes a durability ticket on the host runtime instead, which is a different
/// way of being held rather than not being held at all.
pub(crate) fn ws_channel(id: u64) -> celld_logic::Channel {
    let registry = ws_registry();
    let registry = registry.lock().unwrap();
    if registry
        .metadata
        .get(&id)
        .is_some_and(|meta| meta.hibernatable)
    {
        celld_logic::Channel::WsHibernatable
    } else {
        celld_logic::Channel::WsSelf
    }
}

/// The frames of one flush inside a socket's ordered queue.
///
/// A segment rather than a flat list of frames, because every flush holds a
/// gate ticket of its own. A later flush's frames can trail a write that an
/// earlier flush's ticket does not cover, so releasing a socket's whole queue
/// on the earlier verdict would send them before their own proof arrived. The
/// queue therefore drains its settled prefix and stops.
struct WsSegment {
    /// Names this segment to the flush that owns it. The position in the
    /// queue cannot: an earlier segment can drain first and move it.
    token: u64,
    frames: Vec<WsOut>,
    /// What the gate answered for this segment, once its flush has an answer.
    /// A segment with no owning flush carries `Ok` from the start: the frames
    /// are already released and only wait for the order in front of them.
    settled: Option<Result<(), String>>,
}

/// The frames one instance holds behind its output gates, and the flushes
/// that own them.
///
/// The fields are one invariant, so they are one value: a socket has a queue
/// exactly while the flushes counted in `flushes` will drain it, and a count
/// that falls must notify `done`. Held together, no caller can take the queue
/// of one instance of the services and the count of another.
#[derive(Default)]
pub(crate) struct WsFlushState {
    /// Frames held back because the handler that produced them has written
    /// something not yet durable, one queue of segments per socket.
    ///
    /// Every later frame for a socket that already holds frames joins the
    /// queue rather than overtaking it: a socket's frames must arrive in the
    /// order the script sent them. Ordering is a property of one socket,
    /// which is what the key says.
    ///
    /// Owned by the services, not by a thread. It is filled inside the
    /// isolate and drained by an op, and an op runs on the host runtime — so
    /// a thread-local queue was filled on one thread and taken, empty, on
    /// another. The frames never left the process, and because the queue
    /// stayed non-empty every later frame joined them.
    deferred: std::sync::Mutex<HashMap<u64, std::collections::VecDeque<WsSegment>>>,
    /// How many flushes still hold frames for a socket. A count, not a flag:
    /// one flush can finish and drain its segment while a later frame starts
    /// another.
    flushes: std::sync::Mutex<HashMap<u64, usize>>,
    /// Names the next segment. Monotonic within one instance's services, so
    /// a token identifies a segment for as long as any flush can name it.
    next_token: AtomicU64,
    done: tokio::sync::Notify,
}

impl WsFlushState {
    /// Send `frames` on their sockets, or hold each behind its socket's gate.
    ///
    /// One call is one flush: every socket the batch touches takes at most
    /// one segment, so a broadcast that one gate ticket covers costs one
    /// ticket rather than one for each client.
    ///
    /// `gated` is the core's answer for this batch's event. A socket that
    /// already holds frames joins them whatever that answer is, so a later
    /// frame cannot overtake an earlier one. An ungated batch therefore joins
    /// as a segment that is settled from the start, and a gated one joins as
    /// a segment this flush must release.
    ///
    /// Returns the guard for the flush this batch started, and only then, so
    /// the caller cannot leave a held segment with no flush behind it and
    /// cannot await a gate that holds nothing. The counts are taken while the
    /// queue lock is held: a teardown that reads them must not observe a
    /// queued frame with no flush behind it.
    pub(crate) fn emit_or_defer(
        self: &Arc<Self>,
        registry: &std::sync::Mutex<WsRegistry>,
        frames: impl IntoIterator<Item = (u64, WsOut)>,
        gated: bool,
    ) -> Option<WsFlushGuard> {
        // Held until every frame is either queued or sent. A flush runs on
        // another thread, so releasing between the check and the send would
        // let it drain its queue in between, putting a later frame ahead of
        // an earlier one.
        let mut deferred = self.deferred.lock().unwrap();
        let mut direct = Vec::new();
        let mut mine = std::collections::HashSet::new();
        let mut segments = Vec::new();
        for (id, out) in frames {
            if mine.contains(&id) {
                deferred
                    .get_mut(&id)
                    .and_then(std::collections::VecDeque::back_mut)
                    .expect("a socket this batch queued still holds its segment")
                    .frames
                    .push(out);
                continue;
            }
            let token = self.next_token.fetch_add(1, Ordering::Relaxed);
            match deferred.entry(id) {
                std::collections::hash_map::Entry::Vacant(_) if !gated => direct.push((id, out)),
                entry => {
                    entry.or_default().push_back(WsSegment {
                        token,
                        frames: vec![out],
                        settled: (!gated).then_some(Ok(())),
                    });
                    mine.insert(id);
                    if gated {
                        segments.push((id, token));
                    }
                }
            }
        }
        let flushing = (!segments.is_empty()).then(|| {
            let mut flushes = self.flushes.lock().unwrap();
            for (id, _) in &segments {
                *flushes.entry(*id).or_default() += 1;
            }
            drop(flushes);
            WsFlushGuard {
                state: self.clone(),
                segments,
            }
        });
        // Still under the queue lock: a frame this batch sends directly has
        // no queue on its socket, and a flush that acquired the lock first
        // cannot install one between the check above and this send.
        emit_frames(registry, direct);
        flushing
    }

    /// Wait until no flush holds frames for `id` any more.
    async fn await_flushes(&self, id: u64) {
        loop {
            // Registered before the count is read. A flush that finishes in
            // between must find a waiter to wake, or this parks forever on a
            // notification that already happened.
            let notified = self.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.flushes.lock().unwrap().contains_key(&id) {
                return;
            }
            notified.await;
        }
    }

    /// Wait until no flush of this instance still owns deferred frames.
    async fn await_all_flushes(&self) {
        loop {
            // Register before observing the map, exactly as the per-socket
            // wait does. The final guard can otherwise notify between the
            // empty check and waiter registration, leaving shutdown parked
            // forever.
            let notified = self.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.flushes.lock().unwrap().is_empty() {
                return;
            }
            notified.await;
        }
    }
}

/// The flush state of the services the caller runs under.
///
/// A flush task outlives the dispatch that spawned it and runs on a runtime
/// with no services of its own, so every caller that hands work to one
/// resolves the state here first and moves it in. Resolving inside the task
/// would answer from whichever instance that runtime reaches, which is the
/// process-wide sharing this state exists to end.
pub(crate) fn ws_flush_state() -> Arc<WsFlushState> {
    asyncrt::services().websockets().flush.clone()
}

/// One held segment and the flush that owns it, for a test that stands in for
/// the durability ticket. Holding the guard is what keeps the socket's
/// teardown wait honest while the queue exists.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub struct WsHeldFlush(WsFlushGuard);

/// Hold `out` behind `id`'s gate exactly as a gated frame does, and return
/// the flush that holds it.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn ws_defer_frame(id: u64, out: WsOut) -> WsHeldFlush {
    let flush = ws_flush_state();
    let registry = ws_registry();
    WsHeldFlush(
        flush
            .emit_or_defer(&registry, std::iter::once((id, out)), true)
            .expect("a gated frame always opens a segment of its own"),
    )
}

/// How many frames this instance holds for `id`.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn ws_deferred_count(id: u64) -> usize {
    ws_flush_state()
        .deferred
        .lock()
        .unwrap()
        .get(&id)
        .map_or(0, |queue| {
            queue.iter().map(|segment| segment.frames.len()).sum()
        })
}

/// Release a held flush as a settled durability ticket does, then count it
/// out. Consuming the guard is what makes the two happen together.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn ws_release_flush(held: WsHeldFlush) {
    let registry = ws_registry();
    held.0.release(&registry, Ok(()));
}

/// Answer a held flush as a refused durability ticket does.
#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn ws_refuse_flush(held: WsHeldFlush) {
    let registry = ws_registry();
    held.0
        .release(&registry, Err("refused for a test".to_string()));
}

/// The segments one flush holds, and the counts that name it to a teardown.
///
/// The two travel together because either alone is wrong: a segment with no
/// count lets a socket tear down over frames that have not left, and a count
/// with no segment parks that teardown forever.
///
/// The guard counts itself out on every exit path, including the ones that
/// run no code: a flush spawned onto a runtime that is already shutting down
/// is dropped unpolled, and a panic unwinds through it. It cannot help a
/// flush that is merely parked -- nothing drops, so nothing runs -- which is
/// what the teardown wait is bounded for.
///
/// It carries the state rather than resolving it on drop, because the drop
/// can run on a runtime that answers with another instance, or with none.
pub(crate) struct WsFlushGuard {
    state: Arc<WsFlushState>,
    /// The segment this flush owns on each socket the batch reached, as
    /// (socket, token).
    segments: Vec<(u64, u64)>,
}

impl WsFlushGuard {
    /// Give the gate's verdict to every segment this flush holds, then
    /// deliver what each socket can now release.
    ///
    /// `held` is what the gate answered. `Err` means the frames describe a
    /// write the fleet may never have, so they must not be delivered.
    /// Dropping them and leaving the socket open is not an option -- a
    /// WebSocket is an ordered stream, and a peer cannot see a hole in one.
    /// It would read the frames on either side of the gap as consecutive.
    ///
    /// Close instead. A truncated stream is something the peer can detect and
    /// resynchronise from; a silently incomplete one is not. The cell is
    /// reset underneath this as well, but that is a separate path and this
    /// must not depend on its timing.
    ///
    /// Takes the guard by value, so the count this flush holds falls once its
    /// frames have left and never before.
    pub(crate) fn release(self, registry: &std::sync::Mutex<WsRegistry>, held: Result<(), String>) {
        // Keep the lock from the verdict through the registry send. Once a
        // queue is drained away, another release would otherwise look direct
        // and overtake these frames.
        let mut deferred = self.state.deferred.lock().unwrap();
        let mut sending = Vec::new();
        for (id, token) in &self.segments {
            if let Some(segment) = deferred
                .get_mut(id)
                .and_then(|queue| queue.iter_mut().find(|segment| segment.token == *token))
            {
                segment.settled = Some(held.clone());
            }
            drain_settled(&mut deferred, *id, &mut sending);
        }
        emit_frames(registry, sending);
    }
}

/// Take the frames a socket's queue can release: every segment up to the
/// first one whose gate has not answered.
///
/// A segment the gate refused ends the socket instead. Everything queued
/// behind it trails the same unproven write, so it is dropped with the
/// segment rather than sent as though the gap were not there.
fn drain_settled(
    deferred: &mut HashMap<u64, std::collections::VecDeque<WsSegment>>,
    id: u64,
    sending: &mut Vec<(u64, WsOut)>,
) {
    let Some(queue) = deferred.get_mut(&id) else {
        return;
    };
    let mut refused = false;
    while let Some(segment) = queue.front() {
        match &segment.settled {
            None => break,
            Some(Err(_)) => {
                refused = true;
                break;
            }
            Some(Ok(())) => {
                let segment = queue.pop_front().expect("the front segment is present");
                sending.extend(segment.frames.into_iter().map(|frame| (id, frame)));
            }
        }
    }
    if refused {
        deferred.remove(&id);
        sending.push((
            id,
            WsOut::Close(
                1011,
                "celld could not prove the write behind this message durable".to_string(),
            ),
        ));
    } else if queue.is_empty() {
        deferred.remove(&id);
    }
}

impl Drop for WsFlushGuard {
    fn drop(&mut self) {
        {
            let mut flushes = self.state.flushes.lock().unwrap();
            for (id, _) in &self.segments {
                let Some(count) = flushes.get_mut(id) else {
                    continue;
                };
                *count -= 1;
                if *count == 0 {
                    flushes.remove(id);
                }
            }
        }
        self.state.done.notify_waiters();
    }
}

/// Wait until no flush holds frames for `id` any more.
///
/// The socket's own teardown calls this. Closing reads the handler's output
/// with a non-blocking drain and, finding none, answers the peer with the
/// protocol echo of the close the peer itself sent -- so a close frame still
/// behind the gate is not merely late, it is replaced. The wait is bounded by
/// the gate: a cell with no barrier answers at once, a barrier settles when
/// its proof does, and an unprovable one still resolves the flush through its
/// fail-closed arm.
pub async fn ws_await_flushes(id: u64) {
    ws_flush_state().await_flushes(id).await;
}

/// Wait until no WebSocket flush of these services still owns deferred
/// frames.
///
/// Shutdown calls this after the cell handoff has settled every output gate
/// and before connection tasks can unregister their sockets. Waiting per
/// socket cannot enforce that ordering because socket teardown deliberately
/// skips its gate wait once the node is draining.
#[doc(hidden)]
pub async fn ws_await_all_flushes() {
    ws_flush_state().await_all_flushes().await;
}

/// Hand frames that cross no queue to the registry, under one lock.
///
/// The registry is a parameter rather than a lookup: a flush runs on a
/// runtime with no services of its own, so it must send to the instance that
/// owns the socket instead of to whichever instance that runtime reaches.
fn emit_frames(
    registry: &std::sync::Mutex<WsRegistry>,
    frames: impl IntoIterator<Item = (u64, WsOut)>,
) {
    let mut frames = frames.into_iter().peekable();
    if frames.peek().is_none() {
        return;
    }
    let mut registry = registry.lock().unwrap();
    for (id, out) in frames {
        registry.emit(id, out);
    }
}

/// Release a batch into each socket's ordered stream. A socket whose
/// durability-ticket queue is still present joins that queue, so an Actor
/// barrier release cannot overtake an earlier frame on the same socket.
pub fn ws_emit_batch(frames: Vec<(u64, WsOut)>) {
    let flush = ws_flush_state();
    let registry = ws_registry();
    // Ungated: the Actor's own barrier already released these frames, so they
    // need no verdict of their own and no flush comes back to wait for one.
    let flushing = flush.emit_or_defer(&registry, frames, false);
    debug_assert!(
        flushing.is_none(),
        "an already released batch opens no flush",
    );
}

/// Close one socket. A forced generation swap closes the regular and
/// outbound sockets that pin a cell to its old isolate and nothing else: the
/// cell stays on this node, so its hibernatable sockets and auto-response
/// pair survive, which `ws_close_scope` would take with it.
pub fn ws_close(id: u64, code: u16, reason: &str) {
    ws_registry()
        .lock()
        .unwrap()
        .emit(id, WsOut::Close(code, reason.to_string()));
}

/// Break a cell's sockets: the output gate could not prove a write durable, so
/// close every socket the cell owns rather than let a client keep a connection
/// whose acknowledged effects may not have persisted (a reset DO).
pub fn ws_close_scope(scope: &str, code: u16, reason: &str) {
    // A reset cell is a new actor; workerd's hibernation manager dies with
    // the old one and takes the auto-response pair with it.
    ws_auto_responses().lock().unwrap().remove(scope);
    let registry = ws_registry();
    let mut registry = registry.lock().unwrap();
    let ids: Vec<u64> = registry
        .metadata
        .iter()
        .filter(|(_, meta)| meta.scope == scope)
        .map(|(id, _)| *id)
        .collect();
    for id in ids {
        registry.emit(id, WsOut::Close(code, reason.to_string()));
    }
}
