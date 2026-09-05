// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The binary contains connection, startup, shutdown, and V8 shell code. The
// library Actor owns the execution domain and its routed adapters.
#![allow(clippy::disallowed_methods)]

//! Runnable celld vertical slice.
//!
//! One actor serializes every event through `celld-logic`; the actor polls its
//! mailbox, timers, and in-flight effect futures together. This is the
//! execution shape required for monotonic lease ticks to fence the node even
//! when a storage operation remains hung, without spawning a task per effect.

use anyhow::Context as _;
use celld::actor::*;
use celld::bucket::Bucket;
use celld::fleet;
use celld::generation::{
    DeploymentGraph, Generation, GenerationOptions, ReloadOutcome, ReloadRequest, FIRST_GENERATION,
};
use celld::js::{
    ArmGate, AssetCallReq, Compat, DoCallReq, HttpResponse, HttpResponseWebSocket, QueueBinding,
    QueueDispatchReq, RpcCallReq, SvcCallReq, SvcRpcReq, WorkerConfigOptions, WorkflowBinding,
};
use celld::ownership_store::{now_ms, BucketOwnership};
use celld::peer_auth::{self, PeerAuth};
use celld::protocol::DeployPointer;
use celld::runtime::{Replication, RuntimeFetch, RuntimeManager, RuntimeOptions, ServiceFetch};
use celld_logic::{RequestError, Route, WebSocketKind};
use futures_util::stream::{FuturesUnordered, StreamExt};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, Limited, StreamBody};
use hyper::body::{Body as _, Bytes, Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};

// glibc's malloc serializes its arenas behind futexes, and under load the
// sixteen worker threads spent up to half a millisecond blocked per
// acquisition. On a 16-core host jemalloc measured 20% more hello-world
// throughput than glibc (mimalloc 11%), and returned the ~7% of the machine
// that arena-lock sleeps reported as idle.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Let an admitted HTTP request finish, then close the transport even when a
/// response stream or a client keep-alive does not settle. The semantic drain
/// continues after this bound, so durability and resident activity still use
/// the complete shutdown grace.
const CONNECTION_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// Retry a failed shutdown accept quickly enough to keep peer and health
/// traffic available, then reduce repeated error work while the listener is
/// unavailable. The complete shutdown deadline still bounds every retry.
const SHUTDOWN_ACCEPT_RETRY_INITIAL: std::time::Duration = std::time::Duration::from_millis(10);
const SHUTDOWN_ACCEPT_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(1);

/// The lossy stdout writer's flush handle. Every exit path uses
/// `std::process::exit`, which skips destructors — but the last lines before
/// an exit are the fence forensics, exactly the lines that must survive.
static LOG_GUARD: std::sync::Mutex<Option<tracing_appender::non_blocking::WorkerGuard>> =
    std::sync::Mutex::new(None);

#[cfg(all(test, celld_internal_tests))]
// Keep the client open until the child exits, so the accepted connection
// reaches the real drain-time HTTP path instead of closing immediately.
static SHUTDOWN_ACCEPT_FAILURE_CLIENT: std::sync::Mutex<Option<std::net::TcpStream>> =
    std::sync::Mutex::new(None);

#[cfg(all(test, celld_internal_tests))]
fn shutdown_accept_failure_test_checkpoint(active: bool, step: &'static str) {
    if active {
        // The tracing writer is deliberately lossy and asynchronous. This
        // process regression needs every cleanup checkpoint to remain
        // visible and ordered when the child exits through process::exit.
        celld::cli_output::Output::new(celld::cli_output::Format::Text)
            .line(format_args!(
                "shutdown accept failure test checkpoint: {step}"
            ))
            .expect("write the shutdown accept failure test checkpoint");
    }
}

static CONNECTION_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CONNECTION_ERRORS_REPORTED: AtomicU64 = AtomicU64::new(0);
static CONNECTION_ERROR_LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);
static CONNECTION_ERRORS_INCOMPLETE: AtomicU64 = AtomicU64::new(0);
static CONNECTION_ERRORS_TIMEOUT: AtomicU64 = AtomicU64::new(0);
static CONNECTION_ERRORS_OTHER: AtomicU64 = AtomicU64::new(0);

fn record_connection_error(
    error: &hyper::Error,
    surface: HttpSurface,
    peer: Option<std::net::SocketAddr>,
) {
    static STARTED: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let total = CONNECTION_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
    let (cause, cause_total) = if error.is_incomplete_message() {
        (
            "incomplete_message",
            CONNECTION_ERRORS_INCOMPLETE.fetch_add(1, Ordering::Relaxed) + 1,
        )
    } else if error.is_timeout() {
        (
            "timeout",
            CONNECTION_ERRORS_TIMEOUT.fetch_add(1, Ordering::Relaxed) + 1,
        )
    } else {
        (
            "other",
            CONNECTION_ERRORS_OTHER.fetch_add(1, Ordering::Relaxed) + 1,
        )
    };
    let elapsed_ms = STARTED.get_or_init(Instant::now).elapsed().as_millis() as u64;
    let last = CONNECTION_ERROR_LAST_LOG_MS.load(Ordering::Relaxed);
    let observed = elapsed_ms.max(1);
    if (last != 0 && elapsed_ms.saturating_sub(last) < 1_000)
        || CONNECTION_ERROR_LAST_LOG_MS
            .compare_exchange(last, observed, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return;
    }
    let reported = CONNECTION_ERRORS_REPORTED.swap(total, Ordering::Relaxed);
    tracing::warn!(
        event = "http_connection_failures",
        total,
        since_last = total.saturating_sub(reported),
        cause,
        cause_total,
        surface = surface.name(),
        ?peer,
        %error,
        "HTTP connections failed"
    );
}

fn exit_flushed(code: i32) -> ! {
    drop(LOG_GUARD.lock().unwrap().take());
    std::process::exit(code);
}

/// Await shutdown work only while the orchestrator-compatible process budget
/// remains. A zero budget does not poll the future, so best-effort store and
/// diagnostic work cannot begin after the absolute deadline.
async fn before_process_deadline<F>(deadline: tokio::time::Instant, future: F) -> Option<F::Output>
where
    F: std::future::Future,
{
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return None;
    }
    tokio::time::timeout(remaining, future).await.ok()
}

/// Finish process-wide WebSocket frame work before connection tasks can
/// unregister the sockets that receive those frames.
async fn flush_shutdown_connections<Frames, Connections>(
    process_deadline: tokio::time::Instant,
    flush_frames: Frames,
    flush_connections: Connections,
) where
    Frames: std::future::Future<Output = ()>,
    Connections: std::future::Future<Output = ()>,
{
    if before_process_deadline(process_deadline, flush_frames)
        .await
        .is_none()
    {
        return;
    }
    let _ = before_process_deadline(
        process_deadline,
        tokio::time::timeout(CONNECTION_DRAIN_GRACE, flush_connections),
    )
    .await;
}

// Keep the runtime owner and the follower fallback in one consumed selection.
// The process must finalize this value before it starts application services,
// so a caller cannot forget the follower-only owner on a non-runtime path.
struct DurabilityOwnerSelection {
    runtime_owner: Option<celld::node_log::DurabilityOwner>,
    follower: Option<Arc<celld::node_log::FollowerStore>>,
}

impl DurabilityOwnerSelection {
    fn new(follower: Option<Arc<celld::node_log::FollowerStore>>) -> Self {
        Self {
            runtime_owner: None,
            follower,
        }
    }

    fn install_runtime(&mut self, owner: celld::node_log::DurabilityOwner) {
        assert!(
            self.runtime_owner.is_none(),
            "the runtime durability owner was already installed"
        );
        self.runtime_owner = Some(owner);
    }

    fn select(self) -> Option<celld::node_log::DurabilityOwner> {
        self.runtime_owner.or_else(|| {
            self.follower
                .map(celld::node_log::DurabilityOwner::new_follower)
        })
    }
}

// Keep the phase-one durability result inside the complete preparation
// decision. A timeout runs fallback, so it must override a successful drain.
fn clean_reload_is_eligible(
    durability_quiesced: bool,
    drained: bool,
    shutdown_mode: ShutdownMode,
) -> bool {
    durability_quiesced && drained && shutdown_mode == ShutdownMode::Preserve
}

type HttpReply = Response<UnsyncBoxBody<Bytes, std::io::Error>>;

const STALE_ROUTE_HEADER: &str = "x-cells-route-error";
const STALE_ROUTE_VALUE: &str = "stale-owner";
const DURABLE_OBJECT_ROUTING_ERROR_MARKER: &str = "__CELLD_DO_ROUTING_ERROR__:";

fn owner_unreachable(scope: &str, owner: &str, source: anyhow::Error) -> anyhow::Error {
    // Record how the attempt failed, not just that it did. `connect` is the
    // one that decides whether a retry is safe -- a request that never left
    // this node may be re-sent, a truncated read may not, because the owner
    // already ran it. Without these an operator cannot tell an unreachable
    // peer from one that answered badly, and neither can a bug report.
    let transport = source.downcast_ref::<reqwest::Error>();
    // The full chain, because the root cause is the actionable part: a
    // "connect error" one level down still hides refused vs. out-of-fds.
    let cause = format!("{source:#}");
    tracing::warn!(
        %scope,
        %owner,
        error = %source,
        %cause,
        connect = transport.is_some_and(reqwest::Error::is_connect),
        timeout = transport.is_some_and(reqwest::Error::is_timeout),
        request = transport.is_some_and(reqwest::Error::is_request),
        body = transport.is_some_and(reqwest::Error::is_body),
        decode = transport.is_some_and(reqwest::Error::is_decode),
        "peer owner unreachable"
    );
    let detail = serde_json::json!({
        "scope": scope,
        "owner": owner,
    });
    source.context(format!("{DURABLE_OBJECT_ROUTING_ERROR_MARKER}{detail}"))
}

fn response(status: StatusCode, body: impl Into<Bytes>) -> HttpReply {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(
            Full::new(body.into())
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .expect("static HTTP response")
}

fn asset_response(response: axum::response::Response) -> HttpReply {
    response.map(|body| body.map_err(std::io::Error::other).boxed_unsync())
}

fn peer_response(mut response: HttpReply) -> HttpReply {
    response.headers_mut().insert(
        hyper::header::HeaderName::from_static(peer_auth::RESPONSE_VERSION_HEADER),
        hyper::header::HeaderValue::from_static(peer_auth::PROTOCOL_VERSION_TEXT),
    );
    response
}

/// Decides whether a Worker-set response header reaches the caller. The
/// `connection` and `transfer-encoding` headers describe the Worker-side hop,
/// and the host frames every body it sends, so a Worker `content-length` is
/// stale framing it must not repeat — except on a HEAD response, where the
/// body the host measures is empty and the Worker's value is the only
/// representation length the client can get. RFC 9110 forbids the header on a
/// 1xx or 204 status, so those never carry one even on a HEAD path.
fn forwards_worker_header(
    name: &str,
    preserve_representation_length: bool,
    status: StatusCode,
) -> bool {
    !matches!(
        name.to_ascii_lowercase().as_str(),
        "connection" | "transfer-encoding"
    ) && (!name.eq_ignore_ascii_case("content-length")
        || (preserve_representation_length
            && !status.is_informational()
            && status != StatusCode::NO_CONTENT))
}

fn runtime_response(
    worker_response: celld::js::HttpResponse,
    preserve_representation_length: bool,
) -> HttpReply {
    let Ok(status) = StatusCode::from_u16(worker_response.status) else {
        return response(StatusCode::INTERNAL_SERVER_ERROR, "invalid Worker status");
    };
    let mut builder = Response::builder().status(status);
    for (name, value) in worker_response.headers {
        if !forwards_worker_header(&name, preserve_representation_length, status) {
            continue;
        }
        builder = builder.header(name, value);
    }
    let body = match worker_response.stream {
        Some(stream) => {
            let chunks = stream.map(|chunk| {
                chunk
                    .map(|bytes| Frame::data(Bytes::from(bytes)))
                    .map_err(std::io::Error::other)
            });
            StreamBody::new(chunks).boxed_unsync()
        }
        None => Full::new(Bytes::from(worker_response.body))
            .map_err(|never| match never {})
            .boxed_unsync(),
    };
    builder
        .body(body)
        .unwrap_or_else(|_| response(StatusCode::INTERNAL_SERVER_ERROR, "invalid Worker headers"))
}

#[derive(Debug)]
struct StalePeerRoute {
    scope: String,
}

impl std::fmt::Display for StalePeerRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "peer no longer owns {}", self.scope)
    }
}

impl std::error::Error for StalePeerRoute {}

#[derive(Debug)]
struct RoutedRequestError(RequestError);

/// The gate's verdict as the answer's error, with the handler's own failure
/// kept below it when the answer was one: the client needs the verdict, and
/// the operator reading the chain needs what the handler said too. The
/// verdict stays on top, so the routed-error match still finds it.
fn gate_failure(verdict: RequestError, handler: Option<anyhow::Error>) -> anyhow::Error {
    match handler {
        Some(handler) => handler.context(RoutedRequestError(verdict)),
        None => anyhow::Error::new(RoutedRequestError(verdict)),
    }
}

/// The handler's own failure, for a peer reply body that carries the gate's
/// verdict; empty when the handler answered.
fn handler_failure_detail(handler: Option<&anyhow::Error>) -> String {
    match handler {
        Some(handler) => format!("; the handler failed: {handler:#}"),
        None => String::new(),
    }
}

/// The ticket an answer takes through the output gate, or `None` when the
/// gate is off or the answer takes none: see `celld::js::answer_ticket`.
fn gate_ticket<T: celld::js::GatedAnswer>(
    app: &AppHandle,
    result: &anyhow::Result<T>,
) -> Option<GateTicket> {
    app.output_gate
        .then(|| celld::js::answer_ticket(result))
        .flatten()
}

impl std::fmt::Display for RoutedRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "route failed: {:?}", self.0)
    }
}

impl std::error::Error for RoutedRequestError {}

fn classify_remote_attempt(error: &anyhow::Error) -> celld_logic::routing::Attempt {
    if error.downcast_ref::<StalePeerRoute>().is_some()
        || error
            .downcast_ref::<peer_tunnel::StaleTunnelRoute>()
            .is_some()
    {
        celld_logic::routing::Attempt::NotOwner
    } else if error
        .downcast_ref::<reqwest::Error>()
        .is_some_and(reqwest::Error::is_connect)
    {
        celld_logic::routing::Attempt::NeverConnected
    } else {
        celld_logic::routing::Attempt::Ambiguous
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteRetryAction {
    Stop,
    Redispatch,
    WaitForOwner,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteRetryWait {
    Ready,
    Deadline,
    Cancelled,
}

/// Keep the one-retry transport policy, but do not spend that retry on an
/// ownership generation which a reachable peer already rejected.
///
/// A replacement can bind the prior address while the prior session's lease
/// is still live. The new process then gives a definitive `NotOwner` answer
/// for the old `(node, epoch)`. An immediate redispatch resolves that same
/// generation and fails a safe application retry before ownership can move.
struct RemoteRouteRetry {
    dispatcher: celld_logic::routing::Dispatcher,
    generation: Option<(String, u64)>,
    blocked: bool,
    deadline_ms: u64,
    next_wait_ms: u64,
}

impl RemoteRouteRetry {
    const INITIAL_WAIT_MS: u64 = 10;
    const MAX_WAIT_MS: u64 = 250;

    fn new(operation_deadline_ms: u64) -> Self {
        Self {
            dispatcher: celld_logic::routing::Dispatcher::default(),
            generation: None,
            blocked: false,
            deadline_ms: celld::asyncrt::mono_ms().saturating_add(operation_deadline_ms),
            next_wait_ms: Self::INITIAL_WAIT_MS,
        }
    }

    /// Return true when this route is known not to be usable yet.
    fn observe(&mut self, node: &str, epoch: u64) -> bool {
        let changed = self
            .generation
            .as_ref()
            .is_none_or(|(current_node, current_epoch)| {
                current_node != node || *current_epoch != epoch
            });
        if changed {
            self.generation = Some((node.to_string(), epoch));
            self.blocked = false;
            self.dispatcher = celld_logic::routing::Dispatcher::default();
            self.next_wait_ms = Self::INITIAL_WAIT_MS;
        }
        self.blocked
    }

    fn failed(&mut self, attempt: celld_logic::routing::Attempt) -> RemoteRetryAction {
        match attempt {
            celld_logic::routing::Attempt::Ambiguous => RemoteRetryAction::Stop,
            celld_logic::routing::Attempt::NotOwner => {
                if !self.dispatcher.redispatch(attempt) {
                    return RemoteRetryAction::Stop;
                }
                self.blocked = true;
                RemoteRetryAction::WaitForOwner
            }
            celld_logic::routing::Attempt::NeverConnected
                if self.dispatcher.redispatch(attempt) =>
            {
                RemoteRetryAction::Redispatch
            }
            celld_logic::routing::Attempt::NeverConnected => RemoteRetryAction::Stop,
        }
    }

    async fn wait(&mut self, cancel: Option<&mut oneshot::Receiver<()>>) -> RemoteRetryWait {
        let remaining_ms = self.deadline_ms.saturating_sub(celld::asyncrt::mono_ms());
        if remaining_ms == 0 {
            return RemoteRetryWait::Deadline;
        }
        let wait = std::time::Duration::from_millis(remaining_ms.min(self.next_wait_ms));
        self.next_wait_ms = self.next_wait_ms.saturating_mul(2).min(Self::MAX_WAIT_MS);
        match cancel {
            Some(cancel) => celld::asyncrt::select_biased! {
                "a lifecycle cancellation wins a tie with route retry backoff";
                _ = cancel => RemoteRetryWait::Cancelled,
                _ = celld::asyncrt::sleep(wait) => RemoteRetryWait::Ready,
            },
            None => {
                celld::asyncrt::sleep(wait).await;
                RemoteRetryWait::Ready
            }
        }
    }
}

struct WebSocketRouteTiming {
    started: Instant,
    route_resolution_us: u64,
    dispatch_us: u64,
    attempts: u8,
}

impl WebSocketRouteTiming {
    fn emit(
        &self,
        app: &AppHandle,
        scope: &str,
        request_id: Option<celld::js::RequestId>,
        outcome: &str,
        route: &str,
        peer_node: &str,
    ) {
        let request_id = request_id
            .map(celld::js::request_id_string)
            .unwrap_or_default();
        let (node, region) = app
            .runtime
            .as_ref()
            .map_or(("", ""), |runtime| (runtime.node(), runtime.region()));
        tracing::debug!(
            target: "timing",
            event = "websocket_route_timing",
            outcome,
            route,
            peer_node,
            scope,
            request_id,
            node,
            region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            attempts = self.attempts,
            total_us = self.started.elapsed().as_micros() as u64,
            route_resolution_us = self.route_resolution_us,
            dispatch_us = self.dispatch_us,
            "WebSocket cell request resolved"
        );
    }
}

/// The output gate for an effect raised inside a handler, as opposed to the
/// handler's own response.
///
/// Every in-handler channel arrives here holding a `GateReq`. Reuses the routed
/// machinery: `request` pins the cell (no eviction mid-wait) and the core gate
/// decides. `Ok` releases the held effect; `Err` breaks the call, as a routed
/// gate failure would.
async fn dispatch_gate(app: AppHandle, req: celld::js::GateReq) {
    if !app.output_gate {
        let _ = req.reply.send(Ok(()));
        return;
    }
    let routed = match app.request(req.scope.clone()).await {
        Ok(routed) => routed,
        Err(error) => {
            let _ = req.reply.send(Err(error));
            return;
        }
    };
    // The guard pins the cell and releases the request on drop, so the else
    // branch does not leak the just-acquired request.
    let _activity = app.activity(routed.request, req.scope.clone());
    let result = if routed.route == Route::Local {
        app.gate_output(routed.request, req.ticket).await
    } else {
        // The owning isolate should route the cell locally; if it moved off the
        // node mid-call, fail closed rather than acknowledge an unproven write.
        Err(RequestError::NodeFenced)
    };
    let _ = req.reply.send(result);
}

/// Propagates a forwarding-side cancellation over the authenticated peer
/// protocol. Dropping a Reqwest future does not guarantee that its pooled
/// HTTP/1 transport closes, so the owner cannot use transport EOF alone.
struct RemoteFetchAbortGuard {
    http: reqwest::Client,
    auth: Arc<PeerAuth>,
    node: String,
    addr: String,
    scope: String,
    request: Option<celld::js::RequestId>,
    phase: &'static str,
}

impl RemoteFetchAbortGuard {
    fn new(
        app: &AppHandle,
        node: String,
        addr: String,
        scope: String,
        request: Option<celld::js::RequestId>,
    ) -> Self {
        Self {
            http: app.peer_http.clone(),
            auth: app.peer_auth.clone(),
            node,
            addr,
            scope,
            request,
            phase: "response_head",
        }
    }

    fn body_active(&mut self) {
        self.phase = "response_body";
    }

    fn disarm(&mut self) {
        self.request = None;
    }
}

impl Drop for RemoteFetchAbortGuard {
    fn drop(&mut self) {
        let Some(request) = self.request.take() else {
            return;
        };
        let http = self.http.clone();
        let auth = self.auth.clone();
        let node = self.node.clone();
        let addr = self.addr.clone();
        let scope = self.scope.clone();
        let phase = self.phase;
        celld::asyncrt::op_handle().spawn(async move {
            let abort = send_peer_abort(http, auth, node.clone(), addr, scope.clone(), request);
            match tokio::time::timeout(std::time::Duration::from_secs(2), abort).await {
                Ok(Ok(())) => tracing::debug!(
                    event = "peer_fetch_abort_sent",
                    %node,
                    %scope,
                    request_id = %celld::js::request_id_string(request),
                    phase,
                    "propagated a disconnected caller to the cell owner"
                ),
                Ok(Err(error)) => tracing::debug!(
                    event = "peer_fetch_abort_failed",
                    %node,
                    %scope,
                    %error,
                    "could not propagate a disconnected caller to the cell owner"
                ),
                Err(_) => tracing::debug!(
                    event = "peer_fetch_abort_timed_out",
                    %node,
                    %scope,
                    "timed out propagating a disconnected caller to the cell owner"
                ),
            }
        });
    }
}

async fn send_peer_abort(
    http: reqwest::Client,
    auth: Arc<PeerAuth>,
    node: String,
    addr: String,
    scope: String,
    request: celld::js::RequestId,
) -> anyhow::Result<()> {
    let encoded_scope =
        percent_encoding::utf8_percent_encode(&scope, percent_encoding::NON_ALPHANUMERIC);
    let path = format!(
        "/peer/abort/{encoded_scope}/{}",
        celld::js::request_id_string(request)
    );
    let request = auth.sign(
        http.post(format!("http://{addr}{path}")),
        "POST",
        &path,
        &[],
        &node,
    )?;
    let response = request.body(Vec::new()).send().await?;
    peer_auth::validate_response(response.headers())?;
    anyhow::ensure!(
        response.status().is_success(),
        "peer abort failed with {}",
        response.status()
    );
    Ok(())
}

/// Call the reserved cron cell's arm endpoint, through the ordinary Durable
/// Object routing path so ownership resolution and remote forwarding are the
/// ones every other call gets.
async fn arm_cron_schedule(app: AppHandle, cell: String) -> anyhow::Result<()> {
    let (reply, receive) = tokio::sync::oneshot::channel();
    let body = celld::js::RequestBody::Bytes(Bytes::new());
    dispatch_do_call(
        app,
        DoCallReq {
            request_id: None,
            cancel: None,
            deliver_abort_to_handler: false,
            scope: cell,
            name: None,
            url: "https://cron.celld.internal/arm".to_string(),
            method: "POST".to_string(),
            body_guard: celld::js::RequestBodyGuard::of(&body),
            body,
            headers: Vec::new(),
            reply,
            order: None,
            parent: None,
        },
    )
    .await;
    let response = receive.await.context("cron arm dispatch dropped")??;
    if !(200..300).contains(&response.status) {
        anyhow::bail!("cron arm returned status {}", response.status);
    }
    Ok(())
}

/// Arm the current deployment's cron schedule, if it declares one.
///
/// A cron cell has no client to wake it, so somebody has to make the first
/// call; every node makes it, and ownership CAS decides which one keeps the
/// cell while the others route to that owner. No new election, no reserved
/// node -- the same arbiter that makes an alarm fire once per fleet makes a
/// cron trigger fire once too. Boot and adoption both come through here, so
/// a schedule change arrives with the deployment that carries it.
fn spawn_cron_arm(app: AppHandle) {
    let Some(cell) = app.runtime.as_ref().and_then(|runtime| runtime.cron_cell()) else {
        return;
    };
    tokio::spawn(async move {
        // Routing needs node authority, exactly as the wake scan does.
        let deadline = Instant::now() + std::time::Duration::from_secs(30);
        while !app.healthy().await {
            if Instant::now() >= deadline {
                tracing::warn!(cell, "cron schedule not armed: no node authority");
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // A failed arm leaves the schedule silent until the next adoption,
        // so retry rather than log once. The backoff is bounded because a
        // fleet that cannot route for a minute has a larger problem.
        for attempt in 0..5 {
            match arm_cron_schedule(app.clone(), cell.clone()).await {
                Ok(()) => return,
                Err(error) if attempt == 4 => {
                    tracing::error!(cell, %error, "cron schedule not armed");
                }
                Err(error) => {
                    tracing::warn!(cell, %error, attempt, "cron arm failed, retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(500 << attempt.min(5)))
                        .await;
                }
            }
        }
    });
}

/// Adopt the deployment `pointer` names.
///
/// The same two calls boot makes -- `DeploymentGraph::load` and
/// `Generation::build` -- then the flip and the cron arm. A build that fails
/// leaves the current generation serving; nothing has changed until `adopt`.
async fn adopt_deployment(
    app: &AppHandle,
    bucket: &Bucket,
    node: &str,
    region: &str,
    pointer: DeployPointer,
    force: bool,
) -> ReloadOutcome {
    let Some(runtime) = app.runtime.as_ref() else {
        return ReloadOutcome::Failed {
            version: pointer.version,
            prefix: pointer.prefix,
            error: "this node has no Worker runtime".to_string(),
        };
    };
    let current = runtime.generation();
    if !force && pointer.version == current.version() && pointer.prefix == current.prefix() {
        return ReloadOutcome::Unchanged {
            generation: current.id(),
            version: current.version().to_string(),
        };
    }
    let failed = |error: String| ReloadOutcome::Failed {
        version: pointer.version.clone(),
        prefix: pointer.prefix.clone(),
        error,
    };
    let graph = match DeploymentGraph::load(bucket, node.to_string()).await {
        Ok(graph) => graph,
        Err(error) => return failed(format!("{error:#}")),
    };
    let id = runtime.next_generation_id();
    let options = GenerationOptions {
        loader_binding: worker_loader_binding(),
        node: node.to_string(),
        region: region.to_string(),
    };
    // Building compiles every script, so it runs on a blocking thread.
    let built = tokio::task::spawn_blocking(move || Generation::build(id, graph, options)).await;
    let generation = match built {
        Ok(Ok(generation)) => generation,
        Ok(Err(error)) => return failed(format!("{error:#}")),
        Err(error) => return failed(format!("generation build panicked: {error}")),
    };
    let adopted = runtime.adopt(generation);
    // Tell the core, so resident cells move to the new generation at their
    // safe points. The reserved cells move at once, ahead of the cron arm
    // below, which must reach a cron cell already running the new schedule.
    let _ = app.tx.send(Message::GenerationChanged {
        generation: adopted.id(),
        max_age_ms: celld::generation::max_age().as_millis() as u64,
        eager_classes: adopted.reserved_classes(),
    });
    spawn_cron_arm(app.clone());
    ReloadOutcome::Adopted {
        generation: adopted.id(),
        version: adopted.version().to_string(),
        prefix: adopted.prefix().to_string(),
    }
}

/// The pointer watcher: the node's one reader of `deploy/current.json`
/// after boot.
///
/// It polls on a slow interval and adopts what the pointer names through
/// `adopt_deployment`; a `POST /reload` or a managed notification only makes
/// it look now. The pointer is the authority, so a missed nudge degrades to
/// "adopts one interval later". A version that failed to build is remembered
/// and not retried until the pointer names something else or a reload is
/// forced: a broken bundle must not recompile every interval on every node.
fn start_pointer_watcher(
    app: AppHandle,
    bucket: Bucket,
    node: String,
    region: String,
    mut requests: celld::generation::ReloadReceiver,
) {
    tokio::spawn(async move {
        let period = celld::generation::poll_interval();
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut failed: Option<ReloadOutcome> = None;
        loop {
            let request = celld::asyncrt::select! {
                _ = tick.tick() => ReloadRequest { force: false, reply: None },
                request = requests.recv() => match request {
                    Some(request) => request,
                    None => return,
                },
            };
            let outcome = match fleet::read_current_pointer(&bucket).await {
                Err(error) => {
                    tracing::warn!(event = "deployment_pointer_unreadable", %error);
                    ReloadOutcome::Failed {
                        version: String::new(),
                        prefix: String::new(),
                        error: format!("{error:#}"),
                    }
                }
                Ok(pointer) => match &failed {
                    Some(ReloadOutcome::Failed {
                        version, prefix, ..
                    }) if !request.force
                        && version == &pointer.version
                        && prefix == &pointer.prefix =>
                    {
                        failed.clone().expect("matched above")
                    }
                    _ => {
                        adopt_deployment(&app, &bucket, &node, &region, pointer, request.force)
                            .await
                    }
                },
            };
            match &outcome {
                ReloadOutcome::Adopted {
                    generation,
                    version,
                    prefix,
                } => {
                    failed = None;
                    tracing::info!(
                        event = "deployment_adopted",
                        generation,
                        version = %version,
                        prefix = %prefix,
                        "deployment adopted in place"
                    );
                }
                ReloadOutcome::Unchanged { .. } => {}
                ReloadOutcome::Failed {
                    version,
                    prefix,
                    error,
                } => {
                    if failed.as_ref() != Some(&outcome) {
                        tracing::error!(
                            event = "deployment_adoption_failed",
                            version = %version,
                            prefix = %prefix,
                            %error,
                            "deployment did not build; the current generation keeps serving"
                        );
                    }
                    if !version.is_empty() {
                        failed = Some(outcome.clone());
                    }
                }
            }
            if let Some(reply) = request.reply {
                let _ = reply.send(outcome);
            }
        }
    });
}

/// `POST /reload`: adopt the pointer's deployment now and report the outcome.
/// Forced, so an unchanged pointer still builds a new generation and a vars
/// file edit takes effect.
/// Pause or resume balancing. The node publishes the flag in its lease, and
/// one paused lease stops every donor in the fleet, so an operator pauses
/// the fleet from any node and resumes it from the same one.
fn internal_rebalance_switch(paused: bool) -> HttpReply {
    if !celld::ownership_store::set_rebalance_paused(paused) {
        return response(StatusCode::CONFLICT, "this node publishes no lease");
    }
    response(StatusCode::OK, format!("{{\"rebalance_paused\":{paused}}}"))
}

async fn internal_reload(app: AppHandle) -> HttpReply {
    let (reply, receive) = tokio::sync::oneshot::channel();
    let sent = app
        .reload
        .send(ReloadRequest {
            force: true,
            reply: Some(reply),
        })
        .is_ok();
    let outcome = if sent { receive.await.ok() } else { None };
    let (status, body) = match outcome {
        Some(ReloadOutcome::Adopted {
            generation,
            version,
            prefix,
        }) => (
            StatusCode::OK,
            serde_json::json!({
                "ok": true,
                "outcome": "adopted",
                "generation": generation,
                "version": version,
                "prefix": prefix,
            }),
        ),
        Some(ReloadOutcome::Unchanged {
            generation,
            version,
        }) => (
            StatusCode::OK,
            serde_json::json!({
                "ok": true,
                "outcome": "unchanged",
                "generation": generation,
                "version": version,
            }),
        ),
        Some(ReloadOutcome::Failed {
            version,
            prefix,
            error,
        }) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            serde_json::json!({
                "ok": false,
                "outcome": "failed",
                "version": version,
                "prefix": prefix,
                "error": error,
            }),
        ),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({
                "ok": false,
                "outcome": "unavailable",
                "error": "this node has no deployment pointer to reload from",
            }),
        ),
    };
    response(status, body.to_string())
}

fn local_dispatch_request_id(
    request_id: Option<celld::js::RequestId>,
) -> Option<celld::js::RequestId> {
    // Internal Queue, cron, alarm, and peer calls have no client request ID,
    // but a local handler still needs an identity in the abort registry. The
    // drain cancels by core request and resolves that identity through the
    // activity pin; leaving it absent makes a busy internal handler impossible
    // to stop before the process deadline.
    Some(request_id.unwrap_or_else(celld::js::next_request_id))
}

async fn dispatch_do_call(app: AppHandle, call: DoCallReq) {
    let DoCallReq {
        request_id,
        cancel,
        deliver_abort_to_handler,
        scope,
        name,
        url,
        method,
        body,
        mut body_guard,
        headers,
        reply,
        order,
        parent,
    } = call;
    let mut cancel = cancel;
    let mut order = order;
    let mut websocket_timing = headers
        .iter()
        .any(|(name, value)| {
            name.eq_ignore_ascii_case("upgrade") && value.eq_ignore_ascii_case("websocket")
        })
        .then(|| WebSocketRouteTiming {
            started: Instant::now(),
            route_resolution_us: 0,
            dispatch_us: 0,
            attempts: 0,
        });
    let operation = async {
        // Reclaims a streamed body abandoned by an early error below. A local
        // target or the peer HTTP body takes ownership before this guard is
        // disarmed.
        anyhow::ensure!(
            celld_logic::cell::valid_cell_scope(&scope),
            "cell scope is malformed or exceeds the fleet storage limit"
        );
        let mut dispatcher = RemoteRouteRetry::new(app.operation_deadline_ms);
        loop {
            if let Some(timing) = websocket_timing.as_mut() {
                timing.attempts = timing.attempts.saturating_add(1);
            }
            let route_started = Instant::now();
            // A disconnect before routing completes has executed no handler,
            // so cancel the core request and release its activation admission.
            // Once routing completes, the same signal moves into the local or
            // remote dispatch below and aborts work that did start.
            let route = app.request(scope.clone());
            let routed = if deliver_abort_to_handler {
                // Workerd delivers an explicit JavaScript AbortSignal to the
                // target request. Resolve the route first, then give the
                // already-fired receiver to fetch_cell so the handler sees
                // request.signal and its waitUntil work can continue.
                route.await
            } else {
                match cancel.as_mut() {
                    Some(cancel) => celld::asyncrt::select_biased! {
                        "a cancellation that ties route resolution prevents dispatch from starting";
                        _ = cancel => break Err(anyhow::anyhow!("Durable Object call cancelled")),
                        routed = route => routed,
                    },
                    None => route.await,
                }
            };
            let routed = match routed {
                Ok(routed) => routed,
                Err(error) => {
                    if let Some(timing) = websocket_timing.as_mut() {
                        timing.route_resolution_us = timing
                            .route_resolution_us
                            .saturating_add(route_started.elapsed().as_micros() as u64);
                        timing.emit(&app, &scope, request_id, "route_error", "", "");
                    }
                    break Err(anyhow::Error::new(RoutedRequestError(error)));
                }
            };
            if let Some(timing) = websocket_timing.as_mut() {
                timing.route_resolution_us = timing
                    .route_resolution_us
                    .saturating_add(route_started.elapsed().as_micros() as u64);
            }
            let Routed { request, route } = routed;
            let (node, addr, epoch, peer_protocol) = match route {
                Route::Local => {
                    let dispatch_started = Instant::now();
                    let local_request_id = local_dispatch_request_id(request_id);
                    let activity =
                        app.activity_for(request, scope.clone(), local_request_id, "local");
                    let result = async {
                        let runtime = app.runtime.as_ref().context("no cell runtime")?;
                        let response = runtime
                            .fetch_cell(
                                scope.clone(),
                                name,
                                RuntimeFetch {
                                    url,
                                    method,
                                    body,
                                    headers,
                                    request_id: local_request_id,
                                    // Moved on the first attempt and gone on
                                    // a retry, which is right: a retry is a
                                    // second delivery of a call whose place
                                    // in the order was already taken.
                                    order: order.take(),
                                    parent,
                                },
                                cancel.take(),
                            )
                            .await?;
                        // `fetch_cell` cannot return a response until the cell
                        // has installed its request context. That context now
                        // owns an unread tail through its waitUntil work.
                        body_guard.disarm();
                        if let Some(HttpResponseWebSocket::Cell(target)) = &response.websocket {
                            let kind = if celld::js::ws_hibernatable(target.id).unwrap_or(false) {
                                WebSocketKind::Hibernatable
                            } else {
                                WebSocketKind::Regular
                            };
                            app.websocket_opened(target.scope.clone(), target.id, kind)
                                .await?;
                        }
                        Ok(response)
                    }
                    .await;
                    // Output gate (RPO=0): a handler that advanced the cell's
                    // write position has its response held until the core proves
                    // the cell durable. The request is still pinned (the
                    // activity guard has not dropped), so it fails rather than
                    // acknowledges a write the node cannot prove durable. A
                    // handler that failed after it committed takes the same
                    // ticket: its commit is as unproven as a success's, and an
                    // error that took none opened no barrier for the read-only
                    // request that followed to trail (#715).
                    let result = match gate_ticket(&app, &result) {
                        Some(ticket) => {
                            activity.set_phase("output_gate", false, false);
                            if let Some(position) = ticket.position {
                                activity.gate_started(position);
                            }
                            let gated = app.gate_output(request, ticket).await;
                            activity.gate_finished(gated.is_ok());
                            match gated {
                                Ok(()) => result,
                                Err(error) => Err(gate_failure(error, result.err())),
                            }
                        }
                        None => result,
                    };
                    let result = match result {
                        Ok(mut response) => {
                            let body_active = response.stream.is_some();
                            activity.set_phase("response_body", true, body_active);
                            if let Some(stream) = response.stream.take() {
                                response.stream = Some(local_response_stream(stream, activity));
                            } else {
                                drop(activity);
                            }
                            Ok(response)
                        }
                        Err(error) => Err(error),
                    };
                    if let Some(timing) = websocket_timing.as_mut() {
                        timing.dispatch_us = timing
                            .dispatch_us
                            .saturating_add(dispatch_started.elapsed().as_micros() as u64);
                        timing.emit(
                            &app,
                            &scope,
                            request_id,
                            if result.is_ok() { "ok" } else { "error" },
                            "local",
                            app.runtime.as_ref().map_or("", RuntimeManager::node),
                        );
                    }
                    break result;
                }
                Route::Remote {
                    node,
                    addr,
                    epoch,
                    peer_protocol,
                } => (node, addr, epoch, peer_protocol),
            };
            if dispatcher.observe(&node, epoch) {
                let cancel = if deliver_abort_to_handler {
                    None
                } else {
                    cancel.as_mut()
                };
                match dispatcher.wait(cancel).await {
                    RemoteRetryWait::Ready => {
                        app.invalidate_remote(scope.clone(), node, epoch).await;
                        continue;
                    }
                    RemoteRetryWait::Cancelled => {
                        break Err(anyhow::anyhow!("Durable Object call cancelled"));
                    }
                    RemoteRetryWait::Deadline => {
                        break Err(anyhow::anyhow!(
                            "remote owner remained stale until the retry deadline"
                        ));
                    }
                }
            }
            let dispatch_started = Instant::now();
            let streamed_attempt = matches!(body, celld::js::RequestBody::Stream(_));
            let remote_call = async {
                anyhow::ensure!(
                    peer_protocol == peer_auth::PROTOCOL_VERSION,
                    "peer {node} speaks incompatible protocol {peer_protocol}"
                );
                {
                    let control = peer_tunnel::TunnelControl {
                        scope: scope.clone(),
                        name: name.clone(),
                        request_id,
                        capacity_handoff: epoch == 0,
                    };
                    let attempt_body = match &body {
                        celld::js::RequestBody::Bytes(bytes) => {
                            celld::js::RequestBody::Bytes(bytes.clone())
                        }
                        celld::js::RequestBody::Stream(stream_id) => {
                            celld::js::RequestBody::Stream(*stream_id)
                        }
                    };
                    let mut abort = RemoteFetchAbortGuard::new(
                        &app,
                        node.clone(),
                        addr.clone(),
                        scope.clone(),
                        request_id,
                    );
                    let response = peer_tunnel::fetch(
                        &peer_tunnel::Peer {
                            http: &app.peer_http,
                            auth: &app.peer_auth,
                            addr: &addr,
                            node: &node,
                        },
                        &control,
                        url.clone(),
                        method.clone(),
                        headers.clone(),
                        attempt_body,
                    )
                    .await
                    .map_err(|error| owner_unreachable(&scope, &addr, error))?;
                    // Only after the tunnel has taken the stream: `disarm`
                    // releases the guard's registry claim, and a claim that
                    // drops to zero deletes the entry, so disarming before
                    // the take deletes the body out from under the attempt.
                    // An error above keeps the guard armed so an untaken
                    // stream is still cleaned up.
                    body_guard.disarm();
                    if response
                        .headers()
                        .get(STALE_ROUTE_HEADER)
                        .is_some_and(|value| value == STALE_ROUTE_VALUE)
                    {
                        abort.disarm();
                        return Err(owner_unreachable(
                            &scope,
                            &addr,
                            anyhow::Error::new(StalePeerRoute {
                                scope: scope.clone(),
                            }),
                        ));
                    }
                    let status = response.status().as_u16();
                    if status == 101 {
                        // A tunneled upgrade: the inner 101 is the cell's
                        // own handshake for this client's key. Park the
                        // inner upgrade; once this node's client socket
                        // upgrades, the two splice and the hop copies
                        // bytes. The handshake fields are dropped because
                        // the client-facing 101 regenerates them.
                        let response_headers = response
                            .headers()
                            .iter()
                            .filter(|(header, _)| {
                                let header = header.as_str();
                                header != "upgrade"
                                    && header != "connection"
                                    && header != "sec-websocket-accept"
                            })
                            .filter_map(|(header, value)| {
                                value
                                    .to_str()
                                    .ok()
                                    .map(|value| (header.to_string(), value.to_string()))
                            })
                            .collect();
                        abort.disarm();
                        let mut response = response;
                        let parked = peer_tunnel::park_upgrade(hyper::upgrade::on(&mut response));
                        return Ok(HttpResponse {
                            status,
                            headers: response_headers,
                            body: Vec::new(),
                            websocket: Some(HttpResponseWebSocket::Cell(celld::js::WsTarget {
                                id: 0,
                                scope: scope.clone(),
                                tunnel: Some(parked),
                            })),
                            stream: None,
                            write_position: None,
                            observed_position: None,
                        });
                    }
                    let response_headers = response
                        .headers()
                        .iter()
                        .filter_map(|(header, value)| {
                            value
                                .to_str()
                                .ok()
                                .map(|value| (header.to_string(), value.to_string()))
                        })
                        .collect();
                    let stream = forwarder_response_stream(
                        peer_tunnel::response_stream(response.into_body()),
                        abort,
                    );
                    Ok(HttpResponse {
                        status,
                        headers: response_headers,
                        body: Vec::new(),
                        websocket: None,
                        stream: Some(stream),
                        // A proxied remote response wrote on the owner, not
                        // here.
                        write_position: None,
                        observed_position: None,
                    })
                }
            };
            let remote = match cancel.as_mut() {
                Some(cancel) => celld::asyncrt::select_biased! {
                    "a cancellation that ties the remote response prevents returning abandoned work";
                    _ = cancel => break Err(anyhow::anyhow!("Durable Object call cancelled")),
                    remote = remote_call => remote,
                },
                None => remote_call.await,
            };
            if let Some(timing) = websocket_timing.as_mut() {
                timing.dispatch_us = timing
                    .dispatch_us
                    .saturating_add(dispatch_started.elapsed().as_micros() as u64);
            }
            match remote {
                Ok(response) => {
                    if let Some(timing) = websocket_timing.as_ref() {
                        timing.emit(&app, &scope, request_id, "ok", "remote", &node);
                    }
                    break Ok(response);
                }
                Err(error) => {
                    // A streamed upload is not replayable after the HTTP
                    // client starts pulling it. Retrying would send only the
                    // unread suffix as a new request, so fail this attempt
                    // instead of manufacturing a truncated body.
                    if streamed_attempt {
                        break Err(error);
                    }
                    // Epoch zero is a candidate, not an owner. A peer refusal
                    // proves this attempt did not execute and must not consume
                    // the ordinary one-owner stale-route budget;
                    // the core excludes that exact load sample before the
                    // next deterministic placement decision.
                    let capacity_refused = epoch == 0
                        && error
                            .chain()
                            .any(|cause| cause.downcast_ref::<StalePeerRoute>().is_some());
                    if capacity_refused {
                        app.invalidate_remote(scope.clone(), node, epoch).await;
                        // Pace this unbounded retry. Each attempt now costs a
                        // fresh tunnel connection, and a tight loop against a
                        // candidate that is still restoring floods the client
                        // ephemeral-port range into TIME_WAIT (os error 49),
                        // which then poisons every dial for the next 15s.
                        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                        continue;
                    }
                    let attempt = classify_remote_attempt(&error);
                    let action = dispatcher.failed(attempt);
                    tracing::warn!(
                        event = "remote_route_retry",
                        %scope,
                        %node,
                        epoch,
                        ?attempt,
                        ?action,
                        "a peer attempt changed the remote route retry state"
                    );
                    match action {
                        RemoteRetryAction::Stop => {}
                        RemoteRetryAction::Redispatch => {
                            app.invalidate_remote(scope.clone(), node, epoch).await;
                            continue;
                        }
                        RemoteRetryAction::WaitForOwner => {
                            let cancel = if deliver_abort_to_handler {
                                None
                            } else {
                                cancel.as_mut()
                            };
                            match dispatcher.wait(cancel).await {
                                RemoteRetryWait::Ready => {
                                    app.invalidate_remote(scope.clone(), node, epoch).await;
                                    continue;
                                }
                                RemoteRetryWait::Cancelled => {
                                    break Err(anyhow::anyhow!("Durable Object call cancelled"));
                                }
                                RemoteRetryWait::Deadline => {}
                            }
                        }
                    }
                    if let Some(timing) = websocket_timing.as_ref() {
                        timing.emit(&app, &scope, request_id, "error", "remote", &node);
                    }
                    break Err(error);
                }
            }
        }
    };
    let result = operation.await;
    let _ = reply.send(result);
}

async fn dispatch_rpc_call(app: AppHandle, call: RpcCallReq) {
    let RpcCallReq {
        scope,
        name,
        method,
        args,
        reply,
    } = call;
    let result = async {
        anyhow::ensure!(
            celld_logic::cell::valid_cell_scope(&scope),
            "cell scope is malformed or exceeds the fleet storage limit"
        );
        let mut dispatcher = RemoteRouteRetry::new(app.operation_deadline_ms);
        loop {
            let Routed { request, route } = app
                .request(scope.clone())
                .await
                .map_err(|error| anyhow::anyhow!("route RPC {scope}: {error:?}"))?;
            let (node, addr, epoch, peer_protocol) = match route {
                Route::Local => {
                    let local_request_id = local_dispatch_request_id(None);
                    let _activity =
                        app.activity_for(request, scope.clone(), local_request_id, "local");
                    let outcome = app
                        .runtime
                        .as_ref()
                        .context("no cell runtime")?
                        .rpc(scope, name, method, args, local_request_id)
                        .await;
                    // Output gate (RPO=0): an RPC method that advanced the
                    // cell's write position has its reply held until the core
                    // proves the cell durable, exactly as fetch does, and so
                    // does a method that failed after it committed. The
                    // activity guard is still alive, so the cell stays pinned
                    // across the wait.
                    if let Some(ticket) = gate_ticket(&app, &outcome) {
                        if let Err(error) = app.gate_output(request, ticket).await {
                            return Err(gate_failure(error, outcome.err()));
                        }
                    }
                    return Ok(outcome?.data);
                }
                Route::Remote {
                    node,
                    addr,
                    epoch,
                    peer_protocol,
                } => (node, addr, epoch, peer_protocol),
            };
            if dispatcher.observe(&node, epoch) {
                match dispatcher.wait(None).await {
                    RemoteRetryWait::Ready => {
                        app.invalidate_remote(scope.clone(), node, epoch).await;
                        continue;
                    }
                    RemoteRetryWait::Deadline => {
                        anyhow::bail!("remote RPC owner remained stale until the retry deadline")
                    }
                    RemoteRetryWait::Cancelled => unreachable!("RPC route waits have no cancel"),
                }
            }
            anyhow::ensure!(
                peer_protocol == peer_auth::PROTOCOL_VERSION,
                "peer {node} speaks incompatible protocol {peer_protocol}"
            );
            let expects_structured_response = matches!(args, celld::js::RpcData::V8(_));
            {
                let response = peer_tunnel::rpc(
                    &peer_tunnel::Peer {
                        http: &app.peer_http,
                        auth: &app.peer_auth,
                        addr: &addr,
                        node: &node,
                    },
                    &scope,
                    name.as_deref(),
                    &method,
                    &args,
                )
                .await;
                let response = match response {
                    Ok(response) => response,
                    Err(error) => {
                        let attempt = classify_remote_attempt(&error);
                        let action = dispatcher.failed(attempt);
                        tracing::warn!(
                            event = "remote_route_retry",
                            %scope,
                            %node,
                            epoch,
                            ?attempt,
                            ?action,
                            surface = "rpc",
                            "a peer attempt changed the remote route retry state"
                        );
                        match action {
                            RemoteRetryAction::Stop => {}
                            RemoteRetryAction::Redispatch => {
                                app.invalidate_remote(scope.clone(), node, epoch).await;
                                continue;
                            }
                            RemoteRetryAction::WaitForOwner => match dispatcher.wait(None).await {
                                RemoteRetryWait::Ready => {
                                    app.invalidate_remote(scope.clone(), node, epoch).await;
                                    continue;
                                }
                                RemoteRetryWait::Deadline => {}
                                RemoteRetryWait::Cancelled => {
                                    unreachable!("RPC route waits have no cancel")
                                }
                            },
                        }
                        return Err(anyhow::anyhow!("remote RPC transport failed: {error:#}"));
                    }
                };
                if response
                    .headers()
                    .get(STALE_ROUTE_HEADER)
                    .is_some_and(|value| value == STALE_ROUTE_VALUE)
                {
                    let attempt = celld_logic::routing::Attempt::NotOwner;
                    let action = dispatcher.failed(attempt);
                    tracing::warn!(
                        event = "remote_route_retry",
                        %scope,
                        %node,
                        epoch,
                        ?attempt,
                        ?action,
                        surface = "rpc",
                        "a peer attempt changed the remote route retry state"
                    );
                    match action {
                        RemoteRetryAction::Stop => {}
                        RemoteRetryAction::Redispatch => {
                            app.invalidate_remote(scope.clone(), node, epoch).await;
                            continue;
                        }
                        RemoteRetryAction::WaitForOwner => match dispatcher.wait(None).await {
                            RemoteRetryWait::Ready => {
                                app.invalidate_remote(scope.clone(), node, epoch).await;
                                continue;
                            }
                            RemoteRetryWait::Deadline => {}
                            RemoteRetryWait::Cancelled => {
                                unreachable!("RPC route waits have no cancel")
                            }
                        },
                    }
                    anyhow::bail!("remote RPC owner was stale");
                }
                if response.status() == StatusCode::SERVICE_UNAVAILABLE
                    && response
                        .headers()
                        .get("x-celld-overload")
                        .is_some_and(|value| value == "cell")
                {
                    return Err(anyhow::Error::new(celld::js::CellOverloaded));
                }
                anyhow::ensure!(
                    response.status().is_success(),
                    "remote RPC failed with {}",
                    response.status()
                );
                let body = response
                    .into_body()
                    .collect()
                    .await
                    .map_err(|error| anyhow::anyhow!("remote RPC body: {error}"))?
                    .to_bytes();
                return Ok(if expects_structured_response {
                    celld::js::RpcData::V8(body)
                } else {
                    celld::js::RpcData::Json(String::from_utf8_lossy(&body).into_owned())
                });
            }
        }
    }
    .await;
    let _ = reply.send(result);
}

async fn request_payload(
    request: Request<Incoming>,
    trust_forwarded_headers: bool,
    max_body_bytes: usize,
) -> Result<(String, String, Vec<u8>, Vec<(String, String)>), HttpReply> {
    let (parts, body) = request.into_parts();
    let body = collect_limited_body(body, max_body_bytes)
        .await
        .map_err(|error| body_read_error("request", error))?;
    let headers = parts
        .headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_string()))
        })
        .collect();
    Ok((
        request_url(&parts, trust_forwarded_headers),
        parts.method.to_string(),
        body.to_vec(),
        headers,
    ))
}

/// A body at or above this many bytes reaches the Worker as a stream.
///
/// A small body crosses as bytes. The host makes one copy, and the handler
/// reads that copy with no asynchronous operation. A large body crosses as
/// a stream, because one copy of a large body costs more than the
/// operations that read it in parts. The host also streams a body that
/// declares no length, because the host cannot know the size of that body.
const INGRESS_STREAM_THRESHOLD: u64 = 1 << 20;
/// Peer fetches and RPC calls can carry the same application data as public
/// ingress. The frame needs room for its bounded metadata in addition to the
/// application body.
const MAX_PEER_FORWARD_BODY_BYTES: usize = DEFAULT_MAX_REQUEST_BODY_BYTES + (1 << 20);
/// Operator requests carry JSON commands, migration SQL, or workflow event
/// arguments. The 64 MiB ceiling leaves headroom above the 32 MiB D1 result
/// contract without giving an operator route the 1 GiB forwarding allowance.
const MAX_PEER_PROTOCOL_BODY_BYTES: usize = 64 << 20;
/// Control messages contain identifiers and counters, not application data.
const MAX_PEER_CONTROL_BODY_BYTES: usize = 64 << 10;
const INTERNAL_LOG_APPEND_PATH: &str = "/peer/log/append";
/// A streamed body crosses the isolate boundary through a string-only error
/// channel. Keep one private marker in that string so the HTTP entry point can
/// restore status 413; a generic stream error would turn the refusal into 500.
const BODY_LIMIT_STREAM_ERROR: &str = "celld request body limit exceeded";

enum BodyReadError {
    TooLarge,
    Invalid(String),
}

async fn collect_limited_body(body: Incoming, limit: usize) -> Result<Bytes, BodyReadError> {
    if body.size_hint().lower() > limit as u64 {
        return Err(BodyReadError::TooLarge);
    }
    Limited::new(body, limit)
        .collect()
        .await
        .map(|body| body.to_bytes())
        .map_err(|error| {
            if error
                .downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()
            {
                BodyReadError::TooLarge
            } else {
                BodyReadError::Invalid(error.to_string())
            }
        })
}

fn body_read_error(context: &str, error: BodyReadError) -> HttpReply {
    match error {
        BodyReadError::TooLarge => {
            response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large")
        }
        BodyReadError::Invalid(error) => response(
            StatusCode::BAD_REQUEST,
            format!("invalid {context} body: {error}"),
        ),
    }
}

fn request_body_limit_error(error: &anyhow::Error) -> bool {
    // The Worker runtime adds context around a host-stream failure, so inspect
    // the chain instead of requiring the marker to remain the top-level error.
    error
        .chain()
        .any(|cause| cause.to_string().contains(BODY_LIMIT_STREAM_ERROR))
}

fn cell_overload_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<celld::js::CellOverloaded>().is_some()
            // A Queue producer call crosses V8's promise boundary before the
            // public Worker response fails. The opaque text marker prevents an
            // application error from accidentally receiving the 503 contract.
            || cause
                .to_string()
                .contains(celld::js::CELL_OVERLOAD_ERROR_MARKER)
    })
}

fn cell_overload_response() -> HttpReply {
    let mut refused = response(
        StatusCode::SERVICE_UNAVAILABLE,
        "{\"error\":\"cell admission refused\"}",
    );
    refused.headers_mut().insert(
        hyper::header::RETRY_AFTER,
        hyper::header::HeaderValue::from_static("1"),
    );
    refused.headers_mut().insert(
        hyper::header::HeaderName::from_static("x-celld-overload"),
        hyper::header::HeaderValue::from_static("cell"),
    );
    refused
}

// The peer parser returns before a streamed body can cross its limit, so this
// error arrives from the Worker and must retain the ingress 413 contract.
fn internal_do_worker_error(error: anyhow::Error) -> HttpReply {
    if request_body_limit_error(&error) {
        return response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
    }
    if cell_overload_error(&error) {
        return cell_overload_response();
    }
    response(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("cell Worker failed: {error:#}"),
    )
}

/// Divide an ingress request into its metadata and a body that the Worker
/// can read. The function streams the body if one copy of the body costs
/// more than the operations that read it in parts.
async fn ingress_payload(
    request: Request<Incoming>,
    trust_forwarded_headers: bool,
    max_body_bytes: usize,
) -> Result<
    (
        String,
        String,
        celld::js::RequestBody,
        Vec<(String, String)>,
    ),
    HttpReply,
> {
    let declared = request
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared.is_some_and(|length| length > max_body_bytes as u64) {
        return Err(response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body too large",
        ));
    }
    let (url, method, body, headers, held) =
        ingress_payload_parts(request, trust_forwarded_headers, max_body_bytes).map_err(
            |error| {
                response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("request body stream: {error}"),
                )
            },
        )?;
    // The host collects a small body here. A read failure at this point
    // can still return status 400. A streamed body has no such point,
    // because that body fails while the handler reads it. The failure
    // surfaces there instead.
    let body = match held {
        None => body,
        Some(held) => match collect_limited_body(held, max_body_bytes).await {
            Ok(collected) => celld::js::RequestBody::Bytes(collected),
            Err(error) => return Err(body_read_error("request", error)),
        },
    };
    Ok((url, method, body, headers))
}

type IngressPayloadParts = (
    String,
    String,
    celld::js::RequestBody,
    Vec<(String, String)>,
    Option<Incoming>,
);

fn ingress_payload_parts(
    request: Request<Incoming>,
    trust_forwarded_headers: bool,
    max_body_bytes: usize,
) -> Result<IngressPayloadParts, String> {
    let (parts, body) = request.into_parts();
    let headers: Vec<(String, String)> = parts
        .headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_string()))
        })
        .collect();
    let url = request_url(&parts, trust_forwarded_headers);
    let method = parts.method.to_string();
    let declared = parts
        .headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    // A method that carries no body does not stream. There is nothing to
    // pull, and a stream costs the handler one operation to learn this.
    let bodyless =
        matches!(parts.method, hyper::Method::GET | hyper::Method::HEAD) || declared == Some(0);
    if bodyless || declared.is_some_and(|length| length < INGRESS_STREAM_THRESHOLD) {
        return Ok((
            url,
            method,
            celld::js::RequestBody::Bytes(Bytes::new()),
            headers,
            Some(body),
        ));
    }
    let chunks = Limited::new(body, max_body_bytes)
        .into_data_stream()
        .map(|chunk| {
            chunk.map(|bytes| bytes.to_vec()).map_err(|error| {
                if error
                    .downcast_ref::<http_body_util::LengthLimitError>()
                    .is_some()
                {
                    BODY_LIMIT_STREAM_ERROR.to_string()
                } else {
                    error.to_string()
                }
            })
        });
    let stream_id = celld::js::register_body_stream(Box::pin(chunks))?;
    Ok((
        url,
        method,
        celld::js::RequestBody::Stream(stream_id),
        headers,
        None,
    ))
}

/// The refusal every path arm gives a cell scope that fails the charset gate.
///
/// A scope taken from a URL segment reaches `db_path`, which joins it under the
/// data directory, and the replication client, which builds a bucket key from
/// it, so a scope carrying its own path segments walks out of both. The gate
/// itself is reified in `celld_logic::cell`, next to the peer-identity gate it
/// mirrors.
fn malformed_scope() -> HttpReply {
    response(
        StatusCode::BAD_REQUEST,
        "{\"error\":\"malformed_cell_scope\"}",
    )
}

/// Can celld put this host in `request.url`?
///
/// `celld_logic::http::authority_decision` returns the complete sans-IO
/// decision. A Punycode label is the one part that decision cannot settle,
/// because deciding it is the whole of IDNA: `xn--a` passes every other rule
/// and still makes `new URL(request.url)` throw inside a Worker.
///
/// So celld runs a parser only when the decision requires one. The parser is
/// the same `url::Url::parse` that backs `URL` in the isolate, therefore this
/// check agrees with the Worker by construction rather than by a rule celld
/// keeps in step by hand.
fn well_formed_host(host: &str) -> Option<&str> {
    match celld_logic::http::authority_decision(host) {
        celld_logic::http::AuthorityDecision::Reject => None,
        celld_logic::http::AuthorityDecision::Use(host) => Some(host),
        celld_logic::http::AuthorityDecision::NeedsUrlParser(host) => {
            url::Url::parse(&format!("http://{host}/"))
                .is_ok()
                .then_some(host)
        }
    }
}

/// `request.url` controls application routing and absolute links, so celld
/// does not let an untrusted forwarding header or request-target authority set
/// its scheme or host. The path and query always come from the request target.
/// The host comes from `Host`, and the scheme is `http` because celld does not
/// terminate TLS.
///
/// An operator can set `--trust-forwarded-headers` when a trusted proxy
/// replaces both forwarded headers. The trusted read takes the last value
/// because a proxy can append its value after a client-supplied value.
///
/// The host and the scheme are validated whatever their source. `Host` is
/// client-controlled whenever no proxy sits in front, which is the default, so
/// trusting its source is not the same as trusting its shape: an unchecked
/// value moves the path into a fragment or a query. `well_formed_host` is the
/// gate. A value that fails it is dropped rather than repaired, and the next
/// source in the chain applies.
/// A malformed `X-Forwarded-Host` therefore falls through to `Host`, and only
/// an empty chain reaches the fallback host. `Host` can still carry a client
/// value in that case, because a proxy often forwards it rather than replacing
/// it, so the fall-through trades a known-safe host for a well-formed one. The
/// gate is what makes that trade safe: neither value can reshape the URL, and
/// no hostname in `request.url` is trustworthy anyway.
fn request_url(parts: &hyper::http::request::Parts, trust_forwarded_headers: bool) -> String {
    // A header can arrive as several lines as well as one comma-joined line,
    // and `HeaderMap::get` returns only the first line. Select the actual final
    // field before validating it. Skipping an unusable final field would expose
    // an earlier client value again, which is the defect this gate prevents.
    let last_value = |name: &str| {
        parts
            .headers
            .get_all(name)
            .into_iter()
            .next_back()
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(',').next_back())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let first_value = |name: &str| {
        parts
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(',').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let forwarded = |name: &str| trust_forwarded_headers.then(|| last_value(name)).flatten();

    // The gate applies per source, not once at the end of the chain, so a
    // malformed forwarded value falls through to `Host` instead of discarding
    // the whole chain.
    let host = forwarded("x-forwarded-host")
        .and_then(well_formed_host)
        .or_else(|| first_value("host").and_then(well_formed_host))
        .unwrap_or("celld.local");
    let canonical = |scheme: &str| celld_logic::http::canonical_scheme(scheme);
    let scheme = forwarded("x-forwarded-proto")
        .and_then(canonical)
        .unwrap_or("http");
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", hyper::http::uri::PathAndQuery::as_str);
    format!("{scheme}://{host}{path_and_query}")
}

/// Send a fetch to a cell through the dispatcher a Durable Object call from
/// inside a Worker goes through.
///
/// Public ingress used to resolve the route itself and, on finding another
/// owner, answer with a 307 and a JSON description of where the cell lived --
/// with no Location header, so nothing could follow it. A fleet behind a load
/// balancer serves a cell only from the node that happens to own it, which is
/// to say it does not serve a fleet at all.
///
/// Going through `dispatch_do_call` also inherits the redispatch policy and
/// the cancellation channel, so a client that hangs up reaches the owner
/// rather than only the node it connected to. `/do/` and `/runtime/` share this:
/// they differ in what they authenticate, not in how they reach a cell.
async fn dispatch_cell_fetch(
    cell: String,
    name: Option<String>,
    url: String,
    method: String,
    body: celld::js::RequestBody,
    headers: Vec<(String, String)>,
) -> HttpReply {
    let preserve_representation_length = method.eq_ignore_ascii_case("HEAD");
    let (reply, receive) = oneshot::channel();
    let (cancel_tx, cancel) = oneshot::channel();
    let accepted = celld::js::submit_do_call(celld::js::DoCallReq {
        // Named, and named here: the abort fires only for a call that carries
        // both an id and a cancel signal, so leaving this None silently costs
        // the cancellation rather than failing.
        request_id: Some(celld::js::next_request_id()),
        cancel: Some(cancel),
        deliver_abort_to_handler: false,
        scope: cell,
        name,
        url,
        method,
        body_guard: celld::js::RequestBodyGuard::of(&body),
        body,
        headers,
        reply,
        // An ingress call has no caller in this process to be ordered against.
        order: None,
        // Cross-node trace propagation is not available here, so a direct-DO
        // ingress starts without a remote parent.
        parent: None,
    });
    if !accepted {
        return response(
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"error\":\"dispatcher unavailable\"}",
        );
    }
    let _hangup = HangUp(Some(cancel_tx));
    match receive.await {
        Ok(Ok(worker_response)) => {
            runtime_response(worker_response, preserve_representation_length)
        }
        Ok(Err(error)) if request_body_limit_error(&error) => {
            response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large")
        }
        Ok(Err(error)) if cell_overload_error(&error) => cell_overload_response(),
        Ok(Err(error)) => match error.downcast_ref::<RoutedRequestError>() {
            Some(error) => response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("{{\"error\":\"{:?}\"}}", error.0),
            ),
            None => response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("cell Worker failed: {error:#}"),
            ),
        },
        Err(_) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"error\":\"dispatcher dropped the call\"}",
        ),
    }
}

/// One forwarded cell fetch, after transport decode.
pub(crate) struct ForwardedFetch {
    pub(crate) name: Option<String>,
    pub(crate) url: String,
    pub(crate) method: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) request_id: Option<celld::js::RequestId>,
    pub(crate) capacity_handoff: bool,
}

pub(crate) enum ForwardedFetchOutcome {
    /// A plain application response, headers truthful — no sidecars.
    Reply(HttpReply),
    /// The cell accepted a WebSocket, already registered with the core.
    /// The transport decides how the 101 reaches the caller.
    WebSocket {
        target: celld::js::WsTarget,
        headers: Vec<(String, String)>,
    },
}

pub(crate) async fn dispatch_forwarded_fetch(
    app: AppHandle,
    scope: String,
    fetch: ForwardedFetch,
    request_body: celld::js::RequestBody,
) -> ForwardedFetchOutcome {
    let ForwardedFetch {
        name,
        url,
        method,
        headers,
        request_id,
        capacity_handoff,
    } = fetch;
    let _request_body_guard = celld::js::RequestBodyGuard::of(&request_body);
    let preserve_representation_length = method.eq_ignore_ascii_case("HEAD");
    let routed = if capacity_handoff {
        app.capacity_request(scope.clone()).await
    } else {
        app.request(scope.clone()).await
    };
    let result = match routed {
        Ok(Routed {
            request,
            route: Route::Local,
        }) => {
            let activity = app.activity_for(request, scope.clone(), request_id, "forwarded");
            let Some(runtime) = &app.runtime else {
                return ForwardedFetchOutcome::Reply(response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "no cell runtime",
                ));
            };
            let abort_scope = scope.clone();
            // The forwarding node hangs up when its own client does, so this
            // connection going away is the cancellation signal reaching the
            // owner -- and it arrives as a drop, which is why it is a guard
            // rather than the channel `fetch_cell` takes. Without it a handler
            // keeps running on the owner for a client that left the node it
            // dialled.
            let mut abort = AbortPeerFetchOnHangUp {
                runtime: runtime.clone(),
                scope: abort_scope,
                request_id,
                drain_pins: app.drain_pins.clone(),
                handler_active: true,
            };
            let result = runtime
                .fetch_cell(
                    scope,
                    name,
                    RuntimeFetch {
                        url,
                        method,
                        body: request_body,
                        headers,
                        request_id,
                        // A peer's call has no caller in this process, and the
                        // peer protocol does not carry trace context.
                        order: None,
                        parent: None,
                    },
                    None,
                )
                .await;
            // The handler has answered in both arms; a hang-up during the
            // gate wait below cancels the request's remaining work, not a
            // handler that is still running.
            abort.handler_answered();
            // Output gate (RPO=0): a peer-served handler that advanced the
            // cell's committed position holds its reply until the cell is
            // proven durable, exactly as the local dispatch path does, and so
            // does a handler that failed after it committed. This path used to
            // acknowledge unproven writes and could lose them during takeover.
            // The activity guard is still alive, so the request stays pinned
            // across the wait.
            if let Some(ticket) = gate_ticket(&app, &result) {
                activity.set_phase("output_gate", false, false);
                if let Some(position) = ticket.position {
                    activity.gate_started(position);
                }
                let gated = app.gate_output(request, ticket).await;
                activity.gate_finished(gated.is_ok());
                if let Err(error) = gated {
                    return ForwardedFetchOutcome::Reply(peer_response(response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "durability unproven: {error:?}{}",
                            handler_failure_detail(result.as_ref().err())
                        ),
                    )));
                }
            }
            match result {
                Ok(mut worker_response) => {
                    if let Some(HttpResponseWebSocket::Cell(target)) = &worker_response.websocket {
                        let kind = if celld::js::ws_hibernatable(target.id).unwrap_or(false) {
                            WebSocketKind::Hibernatable
                        } else {
                            WebSocketKind::Regular
                        };
                        if let Err(error) = app
                            .websocket_opened(target.scope.clone(), target.id, kind)
                            .await
                        {
                            return ForwardedFetchOutcome::Reply(peer_response(response(
                                StatusCode::SERVICE_UNAVAILABLE,
                                format!("WebSocket core registration failed: {error:#}"),
                            )));
                        }
                    }
                    if let Some(HttpResponseWebSocket::Cell(target)) =
                        worker_response.websocket.take()
                    {
                        abort.disarm();
                        drop(activity);
                        return ForwardedFetchOutcome::WebSocket {
                            target,
                            headers: worker_response.headers,
                        };
                    }
                    let body_active = worker_response.stream.is_some();
                    activity.set_phase("response_body", true, body_active);
                    if let Some(stream) = worker_response.stream.take() {
                        worker_response.stream =
                            Some(owner_response_stream(stream, activity, abort));
                    } else {
                        abort.disarm();
                        drop(activity);
                    }
                    runtime_response(worker_response, preserve_representation_length)
                }
                Err(error) => internal_do_worker_error(error),
            }
        }
        Ok(Routed {
            route: Route::Remote { .. },
            ..
        }) => {
            let mut stale = response(StatusCode::CONFLICT, "stale route");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            stale
        }
        Err(RequestError::CapacityExhausted) => {
            let mut stale = response(StatusCode::CONFLICT, "capacity exhausted");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            stale
        }
        Err(error) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("route failed: {error:?}"),
        ),
    };
    ForwardedFetchOutcome::Reply(peer_response(result))
}

async fn internal_abort(request: Request<Incoming>, app: AppHandle, path: String) -> HttpReply {
    let (parts, body) = request.into_parts();
    let body = match collect_limited_body(body, MAX_PEER_CONTROL_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("abort", error)),
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        let mut denied = response(error.status(), error.message());
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            denied.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
        }
        return peer_response(denied);
    }
    if parts.method != hyper::Method::POST {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let Some((encoded_scope, encoded_request)) = path
        .strip_prefix("/peer/abort/")
        .and_then(|rest| rest.rsplit_once('/'))
    else {
        return peer_response(response(StatusCode::BAD_REQUEST, "invalid abort target"));
    };
    let scope = match percent_encoding::percent_decode_str(encoded_scope).decode_utf8() {
        Ok(scope) => scope.into_owned(),
        Err(_) => return peer_response(response(StatusCode::BAD_REQUEST, "invalid abort scope")),
    };
    if !celld_logic::cell::valid_cell_scope(&scope) {
        return peer_response(malformed_scope());
    }
    let Some(request_id) = celld::js::parse_request_id(encoded_request) else {
        return peer_response(response(StatusCode::BAD_REQUEST, "invalid request id"));
    };
    let result = match &app.runtime {
        Some(runtime) => {
            // The signed target is the exact node session that received the
            // forwarded fetch. A cancellation must not route the cell again:
            // routing can activate work solely to cancel a request that never
            // arrived, and the request id is already process-wide.
            let phase = app
                .drain_pins
                .cancel_request(request_id, "peer_abort_received");
            if phase.is_none() || phase == Some("handler") {
                runtime.abort_fetch(&scope, request_id);
            }
            response(StatusCode::NO_CONTENT, Bytes::new())
        }
        None => response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime"),
    };
    peer_response(result)
}

pub(crate) async fn dispatch_forwarded_rpc(
    app: AppHandle,
    scope: String,
    name: Option<String>,
    rpc_method: String,
    args: celld::js::RpcData,
) -> HttpReply {
    let result = match app.request(scope.clone()).await {
        Ok(Routed {
            request,
            route: Route::Local,
        }) => {
            let local_request_id = local_dispatch_request_id(None);
            let _activity = app.activity_for(request, scope.clone(), local_request_id, "local");
            let Some(runtime) = &app.runtime else {
                return peer_response(response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime"));
            };
            // Output gate on the owner side, so a proxied RPC write is durable
            // before the calling node sees the reply -- the same rule the peer
            // fetch path follows.
            let outcome = runtime
                .rpc(scope, name, rpc_method, args, local_request_id)
                .await;
            if let Some(ticket) = gate_ticket(&app, &outcome) {
                if let Err(error) = app.gate_output(request, ticket).await {
                    return peer_response(response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "durability unproven: {error:?}{}",
                            handler_failure_detail(outcome.as_ref().err())
                        ),
                    ));
                }
            }
            outcome.map(|outcome| outcome.data)
        }
        Ok(Routed {
            route: Route::Remote { .. },
            ..
        }) => {
            let mut stale = response(StatusCode::CONFLICT, "stale route");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            return peer_response(stale);
        }
        Err(error) => Err(anyhow::anyhow!("route failed: {error:?}")),
    };
    match result {
        Ok(celld::js::RpcData::Json(json)) => peer_response(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(
                    Full::new(Bytes::from(json))
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
                .expect("RPC JSON response"),
        ),
        Ok(celld::js::RpcData::V8(bytes)) => peer_response(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/octet-stream")
                .body(
                    Full::new(bytes)
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
                .expect("RPC clone response"),
        ),
        Err(error) if cell_overload_error(&error) => peer_response(cell_overload_response()),
        Err(error) => peer_response(response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("RPC failed: {error:#}"),
        )),
    }
}

#[path = "main/peer_tunnel.rs"]
mod peer_tunnel;
#[path = "main/websocket.rs"]
mod websocket;
use websocket::{handle_websocket, outbound_websocket_task};

async fn handle_ingress(
    request: Request<Incoming>,
    app: AppHandle,
    connection: ConnectionWorkerRequests,
) -> HttpReply {
    if matches!(*request.method(), hyper::Method::GET | hyper::Method::HEAD) {
        // One snapshot for the asset decision and the Worker it may fall
        // into, so a deployment adopted mid-request cannot serve the new
        // generation's index with the old generation's Worker.
        let generation = app.runtime.as_ref().map(RuntimeManager::generation);
        if let Some(resolver) = generation.as_deref().and_then(Generation::ingress_assets) {
            let path = request.uri().path();
            if !resolver.should_run_worker_first(path) {
                let head = request.method() == hyper::Method::HEAD;
                match resolver
                    .ingress_response(path, request.uri().query(), head, request.headers())
                    .await
                {
                    Ok(Some(response)) => return asset_response(response),
                    Ok(None) if resolver.asset_only() => {
                        return response(StatusCode::NOT_FOUND, "Not found");
                    }
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!("celld asset response failed for {path}: {error:#}");
                        return response(
                            StatusCode::BAD_GATEWAY,
                            "Active deployment asset is unavailable",
                        );
                    }
                }
            }
        }
    }

    let (url, method, body, headers) = match ingress_payload(
        request,
        app.trust_forwarded_headers,
        app.max_request_body_bytes,
    )
    .await
    {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let preserve_representation_length = method.eq_ignore_ascii_case("HEAD");
    match app
        .fetch_worker(url, method, body, headers, connection)
        .await
    {
        Ok(worker_response) => runtime_response(worker_response, preserve_representation_length),
        Err(error) if request_body_limit_error(&error) => {
            response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large")
        }
        Err(error) if cell_overload_error(&error) => cell_overload_response(),
        Err(error) => match error.downcast_ref::<celld::pool::AdmitError>() {
            // Saturation is not a failure of the request. Answering it now
            // lets the caller retry or shed; holding the connection until its
            // own deadline is what a node with no capacity used to do.
            Some(refused @ celld::pool::AdmitError::Refused(_)) => response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Worker refused: {refused}"),
            ),
            // A build failure is a fault, not saturation.
            Some(celld::pool::AdmitError::Build(_)) | None => response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Worker failed: {error:#}"),
            ),
        },
    }
}

async fn dispatch_asset_call(app: AppHandle, call: AssetCallReq) {
    let generation = app
        .runtime
        .as_ref()
        .map(|runtime| runtime.generation_by_id(call.generation));
    let response = match generation.as_deref().and_then(|g| g.assets(&call.script)) {
        Some(resolver) => {
            resolver
                .binding_response(&call.url, &call.method, &call.headers)
                .await
        }
        None => Err(anyhow::anyhow!(
            "no asset resolver for script {}",
            call.script
        )),
    };
    let _ = call.reply.send(response);
}

async fn dispatch_service_call(app: AppHandle, call: SvcCallReq) {
    // The target request owns a streamed body once it starts. Keep a fallback
    // guard here as well, so a missing runtime or a cancellation before
    // admission cannot leave the source in the process registry.
    let mut body_guard = call.body_guard;
    let response = match &app.runtime {
        Some(runtime) => {
            runtime
                .fetch_service(ServiceFetch {
                    generation: call.generation,
                    script: call.script,
                    url: call.url,
                    method: call.method,
                    body: call.body,
                    headers: call.headers,
                    cancel: call.cancel,
                })
                .await
        }
        None => Err(anyhow::anyhow!("no Worker runtime")),
    };
    if response.is_ok() {
        // A successful response proves that the target installed its request
        // context. Keep an unread tail there until its waitUntil work ends.
        body_guard.disarm();
    }
    let _ = call.reply.send(response);
}

async fn dispatch_service_rpc(app: AppHandle, call: SvcRpcReq) {
    let response = match &app.runtime {
        Some(runtime) => {
            runtime
                .rpc_service(
                    call.generation,
                    &call.script,
                    call.entrypoint,
                    call.method,
                    call.args,
                )
                .await
        }
        None => Err(anyhow::anyhow!("no Worker runtime")),
    };
    let _ = call.reply.send(response);
}

async fn dispatch_queue_batch(app: AppHandle, call: QueueDispatchReq) {
    let QueueDispatchReq {
        scope,
        generation,
        script,
        lease_id,
        leases,
        batch,
    } = call;
    let queue = batch.queue.clone();
    let result = match &app.runtime {
        Some(runtime) => runtime.queue_service(generation, &script, batch).await,
        None => Err(anyhow::anyhow!("no Worker runtime")),
    };
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(%scope, %queue, %script, %error, "Queue dispatch failed; its lease will expire");
            return;
        }
    };
    let outcome = match result.outcome {
        celld::js::QueueOutcome::Ok => "ok",
        celld::js::QueueOutcome::Exception => "exception",
    };
    let body = serde_json::json!({
        "leaseId": lease_id,
        "leases": leases,
        "outcome": outcome,
        "error": result.error,
        "ackAll": result.ack_all,
        "retryBatch": {
            "retry": result.retry_batch.retry,
            "delaySeconds": result.retry_batch.delay_seconds,
        },
        "explicitAcks": result.explicit_acks,
        "retryMessages": result.retry_messages.into_iter().map(|retry| serde_json::json!({
            "msgId": retry.msg_id,
            "delaySeconds": retry.delay_seconds,
        })).collect::<Vec<_>>(),
    });
    let body = celld::js::RequestBody::Bytes(body.to_string().into_bytes().into());
    let (reply, receive) = oneshot::channel();
    dispatch_do_call(
        app,
        DoCallReq {
            request_id: None,
            cancel: None,
            deliver_abort_to_handler: false,
            scope: scope.clone(),
            name: None,
            url: "https://queue.celld.internal/__qSettle".to_string(),
            method: "POST".to_string(),
            body_guard: celld::js::RequestBodyGuard::of(&body),
            body,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            reply,
            order: None,
            parent: None,
        },
    )
    .await;
    match receive.await {
        Ok(Ok(response)) if (200..300).contains(&response.status) => {}
        Ok(Ok(response)) => tracing::warn!(
            %scope,
            %queue,
            status = response.status,
            "Queue settlement was refused; its lease will expire"
        ),
        Ok(Err(error)) => tracing::warn!(
            %scope,
            %queue,
            %error,
            "Queue settlement routing failed; its lease will expire"
        ),
        Err(error) => tracing::warn!(
            %scope,
            %queue,
            %error,
            "Queue settlement dispatcher dropped; its lease will expire"
        ),
    }
}

async fn internal_probe(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let (parts, body) = request.into_parts();
    let body = match collect_limited_body(body, MAX_PEER_CONTROL_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("probe", error)),
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    if parts.method != hyper::Method::GET {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let Some(challenge) = parts
        .headers
        .get("x-cells-probe-challenge")
        .and_then(|value| value.to_str().ok())
    else {
        return peer_response(response(StatusCode::BAD_REQUEST, "missing probe challenge"));
    };
    match celld::peer_probe::respond(app.peer_auth.source(), &app.advertise, challenge) {
        Ok(probe) => match serde_json::to_vec(&probe) {
            Ok(body) => peer_response(response(StatusCode::OK, body)),
            Err(_) => peer_response(response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "encode probe response",
            )),
        },
        Err(_) => peer_response(response(StatusCode::BAD_REQUEST, "invalid probe challenge")),
    }
}

/// Adopt one released cell for a terminating peer. A successful response
/// proves that this node acquired the next ownership epoch. The restored
/// runtime remains dormant until real traffic needs it. If another peer won
/// the race, the response identifies that current owner.
async fn internal_handoff(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    if app.is_draining() {
        return peer_response(response(
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"ok\":false,\"draining\":true}",
        ));
    }
    let (parts, body) = request.into_parts();
    let body = match collect_limited_body(body, MAX_PEER_CONTROL_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("handoff", error)),
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    if parts.method != hyper::Method::POST {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let request: HandoffRequest = match serde_json::from_slice::<HandoffRequest>(&body) {
        Ok(request) if !request.cell.is_empty() && request.released_epoch > 0 => request,
        Ok(_) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                "invalid handoff cell or epoch",
            ));
        }
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid handoff JSON: {error}"),
            ));
        }
    };
    let mut attempt = app
        .accept_handoff(request.cell.clone(), request.released_epoch)
        .await;
    // Concurrent traffic commonly leaves this successor with a cached route
    // to the donor. The signed release says that exact generation is stale.
    // Retire it and resolve the owner record again; otherwise the successor
    // redirects the donor back to itself forever and no cell is adopted.
    if let Ok(HandoffAttempt::CurrentOwner(owner)) = &attempt {
        if owner.epoch <= request.released_epoch {
            app.invalidate_remote(request.cell.clone(), owner.node.clone(), owner.epoch)
                .await;
            attempt = app
                .accept_handoff(request.cell.clone(), request.released_epoch)
                .await;
        }
    }
    let (status, result) = match attempt {
        Ok(HandoffAttempt::Accepted(owner)) if owner.epoch > request.released_epoch => (
            StatusCode::OK,
            HandoffResponse {
                node: owner.node,
                addr: owner.addr,
                epoch: owner.epoch,
                peer_protocol: owner.peer_protocol,
                published: true,
            },
        ),
        Ok(HandoffAttempt::CurrentOwner(owner)) => (
            StatusCode::CONFLICT,
            HandoffResponse {
                node: owner.node,
                addr: owner.addr,
                epoch: owner.epoch,
                peer_protocol: owner.peer_protocol,
                published: false,
            },
        ),
        Ok(HandoffAttempt::Accepted(_)) | Ok(HandoffAttempt::Refused) | Err(_) => {
            return peer_response(response(
                StatusCode::SERVICE_UNAVAILABLE,
                "handoff capacity unavailable",
            ));
        }
    };
    match serde_json::to_vec(&result) {
        Ok(body) => peer_response(response(status, body)),
        Err(_) => peer_response(response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "encode handoff response",
        )),
    }
}

/// The log tier's follower endpoints (crate::node_log): append fsyncs
/// entries and answers the ack-all vote, seal persists the fence mark
/// before replying, tail hands recovery the held fragment. Fleet-HMAC
/// verified like every internal peer surface.
async fn internal_log(request: Request<Incoming>, app: AppHandle, path: String) -> HttpReply {
    let Some(follower) = app.follower.clone() else {
        return peer_response(response(StatusCode::NOT_FOUND, "no follower store"));
    };
    let entered = celld::asyncrt::mono_us();
    let (mut parts, body) = request.into_parts();
    let body_limit = if path == INTERNAL_LOG_APPEND_PATH {
        // An append includes the LTX bytes from an application transaction,
        // so it needs the same allowance as other application-data routes.
        MAX_PEER_FORWARD_BODY_BYTES
    } else {
        MAX_PEER_CONTROL_BODY_BYTES
    };
    let body = match collect_limited_body(body, body_limit).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("log", error)),
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    if parts.method != hyper::Method::POST {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    if path == "/peer/log/stream" {
        // The ordered append stream (the ordered-transport design):
        // the handshake is fleet-HMAC verified above like every log route,
        // the 101 carries the response auth the leader validates before it
        // upgrades, and the upgraded duplex is served by one reader so
        // apply order equals arrival order.
        let Some(upgrade) = parts.extensions.remove::<hyper::upgrade::OnUpgrade>() else {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                "stream requires an upgradable connection",
            ));
        };
        let store = follower.clone();
        celld::asyncrt::spawn(async move {
            match upgrade.await {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    if let Err(error) = celld::node_log::serve_log_stream(io, store).await {
                        tracing::debug!(%error, "log stream ended");
                    }
                }
                Err(error) => tracing::debug!(%error, "log stream upgrade failed"),
            }
        })
        .detach();
        let mut reply = response(StatusCode::SWITCHING_PROTOCOLS, "");
        reply.headers_mut().insert(
            hyper::header::UPGRADE,
            hyper::header::HeaderValue::from_static("celld-log-stream"),
        );
        reply.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("upgrade"),
        );
        return peer_response(reply);
    }
    let result: anyhow::Result<Vec<u8>> = match path.as_str() {
        // The append body and the tail response are binary — the entries
        // dominate both — while every control message stays JSON.
        INTERNAL_LOG_APPEND_PATH => match celld::node_log::decode_append(&body) {
            Ok(req) => {
                let collected = celld::asyncrt::mono_us();
                let resp = follower.append(req).await;
                tracing::info!(
                    event = "log_append_router",
                    collect_ms = collected.saturating_sub(entered) / 1000,
                    append_ms = (celld::asyncrt::mono_us() - collected) / 1000,
                    body_len = body.len(),
                    "append routed"
                );
                serde_json::to_vec(&resp).map_err(Into::into)
            }
            Err(error) => Err(error),
        },
        "/peer/log/seal" => match serde_json::from_slice::<celld::node_log::SealReq>(&body) {
            Ok(req) => match follower.seal(&req).await {
                Ok(resp) => serde_json::to_vec(&resp).map_err(Into::into),
                Err(error) => Err(error),
            },
            Err(error) => Err(error.into()),
        },
        "/peer/log/tail" => serde_json::from_slice::<celld::node_log::TailReq>(&body)
            .map_err(anyhow::Error::from)
            .map(|req| celld::node_log::encode_tail_resp(&follower.tail(&req))),
        _ => Err(anyhow::anyhow!("unknown log endpoint")),
    };
    match result {
        Ok(body) => peer_response(response(StatusCode::OK, body)),
        Err(error) => peer_response(response(StatusCode::BAD_REQUEST, format!("{error:#}"))),
    }
}

async fn handle_public(
    request: Request<Incoming>,
    app: AppHandle,
    connection: ConnectionWorkerRequests,
) -> Result<HttpReply, Infallible> {
    let path = request.uri().path().to_string();
    let draining = app.is_draining();
    let _public_admission = if path == "/.well-known/celld/health" {
        None
    } else {
        let Some(admission) = app.admit_public() else {
            let mut refused = response(
                StatusCode::SERVICE_UNAVAILABLE,
                "{\"ok\":false,\"draining\":true}",
            );
            refused.headers_mut().insert(
                hyper::header::RETRY_AFTER,
                hyper::header::HeaderValue::from_static("1"),
            );
            refused.headers_mut().insert(
                hyper::header::CONNECTION,
                hyper::header::HeaderValue::from_static("close"),
            );
            return Ok(refused);
        };
        Some(admission)
    };
    if path != "/.well-known/celld/health"
        && app.runtime.is_some()
        && fastwebsockets::upgrade::is_upgrade_request(&request)
    {
        return Ok(handle_websocket(request, app).await);
    }
    let mut result = match path.as_str() {
        "/.well-known/celld/health"
            if !app.is_draining() && app.fleet_ready() && app.healthy().await =>
        {
            response(StatusCode::OK, "{\"ok\":true}")
        }
        "/.well-known/celld/health" => response(StatusCode::SERVICE_UNAVAILABLE, "{\"ok\":false}"),
        _ if app.runtime.is_some() => handle_ingress(request, app, connection).await,
        _ => response(StatusCode::NOT_FOUND, "{\"error\":\"not_found\"}"),
    };
    if draining {
        result.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("close"),
        );
    }
    Ok(result)
}

async fn handle_internal(
    request: Request<Incoming>,
    app: AppHandle,
    shutdown: mpsc::UnboundedSender<ShutdownMode>,
) -> Result<HttpReply, Infallible> {
    let path = request.uri().path().to_string();
    let draining = app.is_draining();
    let result = match path.as_str() {
        "/peer/probe" => internal_probe(request, app).await,
        "/peer/handoff" => internal_handoff(request, app).await,
        _ if path.starts_with("/peer/log/") => internal_log(request, app, path.clone()).await,
        "/state" => response(StatusCode::OK, app.snapshot().await),
        "/reload" if request.method() != hyper::Method::POST => {
            response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        }
        "/reload" => internal_reload(app).await,
        "/rebalance/pause" | "/rebalance/resume" if request.method() != hyper::Method::POST => {
            response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        }
        "/rebalance/pause" | "/rebalance/resume" => {
            internal_rebalance_switch(path == "/rebalance/pause")
        }
        "/shutdown" if request.method() != hyper::Method::POST => {
            response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        }
        "/shutdown" => {
            let preserve_ownership = request
                .uri()
                .query()
                .is_some_and(|query| query.split('&').any(|part| part == "handoff=preserve"));
            let mode = if preserve_ownership {
                ShutdownMode::Preserve
            } else {
                ShutdownMode::Handoff
            };
            let _ = shutdown.send(mode);
            response(StatusCode::OK, "{\"ok\":true}")
        }
        _ if path.starts_with("/peer/abort/") && app.runtime.is_some() => {
            internal_abort(request, app, path).await
        }
        "/peer/tunnel" if app.runtime.is_some() => {
            if peer_tunnel::is_tunnel_request(&request) {
                peer_tunnel::accept(request, app)
            } else {
                peer_response(response(
                    StatusCode::BAD_REQUEST,
                    "tunnel establishment must upgrade",
                ))
            }
        }
        // The operator CLIs' way in (`celld d1`, queues, workflows): the same
        // forwarding dispatch `/do/` uses, with the fleet authentication
        // `/do/` does not have. A runtime class holds application data and
        // answers an operator protocol — arbitrary SQL for D1 — so it must
        // not be reachable from an unauthenticated route; `/do/` refuses a
        // reserved scope below and sends the caller here.
        //
        // It is one route rather than one per class on purpose. D1's design
        // page records what writing the second by hand would cost: the
        // refusal on the *other* route has to be widened in the same change,
        // and gating only the new route "would have left the hole open".
        // With one entrance and one refusal, a class added to
        // `RESERVED_CLASSES` inherits both. (The pre-v5 `/__d1/` alias died
        // with the v0.4.0 stop-the-world upgrade; the CLI in this tree sends
        // `/runtime/`.)
        _ if path.starts_with("/runtime/") && app.runtime.is_some() => {
            let scope = path
                .strip_prefix("/runtime/")
                .expect("matched the prefix")
                .to_string();
            if !celld_logic::cell::valid_cell_scope(&scope) {
                return Ok(peer_response(malformed_scope()));
            }
            // The mirror of `/do/`'s refusal: that route serves every ordinary
            // Durable Object and no reserved one, this route serves the
            // reserved ones and nothing else. Without the check, a signed
            // request could drive a user's Durable Object through a route
            // whose only contract is an operator protocol.
            if !celld::deploy::is_reserved_scope(&scope) {
                return Ok(peer_response(response(
                    StatusCode::FORBIDDEN,
                    "{\"error\":\"only a runtime class is served on /runtime/; use /do/ for a Durable Object\"}",
                )));
            }
            let operator_query = request.uri().query().map(str::to_owned);
            let method = request.method().clone();
            let path_and_query = request
                .uri()
                .path_and_query()
                .map_or_else(|| request.uri().path().to_string(), ToString::to_string);
            let request_headers = request.headers().clone();
            let body = match collect_limited_body(request.into_body(), MAX_PEER_PROTOCOL_BODY_BYTES)
                .await
            {
                Ok(body) => body,
                Err(error) => return Ok(peer_response(body_read_error("operator cell", error))),
            };
            if let Err(error) = app.peer_auth.verify(
                &method,
                &path_and_query,
                &request_headers,
                &body,
                app.peer_auth.source(),
            ) {
                return Ok(peer_response(response(error.status(), error.message())));
            }
            let mut name = None;
            for (key, value) in url::form_urlencoded::parse(
                operator_query.as_deref().unwrap_or_default().as_bytes(),
            ) {
                if key != "name" || name.is_some() || value.is_empty() {
                    return Ok(peer_response(response(
                        StatusCode::BAD_REQUEST,
                        "{\"error\":\"invalid operator query\"}",
                    )));
                }
                name = Some(value.into_owned());
            }
            // A Queue needs its name before its constructor chooses a
            // consumer. The runtime also proves that this name hashes to the
            // requested scope before it exposes the identity to JavaScript.
            dispatch_cell_fetch(
                scope,
                name,
                "http://cell/".to_string(),
                "POST".to_string(),
                celld::js::RequestBody::Bytes(body),
                vec![("content-type".to_string(), "application/json".to_string())],
            )
            .await
        }
        _ if path.starts_with("/do/") && app.runtime.is_some() => {
            let runtime = app.runtime.as_ref().expect("checked runtime");
            let cell = match runtime.cell_scope(&path[4..]) {
                Ok(cell) => cell,
                Err(error) => {
                    return Ok(response(StatusCode::BAD_REQUEST, format!("{error:#}")));
                }
            };
            // This route has no authentication, so it must reach no reserved
            // class at all. A reserved cell's whole fetch surface is an
            // operator protocol — arbitrary SQL for D1, create/terminate/
            // events for a workflow — and its scope is an HMAC over names that
            // sit in the project's config rather than a secret.
            //
            // One question, not one per class. Two hand-written refusals stood
            // here before, and a third reserved class would have needed a third
            // that nothing forces anyone to write. `is_reserved_scope` closes
            // the door for every class in `RESERVED_CLASSES`; the hint below
            // is the only part that is per-class, and a class without one still
            // gets refused.
            if let Some(class) = celld::deploy::reserved_class_of(&cell) {
                let hint = celld::deploy::operator_hint(class);
                return Ok(response(
                    StatusCode::FORBIDDEN,
                    format!(
                        "{{\"error\":\"{class} is a runtime class and is not reachable over /do/; {hint}\"}}"
                    ),
                ));
            }
            let (url, method, body, headers) = match ingress_payload(
                request,
                app.trust_forwarded_headers,
                app.max_request_body_bytes,
            )
            .await
            {
                Ok(payload) => payload,
                Err(response) => return Ok(response),
            };
            dispatch_cell_fetch(cell, None, url, method, body, headers).await
        }
        _ if path.starts_with("/cell/") && !celld_logic::cell::valid_cell_scope(&path[6..]) => {
            malformed_scope()
        }
        _ if path.starts_with("/cell/") => {
            let cell = path[6..].to_string();
            match app.request(cell.clone()).await {
                Ok(Routed {
                    request,
                    route: Route::Local,
                }) => {
                    let _activity = app.activity(request, cell.clone());
                    response(
                        StatusCode::OK,
                        format!("{{\"route\":\"local\",\"cell\":{cell:?}}}"),
                    )
                }
                Ok(Routed {
                    route:
                        Route::Remote {
                            node,
                            addr,
                            epoch,
                            peer_protocol,
                        },
                    ..
                }) => response(
                    StatusCode::TEMPORARY_REDIRECT,
                    format!(
                        "{{\"route\":\"remote\",\"node\":{node:?},\"addr\":{addr:?},\"epoch\":{epoch},\"peer_protocol\":{peer_protocol}}}"
                    ),
                ),
                Err(error) => response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("{{\"error\":\"{error:?}\"}}"),
                ),
            }
        }
        _ if path.starts_with("/evict/") && !celld_logic::cell::valid_cell_scope(&path[7..]) => {
            malformed_scope()
        }
        _ if path.starts_with("/evict/") => {
            app.evict(path[7..].to_string()).await;
            response(StatusCode::OK, "{\"ok\":true}")
        }
        _ => response(StatusCode::NOT_FOUND, "{\"error\":\"not_found\"}"),
    };
    // Close the connection behind any response sent while draining, so
    // keep-alive clients reconnect to a healthy node and the drain loop
    // can finish. A request that raced the drain flag converges on its
    // next request, which hits the gate above.
    let mut result = result;
    // A 101 is exempt: `Connection: close` on it breaks the upgrade, and a
    // tunnel establishment during a drain is exactly how a draining owner
    // keeps serving the calls its handoff has not yet moved. The upgraded
    // connection leaves keep-alive rotation on its own when the call ends.
    if draining && result.status() != StatusCode::SWITCHING_PROTOCOLS {
        result.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("close"),
        );
    }
    Ok(result)
}

#[derive(Clone, Copy)]
enum HttpSurface {
    Public,
    Internal,
    Recovery,
}

impl HttpSurface {
    fn name(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Recovery => "recovery",
        }
    }
}

struct ShutdownConnection {
    stream: tokio::net::TcpStream,
    surface: HttpSurface,
    received: oneshot::Sender<()>,
    #[cfg(all(test, celld_internal_tests))]
    recovered_after_error: bool,
}

fn spawn_shutdown_acceptor(
    listener: tokio::net::TcpListener,
    surface: HttpSurface,
    connections: mpsc::Sender<ShutdownConnection>,
    #[cfg(all(test, celld_internal_tests))] mut fail_next_for_test: bool,
) {
    // Shutdown selects finish whenever a timer, request, or actor message
    // wins. Keep each listener's accept and retry waits in one task so those
    // unrelated events cannot cancel a wait or reset its retry delay.
    tokio::spawn(async move {
        let mut retry_delay = SHUTDOWN_ACCEPT_RETRY_INITIAL;
        let mut recovering = false;
        #[cfg(all(test, celld_internal_tests))]
        let mut connect_after_failure_for_test = false;
        loop {
            #[cfg(all(test, celld_internal_tests))]
            let inject_failure = std::mem::take(&mut fail_next_for_test);
            #[cfg(all(test, celld_internal_tests))]
            let accepted = if inject_failure {
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "injected listener accept failure during shutdown",
                ))
            } else if std::mem::take(&mut connect_after_failure_for_test) {
                let retry_address = listener
                    .local_addr()
                    .expect("read the shutdown accept failure test listener address");
                // Connect before polling accept, so the injected transition
                // does not depend on an operating-system readiness-
                // notification race after the error.
                let connection = std::net::TcpStream::connect(retry_address)
                    .expect("connect after the injected shutdown accept failure");
                *SHUTDOWN_ACCEPT_FAILURE_CLIENT
                    .lock()
                    .expect("lock the shutdown accept failure test connection") = Some(connection);
                listener.accept().await
            } else {
                listener.accept().await
            };
            #[cfg(not(all(test, celld_internal_tests)))]
            let accepted = listener.accept().await;
            match accepted {
                Ok((stream, _)) => {
                    if recovering {
                        tracing::info!(
                            event = "shutdown_accept_recovered",
                            surface = surface.name(),
                            "accepted {} connection after a shutdown accept error",
                            surface.name()
                        );
                    }
                    let (received, wait_until_received) = oneshot::channel();
                    let connection = ShutdownConnection {
                        stream,
                        surface,
                        received,
                        #[cfg(all(test, celld_internal_tests))]
                        recovered_after_error: recovering,
                    };
                    if connections.send(connection).await.is_err() {
                        return;
                    }
                    let _ = wait_until_received.await;
                    retry_delay = SHUTDOWN_ACCEPT_RETRY_INITIAL;
                    recovering = false;
                }
                Err(error) => {
                    tracing::warn!(
                        event = "shutdown_accept_failed",
                        surface = surface.name(),
                        retry_ms = retry_delay.as_millis(),
                        %error,
                        "could not accept {} connection during shutdown; retrying",
                        surface.name()
                    );
                    #[cfg(all(test, celld_internal_tests))]
                    if inject_failure {
                        shutdown_accept_failure_test_checkpoint(true, "listener failure observed");
                        connect_after_failure_for_test = true;
                    }
                    recovering = true;
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = retry_delay.saturating_mul(2).min(SHUTDOWN_ACCEPT_RETRY_MAX);
                }
            }
        }
    });
}

fn serve_http_connection(
    stream: tokio::net::TcpStream,
    surface: HttpSurface,
    app: AppHandle,
    shutdown: mpsc::UnboundedSender<ShutdownMode>,
    mut connection_drain: watch::Receiver<bool>,
    connection_grace: std::time::Duration,
) -> ConnectionFuture {
    // Nagle + delayed-ACK stalls small request/response exchanges by tens
    // of milliseconds; the log tier's append acks measured it directly (a
    // ~70 ms floor on a sub-millisecond VPC path). Responses must leave
    // when written, on every surface.
    let _ = stream.set_nodelay(true);
    Box::pin(async move {
        let peer = stream.peer_addr().ok();
        // Serve on the runtime, not on this task. `main` drives its loop with
        // `block_on`, so serving there put every connection on one core.
        // Awaiting the spawned task keeps shutdown tracking unchanged.
        let served = tokio::spawn(async move {
            let connection_requests = ConnectionWorkerRequests::default();
            let service_requests = connection_requests.clone();
            let service = service_fn(move |request| {
                let app = app.clone();
                let shutdown = shutdown.clone();
                let service_requests = service_requests.clone();
                async move {
                    match surface {
                        HttpSurface::Public => handle_public(request, app, service_requests).await,
                        HttpSurface::Internal => handle_internal(request, app, shutdown).await,
                        HttpSurface::Recovery => {
                            let path = request.uri().path().to_string();
                            Ok(match path.as_str() {
                                "/peer/log/seal" | "/peer/log/tail" => {
                                    internal_log(request, app, path).await
                                }
                                _ => response(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    "predecessor recovery in progress",
                                ),
                            })
                        }
                    }
                }
            });
            let connection = http1::Builder::new()
                // Reclaim a connection that never sends a complete request
                // head. The timeout also bounds an idle keep-alive waiting
                // for its next request.
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(std::time::Duration::from_secs(30))
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades();
            tokio::pin!(connection);
            let result = celld::asyncrt::select! {
                result = &mut connection => Some(result),
                _ = connection_drain.changed() => {
                    connection.as_mut().graceful_shutdown();
                    tokio::time::timeout(connection_grace, &mut connection)
                        .await
                        .ok()
                }
            };
            connection_requests.abort_all();
            match result {
                Some(Err(error)) => record_connection_error(&error, surface, peer),
                None => tracing::warn!(
                    event = "connection_drain_forced",
                    grace_ms = connection_grace.as_millis(),
                    "forced an HTTP connection closed after its graceful drain"
                ),
                Some(Ok(())) => {}
            }
        });
        let _ = served.await;
    })
}

/// Serve the surviving follower disks before recovering this node's own
/// predecessor. After a correlated restart, every predecessor can need a
/// fragment on another restarting node. Waiting to accept until recovery
/// finishes makes those intact disks look lost and seals out acked writes.
/// Only authenticated seal and tail requests run here: the actor has no
/// lease yet, so neither application work nor new log appends can run.
async fn recover_with_follower_listener(
    listener: &tokio::net::TcpListener,
    app: AppHandle,
    recovery: impl std::future::Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let (drain_tx, drain_rx) = watch::channel(false);
    let (shutdown_tx, _shutdown_rx) = mpsc::unbounded_channel();
    let mut connections: FuturesUnordered<ConnectionFuture> = FuturesUnordered::new();
    tokio::pin!(recovery);
    let result = loop {
        celld::asyncrt::select! {
            result = &mut recovery => break result,
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => connections.push(serve_http_connection(
                        stream,
                        HttpSurface::Recovery,
                        app.clone(),
                        shutdown_tx.clone(),
                        drain_rx.clone(),
                        CONNECTION_DRAIN_GRACE,
                    )),
                    Err(error) => break Err(error.into()),
                }
            }
            Some(()) = connections.next(), if !connections.is_empty() => {}
        }
    };
    // Retire the recovery-only keep-alives on either outcome, so no stale
    // service survives into normal operation or an unsuccessful boot.
    let _ = drain_tx.send(true);
    while connections.next().await.is_some() {}
    result
}
#[path = "main/cli.rs"]
mod cli;
use cli::{action_from_process, print_help, worker_loader_binding, Action};

#[cfg(all(test, celld_internal_tests))]
fn shutdown_accept_failure_test_action() -> Action {
    let listen = std::env::var("CELLD_DRAIN_ACCEPT_FAILURE_LISTEN")
        .map(celld::startup::Listen::Explicit)
        .unwrap_or(celld::startup::Listen::LoopbackEphemeral);
    Action::Run(cli::Settings {
        control_plane: false,
        bucket: None,
        load_deployment: false,
        endpoint: None,
        region: "test".to_string(),
        listen,
        internal_listen: celld::startup::Listen::LoopbackEphemeral,
        advertise: None,
        unsafe_public_advertise: false,
        trust_forwarded_headers: false,
        storage_probe: false,
        dev_store: None,
    })
}

fn node_bucket(
    settings: &cli::Settings,
    managed: Option<&celld::control_plane::ManagedStorageConfig>,
    lease: bool,
) -> anyhow::Result<celld::bucket::Bucket> {
    if let Some(database) = &settings.dev_store {
        anyhow::ensure!(
            managed.is_none(),
            "a development node cannot use managed storage"
        );
        return celld::dev::open_local_bucket(database);
    }
    let bucket = settings
        .bucket
        .as_deref()
        .context("the node has no fleet bucket")?;
    if lease {
        fleet::lease_bucket_client_with_credentials(
            bucket,
            settings.endpoint.as_deref(),
            &settings.region,
            managed,
        )
    } else {
        fleet::bucket_client_with_credentials(
            bucket,
            settings.endpoint.as_deref(),
            &settings.region,
            managed,
        )
    }
}

/// celld's own default. Evictions are bounded far tighter than activations
/// because each one carries a durability proof, and a node that lets its whole
/// working set prove durability at once turns a walk down into a thundering
/// herd against the bucket.
const DEFAULT_MAX_CONCURRENT_EVICTIONS: usize = 4;
/// The shutdown handoff parks the successor dormant, so the batch does not
/// start restore work there. The fleet drain token also admits one donor at a
/// time. Keep enough ownership operations in flight to overlap object-store
/// latency: a width of 8 evacuated only about 110 cells inside the default
/// 40-second process deadline.
const DEFAULT_MAX_CONCURRENT_RELEASES: usize = 128;
/// The actor refreshes the counts a node lease publishes once per sample.
const LOAD_SAMPLE_PERIOD: std::time::Duration = std::time::Duration::from_secs(1);
/// How often a node with balancing off samples the fleet leases for the
/// bucket-format gate: the balancing default, one shared fleet sample.
const FORMAT_GATE_SAMPLE_MS: u64 = 5_000;
/// One lease renewal plus one load sample bound how stale a peer's count is,
/// so a batch every five seconds sees its predecessor before it plans.
const DEFAULT_REBALANCE_INTERVAL_MS: u64 = 5_000;
const DEFAULT_REBALANCE_BATCH_CELLS: usize = 32;
/// Preserved SQLite snapshots make a same-node wake a rename instead of a
/// remote restore, but must not grow with the lifetime population of a node.
/// The walk is O(cached cells), so keep it off the hot maintenance cadence.
const LOCAL_CACHE_PRUNE_PERIOD: std::time::Duration = std::time::Duration::from_secs(60);

fn main() -> anyhow::Result<()> {
    celld::env_vars::validate()?;
    // Parse the telemetry group once, before any command or runtime work.
    // Its specialized values share the strict scalar parsers in env_vars.
    let telemetry_config = celld::telemetry::Config::from_env()?;
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    // Before the runtime exists, so every worker thread inherits a PKRU that
    // grants access to V8's pointer-table protection key.
    celld::runtime::init_v8();
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(workers) = celld::env_vars::positive::<usize>("CELLD_TOKIO_THREADS")? {
        builder.worker_threads(workers);
    }
    builder.build()?.block_on(async_main(telemetry_config))
}

async fn async_main(telemetry_config: Option<celld::telemetry::Config>) -> anyhow::Result<()> {
    #[cfg(all(test, celld_internal_tests))]
    let shutdown_accept_failure_test_active =
        std::env::var_os("CELLD_DRAIN_ACCEPT_FAILURE_CHILD").is_some();
    #[cfg(all(test, celld_internal_tests))]
    let action = if shutdown_accept_failure_test_active {
        shutdown_accept_failure_test_action()
    } else {
        action_from_process()?
    };
    #[cfg(not(all(test, celld_internal_tests)))]
    let action = action_from_process()?;
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    // Docker and journald can stop consuming the process pipe during a log
    // burst. Logging must lose diagnostics under that backpressure rather
    // than block the Tokio workers that route requests and renew authority.
    // Parsed before the subscriber exists, because where the log goes depends
    // on which invocation this is: a node logs to stdout, and a CLI subcommand
    // must keep stdout for its answer.
    let log_filter = if matches!(
        &action,
        Action::Dev(arguments) if !arguments.iter().any(|argument| argument == "--logs")
    ) {
        // `celld dev` is an application-facing supervisor. Its default view
        // must stay concise even when the parent inherited a broad RUST_LOG;
        // --logs is the explicit opt-in to that diagnostic stream.
        tracing_subscriber::EnvFilter::new("error")
    } else {
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())
    };
    let (log_sink, log_is_terminal): (Box<dyn std::io::Write + Send>, bool) =
        if action.stdout_is_data() {
            (
                Box::new(std::io::stderr()),
                std::io::IsTerminal::is_terminal(&std::io::stderr()),
            )
        } else {
            (
                Box::new(std::io::stdout()),
                std::io::IsTerminal::is_terminal(&std::io::stdout()),
            )
        };
    let (log_writer, log_guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(8_192)
        .lossy(true)
        .finish(log_sink);
    *LOG_GUARD.lock().unwrap() = Some(log_guard);
    tracing_subscriber::fmt()
        .with_writer(log_writer)
        // The custom writer defeats fmt's own TTY detection, and journald
        // must not receive ANSI escapes.
        .with_ansi(log_is_terminal)
        .with_env_filter(log_filter)
        .init();
    // After the subscriber, because this reports whether the allocator agreed
    // to return freed pages on a timer. A node without that thread holds
    // retention until a thread allocates again, which is the condition behind
    // issue #36, so the operator has to be able to read the answer.
    celld::memory::tune_allocator();
    let mut settings = match action {
        Action::Deploy(arguments) => return fleet::run_deploy(arguments).await,
        Action::Dev(arguments) => return celld::dev::run(arguments).await,
        Action::Cell(arguments) => return celld::cell_cli::run(arguments).await,
        Action::D1(arguments) => return celld::d1_cli::run(arguments).await,
        Action::Kv(arguments) => return celld::kv_cli::run(arguments).await,
        Action::Queue(arguments) => return celld::queue_cli::run(arguments).await,
        Action::Connect(arguments) => {
            return celld::control_plane::handle_connect_command(arguments).await
        }
        Action::Credentials(arguments) => {
            return celld::control_plane::handle_credentials_command(arguments).await
        }
        Action::Token(arguments) => {
            return celld::control_plane::handle_token_command(arguments).await
        }
        Action::Disconnect(arguments) => {
            return celld::control_plane::handle_disconnect_command(arguments).await
        }
        Action::Help => {
            print_help()?;
            return Ok(());
        }
        Action::Version => {
            let profile = if cfg!(debug_assertions) {
                " (debug)"
            } else {
                ""
            };
            celld::cli_output::Output::new(celld::cli_output::Format::Text)
                .line(format_args!("celld {}{profile}", env!("CARGO_PKG_VERSION")))?;
            return Ok(());
        }
        Action::Diagnose {
            mut settings,
            peers,
            read_only,
            json,
        } => {
            let ingress = celld::startup::bind_ingress_listener(&settings.listen).await?;
            let internal =
                celld::startup::bind_internal_listener(&celld::startup::InternalListenerSettings {
                    listen: settings.internal_listen.clone(),
                    advertise: settings.advertise.clone(),
                    unsafe_public_advertise: settings.unsafe_public_advertise,
                })
                .await?;
            // The bind proves the address is free; diagnose never serves on it,
            // and an operator should not read this line as a running listener.
            // These reach the terminal through `fleet::diagnose`, so that one
            // `Output` renders every check and `--json` stays parseable.
            let preamble = vec![
                celld::fleet::Check::ok(
                    format!("listen {}", ingress.listen),
                    "bind check; diagnose does not serve",
                ),
                celld::fleet::Check::ok(
                    format!("internal listen {}", internal.listen),
                    "bind check; diagnose does not serve",
                ),
                celld::fleet::Check::ok(
                    format!("advertise {}", internal.advertise),
                    format!(
                        "{}; direct reachability is not inferred",
                        internal.advertise.scope()
                    ),
                ),
            ];
            let managed_storage = if settings.control_plane {
                match celld::control_plane::installation_storage().context(
                    "managed diagnostics require an existing enrollment; run `celld --control-plane` first",
                )? {
                    celld::control_plane::InstallationStorageConfig::Managed(storage) => {
                        settings.bucket = Some(storage.bucket.clone());
                        settings.endpoint = Some(storage.endpoint.clone());
                        settings.region = storage.region.clone();
                        Some(storage)
                    }
                    celld::control_plane::InstallationStorageConfig::Byo(storage) => {
                        settings.bucket = Some(storage.bucket);
                        settings.endpoint = storage.endpoint;
                        settings.region = storage.region;
                        None
                    }
                }
            } else {
                None
            };
            let bucket = settings
                .bucket
                .ok_or_else(|| anyhow::anyhow!("celld diagnose requires --bucket"))?;
            let client = fleet::bucket_client_with_credentials(
                &bucket,
                settings.endpoint.as_deref(),
                &settings.region,
                managed_storage.as_ref(),
            )?;
            return fleet::diagnose(
                &client,
                peers,
                settings.unsafe_public_advertise,
                read_only,
                json,
                preamble,
            )
            .await;
        }
        Action::Run(settings) => settings,
    };
    celld::startup::raise_file_limit();
    let max_resident = celld::env_vars::optional("CELLD_MAX_RESIDENT_CELLS")?
        // celld has no resident ceiling unless the operator configures one.
        // The clean-sheet prototype originally defaulted to eight, which
        // introduced eviction churn in otherwise unconstrained workloads and
        // made cancellation semantics depend on cold-reactivation latency.
        .unwrap_or(usize::MAX);
    let max_request_body_bytes = celld::env_vars::positive_or(
        "CELLD_MAX_REQUEST_BODY_BYTES",
        DEFAULT_MAX_REQUEST_BODY_BYTES,
    )?;
    anyhow::ensure!(
        max_request_body_bytes <= DEFAULT_MAX_REQUEST_BODY_BYTES,
        "CELLD_MAX_REQUEST_BODY_BYTES cannot exceed {DEFAULT_MAX_REQUEST_BODY_BYTES}"
    );
    let local_cache_max_bytes = local_cache_max_bytes_from_environment()?;
    let fail_publish_once = std::env::var_os("CELLD_TEST_FAIL_PUBLISH_ONCE").is_some();
    let ingress = celld::startup::bind_ingress_listener(&settings.listen).await?;
    let internal =
        celld::startup::bind_internal_listener(&celld::startup::InternalListenerSettings {
            listen: settings.internal_listen.clone(),
            advertise: settings.advertise.clone(),
            unsafe_public_advertise: settings.unsafe_public_advertise,
        })
        .await?;
    let advertise = internal.advertise.to_string();
    let listen = ingress.listen.to_string();
    let listener = ingress.listener;
    let internal_listener = internal.listener;
    let mut adapter_credential_version = None;
    let managed_storage = if settings.control_plane {
        if let Some(spec) = settings.bucket.take() {
            let (backend, name, prefix) = celld::bucket::split_spec(&spec);
            // The control plane issues and validates S3-compatible storage
            // only. Parse through the same seam as Bucket::open so scheme
            // case cannot change this decision or the eventual backend.
            anyhow::ensure!(
                backend == celld::bucket::StorageBackend::S3,
                "--control-plane storage is S3-compatible; a {}:// bucket runs without it",
                match backend {
                    celld::bucket::StorageBackend::Gcs => "gs",
                    celld::bucket::StorageBackend::Azure => "az",
                    celld::bucket::StorageBackend::S3 => "s3",
                    celld::bucket::StorageBackend::Local => "dev",
                }
            );
            // The control plane issues one bucket per fleet and its enrollment
            // API rejects a prefix. Install the parsed S3 name because the API
            // accepts a name, while every engine bucket path accepts a spec.
            anyhow::ensure!(
                prefix.is_empty(),
                "--control-plane does not accept a --bucket prefix"
            );
            settings.bucket = Some(name.to_string());
        }
        let requested_byo =
            settings
                .bucket
                .as_ref()
                .map(|bucket| celld::control_plane::ByoStorageConfig {
                    bucket: bucket.clone(),
                    endpoint: settings.endpoint.clone(),
                    region: settings.region.clone(),
                });
        celld::control_plane::connect_on_startup_with_storage(requested_byo).await?;
        settings.load_deployment = true;
        let (storage, credential_version) =
            celld::control_plane::installation_storage_with_version()?;
        adapter_credential_version = Some(credential_version);
        match storage {
            celld::control_plane::InstallationStorageConfig::Managed(storage) => {
                settings.bucket = Some(storage.bucket.clone());
                settings.endpoint = Some(storage.endpoint.clone());
                settings.region = storage.region.clone();
                Some(storage)
            }
            celld::control_plane::InstallationStorageConfig::Byo(storage) => {
                settings.bucket = Some(storage.bucket);
                settings.endpoint = storage.endpoint;
                settings.region = storage.region;
                None
            }
        }
    } else {
        None
    };
    let storage_credentials =
        managed_storage
            .as_ref()
            .map(|storage| celld::replication::StorageCredentials {
                access_key_id: storage.access_key_id.clone(),
                secret_access_key: storage.secret_access_key.clone(),
                session_token: storage.session_token.clone(),
            });
    let (tx, rx) = mpsc::unbounded_channel();
    let sample_tx = tx.clone();
    let alarm_tx = tx.clone();
    let alarm_observer: celld::runtime::AlarmObserver = Arc::new(move |cell, at_ms| {
        let _ = alarm_tx.send(Message::AlarmObserved {
            cell,
            at_ms,
            covered: false,
        });
    });
    let (fence_tx, mut fence_rx) = mpsc::unbounded_channel();
    let node = std::env::var("CELLD_NODE").unwrap_or_else(|_| random_node_session_id());
    let clean_reload_node = node.clone();
    celld::control_plane::install_reexec_node_session_id(&node)?;
    let probe_public_key = celld::peer_probe::install_signer()?;
    // Cold routes are object-store latency, not compute: the default is
    // derived from the thread count but far above it. See
    // `machine::default_max_activations` for the measurements.
    let max_activations =
        celld::env_vars::positive::<usize>("CELLD_ACTIVATIONS")?.unwrap_or_else(|| {
            celld::machine::default_max_activations(
                std::thread::available_parallelism()
                    .map(usize::from)
                    .unwrap_or(1),
            )
        });
    let max_evictions = celld::env_vars::positive::<usize>("CELLD_EVICTIONS")?
        .unwrap_or(DEFAULT_MAX_CONCURRENT_EVICTIONS);
    let placement_weight = celld::env_vars::positive::<u64>("CELLD_PLACEMENT_WEIGHT")?
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|cpus| cpus.get() as u64)
                .unwrap_or(1)
        });
    let rebalance_interval_ms = celld::env_vars::optional::<u64>("CELLD_REBALANCE_INTERVAL_MS")?
        .unwrap_or(DEFAULT_REBALANCE_INTERVAL_MS);
    // The shared fleet sample's interval: the balancing interval, or the
    // format gate's when balancing is off. Every reader of the sample, the
    // balancing tick, placement and recruitment, judges freshness by it.
    let fleet_sample_ms = if rebalance_interval_ms > 0 {
        rebalance_interval_ms
    } else {
        FORMAT_GATE_SAMPLE_MS
    };
    let rebalance_batch_cells =
        celld::env_vars::positive_or("CELLD_REBALANCE_BATCH_CELLS", DEFAULT_REBALANCE_BATCH_CELLS)?;
    let max_releases = celld::env_vars::positive::<usize>("CELLD_RELEASES")?
        .unwrap_or(DEFAULT_MAX_CONCURRENT_RELEASES);
    let data_dir = std::env::var_os("CELLD_TEST_DATA_DIR")
        .or_else(|| std::env::var_os("CELLD_WATCH"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join(format!("celld-{}", std::process::id())));
    let mut deploy_agent = None;
    // The channel `POST /reload` and a managed notification nudge the pointer
    // watcher through. Without a deployment bucket the receiver is dropped
    // and a reload reports that there is no pointer to reload from.
    let (reload_tx, reload_rx) = celld::generation::reload_channel();
    let (runtime, ownership, peer_key, wake_scan, deploy_bucket) =
        if settings.bucket.is_some() && settings.load_deployment {
            let client = node_bucket(&settings, managed_storage.as_ref(), false)?;
            if settings.control_plane {
                fleet::validate_managed_bucket(&client).await?;
            } else {
                fleet::validate_bucket(&client).await?;
            }
            // The list above proves the bucket answers; it does not prove the
            // store enforces the conditional writes or ranged reads that a
            // cell needs. Test both contracts here before the node serves.
            if settings.storage_probe {
                fleet::probe_storage_before_serving(&client, settings.control_plane).await?;
            }
            let lease_client = node_bucket(&settings, managed_storage.as_ref(), true)?;
            if settings.control_plane {
                celld::control_plane::wait_for_initial_deployment(&client).await?;
                deploy_agent = Some(client.clone());
            }
            let peer_key = peer_auth::load_or_create(&client).await?;
            let graph = DeploymentGraph::load(&client, node.clone()).await?;
            let wake = Arc::new(celld::wake::WakeFlusher::new());
            celld::js::set_arm_gate(ArmGate {
                bucket: client.clone(),
                flusher: wake.clone(),
            });
            // An `r2_buckets` binding lives in the fleet bucket, under the
            // reserved `r2/<bucket_name>/` prefix. celld runs on blob storage
            // rather than providing it, so a binding gets the store the node
            // already holds credentials for instead of a second one.
            celld::js::set_r2_store(client.clone());
            // celld treats replication as a node service, not as a property
            // of today's manifest. Start it even for a stateless deployment
            // so a later deployment can introduce cells without changing the
            // durability contract underneath the node.
            let replication = Some(Replication::start(
                client.clone(),
                &data_dir,
                settings.endpoint.clone(),
                settings.region.clone(),
                storage_credentials.clone(),
            )?);
            // The same two calls a reload makes: there is no boot-only way from
            // a deployment to a serving generation.
            let generation = Generation::build(
                FIRST_GENERATION,
                graph,
                GenerationOptions {
                    loader_binding: worker_loader_binding(),
                    node: node.clone(),
                    region: settings.region.clone(),
                },
            )?;
            let runtime = RuntimeManager::start(
                generation,
                RuntimeOptions {
                    data_dir: data_dir.clone(),
                    replication,
                    wake: Some(wake.clone()),
                    alarm_observer: alarm_observer.clone(),
                    node: node.clone(),
                    region: settings.region.clone(),
                },
            )?;
            let deploy_bucket = Some(client.clone());
            let bucket_ownership = Arc::new(
                BucketOwnership::new(
                    client.clone(),
                    lease_client,
                    node.clone(),
                    probe_public_key.clone(),
                )
                .with_lease_ttl_ms(lease_ttl_ms_from_environment())
                .with_fleet_sample_ms(fleet_sample_ms),
            );
            let wake_scan = Some((client, bucket_ownership.clone()));
            let ownership = Ownership::Bucket(bucket_ownership);
            (
                Some(runtime),
                Some(ownership),
                peer_key,
                wake_scan,
                deploy_bucket,
            )
        } else if let Ok(script_path) = std::env::var("CELLD_TEST_SCRIPT_PATH") {
            let source = std::fs::read_to_string(&script_path)?;
            let do_classes = std::env::var("CELLD_TEST_DO_CLASSES")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect();
            let bindings = std::env::var("CELLD_TEST_DO_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(name, class)| (name.trim().to_string(), class.trim().to_string()))
                .filter(|(name, class)| !name.is_empty() && !class.is_empty())
                .collect();
            // `BINDING=database` pairs, the local-script equivalent of
            // `d1_databases` in a deployed project.
            let d1_bindings: Vec<(String, String)> = std::env::var("CELLD_TEST_D1_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(name, database)| (name.trim().to_string(), database.trim().to_string()))
                .filter(|(name, database)| !name.is_empty() && !database.is_empty())
                .collect();
            // `BINDING=namespace-id`, the `CELLD_TEST_D1_BINDINGS` shape. Day one,
            // for the reason D1's and Workflows' equivalents landed on day one:
            // every runtime test needs it.
            let kv_bindings: Vec<(String, String)> = std::env::var("CELLD_LOCAL_KV_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(name, id)| (name.trim().to_string(), id.trim().to_string()))
                .filter(|(name, id)| !name.is_empty() && !id.is_empty())
                .collect();
            // `BINDING=queue`, the local-script equivalent of one Wrangler Queue
            // producer. A local binding has no configuration object for a default
            // delay, so it uses the Queue default of zero.
            let queue_bindings: Vec<QueueBinding> = std::env::var("CELLD_LOCAL_QUEUE_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(environment, queue)| QueueBinding {
                    environment: environment.trim().to_string(),
                    queue: queue.trim().to_string(),
                    delivery_delay: 0,
                })
                .filter(|binding| !binding.environment.is_empty() && !binding.queue.is_empty())
                .collect();
            for binding in &queue_bindings {
                anyhow::ensure!(
                    celld_logic::cell::valid_cell_scope(&binding.queue),
                    "local Queue binding {} has a queue name that cannot name a cell: {:?}",
                    binding.environment,
                    binding.queue,
                );
            }
            // `BINDING=bucket` pairs, the local-script equivalent of
            // `r2_buckets` in a deployed project.
            let r2_bindings: Vec<(String, String)> = std::env::var("CELLD_TEST_R2_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(name, bucket)| (name.trim().to_string(), bucket.trim().to_string()))
                .filter(|(name, bucket)| !name.is_empty() && !bucket.is_empty())
                .collect();
            let mut do_classes: Vec<String> = do_classes;
            if !d1_bindings.is_empty() {
                do_classes.push(celld::deploy::D1_CLASS.to_string());
            }
            if !kv_bindings.is_empty() {
                do_classes.push(celld::deploy::KV_CLASS.to_string());
            }
            if !queue_bindings.is_empty() {
                do_classes.push(celld::deploy::QUEUE_CLASS.to_string());
            }
            let workflow_bindings: Vec<WorkflowBinding> =
                std::env::var("CELLD_LOCAL_WORKFLOW_BINDINGS")
                    .unwrap_or_default()
                    .split(',')
                    .filter_map(|value| {
                        let (binding, rest) = value.split_once('=')?;
                        let (name, class) = rest.split_once('=')?;
                        Some(WorkflowBinding {
                            environment: binding.trim().to_string(),
                            workflow: name.trim().to_string(),
                            class: class.trim().to_string(),
                        })
                    })
                    .filter(|binding| {
                        !binding.environment.is_empty()
                            && !binding.workflow.is_empty()
                            && !binding.class.is_empty()
                    })
                    .collect();
            // Resolved before the config is built, because the reserved workflow
            // class is script-scoped and `do_classes` has to carry the scoped name.
            let script_name = std::env::var("CELLD_TEST_SCRIPT_NAME")
                .unwrap_or_else(|_| "celld-local".to_string());
            if !workflow_bindings.is_empty() {
                do_classes.push(celld::deploy::workflow_class(&script_name));
            }
            let crons: Vec<String> = std::env::var("CELLD_TEST_CRONS")
                .unwrap_or_default()
                .split(';')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect();
            let options = WorkerConfigOptions {
                src: source,
                script_name,
                do_classes,
                bindings,
                r2_bindings,
                d1_bindings,
                kv_bindings,
                queue_bindings,
                queue_consumers: Vec::new(),
                workflow_bindings,
                ai_binding: fleet::configured_ai_binding(None),
                vars: Vec::new(),
                node: node.clone(),
                modules: Vec::new(),
                compat: Compat::default(),
            };
            let (ownership, peer_key, wake, wake_scan) = if settings.bucket.is_some() {
                let client = node_bucket(&settings, managed_storage.as_ref(), false)?;
                let lease_client = node_bucket(&settings, managed_storage.as_ref(), true)?;
                let peer_key = peer_auth::load_or_create(&client).await?;
                let wake = Arc::new(celld::wake::WakeFlusher::new());
                celld::js::set_arm_gate(ArmGate {
                    bucket: client.clone(),
                    flusher: wake.clone(),
                });
                celld::js::set_r2_store(client.clone());
                let bucket_ownership = Arc::new(
                    BucketOwnership::new(
                        client.clone(),
                        lease_client,
                        node.clone(),
                        probe_public_key.clone(),
                    )
                    .with_lease_ttl_ms(lease_ttl_ms_from_environment())
                    .with_fleet_sample_ms(fleet_sample_ms),
                );
                let wake_scan = Some((client, bucket_ownership.clone()));
                (
                    Some(Ownership::Bucket(bucket_ownership)),
                    peer_key,
                    Some(wake),
                    wake_scan,
                )
            } else {
                (None, random_peer_key(), None, None)
            };
            // A local script is a one-script deployment with no pointer behind
            // it, built through the same function as a bucket deployment.
            let script_name = options.script_name.clone();
            let generation = Generation::build(
                FIRST_GENERATION,
                DeploymentGraph::single(fleet::LoadedDeployment {
                    options,
                    script_name,
                    version: "local".to_string(),
                    prefix: script_path.clone(),
                    asset_binding: None,
                    assets: None,
                    services: Vec::new(),
                    crons,
                }),
                GenerationOptions {
                    loader_binding: worker_loader_binding(),
                    node: node.clone(),
                    region: settings.region.clone(),
                },
            )?;
            (
                Some(RuntimeManager::start(
                    generation,
                    RuntimeOptions {
                        data_dir: data_dir.clone(),
                        replication: None,
                        wake: wake.clone(),
                        alarm_observer: alarm_observer.clone(),
                        node: node.clone(),
                        region: settings.region.clone(),
                    },
                )?),
                ownership,
                peer_key,
                wake_scan,
                None,
            )
        } else {
            let (ownership, peer_key) = if settings.bucket.is_some() {
                let client = node_bucket(&settings, managed_storage.as_ref(), false)?;
                let lease_client = node_bucket(&settings, managed_storage.as_ref(), true)?;
                let peer_key = peer_auth::load_or_create(&client).await?;
                (
                    Some(Ownership::Bucket(Arc::new(
                        BucketOwnership::new(
                            client,
                            lease_client,
                            node.clone(),
                            probe_public_key.clone(),
                        )
                        .with_lease_ttl_ms(lease_ttl_ms_from_environment())
                        .with_fleet_sample_ms(fleet_sample_ms),
                    ))),
                    peer_key,
                )
            } else {
                (None, random_peer_key())
            };
            (None, ownership, peer_key, None, None)
        };
    if let Some(config) = &telemetry_config {
        let sink_bucket = match config.sink {
            celld::telemetry::SinkChoice::Bucket => {
                if settings.bucket.is_none() {
                    anyhow::bail!(
                        "CELLD_OTEL=1 but this node has no bucket; the \
                         bucket sink needs one (CELLD_BUCKET), or choose \
                         CELLD_OTEL_SINK=otlp"
                    );
                }
                // Its own client even for the fleet bucket: each open is its
                // own transport (bucket.rs), so telemetry PUT bursts never
                // share a connection pool with ownership traffic.
                Some(if let Some(bucket) = config.bucket_override.as_deref() {
                    fleet::bucket_client_with_credentials(
                        bucket,
                        settings.endpoint.as_deref(),
                        &settings.region,
                        managed_storage.as_ref(),
                    )?
                } else {
                    node_bucket(&settings, managed_storage.as_ref(), false)?
                })
            }
            // The collector path needs no bucket at all.
            celld::telemetry::SinkChoice::Otlp => None,
        };
        celld::telemetry::init(config, sink_bucket, node.clone(), settings.region.clone())?;
    }
    let peer_auth = Arc::new(PeerAuth::new(peer_key, node.clone())?);
    // Connect-only timeout: a peer request may legitimately run through a
    // restore, but a handshake that never completes provably ran nothing.
    let peer_http = reqwest::Client::builder()
        .connect_timeout(PEER_CONNECT_TIMEOUT)
        // Nagle against delayed ACKs costs tens of milliseconds on every
        // multi-segment peer body. Peer requests are latency-bound, so the
        // socket must write eagerly.
        .tcp_nodelay(true)
        .build()
        .unwrap();
    let paced_handoff = !matches!(
        std::env::var("CELLD_PACED_HANDOFF").as_deref(),
        Ok("0" | "off" | "false")
    );
    let resume_generation = celld::runtime::take_clean_reload_generation(&data_dir, &node);
    let clean_reload_candidate = resume_generation.is_some();
    let drain_pins = DrainPinRegistry::default();
    let actor = Actor::from_environment_with_services(
        AdmissionLimits {
            resident: max_resident,
            activations: max_activations,
            evictions: max_evictions,
            releases: max_releases,
            placement_weight,
        },
        fail_publish_once,
        fence_tx,
        ActorServices {
            runtime: runtime.clone(),
            drain_pins: drain_pins.clone(),
            ownership,
            peer_http: peer_http.clone(),
            peer_auth: peer_auth.clone(),
            paced_handoff,
        },
        ActorIdentity {
            node: node.clone(),
            advertise: advertise.clone(),
            region: settings.region.clone(),
        },
        resume_generation,
    )
    .await?;
    let process_generation = actor.lease_spec.generation.clone();
    let ownership_name = actor.ownership.name();
    // Keep the fleet reader beside the readiness task before the actor moves
    // to its isolated thread. Both handles share the same lease adapter, so
    // readiness evaluates exactly the records that placement publishes.
    let ready_ownership = match &actor.ownership {
        Ownership::Bucket(ownership) => Some(ownership.clone()),
        Ownership::Memory(_) => None,
    };
    let fleet_bucket = ready_ownership
        .as_ref()
        .map(|ownership| ownership.bucket_client());
    let explorer_replication = runtime.as_ref().and_then(RuntimeManager::replication);
    let local_cache_replication = explorer_replication.clone();
    let (websocket_tx, mut websocket_rx) = mpsc::unbounded_channel();
    // The log tier's follower store (crate::node_log): fragments other
    // leaders replicate here live under the node's data dir beside the cell
    // databases.
    let follower = match &actor.ownership {
        Ownership::Bucket(bucket_ownership) if settings.bucket.is_some() => {
            Some(Arc::new(celld::node_log::FollowerStore::new(
                &data_dir,
                Some(Arc::new(bucket_ownership.bucket_client())),
                &node,
            )))
        }
        _ => None,
    };
    #[cfg(all(test, celld_internal_tests))]
    let follower = follower.or_else(|| {
        shutdown_accept_failure_test_active
            .then(|| Arc::new(celld::node_log::FollowerStore::new(&data_dir, None, &node)))
    });
    let app = AppHandle {
        tx,
        runtime,
        reload: reload_tx.clone(),
        peer_http,
        peer_auth,
        advertise: advertise.clone(),
        websockets: websocket_tx,
        draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        fleet_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        public_in_flight: Arc::new(AtomicUsize::new(0)),
        drain_pins,
        trust_forwarded_headers: settings.trust_forwarded_headers,
        // RPO=0 is the default. An operator can disable the output gate to
        // remove object-store replication latency from the write response,
        // explicitly accepting that an acknowledged write can be lost.
        output_gate: celld::env_vars::flag("CELLD_OUTPUT_GATE", true)?,
        max_outbound_websockets: celld::env_vars::positive_or(
            "CELLD_MAX_OUTBOUND_WEBSOCKETS",
            DEFAULT_MAX_OUTBOUND_WEBSOCKETS,
        )?,
        max_request_body_bytes,
        operation_deadline_ms: celld::actor::operation_deadline_ms()?,
        follower: follower.clone(),
    };

    // The in-fleet log tier, v0. The takeover interlock is installed in
    // every posture — a bucket-posture node can take over from a
    // fleet-posture one and must find a complete bucket — while shipping
    // requires the explicit fleet posture.
    // Fleet is the DEFAULT (decided 2026-08-14): a single node behaves
    // exactly like sync-to-bucket — no peers means no record, no shipper,
    // and bucket-proven acks — and once TWO peers appear the maintenance
    // tick recruits them and fleet replication turns on. Two, not one:
    // `maintain` takes(2) from a member set that excludes self, and
    // `NodeLogManager::healthy` wants members.len() >= 2, so the posture
    // engages at three nodes and not before. A one- or two-node fleet
    // therefore asks for `fleet` and keeps bucket-proven acks, which is
    // correct and much slower. One value serves the hobbyist's first node
    // and the fleet it grows into; CELLD_DURABILITY=bucket remains the
    // explicit opt-out.
    let durability = std::env::var("CELLD_DURABILITY").unwrap_or_else(|_| "fleet".into());
    anyhow::ensure!(
        matches!(durability.as_str(), "bucket" | "fleet"),
        "CELLD_DURABILITY must be `bucket` or `fleet`"
    );
    let mut durability_owner = DurabilityOwnerSelection::new(follower.clone());
    if let Ownership::Bucket(bucket_ownership) = &actor.ownership {
        if let (Some(replication), Some(spec)) = (
            app.runtime.as_ref().and_then(RuntimeManager::replication),
            settings.bucket.clone(),
        ) {
            let _ = spec;
            let log_bucket = Arc::new(bucket_ownership.bucket_client());
            let ltx = replication.ltx();
            // Bundle the paced tiering: one PUT per node-flush instead of
            // one per cell-transaction — the Class A collapse, measured at
            // 208x against per-transaction PUTs. On by default; 0 is the
            // opt-out.
            let bundle_mode = celld::env_vars::flag("CELLD_LOG_BUNDLE", true)?;
            let nudge_tx = app.tx.clone();
            let own_log = Arc::new(celld::node_log::OwnLog {
                ownership: bucket_ownership.clone(),
                nudge: Box::new(move || {
                    let _ = nudge_tx.send(Message::NudgeNodeLease);
                }),
                write_lock: tokio::sync::Mutex::new(()),
            });
            let manager = Arc::new(celld::node_log::NodeLogManager::new(
                &format!("{node}/{process_generation}"),
                log_bucket,
                own_log,
                ltx.clone(),
                app.peer_auth.clone(),
                bundle_mode,
                celld::node_log::eviction_policy_from_env()?,
            ));
            *actor.node_log.lock().unwrap() = Some(manager.clone());
            // Recovery-before-install is fatal for every posture: the
            // predecessor's folded state lives in the lease record this
            // session is about to replace, and the install writes a fresh
            // log — so an unrecovered predecessor must stop the boot, or
            // the install erases the only evidence recovery was needed.
            // The ladder waits out the predecessor's own lease because the
            // fence recheck refuses a live lease, retries with backoff,
            // and exits on exhaustion. Serve the retained follower disks
            // throughout the ladder, including retry backoffs, so a fleet
            // restart can recover without waiting for another node's lease.
            recover_with_follower_listener(&internal_listener, app.clone(), async {
                let ttl_backoff =
                    std::time::Duration::from_millis(actor.lease_spec.ttl_ms.max(1_000));
                let mut recovered = Ok(());
                // A boot waits behind a peer's live recovery claim and takes
                // it over only once its heartbeat is stale, so one attempt
                // can legitimately last as long as the peer's gather. The
                // bound is a backstop against a wedge, not a pace.
                for attempt in 1..=4u32 {
                    recovered = match tokio::time::timeout(
                        std::time::Duration::from_secs(15 * 60),
                        manager.recover_self(),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(anyhow::anyhow!("predecessor recovery timed out")),
                    };
                    if recovered.is_ok() || attempt == 4 {
                        break;
                    }
                    if let Err(error) = recovered.as_ref() {
                        eprintln!("celld predecessor recovery attempt {attempt} failed: {error:#}");
                    }
                    // At least one full TTL between attempts, so a
                    // refused-because-live predecessor lease has expired
                    // by the retry.
                    tokio::time::sleep(ttl_backoff.saturating_mul(attempt)).await;
                }
                recovered.map_err(|error| {
                    anyhow::anyhow!(
                        "refusing to install a lease over an unrecovered \
                         predecessor log: {error:#}"
                    )
                })
            })
            .await?;
            let mut owner =
                celld::node_log::DurabilityOwner::new(manager, durability == "fleet", bundle_mode);
            owner.start_background(follower.clone());
            durability_owner.install_runtime(owner);
        }
    }
    let mut durability_owner = durability_owner.select();
    let (do_call_tx, mut do_call_rx) = mpsc::unbounded_channel();
    celld::js::set_do_call_tx(do_call_tx);
    let (gate_tx, mut gate_rx) = mpsc::unbounded_channel();
    celld::js::set_gate_tx(gate_tx);
    let (rpc_call_tx, mut rpc_call_rx) = mpsc::unbounded_channel();
    celld::js::set_rpc_call_tx(rpc_call_tx);
    let (service_call_tx, mut service_call_rx) = mpsc::unbounded_channel();
    celld::js::set_svc_call_tx(service_call_tx);
    let (service_rpc_tx, mut service_rpc_rx) = mpsc::unbounded_channel();
    celld::js::set_svc_rpc_tx(service_rpc_tx);
    let (queue_dispatch_tx, mut queue_dispatch_rx) = mpsc::unbounded_channel();
    celld::js::set_queue_dispatch_tx(queue_dispatch_tx);
    let (asset_call_tx, mut asset_call_rx) = mpsc::unbounded_channel();
    celld::js::set_asset_call_tx(asset_call_tx);
    let (outbound_ws_tx, mut outbound_ws_rx) = mpsc::unbounded_channel();
    celld::js::set_outbound_ws_tx(outbound_ws_tx);
    // KV large values live in the fleet bucket. The cloneable handle follows
    // the R2 binding's startup pattern, so independent cells issue bucket I/O
    // independently instead of queueing behind one task. Blob I/O takes no
    // part in the drain ordering: a blob is written before its row, so an
    // interrupted write leaves collectable bytes and never a dangling row.
    if let Ownership::Bucket(bucket_ownership) = &actor.ownership {
        celld::js::set_kv_blob_store(bucket_ownership.bucket_client());
    }
    // The core is a serial ownership actor, not a Worker executor. It owns the
    // node lease timer, so ingress, proxy retries, and restore completions must
    // not consume every scheduler turn it needs. Its isolated single-thread
    // runtime also keeps state transitions ordered exactly as the deterministic
    // executor models them. Request work, restores, and blocking scans stay on
    // the shared runtime and report their results back as messages.
    let (actor_exit_tx, mut actor_exit_rx) = mpsc::unbounded_channel();
    std::thread::Builder::new()
        .name("celld-core".into())
        .spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .map(|runtime| runtime.block_on(actor.run(rx)));
            let _ = actor_exit_tx.send(result);
        })?;

    // The sampler is a plain ticker: it measures and posts, and decides
    // nothing. Everything downstream of the numbers -- the latch, the target,
    // which cell goes -- is in the core, so a sample sequence replays.
    {
        celld::asyncrt::spawn(async move {
            let mut tick = celld::asyncrt::interval(LOAD_SAMPLE_PERIOD);
            tick.set_missed_tick_behavior(celld::asyncrt::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if sample_tx.send(Message::SampleLoad).is_err() {
                    return;
                }
            }
        })
        .detach();
    }

    // The fleet-aware first-readiness gate: withhold the first health 200
    // until no donor is mid-handoff and every live peer publishes successor
    // capacity. CELLD_READY_FLEET_GATE_MS bounds the silent wait before celld
    // reports the blocking condition, but it cannot make an unsafe fleet
    // ready. Orchestrators key rollout advance on readiness, so a fail-open
    // deadline would erase the only signal that prevents another donor. The
    // gate is one-shot: once open, fleet state never demotes readiness, and a
    // gated node still serves its owned cells through peers.
    let ready_gate_ms: u64 = celld::env_vars::with_default("CELLD_READY_FLEET_GATE_MS", 120_000)?;
    match (&ready_ownership, ready_gate_ms) {
        (Some(ownership), gate_ms) if gate_ms > 0 => {
            let ownership = ownership.clone();
            let bucket = ownership.bucket_client();
            let gate_node = node.clone();
            let gate_flag = app.fleet_ready.clone();
            let max_restoring = max_activations as u64;
            tokio::spawn(async move {
                let started = std::time::Instant::now();
                let mut deadline_reported = false;
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let (current, peers) = tokio::join!(
                        celld::drain_token::read(&bucket),
                        ownership.read_capacity_peers()
                    );
                    // Judge lease and token expiry after the I/O. A slow list
                    // must not make a record live at the time before the read.
                    let now_ms = celld::ownership_store::now_ms();
                    let status = match (&current, &peers) {
                        (Ok(current), Ok(peers)) => celld_logic::drain::fleet_status(
                            current.as_ref().and_then(|current| current.token.as_ref()),
                            &gate_node,
                            now_ms,
                            peers,
                            max_restoring,
                        ),
                        // An unreadable store is not settled; the deadline
                        // bounds the wait.
                        _ => Err(celld_logic::drain::FleetUnsettled::Unreadable),
                    };
                    let waited_ms = started.elapsed().as_millis() as u64;
                    if status.is_ok() {
                        let readiness_reason = if peers.as_ref().ok().is_some_and(|peers| {
                            celld_logic::drain::joining_contributes_successor_capacity(
                                peers, &gate_node, now_ms,
                            )
                        }) {
                            "successor_capacity"
                        } else {
                            "fleet_settled"
                        };
                        gate_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        tracing::info!(
                            event = "ready_gate_open",
                            readiness_reason,
                            waited_ms,
                            "the fleet is settled; serving readiness is open"
                        );
                        return;
                    }
                    if waited_ms >= gate_ms && !deadline_reported {
                        deadline_reported = true;
                        let holder = current
                            .as_ref()
                            .ok()
                            .and_then(|current| current.as_ref())
                            .and_then(|current| current.token.as_ref())
                            .map(|token| token.node.as_str())
                            .unwrap_or_default();
                        tracing::warn!(
                            event = "ready_gate_expired",
                            holder,
                            reason = ?status.err(),
                            waited_ms,
                            "the ready-gate observation deadline expired; readiness stays closed"
                        );
                    }
                }
            });
        }
        _ => app
            .fleet_ready
            .store(true, std::sync::atomic::Ordering::SeqCst),
    }

    // Ownership balancing. Each node reads the shared fleet sample and, when it
    // is the densest node and a peer has room, hands a bounded batch of idle
    // cells through the drain's release-and-adopt pipeline. Cell moves need
    // no fleet-wide lock: the per-cell ownership CAS is the authority, and the
    // densest-node rule elects one donor per consistent sample. A donor
    // plans again only from leases sampled after its last batch had one
    // load-sample period to reach them, so no plan mixes counts from before
    // a batch with counts from after it.
    //
    // The same lease sample carries the bucket-format gate
    // (celld_logic::format): a node pages a large takeover only while every
    // live lease reads a paged epoch. The loop therefore runs even with
    // balancing off, at the balancing interval or FORMAT_GATE_SAMPLE_MS, and
    // the gate is judged before any balancing condition can skip the tick.
    // Off until the first sample, so a takeover in the first seconds of a
    // process clones, which is always safe.
    if let Some(ownership) = ready_ownership.clone() {
        let app = app.clone();
        let node = node.clone();
        let replication = explorer_replication.clone();
        let sample_ms = fleet_sample_ms;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(sample_ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut settled_after_ms = 0;
            loop {
                tick.tick().await;
                let now_ms = celld::ownership_store::now_ms();
                // The view is judged at the instant the sample was taken.
                // Its leases are copies that age while the real nodes renew,
                // and nodes started together renew in lock-step, so judged
                // at this node's clock a view a few seconds old shows every
                // lease dead at once: on the 2026-09-05 fleet that closed the
                // gate and skipped balancing on a third of the ticks.
                let (view_ms, peers) = match ownership.read_shared_capacity_peers(sample_ms).await {
                    Ok(view) => view,
                    Err(error) => {
                        // At info with the chain: a failed tick closes the
                        // format gate, and a fleet run has to say why.
                        tracing::info!(event = "fleet_sample_failed", error = format!("{error:#}"));
                        // A missing or in-flight refresh cannot keep granting
                        // format permission from an earlier fleet membership.
                        (now_ms, Vec::new())
                    }
                };
                if let Some(replication) = &replication {
                    let format = celld_logic::format::BUCKET_FORMAT;
                    let open = celld_logic::format::fleet_reads(&peers, view_ms, format);
                    if replication.set_paged_fleet(open) != open {
                        tracing::info!(
                            event = "paged_gate",
                            open,
                            format,
                            live_leases = peers.iter().filter(|p| p.expires_ms > view_ms).count(),
                            view_age_ms = now_ms.saturating_sub(view_ms),
                            "every live lease reads a paged epoch, or one does not"
                        );
                    }
                }
                if rebalance_interval_ms == 0 {
                    continue;
                }
                let paused = ownership
                    .live()
                    .rebalance_paused
                    .load(std::sync::atomic::Ordering::Relaxed);
                if !app.fleet_ready() || app.is_draining() || paused {
                    continue;
                }
                // One batch outstanding at a time, but one cell must not
                // hold the fleet: a cell whose proof cannot finish stays in
                // flight for a long time, and the densest node is the only
                // donor. Plan around it with the rest of the batch.
                let status = app.drain_status().await;
                let in_flight =
                    status.quiescing + status.evicting + status.releasing + status.adopting;
                if in_flight >= rebalance_batch_cells {
                    continue;
                }
                let Some(cells) = celld_logic::rebalance::surplus(
                    &peers,
                    &node,
                    view_ms,
                    ownership.lease_ttl_ms(),
                    settled_after_ms,
                    rebalance_batch_cells - in_flight,
                ) else {
                    continue;
                };
                tracing::info!(
                    event = "rebalance_batch",
                    cells,
                    "handing idle cells to peers below their ownership target"
                );
                if app.tx.send(Message::Rebalance { cells }).is_err() {
                    return;
                }
                settled_after_ms = now_ms + LOAD_SAMPLE_PERIOD.as_millis() as u64;
            }
        });
    }

    if let Some((client, ownership)) = wake_scan {
        let authority_wait_seconds = if clean_reload_candidate { 60 } else { 10 };
        let deadline = Instant::now() + std::time::Duration::from_secs(authority_wait_seconds);
        while !app.healthy().await {
            if Instant::now() >= deadline {
                anyhow::bail!("node authority was not established before wake scan");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // The boot scan acts for the fleet: it is what finds the alarms this
        // node's previous incarnation left behind, and any orphan that came
        // due while nothing was watching. It probes like the elected waker
        // does, so a rolling restart does not send every node the whole
        // fleet's due set once each.
        let boot_ms = now_ms();
        let live = celld::dead_node_gc::live_nodes(&client, boot_ms).await;
        let due = celld::wake::due_scan(&client, boot_ms as i64).await;
        for (cell, entry_ms) in celld::wake::elected_due(&ownership, &node, &live, due).await {
            let _ = app.tx.send(Message::WakeHint {
                cell,
                entry_ms,
                scope: celld_logic::WakeHintScope::Fleet,
            });
        }

        // And again, on a timer, for the rest of the process's life.
        //
        // The boot scan alone only covers alarms that came due while nothing
        // was watching *before this node started*. A node that dies while
        // this one is already running leaves its cells with armed alarms and
        // no owner, and nothing would look at them again until this process
        // restarted -- an alarm silently not firing, which is the one thing a
        // Durable Object is not allowed to do.
        //
        // Every node lists the due entries on its tick, because a node's own
        // dormant cells can carry alarms it has never loaded (a cell that
        // arrived by an ownership-only handoff), and only that node can wake
        // them. But only the node that holds the advisory waker role acts for
        // the fleet: it reads each due cell's owner outside the activation
        // queue and hints the orphans, while every other node hints with
        // `Owned` scope, which the core answers from memory. Before that
        // split, every node pushed the whole fleet's due set through its own
        // activation queue, one permit and one owner read per entry, and its
        // own alarm wakes queued behind them.
        //
        // Every decision about a hinted entry still stays in the core: a
        // hint for a cell this node already serves is ignored, and one for a
        // cell with a live owner elsewhere resolves to that owner rather
        // than stealing it.
        let scan_app = app.clone();
        let waker_node = node.clone();
        let tick_ms = celld::env_vars::positive::<u64>("CELLD_WAKER_TICK_MS")?.unwrap_or(60_000);
        let period = std::time::Duration::from_millis(tick_ms);
        celld::asyncrt::spawn(async move {
            let mut tick = celld::asyncrt::interval(period);
            tick.set_missed_tick_behavior(celld::asyncrt::MissedTickBehavior::Delay);
            tick.tick().await;
            let mut dead_node_gc = celld::dead_node_gc::DeadNodeGc::default();
            loop {
                tick.tick().await;
                let elected = dead_node_gc
                    .run_elected_pass(&client, &waker_node, tick_ms)
                    .await;
                let due = celld::wake::due_scan(&client, now_ms() as i64).await;
                let (scope, due) = match elected {
                    Some(live) => (
                        celld_logic::WakeHintScope::Fleet,
                        celld::wake::elected_due(&ownership, &waker_node, &live, due).await,
                    ),
                    None => (celld_logic::WakeHintScope::Owned, due),
                };
                for (cell, entry_ms) in due {
                    let hint = Message::WakeHint {
                        cell,
                        entry_ms,
                        scope,
                    };
                    if scan_app.tx.send(hint).is_err() {
                        return;
                    }
                }
            }
        })
        .detach();
    }

    // Arm the schedule, then watch the pointer. Adoption arms it again, so
    // a cron change travels with the deployment that carries it.
    spawn_cron_arm(app.clone());
    if let Some(bucket) = deploy_bucket {
        start_pointer_watcher(
            app.clone(),
            bucket,
            node.clone(),
            settings.region.clone(),
            reload_rx,
        );
    }
    if let Some(client) = deploy_agent {
        celld::control_plane::start_deploy_agent(client.clone(), reload_tx.clone());
        let presence_app = app.clone();
        celld::control_plane::start_presence_agent(celld::control_plane::PresenceRuntime {
            s3: client,
            replication: explorer_replication,
            node_session_id: node,
            advertise,
            listen,
            credential_version: adapter_credential_version
                .expect("managed adapters have a credential version"),
            snapshot: Arc::new(move || {
                let app = presence_app.clone();
                Box::pin(async move { app.presence().await })
            }),
            reload: reload_tx.clone(),
        });
    }

    celld::cli_output::Output::new(celld::cli_output::Format::Text).line(format_args!(
        "celld listening on {} (ownership={ownership_name})",
        listener.local_addr()?
    ))?;
    celld::cli_output::Output::new(celld::cli_output::Format::Text).line(format_args!(
        "celld internal listening on {} (advertise={})",
        internal_listener.local_addr()?,
        app.advertise
    ))?;
    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
    #[cfg(all(test, celld_internal_tests))]
    if shutdown_accept_failure_test_active {
        let _ = shutdown_tx.send(ShutdownMode::Handoff);
    }
    // A SIGTERM (systemd stop, `docker stop`, a Kubernetes pod delete) or a
    // SIGINT begins the same graceful shutdown as `POST /shutdown`, so the
    // orchestrator's ordinary stop drains and hands off instead of killing.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let (connection_drain_tx, connection_drain) = watch::channel(false);
    let drain_ms: u64 = celld::env_vars::positive("CELLD_SHUTDOWN_DRAIN_MS")?.unwrap_or(25_000);
    let shutdown_timing = celld::env_vars::shutdown_timing()?;
    let shutdown_total_ms = shutdown_timing.total_ms;
    let drain_token_wait_ms = shutdown_timing.drain_token_wait_ms;
    // A hung connection must not consume the whole preserve budget: the
    // semantic drain and the clean-reload certificate come out of the same
    // deadline.
    let connection_grace =
        CONNECTION_DRAIN_GRACE.min(std::time::Duration::from_millis(drain_ms / 4));
    let mut connections: FuturesUnordered<ConnectionFuture> = FuturesUnordered::new();
    let mut do_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut gate_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut service_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut queue_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut asset_calls: FuturesUnordered<AssetCallFuture> = FuturesUnordered::new();
    let mut websockets: FuturesUnordered<WebSocketFuture> = FuturesUnordered::new();
    let mut cache_prunes: FuturesUnordered<CachePruneFuture> = FuturesUnordered::new();
    let mut replication_health = tokio::time::interval(std::time::Duration::from_millis(250));
    let mut local_cache_prune = tokio::time::interval_at(
        tokio::time::Instant::now() + LOCAL_CACHE_PRUNE_PERIOD,
        LOCAL_CACHE_PRUNE_PERIOD,
    );
    local_cache_prune.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let shutdown_mode = loop {
        celld::asyncrt::select! {
            connection = listener.accept() => {
                let (stream, _) = connection?;
                connections.push(serve_http_connection(
                    stream,
                    HttpSurface::Public,
                    app.clone(),
                    shutdown_tx.clone(),
                    connection_drain.clone(),
                    connection_grace,
                ));
            }
            connection = internal_listener.accept() => {
                let (stream, _) = connection?;
                connections.push(serve_http_connection(
                    stream,
                    HttpSurface::Internal,
                    app.clone(),
                    shutdown_tx.clone(),
                    connection_drain.clone(),
                    connection_grace,
                ));
            }
            Some(()) = connections.next(), if !connections.is_empty() => {}
            call = do_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("Durable Object call channel closed");
                };
                do_calls.push(Box::pin(dispatch_do_call(app.clone(), call)));
            }
            Some(()) = do_calls.next(), if !do_calls.is_empty() => {}
            req = gate_rx.recv() => {
                let Some(req) = req else {
                    anyhow::bail!("output-gate channel closed");
                };
                gate_calls.push(Box::pin(dispatch_gate(app.clone(), req)));
            }
            Some(()) = gate_calls.next(), if !gate_calls.is_empty() => {}
            call = service_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("service call channel closed");
                };
                service_calls.push(Box::pin(dispatch_service_call(app.clone(), call)));
            }
            call = service_rpc_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("service RPC channel closed");
                };
                service_calls.push(Box::pin(dispatch_service_rpc(app.clone(), call)));
            }
            Some(()) = service_calls.next(), if !service_calls.is_empty() => {}
            call = queue_dispatch_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("Queue dispatch channel closed");
                };
                queue_calls.push(Box::pin(dispatch_queue_batch(app.clone(), call)));
            }
            Some(()) = queue_calls.next(), if !queue_calls.is_empty() => {}
            call = rpc_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("Durable Object RPC channel closed");
                };
                do_calls.push(Box::pin(dispatch_rpc_call(app.clone(), call)));
            }
            call = asset_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("asset call channel closed");
                };
                asset_calls.push(Box::pin(dispatch_asset_call(app.clone(), call)));
            }
            Some(()) = asset_calls.next(), if !asset_calls.is_empty() => {}
            socket = websocket_rx.recv() => {
                let Some(socket) = socket else {
                    anyhow::bail!("WebSocket channel closed");
                };
                websockets.push(socket);
            }
            Some(()) = websockets.next(), if !websockets.is_empty() => {}
            _ = local_cache_prune.tick(), if local_cache_replication.is_some()
                && local_cache_max_bytes.is_some() && cache_prunes.is_empty() => {
                let replication = local_cache_replication.clone().unwrap();
                let max_bytes = local_cache_max_bytes.unwrap();
                cache_prunes.push(Box::pin(async move {
                    let result = celld::asyncrt::blocking(move || {
                        replication.prune_local_cache(max_bytes)
                    }).await;
                    (max_bytes, result)
                }));
            }
            Some((max_bytes, result)) = cache_prunes.next(), if !cache_prunes.is_empty() => {
                match result {
                    Ok(Ok((kept, evicted, bytes))) if evicted > 0 => {
                        tracing::info!(
                            event = "local_cache_pruned",
                            kept,
                            evicted,
                            bytes,
                            max_bytes,
                            "pruned least-recently-used eviction snapshots"
                        );
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "local cache inventory failed");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "local cache pruning task failed");
                    }
                }
            }
            outbound = outbound_ws_rx.recv() => {
                let Some(outbound) = outbound else {
                    anyhow::bail!("outbound WebSocket channel closed");
                };
                let app = app.clone();
                websockets.push(Box::pin(async move {
                    if let Err(error) = outbound_websocket_task(app, outbound).await {
                        eprintln!("celld outbound WebSocket failed: {error:#}");
                    }
                }));
            }
            mode = shutdown_rx.recv() => break mode.unwrap_or(ShutdownMode::Handoff),
            _ = sigterm.recv() => break ShutdownMode::Handoff,
            _ = sigint.recv() => break ShutdownMode::Handoff,
            code = fence_rx.recv() => {
                exit_flushed(code.unwrap_or(3));
            }
            actor_exit = actor_exit_rx.recv() => {
                let error = match actor_exit {
                    Some(Err(error)) => error,
                    Some(Ok(())) => "the core actor stopped unexpectedly".to_string(),
                    None => "the core actor thread panicked".to_string(),
                };
                tracing::error!(
                    event = "core_actor_exit",
                    %error,
                    "SELF-FENCE: the core actor exited unexpectedly"
                );
                exit_flushed(3);
            }
            _ = replication_health.tick() => {
                if let Some(runtime) = &app.runtime {
                    match runtime.replication_status() {
                        Ok(None) => {}
                        Ok(Some(status)) => {
                            eprintln!("SELF-FENCE: replication process exited unexpectedly: {status}");
                            exit_flushed(3);
                        }
                        Err(error) => {
                            eprintln!("SELF-FENCE: replication process health check failed: {error}");
                            exit_flushed(3);
                        }
                    }
                }
            }
        }
    };
    // This deadline starts at the stop signal and covers token acquisition,
    // request drain, ownership handoff, transport flush, and local durability
    // shutdown. Progress can extend only the inner stall deadline. The
    // absolute bound leaves the orchestrator time to observe an orderly exit
    // instead of replacing a progressing donor with SIGKILL.
    let shutdown_started = tokio::time::Instant::now();
    let process_deadline = shutdown_started + std::time::Duration::from_millis(shutdown_total_ms);
    // A donor normally spends its complete budget making safe handoff
    // progress. Reserve at most one second, and never more than ten percent of
    // a short configured budget, for the token CAS. Otherwise a deadline-cut
    // donor leaves the replacement behind the 120-second token TTL and turns
    // serialization into artificial rollout recovery.
    let token_release_reserve_ms = if shutdown_mode == ShutdownMode::Handoff
        && fleet_bucket.is_some()
        && drain_token_wait_ms > 0
    {
        (shutdown_total_ms / 10).min(1_000)
    } else {
        0
    };
    let handoff_deadline =
        process_deadline - std::time::Duration::from_millis(token_release_reserve_ms);
    // Stop public admission immediately. Before a handoff changes any cell
    // lifecycle, finish the public requests which crossed the old admission
    // gate. A DO call reaches its output gate only after it writes, so
    // beginning the final durability cut first would let that call cross the
    // cut without a core activity pin. Peer connections stay open:
    // cells outside the current batch must remain reachable during handoff.
    app.draining
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let stall_window = std::time::Duration::from_millis(drain_ms);
    // New drain-time connections need a fresh watch version. A receiver cloned
    // from `connection_drain` after its signal would close immediately, before
    // it could serve a health response or authenticated peer request.
    let (drain_connection_tx, drain_connection) = watch::channel(false);
    let mut drain_connections: FuturesUnordered<ConnectionFuture> = FuturesUnordered::new();
    // Each acceptor waits until this loop receives its previous connection.
    // One surface therefore cannot occupy both slots and block the other.
    let (shutdown_connection_tx, mut shutdown_connection_rx) = mpsc::channel(2);
    #[cfg(all(test, celld_internal_tests))]
    let mut shutdown_accept_failure_recovered_for_test = !shutdown_accept_failure_test_active;
    #[cfg(all(test, celld_internal_tests))]
    shutdown_accept_failure_test_checkpoint(
        shutdown_accept_failure_test_active,
        "listener failure armed",
    );
    #[cfg(all(test, celld_internal_tests))]
    {
        spawn_shutdown_acceptor(
            listener,
            HttpSurface::Public,
            shutdown_connection_tx.clone(),
            shutdown_accept_failure_test_active,
        );
        spawn_shutdown_acceptor(
            internal_listener,
            HttpSurface::Internal,
            shutdown_connection_tx,
            false,
        );
    }
    #[cfg(not(all(test, celld_internal_tests)))]
    {
        spawn_shutdown_acceptor(
            listener,
            HttpSurface::Public,
            shutdown_connection_tx.clone(),
        );
        spawn_shutdown_acceptor(
            internal_listener,
            HttpSurface::Internal,
            shutdown_connection_tx,
        );
    }
    let mut drain_token_hold: Option<celld::drain_token::Hold> = None;
    let handoff_started = if shutdown_mode == ShutdownMode::Handoff {
        // Serialize donors: claim the fleet drain token beside the
        // admitted-request drain, so simultaneous stop signals hand off one
        // node at a time. A waiting donor keeps serving peers and refuses
        // new handoffs because it is already draining. The wait has its own
        // bound inside the task; the pre-drain deadline never covers it.
        let drain_token: Arc<std::sync::Mutex<Option<celld::drain_token::Outcome>>> =
            Arc::new(std::sync::Mutex::new(None));
        match &fleet_bucket {
            Some(bucket) if drain_token_wait_ms > 0 => {
                let bucket = bucket.clone();
                let slot = drain_token.clone();
                let token_node = clean_reload_node.clone();
                let baseline_ownership = ready_ownership.clone();
                let wait = std::time::Duration::from_millis(drain_token_wait_ms)
                    .min(handoff_deadline.saturating_duration_since(tokio::time::Instant::now()));
                tokio::spawn(async move {
                    let now_ms = celld::ownership_store::now_ms();
                    let restoration_baseline = match baseline_ownership {
                        Some(ownership) => match ownership.read_capacity_peers().await {
                            Ok(peers) => {
                                let mut baseline = peers
                                    .into_iter()
                                    .filter(|peer| peer.expires_ms > now_ms)
                                    .map(|peer| celld_logic::drain::RestorationBaseline {
                                        node: peer.node,
                                        restoring: peer.restoring,
                                    })
                                    .collect::<Vec<_>>();
                                baseline.sort_by(|left, right| left.node.cmp(&right.node));
                                baseline
                            }
                            Err(error) => {
                                tracing::warn!(
                                    event = "drain_restoration_baseline_unavailable",
                                    %error,
                                    "the donor could not snapshot the pre-drain restore baseline"
                                );
                                Vec::new()
                            }
                        },
                        None => Vec::new(),
                    };
                    tracing::info!(
                        event = "drain_restoration_baseline",
                        nodes = restoration_baseline.len(),
                        maximum = restoration_baseline
                            .iter()
                            .map(|baseline| baseline.restoring)
                            .max()
                            .unwrap_or(0),
                        "captured the pre-drain restore baseline"
                    );
                    let outcome = celld::drain_token::acquire(
                        &bucket,
                        &token_node,
                        wait,
                        restoration_baseline,
                    )
                    .await;
                    *slot.lock().expect("drain token slot") = Some(outcome);
                });
            }
            _ => {
                *drain_token.lock().expect("drain token slot") =
                    Some(celld::drain_token::Outcome::Disabled);
            }
        }
        let pre_drain_deadline = (tokio::time::Instant::now() + stall_window).min(handoff_deadline);
        let mut pre_drain_tick = tokio::time::interval(std::time::Duration::from_millis(5));
        let mut admitted = None;
        let admitted_drained = loop {
            if admitted.is_none() && app.public_in_flight() == 0 {
                admitted = Some(true);
            }
            if let Some(admitted_drained) = admitted {
                let token_ready = drain_token.lock().expect("drain token slot").is_some();
                #[cfg(all(test, celld_internal_tests))]
                let token_ready = token_ready && shutdown_accept_failure_recovered_for_test;
                if token_ready {
                    break admitted_drained;
                }
            }
            celld::asyncrt::select! {
                _ = tokio::time::sleep_until(pre_drain_deadline), if admitted.is_none() => {
                    admitted = Some(false);
                }
                _ = tokio::time::sleep_until(handoff_deadline) => break false,
                _ = pre_drain_tick.tick() => {}
                connection = shutdown_connection_rx.recv() => {
                    let Some(connection) = connection else {
                        anyhow::bail!("shutdown listener tasks stopped during pre-drain");
                    };
                    drain_connections.push(serve_http_connection(
                        connection.stream,
                        connection.surface,
                        app.clone(),
                        shutdown_tx.clone(),
                        drain_connection.clone(),
                        connection_grace,
                    ));
                    #[cfg(all(test, celld_internal_tests))]
                    if connection.recovered_after_error {
                        shutdown_accept_failure_recovered_for_test = true;
                    }
                    let _ = connection.received.send(());
                }
                Some(_) = drain_connections.next(), if !drain_connections.is_empty() => {}
                Some(_) = connections.next(), if !connections.is_empty() => {}
                call = do_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Durable Object call channel closed during shutdown pre-drain");
                    };
                    do_calls.push(Box::pin(dispatch_do_call(app.clone(), call)));
                }
                Some(_) = do_calls.next(), if !do_calls.is_empty() => {}
                req = gate_rx.recv() => {
                    let Some(req) = req else {
                        anyhow::bail!("output-gate channel closed during shutdown pre-drain");
                    };
                    gate_calls.push(Box::pin(dispatch_gate(app.clone(), req)));
                }
                Some(_) = gate_calls.next(), if !gate_calls.is_empty() => {}
                call = service_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("service call channel closed during shutdown pre-drain");
                    };
                    service_calls.push(Box::pin(dispatch_service_call(app.clone(), call)));
                }
                call = service_rpc_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("service RPC channel closed during shutdown pre-drain");
                    };
                    service_calls.push(Box::pin(dispatch_service_rpc(app.clone(), call)));
                }
                Some(_) = service_calls.next(), if !service_calls.is_empty() => {}
                call = queue_dispatch_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Queue dispatch channel closed during shutdown pre-drain");
                    };
                    queue_calls.push(Box::pin(dispatch_queue_batch(app.clone(), call)));
                }
                Some(_) = queue_calls.next(), if !queue_calls.is_empty() => {}
                call = rpc_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Durable Object RPC channel closed during shutdown pre-drain");
                    };
                    do_calls.push(Box::pin(dispatch_rpc_call(app.clone(), call)));
                }
                call = asset_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("asset call channel closed during shutdown pre-drain");
                    };
                    asset_calls.push(Box::pin(dispatch_asset_call(app.clone(), call)));
                }
                Some(_) = asset_calls.next(), if !asset_calls.is_empty() => {}
                code = fence_rx.recv() => exit_flushed(code.unwrap_or(3)),
                actor_exit = actor_exit_rx.recv() => {
                    let error = match actor_exit {
                        Some(Err(error)) => error,
                        Some(Ok(())) => "the core actor stopped unexpectedly".to_string(),
                        None => "the core actor thread panicked".to_string(),
                    };
                    tracing::error!(event = "core_actor_exit", %error, "SELF-FENCE during shutdown pre-drain");
                    exit_flushed(3);
                }
            }
        };
        drain_token_hold = match drain_token.lock().expect("drain token slot").take() {
            Some(celld::drain_token::Outcome::Acquired(hold)) => Some(hold),
            _ => None,
        };
        if !admitted_drained {
            tracing::warn!(
                event = "shutdown_pre_drain_forced",
                public_in_flight = app.public_in_flight(),
                do_calls = do_calls.len(),
                gate_calls = gate_calls.len(),
                service_calls = service_calls.len(),
                queue_calls = queue_calls.len(),
                asset_calls = asset_calls.len(),
                grace_ms = stall_window.as_millis(),
                "shutdown reached its admitted-request deadline before cell handoff"
            );
        }
        if admitted_drained {
            let _ = app.tx.send(Message::ReleaseAll);
        }
        admitted_drained
    } else {
        let _ = connection_drain_tx.send(true);
        let _ = app.tx.send(Message::BeginPreserve);
        true
    };
    // Progress extends the stall deadline, but never the complete process
    // deadline. A deadline-cut cell keeps its durable owner record and follows
    // the same lease-expiry recovery path as an abrupt process loss.
    let mut stall_deadline = (tokio::time::Instant::now() + stall_window).min(handoff_deadline);
    let mut handed_off = 0;
    let mut process_deadline_fired = false;
    let mut handoff = tokio::time::interval(std::time::Duration::from_millis(50));
    let drained = if shutdown_mode == ShutdownMode::Handoff && !handoff_started {
        false
    } else {
        loop {
            let shell_drained = connections.is_empty()
                && do_calls.is_empty()
                && gate_calls.is_empty()
                && service_calls.is_empty()
                && queue_calls.is_empty()
                && asset_calls.is_empty()
                && websockets.is_empty();
            // The actor can be busy driving an immediate-effect failure loop, so
            // a status request is not itself allowed to bypass the drain deadline.
            let status = before_process_deadline(
                handoff_deadline,
                tokio::time::timeout(std::time::Duration::from_millis(50), app.drain_status()),
            )
            .await
            .and_then(Result::ok);
            if shutdown_mode == ShutdownMode::Handoff
                && status.is_some_and(|status| status.handed_off > handed_off)
            {
                handed_off = status.expect("checked drain status").handed_off;
                stall_deadline = (tokio::time::Instant::now() + stall_window).min(handoff_deadline);
            }
            // A drain that makes progress can outlive the token TTL, so the
            // holder renews and only a dead holder lets the token lapse.
            let renew_due = drain_token_hold.as_ref().is_some_and(|hold| {
                celld::drain_token::renew_due(hold, celld::ownership_store::now_ms())
            });
            if renew_due {
                let mut lost = false;
                if let (Some(bucket), Some(hold)) = (&fleet_bucket, drain_token_hold.as_mut()) {
                    let Some(renewed) = before_process_deadline(
                        handoff_deadline,
                        celld::drain_token::renew(bucket, &clean_reload_node, hold),
                    )
                    .await
                    else {
                        process_deadline_fired = true;
                        break false;
                    };
                    lost = !renewed;
                }
                if lost {
                    drain_token_hold = None;
                }
            }
            let core_drained = status.is_some_and(|status| match shutdown_mode {
                ShutdownMode::Handoff => {
                    status.occupied == 0
                        && status.activating == 0
                        && status.quiescing == 0
                        && status.evicting == 0
                        && status.releasing == 0
                        && status.adopting == 0
                }
                ShutdownMode::Preserve => status.activating == 0 && status.evicting == 0,
            });
            let completely_drained = match shutdown_mode {
                ShutdownMode::Handoff => core_drained,
                ShutdownMode::Preserve => shell_drained && core_drained,
            };
            if completely_drained {
                break true;
            }
            celld::asyncrt::select! {
                _ = tokio::time::sleep_until(stall_deadline) => {
                    process_deadline_fired = tokio::time::Instant::now() >= handoff_deadline;
                    break false;
                },
                _ = handoff.tick() => {}
                connection = shutdown_connection_rx.recv() => {
                    let Some(connection) = connection else {
                        anyhow::bail!("shutdown listener tasks stopped during drain");
                    };
                    drain_connections.push(serve_http_connection(
                        connection.stream,
                        connection.surface,
                        app.clone(),
                        shutdown_tx.clone(),
                        drain_connection.clone(),
                        connection_grace,
                    ));
                    let _ = connection.received.send(());
                }
                Some(_) = drain_connections.next(), if !drain_connections.is_empty() => {}
                Some(_) = connections.next(), if !connections.is_empty() => {}
                call = do_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Durable Object call channel closed during shutdown");
                    };
                    do_calls.push(Box::pin(dispatch_do_call(app.clone(), call)));
                }
                Some(_) = do_calls.next(), if !do_calls.is_empty() => {}
                req = gate_rx.recv() => {
                    let Some(req) = req else {
                        anyhow::bail!("output-gate channel closed during shutdown");
                    };
                    gate_calls.push(Box::pin(dispatch_gate(app.clone(), req)));
                }
                Some(_) = gate_calls.next(), if !gate_calls.is_empty() => {}
                call = service_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("service call channel closed during shutdown");
                    };
                    service_calls.push(Box::pin(dispatch_service_call(app.clone(), call)));
                }
                call = service_rpc_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("service RPC channel closed during shutdown");
                    };
                    service_calls.push(Box::pin(dispatch_service_rpc(app.clone(), call)));
                }
                Some(_) = service_calls.next(), if !service_calls.is_empty() => {}
                call = queue_dispatch_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Queue dispatch channel closed during shutdown");
                    };
                    queue_calls.push(Box::pin(dispatch_queue_batch(app.clone(), call)));
                }
                Some(_) = queue_calls.next(), if !queue_calls.is_empty() => {}
                call = rpc_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Durable Object RPC channel closed during shutdown");
                    };
                    do_calls.push(Box::pin(dispatch_rpc_call(app.clone(), call)));
                }
                call = asset_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("asset call channel closed during shutdown");
                    };
                    asset_calls.push(Box::pin(dispatch_asset_call(app.clone(), call)));
                }
                Some(_) = asset_calls.next(), if !asset_calls.is_empty() => {}
                socket = websocket_rx.recv() => {
                    let Some(socket) = socket else {
                        anyhow::bail!("WebSocket channel closed during shutdown");
                    };
                    websockets.push(socket);
                }
                outbound = outbound_ws_rx.recv() => {
                    let Some(outbound) = outbound else {
                        anyhow::bail!("outbound WebSocket channel closed during shutdown");
                    };
                    let app = app.clone();
                    websockets.push(Box::pin(async move {
                        if let Err(error) = outbound_websocket_task(app, outbound).await {
                            eprintln!("celld outbound WebSocket failed: {error:#}");
                        }
                    }));
                }
                Some(_) = websockets.next(), if !websockets.is_empty() => {}
                code = fence_rx.recv() => exit_flushed(code.unwrap_or(3)),
                actor_exit = actor_exit_rx.recv() => {
                    let error = match actor_exit {
                        Some(Err(error)) => error,
                        Some(Ok(())) => "the core actor stopped unexpectedly".to_string(),
                        None => "the core actor thread panicked".to_string(),
                    };
                    tracing::error!(event = "core_actor_exit", %error, "SELF-FENCE during shutdown");
                    exit_flushed(3);
                }
            }
        }
    };
    let _ = connection_drain_tx.send(true);
    let _ = drain_connection_tx.send(true);
    // Release the token whether or not the drain completed: the process is
    // about to exit either way, and the next donor must not wait out the TTL.
    // The connection flush below needs nothing from the token, so releasing
    // first keeps the next donor off the TTL during the flush grace.
    if let (Some(bucket), Some(hold)) = (&fleet_bucket, drain_token_hold.take()) {
        if before_process_deadline(
            process_deadline,
            celld::drain_token::release(bucket, &clean_reload_node, hold),
        )
        .await
        .is_none()
        {
            tracing::warn!(
                event = "drain_token_release_deadline",
                "the process deadline skipped or cancelled the drain token release; the TTL bounds the residue"
            );
        }
    }
    #[cfg(all(test, celld_internal_tests))]
    shutdown_accept_failure_test_checkpoint(
        shutdown_accept_failure_test_active,
        "drain token release finished",
    );
    if !handoff_started {
        tracing::error!(
            event = "shutdown_handoff_not_started",
            drain_ms,
            admitted_public = app.public_in_flight(),
            "the shutdown pre-drain did not finish"
        );
    } else if !drained && shutdown_mode == ShutdownMode::Handoff {
        let drain_state = before_process_deadline(
            process_deadline,
            tokio::time::timeout(std::time::Duration::from_millis(50), app.snapshot()),
        )
        .await
        .and_then(Result::ok)
        .unwrap_or_else(|| "{\"error\":\"process_deadline\"}".to_string());
        match before_process_deadline(
            process_deadline,
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                app.drain_status(),
            ),
        )
        .await
        .and_then(Result::ok)
        {
            Some(status) => tracing::error!(
                event = "shutdown_handoff_stalled",
                drain_ms,
                shutdown_total_ms,
                process_deadline_fired,
                occupied = status.occupied,
                activating = status.activating,
                quiescing = status.quiescing,
                evicting = status.evicting,
                releasing = status.releasing,
                adopting = status.adopting,
                handed_off = status.handed_off,
                drain_state,
                "the shutdown handoff made no progress"
            ),
            None => tracing::error!(
                event = "shutdown_handoff_stalled",
                drain_ms,
                shutdown_total_ms,
                process_deadline_fired,
                drain_state,
                "the shutdown handoff made no observable progress because the core status was unavailable"
            ),
        }
    } else if !drained {
        eprintln!(
            "celld preserve drain reached its {drain_ms}ms deadline: connections={} do_calls={} gate_calls={} service_calls={} queue_calls={} asset_calls={} websockets={}",
            connections.len(),
            do_calls.len(),
            gate_calls.len(),
            service_calls.len(),
            queue_calls.len(),
            asset_calls.len(),
            websockets.len(),
        );
    }
    // Let every accepted connection FINISH its current response before the
    // process exits. The demand-driven drain can complete in milliseconds
    // on a quiet node, and exit_flushed is a hard process::exit — without
    // this wait, a diagnostic request accepted during the drain window is
    // reset mid-response, and a WebSocket close frame written during the
    // release races the exit (#262). The watches above already told each
    // connection to close after the response it is serving, so this drains
    // fast; the grace only bounds a stuck peer. The dispatch sets (do_calls,
    // gate_calls, service_calls, asset_calls) are NOT polled here: a
    // connection whose response depends on one stalls for the grace and is
    // then cut, so a drain-window internal route must never dispatch into a
    // cell. Today none does — /state, /health, and /peer/abort answer from the
    // shell.
    let flush_connections = async {
        while drain_connections.next().await.is_some() {}
        while connections.next().await.is_some() {}
        while websockets.next().await.is_some() {}
    };
    flush_shutdown_connections(
        process_deadline,
        celld::js::ws_await_all_flushes(),
        flush_connections,
    )
    .await;
    // The graceful-shutdown drain point: seal our node-log record so the
    // next incarnation starts with no gather. Guarded inside — it seals
    // only when every shipped frame is bucket-covered.
    let durability_quiesced = if let Some(owner) = &mut durability_owner {
        let remaining = process_deadline.saturating_duration_since(tokio::time::Instant::now());
        let completed = if remaining.is_zero() {
            owner.stop_local_now();
            false
        } else {
            owner.quiesce_and_seal_within(remaining).await
        };
        if !completed {
            eprintln!("celld durability quiesce exceeded the process deadline");
        }
        completed
    } else {
        true
    };
    #[cfg(all(test, celld_internal_tests))]
    shutdown_accept_failure_test_checkpoint(
        shutdown_accept_failure_test_active,
        "durability quiesce finished",
    );
    if clean_reload_is_eligible(durability_quiesced, drained, shutdown_mode) {
        let presence = before_process_deadline(process_deadline, app.presence()).await;
        let prepared = match (&app.runtime, presence) {
            (Some(runtime), Some(Some(presence))) => {
                before_process_deadline(
                    process_deadline,
                    runtime.prepare_clean_reload(&presence.cells),
                )
                .await
            }
            (_, Some(None)) | (None, Some(Some(_))) => Some(Err(anyhow::anyhow!(
                "clean reload requires a runtime and a resident snapshot"
            ))),
            (_, None) => None,
        };
        match prepared {
            Some(Ok(pruned)) => {
                match before_process_deadline(process_deadline, app.healthy()).await {
                    Some(true) => match celld::runtime::write_clean_reload_marker(
                        &data_dir,
                        &clean_reload_node,
                        &process_generation,
                    ) {
                        Ok(()) => tracing::info!(
                            event = "clean_reload_prepared",
                            stale_live_databases_pruned = pruned,
                            "prepared local cells for an exact-generation reload"
                        ),
                        Err(error) => tracing::warn!(
                            event = "clean_reload_abandoned",
                            %error,
                            "could not publish the clean local reload certificate"
                        ),
                    },
                    Some(false) => tracing::warn!(
                        event = "clean_reload_abandoned",
                        "node authority was lost while local cells were closing"
                    ),
                    None => tracing::warn!(
                        event = "clean_reload_abandoned",
                        "the process deadline skipped the final authority check"
                    ),
                }
            }
            Some(Err(error)) => tracing::warn!(
                event = "clean_reload_abandoned",
                %error,
                "local reload preparation failed; replacement will use normal recovery"
            ),
            None => tracing::warn!(
                event = "clean_reload_abandoned",
                "local reload preparation exceeded the shutdown deadline"
            ),
        }
    }
    if let Some(owner) = &mut durability_owner {
        let remaining = process_deadline.saturating_duration_since(tokio::time::Instant::now());
        let completed = if remaining.is_zero() {
            owner.stop_local_now();
            false
        } else {
            owner.shutdown_local_within(remaining).await
        };
        if !completed {
            eprintln!("celld local durability shutdown exceeded the process deadline");
        }
    }
    #[cfg(all(test, celld_internal_tests))]
    shutdown_accept_failure_test_checkpoint(
        shutdown_accept_failure_test_active,
        "local durability shutdown finished",
    );
    // Exit without unwinding. Returning from here drops the tokio runtime
    // and the V8 platform underneath tasks and isolates that are still
    // alive -- on a deadline-cut drain that teardown segfaults (status 139
    // observed fleet-wide, 2026-08-10). Nothing below needs a destructor:
    // every release the drain completed proved durability first, and a
    // cell the deadline cut off keeps its owner record exactly as a kill
    // would have left it.
    #[cfg(all(test, celld_internal_tests))]
    shutdown_accept_failure_test_checkpoint(
        shutdown_accept_failure_test_active,
        "final log flush reached",
    );
    exit_flushed(0);
}

use celld::machine::{
    lease_ttl_ms_from_environment, local_cache_max_bytes_from_environment, random_node_session_id,
    random_peer_key, DEFAULT_MAX_OUTBOUND_WEBSOCKETS, PEER_CONNECT_TIMEOUT,
};

/// Signals cancellation when the connection handling this request goes away.
struct HangUp(Option<oneshot::Sender<()>>);

impl Drop for HangUp {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// Abandons a forwarded fetch on the owner when the peer connection carrying
/// it goes away. After the handler answers, a drop cancels only its response
/// body pin because the isolate has no unanswered request left to abort.
struct AbortPeerFetchOnHangUp {
    runtime: RuntimeManager,
    scope: String,
    request_id: Option<celld::js::RequestId>,
    drain_pins: DrainPinRegistry,
    handler_active: bool,
}

impl AbortPeerFetchOnHangUp {
    fn disarm(&mut self) {
        self.request_id = None;
    }

    fn handler_answered(&mut self) {
        self.handler_active = false;
    }
}

impl Drop for AbortPeerFetchOnHangUp {
    fn drop(&mut self) {
        if let Some(request_id) = self.request_id {
            let _ = self
                .drain_pins
                .cancel_request(request_id, "peer_connection_closed");
            if self.handler_active {
                self.runtime.abort_fetch(&self.scope, request_id);
            }
        }
    }
}

fn forwarder_response_stream(
    stream: celld::js::HttpChunkStream,
    mut abort: RemoteFetchAbortGuard,
) -> celld::js::HttpChunkStream {
    abort.body_active();
    Box::pin(futures_util::stream::unfold(
        (stream, Some(abort)),
        |(mut stream, mut abort)| async move {
            match stream.next().await {
                Some(chunk) => {
                    // An upstream body error already ended the owner
                    // transport. A second connection carrying an abort adds
                    // no signal; only a downstream drop while the upstream is
                    // still live needs the explicit peer request.
                    if chunk.is_err() {
                        if let Some(mut abort) = abort.take() {
                            abort.disarm();
                        }
                    }
                    Some((chunk, (stream, abort)))
                }
                None => {
                    if let Some(mut abort) = abort.take() {
                        abort.disarm();
                    }
                    None
                }
            }
        },
    ))
}

fn owner_response_stream(
    stream: celld::js::HttpChunkStream,
    activity: ActivityGuard,
    abort: AbortPeerFetchOnHangUp,
) -> celld::js::HttpChunkStream {
    let cancellation = activity.cancellation();
    Box::pin(futures_util::stream::unfold(
        (stream, Some(activity), Some(abort), cancellation),
        |(mut stream, activity, mut abort, mut cancellation)| async move {
            let chunk = if *cancellation.borrow() {
                None
            } else {
                celld::asyncrt::select! {
                    chunk = stream.next() => chunk,
                    changed = cancellation.changed() => {
                        let _ = changed;
                        None
                    }
                }
            };
            match chunk {
                Some(chunk) => Some((chunk, (stream, activity, abort, cancellation))),
                None => {
                    if let Some(mut abort) = abort.take() {
                        abort.disarm();
                    }
                    drop(activity);
                    None
                }
            }
        },
    ))
}

fn local_response_stream(
    stream: celld::js::HttpChunkStream,
    activity: ActivityGuard,
) -> celld::js::HttpChunkStream {
    let cancellation = activity.cancellation();
    Box::pin(futures_util::stream::unfold(
        (stream, Some(activity), cancellation),
        |(mut stream, activity, mut cancellation)| async move {
            let chunk = if *cancellation.borrow() {
                None
            } else {
                celld::asyncrt::select! {
                    chunk = stream.next() => chunk,
                    changed = cancellation.changed() => {
                        let _ = changed;
                        None
                    }
                }
            };
            match chunk {
                Some(chunk) => Some((chunk, (stream, activity, cancellation))),
                None => {
                    drop(activity);
                    None
                }
            }
        },
    ))
}

#[cfg(all(test, celld_internal_tests))]
mod durability_lifecycle_private {
    include!(env!("CELLD_CONFORMANCE_MAIN_DURABILITY_TESTS"));
}
