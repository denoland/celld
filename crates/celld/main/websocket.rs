// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! WebSockets on the ingress: accepting them, proxying them to the owner,
//! and the tasks that pump each one.
//!
//! A socket outlives the request that opened it, so each becomes its own
//! task. Three shapes exist — a local socket to a cell on this node, a
//! socket proxied to the owning peer, and a socket the worker opened
//! outbound — and they differ only in what sits on the far end.
use super::*;

async fn dispatch_ws_message(
    app: &AppHandle,
    scope: &str,
    ws_id: u64,
    data: celld::js::WsIn,
) -> anyhow::Result<()> {
    // The auto-response short circuit: a matched text frame is answered here
    // in the shell and never becomes a `webSocketMessage`. No routing, no
    // activity, no wake — a hibernated cell stays hibernated, which is the
    // feature.
    if let celld::js::WsIn::Text(text) = &data {
        if let Some(response) = celld::js::ws_auto_response(scope, ws_id, text) {
            celld::js::ws_emit_batch(vec![(ws_id, celld::js::WsOut::Text(response))]);
            return Ok(());
        }
    }
    let Routed { request, route } = app
        .websocket_request(scope.to_string(), ws_id)
        .await
        .map_err(|error| anyhow::anyhow!("route WebSocket {scope}: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "WebSocket owner moved off node");
    let activity = app.activity(request, scope.to_string());
    let dispatch = match app
        .runtime
        .as_ref()
        .context("no cell runtime")?
        .ws_message(scope.to_string(), ws_id, data)
        .await
    {
        Ok(dispatch) => dispatch,
        // A handler that failed after it committed opens the barrier a
        // successful writer's batch opens, with no frames behind it: the
        // commit is as unproven either way, and a read-only batch or response
        // that followed would otherwise trail nothing and reveal it (#715).
        // The frames it captured before it failed are dropped, as before.
        Err(error) => {
            if app.output_gate {
                if let Some(position) = celld::js::failed_write_position(&error) {
                    // The barrier is registered before the activity guard
                    // drops: the core reads the still-pinned request when it
                    // opens the barrier, and a guard dropped first would let
                    // the cell be released under an unregistered write.
                    if let Err(stopped) = app
                        .ws_output(request, scope.to_string(), Vec::new(), Some(position), None)
                        .await
                    {
                        tracing::warn!(scope, position, %stopped, "no barrier for a failed webSocketMessage handler's write");
                    }
                }
            }
            drop(activity);
            return Err(error);
        }
    };
    // The gate captured the handler's outbound frames. With the gate armed, hand
    // them to the cell's barrier queue; else flush them as the handler produced
    // them. Either way the frames only reach a socket from here.
    if !app.output_gate {
        celld::js::ws_emit_batch(dispatch.frames);
    } else if !dispatch.frames.is_empty() || dispatch.write_position.is_some() {
        if let Err(stopped) = app
            .ws_output(
                request,
                scope.to_string(),
                dispatch.frames,
                dispatch.write_position,
                dispatch.observed_position,
            )
            .await
        {
            tracing::warn!(scope, %stopped, "no barrier for a webSocketMessage handler's output");
        }
    }
    drop(activity);
    Ok(())
}

async fn dispatch_ws_closed(
    app: &AppHandle,
    scope: &str,
    ws_id: u64,
    code: u16,
    reason: String,
    was_clean: bool,
) -> anyhow::Result<()> {
    let Routed { request, route } = app
        .websocket_request(scope.to_string(), ws_id)
        .await
        .map_err(|error| anyhow::anyhow!("route WebSocket close {scope}: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "WebSocket owner moved off node");
    let _activity = app.activity(request, scope.to_string());
    let answer = app
        .runtime
        .as_ref()
        .context("no cell runtime")?
        .ws_closed(scope.to_string(), ws_id, code, reason, was_clean)
        .await;
    gate_lifecycle_write(app, request, scope, &answer).await;
    answer.map(|_| ())
}

/// Open the barrier a lifecycle handler's write needs.
///
/// `webSocketOpen` and `webSocketClose` answer nothing a client sees, so a
/// write they made opens a barrier with no frames behind it, whether the
/// handler returned or failed after it committed. A read-only batch or
/// response that follows then trails that barrier instead of revealing the
/// write while a crash can still lose it (#715).
async fn gate_lifecycle_write(
    app: &AppHandle,
    request: u64,
    scope: &str,
    answer: &anyhow::Result<Option<u64>>,
) {
    let position = match answer {
        Ok(position) => *position,
        Err(error) => celld::js::failed_write_position(error),
    };
    if let (true, Some(position)) = (app.output_gate, position) {
        tracing::debug!(
            scope,
            position,
            failed = answer.is_err(),
            "gate lifecycle write"
        );
        if let Err(stopped) = app
            .ws_output(request, scope.to_string(), Vec::new(), Some(position), None)
            .await
        {
            tracing::warn!(scope, position, %stopped, "no barrier for a lifecycle handler's write");
        }
    }
}

async fn finish_websocket(
    app: &AppHandle,
    target: &celld::js::WsTarget,
    code: u16,
    reason: String,
    was_clean: bool,
) {
    let _ = dispatch_ws_closed(app, &target.scope, target.id, code, reason, was_clean).await;
    app.websocket_closed(target.scope.clone(), target.id);
    celld::js::ws_unregister(target.id);
}

/// Release a WebSocket that the Worker accepted but that celld will not upgrade.
///
/// `accept()` registers the socket and charges it to the scope's regular-socket
/// count before the Worker's response reaches this code, and `WsTarget` is plain
/// data: dropping it releases nothing. A rejection that only returns an error
/// status therefore leaks the registration and the count for the life of the
/// process, and the Worker never sees a close. A remote target is registered on
/// the owner node instead, so the entry node has nothing to release — the same
/// split that the upgrade-failure path makes. 1006 is the code for a connection
/// that closed without a close frame, which is exactly what happened.
async fn reject_accepted_websocket(app: &AppHandle, target: &celld::js::WsTarget, reason: &str) {
    if target.tunnel.is_none() {
        finish_websocket(app, target, 1006, reason.to_string(), false).await;
    }
}

/// One accepted WebSocket response after it returns from JavaScript.
///
/// Keep the response shape in one value through every validation and executor
/// exit. A tunneled response attaches its parked connection lifetime here,
/// so dropping an unfinished response cannot leave host state behind.
enum PendingWebSocket {
    Worker(celld::js::websocket::WorkerWebSocket),
    Cell(celld::js::WsTarget),
    Tunnel {
        target: celld::js::WsTarget,
        upgrade: super::peer_tunnel::TunnelUpgradeClaim,
    },
}

impl PendingWebSocket {
    fn new(websocket: HttpResponseWebSocket) -> anyhow::Result<Self> {
        Ok(match websocket {
            HttpResponseWebSocket::Worker(worker) => Self::Worker(worker),
            HttpResponseWebSocket::Cell(target) => match target.tunnel {
                Some(parked) => Self::Tunnel {
                    target,
                    upgrade: super::peer_tunnel::claim_upgrade(parked)?,
                },
                None => Self::Cell(target),
            },
        })
    }

    fn target(&self) -> Option<&celld::js::WsTarget> {
        match self {
            Self::Worker(_) => None,
            Self::Cell(target) => Some(target),
            Self::Tunnel { target, .. } => Some(target),
        }
    }
}

enum OutboundWebSocketSink {
    Cell { app: Box<AppHandle>, scope: String },
    Isolate(celld::js::WsPullSender),
}

impl OutboundWebSocketSink {
    async fn open(&self, websocket: u64, protocol: String) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, scope } => {
                let Routed { request, route } = app
                    .request(scope.clone())
                    .await
                    .map_err(|error| anyhow::anyhow!("route outbound WebSocket: {error:?}"))?;
                anyhow::ensure!(
                    route == Route::Local,
                    "outbound WebSocket cell moved off node"
                );
                let _activity = app.activity(request, scope.clone());
                app.websocket_opened(scope.clone(), websocket, WebSocketKind::Outbound)
                    .await?;
                let answer = app
                    .runtime
                    .as_ref()
                    .context("no cell runtime")?
                    .ws_open(scope.clone(), websocket, protocol)
                    .await;
                gate_lifecycle_write(app, request, scope, &answer).await;
                if answer.is_err() {
                    app.websocket_closed(scope.clone(), websocket);
                }
                answer.map(|_| ())
            }
            Self::Isolate(tx) => tx
                .send(celld::js::WsPull::Open(protocol))
                .await
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    async fn message(&self, websocket: u64, data: celld::js::WsIn) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, scope } => dispatch_ws_message(app, scope, websocket, data).await,
            Self::Isolate(tx) => tx
                .send(data.into())
                .await
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    async fn closed(
        &self,
        websocket: u64,
        code: u16,
        reason: String,
        was_clean: bool,
    ) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, scope } => {
                let result =
                    dispatch_ws_closed(app, scope, websocket, code, reason, was_clean).await;
                app.websocket_closed(scope.clone(), websocket);
                result
            }
            Self::Isolate(tx) => tx
                .send_close(code, reason, was_clean)
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    fn scope(&self) -> &str {
        match self {
            Self::Cell { scope, .. } => scope,
            Self::Isolate(_) => "",
        }
    }
}

/// Carry frames between a Durable Object's socket and a client end another
/// isolate in this process kept, after a `stub.fetch` upgrade.
///
/// A same-isolate pair links its two ends directly and never involves the
/// host. This pair cannot: the cell end lives in the cell's isolate and the
/// client end in the caller's, and neither can reach the other's heap. So
/// each direction takes the route an external client's frames take —
/// `dispatch_ws_message` into the cell, a pull queue out to the caller —
/// which is also why a hibernatable server end needs nothing special here.
///
/// The task ends when either side closes, and it unregisters both.
async fn local_websocket_pipe(
    app: AppHandle,
    id: u64,
    target: celld::js::WsTarget,
    pull: Option<celld::js::WsPullSender>,
    reply: tokio::sync::oneshot::Sender<anyhow::Result<celld::js::OutboundWsOpen>>,
) -> anyhow::Result<()> {
    let Some(pull) = pull else {
        let _ = reply.send(Err(anyhow::anyhow!(
            "a bound WebSocket target needs an isolate queue"
        )));
        return Ok(());
    };
    // Both registrations flush whatever each end queued before it had
    // anywhere to send: the cell's greeting frame is queued while its own
    // fetch handler still runs, and the caller can send the moment it
    // accepts.
    let (caller_tx, mut from_caller) = mpsc::unbounded_channel();
    let (cell_tx, mut from_cell) = mpsc::unbounded_channel();
    celld::js::ws_register(id, caller_tx);
    celld::js::ws_register(target.id, cell_tx);
    let _ = reply.send(Ok(celld::js::OutboundWsOpen {
        protocol: None,
        declined: None,
    }));
    // Set only by a close the caller sent. A close from the cell needs no
    // entry here: the isolate that sent it has already told the cell's own
    // handler, and telling it again would be a second `webSocketClose`.
    let mut caller_close: Option<(u16, String)> = None;
    loop {
        celld::asyncrt::select! {
            frame = from_caller.recv() => match frame {
                Some(celld::js::WsOut::Text(text)) => {
                    if let Err(error) = dispatch_ws_message(
                        &app, &target.scope, target.id, celld::js::WsIn::Text(text),
                    ).await {
                        tracing::warn!(%error, scope = %target.scope, "kept WebSocket message failed");
                        break;
                    }
                }
                Some(celld::js::WsOut::Binary(bytes)) => {
                    if let Err(error) = dispatch_ws_message(
                        &app, &target.scope, target.id, celld::js::WsIn::Binary(bytes),
                    ).await {
                        tracing::warn!(%error, scope = %target.scope, "kept WebSocket message failed");
                        break;
                    }
                }
                Some(celld::js::WsOut::Close(code, reason)) => {
                    caller_close = Some((code, reason));
                    break;
                }
                // The caller's request retired and took its socket with it.
                None => break,
            },
            frame = from_cell.recv() => match frame {
                Some(celld::js::WsOut::Text(text)) => {
                    if pull.send(celld::js::WsPull::Text(text)).await.is_err() { break; }
                }
                Some(celld::js::WsOut::Binary(bytes)) => {
                    if pull.send(celld::js::WsPull::Binary(bytes)).await.is_err() { break; }
                }
                Some(celld::js::WsOut::Close(code, reason)) => {
                    let _ = pull.send_close(code, reason, true);
                    break;
                }
                None => break,
            },
        }
    }
    if let Some((code, reason)) = caller_close {
        let _ = dispatch_ws_closed(&app, &target.scope, target.id, code, reason, true).await;
    }
    app.websocket_closed(target.scope.clone(), target.id);
    celld::js::ws_unregister(target.id);
    celld::js::ws_unregister(id);
    celld::js::ws_pull_unregister(id);
    Ok(())
}

pub(crate) async fn outbound_websocket_task(
    app: AppHandle,
    request: celld::js::OutboundWsReq,
) -> anyhow::Result<()> {
    use hyper::header::{HeaderMap, HeaderName, HeaderValue, SEC_WEBSOCKET_PROTOCOL};

    let celld::js::OutboundWsReq {
        scope,
        id,
        url,
        protocols,
        pull,
        headers,
        want_response,
        target,
        reply,
    } = request;
    if let Some(target) = target {
        return local_websocket_pipe(app, id, target, pull, reply).await;
    }
    let sink = match pull {
        Some(pull) => OutboundWebSocketSink::Isolate(pull),
        None => OutboundWebSocketSink::Cell {
            app: Box::new(app),
            scope: scope.clone(),
        },
    };
    let mut handshake = HeaderMap::new();
    for (name, value) in &headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "upgrade"
                | "connection"
                | "sec-websocket-key"
                | "sec-websocket-version"
                | "sec-websocket-protocol"
                | "host"
        ) {
            continue;
        }
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) else {
            continue;
        };
        handshake.insert(name, value);
    }
    if !protocols.is_empty() {
        handshake.insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&protocols.join(", "))
                .context("invalid WebSocket subprotocol")?,
        );
    }
    let timeout = std::time::Duration::from_secs(10);
    let connected = tokio::time::timeout(timeout, celld::ws_client::connect(&url, handshake)).await;
    let connection = match connected {
        Ok(Ok(connection)) => connection,
        Ok(Err(celld::ws_client::Error::Declined(declined))) if want_response => {
            let declined = celld::js::DeclinedUpgrade {
                status: declined.status.as_u16(),
                headers: declined
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            value.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect(),
                body: declined.body,
            };
            let _ = reply.send(Ok(celld::js::OutboundWsOpen {
                protocol: None,
                declined: Some(declined),
            }));
            return Ok(());
        }
        Ok(Err(error)) => {
            let _ = reply.send(Err(anyhow::anyhow!("{error}")));
            return Ok(());
        }
        Err(_) => {
            let _ = reply.send(Err(anyhow::anyhow!(
                "outbound WebSocket handshake timed out after {}ms",
                timeout.as_millis()
            )));
            return Ok(());
        }
    };
    let socket = connection.socket;
    let protocol = connection
        .headers
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if protocol
        .as_ref()
        .is_some_and(|selected| !protocols.iter().any(|offered| offered == selected))
    {
        let _ = reply.send(Err(anyhow::anyhow!(
            "server selected an unrequested WebSocket subprotocol"
        )));
        return Ok(());
    }

    let (outbound, mut outputs) = mpsc::unbounded_channel();
    if matches!(sink, OutboundWebSocketSink::Cell { .. }) {
        celld::js::ws_register_outbound(id, sink.scope());
    }
    celld::js::ws_register(id, outbound);
    if let Err(error) = sink
        .open(id, protocol.as_deref().unwrap_or_default().to_string())
        .await
    {
        celld::js::ws_unregister(id);
        let _ = reply.send(Err(error));
        return Ok(());
    }
    if reply
        .send(Ok(celld::js::OutboundWsOpen {
            protocol,
            declined: None,
        }))
        .is_err()
    {
        let _ = sink
            .closed(id, 1006, "opening event was cancelled".into(), false)
            .await;
        celld::js::ws_unregister(id);
        return Ok(());
    }

    // Ping and Close are answered by the reader's auto-pong and auto-close, as
    // the previous client library also did unprompted. The pump writes those
    // replies on the same socket, so nothing about that changes here.
    let (close, _writer) = {
        let sink = &sink;
        pump_cell_socket(socket, &mut outputs, true, move |data| {
            sink.message(id, data)
        })
        .await
    };
    let _ = sink
        .closed(id, close.state.0, close.state.1, close.state.2)
        .await;
    celld::js::ws_unregister(id);
    Ok(())
}

fn websocket_close_details(payload: &[u8]) -> (u16, String, bool) {
    match payload {
        [] => (1005, String::new(), true),
        [_] => (1002, String::new(), false),
        [first, second, reason @ ..] => {
            let Ok(reason) = std::str::from_utf8(reason) else {
                return (1007, String::new(), false);
            };
            let code = u16::from_be_bytes([*first, *second]);
            if !celld_logic::schedule::websocket_close_code_is_allowed(code) {
                return (1002, String::new(), false);
            }
            (code, reason.to_string(), true)
        }
    }
}

/// The write half of a cell socket, shared by the reader and the writer.
///
/// The reader needs it because auto-pong and auto-close hand their reply back to
/// the caller once a socket is split. The writer needs it for the cell's own
/// frames, and the close path needs it after both directions stop.
type SocketWriter<S> =
    std::sync::Arc<tokio::sync::Mutex<fastwebsockets::WebSocketWrite<tokio::io::WriteHalf<S>>>>;

/// Close details: the code, the reason, and whether the close was clean.
type CloseState = (u16, String, bool);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseInitiator {
    Peer,
    Application,
    Transport,
}

#[derive(Debug, Eq, PartialEq)]
struct PumpClose {
    state: CloseState,
    initiator: CloseInitiator,
}

async fn write_ws_out<S>(ws: &SocketWriter<S>, out: celld::js::WsOut) -> bool
where
    S: tokio::io::AsyncWrite + Unpin,
{
    use fastwebsockets::Frame;
    let keep_open = matches!(out, celld::js::WsOut::Text(_) | celld::js::WsOut::Binary(_));
    let frame = match out {
        celld::js::WsOut::Text(text) => Frame::text(text.into_bytes().into()),
        celld::js::WsOut::Binary(data) => Frame::binary(data.into()),
        celld::js::WsOut::Close(code, reason) => Frame::close(code, reason.as_bytes()),
    };
    ws.lock().await.write_frame(frame).await.is_ok() && keep_open
}

async fn echo_websocket_close<S>(
    writer: &SocketWriter<S>,
    close: &CloseState,
    application_sent_close: bool,
) where
    S: tokio::io::AsyncWrite + Unpin,
{
    if let Some(code) =
        celld_logic::schedule::websocket_echo_close(close.0, close.2, application_sent_close)
    {
        let _ = write_ws_out(writer, celld::js::WsOut::Close(code, close.1.clone())).await;
    }
}

/// Carry frames between a socket and the cell behind it until one side stops.
///
/// The read is never cancelled. `fastwebsockets::read_frame` consumes header
/// bytes from its buffer and then awaits for the payload, holding what it parsed
/// in local variables, so dropping that future keeps the buffer advanced and
/// loses the header. The next read then treats a payload byte as a frame header
/// and the stream never realigns. Earlier versions of both callers read the
/// socket in the same select as the cell's outbound queue, which drops the losing
/// future on every iteration. Cancelling a read mid-payload fails against that
/// shape.
///
/// One async block therefore owns each direction. The halves are split so that
/// the writer can write while the reader reads, and the writer stops on a signal
/// instead of being dropped, because dropping it can leave a partial frame on
/// the socket.
///
/// The caller sets `auto_close` and `auto_pong` on the socket before the pump
/// runs, and the pump keeps that choice: an automatic reply is written on the
/// same socket, exactly as the unsplit collector wrote it.
///
/// `outbound_close_is_clean` reports a close that the application sends as the
/// close state of the socket. The local path leaves that false, because it
/// answers an unclean end with its own protocol echo after the pump returns.
/// The initiator remains separate because only a peer close needs that echo.
///
/// The returned writer is still live, so the caller can write the close frames
/// that follow.
async fn pump_cell_socket<S, F, Fut>(
    socket: fastwebsockets::WebSocket<S>,
    outputs: &mut mpsc::UnboundedReceiver<celld::js::WsOut>,
    outbound_close_is_clean: bool,
    mut inbound: F,
) -> (PumpClose, SocketWriter<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: FnMut(celld::js::WsIn) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    use fastwebsockets::{FragmentCollectorRead, Frame as WsFrame, OpCode, Payload};

    let (reader, writer) = socket.split(tokio::io::split);
    let mut reader = FragmentCollectorRead::new(reader);
    let writer: SocketWriter<S> = std::sync::Arc::new(tokio::sync::Mutex::new(writer));

    let mut close = PumpClose {
        state: (1006, String::new(), false),
        initiator: CloseInitiator::Transport,
    };
    let (stop_writer, mut stopped) = tokio::sync::oneshot::channel::<()>();
    {
        let obligated_writer = writer.clone();
        let mut obligated = move |frame: WsFrame<'_>| {
            let writer = obligated_writer.clone();
            // The reply borrows the reader's buffer, and the write outlives the
            // callback, so the payload is copied out. The reader builds a pong
            // or a close echo unmasked, and `write_frame` masks it if the role
            // needs it, exactly as the unsplit collector did.
            let reply = WsFrame::new(
                frame.fin,
                frame.opcode,
                None,
                Payload::Owned(frame.payload.to_vec()),
            );
            async move { writer.lock().await.write_frame(reply).await }
        };

        let read = async {
            loop {
                let Ok(frame) = reader.read_frame(&mut obligated).await else {
                    return None;
                };
                let delivered = match frame.opcode {
                    OpCode::Text => {
                        inbound(celld::js::WsIn::Text(
                            String::from_utf8_lossy(&frame.payload).into_owned(),
                        ))
                        .await
                    }
                    OpCode::Binary => {
                        inbound(celld::js::WsIn::Binary(frame.payload.to_vec())).await
                    }
                    OpCode::Close => {
                        return Some(PumpClose {
                            state: websocket_close_details(&frame.payload),
                            initiator: CloseInitiator::Peer,
                        });
                    }
                    _ => Ok(()),
                };
                if delivered.is_err() {
                    return None;
                }
            }
        };

        // `recv` and the stop signal are both cancel-safe, and the write runs in
        // the branch body, so no write is ever cancelled either.
        let write = async {
            loop {
                celld::asyncrt::select_biased! {
                    "a stop signal that ties an outbound frame prevents one more socket write";
                    _ = &mut stopped => return None,
                    output = outputs.recv() => {
                        let output = output?;
                        let closed = match &output {
                            celld::js::WsOut::Close(code, reason) => Some(PumpClose {
                                state: if outbound_close_is_clean {
                                    (*code, reason.clone(), true)
                                } else {
                                    (1006, String::new(), false)
                                },
                                initiator: CloseInitiator::Application,
                            }),
                            _ => None,
                        };
                        if !write_ws_out(&writer, output).await { return closed; }
                    }
                }
            }
        };

        let mut read = std::pin::pin!(read);
        let mut write = std::pin::pin!(write);
        celld::asyncrt::select! {
            result = &mut read => {
                if let Some(details) = result { close = details; }
                // Stop the writer between frames rather than dropping it, so a
                // frame it started reaches the socket whole.
                let _ = stop_writer.send(());
                let _ = write.await;
            }
            result = &mut write => {
                if let Some(details) = result { close = details; }
                // The read is dropped here. That is safe only because the pump
                // is over and the socket is never read again.
            }
        }
    }
    (close, writer)
}

/// Carry a client socket into the top-level Worker request that returned it.
///
/// A Durable Object has a stable scope, so its frames can return through a
/// later cell dispatch. A stateless Worker has no such address. Its pending
/// `__ws_next` operation therefore keeps this request and isolate resident,
/// and this task carries each client frame through that operation's queue.
async fn worker_websocket_task<S>(
    worker: celld::js::WorkerWebSocket,
    mut socket: fastwebsockets::WebSocket<S>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket.set_auto_close(false);
    let id = worker.id();
    let inbound = worker.inbound();
    let (outbound, mut outputs) = mpsc::unbounded_channel();
    celld::js::ws_register(id, outbound);
    let (close, writer) = pump_cell_socket(socket, &mut outputs, true, move |data| {
        let inbound = inbound.clone();
        async move {
            inbound
                .send(data.into())
                .await
                .map_err(|_| anyhow::anyhow!("Worker stopped reading WebSocket"))
        }
    })
    .await;
    if close.initiator == CloseInitiator::Peer {
        echo_websocket_close(&writer, &close.state, false).await;
    }
    let _ = worker
        .inbound()
        .send_close(close.state.0, close.state.1, close.state.2);
}

pub(super) async fn websocket_task<S>(
    app: AppHandle,
    target: celld::js::WsTarget,
    mut socket: fastwebsockets::WebSocket<S>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket.set_auto_close(false);
    let (outbound, mut outputs) = mpsc::unbounded_channel();
    celld::js::ws_register(target.id, outbound);
    // A close the cell sends does not become the close state here: the echo
    // below decides what the peer observes, so the pump must not claim the
    // close was clean.
    let (close, writer) = {
        let app = &app;
        let scope = target.scope.as_str();
        let id = target.id;
        pump_cell_socket(socket, &mut outputs, false, move |data| {
            dispatch_ws_message(app, scope, id, data)
        })
        .await
    };
    if let Err(error) = dispatch_ws_closed(
        &app,
        &target.scope,
        target.id,
        close.state.0,
        close.state.1.clone(),
        close.state.2,
    )
    .await
    {
        tracing::warn!(
            %error,
            scope = %target.scope,
            websocket = target.id,
            "WebSocket close dispatch failed"
        );
    }

    // The close handler is allowed to choose the response code and reason.
    // Its output is queued while dispatch_ws_closed drives V8, so flush it
    // before unregistering the socket or considering a protocol-level echo.
    //
    // Its output can also still be behind the output gate: the handler may
    // read, and what it reads can belong to a write another request has not
    // proved durable yet. The drain below does not block, so a frame still in
    // flight leaves `handler_sent_close` false and the peer is answered with
    // the echo of its own close -- a clean close carrying the wrong reason,
    // which is indistinguishable from the handler choosing it.
    //
    // Bounded, and skipped outright on a draining node. The drain loop polls
    // the gate calls it already dispatched but never receives a new one: it
    // has no `gate_rx` arm. A ticket taken after the main loop broke is sent
    // to a live but unread channel and parks for good, so waiting on it here
    // would hold the socket task open until the drain hits its deadline. The
    // bound covers the same park reached the other way: `is_draining` is read
    // once, and shutdown can begin between that read and the wait. Giving up
    // costs the reason the handler chose, never a frame the gate has not
    // cleared -- the drain below still finds nothing, the echo still answers
    // the peer, and a frame released later lands on a socket that is gone.
    if !app.is_draining() {
        let waited = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            celld::js::ws_await_flushes(target.id),
        )
        .await;
        if waited.is_err() {
            tracing::warn!(
                scope = %target.scope,
                websocket = target.id,
                "gave up waiting for the close handler's frames"
            );
        }
    }
    let mut handler_sent_close = false;
    while let Ok(output) = outputs.try_recv() {
        handler_sent_close |= matches!(output, celld::js::WsOut::Close(_, _));
        if !write_ws_out(&writer, output).await {
            break;
        }
    }
    echo_websocket_close(&writer, &close.state, handler_sent_close).await;
    app.websocket_closed(target.scope.clone(), target.id);
    celld::js::ws_unregister(target.id);
}

/// Headers that the WebSocket server must compute or remove for a 101.
///
/// `fastwebsockets` creates its response before the Worker runs, so blindly
/// appending the Worker's response afterwards can replace the accept hash or
/// restore framing fields that an upgrade cannot carry. This is the exclusion
/// set in kj's `acceptWebSocket`: it forces the three handshake fields, drops
/// the obsolete or request-only fields, and drops `Sec-WebSocket-Extensions`.
fn forwards_worker_websocket_header(name: &str) -> bool {
    !matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "upgrade"
            | "sec-websocket-accept"
            | "keep-alive"
            | "te"
            | "trailer"
            | "content-length"
            | "transfer-encoding"
            | "sec-websocket-key"
            | "sec-websocket-version"
            | "sec-websocket-extensions"
    )
}

pub(crate) async fn handle_websocket(mut request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let started = Instant::now();
    let cancellation = celld::runtime::RequestCancellationLifetime::stateless();
    let request_id = cancellation.request_id();
    let (mut upgrade_response, upgrade) = match fastwebsockets::upgrade::upgrade(&mut request) {
        Ok(upgrade) => upgrade,
        Err(error) => return response(StatusCode::BAD_REQUEST, format!("ws upgrade: {error}")),
    };
    let runtime = app.runtime.as_ref().expect("WebSocket runtime checked");
    let body_started = Instant::now();
    let (url, method, body, headers) = match request_payload(
        request,
        app.trust_forwarded_headers,
        app.max_request_body_bytes,
    )
    .await
    {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let body_read_us = body_started.elapsed().as_micros() as u64;
    let worker_started = Instant::now();
    let mut worker_response = match runtime
        .fetch_worker_pool(url, method, body.into(), headers, cancellation)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_started.elapsed().as_micros() as u64,
                WebSocketConnectionOutcome {
                    outcome: "worker_error",
                    route: "",
                    scope: "",
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                },
            );
            return response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Worker failed: {error:#}"),
            );
        }
    };
    let worker_dispatch_us = worker_started.elapsed().as_micros() as u64;
    let websocket = match worker_response.websocket.take() {
        Some(websocket) => match PendingWebSocket::new(websocket) {
            Ok(websocket) => websocket,
            Err(error) => {
                return response(
                    StatusCode::BAD_GATEWAY,
                    format!("tunneled WebSocket upgrade: {error}"),
                );
            }
        },
        None => {
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_dispatch_us,
                WebSocketConnectionOutcome {
                    outcome: "rejected",
                    route: "",
                    scope: "",
                    status: worker_response.status,
                },
            );
            return runtime_response(worker_response, false);
        }
    };
    let target = websocket.target();
    if worker_response.status != 101 {
        emit_websocket_connection_timing(
            runtime,
            request_id,
            started,
            body_read_us,
            worker_dispatch_us,
            WebSocketConnectionOutcome {
                outcome: "rejected",
                route: "",
                scope: target.map_or("", |target| target.scope.as_str()),
                status: worker_response.status,
            },
        );
        if let Some(target) = target {
            reject_accepted_websocket(&app, target, "unsupported WebSocket route").await;
        }
        return response(StatusCode::BAD_GATEWAY, "unsupported WebSocket route");
    }
    // Workerd shallow-copies the Worker's complete header list into
    // `acceptWebSocket`, which preserves duplicate application fields such as
    // `Set-Cookie`. Validate the complete batch before changing the response,
    // because returning a partially copied handshake is never useful.
    let mut worker_headers = Vec::new();
    for (name, value) in &worker_response.headers {
        if !forwards_worker_websocket_header(name) {
            continue;
        }
        let parsed = hyper::header::HeaderName::from_bytes(name.as_bytes())
            .ok()
            .zip(hyper::header::HeaderValue::from_str(value).ok());
        let Some(header) = parsed else {
            let reason = format!("invalid WebSocket response header: {name}");
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_dispatch_us,
                WebSocketConnectionOutcome {
                    outcome: "rejected",
                    route: "",
                    scope: target.map_or("", |target| target.scope.as_str()),
                    status: StatusCode::BAD_GATEWAY.as_u16(),
                },
            );
            if let Some(target) = target {
                reject_accepted_websocket(&app, target, &reason).await;
            }
            return response(StatusCode::BAD_GATEWAY, "invalid WebSocket response header");
        };
        worker_headers.push(header);
    }
    for (name, value) in worker_headers {
        upgrade_response.headers_mut().append(name, value);
    }
    emit_websocket_connection_timing(
        runtime,
        request_id,
        started,
        body_read_us,
        worker_dispatch_us,
        WebSocketConnectionOutcome {
            outcome: "accepted",
            route: match target {
                Some(target) if target.tunnel.is_some() => "remote",
                Some(_) => "local",
                None => "worker",
            },
            scope: target.map_or("", |target| target.scope.as_str()),
            status: 101,
        },
    );
    let task_app = app.clone();
    let task = Box::pin(run_websocket_upgrade(task_app, upgrade, websocket));
    if app.websockets.send(task).is_err() {
        return response(
            StatusCode::SERVICE_UNAVAILABLE,
            "WebSocket executor stopped",
        );
    }
    upgrade_response.map(|body| body.map_err(|never| match never {}).boxed_unsync())
}

/// Finish the client upgrade through the same owned response value that
/// survives validation. Keeping this work in one future makes a failed client
/// upgrade and a cancelled executor task drop the same owned response.
async fn run_websocket_upgrade(
    task_app: AppHandle,
    upgrade: fastwebsockets::upgrade::UpgradeFut,
    websocket: PendingWebSocket,
) {
    match (upgrade.await, websocket) {
        (Ok(socket), PendingWebSocket::Worker(worker)) => {
            worker_websocket_task(worker, socket).await;
        }
        (Ok(socket), PendingWebSocket::Tunnel { upgrade, .. }) => {
            if let Err(error) = super::peer_tunnel::splice(socket.into_inner(), upgrade).await {
                eprintln!("celld tunneled WebSocket splice ended: {error:#}");
            }
        }
        (Ok(socket), PendingWebSocket::Cell(target)) => {
            websocket_task(task_app, target, socket).await;
        }
        (Err(error), PendingWebSocket::Worker(_)) => {
            eprintln!("celld Worker WebSocket upgrade failed: {error}");
        }
        (Err(error), PendingWebSocket::Cell(target)) => {
            eprintln!("celld WebSocket upgrade failed: {error}");
            finish_websocket(&task_app, &target, 1006, String::new(), false).await;
        }
        (Err(error), PendingWebSocket::Tunnel { .. }) => {
            eprintln!("celld tunneled WebSocket upgrade failed: {error}");
        }
    }
}

#[cfg(all(test, celld_internal_tests))]
pub(super) async fn run_websocket_upgrade_for_test(
    task_app: AppHandle,
    upgrade: fastwebsockets::upgrade::UpgradeFut,
    websocket: HttpResponseWebSocket,
) {
    let websocket = PendingWebSocket::new(websocket).unwrap();
    run_websocket_upgrade(task_app, upgrade, websocket).await;
}

struct WebSocketConnectionOutcome<'a> {
    outcome: &'a str,
    route: &'a str,
    scope: &'a str,
    status: u16,
}

fn emit_websocket_connection_timing(
    runtime: &RuntimeManager,
    request_id: celld::js::RequestId,
    started: Instant,
    body_read_us: u64,
    worker_dispatch_us: u64,
    event_outcome: WebSocketConnectionOutcome<'_>,
) {
    let WebSocketConnectionOutcome {
        outcome,
        route,
        scope,
        status,
    } = event_outcome;
    tracing::debug!(
        target: "timing",
        event = "websocket_connection_timing",
        outcome,
        route,
        scope,
        request_id = %celld::js::request_id_string(request_id),
        node = runtime.node(),
        region = runtime.region(),
        runtime_version = env!("CARGO_PKG_VERSION"),
        status,
        total_us = started.elapsed().as_micros() as u64,
        body_read_us,
        worker_dispatch_us,
        "WebSocket connection resolved"
    );
}

// Both cell socket loops had the same hazard as the tunnel: they read the socket
// in the same select! as the cell's outbound queue, so an outbound frame that
// won the race dropped a read in progress. A lost frame keeps the workload
// ledger exact while delivery is not, so frame-level observation is required.
#[cfg(all(test, celld_internal_tests))]
mod socket_cancel_private {
    include!(env!("CELLD_CONFORMANCE_SOCKET_CANCEL_TESTS"));
}
