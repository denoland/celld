//! HTMLRewriter host ops over the `lol_html` crate — the same parser
//! Workerd embeds.
//!
//! `lol_html` drives content handlers as synchronous callbacks inside
//! `write()`, but a Worker's handlers are JavaScript and can be async.
//! Each rewriter therefore runs on its own thread: when a handler
//! matches, the closure sends an event to the isolate and parks on a
//! command channel. JS services the event — awaiting its own promises
//! freely — by sending interactive commands (`getAttribute`,
//! `setAttribute`, `before`, ...) that the parked closure applies to
//! the live token, so every validation error is `lol_html`'s own. A
//! final `done` command releases the parser. One command is in flight
//! at a time and the thread is provably parked while JS holds the
//! token, so the sync round-trip cannot deadlock; the receive timeout
//! is a backstop against a lost thread, not a protocol state.

use super::*;
use lol_html::html_content::ContentType;
use lol_html::AsciiCompatibleEncoding;
use lol_html::HtmlRewriter;
use lol_html::Selector;
use lol_html::Settings;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

#[derive(serde::Deserialize)]
struct SelectorConfig {
    selector: String,
    element: bool,
    comments: bool,
    text: bool,
}

#[derive(serde::Deserialize, Default)]
struct DocumentConfig {
    #[serde(default)]
    doctype: bool,
    #[serde(default)]
    comments: bool,
    #[serde(default)]
    text: bool,
    #[serde(default)]
    end: bool,
}

#[derive(serde::Deserialize)]
struct RewriterConfig {
    #[serde(default)]
    encoding: Option<String>,
    selectors: Vec<SelectorConfig>,
    // One entry per onDocument() call, in registration order.
    #[serde(default)]
    document: Vec<DocumentConfig>,
}

enum Input {
    Chunk(Vec<u8>),
    End,
}

struct Rewriter {
    input_tx: mpsc::Sender<Input>,
    cmd_tx: mpsc::Sender<String>,
    cmd_resp_rx: Arc<Mutex<mpsc::Receiver<String>>>,
    event_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<String>>>,
    output: Arc<Mutex<VecDeque<u8>>>,
}

fn registry() -> &'static Mutex<HashMap<u64, Rewriter>> {
    static REWRITERS: OnceLock<Mutex<HashMap<u64, Rewriter>>> = OnceLock::new();
    REWRITERS.get_or_init(Default::default)
}

fn next_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// The channel ends every handler closure shares. Events go out, and
/// the closure then services commands until JS sends `done`.
struct Dialogue {
    event_tx: tokio::sync::mpsc::UnboundedSender<String>,
    cmd_rx: Mutex<mpsc::Receiver<String>>,
    // Mutex only for the `Sync` bound `Arc` needs; one thread sends.
    cmd_resp_tx: Mutex<mpsc::Sender<String>>,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl Dialogue {
    /// Announce a token to JS and apply its commands until `done`.
    /// `apply` maps one command to a JSON reply; `Err` aborts the
    /// rewrite with the message JS chose (a thrown handler).
    fn serve(
        &self,
        event: serde_json::Value,
        mut apply: impl FnMut(&serde_json::Value) -> Result<serde_json::Value, String>,
    ) -> Result<serde_json::Value, BoxError> {
        if self.event_tx.send(event.to_string()).is_err() {
            return Err("rewriter cancelled".into());
        }
        loop {
            let raw = self
                .cmd_rx
                .lock()
                .unwrap()
                .recv()
                .map_err(|_| "rewriter cancelled")?;
            let cmd: serde_json::Value =
                serde_json::from_str(&raw).map_err(|error| error.to_string())?;
            if cmd["op"] == "done" {
                return Ok(cmd);
            }
            if cmd["op"] == "abort" {
                // JS already holds the real error; this message only
                // has to end the rewrite, not describe it.
                return Err(cmd["message"].as_str().unwrap_or("handler failed").into());
            }
            let reply = match apply(&cmd) {
                Ok(value) => serde_json::json!({ "ok": value }),
                Err(message) => serde_json::json!({ "error": message }),
            };
            let sent = self.cmd_resp_tx.lock().unwrap().send(reply.to_string());
            if sent.is_err() {
                return Err("rewriter cancelled".into());
            }
        }
    }
}

fn content_type(cmd: &serde_json::Value) -> ContentType {
    if cmd["html"].as_bool().unwrap_or(false) {
        ContentType::Html
    } else {
        ContentType::Text
    }
}

fn text_arg<'v>(cmd: &'v serde_json::Value, key: &str) -> &'v str {
    cmd[key].as_str().unwrap_or("")
}

/// "Parser error: " is Workerd's prefix for every error `lol_html`
/// reports; the vendored suite asserts the combined text.
fn parser_error(error: impl std::fmt::Display) -> String {
    format!("Parser error: {error}")
}

fn element_command(
    element: &mut lol_html::html_content::Element,
    cmd: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let op = cmd["op"].as_str().unwrap_or("");
    Ok(match op {
        "tagName" => serde_json::json!(element.tag_name()),
        "setTagName" => {
            element
                .set_tag_name(text_arg(cmd, "name"))
                .map_err(parser_error)?;
            serde_json::Value::Null
        }
        "namespaceURI" => serde_json::json!(element.namespace_uri()),
        "attributes" => serde_json::json!(element
            .attributes()
            .iter()
            .map(|a| (a.name(), a.value()))
            .collect::<Vec<_>>()),
        "getAttribute" => serde_json::json!(element.get_attribute(text_arg(cmd, "name"))),
        "hasAttribute" => serde_json::json!(element.has_attribute(text_arg(cmd, "name"))),
        "setAttribute" => {
            element
                .set_attribute(text_arg(cmd, "name"), text_arg(cmd, "value"))
                .map_err(parser_error)?;
            serde_json::Value::Null
        }
        "removeAttribute" => {
            element.remove_attribute(text_arg(cmd, "name"));
            serde_json::Value::Null
        }
        "before" => {
            element.before(text_arg(cmd, "content"), content_type(cmd));
            serde_json::Value::Null
        }
        "after" => {
            element.after(text_arg(cmd, "content"), content_type(cmd));
            serde_json::Value::Null
        }
        "prepend" => {
            element.prepend(text_arg(cmd, "content"), content_type(cmd));
            serde_json::Value::Null
        }
        "append" => {
            element.append(text_arg(cmd, "content"), content_type(cmd));
            serde_json::Value::Null
        }
        "setInnerContent" => {
            element.set_inner_content(text_arg(cmd, "content"), content_type(cmd));
            serde_json::Value::Null
        }
        "replace" => {
            element.replace(text_arg(cmd, "content"), content_type(cmd));
            serde_json::Value::Null
        }
        "remove" => {
            element.remove();
            serde_json::Value::Null
        }
        "removeAndKeepContent" => {
            element.remove_and_keep_content();
            serde_json::Value::Null
        }
        "removed" => serde_json::json!(element.removed()),
        other => return Err(format!("unknown element op {other}")),
    })
}

/// One rewriter's whole life, on its own thread. Every handler closure
/// routes through `dialogue`; the input loop feeds `write()` until the
/// JS side signals end-of-stream or drops the channel (cancel).
fn run_rewriter(
    config: RewriterConfig,
    encoding: AsciiCompatibleEncoding,
    input_rx: mpsc::Receiver<Input>,
    dialogue: Arc<Dialogue>,
    event_tx: tokio::sync::mpsc::UnboundedSender<String>,
    output: Arc<Mutex<VecDeque<u8>>>,
) {
    let mut settings = Settings::new().with_encoding(encoding);
    for (index, entry) in config.selectors.iter().enumerate() {
        let selector: Selector = match entry.selector.parse() {
            Ok(selector) => selector,
            // `op_hr_create` already parsed this once; a failure here
            // is unreachable, but a lost thread must still report.
            Err(error) => {
                let _ = event_tx.send(
                    serde_json::json!({ "kind": "error", "message": parser_error(error) })
                        .to_string(),
                );
                return;
            }
        };
        if !entry.element && !entry.comments && !entry.text {
            continue; // `.on(sel, {})`: the selector was validated, and
                      // an empty handler set has nothing to register.
        }
        let mut handlers = lol_html::ElementContentHandlers::default();
        if entry.element {
            let dialogue_ = dialogue.clone();
            handlers = handlers.element(move |element: &mut lol_html::html_content::Element| {
                let done = dialogue_.serve(
                    serde_json::json!({ "kind": "element", "handler": index }),
                    |cmd| element_command(element, cmd),
                )?;
                if done["wantEndTag"].as_bool().unwrap_or(false) {
                    let dialogue__ = dialogue_.clone();
                    // Nested matches close in LIFO order, so the event
                    // carries the JS-issued token instead of trusting
                    // arrival order.
                    let end_token = done["endTagToken"].clone();
                    element.on_end_tag(Box::new(
                        move |end_tag: &mut lol_html::html_content::EndTag| {
                            dialogue__.serve(
                                serde_json::json!({
                                    "kind": "endTag",
                                    "token": end_token,
                                    "name": end_tag.name(),
                                }),
                                |cmd| {
                                    Ok(match cmd["op"].as_str().unwrap_or("") {
                                        "before" => {
                                            end_tag.before(
                                                text_arg(cmd, "content"),
                                                content_type(cmd),
                                            );
                                            serde_json::Value::Null
                                        }
                                        "after" => {
                                            end_tag
                                                .after(text_arg(cmd, "content"), content_type(cmd));
                                            serde_json::Value::Null
                                        }
                                        "remove" => {
                                            end_tag.remove();
                                            serde_json::Value::Null
                                        }
                                        other => return Err(format!("unknown end-tag op {other}")),
                                    })
                                },
                            )?;
                            Ok(())
                        },
                    ))?;
                }
                Ok(())
            });
        }
        if entry.comments {
            let dialogue_ = dialogue.clone();
            handlers = handlers.comments(move |comment: &mut lol_html::html_content::Comment| {
                serve_comment(&dialogue_, comment, index, false)
            });
        }
        if entry.text {
            let dialogue_ = dialogue.clone();
            handlers = handlers.text(move |chunk: &mut lol_html::html_content::TextChunk| {
                serve_text(&dialogue_, chunk, index, false)
            });
        }
        settings =
            settings.append_element_content_handler((std::borrow::Cow::Owned(selector), handlers));
    }

    for (index, doc) in config.document.iter().enumerate() {
        if !doc.doctype && !doc.comments && !doc.text && !doc.end {
            continue;
        }
        let mut handlers = lol_html::DocumentContentHandlers::default();
        if doc.doctype {
            let dialogue_ = dialogue.clone();
            handlers = handlers.doctype(move |doctype: &mut lol_html::html_content::Doctype| {
                dialogue_.serve(
                    serde_json::json!({
                        "kind": "doctype",
                        "docHandler": index,
                        "name": doctype.name(),
                        "publicId": doctype.public_id(),
                        "systemId": doctype.system_id(),
                    }),
                    |cmd| Err(format!("unknown doctype op {}", cmd["op"])),
                )?;
                Ok(())
            });
        }
        if doc.comments {
            let dialogue_ = dialogue.clone();
            handlers = handlers.comments(move |comment: &mut lol_html::html_content::Comment| {
                serve_comment(&dialogue_, comment, index, true)
            });
        }
        if doc.text {
            let dialogue_ = dialogue.clone();
            handlers = handlers.text(move |chunk: &mut lol_html::html_content::TextChunk| {
                serve_text(&dialogue_, chunk, index, true)
            });
        }
        if doc.end {
            let dialogue_ = dialogue.clone();
            handlers = handlers.end(move |end: &mut lol_html::html_content::DocumentEnd| {
                dialogue_.serve(
                    serde_json::json!({ "kind": "documentEnd", "docHandler": index }),
                    |cmd| {
                        Ok(match cmd["op"].as_str().unwrap_or("") {
                            "append" => {
                                end.append(text_arg(cmd, "content"), content_type(cmd));
                                serde_json::Value::Null
                            }
                            other => return Err(format!("unknown document-end op {other}")),
                        })
                    },
                )?;
                Ok(())
            });
        }
        settings = settings.append_document_content_handler(handlers);
    }

    let sink_output = output;
    let sink_events = event_tx.clone();
    let mut rewriter = HtmlRewriter::new(settings, move |chunk: &[u8]| {
        sink_output.lock().unwrap().extend(chunk.iter().copied());
        let _ = sink_events.send(serde_json::json!({ "kind": "output" }).to_string());
    });

    loop {
        match input_rx.recv() {
            Ok(Input::Chunk(bytes)) => {
                if let Err(error) = rewriter.write(&bytes) {
                    let _ = event_tx.send(rewrite_error(error));
                    return;
                }
            }
            Ok(Input::End) => break,
            // The JS side dropped the handle: cancelled.
            Err(_) => return,
        }
    }
    match rewriter.end() {
        Ok(()) => {
            let _ = event_tx.send(serde_json::json!({ "kind": "end" }).to_string());
        }
        Err(error) => {
            let _ = event_tx.send(rewrite_error(error));
        }
    }
}

/// A handler abort carries JS's own message; every other rewriting
/// error is the parser's and gets Workerd's prefix.
fn rewrite_error(error: lol_html::errors::RewritingError) -> String {
    let message = match &error {
        lol_html::errors::RewritingError::ContentHandlerError(inner) => inner.to_string(),
        other => parser_error(other),
    };
    serde_json::json!({ "kind": "error", "message": message }).to_string()
}

fn serve_comment(
    dialogue: &Dialogue,
    comment: &mut lol_html::html_content::Comment,
    handler: usize,
    document: bool,
) -> Result<(), BoxError> {
    dialogue.serve(
        serde_json::json!({
            "kind": "comment",
            "handler": handler,
            "docHandler": handler,
            "document": document,
            "text": comment.text(),
        }),
        |cmd| {
            Ok(match cmd["op"].as_str().unwrap_or("") {
                "setText" => {
                    comment
                        .set_text(text_arg(cmd, "text"))
                        .map_err(parser_error)?;
                    serde_json::Value::Null
                }
                "before" => {
                    comment.before(text_arg(cmd, "content"), content_type(cmd));
                    serde_json::Value::Null
                }
                "after" => {
                    comment.after(text_arg(cmd, "content"), content_type(cmd));
                    serde_json::Value::Null
                }
                "replace" => {
                    comment.replace(text_arg(cmd, "content"), content_type(cmd));
                    serde_json::Value::Null
                }
                "remove" => {
                    comment.remove();
                    serde_json::Value::Null
                }
                "removed" => serde_json::json!(comment.removed()),
                other => return Err(format!("unknown comment op {other}")),
            })
        },
    )?;
    Ok(())
}

fn serve_text(
    dialogue: &Dialogue,
    chunk: &mut lol_html::html_content::TextChunk,
    handler: usize,
    document: bool,
) -> Result<(), BoxError> {
    dialogue.serve(
        serde_json::json!({
            "kind": "text",
            "handler": handler,
            "docHandler": handler,
            "document": document,
            "text": chunk.as_str(),
            "lastInTextNode": chunk.last_in_text_node(),
        }),
        |cmd| {
            Ok(match cmd["op"].as_str().unwrap_or("") {
                "before" => {
                    chunk.before(text_arg(cmd, "content"), content_type(cmd));
                    serde_json::Value::Null
                }
                "after" => {
                    chunk.after(text_arg(cmd, "content"), content_type(cmd));
                    serde_json::Value::Null
                }
                "replace" => {
                    chunk.replace(text_arg(cmd, "content"), content_type(cmd));
                    serde_json::Value::Null
                }
                "remove" => {
                    chunk.remove();
                    serde_json::Value::Null
                }
                "removed" => serde_json::json!(chunk.removed()),
                other => return Err(format!("unknown text op {other}")),
            })
        },
    )?;
    Ok(())
}

pub(super) fn op_hr_create(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let raw = args.get(0).to_rust_string_lossy(scope);
    let config: RewriterConfig = match serde_json::from_str(&raw) {
        Ok(config) => config,
        Err(error) => return loader_throw(scope, &format!("HTMLRewriter config: {error}")),
    };
    // Validate here, synchronously: `transform()` must throw on an
    // unknown charset or a bad selector before any stream work starts.
    let label = config.encoding.as_deref().unwrap_or("utf-8");
    let encoding =
        encoding_rs::Encoding::for_label(label.as_bytes()).and_then(AsciiCompatibleEncoding::new);
    let Some(encoding) = encoding else {
        return loader_throw(
            scope,
            "Parser error: Unknown character encoding has been provided.",
        );
    };
    for entry in &config.selectors {
        if let Err(error) = entry.selector.parse::<Selector>() {
            return loader_throw(scope, &parser_error(error));
        }
    }

    let (input_tx, input_rx) = mpsc::channel();
    let (cmd_tx, cmd_rx) = mpsc::channel();
    let (cmd_resp_tx, cmd_resp_rx) = mpsc::channel();
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
    let output = Arc::new(Mutex::new(VecDeque::new()));
    let dialogue = Arc::new(Dialogue {
        event_tx: event_tx.clone(),
        cmd_rx: Mutex::new(cmd_rx),
        cmd_resp_tx: Mutex::new(cmd_resp_tx),
    });
    let output_ = output.clone();
    // A dedicated OS thread, not a task: the closure parks on a
    // synchronous channel while JS services a handler, which would
    // wedge an async worker.
    std::thread::spawn(move || {
        run_rewriter(config, encoding, input_rx, dialogue, event_tx, output_)
    });

    let id = next_id();
    // Account the rewriter to the request, the way a Worker socket is:
    // request retirement frees an abandoned transform's parser thread.
    current_context().rewriters.lock().unwrap().push(id);
    registry().lock().unwrap().insert(
        id,
        Rewriter {
            input_tx,
            cmd_tx,
            cmd_resp_rx: Arc::new(Mutex::new(cmd_resp_rx)),
            event_rx: Arc::new(tokio::sync::Mutex::new(event_rx)),
            output,
        },
    );
    rv.set(v8::Number::new(scope, id as f64).into());
}

pub(super) fn op_hr_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let Some(bytes) = view_bytes(args.get(1)) else {
        return;
    };
    if let Some(rewriter) = registry().lock().unwrap().get(&id) {
        let _ = rewriter.input_tx.send(Input::Chunk(bytes.to_vec()));
    }
}

pub(super) fn op_hr_end(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    if let Some(rewriter) = registry().lock().unwrap().get(&id) {
        let _ = rewriter.input_tx.send(Input::End);
    }
}

/// One interactive token command: send, then block for the parked
/// closure's reply. The timeout is a lost-thread backstop; see the
/// module comment for why the round-trip cannot deadlock.
pub(super) fn op_hr_cmd(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let cmd = args.get(1).to_rust_string_lossy(scope);
    // `done` and `abort` release the closure without a reply.
    let expects_reply = serde_json::from_str::<serde_json::Value>(&cmd)
        .map(|value| value["op"] != "done" && value["op"] != "abort")
        .unwrap_or(true);
    // Take what the wait needs, then drop the registry guard: the reply
    // wait must not block another isolate's rewriter ops.
    let receiver = {
        let registry = registry().lock().unwrap();
        let Some(rewriter) = registry.get(&id) else {
            return loader_throw(scope, "HTMLRewriter is gone");
        };
        if rewriter.cmd_tx.send(cmd).is_err() {
            return loader_throw(scope, "HTMLRewriter thread exited");
        }
        expects_reply.then(|| Arc::clone(&rewriter.cmd_resp_rx))
    };
    let Some(receiver) = receiver else {
        return;
    };
    let reply = receiver
        .lock()
        .unwrap()
        .recv_timeout(Duration::from_secs(10));
    match reply {
        Ok(reply) => {
            let value = v8::String::new(scope, &reply).unwrap();
            rv.set(value.into());
        }
        Err(_) => loader_throw(scope, "HTMLRewriter command timed out"),
    }
}

pub(super) fn op_hr_event(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let receiver = registry()
        .lock()
        .unwrap()
        .get(&id)
        .map(|rewriter| rewriter.event_rx.clone());
    let async_id = asyncrt::enqueue(async move {
        let Some(receiver) = receiver else {
            return Err("HTMLRewriter is gone".to_string());
        };
        let mut receiver = receiver.lock().await;
        match receiver.recv().await {
            Some(event) => Ok(event),
            None => Ok(serde_json::json!({ "kind": "closed" }).to_string()),
        }
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_hr_take(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let bytes = registry()
        .lock()
        .unwrap()
        .get(&id)
        .map(|rewriter| {
            let mut output = rewriter.output.lock().unwrap();
            output.drain(..).collect::<Vec<u8>>()
        })
        .unwrap_or_default();
    webcrypto_return_bytes(scope, rv, &bytes);
}

/// Free one rewriter: drop its channel ends so the parser thread
/// unblocks and exits. Idempotent — every teardown path calls it.
pub(super) fn free(id: u64) {
    registry().lock().unwrap().remove(&id);
}

pub(super) fn op_hr_free(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    // Dropping the senders unblocks the thread: a parked closure's
    // `recv` fails and the input loop's `recv` fails, so it exits.
    free(id);
}
