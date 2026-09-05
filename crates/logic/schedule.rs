// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Cell-isolate dispatch decisions, reified sans-IO. A cell isolate runs one
//! event at a time. A top-level Worker fetch takes the resident-isolate fast
//! path only when the isolate is idle; if the isolate is already pumping an
//! actor event, the fetch must reschedule to the stateless Worker pool — never
//! run nested — carrying its request identity so the reply still lands
//! (`js.rs`). The executor and the production run loop hold the isolate
//! channels and the pool; this is the pure routing they consult, so a
//! deterministic executor can drive it directly.
//!
//! Small protocol sequencing choices also live here when the executor owns
//! the bytes but not the decision. That keeps the shell mechanical and lets
//! callers exercise the same branch production takes.

/// Return whether a close code can appear in a WebSocket close frame.
///
/// RFC 6455 uses 1005, 1006, and 1015 only for local reporting, so an endpoint
/// cannot put them on the wire. Code 1004 is reserved. The IANA registry
/// assigns the remaining standard codes through 1014, and codes from 3000
/// through 4999 are available to applications and libraries.
pub fn websocket_close_code_is_allowed(code: u16) -> bool {
    matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
}

/// Select the protocol close code the shell must echo after the application
/// close handler has run and all of its output has been written.
///
/// An application-selected close wins. Otherwise RFC 6455 requires a clean
/// peer close to receive a close response; 1005 is the local sentinel for a
/// frame with no status and cannot itself appear on the wire, so it becomes
/// the normal close code 1000. A parsed protocol failure receives its permitted
/// error code, while an abnormal transport end receives no frame.
pub fn websocket_echo_close(
    peer_code: u16,
    peer_was_clean: bool,
    handler_sent_close: bool,
) -> Option<u16> {
    if handler_sent_close {
        None
    } else if !peer_was_clean {
        matches!(peer_code, 1002 | 1007).then_some(peer_code)
    } else if peer_code == 1005 {
        Some(1000)
    } else {
        Some(peer_code)
    }
}
