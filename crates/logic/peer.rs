// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Peer-authentication predicates, reified sans-IO. The signed-request checks in
//! `peer_auth::verify` — a well-formed identity, a timestamp within the clock
//! window, and replay-cache retention — are pure predicates over scalars, and
//! each is a SECURITY fence. A widened identity charset lets a `target` smuggle
//! a path segment past the WrongTarget check; a one-sided clock window replays
//! an old signature forever; retention below the clock window forgets a nonce
//! while a signature bearing it is still acceptable, reopening the replay hole
//! the cache exists to close. Signing/MAC stays in production; the gates move.

pub type Ms = u64;

/// Is a node name or peer identity (`source`/`target`) well-formed? Non-empty,
/// at most 128 bytes, and only ASCII alphanumerics plus `_ - .`. The special
/// path components `.` and `..` are not names. The charset is the fence — admit
/// `/` and an identity could carry a path segment.
pub fn valid_identity(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && !matches!(id, "." | "..")
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// Is a request timestamp within the accepted clock window of `now`? The window
/// is TWO-sided (`abs_diff`): a check that accepted arbitrarily old timestamps
/// would let a captured signature replay forever.
pub fn within_clock_window(now: Ms, timestamp: Ms, window: Ms) -> bool {
    now.abs_diff(timestamp) <= window
}

/// May a replay-cache nonce be evicted — has its retention window elapsed?
/// Retention MUST be at least the clock window, or a nonce is forgotten while a
/// signature bearing it is still within the clock window and would be accepted.
pub fn replay_entry_expired(now: Ms, seen_ms: Ms, retention: Ms) -> bool {
    now.saturating_sub(seen_ms) > retention
}
