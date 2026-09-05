//! error.rs — thiserror-based error model + `Result` alias.
//!
//! Ported from litestream@v0.5.11 litestream.go:32-93
//!
//! The sentinel errors (`ErrNoSnapshots`, `ErrChecksumMismatch`,
//! `ErrLTXCorrupted`, `ErrLTXMissing`) and the structured `LTXError` type with
//! its `IsAutoRecoverable` logic are direct ports of the equivalent Go values.

use std::io;
use thiserror::Error;

/// Crate-wide `Result` alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Top-level error enum for `celld-ltx`.
///
/// Variants map 1-to-1 to the sentinel errors and error categories in
/// `litestream.go`; additional variants cover I/O and other infrastructure.
#[derive(Debug, Error)]
pub enum Error {
    /// No snapshot is available for restore.
    ///
    /// Corresponds to `ErrNoSnapshots` in litestream.go:33.
    #[error("no snapshots available")]
    NoSnapshots,

    /// The replica's rolling checksum does not match what was expected.
    ///
    /// Corresponds to `ErrChecksumMismatch` in litestream.go:34.
    #[error("invalid replica, checksum mismatch")]
    ChecksumMismatch,

    /// An LTX file's contents are corrupt (bad magic, truncated, bad CRC, etc.).
    ///
    /// Corresponds to `ErrLTXCorrupted` in litestream.go:35.
    #[error("ltx file corrupted")]
    LTXCorrupted,

    /// An expected LTX file is absent from the replica.
    ///
    /// Corresponds to `ErrLTXMissing` in litestream.go:36.
    #[error("ltx file missing")]
    LTXMissing,

    /// The requested transaction does not exist on the replica (no backup chain
    /// reaches it).
    ///
    /// Corresponds to `ErrTxNotAvailable` in store.go:27.
    #[error("transaction not available")]
    TxNotAvailable,

    /// Structured LTX error with operation context and recovery hints.
    ///
    /// Corresponds to `LTXError` in litestream.go:40-93.
    #[error("{0}")]
    Ltx(#[from] Box<LTXError>),

    /// Underlying I/O error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    /// The object store refused the credential (HTTP 403).
    #[error("the object store refused the credential")]
    Forbidden,

    /// Any other error not yet given a dedicated variant.
    #[error("{0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

// ── LTXError ─────────────────────────────────────────────────────────────────

/// Structured error for LTX file operations.
///
/// Provides operation context, file path, TXID range, and an optional
/// human-readable recovery hint.  Ported from `LTXError` in
/// litestream@v0.5.11 litestream.go:40-93.
#[derive(Debug)]
pub struct LTXError {
    /// Operation that failed (e.g., `"open"`, `"read"`, `"validate"`).
    pub op: String,
    /// File path, if known.
    pub path: String,
    /// LTX compaction level (0 = L0).
    pub level: i32,
    /// Minimum transaction ID in the file.
    pub min_txid: u64,
    /// Maximum transaction ID in the file.
    pub max_txid: u64,
    /// Underlying cause.
    pub err: Box<dyn std::error::Error + Send + Sync>,
    /// Human-readable recovery hint (empty when not applicable).
    pub hint: String,
}

impl std::fmt::Display for LTXError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Matches the Go Error() method:
        //   if e.Path != "" { return e.Op + " ltx file " + e.Path + ": " + e.Err.Error() }
        //   return e.Op + " ltx file: " + e.Err.Error()
        if self.path.is_empty() {
            write!(f, "{} ltx file: {}", self.op, self.err)
        } else {
            write!(f, "{} ltx file {}: {}", self.op, self.path, self.err)
        }
    }
}

impl std::error::Error for LTXError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.err.as_ref())
    }
}

impl LTXError {
    /// Returns `true` when the error represents local state corruption that
    /// can be fixed by resetting and re-downloading from the remote replica.
    ///
    /// Returns `false` for transient OS errors (permission denied, I/O error)
    /// that should be retried with back-off instead.
    ///
    /// Ported from `LTXError.IsAutoRecoverable` in litestream.go:63-71.
    ///
    /// Uses chain-walking (via `source()`) to mirror Go's `errors.Is` semantics:
    /// a context-wrapped sentinel (e.g. `Error::Other(Box::new(Error::LTXCorrupted))`,
    /// mirroring `fmt.Errorf("%w: bad data", ErrLTXCorrupted)`) is still
    /// recognized as auto-recoverable.
    pub fn is_auto_recoverable(&self) -> bool {
        let err_ref: &dyn std::error::Error = self.err.as_ref();
        // os.IsNotExist equivalent: walk chain for io::ErrorKind::NotFound
        if chain_contains_not_found(err_ref) {
            return true;
        }
        // errors.Is(e.Err, ErrLTXMissing) — walk chain for marker or sentinel
        if chain_contains::<LTXMissingMarker>(err_ref)
            || chain_contains_error_variant(err_ref, ErrorVariantKind::LTXMissing)
        {
            return true;
        }
        // errors.Is(e.Err, ErrLTXCorrupted) || errors.Is(e.Err, ErrChecksumMismatch)
        // Walk chain for marker types or bare sentinel variants.
        if chain_contains::<LTXCorruptedMarker>(err_ref)
            || chain_contains_error_variant(err_ref, ErrorVariantKind::LTXCorrupted)
            || chain_contains::<ChecksumMismatchMarker>(err_ref)
            || chain_contains_error_variant(err_ref, ErrorVariantKind::ChecksumMismatch)
        {
            return true;
        }
        false
    }
}

// ── Chain-walking helpers ─────────────────────────────────────────────────────
//
// Go's `errors.Is` walks the full error chain via `Unwrap()`.  Rust's
// `std::error::Error::source()` is the equivalent.  The helpers below replicate
// that chain-walking so that a context-wrapped sentinel (e.g.
// `Error::Other(Box::new(Error::LTXCorrupted))`) is still detected as a
// recoverable cause — matching litestream.go:63-70.

/// Returns `true` if any error in the `source()` chain can be downcast to `E`.
fn chain_contains<E: std::error::Error + 'static>(err: &(dyn std::error::Error + 'static)) -> bool {
    if err.downcast_ref::<E>().is_some() {
        return true;
    }
    if let Some(src) = err.source() {
        return chain_contains::<E>(src);
    }
    false
}

/// Returns `true` if any error in the `source()` chain is an `io::Error` with
/// kind `ErrorKind::NotFound`.
fn chain_contains_not_found(err: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(io_err) = err.downcast_ref::<io::Error>() {
        if io_err.kind() == io::ErrorKind::NotFound {
            return true;
        }
    }
    if let Some(src) = err.source() {
        return chain_contains_not_found(src);
    }
    false
}

/// Discriminator for which `Error` sentinel variant we are searching for.
#[derive(Clone, Copy)]
enum ErrorVariantKind {
    LTXCorrupted,
    LTXMissing,
    ChecksumMismatch,
}

/// Returns `true` if any error in the `source()` chain is an `Error` enum
/// variant matching `kind`.
///
/// This handles the case where an `Error` sentinel is wrapped inside
/// `Error::Other(...)`, mirroring Go's `errors.Is(err, ErrLTXCorrupted)`.
fn chain_contains_error_variant(
    err: &(dyn std::error::Error + 'static),
    kind: ErrorVariantKind,
) -> bool {
    if let Some(e) = err.downcast_ref::<Error>() {
        let matches = match kind {
            ErrorVariantKind::LTXCorrupted => matches!(e, Error::LTXCorrupted),
            ErrorVariantKind::LTXMissing => matches!(e, Error::LTXMissing),
            ErrorVariantKind::ChecksumMismatch => matches!(e, Error::ChecksumMismatch),
        };
        if matches {
            return true;
        }
    }
    if let Some(src) = err.source() {
        return chain_contains_error_variant(src, kind);
    }
    false
}

// ── Marker error types for downcast detection ─────────────────────────────────
//
// The Go code uses `errors.Is` against sentinel `var` values.  Rust's
// `thiserror` approach is to use concrete unit-struct error types that we can
// `downcast_ref` to.  These are internal implementation details; callers use
// the public `new_ltx_error` constructor which accepts `Error` values and
// wraps them appropriately.

/// Marker wrapping `Error::LTXMissing` so `LTXError::is_auto_recoverable` can
/// detect it via `downcast_ref`.
#[derive(Debug)]
struct LTXMissingMarker;
impl std::fmt::Display for LTXMissingMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ltx file missing")
    }
}
impl std::error::Error for LTXMissingMarker {}

/// Marker wrapping `Error::LTXCorrupted`.
#[derive(Debug)]
struct LTXCorruptedMarker;
impl std::fmt::Display for LTXCorruptedMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ltx file corrupted")
    }
}
impl std::error::Error for LTXCorruptedMarker {}

/// Marker wrapping `Error::ChecksumMismatch`.
#[derive(Debug)]
struct ChecksumMismatchMarker;
impl std::fmt::Display for ChecksumMismatchMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid replica, checksum mismatch")
    }
}
impl std::error::Error for ChecksumMismatchMarker {}

// ── Public constructor ─────────────────────────────────────────────────────────

/// Creates a new [`LTXError`] with appropriate recovery hints based on the
/// underlying error kind.
///
/// Ported from `NewLTXError` in litestream@v0.5.11 litestream.go:74-93.
///
/// Uses chain-walking (matching Go's `errors.Is` semantics) so that a
/// context-wrapped sentinel — e.g. `Error::Other(Box::new(Error::LTXCorrupted))`,
/// mirroring `fmt.Errorf("%w: bad data", ErrLTXCorrupted)` — is still
/// classified and hinted correctly.
///
/// # Arguments
/// * `op`       – Operation name (e.g. `"open"`, `"read"`).
/// * `path`     – File path (empty string if unknown).
/// * `level`    – LTX compaction level.
/// * `min_txid` – Minimum TXID in the file.
/// * `max_txid` – Maximum TXID in the file.
/// * `err`      – Cause; accepts `Error` or any boxed error.
pub fn new_ltx_error(
    op: impl Into<String>,
    path: impl Into<String>,
    level: i32,
    min_txid: u64,
    max_txid: u64,
    err: Error,
) -> LTXError {
    let op = op.into();
    let path = path.into();

    // Choose the boxed inner error and hint based on the error kind.
    // We use chain-walking helpers (matching Go's errors.Is semantics) so that
    // a context-wrapped sentinel — e.g. Error::Other(Box::new(Error::LTXCorrupted))
    // mirroring fmt.Errorf("%w: bad data", ErrLTXCorrupted) — is classified
    // correctly.  The marker wrappers ensure is_auto_recoverable() can detect the
    // kind via downcast_ref after the boxed error is stored.
    //
    // Ported from NewLTXError litestream@v0.5.11 litestream.go:74-93.
    let err_as_std: &dyn std::error::Error = &err;

    let (inner, hint): (Box<dyn std::error::Error + Send + Sync>, String) =
        if chain_contains_not_found(err_as_std) {
            let hint = "LTX file is missing. This can happen after VACUUM, manual checkpoint, \
                        or state corruption. Run 'litestream reset <db>' or delete the \
                        .sqlite-litestream directory and restart."
                .to_string();
            // Preserve the original NotFound error for source() chain fidelity.
            (
                Box::new(io::Error::new(io::ErrorKind::NotFound, err.to_string())),
                hint,
            )
        } else if matches!(&err, Error::LTXMissing)
            || chain_contains::<LTXMissingMarker>(err_as_std)
            || chain_contains_error_variant(err_as_std, ErrorVariantKind::LTXMissing)
        {
            let hint = "LTX file is missing. This can happen after VACUUM, manual checkpoint, \
                        or state corruption. Run 'litestream reset <db>' or delete the \
                        .sqlite-litestream directory and restart."
                .to_string();
            (Box::new(LTXMissingMarker), hint)
        } else if matches!(&err, Error::LTXCorrupted)
            || chain_contains::<LTXCorruptedMarker>(err_as_std)
            || chain_contains_error_variant(err_as_std, ErrorVariantKind::LTXCorrupted)
        {
            let hint = "LTX file is corrupted. Delete the .sqlite-litestream directory and \
                        restart to recover from replica."
                .to_string();
            (Box::new(LTXCorruptedMarker), hint)
        } else if matches!(&err, Error::ChecksumMismatch)
            || chain_contains::<ChecksumMismatchMarker>(err_as_std)
            || chain_contains_error_variant(err_as_std, ErrorVariantKind::ChecksumMismatch)
        {
            let hint = "LTX file is corrupted. Delete the .sqlite-litestream directory and \
                        restart to recover from replica."
                .to_string();
            (Box::new(ChecksumMismatchMarker), hint)
        } else {
            (Box::new(OpaqueError(err.to_string())), String::new())
        };

    LTXError {
        op,
        path,
        level,
        min_txid,
        max_txid,
        err: inner,
        hint,
    }
}

/// Thin wrapper so we can box an arbitrary `Error` string for the catch-all arm.
#[derive(Debug)]
struct OpaqueError(String);
impl std::fmt::Display for OpaqueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for OpaqueError {}
