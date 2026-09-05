#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

//! `celld-ltx` provides embeddable streaming replication for a SQLite database.
//! It captures WAL data as LTX segments, reads and writes replica storage,
//! restores databases, compacts levels, and reads bundle objects.
//!
//! The original replication behavior is a from-scratch Rust reimplementation
//! of Litestream v0.5.11. The block format follows LTX v0.5.2.
//!
//! # Core position and identity helpers
//!
//! Ported from litestream@v0.5.11 litestream.go:18-203 and
//! ltx@v0.5.2 ltx.go:66-145.

pub mod bundle;
pub mod client;
mod codec;
pub mod compaction_level;
pub mod compactor;
pub mod db;
pub mod error;
pub mod host;
pub mod ltx;
mod lz4_block;
pub mod paged;
pub mod paged_vfs;
pub mod replica;
pub mod replica_compactor;
pub mod replica_url;
pub mod wal;

/// The unstable implementation API supports external verification tools.
///
/// This module has no compatibility guarantee.
#[doc(hidden)]
pub mod internal {
    pub use crate::db::internal as db;
    pub use crate::lz4_block::internal as lz4_block;
    pub use crate::replica::internal as replica;

    #[cfg(feature = "s3")]
    pub use crate::client::object_store::internal as object_store;
}

// ── Crate-root re-exports — the ergonomic public surface ────────────────
//
// These `pub use` aliases let a host use `celld_ltx::Db`, `celld_ltx::Replica`,
// and `celld_ltx::restore` without naming the owning module. The original module
// paths, such as `celld_ltx::db::Db`, remain valid.

// Re-export the error model at the crate root.
pub use error::{new_ltx_error, Error, LTXError, Result};
pub use host::{
    DirectFileSystem, FileSystem, HostDirEntry, HostFile, HostFileIo, HostMetadata, HostTaskError,
    LtxHost,
};

/// A SQLite database managed for replication (WAL-mode checkpoint takeover plus
/// the WAL→LTX capture loop). Re-exported from [`crate::db::Db`].
pub use db::Db;

/// SQLite checkpoint mode used by [`Db`] when it checkpoints the WAL.
/// Re-exported from [`crate::db::CheckpointMode`].
pub use db::CheckpointMode;

/// Connects a managed [`Db`] to a replication destination and drives the
/// single-replica sync loop. Re-exported from [`crate::replica::Replica`].
pub use replica::Replica;

/// Restores a database from a [`ReplicaClient`] into a fresh file, optionally up
/// to a target [`TXID`]. Re-exported from [`crate::replica::restore`].
pub use replica::restore;

/// The storage abstraction every replica backend implements.
/// Re-exported from [`crate::client::ReplicaClient`].
pub use client::ReplicaClient;

/// Metadata describing a single LTX file on a replica (the value type returned by
/// [`ReplicaClient`] listings). Re-exported from [`crate::ltx::FileInfo`].
pub use ltx::FileInfo;

/// A [`ReplicaClient`] that stores LTX files on the local filesystem.
/// Re-exported from [`crate::client::file::FileReplicaClient`].
pub use client::file::FileReplicaClient;

/// The bundle overlay (celld-original, self-contained).
/// Re-exported from [`crate::client::bundle`].
pub use client::bundle::BundleFetcher;
pub use client::bundle::BundleOverlayClient;
pub use client::bundle::LocatedRow;

/// The S3/R2/MinIO [`ReplicaClient`], behind the `s3` feature.
/// Re-exported from [`crate::client::object_store::ObjectStoreClient`].
#[cfg(feature = "s3")]
pub use client::object_store::ObjectStoreClient;

/// Configuration for the S3/R2/MinIO backend, behind the `s3` feature.
/// Re-exported from [`crate::client::object_store::ObjectStoreConfig`].
#[cfg(feature = "s3")]
pub use client::object_store::ObjectStoreConfig;

/// Which object-metadata name carries an LTX header timestamp, behind the
/// `s3` feature. Re-exported from
/// [`crate::client::object_store::TimestampMetadataKey`].
#[cfg(feature = "s3")]
pub use client::object_store::TimestampMetadataKey;

/// The underlying `object_store` crate, re-exported so a host can name the
/// shared `Arc<dyn ObjectStore>` that [`ObjectStoreConfig::build_store`] returns
/// and hand it to [`ObjectStoreClient::with_store`].
pub use object_store;

// ── Naming constants ──────────────────────────────────────────────────────────

/// Suffix appended to a database path to obtain the Litestream metadata
/// directory.
///
/// Ported from litestream@v0.5.11 litestream.go:19 (`MetaDirSuffix`).
pub const META_DIR_SUFFIX: &str = "-litestream";

// ── SQLite checkpoint modes ───────────────────────────────────────────────────
//
// Ported from litestream@v0.5.11 litestream.go:22-28.

pub const CHECKPOINT_MODE_PASSIVE: &str = "PASSIVE";
pub const CHECKPOINT_MODE_FULL: &str = "FULL";
pub const CHECKPOINT_MODE_RESTART: &str = "RESTART";
pub const CHECKPOINT_MODE_TRUNCATE: &str = "TRUNCATE";

// ── SQLite WAL size constants ─────────────────────────────────────────────────
//
// Ported from litestream@v0.5.11 litestream.go:95-128.

/// Size of a SQLite WAL file header, in bytes.
pub const WAL_HEADER_SIZE: usize = 32;

/// Size of a SQLite WAL frame header, in bytes.
pub const WAL_FRAME_HEADER_SIZE: usize = 24;

/// Byte offset of the checksum pair inside a WAL header.
pub const WAL_HEADER_CHECKSUM_OFFSET: usize = 24;

/// Byte offset of the checksum pair inside a WAL frame header.
pub const WAL_FRAME_HEADER_CHECKSUM_OFFSET: usize = 16;

// ── TXID ──────────────────────────────────────────────────────────────────────

/// A monotonically increasing transaction identifier.
///
/// Represented as a `u64` and formatted as a zero-padded 16-character
/// lowercase hex string (e.g. `"0000000000000001"`).  This matches the
/// on-disk LTX filename convention and `ltx.TXID.String()` in
/// ltx@v0.5.1 ltx.go:142-144.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TXID(pub u64);

impl TXID {
    /// The zero TXID.
    pub const ZERO: TXID = TXID(0);

    /// Returns the raw `u64` value.
    #[inline]
    pub fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TXID {
    /// Formats as a 16-digit lowercase hex string, zero-padded.
    ///
    /// Ported from `TXID.String()` in ltx@v0.5.1 ltx.go:142.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Parses a TXID from its canonical 16-character lowercase hex representation.
///
/// Returns an error if the string is not exactly 16 hex characters or contains
/// any character outside `[0-9a-fA-F]`.
///
/// Ported from `ParseTXID` in ltx@v0.5.1 ltx.go:130-138.
///
/// Note: Go's `strconv.ParseUint(s, 16, 64)` rejects sign prefixes (`+`, `-`).
/// Rust's `u64::from_str_radix` accepts a leading `+`, which would be a
/// behavioural divergence.  We pre-validate that every byte is a valid hex
/// digit to match Go's stricter contract.
pub fn parse_txid(s: &str) -> Result<TXID> {
    if s.len() != 16 {
        return Err(Error::Other(
            format!("invalid formatted transaction id length: {:?}", s).into(),
        ));
    }
    // Reject any character outside [0-9a-fA-F].  This matches Go's
    // strconv.ParseUint which rejects sign prefixes ('+'/'-') and any
    // non-hex byte, whereas Rust's from_str_radix would accept a leading '+'.
    if !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::Other(
            format!("invalid transaction id format: {:?}", s).into(),
        ));
    }
    let v = u64::from_str_radix(s, 16)
        .map_err(|_| Error::Other(format!("invalid transaction id format: {:?}", s).into()))?;
    Ok(TXID(v))
}

impl From<u64> for TXID {
    fn from(v: u64) -> Self {
        TXID(v)
    }
}

impl From<TXID> for u64 {
    fn from(t: TXID) -> Self {
        t.0
    }
}

// ── Checksum ──────────────────────────────────────────────────────────────────

/// A 64-bit LTX rolling / file checksum.
///
/// The high bit is the `ChecksumFlag` sentinel used by the LTX format to
/// distinguish a real checksum from an absent one.
///
/// Ported from ltx@v0.5.1 ltx.go (type alias `Checksum uint64`).
pub type Checksum = u64;

/// Flag bit set on every valid non-zero LTX checksum.
///
/// Ported from `ChecksumFlag` in ltx@v0.5.1 ltx.go:55.
pub const CHECKSUM_FLAG: Checksum = 1 << 63;

// ── Pos ──────────────────────────────────────────────────────────────────────

/// Replication position — the TXID plus a rolling post-apply checksum.
///
/// Together these two values uniquely identify the state of a database at a
/// point in the replication log.
///
/// Ported from `Pos` / `ParsePos` / `Pos.String` in ltx@v0.5.1 ltx.go:66-109.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Pos {
    /// Transaction ID at this position.
    pub txid: TXID,
    /// Rolling CRC checksum of the database state after applying the
    /// transaction at `txid`.
    pub post_apply_checksum: Checksum,
}

impl Pos {
    /// The zero position (no transactions applied yet).
    pub const ZERO: Pos = Pos {
        txid: TXID::ZERO,
        post_apply_checksum: 0,
    };

    /// Constructs a new `Pos`.
    ///
    /// Ported from `NewPos` in ltx@v0.5.1 ltx.go:72.
    pub fn new(txid: TXID, post_apply_checksum: Checksum) -> Self {
        Pos {
            txid,
            post_apply_checksum,
        }
    }

    /// Returns `true` when this is the zero position.
    ///
    /// Ported from `Pos.IsZero` in ltx@v0.5.1 ltx.go:107.
    pub fn is_zero(self) -> bool {
        self == Pos::ZERO
    }
}

impl std::fmt::Display for Pos {
    /// Formats as `"<txid>/<checksum>"` — e.g. `"0000000000000001/8000000000000001"`.
    ///
    /// Ported from `Pos.String` in ltx@v0.5.1 ltx.go:102.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{:016x}", self.txid, self.post_apply_checksum)
    }
}

/// Parses a `Pos` from its canonical `"<txid>/<checksum>"` string form.
///
/// The string must be exactly 33 characters: 16 hex digits, a slash, and
/// 16 hex digits.
///
/// Ported from `ParsePos` in ltx@v0.5.1 ltx.go:80-99.
pub fn parse_pos(s: &str) -> Result<Pos> {
    if s.len() != 33 {
        return Err(Error::Other(
            format!("invalid formatted position length: {:?}", s).into(),
        ));
    }
    let txid = parse_txid(&s[..16])
        .map_err(|_| Error::Other(format!("invalid position txid: {:?}", &s[..16]).into()))?;
    // Validate that byte 16 is '/' (the separator) even though
    // Go's ParsePos (ltx@v0.5.1 ltx.go:80-99) blindly slices s[:16] and s[17:]
    // without checking the separator byte, so Go would accept e.g.
    // "0000000000000001X8000000000000001".  We keep the stricter check because:
    // (a) the '/' is mandated by the format doc and Pos.String() always emits it,
    // (b) silently accepting a malformed separator would hide caller bugs, and
    // (c) no production caller requires compatibility with the malformed form.
    // This is the conservative interpretation because it rejects bad input.
    if s.as_bytes()[16] != b'/' {
        return Err(Error::Other(
            format!("invalid formatted position (missing /): {:?}", s).into(),
        ));
    }
    // Same sign-prefix pre-validation as parse_txid: reject non-hex characters
    // so we match Go's strconv.ParseUint contract for the checksum field too.
    let checksum_str = &s[17..];
    if !checksum_str.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::Other(
            format!("invalid position checksum: {:?}", checksum_str).into(),
        ));
    }
    let checksum = u64::from_str_radix(checksum_str, 16).map_err(|_| {
        Error::Other(format!("invalid position checksum: {:?}", checksum_str).into())
    })?;
    Ok(Pos {
        txid,
        post_apply_checksum: checksum,
    })
}

// ── SQLite checksum ───────────────────────────────────────────────────────────

/// Computes a running SQLite WAL checksum over a byte slice.
///
/// The slice length **must** be a multiple of 8 bytes (the function panics
/// otherwise, matching the `assert` in the Go source).
///
/// `s0` / `s1` are the running checksum values (pass `(0, 0)` for the first
/// call, then thread the result through subsequent calls for incremental
/// checksumming).
///
/// `big_endian` selects the byte order used to read 32-bit words from `b`:
/// `true` = big-endian (WAL headers with magic `0x377f0683`),
/// `false` = little-endian (WAL headers with magic `0x377f0682`).
///
/// Ported from `Checksum` in litestream@v0.5.11 litestream.go:110-119.
///
/// # Panics
/// Panics if `b.len() % 8 != 0`.
pub fn wal_checksum(big_endian: bool, s0: u32, s1: u32, b: &[u8]) -> (u32, u32) {
    assert!(b.len().is_multiple_of(8), "misaligned checksum byte slice");

    let mut s0 = s0;
    let mut s1 = s1;

    // Iterate over 8-byte units and compute checksum.
    // Matches litestream.go Checksum loop exactly.
    let mut i = 0usize;
    while i < b.len() {
        let w0 = if big_endian {
            u32::from_be_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]])
        } else {
            u32::from_le_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]])
        };
        let w1 = if big_endian {
            u32::from_be_bytes([b[i + 4], b[i + 5], b[i + 6], b[i + 7]])
        } else {
            u32::from_le_bytes([b[i + 4], b[i + 5], b[i + 6], b[i + 7]])
        };
        s0 = s0.wrapping_add(w0).wrapping_add(s1);
        s1 = s1.wrapping_add(w1).wrapping_add(s0);
        i += 8;
    }
    (s0, s1)
}

// ── Forward-slash path cleaning (Go stdlib `path`) ────────────────────────────
//
// The LTX path helpers below build object-store keys with Go's `path.Join`
// (NOT `filepath.Join`): the OS-independent, always-`/` package in
// `go/src/path/path.go`.  `path.Join` concatenates non-empty elements with a
// single `/` then runs the result through `path.Clean`, which collapses
// repeated separators, resolves `.`/`..`, and strips trailing slashes.  A
// naive `format!("{}/ltx", root)` skips this cleaning, so a root carrying a
// trailing slash (e.g. an S3 prefix `"backups/"`) would yield a DIFFERENT
// object key than the real binary (`"backups//ltx"` vs `"backups/ltx"`),
// breaking byte compatibility. We port `Clean`/`Join` byte-for-byte.

/// Lexically cleans a slash-separated path.
///
/// A faithful port of `Clean` in the Go standard library `path` package
/// (`go/src/path/path.go`, `func Clean`).  It applies these rules iteratively
/// until no further processing is possible:
///   1. Replace multiple slashes with a single slash.
///   2. Eliminate each `.` path-name element (the current directory).
///   3. Eliminate each inner `..` element along with the non-`..` element
///      preceding it.
///   4. Eliminate `..` elements that begin a rooted path (i.e. `/..` → `/`).
///
/// The returned path ends in a slash only if it is the root `"/"`.  An empty
/// input returns `"."`.
///
/// `pub(crate)` so `replica_url::clean_replica_url_path` can reuse Go's
/// `path.Clean` semantics rather than reimplementing them.
pub(crate) fn path_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }

    let bytes = path.as_bytes();
    let rooted = bytes[0] == b'/';
    let n = bytes.len();

    // Mirror Go's `lazybuf` with an explicit write cursor `w` over a fully
    // allocated buffer (we do not bother with Go's lazy aliasing optimisation,
    // but we DO preserve its cursor semantics, which the previous Vec-length
    // port got wrong).  `dotdot` marks the index below which `..` elements may
    // not backtrack.  The output is `buf[..w]`, so the byte at index `w` is the
    // next byte to be written — and is logically NOT part of the output yet.
    //
    // The backtrack loop must read `buf[w]` (the byte the cursor lands on),
    // NOT `buf[w-1]`: when it stops on a separator, that separator sits at
    // index `w` and is therefore EXCLUDED from `buf[..w]`.  A Vec-length model
    // that inspects `out.last()` (i.e. `buf[w-1]`) instead leaves the surviving
    // separator in the buffer, doubling it before the next element — the bug
    // this rewrite fixes.  See `go/src/path/path.go` `func Clean` / `lazybuf`.
    let mut buf: Vec<u8> = vec![0u8; n];
    let mut w = 0usize;
    let mut r = 0usize;
    let mut dotdot = 0usize;
    if rooted {
        buf[w] = b'/';
        w += 1;
        r = 1;
        dotdot = 1;
    }

    while r < n {
        if bytes[r] == b'/' {
            // empty path element
            r += 1;
        } else if bytes[r] == b'.' && (r + 1 == n || bytes[r + 1] == b'/') {
            // `.` element
            r += 1;
        } else if bytes[r] == b'.'
            && r + 1 < n
            && bytes[r + 1] == b'.'
            && (r + 2 == n || bytes[r + 2] == b'/')
        {
            // `..` element: remove to last `/`
            r += 2;
            if w > dotdot {
                // can backtrack: step back over the just-ended element body,
                // then keep stepping while the byte at the cursor is not '/'.
                // When this stops on a '/', the cursor rests AT that '/', so it
                // is excluded from `buf[..w]` (matching Go's `out.w` semantics).
                w -= 1;
                while w > dotdot && buf[w] != b'/' {
                    w -= 1;
                }
            } else if !rooted {
                // cannot backtrack, but not rooted, so append `..` element.
                if w > 0 {
                    buf[w] = b'/';
                    w += 1;
                }
                buf[w] = b'.';
                w += 1;
                buf[w] = b'.';
                w += 1;
                dotdot = w;
            }
        } else {
            // real path element; add slash if needed
            if (rooted && w != 1) || (!rooted && w != 0) {
                buf[w] = b'/';
                w += 1;
            }
            // copy element
            while r < n && bytes[r] != b'/' {
                buf[w] = bytes[r];
                w += 1;
                r += 1;
            }
        }
    }

    // Turn empty string into "."
    if w == 0 {
        return ".".to_string();
    }
    buf.truncate(w);
    // SAFETY-free: `buf` is built only from ASCII `/`, `.`, and bytes copied
    // verbatim from the valid-UTF-8 `path`, so it is always valid UTF-8.
    String::from_utf8(buf).expect("path_clean produced valid UTF-8")
}

/// Joins any number of path elements into a single slash-separated path,
/// cleaning the result.
///
/// A faithful port of `Join` in the Go standard library `path` package
/// (`go/src/path/path.go`, `func Join`): empty elements are ignored, and the
/// joined string is passed through [`path_clean`].
fn path_join(elem: &[&str]) -> String {
    // Go returns "" when the total length of all elements is zero (which, since
    // empty elements are skipped, means every element is empty).
    if elem.iter().all(|e| e.is_empty()) {
        return String::new();
    }
    let mut buf = String::new();
    for &e in elem {
        if !buf.is_empty() || !e.is_empty() {
            if !buf.is_empty() {
                buf.push('/');
            }
            buf.push_str(e);
        }
    }
    path_clean(&buf)
}

// ── LTX path helpers ──────────────────────────────────────────────────────────
//
// Ported from litestream@v0.5.11 litestream.go:184-197.

/// Returns the path to the LTX directory under a given root.
///
/// Ported from `LTXDir` in litestream@v0.5.11 litestream.go:185:
/// `return path.Join(root, "ltx")`.
pub fn ltx_dir(root: &str) -> String {
    path_join(&[root, "ltx"])
}

/// Returns the path to the LTX level sub-directory.
///
/// Ported from `LTXLevelDir` in litestream@v0.5.11 litestream.go:190-191:
/// `return path.Join(LTXDir(root), strconv.Itoa(level))`.  Note the
/// composition over the already-cleaned `LTXDir(root)`, which we preserve so
/// `..` resolution matches Go exactly.
pub fn ltx_level_dir(root: &str, level: u32) -> String {
    path_join(&[&ltx_dir(root), &level.to_string()])
}

/// Returns the path to a single LTX file for a given transaction range.
///
/// The filename follows the convention `<minTXID>-<maxTXID>.ltx` with both
/// TXIDs formatted as 16-digit lowercase hex.
///
/// Ported from `LTXFilePath` in litestream@v0.5.11 litestream.go:195-196:
/// `return path.Join(LTXLevelDir(root, level), ltx.FormatFilename(minTXID, maxTXID))`
/// and `FormatFilename` in ltx@v0.5.1 ltx.go:487
/// (`fmt.Sprintf("%s-%s.ltx", minTXID.String(), maxTXID.String())`).
pub fn ltx_file_path(root: &str, level: u32, min_txid: TXID, max_txid: TXID) -> String {
    let filename = format!("{}-{}.ltx", min_txid, max_txid);
    path_join(&[&ltx_level_dir(root, level), &filename])
}
