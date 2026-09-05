//! db.rs — SQLite database lifecycle: WAL-mode setup, the long-running read-lock
//! checkpoint takeover, the WAL→LTX capture loop, and manual checkpointing.
//! Ported from litestream@v0.5.11 `db.go`.
//!
//! The highest-risk interaction is a long-running read transaction with a manual
//! `PRAGMA wal_checkpoint`, so the implementation uses independently tested steps.
//!
//! ## Async/blocking shape
//! `rusqlite::Connection` is `!Sync` and a `rusqlite::Transaction` borrows its
//! `Connection`. Rather than thread a borrow across `.await`, this module keeps
//! the capture API **synchronous** and owns the connection directly: the
//! long-running read transaction is held with raw `BEGIN`/`ROLLBACK` SQL plus a
//! `read_lock_held` flag — exactly as Go does (`acquireReadLock` runs `BEGIN` +
//! `SELECT COUNT(1)`, db.go:956-976; `releaseReadLock` rolls back, db.go:979-992).
//! `Replica` drives `sync()` and `checkpoint()` from a blocking context
//! (`spawn_blocking` or a dedicated DB thread). This sidesteps the !Sync/borrow
//! problem entirely and makes the idempotent-release behavior (issue #934) a
//! trivial flag check.
//!
//! ## What this implements (the functional capture path)
//! - `open`/`init`: WAL-mode DSN, `wal_autocheckpoint(0)`, control tables, read
//!   page size, acquire the read lock, ensure the WAL has ≥1 frame.
//! - `acquire_read_lock`/`release_read_lock`: the checkpoint takeover; release is
//!   idempotent (db.go:979-992 / issue #934).
//! - `sync` → `verify` → `sync_inner` → `write_ltx_from_wal`/`write_ltx_from_db`:
//!   diff the real WAL against the last LTX position and write the next L0 LTX
//!   file (`db.go:1517-1723`), with atomic tmp→rename and the pos cache.
//! - `verify`: the snapshot-on-continuity-break branch lattice
//!   (`db.go:1296-1436`), including the issue #900 / #927 edge cases.
//! - `checkpoint`/`exec_checkpoint`: release→`PRAGMA wal_checkpoint(<mode>)`→
//!   re-acquire (`db.go:1875-1919`) and the two-phase WAL-restart handling
//!   (`db.go:1808-1873`).
//! - `checkpoint_if_needed`: the 3-tier policy + the three anti-feedback flags
//!   (`synced_since_checkpoint`/`synced_to_wal_end`/`last_synced_wal_offset`,
//!   issues #896/#927/#997).
//! - Litestream #1292 / celld #150: seal passive checkpoints with a writer
//!   barrier, retain database-growth pages, and snapshot truncate boundaries.
//! - `crc64`, `pos` (cached + `LTXError` mapping), `reset_local_state`,
//!   `snapshot_to_writer`.
//!
//! ## Deferred work outside the functional path
//! - `setPersistWAL` (the `unsafe` `sqlite3_file_control(SQLITE_FCNTL_PERSIST_WAL)`
//!   FFI): only matters when *all* connections close and SQLite
//!   would delete the WAL; the capture path keeps its own connection open.
//! - The background monitor loop, `ensure_exists`, `sync_status`, `sync_and_wait`,
//!   retention, and retry behavior are not implemented.
//! - A `loom` model of the lock protocol.

use crate::error::{new_ltx_error, Error, Result};
use crate::ltx::{self, lock_pgno, Crc64};
use crate::wal::WalReader;
use crate::{
    ltx_file_path, ltx_level_dir, Pos, CHECKPOINT_MODE_PASSIVE, CHECKPOINT_MODE_RESTART,
    CHECKPOINT_MODE_TRUNCATE, META_DIR_SUFFIX, TXID, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
};
use rusqlite::ffi;
use rusqlite::Connection;
use rusqlite::OpenFlags;
use std::collections::HashMap;
use std::ffi::c_int;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Maps a `rusqlite::Error` into the crate error type.
fn sql_err(e: rusqlite::Error) -> Error {
    Error::Other(Box::new(e))
}

/// SQLite checkpoint mode. Replaces Go's stringly-typed `mode` param; `Display`
/// interpolates straight into `PRAGMA wal_checkpoint(<mode>)` (db.go:1905), so it
/// must render exactly `PASSIVE`/`FULL`/`RESTART`/`TRUNCATE`.
///
/// Ported from litestream@v0.5.11 litestream.go:22-28. RESTART was removed from
/// automatic use (issue #724) but is still callable (`crc64` forces it), so all
/// four variants are kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointMode {
    /// Non-blocking checkpoint; skips if there are active transactions.
    Passive,
    /// Like RESTART but does not block new transactions before flushing.
    Full,
    /// Blocks new transactions, flushes, and restarts the WAL.
    Restart,
    /// Like RESTART plus truncates the WAL file to zero length.
    Truncate,
}

/// The three columns of `PRAGMA wal_checkpoint`: whether a lock stopped the
/// checkpoint, the frames in the WAL, and the frames backfilled.
#[derive(Clone, Copy, Debug)]
struct CheckpointPragma {
    busy: i64,
    wal_frames: i64,
    backfilled: i64,
}

impl std::fmt::Display for CheckpointMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            CheckpointMode::Passive => CHECKPOINT_MODE_PASSIVE,
            CheckpointMode::Full => crate::CHECKPOINT_MODE_FULL,
            CheckpointMode::Restart => CHECKPOINT_MODE_RESTART,
            CheckpointMode::Truncate => CHECKPOINT_MODE_TRUNCATE,
        };
        f.write_str(s)
    }
}

/// `verify()`'s decision result (db.go:1509-1515).
#[derive(Debug, Clone, Default)]
struct SyncInfo {
    /// End of the previous LTX read (byte offset into the WAL).
    offset: i64,
    salt1: u32,
    salt2: u32,
    /// Database page count recorded by the previous LTX file.
    prev_commit: u32,
    /// If true, a full snapshot is required.
    snapshotting: bool,
    /// Reason for the snapshot, for logging and diagnostics.
    reason: String,
}

/// Databases larger than this many pages (4 MiB at 4 KiB) must outgrow
/// themselves in the WAL before a TRUNCATE checkpoint, since the checkpoint
/// ends in a boundary image of the whole database. Smaller ones keep the
/// absolute `truncate_page_n` threshold.
const RELATIVE_TRUNCATE_PAGES: u32 = 1024;

/// A SQLite database managed for replication.
///
/// Owns the connection (WAL mode, auto-checkpoint disabled) so litestream — not
/// SQLite — decides when the WAL is checkpointed, and holds the long-running read
/// transaction that takes over checkpointing.
///
/// Ported from `DB` (db.go:64-198). The synchronous `!Sync` connection stays on
/// the thread that drives it.
/// What `verify` reads back from the last L0 this instance wrote: the
/// header fields, plus the final consumed WAL frame's page. The ported
/// check decodes the whole L0 and searches its pages for that frame; the
/// L0's entry for the final frame's page IS that frame (page-map keeps
/// the last write per page, and nothing follows the final frame), so
/// comparing against the cached frame answers the same question — and a
/// spurious mismatch only forces a snapshot, the safe direction.
fn snapshot_reason_code(reason: &str) -> u8 {
    match reason {
        "" => 1, // first sync leaves the default reason empty
        "wal truncated by another process" => 2,
        "wal header salt reset, snapshotting" => 3,
        "last page does not exist in last ltx file, wal overwritten by another process" => 4,
        "full or restart checkpoint detected, snapshotting" => 5,
        "checkpoint boundary snapshot" => 6,
        "WAL restarted before passive checkpoint barrier" => 8,
        _ => 7,
    }
}

#[derive(Clone, Debug)]
struct LastL0Header {
    wal_offset: i64,
    wal_size: i64,
    wal_salt1: u32,
    wal_salt2: u32,
    commit: u32,
    final_pgno: u32,
    final_page: Vec<u8>,
}

/// Per-phase wall time of one `Db::sync` call, in microseconds.
#[derive(Clone, Copy, Debug, Default)]
pub struct SyncTiming {
    /// Control-table self-heal and WAL existence checks.
    pub prepare_us: u64,
    /// The two halves of `prepare_us`: the schema-version pragma (plus
    /// the DDL when it fires) and the WAL existence stat, so a field
    /// inflation names its syscall.
    pub schema_check_us: u64,
    pub wal_exists_us: u64,
    /// `verify`: the WAL scan and continuity validation.
    pub verify_us: u64,
    /// `sync_inner` minus the fsync: WAL page reads, LTX encode, the file
    /// write and rename.
    pub encode_write_us: u64,
    /// The sub-phases of `encode_write_us`, so a field inflation names its
    /// line instead of hiding in the aggregate: position resolution, the
    /// WAL read (tail or full), page-map plus page collection, the LTX
    /// encode, and the file write-and-rename.
    pub pos_us: u64,
    pub wal_read_us: u64,
    pub map_collect_us: u64,
    pub ltx_encode_us: u64,
    pub file_write_us: u64,
    /// The WAL length seen by this call, for scaling.
    pub wal_len_bytes: u64,
    /// True when this call snapshotted (the full-database path).
    pub snapshot: bool,
    /// Which WAL read ran: 0 valid tail, 1 full because snapshotting, 2 valid
    /// prefix because the offset sat at the WAL start, 3 full because the
    /// bounded read fell back. `wal_read_bytes` is what that read actually
    /// transferred, including a bounded probe past the valid checksum chain.
    pub wal_read_kind: u8,
    pub wal_read_bytes: u64,
    /// The bytes the sync allocated to hold what it read: the tail plus the
    /// header on the tail path, the file otherwise.
    pub wal_image_bytes: u64,
    /// Which verify branch forced the snapshot: 0 none, 1 first sync,
    /// 2 wal truncated by another process, 3 salt reset, 4 last page
    /// missing from the last L0, 5 full or restart checkpoint detected,
    /// 6 checkpoint boundary, 7 other, 8 WAL restarted before the passive
    /// checkpoint barrier.
    pub snapshot_reason: u8,
    /// The cut file's fsync (zero under lazy capture).
    pub fsync_us: u64,
    /// `checkpoint_if_needed`: SQLite's checkpoint, including its own
    /// writes and fsyncs through the SQLite VFS.
    pub checkpoint_us: u64,
    /// One when this call ran a checkpoint pragma or swallowed its busy
    /// error, else zero. Counters rather than flags, so the round ledger
    /// sums them across cells like every other field here. A Queue soak
    /// showed 14% of checkpoint rounds repeating on the next round with only
    /// the sealing frame captured, and `checkpoint_us` alone could not say
    /// whether the backfill was short, the pragma was busy, or the sealing
    /// write failed to restart the WAL.
    pub checkpoint_runs: u64,
    /// The frames the WAL held when the pragma ran and the frames it had
    /// backfilled when it returned: `PRAGMA wal_checkpoint`'s second and
    /// third columns. Equal for a complete backfill; a shortfall names a
    /// reader that pinned the WAL.
    pub checkpoint_wal_frames: u64,
    pub checkpoint_backfilled: u64,
    /// One when the pragma reported a lock it could not take (its first
    /// column), and one when the checkpoint path failed with `SQLITE_BUSY`
    /// and the sync swallowed the error.
    pub checkpoint_busy: u64,
    pub checkpoint_busy_errors: u64,
    /// One when the WAL header changed across the sealing write, so the
    /// logical WAL restarted at its header.
    pub checkpoint_restarts: u64,
}

pub struct Db {
    host: crate::LtxHost,
    path: PathBuf,
    meta_path: PathBuf,
    /// Main connection: writes, PRAGMAs, checkpoints, page-size reads.
    conn: Connection,
    /// Dedicated connection that holds the long-running read transaction.
    ///
    /// Go's `db.db` is a `*sql.DB` **connection pool**: the read lock (`db.rtx`)
    /// lives on one pooled connection while every other `db.db.Exec` grabs a
    /// *different* connection from the pool. A single `rusqlite::Connection`
    /// cannot do that — an `INSERT` issued on the same connection that holds the
    /// read transaction would be buffered in that transaction and not flushed to
    /// the WAL (it would not write a fresh WAL page after a TRUNCATE checkpoint).
    /// So the read lock gets its own connection, mirroring the pool's separation.
    rtx_conn: Connection,
    page_size: u32,

    /// `true` while the long-running read transaction is open (db.go:70 `rtx`).
    /// We track a flag rather than holding a borrowing `rusqlite::Transaction`.
    read_lock_held: bool,

    /// The last `sync` call's per-phase wall time. Telemetry only: the
    /// closed-book capture ledger reads it; nothing branches on it.
    last_sync_timing: SyncTiming,

    // ── Tunables (db.go:131-197) ────────────────────────────────────────────
    /// PASSIVE checkpoint threshold, in pages (db.go:34 default 1000).
    pub min_checkpoint_page_n: u32,
    /// TRUNCATE checkpoint threshold, in pages; 0 disables (db.go:35 default).
    pub truncate_page_n: u32,
    /// Time-based PASSIVE checkpoint interval; 0 disables (db.go:32 default 60s).
    pub checkpoint_interval: Duration,
    /// Busy timeout for SQLite locks (db.go:33 default 1s).
    pub busy_timeout: Duration,

    // ── Anti-feedback-loop bookkeeping (issues #896/#927/#997) ──────────────
    /// True once data has synced since the last checkpoint (#896, db.go:80).
    synced_since_checkpoint: bool,
    /// True if the last sync reached the exact WAL EOF (#927, db.go:88).
    synced_to_wal_end: bool,
    /// Logical end of WAL content after the last sync = `WALOffset + WALSize`
    /// from the last LTX (#997, db.go:96). Used for checkpoint thresholds
    /// instead of file size (stale post-checkpoint frames inflate file size).
    last_synced_wal_offset: i64,
    /// The database's page count as the last capture saw it, for the
    /// truncate guard: a second stat per sync would break the syscall
    /// ledger, and the capture already read the size.
    last_db_pages: u32,
    /// Logical WAL offset through which the last checkpoint backfilled the
    /// database, in the current WAL's coordinates; `WAL_HEADER_SIZE` after
    /// a restart and 0 before any checkpoint. The passive threshold counts
    /// frames appended past this point. The port compared the whole logical
    /// size, which re-fired the checkpoint on every sync after a checkpoint
    /// whose sealing write could not restart the WAL: the backfilled frames
    /// kept counting, so a Queue owner paid a writer barrier, a sealing
    /// write, and a fresh LTX per round until a restart succeeded.
    checkpointed_wal_offset: i64,
    /// The schema version `ensure_control_tables` last verified. The
    /// self-heal exists for a swept `sqlite_schema`, and any sweep bumps
    /// SQLite's schema version, so an unchanged version proves the tables
    /// still stand without re-running DDL through the parser every capture
    /// (~0.8 ms per sync in the 2026-08-25 closed-book ledger).
    verified_schema_version: Option<i64>,
    /// The header fields of the last L0 this instance wrote, keyed by its
    /// TXID. `verify` needs exactly these to place the next incremental
    /// read; re-reading the whole L0 file to parse its header cost more
    /// than the header is long. A miss (restore, compaction, reopen)
    /// falls back to the file.
    last_l0_header: Option<(TXID, LastL0Header)>,

    /// Cached L0 position; `None` = invalid (db.go:106-109).
    pos_cache: Option<Pos>,
    /// The L0 level directory exists: created on the first cut (or after a
    /// `NotFound` on a later one), not probed with `mkdir` on every sync.
    l0_dir_ready: bool,
    /// The WAL, opened once. Every sync used to open it three times and stat
    /// it five; each was a path walk on a directory the node fsyncs thousands
    /// of times a second. See [`Db::with_wal_file`].
    wal_file: Option<crate::HostFile>,
    /// Last L0 `FileInfo` (db.go:99-102; only L0 is tracked in the one-shot).
    max_l0_file_info: Option<ltx::FileInfo>,
}

/// DDL for the replication control tables managed inside each database:
/// `_litestream_seq` forces WAL writes when empty; `_litestream_lock` forces
/// a write lock during sync (db.go:857-864). The text is byte-visible:
/// `sqlite_schema` stores it verbatim in a replicated page, so it must stay
/// character-identical to litestream's, and one copy serves every creation
/// site.
const CONTROL_TABLES_DDL: &str =
    "CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);\
     CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);";

/// True for exactly the two control tables above, compared without case the
/// way SQLite compares identifiers. Application-facing paths (deleteAll
/// sweeps, the SQL authorizer) must exempt these names, so the predicate
/// lives here with the tables. Exact names, not a `_litestream_` prefix:
/// ltx is a port of a pinned litestream, and workerd reserves only `_cf_`,
/// so any other name stays reachable by application SQL.
pub fn is_control_table(name: &str) -> bool {
    name.eq_ignore_ascii_case("_litestream_seq") || name.eq_ignore_ascii_case("_litestream_lock")
}

impl Db {
    /// Default minimum-checkpoint page count (`DefaultMinCheckpointPageN`,
    /// db.go:34).
    pub const DEFAULT_MIN_CHECKPOINT_PAGE_N: u32 = 1000;
    /// Default truncate page count (`DefaultTruncatePageN`, db.go:35).
    pub const DEFAULT_TRUNCATE_PAGE_N: u32 = 121_359;

    /// Default checkpoint interval (`DefaultCheckpointInterval`, db.go:32).
    pub const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
    /// Default busy timeout (`DefaultBusyTimeout`, db.go:33).
    pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(1);

    /// Opens and initializes the database with litestream's WAL-mode setup and
    /// acquires the long-running read lock.
    ///
    /// Ported from `DB.init` (db.go:795-911) — the connection-setup half plus the
    /// read-lock acquire and `ensureWALExists`.
    pub fn open(path: impl AsRef<Path>) -> Result<Db> {
        Self::open_with_host(path, crate::LtxHost::default())
    }

    /// Opens the database with an injected clock and executor host.
    pub fn open_with_host(path: impl AsRef<Path>, host: crate::LtxHost) -> Result<Db> {
        Self::open_with_host_and_optional_vfs(path, host, None)
    }

    /// Opens both managed connections through a named SQLite VFS. Used by the
    /// fault-injection VFS of the test suite and by paged restore's fault-in VFS.
    pub fn open_with_host_and_vfs(
        path: impl AsRef<Path>,
        host: crate::LtxHost,
        vfs: &str,
    ) -> Result<Db> {
        Self::open_with_host_and_optional_vfs(path, host, Some(vfs))
    }

    fn open_with_host_and_optional_vfs(
        path: impl AsRef<Path>,
        host: crate::LtxHost,
        vfs: Option<&str>,
    ) -> Result<Db> {
        let path = path.as_ref().to_path_buf();
        let meta_path = Self::meta_path_for(&path);

        let open = |path: &Path| match vfs {
            Some(vfs) => Connection::open_with_flags_and_vfs(path, OpenFlags::default(), vfs),
            None => Connection::open(path),
        };
        let conn = open(&path).map_err(sql_err)?;

        // DSN pragmas: busy_timeout + wal_autocheckpoint(0) (db.go:818).
        // autocheckpoint MUST be 0 because litestream owns checkpointing.
        // Per-connection, not per-file: the cell's own writer
        // connection keeps SQLite's default 1000-page autocheckpoint, which
        // is safe because this connection's long-running read transaction
        // pins a WAL read mark, so a PASSIVE checkpoint elsewhere can
        // backfill but never reset or truncate the WAL under the reader.
        conn.busy_timeout(Self::DEFAULT_BUSY_TIMEOUT)
            .map_err(sql_err)?;
        conn.pragma_update(None, "wal_autocheckpoint", 0)
            .map_err(sql_err)?;

        // Enable WAL; SQLite returns the new mode on success (db.go:849-853).
        let mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .map_err(sql_err)?;
        if mode != "wal" {
            return Err(Error::Other(
                format!("enable wal failed, mode={mode:?}").into(),
            ));
        }

        conn.execute_batch(CONTROL_TABLES_DDL).map_err(sql_err)?;

        // Dedicated read-lock connection (mirrors a second pooled connection).
        let rtx_conn = open(&path).map_err(sql_err)?;
        rtx_conn
            .busy_timeout(Self::DEFAULT_BUSY_TIMEOUT)
            .map_err(sql_err)?;

        let mut db = Db {
            host,
            path,
            meta_path,
            conn,
            rtx_conn,
            page_size: 0,
            read_lock_held: false,
            last_sync_timing: SyncTiming::default(),
            min_checkpoint_page_n: Self::DEFAULT_MIN_CHECKPOINT_PAGE_N,
            truncate_page_n: Self::DEFAULT_TRUNCATE_PAGE_N,
            checkpoint_interval: Self::DEFAULT_CHECKPOINT_INTERVAL,
            busy_timeout: Self::DEFAULT_BUSY_TIMEOUT,
            synced_since_checkpoint: false,
            synced_to_wal_end: false,
            last_synced_wal_offset: 0,
            last_db_pages: 0,
            checkpointed_wal_offset: 0,
            verified_schema_version: None,
            last_l0_header: None,
            pos_cache: None,
            l0_dir_ready: false,
            wal_file: None,
            max_l0_file_info: None,
        };

        // Start the long-running read transaction (db.go:867-871).
        db.acquire_read_lock()?;

        // Read page size (db.go:874-878).
        let page_size: i64 = db
            .conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .map_err(sql_err)?;
        if page_size <= 0 {
            return Err(Error::Other(
                format!("invalid db page size: {page_size}").into(),
            ));
        }
        // Validated > 0 above; SQLite page sizes are <= 65536.
        db.page_size = page_size as u32;

        // Ensure the meta directory exists (db.go:880-883).
        db.host.create_dir_all(&db.meta_path)?;

        // Clear crash-leftover temp files (db.go:576).
        remove_tmp_files(&db.host, &db.meta_path)?;

        // Ensure the WAL has at least one frame (db.go:886-888).
        db.ensure_wal_exists()?;

        Ok(db)
    }

    // ── Paths ──────────────────────────────────────────────────────────────

    /// Computes the litestream meta-directory path (`.<file>-litestream`) for a
    /// database file path, without opening it. Public so a host can locate (e.g.
    /// to wipe, during a hard recovery) the meta dir of a database it no longer
    /// has an open [`Db`] handle for. Mirrors `DB.MetaPath` (db.go:292) at the
    /// path level.
    pub fn meta_path_for_path(path: impl AsRef<Path>) -> PathBuf {
        Self::meta_path_for(path.as_ref())
    }

    fn meta_path_for(path: &Path) -> PathBuf {
        // Go: filepath.Join(dir, "."+file+MetaDirSuffix) (db.go:206).
        let dir = path.parent();
        let file = path.file_name().map(|s| s.to_owned()).unwrap_or_default();
        let mut name = std::ffi::OsString::from(".");
        name.push(&file);
        name.push(META_DIR_SUFFIX);
        match dir {
            Some(d) if !d.as_os_str().is_empty() => d.join(name),
            _ => PathBuf::from(name),
        }
    }

    /// The database file path. Ported from `DB.Path` (db.go:275).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Path to the litestream meta directory (`.<file>-litestream`).
    /// Ported from `DB.MetaPath` (db.go:292).
    pub fn meta_path(&self) -> &Path {
        &self.meta_path
    }

    /// Path to SQLite's WAL sidecar file (`<db>-wal`).
    /// Ported from `DB.WALPath` (db.go:287-289).
    pub fn wal_path(&self) -> PathBuf {
        let mut s = self.path.clone().into_os_string();
        s.push("-wal");
        PathBuf::from(s)
    }

    /// Root LTX directory (`<meta>/ltx`). Ported from `DB.LTXDir` (db.go:302).
    fn ltx_dir(&self) -> String {
        crate::ltx_dir(&self.meta_path.to_string_lossy())
    }

    /// LTX level sub-directory. Ported from `DB.LTXLevelDir` (db.go:332).
    fn ltx_level_dir(&self, level: u32) -> String {
        ltx_level_dir(&self.meta_path.to_string_lossy(), level)
    }

    /// Local path of a single LTX file. Ported from `DB.LTXPath` (db.go:338).
    ///
    /// `pub` so the [`crate::replica::Replica`] can read the local L0 LTX files
    /// the capture loop wrote and upload them (the Go exported `DB.LTXPath`,
    /// used by `Replica.uploadLTXFile`, replica.go:183).
    pub fn ltx_path(&self, level: u32, min_txid: TXID, max_txid: TXID) -> String {
        ltx_file_path(&self.meta_path.to_string_lossy(), level, min_txid, max_txid)
    }

    /// Reads one local LTX file through the host filesystem.
    pub fn read_ltx_file(&self, level: u32, min_txid: TXID, max_txid: TXID) -> Result<Vec<u8>> {
        let path = self.ltx_path(level, min_txid, max_txid);
        Ok(self.host.read(Path::new(&path))?)
    }

    /// The SQLite page size, in bytes. Ported from `DB.PageSize` (db.go:445).
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    // ── Read-lock takeover (checkpoint prevention) ─────────────────────────

    /// Begins a long-running read transaction to prevent external checkpoints.
    ///
    /// Ported from `acquireReadLock` (db.go:956-976): `BEGIN` then `SELECT
    /// COUNT(1) FROM _litestream_seq` to obtain the SHARED read lock. Held with
    /// raw SQL + a flag instead of a borrowing `rusqlite::Transaction` (see
    /// module docs). Idempotent: a no-op if already held.
    fn acquire_read_lock(&mut self) -> Result<()> {
        if self.read_lock_held {
            return Ok(());
        }
        self.rtx_conn
            .prepare_cached("BEGIN")
            .and_then(|mut statement| statement.execute([]))
            .map_err(sql_err)?;
        // Execute a read query to obtain the read lock. On failure, roll back.
        if let Err(e) = self
            .rtx_conn
            .query_row("SELECT COUNT(1) FROM _litestream_seq", [], |r| {
                r.get::<_, i64>(0)
            })
        {
            let _ = self.rtx_conn.execute_batch("ROLLBACK");
            return Err(sql_err(e));
        }
        self.read_lock_held = true;
        Ok(())
    }

    /// Rolls back the long-running read transaction.
    ///
    /// Ported from `releaseReadLock` (db.go:979-992). Uses the `rollback` helper
    /// semantics: a "no transaction is active" / "already rolled back" error is
    /// swallowed (issue #934) — a double release must return
    /// `Ok(())`. The `read_lock_held` flag is cleared regardless.
    fn release_read_lock(&mut self) -> Result<()> {
        if !self.read_lock_held {
            return Ok(());
        }
        self.read_lock_held = false;
        rollback(&self.rtx_conn)
    }

    // ── WAL bootstrap ──────────────────────────────────────────────────────

    /// Recreates the control tables if they are missing. They are created at
    /// open, but an application-level sweep of `sqlite_schema` can drop them
    /// (they do not carry the protected `_cf_` prefix); `CREATE TABLE IF NOT
    /// EXISTS` on an existing table is a no-op, so this is safe to run on
    /// every capture.
    fn ensure_control_tables(&mut self) -> Result<()> {
        // A swept control table is a schema change, and every schema change
        // bumps SQLite's schema version — so an unchanged version proves
        // the last verification still holds and the DDL (a full parse and
        // execute per statement) can be skipped on the hot capture path.
        // Through the statement cache: this guard runs on every sync, and a
        // fresh `PRAGMA` per sync was one SQL compilation per capture on the
        // fleet profile.
        let version: i64 = self
            .conn
            .prepare_cached("PRAGMA schema_version")
            .and_then(|mut statement| statement.query_row([], |row| row.get(0)))
            .map_err(sql_err)?;
        if self.verified_schema_version == Some(version) {
            return Ok(());
        }
        self.conn
            .execute_batch(CONTROL_TABLES_DDL)
            .map_err(sql_err)?;
        let verified: i64 = self
            .conn
            .prepare_cached("PRAGMA schema_version")
            .and_then(|mut statement| statement.query_row([], |row| row.get(0)))
            .map_err(sql_err)?;
        self.verified_schema_version = Some(verified);
        Ok(())
    }

    /// Ensures the real WAL exists and has a header.
    ///
    /// Ported from `ensureWALExists` (db.go:1199-1209): exit early if the WAL
    /// header is present; otherwise force a write to `_litestream_seq`.
    /// Runs `op` on the held WAL handle, opening it on first use. A WAL that
    /// SQLite recreated, or that a truncation shortened under a read, surfaces
    /// as `NotFound` or `UnexpectedEof`; the handle is dropped and `op` runs
    /// once more on a fresh open, so a stale handle costs one retry, never a
    /// wrong answer. Any other error also drops the handle.
    fn with_wal_file<T>(
        &mut self,
        op: impl Fn(&mut crate::HostFile) -> std::io::Result<T>,
    ) -> std::io::Result<T> {
        let mut attempt = 0;
        loop {
            if self.wal_file.is_none() {
                self.wal_file = Some(self.host.open(&self.wal_path())?);
            }
            let file = self.wal_file.as_mut().expect("opened above");
            match op(file) {
                Ok(value) => return Ok(value),
                Err(error) => {
                    self.wal_file = None;
                    let retry = attempt == 0
                        && matches!(
                            error.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::UnexpectedEof
                        );
                    if !retry {
                        return Err(error);
                    }
                    attempt += 1;
                }
            }
        }
    }

    /// The 32-byte WAL header through the held handle (`readWALHeader`,
    /// litestream.go:138-148: an error if the file is missing or short).
    fn wal_header_bytes(&mut self) -> Result<[u8; WAL_HEADER_SIZE]> {
        let bytes = self.with_wal_file(|file| file.read_exact_at(0, WAL_HEADER_SIZE))?;
        Ok(bytes
            .try_into()
            .expect("the exact read has the header size"))
    }

    /// `n` bytes at `offset` of the WAL through the held handle
    /// (`readWALFileAt`, litestream.go:152-166: a short read is an error).
    fn wal_bytes_at(&mut self, offset: i64, n: i64) -> Result<Vec<u8>> {
        Ok(self.with_wal_file(|file| file.read_exact_at(offset as u64, n as usize))?)
    }

    fn ensure_wal_exists(&mut self) -> Result<()> {
        if self.wal_file_size()? >= WAL_HEADER_SIZE as i64 {
            return Ok(());
        }
        self.conn
            .execute_batch(
                "INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) \
                 ON CONFLICT (id) DO UPDATE SET seq = seq + 1",
            )
            .map_err(sql_err)?;
        Ok(())
    }

    /// Size of the WAL file in bytes, 0 if absent. Ported from `walFileSize`
    /// (db.go:1183-1191).
    fn wal_file_size(&mut self) -> Result<i64> {
        match self.with_wal_file(|file| file.file_len()) {
            Ok(len) => Ok(len as i64),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    /// The size of the main database file in bytes, 0 if absent.
    fn db_file_size(&self) -> Result<i64> {
        match self.host.metadata(&self.path) {
            Ok(md) => Ok(md.len as i64),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    // ── Position ───────────────────────────────────────────────────────────

    /// The highest `(min,max)` TXID pair in the L0 directory, `(0,0)` if none.
    /// Ported from `DB.MaxLTX` (db.go:363-380).
    fn max_ltx(&self) -> Result<(TXID, TXID)> {
        let dir = self.ltx_level_dir(0);
        let entries = match self.host.read_dir(Path::new(&dir)) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((TXID(0), TXID(0))),
            Err(e) => return Err(e.into()),
        };
        let mut min_txid = TXID(0);
        let mut max_txid = TXID(0);
        for ent in entries {
            let name = ent.file_name;
            let name = name.to_string_lossy();
            if let Ok((mn, mx)) = ltx::parse_filename(&name) {
                if mx > max_txid {
                    min_txid = mn;
                    max_txid = mx;
                }
            }
        }
        Ok((min_txid, max_txid))
    }

    /// The current replication position (cached; recomputed from the max L0 file).
    ///
    /// Ported from `DB.Pos` (db.go:392-425). Wraps fs/decode failures in
    /// `LTXError` (db.go:412,418).
    pub fn pos(&mut self) -> Result<Pos> {
        if let Some(p) = self.pos_cache {
            return Ok(p);
        }

        let (min_txid, max_txid) = self.max_ltx()?;
        if min_txid == TXID(0) {
            return Ok(Pos::ZERO); // no replication yet
        }

        let ltx_path = self.ltx_path(0, min_txid, max_txid);
        let bytes = match self.host.read(Path::new(&ltx_path)) {
            Ok(b) => b,
            Err(e) => {
                return Err(Error::Ltx(Box::new(new_ltx_error(
                    "open",
                    &ltx_path,
                    0,
                    min_txid.0,
                    max_txid.0,
                    e.into(),
                ))));
            }
        };

        let decoded = match ltx::decode_file(&bytes) {
            Ok(d) => d,
            Err(_e) => {
                // Decode/verify failure indicates corruption (db.go:417-419).
                return Err(Error::Ltx(Box::new(new_ltx_error(
                    "verify",
                    &ltx_path,
                    0,
                    min_txid.0,
                    max_txid.0,
                    Error::LTXCorrupted,
                ))));
            }
        };

        let pos = Pos::new(decoded.header.max_txid, decoded.trailer.post_apply_checksum);
        self.pos_cache = Some(pos);
        Ok(pos)
    }

    /// Clears the cached position so the next `pos()` recomputes from disk.
    /// Ported from `invalidatePosCache` (db.go:430-434).
    fn invalidate_pos_cache(&mut self) {
        self.pos_cache = None;
    }

    /// Removes local LTX files, forcing a fresh snapshot on the next sync.
    /// Ported from `DB.ResetLocalState` (db.go:309-328).
    pub fn reset_local_state(&mut self) -> Result<()> {
        match self.host.remove_dir_all(Path::new(&self.ltx_dir())) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        self.max_l0_file_info = None;
        self.invalidate_pos_cache();
        Ok(())
    }

    /// Clears the local L0 directory and seeds it with a single baseline L0 LTX
    /// file (`data` for the `min_txid`..`max_txid` range), atomically. The next
    /// [`Db::sync`] then sees the baseline does not match the real WAL and writes
    /// a fresh snapshot at the current database state.
    ///
    /// This is the file-writing tail of `checkDatabaseBehindReplica`
    /// (db.go:1241-1293): clear L0, invalidate the pos cache, write the fetched
    /// remote L0 file to its local path via a temp-file + fsync + rename, then
    /// invalidate the cache again. The *detection* half (compare DB pos vs replica
    /// pos and fetch the bytes) lives on [`crate::replica::Replica`] because the
    /// synchronous `Db` has no `ReplicaClient` handle of its own. Used by
    /// [`crate::replica::Replica::check_database_behind_replica`] (issue #781).
    pub fn seed_l0_baseline(&mut self, min_txid: TXID, max_txid: TXID, data: &[u8]) -> Result<()> {
        // Clear local L0 files (db.go:1241-1249).
        let l0_dir = self.ltx_level_dir(0);
        match self.host.remove_dir_all(Path::new(&l0_dir)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        self.max_l0_file_info = None;
        self.invalidate_pos_cache();
        self.host.create_dir_all(Path::new(&l0_dir))?;

        // Write the baseline file atomically (db.go:1260-1286).
        let local_path = self.ltx_path(0, min_txid, max_txid);
        let tmp_path = format!("{local_path}.tmp");
        let _ = write_file_atomic(&self.host, &tmp_path, &local_path, data)?;
        self.last_l0_header = None;
        self.invalidate_pos_cache();
        Ok(())
    }

    /// Continues a chain at `txid`, whose database has `commit` pages: the
    /// next sync captures a delta at `txid + 1`, not the whole database. A
    /// zero-page L0 at `txid` is the local baseline verify reads back; its
    /// WAL fields name the start of the WAL as it stands, so the first
    /// capture begins at the WAL header. The WAL must exist: an activation
    /// writes the control tables before this runs. Returns the baseline's
    /// bytes: the caller uploads the same object as the epoch's marker, so
    /// the epoch is never empty (CelldPersistencePaged.tla, `BreakNoMarker`).
    /// A whole-database opener is what a paged cell cannot afford: on the
    /// 2 GB fleet whale it took minutes through the fault path, the
    /// durability gate timed out, and every re-activation started it over.
    pub fn seed_continuation(&mut self, txid: TXID, commit: u32) -> Result<Vec<u8>> {
        let wal = self.wal_header_bytes()?;
        let header = ltx::Header {
            version: ltx::VERSION,
            flags: ltx::HEADER_FLAG_NO_CHECKSUM,
            page_size: self.page_size,
            commit,
            min_txid: txid,
            max_txid: txid,
            timestamp: self.host.now_unix_millis(),
            pre_apply_checksum: 0,
            wal_offset: WAL_HEADER_SIZE as i64,
            wal_size: 0,
            wal_salt1: be_u32(&wal[16..]),
            wal_salt2: be_u32(&wal[20..]),
            node_id: 0,
        };
        let baseline = ltx::encode_file(&header, &[], 0)?;
        self.seed_l0_baseline(txid, txid, &baseline)?;
        Ok(baseline)
    }

    // ── Capture loop ───────────────────────────────────────────────────────

    /// Where one `sync` call's wall time went, phase by phase. The phases
    /// sum to the call's wall time minus only dispatch overhead, so a
    /// capture ledger built on this closes its books. `checkpoint_us` is
    /// SQLite's own checkpoint (its writes and fsyncs run through the
    /// SQLite VFS, invisible to the LTX host filesystem), measured here
    /// because no other layer can see it.
    pub fn last_sync_timing(&self) -> SyncTiming {
        self.last_sync_timing
    }

    /// Copies pending data from the WAL into the next L0 LTX file and applies
    /// the checkpoint policy.
    ///
    /// Ported from `DB.Sync` (db.go:994-1056). The public entry point of the
    /// capture loop. This function is synchronous as described in the module docs.
    pub fn sync(&mut self) -> Result<()> {
        self.sync_with_verify_hook(None)
    }

    /// `sync` with an optional hook that runs between `verify` and the
    /// capture read — the window a concurrent WAL change would land in.
    /// Test-only through `internal::sync_with_verify_hook`; production
    /// passes `None`.
    fn sync_with_verify_hook(&mut self, hook: Option<Box<dyn FnOnce()>>) -> Result<()> {
        self.last_sync_timing = SyncTiming::default();
        let phase = crate::host::telemetry_us();
        // Self-heal: recreate the control tables if something swept them out
        // of `sqlite_schema` from under the replicator — without them every
        // capture fails until the database is reopened. A no-op when the
        // tables exist (no schema change, no WAL write).
        self.ensure_control_tables()?;
        let schema_done = crate::host::telemetry_us();
        self.last_sync_timing.schema_check_us = schema_done.saturating_sub(phase);

        // Ensure the WAL has at least one frame (db.go:1017-1020).
        self.ensure_wal_exists()?;
        self.last_sync_timing.wal_exists_us =
            crate::host::telemetry_us().saturating_sub(schema_done);
        self.last_sync_timing.prepare_us = crate::host::telemetry_us().saturating_sub(phase);

        let (orig_wal_size, new_wal_size, synced) = self.verify_and_sync(hook)?;

        // Track that data was synced for time-based checkpoint decisions.
        if synced {
            self.synced_since_checkpoint = true;
        }

        let phase = crate::host::telemetry_us();
        self.checkpoint_if_needed(orig_wal_size, new_wal_size)?;
        self.last_sync_timing.checkpoint_us = crate::host::telemetry_us().saturating_sub(phase);

        // Recompute the cached position (kept for parity with db.go:1037-1041).
        let _ = self.pos()?;

        Ok(())
    }

    /// Verifies the last sync against the current WAL, then syncs.
    ///
    /// Ported from `DB.verifyAndSync` (db.go:1058-1090). Returns
    /// `(orig_wal_size, new_wal_size, synced)` where the sizes are the **logical**
    /// WAL offset (`WALOffset+WALSize` of the last LTX), not file size (#997).
    fn verify_and_sync(&mut self, between: Option<Box<dyn FnOnce()>>) -> Result<(i64, i64, bool)> {
        // Use the last synced WAL offset as the logical size for checkpoint
        // decisions; on the first sync fall back to file size (db.go:1062-1069).
        let mut orig_wal_size = self.last_synced_wal_offset;
        if orig_wal_size == 0 {
            orig_wal_size = self.wal_file_size()?;
        }

        let phase = crate::host::telemetry_us();
        let info = self.verify()?;
        self.last_sync_timing.verify_us = crate::host::telemetry_us().saturating_sub(phase);
        if let Some(between) = between {
            between();
        }
        let phase = crate::host::telemetry_us();
        let synced = self.sync_inner(info)?;
        self.last_sync_timing.encode_write_us = crate::host::telemetry_us()
            .saturating_sub(phase)
            .saturating_sub(self.last_sync_timing.fsync_us);

        let new_wal_size = self.last_synced_wal_offset;
        Ok((orig_wal_size, new_wal_size, synced))
    }

    /// Ensures the LTX state matches where it left off from the real WAL.
    ///
    /// Ported branch-for-branch from `DB.verify` (db.go:1296-1436).
    /// This is the snapshot-on-continuity-break brain — do not refactor for
    /// elegance on pass one.
    fn verify(&mut self) -> Result<SyncInfo> {
        let frame_size = self.page_size as i64 + WAL_FRAME_HEADER_SIZE as i64;
        let mut info = SyncInfo {
            snapshotting: true,
            ..Default::default()
        };

        let pos = self.pos()?;
        if pos.txid == TXID(0) {
            info.offset = WAL_HEADER_SIZE as i64;
            return Ok(info); // first sync
        }

        // Determine the last WAL offset we saved from: the header of the
        // last L0 (db.go:1311-1326). This instance wrote that header one
        // sync ago, so the cache answers without re-reading the file; a
        // key miss (restore, compaction, reopen) reads and parses as the
        // port always did.
        let cache_hit = matches!(&self.last_l0_header, Some((txid, _)) if *txid == pos.txid);
        let (hdr, ltx_bytes) = if cache_hit {
            let (_, cached) = self.last_l0_header.as_ref().expect("matched above");
            (cached.clone(), None)
        } else {
            let ltx_path = self.ltx_path(0, pos.txid, pos.txid);
            let ltx_bytes = match self.host.read(Path::new(&ltx_path)) {
                Ok(b) => b,
                Err(e) => {
                    return Err(Error::Ltx(Box::new(new_ltx_error(
                        "open",
                        &ltx_path,
                        0,
                        pos.txid.0,
                        pos.txid.0,
                        e.into(),
                    ))));
                }
            };
            let parsed = match ltx::Header::parse(&ltx_bytes) {
                Ok(h) => h,
                Err(_) => {
                    return Err(Error::Ltx(Box::new(new_ltx_error(
                        "decode",
                        &ltx_path,
                        0,
                        pos.txid.0,
                        pos.txid.0,
                        Error::LTXCorrupted,
                    ))));
                }
            };
            (
                LastL0Header {
                    wal_offset: parsed.wal_offset,
                    wal_size: parsed.wal_size,
                    wal_salt1: parsed.wal_salt1,
                    wal_salt2: parsed.wal_salt2,
                    commit: parsed.commit,
                    final_pgno: 0,
                    final_page: Vec::new(),
                },
                Some(ltx_bytes),
            )
        };
        info.offset = hdr.wal_offset + hdr.wal_size;
        info.salt1 = hdr.wal_salt1;
        info.salt2 = hdr.wal_salt2;
        info.prev_commit = hdr.commit;

        // If the LTX WAL offset exceeds the real WAL size, the WAL was truncated.
        let wal_size = self.wal_file_size()?;
        if info.offset > wal_size {
            // If we previously synced to the exact WAL end, this truncation is an
            // expected checkpoint: reset to the header and continue incrementally
            // rather than snapshotting (issue #927, db.go:1335-1355).
            if self.synced_to_wal_end {
                self.synced_to_wal_end = false;

                let wal_hdr = self.wal_header_bytes()?;
                info.offset = WAL_HEADER_SIZE as i64;
                info.salt1 = be_u32(&wal_hdr[16..]);
                info.salt2 = be_u32(&wal_hdr[20..]);
                info.snapshotting = false;
                info.reason = String::new();
                return Ok(info);
            }

            info.reason = "wal truncated by another process".to_string();
            return Ok(info);
        }

        // Compare WAL headers; restart from the beginning of the WAL if different.
        let wal_hdr = self.wal_header_bytes()?;
        let salt1 = be_u32(&wal_hdr[16..]);
        let salt2 = be_u32(&wal_hdr[20..]);
        let salt_match = salt1 == hdr.wal_salt1 && salt2 == hdr.wal_salt2;

        // Edge case: LTX represents the start of the WAL (WALOffset=32, WALSize=0).
        // Handle this before computing prev_wal_offset to avoid underflow
        // (32 - 4120 = -4088). See issue #900 (db.go:1375-1383).
        if info.offset == WAL_HEADER_SIZE as i64 {
            if salt_match {
                info.snapshotting = false;
                return Ok(info);
            }
            info.reason = "wal header salt reset, snapshotting".to_string();
            return Ok(info);
        }

        // If the offset is at the start of the first page, we can't check the
        // previous page (db.go:1386-1399).
        let prev_wal_offset = info.offset - frame_size;
        if prev_wal_offset == WAL_HEADER_SIZE as i64 {
            if salt_match {
                info.snapshotting = false;
                return Ok(info);
            }
            info.reason = "wal header salt reset, snapshotting".to_string();
            return Ok(info);
        } else if prev_wal_offset < WAL_HEADER_SIZE as i64 {
            return Err(Error::Other(
                format!("prev WAL offset is less than the header size: {prev_wal_offset}").into(),
            ));
        }

        // If we can't verify the last page is in the last LTX file, snapshot.
        let last_page_match = match &ltx_bytes {
            Some(bytes) => self.last_page_match(bytes, &hdr, prev_wal_offset, frame_size)?,
            None => self.last_page_match_cached(&hdr, prev_wal_offset, frame_size)?,
        };
        if !last_page_match {
            info.reason =
                "last page does not exist in last ltx file, wal overwritten by another process"
                    .to_string();
            return Ok(info);
        }

        // Salt changed (possible FULL/RESTART checkpoint). With a last-page match
        // we assume the WAL was not overwritten (db.go:1412-1431).
        if !salt_match {
            info.offset = WAL_HEADER_SIZE as i64;
            info.salt1 = salt1;
            info.salt2 = salt2;

            let detected =
                self.detect_full_checkpoint(&[(salt1, salt2), (hdr.wal_salt1, hdr.wal_salt2)])?;
            if detected {
                info.reason = "full or restart checkpoint detected, snapshotting".to_string();
            } else {
                info.snapshotting = false;
            }
            return Ok(info);
        }

        info.snapshotting = false;
        Ok(info)
    }

    /// Checks whether the last page read in the WAL exists in the last LTX file.
    ///
    /// Ported from `DB.lastPageMatch` (db.go:1438-1475). Re-reads the last synced
    /// WAL frame and searches the last LTX file's pages for a matching
    /// `(pgno, data)` pair.
    fn last_page_match(
        &mut self,
        ltx_bytes: &[u8],
        hdr: &LastL0Header,
        prev_wal_offset: i64,
        frame_size: i64,
    ) -> Result<bool> {
        if prev_wal_offset <= WAL_HEADER_SIZE as i64 {
            return Ok(false);
        }

        let frame = self.wal_bytes_at(prev_wal_offset, frame_size)?;
        let pgno = be_u32(&frame[0..]);
        let fsalt1 = be_u32(&frame[8..]);
        let fsalt2 = be_u32(&frame[12..]);
        let data = &frame[WAL_FRAME_HEADER_SIZE..];

        if fsalt1 != hdr.wal_salt1 || fsalt2 != hdr.wal_salt2 {
            return Ok(false);
        }

        // Verify the last WAL page exists, byte-for-byte, in the last LTX file.
        // Decode the full file and compare the decompressed page bytes.
        let pages = ltx::decode_file_pages(ltx_bytes).map_err(|_| Error::LTXCorrupted)?;
        for (p, page_data) in &pages {
            if *p == pgno && page_data.as_slice() == data {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The cache-backed page check: compares the WAL frame at
    /// `prev_wal_offset` against the final frame the previous sync
    /// consumed. Equivalent to the ported search when the WAL is
    /// unchanged (the L0's entry for that page IS that frame), and
    /// strictly more conservative when it is not — a mismatch forces a
    /// snapshot, never an incremental read over rewritten history.
    fn last_page_match_cached(
        &mut self,
        hdr: &LastL0Header,
        prev_wal_offset: i64,
        frame_size: i64,
    ) -> Result<bool> {
        if prev_wal_offset <= WAL_HEADER_SIZE as i64 || hdr.final_page.is_empty() {
            return Ok(false);
        }
        let frame = self.wal_bytes_at(prev_wal_offset, frame_size)?;
        let pgno = be_u32(&frame[0..]);
        let fsalt1 = be_u32(&frame[8..]);
        let fsalt2 = be_u32(&frame[12..]);
        let data = &frame[WAL_FRAME_HEADER_SIZE..];
        Ok(fsalt1 == hdr.wal_salt1
            && fsalt2 == hdr.wal_salt2
            && pgno == hdr.final_pgno
            && data == hdr.final_page.as_slice())
    }

    /// Detects whether a FULL or RESTART checkpoint occurred (we may have missed
    /// frames). Ported from `DB.detectFullCheckpoint` (db.go:1477-1507).
    fn detect_full_checkpoint(&self, known_salts: &[(u32, u32)]) -> Result<bool> {
        let wal_bytes = self.host.read(&self.wal_path())?;
        let rd = WalReader::new(&wal_bytes).map_err(Error::from)?;
        let last_known = known_salts.last().copied().unwrap_or((0, 0));
        let mut m = rd.frame_salts_until(last_known);
        for s in known_salts {
            m.remove(s);
        }
        Ok(!m.is_empty())
    }

    /// Read an incremental WAL image through the first invalid frame.
    ///
    /// SQLite can retain stale frames after a checkpoint restarts the logical
    /// WAL. The frame checksum chain is the format's end marker, so grow a
    /// prefix geometrically and stop when `WalReader` stops before the bytes
    /// that were read. The geometric window keeps parsing linear overall and
    /// reads at most one prior window past the valid end. The returned byte
    /// count includes that discarded probe for an honest I/O ledger.
    fn read_valid_wal_image(&mut self, info: &SyncInfo, start: usize) -> Result<(WalImage, usize)> {
        let frame_size = self.page_size as usize + WAL_FRAME_HEADER_SIZE;
        let offset = info.offset;
        let salt1 = info.salt1;
        let salt2 = info.salt2;
        Ok(self.with_wal_file(|file| {
            let file_len = file.file_len()? as usize;
            if file_len < WAL_HEADER_SIZE || start < WAL_HEADER_SIZE || start >= file_len {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
            }
            let complete_end =
                WAL_HEADER_SIZE + ((file_len - WAL_HEADER_SIZE) / frame_size) * frame_size;
            if start >= complete_end || !(start - WAL_HEADER_SIZE).is_multiple_of(frame_size) {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
            }

            let tail_base = if start == WAL_HEADER_SIZE { 0 } else { start };
            let mut bytes = file.read_exact_at(0, WAL_HEADER_SIZE)?;
            let mut read_bytes = bytes.len();
            let mut cursor = start;
            let mut target_frames = 1_usize;
            loop {
                let target_end = start
                    .saturating_add(target_frames.saturating_mul(frame_size))
                    .min(complete_end);
                if target_end > cursor {
                    let chunk = file.read_exact_at(cursor as u64, target_end - cursor)?;
                    read_bytes += chunk.len();
                    bytes.extend_from_slice(&chunk);
                    cursor = target_end;
                }

                let valid_end = {
                    let parsed = if offset == WAL_HEADER_SIZE as i64 {
                        WalReader::new(&bytes)
                    } else if tail_base == 0 {
                        WalReader::new_with_offset(&bytes, offset, salt1, salt2)
                    } else {
                        WalReader::new_with_offset_over_tail(
                            &bytes,
                            tail_base as i64,
                            offset,
                            salt1,
                            salt2,
                        )
                    };
                    let mut reader = parsed.map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                    })?;
                    reader.page_map().map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                    })?;
                    if reader.offset() == 0 {
                        WAL_HEADER_SIZE
                    } else {
                        reader.offset() as usize + frame_size
                    }
                };

                if valid_end < cursor {
                    let keep = if tail_base == 0 {
                        valid_end
                    } else {
                        WAL_HEADER_SIZE + valid_end.saturating_sub(tail_base)
                    };
                    bytes.truncate(keep);
                    return Ok((
                        WalImage {
                            bytes,
                            tail_base,
                            file_len,
                        },
                        read_bytes,
                    ));
                }
                if cursor == complete_end {
                    return Ok((
                        WalImage {
                            bytes,
                            tail_base,
                            file_len,
                        },
                        read_bytes,
                    ));
                }
                target_frames = target_frames.saturating_mul(2);
            }
        })?)
    }

    /// Copies pending bytes from the real WAL into a new L0 LTX file.
    ///
    /// Ported from `DB.sync` (db.go:1517-1723). Returns `true` if an LTX file was
    /// written (there were new pages or we were snapshotting). Atomic
    /// tmp→fsync→rename with the pos cache + anti-feedback flags updated after.
    fn sync_inner(&mut self, mut info: SyncInfo) -> Result<bool> {
        let phase = crate::host::telemetry_us();
        // A capture that starts at the WAL header reads a logical WAL with no
        // backfilled prefix: the first sync, a restart, or a boundary image.
        // The checkpoint trigger counts from the backfilled boundary, so it
        // must not carry an offset from the WAL that just ended, or the new
        // WAL would grow past that offset before its first checkpoint.
        if info.offset == WAL_HEADER_SIZE as i64 {
            self.checkpointed_wal_offset = WAL_HEADER_SIZE as i64;
        }
        let pos = self.pos()?;
        let tx_id = TXID(pos.txid.0 + 1);
        let filename = self.ltx_path(0, tx_id, tx_id);

        let db_size = self.db_file_size()?;
        let mut commit = (db_size / self.page_size as i64) as u32;
        self.last_db_pages = commit;
        self.last_sync_timing.pos_us = crate::host::telemetry_us().saturating_sub(phase);
        let phase = crate::host::telemetry_us();

        // The incremental path reads only the valid checksum chain: the
        // 32-byte WAL header, the previous frame when there is one, and the
        // frames from `info.offset` on. A passive or restart checkpoint can
        // leave a large physical suffix after SQLite restarts the logical WAL
        // at its beginning. Reading to the physical file length made every
        // Queue capture re-read that stale suffix because Queue cells cannot
        // use size-triggered TRUNCATE checkpoints. The progressive reader
        // doubles its valid-prefix window, then drops the bounded look-ahead
        // after the first salt or checksum failure. The sparse image keeps
        // every offset absolute, so the reader and page collectors remain
        // unchanged. An I/O race or a previous-frame mismatch falls back to
        // the full read the port always did.
        let frame_size_bytes = self.page_size as i64 + WAL_FRAME_HEADER_SIZE as i64;
        let mut wal = if info.snapshotting {
            self.last_sync_timing.wal_read_kind = 1;
            let bytes = self.host.read(&self.wal_path())?;
            self.last_sync_timing.wal_read_bytes = bytes.len() as u64;
            self.last_sync_timing.wal_len_bytes = bytes.len() as u64;
            WalImage::whole(bytes)
        } else {
            let start = if info.offset <= WAL_HEADER_SIZE as i64 + frame_size_bytes {
                WAL_HEADER_SIZE
            } else {
                (info.offset - frame_size_bytes) as usize
            };
            match self.read_valid_wal_image(&info, start) {
                Ok((image, read_bytes)) => {
                    self.last_sync_timing.wal_read_kind =
                        if start == WAL_HEADER_SIZE { 2 } else { 0 };
                    self.last_sync_timing.wal_read_bytes = read_bytes as u64;
                    self.last_sync_timing.wal_len_bytes = image.file_len as u64;
                    image
                }
                Err(_) => {
                    self.last_sync_timing.wal_read_kind = 3;
                    let bytes = self.host.read(&self.wal_path())?;
                    self.last_sync_timing.wal_read_bytes = bytes.len() as u64;
                    self.last_sync_timing.wal_len_bytes = bytes.len() as u64;
                    WalImage::whole(bytes)
                }
            }
        };

        self.last_sync_timing.wal_read_us = crate::host::telemetry_us().saturating_sub(phase);
        self.last_sync_timing.wal_image_bytes = wal.bytes.len() as u64;
        self.last_sync_timing.snapshot = info.snapshotting;
        self.last_sync_timing.snapshot_reason = if info.snapshotting {
            snapshot_reason_code(&info.reason)
        } else {
            0
        };
        let phase = crate::host::telemetry_us();

        // Choose the WAL reader start: from the header, or seek to info.offset.
        // A previous-frame mismatch falls back to a full read (snapshot),
        // mirroring NewWALReaderWithOffset's PrevFrameMismatchError handling
        // (db.go:1565-1581).
        // A previous-frame mismatch restarts the read from the header. The
        // sparse tail image is zero-filled below `start`, so a from-header
        // reader over it would see no valid frame and report "nothing to
        // capture" — a silent miss the ship loop would then credit. The
        // mismatch path therefore re-reads the complete WAL first, which is
        // exactly the port's former full-read behavior on this branch.
        let mismatch = !(info.offset == WAL_HEADER_SIZE as i64)
            && matches!(
                wal.reader_at(info.offset, info.salt1, info.salt2),
                Err(crate::wal::WalError::PrevFrameMismatch)
            );
        if mismatch {
            info.offset = WAL_HEADER_SIZE as i64;
            if self.last_sync_timing.wal_read_kind == 0 {
                let bytes = self.host.read(&self.wal_path())?;
                self.last_sync_timing.wal_read_kind = 3;
                self.last_sync_timing.wal_read_bytes = bytes.len() as u64;
                self.last_sync_timing.wal_len_bytes = bytes.len() as u64;
                self.last_sync_timing.wal_image_bytes = bytes.len() as u64;
                wal = WalImage::whole(bytes);
            }
        }
        let mut rd = if info.offset == WAL_HEADER_SIZE as i64 {
            WalReader::new(&wal.bytes).map_err(Error::from)?
        } else {
            wal.reader_at(info.offset, info.salt1, info.salt2)
                .map_err(Error::from)?
        };

        let (page_map, max_offset, wal_commit) = rd.page_map().map_err(Error::from)?;
        if wal_commit > 0 {
            commit = wal_commit;
        }

        let sz = if max_offset > 0 {
            max_offset - info.offset
        } else {
            0
        };
        if sz < 0 {
            return Err(Error::Other(
                format!(
                    "wal size must be positive: sz={sz}, maxOffset={max_offset}, info.offset={}",
                    info.offset
                )
                .into(),
            ));
        }

        // Exit if there are no new WAL pages and we are not snapshotting
        // (db.go:1603-1607).
        if !info.snapshotting && sz == 0 {
            return Ok(false);
        }

        let (rd_salt1, rd_salt2) = rd.salt();

        // Build the page set for the encoder.
        let pages: Vec<(u32, Vec<u8>)> = if info.snapshotting {
            self.collect_snapshot_pages(&wal, &page_map, commit)?
        } else {
            self.collect_wal_pages(&wal, &page_map, info.prev_commit, commit)?
        };
        self.last_sync_timing.map_collect_us = crate::host::telemetry_us().saturating_sub(phase);
        let phase = crate::host::telemetry_us();

        let header = ltx::Header {
            version: ltx::VERSION,
            flags: ltx::HEADER_FLAG_NO_CHECKSUM,
            page_size: self.page_size,
            commit,
            min_txid: tx_id,
            max_txid: tx_id,
            timestamp: self.host.now_unix_millis(),
            pre_apply_checksum: 0,
            wal_offset: info.offset,
            wal_size: sz,
            wal_salt1: rd_salt1,
            wal_salt2: rd_salt2,
            node_id: 0,
        };

        // Encode the LTX file (with HeaderFlagNoChecksum, so post-apply is 0).
        let encoded = ltx::encode_file(&header, &pages, 0)?;
        self.last_sync_timing.ltx_encode_us = crate::host::telemetry_us().saturating_sub(phase);
        let phase = crate::host::telemetry_us();

        // Atomic tmp → fsync → rename (db.go:1609-1685).
        let tmp_filename = format!("{filename}.tmp");
        let parent = Path::new(&tmp_filename).parent().map(Path::to_path_buf);
        if !self.l0_dir_ready {
            if let Some(parent) = &parent {
                self.host.create_dir_all(parent)?;
            }
            self.l0_dir_ready = true;
        }
        // On rename failure, clear the L0 cache + invalidate pos
        // (db.go:1680-1684); the error path below does that. A directory
        // that vanished under a ready flag is recreated once and the cut
        // retried, so the flag saves a `mkdir` per sync without trusting it.
        self.last_sync_timing.fsync_us =
            match write_file_atomic(&self.host, &tmp_filename, &filename, &encoded) {
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    if let Some(parent) = &parent {
                        self.host.create_dir_all(parent)?;
                    }
                    write_file_atomic(&self.host, &tmp_filename, &filename, &encoded)?
                }
                other => other?,
            };
        self.last_sync_timing.file_write_us = crate::host::telemetry_us()
            .saturating_sub(phase)
            .saturating_sub(self.last_sync_timing.fsync_us);
        // The next verify reads exactly these fields back; caching them —
        // plus the final consumed WAL frame for the page check — is what
        // spares it re-reading the file it just watched being written.
        let frame_size = self.page_size as i64 + WAL_FRAME_HEADER_SIZE as i64;
        let final_frame_offset = max_offset - frame_size;
        let (final_pgno, final_page) = match (final_frame_offset >= WAL_HEADER_SIZE as i64)
            .then(|| wal.slice(final_frame_offset, frame_size as usize))
            .flatten()
        {
            Some(frame) => (be_u32(&frame[0..]), frame[WAL_FRAME_HEADER_SIZE..].to_vec()),
            None => (0, Vec::new()),
        };
        self.last_l0_header = Some((
            tx_id,
            LastL0Header {
                wal_offset: info.offset,
                wal_size: sz,
                wal_salt1: rd_salt1,
                wal_salt2: rd_salt2,
                commit,
                final_pgno,
                final_page,
            },
        ));

        // Update the L0 file-info cache and the cached position (db.go:1687-1702).
        self.max_l0_file_info = Some(ltx::FileInfo {
            level: 0,
            min_txid: tx_id,
            max_txid: tx_id,
            pre_apply_checksum: 0,
            post_apply_checksum: 0,
            size: encoded.len() as i64,
            created_at: Some(system_time_from_unix_millis(self.host.now_unix_millis())),
        });
        // The encoder's post-apply pos: for a NoChecksum file the post-apply
        // checksum is 0; the position is (tx_id, 0).
        self.pos_cache = Some(Pos::new(tx_id, 0));

        // Track the logical end of WAL content for checkpoint decisions
        // (db.go:1704-1718, issues #997/#927).
        let final_offset = info.offset + sz;
        self.last_synced_wal_offset = final_offset;
        self.synced_to_wal_end = match self.wal_file_size() {
            Ok(wal_size) => final_offset == wal_size,
            Err(_) => false,
        };

        Ok(true)
    }

    /// Collects the page set for an incremental sync, in ascending page-number
    /// order. Pages absent from the WAL growth range come from the database file
    /// (Litestream #1292 / celld #150).
    fn collect_wal_pages(
        &self,
        wal: &WalImage,
        page_map: &HashMap<u32, i64>,
        prev_commit: u32,
        commit: u32,
    ) -> Result<Vec<(u32, Vec<u8>)>> {
        let mut pgnos: Vec<u32> = page_map.keys().copied().collect();
        let lock = lock_pgno(self.page_size);
        if commit > prev_commit {
            for pgno in (prev_commit + 1)..=commit {
                if pgno != lock && !page_map.contains_key(&pgno) {
                    pgnos.push(pgno);
                }
            }
        }
        pgnos.sort_unstable();

        let mut out = Vec::with_capacity(pgnos.len());
        for pgno in pgnos {
            let data = match page_map.get(&pgno) {
                Some(&offset) => wal.page(offset, self.page_size)?,
                None => self.read_db_page(pgno)?,
            };
            out.push((pgno, data));
        }
        Ok(out)
    }

    /// Collects the full page set for a snapshot: every page `1..=commit`
    /// (skipping the lock page), reading from the WAL where present, else the DB
    /// file. Ported from `DB.writeLTXFromDB` (db.go:1725-1770).
    fn collect_snapshot_pages(
        &self,
        wal: &WalImage,
        page_map: &HashMap<u32, i64>,
        commit: u32,
    ) -> Result<Vec<(u32, Vec<u8>)>> {
        let lock = lock_pgno(self.page_size);
        let mut out = Vec::with_capacity(commit as usize);
        for pgno in (1..=commit).filter(|pgno| *pgno != lock) {
            let data = match page_map.get(&pgno) {
                Some(&offset) => wal.page(offset, self.page_size)?,
                None => self.read_db_page(pgno)?,
            };
            out.push((pgno, data));
        }
        Ok(out)
    }

    /// Reads one page of the main database through the connection's VFS file,
    /// never through a host read of the path. A paged cell's file is a sparse
    /// cache of its cut, and the host read returned every unfaulted page as
    /// zeros: each paged epoch's opener snapshot shipped holes, and the next
    /// epoch restored a database whose integrity_check named every overflow
    /// chain (fleet, 2026-09-02). The VFS faults the page in instead.
    fn read_db_page(&self, pgno: u32) -> Result<Vec<u8>> {
        let mut file: *mut ffi::sqlite3_file = std::ptr::null_mut();
        // SAFETY: `conn` is open, and FILE_POINTER hands out the main file,
        // which lives as long as the connection.
        let rc = unsafe {
            ffi::sqlite3_file_control(
                self.conn.handle(),
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_FILE_POINTER,
                (&mut file as *mut *mut ffi::sqlite3_file).cast(),
            )
        };
        if rc != ffi::SQLITE_OK || file.is_null() {
            return Err(std::io::Error::other("no main database file").into());
        }
        let n = self.page_size as usize;
        let mut page = vec![0u8; n];
        let offset = i64::from(pgno - 1) * i64::from(self.page_size);
        // SAFETY: `file` is the live main file and `page` holds `n` bytes.
        let rc = unsafe {
            let read = (*(*file).pMethods).xRead.expect("xRead");
            read(file, page.as_mut_ptr().cast(), n as c_int, offset)
        };
        if rc != ffi::SQLITE_OK {
            return Err(std::io::Error::other(format!("read page {pgno}: sqlite rc {rc}")).into());
        }
        Ok(page)
    }

    // ── Checkpointing ──────────────────────────────────────────────────────

    /// Performs a checkpoint based on the configured thresholds (3-tier policy).
    ///
    /// Ported from `DB.checkpointIfNeeded` (db.go:1092-1156). Checks
    /// in priority order: TruncatePageN (TRUNCATE, blocking) → MinCheckpointPageN
    /// (PASSIVE) → CheckpointInterval (PASSIVE, gated on `synced_since_checkpoint`).
    fn checkpoint_if_needed(&mut self, orig_wal_size: i64, new_wal_size: i64) -> Result<()> {
        if self.page_size == 0 {
            return Ok(());
        }

        // Priority 1: emergency TRUNCATE (blocking) on the *original* logical
        // size. A truncate ends in a boundary image of the whole database
        // (see `checkpoint`). For a small database that image is the cheap
        // price the threshold was tuned for, so below `RELATIVE_TRUNCATE_PAGES`
        // the threshold is absolute, as upstream's. Above it the WAL must also
        // have grown past the database before a truncate: the image then costs
        // at most what the WAL it replaces did, and the chain stays within 2x
        // of the writes. A fixed threshold made a 1MB-row whale pay a
        // database-sized capture every write, and its chain grew as the
        // square of its size.
        if self.truncate_page_n > 0 {
            let relative = if self.last_db_pages > RELATIVE_TRUNCATE_PAGES {
                self.last_db_pages
            } else {
                0
            };
            let threshold = self.truncate_page_n.max(relative);
            if orig_wal_size >= calc_wal_size(self.page_size, threshold) {
                return self.checkpoint(CheckpointMode::Truncate);
            }
        }

        // Priority 2: PASSIVE once the frames appended since the last
        // backfill reach the threshold. See `checkpointed_wal_offset` for why
        // this is not the whole logical size: a checkpoint whose sealing
        // write could not restart the WAL must cost one retry at the next
        // threshold, not a checkpoint per sync.
        let backfilled_through = self.checkpointed_wal_offset.clamp(
            WAL_HEADER_SIZE as i64,
            new_wal_size.max(WAL_HEADER_SIZE as i64),
        );
        let threshold =
            calc_wal_size(self.page_size, self.min_checkpoint_page_n) - WAL_HEADER_SIZE as i64;
        if new_wal_size - backfilled_through >= threshold {
            return self.checkpoint_passive_swallowing_busy();
        }

        // Priority 3: time-based PASSIVE, gated on data synced since last
        // checkpoint (#896). Uses the DB-file mtime and a logical-size guard so an
        // idle DB does not spin LTX files (db.go:1133-1153).
        if self.checkpoint_interval > Duration::ZERO && self.synced_since_checkpoint {
            let elapsed = self.host.file_age(&self.path)?;
            if elapsed > self.checkpoint_interval && new_wal_size > calc_wal_size(self.page_size, 1)
            {
                return self.checkpoint_passive_swallowing_busy();
            }
        }

        Ok(())
    }

    /// PASSIVE checkpoint that swallows SQLITE_BUSY (log-and-continue, the one
    /// sanctioned best-effort case, db.go:1118-1124).
    fn checkpoint_passive_swallowing_busy(&mut self) -> Result<()> {
        match self.checkpoint(CheckpointMode::Passive) {
            Ok(()) => Ok(()),
            Err(e) if is_sqlite_busy_error(&e) => {
                self.last_sync_timing.checkpoint_runs = 1;
                self.last_sync_timing.checkpoint_busy_errors = 1;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Performs a checkpoint on the WAL.
    ///
    /// Ported from `DB.Checkpoint`/`DB.checkpoint` (db.go:1801-1873).
    /// A passive checkpoint seals the WAL under a short writer barrier before
    /// it runs. If any checkpoint restarts the WAL, a post-checkpoint write lock
    /// protects either an incremental sync or a full boundary snapshot.
    ///
    /// The upstream `chkMu.TryLock` snapshot-vs-checkpoint gate is unnecessary in
    /// the synchronous `Db`. `&mut self` serializes `sync`, `checkpoint`, and
    /// `snapshot`, and one blocking thread owns the `Db`.
    pub fn checkpoint(&mut self, mode: CheckpointMode) -> Result<()> {
        self.checkpoint_with_passive_hooks(mode, None, None, None)
    }

    fn checkpoint_with_passive_hooks(
        &mut self,
        mode: CheckpointMode,
        passive_hook: Option<Box<dyn FnOnce() + Send>>,
        passive_unlocked_hook: Option<Box<dyn FnOnce() + Send>>,
        post_barrier_hook: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<()> {
        // Self-heal, as in `sync`: `checkpoint` writes to both control tables
        // and re-acquires the read lock through `_litestream_seq`, and the
        // invariant is that the tables exist before any control-table
        // statement — not only before a capture.
        self.ensure_control_tables()?;

        // Read the WAL header before the checkpoint to detect a restart.
        let hdr = self.wal_header_bytes()?;

        // Copy the end of the WAL before the checkpoint to capture as much as
        // possible (db.go:1823-1826).
        self.verify_and_sync(None)?;

        let frame_size = self.page_size as i64 + WAL_FRAME_HEADER_SIZE as i64;
        let pre_checkpoint_frame_n = if self.last_synced_wal_offset > WAL_HEADER_SIZE as i64 {
            (self.last_synced_wal_offset - WAL_HEADER_SIZE as i64) / frame_size
        } else {
            0
        };

        // A passive checkpoint does not acquire SQLite's writer lock. Hold a
        // short write transaction on the dedicated read-lock connection, then
        // sync again to seal every commit before running the checkpoint on the
        // main connection. Keep the barrier until the checkpoint completes.
        let pragma = if mode == CheckpointMode::Passive {
            self.exec_passive_checkpoint_with_barrier(hdr, passive_hook, passive_unlocked_hook)?
        } else {
            self.exec_checkpoint(mode)?
        };
        self.last_sync_timing.checkpoint_runs = 1;
        self.last_sync_timing.checkpoint_wal_frames = pragma.wal_frames.max(0) as u64;
        self.last_sync_timing.checkpoint_backfilled = pragma.backfilled.max(0) as u64;
        self.last_sync_timing.checkpoint_busy = u64::from(pragma.busy != 0);
        // The backfilled boundary in this WAL's coordinates. A short backfill
        // (a reader pinned the WAL) leaves the remainder counting toward the
        // next threshold, so a pinned WAL retries at the threshold and an
        // unpinned one does not retry at all.
        self.checkpointed_wal_offset =
            WAL_HEADER_SIZE as i64 + pragma.backfilled.max(0) * frame_size;
        if let Some(hook) = post_barrier_hook {
            hook();
        }

        // Force a write so a restarted WAL has a new header and at least one
        // frame that verify can read.
        self.conn
            .execute_batch(
                "INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) \
                 ON CONFLICT (id) DO UPDATE SET seq = seq + 1",
            )
            .map_err(sql_err)?;

        // If the WAL header is unchanged, the WAL did not restart — done.
        let other = self.wal_header_bytes()?;
        if hdr == other {
            self.synced_since_checkpoint = false;
            return Ok(());
        }
        self.last_sync_timing.checkpoint_restarts = 1;

        // The WAL restarted. Grab the write lock, then either copy the new WAL
        // tail or take a complete boundary image. TRUNCATE always needs the
        // boundary image because SQLite reports zero frames after resetting the
        // WAL. A forced checkpoint also needs one if it covered more frames than
        // the sealed pre-checkpoint sync observed.
        self.conn
            .prepare_cached("BEGIN")
            .and_then(|mut statement| statement.execute([]))
            .map_err(sql_err)?;
        let post = (|| -> Result<()> {
            self.conn
                .prepare_cached("INSERT INTO _litestream_lock (id) VALUES (1)")
                .and_then(|mut statement| statement.execute([]))
                .map_err(sql_err)?;
            if mode == CheckpointMode::Truncate
                || (mode != CheckpointMode::Passive && pragma.wal_frames > pre_checkpoint_frame_n)
            {
                let info = SyncInfo {
                    offset: WAL_HEADER_SIZE as i64,
                    salt1: be_u32(&other[16..]),
                    salt2: be_u32(&other[20..]),
                    snapshotting: true,
                    reason: "checkpoint boundary snapshot".to_string(),
                    ..Default::default()
                };
                self.sync_inner(info)?;
            } else {
                self.verify_and_sync(None)?;
            }
            Ok(())
        })();
        // Always roll back the write transaction (db.go:1849,1867).
        let rb = rollback(&self.conn);
        post?;
        rb?;

        self.synced_since_checkpoint = false;
        Ok(())
    }

    /// Seals and runs a passive checkpoint while holding SQLite's writer lock.
    ///
    /// The long-lived read transaction uses `rtx_conn`, so this method releases
    /// that read lock and temporarily reuses the same connection for the writer
    /// barrier. The checkpoint itself runs through `conn`, as SQLite does not
    /// permit a checkpoint on the connection that owns the write transaction.
    fn exec_passive_checkpoint_with_barrier(
        &mut self,
        pre_checkpoint_header: [u8; WAL_HEADER_SIZE],
        hook: Option<Box<dyn FnOnce() + Send>>,
        unlocked_hook: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<CheckpointPragma> {
        self.release_read_lock()?;
        if let Some(hook) = unlocked_hook {
            hook();
        }

        let result = (|| -> Result<CheckpointPragma> {
            self.rtx_conn
                .prepare_cached("BEGIN")
                .and_then(|mut statement| statement.execute([]))
                .map_err(sql_err)?;
            self.rtx_conn
                .prepare_cached("INSERT INTO _litestream_lock (id) VALUES (1)")
                .and_then(|mut statement| statement.execute([]))
                .map_err(sql_err)?;

            // Writers can cross their own autocheckpoint threshold after the
            // read lock is released but before this barrier wins SQLite's
            // writer lock. A changed WAL header means some of those commits
            // can already live only in the database file. The normal
            // synced-to-end shortcut cannot distinguish that race from our own
            // completed checkpoint, so an incremental LTX can omit the
            // checkpointed pages and produce a malformed restore. Seal the
            // complete boundary while the writer lock makes the database file
            // and the new WAL tail a stable pair. The cost is one database-size
            // LTX file only when another checkpoint wins this narrow gap.
            let barrier_header = self.wal_header_bytes()?;
            if barrier_header != pre_checkpoint_header {
                let info = SyncInfo {
                    offset: WAL_HEADER_SIZE as i64,
                    salt1: be_u32(&barrier_header[16..]),
                    salt2: be_u32(&barrier_header[20..]),
                    snapshotting: true,
                    reason: "WAL restarted before passive checkpoint barrier".to_string(),
                    ..Default::default()
                };
                self.sync_inner(info)?;
            } else {
                // Commits can land between the earlier sync and acquisition of
                // the barrier. This second sync seals them before the
                // checkpoint.
                self.verify_and_sync(None)?;
            }
            if let Some(hook) = hook {
                hook();
            }
            self.run_checkpoint_pragma(CheckpointMode::Passive)
        })();

        // Release the writer barrier before restoring the long-lived read lock.
        // Preserve the operation error if both the operation and cleanup fail.
        let rollback_result = rollback(&self.rtx_conn);
        let reacquire_result = self.acquire_read_lock();
        match result {
            Err(error) => Err(error),
            Ok(pragma) => {
                rollback_result?;
                reacquire_result?;
                Ok(pragma)
            }
        }
    }

    /// Releases the read lock, runs `PRAGMA wal_checkpoint(<mode>)`, and
    /// re-acquires the read lock — re-acquiring even on error.
    ///
    /// Ported from `DB.execCheckpoint` (db.go:1875-1919). The exact
    /// release→checkpoint→re-acquire sequence is load-bearing.
    fn exec_checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointPragma> {
        // Ensure the read lock is removed before the checkpoint; defer the
        // re-acquire so it runs even on early return.
        self.release_read_lock()?;

        let result = self.run_checkpoint_pragma(mode);

        // Re-acquire the read lock immediately after the checkpoint (the deferred
        // re-acquire in Go). If the pragma succeeded, propagate any re-acquire
        // error; otherwise surface the original pragma error.
        let reacquire = self.acquire_read_lock();
        match (result, reacquire) {
            (Ok(pragma), Ok(())) => Ok(pragma),
            (Ok(_), Err(e)) => Err(e),
            (Err(e), _) => Err(e),
        }
    }

    /// Runs the raw `PRAGMA wal_checkpoint(<mode>)` and reads its 3-int result.
    fn run_checkpoint_pragma(&self, mode: CheckpointMode) -> Result<CheckpointPragma> {
        let sql = format!("PRAGMA wal_checkpoint({mode})");
        self.conn
            .prepare_cached(&sql)
            .map_err(sql_err)?
            .query_row([], |row| {
                Ok(CheckpointPragma {
                    busy: row.get::<_, i64>(0)?,
                    wal_frames: row.get::<_, i64>(1)?,
                    backfilled: row.get::<_, i64>(2)?,
                })
            })
            .map_err(sql_err)
    }

    // ── CRC64 ──────────────────────────────────────────────────────────────

    /// Returns a CRC-64/ISO checksum of the database file and its current
    /// position, after forcing a RESTART checkpoint so the DB sits at the WAL
    /// start. Ported from `DB.CRC64` (db.go:2329-2359).
    pub fn crc64(&mut self) -> Result<(u64, Pos)> {
        // Force a RESTART checkpoint to ensure the DB is at the start of the WAL.
        self.checkpoint(CheckpointMode::Restart)?;

        let pos = self.pos()?;

        // Checksum the whole database (CRC64-ISO), page by page through the
        // VFS like every other database read.
        let pages = self.db_file_size()? / i64::from(self.page_size);
        let mut h = Crc64::new();
        for pgno in 1..=u32::try_from(pages).expect("page count") {
            h.update(&self.read_db_page(pgno)?);
        }
        Ok((h.sum64(), pos))
    }

    // ── Snapshot ───────────────────────────────────────────────────────────

    /// Writes a full database snapshot as an LTX file to `w` and returns the
    /// snapshot position. Ported from `DB.SnapshotReader` (db.go:1922-2021),
    /// buffered rather than streamed, as described in `client/mod.rs`.
    ///
    /// The snapshot spans `MinTXID=1 .. MaxTXID=pos.TXID` (db.go:1996-1997). Its
    /// page set is the full DB (lock page skipped), and — being a snapshot — the
    /// rolling post-apply checksum **is** tracked.
    pub fn snapshot_to_writer<W: std::io::Write>(&mut self, w: &mut W) -> Result<Pos> {
        if self.page_size == 0 {
            return Err(Error::Other(
                "db not ready: page size not initialized".into(),
            ));
        }

        let pos = self.pos()?;

        let db_size = self.db_file_size()?;
        let mut commit = (db_size / self.page_size as i64) as u32;

        let wal = WalImage::whole(self.host.read(&self.wal_path())?);
        let mut rd = WalReader::new(&wal.bytes).map_err(Error::from)?;
        let (page_map, max_offset, wal_commit) = rd.page_map().map_err(Error::from)?;
        if wal_commit > 0 {
            commit = wal_commit;
        }
        let wal_offset = rd.offset();
        let sz = if max_offset > 0 {
            max_offset - wal_offset
        } else {
            0
        };
        let (salt1, salt2) = rd.salt();

        let pages = self.collect_snapshot_pages(&wal, &page_map, commit)?;

        // A snapshot tracks the rolling post-apply checksum (MinTXID==1, no
        // NoChecksum flag) — compute it the way decode_file verifies it.
        let lock = lock_pgno(self.page_size);
        let mut rolling: crate::Checksum = crate::CHECKSUM_FLAG;
        for (p, d) in &pages {
            if *p != lock {
                rolling = crate::CHECKSUM_FLAG | (rolling ^ ltx::checksum_page(*p, d));
            }
        }

        let header = ltx::Header {
            version: ltx::VERSION,
            flags: 0,
            page_size: self.page_size,
            commit,
            min_txid: TXID(1),
            max_txid: pos.txid,
            timestamp: self.host.now_unix_millis(),
            pre_apply_checksum: 0,
            wal_offset,
            wal_size: sz,
            wal_salt1: salt1,
            wal_salt2: salt2,
            node_id: 0,
        };

        let encoded = ltx::encode_file(&header, &pages, rolling)?;
        w.write_all(&encoded)?;

        Ok(Pos::new(pos.txid, rolling))
    }

    /// Closes the database, releasing the read lock first so other processes can
    /// checkpoint. Ported from the read-lock-release + connection-close portion of
    /// `DB.Close` (db.go:623-647).
    pub fn close(mut self) -> Result<()> {
        self.release_read_lock()?;
        // The connection is dropped here, closing it.
        Ok(())
    }
}

// ── free functions ─────────────────────────────────────────────────────────

/// Returns the size of the WAL for a given page size & count, in i64 math to
/// avoid u32 overflow with large page sizes. Ported from `calcWALSize`
/// (db.go:1193-1197).
fn calc_wal_size(page_size: u32, page_n: u32) -> i64 {
    WAL_HEADER_SIZE as i64 + (WAL_FRAME_HEADER_SIZE as i64 + page_size as i64) * page_n as i64
}

/// `true` if the error indicates an SQLITE_BUSY condition. Ported from
/// `isSQLiteBusyError` (db.go:1158-1167). Matches both the rusqlite busy code and
/// the Go substrings.
fn is_sqlite_busy_error(err: &Error) -> bool {
    // Prefer matching the rusqlite error code when present.
    if let Error::Other(b) = err {
        if let Some(re) = b.downcast_ref::<rusqlite::Error>() {
            if let Some(code) = re.sqlite_error_code() {
                if code == rusqlite::ErrorCode::DatabaseBusy
                    || code == rusqlite::ErrorCode::DatabaseLocked
                {
                    return true;
                }
            }
        }
    }
    let s = err.to_string();
    s.contains("database is locked") || s.contains("SQLITE_BUSY")
}

/// `true` if the error indicates disk space issues (ENOSPC/EDQUOT). Ported from
/// `isDiskFullError` (db.go:1169-1180) — case-insensitive substring
/// match for parity with the Go table. This classifier matches the background
/// monitor's disk-full temporary-file cleanup behavior (db.go:2304-2309).
#[cfg_attr(not(test), allow(dead_code))]
fn is_disk_full_error(err_msg: &str) -> bool {
    let s = err_msg.to_lowercase();
    s.contains("no space left on device")
        || s.contains("disk quota exceeded")
        || s.contains("enospc")
        || s.contains("edquot")
}

/// Rolls back the connection's current transaction, swallowing the
/// "already rolled back" / "no transaction is active" errors (issue #934).
/// Ported from `rollback` (litestream.go:130-135), adapted to rusqlite's message.
fn rollback(conn: &Connection) -> Result<()> {
    match conn
        .prepare_cached("ROLLBACK")
        .and_then(|mut statement| statement.execute([]).map(|_| ()))
    {
        Ok(()) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            // Go swallows "transaction has already been committed or rolled back".
            // rusqlite/SQLite reports "cannot rollback - no transaction is active".
            if msg.contains("transaction has already been committed or rolled back")
                || msg.contains("no transaction is active")
                || msg.contains("cannot rollback")
            {
                Ok(())
            } else {
                Err(sql_err(e))
            }
        }
    }
}

/// Reads the 32-byte WAL header. Ported from `readWALHeader`
/// (litestream.go:138-148): returns the header bytes; errors if the file is
/// missing or shorter than 32 bytes.
fn read_wal_header(host: &crate::LtxHost, path: &Path) -> Result<[u8; WAL_HEADER_SIZE]> {
    let mut file = host.open(path)?;
    let bytes = file.read_exact_at(0, WAL_HEADER_SIZE)?;
    Ok(bytes
        .try_into()
        .expect("the exact read has the header size"))
}

/// Reads one page's worth of bytes from an in-memory WAL buffer at the frame
/// `offset` (the offset is the start of the *frame header*; the page data follows
/// the 24-byte frame header). Mirrors `walFile.ReadAt(data, offset+WALFrameHeaderSize)`
/// (db.go:1745,1787).
/// What one sync read of the WAL: the whole file (`tail_base` 0), or the
/// 32-byte header followed by the file's bytes from `tail_base` onward.
/// Offsets stay absolute at every consumer; the image maps them.
struct WalImage {
    bytes: Vec<u8>,
    tail_base: usize,
    file_len: usize,
}

impl WalImage {
    fn whole(bytes: Vec<u8>) -> Self {
        let file_len = bytes.len();
        Self {
            bytes,
            tail_base: 0,
            file_len,
        }
    }

    /// A reader positioned at `offset`, over whichever shape this image has.
    fn reader_at(
        &self,
        offset: i64,
        salt1: u32,
        salt2: u32,
    ) -> std::result::Result<WalReader<'_>, crate::wal::WalError> {
        if self.tail_base == 0 {
            WalReader::new_with_offset(&self.bytes, offset, salt1, salt2)
        } else {
            WalReader::new_with_offset_over_tail(
                &self.bytes,
                self.tail_base as i64,
                offset,
                salt1,
                salt2,
            )
        }
    }

    /// `n` bytes at absolute file `offset`, or `None` when the image does not
    /// hold them (a short read, or the gap between the header and the tail).
    fn slice(&self, offset: i64, n: usize) -> Option<&[u8]> {
        if offset < 0 {
            return None;
        }
        let offset = offset as usize;
        let start = if self.tail_base == 0 || offset < WAL_HEADER_SIZE {
            offset
        } else {
            WAL_HEADER_SIZE + offset.checked_sub(self.tail_base)?
        };
        let end = start.checked_add(n)?;
        (end <= self.bytes.len()).then(|| &self.bytes[start..end])
    }

    /// One page's worth of bytes at frame `offset` (the offset is the start of
    /// the *frame header*; the page data follows it).
    fn page(&self, offset: i64, page_size: u32) -> Result<Vec<u8>> {
        self.slice(offset + WAL_FRAME_HEADER_SIZE as i64, page_size as usize)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("short read wal page @ {offset}"),
                ))
            })
    }
}

/// Writes `data` to `tmp_path`, fsyncs, then renames to `final_path` — the
/// crash-consistent atomic-write idiom. On any failure the temp
/// file is removed.
fn write_file_atomic(
    host: &crate::LtxHost,
    tmp_path: &str,
    final_path: &str,
    data: &[u8],
) -> Result<u64> {
    let result = (|| -> Result<u64> {
        let mut file = host.create(Path::new(tmp_path))?;
        file.write_all(data)?;
        let fsync = crate::host::telemetry_us();
        file.sync_all()?;
        let fsync_us = crate::host::telemetry_us().saturating_sub(fsync);
        drop(file);
        host.rename(Path::new(tmp_path), Path::new(final_path))?;
        Ok(fsync_us)
    })();
    if result.is_err() {
        let _ = host.remove_file(Path::new(tmp_path));
    }
    result
}

/// Recursively removes `.tmp` files under `root`. Ported from `removeTmpFiles`
/// (litestream.go:169-182): missing root / errored entries are skipped.
fn remove_tmp_files(host: &crate::LtxHost, root: &Path) -> Result<()> {
    fn walk(host: &crate::LtxHost, dir: &Path) {
        let entries = match host.read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for ent in entries {
            let path = ent.path;
            if ent.is_dir {
                walk(host, &path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                let _ = host.remove_file(&path);
            }
        }
    }
    walk(host, root);
    Ok(())
}

fn system_time_from_unix_millis(milliseconds: i64) -> SystemTime {
    if milliseconds >= 0 {
        UNIX_EPOCH + Duration::from_millis(milliseconds as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis(milliseconds.unsigned_abs())
    }
}

/// Big-endian `u32` from the first four bytes of `b`.
#[inline]
fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

#[doc(hidden)]
pub mod internal {
    /// Counts SQL compilations on the capture's connections: SQLite runs the
    /// authorizer once per compilation, so the counter moves for a fresh
    /// `prepare` and for a re-prepare, and stays still for a cached statement.
    /// (Installing the authorizer expires the statements compiled so far, so
    /// the first sync after this call compiles once more.)
    pub fn count_sql_compilations(db: &super::Db) -> std::sync::Arc<std::sync::atomic::AtomicU64> {
        use rusqlite::hooks::{AuthContext, Authorization};
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        for conn in [&db.conn, &db.rtx_conn] {
            let counter = counter.clone();
            conn.authorizer(Some(move |_: AuthContext<'_>| {
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Authorization::Allow
            }));
        }
        counter
    }

    use super::*;

    pub struct VerifyInfo {
        inner: SyncInfo,
        pub offset: i64,
        pub prev_commit: u32,
        pub snapshotting: bool,
        pub reason: String,
    }

    pub fn calc_wal_size(page_size: u32, page_n: u32) -> i64 {
        super::calc_wal_size(page_size, page_n)
    }

    pub fn is_sqlite_busy_error(error: &Error) -> bool {
        super::is_sqlite_busy_error(error)
    }

    pub fn is_disk_full_error(message: &str) -> bool {
        super::is_disk_full_error(message)
    }

    pub fn read_lock_held(db: &Db) -> bool {
        db.read_lock_held
    }

    /// Run one `sync` with `hook` invoked between `verify` and the capture
    /// read — the only window in which a WAL change can reach the
    /// previous-frame check without `verify` seeing it first.
    pub fn sync_with_verify_hook(db: &mut Db, hook: impl FnOnce() + 'static) -> Result<()> {
        db.sync_with_verify_hook(Some(Box::new(hook)))
    }

    pub fn wal_journal_mode(db: &Db) -> Result<String> {
        db.conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(sql_err)
    }

    pub fn wal_autocheckpoint(db: &Db) -> Result<i64> {
        db.conn
            .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
            .map_err(sql_err)
    }

    pub fn rollback_read_lock_connection(db: &Db) -> Result<()> {
        db.rtx_conn.execute_batch("ROLLBACK").map_err(sql_err)
    }

    pub fn release_read_lock(db: &mut Db) -> Result<()> {
        db.release_read_lock()
    }

    pub fn acquire_read_lock(db: &mut Db) -> Result<()> {
        db.acquire_read_lock()
    }

    pub fn verify(db: &mut Db) -> Result<VerifyInfo> {
        let inner = db.verify()?;
        Ok(VerifyInfo {
            offset: inner.offset,
            prev_commit: inner.prev_commit,
            snapshotting: inner.snapshotting,
            reason: inner.reason.clone(),
            inner,
        })
    }

    pub fn sync_inner(db: &mut Db, info: VerifyInfo) -> Result<bool> {
        db.sync_inner(info.inner)
    }

    pub fn read_wal_header(db: &Db) -> Result<[u8; WAL_HEADER_SIZE]> {
        super::read_wal_header(&db.host, &db.wal_path())
    }

    pub fn invalidate_pos_cache(db: &mut Db) {
        db.invalidate_pos_cache();
    }

    pub fn ltx_level_dir(db: &Db, level: u32) -> String {
        db.ltx_level_dir(level)
    }

    pub fn checkpoint_passive_with_barrier_hook(
        db: &mut Db,
        hook: Box<dyn FnOnce() + Send>,
    ) -> Result<()> {
        db.checkpoint_with_passive_hooks(CheckpointMode::Passive, Some(hook), None, None)
    }

    pub fn checkpoint_passive_with_unlocked_hook(
        db: &mut Db,
        hook: Box<dyn FnOnce() + Send>,
    ) -> Result<()> {
        db.checkpoint_with_passive_hooks(CheckpointMode::Passive, None, Some(hook), None)
    }

    /// Run a passive checkpoint with `hook` invoked after the writer barrier
    /// dropped and before the sealing write: the window in which another
    /// writer can take the lock and restart the WAL itself.
    pub fn checkpoint_passive_with_post_barrier_hook(
        db: &mut Db,
        hook: Box<dyn FnOnce() + Send>,
    ) -> Result<()> {
        db.checkpoint_with_passive_hooks(CheckpointMode::Passive, None, None, Some(hook))
    }

    pub fn be_u32(bytes: &[u8]) -> u32 {
        super::be_u32(bytes)
    }
}
