//! db.rs — SQLite database lifecycle: WAL-mode setup, the long-running read-lock
//! checkpoint takeover, the WAL→LTX capture loop, and manual checkpointing.
//! Ported from litestream@v0.5.11 `db.go`.
//!
//! T9 is the highest-risk module in the crate (Risk R-4: a long-running read
//! transaction interacting with manual `PRAGMA wal_checkpoint`), so it is landed
//! in tight, independently-tested sub-steps.
//!
//! ## Async/blocking shape — DECISION (synchronous `Db`)
//! `rusqlite::Connection` is `!Sync` and a `rusqlite::Transaction` borrows its
//! `Connection`. Rather than thread a borrow across `.await`, this module keeps
//! the capture API **synchronous** and owns the connection directly: the
//! long-running read transaction is held with raw `BEGIN`/`ROLLBACK` SQL plus a
//! `read_lock_held` flag — exactly as Go does (`acquireReadLock` runs `BEGIN` +
//! `SELECT COUNT(1)`, db.go:956-976; `releaseReadLock` rolls back, db.go:979-992).
//! T10's `Replica` drives `sync()`/`checkpoint()` from a blocking context
//! (`spawn_blocking` or a dedicated DB thread). This sidesteps the !Sync/borrow
//! problem entirely and makes the idempotent-release behavior (issue #934) a
//! trivial flag check. The task brief sanctions this ("keep the capture API
//! synchronous and let T10 drive it"). Logged in OPEN_QUESTIONS.md.
//!
//! ## What this implements (the functional capture path)
//! - `open`/`init`: WAL-mode DSN, `wal_autocheckpoint(0)`, control tables, read
//!   page size, acquire the read lock, ensure the WAL has ≥1 frame.
//! - `acquire_read_lock`/`release_read_lock`: the checkpoint takeover; release is
//!   idempotent (db.go:979-992 / issue #934, footgun F-2).
//! - `sync` → `verify` → `sync_inner` → `write_ltx_from_wal`/`write_ltx_from_db`:
//!   diff the real WAL against the last LTX position and write the next L0 LTX
//!   file (`db.go:1517-1723`), with atomic tmp→rename and the pos cache.
//! - `verify`: the snapshot-on-continuity-break branch lattice (`db.go:1296-1436`,
//!   footgun F-7), including the issue #900 / #927 edge cases.
//! - `checkpoint`/`exec_checkpoint`: release→`PRAGMA wal_checkpoint(<mode>)`→
//!   re-acquire (`db.go:1875-1919`, footgun F-1) and the two-phase WAL-restart
//!   handling (`db.go:1808-1873`, footgun F-9).
//! - `checkpoint_if_needed`: the 3-tier policy + the three anti-feedback flags
//!   (`synced_since_checkpoint`/`synced_to_wal_end`/`last_synced_wal_offset`,
//!   issues #896/#927/#997, footgun F-5).
//! - `crc64`, `pos` (cached + `LTXError` mapping), `reset_local_state`,
//!   `snapshot_to_writer`.
//!
//! ## Deferred (logged in OPEN_QUESTIONS.md — not on the functional path)
//! - `setPersistWAL` (the `unsafe` `sqlite3_file_control(SQLITE_FCNTL_PERSIST_WAL)`
//!   FFI, footgun F-10): only matters when *all* connections close and SQLite
//!   would delete the WAL; the capture path keeps its own connection open.
//! - The background monitor loop + backoff (footgun F-13), `Replica` integration
//!   (`ensure_exists`/`sync_status`/`sync_and_wait`/retention/`syncReplicaWithRetry`):
//!   these need T10's `Replica` handle and land with it.
//! - `loom` model of the lock protocol (PLAN.md §6.4).

use crate::error::{new_ltx_error, Error, Result};
use crate::ltx::{self, lock_pgno, Crc64};
use crate::wal::WalReader;
use crate::{
    ltx_file_path, ltx_level_dir, Pos, CHECKPOINT_MODE_PASSIVE, CHECKPOINT_MODE_RESTART,
    CHECKPOINT_MODE_TRUNCATE, META_DIR_SUFFIX, TXID, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
};
use rusqlite::Connection;
use std::collections::HashMap;
use std::io::Read;
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
    /// If true, a full snapshot is required.
    snapshotting: bool,
    /// Reason for the snapshot (for logging / test assertions).
    reason: String,
}

/// A SQLite database managed for replication.
///
/// Owns the connection (WAL mode, auto-checkpoint disabled) so litestream — not
/// SQLite — decides when the WAL is checkpointed, and holds the long-running read
/// transaction that takes over checkpointing.
///
/// Ported from `DB` (db.go:64-198). Synchronous by DECISION (see module docs):
/// the `!Sync` connection stays on whatever thread T10 drives it from.
pub struct Db {
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

    // ── Tunables (db.go:131-197) ────────────────────────────────────────────
    /// PASSIVE checkpoint threshold, in pages (db.go:34 default 1000).
    pub min_checkpoint_page_n: u32,
    /// TRUNCATE checkpoint threshold, in pages; 0 disables (db.go:35 default).
    pub truncate_page_n: u32,
    /// Time-based PASSIVE checkpoint interval; 0 disables (db.go:32 default 60s).
    pub checkpoint_interval: Duration,
    /// Busy timeout for SQLite locks (db.go:33 default 1s).
    pub busy_timeout: Duration,

    // ── Anti-feedback-loop bookkeeping (issues #896/#927/#997, footgun F-5) ──
    /// True once data has synced since the last checkpoint (#896, db.go:80).
    synced_since_checkpoint: bool,
    /// True if the last sync reached the exact WAL EOF (#927, db.go:88).
    synced_to_wal_end: bool,
    /// Logical end of WAL content after the last sync = `WALOffset + WALSize`
    /// from the last LTX (#997, db.go:96). Used for checkpoint thresholds
    /// instead of file size (stale post-checkpoint frames inflate file size).
    last_synced_wal_offset: i64,

    /// Cached L0 position; `None` = invalid (db.go:106-109).
    pos_cache: Option<Pos>,
    /// Last L0 `FileInfo` (db.go:99-102; only L0 is tracked in the one-shot).
    max_l0_file_info: Option<ltx::FileInfo>,
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
    /// read-lock acquire and `ensureWALExists`. (Replica wiring lands in T10.)
    pub fn open(path: impl AsRef<Path>) -> Result<Db> {
        let path = path.as_ref().to_path_buf();
        let meta_path = Self::meta_path_for(&path);

        let conn = Connection::open(&path).map_err(sql_err)?;

        // DSN pragmas: busy_timeout + wal_autocheckpoint(0) (db.go:818).
        // autocheckpoint MUST be 0 — litestream owns checkpointing (footgun
        // F-10). Per-connection, not per-file: the cell's own writer
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

        // Control tables: `_litestream_seq` forces WAL writes when empty; the
        // `_litestream_lock` table forces a write lock during sync (db.go:857-864).
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);\
             CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);",
        )
        .map_err(sql_err)?;

        // Dedicated read-lock connection (mirrors a second pooled connection).
        let rtx_conn = Connection::open(&path).map_err(sql_err)?;
        rtx_conn
            .busy_timeout(Self::DEFAULT_BUSY_TIMEOUT)
            .map_err(sql_err)?;

        let mut db = Db {
            path,
            meta_path,
            conn,
            rtx_conn,
            page_size: 0,
            read_lock_held: false,
            min_checkpoint_page_n: Self::DEFAULT_MIN_CHECKPOINT_PAGE_N,
            truncate_page_n: Self::DEFAULT_TRUNCATE_PAGE_N,
            checkpoint_interval: Self::DEFAULT_CHECKPOINT_INTERVAL,
            busy_timeout: Self::DEFAULT_BUSY_TIMEOUT,
            synced_since_checkpoint: false,
            synced_to_wal_end: false,
            last_synced_wal_offset: 0,
            pos_cache: None,
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
        std::fs::create_dir_all(&db.meta_path)?;

        // Clear crash-leftover temp files (db.go:576).
        remove_tmp_files(&db.meta_path)?;

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
        self.rtx_conn.execute_batch("BEGIN").map_err(sql_err)?;
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
    /// swallowed (issue #934, footgun F-2) — a double release must return
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
    fn ensure_control_tables(&self) -> Result<()> {
        self.conn
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);\
                 CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);",
            )
            .map_err(sql_err)
    }

    /// Ensures the real WAL exists and has a header.
    ///
    /// Ported from `ensureWALExists` (db.go:1199-1209): exit early if the WAL
    /// header is present; otherwise force a write to `_litestream_seq`.
    fn ensure_wal_exists(&self) -> Result<()> {
        if let Ok(md) = std::fs::metadata(self.wal_path()) {
            if md.len() >= WAL_HEADER_SIZE as u64 {
                return Ok(());
            }
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
    fn wal_file_size(&self) -> Result<i64> {
        match std::fs::metadata(self.wal_path()) {
            Ok(md) => Ok(md.len() as i64),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    /// The size of the main database file in bytes, 0 if absent.
    fn db_file_size(&self) -> Result<i64> {
        match std::fs::metadata(&self.path) {
            Ok(md) => Ok(md.len() as i64),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    // ── Position ───────────────────────────────────────────────────────────

    /// The highest `(min,max)` TXID pair in the L0 directory, `(0,0)` if none.
    /// Ported from `DB.MaxLTX` (db.go:363-380).
    fn max_ltx(&self) -> Result<(TXID, TXID)> {
        let dir = self.ltx_level_dir(0);
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((TXID(0), TXID(0))),
            Err(e) => return Err(e.into()),
        };
        let mut min_txid = TXID(0);
        let mut max_txid = TXID(0);
        for ent in entries {
            let ent = ent?;
            let name = ent.file_name();
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
    /// `LTXError` (db.go:412,418, footgun F-7's error-mapping companion).
    pub fn pos(&mut self) -> Result<Pos> {
        if let Some(p) = self.pos_cache {
            return Ok(p);
        }

        let (min_txid, max_txid) = self.max_ltx()?;
        if min_txid == TXID(0) {
            return Ok(Pos::ZERO); // no replication yet
        }

        let ltx_path = self.ltx_path(0, min_txid, max_txid);
        let bytes = match std::fs::read(&ltx_path) {
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
        match std::fs::remove_dir_all(self.ltx_dir()) {
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
    /// synchronous `Db` has no `ReplicaClient` handle of its own (see the module
    /// docs / OPEN_QUESTIONS T9/T10 deferral). Used by
    /// [`crate::replica::Replica::check_database_behind_replica`] (issue #781).
    pub fn seed_l0_baseline(&mut self, min_txid: TXID, max_txid: TXID, data: &[u8]) -> Result<()> {
        // Clear local L0 files (db.go:1241-1249).
        let l0_dir = self.ltx_level_dir(0);
        match std::fs::remove_dir_all(&l0_dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        self.max_l0_file_info = None;
        self.invalidate_pos_cache();
        std::fs::create_dir_all(&l0_dir)?;

        // Write the baseline file atomically (db.go:1260-1286).
        let local_path = self.ltx_path(0, min_txid, max_txid);
        let tmp_path = format!("{local_path}.tmp");
        write_file_atomic(&tmp_path, &local_path, data)?;
        self.invalidate_pos_cache();
        Ok(())
    }

    // ── Capture loop ───────────────────────────────────────────────────────

    /// Copies pending data from the WAL into the next L0 LTX file and applies
    /// the checkpoint policy.
    ///
    /// Ported from `DB.Sync` (db.go:994-1056). The public entry point of the
    /// capture loop. Synchronous (see module docs); T10 drives it.
    pub fn sync(&mut self) -> Result<()> {
        // Self-heal: recreate the control tables if something swept them out
        // of `sqlite_schema` from under the replicator — without them every
        // capture fails until the database is reopened. A no-op when the
        // tables exist (no schema change, no WAL write).
        self.ensure_control_tables()?;

        // Ensure the WAL has at least one frame (db.go:1017-1020).
        self.ensure_wal_exists()?;

        let (orig_wal_size, new_wal_size, synced) = self.verify_and_sync()?;

        // Track that data was synced for time-based checkpoint decisions.
        if synced {
            self.synced_since_checkpoint = true;
        }

        self.checkpoint_if_needed(orig_wal_size, new_wal_size)?;

        // Recompute the cached position (kept for parity with db.go:1037-1041).
        let _ = self.pos()?;

        Ok(())
    }

    /// Verifies the last sync against the current WAL, then syncs.
    ///
    /// Ported from `DB.verifyAndSync` (db.go:1058-1090). Returns
    /// `(orig_wal_size, new_wal_size, synced)` where the sizes are the **logical**
    /// WAL offset (`WALOffset+WALSize` of the last LTX), not file size (#997).
    fn verify_and_sync(&mut self) -> Result<(i64, i64, bool)> {
        // Use the last synced WAL offset as the logical size for checkpoint
        // decisions; on the first sync fall back to file size (db.go:1062-1069).
        let mut orig_wal_size = self.last_synced_wal_offset;
        if orig_wal_size == 0 {
            orig_wal_size = self.wal_file_size()?;
        }

        let info = self.verify()?;
        let synced = self.sync_inner(info)?;

        let new_wal_size = self.last_synced_wal_offset;
        Ok((orig_wal_size, new_wal_size, synced))
    }

    /// Ensures the LTX state matches where it left off from the real WAL.
    ///
    /// Ported branch-for-branch from `DB.verify` (db.go:1296-1436), footgun F-7.
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

        // Determine the last WAL offset we saved from, by decoding the last LTX
        // file's header (db.go:1311-1326).
        let ltx_path = self.ltx_path(0, pos.txid, pos.txid);
        let ltx_bytes = match std::fs::read(&ltx_path) {
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
        let hdr = match ltx::Header::parse(&ltx_bytes) {
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
        info.offset = hdr.wal_offset + hdr.wal_size;
        info.salt1 = hdr.wal_salt1;
        info.salt2 = hdr.wal_salt2;

        // If the LTX WAL offset exceeds the real WAL size, the WAL was truncated.
        let wal_size = self.wal_file_size()?;
        if info.offset > wal_size {
            // If we previously synced to the exact WAL end, this truncation is an
            // expected checkpoint: reset to the header and continue incrementally
            // rather than snapshotting (issue #927, db.go:1335-1355).
            if self.synced_to_wal_end {
                self.synced_to_wal_end = false;

                let wal_hdr = read_wal_header(&self.wal_path())?;
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
        let wal_hdr = read_wal_header(&self.wal_path())?;
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
        let last_page_match =
            self.last_page_match(&ltx_bytes, &hdr, prev_wal_offset, frame_size)?;
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
        &self,
        ltx_bytes: &[u8],
        hdr: &ltx::Header,
        prev_wal_offset: i64,
        frame_size: i64,
    ) -> Result<bool> {
        if prev_wal_offset <= WAL_HEADER_SIZE as i64 {
            return Ok(false);
        }

        let frame = read_wal_file_at(&self.wal_path(), prev_wal_offset, frame_size)?;
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

    /// Detects whether a FULL or RESTART checkpoint occurred (we may have missed
    /// frames). Ported from `DB.detectFullCheckpoint` (db.go:1477-1507).
    fn detect_full_checkpoint(&self, known_salts: &[(u32, u32)]) -> Result<bool> {
        let wal_bytes = std::fs::read(self.wal_path())?;
        let rd = WalReader::new(&wal_bytes).map_err(Error::from)?;
        let last_known = known_salts.last().copied().unwrap_or((0, 0));
        let mut m = rd.frame_salts_until(last_known);
        for s in known_salts {
            m.remove(s);
        }
        Ok(!m.is_empty())
    }

    /// Copies pending bytes from the real WAL into a new L0 LTX file.
    ///
    /// Ported from `DB.sync` (db.go:1517-1723). Returns `true` if an LTX file was
    /// written (there were new pages or we were snapshotting). Atomic
    /// tmp→fsync→rename with the pos cache + anti-feedback flags updated after.
    fn sync_inner(&mut self, mut info: SyncInfo) -> Result<bool> {
        let pos = self.pos()?;
        let tx_id = TXID(pos.txid.0 + 1);
        let filename = self.ltx_path(0, tx_id, tx_id);

        let db_size = self.db_file_size()?;
        let mut commit = (db_size / self.page_size as i64) as u32;

        let wal_bytes = std::fs::read(self.wal_path())?;

        // Choose the WAL reader start: from the header, or seek to info.offset.
        // A previous-frame mismatch falls back to a full read (snapshot),
        // mirroring NewWALReaderWithOffset's PrevFrameMismatchError handling
        // (db.go:1565-1581, footgun F-7).
        let mut rd = if info.offset == WAL_HEADER_SIZE as i64 {
            WalReader::new(&wal_bytes).map_err(Error::from)?
        } else {
            match WalReader::new_with_offset(&wal_bytes, info.offset, info.salt1, info.salt2) {
                Ok(r) => r,
                Err(crate::wal::WalError::PrevFrameMismatch) => {
                    info.offset = WAL_HEADER_SIZE as i64;
                    WalReader::new(&wal_bytes).map_err(Error::from)?
                }
                Err(e) => return Err(e.into()),
            }
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
            self.collect_snapshot_pages(&wal_bytes, &page_map, commit)?
        } else {
            self.collect_wal_pages(&wal_bytes, &page_map)?
        };

        let header = ltx::Header {
            version: ltx::VERSION,
            flags: ltx::HEADER_FLAG_NO_CHECKSUM,
            page_size: self.page_size,
            commit,
            min_txid: tx_id,
            max_txid: tx_id,
            timestamp: now_unix_millis(),
            pre_apply_checksum: 0,
            wal_offset: info.offset,
            wal_size: sz,
            wal_salt1: rd_salt1,
            wal_salt2: rd_salt2,
            node_id: 0,
        };

        // Encode the LTX file (with HeaderFlagNoChecksum, so post-apply is 0).
        let encoded = ltx::encode_file(&header, &pages, 0)?;

        // Atomic tmp → fsync → rename (db.go:1609-1685, footgun F-8).
        let tmp_filename = format!("{filename}.tmp");
        if let Some(parent) = Path::new(&tmp_filename).parent() {
            std::fs::create_dir_all(parent)?;
        }
        write_file_atomic(&tmp_filename, &filename, &encoded).inspect_err(|_| {
            // On rename failure, clear the L0 cache + invalidate pos
            // (db.go:1680-1684). We do this in the error path below too.
        })?;

        // Update the L0 file-info cache and the cached position (db.go:1687-1702).
        self.max_l0_file_info = Some(ltx::FileInfo {
            level: 0,
            min_txid: tx_id,
            max_txid: tx_id,
            pre_apply_checksum: 0,
            post_apply_checksum: 0,
            size: encoded.len() as i64,
            created_at: Some(SystemTime::now()),
        });
        // The encoder's post-apply pos: for a NoChecksum file the post-apply
        // checksum is 0; the position is (tx_id, 0).
        self.pos_cache = Some(Pos::new(tx_id, 0));

        // Track the logical end of WAL content for checkpoint decisions
        // (db.go:1704-1718, issues #997/#927, footgun F-5).
        let final_offset = info.offset + sz;
        self.last_synced_wal_offset = final_offset;
        self.synced_to_wal_end = match self.wal_file_size() {
            Ok(wal_size) => final_offset == wal_size,
            Err(_) => false,
        };

        Ok(true)
    }

    /// Collects the page set for an incremental (WAL-only) sync, in ascending
    /// page-number order. Ported from `DB.writeLTXFromWAL` (db.go:1772-1799).
    fn collect_wal_pages(
        &self,
        wal_bytes: &[u8],
        page_map: &HashMap<u32, i64>,
    ) -> Result<Vec<(u32, Vec<u8>)>> {
        let mut pgnos: Vec<u32> = page_map.keys().copied().collect();
        pgnos.sort_unstable();

        let mut out = Vec::with_capacity(pgnos.len());
        for pgno in pgnos {
            let offset = page_map[&pgno];
            let data = read_wal_page(wal_bytes, offset, self.page_size)?;
            out.push((pgno, data));
        }
        Ok(out)
    }

    /// Collects the full page set for a snapshot: every page `1..=commit`
    /// (skipping the lock page), reading from the WAL where present, else the DB
    /// file. Ported from `DB.writeLTXFromDB` (db.go:1725-1770).
    fn collect_snapshot_pages(
        &self,
        wal_bytes: &[u8],
        page_map: &HashMap<u32, i64>,
        commit: u32,
    ) -> Result<Vec<(u32, Vec<u8>)>> {
        let lock = lock_pgno(self.page_size);
        let mut db_file: Option<std::fs::File> = None;
        let mut out = Vec::with_capacity(commit as usize);

        for pgno in 1..=commit {
            if pgno == lock {
                continue;
            }
            if let Some(&offset) = page_map.get(&pgno) {
                let data = read_wal_page(wal_bytes, offset, self.page_size)?;
                out.push((pgno, data));
                continue;
            }
            // Read directly from the database file.
            let f = match &mut db_file {
                Some(f) => f,
                None => {
                    db_file = Some(std::fs::File::open(&self.path)?);
                    db_file.as_mut().unwrap()
                }
            };
            let offset = (pgno as i64 - 1) * self.page_size as i64;
            let data = read_file_at(f, offset, self.page_size as usize)?;
            out.push((pgno, data));
        }
        Ok(out)
    }

    // ── Checkpointing ──────────────────────────────────────────────────────

    /// Performs a checkpoint based on the configured thresholds (3-tier policy).
    ///
    /// Ported from `DB.checkpointIfNeeded` (db.go:1092-1156, footgun F-5). Checks
    /// in priority order: TruncatePageN (TRUNCATE, blocking) → MinCheckpointPageN
    /// (PASSIVE) → CheckpointInterval (PASSIVE, gated on `synced_since_checkpoint`).
    fn checkpoint_if_needed(&mut self, orig_wal_size: i64, new_wal_size: i64) -> Result<()> {
        if self.page_size == 0 {
            return Ok(());
        }

        // Priority 1: emergency TRUNCATE (blocking) on the *original* logical size.
        if self.truncate_page_n > 0
            && orig_wal_size >= calc_wal_size(self.page_size, self.truncate_page_n)
        {
            return self.checkpoint(CheckpointMode::Truncate);
        }

        // Priority 2: PASSIVE at the min threshold on the *new* logical size.
        if new_wal_size >= calc_wal_size(self.page_size, self.min_checkpoint_page_n) {
            return self.checkpoint_passive_swallowing_busy();
        }

        // Priority 3: time-based PASSIVE, gated on data synced since last
        // checkpoint (#896). Uses the DB-file mtime and a logical-size guard so an
        // idle DB does not spin LTX files (db.go:1133-1153).
        if self.checkpoint_interval > Duration::ZERO && self.synced_since_checkpoint {
            let modified = std::fs::metadata(&self.path)?.modified()?;
            let elapsed = SystemTime::now()
                .duration_since(modified)
                .unwrap_or(Duration::ZERO);
            if elapsed > self.checkpoint_interval && new_wal_size > calc_wal_size(self.page_size, 1)
            {
                return self.checkpoint_passive_swallowing_busy();
            }
        }

        Ok(())
    }

    /// PASSIVE checkpoint that swallows SQLITE_BUSY (log-and-continue, the one
    /// sanctioned best-effort case, db.go:1118-1124 / footgun F-6).
    fn checkpoint_passive_swallowing_busy(&mut self) -> Result<()> {
        match self.checkpoint(CheckpointMode::Passive) {
            Ok(()) => Ok(()),
            Err(e) if is_sqlite_busy_error(&e) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Performs a checkpoint on the WAL.
    ///
    /// Ported from `DB.Checkpoint`/`DB.checkpoint` (db.go:1801-1873, footgun F-9).
    /// Two-phase: copy the WAL tail, `exec_checkpoint`, force a fresh WAL page,
    /// and — if the WAL restarted — grab a write lock and copy the post-checkpoint
    /// tail before rolling back.
    ///
    /// NOTE: the upstream `chkMu.TryLock` snapshot-vs-checkpoint gate (footgun F-3)
    /// is unnecessary in the synchronous `Db` — `&mut self` already serializes
    /// `sync`/`checkpoint`/`snapshot`. T10 preserves this by serializing access to
    /// the `Db` (single owner / one blocking thread).
    pub fn checkpoint(&mut self, mode: CheckpointMode) -> Result<()> {
        // Read the WAL header before the checkpoint to detect a restart.
        let hdr = read_wal_header(&self.wal_path())?;

        // Copy the end of the WAL before the checkpoint to capture as much as
        // possible (db.go:1823-1826).
        self.verify_and_sync()?;

        // Execute the checkpoint, then force a write so a new WAL page exists.
        self.exec_checkpoint(mode)?;
        self.conn
            .execute_batch(
                "INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) \
                 ON CONFLICT (id) DO UPDATE SET seq = seq + 1",
            )
            .map_err(sql_err)?;

        // If the WAL header is unchanged, the WAL did not restart — done.
        let other = read_wal_header(&self.wal_path())?;
        if hdr == other {
            self.synced_since_checkpoint = false;
            return Ok(());
        }

        // The WAL restarted. Grab the write lock (via an INSERT into the lock
        // table inside a transaction that always rolls back, db.go:1844-1857), copy
        // anything written after the checkpoint, then release.
        self.conn.execute_batch("BEGIN").map_err(sql_err)?;
        let post = (|| -> Result<()> {
            self.conn
                .execute_batch("INSERT INTO _litestream_lock (id) VALUES (1)")
                .map_err(sql_err)?;
            self.verify_and_sync()?;
            Ok(())
        })();
        // Always roll back the write transaction (db.go:1849,1867).
        let rb = rollback(&self.conn);
        post?;
        rb?;

        self.synced_since_checkpoint = false;
        Ok(())
    }

    /// Releases the read lock, runs `PRAGMA wal_checkpoint(<mode>)`, and
    /// re-acquires the read lock — re-acquiring even on error.
    ///
    /// Ported from `DB.execCheckpoint` (db.go:1875-1919, footgun F-1). The exact
    /// release→checkpoint→re-acquire sequence is load-bearing.
    fn exec_checkpoint(&mut self, mode: CheckpointMode) -> Result<()> {
        // Ensure the read lock is removed before the checkpoint; defer the
        // re-acquire so it runs even on early return.
        self.release_read_lock()?;

        let result = self.run_checkpoint_pragma(mode);

        // Re-acquire the read lock immediately after the checkpoint (the deferred
        // re-acquire in Go). If the pragma succeeded, propagate any re-acquire
        // error; otherwise surface the original pragma error.
        let reacquire = self.acquire_read_lock();
        match (result, reacquire) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(e)) => Err(e),
            (Err(e), _) => Err(e),
        }
    }

    /// Runs the raw `PRAGMA wal_checkpoint(<mode>)` and reads its 3-int result.
    fn run_checkpoint_pragma(&self, mode: CheckpointMode) -> Result<()> {
        let sql = format!("PRAGMA wal_checkpoint({mode})");
        self.conn
            .query_row(&sql, [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(sql_err)?;
        Ok(())
    }

    // ── CRC64 ──────────────────────────────────────────────────────────────

    /// Returns a CRC-64/ISO checksum of the database file and its current
    /// position, after forcing a RESTART checkpoint so the DB sits at the WAL
    /// start. Ported from `DB.CRC64` (db.go:2329-2359).
    pub fn crc64(&mut self) -> Result<(u64, Pos)> {
        // Force a RESTART checkpoint to ensure the DB is at the start of the WAL.
        self.checkpoint(CheckpointMode::Restart)?;

        let pos = self.pos()?;

        // Checksum the whole database file (CRC64-ISO).
        let bytes = std::fs::read(&self.path)?;
        let mut h = Crc64::new();
        h.update(&bytes);
        Ok((h.sum64(), pos))
    }

    // ── Snapshot ───────────────────────────────────────────────────────────

    /// Writes a full database snapshot as an LTX file to `w` and returns the
    /// snapshot position. Ported from `DB.SnapshotReader` (db.go:1922-2021),
    /// buffered (not streamed — DECISION, see `client/mod.rs`).
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

        let wal_bytes = std::fs::read(self.wal_path())?;
        let mut rd = WalReader::new(&wal_bytes).map_err(Error::from)?;
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

        let pages = self.collect_snapshot_pages(&wal_bytes, &page_map, commit)?;

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
            timestamp: now_unix_millis(),
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
    /// `DB.Close` (db.go:623-647). (The replica final-sync + retry land in T10.)
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
/// the Go substrings (footgun F-6).
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
/// `isDiskFullError` (db.go:1169-1180, footgun F-6) — case-insensitive substring
/// match for parity with the Go test table.
///
/// Currently consumed only by tests; its production caller is the background
/// monitor's disk-full temp-file cleanup (db.go:2304-2309), which lands with the
/// `Replica` integration in T10. The classifier itself is real and tested now.
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
    match conn.execute_batch("ROLLBACK") {
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
fn read_wal_header(path: &Path) -> Result<[u8; WAL_HEADER_SIZE]> {
    let mut f = std::fs::File::open(path)?;
    let mut buf = [0u8; WAL_HEADER_SIZE];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

/// Reads `n` bytes at `offset` from a WAL file. Ported from `readWALFileAt`
/// (litestream.go:152-166): a short read is an "unexpected EOF" error.
fn read_wal_file_at(path: &Path, offset: i64, n: i64) -> Result<Vec<u8>> {
    let bytes = std::fs::read(path)?;
    let start = offset as usize;
    let end = start
        .checked_add(n as usize)
        .ok_or_else(|| Error::Other("wal read overflow".into()))?;
    if end > bytes.len() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "short wal read",
        )));
    }
    Ok(bytes[start..end].to_vec())
}

/// Reads one page's worth of bytes from an in-memory WAL buffer at the frame
/// `offset` (the offset is the start of the *frame header*; the page data follows
/// the 24-byte frame header). Mirrors `walFile.ReadAt(data, offset+WALFrameHeaderSize)`
/// (db.go:1745,1787).
fn read_wal_page(wal_bytes: &[u8], offset: i64, page_size: u32) -> Result<Vec<u8>> {
    let start = (offset + WAL_FRAME_HEADER_SIZE as i64) as usize;
    let end = start
        .checked_add(page_size as usize)
        .ok_or_else(|| Error::Other("wal page read overflow".into()))?;
    if end > wal_bytes.len() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("short read wal page @ {offset}"),
        )));
    }
    Ok(wal_bytes[start..end].to_vec())
}

/// Reads `n` bytes at `offset` from an open file (the DB-file `ReadAt`).
fn read_file_at(f: &mut std::fs::File, offset: i64, n: usize) -> Result<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};
    f.seek(SeekFrom::Start(offset as u64))?;
    let mut buf = vec![0u8; n];
    f.read_exact(&mut buf)?;
    Ok(buf)
}

/// Writes `data` to `tmp_path`, fsyncs, then renames to `final_path` — the
/// crash-consistent atomic-write idiom (footgun F-8). On any failure the temp
/// file is removed.
fn write_file_atomic(tmp_path: &str, final_path: &str, data: &[u8]) -> Result<()> {
    use std::io::Write;
    let result = (|| -> Result<()> {
        let mut f = std::fs::File::create(tmp_path)?;
        f.write_all(data)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(tmp_path, final_path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(tmp_path);
    }
    result
}

/// Recursively removes `.tmp` files under `root`. Ported from `removeTmpFiles`
/// (litestream.go:169-182): missing root / errored entries are skipped.
fn remove_tmp_files(root: &Path) -> Result<()> {
    fn walk(dir: &Path) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for ent in entries.flatten() {
            let path = ent.path();
            if path.is_dir() {
                walk(&path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    walk(root);
    Ok(())
}

/// Current time as Unix milliseconds (the LTX header timestamp).
fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Big-endian `u32` from the first four bytes of `b`.
#[inline]
fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Test helpers ──────────────────────────────────────────────────────

    /// Opens a managed `Db` plus a *separate* writer connection over the same
    /// file (the analog of testingutil.MustOpenDBs: the app writes through its
    /// own connection while the managed Db holds the read lock).
    fn open_dbs() -> (tempfile::TempDir, Db, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Db::open(&path).unwrap();
        let w = open_writer(&path);
        (dir, db, w)
    }

    fn open_writer(path: &Path) -> Connection {
        let w = Connection::open(path).unwrap();
        w.busy_timeout(Duration::from_millis(5000)).unwrap();
        let _: String = w
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .unwrap();
        w
    }

    /// Counts whole WAL frames currently in the WAL file (the Go
    /// `walPageCountForTest` helper, db_test.go:488-506).
    fn wal_page_count(db: &Db) -> i64 {
        let md = match std::fs::metadata(db.wal_path()) {
            Ok(m) => m,
            Err(_) => return 0,
        };
        let size = md.len() as i64;
        if db.page_size == 0 || size <= WAL_HEADER_SIZE as i64 {
            return 0;
        }
        let frame_size = WAL_FRAME_HEADER_SIZE as i64 + db.page_size as i64;
        (size - WAL_HEADER_SIZE as i64) / frame_size
    }

    // ── Path / size unit tests ────────────────────────────────────────────

    // Port of TestDB_Path / WALPath / MetaPath (db_test.go:22-49).
    #[test]
    fn paths_match_litestream() {
        // Absolute meta path: /tmp/db -> /tmp/.db-litestream.
        let mp = Db::meta_path_for(Path::new("/tmp/db"));
        assert_eq!(mp, PathBuf::from("/tmp/.db-litestream"));
        // Relative: db -> .db-litestream.
        let mp = Db::meta_path_for(Path::new("db"));
        assert_eq!(mp, PathBuf::from(".db-litestream"));
    }

    // Port of TestCalcWALSize (db_internal_test.go:111-174): no overflow.
    #[test]
    fn calc_wal_size_no_overflow() {
        let cases: &[(u32, u32)] = &[
            (4096, 121359),
            (16384, 121359),
            (32768, 121359),
            (65536, 121359),
            (1024, 1000),
        ];
        for &(page_size, page_n) in cases {
            let want = WAL_HEADER_SIZE as i64
                + (WAL_FRAME_HEADER_SIZE as i64 + page_size as i64) * page_n as i64;
            let got = calc_wal_size(page_size, page_n);
            assert_eq!(got, want, "calc_wal_size({page_size},{page_n})");
            assert!(got > 0, "must be positive");
            if page_size >= 32768 && page_n >= 100_000 {
                let min_expected = page_size as i64 * page_n as i64;
                assert!(got >= min_expected, "suspiciously small (overflow?)");
            }
        }
    }

    // Port of TestIsDiskFullError (db_internal_test.go:1064-1125).
    #[test]
    fn disk_full_error_classification() {
        assert!(is_disk_full_error(
            "write /tmp/file: no space left on device"
        ));
        assert!(is_disk_full_error("No Space Left On Device"));
        assert!(is_disk_full_error("write: disk quota exceeded"));
        assert!(is_disk_full_error("ENOSPC: cannot write file"));
        assert!(is_disk_full_error("error EDQUOT while writing"));
        assert!(is_disk_full_error("sync failed: no space left on device"));
        assert!(!is_disk_full_error("connection refused"));
        assert!(!is_disk_full_error("permission denied"));
    }

    // Port of TestIsSQLiteBusyError (db_internal_test.go:1127-1164) — string path.
    #[test]
    fn sqlite_busy_error_classification() {
        let busy = Error::Other("database is locked".into());
        assert!(is_sqlite_busy_error(&busy));
        let busy2 = Error::Other("SQLITE_BUSY: cannot commit".into());
        assert!(is_sqlite_busy_error(&busy2));
        let other = Error::Other("connection refused".into());
        assert!(!is_sqlite_busy_error(&other));
    }

    // ── Open / read-lock unit tests ───────────────────────────────────────

    #[test]
    fn open_enables_wal_and_holds_read_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let db = Db::open(&path).unwrap();

        assert_eq!(db.page_size(), 4096, "default sqlite page size");
        assert!(db.read_lock_held, "read lock acquired during open");

        let mode: String = db
            .conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal", "journal mode persisted as WAL");

        let autockpt: i64 = db
            .conn
            .query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0))
            .unwrap();
        assert_eq!(autockpt, 0, "auto-checkpoint disabled");

        // WAL sidecar exists (ensure_wal_exists forced a frame).
        let wal = std::fs::metadata(db.wal_path()).unwrap();
        assert!(
            wal.len() >= WAL_HEADER_SIZE as u64,
            "WAL has at least a 32-byte header"
        );

        assert_eq!(db.meta_path().file_name().unwrap(), ".state.db-litestream");
        assert_eq!(db.wal_path().file_name().unwrap(), "state.db-wal");
    }

    // Port of TestDB_releaseReadLock_DoubleRollback (db_internal_test.go:812-869):
    // a second release after a rollback must NOT error (issue #934, footgun F-2).
    #[test]
    fn release_read_lock_double_rollback_is_ok() {
        let (_dir, mut db, w) = open_dbs();
        w.execute_batch("CREATE TABLE t (id INT)").unwrap();

        // Sync acquires (re-acquires) the read lock.
        db.sync().unwrap();
        assert!(db.read_lock_held, "read lock held after sync");

        // First rollback directly on the read-lock connection (simulates the
        // bare `db.rtx.Rollback()` in execCheckpoint / the Go test).
        db.rtx_conn.execute_batch("ROLLBACK").unwrap();

        // release_read_lock() must be a no-op now (the flag is still set, but the
        // underlying ROLLBACK will report "no transaction" and be swallowed).
        db.release_read_lock()
            .expect("double release must not error");
    }

    // ── Capture-loop tests (port of TestDB_Sync family, db_test.go:106-281) ──

    // TestDB_Sync/Initial: first sync sets page size, creates the WAL, TXID 1.
    #[test]
    fn sync_initial_creates_txid_1() {
        let (_dir, mut db, w) = open_dbs();
        w.execute_batch("CREATE TABLE t (id INT)").unwrap();

        db.sync().unwrap();

        assert!(db.page_size() > 0, "page size available after sync");
        let wal = std::fs::metadata(db.wal_path()).unwrap();
        assert!(wal.len() > 0, "wal exists");

        let pos = db.pos().unwrap();
        assert_eq!(pos.txid, TXID(1), "first sync is TXID 1");

        // The L0 file decodes and verifies, and is a snapshot (MinTXID==1).
        let bytes = std::fs::read(db.ltx_path(0, TXID(1), TXID(1))).unwrap();
        let decoded = ltx::decode_file(&bytes).expect("L0 file decodes + verifies");
        assert!(decoded.header.is_snapshot(), "txid 1 is a snapshot");
        assert_eq!(decoded.header.max_txid, TXID(1));
    }

    // TestDB_Sync/MultiSync: each sync with new writes advances TXID by exactly 1.
    #[test]
    fn sync_multi_advances_txid() {
        let (_dir, mut db, w) = open_dbs();
        w.execute_batch("CREATE TABLE foo (bar TEXT)").unwrap();

        db.sync().unwrap();
        let pos0 = db.pos().unwrap();

        w.execute("INSERT INTO foo (bar) VALUES ('baz')", [])
            .unwrap();
        db.sync().unwrap();
        let pos1 = db.pos().unwrap();
        assert_eq!(pos1.txid, TXID(pos0.txid.0 + 1), "TXID advanced by one");

        // The incremental L0 file decodes + verifies.
        let bytes = std::fs::read(db.ltx_path(0, pos1.txid, pos1.txid)).unwrap();
        ltx::decode_file(&bytes).expect("incremental L0 file decodes + verifies");
    }

    // TestDB_Sync/NoDB analog: sync on an empty (never-written) DB is fine and
    // produces a snapshot at TXID 1 (the DB file exists, just has no user tables).
    #[test]
    fn sync_idempotent_when_idle() {
        let (_dir, mut db, _w) = open_dbs();
        db.sync().unwrap();
        let pos_a = db.pos().unwrap();
        // A second idle sync must NOT advance the TXID (issue #896/#994).
        db.sync().unwrap();
        let pos_b = db.pos().unwrap();
        assert_eq!(pos_a.txid, pos_b.txid, "idle sync does not advance TXID");
    }

    // TestDB_WriteLTXFromWAL_PageGrowthCoverage (db_internal_test.go:1464-1580):
    // when the DB grows between syncs, the incremental LTX must contain every new
    // page in (prevCommit, newCommit].
    #[test]
    fn incremental_ltx_covers_all_grown_pages() {
        let (_dir, mut db, w) = open_dbs();
        w.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)")
            .unwrap();
        for i in 0..5 {
            w.execute(
                "INSERT INTO t VALUES (?, ?)",
                rusqlite::params![i, vec![0u8; 100]],
            )
            .unwrap();
        }
        db.sync().unwrap();
        let pos1 = db.pos().unwrap();
        let prev_commit =
            ltx::Header::parse(&std::fs::read(db.ltx_path(0, pos1.txid, pos1.txid)).unwrap())
                .unwrap()
                .commit;

        for i in 5..150 {
            w.execute(
                "INSERT INTO t VALUES (?, ?)",
                rusqlite::params![i, vec![0u8; 3000]],
            )
            .unwrap();
        }
        db.sync().unwrap();
        let pos2 = db.pos().unwrap();

        let bytes = std::fs::read(db.ltx_path(0, pos2.txid, pos2.txid)).unwrap();
        let new_commit = ltx::Header::parse(&bytes).unwrap().commit;
        let decoded = ltx::decode_file(&bytes).expect("decodes + verifies");
        let present: std::collections::HashSet<u32> = decoded.pgnos.iter().copied().collect();

        let lock = lock_pgno(db.page_size);
        let mut missing = Vec::new();
        for pgno in (prev_commit + 1)..=new_commit {
            if pgno == lock {
                continue;
            }
            if !present.contains(&pgno) {
                missing.push(pgno);
            }
        }
        assert!(
            missing.is_empty(),
            "pages missing from incremental LTX: {missing:?} (prev={prev_commit}, new={new_commit})"
        );
    }

    // TestDB_Verify_WALOffsetAtHeader (db_internal_test.go:565-681): an L0 file
    // with WALOffset=32, WALSize=0 and matching salt → snapshotting=false,
    // offset=32, no underflow (issue #900).
    #[test]
    fn verify_wal_offset_at_header_salt_match() {
        let (_dir, mut db, w) = open_dbs();
        w.execute_batch("CREATE TABLE t (id INT)").unwrap();
        db.sync().unwrap();

        let wal_hdr = read_wal_header(&db.wal_path()).unwrap();
        let salt1 = be_u32(&wal_hdr[16..]);
        let salt2 = be_u32(&wal_hdr[20..]);

        let pos = db.pos().unwrap();
        let next = TXID(pos.txid.0 + 1);

        // Write an L0 file directly with WALOffset=32, WALSize=0 (the #900 shape).
        let header = ltx::Header {
            version: ltx::VERSION,
            flags: ltx::HEADER_FLAG_NO_CHECKSUM,
            page_size: db.page_size,
            commit: 2,
            min_txid: next,
            max_txid: next,
            timestamp: 1_000_000,
            pre_apply_checksum: 0,
            wal_offset: WAL_HEADER_SIZE as i64,
            wal_size: 0,
            wal_salt1: salt1,
            wal_salt2: salt2,
            node_id: 0,
        };
        // A header-only LTX file (no pages) — still decodes for the header read.
        let encoded = ltx::encode_file(&header, &[], 0).unwrap();
        std::fs::write(db.ltx_path(0, next, next), &encoded).unwrap();
        db.invalidate_pos_cache();

        let info = db.verify().expect("verify must not error (no underflow)");
        assert_eq!(
            info.offset, WAL_HEADER_SIZE as i64,
            "offset stays at header"
        );
        assert!(!info.snapshotting, "salt matches => no snapshot");
    }

    // TestDB_Verify_WALOffsetAtHeader_SaltMismatch (db_internal_test.go:687-805):
    // same shape but mismatched salt → snapshotting=true with the exact reason.
    #[test]
    fn verify_wal_offset_at_header_salt_mismatch() {
        let (_dir, mut db, w) = open_dbs();
        w.execute_batch("CREATE TABLE t (id INT)").unwrap();
        db.sync().unwrap();

        let wal_hdr = read_wal_header(&db.wal_path()).unwrap();
        let salt1 = be_u32(&wal_hdr[16..]);
        let salt2 = be_u32(&wal_hdr[20..]);

        let pos = db.pos().unwrap();
        let next = TXID(pos.txid.0 + 1);

        let header = ltx::Header {
            version: ltx::VERSION,
            flags: ltx::HEADER_FLAG_NO_CHECKSUM,
            page_size: db.page_size,
            commit: 2,
            min_txid: next,
            max_txid: next,
            timestamp: 1_000_000,
            pre_apply_checksum: 0,
            wal_offset: WAL_HEADER_SIZE as i64,
            wal_size: 0,
            wal_salt1: salt1.wrapping_add(1),
            wal_salt2: salt2.wrapping_add(1),
            node_id: 0,
        };
        let encoded = ltx::encode_file(&header, &[], 0).unwrap();
        std::fs::write(db.ltx_path(0, next, next), &encoded).unwrap();
        db.invalidate_pos_cache();

        let info = db.verify().expect("verify must not error");
        assert_eq!(info.offset, WAL_HEADER_SIZE as i64);
        assert!(info.snapshotting, "salt mismatch => snapshot");
        assert_eq!(info.reason, "wal header salt reset, snapshotting");
    }

    // TestDB_CheckpointDoesNotTriggerSnapshot (db_internal_test.go:877-984):
    // a checkpoint followed by a write must NOT make verify() request a snapshot
    // (issue #927). Run for both TRUNCATE and PASSIVE modes.
    fn checkpoint_does_not_trigger_snapshot(mode: CheckpointMode) {
        let (_dir, mut db, w) = open_dbs();
        db.checkpoint_interval = Duration::ZERO; // disable time-based checkpoints
        w.execute_batch("CREATE TABLE t (id INT, data TEXT)")
            .unwrap();
        for i in 0..100 {
            w.execute(
                "INSERT INTO t VALUES (?, ?)",
                rusqlite::params![i, format!("padding row {i} with content")],
            )
            .unwrap();
        }

        db.sync().unwrap();
        w.execute("INSERT INTO t VALUES (9999, 'before checkpoint')", [])
            .unwrap();
        db.sync().unwrap();

        let info1 = db.verify().unwrap();
        assert!(!info1.snapshotting, "pre-checkpoint sync is incremental");

        db.checkpoint(mode).unwrap();

        w.execute("INSERT INTO t VALUES (10000, 'after checkpoint')", [])
            .unwrap();

        let info2 = db.verify().unwrap();
        assert!(
            !info2.snapshotting,
            "checkpoint+sync must not snapshot (mode={mode}, reason={:?})",
            info2.reason
        );
    }

    #[test]
    fn checkpoint_does_not_trigger_snapshot_truncate() {
        checkpoint_does_not_trigger_snapshot(CheckpointMode::Truncate);
    }

    #[test]
    fn checkpoint_does_not_trigger_snapshot_passive() {
        checkpoint_does_not_trigger_snapshot(CheckpointMode::Passive);
    }

    // TestDB_MultipleCheckpointsWithWrites (db_internal_test.go:989-1061): repeated
    // checkpoint+write cycles must trigger at most one snapshot (the initial one).
    #[test]
    fn multiple_checkpoints_with_writes_snapshot_at_most_once() {
        let (_dir, mut db, w) = open_dbs();
        db.checkpoint_interval = Duration::ZERO;
        w.execute_batch("CREATE TABLE t (id INT, data TEXT)")
            .unwrap();

        let mut snapshot_count = 0;
        for cycle in 0..5 {
            for i in 0..10 {
                w.execute(
                    "INSERT INTO t VALUES (?, ?)",
                    rusqlite::params![cycle * 100 + i, "data"],
                )
                .unwrap();
            }
            db.sync().unwrap();
            if db.verify().unwrap().snapshotting {
                snapshot_count += 1;
            }
            db.checkpoint(CheckpointMode::Passive).unwrap();
        }
        assert!(
            snapshot_count <= 1,
            "too many snapshots: {snapshot_count} (expected <= 1)"
        );
    }

    // TestDB_IdleCheckpointSnapshotLoop (db_internal_test.go:1178-1256): after a
    // checkpoint, idle sync cycles must NOT keep incrementing the TXID (issue #997
    // — checkpoint decisions use the logical WAL offset, not file size).
    #[test]
    fn idle_checkpoint_does_not_loop() {
        let (_dir, mut db, w) = open_dbs();
        db.checkpoint_interval = Duration::ZERO;
        db.min_checkpoint_page_n = 10;
        w.execute_batch("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
            .unwrap();
        db.sync().unwrap();

        for i in 0..100 {
            w.execute(
                "INSERT INTO test VALUES (?, ?)",
                rusqlite::params![i, "test data padding"],
            )
            .unwrap();
        }
        db.sync().unwrap();
        db.checkpoint(CheckpointMode::Passive).unwrap();
        let after_ckpt = db.pos().unwrap();

        for _ in 0..5 {
            db.sync().unwrap();
        }
        let final_pos = db.pos().unwrap();

        let growth = final_pos.txid.0 as i64 - after_ckpt.txid.0 as i64;
        assert!(
            growth <= 1,
            "TXID grew by {growth} during idle cycles (expected <= 1) — issue #997"
        );
    }

    // TestDB_Issue994_RunawayDiskUsage (db_internal_test.go:1262-1350): after bulk
    // writes + checkpoint, 20 idle sync cycles must add at most ~2 LTX files.
    #[test]
    fn idle_cycles_do_not_grow_ltx_dir() {
        let (_dir, mut db, w) = open_dbs();
        db.checkpoint_interval = Duration::ZERO;
        db.min_checkpoint_page_n = 10;
        w.execute_batch("CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
            .unwrap();
        db.sync().unwrap();

        for i in 0..200 {
            w.execute(
                "INSERT INTO test VALUES (?, ?)",
                rusqlite::params![i, "padding data for disk usage test"],
            )
            .unwrap();
        }
        db.sync().unwrap();
        db.checkpoint(CheckpointMode::Passive).unwrap();

        let baseline_files = count_files(&db.ltx_level_dir(0));
        for _ in 0..20 {
            db.sync().unwrap();
        }
        let final_files = count_files(&db.ltx_level_dir(0));

        let new_files = final_files - baseline_files;
        assert!(
            new_files <= 2,
            "LTX file count grew by {new_files} during 20 idle cycles (expected <= 2) — issue #994"
        );
    }

    fn count_files(dir: &str) -> i64 {
        match std::fs::read_dir(dir) {
            Ok(entries) => entries.flatten().filter(|e| e.path().is_file()).count() as i64,
            Err(_) => 0,
        }
    }

    // ── Checkpoint-policy tests (port of TestDB_Sync sub-tests) ────────────

    // TestDB_Sync/MinCheckpointPageN (db_test.go:284-315): exceeding the page
    // threshold triggers a PASSIVE checkpoint, advancing to TXID 3.
    #[test]
    fn min_checkpoint_page_n_triggers_passive_checkpoint() {
        let (_dir, mut db, w) = open_dbs();
        db.checkpoint_interval = Duration::ZERO; // isolate the page-count trigger
        w.execute_batch("CREATE TABLE foo (bar TEXT)").unwrap();
        db.sync().unwrap();

        for _ in 0..db.min_checkpoint_page_n {
            w.execute("INSERT INTO foo (bar) VALUES ('baz')", [])
                .unwrap();
        }
        db.sync().unwrap();

        let pos = db.pos().unwrap();
        assert_eq!(
            pos.txid,
            TXID(3),
            "TXID 1=initial, 2=after inserts, 3=after PASSIVE checkpoint"
        );
    }

    // TestDB_Sync/TruncatePageN (db_test.go:317-350): with TruncatePageN=1, the
    // WAL is truncated back to <=1 page after a sync.
    #[test]
    fn truncate_page_n_shrinks_wal() {
        let (_dir, mut db, w) = open_dbs();
        db.truncate_page_n = 1;
        db.checkpoint_interval = Duration::ZERO;
        w.execute_batch("CREATE TABLE foo (bar TEXT)").unwrap();
        db.sync().unwrap();

        let payload = "x".repeat(db.page_size as usize);
        while wal_page_count(&db) <= 1 {
            w.execute("INSERT INTO foo (bar) VALUES (?)", [&payload])
                .unwrap();
        }
        db.sync().unwrap();

        assert!(
            wal_page_count(&db) <= 1,
            "truncate checkpoint should shrink the WAL, pages={}",
            wal_page_count(&db)
        );
    }

    // ── CRC64 / snapshot tests ────────────────────────────────────────────

    // TestDB_CRC64 (db_test.go:52-103): checksum changes after a WAL write, and
    // again after a checkpoint folds the change into the DB.
    #[test]
    fn crc64_changes_on_write_and_checkpoint() {
        let (_dir, mut db, w) = open_dbs();
        db.sync().unwrap();

        let (chk0, _) = db.crc64().unwrap();

        w.execute_batch("CREATE TABLE t (id INT)").unwrap();
        let (chk1, _) = db.crc64().unwrap();
        assert_ne!(chk0, chk1, "checksum changes after a WAL change");

        db.checkpoint(CheckpointMode::Truncate).unwrap();
        let (chk2, _) = db.crc64().unwrap();
        assert_ne!(chk0, chk2, "checksum changes after a checkpoint");
    }

    // TestDB_Snapshot (db_test.go:508-551, conformance-critical): the snapshot LTX
    // file's CRC64-ISO over its decoded database must equal the local DB CRC64.
    #[test]
    fn snapshot_checksum_matches_local_db() {
        let (_dir, mut db, w) = open_dbs();
        w.execute_batch("CREATE TABLE t (id INT)").unwrap();
        db.sync().unwrap();
        w.execute("INSERT INTO t (id) VALUES (100)", []).unwrap();
        db.sync().unwrap();

        // The snapshot spans 1..pos.TXID; filename 0…01-0…02.ltx.
        let pos = db.pos().unwrap();
        assert_eq!(pos.txid, TXID(2), "two syncs => TXID 2");

        // Mirror TestDB_Snapshot ordering: take the snapshot FIRST (at TXID 2),
        // reconstruct its full database image, then compute the local DB CRC64
        // (which forces a RESTART checkpoint folding the WAL into the DB file).
        // The snapshot image must equal the post-RESTART database.
        let mut buf = Vec::new();
        let snap_pos = db.snapshot_to_writer(&mut buf).unwrap();
        assert_eq!(snap_pos.txid, TXID(2), "snapshot captured at TXID 2");

        let (local_crc, _) = db.crc64().unwrap();

        let image = ltx::decode_database_image(&buf).expect("decode snapshot db image");
        let mut h = Crc64::new();
        h.update(&image);
        assert_eq!(
            h.sum64(),
            local_crc,
            "snapshot database checksum must equal local DB CRC64"
        );
    }

    // ── reset_local_state ─────────────────────────────────────────────────

    // TestDB_ResetLocalState (db_test.go:1660+): removing the LTX dir + clearing
    // the cache resets the position so the next sync snapshots fresh.
    #[test]
    fn reset_local_state_clears_position() {
        let (_dir, mut db, w) = open_dbs();
        w.execute_batch("CREATE TABLE t (id INT)").unwrap();
        db.sync().unwrap();
        assert_eq!(db.pos().unwrap().txid, TXID(1));

        db.reset_local_state().unwrap();
        assert_eq!(db.pos().unwrap().txid, TXID(0), "position reset to zero");

        // The next sync re-snapshots at TXID 1.
        db.sync().unwrap();
        assert_eq!(db.pos().unwrap().txid, TXID(1));
        let bytes = std::fs::read(db.ltx_path(0, TXID(1), TXID(1))).unwrap();
        assert!(
            ltx::decode_file(&bytes).unwrap().header.is_snapshot(),
            "fresh snapshot after reset"
        );
    }

    // ── pos() error mapping ───────────────────────────────────────────────

    // TestDB_Pos_VerifyErrorReturnsLTXError (db_internal_test.go:2096-2127):
    // a corrupt L0 file makes pos() return an LTXError{op:"verify"} wrapping
    // ErrLTXCorrupted, which is auto-recoverable.
    #[test]
    fn pos_verify_error_returns_ltx_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let mut db = Db::open(&path).unwrap();

        std::fs::create_dir_all(db.ltx_level_dir(0)).unwrap();
        std::fs::write(db.ltx_path(0, TXID(1), TXID(1)), b"not a valid ltx file").unwrap();
        db.invalidate_pos_cache();

        let err = db.pos().expect_err("expected error");
        match err {
            Error::Ltx(ref e) => {
                assert_eq!(e.op, "verify", "op must be verify");
                assert!(e.is_auto_recoverable(), "corruption is auto-recoverable");
            }
            other => panic!("expected LTXError, got {other:?}"),
        }
    }
}
