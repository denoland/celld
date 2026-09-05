// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This is V8-thread SQLite plumbing outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! Cell storage: SQLite backing for the Durable Object storage API.
//!
//! DO's `ctx.storage` is async in JS but synchronous underneath (local SQLite).
//! We expose synchronous Rust ops to V8 and wrap them in `async` in the JS
//! harness — same contract, no thread-hopping. Each cell is its OWN db file
//! (its own replicated, epoch-fenced bucket prefix), so the JS thread holds a
//! `scope -> Connection` map: `open` on activate, `close` on evict. The
//! `scope` column survives from the single-db era and still keys rows, but a
//! db now holds exactly one cell.
use anyhow::Context as _;
use rusqlite::{Connection, OptionalExtension};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(all(test, celld_internal_tests))]
pub(crate) fn poison_sql_for_test(scope: &str, error: &str) {
    sql_critical_errors(|errors| {
        errors
            .borrow_mut()
            .insert(scope.to_string(), error.to_string());
    });
}

/// The storage of every cell one isolate hosts.
///
/// This state was thread-local, which was correct while a cell isolate *was*
/// a thread. It is not correct once a cell event is driven by a tokio task:
/// its turns run on whatever worker holds the isolate, so state keyed by
/// thread would be invisible to the next turn. The state therefore belongs
/// to the isolate, which is the thing a cell truly lives in.
///
/// Nothing here is synchronised, and it does not need to be: a turn holds
/// the isolate lock, so exactly one thread can reach these maps at a time.
/// That is the same guarantee the thread gave, obtained from the isolate
/// instead.
#[derive(Default)]
pub struct Cells {
    dbs: RefCell<HashMap<String, OpenCell>>,
    active_alarms: RefCell<HashMap<String, ActiveAlarm>>,
    /// Committed alarm mutations no turn has taken yet, `-1` for a delete.
    /// The turn that committed one drains it (`take_alarm_moves`) and the
    /// drive reports it to the host: the alarm move is a turn *output*,
    /// like the ops a turn starts, not a side channel to poll.
    alarm_moves: RefCell<HashMap<String, i64>>,
    alarm_dirty: RefCell<HashSet<String>>,
    sync_list_cursors: RefCell<HashMap<u64, SyncListCursor>>,
    sql_cursors: RefCell<HashMap<u64, SqlCursor>>,
    sql_statement_caches: RefCell<HashMap<String, SqlStatementCache>>,
    sql_critical_errors: RefCell<HashMap<String, String>>,
    /// The schema cookie last read for a cell, and what it was read against.
    schema_cookies: RefCell<HashMap<String, SchemaCookie>>,
}

/// The database and the ownership epoch that authorized its activation.
///
/// These values are one residency invariant. Keeping them in one map entry
/// prevents asynchronous object-store work from recovering an epoch through a
/// later ownership lookup after a takeover.
struct OpenCell {
    connection: Connection,
    epoch: u64,
    backing: StorageBacking,
    persisted_position: u64,
    /// The committed-write position the first event of this activation
    /// started at. celld's own activation writes sit at or below it, so a
    /// read-only answer reports what it observed only above it, and a reader
    /// on a cell no handler has written asks for no proof.
    published_position: Option<u64>,
}

enum StorageBacking {
    File {
        path: String,
        root_scope: String,
        facet_path: Vec<String>,
    },
    Embedded {
        root_path: String,
        root_scope: String,
        facet_path: Vec<String>,
        /// The root cell's gate sample, see [`StorageIdentity::root_position`].
        root_position: u64,
        root_observed: Option<u64>,
    },
}

#[derive(Clone)]
pub(crate) struct StorageIdentity {
    pub root_path: String,
    pub root_scope: String,
    pub facet_path: Vec<String>,
    pub epoch: u64,
    /// A committed-write position of `root_scope`, sampled in the isolate that
    /// holds the root database.
    ///
    /// A facet is not a cell: its writes land in the root cell's database, so
    /// an egress it raises must pass the root cell's output gate. The facet's
    /// isolate holds no connection to the root database and therefore cannot
    /// name a position in the root cell's space, so the sample travels with
    /// the facet.
    ///
    /// The sample is a lower bound, and it stays one: [`flush_embedded`] copies
    /// the facet's image on a second connection to the root database file, so
    /// the root cell's own connection never counts those frames and a re-sample
    /// after the copy reports the same number. Order covers them, not
    /// arithmetic. The copy commits to the root database file before the egress
    /// takes its ticket, and the replicator answers a ticket with a sync that
    /// started after it, so the sync that proves this ticket uploads a file
    /// that already holds the facet's write.
    ///
    /// The position is therefore a coverage claim, not a wait condition: the
    /// core acknowledges only a proof that reaches it, so a lower bound makes
    /// the core credit itself with less coverage than the sync obtained, never
    /// with more.
    pub root_position: u64,
    /// What a read-only answer of `root_scope` reports as observed at the same
    /// moment. A facet's read-only egress carries it, so a commit the facet
    /// can reveal is held exactly as the root cell's own reader holds it.
    pub root_observed: Option<u64>,
}

/// The cell an embedded facet's outbound effects gate against, and the sample
/// that names what they can reveal. See [`StorageIdentity::root_position`].
#[derive(Clone)]
pub(crate) struct RootGate {
    pub cell: String,
    /// The epoch the root cell was resident at when the sample was taken. The
    /// core refuses a ticket that names another epoch, so a facet image that
    /// outlived a reset cannot acknowledge a discarded write.
    pub epoch: u64,
    pub position: u64,
    pub observed: Option<u64>,
}

/// A cached `PRAGMA schema_version`, and the two things that can invalidate
/// it.
///
/// The pragma is a real query: it takes a shared pager lock, which is an
/// `fcntl` on the database file. `write_position` samples it twice per cell
/// event — once before the handler and once to take the write delta — so a
/// handler that touches no storage at all still paid two locked reads. A
/// profile of `/c/hello` found them; they were the single largest term the
/// stateless path does not have.
///
/// The cookie cannot move unless a statement runs on the connection, so a
/// sample that sees neither a new prepare nor a new completed change can
/// reuse the last value. Both counters are cheap: one atomic load and one
/// `sqlite3_total_changes64` call, neither of which touches the file.
struct SchemaCookie {
    cookie: u64,
    changes: u64,
    prepares: u64,
}

/// Statements prepared across every cell in the process.
///
/// Bumped by the SQL authorizer, which SQLite calls while preparing each
/// statement, so it counts exactly the events that can move a schema
/// cookie. Process-wide rather than per cell because the authorizer is a
/// bare function with no scope in hand — which only costs a cell an extra
/// pragma when *another* cell ran SQL, and never returns a stale answer.
static SQL_PREPARES: AtomicU64 = AtomicU64::new(0);

/// The KV statements. One text per operation, shared by every call site, so
/// the connection's statement cache holds one compiled statement for each
/// and a request pays no SQL parse for its gets and puts: at 7,000 writes/s
/// the fleet profile put ~7% of a node's CPU in `sqlite3Prepare` for these
/// three texts alone.
const KV_GET_SQL: &str = "SELECT v FROM _cf_KV WHERE scope=?1 AND k=?2";
const KV_PUT_SQL: &str = "INSERT INTO _cf_KV(scope,k,v) VALUES(?1,?2,?3) \
     ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v";
const KV_DELETE_SQL: &str = "DELETE FROM _cf_KV WHERE scope=?1 AND k=?2";

/// The number of SQL compilations so far, for the internal test hooks: a
/// warm cell's gets and puts must not move it.
#[cfg(celld_internal_tests)]
pub fn sql_prepares_for_test() -> u64 {
    SQL_PREPARES.load(Ordering::Relaxed)
}

/// SAFETY: the cursors and statement caches hold raw SQLite handles, so this
/// is not `Send` by inference. It is `Send` in fact: the handles move
/// between threads only with the isolate that owns them, and a thread
/// reaches them only while it holds that isolate's lock.
unsafe impl Send for Cells {}

thread_local! {
    /// The cells of the isolate this thread currently holds, or null.
    ///
    /// A pointer rather than the state itself: the state belongs to the
    /// isolate and only its address is thread business. `install` sets it
    /// for one turn, so the pointer is live exactly when a turn is running.
    static CURRENT_CELLS: std::cell::Cell<*const Cells> =
        const { std::cell::Cell::new(std::ptr::null()) };
    #[cfg(celld_internal_tests)]
    static THREAD_OWNS_CELLS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl Cells {
    /// Make these cells reachable by the storage API for one turn.
    ///
    /// Restores whatever was installed before, so a nested entry — which the
    /// blocking cell run loop still does — leaves the outer turn's cells in
    /// place on the way out.
    pub fn install(&self) -> Installed {
        #[cfg(celld_internal_tests)]
        if THREAD_OWNS_CELLS.get() {
            return Installed(CURRENT_CELLS.get());
        }
        Installed(CURRENT_CELLS.replace(self))
    }
}

pub struct Installed(*const Cells);

impl Drop for Installed {
    fn drop(&mut self) {
        CURRENT_CELLS.set(self.0);
    }
}

#[cfg(celld_internal_tests)]
pub fn install_for_test() {
    thread_local! {
        static OWN: Cells = Cells::default();
    }
    OWN.with(|cells| CURRENT_CELLS.set(cells));
    THREAD_OWNS_CELLS.set(true);
}

/// Reach the current isolate's cells.
fn cells<T>(f: impl FnOnce(&Cells) -> T) -> T {
    let cells = CURRENT_CELLS.get();
    assert!(!cells.is_null(), "cell storage reached outside a turn");
    // SAFETY: non-null only between `Cells::install` and dropping its guard,
    // which spans one turn and therefore holds the isolate lock. The isolate
    // owns the state and outlives the turn.
    f(unsafe { &*cells })
}

// One accessor per map, so a call site names the state it wants and nothing
// else. Each takes the closure the thread-local `with` took, unchanged.
fn dbs<T>(f: impl FnOnce(&RefCell<HashMap<String, OpenCell>>) -> T) -> T {
    cells(|c| f(&c.dbs))
}

fn active_alarms<T>(f: impl FnOnce(&RefCell<HashMap<String, ActiveAlarm>>) -> T) -> T {
    cells(|c| f(&c.active_alarms))
}

fn alarm_moves<T>(f: impl FnOnce(&RefCell<HashMap<String, i64>>) -> T) -> T {
    cells(|c| f(&c.alarm_moves))
}

fn alarm_dirty<T>(f: impl FnOnce(&RefCell<HashSet<String>>) -> T) -> T {
    cells(|c| f(&c.alarm_dirty))
}

fn sync_list_cursors<T>(f: impl FnOnce(&RefCell<HashMap<u64, SyncListCursor>>) -> T) -> T {
    cells(|c| f(&c.sync_list_cursors))
}

fn sql_cursors<T>(f: impl FnOnce(&RefCell<HashMap<u64, SqlCursor>>) -> T) -> T {
    cells(|c| f(&c.sql_cursors))
}

fn sql_statement_caches<T>(f: impl FnOnce(&RefCell<HashMap<String, SqlStatementCache>>) -> T) -> T {
    cells(|c| f(&c.sql_statement_caches))
}

fn sql_critical_errors<T>(f: impl FnOnce(&RefCell<HashMap<String, String>>) -> T) -> T {
    cells(|c| f(&c.sql_critical_errors))
}

const SQL_STATEMENT_CACHE_MAX_SIZE: usize = 1024 * 1024;

#[derive(Clone, Copy)]
struct ActiveAlarm {
    fired_at_ms: i64,
    generation: Option<i64>,
}

struct SyncListCursor {
    scope: String,
    database: *mut rusqlite::ffi::sqlite3,
    statement: *mut rusqlite::ffi::sqlite3_stmt,
}

struct SqlCursor {
    scope: String,
    database: *mut rusqlite::ffi::sqlite3,
    statement: *mut rusqlite::ffi::sqlite3_stmt,
    changes_before: u64,
    cache_query: Option<Arc<str>>,
}

#[derive(Default)]
struct SqlStatementCache {
    entries: HashMap<Arc<str>, CachedSqlStatement>,
    total_size: usize,
    clock: u64,
}

struct CachedSqlStatement {
    statement: *mut rusqlite::ffi::sqlite3_stmt,
    use_count: u64,
    size: usize,
    last_used: u64,
}

impl Drop for CachedSqlStatement {
    fn drop(&mut self) {
        if !self.statement.is_null() {
            // SAFETY: an idle cache slot exclusively owns its statement.
            unsafe {
                rusqlite::ffi::sqlite3_finalize(self.statement);
            }
        }
    }
}

impl Drop for SyncListCursor {
    fn drop(&mut self) {
        // SAFETY: the statement is prepared by sync_list_start(), stored in
        // exactly one cursor, and finalized before its owning connection is
        // removed from DBS.
        unsafe {
            rusqlite::ffi::sqlite3_finalize(self.statement);
        }
    }
}

impl Drop for SqlCursor {
    fn drop(&mut self) {
        if self.statement.is_null() {
            return;
        }
        let statement = std::mem::replace(&mut self.statement, std::ptr::null_mut());
        if let Some(query) = self.cache_query.take() {
            recycle_sql_statement(&self.scope, &query, statement);
        } else {
            // SAFETY: this cursor exclusively owns its one-off statement.
            unsafe {
                rusqlite::ffi::sqlite3_finalize(statement);
            }
        }
    }
}

#[doc(hidden)]
pub fn schema(c: &Connection) -> anyhow::Result<()> {
    // OFF while schema() runs, then NORMAL for the steady state. schema()
    // writes only pragmas and celld's own tables, never user data, so a
    // crash mid-schema loses nothing a re-open does not rebuild. The point
    // is the fsync the next line would otherwise force: switching a fresh
    // database to WAL is durable, so SQLite fsyncs the header under every
    // synchronous level except OFF — and that lone fsync, serialized by the
    // filesystem journal, capped cold-cell creation near 1,000/s at idle
    // CPU while warm cells ran at 22k (engine/pathological-load.md).
    c.pragma_update(None, "synchronous", "OFF")?;
    c.pragma_update(None, "journal_mode", "WAL")?;
    // celld shipped these as `kv`, `alarms` and `cell_metadata` until
    // 2026-08-06 -- names a userland table can collide with, and which a
    // library that drops everything except `_cf_*` will happily delete
    // (denoland/celld#122). Cloudflare's own names are `_cf_KV`, `_cf_ALARM`
    // and `_cf_METADATA`; adopt them and carry existing cells across. The
    // rename is a metadata-only operation, and it runs before the CREATE
    // statements so an already-migrated cell falls straight through.
    for (legacy, current) in [
        ("kv", "_cf_KV"),
        ("alarms", "_cf_ALARM"),
        ("cell_metadata", "_cf_METADATA"),
    ] {
        let exists = |name: &str| -> rusqlite::Result<bool> {
            c.query_row(
                "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1",
                [name],
                |_| Ok(()),
            )
            .optional()
            .map(|found| found.is_some())
        };
        if exists(legacy)? && !exists(current)? {
            c.execute_batch(&format!("ALTER TABLE {legacy} RENAME TO {current}"))?;
        }
    }
    c.execute(
        "CREATE TABLE IF NOT EXISTS _cf_KV \
         (scope TEXT, k TEXT, v TEXT, PRIMARY KEY(scope,k))",
        [],
    )?;
    c.execute(
        "CREATE TABLE IF NOT EXISTS _cf_ALARM \
         (scope TEXT PRIMARY KEY, at_ms INTEGER, retry INTEGER DEFAULT 0, \
          counted_retry INTEGER NOT NULL DEFAULT 0, \
          generation INTEGER NOT NULL DEFAULT 0)",
        [],
    )?;
    c.execute(
        "CREATE TABLE IF NOT EXISTS _cf_METADATA \
         (scope TEXT PRIMARY KEY, actor_name TEXT)",
        [],
    )?;
    c.execute(
        "CREATE TABLE IF NOT EXISTS _cf_FACETS \
         (scope TEXT NOT NULL, path TEXT NOT NULL, image BLOB NOT NULL, \
          PRIMARY KEY(scope,path)) WITHOUT ROWID",
        [],
    )?;
    let alarm_columns = {
        let mut statement = c.prepare("PRAGMA table_info(_cf_ALARM)")?;
        let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
        columns.collect::<rusqlite::Result<Vec<_>>>()?
    };
    if !alarm_columns.iter().any(|column| column == "generation") {
        c.execute(
            "ALTER TABLE _cf_ALARM ADD COLUMN generation INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    if !alarm_columns.iter().any(|column| column == "counted_retry") {
        c.execute(
            "ALTER TABLE _cf_ALARM ADD COLUMN counted_retry INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    // NORMAL, not the FULL default: with WAL, commits then skip the
    // per-commit WAL fsync (measured 1.4ms -> 19us per put on cloud
    // disks; the fsync was the entire single-cell write budget).
    // Process crashes lose nothing. An OS/power crash may lose the
    // last commits locally — celld's durability boundary for node
    // loss is LTX replication either way, and replicated-WAL setups
    // conventionally run NORMAL.
    c.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(())
}

thread_local! {
    /// True only while a statement the application authored is running.
    /// The `_cf_` reservation exists to keep application SQL out of celld's
    /// own tables, and celld's tables now carry that prefix
    /// (denoland/celld#122), so the engine's own statements must not be
    /// judged by it. Everything else the authorizer denies -- pragmas,
    /// ATTACH, load_extension -- stays denied for both.
    static USER_SQL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Run `callback` with application-SQL restrictions in force.
fn as_user_sql<T>(callback: impl FnOnce() -> T) -> T {
    USER_SQL.with(|flag| flag.set(true));
    let result = callback();
    USER_SQL.with(|flag| flag.set(false));
    result
}

fn is_reserved_sql_name(name: &str) -> bool {
    USER_SQL.with(std::cell::Cell::get)
        && (name
            .get(..4)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("_cf_"))
            // The ltx replicator's control tables predate the `_cf_` prefix
            // convention; application SQL that touches them breaks WAL
            // capture for the cell, so they are reserved the same way.
            || celld_ltx::db::is_control_table(name))
}

fn valid_sql_boolean(value: &str) -> bool {
    let value = value.trim();
    let value = if value.len() >= 2
        && ((value.starts_with('\'') && value.ends_with('\''))
            || (value.starts_with('"') && value.ends_with('"')))
    {
        &value[1..value.len() - 1]
    } else {
        value
    };
    matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "false" | "yes" | "no" | "on" | "off" | "1" | "0"
    )
}

fn valid_sql_i32(value: &str) -> bool {
    value.parse::<i32>().is_ok()
        || value
            .strip_prefix("0x")
            .or_else(|| value.strip_prefix("0X"))
            .and_then(|digits| u32::from_str_radix(digits, 16).ok())
            .is_some_and(|number| number <= i32::MAX as u32)
}

fn allowed_sql_pragma(name: &str, value: Option<&str>) -> bool {
    let name = name.to_ascii_lowercase();
    match name.as_str() {
        // `schema_version` (read-only) is a deliberate one-pragma divergence
        // from workerd's list: `write_position` reads the schema cookie on
        // this connection under the normal authorizer, because toggling the
        // authorizer off and on around the read expires prepared statements
        // and must not run concurrently with an active one — and
        // `write_position` is called from egress paths that can overlap a
        // cursor.
        "data_version" | "table_list" | "schema_version" => value.is_none(),
        "case_sensitive_like"
        | "foreign_keys"
        | "defer_foreign_keys"
        | "ignore_check_constraints"
        | "legacy_alter_table"
        | "recursive_triggers"
        | "reverse_unordered_selects" => value.is_none_or(valid_sql_boolean),
        "table_info" | "table_xinfo" | "foreign_key_list" | "index_info" | "index_list"
        | "index_xinfo" => value.is_some_and(|name| !is_reserved_sql_name(name)),
        "foreign_key_check" => value.is_none_or(|name| !is_reserved_sql_name(name)),
        "quick_check" => {
            value.is_none_or(|value| value.parse::<u32>().is_ok() || !is_reserved_sql_name(value))
        }
        "optimize" => value.is_none_or(valid_sql_i32),
        _ => false,
    }
}

fn authorize_sql(context: rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization {
    use rusqlite::hooks::{AuthAction, Authorization};

    // SQLite calls this while preparing a statement, which makes it the one
    // place that sees everything able to move a schema cookie. See
    // `SchemaCookie`.
    SQL_PREPARES.fetch_add(1, Ordering::Relaxed);

    let prohibited = match context.action {
        // The Attach deny is also the only thing blocking VACUUM and VACUUM
        // INTO: SQLite emits no distinct authorizer action for VACUUM and
        // implements it via an internal ATTACH, which lands here. Relaxing
        // Attach (or raising SQLITE_LIMIT_ATTACHED from 0) silently unblocks
        // whole-file rewrites against a replicating database and, via VACUUM
        // INTO, arbitrary file writes from cell SQL.
        AuthAction::Transaction { .. }
        | AuthAction::Savepoint { .. }
        | AuthAction::Attach { .. }
        | AuthAction::Detach { .. }
        | AuthAction::CreateTempIndex { .. }
        | AuthAction::CreateTempTable { .. }
        | AuthAction::CreateTempTrigger { .. }
        | AuthAction::CreateTempView { .. }
        | AuthAction::DropTempIndex { .. }
        | AuthAction::DropTempTable { .. }
        | AuthAction::DropTempTrigger { .. }
        | AuthAction::DropTempView { .. } => true,
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } => !allowed_sql_pragma(pragma_name, pragma_value),
        // A virtual-table module runs native code, so each module needs an
        // explicit entry in this allowlist. vec0 uses ordinary shadow tables,
        // and the reserved-name checks below apply to their table actions.
        AuthAction::CreateVtable { module_name, .. } => {
            !module_name.eq_ignore_ascii_case("fts5")
                && !module_name.eq_ignore_ascii_case("fts5vocab")
                && !module_name.eq_ignore_ascii_case("vec0")
        }
        AuthAction::Function { function_name } => matches!(
            function_name.to_ascii_lowercase().as_str(),
            "load_extension" | "sqlite_version" | "sqlite_source_id"
        ),
        _ => false,
    };
    if prohibited {
        return Authorization::Deny;
    }

    let reserved = match context.action {
        AuthAction::Unknown { arg1, arg2, .. } => {
            arg1.is_some_and(is_reserved_sql_name) || arg2.is_some_and(is_reserved_sql_name)
        }
        AuthAction::CreateIndex {
            index_name,
            table_name,
        }
        | AuthAction::CreateTempIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropIndex {
            index_name,
            table_name,
        }
        | AuthAction::DropTempIndex {
            index_name,
            table_name,
        } => is_reserved_sql_name(index_name) || is_reserved_sql_name(table_name),
        AuthAction::CreateTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::CreateTempTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::DropTrigger {
            trigger_name,
            table_name,
        }
        | AuthAction::DropTempTrigger {
            trigger_name,
            table_name,
        } => is_reserved_sql_name(trigger_name) || is_reserved_sql_name(table_name),
        AuthAction::CreateTable { table_name }
        | AuthAction::CreateTempTable { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::DropTable { table_name }
        | AuthAction::DropTempTable { table_name }
        | AuthAction::Insert { table_name } => is_reserved_sql_name(table_name),
        // ANALYZE is allowed on every table, reserved ones included, which is
        // what workerd does (`sqlite.c++:1104`) and for its reason: a bare
        // `ANALYZE` analyzes all tables, and `PRAGMA optimize` issues one, so
        // name-checking it denies both outright. What ANALYZE writes is
        // `sqlite_stat1` — index names and row counts, not rows — so a
        // reserved table leaks metadata here and no data.
        AuthAction::Analyze { .. } => false,
        AuthAction::CreateView { view_name }
        | AuthAction::CreateTempView { view_name }
        | AuthAction::DropView { view_name }
        | AuthAction::DropTempView { view_name } => is_reserved_sql_name(view_name),
        AuthAction::Read {
            table_name,
            column_name,
        }
        | AuthAction::Update {
            table_name,
            column_name,
        } => is_reserved_sql_name(table_name) || is_reserved_sql_name(column_name),
        AuthAction::AlterTable {
            database_name,
            table_name,
        } => is_reserved_sql_name(database_name) || is_reserved_sql_name(table_name),
        AuthAction::CreateVtable {
            table_name,
            module_name: _,
        }
        | AuthAction::DropVtable {
            table_name,
            module_name: _,
        } => is_reserved_sql_name(table_name),
        AuthAction::Reindex { index_name } => is_reserved_sql_name(index_name),
        _ => false,
    } || context.database_name.is_some_and(is_reserved_sql_name)
        || context.accessor.is_some_and(is_reserved_sql_name);

    if reserved {
        Authorization::Deny
    } else {
        Authorization::Allow
    }
}

fn without_sql_authorizer<T>(connection: &Connection, callback: impl FnOnce() -> T) -> T {
    use rusqlite::hooks::{AuthContext, Authorization};

    connection.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    let result = callback();
    connection.authorizer(Some(authorize_sql));
    result
}

fn without_sql_authorizer_mut<T>(
    connection: &mut Connection,
    callback: impl FnOnce(&mut Connection) -> T,
) -> T {
    use rusqlite::hooks::{AuthContext, Authorization};

    connection.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    let result = callback(connection);
    connection.authorizer(Some(authorize_sql));
    result
}

/// Install sqlite-vec on one cell connection. Other SQLite users in the
/// process do not need the module and must not inherit it through a global
/// `sqlite3_auto_extension` hook.
fn register_vec0_extension(connection: &Connection) -> anyhow::Result<()> {
    type ExtensionInit = unsafe extern "C" fn(
        *mut rusqlite::ffi::sqlite3,
        *mut *mut std::os::raw::c_char,
        *const rusqlite::ffi::sqlite3_api_routines,
    ) -> std::os::raw::c_int;

    // sqlite-vec exposes the symbol as a zero-argument function, although the
    // linked C entry point has SQLite's extension-init signature.
    let initialize = unsafe {
        std::mem::transmute::<*const (), ExtensionInit>(sqlite_vec::sqlite3_vec_init as *const ())
    };
    let mut error = std::ptr::null_mut();
    // SAFETY: `connection` owns a live SQLite handle. sqlite-vec builds with
    // `SQLITE_CORE`, so the entry point uses the linked SQLite API directly
    // and does not read the null extension API table.
    let result = unsafe {
        initialize(
            connection.handle(),
            &mut error,
            std::ptr::null::<rusqlite::ffi::sqlite3_api_routines>(),
        )
    };
    if result == rusqlite::ffi::SQLITE_OK {
        return Ok(());
    }

    let message = if error.is_null() {
        format!("SQLite error {result}")
    } else {
        // SAFETY: an extension error is a NUL-terminated SQLite allocation.
        let message = unsafe { std::ffi::CStr::from_ptr(error) }
            .to_string_lossy()
            .into_owned();
        // SAFETY: sqlite-vec allocated the message with sqlite3_mprintf.
        unsafe { rusqlite::ffi::sqlite3_free(error.cast()) };
        message
    };
    anyhow::bail!("sqlite-vec initialization failed: {message}")
}

/// Open (or replace) the connection for `scope`'s db file.
pub fn open(scope: &str, path: &str) -> anyhow::Result<()> {
    open_with_compat(scope, path, false)
}

pub fn open_with_compat(scope: &str, path: &str, sqlite_vec: bool) -> anyhow::Result<()> {
    open_at_epoch(scope, path, 1, None, sqlite_vec)
}

/// Open a production cell database with the epoch that authorized the
/// activation. A paged restore leaves the file sparse behind a fault-in VFS,
/// so the activation's `vfs` must reach every connection: a plain open reads
/// the holes as zeros and SQLite reports a malformed database.
pub(crate) fn open_at_epoch(
    scope: &str,
    path: &str,
    epoch: u64,
    vfs: Option<&str>,
    sqlite_vec: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(epoch > 0, "a cell activation epoch must be positive");
    let connection = match vfs {
        Some(vfs) => {
            Connection::open_with_flags_and_vfs(path, rusqlite::OpenFlags::default(), vfs)?
        }
        None => Connection::open(path)?,
    };
    finish_open(
        scope,
        connection,
        epoch,
        sqlite_vec,
        StorageBacking::File {
            path: path.to_string(),
            root_scope: scope.to_string(),
            facet_path: Vec::new(),
        },
    )
}

#[cfg(celld_internal_tests)]
pub fn open_with_fault_vfs_for_test(scope: &str, path: &str) -> anyhow::Result<()> {
    finish_open(
        scope,
        crate::fault::open_database(path)?,
        1,
        false,
        StorageBacking::File {
            path: path.to_string(),
            root_scope: scope.to_string(),
            facet_path: Vec::new(),
        },
    )
}

fn finish_open(
    scope: &str,
    c: Connection,
    epoch: u64,
    sqlite_vec: bool,
    backing: StorageBacking,
) -> anyhow::Result<()> {
    if sqlite_vec {
        register_vec0_extension(&c)?;
    }
    schema(&c)?;
    // Match Workerd's SQLite security budgets. Applying native connection
    // limits once here avoids request-path parsing and keeps rejected queries
    // from consuming unbounded parser, VDBE, or expression resources.
    // SAFETY: `c` owns a live SQLite connection for the duration of this call.
    unsafe {
        let database = c.handle();
        for (category, limit) in [
            (rusqlite::ffi::SQLITE_LIMIT_LENGTH, 2_200_000),
            (rusqlite::ffi::SQLITE_LIMIT_SQL_LENGTH, 100_000),
            (rusqlite::ffi::SQLITE_LIMIT_COLUMN, 100),
            (rusqlite::ffi::SQLITE_LIMIT_EXPR_DEPTH, 100),
            (rusqlite::ffi::SQLITE_LIMIT_COMPOUND_SELECT, 5),
            (rusqlite::ffi::SQLITE_LIMIT_VDBE_OP, 25_000),
            (rusqlite::ffi::SQLITE_LIMIT_FUNCTION_ARG, 127),
            (rusqlite::ffi::SQLITE_LIMIT_ATTACHED, 0),
            (rusqlite::ffi::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 50),
            (rusqlite::ffi::SQLITE_LIMIT_VARIABLE_NUMBER, 100),
            (rusqlite::ffi::SQLITE_LIMIT_TRIGGER_DEPTH, 10),
            (rusqlite::ffi::SQLITE_LIMIT_WORKER_THREADS, 0),
        ] {
            rusqlite::ffi::sqlite3_limit(database, category, limit);
        }
    }
    c.authorizer(Some(authorize_sql));
    // rusqlite's default holds 16 statements; the KV texts plus a cell's
    // few hot user statements fit in 64 without evicting each other.
    c.set_prepared_statement_cache_capacity(64);
    close_sync_list_cursors(scope);
    close_sql_cursors(scope);
    close_sql_statement_cache(scope);
    sql_critical_errors(|errors| errors.borrow_mut().remove(scope));
    let persisted_position = total_changes(&c) + schema_version(&c);
    dbs(|d| {
        d.borrow_mut().insert(
            scope.to_string(),
            OpenCell {
                connection: c,
                epoch,
                backing,
                persisted_position,
                published_position: None,
            },
        )
    });
    Ok(())
}

/// Drop `scope`'s connection (evict) so the replicator can release the
/// file.
pub fn close(scope: &str) {
    close_sync_list_cursors(scope);
    close_sql_cursors(scope);
    close_sql_statement_cache(scope);
    sql_critical_errors(|errors| errors.borrow_mut().remove(scope));
    cells(|c| c.schema_cookies.borrow_mut().remove(scope));
    dbs(|d| d.borrow_mut().remove(scope));
    active_alarms(|alarms| alarms.borrow_mut().remove(scope));
    alarm_moves(|moves| {
        moves.borrow_mut().remove(scope);
    });
    alarm_dirty(|dirty| {
        dirty.borrow_mut().remove(scope);
    });
}

fn with<T>(scope: &str, f: impl FnOnce(&Connection) -> T) -> Option<T> {
    dbs(|d| d.borrow().get(scope).map(|cell| f(&cell.connection)))
}

fn with_mut<T>(scope: &str, f: impl FnOnce(&mut Connection) -> T) -> Option<T> {
    dbs(|d| {
        d.borrow_mut()
            .get_mut(scope)
            .map(|cell| f(&mut cell.connection))
    })
}

/// Return the epoch installed with the active cell database.
pub(crate) fn activation_epoch(scope: &str) -> Option<u64> {
    dbs(|d| d.borrow().get(scope).map(|cell| cell.epoch))
}

/// The identity a facet of `scope` inherits, including the root cell's gate
/// sample. A file-backed cell samples its own connection here, which is the
/// only place a position in the root cell's space can be read; an embedded
/// scope passes on the sample it was given, because its isolate cannot take
/// one.
pub(crate) fn storage_identity(scope: &str) -> Option<StorageIdentity> {
    let sample = write_position(scope);
    let observed = observed_position(scope, sample);
    dbs(|databases| {
        databases.borrow().get(scope).map(|cell| {
            let (root_path, root_scope, facet_path, root_position, root_observed) =
                match &cell.backing {
                    StorageBacking::File {
                        path,
                        root_scope,
                        facet_path,
                    } => (
                        path.clone(),
                        root_scope.clone(),
                        facet_path.clone(),
                        sample.unwrap_or(0),
                        observed,
                    ),
                    StorageBacking::Embedded {
                        root_path,
                        root_scope,
                        facet_path,
                        root_position,
                        root_observed,
                    } => (
                        root_path.clone(),
                        root_scope.clone(),
                        facet_path.clone(),
                        *root_position,
                        *root_observed,
                    ),
                };
            StorageIdentity {
                root_path,
                root_scope,
                facet_path,
                epoch: cell.epoch,
                root_position,
                root_observed,
            }
        })
    })
}

pub(crate) fn is_embedded(scope: &str) -> bool {
    dbs(|databases| {
        databases
            .borrow()
            .get(scope)
            .is_some_and(|cell| matches!(cell.backing, StorageBacking::Embedded { .. }))
    })
}

/// The gate an event of `scope` must pass, when `scope` is an embedded facet.
/// `None` for a cell, which gates against itself.
pub(crate) fn embedded_root_gate(scope: &str) -> Option<RootGate> {
    dbs(|databases| {
        let databases = databases.borrow();
        let cell = databases.get(scope)?;
        let StorageBacking::Embedded {
            root_scope,
            root_position,
            root_observed,
            ..
        } = &cell.backing
        else {
            return None;
        };
        Some(RootGate {
            cell: root_scope.clone(),
            epoch: cell.epoch,
            position: *root_position,
            observed: *root_observed,
        })
    })
}

/// Take the root cell's gate sample of a facet that is already open, so the
/// egress of this call is held against what the root cell had committed when
/// the call left it, not against what it had committed when the facet first
/// opened. A parent at another epoch is not refreshed: the facet's image
/// belongs to the epoch it loaded at, and a ticket that named the new epoch
/// could acknowledge a write the reset that ended the old one discarded.
pub(crate) fn refresh_embedded_root(scope: &str, parent: &StorageIdentity) {
    dbs(|databases| {
        let mut databases = databases.borrow_mut();
        let Some(cell) = databases.get_mut(scope) else {
            return;
        };
        if cell.epoch != parent.epoch {
            return;
        }
        if let StorageBacking::Embedded {
            root_position,
            root_observed,
            ..
        } = &mut cell.backing
        {
            *root_position = parent.root_position;
            *root_observed = parent.root_observed;
        }
    });
}

fn owned_sqlite_data(bytes: &[u8]) -> anyhow::Result<rusqlite::serialize::OwnedData> {
    let pointer = unsafe { rusqlite::ffi::sqlite3_malloc64(bytes.len() as u64) }.cast::<u8>();
    let pointer = std::ptr::NonNull::new(pointer)
        .ok_or_else(|| anyhow::anyhow!("allocate an embedded facet database"))?;
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), pointer.as_ptr(), bytes.len());
        Ok(rusqlite::serialize::OwnedData::from_raw_nonnull(
            pointer,
            bytes.len(),
        ))
    }
}

pub(crate) fn open_embedded(
    scope: &str,
    parent: &StorageIdentity,
    name: &str,
    sqlite_vec: bool,
) -> anyhow::Result<()> {
    anyhow::ensure!(parent.facet_path.len() < 3, "Facet nesting depth limit exceeded. The maximum depth including the root Durable Object is 4.");
    let mut facet_path = parent.facet_path.clone();
    facet_path.push(name.to_string());
    let path = serde_json::to_string(&facet_path)?;
    let root = Connection::open(&parent.root_path)?;
    let image = root
        .query_row(
            "SELECT image FROM _cf_FACETS WHERE scope=?1 AND path=?2",
            rusqlite::params![parent.root_scope, path],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?;
    let mut connection = Connection::open_in_memory()?;
    if let Some(image) = image {
        connection.deserialize(
            rusqlite::DatabaseName::Main,
            owned_sqlite_data(&image)?,
            false,
        )?;
        // A runtime incarnation has a unique scope so a continuation from an
        // aborted instance cannot enter its replacement. The SQLite image is
        // one logical facet, so move the legacy per-cell keys to that new
        // scope before application code can read them.
        let transaction = connection.transaction()?;
        for table in ["_cf_KV", "_cf_ALARM", "_cf_METADATA"] {
            transaction.execute(&format!("UPDATE {table} SET scope=?1"), [scope])?;
        }
        transaction.commit()?;
    }
    finish_open(
        scope,
        connection,
        parent.epoch,
        sqlite_vec,
        StorageBacking::Embedded {
            root_path: parent.root_path.clone(),
            root_scope: parent.root_scope.clone(),
            facet_path,
            root_position: parent.root_position,
            root_observed: parent.root_observed,
        },
    )
}

/// Copy a facet's private in-memory SQLite image into the root actor database.
/// This runs before the turn releases its native operations, and again before
/// an in-handler egress takes its output-gate ticket, so an external effect
/// cannot overtake the image that produced it.
pub(crate) fn flush_embedded(scope: &str) {
    let result = dbs(|databases| -> anyhow::Result<()> {
        let mut databases = databases.borrow_mut();
        let Some(cell) = databases.get_mut(scope) else {
            return Ok(());
        };
        let StorageBacking::Embedded {
            root_path,
            root_scope,
            facet_path,
            ..
        } = &cell.backing
        else {
            return Ok(());
        };
        let position = total_changes(&cell.connection) + schema_version(&cell.connection);
        if position == cell.persisted_position {
            return Ok(());
        }
        let image = without_sql_authorizer(&cell.connection, || {
            cell.connection.serialize(rusqlite::DatabaseName::Main)
        })
        .context("serialize the facet database")?
        .to_vec();
        let path = serde_json::to_string(facet_path)?;
        let root = Connection::open(root_path).context("open the root database")?;
        root.busy_timeout(std::time::Duration::from_secs(5))?;
        root.execute(
            "INSERT INTO _cf_FACETS(scope,path,image) VALUES(?1,?2,?3) \
             ON CONFLICT(scope,path) DO UPDATE SET image=excluded.image",
            rusqlite::params![root_scope, path, image],
        )
        .context("write the facet database image")?;
        cell.persisted_position = position;
        Ok(())
    });
    if let Err(error) = result {
        sql_critical_errors(|errors| {
            errors
                .borrow_mut()
                .entry(scope.to_string())
                .or_insert_with(|| format!("persist facet storage: {error}"));
        });
    }
}

pub(crate) fn delete_embedded(parent: &StorageIdentity, name: &str) -> anyhow::Result<()> {
    #[cfg(all(test, celld_internal_tests))]
    anyhow::ensure!(
        !crate::js::take_embedded_delete_fault_for_test(),
        "injected facet image delete failure"
    );
    let mut facet_path = parent.facet_path.clone();
    facet_path.push(name.to_string());
    let mut root = Connection::open(&parent.root_path)?;
    let paths = {
        let mut statement = root.prepare("SELECT path FROM _cf_FACETS WHERE scope=?1")?;
        let rows = statement.query_map([&parent.root_scope], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let transaction = root.transaction()?;
    for path in paths {
        let candidate: Vec<String> = serde_json::from_str(&path)?;
        if candidate.starts_with(&facet_path) {
            transaction.execute(
                "DELETE FROM _cf_FACETS WHERE scope=?1 AND path=?2",
                rusqlite::params![parent.root_scope, path],
            )?;
        }
    }
    transaction.commit()?;
    Ok(())
}

static NEXT_BATCH_SAVEPOINT: AtomicU64 = AtomicU64::new(1);
static NEXT_SYNC_LIST_CURSOR: AtomicU64 = AtomicU64::new(1);
static NEXT_SQL_CURSOR: AtomicU64 = AtomicU64::new(1);

fn with_batch_savepoint<T>(
    connection: &Connection,
    callback: impl FnOnce(&Connection) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    without_sql_authorizer(connection, || {
        let sequence = NEXT_BATCH_SAVEPOINT.fetch_add(1, Ordering::Relaxed);
        let name = format!("cells_batch_{sequence}");
        connection.execute_batch(&format!("SAVEPOINT {name};"))?;
        match callback(connection) {
            Ok(value) => {
                connection.execute_batch(&format!("RELEASE {name};"))?;
                Ok(value)
            }
            Err(error) => {
                let _ = connection.execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name};"));
                Err(error)
            }
        }
    })
}

fn json_to_sql(v: &serde_json::Value) -> rusqlite::types::Value {
    use rusqlite::types::Value;
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Integer(*b as i64),
        serde_json::Value::Number(n) if n.is_i64() => Value::Integer(n.as_i64().unwrap()),
        serde_json::Value::Number(n) => Value::Real(n.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(s) => Value::Text(s.clone()),
        serde_json::Value::Object(o) if o.contains_key("__celld_bytes") => {
            let bytes = o
                .get("__celld_bytes")
                .and_then(|v| v.as_array())
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|v| v.as_u64().map(|n| n as u8))
                        .collect()
                })
                .unwrap_or_default();
            Value::Blob(bytes)
        }
        _ => Value::Null,
    }
}

const INVALID_UTF8_TEXT: &str = "TEXT result is not valid UTF-8; store arbitrary bytes as a BLOB";

fn sql_text(bytes: &[u8]) -> Result<String, &'static str> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| INVALID_UTF8_TEXT)
}

fn vref_to_json(v: rusqlite::types::ValueRef) -> Result<serde_json::Value, &'static str> {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => Ok(serde_json::Value::Null),
        ValueRef::Integer(i) => Ok(serde_json::json!(i)),
        ValueRef::Real(f) => Ok(serde_json::json!(f)),
        ValueRef::Text(bytes) => sql_text(bytes).map(serde_json::Value::String),
        ValueRef::Blob(b) => Ok(serde_json::json!({ "__celld_bytes": b })),
    }
}

fn total_changes(connection: &Connection) -> u64 {
    // SAFETY: `handle()` belongs to this live connection and SQLite only reads
    // its monotonically increasing completed-write counter.
    unsafe { total_changes_for_handle(connection.handle()) }
}

/// The schema cookie, which every DDL statement increments. `total_changes`
/// counts only row changes, so a handler whose sole mutation is `deleteAll()`
/// (a `DROP TABLE` sweep) or user DDL would otherwise look read-only to the
/// output gate and be acknowledged with no durability proof — workerd
/// explicitly forces confirmation on deleteAll (actor-sqlite.c++:863).
///
/// Read under the normal authorizer (`schema_version` is allowlisted
/// read-only for exactly this) and with a fresh statement, never a cached
/// one: this runs from egress paths that can overlap an active cursor, where
/// toggling the authorizer or borrowing the statement cache is not safe.
fn schema_version(connection: &Connection) -> u64 {
    // Through the statement cache: the output gate reads the cookie after
    // every write, and a fresh `PRAGMA` per read was one SQL compilation
    // per write request.
    connection
        .prepare_cached("PRAGMA schema_version")
        .and_then(|mut statement| statement.query_row([], |row| row.get::<_, i64>(0)))
        .unwrap_or(0) as u64
}

/// The cell's committed-write position: SQLite's total completed row changes,
/// data version, and schema cookie, monotonic for the life of the activation.
/// The output gate samples it around a handler to tell a write from a read.
/// The data version detects a facet image committed through its private
/// connection, and the cookie makes DDL-only mutations count as writes.
/// Widened only after the gated-frame flush learned to outlive its dispatch —
/// before that, counting DDL turned every lazily-CREATE-ing connect handler
/// into a "writer" whose held frames were silently lost. `None` when the scope
/// has no open connection (a Worker with no Durable Object storage).
pub fn write_position(scope: &str) -> Option<u64> {
    with(scope, |c| {
        total_changes(c) + data_version(c) + schema_cookie(scope, c)
    })
}

/// The committed-write position an event starts at. The first sample of an
/// activation is also the cell's published baseline: `observed_position`
/// reports nothing at or below it.
pub fn event_start_position(scope: &str) -> Option<u64> {
    let position = write_position(scope)?;
    dbs(|d| {
        if let Some(cell) = d.borrow_mut().get_mut(scope) {
            cell.published_position.get_or_insert(position);
        }
    });
    Some(position)
}

/// The position a read-only answer observed, given `sample` from
/// `write_position`: the sample when a handler advanced it past the cell's
/// published baseline, and `None` otherwise. The caller samples once and
/// derives the write position from the same value, so the two cannot
/// disagree.
pub fn observed_position(scope: &str, sample: Option<u64>) -> Option<u64> {
    let position = sample?;
    let published = dbs(|d| {
        d.borrow()
            .get(scope)
            .and_then(|cell| cell.published_position)
    });
    match published {
        Some(published) if position <= published => None,
        _ => Some(position),
    }
}

fn data_version(connection: &Connection) -> u64 {
    connection
        .prepare_cached("PRAGMA data_version")
        .and_then(|mut statement| statement.query_row([], |row| row.get::<_, i64>(0)))
        .unwrap_or(0) as u64
}

/// What `storage.sync()` samples in its turn, read together from one open
/// connection. A ticket built from the position alone would name a position
/// without its epoch, which a reset can satisfy at the wrong epoch, or a
/// position inside an open transaction, which no proof can cover.
pub struct SyncSample {
    /// Whether the connection is inside an explicit `transaction()` or
    /// `transactionSync()`. Application SQL cannot open one: the authorizer
    /// refuses `BEGIN` and `SAVEPOINT`, and the batch savepoints this module
    /// takes never outlive the op that took them. So a connection outside
    /// autocommit is exactly a storage transaction a handler has not
    /// finished.
    pub in_transaction: bool,
    /// The cell's committed-write position, as `write_position` reports it.
    pub position: u64,
    /// The activation epoch the connection was installed with.
    pub epoch: u64,
}

/// The `storage.sync()` sample, or `None` when the scope has no open
/// connection.
pub fn sync_sample(scope: &str) -> Option<SyncSample> {
    dbs(|d| {
        d.borrow().get(scope).map(|cell| SyncSample {
            in_transaction: !cell.connection.is_autocommit(),
            position: total_changes(&cell.connection) + schema_cookie(scope, &cell.connection),
            epoch: cell.epoch,
        })
    })
}

/// The cell's schema cookie, re-read only when something could have moved it.
fn schema_cookie(scope: &str, connection: &Connection) -> u64 {
    let changes = total_changes(connection);
    let prepares = SQL_PREPARES.load(Ordering::Relaxed);
    let cached = cells(|c| {
        c.schema_cookies
            .borrow()
            .get(scope)
            .filter(|seen| seen.changes == changes && seen.prepares == prepares)
            .map(|seen| seen.cookie)
    });
    if let Some(cookie) = cached {
        return cookie;
    }
    let cookie = schema_version(connection);
    // Read the counter *after* the pragma, not before: preparing it calls
    // the authorizer too, so a count taken beforehand is already stale by
    // the time it is stored and every later sample misses.
    let prepares = SQL_PREPARES.load(Ordering::Relaxed);
    cells(|c| {
        c.schema_cookies.borrow_mut().insert(
            scope.to_string(),
            SchemaCookie {
                cookie,
                changes,
                prepares,
            },
        )
    });
    cookie
}

unsafe fn total_changes_for_handle(database: *mut rusqlite::ffi::sqlite3) -> u64 {
    rusqlite::ffi::sqlite3_total_changes64(database) as u64
}

pub enum StoredValue {
    LegacyJson(String),
    V8(Vec<u8>),
}

fn stored_value(value: rusqlite::types::ValueRef<'_>) -> StoredValue {
    match value {
        rusqlite::types::ValueRef::Blob(bytes) => StoredValue::V8(bytes.to_vec()),
        rusqlite::types::ValueRef::Text(bytes) => {
            StoredValue::LegacyJson(String::from_utf8_lossy(bytes).into_owned())
        }
        value => StoredValue::LegacyJson(
            vref_to_json(value)
                .expect("the TEXT arm is handled before JSON conversion")
                .to_string(),
        ),
    }
}

/// (column names, result rows, rows written) from a cell SQL exec.
type SqlExec = (Vec<String>, Vec<Vec<serde_json::Value>>, u64);

/// Run SQL on the cell's own SQLite (the DO `ctx.storage.sql` API). Cloudflare
/// accepts schema batches in one `exec()` call, so execute multi-statement,
/// parameter-free input as a batch. Queries and parameterized statements still
/// use a prepared statement so their cursor rows are returned.
pub fn sql_exec(scope: &str, query: &str, binds: &[serde_json::Value]) -> Result<SqlExec, String> {
    require_sql_healthy(scope)?;
    let run = with(scope, |c| {
        as_user_sql(|| {
            let changes_before = total_changes(c);
            if binds.is_empty() && query.matches(';').count() > 1 {
                c.execute_batch(query).map_err(|e| e.to_string())?;
                return Ok((
                    Vec::new(),
                    Vec::new(),
                    total_changes(c).saturating_sub(changes_before),
                ));
            }
            let mut stmt = c.prepare(query).map_err(|e| e.to_string())?;
            let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
            let n = cols.len();
            let params = rusqlite::params_from_iter(binds.iter().map(json_to_sql));
            let mut rows = stmt.query(params).map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                let mut r = Vec::with_capacity(n);
                for i in 0..n {
                    r.push(
                        vref_to_json(row.get_ref(i).map_err(|e| e.to_string())?)
                            .map_err(str::to_string)?,
                    );
                }
                out.push(r);
            }
            drop(rows);
            drop(stmt);
            Ok((cols, out, total_changes(c).saturating_sub(changes_before)))
        })
    });
    run.unwrap_or_else(|| Err(format!("no db for {scope}")))
}

/// Execute every complete statement at the front of `input`, returning the
/// untouched suffix beginning with the first incomplete statement. SQLite's
/// own parser determines statement boundaries, including trigger bodies and
/// semicolons inside strings. Rows are stepped and discarded without crossing
/// the V8 boundary.
pub fn sql_ingest(scope: &str, input: &str) -> Result<(String, u64, u64), String> {
    require_sql_healthy(scope)?;
    let sql = CString::new(input).map_err(|error| error.to_string())?;
    let result = with(scope, |connection| -> anyhow::Result<_> {
        // Application SQL, exactly like `sql_exec`: the `_cf_` reservation
        // must hold here too, or the ingest path is a way around it.
        as_user_sql(|| {
            // SAFETY: `sql` remains alive for the entire loop. Every prepared
            // statement is finalized on all paths, and SQLite's tail points within
            // the same NUL-terminated allocation.
            unsafe {
                let database = connection.handle();
                let started_in_transaction = rusqlite::ffi::sqlite3_get_autocommit(database) == 0;
                let start = sql.as_ptr();
                let mut source = start;
                let changes_before = total_changes_for_handle(database);
                let mut statement_count = 0_u64;

                loop {
                    let offset = source.offset_from(start) as usize;
                    let remainder = input
                        .get(offset..)
                        .ok_or_else(|| anyhow::anyhow!("SQLite returned an invalid SQL tail"))?;
                    let mut statement = std::ptr::null_mut();
                    let mut tail = std::ptr::null();
                    let prepare = rusqlite::ffi::sqlite3_prepare_v2(
                        database,
                        source,
                        -1,
                        &mut statement,
                        &mut tail,
                    );
                    if prepare != rusqlite::ffi::SQLITE_OK {
                        if !statement.is_null() {
                            rusqlite::ffi::sqlite3_finalize(statement);
                        }
                        if rusqlite::ffi::sqlite3_complete(source) == 0 {
                            return Ok((
                                remainder.to_string(),
                                total_changes_for_handle(database).saturating_sub(changes_before),
                                statement_count,
                            ));
                        }
                        return Err(sqlite_operation_failure(
                            scope,
                            database,
                            started_in_transaction,
                            prepare,
                            "prepare ingested SQL",
                        ));
                    }
                    if statement.is_null() {
                        return Ok((
                            remainder.to_string(),
                            total_changes_for_handle(database).saturating_sub(changes_before),
                            statement_count,
                        ));
                    }
                    if tail.is_null() || tail < source {
                        rusqlite::ffi::sqlite3_finalize(statement);
                        return Err(anyhow::anyhow!("SQLite returned an invalid SQL tail"));
                    }

                    let consumed_length = tail.offset_from(source) as usize;
                    let consumed = CString::new(std::slice::from_raw_parts(
                        source.cast::<u8>(),
                        consumed_length,
                    ))?;
                    if rusqlite::ffi::sqlite3_complete(consumed.as_ptr()) == 0 {
                        rusqlite::ffi::sqlite3_finalize(statement);
                        return Ok((
                            remainder.to_string(),
                            total_changes_for_handle(database).saturating_sub(changes_before),
                            statement_count,
                        ));
                    }

                    loop {
                        let step = rusqlite::ffi::sqlite3_step(statement);
                        match step {
                            rusqlite::ffi::SQLITE_ROW => {}
                            rusqlite::ffi::SQLITE_DONE => break,
                            _ => {
                                let error = sqlite_operation_failure(
                                    scope,
                                    database,
                                    started_in_transaction,
                                    step,
                                    "step ingested SQL",
                                );
                                rusqlite::ffi::sqlite3_finalize(statement);
                                return Err(error);
                            }
                        }
                    }
                    rusqlite::ffi::sqlite3_finalize(statement);
                    statement_count += 1;
                    source = tail;
                }
            }
        })
    });
    result
        .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
        .map_err(|error| error.to_string())
}

unsafe fn sql_cursor_columns(statement: *mut rusqlite::ffi::sqlite3_stmt) -> Vec<String> {
    let count = rusqlite::ffi::sqlite3_column_count(statement);
    (0..count)
        .map(|index| {
            let name = rusqlite::ffi::sqlite3_column_name(statement, index);
            if name.is_null() {
                String::new()
            } else {
                CStr::from_ptr(name).to_string_lossy().into_owned()
            }
        })
        .collect()
}

/// One column of one SQL result row, in SQLite's own five types.
///
/// Both cursor ops turn this straight into V8 values, so a result row never
/// becomes a JSON string. `serde_json::Value` cannot carry the type faithfully
/// anyway: it holds no byte string, so a BLOB had to travel as a
/// `__celld_bytes` array of decimal numbers, and it holds no infinity, so
/// SQLite's `9e999` collapsed to null.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

unsafe fn sql_cursor_row(
    statement: *mut rusqlite::ffi::sqlite3_stmt,
) -> Result<Vec<SqlValue>, &'static str> {
    let count = rusqlite::ffi::sqlite3_column_count(statement);
    (0..count)
        .map(
            |index| match rusqlite::ffi::sqlite3_column_type(statement, index) {
                rusqlite::ffi::SQLITE_INTEGER => Ok(SqlValue::Integer(
                    rusqlite::ffi::sqlite3_column_int64(statement, index),
                )),
                rusqlite::ffi::SQLITE_FLOAT => Ok(SqlValue::Real(
                    rusqlite::ffi::sqlite3_column_double(statement, index),
                )),
                rusqlite::ffi::SQLITE_TEXT => {
                    let length = rusqlite::ffi::sqlite3_column_bytes(statement, index) as usize;
                    let pointer = rusqlite::ffi::sqlite3_column_text(statement, index);
                    let bytes = if length == 0 {
                        &[][..]
                    } else {
                        std::slice::from_raw_parts(pointer, length)
                    };
                    sql_text(bytes).map(SqlValue::Text)
                }
                rusqlite::ffi::SQLITE_BLOB => {
                    let length = rusqlite::ffi::sqlite3_column_bytes(statement, index) as usize;
                    let pointer: *const u8 =
                        rusqlite::ffi::sqlite3_column_blob(statement, index).cast();
                    let bytes = if length == 0 {
                        Vec::new()
                    } else {
                        std::slice::from_raw_parts(pointer, length).to_vec()
                    };
                    Ok(SqlValue::Blob(bytes))
                }
                _ => Ok(SqlValue::Null),
            },
        )
        .collect()
}

enum SqlStatementCacheLookup {
    Miss,
    Busy,
    Hit {
        statement: *mut rusqlite::ffi::sqlite3_stmt,
        reused: bool,
        query: Arc<str>,
    },
}

fn take_cached_sql_statement(scope: &str, query: &str) -> SqlStatementCacheLookup {
    sql_statement_caches(|caches| {
        let mut caches = caches.borrow_mut();
        let Some(cache) = caches.get_mut(scope) else {
            return SqlStatementCacheLookup::Miss;
        };
        cache.clock = cache.clock.wrapping_add(1);
        let clock = cache.clock;
        let Some(cache_query) = cache
            .entries
            .get_key_value(query)
            .map(|(query, _)| Arc::clone(query))
        else {
            return SqlStatementCacheLookup::Miss;
        };
        let entry = cache
            .entries
            .get_mut(query)
            .expect("cache key disappeared during lookup");
        entry.last_used = clock;
        if entry.statement.is_null() {
            return SqlStatementCacheLookup::Busy;
        }
        let statement = std::mem::replace(&mut entry.statement, std::ptr::null_mut());
        let reused = entry.use_count > 0;
        entry.use_count = entry.use_count.saturating_add(1);
        SqlStatementCacheLookup::Hit {
            statement,
            reused,
            query: cache_query,
        }
    })
}

fn register_cached_sql_statement(scope: &str, query: &str) -> Option<Arc<str>> {
    sql_statement_caches(|caches| {
        let mut caches = caches.borrow_mut();
        let cache = caches.entry(scope.to_string()).or_default();
        cache.clock = cache.clock.wrapping_add(1);
        let clock = cache.clock;
        let size = query.len();
        let cache_query: Arc<str> = Arc::from(query);
        cache.total_size = cache.total_size.saturating_add(size);
        cache.entries.insert(
            Arc::clone(&cache_query),
            CachedSqlStatement {
                statement: std::ptr::null_mut(),
                use_count: 1,
                size,
                last_used: clock,
            },
        );

        while cache.total_size > SQL_STATEMENT_CACHE_MAX_SIZE {
            let Some(lru) = cache
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(query, _)| Arc::clone(query))
            else {
                break;
            };
            if let Some(removed) = cache.entries.remove(&lru) {
                cache.total_size = cache.total_size.saturating_sub(removed.size);
            }
        }
        cache.entries.contains_key(query).then_some(cache_query)
    })
}

fn discard_cached_sql_statement(scope: &str, query: &str) {
    sql_statement_caches(|caches| {
        let mut caches = caches.borrow_mut();
        let Some(cache) = caches.get_mut(scope) else {
            return;
        };
        if let Some(removed) = cache.entries.remove(query) {
            cache.total_size = cache.total_size.saturating_sub(removed.size);
        }
        if cache.entries.is_empty() {
            caches.remove(scope);
        }
    });
}

fn recycle_sql_statement(scope: &str, query: &str, statement: *mut rusqlite::ffi::sqlite3_stmt) {
    // SAFETY: the caller transfers exclusive ownership of a live statement.
    let reusable = unsafe {
        rusqlite::ffi::sqlite3_reset(statement) == rusqlite::ffi::SQLITE_OK
            && rusqlite::ffi::sqlite3_clear_bindings(statement) == rusqlite::ffi::SQLITE_OK
    };
    let mut retained = false;
    if reusable {
        sql_statement_caches(|caches| {
            if let Some(entry) = caches
                .borrow_mut()
                .get_mut(scope)
                .and_then(|cache| cache.entries.get_mut(query))
            {
                if entry.statement.is_null() {
                    entry.statement = statement;
                    retained = true;
                }
            }
        });
    }
    if !retained {
        if !reusable {
            discard_cached_sql_statement(scope, query);
        }
        // SAFETY: no cache slot retained the transferred statement.
        unsafe {
            rusqlite::ffi::sqlite3_finalize(statement);
        }
    }
}

fn close_sql_statement_cache(scope: &str) {
    sql_statement_caches(|caches| {
        caches.borrow_mut().remove(scope);
    });
}

fn discard_in_use_sql_statement(
    scope: &str,
    cache_query: Option<&str>,
    statement: *mut rusqlite::ffi::sqlite3_stmt,
) {
    if let Some(query) = cache_query {
        discard_cached_sql_statement(scope, query);
    }
    if !statement.is_null() {
        // SAFETY: the caller transfers exclusive ownership of the statement.
        unsafe {
            rusqlite::ffi::sqlite3_finalize(statement);
        }
    }
}

/// (cursor id, column names, first row, rows written, reused-cached-query)
/// from starting a SQL cursor.
type SqlCursorStart = (u64, Vec<String>, Option<Vec<SqlValue>>, u64, bool);

/// Prepare and step a SQL statement once. The returned cursor owns the native
/// statement and yields at most one copied row per subsequent call.
pub fn sql_cursor_start(
    scope: &str,
    query: &str,
    binds: &[serde_json::Value],
) -> Result<SqlCursorStart, String> {
    require_sql_healthy(scope)?;
    let (cached_statement, cached_query, cache_busy, reused_cached_query) =
        match take_cached_sql_statement(scope, query) {
            SqlStatementCacheLookup::Miss => (std::ptr::null_mut(), None, false, false),
            SqlStatementCacheLookup::Busy => (std::ptr::null_mut(), None, true, false),
            SqlStatementCacheLookup::Hit {
                statement,
                reused,
                query,
            } => (statement, Some(query), false, reused),
        };
    let sql = CString::new(query).map_err(|error| error.to_string())?;
    let result = with(scope, |connection| -> anyhow::Result<_> {
        // `storage.sql.exec` reaches SQLite here, so this is where the `_cf_`
        // reservation has to hold -- the statement is prepared inside, and
        // preparation is when SQLite consults the authorizer.
        as_user_sql(|| {
            // SAFETY: the connection remains in DBS until close(), which drops all
            // SQL cursors first. SQLite receives transient copies of every bind.
            unsafe {
                let database = connection.handle();
                let started_in_transaction = rusqlite::ffi::sqlite3_get_autocommit(database) == 0;
                let changes_before = total_changes_for_handle(database);
                let start = sql.as_ptr();
                let mut source = start;
                let mut saw_prefix = false;
                loop {
                    let using_cached = source == start && !cached_statement.is_null();
                    let mut cache_query = using_cached
                        .then(|| cached_query.as_ref().expect("cached query key").clone());
                    let mut statement = if using_cached {
                        cached_statement
                    } else {
                        std::ptr::null_mut()
                    };
                    let mut tail = std::ptr::null();
                    if !using_cached {
                        let prepare = rusqlite::ffi::sqlite3_prepare_v2(
                            database,
                            source,
                            -1,
                            &mut statement,
                            &mut tail,
                        );
                        if prepare != rusqlite::ffi::SQLITE_OK {
                            discard_in_use_sql_statement(scope, cache_query.as_deref(), statement);
                            return Err(sqlite_operation_failure(
                                scope,
                                database,
                                started_in_transaction,
                                prepare,
                                "prepare SQL cursor",
                            ));
                        }
                    }
                    if statement.is_null() {
                        return Err(anyhow::anyhow!("SQL code did not contain a statement"));
                    }

                    let has_more = !using_cached
                        && !tail.is_null()
                        && CStr::from_ptr(tail)
                            .to_bytes()
                            .iter()
                            .any(|byte| !byte.is_ascii_whitespace());
                    if has_more {
                        if rusqlite::ffi::sqlite3_bind_parameter_count(statement) != 0 {
                            discard_in_use_sql_statement(scope, None, statement);
                            return Err(anyhow::anyhow!(
                                "Wrong number of parameter bindings for SQL query."
                            ));
                        }
                        loop {
                            let step = rusqlite::ffi::sqlite3_step(statement);
                            match step {
                                rusqlite::ffi::SQLITE_ROW => {}
                                rusqlite::ffi::SQLITE_DONE => break,
                                _ => {
                                    let error = sqlite_operation_failure(
                                        scope,
                                        database,
                                        started_in_transaction,
                                        step,
                                        "step SQL prefix",
                                    );
                                    discard_in_use_sql_statement(scope, None, statement);
                                    return Err(error);
                                }
                            }
                        }
                        rusqlite::ffi::sqlite3_finalize(statement);
                        source = tail;
                        saw_prefix = true;
                        continue;
                    }

                    let expected = rusqlite::ffi::sqlite3_bind_parameter_count(statement) as usize;
                    if expected != binds.len() {
                        discard_in_use_sql_statement(scope, cache_query.as_deref(), statement);
                        return Err(anyhow::anyhow!(
                            "Wrong number of parameter bindings for SQL query."
                        ));
                    }
                    for (offset, value) in binds.iter().map(json_to_sql).enumerate() {
                        if bind_cursor_value(statement, offset as i32 + 1, &value).is_err() {
                            let error = sqlite_failure(database, "bind sync KV cursor");
                            discard_in_use_sql_statement(scope, cache_query.as_deref(), statement);
                            return Err(error);
                        }
                    }
                    if cache_query.is_none() && !cache_busy && !saw_prefix {
                        cache_query = register_cached_sql_statement(scope, query);
                    }
                    let step = rusqlite::ffi::sqlite3_step(statement);
                    return match step {
                        rusqlite::ffi::SQLITE_ROW => {
                            // Read metadata after the first step so SQLite has
                            // automatically recompiled an expired cached statement.
                            let columns = sql_cursor_columns(statement);
                            let row = match sql_cursor_row(statement) {
                                Ok(row) => row,
                                Err(error) => {
                                    discard_in_use_sql_statement(
                                        scope,
                                        cache_query.as_deref(),
                                        statement,
                                    );
                                    return Err(anyhow::anyhow!(error));
                                }
                            };
                            Ok((
                                database,
                                statement,
                                changes_before,
                                columns,
                                Some(row),
                                0,
                                cache_query,
                            ))
                        }
                        rusqlite::ffi::SQLITE_DONE => {
                            let columns = sql_cursor_columns(statement);
                            let rows_written =
                                total_changes_for_handle(database).saturating_sub(changes_before);
                            Ok((
                                database,
                                statement,
                                changes_before,
                                columns,
                                None,
                                rows_written,
                                cache_query,
                            ))
                        }
                        _ => {
                            let error = sqlite_operation_failure(
                                scope,
                                database,
                                started_in_transaction,
                                step,
                                "step SQL cursor",
                            );
                            discard_in_use_sql_statement(scope, cache_query.as_deref(), statement);
                            Err(error)
                        }
                    };
                }
            }
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
    .map_err(|error| error.to_string())?;

    let (database, statement, changes_before, columns, row, rows_written, cache_query) = result;
    if row.is_none() {
        if let Some(query) = cache_query.as_deref() {
            recycle_sql_statement(scope, query, statement);
        } else {
            discard_in_use_sql_statement(scope, None, statement);
        }
        return Ok((0, columns, row, rows_written, reused_cached_query));
    }
    let id = NEXT_SQL_CURSOR.fetch_add(1, Ordering::Relaxed);
    sql_cursors(|cursors| {
        cursors.borrow_mut().insert(
            id,
            SqlCursor {
                scope: scope.to_string(),
                database,
                statement,
                changes_before,
                cache_query,
            },
        );
    });
    Ok((id, columns, row, rows_written, reused_cached_query))
}

/// Step one row. `Some(row)` is a row and carries no write count, because a
/// cursor's `rowsWritten` is only final once SQLite reports DONE; `None`
/// carries that final count. The two are returned together so a caller cannot
/// read a mid-cursor write count that SQLite has not finished producing.
pub fn sql_cursor_next(cursor_id: u64) -> Result<(Option<Vec<SqlValue>>, u64), String> {
    sql_cursors(|cursors| {
        let mut cursors = cursors.borrow_mut();
        let cursor = cursors
            .get_mut(&cursor_id)
            .ok_or_else(|| "SQL cursor is no longer active".to_string())?;
        if let Some(error) = sql_critical_error(&cursor.scope) {
            cursors.remove(&cursor_id);
            return Err(error);
        }
        // SAFETY: the cursor owns its statement on a connection that remains
        // live until the cursor is removed.
        let started_in_transaction =
            unsafe { rusqlite::ffi::sqlite3_get_autocommit(cursor.database) == 0 };
        let step = unsafe { rusqlite::ffi::sqlite3_step(cursor.statement) };
        match step {
            rusqlite::ffi::SQLITE_ROW => match unsafe { sql_cursor_row(cursor.statement) } {
                Ok(row) => Ok((Some(row), 0)),
                Err(error) => {
                    cursors.remove(&cursor_id);
                    Err(error.to_string())
                }
            },
            rusqlite::ffi::SQLITE_DONE => {
                let rows_written = unsafe {
                    total_changes_for_handle(cursor.database).saturating_sub(cursor.changes_before)
                };
                cursors.remove(&cursor_id);
                Ok((None, rows_written))
            }
            _ => {
                let error = sqlite_operation_failure(
                    &cursor.scope,
                    cursor.database,
                    started_in_transaction,
                    step,
                    "step SQL cursor",
                )
                .to_string();
                if let Some(query) = cursor.cache_query.take() {
                    discard_cached_sql_statement(&cursor.scope, &query);
                }
                cursors.remove(&cursor_id);
                Err(error)
            }
        }
    })
}

pub fn sql_cursor_close(cursor_id: u64) {
    sql_cursors(|cursors| {
        cursors.borrow_mut().remove(&cursor_id);
    });
}

/// `(last_insert_rowid, changes)` for the cell's connection. D1 reports both in
/// a statement's `meta`; the DO `SqlStorage` surface exposes neither, so this
/// is a separate read rather than a new field on the cursor result.
pub fn sql_write_meta(scope: &str) -> Result<(i64, u64), String> {
    require_sql_healthy(scope)?;
    with(scope, |connection| {
        Ok((connection.last_insert_rowid(), connection.changes()))
    })
    .unwrap_or_else(|| Err(format!("no db for {scope}")))
}

pub fn sql_database_size(scope: &str) -> Result<u64, String> {
    require_sql_healthy(scope)?;
    with(scope, |connection| {
        without_sql_authorizer(connection, || {
            connection
                .query_row(
                    "SELECT (page_count - freelist_count) * page_size \
                     FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size()",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .map_err(|error| error.to_string())
        })
    })
    .unwrap_or_else(|| Err(format!("no db for {scope}")))
}

// ---- D1 -------------------------------------------------------------------
// The D1 binding's one execution primitive. The first version of the binding
// was assembled out of the DO `SqlStorage` ops -- cursor start, cursor next, a
// separate `sql_write_meta` read, a JS remainder parser -- and each seam
// between them carried a contract nothing enforced: the meta read was only
// correct if no other statement ran on the connection in between, blob binds
// rode a sentinel object that anything else silently turned into NULL, and JS
// re-derived "is this SQL complete" beside SQLite's own answer. `d1_run` is
// the replacement: one typed request in, one typed result out, rows and the
// metadata snapshot taken from the same execution on the same connection, and
// SQLite alone deciding what is and is not a statement.

/// Rows one D1 result set can carry across the binding. In-cell iteration
/// streams; a `.all()` from a Worker materializes the whole result set and
/// structured-clones it into the calling isolate. The bound exists so that
/// "the query was too large" and "the isolate died" are different messages.
pub const D1_MAX_ROWS: usize = 100_000;

/// Bytes one D1 result set can carry across the binding. The result exists in
/// two heaps at once -- materialized in the database cell's isolate, then
/// cloned into the caller's -- and the default isolate heap is 128 MiB, so
/// 32 MiB keeps both copies inside it with room for the application's own
/// state. The row cap bounds ordinary rows; this bounds wide ones, which
/// 100,000 rows of TEXT or BLOB would otherwise not.
pub const D1_MAX_RESULT_BYTES: usize = 32 * 1024 * 1024;

/// One D1 request. The modes are explicit because their contracts differ:
/// a prepared statement is exactly one statement with binds, an exec is many
/// statements with none, and a migration is an exec plus its bookkeeping row
/// inside one transaction. The first adapter multiplexed all three through
/// the general SQL ops, and the differences lived as conventions in JS.
#[derive(serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum D1Request {
    /// `prepare(sql)` with positional binds: one statement, no more.
    Prepared {
        sql: String,
        #[serde(default)]
        params: Vec<serde_json::Value>,
        /// `first()` keeps one row on this side of the boundary, so a first()
        /// over a large table neither clones the table nor trips a cap.
        #[serde(default)]
        first: bool,
    },
    /// `batch(statements)`: prepared statements in one transaction.
    Batch { statements: Vec<D1PreparedRequest> },
    /// `exec(sql)`: multi-statement, SQLite decides where statements end.
    Exec {
        sql: String,
        /// The Worker binding's `exec()` reports counts only, as upstream
        /// does; `celld d1 execute` asks for the rows too.
        #[serde(default)]
        rows: bool,
    },
    /// One migration file plus its bookkeeping row, in one transaction.
    Migrate {
        name: String,
        sql: String,
        /// The wrangler bookkeeping table, per binding
        /// (`migrations_table`). Joined into SQL as an identifier, so it is
        /// refused unless it is a plain identifier.
        table: String,
    },
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct D1PreparedRequest {
    sql: String,
    #[serde(default)]
    params: Vec<serde_json::Value>,
}

/// D1 `meta`, snapshotted inside the same execution that produced the rows.
/// `last_row_id` and `changes` are connection-level in SQLite, so reading
/// them in a second call was only correct while nothing else ran on the
/// cell's connection in between -- an invariant no code enforced.
#[derive(serde::Serialize)]
pub struct D1Meta {
    duration: f64,
    rows_read: u64,
    rows_written: u64,
    last_row_id: i64,
    changes: u64,
    changed_db: bool,
    size_after: u64,
    served_by: &'static str,
    served_by_region: &'static str,
    /// One writer, no replicas: every read is served by the primary, and
    /// saying so honestly beats omitting the field.
    served_by_primary: bool,
}

/// One result set: positional rows plus the column names to shape them with.
/// Rows stay positional on the wire so `.all()` and `.raw()` share one copy.
#[derive(serde::Serialize)]
pub struct D1Rows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

#[derive(serde::Serialize)]
pub struct D1PreparedResult {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    meta: D1Meta,
}

#[derive(serde::Serialize)]
#[serde(untagged)]
pub enum D1Response {
    Prepared(D1PreparedResult),
    Batch(Vec<D1PreparedResult>),
    Exec {
        count: u64,
        duration: f64,
        results: Vec<D1Rows>,
    },
    Migrate {
        count: u64,
        duration: f64,
    },
}

/// A typed refusal. `family` is the prefix applications match on
/// (`D1_ERROR`, `D1_TYPE_ERROR`, `D1_EXEC_ERROR`), kept apart from the
/// message so the JS client builds the error and its `.cause` without
/// parsing its own wire format.
pub struct D1Failure {
    pub family: &'static str,
    pub message: String,
}

fn d1_error(message: impl Into<String>) -> D1Failure {
    D1Failure {
        family: "D1_ERROR",
        message: message.into(),
    }
}

fn d1_exec_error(message: impl Into<String>) -> D1Failure {
    D1Failure {
        family: "D1_EXEC_ERROR",
        message: message.into(),
    }
}

/// The refusal for SQL whose tail SQLite could not parse into a statement.
/// It is loud on purpose: the statements before the tail have already run,
/// so silence here is how a truncated migration once recorded itself as
/// applied.
const D1_INCOMPLETE: &str =
    "the SQL ends in an incomplete statement, which did not run: terminate \
     the last statement with a semicolon, or close an open quote";

/// Encode one bind value, refusing what it cannot represent. The JS client
/// validates at the public boundary (`bind()` throws `D1_TYPE_ERROR` as
/// upstream does); this is the second lock on the same door, because the CLI
/// route reaches this function without that client. The previous adapter
/// mapped anything unrecognized to SQL NULL, so a `Uint8Array` bind stored
/// NULL and nothing said so.
fn d1_bind_value(value: &serde_json::Value) -> Result<rusqlite::types::Value, D1Failure> {
    use rusqlite::types::Value;
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(flag) => Ok(Value::Integer(*flag as i64)),
        serde_json::Value::Number(number) if number.is_i64() => {
            Ok(Value::Integer(number.as_i64().expect("checked i64")))
        }
        serde_json::Value::Number(number) => Ok(Value::Real(number.as_f64().unwrap_or(0.0))),
        serde_json::Value::String(text) => Ok(Value::Text(text.clone())),
        serde_json::Value::Array(items) => {
            // Upstream's wire format for a BLOB bind: an array of bytes.
            let mut bytes = Vec::with_capacity(items.len());
            for item in items {
                match item.as_u64().filter(|byte| *byte < 256) {
                    Some(byte) => bytes.push(byte as u8),
                    None => {
                        return Err(D1Failure {
                            family: "D1_TYPE_ERROR",
                            message: format!(
                                "Type 'object' not supported for value '{item}': an array bind \
                                 must hold bytes 0-255"
                            ),
                        })
                    }
                }
            }
            Ok(Value::Blob(bytes))
        }
        other => Err(D1Failure {
            family: "D1_TYPE_ERROR",
            message: format!("Type 'object' not supported for value '{other}'"),
        }),
    }
}

fn d1_json_string_len(value: &str) -> usize {
    value.chars().fold(2usize, |length, character| {
        let escaped = match character {
            '"' | '\\' | '\u{0008}' | '\t' | '\n' | '\u{000c}' | '\r' => 2,
            '\u{0000}'..='\u{001f}' => 6,
            other => other.len_utf8(),
        };
        length.saturating_add(escaped)
    })
}

/// One result cell as JSON, plus the approximate bytes it will occupy in the
/// caller's heap, for the result-bytes cap. BLOBs cross as arrays of bytes --
/// the shape upstream D1 returns them in -- which is what retired the
/// `__celld_bytes` sentinel on this path.
unsafe fn d1_row_value(
    statement: *mut rusqlite::ffi::sqlite3_stmt,
    index: i32,
    available: usize,
) -> Result<(serde_json::Value, usize), D1RowValueError> {
    let scalar_bytes = std::mem::size_of::<serde_json::Value>();
    match rusqlite::ffi::sqlite3_column_type(statement, index) {
        rusqlite::ffi::SQLITE_INTEGER => {
            if scalar_bytes > available {
                return Err(D1RowValueError::ResultLimit);
            }
            Ok((
                serde_json::json!(rusqlite::ffi::sqlite3_column_int64(statement, index)),
                scalar_bytes,
            ))
        }
        rusqlite::ffi::SQLITE_FLOAT => {
            if scalar_bytes > available {
                return Err(D1RowValueError::ResultLimit);
            }
            Ok((
                serde_json::json!(rusqlite::ffi::sqlite3_column_double(statement, index)),
                scalar_bytes,
            ))
        }
        rusqlite::ffi::SQLITE_TEXT => {
            let length = rusqlite::ffi::sqlite3_column_bytes(statement, index) as usize;
            let pointer = rusqlite::ffi::sqlite3_column_text(statement, index);
            let bytes = if length == 0 {
                &[][..]
            } else {
                std::slice::from_raw_parts(pointer, length)
            };
            let value = sql_text(bytes).map_err(|_| D1RowValueError::InvalidUtf8Text)?;
            // serde_json escapes quotes, backslashes, and controls when the
            // response crosses into V8. Charge that wire shape now: decoded
            // bytes let NUL-heavy TEXT expand sixfold after the cap passed.
            let expanded = scalar_bytes.saturating_add(d1_json_string_len(&value));
            if expanded > available {
                return Err(D1RowValueError::ResultLimit);
            }
            Ok((serde_json::Value::String(value), expanded))
        }
        rusqlite::ffi::SQLITE_BLOB => {
            let length = rusqlite::ffi::sqlite3_column_bytes(statement, index) as usize;
            // A BLOB becomes one serde_json::Value per byte before it reaches
            // JSON or V8. Charge that expansion before allocating it. Raw
            // byte length let a sub-limit BLOB allocate hundreds of MiB and
            // defeat the result cap itself.
            let expanded = length
                .saturating_mul(std::mem::size_of::<serde_json::Value>())
                .saturating_add(scalar_bytes)
                .saturating_add(2);
            if expanded > available {
                return Err(D1RowValueError::ResultLimit);
            }
            let pointer: *const u8 = rusqlite::ffi::sqlite3_column_blob(statement, index).cast();
            let bytes = if length == 0 {
                &[][..]
            } else {
                std::slice::from_raw_parts(pointer, length)
            };
            Ok((
                serde_json::Value::Array(
                    bytes.iter().map(|byte| serde_json::json!(byte)).collect(),
                ),
                expanded,
            ))
        }
        _ => {
            if scalar_bytes > available {
                return Err(D1RowValueError::ResultLimit);
            }
            Ok((serde_json::Value::Null, scalar_bytes))
        }
    }
}

enum D1RowValueError {
    ResultLimit,
    InvalidUtf8Text,
}

fn d1_result_limit_error() -> String {
    format!(
        "query result exceeds {} bytes, which is more than one D1 result set can carry across a \
         binding; narrow the query, or iterate inside a Durable Object where the cursor streams",
        D1_MAX_RESULT_BYTES
    )
}

/// What stepping one statement to completion left behind.
struct D1Stepped {
    rows: Vec<Vec<serde_json::Value>>,
    rows_read: u64,
    /// The result refusal, when one fired. The statement still ran to
    /// completion because its effects happen as its steps do. Stopping an
    /// `INSERT ... RETURNING` at a bad result would silently lose the writes
    /// that the remaining steps carry. Drain first, then refuse the result.
    over: Option<String>,
}

/// Step a prepared statement to completion, keeping at most `keep` rows and
/// charging kept bytes against `budget`. The caller finalizes the statement
/// on every path. `keep` below the row cap means the caller asked for fewer
/// rows (`first()` keeps one), so rows past it are dropped without an error;
/// at the cap, overflow is a refusal.
unsafe fn d1_step_to_done(
    scope: &str,
    database: *mut rusqlite::ffi::sqlite3,
    statement: *mut rusqlite::ffi::sqlite3_stmt,
    started_in_transaction: bool,
    keep: usize,
    budget: &mut usize,
) -> Result<D1Stepped, String> {
    let column_count = rusqlite::ffi::sqlite3_column_count(statement);
    let mut stepped = D1Stepped {
        rows: Vec::new(),
        rows_read: 0,
        over: None,
    };
    loop {
        match rusqlite::ffi::sqlite3_step(statement) {
            rusqlite::ffi::SQLITE_ROW => {
                stepped.rows_read += 1;
                if stepped.over.is_some() || keep == 0 {
                    continue;
                }
                if stepped.rows.len() >= keep {
                    if keep >= D1_MAX_ROWS {
                        stepped.over = Some(format!(
                            "query returned more than {D1_MAX_ROWS} rows, which is more than \
                             one D1 result set can carry across a binding; narrow the query, \
                             or iterate inside a Durable Object where the cursor streams"
                        ));
                    }
                    continue;
                }
                let mut row = Vec::with_capacity(column_count as usize);
                let mut row_bytes = 0usize;
                let mut row_over = false;
                for index in 0..column_count {
                    let available = budget.saturating_sub(row_bytes);
                    let (value, size) = match d1_row_value(statement, index, available) {
                        Ok(value) => value,
                        Err(D1RowValueError::ResultLimit) => {
                            row_over = true;
                            break;
                        }
                        Err(D1RowValueError::InvalidUtf8Text) => {
                            stepped.over = Some(INVALID_UTF8_TEXT.to_string());
                            row_over = true;
                            break;
                        }
                    };
                    row_bytes += size;
                    row.push(value);
                }
                if row_over {
                    if stepped.over.is_none() {
                        stepped.over = Some(d1_result_limit_error());
                    }
                    continue;
                }
                let Some(remaining) = budget.checked_sub(row_bytes) else {
                    stepped.over = Some(d1_result_limit_error());
                    continue;
                };
                *budget = remaining;
                stepped.rows.push(row);
            }
            rusqlite::ffi::SQLITE_DONE => return Ok(stepped),
            code => {
                return Err(d1_sqlite_failure(
                    scope,
                    database,
                    started_in_transaction,
                    code,
                ))
            }
        }
    }
}

unsafe fn d1_rows_read(statement: *mut rusqlite::ffi::sqlite3_stmt, emitted: u64) -> u64 {
    let full_scan_steps = rusqlite::ffi::sqlite3_stmt_status(
        statement,
        rusqlite::ffi::SQLITE_STMTSTATUS_FULLSCAN_STEP,
        0,
    ) as u64;
    let scanned = full_scan_steps.saturating_add(u64::from(full_scan_steps > 0));
    emitted.max(scanned)
}

/// The schema cookie, which moves on every DDL statement. `total_changes`
/// cannot see DDL — a CREATE TABLE writes no counted row — so `changed_db`
/// compares this across the statement instead of trusting
/// `sqlite3_stmt_readonly` alone, which reported `changed_db: true` for
/// `UPDATE ... WHERE 0` where upstream reports the false its name promises.
fn d1_schema_version(connection: &Connection) -> i64 {
    without_sql_authorizer(connection, || {
        connection
            .query_row("PRAGMA schema_version", [], |row| row.get(0))
            .unwrap_or(0)
    })
}

/// SQLite's own message for a failed D1 operation, with the poison
/// classification the other storage ops apply. The message carries no
/// operation prefix, because applications match on it -- upstream reports
/// `D1_ERROR: no such table: x`, not the name of the internal step that
/// noticed.
fn d1_sqlite_failure(
    scope: &str,
    database: *mut rusqlite::ffi::sqlite3,
    started_in_transaction: bool,
    result_code: i32,
) -> String {
    let _ = sqlite_operation_failure(scope, database, started_in_transaction, result_code, "d1");
    // SAFETY: database is the live handle owned by the scope's Connection.
    unsafe {
        CStr::from_ptr(rusqlite::ffi::sqlite3_errmsg(database))
            .to_string_lossy()
            .into_owned()
    }
}

/// Whether `tail` holds another statement. Trivia -- whitespace and comments,
/// terminated or not -- prepares to a null statement, so SQLite's tokenizer
/// answers this rather than a re-implementation of it in JS.
unsafe fn d1_tail_has_statement(
    database: *mut rusqlite::ffi::sqlite3,
    tail: *const std::ffi::c_char,
) -> bool {
    if tail.is_null() {
        return false;
    }
    let mut statement = std::ptr::null_mut();
    let prepare =
        rusqlite::ffi::sqlite3_prepare_v2(database, tail, -1, &mut statement, std::ptr::null_mut());
    if !statement.is_null() {
        rusqlite::ffi::sqlite3_finalize(statement);
        return true;
    }
    // A tail that fails to prepare is not trivia either: it is a statement
    // SQLite refused, and the refusal belongs to the caller's error, not to
    // a silent drop.
    prepare != rusqlite::ffi::SQLITE_OK
}

/// The count and rows of an exec-style run over `source`. On success the
/// whole source ran; the caller owns any surrounding transaction.
unsafe fn d1_run_statements(
    scope: &str,
    connection: &Connection,
    source: &CString,
    input: &str,
    keep_rows: bool,
    failure: fn(String) -> D1Failure,
) -> Result<(u64, Vec<D1Rows>), D1Failure> {
    let database = connection.handle();
    let started_in_transaction = rusqlite::ffi::sqlite3_get_autocommit(database) == 0;
    let mut budget = D1_MAX_RESULT_BYTES;
    let mut source_ptr = source.as_ptr();
    let mut count = 0_u64;
    let mut results = Vec::new();
    loop {
        let mut statement = std::ptr::null_mut();
        let mut tail = std::ptr::null();
        let prepare =
            rusqlite::ffi::sqlite3_prepare_v2(database, source_ptr, -1, &mut statement, &mut tail);
        if prepare != rusqlite::ffi::SQLITE_OK {
            if !statement.is_null() {
                rusqlite::ffi::sqlite3_finalize(statement);
            }
            // An unterminated construct -- an open quote, a statement cut
            // mid-token -- is refused as incomplete rather than reported as
            // whatever syntax error the cut happened to produce, because the
            // statements before it have already run and the operator needs
            // to know work was dropped, not to parse SQLite's guess.
            if rusqlite::ffi::sqlite3_complete(source_ptr) == 0 {
                return Err(failure(D1_INCOMPLETE.to_string()));
            }
            return Err(failure(d1_sqlite_failure(
                scope,
                database,
                started_in_transaction,
                prepare,
            )));
        }
        if statement.is_null() {
            // Nothing but trivia remains. Unlike `sql_ingest`, nothing here
            // re-checks `sqlite3_complete` on what was consumed: exec input
            // is whole, not a stream, so a final statement without its
            // semicolon is a statement, not a fragment (upstream runs
            // `exec("select 1")`).
            return Ok((count, results));
        }
        let offset = tail.offset_from(source.as_ptr()) as usize;
        if input.get(offset..).is_none() {
            rusqlite::ffi::sqlite3_finalize(statement);
            return Err(failure("SQLite returned an invalid SQL tail".to_string()));
        }
        let columns = sql_cursor_columns(statement);
        let keep = if keep_rows && !columns.is_empty() {
            D1_MAX_ROWS
        } else {
            0
        };
        let stepped = d1_step_to_done(
            scope,
            database,
            statement,
            started_in_transaction,
            keep,
            &mut budget,
        );
        rusqlite::ffi::sqlite3_finalize(statement);
        let stepped = stepped.map_err(&failure)?;
        if let Some(over) = stepped.over {
            return Err(failure(over));
        }
        count += 1;
        if keep > 0 {
            results.push(D1Rows {
                columns,
                rows: stepped.rows,
            });
        }
        source_ptr = tail;
    }
}

unsafe fn d1_run_prepared(
    scope: &str,
    connection: &Connection,
    sql: &str,
    params: &[serde_json::Value],
    keep: usize,
    budget: &mut usize,
    started: std::time::Instant,
) -> Result<D1PreparedResult, D1Failure> {
    let mut binds = Vec::with_capacity(params.len());
    for value in params {
        binds.push(d1_bind_value(value)?);
    }
    let source =
        CString::new(sql).map_err(|error| d1_error(format!("invalid SQL text: {error}")))?;
    let database = connection.handle();
    let started_in_transaction = rusqlite::ffi::sqlite3_get_autocommit(database) == 0;
    let changes_before = total_changes_for_handle(database);
    let schema_before = d1_schema_version(connection);
    let mut statement = std::ptr::null_mut();
    let mut tail = std::ptr::null();
    let prepare =
        rusqlite::ffi::sqlite3_prepare_v2(database, source.as_ptr(), -1, &mut statement, &mut tail);
    if prepare != rusqlite::ffi::SQLITE_OK {
        if !statement.is_null() {
            rusqlite::ffi::sqlite3_finalize(statement);
        }
        return Err(d1_error(d1_sqlite_failure(
            scope,
            database,
            started_in_transaction,
            prepare,
        )));
    }
    if statement.is_null() {
        return Err(d1_error("No SQL statements detected."));
    }
    if d1_tail_has_statement(database, tail) {
        rusqlite::ffi::sqlite3_finalize(statement);
        return Err(d1_error(
            "A prepared SQL statement must contain only one statement.",
        ));
    }
    let expected = rusqlite::ffi::sqlite3_bind_parameter_count(statement) as usize;
    if expected != binds.len() {
        rusqlite::ffi::sqlite3_finalize(statement);
        return Err(d1_error(
            "Wrong number of parameter bindings for SQL query.",
        ));
    }
    for (offset, value) in binds.iter().enumerate() {
        if let Err(code) = bind_cursor_value(statement, offset as i32 + 1, value) {
            let failure = d1_sqlite_failure(scope, database, started_in_transaction, code);
            rusqlite::ffi::sqlite3_finalize(statement);
            return Err(d1_error(failure));
        }
    }
    let readonly = rusqlite::ffi::sqlite3_stmt_readonly(statement) != 0;
    let columns = sql_cursor_columns(statement);
    let stepped = d1_step_to_done(
        scope,
        database,
        statement,
        started_in_transaction,
        keep,
        budget,
    );
    let rows_read = stepped
        .as_ref()
        .ok()
        .map(|stepped| d1_rows_read(statement, stepped.rows_read));
    rusqlite::ffi::sqlite3_finalize(statement);
    let stepped = stepped.map_err(d1_error)?;
    if let Some(over) = stepped.over {
        return Err(d1_error(over));
    }
    let rows_written = total_changes_for_handle(database).saturating_sub(changes_before);
    let (last_row_id, changes) = if readonly {
        (0, 0)
    } else {
        (connection.last_insert_rowid(), connection.changes())
    };
    let changed_db =
        !readonly && (rows_written > 0 || d1_schema_version(connection) != schema_before);
    Ok(D1PreparedResult {
        columns,
        rows: stepped.rows,
        meta: D1Meta {
            duration: started.elapsed().as_secs_f64() * 1000.0,
            rows_read: rows_read.unwrap_or(stepped.rows_read),
            rows_written,
            last_row_id,
            changes,
            changed_db,
            size_after: d1_database_size(connection),
            served_by: "celld",
            served_by_region: "local",
            served_by_primary: true,
        },
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum D1TransactionOperation {
    Begin,
    Commit,
    Rollback,
}

impl D1TransactionOperation {
    fn sql(self) -> &'static str {
        match self {
            Self::Begin => "BEGIN IMMEDIATE",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
        }
    }
}

fn d1_poison_transaction(scope: &str, error: String) {
    sql_critical_errors(|errors| {
        errors
            .borrow_mut()
            .entry(scope.to_string())
            .or_insert(error);
    });
}

fn d1_rollback_after_commit_failure(scope: &str, connection: &Connection) -> Result<(), String> {
    #[cfg(celld_internal_tests)]
    if let Some(result_code) = D1_ROLLBACK_FAULT.with(std::cell::Cell::take) {
        // SAFETY: the handle belongs to this live connection.
        let database = unsafe { connection.handle() };
        return Err(d1_critical_transaction_failure(
            scope,
            database,
            result_code,
        ));
    }
    without_sql_authorizer(connection, || connection.execute_batch("ROLLBACK")).map_err(|error| {
        if let rusqlite::Error::SqliteFailure(sqlite, _) = &error {
            // SAFETY: the handle belongs to this live connection.
            let database = unsafe { connection.handle() };
            d1_critical_transaction_failure(scope, database, sqlite.extended_code)
        } else {
            error.to_string()
        }
    })
}

fn d1_transaction_control(
    scope: &str,
    connection: &Connection,
    operation: D1TransactionOperation,
    failure: fn(String) -> D1Failure,
) -> Result<(), D1Failure> {
    #[cfg(celld_internal_tests)]
    let injected = if operation == D1TransactionOperation::Commit {
        D1_COMMIT_FAULT.with(std::cell::Cell::take)
    } else {
        None
    };
    #[cfg(not(celld_internal_tests))]
    let injected: Option<i32> = None;

    let result = if let Some(result_code) = injected {
        Err((result_code, None))
    } else {
        without_sql_authorizer(connection, || connection.execute_batch(operation.sql())).map_err(
            |error| {
                let code = match &error {
                    rusqlite::Error::SqliteFailure(sqlite, _) => Some(sqlite.extended_code),
                    _ => None,
                };
                (code.unwrap_or(rusqlite::ffi::SQLITE_ERROR), Some(error))
            },
        )
    };

    result.map_err(|(result_code, error)| {
        let message = match error {
            Some(error) if operation == D1TransactionOperation::Begin => error.to_string(),
            Some(error) if !matches!(error, rusqlite::Error::SqliteFailure(_, _)) => {
                error.to_string()
            }
            _ => {
                // SAFETY: the handle belongs to this live connection. A failed
                // COMMIT or ROLLBACK can leave transaction state uncertain.
                let database = unsafe { connection.handle() };
                d1_critical_transaction_failure(scope, database, result_code)
            }
        };

        if operation == D1TransactionOperation::Commit && !connection.is_autocommit() {
            if let Err(rollback) = d1_rollback_after_commit_failure(scope, connection) {
                d1_poison_transaction(
                    scope,
                    format!("failed to roll back after COMMIT failure: {rollback}"),
                );
            }
        }

        failure(message)
    })
}

fn d1_critical_transaction_failure(
    scope: &str,
    database: *mut rusqlite::ffi::sqlite3,
    result_code: i32,
) -> String {
    let error =
        sqlite_operation_failure(scope, database, true, result_code, "control D1 transaction")
            .to_string();
    let primary = result_code & 0xff;
    if matches!(
        primary,
        rusqlite::ffi::SQLITE_FULL
            | rusqlite::ffi::SQLITE_IOERR
            | rusqlite::ffi::SQLITE_NOMEM
            | rusqlite::ffi::SQLITE_INTERRUPT
    ) {
        // A D1 call owns its transaction bracket and cannot hand an open or
        // ambiguous one back to application code. Fail closed even when
        // SQLite did not re-enable autocommit after the control failure.
        sql_critical_errors(|errors| {
            errors
                .borrow_mut()
                .entry(scope.to_string())
                .or_insert_with(|| error.clone());
        });
    }
    error
}

#[cfg(celld_internal_tests)]
thread_local! {
    static D1_COMMIT_FAULT: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
    static D1_ROLLBACK_FAULT: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
}

#[cfg(celld_internal_tests)]
pub fn set_d1_commit_fault_for_test(result_code: i32) {
    D1_COMMIT_FAULT.with(|fault| fault.set(Some(result_code)));
}

#[cfg(celld_internal_tests)]
pub fn set_d1_rollback_fault_for_test(result_code: i32) {
    D1_ROLLBACK_FAULT.with(|fault| fault.set(Some(result_code)));
}

/// Run one D1 request against a cell's SQLite: the binding's single seam
/// into the engine. Everything a statement's `meta` needs is read here,
/// inside the same execution, on the same connection.
pub fn d1_run(scope: &str, request: &D1Request) -> Result<D1Response, D1Failure> {
    require_sql_healthy(scope).map_err(d1_error)?;
    let started = std::time::Instant::now();
    match request {
        D1Request::Prepared { sql, params, first } => {
            let keep = if *first { 1 } else { D1_MAX_ROWS };
            with(scope, |connection| {
                as_user_sql(|| {
                    let mut budget = D1_MAX_RESULT_BYTES;
                    // SAFETY: the connection lives in DBS until close(), and
                    // the helper finalizes its statement on every path.
                    unsafe {
                        d1_run_prepared(scope, connection, sql, params, keep, &mut budget, started)
                    }
                    .map(D1Response::Prepared)
                })
            })
            .unwrap_or_else(|| Err(d1_error(format!("no db for {scope}"))))
        }
        D1Request::Batch { statements } => with(scope, |connection| {
            if !connection.is_autocommit() {
                return Err(d1_error("batch cannot start inside an open transaction"));
            }
            d1_transaction_control(scope, connection, D1TransactionOperation::Begin, d1_error)?;
            let ran = as_user_sql(|| {
                let mut results = Vec::with_capacity(statements.len());
                let mut budget = D1_MAX_RESULT_BYTES;
                for statement in statements {
                    let statement_started = std::time::Instant::now();
                    // SAFETY: as for the prepared mode above. Every statement
                    // uses this connection inside the same transaction.
                    results.push(unsafe {
                        d1_run_prepared(
                            scope,
                            connection,
                            &statement.sql,
                            &statement.params,
                            D1_MAX_ROWS,
                            &mut budget,
                            statement_started,
                        )
                    }?);
                }
                Ok(results)
            });
            match ran {
                Ok(results) => {
                    d1_transaction_control(
                        scope,
                        connection,
                        D1TransactionOperation::Commit,
                        d1_error,
                    )?;
                    Ok(D1Response::Batch(results))
                }
                Err(failure) => {
                    if !connection.is_autocommit() {
                        let _ = d1_transaction_control(
                            scope,
                            connection,
                            D1TransactionOperation::Rollback,
                            d1_error,
                        );
                    }
                    Err(failure)
                }
            }
        })
        .unwrap_or_else(|| Err(d1_error(format!("no db for {scope}")))),
        D1Request::Exec { sql, rows } => {
            let source = CString::new(sql.as_str())
                .map_err(|error| d1_exec_error(format!("invalid SQL text: {error}")))?;
            with(scope, |connection| {
                // One transaction per exec, as upstream D1 runs it (verified
                // against workerd's D1 under miniflare: a failing line rolls
                // the whole call back). Without the bracket the statements
                // before a failure land, so "the exec failed" and "half the
                // exec happened" were both true at once — the worst reading
                // for an operator importing a dump.
                if !connection.is_autocommit() {
                    return Err(d1_exec_error(
                        "exec cannot start inside an open transaction",
                    ));
                }
                d1_transaction_control(
                    scope,
                    connection,
                    D1TransactionOperation::Begin,
                    d1_exec_error,
                )?;
                let ran = as_user_sql(|| {
                    // SAFETY: the connection lives in DBS until close();
                    // every prepared statement is finalized on all paths.
                    unsafe {
                        d1_run_statements(scope, connection, &source, sql, *rows, d1_exec_error)
                    }
                });
                match ran {
                    Ok((count, results)) => {
                        d1_transaction_control(
                            scope,
                            connection,
                            D1TransactionOperation::Commit,
                            d1_exec_error,
                        )?;
                        Ok(D1Response::Exec {
                            count,
                            duration: started.elapsed().as_secs_f64() * 1000.0,
                            results,
                        })
                    }
                    Err(failure) => {
                        if !connection.is_autocommit() {
                            let _ = d1_transaction_control(
                                scope,
                                connection,
                                D1TransactionOperation::Rollback,
                                d1_exec_error,
                            );
                        }
                        Err(failure)
                    }
                }
            })
            .unwrap_or_else(|| Err(d1_exec_error(format!("no db for {scope}"))))
        }
        D1Request::Migrate { name, sql, table } => {
            if !d1_plain_identifier(table) {
                return Err(d1_error(format!(
                    "migrations_table {table:?} is not a plain identifier"
                )));
            }
            let source = CString::new(sql.as_str())
                .map_err(|error| d1_error(format!("invalid SQL text: {error}")))?;
            with(scope, |connection| {
                // The transaction is what makes the durability claim true:
                // a migration that fails part-way rolls back whole, and the
                // operator fixes the file and re-runs. Without it the
                // statements that DID land are exactly the ones a re-run
                // trips over. BEGIN/COMMIT run outside `as_user_sql` -- they
                // are the engine's own bracket, not application SQL.
                if !connection.is_autocommit() {
                    return Err(d1_error(
                        "a migration cannot start inside an open transaction",
                    ));
                }
                // The authorizer denies Transaction actions outright -- that
                // is what keeps BEGIN out of application SQL -- so the
                // engine's own bracket must step around it, exactly as
                // `transaction_control` does.
                d1_transaction_control(scope, connection, D1TransactionOperation::Begin, d1_error)?;
                let applied = as_user_sql(|| {
                    // SAFETY: as for the exec mode above.
                    let (count, _) = unsafe {
                        d1_run_statements(scope, connection, &source, sql, false, d1_error)
                    }?;
                    // The bookkeeping row rides the same transaction. The
                    // name is a bound parameter, so a file name never
                    // reaches the SQL text.
                    connection
                        .execute(
                            &format!(
                                "CREATE TABLE IF NOT EXISTS \"{table}\" (\
                                 id INTEGER PRIMARY KEY AUTOINCREMENT, \
                                 name TEXT UNIQUE, \
                                 applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)"
                            ),
                            [],
                        )
                        .map_err(|error| d1_error(error.to_string()))?;
                    connection
                        .execute(
                            &format!("INSERT INTO \"{table}\" (name) VALUES (?)"),
                            [name],
                        )
                        .map_err(|error| d1_error(error.to_string()))?;
                    Ok(count)
                });
                match applied {
                    Ok(count) => {
                        d1_transaction_control(
                            scope,
                            connection,
                            D1TransactionOperation::Commit,
                            d1_error,
                        )?;
                        Ok(D1Response::Migrate {
                            count,
                            duration: started.elapsed().as_secs_f64() * 1000.0,
                        })
                    }
                    Err(failure) => {
                        // Keep the statement error. SQLite can already have
                        // rolled the transaction back, and a real rollback
                        // failure goes through the critical-error classifier.
                        if !connection.is_autocommit() {
                            let _ = d1_transaction_control(
                                scope,
                                connection,
                                D1TransactionOperation::Rollback,
                                d1_error,
                            );
                        }
                        Err(failure)
                    }
                }
            })
            .unwrap_or_else(|| Err(d1_error(format!("no db for {scope}"))))
        }
    }
}

fn d1_plain_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn d1_database_size(connection: &Connection) -> u64 {
    without_sql_authorizer(connection, || {
        connection
            .query_row(
                "SELECT (page_count - freelist_count) * page_size \
                 FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size()",
                [],
                |row| row.get::<_, u64>(0),
            )
            .unwrap_or(0)
    })
}

/// The string-in, string-out wrapper the V8 op calls: `{ok: ...}` or
/// `{error: {family, message}}`.
pub fn d1_run_json(scope: &str, request: &str) -> String {
    let response = match serde_json::from_str::<D1Request>(request) {
        Ok(request) => d1_run(scope, &request),
        Err(error) => Err(d1_error(format!("invalid D1 request: {error}"))),
    };
    match response {
        Ok(response) => serde_json::json!({ "ok": response }).to_string(),
        Err(failure) => serde_json::json!({
            "error": { "family": failure.family, "message": failure.message },
        })
        .to_string(),
    }
}

/// `Ok(Some(at_ms))` when an outermost commit published a dirty committed
/// alarm: the caller performs the arm-time wake-entry gate before acking the
/// transaction. Rollbacks restore previously committed (already covered)
/// state and never gate.
pub fn transaction_control(
    scope: &str,
    action: &str,
    nested: bool,
    savepoint: &str,
) -> Result<Option<i64>, String> {
    if let Some(error) = sql_critical_error(scope) {
        let result = match action {
            "rollback" => Ok(None),
            "rollback_explicit" => Err(error),
            "commit" => {
                Err("Cannot commit transaction due to an earlier SQL critical error".to_string())
            }
            _ => Err(error),
        };
        if result.is_ok() && !nested && action == "rollback" {
            publish_alarm_if_transaction_dirty(scope);
        }
        return result;
    }
    if nested
        && savepoint
            .strip_prefix("cells_tx_")
            .is_none_or(|suffix| suffix.parse::<u64>().is_err())
    {
        return Err("invalid storage transaction savepoint".to_string());
    }
    let result = with(scope, |connection| {
        without_sql_authorizer(connection, || {
            let query = match (action, nested) {
                ("start", false) => "BEGIN IMMEDIATE".to_string(),
                ("start", true) => format!("SAVEPOINT {savepoint}"),
                ("commit", false) => "COMMIT".to_string(),
                ("commit", true) => format!("RELEASE {savepoint}"),
                ("rollback", false) => "ROLLBACK".to_string(),
                ("rollback_explicit", false) => "ROLLBACK".to_string(),
                ("rollback", true) => {
                    format!("ROLLBACK TO {savepoint}; RELEASE {savepoint}")
                }
                ("rollback_explicit", true) => {
                    format!("ROLLBACK TO {savepoint}; RELEASE {savepoint}")
                }
                _ => return Err("invalid storage transaction action".to_string()),
            };
            connection.execute_batch(&query).map_err(|error| {
                // A critical failure while committing or rolling back can make
                // SQLite destroy the whole transaction; classify it so the
                // actor poisons exactly like a mid-statement critical error.
                if action != "start" {
                    if let rusqlite::Error::SqliteFailure(failure, _) = &error {
                        // SAFETY: the handle belongs to this live connection.
                        let database = unsafe { connection.handle() };
                        return sqlite_operation_failure(
                            scope,
                            database,
                            true,
                            failure.extended_code,
                            "control storage transaction",
                        )
                        .to_string();
                    }
                }
                error.to_string()
            })
        })
    })
    .unwrap_or_else(|| Err(format!("no db for {scope}")));
    let result = result.map(|()| None);
    if result.is_ok() && !nested && matches!(action, "commit" | "rollback" | "rollback_explicit") {
        let published = publish_alarm_if_transaction_dirty(scope);
        if action == "commit" {
            return Ok(published);
        }
    }
    result
}

#[cfg(celld_internal_tests)]
pub fn set_query_only_for_test(scope: &str, enabled: bool) -> anyhow::Result<()> {
    with(scope, |connection| {
        without_sql_authorizer(connection, || {
            connection
                .pragma_update(None, "query_only", enabled)
                .map_err(Into::into)
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

#[cfg(celld_internal_tests)]
pub fn set_max_page_count_for_test(scope: &str, pages: u32) -> anyhow::Result<()> {
    with(scope, |connection| {
        without_sql_authorizer(connection, || {
            connection
                .pragma_update(None, "max_page_count", pages)
                .map_err(Into::into)
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

#[cfg(celld_internal_tests)]
pub fn set_write_fault_for_test(enabled: bool) {
    crate::fault::set_write_fault(enabled);
}

/// Shrink the page cache so a large write spills to disk mid-statement.
#[cfg(celld_internal_tests)]
pub fn set_cache_size_for_test(scope: &str, pages: i32) -> anyhow::Result<()> {
    with(scope, |connection| {
        without_sql_authorizer(connection, || {
            connection
                .pragma_update(None, "cache_size", pages)
                .map_err(Into::into)
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Interrupt the next statement on `scope` through SQLite's real interrupt
/// machinery: a progress handler that aborts on its first callback.
#[cfg(celld_internal_tests)]
pub fn set_interrupt_fault_for_test(scope: &str, enabled: bool) -> anyhow::Result<()> {
    unsafe extern "C" fn interrupt(_: *mut std::ffi::c_void) -> std::ffi::c_int {
        1
    }
    with(scope, |connection| {
        // SAFETY: the handle belongs to this live connection.
        unsafe {
            let database = connection.handle();
            if enabled {
                rusqlite::ffi::sqlite3_progress_handler(
                    database,
                    1,
                    Some(interrupt),
                    std::ptr::null_mut(),
                );
            } else {
                rusqlite::ffi::sqlite3_progress_handler(database, 0, None, std::ptr::null_mut());
            }
        }
        Ok(())
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Register `cells_nomem_for_test()`, a scalar that reports allocation failure
/// through SQLite's own OOM path (`sqlite3_result_error_nomem`).
#[cfg(celld_internal_tests)]
pub fn register_nomem_function_for_test(scope: &str) -> anyhow::Result<()> {
    unsafe extern "C" fn nomem(
        context: *mut rusqlite::ffi::sqlite3_context,
        _argc: std::ffi::c_int,
        _argv: *mut *mut rusqlite::ffi::sqlite3_value,
    ) {
        rusqlite::ffi::sqlite3_result_error_nomem(context);
    }
    with(scope, |connection| {
        // SAFETY: the handle belongs to this live connection; the function is
        // stateless.
        let rc = unsafe {
            rusqlite::ffi::sqlite3_create_function_v2(
                connection.handle(),
                c"cells_nomem_for_test".as_ptr(),
                0,
                rusqlite::ffi::SQLITE_UTF8,
                std::ptr::null_mut(),
                Some(nomem),
                None,
                None,
                None,
            )
        };
        anyhow::ensure!(
            rc == rusqlite::ffi::SQLITE_OK,
            "create nomem function: {rc}",
        );
        Ok(())
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

#[cfg(celld_internal_tests)]
pub fn get(scope: &str, key: &str) -> Option<String> {
    with(scope, |c| {
        c.prepare_cached(KV_GET_SQL)
            .ok()?
            .query_row([scope, key], |r| r.get::<_, String>(0))
            .ok()
    })?
}

pub fn get_stored(scope: &str, key: &str) -> anyhow::Result<Option<StoredValue>> {
    with(scope, |c| {
        c.prepare_cached(KV_GET_SQL)?
            .query_row([scope, key], |row| Ok(stored_value(row.get_ref(0)?)))
            .optional()
            .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

#[cfg(celld_internal_tests)]
pub fn key_names_for_test(scope: &str) -> anyhow::Result<Vec<String>> {
    with(scope, |connection| {
        let mut statement = connection.prepare("SELECT k FROM _cf_KV WHERE scope=?1 ORDER BY k")?;
        let rows = statement.query_map([scope], |row| row.get(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Fetch multiple keys in storage order, omitting absent keys. Duplicate input
/// keys are coalesced, matching Durable Object storage's map-like result.
#[cfg(celld_internal_tests)]
pub fn get_many(scope: &str, keys: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    get_many_stored(scope, keys)?
        .into_iter()
        .map(|(key, value)| match value {
            StoredValue::LegacyJson(value) => Ok((key, value)),
            StoredValue::V8(_) => Err(anyhow::anyhow!(
                "structured-clone value cannot be read as legacy JSON",
            )),
        })
        .collect()
}

pub fn get_many_stored(scope: &str, keys: &[String]) -> anyhow::Result<Vec<(String, StoredValue)>> {
    let mut keys = keys.to_vec();
    keys.sort();
    keys.dedup();
    with(scope, |c| {
        let mut statement = c.prepare_cached(KV_GET_SQL)?;
        let mut values = Vec::new();
        for key in keys {
            if let Some(value) = statement
                .query_row(rusqlite::params![scope, key], |row| {
                    Ok(stored_value(row.get_ref(0)?))
                })
                .optional()?
            {
                values.push((key, value));
            }
        }
        Ok(values)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

#[cfg(celld_internal_tests)]
pub fn put(scope: &str, key: &str, val: &str) {
    with(scope, |c| {
        c.prepare_cached(KV_PUT_SQL)
            .and_then(|mut statement| statement.execute([scope, key, val]))
    });
}

/// Run an application-visible storage write and preserve a critical SQLite
/// failure before another API can reuse the connection. SQLite can end an
/// explicit transaction when a write returns FULL, IOERR, NOMEM, or
/// INTERRUPT. The caller can catch that statement error, so every write entry
/// point must reject a later mutation after the transaction has disappeared.
fn with_application_storage_write<T>(
    scope: &str,
    write: impl FnOnce(&mut Connection) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    require_sql_healthy(scope).map_err(anyhow::Error::msg)?;
    with_mut(scope, |connection| {
        let started_in_transaction = !connection.is_autocommit();
        write(connection).inspect_err(|error| {
            let result_code = error.chain().find_map(|cause| {
                let rusqlite::Error::SqliteFailure(failure, _) =
                    cause.downcast_ref::<rusqlite::Error>()?
                else {
                    return None;
                };
                Some(failure.extended_code)
            });
            if let Some(result_code) = result_code {
                // SAFETY: the handle belongs to this live connection.
                let database = unsafe { connection.handle() };
                record_sqlite_operation_failure(
                    scope,
                    database,
                    started_in_transaction,
                    result_code,
                    &error.to_string(),
                );
            }
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

pub fn put_serialized(scope: &str, key: &str, value: &[u8]) -> anyhow::Result<()> {
    with_application_storage_write(scope, |c| {
        c.prepare_cached(KV_PUT_SQL)?
            .execute(rusqlite::params![scope, key, value])
            .map(|_| ())
            .map_err(Into::into)
    })
}

/// Atomically apply a multi-key put.
#[cfg(celld_internal_tests)]
pub fn put_many(scope: &str, entries: &[(String, String)]) -> anyhow::Result<()> {
    if entries.is_empty() {
        return with(scope, |_| ()).ok_or_else(|| anyhow::anyhow!("no db for {scope}"));
    }
    with_mut(scope, |c| {
        without_sql_authorizer_mut(c, |c| {
            if !c.is_autocommit() {
                return with_batch_savepoint(c, |c| {
                    let mut statement = c.prepare_cached(KV_PUT_SQL)?;
                    for (key, value) in entries {
                        statement.execute(rusqlite::params![scope, key, value])?;
                    }
                    Ok(())
                });
            }
            let transaction =
                c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            {
                let mut statement = transaction.prepare_cached(KV_PUT_SQL)?;
                for (key, value) in entries {
                    statement.execute(rusqlite::params![scope, key, value])?;
                }
            }
            transaction.commit()?;
            Ok(())
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

pub fn put_many_serialized(scope: &str, entries: &[(String, Vec<u8>)]) -> anyhow::Result<()> {
    if entries.is_empty() {
        return with(scope, |_| ()).ok_or_else(|| anyhow::anyhow!("no db for {scope}"));
    }
    with_application_storage_write(scope, |c| {
        without_sql_authorizer_mut(c, |c| {
            let write = |c: &Connection| -> anyhow::Result<()> {
                let mut statement = c.prepare_cached(KV_PUT_SQL)?;
                for (key, value) in entries {
                    statement.execute(rusqlite::params![scope, key, value])?;
                }
                Ok(())
            };
            if !c.is_autocommit() {
                return with_batch_savepoint(c, write);
            }
            let transaction =
                c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            write(&transaction)?;
            transaction.commit()?;
            Ok(())
        })
    })
}

pub fn delete(scope: &str, key: &str) -> anyhow::Result<bool> {
    with_application_storage_write(scope, |c| {
        c.prepare_cached(KV_DELETE_SQL)?
            .execute([scope, key])
            .map(|n| n > 0)
            .map_err(Into::into)
    })
}

/// Atomically delete multiple keys and return the number that existed.
pub fn delete_many(scope: &str, keys: &[String]) -> anyhow::Result<usize> {
    if keys.is_empty() {
        return with(scope, |_| 0).ok_or_else(|| anyhow::anyhow!("no db for {scope}"));
    }
    with_application_storage_write(scope, |c| {
        without_sql_authorizer_mut(c, |c| {
            if !c.is_autocommit() {
                return with_batch_savepoint(c, |c| {
                    let mut statement = c.prepare_cached(KV_DELETE_SQL)?;
                    let mut deleted = 0;
                    for key in keys {
                        deleted += statement.execute(rusqlite::params![scope, key])?;
                    }
                    Ok(deleted)
                });
            }
            let transaction =
                c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let mut deleted = 0;
            {
                let mut statement = transaction.prepare_cached(KV_DELETE_SQL)?;
                for key in keys {
                    deleted += statement.execute(rusqlite::params![scope, key])?;
                }
            }
            transaction.commit()?;
            Ok(deleted)
        })
    })
}

/// List a half-open key range in bytewise SQLite text order.
#[cfg(celld_internal_tests)]
pub fn list(
    scope: &str,
    begin: Option<&str>,
    end: Option<&str>,
    limit: Option<usize>,
    reverse: bool,
) -> anyhow::Result<Vec<(String, String)>> {
    list_with_options(scope, begin, end, None, None, limit, reverse)
}

/// List storage using the full Durable Object key-filter surface. Predicates
/// stay in SQLite so large objects are not materialized and filtered in V8.
#[cfg(celld_internal_tests)]
pub fn list_with_options(
    scope: &str,
    begin: Option<&str>,
    end: Option<&str>,
    start_after: Option<&str>,
    prefix: Option<&str>,
    limit: Option<usize>,
    reverse: bool,
) -> anyhow::Result<Vec<(String, String)>> {
    list_stored_with_options(scope, begin, end, start_after, prefix, limit, reverse)?
        .into_iter()
        .map(|(key, value)| match value {
            StoredValue::LegacyJson(value) => Ok((key, value)),
            StoredValue::V8(_) => Err(anyhow::anyhow!(
                "structured-clone value cannot be read as legacy JSON",
            )),
        })
        .collect()
}

pub fn list_stored_with_options(
    scope: &str,
    begin: Option<&str>,
    end: Option<&str>,
    start_after: Option<&str>,
    prefix: Option<&str>,
    limit: Option<usize>,
    reverse: bool,
) -> anyhow::Result<Vec<(String, StoredValue)>> {
    with(scope, |c| {
        let (query, parameters) =
            list_query(scope, begin, end, start_after, prefix, limit, reverse);
        let mut statement = c.prepare(&query)?;
        let rows = statement.query_map(rusqlite::params_from_iter(parameters), |row| {
            Ok((row.get(0)?, stored_value(row.get_ref(1)?)))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

fn list_query(
    scope: &str,
    begin: Option<&str>,
    end: Option<&str>,
    start_after: Option<&str>,
    prefix: Option<&str>,
    limit: Option<usize>,
    reverse: bool,
) -> (String, Vec<rusqlite::types::Value>) {
    use rusqlite::types::Value;

    let order = if reverse { "DESC" } else { "ASC" };
    let mut query = String::from("SELECT k,v FROM _cf_KV WHERE scope=?");
    let mut parameters = vec![Value::Text(scope.to_string())];
    if let Some(begin) = begin {
        query.push_str(" AND k>=?");
        parameters.push(Value::Text(begin.to_string()));
    }
    if let Some(start_after) = start_after {
        query.push_str(" AND k>?");
        parameters.push(Value::Text(start_after.to_string()));
    }
    if let Some(end) = end {
        query.push_str(" AND k<?");
        parameters.push(Value::Text(end.to_string()));
    }
    if let Some(prefix) = prefix {
        query.push_str(" AND k>=?");
        parameters.push(Value::Text(prefix.to_string()));
        if let Some(upper_bound) = prefix_upper_bound(prefix) {
            query.push_str(" AND k<?");
            parameters.push(Value::Text(upper_bound));
        }
    }
    query.push_str(&format!(" ORDER BY k {order} LIMIT ?"));
    parameters.push(Value::Integer(
        limit.unwrap_or(usize::MAX).min(i64::MAX as usize) as i64,
    ));
    (query, parameters)
}

fn sqlite_failure(database: *mut rusqlite::ffi::sqlite3, operation: &str) -> anyhow::Error {
    // SAFETY: database is the live handle owned by the scope's Connection.
    let detail = unsafe {
        CStr::from_ptr(rusqlite::ffi::sqlite3_errmsg(database))
            .to_string_lossy()
            .into_owned()
    };
    anyhow::anyhow!("{operation}: {detail}")
}

pub fn sql_critical_error(scope: &str) -> Option<String> {
    sql_critical_errors(|errors| errors.borrow().get(scope).cloned())
}

fn require_sql_healthy(scope: &str) -> Result<(), String> {
    match sql_critical_error(scope) {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn record_sqlite_operation_failure(
    scope: &str,
    database: *mut rusqlite::ffi::sqlite3,
    started_in_transaction: bool,
    result_code: i32,
    error: &str,
) {
    // The classification is sans-IO (`celld_logic::sqlite`); only the autocommit
    // read is FFI. Pin the reified codes against rusqlite's at COMPILE time, so
    // a drift is a build error, not a debug-only surprise.
    const _: () = assert!(
        celld_logic::sqlite::SQLITE_FULL == rusqlite::ffi::SQLITE_FULL
            && celld_logic::sqlite::SQLITE_IOERR == rusqlite::ffi::SQLITE_IOERR
            && celld_logic::sqlite::SQLITE_NOMEM == rusqlite::ffi::SQLITE_NOMEM
            && celld_logic::sqlite::SQLITE_INTERRUPT == rusqlite::ffi::SQLITE_INTERRUPT,
    );
    let now_autocommit = unsafe { rusqlite::ffi::sqlite3_get_autocommit(database) != 0 };
    if celld_logic::sqlite::poisons_actor(result_code, started_in_transaction, now_autocommit) {
        sql_critical_errors(|errors| {
            errors
                .borrow_mut()
                .entry(scope.to_string())
                .or_insert_with(|| error.to_string());
        });
    }
}

fn sqlite_operation_failure(
    scope: &str,
    database: *mut rusqlite::ffi::sqlite3,
    started_in_transaction: bool,
    result_code: i32,
    operation: &str,
) -> anyhow::Error {
    let error = sqlite_failure(database, operation);
    record_sqlite_operation_failure(
        scope,
        database,
        started_in_transaction,
        result_code,
        &error.to_string(),
    );
    error
}

/// On failure this returns the raw result code and formats nothing, because
/// its callers disagree on message shape: the KV cursor paths prefix the
/// internal step, and D1 must not — applications match on SQLite's text, and
/// a D1 bind that surfaced "bind sync KV cursor: ..." pointed the reader at
/// a KV cursor no D1 request ever touches.
unsafe fn bind_cursor_value(
    statement: *mut rusqlite::ffi::sqlite3_stmt,
    index: i32,
    value: &rusqlite::types::Value,
) -> Result<(), i32> {
    use rusqlite::types::Value;
    let result = match value {
        Value::Null => rusqlite::ffi::sqlite3_bind_null(statement, index),
        Value::Integer(value) => rusqlite::ffi::sqlite3_bind_int64(statement, index, *value),
        Value::Real(value) => rusqlite::ffi::sqlite3_bind_double(statement, index, *value),
        Value::Text(value) => rusqlite::ffi::sqlite3_bind_text(
            statement,
            index,
            value.as_ptr().cast(),
            value.len().min(i32::MAX as usize) as i32,
            rusqlite::ffi::SQLITE_TRANSIENT(),
        ),
        Value::Blob(value) => rusqlite::ffi::sqlite3_bind_blob(
            statement,
            index,
            value.as_ptr().cast(),
            value.len().min(i32::MAX as usize) as i32,
            rusqlite::ffi::SQLITE_TRANSIENT(),
        ),
    };
    if result == rusqlite::ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(result)
    }
}

fn close_sync_list_cursors(scope: &str) {
    sync_list_cursors(|cursors| {
        cursors
            .borrow_mut()
            .retain(|_, cursor| cursor.scope != scope);
    });
}

fn close_sql_cursors(scope: &str) {
    sql_cursors(|cursors| {
        cursors
            .borrow_mut()
            .retain(|_, cursor| cursor.scope != scope);
    });
}

pub fn sync_list_start(
    scope: &str,
    begin: Option<&str>,
    end: Option<&str>,
    start_after: Option<&str>,
    prefix: Option<&str>,
    limit: Option<usize>,
    reverse: bool,
) -> anyhow::Result<u64> {
    close_sync_list_cursors(scope);
    let (query, parameters) = list_query(scope, begin, end, start_after, prefix, limit, reverse);
    let query = CString::new(query)?;
    let cursor = with(scope, |connection| -> anyhow::Result<SyncListCursor> {
        // SAFETY: the connection remains resident in DBS for the cursor's
        // lifetime. close() finalizes all of its cursors before removal.
        unsafe {
            let database = connection.handle();
            let mut statement = std::ptr::null_mut();
            let result = rusqlite::ffi::sqlite3_prepare_v2(
                database,
                query.as_ptr(),
                -1,
                &mut statement,
                std::ptr::null_mut(),
            );
            if result != rusqlite::ffi::SQLITE_OK {
                return Err(sqlite_failure(database, "prepare sync KV cursor"));
            }
            for (offset, value) in parameters.iter().enumerate() {
                if bind_cursor_value(statement, offset as i32 + 1, value).is_err() {
                    let error = sqlite_failure(database, "bind sync KV cursor");
                    rusqlite::ffi::sqlite3_finalize(statement);
                    return Err(error);
                }
            }
            Ok(SyncListCursor {
                scope: scope.to_string(),
                database,
                statement,
            })
        }
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))?;
    let id = NEXT_SYNC_LIST_CURSOR.fetch_add(1, Ordering::Relaxed);
    sync_list_cursors(|cursors| cursors.borrow_mut().insert(id, cursor));
    Ok(id)
}

pub fn sync_list_next(cursor_id: u64) -> anyhow::Result<Option<(String, StoredValue)>> {
    sync_list_cursors(|cursors| {
        let mut cursors = cursors.borrow_mut();
        let cursor = cursors
            .get_mut(&cursor_id)
            .ok_or_else(|| anyhow::anyhow!("sync KV cursor was invalidated"))?;
        // SAFETY: the cursor owns a prepared statement on its still-live
        // connection. Column bytes are copied before the next step/finalize.
        let result = unsafe { rusqlite::ffi::sqlite3_step(cursor.statement) };
        match result {
            rusqlite::ffi::SQLITE_ROW => {
                let key = unsafe {
                    let length = rusqlite::ffi::sqlite3_column_bytes(cursor.statement, 0) as usize;
                    let pointer = rusqlite::ffi::sqlite3_column_text(cursor.statement, 0);
                    if length == 0 {
                        String::new()
                    } else {
                        String::from_utf8_lossy(std::slice::from_raw_parts(pointer, length))
                            .into_owned()
                    }
                };
                let value = unsafe {
                    match rusqlite::ffi::sqlite3_column_type(cursor.statement, 1) {
                        rusqlite::ffi::SQLITE_BLOB => {
                            let length =
                                rusqlite::ffi::sqlite3_column_bytes(cursor.statement, 1) as usize;
                            let pointer =
                                rusqlite::ffi::sqlite3_column_blob(cursor.statement, 1).cast();
                            StoredValue::V8(if length == 0 {
                                Vec::new()
                            } else {
                                std::slice::from_raw_parts(pointer, length).to_vec()
                            })
                        }
                        rusqlite::ffi::SQLITE_TEXT => {
                            let length =
                                rusqlite::ffi::sqlite3_column_bytes(cursor.statement, 1) as usize;
                            let pointer = rusqlite::ffi::sqlite3_column_text(cursor.statement, 1);
                            StoredValue::LegacyJson(if length == 0 {
                                String::new()
                            } else {
                                String::from_utf8_lossy(std::slice::from_raw_parts(pointer, length))
                                    .into_owned()
                            })
                        }
                        rusqlite::ffi::SQLITE_INTEGER => StoredValue::LegacyJson(
                            rusqlite::ffi::sqlite3_column_int64(cursor.statement, 1).to_string(),
                        ),
                        rusqlite::ffi::SQLITE_FLOAT => StoredValue::LegacyJson(
                            rusqlite::ffi::sqlite3_column_double(cursor.statement, 1).to_string(),
                        ),
                        _ => StoredValue::LegacyJson("null".to_string()),
                    }
                };
                Ok(Some((key, value)))
            }
            rusqlite::ffi::SQLITE_DONE => {
                cursors.remove(&cursor_id);
                Ok(None)
            }
            _ => {
                let error = sqlite_failure(cursor.database, "step sync KV cursor");
                cursors.remove(&cursor_id);
                Err(error)
            }
        }
    })
}

/// Smallest string strictly above every string beginning with `prefix` under
/// SQLite's default binary UTF-8 collation.
fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut characters = prefix.chars().collect::<Vec<_>>();
    for index in (0..characters.len()).rev() {
        let mut next = characters[index] as u32 + 1;
        while next <= char::MAX as u32 {
            if let Some(character) = char::from_u32(next) {
                characters.truncate(index);
                characters.push(character);
                return Some(characters.into_iter().collect());
            }
            next += 1;
        }
    }
    None
}

/// A local SQLite transaction implementing the portable portion of Workerd's
/// ActorCache transaction contract.
#[cfg(celld_internal_tests)]
pub struct KvTransaction<'connection> {
    scope: String,
    transaction: rusqlite::Transaction<'connection>,
}

#[cfg(celld_internal_tests)]
impl KvTransaction<'_> {
    pub fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        match self
            .transaction
            .prepare_cached(KV_GET_SQL)?
            .query_row(rusqlite::params![self.scope, key], |row| row.get(0))
        {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn get_many(&self, keys: &[String]) -> anyhow::Result<Vec<(String, String)>> {
        let mut keys = keys.to_vec();
        keys.sort();
        keys.dedup();
        let mut statement = self.transaction.prepare_cached(KV_GET_SQL)?;
        let mut values = Vec::new();
        for key in keys {
            if let Some(value) = statement
                .query_row(rusqlite::params![self.scope, key], |row| {
                    row.get::<_, String>(0)
                })
                .optional()?
            {
                values.push((key, value));
            }
        }
        Ok(values)
    }

    pub fn put(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.transaction
            .prepare_cached(KV_PUT_SQL)?
            .execute(rusqlite::params![self.scope, key, value])?;
        Ok(())
    }

    pub fn put_many(&self, entries: &[(String, String)]) -> anyhow::Result<()> {
        let mut statement = self.transaction.prepare_cached(KV_PUT_SQL)?;
        for (key, value) in entries {
            statement.execute(rusqlite::params![self.scope, key, value])?;
        }
        Ok(())
    }

    pub fn delete_many(&self, keys: &[String]) -> anyhow::Result<usize> {
        let mut statement = self.transaction.prepare_cached(KV_DELETE_SQL)?;
        let mut deleted = 0;
        for key in keys {
            deleted += statement.execute(rusqlite::params![self.scope, key])?;
        }
        Ok(deleted)
    }

    pub fn get_alarm(&self) -> anyhow::Result<Option<i64>> {
        Ok(self
            .transaction
            .query_row(
                "SELECT at_ms FROM _cf_ALARM WHERE scope=?1",
                [self.scope.as_str()],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn set_alarm(&self, at_ms: i64) -> anyhow::Result<()> {
        self.transaction.execute(
            "INSERT INTO _cf_ALARM(scope,at_ms,retry,counted_retry,generation) \
             VALUES(?1,?2,0,0,random()) \
             ON CONFLICT(scope) DO UPDATE SET \
               at_ms=excluded.at_ms, retry=0, counted_retry=0, generation=random()",
            rusqlite::params![self.scope, at_ms],
        )?;
        Ok(())
    }

    pub fn delete_alarm(&self) -> anyhow::Result<()> {
        self.transaction.execute(
            "DELETE FROM _cf_ALARM WHERE scope=?1",
            [self.scope.as_str()],
        )?;
        Ok(())
    }

    pub fn list(
        &self,
        begin: Option<&str>,
        end: Option<&str>,
        limit: Option<usize>,
        reverse: bool,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let order = if reverse { "DESC" } else { "ASC" };
        let query = format!(
            "SELECT k,v FROM _cf_KV \
             WHERE scope=?1 AND (?2 IS NULL OR k>=?2) AND (?3 IS NULL OR k<?3) \
             ORDER BY k {order} LIMIT ?4",
        );
        let mut statement = self.transaction.prepare(&query)?;
        let rows = statement.query_map(
            rusqlite::params![
                self.scope,
                begin,
                end,
                limit.unwrap_or(usize::MAX).min(i64::MAX as usize) as i64,
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

/// Run a KV transaction and commit only if the callback succeeds.
#[cfg(celld_internal_tests)]
pub fn transaction<T>(
    scope: &str,
    callback: impl FnOnce(&KvTransaction<'_>) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    with_mut(scope, |connection| {
        without_sql_authorizer_mut(connection, |connection| {
            let transaction =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let transaction = KvTransaction {
                scope: scope.to_string(),
                transaction,
            };
            let value = callback(&transaction)?;
            transaction.transaction.commit()?;
            Ok(value)
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Persist the optional human-readable name associated with an idFromName()
/// actor. This is runtime identity metadata, not user storage, so deleteAll()
/// must not remove it.
pub fn set_actor_name(scope: &str, name: &str) -> anyhow::Result<()> {
    with(scope, |connection| -> anyhow::Result<()> {
        connection.execute(
            "INSERT INTO _cf_METADATA(scope, actor_name) VALUES(?1, ?2) \
             ON CONFLICT(scope) DO NOTHING",
            rusqlite::params![scope, name],
        )?;
        let stored: Option<String> = connection
            .query_row(
                "SELECT actor_name FROM _cf_METADATA WHERE scope=?1",
                [scope],
                |row| row.get(0),
            )
            .optional()?;
        if stored.as_deref() != Some(name) {
            anyhow::bail!("actor name conflicts with persisted identity for {scope}");
        }
        Ok(())
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

pub fn get_actor_name(scope: &str) -> anyhow::Result<Option<String>> {
    with(scope, |connection| {
        connection
            .query_row(
                "SELECT actor_name FROM _cf_METADATA WHERE scope=?1",
                [scope],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Delete all user storage, optionally deleting the alarm. Workerd's
/// ActorCache exposes this distinction internally; compatibility flags decide
/// which form the public JS deleteAll() operation selects.
pub fn delete_all_with_alarm(scope: &str, delete_alarm: bool) -> anyhow::Result<()> {
    let result = with_application_storage_write(scope, |c| -> anyhow::Result<()> {
        let mut statement = c.prepare(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        )?;
        let tables = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        without_sql_authorizer(c, || {
            c.execute_batch("PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE;")?;
            let result = (|| -> anyhow::Result<()> {
                for table in tables {
                    if table == "_cf_METADATA" || (!delete_alarm && table == "_cf_ALARM") {
                        continue;
                    }
                    // The ltx replicator owns its control tables. Dropping
                    // them here fails every subsequent WAL capture for the
                    // cell ("no such table: _litestream_seq") until the
                    // database is reopened, which also wedges the output
                    // gate: writes after deleteAll() can never prove durable.
                    if celld_ltx::db::is_control_table(&table) {
                        continue;
                    }
                    let quoted = table.replace('"', "\"\"");
                    c.execute_batch(&format!("DROP TABLE IF EXISTS \"{quoted}\";"))?;
                }
                c.execute_batch("COMMIT;")?;
                schema(c)?;
                Ok(())
            })();
            if result.is_err() {
                let _ = c.execute_batch("ROLLBACK;");
            }
            let _ = c.execute_batch("PRAGMA foreign_keys=ON;");
            result
        })
    });
    if result.is_ok() && delete_alarm {
        publish_alarm(scope);
    }
    result
}

// An alarm write that fails must reach the caller: setAlarm is a durability
// promise, and a swallowed SQLITE_FULL here resolved the JS promise while
// persisting nothing, then republished the stale committed state.
//
// Returns the committed alarm value when the write committed immediately
// (autocommit): the caller performs the arm-time wake-entry gate before
// acking. `None` means the write is deferred inside an explicit transaction;
// the gate runs at that transaction's commit instead.
pub fn set_alarm(scope: &str, at_ms: i64) -> anyhow::Result<Option<i64>> {
    with_application_storage_write(scope, |c| {
        c.execute(
            "INSERT INTO _cf_ALARM(scope,at_ms,retry,counted_retry,generation) \
             VALUES(?1,?2,0,0,random()) \
             ON CONFLICT(scope) DO UPDATE SET \
               at_ms=excluded.at_ms, retry=0, counted_retry=0, generation=random()",
            rusqlite::params![scope, at_ms],
        )
        .map(|_| ())
        .map_err(Into::into)
    })?;
    Ok(publish_alarm(scope))
}

pub fn get_alarm(scope: &str) -> Option<i64> {
    let alarm = alarm_state(scope);
    let hidden_during_handler = active_alarms(|alarms| {
        alarms
            .borrow()
            .get(scope)
            .is_some_and(|active| active.generation == alarm.map(|(_, generation)| generation))
    });
    if hidden_during_handler {
        None
    } else {
        alarm.map(|(at_ms, _)| at_ms)
    }
}

pub fn delete_alarm(scope: &str) -> anyhow::Result<()> {
    with_application_storage_write(scope, |c| {
        c.execute("DELETE FROM _cf_ALARM WHERE scope=?1", [scope])
            .map(|_| ())
            .map_err(Into::into)
    })?;
    let _ = publish_alarm(scope); // a delete loosens: never gated
    Ok(())
}

/// Drain the alarm moves committed since the last take, in this isolate.
///
/// Called at the end of every cell turn, under the isolate lock like all
/// storage. Only an alarm mutation fills the map, so the ordinary request
/// path pays one empty-map read and no SQLite.
pub fn take_alarm_moves() -> Vec<(String, i64)> {
    alarm_moves(|moves| moves.borrow_mut().drain().collect())
}

/// Publish committed alarm state to the watcher. Returns the committed value
/// (`-1` for none) when publication happened, `None` when the mutation is
/// still inside an explicit transaction (published at its boundary) or the
/// db is gone. The returned value drives the arm-time wake-entry gate.
fn publish_alarm(scope: &str) -> Option<i64> {
    // A storage transaction may call setAlarm()/deleteAlarm() several times
    // before committing or may roll them back. Publish only committed state;
    // the outer transaction boundary reconciles the watcher after either
    // commit or rollback.
    let state = with(scope, |connection| {
        if connection.is_autocommit() {
            Ok(connection
                .query_row(
                    "SELECT at_ms FROM _cf_ALARM WHERE scope=?1",
                    [scope],
                    |row| row.get::<_, i64>(0),
                )
                .ok()
                .unwrap_or(-1))
        } else {
            Err(())
        }
    });
    let at_ms = match state {
        Some(Ok(at_ms)) => at_ms,
        Some(Err(())) => {
            alarm_dirty(|dirty| {
                dirty.borrow_mut().insert(scope.to_string());
            });
            return None;
        }
        None => return None,
    };
    alarm_dirty(|dirty| {
        dirty.borrow_mut().remove(scope);
    });
    alarm_moves(|moves| {
        moves.borrow_mut().insert(scope.to_string(), at_ms);
    });
    Some(at_ms)
}

fn publish_alarm_if_transaction_dirty(scope: &str) -> Option<i64> {
    let dirty = alarm_dirty(|scopes| scopes.borrow().contains(scope));
    if dirty {
        publish_alarm(scope)
    } else {
        None
    }
}

/// Scheduled time and retry count for an alarm due at/before `now_ms`.
pub fn due_alarm_entry(scope: &str, now_ms: i64) -> Option<(i64, i64)> {
    with(scope, |c| {
        c.query_row(
            "SELECT at_ms,retry FROM _cf_ALARM WHERE scope=?1 AND at_ms<=?2",
            rusqlite::params![scope, now_ms],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .ok()
    })
    .flatten()
}

fn alarm_state(scope: &str) -> Option<(i64, i64)> {
    with(scope, |connection| {
        connection
            .query_row(
                "SELECT at_ms,generation FROM _cf_ALARM WHERE scope=?1",
                [scope],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok()
    })?
}

/// The persisted alarm in a restored cell DB, read directly by path — the
/// activation executor seeds the lifecycle Cell's alarm mirror from this at
/// restore time, before the isolate opens the scope's `with` connection. Read
/// only; the connection is dropped before `spawn_cell` opens the same file.
/// Returns (due_wall_ms, generation, retry, counted_retry); `None` if unarmed.
/// A paged restore's file is sparse behind `vfs`; a plain open of it reads
/// holes and silently drops the alarm. That open must also be read-write:
/// faulting hydrates the local file, which a read-only descriptor cannot.
pub fn persisted_alarm(
    db_path: &str,
    scope: &str,
    vfs: Option<&str>,
) -> Option<(i64, i64, u32, u32)> {
    let connection = match vfs {
        Some(vfs) => rusqlite::Connection::open_with_flags_and_vfs(
            db_path,
            rusqlite::OpenFlags::default(),
            vfs,
        )
        .ok()?,
        None => rusqlite::Connection::open_with_flags(
            db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .ok()?,
    };
    connection
        .query_row(
            "SELECT at_ms,generation,retry,counted_retry FROM _cf_ALARM WHERE scope=?1",
            [scope],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get::<_, i64>(2)? as u32,
                    row.get::<_, i64>(3)? as u32,
                ))
            },
        )
        .ok()
}

/// Enter an alarm handler. Until the handler changes the alarm, `getAlarm()`
/// observes null as required by the Durable Object contract.
pub fn begin_alarm_handler(scope: &str, fired_at_ms: i64) {
    let generation = alarm_state(scope).map(|(_, generation)| generation);
    active_alarms(|alarms| {
        alarms.borrow_mut().insert(
            scope.to_string(),
            ActiveAlarm {
                fired_at_ms,
                generation,
            },
        );
    });
}

pub fn active_alarm_scheduled_time(scope: &str) -> Option<i64> {
    active_alarms(|alarms| alarms.borrow().get(scope).map(|active| active.fired_at_ms))
}

/// Finish an alarm handler. A successful handler consumes its original alarm;
/// a failed handler reschedules it. Any alarm explicitly changed by the
/// handler wins over automatic cleanup or retry.
pub fn finish_alarm_handler(scope: &str, succeeded: bool, now_ms: i64) {
    finish_alarm_handler_with_retry_policy(scope, succeeded, now_ms, true);
}

pub fn finish_alarm_handler_with_retry_policy(
    scope: &str,
    succeeded: bool,
    now_ms: i64,
    retry_counts_against_limit: bool,
) {
    let active = active_alarms(|alarms| alarms.borrow_mut().remove(scope));
    let Some(active) = active else {
        return;
    };
    let current_generation = alarm_state(scope).map(|(_, generation)| generation);
    if active.generation != current_generation {
        return;
    }
    if succeeded {
        with(scope, |connection| {
            connection.execute(
                "DELETE FROM _cf_ALARM WHERE scope=?1 AND at_ms=?2",
                rusqlite::params![scope, active.fired_at_ms],
            )
        });
        publish_alarm(scope);
    } else {
        bump_alarm_with_policy(scope, now_ms, retry_counts_against_limit);
    }
}

/// Delete the fired alarm unless the handler re-armed it to the future.
#[cfg(celld_internal_tests)]
pub fn clear_alarm_if_due(scope: &str, fired_at_ms: i64) {
    with(scope, |c| {
        c.execute(
            "DELETE FROM _cf_ALARM WHERE scope=?1 AND at_ms<=?2",
            rusqlite::params![scope, fired_at_ms],
        )
    });
    publish_alarm(scope);
}

pub fn bump_alarm_with_policy(scope: &str, now_ms: i64, counts_against_limit: bool) {
    with(scope, |c| {
        let (retry, counted_retry): (i64, i64) = c
            .query_row(
                "SELECT retry,counted_retry FROM _cf_ALARM WHERE scope=?1",
                [scope],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap_or((0, 0));
        // The schedule is sans-IO (`celld_logic::alarm::alarm_retry`); this fn
        // is its executor, reading the counters and writing the new row.
        match celld_logic::alarm::alarm_retry(now_ms, retry, counted_retry, counts_against_limit) {
            None => {
                let _ = c.execute("DELETE FROM _cf_ALARM WHERE scope=?1", [scope]);
            }
            Some(at_ms) => {
                let _ = c.execute(
                    "UPDATE _cf_ALARM SET at_ms=?2, retry=retry+1, \
                       counted_retry=counted_retry+?3 WHERE scope=?1",
                    rusqlite::params![scope, at_ms, i64::from(counts_against_limit)],
                );
            }
        }
    });
    publish_alarm(scope);
}

#[cfg(celld_internal_tests)]
include!(env!("CELLD_INTERNAL_STORAGE_OBSERVERS"));
