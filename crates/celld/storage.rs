// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Cell storage: SQLite backing for the Durable Object storage API.
//!
//! DO's `ctx.storage` is async in JS but synchronous underneath (local SQLite).
//! We expose synchronous Rust ops to V8 and wrap them in `async` in the JS
//! harness — same contract, no thread-hopping. Each cell is its OWN db file
//! (its own replicated, epoch-fenced bucket prefix), so the JS thread holds a
//! `scope -> Connection` map: `open` on activate, `close` on evict. The
//! `scope` column survives from the single-db era and still keys rows, but a
//! db now holds exactly one cell.
use rusqlite::{Connection, OptionalExtension};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
    dbs: RefCell<HashMap<String, Connection>>,
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
    /// Set once a test has given this thread cells of its own. Production
    /// never sets it, because no thread there owns a cell's storage.
    static THREAD_OWNS_CELLS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl Cells {
    /// Make these cells reachable by the storage API for one turn.
    ///
    /// Restores whatever was installed before, so a nested entry — which the
    /// blocking cell run loop still does — leaves the outer turn's cells in
    /// place on the way out.
    pub fn install(&self) -> Installed {
        if THREAD_OWNS_CELLS.get() {
            // A test opened this cell's database on the thread, before the
            // isolate that serves it existed. There the thread's cells are
            // the isolate's, so entering must not shadow them.
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

/// Give this thread cells of its own, as though it were an isolate.
///
/// A test drives the storage API directly and has no isolate to own the
/// state. Production has one and must use it: `cells` asserts rather than
/// falling back to a per-thread map, so a storage call that escapes a turn
/// fails loudly instead of quietly reading state no cell can see.
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
fn dbs<T>(f: impl FnOnce(&RefCell<HashMap<String, Connection>>) -> T) -> T {
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

fn schema(c: &Connection) -> anyhow::Result<()> {
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
            || name
                .get(..12)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("_litestream_")))
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
    finish_open(scope, Connection::open(path)?, sqlite_vec)
}

/// Open `scope` through the fault-injection VFS so tests can fail its writes.
#[cfg(all(test, celld_internal_tests))]
pub fn open_with_fault_vfs_for_test(scope: &str, path: &str) -> anyhow::Result<()> {
    finish_open(scope, crate::fault::open_database(path)?, false)
}

fn finish_open(scope: &str, c: Connection, sqlite_vec: bool) -> anyhow::Result<()> {
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
    close_sync_list_cursors(scope);
    close_sql_cursors(scope);
    close_sql_statement_cache(scope);
    sql_critical_errors(|errors| errors.borrow_mut().remove(scope));
    dbs(|d| d.borrow_mut().insert(scope.to_string(), c));
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
    dbs(|d| d.borrow().get(scope).map(f))
}

fn with_mut<T>(scope: &str, f: impl FnOnce(&mut Connection) -> T) -> Option<T> {
    dbs(|d| d.borrow_mut().get_mut(scope).map(f))
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

fn vref_to_json(v: rusqlite::types::ValueRef) -> serde_json::Value {
    use rusqlite::types::ValueRef;
    match v {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => serde_json::json!(i),
        ValueRef::Real(f) => serde_json::json!(f),
        ValueRef::Text(t) => serde_json::Value::String(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => serde_json::json!({ "__celld_bytes": b }),
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
    connection
        .query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0))
        .unwrap_or(0) as u64
}

/// The cell's committed-write position: SQLite's total completed row changes
/// plus the schema cookie, monotonic for the life of the activation. The
/// output gate samples it around a handler to tell a write from a read; the
/// cookie term makes DDL-only mutations (deleteAll, user DDL) count as
/// writes. Widened only after the gated-frame flush learned to outlive its
/// dispatch — before that, counting DDL turned every lazily-CREATE-ing
/// connect handler into a "writer" whose held frames were silently lost.
/// `None` when the scope has no open connection (a Worker with no Durable
/// Object storage).
pub fn write_position(scope: &str) -> Option<u64> {
    with(scope, |c| total_changes(c) + schema_cookie(scope, c))
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
        value => StoredValue::LegacyJson(vref_to_json(value).to_string()),
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
                    r.push(vref_to_json(row.get_ref(i).map_err(|e| e.to_string())?));
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

unsafe fn sql_cursor_row(statement: *mut rusqlite::ffi::sqlite3_stmt) -> Vec<serde_json::Value> {
    let count = rusqlite::ffi::sqlite3_column_count(statement);
    (0..count)
        .map(
            |index| match rusqlite::ffi::sqlite3_column_type(statement, index) {
                rusqlite::ffi::SQLITE_INTEGER => {
                    serde_json::json!(rusqlite::ffi::sqlite3_column_int64(statement, index))
                }
                rusqlite::ffi::SQLITE_FLOAT => {
                    serde_json::json!(rusqlite::ffi::sqlite3_column_double(statement, index))
                }
                rusqlite::ffi::SQLITE_TEXT => {
                    let length = rusqlite::ffi::sqlite3_column_bytes(statement, index) as usize;
                    let pointer = rusqlite::ffi::sqlite3_column_text(statement, index);
                    let value = if length == 0 {
                        String::new()
                    } else {
                        String::from_utf8_lossy(std::slice::from_raw_parts(pointer, length))
                            .into_owned()
                    };
                    serde_json::Value::String(value)
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
                    serde_json::json!({ "__celld_bytes": bytes })
                }
                _ => serde_json::Value::Null,
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
type SqlCursorStart = (u64, Vec<String>, Option<Vec<serde_json::Value>>, u64, bool);

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
                        if let Err(error) =
                            bind_cursor_value(database, statement, offset as i32 + 1, &value)
                        {
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
                            let row = sql_cursor_row(statement);
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

pub fn sql_cursor_next(cursor_id: u64) -> Result<(Option<Vec<serde_json::Value>>, u64), String> {
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
            rusqlite::ffi::SQLITE_ROW => {
                let row = unsafe { sql_cursor_row(cursor.statement) };
                Ok((Some(row), 0))
            }
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

#[cfg(all(test, celld_internal_tests))]
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

#[cfg(all(test, celld_internal_tests))]
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

/// Arm or clear the thread-local write fault of the test VFS. Only databases
/// opened by `open_with_fault_vfs_for_test` are affected.
#[cfg(all(test, celld_internal_tests))]
pub fn set_write_fault_for_test(enabled: bool) {
    crate::fault::set_write_fault(enabled);
}

/// Shrink the page cache so a large write spills to disk mid-statement, the
/// same trick Workerd's SQLITE_IOERR test uses.
#[cfg(all(test, celld_internal_tests))]
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
#[cfg(all(test, celld_internal_tests))]
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

/// Register `cells_nomem_for_test()`, a scalar that reports allocation
/// failure through SQLite's own OOM path (`sqlite3_result_error_nomem`), so a
/// statement fails with SQLITE_NOMEM exactly as a real allocator failure
/// would.
#[cfg(all(test, celld_internal_tests))]
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

#[cfg(all(test, celld_internal_tests))]
pub fn sql_limit_for_test(scope: &str, category: i32) -> anyhow::Result<i32> {
    with(scope, |connection| {
        // SAFETY: a negative new value queries the live connection's current
        // limit without changing it.
        Ok(unsafe { rusqlite::ffi::sqlite3_limit(connection.handle(), category, -1) })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

#[cfg(all(test, celld_internal_tests))]
pub fn sql_statement_cache_stats(scope: &str) -> (usize, usize, usize) {
    sql_statement_caches(|caches| {
        caches.borrow().get(scope).map_or((0, 0, 0), |cache| {
            (
                cache.entries.len(),
                cache.total_size,
                cache
                    .entries
                    .values()
                    .filter(|entry| !entry.statement.is_null())
                    .count(),
            )
        })
    })
}

// Test-only single-key helpers; the runtime goes through the batched forms.
#[cfg(test)]
pub fn get(scope: &str, key: &str) -> Option<String> {
    with(scope, |c| {
        c.query_row(
            "SELECT v FROM _cf_KV WHERE scope=?1 AND k=?2",
            [scope, key],
            |r| r.get::<_, String>(0),
        )
        .ok()
    })?
}

pub fn get_stored(scope: &str, key: &str) -> anyhow::Result<Option<StoredValue>> {
    with(scope, |c| {
        c.query_row(
            "SELECT v FROM _cf_KV WHERE scope=?1 AND k=?2",
            [scope, key],
            |row| Ok(stored_value(row.get_ref(0)?)),
        )
        .optional()
        .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Fetch multiple keys in storage order, omitting absent keys. Duplicate input
/// keys are coalesced, matching Durable Object storage's map-like result.
#[cfg(all(test, celld_internal_tests))]
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
        let mut statement = c.prepare("SELECT v FROM _cf_KV WHERE scope=?1 AND k=?2")?;
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

#[cfg(test)]
pub fn put(scope: &str, key: &str, val: &str) {
    with(scope, |c| {
        c.execute(
            "INSERT INTO _cf_KV(scope,k,v) VALUES(?1,?2,?3) \
         ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
            [scope, key, val],
        )
    });
}

pub fn put_serialized(scope: &str, key: &str, value: &[u8]) -> anyhow::Result<()> {
    with(scope, |c| {
        c.execute(
            "INSERT INTO _cf_KV(scope,k,v) VALUES(?1,?2,?3) \
             ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
            rusqlite::params![scope, key, value],
        )
        .map(|_| ())
        .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Atomically apply a multi-key put.
#[cfg(all(test, celld_internal_tests))]
pub fn put_many(scope: &str, entries: &[(String, String)]) -> anyhow::Result<()> {
    if entries.is_empty() {
        return with(scope, |_| ()).ok_or_else(|| anyhow::anyhow!("no db for {scope}"));
    }
    with_mut(scope, |c| {
        without_sql_authorizer_mut(c, |c| {
            if !c.is_autocommit() {
                return with_batch_savepoint(c, |c| {
                    let mut statement = c.prepare(
                        "INSERT INTO _cf_KV(scope,k,v) VALUES(?1,?2,?3) \
                     ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
                    )?;
                    for (key, value) in entries {
                        statement.execute(rusqlite::params![scope, key, value])?;
                    }
                    Ok(())
                });
            }
            let transaction =
                c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            {
                let mut statement = transaction.prepare(
                    "INSERT INTO _cf_KV(scope,k,v) VALUES(?1,?2,?3) \
                 ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
                )?;
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
    with_mut(scope, |c| {
        without_sql_authorizer_mut(c, |c| {
            let write = |c: &Connection| -> anyhow::Result<()> {
                let mut statement = c.prepare(
                    "INSERT INTO _cf_KV(scope,k,v) VALUES(?1,?2,?3) \
                 ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
                )?;
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
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

pub fn delete(scope: &str, key: &str) -> anyhow::Result<bool> {
    with(scope, |c| {
        c.execute("DELETE FROM _cf_KV WHERE scope=?1 AND k=?2", [scope, key])
            .map(|n| n > 0)
            .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// Atomically delete multiple keys and return the number that existed.
pub fn delete_many(scope: &str, keys: &[String]) -> anyhow::Result<usize> {
    if keys.is_empty() {
        return with(scope, |_| 0).ok_or_else(|| anyhow::anyhow!("no db for {scope}"));
    }
    with_mut(scope, |c| {
        without_sql_authorizer_mut(c, |c| {
            if !c.is_autocommit() {
                return with_batch_savepoint(c, |c| {
                    let mut statement = c.prepare("DELETE FROM _cf_KV WHERE scope=?1 AND k=?2")?;
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
                let mut statement =
                    transaction.prepare("DELETE FROM _cf_KV WHERE scope=?1 AND k=?2")?;
                for key in keys {
                    deleted += statement.execute(rusqlite::params![scope, key])?;
                }
            }
            transaction.commit()?;
            Ok(deleted)
        })
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))
}

/// List a half-open key range in bytewise SQLite text order.
#[cfg(all(test, celld_internal_tests))]
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
#[cfg(all(test, celld_internal_tests))]
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

fn sqlite_operation_failure(
    scope: &str,
    database: *mut rusqlite::ffi::sqlite3,
    started_in_transaction: bool,
    result_code: i32,
    operation: &str,
) -> anyhow::Error {
    let error = sqlite_failure(database, operation);
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
    error
}

unsafe fn bind_cursor_value(
    database: *mut rusqlite::ffi::sqlite3,
    statement: *mut rusqlite::ffi::sqlite3_stmt,
    index: i32,
    value: &rusqlite::types::Value,
) -> anyhow::Result<()> {
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
        Err(sqlite_failure(database, "bind sync KV cursor"))
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
                if let Err(error) = bind_cursor_value(database, statement, offset as i32 + 1, value)
                {
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
#[cfg(all(test, celld_internal_tests))]
pub struct KvTransaction<'connection> {
    scope: String,
    transaction: rusqlite::Transaction<'connection>,
}

#[cfg(all(test, celld_internal_tests))]
impl KvTransaction<'_> {
    pub fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        match self.transaction.query_row(
            "SELECT v FROM _cf_KV WHERE scope=?1 AND k=?2",
            rusqlite::params![self.scope, key],
            |row| row.get(0),
        ) {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn get_many(&self, keys: &[String]) -> anyhow::Result<Vec<(String, String)>> {
        let mut keys = keys.to_vec();
        keys.sort();
        keys.dedup();
        let mut statement = self
            .transaction
            .prepare("SELECT v FROM _cf_KV WHERE scope=?1 AND k=?2")?;
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
        self.transaction.execute(
            "INSERT INTO _cf_KV(scope,k,v) VALUES(?1,?2,?3) \
             ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
            rusqlite::params![self.scope, key, value],
        )?;
        Ok(())
    }

    pub fn put_many(&self, entries: &[(String, String)]) -> anyhow::Result<()> {
        let mut statement = self.transaction.prepare(
            "INSERT INTO _cf_KV(scope,k,v) VALUES(?1,?2,?3) \
             ON CONFLICT(scope,k) DO UPDATE SET v=excluded.v",
        )?;
        for (key, value) in entries {
            statement.execute(rusqlite::params![self.scope, key, value])?;
        }
        Ok(())
    }

    pub fn delete_many(&self, keys: &[String]) -> anyhow::Result<usize> {
        let mut statement = self
            .transaction
            .prepare("DELETE FROM _cf_KV WHERE scope=?1 AND k=?2")?;
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
#[cfg(all(test, celld_internal_tests))]
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
    let result = with(scope, |c| -> anyhow::Result<()> {
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
                    if table
                        .get(..12)
                        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("_litestream_"))
                    {
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
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")));
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
    with(scope, |c| {
        c.execute(
            "INSERT INTO _cf_ALARM(scope,at_ms,retry,counted_retry,generation) \
             VALUES(?1,?2,0,0,random()) \
             ON CONFLICT(scope) DO UPDATE SET \
               at_ms=excluded.at_ms, retry=0, counted_retry=0, generation=random()",
            rusqlite::params![scope, at_ms],
        )
        .map(|_| ())
        .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))?;
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
    with(scope, |c| {
        c.execute("DELETE FROM _cf_ALARM WHERE scope=?1", [scope])
            .map(|_| ())
            .map_err(Into::into)
    })
    .unwrap_or_else(|| Err(anyhow::anyhow!("no db for {scope}")))?;
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

/// Retry count of `scope`'s alarm if it is due at/before `now_ms` (each cell
/// thread self-polls its own db between requests — no central scheduler).
#[cfg(all(test, celld_internal_tests))]
pub fn due_alarm(scope: &str, now_ms: i64) -> Option<i64> {
    due_alarm_entry(scope, now_ms).map(|(_, retry)| retry)
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
pub fn persisted_alarm(db_path: &str, scope: &str) -> Option<(i64, i64, u32, u32)> {
    let connection =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .ok()?;
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
#[cfg(all(test, celld_internal_tests))]
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

/// Externally injected storage tests.
///
/// These cases drive this module's own functions, so unlike the Web-platform
/// suites they say nothing about the shell around them.
#[cfg(all(test, celld_internal_tests))]
mod conformance_actor_cache_tests {
    include!(env!("CELLD_CONFORMANCE_ACTOR_CACHE_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_actor_sqlite_tests {
    include!(env!("CELLD_CONFORMANCE_ACTOR_SQLITE_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_sqlite_kv_tests {
    include!(env!("CELLD_CONFORMANCE_SQLITE_KV_TESTS"));
}

#[cfg(test)]
mod internal_table_names {
    use super::*;

    /// denoland/celld#122: celld's tables were `kv`, `alarms` and
    /// `cell_metadata`, which an application can collide with and which a
    /// library that keeps only `_cf_*` will drop. Opening an existing cell
    /// must carry it to the Cloudflare names without losing a row.
    #[test]
    fn legacy_tables_are_renamed_in_place() {
        let file = std::env::temp_dir().join(format!("celld-rename-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&file);
        let connection = Connection::open(&file).expect("open");

        // The pre-2026-08-06 schema, with a row worth keeping.
        connection
            .execute_batch(
                "CREATE TABLE kv (scope TEXT, k TEXT, v TEXT, PRIMARY KEY(scope,k));
                 CREATE TABLE alarms (scope TEXT PRIMARY KEY, at_ms INTEGER);
                 CREATE TABLE cell_metadata (scope TEXT PRIMARY KEY, actor_name TEXT);
                 INSERT INTO kv VALUES('s','k','v');",
            )
            .expect("legacy schema");

        schema(&connection).expect("migrate");

        let names: Vec<String> = {
            let mut statement = connection
                .prepare(
                    "SELECT name FROM sqlite_schema WHERE type='table' \
                     AND name NOT LIKE 'sqlite_%' ORDER BY name",
                )
                .expect("prepare");
            let rows = statement
                .query_map([], |row| row.get::<_, String>(0))
                .expect("query");
            rows.collect::<rusqlite::Result<Vec<_>>>().expect("collect")
        };
        assert_eq!(names, ["_cf_ALARM", "_cf_KV", "_cf_METADATA"]);

        let kept: String = connection
            .query_row("SELECT v FROM _cf_KV WHERE k='k'", [], |row| row.get(0))
            .expect("the row survives the rename");
        assert_eq!(kept, "v");

        // Idempotent: opening an already-migrated cell is a no-op.
        schema(&connection).expect("second open");
        let kept_again: String = connection
            .query_row("SELECT v FROM _cf_KV WHERE k='k'", [], |row| row.get(0))
            .expect("still there");
        assert_eq!(kept_again, "v");
        let _ = std::fs::remove_file(&file);
    }
}
