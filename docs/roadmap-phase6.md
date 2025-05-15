# Roadmap: Phase 6 - Alarms API (V1 - Best Effort)

**Status:** Not Started **Depends On:** Phase 5 (Internal Control Plane) **Leads
To:** Phase 7 (Advanced Demos), Future Streams (Exactly-Once Alarms, Cron)

## Goal

Implement a time-based Alarms API, inspired by Cloudflare Durable Objects,
allowing user code within a cell (`main.ts`) to schedule its `onAlarm` handler
to be executed at a specific future time. This implementation (V1) will provide
**best-effort** dispatch semantics using a centralized system database managed
by a single leader node.

## Non-Goals (V1)

- Exactly-once or at-least-once dispatch guarantees (dispatch is fire-and-forget
  after delete; if dispatch fails or no owner is found, the alarm is lost).
- Cron-based scheduling (though the central DB could accommodate this later).
- Payloads associated with alarms.
- Complex retry logic for failed dispatches.
- Authentication on internal RPC calls (relies on Phase 5 network separation).

## Architecture: Centralized System Database & RPC Forwarding

- **System Tenant and Cell:** Use the tenant name `_system` and cell ID `main`
  for shared system state.
- **System Data Storage:** A single SQLite database
  (`data/_system/sqlite/main.db`) storing all shared system state cluster-wide,
  including pending alarms.
  - **Database Library:** Use synchronous `rusqlite` for direct database
    operations on the leader node. _[Note: Currently assuming these operations
    are fast enough not to require `spawn_blocking`, as the service runs in its
    own background task. Re-evaluate if performance issues arise.]_
  - **Table (`global_alarms`):**
    `scheduled_time_unix_ms INTEGER NOT NULL, tenant TEXT NOT NULL, cell_id TEXT NOT NULL, PRIMARY KEY (scheduled_time_unix_ms, tenant, cell_id)`.
  - **Index:**
    `idx_global_alarms_scheduled_time ON global_alarms (scheduled_time_unix_ms)`.
- **Durability (Litestream):** This central DB (`main.db` for the `_system`
  tenant) uses its _own_ `SqliteReplica` instance for Litestream replication to
  S3.
  - **S3 Backup Path:** Litestream backups for the system database will reside
    at `s3://<your-bucket>/<your-prefix>/sqlite/_system/main/`.
- **Alarm Scheduler Service:**
  - A new background service
    (`alarm_scheduler_service.rs::AlarmSchedulerService`).
  - **Leadership:** Determined by holding S3 lock `"alarm_scheduler_lock"`.
  - **Leader Responsibilities:** Manages its own `SqliteReplica` for `_system`
    tenant / `main` cell_id, performs restore (using S3 lock `"_system:main"`),
    starts replication, queries local DB (`rusqlite`), dispatches alarms via
    HTTP RPC to the appropriate node's internal API.
- **UDS Communication (Two Sockets):**
  - **Primary Socket (`main.sock`):** Rust connects -> Deno listens
    (`Deno.serve`). Used for proxying incoming HTTP/WebSocket requests from the
    main router and sending `POST /_internal/alarm` triggers (HTTP over UDS)
    from Rust to Deno. Path passed via `DENO_SERVE_ADDRESS`.
  - **Control Socket (`control.sock`):** Deno connects -> Rust listens
    (`tokio::net::UnixListener`). Used for Deno sending `set/delete/getAlarm`
    commands back to Rust using **manually constructed HTTP requests over UDS**.
    Path passed via `CELL_CONTROL_SOCKET`.
- **API Call Flow (`set/delete/getAlarm` via Control Socket):**
  - User code calls `ctx.setAlarm(timestamp)`.
  - `bootstrap.ts` connects to the `control.sock` UDS path specified by
    `CELL_CONTROL_SOCKET`.
  - `bootstrap.ts` **manually constructs a raw HTTP request string** (e.g.,
    `POST /_internal/alarms HTTP/1.1\r\nHost: control\r\nContent-Type: application/json\r\nContent-Length: N\r\n\r\n{"scheduled_time_unix_ms": ...}`),
    encodes it to bytes, and writes it to the Control Socket `Deno.UnixConn`.
  - The Rust host process (`process_manager.rs` task associated with the Deno
    process) listens on the Control Socket, accepts the connection, reads bytes
    from the stream, and **parses the incoming raw HTTP request** (e.g., using
    `httparse`).
  - The Rust host task extracts the method, path, headers, and body. It then
    acts as an HTTP client, sending the _actual_ internal HTTP request (e.g.,
    `POST /_internal/alarms` with the parsed body) to the **local internal API
    listener** (`http://localhost:<internal_listen_addr>/_internal/alarms`)
    using `reqwest`.
  - The `InternalAPI` handler performs leader check and potential HTTP
    forwarding to the actual leader node.
  - The leader's `InternalAPI` handler processes the HTTP request (e.g.,
    interacts with the system DB).
  - The `InternalAPI` HTTP response returns to the Rust host process's task.
  - The Rust host task **formats this response as a raw HTTP response string**
    (e.g.,
    `HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: M\r\n\r\n{"status": "success"}`),
    encodes it to bytes, and writes it back over the Control Socket connection.
  - `bootstrap.ts` reads bytes from the Control Socket, **parses the raw HTTP
    response** (status line, headers, body), and resolves the original Promise
    returned by `ctx.setAlarm` based on the parsed response.
- **Alarm Trigger Path (Primary UDS):**
  - Scheduler Leader (`AlarmSchedulerService`) -> finds due alarm, deletes from
    DB.
  - Scheduler Leader -> finds target node for the cell.
  - Scheduler Leader -> `POST /_internal/dispatch_alarm` (HTTP RPC) -> Target
    Node `InternalAPI`.
  - Target Node `InternalAPI` -> `ProcessManager::get_or_spawn_process` (spawns
    if needed).
  - Target Node `InternalAPI` -> Connects to Deno's Primary Socket
    (`main.sock`).
  - Target Node `InternalAPI` -> Sends `POST /_internal/alarm` (HTTP over UDS).
  - Deno `Deno.serve` on Primary Socket -> receives `POST /_internal/alarm` ->
    calls `userModule.onAlarm`.

## Internal HTTP Alarm RPC Request/Response Examples

These requests are handled by the HTTP service `router.rs::InternalAPI`
listening on the internal port. They represent the _actual_ HTTP calls made via
`reqwest`, potentially forwarded between nodes. The transport over the control
socket uses raw HTTP formatting.

1. **Set Alarm:** `POST /_internal/alarms`
   - **Forwarding:** Request must be forwarded to the current scheduler leader
     node.
   - **Request Body (JSON):** Matches
     `struct SetAlarmRequest { tenant: String, cell_id: String, scheduled_time_unix_ms: i64 }`
     ```json
     {
       "tenant": "your-tenant-id",
       "cell_id": "your-cell-id",
       "scheduled_time_unix_ms": 1735689600000
     }
     ```
   - **Leader Action:**
     `INSERT OR REPLACE INTO global_alarms (scheduled_time_unix_ms, tenant, cell_id) VALUES (?, ?, ?)`
     using `rusqlite`.
   - **Response (Success):** `200 OK` with `{"status": "success"}`.

2. **Delete Alarm:** `DELETE /_internal/alarms`
   - **Forwarding:** Request must be forwarded to the current scheduler leader
     node.
   - **Request Body (JSON):** Matches
     `struct DeleteAlarmRequest { tenant: String, cell_id: String }`
     ```json
     {
       "tenant": "your-tenant-id",
       "cell_id": "your-cell-id"
     }
     ```
   - **Leader Action:**
     `DELETE FROM global_alarms WHERE tenant = ? AND cell_id = ?` using
     `rusqlite`.
   - **Response (Success):** `200 OK` with
     `{"status": "success", "deleted_count": 1}` (or 0).

3. **Get Alarm:**
   `GET /_internal/alarms?tenant=your-tenant-id&cell_id=your-cell-id`
   - **Forwarding:** Request must be forwarded to the current scheduler leader
     node.
   - **Leader Action:**
     `SELECT scheduled_time_unix_ms FROM global_alarms WHERE tenant = ? AND cell_id = ?`
     using `rusqlite`.
   - **Response (Success):** `200 OK` with JSON matching
     `struct GetAlarmResponse { tenant: String, cell_id: String, scheduled_time_unix_ms: Option<i64> }`
     ```json
     // Found
     { "tenant": "your-tenant-id", "cell_id": "your-cell-id", "scheduled_time_unix_ms": 1735689600000 }
     // Not Found
     { "tenant": "your-tenant-id", "cell_id": "your-cell-id", "scheduled_time_unix_ms": null }
     ```

4. **Dispatch Alarm:** `POST /_internal/dispatch_alarm`
   - **Source:** Sent _by the scheduler leader_ to the target node hosting the
     cell.
   - **Forwarding:** Sent directly to the target node's internal listener. _Not_
     forwarded further.
   - **Request Body (JSON):** Matches
     `struct DispatchAlarmRequest { tenant: String, cell_id: String }`
     ```json
     {
       "tenant": "your-tenant-id",
       "cell_id": "your-cell-id"
     }
     ```
   - **Target Node Action:** Uses `ProcessManager::get_or_spawn_process` to
     get/spawn the cell, then sends `POST /_internal/alarm` to the Deno process
     via the _primary_ UDS (`main.sock`).
   - **Response (Success):** `200 OK` with `{"status": "success"}`.

5. **Trigger `onAlarm` (Primary UDS):** `POST /_internal/alarm`
   - **Source:** Sent _by the target node's Rust host_ (`InternalAPI` handler
     for `dispatch_alarm`) to the Deno process over the _primary_ Unix Domain
     Socket (`main.sock`).
   - **Forwarding:** N/A (UDS local call).
   - **Request Body:** Empty.
   - **Deno Action:** The `bootstrap.ts` internal `Deno.serve` handler receives
     this, finds the `onAlarm` handler in the user code, and executes
     `await userModule.onAlarm?.(ctx);`.
   - **Response (UDS):** The Deno handler should return a simple `200 OK`
     response over UDS.

## Key Tasks & Checkpoints

_Stub implementations using `unimplemented!()` are acceptable initially._

1. **Define Basic Structures & Service:**
   - [ ] Add `alarm_scheduler_service.rs` file.
   - [ ] Define structs `SetAlarmRequest`, `DeleteAlarmRequest`,
         `GetAlarmResponse`, `DispatchAlarmRequest` (in `router.rs` or a shared
         module).
   - [ ] Define `AlarmSchedulerService` struct in `alarm_scheduler_service.rs`.
         It should hold `Arc<NodeState>` and `check_interval: Duration`.
         Implement `new`.
   - [ ] Modify `main.rs`: Add `mod alarm_scheduler_service;`.
   - [ ] Modify `main.rs::start_server`: Instantiate `AlarmSchedulerService` and
         add it as a background service using `background_service()`. Ensure
         it's only added if S3 config is present (needed for locking).
   - [ ] Implement `impl BackgroundService for AlarmSchedulerService` with a
         basic `start` loop skeleton (using `tokio::select!` on
         `interval_timer.tick()` and `shutdown.changed()`). Add a small
         delay/backoff within the loop if lock acquisition fails repeatedly.
   - **Checkpoint:** `cargo check` passes. Server compiles and runs with the new
     (empty) service.

2. **Verify `SqliteReplica` for System DB:**
   - [ ] Verify `SqliteReplica::initialize` and `write_config` correctly
         generate the local path (`data/_system/sqlite/main.db`) and S3 path
         (`sqlite/_system/main/`) when `tenant` is `_system` and `cell_id` is
         `main`.
   - [ ] Verify/Update `SqliteReplica::ensure_restored` uses the correct S3 lock
         name (`"_system:main"`) for the system database restore coordination.
   - **Checkpoint:** `cargo check` passes. Existing `SqliteReplica` tests should
     ideally still pass.

3. **Implement Scheduler Leader Loop:**
   - [ ] In `AlarmSchedulerService::start`, implement the leader loop within the
         `interval_timer.tick()` block:
     - Get `node_id` from `node_state.peer_manager`.
     - Get `lock_manager` from `node_state.distributed_lock`.
     - Call `lock_manager.try_acquire("alarm_scheduler_lock", node_id, ...)`
     - **If Leader (lock acquired):**
       - [ ] Call `initialize_and_restore_system_db` helper function (implement
             this next).
       - [ ] Call `process_due_alarms` helper function (implement this later).
       - [ ] Ensure lock is released or refreshed appropriately.
     - **If Not Leader (lock error):** Log appropriately and potentially backoff
       before next check.
   - [ ] Implement `AlarmSchedulerService::initialize_and_restore_system_db`:
     - Get or create `SqliteReplica` instance for tenant `_system`, cell_id
       `main`. Store it potentially in `AlarmSchedulerService` or retrieve as
       needed.
     - Call `system_replica.ensure_restored(lock_manager, node_id, ...)` using
       the system DB lock `"_system:main"`. Handle
       `RestoreState::WaitingForLock` / `Failed` gracefully.
     - If restore successful (`Complete`), call
       `system_replica.start_replication().await`.
     - If DB was newly created/restored empty, use
       `rusqlite::Connection::open()` and `conn.execute_batch()` _[Note:
       spawn_blocking recommended here if schema creation could be slow, but
       proceeding without for now]_ to run
       `CREATE TABLE IF NOT EXISTS global_alarms (...)` and
       `CREATE INDEX IF NOT EXISTS idx_global_alarms_scheduled_time ON global_alarms (...)`.
   - **Checkpoint:** `cargo check` passes. Scheduler leader can acquire lock,
     restore system DB, start its replication, and create schema.

4. **Implement Alarm Processing (Leader):**
   - [ ] Implement `AlarmSchedulerService::process_due_alarms`:
     - Get system DB path (`data/_system/sqlite/main.db`).
     - Open `rusqlite::Connection`. _[Note: DB queries here are blocking. Run in
       spawn_blocking if they cause performance issues.]_
     - Perform
       `SELECT tenant, cell_id FROM global_alarms WHERE scheduled_time_unix_ms <= ? ORDER BY scheduled_time_unix_ms LIMIT 100`
       (get current time in ms).
     - For each due alarm (`tenant`, `cell_id`):
       - `DELETE FROM global_alarms WHERE tenant = ? AND cell_id = ?`.
       - Find target node address:
         `node_state.peer_manager.get_cell_owners(tenant, cell_id)`. Pick the
         first owner.
       - **If owner list is empty:** Log a warning (e.g., "No owner found for
         due alarm tenant/cell...") and drop the alarm (best effort V1).
       - **If owner found:**
         - Construct `DispatchAlarmRequest`.
         - Send `POST http://{target_addr}/_internal/dispatch_alarm` using
           `reqwest::Client` with the JSON body.
         - Log success or failure. If dispatch fails (e.g., network error,
           target returns 5xx), log the error and drop the alarm (it was already
           deleted locally - best effort V1).
   - **Checkpoint:** `cargo check` passes. Leader can query and delete alarms,
     and attempt to dispatch them following V1 best-effort semantics.

5. **Implement Internal HTTP API (`router.rs::InternalAPI`):** Ensure the Rust
   code handling the control socket (Task 6) calls these `InternalAPI` HTTP
   handlers via `reqwest` to `localhost:<internal_listen_addr>`.
   - [ ] **Refactor Leader Finding:** Create a helper async function
         `InternalAPI::find_scheduler_leader_address(&self) -> Result<Option<String>, anyhow::Error>`
         that:
     - Gets `lock_manager` and `peer_manager` from `self.node_state`.
     - Calls `lock_manager.get_lock_info("alarm_scheduler_lock")`.
     - If lock exists, finds the peer address using
       `peer_manager.get_all_peer_info()` (or add a dedicated
       `get_peer_address_by_id` method to `PeerManager`). Handle peer lookup
       errors.
     - Returns `Ok(Some(address))` or `Ok(None)` if no leader, or `Err` on
       failure.
   - [ ] **Implement Forwarding:** Modify `InternalAPI::request_filter` (or
         create a dedicated forwarding middleware):
     - For `POST/DELETE/GET /_internal/alarms` paths:
       - Call `find_scheduler_leader_address`. Handle errors (return 5xx).
       - If a leader address is found and it's _not_ the local node's address
         (`peer_manager.get_local_peer()`):
         - Use `reqwest::Client` to forward the _original HTTP request_ (method,
           path, query, headers, body) to
           `http://{leader_address}{original_path_and_query}`.
         - Read the response from the leader (status, headers, body).
         - Write the leader's response back to the original `session`. Use
           `session.write_response_header` and `session.write_response_body`.
         - Return `Ok(true)` to indicate the request is handled.
   - [ ] **Implement Leader Handlers:** Add logic _after_ the forwarding check
         for `/_internal/alarms` paths:
     - `POST /_internal/alarms`: Read body into `SetAlarmRequest`, open
       `_system/sqlite/main.db` with `rusqlite` _[spawn_blocking?]_ , execute
       `INSERT OR REPLACE`. Return `200 OK` or `5xx`.
     - `DELETE /_internal/alarms`: Read body into `DeleteAlarmRequest`, open DB
       _[spawn_blocking?]_ , execute `DELETE`. Return `200 OK` with
       `deleted_count` or `5xx`.
     - `GET /_internal/alarms`: Parse query params `tenant`, `cell_id`. Open DB
       _[spawn_blocking?]_ , execute `SELECT`. Return `200 OK` with
       `GetAlarmResponse` JSON or `5xx`.
   - [ ] **Implement Dispatch Receiver:** Add logic for
         `POST /_internal/dispatch_alarm`:
     - Read body into `DispatchAlarmRequest`.
     - Call
       `node_state.process_manager.get_or_spawn_process(&req.tenant, &req.cell_id, false, node_state.clone())`.
       Handle errors (return 5xx).
     - Get the `socket_path` (primary socket) from the result.
     - **Send UDS Request:** Use `tokio::net::UnixStream::connect(&socket_path)`
       and manually write an HTTP
       `POST /_internal/alarm HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n`
       request to the stream. Read the response (optional, but good practice).
     - Return `200 OK` or `5xx`.
   - **Checkpoint:** `cargo check` passes. Internal API can forward, handle
     leader actions, and trigger UDS calls.

6. **Rust Host UDS Handling (Control Socket - Manual HTTP Parsing):**
   - [ ] **Modify `process_manager.rs::spawn_deno_process`:**
     - Create `main.sock` (primary) and `control.sock` paths.
     - Pass paths via `DENO_SERVE_ADDRESS` and `CELL_CONTROL_SOCKET`.
     - **Bind and Listen:** After spawn, `tokio::net::UnixListener::bind` to
       `control.sock`.
   - [ ] **Modify `ProcessEntry`:** Add a field to hold the
         `tokio::task::JoinHandle` for the control socket listener task (or
         similar mechanism to manage its lifecycle). [Needs refinement on exact
         mechanism, e.g., maybe the `UnixStream` itself if the task owns it
         exclusively?].
   - [ ] **Implement Control Connection Accept & Handling Task:**
     - In `ProcessManager` or similar (perhaps triggered after spawn),
       `accept()` the connection on the `control.sock` listener. This likely
       happens once per Deno process lifetime. Store the accepted
       `tokio::net::UnixStream`.
     - Spawn a dedicated `tokio::task` (the "control task") to handle this
       single `UnixStream` for the lifetime of the Deno process. Store its
       `JoinHandle` in `ProcessEntry`.
     - **HTTP Parsing Loop:** Within the control task:
       - Continuously read data from the control `UnixStream` into a buffer.
       - **Parse Raw HTTP Request:** Use a suitable library (e.g., `httparse`)
         to parse the buffered bytes as an HTTP/1.1 request (method, path,
         headers, body). Handle partial reads and parsing errors gracefully
         (e.g., log error, potentially close connection).
       - **Process Parsed Request:**
         - Based on parsed method/path (`POST /_internal/alarms`,
           `DELETE /_internal/alarms`, `GET /_internal/alarms`):
           - Extract necessary data (body for POST/DELETE, query params derived
             from path for GET). Get `tenant`/`cell_id` associated with the Deno
             process.
           - Construct the corresponding JSON body or URL query string for the
             _actual_ internal HTTP API call.
           - Use `reqwest::Client` to send the request to
             `http://localhost:<internal_listen_addr>/_internal/...`.
           - Await the `reqwest` response (`resp`).
           - **Format Raw HTTP Response:** Manually construct the HTTP response
             string/bytes based on `resp.status()`, relevant `resp.headers()`
             (like `Content-Type`, `Content-Length`), and `resp.bytes().await`.
             (e.g.,
             `HTTP/1.1 {status_code} {reason}\r\nContent-Type:...\r\nContent-Length:...\r\n\r\n{body}`).
           - Write the raw HTTP response bytes back to the control `UnixStream`.
             Handle write errors.
       - Handle UDS read/write errors, HTTP parsing/formatting errors, and
         `reqwest` errors gracefully.
       - Ensure the loop terminates cleanly when the Deno process exits or the
         UDS connection closes.
   - [ ] **Lifecycle Management:** Ensure the control task is aborted/cleaned up
         when the corresponding Deno process is killed or terminates (e.g., via
         the `JoinHandle` stored in `ProcessEntry`).
   - **Checkpoint:** `cargo check` passes. Rust host can listen on control
     socket, parse raw HTTP requests, make internal `reqwest` calls, and write
     raw HTTP responses back. Control task lifecycle is managed.

7. **Deno Integration (`bootstrap.ts` - Manual HTTP over UDS):**
   - [ ] **Establish Control Connection:**
     - Read `CELL_CONTROL_SOCKET` env var.
     - `await Deno.connect(...)` to the control socket path. Store the
       `Deno.UnixConn` (e.g., `controlConn`).
   - [ ] **Implement Manual HTTP-over-UDS Helpers:**
     - Use very simple HTTP/1.0, one request/response per connection.
     - Create helper async function (e.g.,
       `sendHttpRequestOverUds(requestOptions: { method: string, path: string, headers?: Record<string, string>, body?: Uint8Array | string })`):
       - Takes method, path, headers, body.
       - **Construct Raw HTTP Request:** Manually builds the HTTP request string
         (e.g.,
         `POST /_internal/alarms HTTP/1.1\r\nHost: control\r\nContent-Type: application/json\r\nContent-Length: N\r\n\r\n{json_body}`).
         Calculate `Content-Length` correctly. Use `Host: control` or similar
         placeholder.
       - Encodes the string to `Uint8Array`.
       - Writes the bytes to `controlConn` using `controlConn.write()`.
       - **Read Raw HTTP Response:** Reads data iteratively from `controlConn`
         using `controlConn.read()` into a buffer until headers are complete
         (looking for `\r\n\r\n`). Reads the body based on `Content-Length`
         header if present.
       - **Parse Raw HTTP Response:** Manually parses the status line
         (extracting status code), headers (storing relevant ones like
         `Content-Type`), and body from the received bytes. Handle potential
         chunking/buffering and parsing errors.
       - Returns an object representing the parsed response (e.g.,
         `{ status: number, headers: Record<string, string>, body: Uint8Array }`).
     - **`// TODO: Replace manual HTTP with native fetch over UDS when available https://github.com/denoland/deno/issues/8821`**
   - [ ] **Update `ctx` Methods:**
     - Modify `setAlarm(timestamp: number): Promise<void>`:
       - Prepare JSON body `{ scheduled_time_unix_ms: timestamp }`.
       - Call `sendHttpRequestOverUds` with `POST`, path `/_internal/alarms`,
         appropriate headers (`Content-Type: application/json`,
         `Content-Length`), and the JSON body.
       - Await the response. Check parsed status code (e.g., 200). Throw error
         if status indicates failure.
     - Modify `deleteAlarm(): Promise<boolean>`:
       - Prepare JSON body `{}` or perhaps include tenant/cell? [Check internal
         API req].
       - Call `sendHttpRequestOverUds` with `DELETE`, path `/_internal/alarms`,
         headers, and body.
       - Await the response. Check status code. Parse JSON body to get
         `deleted_count` if needed. Return `true` if successful (e.g., status
         200).
     - Modify `getAlarm(): Promise<number | null>`:
       - Call `sendHttpRequestOverUds` with `GET` and path `/_internal/alarms`
         (tenant/cell info needs to be implicitly associated by the Rust host or
         passed in path/query if the API changes). [Clarify how tenant/cell are
         passed for GET via UDS]. For now, assume path is just
         `/_internal/alarms`.
       - Await the response. Check status code. If 200, parse JSON body
         (`GetAlarmResponse`) and return `scheduled_time_unix_ms` (or `null`).
         Return `null` on 404 or other errors.
     - **Add `onAlarm` Handler:** Define
       `onAlarm?(ctx: Context): Promise<void> | void;` in the `Server`
       interface.
     - Ensure `X-Cell-Tenant` and `X-Cell-Id` are available via env vars (set in
       `process_manager.rs::spawn_deno_process`).
   - [ ] **Handle `onAlarm` Trigger (Primary Socket):**
     - The existing `Deno.serve` listening on the primary socket
       (`DENO_SERVE_ADDRESS`) needs a handler for `POST /_internal/alarm`.
     - This handler should find the `onAlarm` function in the loaded user module
       (`userModule.onAlarm`) and execute `await userModule.onAlarm?.(ctx);`.
     - Return a simple `200 OK` HTTP response over the primary UDS.
   - **Checkpoint:** Deno code compiles (`deno check bootstrap.ts`). Rust code
     compiles (`cargo check`). Deno can send/receive manual HTTP
     requests/responses over control socket. Deno `onAlarm` handler is callable
     via primary socket.

8. **Router Enhancement (`router.rs` - `Proxy`):**
   - [ ] Verify `Proxy::request_filter` correctly rejects requests where the
         `Host` header targets the `_system` tenant.
   - [ ] **Add Test Case:** Implement an integration test specifically verifying
         that a request like `GET /some/path HTTP/1.1\r\nHost: _system\r\n...`
         is rejected (e.g., with 400 or 403).
   - **Checkpoint:** `cargo check` passes. Integration test for `_system` host
     rejection passes.

9. **Demo & Testing:**
   - [ ] Update/create demos and integration tests demonstrating alarm
         functionality (set, get, delete, trigger).
   - **Checkpoint:** Integration tests pass.

## Success Criteria

- [ ] User code (`main.ts`) can call `ctx.setAlarm(timestamp)` and have its
      `onAlarm` handler execute approximately at that timestamp.
- [ ] User code can call `ctx.deleteAlarm()` to cancel pending alarms.
- [ ] User code can call `ctx.getAlarm()` to retrieve the scheduled time.
- [ ] Alarms are persisted durably via the central `data/_system/sqlite/main.db`
      replication to S3.
- [ ] Scheduler role (`AlarmSchedulerService` leader) fails over to another node
      if the leader fails (using S3 lock `"alarm_scheduler_lock"`).
- [ ] The new scheduler correctly restores the central alarm DB state
      (`_system/sqlite/main.db`) from S3 using its dedicated lock
      (`"_system:main"`).
- [ ] Alarms trigger correctly even if the target cell process is not running
      when the alarm becomes due (`get_or_spawn_process` handles wake-up).
- [ ] Internal RPCs (`/_internal/alarms`, `/_internal/dispatch_alarm`) use the
      dedicated internal HTTP port and are correctly forwarded to the scheduler
      leader when necessary.
- [ ] Communication for `set/get/deleteAlarm` between Deno and the Rust host
      uses manually parsed/formatted HTTP requests/responses over the control
      UDS.
- [ ] The `onAlarm` trigger uses an HTTP `POST /_internal/alarm` call over the
      primary UDS.
- [ ] Public proxy (`Proxy`) rejects requests targeting the `_system` tenant via
      the Host header.
