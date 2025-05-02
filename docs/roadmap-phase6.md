# Roadmap: Phase 6 - Alarms API (V1 - Best Effort)

**Status:** Not Started **Depends On:** Phase 5 (Internal Control Plane) **Leads
To:** Phase 7 (Advanced Demos), Future Streams (Exactly-Once Alarms, Cron)

## Goal

Implement a time-based Alarms API, inspired by Cloudflare Durable Objects,
allowing user code within a room to schedule its `onAlarm` handler to be
executed at a specific future time. This implementation (V1) will provide
**best-effort** dispatch semantics.

## Non-Goals (V1)

- Exactly-once or at-least-once dispatch guarantees (dispatch is fire-and-forget
  after delete).
- Cron-based scheduling (though the central DB could accommodate this later).
- Payloads associated with alarms.
- Complex retry logic for failed dispatches.
- Authentication on internal RPC calls (relies on Phase 5 network separation).

## Architecture: Centralized System Database

- **System Data Storage:** A single SQLite database
  (`data/_system/sqlite/main.db`) storing all shared system state cluster-wide,
  including pending alarms.
  - Table (`globalalarms`):
    `scheduled_time_unix_ms INTEGER NOT NULL, tenant TEXT NOT NULL, room_id TEXT NOT NULL, PRIMARY KEY (scheduled_time_unix_ms, tenant, room_id)`
  - Index:
    `idx_globalalarms_scheduled_time ON globalalarms (scheduled_time_unix_ms)`
  - _Note: This central DB can potentially store other system-level tables in
    the future (e.g., for cron jobs)._
- **Durability:** This central DB (`main.db` for the `_system` tenant) uses its
  own `SqliteReplica` instance for Litestream replication to S3 (e.g.,
  `s3://.../sqlite/_system/main/`).
- **Scheduler Role:** A single node acts as the Alarm Scheduler, determined by
  holding an S3 distributed lock (`roomd_alarm_scheduler_lock`).
- **Scheduler Responsibilities:**
  - Manages the lifecycle of the _local copy_ of `_system/sqlite/main.db`,
    including restore from S3 (using a _separate_ S3 lock:
    `roomd_system_db_lock`) upon acquiring leadership.
  - Runs `litestream replicate` for its local `main.db` under `_system`.
  - Periodically queries its local `main.db` for due alarms (`SELECT` from
    `globalalarms`).
  - Dispatches alarms via internal RPCs over the internal network (Phase 5).
- **Dispatch:** Uses "delete-first" logic. The scheduler deletes the alarm row
  from `globalalarms` _before_ sending the dispatch RPC to the target node.
- **Wake-Up:** Dispatch RPC triggers `ProcessManager` on the target node,
  ensuring the room process is running (handling cold starts and restores)
  before the final trigger.
- **API Calls (`set/delete/getAlarm`):** Implemented as internal RPCs from the
  room's host node to the current scheduler leader node over the internal
  network. These RPCs modify the `globalalarms` table in the leader's local copy
  of `_system/main.db`.
- **Final Trigger (`onAlarm`):** Sent from the target node's Rust host to the
  Deno process via the existing UDS connection.

## Key Tasks

1. **Central DB Setup (`_system/main.db`):**
   - Define constants for lock names (`SCHEDULER_LOCK_NAME`, `SYSTEM_DB_LOCK`).
   - Ensure `SqliteReplica::initialize` can handle the `_system` tenant and
     `main.db` database name correctly, including generating the S3 path
     `sqlite/_system/main/`.
   - Modify `NodeState` initialization (if necessary, likely handled by
     scheduler service) to potentially hold `system_replica: Arc<SqliteReplica>`
     for the scheduler leader.
   - Ensure the `globalalarms` table and index are created (e.g., via
     `PRAGMA user_version` check or similar) when the `_system/main.db` is
     initialized/restored by the scheduler.

2. **Alarm Scheduler Service (`alarm_scheduler_service.rs`):**
   - Create the `BackgroundService` implementation.
   - Implement leader election loop using `SCHEDULER_LOCK_NAME`.
   - **If Leader:**
     - Initialize/obtain `SqliteReplica` for `tenant="_system"`,
       `room_id="main"`.
     - Implement `ensure_restored` logic for the system replica using
       `SYSTEM_DB_LOCK`. Handle errors gracefully.
     - Implement logic to start/manage `litestream replicate` for the system DB.
     - Implement scheduler query loop (`SELECT` from `globalalarms` in local
       `_system/main.db`).
     - Implement dispatch logic:
       - `DELETE` due alarm row from local `_system/main.db` (`rusqlite`).
       - Find target node address using `PeerManager`.
       - Send `POST /_internal/dispatch_alarm` RPC to target node (using
         `reqwest` or similar HTTP client configured for internal network). Log
         success/failure (best-effort).

3. **RPC Implementation (Rust Internal Handlers):**
   - Add handlers to the internal Pingora service
     (`router.rs -> InternalAPI::request_filter` or dedicated functions):
     - `POST /_internal/alarms` (`setAlarm`): Needs access to the _scheduler
       leader_. This implies the request needs to be forwarded from the
       receiving node to the scheduler leader (using `PeerManager` to find
       leader based on `SCHEDULER_LOCK_NAME`). The _leader_ then parses the
       body, gets its local `_system/main.db` path, performs `INSERT OR REPLACE`
       into `globalalarms`.
     - `DELETE /_internal/alarms` (`deleteAlarm`): Forward request to scheduler
       leader. Leader parses body, gets its DB path, performs `DELETE` from
       `globalalarms`.
     - `GET /_internal/alarms` (`getAlarm`): Forward request to scheduler
       leader. Leader parses query params, gets its DB path, performs `SELECT`
       from `globalalarms`.
     - `POST /_internal/dispatch_alarm` (Received by target node): Parses body
       (`tenant`, `room_id`), uses `ProcessManager::get_or_spawn_process`. On
       success, sends `POST /_internal/alarm` via UDS to the Deno process's
       socket. Returns 2xx/5xx.

4. **RPC Client Logic (Rust):**
   - Implement helper functions (callable from Deno `bootstrap.ts` context):
     - Function to find the current scheduler leader address (query
       `SCHEDULER_LOCK_NAME` S3 object for node ID, use `PeerManager` to map
       node ID to address). Add caching.
     - Use an HTTP client to send requests (`/alarms` GET/POST/DELETE) to the
       _leader's_ internal port.

5. **Deno Integration (`bootstrap.ts`):**
   - Add `setAlarm`, `deleteAlarm`, `getAlarm` async functions to the `ctx`
     object provided to user code. These functions call the Rust RPC client
     helpers (Step 4) to contact the scheduler leader.
   - Add `onAlarm?: (ctx: Context) => Promise<void> | void;` to the `Server`
     interface expected from user code.
   - Modify the `Deno.serve` UDS handler to intercept `POST /_internal/alarm`,
     verify it's a valid internal trigger (e.g., check source IP if possible, or
     rely on network isolation), and execute `await userModule.onAlarm?.(ctx);`.

6. **Router Enhancement (`router.rs`):**
   - Modify `Proxy::request_filter` to explicitly reject requests where the
     `Host` header starts with `_system` (e.g., `_system` or
     `_system.anything`). Return a `400 Bad Request` or `404 Not Found`.

7. **Demo & Testing:**
   - Update AI Chat demo (or similar) to use `setAlarm` and implement `onAlarm`.
   - Develop integration tests covering:
     - Setting and receiving an alarm on a single node.
     - Setting an alarm, shutting down the node, starting another, verifying the
       new scheduler picks it up and dispatches it correctly (testing central DB
       restore and scheduler failover).
     - Setting an alarm for a dormant room and verifying it wakes up correctly.
     - Verifying requests with `Host: _system` are rejected by the public proxy.

## Success Criteria

- User code can call `ctx.setAlarm(timestamp)` and have the `onAlarm` handler
  execute approximately at that timestamp.
- User code can call `ctx.deleteAlarm()` to cancel pending alarms.
- User code can call `ctx.getAlarm()` to retrieve the scheduled time.
- Alarms are persisted durably via the central `_system/main.db` replication.
- Scheduler role fails over to another node if the leader fails (using S3 lock).
- The new scheduler correctly restores the central alarm DB state
  (`_system/main.db`) from S3.
- Alarms trigger correctly even if the target room process is not running when
  the alarm becomes due (wake-up works).
- Internal RPCs use the dedicated internal port and are correctly routed to the
  scheduler leader when necessary.
- Public proxy rejects requests targeting the `_system` tenant.
