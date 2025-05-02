# Roadmap: Phase 6 - Alarms API (V1 - Best Effort)

**Status:** Not Started
**Depends On:** Phase 5 (Internal Control Plane)
**Leads To:** Phase 7 (Advanced Demos), Future Streams (Exactly-Once Alarms, Cron)

## Goal

Implement a time-based Alarms API, inspired by Cloudflare Durable Objects,
allowing user code within a room to schedule its `onAlarm` handler to be
executed at a specific future time. This implementation (V1) will provide
**best-effort** dispatch semantics.

## Non-Goals (V1)

- Exactly-once or at-least-once dispatch guarantees (dispatch is fire-and-forget
  after delete).
- Cron-based scheduling.
- Payloads associated with alarms.
- Complex retry logic for failed dispatches.
- Authentication on internal RPC calls (relies on Phase 5 network separation).

## Architecture: Centralized "System Alarm Room"

- **Alarm Storage:** A single SQLite database (`data/_system/sqlite/_alarms.db`)
  storing all pending alarms cluster-wide.
  - Table:
    `global_alarms (scheduled_time_unix_ms INTEGER NOT NULL, tenant TEXT NOT NULL, room_id TEXT NOT NULL, PRIMARY KEY (scheduled_time_unix_ms, tenant, room_id))`
  - Index:
    `idx_global_alarms_scheduled_time ON global_alarms (scheduled_time_unix_ms)`
- **Durability:** This central DB (`_alarms.db`) uses its own `SqliteReplica`
  instance for Litestream replication to S3 (e.g.,
  `s3://.../sqlite/_system/_alarms/`).
- **Scheduler Role:** A single node acts as the Alarm Scheduler, determined by
  holding an S3 distributed lock (`_roomd_alarm_scheduler_lock`).
- **Scheduler Responsibilities:**
  - Manages the lifecycle of the _local copy_ of `_system/_alarms.db`, including
    restore from S3 (using a _separate_ S3 lock: `_roomd_system_alarms_db_lock`)
    upon acquiring leadership.
  - Runs `litestream replicate` for its local `_alarms.db`.
  - Periodically queries its local `_alarms.db` for due alarms.
  - Dispatches alarms via internal RPCs over the internal network (Phase 5).
- **Dispatch:** Uses "delete-first" logic. The scheduler deletes the alarm row
  _before_ sending the dispatch RPC to the target node.
- **Wake-Up:** Dispatch RPC triggers `ProcessManager` on the target node,
  ensuring the room process is running (handling cold starts and restores)
  before the final trigger.
- **API Calls (`set/delete/getAlarm`):** Implemented as internal RPCs from the
  room's host node to the current scheduler leader node over the internal
  network.
- **Final Trigger (`onAlarm`):** Sent from the target node's Rust host to the
  Deno process via the existing UDS connection.

## Key Tasks

1. **Central DB Setup:**
   - Define constants for lock names (`SCHEDULER_LOCK_NAME`,
     `SYSTEM_ALARMS_DB_LOCK`).
   - Ensure `SqliteReplica::initialize` can handle the `_system/_alarms` path.
   - Modify `NodeState` to hold `system_alarm_replica: Arc<SqliteReplica>`
     (initialized at startup).
   - Ensure the `global_alarms` table and index are created when the DB is
     initialized.

2. **Alarm Scheduler Service (`alarm_scheduler_service.rs`):**
   - Create the `BackgroundService` implementation.
   - Implement leader election loop using `SCHEDULER_LOCK_NAME`.
   - **If Leader:**
     - Implement `ensure_restored` logic for the `system_alarm_replica` using
       `SYSTEM_ALARMS_DB_LOCK`. Handle errors gracefully.
     - Implement logic to start/manage `litestream replicate` for the system DB.
     - Implement scheduler query loop (`SELECT` from local `_alarms.db`).
     - Implement dispatch logic:
       - `DELETE` due alarm row from local DB (`rusqlite`).
       - Find target node address using `PeerManager`.
       - Send `POST /_internal/dispatch_alarm` RPC to target node (using
         `reqwest` or similar HTTP client configured for internal network). Log
         success/failure (best-effort).

3. **RPC Implementation (Rust Handlers):**
   - Add handlers to the internal Pingora service (from Phase 5):
     - `POST /_internal/alarms` (`setAlarm`): Parses body, gets local
       `_alarms.db` path, performs `INSERT OR REPLACE`. Needs access to
       `NodeState` (for DB path/replica info).
     - `DELETE /_internal/alarms` (`deleteAlarm`): Parses body, gets DB path,
       performs `DELETE`.
     - `GET /_internal/alarms` (`getAlarm`): Parses query params, gets DB path,
       performs `SELECT`.
     - `POST /_internal/dispatch_alarm`: Parses body (`tenant`, `room_id`), uses
       `ProcessManager::get_or_spawn_process`. On success, sends
       `POST /_internal/alarm` via UDS to the Deno process's socket. Returns
       2xx/5xx.

4. **RPC Client Logic (Rust):**
   - Implement helper functions (callable from `bootstrap.ts` context) that:
     - Find the current scheduler leader address (query `SCHEDULER_LOCK_NAME` S3
       object, use `PeerManager` to map node ID). Add caching if needed later.
     - Use an HTTP client to send requests to the leader's internal port for
       `/set_alarm`, `/delete_alarm`, `/get_alarm`.

5. **Deno Integration (`bootstrap.ts`):**
   - Add `setAlarm`, `deleteAlarm`, `getAlarm` async functions to the `ctx`
     object provided to user code. These functions call the Rust RPC client
     helpers (Step 4).
   - Add `onAlarm?: (ctx: Context) => Promise<void> | void;` to the `Server`
     interface.
   - Modify the `Deno.serve` UDS handler to intercept `POST /_internal/alarm`,
     verify it's a valid internal trigger, and execute
     `await userModule.onAlarm?.(ctx);`.

6. **Demo & Testing:**
   - Update AI Chat demo (or similar) to use `setAlarm` and implement `onAlarm`.
   - Develop integration tests covering:
     - Setting and receiving an alarm on a single node.
     - Setting an alarm, shutting down the node, starting another, verifying the
       new scheduler picks it up and dispatches it correctly (testing central DB
       restore and scheduler failover).
     - Setting an alarm for a dormant room and verifying it wakes up correctly.

## Success Criteria

- User code can call `ctx.setAlarm(timestamp)` and have the `onAlarm` handler
  execute approximately at that timestamp.
- User code can call `ctx.deleteAlarm()` to cancel pending alarms.
- User code can call `ctx.getAlarm()` to retrieve the scheduled time.
- Alarms are persisted durably via the central `_system/_alarms.db` replication.
- Scheduler role fails over to another node if the leader fails (using S3 lock).
- The new scheduler correctly restores the central alarm DB state from S3.
- Alarms trigger correctly even if the target room process is not running when
  the alarm becomes due (wake-up works).
- Internal RPCs use the dedicated internal port.
