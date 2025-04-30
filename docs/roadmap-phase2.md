# Phase 2 Implementation Checklist: S3 Locking & Robust Litestream Recovery

Work through this file, updating it as you go along. Make sure `cargo check`
runs often.

**Part 1: Implement Distributed Locking (`src/distributed_lock.rs`)**

- **[x] 1.1. Create File & Module:**
  - Create `src/distributed_lock.rs`.
  - Add `pub mod distributed_lock;` to `src/main.rs`.

- **[x] 1.2. Define Core Types:**
  - In `distributed_lock.rs`, define
    `pub struct LockInfo { pub node_id: String, #[serde(with = "chrono::serde::ts_seconds")] pub timestamp: DateTime<Utc>, pub ttl_secs: u64 }`
    (derive `Serialize`, `Deserialize`, `Debug`, `Clone`).
  - Define
    `pub enum LockAcquireError { LockHeld(Option<LockInfo>), S3Error(String), SerializationError(String), BadLockData(String), Other(String) }`
    (derive `Debug`, `thiserror::Error`).
  - Define
    `pub struct LockHandle { pub lock_key: String, node_id: String, #[cfg(test)] etag: Option<String> }`
    (derive `Debug`). We store the `node_id` used to acquire it for potential
    conditional release. ETag primarily for testing conditional operations if
    needed.

- **[x] 1.3. Define `DistributedLock` Trait:**
  - In `distributed_lock.rs`, define the trait:
    ```rust
    use async_trait::async_trait;
    use std::time::Duration;
    // ... other imports

    #[async_trait]
    pub trait DistributedLock: Send + Sync {
        async fn try_acquire(&self, lock_name: &str, node_id: &str, ttl: Duration) -> Result<LockHandle, LockAcquireError>;
        async fn release(&self, handle: LockHandle) -> Result<(), anyhow::Error>;
        // Optional: async fn renew(...)
    }
    ```

- **[x] 1.4. Implement `S3DistributedLock` Struct:**
  - In `distributed_lock.rs`, define
    `pub struct S3DistributedLock { s3_client: aws_sdk_s3::Client, bucket: String, prefix: String }`.
  - Implement `impl S3DistributedLock`.

- **[x] 1.5. Implement `S3DistributedLock::new`:**
  - Define
    `pub fn new(s3_client: aws_sdk_s3::Client, bucket: String, mut prefix: String) -> Self`.
  - Ensure `prefix` ends with `/`. Use a default like
    `"cluster_state/locks/restore/"` if needed.
  - Store `s3_client`, `bucket`, `prefix`.

- **[x] 1.6. Implement `fn get_lock_key` Helper:**
  - Define `fn get_lock_key(&self, lock_name: &str) -> String`.
  - **Opinion:** Use SHA256 hashing for the `lock_name` part to handle arbitrary
    `tenant`/`room_id` characters safely. E.g.,
    `use sha2::{Digest, Sha256}; let hash = Sha256::digest(lock_name.as_bytes()); format!("{}{:x}.lock", self.prefix, hash)`
  - Return the full S3 key path.

- **[x] 1.7. Implement `DistributedLock::try_acquire` for `S3DistributedLock`:**
  - Call `self.get_lock_key(lock_name)`.
  - Create
    `LockInfo { node_id: node_id.to_string(), timestamp: Utc::now(), ttl_secs: ttl.as_secs() }`.
  - Serialize `LockInfo` to JSON bytes using `serde_json::to_vec`. Handle
    serialization error -> `LockAcquireError::SerializationError`.
  - Call
    `self.s3_client.put_object().bucket(...).key(...).body(...).if_none_match("*").send().await`.
    (`if_none_match = "*"` ensures atomic creation).
  - Handle `Result`:
    - `Ok(output)`: Lock acquired. Return
      `Ok(LockHandle { lock_key, node_id: node_id.to_string(), #[cfg(test)] etag: output.e_tag })`.
    - `Err(sdk_err)`: Check if it's a conditional check failure (e.g.,
      `sdk_err.code() == Some("PreconditionFailed")` or similar - **verify exact
      error code/type** with AWS SDK docs/tests).
      - If conditional fail:
        - _(Implement Expired Lock Check - Recommended)_:
          - Call
            `self.s3_client.get_object().bucket(...).key(...).send().await`.
          - Handle `NoSuchKey` error -> Maybe retry `put_object` once? Or return
            error.
          - Handle other Get errors -> `LockAcquireError::S3Error`.
          - If Get succeeds: Read body, deserialize `LockInfo`. Handle
            deserialize error -> `LockAcquireError::BadLockData`.
          - Check if
            `lock_info.timestamp + Duration::from_secs(lock_info.ttl_secs) < Utc::now()`.
          - If expired: Log it. Call
            `self.s3_client.delete_object()...send().await`. Handle delete
            errors. Retry the original `put_object` _once_. Return its result.
          - If _not_ expired: Return
            `Err(LockAcquireError::LockHeld(Some(lock_info)))`.
        - _(Simpler Version - No Expiry Check):_ Return
          `Err(LockAcquireError::LockHeld(None))`.
      - If other S3 error: Convert to string and return
        `Err(LockAcquireError::S3Error(e.to_string()))`.

- **[x] 1.8. Implement `DistributedLock::release` for `S3DistributedLock`:**
  - Call
    `self.s3_client.delete_object().bucket(&self.bucket).key(&handle.lock_key).send().await`.
  - _(Optional Safety Check):_ Could add `expected_owner: handle.node_id` to the
    delete request if S3 supports conditional delete based on object
    metadata/tags matching the owner node ID. For simplicity, a direct delete is
    acceptable initially.
  - Handle S3 errors -> `anyhow::Error`. Return `Ok(())` on success.

- **[x] 1.9 Add S3 Lock Unit/Integration Tests:**
  - In `src/distributed_lock.rs` -> `mod tests`.
  - Use `MinioTestServer`.
  - `test_acquire_release`: Acquire lock, verify S3 object exists, release,
    verify object deleted.
  - `test_acquire_fails_if_held`: Node A acquires lock. Node B attempts acquire
    -> should fail with `LockHeld`. Node A releases. Node B attempts acquire ->
    should succeed.
  - `test_acquire_succeeds_if_expired`: _(If expiry check implemented)_ Node A
    acquires lock with short TTL. Wait > TTL. Node B attempts acquire -> should
    succeed (after potentially deleting stale lock).

**Part 2: Integrate Locking and State Machine**

- **[x] 2.1. Add Lock Manager to `NodeState`:**
  - In `src/main.rs`, add
    `pub distributed_lock: Option<Arc<dyn DistributedLock>>` field to
    `NodeState`.
  - In `start_server`:
    - If `config.has_s3_config()`:
      - Get/create the S3 client (`aws_sdk_s3::Client`).
      - Create `lock_prefix` string (e.g.,
        `config.s3_prefix.clone().unwrap_or_default() + "locks/restore/"`).
      - Instantiate
        `let lock_manager = Arc::new(S3DistributedLock::new(s3_client, config.s3_bucket.clone().unwrap(), lock_prefix));`.
      - Store `Some(lock_manager)` in `NodeState`.
    - Else: store `None`.

- **[x] 2.2. Define `RestoreState` Enum:**
  - In `src/process_manager.rs` (or `src/lib.rs`), define
    `#[derive(Debug, Clone, PartialEq)] pub enum RestoreState { Idle, AcquiringLock, WaitingForLock, Restoring, Complete(bool), Failed(String) }`.

- **[x] 2.3. Add State to `ProcessEntry`:**
  - In `src/process_manager.rs`, modify `ProcessEntry`: add
    `pub restore_state: tokio::sync::Mutex<RestoreState>` (use
    `tokio::sync::Mutex` as state updates will happen across `.await` points).
    Initialize to `RestoreState::Idle`.
  - Modify `processes` field in `ProcessManager` to
    `Mutex<HashMap<String, Arc<ProcessEntry>>>`. This allows holding a lock on a
    _single_ entry across awaits without blocking the whole map.

- **[x] 2.4. Refactor Restore Logic in `SqliteReplica`:**
  - Modify `SqliteReplica::initialize`: Remove the internal call to
    `restore_if_needed`. It should just return `Ok(Some(Self))` or `Ok(None)`.
  - Rename the existing `restore_if_needed` to
    `async fn run_restore(&self) -> Result<bool>`. Keep its internal logic
    (check exists, write config, run `litestream restore`, handle no backup,
    return bool indicating if data was actually restored).
  - Add new method
    `async fn ensure_restored(&self, lock_manager: Arc<dyn DistributedLock>, self_node_id: &str, lock_ttl: Duration) -> Result<RestoreState, anyhow::Error>`:
    - Implement logic as described in step 5 (Pre-computation) of the thought
      process above, using `try_acquire`, RAII or explicit `release`, and
      `run_restore`.
    - Return `Ok(RestoreState::...)` on logical completion (lock held, restore
      complete, new DB) or `Err(...)` only on _unexpected_ errors (like failure
      to contact S3 for the lock).

- **[x] 2.5. Integrate State Machine into `ProcessManager`:**
  - Modify `get_or_spawn_process`:
    - It now needs access to `NodeState` (pass `node_state: Arc<NodeState>` as
      arg).
    - Change how `ProcessEntry` is handled:
      - Lock the main `processes` map briefly to check for existing entry or
        insert a _new_, partially initialized `Arc<ProcessEntry>` (with state
        `Idle`).
      - Unlock the main map. Get a mutable lock specifically on the
        `Arc<Mutex<ProcessEntry>>` for the target room.
    - **Before** `spawn_deno_process`:
      - If `entry.replica.is_some()` and
        `node_state.distributed_lock.is_some()`:
        - Get lock manager and node ID from `node_state`.
        - Lock the entry's `restore_state`, set to `AcquiringLock`. Unlock.
        - Call
          `let restore_result = entry.replica.as_ref().unwrap().ensure_restored(...).await;`.
        - Lock the entry's `restore_state` again.
        - `match restore_result`:
          - `Ok(state @ RestoreState::Complete(_))`: Update state, log success,
            proceed to spawn Deno.
          - `Ok(state @ RestoreState::WaitingForLock)`: Update state, log info,
            **return
            `Err(ProxyError::InternalError(anyhow!("Database restore lock held by another node")))`**.
          - `Ok(state @ RestoreState::Failed(_))`: Update state, log error,
            **return
            `Err(ProxyError::InternalError(anyhow!("Database restore failed: {:?}", state)))`**.
          - `Err(e)` (unexpected lock error): Update state to `Failed`, log
            error, **return
            `Err(ProxyError::InternalError(anyhow!("Error during restore locking: {}", e)))`**.
      - Else (no S3 / no replica):
        - Lock entry state, call `create_empty_database` if needed, set state to
          `Complete(false)`. Unlock. Proceed to spawn.
  - Ensure appropriate logging is added for each state transition.

## 3. Implement Durability Test (tests/test-mesh.rs)

- [x] Add #[tokio::test] async fn test_restore_coordination().
- [x] Use TestEnv to start Node A (Port X).
- [x] Send request to Node A (Room R) to create data (basic-db is good).
- [x] Wait: Add a significant sleep (e.g., 5-10 seconds) to allow Litestream to
      replicate to S3.
- [x] Stop Node A: test_env.kill_roomd_instance(0, Signal::SIGTERM).
- [x] Spawn Node B (Port Y).
- [x] Spawn Node C (Port Z) immediately.
- [x] Wait for B and C to be ready (wait_for_server_ready).
- [x] Send request for Room R to Node B. Check response (e.g., counter = 2).
- [x] Send request for Room R to Node C. Check response (e.g., counter = 2 or 3
      depending on timing).
- [ ] Verify Logs: Add helper to capture/check logs (or manually inspect during
      development). Confirm one node logged "Acquiring Lock" -> "Restoring" ->
      "Complete", and the other logged "Acquiring Lock" -> "WaitingForLock".

## 4. Clean up refactor MinioTestServer port allocation

We use MinioTestServer extensively. It allows minio to spin up an ephemeral
docker instance for each test. Currently each usage needs to have a unique port
hard coded into the tests to avoid conflicting with other instances. We can
avoid this by allowing docker to assign a port instead. Okay, here is a plan of
action to modify the `MinioTestServer` in `test_utils.rs` to use dynamically
assigned ports by Docker:

4.1 **Modify the `start` function signature:** * Remove the `port: u16` argument
from the `MinioTestServer::start` function signature. It will look like
`pub fn start() -> Self {`.

4.2 **Modify the `docker run` command in `start`:** * **Remove explicit port
mapping:** Delete the arguments `"-p", format!("{}:9000", port).as_str(),`. *
**Add publish-all flag:** Insert the `"-P"` flag into the `docker run`
arguments. This tells Docker to map container port 9000 to a _random_ available
port on the host.

4.3 **Retrieve the dynamically assigned port:** * After the `docker run` command
successfully spawns and waits (after the `assert!(status.success()...)` line),
you need to get the port Docker assigned. * Execute a `docker port` command:

````rust
 let port_output = Command::new("docker") .args(["port", &docker_name,
"9000"]) .output() .expect("Failed to get port from docker");
assert!(port_output.status.success(), "docker port command failed");

        let port_string = String::from_utf8_lossy(&port_output.stdout);
        // The output is typically in the format "0.0.0.0:xxxxx" or "[::]:xxxxx"
        let port: u16 = port_string
            .split(':')
            .last()
            .expect("Unexpected docker port output format")
            .trim()
            .parse()
            .expect("Failed to parse port number");
        ```
    * Make sure to handle potential errors during command execution and parsing.

4.4. **Update `MinioTestServer` struct initialization:** * When creating the
`MinioTestServer` instance at the end of the `start` function, use the `port`
variable obtained in the previous step:
`rust
         MinioTestServer {
             access_key_id: access_key.to_string(),
             secret_access_key: secret_key.to_string(),
             docker_name,
             port, // Use the dynamically retrieved port here
             endpoint: format!("http://localhost:{}", port), // And here
         }`

4.5. **Verify `create_bucket` and `has_files_for_room`:** * These functions use
`self.port`. Since `self.port` is now correctly set with the dynamic port in the
`start` function, they _should_ work without changes. However, double-check the
construction of the `MC_HOST_minio` environment variable string to ensure it
uses the correct dynamic port.

4.6. **Update Test Calls:** * Go through your test suite and remove the port
argument from all calls to `MinioTestServer::start(...)`.
````
