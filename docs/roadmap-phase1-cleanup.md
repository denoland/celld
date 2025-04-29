### Pre-Phase 2 Refactoring Checklist (Revised)

**1. Centralize Configuration:**

- **Goal:** Deduplicate environment variable reading (especially for S3),
  improve clarity of dependencies, and enable stricter startup validation.
- **Actions:**
  - [ ] **Define `Config` Struct:** Create a `Config` struct (in a new
    `src/config.rs`). Include fields for:
    - `listen_addr: String`
    - `advertise_addr: String`
    - `data_dir: PathBuf`
    - `heartbeat_interval: Duration`
    - `s3_endpoint: Option<String>`
    - `s3_bucket: Option<String>`
    - `s3_region: Option<String>`
    - `s3_prefix: Option<String>`
    - `s3_access_key_id: Option<String>`
    - `s3_secret_access_key: Option<String>`
  - [ ] **Parse Config at Startup:** In `main.rs` -> `main()`, parse all
        relevant environment variables _once_ into an instance of the `Config`
        struct. Handle defaults appropriately within the parsing logic. Exit
        with error if mandatory variables (like `ADVERTISE_ADDR`) are missing.
  - [ ] **Remove `Lazy` Static Config:** Delete the `Lazy` static definitions
        for `LISTEN_ADDR`, `ADVERTISE_ADDR`, `DATA_DIR`, and
        `ROOMD_HEARTBEAT_INTERVAL` from `main.rs`.
  - [ ] **Remove Duplicate S3 Env Reading:**
    - Delete the `S3ClusterMembership::from_env` factory method in
      `cluster_membership.rs`.
    - Modify `S3ClusterMembership::new` to take the necessary S3 config values
      as direct arguments (e.g., `endpoint: Option<&str>`, `bucket: &str`,
      `region: &str`, `prefix: &str`, `key_id: &str`, `secret_key: &str`, etc.).
    - Delete the `get_s3_cfg_for_tenant` function and `Lazy` statics for S3
      config in `sqlite_replica.rs`.
    - Modify `SqliteReplica::new` to take an `Option<Config>` struct (defined
      in `config.rs`) as a parameter.
  - [ ] **Pass Config Down:** In `main.rs` -> `start_server`, take the `Config`
        struct as an argument. Pass relevant config values or an `Arc<Config>`
        down to components that need them (e.g., `HeartbeatService`,
        `ProcessManager`, `PeerManager`, `S3ClusterMembership::new`,
        `SqliteReplica::new`).
  - [ ] **Implement Strict S3 Startup:** Modify `main.rs` -> `start_server`:
    - Check if the necessary S3 fields _are present_ in the `Config` struct.
    - If S3 fields **are present**:
      - Attempt to instantiate `S3ClusterMembership` using `::new()` with the
        config values.
      - Attempt the initial `membership.register().await` call.
      - If _either_ instantiation or registration fails, treat this as a **fatal
        error**: log the error and `std::process::exit(1)`. Do _not_ fall back
        to standalone mode.
    - If S3 fields **are absent**: Proceed in standalone mode
      (`cluster_membership` field in `NodeState` remains `None`). Log this
      clearly (`info!`).

**2. Refactor `ProcessManager::get_or_spawn_process`:**

- **Goal:** Reduce complexity and isolate concerns before adding the restore
  state machine.
- **File:** `process_manager.rs`
- **Actions:**
  - [ ] **Extract Spawning Logic:** Move the Deno process
        `std::process::Command` setup (arguments, env vars) into a separate
        private helper function (e.g.,
        `fn spawn_deno_process(...) -> Result<ChildOnParentExit>`).
  - [ ] **Isolate Replica Interaction:** Modify `SqliteReplica::new` to
        potentially perform the `restore_if_needed` check internally during its
        creation _if_ S3 config is provided. Alternatively, add a dedicated
        method like
        `SqliteReplica::initialize(s3_config: Option<S3Config>).await -> Result<Option<Self>>`
        that handles config writing, restore attempt, and _then_ returns the
        `SqliteReplica` instance. Then, call this from `get_or_spawn_process`
        _before_ spawning Deno. Modify `ProcessEntry` to store
        `Option<SqliteReplica>`. The goal is to contain the restore logic
        associated with the replica itself. (This prepares for adding locking
        within that initialization logic).
