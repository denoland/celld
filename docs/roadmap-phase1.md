# Roadmap Phase 1: Dynamic Node Discovery & Basic Heartbeat

- **Problem:** Static `KNOWN_PEERS` prevents dynamic scaling and fault
  tolerance.
- **Goal:** Nodes dynamically discover each other using S3 as a registry.
- **Tasks:**
  1. **S3 Node Registry Design:**
     - Define an S3 structure (e.g., `s3://YOUR_BUCKET/cluster_state/nodes/`).
     - Each active node writes/updates an object representing itself (e.g.,
       `s3://.../nodes/<node-id>.json`).
     - Content: Node ID, public endpoint address (for proxying), last heartbeat
       timestamp.
  2. **Implement Heartbeat:**
     - Each `roomd` instance periodically updates its object in S3 (e.g., every
       30 seconds) to signify it's alive.
  3. **Implement Registry Reader:**
     - Each `roomd` instance periodically lists objects in the S3 `nodes/`
       prefix.
     - It builds its _dynamic peer list_ based on recently heartbeated nodes
       (e.g., seen within the last 60-90 seconds).
     - Replace the `KNOWN_PEERS` logic with this dynamic list for consistent
       hash proxying.
  4. **Graceful Shutdown:** When a node shuts down cleanly, it should attempt to
     delete its registry object from S3.
- **Abstractions:**
  - Introduce a `ClusterMembership` trait (Rust).
  - Implement `S3ClusterMembership` using the AWS Rust SDK. Methods could
    include `register()`, `heartbeat()`, `getActivePeers()`, `unregister()`.
- **Outcome:** Nodes can start, register themselves in S3, discover other active
  nodes, and proxy requests. Stale/dead nodes eventually disappear from the
  active list.

## TODO

- [x] Add `NodeState` which is often wrapped in Arc and contains
      `ProcessManager` and `PeerManager`. Proxy should have one member, just
      `Arc<NodeState>`. Make `ProcessManager` and `PeerManager` non-cloneable -
      since we lift the `Arc` to `NodeState`.
- [x] Add `src/cluster_membership.rs` which has the trait and S3 implementation.
      Add standalone unit tests for the S3 implementation which spins up a
      standalone ephemeral Minio instance for each test using
      `crate::test_utils::MinioTestServer`. - Verify `register` creates the
      correct S3 object key and content. - Verify `heartbeat` updates the
      timestamp field correctly. - Verify `getActivePeers` correctly lists
      objects, parses JSON, _filters out stale nodes_ based on the heartbeat
      timestamp, and returns the list of active peers (Node ID + Advertise
      Addr). - Verify `unregister` deletes the correct S3 object.

- [ ] Integrate `cluster_membership` into the rest of roomd. See bottom section
      "INTEGRATION" for details.

- [ ] Use Pingora graceful shutdown mechanism to delete the registry object. See
      `src/pingora/docs/user_guide/graceful.md` and
      `docs/user_guide/start_stop.md` and `ShutdownWatch`.
      `ServerApp::process_new` is a hook for new connections and receives a
      `ShutdownWatch`. `BackgroundService::start` also receives a
      `ShutdownWatch`. Add an integration test in tests/test-mesh.rs which sends
      a SIGTERM and checks that the node is immediately removed from MINIO.

- [ ] Add further integration tests in `tests/test-mesh.rs`
  - [ ] `test_single_node_registers`: Start one instance, wait, verify its
        object appears correctly in Minio.
  - [ ] `test_heartbeat_updates_timestamp`: Start one instance, wait longer than
        heartbeat interval, check S3 object timestamp was updated.
  - [ ] `test_two_nodes_discover_each_other`: Start instance A, then instance B.
        Verify B sees A, and A sees B after polling S3.
  - [ ] `test_graceful_shutdown_removes_entry`: Start instance A, send
        SIGTERM/SIGINT, verify its S3 object is deleted.
  - [ ] `test_stale_node_is_ignored`: Start A and B. Kill A abruptly. Wait for
        longer than the heartbeat timeout. Verify B no longer lists A as an
        active peer (tests the filtering logic in `getActivePeers`).

## Notes

**Node ID: UUID vs. Address**

- **Recommendation:** Use a **UUID generated at startup** for each `roomd`
  instance as the unique `Node ID`.
- **Why:**
  - **Uniqueness:** UUIDs guarantee uniqueness across instances, even if
    multiple instances are accidentally started on the same machine or if IP
    addresses change (common in containerized/cloud environments).
  - **Stability:** The UUID remains constant for the _lifetime of that specific
    process instance_.
  - **Decoupling:** It separates the node's _identity_ (UUID) from its _network
    location_ (address:port).
- **How:** Generate a UUID (using a Rust crate like `uuid`) when the `roomd`
  process starts and store it in memory. This UUID will be used when registering
  the node in S3.
- **S3 Object Content:** The JSON object stored in S3 for each node should
  contain _both_ the `node_id` (UUID) and the `advertise_addr` (the network
  address other nodes should use to contact it).
  ```json
  // s3://YOUR_BUCKET/cluster_state/nodes/a1b2c3d4-e5f6-7890-1234-567890abcdef.json
  {
    "node_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
    "advertise_addr": "192.168.1.100:8080", // Or whatever ADVERTISE_ADDR was provided
    "heartbeat_timestamp": "2025-04-29T12:38:00Z" // ISO 8601 format
  }
  ```

**`SELF_ADDR` Ergonomics**

- With the UUID approach, the need for `SELF_ADDR` to _identify_ self goes away.
- When a node fetches the list of peers from S3, it can simply compare the
  `node_id` in each entry against its _own_ generated UUID (which it knows).
- You _will_ still need configuration for:
  - `LISTEN_ADDR` env var: The actual network interface and port the `roomd`
    process binds to locally (e.g., `0.0.0.0:8080` or `127.0.0.1:8080`).
  - `ADVERTISE_ADDR` en vvar: The address that this node tells _other_ nodes to
    use to reach it. This is the address that gets written into the S3 registry
    object. In simple cases (like local testing), it might be the same as
    `LISTEN_ADDR` (but using a specific IP like `127.0.0.1` instead of
    `0.0.0.0`). In container/NAT scenarios, it might be different (e.g., a
    public IP or service DNS name). If `ADVERTISE_ADDR` is not provided, then it
    is assumed to be the same as `LISTEN_ADDR`.

## INTEGRATION

**1. `main.rs` (Server Setup & State)**

- **Add Env Vars (Lazy Statics):**
  - Define `static LISTEN_ADDR: Lazy<String>`: Read `LISTEN_ADDR` env var.
    Default to `0.0.0.0:<port_from_advertise_addr>` if not set or invalid
    format.
  - Define `static ADVERTISE_ADDR: Lazy<String>`: Read `ADVERTISE_ADDR` env var.
    This _must_ be set (panic or return error if missing/invalid, e.g.,
    `1.2.3.4:8080`).
  - **(Remove):** Delete the `static DATA_PORT: Lazy<u16>` definition.
- **Remove Old Env Vars:**
  - Delete all code reading or using the `SELF_ADDR` environment variable. Node
    self-identification will use the generated UUID.
  - Delete all code reading or using the `KNOWN_PEERS` environment variable.
    Peer discovery now comes exclusively from `ClusterMembership` (S3).
- **Modify `NodeState`:**
  - Add field: `pub cluster_membership: Arc<dyn ClusterMembership>`.
  - Keep field: `pub peer_manager: Arc<PeerManager>` (Ensure `PeerManager` is
    `Arc` wrapped for sharing).
- **Update `start_server` / `main` function:**
  - **Node ID:** Generate a unique `node_id` (UUID string). This uniquely
    identifies this running instance.
  - **Get Addresses:** Obtain the values from `*LISTEN_ADDR` and
    `*ADVERTISE_ADDR`.
  - **Instantiate `S3ClusterMembership`:** Update `S3ClusterMembership::new` to
    accept `node_id` and `advertise_addr: &str` (using `&*ADVERTISE_ADDR`). It
    should read S3 env vars internally (see point 3).
  - Wrap `S3ClusterMembership` in an `Arc`, store in `NodeState`.
  - **Register Node:** Call `node_state.cluster_membership.register().await`
    _after_ initialization.
  - **Instantiate `PeerManager`:** Initialize `PeerManager::new` with the
    generated `node_id` (UUID) and the `advertise_addr` string
    (`&*ADVERTISE_ADDR`).
  - **Bind Server:** Modify `proxy_service.add_tcp(&self_addr)` to use the
    address from `*LISTEN_ADDR`.
  - **Add Background Service (Heartbeat & Peer Update):**
    - Create a new Pingora `BackgroundService`.
    - Pass `node_state.cluster_membership.clone()` and
      `node_state.peer_manager.clone()`.
    - Inside its loop (e.g., every 15 seconds):
      - Call `cluster_membership.heartbeat().await`.
      - Call `peers = cluster_membership.get_active_peers().await`.
      - Call `peer_manager.update_peers(peers)`.
  - **Add Graceful Shutdown Hook:**
    - Use `server.graceful_shutdown_hooks.add(...)`.
    - Get `node_state.cluster_membership.clone()`.
    - Inside the hook, call `cluster_membership.unregister().await`.

**2. `peer_manager.rs` (Dynamic Peer Handling)**

- **Remove Env Var Usage:** Delete any code related to `KNOWN_PEERS` or
  `SELF_ADDR`.
- **Modify `PeerManager` struct:**
  - Keep `ring: HashRing<String>` (stores `advertise_addr` strings).
  - Change `peers: Vec<String>` to `peers: Vec<NodeInfo>` (store the full info).
  - Change `self_id: String` to `self_node_id: String` (the UUID).
  - Add `self_advertise_addr: String`.
  - Wrap internal state (`ring`, `peers`) in an `Arc<RwLock<PeerManagerState>>`
    for thread safety.
- **Update `PeerManager::new`:**
  - Change signature to
    `pub fn new(self_node_id: String, self_advertise_addr: String) -> Self`.
  - Initialize the `RwLock` with state containing an empty `peers` list and a
    `ring` with only the `self_advertise_addr`.
- **Add `update_peers(&self, active_peers: Vec<NodeInfo>)` method:**
  - Acquire write lock on internal state.
  - Store `active_peers` list.
  - Create new `HashRing`.
  - Add `self_advertise_addr` to ring.
  - Add each `peer.advertise_addr` from `active_peers` to ring.
  - Replace old ring with new one.
- **Update Methods (`get_owner_peer`, `is_local_owner`, etc.):**
  - Acquire read lock on internal state.
  - Implement logic based on the lock-protected `ring`, `peers`
    (`Vec<NodeInfo>`), `self_node_id`, and `self_advertise_addr`.

**3. `cluster_membership.rs` (Configuration Reading)**

- **Modify `S3ClusterMembership::new`:**
  - Remove S3 config parameters (`endpoint_url`, `region`, etc.).
  - Read S3 config directly from `ROOMD_S3_*` env vars internally (like
    `sqlite_replica.rs`), potentially using `once_cell::sync::Lazy` or direct
    `std::env::var` calls within `new`. Handle missing mandatory vars.
  - Keep parameters: `node_id: String`, `advertise_addr: &str`, optional
    `staleness_threshold`.

**4. `main.rs` (Proxy Logic & Debug Endpoints)**

- **Verify `Proxy::upstream_peer`:** Ensure it uses the lock-protected
  `peer_manager` methods correctly.
- **Update `Proxy::request_filter` (Debug Endpoints):**
  - Modify `/_mesh/peers` to use `peer_manager.get_all_peers()` (returns
    `Vec<NodeInfo>`) and format output.
  - Ensure `/_mesh/owner/` uses the updated `PeerManager` correctly.
