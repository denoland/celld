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

- [ ] Use Pingora graceful shutdown mechanism to delete the registry object. See
      `src/pingora/docs/user_guide/graceful.md` and
      `docs/user_guide/start_stop.md` and `ShutdownWatch`.
      `ServerApp::process_new` is a hook for new connections and receives a
      `ShutdownWatch`. `BackgroundService::start` also receives a
      `ShutdownWatch`. Add an integration test in tests/test-mesh.rs which sends
      a SIGTERM and checks that the node is immediately removed from MINIO.

- [ ] Add `LISTEN_ADDR` (replaces `DATA_PORT`) and `ADVERTISE_ADDR` to the
      command line args. These should be used to bind the server and register
      with S3.\
      Throughout the codebase (grep): - Remove `SELF_ADDR` env var from the code
      (see below, UUID replaces it). - Remove `KNOWN_PEERS` (discovered from
      S3/Minio instead).

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
