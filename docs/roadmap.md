# roomd · Roadmap

_Last updated: 2025-04-25_

## What is this?

roomd gives you a new kind of building block:

🧱 **Many tiny, durable, real-time workers**—each with its own code, its own
SQLite state, and sub‑100ms cold start.

You don’t need Kubernetes, Postgres, or global state to build collaborative
software.

You spin up a container, mount a directory, and get:

- Durable objects that speak WebSocket and HTTP
- Per-room state with S3-backed persistence
- Built-in Deno isolation per tenant or project
- Lightweight mesh networking
- Familiar APIs inspired by PartyKit and Durable Objects

## Why this matters?

roomd aims to be a simple, resilient "substrate" for building stateful,
distributed applications, particularly envisioning a future of collaborative AI
agents. It provides durable "rooms" that manage persistent state (using SQLite

- Litestream to S3) and handles complexities like node discovery, replication,
  and failover automatically. This drastically lowers the barrier for
  developers, allowing them to focus on application logic and rapidly deploy
  sophisticated, reliable systems without getting bogged down in complex
  infrastructure plumbing.

## Roadmap

**Goal:** MVP demonstrating a multi-node `roomd` cluster where nodes can
join/leave, rooms remain available after node failures, and data is persisted
via Litestream/S3.

**Core Principle:** Leverage S3 as the "source of truth" for cluster membership
and potentially for lightweight locking/coordination, minimizing direct
peer-to-peer dependencies beyond proxying.

### Phase 1: Dynamic Node Discovery & Basic Heartbeat

DONE

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

### Phase 2: S3-based Locking & Robust Litestream Recovery

DONE

- **Problem:** `litestream restore` might be slow and needs coordination,
  especially during startup or node takeover.
- **Goal:** Ensure only one node actively tries to restore/manage a specific
  database replica at a time, and make the restore process more stateful.
- **Tasks:**
  1. **S3 Lock Design for Restore:**
     - Use S3 objects as crude mutexes. Define a location (e.g.,
       `s3://YOUR_BUCKET/cluster_state/locks/restore/<db_name>.lock`).
     - To acquire a lock: Attempt to create the lock object using a conditional
       write (e.g., `PutObject` with `If-None-Match: *` header, or
       check-then-put, though less atomic). The object could contain the lock
       holder's node ID and a timestamp.
     - To release: Delete the object. Implement lock leases (TTL) based on the
       timestamp in the object, requiring periodic renewal by the holder.
  2. **Integrate Locking with Restore:**
     - Before a node initiates `litestream restore` for a database it believes
       it should host, it must acquire the corresponding S3 lock.
     - If the lock cannot be acquired, the node waits or assumes another node is
       handling it.
  3. **Improve Restore State Machine:**
     - Refactor the simple `async fn` restore call.
     - Introduce internal states: `Idle`, `AcquiringLock`,
       `Restoring(progress?)`, `RestoreComplete`, `RestoreFailed`,
       `WaitingForLock`.
     - Provide better logging and potentially internal metrics for restore
       duration/status. This helps diagnose the "slow restore" concern.
  4. **Durability Test 1 (Basic Restore):**
     - _When:_ Implement _after_ completing Phase 2.
     - _Scenario:_ Start Node A -> Create Room X -> Write Data -> Stop Node A ->
       Start Node B -> Verify Node B acquires lock, restores Room X from S3, and
       serves the data.
- **Abstractions:**
  - Consider a `DistributedLock` trait (Rust).
  - Implement `S3DistributedLock`. Methods:
    `try_acquire(lock_name, node_id, ttl)`, `release(lock_name, node_id)`.
- **Outcome:** Litestream restores are coordinated via S3 locks, preventing
  conflicts. The restore process is more observable and robust.

### Phase 3: Room Resilience & Takeover

MOSTLY DONE, test_concurrent_takeover_locking and test_proxy_forwarding_retry
added but not working. test_node_failure_takeover does work as well as existing
tests. Will move on to Phase 4.

- **Problem:** If a node hosting a room fails, the room becomes inaccessible.
- **Goal:** Another node automatically takes over responsibility for the room,
  ensuring availability.
- **Tasks:**
  1. **Define Room "Ownership":** Consistent hashing determines the _preferred_
     owner(s) for a room based on the _current_ active node list (from Phase 1).
     Let's say it defines an ordered list of candidate nodes.
  2. **Failure Detection:** Nodes monitor the S3 registry (Phase 1). If the
     primary node for a room stops heartbeating, other candidate nodes notice.
  3. **Takeover Logic:**
     - The next highest-priority _active_ candidate node (from the consistent
       hash list) for the failed room attempts to take over.
     - **Crucial Step:** It attempts to acquire the S3 `restore lock` (from
       Phase 2) for that room's database.
     - If the lock is acquired successfully:
       - It performs the `litestream restore` (which is now robust).
       - Once restored, it starts serving requests for that room.
       - (Optional but recommended): It might update a simple status object in
         S3 indicating it's the new active host (e.g.,
         `s3://.../rooms/<room_id>/status.json` with
         `{"active_node": "node-id"}`). This helps other nodes quickly know who
         _currently_ serves the room without just relying on the hash + liveness
         check.
  4. **Proxying Update:** When proxying, if the primary target node is marked as
     inactive (via S3 registry), try the next node in the consistent hash list.
     If that node has successfully taken over (check optional status object or
     assume it will if it's live and next-in-line), route the request there.
  5. **Durability Test 2 (Node Failure):**
     - _When:_ Implement _after_ completing Phase 3.
     - _Scenario:_ Start Nodes A, B, C -> Create Room X (hashes primarily to A)
       -> Write Data -> Kill Node A abruptly (don't let it unregister cleanly)
       -> Wait for heartbeats to time out -> Send request for Room X -> Verify
       Node B or C (next in line) acquires lock, restores, and serves the data.
- **Outcome:** Rooms remain available even if the primary node fails. The system
  demonstrates basic self-healing.

### Phase 4: Demo & Refinement

- **Goal:** Package the MVP features into a compelling demonstration.
- **Tasks:**
  1. **Multi-Node Demo Script:**
     - Script to easily launch 2-3 `roomd` nodes locally (e.g., using Docker
       Compose or simple shell scripts).
     - A simple client application/script that connects to _any_ node:
       - Creates a few rooms (which will be distributed by the hash).
       - Writes/reads data to demonstrate basic function.
       - _Crucially:_ Includes steps to kill one node and show that requests for
         its rooms are automatically routed to and served by a backup node after
         a short delay (discovery timeout + restore time).
       - (Bonus): Show a new node starting, joining the cluster (visible via
         logs/querying state?), and taking load for new rooms.
  2. **Configuration:** Ensure S3 bucket, region, heartbeat intervals, timeouts,
     etc., are easily configurable (e.g., via environment variables or a config
     file).
  3. **Logging/Observability:** Enhance logging to make the dynamic discovery,
     node failures, lock acquisition, and room takeovers clearly visible.
  4. **README:** Update documentation explaining the architecture (S3 usage),
     setup, configuration, and how to run the demo.
- **Outcome:** A clear demonstration of the system's dynamic nature and
  resilience, proving the core value proposition of the "substrate".

**Addressing Specific Points:**

- **Durability Tests:** Integrated into Phases 2 and 3, starting simple and
  progressing to failure scenarios.
- **S3 Locking:** Addressed in Phase 2 (restore coordination) and implicitly in
  Phase 3 (takeover coordination). Structure uses specific S3 object paths and
  relies on atomic operations (`If-None-Match: *`) or careful check-then-set
  logic with leases.
- **Abstractions (Rust):** Suggested `ClusterMembership` and `DistributedLock`
  traits with S3 implementations. Keep these focused; don't over-abstract
  initially.
- **Room Resilience:** Core part of Phase 3, using S3 registry for liveness and
  S3 locks for takeover coordination.
- **Leader Election Libraries (Rust):** S3 locking _is_ acting as a simple,
  distributed mutex or per-room leader election mechanism here. Dedicated
  libraries (raft, etcd-client) add significant complexity or external
  dependencies (etcd, Consul). Sticking to S3 aligns with your goal of
  minimizing dependencies and complexity _for now_. S3 conditional puts/gets are
  your primary tool.
- **Multi-Node Demo:** Outlined in Phase 4. Focus on showing dynamic join/leave
  and fault tolerance.
- **Litestream Restore Slowness:** The improved state management (Phase 2) will
  help _manage_ it, but won't inherently speed it up. If it's _critically_ slow
  for large DBs, that's a deeper issue maybe for Stream 2 (e.g., investigating
  Litestream performance, incremental restores if possible, or alternative
  replication strategies). The MVP focuses on making the _process_ reliable
  first.
- **Migrations/Alarms API:** Acknowledge these are important but defer them
  post-MVP (Stream 2?). Alarms likely _will_ benefit from a more robust leader
  election (perhaps still S3-based, electing a single "alarm scheduler" node) or
  could potentially be implemented via durable functions/workflows triggered by
  S3 events if your cloud provider supports that, but let's get the core stable
  first.
