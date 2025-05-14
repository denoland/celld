# Architecture: Deno Cells Runtime

## 1. Introduction

Deno Cells provide a self-hostable, multi-tenant runtime environment for
deploying stateful JavaScript and TypeScript applications. The system is
designed for scalability, resilience, and ease of state management, leveraging
Deno for isolated compute, SQLite for local state, and S3 as the primary cloud
backing service.

**Multi-Tenancy**: The runtime handles multiple tenants, where each tenant is
typically delineated by the `Host` header of incoming HTTP requests. Each tenant
has its own dedicated data directory structure for application code, static
assets, and persisted state.

**Request Handling**:

- Static files (e.g., `index.html`, `client.js`) for a tenant are served
  directly from the tenant's `static/` directory.
- Requests to the path `/cell/{cellId}` are dynamic and map to a specific,
  stateful **Cell Isolate**. Each `{cellId}` within a tenant corresponds to a
  unique Deno isolate.

**Cells**: A "Cell" is a fundamental unit of compute and state. It's a Deno
isolate coupled with a private SQLite database. Cells are designed to be:

- **Stateful**: Each cell manages its own state, persisted via Litestream to S3.
- **Addressable**: Uniquely identified by a tenant and a `cellId`.
- **Isolated**: Runs in a separate Deno subprocess, ensuring resource and
  security boundaries.
- **Durable**: State is replicated to S3, allowing for recovery.
- **Scalable**: The system can scale horizontally by adding more `celld` nodes,
  with cells distributed across these nodes.

## 2. Key Architectural Principles

- **S3 as the Sole Cloud Primitive**: The system is designed with a minimal
  cloud footprint, relying exclusively on an S3-compatible object store for:
  - **Durable State Persistence**: Litestream replicates SQLite databases to S3.
  - **Distributed Locking**: Ensures a cell runs on only one node at a time.
  - **Cluster Service Discovery**: Nodes discover each other by registering
    their presence in S3.
- **One Isolate Per Cell, Always**: Guarantees that for any given tenant and
  `cellId`, only one Deno isolate is active across the entire cluster. This
  simplifies state management and consistency.
- **Consistent Hash Routing**: Cells are distributed among `celld` nodes using a
  consistent hashing algorithm based on the tenant and `cellId`. This ensures
  requests are routed to the correct node hosting the cell.
- **Horizontal Scalability**: New `celld` nodes can be added to the cluster to
  increase capacity. The system automatically discovers new nodes and rebalances
  the distribution of cell ownership (implicitly via consistent hashing).
- **Resilience**: The system is designed to handle node failures. If a node goes
  down, the lock for the cells it hosted will eventually time out, allowing
  another node to take ownership and restore the cell's state from S3.
- **Fast Cold Starts**: Efforts are made to achieve fast cold starts for
  isolates, though specific mechanisms like TCP header peek and Deno subprocess
  reuse (mentioned in the original MVP architecture) are subject to current
  implementation details.

## 3. System Components

The Deno Cells runtime is composed of several key components working in concert:

### 3.1. Proxy Router (Pingora-based)

- **Technology**: Utilizes Pingora (Rust) for robust and high-performance
  HTTP/1.x and WebSocket proxying.
- **Responsibilities**:
  - **Tenant Extraction**: Parses the `Host:` header to determine the tenant
    context.
  - **Static File Serving**: Serves static assets directly from the tenant's
    `$DATA_DIR/<tenant>/static/` directory.
  - **Cell Request Proxying**: Intercepts requests for `/cell/{cellId}`.
    - Determines the owner node for the cell using the `PeerManager`.
    - If the current node is the owner, it forwards the request to the local
      Deno isolate (via a Unix domain socket).
    - If a remote node is the owner, it proxies the request to that node's
      `advertise_addr`.

### 3.2. Cluster Membership & Peer Manager (S3-based)

- **Cluster Membership (`S3ClusterMembership`)**:
  - **Discovery**: Nodes register themselves by writing their `NodeInfo` (node
    ID, `advertise_addr`, heartbeat timestamp) to a designated S3 prefix.
  - **Health Monitoring**: Nodes periodically update their heartbeat timestamp
    in S3. Stale nodes (those that haven't heartbeated recently) are considered
    inactive.
  - The `HeartbeatService` is a background task that manages these S3
    interactions.
- **Peer Manager (`PeerManager`)**:
  - Maintains a list of currently active peers in the cluster by querying the
    `ClusterMembership` service.
  - Uses a consistent hash ring (`HashRing`) populated with the `advertise_addr`
    of active peers.
  - Provides a mechanism to determine the owner node(s) for any given `cellId`
    (combined with tenant).

### 3.3. Process Manager (`ProcessManager`)

- **Isolate Lifecycle Management**: Responsible for spawning, tracking, and
  terminating Deno subprocesses (isolates) for each cell.
- **Ensuring Uniqueness**:
  - Acquires a **distributed lock** via S3 (using `DistributedLock`) before
    spawning an isolate for a specific `tenant/cellId`. The lock name is
    typically `<tenant>/<cellId>`.
  - This ensures that only one `ProcessManager` across the cluster can claim
    ownership and run an isolate for a given cell.
- **Deno Process Spawning**:
  - Constructs and executes the `deno run` command with appropriate permissions
    (e.g., `--allow-net`, `--allow-read`, `--allow-write`, `--allow-env`) and
    environment variables (including `DENO_SERVE_ADDRESS` for the Unix socket
    and `X-Cell-Id`). The Deno processes are meant to not have full access to
    the system, only network and some env vars and files are permitted.
  - Manages the Unix domain socket used for communication between the Pingora
    proxy and the Deno isolate. These sockets are created in a system temporary
    directory.
- **Integration with State Persistence**: Coordinates with `SqliteReplica` to
  ensure database restoration before the Deno process fully starts.

### 3.4. State Persistence (`SqliteReplica` & Litestream)

- **Per-Cell SQLite Database**: Each cell is backed by its own SQLite database
  file (e.g., `<data-dir>/<tenant>/sqlite/<cell-id>.db`).
- **Litestream Integration**:
  - The `SqliteReplica` module manages the Litestream configuration
    (`<cell-id>.yml`) and processes for each cell.
  - **Replication**: Spawns a `litestream replicate` process to continuously
    back up the SQLite WAL (Write-Ahead Log) to S3.
  - **Restoration**: On cold start for a cell (i.e., no local DB exists, but a
    lock is acquired), `SqliteReplica` attempts to restore the database from S3
    using `litestream restore`. This is coordinated with the distributed lock to
    prevent race conditions during restore.
  - If no backup is found in S3, an empty SQLite database is created locally.

### 3.5. Lock Management (`DistributedLock` & `LockGuardTTLUpdater`)

- **Distributed Locks via S3**: Implements a locking mechanism using S3 objects.
  Acquiring a lock involves creating an object in S3 with a specific name (e.g.,
  `<tenant>/<cellId>`) and a TTL.
- **Lock Guard (`LockGuard`)**: An RAII-style guard that represents an acquired
  lock. When the guard is dropped (e.g., the Deno process for the cell
  terminates), the lock is released from S3.
- **Lock Renewal (`LockGuardTTLUpdater`)**: A background service that
  periodically renews the TTL of all active locks held by the current node. This
  prevents locks from expiring while the corresponding cell is still healthy and
  running. The renewal interval is typically `lock_guard_ttl / 3`. A common TTL
  for locks might be 90 seconds, with renewals every 30 seconds (timeout occurs
  if renewal fails for 90 seconds). User defined it as 30s timeout and 30s
  renewal, this part of code is configurable (`config.lock_guard_ttl` in
  main.rs, `DEFAULT_STALENESS_THRESHOLD` in cluster_membership.rs is 90s).

## 4. Cell Lifecycle & Management

### 4.1. Cell Activation (Cold Start)

1. A request arrives at the proxy for `/cell/{cellId}` for a given tenant.
2. The `Proxy Router` consults the `PeerManager` to find the owner node for this
   cell.
3. If the current node is the owner but the isolate is not running: a. The
   `ProcessManager` attempts to acquire a distributed lock for
   `<tenant>/<cellId>` in S3. * If the lock is already held by another node, the
   request might be re-proxied or an error returned (depending on strategy). *
   If the lock is held by the current node (e.g. process creation in progress
   for another request for the same cell), it will wait. b. Once the lock is
   acquired, the `SqliteReplica` component attempts to restore the cell's SQLite
   database from S3. If no backup exists, an empty database is created. c. The
   `ProcessManager` spawns a new Deno subprocess, configuring it to listen on a
   unique Unix domain socket. The Deno runtime executes the tenant's `main.ts`
   script. d. The `DENO_SERVE_ADDRESS` (pointing to the Unix socket) and
   `X-Cell-Id` environment variables are passed to the Deno isolate.
4. The proxy connects to the Deno isolate via the Unix socket and forwards the
   request.

### 4.2. Cell Activity & Connection Management

- The Deno isolate handles HTTP requests and WebSocket connections as defined in
  the tenant's `main.ts`.
- The `ProcessManager` tracks the number of active incoming connections (HTTP
  requests, WebSockets) to each Deno isolate.
- It also checks for active outbound TCP connections made by the Deno isolate.

### 4.3. Cell Deactivation (Idle Timeout)

- A background service, `ProcessReaper`, periodically checks for idle Deno
  isolates.
- **Idleness Definition**: An isolate is considered idle if it has no active
  incoming connections (e.g., open WebSockets or ongoing HTTP requests) AND no
  active outbound TCP connections.
- **Timeout**: If an isolate remains idle for a configured duration (e.g.,
  `DEFAULT_IDLE_TIMEOUT` of 60 seconds), the `ProcessReaper` will:
  1. Initiate shutdown of the Litestream replication for that cell.
  2. Terminate the Deno subprocess.
  3. The `LockGuard` for the cell is dropped, releasing the distributed lock in
     S3.
  4. The temporary Unix domain socket is cleaned up.

## 5. Data Management

### 5.1. Directory Structure (Simplified)

```
<data-dir>/
└── <tenant-hostname>/  # e.g., myapp.localhost
├── static/             # Served at / for the tenant
│   └── index.html
├── src/
│   └── main.ts         # Cell logic for this tenant
├── sqlite/             # Per-cell SQLite DBs and Litestream configs
│   ├── <cell-id>.db
│   └── <cell-id>.yml   # Litestream config for <cell-id>.db
└── prod.env            # Optional environment variables for cells of tenant
```

- **Unix Domain Sockets**: These are created in a system-wide temporary
  directory, not within the `<data-dir>`.

### 5.2. Data Flow & Consistency

- **Writes**: JavaScript code in `main.ts` writes to `cell.db` (an SQLite
  connection). These writes are first committed to the local SQLite file.
- **Replication**: Litestream monitors the SQLite WAL file and replicates
  committed changes to S3.
- **Consistency Aim**: The system aims for strong consistency. Once a write is
  replicated to S3 by Litestream, it is considered durable.
- **Litestream Limitation (Acknowledged)**: There is a brief window between a
  write being committed to the local SQLite database and Litestream successfully
  replicating it to S3. If a `celld` node crashes during this window, those
  specific writes might be lost. This is an accepted limitation for the current
  version, with potential future enhancements to mitigate it (e.g., synchronous
  replication options if Litestream supports them, or application-level
  acknowledgments).

## 6. Networking and Routing

- **Advertise Address (`ADVERTISE_ADDR`)**: The main address and port where the
  Pingora proxy listens for public traffic (e.g., `0.0.0.0:8000`). The address
  that this node advertises to other nodes in the cluster for inter-node
  communication (e.g., proxying cell requests). This must be reachable by other
  `celld` nodes. It's stored in S3 as part of `NodeInfo`.
- **Internal Listen Address (`internal_listen_addr`)**: A separate address and
  port for internal node-specific APIs, such as metrics or health checks (e.g.,
  `127.0.0.1:8001`). Currently used for an `InternalAPI` service providing mesh
  stats.
