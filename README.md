# Deno Cells

<img src="docs/cells.svg" alt="Deno Cells Logo" width="200">

## Simple, Stateful, Scalable Compute Units

**Cells** is a self-hosted server for building stateful, distributed
applications. It provides a model where each uniquely identified entity (a
"cell") runs as a single JavaScript isolate with its own private, synchronous
SQLite database. The system guarantees that for any given cell ID, there is
exactly one active instance running across the entire cluster and handles
routing requests to it. S3 (or an S3-compatible service) is its sole external
dependency, used for durable storage of these SQLite databases (via Litestream
replication) and for cluster coordination.

This project is currently **experimental** and under active development.
Breaking changes are expected.

**Available at: `ghcr.io/denoland/cells`**

## Key Highlights

- Self-Hosted: Run Cells on your own infrastructure using Docker. You own your
  data and the operational environment.
- Scalable: Multiple Cells server instances can be linked via S3 to form a
  robust cluster. This allows you to scale your application capacity
  horizontally as your needs grow, with new nodes automatically joining and
  sharing the workload.
- Simple: Each "cell" (identified by a unique ID) has its own isolate and a
  private SQLite database. The system ensures only one active instance of a cell
  per ID across the entire cluster.
- Uses S3 for durable storage of SQLite databases and for essential cluster
  coordination tasks like service discovery and locking, keeping external
  dependencies minimal and robust.
- Automatic HTTP/WebSocket Routing: Intelligently routes incoming HTTP and
  WebSocket requests to the correct cell instance, activating it if necessary.
- Built-in SQLite per Cell: Each cell comes with its own private SQLite
  database, offering fast, synchronous, and convenient persistence for its
  application state, durably backed up to S3.
- Modern JavaScript Runtime: Leverages the Deno runtime for secure and efficient
  JavaScript/TypeScript execution.
- Durable Execution with Workflows: The `jsr:@ry/cells` SDK includes a powerful
  workflow API, enabling you to define long-running, fault-tolerant processes
  that can survive restarts and continue execution, ideal for complex,
  multi-step operations. (Learn more:
  [jsr.io/@ry/cells](https://jsr.io/@ry/cells))

## Getting Started

The best way to understand how to operate Cells is to use its built-in help
command.

First, ensure you have Docker installed. Then, pull the latest image:

```bash
docker pull ghcr.io/denoland/cells:latest
```

To see the available commands, configuration options, and a detailed operational
overview:

```bash
docker run --rm ghcr.io/denoland/cells --help
```

This command is your primary reference for running and managing Cells instances.

## Cell API & Example

Within your JavaScript/TypeScript code, you interact with the Cells system using
the `@ry/cells` module (soon it will graduate to `@deno/cells`).

**Example: Incrementing Counter**

This simple cell maintains a counter in its SQLite database and increments it on
each HTTP request.

```typescript
import { cell } from "jsr:@ry/cells";

// Initialize the database schema if it doesn't exist
cell.db.exec(`CREATE TABLE IF NOT EXISTS c (id TEXT PRIMARY KEY, v INTEGER)`);
// Insert the counter row if it doesn't exist
cell.db.exec(`INSERT OR IGNORE INTO c (id, v) VALUES ('hits', 0)`);

cell.request((req) => {
  // Increment the counter
  cell.db.prepare(`UPDATE c SET v = v + 1 WHERE id = 'hits'`).run();
  // Read the new value
  const result = cell.db.prepare(`SELECT v FROM c WHERE id = 'hits'`).get();
  // Respond with the count and the cell's ID
  return new Response(`${result.v} (cell ID: ${cell.id})\n`);
});
```

This `main.ts` would be the entry point for your cell's logic. You can then run
this like so:

```bash
docker run -p 8000:8000 -v $PWD:/app ghcr.io/denoland/cells ./main.ts
```

## Core Concepts

### Cells (Actor-like Instances)

A "cell" is an instance of your application logic associated with a unique ID
(e.g., `chat1`, `user123`, `agent-xyz`). It consists of:

- A Deno JavaScript/TypeScript isolate executing your code.
- A private SQLite database, providing durable, synchronous storage for that
  specific cell, with its state replicated to S3.
- User-provided code (like the example above) that defines its behavior for
  handling HTTP requests, WebSocket connections, messages, and workflows.

Cells are addressable via their ID. The system ensures that only one instance of
a cell for a particular ID is active at any given time across the entire
cluster. If a cell is idle (no active WebSocket or outbound TCP connections, and
no running workflows), it will automatically terminate to conserve resources.

### Clustering and S3

Cells achieves scalability and resilience using an S3 bucket (or S3-compatible
service) as its sole external dependency. When you configure multiple Cells
server nodes to point to the same S3 bucket and region:

- They automatically discover each other and form a robust cluster using
  consistent hashing to distribute cells.
- Individual cells (the actor instances with their JS isolate and SQLite DB) are
  distributed across these server nodes.
- The SQLite database for each cell is durably persisted to and replicated from
  S3 using Litestream.
- The system handles routing requests to the server node currently hosting the
  target cell, activating it if necessary.

This architecture simplifies deployment and scaling, allowing you to focus on
your applications.

## Routing

Cells routes requests based on the tenant host and cell ID:

- `http://<tenant_host>/cell/<cell_id>` -> Activates the specified Cell and
  routes the HTTP request to it.
- `ws://<tenant_host>/cell/<cell_id>` -> Activates the specified Cell and
  establishes a WebSocket connection.
- `http://<tenant_host>/<path>` -> Serves static files from the tenant's
  `static/` directory.

**Example:** `http://myapp.localhost:3000/cell/chat1`. `myapp.localhost` is the
tenant domain, while `chat1` is the Cell ID.

## Data Layout

Cells expects a specific directory structure for each tenant (typically derived
from the hostname):

```
<data-dir>/
└── myapp.localhost/       # Tenant directory (e.g., based on hostname)
    ├── static/            # Static files served at the root of the tenant domain
    │   └── index.html
    │   └── client.js
    │   └── ...
    ├── src/
    │   └── main.ts        # Entrypoint for the Cell's JavaScript/TypeScript logic
    └── sqlite/            # SQLite databases, one per cell ID (managed by Cells, backed by S3)
        └── <cell_id_A>.db
        └── <cell_id_B>.db
        └── ...
```

In single-tenant mode, `[SRC_FILE]` and `[STATIC_DIR]` arguments to the `celld`
command define these for a default tenant.

## How It Works

1. **Request Arrival**: An HTTP or WebSocket request arrives at any server node
   in the Cells cluster. The request typically includes information that
   identifies a target tenant and cell (e.g., in the hostname and path).
2. **Cell Activation & Routing**: The receiving node determines which node in
   the cluster is (or should be) hosting the target cell using consistent
   hashing.
   - If the cell is already active on a node, the request is proxied to that
     node.
   - If the cell is not active, one node is elected to activate it. This
     involves:
     - Fetching the cell's code for the tenant (e.g., `src/main.ts`) if not
       already cached.
     - Restoring its SQLite database from S3 (via Litestream).
     - Starting a new Deno isolate with the code and database.
3. **Code Execution**: The Deno isolate for the cell processes the request using
   the handlers defined in your code (e.g., `cell.request()`, `cell.connect()`,
   or workflow steps). It can read from and write to its private SQLite database
   synchronously.
4. **Persistence**: Changes to the SQLite database are written locally and then
   asynchronously replicated to S3 by Litestream, ensuring durability. Workflow
   state is also persisted, allowing for durable execution.
5. **Response**: The cell sends a response back to the client (for direct
   requests) or continues its workflow.

## Use Cases

- **Stateful Services:** Ideal for applications requiring durable storage per
  entity (e.g., user accounts, game sessions, collaborative documents, shopping
  carts). Each entity can be a cell.
- **Real-time Applications:** Leverage WebSockets for interactive experiences
  like chat rooms, live dashboards, and multiplayer games.
- **AI Agents:** Each AI agent can be modeled as a cell, maintaining its own
  state (memory, conversation history, goals) in its private SQLite database and
  executing its logic within its dedicated Deno isolate. The workflow API can
  manage long-running agent tasks.
- **Durable Workflows & Long-Running Processes:** Implement complex, multi-step
  business logic, background tasks, or sagas that need to run reliably over
  extended periods, survive failures, and maintain state. The `jsr:@ry/cells`
  workflow API is designed for this. (Learn more:
  [jsr.io/@ry/cells](https://jsr.io/@ry/cells))
- **Actor-like Systems:** Provides a programming model with single-threaded
  execution per entity, simplifying state management and concurrency concerns.
- **Scalable Microservices:** Build individual stateful components (cells) that
  can be scaled independently by distributing them across a growing cluster of
  `celld` nodes.

## Configuration

Cells is configured primarily through command-line arguments or environment
variables. Key configuration options typically include:

- **S3 Details (Environment Variables):**
  - `CELL_S3_ENDPOINT`
  - `CELL_S3_BUCKET`
  - `CELL_S3_ACCESS_KEY_ID`
  - `CELL_S3_SECRET_ACCESS_KEY`
  - `CELL_S3_REGION`
  - `CELL_S3_PREFIX` (optional prefix within the bucket)
- `ADVERTISE_ADDR`: The address this node should advertise to other nodes in the
  cluster.
- Network ports for HTTP and internal cluster communication.
- Paths to your Deno application code (e.g., `src/main.ts`) and static files
  when running in single-tenant mode.

Run `docker run --rm ghcr.io/denoland/cells --help` for a full list of options
and detailed explanations.

## Limitations

- **Durability:** SQLite writes are synchronous locally. S3 replication is
  asynchronous; very recent writes (seconds) may be lost on catastrophic node
  failure before replication completes. Workflow state is designed for higher
  durability.
- **Consistency:** Strong consistency for operations within an active Cell (due
  to single ownership). The state in S3 is eventually consistent with the last
  successful replication.
- **Cell DB Size:** Best suited for small to medium-sized SQLite databases
  (e.g., \<500MB) to ensure fast hydration times when a cell activates on a new
  node.
- **Scaling of Individual Cells:** An individual Cell (a Deno isolate) does not
  automatically scale its own resources. The system scales by adding more
  `celld` server nodes to the cluster, allowing more Cells to run concurrently.

## Development Status

Cells is currently **experimental**. While the core functionality is in place,
expect APIs and internal mechanisms to change. We are actively seeking feedback.
