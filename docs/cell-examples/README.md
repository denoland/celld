# Deno Cells

## Simple, Stateful, Scalable Compute Units

Deno Cells provide a new primitive for building modern applications: **tiny,
persistent, real-time compute units** called "Cells".

Stop wiring together separate databases, caches, pub/sub systems, and task
queues. A Cell bundles:

- **Compute:** A secure Deno V8 isolate running your JavaScript/TypeScript code.
- **State:** A private, built-in SQLite database automatically persisted to S3.
- **Connectivity:** Native support for both HTTP requests and WebSocket
  connections.

Deploy **stateful services** – not just functions – with built-in persistence,
resilience, and real-time capabilities, letting you focus purely on your
application logic.

## Ideal For...

Cells significantly reduce the operational overhead for a variety of
applications:

- **Real-time Collaboration:** Document editing, shared whiteboards, presence
  indicators.
- **Multiplayer Game Backends:** Managing game state, player connections, and
  interactions.
- **IoT Device Hubs:** Handling connections, state, and commands for numerous
  devices.
- **Workflow Orchestration:** Implementing complex, long-running business
  processes.
- **Interactive Dashboards:** Pushing real-time data updates to user interfaces.

And notably, Cells provide an excellent substrate for **AI Agents**. Each Cell
can give an agent its own isolated compute environment, persistent memory (via
its private SQLite database), and the ability to run long-running tasks or
workflows using built-in primitives like alarms.

## Key Benefits

- **Effortless Persistence:** Every Cell gets its own SQLite database. State
  changes are automatically streamed to S3 (or compatible storage) via
  Litestream, ensuring durability across restarts and failures with zero complex
  database setup. Perfect for user data, workflow state, or agent memory.
- **Radical Simplicity & DX:** Write straightforward JavaScript/TypeScript using
  an elegant, implicit API. Access state (`cell.db`), communication
  (`cell.broadcast`), identity (`cell.id`), and scheduling (`cell.alarm`)
  directly within your code.
- **Built for Interaction:** Native support for both standard HTTP requests and
  persistent WebSocket connections makes building interactive, real-time
  experiences trivial – whether for UIs or agent communication.
- **Isolated & Secure:** Each Cell runs in a separate Deno V8 isolate. This
  provides robust security boundaries, prevents noisy neighbor problems, and
  simplifies resource management, crucial for multi-tenant services or running
  numerous independent agents.
- **Resilient & Scalable:** Cells operate within a mesh network. Automatic state
  recovery from S3 and request forwarding handle node failures transparently.
  Scale horizontally by simply adding more `celld` nodes.
- **Efficient & Responsive:** Designed for fast activation (sub-100ms target)
  using pre-warmed isolates, ensuring responsive applications even for
  infrequently accessed Cells without paying heavily for idle compute.

## Features

- **Per-Cell Deno Isolates:** Secure, single-threaded JS/TS execution per Cell.
- **Per-Cell SQLite DB:** Private, persistent state, automatically managed.
- **Automatic S3 Persistence:** Continuous backup & recovery via Litestream.
- **WebSocket & HTTP Support:** Real-time and request/response interaction.
- **Mesh Networking:** Automatic discovery, routing, and failover (details
  forthcoming).
- **Scheduling Primitives:** Built-in `cell.alarm` for timed events/workflow
  steps.
- **Fast Activation:** Minimal latency for initializing and running Cells.
- **Simple Implicit API:** Focus on logic, not boilerplate.

## Quick Start (Docker)

Get started instantly with the official Docker image:

**1. Single Node:**

```bash
# Create a directory to hold cell code and data
mkdir ./cell-data

# Run the celld container, mapping port 8000 and the data volume
docker run --rm -it -p 8000:8000 -v "./cell-data:/data" denoland/cell:latest
```

This starts a single `celld` node listening on port 8000. By default, it looks
for Cell code in `/data/<hostname>/code/main.ts` within the container (which
maps to `./cell-data/<hostname>/code/main.ts` on your host). You'll need to
create this file with your Cell logic (see API example below).

**2. Accessing Cells:**

Cells are typically accessed via hostname and a path identifying the Cell:

- `http://my-app.localhost:8000/cell/<cell-id>` (HTTP Request)
- `ws://my-app.localhost:8000/cell/<cell-id>` (WebSocket Connection)

Replace `<cell-id>` with a unique identifier for the specific Cell instance you
want to interact with (e.g., `user123`, `game456`, `agent-abc`). The runtime
routes the request to the correct Cell instance, creating it and restoring state
from S3 if necessary. _(Note: Multi-node setup involves service discovery, often
via S3 - see full documentation when available)._

## API Example

Define your Cell's behavior (e.g., in
`./cell-data/my-app.localhost/code/main.ts`). The API uses implicit context
(`cell.id`, `cell.db`, etc.), made reliable by the one-isolate-per-cell
architecture.

```typescript
import { cell } from "jsr:@deno/cell";

// Top-level code runs once when the isolate starts for a specific cell.
// Good place to initialize DB schema.
console.log(`[${cell.id}] Isolate starting. Initializing schema.`);
cell.db.exec(`
  CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v ANY);
  CREATE TABLE IF NOT EXISTS events (ts TEXT DEFAULT CURRENT_TIMESTAMP, type TEXT, data TEXT);
`);
// Example: Initialize a counter if it doesn't exist
cell.db.run(`INSERT OR IGNORE INTO kv (k, v) VALUES ('visits', 0)`);

// Handle HTTP requests: Increment & return a counter
cell.request(async (req) => {
  console.log(`[${cell.id}] HTTP ${req.method}`);
  await cell.db.run(
    `UPDATE kv SET v = json_extract(v, '$') + 1 WHERE k = 'visits'`,
  );
  const count = await cell.db.get<{ v: number }>(
    `SELECT v FROM kv WHERE k = 'visits'`,
  );
  await cell.db.run(
    `INSERT INTO events (type, data) VALUES (?, ?)`,
    ["http_request", JSON.stringify({ path: new URL(req.url).pathname })],
  );
  return new Response(`Cell ${cell.id} visits: ${count?.v ?? 0}\n`);
});

// Handle WebSocket connections: Send welcome message
cell.connect((ws) => {
  console.log(`[${cell.id}] WS Connect`);
  ws.send(JSON.stringify({ msg: `Welcome to Cell ${cell.id}` }));
  // Maybe retrieve some initial state for the client
  const visits = cell.db.getSync<{ v: number }>(
    `SELECT v FROM kv WHERE k = 'visits'`,
  );
  ws.send(JSON.stringify({ type: "visits", count: visits?.v ?? 0 }));
});

// Handle WebSocket messages: Log, store event, and broadcast
cell.message(async (msg, ws) => {
  console.log(`[${cell.id}] WS Message:`, msg);
  await cell.db.run(`INSERT INTO events (type, data) VALUES (?, ?)`, [
    "websocket_message",
    msg,
  ]);
  // Echo to all clients connected to *this* cell
  cell.broadcast({ from: cell.id, data: msg });
});

// Handle scheduled alarms
cell.alarm(async () => {
  console.log(`[${cell.id}] Scheduled alarm triggered!`);
  // Example: Perform periodic cleanup or check
  const timestamp = new Date(Date.now() - 60 * 60 * 1000).toISOString(); // 1 hour ago
  await cell.db.run(`DELETE FROM events WHERE ts < ?`, [timestamp]);
  await cell.db.run(`INSERT INTO events (type, data) VALUES (?, ?)`, [
    "alarm",
    "periodic_cleanup",
  ]);
  cell.broadcast({ type: "system", message: "Performed periodic cleanup" });
});

// Example: Schedule an alarm 5 seconds after startup (only runs once on init)
// In a real app, you might schedule alarms based on events or requests.
if (cell.db.getSync(`SELECT v FROM kv WHERE k = 'first_init'`) === null) {
  console.log(`[${cell.id}] Scheduling initial alarm.`);
  cell.scheduleAlarm(Date.now() + 5000);
  cell.db.run(`INSERT INTO kv (k, v) VALUES ('first_init', 1)`);
}

console.log(`[${cell.id}] Initialization complete. Ready for requests.`);
```

The API is designed for brevity. Register handlers, use the `cell` object for
context and capabilities. That's the core loop.

## Persistence (S3 Replication)

To enable durable persistence across restarts and nodes, configure these
environment variables when running `celld` (e.g., via
`docker run -e VAR=value ...`):

- `CELL_S3_ENDPOINT`: Your S3 endpoint URL (e.g.,
  `https://s3.us-east-1.amazonaws.com` or `http://localhost:9000` for MinIO).
- `CELL_S3_BUCKET`: The S3 bucket name for storing database replicas.
- `CELL_S3_REGION`: The S3 region (defaults to `us-east-1`).
- `CELL_S3_PREFIX`: Optional path prefix within the bucket (defaults to
  `deno-cells`).
- `CELL_S3_ACCESS_KEY_ID`: Your S3 Access Key ID.
- `CELL_S3_SECRET_ACCESS_KEY`: Your S3 Secret Access Key.

When configured, `celld` automatically uses Litestream to:

- Restore a Cell's state from S3 on activation (if replica exists).
- Continuously replicate database changes to S3.
- Snapshot the database state gracefully on shutdown.
