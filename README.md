# Deno Cells

<img src="docs/cells.svg" alt="Deno Cells Logo" width="200">

## Simple, Stateful, Scalable Compute Units

Deno Cells are self-hostable compute units for modern apps. They bundle:

- **Deno** (isolated compute)
- **SQLite** (private state)
- **Litestream** to **S3** (persistence, replication)
- **HTTP** and **WebSocket** (connectivity)

Build stateful services. Focus on logic. S3 is the main dependency.

## Why Deno Cells?

`celld` runs durable "cells" that manage state. It handles discovery,
replication, and failover.

- Easy Persistence: SQLite per Cell, backed by S3.
- Simple Code: JavaScript/TypeScript, implicit API (`cell.db`,
  `cell.broadcast`).
- Interactive: Built-in HTTP & WebSockets.
- Scalable: Add `celld` nodes.

## Ideal For

- Real-time collaboration
- Game backends
- IoT hubs
- AI agent memory/compute

## Quick Start (Single Node Docker)

```bash
# Make a data directory
mkdir ./cell-data
# Run (replace S3 details and <hostname>)
docker run --rm -it -p 8000:8000 -v "./cell-data:/data" \
  ghcr.io/denoland/cells:latest
```

Create `./cell-data/<hostname>/src/main.ts` for your Cell logic.

For requests without a Host header or with unrecognized hostnames, the system
falls back to a "default" tenant located at `./cell-data/default/src/main.ts`.

## Accessing Cells

- `http://<tenant-hostname>:<port>/cell/<cell-id>` (HTTP)
- `ws://<tenant-hostname>:<port>/cell/<cell-id>` (WebSocket)
- `http://localhost:8000/cell/<cell-id>` (no Host header - uses default tenant)

Routing is based on the request Host header and cell ID:

```
http://myapp.localhost:3000/cell/chat1
       └─────┬───────┘           └─┬─┘
       tenant domain           cell ID
```

Each cell runs in an isolated subprocess with its own state and lifecycle. Cells
with active WebSocket connections or outbound TCP connections remain alive,
automatically terminating when idle.

## Data layout

The data directory contains one folder per tenant domain:

```
<data-dir>/
└── myapp.localhost/
    ├── static/          # Served at /
    │   └── index.html, client.js, etc.
    ├── src/
    │   └── main.ts      # logic for all cells
    ├── sqlite/          # sqlite per room
    │   └── A.db
    │   └── B.db
```

## Writing Cell Code

Example `main.ts`:

```typescript
import { cell } from "jsr:@ry/cells";

// Runs once per cell start. Init DB schema.
cell.db.exec(`CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v ANY)`);

// Handle HTTP
cell.request((req: Request): Response => {
  // Use cell.db to read/write state
  return new Response(`Cell ${cell.id} says hi\n`);
});

// Handle WebSockets
cell.connect((socket: WebSocket, id: string) => {
  socket.send(JSON.stringify({ message: `Welcome to Cell ${cell.id}` }));
});

cell.message((event: MessageEvent, socket: WebSocket, id: string) => {
  // cell.broadcast(event.data);
});
```

Key API: `cell.id`, `cell.db`, `cell.request`, `cell.connect`, `cell.message`,
`cell.broadcast`.

## Configuration

For Persistence & Discovery, setup S3:

- `CELL_S3_ENDPOINT`
- `CELL_S3_BUCKET`
- `CELL_S3_ACCESS_KEY_ID`
- `CELL_S3_SECRET_ACCESS_KEY`
- `CELL_S3_REGION`
- `CELL_S3_PREFIX`

Also use

- `ADVERTISE_ADDR` to set the node's public address.

## Roadmap

Done / Mostly Done:

- Core functionality (Compute, State, HTTP/WS)
- Litestream/S3 Persistence
- Dynamic Node Discovery (via S3)
- Cell Resilience & Takeover
- Internal Control Plane

In-Progress / Next:

- Alarms API (Scheduling)
- Cron
- Workflow API (Like inngest)
- Developer Experience
- Advanced Demos

## Local Development

### Prerequisites

- Deno v2.3+ (to use `DENO_SERVE_ADDRESS` env var)
- sqlite3: `brew install sqlite3`
- [litestream](https://litestream.io/install/)
- MinIO for testing: `brew install minio`
- MinIO client for testing: `brew install minio/stable/mc`
- Configure `*.localhost` to be resolved to the local loopback address (if not
  configured by default)

### Commands

- Build: `cargo build`
- Run: `cargo run`
- Test All: `cargo test`
