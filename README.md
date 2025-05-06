# Deno Cells

**Lightweight, Durable, Single-Threaded State Machines for the Web**

celld lets you spin up **tiny, real-time state machines** — each tied to a URL,
WebSocket, or HTTP API.

Every **cell** is:

- A **single-threaded, sandboxed** program (powered by Deno)
- A **durable SQLite database**, streamed to S3/MinIO via Litestream
- A **real-time WebSocket+HTTP endpoint** for clients
- A **recoverable process** that survives peer failures and redeploys

Build distributed systems where **each cell is its own durable, recoverable
machine**—without databases, Kubernetes, or complex scaling layers.

## Why celld?

- Build multiplayer chat, turn-based games, collaborative docs, AI agent
  swarms—anything that needs a durable, memoryful **state machine** per key.
- Deploy locally in a Docker container or scale out horizontally; cells
  auto-route across the mesh.
- Write code the easy way: no multi-threaded race conditions, no locks, no
  shared memory.
- Familiar developer experience: just drop a `main.ts` with `onConnect`,
  `onMessage`, `onRequest`, etc.—no boilerplate `Deno.serve`.

## Architecture Highlights

- **Single-threaded state machines**\
  Each cell runs independently on an event loop—no concurrency bugs, no global
  locks, easy to reason about.

- **Sub-50 ms cold-starts**\
  First client byte typically served in < 50 ms via pre-warmed Deno subprocesses
  and TCP header peeking.

- **Durable state per cell**\
  Each cell lazily gets a local SQLite DB; Litestream replicates changes
  incrementally to object storage.

- **Real-time mesh**\
  Peers discover each other, shard cells via consistent hash, and forward
  WebSocket streams transparently across the network.

- **Static asset offload**\
  Serve `/static/*` directly from disk for maximum speed.

- **Observability built-in**\
  JSON logs per cell, Prometheus metrics for active cells, cold-start times,
  replication lag, etc.

## Quick Start (Single Node)

```bash
docker run --rm -ti -p 3000:3000 \
  -e KNOWN_PEERS="127.0.0.1:3000" \
  -v "$PWD/data:/data" \
  denoland/celld
```

Open two tabs:

- http://ws-echo.localhost:3000/cells/chat1
- http://ws-echo.localhost:3000/cells/chat1

Type—messages echo between the tabs.

## Two-node mesh demo (no Docker)

```bash
# terminal 1
CELLD_PEER_ADDR=127.0.0.1:3000 KNOWN_PEERS=127.0.0.1:3000,127.0.0.1:4000 \
celld --port 3000 --data-dir ./data

# terminal 2
CELLD_PEER_ADDR=127.0.0.1:4000 KNOWN_PEERS=127.0.0.1:3000,127.0.0.1:4000 \
celld --port 4000 --data-dir ./data
```

Clients can connect to either port; cells automatically find their owner.

## Writing cell code

Example: `data/ws-echo.localhost/code/main.ts`

```ts
export default {
  onConnect(ws, { cell }) {
    ws.send(JSON.stringify({ type: "welcome" }));
    cell.broadcast(JSON.stringify({ type: "join", id: ws.id }), [ws.id]);
  },

  onMessage(msg, ws, { cell }) {
    cell.broadcast(msg); // simple echo
  },

  onRequest(req, { cell }) {
    const { pathname } = new URL(req.url);

    if (pathname === "/stats") {
      const [{ count }] = cell.db
        .prepare("SELECT COUNT(*) AS count FROM requests")
        .all();
      return new Response(`Requests so far: ${count}`);
    }

    return new Response("hello from celld\n");
  },
};
```

Each cell automatically:

- Gets a WebSocket upgrade path
- Lazily provisions its own SQLite DB
- Persists state immediately to S3/MinIO without operator involvement

## CLI Help

```
celld 0.1.0
Self-hosted real-time runtime for isolated JavaScript cells.

USAGE:
    celld [OPTIONS]

OPTIONS:
    -p, --port <PORT>           Port to bind the HTTP/WebSocket server [default: 3000]
    -d, --data-dir <DIR>        Root data directory [default: ./data]
    -n, --known-peers <PEERS>   Comma-separated list of peer addresses (host:port)
    -h, --help                  Print help information
    --version                   Print version info

DESCRIPTION:
    Celld is a lightweight runtime inspired by Durable Objects and Deno Deploy.
    It runs tenant-isolated JavaScript logic in per-cell Deno subprocesses,
    supports real-time WebSocket connections, and serves static files per tenant.

    Routing is based on the request Host and cell ID:

        http://myapp.localhost:3000/cells/chat1
              └──────────┬────────┘ └────┬────┘
                   tenant domain        cell ID

    Each cell runs in an isolated subprocess with its own state and lifecycle.

DATA LAYOUT:
    The data directory contains one folder per tenant domain:

        <data-dir>/
        └── myapp.localhost/
            ├── static/          # Served at /
            │   └── index.html, client.js, etc.
            ├── code/            # Cell logic
            │   └── main.ts      # Exports onConnect, onMessage, etc.
            └── sockets/         # Internal cell sockets (created at runtime)

EXAMPLE:
    Run celld with two peers:
        celld --port 3000 --data-dir ./data --known-peers localhost:3000,localhost:4000

    Open:
        http://myapp.localhost:3000/cells/chat1

MORE:
    - /cells/{cellId} connections are upgraded to WebSockets
    - Each tenant's code is hot-reloaded when the cell starts
    - Durable state and cold-start recovery with SQLite + S3/MinIO

S3 REPLICATION:
    Configure SQLite replication to S3/MinIO via these environment variables:

    CELLD_S3_ENDPOINT           S3 endpoint URL (e.g., http://localhost:9000)
    CELLD_S3_BUCKET             Bucket name for storing cell databases
    CELLD_S3_REGION             S3 region (defaults to us-east-1 if not specified)
    CELLD_S3_PREFIX             Path prefix within bucket (defaults to "celld")
    CELLD_S3_ACCESS_KEY_ID      S3 access key ID
    CELLD_S3_SECRET_ACCESS_KEY  S3 secret access key

    When these variables are set, celld will automatically:
    - Restore cell databases from S3 on cold start
    - Continuously replicate changes to S3
    - Gracefully flush and snapshot on shutdown

PROJECT:
    https://github.com/denoland/celld
```
