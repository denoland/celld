# Architecture: Self‑Hosted Deno Deploy (MVP)

A containerized, multi‑tenant Deno runtime delivered via Docker. It combines
**PartyKit‑style cells**, **Erlang‑inspired mesh routing**, **real‑time
WebSocket hooks**, and **sub‑50 ms cold starts**, with secure per‑tenant
isolation and fast static asset serving.

### 1. Quickstart Demo

```bash
# Boot two peer‑aware runtime nodes
docker run \
  -e KNOWN_PEERS="localhost:3000,localhost:4000" \
  -e DATA_DIR="/data" \
  -p 3000:3000 \
  -ti denoland/deploy

docker run \
  -e KNOWN_PEERS="localhost:3000,localhost:4000" \
  -e DATA_DIR="/data" \
  -p 4000:3000 \
  -ti denoland/deploy
```

Open two browser tabs pointing at `http://tenant.local:3000/cells/chat1`.\
Type in one tab and see the message appear in the other—across containers.

### 2. High‑Level Goals

- **Cell API**\
  First‑class `/cells/{cellId}` endpoint with handlers\
  `cell.request`, `cell.connect`, `cell.message`, `cell.close`, and `cell.error`
- **Peer Mesh**\
  Containers read `KNOWN_PEERS`, form an Erlang‑style mesh, and shard cells by
  consistent hashing
- **Ultra‑Fast Cold‑Start**\
  TCP header peek + Deno subprocess reuse → first byte in < 50 ms
- **Static Asset Offload**\
  Proxy serves `/index.html`, `/client.js`, etc. directly from disk
- **Strict Isolation**\
  Each tenant runs in its own Deno subprocess (V8 sandbox + cgroup limits)
- **Horizontal Scale‑Out**\
  Add nodes to increase capacity; cells auto‑route to their owner

### 3. Core Components

#### 3.1 Proxy Router (Port 3000)

- **Technology**: Pingora (Rust) for HTTP/1.x, WebSocket, future TLS
- **Pipeline**:
  1. **TenantExtraction** – parse `Host:` → tenant context
  2. **StaticFileModule** – serve `$DATA_DIR/<tenant>/static/*`
  3. **CellProxyService** – intercept `/cells/{cellId}`, decide local vs remote

- **Routing Logic**:
  - **Local** – connect to Unix socket at `/data/<tenant>/sockets/{cellId}.sock`
  - **Remote** – forward connection to the owning peer (consistent‑hash on
    `cellId`)

#### 3.2 Peer Mesh

- **Discovery** – read `KNOWN_PEERS` at startup
- **Handshake** – establish TCP connections to peers, maintain live list
- **Sharding** – hash `cellId` → peer index (ensures exactly one owner per cell)
- **Use‑cases** – cross‑container WS proxying, fail‑over, elastic scaling

#### 3.3 Subprocess Manager

- **Bootstrap Shim** – `jsr:@ry/cells` provides the `cell` singleton with
  implicit API
- **Permissions** – Deno CLI flags (`--allow-net`, `--allow-read`, etc.)
- **Resource Limits** – Linux cgroups v2 for CPU & memory
- **Lifecycle** – reuse warm processes, scale to zero on idle, spawn single‑use
  isolates for cold‑start tests

### 4. Developer API

Drop `data/<tenant>/src/main.ts` using the cell API:

```ts
import { cell } from "jsr:@ry/cells";

console.log(`[${cell.id}] Initializing...`);

// Handle HTTP requests
cell.request((req: Request): Response => {
  return new Response(`Cell ${cell.id} got ${req.method}`, {
    status: 200,
  });
});

// Handle WebSocket connections
cell.connect((socket: WebSocket, id: string) => {
  console.log(`[${cell.id}] Client connected: ${id}`);
  socket.send(`Welcome to cell ${cell.id}!`);
});

// Handle WebSocket messages
cell.message((event: MessageEvent, socket: WebSocket, id: string) => {
  console.log(`[${cell.id}] Message from ${id}: ${event.data}`);
  // Echo to all connected clients
  cell.broadcast(`${id}: ${event.data}`);
});

// Handle WebSocket disconnections
cell.close((socket: WebSocket, id: string) => {
  console.log(`[${cell.id}] Client disconnected: ${id}`);
});

// Handle WebSocket errors
cell.error((error: Error | ErrorEvent | Event) => {
  console.error(`[${cell.id}] WebSocket error:`, error);
});
```

- **No manual `Deno.serve`** – the `cell` singleton takes care of HTTP & WS
- All dynamic behavior lives under `/cells/{cellId}`

### 5. Storage & Layout

```
/data/
└── <tenant>/
    ├── static/        # index.html, client.js, assets
    ├── src/          # main.ts only
    └── sockets/       # {cellId}.sock per active cell
```

### 6. Observability

- **Proxy Logs** – routing decisions, peer health, cold‑start timings
- **Subprocess Logs** – `console.*` from JS code
- **Future** – OpenTelemetry, per-cell metrics, dashboards

### 7. Inspirations & Aspirations

- **Erlang VM Mesh** – dynamic, fault‑tolerant distributed nodes
- **Cloudflare Durable Objects** – location‑transparent, stateful APIs
- **PartyKit** – seamless real‑time front-end ↔ back-end hooks

### 8. Future Enhancements

- **Central Control Plane** (`app.deno.com`) for orchestration & config
- **Automated TLS** – certificate management via Let's Encrypt
- **Metering & Billing** – per-tenant usage tracking (CPU, memory, bandwidth)
- **Background Jobs & Cron** – scheduled tasks
- **Multi‑Region & Geo‑Routing** – global distribution
- **CLI & Dashboard** – user-friendly deploys, logs, and metrics

## What's Missing?

1. **Authentication & ACLs** – secure per-tenant access control (JWT, API keys)
2. **Backpressure & Error Handling** – graceful degradation under load
3. **Durable Storage** – integrated KV or database for persisted state
4. **Testing Strategy** – unit/integration tests for proxy, mesh, and shim
5. **Shard Rebalancing** – migrating cells when peers change
6. **Developer UX** – CLI design and dashboard mockups

- Details on how cells persist state or recover after failure (e.g., cold‑start
  rehydration)
- Examples of real-world workflows (e.g., multiplayer games, collaborative
  editing)
- Security considerations around WebSocket origin checks and CORS
- Performance benchmarks (throughput, latency under load)
