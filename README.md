# roomd

The mesh‑aware daemon that lets you run **Durable‑Object‑style “rooms”** on your
own infrastructure, with sub‑50 ms cold‑starts, real‑time WebSockets, and
durable SQLite state replicated to S3 or MinIO via Litestream.

## Why roomd?

- Build multiplayer chat, games, CRDT docs, AI agent swarms—anything that needs
  a stateful “room” of logic.
- Deploy locally in one Docker container or scale out to 1 000+ nodes; each room
  is automatically routed to exactly one peer.
- Keep state safe: every room writes to its own SQLite file that streams WAL
  changes to object storage.
- Enjoy a first‑class developer DX: drop a `main.ts` file that exports familiar
  hooks (`onConnect`, `onMessage`, `onRequest`, …)—no boilerplate Deno.serve.

## Features

- **Room API**\
  `/rooms/{roomId}` endpoint, PartyKit‑style hooks, automatic WebSocket upgrade.
- **Mesh routing**\
  Peers discover each other from `KNOWN_PEERS`, consistent‑hash every roomId to
  a single owner, proxy WS frames across nodes.
- **Lightning cold‑start**\
  Deno subprocess reuse plus TCP header peek; first byte < 50 ms on an idle
  node.
- **Durable state**\
  Per‑room SQLite; Litestream replicates WALs to S3/MinIO every second.
- **Static asset offload**\
  Serve `/static/*` straight from disk before it hits JavaScript.
- **Observability**\
  JSON logs per room, Prometheus endpoint (active rooms, cold‑start latency,
  replication lag).

## Quick start (single node)

```bash
# one‑liner demo
docker run --rm -ti -p 3000:3000 \
  -e KNOWN_PEERS="127.0.0.1:3000" \
  -v "$PWD/data:/data" \
  denoland/roomd
```

Open two tabs:

- http://ws‑echo.localhost:3000/rooms/chat1
- http://ws‑echo.localhost:3000/rooms/chat1

Type—messages echo between the tabs.

## Two‑node mesh demo (no Docker)

```bash
# terminal 1
ROOMD_PEER_ADDR=127.0.0.1:3000 KNOWN_PEERS=127.0.0.1:3000,127.0.0.1:4000 \
roomd --port 3000 --data-dir ./data

# terminal 2
ROOMD_PEER_ADDR=127.0.0.1:4000 KNOWN_PEERS=127.0.0.1:3000,127.0.0.1:4000 \
roomd --port 4000 --data-dir ./data
```

Any client can connect to either port; rooms automatically locate their owner.

## Writing room code

`data/ws-echo.localhost/code/main.ts`

```ts
export default {
  onConnect(ws, { room }) {
    ws.send(JSON.stringify({ type: "welcome" }));
    room.broadcast(JSON.stringify({ type: "join", id: ws.id }), [ws.id]);
  },

  onMessage(msg, ws, { room }) {
    room.broadcast(msg); // simple echo
  },

  onRequest(req, { room }) {
    const { pathname } = new URL(req.url);

    if (pathname === "/stats") {
      const [{ count }] = room.db
        .prepare("SELECT COUNT(*) AS count FROM requests")
        .all();
      return new Response(`Requests so far: ${count}`);
    }

    return new Response("hello from roomd\n");
  },
};
```

Access a ready‑to‑use SQLite handle at `room.db`—no boilerplate, created lazily
on first use.

## Environment variables

| Variable              | Purpose                                           |
| --------------------- | ------------------------------------------------- |
| `KNOWN_PEERS`         | Comma‑separated host:port list for peer discovery |
| `ROOMD_S3_ENDPOINT`   | S3 or MinIO URL (`http://localhost:9000`)         |
| `ROOMD_S3_BUCKET`     | Bucket name (`roomd-dev`)                         |
| `ROOMD_S3_REGION`     | Region (`us-east-1`)                              |
| `ROOMD_S3_PREFIX`     | Path prefix per tenant (`roomd`)                  |
| `ROOMD_S3_ACCESS_KEY` | Access key                                        |
| `ROOMD_S3_SECRET_KEY` | Secret key                                        |

roomd builds a Litestream config automatically:

```
s3://$ROOMD_S3_BUCKET/$ROOMD_S3_PREFIX/<tenant>/<roomId>
```

## CLI help excerpt

```
roomd 0.1.0  Self‑hosted Durable‑Object runtime

USAGE:
  roomd [--port 3000] [--data-dir ./data]

OPTIONS:
  -p, --port <PORT>           HTTP / WS port [default: 3000]
  -d, --data-dir <DIR>        Tenant data root [default: ./data]
  -n, --known-peers <LIST>    "host:port,host:port" mesh bootstrap
  --log-level <LEVEL>         error | warn | info | debug
```

See docs/desired-help-output.txt for full details.

## Roadmap snapshot

1. Seamless `roomd dev` hot‑reload, tunnel URL.
2. Peer mTLS for zero‑config secure clusters.
3. Resource quotas per isolate (cgroups + V8).
4. Hosted control plane (optional SaaS).
5. Geo‑sharding and room migration.

Full roadmap lives in docs/roadmap.md.
