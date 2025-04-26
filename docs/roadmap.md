# roomd · Roadmap

_Last updated: 2025-04-25_

## Why this matters

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

This isn’t a toy edge runtime. It’s a platform for building multiplayer tools,
collaborative agents, and custom backend logic that persists and scales
horizontally.

## MVP Goals

- [ ] **AI agent chat demo**\
      HTML+WS chat app where a user can say:\
      _“bot, say hello in 2 minutes”_\
      and it does.\
      Backed by SQLite + delayed task + OpenAI completion.\
      Must survive peer crash and room migration.

- [ ] **Durable resharding**\
      Room state must persist even if a container shuts down.\
      Must flush Litestream replica and resume correctly on new owner.\
      Graceful drain + lock expiration + proxy-until-resume strategy.

- [ ] **1 million room demo (if time permits)**\
      A stress test and narrative win:\
      show that you can launch, manage, and persist 1 million lightweight,
      distributed, stateful units.

- [ ] **Local DX polish**\
      Add TypeScript types for room API.\
      Improve LSP/VSCode experience.\
      `deno run -A roomd init` for bootstrapping a new project.\
      Optional: hot-reload loop for local code changes.

- [ ] **Observability**\
      Add per-room metrics:
  - room start time
  - number of connections
  - bytes in/out
  - room lifetime\
    Streaming logs per tenant.\
    Future: Prometheus exporter.

- [ ] **Cron & workflows**\
      Basic scheduler inside isolate using SQLite.\
      `jobs (run_at, payload)` table\
      Execute via `setInterval()` and broadcast result.\
      Use case: time-based messages, reminders, simple background agents.

- [ ] **Multi-tenant sandboxing & metering**\
      Resource tracking per tenant.
  - Number of rooms
  - CPU time (via cgroups?)
  - Network bytes
  - Litestream storage\
    Optional: flat-file billing log.\
    Supports subhosting / billing infrastructure.

## Immediate Next Steps

- Add `jobs` table + background scanner to each room
- Build HTML chat with `/later 60 Hello` command
- Integrate OpenAI call for reminder-style response
- Flush and restore database when peer is drained
- Add SIGTERM → drain logic
- Create `Dockerfile` that does everything:
  - Serves HTTP/WebSocket
  - Runs Litestream
  - Reads a mounted `data/` directory
- Write README for single-container usage

## Out of Scope (for MVP)

- Hyper port: paused (Pingora is fast enough)
- TLS / Let’s Encrypt / ALB integration
- Dynamic autoscaling or live rebalancing
- Postgres / global CRUD APIs

## Vision

roomd is not a better server—it’s a smaller cloud.

A cloud made of tiny, durable, scheduled functions.\
Something you can spin up in a devcontainer, ship in Fargate, or run on bare
metal.\
Multiplayer state and memory, without infrastructure hell.

Build weird things. Build good things. Build rooms.
