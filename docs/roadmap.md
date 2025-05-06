# celld · Roadmap

_Last updated: 2025-05-02_

## What is this?

celld gives you a new kind of building block:

🧱 **Many tiny, durable, real-time workers**—each with its own code, its own
SQLite state, and sub‑100ms cold start.

You don’t need Kubernetes, Postgres, or global state to build collaborative
software.

You spin up a container, mount a directory, and get:

- Durable objects that speak WebSocket and HTTP
- Per-cell state with S3-backed persistence
- Built-in Deno isolation per tenant or project
- Lightweight mesh networking
- Familiar APIs inspired by PartyKit and Durable Objects

## Why this matters?

celld aims to be a simple, resilient "substrate" for building stateful,
distributed applications, particularly envisioning a future of collaborative AI
agents. It provides durable "cells" that manage persistent state (using SQLite

- Litestream to S3) and handles complexities like node discovery, replication,
  and failover automatically. This drastically lowers the barrier for
  developers, allowing them to focus on application logic and rapidly deploy
  sophisticated, reliable systems without getting bogged down in complex
  infrastructure plumbing.

## Roadmap

**Goal:** Demonstrate a multi-node `celld` cluster where nodes can join/leave,
cells remain available after node failures, and data is persisted via
Litestream/S3. Add Alarms and improve DX. Work towards a demo of an AI agent
running in a cell that you can ask "say 'ping' in 10 seconds".

**Core Principle:** Leverage S3 as the "source of truth" for cluster membership
and potentially for lightweight locking/coordination, minimizing direct
peer-to-peer dependencies beyond proxying for data plane. Introduce dedicated
internal communication path for control plane operations.

### Phase 1: Dynamic Node Discovery & Basic Heartbeat

DONE

### Phase 2: S3-based Locking & Robust Litestream Recovery

DONE

### Phase 3: Cell Resilience & Takeover

MOSTLY DONE, `test_concurrent_takeover_locking` and
`test_proxy_forwarding_retry` added but not working.
`test_node_failure_takeover` does work as well as existing ests. Will move on.

### Phase 4: Demo & Refinement

SKIPPING FOR NOW.

- **Goal:** Package the current features into a compelling demonstration.
- **Tasks:**
  1. **Multi-Node Demo Script:**
     - Script to easily launch 2-3 `celld` nodes locally (e.g., using Docker
       Compose or simple shell scripts).
     - A simple client application/script that connects to _any_ node:
       - Creates a few cells (which will be distributed by the hash).
       - Writes/reads data to demonstrate basic function.
       - _Crucially:_ Includes steps to kill one node and show that requests for
         its cells are automatically routed to and served by a backup node after
         a short delay (discovery timeout + restore time).
       - (Bonus): Show a new node starting, joining the cluster (visible via
         logs/querying state?), and taking load for new cells.
  2. **Configuration:** Ensure S3 bucket, region, heartbeat intervals, timeouts,
     etc., are easily configurable (e.g., via environment variables or a config
     file).
  3. **Logging/Observability:** Enhance logging to make the dynamic discovery,
     node failures, lock acquisition, and cell takeovers clearly visible.
  4. **README:** Update documentation explaining the architecture (S3 usage),
     setup, configuration, and how to run the demo.
- **Outcome:** A clear demonstration of the system's dynamic nature and
  resilience, proving the core value proposition of the "substrate".

### Phase 5: Internal Control Plane

DONE

- **Problem:** Internal operations (cluster status checks, future RPCs like
  alarm scheduling/dispatch) currently lack a dedicated, secure communication
  path separate from the public data plane. Debug endpoints (`/_mesh`) are
  inappropriately exposed.
- **Goal:** Establish a separate internal network listener for secure
  node-to-node communication and internal API calls.
- **Tasks:**
  1. **Configuration:** Add new configuration option (e.g., env var
     `INTERNAL_LISTEN_ADDR`) for the internal listener address/port.
  2. **Pingora Setup:** Configure Pingora (`main.rs`) to start a _second_
     listener service bound to the internal address.
  3. **Internal Handlers:** Implement basic handlers on the internal service.
  4. **Migrate Debug Endpoints:** Move the existing `/_mesh/peers` and
     `/_mesh/owner` endpoint logic from the public `ProxyHttp` service to
     handlers on the new internal service. Update any testing/scripts that used
     the old paths.
  5. **Security:** Document that the internal port should be firewalled
     appropriately, only allowing access from other cluster nodes. (No
     application-level auth added in this phase).
- **Outcome:** A dedicated internal communication path exists, improving
  security posture and preparing for features requiring node-to-node RPCs. Debug
  endpoints are no longer publicly exposed.

### Phase 6: Alarms API (Best Effort)

CURRENTLY IN PROGRESS

- **Problem:** Cells cannot schedule tasks to run at a specific time in the
  future, limiting workflow and agent capabilities.
- **Goal:** Implement a Durable Objects-inspired Alarms API allowing cells to
  set, delete, and handle time-based alarms, using best-effort dispatch
  semantics.
- **Depends On:** Phase 5 (Internal Control Plane for RPCs).
- **Tasks:** See detailed `roadmap-phase6.md`.
- **Outcome:** Cells can reliably schedule `onAlarm` handler execution for
  future timestamps, enabling time-based workflows even for dormant cells.

### Phase 7: Developer Experience & Advanced Demos

- **Problem:** The current JS/TS API (`export default`) limits DX and local
  testing. Lack of compelling demos hinders adoption.
- **Goal:** Improve the developer experience and create showcase demos.
- **Tasks:**
  1. **JS API Refactor:** Change API to `import cell from "..."; cell({...});`
     style for better type checking, LSP support, and local testability.
  2. **Compelling Demos:** Develop 1-2 demos showcasing core features, including
     resilience (Phase 4) and Alarms (Phase 6). Examples: Collaborative Pixel
     Canvas, AI Chat w/ Timers, Simple Turn-Based Game.
  3. **Documentation:** Enhance documentation based on API changes and demo
     development.
- **Outcome:** `celld` is easier and more pleasant to develop for. Clear
  examples demonstrate its capabilities.

### Future Streams

- Deploy to Fargate
- Exactly-Once Alarm Semantics
- Cron Support
- Database Migrations API
- Inter-Cell Communication API
- Dynamic Code Deployment (S3-based)
- Enhanced Observability (Distributed Tracing, Dashboards)
- Performance Optimizations (Litestream tuning, proxy improvements)
- Advanced Security (Per-tenant/cell auth tokens, resource limits)
