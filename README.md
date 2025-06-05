# Deno Cells

<img src="docs/cells.svg" alt="Deno Cells Logo" width="200">

## Simple, Stateful, Scalable Compute

**Deno Cells** is a self-hosted runtime for stateful, distributed
JavaScript/TypeScript applications.

Each **cell** is a uniquely identified, single-threaded **Deno isolate** with
its own private **SQLite database**. Cells activate on demand and **scale
horizontally**-but for any given ID, the system guarantees **exactly one active
instance** across the cluster.

Cells run reliably on your own infrastructure. The only external dependency is
**S3 (or compatible)**-used for both storage and coordination.

 **Experimental** - APIs may change.

**Docker image:** `ghcr.io/denoland/cells` 

**SDK:** [`jsr:@ry/cells`](https://jsr.io/@ry/cells)

## Highlights

- **Per-ID single-instance isolation** across the cluster
- **Durable state**: SQLite DB per cell, replicated to S3
- **Workflow API** for fault-tolerant, long-lived tasks with built-in retries
- **System Cell**: A built-in scheduler that triggers alarms on a schedule
- **Cold starts \~100ms** for small cells
- **Runs anywhere** with only S3 as external dependency

## Quick Start

```bash
docker pull ghcr.io/denoland/cells
docker run ghcr.io/denoland/cells --help
```

## Hello World

```ts
import { cell } from "jsr:@ry/cells";

cell.db.exec(`CREATE TABLE IF NOT EXISTS c (id TEXT PRIMARY KEY, v INTEGER)`);
cell.db.exec(`INSERT OR IGNORE INTO c (id, v) VALUES ('hits', 0)`);

cell.request(() => {
  cell.db.exec(`UPDATE c SET v = v + 1 WHERE id = 'hits'`);
  const { v } = cell.db.prepare(`SELECT v FROM c WHERE id = 'hits'`).get();
  return new Response(`${v} (cell ID: ${cell.id})\n`);
});
```

Run it:

```bash
docker run -p 8000:8000 -v $PWD:/app ghcr.io/denoland/cells ./main.ts
```

## Workflow Example

```ts
import { cell } from "jsr:@ry/cells";
import { delay } from "jsr:@std/async";

type MyWorkflow = {
  "two-steps": {
    value: number;
  };
};

const workflow = cell.initWorkflow<MyWorkflow>();

workflow.define({
  name: "two-steps",
  handler: async (ctx) => {
    const step1Result = await ctx.step.run("multiply by 2", async () => {
      await delay(10);
      return ctx.event.data.value * 2;
    });

    await ctx.step.run("add 1", async () => {
      await delay(10);
      return step1Result + 1;
    });
  },
});

workflow.dispatch("two-steps", { value: 42 });
```

Workflow steps are **durable**, **ordered**, and **automatically retried** (4
times by default) if a crash occurs mid-run.

## Concepts

- **Cell** = Deno isolate + SQLite DB + your code.
- **Cluster** = Multiple nodes coordinate via S3 and consistent hashing.
- **Routing**: HTTP/WebSocket requests are routed to the node hosting the cell.
  If a node fails, another safely takes over.
- **System Cell**: A built-in scheduler that triggers alarms and workflow steps
  on schedule.
- **Single-tenancy or multi-tenancy**: Routing can be based on hostname, or
  skipped entirely in simpler deployments.

## Routing

- `GET /cell/<id>` → Routes to cell’s HTTP handler
- `WS /cell/<id>` → Opens a WebSocket to the cell
- `/` → Serves static files from tenant’s `static/` directory

## Use Cases

- **AI agents**: Isolated memory + logic per agent, with long-running tasks
- **Multiplayer games & chat**: Real-time sessions backed by persistent state
- **Business workflows**: Durable background logic with retries and timers
- **Per-user state**: Each user or object gets its own stateful cell

## Operational Notes

- **Replication delay**: SQLite writes are sync locally; replication to S3 is
  async (a few seconds).
- **DB size**: Best under ~500MB per cell to ensure fast activation.
- **No vertical autoscaling**: Scale by adding nodes to the cluster and sharding
  work over many cells, but there's a limit to how much a single cell can
  handle.
- **Request loss on crash**: In-flight requests are not preserved; workflows
  resume from last successful step.
