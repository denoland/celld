# Deno Cells SDK

This module contains the TypeScript SDK for developing applications that run on
[Deno Cells](https://ghcr.io/denoland/cells). Deno Cells provides a model where
each uniquely identified entity (a "cell") runs as a single
JavaScript/TypeScript isolate with its own private, synchronous SQLite database,
durably persisted to S3.

For comprehensive information about the Deno Cells project, how to run it, and
its architecture, please refer to the main project page:
**[ghcr.io/denoland/cells](https://ghcr.io/denoland/cells)**

## SDK Usage

The primary way to interact with the Cells environment from within your cell
code is through the `cell` object imported from `jsr:@ry/cells`.

### Example: A Simple Counter Cell

Here's a basic example of a cell that maintains a counter in its private SQLite
database:

```typescript
import { cell } from "jsr:@ry/cells";

// Initialize the database schema if it doesn't exist for this cell
cell.db.exec(
  `CREATE TABLE IF NOT EXISTS counter (id TEXT PRIMARY KEY, value INTEGER)`,
);

// Insert the counter row if it doesn't exist
cell.db.exec(`INSERT OR IGNORE INTO counter (id, value) VALUES ('hits', 0)`);

// Handle incoming HTTP requests to this cell
cell.request((req) => {
  // Increment the counter
  cell.db.prepare(`UPDATE counter SET value = value + 1 WHERE id = 'hits'`)
    .run();

  // Read the new value
  const result = cell.db.prepare(`SELECT value FROM counter WHERE id = 'hits'`)
    .get();

  // Respond with the new count and the cell's unique ID
  return new Response(
    `Count: ${result.value} (Served by Cell ID: ${cell.id})\n`,
  );
});
```

This `main.ts` would be the entry point for your cell's logic. You can then run
this like so:

```bash
docker run -p 8000:8000 -v $PWD:/app ghcr.io/denoland/cells ./main.ts
```

## Durable Workflows

A key feature of the Deno Cells SDK is the **Workflow API**, available via
`cell.workflow`. This API allows you to define long-running, fault-tolerant
processes that can maintain state, survive restarts, and continue execution.
It's designed for implementing complex, multi-step operations reliably within a
cell.

### Basic Workflow Example

```typescript
import { cell } from "jsr:@ry/cells";

// Define a workflow
const emailWorkflow = cell.workflow.define<{
  to: string;
  subject: string;
}>({
  name: "send.email",
  handler: async ({ input, step }) => {
    // Each step is automatically memoized and fault-tolerant
    const html = await step.run("render-template", async () => {
      return `<h1>Hello ${input.to}!</h1>`;
    });

    const result = await step.run("send-email", async () => {
      // This would call your email service
      return { messageId: "msg_123" };
    });

    return { messageId: result.messageId };
  },
});

// Dispatch the workflow
const runId = cell.workflow.dispatch(emailWorkflow, {
  to: "user@example.com",
  subject: "Welcome!",
});

// Monitor progress
const progress = cell.workflow.getRunProgress(runId);
```

### Workflow Features

- **Fault tolerance**: Steps are memoized and won't re-execute on retry
- **Composition**: Workflows can invoke other workflows using `step.invoke()`
- **Monitoring**: Track progress and status of workflow runs
- **Lazy initialization**: Workflow tables are only created when first used

For complete workflow documentation, see [workflow.md](../docs/workflow.md).
