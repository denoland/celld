# Deno Cells SDK

This module contains the TypeScript SDK for developing applications that run on
[Deno Cells](https://ghcr.io/denoland/cells). Deno Cells provides a model where
each uniquely identified entity (a "cell") runs as a single
JavaScript/TypeScript isolate with its own private, synchronous SQLite database,
durably persisted to S3.

For information about how to use Deno Cells run this:
```
docker run ghcr.io/denoland/cells --help
```

The primary way to interact with the Cells environment from within your cell
code is through the `cell` object imported from `jsr:@ry/cells`.

## Example

Here's a basic example of a cell that maintains a counter in its private SQLite
database:

```typescript
import { cell } from "jsr:@ry/cells";

cell.db.exec(
  `CREATE TABLE IF NOT EXISTS counter (id TEXT PRIMARY KEY, value INTEGER)`,
);
cell.db.exec(`INSERT OR IGNORE INTO counter (id, value) VALUES ('hits', 0)`);

cell.request((req) => {
  cell.db.exec(`UPDATE counter SET value = value + 1 WHERE id = 'hits'`);
  const result = cell.db.prepare(`SELECT value FROM counter WHERE id = 'hits'`)
    .get();
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

And you can hit the cell with:

```bash
curl http://localhost:8000/cell/foo
```
## Cell Object

`cell.id` is a unique identifier for the cell, which corresponds to the URL that
invoked it (e.g., `http://localhost:8000/cell/foo` would have `cell.id` of
`"foo"`).

`cell.tenant` is corresponds to the tenant ID of the cell in multi-tenant mode.

## HTTP Request Handling

`cell.request(cb)` registers a callback to handle HTTP requests sent to the
cell.

```ts
import { cell } from "jsr:@ry/cells";
cell.request((req) => {
  const url = new URL(req.url);
  if (url.pathname === "/hello") {
    return new Response("Hello from Cell!");
  } else {
    return new Response("Not Found", { status: 404 });
  }
});
```

## WebSockets

`cell.connect(cb)` registers a callback to handle WebSocket connections, while
`cell.message(cb)` registers a callback to handle messages sent over WebSockets.

`cell.broadcast(message)` sends a message to all connected WebSocket clients.

`cell.close(cb)` registers a callback to handle WebSocket disconnections.

`cell.error(cb)` registers a callback to handle errors that occur during
WebSocket communication.

`cell.broadcast(message)` sends a message to all connected WebSocket clients.

`cell.sockets` provides access to the currently connected WebSocket clients.

## Alarms

`cell.alarm(cb)` registers a callback to handle alarms, which are scheduled.

`cell.setAlarm(scheduledTimeUnixMs): Promise<ScheduledTaskId>` schedules an alarm to trigger at a specific time,

`cell.deleteAlarm(id: ScheduledTaskId): boolean` deletes a
scheduled alarm and returns true if deleted, false if not found.

`cell.getAlarm(id?: ScheduledTaskId): number | null`
gets the time when the alarm is scheduled to trigger, or `null` if it doesn't
exist. If no ID provided, returns the next scheduled alarm time.

## SQLite Database

Each cell has its own private SQLite database `cell.db`, which is automatically
created and backed up to S3. It is an instance of `DatabaseSync` from the
`node:sqlite` API (which is supported by Deno).

See https://nodejs.org/api/sqlite.html

`cell.db.exec(sql: string)` executes a SQL statement that does not return

`cell.db.prepare(sql: string)` prepares a SQL statement for execution. Returns a
`StatementSync` object that can be used to execute the statement with
parameters.

`statement.all([namedParameters][, ...anonymousParameters])` executes the
prepared statement and returns all results as an array. 

`statement.get([namedParameters][, ...anonymousParameters])` executes the
prepared statement and returns the first result as an object, or `undefined` if
no results were found.

`statement.run([namedParameters][, ...anonymousParameters])` executes the
prepared statement and returns run result info (changes, lastInsertRowid).


## Durable Workflows

A key feature of the Deno Cells SDK is the **Workflow API**, available via
`cell.workflow`. Inspired by Inngest, this API allows you to define
long-running, fault-tolerant processes that can maintain state, survive
restarts, and continue execution. It's designed for implementing complex,
multi-step operations reliably within a cell.

Use the `cell.workflow.define` function to create type-safe workflow
definitions.

#### Simple Workflows (No Input)

```typescript
const dailyCleanup = cell.workflow.define<null, void>({
  name: "daily.cleanup",
  handler: async ({ step }) => {
    await step.run("delete-temp-files", () => deleteTempFiles());
    await step.run("optimize-database", () => optimizeDB());
  },
});

// Dispatch without arguments
cell.workflow.dispatch(dailyCleanup);
```

#### Workflows with Input

```typescript
const processOrder = cell.workflow.define<{
  orderId: string;
  items: Array<{ productId: string; quantity: number }>;
}, {
  orderId: string;
  paymentId: string;
  trackingNumber: string;
}>({
  name: "order.process",
  handler: async ({ input, step }) => {
    const validation = await step.run(
      "validate-order",
      () => validateOrder(input.orderId),
    );

    const payment = await step.run(
      "charge-payment",
      () => chargeCustomer(input.orderId),
    );

    const shipment = await step.run(
      "create-shipment",
      () => shipOrder(input.orderId, input.items),
    );

    return {
      orderId: input.orderId,
      paymentId: payment.id,
      trackingNumber: shipment.tracking,
    };
  },
});

// Type-safe dispatch
cell.workflow.dispatch(processOrder, {
  orderId: "order_123",
  items: [{ productId: "prod_abc", quantity: 2 }],
});
```

### Step Functions

Steps are the building blocks of durable execution. Each step is memoized in the
Cell's database.

#### `step.run(name, fn)`

Execute code as a durable step:

```typescript
const result = await step.run("fetch-user", async () => {
  const user = await fetch(`/api/users/${userId}`);
  return user.json();
});
```

- Results are cached and reused on retry
- Steps must have unique, stable names
- Functions should be idempotent

#### `step.invoke(workflow, input?)`

Invoke another workflow as a step:

```typescript
const emailWorkflow = cell.workflow.define<
  { to: string; subject: string },
  { messageId: string }
>({
  name: "send.email",
  handler: async ({ input }) => {
    // Send email logic
    return { messageId: "msg_123" };
  },
});

const mainWorkflow = cell.workflow.define<null, { emailId: string }>({
  name: "user.onboard",
  handler: async ({ step }) => {
    // Invoke another workflow
    const result = await step.invoke(emailWorkflow, {
      to: "user@example.com",
      subject: "Welcome!",
    });

    return { emailId: result.messageId };
  },
});
```

### Monitoring Workflows

#### Get Run Progress

```typescript
const runId = cell.workflow.dispatch(myWorkflow, { data: "example" });

// Check progress
const progress = cell.workflow.getRunProgress(runId);
console.log(progress);
// {
//   id: "01JWQF48VFXJ5Q0BCRM03HA2XQ",
//   workflowName: "my.workflow",
//   dispatchedAt: "2025-06-02T04:42:28.584Z",
//   completedAt: null,
//   steps: [{
//     stepIndex: 1,
//     name: "fetch-user",
//     outputData: { ... },
//     completedAt: "2025-06-02T04:42:29.584Z"
//   }]
// }
```

#### List All Runs

```typescript
// All runs in this Cell
const allRuns = cell.workflow.listRuns();

// Filter options
const pendingRuns = cell.workflow.listRuns({ status: "pending" });
const recentSignups = cell.workflow.listRuns({
  workflowName: "user.signup",
  status: "completed",
  limit: 10,
});
```

### Architecture & Isolation

Workflows leverage Cells' architecture for reliability:

1. **Single Isolate**: All workflows in a Cell run in its single V8 isolate
2. **SQLite Persistence**: Step results and workflow state are stored in the
   Cell's SQLite database
3. **Automatic Recovery**: When a Cell restarts, pending workflows resume from
   their last completed step
4. **Cell Isolation**: Workflows cannot access or interfere with other Cells
5. **Lazy Initialization**: Workflow tables are only created when first used

This design ensures workflows are both durable and secure, with complete data
isolation between Cells.

### Complete Workflow Example

```typescript
// Email workflow (reusable)
const sendEmail = cell.workflow.define<{
  to: string;
  subject: string;
  template: string;
  data: Record<string, any>;
}, { messageId: string }>({
  name: "email.send",
  handler: async ({ input, step }) => {
    const html = await step.run(
      "render-template",
      () => renderTemplate(input.template, input.data),
    );

    const result = await step.run(
      "send-via-smtp",
      () => smtpSend(input.to, input.subject, html),
    );

    return { messageId: result.id };
  },
});

// User signup workflow
const userSignup = cell.workflow.define<{
  email: string;
  name: string;
}, {
  userId: string;
  welcomeEmailId: string;
}>({
  name: "user.signup",
  handler: async ({ input, step }) => {
    // Create user
    const user = await step.run(
      "create-user",
      () => createUser(input.email, input.name),
    );

    // Send welcome email (via workflow composition)
    const welcomeEmail = await step.invoke(sendEmail, {
      to: input.email,
      subject: "Welcome!",
      template: "welcome",
      data: { name: input.name },
    });

    // Set up account
    await step.run(
      "initialize-account",
      () => setupUserAccount(user.id),
    );

    return {
      userId: user.id,
      welcomeEmailId: welcomeEmail.messageId,
    };
  },
});

// Usage
const runId = cell.workflow.dispatch(userSignup, {
  email: "alice@example.com",
  name: "Alice",
});
```

### Workflow Roadmap

- [ ] Support cron-scheduled workflows
- [ ] Support interval-based workflows
- [ ] Support `retries` in event-triggered workflows
- [ ] Support `concurrency` in event-triggered workflows
- [ ] Add logging (automatically records what's happening in a workflow run)
- [ ] Track retry count for each workflow step with configurable limits
- [ ] Support `NonRetriableError` and `RetryAfterError`
- [ ] Pass correct value for `attempt` in workflow handler calls
- [ ] Prevent idle shutdown when workflows are running
