# Cells Workflow API (MVP)

This module defines a minimal workflow runtime for use with the Cells runtime
(`jsr:@ry/cells`). Inspired by Inngest, it supports durable background functions
triggered by events or schedules.

Import:

```ts
import * as workflow from "jsr:@deno/cells/workflow";
```

## Defining workflows

### `workflow.on(eventName: string, handler: HandlerFn)`

Register a workflow that runs when a named event is dispatched.

```ts
workflow.on("user.signup", async ({ event, step }) => {
  const user = await step("fetch-user", () => getUser(event.userId));
  await step("send-email", () => sendWelcomeEmail(user));
});
```

### `workflow.cron(cronExpr: string, handler: HandlerFn)`

Run a workflow on a recurring cron schedule.

```ts
workflow.cron("0 8 * * *", async ({ step }) => {
  await step("daily-report", () => generateDailyReport());
});
```

### `workflow.every(duration: string, handler: HandlerFn)`

Run a workflow at a fixed interval (e.g. `"1h"`, `"5m"`).

```ts
workflow.every("1h", async ({ step }) => {
  await step("ping", () => heartbeat());
});
```

### `workflow.define(config: WorkflowConfig)`

Low-level, full API for defining workflows.

```ts
workflow.define({
  event: "user.signup",
  handle: async ({ event, step }) => {
    await step("fetch-user", () => getUser(event.userId));
  },
});
```

Supports these fields:

```ts
type WorkflowConfig =
  | { event: string; handle: HandlerFn; retries?: number; concurrency?: number }
  | { cron: string; handle: HandlerFn; retries?: number }
  | { every: string; handle: HandlerFn; retries?: number };
```

## Dispatching Events

### `workflow.dispatch(name: string, data: Record<string, unknown>)`

Triggers a workflow by event name.

```ts
await workflow.dispatch("user.signup", { userId: "abc123" });
```

## Step Functions

Each workflow handler receives a `step` object, which provides durable,
retryable execution.

### `await step(name: string, fn: () => Promise<T>): Promise<T>`

Runs a named unit of work. Result is persisted. Retries on failure. Can be
rehydrated after crash or cold start.

```ts
const user = await step("fetch-user", () => getUser(event.userId));
```

- **Steps must be named.**
- Step functions should be pure and idempotent.
- Result is automatically cached on success and reused on re-run.
- Each step is retried up to 4 additional times on failure (5 attempts total).
- You can prevent retry by throwing a `NonRetriableError`.
- You can customize retry timing by throwing a `RetryAfterError(dateOrMs)`.

### `await step.sleep(duration: string): Promise<void>`

Pause workflow execution for the given duration. Durable sleep — does not block
compute.

```ts
await step.sleep("5m");
```

## Retry Behavior

- Functions and steps are retried up to **4 times by default**, in addition to
  the first attempt (5 total).
- You can configure retries globally for the workflow via `retries` in
  `workflow.define()`.
- Use `attempt` in the handler context to alter behavior across retries:

```ts
workflow.define({
  event: "user.signup",
  retries: 10,
  handle: async ({ event, step, attempt }) => {
    if (attempt > 5) console.warn("This is getting excessive");
    await step("send-email", () => sendEmail(event.userId));
  },
});
```

- Throw `new NonRetriableError(reason)` to stop all retries.
- Throw `new RetryAfterError(reason, dateOrMs)` to retry at a specific time.

## Types

```ts
type HandlerFn = (ctx: {
  event?: { name: string; data: Record<string, unknown> };
  step: {
    <T>(name: string, fn: () => Promise<T>): Promise<T>;
    sleep(duration: string): Promise<void>;
  };
  attempt: number;
}) => Promise<void>;
```

## Design Philosophy

- **Minimal & composable**: Each feature builds on top of Cells' primitives.
- **Durable by default**: Steps persist state and retry automatically.
- **Familiar**: Inspired by Inngest, but streamlined for Deno Cells.

## Not Yet in MVP

- Fan-out
- Multi-event triggers
- Step output typing or validation
- UI or dashboard support
- Metadata (versioning, etc.)

## Example

```ts
workflow.on("newsletter.subscribe", async ({ event, step }) => {
  await step("store-subscriber", () => saveToDB(event.data));
  await step.sleep("1h");
  await step("send-followup", () => sendEmail(event.data.email));
});
```
