# Cells Workflow API (MVP)

This module defines a minimal workflow runtime for use with the Cells runtime
(`jsr:@deno/cells`). Inspired by Inngest, it supports durable background
functions triggered by events or schedules.

Import:

```ts
import * as workflow from "jsr:@deno/cells/workflow";
```

## Defining workflows

### `workflow.define(config: WorkflowConfig)`

This is the primary API for defining workflows. It supports configuration for
event-triggered, cron-scheduled, or interval-based workflows.

```ts
// Event-triggered workflow
workflow.define({
  event: "user.signup",
  handle: async ({ event, step }) => {
    const user = await step.run("fetch-user", () => getUser(event.userId));
    await step.run("send-email", () => sendWelcomeEmail(user));
  },
});

// Cron-scheduled workflow
workflow.define({
  cron: "0 8 * * *", // Runs daily at 8 AM
  handle: async ({ step }) => {
    await step.run("daily-report", () => generateDailyReport());
  },
});

// Interval-based workflow
workflow.define({
  every: "1h", // Runs every hour
  handle: async ({ step }) => {
    await step.run("ping", () => heartbeat());
  },
});
```

The `WorkflowConfig` object supports the following fields:

```ts
type WorkflowConfig =
  | { event: string; handle: HandlerFn; retries?: number; concurrency?: number }
  | { cron: string; handle: HandlerFn; retries?: number }
  | { every: string; handle: HandlerFn; retries?: number };
```

## Dispatching Events

### `workflow.dispatch(name: string, data: Record<string, unknown>)`

Triggers a workflow by event name. This is used for workflows defined with an
`event` trigger.

```ts
await workflow.dispatch("user.signup", { userId: "abc123" });
```

## Step Functions

Each workflow handler receives a `step` object, which provides various tools for
orchestrating durable, retryable execution.

### MVP Step Methods:

- **`await step.run<T>(name: string, fn: () => Promise<T>): Promise<T>`**

  - Run synchronous or asynchronous code as a retriable step in your function.
  - The result is persisted. Retries on failure. Can be rehydrated after a crash
    or cold start.
  - **Example:**
    ```ts
    const user = await step.run("fetch-user", () => getUser(event.userId));
    ```
  - **Details:**
    - Steps must be named.
    - Step functions should be pure and idempotent for predictable recovery.
    - The result is automatically cached on success and reused on re-run if the
      workflow re-executes.
    - Each step is retried up to 4 additional times on failure (5 attempts total
      by default).
    - You can prevent retry by throwing a `NonRetriableError`.
    - You can customize retry timing by throwing a `RetryAfterError(dateOrMs)`.

- **`await step.sleep(duration: string): Promise<void>`**

  - Sleep for a given amount of time (e.g., `"5m"`, `"1h"`).
  - This is a durable sleep — it does not block compute resources.
  - **Example:**
    ```ts
    await step.sleep("5m");
    ```

### Future Step Methods (Postponed from MVP):

- **`step.sleepUntil(dateTime: string | Date): Promise<void>`**

  - Sleep until a given specific time.
  - Similar to `step.sleep`, this would be a durable operation.

- **`step.invoke<T>(workflowId: string, input: unknown): Promise<T>`**

  - Invoke another Cells workflow (or potentially an Inngest function if
    interoperability is considered) as a step, receiving the result of the
    invoked function.
  - This would allow for composing workflows and reusing logic.

- **`step.waitForEvent<T>(eventName: string, options: { timeout: string; match?: Record<string, any> }): Promise<T | null>`**

  - Pause a function's execution until another specific event is received.
  - Useful for coordinating workflows based on external signals or user actions.

- **`step.sendEvent(eventName: string, data: Record<string, unknown>): Promise<void>`**

  - Send event(s) reliably from within your function.
  - This would be the preferred way to dispatch events from within a running
    workflow to ensure they are sent durably and integrate with the workflow's
    execution guarantees, potentially differing from the global
    `workflow.dispatch` which is for external triggers.

## Retry Behavior

- Functions and steps are retried up to **4 times by default**, in addition to
  the first attempt (5 total).
- You can configure retries globally for the workflow via `retries` in
  `workflow.define()`.
- Use `attempt` in the handler context to alter behavior across retries:

<!-- end list -->

```ts
workflow.define({
  event: "user.signup",
  retries: 10,
  handle: async ({ event, step, attempt }) => {
    if (attempt > 5) console.warn("This is getting excessive");
    await step.run("send-email", () => sendEmail(event.userId));
  },
});
```

- Throw `new NonRetriableError(reason)` to stop all retries for a step or
  workflow.
- Throw `new RetryAfterError(reason, dateOrMs)` to instruct a step or workflow
  to retry after a specific delay or at a certain time.

## Types

```ts
type HandlerFn = (ctx: {
  event?: { name: string; data: Record<string, unknown> }; // Present for 'event' triggered workflows
  step: {
    run<T>(name: string, fn: () => Promise<T>): Promise<T>;
    sleep(duration: string): Promise<void>;
    // Future step methods like sleepUntil, invoke, waitForEvent, sendEvent would be added here
  };
  attempt: number; // Current attempt number for the workflow execution
}) => Promise<void>;
```

## Design Philosophy

- **Minimal & composable**: Each feature builds on top of Cells' primitives. For
  MVP, we focus on the most critical step functionalities.
- **Durable by default**: Steps persist state and retry automatically.
- **Familiar**: Inspired by Inngest, but streamlined for Deno Cells.

## Not Yet in MVP (Future Considerations)

Beyond the postponed step methods mentioned above, other features for future
consideration include:

- Fan-out / Fan-in patterns for parallel execution.
- More sophisticated step output typing or validation.
- UI or dashboard support for monitoring and managing workflows.
- Workflow versioning and metadata.
- Helper functions for defining workflows (e.g., `workflow.on`, `workflow.cron`)
  as ergonomic sugar over `workflow.define`.

## Example

This example demonstrates a workflow triggered by a "newsletter.subscribe"
event, using the MVP step methods.

```ts
workflow.define({
  event: "newsletter.subscribe",
  handle: async ({ event, step }) => {
    // Assuming event.data contains subscriber information e.g., { email: "..." }
    await step.run("store-subscriber", () => saveToDB(event.data));
    await step.sleep("1h"); // Wait for an hour
    await step.run(
      "send-followup",
      () => sendEmail(event.data.email, "Welcome Follow-up!"),
    );
  },
});
```
