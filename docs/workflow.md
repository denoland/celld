# Cells Workflow API (MVP)

This module defines a minimal workflow runtime for use with the Cells runtime
(`jsr:@deno/cells`). Inspired by Inngest, it supports durable background
functions triggered by events or schedules.

Import:

```ts
import { cell, types } from "jsr:@deno/cells";
```

## Initialization

First, initialize the workflow instance with the type definition that defines
event names for event-triggered workflows and their input data types. For
example:

```ts
type UserWorkflows = {
  "user.signup": { userId: string };
  "newsletter.subscribe": { email: string } & Record<string, types.JSONValue>;
};

const workflow = cell.initWorkflow<UserWorkflows>();
```

The type argument here is used to provide better intellisense and type checking.
Currently there is no runtime validation.

Note that `initWorkflow` can be called only once. If you call it more than once,
it will throw an runtime error.

## Defining workflows

Next you need to define what each workflow does when it is triggered.

### `workflow.define(config: WorkflowConfig)`

This is the primary API for defining workflows. It supports configuration for
the following trigger types:

- event-triggered
- cron-scheduled (🚧 unsupported yet)
- interval-based (🚧 unsupported yet)

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
  | { every: string; h🚧andle: HandlerFn; retries?: number };
```

## Dispatching Events

### `workflow.dispatch(name: string, data: JSONValue): WorkflowRunId`

Triggers a workflow by event name. This is used for workflows defined with an
`event` trigger.

The second argument is is the input data for the triggered workflow, which can
be be referenced with `ctx.event.data` in the workflow handler. This value is
cached in DB and reused if the workflow is retried. Its concrete type is
inferred from what you provided in `cell.initWorkflow`:

```ts
type UserWorkflows = {
  "user.signup": { userId: string };
};

const workflow = cell.initWorkflow<UserWorkflows>();

// ✅ OK!
const runId = workflow.dispatch("user.signup", { userId: "abc123" });

// ❌ Doesn't pass type check because `userId` is not supplied
workflow.dispatch("user.signup", { unknownProperty: 42 });
```

`workflow.dispatch` returns the ID of the dispatched workflow run. The actual
workflow run executes in the background. You can use the returned ID to query
the progress.

## Querying Workflow Run Progress

### `workflow.getRunProgress(runId: WorkflowRunId): WorkflowRunProgress | null`

Returns the progress of a workflow run. If the run is not found, it returns
`null`.

```ts
const runId = workflow.dispatch("user.signup", { userId: "abc123" });

// Some time later...

const progress = workflow.getRunProgress(runId);
console.log(progress);
// {
//   id: "01JWQF48VFXJ5Q0BCRM03HA2XQ",
//   workflowName: "user.signup",
//   dispatchedAt: "2025-06-02T04:42:28.584Z",
//   completedAt: null,
//   steps: [
//     {
//       stepIndex: 0,
//       name: "fetch-user",
//       outputData: { id: "abc123", name: "Alice" },
//       completedAt: "2025-06-02T04:42:29.584Z",
//     }
//   ]
// }
```

## Step Functions

Each workflow handler receives a `step` object, which provides various tools for
orchestrating durable, retryable execution.

### MVP Step Methods:

- **`await step.run<T extends JSONValue>(name: string, fn: () => Promise<T>): Promise<T>`**

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
    - The execution order of steps must not be changed across re-runs. In
      particular, you should not put `step.run` inside `if-else`. Violating this
      rule would cause a cached result from a wrong workflow step to be used.
    - Each step is retried up to 4 additional times on failure (5 attempts total
      by default).
    - You can prevent retry by throwing a `NonRetriableError`.
    - You can customize retry timing by throwing a `RetryAfterError(dateOrMs)`.
      By default, the retry is scheduled in 1 second.

- **`await step.sleep(duration: string): Promise<void>`**

  - 🚧 Not implemented yet.
  - Sleep for a given amount of time (e.g., `"5m"`, `"1h"`).
  - This is a durable sleep — it does not block compute resources.
  - **Example:**
    ```ts
    await step.sleep("5m");
    ```

### Future Step Methods (Postponed from MVP):

- **`step.sleepUntil(dateTime: string | Date): Promise<void>`**

  - 🚧 Not implemented yet.
  - Sleep until a given specific time.
  - Similar to `step.sleep`, this would be a durable operation.

- **`step.invoke<T>(workflowId: string, input: unknown): Promise<T>`**

  - 🚧 Not implemented yet.
  - Invoke another Cells workflow (or potentially an Inngest function if
    interoperability is considered) as a step, receiving the result of the
    invoked function.
  - This would allow for composing workflows and reusing logic.

- **`step.waitForEvent<T>(eventName: string, options: { timeout: string; match?: Record<string, any> }): Promise<T | null>`**

  - 🚧 Not implemented yet.
  - Pause a function's execution until another specific event is received.
  - Useful for coordinating workflows based on external signals or user actions.

- **`step.sendEvent(eventName: string, data: Record<string, unknown>): Promise<void>`**

  - 🚧 Not implemented yet.
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
  to retry after a specific delay or at a certain time. By default, the retry is
  scheduled in 1 second.

## Suspension and Resumption

Ongoing workflow runs will be suspended when the cell is gracefully shut down.
In this case, resumption is automatically scheduled in 10 seconds, assuming the
cell is restarted within 10 seconds on another node (or it can be the same node,
if the reason of shutdown was eviction due to temporary resource shortage).

## Types

```ts
type HandlerFn = (ctx: {
  event?: { name: string; data: JSONValue }; // Present for 'event' triggered workflows
  step: {
    run<T extends JSONValue>(name: string, fn: () => Promise<T>): Promise<T>;
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

## TODOs

- [ ] Support cron-scheduled workflows
- [ ] Support interval-based workflows
- [ ] Support `retries` in event-triggered workflows
- [ ] Support `concurrency` in event-triggered workflows
- [ ] Rename `workflowName` to `eventName` in `WorkflowRunProgress` for
      consistency
- [ ] Add logging (automatically records what's happening in a workflow run,
      such as step failure, suspension due to cell shutdown, resume, retry,
      etc.)
- [ ] Track the retry count for each workflow step, and stop the retry after
      failing 5 times by default, or respecting the `retries` config in
      `workflow.define`
- [ ] Support `NonRetriableError`
- [ ] Support `RetryAfterError`
- [ ] Pass the correct value for `attempt` in workflow handler calls
- [ ] Tell the Rust side that the cell is not idle when there are running
      workflows to prevent idle shutdown
