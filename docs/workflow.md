# Cells Workflow API (MVP)

This module defines a minimal workflow runtime for use with the Cells runtime
(`jsr:@deno/cells`). Inspired by Inngest, it supports durable background
functions triggered by events or schedules.

Import:

```ts
import { cell, define, dispatch } from "jsr:@deno/cells";
```

## Workflow Definitions

Workflows are defined using the `define` function, which creates a type-safe
workflow definition. Each workflow has an event name and a handler function that
processes the input data.

### `define(config: EventWorkflowConfig)`

This is the primary API for defining workflows. Currently, only event-triggered
workflows are supported.

```ts
// Event-triggered workflow
const userSignup = define({
  event: "user.signup",
  handler: async (input: { userId: string }, { step }) => {
    const user = await step.run("fetch-user", () => getUser(input.userId));
    await step.run("send-email", () => sendWelcomeEmail(user));
    return { success: true, userId: input.userId };
  },
});

// Workflow with typed input and output
const processOrder = define({
  event: "order.process",
  handler: async (input: { orderId: string; amount: number }, { step }) => {
    const payment = await step.run(
      "process-payment",
      () => chargeCard(input.amount),
    );
    const shipment = await step.run(
      "ship-order",
      () => shipOrder(input.orderId),
    );
    return { paymentId: payment.id, trackingNumber: shipment.tracking };
  },
});
```

The `EventWorkflowConfig` object supports the following fields:

```ts
type EventWorkflowConfig<Input = any, Output = any> = {
  event: string;
  handler: (input: Input, ctx: WorkflowCtx) => Promise<Output>;
  retries?: number; // 🚧 Not implemented yet
  concurrency?: number; // 🚧 Not implemented yet
};
```

## Dispatching Workflows

### `dispatch<W>(workflow: W, input: WorkflowInput<W>): WorkflowRunId`

Triggers a workflow with type-safe input validation. The input type is
automatically inferred from the workflow definition.

```ts
const userSignup = define({
  event: "user.signup",
  handler: async (input: { userId: string; email: string }) => {
    // Handle signup logic
    return { success: true };
  },
});

// ✅ Type-safe dispatch
const runId = dispatch(userSignup, {
  userId: "abc123",
  email: "user@example.com",
});

// ❌ TypeScript error - missing required fields
// dispatch(userSignup, { userId: "abc123" }); // Error: missing 'email'

// ❌ TypeScript error - wrong field type
// dispatch(userSignup, { userId: 123, email: "user@example.com" }); // Error: userId should be string
```

`dispatch` returns the ID of the dispatched workflow run. The actual workflow
executes in the background. You can use the returned ID to query the progress.

## Querying Workflow Run Progress

### `getRunProgress(runId: WorkflowRunId): WorkflowRunProgress | null`

Returns the progress of a workflow run. If the run is not found, it returns
`null`.

```ts
import { getRunProgress } from "jsr:@deno/cells";

const runId = dispatch(userSignup, {
  userId: "abc123",
  email: "user@example.com",
});

// Some time later...

const progress = getRunProgress(runId);
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

### `listRuns(options?: ListRunsOptions): WorkflowRunProgress[]`

Returns a list of workflow runs with optional filtering. Useful for building
workflow management dashboards.

```ts
import { listRuns } from "jsr:@deno/cells";

// Get all workflow runs
const allRuns = listRuns();

// Filter by workflow name
const signupRuns = listRuns({ workflowName: "user.signup" });

// Filter by completion status
const pendingRuns = listRuns({ status: "pending" });
const completedRuns = listRuns({ status: "completed" });

// Limit results
const recentRuns = listRuns({ limit: 10 });

// Combine filters
const recentCompletedSignups = listRuns({
  workflowName: "user.signup",
  status: "completed",
  limit: 5,
});
```

**Options:**

- `workflowName?: string` - Filter by specific workflow event name
- `status?: "pending" | "completed"` - Filter by completion status
- `limit?: number` - Maximum number of results to return

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
    const user = await step.run("fetch-user", () => getUser(input.userId));
    ```

- **`await step.invoke<W>(workflow: W, input: WorkflowInput<W>): Promise<WorkflowOutput<W>>`**

  - Invoke another workflow as a step, receiving the result of the invoked
    workflow.
  - This allows for composing workflows and reusing logic.
  - The invocation is durable - if the parent workflow crashes, it will resume
    and wait for the child workflow to complete.
  - **Example:**
    ```ts
    const emailWorkflow = define({
      event: "send.email",
      handler: async (input: { email: string; subject: string }) => {
        // Send email logic
        return { messageId: "msg123" };
      },
    });

    const signupWorkflow = define({
      event: "user.signup",
      handler: async (input: { userId: string; email: string }, { step }) => {
        const result = await step.invoke(emailWorkflow, {
          email: input.email,
          subject: "Welcome!",
        });
        return { success: true, emailMessageId: result.messageId };
      },
    });
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
const userSignup = define({
  event: "user.signup",
  retries: 10, // 🚧 Not implemented yet
  handler: async (input: { userId: string }, { step, attempt }) => {
    if (attempt > 5) console.warn("This is getting excessive");
    await step.run("send-email", () => sendEmail(input.userId));
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
type WorkflowCtx = {
  step: {
    run<T extends JSONValue>(name: string, fn: () => Promise<T>): Promise<T>;
    invoke<W extends WorkflowDef<WorkflowConfig>>(
      workflow: W,
      input: WorkflowInput<W>,
    ): Promise<WorkflowOutput<W>>;
    // Future step methods like sleep, sleepUntil, waitForEvent, sendEvent
  };
  attempt: number; // Current attempt number for the workflow execution
};

type EventWorkflowConfig<Input = any, Output = any> = {
  event: string;
  handler: (input: Input, ctx: WorkflowCtx) => Promise<Output>;
  retries?: number;
  concurrency?: number;
};
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
const newsletterSubscribe = define({
  event: "newsletter.subscribe",
  handler: async (input: { email: string; name: string }, { step }) => {
    await step.run("store-subscriber", () => saveToDB(input));
    // await step.sleep("1h"); // 🚧 Not implemented yet - Wait for an hour
    await step.run(
      "send-followup",
      () => sendEmail(input.email, "Welcome Follow-up!"),
    );
    return { subscribed: true };
  },
});

// Dispatch the workflow
const runId = dispatch(newsletterSubscribe, {
  email: "user@example.com",
  name: "John Doe",
});
```

## TODOs

- [ ] Support cron-scheduled workflows
- [ ] Support interval-based workflows
- [ ] Support `retries` in event-triggered workflows
- [ ] Support `concurrency` in event-triggered workflows
- [ ] Add `step.sleep()` and `step.sleepUntil()` for durable delays
- [ ] Add `step.waitForEvent()` for event coordination
- [ ] Add `step.sendEvent()` for reliable event dispatch from workflows
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
