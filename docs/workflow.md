# Cells Workflow API (MVP)

This module defines a minimal workflow runtime for use with the Cells runtime
(`jsr:@ry/cells`). Inspired by Inngest, it supports durable background functions
triggered by events or schedules.

Import:

```ts
import { cell } from "jsr:@ry/cells";
```

## Defining Workflows

Use the `cell.workflow.define` function to create type-safe workflow
definitions.

### Simple Workflows (No Input)

```ts
const dailyCleanup = cell.workflow.define({
  name: "daily.cleanup",
  handler: async ({ step }) => {
    await step.run("delete-temp-files", () => deleteTempFiles());
    await step.run("optimize-database", () => optimizeDB());
  },
});

// Dispatch without arguments
cell.workflow.dispatch(dailyCleanup);
```

### Workflows with Input

```ts
const processOrder = cell.workflow.define<{
  orderId: string;
  items: Array<{ productId: string; quantity: number }>;
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

## Step Functions

Steps are the building blocks of durable execution. Each step is memoized in the
Cell's database.

### `step.run(name, fn)`

Execute code as a durable step:

```ts
const result = await step.run("fetch-user", async () => {
  const user = await fetch(`/api/users/${userId}`);
  return user.json();
});
```

- Results are cached and reused on retry
- Steps must have unique, stable names
- Functions should be idempotent

### `step.invoke(workflow, input?)`

Invoke another workflow as a step:

```ts
const emailWorkflow = cell.workflow.define<{ to: string; subject: string }>({
  name: "send.email",
  handler: async ({ input }) => {
    // Send email logic
    return { messageId: "msg_123" };
  },
});

const mainWorkflow = cell.workflow.define({
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

### `step.sleep(name, durationMs)`

Pause workflow execution for a specified duration:

```ts
const reminderWorkflow = define<{ message: string }, null>({
  name: "send.reminder",
  handler: async ({ input, step }) => {
    await step.run("send-initial", () => sendEmail(input.message));

    // Wait 24 hours before sending reminder
    await step.sleep("wait-24h", 24 * 60 * 60 * 1000);

    await step.run(
      "send-reminder",
      () => sendEmail(`Reminder: ${input.message}`),
    );
  },
});
```

Note: During sleep, the cell may shut down to save resources. The workflow will
resume when the sleep duration expires, even if running on a different node.

## Monitoring Workflows

### Get Run Progress

```ts
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

### List All Runs

```ts
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

## Architecture & Isolation

Workflows leverage Cells' architecture for reliability:

1. **Single Isolate**: All workflows in a Cell run in its single V8 isolate
2. **SQLite Persistence**: Step results and workflow state are stored in the
   Cell's SQLite database
3. **Automatic Recovery**: When a Cell restarts, pending workflows resume from
   their last completed step
4. **Cell Isolation**: Workflows cannot access or interfere with other Cells

This design ensures workflows are both durable and secure, with complete data
isolation between Cells.

## Complete Example

```ts
// Email workflow (reusable)
const sendEmail = cell.workflow.define<{
  to: string;
  subject: string;
  template: string;
  data: Record<string, any>;
}>({
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
