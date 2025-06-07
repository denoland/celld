# Cells Workflow API

The Cells Workflow API provides **durable execution** for long-running, reliable
background processes. Workflows are private to each Cell—they run in the Cell's
single isolate and persist state in its SQLite database, ensuring complete
isolation between Cells.

## Key Concepts

**Durable execution** means your workflows survive crashes, restarts, and
failures:

- **Step memoization**: Each step's result is persisted to the Cell's SQLite
  database
- **Automatic recovery**: Workflows resume from their last completed step after
  crashes
- **Workflow composition**: Workflows can invoke other workflows within the same
  Cell
- **Exactly-once semantics**: Steps are idempotent and results are cached

Import:

```ts
import { define, dispatch } from "jsr:@deno/cells/workflow";
```

## Defining Workflows

Use the `define` function to create type-safe workflow definitions.

### Simple Workflows (No Input)

```ts
const dailyCleanup = define({
  event: "daily.cleanup",
  handler: async ({ step }) => {
    await step.run("delete-temp-files", () => deleteTempFiles());
    await step.run("optimize-database", () => optimizeDB());
  },
});

// Dispatch without arguments
dispatch(dailyCleanup);
```

### Workflows with Input

```ts
const processOrder = define<{
  orderId: string;
  items: Array<{ productId: string; quantity: number }>;
}>({
  event: "order.process",
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
dispatch(processOrder, {
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
const emailWorkflow = define<{ to: string; subject: string }>({
  event: "send.email",
  handler: async ({ input }) => {
    // Send email logic
    return { messageId: "msg_123" };
  },
});

const mainWorkflow = define({
  event: "user.onboard",
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

## Monitoring Workflows

### Get Run Progress

```ts
const runId = dispatch(myWorkflow, { data: "example" });

// Check progress
const progress = getRunProgress(runId);
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
const allRuns = listRuns();

// Filter options
const pendingRuns = listRuns({ status: "pending" });
const recentSignups = listRuns({
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
const sendEmail = define<{
  to: string;
  subject: string;
  template: string;
  data: Record<string, any>;
}>({
  event: "email.send",
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
const userSignup = define<{
  email: string;
  name: string;
}>({
  event: "user.signup",
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
const runId = dispatch(userSignup, {
  email: "alice@example.com",
  name: "Alice",
});
```

## API Reference

### Types

```ts
// Workflow context - conditionally includes input
type WorkflowCtx<TInput = void> = {
  step: WorkflowStep;
  attempt: number;
} & (TInput extends void ? {} : { input: TInput });

// Configuration
type EventWorkflowConfig<Input = void, Output = any> = {
  event: string;
  handler: (ctx: WorkflowCtx<Input>) => Promise<Output>;
  retries?: number; // 🚧 Coming soon
  concurrency?: number; // 🚧 Coming soon
};
```

### Functions

- `define(config)` - Define a workflow
- `define<Input>(config)` - Define a workflow with typed input
- `dispatch(workflow)` - Run a void workflow
- `dispatch(workflow, input)` - Run a workflow with input
- `getRunProgress(runId)` - Get workflow run status
- `listRuns(options?)` - List workflow runs with filtering

## Coming Soon

- [ ] Cron-scheduled workflows
- [ ] Configurable retries and concurrency limits
- [ ] `step.sleep()` and `step.sleepUntil()` for durable delays
- [ ] `step.waitForEvent()` for event coordination
- [ ] `NonRetriableError` and `RetryAfterError` for retry control
