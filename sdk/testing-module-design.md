# Cells Testing SDK Design

This document describes the current design plan for the testing SDK of cells.

## Example 1

Suppose a user has a workflow definition that looks like this:

```ts
export const myWorkflow = cell.workflow.define<
  { username: string; email: string; phoneNumber: string },
  string
>({
  name: "my-workflow",
  handler: async ({ input, step, db }) => {
    await step.run("send-email", async () => {
      await sendEmail(input.email);

      // Save the email sent log to the database
      db.prepare(`INSERT INTO logs (text) VALUES (?)`).run(
        `${input.username} signup email sent to ${input.email}`,
      );

      return null;
    });

    await step.run("send-sms", async () => {
      await sendSms(input.phoneNumber);

      // Save the SMS sent log to the database
      db.prepare(`INSERT INTO logs (text) VALUES (?)`).run(
        `${input.username} signup SMS sent to ${input.phoneNumber}`,
      );

      return null;
    });

    return "finished";
  },
});
```

For this workflow definition, the user can test it with mocked steps, e.g.

```ts
import { myWorkflow } from "./my-workflow.ts";
import { testing } from "jsr:@ry/cells";

Deno.test("test my-workflow", async () => {
  const testEnv = testing.createTestWorkflowEnvironment();

  const mocker = testEnv.createMocker(myWorkflow);

  mocker.mock("send-email", () => {
    // Pretend email was sent successfully and save the log to the database
    testEnv.db.prepare(`INSERT INTO logs (text) VALUES (?)`).run(
      `${input.username} signup email sent to ${input.email}`,
    );

    return null;
  });

  mocker.mock("send-sms", () => {
    // Pretend SMS was sent successfully and save the log to the database
    testEnv.db.prepare(`INSERT INTO logs (text) VALUES (?)`).run(
      `${input.username} signup SMS sent to ${input.phoneNumber}`,
    );

    return null;
  });

  const runId = testEnv.dispatch(mocker, {
    username: "alice",
    email: "alice@example.com",
    phoneNumber: "+1-555-0100",
  });

  const result = await testEnv.waitUntilCompletion(runId);

  assertEquals(result, "finished");
});
```

## Example 2

Let's consider a more complicated example where there are two workflow
definitions and one of them is a parent workflow that invokes the other with
`step.invoke`.

```ts
export const parentWorkflow = cell.workflow.define<
  { value: number },
  { finalResult: number }
>({
  name: "parent",
  handler: async ({ input, step, db }) => {
    const result = await step.invoke(childWorkflow, { x: input.value });
    return { finalResult: result };
  },
});

export const childWorkflow = cell.workflow.define<
  { x: number },
  number
>({
  name: "child",
  handler: async ({ input, step, db }) => {
  },
});
```

## Possible features

- time jump (skipping `step.sleep`) for faster testing
- violation detection: making each step forcibly fail even if it's mocked and
  then retry the workflow run to make sure that any rule violation does not
  exist in workflow logic
