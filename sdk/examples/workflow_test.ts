import { cell } from "../cell.ts";
import { assert, assertEquals, assertExists } from "jsr:@std/assert";

// Example workflow definition
const emailWorkflow = cell.workflow.define<
  { email: string; subject: string },
  { messageId: string }
>({
  name: "send-email",
  handler: async ({ input, step }) => {
    const messageId = await step.run("send-email", () => {
      // In production, this would actually send an email
      console.log(`Sending email to ${input.email}: ${input.subject}`);
      return `msg-${crypto.randomUUID()}`;
    });

    await step.run("log-email", () => {
      console.log(`Email sent with ID: ${messageId}`);
      return null;
    });

    return { messageId };
  },
});

const userWorkflow = cell.workflow.define<
  { email: string },
  { userId: number }
>({
  name: "create-user",
  handler: async ({ input, step, db }) => {
    const userId = await step.run("insert-user", () => {
      const result = db.prepare(
        "INSERT INTO users (email) VALUES (?) RETURNING id",
      ).get(input.email) as { id: number };
      return result.id;
    });

    return { userId };
  },
});

// Test using Deno.test
Deno.test("emailWorkflow sends email and logs result", async () => {
  using env = cell.workflow.createTestEnvironment();

  // Mock the email sending step
  env.mockStep("send-email", () => "msg-12345");

  // Mock the logging step
  let loggedMessageId: string | null = null;
  env.mockStep("log-email", () => {
    loggedMessageId = "msg-12345";
    return null;
  });

  // Run the workflow
  const { waitForCompletion } = await env.runWorkflow(emailWorkflow, {
    email: "test@example.com",
    subject: "Test Email",
  });

  const result = await waitForCompletion();

  // Verify the result
  assertEquals(result.messageId, "msg-12345");
  assertEquals(loggedMessageId, "msg-12345");
});

// Test with database access
Deno.test("workflow with database operations", async () => {
  using env = cell.workflow.createTestEnvironment();

  // Initialize database tables
  env.db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT NOT NULL,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Run the workflow
  const { waitForCompletion } = await env.runWorkflow(userWorkflow, {
    email: "newuser@example.com",
  });

  const result = await waitForCompletion();

  // Verify the user was created
  const user = env.db.prepare("SELECT * FROM users WHERE id = ?").get(
    result.userId,
  );
  assertExists(user);
});

// Test workflow invocation
Deno.test("workflow can invoke another workflow", async () => {
  // Define child workflow BEFORE creating test environment
  const multiplyWorkflow = cell.workflow.define<
    { a: number; b: number },
    number
  >({
    name: "multiply",
    handler: ({ input }) => {
      return input.a * input.b;
    },
  });

  // Define parent workflow that invokes child
  const calculateWorkflow = cell.workflow.define<
    { x: number },
    { doubled: number; tripled: number }
  >({
    name: "calculate",
    handler: async ({ input, step }) => {
      const doubled = await step.invoke(multiplyWorkflow, { a: input.x, b: 2 });
      const tripled = await step.invoke(multiplyWorkflow, { a: input.x, b: 3 });
      return { doubled, tripled };
    },
  });

  // Create test environment AFTER defining workflows
  using env = cell.workflow.createTestEnvironment();

  // Run the parent workflow
  const { waitForCompletion } = await env.runWorkflow(calculateWorkflow, {
    x: 10,
  });

  const result = await waitForCompletion();

  // Verify the results
  assertEquals(result.doubled, 20);
  assertEquals(result.tripled, 30);
});

// Test workflow with sleep
Deno.test("workflow with sleep step", async () => {
  // Define a workflow that uses sleep
  const delayedProcessingWorkflow = cell.workflow.define<
    { message: string; delayMs: number },
    { processedMessage: string; duration: number }
  >({
    name: "delayed-processing",
    handler: async ({ input, step }) => {
      // Record when we started
      const startTime = await step.run("record-start", () => {
        return Date.now();
      });

      // Sleep for the specified duration
      await step.sleep("wait-before-processing", input.delayMs);

      // Process the message after the delay
      const result = await step.run("process-message", () => {
        const endTime = Date.now();
        const duration = endTime - startTime;
        const processedMessage = `Processed: ${input.message.toUpperCase()}`;
        return { processedMessage, duration };
      });

      return result;
    },
  });

  using env = cell.workflow.createTestEnvironment();

  // Run the workflow with a short sleep
  const { waitForCompletion } = await env.runWorkflow(
    delayedProcessingWorkflow,
    {
      message: "hello world",
      delayMs: 100, // Sleep for 100ms
    },
  );

  const result = await waitForCompletion();

  // Verify the results
  assertEquals(result.processedMessage, "Processed: HELLO WORLD");
  // Verify that the sleep actually happened (duration should be at least the sleep time)
  assert(
    result.duration >= 100,
    `Expected duration >= 100ms, got ${result.duration}ms`,
  );
});
