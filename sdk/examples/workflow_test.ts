import { cell } from "../cell.ts";
import { assertEquals, assertExists } from "jsr:@std/assert";

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
    handler: async ({ input }) => {
      console.log("Multiply workflow called with:", input);
      const result = input.a * input.b;
      console.log("Multiply returning:", result);
      return result;
    },
  });

  // Define parent workflow that invokes child
  const calculateWorkflow = cell.workflow.define<
    { x: number },
    { doubled: number; tripled: number }
  >({
    name: "calculate",
    handler: async ({ input, step }) => {
      console.log("Calculate workflow started with input:", input);
      console.log("About to invoke multiply for doubled...");
      const doubled = await step.invoke(multiplyWorkflow, { a: input.x, b: 2 });
      console.log("Doubled result:", doubled);

      console.log("About to invoke multiply for tripled...");
      const tripled = await step.invoke(multiplyWorkflow, { a: input.x, b: 3 });
      console.log("Tripled result:", tripled);

      return { doubled, tripled };
    },
  });

  // Create test environment AFTER defining workflows
  using env = cell.workflow.createTestEnvironment();

  // Run the parent workflow
  console.log("Starting parent workflow...");
  const { runId, waitForCompletion } = await env.runWorkflow(
    calculateWorkflow,
    {
      x: 10,
    },
  );
  console.log("Parent workflow started with runId:", runId);

  const result = await waitForCompletion();

  // Verify the results
  assertEquals(result.doubled, 20);
  assertEquals(result.tripled, 30);
});
