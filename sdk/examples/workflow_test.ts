import { TestEnvironment } from "../testing.ts";
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
  using env = new TestEnvironment();

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
  using env = new TestEnvironment();

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
