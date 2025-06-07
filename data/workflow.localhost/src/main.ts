import {
  cell,
  define,
  dispatch,
  getRunProgress,
  types,
} from "../../../sdk/mod.ts";
import { delay } from "jsr:@std/async@1.0.13/delay";
import { randomIntegerBetween } from "jsr:@std/random@0.1.1";

cell.db.exec(`
  CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'utc'))
  )
`);
cell.db.exec(`
  CREATE TABLE IF NOT EXISTS key_values (
    key TEXT PRIMARY KEY NOT NULL,
    value INTEGER NOT NULL
  )
`);

const reliableWorkflow = define<
  { username: string; email: string; phoneNumber: string }
>({
  event: "reliable",
  handler: async ({ input, step }) => {
    await step.run("send-email", async () => {
      // Simulate a delay of sending email
      await delay(500);
      // Save the email sent log to the database
      cell.db.prepare(`INSERT INTO logs (text) VALUES (?)`).run(
        `${input.username} signup email sent to ${input.email}`,
      );
      return null;
    });

    await step.run("send-sms", async () => {
      // Simulate a delay of sending SMS
      await delay(800);
      // Save the SMS sent log to the database
      cell.db.prepare(`INSERT INTO logs (text) VALUES (?)`).run(
        `${input.username} signup SMS sent to ${input.phoneNumber}`,
      );
      return null;
    });
  },
});

// Generate a random number in the first step, and then the second step fails
// until an entry with key "flaky" is present in `key_values` table. Finally,
// the last step multiplies the random number by 2.
// This aims to verify the result memoization in retried workflow runs.
const flakyWorkflow = define({
  event: "flaky",
  handler: async ({ step }) => {
    const randomNumber = await step.run(
      "generate-random-number",
      async () => {
        await delay(500);
        const num = randomIntegerBetween(0, 1_000_000);
        return num;
      },
    );

    await step.run("throws-until-flaky-key-is-set", async () => {
      const flakyToggle = cell.db.prepare(
        `SELECT value FROM key_values WHERE key = 'flaky'`,
      ).get();
      if (flakyToggle === undefined) {
        throw new Error("flaky key is not set");
      }
      return null;
    });

    await step.run("multiply-random-number-by-2", async () => {
      return randomNumber * 2;
    });
  },
});

cell.request(async (req: Request) => {
  const url = new URL(req.url);

  const lastPathSegment = url.pathname.split("/").at(-1);

  if (lastPathSegment === "logs" && req.method === "GET") {
    const logs = cell.db.prepare(`SELECT * FROM logs ORDER BY created_at DESC`)
      .all();
    return Response.json(logs);
  }

  if (lastPathSegment === "kv" && req.method === "GET") {
    const key = url.searchParams.get("key");
    if (!key) {
      return Response.json({ error: "key is required" }, { status: 400 });
    }
    const value = cell.db.prepare(`SELECT value FROM key_values WHERE key = ?`)
      .get(key);
    return Response.json(value);
  }

  if (lastPathSegment === "kv" && req.method === "POST") {
    const { key, value } = await req.json();
    cell.db.prepare(`INSERT INTO key_values (key, value) VALUES (?, ?)`).run(
      key,
      value,
    );
    return new Response("OK");
  }

  if (lastPathSegment === "reliable" && req.method === "POST") {
    const { username, email, phoneNumber } = await req.json();
    const runId = dispatch(reliableWorkflow, {
      username,
      email,
      phoneNumber,
    });
    return new Response(runId);
  }

  if (lastPathSegment === "flaky" && req.method === "POST") {
    const runId = dispatch(flakyWorkflow);
    return new Response(runId);
  }

  if (lastPathSegment === "run-progress" && req.method === "GET") {
    const runId = url.searchParams.get("id");
    if (!runId) {
      return Response.json({ error: "id is required" }, { status: 400 });
    }
    const runProgress = getRunProgress(runId as types.WorkflowRunId);
    return Response.json(runProgress);
  }

  return new Response("Not found", { status: 404 });
});
