import { cell, types } from "../../../jsr-cells/mod.ts";
import { delay } from "jsr:@std/async@1.0.13/delay";

cell.db.exec(`
  CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now', 'utc'))
  )
`);

type MyWorkflow = {
  "reliable": {
    username: string;
    email: string;
    phoneNumber: string;
  };
};
const workflow = cell.initWorkflow<MyWorkflow>();
workflow.define({
  name: "reliable",
  handler: async (ctx) => {
    await ctx.step.run("send-email", async () => {
      // Simulate a delay of sending email
      await delay(500);
      // Save the email sent log to the database
      cell.db.prepare(
        `
        INSERT INTO logs (text) VALUES (?)
        `,
      ).run(
        `${ctx.event.data.username} signup email sent to ${ctx.event.data.email}`,
      );
      return null;
    });

    await ctx.step.run("send-sms", async () => {
      // Simulate a delay of sending SMS
      await delay(800);
      // Save the SMS sent log to the database
      cell.db.prepare(
        `
        INSERT INTO logs (text) VALUES (?)
        `,
      ).run(
        `${ctx.event.data.username} signup SMS sent to ${ctx.event.data.phoneNumber}`,
      );
      return null;
    });
  },
});

cell.request(async (req: Request) => {
  const url = new URL(req.url);

  const lastPathSegment = url.pathname.split("/").at(-1);

  if (lastPathSegment === "logs" && req.method === "GET") {
    const logs = cell.db.prepare(`
      SELECT * FROM logs ORDER BY created_at DESC
    `).all();
    return Response.json(logs);
  }

  if (lastPathSegment === "reliable" && req.method === "POST") {
    const { username, email, phoneNumber } = await req.json();
    const runId = workflow.dispatch("reliable", {
      username,
      email,
      phoneNumber,
    });
    return new Response(runId);
  }

  if (lastPathSegment === "run-progress" && req.method === "GET") {
    const runId = url.searchParams.get("id");
    if (!runId) {
      return Response.json({ error: "id is required" }, { status: 400 });
    }
    const runProgress = workflow.getRunProgress(runId as types.WorkflowRunId);
    return Response.json(runProgress);
  }

  return new Response("Not found", { status: 404 });
});
