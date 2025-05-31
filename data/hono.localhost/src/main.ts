import { cell } from "../../../jsr-cells/mod.ts";
import { Hono } from "npm:hono@4.7.10";

cell.db.exec(`
  CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    method TEXT,
    path TEXT,
    user_agent TEXT,
    status INTEGER,
    timestamp TEXT DEFAULT CURRENT_TIMESTAMP
  );
  CREATE INDEX IF NOT EXISTS idx_requests_timestamp ON requests (timestamp);
`);

cell.db.exec(`
  CREATE TABLE IF NOT EXISTS errors (
    error TEXT NOT NULL,
    timestamp TEXT DEFAULT CURRENT_TIMESTAMP
  );
  CREATE INDEX IF NOT EXISTS idx_errors_timestamp ON errors (timestamp);
`);

const app = new Hono();

app.use("*", async (c, next) => {
  await next();
  cell.db.prepare(
    "INSERT INTO requests (method, path, user_agent, status) VALUES (?, ?, ?, ?)",
  ).run(
    c.req.method,
    c.req.path,
    c.req.header("user-agent") ?? "",
    c.res.status,
  );
});

app.get("/", (c) => c.text("hello from hono"));

app.get("/logs", (c) => {
  const logs = cell.db
    .prepare(
      "SELECT method, path, user_agent, status, timestamp FROM requests ORDER BY timestamp DESC",
    )
    .all();
  return c.json(logs);
});

app.get("/boom", () => {
  throw new Error("Boom");
});

app.get("/errors", (c) => {
  const errors = cell.db
    .prepare("SELECT error FROM errors ORDER BY timestamp DESC")
    .all();
  return c.json(errors);
});

app.onError((err, c) => {
  cell.db.prepare("INSERT INTO errors (error) VALUES (?)").run(
    Deno.inspect(err),
  );
  return c.text("Internal Server Error from hono", 500);
});

app.notFound((c) => c.text(`Not Found from hono`, 404));

cell.request(app.fetch);
