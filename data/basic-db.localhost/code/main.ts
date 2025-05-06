import { cell } from "jsr:@ry/cells";

console.log(`[${cell.id}] Initializing...`);

cell.db.exec(`
	CREATE TABLE IF NOT EXISTS requests (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		method TEXT,
		url TEXT,
		user_agent TEXT,
		timestamp TEXT DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_requests_timestamp ON requests (timestamp);
`);

cell.request((req: Request): Response => {
  const userAgent = req.headers.get("user-agent");
  const timestamp = new Date().toISOString();

  const insert = cell.db.prepare(
    "INSERT INTO requests (method, url, user_agent, timestamp) VALUES (?, ?, ?, ?)",
  );

  insert.run(req.method, req.url, userAgent, timestamp);

  const countRow = cell.db
    .prepare("SELECT COUNT(*) AS count FROM requests")
    .get() as { count: number };
  const count = countRow?.count;
  return new Response(String(count) + "\n", { status: 200 });
});
