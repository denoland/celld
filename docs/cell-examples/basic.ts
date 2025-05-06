import { cell } from "jsr:@deno/cells";

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

cell.request((req: Request): Promise<Response> => {
  const userAgent = req.headers.get("user-agent") ?? "unknown";

  // Access cell.id implicitly
  console.log(`[${cell.id}] Received request: ${req.method} ${req.url}`);

  // Use cell.db implicitly to insert request data
  // Assuming cell.db has methods like run(), get(), all(), exec() etc.
  try {
    cell.db.run(
      "INSERT INTO requests (method, url, user_agent) VALUES (?, ?, ?)",
      [req.method, req.url, userAgent]
    );

    // Use cell.db implicitly to get the total count
    const countResult = cell.db.get<{ count: number }>(
      "SELECT COUNT(*) AS count FROM requests"
    );
    const count = countResult?.count ?? 0;
    console.log(`[${cell.id}] Total requests recorded: ${count}`);
    return new Response(`${count}\n`);

  } catch (error) {
    // Access cell.id implicitly
    console.error(`[${cell.id}] Database operation failed:`, error);
    return new Response("Internal Server Error", { status: 500 });
  }
});


cell.connect((ws) => {
  // handle websockets connection
});

cell.message((msg, ws) => {
  // handle a websocket message
}):
