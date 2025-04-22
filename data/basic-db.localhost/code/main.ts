export default {
  onStart(ctx) {
    ctx.db.exec(`
      CREATE TABLE IF NOT EXISTS requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        method TEXT,
        url TEXT,
        user_agent TEXT,
        timestamp TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_requests_timestamp ON requests (timestamp);
    `);
  },

  async onRequest(req: Request, ctx: { room: Room }): Promise<Response> {
    const userAgent = req.headers.get("user-agent");
    const timestamp = new Date().toISOString();

    const insert = ctx.db.prepare("INSERT INTO requests (method, url, user_agent, timestamp) VALUES (?, ?, ?, ?)");
    insert.run(req.method, req.url, userAgent, timestamp);

    const countRow = ctx.db
      .prepare("SELECT COUNT(*) AS count FROM requests")
      .get();
    const count = countRow?.count;

    return new Response(String(count), { status: 200 });
  },
};
