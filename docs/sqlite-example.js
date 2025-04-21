// ctx.db is an instance of DatabaseSync from node:sqlite3
export default {
  async onConnect(ws, ctx) {
    // Ensure the messages table exists
    ctx.db.exec(`
      CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY,
        room_id TEXT,
        text TEXT,
        created_at TEXT
      )
    `);
  },

  async onMessage(ws, text, ctx) {
    const timestamp = new Date().toISOString();

    ctx.db.exec(
      "INSERT INTO messages (room_id, text, created_at) VALUES (?, ?, ?)",
      [ctx.roomId, text, timestamp]
    );

    const history = ctx.db.query(
      "SELECT text, created_at FROM messages WHERE room_id = ? ORDER BY id DESC LIMIT 10",
      [ctx.roomId]
    );

    ws.send(JSON.stringify({ type: "history", messages: history }));
  },
};

