import { cell } from "../../../sdk/mod.ts";

cell.db.exec(`
  CREATE TABLE IF NOT EXISTS alarm_count (
    cell_id TEXT NOT NULL PRIMARY KEY,
    count INTEGER NOT NULL
  )
`);

cell.request(async (req: Request) => {
  switch (req.method) {
    case "GET": {
      const row = cell.db.prepare(`
        SELECT count FROM alarm_count WHERE cell_id = ?
      `).get(cell.id) as { count?: number };
      return Response.json({ count: row?.count ?? 0 });
    }
    case "POST": {
      // Start a new alarm, which will recursively trigger another alarm
      const id = await cell.setAlarm(Date.now() + 1000);
      return new Response(id);
    }
    default: {
      return Response.json({ error: "Method not allowed" }, {
        status: 405,
      });
    }
  }
});

// This alarm will trigger itself recursively at 1 second intervals.
cell.alarm(async () => {
  cell.db.prepare(`
    INSERT INTO alarm_count (cell_id, count) VALUES (?, 1)
    ON CONFLICT(cell_id) DO UPDATE SET count = count + 1
  `)
    .run(cell.id);

  await cell.setAlarm(Date.now() + 1000);
});
