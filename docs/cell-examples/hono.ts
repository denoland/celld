import { cell } from "jsr:@deno/cell"; // Use 'cell' import
import { Hono } from "hono";           // User brings their own Hono

console.log(`[${cell.id}] Hono-based cell starting.`);
cell.db.exec(
	`CREATE TABLE IF NOT EXISTS kvs (key TEXT PRIMARY KEY, value TEXT)`
);

cell.alarm(() => {
  console.log(`[${cell.id}] Scheduled alarm triggered!`);
  cell.broadcast({ type: "alarm", message: "Periodic check-in!" });
});

cell.connect((ws) => {
  console.log(`[${cell.id}] WS client connected.`);
  ws.send(JSON.stringify({ type: "welcome", id: cell.id }));
});

cell.message((msg, ws) => {
  console.log(`[${cell.id}] Message from WS:`, msg);
  cell.broadcast({ type: "message", data: msg });
});

const app = new Hono();

// GET http://my-host/cell/foo
app.get('/', (c) => {
  const message = `Hello from Hono inside cell ${cell.id ?? 'unknown!'}`;
  console.log(`[${cell.id ?? 'unknown'}] Hono GET / handler`);
  return c.text(message);
});

cell.request(app.fetch);
