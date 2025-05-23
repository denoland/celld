import { cell } from "../../../jsr-cells/mod.ts";

cell.db.exec(`CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v ANY)`);

cell.request((req: Request): Response => {
  return new Response(`Cell ${cell.id} (default tenant) says hi\n`);
});

cell.connect((socket: WebSocket, id: string) => {
  socket.send(JSON.stringify({ message: `Welcome to Cell ${cell.id} (default tenant)` }));
});

cell.message((event: MessageEvent, socket: WebSocket, id: string) => {
  cell.broadcast(event.data);
});

// Test: Ensure the cell can handle a basic request
if (import.meta.main) {
  console.log("Default tenant cell initialized successfully");
}