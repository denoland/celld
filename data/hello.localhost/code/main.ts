import { cell } from "jsr:@ry/cells";

console.log(`[${cell.id}] Initializing...`);

cell.request((_) => new Response("hello\n"));
