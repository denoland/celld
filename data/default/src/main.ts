import { cell } from "../../../jsr-cells/mod.ts";

cell.request((req: Request): Response => {
  return new Response(`Cell ${cell.id} (default tenant) says hi\n`);
});
