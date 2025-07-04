import { cell } from "../../../sdk/mod.ts";

cell.request((req: Request, ctx): Response => {
  return new Response(`Cell ${cell.id} (default tenant) says hi\n`);
});
