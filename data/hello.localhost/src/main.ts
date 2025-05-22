import { cell } from "../../../jsr-cells/mod.ts";

cell.request((_) => new Response("hello\n"));
