import { cell } from "../../../sdk/mod.ts";

cell.request((_) => new Response("hello\n"));
