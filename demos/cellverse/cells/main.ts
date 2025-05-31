import { cell } from "jsr:@ry/cells";

cell.request((_) => new Response("hello\n"));
