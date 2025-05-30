import { cell } from "../../../jsr-cells/mod.ts";

console.log(`[${cell.id}] Initializing...`);

cell.request((req: Request): Response => {
  const single = cell.vectorize.vectorize({
    input: {
      text: "Deno is a modern runtime", // maybe rename to `source` or `original`?
      embedding: new Float64Array([0.1, 0.2]),
    },
    persist: true,
    namespace: "default",
    metadata: { type: "tech" },
  });

  const { id, embedding } = single;

  const query = cell.vectorize.queryByEmbedding(embedding, {
    topK: 3,
    namespace: "default",
  });

  console.log(query);

  return new Response("Vectorized input successfully!\n", { status: 200 });
});
