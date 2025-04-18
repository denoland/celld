export default {
  async fetch(_request: Request, ctx: { roomId: string }) {
    console.log("Handling request", { roomId: ctx.roomId });
    return new Response("hello from hello.localhost\n");
  },
};
