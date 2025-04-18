export default {
  // Called when the server starts
  async onStart(ctx: { roomId: string }) {
    console.log("Server started for room", { roomId: ctx.roomId });
  },

  // Called for HTTP requests
  async onRequest(_request: Request, ctx: { roomId: string }) {
    console.log("Handling request", { roomId: ctx.roomId });
    return new Response("hello from hello.localhost\n");
  },
};
