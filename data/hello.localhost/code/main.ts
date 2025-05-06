export default {
  async onRequest(_request: Request, ctx: { cellId: string }) {
    //console.log("Handling request", { cellId: ctx.cellId });
    return new Response("hello from hello.localhost\n");
  },
};
