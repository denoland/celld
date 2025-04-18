// WebSocket echo server with export default pattern
export default {
  // WebSocket handlers
  async webSocketOpen(ws: WebSocket, ctx: { roomId: string }) {
    console.log("WebSocket opened", { roomId: ctx.roomId });
    // Send a welcome message
    ws.send(
      JSON.stringify({ type: "welcome", message: "Welcome to ws-echo.local!" }),
    );
  },

  async webSocketMessage(ws: WebSocket, data: string, ctx: { roomId: string }) {
    console.log("Received message:", data, { roomId: ctx.roomId });
    // Echo the message back with a timestamp
    const timestamp = new Date().toISOString();
    ws.send(JSON.stringify({
      type: "echo",
      originalMessage: data,
      timestamp,
      roomId: ctx.roomId,
    }));
  },

  async webSocketClose(ws: WebSocket, ctx: { roomId: string }) {
    console.log("WebSocket closed", { roomId: ctx.roomId });
  },

  // HTTP handler
  async fetch(request: Request, ctx: { roomId: string }) {
    const url = new URL(request.url);
    console.log(`Request for path: ${url.pathname}`, { roomId: ctx.roomId });

    if (url.pathname === "/ping") {
      return new Response("pong");
    }

    return new Response("hello from ws-echo.local\n");
  },
};
