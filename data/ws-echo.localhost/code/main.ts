export default {
  // Called when the server starts, before accepting connections
  async onStart(ctx: { roomId: string }) {
    console.log("Server started for room", { roomId: ctx.roomId });
  },

  // Called when a new WebSocket connection is established
  async onConnect(ws: WebSocket, ctx: { roomId: string }) {
    console.log("WebSocket connected", { roomId: ctx.roomId });
    // Send a welcome message
    ws.send(
      JSON.stringify({ type: "welcome", message: "Welcome to ws-echo.local!" }),
    );
  },

  // Called when a WebSocket message is received
  async onMessage(ws: WebSocket, data: string, ctx: { roomId: string }) {
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

  // Called when a WebSocket connection is closed
  async onClose(ws: WebSocket, ctx: { roomId: string }) {
    console.log("WebSocket closed", { roomId: ctx.roomId });
  },

  // Called when a WebSocket error occurs
  async onError(ws: WebSocket, error: Event, ctx: { roomId: string }) {
    console.error("WebSocket error:", error, { roomId: ctx.roomId });
  },

  // Called for HTTP requests
  async onRequest(request: Request, ctx: { roomId: string }) {
    const url = new URL(request.url);
    console.log(`Request for path: ${url.pathname}`, { roomId: ctx.roomId });

    if (url.pathname === "/ping") {
      return new Response("pong");
    }

    return new Response("hello from ws-echo.local\n");
  },
};
