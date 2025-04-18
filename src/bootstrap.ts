if (import.meta.main) {
  if (Deno.args.length !== 1) {
    console.error("Usage: bootstrap.ts <path-to-user-module>");
    Deno.exit(1);
  }
  const userModulePath = Deno.args[0];
  await bootstrap(userModulePath);
}

interface Context {
  roomId: string;
}

interface Server {
  webSocketOpen?: (ws: WebSocket, ctx: Context) => Promise<void>;
  webSocketMessage?: (
    ws: WebSocket,
    msg: string,
    ctx: Context,
  ) => Promise<void>;
  webSocketClose?: (ws: WebSocket, ctx: Context) => Promise<void>;
  fetch?: (req: Request, ctx: { roomId: string }) => Promise<Response>;
}

async function bootstrap(userModulePath: string) {
  const module = await import(userModulePath);
  // Support export default pattern
  const userModule: Server = module.default || module;

  // Get the room ID from environment variable once, when the process starts
  const roomId = Deno.env.get("X-Room-Id") || "";
  console.log(`Bootstrap starting with roomId: ${roomId}`);

  Deno.serve(async (req) => {
    // Use the room ID from the environment variable for all requests
    const ctx = { roomId };

    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
      const { response, socket } = Deno.upgradeWebSocket(req);
      socket.onopen = () => userModule.webSocketOpen?.(socket, ctx);
      socket.onmessage = (e) =>
        userModule.webSocketMessage?.(socket, e.data as string, ctx);
      socket.onclose = () => userModule.webSocketClose?.(socket, ctx);
      return response;
    }

    if (userModule.fetch) {
      return userModule.fetch(req, ctx);
    }

    return new Response("Not Found", { status: 404 });
  });
}
