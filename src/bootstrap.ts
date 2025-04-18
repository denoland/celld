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
  onConnect?: (ws: WebSocket, ctx: Context) => Promise<void> | void;
  onMessage?: (
    ws: WebSocket,
    msg: string,
    ctx: Context,
  ) => Promise<void> | void;
  onClose?: (ws: WebSocket, ctx: Context) => Promise<void> | void;
  onError?: (ws: WebSocket, error: Event, ctx: Context) => Promise<void> | void;
  onRequest?: (req: Request, ctx: Context) => Promise<Response> | Response;
  onStart?: (ctx: Context) => Promise<void> | void;
}

async function bootstrap(userModulePath: string) {
  const module = await import(userModulePath);
  // Support export default pattern
  const userModule: Server = module.default || module;

  // Get the room ID from environment variable once, when the process starts
  const roomId = Deno.env.get("X-Room-Id") || "";
  console.log(`Bootstrap starting with roomId: ${roomId}`);

  // Create context object
  const ctx = { roomId };

  // Call onStart if it exists
  if (userModule.onStart) {
    await userModule.onStart(ctx);
  }

  Deno.serve(async (req) => {
    // Handle WebSocket connections
    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
      const { response, socket } = Deno.upgradeWebSocket(req);

      // Set up the WebSocket event handlers
      socket.onopen = () => {
        if (userModule.onConnect) {
          userModule.onConnect(socket, ctx);
        }
      };

      socket.onmessage = (e) => {
        if (userModule.onMessage) {
          userModule.onMessage(socket, e.data as string, ctx);
        }
      };

      socket.onclose = () => {
        if (userModule.onClose) {
          userModule.onClose(socket, ctx);
        }
      };

      socket.onerror = (error) => {
        if (userModule.onError) {
          userModule.onError(socket, error, ctx);
        }
      };

      return response;
    }

    // Handle HTTP requests
    if (userModule.onRequest) {
      return userModule.onRequest(req, ctx);
    }

    return new Response("Not Found", { status: 404 });
  });
}
