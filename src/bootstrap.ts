import { DatabaseSync } from "node:sqlite";

if (import.meta.main) {
  if (Deno.args.length !== 1) {
    console.error("Usage: bootstrap.ts <path-to-user-module>");
    Deno.exit(1);
  }
  const userModulePath = Deno.args[0];
  await bootstrap(userModulePath);
}

// Extended WebSocket interface that includes state and id
export interface Connection extends WebSocket {
  id: string;
  state: Record<string, unknown> | null;
  setState(state: Record<string, unknown> | null): void;
}

// Room context similar to PartyKit
export interface Room {
  id: string;
  name: string;
  connections: Map<string, Connection>;
  broadcast(
    msg: string | ArrayBuffer | ArrayBufferView,
    without?: string[],
  ): void;
  getConnection(id: string): Connection | undefined;
  getConnections(): Iterable<Connection>;
}

interface Context {
  roomId: string; // superfluous - remove in favor of room.id
  room: Room;
  db: DatabaseSync; // should this be in Room interface?
}

interface Server {
  onConnect?: (connection: Connection, ctx: Context) => Promise<void> | void;
  onMessage?: (
    message: string | ArrayBuffer,
    sender: Connection,
    ctx: Context,
  ) => Promise<void> | void;
  onClose?: (connection: Connection, ctx: Context) => Promise<void> | void;
  onError?: (
    connection: Connection,
    error: Event,
    ctx: Context,
  ) => Promise<void> | void;
  onRequest?: (req: Request, ctx: Context) => Promise<Response> | Response;
  onStart?: (ctx: Context) => Promise<void> | void;
}

// Extends WebSocket to implement the Connection interface
function extendWebSocket(socket: WebSocket, id: string): Connection {
  const connection = socket as Connection;
  connection.id = id;
  connection.state = null;

  connection.setState = function (newState: Record<string, unknown> | null) {
    this.state = newState;
    return this.state;
  };

  return connection;
}

async function bootstrap(userModulePath: string) {
  const module = await import(userModulePath);
  // Support export default pattern
  const userModule: Server = module.default || module;

  // Get the room ID from environment variable once, when the process starts
  const roomId = Deno.env.get("X-Room-Id") || "";

  // Create a Room object to track connections and provide broadcast functionality
  const connections = new Map<string, Connection>();

  const room: Room = {
    id: roomId,
    name: userModulePath.split("/").pop()?.replace(/\.ts$/, "") || "unknown",
    connections,

    broadcast(
      message: string | ArrayBuffer | ArrayBufferView,
      without: string[] = [],
    ) {
      const msg = typeof message === "string" ? message : message;
      for (const [id, conn] of connections.entries()) {
        if (!without.includes(id) && conn.readyState === WebSocket.OPEN) {
          conn.send(msg);
        }
      }
    },

    getConnection(id: string): Connection | undefined {
      return connections.get(id);
    },

    getConnections(): Iterable<Connection> {
      return connections.values();
    },
  };

  // Create context object
  const ctx = { roomId, room };

  let dbInstance: DatabaseSync | null = null;
  Object.defineProperty(ctx, "db", {
    configurable: true,
    enumerable: true,
    get() {
      if (!dbInstance) {
        dbInstance = new DatabaseSync(`./sqlite/${roomId}.db`);
      }
      return dbInstance;
    },
  });

  if (userModule.onStart) {
    await userModule.onStart(ctx);
  }

  Deno.serve(async (req) => {
    // Handle WebSocket connections
    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
      const { response, socket } = Deno.upgradeWebSocket(req);

      // Create a unique ID for this connection
      const connectionId = crypto.randomUUID();

      // Extend the WebSocket with our Connection interface
      const connection = extendWebSocket(socket, connectionId);

      // Track the connection
      connections.set(connectionId, connection);

      // Set up the WebSocket event handlers
      socket.onopen = () => {
        if (userModule.onConnect) {
          userModule.onConnect(connection, ctx);
        }
      };

      socket.onmessage = (e) => {
        if (userModule.onMessage) {
          userModule.onMessage(e.data, connection, ctx);
        }
      };

      socket.onclose = () => {
        // Remove the connection from our tracking map
        connections.delete(connectionId);

        if (userModule.onClose) {
          userModule.onClose(connection, ctx);
        }
      };

      socket.onerror = (error) => {
        if (userModule.onError) {
          userModule.onError(connection, error, ctx);
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
