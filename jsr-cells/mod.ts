import { DatabaseSync } from "node:sqlite";

// Create a Cell class to track sockets and provide broadcast functionality
export class Cell {
  id: string;
  sockets: Map<string, WebSocket>;
  private dbInstance: DatabaseSync | null = null;
  private onRequestCallback:
    | ((req: Request) => Promise<Response> | Response | void)
    | null = null;
  private onConnectCallback:
    | ((socket: WebSocket, id: string) => Promise<void> | void)
    | null = null;
  private onMessageCallback:
    | ((
      event: MessageEvent,
      socket: WebSocket,
      id: string,
    ) => Promise<void> | void)
    | null = null;
  private onCloseCallback:
    | ((socket: WebSocket, id: string) => Promise<void> | void)
    | null = null;
  private onErrorCallback:
    | ((error: Error | ErrorEvent | Event) => Promise<void> | void)
    | null = null;
  private onAlarmCallback:
    | (() => Promise<void> | void)
    | null = null;

  constructor() {
    // Get the cell ID from environment variable when the process starts
    this.id = Deno.env.get("X-Cell-Id")!;
    this.sockets = new Map<string, WebSocket>();
    this.setupServer();
  }

  broadcast(
    message: string | ArrayBuffer | ArrayBufferView,
    without: string[] = [],
  ): void {
    const msg = typeof message === "string" ? message : message;
    for (const [id, conn] of this.sockets.entries()) {
      if (!without.includes(id) && conn.readyState === WebSocket.OPEN) {
        conn.send(msg);
      }
    }
  }

  getWebSocket(id: string): WebSocket | undefined {
    return this.sockets.get(id);
  }

  getWebSockets(): Iterable<WebSocket> {
    return this.sockets.values();
  }

  request(cb: (req: Request) => Promise<Response> | Response | void): void {
    this.onRequestCallback = cb;
  }

  onAlarm(cb: () => Promise<void> | void): void {
    this.onAlarmCallback = cb;
  }

  connect(cb: (socket: WebSocket, id: string) => Promise<void> | void): void {
    this.onConnectCallback = cb;
  }

  message(
    cb: (
      event: MessageEvent,
      socket: WebSocket,
      id: string,
    ) => Promise<void> | void,
  ): void {
    this.onMessageCallback = cb;
  }

  close(cb: (socket: WebSocket, id: string) => Promise<void> | void): void {
    this.onCloseCallback = cb;
  }

  error(cb: (error: Error | ErrorEvent | Event) => Promise<void> | void): void {
    this.onErrorCallback = cb;
  }

  get db(): DatabaseSync {
    if (!this.dbInstance) {
      this.dbInstance = new DatabaseSync(`./sqlite/${this.id}.db`);
    }
    return this.dbInstance;
  }

  private setupServer(): void {
    Deno.serve(async (req) => {
      // Handle WebSocket connections
      if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
        const { response, socket } = Deno.upgradeWebSocket(req);

        // Create a unique ID for this socket
        const socketId = crypto.randomUUID();

        this.sockets.set(socketId, socket);

        // Set up the WebSocket event handlers
        socket.onopen = () => {
          if (this.onConnectCallback) {
            this.onConnectCallback(socket, socketId);
          }
        };

        socket.onmessage = (e) => {
          if (this.onMessageCallback) {
            this.onMessageCallback(e, socket, socketId);
          }
        };

        socket.onclose = () => {
          this.sockets.delete(socketId);

          if (this.onCloseCallback) {
            this.onCloseCallback(socket, socketId);
          }
        };

        socket.onerror = (error) => {
          if (this.onErrorCallback) {
            this.onErrorCallback(error);
          }
        };

        return response;
      }

      const url = new URL(req.url);

      if (req.method === "POST" && url.pathname === "/_internal/alarm") {
        // Invoke the onAlarm callback
        if (this.onAlarmCallback) {
          await this.onAlarmCallback();
        }

        return new Response("OK", { status: 200 });
      }

      // Handle HTTP requests
      if (this.onRequestCallback) {
        const result = this.onRequestCallback(req);
        if (result instanceof Promise) {
          return await result;
        }
        if (result instanceof Response) {
          return result;
        }
      }

      return new Response("Not Found", { status: 404 });
    });
  }
}

// Export a singleton instance of the Cell class
export const cell: Cell = new Cell();
