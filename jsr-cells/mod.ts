import { DatabaseSync } from "node:sqlite";

// Create a Cell class to track sockets and provide broadcast functionality
export class Cell {
  tenant: string;
  id: string;
  ctlClient: Deno.HttpClient;
  sockets: Map<string, WebSocket>;
  private server: Deno.HttpServer | null = null;
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
    // Get the config params from env var when the process starts
    this.tenant = Deno.env.get("X-Tenant")!;
    this.id = Deno.env.get("X-Cell-Id")!;
    const ctlSockPath = Deno.env.get("CELL_CONTROL_SOCKET")!;
    this.ctlClient = Deno.createHttpClient({
      proxy: {
        transport: "unix",
        path: ctlSockPath,
      },
    });
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

  alarm(cb: () => Promise<void> | void): void {
    this.onAlarmCallback = cb;
  }

  async getAlarm(): Promise<number | null> {
    const res = await fetch(
      `http://localhost/_internal/alarms?tenant=${this.tenant}&cell_id=${this.id}`,
      {
        client: this.ctlClient,
      },
    );
    if (res.status !== 200) {
      return null;
    }
    const data = await res.json();
    return data.scheduled_time_unix_ms;
  }

  async setAlarm(scheduledTimeUnixMs: number): Promise<void> {
    await fetch("http://localhost/_internal/alarms", {
      client: this.ctlClient,
      method: "POST",
      body: JSON.stringify({
        tenant: this.tenant,
        cell_id: this.id,
        scheduled_time_unix_ms: scheduledTimeUnixMs,
      }),
    });
  }

  async deleteAlarm(): Promise<boolean> {
    const res = await fetch("http://localhost/_internal/alarms", {
      client: this.ctlClient,
      method: "DELETE",
      body: JSON.stringify({
        tenant: this.tenant,
        cell_id: this.id,
      }),
    });
    return res.status === 200;
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
    this.server = Deno.serve(async (req) => {
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
        // Invoke the alarm callback
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

    // Handle SIGTERM for graceful shutdown
    Deno.addSignalListener("SIGTERM", () => {
      console.log("SIGTERM received, shutting down gracefully...");
      cell.shutdown();
    });
  }

  async shutdown(): Promise<void> {
    if (this.server) {
      await this.server.shutdown();
      this.server = null;
    }

    // Close all WebSocket connections
    for (const socket of this.sockets.values()) {
      socket.close(1000, "Server shutting down");
    }
    this.sockets.clear();

    // Close database connection if open
    if (this.dbInstance) {
      this.dbInstance.close();
      this.dbInstance = null;
    }

    console.log("Shutdown complete");
  }
}

// Export a singleton instance of the Cell class
export const cell: Cell = new Cell();
