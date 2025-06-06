import { DatabaseSync } from "node:sqlite";
import {
  type DbAccessor,
  type JSONValue,
  type ScheduledTaskId,
  scheduledTaskId,
  type Task,
  type TaskScheduler,
} from "./types.ts";
import { Workflow } from "./workflow.ts";
import { ulid } from "jsr:@std/ulid@^1.0.0/ulid";
import { logger, setup as setupLogger } from "./logger.ts";

// Create a Cell class to track sockets and provide broadcast functionality
export class Cell implements DbAccessor, TaskScheduler {
  tenant: string;
  id: string;
  ctlClient: Deno.HttpClient;
  sockets: Map<string, WebSocket>;

  private server: Deno.HttpServer | null = null;
  private dbPath: string;
  private dbInstance: DatabaseSync | null = null;
  private workflow: Workflow<Record<string, JSONValue>> | null = null;
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

  static #defaultTenant: string;
  static #defaultId: string;
  static #defaultDbPath: string;
  static #defaultCtlSockPath: string;
  static {
    this.#defaultTenant = Deno.env.get("X-Tenant")!;
    this.#defaultId = Deno.env.get("X-Cell-Id")!;
    this.#defaultDbPath = `./sqlite/${this.#defaultId}.db`;
    this.#defaultCtlSockPath = Deno.env.get("CELL_CONTROL_SOCKET")!;
  }

  constructor(args?: {
    tenant?: string;
    id?: string;
    dbPath?: string;
    ctlSockPath?: string;
  }) {
    this.tenant = args?.tenant ?? Cell.#defaultTenant;
    this.id = args?.id ?? Cell.#defaultId;
    setupLogger(this.tenant, this.id, "DEBUG");
    this.dbPath = args?.dbPath ?? Cell.#defaultDbPath;
    const ctlSockPath = args?.ctlSockPath ?? Cell.#defaultCtlSockPath;
    this.ctlClient = Deno.createHttpClient({
      proxy: {
        transport: "unix",
        path: ctlSockPath,
      },
    });
    this.sockets = new Map<string, WebSocket>();
    this.setupServer();
    this.setupTables();
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

  /**
   * Initialize a workflow. This method can be called only once.
   *
   * @param T - The type of the workflow inputs.
   * @returns The initialized workflow.
   */
  initWorkflow<T extends Record<string, JSONValue>>(): Workflow<T> {
    if (this.workflow) {
      throw new Error("Workflow already initialized");
    }
    const workflow = new Workflow<T>(this, this);
    this.workflow = workflow as Workflow<Record<string, JSONValue>>;
    return workflow;
  }

  getWebSocket(id: string): WebSocket | undefined {
    return this.sockets.get(id);
  }

  getWebSockets(): Iterable<WebSocket> {
    return this.sockets.values();
  }

  request(cb: (req: Request) => Promise<Response> | Response | void): void {
    if (this.onRequestCallback) {
      throw new Error(
        `Handler for request already registered for cell ${this.id}`,
      );
    }
    this.onRequestCallback = cb;
  }

  alarm(cb: () => Promise<void> | void): void {
    if (this.onAlarmCallback) {
      throw new Error(
        `Handler for alarm already registered for cell ${this.id}`,
      );
    }
    this.onAlarmCallback = cb;
  }

  /**
   * Get the scheduled time of the next alarm.
   *
   * @param id - The ID of the task to get. If not provided, the next alarm is returned.
   * @returns The scheduled time of the next alarm, or null if no such task exists.
   */
  getAlarm(id?: ScheduledTaskId): number | null {
    if (id === undefined) {
      // Get the closest "user-defined-alarm" task
      const result = this.db.prepare(`
        SELECT
          scheduled_time_unix_ms
        FROM scheduled_tasks
        WHERE JSON_EXTRACT(payload, '$.kind') = 'user-defined-alarm'
        ORDER BY scheduled_time_unix_ms ASC LIMIT 1
      `).get();
      if (!result) {
        return null;
      }
      return result.scheduled_time_unix_ms as number;
    }

    const result = this.db.prepare(`
      SELECT scheduled_time_unix_ms FROM scheduled_tasks WHERE id = ?
    `).get(id);
    if (!result) {
      return null;
    }
    return result.scheduled_time_unix_ms as number;
  }

  setAlarm(scheduledTimeUnixMs: number): Promise<ScheduledTaskId> {
    return this.schedule({
      kind: "user-defined-alarm",
      scheduledTimeUnixMs,
    });
  }

  deleteAlarm(id: ScheduledTaskId): boolean {
    const result = this.db.prepare(`
      DELETE FROM scheduled_tasks WHERE id = ?
    `).run(id);

    // We don't bother to delete a global_alarms record here because "spurious"
    // alarm invocation does not have any bad effect on the correctness.
    // In other words, it's okay to receive an alarm issued by the system main
    // cell even if that alarm is supposed to trigger the deleted task, because
    // a task is executed only when the scheduled timestamp matches.

    return result.changes > 0;
  }

  private async handleAlarm(scheduledTimeUnixMs: number): Promise<void> {
    // Retrieve tasks scheduled at the given timestamp
    const tasks = this.db.prepare(`
      SELECT id, payload FROM scheduled_tasks WHERE scheduled_time_unix_ms = ?
    `).all(scheduledTimeUnixMs);

    // Dispatch the associated operations based on the task kind
    for (const task of tasks) {
      const payload = JSON.parse(task.payload as string) as Task;
      switch (payload.kind) {
        case "user-defined-alarm": {
          await this.onAlarmCallback?.();
          break;
        }
        case "resume-all-pending-workflow-runs": {
          if (this.workflow) {
            this.workflow.resumeAllPendingWorkflowRuns();
          }
          break;
        }
        case "retry-workflow-run": {
          if (this.workflow) {
            this.workflow.retry(payload.workflowRunId);
          }
          break;
        }
        default: {
          throw new Error(`Unknown task kind: ${payload satisfies never}`);
        }
      }

      // Delete the task
      this.db.prepare(`
        DELETE FROM scheduled_tasks WHERE id = ?
      `).run(task.id);
    }
  }

  connect(cb: (socket: WebSocket, id: string) => Promise<void> | void): void {
    if (this.onConnectCallback) {
      throw new Error(
        `Handler for connect already registered for cell ${this.id}`,
      );
    }
    this.onConnectCallback = cb;
  }

  message(
    cb: (
      event: MessageEvent,
      socket: WebSocket,
      id: string,
    ) => Promise<void> | void,
  ): void {
    if (this.onMessageCallback) {
      throw new Error(
        `Handler for message already registered for cell ${this.id}`,
      );
    }
    this.onMessageCallback = cb;
  }

  close(cb: (socket: WebSocket, id: string) => Promise<void> | void): void {
    if (this.onCloseCallback) {
      throw new Error(
        `Handler for close already registered for cell ${this.id}`,
      );
    }
    this.onCloseCallback = cb;
  }

  error(cb: (error: Error | ErrorEvent | Event) => Promise<void> | void): void {
    if (this.onErrorCallback) {
      throw new Error(
        `Handler for error already registered for cell ${this.id}`,
      );
    }
    this.onErrorCallback = cb;
  }

  get db(): DatabaseSync {
    if (!this.dbInstance) {
      this.dbInstance = new DatabaseSync(this.dbPath);
    }
    return this.dbInstance;
  }

  private setupServer(): void {
    this.server = Deno.serve(async (req) => {
      logger().debug({ url: req.url, method: req.method });
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
        const body = await req.text();
        const scheduledTimeUnixMs = parseInt(body, 10);
        if (Number.isNaN(scheduledTimeUnixMs)) {
          return new Response(`Unable to parse scheduled time: ${body}`, {
            status: 400,
          });
        }

        await this.handleAlarm(scheduledTimeUnixMs);

        // Get the next task's scheduled time and schedule it as a global alarm
        const nextTask = this.db.prepare(`
          SELECT scheduled_time_unix_ms FROM scheduled_tasks ORDER BY scheduled_time_unix_ms ASC LIMIT 1
        `).get();
        if (nextTask !== undefined) {
          // TODO(magurotuna): The next task's schedule could be piggybacked on
          // the response instead of a separate request.
          await this.scheduleGlobalAlarm(
            nextTask.scheduled_time_unix_ms as number,
          );
        }

        return new Response("OK", { status: 200 });
      }

      // Handle HTTP requests
      if (this.onRequestCallback) {
        const modifiedReq = new Request(
          req.url.replace(/^http\+unix:/, "http:"),
          req,
        );
        const result = this.onRequestCallback(modifiedReq);
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
      logger().info(
        `SIGTERM received, shutting down ${this.tenant}/${this.id} gracefully...`,
      );
      this.shutdown();
    });
  }

  private setupTables(): void {
    // Ensure `scheduled_tasks` table exists.
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS scheduled_tasks (
        id TEXT PRIMARY KEY NOT NULL,
        scheduled_time_unix_ms INTEGER NOT NULL,
        payload TEXT NOT NULL
      )
    `);
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_schedule_time ON scheduled_tasks (scheduled_time_unix_ms)
    `);
  }

  async shutdown(): Promise<never> {
    if (this.server) {
      await this.server.shutdown();
      this.server = null;
    }

    logger().debug("Server closed");

    // Close all WebSocket connections
    for (const socket of this.sockets.values()) {
      socket.close(1000, "Server shutting down");
    }
    this.sockets.clear();

    logger().debug("WebSocket connections closed");

    // If there are ongoing workflow runs, schedule a task to resume them on
    // another node later.
    if (Workflow.runningWorkflows() > 0) {
      logger().debug("Scheduling a task to resume all pending workflow runs");
      await this.schedule({
        kind: "resume-all-pending-workflow-runs",
        scheduledTimeUnixMs: Date.now() + 10_000,
      });
      logger().debug("Scheduled to resume all pending workflow runs");
    }

    // Close database connection if open
    if (this.dbInstance) {
      this.dbInstance.close();
      this.dbInstance = null;
      logger().debug("Database connection closed");
    }

    logger().info("Shutdown complete");

    Deno.exit(0);
  }

  async schedule(task: Task): Promise<ScheduledTaskId> {
    const id = scheduledTaskId(ulid());
    this.db.prepare(`
      INSERT INTO scheduled_tasks (id, scheduled_time_unix_ms, payload) VALUES (?, ?, ?)
    `).run(id, task.scheduledTimeUnixMs, JSON.stringify(task));

    await this.scheduleGlobalAlarm(task.scheduledTimeUnixMs);

    return id;
  }

  private async scheduleGlobalAlarm(
    scheduledTimeUnixMs: number,
  ): Promise<void> {
    try {
      const res = await fetch("http://localhost/_internal/alarms", {
        client: this.ctlClient,
        method: "POST",
        body: JSON.stringify({
          tenant: this.tenant,
          cell_id: this.id,
          scheduled_time_unix_ms: scheduledTimeUnixMs,
        }),
      });
      if (!res.ok) {
        logger().error(
          `Failed to schedule global alarm: ${await res.text()}`,
        );
      }
    } catch (e) {
      logger().error(e);
    }
  }
}

// Export a singleton instance of the Cell class
export const cell: Cell = new Cell();
