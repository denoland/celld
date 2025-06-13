import { DatabaseSync } from "node:sqlite";
import {
  type DbAccessor,
  type ScheduledTaskId,
  scheduledTaskId,
  type Task,
  type TaskScheduler,
} from "./types.ts";
import { WorkflowRuntime } from "./workflow.ts";
import { ulid } from "jsr:@std/ulid@^1.0.0/ulid";

// Create a Cell class to track sockets and provide broadcast functionality
export class Cell implements DbAccessor, TaskScheduler {
  tenant: string;
  id: string;
  ctlClient: Deno.HttpClient;
  sockets: Map<string, WebSocket>;

  #server: Deno.HttpServer | null = null;
  #dbPath: string;
  #dbInstance: DatabaseSync | null = null;
  #workflow: WorkflowRuntime | null = null;
  #onRequestCallback:
    | ((req: Request) => Promise<Response> | Response | void)
    | null = null;
  #onConnectCallback:
    | ((socket: WebSocket, id: string) => Promise<void> | void)
    | null = null;
  #onMessageCallback:
    | ((
      event: MessageEvent,
      socket: WebSocket,
      id: string,
    ) => Promise<void> | void)
    | null = null;
  #onCloseCallback:
    | ((socket: WebSocket, id: string) => Promise<void> | void)
    | null = null;
  #onErrorCallback:
    | ((error: Error | ErrorEvent | Event) => Promise<void> | void)
    | null = null;
  #onAlarmCallback:
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
    this.#dbPath = args?.dbPath ?? Cell.#defaultDbPath;
    const ctlSockPath = args?.ctlSockPath ?? Cell.#defaultCtlSockPath;
    this.ctlClient = Deno.createHttpClient({
      proxy: {
        transport: "unix",
        path: ctlSockPath,
      },
    });
    this.sockets = new Map<string, WebSocket>();
    this.#setupServer();
    this.#setupTables();
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
    if (this.#onRequestCallback) {
      throw new Error(
        `Handler for request already registered for cell ${this.id}`,
      );
    }
    this.#onRequestCallback = cb;
  }

  alarm(cb: () => Promise<void> | void): void {
    if (this.#onAlarmCallback) {
      throw new Error(
        `Handler for alarm already registered for cell ${this.id}`,
      );
    }
    this.#onAlarmCallback = cb;
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

  // Track the currently scheduled global alarm time
  #currentGlobalAlarmTime: number | null = null;

  async #handleAlarm(): Promise<void> {
    const currentTime = Date.now();

    // Clear our tracked alarm time since we're handling it now
    this.#currentGlobalAlarmTime = null;

    // Retrieve ALL tasks that are due now or overdue
    const dueTasks = this.db.prepare(`
      SELECT id, payload FROM scheduled_tasks
      WHERE scheduled_time_unix_ms <= ?
      ORDER BY scheduled_time_unix_ms ASC
    `).all(currentTime);

    // Dispatch the associated operations based on the task kind
    for (const task of dueTasks) {
      const payload = JSON.parse(task.payload as string) as Task;
      try {
        switch (payload.kind) {
          case "user-defined-alarm": {
            await this.#onAlarmCallback?.();
            break;
          }
          case "resume-all-pending-workflow-runs": {
            if (this.#workflow) {
              this.#workflow.resumeAllPendingWorkflowRuns();
            }
            break;
          }
          case "retry-workflow-run": {
            if (this.#workflow) {
              this.#workflow.retry(payload.workflowRunId);
            }
            break;
          }
          case "wake-sleep-step": {
            // Mark the sleep step as completed
            this.db.prepare(`
            UPDATE workflow_steps
            SET completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc')
            WHERE workflow_run_id = ? AND step_index = ?
          `).run(payload.workflowRunId, payload.stepIndex);

            // Retry the workflow directly
            if (this.workflow) {
              this.workflow.retry(payload.workflowRunId);
            }
            break;
          }
          default: {
            throw new Error(`Unknown task kind: ${payload satisfies never}`);
          }
        }
      } catch (error) {
        console.error(`Error processing task ${task.id}:`, error);
        // Continue processing other tasks even if one fails
      }

      // Delete the task
      this.db.prepare(`
        DELETE FROM scheduled_tasks WHERE id = ?
      `).run(task.id);
    }

    // AFTER processing all due tasks, schedule the next alarm if any
    await this.#scheduleNextAlarm();
  }

  async #scheduleNextAlarm(): Promise<void> {
    // Get the next task that needs to run
    const nextTask = this.db.prepare(`
      SELECT scheduled_time_unix_ms FROM scheduled_tasks
      ORDER BY scheduled_time_unix_ms ASC
      LIMIT 1
    `).get();

    if (nextTask) {
      const nextTime = nextTask.scheduled_time_unix_ms as number;

      if (
        !this.#currentGlobalAlarmTime || nextTime < this.#currentGlobalAlarmTime
      ) {
        await this.#scheduleGlobalAlarm(nextTime);
      }
    } else {
      // No more tasks, clear our tracked time
      this.#currentGlobalAlarmTime = null;
    }
  }

  connect(cb: (socket: WebSocket, id: string) => Promise<void> | void): void {
    if (this.#onConnectCallback) {
      throw new Error(
        `Handler for connect already registered for cell ${this.id}`,
      );
    }
    this.#onConnectCallback = cb;
  }

  message(
    cb: (
      event: MessageEvent,
      socket: WebSocket,
      id: string,
    ) => Promise<void> | void,
  ): void {
    if (this.#onMessageCallback) {
      throw new Error(
        `Handler for message already registered for cell ${this.id}`,
      );
    }
    this.#onMessageCallback = cb;
  }

  close(cb: (socket: WebSocket, id: string) => Promise<void> | void): void {
    if (this.#onCloseCallback) {
      throw new Error(
        `Handler for close already registered for cell ${this.id}`,
      );
    }
    this.#onCloseCallback = cb;
  }

  error(cb: (error: Error | ErrorEvent | Event) => Promise<void> | void): void {
    if (this.#onErrorCallback) {
      throw new Error(
        `Handler for error already registered for cell ${this.id}`,
      );
    }
    this.#onErrorCallback = cb;
  }

  get db(): DatabaseSync {
    if (!this.#dbInstance) {
      this.#dbInstance = new DatabaseSync(this.#dbPath);
    }
    return this.#dbInstance;
  }

  get workflow(): WorkflowRuntime {
    if (!this.#workflow) {
      this.#workflow = new WorkflowRuntime(this, this);
    }
    return this.#workflow;
  }

  #setupServer(): void {
    this.#server = Deno.serve(async (req) => {
      console.error({ url: req.url, method: req.method });
      // Handle WebSocket connections
      if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
        const { response, socket } = Deno.upgradeWebSocket(req);

        // Create a unique ID for this socket
        const socketId = crypto.randomUUID();

        this.sockets.set(socketId, socket);

        // Set up the WebSocket event handlers
        socket.onopen = () => {
          if (this.#onConnectCallback) {
            this.#onConnectCallback(socket, socketId);
          }
        };

        socket.onmessage = (e) => {
          if (this.#onMessageCallback) {
            this.#onMessageCallback(e, socket, socketId);
          }
        };

        socket.onclose = () => {
          this.sockets.delete(socketId);

          if (this.#onCloseCallback) {
            this.#onCloseCallback(socket, socketId);
          }
        };

        socket.onerror = (error) => {
          if (this.#onErrorCallback) {
            this.#onErrorCallback(error);
          }
        };

        return response;
      }

      const url = new URL(req.url);

      if (req.method === "POST" && url.pathname === "/_internal/alarm") {
        await this.#handleAlarm();
        return new Response("OK", { status: 200 });
      }

      // Handle HTTP requests
      if (this.#onRequestCallback) {
        const modifiedReq = new Request(
          req.url.replace(/^http\+unix:/, "http:"),
          req,
        );
        const result = this.#onRequestCallback(modifiedReq);
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
      console.log(
        `SIGTERM received, shutting down ${this.tenant}/${this.id} gracefully...`,
      );
      this.shutdown();
    });
  }

  #setupTables(): void {
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
    if (this.#server) {
      await this.#server.shutdown();
      this.#server = null;
    }

    // Close all WebSocket connections
    for (const socket of this.sockets.values()) {
      socket.close(1000, "Server shutting down");
    }
    this.sockets.clear();

    // If there are ongoing workflow runs, schedule a task to resume them on
    // another node later.
    if (WorkflowRuntime.runningWorkflows() > 0) {
      await this.schedule({
        kind: "resume-all-pending-workflow-runs",
        scheduledTimeUnixMs: Date.now() + 10_000,
      });
    }

    // Close database connection if open
    if (this.#dbInstance) {
      this.#dbInstance.close();
      this.#dbInstance = null;
    }

    console.log(
      `Shutdown complete for ${this.tenant}/${this.id}`,
    );
    Deno.exit(0);
  }

  async schedule(task: Task): Promise<ScheduledTaskId> {
    const id = scheduledTaskId(ulid());
    this.db.prepare(`
      INSERT INTO scheduled_tasks (id, scheduled_time_unix_ms, payload) VALUES (?, ?, ?)
    `).run(id, task.scheduledTimeUnixMs, JSON.stringify(task));

    // Check if we need to update the global alarm
    // This happens if:
    // 1. We don't have a global alarm scheduled yet, OR
    // 2. The new task should run before the current global alarm
    if (
      !this.#currentGlobalAlarmTime ||
      task.scheduledTimeUnixMs < this.#currentGlobalAlarmTime
    ) {
      await this.#scheduleGlobalAlarm(task.scheduledTimeUnixMs);
    }

    return id;
  }

  async #scheduleGlobalAlarm(
    scheduledTimeUnixMs: number,
  ): Promise<void> {
    // Update our tracked time before making the call
    this.#currentGlobalAlarmTime = scheduledTimeUnixMs;

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
}

// Export a singleton instance of the Cell class
export const cell: Cell = new Cell();
