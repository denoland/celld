import { DatabaseSync } from "node:sqlite";
import { WorkflowRuntime } from "./workflow.ts";
import { ulid } from "jsr:@std/ulid@1/ulid";
import {
  type DbAccessor,
  type JSONValue,
  scheduledTaskId,
  type TaskScheduler,
  type Unvoidable,
  type Voidable,
  type WorkflowDef,
  type WorkflowRunId,
} from "./types.ts";

/**
 * Create an in-memory SQLite database for testing
 */
export function createTestDatabase(): DatabaseSync {
  return new DatabaseSync(":memory:");
}

/**
 * Wait for a condition to be true, with timeout
 */
export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  options?: {
    timeoutMs?: number;
    intervalMs?: number;
    message?: string;
  },
): Promise<void> {
  const timeoutMs = options?.timeoutMs ?? 5000;
  const intervalMs = options?.intervalMs ?? 100;
  const message = options?.message ?? "Condition not met";

  const startTime = Date.now();

  while (true) {
    const result = await condition();
    if (result) {
      return;
    }

    if (Date.now() - startTime > timeoutMs) {
      throw new Error(`Timeout waiting for condition: ${message}`);
    }

    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

/**
 * Helper to create test data in database
 */
export function setupTestData(
  db: DatabaseSync,
  setup: (db: DatabaseSync) => void,
): void {
  setup(db);
}

/**
 * Time control utilities for testing
 */
export class TimeController {
  #currentTime: number;
  #originalDateNow: typeof Date.now;
  #originalSetTimeout: typeof setTimeout;
  #timers: Map<number, { callback: () => void; triggerTime: number }>;
  #nextTimerId: number;

  constructor(initialTime?: number) {
    this.#currentTime = initialTime ?? Date.now();
    this.#originalDateNow = Date.now;
    this.#originalSetTimeout = globalThis.setTimeout;
    this.#timers = new Map();
    this.#nextTimerId = 1;
  }

  /**
   * Install time mocks
   */
  install(): void {
    // Mock Date.now
    Date.now = () => this.#currentTime;

    // Mock setTimeout
    (globalThis as any).setTimeout = (callback: () => void, delay: number) => {
      const timerId = this.#nextTimerId++;
      this.#timers.set(timerId, {
        callback,
        triggerTime: this.#currentTime + delay,
      });
      return timerId;
    };
  }

  /**
   * Restore original time functions
   */
  uninstall(): void {
    Date.now = this.#originalDateNow;
    globalThis.setTimeout = this.#originalSetTimeout;
  }

  /**
   * Advance time by specified milliseconds
   */
  advance(ms: number): void {
    this.#currentTime += ms;

    // Trigger any timers that should fire
    for (const [timerId, timer] of this.#timers.entries()) {
      if (timer.triggerTime <= this.#currentTime) {
        timer.callback();
        this.#timers.delete(timerId);
      }
    }
  }

  /**
   * Set current time to specific value
   */
  setTime(time: number | Date): void {
    this.#currentTime = typeof time === "number" ? time : time.getTime();
  }

  /**
   * Get current mocked time
   */
  get currentTime(): number {
    return this.#currentTime;
  }
}

/**
 * Test environment for workflows
 */
export interface TestEnvironment {
  db: DatabaseSync;
  mockStep: <T extends Voidable<JSONValue>>(
    stepName: string,
    handler: () => T,
  ) => void;
  runWorkflow: <TInput extends JSONValue, TOutput extends Voidable<JSONValue>>(
    workflow: WorkflowDef<TInput, TOutput>,
    input: TInput,
  ) => Promise<{
    runId: WorkflowRunId;
    waitForCompletion: () => Promise<TOutput>;
  }>;
  close: () => void;
}

/**
 * Create a test environment for workflows
 */
export function createTestEnvironment(): TestEnvironment {
  const db = new DatabaseSync(":memory:");
  const dbAccessor: DbAccessor = {
    get db() {
      return db;
    },
  };

  // Mock task scheduler
  const taskScheduler: TaskScheduler = {
    schedule: (task) => {
      const id = scheduledTaskId(ulid());
      // Create scheduled_tasks table if not exists
      db.exec(`
        CREATE TABLE IF NOT EXISTS scheduled_tasks (
          id TEXT PRIMARY KEY NOT NULL,
          scheduled_time_unix_ms INTEGER NOT NULL,
          payload TEXT NOT NULL
        )
      `);
      db.prepare(`
        INSERT INTO scheduled_tasks (id, scheduled_time_unix_ms, payload) VALUES (?, ?, ?)
      `).run(id, task.scheduledTimeUnixMs, JSON.stringify(task));
      return id;
    },
  };

  const runtime = new WorkflowRuntime(dbAccessor, taskScheduler);

  // Initialize tables
  runtime.listRuns();

  const mockedSteps = new Map<string, () => Voidable<JSONValue>>();
  const workflowWrappers = new Map<
    string,
    WorkflowDef<JSONValue, Voidable<JSONValue>>
  >();

  return {
    db,
    mockStep: <T extends Voidable<JSONValue>>(
      stepName: string,
      handler: () => T,
    ) => {
      mockedSteps.set(stepName, handler);
    },
    runWorkflow: async <
      TInput extends JSONValue,
      TOutput extends Voidable<JSONValue>,
    >(
      workflow: WorkflowDef<TInput, TOutput>,
      input: TInput,
    ) => {
      // Get or create a wrapper workflow that uses mocked steps
      let wrapperWorkflow = workflowWrappers.get(workflow.name) as
        | WorkflowDef<TInput, TOutput>
        | undefined;
      if (!wrapperWorkflow) {
        wrapperWorkflow = runtime.define<TInput, TOutput>({
          name: workflow.name,
          handler: async (ctx) => {
            const originalStep = ctx.step;
            const mockedStep: typeof ctx.step = {
              ...originalStep,
              run: async <StepOutput extends Voidable<JSONValue>>(
                name: string,
                fn: () => StepOutput | Promise<StepOutput>,
              ) => {
                const mockedHandler = mockedSteps.get(name);
                if (mockedHandler) {
                  const result = await mockedHandler();
                  if (result === undefined) {
                    return null as Unvoidable<StepOutput>;
                  } else {
                    return result as Unvoidable<StepOutput>;
                  }
                }
                return originalStep.run(name, fn);
              },
              invoke: originalStep.invoke,
              sleep: originalStep.sleep,
            };
            // Call the original workflow handler with mocked step
            const originalCtx = { ...ctx, step: mockedStep };
            return workflow.config.handler(originalCtx);
          },
        });
        workflowWrappers.set(
          workflow.name,
          wrapperWorkflow as unknown as WorkflowDef<
            JSONValue,
            Voidable<JSONValue>
          >,
        );
      }

      const runId = runtime.dispatch(wrapperWorkflow, input);

      return {
        runId,
        waitForCompletion: async () => {
          const MAX_RETRIES = 50;
          const INTERVAL_MS = 100;

          for (let i = 0; i < MAX_RETRIES; i++) {
            const progress = runtime.getRunProgress(runId);
            if (!progress) {
              throw new Error(`Workflow run not found: ${runId}`);
            }
            if (progress.completedAt !== null) {
              return progress.outputData as TOutput;
            }
            await new Promise((resolve) => setTimeout(resolve, INTERVAL_MS));
          }

          throw new Error("Workflow did not complete within timeout");
        },
      };
    },
    close: () => {
      db.close();
    },
  };
}
