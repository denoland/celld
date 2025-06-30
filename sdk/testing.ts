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
  ) => {
    runId: WorkflowRunId;
    waitForCompletion: () => Promise<TOutput>;
  };
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
    runWorkflow: <
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
          handler: (ctx) => {
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
