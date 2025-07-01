import { DatabaseSync } from "node:sqlite";
import { WorkflowRuntime } from "./workflow.ts";
import { ulid } from "jsr:@std/ulid@1/ulid";
import { delay } from "jsr:@std/async@1/delay";
import {
  type DbAccessor,
  type JSONValue,
  scheduledTaskId,
  type TaskScheduler,
  type Unvoidable,
  type Voidable,
  type WorkflowCtx,
  type WorkflowDef,
  type WorkflowRegistry,
  type WorkflowRunId,
} from "./types.ts";

/**
 * Test environment for workflows.
 * Provides an isolated runtime for testing workflows with mocked steps and in-memory database.
 */
export class TestEnvironment implements Disposable {
  readonly #db: DatabaseSync;
  readonly #runtime: WorkflowRuntime;
  readonly #mockedSteps = new Map<string, () => Voidable<JSONValue>>();
  readonly #workflowWrappers = new Map<
    string,
    WorkflowDef<JSONValue, Voidable<JSONValue>>
  >();

  constructor(workflowRegistry?: WorkflowRegistry) {
    // Create in-memory database
    this.#db = new DatabaseSync(":memory:");

    // Create database accessor
    const dbAccessor: DbAccessor = {
      db: this.#db,
    };

    // Create mock task scheduler
    const taskScheduler = this.#createTaskScheduler();

    // Create workflow runtime
    this.#runtime = new WorkflowRuntime(dbAccessor, taskScheduler);

    // Initialize tables by calling listRuns
    this.#runtime.listRuns();

    // Register workflows from the provided registry with wrapping for mocking support
    if (workflowRegistry) {
      for (const [name, handler] of workflowRegistry) {
        // Create a wrapped handler that supports mocking
        const wrappedHandler = async (ctx: WorkflowCtx<JSONValue>) => {
          const originalStep = ctx.step;
          const mockedStep: typeof ctx.step = {
            run: async <StepOutput extends Voidable<JSONValue>>(
              stepName: string,
              fn: () => StepOutput | Promise<StepOutput>,
            ) => {
              const mockedHandler = this.#mockedSteps.get(stepName);
              if (mockedHandler) {
                const result = await mockedHandler();
                if (result === undefined) {
                  return null as Unvoidable<StepOutput>;
                } else {
                  return result as Unvoidable<StepOutput>;
                }
              }
              return originalStep.run(stepName, fn);
            },
            invoke: (workflow, input) => originalStep.invoke(workflow, input),
            sleep: (name, durationMs) => originalStep.sleep(name, durationMs),
          };
          // Call the original handler with mocked step
          const mockedCtx = { ...ctx, step: mockedStep };
          return await handler(mockedCtx);
        };

        this.#runtime.define<JSONValue, Voidable<JSONValue>>({
          name,
          handler: wrappedHandler,
        });
      }
    }
  }

  /**
   * Get the test database instance.
   */
  get db(): DatabaseSync {
    return this.#db;
  }

  /**
   * Mock a workflow step with a custom handler.
   * @param stepName - Name of the step to mock
   * @param handler - Function that returns the mocked result
   */
  mockStep<T extends Voidable<JSONValue>>(
    stepName: string,
    handler: () => T,
  ): void {
    this.#mockedSteps.set(stepName, handler);
  }

  /**
   * Run a workflow with mocked steps.
   * @param workflow - The workflow definition to run
   * @param input - Input data for the workflow
   * @returns Object with runId and waitForCompletion function
   */
  runWorkflow<TInput extends JSONValue, TOutput extends Voidable<JSONValue>>(
    workflow: WorkflowDef<TInput, TOutput>,
    input: TInput,
  ): {
    runId: WorkflowRunId;
    waitForCompletion: () => Promise<TOutput>;
  } {
    // For workflows from the registry, they're already wrapped, so dispatch by name
    // For other workflows, create a wrapper
    const registryWorkflows = this.#runtime.getWorkflowRegistry?.() ||
      new Map();
    if (registryWorkflows.has(workflow.name)) {
      // Workflow is already registered and wrapped, dispatch by name
      const runId = this.#runtime.dispatchByName(workflow.name, input);
      if (!runId) {
        throw new Error(`Workflow ${workflow.name} not found in runtime`);
      }
      return {
        runId,
        waitForCompletion: () => this.#waitForCompletion(runId),
      };
    }

    // Get or create a wrapper workflow that uses mocked steps
    let wrapperWorkflow = this.#workflowWrappers.get(workflow.name) as
      | WorkflowDef<TInput, TOutput>
      | undefined;

    if (!wrapperWorkflow) {
      wrapperWorkflow = this.#createWrapperWorkflow(workflow);
      this.#workflowWrappers.set(
        workflow.name,
        wrapperWorkflow as unknown as WorkflowDef<
          JSONValue,
          Voidable<JSONValue>
        >,
      );
    }

    const runId = this.#runtime.dispatch(wrapperWorkflow, input);

    return {
      runId,
      waitForCompletion: () => this.#waitForCompletion(runId),
    };
  }

  /**
   * Close the test environment and clean up resources.
   */
  close(): void {
    this.#db.close();
  }

  /**
   * Dispose of the test environment.
   * Called automatically when using the `using` syntax.
   */
  [Symbol.dispose](): void {
    this.close();
  }

  #createTaskScheduler(): TaskScheduler {
    // Create scheduled_tasks table
    this.#db.exec(`
      CREATE TABLE IF NOT EXISTS scheduled_tasks (
        id TEXT PRIMARY KEY NOT NULL,
        scheduled_time_unix_ms INTEGER NOT NULL,
        payload TEXT NOT NULL
      )
    `);

    return {
      schedule: (task) => {
        const id = scheduledTaskId(ulid());
        this.#db.prepare(`
          INSERT INTO scheduled_tasks (id, scheduled_time_unix_ms, payload) VALUES (?, ?, ?)
        `).run(id, task.scheduledTimeUnixMs, JSON.stringify(task));
        return id;
      },
    };
  }

  #createWrapperWorkflow<
    TInput extends JSONValue,
    TOutput extends Voidable<JSONValue>,
  >(
    workflow: WorkflowDef<TInput, TOutput>,
  ): WorkflowDef<TInput, TOutput> {
    return this.#runtime.define<TInput, TOutput>({
      name: workflow.name,
      handler: async (ctx) => {
        const originalStep = ctx.step;
        const mockedStep: typeof ctx.step = {
          run: async <StepOutput extends Voidable<JSONValue>>(
            name: string,
            fn: () => StepOutput | Promise<StepOutput>,
          ) => {
            const mockedHandler = this.#mockedSteps.get(name);
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
          invoke: (workflow, input) => originalStep.invoke(workflow, input),
          sleep: (name, durationMs) => originalStep.sleep(name, durationMs),
        };
        // Call the original workflow handler with mocked step
        const originalCtx = { ...ctx, step: mockedStep };
        return await workflow.config.handler(originalCtx);
      },
    });
  }

  async #waitForCompletion<TOutput>(
    runId: WorkflowRunId,
  ): Promise<TOutput> {
    const MAX_RETRIES = 50;
    const INTERVAL_MS = 100;

    for (let i = 0; i < MAX_RETRIES; i++) {
      const progress = this.#runtime.getRunProgress(runId);
      if (!progress) {
        throw new Error(`Workflow run not found: ${runId}`);
      }

      if (progress.completedAt !== null) {
        return progress.outputData as TOutput;
      }

      await delay(INTERVAL_MS);
    }

    throw new Error("Workflow did not complete within timeout");
  }
}
