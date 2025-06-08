import { ulid } from "jsr:@std/ulid@1/ulid";
import { assert } from "jsr:@std/assert@1/assert";
import {
  type DbAccessor,
  type EventWorkflowConfig,
  type JSONValue,
  type Serializable,
  type TaskScheduler,
  type WorkflowConfig,
  type WorkflowCtx,
  type WorkflowDef,
  type WorkflowInput,
  type WorkflowOutput,
  type WorkflowRunId,
  workflowRunId,
  type WorkflowRunProgress,
  type WorkflowStep,
  WorkflowSuspendedError,
} from "./types.ts";
import { fromJson, toJson } from "./serde.ts";

// Database row helper types (internal to workflow implementation)
interface WorkflowRunRow {
  id: string;
  workflow_name: string;
  input_data: string;
  output_data: string | null;
  dispatched_at: string;
  completed_at: string | null;
}

interface WorkflowStepRow {
  workflow_run_id: string;
  step_index: number;
  name: string;
  output_data: string;
  completed_at: string;
}

interface WorkflowInvocationRow {
  parent_run_id: string;
  step_index: number;
  child_run_id: string;
}

// Singleton pattern for global workflow runtime
let globalRuntime: WorkflowRuntime | null = null;

export function getRuntime(): WorkflowRuntime {
  if (!globalRuntime) {
    throw new Error("WorkflowRuntime not initialized");
  }
  return globalRuntime;
}

export function setRuntime(runtime: WorkflowRuntime): void {
  globalRuntime = runtime;
}

export class WorkflowRuntime {
  static #runningWorkflows = 0;

  #dbAccessor: DbAccessor;
  #taskScheduler: TaskScheduler;

  // Promise tracking for in-memory invocations
  #pendingInvocations = new Map<string, (output: JSONValue) => void>();

  // Store workflow definitions by name
  #workflows = new Map<
    string,
    (ctx: WorkflowCtx<JSONValue>) => Promise<JSONValue>
  >();

  static runningWorkflows(): number {
    return this.#runningWorkflows;
  }

  register<Input extends Serializable, Output extends Serializable>(
    name: string,
    handler: (ctx: WorkflowCtx<Input>) => Promise<Output>,
  ): void {
    this.#workflows.set(
      name,
      handler as unknown as (ctx: WorkflowCtx<JSONValue>) => Promise<JSONValue>,
    );
  }

  // Method to allow step.invoke to register pending promises
  registerPendingInvocation(
    runId: string,
    resolver: (output: JSONValue) => void,
  ): void {
    this.#pendingInvocations.set(runId, resolver);
  }

  constructor(dbAccessor: DbAccessor, taskScheduler: TaskScheduler) {
    this.#dbAccessor = dbAccessor;
    this.#taskScheduler = taskScheduler;

    this.#dbAccessor.db.exec(`
      CREATE TABLE IF NOT EXISTS workflow_runs (
        id TEXT PRIMARY KEY NOT NULL,
        workflow_name TEXT NOT NULL,
        input_data TEXT NOT NULL,
        output_data TEXT,
        dispatched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc')),
        completed_at TEXT
      );
    `);

    this.#dbAccessor.db.exec(`
      CREATE TABLE IF NOT EXISTS workflow_steps (
        workflow_run_id TEXT NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
        step_index INTEGER NOT NULL,
        name TEXT NOT NULL,
        output_data TEXT NOT NULL,
        completed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc')),
        UNIQUE(workflow_run_id, step_index)
      );
    `);

    this.#dbAccessor.db.exec(`
      CREATE TABLE IF NOT EXISTS workflow_invocations (
        parent_run_id TEXT NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
        step_index INTEGER NOT NULL,
        child_run_id TEXT NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
        PRIMARY KEY (parent_run_id, step_index)
      );
    `);
  }

  dispatchByName(
    workflowName: string,
    inputData: JSONValue,
  ): WorkflowRunId | null {
    const handler = this.#workflows.get(workflowName);
    if (!handler) {
      return null;
    }

    const runId = workflowRunId(ulid());
    this.#dbAccessor.db.prepare(
      `INSERT INTO workflow_runs (id, workflow_name, input_data) VALUES (?, ?, ?)`,
    ).run(runId, workflowName, JSON.stringify(inputData));

    const step = new WorkflowStepImpl(runId, this.#dbAccessor, this);

    this.#dispatchInner(
      handler,
      runId,
      workflowName.toString(),
      inputData,
      step,
    );

    return runId;
  }

  /**
   * Retry a workflow run.
   *
   * @param runId The ID of the workflow run to retry.
   * @returns `true` if the workflow run was retried, `false` otherwise.
   */
  retry(runId: WorkflowRunId): boolean {
    const runResult = this.#dbAccessor.db.prepare(
      `SELECT
        workflow_name,
        input_data,
        completed_at
      FROM workflow_runs
      WHERE id = ?
    `,
    ).get(runId);
    if (!runResult) {
      return false;
    }

    const workflowName = runResult.workflow_name as string;
    const inputData = JSON.parse(
      runResult.input_data as string,
    ) as JSONValue;

    const handler = this.#workflows.get(workflowName);
    if (!handler) {
      return false;
    }

    const step = new WorkflowStepImpl(runId, this.#dbAccessor, this);

    this.#dispatchInner(
      handler,
      runId,
      workflowName.toString(),
      inputData,
      step,
    );

    return true;
  }

  resumeAllPendingWorkflowRuns() {
    const pendingRunIds = this.#dbAccessor.db.prepare(
      `SELECT id FROM workflow_runs WHERE completed_at IS NULL`,
    ).all();

    for (const { id } of pendingRunIds) {
      this.retry(id as WorkflowRunId);
    }
  }

  async #dispatchInner(
    handler: (ctx: WorkflowCtx<JSONValue>) => Promise<JSONValue>,
    runId: WorkflowRunId,
    _workflowName: string,
    inputData: JSONValue,
    step: WorkflowStepImpl,
  ) {
    WorkflowRuntime.#runningWorkflows++;
    let suspended = false;
    let output: JSONValue | undefined = undefined;

    try {
      // Execute the workflow handler
      output = await handler({ input: inputData, step, attempt: 1 });

      // Store output in database
      this.#dbAccessor.db.prepare(
        `UPDATE workflow_runs SET output_data = ?, completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc') WHERE id = ?`,
      ).run(toJson(output), runId);

      // Notify any waiting parent (in-memory)
      const resolver = this.#pendingInvocations.get(runId);
      if (resolver) {
        resolver(output);
        this.#pendingInvocations.delete(runId);
      }

      // Check for parent workflows that need retry (crash recovery)
      const waitingParents = this.#dbAccessor.db.prepare(`
        SELECT DISTINCT parent_run_id FROM workflow_invocations WHERE child_run_id = ?
      `).all(runId);

      for (const { parent_run_id } of waitingParents) {
        this.retry(parent_run_id as WorkflowRunId);
      }
    } catch (e) {
      if (e instanceof WorkflowSuspendedError) {
        suspended = true;
        return;
      }
      console.error(e);
      // Schedule a retry in 1 second.
      this.#scheduleRetry(runId, Date.now() + 1000);
      return;
    } finally {
      WorkflowRuntime.#runningWorkflows--;
      // Clean up pending invocations if we're suspending
      if (suspended) {
        // The Promise will never resolve, but that's OK
        // On restart, we'll check the database
      }
    }
  }

  #scheduleRetry(runId: WorkflowRunId, scheduledTimeUnixMs: number) {
    this.#taskScheduler.schedule({
      kind: "retry-workflow-run",
      scheduledTimeUnixMs,
      workflowRunId: runId,
    });
  }

  getRunProgress(runId: WorkflowRunId): WorkflowRunProgress | null {
    // Retrieve the workflow run joined with its workflow steps from DB.
    const runResult = this.#dbAccessor.db.prepare(`
      SELECT
        id,
        workflow_name,
        dispatched_at,
        completed_at
      FROM workflow_runs
      WHERE id = ?
    `).get(runId);
    if (!runResult) {
      return null;
    }

    const stepsResult = this.#dbAccessor.db.prepare(`
      SELECT
        step_index,
        name,
        output_data,
        completed_at
      FROM workflow_steps
      WHERE workflow_run_id = ?
      ORDER BY step_index ASC
    `).all(runId);

    return {
      id: workflowRunId(runResult.id as string),
      workflowName: runResult.workflow_name as string,
      dispatchedAt: new Date(runResult.dispatched_at as string),
      completedAt: runResult.completed_at
        ? new Date(runResult.completed_at as string)
        : null,
      steps: stepsResult.map((row) => {
        return {
          stepIndex: row.step_index as number,
          name: row.name as string,
          outputData: fromJson(row.output_data as string) as JSONValue,
          completedAt: new Date(row.completed_at as string),
        };
      }),
    };
  }

  listRuns(options?: {
    workflowName?: string;
    status?: "pending" | "completed";
    limit?: number;
  }): WorkflowRunProgress[] {
    let sql = `
      SELECT
        id,
        workflow_name,
        dispatched_at,
        completed_at
      FROM workflow_runs
    `;

    const conditions: string[] = [];
    const params: (string | number)[] = [];

    if (options?.workflowName) {
      conditions.push("workflow_name = ?");
      params.push(options.workflowName);
    }

    if (options?.status === "pending") {
      conditions.push("completed_at IS NULL");
    } else if (options?.status === "completed") {
      conditions.push("completed_at IS NOT NULL");
    }

    if (conditions.length > 0) {
      sql += ` WHERE ${conditions.join(" AND ")}`;
    }

    sql += " ORDER BY dispatched_at DESC";

    if (options?.limit) {
      sql += " LIMIT ?";
      params.push(options.limit);
    }

    const runResults = this.#dbAccessor.db.prepare(sql).all(...params);

    return runResults.map((runResult) => {
      const stepsResult = this.#dbAccessor.db.prepare(`
        SELECT
          step_index,
          name,
          output_data,
          completed_at
        FROM workflow_steps
        WHERE workflow_run_id = ?
        ORDER BY step_index ASC
      `).all(runResult.id);

      return {
        id: workflowRunId(runResult.id as string),
        workflowName: runResult.workflow_name as string,
        dispatchedAt: new Date(runResult.dispatched_at as string),
        completedAt: runResult.completed_at
          ? new Date(runResult.completed_at as string)
          : null,
        steps: stepsResult.map((row) => {
          return {
            stepIndex: row.step_index as number,
            name: row.name as string,
            outputData: fromJson(row.output_data as string) as JSONValue,
            completedAt: new Date(row.completed_at as string),
          };
        }),
      };
    });
  }
}

class WorkflowStepImpl implements WorkflowStep {
  #currentIndex: number;
  #runId: WorkflowRunId;
  #dbAccessor: DbAccessor;
  #runtime: WorkflowRuntime;

  constructor(
    runId: WorkflowRunId,
    dbAccessor: DbAccessor,
    runtime: WorkflowRuntime,
  ) {
    // Start at 0, increment before each step operation
    // This ensures step indices are 1-based in the database
    this.#currentIndex = 0;
    this.#runId = runId;
    this.#dbAccessor = dbAccessor;
    this.#runtime = runtime;
  }

  async run<StepOutput extends JSONValue>(
    name: string,
    fn: () => StepOutput | Promise<StepOutput>,
  ): Promise<StepOutput> {
    // TODO(magurotuna): We solely rely on the order of steps executed to
    // retrieve memoized results. This scheme would not work if the order is not
    // guaranteed, for instance:
    //
    // ```
    // if (Math.random() > 0.5) {
    //   await step.run("if-branch", () => { /* do something */ });
    // } else {
    //   await step.run("else-branch", () => { /* do something */ });
    // }
    // ```
    //
    // This issue is very similar to React Hooks, and they force users not to
    // use hooks in conditionals. We may want to consider a similar approach.
    this.#currentIndex++;

    // Check if the step for this run was already executed.
    // If it was, return the result from the DB.

    const memoizedResult = this.#dbAccessor.db.prepare(
      `SELECT output_data FROM workflow_steps WHERE workflow_run_id = ? AND step_index = ?`,
    )
      .get(this.#runId, this.#currentIndex);
    if (memoizedResult) {
      assert(typeof memoizedResult.output_data === "string");
      return fromJson(memoizedResult.output_data) as StepOutput;
    }

    // Otherwise, run the provided function and store the result in the DB.

    // If this function throws an error, that is bubbled up to the workflow
    // handler, caught there, and then the retry is scheduled.
    const result = await fn();

    this.#dbAccessor.db.prepare(
      `INSERT INTO workflow_steps (workflow_run_id, step_index, name, output_data) VALUES (?, ?, ?, ?)`,
    )
      .run(this.#runId, this.#currentIndex, name, toJson(result));

    return result;
  }

  // deno-lint-ignore no-explicit-any
  async invoke<W extends WorkflowDef<WorkflowConfig<any, any>>>(
    workflow: W,
    ...args: WorkflowInput<W> extends never ? [] : [WorkflowInput<W>]
  ): Promise<WorkflowOutput<W>> {
    this.#currentIndex++;

    const input = args.length > 0 ? args[0] : null;

    const memoized = this.#dbAccessor.db.prepare(
      `SELECT output_data FROM workflow_steps WHERE workflow_run_id = ? AND step_index = ?`,
    ).get(this.#runId, this.#currentIndex);

    if (memoized) {
      return fromJson(memoized.output_data as string) as WorkflowOutput<W>;
    }

    const existing = this.#dbAccessor.db.prepare(`
      SELECT r.completed_at, r.output_data
      FROM workflow_invocations i
      JOIN workflow_runs r ON i.child_run_id = r.id
      WHERE i.parent_run_id = ? AND i.step_index = ?
    `).get(this.#runId, this.#currentIndex);

    if (existing) {
      if (existing.completed_at) {
        // Child finished while we were down - store and return
        const output = fromJson(existing.output_data as string);
        this.#storeStepResult(
          `invoke:${workflow.name}`,
          output as JSONValue,
        );
        return output as WorkflowOutput<W>;
      }
      // Child still running - suspend parent
      throw new WorkflowSuspendedError("Child workflow pending");
    }

    // 3. First invocation - dispatch child
    const childRunId = this.#runtime.dispatchByName(
      workflow.name,
      input as JSONValue,
    );
    if (!childRunId) {
      throw new Error(`Workflow ${workflow.name} not found`);
    }

    // Record parent-child relationship
    this.#dbAccessor.db.prepare(`
      INSERT INTO workflow_invocations (parent_run_id, step_index, child_run_id)
      VALUES (?, ?, ?)
    `).run(this.#runId, this.#currentIndex, childRunId);

    // 4. Wait for completion using promise (efficient path)
    return await new Promise<WorkflowOutput<W>>((resolve) => {
      this.#runtime.registerPendingInvocation(
        childRunId,
        (output: JSONValue) => {
          this.#storeStepResult(`invoke:${workflow.name}`, output);
          resolve(output as WorkflowOutput<W>);
        },
      );
    });
  }

  #storeStepResult(name: string, result: JSONValue): void {
    this.#dbAccessor.db.prepare(
      `INSERT INTO workflow_steps (workflow_run_id, step_index, name, output_data) VALUES (?, ?, ?, ?)`,
    ).run(this.#runId, this.#currentIndex, name, toJson(result));
  }

  // TODO: add more methods like sleep, sleepUntil, etc.
}

// deno-lint-ignore no-explicit-any
export function define<Input = void, Output = any>(
  config: EventWorkflowConfig<Input, Output>,
): WorkflowDef<WorkflowConfig<Input, Output>> {
  const runtime = getRuntime();

  // Wrapper to convert from new API to internal format
  const wrappedHandler = async (
    ctx: WorkflowCtx<JSONValue>,
  ) => {
    if (ctx.input === null || ctx.input === undefined) {
      return await config.handler(
        { step: ctx.step, attempt: ctx.attempt } as WorkflowCtx<Input>,
      );
    } else {
      return await config.handler(
        { ...ctx, input: ctx.input as Input } as unknown as WorkflowCtx<Input>,
      );
    }
  };

  // deno-lint-ignore no-explicit-any
  runtime.register(config.event, wrappedHandler as any);
  return { config, name: config.event };
}

// deno-lint-ignore no-explicit-any
export function dispatch<W extends WorkflowDef<WorkflowConfig<any, any>>>(
  workflow: W,
  ...args: WorkflowInput<W> extends never ? [] : [WorkflowInput<W>]
): WorkflowRunId {
  const runtime = getRuntime();
  const input = args.length > 0 ? args[0] : null;
  const runId = runtime.dispatchByName(workflow.name, input as JSONValue);
  if (!runId) {
    throw new Error(`Workflow ${workflow.name} not found`);
  }
  return runId;
}

export function getRunProgress(
  runId: WorkflowRunId,
): WorkflowRunProgress | null {
  return getRuntime().getRunProgress(runId);
}

export function listRuns(options?: {
  workflowName?: string;
  status?: "pending" | "completed";
  limit?: number;
}): WorkflowRunProgress[] {
  return getRuntime().listRuns(options);
}
