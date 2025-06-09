import { ulid } from "jsr:@std/ulid@1/ulid";
import { assert } from "jsr:@std/assert@1/assert";
import {
  type DbAccessor,
  type JSONValue,
  type TaskScheduler,
  type Unvoidable,
  type Voidable,
  type WorkflowConfig,
  type WorkflowCtx,
  type WorkflowDef,
  type WorkflowRunId,
  workflowRunId,
  type WorkflowRunProgress,
  type WorkflowStep,
} from "./types.ts";

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
    (ctx: WorkflowCtx<JSONValue>) => Promise<Voidable<JSONValue>>
  >();

  static runningWorkflows(): number {
    return this.#runningWorkflows;
  }

  register<Input extends JSONValue, Output extends Voidable<JSONValue>>(
    name: string,
    handler: (ctx: WorkflowCtx<Input>) => Promise<Output>,
  ): void {
    this.#dbAccessor.db.prepare(
      `INSERT OR IGNORE INTO workflows (name) VALUES (?)`,
    ).run(name);
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
      CREATE TABLE IF NOT EXISTS workflows (
        name TEXT PRIMARY KEY NOT NULL,
        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc'))
      );
    `);

    this.#dbAccessor.db.exec(`
      CREATE TABLE IF NOT EXISTS workflow_runs (
        id TEXT PRIMARY KEY NOT NULL,
        workflow_name TEXT NOT NULL REFERENCES workflows(name) ON DELETE CASCADE,
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
        -- For 'invoke' steps, this is set when the child workflow completes.
        -- For 'run' steps, this should always be present because a record is created only after the step has completed.
        output_data TEXT,
        step_type TEXT NOT NULL,
        -- For 'invoke' steps, this is set when the child workflow is dispatched.
        -- For 'run' steps, this is always null.
        invoked_workflow_run_id TEXT REFERENCES workflow_runs(id) ON DELETE CASCADE,
        -- For 'invoke' steps, this is set when the child workflow completes.
        -- For 'run' steps, this should always be present because a record is created only after the step has completed.
        completed_at TEXT,
        UNIQUE(workflow_run_id, step_index)
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
    const inputData = fromJson(runResult.input_data as string);

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
    handler: (ctx: WorkflowCtx<JSONValue>) => Promise<Voidable<JSONValue>>,
    runId: WorkflowRunId,
    _workflowName: string,
    inputData: JSONValue,
    step: WorkflowStepImpl,
  ) {
    WorkflowRuntime.#runningWorkflows++;
    try {
      // Execute the workflow handler
      const output = await handler({ input: inputData, step, attempt: 1 });

      // Store output in database
      this.#dbAccessor.db.prepare(
        `UPDATE workflow_runs SET output_data = ?, completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc') WHERE id = ?`,
      ).run(toJson(output), runId);

      // Notify any waiting parent (in-memory)
      const resolver = this.#pendingInvocations.get(runId);
      if (resolver) {
        // handler's return type may be `void`, which becomes `undefined` at runtime.
        // In that case, we pass `null` to the resolver as a valid JSON value.
        if (output === undefined) {
          resolver(null);
        } else {
          resolver(output);
        }
        this.#pendingInvocations.delete(runId);
      }

      // Check for parent workflows that need retry (crash recovery)
      const waitingParents = this.#dbAccessor.db.prepare(`
        SELECT DISTINCT workflow_run_id
        FROM workflow_steps
        WHERE invoked_workflow_run_id = ? AND step_type = 'invoke' AND output_data IS NULL
      `).all(runId);

      for (const { workflow_run_id } of waitingParents) {
        this.retry(workflow_run_id as WorkflowRunId);
      }
    } catch (e) {
      console.error(e);
      // Schedule a retry in 1 second.
      this.#scheduleRetry(runId, Date.now() + 1000);
      return;
    } finally {
      WorkflowRuntime.#runningWorkflows--;
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
        output_data,
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
        step_type,
        invoked_workflow_run_id,
        completed_at
      FROM workflow_steps
      WHERE workflow_run_id = ?
      ORDER BY step_index ASC
    `).all(runId);

    return {
      id: workflowRunId(runResult.id as string),
      workflowName: runResult.workflow_name as string,
      outputData: fromJson(runResult.output_data as string | null),
      dispatchedAt: new Date(runResult.dispatched_at as string),
      completedAt: runResult.completed_at
        ? new Date(runResult.completed_at as string)
        : null,
      steps: stepsResult.map((row) => {
        return {
          stepIndex: row.step_index as number,
          name: row.name as string,
          outputData: fromJson(row.output_data as string | null),
          stepType: row.step_type as string,
          invokedWorkflowRunId: row.invoked_workflow_run_id as string | null,
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
        output_data,
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
          step_type,
          invoked_workflow_run_id,
          completed_at
        FROM workflow_steps
        WHERE workflow_run_id = ?
        ORDER BY step_index ASC
      `).all(runResult.id);

      return {
        id: workflowRunId(runResult.id as string),
        workflowName: runResult.workflow_name as string,
        outputData: fromJson(runResult.output_data as string | null),
        dispatchedAt: new Date(runResult.dispatched_at as string),
        completedAt: runResult.completed_at
          ? new Date(runResult.completed_at as string)
          : null,
        steps: stepsResult.map((row) => {
          return {
            stepIndex: row.step_index as number,
            name: row.name as string,
            outputData: fromJson(row.output_data as string | null),
            stepType: row.step_type as string,
            invokedWorkflowRunId: row.invoked_workflow_run_id as string | null,
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

  async run<StepOutput extends Voidable<JSONValue>>(
    name: string,
    fn: () => StepOutput | Promise<StepOutput>,
  ): Promise<Unvoidable<StepOutput>> {
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
      return fromJson(memoizedResult.output_data) as Unvoidable<StepOutput>;
    }

    // Otherwise, run the provided function and store the result in the DB.

    // If this function throws an error, that is bubbled up to the workflow
    // handler, caught there, and then the retry is scheduled.
    const result = await fn();

    this.#dbAccessor.db.prepare(
      `INSERT INTO workflow_steps (workflow_run_id, step_index, name, output_data, step_type) VALUES (?, ?, ?, ?, 'run')`,
    )
      .run(this.#runId, this.#currentIndex, name, toJson(result));

    // step handler's return type may be `void`, which becomes `undefined` at runtime.
    // In that case, we return `null` to the caller as a valid JSON value.
    if (result === undefined) {
      return null as Unvoidable<StepOutput>;
    }

    return result as Unvoidable<StepOutput>;
  }

  async invoke<
    Input extends JSONValue,
    Output extends Voidable<JSONValue>,
  >(
    workflow: WorkflowDef<Input, Output>,
    input?: Input,
  ): Promise<Output> {
    this.#currentIndex++;
    const inputData = input ?? null;

    // Check if we already have this invoke step with a result
    const existingStep = this.#dbAccessor.db.prepare(`
      SELECT output_data, invoked_workflow_run_id, step_type, completed_at
      FROM workflow_steps
      WHERE workflow_run_id = ? AND step_index = ?
    `).get(this.#runId, this.#currentIndex);

    // If we have a completed invoke step, return the cached result
    if (existingStep?.completed_at && existingStep.step_type === "invoke") {
      assert(
        typeof existingStep.output_data === "string",
        "output_data should be a string if the invoke step is completed",
      );
      return fromJson(existingStep.output_data) as Output;
    }

    // If we have an invoke step but no result yet, check child status
    if (
      existingStep?.invoked_workflow_run_id &&
      existingStep.step_type === "invoke"
    ) {
      const childRun = this.#dbAccessor.db.prepare(`
        SELECT output_data, completed_at FROM workflow_runs WHERE id = ?
      `).get(existingStep.invoked_workflow_run_id);

      if (childRun?.completed_at) {
        // Child completed while parent was down - update step and return

        // The child run record should have a non-null string value in
        // output_data field at this point because it's completed.
        assert(typeof childRun.output_data === "string");
        const output = fromJson(childRun.output_data);
        this.#updateInvokeStepResult(output);
        return output as Output;
      } else {
        // Child still running - wait for it using Promise
        return await new Promise<Output>((resolve) => {
          this.#runtime.registerPendingInvocation(
            existingStep.invoked_workflow_run_id as string,
            (output: JSONValue) => {
              this.#updateInvokeStepResult(output);
              resolve(output as Output);
            },
          );
        });
      }
    }

    // First invocation - dispatch child workflow
    const childRunId = this.#runtime.dispatchByName(
      workflow.name,
      inputData,
    );
    if (!childRunId) {
      throw new Error(`Workflow ${workflow.name} not found`);
    }

    // Store the invoke step (without result yet)
    this.#storeInvokeStep(`invoke:${workflow.name}`, childRunId);

    // Wait for child completion
    return await new Promise<Output>((resolve) => {
      this.#runtime.registerPendingInvocation(
        childRunId,
        (output: JSONValue) => {
          this.#updateInvokeStepResult(output);
          resolve(output as Output);
        },
      );
    });
  }

  #storeStepResult(
    name: string,
    result: JSONValue,
    stepType: string = "run",
    invokedRunId?: string,
  ): void {
    if (stepType === "invoke" && invokedRunId) {
      this.#dbAccessor.db.prepare(
        `INSERT INTO workflow_steps (workflow_run_id, step_index, name, output_data, step_type, invoked_workflow_run_id)
         VALUES (?, ?, ?, ?, ?, ?)`,
      ).run(
        this.#runId,
        this.#currentIndex,
        name,
        toJson(result),
        stepType,
        invokedRunId,
      );
    } else {
      this.#dbAccessor.db.prepare(
        `INSERT INTO workflow_steps (workflow_run_id, step_index, name, output_data, step_type)
         VALUES (?, ?, ?, ?, ?)`,
      ).run(
        this.#runId,
        this.#currentIndex,
        name,
        toJson(result),
        stepType,
      );
    }
  }

  #storeInvokeStep(name: string, invokedRunId: string): void {
    this.#dbAccessor.db.prepare(
      `INSERT INTO workflow_steps (workflow_run_id, step_index, name, step_type, invoked_workflow_run_id)
       VALUES (?, ?, ?, 'invoke', ?)`,
    ).run(this.#runId, this.#currentIndex, name, invokedRunId);
  }

  #updateInvokeStepResult(result: JSONValue): void {
    this.#dbAccessor.db.prepare(
      `UPDATE workflow_steps SET output_data = ?, completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc')
       WHERE workflow_run_id = ? AND step_index = ?`,
    ).run(toJson(result), this.#runId, this.#currentIndex);
  }

  // TODO: add more methods like sleep, sleepUntil, etc.
}

export function define<
  Input extends JSONValue,
  Output extends Voidable<JSONValue>,
>(
  config: WorkflowConfig<Input, Output>,
): WorkflowDef<Input, Output> {
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

  runtime.register(config.name, wrappedHandler);
  return { config, name: config.name };
}

export function dispatch<
  Input extends JSONValue,
  Output extends Voidable<JSONValue>,
>(
  workflow: WorkflowDef<Input, Output>,
  input?: Input,
): WorkflowRunId {
  const runtime = getRuntime();
  const inputData = input ?? null;
  const runId = runtime.dispatchByName(workflow.name, inputData);
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

/** Safe JSON serialization that handles void (which is undefined at runtime) */
function toJson(v: Voidable<JSONValue>): string {
  return v === undefined ? "null" : JSON.stringify(v);
}

/** Safe JSON deserialization that handles null as well as string */
function fromJson(json: string | null): JSONValue {
  if (json === null) {
    return null;
  }

  return JSON.parse(json);
}
