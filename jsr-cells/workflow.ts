import { ulid } from "jsr:@std/ulid@1/ulid";
import { assert } from "jsr:@std/assert@1/assert";
import {
  type DbAccessor,
  type JSONValue,
  type TaskScheduler,
  type WorkflowDefinition,
  type WorkflowRunId,
  workflowRunId,
  type WorkflowRunProgress,
  type WorkflowStep,
} from "./types.ts";

export class Workflow<WorkflowInputs extends Record<string, JSONValue>> {
  static #runningWorkflows = 0;

  #dbAccessor: DbAccessor;
  #taskScheduler: TaskScheduler;

  #handlers: {
    [K in keyof WorkflowInputs]?: WorkflowDefinition<
      WorkflowInputs,
      K
    >["handler"];
  } = {};

  static runningWorkflows(): number {
    return this.#runningWorkflows;
  }

  constructor(dbAccessor: DbAccessor, taskScheduler: TaskScheduler) {
    this.#dbAccessor = dbAccessor;
    this.#taskScheduler = taskScheduler;

    // Create tables if not exist yet
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
  }

  define<WorkflowName extends keyof WorkflowInputs>(
    definition: WorkflowDefinition<WorkflowInputs, WorkflowName>,
  ) {
    this.#dbAccessor.db.prepare(
      `INSERT OR IGNORE INTO workflows (name) VALUES (?)`,
    ).run(definition.name.toString());
    this.#handlers[definition.name] = definition.handler;
  }

  dispatch<WorkflowName extends keyof WorkflowInputs>(
    workflowName: WorkflowName,
    inputData: WorkflowInputs[WorkflowName],
  ): WorkflowRunId | null {
    const handler = this.#handlers[workflowName];
    if (!handler) {
      return null;
    }

    const runId = workflowRunId(ulid());
    // Insert a new workflow run.
    this.#dbAccessor.db.prepare(
      `INSERT INTO workflow_runs (id, workflow_name, input_data) VALUES (?, ?, ?)`,
    ).run(runId, workflowName.toString(), JSON.stringify(inputData));

    const step = new WorkflowStepImpl(runId, this.#dbAccessor);

    // Dispatch a new workflow run as a fire-and-forget promise.
    this.#dispatchInner(handler, runId, workflowName, inputData, step);

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
    ) as WorkflowInputs[string];

    const handler = this.#handlers[workflowName];
    if (!handler) {
      return false;
    }

    const step = new WorkflowStepImpl(runId, this.#dbAccessor);

    // Dispatch a new workflow run as a fire-and-forget promise.
    this.#dispatchInner(handler, runId, workflowName, inputData, step);

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

  async #dispatchInner<WorkflowName extends keyof WorkflowInputs>(
    handler: WorkflowDefinition<WorkflowInputs, WorkflowName>["handler"],
    runId: WorkflowRunId,
    workflowName: WorkflowName,
    inputData: WorkflowInputs[WorkflowName],
    step: WorkflowStepImpl,
  ) {
    Workflow.#runningWorkflows++;

    try {
      await handler({
        event: {
          id: runId,
          name: workflowName,
          data: inputData,
        },
        step,
        attempt: 1,
      });
    } catch (e) {
      console.error(e);
      // Schedule a retry in 1 second.
      this.#scheduleRetry(runId, Date.now() + 1000);
      return;
    } finally {
      Workflow.#runningWorkflows--;
    }

    // Mark the workflow run as completed.
    this.#dbAccessor.db.prepare(
      `UPDATE workflow_runs SET completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc') WHERE id = ?`,
    ).run(runId);
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
          outputData: JSON.parse(row.output_data as string),
          completedAt: new Date(row.completed_at as string),
        };
      }),
    };
  }
}

class WorkflowStepImpl implements WorkflowStep {
  #currentIndex: number;
  #runId: WorkflowRunId;
  #dbAccessor: DbAccessor;

  constructor(runId: WorkflowRunId, dbAccessor: DbAccessor) {
    this.#currentIndex = 0;
    this.#runId = runId;
    this.#dbAccessor = dbAccessor;
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
      return JSON.parse(memoizedResult.output_data) as StepOutput;
    }

    // Otherwise, run the provided function and store the result in the DB.

    // If this function throws an error, that is bubbled up to the workflow
    // handler, caught there, and then the retry is scheduled.
    const result = await fn();

    this.#dbAccessor.db.prepare(
      `INSERT INTO workflow_steps (workflow_run_id, step_index, name, output_data) VALUES (?, ?, ?, ?)`,
    )
      .run(this.#runId, this.#currentIndex, name, JSON.stringify(result));

    return result;
  }

  // TODO: add more methods like sleep, sleepUntil, invoke, etc.
}
