import { cell } from "./cell.ts";
import { ulid } from "@std/ulid";
import { assert } from "@std/assert";

export class Workflow<WorkflowInputs extends Record<string, JSONValue>> {
  static #runningWorkflows = 0;

  #handlers: {
    [K in keyof WorkflowInputs]?: WorkflowDefinition<
      WorkflowInputs,
      K
    >["handler"];
  } = {};

  constructor() {
    // Create tables if not exist yet
    cell.db.exec(`
      CREATE TABLE IF NOT EXISTS workflows (
        name TEXT PRIMARY KEY NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now', 'utc'))
      );
    `);

    cell.db.exec(`
      CREATE TABLE IF NOT EXISTS workflow_runs (
        id TEXT PRIMARY KEY NOT NULL,
        workflow_name TEXT NOT NULL REFERENCES workflows(name) ON DELETE CASCADE,
        input_data TEXT NOT NULL,
        dispatched_at TEXT NOT NULL DEFAULT (datetime('now', 'utc'))
        completed_at TEXT
      );
    `);

    cell.db.exec(`
      CREATE TABLE IF NOT EXISTS workflow_steps (
        workflow_run_id TEXT NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
        index INTEGER NOT NULL,
        name TEXT NOT NULL,
        output_data TEXT NOT NULL,
        completed_at TEXT NOT NULL DEFAULT (datetime('now', 'utc')),
        UNIQUE(workflow_run_id, index)
      );
    `);
  }

  define<WorkflowName extends keyof WorkflowInputs>(
    definition: WorkflowDefinition<WorkflowInputs, WorkflowName>,
  ) {
    cell.db.prepare(
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
    cell.db.prepare(
      `INSERT INTO workflow_runs (id, workflow_name, input_data) VALUES (?, ?, ?)`,
    ).run(runId, workflowName.toString(), JSON.stringify(inputData));

    const step = new WorkflowStep(runId);

    Workflow.#runningWorkflows++;

    // Dispatch a new workflow run as a fire-and-forget promise.
    handler({
      event: {
        id: runId,
        name: workflowName,
        data: inputData,
      },
      step,
      attempt: 1,
    }).then(() => {
      Workflow.#runningWorkflows--;
      // Mark the workflow run as completed.
      cell.db.prepare(
        `UPDATE workflow_runs SET completed_at = datetime('now', 'utc') WHERE id = ?`,
      ).run(runId);
    }).catch(() => {
      Workflow.#runningWorkflows--;
      // TODO(magurotuna): schedule a retry
    });

    return runId;
  }

  getRunProgress(runId: WorkflowRunId): WorkflowRunProgress | null {
    // Retrieve the workflow run joined with its workflow steps from DB.
    const runResult = cell.db.prepare(`
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

    const stepsResult = cell.db.prepare(`
      SELECT
        index,
        name,
        completed_at
      FROM workflow_steps
      WHERE workflow_run_id = ?
      ORDER BY index ASC
    `).all(runId);

    return {
      id: workflowRunId(runResult.id as string),
      workflowName: runResult.workflow_name as string,
      dispatchedAt: parseUTCDateTime(runResult.dispatched_at as string),
      completedAt: runResult.completed_at
        ? parseUTCDateTime(runResult.completed_at as string)
        : null,
      steps: stepsResult.map((row) => {
        return {
          index: row.index as number,
          name: row.name as string,
          completedAt: parseUTCDateTime(row.completed_at as string),
        };
      }),
    };
  }
}

class WorkflowStep {
  #currentIndex: number;
  #runId: WorkflowRunId;

  constructor(runId: WorkflowRunId) {
    this.#currentIndex = 0;
    this.#runId = runId;
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

    const memoizedResult = cell.db.prepare(
      `SELECT output_data FROM workflow_steps WHERE workflow_run_id = ? AND index = ?`,
    )
      .get(this.#runId, this.#currentIndex);
    if (memoizedResult) {
      assert(typeof memoizedResult.output_data === "string");
      return JSON.parse(memoizedResult.output_data) as StepOutput;
    }

    // Otherwise, run the provided function and store the result in the DB.
    // TODO(magurotuna): do error handling
    const result = await fn();
    cell.db.prepare(
      `INSERT INTO workflow_steps (workflow_run_id, index, name, output_data) VALUES (?, ?, ?, ?)`,
    )
      .run(this.#runId, this.#currentIndex, name, JSON.stringify(result));

    return result;
  }

  // TODO: add more methods like sleep, sleepUntil, invoke, etc.
}

function parseUTCDateTime(utcString: string): Date {
  return new Date(utcString + "Z");
}

type WorkflowRunProgress = {
  id: WorkflowRunId;
  workflowName: string;
  dispatchedAt: Date;
  completedAt: Date | null;
  steps: { index: number; name: string; completedAt: Date }[];
};

declare const __brand: unique symbol;
type Brand<T, TBrand> = T & { [__brand]: TBrand };

type WorkflowRunId = Brand<string, "WorkflowRunId">;

function workflowRunId(value: string): WorkflowRunId {
  return value as WorkflowRunId;
}

type JSONPrimitive = string | number | boolean | null;
type JSONValue =
  | JSONPrimitive
  | { [key: string]: JSONValue }
  | JSONValue[];

type WorkflowDefinition<
  WorkflowInputs extends Record<string, JSONValue>,
  WorkflowName extends keyof WorkflowInputs,
> = {
  name: WorkflowName;
  handler: (ctx: {
    event: {
      id: WorkflowRunId;
      name: WorkflowName;
      data: WorkflowInputs[WorkflowName];
    };
    step: WorkflowStep;
    attempt: number;
  }) => Promise<void>;
};
