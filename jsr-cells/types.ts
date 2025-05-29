import type { DatabaseSync } from "node:sqlite";

export interface DbAccessor {
  get db(): DatabaseSync;
}

export type Task = {
  scheduledTimeUnixMs: number;
} & TaskKind;

export type TaskKind = RetryWorkflowRun | ResumeAllPendingWorkflowRuns;

export type RetryWorkflowRun = {
  kind: "retry-workflow-run";
  workflowRunId: WorkflowRunId;
};

export type ResumeAllPendingWorkflowRuns = {
  kind: "resume-all-pending-workflow-runs";
};

export interface TaskScheduler {
  schedule(task: Task): Promise<void>;
}

export type WorkflowRunProgress = {
  id: WorkflowRunId;
  workflowName: string;
  dispatchedAt: Date;
  completedAt: Date | null;
  steps: {
    stepIndex: number;
    name: string;
    outputData: JSONValue;
    completedAt: Date;
  }[];
};

declare const __brand: unique symbol;
type Brand<T, TBrand> = T & { [__brand]: TBrand };

export type WorkflowRunId = Brand<string, "WorkflowRunId">;

export function workflowRunId(value: string): WorkflowRunId {
  return value as WorkflowRunId;
}

export type JSONPrimitive = string | number | boolean | null;
export type JSONValue =
  | JSONPrimitive
  | { [key: string]: JSONValue }
  | JSONValue[];

export interface WorkflowStep {
  run<StepOutput extends JSONValue>(
    name: string,
    fn: () => StepOutput | Promise<StepOutput>,
  ): Promise<StepOutput>;
}

export type WorkflowDefinition<
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
  }) => Promise<void> | void;
};
