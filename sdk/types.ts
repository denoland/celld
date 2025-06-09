import type { DatabaseSync } from "node:sqlite";

export interface DbAccessor {
  get db(): DatabaseSync;
}

export type Task = {
  scheduledTimeUnixMs: number;
} & TaskKind;

export type TaskKind =
  | UserDefinedAlarm
  | RetryWorkflowRun
  | ResumeAllPendingWorkflowRuns;

export type UserDefinedAlarm = {
  kind: "user-defined-alarm";
};

export type RetryWorkflowRun = {
  kind: "retry-workflow-run";
  workflowRunId: WorkflowRunId;
};

export type ResumeAllPendingWorkflowRuns = {
  kind: "resume-all-pending-workflow-runs";
};

export interface TaskScheduler {
  schedule(task: Task): Promise<ScheduledTaskId> | ScheduledTaskId;
}

export type WorkflowRunProgress = {
  id: WorkflowRunId;
  workflowName: string;
  outputData: JSONValue | null;
  dispatchedAt: Date;
  completedAt: Date | null;
  steps: {
    stepIndex: number;
    name: string;
    outputData: JSONValue;
    stepType?: string;
    invokedWorkflowRunId?: string | null;
    completedAt: Date;
  }[];
};

declare const __brand: unique symbol;
type Brand<T, TBrand> = T & { [__brand]: TBrand };

export type WorkflowRunId = Brand<string, "WorkflowRunId">;

export function workflowRunId(value: string): WorkflowRunId {
  return value as WorkflowRunId;
}

export type ScheduledTaskId = Brand<string, "ScheduledTaskId">;

export function scheduledTaskId(value: string): ScheduledTaskId {
  return value as ScheduledTaskId;
}

export type JSONPrimitive = string | number | boolean | null;
export type JSONValue =
  | JSONPrimitive
  | { [key: string]: JSONValue }
  | JSONValue[];

export type Voidable<T> = T | void;
export type Unvoidable<T> = T extends void ? null : T;

export interface WorkflowStep {
  run<StepOutput extends Voidable<JSONValue>>(
    name: string,
    fn: () => StepOutput | Promise<StepOutput>,
  ): Promise<Unvoidable<StepOutput>>;

  invoke<
    Input extends JSONValue,
    Output extends Voidable<JSONValue>,
  >(
    workflow: WorkflowDef<Input, Output>,
    input?: Input,
  ): Promise<Output>;
}

export interface WorkflowConfig<
  Input extends JSONValue = null,
  Output extends Voidable<JSONValue> = null,
> {
  name: string;
  handler: (ctx: WorkflowCtx<Input>) => Promise<Output> | Output;
  retries?: number;
  concurrency?: number;
}

export interface WorkflowDef<
  Input extends JSONValue,
  Output extends Voidable<JSONValue>,
> {
  readonly config: WorkflowConfig<Input, Output>;
  readonly name: string;
}

export type WorkflowCtx<Input = void> = {
  step: WorkflowStep;
  attempt: number;
} & (Input extends void ? Record<PropertyKey, never> : { input: Input });
