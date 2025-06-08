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

export type ScheduledTaskId = Brand<string, "ScheduledTaskId">;

export function scheduledTaskId(value: string): ScheduledTaskId {
  return value as ScheduledTaskId;
}

export type JSONPrimitive = string | number | boolean | null;
export type JSONValue =
  | JSONPrimitive
  | { [key: string]: JSONValue }
  | JSONValue[];

export type Serializable = JSONValue | void;

export interface WorkflowStep {
  run<StepOutput extends JSONValue>(
    name: string,
    fn: () => StepOutput | Promise<StepOutput>,
  ): Promise<StepOutput>;

  // deno-lint-ignore no-explicit-any
  invoke<W extends WorkflowDef<WorkflowConfig<any, any>>>(
    workflow: W,
    input?: WorkflowInput<W> extends never ? undefined : WorkflowInput<W>,
  ): Promise<WorkflowOutput<W>>;
}

export interface EventWorkflowConfig<Input = void, Output = unknown> {
  event: string;
  handler: (ctx: WorkflowCtx<Input>) => Promise<Output>;
  retries?: number;
  concurrency?: number;
}

export type WorkflowConfig<Input = void, Output = unknown> =
  EventWorkflowConfig<
    Input,
    Output
  >;

// deno-lint-ignore no-explicit-any
export interface WorkflowDef<Config extends WorkflowConfig<any, any>> {
  readonly config: Config;
  readonly name: string;
}

export type WorkflowInput<W> = W extends WorkflowDef<infer C>
  ? C extends EventWorkflowConfig<infer I, unknown> ? I extends void ? never : I
  : never
  : never;

export type WorkflowOutput<W> = W extends WorkflowDef<infer C>
  ? C extends EventWorkflowConfig<unknown, infer O> ? O : never
  : never;

export type WorkflowCtx<Input = void> = {
  step: WorkflowStep;
  attempt: number;
} & (Input extends void ? Record<PropertyKey, never> : { input: Input });

export class WorkflowSuspendedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WorkflowSuspendedError";
  }
}
