import { DatabaseSync } from "node:sqlite";
import {
  type DbAccessor,
  type JSONValue,
  Workflow,
  type WorkflowRunId,
  type WorkflowRunProgress,
} from "./workflow.ts";
import { assertType, type IsExact } from "@std/testing/types";
import { assertEquals, assertExists } from "@std/assert";
import { delay } from "@std/async";

function generateTempDbAccessor(): DbAccessor {
  const db = new DatabaseSync(":memory:");
  return {
    get db() {
      return db;
    },
  };
}

async function waitUntilWorkflowRunCompleted<
  T extends Record<string, JSONValue>,
>(
  workflow: Workflow<T>,
  runId: WorkflowRunId,
): Promise<WorkflowRunProgress> {
  while (true) {
    const progress = workflow.getRunProgress(runId);
    if (progress?.completedAt) {
      return progress;
    }
    await delay(100);
  }
}

Deno.test("Workflow type", () => {
  type MyWorkflow = {
    "user.signup": {
      userId: string;
      email: string;
    };
    "user.login": {
      userId: string;
    };
  };

  const workflow = new Workflow<MyWorkflow>(generateTempDbAccessor());

  // workflow.define should accept only "user.signup" or "user.login"
  type WorkflowNames = Parameters<typeof workflow.define>[0]["name"];
  assertType<IsExact<WorkflowNames, "user.signup" | "user.login">>(true);

  // Test that the user.signup's event.data parameter is correctly typed
  type UserSignupHandler = Parameters<
    typeof workflow.define<"user.signup">
  >[0]["handler"];
  type UserSignupHandlerEventArg = Parameters<UserSignupHandler>[0]["event"];
  assertType<IsExact<UserSignupHandlerEventArg["name"], "user.signup">>(true);
  assertType<
    IsExact<
      UserSignupHandlerEventArg["data"],
      { userId: string; email: string }
    >
  >(true);

  // Test that the user.login's event.data parameter is correctly typed
  type UserLoginHandler = Parameters<
    typeof workflow.define<"user.login">
  >[0]["handler"];
  type UserLoginHandlerEventArg = Parameters<UserLoginHandler>[0]["event"];
  assertType<IsExact<UserLoginHandlerEventArg["name"], "user.login">>(true);
  assertType<
    IsExact<
      UserLoginHandlerEventArg["data"],
      { userId: string }
    >
  >(true);
});

Deno.test("Define a workflow with no step and dispatch it", async () => {
  const dbAccessor = generateTempDbAccessor();

  type MyWorkflow = {
    "no-step": {
      value: number;
    };
  };

  const workflow = new Workflow<MyWorkflow>(dbAccessor);

  let payload: number | null = null;

  workflow.define({
    name: "no-step",
    handler: (ctx) => {
      payload = ctx.event.data.value;
    },
  });

  const runId = workflow.dispatch("no-step", { value: 42 });
  assertExists(runId);
  const progress = await waitUntilWorkflowRunCompleted(workflow, runId);
  assertEquals(progress.steps.length, 0);
  assertEquals(payload, 42);
});
