import { DatabaseSync } from "node:sqlite";
import { Workflow } from "./workflow.ts";
import type {
  DbAccessor,
  JSONValue,
  TaskScheduler,
  WorkflowRunId,
  WorkflowRunProgress,
} from "./types.ts";
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

function generateMockTaskScheduler(dbAccessor: DbAccessor): TaskScheduler {
  return {
    schedule: async (_task) => {
      // no-op
    },
  };
}

async function waitUntilPredicate<T extends Record<string, JSONValue>>(
  workflow: Workflow<T>,
  runId: WorkflowRunId,
  predicate: (progress: WorkflowRunProgress) => boolean,
): Promise<WorkflowRunProgress> {
  const MAX_RETRIES = 100;
  const INTERVAL_MS = 100;

  for (let i = 0; i < MAX_RETRIES; i++) {
    const progress = workflow.getRunProgress(runId);
    if (!progress) {
      throw new Error(`workflow run not found for the given runId: ${runId}`);
    }
    if (predicate(progress)) {
      return progress;
    }
    await delay(INTERVAL_MS);
  }

  throw new Error(
    `Predicate did not return true after ${MAX_RETRIES} retries with ${INTERVAL_MS}ms interval`,
  );
}

function waitUntilWorkflowRunCompleted<
  T extends Record<string, JSONValue>,
>(
  workflow: Workflow<T>,
  runId: WorkflowRunId,
): Promise<WorkflowRunProgress> {
  return waitUntilPredicate(
    workflow,
    runId,
    (progress) => progress.completedAt !== null,
  );
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

  const dbAccessor = generateTempDbAccessor();
  const taskScheduler = generateMockTaskScheduler(dbAccessor);

  const workflow = new Workflow<MyWorkflow>(dbAccessor, taskScheduler);

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

Deno.test("Dispatch a workflow that is not defined returns null and nothing happens", () => {
  type MyWorkflow = {
    "foo": null;
  };

  const dbAccessor = generateTempDbAccessor();
  const taskScheduler = generateMockTaskScheduler(dbAccessor);

  const workflow = new Workflow<MyWorkflow>(dbAccessor, taskScheduler);

  const runId = workflow.dispatch("foo", null);
  assertEquals(runId, null);
});

Deno.test("Define a workflow with no step and dispatch it", async () => {
  type MyWorkflow = {
    "no-step": {
      value: number;
    };
  };

  const dbAccessor = generateTempDbAccessor();
  const taskScheduler = generateMockTaskScheduler(dbAccessor);

  const workflow = new Workflow<MyWorkflow>(dbAccessor, taskScheduler);

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

Deno.test("Define a workflow with one step and dispatch it", async () => {
  type MyWorkflow = {
    "one-step": {
      value: number;
    };
  };

  const dbAccessor = generateTempDbAccessor();
  const taskScheduler = generateMockTaskScheduler(dbAccessor);

  const workflow = new Workflow<MyWorkflow>(dbAccessor, taskScheduler);

  workflow.define({
    name: "one-step",
    handler: async (ctx) => {
      await ctx.step.run("sleep 10ms and then multiply by 2", async () => {
        await delay(10);
        return ctx.event.data.value * 2;
      });
    },
  });

  const runId = workflow.dispatch("one-step", { value: 42 });
  assertExists(runId);

  const progress1 = workflow.getRunProgress(runId);
  assertExists(progress1);
  assertEquals(progress1.completedAt, null);

  const progress2 = await waitUntilWorkflowRunCompleted(workflow, runId);
  assertExists(progress2);
  assertExists(progress2.completedAt);

  assertEquals(progress2.steps.length, 1);
  assertEquals(progress2.steps[0].name, "sleep 10ms and then multiply by 2");
  assertEquals(progress2.steps[0].outputData, 84);
});

Deno.test("Define a workflow with two steps and dispatch it", async () => {
  type MyWorkflow = {
    "two-steps": {
      value: number;
    };
  };

  const dbAccessor = generateTempDbAccessor();
  const taskScheduler = generateMockTaskScheduler(dbAccessor);

  const workflow = new Workflow<MyWorkflow>(dbAccessor, taskScheduler);

  workflow.define({
    name: "two-steps",
    handler: async (ctx) => {
      const step1Result = await ctx.step.run(
        "sleep 10ms and then multiply by 2",
        async () => {
          await delay(10);
          return ctx.event.data.value * 2;
        },
      );

      await ctx.step.run("sleep 10ms and then add 1", async () => {
        await delay(10);
        return step1Result + 1;
      });
    },
  });

  const runId = workflow.dispatch("two-steps", { value: 42 });
  assertExists(runId);

  const progress1 = workflow.getRunProgress(runId);
  assertExists(progress1);
  assertEquals(progress1.completedAt, null);

  const progress2 = await waitUntilWorkflowRunCompleted(workflow, runId);
  assertExists(progress2);
  assertExists(progress2.completedAt);

  assertEquals(progress2.steps.length, 2);
  assertEquals(progress2.steps[0].name, "sleep 10ms and then multiply by 2");
  assertEquals(progress2.steps[0].outputData, 84);
  assertEquals(progress2.steps[1].name, "sleep 10ms and then add 1");
  assertEquals(progress2.steps[1].outputData, 85);
});
