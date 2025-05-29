import { DatabaseSync } from "node:sqlite";
import { Workflow } from "./workflow.ts";
import {
  type DbAccessor,
  type JSONValue,
  scheduledTaskId,
  type Task,
  type TaskScheduler,
  type WorkflowRunId,
  type WorkflowRunProgress,
} from "./types.ts";
import { assertType, type IsExact } from "@std/testing/types";
import { assertEquals, assertExists } from "@std/assert";
import { delay } from "@std/async";
import { randomIntegerBetween } from "@std/random";
import { ulid } from "@std/ulid";

function generateTempDbAccessor(): DbAccessor {
  const db = new DatabaseSync(":memory:");
  return {
    get db() {
      return db;
    },
  };
}

function generateMockTaskScheduler(dbAccessor: DbAccessor): TaskScheduler {
  // Ensure `scheduled_tasks` table exists.
  dbAccessor.db.exec(`
    CREATE TABLE IF NOT EXISTS scheduled_tasks (
      id TEXT PRIMARY KEY NOT NULL,
      scheduled_time_unix_ms INTEGER NOT NULL,
      payload TEXT NOT NULL
    )
  `);
  dbAccessor.db.exec(`
    CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_schedule_time ON scheduled_tasks (scheduled_time_unix_ms)
  `);

  return {
    schedule: (task) => {
      const id = scheduledTaskId(ulid());
      dbAccessor.db.prepare(`
        INSERT INTO scheduled_tasks (id, scheduled_time_unix_ms, payload) VALUES (?, ?, ?)
      `).run(id, task.scheduledTimeUnixMs, JSON.stringify(task));
      return id;
    },
  };
}

async function startScheduledTaskPoller(args: {
  dbAccessor: DbAccessor;
  taskHandler: (task: Task) => void;
  intervalMs: number;
  abortSignal: AbortSignal;
}) {
  try {
    while (!args.abortSignal.aborted) {
      const tasks = args.dbAccessor.db.prepare(`
      SELECT * FROM scheduled_tasks WHERE scheduled_time_unix_ms <= ?
    `).all(Date.now());

      for (const task of tasks) {
        const payload = JSON.parse(task.payload as string) as Task;
        args.taskHandler(payload);
      }

      await delay(args.intervalMs, { signal: args.abortSignal });
    }
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      return;
    }
    throw error;
  }
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

Deno.test("Define a workflow with three steps and dispatch it - confirming the value is memoized", async () => {
  type MyWorkflow = {
    "three-steps": null;
  };

  const dbAccessor = generateTempDbAccessor();
  const taskScheduler = generateMockTaskScheduler(dbAccessor);

  const workflow = new Workflow<MyWorkflow>(dbAccessor, taskScheduler);

  let step2FirstAttempt = true;

  workflow.define({
    name: "three-steps",
    handler: async (ctx) => {
      const step1Result = await ctx.step.run(
        "generate a random integer",
        () => {
          return randomIntegerBetween(0, 1 << 30);
        },
      );

      await ctx.step.run("fail at first attempt, then succeed", () => {
        if (step2FirstAttempt) {
          step2FirstAttempt = false;
          throw new Error("step2 failed");
        }
        return null;
      });

      await ctx.step.run("multiply by 2", () => {
        return step1Result * 2;
      });
    },
  });

  const runId = workflow.dispatch("three-steps", null);
  assertExists(runId);

  const progressAfterFail = await await waitUntilPredicate(
    workflow,
    runId,
    (progress) => {
      // Wait until the step 1 is completed.
      return progress.steps.length >= 1;
    },
  );
  // Since the step 2 should have failed, the workflow run should not be completed yet.
  assertEquals(progressAfterFail.completedAt, null);
  assertEquals(typeof progressAfterFail.steps[0].outputData, "number");
  const step1Output = progressAfterFail.steps[0].outputData as number;

  // When step 2 fails, a retry is scheduled i.e. a new record is inserted into
  // `scheduled_tasks` table. To process it, we start a task poller that
  // periodically retrieves scheduled tasks and dispatches them.
  // In the actual environment, this polling is achieved by leveraging the alarm
  // mechanism.
  const abortController = new AbortController();

  const pollerPromise = startScheduledTaskPoller({
    dbAccessor,
    taskHandler: (task) => {
      switch (task.kind) {
        case "retry-workflow-run":
          workflow.retry(task.workflowRunId);
          break;
        default:
          throw new Error("Only retry-workflow-run is relevant for this test");
      }
    },
    intervalMs: 500,
    abortSignal: abortController.signal,
  });

  await using stack = new AsyncDisposableStack();
  stack.defer(async () => {
    abortController.abort();
    await pollerPromise;
  });

  // Wait until the run is retried and completed.
  const progressAfterRetry = await waitUntilWorkflowRunCompleted(
    workflow,
    runId,
  );

  assertEquals(progressAfterRetry.steps.length, 3);
  // The step 3 should have been executed with the memoized output of step 1
  // from the previous run.
  assertEquals(progressAfterRetry.steps[2].outputData, step1Output * 2);
});
