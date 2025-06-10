import { DatabaseSync } from "node:sqlite";
import {
  WorkflowRuntime,
} from "./workflow.ts";
import {
  type DbAccessor,
  scheduledTaskId,
  type TaskScheduler,
  type WorkflowRunId,
  workflowRunId,
  type WorkflowRunProgress,
} from "./types.ts";
import { assert, assertEquals, assertExists } from "jsr:@std/assert@1";
import { delay } from "jsr:@std/async@1/delay";
import { ulid } from "jsr:@std/ulid@1/ulid";
import { Cell } from "./cell.ts";

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

function createTestCell(): Cell {
  return new Cell({
    tenant: "test",
    id: "test-cell",
    dbPath: ":memory:",
    ctlSockPath: "/dev/null"
  });
}


// Helper to wait for workflow completion
async function waitForCompletion(
  runtime: WorkflowRuntime,
  runId: WorkflowRunId,
): Promise<WorkflowRunProgress> {
  const MAX_RETRIES = 100;
  const INTERVAL_MS = 100;

  for (let i = 0; i < MAX_RETRIES; i++) {
    const progress = runtime.getRunProgress(runId);
    if (!progress) {
      throw new Error(`workflow run not found for the given runId: ${runId}`);
    }
    if (progress.completedAt !== null) {
      return progress;
    }
    await delay(INTERVAL_MS);
  }

  throw new Error(`Workflow did not complete after ${MAX_RETRIES} retries`);
}

Deno.test("workflow type safety", () => {
  const rt = createTestRuntime().runtime;

  withRuntime(rt, () => {
    // Test that workflows are properly typed
    const _userSignup = define<
      { userId: string; email: string },
      { success: boolean; welcomeMessage: string }
    >({
      name: "user.signup",
      handler: async ({ input }) => {
        await delay(1);
        return { success: true, welcomeMessage: `Welcome ${input.userId}!` };
      },
    });

    const _userLogin = define<
      { userId: string },
      { loggedIn: boolean; sessionId: string }
    >({
      name: "user.login",
      handler: async (_) => {
        await delay(1);
        return { loggedIn: true, sessionId: "session_123" };
      },
    });
  });
});

Deno.test("dispatch undefined workflow throws error", () => {
  const rt = createTestRuntime().runtime;

  withRuntime(rt, () => {
    const undefinedWorkflow = {
      config: { name: "nonexistent", handler: async () => {} },
      name: "nonexistent",
    };

    try {
      dispatch(undefinedWorkflow);
      throw new Error("Expected error was not thrown");
    } catch (error) {
      assertEquals((error as Error).message, "Workflow nonexistent not found");
    }
  });
});

Deno.test("define workflow with no steps and dispatch it", async () => {
  const { runtime } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    const noStepWorkflow = define<{ value: number }, { processed: number }>({
      name: "no-step",
      handler: async ({ input }) => {
        await delay(1);
        return { processed: input.value * 2 };
      },
    });

    const runId = dispatch(noStepWorkflow, { value: 42 });
    assertExists(runId);

    const progress = await waitForCompletion(runtime, runId);
    assertEquals(progress.steps.length, 0); // No step.run() calls
    assertExists(progress.completedAt);
  });
});

Deno.test("define workflow with one step and dispatch it", async () => {
  const { runtime } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    const oneStepWorkflow = define<{ value: number }, { finalResult: number }>({
      name: "one-step",
      handler: async ({ input, step }) => {
        const result = await step.run(
          "sleep 10ms and then multiply by 2",
          async () => {
            await delay(10);
            return input.value * 2;
          },
        );
        return { finalResult: result };
      },
    });

    const runId = dispatch(oneStepWorkflow, { value: 42 });
    assertExists(runId);

    const progress1 = runtime.getRunProgress(runId);
    assertExists(progress1);
    assertEquals(progress1.completedAt, null);

    const progress2 = await waitForCompletion(runtime, runId);
    assertEquals(progress2.steps.length, 1);
    assertEquals(progress2.steps[0].outputData, 84);
    assertExists(progress2.completedAt);
  });
});

Deno.test("step.run that was already executed is not executed again on retry", async () => {
  const { runtime, dbAccessor } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    // Simulate a workflow that was dispatched but not completed
    const runId = workflowRunId(ulid());

    // Insert test data - first register a workflow
    dbAccessor.db.prepare(`
      INSERT OR IGNORE INTO workflows (name) VALUES ('workflow')
    `).run();

    // Insert a workflow run record to simulate a dispatched workflow
    dbAccessor.db.prepare(`
      INSERT INTO workflow_runs (id, workflow_name, input_data)
      VALUES (?, 'workflow', 'null')
    `).run(runId);

    // Insert a workflow step record to simulate a step that was executed
    dbAccessor.db.prepare(`
      INSERT INTO workflow_steps (workflow_run_id, step_index, name, output_data, step_type)
      VALUES (?, 1, 'step1', '"first run"', 'run')
    `).run(runId);

    let step1Reexecuted = false;

    // Define a workflow
    const _workflow = define<null, string>({
      name: "workflow",
      handler: async ({ step }) => {
        const result = await step.run("step1", () => {
          step1Reexecuted = true;
          return "second run";
        });

        await step.run("step2", async () => {
          await delay(1);
        });

        return result;
      },
    });

    // Retry the workflow
    const retried = runtime.retry(runId);
    assertEquals(retried, true);

    const progress = await waitForCompletion(runtime, runId);
    assert(!step1Reexecuted);
    assertEquals(progress.steps.length, 2);
    assertEquals(progress.steps[0].name, "step1");
    assertEquals(progress.steps[0].outputData, "first run");
    assertEquals(progress.steps[1].name, "step2");
    assertEquals(progress.outputData, "first run");
  });
});

Deno.test("step.invoke enables workflow composition", async () => {
  const { runtime, dbAccessor } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    const multiply = define<{ a: number; b: number }, number>({
      name: "multiply",
      handler: async ({ input }) => {
        await delay(1);
        return input.a * input.b;
      },
    });

    const calculate = define<
      { x: number },
      { doubled: number; tripled: number }
    >({
      name: "calculate",
      handler: async ({ input, step }) => {
        const doubled = await step.invoke(multiply, { a: input.x, b: 2 });
        const tripled = await step.invoke(multiply, { a: input.x, b: 3 });
        return { doubled, tripled };
      },
    });

    const runId = dispatch(calculate, { x: 5 });
    const progress = await waitForCompletion(runtime, runId);

    // Should have 2 invoke steps
    assertEquals(progress.steps.length, 2);

    // Check the output data was stored
    const outputRow = dbAccessor.db.prepare(
      "SELECT output_data FROM workflow_runs WHERE id = ?",
    ).get(runId);

    const output = JSON.parse(outputRow!.output_data as string);
    assertEquals(output, { doubled: 10, tripled: 15 });
  });
});

Deno.test("step.invoke recovers from crash", async () => {
  const { runtime, dbAccessor } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    // Simulate a parent workflow that was suspended waiting for child
    const parentId = workflowRunId(ulid());
    const childId = workflowRunId(ulid());

    // Insert test data - first register workflows
    dbAccessor.db.prepare(`
      INSERT OR IGNORE INTO workflows (name) VALUES ('parent')
    `).run();

    dbAccessor.db.prepare(`
      INSERT OR IGNORE INTO workflows (name) VALUES ('child')
    `).run();

    dbAccessor.db.prepare(`
      INSERT INTO workflow_runs (id, workflow_name, input_data) 
      VALUES (?, 'parent', '{"x": 5}')
    `).run(parentId);

    dbAccessor.db.prepare(`
      INSERT INTO workflow_runs (id, workflow_name, input_data, output_data, completed_at)
      VALUES (?, 'child', '{"a": 5, "b": 2}', '10', datetime('now'))
    `).run(childId);

    dbAccessor.db.prepare(`
      INSERT INTO workflow_steps (workflow_run_id, step_index, name, step_type, invoked_workflow_run_id)
      VALUES (?, 1, 'invoke:child', 'invoke', ?)
    `).run(parentId, childId);

    // Define workflows
    const child = define<{ a: number; b: number }, number>({
      name: "child",
      handler: async ({ input }) => {
        await delay(1);
        return input.a * input.b;
      },
    });
    const _parent = define<{ x: number }, { result: number }>({
      name: "parent",
      handler: async ({ input, step }) => {
        const result = await step.invoke(child, { a: input.x, b: 2 });
        return { result };
      },
    });

    // Retry parent - should pick up child's result
    const retried = runtime.retry(parentId);
    assertEquals(retried, true);

    await waitForCompletion(runtime, parentId);

    // Verify parent completed with child's output
    const parentRun = dbAccessor.db.prepare(
      "SELECT output_data FROM workflow_runs WHERE id = ?",
    ).get(parentId);

    assertExists(parentRun);
    assertEquals(JSON.parse(parentRun.output_data as string), { result: 10 });
  });
});

Deno.test("step.invoke that was already executed is not executed again on retry", async () => {
  const { runtime, dbAccessor } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    // Simulate a parent workflow that was suspended waiting for child
    const parentId = workflowRunId(ulid());
    const childId = workflowRunId(ulid());

    // Insert test data - first register workflows
    dbAccessor.db.prepare(`
      INSERT OR IGNORE INTO workflows (name) VALUES ('parent')
    `).run();

    dbAccessor.db.prepare(`
      INSERT OR IGNORE INTO workflows (name) VALUES ('child')
    `).run();

    dbAccessor.db.prepare(`
      INSERT INTO workflow_runs (id, workflow_name, input_data) 
      VALUES (?, 'parent', 'null')
    `).run(parentId);

    // Intentionally set the output data to an empty string (falsy value)
    dbAccessor.db.prepare(`
      INSERT INTO workflow_runs (id, workflow_name, input_data, output_data, completed_at)
      VALUES (?, 'child', 'null', '""', datetime('now'))
    `).run(childId);

    dbAccessor.db.prepare(`
      INSERT INTO workflow_steps (workflow_run_id, step_index, name, step_type, invoked_workflow_run_id, output_data, completed_at)
      VALUES (?, 1, 'invoke:child', 'invoke', ?, '""', datetime('now'))
    `).run(parentId, childId);

    let childReexecuted = false;

    // Define workflows
    const child = define<null, string>({
      name: "child",
      handler: () => {
        childReexecuted = true;
        return "oops, re-executed!";
      },
    });
    const _parent = define<null, string>({
      name: "parent",
      handler: async ({ step }) => {
        const result = await step.invoke(child, null);
        return result;
      },
    });

    // Retry parent - should pick up the cached result of the child without re-executing it
    const retried = runtime.retry(parentId);
    assertEquals(retried, true);

    const progress = await waitForCompletion(runtime, parentId);
    assert(!childReexecuted);
    assertEquals(progress.steps.length, 1);
    assertEquals(progress.steps[0].name, "invoke:child");
    assertEquals(progress.steps[0].outputData, "");
    assertEquals(progress.outputData, "");
  });
});

Deno.test("multiple step.invoke calls work sequentially", async () => {
  const { runtime } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    const add = define<{ a: number; b: number }, number>({
      name: "add",
      handler: async ({ input }) => {
        await delay(1);
        return input.a + input.b;
      },
    });

    const chain = define<{ x: number }, number>({
      name: "chain",
      handler: async ({ input, step }) => {
        const r1 = await step.invoke(add, { a: input.x, b: 1 });
        const r2 = await step.invoke(add, { a: r1, b: 2 });
        const r3 = await step.invoke(add, { a: r2, b: 3 });
        return r3; // Should be x + 6
      },
    });

    const runId = dispatch(chain, { x: 10 });
    const progress = await waitForCompletion(runtime, runId);

    assertEquals(progress.steps.length, 3);

    // Final output should be 16 (10 + 1 + 2 + 3)
    const outputRow = runtime.getRunProgress(runId);
    assertExists(outputRow);
    assertEquals(outputRow.steps[0].outputData, 11); // 10 + 1
    assertEquals(outputRow.steps[1].outputData, 13); // 11 + 2
    assertEquals(outputRow.steps[2].outputData, 16); // 13 + 3
  });
});

Deno.test("parent down child completed recovery", async () => {
  const { runtime, dbAccessor } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    // Test the scenario where parent workflow was down when child completed
    const child = define<{ x: number }, number>({
      name: "child",
      handler: async ({ input }) => {
        await delay(1);
        return input.x * 10;
      },
    });

    const parent = define<{ value: number }, { processed: number }>({
      name: "parent",
      handler: async ({ input, step }) => {
        const result = await step.invoke(child, { x: input.value });
        return { processed: result };
      },
    });

    // 1. Start parent workflow
    const parentRunId = dispatch(parent, { value: 5 });

    // 2. Let it partially execute (should dispatch child and suspend)
    await delay(50);

    // 3. Simulate finding the child completed while parent was down
    const childRuns = dbAccessor.db.prepare(`
      SELECT invoked_workflow_run_id as child_run_id 
      FROM workflow_steps 
      WHERE workflow_run_id = ? AND step_type = 'invoke'
    `).all(parentRunId);

    assertEquals(childRuns.length, 1);
    const childRunId = childRuns[0].child_run_id;

    // 4. Manually complete the child (simulate it finished while parent was down)
    dbAccessor.db.prepare(`
      UPDATE workflow_runs 
      SET output_data = ?, completed_at = datetime('now')
      WHERE id = ?
    `).run(JSON.stringify(50), childRunId); // 5 * 10 = 50

    // 5. Retry parent - should pick up completed child result
    const retried = runtime.retry(parentRunId);
    assertEquals(retried, true);

    // 6. Wait for parent completion
    const progress = await waitForCompletion(runtime, parentRunId);

    // 7. Verify parent used child's result
    assertEquals(progress.steps.length, 1);
    assertEquals(progress.steps[0].name, "invoke:child");
    assertEquals(progress.steps[0].outputData, 50);

    // Verify final parent output
    const parentRun = dbAccessor.db.prepare(
      "SELECT output_data FROM workflow_runs WHERE id = ?",
    ).get(parentRunId);

    assertExists(parentRun);
    assertEquals(JSON.parse(parentRun.output_data as string), {
      processed: 50,
    });
  });
});

Deno.test("void returning workflows work correctly", async () => {
  const { runtime, dbAccessor } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    // Define a workflow that returns void (no explicit return)
    const voidWorkflow = define({
      name: "void-workflow",
      handler: async (_) => {
        await delay(1);
        // Explicitly return nothing (void)
      },
    });

    // Define a parent workflow that invokes the void workflow
    const parent = define({
      name: "parent",
      handler: async ({ step }) => {
        const result = await step.invoke(voidWorkflow);
        // The return type of `void` is converted to `null` although the runtime
        // representation of `void` is `undefined`. This is to store the step
        // result as a valid JSON value.
        return { completed: true, voidResult: result === null };
      },
    });

    const runId = dispatch(parent);
    const progress = await waitForCompletion(runtime, runId);

    // Verify the workflow completed successfully
    assertEquals(progress.steps.length, 1);
    assertEquals(progress.steps[0].name, "invoke:void-workflow");
    assertEquals(progress.steps[0].outputData, null);

    // Verify final parent output shows void was handled correctly
    const dbRow = dbAccessor.db.prepare(
      "SELECT output_data FROM workflow_runs WHERE id = ?",
    ).get(runId);
    assertExists(dbRow);
    const actualOutput = JSON.parse(dbRow.output_data as string);
    assertEquals(actualOutput, { completed: true, voidResult: true });
  });
});

Deno.test("getRunProgress public API works correctly", async () => {
  const { runtime } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    const testWorkflow = define<{ value: number }, { result: number }>({
      name: "test-progress",
      handler: async ({ input, step }) => {
        const doubled = await step.run("double", () => input.value * 2);
        return { result: doubled };
      },
    });

    const runId = dispatch(testWorkflow, { value: 5 });

    // Test that getRunProgress works the same as runtime.getRunProgress
    const publicProgress = getRunProgress(runId);
    const runtimeProgress = runtime.getRunProgress(runId);

    // Both should return the same data
    assertEquals(publicProgress?.id, runtimeProgress?.id);
    assertEquals(publicProgress?.workflowName, runtimeProgress?.workflowName);

    // Wait for completion and test again
    await waitForCompletion(runtime, runId);

    const finalProgress = getRunProgress(runId);
    assertExists(finalProgress);
    assertEquals(finalProgress.steps.length, 1);
    assertEquals(finalProgress.steps[0].name, "double");
    assertEquals(finalProgress.steps[0].outputData, 10);
    assertExists(finalProgress.completedAt);
  });
});

Deno.test("listRuns public API works correctly", async () => {
  const { runtime } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    const workflow1 = define<{ x: number }, { result: number }>({
      name: "workflow-1",
      handler: async ({ input }) => {
        await delay(1);
        return { result: input.x * 2 };
      },
    });

    const workflow2 = define<{ y: number }, { result: number }>({
      name: "workflow-2",
      handler: async ({ input }) => {
        await delay(1);
        return { result: input.y * 3 };
      },
    });

    // Dispatch multiple workflows
    const runId1 = dispatch(workflow1, { x: 10 });
    const runId2 = dispatch(workflow2, { y: 20 });
    const runId3 = dispatch(workflow1, { x: 30 });

    // Wait for all to complete
    await waitForCompletion(runtime, runId1);
    await waitForCompletion(runtime, runId2);
    await waitForCompletion(runtime, runId3);

    // Test listRuns without filters
    const allRuns = listRuns();
    assertEquals(allRuns.length, 3);

    // Test filtering by workflow name
    const workflow1Runs = listRuns({ workflowName: "workflow-1" });
    assertEquals(workflow1Runs.length, 2);
    workflow1Runs.forEach((run) => {
      assertEquals(run.workflowName, "workflow-1");
    });

    const workflow2Runs = listRuns({ workflowName: "workflow-2" });
    assertEquals(workflow2Runs.length, 1);
    assertEquals(workflow2Runs[0].workflowName, "workflow-2");

    // Test filtering by status
    const completedRuns = listRuns({ status: "completed" });
    assertEquals(completedRuns.length, 3);
    completedRuns.forEach((run) => {
      assertExists(run.completedAt);
    });

    // Test limit
    const limitedRuns = listRuns({ limit: 2 });
    assertEquals(limitedRuns.length, 2);

    // Test combined filters
    const combinedRuns = listRuns({
      workflowName: "workflow-1",
      status: "completed",
      limit: 1,
    });
    assertEquals(combinedRuns.length, 1);
    assertEquals(combinedRuns[0].workflowName, "workflow-1");
    assertExists(combinedRuns[0].completedAt);
  });
});

Deno.test("step.sleep suspends and resumes workflow execution", async () => {
  const { runtime, dbAccessor } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    const sleepWorkflow = define<{ durationMs: number }, { message: string }>({
      name: "sleep-test",
      handler: async ({ input, step }) => {
        await step.run("before-sleep", () => "started");
        await step.sleep("wait", input.durationMs);
        await step.run("after-sleep", () => "completed");
        return { message: `Slept for ${input.durationMs}ms` };
      },
    });

    const startTime = Date.now();
    const runId = dispatch(sleepWorkflow, { durationMs: 100 });

    // Initially workflow should be suspended (not completed)
    await delay(50);
    let progress = runtime.getRunProgress(runId);
    assertExists(progress);
    assertEquals(progress.completedAt, null);
    assertEquals(progress.steps.length, 2); // before-sleep and sleep step

    // Check sleep step was created with correct metadata
    const sleepStep = progress.steps.find((s) => s.stepType === "sleep");
    assertExists(sleepStep);
    assertEquals(sleepStep.name, "wait");
    assertEquals(sleepStep.completedAt, null);

    // Check sleep step metadata
    const sleepData = sleepStep.outputData as {
      wakeUpTime: number;
      durationMs: number;
    };
    assertEquals(sleepData.durationMs, 100);
    assertExists(sleepData.wakeUpTime);

    // Simulate alarm processing - mark sleep step as completed and retry workflow
    dbAccessor.db.prepare(`
      UPDATE workflow_steps 
      SET completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', 'utc')
      WHERE workflow_run_id = ? AND step_index = ? AND step_type = 'sleep'
    `).run(runId, sleepStep.stepIndex);

    const retried = runtime.retry(runId);
    assertEquals(retried, true);

    // Wait for workflow completion
    progress = await waitForCompletion(runtime, runId);

    // Verify workflow completed successfully
    assertEquals(progress.steps.length, 3); // before-sleep, sleep, after-sleep
    assertEquals(progress.steps[0].name, "before-sleep");
    assertEquals(progress.steps[0].outputData, "started");
    assertEquals(progress.steps[1].name, "wait");
    assertEquals(progress.steps[1].stepType, "sleep");
    assertExists(progress.steps[1].completedAt); // Sleep step is now completed
    assertEquals(progress.steps[2].name, "after-sleep");
    assertEquals(progress.steps[2].outputData, "completed");

    // Verify final output
    assertEquals(progress.outputData, { message: "Slept for 100ms" });
    assertExists(progress.completedAt);

    const elapsed = Date.now() - startTime;
    // Should take more than 50ms (we waited that long before simulating alarm)
    assert(elapsed >= 50);
  });
});

Deno.test("step.sleep is idempotent on retry", async () => {
  const { runtime, dbAccessor } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    // Simulate a workflow that was interrupted during sleep
    const runId = workflowRunId(ulid());

    // Insert test data - first register workflow
    dbAccessor.db.prepare(`
      INSERT OR IGNORE INTO workflows (name) VALUES ('sleep-retry-test')
    `).run();

    // Insert workflow run
    dbAccessor.db.prepare(`
      INSERT INTO workflow_runs (id, workflow_name, input_data)
      VALUES (?, 'sleep-retry-test', '{"durationMs": 10000}')  -- 10 seconds!
    `).run(runId);

    // Insert completed sleep step (simulating alarm already fired)
    dbAccessor.db.prepare(`
      INSERT INTO workflow_steps (workflow_run_id, step_index, name, step_type, output_data, completed_at)
      VALUES (?, 1, 'wait', 'sleep', '{"wakeUpTime": 1234567890, "durationMs": 10000}', datetime('now'))
    `).run(runId);

    define<{ durationMs: number }, string>({
      name: "sleep-retry-test",
      handler: async ({ input, step }) => {
        // Sleep method will be called but won't suspend since step already completed
        await step.sleep("wait", input.durationMs);
        return "completed";
      },
    });

    const startTime = Date.now();

    // Retry the workflow
    const retried = runtime.retry(runId);
    assertEquals(retried, true);

    // This should complete almost immediately since sleep is skipped
    const progress = await waitForCompletion(runtime, runId); // 1 second timeout

    const elapsed = Date.now() - startTime;
    // Should complete in under 500ms if sleep was skipped
    assert(
      elapsed < 500,
      `Workflow took ${elapsed}ms - sleep was not skipped!`,
    );

    assertEquals(progress.steps.length, 1);
    assertEquals(progress.steps[0].name, "wait");
    assertEquals(progress.steps[0].stepType, "sleep");
    assertExists(progress.steps[0].completedAt);
    assertEquals(progress.outputData, "completed");
  });
});

Deno.test("step.sleep schedules wake-up task correctly", async () => {
  const { runtime, dbAccessor } = createTestRuntime();

  await withRuntimeAsync(runtime, async () => {
    const taskSchedulingWorkflow = define<{ sleepMs: number }, null>({
      name: "task-scheduling-test",
      handler: async ({ input, step }) => {
        await step.sleep("test-sleep", input.sleepMs);
        return null;
      },
    });

    const runId = dispatch(taskSchedulingWorkflow, { sleepMs: 500 });

    // Let workflow execute and suspend
    await delay(50);

    // Check that a wake-sleep-step task was scheduled
    const tasks = dbAccessor.db.prepare(`
      SELECT * FROM scheduled_tasks 
      WHERE JSON_EXTRACT(payload, '$.kind') = 'wake-sleep-step'
    `).all();

    assertEquals(tasks.length, 1);

    const task = tasks[0];
    const payload = JSON.parse(task.payload as string);

    assertEquals(payload.kind, "wake-sleep-step");
    assertEquals(payload.workflowRunId, runId);
    assertEquals(payload.stepIndex, 1);
  });
});
