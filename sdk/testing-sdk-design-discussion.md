# Testing SDK Design Discussion

## Overview

This document captures the discussion about designing a testing SDK for Cells
workflows, including the discovery of fundamental issues with database access
that need to be addressed first.

## Initial Goal

Design a user-friendly testing SDK that allows SDK users to test their workflows
with features like:

- Mocking workflow steps
- Database access in tests
- Time control (skipping delays)
- Assertion helpers

## Key Design Decisions

### 1. Use Standard `Deno.test`

- Stick with `Deno.test` instead of custom test runners
- Better ecosystem compatibility and familiarity
- Testing SDK provides utilities that work within `Deno.test`

### 2. Minimal API Surface

The minimal testing SDK should provide:

- Run a workflow with test input
- Mock external dependencies (step.run() calls)
- Access to test database
- Basic assertions on workflow output

### 3. WorkflowTester API Design

```typescript
export class WorkflowTester<TInput, TOutput> {
  constructor(workflow: WorkflowDefinition<TInput, TOutput>);

  withInput(input: TInput): this;
  mockStep(stepName: string, handler: () => any): this;
  async run(): Promise<TOutput>;
  get db(): Database;
  cleanup(): void;
}
```

Usage example:

```typescript
Deno.test("basic workflow test", async () => {
  const tester = new WorkflowTester(myWorkflow);

  const result = await tester
    .withInput({ username: "alice", email: "alice@example.com" })
    .mockStep("send-email", () => ({ sent: true }))
    .mockStep("send-sms", () => ({ sent: true }))
    .run();

  assertEquals(result, "finished");
  tester.cleanup();
});
```

## The Database Access Problem

During design, we discovered that the current `cell.db` global singleton pattern
creates significant testing challenges:

### Current Issues:

1. `cell.db` is a global singleton connected to production database
2. Can't swap to in-memory database for tests
3. Tight coupling makes testing difficult
4. No way to provide different databases for different environments

### Examples of Current Usage:

From analyzing the example programs in `data/` directory:

- Table creation at startup: `cell.db.exec("CREATE TABLE...")`
- HTTP handlers: `cell.db.prepare("INSERT...").run()`
- Workflow steps: `cell.db.prepare("SELECT...").get()`

## Proposed Solution: Database Access Redesign

### New API Design

1. **Add `cell.init()` for initialization:**

```typescript
cell.init((db) => {
  // Initialize schema - db is provided by the framework
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY,
      email TEXT UNIQUE
    );
  `);
});
```

2. **Pass database through context:**

```typescript
// In HTTP handlers - ctx.db
cell.request((req, ctx) => {
  ctx.db.prepare("INSERT INTO users...").run();
});

// In workflow steps - step.db
const workflow = cell.workflow.define({
  handler: async ({ input, step }) => {
    await step.run("save", async () => {
      step.db.prepare("INSERT INTO logs...").run();
    });
  },
});
```

3. **Remove global `cell.db` access**

### Benefits:

- Framework controls database creation (production vs test)
- Easy to swap databases for testing
- Explicit dependencies
- Supports multi-tenancy
- Clean separation of concerns

### Why `cell.init()` with Closure:

- Framework can provide different databases based on environment
- User provides initialization logic, framework provides database
- Single initialization point
- Works transparently in tests

## Implementation Plan

Since the SDK is not yet published, we can make breaking changes directly:

### Phase 1: Redesign Database Access

1. Remove `cell.db` completely
2. Add `cell.init()` method that accepts initialization callback
3. Add required context parameter to request handlers with `db` property
4. Add `db` property to step object

### Phase 2: Update Examples

5. Update all example applications in `data/` directory
6. Ensure all examples use the new patterns

### Phase 3: Build Testing SDK

7. Implement WorkflowTester using new database access pattern
8. Add test coverage for the testing SDK

### Phase 4: Documentation

9. Create comprehensive documentation
10. Add testing guide with examples

## Migration Example

Before:

```typescript
cell.db.exec(`CREATE TABLE IF NOT EXISTS ...`);

cell.request((req) => {
  cell.db.prepare("INSERT INTO ...").run(...);
});
```

After:

```typescript
cell.init((db) => {
  db.exec(`CREATE TABLE IF NOT EXISTS ...`);
});

cell.request((req, ctx) => {
  ctx.db.prepare("INSERT INTO ...").run(...);
});
```

## Next Steps

1. Implement the database access redesign in the Cells SDK
2. Build the testing SDK on top of the new database access pattern
3. Create comprehensive documentation and examples
4. Migrate existing example applications

## Key Insights

- The global `cell.db` pattern is the root cause of testing difficulties
- Database should flow through execution context rather than global state
- Testing SDK design revealed fundamental architectural improvements needed
- Phased implementation allows backward compatibility during migration
