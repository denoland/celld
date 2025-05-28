import { Workflow } from "./workflow.ts";
import { assertType, type IsExact } from "jsr:@std/testing@1.0.13/types";

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

  const workflow = new Workflow<MyWorkflow>();

  // workflow.define should accept only "user.signup" or "user.login"
  type WorkflowNames = Parameters<typeof workflow.define>[0]["name"];
  assertType<IsExact<WorkflowNames, "user.signup" | "user.login">>(true);

  // Can we test that `event.data` is strictly typed depending on the workflow name?
});
