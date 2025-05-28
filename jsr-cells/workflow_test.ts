import { Workflow } from "./workflow.ts";
import { assertType, type IsExact } from "@std/testing/types";

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
