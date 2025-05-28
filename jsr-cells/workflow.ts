export class Workflow<WorkflowInputs extends Record<string, JSONValue>> {
  define<WorkflowName extends keyof WorkflowInputs>(
    definition: WorkflowDefinition<WorkflowInputs, WorkflowName>,
  ) {
    // TODO
  }
}

type JSONPrimitive = string | number | boolean | null;
type JSONValue =
  | JSONPrimitive
  | { [key: string]: JSONValue }
  | JSONValue[];

type WorkflowDefinition<
  WorkflowInputs extends Record<string, JSONValue>,
  WorkflowName extends keyof WorkflowInputs,
> = {
  name: WorkflowName;
  handler: (ctx: {
    event: {
      name: WorkflowName;
      data: WorkflowInputs[WorkflowName];
    };
    step: WorkflowStep;
    attempt: number;
  }) => Promise<void>;
};

type WorkflowStep = {
  run: <StepOutput extends JSONValue>(
    name: string,
    fn: () => StepOutput | Promise<StepOutput>,
  ) => Promise<StepOutput>;
  // TODO: add more methods like sleep, sleepUntil, invoke, etc.
};

type MyWorkflow = {
  "user.signup": {
    userId: string;
    email: string;
  };
  "user.login": {
    userId: string;
  };
};

const workflow1 = new Workflow<MyWorkflow>();
workflow1.define({
  name: "user.signup",
  handler: async ({ event, step, attempt }) => {
    const userInfo = await step.run("fetch-user", () => {
      // do something...
      return {
        userName: "Yusuke",
        age: 20,
        region: "Japan",
      };
    });

    await step.run("send-email", () => {
      // do something with userInfo
      return true;
    });
  },
});

const workflow2 = new Workflow();
