// Import the Connection and Cell types from bootstrap.ts
import { Cell, Connection } from "../../../src/bootstrap.ts";

export default {
  // Called when the server starts, before accepting connections
  async onStart(ctx: { cellId: string; cell: Cell }) {
    console.log("Environment test server started", ctx.cellId);
  },

  // Called for HTTP requests
  async onRequest(request: Request, ctx: { cellId: string; cell: Cell }) {
    // TODO: This should use Deno.env.toObject() in the future once permissions are properly set
    // For now, we're explicitly listing the environment variables we expect to be present
    const envVars = {
      "TEST_ENV_VAR": Deno.env.get("TEST_ENV_VAR"),
      "ANOTHER_TEST_VAR": Deno.env.get("ANOTHER_TEST_VAR"),
      "X-Cell-Id": Deno.env.get("X-Cell-Id"),
    };

    return new Response(
      JSON.stringify(envVars),
      {
        headers: {
          "Content-Type": "application/json",
        },
      },
    );
  },
};
