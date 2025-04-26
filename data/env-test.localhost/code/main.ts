// Import the Connection and Room types from bootstrap.ts
import { Connection, Room } from "../../../src/bootstrap.ts";

export default {
  // Called when the server starts, before accepting connections
  async onStart(ctx: { roomId: string; room: Room }) {
    console.log("Environment test server started", ctx.roomId);
  },

  // Called for HTTP requests
  async onRequest(request: Request, ctx: { roomId: string; room: Room }) {
    // TODO: This should use Deno.env.toObject() in the future once permissions are properly set
    // For now, we're explicitly listing the environment variables we expect to be present
    const envVars = {
      "TEST_ENV_VAR": Deno.env.get("TEST_ENV_VAR"),
      "ANOTHER_TEST_VAR": Deno.env.get("ANOTHER_TEST_VAR"),
      "X-Room-Id": Deno.env.get("X-Room-Id")
    };
    
    return new Response(
      JSON.stringify(envVars),
      {
        headers: {
          "Content-Type": "application/json",
        },
      }
    );
  },
};