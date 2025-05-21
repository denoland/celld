import { cell } from "../../../jsr-cells/mod.ts";

console.log(`[${cell.id}] Initializing environment test server...`);

cell.request((req: Request): Response => {
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
});
