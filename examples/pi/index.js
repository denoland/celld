import { DurableObject } from "cloudflare:workers";
import { PiHarness } from "agents/harness";
import { Lifecycle } from "agents/lifecycle";
import { createWorkersAI } from "agents/providers/pi";

const ENDPOINT = "https://api.cloudflare.com/client/v4/accounts";

function createDirectAi(accountId, token) {
  const url = `${ENDPOINT}/${accountId}/ai/v1/chat/completions`;
  return {
    async run(model, input, options) {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ model, ...input }),
        signal: options?.signal,
      });
      if (options?.returnRawResponse) return response;
      if (!response.ok) {
        const detail = await response.text().catch(() => "");
        throw new Error(
          `Workers AI returned ${response.status}${
            detail ? `: ${detail.slice(0, 512)}` : ""
          }`,
        );
      }
      return response.json();
    },
  };
}

function required(env, name) {
  const value = env[name];
  if (!value) {
    throw new Error(
      `the pi example needs ${name}; set it with CELLD_VAR_${name} or a CELLD_VARS_FILE entry`,
    );
  }
  return value;
}

export class PiAgent extends DurableObject {
  harness;
  lifecycle;

  constructor(ctx, env) {
    super(ctx, env);
    const ai = createDirectAi(
      required(env, "CLOUDFLARE_ACCOUNT_ID"),
      required(env, "CLOUDFLARE_API_TOKEN"),
    );
    const runtime = createWorkersAI(ai, {
      ...(env.PI_MODEL ? { model: env.PI_MODEL } : {}),
    });
    this.harness = new PiHarness({
      models: runtime.models,
      model: runtime.model,
      compaction: {
        enabled: false,
        reserveTokens: 0,
        keepRecentTokens: 0,
      },
    });
    this.lifecycle = Lifecycle.install(this).use(this.harness);
  }

  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === "/messages") {
      return Response.json(await this.harness.getMessages());
    }

    const result = await this.harness.prompt("ping");
    const errors = result.messages
      .filter((message) => message.stopReason === "error")
      .map((message) => message.error);
    return Response.json({
      status: result.status,
      operationId: result.operationId,
      errors,
      messages: result.messages,
    }, { status: result.status === "completed" ? 200 : 502 });
  }
}

export default {
  fetch(request, env) {
    const id = env.PI_AGENT.idFromName("local-pi");
    return env.PI_AGENT.get(id).fetch(request);
  },
};
