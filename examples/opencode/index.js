import { DurableObject } from "cloudflare:workers";
import { OpenCodeWorkerd } from "@opencode-ai/sdk/workerd";

const PROMPT = "hello world";

export class OpenCodeDO extends DurableObject {
  async fetch(_request) {
    const checks = [];
    const check = async (name, fn) => {
      const started = Date.now();
      try {
        const value = await fn();
        checks.push({ name, ok: true, ms: Date.now() - started });
        return value;
      } catch (error) {
        checks.push({
          name,
          ok: false,
          ms: Date.now() - started,
          error: serializeError(error),
        });
        throw error;
      }
    };

    try {
      const opencode = await OpenCodeWorkerd.create({
        storage: this.ctx.storage,
      });

      // The SDK resolves the model from its own provider registry. Report the
      // resolution instead of a model this example names: a request for an
      // unregistered provider falls back without an error, so a hardcoded name
      // in the response would claim a model that never ran.
      const model = await check(
        "model.default",
        () => opencode.model.default(),
      );
      const providers = await check(
        "provider.list",
        () => opencode.provider.list(),
      );

      await check("health.get", () => opencode.health.get());
      const session = await check(
        "session.create",
        () => opencode.session.create(),
      );
      await check(
        "session.get",
        () => opencode.session.get({ sessionID: session.id }),
      );

      // `prompt` only enqueues the message and returns before the model runs.
      // `wait` blocks until the run completes, so a probe that stops at
      // `prompt` reports success even when no model answers.
      await check(
        "session.prompt",
        () => opencode.session.prompt({ sessionID: session.id, text: PROMPT }),
      );
      await check(
        "session.wait",
        () => opencode.session.wait({ sessionID: session.id }),
      );

      const messages = await check(
        "message.list",
        () => opencode.message.list({ sessionID: session.id }),
      );

      const reply = assistantText(messages);
      if (!reply) {
        return Response.json({
          ok: false,
          error: "the session completed without an assistant reply",
          model: modelRef(model),
          checks,
        }, { status: 502 });
      }

      return Response.json({
        ok: true,
        model: modelRef(model),
        providers: (providers?.data ?? providers ?? []).map((entry) =>
          entry.id
        ),
        prompt: PROMPT,
        reply,
        checks,
      });
    } catch (error) {
      return Response.json({
        ok: false,
        error: serializeError(error),
        checks,
      }, { status: 500 });
    }
  }
}

function modelRef(model) {
  const data = model?.data ?? model;
  return {
    providerID: data?.providerID ?? null,
    modelID: data?.modelID ?? null,
  };
}

// An assistant reply is one entry of a `content` array that also carries
// reasoning and tool parts. Join the text parts only, because a reasoning part
// is present even when the model produced no answer.
function assistantText(messages) {
  const rows = messages?.data ?? messages ?? [];
  for (const message of rows) {
    if (message?.type !== "assistant") continue;
    const text = (message.content ?? [])
      .filter((part) => part?.type === "text")
      .map((part) => part.text)
      .join("")
      .trim();
    if (text) return text;
  }
  return null;
}

function serializeError(value, seen = new Set()) {
  if (value === null || typeof value !== "object") return value;
  if (seen.has(value)) return "[circular]";
  seen.add(value);
  return {
    name: value.name,
    message: value.message,
    stack: value.stack,
    cause: serializeError(value.cause, seen),
    ...Object.fromEntries(Object.entries(value)),
  };
}

export default {
  fetch(request, env) {
    const id = env.OPENCODE.idFromName("reporter-main-f4e19cd2");
    return env.OPENCODE.get(id).fetch(request);
  },
};
