import { WorkerEntrypoint } from "cloudflare:workers";

const WORKER_ID = "hello-v1";
const WORKER_MAIN = "worker.js";
const WORKER_SOURCE = `
  export default {
    fetch() {
      console.log("hello from loaded code");
      return new Response("Hello from a Dynamic Worker!\\n");
    }
  };
`;

export class DynamicWorkerTail extends WorkerEntrypoint {
  tail(events) {
    for (const event of events) {
      for (const log of event.logs) {
        console.log(`dynamic-worker-tail: ${log.message.join(" ")}`, {
          workerId: this.ctx.props.workerId,
          level: log.level,
        });
      }
    }
  }
}

export default {
  fetch(request, env, ctx) {
    const worker = env.LOADER.get(WORKER_ID, () => ({
      compatibilityDate: "2025-01-01",
      mainModule: WORKER_MAIN,
      modules: { [WORKER_MAIN]: WORKER_SOURCE },
      tails: [
        ctx.exports.DynamicWorkerTail({
          props: { workerId: WORKER_ID },
        }),
      ],
    }));
    return worker.getEntrypoint().fetch(request);
  },
};
