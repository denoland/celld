import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

// The code of the facet. A real application would load it from a bundle,
// a database, or a request; here it is a string so the example is one file.
const CODE = `
  import { DurableObject } from "cloudflare:workers";
  export class App extends DurableObject {
    async fetch() {
      const n = this.ctx.storage.kv.get("n") ?? 0;
      this.ctx.storage.kv.put("n", n + 1);
      const greeting = await this.env.GREETER.greet("facet");
      return new Response(String(n) + ":" + greeting);
    }
  }
`;

export class Greeter extends WorkerEntrypoint {
  greet(name) {
    return `${this.ctx.props.greeting}, ${name}`;
  }
}

export class Supervisor extends DurableObject {
  fetch(request) {
    const worker = this.env.LOADER.get("app-v1", () => ({
      compatibilityDate: "2025-01-01",
      mainModule: "app.js",
      modules: { "app.js": CODE },
      env: {
        GREETER: this.ctx.exports.Greeter({
          props: { greeting: "hello" },
        }),
      },
    }));
    const facet = this.ctx.facets.get("app", () => ({
      class: worker.getDurableObjectClass("App"),
    }));
    return facet.fetch(request);
  }
}

export default {
  fetch(request, env) {
    return env.SUPERVISOR.getByName("only").fetch(request);
  },
};
