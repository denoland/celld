# Self-Hosted Deno Deploy

**Run JavaScript apps & functions securely. Anywhere.**

A self-contained runtime for securely hosting JavaScript/TypeScript apps,
plugins, and APIs real Deno subprocesses.

Push code. Route traffic. Enforce limits. Observe everything.

**One container. Multi-tenant. No cloud needed.**

## Development Setup

This project is a Rust application that requires building for Linux ARM64, even if you're developing on another platform like macOS. We use Docker to build and run the project.

### Prerequisites

- Docker installed on your development machine
- Rust (optional, for local development)

### Building and Running

1. Clone this repository
2. Run the build script:

```bash
./build.sh
```

This will build a Docker image and run the container with the proxy listening on port 3000.

### Environment Variables

- `APPS_DIR`: Directory where app code is stored (default: "./apps")

## Why?

You need to run dynamic code in production—but:

- You don’t trust it fully
- You want isolation, observability, and control
- You don’t want to spin up Kubernetes or roll your own platform

Self-hosted Deno Deploy gives you a drop-in container to:

- Run per-tenant logic or plugins
- Host full HTTP/WS apps
- Execute webhooks or background jobs
- Track resource usage and runtime behavior

## Get started

```bash
docker run -p 3000:3000 -p 3001:3001 --rm -ti denoland/deploy
```

Port 3000 is the data plane, port 3001 is control plane.

## Deploy an app or plugin

```bash
deno deploy -i local -d greeter
```

Or via `curl`:

```bash
curl -X POST http://localhost:3001/deploy \
  -F 'plugin=greeter' \
  -F 'file=@main.ts'
```

**main.ts**

```tsx
import { Hono } from "npm:hono";
const app = new Hono();
app.get("/", (c) => c.text("Hello " + c.req.query("name")));
export default app;
```

Execute it

```bash
curl http://localhost:3000/run/greeter?name=Alice
# → "Hello Alice"
```

## Host a full web app

```bash
deno deploy -i local -d app.example.com
```

**main.ts**

```
Deno.serve(() => {
  return new Response("<h1>Hello from your app</h1>", {
    headers: { "Content-Type": "text/html" }
  });
});
```

```bash
curl -H "Host: app.example.com" http://localhost:3000
```

Serve full apps over custom domains or wildcard subdomains with proper
isolation.

## Invocation options

- **Path-based**

  `POST /run/plugin=name`

  → Great for lightweight plugin logic, webhooks, jobs

- **Wildcard subdomain**

  `name.localhost`

  → Good for preview apps or internal multi-tenant routing

- **Custom domain**

  `app.example.com`

  → Ideal for fully hosted frontend or backend apps

---

## What makes this different from `deno run`?

| Feature       | `deno run`        | Self-Hosted Deno Deploy                 |
| ------------- | ----------------- | --------------------------------------- |
| Target        | One app           | Many apps, scripts, plugins             |
| Isolation     | Manual            | Subprocess + Deno permissions + cgroups |
| Routing       | You build it      | Built-in by domain or plugin name       |
| Observability | None              | OTel-compatible logs and metrics        |
| Deployments   | Manual or CI only | Push/pull-based model                   |
| Multi-tenancy | No                | Yes                                     |

## Key features

- **Real Deno subprocess per app**
- **CPU & memory limits via cgroups**
- **Per-app permissions (`--allow-net`, etc.)**
- **Static files + WebSocket support**
- **Push-to-deploy from CLI or CI**
- **OpenTelemetry usage export**
- **Zero-config startup for dev**
- **Custom and wildcard domain support**

## Who it's for

- Teams building plugin systems
- Internal platforms running user code
- SaaS builders replacing `eval()`
- Engineers looking for a small, safe function host

> It’s eval() you can trust.
>
> It’s Cloudflare Workers—but yours.
>
> It’s what `deno run` would be if it scaled to 100 apps.

## Coming soon

- Pull-based deployments
- Built-in TLS via Let’s Encrypt
- Durable event queues
- Federated control plane
