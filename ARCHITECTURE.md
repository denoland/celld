# Architecture: Self-Hosted Deno Deploy (MVP)

This document outlines the architecture of the MVP for Self-Hosted Deno Deploy,
a containerized, multi-tenant runtime for securely running JavaScript and
TypeScript applications using the Deno runtime.

## Goals

- Host multiple apps/plugins/scripts per instance
- Run each app in an isolated Deno subprocess
- Enforce per-app resource limits (CPU, memory) and permissions
- Support three invocation modes: path-based, wildcard subdomain, and full
  custom domain
- Provide a simple push-based deploy mechanism
- Expose structured logs and resource usage via OpenTelemetry (OTel)

## System Overview

The system consists of a single, self-contained runtime container that manages
multiple independent JavaScript/TypeScript applications:

- Accepts code deployments via HTTP (`/deploy`)
- Routes incoming requests (HTTP/WS) to the appropriate app
- Manages isolated Deno subprocesses for each app, enforcing permissions and
  resource constraints
- Utilizes Linux cgroups for fine-grained resource isolation (CPU, memory)
- Collects structured usage metrics, logs, and traces via OpenTelemetry (OTel)
- Implements network metering via per-app counters using iptables and Linux
  cgroup networking features

## Components

### Proxy Router (data-plane)

- Listens on port 4000
- Routes requests based on:
  - Host header (for domain-based routing)
  - Path (`/run?plugin=name` for path-based invocation)
- Serves static files and upgrades WebSocket connections
- Forwards requests to the corresponding Deno subprocess

### Deployment API (control-plane)

- Listens on port 4001
- Handles code uploads via `POST /deploy`
  - Accepts plugin name or domain
  - Stores code in local filesystem under `/apps/<name>`
  - Optionally processes `config.json`
- Initializes subprocess or marks app for lazy boot

### Subprocess Manager

- Manages lifecycle of Deno subprocess per app:
  - Uses real `deno` binary with version pinning
  - Applies permissions from `config.json` (file, network, environment)
  - Enforces resource limits using Linux cgroups v2:
    - `cpu.max` for CPU constraints
    - `memory.max` to cap memory usage, triggering an out-of-memory (OOM) kill
      if exceeded
    - Network bandwidth and usage metered using per-app network namespaces and
      iptables counters
- Monitors subprocesses and restarts them if they crash or upon re-deployment
- Implements scale-to-zero via idle timeout and automatic shutdown

### App Storage

- Apps live on disk at `/apps/<id>/`
  - `code/` contains source files
  - `config.json` defines runtime behavior
- App ID may be plugin name (`plugin-greeter`), wildcard domain
  (`foo.localhost`), or full domain (`app.example.com`)

### Observability & Metering

- Each Deno subprocess emits logs, distributed traces, and metrics
- Runtime collects detailed resource usage data, including:
  - CPU usage (time spent in user and kernel mode)
  - Peak and average memory footprint
  - Network bytes sent and received (tracked via iptables and cgroup networking)
  - Request count and invocation timings
- Exports structured metrics and logs via OpenTelemetry-compatible streams for
  easy integration with existing monitoring and alerting systems

## Invocation Modes

| Mode               | Example             | Use Case                | Security Strategy           |
| ------------------ | ------------------- | ----------------------- | --------------------------- |
| Path-based         | `/run/greeter`      | Plugin logic, webhooks  | JSON-only, no HTML response |
| Wildcard subdomain | `greeter.localhost` | Internal apps, previews | Assumes local use           |
| Custom domain      | `app.example.com`   | Full public apps        | Browser isolation, TLS      |

## Security Model

- Each application runs as a fully isolated subprocess
- Permissions strictly enforced through Deno CLI flags (`--allow-net`,
  `--allow-read`, etc.)
- Resource boundaries enforced via Linux cgroups for CPU, memory, and network
  usage
- Path-based invocations explicitly restricted to JSON or plain text responses
  to prevent cross-site scripting
- Network egress is carefully controlled and metered at the per-app level
- Logs, traces, and metrics are scoped and isolated per applicationlied using
  Linux cgroups
- Path-based apps restricted to non-HTML responses
- Logs and network access scoped per app

## Example Flow

1. User deploys a plugin:
   ```
   curl -X POST http://localhost:4001/deploy \
     -F 'name=greeter' \
     -F 'file=@main.ts'
   ```
2. Runtime saves code to `/apps/greeter/code/main.ts`
3. Proxy receives:
   ```
   POST /run/greeter
   ```
4. Subprocess manager starts (or reuses) Deno subprocess for `greeter`
5. Request is passed to subprocess and response returned

## Not in MVP (Future Work)

- Pull-based deployments
- Automatic TLS provisioning and certificate management
- Durable event queues and task retries
- Centralized domain-to-app mapping database
- Multi-instance coordination via a remote control plane
- Advanced resource quotas and dynamic scaling policies

- Pull-based deployments
- TLS provisioning and certificate management
- Durable background tasks and retry queues
- Full domain <-> app ID registry (via database)
- Control plane for multi-instance coordination
- Metering

## Summary

The MVP delivers a lightweight, powerful system for running secure JavaScript
apps in a multi-tenant runtime. It provides isolation, observability, and
deployment tooling out of the box—ready to be embedded, extended, or scaled.
