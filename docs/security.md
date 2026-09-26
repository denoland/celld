# Security

celld v0.6.0 is a beta release. It is not safe for hostile multi-tenant use.
Security fixes apply to the latest release only, so older beta releases do not
receive fixes.

## Security boundary

One fleet runs one application, and celld trusts the application code, the fleet
nodes, and the operators. Application code can use its configured bindings and
can consume shared node resources. Do not run code from mutually distrusting
tenants in one fleet.

The engine's host functions are not reachable from application code. An
internal script receives them as function parameters, so a bundle cannot name
a host function, and `globalThis` carries no `__`-prefixed property. A worker
that the Worker Loader loads with `globalOutbound: null` therefore reaches the
host only through the capabilities in its `env`.

celld depends on two external security boundaries:

- A trusted private network protects the internal listener. Use an encrypted
  overlay when the private network does not provide confidentiality.
- Object storage credentials control the fleet. Give each credential access to
  one fleet bucket only.

## Separate the listeners

celld opens two HTTP listeners. The public listener serves the deployed Worker,
and the internal listener serves the peer protocol and the operator API.

Use `--listen` for the public listener. Expose this listener through a load
balancer, a reverse proxy, or a public firewall rule.

Use `--internal-listen` for the internal listener. Its default address is
`127.0.0.1:0`, so celld selects an available loopback port at each start. Use
`--advertise` to give peers an address that reaches this listener.

An explicit advertised address requires an explicit internal-listener address.
celld also rejects an explicit non-loopback public listener without an explicit
internal listener. These checks prevent an obsolete one-listener configuration.

celld cannot verify a hostname or a translated port. You must route the
advertised address to the internal listener, and you must not route it to the
public listener.

The public listener reserves only `/.well-known/celld/health`. A healthy node
returns a 200 response with `{"ok":true}`, and an unhealthy node returns a 503
response. The deployed Worker owns `/health` and every other public path.

The internal listener does not pass an unknown path to the Worker. It returns a
404 response, so an operator request cannot become an application request.

## Protect each surface

| Surface | Required protection |
| --- | --- |
| The public listener | Terminate TLS and authenticate users in a proxy or the application. |
| The internal listener | Restrict access to trusted operators and fleet nodes. Use an encrypted overlay when the private network does not provide confidentiality. |
| The fleet bucket | Restrict the credentials to one fleet bucket. |

The internal listener has three request groups:

- Most operator routes let an operator inspect or control a node without
  request authentication.
- The `/peer/tunnel` route establishes a tunnel for cell fetch, RPC, and
  WebSocket calls. The establishment request carries the fleet HMAC, a clock
  limit, and replay protection, so only a holder of the fleet secret can open
  a tunnel. Each call then crosses inside the tunnel as plain HTTP with the
  cell scope in a reserved header, and these inner calls are not signed.
- The peer-control routes coordinate fleet nodes, and the reserved-cell routes
  access runtime state. These routes use the fleet HMAC for request
  authentication, a clock limit, and replay protection.

All three groups require the trusted private network.

The tunnel permits a request body to stream to the owner, so the fleet HMAC
cannot sign an individual call. The establishment signature decides who can
open a tunnel, and a tunneled call can address an application class or a
runtime class, so every path to a runtime class demands the fleet secret.
The signature does not authenticate the bytes after the establishment, and
it does not encrypt any traffic. The private network must therefore stay
trusted, and the fleet HMAC does not replace this network boundary.

celld does not terminate TLS on either listener. Do not expose the internal
listener to the public internet. Use an encrypted overlay such as WireGuard or
Tailscale when the network does not provide the required confidentiality.

## Use the internal operator API

The internal operator API is available in the released binary. It is an alpha
interface, so a release can change its paths or response formats.

These routes do not authenticate the caller:

- `/state` reports node state.
- `/cell/<SCOPE>` resolves or activates a cell.
- `/evict/<SCOPE>` tries to evict a resident cell and reports the result.
- `/do/<ID>` sends a direct request to an ordinary Durable Object.
- `POST /shutdown` starts a graceful ownership handoff. The
  `handoff=preserve` query prepares a same-node reload.

The `/do/<ID>` route refuses every reserved runtime class. These classes include
D1, Workflows, KV, and Queues. Their operator protocols can access application
data or change runtime state, so they use the HMAC-authenticated
`/runtime/<SCOPE>` route.

`/peer/probe` returns a signed diagnostic response. The peer protocol also
uses other reserved internal paths. An operator must not call these paths
directly.

### Read an eviction result

The eviction request waits for the accepted operation to finish. A refused
request does not wait for an idle period or another lifecycle transition.
An accepted request can join an eviction that is already running, and each
waiting caller receives that operation's result.

The Rust method `AppHandle::evict` returns `Result<EvictSuccess, EvictError>`.
The success value `Evicted` confirms that the awaited eviction completes its
runtime stop. The success value `AlreadyAbsent` confirms settled local absence
when the Actor handles the request. A caller that requires a completed eviction
must check for `Evicted`.

Multiple callers can join one eviction and receive `Evicted`, so the successful
call count does not measure the runtime stop count.
The error distinguishes a refusal, a cancellation, and an execution failure.
The Rust error methods `kind()` and `reason()` return the `kind` and `reason`
strings of the HTTP error body.

The response uses `application/json`. Both success values use status 200 with
`{"ok":true}`, so the HTTP response does not distinguish them.
It confirms a completed eviction or a settled local absence at the decision
point. A later request can reactivate the cell before the response arrives.

A missing cell or a settled `Inactive`, `Dormant`, or `Remote` cell counts
as locally absent. A pending activation, stop, or ownership transfer prevents
that result. The request does not evict a remote runtime. A dormant cell
keeps its ownership and its hibernated host sockets.

The node checks its availability before it reports local absence. The node
refuses an eviction during preservation, reload, or local inventory
confirmation, or when it lacks authority.

Every eviction error has this structure:

```json
{"ok":false,"error":{"kind":"refused","reason":"cell_active"}}
```

The `kind` and `reason` values have these meanings:

| Status | Kind | Reason | Cause |
| --- | --- | --- | --- |
| 409 | `refused` | `cell_active` | The cell has active work or a socket that requires a runtime. |
| 409 | `refused` | `cell_transitioning` | The cell has another lifecycle transition. |
| 409 | `refused` | `alarm_imminent` | The alarm residency policy retains the cell. |
| 409 | `refused` | `alarm_uncovered` | The alarm coverage is unconfirmed, or a firing alarm blocks eviction during pressure shedding. |
| 503 | `refused` | `node_unavailable` | The node cannot accept the eviction. |
| 503 | `refused` | `eviction_limit` | The node has reached its eviction concurrency limit. |
| 409 | `cancelled` | `new_activity` | A new request cancels an accepted eviction. |
| 409 | `cancelled` | `alarm_activity` | An alarm observation or firing cancels an accepted eviction. |
| 409 | `cancelled` | `node_fenced` | The node loses its authority during the eviction. |
| 503 | `failed` | `actor_unavailable` | The request cannot reach the Actor. |
| 500 | `failed` | `reply_lost` | A delivered request loses its reply, so its outcome is unknown. |
| 500 | `failed` | `durability_failed` | The durability verification fails. |
| 500 | `failed` | `durability_timeout` | The durability verification exceeds its operation deadline. |
| 500 | `failed` | `runtime_stop_failed` | The runtime stop reports a failure. |

The runtime stop has no overall timeout. A stop that does not return keeps
the eviction request pending.

An error does not prove that the runtime remains resident. A malformed scope
still returns status 400 through the existing scope validation.

## Set the forwarded-header policy

celld ignores `X-Forwarded-Host` and `X-Forwarded-Proto` by default. Set
`--trust-forwarded-headers` or `CELLD_TRUST_FORWARDED_HEADERS=1` only when a
trusted proxy replaces both headers. celld uses the last value in each header,
so an earlier client value does not override the proxy value.

celld always takes the path and query from the request target. It ignores the
scheme and authority in an absolute-form target, so a client cannot bypass the
host policy through the request line.

Without a trusted proxy, the `Host` header controls the hostname in
`request.url`. celld accepts a hostname, an IPv4 address, or a bracketed IPv6
address, with an optional port. It rejects malformed and noncanonical values,
and it uses `celld.local` when no source gives a valid host.

These checks keep the path and query valid, but they do not make the hostname
trustworthy. An application must not use an unchecked hostname for an
authorization decision. Use a trusted proxy or check the hostname against a
list in the Worker.

## Limit request bodies

The public Worker listener and `/do/<ID>` have a 1 GiB request body limit by
default. Set `CELLD_MAX_REQUEST_BODY_BYTES` to a smaller positive value to
change the limit. celld returns status 413 for a declared oversized body and
when a Worker reads past the limit.

For a method other than `GET` or `HEAD`, `/do/<ID>` streams a body when its
declared length is at least 1 MiB or its length is unknown. celld collects each
smaller body before dispatch.

## Protect the fleet bucket

The fleet bucket is the root of authority for the fleet. It stores the
deployments, the cell state, the ownership leases, the node leases, and the
shared peer-authentication secret.

A person who holds the bucket credentials controls the fleet. Give each
credential access to one fleet bucket only, and replace a credential after a
suspected disclosure.

## Understand cell ownership

Each cell is a SQLite database with one writer. One node owns a cell at a time,
and an ownership epoch fences each cell. A node that loses its lease cannot
modify the current cell state.

This fencing protects storage consistency, but it does not isolate hostile
applications. See [what celld guarantees](guarantees.md) for the storage protocol,
and see [limitations](limitations.md) for the complete beta boundary.
