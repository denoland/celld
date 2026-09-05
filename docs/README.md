# celld documentation

celld is a stateful distributed system. It runs server-side JavaScript on
your machines, and it stores the long-term state in a bucket that you own:
S3-compatible, Google Cloud Storage, or Azure Blob Storage. The JavaScript
API and the configuration follow Cloudflare Workers: Workers, Durable
Objects, KV, Queues, D1, R2, Workflows, Cron Triggers, and static assets.

In Cloudflare terms, a cell is a Durable Object: a small server with a
name and a private SQLite database. You make one cell for each user, each
document, each chat room, or each AI agent. A cell serves HTTP, holds
WebSocket connections, sets alarms, and makes outbound connections. Cells
share no database, so the application divides into cells from the start.
Each cell runs on one thread: a second request interleaves only while the
first awaits, and storage operations are synchronous and never interleave,
so the data in a cell stays consistent.

You run celld as one process on each machine. That process is a **node**,
and the nodes that share one bucket are a **fleet**. Any node can serve
any cell, so you add capacity by starting another node against the same
bucket.

A cell has the same states as a Durable Object. A **resident** cell is in
memory: it is **active** while it does work, and **idle** when it waits.
celld removes an idle cell from memory after `CELLD_IDLE_EVICT_S` seconds
without work, and earlier under memory pressure or at the residency cap.
Without `CELLD_IDLE_EVICT_S`, an idle cell stays in memory until pressure
or the cap removes it. A cell that then keeps its
hibernatable WebSocket clients, and stays on its node, is **hibernated**.
A cell that no node holds is **inactive**. An inactive cell is only an
object in the bucket, so it costs almost zero, and every cell starts in
this state.

A cell keeps no memory across these transitions, so the constructor
runs again on the next event. A hibernated cell wakes the same way a
cold start does, except that its WebSocket clients stay connected and
it stays on its node.

One 8 GB node holds 1,000 resident cells, so one resident cell costs
approximately $0.05 each month.

Exactly one node serves a cell at a time. The nodes do not elect a leader
or keep a membership list: a node claims a cell by writing a small record
to the bucket, and it writes that record with a condition the storage
enforces — the write succeeds only if nobody else changed the record
first. Object storage therefore decides who wins, and two nodes cannot
both claim the same cell. The claim expires unless the node keeps
renewing it, so a machine that dies releases its cells without anyone
having to declare it dead.

celld does not answer a write until the data survives a failure, so no
write you were told succeeded is ever lost. That guarantee is called
RPO=0, for a recovery point of zero.

How celld earns it depends on how many nodes you run. One node writes the
data to the bucket first, which costs one storage round trip. Two or more
nodes are faster: the node serving the cell sends each write to another
node as well, and answers as soon as that node has the data on its own
disk. celld uploads the data to the bucket afterwards, so the bucket
still holds the long-term state. Run two or more nodes if write latency
matters to you; a single node has nobody to send to, so every write waits
for the bucket. `CELLD_DURABILITY` selects this behavior and defaults to
`fleet`.

When a node stops, another node takes the cell over. It first collects
whatever the stopped node had not uploaded yet, so the takeover starts
from a complete history. The [guarantees](guarantees.md) page
gives the full mechanism and the exact properties that the bucket must
provide.

When an event sets an alarm before its response boundary, celld does not send a
successful response until a durable wake entry covers the alarm. celld does
not let a later `waitUntil` alarm delay another event's response.

A hibernated cell fires its alarm on the node that owns it. One node in the
fleet holds the waker role at a time. The waker reads the due wake entries of
the whole fleet, and it wakes only the cells whose owner node has stopped. Every
other node wakes only its own hibernated cells, so the alarm load of the fleet
does not queue on one node.

## What do you build with cells

A cell fits a workload that divides into named, stateful units:

- **Real-time applications.** A multiplayer game, a chat room, or a
  collaborative document is one cell. The cell holds the WebSocket
  connections and the state of one room, so the room needs no lock and
  no external message bus.
- **Agents.** Each AI agent is one cell. The cell holds the memory, the
  schedule, and the inbox of one agent in its own SQLite database. An
  inactive agent has no resident process, so a large agent fleet costs
  almost nothing between events.
- **Sharded web applications.** One cell for each user, each tenant, or
  each device shards the application from the start. The contention of
  one shared database does not appear, because no shared database
  exists.

## Contents

- [What do you build with cells](#what-do-you-build-with-cells)
- [Install](#install)
- [Configure object storage](#configure-object-storage)
- [Deploy an application](#deploy-an-application)
- [Operate D1 and KV](#operate-d1-and-kv)
- [Start a node](#start-a-node)
- [Add nodes](#add-nodes)
- [Shut down and roll out a node](#shut-down-and-roll-out-a-node)
- [Diagnose a fleet](#diagnose-a-fleet)
- [List Durable Objects](#list-durable-objects)
- [Environment variables](#environment-variables)
- [Cloudflare compatibility](cloudflare-compat.md)
- [What celld guarantees](guarantees.md)
- [Limitations](limitations.md)
- [Security](security.md)
- [Telemetry](telemetry.md)
- [Testing](testing.md)
- [WebAssembly](wasm.md)

## Install

The installer downloads the `celld` binary. Replication runs inside the
celld process, so a node needs no external replicator.

```sh
curl -fsSL https://celld.dev/install.sh | sh
```

If the installer tells you, add `~/.local/bin` to `PATH`. To install one
exact release, set `CELLD_VERSION` to its tag, for example `v0.0.1`; to
go back, run the installer again with the previous tag. The releases are
on [GitHub](https://github.com/denoland/celld/releases), and each release
has a GitHub Actions build attestation: verify a downloaded file with
`gh attestation verify <asset> --repo denoland/celld`.

## Configure object storage

The `s3://`, `gs://`, and `az://` scheme names are case-insensitive.
celld ignores leading and trailing whitespace in `CELLD_BUCKET`.

For an S3-compatible bucket, celld uses the standard AWS credential
chain. On Amazon EKS, celld reads the Pod Identity credentials from the
injected environment variables and the authorization-token file. For
Cloudflare R2: create a bucket, create an S3 API token with access to
that bucket, and set these variables:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=auto
export S3_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com
export CELLD_BUCKET=s3://YOUR-BUCKET
```

For Google Cloud Storage, celld uses Application Default Credentials.
Create a bucket. Then authenticate with `gcloud auth application-default
login`, or point `GOOGLE_APPLICATION_CREDENTIALS` at a service-account
key that has access to the bucket. Then set the bucket:

```sh
export CELLD_BUCKET=gs://YOUR-BUCKET
```

A `gs://` bucket takes no `S3_ENDPOINT` and no `AWS_*` credentials, and
celld ignores the storage region. On a Compute Engine instance, celld can
use the attached service account, but the access scopes of the instance
cap this credential, and the default scope permits only storage reads.
Create the instance with the `cloud-platform` scope, so the IAM role of
the service account controls the access.

For Azure Blob Storage, the bucket NAME is the container, and the
storage account comes from the environment. Create a container. Then
set the account and one credential:

```sh
export AZURE_STORAGE_ACCOUNT_NAME=YOUR-ACCOUNT
export AZURE_STORAGE_ACCOUNT_KEY=...
export CELLD_BUCKET=az://YOUR-CONTAINER
```

celld accepts three Azure credential families — a storage account key, a
managed identity, and a workload identity — and you must configure
exactly one. A system-assigned managed identity needs only the account
name. A user-assigned managed identity needs exactly one selector among
`AZURE_CLIENT_ID`, `AZURE_OBJECT_ID`, and `AZURE_MSI_RESOURCE_ID`,
because two selectors can name two different identities. An identity
must hold the Azure Blob data-plane permission to read, write, list, and
delete blobs; the built-in
[`Storage Blob Data Contributor`](https://learn.microsoft.com/azure/role-based-access-control/built-in-roles#storage-blob-data-contributor)
role supplies it.

celld reads a managed identity from the Azure instance metadata service,
which an Azure VM and an AKS node supply. Azure App Service and Azure
Container Apps supply a different endpoint, so a managed identity does
not work there; use a workload identity or an account key instead. The
standard AKS workload identity environment (`AZURE_AUTHORITY_HOST`,
`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`)
works with the public authority host, `https://login.microsoftonline.com`,
with or without a trailing slash; celld rejects a sovereign or custom
host. celld also rejects each recognized Azure configuration variable
outside these families — another credential source, an endpoint override,
the OneLake endpoint — and it ignores an `AZURE_*` name that
`object_store` 0.12 does not recognize, because that name cannot change
the client.

An `az://` bucket takes no `S3_ENDPOINT` and no `AWS_*` credentials, and
celld ignores the storage region. celld qualifies an `az://` bucket for
a production fleet. The startup test verifies the conditional writes and
ranged reads. A live Azure test from 2026-08-18 covers the multipart upload
path. See [guarantees](guarantees.md). For local development against Azurite,
set `AZURE_STORAGE_USE_EMULATOR=true`; Azurite is a development store, so
celld does not qualify it for a fleet either.

The bucket credentials give full control of the fleet, so keep them
safe. The bucket contains the deployments, the SQLite replicas, the
ownership records, the node leases, and the peer-authentication secret.

A bucket value can add a key prefix: `s3://YOUR-BUCKET/PREFIX`. Every
object of the fleet then goes below `PREFIX/`, so two fleets can share one
bucket. A bucket value without a prefix keeps the objects at the root of
the bucket, therefore an existing fleet does not move its data.

The store must provide conditional writes. A ranged read must return the
exact requested bytes. The store must also provide read-after-write
consistency. Cell ownership and stored data depend on these properties.
Amazon S3, Cloudflare R2, Google Cloud Storage, Tigris, and Azure Blob Storage
qualify; Backblaze B2, Hetzner, and DigitalOcean Spaces do not. MinIO
(the community edition) passes the storage test, but celld has not
qualified it for production; do not use RELEASE.2025-09-06T17-38-46Z,
which rejects the conditional create that the first deploy sends
(denoland/celld#162). See [what celld guarantees](guarantees.md) for the
exact requirements.

## Deploy an application

If the project contains Worker code, install `esbuild` on `PATH`; an
asset-only project does not need it. Then run `celld deploy` from an
applicable Wrangler project:

```sh
git clone https://github.com/denoland/celld
cd celld/examples/counter
celld deploy . \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

`celld deploy` accepts module Workers, Durable Object bindings, service
bindings, variables, cron triggers, D1 databases, KV namespaces, Queues,
R2 buckets, Workflows, WebAssembly modules, and static assets. An asset project can include a
Worker or be asset-only, and the asset functions include the assets
binding, HTML handling, not-found handling, worker-first routes,
`_headers`, and `_redirects`. If the Wrangler configuration contains an
unknown key, the deploy stops with an error. See the
[Cloudflare compatibility](cloudflare-compat.md) page for the complete
deployment boundary.

A new deployment manifest records the full SHA-256 digest of each JavaScript
or WebAssembly module. A node verifies each module before it builds the
deployment, so changed module bytes cannot become active. A node also accepts
a legacy 16-character digest and verifies that prefix, so an upgrade can load
an existing deployment.

A running node does not restart for a new deployment. Each node reads
`deploy/current.json` every 30 seconds (`CELLD_DEPLOY_POLL_S`) and adopts
a new deployment in place, and `POST /reload` on the internal listener
makes a node adopt the pointer now. A node builds the new deployment
beside the one it serves, and then it switches new requests to the new
deployment in one step. A request that started on the previous deployment
finishes on it. A deployment that does not build leaves the current
deployment serving, and the node reports the failure in its log and in
the `/reload` response. `POST /reload` also rebuilds an unchanged
deployment, so an edit to `CELLD_VARS_FILE` takes effect without a
restart.

A Durable Object that is not resident runs the new deployment at its
next activation. A resident Durable Object moves to the new deployment
at a safe point: no request runs in it, no alarm handler runs in it, no
output waits for durability, and no regular WebSocket is open. A request
that arrives while the object moves waits for the new code. The move
keeps the object's storage, its epoch, and its hibernatable WebSockets,
and it does not read or write the bucket. An object that reaches no safe
point in `CELLD_DEPLOY_MAX_AGE_S` seconds (default 60) is forced: celld
cancels its running work and closes its regular WebSockets with code
1012, which is what a Cloudflare deployment does to every object. A
value of 0 forces every resident object at the adoption. In this window,
a request on one deployment can call a Durable Object on the other, so
two adjacent versions must accept each other's calls. The `/state`
response reports the deployment a node serves, the deployments it still
drains, the objects that are moving, and the deployment each resident
object runs.

## Develop an application locally

Run `celld dev` in a Wrangler project:

```sh
celld dev
```

The command opens a local object store, deploys the application, and starts
one celld node. It does not require Docker or a cloud bucket. The supervisor
stops the node when the command finishes or returns an error. On Linux, the
kernel also stops the node if the supervisor exits abruptly.

The Worker listener uses `http://127.0.0.1:9876` by default. Use `--port` to
select a different port:

```sh
celld dev --port 3000
```

Use `--host` to select the Worker listener interface:

```sh
celld dev --host 0.0.0.0
```

A non-loopback IP exposes the Worker listener to the network. The internal
operator listener stays on loopback, so another machine cannot use its
operator API.

The default display uses color to identify each status and the application
URL. It hides the node warning and information logs, so the errors and the
listener remain easy to find. Use `--logs` to show these logs:

```sh
celld dev --logs
```

Set `NO_COLOR` to disable color. Set `FORCE_COLOR` to enable color when
the output is not a terminal. `NO_COLOR` always takes priority when both
variables are set.

You can also give the command a project directory or a Wrangler
configuration file:

```sh
celld dev ./examples/counter
```

The command stores the local objects and the celld work files in `.celld/dev`
below the project directory. Add `.celld/` to the application's `.gitignore`
file. A normal shutdown keeps this directory, so the next invocation uses the
same durable application state.

This persistence holds across a configuration change, and celld does not
migrate the stored state to match the new configuration. An object therefore
keeps a value that the previous configuration wrote, and the application can
reject that value and fail. The error names the stored value and not the
configuration, so the failure can look unrelated to the change you made. Use
`--clean` to start the command from an empty local state:

```sh
celld dev --clean
```

The flag deletes `.celld/dev` before the server starts, and it deletes nothing
else below `.celld`. The command reports the outcome on the console. An absent
`.celld/dev` directory is the wanted result, so the flag succeeds on a project
that never ran `celld dev`.

The command does not expose the local object store through a fleet flag. A
regular node or an operator subcommand must use a supported cloud bucket.

The command watches the project directory. A source or configuration
change builds a new deployment and restarts the local node. The current
application continues to run during the build, and a failed build does
not replace it. The restart retains the durable application state.

A read of a project file is not a change, so a tool that only reads the project
does not start a build.

The watcher ignores `.celld`, `.git`, `.wrangler`, `node_modules`, and `target`
directories at each depth. Use `--watch-ignore PATTERN` to ignore an additional
project-relative glob, and repeat the option to add more globs. The
`--watch-ignore` option cannot be used with `--no-watch`. Use `--no-watch` to
disable all automatic builds and restarts.

The watcher does not watch a source file outside the project directory. Worker
projects need `esbuild` on `PATH`, and asset-only projects do not need it.

## Operate D1 and KV

`celld d1` runs SQL and migrations against a deployed D1 database. The
command uses the fleet bucket to find a node, and the node routes the
operation to the database cell.

```sh
celld d1 migrations apply ledger --bucket "$CELLD_BUCKET"
```

The `migrations_dir` value must be a relative path inside the project. An
absolute path or a path with a `..` component is invalid.

`celld kv` reads and writes a deployed KV namespace. The bulk commands use
the Wrangler file format, so `wrangler kv bulk get` can export data for
`celld kv bulk put`.

```sh
celld kv bulk put sessions wrangler-export.json --bucket "$CELLD_BUCKET"
```

`celld kv bulk get` writes each row before it reads the next value, so the
command does not retain the complete namespace. A named output file changes
only after the export completes. If a stdout export fails, the output remains
an incomplete JSON array and a consumer cannot import it as a complete export.

`celld kv list` prints at most 1000 keys, because a namespace can hold
many more keys than an operator wants to read. The command reports on
stderr that more keys exist, and it gives the `--after KEY` that continues
the listing. Pass `--all` to read every key, or `--json` for one JSON
object per key.

Every celld command writes its data to stdout and its messages to stderr,
so a redirect or a pipe carries only data:

```sh
celld kv list sessions --all --json --bucket "$CELLD_BUCKET" > keys.ndjson
```

`celld --help`, `celld --version`, and the listener announcements use the same
stdout sink. The sink treats a closed pipe as a successful stop, and it reports
all other write and flush errors.

## Start a node

For local development, the default listener is sufficient:

```sh
celld \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

For a fleet node, bind the public and internal listeners separately. The
ingress can reach the public listener, and the other nodes can reach the
internal listener:

```sh
celld \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION" \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise node-a.internal:8081
```

An explicit advertised address requires an explicit internal-listener
address: set both command-line options, or their equivalent environment
variables. celld also rejects an explicit non-loopback public listener
without an explicit internal listener, because that shape identifies an
obsolete one-listener configuration.

celld cannot verify that an advertised hostname or a translated port
reaches the internal listener. You must route the advertised address to
the internal listener, and you must not route it to the public Worker
listener.

## Add nodes

Start each node with the same bucket settings. Give each internal listener
a different address that the other nodes can reach, and set `--advertise`
to that address. The nodes find each other through the leases in the
bucket; there is no join command and no fixed membership list.

The bucket supplies discovery and authority, not network reachability.
Cell fetch and RPC traffic has a protocol version but has no content
signature, so the private network and its code are the security boundary.
Peer-control and reserved-cell operator requests use the fleet HMAC, but celld does not
terminate TLS. Put the advertised addresses on a private network that you
trust, or use an encrypted overlay such as WireGuard or Tailscale. The
internal listener also has an unauthenticated operator API, so it must not
reach the public internet. See the [security](security.md) page for the
complete boundary.

The listener that receives a request decides where a new cell activates,
so a load balancer must include a new node in its rotation. A node below
its residency cap keeps every cell that activates on it. A node at its
cap, and a node that sheds under memory pressure, hands the activation to
the peer with the lowest load, and an empty node wins that comparison. A
shed eviction releases the ownership record
(`CELLD_PRESSURE_OWNERSHIP=release`, the default), so the next request
can move the cell. An idle eviction keeps the ownership record, so an
idle cell wakes on its own node. An idle eviction must stop the runtime
within `CELLD_OPERATION_DEADLINE_MS`. If the runtime does not stop in
time, the node keeps the cell resident at its current epoch and waits
one more idle period before the next attempt.

A new node also receives existing idle cells. Every node reads a shared
fleet sample every five seconds (`CELLD_REBALANCE_INTERVAL_MS`; `0`
disables balancing). One node claims each refresh through a conditional
write and reads the node leases for the fleet. The other nodes read
the result from `fleet/capacity-v1.json`. A node judges the leases in a
sample at the moment the sample was taken, because the copied leases age
while the nodes renew the originals. A sample expires after one
configured interval. A node that reads during a refresh keeps the
previous sample while that sample is less than three intervals old. A
refresher that fails puts the previous sample back, so the next reader
refreshes at once, and a refresher that stops holds its claim for at most
30 seconds. An unavailable sample stops balancing and closes the
bucket-format gate.
With balancing disabled, the format check still reads the shared sample
every five seconds. A node that hands an activation to a peer reads the
same shared sample, so a burst of activations at a full node costs one
read each. Readiness, a drain, follower recruitment, and lease authority
read the node leases directly.

Each node has an ownership target: the fleet's owned cells, divided in
proportion to the node weights
(`CELLD_PLACEMENT_WEIGHT`, default: the CPU count). The node with the
most owned cells per unit of weight hands at most 32 idle cells
(`CELLD_REBALANCE_BATCH_CELLS`) to the peer that is furthest below its
target, and it hands over no more cells than the receivers have room
for. A receiver fills to 2% below its target, so a stale sample cannot
make two nodes trade the same cells. Only a hibernated cell moves. Its
move is one ownership record write and one signed acquire, it stays
hibernated on the new owner, and its parked hibernatable WebSockets close
with code 1012 so the clients reconnect to the new owner. A resident cell
does not move: it hibernates through idle eviction first
(`CELLD_IDLE_EVICT_S`) and moves after. Therefore a fleet without idle
eviction balances only the cells that hibernate on their own. A request
that arrives for a moving cell waits for the new owner and then follows
the normal route to it. A node that drains receives no cell, and a node
with cold activations queued behind its activation ceiling (`restoring` in
its lease) receives no cell until that backlog clears. The fleet moves nothing while a
node reports a lease without a weight, so a rolling upgrade to this
version completes before the first move. An operator can pause the fleet with
`POST /rebalance/pause` on the internal listener of any node.

## Feed an autoscaler

celld does not scale itself. An external system starts and stops the
nodes, and celld hands the cells off. Two surfaces report the numbers
that a scaling decision needs.

Each node writes its lease to `nodes/<node>.json` in the bucket, and
each renewal refreshes it (`CELLD_TTL_MS`, default 10000, renewed at
one third of the lifetime). The lease carries a load block:
`owned_cells`, `placement_weight`, `resident_cells`, `host_websockets`,
`rss_bytes`, `in_use_bytes`, `cpu_percent_x100`, `open_fds`, `pressured`,
`memory_headroom`, `shed_cells`, and `restoring`. A script can read these
objects with the
bucket credentials alone, and `sampled_ms` dates each sample. The
`owned_cells` value includes the dormant cells and the cells that are waking
from dormancy. A started node omits the value until it has read the
ownership record of every cell on its own disk, so a restarted node does
not report an empty node while it still owns its hibernated cells. The
`resident_cells` value includes only the cells that consume the local
runtime capacity.

The internal listener answers `GET /state` with the live counters:
`owned_cells`, `occupied`, `capacity_waiting`, `activation_waiting`, `restoring`,
`shedding`, and the memory values. The `handed_off` counter is the number of
cells this node gave to a peer, `rebalanced` is the part of that number
that balancing moved, and `rebalance_failed` is the number of balancing
moves that no peer acquired. A positive `capacity_waiting` means
that the node queues activations behind its residency cap, and that is
the direct signal to add a node. A `pressured` lease is the memory form
of the same signal. Scale down only when every remaining node reports
`memory_headroom` and a small `restoring` backlog. A drain into a fleet
without spare capacity parks the cells dormant on the survivors, and
each later activation must then shed a resident cell to run.

The public `/.well-known/celld/health` path stays a boolean. It reports
503 during a drain and before a joining node settles, so an
orchestrator can gate a rolling update on it, but it carries no
utilization number.

## Shut down and roll out a node

celld shuts a node down gracefully on SIGTERM or SIGINT, the signals that
`systemctl stop`, `docker stop`, and a Kubernetes pod delete send. The
`/.well-known/celld/health` path reports the node as unhealthy, so a load balancer
stops public routing to it. New public requests receive a 503 response,
and celld closes each connection so the client can retry on a healthy
node. The node finishes the public HTTP requests that it accepted before
shutdown, and it continues to accept versioned peer traffic for cells that
it has not handed off.

You must set a longer orchestrator stop grace, such as systemd
`TimeoutStopSec` or Kubernetes `terminationGracePeriodSeconds`. The grace
must cover the drain-token wait, the expected complete handoff, and one
no-progress interval (the settings below). Otherwise, the orchestrator
can send SIGKILL before celld completes the handoff.

The handoff works in batches. The node reserves a batch of cells and
stops new local routes to them: an accepted request stays local until it
finishes, and a new request waits for the successor route. The node
also cancels each firing alarm in the batch. The alarm runtime records a retry
and arms its wake entry, so the successor can run the alarm at least once.
The node cancels an active internal fetch or RPC handler before the durability
proof, so a Queue broker cannot retain the batch until the process stops.
The node proves the batch durable in the live durability ensemble, and it
then closes each database. The node publishes one full L9 snapshot of the
closed database and verifies that the bucket contains a restore object. The
successor therefore does not replay the complete transaction history. If the
snapshot fails, the node materializes the additive L0 chain within the same
durability deadline. A database larger than the deadline can upload, 80 MiB
at the default deadline of 10 seconds, skips the snapshot and hands off
through its L0 chain at once, and a snapshot that fails once is not retried
for that cell. The node does not release the cell unless one complete
restore path is visible. The node then releases each ownership
record and asks a
compatible peer to acquire it. The peer acknowledges after the ownership
update and keeps the cell dormant, so the handoff does not restore an
unused runtime; a later request starts the cell under the peer activation
limit. The donor starts the next batch after each acknowledgement.

celld orders the resident cells by their local request count. The newest local
request ID resolves an equal count, and the cell ID resolves an unused cell.
Therefore, a total stop bound moves shared resources before one-time resources.

Four settings pace this work. `CELLD_RELEASES` sets the maximum number
of complete handoffs in progress (default 128), covering activity
cancellation, the durability proof, the final snapshot, the ownership
release, and the successor update; `CELLD_ACTIVATIONS` limits the
demand-driven restore and startup work. `CELLD_SHUTDOWN_DRAIN_MS` sets
the maximum interval without a completed handoff (default 25000). Each
successor acknowledgement starts the interval again, so a large handoff
can continue while it makes progress. `CELLD_SHUTDOWN_TOTAL_MS` sets a 40000 ms
default bound for the complete process stop. An orchestrator stop grace must
be longer than this bound, so celld can finish its local durability shutdown.
The `CELLD_DRAIN_TOKEN_WAIT_MS` value must not exceed three quarters of the
total bound. celld rejects a larger value at startup, so one quarter remains
for the request drain, the handoff, the transport flush, and the local
durability shutdown.
A handoff reserves at most one second and ten percent of this bound for the
drain-token release. This reserve does not apply to a same-node preserve.
A same-node preserve operation uses the shorter drain value as its semantic
limit because it has no successor acknowledgements.

Simultaneous stop signals do not flood the surviving nodes. A draining
node claims a fleet drain token in the bucket before it releases cells,
so concurrent donors hand off one node at a time. A donor that cannot
claim the token within `CELLD_DRAIN_TOKEN_WAIT_MS` milliseconds (default
30000; `0` disables the token) proceeds without it, because the
orchestrator grace is finite. The token is advisory: a dead holder's
claim expires, and a handoff without the token is still safe.

A fresh process also holds its first healthy response until the fleet is
settled. The process requires its live node lease, no active donor, and memory
below every pressure low watermark on each live node. A donor records each
node's restore backlog before it releases ownership. The replacement permits
one `CELLD_ACTIVATIONS` budget above each recorded value. The incumbent
ownership counts must remain within one equal successor share above the fleet
mean. A joining process can satisfy this condition when it advertises paced
handoff support and owns fewer cells than the busiest incumbent. The process
publishes this successor capacity before readiness, so an idle rollout can
repair the ownership distribution during the next donor handoff.

An initial spare has no drain token, so it has no rollout restore baseline to
check. It still requires memory headroom and acceptable ownership distribution.

An older peer does not publish the low-watermark result, so the gate uses that
peer's pressure latch during a mixed-version update. An unreadable fleet or an
unsettled condition keeps the process unhealthy until the condition clears.
`CELLD_READY_FLEET_GATE_MS` sets the observation deadline in milliseconds
(default 120000; `0` disables the gate). celld emits `ready_gate_expired` once
at that deadline, but readiness stays closed. Set an orchestrator rollout
deadline so a persistent capacity problem fails the rollout. After the first
healthy response, fleet state does not remove readiness again.

A deadline-cut handoff can leave a node-log recovery for the replacement.
One process reads and uploads that dead session, and the other processes wait
for its result. A waiting process can replace an unresponsive recovery after
30 seconds, so a failed recovery cannot block the fleet permanently. A
recovery store error keeps the session unsealed, so a later recovery must read
every retained bundle before the replacement can restore the acknowledged
state.

Recovery records a checkpoint after each group of 32 completed cell epochs.
A later attempt uses these checkpoints to skip repeated coverage checks and
uploads for the recorded transactions. Each checkpoint follows the data uploads,
so an interrupted upload cannot create a checkpoint for incomplete data.
An unavailable or invalid checkpoint causes recovery to check the cell coverage
again. A checkpoint write failure does not stop recovery. The checkpoints do
not replace the follower evidence or the session seal, and different sessions
continue to recover independently.

The internal listener continues to accept `/state` requests during the
drain, and the response reports the handoff and restore counters. The
public health response identifies the active drain with a 503 status.

A remote cell call can resolve an owner generation that a replacement process
rejects. celld then waits for a different `(node, epoch)` and refreshes the
route. The wait uses `CELLD_OPERATION_DEADLINE_MS` as its total bound (default
15000). celld retries only a peer attempt that proves that the handler did not
start. It does not retry an ambiguous attempt because the handler can have
completed without returning its response.

A marked stale-owner response to the outer peer tunnel proves that the
application request did not start. celld can therefore refresh that route for
an HTTP, an RPC, or a WebSocket request.

An application must keep one stable operation ID when it retries an ambiguous
fetch, RPC, D1, or service operation. A WebSocket transport cannot move between
owners, so a client must reconnect and keep the same application operation ID.

To roll out a new version, use the rolling update of your orchestrator:
stop each node with SIGTERM, wait for its replacement to report healthy,
then move to the next node. celld paces the cell handoffs inside each node
shutdown, and the first-readiness gate paces the update against fleet
recovery, so a deployer does not need a separate fleet-level handoff gate.

Some upgrades are exceptions:

- The upgrade from v0.1.0 to v0.2.0 must not be a rolling update. Stop
  every v0.1.0 node, then start the v0.2.0 nodes. Two changes require
  this: v0.2.0 nodes advertise the internal listener, so ownership
  records that v0.1.0 nodes wrote name an address that v0.1.0 peers can
  not follow to a v0.2.0 node; and v0.2.0 compacts replicated data into
  block objects that a v0.1.0 reader can not restore. A fleet must not
  mix the two versions.
- The upgrade from v0.2.1 to v0.3.0 can use a rolling update. v0.3.0
  changes the default durability from `bucket` to `fleet`: a single node
  keeps the v0.2.1 behavior, and a fleet of two or more nodes activates
  fleet replication automatically. Stage the v0.3.0 binary on every node,
  and restart one node at a time. A mixed fleet stays safe, but a v0.3.0
  node cannot replicate to a v0.2.x peer, so it acknowledges writes
  through the bucket and retries until the peer runs v0.3.0. Do not start
  a v0.2.x binary after that node runs v0.3.0 unless the shutdown log
  contains `node-log close: sealed epoch`. A graceful stop attempts this
  seal, but a stop under load can leave the record open, and a v0.2.x
  binary cannot read writes that wait in the replicated log or bundle
  objects — this downgrade can lose acknowledged writes.
- The upgrade from v0.3.0 to v0.4.0 must not use a rolling update. Stop
  every v0.3.0 node, and then start the v0.4.0 nodes. v0.4.0 moves every
  proxied cell call — the fetch, the RPC, and the WebSocket — onto one
  tunneled connection that carries plain HTTP, and the peer protocol
  refuses a different version, so the two versions cannot proxy calls to
  each other. v0.4.0 also stores each new large KV value under its ownership
  epoch and writes an epoch-qualified row reference. A v0.3.0 node cannot
  read that reference, so a mixed fleet can make a committed KV value
  unavailable. The tunnel establishment carries the version, so a later
  protocol change can negotiate instead of refuse.
- The upgrade from v0.4.0 to v0.4.1 can use a rolling update. v0.4.1
  restores a large cell by paging, and a paged epoch continues the chain
  of the epoch before it instead of opening with a whole-database snapshot.
  A v0.4.0 node cannot restore such an epoch. Each v0.4.1 node therefore
  publishes the bucket format it reads in its lease, and it pages a
  takeover only while every live lease reads that format. A v0.4.1 node in
  a mixed fleet clones a large cell the way v0.4.0 does, and paging starts
  on its own one lease lifetime after the last v0.4.0 node stops. Do not
  start a v0.4.0 binary after that point: a cell that paged in has no
  epoch a v0.4.0 node can restore, and it stays unavailable on that node
  until a v0.4.1 node takes it over.

The internal listener also provides an alpha operator API. `/state`
reports the node state, and its `node_load` object is the same load sample
that the node lease publishes. `POST /reload` adopts the deployment pointer now,
and `POST /shutdown` starts the same graceful
handoff. `POST /rebalance/pause` stops balancing. The node publishes the
pause in its lease, and one paused lease stops every move in the fleet, so
a pause on one node pauses the fleet. `POST /rebalance/resume` on the same
node resumes it. The `POST /shutdown?handoff=preserve` request prepares a clean
same-node reload and keeps the ownership records. A release can change
this API, so keep the operator tooling and the celld release together.

## Diagnose a fleet

`celld diagnose` reads the node leases in the bucket and sends a probe to
each live peer. It does not take a lease, and it does not change
ownership.

```sh
celld diagnose \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

To probe only some nodes, use `--peer NODE_ID` one or more times. The
report identifies expired records, unsafe or incorrect advertised
addresses, peers that it cannot reach, authentication failures, and
protocol versions that do not agree.

Each node line also shows `restoring`: the count of cold routes that hold
an activation permit or wait for one (a capacity waiter already holds a
permit, so each cold route counts once). During a rolling update, wait
for every node to report `restoring=0` before you restart the next node,
so one restart's cold work finishes before the next restart removes more
warm capacity. A node that restarts recovers its previous session's log
before it takes a lease. If a peer is already recovering that log, the
node waits behind the peer while the peer's heartbeat is fresh, and it
takes the recovery over only when the heartbeat stops.

## List Durable Objects

List the Durable Object instances in the bucket:

```sh
celld cell list \
  --bucket "$CELLD_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

Each line is a `Class:ID` cell scope. The ID is the Durable Object ID: a
64-character hash unless the application supplies its own name. Give a
class name to list only that class, and pass `--json` for one JSON object
per line. The command puts the class in the storage prefix, so `--limit`
applies only to that class. An instance appears after the first event reaches
it, because its owner then writes an ownership record to the bucket. An ID
that an application only derives does not appear.

The listing also shows celld's own cells. A D1 database, a KV namespace,
and a Workflow are each a cell in a reserved class, and their names start
with `__`. They hold real data in the bucket, therefore the command shows
them. The `--json` output marks each row with `"reserved": true` or
`false`, so a script can select one kind:

```sh
celld cell list --all --json --bucket "$CELLD_BUCKET" |
  jq -r 'select(.reserved | not) | .scope'
```

A storage request returns at most 1000 instances, so the command stops at
1000 instances and writes this line to stderr:

```
1000 cells shown; more exist. Continue with --after Room:d99d9174b25e46310694dd931b47fbde70a7460bb7b210b546060651ea2ff6e0
```

Pass that `--after SCOPE` to read the next 1000 instances, or use
`--limit N` for a different bound:

```sh
celld cell list --bucket "$CELLD_BUCKET" \
  --after Room:d99d9174b25e46310694dd931b47fbde70a7460bb7b210b546060651ea2ff6e0
```

Pass `--all` to read the whole listing in one command. This makes one
request for each 1000 instances, so it can take minutes on a large fleet;
the command reports its progress and its request count on stderr.

The order is the storage order, and `--after` continues from the last
instance printed, so a sequence of `--after` commands lists each instance
one time. An instance that an application creates during the sequence can
appear or not appear, because the listing is not a snapshot.

The command writes the instances to stdout and every message to stderr,
so a script reads only instance data:

```sh
celld cell list --all --json --bucket "$CELLD_BUCKET" > cells.ndjson
```

## Hot-cell overload

celld admits a maximum of 64 concurrent fetch events for one Durable Object.
Set `CELLD_MAX_CELL_REQUESTS` to use a different positive limit.

A Queue broker admits a maximum of 256 concurrent producer calls. It commits
at most 64 calls in one transaction, and four transactions can overlap their
durability proofs. These fixed bounds limit the copied message bodies while a
storage proof is slow.

celld returns HTTP status `503` when the target reaches this limit, and it
does not start the excess event. The response contains `Retry-After: 1` and
`X-Celld-Overload: cell`, so the application can retry or reject the work.
A local or remote Queue owner uses the same status and headers when its
producer reservations are full. The Queue response body is
`{"error":"cell admission refused"}`. A fixed-rate client must count the
response as rejected work, so an immediate retry does not increase the
configured offered rate.

A caught Queue producer error contains `cell overload: admission refused`, so
the Worker can return the same admission response to the client.

The runtime writes a `cell_overload_refused` log event when a target becomes
saturated. The event contains the cell scope, the node, the region, the
in-flight count, and the limit. Count these overload responses separately
from the transport errors and the application failures.

## Environment variables

For the full list, run `celld -h`. This table shows the primary settings:

| variable | purpose |
| --- | --- |
| `CELLD_BUCKET` | The fleet bucket, and an optional key prefix. The same as `--bucket` |
| `S3_ENDPOINT` | The S3-compatible endpoint. The same as `--endpoint` |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | The storage region |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Explicit AWS credentials. The standard AWS credential chain is also available |
| `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_SERVICE_ACCOUNT_KEY` | Google credentials for a `gs://` bucket. Application Default Credentials are also available |
| `AZURE_STORAGE_ACCOUNT_NAME` | The storage account for an `az://` bucket. The bucket NAME is the container |
| `AZURE_STORAGE_ACCOUNT_KEY` | The storage account key. Do not combine it with an identity selector |
| `AZURE_AUTHORITY_HOST`, `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE` | The standard AKS workload identity environment for an `az://` bucket. The authority must be the public Azure host |
| `AZURE_STORAGE_USE_EMULATOR` | Set to `true` to develop against Azurite. celld does not qualify Azurite for a production fleet |
| `CELLD_ADDR` | The public Worker listener. The same as `--listen` |
| `CELLD_INTERNAL_ADDR` | The peer and operator listener. The same as `--internal-listen` |
| `CELLD_ADVERTISE` | The internal address that peers can reach. The same as `--advertise` |
| `CELLD_UNSAFE_PUBLIC_ADVERTISE` | Set to `1` to permit a literal public IP in `CELLD_ADVERTISE`. This setting does not resolve a DNS name or restrict the internal listener |
| `CELLD_NODE` | An explicit node-session ID. Use 1 to 128 ASCII letters, numbers, dots, dashes, or underscores. The value cannot be `.` or `..` |
| `CELLD_WATCH` | The local work directory for SQLite and replication |
| `CELLD_ESBUILD` | The path of the esbuild executable |
| `CELLD_ACTIVATIONS` | The limit for concurrent cold-cell activations (default: 8 for each available CPU, at least 16 and at most 128). A cold activation waits on the object store for most of its time, so the default is above the CPU count |
| `CELLD_DEPLOY_POLL_S` | The interval in seconds at which a node reads the deployment pointer and adopts a new deployment in place (default: 30) |
| `CELLD_DEPLOY_MAX_AGE_S` | How long a resident Durable Object can keep the previous deployment's code after an adoption before celld forces the move (default: 60; 0 forces at once) |
| `CELLD_OPERATION_DEADLINE_MS` | The deadline for a non-restore operation (default: 15000) |
| `CELLD_WORKER_LOADER` | Bind a Worker Loader (Code Mode) at this `env` name. A Worker can then start isolates at runtime. Off unless set (experimental) |
| `CELLD_MAX_LOADED_WORKERS` | The limit for concurrent loaded workers (default: 256) |
| `CELLD_MAX_CELL_REQUESTS` | The concurrent fetch limit for one Durable Object or Queue broker (default: 64) |
| `CELLD_MAX_REQUEST_BODY_BYTES` | The body limit for a public Worker request or a direct Durable Object request (default: 1 GiB) |
| `CELLD_MAX_RESIDENT_CELLS` | The hard limit for resident cells, enforced at admission |
| `CELLD_IDLE_EVICT_S` | The age in seconds after which an idle resident cell leaves memory and hibernates (unset: only pressure or the residency cap removes an idle cell) |
| `CELLD_PLACEMENT_WEIGHT` | The ownership share of this node, relative to the other nodes' weights (default: the CPU count) |
| `CELLD_REBALANCE_INTERVAL_MS` | The interval between shared fleet sample reads and the maximum sample age, in milliseconds (default: 5000; 0 disables balancing) |
| `CELLD_REBALANCE_BATCH_CELLS` | The maximum idle cells one balancing batch hands over (default: 32; the release ceiling also applies) |
| `CELLD_MAX_RSS_MB` | The memory threshold for pressure shedding, applied to the greater of the allocator-adjusted RSS and the allocator-adjusted active cgroup working set (default: 80% of the available memory; 0 disables the threshold and the absolute cap) |
| `CELLD_OUTPUT_GATE` | The default is `1`, so celld proves each write durable before it acknowledges the write. Set `0` to remove the replication wait and accept possible loss of an acknowledged write |
| `CELLD_DURABILITY` | How celld proves a write durable before it answers. The default is `fleet`: the node serving the cell sends the write to one or two other nodes and answers once they hold it on disk, or once the bucket upload finishes, whichever comes first. This needs two or more nodes; a single node has nobody to send to, so every write waits for the bucket. Set `bucket` to always wait for the bucket |
| `CELLD_LOG_CAPTURE_WORKERS` | The limit for concurrent log-capture workers (default: 8) |
| `CELLD_LOG_PIPELINE` | The limit for fleet log rounds that can be in flight (default: 4) |
| `CELLD_LOG_GROUP_COMMIT_MS` | The wait before celld captures pending Queue writes for a fleet log round (default: 1 ms). The wait does not apply to another cell class. Set `0` to disable the wait |
| `CELLD_QUEUE_PRODUCER_GROUP_MS` | The wait before a Queue owner commits concurrent producer calls in one SQLite transaction (default: 4 ms). Set `0` to disable the wait |
| `CELLD_LOG_HEDGE_MS` | The wait before a leader sends a second copy of a slow log append to a follower. The default is adaptive: celld derives the wait from the slowest recent append in the ensemble (4 times that append, at least 250 ms, and always below the eviction backstop), so a loaded fleet does not send copies for honest slow appends. Set a value to use a fixed wait in milliseconds, and set `0` to disable the second copy. An append is idempotent per sequence, so the copy is safe, and the leader uses the answer that arrives first and confirms |
| `CELLD_LTX_TRUNCATE_PAGES` | The WAL size, in pages, at which celld truncates an ordinary cell's WAL file at the next checkpoint (default: 128, a 512 KiB cap). A passive checkpoint does not shrink the WAL file, so each capture reads the stale region after a restart. The truncate keeps the read small. A truncate ends in a full image of the database, so a database larger than 4 MiB also waits until its WAL is larger than the database. Queue cells use passive checkpoints only. Set `0` to disable the truncate for all cells |
| `CELLD_LTX_COMPACTION` | The default is `1`: celld creates additive L1 objects, and a takeover reads tens of objects instead of thousands. Set `0` on every node of a mixed fleet until all nodes can read v0.5.2 block objects, because an old reader cannot take over a cell after its first L1 publication |
| `CELLD_LTX_COMPACTION_MIN_TXIDS` | The durable TXID distance that queues a background L1 attempt (default: 256) |
| `CELLD_LTX_COMPACTION_MIN_MB` | The L0 bytes since the last fold that queue a background L1 attempt on their own, in MiB (default: 32, at most 64). A cell with large rows reaches it in a few transactions, so its tail folds before it outgrows the fold's input cap and a takeover reads a few objects instead of hundreds |
| `CELLD_LTX_PAGED` | The default is `1`: celld restores a taken-over cell by paging when its chain is at least `CELLD_LTX_PAGED_MIN_MB`. It opens the database through a fault-in VFS that reads each page from the bucket on first use, instead of downloading the whole restore chain first, and a smaller chain is cloned. Set `0` to clone every chain. A paged cell's local file is a cache, so celld does not preserve it as an eviction snapshot and does not publish a handoff snapshot from it. A paged epoch continues the chain it paged in. celld uploads a small marker object at the next transaction before the cell serves, and its later objects follow it, so a restore composes the epochs. Set `0` on every node of a mixed fleet until all nodes run v0.4.1 or later, because a paged epoch holds no whole-database snapshot and a node before v0.4.1 restores one epoch only. Such a node refuses the activation of that cell, and the refusal is permanent for the cell. A full activation opens its epoch with a whole-database snapshot. A node pages only while every live lease in the fleet publishes a bucket format that reads a paged epoch, so a rolling update from a release without paged restore clones until that release has left the fleet; the `paged_gate` log event reports each change. A clean reload does not keep a paged cell resident; the next process pages it in again. An inspect of a paged cell must read the bucket, because celld does not copy the cell's sparse file in place. After the activation, celld fills the rest of the file in the background at `CELLD_LTX_HYDRATE_MBPS`, so a cell that stays resident soon reads only its local file |
| `CELLD_LTX_PAGED_MIN_MB` | The chain size, in MiB, from which a restore pages instead of cloning (default: 256). A clone of a smaller chain takes seconds and fits the node's memory, so it needs no fault path and no hydration. Set `0` to page every chain |
| `CELLD_LTX_HYDRATE_MBPS` | The rate, in MiB per second, at which a paged cell's file fills in the background (default: 16). One cell fills at a time per node. Set `0` to keep a paged cell sparse, so every cold page is read from the bucket on first use |
| `CELLD_LTX_COMPACTIONS` | The node-wide limit for concurrent background L1 attempts (default: 2). `CELLD_RELEASES` bounds final handoff snapshots |
| `CELLD_LTX_DURABILITY_TIMEOUT_SECS` | The budget for one durability proof, in seconds (default: 10). The budget runs from the moment the node starts to upload the write. A write that waits behind the uploads of other cells waits while the node proves other writes, for at most six budgets. The same budget bounds the final snapshot or L0 fallback of a handoff. A slow object store can need a longer budget for a large write |
| `CELLD_VAR_*`, `CELLD_VARS_FILE` | Worker variable overrides |
| `RUST_LOG` | The runtime log filter |

The help output also shows the advanced tuning switches and their
defaults.

An unset variable selects its documented default. A Boolean variable
accepts only `0` or `1`. celld exits during startup when a supplied value
is invalid.
