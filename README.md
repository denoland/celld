# celld

Self-hosted, distributed **Durable Objects**.

celld is an open-source daemon that runs a Cloudflare Workers application
on your own machines: Workers, Durable Objects, KV, Queues, D1, R2,
Workflows, Cron Triggers, and static assets, deployed from the
`wrangler.json` that you already have. Each object is a cell: a named
server with its own SQLite database. celld stores the long-term state in a bucket
that you own — S3-compatible, Google Cloud Storage, or Azure Blob
Storage — and needs no serving control plane and no consensus service.
A cell that nothing is serving costs almost nothing. Learn more at
[celld.dev](https://celld.dev) or read the
[documentation](https://celld.dev/docs).

## How it works

A node is one `celld` process, and you run one on each machine. Every
node embeds V8 and executes Wrangler bundles. The nodes that share one
bucket are a fleet, and that bucket holds the deployments, the cell
state, and small ownership records. A conditional bucket write gives a node the
ownership of a cell, so exactly one node owns a cell at a time — no
membership protocol, no failure detector, no consensus service. Signed
peer HTTP provides routing and replicated-log transport.

celld captures each committed SQLite write as LTX data, the transaction
format the replication uses. A single node proves the write durable by
uploading that data to the bucket. A fleet of two or more nodes proves it
sooner: the owner sends the data to one or two other nodes, and the write
is durable as soon as they hold it on their disks. celld uploads the data
to the bucket afterwards. A single node has no other node to send to, so
each write waits for the bucket, and those writes are much slower.
Before a takeover restores a cell, celld recovers an open log from the
prior owner, so the bucket holds the long-term state and nodes stay
replaceable. See [what celld guarantees](docs/guarantees.md) for the
complete protocol.

## What runs on celld

celld runs the programmatic Workers platform: the runtime, and each binding
that Cloudflare builds on Workers and Durable Objects. A KV namespace, a
queue, a D1 database, a Workflow, and the R2 index are each a cell, so
they get the same lease, the same replication, and the same failover as a
Durable Object. Each row links to a project that deploys as-is:

| service | example |
| --- | --- |
| Workers: fetch handlers, service bindings, JS RPC, Node.js compat | [`hello`](examples/hello) |
| Durable Objects: SQLite storage, alarms, hibernating WebSockets | [`counter`](examples/counter) |
| KV: list, metadata, expiration, bulk import | [`kv`](examples/kv) |
| Queues: producers, batching consumers, retries, dead letters | [docs](docs/cloudflare-compat.md#queues) |
| D1: SQL databases, batches, migrations | [`d1`](examples/d1) |
| R2: reads, writes, lists, multipart uploads | [`r2`](examples/r2) |
| Workflows: durable steps, sleeps, events, pause and restart | [`workflow`](examples/workflow) |
| Cron Triggers: one run for each occurrence across the fleet | [`cron`](examples/cron) |
| Static assets: asset-only or with a Worker, `_headers`, `_redirects` | [docs](docs/cloudflare-compat.md#static-assets) |

A product that needs the Cloudflare network, a GPU, or a browser farm is out
of scope. The [Cloudflare compatibility](docs/cloudflare-compat.md) page
lists each gap in the services above.

## Install

The installer downloads the `celld` binary (provenance is verifiable with
`gh attestation verify`):

```sh
curl -fsSL https://celld.dev/install.sh | sh
```

Put `~/.local/bin` on your `PATH` if the installer asks you to.

Worker projects deployed with `celld deploy` need
[esbuild](https://esbuild.github.io) on `PATH`; asset-only projects do not.

The installer keeps each release under `~/.local/lib/celld/releases` and points
one symlink at the current one. To remove celld, delete the symlink and the
releases:

```sh
rm `which celld` && rm -rf ~/.local/lib/celld
```

## Container

The release image contains the `celld` binary and is published for Linux
x86-64 and ARM64:

```sh
docker run --rm ghcr.io/denoland/celld --version
```

Persist the runtime's local state and pass the standard AWS credential
environment through:

```sh
docker volume create celld-state
docker run --rm --network host \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  -e CELLD_WATCH=/var/lib/celld/state \
  -v celld-state:/var/lib/celld \
  ghcr.io/denoland/celld \
  --bucket s3://my-cells-bucket \
  --endpoint https://ACCOUNT.r2.cloudflarestorage.com \
  --region auto \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise node-a.internal:8081
```

Drop `--endpoint` and `--region` for AWS S3. Expose port 8080 through the load
balancer, and keep port 8081 on the private network.

## Run it

Run an application locally without a cloud bucket:

```sh
celld dev
```

The command starts one celld node and uses a local object store. It does not
require Docker or a cloud bucket. The Worker listener uses
`http://127.0.0.1:9876`. Use `celld dev --port PORT` to select a different
Worker port. Use `celld dev --host IP` to select a different interface. A
non-loopback IP exposes the Worker listener to the network, and the internal
operator listener stays on loopback. The command keeps the application state
in `.celld/dev`, so a later invocation uses the same durable data.

The state also survives a configuration change, and celld does not migrate it,
so an object can keep a value that the new configuration rejects. The failure
then looks unrelated to the change. Use `celld dev --clean` to delete
`.celld/dev` before the server starts and run from an empty local state.

The default display highlights the application URL and hides the node
warning and information logs. Use `celld dev --logs` to show these logs.
The errors remain visible without this flag. Set `NO_COLOR` to disable color,
or set `FORCE_COLOR` to enable color when the output is not a terminal.
`NO_COLOR` always takes priority.

The command watches the project and rebuilds the application after a
source or configuration change. It keeps the current application running
if a build fails, and a successful restart retains the durable state. See
the [documentation](docs/README.md#develop-an-application-locally) for
the complete local-development contract.

celld uses the standard AWS credential chain. On Amazon EKS, celld reads the
Pod Identity credentials from the injected environment variables and the
authorization-token file. Deploy to an S3-compatible bucket, then start celld
against the same bucket:

```sh
celld deploy . \
  --bucket s3://my-cells-bucket

celld \
  --bucket s3://my-cells-bucket \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise 10.0.0.12:8081
```

One node proves each write through the bucket, so a write costs one storage
round trip. Start a second node against the same bucket, and celld sends each
write to it: the write then finishes as soon as the second node holds the data
on its disk, which is much faster than the round trip.

Run two or more nodes if write latency matters to you. The second node needs
no extra configuration, because a node finds the other nodes through the
bucket.

How much faster depends on how far your bucket is and how loaded the fleet
is. A write to a region-local store takes about 90 ms. One loaded lab fleet
measured about 600 ms against a store that was not region-local, and about
25 ms once a second node held the write. See
[what celld guarantees](docs/guarantees.md#the-ensemble-needs-two-nodes).

Use `--endpoint` for another S3-compatible service and `--region` when it
cannot be inferred. A `gs://` bucket selects Google Cloud Storage: celld then
uses the Cloud Storage XML API with generation preconditions and
authenticates with Application Default Credentials. celld rejects an S3
`--endpoint` for a `gs://` bucket, and it ignores the storage region:

```sh
celld deploy . --bucket gs://my-cells-bucket
celld --bucket gs://my-cells-bucket --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 --advertise 10.0.0.12:8081
```

An `az://` bucket selects Azure Blob Storage, where the NAME is the
container and `AZURE_STORAGE_ACCOUNT_NAME` names the storage account.
celld requires exactly one credential family: a storage account key, a
managed identity, or a workload identity. An AKS workload identity uses
`AZURE_AUTHORITY_HOST`, `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and
`AZURE_FEDERATED_TOKEN_FILE`, and the authority host must identify the
public Azure cloud. A Microsoft Entra identity needs data-plane permission
to read, write, list, and delete blobs; the `Storage Blob Data Contributor`
role supplies these. celld rejects an S3 `--endpoint` for an `az://` bucket,
and it ignores the storage region. See
[what celld guarantees](docs/guarantees.md) for the qualification:

```sh
export AZURE_STORAGE_ACCOUNT_NAME=myaccount
celld deploy . --bucket az://my-cells-container
celld --bucket az://my-cells-container --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 --advertise 10.0.0.12:8081
```

A fleet runs one application, and every node loads its latest successfully
committed deployment from `deploy/current.json`. `celld deploy` invokes
`esbuild` from `PATH` for Worker code, accepts the supported Wrangler config
subset — including co-deployed or asset-only static assets — and writes the
deployment objects directly, using the documented types in
`crates/celld/protocol.rs`. Every node discovers owners and peers from
bucket leases; there is no account or join service. Run `celld --help` for
the complete command line.

Peer HTTP and the operator API use the internal listener. Put every
advertised address on a trusted private network or an encrypted overlay such
as WireGuard or Tailscale, and do not publish the internal port. celld
rejects a literal public IP unless you supply `--unsafe-public-advertise`.
An explicit advertised address requires an explicit internal-listener
address, and you must route the advertised address to the internal
listener — celld cannot verify a hostname or a translated port. The first
current node creates `fleet/peer-auth.json` in the bucket. Cell fetch and RPC
requests carry a protocol version and depend on the trusted private network.
Peer-control and reserved-cell operator requests use the fleet HMAC. This HMAC
binds each body and supplies a clock limit and replay protection. Treat access
to the bucket and its credentials as fleet administrator access.

## Operate a fleet

`celld diagnose` enumerates every node lease by default, then performs a signed
direct probe of each live peer:

```sh
celld diagnose --bucket s3://my-cells-bucket
```

The report keeps checking after an individual failure and distinguishes
expired records, malformed or unsafe advertise addresses, unreachable peers,
and incompatible protocols. It also prints each node's coarse owned-cell,
resident-cell, WebSocket, RSS, CPU, file-descriptor, pressure, and shedding
sample. Pass one or more `--peer NODE_ID` options to restrict the check.

`celld cell list` lists the Durable Object instances in the fleet bucket:

```sh
celld cell list --bucket s3://my-cells-bucket
```

The command prints one `Class:ID` cell scope per line. Give a class name to
list only the instances of that class, and pass `--json` for one JSON object
per line. An instance appears after the first event reaches it, because its
owner then writes an ownership record to the bucket. An ID that an application
only derives does not appear.

The listing is bounded. One storage request returns at most 1000 instances,
so the command prints at most 1000 and reports on stderr that more exist.
Pass `--after SCOPE` to continue from the last instance printed, or `--all`
to read the whole listing.

`celld d1` runs SQL and migrations against a deployed D1 database. It finds a
node through the same node leases, and that node sends the work to the node
that owns the database:

```sh
celld d1 migrations apply ledger --bucket s3://my-cells-bucket
```

The migration file extension is ASCII case-insensitive, so `.sql` and `.SQL`
files are migrations. The command ignores a file with a different extension.

`celld kv` reads and writes a deployed KV namespace. Its bulk commands use the
Wrangler file format, so a Wrangler export can migrate directly into celld:

```sh
celld kv bulk put sessions wrangler-export.json \
  --bucket s3://my-cells-bucket
```

`celld queue` inspects and controls a deployed Queue. A queue can continue to
accept messages while delivery is paused:

```sh
celld queue info jobs --bucket s3://my-cells-bucket
celld queue pause jobs --bucket s3://my-cells-bucket
celld queue resume jobs --bucket s3://my-cells-bucket
```

Set a hard resident-cell limit on each loaded node:

```sh
CELLD_MAX_RESIDENT_CELLS=1000 \
celld --bucket s3://my-cells-bucket --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 --advertise node-a.internal:8081
```

celld balances ownership across the fleet. Every node reads a shared fleet
sample every five seconds. One node refreshes the sample from the node
leases, so each refresh reads each lease once for the fleet. The node with
the most owned cells per unit of weight hands at most 32 hibernated cells
per sample to the peer that is furthest below its share. A hibernated cell moves as one record write, and
its parked hibernatable WebSockets close with code 1012 so the clients
reconnect to the new owner. A resident cell moves only after idle eviction
hibernates it (`CELLD_IDLE_EVICT_S`). Set `CELLD_PLACEMENT_WEIGHT` to give a
node a larger or a smaller share than its CPU count, and set
`CELLD_REBALANCE_INTERVAL_MS=0` to disable balancing. `POST /rebalance/pause`
on the internal listener of any node pauses the fleet.

celld enables a memory-pressure threshold at 80% of the available memory by
default. Set `CELLD_MAX_RSS_MB` to change the threshold, or set it to `0` to
disable memory-pressure shedding. In a Linux cgroup, the threshold uses the
greater of the allocator-adjusted RSS and the active cgroup working set. celld
calculates the working set as `memory.current` less `inactive_file` from
`memory.stat`, then it removes the measured allocator slack. This calculation
includes active kernel charges that process RSS does not report, and it excludes
file pages and allocator pages that celld cannot return by shedding a cell. The
`/state` route reports all four input measurements.

A separate absolute cap applies to the complete cgroup charge at 95% of the
available memory. celld uses the process RSS when it cannot read a cgroup
charge. The cap protects the node when shedding cannot return a kernel charge.
The node logs a warning when the cap applies. The cap is a share of the
available memory, not a share of the threshold. Therefore, a
`CELLD_MAX_RSS_MB` value at or above 95% makes the cap the effective limit, and
celld reports the decision at startup.
`CELLD_MAX_RSS_MB=0` disables the threshold and the cap together. When celld
cannot read the size of the available memory, it applies a cap of 125% of an
explicit threshold.

Under pressure, celld durably replicates and fences the least-recently used
idle cells, publishes them as unowned without resetting their epochs, and
refuses to reacquire new unowned cells. It does not shed a cell with active
work or a live host WebSocket. A spare node receives no assignment; it
acquires a released cell through the same bucket protocol when normal
traffic reaches it. Each limit releases separately. The threshold releases at
80% of its value, and the cap releases at 80% of its value. Therefore, a
crossing of one limit does not hold the node against the other.

Each isolate also has a V8 heap limit, separate from the memory of the node.
The default is 128 MB, and it matches the limit of a Durable Object on
Cloudflare; set `CELLD_V8_HEAP_LIMIT_MB` to change it. Each hibernatable
WebSocket client holds state in the heap, so the limit decides how many
clients a cell can carry: approximately 50,000 at the default, and
approximately 512 MB for 100,000.

An isolate above 90% of this limit refuses a new hibernatable WebSocket, and
it accepts again when the use of the heap falls under 90%. An isolate that
reaches the limit also stops the materialization of a SQL result set; both
errors name the heap. celld measures the heap before each event, and the
isolate serves again when the use falls under 75% of the limit. An idle
isolate holds a dead heap until something allocates, so celld forces a
collection when a measurement is above that share. A restart of the process
is not necessary.

## Contributions

Pull requests are disabled. Coding agents make it too easy to send a large,
low-context change that costs maintainers more time than it saves. Thoughtful
contributions are welcome; please understand the code, keep the patch focused,
and respect the review time you are asking for.

Send a `git format-patch` attachment to [ry@deno.com](mailto:ry@deno.com).

Contributor License Agreement: By emailing a patch, you certify that you have
the right to submit it and assign to Deno Land Inc. all rights in the patch
that you can assign. Where a right cannot be assigned, you grant Deno Land
Inc. a perpetual, irrevocable, worldwide, royalty-free, transferable,
sublicensable license to use, modify, combine, relicense, redistribute, or
publish the patch, in whole or in part, with or without attribution.

## License

[Apache-2.0](LICENSE)

See the [limitations](docs/limitations.md) and
[security](docs/security.md) pages before operating a public fleet.
