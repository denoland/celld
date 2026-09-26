# What celld guarantees

celld makes two promises about your data. Exactly one node serves a cell
at a time, so two machines never write the same database. And celld does
not answer a write until that write survives a failure, so nothing you
were told succeeded is lost.

This page explains how celld keeps both promises. Fencing prevents a
slow, paused, or disconnected node's writes from damaging the current
owner's data after it loses a cell.
Each ownership epoch has a separate storage prefix, so a stale node's
writes cannot overwrite the current owner's data.
Both guarantees require a bucket with working conditional writes and
ranged reads, and a supervisor that restarts the process.

## What the bucket must provide

celld needs four properties from the object store:

- A conditional create: the write must fail when the object exists.
- A conditional overwrite: the write must fail when the object changed
  after the read.
- Read-after-write consistency: a read after a successful write must
  return that write.
- Ranged reads: a read must return the requested byte range and the bytes
  from that range.

The qualified stores are Amazon S3, Cloudflare R2, Tigris, Google Cloud
Storage, and Azure Blob Storage. celld's release tests run against R2,
and the S3 path uses the same client and the same headers.

Backblaze B2, Hetzner Object Storage, and DigitalOcean Spaces do not
implement the required conditional writes. celld is not correct on such
a store: two nodes can then own one cell. A store can also accept the
conditional headers and ignore the condition, and that store fails late
and silently, so run the storage test below.

MinIO (the community edition) implements the conditional writes and
passes the storage test, but celld has not qualified it for production.
One release is broken: RELEASE.2025-09-06T17-38-46Z answers the
conditional create of an absent object with `NoSuchKey`, so the first
deploy fails (denoland/celld#162). Use RELEASE.2025-09-07T16-13-09Z or
later.

The request dialect differs per provider. An S3-compatible bucket gets
the `If-None-Match: *` and `If-Match` headers, and the condition
compares the etag. A `gs://` bucket selects the Cloud Storage XML API
with the `x-goog-if-generation-match` precondition and OAuth
credentials, because Cloud Storage does not apply `If-Match` to a PUT.
An `az://` bucket (the NAME is the container) uses the same `If-`
headers, which Put Blob applies. The adapter treats `AlreadyExists` and
`Precondition` (HTTP 412 on Azure) as clean conditional-write
rejections and keeps every other error ambiguous, because an ambiguous
write can have changed the object.

Azure was qualified on 2026-08-18 under an account key, a VM managed
identity, and an AKS workload identity, single-node. A managed identity
on Azure App Service or Azure Container Apps does not work; see
[limitations](limitations.md).

## The storage test

No store publishes these properties, so celld asks the store directly.
The command `celld diagnose` sends four conditional writes to your
bucket and reports the result:

```
ok bucket conditional write (create, reject-create, update, reject-stale)
```

Two of the four writes must fail. A store that accepts either one
cannot fence a cell, so celld names the store as the fault and the
command exits with an error. Each node also runs these writes before it
serves. The node then requests part of a second object and verifies the
returned range and bytes.

The startup test uses new objects for each attempt. It makes at most
three attempts when an operation fails without a clear cause. The node
starts with a warning after all three attempts fail because a temporary
outage can end after startup. The node stops immediately when a required
conditional write or ranged read is unsupported. It also stops when the
store ignores a condition or returns a wrong range or wrong bytes. A node
cannot disable this startup test. Use `celld diagnose --read-only` with a
credential that cannot write.

The diagnose test writes and deletes one small object under `probe/`.
The startup test uses a second object for the ranged read. A process
that stops mid-test can leave an object behind; it is small, and celld
never reads it.

celld reserves `probe/`, `cells/`, `nodes/`, `node-cells/`, `fleet/`,
`deploy/`, `deploy-blobs/`, `log/`, `wake/`, and `telemetry/`. celld deletes
objects under some of these prefixes, so an application must not write under
any of them.

celld requires ranged reads for stored cell data. The startup test stops
a node when the store ignores the `Range` header or returns incorrect
bytes.

## The supervisor

You must run celld under a supervisor that restarts the process, such
as systemd, Docker with a restart policy, or Kubernetes. A node fences
itself when it loses its lease (the mechanism is below), and a fenced
process exits; without a restart, the fleet loses that capacity until
an operator intervenes.

The supervisor must restart without an attempt limit, and it must wait
at least one lease lifetime between attempts. A node that cannot
acquire a lease at startup retries and does not exit, so a repeated
fence needs a node that acquires a lease and then loses it, and the
wait keeps that cycle slow enough to observe.

## The mechanism

The short version: the ownership records use conditional writes, so two
nodes cannot acquire one cell. The replicated data carries its fencing
epoch in the object key, so a node that lost ownership writes only into
a superseded prefix. And before celld acknowledges a write, it proves
the write durable and confirms that it still owns the cell.

### The ownership record

Each cell has one ownership record in the bucket. The record names the
owner node's session and carries a fencing epoch. A node acquires a
cell with a conditional create when no record exists, or a compare-and-swap
on an existing record. The bucket accepts only one competing write, so two
nodes cannot acquire the same cell.

Every activation advances the epoch, a takeover and a local wake alike.
Each owner therefore replicates under a fresh epoch, and an epoch never
has two writers.

### The epoch prefix

The replicator copies each cell's SQLite data to the bucket under
`cells/<cell>/ltx/e<epoch>/`, with plain unconditional PUTs. The epoch
in the key is the fence: a node that lost ownership can keep writing,
but its writes land in a superseded prefix, and a restore selects the
current lineage. (The tiering path can first combine segments from many
cells into a node bundle, and it drains each segment into its per-cell
prefix later.)

The compactor reads retained bundles when a cell's segments leave the
in-memory index. A failed compaction retries after 30 seconds, and repeated
failures increase the delay to a maximum of 300 seconds. The bundle collector
advances through retained objects and deletes a bundle only when the per-cell
prefixes cover every segment in that bundle.

The prefix protects the new owner's data from stale writes. The next
two sections protect the durability promise.

### The acknowledgement rule (RPO=0)

A gate holds each write response until a durability proof covers the
write. A read-only response waits in the same way when the object has
committed a write that no proof covers yet. An error answer waits under
the same rule, because the message of a handler that throws can carry a
value that the handler read. An R2 mutation waits for the source object's write
proof. Therefore, the mutation cannot change the application bucket before the
source write is durable. A raw TCP connection, write, TLS upgrade, or shutdown
waits for the same proof. A response body that streams gets the same rule for
each chunk, so a chunk that an object produces after the response head waits
for a proof of the writes it can reveal. A client therefore cannot act on a
value that a crash can still lose. After a
bucket proof, celld reads the ownership record once and acknowledges
only if the record still names this node at this epoch. A partitioned
node can commit locally and replicate into its superseded prefix, but
the ownership read then shows the new owner, so celld does not
acknowledge the write. The check reads the record instead of comparing a
clock, so a paused process or a skewed clock cannot pass it.

A fleet proof does not require this read. The owner sends each write to
one or two other nodes, which hold a copy of its recent writes; those
nodes are its followers, and the set of them is the ensemble. Every
follower must fsync the write, and a takeover seals the prior node-log
session before it restores, so the stale owner cannot complete another
fleet proof.

An unfinished SQL write cursor can return rows before SQLite commits the
write. Outside an explicit transaction, the application must consume those
rows before output or `storage.sync()`, so the gate can cover the commit.
celld rejects that output while the write cursor remains unfinished.

### The ensemble needs two nodes

A node picks its followers from the other nodes in the fleet, so it never
counts itself. One follower is enough, therefore a fleet needs two
running celld nodes before any node can complete a fleet proof.
`CELLD_DURABILITY=fleet` is the default, so a fleet of one node requests the
fleet posture and does not get it.

A node recruits up to two followers, so a fleet of three or more nodes holds
three copies of an acknowledged write. The ensemble keeps acknowledging while
one follower remains, therefore a fleet does not fall back to the bucket each
time it loses a follower.

A node without an ensemble stays correct. It acknowledges each write on a
bucket proof instead, so celld still does not acknowledge a write before a
durability proof covers it. The cost is latency: the write waits for the object
store, and an object store round trip is much slower than a follower fsync.

### The takeover recovery gate

The default fleet mode can acknowledge a write once the node-log
ensemble stores it; the bucket upload can complete later. Each process
session therefore creates a conditional node-log record before its
first fleet-durable acknowledgement.

A cold activation checks the prior owner's log records before it reads
the bucket. An absent record proves that the session never acknowledged
past the bucket, and a sealed record proves that recovery completed. An
open or recovering record makes the activation run recovery: it fences
the record with a compare-and-swap, seals the reachable followers,
uploads their retained segments and bundles into the per-cell prefixes,
and then marks the record sealed. The activation cannot restore until
this sequence completes.

A cell can stop while its node session stays open. If acknowledged writes
remain outside its per-cell prefix, its next activation gathers that tail.
The log epoch becomes active before its first fleet proof. An active epoch
requires at least one current follower to return its complete retained range.
A fleet acknowledgement requires every ensemble member to store the write,
so one complete follower tail can provide the recovery evidence. If no
follower proves completeness, the activation fails and keeps the recovery
requirement. An inactive epoch has issued no fleet proof, so its bucket
bundles can provide the recovery data. When a stopped cell has a recorded
recovery requirement, the node keeps the log epoch until a bucket proof
covers its acknowledged data. A staged upload can complete after the cell
stops, so its bucket proof permits an ensemble change without another
activation. The next activation still folds any tail outside the per-cell
prefix.

A follower restart retains evidence of a damaged batch or a gap before a
later batch. The follower cannot certify the affected range until valid data
covers the gap. A directory read error fails the request and permits a retry.
The error does not become permanent evidence of a gap. A follower's HTTP
error does not prove that its data is absent, even when its lease has expired.
When no follower can return a complete tail, persistent response errors
can block recovery and startup. This check cannot detect a deleted final
batch if no later batch or persisted end records its range.

A torn batch can contain an unacknowledged write. Recovery cannot distinguish
that case from damage after an acknowledgement. Therefore, the existing loss
record can report an uncertified range even when no acknowledged write is
missing.

The `/peer/log/tail` endpoint selects its response through the request's
`tail_format` field. A missing field or the value `1` selects the older
entries-only format, `CLT1`. The value `2` selects the ranged format, `CLT2`,
and another value fails the request. An older caller gets an HTTP error
when the follower cannot certify a known fragment, so an incomplete tail
cannot become a successful entries-only response.

New nodes request the ranged format and decode both response formats. An
entries-only response provides no range evidence, so recovery treats that
response as inconclusive. It proves neither completeness nor data loss.
A recovery that requires a follower witness waits until a current member
returns the necessary range evidence. Therefore, response compatibility does
not guarantee uninterrupted recovery or startup during a mixed-version update.
Old nodes retain their existing recovery checks.

A restarting node serves authenticated follower seal and tail requests before
it completes its own predecessor recovery. Therefore, nodes that restart
together can recover acknowledged writes from their surviving follower disks.
The node accepts application requests and new follower appends only after
startup completes.

A large dead node can hold this recovery open for minutes. The recovery
reads the retained bundles in windows of at most 512 MiB. It uploads the
rows of one window and releases them before it reads the next window, so
the memory of the recovery does not grow with the size of the session. A
cell that has rows in several windows receives one object for each window.
A recovery attempt that fails or times out does not fail the waiting
requests.
The cell waits, and it retries the activation with a backoff
(`CELLD_RECOVERY_RETRY_MS`, default 1000). The requests fail with a
resolve error only after the retry budget is spent
(`CELLD_RECOVERY_RETRIES`, default 240).

### Epoch-chain restore

A restore composes the epoch prefixes that contain LTX data into one
chain. It starts at the newest prefix and walks down. An epoch that
opened with a whole-database snapshot starts the chain at transaction
one. An epoch that paged in continues its predecessor: its prefix holds a
marker object at the successor of the cut it paged from, and then its own
deltas, so the chain links the predecessor up to that cut and the epoch
from the cut onward. A link must end exactly at its successor's cut, so an
epoch that does not continue the chain is not part of it. celld no longer
writes an epoch seal object, and a legacy `e<epoch>.seal.json` object does
not limit the chain.

A fenced node can append an unacknowledged tail to an older prefix. When
a successor opened with a whole-database snapshot, a later restore reads
the full chain and can expose that tail. This does not violate the
contract, because a failed or absent acknowledgement does not prove that
the write is absent, and a node-log recovery or a bundle drain can add an
acknowledged tail after an earlier restore. When a successor paged in, the
chain clips the older prefix at the successor's cut. This is safe because
the takeover recovery gate seals the prior node-log session before the
successor restores, so no write past the cut can be acknowledged.

A paged cell's restore reads no chain up front. The cell opens over a
sparse local file, and each page the cell touches is read from the
objects on first use. The node then fills the rest of the file in the
background, and a filled cell reads only its local file. A chain smaller
than `CELLD_LTX_PAGED_MIN_MB` is downloaded whole, as before.

### Self-fencing

Each node holds a lease in the bucket. The lease carries an expiry, and
the node renews it after one third of the lifetime (`CELLD_TTL_MS`,
default 10000 ms). A renewal that does not reach the bucket does not
fence the node, because the node retries while the published expiry has
not passed.

A node that cannot reach the bucket cannot renew and cannot replicate,
so it must not own cells. When its published expiry passes, it fences
itself: it stops each active cell, and it fails every request that it
has not completed. A node also fences at once when its lease record is
gone, or no longer matches the record that the node published, because
the node can no longer prove that it holds the authority.

The fence writes nothing to the bucket. Each peer already reads the
lease as dead or replaced, so a peer can acquire the cells through the
ownership records. And a request is safe even before the fence runs,
because celld compares the current time against the published expiry
each time it routes a request.

An ingress checks a cached remote route against its observed lease deadline
for each new request. At the deadline, the ingress reads ownership again,
even if the previous owner keeps its connections open without a response.
Recovery still needs the bucket and the required durable data.
A draining ingress can resolve and forward to a live remote owner. It
refuses new ownership of an unowned cell or a cell with an expired owner.

A request already sent to the previous owner can remain incomplete. The
ingress does not replay that request, because the handler can have committed
a write before it stopped. The caller can cancel the request.

A fenced node logs a line that starts with `SELF-FENCE:` and stops with
the exit code 3. celld reports other internal failures with the same
prefix and code, so the line names the cause. An expired lease uses the
`node_lease_watchdog_fence` event. A missing record uses the
`node_lease_record_missing_fence` event, and a record that no longer
matches the node uses the `node_lease_record_mismatch_fence` event. The
mismatch event does not name an author, because the node cannot prove
which writer wrote that record. The fenced state is terminal:
only a restart returns the node to the fleet, through the same
cold-activation path that a peer failure uses.

The fence line names the lease state, and it cannot name the store
request that produced that state. A removed record, a replaced record
and a failed read are three different operator problems, so celld can
report each lease request on its own. Set
`RUST_LOG=celld=info,store=debug` to turn this on. The node then logs a
`node_lease_read` event and a `node_lease_write` event for each request
against its own lease record. Each event carries an `outcome` field:
`found`, `missing`, `applied`, `rejected` or `error`. An `error`
outcome carries the store failure in an `error` field, and a `found`
outcome carries the `generation` field of the record that came back.
The target is off at every other filter, so a node in production pays
nothing for it.

The failure of a node is a normal input, not a recovery procedure; the
[testing page](testing.md) shows the kill tests that exercise this
path.

## Alarm discovery and the wake format

SQLite stores the alarm deadline, consumption, retry state, and installation
identity. An alarm hint only causes celld to read SQLite. The hint does not
authorize an alarm handler to run.

Each committed alarm installation has a separate object under `wake/entries/`.
The ownership epoch and a persistent SQLite sequence identify the installation.
An alarm response waits for its publication PUT and the existing output proof.
Updates within the same minute also require a new publication PUT.

Cleanup uses conditional writes to publish a retirement record under
`wake/retired/`. The record requires a durability proof, a current owner,
and a confirmed replacement publication when an alarm remains armed.
Cleanup deletes only older identities or a proven consumed identity.
An old DELETE therefore cannot delete a later installation.
This protocol requires no conditional DELETE operation from the bucket.
After the proof, celld attempts to delete known obsolete publications directly.
These deletions do not block an alarm response.

Each cleanup pass lists at most 128 objects and processes at most eight cells
concurrently. The next pass continues the listing, and the final page restarts
the scan. A failed DELETE or a late PUT can require another complete scan.
The default interval is 60 seconds. `CELLD_WAKER_TICK_MS` changes this interval
and the due-scan interval. A large inventory can require many intervals, and
an additional node can repeat the same reads and deletes.
The collector requests a SQLite refresh for a remaining installation only when
its minute is due. Future installations stay dormant until that minute.
The timer still controls delivery, so the refresh cannot run the handler early.

The `wake/format.json` object selects format 2. The `wake/waker.json` object
holds the advisory lease for the fleet's waker role. A new node or deployment
initializes an empty fleet or upgrades a stopped v0.4.1 fleet automatically.
An unsupported format prevents startup. Existing application objects outside
the reserved namespaces do not prevent initialization.
The previous `wake-format.json`, `wake-v2/`, and `wake-retired-v2/` names
also identify unsupported engine data and prevent fresh initialization.

### Start a fleet with this format

An empty fleet initializes this format automatically. An upgrade from v0.4.1
uses the same bucket and preserves the existing node data directories.
The upgrade requires a stopped fleet. Mixed versions cannot share a serving
fleet because v0.4.1 cannot read the new alarm discovery entries.

1. Stop application traffic and deployment writers. Stop every old node and
   its supervisor, then wait for every node lease to expire.
2. Back up the stopped fleet's bucket and node data. Keep the node names,
   peer addresses, and data directories for the restart. A follower disk can
   contain acknowledged writes that the bucket does not yet contain.
3. Prevent the old binaries from restarting or writing to the bucket.
   Revoke their credentials or remove their access through the deployment
   system. The format marker cannot stop an old binary from writing.
4. Start the new binary on every node with the same configuration, data, and
   addresses. Wait for the fleet to become healthy before resuming traffic.

Startup upgrades the format automatically. The nodes can start together and
complete the same migration. A live node lease prevents the migration, so
stop every old writer and wait for its lease to expire before the restart.
The operator must prevent old writers from returning after the upgrade.

The migration inventories stored cells and creates a discovery seed for each
cell. It preserves the databases, node logs, ownership records, deployments,
and application objects. Existing legacy wake entries remain unused.
The inventory uses pages of at most 128 entries and includes cells whose
old alarm discovery entry is missing.

A discovery seed makes recovery read the cell's SQLite alarm state. Recovery
creates the installation identity without changing the deadline or retry
state. A seed does not authorize an alarm handler to run. The normal
retirement proof removes the seed after durable recovery and replacement
discovery, so an interrupted migration cannot silently discard an alarm.
Recovery can load cells with future alarms while it processes these seeds.

If a node stops during the migration, another starting node resumes it.
The nodes cannot serve until the complete inventory succeeds. Later starts
use the completed format without another migration. Alarms can run late
during the outage.

Do not roll back by starting an old binary against the upgraded fleet.
Restore the complete stopped-fleet backup for a rollback. That restoration
loses writes made after the backup, so preserve those writes separately.

Normal restarts and ownership transfers within this format preserve the
alarm history. Each restored writer takes a new ownership epoch and keeps
the stored sequence and consumed state, so an old mutation cannot target
a later installation.
