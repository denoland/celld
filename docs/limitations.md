# Limitations

celld is an alpha. The current release has these operational limits. See
[Cloudflare compatibility](cloudflare-compat.md) for the supported services,
APIs, and Wrangler configuration.

## Fleets

- A fleet runs one application. celld has no account service, multi-tenant
  scheduler, or managed ingress.
- A fleet stores its durable state in an S3-compatible bucket, a Google Cloud
  Storage bucket, or an Azure Blob Storage container. The `celld dev` command
  instead uses a local SQLite object store. A regular node or an operator
  subcommand cannot select this local backend.
- Ownership balancing counts cells by node weight. It does not measure the
  CPU or memory that one cell uses, and it moves only hibernated cells, so a
  fleet without idle eviction balances only the cells that hibernate on
  their own.

## Networking and security

- celld does not terminate TLS. Terminate public TLS at an ingress proxy, and
  put the internal listener on a private network or an encrypted overlay.
- Peer traffic crosses the internal network as plaintext HTTP. The fleet HMAC
  authenticates tunnel establishment and control requests, and it does not
  encrypt application data on the wire. The network layer must provide the
  confidentiality.
- The fleet bucket controls the fleet. Give its credentials access to one fleet
  only. See [Security](security.md).

## Object storage credentials

- celld supports different credential methods for each object storage provider.
  See [Configure object storage](README.md#configure-object-storage) for the
  available methods and their precedence.
- Azure identity support is limited to the public Azure cloud. A managed
  identity from Azure App Service or Azure Container Apps does not work. Use a
  workload identity or a storage account key on these platforms.

## WebSockets

- An outbound Durable Object WebSocket keeps its cell resident. The connection
  closes if the cell moves to another node, so the application must store the
  connection intent and reconnect.
- A node limits the cells and outbound WebSockets that can remain resident.

## Platforms and updates

- The installer supplies binaries for Linux x86-64, Linux ARM64, and Apple
  Silicon. Windows is not supported.
