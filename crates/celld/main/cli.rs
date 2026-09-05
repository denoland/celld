// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The command line: what the process was asked to do, and the help text
//! that documents it.
//!
//! Parsing answers one question — which `Action` to take — and refuses
//! anything ambiguous rather than guessing. The help text is the public
//! description of the configuration surface and remains stable across builds.
pub(crate) struct Settings {
    pub(crate) control_plane: bool,
    pub(crate) bucket: Option<String>,
    pub(crate) load_deployment: bool,
    pub(crate) endpoint: Option<String>,
    pub(crate) region: String,
    pub(crate) listen: celld::startup::Listen,
    pub(crate) internal_listen: celld::startup::Listen,
    pub(crate) advertise: Option<String>,
    pub(crate) unsafe_public_advertise: bool,
    /// Whether forwarded scheme and host headers can set `request.url`.
    /// This option is off unless a trusted proxy replaces both headers.
    pub(crate) trust_forwarded_headers: bool,
    /// Whether a node tests the bucket's conditional writes and ranged reads
    /// before it serves. On by default, because a store that mishandles either
    /// operation cannot safely serve cell data.
    pub(crate) storage_probe: bool,
    /// Set only by the `celld dev` supervisor for its child node. No fleet
    /// flag or public environment variable selects the local backend.
    pub(crate) dev_store: Option<std::path::PathBuf>,
}

pub(crate) enum Action {
    Run(Settings),
    Diagnose {
        settings: Settings,
        peers: Vec<String>,
        /// Skip the write probe, for an operator who diagnoses with a
        /// credential that cannot write.
        read_only: bool,
        /// Print one JSON object per check instead of a text verdict line.
        json: bool,
    },
    Deploy(Vec<String>),
    Dev(Vec<String>),
    Cell(Vec<String>),
    D1(Vec<String>),
    Kv(Vec<String>),
    Queue(Vec<String>),
    Connect(Vec<String>),
    Credentials(Vec<String>),
    Token(Vec<String>),
    Disconnect(Vec<String>),
    Help,
    Version,
}

impl Action {
    /// Whether this invocation's stdout carries data rather than a log stream.
    ///
    /// A node's stdout *is* its log, and Docker and journald read it. A CLI
    /// subcommand's stdout is its answer: `celld kv get KEY > value` must write
    /// the value and nothing else, and `celld d1 execute` prints JSON a script
    /// parses. Sharing one stream between the two prefixes an operator's data
    /// with whatever the process happened to warn about at startup, which is
    /// how this was found -- an allocator warning landed in front of a value.
    pub(crate) fn stdout_is_data(&self) -> bool {
        !matches!(
            self,
            Self::Run(_) | Self::Dev(_) | Self::Help | Self::Version
        )
    }
}

pub(crate) fn action_from_process() -> anyhow::Result<Action> {
    let mut arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(action) = arguments.first().map(String::as_str) {
        let arguments = arguments[1..].to_vec();
        match action {
            "deploy" => return Ok(Action::Deploy(arguments)),
            "dev" => return Ok(Action::Dev(arguments)),
            "cell" => return Ok(Action::Cell(arguments)),
            "d1" => return Ok(Action::D1(arguments)),
            "kv" => return Ok(Action::Kv(arguments)),
            "queue" => return Ok(Action::Queue(arguments)),
            "connect" => return Ok(Action::Connect(arguments)),
            "credentials" => return Ok(Action::Credentials(arguments)),
            "token" => return Ok(Action::Token(arguments)),
            "disconnect" => return Ok(Action::Disconnect(arguments)),
            _ => {}
        }
    }
    let diagnose = arguments.first().is_some_and(|value| value == "diagnose");
    if diagnose {
        arguments.remove(0);
    }
    if matches!(
        arguments.first().map(String::as_str),
        Some("--help" | "-h" | "help")
    ) {
        return Ok(Action::Help);
    }
    if matches!(
        arguments.first().map(String::as_str),
        Some("--version" | "-V" | "version")
    ) {
        return Ok(Action::Version);
    }
    let env = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };
    let celld_bucket = env("CELLD_BUCKET");
    let fixture_bucket = env("CELLD_TEST_BUCKET");
    let control_plane = celld::env_vars::flag("CELLD_CLOUD", false)?;
    let configured_listen = env("CELLD_ADDR");
    let configured_internal_listen = env("CELLD_INTERNAL_ADDR");
    let configured_advertise = env("CELLD_ADVERTISE");
    if !diagnose
        && arguments.is_empty()
        && !control_plane
        && celld_bucket.is_none()
        && fixture_bucket.is_none()
        && configured_listen.is_none()
        && configured_internal_listen.is_none()
        && configured_advertise.is_none()
    {
        return Ok(Action::Help);
    }
    let mut listen_configured = configured_listen.is_some();
    let mut internal_listen_configured = configured_internal_listen.is_some();
    let mut peers = Vec::new();
    let mut read_only = false;
    let mut json = false;
    let mut settings = Settings {
        control_plane,
        bucket: fixture_bucket.or_else(|| celld_bucket.clone()),
        load_deployment: celld_bucket.is_some(),
        endpoint: env("S3_ENDPOINT"),
        region: env("AWS_REGION")
            .or_else(|| env("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|| "us-east-1".to_string()),
        // This placeholder is resolved after command-line mode flags. An
        // explicit environment or CLI address always wins; otherwise managed
        // mode selects the first free demo port and standalone uses 8080,
        // matching celld's listener contract.
        listen: configured_listen
            .map(celld::startup::Listen::Explicit)
            .unwrap_or(celld::startup::Listen::AutoLoopback),
        internal_listen: configured_internal_listen
            .map(celld::startup::Listen::Explicit)
            .unwrap_or(celld::startup::Listen::LoopbackEphemeral),
        advertise: configured_advertise,
        unsafe_public_advertise: celld::env_vars::flag("CELLD_UNSAFE_PUBLIC_ADVERTISE", false)?,
        trust_forwarded_headers: celld::env_vars::flag("CELLD_TRUST_FORWARDED_HEADERS", false)?,
        storage_probe: celld::env_vars::flag("CELLD_STORAGE_PROBE", true)?,
        dev_store: if diagnose {
            None
        } else {
            std::env::var_os("CELLD_INTERNAL_DEV_STORE").map(std::path::PathBuf::from)
        },
    };
    let mut args = arguments.into_iter();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--control-plane" => settings.control_plane = true,
            "--bucket" => {
                let bucket = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--bucket requires a value"))?;
                settings.bucket = Some(bucket);
                settings.load_deployment = true;
            }
            "--endpoint" => {
                settings.endpoint = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--endpoint requires a value"))?,
                );
            }
            "--region" => {
                settings.region = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--region requires a value"))?;
            }
            "--listen" => {
                settings.listen = celld::startup::Listen::Explicit(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--listen requires a value"))?,
                );
                listen_configured = true;
            }
            "--internal-listen" => {
                settings.internal_listen = celld::startup::Listen::Explicit(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--internal-listen requires a value"))?,
                );
                internal_listen_configured = true;
            }
            "--advertise" => {
                settings.advertise = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--advertise requires a value"))?,
                );
            }
            "--unsafe-public-advertise" => settings.unsafe_public_advertise = true,
            "--trust-forwarded-headers" => settings.trust_forwarded_headers = true,
            "--peer" if diagnose => {
                let peer = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--peer requires a node-session ID"))?;
                celld::startup::validate_peer_id(&peer)?;
                peers.push(peer);
            }
            "--read-only" if diagnose => read_only = true,
            "--json" if diagnose => json = true,
            "--no-control-plane" => settings.control_plane = false,
            other => {
                anyhow::bail!("unknown command or option: {other}; run `celld --help` for usage")
            }
        }
    }
    if !listen_configured {
        settings.listen = if settings.control_plane {
            celld::startup::Listen::AutoLoopback
        } else {
            celld::startup::Listen::Explicit("127.0.0.1:8080".to_string())
        };
    }
    let public_listener_is_non_loopback = match &settings.listen {
        celld::startup::Listen::Explicit(address) => address
            .parse::<std::net::SocketAddr>()
            .is_ok_and(|address| !address.ip().is_loopback()),
        _ => false,
    };
    if listen_configured && public_listener_is_non_loopback && !internal_listen_configured {
        anyhow::bail!(
            "a non-loopback --listen or CELLD_ADDR requires an explicit --internal-listen or \
             CELLD_INTERNAL_ADDR; celld does not reuse the public Worker listener for peers"
        );
    }
    if settings.advertise.is_some() && !internal_listen_configured {
        anyhow::bail!(
            "--advertise or CELLD_ADVERTISE requires an explicit --internal-listen or \
             CELLD_INTERNAL_ADDR; the default internal listener uses a random loopback port"
        );
    }
    if settings.dev_store.is_some() {
        anyhow::ensure!(
            !settings.control_plane
                && settings.bucket.as_deref() == Some("celld-dev")
                && settings.endpoint.is_none()
                && settings.advertise.is_none(),
            "the internal development store requires the celld dev node configuration"
        );
    }
    Ok(if diagnose {
        Action::Diagnose {
            settings,
            peers,
            read_only,
            json,
        }
    } else {
        Action::Run(settings)
    })
}

pub(crate) fn print_help() -> anyhow::Result<()> {
    let help = format!(
        r#"celld — self-hosted, distributed Durable Objects

USAGE:
  celld --bucket [s3://|gs://|az://]NAME[/PREFIX] [OPTIONS]
  celld deploy [PROJECT] --bucket [s3://|gs://|az://]NAME[/PREFIX] [OPTIONS]
  celld dev [PROJECT] [--host IP] [--port PORT] [--logs]
  celld cell list [CLASS] --bucket [s3://|gs://|az://]NAME[/PREFIX] [OPTIONS]
  celld d1 migrations apply DATABASE [PROJECT] --bucket [s3://|gs://|az://]NAME[/PREFIX]
  celld d1 execute DATABASE --command SQL [PROJECT] --bucket [s3://|gs://|az://]NAME[/PREFIX]
  celld kv get|put|delete|list|info NAMESPACE --bucket [s3://|gs://|az://]NAME[/PREFIX]
  celld queue info|peek|purge|pause|resume|redrive QUEUE --bucket [s3://|gs://|az://]NAME[/PREFIX]
  celld diagnose --bucket [s3://|gs://|az://]NAME[/PREFIX] [OPTIONS] [--peer NODE_ID]...

Production install: celld --bucket s3://NAME [OPTIONS]
                    celld --bucket gs://NAME [OPTIONS]

OPTIONS:
  --bucket [s3://|gs://|az://]NAME[/PREFIX]
                         Fleet bucket; s3:// (or no scheme) uses the standard
                         AWS credential chain, gs:// selects Google Cloud
                         Storage via Application Default Credentials, az://
                         names an Azure Blob Storage container and takes its
                         account from AZURE_STORAGE_ACCOUNT_NAME (celld
                         rejects --endpoint and ignores --region for both). A
                         PREFIX puts every object under it, so several fleets
                         can share one bucket
  --endpoint URL         Optional S3-compatible endpoint
  --region REGION        Storage region (default: AWS_REGION or us-east-1)
  --listen IP:PORT       Public Worker listener (default: 127.0.0.1:8080;
                         a non-loopback address requires --internal-listen)
  --internal-listen IP:PORT
                         Peer and unauthenticated operator listener
                         (default: 127.0.0.1:0)
  --advertise ADDR:PORT  Address peers can reach: IP:PORT or HOST:PORT
                         (requires --internal-listen or CELLD_INTERNAL_ADDR)
  --peer NODE_ID         Diagnose one node with a signed direct probe; repeatable
  --json                 Print one JSON object per diagnostic check
  --read-only            Skip the bucket write probe. celld diagnose otherwise
                         tests the conditional write, which writes and deletes a
                         small object under the probe/ prefix
  --unsafe-public-advertise
                         Permit a literal public IP in --advertise. The flag does
                         not resolve hostnames or restrict --internal-listen.
                         Peer traffic has no built-in TLS, and operator routes
                         permit unauthenticated work, eviction, state inspection,
                         and shutdown
  --trust-forwarded-headers
                         Let X-Forwarded-Host and X-Forwarded-Proto set the
                         scheme and host in request.url. Set this option only
                         when a trusted proxy replaces both headers
  -h, --help             Show this help
  -V, --version          Show the celld version

ENVIRONMENT:
  Boolean variables accept only `0` or `1`; invalid values stop startup.
  CELLD_BUCKET                    Fleet bucket and prefix; same as --bucket
  S3_ENDPOINT                     S3-compatible endpoint; same as --endpoint
  AWS_REGION, AWS_DEFAULT_REGION  Storage region (default: us-east-1)
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
                                  Explicit credentials in the standard AWS chain
  CELLD_ADDR                      Public Worker listener; same as --listen
  CELLD_INTERNAL_ADDR             Peer and operator listener; same as --internal-listen
  CELLD_ADVERTISE                 Peer-reachable address; same as --advertise
  CELLD_UNSAFE_PUBLIC_ADVERTISE   `1` permits a literal public IP in
                                  CELLD_ADVERTISE; it does not resolve hostnames
                                  or restrict CELLD_INTERNAL_ADDR
  CELLD_TRUST_FORWARDED_HEADERS   `1` trusts X-Forwarded-Host and
                                  X-Forwarded-Proto
  CELLD_NODE                      Node-session ID (default: generated)
  CELLD_WATCH                     Local SQLite/replication working directory
  CELLD_ESBUILD                   Override esbuild executable path
  CELLD_ACTIVATIONS               Concurrent cold activations
  CELLD_EVICTIONS                 Concurrent evictions (default: 4)
  CELLD_VARS_FILE, CELLD_VAR_*    Worker variable overrides
  CELLD_ASSET_CACHE_DIR           Downloaded static-asset cache directory
  CELLD_ASSET_CACHE_BYTES         Asset cache limit

TUNING:
  CELLD_STORAGE_PROBE             `0` skips the startup storage-contract test
                                  (default: on)
  CELLD_TTL_MS                    Node lease lifetime (default: 10000)
  CELLD_OPERATION_DEADLINE_MS     Non-restore operation deadline (default: 15000)
  CELLD_SHUTDOWN_DRAIN_MS         Shutdown handoff no-progress interval (default: 25000)
  CELLD_SHUTDOWN_TOTAL_MS         Complete process stop bound (default: {shutdown_total_ms})
  CELLD_DRAIN_TOKEN_WAIT_MS       Drain-token wait before an unserialized
                                  handoff; at most {token_wait_numerator}/{token_wait_denominator} of the complete
                                  stop bound (default: {drain_token_wait_ms}; 0 disables)
  CELLD_READY_FLEET_GATE_MS       First-readiness wait for fleet capacity
                                  (default: 120000; 0 disables)
  CELLD_REBALANCE_INTERVAL_MS     Ownership balancing sample interval
                                  (default: 5000; 0 disables)
  CELLD_REBALANCE_BATCH_CELLS     Idle cells one balancing batch moves
                                  (default: 32)
  CELLD_PLACEMENT_WEIGHT          Relative ownership share (default: the
                                  CPU count)
  CELLD_IDLE_EVICT_S              Idle-cell eviction age (disabled unless set)
  CELLD_RECOVERY_RETRY_MS         Dead-owner recovery retry backoff (default: 1000)
  CELLD_RECOVERY_RETRIES          Recovery retries before a request fails (default: 240)
  CELLD_LOCAL_CACHE_MAX_BYTES     Hibernated SQLite cache limit (default: 2 GiB; 0 disables)
  CELLD_MAX_RESIDENT_CELLS        Resident-cell hard cap, enforced at admission
  CELLD_MAX_CELL_REQUESTS         Concurrent fetches per cell (default: 64)
  CELLD_MAX_REQUEST_BODY_BYTES    Ingress request body limit (default: 1 GiB)
  CELLD_PRESSURE_OWNERSHIP        release to rebalance, sticky to cache locally
  CELLD_MAX_RSS_MB                Active-memory shed threshold (default: 80%; 0 disables)
  CELLD_ALARM_RESIDENT_MS         Near-alarm residency window
  CELLD_WAKER_TICK_MS             Orphan-alarm scan interval
  CELLD_V8_HEAP_LIMIT_MB          Per-isolate V8 heap limit
  CELLD_FETCH_TIMEOUT_S           Outbound fetch timeout
  CELLD_HANDLER_BUDGET_S          JavaScript handler budget
  CELLD_TOKIO_THREADS             Tokio runtime worker threads
  CELLD_OUTPUT_GATE               `0` removes the durability wait from writes
  CELLD_DURABILITY                `fleet` acks when every follower holds the
                                  write on disk, or when the bucket upload
                                  wins, and uploads behind either way; needs
                                  2+ nodes, else every ack waits for the
                                  bucket and writes are much slower
                                  (default: `fleet`; `bucket` always waits)
  CELLD_LOG_CAPTURE_WORKERS       Concurrent log-capture workers (default: 8)
  CELLD_LOG_PIPELINE              Fleet log rounds in flight (default: 4)
  CELLD_LOG_GROUP_COMMIT_MS       Queue group-commit wait (default: 1 ms; 0 disables)
  CELLD_LOG_HEDGE_MS              Duplicate a slow log append (default: adaptive; 0 disables)
  CELLD_LTX_TRUNCATE_PAGES        WAL pages before a truncate checkpoint (default: 128; Queues never truncate; 0 disables)
  RUST_LOG                        Runtime log filter (default: info)

EXPERIMENTAL:
  CELLD_WORKER_LOADER             Worker Loader binding name for Code Mode
  CELLD_AI_BINDING, CELLD_AI_URL  AI binding name and endpoint

Documentation: https://celld.dev/docs"#,
        shutdown_total_ms = celld::env_vars::DEFAULT_SHUTDOWN_TOTAL_MS,
        drain_token_wait_ms = celld::env_vars::DEFAULT_DRAIN_TOKEN_WAIT_MS,
        token_wait_numerator = celld::env_vars::MAX_DRAIN_TOKEN_WAIT_NUMERATOR,
        token_wait_denominator = celld::env_vars::MAX_DRAIN_TOKEN_WAIT_DENOMINATOR,
    );
    celld::cli_output::Output::new(celld::cli_output::Format::Text).help(&help)
}

pub(crate) fn worker_loader_binding() -> Option<String> {
    std::env::var("CELLD_WORKER_LOADER")
        .ok()
        .filter(|name| !name.is_empty())
}
