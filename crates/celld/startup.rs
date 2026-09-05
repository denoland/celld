// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Process startup primitives shared by the standalone and eventual managed
//! celld adapters.
//!
//! Listener policy is copied from celld's production startup path. Keeping it
//! outside the executor matters: both sockets are reserved before storage or
//! V8 work starts, and the executor receives an already-validated peer
//! address for the internal socket.

use anyhow::{anyhow, Context};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::{TcpListener, TcpSocket};
use tracing::warn;

const AUTO_LISTEN_START: u16 = 8080;
const AUTO_LISTEN_END: u16 = 8099;

/// `TcpListener::bind` inherits socket2's historical backlog of 128. That is
/// smaller than a routine reconnect cohort at the target fleet density: 1,200
/// chat clients returning together filled every node's accept queue and
/// produced connection resets while the runtime itself remained healthy.
/// Request the common Linux `somaxconn` default; the kernel still applies its
/// configured ceiling.
const LISTEN_BACKLOG: u32 = 4_096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Listen {
    AutoLoopback,
    LoopbackEphemeral,
    Explicit(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InternalListenerSettings {
    pub listen: Listen,
    pub advertise: Option<String>,
    pub unsafe_public_advertise: bool,
}

/// How peers should reach this node.
///
/// A hostname is allowed because container and PaaS environments often hand
/// out a stable name and an unstable or ambiguous IP. The name is deliberately
/// not resolved here: reachability is defined from each peer's network.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Advertise {
    Addr(SocketAddr),
    Name { host: String, port: u16 },
}

impl Advertise {
    pub fn scope(&self) -> &'static str {
        match self {
            Self::Addr(address) => advertise_scope(address.ip()),
            Self::Name { .. } => "resolved by each peer",
        }
    }

    pub fn is_public_ip(&self) -> bool {
        matches!(self, Self::Addr(address) if advertise_scope(address.ip()) == "operator-routed network")
    }
}

impl fmt::Display for Advertise {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Addr(address) => write!(formatter, "{address}"),
            Self::Name { host, port } => write!(formatter, "{host}:{port}"),
        }
    }
}

fn advertise_scope(address: IpAddr) -> &'static str {
    match address {
        IpAddr::V4(address) if address.is_loopback() => "same host only",
        IpAddr::V4(address) if address.is_private() || is_shared_overlay(address) => {
            "private network"
        }
        IpAddr::V4(address) if address.is_link_local() => "local link only",
        IpAddr::V6(address) if address.is_loopback() => "same host only",
        IpAddr::V6(address) if address.is_unique_local() => "private network",
        IpAddr::V6(address) if address.is_unicast_link_local() => "local link only",
        _ => "operator-routed network",
    }
}

/// RFC 6598 shared address space is not globally routable. Overlay networks
/// such as Tailscale allocate peer addresses from this exact `100.64.0.0/10`
/// block, so treating it as public rejects the private overlay operators are
/// otherwise instructed to use.
fn is_shared_overlay(address: Ipv4Addr) -> bool {
    let octets = address.octets();
    octets[0] == 100 && (64..=127).contains(&octets[1])
}

#[derive(Debug)]
pub struct BoundListener {
    pub listener: TcpListener,
    pub listen: SocketAddr,
}

#[derive(Debug)]
pub struct BoundInternalListener {
    pub listener: TcpListener,
    pub listen: SocketAddr,
    pub advertise: Advertise,
}

/// Descriptors one resident cell holds: its SQLite database, WAL, and shared-
/// memory file, plus what replication keeps open for them. Measured at ~7 on
/// Linux; budgeted at 8.
const FDS_PER_CELL: u64 = 8;

/// Below this the node cannot reach the density the runtime is designed for,
/// so say so at startup rather than at the failing activation.
const MIN_RESIDENT_CELLS: u64 = 1_000;

/// Raise the descriptor limit to its hard ceiling, and warn when even that
/// bounds residency.
#[cfg(unix)]
pub fn raise_file_limit() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return;
    }
    if limit.rlim_cur < limit.rlim_max {
        let raised = libc::rlimit {
            rlim_cur: limit.rlim_max,
            ..limit
        };
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) };
        // Re-read rather than assume the raise took: macOS reports an
        // unlimited hard limit, refuses it as a soft limit, and caps the
        // process at kern.maxfilesperproc.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
            return;
        }
    }
    let cells = limit.rlim_cur / FDS_PER_CELL;
    if cells < MIN_RESIDENT_CELLS {
        warn!(
            event = "file_limit_bounds_residency",
            open_files = limit.rlim_cur,
            resident_cells = cells,
            "the open-file limit caps this node near {cells} resident cells; \
             raise it (ulimit -n {}, or LimitNOFILE= in the unit file)",
            MIN_RESIDENT_CELLS * FDS_PER_CELL
        );
    }
}

#[cfg(not(unix))]
pub fn raise_file_limit() {}

fn bind_tcp_listener(address: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = match address {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };
    #[cfg(unix)]
    socket.set_reuseaddr(true)?;
    socket.bind(address)?;
    socket.listen(LISTEN_BACKLOG)
}

async fn bind_listener(listen: &Listen, option: &str) -> anyhow::Result<BoundListener> {
    let (listener, listen) = match listen {
        Listen::Explicit(address) => {
            let address = parse_socket_address(address, option)?;
            let listener =
                bind_tcp_listener(address).with_context(|| format!("bind {option} {address}"))?;
            let listen = listener.local_addr().context("read bound listen address")?;
            (listener, listen)
        }
        Listen::AutoLoopback => {
            let mut last_error = None;
            let mut bound = None;
            for port in AUTO_LISTEN_START..=AUTO_LISTEN_END {
                let address = SocketAddr::from(([127, 0, 0, 1], port));
                match bind_tcp_listener(address) {
                    Ok(listener) => {
                        bound = Some((listener, address));
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {
                        last_error = Some(error);
                    }
                    Err(error) => return Err(error).with_context(|| format!("bind {address}")),
                }
            }
            bound.ok_or_else(|| {
                anyhow!(
                    "no free managed demo port in 127.0.0.1:{AUTO_LISTEN_START}-{AUTO_LISTEN_END}: {}",
                    last_error
                        .map(|error| error.to_string())
                        .unwrap_or_else(|| "range exhausted".to_string())
                )
            })?
        }
        Listen::LoopbackEphemeral => {
            let address = SocketAddr::from(([127, 0, 0, 1], 0));
            let listener =
                bind_tcp_listener(address).with_context(|| format!("bind {option} {address}"))?;
            let listen = listener.local_addr().context("read bound listen address")?;
            (listener, listen)
        }
    };

    Ok(BoundListener { listener, listen })
}

pub async fn bind_ingress_listener(listen: &Listen) -> anyhow::Result<BoundListener> {
    bind_listener(listen, "--listen").await
}

pub async fn bind_internal_listener(
    settings: &InternalListenerSettings,
) -> anyhow::Result<BoundInternalListener> {
    let bound = bind_listener(&settings.listen, "--internal-listen").await?;
    let listen = bound.listen;

    let advertise = if let Some(address) = settings.advertise.as_deref() {
        parse_advertise(address)?
    } else if listen.ip().is_unspecified() {
        return Err(anyhow!(
            "--advertise is required when --internal-listen uses an unspecified address ({listen}).\n\
             Give the address peers should dial, for example --advertise \
             {}:{} or --advertise node1.example.com:{}",
            "10.0.0.1",
            listen.port(),
            listen.port()
        ));
    } else {
        Advertise::Addr(listen)
    };
    if advertise.is_public_ip() && !settings.unsafe_public_advertise {
        return Err(anyhow!(
            "--advertise {advertise} is a public IP address; the internal listener \
             has an unauthenticated operator API, and peer HTTP carries application \
             data without built-in TLS. Use a private overlay, or pass \
             --unsafe-public-advertise to accept this exposure explicitly"
        ));
    }

    Ok(BoundInternalListener {
        listener: bound.listener,
        listen,
        advertise,
    })
}

fn parse_socket_address(value: &str, option: &str) -> anyhow::Result<SocketAddr> {
    value
        .parse()
        .with_context(|| format!("{option} must be an IP socket address such as 127.0.0.1:8080"))
}

/// Parse `--advertise`, accepting `IP:PORT` or `HOST:PORT`.
pub fn parse_advertise(value: &str) -> anyhow::Result<Advertise> {
    let value = value.trim();
    if let Ok(address) = value.parse::<SocketAddr>() {
        if address.port() == 0 {
            return Err(anyhow!("--advertise must use a nonzero port"));
        }
        if address.ip().is_unspecified() || address.ip().is_multicast() {
            return Err(anyhow!(
                "--advertise must be a concrete unicast address, not {address}"
            ));
        }
        if matches!(address.ip(), IpAddr::V4(ip) if ip.is_broadcast()) {
            return Err(anyhow!("--advertise cannot use the IPv4 broadcast address"));
        }
        return Ok(Advertise::Addr(address));
    }

    let (host, port) = value.rsplit_once(':').ok_or_else(|| {
        anyhow!("--advertise must be HOST:PORT or IP:PORT, such as node1.example.com:8080")
    })?;
    let port: u16 = port.parse().with_context(|| {
        format!("--advertise port {port:?} is not a number between 1 and 65535")
    })?;
    if port == 0 {
        return Err(anyhow!("--advertise must use a nonzero port"));
    }
    if !is_dns_name(host) {
        return Err(anyhow!(
            "--advertise host {host:?} is not a valid hostname or IP address"
        ));
    }
    Ok(Advertise::Name {
        host: host.to_string(),
        port,
    })
}

fn is_dns_name(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '-')
        })
}

pub fn validate_peer_id(value: &str) -> anyhow::Result<()> {
    crate::machine::validate_node_name(value).map_err(|_| {
        anyhow!("--peer must be one safe node-session ID without a path or whitespace")
    })
}
