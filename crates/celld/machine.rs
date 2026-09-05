// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! What the process can learn about where it is running.
//!
//! Two kinds of fact, both read once at startup or sampled on a timer:
//! the environment the operator set, and the machine underneath — memory,
//! CPU ticks, page size — which is per-platform and therefore duplicated
//! behind `cfg` for each one.
use celld_logic::OwnershipOnEvict;
use rand::RngCore as _;

/// Validate one node name before it enters a storage key, a local path, or a
/// peer message. All three surfaces use this same restricted identity.
pub(crate) fn validate_node_name(node: &str) -> anyhow::Result<()> {
    if celld_logic::peer::valid_identity(node) {
        Ok(())
    } else {
        anyhow::bail!(
            "must be a safe node name of 1 to 128 ASCII letters, numbers, dots, dashes, or underscores"
        )
    }
}

/// The default bound on concurrent cold activations for a machine with this
/// many hardware threads: eight per thread, at least 16, at most 128.
///
/// A cold activation is object-store latency, not compute. Measured on GCS
/// from a 4-vCPU node (2026-09-01), the route was ~100 ms of round trips
/// (owner read 25 ms, ownership CAS 60 ms, restore 10 ms) and ~10 ms of CPU,
/// so a bound equal to the thread count admitted 35-40 activations a second
/// per node whatever the client offered, and 1,000 hibernated alarms due in
/// one minute were 29 s late at the tail (issue #42 found the same ceiling
/// on two-core nodes). Eight per thread keeps the CPU share of the route
/// below one core per thread at full duty (measured 30 % of four vCPUs at
/// 16 slots, 43 % at 64); the floor keeps a one- or two-thread node from
/// serializing an I/O-bound path; the cap is the point past which more
/// concurrent cold I/O can itself become an object-store storm, and it is
/// the value the previous default already capped at.
pub fn default_max_activations(available_parallelism: usize) -> usize {
    const PER_THREAD: usize = 8;
    const FLOOR: usize = 16;
    const CAP: usize = 128;
    available_parallelism
        .saturating_mul(PER_THREAD)
        .clamp(FLOOR, CAP)
}

pub fn random_node_session_id() -> String {
    let mut bytes = [0_u8; 16];
    crate::asyncrt::rng("session").fill_bytes(&mut bytes);
    let suffix = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("node_{suffix}")
}

pub fn random_peer_key() -> [u8; 32] {
    let mut key = [0_u8; 32];
    crate::asyncrt::rng("peer_key").fill_bytes(&mut key);
    key
}

pub fn random_process_generation() -> String {
    let mut generation = [0_u8; 32];
    crate::asyncrt::rng("generation").fill_bytes(&mut generation);
    generation
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Watermarks from celld's public environment contract.
/// `CELLD_PRESSURE_OWNERSHIP`: `release` (the default) or `sticky`.
/// The lease lifetime, as the capacity listing needs it to decide which node
/// records are stale enough to skip reading. Same variable and default the
/// lease itself uses.
/// Concurrent outbound WebSockets one cell may hold
/// (`CELLD_MAX_OUTBOUND_WEBSOCKETS`).
pub const DEFAULT_MAX_OUTBOUND_WEBSOCKETS: usize = 32;

/// Backoff between dead-owner log recovery retries. One second keeps a
/// waiting takeover responsive to the sweep finishing without hammering
/// the recovery lock.
pub const DEFAULT_OWNER_LOG_RECOVERY_BACKOFF_MS: u64 = 1_000;

/// Recovery retry cycles before a takeover request fails. 240 cycles at
/// the default backoff covers the ~155 s dead-leader sweep measured for
/// a thousand-cell owner with a wide margin;
/// clients bound their own waits far earlier, so the cap only fires on
/// a recovery that is truly wedged.
pub const DEFAULT_OWNER_LOG_RECOVERY_ATTEMPTS: u32 = 240;

pub const DEFAULT_LOCAL_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Matches the fleet and ownership-store clients. Long enough to survive a
/// slow but live peer, short enough that a stale address does not hold a
/// request for the kernel's own connect timeout.
pub const PEER_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

pub fn lease_ttl_ms_from_environment() -> u64 {
    crate::env_vars::positive_or("CELLD_TTL_MS", 10_000).expect("validated CELLD_TTL_MS")
}

pub fn ownership_on_evict_from_environment() -> anyhow::Result<OwnershipOnEvict> {
    match std::env::var("CELLD_PRESSURE_OWNERSHIP") {
        Err(_) => Ok(OwnershipOnEvict::Release),
        Ok(value) => match value.trim() {
            "release" => Ok(OwnershipOnEvict::Release),
            "sticky" => Ok(OwnershipOnEvict::Sticky),
            other => Err(anyhow::anyhow!(
                "CELLD_PRESSURE_OWNERSHIP must be `release` or `sticky`, not `{other}`"
            )),
        },
    }
}

/// Total memory this process may use: the cgroup limit when one applies
/// (containers), the machine otherwise.
#[cfg(target_os = "linux")]
#[allow(clippy::disallowed_methods)] // cgroup and `/proc` files describe the host.
fn total_memory_bytes() -> Option<u64> {
    if let Some(limit) = crate::memory::cgroup_limit_bytes() {
        return Some(limit);
    }
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kb = meminfo
        .lines()
        .find(|line| line.starts_with("MemTotal:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?;
    Some(kb.saturating_mul(1024))
}

#[cfg(target_os = "macos")]
fn total_memory_bytes() -> Option<u64> {
    let mut size: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    let ok = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&raw mut size).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (ok == 0 && size > 0).then_some(size)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn total_memory_bytes() -> Option<u64> {
    None
}

pub fn pressure_config_from_environment() -> anyhow::Result<celld_logic::pressure::PressureConfig> {
    // Residency is a hard cap (`CELLD_MAX_RESIDENT_CELLS` -> `Config::max_resident`),
    // enforced at admission -- not a pressure watermark, so it is not built here.
    // Memory shedding is on by default: a node that runs into its memory ceiling
    // must give cells back, not be killed. The arithmetic lives in the core;
    // the shell supplies only the two facts it can read.
    let config = celld_logic::pressure::PressureConfig::from_limits(
        total_memory_bytes(),
        crate::env_vars::optional::<u64>("CELLD_MAX_RSS_MB")?,
    );
    if config.ceiling_above_cap() {
        tracing::warn!(
            high_bytes = config.high_bytes,
            rss_hard_bytes = config.rss_hard_bytes,
            "CELLD_MAX_RSS_MB is at or above the absolute cap, so the node \
             decides on its complete cgroup charge or RSS fallback"
        );
    }
    Ok(config)
}

pub fn local_cache_max_bytes_from_environment() -> anyhow::Result<Option<u64>> {
    let bytes = crate::env_vars::with_default(
        "CELLD_LOCAL_CACHE_MAX_BYTES",
        DEFAULT_LOCAL_CACHE_MAX_BYTES,
    )?;
    Ok((bytes > 0).then_some(bytes))
}

#[cfg(all(test, celld_internal_tests))]
mod internal_tests {
    include!(env!("CELLD_INTERNAL_MACHINE_TESTS"));
}
