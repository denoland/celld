// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Bucket ownership effect adapter, over either conditional-write dialect.
//!
//! This module deliberately contains serialization, wall-clock sampling, SDK
//! configuration and error classification only. Ownership decisions remain in
//! `celld-logic`.

use crate::bucket::Bucket;
use anyhow::Context;
use celld_logic::{
    CapacityPeer, CasGuard, CasOutcome, LeaseCasOutcome, NodeLeaseRecord, OwnerRecord,
};
use futures_util::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Serialize)]
struct OwnerWire<'a> {
    node: &'a str,
    epoch: u64,
}

#[derive(Deserialize)]
struct OwnerWireOwned {
    node: String,
    epoch: u64,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(from = "NodeLeaseWireRaw")]
pub(crate) struct NodeLeaseWire {
    pub(crate) node: String,
    pub(crate) expires_ms: u64,
    #[serde(default)]
    pub(crate) addr: String,
    #[serde(default)]
    pub(crate) probe_public_key: String,
    #[serde(default)]
    pub(crate) peer_protocol: u16,
    /// This node accepts a signed shutdown-adoption request and responds only
    /// after it publishes the requested cell. The default keeps old node
    /// records readable during a mixed-version rollout.
    #[serde(default)]
    pub(crate) paced_handoff: bool,
    /// The process generation. In production this IS `probe_public_key`,
    /// published twice under two names. Part 1 (this release) reads the
    /// probe key when the old field is absent, see `NodeLeaseWireRaw`.
    /// Part 2, after 2026-10-01: stop writing this field, publish only
    /// `probe_public_key`, and derive the generation from it everywhere,
    /// which needs the probe signer to draw from the simulation-aware RNG
    /// so the deterministic worlds keep their replay, and the private
    /// fixtures that plant `ownership_index_generation` to plant
    /// `probe_public_key` instead. The reader fallback stays; it becomes
    /// the only path.
    #[serde(default, rename = "ownership_index_generation")]
    pub(crate) generation: String,
    /// The folded node log: absent until the
    /// session's first fleet open. Every writer of this record carries it
    /// through unchanged except the log tier itself and recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) log: Option<NodeLogWire>,
    #[serde(default)]
    pub(crate) load: NodeLoadWire,
}

// A separate, versioned key keeps advisory samples out of the node-lease
// namespace. Neither a cached lease nor the refresh claim grants authority.
const CAPACITY_SAMPLE_KEY: &str = "fleet/capacity-v1.json";
const CAPACITY_REFRESH_TIMEOUT_MS: u64 = 30_000;

/// One fleet scan. The lease bodies are kept as the refresher read them,
/// uninterpreted, so the refresher's release never mediates a peer's record:
/// a field a newer release added survives a refresh by an older one, where a
/// typed copy would have dropped it and every newer reader would have seen
/// the default for the length of a rollout.
#[derive(Clone, Deserialize, Serialize)]
struct CapacitySample {
    started_ms: u64,
    leases: Vec<Box<RawValue>>,
}

impl CapacitySample {
    fn age_ms(&self, now: u64) -> Option<u64> {
        now.checked_sub(self.started_ms)
    }

    /// The instant this sample was taken, and its peers.
    fn taken(&self) -> anyhow::Result<(u64, Vec<CapacityPeer>)> {
        Ok((self.started_ms, self.peers()?))
    }

    fn peers(&self) -> anyhow::Result<Vec<CapacityPeer>> {
        self.leases
            .iter()
            .map(|lease| {
                let lease: NodeLeaseWire = serde_json::from_str(lease.get())
                    .context("decode a lease in the fleet sample")?;
                Ok(lease.into_capacity_peer())
            })
            .collect()
    }
}

// Externally tagged on purpose: an internally tagged enum buffers its body
// before it picks the variant, and a `RawValue` cannot be read back out of
// that buffer.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CapacitySampleWire {
    /// A refresh is in flight. `previous` is the sample it replaces, so a
    /// reader whose tick lands inside the refresh keeps the view it had a
    /// tick ago instead of closing its format gate for an interval.
    Refreshing {
        expires_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        previous: Option<CapacitySample>,
    },
    Ready(CapacitySample),
}

impl NodeLeaseWire {
    fn into_capacity_peer(self) -> CapacityPeer {
        CapacityPeer {
            node: self.node,
            addr: self.addr,
            expires_ms: self.expires_ms,
            peer_protocol: self.peer_protocol,
            sampled_ms: self.load.sampled_ms,
            owned_cells: self.load.owned_cells,
            placement_weight: self.load.placement_weight,
            bucket_format: self.load.bucket_format,
            resident_cells: self.load.resident_cells,
            host_websockets: self.load.host_websockets,
            rss_bytes: self.load.rss_bytes,
            in_use_bytes: self.load.in_use_bytes,
            pressured: self.load.pressured,
            memory_headroom: self.load.memory_headroom,
            restoring: self.load.restoring,
            paced_handoff: self.paced_handoff,
            rebalance_paused: self.load.rebalance_paused.unwrap_or(false),
            draining: self.load.draining.unwrap_or(false),
        }
    }
}

/// The lease as it sits in the bucket. Deserialization goes through this
/// shape so a record written without `ownership_index_generation` still
/// yields a generation: a writer that has completed part 2 above publishes
/// only the probe key, and an empty generation would make its peers treat
/// a live node as a pre-generation one for the length of a rollout.
#[derive(Deserialize)]
struct NodeLeaseWireRaw {
    node: String,
    expires_ms: u64,
    #[serde(default)]
    addr: String,
    #[serde(default)]
    probe_public_key: String,
    #[serde(default)]
    peer_protocol: u16,
    #[serde(default)]
    paced_handoff: bool,
    #[serde(default, rename = "ownership_index_generation")]
    generation: String,
    #[serde(default)]
    log: Option<NodeLogWire>,
    #[serde(default)]
    load: NodeLoadWire,
}

impl From<NodeLeaseWireRaw> for NodeLeaseWire {
    fn from(raw: NodeLeaseWireRaw) -> Self {
        let generation = if raw.generation.is_empty() {
            raw.probe_public_key.clone()
        } else {
            raw.generation
        };
        Self {
            node: raw.node,
            expires_ms: raw.expires_ms,
            addr: raw.addr,
            probe_public_key: raw.probe_public_key,
            peer_protocol: raw.peer_protocol,
            paced_handoff: raw.paced_handoff,
            generation,
            log: raw.log,
            load: raw.load,
        }
    }
}

/// The folded log fields, exactly the old log/<session>.json body: the
/// record moved into the lease, the shape did not change.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct NodeLogWire {
    pub(crate) state: String,
    pub(crate) epoch: u64,
    pub(crate) ensemble: Vec<String>,
    pub(crate) tiered: u64,
    #[serde(default)]
    pub(crate) active: bool,
    /// The node recovering this log and its last heartbeat, while the state
    /// is `recovering`. Absent from records older than the claim.
    #[serde(default)]
    pub(crate) claimant: Option<String>,
    #[serde(default)]
    pub(crate) claimed_ms: Option<u64>,
}

pub(crate) fn log_state_from_wire(
    log: &Option<NodeLogWire>,
) -> Option<celld_logic::log_tier::LogState> {
    match log.as_ref().map(|log| log.state.as_str()) {
        Some("open") => Some(celld_logic::log_tier::LogState::Open),
        Some("recovering") => Some(celld_logic::log_tier::LogState::Recovering),
        Some("sealed") => Some(celld_logic::log_tier::LogState::Sealed),
        _ => None,
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct NodeLoadWire {
    pub sampled_ms: u64,
    /// Confirmed ownership, including cells that are dormant and consume no
    /// residency slot. `None` keeps an older node distinct from an empty one.
    #[serde(default)]
    pub owned_cells: Option<usize>,
    /// The share of fleet ownership this node wants, relative to the other
    /// nodes' weights. `None` from a node that predates rebalancing.
    #[serde(default)]
    pub placement_weight: Option<u64>,
    /// The newest bucket format this node reads
    /// (`celld_logic::format::BUCKET_FORMAT`). `None` from a release before
    /// the field, which reads format 1. A node writes a newer format only
    /// while every live lease reads it, so a rolling update never leaves a
    /// cell an old node cannot restore.
    #[serde(default)]
    pub bucket_format: Option<u16>,
    pub resident_cells: usize,
    pub host_websockets: usize,
    pub rss_bytes: u64,
    /// The allocator-adjusted RSS fallback, published next to `rss_bytes` so a
    /// gap between the two is visible.
    ///
    /// `None` means the node did not report it, which a node before this field
    /// existed does not. That is not the same as zero, and a consumer that
    /// ranks nodes must not read a silent zero as the emptiest node in the
    /// fleet for the length of a rolling upgrade.
    #[serde(default)]
    pub in_use_bytes: Option<u64>,
    /// The cgroup working set (`memory.current - inactive_file`).
    #[serde(default)]
    pub cgroup_working_set_bytes: Option<u64>,
    /// The complete cgroup charge from `memory.current`.
    #[serde(default)]
    pub cgroup_current_bytes: Option<u64>,
    pub cpu_percent_x100: u64,
    pub open_fds: u64,
    pub fd_limit: u64,
    pub pressured: bool,
    /// Every configured memory measurement is below its low watermark. `None`
    /// means that a peer predates this field.
    #[serde(default)]
    pub memory_headroom: Option<bool>,
    pub shed_cells: u64,
    /// Cold demand queued behind the activation ceiling. Zero in steady
    /// state; positive only while a restore burst saturates the node, so a
    /// rollout waits it out before it restarts the next node.
    #[serde(default)]
    pub restoring: u64,
    /// An operator paused balancing here (`POST /rebalance/pause`). `None`
    /// from a node that predates the switch, which reads as not paused.
    #[serde(default)]
    pub rebalance_paused: Option<bool>,
    /// The node is draining. `None` from a node that predates the field.
    #[serde(default)]
    pub draining: Option<bool>,
}

#[cfg(all(test, celld_internal_tests))]
#[derive(Clone)]
pub(crate) struct AmbientLoadSample {
    pub rss_bytes: u64,
    pub in_use_bytes: u64,
    pub cgroup_working_set_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    pub open_fds: u64,
    pub fd_limit: u64,
}

/// A node record older than this is not worth reading. Three lease
/// lifetimes, floored, so a fleet with a short TTL does not discard records
/// a slow renewal would have refreshed.
const CAPACITY_RECORD_RECENCY_FLOOR_MS: u64 = 60_000;

fn capacity_record_is_recent(last_modified_secs: i64, now_ms: u64, lease_ttl_ms: u64) -> bool {
    let window_ms = lease_ttl_ms
        .saturating_mul(3)
        .max(CAPACITY_RECORD_RECENCY_FLOOR_MS);
    last_modified_secs >= (now_ms.saturating_sub(window_ms) / 1_000) as i64
}

pub fn now_ms() -> u64 {
    crate::asyncrt::wall_ms().max(0) as u64
}

/// Why a node-lease conditional write did not apply.
///
/// The core decides from this class alone, so the class is a variant and
/// not a flag beside one error type: every failure on this lane has to
/// name which one it is, and a guard added later cannot inherit the
/// wrong answer by saying nothing.
///
/// [`Self::NotCommitted`] means the record in the store still holds what
/// the prior attempt left: either no request byte reached the store, or
/// the store answered in a way that proves it wrote no object. The core
/// keeps its prior authority and retries with the ETag it already has.
/// [`Self::Ambiguous`] means the request reached the store, or can have,
/// so only a readback decides what the record now holds. Ambiguous is the
/// safe default of the two: it costs a readback round trip out of the
/// remaining authority, while a wrong `NotCommitted` sends the next
/// attempt in with a stale ETag.
#[derive(Debug)]
pub enum LeaseCasError {
    NotCommitted(anyhow::Error),
    Ambiguous(anyhow::Error),
}

impl LeaseCasError {
    pub fn error(&self) -> &anyhow::Error {
        match self {
            Self::NotCommitted(error) | Self::Ambiguous(error) => error,
        }
    }

    pub fn into_error(self) -> anyhow::Error {
        match self {
            Self::NotCommitted(error) | Self::Ambiguous(error) => error,
        }
    }
}

impl std::fmt::Display for LeaseCasError {
    /// Forwards the formatter, so `{:#}` still prints anyhow's context
    /// chain — the reason a lease-CAS log line names its cause.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.error(), f)
    }
}

/// The production-compatible conditional object store used by ownership
/// effects. A failed write is reported to the core as ambiguous unless the
/// store definitively returned HTTP 412, or the attempt provably wrote no
/// object. See [`LeaseCasError`].
pub struct BucketOwnership {
    bucket: Bucket,
    lease_bucket: Bucket,
    node: String,
    probe_public_key: String,
    live: Arc<LiveLoad>,
    lease_ttl_ms: u64,
    /// The shared fleet sample's interval and maximum age, the balancing
    /// interval in production. Placement and recruitment read the sample
    /// through it so a burst of either costs one GET each, not a fleet scan.
    fleet_sample_ms: u64,
    /// A deterministic execution must not publish ambient process telemetry into
    /// its authoritative store. The override leaves the simulated clock and
    /// the live Actor counters load-bearing.
    #[cfg(all(test, celld_internal_tests))]
    ambient_load_override: Option<AmbientLoadSample>,
    /// A gated tooth observes whether a deterministic override reached the
    /// production sampler. Such a call advances jemalloc's epoch and reads
    /// process resources even if the caller later replaces the values.
    #[cfg(all(test, celld_internal_tests))]
    production_load_samples: AtomicUsize,
    /// The full folded log object this node's OWN lease record carries,
    /// tagged with a publish sequence.
    /// Renewals snapshot (seq, object) ONCE when they serialize the wire
    /// body, and the applied notification reports the seq of the body
    /// that actually landed — never a re-read of this slot. A confirmation
    /// must carry the identity of the write it confirms.
    own_log: std::sync::Mutex<(u64, Option<NodeLogWire>)>,
    /// Every APPLIED write of our own lease record, as (etag, the publish
    /// seq its body carried). A waiter that published seq S is satisfied
    /// only by applied seq >= S; combined with the OwnLog write lock
    /// (one publish outstanding at a time), >= S implies the applied body
    /// IS the waiter's object.
    applied: tokio::sync::watch::Sender<(String, u64)>,
}

/// What this node currently looks like, for peers deciding where to place a
/// cell. The executor owns these numbers and publishes them on every lease
/// renewal; nothing here decides anything locally.
pub fn set_node_load(load: std::sync::Arc<LiveLoad>) {
    crate::asyncrt::services().set_node_load(load);
}

/// Pause or resume balancing on this node. False when nothing publishes
/// load (no bucket), which is also when there is no fleet to balance.
pub fn set_rebalance_paused(paused: bool) -> bool {
    crate::asyncrt::services()
        .node_load()
        .map(|load| {
            load.rebalance_paused
                .store(paused, std::sync::atomic::Ordering::Relaxed)
        })
        .is_some()
}

/// Is this node over a resource ceiling and recovering? False when nothing
/// publishes load (no bucket), which is also when there is no pressure
/// sampler to say otherwise.
pub fn node_is_shedding() -> bool {
    crate::asyncrt::services()
        .node_load()
        .is_some_and(|load| load.pressured.load(std::sync::atomic::Ordering::Relaxed))
}

#[derive(Debug, Default)]
pub struct LiveLoad {
    pub owned_cells: AtomicUsize,
    /// False until the actor has confirmed every record on its own disk
    /// after a start. The lease publishes no owned count until then, so a
    /// peer never balances against a node that only looks empty.
    pub ownership_confirmed: AtomicBool,
    /// Zero until the actor installs the configured weight, and published
    /// as absent until then, so a peer never balances against a default.
    pub placement_weight: AtomicU64,
    pub resident_cells: AtomicUsize,
    pub host_websockets: AtomicUsize,
    pub cpu_percent_x100: AtomicU64,
    pub pressured: AtomicBool,
    /// Stricter than not pressured: the last sample cleared every resume line.
    pub memory_headroom: AtomicBool,
    /// Cells shed since this process started. Monotonic, and only ever read
    /// by a human or a diagnostic -- placement uses the levels, not the rate.
    pub shed_cells: AtomicU64,
    /// Cold demand queued behind the activation ceiling, republished on every
    /// lease renewal for a rollout to pace on.
    pub restoring: AtomicU64,
    /// Set by `POST /rebalance/pause`. Published in the lease, where one
    /// paused node stops every donor in the fleet.
    pub rebalance_paused: AtomicBool,
    /// Set for the rest of the process once a drain begins, so a peer never
    /// hands a cell to a node that is giving its own away.
    pub draining: AtomicBool,
}

impl BucketOwnership {
    /// Creates an adapter with an isolated lease pool and the process key
    /// advertised for challenge-bound direct probes.
    pub fn new(
        bucket: Bucket,
        lease_bucket: Bucket,
        node: String,
        probe_public_key: String,
    ) -> Self {
        Self {
            bucket,
            lease_bucket,
            node,
            probe_public_key,
            live: Arc::new(LiveLoad::default()),
            lease_ttl_ms: 0,
            fleet_sample_ms: 5_000,
            #[cfg(all(test, celld_internal_tests))]
            ambient_load_override: None,
            #[cfg(all(test, celld_internal_tests))]
            production_load_samples: AtomicUsize::new(0),
            own_log: std::sync::Mutex::new((0, None)),
            applied: tokio::sync::watch::channel((String::new(), 0)).0,
        }
    }

    /// The lease lifetime this fleet renews on, used to decide which node
    /// records are still worth reading.
    pub fn with_fleet_sample_ms(mut self, sample_ms: u64) -> Self {
        self.fleet_sample_ms = sample_ms;
        self
    }

    pub fn with_lease_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.lease_ttl_ms = ttl_ms;
        self
    }

    #[cfg(all(test, celld_internal_tests))]
    pub(crate) fn with_ambient_load_for_test(mut self, load: AmbientLoadSample) -> Self {
        self.ambient_load_override = Some(load);
        self
    }

    #[cfg(all(test, celld_internal_tests))]
    pub(crate) fn load_sample_for_test(&self) -> NodeLoadWire {
        self.process_load()
    }

    #[cfg(all(test, celld_internal_tests))]
    pub(crate) fn production_load_samples_for_test(&self) -> usize {
        self.production_load_samples.load(Ordering::Relaxed)
    }

    pub fn lease_ttl_ms(&self) -> u64 {
        self.lease_ttl_ms
    }

    /// The store's own transport, shared with the log tier. A fresh client
    /// costs tens of milliseconds of rustls setup at boot, and boot speed is
    /// load-bearing: the clean-reload resume window is the predecessor's
    /// remaining lease TTL, and two extra client constructions can consume
    /// enough of a short lease to prevent a clean reload.
    pub fn bucket_client(&self) -> Bucket {
        self.bucket.clone()
    }

    /// The counters this node publishes to its peers.
    pub fn live(&self) -> Arc<LiveLoad> {
        self.live.clone()
    }

    /// The storage scheme this adapter coordinates through (`s3` or `gs`),
    /// for the startup banner.
    pub fn storage_scheme(&self) -> &'static str {
        self.bucket.scheme()
    }

    /// Stable identity for this exact lease-writing process.
    pub fn process_generation(&self) -> Option<&str> {
        (!self.probe_public_key.is_empty()).then_some(self.probe_public_key.as_str())
    }

    pub async fn read_owner(&self, cell: &str) -> anyhow::Result<Option<OwnerRecord>> {
        let key = format!("cells/{cell}/own.json");
        let Some((owner, etag)) = load_json::<OwnerWireOwned>(&self.bucket, &key).await? else {
            return Ok(None);
        };
        let record = OwnerRecord {
            node: (!owner.node.is_empty()).then_some(owner.node),
            epoch: owner.epoch,
            etag,
        };
        Ok(Some(record))
    }

    pub async fn read_node_lease(&self, owner: &str) -> anyhow::Result<Option<NodeLeaseRecord>> {
        load_node_lease(&self.bucket, owner).await
    }

    /// Read this process's authority record through the isolated lease pool.
    pub async fn read_self_node_lease(
        &self,
        owner: &str,
    ) -> anyhow::Result<Option<NodeLeaseRecord>> {
        let started_us = lease_request_started_us();
        let result = load_node_lease(&self.lease_bucket, owner).await;
        log_lease_request(
            "node_lease_read",
            format_args!("nodes/{owner}.json"),
            self.lease_bucket.scheme(),
            started_us,
            match &result {
                // The generation is the field that separates "our own record
                // came back" from "another writer's record came back", which
                // is the distinction a fence on a readback turns on.
                Ok(Some(record)) => LeaseRequestOutcome::Found {
                    generation: &record.generation,
                },
                Ok(None) => LeaseRequestOutcome::Missing,
                Err(error) => LeaseRequestOutcome::Error(error),
            },
        );
        result
    }

    /// Enumerate the fleet membership records used for advisory placement.
    /// The adapter owns pagination and bounded I/O concurrency; the core gets
    /// every decoded observation and owns all filtering and selection policy.
    pub async fn read_capacity_peers(&self) -> anyhow::Result<Vec<CapacityPeer>> {
        Ok(self
            .read_capacity_leases()
            .await?
            .into_iter()
            .map(NodeLeaseWire::into_capacity_peer)
            .collect())
    }

    async fn read_capacity_leases(&self) -> anyhow::Result<Vec<NodeLeaseWire>> {
        self.read_capacity_lease_bodies()
            .await?
            .iter()
            .map(|body| {
                serde_json::from_str(body.get()).with_context(|| {
                    format!(
                        "decode {}://{}/nodes/",
                        self.bucket.scheme(),
                        self.bucket.name
                    )
                })
            })
            .collect()
    }

    /// Every live lease record's body, checked to be JSON and nothing more.
    async fn read_capacity_lease_bodies(&self) -> anyhow::Result<Vec<Box<RawValue>>> {
        const READ_CONCURRENCY: usize = 16;
        let current_ms = now_ms();
        let mut nodes = Vec::new();
        for object in self.bucket.list("nodes/").await? {
            // A record nothing has rewritten in several lease lifetimes
            // belongs to a node that is not coming back. Skipping it here
            // is the difference between reading the live fleet and
            // reading every node that has ever run: the listing is what
            // the placement decision costs, and it is paid on every
            // unowned cell.
            if !capacity_record_is_recent(
                object.last_modified.timestamp(),
                current_ms,
                self.lease_ttl_ms,
            ) {
                continue;
            }
            let Some(node) = object
                .location
                .as_ref()
                .strip_prefix("nodes/")
                .and_then(|key| key.strip_suffix(".json"))
            else {
                continue;
            };
            if !node.is_empty() {
                nodes.push(node.to_string());
            }
        }
        nodes.sort();
        nodes.dedup();

        let mut reads = stream::iter(nodes.into_iter().map(|node| async move {
            let key = format!("nodes/{node}.json");
            Ok::<_, anyhow::Error>(
                load_json::<Box<RawValue>>(&self.bucket, &key)
                    .await?
                    .map(|(lease, _)| lease),
            )
        }))
        .buffer_unordered(READ_CONCURRENCY);
        let mut peers = Vec::new();
        while let Some(peer) = reads.next().await {
            if let Some(peer) = peer? {
                peers.push(peer);
            }
        }
        Ok(peers)
    }

    /// The shared fleet sample at the configured interval, for a reader that
    /// is not the balancing tick: a placement. Each costs one GET, and a
    /// burst of them under a shed or a cap costs one GET each instead of one
    /// fleet scan each, which on a hundred nodes was a hundred GETs per
    /// queued activation at the moment the bucket was already busiest. A
    /// stale sample makes this reader the refresher, so the scan still
    /// happens once per interval whoever asks first. Follower recruitment
    /// stays on the direct read: it is bounded by eviction events, and the
    /// store operations a shared read adds inside the maintenance tick shift
    /// every scripted fault ordinal in the generative S3 worlds.
    pub async fn read_shared_capacity_view(&self) -> anyhow::Result<(u64, Vec<CapacityPeer>)> {
        self.read_shared_capacity_peers(self.fleet_sample_ms).await
    }

    /// Read the periodic fleet sample shared by balancing and format checks.
    /// One conditional claim admits a refresher; other nodes read its result.
    /// This bounds periodic lease GETs to one fleet scan instead of one scan
    /// per node. Placement, readiness and lease authority use direct reads.
    /// An unavailable sample is an error, never permission to use stale data
    /// or fall back to a fleet scan on every reader.
    ///
    /// Returns the instant the sample was taken with the peers. A caller
    /// judges lease expiry at that instant, not at its own clock: the copied
    /// leases age while the real nodes keep renewing, and nodes started
    /// together renew in lock-step, so a view a few seconds old shows every
    /// lease dead in the same second. On the 2026-09-05 fleet a reader that
    /// judged at its own clock closed its format gate on a third of its
    /// ticks and skipped balancing on them.
    pub async fn read_shared_capacity_peers(
        &self,
        sample_ms: u64,
    ) -> anyhow::Result<(u64, Vec<CapacityPeer>)> {
        anyhow::ensure!(sample_ms > 0, "the fleet sample interval must be positive");
        // Two attempts: a node that loses the claim race re-reads once, so it
        // takes the winner's previous view or result instead of reporting the
        // sample unavailable. Ticks on nodes started together stay aligned
        // for hours, so the race is the common case, not the exception: on
        // the 2026-09-05 fleet two of three nodes lost it every other tick
        // and closed their format gate each time.
        for attempt in 0..2 {
            match self.read_or_claim_capacity_sample(sample_ms).await? {
                Some(peers) => return Ok(peers),
                None if attempt == 0 => continue,
                None => anyhow::bail!("another node claimed the fleet sample refresh"),
            }
        }
        unreachable!("the attempt loop returns or bails")
    }

    /// One pass of `read_shared_capacity_peers`: `Ok(None)` when another
    /// node won the claim in the same instant.
    async fn read_or_claim_capacity_sample(
        &self,
        sample_ms: u64,
    ) -> anyhow::Result<Option<(u64, Vec<CapacityPeer>)>> {
        let current = load_json::<CapacitySampleWire>(&self.bucket, CAPACITY_SAMPLE_KEY).await?;
        let now = now_ms();
        let (token, previous) = match current {
            Some((CapacitySampleWire::Ready(sample), _))
                if sample.age_ms(now).is_some_and(|age| age < sample_ms) =>
            {
                return sample.taken().map(Some);
            }
            Some((
                CapacitySampleWire::Refreshing {
                    expires_ms,
                    previous,
                },
                _,
            )) if expires_ms > now => {
                // A tick that lands inside a refresh keeps the sample the
                // refresh replaces. Nodes started together tick in one
                // cluster, so a sample the cluster found fresh at 4.9 s sits
                // until the next cluster at 9.9 s, and the view the claim
                // then carries is already two intervals old; readers in that
                // cluster keep it while it is younger than three. Past that
                // a stalled refresher cannot keep granting format permission
                // from an old membership. On the 2026-09-05 fleet a
                // two-interval bound failed about one tick a minute on the
                // nodes that never won the claim.
                return match previous {
                    Some(sample) if sample.age_ms(now).is_some_and(|age| age < 3 * sample_ms) => {
                        sample.taken().map(Some)
                    }
                    _ => anyhow::bail!("the fleet sample is being refreshed"),
                };
            }
            Some((CapacitySampleWire::Ready(sample), token)) => (Some(token), Some(sample)),
            Some((CapacitySampleWire::Refreshing { previous, .. }, token)) => {
                (Some(token), previous)
            }
            None => (None, None),
        };
        let expires_ms = now.saturating_add(CAPACITY_REFRESH_TIMEOUT_MS);
        let claim = CapacitySampleWire::Refreshing {
            expires_ms,
            previous: previous.clone(),
        };
        let Some(claim_token) = self
            .bucket
            .put_cas(
                CAPACITY_SAMPLE_KEY,
                serde_json::to_vec(&claim)?,
                token.as_deref(),
            )
            .await?
        else {
            return Ok(None);
        };
        anyhow::ensure!(
            now_ms() < expires_ms,
            "the fleet sample refresh claim expired"
        );

        // Stamp before the listing, not after the GETs: slow I/O must not
        // turn an old membership view into a newly fresh format permission.
        let started_ms = now_ms();
        let leases = match self.read_capacity_lease_bodies().await {
            Ok(leases) => leases,
            Err(error) => {
                // Give the previous sample back under the claim. The next
                // reader then claims again at once instead of every node
                // waiting out the claim with a closed gate.
                if let Some(previous) = previous {
                    let restored = CapacitySampleWire::Ready(previous);
                    let _ = self
                        .bucket
                        .put_cas(
                            CAPACITY_SAMPLE_KEY,
                            serde_json::to_vec(&restored)?,
                            Some(&claim_token),
                        )
                        .await;
                }
                return Err(error);
            }
        };
        let sample = CapacitySample { started_ms, leases };
        // An expired refresher can finish after its successor. Matching the
        // claim token prevents it from overwriting that successor's sample.
        self.bucket
            .put_cas(
                CAPACITY_SAMPLE_KEY,
                serde_json::to_vec(&CapacitySampleWire::Ready(sample.clone()))?,
                Some(&claim_token),
            )
            .await?
            .context("the fleet sample refresh claim was replaced")?;
        let now = now_ms();
        anyhow::ensure!(
            now < expires_ms && sample.age_ms(now).is_some_and(|age| age < sample_ms),
            "the fleet sample expired during refresh",
        );
        tracing::info!(
            event = "fleet_sample_refreshed",
            leases = sample.leases.len(),
            elapsed_ms = now.saturating_sub(started_ms),
            "refreshed the shared fleet sample"
        );
        sample.taken().map(Some)
    }

    /// Publish a cell as unowned, keeping its epoch.
    ///
    /// Read-then-conditional-write, because the release is only safe against
    /// the exact record this node wrote: a takeover in the meantime means the
    /// cell is someone else's now, and blanking it would strip a live owner's
    /// claim. Rejection is an ordinary outcome, not an error -- the record
    /// keeps naming whoever it names, and nothing was lost.
    pub async fn release_owner(&self, cell: &str, epoch: u64) -> anyhow::Result<CasOutcome> {
        let Some(current) = self.read_owner(cell).await? else {
            return Ok(CasOutcome::Rejected);
        };
        if current.node.as_deref() != Some(self.node.as_str()) || current.epoch != epoch {
            return Ok(CasOutcome::Rejected);
        }
        let key = format!("cells/{cell}/own.json");
        let body = serde_json::to_vec(&OwnerWire { node: "", epoch })?;
        match self.bucket.put_cas(&key, body, Some(&current.etag)).await? {
            Some(_) => Ok(CasOutcome::Applied),
            None => Ok(CasOutcome::Rejected),
        }
    }

    pub async fn cas_owner(
        &self,
        cell: &str,
        guard: CasGuard,
        epoch: u64,
    ) -> anyhow::Result<CasOutcome> {
        let key = format!("cells/{cell}/own.json");
        let body = serde_json::to_vec(&OwnerWire {
            node: &self.node,
            epoch,
        })?;
        let etag = match &guard {
            CasGuard::Absent => None,
            CasGuard::Match(etag) => Some(etag.as_str()),
        };
        match self.bucket.put_cas(&key, body, etag).await? {
            Some(_) => Ok(CasOutcome::Applied),
            None => Ok(CasOutcome::Rejected),
        }
    }

    pub async fn cas_node_lease(
        &self,
        guard: CasGuard,
        record: &NodeLeaseRecord,
        stamped: &mut Option<celld_logic::log_tier::LogState>,
    ) -> Result<LeaseCasOutcome, LeaseCasError> {
        // Both guards below stop the attempt before the transport, so the
        // record in the store is untouched and the core needs no readback
        // to learn that.
        if self.probe_public_key.is_empty() {
            return Err(LeaseCasError::NotCommitted(anyhow::anyhow!(
                "refusing to publish a node lease without a signed-probe key"
            )));
        }
        let key = format!("nodes/{}.json", self.node);
        // Snapshot the folded log ONCE, before serialization: the applied
        // notification below reports THIS seq — the identity of the body
        // that landed — never a re-read of the slot.
        let (log_seq, log) = self.own_log.lock().unwrap().clone();
        // Report the stamp through the out-parameter, synchronously,
        // before the first await: a caller that times this future out
        // still learns what the possibly-landed body carried.
        *stamped = log_state_from_wire(&log);
        let body = serde_json::to_vec(&NodeLeaseWire {
            node: record.node.clone(),
            expires_ms: record.expires_ms,
            addr: record.addr.clone(),
            probe_public_key: self.probe_public_key.clone(),
            peer_protocol: record.peer_protocol,
            paced_handoff: true,
            // Part 2, after 2026-10-01: delete this line. Every reader since
            // part 1 falls back to `probe_public_key`, which is the same
            // value. See the field's doc on `NodeLeaseWire`.
            generation: record.generation.clone(),
            load: self.process_load(),
            // The CORE's lease writes carry the folded log through
            // UNCHANGED: the full object lives in own_log, written only by
            // the log tier's own core-mediated updates.
            log,
        })
        .map_err(|error| LeaseCasError::NotCommitted(error.into()))?;
        let etag = match &guard {
            CasGuard::Absent => None,
            CasGuard::Match(etag) => Some(etag.as_str()),
        };
        let started_us = lease_request_started_us();
        let written = self.lease_bucket.put_cas(&key, body, etag).await;
        log_lease_request(
            "node_lease_write",
            format_args!("{key}"),
            self.lease_bucket.scheme(),
            started_us,
            match &written {
                Ok(Some(_)) => LeaseRequestOutcome::Applied,
                Ok(None) => LeaseRequestOutcome::Rejected,
                Err(error) => LeaseRequestOutcome::Error(error),
            },
        );
        // `put_cas` folds a clean 412/409 into `Ok(None)`, so every failure
        // left here either carries a store answer that proves no object was
        // written, or describes a write that can have committed.
        let applied = written.map_err(|error| {
            if crate::bucket::cas_write_did_not_commit(&error) {
                LeaseCasError::NotCommitted(error)
            } else {
                LeaseCasError::Ambiguous(error)
            }
        })?;
        match applied {
            Some(etag) => {
                let _ = self.applied.send((etag.clone(), log_seq));
                Ok(LeaseCasOutcome::Applied { etag })
            }
            None => Ok(LeaseCasOutcome::Rejected),
        }
    }

    fn process_load(&self) -> NodeLoadWire {
        #[cfg(all(test, celld_internal_tests))]
        if let Some(ambient) = &self.ambient_load_override {
            let rss_bytes = ambient.rss_bytes.max(1);
            return NodeLoadWire {
                sampled_ms: now_ms(),
                owned_cells: owned_cells(&self.live),
                placement_weight: placement_weight(&self.live),
                bucket_format: Some(celld_logic::format::BUCKET_FORMAT),
                resident_cells: self.live.resident_cells.load(Ordering::Relaxed),
                host_websockets: self.live.host_websockets.load(Ordering::Relaxed),
                rss_bytes,
                in_use_bytes: Some(ambient.in_use_bytes.max(1).min(rss_bytes)),
                cgroup_working_set_bytes: ambient.cgroup_working_set_bytes,
                cgroup_current_bytes: ambient.cgroup_current_bytes,
                cpu_percent_x100: self.live.cpu_percent_x100.load(Ordering::Relaxed),
                open_fds: ambient.open_fds,
                fd_limit: ambient.fd_limit,
                pressured: self.live.pressured.load(Ordering::Relaxed),
                memory_headroom: Some(self.live.memory_headroom.load(Ordering::Relaxed)),
                shed_cells: self.live.shed_cells.load(Ordering::Relaxed),
                restoring: self.live.restoring.load(Ordering::Relaxed),
                rebalance_paused: Some(self.live.rebalance_paused.load(Ordering::Relaxed)),
                draining: Some(self.live.draining.load(Ordering::Relaxed)),
            };
        }
        #[cfg(all(test, celld_internal_tests))]
        self.production_load_samples.fetch_add(1, Ordering::Relaxed);
        process_load(&self.live)
    }
}

#[cfg(all(test, celld_internal_tests))]
mod internal_tests {
    include!(env!("CELLD_INTERNAL_OWNERSHIP_STORE_TESTS"));
}

impl BucketOwnership {
    /// Replace the folded log object the next lease write carries and
    /// return its publish seq. The caller nudges a renewal and awaits
    /// `applied_log` reaching that seq; the store never initiates writes.
    pub(crate) fn set_own_log(&self, log: Option<NodeLogWire>) -> u64 {
        let mut slot = self.own_log.lock().unwrap();
        slot.0 += 1;
        slot.1 = log;
        slot.0
    }

    pub(crate) fn own_log(&self) -> Option<NodeLogWire> {
        self.own_log.lock().unwrap().1.clone()
    }

    pub(crate) fn applied_log(&self) -> tokio::sync::watch::Receiver<(String, u64)> {
        self.applied.subscribe()
    }
}

/// The class of a completed node-lease store request.
///
/// A fence names the lease state the core observed, not the store request
/// that produced it. A removed record, a replaced record and a failed read
/// are three different operator problems, so the class has to survive to the
/// log line.
enum LeaseRequestOutcome<'a> {
    Found { generation: &'a str },
    Missing,
    Applied,
    Rejected,
    Error(&'a anyhow::Error),
}

impl LeaseRequestOutcome<'_> {
    fn class(&self) -> &'static str {
        match self {
            Self::Found { .. } => "found",
            Self::Missing => "missing",
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Error(_) => "error",
        }
    }
}

/// Sample the domain clock only when the `store` target is on.
///
/// The lease path runs a few requests per TTL, so the sample is not a
/// throughput concern. It is a determinism concern: an unconditional clock
/// read in the adapter is an execution-domain effect that the simulator would
/// have to account for on every run, including the runs that never log.
fn lease_request_started_us() -> Option<u64> {
    lease_request_logging_on().then(crate::asyncrt::mono_us)
}

fn lease_request_logging_on() -> bool {
    tracing::enabled!(target: "store", tracing::Level::DEBUG)
}

/// Report one completed node-lease store request on the opt-in `store` target.
///
/// The target is off under the default `info` filter, and
/// `RUST_LOG=celld=info,store=debug` turns it on. A dedicated target rather
/// than a `CELLD_*` variable keeps the switch on the mechanism operators
/// already use for the `timing` target, so this adds no configuration surface.
fn log_lease_request(
    event: &'static str,
    key: std::fmt::Arguments<'_>,
    scheme: &str,
    started_us: Option<u64>,
    outcome: LeaseRequestOutcome<'_>,
) {
    let Some(started_us) = started_us else {
        return;
    };
    // The full chain, because the class an operator needs is usually in a
    // source: a 403 and a connection reset both arrive as a read error.
    let error = match &outcome {
        LeaseRequestOutcome::Error(error) => Some(format!("{error:#}")),
        _ => None,
    };
    tracing::debug!(
        target: "store",
        event,
        outcome = outcome.class(),
        key = %key,
        scheme,
        duration_us = crate::asyncrt::mono_us().saturating_sub(started_us),
        generation = match &outcome {
            LeaseRequestOutcome::Found { generation } => Some(*generation),
            _ => None,
        },
        error = error.as_deref(),
        "node lease store request completed"
    );
}

pub(crate) async fn load_node_lease(
    bucket: &Bucket,
    owner: &str,
) -> anyhow::Result<Option<NodeLeaseRecord>> {
    let key = format!("nodes/{owner}.json");
    Ok(load_json::<NodeLeaseWire>(bucket, &key)
        .await?
        .map(|(lease, etag)| NodeLeaseRecord {
            log_state: log_state_from_wire(&lease.log),
            node: lease.node,
            addr: lease.addr,
            expires_ms: lease.expires_ms,
            peer_protocol: lease.peer_protocol,
            generation: lease.generation,
            etag,
        }))
}

async fn load_json<T: for<'de> Deserialize<'de>>(
    bucket: &Bucket,
    key: &str,
) -> anyhow::Result<Option<(T, String)>> {
    let Some((bytes, etag)) = bucket.get(key).await? else {
        return Ok(None);
    };
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("decode {}://{}/{key}", bucket.scheme(), bucket.name))?;
    Ok(Some((value, etag)))
}

fn owned_cells(live: &LiveLoad) -> Option<usize> {
    live.ownership_confirmed
        .load(Ordering::Relaxed)
        .then(|| live.owned_cells.load(Ordering::Relaxed))
}

/// Sample the process with the schema a node lease publishes. `/state`
/// reports the same sample, so an operator reads the numbers peers rank
/// this node by rather than a second set assembled from other sources.
fn placement_weight(live: &LiveLoad) -> Option<u64> {
    let weight = live.placement_weight.load(Ordering::Relaxed);
    (weight > 0).then_some(weight)
}

/// Sample the process with the schema a node lease publishes. `/state`
/// reports the same sample, so an operator reads the numbers peers rank
/// this node by rather than a second set assembled from other sources.
#[allow(clippy::disallowed_methods)] // `/proc` is host telemetry, not node storage.
pub(crate) fn process_load(live: &LiveLoad) -> NodeLoadWire {
    // One sample for both numbers. Reading the resident set size here and the
    // in-use figure from a value the actor wrote on its own timer would publish
    // a pair from two instants, and before the first sample it would publish a
    // real resident set size beside an in-use figure of zero -- which reads as
    // total allocator retention.
    let memory = crate::memory::sample();
    // A 1-byte floor is the sentinel a platform without /proc leaves behind.
    // Both numbers take it, so the in-use figure can never read as zero beside
    // a real resident set size.
    let rss_bytes = memory.rss_bytes.max(1);
    let in_use_bytes = memory.in_use_bytes.max(1).min(rss_bytes);

    #[cfg(target_os = "linux")]
    let open_fds = std::fs::read_dir("/proc/self/fd")
        .map(|entries| entries.count() as u64)
        .unwrap_or_default();
    #[cfg(not(target_os = "linux"))]
    let open_fds = 0;

    #[cfg(unix)]
    let fd_limit = {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
            limit.rlim_cur
        } else {
            0
        }
    };
    #[cfg(not(unix))]
    let fd_limit = 0;

    NodeLoadWire {
        sampled_ms: now_ms(),
        owned_cells: owned_cells(live),
        placement_weight: placement_weight(live),
        bucket_format: Some(celld_logic::format::BUCKET_FORMAT),
        rss_bytes,
        in_use_bytes: Some(in_use_bytes),
        cgroup_working_set_bytes: memory.cgroup_working_set_bytes,
        cgroup_current_bytes: memory.cgroup_current_bytes,
        open_fds,
        fd_limit,
        cpu_percent_x100: live.cpu_percent_x100.load(Ordering::Relaxed),
        resident_cells: live.resident_cells.load(Ordering::Relaxed),
        host_websockets: live.host_websockets.load(Ordering::Relaxed),
        pressured: live.pressured.load(Ordering::Relaxed),
        memory_headroom: Some(live.memory_headroom.load(Ordering::Relaxed)),
        shed_cells: live.shed_cells.load(Ordering::Relaxed),
        restoring: live.restoring.load(Ordering::Relaxed),
        rebalance_paused: Some(live.rebalance_paused.load(Ordering::Relaxed)),
        draining: Some(live.draining.load(Ordering::Relaxed)),
    }
}
