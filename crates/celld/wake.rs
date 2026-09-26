// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Immutable alarm discovery publications and restart-safe reclamation.
//!
//! SQLite assigns each installation a writer epoch and sequence. A foreground
//! PUT proves discovery before acknowledgment. Background work proves SQLite
//! replication and ownership before it advances a monotone retirement record.
//! Paginated, repeated LIST passes reclaim older objects, including mutations
//! whose caller stopped waiting before their remote effect.
use crate::bucket::Bucket;
use crate::ownership_store::BucketOwnership;
use celld_logic::wake::elected_hint_needed;
use celld_logic::wake::parse_entry_key;
use celld_logic::wake::WakeCore;
use celld_logic::wake::{AlarmSnapshot, PublicationId, Retirement, ENTRY_PREFIX};
use futures_util::stream::{self, StreamExt as _};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::time::Duration;
use tracing::warn;

/// Stay-resident threshold: alarms due sooner than this keep their cell
/// resident when residency is cheaper than a wake cycle. Alarms further out
/// are evicted behind an entry.
pub fn resident_ms() -> i64 {
    static MS: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *MS.get_or_init(|| {
        crate::env_vars::with_default("CELLD_ALARM_RESIDENT_MS", 3_600_000)
            .expect("validated CELLD_ALARM_RESIDENT_MS")
    })
}

/// Scan the due wake buckets: used by the boot-time orphan scan and the
/// periodic waker tick.
///
/// A delimiter listing enumerates armed minutes, then opens only due minutes.
/// Cleanup separately pages through all publications, including future ones.
/// Parsing every prefix avoids relying on provider ordering for discovery.
///
/// The returned minute bounds an Actor wake hint; SQLite supplies the actual
/// deadline and delivery authority. Malformed entries cannot reach ownership
/// or cleanup, because deleting an unclassified name could destroy live work.
pub async fn due_scan(bucket: &Bucket, now_ms: i64) -> Vec<(String, i64)> {
    /// Bounded like every other fan-out against the store: a long outage can
    /// leave one due bucket per minute of backlog, and opening them all at
    /// once is the thundering herd the eviction bound exists to prevent.
    const BUCKET_READ_CONCURRENCY: usize = 16;

    let mut due = Vec::new();
    let minutes = match bucket.common_prefixes(ENTRY_PREFIX).await {
        Ok(minutes) => minutes,
        Err(e) => {
            warn!(error = %e, "wake due scan bucket listing failed");
            return due;
        }
    };
    let overdue: Vec<String> = minutes
        .into_iter()
        .filter(|minute| {
            celld_logic::wake::parse_minute_prefix(minute).is_some_and(|ms| ms <= now_ms)
        })
        .collect();

    let mut listings = futures_util::stream::iter(
        overdue
            .into_iter()
            .map(|minute| async move { (bucket.list(&minute).await, minute) }),
    )
    .buffer_unordered(BUCKET_READ_CONCURRENCY);
    while let Some((objects, minute)) = listings.next().await {
        let objects = match objects {
            Ok(objects) => objects,
            Err(e) => {
                warn!(error = %e, %minute, "wake due scan entry listing failed");
                continue;
            }
        };
        for object in objects {
            if let Some(entry) = parse_entry_key(object.location.as_ref()) {
                if entry.minute_ms <= now_ms {
                    due.push((entry.cell, entry.minute_ms));
                }
            }
        }
    }
    due.sort();
    due.dedup_by(|a, b| a.0 == b.0);
    due
}

/// Owner records the elected waker reads at once while it sorts the fleet's
/// due entries. These reads are deliberately outside the activation queue:
/// they hold no permit, so the node's own alarm wakes and cold requests never
/// queue behind the fleet's due set, and 32 in flight clears a thousand
/// entries in about a second of store latency.
const ELECTED_PROBE_CONCURRENCY: usize = 32;

/// The elected waker's pass over the due entries: the ones this node must
/// hint with `WakeHintScope::Fleet`.
///
/// One owner read per entry, without an activation permit, decides through
/// `elected_hint_needed` against the node leases the same tick's dead-node
/// scan read. Before this pass existed, every due entry in the fleet went
/// through the core's route on every node: an owner read under a permit,
/// then `Remote`, for cells whose live owners were about to fire the alarm
/// themselves. A read that fails keeps its entry, because the core's own
/// resolution is the authority and an entry dropped here on a transient
/// error would wait a whole tick.
pub async fn elected_due(
    ownership: &BucketOwnership,
    node: &str,
    live: &BTreeSet<String>,
    due: Vec<(String, i64)>,
) -> Vec<(String, i64)> {
    probe_entries(ownership, node, live, true, due).await
}

/// Keep a persisted local owner even when this process has lost its in-memory
/// cell. Only the elected scanner resolves dead or unknown foreign owners.
/// An Owned hint alone cannot recover an unknown cell after a same-name restart.
pub(crate) async fn probe_entries(
    ownership: &BucketOwnership,
    node: &str,
    live: &BTreeSet<String>,
    elected: bool,
    entries: Vec<(String, i64)>,
) -> Vec<(String, i64)> {
    let probed: Vec<(
        String,
        i64,
        anyhow::Result<Option<celld_logic::OwnerRecord>>,
    )> = stream::iter(entries)
        .map(|(cell, entry_ms)| async move {
            let owner = ownership.read_owner(&cell).await;
            (cell, entry_ms, owner)
        })
        .buffer_unordered(ELECTED_PROBE_CONCURRENCY)
        .collect()
        .await;
    let mut failed = 0usize;
    let mut hinted: Vec<(String, i64)> = probed
        .into_iter()
        .filter_map(|(cell, entry_ms, owner)| match owner {
            Err(_) => {
                failed += 1;
                elected.then_some((cell, entry_ms))
            }
            Ok(record) => {
                let owner = record.as_ref().and_then(|record| record.node.as_deref());
                let owner_live = owner.is_some_and(|owner| live.contains(owner));
                (owner == Some(node) || (elected && elected_hint_needed(owner, node, owner_live)))
                    .then_some((cell, entry_ms))
            }
        })
        .collect();
    if failed > 0 {
        warn!(
            failed,
            elected, "wake probe owner reads failed; only the elected scanner hints those entries"
        );
    }
    hinted.sort();
    hinted
}

/// Run the periodic wake scan after node authority and the boot scan complete.
/// The interval, election, store reads, and Actor hints stay in one path so a
/// caller cannot run a scan without delivering the entries it discovers.
pub async fn run_periodic(
    bucket: Bucket,
    ownership: std::sync::Arc<BucketOwnership>,
    node: String,
    actor: tokio::sync::mpsc::UnboundedSender<crate::actor::Message>,
    tick_ms: u64,
) {
    tokio::join!(
        run_due_scans(
            bucket.clone(),
            ownership.clone(),
            node.clone(),
            actor.clone(),
            tick_ms
        ),
        run_cleanup(bucket, ownership, node, actor, tick_ms),
    );
}

async fn run_cleanup(
    bucket: Bucket,
    ownership: std::sync::Arc<BucketOwnership>,
    node: String,
    actor: tokio::sync::mpsc::UnboundedSender<crate::actor::Message>,
    tick_ms: u64,
) {
    let mut tick = crate::asyncrt::interval(Duration::from_millis(tick_ms));
    tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
    let mut collector = Collector::default();
    tick.tick().await;
    loop {
        tick.tick().await;
        let refresh = match collector.pass(&bucket).await {
            Ok(refresh) => refresh,
            Err(error) => {
                warn!(%error, "wake cleanup page failed");
                continue;
            }
        };
        let now_ms = crate::asyncrt::wall_ms();
        let elected = try_hold_waker(
            &bucket,
            &node,
            now_ms,
            tick_ms.saturating_mul(3).min(i64::MAX as u64) as i64,
        )
        .await;
        let live = if elected {
            crate::dead_node_gc::live_nodes(&bucket, now_ms.max(0) as u64).await
        } else {
            BTreeSet::new()
        };
        let refresh = probe_entries(&ownership, &node, &live, elected, refresh).await;
        for (cell, entry_ms) in refresh {
            if actor
                .send(crate::actor::Message::MaintainWake { cell: cell.clone() })
                .is_err()
            {
                return;
            }
            if actor
                .send(crate::actor::Message::WakeHint {
                    cell,
                    entry_ms,
                    // The probe excludes live foreign owners before admission.
                    // Fleet also admits a persisted local owner absent from RAM.
                    scope: celld_logic::WakeHintScope::Fleet,
                })
                .is_err()
            {
                return;
            }
        }
        if actor.is_closed() {
            return;
        }
    }
}

async fn run_due_scans(
    bucket: Bucket,
    ownership: std::sync::Arc<BucketOwnership>,
    node: String,
    actor: tokio::sync::mpsc::UnboundedSender<crate::actor::Message>,
    tick_ms: u64,
) {
    let mut tick = crate::asyncrt::interval(Duration::from_millis(tick_ms));
    tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut dead_node_gc = crate::dead_node_gc::DeadNodeGc::default();
    loop {
        tick.tick().await;
        let elected = dead_node_gc.run_elected_pass(&bucket, &node, tick_ms).await;
        let due = due_scan(&bucket, crate::asyncrt::wall_ms()).await;
        let (scope, due) = match elected {
            Some(live) => (
                celld_logic::WakeHintScope::Fleet,
                elected_due(&ownership, &node, &live, due).await,
            ),
            None => (celld_logic::WakeHintScope::Owned, due),
        };
        for (cell, entry_ms) in due {
            if actor
                .send(crate::actor::Message::WakeHint {
                    cell,
                    entry_ms,
                    scope,
                })
                .is_err()
            {
                return;
            }
        }
    }
}

#[derive(serde::Deserialize)]
struct WakerRoleLease {
    node: String,
    expires_ms: i64,
}

async fn same_node_still_holds_waker(bucket: &Bucket, node: &str) -> bool {
    let Ok(Some((bytes, _))) = bucket.get("wake/waker.json").await else {
        return false;
    };
    let Ok(lease) = serde_json::from_slice::<WakerRoleLease>(&bytes) else {
        return false;
    };
    lease.node == node && lease.expires_ms > crate::asyncrt::wall_ms()
}

/// Advisory waker-role lease: one holder per fleet to avoid N nodes polling.
/// Correctness never depends on it — concurrent wakers race activation CAS
/// harmlessly — so a caller skips the tick when another node holds the role.
pub async fn try_hold_waker(bucket: &Bucket, node: &str, now_ms: i64, ttl_ms: i64) -> bool {
    const KEY: &str = "wake/waker.json";
    let body =
        |expires: i64| format!("{{\"node\":{node:?},\"expires_ms\":{expires}}}").into_bytes();
    let token = match bucket.get(KEY).await {
        // absent (or unreadable): claim if absent
        Ok(None) | Err(_) => None,
        Ok(Some((bytes, etag))) => {
            let lease = serde_json::from_slice::<WakerRoleLease>(&bytes).ok();
            let held_by_us = lease.as_ref().is_some_and(|lease| lease.node == node);
            let expires = lease.map_or(0, |lease| lease.expires_ms);
            if !celld_logic::wake::waker_may_claim(held_by_us, expires, now_ms) {
                return false;
            }
            Some(etag)
        }
    };
    match bucket
        .put_cas(KEY, body(now_ms + ttl_ms), token.as_deref())
        .await
    {
        Ok(Some(_)) => true,
        // The cleanup loop and the due-scan loop renew the same node's role.
        // A clean CAS conflict can be their overlap, so check the winner
        // before cancelling a GC pass and repeating its fleet-wide LIST.
        Ok(None) => same_node_still_holds_waker(bucket, node).await,
        Err(_) => false,
    }
}

/// Per-node publication and retirement caches. Durable inventory belongs to
/// LIST; an error or cancellation leaves no reservation for a later task.
#[derive(Default)]
pub struct WakeFlusher {
    pub core: Mutex<WakeCore>,
    settled: Mutex<BTreeMap<String, PublicationId>>,
    #[cfg(all(test, celld_internal_tests))]
    reuse_identity: std::sync::atomic::AtomicBool,
}

/// The alarm source and its replication proof belong to the same host.
/// Keeping this boundary together prevents cleanup from proving a position
/// through a replacement runtime that did not produce the snapshot.
pub(crate) trait AlarmHost: Sync {
    fn alarm_snapshot(&self, cell: &str) -> anyhow::Result<AlarmSnapshot>;
    fn prove_alarm<'a>(
        &'a self,
        cell: &'a str,
        epoch: u64,
        position: u64,
    ) -> impl std::future::Future<Output = anyhow::Result<(u64, celld_logic::ProofSource)>> + Send + 'a;
}

impl AlarmHost for crate::cell_host::CellHost {
    fn alarm_snapshot(&self, cell: &str) -> anyhow::Result<AlarmSnapshot> {
        Ok(self.with_alarm(cell, |alarm, _| alarm))
    }

    fn prove_alarm<'a>(
        &'a self,
        cell: &'a str,
        epoch: u64,
        position: u64,
    ) -> impl std::future::Future<Output = anyhow::Result<(u64, celld_logic::ProofSource)>> + Send + 'a
    {
        self.await_durable(cell, epoch, position)
    }
}

impl WakeFlusher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish exactly the identity captured at the committed source. An old
    /// PUT can finish after retirement, but can only recreate an obsolete key.
    pub(crate) async fn publish(
        &self,
        bucket: &Bucket,
        cell: &str,
        alarm: AlarmSnapshot,
    ) -> anyhow::Result<()> {
        if alarm.at_ms().is_none() {
            return Ok(());
        }
        anyhow::ensure!(
            alarm.source().is_some_and(|source| source.id.epoch > 0)
                && alarm.at_ms().is_some_and(|at| at >= 0),
            "armed alarm has no committed wake identity"
        );
        let publication = self.core.lock().unwrap().publication(cell, alarm);
        let Some(publication) = publication else {
            return Ok(());
        };
        #[cfg(all(test, celld_internal_tests))]
        let publication = if self
            .reuse_identity
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            celld_logic::wake::Publication {
                key: celld_logic::wake::entry_key(
                    alarm.at_ms().unwrap(),
                    cell,
                    PublicationId {
                        epoch: alarm.source().unwrap().id.epoch,
                        sequence: 1,
                    },
                ),
                alarm,
            }
        } else {
            publication
        };
        let body = serde_json::to_vec(&serde_json::json!({
            "format": 2, "cell": cell, "due_ms": alarm.at_ms(),
            "identity": alarm.source().unwrap().id.to_string(),
        }))?;
        bucket.put(&publication.key, body).await?;
        self.core.lock().unwrap().confirm(cell, alarm);
        Ok(())
    }

    /// Bind the durability wait to the original writer and source snapshot.
    /// The equality check rejects queued observations from a replaced runtime.
    /// A subsequent arm is safe: this certificate cannot retire its larger ID.
    pub(crate) async fn maintain(
        &self,
        bucket: &Bucket,
        cell: &str,
        alarm: AlarmSnapshot,
        host: &impl AlarmHost,
        ownership: &crate::actor::Ownership,
        node: &str,
    ) -> anyhow::Result<()> {
        let Some(source) = alarm.source() else {
            tracing::debug!(%cell, "wake maintenance has no resident source; awaiting discovery");
            return Ok(());
        };
        if self
            .settled
            .lock()
            .unwrap()
            .get(cell)
            .is_some_and(|id| *id >= source.id)
        {
            return Ok(());
        }
        if !host.alarm_snapshot(cell)?.same_installation(alarm) {
            return Ok(());
        }
        self.publish(bucket, cell, alarm).await?;
        let (durable, _) = host
            .prove_alarm(cell, source.id.epoch, source.position)
            .await?;
        anyhow::ensure!(durable >= source.position, "wake retirement proof is short");
        let owner = ownership
            .read_owner(cell)
            .await
            .map_err(|_| anyhow::anyhow!("wake retirement ownership read failed"))?;
        anyhow::ensure!(
            owner.is_some_and(|owner| {
                owner.epoch == source.id.epoch && owner.node.as_deref() == Some(node)
            }),
            "wake retirement lost writer authority"
        );
        let proof = RetirementProof(Retirement {
            id: source.id,
            at_ms: alarm.at_ms(),
        });
        let previous = publish_retirement(bucket, cell, &proof).await?;
        // Normal churn must not wait behind the fleet's bounded LIST cursor.
        // The same durable proof authorizes these exact immutable names. This
        // advisory batch can be lost or cancelled without blocking later arms.
        let mut obsolete: BTreeSet<_> = self
            .core
            .lock()
            .unwrap()
            .take_retired(cell, proof.0)
            .into_iter()
            .collect();
        if let Some(previous) = previous {
            if let Some(at_ms) = previous.at_ms.filter(|_| proof.0.retires(previous.id)) {
                obsolete.insert(celld_logic::wake::entry_key(at_ms, cell, previous.id));
            }
        }
        for key in obsolete {
            if let Err(error) = bucket.delete(&key).await {
                warn!(%cell, %key, %error, "wake eager entry delete failed; collector will retry");
            }
        }
        let mut settled = self.settled.lock().unwrap();
        let id = settled.entry(cell.to_string()).or_insert(source.id);
        *id = (*id).max(source.id);
        Ok(())
    }

    pub fn covered(&self, cell: &str, alarm: AlarmSnapshot) -> bool {
        self.core.lock().unwrap().covered(cell, alarm)
    }

    pub fn forget(&self, cell: &str) {
        self.core.lock().unwrap().forget(cell);
        self.settled.lock().unwrap().remove(cell);
    }

    // Reuse a retired name at the actual PUT boundary, so the independent
    // store observer must catch a later DELETE destroying acknowledged work.
    #[cfg(all(test, celld_internal_tests))]
    pub(crate) fn reuse_publication_identity_for_tooth(&self) {
        self.reuse_identity
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

// Only `maintain` constructs this after all three remote proofs. A bare
// boolean from an alarm notification cannot authorize destructive cleanup.
struct RetirementProof(Retirement);

fn retirement_key(cell: &str) -> String {
    format!("wake/retired/{cell}.json")
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RetirementRecord {
    format: u8,
    identity: String,
    at_ms: Option<i64>,
}

impl RetirementRecord {
    fn decode(bytes: &[u8]) -> anyhow::Result<Retirement> {
        let record: Self = serde_json::from_slice(bytes)?;
        anyhow::ensure!(record.format == 2, "unsupported wake retirement format");
        let id = PublicationId::parse(&record.identity)
            .ok_or_else(|| anyhow::anyhow!("invalid wake retirement identity"))?;
        anyhow::ensure!(
            id.epoch > 0 && record.at_ms.is_none_or(|at| at >= 0),
            "invalid wake retirement record"
        );
        Ok(Retirement {
            id,
            at_ms: record.at_ms,
        })
    }
}

async fn publish_retirement(
    bucket: &Bucket,
    cell: &str,
    proof: &RetirementProof,
) -> anyhow::Result<Option<Retirement>> {
    let retirement = proof.0;
    let key = retirement_key(cell);
    let body = serde_json::to_vec(&RetirementRecord {
        format: 2,
        identity: retirement.id.to_string(),
        at_ms: retirement.at_ms,
    })?;
    // Each retry retains the same source identity. An ambiguous CAS can have
    // applied; reading it back is sufficient, without reassigning its token
    // to current intent or falling back to an unconditional overwrite.
    for _ in 0..3 {
        let observed = bucket.get(&key).await?;
        let previous = observed
            .as_ref()
            .map(|(bytes, _)| RetirementRecord::decode(bytes))
            .transpose()?;
        if let Some(old) = previous {
            if old.id == retirement.id {
                anyhow::ensure!(
                    old == retirement,
                    "wake identity reused for a different alarm"
                );
                return Ok(previous);
            }
            if old.id > retirement.id {
                return Ok(previous);
            }
        }
        let token = observed.as_ref().map(|(_, token)| token.as_str());
        if bucket.put_cas(&key, body.clone(), token).await?.is_some() {
            return Ok(previous);
        }
    }
    anyhow::bail!("wake retirement CAS remained contended")
}

/// One bounded pass through the provider's opaque LIST continuation token.
/// Keep the next token only on success, and wrap after the last page: a late
/// PUT can appear behind any cursor, even after a successful DELETE.
#[derive(Default)]
pub(crate) struct Collector {
    cursor: Option<String>,
}

impl Collector {
    pub(crate) async fn pass(&mut self, bucket: &Bucket) -> anyhow::Result<Vec<(String, i64)>> {
        const PAGE_SIZE: usize = 128;
        const CONCURRENCY: usize = 8;
        let now_ms = crate::asyncrt::wall_ms();
        let page = match bucket
            .objects_page(ENTRY_PREFIX, self.cursor.clone(), PAGE_SIZE)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                // Provider tokens can expire. Retrying the same invalid token
                // forever would make a finite interruption strand all cleanup.
                self.cursor = None;
                return Err(error);
            }
        };
        let mut cells: BTreeMap<String, Vec<(String, celld_logic::wake::ListedEntry)>> =
            BTreeMap::new();
        for object in page.objects {
            let key = object.location.to_string();
            if let Some(entry) = parse_entry_key(&key) {
                cells
                    .entry(entry.cell.clone())
                    .or_default()
                    .push((key, entry));
            }
        }
        let mut pending = stream::iter(cells)
            .map(|(cell, entries)| async move {
                let record = match bucket.get(&retirement_key(&cell)).await {
                    Ok(Some((bytes, _))) => match RetirementRecord::decode(&bytes) {
                        Ok(record) => Some(record),
                        Err(error) => {
                            warn!(%cell, %error, "wake retirement record rejected");
                            return None;
                        }
                    },
                    Ok(None) => None,
                    Err(error) => {
                        warn!(%cell, %error, "wake retirement read failed");
                        return None;
                    }
                };
                let mut refresh = None;
                for (key, entry) in entries {
                    if record.is_some_and(|record| record.retires(entry.id)) {
                        // No mutation completion changes eligibility. Failure,
                        // timeout, or task cancellation is retried by another LIST.
                        if let Err(error) = bucket.delete(&key).await {
                            warn!(%cell, %key, %error, "wake obsolete entry delete failed");
                        }
                    } else if entry.minute_ms <= now_ms {
                        // Even a matching certificate can trail an unreported
                        // consume. A bounded truth refresh also repairs that cut
                        // after the former owner and its callback inventory vanish.
                        // Future entries wait until their minute: refreshing them
                        // now would reactivate every sleeping cell on each sweep.
                        refresh = Some(refresh.unwrap_or(entry.minute_ms).min(entry.minute_ms));
                    }
                }
                refresh.map(|minute| (cell, minute))
            })
            .buffer_unordered(CONCURRENCY);
        let mut refresh = Vec::new();
        while let Some(cell) = pending.next().await {
            if let Some(cell) = cell {
                refresh.push(cell);
            }
        }
        self.cursor = page.page_token;
        Ok(refresh)
    }
}
