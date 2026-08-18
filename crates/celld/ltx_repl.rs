// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! In-process replication backend built on `celld-ltx`.
//!
//! One shared `object_store` client for the whole node, and a managed
//! `celld_ltx::Db` per resident cell that captures the cell's committed WAL
//! and uploads it on demand. No external process, no directory-watch lag — a
//! just-written cell is registered the instant it activates, so the output
//! gate can prove a fresh cell durable with no cold-start window.
//!
//! The object layout is `cells/<cell>/ltx/e<epoch>/` in the bucket, mirroring
//! the local `<watch>/<cell>/ltx/e<epoch>/db.sqlite` tree. This backend builds
//! its own object-store clients rather than going through `bucket::Bucket`, so
//! it carries the fleet's key prefix itself: without that, two fleets sharing
//! one bucket would replicate over each other.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use celld_ltx::object_store::ObjectStore;
use celld_ltx::replica;
use celld_ltx::replica_compactor::ReplicaCompactor;
use celld_ltx::Db;
use celld_ltx::ObjectStoreClient;
use celld_ltx::ObjectStoreConfig;
use celld_ltx::Replica;
use celld_ltx::TXID;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tracing::info;
use tracing::warn;

use crate::replication::prune_watch;
use crate::replication::sqlite_snapshot;
use crate::replication::ActivationOptions;
use crate::replication::ActivationResult;
use crate::replication::RestoredSnapshot;
use crate::replication::StorageCredentials;
use crate::replication::SyncWait;

/// Max cells uploading concurrently across the node. Caps blocking-pool threads
/// and in-flight object-store requests under high write fan-out.
const SYNC_CONCURRENCY: usize = 64;

/// Max LTX object downloads across every restore on this node. A hot cell can
/// contain thousands of L0 files, so serial reads turn a takeover into minutes
/// of terminal failures. This shared ceiling hides round-trip latency without
/// multiplying the bound by the activation count.
const RESTORE_DOWNLOAD_CONCURRENCY: usize = 64;

/// One attempt consumes at most this many source objects. This bound keeps a
/// first compaction of an old, write-hot cell from reading its complete L0
/// history into memory.
const COMPACTION_MAX_FILES: usize = 256;

/// The current `ReplicaClient` interface buffers objects, so bound the complete
/// input set until the client gains a streaming read and write surface.
const COMPACTION_MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
struct CompactionConfig {
    min_txids: u64,
    concurrency: usize,
}

struct CellCompaction {
    cell: String,
    epoch: u64,
    client: ObjectStoreClient,
    local_path: PathBuf,
    queue: mpsc::UnboundedSender<CompactionWork>,
    min_txids: u64,
    compacted_txid: AtomicU64,
    queued: AtomicBool,
    cancelled: AtomicBool,
    cancel: Notify,
}

struct CompactionWork {
    cell: Weak<Cell>,
    queued_at: Instant,
}

/// One resident cell's replication state: the `celld_ltx::Db` shadowing its WAL
/// (behind a `std::sync::Mutex` because the `rusqlite` handle is `!Sync` and
/// must never cross an `.await`, so every capture+upload runs inside a
/// `spawn_blocking` closure) plus the durability tickets the output gate waits
/// on. `req_seq` counts durability requests; `synced_seq` is the highest ticket
/// a completed background sync captured. A write waits for `synced_seq >= its
/// ticket`, so concurrent writes to one cell ride a single batched upload —
/// and, because a sync credits only tickets whose writes committed before it
/// started (which the sync's `db.sync` captures), never one it did not upload.
struct Cell {
    replica: Mutex<Replica<ObjectStoreClient>>,
    req_seq: AtomicU64,
    synced_seq: AtomicU64,
    durable_txid: AtomicU64,
    /// Set while a sync for this cell is in flight, so the loop never runs two
    /// at once for one cell (they would serialize on the mutex and waste work).
    syncing: AtomicBool,
    /// Notified when `synced_seq` advances (or a sync fails), waking waiters.
    ready: Notify,
    compaction: Option<CellCompaction>,
}
type CellHandle = Arc<Cell>;

pub struct LtxRepl {
    /// Local root: cell dbs live at `watch/<cell>/ltx/e<epoch>/db.sqlite`.
    watch: PathBuf,
    bucket: String,
    /// The bucket spec's key prefix: empty, or slash-terminated.
    prefix: String,
    endpoint: Option<String>,
    region: String,
    credentials: Option<StorageCredentials>,
    /// One connection pool for the whole node, shared by every cell client.
    store: Arc<dyn ObjectStore>,
    cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>>,
    /// Woken when a cell's `committed` advances, so the background loop syncs
    /// without polling; a slow tick backstops any missed notification.
    dirty: Arc<Notify>,
    /// Shared by every activation, so the restore bound is per node, not per
    /// cell. The generic LTX restore keeps its sequential compatibility path.
    restore_slots: Arc<Semaphore>,
    compaction_queue: Option<mpsc::UnboundedSender<CompactionWork>>,
    compaction_min_txids: u64,
}

impl LtxRepl {
    /// Private-corpus constructor over an injected store, so the epoch-seal
    /// protocol runs against an in-memory bucket instead of S3.
    #[cfg(all(test, celld_internal_tests))]
    pub fn start_with_store_for_test(watch: &Path, store: Arc<dyn ObjectStore>) -> Self {
        let cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>> = Arc::default();
        let dirty = Arc::new(Notify::new());
        let slots = Arc::new(Semaphore::new(SYNC_CONCURRENCY));
        tokio::spawn(sync_loop(cells.clone(), dirty.clone(), slots));
        Self {
            watch: watch.to_path_buf(),
            bucket: "test".into(),
            prefix: String::new(),
            endpoint: None,
            region: "auto".into(),
            credentials: None,
            store,
            cells,
            dirty,
            restore_slots: Arc::new(Semaphore::new(RESTORE_DOWNLOAD_CONCURRENCY)),
            compaction_queue: None,
            compaction_min_txids: 0,
        }
    }

    /// Private-corpus constructor with additive L1 compaction enabled.
    #[cfg(all(test, celld_internal_tests))]
    pub fn start_with_compaction_for_test(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        min_txids: u64,
        concurrency: usize,
    ) -> Self {
        let cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>> = Arc::default();
        let dirty = Arc::new(Notify::new());
        let sync_slots = Arc::new(Semaphore::new(SYNC_CONCURRENCY));
        tokio::spawn(sync_loop(cells.clone(), dirty.clone(), sync_slots));
        let config = CompactionConfig {
            min_txids,
            concurrency,
        };
        let queue = start_compaction_loop(config);
        Self {
            watch: watch.to_path_buf(),
            bucket: "test".into(),
            prefix: String::new(),
            endpoint: None,
            region: "auto".into(),
            credentials: None,
            store,
            cells,
            dirty,
            restore_slots: Arc::new(Semaphore::new(RESTORE_DOWNLOAD_CONCURRENCY)),
            compaction_queue: Some(queue),
            compaction_min_txids: min_txids,
        }
    }

    pub(crate) fn start(
        watch: &Path,
        backend: crate::bucket::StorageBackend,
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: String,
        credentials: Option<StorageCredentials>,
    ) -> anyhow::Result<Self> {
        let compaction = compaction_config_from_env()?;
        // Everything downstream of the store is backend-agnostic already,
        // so the dialect decides construction and nothing else.
        let store = match backend {
            crate::bucket::StorageBackend::Gcs => crate::bucket::gcs_replica_store(&bucket)?,
            crate::bucket::StorageBackend::S3 => {
                node_config(&bucket, endpoint.as_deref(), &region, credentials.as_ref())
                    .build_store()
                    .map_err(|error| anyhow!("build shared object store: {error}"))?
            }
        };
        let cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>> = Arc::default();
        let dirty = Arc::new(Notify::new());
        // Bound how many cells upload at once so one slow cell cannot stall the
        // others and a thousand hot cells cannot open a thousand uploads.
        let slots = Arc::new(Semaphore::new(SYNC_CONCURRENCY));
        tokio::spawn(sync_loop(cells.clone(), dirty.clone(), slots));
        let compaction_queue = compaction.map(start_compaction_loop);
        Ok(Self {
            watch: watch.to_path_buf(),
            bucket,
            prefix,
            endpoint,
            region,
            credentials,
            store,
            cells,
            dirty,
            restore_slots: Arc::new(Semaphore::new(RESTORE_DOWNLOAD_CONCURRENCY)),
            compaction_queue,
            compaction_min_txids: compaction.map_or(0, |config| config.min_txids),
        })
    }

    fn db_path(&self, cell: &str, epoch: u64) -> PathBuf {
        self.watch
            .join(cell)
            .join("ltx")
            .join(format!("e{epoch}"))
            .join("db.sqlite")
    }

    /// A per-cell client over the shared store, keyed to the cell's epoch
    /// prefix. `cells/<cell>/ltx/e<epoch>` matches [`Self::db_path`]'s remote
    /// twin so the same coordinates address local and replica state.
    fn client_for(&self, cell: &str, epoch: u64) -> ObjectStoreClient {
        let mut config = node_config(
            &self.bucket,
            self.endpoint.as_deref(),
            &self.region,
            self.credentials.as_ref(),
        );
        config.path = format!("{}cells/{cell}/ltx/e{epoch}", self.prefix);
        ObjectStoreClient::with_store(config, self.store.clone())
    }

    /// Highest epoch under `cells/<cell>/ltx/` that holds any LTX — the newest
    /// durable copy to restore on takeover.
    async fn highest_nonempty_epoch(&self, cell: &str) -> anyhow::Result<Option<u64>> {
        use celld_ltx::object_store::path::Path as ObjPath;
        let base = ObjPath::from(format!("{}cells/{cell}/ltx", self.prefix));
        let listing = self.store.list_with_delimiter(Some(&base)).await?;
        let mut best: Option<u64> = None;
        for prefix in listing.common_prefixes {
            if let Some(epoch) = prefix
                .filename()
                .and_then(|name| name.strip_prefix('e'))
                .and_then(|value| value.parse::<u64>().ok())
            {
                best = Some(best.map_or(epoch, |current| current.max(epoch)));
            }
        }
        Ok(best)
    }

    /// The seal for a restore-source epoch: a file *beside* the epoch
    /// directories (`cells/<cell>/ltx/e<from>.seal.json`), so nothing that
    /// lists an epoch's LTX contents parses it, and `highest_nonempty_epoch`'s
    /// prefix listing never mistakes it for an epoch.
    fn seal_key(&self, cell: &str, epoch: u64) -> String {
        format!("{}cells/{cell}/ltx/e{epoch}.seal.json", self.prefix)
    }

    /// Read the source epoch's seal, writing it first if this activation is
    /// the first to restore from `from` (Cellarium §5.6).
    ///
    /// Restore's rule is "highest non-empty epoch", so if a takeover's new
    /// epoch stays empty (it dies before its first sync) while the fenced old
    /// owner keeps uploading into the old prefix — its PUTs are unconditional
    /// and the bucket checks nothing — the *next* activation restores the old prefix
    /// again, now including the zombie's tail: writes no client was ever
    /// acknowledged (the ownership re-read refuses the zombie's acks), and
    /// possibly writes a client was told had definitively failed, resurrect.
    ///
    /// The seal closes this by fixing, before the first restore reads a byte,
    /// the TXID every restore of this epoch may read through. Ordering makes
    /// it safe for acknowledged writes: an ack requires the ownership record
    /// to still name the writer, the takeover's record CAS precedes this seal,
    /// and the acked write's upload preceded its ownership read — so every
    /// acked write is at or below the sealed TXID. Conversely a fenced owner's
    /// later uploads land above the seal and are never restored again.
    /// First-writer-wins (conditional create) keeps every later restorer on
    /// the same cut. A seal-write failure fails the activation: restoring an
    /// unsealed prefix reopens the hole.
    async fn read_or_seal(
        &self,
        cell: &str,
        from: u64,
        by_epoch: u64,
        client: &ObjectStoreClient,
    ) -> anyhow::Result<u64> {
        use celld_ltx::object_store::path::Path as ObjPath;
        use celld_ltx::object_store::PutMode;
        use celld_ltx::object_store::PutOptions;
        use celld_ltx::object_store::PutPayload;

        #[derive(serde::Serialize, serde::Deserialize)]
        struct EpochSeal {
            max_txid: u64,
            sealed_by_epoch: u64,
        }

        let key = ObjPath::from(self.seal_key(cell, from));
        let existing = |bytes: Vec<u8>| -> anyhow::Result<u64> {
            let seal: EpochSeal = serde_json::from_slice(&bytes)?;
            Ok(seal.max_txid)
        };
        match self.store.get(&key).await {
            Ok(get) => return existing(get.bytes().await?.to_vec()),
            Err(celld_ltx::object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(anyhow!("read seal for {cell} e{from}: {error}")),
        }
        let plan = replica::calc_restore_plan(client, TXID(0))
            .await
            .map_err(|error| anyhow!("plan seal for {cell} e{from}: {error}"))?;
        let cap = plan
            .iter()
            .map(|info| info.max_txid.0)
            .max()
            .ok_or_else(|| anyhow!("seal target {cell} e{from} is empty"))?;
        let seal = EpochSeal {
            max_txid: cap,
            sealed_by_epoch: by_epoch,
        };
        let put = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&key, PutPayload::from(serde_json::to_vec(&seal)?), put)
            .await
        {
            Ok(_) => {
                info!(cell, from, to = by_epoch, cap, "sealed restore source");
                Ok(cap)
            }
            Err(celld_ltx::object_store::Error::AlreadyExists { .. }) => {
                // A racing claimant sealed first; its cut is the one every
                // restore must share.
                let get = self
                    .store
                    .get(&key)
                    .await
                    .map_err(|error| anyhow!("reread seal for {cell} e{from}: {error}"))?;
                existing(get.bytes().await?.to_vec())
            }
            Err(error) => Err(anyhow!("seal {cell} e{from}: {error}")),
        }
    }

    /// Does the bucket hold any LTX for this cell at this epoch? The fail-closed
    /// eviction gate: never delete the last local copy of state the bucket
    /// cannot restore.
    pub async fn epoch_replicated(&self, cell: &str, epoch: u64) -> bool {
        let client = self.client_for(cell, epoch);
        matches!(
            replica::calc_restore_plan(&client, TXID(0)).await,
            Ok(plan) if !plan.is_empty()
        )
    }

    pub async fn activate(
        &self,
        options: ActivationOptions<'_>,
    ) -> anyhow::Result<ActivationResult> {
        let ActivationOptions {
            cell,
            epoch,
            fresh,
            took_over,
            resume_local,
        } = options;
        let dst = self.db_path(cell, epoch);
        std::fs::create_dir_all(dst.parent().unwrap())?;

        // Reuse a preserved local eviction snapshot when it is safe to: the
        // same epoch always, the previous epoch only when we did not take the
        // cell from another node.
        // `.evicted` is the current name; `.hibernated` is what releases
        // before 2026-08-05 wrote. Accept both, so an upgrade reuses the
        // snapshots already on disk instead of restoring every cell from the
        // bucket. Writes always use the new name, so the old one dies out.
        let legacy = |path: &PathBuf| path.with_extension("hibernated");
        let same_epoch = dst.with_extension("evicted");
        let previous = celld_logic::restore::previous_epoch_reusable(epoch, took_over)
            .then(|| self.db_path(cell, epoch - 1).with_extension("evicted"));
        let is_file = |path: &PathBuf| path.is_file();
        let first_present = |path: PathBuf| {
            if path.is_file() {
                Some(path)
            } else {
                Some(legacy(&path)).filter(is_file)
            }
        };
        let local_snapshot = (!fresh && !resume_local)
            .then(|| first_present(same_epoch.clone()).or_else(|| previous.and_then(first_present)))
            .flatten();

        let mut restored = resume_local;
        if resume_local {
            anyhow::ensure!(
                dst.is_file(),
                "clean reload database is missing: {}",
                dst.display()
            );
            info!(cell, epoch, "resumed clean local replica");
        } else if let Some(snapshot) = local_snapshot {
            std::fs::rename(&snapshot, &dst)?;
            info!(cell, epoch, "reused local eviction snapshot");
            restored = true;
        } else if !fresh {
            // Restore the newest durable epoch into this epoch's path — up to
            // its seal, never past it. Sealing before reading fixes the cut
            // every later restore of this prefix shares, so a fenced owner's
            // late uploads into it can never resurrect (see `read_or_seal`).
            if let Some(from) = self.highest_nonempty_epoch(cell).await? {
                let client = self.client_for(cell, from);
                let cap = self.read_or_seal(cell, from, epoch, &client).await?;
                let _ = std::fs::remove_file(&dst);
                let stats = replica::restore_with_download_slots(
                    &client,
                    &dst,
                    TXID(cap),
                    self.restore_slots.clone(),
                )
                .await
                .map_err(|error| anyhow!("restore {cell} e{from}@{cap}: {error}"))?;
                let levels = stats
                    .by_level
                    .iter()
                    .map(|(level, count)| format!("L{level}:{count}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                info!(
                    event = "restore_plan",
                    cell,
                    epoch = from,
                    objects = stats.objects,
                    bytes = stats.bytes,
                    %levels,
                    "computed restore plan"
                );
                info!(cell, from, to = epoch, cap, "restored remote replica");
                restored = true;
            }
        }

        // Open the managed Db (creates a fresh WAL db when nothing was restored)
        // and pair it with this epoch's client. Registration is immediate: the
        // cell can be proved durable on its very first write. The just-opened
        // db's position is the replica's seed -- 0 for a fresh cell, the
        // restored max otherwise, and equal to the remote under epoch fencing --
        // so the first sync skips the `calc_pos` listing that otherwise storms a
        // rate-limiting store. On the rare decode error we leave it unseeded and
        // fall back to that listing.
        let dst_ = dst.clone();
        let (db, seed) = tokio::task::spawn_blocking(move || {
            let mut db = Db::open(&dst_)?;
            let seed = db.pos().ok();
            anyhow::Ok((db, seed))
        })
        .await?
        .map_err(|error| anyhow!("open managed db {}: {error}", dst.display()))?;
        let mut replica = Replica::new(db, self.client_for(cell, epoch));
        if let Some(pos) = seed {
            replica.seed_pos(pos);
        }
        let handle = Arc::new(Cell {
            replica: Mutex::new(replica),
            req_seq: AtomicU64::new(0),
            synced_seq: AtomicU64::new(0),
            durable_txid: AtomicU64::new(seed.map_or(0, |pos| pos.txid.0)),
            syncing: AtomicBool::new(false),
            ready: Notify::new(),
            compaction: self.compaction_queue.as_ref().map(|queue| CellCompaction {
                cell: cell.to_string(),
                epoch,
                client: self.client_for(cell, epoch),
                local_path: Db::meta_path_for_path(&dst),
                queue: queue.clone(),
                min_txids: self.compaction_min_txids,
                compacted_txid: AtomicU64::new(0),
                queued: AtomicBool::new(false),
                cancelled: AtomicBool::new(false),
                cancel: Notify::new(),
            }),
        });
        self.cells
            .lock()
            .unwrap()
            .insert((cell.to_string(), epoch), handle.clone());
        if let Some(pos) = seed {
            maybe_queue_compaction(&handle, pos.txid.0);
        }

        Ok(ActivationResult {
            path: dst,
            restored,
        })
    }

    /// The output gate's primitive: take a durability ticket and return once a
    /// background sync that captured this write has completed, coalescing
    /// concurrent writes to one cell into a single upload. The write committed
    /// before this call, so any sync starting after our ticket captures it —
    /// we wait for `synced_seq >= my ticket`, not for a position, sidestepping
    /// the total_changes↔LTX-txid mismatch that a position compare would hit.
    /// Returns `position` (which the completed sync provably covered) for the
    /// core's coverage check.
    pub async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<u64> {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            anyhow::bail!("ltx cell not resident: {cell} epoch {epoch}");
        };
        let ticket = handle.req_seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.dirty.notify_one();
        // A sustained write burst (a large upload landing in one cell) can
        // legitimately need more than one capture+upload cycle to prove; on a
        // slow or busy store a fixed 10s fences the cell out from under an
        // active request. Keep 10s as the default, let operators raise it.
        let timeout_secs =
            crate::env_vars::with_default("CELLD_LTX_DURABILITY_TIMEOUT_SECS", 10u64)?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            // Register the waiter before checking, so a sync that completes
            // between the check and the await is not missed.
            let ready = handle.ready.notified();
            if handle.synced_seq.load(Ordering::SeqCst) >= ticket {
                return Ok(position);
            }
            if tokio::time::timeout_at(deadline, ready).await.is_err() {
                anyhow::bail!("ltx durability timed out for {cell} epoch {epoch}");
            }
        }
    }

    /// A direct, synchronous durability pass for the rare eviction
    /// gates (not the hot write path). Also advances the cell's durable position
    /// so any output-gate waiters ride it.
    pub async fn sync_wait(&self, cell: &str, epoch: u64, _timeout: Duration) -> SyncWait {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            return SyncWait::Unsupported;
        };
        match sync_cell(handle).await {
            Some(true) => SyncWait::Durable,
            Some(false) => SyncWait::Failed,
            None => SyncWait::Unsupported,
        }
    }

    pub async fn evict(&self, cell: &str, epoch: u64, preserve_local: bool) {
        // A final durability pass so no acknowledged write is stranded, then
        // drop the managed Db (releasing the WAL) before touching the file.
        let _ = self.sync_wait(cell, epoch, Duration::from_secs(10)).await;
        let removed = self
            .cells
            .lock()
            .unwrap()
            .remove(&(cell.to_string(), epoch));
        if let Some(handle) = removed {
            cancel_compaction(&handle);
        }
        let db = self.db_path(cell, epoch);
        if preserve_local {
            let preserved = db.with_extension("evicted");
            if let Err(error) = std::fs::rename(&db, &preserved) {
                warn!(cell, epoch, %error, "preserve local snapshot failed");
            }
        }
        // Clear the WAL/meta siblings and the live db regardless: a reactivation
        // restores or reuses the `.hibernated` copy.
        for suffix in ["-wal", "-shm"] {
            let mut sibling = db.clone().into_os_string();
            sibling.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(sibling));
        }
        let _ = std::fs::remove_dir_all(Db::meta_path_for_path(&db));
        if !preserve_local {
            let _ = std::fs::remove_file(&db);
        }
    }

    /// Copy the live epoch into a private read-only snapshot for inspection.
    pub fn snapshot_active(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<RestoredSnapshot>> {
        let source = self.db_path(cell, epoch);
        if !source.is_file() {
            return Ok(None);
        }
        let directory = self.watch.join(format!(".inspect-{cell}-e{epoch}"));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory)?;
        let path = directory.join("db.sqlite");
        sqlite_snapshot(&source, &path)?;
        Ok(Some(RestoredSnapshot::new(epoch, path, directory)))
    }

    /// Restore the newest durable replica into a private snapshot without
    /// claiming or activating the cell.
    pub async fn restore_snapshot(&self, cell: &str) -> anyhow::Result<Option<RestoredSnapshot>> {
        let Some(epoch) = self.highest_nonempty_epoch(cell).await? else {
            return Ok(None);
        };
        let directory = self.watch.join(format!(".restore-{cell}"));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory)?;
        let path = directory.join("db.sqlite");
        replica::restore_with_download_slots(
            &self.client_for(cell, epoch),
            &path,
            TXID(0),
            self.restore_slots.clone(),
        )
        .await
        .map_err(|error| anyhow!("restore snapshot {cell} e{epoch}: {error}"))?;
        Ok(Some(RestoredSnapshot::new(epoch, path, directory)))
    }

    pub fn prune_local_cache(&self, max_bytes: u64) -> (usize, usize, u64) {
        prune_watch(&self.watch, max_bytes)
    }

    /// Close the replicator handle while retaining the live database and WAL
    /// exactly where the local path encodes them.
    pub fn close_for_reload(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        let removed = self
            .cells
            .lock()
            .unwrap()
            .remove(&(cell.to_string(), epoch));
        if let Some(handle) = removed {
            cancel_compaction(&handle);
        }
        let path = self.db_path(cell, epoch);
        anyhow::ensure!(
            path.is_file(),
            "resident database is missing: {}",
            path.display()
        );
        Ok(())
    }

    /// Enumerate live-named databases. Cached `.evicted` files are separate
    /// and remain under the ordinary cache byte limit.
    pub fn local_cells(&self) -> Vec<celld_logic::LocalCell> {
        let mut cells = Vec::new();
        let Ok(cell_dirs) = std::fs::read_dir(&self.watch) else {
            return cells;
        };
        for cell_dir in cell_dirs.flatten() {
            let Ok(kind) = cell_dir.file_type() else {
                continue;
            };
            if !kind.is_dir() {
                continue;
            }
            let Some(cell) = cell_dir.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(epochs) = std::fs::read_dir(cell_dir.path().join("ltx")) else {
                continue;
            };
            for epoch_dir in epochs.flatten() {
                let Some(epoch) = epoch_dir
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix('e'))
                    .and_then(|epoch| epoch.parse::<u64>().ok())
                else {
                    continue;
                };
                if epoch_dir.path().join("db.sqlite").is_file() {
                    cells.push(celld_logic::LocalCell {
                        id: cell.clone(),
                        epoch,
                    });
                }
            }
        }
        cells.sort();
        cells.dedup();
        cells
    }

    /// Delete stale live-named epochs after the runtime has identified and
    /// closed its exact resident set. Remote replicas remain authoritative.
    pub fn prune_stale_live(
        &self,
        keep: &std::collections::BTreeSet<(String, u64)>,
    ) -> anyhow::Result<usize> {
        let stale: Vec<_> = self
            .local_cells()
            .into_iter()
            .filter(|cell| !keep.contains(&(cell.id.clone(), cell.epoch)))
            .collect();
        for cell in &stale {
            if let Some(parent) = self.db_path(&cell.id, cell.epoch).parent() {
                std::fs::remove_dir_all(parent)?;
            }
        }
        let remaining: std::collections::BTreeSet<_> = self
            .local_cells()
            .into_iter()
            .map(|cell| (cell.id, cell.epoch))
            .collect();
        anyhow::ensure!(
            &remaining == keep,
            "clean reload inventory mismatch after pruning: expected {}, found {}",
            keep.len(),
            remaining.len()
        );
        Ok(stale.len())
    }

    /// No external process to watch: the in-process replicator is healthy as
    /// long as celld is running.
    pub fn process_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        Ok(None)
    }
}

/// One capture+upload for a cell: advance its durable position on success and
/// wake its waiters. Everything committed before the capture is durable once
/// uploaded, so the target is read before `db.sync`. The `rusqlite` handle is
/// `!Sync`, so the whole pass runs on a blocking thread with `block_on` for the
/// async upload. `Some(true)` means success, `Some(false)` means failure, and
/// `None` means that the replica lost its database.
async fn sync_cell(handle: CellHandle) -> Option<bool> {
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        // Tickets taken before the capture: their writes committed before
        // `db.sync` runs, so it captures them. Read before the capture so a
        // ticket taken during the sync is credited by the next one, not this.
        let captured = handle.req_seq.load(Ordering::SeqCst);
        let mut replica = handle.replica.lock().unwrap();
        let db = replica.db_mut()?;
        if let Err(error) = db.sync() {
            warn!(%error, "ltx wal capture failed");
            handle.ready.notify_waiters();
            return Some(false);
        }
        let durable_txid = match runtime.block_on(replica.sync()) {
            Ok(()) => Some(replica.pos().txid.0),
            Err(error) => {
                warn!(%error, "ltx upload failed");
                None
            }
        };
        drop(replica);
        if let Some(durable_txid) = durable_txid {
            handle.synced_seq.fetch_max(captured, Ordering::SeqCst);
            handle.durable_txid.store(durable_txid, Ordering::SeqCst);
            maybe_queue_compaction(&handle, durable_txid);
        }
        handle.ready.notify_waiters();
        Some(durable_txid.is_some())
    })
    .await
    .unwrap_or(Some(false))
}

fn maybe_queue_compaction(handle: &CellHandle, durable_txid: u64) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    if compaction.cancelled.load(Ordering::SeqCst)
        || durable_txid.saturating_sub(compaction.compacted_txid.load(Ordering::SeqCst))
            < compaction.min_txids
        || compaction
            .queued
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
    {
        return;
    }
    if compaction
        .queue
        .send(CompactionWork {
            cell: Arc::downgrade(handle),
            queued_at: Instant::now(),
        })
        .is_err()
    {
        compaction.queued.store(false, Ordering::SeqCst);
    }
}

fn cancel_compaction(handle: &CellHandle) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    compaction.cancelled.store(true, Ordering::SeqCst);
    compaction.cancel.notify_waiters();
}

fn start_compaction_loop(config: CompactionConfig) -> mpsc::UnboundedSender<CompactionWork> {
    let (queue, mut work) = mpsc::unbounded_channel::<CompactionWork>();
    let slots = Arc::new(Semaphore::new(config.concurrency));
    tokio::spawn(async move {
        while let Some(work) = work.recv().await {
            let Ok(permit) = slots.clone().acquire_owned().await else {
                break;
            };
            let Some(cell) = work.cell.upgrade() else {
                continue;
            };
            tokio::spawn(async move {
                let _permit = permit;
                compact_cell(cell, work.queued_at).await;
            });
        }
    });
    queue
}

async fn compact_cell(handle: CellHandle, queued_at: Instant) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    let cancelled = compaction.cancel.notified();
    tokio::pin!(cancelled);
    if compaction.cancelled.load(Ordering::SeqCst) {
        return;
    }

    let queue_ms = queued_at.elapsed().as_millis() as u64;
    let started = Instant::now();
    let compactor = ReplicaCompactor::new(&compaction.client)
        .with_verification(true)
        .with_local_path(&compaction.local_path)
        .with_limits(COMPACTION_MAX_FILES, COMPACTION_MAX_INPUT_BYTES);
    let worker = compactor.compact(1);
    tokio::pin!(worker);
    let result = tokio::select! {
        biased;
        _ = &mut cancelled => None,
        result = &mut worker => Some(result),
    };

    let mut completed = false;
    match result {
        Some(Ok(Some(output))) => {
            let info = output.info;
            compaction
                .compacted_txid
                .store(info.max_txid.0, Ordering::SeqCst);
            completed = true;
            info!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = info.level,
                min_txid = info.min_txid.0,
                max_txid = info.max_txid.0,
                input_objects = output.input_files,
                input_bytes = output.input_bytes,
                local_input_objects = output.local_input_files,
                remote_input_objects = output.input_files - output.local_input_files,
                output_bytes = info.size,
                queue_ms,
                elapsed_ms = started.elapsed().as_millis() as u64,
                result = "ok",
                "compacted an additive LTX level"
            );
        }
        Some(Ok(None)) => {
            compaction
                .compacted_txid
                .store(handle.durable_txid.load(Ordering::SeqCst), Ordering::SeqCst);
            completed = true;
            info!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = started.elapsed().as_millis() as u64,
                result = "no_work",
                "the additive LTX level is current"
            );
        }
        Some(Err(error)) => {
            warn!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = started.elapsed().as_millis() as u64,
                result = "error",
                %error,
                "additive LTX compaction failed"
            );
        }
        None => {
            info!(
                event = "ltx_compaction",
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = started.elapsed().as_millis() as u64,
                result = "cancelled",
                "cancelled an additive LTX compaction"
            );
        }
    }
    compaction.queued.store(false, Ordering::SeqCst);

    if completed && !compaction.cancelled.load(Ordering::SeqCst) {
        // Pace consecutive rounds for one cell: a restart with a large tail
        // otherwise drains back-to-back for minutes. The pause matches the
        // round it follows (capped), so a cell compacts at half duty cycle
        // while the worker slot frees for other cells immediately — this
        // task detaches and does not hold the concurrency permit.
        let pause = started.elapsed().min(std::time::Duration::from_secs(2));
        let handle_ = handle.clone();
        tokio::spawn(async move {
            tokio::time::sleep(pause).await;
            let durable_txid = handle_.durable_txid.load(Ordering::SeqCst);
            maybe_queue_compaction(&handle_, durable_txid);
        });
    }
}

fn compaction_config_from_env() -> anyhow::Result<Option<CompactionConfig>> {
    // On by default. A mixed fleet must set `0` until every node can read
    // v0.5.2 block objects. An old reader cannot take over a cell after its
    // first L1 publication.
    let enabled = crate::env_vars::flag("CELLD_LTX_COMPACTION", true)?;
    if !enabled {
        return Ok(None);
    }

    let min_txids = crate::env_vars::with_default("CELLD_LTX_COMPACTION_MIN_TXIDS", 256)?;
    let concurrency = crate::env_vars::with_default("CELLD_LTX_COMPACTIONS", 2)?;
    anyhow::ensure!(
        min_txids >= 2,
        "CELLD_LTX_COMPACTION_MIN_TXIDS must be at least 2"
    );
    anyhow::ensure!(concurrency > 0, "CELLD_LTX_COMPACTIONS must be positive");
    Ok(Some(CompactionConfig {
        min_txids: min_txids as u64,
        concurrency,
    }))
}

/// The node's background sync loop: wake on a dirty cell (or a slow tick) and
/// launch a sync for every cell whose committed position runs ahead of its
/// durable one. Each cell's sync is an independent, self-rescheduling task —
/// the loop does *not* wait for the batch to finish — so one slow cell's upload
/// never stalls the others (a cell keeps its own cadence up to the concurrency
/// bound). A cell's writes reported between its syncs still clear on one upload:
/// the batching win, without the cross-cell head-of-line blocking.
async fn sync_loop(
    cells: Arc<Mutex<HashMap<(String, u64), CellHandle>>>,
    dirty: Arc<Notify>,
    slots: Arc<Semaphore>,
) {
    loop {
        tokio::select! {
            _ = dirty.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
        let work: Vec<CellHandle> = {
            let map = cells.lock().unwrap();
            map.values()
                .filter(|c| c.req_seq.load(Ordering::SeqCst) > c.synced_seq.load(Ordering::SeqCst))
                .cloned()
                .collect()
        };
        for cell in work {
            // Claim the cell; skip if a sync is already in flight for it.
            if cell
                .syncing
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                continue;
            }
            let slots = slots.clone();
            let dirty = dirty.clone();
            tokio::spawn(async move {
                // Keep syncing this cell while it stays dirty, rather than
                // notifying the main loop to re-scan every completion — that made
                // the loop wake O(cells) times and starved throughput as cells
                // accumulated. This is not a busy loop: each iteration awaits an
                // object-store upload (~one round-trip). A *failed* sync would
                // not, so it backs off, keeping the only tight iterations the
                // ones that actually uploaded.
                loop {
                    let ok = {
                        let _permit = slots.acquire().await;
                        sync_cell(cell.clone()).await
                    };
                    if cell.req_seq.load(Ordering::SeqCst) <= cell.synced_seq.load(Ordering::SeqCst)
                    {
                        break;
                    }
                    if ok != Some(true) {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
                cell.syncing.store(false, Ordering::SeqCst);
                // A write landing in the clear window is picked up next tick;
                // nudge the loop so it does not wait the full interval.
                if cell.req_seq.load(Ordering::SeqCst) > cell.synced_seq.load(Ordering::SeqCst) {
                    dirty.notify_one();
                }
            });
        }
    }
}

/// Node-level object-store config (no per-cell prefix). `build_store` on this
/// yields the one shared client; per-cell clients set only `path`.
fn node_config(
    bucket: &str,
    endpoint: Option<&str>,
    region: &str,
    credentials: Option<&StorageCredentials>,
) -> ObjectStoreConfig {
    let endpoint = endpoint.unwrap_or_default().to_string();
    // Static credentials come from the managed control plane when present,
    // else the `AWS_*` env the node already carries. Without this,
    // `build_store` sees empty keys and object_store falls back to the
    // instance credential provider, which off-EC2 sends unsigned requests (R2
    // answers "404 page not found").
    let env = |key: &str| std::env::var(key).ok().filter(|value| !value.is_empty());
    let access_key_id = credentials
        .map(|c| c.access_key_id.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_ACCESS_KEY_ID"))
        .unwrap_or_default();
    let secret_access_key = credentials
        .map(|c| c.secret_access_key.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_SECRET_ACCESS_KEY"))
        .unwrap_or_default();
    // Temporary R2/STS credentials require the session token, or signing fails.
    let session_token = credentials
        .and_then(|c| c.session_token.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_SESSION_TOKEN"))
        .unwrap_or_default();
    ObjectStoreConfig {
        bucket: bucket.to_string(),
        path: String::new(),
        region: region.to_string(),
        // A custom endpoint (R2/MinIO) uses path-style addressing, matching
        // `ObjectStoreConfig::from_url`'s default for non-AWS hosts.
        force_path_style: !endpoint.is_empty(),
        endpoint,
        access_key_id,
        secret_access_key,
        session_token,
        skip_verify: false,
        part_size: 0,
    }
}
