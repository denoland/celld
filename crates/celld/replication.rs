// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Shared replication primitives for the in-process LTX backend
//! ([`crate::ltx_repl::LtxRepl`]). Each cell db lives at
//! `<watch>/<cell>/ltx/e<epoch>/db.sqlite` and replicates to
//! `cells/<cell>/ltx/e<epoch>/` in the bucket — epoch-in-prefix is the
//! data-path fence: a stale owner writes a dead prefix.
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// The first retry delay for a cell-release operation.
///
/// The actor uses this delay when a host keeps a cell. The snapshot publisher
/// uses it as the first step of a linear backoff after the L0 proof passes.
pub(crate) const CELL_RELEASE_RETRY_BASE_MS: u64 = 50;

/// The remote artifact that a successor can use after an eviction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictionRestoreArtifact {
    /// A full L9 snapshot covers the closed database.
    Snapshot,
    /// The proven additive L0 chain remains the restore path.
    L0Chain,
}

/// Outcome of a blocking replication wait on one cell db.
pub enum SyncWait {
    /// The latest local commit is in the bucket.
    Durable,
    /// The replicator does not track this cell; the caller decides its
    /// fallback.
    Unsupported,
    /// The wait failed or timed out.
    Failed,
}

pub struct RestoredSnapshot {
    pub epoch: u64,
    path: PathBuf,
    directory: PathBuf,
    filesystem: Arc<dyn celld_ltx::FileSystem>,
}

impl RestoredSnapshot {
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Construct a snapshot whose `directory` is removed on drop, handing the
    /// caller an inspection copy with RAII cleanup.
    pub(crate) fn new(
        epoch: u64,
        path: PathBuf,
        directory: PathBuf,
        filesystem: Arc<dyn celld_ltx::FileSystem>,
    ) -> Self {
        Self {
            epoch,
            path,
            directory,
            filesystem,
        }
    }
}

impl Drop for RestoredSnapshot {
    fn drop(&mut self) {
        let _ = self.filesystem.remove_dir_all(&self.directory);
    }
}

#[derive(Clone)]
pub struct StorageCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

pub struct ActivationOptions<'a> {
    pub cell: &'a str,
    pub epoch: u64,
    /// The epoch-one ownership record was created conditionally by this
    /// activation. No earlier replica can exist for this cell.
    pub fresh: bool,
    /// This activation seized the cell from a DIFFERENT node. When false the
    /// ownership record still named us at `epoch - 1`, so no other process
    /// has written the cell since we evicted it and our preserved local
    /// state is authoritative.
    pub took_over: bool,
    /// Open the exact existing local epoch after a certified node-level
    /// handoff. This path performs no remote discovery or restore.
    pub resume_local: bool,
    /// The owner a takeover displaced, from the record version the acquire
    /// consumed. The node-log takeover interlock recovers that node's log
    /// before the restore reads or seals anything; `None` means the record
    /// was released or absent, which the release path already proved
    /// durable.
    pub prior: Option<String>,
}

pub struct ActivationResult {
    pub path: PathBuf,
    pub restored: bool,
    /// The paged VFS registered for this activation, when the restore paged
    /// instead of downloading. The local main-db file is then sparse, so every
    /// connection to `path` — the actor's included — must open through this
    /// VFS; a plain open reads holes as zeros and SQLite reports a malformed
    /// database (the 2026-09-01 cold-whale fleet failure).
    pub vfs: Option<String>,
}

/// Is this a preserved eviction snapshot? `.hibernated` is the pre-2026-08-05
/// name and is still recognised, or a node upgrading would keep those files
/// forever without counting them.
fn is_snapshot(path: &std::path::Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext == "evicted" || ext == "hibernated")
}

/// The preserved-snapshot cache, held in memory so the prune does not have to
/// walk the data directory to discover it has nothing to do.
///
/// `celld_logic::cache::plan_eviction` states the contract this exists to
/// honour: "An empty result means the cache already fits — the common case,
/// and it must never do I/O-worthy work then." The walk that used to feed it
/// broke that promise, because finding out the cache already fits meant
/// reading every directory under the watch root and stat-ing every entry in
/// them, once a minute, forever.
///
/// Keyed by path rather than by cell and epoch: a reactivation may consume the
/// *previous* epoch's snapshot, and either the current or the legacy name, so
/// the path is the only key that covers every case uniformly.
pub(crate) struct PreservedCache {
    filesystem: Arc<dyn celld_ltx::FileSystem>,
    entries: std::collections::BTreeMap<PathBuf, celld_logic::cache::CacheEntry>,
    bytes: u64,
    /// Prunes served since the index was last rebuilt from the filesystem.
    /// `None` until the first adoption or while a failed mutation requires a
    /// rescan.
    since_resync: Option<u32>,
}

/// Rebuild after this many prunes. The index is maintained at every point that
/// creates or consumes a snapshot, but a failed rename or a file removed from
/// outside would drift it. Over-counting is harmless -- the node evicts a
/// little early -- while under-counting would let the cache grow past its
/// ceiling unnoticed, which is the whole thing this bounds, so the truth is
/// re-read periodically. At the 60 s prune period this is hourly.
const RESYNC_EVERY: u32 = 60;

impl PreservedCache {
    pub(crate) fn new(filesystem: Arc<dyn celld_ltx::FileSystem>) -> Self {
        Self {
            filesystem,
            entries: std::collections::BTreeMap::new(),
            bytes: 0,
            since_resync: None,
        }
    }

    /// Record a snapshot this process just created.
    ///
    /// The size and mtime are read from the file rather than assumed: `rename`
    /// carries the original modification time, so stamping "now" here would
    /// quietly reorder the LRU against what the filesystem actually holds.
    pub(crate) fn insert(&mut self, path: PathBuf) -> std::io::Result<()> {
        let meta = match self.filesystem.metadata(&path) {
            Ok(meta) => meta,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // Reactivation can consume the renamed snapshot before this
                // bookkeeping call gets the cache lock. The absent path has no
                // bytes to account, and an older entry for the same path is now
                // stale.
                self.forget(&path);
                return Ok(());
            }
            Err(error) => {
                // The renamed snapshot exists, but this process cannot account
                // for its size. Force the next prune to rebuild the complete
                // index instead of treating the current byte total as truth.
                self.since_resync = None;
                return Err(error);
            }
        };
        let entry = celld_logic::cache::CacheEntry {
            last_used_ms: meta.modified_unix_millis,
            bytes: meta.len,
        };
        if let Some(previous) = self.entries.insert(path, entry) {
            self.bytes = self.bytes.saturating_sub(previous.bytes);
        }
        self.bytes = self.bytes.saturating_add(entry.bytes);
        Ok(())
    }

    /// Drop a snapshot that has been consumed or deleted.
    pub(crate) fn forget(&mut self, path: &std::path::Path) {
        if let Some(entry) = self.entries.remove(path) {
            self.bytes = self.bytes.saturating_sub(entry.bytes);
        }
    }

    /// Bring the cache under `max_bytes`, returning (kept, evicted, bytes).
    ///
    /// Answers without touching the filesystem whenever the cache already
    /// fits, which is the steady state.
    pub(crate) fn prune(
        &mut self,
        watch: &std::path::Path,
        max_bytes: u64,
    ) -> std::io::Result<(usize, usize, u64)> {
        let mut first_error = None;
        match self.since_resync {
            // First prune of the process: adopt whatever a previous process
            // left behind. The walk rebuilds from ground truth, so anything
            // inserted before now is corrected rather than double-counted.
            None | Some(RESYNC_EVERY..) => {
                if let Err(error) = self.adopt(watch) {
                    // Adoption is transactional, so its failure leaves only
                    // entries that this process inserted or a prior complete
                    // scan installed. Deleting any such cache entry is safe.
                    // Continue from that known lower bound, then report that
                    // the complete on-disk accounting remains unavailable.
                    first_error = Some(error);
                }
            }
            Some(count) => self.since_resync = Some(count + 1),
        }
        if self.bytes <= max_bytes {
            return match first_error {
                Some(error) => Err(error),
                None => Ok((self.entries.len(), 0, self.bytes)),
            };
        }
        let mut evicted = 0;
        let mut failed_paths = std::collections::BTreeSet::new();
        loop {
            // A failed deletion remains part of the byte total and is pinned
            // for this invocation. Replan the unfailed entries against the
            // remaining budget, so a failed planned victim makes another LRU
            // entry eligible instead of stopping progress above the ceiling.
            let mut pinned_bytes = 0_u64;
            let mut paths = Vec::new();
            let mut entries = Vec::new();
            for (path, entry) in &self.entries {
                if failed_paths.contains(path) {
                    pinned_bytes = pinned_bytes.saturating_add(entry.bytes);
                } else {
                    paths.push(path.clone());
                    entries.push(*entry);
                }
            }
            let remaining_budget = max_bytes.saturating_sub(pinned_bytes);
            let plan = celld_logic::cache::plan_eviction(&entries, remaining_budget);
            if plan.is_empty() {
                break;
            }
            for index in plan {
                match self.filesystem.remove_file(&paths[index]) {
                    Ok(()) => {
                        self.forget(&paths[index]);
                        evicted += 1;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        self.forget(&paths[index]);
                        evicted += 1;
                    }
                    Err(error) => {
                        // Keep the failed entry and its bytes in the inventory.
                        // Excluding it from later plans guarantees termination,
                        // while replanning gives every remaining entry a chance
                        // to restore the known index to its byte ceiling.
                        failed_paths.insert(paths[index].clone());
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok((self.entries.len(), evicted, self.bytes)),
        }
    }

    /// Read one directory for an adoption without treating an aggregated
    /// `NotFound` as an empty directory.
    ///
    /// A queued descendant can disappear after its parent was read, which is a
    /// complete observation of that now-absent subtree. The direct backend can
    /// also aggregate one vanished entry into the directory-level result. If
    /// the directory still exists, retry once and then propagate the error so
    /// the caller retains its known index instead of under-counting the
    /// directory. A missing watch root is always an error because a first-start
    /// scan cannot infer a complete inventory from it.
    fn read_adoption_directory(
        &self,
        watch: &std::path::Path,
        directory: &std::path::Path,
    ) -> std::io::Result<Option<Vec<celld_ltx::HostDirEntry>>> {
        for attempt in 0..=1 {
            match self.filesystem.read_dir(directory) {
                Ok(entries) => return Ok(Some(entries)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    match self.filesystem.metadata(directory) {
                        Err(confirm) if confirm.kind() == std::io::ErrorKind::NotFound => {
                            if directory == watch {
                                return Err(error);
                            }
                            return Ok(None);
                        }
                        Ok(metadata) if metadata.is_dir && attempt == 0 => continue,
                        Ok(_) => return Err(error),
                        Err(confirm) => return Err(confirm),
                    }
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("the bounded directory retry returned from every branch")
    }

    /// Read the preserved snapshots off disk and replace the index with them.
    ///
    /// The only walk left, and it runs once per process plus a periodic
    /// resync. `file_type` answers the recursion question from the directory
    /// entry itself, so a `stat` is spent only on the files that are actually
    /// candidates -- the live databases, their WALs, their shared-memory
    /// indexes and the meta directories are skipped on their names.
    fn adopt(&mut self, watch: &std::path::Path) -> std::io::Result<()> {
        let mut entries = std::collections::BTreeMap::new();
        let mut bytes = 0_u64;
        let mut stack = vec![watch.to_path_buf()];
        while let Some(directory) = stack.pop() {
            let Some(read) = self.read_adoption_directory(watch, &directory)? else {
                continue;
            };
            for item in read {
                let path = item.path;
                if item.is_dir {
                    stack.push(path);
                    continue;
                }
                if !is_snapshot(&path) {
                    continue;
                }
                let meta = match self.filesystem.metadata(&path) {
                    Ok(meta) => meta,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        // Reactivation or stale-live pruning removed this path
                        // after the directory read. It is absent from the final
                        // cache image, so skipping it preserves complete
                        // accounting.
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let entry = celld_logic::cache::CacheEntry {
                    last_used_ms: meta.modified_unix_millis,
                    bytes: meta.len,
                };
                bytes = bytes.saturating_add(entry.bytes);
                entries.insert(path, entry);
            }
        }
        self.entries = entries;
        self.bytes = bytes;
        self.since_resync = Some(0);
        Ok(())
    }
}

/// Copy a live database into a standalone snapshot. SQLite's backup API
/// includes committed WAL state without checkpointing or interfering with the
/// replicator's ownership of the WAL.
pub(crate) fn sqlite_snapshot(
    source: &std::path::Path,
    destination: &std::path::Path,
    vfs: Option<&str>,
) -> anyhow::Result<()> {
    {
        let open = |path: &std::path::Path, flags, role| {
            #[cfg(celld_internal_tests)]
            {
                match vfs {
                    Some(vfs) => crate::fault::with_connection_role(role, || {
                        Connection::open_with_flags_and_vfs(path, flags, vfs)
                    }),
                    None => Connection::open_with_flags(path, flags),
                }
            }
            #[cfg(not(celld_internal_tests))]
            {
                let _ = role;
                debug_assert!(vfs.is_none());
                Connection::open_with_flags(path, flags)
            }
        };
        let source = open(
            source,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            "sqlite_snapshot_source",
        )?;
        let mut destination = open(
            destination,
            rusqlite::OpenFlags::default(),
            "sqlite_snapshot_destination",
        )?;
        let backup = rusqlite::backup::Backup::new(&source, &mut destination)?;
        backup.run_to_completion(64, Duration::from_millis(5), None)?;
    }
    Ok(())
}

#[cfg(celld_internal_tests)]
#[doc(hidden)]
pub fn sqlite_snapshot_on_vfs_for_test(
    source: &std::path::Path,
    destination: &std::path::Path,
    vfs: &str,
) -> anyhow::Result<()> {
    sqlite_snapshot(source, destination, Some(vfs))
}
