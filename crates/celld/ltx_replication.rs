// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The node's replication engine behind one engine-neutral handle.

use crate::ltx_repl::LtxRepl;
use crate::replication::ActivationOptions;
use crate::replication::StorageCredentials;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

/// The node's replication engine: the in-process `celld-ltx` replicator,
/// hidden behind this wrapper so nothing else touches the backend directly.
#[derive(Clone)]
pub struct Replication {
    ltx: Arc<LtxRepl>,
}

impl Replication {
    /// See `LtxRepl::set_paged_fleet`: the fleet sampler's answer to whether
    /// every live lease reads a paged epoch.
    pub fn set_paged_fleet(&self, ready: bool) -> bool {
        self.ltx.set_paged_fleet(ready)
    }

    /// Whether this node would page a large takeover now.
    pub fn paged_fleet(&self) -> bool {
        self.ltx.paged_fleet()
    }

    pub fn start(
        bucket: crate::bucket::Bucket,
        watch: &Path,
        endpoint: Option<String>,
        region: String,
        credentials: Option<StorageCredentials>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            ltx: Arc::new(LtxRepl::start(
                watch,
                bucket.backend(),
                bucket.name,
                bucket.prefix,
                endpoint,
                region,
                credentials,
            )?),
        })
    }

    /// The log tier installs its shipper and takeover interlock here.
    pub fn ltx(&self) -> Arc<LtxRepl> {
        self.ltx.clone()
    }

    pub(crate) async fn restore(
        &self,
        cell: &str,
        spec: &celld_logic::RestoreSpec,
        allow_paged: bool,
    ) -> anyhow::Result<(PathBuf, bool, Option<String>)> {
        let options = ActivationOptions {
            cell,
            epoch: spec.epoch,
            fresh: spec.fresh,
            took_over: spec.took_over,
            resume_local: spec.resume_local,
            prior: spec.prior.clone(),
        };
        let activated = self.ltx.activate_with(options, allow_paged).await?;
        Ok((activated.path, activated.restored, activated.vfs))
    }

    pub fn process_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.ltx.process_status()
    }

    /// Enforce the byte ceiling on preserved eviction snapshots.
    ///
    /// The directory walk is synchronous, so callers must run this on a
    /// blocking executor rather than the runtime's serving thread.
    pub fn prune_local_cache(&self, max_bytes: u64) -> std::io::Result<(usize, usize, u64)> {
        self.ltx.prune_local_cache(max_bytes)
    }

    pub async fn close_for_reload(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        self.ltx.close_for_reload(cell, epoch).await
    }

    pub fn local_cells(&self) -> Vec<celld_logic::LocalCell> {
        self.ltx.local_cells()
    }

    pub fn prune_stale_live(&self, keep: &BTreeSet<(String, u64)>) -> anyhow::Result<usize> {
        self.ltx.prune_stale_live(keep)
    }

    /// Copy the exact published epoch into a private read-only snapshot.
    pub fn snapshot_active(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<crate::replication::RestoredSnapshot>> {
        self.ltx.snapshot_active(cell, epoch)
    }

    /// Restore the newest completed replica without claiming or activating it.
    pub async fn restore_snapshot(
        &self,
        cell: &str,
    ) -> anyhow::Result<Option<crate::replication::RestoredSnapshot>> {
        self.ltx.restore_snapshot(cell).await
    }

    /// The eviction gate. `LtxRepl::handoff_gate` holds the reasoning: it
    /// owns both questions the gate asks, so the V8 host and the scripted
    /// host cannot drift apart on which one a revocable eviction takes.
    pub(crate) async fn ensure_durable(
        &self,
        cell: &str,
        epoch: u64,
        revocable: bool,
    ) -> anyhow::Result<()> {
        self.ltx.handoff_gate(cell, epoch, revocable).await
    }

    /// The output-gate durability wait: return the committed-write position the
    /// replica has proved durable, at least covering `position`, and which
    /// mechanism proved it (the fences differ; see `celld_logic::ProofSource`).
    /// The replicator batches concurrent writes to one cell behind a
    /// background sync and reports the real durable position.
    pub(crate) async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<(u64, celld_logic::ProofSource)> {
        self.ltx.await_durable(cell, epoch, position).await
    }

    pub(crate) async fn evict(
        &self,
        cell: &str,
        epoch: u64,
        preserve_local: bool,
        abandon: Option<&crate::replication::EvictionAbandon>,
    ) -> anyhow::Result<()> {
        self.ltx
            .evict(cell, epoch, preserve_local, abandon)
            .await
            .map(|_| ())
    }

    pub(crate) async fn release(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        self.ltx.release(cell, epoch).await
    }

    pub(crate) async fn close_in_place(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        self.ltx.close_in_place(cell, epoch).await
    }
}
