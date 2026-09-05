// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Durable hints for the cell epochs a recovery has already folded. The
//! per-cell layout remains the data, and the session's witness and seal remain
//! the authority. Checkpoints only avoid repeating cold coverage listings when
//! the recovering process dies and loses its in-memory watermarks.

use crate::asyncrt;
use crate::bucket::Bucket;
use anyhow::{anyhow, Context};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::BTreeMap;

// One completed upload window per checkpoint amortizes the extra PUT without
// coupling different sessions or changing the existing upload concurrency.
const BATCH: usize = super::RECOVERY_UPLOAD_CONCURRENCY;
const IO_TIMEOUT: std::time::Duration = super::RECOVERY_HEARTBEAT;

#[derive(Serialize, Deserialize)]
pub(super) struct CoveredCell {
    pub cell: String,
    pub epoch: u64,
    pub through: u64,
}

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    version: u32,
    cells: Vec<CoveredCell>,
}

pub(super) struct Progress {
    prefix: String,
    covered: BTreeMap<(String, u64), u64>,
    pending: Vec<CoveredCell>,
    writable: bool,
}

impl Progress {
    pub async fn load(
        manager: &super::NodeLogManager,
        session: &str,
        beat: &mut super::ClaimBeat,
    ) -> anyhow::Result<Self> {
        let bucket = &manager.bucket;
        let mut progress = Self {
            prefix: format!("log/{session}/recovered/"),
            covered: BTreeMap::new(),
            pending: Vec::new(),
            writable: true,
        };
        let listed = match asyncrt::timeout(IO_TIMEOUT, bucket.list(&progress.prefix)).await {
            Ok(Ok(listed)) => listed,
            result => {
                tracing::warn!(
                    session,
                    ?result,
                    "recovery checkpoints unavailable; checking cell coverage"
                );
                return Ok(progress);
            }
        };
        let reads = listed.into_iter().map(|meta| async move {
            let key = meta.location.as_ref();
            let read = async {
                let (bytes, _) = bucket
                    .get(key)
                    .await?
                    .ok_or_else(|| anyhow!("checkpoint disappeared"))?;
                // Content-addressing keeps interrupted or corrupt metadata
                // from over-reporting coverage. Unknown versions are hints
                // we cannot use, not a reason to refuse a recoverable log.
                let digest = format!("{:x}.json", sha2::Sha256::digest(&bytes));
                anyhow::ensure!(
                    key.rsplit('/').next() == Some(digest.as_str()),
                    "checkpoint digest mismatch"
                );
                let checkpoint: Checkpoint = serde_json::from_slice(&bytes)?;
                anyhow::ensure!(checkpoint.version == 1, "unknown checkpoint version");
                Ok::<_, anyhow::Error>(checkpoint.cells)
            };
            let result = asyncrt::timeout(IO_TIMEOUT, read)
                .await
                .context("checkpoint read timed out")
                .and_then(|result| result);
            (meta, result)
        });
        let mut reads = futures_util::stream::iter(reads).buffer_unordered(BATCH);
        while let Some((meta, result)) = reads.next().await {
            manager.beat_claim(session, beat).await?;
            match result {
                Ok(cells) => {
                    for cell in cells {
                        let through = progress.covered.entry((cell.cell, cell.epoch)).or_default();
                        *through = (*through).max(cell.through);
                    }
                }
                Err(error) => {
                    // Stop at the first unavailable hint. Waiting through
                    // every failed read window could make this optional
                    // optimization slower than checking the cell layout.
                    tracing::warn!(key = %meta.location, %error,
                        "recovery checkpoint unavailable; checking remaining cell coverage");
                    break;
                }
            }
        }
        Ok(progress)
    }

    pub fn through(&self, cell: &str, epoch: u64) -> Option<u64> {
        self.covered.get(&(cell.to_string(), epoch)).copied()
    }

    /// Called only after all uploads for this cell epoch complete, or a
    /// coverage lookup proves they were already durable. An interrupted cell
    /// never contributes a hint, even if some of its PUTs reached the bucket.
    pub async fn completed(&mut self, bucket: &Bucket, cell: CoveredCell) {
        if !self.writable {
            return;
        }
        self.pending.push(cell);
        if self.pending.len() < BATCH {
            return;
        }
        let checkpoint = Checkpoint {
            version: 1,
            cells: std::mem::take(&mut self.pending),
        };
        let write = async {
            let bytes = serde_json::to_vec(&checkpoint)?;
            let key = format!("{}{:x}.json", self.prefix, sha2::Sha256::digest(&bytes));
            bucket.put(&key, bytes).await
        };
        if let Err(error) = asyncrt::timeout(IO_TIMEOUT, write)
            .await
            .context("checkpoint write timed out")
            .and_then(|result| result)
        {
            // Checkpoint storage must not become a second serving dependency.
            // The uploaded cells are still durable; a later process simply
            // repeats their coverage checks. An ambiguous PUT is safe to read
            // on that retry because every hint followed its data's completion.
            self.writable = false;
            tracing::warn!(%error, "recovery checkpoint failed; continuing without new checkpoints");
        }
    }
}
