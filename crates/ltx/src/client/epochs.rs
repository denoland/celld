//! A restore view over the epochs of one cell.
//!
//! A paged activation continues the chain it paged in: its first object
//! follows the cut's last txid instead of opening the epoch with a
//! whole-database snapshot (see [`crate::db::Db::seed_continuation`]). The
//! objects a restore needs can therefore span epochs. The chain walks down
//! from the newest epoch until one that opens with a snapshot, lists every
//! level of each visited epoch once, clips an epoch to the txids before the
//! next epoch's first (a fold that landed after the cut was never part of
//! it), and routes every read by txid. The planner and the page map then
//! run over it unchanged.
use crate::client::ReplicaClient;
use crate::compaction_level::SNAPSHOT_LEVEL;
use crate::error::Error;
use crate::error::Result;
use crate::ltx::FileInfo;
use crate::TXID;
use async_trait::async_trait;
use futures_util::stream;
use futures_util::StreamExt;
use futures_util::TryStreamExt;

/// Listings per epoch in flight while a chain builds.
const LIST_CONCURRENCY: usize = 4;

struct Span<C> {
    epoch: u64,
    client: C,
    /// The first txid this epoch serves; the span before it ends just short.
    lo: TXID,
    /// Files by level, clipped to this span.
    levels: Vec<Vec<FileInfo>>,
}

pub struct EpochChain<C> {
    spans: Vec<Span<C>>,
}

impl<C: ReplicaClient> EpochChain<C> {
    /// Walks `epochs` (ascending) down from the newest until an epoch that
    /// opens with a snapshot, listing every level of each visited epoch. The
    /// link below a continued epoch is the epoch whose objects, clipped to
    /// the txids before the continuation's first, end exactly at the cut; an
    /// epoch below that ends elsewhere is skipped. A fenced owner's late
    /// snapshot can land in an intermediate epoch, whole but older than the
    /// cut, and taking it as the base loses the writes between it and the
    /// cut (CelldPersistencePaged.tla, `BreakLinkByOrder`). An epoch without
    /// objects contributes nothing; no chain at all is `TxNotAvailable`.
    pub async fn build(mut epochs: Vec<(u64, C)>) -> Result<Self> {
        let mut spans: Vec<Span<C>> = Vec::new();
        while let Some((epoch, client)) = epochs.pop() {
            let mut levels = list_levels(&client).await?;
            if let Some(above) = spans.last() {
                // Clip to the txids before the continuation's first, then
                // the link must end at the cut.
                for files in &mut levels {
                    files.retain(|f| f.max_txid < above.lo);
                }
                let ends = levels.iter().flatten().map(|f| f.max_txid).max();
                if ends != Some(TXID(above.lo.0 - 1)) {
                    continue;
                }
            }
            let Some(lo) = levels.iter().flatten().map(|f| f.min_txid).min() else {
                continue;
            };
            spans.push(Span {
                epoch,
                client,
                lo,
                levels,
            });
            if lo == TXID(1) {
                break;
            }
        }
        if spans.is_empty() {
            return Err(Error::TxNotAvailable);
        }
        spans.reverse();
        Ok(Self { spans })
    }

    /// The epochs in the chain, oldest first, each with the first txid it
    /// serves.
    pub fn spans(&self) -> Vec<(u64, TXID)> {
        self.spans.iter().map(|s| (s.epoch, s.lo)).collect()
    }

    /// The last txid of the chain: the cut a continuation starts after.
    pub fn max_txid(&self) -> TXID {
        self.spans
            .iter()
            .flat_map(|s| s.levels.iter().flatten())
            .map(|f| f.max_txid)
            .max()
            .unwrap_or(TXID(0))
    }

    fn span(&self, txid: TXID) -> &Span<C> {
        self.spans
            .iter()
            .rev()
            .find(|s| s.lo <= txid)
            .unwrap_or(&self.spans[0])
    }
}

async fn list_levels<C: ReplicaClient>(client: &C) -> Result<Vec<Vec<FileInfo>>> {
    stream::iter(0..=SNAPSHOT_LEVEL)
        .map(|level| async move { client.ltx_files(level, TXID(0)).await })
        .buffered(LIST_CONCURRENCY)
        .try_collect()
        .await
}

#[async_trait]
impl<C: ReplicaClient> ReplicaClient for EpochChain<C> {
    async fn ltx_files(&self, level: i32, seek: TXID) -> Result<Vec<FileInfo>> {
        Ok(self
            .spans
            .iter()
            .flat_map(|s| s.levels.get(level as usize).into_iter().flatten())
            .filter(|f| f.min_txid >= seek)
            .cloned()
            .collect())
    }

    async fn open_ltx_file(&self, level: i32, min_txid: TXID, max_txid: TXID) -> Result<Vec<u8>> {
        self.span(min_txid)
            .client
            .open_ltx_file(level, min_txid, max_txid)
            .await
    }

    async fn read_range(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        self.span(min_txid)
            .client
            .read_range(level, min_txid, max_txid, offset, len)
            .await
    }

    async fn write_ltx_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        data: &[u8],
    ) -> Result<FileInfo> {
        self.span(min_txid)
            .client
            .write_ltx_file(level, min_txid, max_txid, data)
            .await
    }

    async fn delete_ltx_files(&self, files: &[FileInfo]) -> Result<()> {
        for file in files {
            self.span(file.min_txid)
                .client
                .delete_ltx_files(std::slice::from_ref(file))
                .await?;
        }
        Ok(())
    }

    async fn delete_all(&self) -> Result<()> {
        Err(Error::Other("an epoch chain is a restore view".into()))
    }
}
