//! Additive LTX compaction through a [`ReplicaClient`].
//!
//! This module ports the storage-independent part of Litestream v0.5.16's
//! `Compactor`. It creates a destination object but never deletes a source.

use crate::client::ReplicaClient;
use crate::compaction_level::SNAPSHOT_LEVEL;
use crate::compactor::Compactor;
use crate::error::{Error, Result};
use crate::ltx::{FileInfo, HEADER_FLAG_NO_CHECKSUM};
use crate::ltx_file_path;
use crate::LtxHost;
use crate::TXID;
use std::io::Cursor;
use std::path::Path;
use std::path::PathBuf;

/// The immutable object and source volume from one compaction attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionOutput {
    /// The published destination object.
    pub info: FileInfo,
    /// The number of source objects in the merge.
    pub input_files: usize,
    /// The sum of the source object sizes.
    pub input_bytes: u64,
    /// The number of source objects read from the local LTX directory.
    pub local_input_files: usize,
}

/// Compacts LTX objects between two adjacent replica levels.
pub struct ReplicaCompactor<'a, C> {
    client: &'a C,
    verify: bool,
    max_files: usize,
    max_input_bytes: u64,
    /// The epoch's first txid. An epoch that continues a chain it paged in
    /// starts at its cut, not at 1, so an empty destination level continues
    /// from here rather than from the first txid that never existed here.
    base: TXID,
    local_path: Option<PathBuf>,
    host: LtxHost,
}

impl<'a, C: ReplicaClient> ReplicaCompactor<'a, C> {
    pub fn new(client: &'a C) -> Self {
        Self {
            client,
            verify: false,
            max_files: usize::MAX,
            max_input_bytes: u64::MAX,
            base: TXID(1),
            local_path: None,
            host: LtxHost::default(),
        }
    }

    /// Enables a destination-level continuity check after publication.
    /// The txid the epoch's chain starts at (see the `base` field).
    pub fn with_base(mut self, base: TXID) -> Self {
        self.base = base;
        self
    }

    pub fn with_verification(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }

    /// Limits one compaction attempt to a contiguous source prefix.
    pub fn with_limits(mut self, max_files: usize, max_input_bytes: u64) -> Self {
        self.max_files = max_files;
        self.max_input_bytes = max_input_bytes;
        self
    }

    /// Uses the local LTX directory before it reads an object from the replica.
    pub fn with_local_path(mut self, path: impl AsRef<Path>) -> Self {
        self.local_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Uses an injected clock and executor host.
    pub fn with_host(mut self, host: LtxHost) -> Self {
        self.host = host;
        self
    }

    /// Compacts one new object prefix from `destination_level - 1`.
    ///
    /// The method returns `Ok(None)` when the destination already covers every
    /// available source object. It publishes one immutable destination object
    /// and leaves every source object intact.
    pub async fn compact(&self, destination_level: i32) -> Result<Option<CompactionOutput>> {
        if !(1..SNAPSHOT_LEVEL).contains(&destination_level) {
            return Err(invalid("the destination compaction level is invalid"));
        }
        if self.max_files == 0 || self.max_input_bytes == 0 {
            return Err(invalid("the compaction limits must be positive"));
        }

        let destination = self.client.ltx_files(destination_level, TXID(0)).await?;
        let previous_max = destination
            .iter()
            .map(|file| file.max_txid)
            .max()
            .unwrap_or(TXID(0));
        let seek = TXID(previous_max.0.wrapping_add(1).max(self.base.0));
        let source_level = destination_level - 1;
        let available = self
            .client
            .ltx_files_bounded(source_level, seek, self.max_files)
            .await?;
        let mut source = Vec::new();
        let mut input_bytes = 0u64;
        for file in available {
            let size = u64::try_from(file.size)
                .map_err(|_| invalid("a compaction source has a negative size"))?;
            if source.len() == self.max_files {
                break;
            }
            let total = input_bytes
                .checked_add(size)
                .ok_or_else(|| invalid("the compaction source size overflows"))?;
            if total > self.max_input_bytes {
                if source.is_empty() {
                    return Err(invalid("a compaction source exceeds the byte limit"));
                }
                break;
            }
            input_bytes = total;
            source.push(file);
        }
        if source.is_empty() {
            return Ok(None);
        }

        let min_txid = source
            .iter()
            .map(|file| file.min_txid)
            .min()
            .ok_or_else(|| invalid("the compaction source is empty"))?;
        let max_txid = source
            .iter()
            .map(|file| file.max_txid)
            .max()
            .ok_or_else(|| invalid("the compaction source is empty"))?;
        if min_txid != seek {
            return Err(invalid(
                "the compaction source does not continue the destination level",
            ));
        }
        let mut readers = Vec::with_capacity(source.len());
        let mut local_input_files = 0usize;
        for file in &source {
            let bytes = match &self.local_path {
                Some(path) => {
                    let filename = ltx_file_path(
                        &path.to_string_lossy(),
                        file.level as u32,
                        file.min_txid,
                        file.max_txid,
                    );
                    match self.host.read_file(filename).await {
                        Ok(bytes) => {
                            local_input_files += 1;
                            bytes
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            self.client
                                .open_ltx_file(file.level, file.min_txid, file.max_txid)
                                .await?
                        }
                        Err(error) => return Err(Error::Io(error)),
                    }
                }
                None => {
                    self.client
                        .open_ltx_file(file.level, file.min_txid, file.max_txid)
                        .await?
                }
            };
            readers.push(Cursor::new(bytes));
        }

        // The merge is pure CPU over as much as 64 MiB and must not hold a
        // runtime worker: a restart drain runs rounds back-to-back, and on a
        // 4-vCPU node two pegged workers starved co-owned cells' durable
        // writes (2026-08-12 fleet roll).
        let (header, output) = self
            .host
            .run_blocking(move || {
                let mut compactor = Compactor::new(Vec::new(), readers);
                compactor.header_flags = HEADER_FLAG_NO_CHECKSUM;
                compactor.compact()?;
                Ok::<_, Error>((compactor.header(), compactor.into_writer()))
            })
            .await
            .map_err(|_| invalid("the compaction merge task panicked"))??;
        if header.min_txid != min_txid || header.max_txid != max_txid {
            return Err(invalid(
                "a compaction source key does not match its LTX header",
            ));
        }
        let info = self
            .client
            .write_ltx_file(destination_level, min_txid, max_txid, &output)
            .await?;

        if self.verify {
            self.verify_level(destination_level).await?;
        }
        Ok(Some(CompactionOutput {
            info,
            input_files: source.len(),
            input_bytes,
            local_input_files,
        }))
    }

    /// Verifies that a destination level has neither gaps nor overlaps.
    pub async fn verify_level(&self, level: i32) -> Result<()> {
        let files = self.client.ltx_files(level, TXID(0)).await?;
        for pair in files.windows(2) {
            let expected = pair[0].max_txid.0.wrapping_add(1);
            if pair[1].min_txid != TXID(expected) {
                return Err(invalid("the compaction level is not contiguous"));
            }
        }
        Ok(())
    }
}

fn invalid(message: &'static str) -> Error {
    Error::Other(message.into())
}
