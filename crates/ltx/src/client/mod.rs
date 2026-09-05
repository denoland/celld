//! client — the `ReplicaClient` trait.
//!
//! Ported from litestream@v0.5.11 `replica_client.go`. The trait is the storage
//! abstraction that every backend implements.
//!
//! # Buffered I/O
//! Go's `ReplicaClient` uses `io.Reader`/`io.ReadCloser`. We take/return owned
//! byte buffers (`&[u8]` / `Vec<u8>`) instead. L0 files are bounded in size;
//! large snapshots remain buffered.

use crate::error::Result;
use crate::ltx::FileInfo;
use crate::TXID;
use async_trait::async_trait;

pub mod bundle;
pub mod epochs;
pub mod file;
pub mod object_store;

/// Client for reading and writing LTX files on a replica backend.
///
/// Ported from the `ReplicaClient` interface (replica_client.go:19-51). Methods
/// take a compaction `level` (0 = L0, the only level in the one-shot scope).
#[async_trait]
pub trait ReplicaClient: Send + Sync {
    /// Returns all LTX files for `level`, sorted ascending by `min_txid`, that
    /// start at or after `seek`.
    async fn ltx_files(&self, level: i32, seek: TXID) -> Result<Vec<FileInfo>>;

    /// Returns at most `limit` LTX files at or after `seek`.
    ///
    /// Remote clients can stop their object listing after they collect the
    /// requested prefix. The default keeps compatibility with local and custom
    /// clients.
    async fn ltx_files_bounded(
        &self,
        level: i32,
        seek: TXID,
        limit: usize,
    ) -> Result<Vec<FileInfo>> {
        let mut files = self.ltx_files(level, seek).await?;
        files.truncate(limit);
        Ok(files)
    }

    /// Reads an LTX file. Returns an `io::ErrorKind::NotFound` error (wrapped)
    /// if the file does not exist.
    async fn open_ltx_file(&self, level: i32, min_txid: TXID, max_txid: TXID) -> Result<Vec<u8>>;

    /// Reads bytes `[offset, offset+len)` of an LTX file. Paged restore fetches
    /// an object's trailing page index and then single page frames this way,
    /// without downloading the whole object. `offset` and `len` must lie within
    /// the object (the caller knows its size from the [`FileInfo`] listing); a
    /// short object yields the available bytes.
    ///
    /// The default reads the whole object and slices it, so a backend without a
    /// native ranged read still works; a remote backend overrides this with a
    /// ranged GET so the read cost is proportional to the range, not the object.
    async fn read_range(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        let bytes = self.open_ltx_file(level, min_txid, max_txid).await?;
        let start = (offset as usize).min(bytes.len());
        let end = start.saturating_add(len as usize).min(bytes.len());
        Ok(bytes[start..end].to_vec())
    }

    /// Writes an LTX file to the replica and returns its metadata.
    async fn write_ltx_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        data: &[u8],
    ) -> Result<FileInfo>;

    /// Deletes the given LTX files.
    async fn delete_ltx_files(&self, files: &[FileInfo]) -> Result<()>;

    /// Deletes all files on the replica.
    async fn delete_all(&self) -> Result<()>;
}
