use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{
  alarm_processor::{Alarm, AlarmError, AlarmProcessor},
  distributed_lock::LockGuard,
  sqlite_replica::SqliteReplica,
};

/// The system cell is a special cell that is used to store global state for the
/// cluster. It has a unique tenant name `_system` and a unique cell id `main`.
///
/// The key difference between this and a regular cell is that the system cell
/// does not have a Deno process associated with it.
pub trait SystemCell: AlarmProcessor {}

/// S3-based implementation of [`SystemCell`].
pub struct S3SystemCell {
  /// Guard for ensuring the uniqueness of the system cell in the cluster
  lock_guard: LockGuard,
  // SQLite replication to S3/MinIO
  replica: SqliteReplica,
}

impl SystemCell for S3SystemCell {}

#[async_trait]
impl AlarmProcessor for S3SystemCell {
  async fn get(
    &self,
    tenant: &str,
    cell_id: &str,
  ) -> Result<Alarm, AlarmError> {
    todo!()
  }

  async fn delete(
    &self,
    tenant: &str,
    cell_id: &str,
  ) -> Result<Alarm, AlarmError> {
    todo!()
  }

  async fn set(
    &self,
    tenant: &str,
    cell_id: &str,
    scheduled_time_unix_ms: u64,
  ) -> Result<(), AlarmError> {
    todo!()
  }

  async fn dispatch(
    &self,
    current_timestamp: DateTime<Utc>,
  ) -> Result<(), AlarmError> {
    todo!()
  }
}

/// [`SystemCell`] for a single-node cluster.
pub struct StandaloneSystemCell {
  // TODO: add sqlite client
}

impl SystemCell for StandaloneSystemCell {}

#[async_trait]
impl AlarmProcessor for StandaloneSystemCell {
  async fn get(
    &self,
    tenant: &str,
    cell_id: &str,
  ) -> Result<Alarm, AlarmError> {
    todo!()
  }

  async fn delete(
    &self,
    tenant: &str,
    cell_id: &str,
  ) -> Result<Alarm, AlarmError> {
    todo!()
  }

  async fn set(
    &self,
    tenant: &str,
    cell_id: &str,
    scheduled_time_unix_ms: u64,
  ) -> Result<(), AlarmError> {
    todo!()
  }

  async fn dispatch(
    &self,
    current_timestamp: DateTime<Utc>,
  ) -> Result<(), AlarmError> {
    todo!()
  }
}
