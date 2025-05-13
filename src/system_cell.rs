use std::{fs, path::Path};

use anyhow::Context as _;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{
  alarm_processor::{Alarm, AlarmError, AlarmProcessor},
  distributed_lock::LockGuard,
  sqlite_replica::SqliteReplica,
};

pub const SYSTEM_TENANT: &str = "_system";
pub const SYSTEM_CELL_ID: &str = "main";

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
  conn: rusqlite::Connection,
}

impl StandaloneSystemCell {
  pub fn new(data_dir: &Path) -> anyhow::Result<Self> {
    let sqlite_dir = data_dir.join(SYSTEM_TENANT).join("sqlite");
    fs::create_dir_all(&sqlite_dir)
      .context("Failed to create sqlite directory")?;
    let db_path = sqlite_dir.join(format!("{SYSTEM_CELL_ID}.db"));
    let conn = rusqlite::Connection::open(db_path)?;

    conn.execute(
      "CREATE TABLE IF NOT EXISTS global_alarms (
        tenant TEXT NOT NULL,
        cell_id TEXT NOT NULL,
        scheduled_time_unix_ms INTEGER NOT NULL,
        PRIMARY KEY (tenant, cell_id)
      )",
      (),
    )?;

    conn.execute(
      "CREATE INDEX IF NOT EXISTS idx_global_alarms_schedule_time ON global_alarms (scheduled_time_unix_ms)",
      (),
    )?;

    Ok(Self { conn })
  }
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
