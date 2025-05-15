use std::{fs, path::Path, sync::Arc};

use anyhow::Context as _;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::error;

use crate::{
  alarm_processor::{Alarm, AlarmError, AlarmProcessor},
  distributed_lock::LockGuard,
  node_state::NodeState,
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
  node_state: Arc<NodeState>,

  /// Guard for ensuring the uniqueness of the system cell in the cluster
  lock_guard: LockGuard,

  // SQLite replication to S3/MinIO
  replica: SqliteReplica,
}

impl SystemCell for S3SystemCell {}

#[async_trait(?Send)]
impl AlarmProcessor for S3SystemCell {
  fn get(&self, tenant: &str, cell_id: &str) -> Result<Alarm, AlarmError> {
    todo!()
  }

  fn delete(&self, tenant: &str, cell_id: &str) -> Result<(), AlarmError> {
    todo!()
  }

  fn set(
    &self,
    tenant: &str,
    cell_id: &str,
    scheduled_time_unix_ms: u64,
  ) -> Result<(), AlarmError> {
    todo!()
  }

  async fn dispatch(
    &mut self,
    current_timestamp: DateTime<Utc>,
    limit: u32,
  ) -> Result<(), AlarmError> {
    todo!()
  }
}

/// [`SystemCell`] for a single-node cluster.
pub struct StandaloneSystemCell {
  node_state: Arc<NodeState>,
  conn: rusqlite::Connection,
}

impl StandaloneSystemCell {
  pub fn new(
    node_state: Arc<NodeState>,
    data_dir: &Path,
  ) -> anyhow::Result<Self> {
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

    Ok(Self { node_state, conn })
  }
}

impl SystemCell for StandaloneSystemCell {}

#[async_trait(?Send)]
impl AlarmProcessor for StandaloneSystemCell {
  fn get(&self, tenant: &str, cell_id: &str) -> Result<Alarm, AlarmError> {
    let mut stmt = self.conn.prepare(
      "SELECT tenant, cell_id, scheduled_time_unix_ms FROM global_alarms WHERE tenant = ? AND cell_id = ?",
    )?;
    let alarm = stmt
      .query_row((tenant, cell_id), |row| {
        Ok(Alarm {
          tenant: row.get("tenant")?,
          cell_id: row.get("cell_id")?,
          scheduled_time_unix_ms: row.get("scheduled_time_unix_ms")?,
        })
      })
      .map_err(|e| match e {
        rusqlite::Error::QueryReturnedNoRows => AlarmError::AlarmNotFound,
        _ => AlarmError::SQLiteError(e),
      })?;

    Ok(alarm)
  }

  fn delete(&self, tenant: &str, cell_id: &str) -> Result<(), AlarmError> {
    let mut stmt = self
      .conn
      .prepare("DELETE FROM global_alarms WHERE tenant = ? AND cell_id = ?")?;
    let affected_rows = stmt.execute((tenant, cell_id))?;

    if affected_rows == 0 {
      Err(AlarmError::AlarmNotFound)
    } else {
      Ok(())
    }
  }

  fn set(
    &self,
    tenant: &str,
    cell_id: &str,
    scheduled_time_unix_ms: u64,
  ) -> Result<(), AlarmError> {
    let mut stmt = self
      .conn
      .prepare(
        "INSERT OR REPLACE INTO global_alarms (tenant, cell_id, scheduled_time_unix_ms) VALUES (?, ?, ?)",
      )?;
    stmt.insert((tenant, cell_id, scheduled_time_unix_ms))?;

    Ok(())
  }

  async fn dispatch(
    &mut self,
    current_timestamp: DateTime<Utc>,
    limit: u32,
  ) -> Result<(), AlarmError> {
    let tx = self.conn.transaction()?;

    let mut stmt = tx.prepare(
      "SELECT tenant, cell_id, scheduled_time_unix_ms FROM global_alarms WHERE scheduled_time_unix_ms <= ? ORDER BY scheduled_time_unix_ms LIMIT ?",
    )?;
    let rows = stmt.query_map(
      [current_timestamp.timestamp_millis() as i64, limit as i64],
      |row| {
        Ok(Alarm {
          tenant: row.get("tenant")?,
          cell_id: row.get("cell_id")?,
          scheduled_time_unix_ms: row.get("scheduled_time_unix_ms")?,
        })
      },
    )?;
    let alarms = rows.collect::<Result<Vec<_>, _>>()?;
    stmt.finalize()?;

    let futs = alarms.into_iter().map(|alarm| {
      let node_state = self.node_state.clone();

      async move {
        // If the local node is the owner of the cell responsible for the alarm,
        // spawn (or get) a Deno process and dispatch the alarm via UDS.
        if node_state
          .peer_manager
          .is_local_owner(&alarm.tenant, &alarm.cell_id)
        {
            node_state
            .process_manager
            .get_or_spawn_process(
              &alarm.tenant,
              &alarm.cell_id,
              node_state.clone(),
            )
            .await
            .inspect_err(|e| {
              error!(?alarm, error = ?e, "Failed to spawn or get Deno process for alarm");
            }).ok()?;

          // TODO: Dispatch the alarm via UDS

          return Some(alarm);
        }

        // Otherwise, find the target node in the cluster
        let candidates = node_state
          .peer_manager
          .get_cell_owners(&alarm.tenant, &alarm.cell_id);

        for candidate in candidates {
          // TODO: Send alarm to the candidate
          // If error, continue to the next candidate
          // If success, return the alarm
          return Some(alarm);
        }

        None
      }
    });

    let mut stmt =
      tx.prepare("DELETE FROM global_alarms WHERE tenant = ? AND cell_id = ?")?;
    for processed_alarm in futures::future::join_all(futs)
      .await
      .into_iter()
      .filter_map(|r| r)
    {
      stmt.execute([processed_alarm.tenant, processed_alarm.cell_id])?;
    }
    stmt.finalize()?;

    tx.commit()?;

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use std::{path::PathBuf, time::Duration};

  use super::*;

  #[test]
  fn test_standalone_system_cell() {
    let data_dir = tempfile::tempdir().unwrap();
    let config = crate::config::Config {
      data_dir: PathBuf::from(data_dir.path()),
      listen_addr: "127.0.0.1:8000".to_string(),
      advertise_addr: "127.0.0.1:8000".to_string(),
      internal_listen_addr: "127.0.0.1:8001".to_string(),
      s3_endpoint: None,
      s3_bucket: None,
      s3_region: None,
      s3_path: None,
      s3_access_key_id: None,
      s3_secret_access_key: None,
      heartbeat_interval: Duration::from_secs(30),
      staleness_threshold: Duration::from_secs(90),
      lock_guard_ttl: Duration::from_secs(30),
    };
    let node_state = NodeState::new_for_benchmark(config);
    let system_cell =
      StandaloneSystemCell::new(node_state, data_dir.path()).unwrap();

    let result = system_cell.get("mytenant", "foo").unwrap_err();
    assert!(matches!(result, AlarmError::AlarmNotFound), "{result:?}");

    let result = system_cell.delete("mytenant", "foo").unwrap_err();
    assert!(matches!(result, AlarmError::AlarmNotFound), "{result:?}");

    system_cell.set("mytenant", "foo", 1000).unwrap();

    let result = system_cell.get("mytenant", "foo").unwrap();
    assert_eq!(result.scheduled_time_unix_ms, 1000);
    let result = system_cell.get("mytenant", "different_cell").unwrap_err();
    assert!(matches!(result, AlarmError::AlarmNotFound), "{result:?}");

    system_cell.delete("mytenant", "foo").unwrap();

    let result = system_cell.get("mytenant", "foo").unwrap_err();
    assert!(matches!(result, AlarmError::AlarmNotFound), "{result:?}");
  }
}
