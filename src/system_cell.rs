use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::{
  alarm_processor::AlarmProcessor, distributed_lock::LockGuard,
  node_state::NodeState, sqlite_replica::SqliteReplica,
};

pub const SYSTEM_TENANT: &str = "_system";
pub const SYSTEM_CELL_ID: &str = "main";

pub struct SystemCell {
  node_state: Arc<NodeState>,

  /// Guard for ensuring the uniqueness of the system cell in the cluster
  lock_guard: LockGuard,

  /// SQLite replication to S3/MinIO
  /// `None` if S3/MinIO is not configured (i.e. standalone mode)
  replica: Option<SqliteReplica>,

  alarm_processor: AlarmProcessor,
}

/// The system cell is a special cell that is used to store global state for the
/// cluster. It has a unique tenant name `_system` and a unique cell id `main`.
///
/// The key difference between this and a regular cell is that the system cell
/// does not have a Deno process associated with it.
impl SystemCell {
  pub fn new(
    node_state: Arc<NodeState>,
    lock_guard: LockGuard,
  ) -> Result<Self, anyhow::Error> {
    let db_path = node_state
      .config
      .data_dir
      .join(SYSTEM_TENANT)
      .join("sqlite")
      .join(format!("{SYSTEM_CELL_ID}.db"));

    // TODO: SQLite replication

    Ok(Self {
      node_state,
      lock_guard,
      replica: None,
      alarm_processor: AlarmProcessor::new(&db_path)?,
    })
  }

  pub async fn dispatch_due_alarms(
    &self,
    current_timestamp: DateTime<Utc>,
    limit: u32,
  ) -> Result<(), anyhow::Error> {
    self
      .alarm_processor
      .dispatch(self.node_state.clone(), current_timestamp, limit)
      .await?;
    Ok(())
  }
}
