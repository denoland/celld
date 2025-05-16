use std::{path::Path, sync::Arc};

use tracing::{debug, info, warn};

use crate::{
  alarm_processor::AlarmProcessor,
  distributed_lock::LockGuard,
  node_state::NodeState,
  sqlite_replica::{create_empty_database, SqliteReplica},
};

pub const SYSTEM_TENANT: &str = "_system";
pub const SYSTEM_CELL_ID: &str = "main";

pub struct SystemCell {
  node_state: Arc<NodeState>,

  /// Guard for ensuring the uniqueness of the system cell in the cluster
  _lock_guard: LockGuard,

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
  pub async fn new(
    node_state: Arc<NodeState>,
    lock_guard: LockGuard,
  ) -> Result<Self, anyhow::Error> {
    let db_path = node_state
      .config
      .data_dir
      .join(SYSTEM_TENANT)
      .join("sqlite")
      .join(format!("{SYSTEM_CELL_ID}.db"));

    let maybe_replica =
      setup_sqlite_replica(node_state.clone(), &lock_guard, &db_path).await?;

    Ok(Self {
      node_state,
      _lock_guard: lock_guard,
      replica: maybe_replica,
      alarm_processor: AlarmProcessor::new(&db_path)?,
    })
  }

  pub fn alarm_processor(&self) -> &AlarmProcessor {
    &self.alarm_processor
  }
}

async fn setup_sqlite_replica(
  node_state: Arc<NodeState>,
  lock_guard: &LockGuard,
  db_path: &Path,
) -> Result<Option<SqliteReplica>, anyhow::Error> {
  let maybe_replica = match SqliteReplica::initialize(
    &node_state.config.data_dir,
    SYSTEM_TENANT,
    SYSTEM_CELL_ID,
    node_state.config.to_s3_config(),
  )
  .await
  {
    Ok(replica_opt) => {
      if replica_opt.is_some() {
        debug!(
          tenant = %SYSTEM_TENANT,
          cell_id = %SYSTEM_CELL_ID,
          "S3 replication initialized successfully"
        );
      }
      replica_opt
    }
    Err(e) => {
      warn!(
        tenant = %SYSTEM_TENANT,
        cell_id = %SYSTEM_CELL_ID,
        error = %e,
        "Fatal error initializing replica"
      );
      None
    }
  };

  // Handle database restoration if necessary
  if let Some(ref replica) = maybe_replica {
    info!(
      tenant = %SYSTEM_TENANT,
      cell_id = %SYSTEM_CELL_ID,
      "Starting coordinated database restore process"
    );

    // Call ensure_restored to perform the restore with distributed locking
    replica.ensure_restored(&lock_guard).await;
    replica.start_replication().await?;
  } else if !db_path.exists() {
    // No replica, but we still need a database - create an empty one
    debug!(
      tenant = %SYSTEM_TENANT,
      cell_id = %SYSTEM_CELL_ID,
      "No S3 replication, creating empty database"
    );

    if let Err(e) = create_empty_database(&db_path) {
      warn!("Failed to create empty database file: {}", e);
    }
  } else {
    // No replica and the database already exists
    debug!(
      tenant = %SYSTEM_TENANT,
      cell_id = %SYSTEM_CELL_ID,
      db_path = %db_path.display(),
      "No replica and database already exists, using existing database"
    );
  }

  Ok(maybe_replica)
}
