use anyhow::{Context, Result};
use dashmap::DashMap;
use if_chain::if_chain;
use nix::sys::signal::Signal;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::net::UnixStream;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, trace, warn};
use uuid::Uuid;

use crate::active_connections;
use crate::alarm_processor::AlarmProcessor;
use crate::child_on_parent_exit::ChildOnParentExit;
use crate::control_socket_listener::ControlSocket;
use crate::distributed_lock::LockAcquireError;
use crate::distributed_lock::LockHandle;
use crate::distributed_lock::LockInfo;
use crate::distributed_lock::LockStateKind;
use crate::peer_manager::PeerManager;
use crate::pingora::prelude::*;
use crate::router::ProxyError;
use crate::sqlite_replica::{create_empty_database, SqliteReplica};
use crate::NodeState;

pub const SYSTEM_TENANT: &str = "_system";
pub const SYSTEM_CELL_ID: &str = "main";

#[derive(Debug)]
pub struct CellEntry {
  /// SQLite replication to S3/MinIO
  /// `None` if S3/MinIO is not configured
  replica: Option<SqliteReplica>,

  inner: CellEntryInner,
}

impl CellEntry {
  pub async fn terminate(&mut self) {
    match &mut self.inner {
      CellEntryInner::Normal {
        parent_exit_guard, ..
      } => {
        parent_exit_guard.kill(Signal::SIGTERM);
        parent_exit_guard.wait();
      }
      CellEntryInner::SystemMain { alarm_processor } => {
        if let Err(e) = alarm_processor.handle().shutdown().await {
          error!(
            error = ?e,
            "Error shutting down alarm processor"
          );
        }
      }
    }

    if let Some(replica) = &mut self.replica {
      if let Err(e) = replica.shutdown().await {
        error!(
          error = ?e,
          "Error shutting down Litestream replication"
        );
      }
    }
  }

  /// Get the UDS path through which the cell is listening for incoming HTTP
  /// requests.
  pub fn get_socket_path(&self) -> Option<&Path> {
    match &self.inner {
      CellEntryInner::Normal { socket_path, .. } => Some(socket_path.as_path()),
      CellEntryInner::SystemMain { .. } => None,
    }
  }

  /// Get the alarm processor if this is the system main cell.
  pub fn alarm_processor(&self) -> Option<&AlarmProcessor> {
    match &self.inner {
      CellEntryInner::SystemMain { alarm_processor } => Some(alarm_processor),
      CellEntryInner::Normal { .. } => None,
    }
  }

  /// Returns true if the cell is considered idle.
  /// Note that the system main cell is never considered idle. The only case
  /// where it is evicted is when the system main cell needs to be relocated to
  /// another node in the cluster.
  pub fn is_idle(&self, idle_timeout: Duration) -> bool {
    match &self.inner {
      CellEntryInner::SystemMain { .. } => false,
      CellEntryInner::Normal {
        incoming_connections,
        pid,
        last_used,
        ..
      } => {
        let now = std::time::Instant::now();
        if now.duration_since(*last_used) < idle_timeout {
          return false;
        }

        *incoming_connections == 0 || active_connections::count(*pid) == 0
      }
    }
  }
}

impl Drop for CellEntry {
  fn drop(&mut self) {
    match &mut self.inner {
      CellEntryInner::Normal {
        parent_exit_guard, ..
      } => {
        let finished = matches!(parent_exit_guard.try_wait(), Ok(Some(_)));
        if !finished {
          parent_exit_guard.kill(Signal::SIGKILL);
          parent_exit_guard.wait();
        }
      }
      CellEntryInner::SystemMain { alarm_processor } => {
        alarm_processor.kill();
      }
    }

    if let Some(replica) = self.replica.take() {
      // `kill_on_drop` flag is enabled for `litestream replicate` process. So
      // dropping the `replica` will send SIGKILL to the process, if the process
      // is still running.
      drop(replica);
    }
  }
}

#[derive(Debug)]
enum CellEntryInner {
  SystemMain {
    alarm_processor: AlarmProcessor,
  },
  Normal {
    pid: u32,
    socket_path: PathBuf,
    last_used: Instant,
    /// Number of current websocket connections to this process
    /// This is used to determine if the process should be kept alive
    incoming_connections: usize,
    /// Guard for automatic termination on parent exit
    parent_exit_guard: ChildOnParentExit,
    /// Keep tempdir alive as long as process exists
    _socket_tempdir: TempDir,
  },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CellKey {
  host: String,
  cell_id: String,
}

impl CellKey {
  fn new(host: impl ToString, cell_id: impl ToString) -> Self {
    CellKey {
      host: host.to_string(),
      cell_id: cell_id.to_string(),
    }
  }
}

pub struct CellManager {
  pub data_dir: PathBuf,
  pub cells: DashMap<CellKey, LockHandle>,
  control_socket_path: PathBuf,
}

impl CellManager {
  pub fn new(data_dir: PathBuf, control_socket: &ControlSocket) -> Self {
    let data_dir = data_dir.clone();
    // TODO: Implement a cleanup mechanism for old empty database files
    // This could be done by:
    // 1. Tracking database access times
    // 2. Running a periodic cleanup job that removes databases that haven't been
    //    accessed for a long time (e.g., weeks or months)
    // 3. Only removing databases that are successfully backed up to S3/MinIO
    CellManager {
      data_dir: std::fs::canonicalize(data_dir.clone()).unwrap(),
      cells: DashMap::new(),
      control_socket_path: control_socket.socket_path.clone(),
    }
  }

  /// Get the handle to the system main cell if exists.
  pub async fn get_system_main_cell(&self) -> Option<LockHandle> {
    let system_main_cell_key = CellKey::new(SYSTEM_TENANT, SYSTEM_CELL_ID);

    // Get the clone of the key and handle with minimum lock contention
    let (key, handle) = {
      let entry = self.cells.get(&system_main_cell_key)?;
      let key = entry.key().clone();
      let handle = entry.value().clone();
      (key, handle)
    };

    let (maybe_handle, should_remove) = {
      match handle.ping().await {
        Ok(status) => match status {
          LockStateKind::Init => {
            unreachable!("CellEntry should be inserted to self.cells after it transitioned to Active state");
          }
          LockStateKind::Active => (Some(handle), false),
          LockStateKind::Releasing => (None, false),
          LockStateKind::Released => (None, true),
        },
        Err(e) => {
          error!(
            error = ?e,
            ?key,
            "Error pinging system main cell, removing it from cells"
          );
          (None, true)
        }
      }
    };

    if should_remove {
      self.cells.remove(&system_main_cell_key);
    }

    maybe_handle
  }

  #[instrument(skip(self, node_state))]
  pub async fn ensure_system_main_cell_spawned(
    &self,
    node_state: Arc<NodeState>,
  ) -> anyhow::Result<()> {
    let retry_count = node_state.config.system_main_cell_spawn_retries;
    let retry_delay = node_state.config.system_main_cell_retry_delay;

    // Retry with configurable count and interval to account for the delay in
    // the propagation of cluster membership change.
    // We may want to adjust this number based on the actual delay and what
    // value is set as the heartbeat interval.
    for _ in 0..retry_count {
      if self.get_system_main_cell().await.is_some() {
        return Ok(());
      }

      match self.spawn_system_main_cell(node_state.clone()).await {
        Ok(()) => {
          return Ok(());
        }
        Err(e) => {
          error!(
            error = ?e,
            "Failed to spawn system main cell"
          );
          sleep(retry_delay).await;
        }
      }
    }

    Err(anyhow::anyhow!("Failed to spawn system main cell"))
  }

  #[instrument(skip(self, node_state))]
  async fn spawn_system_main_cell(
    &self,
    node_state: Arc<NodeState>,
  ) -> Result<(), CellManagerError> {
    let cell_key = CellKey::new(SYSTEM_TENANT, SYSTEM_CELL_ID);
    let lock_name = format!("{SYSTEM_TENANT}/{SYSTEM_CELL_ID}");
    let node_id = node_state.peer_manager.get_local_node_id();
    let lock_handle = node_state
      .distributed_lock
      .clone()
      .try_acquire(
        &lock_name,
        node_id,
        node_state.config.lock_guard_ttl_global,
        node_state.config.lock_guard_ttl_local,
        node_state.distributed_lock.clone(),
      )
      .await
      .map_err(|e| {
        tracing::warn!(
          tenant = SYSTEM_TENANT,
          cell_id = SYSTEM_CELL_ID,
          lock_name,
          ?node_id,
          error = ?e,
          "Failed to acquire lock on cell"
        );
        match e {
          LockAcquireError::LockHeld(maybe_lock_info) => {
            match maybe_lock_info {
              Some(lock_info) if lock_info.node_id == *node_id => {
                CellManagerError::CellCreationInProgress
              }
              _ => CellManagerError::LockContention(maybe_lock_info),
            }
          }
          LockAcquireError::UnableToRenewExpiredLock(_) => {
            // This error should never happen here
            CellManagerError::Internal(e.into())
          }
          LockAcquireError::S3Error(e) => CellManagerError::S3(e.to_string()),
          LockAcquireError::SerdeError(e) => CellManagerError::Serde(e),
        }
      })?;

    let tenant_dir = self.data_dir.join(SYSTEM_TENANT);

    let sqlite_dir = tenant_dir.join("sqlite");
    std::fs::create_dir_all(&sqlite_dir).unwrap_or_else(|e| {
      warn!("Failed to create sqlite directory: {}", e);
    });

    let db_path = sqlite_dir.join(format!("{SYSTEM_CELL_ID}.db"));

    let replica = self
      .setup_sqlite_replica(
        SYSTEM_TENANT,
        SYSTEM_CELL_ID,
        node_state.clone(),
        &lock_handle,
        &db_path,
      )
      .await?;

    let entry = CellEntry {
      inner: CellEntryInner::SystemMain {
        alarm_processor: AlarmProcessor::new(&db_path)?,
      },
      replica: replica.clone(),
    };

    lock_handle.set_resource(entry).await?;

    self.cells.insert(cell_key, lock_handle);

    Ok(())
  }

  /// Terminate cells (including both normal cells and the system main cell)
  /// that are no longer owned by this node according the the peer manager.
  pub async fn terminate_unowned_cells(&self, peer_manager: &PeerManager) {
    let mut released_keys = HashSet::new();

    for entry in &self.cells {
      let key = entry.key();
      if !peer_manager.is_local_owner(&key.host, &key.cell_id) {
        entry.value().release().await;
        released_keys.insert(key.clone());
      }
    }

    self.cells.retain(|key, _| !released_keys.contains(key));
  }

  fn get_cell_handle(&self, cell_key: &CellKey) -> Option<LockHandle> {
    let entry = self.cells.get(cell_key)?;
    Some(entry.value().clone())
  }

  /// Track a new connection to the process
  pub async fn increment_connection_count(&self, cell_key: &CellKey) -> bool {
    let Some(handle) = self.get_cell_handle(cell_key) else {
      return false;
    };

    match handle
      .mutate_resource(Box::new(|cell_entry| match cell_entry.inner {
        CellEntryInner::Normal {
          ref mut incoming_connections,
          ref mut last_used,
          ..
        } => {
          *incoming_connections += 1;
          *last_used = Instant::now();
        }
        CellEntryInner::SystemMain { .. } => {}
      }))
      .await
    {
      Ok(()) => true,
      Err(e) => {
        error!(
          error = ?e,
          "Error incrementing connection count"
        );
        false
      }
    }
  }

  /// Track a closed connection to the process
  pub async fn decrement_connection_count(&self, cell_key: &CellKey) -> bool {
    let Some(handle) = self.get_cell_handle(cell_key) else {
      return false;
    };

    match handle
      .mutate_resource(Box::new(|cell_entry| match cell_entry.inner {
        CellEntryInner::Normal {
          ref mut incoming_connections,
          ref mut last_used,
          ..
        } => {
          assert!(*incoming_connections > 0);
          *incoming_connections -= 1;
          *last_used = Instant::now();
        }
        CellEntryInner::SystemMain { .. } => {}
      }))
      .await
    {
      Ok(()) => true,
      Err(e) => {
        error!(
          error = ?e,
          "Error decrementing connection count"
        );
        false
      }
    }
  }

  #[instrument(skip(self, node_state), fields(host = %host, cell_id = %cell_id))]
  pub async fn get_or_spawn_normal_cell(
    &self,
    host: &str,
    cell_id: &str,
    node_state: Arc<NodeState>,
  ) -> Result<(PathBuf, UnixStream, CellKey), CellManagerError> {
    // Create a combined key for host and cell to ensure one isolate per cell
    let cell_key = CellKey::new(host, cell_id);

    // First, try to find and connect to an existing ready cell
    if_chain! {
      if let Some(handle) = self.get_cell_handle(&cell_key);
      if let Ok(Some(socket_path)) = handle.get_socket_path().await;
      if let Ok(stream) = UnixStream::connect(&socket_path).await;
      then {
        debug!(
          socket = %socket_path.display(),
          "Connected to existing cell socket"
        );
        return Ok((socket_path, stream, cell_key));
      }
    }

    // Fall through to spawn a new cell

    // checked higher up, but done again here for safety
    assert!(!host.contains('/') && !host.contains(".."));

    // Always compute tenant_dir for SQLite and process spawning
    let tenant_dir = self.data_dir.join(host);

    // Determine the source file path
    let main_script =
      if let Some(ref single_tenant) = node_state.config.single_tenant {
        // In single-tenant mode, use the specified source file
        single_tenant.src_file.clone()
      } else {
        // In multi-tenant mode, use the standard path structure
        let app_code_dir = tenant_dir.join("src");
        app_code_dir.join("main.ts") // TODO support main.js
      };

    if !main_script.exists() {
      error!("Application code not found at {}", main_script.display());
      return Err(CellManagerError::Internal(
        ProxyError::AppNotFound(host.to_string()).into(),
      ));
    }

    // Acquire a lock on the cell to declare ownership of the combination of
    // tenant and cellId.
    let lock_name = format!("{host}/{cell_id}");
    let node_id = node_state.peer_manager.get_local_node_id();
    let lock_handle = node_state
      .distributed_lock
      .clone()
      .try_acquire(
        &lock_name,
        node_id,
        node_state.config.lock_guard_ttl_global,
        node_state.config.lock_guard_ttl_local,
        node_state.distributed_lock.clone(),
      )
      .await
      .map_err(|e| {
        tracing::warn!(
          tenant = host,
          cell_id,
          lock_name,
          ?node_id,
          error = ?e,
          "Failed to acquire lock on cell"
        );
        match e {
          LockAcquireError::LockHeld(maybe_lock_info) => {
            match maybe_lock_info {
              Some(lock_info) if lock_info.node_id == *node_id => {
                CellManagerError::CellCreationInProgress
              }
              _ => CellManagerError::LockContention(maybe_lock_info),
            }
          }
          LockAcquireError::UnableToRenewExpiredLock(_) => {
            // This error should never happen here
            CellManagerError::Internal(e.into())
          }
          LockAcquireError::S3Error(e) => CellManagerError::S3(e.to_string()),
          LockAcquireError::SerdeError(e) => CellManagerError::Serde(e),
        }
      })?;

    // Create a temporary directory for the socket
    // This will be automatically cleaned up when dropped
    let socket_tempdir = tempfile::tempdir()
      .with_context(|| "Failed to create temporary directory for socket")?;

    let serve_socket_name = {
      let uuid_string = Uuid::new_v4().to_string();
      let first_segment: &str = &uuid_string[0..8];
      format!("{first_segment}.sock")
    };
    let serve_socket_path = socket_tempdir.path().join(serve_socket_name);

    // Create SQLite directory
    let sqlite_dir = tenant_dir.join("sqlite");
    std::fs::create_dir_all(&sqlite_dir).unwrap_or_else(|e| {
      warn!("Failed to create sqlite directory: {}", e);
    });

    // Configure SQLite replication if S3 is configured
    let db_path = sqlite_dir.join(format!("{cell_id}.db"));

    let replica = self
      .setup_sqlite_replica(
        host,
        cell_id,
        node_state.clone(),
        &lock_handle,
        &db_path,
      )
      .await?;

    let spawn_start = Instant::now();

    debug!(
      tenant = %host,
      cell_id = %cell_id,
      socket_path = %serve_socket_path.display(),
      main_script = %main_script.display(),
      "About to spawn Deno process"
    );

    // Spawn the Deno process
    let mut child_guard = spawn_deno_process(
      host,
      cell_id,
      &tenant_dir,
      &serve_socket_path,
      &self.control_socket_path,
      &main_script,
      &node_state.config,
    )?;

    debug!(
      tenant = %host,
      cell_id = %cell_id,
      pid = child_guard.pid().unwrap_or(0),
      "Deno process spawned successfully"
    );

    // Convert to tokio::process::Child using the PID
    let pid = child_guard.pid().unwrap() as u32;

    // --- Wait for the socket to become available (crucial for cold start) ---
    let socket_ = serve_socket_path.clone();
    let wait_start = Instant::now();
    let wait_timeout = Duration::from_secs(10); // Timeout for socket connection

    // Use minimal polling for fastest possible connection with exponential backoff
    let mut delay = Duration::from_micros(50);
    let max_delay = Duration::from_millis(20);

    // Wait for the socket to be available and connect to it
    let stream = loop {
      // Check if Deno early exited before the timeout
      if let Some(status) = child_guard.try_wait().unwrap_or(None) {
        error!(
          pid = pid,
          socket = %socket_.display(),
          tenant = %host,
          cell_id = %cell_id,
          "Deno process exited early {}. Use CELL_DENO_OUTPUT=1 to see output",
          status
        );

        lock_handle.release().await;

        // The tempdir will be dropped at the end of this scope, cleaning up the socket file
        return Err(
          anyhow::anyhow!("Deno process exited before socket became available")
            .into(),
        );
      }

      if wait_start.elapsed() > wait_timeout {
        error!(
          pid = pid,
          socket = %socket_.display(),
          tenant = %host,
          cell_id = %cell_id,
          "Timeout waiting for Deno process socket"
        );

        // Remove the entry from the map to avoid stale entries
        if let Some((_, handle)) = self.cells.remove(&cell_key) {
          handle.release().await;
        }

        // The tempdir will be dropped at the end of this scope, cleaning up the socket file
        return Err(
          anyhow::anyhow!("Timeout waiting for process socket").into(),
        );
      }

      debug!(
        pid = pid,
        socket = %socket_.display(),
        tenant = %host,
        cell_id = %cell_id,
        elapsed = ?wait_start.elapsed(),
        "Attempting to connect to Deno socket"
      );

      match UnixStream::connect(&socket_).await {
        Ok(stream) => {
          debug!(
            pid = pid,
            socket = %socket_.display(),
            tenant = %host,
            cell_id = %cell_id,
            socket_wait_duration = ?wait_start.elapsed(),
            total_startup_duration = ?spawn_start.elapsed(),
            "Socket connected!"
          );
          // We have a connected socket
          break stream; // Socket is ready and connected, return the stream
        }
        Err(ref e)
          if e.kind() == std::io::ErrorKind::ConnectionRefused
            || e.kind() == std::io::ErrorKind::NotFound =>
        {
          trace!(
            pid = pid,
            socket = %socket_.display(),
            tenant = %host,
            cell_id = %cell_id,
            error = %e,
            error_kind = ?e.kind(),
            "Socket not yet available, retrying after delay"
          );
          // Socket not ready yet, use minimal polling with exponential backoff
          sleep(delay).await;
          // Increase delay with exponential backoff, but cap at max_delay
          delay = std::cmp::min(delay * 2, max_delay);
        }
        Err(e) => {
          error!(
            pid = pid,
            socket = %socket_.display(),
            error = %e,
            "Error connecting to socket during startup"
          );

          // Remove the entry from the map to avoid stale entries
          if let Some((_, handle)) = self.cells.remove(&cell_key) {
            handle.release().await;
          }

          // The tempdir will be dropped at the end of this scope, cleaning up the socket file
          return Err(
            anyhow::anyhow!("Error connecting to process socket: {}", e).into(),
          );
        }
      }
    };

    // Create a new CellEntry with proper values
    let entry = CellEntry {
      inner: CellEntryInner::Normal {
        pid,
        socket_path: serve_socket_path.clone(),
        last_used: Instant::now(),
        incoming_connections: 0,
        parent_exit_guard: child_guard,
        _socket_tempdir: socket_tempdir,
      },
      replica: replica.clone(),
    };

    lock_handle.set_resource(entry).await?;

    self.cells.insert(cell_key.clone(), lock_handle);

    Ok((serve_socket_path, stream, cell_key))
  }

  pub async fn terminate_all(&self) {
    let num_cells = self.cells.len();

    debug!("Terminating {num_cells} cells");

    // TODO(magurotuna): Can we do this concurrently?
    for handle in self.cells.iter().map(|entry| entry.value().clone()) {
      debug!(descriptor = ?handle.descriptor(), "Cell termination starts");

      handle.release().await;
    }

    debug!("{num_cells} cells terminated");
  }

  async fn setup_sqlite_replica(
    &self,
    host: &str,
    cell_id: &str,
    node_state: Arc<NodeState>,
    lock_handle: &LockHandle,
    db_path: &Path,
  ) -> Result<Option<SqliteReplica>, CellManagerError> {
    // Try to initialize the SqliteReplica with the S3 config
    let mut replica = match SqliteReplica::initialize(
      &self.data_dir,
      host,
      cell_id,
      node_state.config.to_s3_config(),
    )
    .await
    {
      Ok(replica_opt) => {
        if replica_opt.is_some() {
          debug!(
            tenant = %host,
            cell_id = %cell_id,
            "S3 replication initialized successfully"
          );
        }
        replica_opt
      }
      Err(e) => {
        warn!(
          tenant = %host,
          cell_id = %cell_id,
          error = %e,
          "Fatal error initializing replica"
        );
        None
      }
    };

    // Handle database restoration if necessary
    if let Some(ref mut replica) = replica {
      info!(
        tenant = %host,
        cell_id = %cell_id,
        "Starting coordinated database restore process"
      );

      // Call ensure_restored to perform the restore with distributed locking
      replica.ensure_restored(lock_handle).await;
      replica.start_replication().await?;
    } else if !db_path.exists() {
      // No replica, but we still need a database - create an empty one
      debug!(
        tenant = %host,
        cell_id = %cell_id,
        "No S3 replication, creating empty database"
      );

      if let Err(e) = create_empty_database(db_path) {
        warn!("Failed to create empty database file: {}", e);
      }
    } else {
      // No replica and the database already exists
      debug!(
        tenant = %host,
        cell_id = %cell_id,
        db_path = %db_path.display(),
        "No replica and database already exists, using existing database"
      );
    }

    Ok(replica)
  }
}

#[derive(Debug, thiserror::Error)]
pub enum CellManagerError {
  #[error("Cell creation in progress; retry later")]
  CellCreationInProgress,
  #[error("Cell lock held by another node: {0:?}")]
  LockContention(Option<LockInfo>),
  #[error("S3 operation failed: {0}")]
  S3(String),
  #[error(transparent)]
  Serde(serde_json::Error),
  #[error("Internal error: {0}")]
  Internal(#[from] anyhow::Error),
}

impl From<CellManagerError> for Box<Error> {
  fn from(e: CellManagerError) -> Self {
    use CellManagerError::*;
    match e {
      CellCreationInProgress => Error::explain(
        ErrorType::HTTPStatus(http::StatusCode::INTERNAL_SERVER_ERROR.into()),
        "Failed to get or spawn cell",
      ),
      LockContention(_) => Error::explain(
        ErrorType::HTTPStatus(http::StatusCode::INTERNAL_SERVER_ERROR.into()),
        "Cell is being handled by another node",
      ),
      S3(_) => Error::explain(
        ErrorType::HTTPStatus(http::StatusCode::INTERNAL_SERVER_ERROR.into()),
        "S3 operation failed",
      ),
      Serde(_) => Error::explain(
        ErrorType::HTTPStatus(http::StatusCode::INTERNAL_SERVER_ERROR.into()),
        "Failed to serialize or deserialize lock data",
      ),
      Internal(_) => Error::explain(
        ErrorType::HTTPStatus(http::StatusCode::INTERNAL_SERVER_ERROR.into()),
        "Internal server error during cell lookup",
      ),
    }
  }
}

fn parse_env_vars(content: &str) -> HashMap<String, String> {
  let mut env_vars = HashMap::new();
  for line in content.lines() {
    // Skip empty lines and comment lines that start with #
    if line.trim().is_empty() || line.trim().starts_with('#') {
      continue;
    }

    if let Some((key, value)) = line.split_once('=') {
      let key = key.trim();

      // Handle inline comments in values (e.g., VALUE=foo # comment)
      let value = if let Some(comment_pos) = value.find('#') {
        value[..comment_pos].trim()
      } else {
        value.trim()
      };

      if !key.is_empty() {
        env_vars.insert(key.to_string(), value.to_string());
      }
    }
  }
  env_vars
}

/// Spawn a Deno process for the given host and cell
#[instrument(skip(serve_socket_path, control_socket_path), fields(host = %host, cell_id = %cell_id))]
fn spawn_deno_process(
  host: &str,
  cell_id: &str,
  tenant_dir: &PathBuf,
  // The socket path the Deno process will listen on for HTTP requests
  serve_socket_path: &Path,
  // The socket path the Deno process will connect to
  control_socket_path: &Path,
  main_script: &PathBuf,
  config: &crate::config::Config,
) -> Result<ChildOnParentExit> {
  let ctl_socket_canonicalized = std::fs::canonicalize(control_socket_path)?;

  let mut cmd = std::process::Command::new("deno");
  cmd
    .current_dir(tenant_dir)
    .env(
      "DENO_SERVE_ADDRESS",
      format!("unix:{}", serve_socket_path.display()),
    )
    .env(
      "CELL_CONTROL_SOCKET",
      format!("{}", ctl_socket_canonicalized.display()),
    )
    .env("X-Tenant", host)
    .env("X-Cell-Id", cell_id);

  // Load environment variables from prod.env file
  let env_file_path = tenant_dir.join("prod.env");
  // X-Cell-Id and CELL_CONTROL_SOCKET are allowed by default
  let mut env_vars = vec![
    "X-Tenant".to_string(),
    "X-Cell-Id".to_string(),
    "CELL_CONTROL_SOCKET".to_string(),
  ];

  match fs::read_to_string(&env_file_path) {
    Ok(env_contents) => {
      // Parse environment variables from the file contents
      let parsed_env_vars = parse_env_vars(&env_contents);

      // Apply the parsed variables to the command
      for (key, value) in &parsed_env_vars {
        debug!("Setting environment variable: {}", key);
        cmd.env(key, value);
        env_vars.push(key.clone());
      }
    }
    Err(e) => {
      if e.kind() != std::io::ErrorKind::NotFound {
        warn!(
          "Error reading environment file {}: {}",
          env_file_path.display(),
          e
        );
      }
    }
  }

  debug!(
    host = %host,
    cell_id = %cell_id,
    socket_path = %serve_socket_path.display(),
    main_script = %main_script.display(),
    "Preparing deno command"
  );

  cmd.arg("run").arg("--no-prompt");

  // Add --env-file argument if configured for single-tenant mode
  if let Some(ref single_tenant) = config.single_tenant {
    if let Some(ref env_file) = single_tenant.env_file {
      assert!(env_file.is_absolute(), "env_file path must be absolute");
      cmd.arg(format!("--env-file={}", env_file.display()));

      // Parse the env file to add variables to --allow-env
      match fs::read_to_string(env_file) {
        Ok(env_contents) => {
          let parsed_env_vars = parse_env_vars(&env_contents);
          for (key, _) in parsed_env_vars {
            env_vars.push(key);
          }
        }
        Err(e) => {
          warn!(
            "Error reading environment file {}: {}",
            env_file.display(),
            e
          );
        }
      }
    }
  }

  cmd
    .arg(format!(
      "--allow-read={},{},{}",
      tenant_dir.display(),
      serve_socket_path.display(),
      ctl_socket_canonicalized.display()
    ))
    .arg(format!(
      "--allow-write={},{},{}",
      tenant_dir.display(),
      serve_socket_path.display(),
      ctl_socket_canonicalized.display()
    ))
    .arg("--allow-net");

  // Only allow specifically named environment variables
  cmd.arg(format!("--allow-env={}", env_vars.join(",")));

  cmd.arg(main_script);

  // Eventually we'll want to collect Deno stdio output into an otel stream.
  if std::env::var_os("CELL_DENO_OUTPUT").is_none() {
    cmd
      .stdout(std::process::Stdio::null())
      .stderr(std::process::Stdio::null());
  }

  // Spawn with ChildOnParentExit for automatic termination when parent exits
  ChildOnParentExit::spawn(cmd).with_context(|| {
    format!("Failed to spawn Deno process for {host} with parent-exit guard")
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test_log::test]
  fn test_parse_env_vars() {
    // Test content with various environment variable formats
    let content = r#"
# This is a comment
SIMPLE_VAR=simple_value
QUOTED_VAR="quoted value"
INLINE_COMMENT=value with comment # this is a comment
"#;
    let env_vars = parse_env_vars(content);
    assert_eq!(
      env_vars.get("SIMPLE_VAR"),
      Some(&"simple_value".to_string())
    );
    assert_eq!(
      env_vars.get("QUOTED_VAR"),
      Some(&"\"quoted value\"".to_string())
    );
    assert_eq!(
      env_vars.get("INLINE_COMMENT"),
      Some(&"value with comment".to_string())
    );
  }
}
