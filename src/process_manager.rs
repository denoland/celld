use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::net::UnixStream;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::child_on_parent_exit::ChildOnParentExit;
use crate::sqlite_replica::{create_empty_database, SqliteReplica};
use crate::NodeState;
use crate::ProxyError;

/// Represents the current state of a database restore operation
#[derive(Debug, Clone, PartialEq)]
pub enum RestoreState {
  /// Initial state, no restore has been attempted
  Idle,
  /// Attempting to acquire a distributed lock
  AcquiringLock,
  /// Another node holds the lock, waiting for it to complete
  WaitingForLock,
  /// Actively restoring from backup
  Restoring,
  /// Restore process completed (bool indicates if data was actually restored)
  Complete(bool),
  /// Restore failed with the specified error message
  Failed(String),
}

pub struct ProcessEntry {
  pub pid: u32,
  pub socket_path: PathBuf,
  pub last_used: Instant,
  pub parent_exit_guard: ChildOnParentExit, // Guard for automatic termination on parent exit
  pub single_use: bool,                     // Flag for single-use isolates
  pub active_connections: u32, // Counter for active connections (including WebSockets)
  pub _socket_tempdir: TempDir, // Keep tempdir alive as long as process exists
  pub replica: Option<SqliteReplica>, // SQLite replication to S3/MinIO
  /// State of the database restore operation
  pub restore_state: RestoreState,
}

pub struct ProcessManager {
  pub data_dir: PathBuf,
  pub processes: Mutex<HashMap<String, ProcessEntry>>,
}

impl ProcessManager {
  pub fn new(data_dir: PathBuf) -> Self {
    let data_dir = data_dir.clone();
    // TODO: Implement a cleanup mechanism for old empty database files
    // This could be done by:
    // 1. Tracking database access times
    // 2. Running a periodic cleanup job that removes databases that haven't been
    //    accessed for a long time (e.g., weeks or months)
    // 3. Only removing databases that are successfully backed up to S3/MinIO
    ProcessManager {
      data_dir: std::fs::canonicalize(data_dir.clone()).unwrap(),
      processes: Mutex::new(HashMap::new()),
    }
  }

  /// Track a new connection to the process
  pub async fn increment_connection_count(
    &self,
    host: &str,
    room_id: &str,
  ) -> bool {
    let process_key = format!("{}:{}", host, room_id);

    let mut processes = self.processes.lock().unwrap();
    if let Some(entry) = processes.get_mut(&process_key) {
      entry.active_connections += 1;
      entry.last_used = Instant::now();
      return true;
    }

    false
  }

  /// Track a closed connection to the process
  pub async fn decrement_connection_count(
    &self,
    host: &str,
    room_id: &str,
  ) -> bool {
    let process_key = format!("{}:{}", host, room_id);

    let mut processes = self.processes.lock().unwrap();
    if let Some(entry) = processes.get_mut(&process_key) {
      if entry.active_connections > 0 {
        entry.active_connections -= 1;
      }
      entry.last_used = Instant::now();
      return true;
    }

    false
  }

  #[instrument(skip(self, node_state), fields(host = %host, room_id = %room_id))]
  pub async fn get_or_spawn_process(
    &self,
    host: &str,
    room_id: &str,
    single_use: bool,
    node_state: Arc<NodeState>,
  ) -> Result<(PathBuf, UnixStream), ProxyError> {
    // Create a combined key for host and room to ensure one isolate per room
    let process_key = format!("{}:{}", host, room_id);

    // For single_use requests, always spawn a new process
    // TODO: This should not be supported in production
    if !single_use {
      // First, try to find and connect to an existing process, keeping the lock time minimal
      let socket_path_opt = {
        let processes = self.processes.lock().unwrap();
        if let Some(entry) = processes.get(&process_key) {
          // Skip single-use entries when looking for a regular process
          if !entry.single_use {
            Some(entry.socket_path.clone())
          } else {
            None
          }
        } else {
          None
        }
      };

      // If we found a socket path, try to connect without holding the lock
      if let Some(socket_path) = socket_path_opt {
        match UnixStream::connect(&socket_path).await {
          Ok(stream) => {
            debug!(
              socket = %socket_path.display(),
              "Connected to existing process socket"
            );
            return Ok((socket_path, stream));
          }
          Err(e) => {
            error!(
              socket = %socket_path.display(),
              error = %e,
              "Failed to connect to existing process socket, spawn new one"
            );
            // Fall through to spawn a new process
          }
        }
      }
    }

    // Validate host format briefly (prevent directory traversal)
    if host.contains('/') || host == ".." {
      return Err(ProxyError::InvalidHost);
    }

    let tenant_dir = self.data_dir.join(host);
    let app_code_dir = tenant_dir.join("code");
    let main_script = app_code_dir.join("main.ts");

    if !main_script.exists() {
      warn!("Application code not found at {}", main_script.display());
      return Err(ProxyError::AppNotFound(host.to_string()));
    }

    // Create a temporary directory for the socket
    // This will be automatically cleaned up when dropped
    let socket_tempdir = tempfile::tempdir()
      .with_context(|| "Failed to create temporary directory for socket")?;

    let socket_name = {
      let uuid_string = Uuid::new_v4().to_string();
      let first_segment: &str = &uuid_string[0..8];
      format!("{}.sock", first_segment)
    };
    let socket_path = socket_tempdir.path().join(socket_name);

    // Create SQLite directory
    let sqlite_dir = tenant_dir.join("sqlite");
    std::fs::create_dir_all(&sqlite_dir).unwrap_or_else(|e| {
      warn!("Failed to create sqlite directory: {}", e);
    });

    // Configure SQLite replication if S3 is configured
    let db_path = sqlite_dir.join(format!("{}.db", room_id));

    // Try to initialize the SqliteReplica with the S3 config
    let replica = match SqliteReplica::initialize(
      &self.data_dir,
      host,
      room_id,
      node_state.config.to_s3_config(),
    )
    .await
    {
      Ok(replica_opt) => {
        if replica_opt.is_some() {
          debug!(
            tenant = %host,
            room_id = %room_id,
            "S3 replication initialized successfully"
          );
        }
        replica_opt
      }
      Err(e) => {
        warn!(
          tenant = %host,
          room_id = %room_id,
          error = %e,
          "Fatal error initializing replica"
        );
        None
      }
    };

    // Initialize restore state variable
    let restore_state;

    // Handle database restoration if necessary
    if let Some(ref replica) = replica {
      // Only attempt coordinated restore if we have a distributed lock manager
      let lock_manager = node_state.distributed_lock.as_ref().unwrap();
      let node_id = node_state.peer_manager.get_local_node_id();

      // Use a reasonable lock TTL (e.g., 30 seconds)
      let lock_ttl = std::time::Duration::from_secs(30);

      info!(
        tenant = %host,
        room_id = %room_id,
        "Starting coordinated database restore process"
      );

      // Call ensure_restored to perform the restore with distributed locking
      let restore_result = replica
        .ensure_restored(lock_manager.clone(), node_id, lock_ttl)
        .await?;

      // Update the restore state based on the result
      match restore_result {
        state @ RestoreState::Complete(_) => {
          info!(
            tenant = %host,
            room_id = %room_id,
            restored = match &state {
              RestoreState::Complete(restored) => *restored,
              _ => false
            },
            "Database restore completed successfully"
          );
          // Update state and continue to spawn Deno
          restore_state = state;
        }
        RestoreState::WaitingForLock => {
          info!(
            tenant = %host,
            room_id = %room_id,
            "Database restore lock held by another node"
          );
          // Update state and return error
          return Err(ProxyError::InternalError(anyhow!(
              "Database restore lock held by another node, please try again later"
            )));
        }
        state @ RestoreState::Failed(_) => {
          let err_msg = match &state {
            RestoreState::Failed(msg) => msg.clone(),
            _ => "Unknown error".to_string(),
          };
          error!(
            tenant = %host,
            room_id = %room_id,
            error = %err_msg,
            "Database restore failed"
          );
          // Update state and return error
          return Err(ProxyError::InternalError(anyhow!(
            "Database restore failed: {}",
            err_msg
          )));
        }
        _ => {
          // Unexpected state
          error!(
            tenant = %host,
            room_id = %room_id,
            "Unexpected database restore state"
          );
          // Update state and return error
          return Err(ProxyError::InternalError(anyhow!(
            "Unexpected database restore state"
          )));
        }
      }

      replica.start_replication().await?;
    } else if !db_path.exists() {
      // No replica, but we still need a database - create an empty one
      info!(
        tenant = %host,
        room_id = %room_id,
        "No S3 replication, creating empty database"
      );

      if let Err(e) = create_empty_database(&db_path) {
        warn!("Failed to create empty database file: {}", e);
      }

      // Update state to Complete(false) since we created a new empty DB
      restore_state = RestoreState::Complete(false);
    } else {
      // No replica and the database already exists
      info!(
        tenant = %host,
        room_id = %room_id,
        db_path = %db_path.display(),
        "No replica and database already exists, using existing database"
      );
      restore_state = RestoreState::Complete(false);
    }

    let spawn_start = Instant::now();

    debug!(
      tenant = %host,
      room_id = %room_id,
      socket_path = %socket_path.display(),
      main_script = %main_script.display(),
      "About to spawn Deno process"
    );

    // Spawn the Deno process
    let child_guard = spawn_deno_process(
      &self.data_dir,
      host,
      room_id,
      &tenant_dir,
      &socket_path,
      &main_script,
    )?;

    debug!(
      tenant = %host,
      room_id = %room_id,
      pid = child_guard.pid().unwrap_or(0),
      "Deno process spawned successfully"
    );

    // Convert to tokio::process::Child using the PID
    let pid = child_guard.pid().unwrap() as u32;

    // Create a new ProcessEntry with proper values
    let entry = ProcessEntry {
      pid,
      socket_path: socket_path.clone(),
      last_used: Instant::now(),
      parent_exit_guard: child_guard,
      single_use,
      active_connections: 0,
      _socket_tempdir: socket_tempdir,
      replica: replica.clone(),
      restore_state,
    };

    // Insert the entry into the processes map using a short-lived lock
    {
      let mut processes = self.processes.lock().unwrap();
      processes.insert(process_key.clone(), entry);
    }

    // --- Wait for the socket to become available (crucial for cold start) ---
    let socket_ = socket_path.clone();
    let wait_start = Instant::now();
    let wait_timeout = Duration::from_secs(10); // Timeout for socket connection

    // Use minimal polling for fastest possible connection with exponential backoff
    let mut delay = Duration::from_micros(50);
    let max_delay = Duration::from_millis(20);

    // Wait for the socket to be available and connect to it
    let stream = loop {
      if wait_start.elapsed() > wait_timeout {
        error!(
          pid = pid,
          socket = %socket_.display(),
          tenant = %host,
          room_id = %room_id,
          "Timeout waiting for Deno process socket"
        );

        // Remove the entry from the map to avoid stale entries
        let mut processes = self.processes.lock().unwrap();
        if let Some(entry) = processes.remove(&process_key) {
          // Kill the process to avoid zombies
          entry.parent_exit_guard.kill();
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
        room_id = %room_id,
        elapsed = ?wait_start.elapsed(),
        "Attempting to connect to Deno socket"
      );

      match UnixStream::connect(&socket_).await {
        Ok(stream) => {
          debug!(
            pid = pid,
            socket = %socket_.display(),
            tenant = %host,
            room_id = %room_id,
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
          debug!(
            pid = pid,
            socket = %socket_.display(),
            tenant = %host,
            room_id = %room_id,
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
          let mut processes = self.processes.lock().unwrap();
          if let Some(entry) = processes.remove(&process_key) {
            // Kill the process to avoid zombies
            entry.parent_exit_guard.kill();
          }

          // The tempdir will be dropped at the end of this scope, cleaning up the socket file
          return Err(
            anyhow::anyhow!("Error connecting to process socket: {}", e).into(),
          );
        }
      }
    };

    // For single-use isolates, use a unique key with a UUID suffix
    // This allows multiple single-use isolates for the same host+room combination
    if single_use {
      let unique_key = format!("{}:{}-{}", host, room_id, Uuid::new_v4());

      // Move the entry to a unique key
      let mut processes = self.processes.lock().unwrap();
      if let Some(entry) = processes.remove(&process_key) {
        processes.insert(unique_key, entry);
      }
    }

    Ok((socket_path, stream))
  }

  pub async fn kill_all(&self) {
    // Move all entries into a local collection to minimize lock duration
    let entries: Vec<_> = {
      let mut processes = self.processes.lock().unwrap();
      processes.drain().collect()
    };

    // Kill all processes without holding the lock
    for (_, entry) in entries {
      entry.parent_exit_guard.kill();
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

/// Spawn a Deno process for the given host and room
#[instrument(skip(data_dir, socket_path, room_id), fields(host = %host, room_id = %room_id))]
fn spawn_deno_process(
  data_dir: &Path,
  host: &str,
  room_id: &str,
  tenant_dir: &PathBuf,
  socket_path: &Path,
  main_script: &PathBuf,
) -> Result<ChildOnParentExit> {
  // Path to bootstrap.ts script
  let bootstrap_script =
    data_dir.parent().unwrap().join("src").join("bootstrap.ts");

  let mut cmd = std::process::Command::new("deno");
  cmd
    .current_dir(tenant_dir)
    .env(
      "DENO_SERVE_ADDRESS",
      format!("unix:{}", socket_path.display()),
    )
    .env("X-Room-Id", room_id); // Pass room ID to bootstrap.ts

  // Load environment variables from prod.env file
  let env_file_path = tenant_dir.join("prod.env");
  let mut env_vars = Vec::new();

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

  // Add X-Room-Id to allowed env vars
  env_vars.push("X-Room-Id".to_string());

  debug!(
    host = %host,
    room_id = %room_id,
    socket_path = %socket_path.display(),
    bootstrap_script = ?bootstrap_script.display(),
    main_script = %main_script.display(),
    "Preparing deno command"
  );

  cmd
    .arg("run")
    .arg("--no-prompt")
    .arg(format!("--allow-read={}", tenant_dir.display()))
    .arg(format!("--allow-write={}", tenant_dir.display()))
    .arg(format!("--allow-read={}", socket_path.display()))
    .arg(format!("--allow-write={}", socket_path.display()))
    .arg("--allow-net");

  // Only allow specifically named environment variables
  if !env_vars.is_empty() {
    cmd.arg(format!("--allow-env={}", env_vars.join(",")));
  } else {
    cmd.arg("--allow-env=X-Room-Id"); // Default minimum permission
  }

  cmd
    .arg(&bootstrap_script) // Use bootstrap.ts instead of main.ts directly
    .arg(main_script); // Pass main.ts as argument to bootstrap.ts

  // Spawn with ChildOnParentExit for automatic termination when parent exits
  ChildOnParentExit::spawn(cmd).with_context(|| {
    format!(
      "Failed to spawn Deno process for {} with parent-exit guard",
      host
    )
  })
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
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
