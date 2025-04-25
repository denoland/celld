use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::net::UnixStream;
use tokio::time::sleep;
use tracing::{error, info, instrument, trace, warn};
use uuid::Uuid;

use crate::child_on_parent_exit::ChildOnParentExit;
use crate::sqlite_replica::{create_empty_database, SqliteReplica};
use crate::ProxyError;

pub struct ProcessEntry {
  pub pid: u32,
  pub socket_path: PathBuf,
  pub last_used: Instant,
  pub parent_exit_guard: ChildOnParentExit, // Guard for automatic termination on parent exit
  pub single_use: bool,                     // Flag for single-use isolates
  pub active_connections: u32, // Counter for active connections (including WebSockets)
  pub _socket_tempdir: TempDir, // Keep tempdir alive as long as process exists
  pub replica: Option<SqliteReplica>, // SQLite replication to S3/MinIO
}

#[derive(Clone)]
pub struct ProcessManager {
  pub data_dir: PathBuf,
  pub processes: Arc<Mutex<HashMap<String, ProcessEntry>>>,
}

impl ProcessManager {
  pub fn new(data_dir: PathBuf) -> Self {
    // TODO: Implement a cleanup mechanism for old empty database files
    // This could be done by:
    // 1. Tracking database access times
    // 2. Running a periodic cleanup job that removes databases that haven't been
    //    accessed for a long time (e.g., weeks or months)
    // 3. Only removing databases that are successfully backed up to S3/MinIO
    ProcessManager {
      data_dir: std::fs::canonicalize(data_dir.clone()).unwrap(),
      processes: Arc::new(Mutex::new(HashMap::new())),
    }
  }

  /// Track a new connection to the process
  pub async fn increment_connection_count(
    &self,
    host: &str,
    room_id: &str,
  ) -> bool {
    let process_key = format!("{}:{}", host, room_id);

    // Scope the lock to minimize lock holding time
    let result = {
      let mut processes = self.processes.lock().unwrap();
      if let Some(entry) = processes.get_mut(&process_key) {
        entry.active_connections += 1;
        entry.last_used = Instant::now();
        (true, entry.pid, entry.active_connections)
      } else {
        (false, 0, 0)
      }
    };

    if result.0 {
      info!(
        host = %host,
        room_id = %room_id,
        pid = result.1,
        active_connections = result.2,
        "Incremented active connection count"
      );
    }

    result.0
  }

  /// Track a closed connection to the process
  pub async fn decrement_connection_count(
    &self,
    host: &str,
    room_id: &str,
  ) -> bool {
    let process_key = format!("{}:{}", host, room_id);

    // Scope the lock to minimize lock holding time
    let result = {
      let mut processes = self.processes.lock().unwrap();
      if let Some(entry) = processes.get_mut(&process_key) {
        if entry.active_connections > 0 {
          entry.active_connections -= 1;
        }
        entry.last_used = Instant::now();
        (true, entry.pid, entry.active_connections)
      } else {
        (false, 0, 0)
      }
    };

    if result.0 {
      info!(
        host = %host,
        room_id = %room_id,
        pid = result.1,
        active_connections = result.2,
        "Decremented active connection count"
      );
    }

    result.0
  }

  #[instrument(skip(self), fields(host = %host, room_id = %room_id))]
  pub async fn get_or_spawn_process(
    &self,
    host: &str,
    room_id: &str,
    single_use: bool,
  ) -> Result<(PathBuf, UnixStream), ProxyError> {
    // Create a combined key for host and room to ensure one isolate per room
    let process_key = format!("{}:{}", host, room_id);

    // For single_use requests, always spawn a new process
    // TODO: This should not be supported in production
    if !single_use {
      // First, try to find and connect to an existing process, keeping the lock time minimal
      let socket_path_opt = {
        let mut processes = self.processes.lock().unwrap();
        if let Some(entry) = processes.get_mut(&process_key) {
          // Skip single-use entries when looking for a regular process
          if !entry.single_use {
            entry.last_used = Instant::now();
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
        info!("Found running process for host and room");
        match UnixStream::connect(&socket_path).await {
          Ok(stream) => {
            info!(
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

    // --- Process not running, need to spawn ---
    info!("No running process found, spawning new one");

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
    let mut replica = None;
    let db_path = sqlite_dir.join(format!("{}.db", room_id));

    if let Some(s3_config) = crate::sqlite_replica::get_s3_cfg_for_tenant(host)
    {
      info!(
        tenant = %host,
        room_id = %room_id,
        "S3 replication enabled for room"
      );

      let replica_instance =
        SqliteReplica::new(&self.data_dir, host, room_id, s3_config);

      // Restore the database if needed
      match replica_instance.restore_if_needed().await {
        Ok(restored) => {
          info!(
            tenant = %host,
            room_id = %room_id,
            restored = restored,
            "Database restore completed successfully"
          );
        }
        Err(e) => {
          warn!(
            tenant = %host,
            room_id = %room_id,
            error = %e,
            "Failed to restore database, falling back to empty database"
          );

          // Create empty database file if restore failed and file doesn't exist
          if !db_path.exists() {
            create_empty_database(&db_path).unwrap_or_else(|e| {
              warn!("Failed to create empty database file: {}", e);
            });
          }
        }
      }

      // Start replication
      match replica_instance.start_replication().await {
        Ok(_) => {
          info!(
            tenant = %host,
            room_id = %room_id,
            "Started SQLite replication"
          );
          replica = Some(replica_instance);
        }
        Err(e) => {
          warn!(
            tenant = %host,
            room_id = %room_id,
            error = %e,
            "Failed to start replication"
          );
        }
      }
    } else {
      // No S3 config available, create empty database file
      if !db_path.exists() {
        create_empty_database(&db_path).unwrap_or_else(|e| {
          warn!("Failed to create empty database file: {}", e);
        });
      }

      info!(
        tenant = %host,
        room_id = %room_id,
        "S3 replication disabled (no configuration found)"
      );
    }

    // Path to bootstrap.ts script
    let bootstrap_script = self
      .data_dir
      .parent()
      .unwrap()
      .join("src")
      .join("bootstrap.ts");

    let spawn_start = Instant::now();
    info!(
      bootstrap = %bootstrap_script.display(),
      script = %main_script.display(),
      socket = %socket_path.display(),
      "Spawning Deno process"
    );

    let mut cmd = std::process::Command::new("deno");
    cmd
      .current_dir(&tenant_dir)
      .env(
        "DENO_SERVE_ADDRESS",
        format!("unix:{}", socket_path.display()),
      )
      .env("X-Room-Id", room_id) // Pass room ID to bootstrap.ts
      .arg("run")
      .arg("--no-prompt")
      .arg(format!("--allow-read={}", tenant_dir.display()))
      .arg(format!("--allow-write={}", tenant_dir.display()))
      .arg(format!("--allow-read={}", socket_path.display()))
      .arg(format!("--allow-write={}", socket_path.display()))
      .arg("--allow-net")
      .arg("--allow-env=X-Room-Id") // Permission to read env var
      .arg(&bootstrap_script) // Use bootstrap.ts instead of main.ts directly
      .arg(&main_script); // Pass main.ts as argument to bootstrap.ts

    // Spawn with ChildOnParentExit for automatic termination when parent exits
    let child_guard = ChildOnParentExit::spawn(cmd).with_context(|| {
      format!(
        "Failed to spawn Deno process for {} with parent-exit guard",
        host
      )
    })?;

    // Convert to tokio::process::Child using the PID
    let pid = child_guard.pid().unwrap() as u32;

    info!(
        pid = pid,
        spawn_duration = ?spawn_start.elapsed(),
        "Deno process spawned with parent-exit guard"
    );

    // --- Wait for the socket to become available (crucial for cold start) ---
    let socket_ = socket_path.clone();
    let wait_start = Instant::now();
    let wait_timeout = Duration::from_secs(10); // Timeout for socket connection

    // Use minimal polling for fastest possible connection with exponential backoff
    let mut delay = Duration::from_micros(5); // Start with even smaller initial delay
    let max_delay = Duration::from_millis(1); // Reduce max delay to improve responsiveness

    // Wait for the socket to be available and connect to it
    let stream = loop {
      if wait_start.elapsed() > wait_timeout {
        error!(
          pid = pid,
          socket = %socket_.display(),
          "Timeout waiting for Deno process socket"
        );
        // Kill the process using ChildOnParentExit
        child_guard.kill();
        // The tempdir will be dropped at the end of this scope, cleaning up the socket file
        return Err(
          anyhow::anyhow!("Timeout waiting for process socket").into(),
        );
      }

      match UnixStream::connect(&socket_).await {
        Ok(stream) => {
          info!(
            pid = pid,
            socket = %socket_.display(),
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
          // Kill the process using ChildOnParentExit
          child_guard.kill();
          // The tempdir will be dropped at the end of this scope, cleaning up the socket file
          return Err(
            anyhow::anyhow!("Error connecting to process socket: {}", e).into(),
          );
        }
      }
    };

    let entry = ProcessEntry {
      pid,
      socket_path: socket_path.clone(),
      last_used: Instant::now(),
      parent_exit_guard: child_guard, // Store the parent exit guard
      single_use,
      active_connections: 0, // Initialize with zero connections
      _socket_tempdir: socket_tempdir, // Keep tempdir alive as long as process exists
      replica, // Store the SQLite replica (None if not enabled)
    };

    // For single-use isolates, use a unique key with a UUID suffix
    // This allows multiple single-use isolates for the same host+room combination
    let final_key = if single_use {
      format!("{}:{}-{}", host, room_id, Uuid::new_v4())
    } else {
      // Use the combined host:room_id key
      process_key
    };

    // Add the entry to the processes map (minimal lock time)
    {
      let mut processes = self.processes.lock().unwrap();
      processes.insert(final_key, entry);
    }

    info!(
      single_use = single_use,
      host = %host,
      room_id = %room_id,
      "Process entry added to map"
    );

    Ok((socket_path, stream))
  }

  #[instrument(skip(self))]
  pub async fn start_reaper(
    &self,
    idle_timeout: Duration,
    reaper_interval: Duration,
  ) {
    info!("Starting idle process reaper task");
    loop {
      sleep(reaper_interval).await;
      trace!("Reaper checking for idle processes...");

      // Collect hosts to reap without holding the lock too long
      let hosts_to_reap = {
        let processes = self.processes.lock().unwrap();
        let now = Instant::now();
        let mut to_reap = Vec::new();

        for (host, entry) in processes.iter() {
          if now.duration_since(entry.last_used) > idle_timeout {
            info!(
              host = %host,
              pid = entry.pid,
              idle_duration = ?now.duration_since(entry.last_used),
              "Process marked for reaping due to inactivity"
            );
            to_reap.push(host.clone());
          }
        }
        to_reap
      };

      // Process each host to reap
      for host in hosts_to_reap {
        // Get the entry to remove
        let entry = {
          let mut processes = self.processes.lock().unwrap();
          processes.remove(&host)
        };

        // If we got an entry, reap it
        if let Some(entry) = entry {
          warn!(
            host = %host,
            pid = entry.pid,
            "Reaping idle process"
          );

          // Shutdown SQLite replica if it exists
          if let Some(replica) = &entry.replica {
            info!(
              host = %host,
              pid = entry.pid,
              "Flushing and shutting down SQLite replica before killing process"
            );

            // Wait for flush with a timeout of 2 seconds
            match replica
              .wait_for_flush(std::time::Duration::from_secs(2))
              .await
            {
              Ok(_) => info!("SQLite replica flush successful"),
              Err(e) => warn!("Error flushing SQLite replica: {}", e),
            }

            // Now shutdown the replica
            match replica.shutdown().await {
              Ok(_) => info!("SQLite replica shutdown successful"),
              Err(e) => warn!("Error shutting down SQLite replica: {}", e),
            }
          }

          // Now kill the Deno process using the parent_exit_guard
          entry.parent_exit_guard.kill();
          // The Drop implementation of ChildOnParentExit will finish the process cleanup
          // The TempDir will be dropped when entry is dropped, cleaning up the socket file
          info!(
            host = %host,
            pid = entry.pid,
            "Killed process using parent-exit guard"
          );
          info!(
            host = %host,
            pid = entry.pid,
            "Process reaped successfully"
          );
        }
      }
      trace!("Reaper check complete.");
    }
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
