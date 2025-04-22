use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, instrument, trace, warn};
use uuid::Uuid;

use crate::child_on_parent_exit::ChildOnParentExit;
use crate::ProxyError;

pub struct ProcessEntry {
  pub pid: u32,
  pub socket_path: PathBuf,
  pub last_used: Instant,
  pub parent_exit_guard: ChildOnParentExit, // Guard for automatic termination on parent exit
  pub single_use: bool,                     // Flag for single-use isolates
  pub active_connections: u32, // Counter for active connections (including WebSockets)
  pub _socket_tempdir: TempDir, // Keep tempdir alive as long as process exists
}

#[derive(Clone)]
pub struct ProcessManager {
  pub data_dir: PathBuf,
  pub processes: Arc<Mutex<HashMap<String, ProcessEntry>>>,
}

impl ProcessManager {
  pub fn new(data_dir: PathBuf) -> Self {
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
    let mut processes = self.processes.lock().await;
    if let Some(entry) = processes.get_mut(&process_key) {
      entry.active_connections += 1;
      entry.last_used = Instant::now();
      info!(
        host = %host,
        room_id = %room_id,
        pid = entry.pid,
        active_connections = entry.active_connections,
        "Incremented active connection count"
      );
      true
    } else {
      false
    }
  }

  /// Track a closed connection to the process
  pub async fn decrement_connection_count(
    &self,
    host: &str,
    room_id: &str,
  ) -> bool {
    let process_key = format!("{}:{}", host, room_id);
    let mut processes = self.processes.lock().await;
    if let Some(entry) = processes.get_mut(&process_key) {
      if entry.active_connections > 0 {
        entry.active_connections -= 1;
      }
      entry.last_used = Instant::now();
      info!(
        host = %host,
        room_id = %room_id,
        pid = entry.pid,
        active_connections = entry.active_connections,
        "Decremented active connection count"
      );
      true
    } else {
      false
    }
  }

  #[instrument(skip(self), fields(host = %host, room_id = %room_id))]
  pub async fn get_or_spawn_process(
    &self,
    host: &str,
    room_id: &str,
    single_use: bool,
  ) -> Result<(PathBuf, UnixStream), ProxyError> {
    let mut processes = self.processes.lock().await;

    // Create a combined key for host and room to ensure one isolate per room
    let process_key = format!("{}:{}", host, room_id);

    // For single_use requests, always spawn a new process
    // TODO: This should not be supported in production
    if !single_use {
      if let Some(entry) = processes.get_mut(&process_key) {
        // Skip single-use entries when looking for a regular process
        if !entry.single_use {
          entry.last_used = Instant::now();
          info!("Found running process for host and room");
          // Connect to the socket
          let socket_path = entry.socket_path.clone();
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

    // TODO: this should probably be colocated with SqliteReplica code?
    std::fs::create_dir_all(tenant_dir.join("sqlite")).unwrap_or_else(|e| {
      warn!("Failed to create sqlite directory: {}", e);
    });

    // Path to bootstrap.ts script
    let bootstrap_script = self
      .data_dir
      .parent()
      .unwrap()
      .join("src")
      .join("bootstrap.ts");

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

    info!(pid = pid, "Deno process spawned with parent-exit guard");

    // --- Wait for the socket to become available (crucial for cold start) ---
    let socket_ = socket_path.clone();
    let wait_start = Instant::now();
    let wait_timeout = Duration::from_secs(10); // Timeout for socket connection

    // Use minimal polling for fastest possible connection
    let delay = Duration::from_micros(100);

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
            duration = ?wait_start.elapsed(),
            "Socket connected!"
          );
          // We have a connected socket
          break stream; // Socket is ready and connected, return the stream
        }
        Err(ref e)
          if e.kind() == std::io::ErrorKind::ConnectionRefused
            || e.kind() == std::io::ErrorKind::NotFound =>
        {
          // Socket not ready yet, use minimal polling with a tiny delay
          sleep(delay).await;
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
    };

    // For single-use isolates, use a unique key with a UUID suffix
    // This allows multiple single-use isolates for the same host+room combination
    let final_key = if single_use {
      format!("{}:{}-{}", host, room_id, Uuid::new_v4())
    } else {
      // Use the combined host:room_id key
      process_key
    };

    processes.insert(final_key, entry);
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

      let mut processes = self.processes.lock().await;
      let now = Instant::now();
      let mut hosts_to_reap = Vec::new();

      for (host, entry) in processes.iter() {
        if now.duration_since(entry.last_used) > idle_timeout {
          info!(
            host = %host,
            pid = entry.pid,
            idle_duration = ?now.duration_since(entry.last_used),
            "Process marked for reaping due to inactivity"
          );
          hosts_to_reap.push(host.clone());
        }
      }

      // Separate loop for removal to avoid mutable borrow issues while iterating
      for host in hosts_to_reap {
        if let Some(entry) = processes.remove(&host) {
          warn!(
            host = %host,
            pid = entry.pid,
            "Reaping idle process"
          );

          // Kill process using the parent_exit_guard
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
}
