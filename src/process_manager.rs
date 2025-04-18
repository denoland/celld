use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, instrument, trace, warn};
use uuid::Uuid;

use crate::ProxyError;

pub struct ProcessEntry {
  pub pid: u32,
  pub socket_path: PathBuf,
  pub last_used: Instant,
  pub process_handle: Child,   // To kill the process
  pub single_use: bool,        // Flag for single-use isolates
  pub active_connections: u32, // Counter for active connections (including WebSockets)
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
  pub async fn increment_connection_count(&self, host: &str) -> bool {
    let mut processes = self.processes.lock().await;
    if let Some(entry) = processes.get_mut(host) {
      entry.active_connections += 1;
      entry.last_used = Instant::now();
      info!(
        host = %host,
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
  pub async fn decrement_connection_count(&self, host: &str) -> bool {
    let mut processes = self.processes.lock().await;
    if let Some(entry) = processes.get_mut(host) {
      if entry.active_connections > 0 {
        entry.active_connections -= 1;
      }
      entry.last_used = Instant::now();
      info!(
        host = %host,
        pid = entry.pid,
        active_connections = entry.active_connections,
        "Decremented active connection count"
      );
      true
    } else {
      false
    }
  }

  #[instrument(skip(self), fields(host = %host))]
  pub async fn get_or_spawn_process(
    &self,
    host: &str,
    single_use: bool,
  ) -> Result<(PathBuf, UnixStream), ProxyError> {
    let mut processes = self.processes.lock().await;

    // For single_use requests, always spawn a new process
    // TODO: This should not be supported in production
    if !single_use {
      if let Some(entry) = processes.get_mut(host) {
        // Skip single-use entries when looking for a regular process
        if !entry.single_use {
          entry.last_used = Instant::now();
          info!("Found running process for host");
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

    let app_code_dir = self.data_dir.join(host).join("code");
    let main_script = app_code_dir.join("main.ts");
    let sockets_dir = self.data_dir.join(host).join("sockets");

    if !main_script.exists() {
      warn!("Application code not found at {}", main_script.display());
      return Err(ProxyError::AppNotFound(host.to_string()));
    }

    // Create sockets dir if it doesn't exist
    tokio::fs::create_dir_all(&sockets_dir)
      .await
      .with_context(|| {
        format!(
          "Failed to create sockets directory: {}",
          sockets_dir.display()
        )
      })?;

    let socket_name = {
      let uuid_string = Uuid::new_v4().to_string();
      let first_segment: &str = &uuid_string[0..8];
      format!("{}.sock", first_segment)
    };
    let socket_path = sockets_dir.join(socket_name);

    info!(
      script = %main_script.display(),
      socket = %socket_path.display(),
      "Spawning Deno process"
    );

    let mut process_handle = Command::new("deno")
      .env("DENO_SERVE_ADDRESS", socket_path.clone())
      .arg("run")
      .arg(format!("--allow-read={}", app_code_dir.display()))
      .arg(format!("--allow-read={}", socket_path.display()))
      .arg(format!("--allow-write={}", socket_path.display()))
      .arg("--allow-net")
      .arg(&main_script)
      .spawn()
      .with_context(|| format!("Failed to spawn Deno process for {}", host))?;

    let pid = process_handle.id().ok_or_else(|| {
      anyhow::anyhow!("Failed to get PID for spawned process")
    })?;
    info!(pid = pid, "Deno process spawned");

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
        // Attempt to kill the potentially zombie process
        let _ = process_handle.kill().await;
        // Also try cleaning up the socket file if it exists
        let _ = tokio::fs::remove_file(&socket_).await;
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
          let _ = process_handle.kill().await;
          let _ = tokio::fs::remove_file(&socket_).await; // Cleanup attempt
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
      process_handle, // Move handle into entry
      single_use,
      active_connections: 0, // Initialize with zero connections
    };

    // For single-use isolates, use a unique key with a UUID suffix
    // This allows multiple single-use isolates for the same host
    let process_key = if single_use {
      format!("{}-{}", host, Uuid::new_v4())
    } else {
      host.to_string()
    };

    processes.insert(process_key, entry);
    info!(single_use = single_use, "Process entry added to map");

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
        if let Some(mut entry) = processes.remove(&host) {
          warn!(
            host = %host,
            pid = entry.pid,
            "Reaping idle process"
          );

          // Attempt to kill the process
          if let Err(e) = entry.process_handle.kill().await {
            error!(
              host = %host,
              pid = entry.pid,
              error = %e,
              "Failed to kill process during reap"
            );
            // Decide if you want to keep the entry for retry or fully remove
          }

          // Attempt to clean up the socket file
          if let Err(e) = tokio::fs::remove_file(&entry.socket_path).await {
            // Log error but continue cleanup - file might already be gone
            if e.kind() != std::io::ErrorKind::NotFound {
              error!(
                host = %host,
                pid = entry.pid,
                socket = %entry.socket_path.display(),
                error = %e,
                "Failed to remove socket file during reap"
              );
            }
          }
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
