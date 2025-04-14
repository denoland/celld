use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tracing::{error, info, instrument, warn};

const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const REAPER_INTERVAL: Duration = Duration::from_secs(10);

struct ProcessEntry {
  pid: u32,
  socket_path: PathBuf,
  last_used: Instant,
  process_handle: Child,
}

pub struct ProcessManager {
  data_dir: PathBuf,
  processes: Arc<Mutex<HashMap<String, ProcessEntry>>>,
}

impl ProcessManager {
  fn new(data_dir: PathBuf) -> Self {
    ProcessManager {
      data_dir,
      processes: Arc::new(Mutex::new(HashMap::new())),
    }
  }

  #[instrument(skip(self), fields(host = %host))]
  async fn get_or_spawn_process(
    &self,
    host: &str,
  ) -> Result<PathBuf, ProxyError> {
    let mut processes = self.processes.lock().await;

    if let Some(entry) = processes.get_mut(host) {
      entry.last_used = Instant::now();
      info!("Found running process for host");
      return Ok(entry.socket_path.clone());
    }

    // --- Process not running, need to spawn ---
    info!("No running process found, spawning new one");

    // Validate host format briefly (prevent directory traversal)
    if host.contains('/') || host == ".." {
      return Err(ProxyError::InvalidHost);
    }

    let code_dir = self.data_dir.join(host).join("code");
    let main_script = code_dir.join("main.ts");
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

    let socket_name = format!("{}.sock", Uuid::new_v4());
    let socket_path = sockets_dir.join(socket_name);
    let socket_path_str = socket_path
      .to_str()
      .ok_or_else(|| anyhow::anyhow!("Invalid socket path"))?
      .to_string();

    // Permissions: Allow reading app code dir, allow listening on the specific socket
    let allow_read_perm = format!("--allow-read={}", app_code_dir.display());
    // Note: Deno needs permission to the *specific* socket path
    let allow_listen_perm = format!("--allow-listen=unix:{}", socket_path_str);

    let deno_cmd = "deno"; // Assuming deno is in PATH

    info!(command = %deno_cmd, script = %main_script.display(), socket = %socket_path_str, "Spawning Deno process");

    let mut cmd = Command::new(deno_cmd);
    cmd.arg("run");
    cmd.arg(allow_read_perm);
    cmd.arg(allow_listen_perm);
    // Add other permissions as needed, e.g., --allow-env
    cmd.arg("--no-check"); // Faster startup for dev/MVP
    cmd.arg(&main_script);
    cmd.arg(format!("--listen-socket={}", socket_path_str)); // Pass socket path to script

    // Stdio setup - inherit for easy debugging in MVP
    cmd.stdout(std::process::Stdio::inherit());
    cmd.stderr(std::process::Stdio::inherit());

    let mut process_handle = cmd
      .spawn()
      .with_context(|| format!("Failed to spawn Deno process for {}", host))?;

    let pid = process_handle.id().ok_or_else(|| {
      anyhow::anyhow!("Failed to get PID for spawned process")
    })?;
    info!(pid = pid, "Deno process spawned");

    // --- Wait for the socket to become available (crucial for cold start) ---
    let socket_clone = socket_path.clone();
    let wait_start = Instant::now();
    let wait_timeout = Duration::from_secs(10); // Timeout for socket connection

    loop {
      if wait_start.elapsed() > wait_timeout {
        error!(pid = pid, socket = %socket_clone.display(), "Timeout waiting for Deno process socket");
        // Attempt to kill the potentially zombie process
        let _ = process_handle.kill().await;
        // Also try cleaning up the socket file if it exists
        let _ = tokio::fs::remove_file(&socket_clone).await;
        return Err(
          anyhow::anyhow!("Timeout waiting for process socket").into(),
        );
      }

      match UnixStream::connect(&socket_clone).await {
        Ok(_) => {
          info!(pid = pid, socket = %socket_clone.display(), duration = ?wait_start.elapsed(), "Socket connected!");
          break; // Socket is ready
        }
        Err(ref e)
          if e.kind() == std::io::ErrorKind::ConnectionRefused
            || e.kind() == std::io::ErrorKind::NotFound =>
        {
          // Socket not ready yet, wait and retry
          sleep(Duration::from_millis(50)).await; // Short delay
        }
        Err(e) => {
          error!(pid = pid, socket = %socket_clone.display(), error = %e, "Error connecting to socket during startup");
          let _ = process_handle.kill().await;
          let _ = tokio::fs::remove_file(&socket_clone).await; // Cleanup attempt
          return Err(
            anyhow::anyhow!("Error connecting to process socket: {}", e).into(),
          );
        }
      }
    }

    let entry = ProcessEntry {
      pid,
      socket_path: socket_path.clone(),
      last_used: Instant::now(),
      process_handle, // Move handle into entry
    };

    processes.insert(host.to_string(), entry);
    info!("Process entry added to map");

    Ok(socket_path)
  }

  #[instrument(skip(self))]
  async fn start_reaper(self: Arc<Self>) {
    info!("Starting idle process reaper task");
    loop {
      sleep(Duration::from_secs(REAPER_INTERVAL_SECONDS)).await;
      trace!("Reaper checking for idle processes...");

      let mut processes = self.processes.lock().await;
      let now = Instant::now();
      let idle_timeout = self.idle_timeout;
      let mut hosts_to_reap = Vec::new();

      for (host, entry) in processes.iter() {
        if now.duration_since(entry.last_used) > idle_timeout {
          info!(host = %host, pid = entry.pid, idle_duration = ?now.duration_since(entry.last_used), "Process marked for reaping due to inactivity");
          hosts_to_reap.push(host.clone());
        }
      }

      // Separate loop for removal to avoid mutable borrow issues while iterating
      for host in hosts_to_reap {
        if let Some(mut entry) = processes.remove(&host) {
          warn!(host = %host, pid = entry.pid, "Reaping idle process");

          // Attempt to kill the process
          if let Err(e) = entry.process_handle.kill().await {
            error!(host = %host, pid = entry.pid, error = %e, "Failed to kill process during reap");
            // Decide if you want to keep the entry for retry or fully remove
          }

          // Attempt to clean up the socket file
          let socket_path = entry.socket_path.clone(); // Clone before moving entry
          if let Err(e) = tokio::fs::remove_file(&socket_path).await {
            // Log error but continue cleanup - file might already be gone
            if e.kind() != std::io::ErrorKind::NotFound {
              error!(host = %host, pid = entry.pid, socket = %socket_path.display(), error = %e, "Failed to remove socket file during reap");
            }
          }
          info!(host = %host, pid = entry.pid, "Process reaped successfully");
        }
      }
      trace!("Reaper check complete.");
    }
  }
}
