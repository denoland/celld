use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, instrument, trace, warn};
use uuid::Uuid;

mod http_header_parser;

// Default values, can be overridden when creating Proxy
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_REAPER_INTERVAL: Duration = Duration::from_secs(10);

/// Proxy handles TCP connections and proxies them to the appropriate Deno process
///
/// It listens on a provided TCP address and forwards incoming connections
/// to the appropriate Deno process based on the Host header.
struct Proxy {
  process_manager: ProcessManager,
  idle_timeout: Duration,
  reaper_interval: Duration,
  listener: Option<TcpListener>,
}

impl Proxy {
  fn new(
    process_manager: ProcessManager,
    idle_timeout: Duration,
    reaper_interval: Duration,
  ) -> Self {
    Proxy {
      process_manager,
      idle_timeout,
      reaper_interval,
      listener: None,
    }
  }

  /// Listen on the specified address
  async fn listen(&mut self, addr: SocketAddr) -> Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    info!("Proxy listening on http://{}", local_addr);
    self.listener = Some(listener);
    Ok(local_addr)
  }

  /// Get the local address of the listener
  fn local_addr(&self) -> Option<SocketAddr> {
    self.listener.as_ref().and_then(|l| l.local_addr().ok())
  }

  /// Run the proxy server
  async fn run(&mut self, addr: SocketAddr) -> Result<()> {
    // Start the reaper task
    let reaper_manager = self.process_manager.clone();
    let idle_timeout = self.idle_timeout;
    let reaper_interval = self.reaper_interval;
    tokio::spawn(async move {
      reaper_manager
        .start_reaper(idle_timeout, reaper_interval)
        .await;
    });

    // Set up the listener if not already done
    if self.listener.is_none() {
      self.listen(addr).await?;
    }

    let listener = self.listener.as_ref().unwrap();

    // Main accept loop
    loop {
      let (client_stream, remote_addr) = match listener.accept().await {
        Ok(s) => s,
        Err(e) => {
          error!("Failed to accept connection: {:?}", e);
          continue; // Keep listening
        }
      };
      trace!("Accepted connection from {}", remote_addr);

      // Clone the process manager for this connection
      let process_manager_ = self.process_manager.clone();

      // Clone what we need for the connection handler
      let pm = process_manager_.clone();

      // Spawn a Tokio task for direct TCP proxying
      tokio::task::spawn(async move {
        // Use a standalone function instead of a self method
        handle_connection(client_stream, remote_addr, pm).await;
      });
    }
  }
}

/// Handle an individual client connection
async fn handle_connection(
  client_stream: tokio::net::TcpStream,
  remote_addr: SocketAddr,
  process_manager: ProcessManager,
) {
  // Create buffered readers/writers for the client connection
  let (mut client_read, mut client_write) = tokio::io::split(client_stream);

  let read_start = Instant::now();

  // Use the http_header_parser module to extract routing information
  let http_info =
    match http_header_parser::parse_http_headers(&mut client_read).await {
      Ok(info) => {
        if info.host.is_none() {
          warn!("Invalid HTTP request: missing host header");
          let resp = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
          let _ = client_write.write_all(resp.as_bytes()).await;
          return;
        }
        info
      }
      Err(msg) => {
        warn!("Error parsing HTTP headers: {}", msg);
        let resp = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
        let _ = client_write.write_all(resp.as_bytes()).await;
        return;
      }
    };

  // Extract the information we need
  let host = http_info.host.unwrap();
  let single_use = http_info.single_use;
  let headers_buf = http_info.header_buffer;

  info!(
      host = %host,
      single_use = %single_use,
      "Request headers parsed in {:?}", read_start.elapsed()
  );

  // Get or spawn the upstream process and get a connected socket
  let (socket_path, upstream_conn) = match process_manager
    .get_or_spawn_process(&host, single_use)
    .await
  {
    Ok((path, stream)) => (path, stream),
    Err(e) => {
      error!(host = %host, error = %e, "Failed to get or spawn process");
      let status_line = match e {
        ProxyError::AppNotFound(_) => "HTTP/1.1 404 Not Found",
        ProxyError::InvalidHost => "HTTP/1.1 400 Bad Request",
        _ => "HTTP/1.1 500 Internal Server Error",
      };
      let resp = format!("{}\r\nContent-Length: 0\r\n\r\n", status_line);
      let _ = client_write.write_all(resp.as_bytes()).await;
      return;
    }
  };

  info!(
      host = %host,
      socket = %socket_path.display(),
      "Connected to upstream"
  );

  // Create read/write streams for the upstream connection
  let (mut upstream_read, mut upstream_write) = tokio::io::split(upstream_conn);

  // Forward the headers we've already read to the upstream
  if let Err(e) = upstream_write.write_all(&headers_buf).await {
    error!(
        host = %host,
        error = %e,
        "Failed to forward headers to upstream"
    );
    return;
  }

  // Bidirectional copy between client and upstream
  // We need two tasks: client -> upstream and upstream -> client
  let client_to_upstream = tokio::spawn(async move {
    let mut buffer = [0u8; 16384];
    loop {
      match client_read.read(&mut buffer).await {
        Ok(0) => break, // EOF
        Ok(n) => {
          if let Err(e) = upstream_write.write_all(&buffer[..n]).await {
            error!("Error writing to upstream: {:?}", e);
            break;
          }
        }
        Err(e) => {
          error!("Error reading from client: {:?}", e);
          break;
        }
      }
    }
  });

  let upstream_to_client = tokio::spawn(async move {
    let mut buffer = [0u8; 16384];
    loop {
      match upstream_read.read(&mut buffer).await {
        Ok(0) => break, // EOF
        Ok(n) => {
          if let Err(e) = client_write.write_all(&buffer[..n]).await {
            error!("Error writing to client: {:?}", e);
            break;
          }
        }
        Err(e) => {
          error!("Error reading from upstream: {:?}", e);
          break;
        }
      }
    }
  });

  // Wait for both transfer directions to complete
  let (client_result, upstream_result) =
    tokio::join!(client_to_upstream, upstream_to_client);

  // Handle any errors in the transfer tasks
  if let Err(e) = client_result {
    error!("Client to upstream transfer task failed: {:?}", e);
  }

  if let Err(e) = upstream_result {
    error!("Upstream to client transfer task failed: {:?}", e);
  }

  // The connection is now complete
  info!(
      host = %host,
      "Connection completed"
  );

  // If this was a single-use isolate, clean it up
  if single_use {
    let host_clone = host.clone();
    let manager_clone = process_manager.clone();

    tokio::spawn(async move {
      // Minimal wait for connection cleanup
      tokio::time::sleep(Duration::from_millis(10)).await;

      info!(
          host = %host_clone,
          "Cleaning up single-use isolate"
      );
      let mut processes = manager_clone.processes.lock().await;

      let keys_to_remove: Vec<String> = processes
        .iter()
        .filter_map(|(k, v)| {
          if k.starts_with(&host_clone) && v.single_use {
            Some(k.clone())
          } else {
            None
          }
        })
        .collect();

      for key in keys_to_remove {
        if let Some(mut entry) = processes.remove(&key) {
          info!(
              host = %host_clone,
              pid = entry.pid,
              "Terminating single-use isolate"
          );
          let _ = entry.process_handle.kill().await;
          let _ = tokio::fs::remove_file(&entry.socket_path).await;
        }
      }
    });
  }

  trace!("Connection task finished for {}", remote_addr);
}

static DATA_PORT: Lazy<u16> = Lazy::new(|| {
  std::env::var("DATA_PORT")
    .ok()
    .and_then(|s| s.parse().ok())
    .map_or_else(|| 3000u16, |port| port)
});

static DATA_DIR: Lazy<PathBuf> = Lazy::new(|| {
  let path = PathBuf::from(
    std::env::var("DATA").unwrap_or_else(|_| "./data".to_string()),
  );

  if !path.is_dir() {
    error!(
      "DATA_DIR ('{}') is not an existing directory.",
      path.display()
    );
    std::process::exit(1);
  }

  info!("Using DATA_DIR: {}", path.display());
  path
});

#[derive(Debug, thiserror::Error)]
enum ProxyError {
  #[error("Invalid hostname format")]
  InvalidHost,
  #[error("Application not found for host: {0}")]
  AppNotFound(String),
  #[error("Internal Server Error: {0}")]
  InternalError(#[from] anyhow::Error),
}

struct ProcessEntry {
  pid: u32,
  socket_path: PathBuf,
  last_used: Instant,
  process_handle: Child, // To kill the process
  single_use: bool,      // Flag for single-use isolates
}

// Shared mutable state for process entries

#[derive(Clone)]
struct ProcessManager {
  data_dir: PathBuf,
  processes: Arc<Mutex<HashMap<String, ProcessEntry>>>,
}

impl ProcessManager {
  fn new(data_dir: PathBuf) -> Self {
    let pm = ProcessManager {
      data_dir: data_dir.clone(),
      processes: Arc::new(Mutex::new(HashMap::new())),
    };

    // Spawn a task to clean up any stale socket files at startup
    tokio::spawn(async move {
      // Clean up any leftover socket files from previous runs
      if let Ok(entries) = tokio::fs::read_dir(&data_dir).await {
        let mut entries = entries;
        while let Ok(Some(entry)) = entries.next_entry().await {
          let host_dir = entry.path();
          if !host_dir.is_dir() {
            continue;
          }

          let sockets_dir = host_dir.join("sockets");
          if !sockets_dir.exists() || !sockets_dir.is_dir() {
            continue;
          }

          if let Ok(socket_entries) = tokio::fs::read_dir(&sockets_dir).await {
            let mut socket_entries = socket_entries;
            while let Ok(Some(socket_entry)) = socket_entries.next_entry().await
            {
              let socket_path = socket_entry.path();
              if socket_path.extension().map_or(false, |ext| ext == "sock") {
                // Try to remove the socket file
                if let Err(e) = tokio::fs::remove_file(&socket_path).await {
                  if e.kind() != std::io::ErrorKind::NotFound {
                    // Ignore "not found" errors
                    eprintln!(
                      "Failed to remove stale socket file {}: {}",
                      socket_path.display(),
                      e
                    );
                  }
                } else {
                  eprintln!(
                    "Removed stale socket file: {}",
                    socket_path.display()
                  );
                }
              }
            }
          }
        }
      }
    });

    pm
  }

  #[instrument(skip(self), fields(host = %host))]
  async fn get_or_spawn_process(
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

    let socket_name = format!("{}.sock", Uuid::new_v4());
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
  async fn start_reaper(
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

#[tokio::main]
async fn main() -> Result<()> {
  // Initialize tracing for console logging
  tracing_subscriber::fmt::init();

  let process_manager = ProcessManager::new(DATA_DIR.clone());

  // Create a proxy with default timeout values
  let mut proxy = Proxy::new(
    process_manager,
    DEFAULT_IDLE_TIMEOUT,
    DEFAULT_REAPER_INTERVAL,
  );

  let addr = SocketAddr::from(([0, 0, 0, 0], *DATA_PORT));
  proxy.run(addr).await?;

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_process_manager_direct() {
    // Create a process manager
    let process_manager = ProcessManager::new(DATA_DIR.clone());

    // Get socket and connected stream for "ry.local"
    let (socket_path, unix_stream) = process_manager
      .get_or_spawn_process("ry.local", false)
      .await
      .unwrap();
    info!(
      socket = %socket_path.display(),
      "Got socket path and stream"
    );

    // Build a simple HTTP request
    let req_body = "GET / HTTP/1.1\r\nHost: ry.local\r\n\r\n";

    // Create read/write streams from the unix socket
    let (mut reader, mut writer) = tokio::io::split(unix_stream);

    // Send the request
    writer.write_all(req_body.as_bytes()).await.unwrap();

    // Read the response
    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);

    // Verify the response contains the expected body
    assert!(
      response.contains("hello from ry.local"),
      "Response didn't contain expected content"
    );
  }

  #[tokio::test]
  async fn test_proxy_with_ephemeral_port() {
    // Create a process manager
    let process_manager = ProcessManager::new(DATA_DIR.clone());

    // Create a proxy with custom timeouts
    let mut proxy = Proxy::new(
      process_manager,
      Duration::from_secs(60), // idle_timeout
      Duration::from_secs(10), // reaper_interval
    );

    // Use port 0 for an ephemeral port
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));

    // Just bind the listener but don't run the proxy yet
    let actual_addr = proxy.listen(addr).await.unwrap();

    // Start the proxy in the background
    let proxy_handle = tokio::spawn(async move {
      // The listen step is already done, so just run the proxy
      if let Err(e) = proxy.run(addr).await {
        error!("Proxy error: {}", e);
      }
    });

    // Give it a moment to start up
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect to the proxy via TCP using the actual port
    let tcp_stream = tokio::net::TcpStream::connect(actual_addr).await.unwrap();
    let (mut reader, mut writer) = tokio::io::split(tcp_stream);

    // Send HTTP request
    let req_body = "GET / HTTP/1.1\r\nHost: ry.local\r\n\r\n";
    writer.write_all(req_body.as_bytes()).await.unwrap();

    // Read the response
    let mut buf = vec![0; 4096];
    let n = reader.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);

    // Verify the response contains the expected body
    assert!(
      response.contains("hello from ry.local"),
      "Response didn't contain expected content"
    );

    // Clean up
    proxy_handle.abort();
  }
}
