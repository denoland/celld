use anyhow::Result;
use once_cell::sync::Lazy;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{error, info, trace, warn};

mod http_header_parser;
mod process_manager;

use process_manager::ProcessManager;

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
    data_dir: PathBuf,
    idle_timeout: Duration,
    reaper_interval: Duration,
  ) -> Self {
    Proxy {
      process_manager: ProcessManager::new(data_dir),
      idle_timeout,
      reaper_interval,
      listener: None,
    }
  }

  /// Listen on the specified address
  async fn bind(&mut self, addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    self.listener = Some(listener);
    Ok(())
  }

  /// Get the local address of the listener
  fn local_addr(&self) -> Option<SocketAddr> {
    self.listener.as_ref().and_then(|l| l.local_addr().ok())
  }

  /// Run the proxy server
  async fn run(&mut self, exit_on_error: bool) -> Result<()> {
    // Start the reaper task
    let process_manager_ = self.process_manager.clone();
    let idle_timeout = self.idle_timeout;
    let reaper_interval = self.reaper_interval;

    tokio::spawn(async move {
      process_manager_
        .start_reaper(idle_timeout, reaper_interval)
        .await;
    });

    // Set up the listener if not already done
    if self.listener.is_none() {
      error!("Call proxy.bind() to set up the listener");
      return Err(anyhow::anyhow!("Listener not set up").into());
    }

    let listener = self.listener.as_ref().unwrap();

    let local_addr = listener.local_addr()?;
    info!("Proxy listening on http://{}", local_addr);

    loop {
      let (client_stream, remote_addr) = match listener.accept().await {
        Ok(s) => s,
        Err(e) => {
          error!("Failed to accept connection: {:?}", e);
          if exit_on_error {
            break Err(e.into());
          } else {
            continue;
          }
        }
      };
      trace!("Accepted connection from {}", remote_addr);

      let process_manager_ = self.process_manager.clone();

      tokio::task::spawn(async move {
        handle_connection(client_stream, remote_addr, process_manager_).await;
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
pub enum ProxyError {
  #[error("Invalid hostname format")]
  InvalidHost,
  #[error("Application not found for host: {0}")]
  AppNotFound(String),
  #[error("Internal Server Error: {0}")]
  InternalError(#[from] anyhow::Error),
}

#[tokio::main]
async fn main() -> Result<()> {
  // Initialize tracing for console logging
  tracing_subscriber::fmt::init();

  // Create a proxy with default timeout values
  let mut proxy = Proxy::new(
    DATA_DIR.clone(),
    DEFAULT_IDLE_TIMEOUT,
    DEFAULT_REAPER_INTERVAL,
  );

  let addr = SocketAddr::from(([0, 0, 0, 0], *DATA_PORT));
  proxy.bind(addr).await?;
  proxy.run(false).await?;

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_proxy_with_ephemeral_port() {
    // Create a proxy with custom timeouts
    let mut proxy = Proxy::new(
      DATA_DIR.clone(),
      DEFAULT_IDLE_TIMEOUT,
      DEFAULT_REAPER_INTERVAL,
    );

    // Use port 0 for an ephemeral port
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));

    // Just bind the listener but don't run the proxy yet
    proxy.bind(addr).await.unwrap();
    let proxy_addr = proxy.local_addr().unwrap();

    // Start the proxy in the background
    tokio::spawn(async move {
      // The listen step is already done, so just run the proxy
      if let Err(e) = proxy.run(true).await {
        error!("Proxy error: {}", e);
      }
    });

    // Give it a moment to start up
    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = reqwest::Client::new()
      .get(format!("http://localhost:{}/", proxy_addr.port()))
      .header("Host", "ry.local")
      .send()
      .await
      .unwrap()
      .text()
      .await
      .unwrap();

    assert!(
      response.contains("hello from ry.local"),
      "Response didn't contain expected content"
    );
  }
}