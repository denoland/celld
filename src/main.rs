use anyhow::{Context, Result};
use http_body_util::Full;
use hyper::body::{Body, Bytes};
use hyper::header::HOST;
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, instrument, trace, warn};
use uuid::Uuid;

const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const REAPER_INTERVAL: Duration = Duration::from_secs(10);

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
  #[error("Missing or invalid Host header")]
  MissingHost,
  #[error("Invalid hostname format")]
  InvalidHost,
  #[error("Application not found for host: {0}")]
  AppNotFound(String),
  #[error("Internal Server Error: {0}")]
  InternalError(#[from] anyhow::Error),
  #[error("Upstream application error: {0}")]
  UpstreamError(String),
  #[error("Upstream connection failed")]
  UpstreamConnectionFailed,
}

struct ProcessEntry {
  pid: u32,
  socket_path: PathBuf,
  last_used: Instant,
  process_handle: Child, // To kill the process
}

// Use Arc<Mutex<...>> for shared mutable state across async tasks/requests
type SharedProcessMap = Arc<Mutex<HashMap<String, ProcessEntry>>>;

struct ProcessManager {
  data_dir: PathBuf,
  processes: SharedProcessMap,
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

    info!(script = %main_script.display(), socket = %socket_path.display(), "Spawning Deno process");

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
      sleep(REAPER_INTERVAL).await;
      trace!("Reaper checking for idle processes...");

      let mut processes = self.processes.lock().await;
      let now = Instant::now();
      let mut hosts_to_reap = Vec::new();

      for (host, entry) in processes.iter() {
        if now.duration_since(entry.last_used) > IDLE_TIMEOUT {
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

#[derive(Clone)]
struct ProxyService {
  process_manager: Arc<ProcessManager>,
}

// impl Service<Request<Incoming>> for ProxyService {
impl<B> Service<Request<B>> for ProxyService
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
  type Response = Response<Full<Bytes>>; // Using Full for simplicity now
  type Error = ProxyError; // Use our custom error type
  type Future = std::pin::Pin<
    Box<
      dyn std::future::Future<Output = Result<Self::Response, Self::Error>>
        + Send,
    >,
  >;

  #[instrument(skip(self, req), fields(uri = %req.uri(), method = %req.method()))]
  fn call(&self, req: Request<B>) -> Self::Future {
    let manager = self.process_manager.clone();

    Box::pin(async move {
      // 1. Get Host header
      let host_header = req.headers().get(HOST).and_then(|h| h.to_str().ok());
      let host = match host_header {
        Some(h) => h.split(':').next().unwrap_or(h).to_string(), // Remove port if present
        None => return Err(ProxyError::MissingHost),
      };
      info!(host = %host, "Routing request");

      // 2. Get or spawn process, get socket path
      let socket_path = manager.get_or_spawn_process(&host).await?;
      info!(socket=%socket_path.display(), "Got socket path");

      // 3. Connect to the Unix Socket
      let stream = match UnixStream::connect(&socket_path).await {
        Ok(s) => TokioIo::new(s), // Wrap in TokioIo for Hyper
        Err(e) => {
          error!(host=%host, socket=%socket_path.display(), error=%e, "Failed to connect to upstream socket");
          // Potentially mark the process as dead/stale here? Or let reaper handle it.
          return Err(ProxyError::UpstreamConnectionFailed);
        }
      };
      info!("Connected to Unix socket");

      // 4. Use Hyper client handshake over the Unix Stream
      let (mut sender, conn) = match hyper::client::conn::http1::handshake(
        stream,
      )
      .await
      {
        Ok((s, c)) => (s, c),
        Err(e) => {
          error!(host=%host, socket=%socket_path.display(), error=%e, "Hyper handshake failed with upstream");
          return Err(ProxyError::UpstreamError(format!(
            "Handshake failed: {}",
            e
          )));
        }
      };
      info!("Hyper handshake complete");

      // 5. Spawn the connection task to poll the connection
      // The connection task is responsible for driving the underlying IO.
      // Clone values that will be moved into the async block
      let host_clone = host.clone();
      let socket_path_clone = socket_path.clone();
      tokio::spawn(async move {
        if let Err(e) = conn.await {
          // Log connection errors (e.g., upstream closed unexpectedly)
          warn!(host=%host_clone, socket=%socket_path_clone.display(), error = %e, "Upstream connection error during processing");
        }
        trace!("Upstream connection task finished");
      });

      // 6. Send the request to the upstream Deno process
      info!("Sending request to upstream");
      let upstream_resp = match sender.send_request(req).await {
        Ok(resp) => resp,
        Err(e) => {
          error!(host=%host, socket=%socket_path.display(), error=%e, "Failed to send request upstream");
          return Err(ProxyError::UpstreamError(format!(
            "Send request failed: {}",
            e
          )));
        }
      };
      info!(status = %upstream_resp.status(), "Received response from upstream");

      // 7. Convert response body (assuming simple Full<Bytes> for now)
      // For streaming, you'd use Body::wrap_stream or similar
      let (parts, incoming_body) = upstream_resp.into_parts();
      let body_bytes = http_body_util::BodyExt::collect(incoming_body)
        .await
        .map_err(|e| {
          ProxyError::UpstreamError(format!(
            "Failed to read upstream body: {}",
            e
          ))
        })?
        .to_bytes();

      let response = Response::from_parts(parts, Full::new(body_bytes));

      Ok(response)
    })
  }
}

#[tokio::main]
async fn main() -> Result<()> {
  // Initialize tracing for console logging
  tracing_subscriber::fmt::init();

  let process_manager = Arc::new(ProcessManager::new(DATA_DIR.clone()));

  let reaper_manager = Arc::clone(&process_manager);
  tokio::spawn(async move {
    reaper_manager.start_reaper().await;
  });

  let addr = SocketAddr::from(([0, 0, 0, 0], *DATA_PORT));

  let listener = TcpListener::bind(addr).await?;
  info!("Listening on http://{}", addr);

  // 6. The main accept loop
  loop {
    let (stream, remote_addr) = match listener.accept().await {
      Ok(s) => s,
      Err(e) => {
        // Log listener accept errors (less common)
        error!("Failed to accept connection: {:?}", e);
        continue; // Keep listening
      }
    };
    trace!("Accepted connection from {}", remote_addr);

    // Wrap the raw TCP stream in Hyper's TokioIo adapter
    let io = TokioIo::new(stream);

    // Clone the Arc for the service instance needed by this connection's task.
    // This is cheap as it only bumps the reference count.
    let process_manager_ = process_manager.clone();

    // Spawn a Tokio task for each connection.
    // The `move` keyword moves ownership of `io` and `manager_for_task` into the task.
    tokio::task::spawn(async move {
      // Create an instance of our Service (`ProxyService`) for this connection.
      // It captures the cloned Arc<ProcessManager>.
      let service = ProxyService {
        process_manager: process_manager_,
      };

      // Use hyper's connection builder to serve the connection.
      // It will internally call `service.call` for each request on this connection.
      let builder = http1::Builder::new();

      // Configure the executor (required by hyper-util builders)
      let conn_builder = builder; // http1::Builder doesn't have executor in hyper 1.x

      // Serve the connection.
      // `serve_connection` drives the HTTP state machine for the connection.
      if let Err(err) = conn_builder.serve_connection(io, service).await {
        // Log errors specific to *this* connection (e.g., IO errors,
        // malformed HTTP requests that Hyper rejects before your service sees them).
        // Errors *returned* by your `service.call` (like ProxyError) should ideally
        // be handled by your service logic to produce an appropriate HTTP error response,
        // they typically won't cause `serve_connection` itself to return an Err here unless
        // there's an issue writing the response back, etc.
        warn!("Error serving connection from {}: {:?}", remote_addr, err);
      }
      trace!("Connection task finished for {}", remote_addr);
    });
  }
  // Note: This loop runs forever, so Ok(()) is never reached in practice.
  // You might add signal handling (e.g., Ctrl+C) to break the loop for graceful shutdown.
}

#[cfg(test)]
mod tests {
  use super::*;
  use hyper::service::Service;
  
  #[tokio::test]
  async fn test_proxy_service() {
    // Create a mock request with host header
    let req = Request::builder()
      .uri("http://localhost:3000/foo")
      .header(HOST, "ry.local")
      .body(http_body_util::Empty::<Bytes>::new())
      .unwrap();
      
    // Create the service
    let service = ProxyService {
      process_manager: Arc::new(ProcessManager::new(DATA_DIR.clone())),
    };
    
    // Call the service directly
    let response = service.call(req).await.unwrap();
    assert_eq!(response.status(), hyper::StatusCode::OK);
    
    // Check the response body
    let full_body = response.into_body();
    let collected = http_body_util::BodyExt::collect(full_body).await.unwrap();
    let bytes = collected.to_bytes();
    let body_str = String::from_utf8_lossy(&bytes);
    assert_eq!(body_str, "hello from ry.local");
  }
}
