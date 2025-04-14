use anyhow::{Context, Result};
use async_trait::async_trait;
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::HOST;
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper::{Request, Response};
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // For manual proxying if needed
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};
use tracing_subscriber::{
  layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};
use uuid::Uuid;

const IDLE_TIMEOUT_SECONDS: u64 = 300; // 5 minutes
const REAPER_INTERVAL_SECONDS: u64 = 60; // Check every minute

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
  apps_dir: PathBuf,
  processes: SharedProcessMap,
  idle_timeout: Duration,
}

impl ProcessManager {
  fn new(apps_dir: PathBuf, idle_timeout: Duration) -> Self {
    ProcessManager {
      apps_dir,
      processes: Arc::new(Mutex::new(HashMap::new())),
      idle_timeout,
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

    let app_code_dir = self.apps_dir.join(host).join("code");
    let main_script = app_code_dir.join("main.ts");
    let sockets_dir = self.apps_dir.join(host).join("sockets");

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

#[derive(Clone)]
struct ProxyService {
  process_manager: Arc<ProcessManager>,
}

impl Service<Request<Incoming>> for ProxyService {
  type Response = Response<Full<Bytes>>; // Using Full for simplicity now
  type Error = ProxyError; // Use our custom error type
  type Future = std::pin::Pin<
    Box<
      dyn std::future::Future<Output = Result<Self::Response, Self::Error>>
        + Send,
    >,
  >;

  #[instrument(skip(self, req), fields(uri = %req.uri(), method = %req.method()))]
  fn call(&self, req: Request<Incoming>) -> Self::Future {
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
      let (mut sender, conn) = match http1::handshake(stream).await {
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
      tokio::spawn(async move {
        if let Err(e) = conn.await {
          // Log connection errors (e.g., upstream closed unexpectedly)
          warn!(host=%host, socket=%socket_path.display(), error = %e, "Upstream connection error during processing");
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

// Helper function to convert ProxyError to a Hyper Response
fn error_response(err: ProxyError) -> Response<Full<Bytes>> {
  let status = match err {
    ProxyError::MissingHost | ProxyError::InvalidHost => {
      hyper::StatusCode::BAD_REQUEST
    }
    ProxyError::AppNotFound(_) => hyper::StatusCode::NOT_FOUND,
    ProxyError::InternalError(_) => hyper::StatusCode::INTERNAL_SERVER_ERROR,
    ProxyError::UpstreamError(_) | ProxyError::UpstreamConnectionFailed => {
      hyper::StatusCode::BAD_GATEWAY
    }
  };

  // Log internal errors fully
  if matches!(err, ProxyError::InternalError(_))
    || matches!(err, ProxyError::UpstreamError(_))
  {
    error!("Responding with status {}: {}", status, err);
  } else {
    warn!("Responding with status {}: {}", status, err);
  }

  Response::builder()
    .status(status)
    .header(hyper::header::CONTENT_TYPE, "text/plain")
    .body(Full::new(Bytes::from(err.to_string())))
    .unwrap_or_else(|_| {
      Response::builder()
        .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
        .body(Full::new(Bytes::from("Internal Server Error")))
        .unwrap()
    }) // Should not fail
}

#[tokio::main]
async fn main() -> Result<()> {
  // Setup tracing subscriber
  tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())
    .with(
      EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info")),
    ) // Default to 'info'
    .init();

  info!("Starting Deno Host MVP...");

  // 1. Read APPS env var
  let apps_dir_str =
    std::env::var("APPS").context("APPS environment variable not set")?;
  let apps_dir = PathBuf::from(apps_dir_str);
  if !apps_dir.is_dir() {
    anyhow::bail!(
      "APPS directory not found or not a directory: {}",
      apps_dir.display()
    );
  }
  info!("Using APPS directory: {}", apps_dir.display());

  // 2. Create Process Manager
  let idle_timeout = Duration::from_secs(IDLE_TIMEOUT_SECONDS);
  let process_manager =
    Arc::new(ProcessManager::new(apps_dir.clone(), idle_timeout));

  // 3. Start the reaper task
  let reaper_manager = Arc::clone(&process_manager);
  tokio::spawn(async move {
    reaper_manager.start_reaper().await;
  });

  // 4. Setup Hyper Server
  let addr = SocketAddr::from(([0, 0, 0, 0], 3000)); // Listen on 0.0.0.0:3000
  info!("Listening on http://{}", addr);

  // Create a service maker function
  let make_svc = move || {
    let manager_clone = Arc::clone(&process_manager);
    async move {
      Ok::<_, Infallible>(hyper::service::service_fn(
        move |req: Request<Incoming>| {
          let service = ProxyService {
            process_manager: manager_clone.clone(),
          };
          async move {
            match service.call(req).await {
              Ok(resp) => Ok(resp),
              Err(e) => Ok(error_response(e)), // Convert our error to a response
            }
          }
        },
      ))
    }
  };

  // Build the server
  let builder = hyper::server::Server::bind(&addr);
  let server =
    builder.serve(hyper::service::make_service_fn(|_conn| make_svc())); // Use make_service_fn

  // Run the server
  if let Err(e) = server.await {
    error!("Server error: {}", e);
    return Err(e.into());
  }

  Ok(())
}
