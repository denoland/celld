mod process_manager;

use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::{combinators::BoxBody, Full};
use hyper::server::conn::http1;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::fs;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tracing::{error, info, instrument, warn};


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


static DATA_PORT: Lazy<u16> = Lazy::new(|| {
  env::var("DATA_PORT")
    .ok()
    .and_then(|s| s.parse().ok()) // If Some(string), attempts parse -> Option<u16>
    .map_or_else(|| 3000u16, |port| port)
});

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

      // Steps 3-7 (Connecting to socket, handshake, proxying) are removed for now.

      // 3. Return a simple "Hello World" response for now.
      info!("Skipping upstream connection, returning placeholder response.");
      let response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, HeaderValue::from_static("text/plain"))
        .body(Full::new(Bytes::from("Hello World")))
        // If Response::builder fails (highly unlikely for static values), wrap it in our error type
        .map_err(|e| {
          ProxyError::InternalError(anyhow::anyhow!(
            "Failed to build static response: {}",
            e
          ))
        })?;

      Ok(response)
    })
  }
}

#[tokio::main]
async fn main() {
  let data_dir = env::var("DATA").unwrap_or_else(|_| "./data".to_string());
  let data_path = PathBuf::from(&data_dir);

  // Create deploy_data directory if it doesn't exist
  if !data_path.exists() {
    println!("Creating deploy_data directory: {}", data_dir);
    fs::create_dir_all(&data_path).await?;
  }

  let process_manager = ProcessManager::new(data_path);

  let addr = SocketAddr::from(([127, 0, 0, 1], *DATA_PORT));
  let listener = TcpListener::bind(addr).await?;

  println!("Proxy server listening on {}", addr);
  println!("Using DATA directory: {}", data_path.display());

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
    builder.serve(hyper::service::make_service_fn(|_conn| make_svc()));

  // Run the server
  if let Err(e) = server.await {
    error!("Server error: {}", e);
    return Err(e.into());
  }

  Ok(())
}
