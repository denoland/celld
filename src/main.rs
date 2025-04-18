mod process_manager;
mod process_reaper;

use once_cell::sync::Lazy;
use pingora::http::StatusCode;
use pingora::prelude::*;
use pingora::server::configuration::ServerConf;
use pingora::services::background::background_service;
use pingora::upstreams::peer::HttpPeer;
use pingora::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use process_manager::ProcessManager;
use process_reaper::ProcessReaper;

// Default values, can be overridden when creating ProcessManager
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_REAPER_INTERVAL: Duration = Duration::from_secs(10);

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

/// DenoProxyApp implements the HTTP proxy service for Deno processes
struct DenoProxyApp {
  process_manager: ProcessManager,
}

#[derive(Debug, Default)]
pub struct MyCtx {
  tenant: String,
}

#[async_trait::async_trait]
impl ProxyHttp for DenoProxyApp {
  type CTX = MyCtx;

  // Required implementation of new_ctx
  fn new_ctx(&self) -> Self::CTX {
    MyCtx::default()
  }

  // Called when the entire response is sent to the downstream, or when there is a fatal error
  async fn logging(
    &self,
    _session: &mut Session,
    _e: Option<&Error>,
    ctx: &mut Self::CTX,
  ) {
    if !ctx.tenant.is_empty() {
      let _ = self
        .process_manager
        .decrement_connection_count(&ctx.tenant)
        .await;
    }
  }

  async fn request_filter(
    &self,
    session: &mut Session,
    ctx: &mut Self::CTX,
  ) -> Result<bool> {
    let req_header = session.req_header();

    // Extract and validate host header
    let host =
      if let Some(header_value) = req_header.headers.get(http::header::HOST) {
        header_value.to_str().map_err(|_| {
          error!("Host header contains invalid characters");
          pingora::Error::explain(
            ErrorType::HTTPStatus(StatusCode::BAD_REQUEST.into()),
            "Invalid Host header encoding",
          )
        })?
      } else {
        error!("Missing host header");
        return Err(pingora::Error::explain(
          ErrorType::HTTPStatus(StatusCode::BAD_REQUEST.into()),
          "Missing Host header",
        ));
      };

    // Extract hostname without port
    let hostname = host.split(':').next().unwrap_or(host);
    ctx.tenant = hostname.to_string();

    // Only handle GET and HEAD requests
    if req_header.method != http::Method::GET
      && req_header.method != http::Method::HEAD
    {
      return Ok(false);
    }

    // Process the path and handle static files
    let rel_path = req_header.uri.path().trim_start_matches('/');

    // Create a String to store our modified path
    let rel_path_ = if rel_path.is_empty() || rel_path.ends_with('/') {
      format!("{}index.html", rel_path)
    } else {
      rel_path.to_string()
    };

    // Construct the file path
    let tenant_dir = self.process_manager.data_dir.join(&ctx.tenant);
    let static_dir = tenant_dir.join("static");
    let file_path = static_dir.join(rel_path_);

    // Try to read the file
    let file = match std::fs::read(&file_path) {
      Ok(file) => file,
      Err(_) => {
        info!("File not found: {}", file_path.display());
        return Ok(false);
      }
    };

    // Determine content type based on file extension
    let content_type = match rel_path.rsplit('.').next() {
      Some("html") | Some("htm") => "text/html",
      Some("css") => "text/css",
      Some("js") => "application/javascript",
      Some("json") => "application/json",
      Some("png") => "image/png",
      Some("jpg") | Some("jpeg") => "image/jpeg",
      Some("gif") => "image/gif",
      Some("svg") => "image/svg+xml",
      Some("webp") => "image/webp",
      Some("ico") => "image/x-icon",
      Some("woff") => "font/woff",
      Some("woff2") => "font/woff2",
      Some("ttf") => "font/ttf",
      Some("txt") => "text/plain",
      Some("pdf") => "application/pdf",
      Some("xml") => "application/xml",
      _ => "application/octet-stream",
    };

    let content_length = file.len();

    // Build and send response
    let mut resp =
      pingora::http::ResponseHeader::build(StatusCode::OK, Some(2)).unwrap();
    resp
      .insert_header(http::header::CONTENT_LENGTH, content_length.to_string())
      .unwrap();
    resp
      .insert_header(http::header::CONTENT_TYPE, content_type)
      .unwrap();

    let end_of_stream = req_header.method == http::Method::HEAD;
    session
      .write_response_header(Box::new(resp), end_of_stream)
      .await?;

    if !end_of_stream {
      session.write_response_body(Some(file.into()), true).await?;
    }

    session.set_keepalive(None);
    Ok(true)
  }

  // This method is called for each HTTP request to determine the upstream server
  async fn upstream_peer(
    &self,
    session: &mut Session,
    ctx: &mut Self::CTX,
  ) -> pingora::Result<Box<HttpPeer>> {
    // Check for the single-use header
    let single_use = session
      .req_header()
      .headers
      .contains_key("x-single-use-isolate");

    info!(
      host = %ctx.tenant,
      single_use = %single_use,
      "Processing request"
    );

    // Get or spawn the process
    let socket_path: PathBuf = {
      match self
        .process_manager
        .get_or_spawn_process(&ctx.tenant, single_use)
        .await
      {
        Ok((path, _stream)) => {
          // We only need the path, Pingora will handle the connection
          // Increment active connection count
          self
            .process_manager
            .increment_connection_count(&ctx.tenant)
            .await;
          path
        }
        Err(ProxyError::AppNotFound(host_not_found)) => {
          info!("Application not found for host: {}", host_not_found);
          return Err(pingora::Error::explain(
            ErrorType::HTTPStatus(StatusCode::NOT_FOUND.into()),
            format!("App not found: {}", host_not_found),
          ));
        }
        Err(ProxyError::InvalidHost) => {
          info!("Invalid hostname format: {}", ctx.tenant);
          return Err(pingora::Error::explain(
            ErrorType::HTTPStatus(StatusCode::BAD_REQUEST.into()),
            "Invalid hostname format provided",
          ));
        }
        Err(e) => {
          error!("Error getting or spawning process: {:?}", e);
          return Err(pingora::Error::explain(
            ErrorType::HTTPStatus(StatusCode::INTERNAL_SERVER_ERROR.into()),
            "Internal server error during process lookup",
          ));
        }
      }
    }; // Mutex guard dropped here

    // Configure backend using the Unix Domain Socket
    let socket_path_str = match socket_path.to_str() {
      Some(s) => s.to_string(),
      None => {
        error!("Invalid UTF-8 in socket path: {:?}", socket_path);
        return Err(pingora::Error::explain(
          ErrorType::HTTPStatus(StatusCode::INTERNAL_SERVER_ERROR.into()),
          "Invalid backend path encoding",
        ));
      }
    };

    // Create a Backend using the Unix Domain Socket address
    let sni = ctx.tenant.clone();
    match HttpPeer::new_uds(&socket_path_str, false, sni) {
      Ok(peer) => {
        info!(
          host = %ctx.tenant,
          socket = %socket_path.display(),
          "Selected upstream UDS peer"
        );
        Ok(Box::new(peer))
      }
      Err(e) => {
        error!("Failed to create HTTP peer: {:?}", e);
        Err(pingora::Error::because(
          ErrorType::HTTPStatus(StatusCode::SERVICE_UNAVAILABLE.into()),
          "Failed to connect to upstream application",
          e,
        ))
      }
    }
  }
}

/// Starts the server with the given data directory and port
/// Returns the server instance
fn start_server(data_dir: PathBuf, port: u16) -> Server {
  // Create a server configuration
  let server_conf = Arc::new(ServerConf::new().unwrap());

  // Create a new Pingora server
  let mut server = Server::new(None).unwrap();

  // Create the process manager with default timeout values
  let process_manager = ProcessManager::new(data_dir.clone());

  // Create the proxy app that will handle routing
  let app = DenoProxyApp {
    process_manager: process_manager.clone(),
  };

  // Create an HTTP proxy service with our app
  let mut proxy_service = http_proxy_service(&server_conf, app);

  // Configure the proxy service to listen on the specified port
  let listen_addr = format!("0.0.0.0:{}", port);
  proxy_service.add_tcp(&listen_addr);

  let reaper_service = background_service(
    "process_reaper",
    ProcessReaper::new(
      process_manager.clone(),
      DEFAULT_IDLE_TIMEOUT,
      DEFAULT_REAPER_INTERVAL,
    ),
  );
  server.add_service(reaper_service);

  // Add the proxy service to the server
  server.add_service(proxy_service);

  info!("Starting Deno Deploy proxy server on port {}", port);
  server
}

fn main() {
  tracing_subscriber::fmt::init();
  let server = start_server(DATA_DIR.clone(), *DATA_PORT);
  server.run_forever();
}

#[cfg(test)]
mod tests {
  use super::*;

  // inspired by https://github.com/cloudflare/pingora/blob/caa6a0/pingora-core/tests/utils/mod.rs
  pub static TEST_SERVER: Lazy<std::thread::JoinHandle<()>> = Lazy::new(|| {
    let data_dir = PathBuf::from("./data");
    let h = std::thread::spawn(|| {
      let server = start_server(data_dir, 6146);
      server.run_forever();
    });
    std::thread::sleep(Duration::from_secs(2));
    h
  });

  pub fn init() {
    let _ = *TEST_SERVER;
  }

  #[tokio::test]
  async fn test_proxy_with_ephemeral_port() {
    init();

    let response = reqwest::Client::new()
      .get("http://127.0.0.1:6146/foo")
      .header("Host", "ry.local")
      .send()
      .await
      .unwrap()
      .text()
      .await
      .unwrap();
    assert_eq!("hello from ry.local\n", response);
  }

  #[tokio::test]
  async fn test_static_file_serving() {
    init();

    // Test fetching the index.html file
    for x in ["/", "/index.html"] {
      let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:6146{}", x))
        .header("Host", "ry.local")
        .send()
        .await
        .unwrap();
      assert_eq!(response.status(), 200);
      //assert_eq!(response.headers().get("content-type").unwrap(), "text/html");
      let content = response.text().await.unwrap();
      assert_eq!(content, "<h1>Hello from ry.local</h1>\n");
    }
  }
}
