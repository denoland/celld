use once_cell::sync::Lazy;
use pingora::http::StatusCode;
use pingora::prelude::*;
use pingora::server::configuration::ServerConf;
use pingora::services::background::background_service;
use pingora::upstreams::peer::HttpPeer;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

mod process_manager;
mod process_reaper;

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

#[async_trait::async_trait]
impl ProxyHttp for DenoProxyApp {
  type CTX = ();

  // Required implementation of new_ctx
  fn new_ctx(&self) -> Self::CTX {
    ()
  }

  // This method is called for each HTTP request to determine the upstream server
  async fn upstream_peer(
    &self,
    session: &mut Session,
    _ctx: &mut Self::CTX,
  ) -> pingora::Result<Box<HttpPeer>> {
    let req_header = session.req_header();

    let host =
      if let Some(header_value) = req_header.headers.get(http::header::HOST) {
        // Assign to host_with_port if conversion succeeds, otherwise propagate error
        header_value.to_str().map_err(|_| {
          error!("Host header contains invalid characters");
          pingora::Error::explain(
            ErrorType::HTTPStatus(StatusCode::BAD_REQUEST.into()),
            "Invalid Host header encoding",
          )
        })?
      } else {
        // Header is missing, return error Result
        error!("Missing host header");
        return Err(pingora::Error::explain(
          ErrorType::HTTPStatus(StatusCode::BAD_REQUEST.into()),
          "Missing Host header",
        ));
      };

    // Check for the single-use header
    let single_use = session
      .req_header()
      .headers
      .contains_key("x-single-use-isolate");

    info!(
      host = %host,
      single_use = %single_use,
      "Processing request"
    );

    // Get or spawn the process
    let socket_path: PathBuf = {
      match self
        .process_manager
        .get_or_spawn_process(&host, single_use)
        .await
      {
        Ok((path, _stream)) => {
          // We only need the path, Pingora will handle the connection
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
          info!("Invalid hostname format: {}", host);
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
    let sni = format!("{}.local", host); // Dummy SNI name, not actually used for UDS
    match HttpPeer::new_uds(&socket_path_str, false, sni) {
      Ok(peer) => {
        info!(
          host = %host,
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
  let process_manager = ProcessManager::new(data_dir);

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
      .get("http://127.0.0.1:6146/")
      .header("Host", "ry.local")
      .send()
      .await
      .unwrap()
      .text()
      .await
      .unwrap();
    assert_eq!("hello from ry.local\n", response);
  }
}
