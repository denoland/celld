mod child_on_parent_exit;
mod deno_benchmark;
mod peer_manager;
mod process_manager;
mod process_reaper;
mod sqlite_replica;
#[cfg(test)]
pub mod test_utils;

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
use tracing::{error, debug};

use peer_manager::PeerManager;
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

  debug!("Using DATA_DIR: {}", path.display());
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
  peer_manager: PeerManager,
}

#[derive(Debug, Default)]
pub struct MyCtx {
  tenant: String,
  room_id: Option<String>,
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
      let default_room = "default-room".to_string();
      let room_id = ctx.room_id.as_ref().unwrap_or(&default_room);

      let _ = self
        .process_manager
        .decrement_connection_count(&ctx.tenant, room_id)
        .await;
    }
  }

  async fn request_filter(
    &self,
    session: &mut Session,
    ctx: &mut Self::CTX,
  ) -> Result<bool> {
    // Track request start time
    let filter_start = std::time::Instant::now();
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

    // Get the path
    let path = req_header.uri.path();

    // Special endpoints for mesh network debugging

    // Endpoint to list all peers in the mesh
    // TODO this shouldn't be on data_port but instead on control_port
    if path == "/_mesh/peers" {
      let peers = self.peer_manager.get_all_peers();
      let local_peer = self.peer_manager.get_local_peer();

      // Build a JSON array of peers
      let mut peer_json = String::from("[");
      for (i, peer) in peers.iter().enumerate() {
        if i > 0 {
          peer_json.push(',');
        }
        peer_json.push_str(&format!(
          "{{\"address\":\"{}\",\"is_local\":{}}}",
          peer,
          peer == local_peer
        ));
      }
      peer_json.push(']');

      // Return a JSON response with all peers
      let response = format!(
        "{{\"peers\":{},\"count\":{},\"local\":\"{}\"}}",
        peer_json,
        peers.len(),
        local_peer
      );

      let content_length = response.len();
      let mut resp =
        pingora::http::ResponseHeader::build(StatusCode::OK, Some(2)).unwrap();
      resp
        .insert_header(http::header::CONTENT_LENGTH, content_length.to_string())
        .unwrap();
      resp
        .insert_header(http::header::CONTENT_TYPE, "application/json")
        .unwrap();

      session.write_response_header(Box::new(resp), false).await?;
      session
        .write_response_body(Some(response.into()), true)
        .await?;
      session.set_keepalive(None);
      return Ok(true);
    }

    // Endpoint to check which peer is responsible for a room
    // TODO this shouldn't be on data_port but instead on control_port
    if let Some(room_id) = path.strip_prefix("/_mesh/owner/") {
      if !room_id.is_empty() {
        let owner = self.peer_manager.get_owner_peer(room_id);
        let is_local = self.peer_manager.is_local_owner(room_id);

        // Return a simple JSON response with owner information
        let response = format!(
          "{{\"room_id\":\"{}\",\"owner\":\"{}\",\"is_local\":{}}}",
          room_id, owner, is_local
        );

        let content_length = response.len();
        let mut resp =
          pingora::http::ResponseHeader::build(StatusCode::OK, Some(2))
            .unwrap();
        resp
          .insert_header(
            http::header::CONTENT_LENGTH,
            content_length.to_string(),
          )
          .unwrap();
        resp
          .insert_header(http::header::CONTENT_TYPE, "application/json")
          .unwrap();

        session.write_response_header(Box::new(resp), false).await?;
        session
          .write_response_body(Some(response.into()), true)
          .await?;
        session.set_keepalive(None);
        return Ok(true);
      }
    }

    // Check if this is a /room/* path - if so, let the default proxy path handle it
    if let Some(room_path) = path.strip_prefix("/room/") {
      if !room_path.is_empty() {
        // Store the room ID as the first path segment
        let room_id = room_path.split('/').next().unwrap_or(room_path);
        // Store the room ID in the context for later use
        ctx.room_id = Some(room_id.to_string());
        return Ok(false); // Let it be handled by the upstream_peer method
      }
    }

    // Process the path and handle static files for non-room paths
    let rel_path = path.trim_start_matches('/');

    // Create a String to store our modified path
    let rel_path_ = if rel_path.is_empty() || rel_path.ends_with('/') {
      format!("{}index.html", rel_path)
    } else {
      rel_path.to_string()
    };

    // Construct the file path
    let tenant_dir = self.process_manager.data_dir.join(&ctx.tenant);
    let static_dir = tenant_dir.join("static");
    let file_path = static_dir.join(&rel_path_);

    // Try to read the file
    let file = match std::fs::read(&file_path) {
      Ok(file) => file,
      Err(_) => {
        debug!("File not found: {}", file_path.display());
        return Err(pingora::Error::explain(
          ErrorType::HTTPStatus(StatusCode::NOT_FOUND.into()),
          "Not found",
        ));
      }
    };

    // Determine content type based on file extension
    let content_type = match rel_path_.rsplit('.').next() {
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
    // Start timing the request path
    let request_start = std::time::Instant::now();

    // Check for the single-use header
    let single_use = session
      .req_header()
      .headers
      .contains_key("x-single-use-isolate");

    // Get the room_id from the context, or use a default value
    let room_id = match &ctx.room_id {
      Some(id) => id.as_str(),
      None => "default-room", // Default room ID if none specified
    };

    debug!(
      host = %ctx.tenant,
      room_id = %room_id,
      single_use = %single_use,
      request_init_time = ?request_start.elapsed(),
      "Processing request"
    );

    // Check if this instance is responsible for this room
    if !self.peer_manager.is_local_owner(room_id) {
      // We need to forward this request to the responsible peer
      let upstream_addr = self.peer_manager.get_owner_peer(room_id);

      debug!(
        host = %ctx.tenant,
        room_id = %room_id,
        responsible_peer = %upstream_addr,
        "Forwarding request to responsible peer"
      );

      // Create a Backend using the responsible peer's socket address
      let sni = ctx.tenant.clone();

      // Create HTTP peer for the remote peer
      let peer = HttpPeer::new(upstream_addr, false, sni);

      debug!(
        host = %ctx.tenant,
        room_id = %room_id,
        upstream = %upstream_addr,
        "Selected remote peer"
      );
      return Ok(Box::new(peer));
    }

    // We are the responsible peer, so handle the request locally
    debug!(
      host = %ctx.tenant,
      room_id = %room_id,
      "This instance is responsible for handling the request"
    );

    let socket_path: PathBuf = {
      match self
        .process_manager
        .get_or_spawn_process(&ctx.tenant, room_id, single_use)
        .await
      {
        Ok((path, _stream)) => {
          // We only need the path, Pingora will handle the connection
          // Increment active connection count
          let default_room = "default-room".to_string();
          let room_id = ctx.room_id.as_ref().unwrap_or(&default_room);
          self
            .process_manager
            .increment_connection_count(&ctx.tenant, room_id)
            .await;
          path
        }
        Err(ProxyError::AppNotFound(host_not_found)) => {
          debug!("Application not found for host: {}", host_not_found);
          return Err(pingora::Error::explain(
            ErrorType::HTTPStatus(StatusCode::NOT_FOUND.into()),
            format!("App not found: {}", host_not_found),
          ));
        }
        Err(ProxyError::InvalidHost) => {
          debug!("Invalid hostname format: {}", ctx.tenant);
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
    debug!(
      process_manager_time = ?request_start.elapsed(),
      "Process manager get_or_spawn_process completed"
    );

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
    let peer_start = std::time::Instant::now();
    let sni = ctx.tenant.clone();
    match HttpPeer::new_uds(&socket_path_str, false, sni) {
      Ok(peer) => {
        debug!(
          host = %ctx.tenant,
          socket = %socket_path.display(),
          uds_peer_creation_time = ?peer_start.elapsed(),
          total_time_so_far = ?request_start.elapsed(),
          "Selected upstream UDS peer"
        );
        // Assume anything after this point is handled by Pingora proxy machinery
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
fn start_server(
  data_dir: PathBuf,
  data_port: u16,
  known_peers: Vec<String>,
  self_addr: Option<String>,
) -> Server {
  // Create a server configuration
  let server_conf = Arc::new(ServerConf::new().unwrap());

  // Create a new Pingora server
  let mut server = Server::new(None).unwrap();

  let self_addr =
    self_addr.unwrap_or_else(|| format!("127.0.0.1:{}", data_port));

  // Create the process manager with default timeout values
  let process_manager = ProcessManager::new(data_dir.clone());

  // Create the proxy app that will handle routing
  let app = DenoProxyApp {
    process_manager: process_manager.clone(),
    peer_manager: {
      let peer_manager = PeerManager::new(known_peers, self_addr.clone());
      debug!(
        "Peer manager initialized with {} peers",
        peer_manager.num_peers()
      );
      peer_manager
    },
  };

  // Create an HTTP proxy service with our app
  let mut proxy_service = http_proxy_service(&server_conf, app);

  // Configure the proxy service to listen on the specified port
  proxy_service.add_tcp(&self_addr);

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

  debug!("Starting Deno Deploy proxy server on port {}", data_port);
  server
}

fn main() {
  tracing_subscriber::fmt::init();

  // Check if we should run the benchmark
  if std::env::var("DENO_BENCH").unwrap_or_default() == "1" {
    // Get iteration count from environment variable
    let iterations = std::env::var("BENCH_ITERATIONS")
      .unwrap_or_else(|_| "100".to_string())
      .parse::<usize>()
      .unwrap_or(100);

    println!(
      "Running Deno coldstart benchmark (iterations: {})...",
      iterations
    );

    // Create a tokio runtime for the benchmark
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
      deno_benchmark::benchmark_deno_coldstart(iterations)
        .await
        .unwrap();
    });
    return;
  }

  // Normal server startup
  let self_addr = std::env::var("SELF_ADDR")
    .ok()
    .map(|s| s.trim().to_string());
  let known_peers = std::env::var("KNOWN_PEERS")
    .unwrap_or_default()
    .split(',')
    .map(str::trim)
    .filter(|s| !s.is_empty())
    .map(String::from)
    .collect::<Vec<_>>();

  let server =
    start_server(DATA_DIR.clone(), *DATA_PORT, known_peers, self_addr);
  server.run_forever();
}

#[cfg(test)]
mod tests {
  use super::*;
  use futures_util::{SinkExt, StreamExt};
  use serde_json::Value;
  use std::time::Duration;
  use tokio_tungstenite::tungstenite::protocol::Message;

  // inspired by https://github.com/cloudflare/pingora/blob/caa6a0/pingora-core/tests/utils/mod.rs
  pub static TEST_SERVER: Lazy<std::thread::JoinHandle<()>> = Lazy::new(|| {
    let data_dir = PathBuf::from("./data");
    // Ensure we're using a clean environment for the test server
    std::env::set_var("KNOWN_PEERS", ""); // Empty string forces standalone mode
    std::env::set_var("DATA_PORT", "6146"); // Explicitly set port for test server
    assert_eq!(*DATA_PORT, 6146);

    let h = std::thread::spawn(|| {
      let server = start_server(data_dir, 6146, vec![], None);
      server.run_forever();
    });

    // Give more time for the server to initialize completely
    std::thread::sleep(Duration::from_secs(3));
    h
  });

  pub fn init() {
    let _ = *TEST_SERVER;
  }

  #[tokio::test]
  async fn test_proxy_with_ephemeral_port() {
    init();

    // Give the server a moment to fully initialize
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = reqwest::Client::new()
      .get("http://127.0.0.1:6146/room/foo")
      .header("Host", "hello.localhost")
      .timeout(Duration::from_secs(5)) // Add a timeout to prevent hanging
      .send()
      .await
      .unwrap()
      .text()
      .await
      .unwrap();
    assert_eq!("hello from hello.localhost\n", response);
  }

  #[tokio::test]
  async fn test_static_file_serving() {
    init();

    // Test fetching the index.html file
    for x in ["/", "/index.html"] {
      let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:6146{}", x))
        .header("Host", "hello.localhost")
        .send()
        .await
        .unwrap();
      assert_eq!(response.status(), 200);
      assert_eq!(response.headers().get("content-type").unwrap(), "text/html");
      let content = response.text().await.unwrap();
      assert_eq!(content, "<h1>Hello from hello.localhost</h1>\n");
    }
  }

  #[tokio::test]
  async fn basic_db() {
    init();

    // Use a unique room name for this test
    let room_name = format!(
      "test-db-{}",
      std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
    );

    // Make first request to room
    let first_response = reqwest::Client::new()
      .get(format!("http://127.0.0.1:6146/room/{}", room_name))
      .header("Host", "basic-db.localhost")
      .send()
      .await
      .unwrap()
      .text()
      .await
      .unwrap();
    assert_eq!(first_response.trim(), "1");

    // Verify SQLite database exists and has correct record count
    let db_path = format!("data/basic-db.localhost/sqlite/{}.db", room_name);
    assert!(std::path::Path::new(&db_path).exists());

    let output = std::process::Command::new("sqlite3")
      .arg(&db_path)
      .arg("SELECT COUNT(*) FROM requests;")
      .output()
      .expect("Failed to execute sqlite3 command");
    let count_str = String::from_utf8_lossy(&output.stdout).to_string();
    let count = count_str.trim();
    assert_eq!(
      count, "1",
      "Database should have 1 record after first request"
    );

    // Make second request to same room
    let second_response = reqwest::Client::new()
      .get(format!("http://127.0.0.1:6146/room/{}", room_name))
      .header("Host", "basic-db.localhost")
      .send()
      .await
      .unwrap()
      .text()
      .await
      .unwrap();
    assert_eq!(second_response.trim(), "2");

    // Database should reflect updated count
    let output = std::process::Command::new("sqlite3")
      .arg(&db_path)
      .arg("SELECT COUNT(*) FROM requests;")
      .output()
      .expect("Failed to execute sqlite3 command");
    let count_str = String::from_utf8_lossy(&output.stdout).to_string();
    let count = count_str.trim();
    assert_eq!(
      count, "2",
      "Database should have 2 records after second request"
    );
  }

  /// Helper function to connect to a WebSocket room and handle initial messages
  async fn connect_to_room(
    room_id: &str,
  ) -> (
    tokio_tungstenite::WebSocketStream<
      tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    String, // username
  ) {
    // Create URL with proper host header in the URL
    let url = format!("ws://ws-echo.localhost:6146/room/{}", room_id);

    // Add a small delay before connecting to ensure the server is ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (mut ws_stream, _) = tokio_tungstenite::connect_async(url)
      .await
      .expect(&format!("Failed to connect to room {}", room_id));

    // Read welcome message
    let welcome_msg = ws_stream.next().await.unwrap().unwrap();
    let welcome_data: Value =
      serde_json::from_str(&welcome_msg.to_string()).unwrap();
    assert_eq!(welcome_data["type"], "welcome");
    let username = welcome_data["username"].as_str().unwrap().to_string();

    // Read userlist message
    let userlist_msg = ws_stream.next().await.unwrap().unwrap();
    let userlist_data: Value =
      serde_json::from_str(&userlist_msg.to_string()).unwrap();
    assert_eq!(userlist_data["type"], "userlist");

    (ws_stream, username)
  }

  #[tokio::test]
  async fn test_websocket_echo() {
    init();

    // Connect to room
    let (mut ws_stream, _) = connect_to_room("test-room").await;

    // Send a test message
    let test_message = "Hello WebSocket";
    ws_stream
      .send(Message::Text(test_message.to_string().into()))
      .await
      .unwrap();

    // Receive the chat message response
    let chat_msg = ws_stream.next().await.unwrap().unwrap();
    let chat_data: Value = serde_json::from_str(&chat_msg.to_string()).unwrap();

    // Verify message content
    assert_eq!(chat_data["type"], "chat");
    assert_eq!(chat_data["message"], test_message);
    assert!(
      chat_data.get("username").is_some(),
      "Chat message should include username"
    );

    // Clean up
    ws_stream.close(None).await.unwrap();
  }

  #[tokio::test]
  async fn test_websocket_broadcast() {
    init();

    // Connect first client to the room
    let (mut client1, username1) = connect_to_room("broadcast-test").await;

    // Add a small delay to ensure the first client is fully registered
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect second client to the same room
    let (mut client2, _) = connect_to_room("broadcast-test").await;

    // Client 1 should receive join notification and updated user list
    for _ in 0..2 {
      let msg = client1.next().await.unwrap().unwrap();
      let data: Value = serde_json::from_str(&msg.to_string()).unwrap();
      match data["type"].as_str().unwrap() {
        "system" => assert!(data["message"]
          .as_str()
          .unwrap()
          .contains("has joined the room")),
        "userlist" => {
          let users = data["users"].as_array().unwrap();
          assert_eq!(users.len(), 2, "Should have 2 users in the room");
        }
        _ => {}
      }
    }

    // Send a chat message from client 1
    let chat_message = "Hello everyone!";
    client1
      .send(Message::Text(chat_message.to_string().into()))
      .await
      .unwrap();

    // Both clients should receive the message
    let msg1 = client1.next().await.unwrap().unwrap();
    let msg_data1: Value = serde_json::from_str(&msg1.to_string()).unwrap();
    assert_eq!(msg_data1["type"], "chat");
    assert_eq!(msg_data1["message"], chat_message);

    let msg2 = client2.next().await.unwrap().unwrap();
    let msg_data2: Value = serde_json::from_str(&msg2.to_string()).unwrap();
    assert_eq!(msg_data2["type"], "chat");
    assert_eq!(msg_data2["message"], chat_message);
    assert_eq!(msg_data2["username"], username1);

    client1.close(None).await.unwrap();
    client2.close(None).await.unwrap();
  }

  #[tokio::test]
  async fn test_separate_isolates_per_room() {
    init();

    // Connect to two different rooms
    let (mut client1, username1) = connect_to_room("room-1").await;
    let (mut client2, username2) = connect_to_room("room-2").await;

    // Send a message in room-1
    let message_room1 = "This message should only be in room-1";
    client1
      .send(Message::Text(message_room1.to_string().into()))
      .await
      .unwrap();

    // Client in room-1 should receive the message
    let msg1 = client1.next().await.unwrap().unwrap();
    let msg_data1: Value = serde_json::from_str(&msg1.to_string()).unwrap();
    assert_eq!(msg_data1["type"], "chat");
    assert_eq!(msg_data1["message"], message_room1);
    assert_eq!(msg_data1["username"], username1);

    // Send a message in room-2
    let message_room2 = "This message should only be in room-2";
    client2
      .send(Message::Text(message_room2.to_string().into()))
      .await
      .unwrap();

    // Client in room-2 should receive the message
    let msg2 = client2.next().await.unwrap().unwrap();
    let msg_data2: Value = serde_json::from_str(&msg2.to_string()).unwrap();
    assert_eq!(msg_data2["type"], "chat");
    assert_eq!(msg_data2["message"], message_room2);
    assert_eq!(msg_data2["username"], username2);

    // Verify isolation: room-1 should not receive messages sent to room-2
    let timeout_duration = Duration::from_millis(300);
    tokio::select! {
      maybe_msg = tokio::time::timeout(timeout_duration, client1.next()) => {
        if let Ok(Some(Ok(_))) = maybe_msg {
          panic!("Room isolation failure: room-1 received a message from room-2");
        }
      }
      _ = tokio::time::sleep(timeout_duration) => {
        // Expected case: timeout without receiving cross-room message
      }
    }

    client1.close(None).await.unwrap();
    client2.close(None).await.unwrap();
  }
}
