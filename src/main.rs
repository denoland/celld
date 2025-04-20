mod child_on_parent_exit;
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

    // Check if this is a /room/* path - if so, let the default proxy path handle it
    if let Some(room_path) = path.strip_prefix("/room/") {
      if !room_path.is_empty() {
        // Store the room ID as the first path segment
        let room_id = room_path.split('/').next().unwrap_or(room_path);
        // Store the room ID in the context for later use
        ctx.room_id = Some(room_id.to_string());
        info!(room_id = %room_id, "Proxying request to room");
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
        info!("File not found: {}", file_path.display());
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
    // Check for the single-use header
    let single_use = session
      .req_header()
      .headers
      .contains_key("x-single-use-isolate");

    // Room ID is now passed to the process directly via environment variable

    info!(
      host = %ctx.tenant,
      room_id = ?ctx.room_id,
      single_use = %single_use,
      "Processing request"
    );

    // Get or spawn the process
    // Get the room_id from the context, or use a default value
    let room_id = match &ctx.room_id {
      Some(id) => id.as_str(),
      None => "default-room", // Default room ID if none specified
    };

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
      .get("http://127.0.0.1:6146/room/foo")
      .header("Host", "hello.localhost")
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
  async fn test_websocket_echo() {
    use futures_util::{SinkExt, StreamExt};
    use serde_json::Value;
    use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
    init();

    // Connect to the WebSocket server with the host in the URL - now using /room/test-room path
    let url =
      url::Url::parse("ws://ws-echo.localhost:6146/room/test-room").unwrap();

    let (ws_stream, _) = connect_async(url)
      .await
      .expect("Failed to connect to WebSocket server");

    let (mut write, mut read) = ws_stream.split();

    // We should receive a welcome message from onConnect handler first
    let welcome_msg = read
      .next()
      .await
      .expect("Failed to receive welcome message")
      .unwrap();
    let welcome_data: Value =
      serde_json::from_str(&welcome_msg.to_string()).unwrap();
    assert_eq!(welcome_data["type"], "welcome");
    // The message format has changed to include a username
    // Check that it contains "Welcome to the chat room" instead of exact match
    assert!(
      welcome_data["message"]
        .as_str()
        .unwrap()
        .contains("Welcome to the chat room"),
      "Welcome message should contain 'Welcome to the chat room'"
    );

    // Read and process the userlist message
    let userlist_msg = read.next().await.unwrap().unwrap();
    let userlist_data: Value =
      serde_json::from_str(&userlist_msg.to_string()).unwrap();
    assert_eq!(userlist_data["type"], "userlist");

    // Send a test message
    let test_message = "Hello WebSocket";
    write
      .send(Message::Text(test_message.to_string()))
      .await
      .unwrap();

    // Receive the chat message response
    let chat_msg = read
      .next()
      .await
      .expect("Failed to receive chat message")
      .unwrap();
    let chat_data: Value = serde_json::from_str(&chat_msg.to_string()).unwrap();

    assert_eq!(chat_data["type"], "chat");
    assert_eq!(chat_data["message"], test_message);
    // Username should be present
    assert!(
      chat_data.get("username").is_some(),
      "Chat message should include username"
    );

    // The room ID is now handled in the process rather than in each message
    // We can check that our message was properly routed to the right process

    // Close the connection
    write.send(Message::Close(None)).await.unwrap();
    // onClose handler will be called automatically
  }

  #[tokio::test]
  async fn test_websocket_broadcast() {
    use futures_util::{SinkExt, StreamExt};
    use serde_json::Value;
    use std::time::Duration;
    use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
    init();

    // Connect with the first client
    let url1 =
      url::Url::parse("ws://ws-echo.localhost:6146/room/broadcast-test")
        .unwrap();
    let (ws_stream1, _) = connect_async(url1)
      .await
      .expect("Failed to connect first client");
    let (mut write1, mut read1) = ws_stream1.split();

    // Read the welcome message from client 1
    let welcome_msg1 = read1.next().await.unwrap().unwrap();
    let welcome_data1: Value =
      serde_json::from_str(&welcome_msg1.to_string()).unwrap();
    assert_eq!(welcome_data1["type"], "welcome");

    // Extract the username client 1 was assigned
    let username1 = welcome_data1["username"].as_str().unwrap();
    println!("Client 1 username: {}", username1);

    // Process the userlist message
    let userlist_msg = read1.next().await.unwrap().unwrap();
    let userlist_data: Value =
      serde_json::from_str(&userlist_msg.to_string()).unwrap();
    assert_eq!(userlist_data["type"], "userlist");

    // Connect with a second client
    let url2 =
      url::Url::parse("ws://ws-echo.localhost:6146/room/broadcast-test")
        .unwrap();
    let (ws_stream2, _) = connect_async(url2)
      .await
      .expect("Failed to connect second client");
    let (mut write2, mut read2) = ws_stream2.split();

    // Read the welcome message from client 2
    let welcome_msg2 = read2.next().await.unwrap().unwrap();
    let welcome_data2: Value =
      serde_json::from_str(&welcome_msg2.to_string()).unwrap();
    assert_eq!(welcome_data2["type"], "welcome");

    // Extract the username client 2 was assigned
    let username2 = welcome_data2["username"].as_str().unwrap();
    println!("Client 2 username: {}", username2);

    // Client 1 should receive a system message about client 2 joining
    let join_msg = read1.next().await.unwrap().unwrap();
    let join_data: Value = serde_json::from_str(&join_msg.to_string()).unwrap();
    assert_eq!(join_data["type"], "system");
    assert!(join_data["message"]
      .as_str()
      .unwrap()
      .contains("has joined the room"));

    // Both clients should receive an updated user list
    let userlist_msg1 = read1.next().await.unwrap().unwrap();
    let userlist_data1: Value =
      serde_json::from_str(&userlist_msg1.to_string()).unwrap();
    assert_eq!(userlist_data1["type"], "userlist");

    // Make sure client 2 receives the user list message
    let userlist_msg2 = read2.next().await.unwrap().unwrap();
    let userlist_data2: Value =
      serde_json::from_str(&userlist_msg2.to_string()).unwrap();
    assert_eq!(userlist_data2["type"], "userlist");

    // Verify that both clients appear in the user list
    let users = userlist_data2["users"].as_array().unwrap();
    assert_eq!(users.len(), 2, "Should have 2 users in the room");

    // Send a chat message from client 1
    let chat_message = "Hello everyone!";
    write1
      .send(Message::Text(chat_message.to_string()))
      .await
      .unwrap();

    // Client 1 should get the message back (broadcast includes sender)
    let client1_received = read1.next().await.unwrap().unwrap();
    let client1_msg_data: Value =
      serde_json::from_str(&client1_received.to_string()).unwrap();
    assert_eq!(client1_msg_data["type"], "chat");
    assert_eq!(client1_msg_data["message"], chat_message);
    assert_eq!(client1_msg_data["username"], username1);

    // Client 2 should also receive the broadcasted message
    let client2_received = read2.next().await.unwrap().unwrap();
    let client2_msg_data: Value =
      serde_json::from_str(&client2_received.to_string()).unwrap();
    assert_eq!(client2_msg_data["type"], "chat");
    assert_eq!(client2_msg_data["message"], chat_message);
    assert_eq!(client2_msg_data["username"], username1);

    // Test a nickname change
    let new_nickname = "SuperUser";
    let nickname_cmd = format!(
      "{{\"type\":\"nickname\",\"username\":\"{}\"}}",
      new_nickname
    );
    write1.send(Message::Text(nickname_cmd)).await.unwrap();

    // Both clients should receive a system message about the nickname change
    let name_change_msg1 = read1.next().await.unwrap().unwrap();
    let name_change_data1: Value =
      serde_json::from_str(&name_change_msg1.to_string()).unwrap();
    assert_eq!(name_change_data1["type"], "system");
    assert!(name_change_data1["message"]
      .as_str()
      .unwrap()
      .contains("is now known as"));

    let name_change_msg2 = read2.next().await.unwrap().unwrap();
    let name_change_data2: Value =
      serde_json::from_str(&name_change_msg2.to_string()).unwrap();
    assert_eq!(name_change_data2["type"], "system");
    assert!(name_change_data2["message"]
      .as_str()
      .unwrap()
      .contains("is now known as"));

    // Both clients should receive an updated user list
    let updated_list_msg1 = read1.next().await.unwrap().unwrap();
    let updated_list_data1: Value =
      serde_json::from_str(&updated_list_msg1.to_string()).unwrap();
    assert_eq!(updated_list_data1["type"], "userlist");

    let updated_list_msg2 = read2.next().await.unwrap().unwrap();
    let updated_list_data2: Value =
      serde_json::from_str(&updated_list_msg2.to_string()).unwrap();
    assert_eq!(updated_list_data2["type"], "userlist");

    // Verify the nickname was changed in the user list
    let updated_users = updated_list_data2["users"].as_array().unwrap();
    let username_found = updated_users
      .iter()
      .any(|user| user["username"].as_str().unwrap() == new_nickname);
    assert!(username_found, "User list should contain the new nickname");

    // Close both connections
    write1.send(Message::Close(None)).await.unwrap();
    write2.send(Message::Close(None)).await.unwrap();

    // Give the server time to process the closures
    tokio::time::sleep(Duration::from_millis(200)).await;
  }

  #[tokio::test]
  async fn test_separate_isolates_per_room() {
    use futures_util::{SinkExt, StreamExt};
    use serde_json::Value;
    use std::time::Duration;
    use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
    init();

    // Connect to room-1
    let url_room1 =
      url::Url::parse("ws://ws-echo.localhost:6146/room/room-1").unwrap();
    let (ws_stream1, _) = connect_async(url_room1)
      .await
      .expect("Failed to connect to room-1");
    let (mut write1, mut read1) = ws_stream1.split();

    // Read welcome message for room-1
    let welcome_msg1 = read1.next().await.unwrap().unwrap();
    let welcome_data1: Value =
      serde_json::from_str(&welcome_msg1.to_string()).unwrap();
    assert_eq!(welcome_data1["type"], "welcome");
    let username1 = welcome_data1["username"].as_str().unwrap();

    // Process the userlist message for room-1
    let userlist_msg1 = read1.next().await.unwrap().unwrap();
    let userlist_data1: Value =
      serde_json::from_str(&userlist_msg1.to_string()).unwrap();
    assert_eq!(userlist_data1["type"], "userlist");

    // Connect to room-2
    let url_room2 =
      url::Url::parse("ws://ws-echo.localhost:6146/room/room-2").unwrap();
    let (ws_stream2, _) = connect_async(url_room2)
      .await
      .expect("Failed to connect to room-2");
    let (mut write2, mut read2) = ws_stream2.split();

    // Read welcome message for room-2
    let welcome_msg2 = read2.next().await.unwrap().unwrap();
    let welcome_data2: Value =
      serde_json::from_str(&welcome_msg2.to_string()).unwrap();
    assert_eq!(welcome_data2["type"], "welcome");
    let username2 = welcome_data2["username"].as_str().unwrap();

    // Process the userlist message for room-2
    let userlist_msg2 = read2.next().await.unwrap().unwrap();
    let userlist_data2: Value =
      serde_json::from_str(&userlist_msg2.to_string()).unwrap();
    assert_eq!(userlist_data2["type"], "userlist");

    // Now we have two connections to different rooms
    // Send a message in room-1
    let message_room1 = "This message should only be in room-1";
    write1
      .send(Message::Text(message_room1.to_string()))
      .await
      .unwrap();

    // Client in room-1 should receive the message
    let msg_received1 = read1.next().await.unwrap().unwrap();
    let msg_data1: Value =
      serde_json::from_str(&msg_received1.to_string()).unwrap();
    assert_eq!(msg_data1["type"], "chat");
    assert_eq!(msg_data1["message"], message_room1);
    assert_eq!(msg_data1["username"], username1);

    // Now send a message in room-2
    let message_room2 = "This message should only be in room-2";
    write2
      .send(Message::Text(message_room2.to_string()))
      .await
      .unwrap();

    // Client in room-2 should receive the message
    let msg_received2 = read2.next().await.unwrap().unwrap();
    let msg_data2: Value =
      serde_json::from_str(&msg_received2.to_string()).unwrap();
    assert_eq!(msg_data2["type"], "chat");
    assert_eq!(msg_data2["message"], message_room2);
    assert_eq!(msg_data2["username"], username2);

    // Set a timeout for reading from room-1 to confirm isolation
    let timeout_duration = Duration::from_millis(500);
    tokio::select! {
      maybe_msg = tokio::time::timeout(timeout_duration, read1.next()) => {
        if let Ok(Some(Ok(_))) = maybe_msg {
          panic!("Room 1 received a message when it shouldn't have - rooms are not properly isolated!");
        }
      }
      _ = tokio::time::sleep(timeout_duration) => {
        // This is the expected case - we should time out without receiving anything
      }
    }

    // Clean up
    write1.send(Message::Close(None)).await.unwrap();
    write2.send(Message::Close(None)).await.unwrap();

    // Give the server time to process the closures
    tokio::time::sleep(Duration::from_millis(200)).await;
  }
}
