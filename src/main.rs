mod child_on_parent_exit;
mod cluster_membership;
mod config;
mod deno_benchmark;
mod distributed_lock;
mod heartbeat_service;
mod node_state;
mod peer_manager;
mod process_manager;
mod process_reaper;
mod router;
mod sqlite_replica;
#[cfg(test)]
pub mod test_utils;

use pingora::prelude::*;
use pingora::server::configuration::ServerConf;
use pingora::services::background::background_service;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use node_state::NodeState;
use process_reaper::ProcessReaper;
use router::{InternalAPI, Proxy};

// Default values, can be overridden when creating ProcessManager
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_REAPER_INTERVAL: Duration = Duration::from_secs(10);

/// Starts the server with the given configuration
/// Returns the server instance
fn start_server(config: config::Config) -> Server {
  // Create a server configuration
  let mut pingora_config = ServerConf::new().unwrap();
  //pingora_config.graceful_shutdown_timeout_seconds = Some(1);
  pingora_config.grace_period_seconds =
    std::env::var("ROOMD_GRACE_PERIOD_SECONDS")
      .ok()
      .and_then(|s| s.parse().ok());

  // not sure why we need this...
  let pingora_config2 = Arc::new(ServerConf::new().unwrap());

  let mut server = Server::new_with_opt_and_conf(None, pingora_config);

  // Create NodeState from configuration
  let node_state = match NodeState::new(config) {
    Ok(state) => state,
    Err(e) => {
      error!("Failed to initialize node state: {}", e);
      std::process::exit(1);
    }
  };

  // Create the proxy app that will handle routing
  let app = Proxy {
    node_state: node_state.clone(),
  };

  // Create an HTTP proxy service with our app
  let mut proxy_service = http_proxy_service(&pingora_config2, app);

  // Configure the proxy service to listen on the specified address
  proxy_service.add_tcp(&node_state.config.listen_addr);

  // Create the internal API handler
  let internal_api = InternalAPI {
    node_state: node_state.clone(),
  };

  // Create an HTTP service for the internal API
  let mut internal_service = http_proxy_service(&pingora_config2, internal_api);

  // Configure the internal service to listen on the internal address
  internal_service.add_tcp(&node_state.config.internal_listen_addr);

  server.add_service(background_service(
    "process_reaper",
    ProcessReaper::new(
      node_state.clone(),
      DEFAULT_IDLE_TIMEOUT,
      DEFAULT_REAPER_INTERVAL,
    ),
  ));

  // Add a background service for S3 heartbeat and peer discovery if cluster membership is enabled
  if let Some(cm) = &node_state.cluster_membership {
    let cm_clone = cm.clone();
    let peer_manager_clone = node_state.peer_manager.clone();

    server.add_service(background_service(
      "s3_heartbeat",
      heartbeat_service::HeartbeatService {
        cluster_membership: cm_clone,
        peer_manager: peer_manager_clone,
        interval: node_state.config.heartbeat_interval,
      },
    ));
  }

  // Add the public proxy service to the server
  server.add_service(proxy_service);

  // Add the internal service to the server
  server.add_service(internal_service);

  debug!(
    "Starting Deno Deploy proxy server on {} (public) and {} (internal)",
    node_state.config.listen_addr, node_state.config.internal_listen_addr
  );
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

  // Parse configuration from environment variables
  let config = match config::Config::from_env() {
    Ok(config) => config,
    Err(err) => {
      error!("{}", err);
      std::process::exit(1);
    }
  };

  info!("Starting server with dynamic cluster membership");

  // Start the server with configuration
  let server = start_server(config);
  server.run_forever();
}

#[cfg(test)]
mod tests {
  use super::*;
  use futures_util::{SinkExt, StreamExt};
  use once_cell::sync::Lazy;
  use serde_json::Value;
  use std::time::Duration;
  use tokio_tungstenite::tungstenite::protocol::Message;

  // inspired by https://github.com/cloudflare/pingora/blob/caa6a0/pingora-core/tests/utils/mod.rs
  pub static TEST_SERVER: Lazy<std::thread::JoinHandle<()>> = Lazy::new(|| {
    let _ = tracing_subscriber::fmt::try_init();

    // Ensure we're using a clean environment for the test server
    std::env::set_var("ADVERTISE_ADDR", "127.0.0.1:6146"); // Set advertise address
    std::env::set_var("LISTEN_ADDR", "127.0.0.1:6146"); // Set listen address
    std::env::set_var("INTERNAL_LISTEN_ADDR", "127.0.0.1:6147"); // Set internal address
    std::env::set_var("DATA", "./data"); // Set data directory
    std::env::set_var("ROOMD_HEARTBEAT_INTERVAL", "2"); // Fast heartbeat for tests
    std::env::set_var("ROOMD_GRACE_PERIOD_SECONDS", "0");

    let h = std::thread::spawn(|| {
      // Create config from environment variables
      let config = config::Config::from_env().expect("Failed to parse config");
      let server = start_server(config);
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
      .unwrap_or_else(|_| panic!("Failed to connect to room {}", room_id));

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

  #[tokio::test]
  async fn env_test() {
    init();
    let response = reqwest::Client::new()
      .get("http://127.0.0.1:6146/room/test-room")
      .header("Host", "env-test.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(response.status(), 200);
    let env_vars: Value = response.json().await.unwrap();
    let env_obj = env_vars.as_object().unwrap();
    assert_eq!(env_obj["TEST_ENV_VAR"], "test_value");
    assert_eq!(env_obj["ANOTHER_TEST_VAR"], "another_value");
    assert_eq!(env_obj["X-Room-Id"], "test-room");
    assert_eq!(env_obj.len(), 3, "Expected exactly 4 environment variables");
  }
}
