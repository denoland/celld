#[path = "../src/test_utils.rs"]
mod test_utils;

use futures_util::{SinkExt, StreamExt};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use serde_json::Value;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use test_utils::MinioTestServer;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;
use uuid::Uuid;

/// Tests that we can connect to a room through any node in the mesh
#[tokio::test]
async fn test_mesh_room_connection() {
  // Start 3 server instances with different ports
  let ports = [6001, 6002, 6003];
  let _test_env = TestEnv::new(&ports);

  // Servers are already initialized with the TCP health checks

  // Create a room ID that should be consistently owned by one node
  let room_id = "test-mesh-room";

  // Connect to the room through each server
  let mut connections = Vec::new();
  let mut usernames = Vec::new();

  for &port in &ports {
    let (conn, username) = connect_to_room(port, room_id).await;
    connections.push(conn);
    usernames.push(username);
  }

  // Sleep to allow messages to propagate
  sleep(Duration::from_millis(500)).await;

  // Send a message from the first connection
  let test_message = "Hello from the mesh test!";
  connections[0]
    .send(Message::Text(test_message.to_string().into()))
    .await
    .unwrap();

  // All connections should receive the message
  for (i, conn) in connections.iter_mut().enumerate() {
    // Read a chat message, ignoring any system/userlist messages
    let data = read_message_of_type(conn, "chat", 5000).await;

    // Verify it's the message we sent
    assert_eq!(data["message"].as_str().unwrap(), test_message);

    // If this is not the sender, check the username matches the first connection
    if i > 0 {
      assert_eq!(data["username"].as_str().unwrap(), usernames[0]);
    }
  }

  // Clean up connections
  for mut conn in connections {
    conn.close(None).await.unwrap();
  }
}

/// Tests that messages broadcast correctly across the mesh
#[tokio::test]
async fn test_mesh_message_broadcast() {
  // Start 3 server instances with different ports
  let ports = [6011, 6012, 6013];
  let _test_env = TestEnv::new(&ports);

  // Servers are already initialized with the TCP health checks

  // Create a room ID
  let room_id = "broadcast-mesh-test";

  // Connect two clients to the room through different servers
  let (mut client1, username1) = connect_to_room(ports[0], room_id).await;

  // The second client connection will now wait until it gets proper welcome messages
  let (mut client2, _) = connect_to_room(ports[1], room_id).await;

  // Client 1 should receive system message about client 2 joining
  let system_data = read_message_of_type(&mut client1, "system", 5000).await;
  assert!(system_data["message"]
    .as_str()
    .unwrap()
    .contains("has joined the room"));

  // Client 1 should receive an updated userlist
  let userlist_data =
    read_message_of_type(&mut client1, "userlist", 5000).await;
  let users = userlist_data["users"].as_array().unwrap();
  assert_eq!(users.len(), 2, "Should have 2 users in the room");

  // Send a chat message from client 1
  let chat_message = "Hello from the mesh test!";
  client1
    .send(Message::Text(chat_message.to_string().into()))
    .await
    .unwrap();

  // Both clients should receive the message
  let msg_data1 = read_message_of_type(&mut client1, "chat", 5000).await;
  assert_eq!(msg_data1["message"].as_str().unwrap(), chat_message);

  let msg_data2 = read_message_of_type(&mut client2, "chat", 5000).await;
  assert_eq!(msg_data2["message"].as_str().unwrap(), chat_message);
  assert_eq!(msg_data2["username"].as_str().unwrap(), username1);

  // Clean up connections
  client1.close(None).await.unwrap();
  client2.close(None).await.unwrap();
}

/// Tests dynamic node membership in the mesh
#[tokio::test]
async fn test_mesh_dynamic_membership() {
  // Start 3 server instances with different ports
  let ports = [6041, 6042, 6043];
  let mut test_env = TestEnv::new(&ports);

  // 1. Query the first node's /_mesh/peers endpoint to check if all nodes are visible
  let peers_url = format!("http://localhost:{0}/_mesh/peers", ports[0]);
  let peers_response = reqwest::get(&peers_url).await.unwrap();
  let peers_text = peers_response.text().await.unwrap();
  let peers_value: serde_json::Value =
    serde_json::from_str(&peers_text).unwrap();
  println!("Full peers response: {:?}", peers_value);
  let peers = peers_value["peers"].as_array().unwrap();
  assert_eq!(peers.len(), 2);

  // Collect node IDs for later comparison
  let original_node_ids: Vec<String> = peers
    .iter()
    .map(|peer| peer["node_id"].as_str().unwrap().to_string())
    .collect();

  // 2. Stop one node gracefully using SIGTERM
  println!("Gracefully stopping the second node...");
  test_env.kill_roomd_instance(1, Signal::SIGTERM);

  // Wait for heartbeat interval (shorter for tests)
  println!("Waiting for heartbeat interval to expire...");
  tokio::time::sleep(Duration::from_secs(3)).await;

  // Check peers again - should have one fewer node
  let updated_peers_response = reqwest::get(&peers_url).await.unwrap();
  let updated_peers_text = updated_peers_response.text().await.unwrap();
  println!("Updated peers response: {}", updated_peers_text);
  let updated_peers_value: serde_json::Value =
    serde_json::from_str(&updated_peers_text).unwrap();
  //println!("Updated peers full response: {:?}", updated_peers_value);
  let updated_peers = updated_peers_value["peers"].as_array().unwrap();
  //println!("Found {} peers after SIGTERM", updated_peers.len());
  assert_eq!(updated_peers.len(), 1);

  // Start a new node
  println!("Starting a new node...");
  let new_port = 6044;
  test_env.spawn_roomd_instance(new_port);
  TestEnv::wait_for_server_ready(new_port);

  // Wait for peer exchange
  println!("Waiting for peer exchange...");
  tokio::time::sleep(Duration::from_secs(3)).await;

  // Check peers again - should have more nodes now with the new node
  let recovery_peers_response = reqwest::get(&peers_url).await.unwrap();
  let recovery_peers_text = recovery_peers_response.text().await.unwrap();
  println!("Recovery peers response: {}", recovery_peers_text);
  let recovery_peers_value: serde_json::Value =
    serde_json::from_str(&recovery_peers_text).unwrap();
  let recovery_peers = recovery_peers_value["peers"].as_array().unwrap();
  assert_eq!(recovery_peers.len(), 2); // Back to having two again.

  // Verify at least one node in the final set has a node_id not in the original set
  let new_node_ids: Vec<String> = recovery_peers
    .iter()
    .map(|peer| peer["node_id"].as_str().unwrap().to_string())
    .collect();

  println!("Original node IDs: {:?}", original_node_ids);
  println!("Final node IDs: {:?}", new_node_ids);

  let has_new_node = new_node_ids
    .iter()
    .any(|id| !original_node_ids.contains(id));
  assert!(
    has_new_node,
    "Mesh should contain a newly added node with a new ID"
  );
}

/// Tests that room isolation works properly in the mesh
#[tokio::test]
async fn test_mesh_room_isolation() {
  // Start 3 server instances with different ports
  let ports = [6021, 6022, 6023];
  let _test_env = TestEnv::new(&ports);

  // Servers are already initialized with the TCP health checks

  // Connect to two different rooms through different servers
  let (mut client1, username1) = connect_to_room(ports[0], "room-a").await;
  let (mut client2, username2) = connect_to_room(ports[1], "room-b").await;

  // Send a message in room-a
  let message_room1 = "This message should only be in room-a";
  client1
    .send(Message::Text(message_room1.to_string().into()))
    .await
    .unwrap();

  // Client in room-a should receive the message
  let msg_data1 = read_message_of_type(&mut client1, "chat", 5000).await;
  assert_eq!(msg_data1["message"].as_str().unwrap(), message_room1);
  assert_eq!(msg_data1["username"].as_str().unwrap(), username1);

  // Send a message in room-b
  let message_room2 = "This message should only be in room-b";
  client2
    .send(Message::Text(message_room2.to_string().into()))
    .await
    .unwrap();

  // Client in room-b should receive the message
  let msg_data2 = read_message_of_type(&mut client2, "chat", 5000).await;
  assert_eq!(msg_data2["message"].as_str().unwrap(), message_room2);
  assert_eq!(msg_data2["username"].as_str().unwrap(), username2);

  // Verify isolation: room-a should not receive messages sent to room-b
  let timeout_duration = Duration::from_millis(300);
  tokio::select! {
      maybe_msg = tokio::time::timeout(timeout_duration, client1.next()) => {
          if let Ok(Some(Ok(_))) = maybe_msg {
              panic!("Room isolation failure: room-a received a message from room-b");
          }
      }
      _ = tokio::time::sleep(timeout_duration) => {
          // Expected case: timeout without receiving cross-room message
      }
  }

  // Clean up connections
  client1.close(None).await.unwrap();
  client2.close(None).await.unwrap();
}

struct TestEnv {
  servers: Vec<Child>,
  ports: Vec<u16>,
  minio_server: MinioTestServer,
  test_id: String,
  bucket_name: String,
}

impl TestEnv {
  // Start mesh nodes with the provided ports, using a real MinIO server
  fn new(ports: &[u16]) -> Self {
    // Start MinIO server for testing with a unique port based on the first mesh port
    let minio_port = 9000 + (ports[0] % 1000);
    let bucket_name = "test-mesh-bucket".to_string();
    let minio_server = MinioTestServer::start(minio_port);
    minio_server.create_bucket(&bucket_name).unwrap();

    println!("Started MinIO server on port {}", minio_port);

    let servers = Vec::new();
    let test_id = Uuid::new_v4().simple().to_string();
    let mut test_env = TestEnv {
      servers,
      ports: ports.to_vec(),
      minio_server,
      bucket_name,
      test_id: test_id.to_string(),
    };

    for (_i, &port) in ports.iter().enumerate() {
      test_env.spawn_roomd_instance(port);
    }

    // Wait for servers to be ready by probing TCP connections
    println!("Waiting for servers to initialize...");
    for &port in ports {
      Self::wait_for_server_ready(port);
    }

    // Longer delay for peer exchange after TCP connections are ready
    // This is important to give time for S3 registration and peer discovery
    std::thread::sleep(Duration::from_secs(2));
    println!("All servers are ready now");
    test_env
  }

  fn kill_roomd_instance(&mut self, index: usize, signal: Signal) {
    let server = self.servers.remove(index);
    let _ = self.ports.remove(index);
    let pid = Pid::from_raw(server.id() as i32);
    kill(pid, signal).unwrap();
  }

  fn spawn_roomd_instance(&mut self, port: u16) {
    let advertise_addr = format!("127.0.0.1:{}", port);
    let server = Command::new(env!("CARGO_BIN_EXE_roomd"))
      .env("ADVERTISE_ADDR", &advertise_addr)
      .env("DATA", "./data")
      .env("ROOMD_HEARTBEAT_INTERVAL", "2")
      .env(
        "ROOMD_S3_ENDPOINT",
        format!("http://localhost:{}", self.minio_server.port),
      )
      .env("ROOMD_S3_BUCKET", &self.bucket_name)
      .env("ROOMD_S3_REGION", "us-east-1")
      .env("ROOMD_S3_PREFIX", format!("roomd-test-{}", self.test_id))
      .env("ROOMD_S3_ACCESS_KEY_ID", &self.minio_server.access_key_id)
      .env(
        "ROOMD_S3_SECRET_ACCESS_KEY",
        &self.minio_server.secret_access_key,
      )
      //.env("RUST_LOG", "debug")
      .stdout(Stdio::inherit())
      .stderr(Stdio::inherit())
      .spawn()
      .expect(&format!("Failed to start server on port {}", port));

    self.servers.push(server);
    self.ports.push(port);
    println!(
      "Started server on port {} with ADVERTISE_ADDR={} and S3 mesh",
      port, advertise_addr
    );
  }

  // Wait for a server to be ready by probing its TCP port
  fn wait_for_server_ready(port: u16) {
    const MAX_ATTEMPTS: usize = 10;
    const RETRY_DELAY_MS: u64 = 200;
    for attempt in 1..=MAX_ATTEMPTS {
      match std::net::TcpStream::connect(format!("127.0.0.1:{}", port)) {
        Ok(_) => {
          return;
        }
        Err(_) => {
          println!(
            "Waiting for server on port {} (attempt {}/{})",
            port, attempt, MAX_ATTEMPTS
          );
          std::thread::sleep(Duration::from_millis(RETRY_DELAY_MS));
        }
      }
    }
    panic!("Server on port {} failed to start", port);
  }
}

impl Drop for TestEnv {
  fn drop(&mut self) {
    for _i in 0..self.servers.len() {
      self.kill_roomd_instance(0, Signal::SIGKILL);
    }
  }
}

/// Helper function to read a message of a specific type from a WebSocket stream
async fn read_message_of_type(
  stream: &mut tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
  >,
  msg_type: &str,
  timeout_ms: u64,
) -> Value {
  let timeout_duration = Duration::from_millis(timeout_ms);
  let deadline = std::time::Instant::now() + timeout_duration;

  while std::time::Instant::now() < deadline {
    let maybe_msg = tokio::time::timeout(
      std::cmp::min(timeout_duration, deadline - std::time::Instant::now()),
      stream.next(),
    )
    .await;

    match maybe_msg {
      Ok(Some(Ok(msg))) => {
        let data: Value = serde_json::from_str(&msg.to_string()).unwrap();
        if let Some(t) = data["type"].as_str() {
          if t == msg_type {
            return data;
          }
          println!("Ignoring message of type: {}", t);
        }
      }
      _ => break,
    }
  }

  panic!("Timeout waiting for message of type: {}", msg_type);
}

/// Helper function to connect to a WebSocket room and handle initial messages
async fn connect_to_room(
  port: u16,
  room_id: &str,
) -> (
  tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
  >,
  String, // username
) {
  // Use the hostname directly with the test port
  let url =
    Url::parse(&format!("ws://ws-echo.localhost:{}/room/{}", port, room_id))
      .unwrap();

  println!("Connecting to WebSocket at {}", url);

  let (mut ws_stream, _) = tokio_tungstenite::connect_async(url.to_string())
    .await
    .expect(&format!(
      "Failed to connect to room {} on port {}",
      room_id, port
    ));

  // Read welcome message
  println!("Connected, waiting for welcome message");
  let welcome_data =
    read_message_of_type(&mut ws_stream, "welcome", 5000).await;
  let username = welcome_data["username"].as_str().unwrap().to_string();

  // Read userlist message
  let _userlist_data =
    read_message_of_type(&mut ws_stream, "userlist", 5000).await;

  println!("Connected to room {} as {}", room_id, username);
  (ws_stream, username)
}
