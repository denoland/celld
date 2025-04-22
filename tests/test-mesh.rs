#[path = "../src/test_utils.rs"]
mod test_utils;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
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
  let _test_env = MeshTest::new(&ports);

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
  let _test_env = MeshTest::new(&ports);

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

/// Tests SQLite replication to S3/MinIO in the mesh
#[tokio::test]
#[ignore]
// TODO PeerManager doesn't have any mechanism to detect failed peers and update
// the routing. When we kill the first server, the second server still thinks
// that the first server is responsible for the room based on the hash ring, so
// it tries to forward the request to the first server, which is now down.
async fn test_sqlite_replication() {
  // Start MinIO server for testing
  let minio_port = 9009;
  let bucket_name = "roomd-sqlreplica-test";
  let minio_server = MinioTestServer::start(minio_port);
  minio_server.create_bucket(bucket_name).unwrap();

  // Generate a unique room ID
  let room_id = format!("repl-test-{}", Uuid::new_v4().simple().to_string());

  // Start a mesh with S3 replication enabled
  let ports = [6031, 6032];
  let mut servers = Vec::new();
  let test_prefix = format!("test-{}", Uuid::new_v4().simple().to_string());

  println!("Starting servers with S3 replication enabled");
  for (i, &port) in ports.iter().enumerate() {
    // Create a comma-separated string of peers
    let peers: Vec<String> = ports
      .iter()
      .enumerate()
      .filter(|(j, _)| *j != i) // Skip self
      .map(|(_, p)| format!("127.0.0.1:{}", p))
      .collect();

    let known_peers = peers.join(",");
    let self_addr = format!("127.0.0.1:{}", port);

    // Start server with S3 replication environment variables
    let server = Command::new(env!("CARGO_BIN_EXE_roomd"))
      .env("DATA_PORT", port.to_string())
      .env("SELF_ADDR", self_addr.clone())
      .env("KNOWN_PEERS", known_peers.clone())
      .env("DATA", "./data") // Point to the data directory
      .env("RUST_LOG", "debug") // More detailed logging
      // S3/MinIO configuration for replication
      .env(
        "ROOMD_S3_ENDPOINT",
        format!("http://localhost:{}", minio_port),
      )
      .env("ROOMD_S3_BUCKET", bucket_name)
      .env("ROOMD_S3_REGION", "us-east-1")
      .env("ROOMD_S3_PREFIX", &test_prefix)
      .env("ROOMD_S3_ACCESS_KEY_ID", &minio_server.access_key)
      .env("ROOMD_S3_SECRET_ACCESS_KEY", &minio_server.secret_key)
      .stdout(Stdio::inherit()) // Show output for debugging
      .stderr(Stdio::inherit())
      .spawn()
      .expect(&format!("Failed to start server on port {}", port));

    servers.push(server);
    println!(
      "Started server on port {} with S3 replication enabled",
      port
    );
  }

  // Wait for servers to be ready
  println!("Waiting for servers to initialize...");
  for &port in &ports {
    MeshTest::wait_for_server_ready(port);
  }
  std::thread::sleep(Duration::from_millis(500));

  // Connect to the room on the first server and make a database write
  let tenant = "basic-db.localhost";
  let url = format!("http://{}:{}/room/{}", tenant, ports[0], room_id);
  println!("Making first request to create DB: {}", url);

  // First request to initialize the database and increment counter to 1
  let first_response = reqwest::Client::new()
    .get(&url)
    .header("Host", tenant)
    .send()
    .await
    .expect("Failed to send first request")
    .text()
    .await
    .expect("Failed to get first response text");

  assert_eq!(
    first_response.trim(),
    "1",
    "First request should return count = 1"
  );

  // Allow time for replication to occur
  println!("Waiting for replication to S3...");
  tokio::time::sleep(Duration::from_secs(5)).await;

  // Check that files exist in MinIO
  println!("Checking if files are in MinIO...");
  let output = std::process::Command::new("docker")
    .args([
      "run",
      "--network=host",
      "--rm",
      "-e",
      &format!(
        "MC_HOST_minio=http://{}:{}@localhost:{}",
        minio_server.access_key, minio_server.secret_key, minio_port
      ),
      "minio/mc",
      "ls",
      "--recursive",
      &format!("minio/{}", bucket_name),
    ])
    .output()
    .expect("Failed to run MinIO ls command");

  let minio_listing = String::from_utf8_lossy(&output.stdout);
  println!("MinIO bucket contents:\n{}", minio_listing);

  // Verify that the room ID appears in the MinIO listing
  assert!(
    minio_listing.contains(&room_id),
    "Room database should be replicated to MinIO"
  );

  // Kill server 1 and only use server 2 from now on
  println!("Killing first server to simulate failure...");
  servers[0].kill().expect("Failed to kill first server");

  // Wait for peer manager to detect the failure and update routing
  println!("Waiting for peer manager to update...");
  tokio::time::sleep(Duration::from_secs(2)).await;

  // Make a request to the second server (should restore DB from S3)
  let second_url = format!("http://{}:{}/room/{}", tenant, ports[1], room_id);
  println!("Making request to second server: {}", second_url);

  let second_response = reqwest::Client::new()
    .get(&second_url)
    .header("Host", tenant)
    .send()
    .await
    .expect("Failed to send second request")
    .text()
    .await
    .expect("Failed to get second response text");

  // The counter should be incremented to 2 since the previous state was restored
  assert_eq!(
    second_response.trim(),
    "2",
    "Second request should restore DB and return count = 2"
  );

  // Clean up the servers
  for mut server in servers {
    if let Err(e) = server.kill() {
      eprintln!("Failed to kill server: {}", e);
    }
  }
}

/// Tests that room isolation works properly in the mesh
#[tokio::test]
async fn test_mesh_room_isolation() {
  // Start 3 server instances with different ports
  let ports = [6021, 6022, 6023];
  let _test_env = MeshTest::new(&ports);

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

struct MeshTest {
  servers: Vec<Child>,
  ports: Vec<u16>,
}

impl MeshTest {
  // Wait for a server to be ready by probing its TCP port
  fn wait_for_server_ready(port: u16) {
    const MAX_ATTEMPTS: usize = 5;
    const RETRY_DELAY_MS: u64 = 100;
    for _attempt in 1..=MAX_ATTEMPTS {
      match std::net::TcpStream::connect(format!("127.0.0.1:{}", port)) {
        Ok(_) => {
          return;
        }
        Err(_) => {
          std::thread::sleep(Duration::from_millis(RETRY_DELAY_MS));
        }
      }
    }
    panic!("Server on port {} failed to start", port);
  }

  // Start mesh nodes with the provided ports
  fn new(ports: &[u16]) -> Self {
    let mut servers = Vec::new();

    for (i, &port) in ports.iter().enumerate() {
      // Create a comma-separated string of peers for KNOWN_PEERS env var
      let peers: Vec<String> = ports
        .iter()
        .enumerate()
        .filter(|(j, _)| *j != i) // Skip self
        .map(|(_, p)| format!("127.0.0.1:{}", p))
        .collect();

      let known_peers = peers.join(",");
      let self_addr = format!("127.0.0.1:{}", port);

      // Start server with appropriate environment variables
      // The key environment variables:
      // - DATA_PORT: The port the server listens on
      // - SELF_ADDR: The address the server identifies itself as in the mesh
      // - KNOWN_PEERS: Other peers in the mesh
      // - DATA: Path to the data directory with application code
      let server = Command::new(env!("CARGO_BIN_EXE_roomd"))
        .env("DATA_PORT", port.to_string())
        .env("SELF_ADDR", self_addr.clone())
        .env("KNOWN_PEERS", known_peers.clone())
        .env("DATA", "./data") // Point to the data directory
        .env("RUST_LOG", "debug") // More detailed logging
        .stdout(Stdio::inherit()) // Show output for debugging
        .stderr(Stdio::inherit())
        .spawn()
        .expect(&format!("Failed to start server on port {}", port));

      servers.push(server);
      println!(
        "Started server on port {} with SELF_ADDR={} and KNOWN_PEERS={}",
        port, self_addr, known_peers
      );
    }

    // Wait for servers to be ready by probing TCP connections
    println!("Waiting for servers to initialize...");
    for &port in ports {
      Self::wait_for_server_ready(port);
    }

    // Brief delay for peer exchange after TCP connections are ready
    std::thread::sleep(Duration::from_millis(500));
    println!("All servers are ready now");

    MeshTest {
      servers,
      ports: ports.to_vec(),
    }
  }

  // Start mesh nodes with S3/MinIO replication enabled
  fn new_with_s3(ports: &[u16], minio_port: u16, bucket: &str) -> Self {
    let mut servers = Vec::new();
    let test_id = Uuid::new_v4().simple().to_string();

    for (i, &port) in ports.iter().enumerate() {
      // Create a comma-separated string of peers for KNOWN_PEERS env var
      let peers: Vec<String> = ports
        .iter()
        .enumerate()
        .filter(|(j, _)| *j != i) // Skip self
        .map(|(_, p)| format!("127.0.0.1:{}", p))
        .collect();

      let known_peers = peers.join(",");
      let self_addr = format!("127.0.0.1:{}", port);

      // Start server with S3 replication environment variables
      let server = Command::new(env!("CARGO_BIN_EXE_roomd"))
        .env("DATA_PORT", port.to_string())
        .env("SELF_ADDR", self_addr.clone())
        .env("KNOWN_PEERS", known_peers.clone())
        .env("DATA", "./data") // Point to the data directory
        .env("RUST_LOG", "debug") // More detailed logging
        // S3/MinIO configuration
        .env(
          "ROOMD_S3_ENDPOINT",
          format!("http://localhost:{}", minio_port),
        )
        .env("ROOMD_S3_BUCKET", bucket)
        .env("ROOMD_S3_REGION", "us-east-1")
        .env("ROOMD_S3_PREFIX", format!("roomd-test-{}", test_id))
        .env("ROOMD_S3_ACCESS_KEY_ID", "minioadmin")
        .env("ROOMD_S3_SECRET_ACCESS_KEY", "minioadmin")
        .stdout(Stdio::inherit()) // Show output for debugging
        .stderr(Stdio::inherit())
        .spawn()
        .expect(&format!("Failed to start server on port {}", port));

      servers.push(server);
      println!(
        "Started server on port {} with S3 replication enabled",
        port
      );
    }

    // Wait for servers to be ready by probing TCP connections
    println!("Waiting for servers to initialize...");
    for &port in ports {
      Self::wait_for_server_ready(port);
    }

    // Brief delay for peer exchange after TCP connections are ready
    std::thread::sleep(Duration::from_millis(500));
    println!("All servers are ready now");

    MeshTest {
      servers,
      ports: ports.to_vec(),
    }
  }
}

impl Drop for MeshTest {
  fn drop(&mut self) {
    // Cleanup: kill all server processes
    for (i, server) in self.servers.iter_mut().enumerate() {
      match server.kill() {
        Ok(_) => println!("Killed server on port {}", self.ports[i]),
        Err(e) => {
          eprintln!("Failed to kill server on port {}: {}", self.ports[i], e)
        }
      }
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
