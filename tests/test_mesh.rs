mod common;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;
use uuid::Uuid;

use common::TestEnv;

/// Tests that we can connect to a cell through any node in the mesh
#[tokio::test]
async fn test_mesh_cell_connection() {
  // Start 3 server instances with auto-allocated ports
  let test_env = TestEnv::new(3);

  // Servers are already initialized with the TCP health checks

  // Create a cell ID that should be consistently owned by one node
  let cell_id = "test-mesh-cell";

  // Connect to the cell through each server
  let mut connections = Vec::new();
  let mut usernames = Vec::new();

  for port in &test_env.ports {
    let (conn, username) = connect_to_cell(port.public(), cell_id).await;
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
  // Start 3 server instances with auto-allocated ports
  let test_env = TestEnv::new(3);

  // Servers are already initialized with the TCP health checks

  // Create a cell ID
  let cell_id = "broadcast-mesh-test";

  // Connect two clients to the cell through different servers
  let (mut client1, username1) =
    connect_to_cell(test_env.ports[0].public(), cell_id).await;

  // The second client connection will now wait until it gets proper welcome messages
  let (mut client2, _) =
    connect_to_cell(test_env.ports[1].public(), cell_id).await;

  // Client 1 should receive system message about client 2 joining
  let system_data = read_message_of_type(&mut client1, "system", 5000).await;
  assert!(system_data["message"]
    .as_str()
    .unwrap()
    .contains("has joined the cell"));

  // Client 1 should receive an updated userlist
  let userlist_data =
    read_message_of_type(&mut client1, "userlist", 5000).await;
  let users = userlist_data["users"].as_array().unwrap();
  assert_eq!(users.len(), 2, "Should have 2 users in the cell");

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
  let mut test_env = TestEnv::new(3);

  // 1. Query the first node's internal mesh/peers endpoint to check if all nodes are visible
  let peers_url = format!(
    "http://localhost:{}/_internal/mesh/peers",
    test_env.ports[0].internal()
  );
  let peers_response = reqwest::get(&peers_url).await.unwrap();
  let peers_text = peers_response.text().await.unwrap();
  let peers_value: serde_json::Value =
    serde_json::from_str(&peers_text).unwrap();
  println!("Full peers response: {:?}", peers_value);
  let peers = peers_value["peers"].as_array().unwrap();
  assert_eq!(peers.len(), 3);

  // Collect node IDs for later comparison
  let original_node_ids: Vec<String> = peers
    .iter()
    .map(|peer| peer["node_id"].as_str().unwrap().to_string())
    .collect();

  println!("killing stopping the second node...");
  test_env.kill_cell_instance(1);

  // Wait for heartbeat interval (shorter for tests)
  println!("Waiting for heartbeat interval to expire...");
  tokio::time::sleep(Duration::from_secs(8)).await;

  // Check peers again - should have one fewer node
  let updated_peers_response = reqwest::get(&peers_url).await.unwrap();
  let updated_peers_text = updated_peers_response.text().await.unwrap();
  println!("Updated peers response: {}", updated_peers_text);
  let updated_peers_value: serde_json::Value =
    serde_json::from_str(&updated_peers_text).unwrap();
  //println!("Updated peers full response: {:?}", updated_peers_value);
  let updated_peers = updated_peers_value["peers"].as_array().unwrap();
  //println!("Found {} peers after SIGTERM", updated_peers.len());
  assert_eq!(updated_peers.len(), 2);

  // Start a new node
  println!("Starting a new node...");
  let new_port = TestEnv::allocate_ports(7044, 1, 2);
  assert_eq!(new_port.len(), 1);
  test_env.spawn_cell_instance(new_port);

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
  assert_eq!(recovery_peers.len(), 3); // Back to having two again.

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

/// Tests that cell isolation works properly in the mesh
#[tokio::test]
async fn test_mesh_cell_isolation() {
  // Start 3 server instances with auto-allocated ports
  let test_env = TestEnv::new(3);

  // Servers are already initialized with the TCP health checks

  // Connect to two different cells through different servers
  let (mut client1, username1) =
    connect_to_cell(test_env.ports[0].public(), "cell-a").await;
  let (mut client2, username2) =
    connect_to_cell(test_env.ports[1].public(), "cell-b").await;

  // Send a message in cell-a
  let message_cell1 = "This message should only be in cell-a";
  client1
    .send(Message::Text(message_cell1.to_string().into()))
    .await
    .unwrap();

  // Client in cell-a should receive the message
  let msg_data1 = read_message_of_type(&mut client1, "chat", 5000).await;
  assert_eq!(msg_data1["message"].as_str().unwrap(), message_cell1);
  assert_eq!(msg_data1["username"].as_str().unwrap(), username1);

  // Send a message in cell-b
  let message_cell2 = "This message should only be in cell-b";
  client2
    .send(Message::Text(message_cell2.to_string().into()))
    .await
    .unwrap();

  // Client in cell-b should receive the message
  let msg_data2 = read_message_of_type(&mut client2, "chat", 5000).await;
  assert_eq!(msg_data2["message"].as_str().unwrap(), message_cell2);
  assert_eq!(msg_data2["username"].as_str().unwrap(), username2);

  // Verify isolation: cell-a should not receive messages sent to cell-b
  let timeout_duration = Duration::from_millis(300);
  tokio::select! {
      maybe_msg = tokio::time::timeout(timeout_duration, client1.next()) => {
          if let Ok(Some(Ok(_))) = maybe_msg {
              panic!("Cell isolation failure: cell-a received a message from cell-b");
          }
      }
      _ = tokio::time::sleep(timeout_duration) => {
          // Expected case: timeout without receiving cross-cell message
      }
  }

  // Clean up connections
  client1.close(None).await.unwrap();
  client2.close(None).await.unwrap();
}

/// Tests the node failure scenario - when the primary node for a cell fails,
/// another node automatically takes over responsibility for the cell (Durability Test 2)
#[tokio::test]
async fn test_node_failure_takeover() {
  // Setup three nodes in the mesh with auto-allocated ports
  let mut test_env = TestEnv::new(3);

  // Use unique cell ID to avoid conflicts with other tests
  let test_cell_id = format!("failover-test-{}", Uuid::new_v4().simple());
  println!("Testing failover with cell ID: {}", test_cell_id);

  // Find which node is the primary owner for this cell
  let mut primary_owner_port = 0;
  let mut secondary_owners = Vec::new();

  for port in &test_env.ports {
    let public_port = port.public();
    let internal_port = port.internal();
    let owner_url = format!(
      "http://localhost:{}/_internal/mesh/owner/basic-db.localhost/{}",
      internal_port, test_cell_id
    );
    let owner_resp = reqwest::get(&owner_url)
      .await
      .unwrap()
      .json::<serde_json::Value>()
      .await
      .unwrap();

    println!("Node on port {} owner info: {}", public_port, owner_resp);

    let is_owner = owner_resp["is_local"].as_bool().unwrap();
    if is_owner {
      primary_owner_port = public_port;
    } else {
      secondary_owners.push(public_port);
    }
  }

  assert_ne!(
    primary_owner_port, 0,
    "Failed to find primary owner for test cell"
  );
  assert!(
    !secondary_owners.is_empty(),
    "Failed to find secondary owners for test cell"
  );

  println!("Primary owner is on port: {}", primary_owner_port);
  println!("Secondary owners are on ports: {:?}", secondary_owners);

  // Find the index of the primary owner in the test_env.ports array
  let primary_index = test_env
    .ports
    .iter()
    .position(|p| p.public() == primary_owner_port)
    .unwrap();

  // Send request to the primary node to create data in the cell
  let url = format!(
    "http://basic-db.localhost:{}/cell/{}",
    primary_owner_port, test_cell_id
  );
  let client = reqwest::Client::builder().build().unwrap();

  // Make the first request to create the cell
  let response = client.get(&url).send().await.unwrap();
  assert_eq!(response.status(), 200);
  let content = response.text().await.unwrap();
  assert_eq!(content.trim(), "1", "First request should return 1");

  // Make a second request to update data
  let response2 = client.get(&url).send().await.unwrap();
  assert_eq!(response2.status(), 200);
  let content2 = response2.text().await.unwrap();
  assert_eq!(content2.trim(), "2", "Second request should return 2");

  // Wait for Litestream to replicate data to S3
  println!("Waiting for Litestream to replicate data to S3...");
  sleep(Duration::from_secs(5)).await;

  // Abruptly kill the primary node instead of gracefully shutting down
  // (simulate node failure)
  println!("Killing primary node on port {}...", primary_owner_port);
  // We already found the primary_index earlier
  test_env.kill_cell_instance(primary_index);

  // Wait for node failure to be detected (heartbeat timeout)
  // CELL_STALENESS_THRESHOLD_SECS is set to 6 seconds in TestEnv::spawn_cell_instance
  // Also CELL_LOCK_GUARD_TTL_SECS is set to 6 seconds in TestEnv::spawn_cell_instance,
  // meaning that the lock on the cell ("basic-db.localhost", test_cell_id)
  // should expire 6 seconds after the primary node is killed.
  println!("Waiting for primary node failure to be detected...");
  sleep(Duration::from_secs(8)).await;

  // Try to access the cell through a secondary node
  let secondary_port = secondary_owners[0];
  let secondary_url = format!(
    "http://basic-db.localhost:{}/cell/{}",
    secondary_port, test_cell_id
  );

  // This request should trigger takeover if not already happened
  // It might take several tries before the failover completes
  println!(
    "Sending request to secondary node on port {}...",
    secondary_port
  );
  let resp = client.get(&secondary_url).send().await.unwrap();
  let content3 = resp.text().await.unwrap();
  assert_eq!(content3.trim(), "3");

  // Make another request to confirm the cell is still operational
  let response4 = client.get(&secondary_url).send().await.unwrap();
  assert_eq!(response4.status(), 200);
  let content4 = response4.text().await.unwrap();
  assert_eq!(content4.trim(), "4");

  // Check which node owns the cell now (should be one of the secondary nodes)
  let mut new_owner_found = false;
  for &public_port in &secondary_owners {
    // Find the matching internal port for this public port
    let i = test_env
      .ports
      .iter()
      .position(|p| p.public() == public_port)
      .unwrap_or_else(|| {
        // This could happen if ports changed - find the index in the secondary_owners array instead
        secondary_owners
          .iter()
          .position(|&p| p == public_port)
          .unwrap()
      });
    let internal_port = test_env.ports[i].internal();

    let owner_url = format!(
      "http://localhost:{}/_internal/mesh/owner/basic-db.localhost/{}",
      internal_port, test_cell_id
    );
    let owner_resp = reqwest::get(&owner_url)
      .await
      .unwrap()
      .json::<serde_json::Value>()
      .await
      .unwrap();

    println!(
      "After failover, node on port {} owner info: {}",
      public_port, owner_resp
    );

    let is_owner = owner_resp["is_local"].as_bool().unwrap();
    if is_owner {
      new_owner_found = true;
      println!("New owner after failover is on port: {}", public_port);
      break;
    }
  }

  assert!(
    new_owner_found,
    "Failed to find a new owner after primary node failure"
  );
}

/// Tests concurrent takeover attempts to verify only one node succeeds via locking
#[tokio::test]
async fn test_concurrent_takeover_locking() {
  // Setup three celld nodes in the mesh with auto-allocated ports
  let mut test_env = TestEnv::new(3);

  // Use unique cell ID to avoid conflicts with other tests
  let test_cell_id = format!("takeover-lock-test-{}", Uuid::new_v4().simple());
  println!("Testing concurrent takeover with cell ID: {}", test_cell_id);

  // Find which node is the primary owner for this cell
  let mut primary_owner_port = 0;
  let mut secondary_owners = Vec::new();

  for port in &test_env.ports {
    let public_port = port.public();
    let internal_port = port.internal();
    let owner_url = format!(
      "http://localhost:{}/_internal/mesh/owner/basic-db.localhost/{}",
      internal_port, test_cell_id
    );
    let owner_resp = reqwest::get(&owner_url)
      .await
      .unwrap()
      .json::<serde_json::Value>()
      .await
      .unwrap();

    println!("Node on port {} owner info: {}", public_port, owner_resp);

    let is_owner = owner_resp["is_local"].as_bool().unwrap();
    if is_owner {
      primary_owner_port = public_port;
    } else {
      secondary_owners.push(public_port);
    }
  }

  assert_ne!(
    primary_owner_port, 0,
    "Failed to find primary owner for test cell"
  );
  assert!(
    secondary_owners.len() >= 2,
    "Need at least 2 secondary owners for this test"
  );

  println!("Primary owner is on port: {}", primary_owner_port);
  println!("Secondary owners are on ports: {:?}", secondary_owners);

  // Create initial data on the primary node
  let url = format!(
    "http://basic-db.localhost:{}/cell/{}",
    primary_owner_port, test_cell_id
  );
  let client = reqwest::Client::builder().build().unwrap();
  let response = client.get(&url).send().await.unwrap();
  assert_eq!(response.status(), 200);
  assert_eq!(response.text().await.unwrap().trim(), "1");

  // Wait for Litestream to replicate data to S3
  println!("Waiting for Litestream to replicate data to S3...");
  sleep(Duration::from_secs(5)).await;

  // Shutdown the primary node
  println!(
    "Shutting down primary node on port {}...",
    primary_owner_port
  );
  let primary_index = test_env
    .ports
    .iter()
    .position(|p| p.public() == primary_owner_port)
    .unwrap();
  test_env.graceful_shutdown_cell_instance(primary_index);

  // Sleep to ensure the primary node has fully shutdown
  sleep(Duration::from_secs(10)).await;

  // Create URLs for the secondary nodes
  let secondary_urls: Vec<String> = secondary_owners
    .iter()
    .map(|&public_port| {
      format!(
        "http://basic-db.localhost:{}/cell/{}",
        public_port, test_cell_id
      )
    })
    .collect();

  // Prepare concurrent requests to multiple nodes to trigger takeover race
  println!("Sending concurrent requests to trigger takeover race...");
  let concurrent_requests = secondary_urls
    .iter()
    .map(|url| {
      let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
      let url = url.clone();
      async move {
        let resp = client.get(&url).send().await.unwrap();
        assert_eq!(resp.status(), 200);
        resp.text().await.unwrap_or_default()
      }
    })
    .collect::<Vec<_>>();

  // Wait for all requests to complete
  let results = futures::future::join_all(concurrent_requests).await;

  // Analyze the results
  let mut success_count = 0;
  for body in results {
    success_count += 1;
    let value = body.trim();
    assert!(
      value == "2" || value == "3",
      "Successful takeover should return 2 or 3, got {}",
      value
    );
  }

  // At least one request should succeed (the one that got the lock)
  assert_eq!(success_count, 2);

  // Send another request to whichever node succeeded - they should all route to
  // the same place now
  println!(
    "Sending another request to verify cell stability after takeover..."
  );
  let stabilized_url = &secondary_urls[0];
  let final_response = client.get(stabilized_url).send().await.unwrap();
  assert_eq!(final_response.status(), 200);
  assert_eq!(final_response.text().await.unwrap().trim(), "4");

  // Check which node owns the cell now (only one should claim ownership)
  let mut owner_count = 0;
  for &public_port in &secondary_owners {
    // Find the matching internal port
    let i = test_env
      .ports
      .iter()
      .position(|p| p.public() == public_port)
      .unwrap_or_else(|| {
        // This could happen if ports changed - fall back to a reasonable default
        println!(
          "Warning: Could not find public port {} in test_env.public_ports",
          public_port
        );
        0
      });
    let internal_port = test_env.ports[i].internal();

    let owner_url = format!(
      "http://localhost:{}/_internal/mesh/owner/basic-db.localhost/{}",
      internal_port, test_cell_id
    );
    let owner_resp = reqwest::get(&owner_url)
      .await
      .unwrap()
      .json::<serde_json::Value>()
      .await
      .unwrap();

    println!(
      "After takeover, node on port {} owner info: {}",
      public_port, owner_resp
    );

    if owner_resp["is_local"].as_bool().unwrap() {
      owner_count += 1;
      println!("Owner after takeover is on port: {}", public_port);
    }
  }

  assert_eq!(owner_count, 1);
}

/// Tests proxy forwarding to verify it correctly retries down the owner list
#[tokio::test]
// Not working yet - not sure how pingera can allow us to do this.  Might need
// to move to hyper to get better control of the request forwarding.
#[ignore]
async fn test_proxy_forwarding_retry() {
  // Setup three nodes in the mesh with auto-allocated ports
  let mut test_env = TestEnv::new(3);

  // Use unique cell ID to avoid conflicts with other tests
  let test_cell_id = format!("proxy-retry-test-{}", Uuid::new_v4().simple());
  println!("Testing proxy forwarding with cell ID: {}", test_cell_id);

  // Find which node is the primary owner for this cell
  let mut owner_info = Vec::new();
  for port in &test_env.ports {
    let public_port = port.public();
    let internal_port = port.internal();
    let owner_url = format!(
      "http://localhost:{}/_internal/mesh/owner/basic-db.localhost/{}",
      internal_port, test_cell_id
    );
    let owner_resp = reqwest::get(&owner_url)
      .await
      .unwrap()
      .json::<serde_json::Value>()
      .await
      .unwrap();

    println!("Node on port {} owner info: {}", public_port, owner_resp);

    let is_owner = owner_resp["is_local"].as_bool().unwrap();
    let owner_addr = owner_resp["owner"].as_str().unwrap().to_string();

    owner_info.push((public_port, is_owner, owner_addr));
  }

  // Sort owner info to get primary and backups in order
  owner_info.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by is_owner (true first)

  let primary_owner_port = owner_info[0].0;
  println!("Primary owner is on port: {}", primary_owner_port);

  // Send a request to a non-owner node to verify it forwards to primary
  let non_owner_port = owner_info
    .iter()
    .find(|(_, is_owner, _)| !is_owner)
    .unwrap()
    .0;
  println!("Testing forwarding from non-owner port: {}", non_owner_port);

  // Create initial data by sending request to a non-owner node (should forward to primary)
  let url = format!(
    "http://basic-db.localhost:{}/cell/{}",
    non_owner_port, test_cell_id
  );
  let client = reqwest::Client::builder().build().unwrap();
  let response1 = client.get(&url).send().await.unwrap();
  assert_eq!(response1.status(), 200);
  assert_eq!(response1.text().await.unwrap().trim(), "1");

  // Send a second request to verify counter increments
  let response2 = client.get(&url).send().await.unwrap();
  assert_eq!(response2.status(), 200);
  assert_eq!(response2.text().await.unwrap().trim(), "2");

  // Kill the primary node
  println!("Killing primary node on port {}...", primary_owner_port);
  let primary_index = test_env
    .ports
    .iter()
    .position(|p| p.public() == primary_owner_port)
    .unwrap();
  test_env.kill_cell_instance(primary_index);

  // Wait for heartbeat timeout to detect node failure
  println!("Waiting for primary node failure to be detected...");
  sleep(Duration::from_secs(5)).await;

  // Send more requests to the same non-owner - it should retry forwarding to next in line
  println!("Sending request to non-owner after primary failure...");

  // This may take a few tries as the system detects failure and adjusts
  let mut success = false;
  for i in 1..=5 {
    match client.get(&url).send().await {
      Ok(response) => {
        if response.status().is_success() {
          let content = response.text().await.unwrap();
          println!("Attempt {}: Success, got: {}", i, content);
          // This should be "3" if the forwarding is working correctly
          assert_eq!(
            content.trim(),
            "3",
            "Counter should continue from previous value"
          );
          success = true;
          break;
        } else {
          println!("Attempt {}: Got status: {}", i, response.status());
        }
      }
      Err(e) => {
        println!("Attempt {}: Request error: {}", i, e);
      }
    }

    // Wait before retrying
    sleep(Duration::from_secs(1)).await;
  }

  assert!(
    success,
    "Proxy forwarding should eventually succeed with retries"
  );

  // Send one more request to verify stable forwarding
  println!("Testing stable forwarding after recovery...");
  let response4 = client.get(&url).send().await.unwrap();
  assert_eq!(response4.status(), 200);
  assert_eq!(response4.text().await.unwrap().trim(), "4");

  // Verify the forwarding path now points to a different node
  let peer_info_url =
    format!("http://localhost:{}/_mesh/peers", non_owner_port);
  let new_peers = reqwest::get(&peer_info_url)
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();

  println!("New peer info after primary failure: {}", new_peers);

  // Get the new owner info
  let new_owner_url = format!(
    "http://localhost:{}/_mesh/owner/{}",
    non_owner_port, test_cell_id
  );
  let new_owner_resp = reqwest::get(&new_owner_url)
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();

  println!("New owner info from non-owner node: {}", new_owner_resp);

  // The owner address should be different from the failed primary
  assert_ne!(
    new_owner_resp["owner"].as_str().unwrap(),
    owner_info[0].2,
    "New owner should be different from failed primary"
  );
}

/// Tests that database restore coordination works properly across nodes
#[tokio::test]
async fn test_restore_coordination() {
  // Use unique cell ID to avoid conflicts with other tests
  let test_cell_id = format!("restore-coord-{}", Uuid::new_v4().simple());
  println!("test_restore_coordination with cell ID: {}", test_cell_id);

  // Create a single-node environment
  let mut test_env = TestEnv::new(1);
  let port_a = test_env.ports[0].public();

  // Send request to Node A to create data in the cell
  let url_a =
    format!("http://basic-db.localhost:{}/cell/{}", port_a, test_cell_id);
  let client = reqwest::Client::builder().build().unwrap();

  let response_a = client.get(&url_a).send().await.unwrap();
  assert_eq!(response_a.status(), 200);
  let content_a = response_a.text().await.unwrap();
  assert_eq!(content_a.trim(), "1");

  // Make a second request to ensure data is updated
  println!("Sending second request to Node A");
  let response_a2 = client.get(&url_a).send().await.unwrap();
  let content_a2 = response_a2.text().await.unwrap();
  assert_eq!(content_a2.trim(), "2");

  // Verify SQLite database exists
  let db_path = test_env.server_dirs[0].path().join(format!(
    "data/basic-db.localhost/sqlite/{}.db",
    test_cell_id
  ));
  assert!(db_path.exists());

  println!("Waiting for Litestream to replicate data to S3...");
  sleep(Duration::from_secs(5)).await;

  // Stop Node A gracefully
  println!("Stopping Node A...");
  test_env.graceful_shutdown_cell_instance(0);

  // Rest of the test remains unchanged
  // Spawn two more nodes with auto-allocated ports
  println!("Starting Node B and Node C with auto-allocated ports");

  let (port_b, port_c) = {
    let mut ports = TestEnv::allocate_ports(7600, 2, 2);
    assert_eq!(ports.len(), 2);
    let port_b = ports.swap_remove(0);
    let port_c = ports.swap_remove(0);
    (port_b, port_c)
  };

  let url_b = format!(
    "http://basic-db.localhost:{}/cell/{}",
    port_b.public(),
    test_cell_id
  );
  let url_c = format!(
    "http://basic-db.localhost:{}/cell/{}",
    port_c.public(),
    test_cell_id
  );
  let owner_url_b = format!(
    "http://localhost:{}/_internal/mesh/owner/basic-db.localhost/{}",
    port_b.internal(),
    test_cell_id
  );
  let owner_url_c = format!(
    "http://localhost:{}/_internal/mesh/owner/basic-db.localhost/{}",
    port_c.internal(),
    test_cell_id
  );

  println!("Starting Node B on port {}", port_b.public());
  test_env.spawn_cell_instance(vec![port_b]);
  println!("Starting Node C on port {}", port_c.public());
  test_env.spawn_cell_instance(vec![port_c]);

  // Give time for the started nodes to settle on the latest view of the cluster
  sleep(Duration::from_secs(8)).await;

  // Determine which node is responsible for the cell by querying both

  let owner_resp_b = reqwest::get(&owner_url_b)
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();
  let owner_resp_c = reqwest::get(&owner_url_c)
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();

  println!("Node B owner info: {}", owner_resp_b);
  println!("Node C owner info: {}", owner_resp_c);

  // Get the owner for the test cell
  let is_b_owner = owner_resp_b["is_local"].as_bool().unwrap();
  let is_c_owner = owner_resp_c["is_local"].as_bool().unwrap();
  assert!(
    is_b_owner != is_c_owner,
    "Only one node should be the owner of the cell"
  );

  // First, try the node that should be the owner (more likely to succeed first)
  let (first_url, second_url) = if is_b_owner {
    (url_b.clone(), url_c.clone())
  } else {
    (url_c.clone(), url_b.clone())
  };

  let response_owner = client.get(&first_url).send().await.unwrap();
  let content_owner = response_owner.text().await.unwrap();
  assert_eq!(content_owner.trim(), "3");

  let response_not_owner = client.get(&second_url).send().await.unwrap();
  let content_not_owner = response_not_owner.text().await.unwrap();
  assert_eq!(content_not_owner.trim(), "4");

  // Note: In a full integration, we would also check logs to confirm one node
  // logged "Acquiring Lock" -> "Restoring" -> "Complete", and the other logged
  // "Acquiring Lock" -> "WaitingForLock". However, we'd need more detailed log
  // capturing for that level of verification.
}

/// Tests that replication and restore works correctly within a single cell
#[tokio::test]
async fn test_restore_single() {
  // Create a single-node environment
  let mut test_env = TestEnv::new(1);
  let port = test_env.ports[0].public();

  let test_cell_id = "test-restore";

  let url = format!("http://basic-db.localhost:{}/cell/{}", port, test_cell_id);
  let client = reqwest::Client::builder().build().unwrap();

  let response1 = client.get(&url).send().await.unwrap();
  assert_eq!(response1.status(), 200);
  let content1 = response1.text().await.unwrap();
  assert_eq!(content1.trim(), "1", "First request should return 1");

  sleep(Duration::from_secs(2)).await;

  println!("Shutting down celld instance...");
  test_env.graceful_shutdown_cell_instance(0);

  let new_port = TestEnv::allocate_ports(7620, 1, 2);
  assert_eq!(new_port.len(), 1);
  let new_url = format!(
    "http://basic-db.localhost:{}/cell/{}",
    new_port[0].public(),
    test_cell_id
  );
  test_env.spawn_cell_instance(new_port);

  let response2 = client.get(&new_url).send().await.unwrap();
  assert_eq!(response2.status(), 200);
  let content2 = response2.text().await.unwrap();
  assert_eq!(
    content2.trim(),
    "2",
    "Restored data should reflect previous state"
  );

  let response3 = client.get(&new_url).send().await.unwrap();
  assert_eq!(response3.status(), 200);
  let content3 = response3.text().await.unwrap();
  assert_eq!(content3.trim(), "3", "Third request should return 3");
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

/// Helper function to connect to a WebSocket cell and handle initial messages
async fn connect_to_cell(
  port: u16,
  cell_id: &str,
) -> (
  tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
  >,
  String, // username
) {
  // Use the hostname directly with the test port
  let url =
    Url::parse(&format!("ws://ws-echo.localhost:{}/cell/{}", port, cell_id))
      .unwrap();

  println!("Connecting to WebSocket at {}", url);

  let (mut ws_stream, _) = tokio_tungstenite::connect_async(url.to_string())
    .await
    .unwrap_or_else(|e| {
      panic!(
        "Failed to connect to cell {} on port {}: {}",
        cell_id, port, e
      )
    });

  // Read welcome message
  println!("Connected, waiting for welcome message");
  let welcome_data =
    read_message_of_type(&mut ws_stream, "welcome", 5000).await;
  let username = welcome_data["username"].as_str().unwrap().to_string();

  // Read userlist message
  let _userlist_data =
    read_message_of_type(&mut ws_stream, "userlist", 5000).await;

  println!("Connected to cell {} as {}", cell_id, username);
  (ws_stream, username)
}
