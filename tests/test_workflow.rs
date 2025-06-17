mod common;

use common::TestEnv;
use serde_json::json;
use tracing::{debug, info};

#[test_log::test(tokio::test)]
async fn test_reliable_workflow_in_single_node_cluster() {
  let test_env = TestEnv::new(1);
  let port = test_env.ports[0].public();

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  let run_id = dispatch_workflow(
    &client,
    &url,
    "reliable",
    json!({
      "username": "magurotuna",
      "email": "magurotuna@example.com",
      "phoneNumber": "+1234567890"
    }),
  )
  .await;

  assert!(
    wait_for_workflow_completion(&client, &url, &run_id, 10).await,
    "Reliable workflow should complete"
  );

  // Get logs
  let res = client
    .get(format!("{}/logs", url))
    .header("host", "workflow.localhost")
    .send()
    .await
    .unwrap();

  #[derive(serde::Deserialize)]
  struct Log {
    #[allow(dead_code)]
    id: u64,
    text: String,
    #[allow(dead_code)]
    created_at: String,
  }

  let content = res.json::<Vec<Log>>().await.unwrap();
  assert_eq!(content.len(), 2);
  assert_eq!(content[0].text, "magurotuna signup SMS sent to +1234567890");
  assert_eq!(
    content[1].text,
    "magurotuna signup email sent to magurotuna@example.com"
  );
}

#[test_log::test(tokio::test)]
async fn test_flaky_workflow_in_single_node_cluster() {
  let test_env = TestEnv::new(1);
  let port = test_env.ports[0].public();

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  let run_id = dispatch_workflow(&client, &url, "flaky", json!({})).await;

  let content = wait_for_workflow_step_count(&client, &url, &run_id, 1, 10)
    .await
    .expect("First step should complete");

  assert_eq!(content["workflowName"], "flaky");
  assert_eq!(content["steps"][0]["name"], "generate-random-number");
  let generated_random_number =
    content["steps"][0]["outputData"].as_u64().unwrap();

  // Set `flaky: 1` to unblock the workflow
  set_key_value(&client, &url, "flaky", 1).await;

  // Wait until all 3 steps are completed
  let content = wait_for_workflow_step_count(&client, &url, &run_id, 3, 10)
    .await
    .expect("All 3 steps should complete");

  assert_eq!(content["workflowName"], "flaky");
  assert_eq!(content["steps"][2]["name"], "multiply-random-number-by-2");
  let last_step_output = content["steps"][2]["outputData"].as_u64().unwrap();

  // The last step should return the result of multiplying the memoized random
  // number by 2
  assert_eq!(last_step_output, generated_random_number * 2);
}

#[flaky_test::flaky_test(tokio)]
async fn test_workflow_automatic_resume_after_node_failure() {
  let mut test_env = TestEnv::new(3);

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let client = reqwest::Client::new();

  // Find which node is the primary owner for this cell
  let mut primary_owner_port = 0;
  let mut primary_owner_index = usize::MAX;
  let mut secondary_owners = Vec::new();

  for (i, port) in test_env.ports.iter().enumerate() {
    let public_port = port.public();
    let internal_port = port.internal();
    let owner_url = format!(
      "http://localhost:{}/_internal/mesh/owner/workflow.localhost/{}",
      internal_port, cell_id
    );
    let owner_resp = client
      .get(&owner_url)
      .send()
      .await
      .unwrap()
      .json::<serde_json::Value>()
      .await
      .unwrap();

    info!("Node on port {} owner info: {}", public_port, owner_resp);

    let is_owner = owner_resp["is_local"].as_bool().unwrap();
    if is_owner {
      primary_owner_port = public_port;
      primary_owner_index = i;
    } else {
      secondary_owners.push(public_port);
    }
  }

  assert_ne!(
    primary_owner_port, 0,
    "Failed to find primary owner for test cell"
  );
  assert_ne!(
    primary_owner_index,
    usize::MAX,
    "Failed to find primary owner for test cell"
  );
  assert!(
    !secondary_owners.is_empty(),
    "Failed to find secondary owners for test cell"
  );

  info!("Primary owner is on port: {}", primary_owner_port);
  info!("Secondary owners are on ports: {:?}", secondary_owners);

  let primary_url =
    format!("http://localhost:{}/cell/{}", primary_owner_port, cell_id);

  // Dispatch the flaky workflow on primary node
  let run_id =
    dispatch_workflow(&client, &primary_url, "flaky", json!({})).await;

  // Wait until the first step is completed
  let content =
    wait_for_workflow_step_count(&client, &primary_url, &run_id, 1, 10)
      .await
      .expect("First step should complete");

  assert_eq!(content["workflowName"], "flaky");
  assert_eq!(content["steps"][0]["name"], "generate-random-number");
  let generated_random_number =
    content["steps"][0]["outputData"].as_u64().unwrap();

  info!("Waiting for Litestream to replicate data to S3...");
  tokio::time::sleep(std::time::Duration::from_secs(5)).await;

  // Shutdown the primary owner
  test_env.graceful_shutdown_cell_instance(primary_owner_index);

  // Wait for the shutdown to be detected
  debug!("Waiting for primary node failure to be detected...");
  tokio::time::sleep(std::time::Duration::from_secs(8)).await;

  let secondary_url =
    format!("http://localhost:{}/cell/{}", secondary_owners[0], cell_id);
  info!(
    "Sending request to secondary node on port {}...",
    secondary_owners[0]
  );

  // Unblock the workflow on secondary node by setting `flaky: 1`
  set_key_value(&client, &secondary_url, "flaky", 1).await;

  // Wait for the workflow to be resumed (the resume is scheduled 10s after the
  // primary owner is gracefully shutdown)
  tokio::time::sleep(std::time::Duration::from_secs(10)).await;

  // Wait until all 3 steps are completed on secondary node
  let content =
    wait_for_workflow_step_count(&client, &secondary_url, &run_id, 3, 10)
      .await
      .expect("All 3 steps should complete on secondary node");

  assert_eq!(content["workflowName"], "flaky");
  assert_eq!(content["steps"][2]["name"], "multiply-random-number-by-2");
  let last_step_output = content["steps"][2]["outputData"].as_u64().unwrap();

  // The last step should return the result of multiplying the memoized random
  // number by 2
  assert_eq!(last_step_output, generated_random_number * 2);
}

#[test_log::test(tokio::test)]
async fn test_invoke_workflow() {
  let test_env = TestEnv::new(1);
  let port = test_env.ports[0].public();

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  // Scenario 1: Invoked workflow completes without shutdown
  let run_id_parent =
    dispatch_workflow(&client, &url, "parent", json!({ "value": 10 })).await;

  assert!(
    wait_for_workflow_completion(&client, &url, &run_id_parent, 20).await,
    "Parent workflow should complete"
  );
}

#[test_log::test(tokio::test)]
async fn test_sleep_workflow() {
  let test_env = TestEnv::new(1);
  let port = test_env.ports[0].public();

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  let start_time = std::time::Instant::now();

  // Dispatch the sleep workflow
  let run_id = dispatch_workflow(
    &client,
    &url,
    "sleep",
    json!({ "sleepDurationMs": 1000 }),
  )
  .await;

  // Wait for workflow completion
  assert!(
    wait_for_workflow_completion(&client, &url, &run_id, 10).await,
    "Sleep workflow should complete"
  );

  let elapsed = start_time.elapsed();
  // Should take at least 1 second due to the sleep
  assert!(
    elapsed.as_secs() >= 1,
    "Workflow completed too quickly: {}ms",
    elapsed.as_millis()
  );

  // Get logs to verify the workflow steps
  let res = client
    .get(format!("{}/logs", url))
    .header("host", "workflow.localhost")
    .send()
    .await
    .unwrap();

  #[derive(serde::Deserialize)]
  struct Log {
    #[allow(dead_code)]
    id: u64,
    text: String,
    #[allow(dead_code)]
    created_at: String,
  }

  let content = res.json::<Vec<Log>>().await.unwrap();
  assert_eq!(content.len(), 2);
  // Logs are returned in reverse chronological order
  assert_eq!(content[0].text, "Sleep completed after 1000ms");
  assert_eq!(content[1].text, "Starting sleep for 1000ms");
}

#[test_log::test(tokio::test)]
async fn test_sleep_workflow_single_node_no_s3() {
  use std::process::Command;
  use std::time::{Duration, Instant};
  use tempfile::TempDir;
  use uuid::Uuid;

  // Allocate ports for this test
  let ports = TestEnv::allocate_ports(7600, 1, 2);
  let port = &ports[0];
  let advertise_addr = format!("127.0.0.1:{}", port.public());
  let internal_addr = format!("127.0.0.1:{}", port.internal());

  // Create temporary directory with proper structure
  let temp_dir = TempDir::new().expect("Failed to create temp dir");
  let project_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
  let src_data_path = project_root.join("data");
  let src_sdk_path = project_root.join("sdk");
  let dst_data_path = temp_dir.path().join("data");
  let dst_sdk_path = temp_dir.path().join("sdk");

  // Copy directories
  std::fs::create_dir_all(&dst_data_path).unwrap();
  common::copy_directory_without_sqlite(&src_sdk_path, &dst_sdk_path).unwrap();
  common::copy_directory_without_sqlite(&src_data_path, &dst_data_path).unwrap();

  info!("Starting celld in single-node mode without S3...");

  // Start celld without any S3 configuration
  let mut celld_process = Command::new(env!("CARGO_BIN_EXE_celld"))
    .env("RUST_LOG", "debug,celld=warn") // Enable debug logging to see our alarm debugging
    .env("ADVERTISE_ADDR", &advertise_addr)
    .env("INTERNAL_LISTEN_ADDR", &internal_addr)
    .env("DATA", &dst_data_path)
    .env("CELL_HEARTBEAT_INTERVAL", "1")
    .env("CELL_GRACE_PERIOD_SECONDS", "5")
    .env("CELL_STALENESS_THRESHOLD_SECS", "6")
    .env("CELL_LOCK_GUARD_TTL_SECS", "6")
    .env("CELL_ALARM_SCHEDULER_INTERVAL_SECS", "1") // Check alarms frequently
    .env("CELL_DENO_OUTPUT", "1")
    // Explicitly do NOT set any CELL_S3_* environment variables
    .current_dir(temp_dir.path())
    .spawn()
    .expect("Failed to start celld");

  // Wait for server to be ready
  info!("Waiting for celld to be ready on port {}...", port.public());
  let max_attempts = 20;
  let mut server_ready = false;
  for attempt in 1..=max_attempts {
    match std::net::TcpStream::connect(&advertise_addr) {
      Ok(_) => {
        info!("Celld is ready on port {}", port.public());
        server_ready = true;
        break;
      }
      Err(_) => {
        info!("Waiting for celld (attempt {}/{})", attempt, max_attempts);
        tokio::time::sleep(Duration::from_millis(500)).await;
      }
    }
  }

  if !server_ready {
    // Kill the process and get output for debugging
    let _ = celld_process.kill();
    let _ = celld_process.wait().unwrap();
    panic!("couldn't start celld");
  }

  // Give it a moment more to fully initialize
  tokio::time::sleep(Duration::from_secs(2)).await;

  let cell_id = Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port.public(), cell_id);
  let client = reqwest::Client::new();

  info!("Testing delay-sleep workflow with exact cellverse step pattern...");
  let start_time = Instant::now();

  let sleep_ms = 5000;

  // Test WebSocket dispatch like cellverse with persistent connection
  let (run_id, mut ws_connection) = dispatch_workflow_via_websocket_persistent(
    &url,
    sleep_ms,
    500,
  )
  .await;

  info!("Delay-sleep workflow dispatched, run_id: {}", run_id);

  // Wait for workflow to reach the sleep step, but don't poll for completion
  info!("Waiting for workflow to reach sleep step...");
  tokio::time::sleep(Duration::from_secs(2)).await;

  // Check the global_alarms table directly using rusqlite
  let db_path = dst_data_path.join("_system/sqlite/main.db");
  info!("Checking global alarms table at: {:?}", db_path);

  let alarm_count =  {
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count = conn.query_row("SELECT COUNT(*) FROM global_alarms", [], |row| { row.get::<_, i32>(0) }).unwrap();
    assert_eq!(count, 1, "Expected exactly 1 global alarm to be scheduled.");
  };


  // Wait for workflow to reach the sleep step, but don't poll for completion
  info!("Waiting for workflow to reach sleep step...");
  tokio::time::sleep(Duration::from_millis(sleep_ms + 4000)).await;


  let alarm_count =  {
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let count = conn.query_row("SELECT COUNT(*) FROM global_alarms", [], |row| { row.get::<_, i32>(0) }).unwrap();
    assert_eq!(count, 0, "Expected no global alarms after sleep step.");
  };

  // Clean up: close WebSocket connection and kill the celld process
  info!("Closing WebSocket connection...");
  ws_connection.close().await;
  
  let _ = celld_process.kill();
  let _ = celld_process.wait().unwrap();
}

// Helper functions for workflow testing
async fn dispatch_workflow(
  client: &reqwest::Client,
  url: &str,
  endpoint: &str,
  payload: serde_json::Value,
) -> String {
  let res = client
    .post(format!("{}/{}", url, endpoint))
    .header("host", "workflow.localhost")
    .json(&payload)
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);
  res.text().await.unwrap()
}

async fn wait_for_workflow_completion(
  client: &reqwest::Client,
  url: &str,
  run_id: &str,
  max_attempts: usize,
) -> bool {
  for _ in 0..max_attempts {
    let res = client
      .get(format!("{}/run-progress", url))
      .header("host", "workflow.localhost")
      .query(&[("id", run_id)])
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.json::<serde_json::Value>().await.unwrap();
    if !content["completedAt"].is_null() {
      return true;
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
  }
  false
}

async fn set_key_value(
  client: &reqwest::Client,
  url: &str,
  key: &str,
  value: i32,
) {
  let res = client
    .post(format!("{}/kv", url))
    .header("host", "workflow.localhost")
    .json(&json!({ "key": key, "value": value }))
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);
}

struct WebSocketConnection {
  sender: futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, tokio_tungstenite::tungstenite::protocol::Message>,
  receiver: futures_util::stream::SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
}

impl WebSocketConnection {
  async fn close(mut self) {
    use futures_util::SinkExt;
    let _ = self.sender.close().await;
  }
}

async fn dispatch_workflow_via_websocket_persistent(
  base_url: &str,
  sleep_duration_ms: u64,
  delay_ms: u64,
) -> (String, WebSocketConnection) {
  use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
  use futures_util::{SinkExt, StreamExt};
  
  // Convert HTTP URL to WebSocket URL and add proper host header
  let ws_url = base_url.replace("http://", "ws://");
  info!("Connecting to WebSocket: {}", ws_url);
  
  // Create request with proper Host header for workflow.localhost
  let request = tokio_tungstenite::tungstenite::http::Request::builder()
    .uri(&ws_url)
    .header("Host", "workflow.localhost")
    .header("Connection", "Upgrade")
    .header("Upgrade", "websocket")
    .header("Sec-WebSocket-Version", "13")
    .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
    .body(())
    .unwrap();
  
  let (ws_stream, _) = connect_async(request).await.expect("Failed to connect to WebSocket");
  let (mut ws_sender, mut ws_receiver) = ws_stream.split();
  
  // Send dispatch message
  let dispatch_message = json!({
    "type": "dispatch_delay_sleep",
    "sleepDurationMs": sleep_duration_ms,
    "delayMs": delay_ms
  });
  
  info!("Sending WebSocket message: {}", dispatch_message);
  ws_sender
    .send(Message::Text(dispatch_message.to_string().into()))
    .await
    .expect("Failed to send WebSocket message");
  
  // Wait for response with run_id
  while let Some(msg) = ws_receiver.next().await {
    match msg {
      Ok(Message::Text(text)) => {
        info!("Received WebSocket response: {}", text);
        let response: serde_json::Value = serde_json::from_str(&text).unwrap();
        if response["type"] == "workflow_dispatched" {
          let run_id = response["runId"].as_str().unwrap().to_string();
          info!("Keeping WebSocket connection open like cellverse...");
          return (run_id, WebSocketConnection { sender: ws_sender, receiver: ws_receiver });
        } else if response["type"] == "error" {
          panic!("WebSocket error: {}", response["message"]);
        }
      }
      Ok(Message::Close(_)) => {
        panic!("WebSocket closed unexpectedly");
      }
      Err(e) => {
        panic!("WebSocket error: {}", e);
      }
      _ => {} // Ignore other message types
    }
  }
  
  panic!("Did not receive workflow_dispatched response");
}

async fn wait_for_workflow_step_count(
  client: &reqwest::Client,
  url: &str,
  run_id: &str,
  expected_steps: usize,
  max_attempts: usize,
) -> Option<serde_json::Value> {
  for _ in 0..max_attempts {
    let res = client
      .get(format!("{}/run-progress", url))
      .header("host", "workflow.localhost")
      .query(&[("id", run_id)])
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.json::<serde_json::Value>().await.unwrap();
    if content["steps"].as_array().unwrap().len() == expected_steps {
      return Some(content);
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
  }
  None
}
