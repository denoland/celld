mod common;

use common::TestEnv;
use tracing::info;

#[test_log::test(tokio::test)]
async fn test_system_main_cell_relocation() {
  const SYSTEM_TENANT: &str = "_system";
  const SYSTEM_CELL_ID: &str = "main";
  const CELL_HASHRING_SEED: &str = "42";

  let client = reqwest::Client::new();

  // Find two available ports that will trigger relocation with the given seed
  let seed: u64 = CELL_HASHRING_SEED.parse().unwrap();
  let (initial_port, second_port) =
    TestEnv::find_relocation_ports(seed, SYSTEM_TENANT, SYSTEM_CELL_ID)
      .expect("Could not find available ports that trigger relocation");

  let initial_ports = vec![initial_port];
  let second_ports = vec![second_port];

  // Start with a single node cluster using a seeded hasher
  let mut test_env = TestEnv::new_with_ports_and_envs(
    initial_ports,
    "test_system_main_cell_relocation",
    // Set the environment variable for deterministic hashing
    &[("CELL_HASHRING_SEED", CELL_HASHRING_SEED)],
  )
  .await;

  let initial_port = test_env.ports[0].public();
  let initial_internal_port = test_env.ports[0].internal();

  // Wait for initial setup to complete
  tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

  // Verify the initial node owns the system main cell
  let initial_owner_url = format!(
    "http://localhost:{}/_internal/mesh/owner/{}/{}",
    initial_internal_port, SYSTEM_TENANT, SYSTEM_CELL_ID
  );

  let initial_resp = client
    .get(&initial_owner_url)
    .send()
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();

  assert!(
    initial_resp["is_local"].as_bool().unwrap(),
    "Initial node should own the system main cell"
  );
  let initial_owner = initial_resp["owner"].as_str().unwrap();
  info!("Initial system main cell owner: {}", initial_owner);

  let test_cell_id = uuid::Uuid::new_v4().simple().to_string();

  // Set a new alarm to save something to the system main cell's DB
  let test_cell_url =
    format!("http://localhost:{}/cell/{}", initial_port, test_cell_id);

  let res = client
    .post(&test_cell_url)
    .header("host", "alarm.localhost")
    .body(u32::MAX.to_string()) // Will never be dispatched
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  // Wait for Litestream to replicate data to S3
  info!("Waiting for Litestream to replicate data to S3...");
  tokio::time::sleep(std::time::Duration::from_secs(5)).await;

  // Add a second node that should trigger relocation due to deterministic hashing
  let second_internal_port = second_ports[0].internal();
  test_env
    .spawn_cell_instance(second_ports, "test_system_main_cell_relocation")
    .await;

  // Wait for cluster membership to stabilize
  tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

  // Check who owns the system main cell now from the second node's perspective
  let owner_check_url = format!(
    "http://localhost:{}/_internal/mesh/owner/{}/{}",
    second_internal_port, SYSTEM_TENANT, SYSTEM_CELL_ID
  );

  let owner_resp = client
    .get(&owner_check_url)
    .send()
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();

  let current_owner = owner_resp["owner"].as_str().unwrap();
  info!(
    "System main cell owner after adding second node: {}",
    current_owner
  );

  assert!(owner_resp["is_local"].as_bool().unwrap());

  info!("System main cell has been relocated to the new node!");

  // Verify the initial node no longer owns the system main cell
  let initial_check_resp = client
    .get(&initial_owner_url)
    .send()
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();

  assert!(
    !initial_check_resp["is_local"].as_bool().unwrap(),
    "Initial node should no longer own the system main cell"
  );

  // Verify both nodes see the same owner
  let initial_owner_view = initial_check_resp["owner"].as_str().unwrap();
  assert_eq!(
    current_owner, initial_owner_view,
    "Both nodes should agree on who owns the system main cell"
  );

  // Get the alarm to see the system main cell's DB has been moved to the new node
  let res = client
    .get(&test_cell_url)
    .header("host", "alarm.localhost")
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  let content = res.text().await.unwrap();
  assert_ne!(content, "null");

  info!("System main cell relocation test completed successfully!");
}

// In this test, the system main cell is relocated Node A -> Node B -> Node A.
// Any update made to the system main cell's DB in Node B should be replicated
// to Node A.
#[test_log::test(tokio::test)]
async fn test_system_main_cell_relocation_with_existing_db() {
  const SYSTEM_TENANT: &str = "_system";
  const SYSTEM_CELL_ID: &str = "main";
  const CELL_HASHRING_SEED: &str = "42";

  let client = reqwest::Client::new();

  // Find two available ports that will trigger relocation with the given seed
  let seed: u64 = CELL_HASHRING_SEED.parse().unwrap();
  let (initial_port, second_port) =
    TestEnv::find_relocation_ports(seed, SYSTEM_TENANT, SYSTEM_CELL_ID)
      .expect("Could not find available ports that trigger relocation");

  let initial_ports = vec![initial_port];
  let second_ports = vec![second_port];

  // Start with a single node cluster using a seeded hasher
  let mut test_env = TestEnv::new_with_ports_and_envs(
    initial_ports,
    "test_system_main_cell_relocation_with_existing_db",
    // Set the environment variable for deterministic hashing
    &[("CELL_HASHRING_SEED", CELL_HASHRING_SEED)],
  )
  .await;

  let initial_port = test_env.ports[0].public();
  let initial_internal_port = test_env.ports[0].internal();

  // Wait for initial setup to complete
  tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

  // Verify the initial node owns the system main cell
  let initial_owner_url = format!(
    "http://localhost:{}/_internal/mesh/owner/{}/{}",
    initial_internal_port, SYSTEM_TENANT, SYSTEM_CELL_ID
  );

  let initial_resp = client
    .get(&initial_owner_url)
    .send()
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();

  assert!(
    initial_resp["is_local"].as_bool().unwrap(),
    "Initial node should own the system main cell"
  );
  let initial_owner = initial_resp["owner"].as_str().unwrap();
  info!("Initial system main cell owner: {}", initial_owner);

  // Get the alarm, which should not exist yet
  let test_cell_id = uuid::Uuid::new_v4().simple().to_string();
  let test_cell_initial_node_url =
    format!("http://localhost:{}/cell/{}", initial_port, test_cell_id);
  let res = client
    .get(&test_cell_initial_node_url)
    .header("host", "alarm.localhost")
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  let content = res.text().await.unwrap();
  assert_eq!(content, "null");

  // Add a second node that should trigger relocation due to deterministic hashing
  let second_port = second_ports[0].public();
  let second_internal_port = second_ports[0].internal();
  test_env
    .spawn_cell_instance(
      second_ports,
      "test_system_main_cell_relocation_with_existing_db",
    )
    .await;

  // Wait for cluster membership to stabilize
  tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

  // Check who owns the system main cell now from the second node's perspective
  let owner_check_url = format!(
    "http://localhost:{}/_internal/mesh/owner/{}/{}",
    second_internal_port, SYSTEM_TENANT, SYSTEM_CELL_ID
  );

  let owner_resp = client
    .get(&owner_check_url)
    .send()
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();

  let current_owner = owner_resp["owner"].as_str().unwrap();
  info!(
    "System main cell owner after adding second node: {}",
    current_owner
  );

  assert!(owner_resp["is_local"].as_bool().unwrap());

  info!("System main cell has been relocated to the new node!");

  // Verify the initial node no longer owns the system main cell
  let initial_check_resp = client
    .get(&initial_owner_url)
    .send()
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();

  assert!(
    !initial_check_resp["is_local"].as_bool().unwrap(),
    "Initial node should no longer own the system main cell"
  );

  // Verify both nodes see the same owner
  let initial_owner_view = initial_check_resp["owner"].as_str().unwrap();
  assert_eq!(
    current_owner, initial_owner_view,
    "Both nodes should agree on who owns the system main cell"
  );

  // Set a new alarm to save something to the system main cell's DB on the second node
  let test_cell_second_node_url =
    format!("http://localhost:{}/cell/{}", second_port, test_cell_id);

  let res = client
    .post(&test_cell_second_node_url)
    .header("host", "alarm.localhost")
    .body(u32::MAX.to_string()) // Will never be dispatched
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  // Wait for Litestream to replicate data to S3
  info!("Waiting for Litestream to replicate data to S3...");
  tokio::time::sleep(std::time::Duration::from_secs(5)).await;

  // Get the alarm, which should now exist
  let res = client
    .get(&test_cell_initial_node_url)
    .header("host", "alarm.localhost")
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  let content = res.text().await.unwrap();
  assert_ne!(content, "null");

  // Shutdown the second node, which should trigger relocation of the system
  // main cell back to the initial node
  test_env.graceful_shutdown_cell_instance(1);

  info!("Waiting for second node shutdown to be detected...");
  tokio::time::sleep(tokio::time::Duration::from_secs(8)).await;

  // Verify the initial node owns the system main cell again
  let initial_resp = client
    .get(&initial_owner_url)
    .send()
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();

  assert!(
    initial_resp["is_local"].as_bool().unwrap(),
    "Initial node should own the system main cell"
  );

  // Get the alarm to verify that the system main cell's DB has been restored
  // to the initial node
  let res = client
    .get(&test_cell_initial_node_url)
    .header("host", "alarm.localhost")
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  let content = res.text().await.unwrap();
  assert_ne!(content, "null");
}
