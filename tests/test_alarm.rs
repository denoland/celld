mod common;

use common::TestEnv;

#[tokio::test]
async fn test_alarm_crud_in_single_node_cluster() {
  let test_env = TestEnv::new(1);
  let port = test_env.public_ports[0];

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  // Get alarm (none is set)
  {
    let res = client
      .get(&url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, "null");
  }

  // Delete alarm (nothing deleted)
  {
    let res = client
      .delete(&url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"deleted":false}"#);
  }

  // Set alarm
  {
    let res = client
      .post(&url)
      .header("host", "alarm.localhost")
      .body("5000")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
  }

  // Get alarm (should exist)
  {
    let res = client
      .get(&url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_ne!(content, "null");
  }

  // Delete alarm
  {
    let res = client
      .delete(&url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"deleted":true}"#);
  }

  // Get alarm (should not exist)
  {
    let res = client
      .get(&url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, "null");
  }
}

#[tokio::test]
async fn test_alarm_dispatch_in_single_node_cluster() {
  let test_env = TestEnv::new(1);
  let port = test_env.public_ports[0];

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let alarm_count_url = format!("{url}/getAlarmCount");
  let client = reqwest::Client::new();

  // Get initial alarm count
  {
    let res = client
      .get(&alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"count":0}"#);
  }

  // Set a new alarm scheduled 1 second from now
  {
    let res = client
      .post(&url)
      .header("host", "alarm.localhost")
      .body("1000")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
  }

  // Wait for the alarm to be dispatched
  tokio::time::sleep(std::time::Duration::from_secs(5)).await;

  // Get alarm count, should be 1 now
  {
    let res = client
      .get(&alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"count":1}"#);
  }
}

#[tokio::test]
async fn test_multiple_cells_alarm_dispatch_in_single_node_cluster() {
  let test_env = TestEnv::new(1);
  let port = test_env.public_ports[0];

  let cell1_id = uuid::Uuid::new_v4().simple().to_string();
  let cell1_url = format!("http://localhost:{}/cell/{}", port, cell1_id);
  let cell1_alarm_count_url = format!("{cell1_url}/getAlarmCount");

  let cell2_id = uuid::Uuid::new_v4().simple().to_string();
  let cell2_url = format!("http://localhost:{}/cell/{}", port, cell2_id);
  let cell2_alarm_count_url = format!("{cell2_url}/getAlarmCount");

  let client = reqwest::Client::new();

  // Get initial alarm count for cell1
  {
    let res = client
      .get(&cell1_alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"count":0}"#);
  }

  // Get initial alarm count for cell2
  {
    let res = client
      .get(&cell2_alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"count":0}"#);
  }

  // Set a new alarm for cell1 scheduled 1 second from now
  {
    let res = client
      .post(&cell1_url)
      .header("host", "alarm.localhost")
      .body("1000")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
  }

  // Set a new alarm for cell2 scheduled 6 seconds from now
  {
    let res = client
      .post(&cell2_url)
      .header("host", "alarm.localhost")
      .body("6000")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
  }

  // Wait for the alarm to be dispatched
  tokio::time::sleep(std::time::Duration::from_secs(5)).await;

  // Get alarm count for cell1, should be 1 now
  {
    let res = client
      .get(&cell1_alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"count":1}"#);
  }

  // Alarm count for cell2 should still be 0
  {
    let res = client
      .get(&cell2_alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"count":0}"#);
  }

  // Wait for the alarm to be dispatched
  tokio::time::sleep(std::time::Duration::from_secs(5)).await;

  // Get alarm count for cell1, should be 1 now
  {
    let res = client
      .get(&cell1_alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"count":1}"#);
  }

  // Get alarm count for cell2, should be 1 now
  {
    let res = client
      .get(&cell2_alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"count":1}"#);
  }
}

#[tokio::test]
async fn system_cell_takeover() {
  let client = reqwest::Client::new();

  let mut test_env = TestEnv::new(2);

  let mut system_cell_index = None;
  let mut secondary_cell_external_ports = Vec::new();

  for (index, internal_port) in test_env.internal_ports.iter().enumerate() {
    let owner_url = format!(
      "http://localhost:{internal_port}/_internal/mesh/owner/_system/main"
    );
    let owner_resp = client
      .get(&owner_url)
      .send()
      .await
      .unwrap()
      .json::<serde_json::Value>()
      .await
      .unwrap();
    if owner_resp["is_local"].as_bool().unwrap() {
      system_cell_index = Some(index);
    } else {
      secondary_cell_external_ports.push(test_env.public_ports[index]);
    }
  }

  let system_cell_index = system_cell_index.unwrap();
  assert!(!secondary_cell_external_ports.is_empty());

  let test_cell_id = uuid::Uuid::new_v4().simple().to_string();

  // Attempt to get an alarm (none should exist)
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      test_env.public_ports[system_cell_index], test_cell_id
    );

    let res = client
      .get(&test_cell_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, "null");
  }

  // Set a new alarm to save something to the system cell's DB
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      test_env.public_ports[system_cell_index], test_cell_id
    );

    let res = client
      .post(&test_cell_url)
      .header("host", "alarm.localhost")
      .body(u32::MAX.to_string()) // Will never be dispatched
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
  }

  // Wait for Litestream to replicate data to S3
  println!("Waiting for Litestream to replicate data to S3...");
  tokio::time::sleep(std::time::Duration::from_secs(5)).await;

  // Shutdown the node that the system cell belongs to
  test_env.graceful_shutdown_cell_instance(system_cell_index);

  // Wait for the shutdown to be detected
  println!("Waiting for primary node failure to be detected...");
  tokio::time::sleep(std::time::Duration::from_secs(8)).await;

  // Get the set alarm through the secondary node to see the system cell's DB has been restored
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      secondary_cell_external_ports[0], test_cell_id
    );

    let res = client
      .get(&test_cell_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_ne!(content, "null");
  }
}

#[tokio::test]
async fn alarm_crud_operations_forwarded_to_system_cell_node() {
  let client = reqwest::Client::new();

  let test_env = TestEnv::new(2);

  let mut system_cell_index = None;
  struct Port {
    internal: u16,
    external: u16,
  }
  let mut secondary_cell_ports = Vec::new();

  // Identify the node where the system cell is running
  for (index, internal_port) in test_env.internal_ports.iter().enumerate() {
    let owner_url = format!(
      "http://localhost:{internal_port}/_internal/mesh/owner/_system/main"
    );
    let owner_resp = client
      .get(&owner_url)
      .send()
      .await
      .unwrap()
      .json::<serde_json::Value>()
      .await
      .unwrap();
    if owner_resp["is_local"].as_bool().unwrap() {
      system_cell_index = Some(index);
    } else {
      secondary_cell_ports.push(Port {
        internal: test_env.internal_ports[index],
        external: test_env.public_ports[index],
      });
    }
  }

  let system_cell_index = system_cell_index.unwrap();
  assert!(!secondary_cell_ports.is_empty());

  // Find a cell ID that does NOT belong to the system cell's node
  let test_cell_id = {
    let mut test_cell_id = None;
    for _ in 0.. {
      let secondary_cell_internal_port = secondary_cell_ports[0].internal;
      let tmp_id = uuid::Uuid::new_v4().simple().to_string();
      let owner_url = format!(
      "http://localhost:{secondary_cell_internal_port}/_internal/mesh/owner/alarm.localhost/{tmp_id}"
    );
      let owner_resp = client
        .get(&owner_url)
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
      if owner_resp["is_local"].as_bool().unwrap() {
        test_cell_id = Some(tmp_id);
        break;
      }
    }

    test_cell_id.unwrap()
  };

  // Attempt to get an alarm (none should exist)
  // Request flow should look like this:
  // client -> primary node -> secondary node (where test_cell_id runs) -> isolate (which issues GetAlarm request) -> secondary node -> primary node -> system cell -> primary node -> secondary node -> isolate
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      test_env.public_ports[system_cell_index], test_cell_id
    );

    let res = client
      .get(&test_cell_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, "null");
  }

  // Set a new alarm to save something to the system cell's DB
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      test_env.public_ports[system_cell_index], test_cell_id
    );

    let res = client
      .post(&test_cell_url)
      .header("host", "alarm.localhost")
      .body(u32::MAX.to_string()) // Will never be dispatched
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
  }

  // Get the alarm through the secondary node to see the system cell's DB has been updated
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      secondary_cell_ports[0].external, test_cell_id
    );

    let res = client
      .get(&test_cell_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_ne!(content, "null");
  }

  // Delete the alarm
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      secondary_cell_ports[0].external, test_cell_id
    );

    let res = client
      .delete(&test_cell_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
  }

  // Try to get an alarm again, should not exist
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      secondary_cell_ports[0].external, test_cell_id
    );

    let res = client
      .get(&test_cell_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, "null");
  }
}

// There are two celld nodes in the cluster:
// 1. A primary node (where the system cell is running)
// 2. A secondary node (where "alarm.localhost/<some-cell-id>" is running)
// This test verifies that the primary node will dispatch an alarm to the secondary node and then alarm.localhost's alarm handler will be triggered.
#[tokio::test]
async fn alarm_dispatch_to_remote_cell_owner() {
  let client = reqwest::Client::new();

  let test_env = TestEnv::new(2);

  let mut system_cell_index = None;
  struct Port {
    internal: u16,
    #[allow(dead_code)]
    external: u16,
  }
  let mut secondary_cell_ports = Vec::new();

  // Identify the node where the system cell is running
  for (index, internal_port) in test_env.internal_ports.iter().enumerate() {
    let owner_url = format!(
      "http://localhost:{internal_port}/_internal/mesh/owner/_system/main"
    );
    let owner_resp = client
      .get(&owner_url)
      .send()
      .await
      .unwrap()
      .json::<serde_json::Value>()
      .await
      .unwrap();
    if owner_resp["is_local"].as_bool().unwrap() {
      system_cell_index = Some(index);
    } else {
      secondary_cell_ports.push(Port {
        internal: test_env.internal_ports[index],
        external: test_env.public_ports[index],
      });
    }
  }

  let system_cell_index = system_cell_index.unwrap();
  assert!(!secondary_cell_ports.is_empty());

  // Find a cell ID that does NOT belong to the system cell's node
  let test_cell_id = {
    let mut test_cell_id = None;
    for _ in 0.. {
      let secondary_cell_internal_port = secondary_cell_ports[0].internal;
      let tmp_id = uuid::Uuid::new_v4().simple().to_string();
      let owner_url = format!(
      "http://localhost:{secondary_cell_internal_port}/_internal/mesh/owner/alarm.localhost/{tmp_id}"
    );
      let owner_resp = client
        .get(&owner_url)
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
      if owner_resp["is_local"].as_bool().unwrap() {
        test_cell_id = Some(tmp_id);
        break;
      }
    }

    test_cell_id.unwrap()
  };

  let cell_url = format!(
    "http://localhost:{}/cell/{}",
    test_env.public_ports[system_cell_index], test_cell_id
  );
  let alarm_count_url = format!("{cell_url}/getAlarmCount");

  // Get initial alarm count, which must be 0
  {
    let res = client
      .get(&alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"count":0}"#);
  }

  // Set a new alarm scheduled 1 second from now
  {
    let res = client
      .post(&cell_url)
      .header("host", "alarm.localhost")
      .body("1000")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
  }

  // Wait for the alarm to be dispatched
  tokio::time::sleep(std::time::Duration::from_secs(3)).await;

  // Get alarm count, should be 1 now
  {
    let res = client
      .get(&alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"count":1}"#);
  }
}
