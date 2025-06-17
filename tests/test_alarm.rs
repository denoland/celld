mod common;

use common::TestEnv;

#[test_log::test(tokio::test)]
async fn test_alarm_crud_in_single_node_cluster() {
  let test_env =
    TestEnv::new(1, "test_alarm_crud_in_single_node_cluster").await;
  let port = test_env.ports[0].public();

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
      .query(&[("id", "not-existing-id")])
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, r#"{"deleted":false}"#);
  }

  // Set alarm
  let alarm_id = {
    let res = client
      .post(&url)
      .header("host", "alarm.localhost")
      .body("5000")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    res.text().await.unwrap()
  };

  // Get alarm (should exist)
  {
    let res = client
      .get(&url)
      .query(&[("id", &alarm_id)])
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
      .query(&[("id", &alarm_id)])
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
      .query(&[("id", &alarm_id)])
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, "null");
  }
}

#[test_log::test(tokio::test)]
async fn test_alarm_dispatch_in_single_node_cluster() {
  let test_env =
    TestEnv::new(1, "test_alarm_dispatch_in_single_node_cluster").await;
  let port = test_env.ports[0].public();

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

#[test_log::test(tokio::test)]
async fn test_multiple_cells_alarm_dispatch_in_single_node_cluster() {
  let test_env = TestEnv::new(
    1,
    "test_multiple_cells_alarm_dispatch_in_single_node_cluster",
  )
  .await;
  let port = test_env.ports[0].public();

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

#[test_log::test(tokio::test)]
async fn test_system_main_cell_takeover() {
  let client = reqwest::Client::new();

  let mut test_env = TestEnv::new(2, "test_system_main_cell_takeover").await;

  let mut system_main_cell_index = None;
  let mut secondary_cell_external_ports = Vec::new();

  for (index, port) in test_env.ports.iter().enumerate() {
    let owner_url = format!(
      "http://localhost:{}/_internal/mesh/owner/_system/main",
      port.internal()
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
      system_main_cell_index = Some(index);
    } else {
      secondary_cell_external_ports.push(port.public());
    }
  }

  let system_main_cell_index = system_main_cell_index.unwrap();
  assert!(!secondary_cell_external_ports.is_empty());

  let test_cell_id = uuid::Uuid::new_v4().simple().to_string();

  // Attempt to get an alarm (none should exist)
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      test_env.ports[system_main_cell_index].public(),
      test_cell_id
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

  // Set a new alarm to save something to the system main cell's DB
  let alarm_id = {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      test_env.ports[system_main_cell_index].public(),
      test_cell_id
    );

    let res = client
      .post(&test_cell_url)
      .header("host", "alarm.localhost")
      .body(u32::MAX.to_string()) // Will never be dispatched
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    res.text().await.unwrap()
  };

  // Wait for Litestream to replicate data to S3
  println!("Waiting for Litestream to replicate data to S3...");
  tokio::time::sleep(std::time::Duration::from_secs(5)).await;

  // Shutdown the node that the system main cell belongs to
  test_env.graceful_shutdown_cell_instance(system_main_cell_index);

  // Wait for the shutdown to be detected
  println!("Waiting for primary node failure to be detected...");
  tokio::time::sleep(std::time::Duration::from_secs(8)).await;

  // Get the set alarm through the secondary node to see the system main cell's DB has been restored
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      secondary_cell_external_ports[0], test_cell_id
    );

    let res = client
      .get(&test_cell_url)
      .query(&[("id", &alarm_id)])
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_ne!(content, "null");
  }
}

#[test_log::test(tokio::test)]
async fn test_alarm_crud_operations_forwarded_to_system_main_cell_node() {
  let client = reqwest::Client::new();

  let test_env = TestEnv::new(
    2,
    "test_alarm_crud_operations_forwarded_to_system_main_cell_node",
  )
  .await;

  let mut system_main_cell_index = None;
  struct Port {
    internal: u16,
    external: u16,
  }
  let mut secondary_cell_ports = Vec::new();

  // Identify the node where the system main cell is running
  for (index, port) in test_env.ports.iter().enumerate() {
    let owner_url = format!(
      "http://localhost:{}/_internal/mesh/owner/_system/main",
      port.internal()
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
      system_main_cell_index = Some(index);
    } else {
      secondary_cell_ports.push(Port {
        internal: port.internal(),
        external: port.public(),
      });
    }
  }

  let system_main_cell_index = system_main_cell_index.unwrap();
  assert!(!secondary_cell_ports.is_empty());

  // Find a cell ID that does NOT belong to the system main cell's node
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
  // client -> primary node -> secondary node (where test_cell_id runs) -> isolate (which issues GetAlarm request) -> secondary node -> primary node -> system main cell -> primary node -> secondary node -> isolate
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      test_env.ports[system_main_cell_index].public(),
      test_cell_id
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

  // Set a new alarm to save something to the system main cell's DB
  let alarm_id = {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      test_env.ports[system_main_cell_index].public(),
      test_cell_id
    );

    let res = client
      .post(&test_cell_url)
      .header("host", "alarm.localhost")
      .body(u32::MAX.to_string()) // Will never be dispatched
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    res.text().await.unwrap()
  };

  // Get the alarm through the secondary node to see the system main cell's DB has been updated
  {
    let test_cell_url = format!(
      "http://localhost:{}/cell/{}",
      secondary_cell_ports[0].external, test_cell_id
    );

    let res = client
      .get(&test_cell_url)
      .query(&[("id", &alarm_id)])
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
      .query(&[("id", &alarm_id)])
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
// 1. A primary node (where the system main cell is running)
// 2. A secondary node (where "alarm.localhost/<some-cell-id>" is running)
// This test verifies that the primary node will dispatch an alarm to the secondary node and then alarm.localhost's alarm handler will be triggered.
#[test_log::test(tokio::test)]
async fn test_alarm_dispatch_to_remote_cell_owner() {
  let client = reqwest::Client::new();

  let test_env =
    TestEnv::new(2, "test_alarm_dispatch_to_remote_cell_owner").await;

  let mut system_main_cell_index = None;
  struct Port {
    internal: u16,
    #[allow(dead_code)]
    external: u16,
  }
  let mut secondary_cell_ports = Vec::new();

  // Identify the node where the system main cell is running
  for (index, port) in test_env.ports.iter().enumerate() {
    let owner_url = format!(
      "http://localhost:{}/_internal/mesh/owner/_system/main",
      port.internal()
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
      system_main_cell_index = Some(index);
    } else {
      secondary_cell_ports.push(Port {
        internal: port.internal(),
        external: port.public(),
      });
    }
  }

  let system_main_cell_index = system_main_cell_index.unwrap();
  assert!(!secondary_cell_ports.is_empty());

  // Find a cell ID that does NOT belong to the system main cell's node
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
    test_env.ports[system_main_cell_index].public(),
    test_cell_id
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
  let _alarm_id = {
    let res = client
      .post(&cell_url)
      .header("host", "alarm.localhost")
      .body("1000")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    res.text().await.unwrap()
  };

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

async fn test_multi_alarms_with_delays(delays: &[u32], test_case_name: &str) {
  let test_env = TestEnv::new(1, test_case_name).await;
  let port = test_env.ports[0].public();

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

  // Schedule alarms with the given delays
  let start_time = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap()
    .as_millis();
  println!("Current time: {}ms", start_time);

  for delay in delays {
    let res = client
      .post(&url)
      .header("host", "alarm.localhost")
      .body(delay.to_string())
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    // Print the alarm ID for debugging
    let alarm_id = res.text().await.unwrap();
    println!(
      "Scheduled alarm {} with delay {}ms at time {}",
      alarm_id,
      delay,
      start_time + *delay as u128
    );
  }

  // Wait for all alarms to fire (with buffer time)
  tokio::time::sleep(std::time::Duration::from_secs(3)).await;

  // Check final count - all alarms should have fired
  {
    let res = client
      .get(&alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, format!(r#"{{"count":{}}}"#, delays.len()));
  }
}

#[test_log::test(tokio::test)]
async fn test_multi_alarm_forward() {
  test_multi_alarms_with_delays(&[500, 1000, 1500], "test_multi_alarm_forward")
    .await;
}

#[test_log::test(tokio::test)]
async fn test_multi_alarm_reverse() {
  test_multi_alarms_with_delays(&[1500, 1000, 500], "test_multi_alarm_reverse")
    .await;
}

#[test_log::test(tokio::test)]
async fn test_multi_alarm_sequential() {
  let test_env = TestEnv::new(1, "test_multi_alarm_sequential").await;
  let port = test_env.ports[0].public();

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

  // Schedule and wait for each alarm sequentially
  for i in 1..=3 {
    println!("Scheduling alarm {} with 500ms delay", i);

    let res = client
      .post(&url)
      .header("host", "alarm.localhost")
      .body("500")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let alarm_id = res.text().await.unwrap();
    println!("Scheduled alarm {} with ID {}", i, alarm_id);

    // Wait for this alarm to fire (500ms + buffer for rescheduling)
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Check that alarm count has increased
    let res = client
      .get(&alarm_count_url)
      .header("host", "alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.text().await.unwrap();
    assert_eq!(content, format!(r#"{{"count":{}}}"#, i));
    println!("Confirmed alarm {} fired, count is now {}", i, i);
  }
}

#[test_log::test(tokio::test)]
async fn test_recursive_alarm() {
  let test_env = TestEnv::new(1, "test_recursive_alarm").await;
  let port = test_env.ports[0].public();

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  #[derive(Debug, serde::Deserialize)]
  struct GetResponse {
    count: u32,
  }

  // Get initial alarm count
  {
    let res = client
      .get(&url)
      .header("host", "recursive-alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
    let content = res.json::<GetResponse>().await.unwrap();
    assert_eq!(content.count, 0);
  }

  // Start a recursive alarm
  {
    let res = client
      .post(&url)
      .header("host", "recursive-alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
  }

  // Wait for the alarm to be dispatched some times
  tokio::time::sleep(std::time::Duration::from_secs(2)).await;

  // Get alarm count (which will be compared to later)
  let count = {
    let res = client
      .get(&url)
      .header("host", "recursive-alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
    let content = res.json::<GetResponse>().await.unwrap();
    content.count
  };

  // Wait for the alarm to be dispatched more times
  tokio::time::sleep(std::time::Duration::from_secs(2)).await;

  // Get alarm count, should be greater than the initial count
  {
    let res = client
      .get(&url)
      .header("host", "recursive-alarm.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
    let content = res.json::<GetResponse>().await.unwrap();
    assert!(
      content.count > count,
      "content.count: {}, count: {}",
      content.count,
      count
    );
  }
}
