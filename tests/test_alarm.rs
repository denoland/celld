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
  tokio::time::sleep(std::time::Duration::from_secs(3)).await;

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
