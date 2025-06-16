mod common;

use common::TestEnv;
use serde_json::json;
use tracing::{debug, info};

#[test_log::test(tokio::test)]
async fn test_reliable_workflow_in_single_node_cluster() {
  let test_env = TestEnv::new(1).await;
  let port = test_env.ports[0].public();

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  // Make a request to POST /reliable to dispatch the workflow
  let run_id = {
    let res = client
      .post(format!("{}/reliable", url))
      .header("host", "workflow.localhost")
      .json(&json!({
        "username": "magurotuna",
        "email": "magurotuna@example.com",
        "phoneNumber": "+1234567890"
      }))
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    res.text().await.unwrap()
  };

  // Get run progress until it's completed
  let mut completed = false;
  for _ in 0..10 {
    let res = client
      .get(format!("{}/run-progress", url))
      .header("host", "workflow.localhost")
      .query(&[("id", &run_id)])
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.json::<serde_json::Value>().await.unwrap();
    assert_eq!(content["workflowName"], "reliable");
    if !content["completedAt"].is_null() {
      completed = true;
      break;
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
  }
  assert!(completed);

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
  let test_env = TestEnv::new(1).await;
  let port = test_env.ports[0].public();

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  // Make a request to POST /flaky to dispatch the workflow
  let run_id = {
    let res = client
      .post(format!("{}/flaky", url))
      .header("host", "workflow.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    res.text().await.unwrap()
  };

  // Get run progress until the first step is completed
  let mut generated_random_number = None;
  for _ in 0..10 {
    let res = client
      .get(format!("{}/run-progress", url))
      .header("host", "workflow.localhost")
      .query(&[("id", &run_id)])
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.json::<serde_json::Value>().await.unwrap();
    assert_eq!(content["workflowName"], "flaky");
    if content["steps"].as_array().unwrap().len() == 1 {
      assert_eq!(content["steps"][0]["name"], "generate-random-number");
      generated_random_number =
        Some(content["steps"][0]["outputData"].as_u64().unwrap());
      break;
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
  }
  let generated_random_number = generated_random_number.unwrap();

  // Set `flaky: 1` to the key_values table to unblock the workflow
  let res = client
    .post(format!("{}/kv", url))
    .header("host", "workflow.localhost")
    .json(&json!({ "key": "flaky", "value": 1 }))
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  // Now the workflow should be able to make progress. Wait until its completion
  let mut last_step_output = None;
  for _ in 0..10 {
    let res = client
      .get(format!("{}/run-progress", url))
      .header("host", "workflow.localhost")
      .query(&[("id", &run_id)])
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.json::<serde_json::Value>().await.unwrap();
    assert_eq!(content["workflowName"], "flaky");
    if content["steps"].as_array().unwrap().len() == 3 {
      assert_eq!(content["steps"][2]["name"], "multiply-random-number-by-2");
      last_step_output =
        Some(content["steps"][2]["outputData"].as_u64().unwrap());
      break;
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
  }
  let last_step_output = last_step_output.unwrap();

  // The last step should return the result of multiplying the memoized random
  // number by 2
  assert_eq!(last_step_output, generated_random_number * 2);
}

#[test_log::test(tokio::test)]
async fn test_workflow_automatic_resume_after_node_failure() {
  let mut test_env = TestEnv::new(3).await;

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

  // Make a request to POST /flaky to dispatch the workflow
  let run_id = {
    let res = client
      .post(format!("{}/flaky", primary_url))
      .header("host", "workflow.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    res.text().await.unwrap()
  };

  // Get run progress until the first step is completed
  let mut generated_random_number = None;
  for _ in 0..10 {
    let res = client
      .get(format!("{}/run-progress", primary_url))
      .header("host", "workflow.localhost")
      .query(&[("id", &run_id)])
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.json::<serde_json::Value>().await.unwrap();
    assert_eq!(content["workflowName"], "flaky");
    if content["steps"].as_array().unwrap().len() == 1 {
      assert_eq!(content["steps"][0]["name"], "generate-random-number");
      generated_random_number =
        Some(content["steps"][0]["outputData"].as_u64().unwrap());
      break;
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
  }
  let generated_random_number = generated_random_number.unwrap();

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

  // Now the dispatched workflow run should be automatically resumed by another
  // node. Unblock the workflow by setting `flaky: 1` to the key_values table
  let res = client
    .post(format!("{}/kv", secondary_url))
    .header("host", "workflow.localhost")
    .json(&json!({ "key": "flaky", "value": 1 }))
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  // Wait for the workflow to be resumed (the resume is scheduled 10s after the
  // primary owner is gracefully shutdown)
  tokio::time::sleep(std::time::Duration::from_secs(10)).await;

  // Now the workflow should be able to make progress. Wait until its completion
  let mut last_step_output = None;
  for _ in 0..10 {
    let res = client
      .get(format!("{}/run-progress", secondary_url))
      .header("host", "workflow.localhost")
      .query(&[("id", &run_id)])
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.json::<serde_json::Value>().await.unwrap();
    assert_eq!(content["workflowName"], "flaky");
    if content["steps"].as_array().unwrap().len() == 3 {
      assert_eq!(content["steps"][2]["name"], "multiply-random-number-by-2");
      last_step_output =
        Some(content["steps"][2]["outputData"].as_u64().unwrap());
      break;
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
  }
  let last_step_output = last_step_output.unwrap();

  // The last step should return the result of multiplying the memoized random
  // number by 2
  assert_eq!(last_step_output, generated_random_number * 2);
}

#[test_log::test(tokio::test)]
async fn test_invoke_workflow() {
  let test_env = TestEnv::new(1).await;
  let port = test_env.ports[0].public();

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  // Scenario 1: Invoked workflow completes without shutdown
  let run_id_parent = {
    let res = client
      .post(format!("{}/parent", url))
      .header("host", "workflow.localhost")
      .json(&json!({ "value": 10 }))
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);
    res.text().await.unwrap()
  };

  let mut completed = false;
  for _ in 0..20 {
    let res = client
      .get(format!("{}/run-progress", url))
      .header("host", "workflow.localhost")
      .query(&[("id", &run_id_parent)])
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.json::<serde_json::Value>().await.unwrap();
    if !content["completedAt"].is_null() {
      assert_eq!(content["steps"][0]["name"], "invoke:child");
      assert_eq!(content["steps"][0]["outputData"], 15);
      assert_eq!(content["outputData"]["finalResult"], 15);
      completed = true;
      break;
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
  }
  assert!(completed, "Parent workflow did not complete");
}

#[test_log::test(tokio::test)]
async fn test_sleep_workflow() {
  let test_env = TestEnv::new(1).await;
  let port = test_env.ports[0].public();

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  let start_time = std::time::Instant::now();

  // Make a request to POST /sleep to dispatch the sleep workflow
  let run_id = {
    let res = client
      .post(format!("{}/sleep", url))
      .header("host", "workflow.localhost")
      .json(&json!({ "sleepDurationMs": 1000 }))
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    res.text().await.unwrap()
  };

  // Get run progress until it's completed
  let mut completed = false;
  for _ in 0..10 {
    let res = client
      .get(format!("{}/run-progress", url))
      .header("host", "workflow.localhost")
      .query(&[("id", &run_id)])
      .send()
      .await
      .unwrap();
    assert_eq!(res.status(), 200);

    let content = res.json::<serde_json::Value>().await.unwrap();
    assert_eq!(content["workflowName"], "sleep-test");
    if !content["completedAt"].is_null() {
      completed = true;
      assert_eq!(content["outputData"]["message"], "Slept for 1000ms");
      break;
    }
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
  }
  assert!(completed);

  let elapsed = start_time.elapsed();
  // Should take at least 1 second due to the sleep
  assert!(
    elapsed.as_millis() >= 800,
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
