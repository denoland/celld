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
async fn steps_completed_at() {
  let test_env = TestEnv::new(1);
  let port = test_env.ports[0].public();
  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  // Dispatch the reliable workflow
  let run_id = dispatch_workflow(
    &client,
    &url,
    "reliable",
    json!({
      "username": "testuser",
      "email": "test@example.com",
      "phoneNumber": "+1234567890"
    }),
  )
  .await;

  // Wait for workflow completion
  assert!(
    wait_for_workflow_completion(&client, &url, &run_id, 10).await,
    "Workflow should complete"
  );

  // Get and verify workflow runs
  let run_rows = get_workflow_runs_from_db(&test_env, &cell_id);
  assert_eq!(run_rows.len(), 1, "Should have 1 workflow run");

  // Verify the workflow run has completed_at set
  let (run_id_db, workflow_name, output_data, completed_at) = &run_rows[0];
  assert_eq!(run_id_db, &run_id);
  assert_eq!(workflow_name, "reliable");
  assert!(
    output_data.is_some(),
    "Workflow run should have output_data"
  );
  assert!(
    completed_at.is_some(),
    "completed_at should be set for workflow run '{}' but got None",
    run_id
  );

  // Get and verify workflow steps
  let step_rows =
    get_workflow_steps_from_db(&test_env, &cell_id, Some(&run_id));
  assert_eq!(step_rows.len(), 2, "Should have 2 workflow steps");

  // Check first step (send-email)
  let (_, step_idx, step_name, step_type, completed_at) = &step_rows[0];
  assert_eq!(*step_idx, 1);
  assert_eq!(step_name, "send-email");
  assert_eq!(step_type, "run");
  assert!(
    completed_at.is_some(),
    "completed_at should be set for step 'send-email' but got None"
  );

  // Check second step (send-sms)
  let (_, step_idx, step_name, step_type, completed_at) = &step_rows[1];
  assert_eq!(*step_idx, 2);
  assert_eq!(step_name, "send-sms");
  assert_eq!(step_type, "run");
  assert!(
    completed_at.is_some(),
    "completed_at should be set for step 'send-sms' but got None"
  );
}

#[test_log::test(tokio::test)]
async fn invoke_and_sleep_steps_completed_at() {
  let test_env = TestEnv::new(1);
  let port = test_env.ports[0].public();
  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  // Test invoke workflow
  let _run_id_parent =
    dispatch_workflow(&client, &url, "parent", json!({ "value": 10 })).await;

  assert!(
    wait_for_workflow_completion(&client, &url, &_run_id_parent, 20).await,
    "Parent workflow should complete"
  );

  // Test sleep workflow
  let _run_id_sleep = dispatch_workflow(
    &client,
    &url,
    "sleep",
    json!({ "sleepDurationMs": 1000 }),
  )
  .await;

  assert!(
    wait_for_workflow_completion(&client, &url, &_run_id_sleep, 10).await,
    "Sleep workflow should complete"
  );

  // Get and verify all workflow runs
  let run_rows = get_workflow_runs_from_db(&test_env, &cell_id);

  // Verify all workflow runs have completed_at set
  for (run_id_db, workflow_name, output_data, completed_at) in &run_rows {
    assert!(
      completed_at.is_some(),
      "completed_at should be set for workflow run '{}' (name: {}) but got None",
      run_id_db, workflow_name
    );
    assert!(
      output_data.is_some(),
      "output_data should be set for completed workflow run '{}' (name: {})",
      run_id_db,
      workflow_name
    );
  }

  // Get all workflow steps and verify completed_at
  let step_rows = get_workflow_steps_from_db(&test_env, &cell_id, None);

  // Verify all steps have completed_at set
  for (run_id_, step_idx, step_name, step_type, completed_at) in &step_rows {
    assert!(
      completed_at.is_some(),
      "completed_at should be set for step '{}' (type: {}, run_id: {}, step_idx: {}) but got None",
      step_name, step_type, run_id_, step_idx
    );
  }
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

fn get_workflow_steps_from_db(
  test_env: &TestEnv,
  cell_id: &str,
  run_id_filter: Option<&str>,
) -> Vec<(String, i32, String, String, Option<String>)> {
  let server_dir = &test_env.server_dirs[0];
  let db_path = server_dir
    .path()
    .join("data")
    .join("workflow.localhost")
    .join("sqlite")
    .join(format!("{}.db", cell_id));

  let conn =
    rusqlite::Connection::open(&db_path).expect("Failed to open database");

  let (sql, params): (String, Vec<String>) = if let Some(run_id) = run_id_filter
  {
    (
      "SELECT workflow_run_id, step_index, name, step_type, completed_at
       FROM workflow_steps
       WHERE workflow_run_id = ?
       ORDER BY step_index"
        .to_string(),
      vec![run_id.to_string()],
    )
  } else {
    (
      "SELECT workflow_run_id, step_index, name, step_type, completed_at
       FROM workflow_steps
       ORDER BY workflow_run_id, step_index"
        .to_string(),
      vec![],
    )
  };

  let mut stmt = conn.prepare(&sql).expect("Failed to prepare statement");

  stmt
    .query_map(rusqlite::params_from_iter(params.iter()), |row| {
      Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
      ))
    })
    .expect("Failed to query")
    .collect::<Result<Vec<_>, _>>()
    .expect("Failed to collect results")
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

fn get_workflow_runs_from_db(
  test_env: &TestEnv,
  cell_id: &str,
) -> Vec<(String, String, Option<String>, Option<String>)> {
  let server_dir = &test_env.server_dirs[0];
  let db_path = server_dir
    .path()
    .join("data")
    .join("workflow.localhost")
    .join("sqlite")
    .join(format!("{}.db", cell_id));

  let conn =
    rusqlite::Connection::open(&db_path).expect("Failed to open database");

  let mut stmt = conn
    .prepare(
      "SELECT id, workflow_name, output_data, completed_at
       FROM workflow_runs
       ORDER BY dispatched_at",
    )
    .expect("Failed to prepare statement");

  stmt
    .query_map([], |row| {
      Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    })
    .expect("Failed to query")
    .collect::<Result<Vec<_>, _>>()
    .expect("Failed to collect results")
}
