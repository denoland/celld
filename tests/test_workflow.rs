mod common;

use common::TestEnv;
use serde_json::json;

#[test_log::test(tokio::test)]
async fn test_reliable_workflow_in_single_node_cluster() {
  let test_env = TestEnv::new(1);
  let port = test_env.public_ports[0];

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
