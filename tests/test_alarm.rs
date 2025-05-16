mod common;

use common::TestEnv;

#[tokio::test]
async fn test_alarm_in_single_node_cluster() {
  let test_env = TestEnv::new(1);
  let port = test_env.public_ports[0];

  let cell_id = "test-cell-id";
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);
  let client = reqwest::Client::new();

  let res = client
    .get(url)
    .header("host", "alarm.localhost")
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  let content = res.text().await.unwrap();
  assert_eq!(content, "null");
}
