mod common;

use common::TestEnv;

#[test_log::test(tokio::test)]
async fn test_hono() {
  let test_env = TestEnv::new(1);
  let port = test_env.ports[0].public();

  let cell_id = uuid::Uuid::new_v4().simple().to_string();
  let url = format!("http://localhost:{}/cell/{}", port, cell_id);

  let client = reqwest::Client::new();

  let res = client
    .get(format!("{url}/"))
    .header("host", "hono.localhost")
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);
  assert_eq!(res.text().await.unwrap(), "hello from hono");

  let res = client
    .get(format!("{url}/boom"))
    .header("host", "hono.localhost")
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 500);
  assert_eq!(res.text().await.unwrap(), "Internal Server Error from hono");

  let res = client
    .get(format!("{url}/no-exist"))
    .header("host", "hono.localhost")
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 404);
  assert_eq!(res.text().await.unwrap(), "Not Found from hono");

  let res = client
    .get(format!("{url}/errors"))
    .header("host", "hono.localhost")
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  #[derive(serde::Deserialize)]
  struct Error {
    error: String,
  }

  let errors = res.json::<Vec<Error>>().await.unwrap();
  assert_eq!(errors.len(), 1);
  assert!(errors[0].error.starts_with("Error: Boom\n"));

  let res = client
    .get(format!("{url}/logs"))
    .header("host", "hono.localhost")
    .send()
    .await
    .unwrap();
  assert_eq!(res.status(), 200);

  #[derive(serde::Deserialize)]
  struct Log {
    method: String,
    path: String,
    #[allow(dead_code)]
    user_agent: String,
    status: u16,
    #[allow(dead_code)]
    timestamp: String,
  }

  let logs = res.json::<Vec<Log>>().await.unwrap();
  assert_eq!(logs.len(), 4);

  assert_eq!(logs[0].method, "GET");
  assert_eq!(logs[0].path, "/errors");
  assert_eq!(logs[0].status, 200);

  assert_eq!(logs[1].method, "GET");
  assert_eq!(logs[1].path, "/no-exist");
  assert_eq!(logs[1].status, 404);

  assert_eq!(logs[2].method, "GET");
  assert_eq!(logs[2].path, "/boom");
  assert_eq!(logs[2].status, 500);

  assert_eq!(logs[3].method, "GET");
  assert_eq!(logs[3].path, "/");
  assert_eq!(logs[3].status, 200);
}
