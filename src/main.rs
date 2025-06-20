mod active_connections;
mod alarm_processor;
mod alarm_scheduler;
mod benchmark_deno_startup;
mod cell_manager;
mod child_on_parent_exit;
mod cluster_membership;
mod config;
mod consistent_hash;
mod control_socket_listener;
mod distributed_lock;
mod heartbeat_service;
mod node_state;
mod peer_manager;
mod process_reaper;
mod router;
mod sqlite_replica;
#[cfg(test)]
pub mod test_utils;

use clap::{Arg, Command};
use pingora::prelude::*;
use pingora::server::configuration::ServerConf;
use pingora::services::background::background_service;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use node_state::NodeState;
use process_reaper::ProcessReaper;
use router::{InternalAPI, Proxy};

// Default values, can be overridden when creating ProcessManager
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_REAPER_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
struct Args {
  src_file: Option<PathBuf>,
  static_dir: Option<PathBuf>,
  env_file: Option<PathBuf>,
}

fn create_command() -> Command {
  Command::new("celld")
    .version("0.1.0")
    .about("Deno Cells - Simple, Stateful, Scalable Compute Units")
    .long_about(include_str!("help.txt"))
    .arg(
      Arg::new("src_file")
        .help("source file for default tenant (enables single-tenant mode)")
        .value_name("SRC_FILE")
        .index(1),
    )
    .arg(
      Arg::new("static_dir")
        .help("static files directory for default tenant")
        .value_name("STATIC_DIR")
        .index(2),
    )
    .arg(
      Arg::new("env_file")
        .long("env-file")
        .help(
          "Load environment variables from this file for single-tenant mode",
        )
        .value_name("PATH")
        .value_parser(clap::value_parser!(PathBuf)),
    )
    .arg(
      Arg::new("static_fallback")
        .long("static-fallback")
        .value_name("STRATEGY")
        .help(
          r#"Static file fallback strategy for missing files:
  strict              Return 404 (default)
  spa[:<FILE>]        Serve <FILE> for SPA routes (default: index.html)
  custom404[:<FILE>]  Serve custom 404 page (default: 404.html)"#,
        )
        .required(false),
    )
}

/// Starts the server with the given configuration
/// Returns the server instance
fn start_server(config: config::Config) -> Server {
  // Create a server configuration
  let mut pingora_config = ServerConf::new().unwrap();
  //pingora_config.graceful_shutdown_timeout_seconds = Some(1);
  pingora_config.grace_period_seconds =
    std::env::var("CELL_GRACE_PERIOD_SECONDS")
      .ok()
      .and_then(|s| s.parse().ok());

  // not sure why we need this...
  let pingora_config2 = Arc::new(ServerConf::new().unwrap());

  let mut server = Server::new_with_opt_and_conf(None, pingora_config);

  // Create NodeState from configuration
  let node_state = match NodeState::new(config) {
    Ok(state) => state,
    Err(e) => {
      error!("Failed to initialize node state: {}", e);
      std::process::exit(1);
    }
  };

  // Create the proxy app that will handle routing
  let app = Proxy {
    node_state: node_state.clone(),
  };

  // Create an HTTP proxy service with our app
  let mut proxy_service = http_proxy_service(&pingora_config2, app);
  proxy_service.threads = Some(1);

  // Configure the proxy service to listen on the specified address
  proxy_service.add_tcp(&node_state.config.listen_addr.to_string());

  // Create the internal API handler
  let internal_api = InternalAPI {
    node_state: node_state.clone(),
  };

  // Create an HTTP service for the internal API
  let mut internal_service = http_proxy_service(&pingora_config2, internal_api);
  internal_service.threads = Some(1);

  // Configure the internal service to listen on the internal address
  internal_service.add_tcp(&node_state.config.internal_listen_addr.to_string());

  server.add_service({
    let mut s = background_service(
      "process_reaper",
      ProcessReaper::new(
        node_state.clone(),
        DEFAULT_IDLE_TIMEOUT,
        DEFAULT_REAPER_INTERVAL,
      ),
    );
    s.threads = Some(1);
    s
  });

  // Add a background service for S3 heartbeat and peer discovery
  server.add_service({
    let mut s = background_service(
      "s3_heartbeat",
      heartbeat_service::HeartbeatService {
        node_state: node_state.clone(),
        interval: node_state.config.heartbeat_interval,
        staleness_threshold: node_state.config.staleness_threshold,
      },
    );
    s.threads = Some(1);
    s
  });

  // Add a background service for alarm scheduler
  server.add_service({
    let mut s = background_service(
      "alarm_scheduler",
      alarm_scheduler::AlarmScheduler {
        node_state: node_state.clone(),
        interval: node_state.config.alarm_scheduler_interval,
      },
    );
    s.threads = Some(1);
    s
  });

  // Add a background service for control socket listener
  server.add_service({
    let mut s = background_service(
      "control_socket_listener",
      control_socket_listener::ControlSocketListener {
        node_state: node_state.clone(),
      },
    );
    s.threads = Some(1);
    s
  });

  // Add the public proxy service to the server
  server.add_service(proxy_service);

  // Add the internal service to the server
  server.add_service(internal_service);

  debug!(
    "Starting Deno Deploy proxy server on {} (public) and {} (internal)",
    node_state.config.listen_addr, node_state.config.internal_listen_addr
  );
  server
}

fn main() {
  // tracing_subscriber::fmt::init();

  use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

  tracing_subscriber::registry()
    .with(console_subscriber::spawn())
    .with(tracing_subscriber::fmt::layer())
    .init();

  // see benchmark_deno_startup.sh
  if std::env::var("BENCHMARK_DENO_STARTUP").is_ok() {
    let iterations = 100;
    info!(
      "Running Deno startup time benchmark (iterations: {})...",
      iterations
    );

    // Create a tokio runtime for the benchmark
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
      benchmark_deno_startup::run(iterations).await.unwrap();
    });
    return;
  }

  // Parse CLI arguments
  let matches = create_command().get_matches();

  let args = Args {
    src_file: matches.get_one::<String>("src_file").map(|s| {
      let path = PathBuf::from(s);
      // Convert to absolute path to avoid issues with working directory
      std::fs::canonicalize(&path).unwrap_or(path)
    }),
    static_dir: matches.get_one::<String>("static_dir").map(|s| {
      let path = PathBuf::from(s);
      // Convert to absolute path to avoid issues with working directory
      std::fs::canonicalize(&path).unwrap_or(path)
    }),
    env_file: matches.get_one::<PathBuf>("env_file").cloned(),
  };

  // Parse configuration from environment variables
  let mut config = match config::Config::from_env() {
    Ok(config) => config,
    Err(err) => {
      error!("{}", err);
      std::process::exit(1);
    }
  };

  // Parse static fallback strategy if provided
  if let Some(strategy_str) = matches.get_one::<String>("static_fallback") {
    config.static_fallback =
      config::StaticFallbackStrategy::from_str(strategy_str);
  }

  // Configure single-tenant mode if CLI arguments are provided
  if let Some(src_file) = args.src_file {
    if !src_file.exists() {
      error!("Source file does not exist: {}", src_file.display());
      std::process::exit(1);
    }

    // Validate static directory if provided
    if let Some(ref static_dir) = args.static_dir {
      if !static_dir.is_dir() {
        error!("Static directory does not exist: {}", static_dir.display());
        std::process::exit(1);
      }
    }

    // Resolve .env file path if explicitly provided
    let env_file = if let Some(env_file_path) = args.env_file {
      // User provided explicit --env-file path
      if !env_file_path.exists() {
        error!(
          "Environment file does not exist: {}",
          env_file_path.display()
        );
        std::process::exit(1);
      }
      // Canonicalize to get absolute path
      let env_file_abs =
        std::fs::canonicalize(&env_file_path).unwrap_or(env_file_path);
      info!("Using environment file: {}", env_file_abs.display());
      Some(env_file_abs)
    } else {
      None
    };

    config.single_tenant = Some(config::SingleTenantConfig {
      src_file,
      static_dir: args.static_dir,
      env_file,
    });

    info!("Starting server in single-tenant mode");
  } else {
    info!("Starting server with dynamic cluster membership");
  }

  // Start the server with configuration
  let server = start_server(config);
  server.run_forever();
}

#[cfg(test)]
mod tests {
  use super::*;
  use futures_util::{SinkExt, StreamExt};
  use once_cell::sync::Lazy;
  use serde_json::Value;
  use std::time::Duration;
  use tokio_tungstenite::tungstenite::protocol::Message;

  // inspired by https://github.com/cloudflare/pingora/blob/caa6a0/pingora-core/tests/utils/mod.rs
  pub static TEST_SERVER: Lazy<std::thread::JoinHandle<()>> = Lazy::new(|| {
    let _ = tracing_subscriber::fmt::try_init();

    // Ensure we're using a clean environment for the test server
    std::env::set_var("ADVERTISE_ADDR", "127.0.0.1:6146"); // Set advertise address
    std::env::set_var("LISTEN_ADDR", "127.0.0.1:6146"); // Set listen address
    std::env::set_var("INTERNAL_LISTEN_ADDR", "127.0.0.1:6147"); // Set internal address
    std::env::set_var("DATA", "./data"); // Set data directory
    std::env::set_var("CELL_HEARTBEAT_INTERVAL", "2"); // Fast heartbeat for tests
    std::env::set_var("CELL_GRACE_PERIOD_SECONDS", "0");

    let h = std::thread::spawn(|| {
      // Create config from environment variables
      let config = config::Config::from_env().expect("Failed to parse config");
      let server = start_server(config);
      server.run_forever();
    });

    // Give more time for the server to initialize completely
    std::thread::sleep(Duration::from_secs(3));
    h
  });

  pub fn init() {
    let _ = *TEST_SERVER;
  }

  #[test_log::test(tokio::test)]
  async fn test_proxy_with_ephemeral_port() {
    init();

    // Give the server a moment to fully initialize
    tokio::time::sleep(Duration::from_millis(500)).await;

    let response = reqwest::Client::new()
      .get("http://127.0.0.1:6146/cell/foo")
      .header("Host", "hello.localhost")
      .timeout(Duration::from_secs(5)) // Add a timeout to prevent hanging
      .send()
      .await
      .unwrap()
      .text()
      .await
      .unwrap();
    assert_eq!("hello\n", response);
  }

  #[test_log::test(tokio::test)]
  async fn test_static_file_serving() {
    init();

    // Test fetching the index.html file
    for x in ["/", "/index.html"] {
      let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:6146{}", x))
        .header("Host", "hello.localhost")
        .send()
        .await
        .unwrap();
      assert_eq!(response.status(), 200);
      assert_eq!(response.headers().get("content-type").unwrap(), "text/html");
      let content = response.text().await.unwrap();
      assert_eq!(content, "<h1>Hello from hello.localhost</h1>\n");
    }
  }

  #[test_log::test(tokio::test)]
  async fn basic_db() {
    init();

    // Use a unique cell name for this test
    let cell_name = format!(
      "test-db-{}",
      std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
    );

    // Make first request to cell
    let first_response = reqwest::Client::new()
      .get(format!("http://127.0.0.1:6146/cell/{}", cell_name))
      .header("Host", "basic-db.localhost")
      .send()
      .await
      .unwrap()
      .text()
      .await
      .unwrap();
    assert_eq!(first_response.trim(), "1");

    // Verify SQLite database exists and has correct record count
    let db_path = format!("data/basic-db.localhost/sqlite/{}.db", cell_name);
    assert!(std::path::Path::new(&db_path).exists());

    let output = std::process::Command::new("sqlite3")
      .arg(&db_path)
      .arg("SELECT COUNT(*) FROM requests;")
      .output()
      .expect("Failed to execute sqlite3 command");
    let count_str = String::from_utf8_lossy(&output.stdout).to_string();
    let count = count_str.trim();
    assert_eq!(
      count, "1",
      "Database should have 1 record after first request"
    );

    // Make second request to same cell
    let second_response = reqwest::Client::new()
      .get(format!("http://127.0.0.1:6146/cell/{}", cell_name))
      .header("Host", "basic-db.localhost")
      .send()
      .await
      .unwrap()
      .text()
      .await
      .unwrap();
    assert_eq!(second_response.trim(), "2");

    // Database should reflect updated count
    let output = std::process::Command::new("sqlite3")
      .arg(&db_path)
      .arg("SELECT COUNT(*) FROM requests;")
      .output()
      .expect("Failed to execute sqlite3 command");
    let count_str = String::from_utf8_lossy(&output.stdout).to_string();
    let count = count_str.trim();
    assert_eq!(
      count, "2",
      "Database should have 2 records after second request"
    );
  }

  /// Helper function to connect to a WebSocket cell and handle initial messages
  async fn connect_to_cell(
    cell_id: &str,
  ) -> (
    tokio_tungstenite::WebSocketStream<
      tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    String, // username
  ) {
    // Create URL with proper host header in the URL
    let url = format!("ws://ws-echo.localhost:6146/cell/{}", cell_id);

    // Add a small delay before connecting to ensure the server is ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (mut ws_stream, _) = tokio_tungstenite::connect_async(url)
      .await
      .unwrap_or_else(|e| {
        panic!("Failed to connect to cell {}: {}", cell_id, e)
      });

    // Read welcome message
    let welcome_msg = ws_stream.next().await.unwrap().unwrap();
    let welcome_data: Value =
      serde_json::from_str(&welcome_msg.to_string()).unwrap();
    assert_eq!(welcome_data["type"], "welcome");
    let username = welcome_data["username"].as_str().unwrap().to_string();

    // Read userlist message
    let userlist_msg = ws_stream.next().await.unwrap().unwrap();
    let userlist_data: Value =
      serde_json::from_str(&userlist_msg.to_string()).unwrap();
    assert_eq!(userlist_data["type"], "userlist");

    (ws_stream, username)
  }

  #[test_log::test(tokio::test)]
  async fn test_websocket_echo() {
    init();

    // Connect to cell
    let (mut ws_stream, _) = connect_to_cell("test-cell").await;

    // Send a test message
    let test_message = "Hello WebSocket";
    ws_stream
      .send(Message::Text(test_message.to_string().into()))
      .await
      .unwrap();

    // Receive the chat message response
    let chat_msg = ws_stream.next().await.unwrap().unwrap();
    let chat_data: Value = serde_json::from_str(&chat_msg.to_string()).unwrap();

    // Verify message content
    assert_eq!(chat_data["type"], "chat");
    assert_eq!(chat_data["message"], test_message);
    assert!(
      chat_data.get("username").is_some(),
      "Chat message should include username"
    );

    // Clean up
    ws_stream.close(None).await.unwrap();
  }

  #[test_log::test(tokio::test)]
  async fn test_websocket_broadcast() {
    init();

    // Connect first client to the cell
    let (mut client1, username1) = connect_to_cell("broadcast-test").await;

    // Add a small delay to ensure the first client is fully registered
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect second client to the same cell
    let (mut client2, _) = connect_to_cell("broadcast-test").await;

    // Client 1 should receive join notification and updated user list
    for _ in 0..2 {
      let msg = client1.next().await.unwrap().unwrap();
      let data: Value = serde_json::from_str(&msg.to_string()).unwrap();
      match data["type"].as_str().unwrap() {
        "system" => assert!(data["message"]
          .as_str()
          .unwrap()
          .contains("has joined the cell")),
        "userlist" => {
          let users = data["users"].as_array().unwrap();
          assert_eq!(users.len(), 2, "Should have 2 users in the cell");
        }
        _ => {}
      }
    }

    // Send a chat message from client 1
    let chat_message = "Hello everyone!";
    client1
      .send(Message::Text(chat_message.to_string().into()))
      .await
      .unwrap();

    // Both clients should receive the message
    let msg1 = client1.next().await.unwrap().unwrap();
    let msg_data1: Value = serde_json::from_str(&msg1.to_string()).unwrap();
    assert_eq!(msg_data1["type"], "chat");
    assert_eq!(msg_data1["message"], chat_message);

    let msg2 = client2.next().await.unwrap().unwrap();
    let msg_data2: Value = serde_json::from_str(&msg2.to_string()).unwrap();
    assert_eq!(msg_data2["type"], "chat");
    assert_eq!(msg_data2["message"], chat_message);
    assert_eq!(msg_data2["username"], username1);

    client1.close(None).await.unwrap();
    client2.close(None).await.unwrap();
  }

  #[test_log::test(tokio::test)]
  async fn test_separate_isolates_per_cell() {
    init();

    // Connect to two different cells
    let (mut client1, username1) = connect_to_cell("cell-1").await;
    let (mut client2, username2) = connect_to_cell("cell-2").await;

    // Send a message in cell-1
    let message_cell1 = "This message should only be in cell-1";
    client1
      .send(Message::Text(message_cell1.to_string().into()))
      .await
      .unwrap();

    // Client in cell-1 should receive the message
    let msg1 = client1.next().await.unwrap().unwrap();
    let msg_data1: Value = serde_json::from_str(&msg1.to_string()).unwrap();
    assert_eq!(msg_data1["type"], "chat");
    assert_eq!(msg_data1["message"], message_cell1);
    assert_eq!(msg_data1["username"], username1);

    // Send a message in cell-2
    let message_cell2 = "This message should only be in cell-2";
    client2
      .send(Message::Text(message_cell2.to_string().into()))
      .await
      .unwrap();

    // Client in cell-2 should receive the message
    let msg2 = client2.next().await.unwrap().unwrap();
    let msg_data2: Value = serde_json::from_str(&msg2.to_string()).unwrap();
    assert_eq!(msg_data2["type"], "chat");
    assert_eq!(msg_data2["message"], message_cell2);
    assert_eq!(msg_data2["username"], username2);

    // Verify isolation: cell-1 should not receive messages sent to cell-2
    let timeout_duration = Duration::from_millis(300);
    tokio::select! {
      maybe_msg = tokio::time::timeout(timeout_duration, client1.next()) => {
        if let Ok(Some(Ok(_))) = maybe_msg {
          panic!("Cell isolation failure: cell-1 received a message from cell-2");
        }
      }
      _ = tokio::time::sleep(timeout_duration) => {
        // Expected case: timeout without receiving cross-cell message
      }
    }

    client1.close(None).await.unwrap();
    client2.close(None).await.unwrap();
  }

  #[test_log::test(tokio::test)]
  async fn env_test() {
    init();
    let response = reqwest::Client::new()
      .get("http://127.0.0.1:6146/cell/test-cell")
      .header("Host", "env-test.localhost")
      .send()
      .await
      .unwrap();
    assert_eq!(response.status(), 200);
    let env_vars: Value = response.json().await.unwrap();
    let env_obj = env_vars.as_object().unwrap();
    assert_eq!(env_obj["TEST_ENV_VAR"], "test_value");
    assert_eq!(env_obj["ANOTHER_TEST_VAR"], "another_value");
    assert_eq!(env_obj["X-Cell-Id"], "test-cell");
    assert_eq!(env_obj.len(), 3, "Expected exactly 4 environment variables");
  }

  #[test_log::test(tokio::test)]
  async fn test_default_tenant() {
    init();

    // Test without Host header - should use default tenant
    let response = reqwest::Client::new()
      .get("http://127.0.0.1:6146/cell/test")
      .send()
      .await
      .unwrap()
      .text()
      .await
      .unwrap();
    assert!(response.contains("default tenant"));
  }

  #[test_log::test(tokio::test)]
  async fn test_internal_endpoint_is_not_accessible_to_external_clients() {
    init();

    let cell_id = uuid::Uuid::new_v4().simple().to_string();

    let forbidden_endpoints = [
      "/_internal",
      "/_internal?foo=bar",
      "/_internal/?foo=bar",
      "/_internal/alarm",
      "/_internal/alarm?foo=bar",
      "/_internal/alarm2",
      "/_internal/foo/bar",
      "/_internal/foo/bar",
    ];

    for endpoint in forbidden_endpoints {
      let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:6146/cell/{cell_id}{endpoint}"))
        .header("Host", "hello.localhost")
        .send()
        .await
        .unwrap();
      assert_eq!(response.status(), 403);
      let response_text = response.text().await.unwrap();
      assert_eq!(
        response_text,
        "Requests to internal endpoints are forbidden"
      );
    }

    let allowed_endpoints = [
      "/",
      "/internal",
      "/internal?foo=bar",
      "/internal_?foo=baaar",
      "/_internal_",
      "/_internal2",
      "/_internal2/alarm",
      "/_internal2/alarm?foo=bar",
      "/_internal2/foo/bar",
    ];

    for endpoint in allowed_endpoints {
      let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:6146/cell/{cell_id}{endpoint}"))
        .header("Host", "hello.localhost")
        .send()
        .await
        .unwrap();
      assert_eq!(response.status(), 200);
      let response_text = response.text().await.unwrap();
      assert_eq!(response_text, "hello\n");
    }
  }

  #[test_log::test(tokio::test)]
  async fn test_auto_created_tables() {
    use rusqlite::Connection;
    use std::fs;

    init();

    let get_tables = |db_path: &str| -> Vec<String> {
      if fs::metadata(db_path).is_err() {
        return vec![];
      }
      let conn = Connection::open(db_path).unwrap();
      let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").unwrap();
      stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    };

    // Test hello.localhost
    let c = reqwest::Client::new();
    c.get("http://127.0.0.1:6146/cell/tables")
      .header("Host", "hello.localhost")
      .send()
      .await
      .unwrap();
    let hello_tables = get_tables("data/hello.localhost/sqlite/tables.db");
    assert_eq!(hello_tables, vec!["scheduled_tasks"]);

    // Test basic-db.localhost
    c.get("http://127.0.0.1:6146/cell/tables")
      .header("Host", "basic-db.localhost")
      .send()
      .await
      .unwrap();
    let basic_tables = get_tables("data/basic-db.localhost/sqlite/tables.db");
    assert_eq!(basic_tables, vec!["requests", "scheduled_tasks"]);

    // Test workflow.localhost
    c.get("http://127.0.0.1:6146/cell/tables")
      .header("Host", "workflow.localhost")
      .send()
      .await
      .unwrap();
    let workflow_tables =
      get_tables("data/workflow.localhost/sqlite/tables.db");
    assert_eq!(
      workflow_tables,
      vec![
        "key_values",
        "logs",
        "scheduled_tasks",
        "workflow_runs",
        "workflow_steps",
        "workflows"
      ]
    );
  }
}
