use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::{combinators::BoxBody, Full};
use hyper::server::conn::http1;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::fs;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

struct DenoProcess {
  process: Child,
  app_name: String,
}

async fn handle_request(
  req: Request<hyper::body::Incoming>,
  deploy_data_dir: PathBuf,
  processes: Arc<Mutex<HashMap<String, DenoProcess>>>,
) -> Result<Response<BoxBody<Bytes, BoxError>>, hyper::Error> {
  match proxy_service(req, deploy_data_dir, processes).await {
    Ok(response) => Ok(response),
    Err(e) => {
      eprintln!("Error handling request: {}", e);
      let body =
        Full::new(Bytes::from(format!("Internal server error: {}", e)))
          .map_err(|never| match never {})
          .boxed();

      Ok(
        Response::builder()
          .status(StatusCode::INTERNAL_SERVER_ERROR)
          .body(body)
          .unwrap(),
      )
    }
  }
}

async fn proxy_service(
  req: Request<hyper::body::Incoming>,
  deploy_data_dir: PathBuf,
  processes: Arc<Mutex<HashMap<String, DenoProcess>>>,
) -> Result<Response<BoxBody<Bytes, BoxError>>, BoxError> {
  let uri = req.uri();

  println!("Received request: {} {}", req.method(), uri.path());

  // Check for path-based invocations
  if uri.path().starts_with("/run/") {
    let parts: Vec<&str> = uri.path().split('/').collect();
    if parts.len() >= 3 {
      let plugin_name = parts[2];
      println!("Running plugin: {}", plugin_name);

      // Check if plugin exists in deploy_data directory
      let plugin_path = deploy_data_dir.join(plugin_name);
      if !plugin_path.exists() {
        return Ok(response_not_found(format!(
          "Plugin '{}' not found",
          plugin_name
        )));
      }

      // Check if we have a Deno process for this plugin
      let mut processes_map = processes.lock().unwrap();

      if !processes_map.contains_key(plugin_name) {
        // Start a new Deno process
        let plugin_code_path = plugin_path.join("code").join("main.ts");
        if !plugin_code_path.exists() {
          return Ok(response_not_found(format!(
            "Plugin code not found for '{}'",
            plugin_name
          )));
        }

        println!("Starting Deno process for plugin: {}", plugin_name);

        // Start Deno subprocess using Deno Deploy format
        let deno_process = Command::new("deno")
          .arg("run")
          .arg("--allow-net")
          .arg("--allow-read")
          .arg("--unstable")
          .arg("--no-check")
          .arg("https://deno.land/x/deploy/deployctl.ts")
          .arg("run")
          .arg("--no-check")
          .arg("--watch=false")
          .arg(plugin_code_path.to_str().unwrap())
          .stdin(Stdio::null())
          .stdout(Stdio::piped())
          .stderr(Stdio::piped())
          .spawn();

        match deno_process {
          Ok(process) => {
            processes_map.insert(
              plugin_name.to_string(),
              DenoProcess {
                process,
                app_name: plugin_name.to_string(),
              },
            );
            println!("Deno process started for plugin: {}", plugin_name);
          }
          Err(e) => {
            return Ok(response_error(format!(
              "Failed to start Deno process: {}",
              e
            )));
          }
        }
      }

      // For this simple version, just return success that the plugin is running
      return Ok(response_success(format!(
        "Plugin '{}' is running",
        plugin_name
      )));
    }
  }

  // If no plugin was specified or path doesn't match expected format
  Ok(response_not_found("Invalid request path".to_string()))
}

fn response_success(message: String) -> Response<BoxBody<Bytes, BoxError>> {
  let body = Full::new(Bytes::from(message))
    .map_err(|never| match never {})
    .boxed();

  Response::builder()
    .status(StatusCode::OK)
    .header("Content-Type", "text/plain")
    .body(body)
    .unwrap()
}

fn response_not_found(message: String) -> Response<BoxBody<Bytes, BoxError>> {
  let body = Full::new(Bytes::from(message))
    .map_err(|never| match never {})
    .boxed();

  Response::builder()
    .status(StatusCode::NOT_FOUND)
    .header("Content-Type", "text/plain")
    .body(body)
    .unwrap()
}

fn response_error(message: String) -> Response<BoxBody<Bytes, BoxError>> {
  let body = Full::new(Bytes::from(message))
    .map_err(|never| match never {})
    .boxed();

  Response::builder()
    .status(StatusCode::INTERNAL_SERVER_ERROR)
    .header("Content-Type", "text/plain")
    .body(body)
    .unwrap()
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
  let deploy_data_dir =
    env::var("DEPLOY_DATA").unwrap_or_else(|_| "./deploy_data".to_string());
  let deploy_data_path = PathBuf::from(&deploy_data_dir);

  // Create deploy_data directory if it doesn't exist
  if !deploy_data_path.exists() {
    println!("Creating deploy_data directory: {}", deploy_data_dir);
    fs::create_dir_all(&deploy_data_path).await?;
  }

  // Create a hashmap to store running Deno processes
  let processes: Arc<Mutex<HashMap<String, DenoProcess>>> =
    Arc::new(Mutex::new(HashMap::new()));

  let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
  let listener = TcpListener::bind(addr).await?;

  println!("Proxy server listening on {}", addr);
  println!(
    "Using deploy_data directory: {}",
    deploy_data_path.display()
  );

  // Accept and process each connection
  loop {
    let (tcp_stream, _) = listener.accept().await?;
    let io = TokioIo::new(tcp_stream);
    let deploy_data_path_clone = deploy_data_path.clone();
    let processes_clone = processes.clone();

    // Handle the connection in a new task
    tokio::spawn(async move {
      let service = hyper::service::service_fn(move |req| {
        handle_request(
          req,
          deploy_data_path_clone.clone(),
          processes_clone.clone(),
        )
      });

      if let Err(err) =
        http1::Builder::new().serve_connection(io, service).await
      {
        eprintln!("Error serving connection: {:?}", err);
      }
    });
  }
}
