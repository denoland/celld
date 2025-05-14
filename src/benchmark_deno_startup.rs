use anyhow::Result;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::node_state::NodeState;

/// Benchmark function to measure Deno process coldstart performance
pub async fn run(iterations: usize) -> Result<()> {
  // Create a mock configuration and process manager
  let config = match Config::from_env() {
    Ok(config) => config,
    Err(_) => {
      // Create a minimal default config for benchmark purposes
      Config {
        data_dir: PathBuf::from("./data"),
        listen_addr: "127.0.0.1:8000".to_string(),
        advertise_addr: "127.0.0.1:8000".to_string(),
        internal_listen_addr: "127.0.0.1:8001".to_string(),
        s3_endpoint: None,
        s3_bucket: None,
        s3_region: None,
        s3_path: None,
        s3_access_key_id: None,
        s3_secret_access_key: None,
        heartbeat_interval: Duration::from_secs(30),
        staleness_threshold: Duration::from_secs(90),
        lock_guard_ttl: Duration::from_secs(30),
      }
    }
  };

  // Create minimal node_state for benchmark
  let node_state = NodeState::new_for_benchmark(config.clone());

  // Record the startup time for each iteration
  let mut results = Vec::with_capacity(iterations);

  // The host and cell to use for testing
  let host = "hello.localhost";
  let cell_id = "benchmark";

  // Perform the benchmark
  println!(
    "Starting Deno coldstart benchmark with {} iterations...",
    iterations
  );
  for i in 0..iterations {
    // Start timing
    let start = Instant::now();

    // Use single_use isolate to ensure a new process each time

    let process_key =
      crate::process_manager::ProcessKey::new_single_use(host, cell_id);
    let (_socket_path, _stream) = node_state
      .clone()
      .process_manager
      .get_or_spawn_process(host, cell_id, &process_key, node_state.clone())
      .await?;
    let elapsed = start.elapsed();

    // Record the time
    results.push(elapsed);

    if i % 10 == 0 {
      println!("Iteration {}: {} ms", i, elapsed.as_millis());
    }

    // Wait a bit to prevent resource exhaustion
    tokio::time::sleep(Duration::from_millis(50)).await;
  }

  // Calculate statistics
  if !results.is_empty() {
    // Sort for percentiles
    let mut sorted_results = results.clone();
    sorted_results.sort();

    // Calculate percentiles
    let p50 = sorted_results[results.len() / 2];
    let p90 = sorted_results[results.len() * 9 / 10];
    let p99 = sorted_results[results.len() * 99 / 100];

    // Calculate min/max/avg
    let min = *sorted_results.first().unwrap();
    let max = *sorted_results.last().unwrap();
    let total: Duration = results.iter().sum();
    let avg = total / results.len() as u32;

    println!("\nDeno Startup Statistics:");
    println!("Min: {} ms", min.as_millis());
    println!("Max: {} ms", max.as_millis());
    println!("Avg: {} ms", avg.as_millis());
    println!("p50: {} ms", p50.as_millis());
    println!("p90: {} ms", p90.as_millis());
    println!("p99: {} ms", p99.as_millis());
  }

  node_state.process_manager.terminate_all().await;

  Ok(())
}
