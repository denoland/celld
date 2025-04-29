use anyhow::Result;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::process_manager::ProcessManager;

/// Benchmark function to measure Deno process coldstart performance
pub async fn benchmark_deno_coldstart(iterations: usize) -> Result<()> {
  // Get current working directory
  let current_dir = std::env::current_dir()?;

  // Setup the process manager with the same data directory
  let data_dir = current_dir.join("data");
  let process_manager = ProcessManager::new(data_dir.clone());

  // Create a minimal config for benchmarking
  let config = Config {
    listen_addr: "127.0.0.1:3000".to_string(),
    advertise_addr: "127.0.0.1:3000".to_string(),
    data_dir: data_dir.clone(),
    heartbeat_interval: Duration::from_secs(30),
    s3_endpoint: None,
    s3_bucket: None,
    s3_region: None,
    s3_prefix: None,
    s3_access_key_id: None,
    s3_secret_access_key: None,
  };

  // Record the startup time for each iteration
  let mut results = Vec::with_capacity(iterations);

  // The host and room to use for testing
  let host = "hello.localhost";
  let room_id = "benchmark";

  // Perform the benchmark
  println!(
    "Starting Deno coldstart benchmark with {} iterations...",
    iterations
  );
  for i in 0..iterations {
    // Start timing
    let start = Instant::now();

    // Use single_use isolate to ensure a new process each time
    let (_socket_path, _stream) = process_manager
      .get_or_spawn_process(host, room_id, true, &config)
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

    println!("\nDeno Coldstart Statistics:");
    println!("Min: {} ms", min.as_millis());
    println!("Max: {} ms", max.as_millis());
    println!("Avg: {} ms", avg.as_millis());
    println!("p50: {} ms", p50.as_millis());
    println!("p90: {} ms", p90.as_millis());
    println!("p99: {} ms", p99.as_millis());
  }

  process_manager.kill_all().await;

  Ok(())
}
