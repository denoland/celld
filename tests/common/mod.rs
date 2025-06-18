mod captured_subprocess;
#[path = "../../src/consistent_hash.rs"]
mod consistent_hash;
#[path = "../../src/test_utils.rs"]
mod test_utils;

use captured_subprocess::CapturedSubprocess;
use consistent_hash::create_consistent_hash;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;
use tempfile::TempDir;
use test_utils::MinioTestServer;
use tracing::{info, warn};
use uuid::Uuid;

static USED_PORTS: LazyLock<Mutex<HashSet<u16>>> =
  LazyLock::new(|| Mutex::new(HashSet::new()));

#[derive(Debug)]
pub struct PortLease {
  public: u16,
  internal: u16,
}

impl PortLease {
  pub fn public(&self) -> u16 {
    self.public
  }

  pub fn internal(&self) -> u16 {
    self.internal
  }
}

impl Drop for PortLease {
  fn drop(&mut self) {
    let mut lock = USED_PORTS.lock().unwrap();
    lock.remove(&self.public);
    lock.remove(&self.internal);
  }
}

/// Time to wait for servers to be ready after they are started.
/// This is important to give time for S3 registration and peer discovery.
const SERVER_STARTUP_WAIT_SECS: u64 = 2;

/// Interval in seconds between heartbeats that each cell sends.
/// This is also the frequency of cluster membership sync. So to avoid flaky
/// tests, this value should be smaller than [`SERVER_STARTUP_WAIT_SECS`].
const CELL_HEARTBEAT_INTERVAL_SECS: u64 = 1;

pub struct TestEnv {
  /// Celld servers
  servers: Vec<CapturedSubprocess>,
  /// Celld server ports (public and internal)
  pub ports: Vec<PortLease>,
  pub minio_server: MinioTestServer,
  pub test_id: String,
  pub bucket_name: String,
  /// Temporary directories for each server instance's data
  pub server_dirs: Vec<TempDir>,
  /// Environment variables to set for each server instance. Overrides the
  /// default ones if name collides.
  envs: HashMap<String, String>,
  /// The buffer for stdout and stderr logs of cells that were gracefully
  /// shutdown.
  /// The key is the cell name, and the value is a pair of stdout and stderr.
  shutdown_node_logs: HashMap<String, (String, String)>,
}

impl TestEnv {
  // Reserve a block of consecutive free ports
  pub fn allocate_ports(
    base: u16,
    count: usize,
    spacing: u16,
  ) -> Vec<PortLease> {
    let mut lock = USED_PORTS.lock().unwrap();
    let mut allocated = Vec::with_capacity(count);
    let mut next_port = base;

    while allocated.len() < count {
      // Check if this port or its internal port (port+1) are already used
      if !lock.contains(&next_port) && !lock.contains(&(next_port + 1)) {
        // Reserve both the public port and its internal port
        lock.insert(next_port);
        lock.insert(next_port + 1);
        allocated.push(PortLease {
          public: next_port,
          internal: next_port + 1,
        });
      }
      // Move to next port candidate with spacing to avoid conflicts
      next_port += spacing;
    }

    allocated
  }

  /// Find two available ports that will trigger cell relocation for any tenant/cell
  /// when the second node joins, given a specific hashring seed.
  ///
  /// This implementation uses the actual consistent hash algorithm to dynamically
  /// find port pairs that will cause ownership to transfer from node1 to node2.
  #[allow(dead_code)]
  pub fn find_relocation_ports(
    seed: u64,
    tenant: &str,
    cell_id: &str,
  ) -> Option<(PortLease, PortLease)> {
    // Search for available port pairs and test them with the hash ring
    // Use mid-range port numbers to avoid common dev ports (8000, 3000, etc.)
    // and ephemeral port range (32768-65535)
    let base_port_range = 20000..30000;

    for base_port in base_port_range.step_by(50) {
      let (port1, port2) = {
        let mut ports = Self::allocate_ports(base_port, 2, 2);
        assert_eq!(ports.len(), 2);
        let port1 = ports.swap_remove(0);
        let port2 = ports.swap_remove(0);
        (port1, port2)
      };

      // Test if this port combination causes relocation using actual hash ring
      if Self::test_relocation_with_hash_ring(
        &port1, &port2, seed, tenant, cell_id,
      ) {
        return Some((port1, port2));
      }
    }

    None
  }

  /// Test whether adding a second node with port2 causes the specified cell
  /// to relocate from port1 to port2, using the actual hash ring algorithm
  fn test_relocation_with_hash_ring(
    port1: &PortLease,
    port2: &PortLease,
    seed: u64,
    tenant: &str,
    cell_id: &str,
  ) -> bool {
    let addr1: SocketAddr =
      format!("127.0.0.1:{}", port1.public()).parse().unwrap();
    let addr2: SocketAddr =
      format!("127.0.0.1:{}", port2.public()).parse().unwrap();

    let mut ring1 = create_consistent_hash(Some(seed));
    ring1.add(addr1);

    let mut ring2 = create_consistent_hash(Some(seed));
    ring2.add(addr1);
    ring2.add(addr2);

    // Use the same cell hash key format as peer_manager.rs
    let cell_key = Self::cell_hash_key(tenant, cell_id);

    let owner_before = ring1.get(&cell_key).copied().unwrap();
    let owner_after = ring2.get(&cell_key).copied().unwrap();

    // Return true if ownership changed from addr1 to addr2
    owner_before == addr1 && owner_after == addr2
  }

  /// Create the same cell hash key format as peer_manager.rs
  fn cell_hash_key(tenant: &str, cell_id: &str) -> String {
    format!("{}/{}", tenant, cell_id)
  }

  // Start mesh nodes with auto-allocated non-conflicting ports
  pub async fn new(count: usize, test_case_name: &str) -> Self {
    // Start with port 7500 and use spacing of 2 to avoid conflicts with internal ports
    let ports = Self::allocate_ports(7500, count, 2);
    Self::new_with_ports(ports, test_case_name).await
  }

  pub async fn new_with_ports(
    ports: Vec<PortLease>,
    test_case_name: &str,
  ) -> Self {
    Self::new_with_ports_and_envs(ports, test_case_name, &[]).await
  }

  pub async fn new_with_ports_and_envs(
    ports: Vec<PortLease>,
    test_case_name: &str,
    envs: &[(&str, &str)],
  ) -> Self {
    // Start MinIO server for testing with a dynamically assigned port
    let bucket_name = "test-mesh-bucket".to_string();
    let minio_server = MinioTestServer::start();
    minio_server.create_bucket(&bucket_name).unwrap();

    let test_id = Uuid::new_v4().simple().to_string();

    let mut test_env = TestEnv {
      servers: Vec::new(),
      ports: Vec::new(),
      minio_server,
      bucket_name,
      test_id: test_id.to_string(),
      server_dirs: Vec::new(),
      envs: envs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect(),
      shutdown_node_logs: HashMap::new(),
    };

    test_env.spawn_cell_instance(ports, test_case_name).await;

    // Longer delay for peer exchange after TCP connections are ready
    // This is important to give time for S3 registration and peer discovery
    tokio::time::sleep(Duration::from_secs(SERVER_STARTUP_WAIT_SECS)).await;
    info!("All servers are ready now");
    test_env
  }

  /// Kill a server instance by index, shifting the remaining instances to the
  /// left.
  ///
  /// For example, if we have servers [A, B, C] and call this function
  /// with index 1, the servers will become [A, C]. Note that C's index has
  /// changed.
  // TODO: Can we do this without shifting the remaining instances? Maybe by not
  // relying on the index but on the server name or something?
  pub fn kill_cell_instance(&mut self, index: usize) {
    let mut server = self.servers.remove(index);
    let _ = self.ports.remove(index);
    let _tmp_dir = self.server_dirs.remove(index); // Remove and drop TempDir

    let pid = Pid::from_raw(server.child().id() as i32);
    // Use SIGKILL to avoid long graceful shutdown times
    kill(pid, Signal::SIGKILL).unwrap();
    server.child_mut().wait().unwrap();
  }

  #[allow(dead_code)]
  pub fn graceful_shutdown_cell_instance(&mut self, index: usize) {
    let mut server = self.servers.remove(index);
    let port = self.ports.remove(index);
    let _tmp_dir = self.server_dirs.remove(index); // Remove and drop TempDir

    let pid = Pid::from_raw(server.child().id() as i32);
    kill(pid, Signal::SIGTERM).unwrap();
    server.child_mut().wait().unwrap();
    let node_name = format!("celld({})", port.public());
    let (stdout, stderr) = server.dump();
    self.shutdown_node_logs.insert(node_name, (stdout, stderr));
  }

  pub async fn spawn_cell_instance(
    &mut self,
    ports: Vec<PortLease>,
    node_name_prefix: &str,
  ) {
    for (i, port) in ports.into_iter().enumerate() {
      let advertise_addr = format!("127.0.0.1:{}", port.public());
      let internal_addr = format!("127.0.0.1:{}", port.internal());
      let node_name = format!("{}-{}", node_name_prefix, i);

      // Prepare a properly structured temporary directory
      // This creates a temp dir with both sdk/ and data/ to maintain relative imports
      let (temp_dir, data_dir_path) = prepare_test_directory()
        .expect("Failed to prepare temp directory with proper structure");

      let server_cmd = Command::new(env!("CARGO_BIN_EXE_celld"));
      let server_cmd_setup = |cmd: &mut Command| {
        cmd
          .env("RUST_LOG", "info,celld=warn")
          .env("CELL_NODE_NAME", &node_name)
          .env("ADVERTISE_ADDR", &advertise_addr)
          .env("INTERNAL_LISTEN_ADDR", &internal_addr)
          .current_dir(temp_dir.path())
          .env("DATA", &data_dir_path)
          .env(
            "CELL_HEARTBEAT_INTERVAL",
            CELL_HEARTBEAT_INTERVAL_SECS.to_string(),
          )
          .env("CELL_GRACE_PERIOD_SECONDS", "5")
          // Use a shorter staleness threshold for tests to detect failures faster
          .env("CELL_STALENESS_THRESHOLD_SECS", "6")
          .env("CELL_LOCK_GUARD_TTL_SECS", "6")
          .env("CELL_LOCK_GRACEFUL_SHUTDOWN_TIMEOUT_SECS", "2")
          // Configure alarm scheduler to check alarms every second
          .env("CELL_ALARM_SCHEDULER_INTERVAL_SECS", "1")
          .env(
            "CELL_S3_ENDPOINT",
            format!("http://127.0.0.1:{}", self.minio_server.port),
          )
          .env("CELL_S3_BUCKET", &self.bucket_name)
          .env("CELL_S3_REGION", "us-east-1")
          .env("CELL_S3_PREFIX", format!("celld-test-{}", self.test_id))
          .env("CELL_S3_ACCESS_KEY_ID", &self.minio_server.access_key_id)
          .env(
            "CELL_S3_SECRET_ACCESS_KEY",
            &self.minio_server.secret_access_key,
          )
          .env("CELL_DENO_OUTPUT", "1")
          .envs(&self.envs);

        // Propagate the RUST_LOG env var to the celld processes if it exists
        // This overrides the default log level set above.
        // This is useful for setting more verbose logs in CI, which would help
        // us understand what is going on in the tests that may be flaky.
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
          cmd.env("RUST_LOG", rust_log);
        }
      };
      let server = CapturedSubprocess::new(
        format!("celld({})", port.public()),
        server_cmd,
        server_cmd_setup,
      );

      info!(
        "Started server on port {} with ADVERTISE_ADDR={}, S3 mesh, DATA_DIR={:?}",
        port.public(), advertise_addr, data_dir_path
      );

      Self::wait_for_server_ready(&port, &node_name).await;

      self.servers.push(server);
      self.ports.push(port);
      self.server_dirs.push(temp_dir); // Store the TempDir
    }
  }

  // Wait for a server to be ready by checking its health endpoint
  async fn wait_for_server_ready(port: &PortLease, node_name: &str) {
    const MAX_ATTEMPTS: usize = 10;
    const RETRY_DELAY_MS: u64 = 200;

    let client = reqwest::Client::new();

    for p in [port.public(), port.internal()] {
      let mut ok = false;

      for attempt in 1..=MAX_ATTEMPTS {
        if attempt > 1 {
          tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
        }

        let Ok(res) = client
          .get(format!("http://127.0.0.1:{}/_health", p))
          .send()
          .await
        else {
          warn!(
            port = p,
            attempt, "Failed to connect to health check endpoint"
          );
          continue;
        };

        if !res.status().is_success() {
          warn!(
            port = p,
            attempt, "Health check endpoint returned non-success status",
          );
          continue;
        }

        let body = res.text().await.unwrap();
        if !body.contains(node_name) {
          warn!(
            port = p,
            expected_node_name = node_name,
            body,
            attempt,
            "Health check endpoint returned unexpected node name",
          );
          continue;
        }

        ok = true;
        break;
      }

      if !ok {
        panic!(
          "Server on port {} for node {} failed to start",
          p, node_name
        );
      }
    }
  }
}

impl Drop for TestEnv {
  fn drop(&mut self) {
    // Kill all server instances
    for _i in 0..self.servers.len() {
      self.kill_cell_instance(0);
    }

    if std::thread::panicking() {
      for (node_name, (stdout, stderr)) in self.shutdown_node_logs.iter() {
        #[allow(clippy::print_stdout)]
        {
          println!(
            "---- terminated node {} stdout ----\n{}",
            node_name, stdout
          );
        }
        #[allow(clippy::print_stderr)]
        {
          eprintln!(
            "---- terminated node {} stderr ----\n{}",
            node_name, stderr
          );
        }
      }
    }
  }
}

/// Prepares a temporary directory with the correct project structure
/// to ensure relative imports like "../../../sdk/mod.ts" work correctly
///
/// The structure created is:
/// temp_dir/
/// ├── sdk/  (copied from CARGO_MANIFEST_DIR/sdk)
/// └── data/       (copied from CARGO_MANIFEST_DIR/data, skipping sqlite directories)
///
/// Returns the temp directory and the path to the data directory within it
fn prepare_test_directory() -> io::Result<(TempDir, PathBuf)> {
  // Create a new temporary directory
  let temp_dir =
    TempDir::new().expect("Failed to create temp dir for test environment");

  let project_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .canonicalize()
    .expect("Failed to find project root directory");

  // Create paths for source and destination directories
  let src_data_path = project_root.join("data");
  let src_sdk_path = project_root.join("sdk");

  let dst_data_path = temp_dir.path().join("data");
  let dst_jsr_cells_path = temp_dir.path().join("sdk");

  // Verify source directories exist
  if !src_data_path.exists() {
    panic!("Source data directory not found at {:?}", src_data_path);
  }

  if !src_sdk_path.exists() {
    panic!("Source sdk directory not found at {:?}", src_sdk_path);
  }

  // Create data directory in temp dir
  fs::create_dir_all(&dst_data_path)?;

  // Copy sdk directory (needed for relative imports)
  // Note sdk does not contain sqlite directories, but we can reuse the
  // same function.
  copy_directory_without_sqlite(&src_sdk_path, &dst_jsr_cells_path)?;

  // Copy data directory, skipping sqlite directories
  copy_directory_without_sqlite(&src_data_path, &dst_data_path)?;

  Ok((temp_dir, dst_data_path))
}

/// Recursively copies a directory and all its contents, skipping sqlite directories
pub fn copy_directory_without_sqlite(
  src: impl AsRef<Path>,
  dst: impl AsRef<Path>,
) -> io::Result<()> {
  fs::create_dir_all(&dst)?;

  for entry in fs::read_dir(src)? {
    let entry = entry?;
    let file_name = entry.file_name();
    let path = entry.path();

    // Skip sqlite directories
    if path.ends_with("sqlite") || path.to_string_lossy().contains("/sqlite/") {
      continue;
    }

    let ty = entry.file_type()?;
    if ty.is_dir() {
      copy_directory_without_sqlite(&path, dst.as_ref().join(&file_name))?;
    } else {
      fs::copy(&path, dst.as_ref().join(&file_name))?;
    }
  }

  Ok(())
}

/// Example of using the port allocation mechanism to get non-conflicting ports
#[test_log::test]
pub fn test_port_allocation() {
  // Allocate 3 ports starting from 8000 with spacing of 2
  let ports = TestEnv::allocate_ports(8000, 3, 2);

  // Check we got 3 ports
  assert_eq!(ports.len(), 3);

  // Check the spacing is correct (each port should be 2 more than the previous)
  for i in 1..ports.len() {
    assert_eq!(ports[i].public(), ports[i - 1].public() + 2);
  }

  // Try to allocate the same ports again - should get different ones
  let ports2 = TestEnv::allocate_ports(8000, 3, 2);

  // Check none of the ports in ports2 are in ports
  for p2 in &ports2 {
    assert!(ports.iter().all(|p| p.public() != p2.public()));
  }

  // Manually mark one port as used
  {
    let mut lock = USED_PORTS.lock().unwrap();
    lock.insert(8100);
    lock.insert(8101); // its internal port
  }

  // Try to allocate starting from 8098 - should skip 8100
  let ports3 = TestEnv::allocate_ports(8098, 3, 2);
  assert_eq!(ports3.len(), 3);
  assert!(ports3.iter().all(|p| p.public() != 8100));
}

/// Example of using the automatic port allocation in TestEnv
#[test_log::test(tokio::test)]
async fn test_auto_port_allocation() {
  // Create two TestEnv instances with 3 nodes each
  let env1 = TestEnv::new(3, "test_auto_port_allocation_env1").await;
  let env2 = TestEnv::new(3, "test_auto_port_allocation_env2").await;

  info!("Env1 ports: {:?}", env1.ports);
  info!("Env2 ports: {:?}", env2.ports);

  // Check that no ports conflict
  for p1 in &env1.ports {
    for p2 in &env2.ports {
      assert_ne!(
        p1.public(),
        p2.public(),
        "Port {} was reused between environments",
        p1.public()
      );

      assert_ne!(
        p1.public(),
        p2.internal(),
        "Port {} was reused between environments",
        p1.public()
      );

      assert_ne!(
        p1.internal(),
        p2.public(),
        "Port {} was reused between environments",
        p1.public()
      );

      assert_ne!(
        p1.internal(),
        p2.internal(),
        "Port {} was reused between environments",
        p1.internal()
      );
    }
  }

  // Check that each public port's corresponding internal port is correct (public+1)
  for port in env1.ports.iter().chain(env2.ports.iter()) {
    assert_eq!(
      port.public() + 1,
      port.internal(),
      "Internal port should be public port + 1"
    );
  }
}
