mod captured_subprocess;
#[path = "../../src/test_utils.rs"]
mod test_utils;

use captured_subprocess::CapturedSubprocess;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use std::collections::HashSet;
use std::process::Command;
use std::sync::Mutex;
use std::time::Duration;
use test_utils::MinioTestServer;
use uuid::Uuid;

lazy_static::lazy_static! {
  static ref USED_PORTS: Mutex<HashSet<u16>> = Mutex::new(HashSet::new());
}

pub struct TestEnv {
  /// Celld servers
  servers: Vec<CapturedSubprocess>,
  /// Celld server ports (external ports)
  ports: Vec<u16>,
  pub minio_server: MinioTestServer,
  test_id: String,
  bucket_name: String,
  /// Make public_ports public for tests that need to access them
  /// TODO(magurotuna): this is identical to `ports` . Maybe remove one of them?
  pub public_ports: Vec<u16>,
  /// Celld server ports (internal ports)
  pub internal_ports: Vec<u16>,
}

impl TestEnv {
  // Reserve a block of consecutive free ports
  pub fn allocate_ports(base: u16, count: usize, spacing: u16) -> Vec<u16> {
    let mut lock = USED_PORTS.lock().unwrap();
    let mut allocated = Vec::with_capacity(count);
    let mut next_port = base;

    while allocated.len() < count {
      // Check if this port or its internal port (port+1) are already used
      if !lock.contains(&next_port) && !lock.contains(&(next_port + 1)) {
        // Reserve both the public port and its internal port
        lock.insert(next_port);
        lock.insert(next_port + 1);
        allocated.push(next_port);
      }
      // Move to next port candidate with spacing to avoid conflicts
      next_port += spacing;
    }

    allocated
  }

  // Start mesh nodes with auto-allocated non-conflicting ports
  pub fn new(count: usize) -> Self {
    // Start with port 7500 and use spacing of 2 to avoid conflicts with internal ports
    let ports = Self::allocate_ports(7500, count, 2);
    Self::new_with_ports(&ports)
  }

  // Backwards compatibility method that takes explicit ports
  pub fn new_with_ports(ports: &[u16]) -> Self {
    // Mark all the provided ports and their internal ports as used
    let mut lock = USED_PORTS.lock().unwrap();
    for &port in ports {
      lock.insert(port);
      lock.insert(port + 1);
    }
    drop(lock);

    // Calculate internal ports
    let public_ports = ports.to_vec();
    let internal_ports: Vec<u16> = ports.iter().map(|&p| p + 1).collect();
    // Start MinIO server for testing with a dynamically assigned port
    let bucket_name = "test-mesh-bucket".to_string();
    let minio_server = MinioTestServer::start();
    minio_server.create_bucket(&bucket_name).unwrap();

    let servers = Vec::new();
    let test_id = Uuid::new_v4().simple().to_string();
    let mut test_env = TestEnv {
      servers,
      ports: ports.to_vec(),
      minio_server,
      bucket_name,
      test_id: test_id.to_string(),
      public_ports: public_ports.clone(),
      internal_ports: internal_ports.clone(),
    };

    for &port in ports.iter() {
      test_env.spawn_cell_instance(port);
    }

    // Wait for servers to be ready by probing TCP connections
    println!("Waiting for servers to initialize...");
    for &port in ports {
      Self::wait_for_server_ready(port);
    }

    // Longer delay for peer exchange after TCP connections are ready
    // This is important to give time for S3 registration and peer discovery
    std::thread::sleep(Duration::from_secs(2));
    println!("All servers are ready now");
    test_env
  }

  pub fn kill_cell_instance(&mut self, index: usize) {
    let mut server = self.servers.remove(index);
    let _ = self.ports.remove(index);
    let pid = Pid::from_raw(server.child().id() as i32);
    // Use SIGKILL to avoid long graceful shutdown times
    kill(pid, Signal::SIGKILL).unwrap();
    server.child_mut().wait().unwrap();
  }

  pub fn graceful_shutdown_cell_instance(&mut self, index: usize) {
    let mut server = self.servers.remove(index);
    let _ = self.ports.remove(index);
    let pid = Pid::from_raw(server.child().id() as i32);
    kill(pid, Signal::SIGTERM).unwrap();
    server.child_mut().wait().unwrap();
  }

  pub fn spawn_cell_instance(&mut self, port: u16) {
    let advertise_addr = format!("127.0.0.1:{}", port);
    let internal_addr = format!("127.0.0.1:{}", port + 1);
    let server_cmd = Command::new(env!("CARGO_BIN_EXE_celld"));
    let server_cmd_setup = |cmd: &mut Command| {
      cmd
        .env("RUST_LOG", "info")
        .env("ADVERTISE_ADDR", &advertise_addr)
        .env("INTERNAL_LISTEN_ADDR", &internal_addr)
        // TODO: Each TestEnv instance should have its own data directory to
        // avoid conflicts between test cases running in parallel
        .env("DATA", "./data")
        .env("CELL_HEARTBEAT_INTERVAL", "2")
        .env("CELL_GRACE_PERIOD_SECONDS", "5")
        // Use a shorter staleness threshold for tests to detect failures faster
        .env("CELL_STALENESS_THRESHOLD_SECS", "6")
        .env("CELL_LOCK_GUARD_TTL_SECS", "6")
        .env("CELL_SYSTEM_CELL_TAKEOVER_INTERVAL_SECS", "2")
        // Configure alarm scheduler to check alarms every second
        .env("CELL_ALARM_SCHEDULER_INTERVAL_SECS", "1")
        .env(
          "CELL_S3_ENDPOINT",
          format!("http://localhost:{}", self.minio_server.port),
        )
        .env("CELL_S3_BUCKET", &self.bucket_name)
        .env("CELL_S3_REGION", "us-east-1")
        .env("CELL_S3_PREFIX", format!("celld-test-{}", self.test_id))
        .env("CELL_S3_ACCESS_KEY_ID", &self.minio_server.access_key_id)
        .env(
          "CELL_S3_SECRET_ACCESS_KEY",
          &self.minio_server.secret_access_key,
        );
    };
    let server = CapturedSubprocess::new(server_cmd, server_cmd_setup);

    self.servers.push(server);
    self.ports.push(port);
    println!(
      "Started server on port {} with ADVERTISE_ADDR={} and S3 mesh",
      port, advertise_addr
    );
  }

  // Wait for a server to be ready by probing its TCP port
  pub fn wait_for_server_ready(port: u16) {
    const MAX_ATTEMPTS: usize = 10;
    const RETRY_DELAY_MS: u64 = 200;

    // Check both the data port and the internal port (port + 1)
    let ports = [port, port + 1];

    for &p in &ports {
      for attempt in 1..=MAX_ATTEMPTS {
        match std::net::TcpStream::connect(format!("127.0.0.1:{}", p)) {
          Ok(_) => {
            println!("Port {} is ready", p);
            break;
          }
          Err(_) => {
            println!(
              "Waiting for server on port {} (attempt {}/{})",
              p, attempt, MAX_ATTEMPTS
            );
            std::thread::sleep(Duration::from_millis(RETRY_DELAY_MS));
            if attempt == MAX_ATTEMPTS {
              panic!("Server on port {} failed to start", p);
            }
          }
        }
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

    // Release all ports
    let mut lock = USED_PORTS.lock().unwrap();
    for &port in &self.ports {
      lock.remove(&port);
      lock.remove(&(port + 1));
    }
  }
}

/// Example of using the port allocation mechanism to get non-conflicting ports
#[test]
pub fn test_port_allocation() {
  // Allocate 3 ports starting from 8000 with spacing of 2
  let ports = TestEnv::allocate_ports(8000, 3, 2);

  // Check we got 3 ports
  assert_eq!(ports.len(), 3);

  // Check the spacing is correct (each port should be 2 more than the previous)
  for i in 1..ports.len() {
    assert_eq!(ports[i], ports[i - 1] + 2);
  }

  // Try to allocate the same ports again - should get different ones
  let ports2 = TestEnv::allocate_ports(8000, 3, 2);

  // Check none of the ports in ports2 are in ports
  for &p in &ports2 {
    assert!(!ports.contains(&p));
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
  assert!(!ports3.contains(&8100));
}

/// Example of using the automatic port allocation in TestEnv
#[test]
pub fn test_auto_port_allocation() {
  // Create two TestEnv instances with 3 nodes each
  let env1 = TestEnv::new(3);
  let env2 = TestEnv::new(3);

  println!("Env1 public ports: {:?}", env1.public_ports);
  println!("Env1 internal ports: {:?}", env1.internal_ports);
  println!("Env2 public ports: {:?}", env2.public_ports);
  println!("Env2 internal ports: {:?}", env2.internal_ports);

  // Check that none of the ports in env1 are in env2
  for &p1 in &env1.public_ports {
    for &p2 in &env2.public_ports {
      assert_ne!(p1, p2, "Port {} was reused between environments", p1);
    }
  }

  // Check that public and internal ports don't conflict
  for &pub1 in &env1.public_ports {
    for &int2 in &env2.internal_ports {
      assert_ne!(
        pub1, int2,
        "Public port {} conflicts with internal port",
        pub1
      );
    }
  }

  for &int1 in &env1.internal_ports {
    for &pub2 in &env2.public_ports {
      assert_ne!(
        int1, pub2,
        "Internal port {} conflicts with public port",
        int1
      );
    }
  }

  // Check that each public port's corresponding internal port is correct (public+1)
  for i in 0..env1.public_ports.len() {
    assert_eq!(
      env1.internal_ports[i],
      env1.public_ports[i] + 1,
      "Internal port should be public port + 1"
    );
  }
}
