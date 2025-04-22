use std::process::Command;
use std::time::Duration;
use uuid::Uuid;

// Simple wrapper to manage the MinIO test server for tests
pub struct MinioTestServer {
  pub access_key: String,
  pub secret_key: String,
  pub port: u16,
  pub endpoint: String,
  pub docker_name: String,
}

impl MinioTestServer {
  pub fn start(port: u16) -> Self {
    let access_key = "adminadmin";
    let secret_key = "adminadmin";
    let docker_name = format!("minio-test-server-{}", Uuid::new_v4());
    let status = std::process::Command::new("docker")
      .args([
        "run",
        "--rm",
        "-p",
        format!("{}:9000", port).as_str(),
        "-detach",
        "--name",
        docker_name.as_str(),
        "-e",
        &format!("MINIO_ROOT_USER={}", access_key),
        "-e",
        &format!("MINIO_ROOT_PASSWORD={}", secret_key),
        "-e",
        "MINIO_REGION_NAME=us-east-1",
        "minio/minio",
        "server",
        "/data",
      ])
      .spawn()
      .unwrap()
      .wait()
      .unwrap();
    assert!(status.success(), "MinIO server failed to start");

    // Give MinIO some time to start
    std::thread::sleep(Duration::from_secs(3));

    MinioTestServer {
      access_key: access_key.to_string(),
      secret_key: secret_key.to_string(),
      docker_name,
      port,
      endpoint: format!("http://localhost:{}", port),
    }
  }

  pub fn create_bucket(&self, bucket_name: &str) -> Result<(), String> {
    let status = Command::new("docker")
      .args([
        "run",
        "--network=host",
        "--rm",
        "-e",
        &format!(
          "MC_HOST_minio=http://{}:{}@localhost:{}",
          self.access_key, self.secret_key, self.port,
        ),
        "minio/mc",
        "mb",
        &format!("minio/{}", bucket_name),
      ])
      .spawn()
      .unwrap()
      .wait()
      .unwrap();
    if !status.success() {
      return Err("Failed to create bucket".to_string());
    }

    Ok(())
  }

  pub fn has_files_for_room(&self, bucket: &str, room_id: &str) -> bool {
    let output = Command::new("docker")
      .args([
        "run",
        "--network=host",
        "--rm",
        "-e",
        &format!(
          "MC_HOST_minio=http://{}:{}@localhost:{}",
          self.access_key, self.secret_key, self.port
        ),
        "minio/mc",
        "ls",
        "--recursive",
        &format!("minio/{}", bucket),
      ])
      .output()
      .expect("Failed to list MinIO bucket contents");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("MinIO bucket contents: {}", stdout);

    stdout.contains(room_id)
  }
}

impl Drop for MinioTestServer {
  fn drop(&mut self) {
    let status = std::process::Command::new("docker")
      .args(["kill", self.docker_name.as_str()])
      .spawn()
      .unwrap()
      .wait()
      .unwrap();
    println!("Stopping MinIO server: {}", self.docker_name);
    assert!(status.success(), "MinIO server failed to stop");
  }
}
