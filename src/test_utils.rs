use std::process::Command;
use std::time::Duration;
use uuid::Uuid;

// Simple wrapper to manage the MinIO test server for tests
#[allow(dead_code)]
pub struct MinioTestServer {
  pub access_key_id: String,
  pub secret_access_key: String,
  pub port: u16,
  pub endpoint: String,
  pub docker_name: String,
}

#[allow(dead_code)]
impl MinioTestServer {
  pub fn start() -> Self {
    let access_key = "adminadmin";
    let secret_key = "adminadmin";
    let docker_name = format!("minio-test-server-{}", Uuid::new_v4());
    let status = std::process::Command::new("docker")
      .args([
        "run",
        "--rm",
        "-P", // Dynamically assign port
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

    // Get the dynamically assigned port
    let port_output = Command::new("docker")
      .args(["port", &docker_name, "9000"])
      .output()
      .expect("Failed to get port from docker");
    assert!(port_output.status.success(), "docker port command failed");

    let port_string = String::from_utf8_lossy(&port_output.stdout);
    // The output is typically in the format "0.0.0.0:xxxxx" or "[::]:xxxxx"
    let port: u16 = port_string
      .split(':')
      .last()
      .expect("Unexpected docker port output format")
      .trim()
      .parse()
      .expect("Failed to parse port number");

    MinioTestServer {
      access_key_id: access_key.to_string(),
      secret_access_key: secret_key.to_string(),
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
          self.access_key_id, self.secret_access_key, self.port,
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

  #[cfg(test)]
  #[allow(dead_code)]
  pub fn has_files_for_cell(&self, bucket: &str, cell_id: &str) -> bool {
    let output = Command::new("docker")
      .args([
        "run",
        "--network=host",
        "--rm",
        "-e",
        &format!(
          "MC_HOST_minio=http://{}:{}@localhost:{}",
          self.access_key_id, self.secret_access_key, self.port
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

    stdout.contains(cell_id)
  }

  pub fn clear_bucket_files(
    &self,
    bucket_name: &str,
    prefix: &str,
  ) -> Result<(), anyhow::Error> {
    let status = Command::new("docker")
      .args([
        "run",
        "--network=host",
        "--rm",
        "-e",
        &format!(
          "MC_HOST_minio=http://{}:{}@localhost:{}",
          self.access_key_id, self.secret_access_key, self.port
        ),
        "minio/mc",
        "rm",
        "--recursive",
        "--force",
        &format!("minio/{}/{}", bucket_name, prefix),
      ])
      .spawn()?
      .wait()?;

    if !status.success() {
      return Err(anyhow::anyhow!("Failed to clear bucket files"));
    }

    Ok(())
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
