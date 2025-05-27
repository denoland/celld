use std::io::{self, BufRead, BufReader};
use std::process::{Child, Command};
use tempfile::TempDir;
use tracing::trace;

// Simple wrapper to manage the MinIO test server for tests
#[allow(dead_code)]
pub struct MinioTestServer {
  pub access_key_id: String,
  pub secret_access_key: String,
  pub port: u16,
  pub endpoint: String,
  pub tempdir: TempDir,
  pub process: Child,
}

#[allow(dead_code)]
impl MinioTestServer {
  pub fn start() -> Self {
    let access_key = "adminadmin";
    let secret_key = "adminadmin";

    let tempdir = TempDir::new().unwrap();

    // Start minio process with in-memory storage
    let mut process = std::process::Command::new("minio")
      .args(["server", tempdir.path().to_str().unwrap(), "--address=:0"])
      .env("MINIO_ROOT_USER", access_key)
      .env("MINIO_ROOT_PASSWORD", secret_key)
      .env("MINIO_REGION_NAME", "us-east-1")
      .env("MINIO_CI_CD", "on") // Enable in-memory storage mode
      .current_dir(tempdir.path()) // Set working directory for logs
      .stdout(std::process::Stdio::piped())
      .stderr(std::process::Stdio::piped())
      .spawn()
      .expect("Failed to start minio server");

    let stdout = process.stdout.take().unwrap();
    let reader = BufReader::new(stdout);
    std::thread::spawn(move || {
      for line in reader.lines() {
        let line = line.unwrap();
        trace!("[minio] {line}");
      }
    });

    let stderr = process.stderr.take().unwrap();
    let reader = BufReader::new(stderr);
    let (port_tx, port_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
      for line in reader.lines() {
        let line = line.unwrap();
        trace!("[minio] {line}");
        if let Some(port) = extract_minio_api_port(&line) {
          port_tx.send(port).unwrap();
        }
      }
    });

    let port = port_rx.recv().unwrap();

    MinioTestServer {
      access_key_id: access_key.to_string(),
      secret_access_key: secret_key.to_string(),
      tempdir,
      port,
      process,
      endpoint: format!("http://127.0.0.1:{}", port),
    }
  }

  pub fn create_bucket(&self, bucket_name: &str) -> Result<(), String> {
    // Configure the minio client with environment variables
    let mc_env = format!(
      "http://{}:{}@127.0.0.1:{}",
      self.access_key_id, self.secret_access_key, self.port
    );

    // Create the bucket directly using environment variable method
    let status = Command::new("mc")
      .env("MC_HOST_minio", &mc_env)
      .args([
        "mb",
        &format!("minio/{}", bucket_name),
        "--ignore-existing", // Don't fail if bucket already exists
      ])
      .stdout(std::process::Stdio::null())
      .stderr(std::process::Stdio::null())
      .spawn()
      .unwrap()
      .wait()
      .unwrap();

    if !status.success() {
      eprintln!("Failed to create bucket with MC_HOST_minio={}", mc_env);
      return Err("Failed to create bucket".to_string());
    }

    Ok(())
  }

  #[cfg(test)]
  #[allow(dead_code)]
  pub fn has_files_for_cell(&self, bucket: &str, cell_id: &str) -> bool {
    // Configure the minio client with environment variables
    let mc_env = format!(
      "http://{}:{}@127.0.0.1:{}",
      self.access_key_id, self.secret_access_key, self.port
    );

    let output = Command::new("mc")
      .env("MC_HOST_minio", &mc_env)
      .args(["ls", "--recursive", &format!("minio/{}", bucket)])
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
    // Configure the minio client with environment variables
    let mc_env = format!(
      "http://{}:{}@127.0.0.1:{}",
      self.access_key_id, self.secret_access_key, self.port
    );

    let status = Command::new("mc")
      .env("MC_HOST_minio", &mc_env)
      .args([
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
    // Kill the minio process
    if let Err(e) = self.process.kill() {
      eprintln!("Failed to kill minio server: {}", e);
    }

    // The tempdir will be cleaned up automatically when it's dropped
  }
}

/// Parses MinIO stderr line to find the API port
fn extract_minio_api_port(line: &str) -> Option<u16> {
  if !line.contains("API:") {
    return None;
  }

  for part in line.split_whitespace() {
    if let Some(port_candidate_str) = part.strip_prefix("http://127.0.0.1:") {
      let port_numeric_part: String = port_candidate_str
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
      if !port_numeric_part.is_empty() {
        if let Ok(port_num) = port_numeric_part.parse::<u16>() {
          return Some(port_num);
        }
      }
    }
  }

  None
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Cursor; // To simulate a readable stream from a byte slice.

  // Utility function to call extract_minio_api_port on a BufRead source
  fn parse_minio_api_port<R: BufRead + ?Sized>(
    reader: &mut R,
  ) -> io::Result<Option<u16>> {
    for line_result in reader.lines() {
      let line = line_result?;
      if let Some(port) = extract_minio_api_port(&line) {
        return Ok(Some(port));
      }
    }
    Ok(None)
  }

  #[test]
  fn test_parse_minio_output() {
    let full_output_str = r#"
# MINIO_CI_CD=1 minio server . --address=:0
INFO: Formatting 1st pool, 1 set(s), 1 drives per set.
INFO: WARNING: Host local has more than 0 drives of set. A host failure will result in data becoming unavailable.
MinIO Object Storage Server
Copyright: 2015-2025 MinIO, Inc.
License: GNU AGPLv3 - https://www.gnu.org/licenses/agpl-3.0.html
Version: RELEASE.2025-04-22T22-12-26Z (go1.24.2 darwin/arm64)

API: http://192.168.1.101:54241  http://192.168.64.1:54241  http://127.0.0.1:54321
    RootUser: minioadmin
    RootPass: minioadmin

WebUI: http://192.168.1.101:54240 http://192.168.64.1:54240 http://127.0.0.1:54240
    RootUser: minioadmin
    RootPass: minioadmin

CLI: https://min.io/docs/minio/linux/reference/minio-mc.html#quickstart
    $ mc alias set 'myminio' 'http://192.168.1.101:54241' 'minioadmin' 'minioadmin'

Docs: https://docs.min.io
WARN: Detected default credentials 'minioadmin:minioadmin', we recommend that you change these values with 'MINIO_ROOT_USER' and 'MINIO_ROOT_PASSWORD' environment variables
        "#;
    // Create a BufReader from a string slice using Cursor.
    let mut reader_positive = Cursor::new(full_output_str.as_bytes());
    // Test the positive case: port found.
    assert_eq!(
      parse_minio_api_port(&mut reader_positive).unwrap(),
      Some(54321),
      "Should parse the port 54321 for 127.0.0.1"
    );

    // Test a negative case: API line for 127.0.0.1 is missing.
    let output_no_127_api_str = r#"
API: http://192.168.1.101:54241 http://example.com:8080
        "#;
    let mut reader_no_127 = Cursor::new(output_no_127_api_str.as_bytes());
    assert_eq!(
      parse_minio_api_port(&mut reader_no_127).unwrap(),
      None,
      "Should return None if 127.0.0.1 API is not present"
    );

    // Test a negative case: No API line at all.
    let output_no_api_line_str = r#"
MinIO Object Storage Server
WebUI: http://127.0.0.1:54240
        "#;
    let mut reader_no_api = Cursor::new(output_no_api_line_str.as_bytes());
    assert_eq!(
      parse_minio_api_port(&mut reader_no_api).unwrap(),
      None,
      "Should return None if no API line is present"
    );

    // Test an empty input.
    let empty_output_str = "";
    let mut reader_empty = Cursor::new(empty_output_str.as_bytes());
    assert_eq!(
      parse_minio_api_port(&mut reader_empty).unwrap(),
      None,
      "Should return None for empty input"
    );
  }
}
