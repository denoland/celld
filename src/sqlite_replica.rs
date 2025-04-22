//! SqliteReplica abstraction for managing SQLite WAL replication to S3/MinIO via Litestream
//!
//! This module defines the **SqliteReplica** abstraction that encapsulates the full lifecycle
//! of durable state replication for a single Deno room isolate. It is designed to:
//!
//! 1. generate and write a per-room Litestream configuration file (`<room_id>.yml`),
//! 2. perform a **cold-start restore** from S3 or MinIO when no local database exists,
//! 3. spawn a long-running `litestream replicate` process in a **non-blocking** manner,
//! 4. trigger one-off `litestream backup` snapshots on room shutdown or parent exit,
//! 5. manage replication processes that may **outlive** the Deno isolate, tracking
//!    their execution and completion to ensure final backups finish.
//!
//! All **tests** for this abstraction live in this file and exercise against a real
//! MinIO server. Tests will either spin up a dedicated MinIO instance per-suite or
//! share a single instance for performance, then:
//!
//! - verify configuration file creation,
//! - validate `restore`, `replicate`, and `backup` commands against the live MinIO,
//! - cleanly shut down MinIO and replication processes,
//! - simulate empty-state (new room) and existing-state scenarios.
//!
//! # Directory layout
//!
//! ```text
//! <data-dir>/
//! └── <tenant>/
//!     ├── static/        # static assets
//!     ├── code/          # user-provided TypeScript hooks (main.ts)
//!     ├── sockets/       # runtime-generated Unix sockets
//!     └── sqlite/        # per-room state and config
//!         ├── <room_id>.db    # SQLite database file
//!         ├── <room_id>.yml   # Litestream YAML config for this room
//!         └── ...
//! ```
//!
//! # Non-blocking requirements
//! All SqliteReplica methods are **blocking** by nature (file I/O, process spawning).
//! Consumers **must** invoke them inside `tokio::task::spawn_blocking` or equivalent
//! to avoid stalling the async event loop.
//!
//! # Long-running processes
//! Litestream replication processes may continue after their corresponding Deno
//! subprocess exits. The SqliteReplica abstraction must:
//!
//! - track the `Child` handle for replication,
//! - allow querying or awaiting process termination,
//! - ensure backups complete before cleanup.

use anyhow::{anyhow, Context, Result};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use tokio::process::Command;
use tracing::{debug, info, warn};

/// Configuration for a MinIO or S3 replica target
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
  /// S3 endpoint URL (e.g., http://localhost:9000)
  pub endpoint: String,
  /// S3 bucket name
  pub bucket: String,
  /// S3 path prefix within the bucket
  pub path: String,
  /// AWS region (often 'us-east-1' for MinIO)
  pub region: String,
  /// AWS access key
  pub access_key_id: String,
  /// AWS secret key
  pub secret_access_key: String,
}

/// Configuration for a Litestream S3 replica
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LitestreamS3Replica {
  /// Replica type (only 's3' currently supported)
  #[serde(rename = "type")]
  pub replica_type: String,
  /// Optional replica name
  #[serde(skip_serializing_if = "Option::is_none")]
  pub name: Option<String>,
  /// S3 bucket name
  pub bucket: String,
  /// S3 path prefix
  pub path: String,
  /// S3 region
  pub region: String,
  /// S3 endpoint URL (for custom S3 implementations like MinIO)
  #[serde(skip_serializing_if = "Option::is_none")]
  pub endpoint: Option<String>,
  /// AWS access key
  #[serde(rename = "access-key-id")]
  pub access_key_id: String,
  /// AWS secret key
  #[serde(rename = "secret-access-key")]
  pub secret_access_key: String,
  /// Use path style for S3 URLs (automatically enabled when endpoint is set)
  #[serde(
    rename = "force-path-style",
    skip_serializing_if = "Option::is_none"
  )]
  pub force_path_style: Option<bool>,
  /// Skip TLS verification for S3 endpoint
  #[serde(rename = "skip-verify", skip_serializing_if = "Option::is_none")]
  pub skip_verify: Option<bool>,
  /// Sync interval in seconds
  #[serde(rename = "sync-interval", skip_serializing_if = "Option::is_none")]
  pub sync_interval: Option<String>,
  /// Snapshot interval
  #[serde(
    rename = "snapshot-interval",
    skip_serializing_if = "Option::is_none"
  )]
  pub snapshot_interval: Option<String>,
}

/// Configuration for a single database
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LitestreamDatabaseConfig {
  /// Path to the SQLite database file
  pub path: String,
  /// List of replicas for this database
  pub replicas: Vec<LitestreamS3Replica>,
}

/// Configuration for Litestream
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LitestreamConfig {
  /// Database configurations
  pub dbs: Vec<LitestreamDatabaseConfig>,
}

/// Represents a SQLite database with replication capabilities
pub struct SqliteReplica {
  /// Tenant identifier
  tenant: String,
  /// Room identifier
  room_id: String,
  /// Base data directory for all tenants and rooms
  data_dir: PathBuf,
  /// S3/MinIO configuration for replication
  s3_config: S3Config,
  /// Path to the SQLite database file
  db_path: PathBuf,
  /// Path to the Litestream configuration file
  config_path: PathBuf,
  /// Handle to the replication child process
  replication_process: Arc<Mutex<Option<Child>>>,
}

impl SqliteReplica {
  /// Creates a new SqliteReplica instance
  pub fn new(
    data_dir: &Path,
    tenant: &str,
    room_id: &str,
    s3_config: S3Config,
  ) -> Self {
    let room_dir = data_dir.join(tenant).join("sqlite");
    let db_path = room_dir.join(format!("{}.db", room_id));
    let config_path = room_dir.join(format!("{}.yml", room_id));

    fs::create_dir_all(&room_dir).unwrap_or_else(|e| {
      warn!("Failed to create room directory: {}", e);
    });

    SqliteReplica {
      tenant: tenant.to_string(),
      room_id: room_id.to_string(),
      data_dir: data_dir.to_path_buf(),
      s3_config,
      db_path,
      config_path,
      replication_process: Arc::new(Mutex::new(None)),
    }
  }

  /// Returns the path to the SQLite database file
  pub fn db_path(&self) -> &Path {
    &self.db_path
  }

  /// Checks if the database file exists
  pub fn db_exists(&self) -> bool {
    self.db_path.exists() && self.db_path.is_file()
  }

  /// Writes Litestream config file if needed
  pub fn write_config(&self) -> Result<()> {
    // Skip if config file already exists
    if self.config_path.exists() {
      debug!("Config file already exists: {:?}", self.config_path);
      return Ok(());
    }

    // Create parent directory if needed
    if let Some(parent) = self.config_path.parent() {
      fs::create_dir_all(parent).with_context(|| {
        format!("Failed to create config dir: {:?}", parent)
      })?;
    }

    let replica = LitestreamS3Replica {
      replica_type: "s3".to_string(),
      name: Some(format!("{}-replica", self.room_id)),
      bucket: self.s3_config.bucket.clone(),
      path: format!("{}/{}", self.s3_config.path, self.room_id),
      region: self.s3_config.region.clone(),
      endpoint: Some(self.s3_config.endpoint.clone()),
      access_key_id: self.s3_config.access_key_id.clone(),
      secret_access_key: self.s3_config.secret_access_key.clone(),
      force_path_style: Some(true), // Needed for MinIO
      skip_verify: None,
      sync_interval: Some("1s".to_string()),
      snapshot_interval: Some("24h".to_string()),
    };

    let db_config = LitestreamDatabaseConfig {
      path: self.db_path.to_string_lossy().to_string(),
      replicas: vec![replica],
    };

    let config = LitestreamConfig {
      dbs: vec![db_config],
    };

    let yaml = serde_yaml::to_string(&config)
      .context("Failed to serialize Litestream config")?;

    let mut file = File::create(&self.config_path).with_context(|| {
      format!("Failed to create config file: {:?}", self.config_path)
    })?;

    file
      .write_all(yaml.as_bytes())
      .context("Failed to write config file")?;

    debug!("Wrote Litestream config to {:?}", self.config_path);
    Ok(())
  }

  /// Checks if DB exists; if not, calls `litestream restore`
  pub async fn restore_if_needed(&self) -> Result<bool> {
    // If the database already exists, nothing to do
    if self.db_exists() {
      debug!("Database already exists: {:?}", self.db_path);
      return Ok(false);
    }

    // Ensure config is written
    self.write_config()?;

    // Create parent directory if needed
    if let Some(parent) = self.db_path.parent() {
      fs::create_dir_all(parent).with_context(|| {
        format!("Failed to create DB directory: {:?}", parent)
      })?;
    }

    info!("Restoring database from S3: {:?}", self.db_path);

    // Try to restore from S3
    let output = Command::new("litestream")
      .arg("restore")
      .arg("-config")
      .arg(&self.config_path)
      .arg(&self.db_path)
      .stdout(Stdio::piped())
      .stderr(Stdio::piped())
      .output()
      .await
      .context("Failed to execute litestream restore")?;

    if !output.status.success() {
      let stderr = String::from_utf8_lossy(&output.stderr);
      let stdout = String::from_utf8_lossy(&output.stdout);
      println!(
        "Litestream restore output:\nSTDOUT: {}\nSTDERR: {}",
        stdout, stderr
      );

      // Check if it failed because replica doesn't exist (first time)
      if stdout.contains("no matching backups found") {
        // This is a first-time setup, create an empty database
        debug!("No existing replicas found, creating new empty database");
        File::create(&self.db_path).with_context(|| {
          format!("Failed to create empty DB: {:?}", self.db_path)
        })?;
        info!("Created new empty database: {:?}", self.db_path);
        return Ok(false); // Indicate that no restoration occurred
      }

      return Err(anyhow!("Litestream restore failed: {}", stderr));
    }

    info!("Database restored successfully: {:?}", self.db_path);
    Ok(true)
  }

  /// Spawns `litestream replicate -config ...` in background
  pub async fn start_replication(&self) -> Result<()> {
    // Make sure we have a config file
    self.write_config()?;

    // Make sure the database exists
    if !self.db_exists() {
      return Err(anyhow!("Database does not exist: {:?}", self.db_path));
    }

    // Check if replication is already running
    let mut process_guard = self.replication_process.lock().unwrap();
    if let Some(_child) = &mut *process_guard {
      debug!("Replication already running");
      return Err(anyhow!("Replication already running"));
    }

    info!("Starting Litestream replication for {:?}", self.db_path);

    // Start the replication process
    // Use std::process::Command here because tokio::process::Child doesn't implement Send
    // and we need to store it in the Arc<Mutex<Option<Child>>>
    let child = std::process::Command::new("litestream")
      .arg("replicate")
      .arg("-config")
      .arg(&self.config_path)
      .stdout(Stdio::piped())
      .stderr(Stdio::piped())
      .spawn()
      .context("Failed to start litestream replicate")?;

    *process_guard = Some(child);
    drop(process_guard);

    debug!("Litestream replication started");
    Ok(())
  }

  /// Kills the replicate process
  pub async fn shutdown(&self) -> Result<()> {
    let mut process_guard = self.replication_process.lock().unwrap();

    if let Some(mut child) = process_guard.take() {
      info!("Shutting down replication for {:?}", self.db_path);

      //Send SIGTERM for graceful shutdown
      let pid = Pid::from_raw(child.id() as i32);
      kill(pid, Signal::SIGTERM)?;

      child.wait()?;
    } else {
      debug!("No replication process to shutdown");
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::process::Command;
  use std::time::Duration;
  use tempfile;

  // Simple wrapper to manage the MinIO test server
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
      let docker_name = format!("minio-test-server-{}", uuid::Uuid::new_v4());
      let status = std::process::Command::new("docker")
        .args([
          "run",
          "--rm",
          "-p",
          format!("{}:9000", port).as_str(),
          "-detatch",
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

    pub fn create_bucket(&self, bucket_name: &str) -> Result<()> {
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
        return Err(anyhow!("Failed to create bucket"));
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
      assert!(status.success(), "MinIO server failed to stop");
    }
  }

  // Helper to create test S3Config
  fn create_test_s3_config(
    test_name: &str,
    minio: &MinioTestServer,
  ) -> S3Config {
    minio.create_bucket(test_name).unwrap();
    S3Config {
      endpoint: minio.endpoint.clone(),
      region: "us-east-1".to_string(),
      bucket: test_name.to_string(),
      path: format!("roomd-test-{}", test_name),
      access_key_id: minio.access_key.clone(),
      secret_access_key: minio.secret_key.clone(),
    }
  }

  // Helper to check if MinIO bucket has any data for our room
  async fn minio_bucket_has_room_data(
    room_id: &str,
    minio: &MinioTestServer,
  ) -> Result<bool> {
    let output = Command::new("docker")
      .args([
        "run",
        "--network=host",
        "--rm",
        "-e",
        &format!(
          "MC_HOST_minio=http://{}:{}@localhost:{}",
          minio.access_key, minio.secret_key, minio.port
        ),
        "minio/mc",
        "ls",
        "--recursive",
        &format!("minio/{}", room_id), // Just check the bucket itself
      ])
      .output()
      .context("Failed to list MinIO bucket contents")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("Checking bucket '{}', contents: {}", room_id, stdout);

    // If we got any output, there's data
    Ok(!stdout.trim().is_empty())
  }

  #[tokio::test]
  async fn basic_replica_lifecycle() {
    tracing_subscriber::fmt::init();

    // Initialize MinIO per-test instance
    let minio_guard = MinioTestServer::start(9001);

    // Use a temporary directory for the test
    let temp_dir = tempfile::TempDir::new().unwrap();
    let data_dir = temp_dir.path();

    let room_id = "basic-replica-lifecycle";
    let s3_config = create_test_s3_config(room_id, &minio_guard);
    let replica =
      SqliteReplica::new(data_dir, "test-tenant", room_id, s3_config);

    // On first run, restore_if_needed will return false (no restoration)
    // but will create an empty DB file
    let restored = replica.restore_if_needed().await.unwrap();
    println!(
      "Restore result: {}",
      if restored {
        "restored from backup"
      } else {
        "created new empty DB"
      }
    );

    // Insert data using sqlite3 CLI
    let db_path = replica.db_path().to_string_lossy().to_string();
    // Make sure DB exists after restore
    assert!(
      std::fs::metadata(&db_path).is_ok(),
      "DB file should exist after restore"
    );

    let output = std::process::Command::new("sqlite3")
            .arg(&db_path)
            .arg("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test VALUES (1, 'test-data');")
            .output()
            .unwrap();

    if !output.status.success() {
      println!("SQLite error: {}", String::from_utf8_lossy(&output.stderr));
      println!("SQLite output: {}", String::from_utf8_lossy(&output.stdout));
    }

    assert!(output.status.success(), "SQLite command should succeed");
    println!("Inserted data into SQLite DB {}", db_path);

    // Start replication
    assert!(
      replica.start_replication().await.is_ok(),
      "Should start Litestream"
    );

    // Give the replication process time to run and create snapshots
    println!("Waiting for replication to complete...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Stop replication cleanly
    replica.shutdown().await.unwrap();

    assert!(
      minio_bucket_has_room_data(&room_id, &minio_guard)
        .await
        .unwrap(),
      "Expected replica data"
    );
  }
}
