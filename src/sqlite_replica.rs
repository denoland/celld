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
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, info, warn};
use wait_timeout::ChildExt;

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

/// Configuration for a Litestream replica
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LitestreamReplica {
  /// Replica type (only 's3' currently supported)
  #[serde(rename = "type")]
  pub replica_type: String,
  /// S3 endpoint URL
  pub endpoint: String,
  /// S3 region
  pub region: String,
  /// S3 bucket name
  pub bucket: String,
  /// S3 path prefix
  pub path: String,
  /// AWS access key
  pub access_key_id: String,
  /// AWS secret key
  pub secret_access_key: String,
  /// Sync interval in seconds
  #[serde(skip_serializing_if = "Option::is_none")]
  pub sync_interval: Option<String>,
}

/// Configuration for Litestream
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LitestreamConfig {
  /// Path to the SQLite database file
  pub db: String,
  /// List of replicas for this database
  pub replicas: Vec<LitestreamReplica>,
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
    /// Writes config file if needed
    pub async fn write_config(&self, backend: &ReplicaBackend) -> Result<()>;

    /// Checks if DB exists; if not, calls `litestream restore`
    pub async fn restore_if_needed(&self) -> Result<()>;

    /// Spawns `litestream replicate -config ...` in background
    pub async fn start_replication(&self) -> Result<()>;

    /// Waits N seconds after isolate exit to allow WAL flush
    pub async fn wait_for_flush(&self, timeout: Duration) -> Result<()>;

    /// Kills the replicate process
    pub async fn shutdown(&self) -> Result<()>;
}



#[cfg(test)]
mod tests {
  use super::*;
  use once_cell::sync::Lazy;
  use std::fs;
  use std::io::Read;
  use std::process::{Child, Command, Stdio};
  use std::time::Duration;
  use tempfile::TempDir;
  use crate::child_on_parent_exit::ChildOnParentExit;

  // Static MinIO test server that will be shared across all tests
  pub static MINIO_SERVER: Lazy<MinioTestServer> =
    Lazy::new(|| MinioTestServer::start());

  // Simple wrapper to manage the MinIO test server
  pub struct MinioTestServer {
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: String,
    child: ChildOnParentExit,
  }

  impl MinioTestServer {
    pub fn start() -> Self {
      let access_key = "adminadmin";
      let secret_key = "adminadmin";
      let mut cmd = Command::new("docker");
      cmd.args([
          "run",
          "--rm",
          "-p",
          "9000:9000",
          "-e",
          &format!("MINIO_ROOT_USER={}", access_key),
          "-e",
          &format!("MINIO_ROOT_PASSWORD={}", secret_key),
          "-e",
          "MINIO_REGION_NAME=us-east-1",
          "minio/minio",
          "server",
          "/data",
        ]);
      let child = ChildOnParentExit::spawn(cmd).unwrap();

      MinioTestServer {
        child,
        access_key: access_key.to_string(),
        secret_key: secret_key.to_string(),
        endpoint: "http://localhost:9000".to_string(),
      }
    }

    pub fn create_bucket(&self, bucket_name: &str) -> Result<()> {
      let output = Command::new("docker")
        .args([
          "run",
          "--network=host",
          "--rm",
          "-e",
          &format!(
            "MC_HOST_minio=http://{}:{}@localhost:9000",
            self.access_key, self.secret_key
          ),
          "minio/mc",
          "mb",
          &format!("minio/{}", bucket_name),
        ])
        .output()
        .context("Failed to create MinIO bucket")?;

      if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // If the bucket already exists, that's fine
        if !stderr.contains("already exists") {
          return Err(anyhow!("Failed to create bucket: {}", stderr));
        }
      }

      Ok(())
    }
  }

  // Helper to create test S3Config
  fn create_test_s3_config(
    test_name: &str,
  ) -> S3Config {
    let minio = &*MINIO_SERVER;
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

	#[tokio::test]
	async fn basic_replica_lifecycle() -> Result<()> {
			let temp_dir = TempDir::new()?;
			let data_dir = temp_dir.path();

			let room_id = format!("basic-{}", uuid::Uuid::new_v4());
			let s3_config = create_test_s3_config(&room_id);
			let replica = SqliteReplica::new(data_dir, "test-tenant", &room_id, s3_config);

			let restored = replica.restore_if_needed().await?;
			assert!(restored, "DB should be created on first restore");
			assert!(replica.db_exists(), "DB must exist after restore");

			// Insert data using sqlite3 CLI
			let db_path = replica.db_path().to_string_lossy().to_string();
			let output = std::process::Command::new("sqlite3")
					.arg(&db_path)
					.arg("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test VALUES (1, 'test-data');")
					.output()
					.unwrap();
			assert!(output.success());

			// Start replication
			assert!(replica.start_replication().await.is_ok(), "Should start Litestream");

			// Give time for WAL flush
			tokio::time::sleep(Duration::from_secs(2)).await;

			// Trigger a final flush if supported (e.g., -once)
			assert!(replica.flush_or_backup().await.is_ok(), "Final backup should work");

			// Stop replication cleanly
			replica.shutdown().await?;

			// Optionally: verify something was uploaded to MinIO
			assert!(minio_bucket_has_room_data(&room_id).await?, "Expected replica data");

			Ok(())
	}


}
