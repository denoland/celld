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

use crate::config::S3Config;
use crate::distributed_lock;
use anyhow::{anyhow, Context, Result};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::process::Command;
use tracing::{debug, error, info, instrument, warn};

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
#[derive(Debug, Clone)]
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
  replication_process: Arc<Mutex<Option<tokio::process::Child>>>,
}

impl SqliteReplica {
  /// Initialize a new SQLite replica if S3 is configured
  /// Returns Some(Self) if initialized, None if S3 is not configured
  /// Note: This does NOT restore the database, only sets up the replica
  pub async fn initialize(
    data_dir: &Path,
    tenant: &str,
    room_id: &str,
    s3_config: Option<S3Config>,
  ) -> Result<Option<Self>> {
    // If S3 is not configured, return None (no replication)
    let s3_config = match s3_config {
      Some(config) => config,
      None => {
        debug!("S3 not configured, skipping SQLite replica initialization");
        return Ok(None);
      }
    };

    let db_path = Path::new(data_dir)
      .join(tenant)
      .join("sqlite")
      .join(format!("{}.db", room_id));

    let data_dir = Path::new(data_dir).to_path_buf();

    // Create the configuration directory
    let config_dir = data_dir.join(tenant).join("sqlite");
    fs::create_dir_all(&config_dir)
      .context("Failed to create sqlite directory")?;

    // Create a unique config file name for this replica
    let config_file = config_dir.join(format!("{}.yml", room_id));

    // Create SqliteReplica instance (without running restore yet)
    let replica = Self {
      tenant: tenant.to_string(),
      room_id: room_id.to_string(),
      data_dir: data_dir.clone(),
      db_path: db_path.clone(),
      s3_config: s3_config.clone(),
      config_path: config_file.clone(),
      replication_process: Arc::new(Mutex::new(None)),
    };

    // Write the config file (needed for restore/replicate operations)
    if let Err(err) = replica.write_config() {
      warn!(
        tenant = %tenant,
        room_id = %room_id,
        error = %err,
        "Failed to write litestream config"
      );
      // Continue anyway - we'll try again later if needed
    }

    // Return the replica (restore will be done separately)
    Ok(Some(replica))
  }

  /// Run the restore operation for this replica
  /// Returns true if data was restored, false if no backup was found or the database already exists
  #[instrument(skip(self))]
  pub async fn run_restore(&self) -> Result<bool> {
    // If the database already exists, nothing to restore
    if self.db_path.exists() {
      debug!(
        tenant = %self.tenant,
        room_id = %self.room_id,
        db_path = %self.db_path.display(),
        "Database already exists, skipping restore"
      );
      return Ok(false);
    }

    // Ensure the database directory exists
    if let Some(parent) = self.db_path.parent() {
      fs::create_dir_all(parent)
        .context("Failed to create database directory")?;
    }

    info!(
      tenant = %self.tenant,
      room_id = %self.room_id,
      db_path = %self.db_path.display(),
      config_path = %self.config_path.display(),
      "Attempting to restore database from S3"
    );

    // Verify config file exists
    if !self.config_path.exists() {
      info!(
        tenant = %self.tenant,
        room_id = %self.room_id,
        config_path = %self.config_path.display(),
        "Config file doesn't exist, generating it"
      );
      // Try to write config file
      if let Err(e) = self.write_config() {
        warn!(
          tenant = %self.tenant,
          room_id = %self.room_id,
          error = %e,
          "Failed to write config file for restore"
        );
      }
    }

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

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
      // Check for the specific "no matching backup" messages which mean there's no backup yet
      if stdout.contains("no matching backups")
        || stderr.contains("no matching backups")
        || stderr.contains("no matching replica")
        || stderr.contains("no generations found")
        || stderr.contains("failed to run")
        || stderr.contains("no matching backups found")
      {
        info!(
          tenant = %self.tenant,
          room_id = %self.room_id,
          stdout = %stdout,
          stderr = %stderr,
          "No existing backup found in S3, creating new database"
        );

        // Create an empty database instead
        if let Err(e) = create_empty_database(&self.db_path) {
          error!(
            tenant = %self.tenant,
            room_id = %self.room_id,
            error = %e,
            db_path = %self.db_path.display(),
            "Failed to create empty database after restore found no backups"
          );
          return Err(e);
        }

        // This wasn't an error, just no backup available yet
        return Ok(false);
      }

      // Any other error is unexpected
      warn!(
        tenant = %self.tenant,
        room_id = %self.room_id,
        status = ?output.status,
        stdout = %stdout,
        stderr = %stderr,
        "Litestream restore failed"
      );

      return Err(anyhow!("Litestream restore failed: {}", stderr));
    }

    info!(
      tenant = %self.tenant,
      room_id = %self.room_id,
      "Successfully restored database from S3"
    );

    // Database was successfully restored
    Ok(true)
  }

  /// Ensure the database is restored, coordinating with other nodes using distributed lock
  #[instrument(skip(self, lock_manager), fields(node_id = %self_node_id, lock_ttl_secs = %lock_ttl.as_secs()))]
  pub async fn ensure_restored(
    &self,
    lock_manager: Arc<dyn distributed_lock::DistributedLock>,
    self_node_id: &str,
    lock_ttl: std::time::Duration,
  ) -> Result<crate::process_manager::RestoreState, anyhow::Error> {
    use crate::process_manager::RestoreState;

    // Check if database already exists locally
    if self.db_path.exists() {
      debug!(
        tenant = %self.tenant,
        room_id = %self.room_id,
        "Database already exists locally, no restore needed"
      );
      return Ok(RestoreState::Complete(false));
    }

    // Create a lock name based on tenant and room_id
    let lock_name = format!("{}:{}", self.tenant, self.room_id);

    // Try to acquire the distributed lock
    debug!(
      tenant = %self.tenant,
      room_id = %self.room_id,
      node_id = %self_node_id,
      "Attempting to acquire distributed lock for restore"
    );

    // Attempt to acquire the lock
    match lock_manager
      .try_acquire(&lock_name, self_node_id, lock_ttl)
      .await
    {
      Ok(lock_handle) => {
        // We got the lock, proceed with restore
        info!(
          tenant = %self.tenant,
          room_id = %self.room_id,
          "Lock acquired, proceeding with database restore"
        );

        // Run the actual restore operation
        let restore_result = match self.run_restore().await {
          Ok(restored) => {
            // Restore succeeded
            info!(
              tenant = %self.tenant,
              room_id = %self.room_id,
              "Database restore completed successfully (restored = {})",
              restored
            );

            // Return Complete state with whether data was actually restored
            RestoreState::Complete(restored)
          }
          Err(e) => {
            // Restore failed
            error!(
              tenant = %self.tenant,
              room_id = %self.room_id,
              error = %e,
              "Database restore failed"
            );

            // Return Failed state with error message
            RestoreState::Failed(e.to_string())
          }
        };

        // Release the lock regardless of restore outcome
        if let Err(e) = lock_manager.release(lock_handle).await {
          warn!(
            tenant = %self.tenant,
            room_id = %self.room_id,
            error = %e,
            "Failed to release distributed lock after restore"
          );
          // Continue anyway, the lock will expire on its own
        } else {
          debug!(
            tenant = %self.tenant,
            room_id = %self.room_id,
            "Successfully released distributed lock after restore"
          );
        }

        // Return the final state
        Ok(restore_result)
      }
      Err(distributed_lock::LockAcquireError::LockHeld(lock_info)) => {
        // Another node holds the lock
        if let Some(info) = lock_info {
          info!(
            tenant = %self.tenant,
            room_id = %self.room_id,
            owner_node_id = %info.node_id,
            acquired_at = %info.timestamp,
            "Distributed lock already held by another node"
          );
        } else {
          info!(
            tenant = %self.tenant,
            room_id = %self.room_id,
            "Distributed lock already held by unknown node"
          );
        }

        // Return WaitingForLock state
        Ok(RestoreState::WaitingForLock)
      }
      Err(e) => {
        // Unexpected error acquiring lock
        error!(
          tenant = %self.tenant,
          room_id = %self.room_id,
          error = %e,
          "Failed to acquire distributed lock for restore"
        );

        // Return Failed state with error message
        Ok(RestoreState::Failed(format!(
          "Lock acquisition failed: {}",
          e
        )))
      }
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

    assert!(!self.tenant.contains('/'));
    assert!(!self.room_id.contains('/'));
    let path = self
      .s3_config
      .subpath(&format!("sqlite/{}/{}", self.tenant, self.room_id));

    let replica = LitestreamS3Replica {
      replica_type: "s3".to_string(),
      name: Some(format!("{}-replica", self.room_id)),
      bucket: self.s3_config.bucket.clone(),
      path,
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

    // Start the replication process with tokio::process::Command
    let child = Command::new("litestream")
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

  /// Waits for replication to flush writes with a timeout
  pub async fn wait_for_flush(
    &self,
    timeout: std::time::Duration,
  ) -> Result<()> {
    info!("Waiting for replication flush for {:?}", self.db_path);

    // For clean shutdown, we'll just pause briefly to allow replication to flush
    // The SIGTERM to the replication process will cause it to flush WAL segments
    tokio::time::sleep(timeout).await;

    debug!(
      "Waited {:?} for replication flush for {:?}",
      timeout, self.db_path
    );
    Ok(())
  }

  /// Kills the replicate process
  pub async fn shutdown(&self) -> Result<()> {
    // Take the child process out of the mutex without holding the lock during the await
    let mut child_opt = {
      let mut process_guard = self.replication_process.lock().unwrap();
      process_guard.take()
    };

    if let Some(ref mut child) = child_opt {
      info!("Shutting down replication for {:?}", self.db_path);

      // Send SIGTERM for graceful shutdown
      if let Some(id) = child.id() {
        let pid = Pid::from_raw(id as i32);
        kill(pid, Signal::SIGTERM)?;
      } else {
        // Process exited already
        debug!("Process already exited");
      }

      // Wait for process to exit (without holding the lock)
      match child.wait().await {
        Ok(status) => {
          debug!("Replication process exited with status: {:?}", status)
        }
        Err(e) => warn!("Error waiting for replication process to exit: {}", e),
      }
    } else {
      debug!("No replication process to shutdown");
    }

    Ok(())
  }
}

/// Creates an empty but valid SQLite database file
pub fn create_empty_database(db_path: &Path) -> Result<()> {
  debug!(path = %db_path.display(), "Creating empty SQLite database file");

  // Make sure parent directory exists
  if let Some(parent) = db_path.parent() {
    if !parent.exists() {
      debug!(path = %parent.display(), "Creating parent directory for database");
      std::fs::create_dir_all(parent).with_context(|| {
        format!(
          "Failed to create parent directory for database: {}",
          parent.display()
        )
      })?;
    }
  }

  // Now create the database
  let output = std::process::Command::new("sqlite3")
    .arg(db_path)
    // https://litestream.io/tips/
    .arg("PRAGMA busy_timeout = 5000; PRAGMA journal_mode=WAL;")
    .output()
    .with_context(|| {
      format!(
        "Failed to execute sqlite3 command for {}",
        db_path.display()
      )
    })?;

  if !output.status.success() {
    let stderr = String::from_utf8_lossy(&output.stderr);
    return Err(anyhow!(
      "sqlite3 command failed with status: {} and error: {}",
      output.status,
      stderr
    ));
  }

  // Verify file was created
  if !db_path.exists() {
    return Err(anyhow!(
      "sqlite3 command appeared to succeed but database file wasn't created at {}", 
      db_path.display()
    ));
  }

  debug!(path = %db_path.display(), "Empty SQLite database file created successfully");
  Ok(())
}

#[cfg(test)]
pub mod tests {
  use super::*;
  use crate::test_utils::MinioTestServer;
  use std::time::Duration;
  use tempfile;

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
      path: Some(format!("roomd-test-{}", test_name)),
      access_key_id: minio.access_key_id.clone(),
      secret_access_key: minio.secret_access_key.clone(),
    }
  }

  #[test]
  fn test_create_empty_database_file() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let result = create_empty_database(&db_path);
    assert!(result.is_ok());
    assert!(db_path.exists(), "Database file should exist");
    let output = std::process::Command::new("sqlite3")
      .arg(&db_path)
      .arg(".tables") // List tables (should return empty but succeed)
      .output()
      .expect("Failed to execute sqlite3 command");
    assert!(output.status.success());
  }

  #[tokio::test]
  async fn basic_replica_lifecycle() {
    let _ = tracing_subscriber::fmt::try_init();

    // Initialize MinIO per-test instance
    let minio_guard = MinioTestServer::start(9001);

    // Use a temporary directory for the test
    let temp_dir = tempfile::TempDir::new().unwrap();
    let data_dir = temp_dir.path();

    let room_id = "basic-replica-lifecycle";
    let s3_config = create_test_s3_config(room_id, &minio_guard);

    // Initialize the SqliteReplica with the new API
    let replica = SqliteReplica::initialize(
      data_dir,
      "test-tenant",
      room_id,
      Some(s3_config),
    )
    .await
    .unwrap()
    .unwrap();

    // Verify config file was created
    let config_path = data_dir
      .join("test-tenant")
      .join("sqlite")
      .join(format!("{}.yml", room_id));
    assert!(config_path.exists(), "Config file should exist");

    // On first run, restore should return false (no restoration)
    // but will create an empty DB file
    let restored = replica.run_restore().await.unwrap();
    assert!(!restored, "No data should be restored on first run");

    // Make sure DB exists after restore operation
    let db_path = replica.db_path().to_string_lossy().to_string();
    assert!(
      std::fs::metadata(&db_path).is_ok(),
      "DB file should exist after restore"
    );

    // Verify DB is accessible, insert test data
    let output = std::process::Command::new("sqlite3")
      .arg(&db_path)
      .arg("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO test VALUES (1, 'test-data');")
      .output()
      .unwrap();

    assert!(
      output.status.success(),
      "SQLite command should succeed: {}",
      String::from_utf8_lossy(&output.stderr)
    );
    println!("Inserted data into SQLite DB {}", db_path);

    // Start replication
    let start_result = replica.start_replication().await;
    assert!(
      start_result.is_ok(),
      "Should start Litestream: {:?}",
      start_result
    );

    // Give the replication process time to run and create snapshots
    println!("Waiting for replication to complete...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Test that wait_for_flush works
    let flush_result = replica.wait_for_flush(Duration::from_millis(500)).await;
    assert!(flush_result.is_ok(), "Wait for flush should succeed");

    // Stop replication cleanly
    let shutdown_result = replica.shutdown().await;
    assert!(
      shutdown_result.is_ok(),
      "Shutdown should succeed: {:?}",
      shutdown_result
    );

    // Check MinIO to ensure data was replicated
    assert!(minio_guard.has_files_for_room(room_id, room_id));

    println!("Basic replica lifecycle test completed successfully");
  }
}
