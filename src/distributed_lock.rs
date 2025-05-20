//! Provides a distributed locking mechanism using S3 objects.
//!
//! ## Purpose
//!
//! In a distributed system like `celld` where multiple nodes might operate on
//! shared resources or need to coordinate actions, a distributed lock is
//! crucial to prevent race conditions and ensure data consistency. This module
//! implements such a lock, specifically tailored for controlling access during
//! critical, potentially long-running operations like restoring SQLite
//! databases from backups.
//!
//! ## Mechanism: S3-based Locking
//!
//! This implementation leverages Amazon S3 (or compatible services like MinIO)
//! as a coordination point. It uses S3 objects as "lock files":
//!
//! 1.  **Atomic Acquisition:** A lock is acquired by attempting to create a
//!     specific S3 object using a conditional `PutObject` request
//!     (`If-None-Match: *`). This ensures that only the first node to
//!     successfully create the object acquires the lock.
//! 2.  **Lock Information:** The content of the lock object (`LockInfo`) stores
//!     metadata about the lock holder, including the holding `node_id`, a
//!     `timestamp` of acquisition, and a Time-To-Live (`ttl_secs`).
//! 3.  **Lease/Expiry:** Locks have a TTL. If a node attempts to acquire a lock
//!     that already exists, it checks the lock object's timestamp and TTL. If
//!     the lock has expired (presumably because the previous holder crashed or
//!     failed), the attempting node can try to delete the stale lock and
//!     acquire it.
//! 4.  **Release:** The lock is explicitly released by deleting the
//!     corresponding S3 object.
//!
//! ## Context in `celld`: Coordinating SQLite Restores
//!
//! The primary use case within `celld` is to coordinate the `litestream
//! restore` process for SQLite databases associated with specific cells
//! (`tenant`/`cell_id`). When a `celld` node needs to potentially restore a
//! database (e.g., on cold start or node takeover), it must first acquire the
//! distributed lock for that specific database.
//!
//! - **Prevents Conflicts:** This ensures that only one node actively restores
//!   a given database at any time, preventing multiple nodes from writing to the
//!   same local database file simultaneously or performing redundant restore
//!   operations.
//! - **Failure Handling:** The lock TTL helps recover from scenarios where a
//!   node acquires a lock but crashes before releasing it.
//!
//! ### Example S3 Lock Path
//!
//! The specific S3 key for a lock is determined by a configured prefix and a
//! hash of the unique resource being locked (typically the tenant and cell ID
//! combined).
//!
//! For example, if the S3 bucket is `my-celld-state` and the lock prefix is
//! configured as `cluster_state/locks/restore/`, acquiring a lock for the
//! database corresponding to tenant `my-app.localhost` and cell
//! `user-session-abc` might result in an attempt to atomically create an S3
//! object like:
//!
//! ```text
//! s3://my-celld-state/cluster_state/locks/restore/f8a3b1e4c9d0...{hash_of_"my-app.localhost/user-session-abc"}...e5f6a7b8.lock
//! ```
//!
//! The content of this object would be a JSON representation of the `LockInfo`
//! struct.
//!
//! ## Usage
//!
//! This module defines the `DistributedLock` trait and provides the
//! `S3DistributedLock` implementation. Components like `SqliteReplica` or
//! `ProcessManager` use an instance conforming to this trait (typically
//! injected via `NodeState`) to acquire and release locks before performing
//! operations like `litestream restore`.

use anyhow::{Context, Error as AnyhowError};
use async_trait::async_trait;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error, info, warn};

use crate::cluster_membership::NodeId;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LockInfo {
  pub node_id: NodeId,
  #[serde(with = "chrono::serde::ts_seconds")]
  pub timestamp: DateTime<Utc>,
  pub ttl_secs: u64,
}

#[derive(Debug, Error)]
pub enum LockAcquireError {
  #[error("Lock already held: {0:?}")]
  LockHeld(Option<LockInfo>),
  #[error("S3 operation failed: {0}")]
  S3Error(String),
  #[error("Failed to serialize or deserialize lock data: {0}")]
  SerdeError(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct LockHandle {
  pub lock_key: String,
  node_id: NodeId,
}

// Should not be Clone
pub struct LockGuard {
  lock_key: String,
  node_id: NodeId,
  /// Reference back to the manager to call release
  /// Use Arc if the manager itself is shared via Arc
  lock_manager: Arc<dyn DistributedLock>,
  /// If present, this guard will notify the given channel when the lock is released.
  release_notifier:
    Option<tokio::sync::oneshot::Sender<Result<(), anyhow::Error>>>,
  /// A channel to request a ttl renewal for the lock
  ttl_renewal_request_chan: tokio::sync::mpsc::Sender<Duration>,
  /// A tokio task that waits for ttl renewal requests and performs the renewal
  ttl_update_task: tokio::task::JoinHandle<()>,
}

impl LockGuard {
  fn new(
    lock_key: String,
    node_id: NodeId,
    lock_manager: Arc<dyn DistributedLock>,
  ) -> Self {
    let (ttl_renewal_request_tx, mut ttl_renewal_request_rx) =
      tokio::sync::mpsc::channel(1);

    let ttl_update_task = tokio::spawn({
      let lock_manager = lock_manager.clone();
      let handle = LockHandle {
        lock_key: lock_key.clone(),
        node_id: node_id.clone(),
      };

      async move {
        while let Some(new_ttl) = ttl_renewal_request_rx.recv().await {
          // Call the underlying lock manager's renew function
          match lock_manager.renew(handle.clone(), new_ttl).await {
            Ok(_) => {
              // Successfully renewed. The LockGuard remains valid.
              // The timestamp/TTL was updated in S3 by the lock manager.
              debug!(
                lock_key = handle.lock_key,
                "Successfully renewed lock key"
              );
            }
            Err(lock_err) => {
              // Renewal failed. The lock might be lost.
              error!(
                error = ?lock_err,
                lock_key = handle.lock_key,
                "Failed to renew lock key"
              );
              // TODO: Maybe we should report the error to the renewal requester in some way
              return;
            }
          }
        }
      }
    });

    Self {
      lock_key,
      node_id,
      lock_manager,
      release_notifier: None,
      ttl_renewal_request_chan: ttl_renewal_request_tx,
      ttl_update_task,
    }
  }

  /// Request a ttl renewal for the lock. If there is already a renewal request enqueued, no action is taken.
  pub fn request_ttl_renewal(&self, new_ttl: Duration) {
    if let Err(e) = self.ttl_renewal_request_chan.try_send(new_ttl) {
      warn!(
        lock_key = %self.lock_key,
        node_id = ?self.node_id,
        error = ?e,
        "Failed to request lock ttl renewal"
      );
    }
  }

  /// Set a notifier that will be notified when the lock is released.
  pub fn set_release_notifier(
    &mut self,
    notifier: tokio::sync::oneshot::Sender<Result<(), anyhow::Error>>,
  ) {
    self.release_notifier = Some(notifier);
  }
}

impl fmt::Debug for LockGuard {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("LockGuard")
      .field("lock_key", &self.lock_key)
      .field("node_id", &self.node_id)
      // Since Arc<dyn DistributedLock> isn't Debug, provide a placeholder
      .field("lock_manager", &"<DistributedLock Manager>")
      .finish() // Complete the struct formatting
  }
}

/// Release the lock in the background task on drop.
impl Drop for LockGuard {
  fn drop(&mut self) {
    self.ttl_update_task.abort();

    let handle = LockHandle {
      lock_key: self.lock_key.clone(),
      node_id: self.node_id.clone(),
    };
    let lock_manager = self.lock_manager.clone();
    let release_notifier = self.release_notifier.take();

    tracing::debug!(
      lock_key = %self.lock_key,
      node_id = ?self.node_id,
      "Dropping LockGuard, releasing lock"
    );

    // It's okay not even to catch any errors with the release here since the
    // lock will time out by itself eventually after the TTL expires.
    tokio::spawn(async move {
      let release_result = lock_manager.release(handle).await;
      if let Some(notifier) = release_notifier {
        if let Err(e) = notifier.send(release_result) {
          tracing::error!(
            error = ?e,
            "Error sending release notification",
          );
        }
      }
      tracing::debug!("LockGuard dropped, lock released");
    });
  }
}

#[async_trait]
pub trait DistributedLock: Send + Sync {
  /// Attempts to atomically acquire a distributed lock for a resource.
  async fn try_acquire(
    self: Arc<Self>,
    lock_name: &str,
    node_id: &NodeId,
    ttl: Duration,
  ) -> Result<LockGuard, LockAcquireError>;

  /// Releases a previously acquired distributed lock.
  async fn release(&self, handle: LockHandle) -> Result<(), AnyhowError>;

  /// Renews an existing lock by updating its timestamp and TTL.
  /// This extends the lock's expiration time without releasing and re-acquiring it.
  ///
  /// Returns a new LockHandle with the updated information on success.
  /// Fails if the lock doesn't exist or is held by a different node.
  #[allow(dead_code)]
  async fn renew(
    &self,
    handle: LockHandle,
    new_ttl: Duration,
  ) -> Result<LockHandle, LockAcquireError>;
}

pub struct S3DistributedLock {
  s3_client: S3Client,
  bucket: String,
  prefix: String,
}

impl S3DistributedLock {
  pub fn new(s3_client: S3Client, bucket: String, mut prefix: String) -> Self {
    if !prefix.is_empty() && !prefix.ends_with('/') {
      prefix.push('/');
    }
    if prefix.is_empty() {
      prefix = "cluster_state/locks/restore/".to_string();
    }
    Self {
      s3_client,
      bucket,
      prefix,
    }
  }

  fn get_lock_key(&self, lock_name: &str) -> String {
    let hash = Sha256::digest(lock_name.as_bytes());
    format!("{}{:x}.lock", self.prefix, hash)
  }
}

#[async_trait]
impl DistributedLock for S3DistributedLock {
  async fn try_acquire(
    self: Arc<Self>,
    lock_name: &str,
    node_id: &NodeId,
    ttl: Duration,
  ) -> Result<LockGuard, LockAcquireError> {
    let lock_key = self.get_lock_key(lock_name);
    debug!(lock_key, ?node_id, ?ttl, "Attempting to acquire S3 lock");

    let lock_info = LockInfo {
      node_id: node_id.clone(),
      timestamp: Utc::now(),
      ttl_secs: ttl.as_secs(),
    };

    let body_bytes = match serde_json::to_vec(&lock_info) {
      Ok(bytes) => bytes,
      Err(e) => {
        warn!(error = %e, lock_key, "Failed to serialize lock info");
        return Err(LockAcquireError::SerdeError(e));
      }
    };

    let put_result = self
      .s3_client
      .put_object()
      .bucket(&self.bucket)
      .key(&lock_key)
      .body(ByteStream::from(body_bytes.clone()))
      .if_none_match("*")
      .send()
      .await;

    match put_result {
      Ok(_) => {
        info!(lock_key, ?node_id, "Successfully acquired S3 lock");
        Ok(LockGuard::new(lock_key, node_id.clone(), self))
      }
      Err(SdkError::ServiceError(service_err)) => {
        let raw_err = service_err.into_err();
        if raw_err.code() == Some("PreconditionFailed") {
          warn!(lock_key, ?node_id, "Lock acquisition failed (precondition failed), checking if existing lock expired");

          match self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(&lock_key)
            .send()
            .await
          {
            Ok(get_output) => {
              let body = match get_output.body.collect().await {
                Ok(agg) => agg.into_bytes(),
                Err(e) => {
                  warn!(error = ?e, lock_key, "Failed to read existing lock object body");
                  return Err(LockAcquireError::S3Error(format!(
                    "Failed to read lock body for key '{}': {:?}",
                    lock_key, e
                  )));
                }
              };
              let existing_lock_info: LockInfo = match serde_json::from_slice(
                &body,
              ) {
                Ok(info) => info,
                Err(e) => {
                  warn!(error = ?e, lock_key, "Failed to deserialize existing lock data");
                  return Err(LockAcquireError::SerdeError(e));
                }
              };

              let expiry_time = existing_lock_info.timestamp
                + Duration::from_secs(existing_lock_info.ttl_secs);
              if expiry_time < Utc::now() {
                warn!(lock_key, existing_node_id = ?existing_lock_info.node_id, ?expiry_time, "Existing lock expired, attempting to delete and re-acquire");

                if let Err(e) = self
                  .s3_client
                  .delete_object()
                  .bucket(&self.bucket)
                  .key(&lock_key)
                  .send()
                  .await
                {
                  if let SdkError::ServiceError(del_err_sdk) = e {
                    let del_err = del_err_sdk.into_err();
                    if del_err.code() != Some("NoSuchKey") {
                      warn!(error = ?del_err, lock_key, "Failed to delete expired lock, acquisition fails");
                      return Err(LockAcquireError::S3Error(format!(
                        "Failed to delete expired lock for key '{}': {:?}",
                        lock_key, del_err
                      )));
                    } else {
                      warn!(lock_key, "Expired lock was already deleted, proceeding to retry put");
                    }
                  } else {
                    return Err(LockAcquireError::S3Error(format!(
                      "Failed to delete expired lock for key '{}' (SDK Error): {:?}",
                      lock_key, e
                    )));
                  }
                }

                info!(
                  lock_key,
                  ?node_id,
                  "Retrying lock acquisition after deleting expired lock"
                );
                match self
                  .s3_client
                  .put_object()
                  .bucket(&self.bucket)
                  .key(&lock_key)
                  .body(ByteStream::from(body_bytes))
                  .if_none_match("*")
                  .send()
                  .await
                {
                  Ok(_) => {
                    info!(
                      lock_key,
                      ?node_id,
                      "Successfully acquired S3 lock on retry"
                    );
                    Ok(LockGuard::new(lock_key, node_id.clone(), self))
                  }
                  Err(SdkError::ServiceError(retry_service_err)) => {
                    let retry_put_err = retry_service_err.into_err();
                    if retry_put_err.code() == Some("PreconditionFailed") {
                      warn!(
                        lock_key,
                        ?node_id,
                        "Lock was acquired by another node during retry"
                      );
                      return Err(LockAcquireError::LockHeld(None));
                    }
                    Err(LockAcquireError::S3Error(format!(
                      "Retry put failed for key '{}': {:?}",
                      lock_key, retry_put_err
                    )))
                  }
                  Err(e) => Err(LockAcquireError::S3Error(format!(
                    "Retry put failed for key '{}' (SDK Error): {:?}",
                    lock_key, e
                  ))),
                }
              } else {
                info!(lock_key, existing_node_id = ?existing_lock_info.node_id, ?expiry_time, "Existing lock is still valid");
                Err(LockAcquireError::LockHeld(Some(existing_lock_info)))
              }
            }
            Err(SdkError::ServiceError(get_service_err)) => {
              let get_err = get_service_err.into_err();
              if get_err.code() == Some("NoSuchKey") {
                warn!(
                  lock_key,
                  ?node_id,
                  "Lock disappeared between Put failure and Get, retrying Put"
                );
                match self
                  .s3_client
                  .put_object()
                  .bucket(&self.bucket)
                  .key(&lock_key)
                  .body(ByteStream::from(body_bytes))
                  .if_none_match("*")
                  .send()
                  .await
                {
                  Ok(_) => {
                    info!(
                      lock_key,
                      ?node_id,
                      "Successfully acquired S3 lock on retry (after NoSuchKey)"
                    );
                    Ok(LockGuard::new(lock_key, node_id.clone(), self))
                  }
                  Err(SdkError::ServiceError(retry_service_err)) => {
                    let retry_put_err = retry_service_err.into_err();
                    if retry_put_err.code() == Some("PreconditionFailed") {
                      warn!(lock_key, ?node_id, "Lock was acquired by another node during retry (after NoSuchKey)");
                      return Err(LockAcquireError::LockHeld(None));
                    }
                    Err(LockAcquireError::S3Error(format!(
                      "Retry put failed after NoSuchKey for key '{}': {:?}",
                      lock_key, retry_put_err
                    )))
                  }
                  Err(e) => Err(LockAcquireError::S3Error(format!(
                    "Retry put failed after NoSuchKey for key '{}' (SDK Error): {:?}",
                    lock_key, e
                  ))),
                }
              } else {
                warn!(error = ?get_err, lock_key, "Failed to get existing lock details");
                Err(LockAcquireError::S3Error(format!(
                  "Failed to get lock for key '{}': {:?}",
                  lock_key, get_err
                )))
              }
            }
            Err(e) => {
              warn!(error = ?e, lock_key, "SDK error during get existing lock details");
              Err(LockAcquireError::S3Error(format!(
                "SDK error getting lock for key '{}': {:?}",
                lock_key, e
              )))
            }
          }
        } else {
          warn!(error = ?raw_err, lock_key, ?node_id, "Unhandled S3 PutObject error during lock acquisition");
          Err(LockAcquireError::S3Error(format!(
            "Unhandled S3 PutObject error for key '{}': {:?}",
            lock_key, raw_err
          )))
        }
      }
      Err(e) => {
        warn!(error = ?e, lock_key, ?node_id, "SDK error during lock acquisition");
        let source_msg = e
          .source()
          .map_or_else(|| "Unknown source".to_string(), |s| s.to_string());
        Err(LockAcquireError::S3Error(format!(
          "SDK Error for key '{}': {} (Source: {})",
          lock_key, e, source_msg
        )))
      }
    }
  }

  async fn release(&self, handle: LockHandle) -> Result<(), AnyhowError> {
    debug!(lock_key = %handle.lock_key, node_id = ?handle.node_id, "Releasing S3 lock");
    match self
      .s3_client
      .delete_object()
      .bucket(&self.bucket)
      .key(&handle.lock_key)
      .send()
      .await
    {
      Ok(_) => {
        info!(lock_key = %handle.lock_key, node_id = ?handle.node_id, "Successfully released S3 lock");
        Ok(())
      }
      Err(SdkError::ServiceError(service_err)) => {
        let del_err = service_err.into_err();
        if del_err.code() == Some("NoSuchKey") {
          warn!(lock_key = %handle.lock_key, node_id = ?handle.node_id, "Attempted to release a lock that does not exist (or was already released)");
          Ok(())
        } else {
          warn!(error = ?del_err, lock_key = %handle.lock_key, node_id = ?handle.node_id, "Failed to release S3 lock (Service Error)");
          Err(AnyhowError::new(del_err))
            .context(format!("Failed to release S3 lock: {}", handle.lock_key))
        }
      }
      Err(e) => {
        warn!(error = ?e, lock_key = %handle.lock_key, node_id = ?handle.node_id, "Failed to release S3 lock (SDK Error)");
        Err(AnyhowError::new(e))
          .context(format!("SDK Error releasing S3 lock: {}", handle.lock_key))
      }
    }
  }

  async fn renew(
    &self,
    handle: LockHandle,
    new_ttl: Duration,
  ) -> Result<LockHandle, LockAcquireError> {
    let lock_key = &handle.lock_key;
    let node_id = &handle.node_id;

    debug!(lock_key, ?node_id, ?new_ttl, "Attempting to renew S3 lock");

    // First check if we actually own this lock
    match self
      .s3_client
      .get_object()
      .bucket(&self.bucket)
      .key(lock_key)
      .send()
      .await
    {
      Ok(get_output) => {
        // Capture the ETag for conditional update
        let etag = get_output.e_tag.clone();

        // Read the current lock info
        let body = match get_output.body.collect().await {
          Ok(agg) => agg.into_bytes(),
          Err(e) => {
            warn!(error = ?e, lock_key, "Failed to read existing lock object body during renewal");
            return Err(LockAcquireError::S3Error(format!(
              "Failed to read lock body for key '{}' during renewal: {:?}",
              lock_key, e
            )));
          }
        };

        let existing_lock_info: LockInfo = match serde_json::from_slice(&body) {
          Ok(info) => info,
          Err(e) => {
            warn!(error = ?e, lock_key, "Failed to deserialize existing lock data during renewal");
            return Err(LockAcquireError::SerdeError(e));
          }
        };

        // Verify this is our lock
        if existing_lock_info.node_id != *node_id {
          warn!(
            lock_key,
            attempted_node_id = ?node_id,
            actual_node_id = ?existing_lock_info.node_id,
            "Cannot renew lock owned by different node"
          );
          return Err(LockAcquireError::LockHeld(Some(existing_lock_info)));
        }

        // Create updated lock info with new timestamp and TTL
        let updated_lock_info = LockInfo {
          node_id: node_id.clone(),
          timestamp: Utc::now(),
          ttl_secs: new_ttl.as_secs(),
        };

        // Serialize the updated lock info
        let body_bytes = match serde_json::to_vec(&updated_lock_info) {
          Ok(bytes) => bytes,
          Err(e) => {
            warn!(error = %e, lock_key, "Failed to serialize updated lock info during renewal");
            return Err(LockAcquireError::SerdeError(e));
          }
        };

        // Update the lock object with new info
        // Use conditional put with the ETag to prevent race conditions
        let put_request = self
          .s3_client
          .put_object()
          .bucket(&self.bucket)
          .key(lock_key)
          .body(ByteStream::from(body_bytes));

        // Only add the if_match condition if we have an ETag
        let put_request = if let Some(tag) = etag {
          put_request.if_match(tag)
        } else {
          put_request
        };

        match put_request.send().await {
          Ok(_) => {
            info!(lock_key, ?node_id, ?new_ttl, "Successfully renewed S3 lock");
            Ok(LockHandle {
              lock_key: lock_key.clone(),
              node_id: node_id.clone(),
            })
          }
          Err(SdkError::ServiceError(service_err)) => {
            let err = service_err.into_err();
            // Check if this was a precondition failure (ETag mismatch)
            if err.code() == Some("PreconditionFailed") {
              warn!(
                lock_key,
                ?node_id,
                "Failed to renew lock: object was modified between read and update"
              );
              return Err(LockAcquireError::S3Error(format!(
                "Lock object for key '{}' was modified between ownership check and renewal",
                lock_key
              )));
            }

            warn!(error = ?err, lock_key, ?node_id, "Failed to update lock object during renewal");
            Err(LockAcquireError::S3Error(format!(
              "Failed to update lock object for key '{}' during renewal: {:?}",
              lock_key, err
            )))
          }
          Err(e) => {
            warn!(error = ?e, lock_key, ?node_id, "Failed to update lock object during renewal");
            Err(LockAcquireError::S3Error(format!(
              "Failed to update lock object for key '{}' during renewal: {:?}",
              lock_key, e
            )))
          }
        }
      }
      Err(SdkError::ServiceError(service_err)) => {
        let get_err = service_err.into_err();
        if get_err.code() == Some("NoSuchKey") {
          warn!(lock_key, ?node_id, "Cannot renew lock: lock does not exist");
          Err(LockAcquireError::S3Error(format!(
            "Cannot renew lock for key '{}': lock does not exist",
            lock_key
          )))
        } else {
          warn!(error = ?get_err, lock_key, ?node_id, "Failed to get lock during renewal");
          Err(LockAcquireError::S3Error(format!(
            "Failed to get lock for key '{}' during renewal: {:?}",
            lock_key, get_err
          )))
        }
      }
      Err(e) => {
        warn!(error = ?e, lock_key, ?node_id, "SDK error during lock renewal");
        Err(LockAcquireError::S3Error(format!(
          "SDK error during renewal of lock for key '{}': {:?}",
          lock_key, e
        )))
      }
    }
  }
}

pub struct StandaloneDistributedLock;

#[async_trait]
impl DistributedLock for StandaloneDistributedLock {
  async fn try_acquire(
    self: Arc<Self>,
    lock_name: &str,
    node_id: &NodeId,
    _ttl: Duration,
  ) -> Result<LockGuard, LockAcquireError> {
    Ok(LockGuard::new(lock_name.to_string(), node_id.clone(), self))
  }

  async fn release(&self, _handle: LockHandle) -> Result<(), AnyhowError> {
    Ok(())
  }

  async fn renew(
    &self,
    handle: LockHandle,
    _new_ttl: Duration,
  ) -> Result<LockHandle, LockAcquireError> {
    Ok(handle)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::MinioTestServer;
  use aws_config::meta::credentials::CredentialsProviderChain;
  use aws_sdk_s3::config::{Credentials, Region};
  use aws_sdk_s3::Client;

  use std::sync::Arc;
  use std::time::Duration;
  use tokio::time::sleep;

  async fn setup_test_env() -> (Arc<S3DistributedLock>, String, MinioTestServer)
  {
    let minio = MinioTestServer::start();
    let bucket = "test-lock-bucket".to_string();
    minio
      .create_bucket(&bucket)
      .expect("Failed to create bucket");

    let endpoint_url = format!("http://127.0.0.1:{}", minio.port);

    // Create credentials from MinIO server
    let credentials = Credentials::new(
      minio.access_key_id.clone(),
      minio.secret_access_key.clone(),
      None,
      None,
      "manual-minio",
    );
    let credentials_provider =
      CredentialsProviderChain::first_try("manual-minio", credentials);

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
      .endpoint_url(&endpoint_url)
      .region(Region::new("us-east-1"))
      .credentials_provider(credentials_provider)
      .load()
      .await;

    // Force path-style addressing for S3 (needed for MinIO)
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
      .force_path_style(true)
      .build();

    let s3_client = Client::from_conf(s3_config);

    let prefix = "test_locks".to_string();
    let lock_manager =
      S3DistributedLock::new(s3_client.clone(), bucket.clone(), prefix);

    // Return a more concise tuple - endpoint not used in tests
    (Arc::new(lock_manager), bucket, minio)
  }

  async fn s3_object_exists(client: &Client, bucket: &str, key: &str) -> bool {
    client
      .head_object()
      .bucket(bucket)
      .key(key)
      .send()
      .await
      .is_ok()
  }

  #[test_log::test(tokio::test)]
  async fn test_acquire_release() {
    let (lock_manager, bucket, _minio) = setup_test_env().await;

    let lock_name = "test_lock_1";
    let node_id = NodeId::new("node_a");
    let ttl = Duration::from_secs(60);

    let mut guard = lock_manager
      .clone()
      .try_acquire(lock_name, &node_id, ttl)
      .await
      .expect("Failed to acquire lock");

    assert!(
      s3_object_exists(&lock_manager.s3_client, &bucket, &guard.lock_key).await,
      "Lock object should exist after acquire"
    );

    let (tx, rx) = tokio::sync::oneshot::channel();
    guard.set_release_notifier(tx);

    drop(guard);

    // Wait until the release is complete
    rx.await.unwrap().unwrap();

    let lock_key_after_release = lock_manager.get_lock_key(lock_name);
    assert!(
      !s3_object_exists(
        &lock_manager.s3_client,
        &bucket,
        &lock_key_after_release
      )
      .await,
      "Lock object should not exist after release"
    );
  }

  #[test_log::test(tokio::test)]
  async fn test_acquire_fails_if_held() {
    let (lock_manager, _bucket, _minio) = setup_test_env().await;

    let lock_name = "test_lock_2";
    let node_a = NodeId::new("node_a");
    let node_b = NodeId::new("node_b");
    let ttl = Duration::from_secs(60);

    let mut guard_a = lock_manager
      .clone()
      .try_acquire(lock_name, &node_a, ttl)
      .await
      .expect("Node A failed to acquire lock");

    let (tx, rx) = tokio::sync::oneshot::channel();
    guard_a.set_release_notifier(tx);

    let guard_b = lock_manager
      .clone()
      .try_acquire(lock_name, &node_b, ttl)
      .await;

    match guard_b {
      Err(LockAcquireError::LockHeld(Some(info))) => {
        assert_eq!(info.node_id, node_a);
      }
      _ => panic!("Node B should have failed with LockHeld, got {:?}", guard_b),
    }

    drop(guard_a);

    // Wait until the release is complete
    rx.await.unwrap().unwrap();

    // Now node B should be able to acquire the lock
    let mut guard_b = lock_manager
      .try_acquire(lock_name, &node_b, ttl)
      .await
      .expect("Node B failed to acquire lock after release");

    let (tx, rx) = tokio::sync::oneshot::channel();
    guard_b.set_release_notifier(tx);

    drop(guard_b);

    // Wait until the release is complete and ensure it succeeded
    rx.await.unwrap().unwrap();
  }

  #[test_log::test(tokio::test)]
  async fn test_acquire_succeeds_if_expired() {
    let (lock_manager, _bucket, _minio) = setup_test_env().await;

    let lock_name = "test_lock_3";
    let node_a = NodeId::new("node_a");
    let node_b = NodeId::new("node_b");
    let short_ttl = Duration::from_secs(2);
    let long_ttl = Duration::from_secs(60);

    let mut guard_a = lock_manager
      .clone()
      .try_acquire(lock_name, &node_a, short_ttl)
      .await
      .expect("Node A failed to acquire lock with short TTL");

    let (tx_guard_a, rx_guard_a) = tokio::sync::oneshot::channel();
    guard_a.set_release_notifier(tx_guard_a);

    sleep(short_ttl + Duration::from_secs(1)).await;

    let mut guard_b = lock_manager
      .try_acquire(lock_name, &node_b, long_ttl)
      .await
      .expect("Node B failed to acquire expired lock");

    let (tx_guard_b, rx_guard_b) = tokio::sync::oneshot::channel();
    guard_b.set_release_notifier(tx_guard_b);

    assert_eq!(guard_b.node_id, node_b);

    drop(guard_a);
    // Wait until the release of guard_a is complete
    let release_a_result = rx_guard_a.await.unwrap();

    assert!(
      release_a_result.is_ok(),
      "Releasing stale handle should be okay (lock already gone or owned by B)"
    );

    drop(guard_b);
    // Wait until the release of guard_b is complete and ensure it succeeded
    rx_guard_b.await.unwrap().unwrap();
  }

  #[test_log::test(tokio::test)]
  async fn test_release_non_existent_lock() {
    let (lock_manager, _bucket, _minio) = setup_test_env().await;

    let lock_name = "test_lock_non_existent";
    let node_id = NodeId::new("node_a");

    let fake_handle = LockHandle {
      lock_key: lock_manager.get_lock_key(lock_name),
      node_id,
    };

    let result = lock_manager.release(fake_handle).await;

    assert!(
      result.is_ok(),
      "Releasing a non-existent lock should succeed, got {:?}",
      result
    );
  }

  #[test_log::test(tokio::test)]
  async fn test_concurrent_acquisition_attempts() {
    let (lock_manager, _bucket, _minio) = setup_test_env().await;

    let lock_name = "test_lock_concurrent";
    let node_ids = vec![
      NodeId::new("node_1"),
      NodeId::new("node_2"),
      NodeId::new("node_3"),
    ];
    let ttl = Duration::from_secs(10);

    let mut handles = vec![];
    for node_id in &node_ids {
      let lock_manager_ = lock_manager.clone();
      let node_id = node_id.clone();
      let handle = tokio::spawn(async move {
        lock_manager_.try_acquire(lock_name, &node_id, ttl).await
      });
      handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;

    let mut success_count = 0;
    let mut held_count = 0;
    let mut acquired_guard: Option<LockGuard> = None;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut tx = Some(tx);

    for result in results {
      match result {
        Ok(Ok(mut guard)) => {
          success_count += 1;
          if let Some(tx) = tx.take() {
            guard.set_release_notifier(tx);
          }
          acquired_guard = Some(guard);
        }
        Ok(Err(LockAcquireError::LockHeld(_))) => {
          held_count += 1;
        }
        Ok(Err(e)) => {
          panic!("Unexpected lock acquisition error: {:?}", e);
        }
        Err(e) => {
          panic!("Join error: {:?}", e);
        }
      }
    }

    assert_eq!(success_count, 1, "Exactly one node should acquire the lock");
    assert_eq!(
      held_count,
      node_ids.len() - 1,
      "All other nodes should fail with LockHeld"
    );

    if let Some(guard) = acquired_guard {
      drop(guard);
      // Wait until the release is complete and ensure it succeeded
      rx.await.unwrap().unwrap();
    }
  }

  #[test_log::test(tokio::test)]
  async fn test_lock_renewal() {
    let (lock_manager, bucket, _minio) = setup_test_env().await;

    let lock_name = "test_lock_renewal";
    let node_id = NodeId::new("node_a");
    let initial_ttl = Duration::from_secs(10);
    let new_ttl = Duration::from_secs(60);

    // First acquire the lock
    let mut guard = lock_manager
      .clone()
      .try_acquire(lock_name, &node_id, initial_ttl)
      .await
      .expect("Failed to acquire lock initially");

    let (tx, rx) = tokio::sync::oneshot::channel();
    guard.set_release_notifier(tx);

    // Check lock exists
    assert!(
      s3_object_exists(&lock_manager.s3_client, &bucket, &guard.lock_key).await,
      "Lock object should exist after acquire"
    );

    // Get the initial lock info to verify timestamps later
    let initial_lock_info =
      get_lock_info(&lock_manager.s3_client, &bucket, &guard.lock_key)
        .await
        .expect("Failed to get initial lock info");

    // Sleep a bit to ensure timestamp will be different
    sleep(Duration::from_secs(1)).await; // Increased sleep time to ensure timestamp difference

    // Renew the lock with a longer TTL
    guard.request_ttl_renewal(new_ttl);

    // Wait for the renewal to complete
    // TODO(magurotuna): introduce more reliable, less flaky way to wait for the renewal to complete
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Lock should still exist
    assert!(
      s3_object_exists(&lock_manager.s3_client, &bucket, &guard.lock_key).await,
      "Lock object should still exist after renewal"
    );

    // Get the updated lock info
    let renewed_lock_info =
      get_lock_info(&lock_manager.s3_client, &bucket, &guard.lock_key)
        .await
        .expect("Failed to get renewed lock info");

    // Print timestamps for debugging
    println!("Initial timestamp: {:?}", initial_lock_info.timestamp);
    println!("Renewed timestamp: {:?}", renewed_lock_info.timestamp);

    // Verify the TTL was updated
    assert_eq!(
      renewed_lock_info.ttl_secs,
      new_ttl.as_secs(),
      "TTL should be updated to the new value"
    );

    // Verify the timestamp was updated (renewed)
    assert!(
      renewed_lock_info.timestamp > initial_lock_info.timestamp,
      "Lock timestamp should be updated during renewal"
    );

    // Verify the owner stayed the same
    assert_eq!(
      renewed_lock_info.node_id, node_id,
      "Lock should still be owned by the same node after renewal"
    );

    // Test that another node can't renew our lock
    let different_node_id = NodeId::new("node_b");
    let invalid_handle = LockHandle {
      lock_key: guard.lock_key.clone(),
      node_id: different_node_id,
    };

    match lock_manager.renew(invalid_handle, new_ttl).await {
      Err(LockAcquireError::LockHeld(Some(info))) => {
        assert_eq!(
          info.node_id, node_id,
          "Error should indicate the real lock owner"
        );
      }
      other => panic!("Expected LockHeld error, got {:?}", other),
    }

    // Wait until the release is complete and ensure it succeeded
    drop(guard);
    rx.await.unwrap().unwrap();
  }

  // Helper function to get lock info from S3
  async fn get_lock_info(
    client: &Client,
    bucket: &str,
    key: &str,
  ) -> Result<LockInfo, String> {
    match client.get_object().bucket(bucket).key(key).send().await {
      Ok(output) => match output.body.collect().await {
        Ok(agg) => {
          match serde_json::from_slice::<LockInfo>(&agg.into_bytes()) {
            Ok(info) => Ok(info),
            Err(e) => Err(format!("Failed to deserialize lock info: {}", e)),
          }
        }
        Err(e) => Err(format!("Failed to read lock body: {}", e)),
      },
      Err(e) => Err(format!("Failed to get lock object: {}", e)),
    }
  }
}
