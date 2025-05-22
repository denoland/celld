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
use std::ops::DerefMut as _;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::cluster_membership::NodeId;
use crate::extendable_timer::spawn_extendable_timer;
use crate::process_manager::ProcessEntry;

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
  #[error("Unable to renew expired lock: {0:?}")]
  UnableToRenewExpiredLock(Option<LockInfo>),
  #[error("S3 operation failed: {0}")]
  S3Error(String),
  #[error("Failed to serialize or deserialize lock data: {0}")]
  SerdeError(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct LockDescriptor {
  pub lock_key: String,
  node_id: NodeId,
}

#[derive(Debug)]
pub enum LockReleaseReason {
  /// The lock was released because the lock was explicitly released
  ExplicitRelease,
  /// The lock was released because the ttl expired
  TTLExpired,
  /// The lock was released because the ttl renewal failed
  TTLRenewalFailed,
}

/// TODO: Ideally this should be a generic over the protected resource type `T`.
/// But doing this would make `DistributedLock` not dyn-compatible, so here we
/// hardcode the resource type as `ProcessEntry` for now.
#[derive(Debug, Clone)]
pub struct LockHandle {
  descriptor: LockDescriptor,
  guard: Arc<Mutex<LockGuard>>,
}

/// Should not be Clone
///
/// TODO: Ideally this should be a generic over the protected resource type `T`.
/// But doing this would make `DistributedLock` not dyn-compatible, so here we
/// hardcode the resource type as `ProcessEntry` for now.
enum LockGuard {
  /// Lock is acquired, but the TTL renewal task has not started and resource is
  /// not yet populated
  Init {
    descriptor: LockDescriptor,
    lock_manager: Arc<dyn DistributedLock>,
  },
  /// Lock is acquired, but the resource is not yet populated
  TtlRenewalTaskStarted {
    descriptor: LockDescriptor,
    lock_manager: Arc<dyn DistributedLock>,
    ttl_renewal_task: JoinHandle<()>,
  },
  /// Lock is acquired, and the resource is populated
  Active {
    descriptor: LockDescriptor,
    /// Reference back to the manager to call release
    /// Use Arc if the manager itself is shared via Arc
    lock_manager: Arc<dyn DistributedLock>,
    ttl_renewal_task: JoinHandle<()>,
    protected_resource: ProcessEntry,
  },
  /// Lock has been released
  Released {
    descriptor: LockDescriptor,
    reason: LockReleaseReason,
  },
}

impl LockHandle {
  async fn new(
    lock_descriptor: LockDescriptor,
    ttl: Duration,
    lock_manager: Arc<dyn DistributedLock>,
  ) -> Self {
    let guard = Arc::new(Mutex::new(LockGuard::Init {
      descriptor: lock_descriptor.clone(),
      lock_manager: lock_manager.clone(),
    }));

    // Spawn a task that will forcibly release the lock once the TTL expires.
    // TTL is updated whenever the new TTL value is successfully synced to the
    // shared storage (i.e. S3).
    let ttl_expiry_checker_handle = spawn_extendable_timer(
      tokio::time::Instant::now() + ttl,
      {
        let guard = guard.clone();
        async move {
          let mut guard = guard.lock().await;
          info!(descriptor = ?guard.descriptor(), "TTL expired, forcibly releasing lock");
          if let Err(e) = guard.release().await {
            warn!(error = ?e, descriptor = ?guard.descriptor(), "Failed to release lock");
          }
        }
      },
    );

    let ttl_renewal_task = tokio::spawn({
      let lock_descriptor = lock_descriptor.clone();
      let lock_manager = lock_manager.clone();
      async move {
        let mut interval = tokio::time::interval(ttl / 3);
        loop {
          interval.tick().await;

          let new_deadline = tokio::time::Instant::now() + ttl;

          // Renew the lock TTL, and on success, reset the deadline of the local
          // TTL expiry checker
          match lock_manager.renew(&lock_descriptor, ttl).await {
            Ok(_) => {
              ttl_expiry_checker_handle.extend(new_deadline);
            }
            Err(e) => {
              warn!(error = ?e, ?lock_descriptor, "Failed to renew lock, stopping TTL renewal task");
            }
          }
        }
      }
    });

    guard
      .lock()
      .await
      .transition_to_ttl_renewal_task_started(ttl_renewal_task);

    Self {
      descriptor: lock_descriptor,
      guard,
    }
  }

  pub fn descriptor(&self) -> &LockDescriptor {
    &self.descriptor
  }

  /// Perform the given operation on the protected resource.
  pub async fn update_inner(
    &self,
    update_inner: impl FnOnce(&mut ProcessEntry),
  ) {
    match self.guard.lock().await.deref_mut() {
      LockGuard::Init { .. } => {}
      LockGuard::TtlRenewalTaskStarted { .. } => {}
      LockGuard::Active {
        protected_resource, ..
      } => {
        update_inner(protected_resource);
      }
      LockGuard::Released { .. } => {}
    }
  }

  pub async fn release(&self) -> anyhow::Result<()> {
    self.guard.lock().await.release().await
  }
}

impl LockGuard {
  fn descriptor(&self) -> &LockDescriptor {
    match self {
      LockGuard::Init { descriptor, .. } => descriptor,
      LockGuard::TtlRenewalTaskStarted { descriptor, .. } => descriptor,
      LockGuard::Active { descriptor, .. } => descriptor,
      LockGuard::Released { descriptor, .. } => descriptor,
    }
  }

  fn transition_to_ttl_renewal_task_started(
    &mut self,
    ttl_renewal_task: JoinHandle<()>,
  ) {
    assert!(matches!(self, LockGuard::Init { .. }));

    match self {
      LockGuard::Init {
        descriptor,
        lock_manager,
      } => {
        *self = LockGuard::TtlRenewalTaskStarted {
          descriptor: descriptor.clone(),
          lock_manager: lock_manager.clone(),
          ttl_renewal_task,
        };
      }
      _ => {
        unreachable!("LockGuard is not in the Init state");
      }
    }
  }

  /// Release the lock and perform the necessary cleanup.
  ///
  /// Since this method requires &mut, we don't need to worry about some other
  /// execution path trying to use the protected resource while the release is
  /// running.
  async fn release(&mut self) -> anyhow::Result<()> {
    match self {
      LockGuard::Init {
        descriptor,
        lock_manager,
      } => {
        // Release the lock
        lock_manager.release(&descriptor).await?;
        tracing::debug!(?descriptor, "LockGuard dropped, lock released");

        // Transition to the Released state
        *self = LockGuard::Released {
          descriptor: descriptor.clone(),
          reason: LockReleaseReason::ExplicitRelease,
        };
      }
      LockGuard::TtlRenewalTaskStarted {
        descriptor,
        lock_manager,
        ttl_renewal_task,
      } => {
        // Stop the TTL renewal task
        ttl_renewal_task.abort();

        // Release the lock
        lock_manager.release(&descriptor).await?;
        tracing::debug!(?descriptor, "LockGuard dropped, lock released");

        // Transition to the Released state
        *self = LockGuard::Released {
          descriptor: descriptor.clone(),
          reason: LockReleaseReason::ExplicitRelease,
        };
      }
      LockGuard::Active {
        descriptor,
        lock_manager,
        protected_resource,
        ttl_renewal_task,
      } => {
        // Stop the TTL renewal task
        ttl_renewal_task.abort();

        // Release the lock
        lock_manager.release(&descriptor).await?;
        tracing::debug!(?descriptor, "LockGuard dropped, lock released");

        // Transition to the Released state
        *self = LockGuard::Released {
          descriptor: descriptor.clone(),
          reason: LockReleaseReason::ExplicitRelease,
        };

        todo!("properly shutdown the protected resource");
      }
      LockGuard::Released { reason, descriptor } => {
        tracing::debug!(?reason, ?descriptor, "Lock was already released");
      }
    }

    Ok(())
  }
}

impl fmt::Debug for LockGuard {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match &self {
      LockGuard::Init { descriptor, .. } => f
        .debug_struct("LockGuard::Init")
        .field("descriptor", descriptor)
        .finish(),
      LockGuard::TtlRenewalTaskStarted { descriptor, .. } => f
        .debug_struct("LockGuard::TtlRenewalTaskStarted")
        .field("descriptor", descriptor)
        .finish(),
      LockGuard::Active { descriptor, .. } => f
        .debug_struct("LockGuard::Active")
        .field("descriptor", descriptor)
        .finish(),
      LockGuard::Released { descriptor, reason } => f
        .debug_struct("LockGuard::Released")
        .field("descriptor", descriptor)
        .field("reason", reason)
        .finish(),
    }
  }
}

/// LockGuard should be dropped only after the lock has been properly released.
impl Drop for LockGuard {
  fn drop(&mut self) {
    if !matches!(self, LockGuard::Released { .. }) {
      error!(lock_descriptor = ?self.descriptor(), "LockGuard dropped without releasing lock. ");
    }
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
  ) -> Result<LockHandle, LockAcquireError>;

  /// Releases a previously acquired distributed lock.
  async fn release(
    &self,
    lock_descriptor: &LockDescriptor,
  ) -> Result<(), AnyhowError>;

  /// Renews an existing lock by updating its timestamp and TTL.
  /// This extends the lock's expiration time without releasing and re-acquiring it.
  ///
  /// Returns a new LockHandle with the updated information on success.
  /// Fails if the lock doesn't exist or is held by a different node.
  #[allow(dead_code)]
  async fn renew(
    &self,
    lock_descriptor: &LockDescriptor,
    new_ttl: Duration,
  ) -> Result<LockDescriptor, LockAcquireError>;
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
  ) -> Result<LockHandle, LockAcquireError> {
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
        Ok(
          LockHandle::new(
            LockDescriptor {
              lock_key,
              node_id: node_id.clone(),
            },
            ttl,
            self,
          )
          .await,
        )
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
                    Ok(
                      LockHandle::new(
                        LockDescriptor {
                          lock_key,
                          node_id: node_id.clone(),
                        },
                        ttl,
                        self,
                      )
                      .await,
                    )
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
                    Ok(LockHandle::new(
                      LockDescriptor {
                        lock_key,
                        node_id: node_id.clone(),
                      },
                      ttl,
                      self,
                    ).await)
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

  async fn release(
    &self,
    lock_descriptor: &LockDescriptor,
  ) -> Result<(), AnyhowError> {
    debug!(lock_key = %lock_descriptor.lock_key, node_id = ?lock_descriptor.node_id, "Releasing S3 lock");
    match self
      .s3_client
      .delete_object()
      .bucket(&self.bucket)
      .key(&lock_descriptor.lock_key)
      .send()
      .await
    {
      Ok(_) => {
        info!(lock_key = %lock_descriptor.lock_key, node_id = ?lock_descriptor.node_id, "Successfully released S3 lock");
        Ok(())
      }
      Err(SdkError::ServiceError(service_err)) => {
        let del_err = service_err.into_err();
        if del_err.code() == Some("NoSuchKey") {
          warn!(lock_key = %lock_descriptor.lock_key, node_id = ?lock_descriptor.node_id, "Attempted to release a lock that does not exist (or was already released)");
          Ok(())
        } else {
          warn!(error = ?del_err, lock_key = %lock_descriptor.lock_key, node_id = ?lock_descriptor.node_id, "Failed to release S3 lock (Service Error)");
          Err(AnyhowError::new(del_err)).context(format!(
            "Failed to release S3 lock: {}",
            lock_descriptor.lock_key
          ))
        }
      }
      Err(e) => {
        warn!(error = ?e, lock_key = %lock_descriptor.lock_key, node_id = ?lock_descriptor.node_id, "Failed to release S3 lock (SDK Error)");
        Err(AnyhowError::new(e)).context(format!(
          "SDK Error releasing S3 lock: {}",
          lock_descriptor.lock_key
        ))
      }
    }
  }

  async fn renew(
    &self,
    lock_descriptor: &LockDescriptor,
    new_ttl: Duration,
  ) -> Result<LockDescriptor, LockAcquireError> {
    let lock_key = &lock_descriptor.lock_key;
    let node_id = &lock_descriptor.node_id;

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

        // Verify the lock has not expired
        if existing_lock_info.timestamp
          + Duration::from_secs(existing_lock_info.ttl_secs)
          < Utc::now()
        {
          warn!(lock_key, "Cannot renew lock: lock has expired");
          return Err(LockAcquireError::UnableToRenewExpiredLock(Some(
            existing_lock_info,
          )));
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
            Ok(LockDescriptor {
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
    ttl: Duration,
  ) -> Result<LockHandle, LockAcquireError> {
    Ok(
      LockHandle::new(
        LockDescriptor {
          lock_key: lock_name.to_string(),
          node_id: node_id.clone(),
        },
        ttl,
        self,
      )
      .await,
    )
  }

  async fn release(
    &self,
    _descriptor: &LockDescriptor,
  ) -> Result<(), AnyhowError> {
    Ok(())
  }

  async fn renew(
    &self,
    handle: &LockDescriptor,
    _new_ttl: Duration,
  ) -> Result<LockDescriptor, LockAcquireError> {
    Ok(handle.clone())
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

    let handle = lock_manager
      .clone()
      .try_acquire(lock_name, &node_id, ttl)
      .await
      .expect("Failed to acquire lock");

    assert!(
      s3_object_exists(
        &lock_manager.s3_client,
        &bucket,
        &handle.descriptor.lock_key
      )
      .await,
      "Lock object should exist after acquire"
    );

    handle.release().await.unwrap();

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

    let handle_a = lock_manager
      .clone()
      .try_acquire(lock_name, &node_a, ttl)
      .await
      .expect("Node A failed to acquire lock");

    let handle_b = lock_manager
      .clone()
      .try_acquire(lock_name, &node_b, ttl)
      .await;

    match handle_b {
      Err(LockAcquireError::LockHeld(Some(info))) => {
        assert_eq!(info.node_id, node_a);
      }
      _ => panic!(
        "Node B should have failed with LockHeld, got {:?}",
        handle_b
      ),
    }

    handle_a.release().await.unwrap();

    // Now node B should be able to acquire the lock
    let handle_b = lock_manager
      .try_acquire(lock_name, &node_b, ttl)
      .await
      .expect("Node B failed to acquire lock after release");

    handle_b.release().await.unwrap();
  }

  #[test_log::test(tokio::test)]
  async fn test_acquire_succeeds_if_expired() {
    let (lock_manager, _bucket, _minio) = setup_test_env().await;

    let lock_name = "test_lock_3";
    let node_a = NodeId::new("node_a");
    let node_b = NodeId::new("node_b");
    let short_ttl = Duration::from_secs(2);
    let long_ttl = Duration::from_secs(60);

    let handle_a = lock_manager
      .clone()
      .try_acquire(lock_name, &node_a, short_ttl)
      .await
      .expect("Node A failed to acquire lock with short TTL");

    // Wait until the lock expires
    sleep(short_ttl + Duration::from_secs(1)).await;

    let handle_b = lock_manager
      .try_acquire(lock_name, &node_b, long_ttl)
      .await
      .expect("Node B failed to acquire expired lock");

    assert_eq!(handle_b.descriptor.node_id, node_b);

    handle_a
      .release()
      .await
      .expect("Attempt to release the expired lock should be okay");

    handle_b.release().await.unwrap();
  }

  #[test_log::test(tokio::test)]
  async fn test_release_non_existent_lock() {
    let (lock_manager, _bucket, _minio) = setup_test_env().await;

    let lock_name = "test_lock_non_existent";
    let node_id = NodeId::new("node_a");

    let fake_handle = LockDescriptor {
      lock_key: lock_manager.get_lock_key(lock_name),
      node_id,
    };

    let result = lock_manager.release(&fake_handle).await;

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
    let mut acquired_handle: Option<LockHandle> = None;

    for result in results {
      match result {
        Ok(Ok(mut handle)) => {
          success_count += 1;
          acquired_handle = Some(handle);
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

    if let Some(handle) = acquired_handle {
      handle.release().await.unwrap();
    }
  }

  #[test_log::test(tokio::test)]
  async fn test_lock_renewal() {
    let (lock_manager, bucket, _minio) = setup_test_env().await;

    let lock_name = "test_lock_renewal";
    let node_id = NodeId::new("node_a");
    // Lock renewal happens every one third of the TTL i.e. 2 seconds.
    let ttl = Duration::from_secs(6);

    // First acquire the lock
    let handle = lock_manager
      .clone()
      .try_acquire(lock_name, &node_id, ttl)
      .await
      .expect("Failed to acquire lock initially");

    // Check lock exists
    assert!(
      s3_object_exists(
        &lock_manager.s3_client,
        &bucket,
        &handle.descriptor.lock_key
      )
      .await,
      "Lock object should exist after acquire"
    );

    // Get the initial lock info to verify timestamps later
    let initial_lock_info = get_lock_info(
      &lock_manager.s3_client,
      &bucket,
      &handle.descriptor.lock_key,
    )
    .await
    .expect("Failed to get initial lock info");

    // Sleep for 5 seconds to ensure the lock will have been renewed
    sleep(Duration::from_secs(5)).await;

    // Lock should still exist
    assert!(
      s3_object_exists(
        &lock_manager.s3_client,
        &bucket,
        &handle.descriptor.lock_key
      )
      .await,
      "Lock object should still exist after renewal"
    );

    // Get the updated lock info
    let renewed_lock_info = get_lock_info(
      &lock_manager.s3_client,
      &bucket,
      &handle.descriptor.lock_key,
    )
    .await
    .expect("Failed to get renewed lock info");

    // Print timestamps for debugging
    println!("Initial timestamp: {:?}", initial_lock_info.timestamp);
    println!("Renewed timestamp: {:?}", renewed_lock_info.timestamp);

    // Verify the timestamp was updated (renewed)
    assert!(
      renewed_lock_info.timestamp > initial_lock_info.timestamp,
      "Lock timestamp should be updated during renewal, but got renewed timestamp {:?} and initial timestamp {:?}",
      renewed_lock_info.timestamp,
      initial_lock_info.timestamp
    );

    // Verify the owner stayed the same
    assert_eq!(
      renewed_lock_info.node_id, node_id,
      "Lock should still be owned by the same node after renewal"
    );

    // Test that another node can't renew our lock
    let different_node_id = NodeId::new("node_b");
    let invalid_descriptor = LockDescriptor {
      lock_key: handle.descriptor.lock_key.clone(),
      node_id: different_node_id,
    };

    match lock_manager.renew(&invalid_descriptor, ttl).await {
      Err(LockAcquireError::LockHeld(Some(info))) => {
        assert_eq!(
          info.node_id, node_id,
          "Error should indicate the real lock owner"
        );
      }
      other => panic!("Expected LockHeld error, got {:?}", other),
    }

    handle.release().await.unwrap();
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
