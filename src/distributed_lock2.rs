use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::{interval, sleep_until, Instant, Sleep};
use tracing::{debug, error, info, warn};

// use crate::distributed_lock::DistributedLock;
use crate::cluster_membership::NodeId;
use crate::process_manager::ProcessEntry;

/// Serialization format of the lock saved in the backing storage for sync
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
  pub node_id: NodeId,
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
  ) -> anyhow::Result<()>;

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

#[derive(Debug)]
enum LockReleaseReason {
  /// The lock was released because the lock was explicitly released
  ExplicitRelease,
  /// The lock was released because the ttl expired
  TTLExpired,
}

enum LockState {
  Init {
    descriptor: LockDescriptor,
    lock_manager: Arc<dyn DistributedLock>,
  },
  Active {
    descriptor: LockDescriptor,
    lock_manager: Arc<dyn DistributedLock>,
    protected_resource: ProcessEntry,
  },
  Released {
    descriptor: LockDescriptor,
    reason: LockReleaseReason,
  },
}

impl LockState {
  fn descriptor(&self) -> &LockDescriptor {
    match self {
      LockState::Init { descriptor, .. } => descriptor,
      LockState::Active { descriptor, .. } => descriptor,
      LockState::Released { descriptor, .. } => descriptor,
    }
  }

  async fn renew_ttl(
    &self,
    ttl: Duration,
    ttl_expiry_timer: &mut Pin<&mut Sleep>,
  ) -> anyhow::Result<()> {
    match self {
      LockState::Init {
        descriptor,
        lock_manager,
      } => {
        let now = Instant::now();
        lock_manager.renew(descriptor, ttl).await?;
        ttl_expiry_timer.as_mut().reset(now + ttl);
      }
      LockState::Active {
        lock_manager,
        descriptor,
        ..
      } => {
        let now = Instant::now();
        lock_manager.renew(descriptor, ttl).await?;
        ttl_expiry_timer.as_mut().reset(now + ttl);
      }
      LockState::Released { .. } => {
        // Do nothing
      }
    }

    Ok(())
  }

  async fn release_lock(
    &mut self,
    reason: LockReleaseReason,
    ttl_expiry_timer: Option<&mut Pin<&mut Sleep>>,
  ) -> anyhow::Result<()> {
    match self {
      LockState::Init {
        descriptor,
        lock_manager,
      } => {
        // Release the lock
        lock_manager.release(descriptor).await?;
        debug!(?descriptor, "Lock released");

        *self = LockState::Released {
          descriptor: descriptor.clone(),
          reason,
        };
      }
      LockState::Active {
        descriptor,
        lock_manager,
        protected_resource,
      } => {
        // Releasing the resource gracefully may be taking some time.
        // To ensure that no other node will detect the lock as expired during the
        // release process, if the deadline is close, we first extend it before
        // releasing the lock.
        if let Some(timer) = ttl_expiry_timer {
          let now = Instant::now();
          let deadline = timer.deadline();
          if deadline < now + Duration::from_secs(10) {
            // The current deadline is too close. Reset it to 30 seconds from now.
            let new_ttl = Duration::from_secs(30);
            lock_manager.renew(&descriptor, new_ttl).await?;
            timer.as_mut().reset(now + new_ttl);
          }
        }

        // The release order matters. Deno process and Litestream replication must
        // be stopped *before* other nodes detect the lock as released.

        // Shutdown the protected resource gracefully
        // TODO: Add a timeout to this operation (maybe 5s or so?)
        protected_resource.terminate();

        // Release the lock
        lock_manager.release(descriptor).await?;
        debug!(?descriptor, "Lock released");

        *self = LockState::Released {
          descriptor: descriptor.clone(),
          reason,
        };
      }
      LockState::Released { .. } => {
        // Lock was already released. Do nothing.
      }
    }

    Ok(())
  }
}

enum LockStateRequest {
  SetResource(SetResourceRequest),
  Release(ReleaseRequest),
}

struct SetResourceRequest {
  resource: ProcessEntry,
  res_chan: oneshot::Sender<()>,
}

struct ReleaseRequest {
  res_chan: oneshot::Sender<anyhow::Result<()>>,
}

pub struct LockHandle {
  tx: mpsc::UnboundedSender<LockStateRequest>,
}

impl Drop for LockHandle {
  fn drop(&mut self) {
    let (tx, _rx) = oneshot::channel();
    let req = LockStateRequest::Release(ReleaseRequest { res_chan: tx });

    if let Err(_) = self.tx.send(req) {
      warn!("Failed to send release request to lock state loop (loop already exited");
    }
  }
}

impl LockHandle {
  pub fn new(
    lock_descriptor: LockDescriptor,
    lock_manager: Arc<dyn DistributedLock>,
    ttl: Duration,
  ) -> Self {
    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn({
      let descriptor = lock_descriptor.clone();
      let lock_manager = lock_manager.clone();

      lock_state_loop(descriptor, lock_manager, ttl, rx)
    });

    Self { tx }
  }
}

async fn lock_state_loop(
  descriptor: LockDescriptor,
  lock_manager: Arc<dyn DistributedLock>,
  ttl: Duration,
  mut request_rx: mpsc::UnboundedReceiver<LockStateRequest>,
) {
  let mut state = LockState::Init {
    descriptor,
    lock_manager,
  };

  let mut ttl_renewal_interval = interval(ttl / 3);
  // TODO: maybe subtract some second from this to account for the clock skew or network latency
  let mut ttl_expiry_timer = std::pin::pin!(sleep_until(Instant::now() + ttl));

  loop {
    tokio::select! {
      biased;

      _ = &mut ttl_expiry_timer => {
        info!(descriptor = ?state.descriptor(), "TTL expired; forcibly releasing lock");
        // TTL expired; forcibly release the lock and exit the loop

        // TODO(magurotuna): actually, it is too late to terminate the process
        // here because at this point another node may acquire the lock i.e.
        // there are two nodes running the same cell and its associated SQLite
        // replication. Probably we should do two phase expiry management, where
        // shorter deadline is used to start the release process and longer one
        // represents the actual expiry visible to other nodes in the cluster.

        if let Err(e) = state.release_lock(LockReleaseReason::TTLExpired, None).await {
          error!(error = ?e, descriptor = ?state.descriptor(), "Failed to release lock");
        }

        return;
      }

      _ = ttl_renewal_interval.tick() => {
        // Renew the TTL using the lock manager
        if let Err(e) = state.renew_ttl(ttl, &mut ttl_expiry_timer).await {
          error!(error = ?e, descriptor = ?state.descriptor(), "Failed to renew TTL");
        }
      }

      req = request_rx.recv() => {
        match req {
          Some(LockStateRequest::SetResource(req)) => {
            handle_set_resource(req, &mut state);
          }
          Some(LockStateRequest::Release(req)) => {
            handle_release(req, &mut state, &mut ttl_expiry_timer);
          }
          None => {
            // LockHandle was dropped; at this point, the state must be `Released`
            if !matches!(state, LockState::Released { .. }) {
              error!("LockHandle was dropped while in unexpected state");
            }
          }
        }
      }
    }
  }
}

/// Set the provided resource in the lock state, transitioning the state to `Active`.
/// This must be called when the state is `Init`.
fn handle_set_resource(req: SetResourceRequest, state: &mut LockState) {
  assert!(matches!(state, LockState::Init { .. }));

  match state {
    LockState::Init {
      descriptor,
      lock_manager,
    } => {
      *state = LockState::Active {
        descriptor: descriptor.clone(),
        lock_manager: lock_manager.clone(),
        protected_resource: req.resource,
      };

      let _ = req.res_chan.send(());
    }
    _ => unreachable!(),
  }
}

async fn handle_release(
  req: ReleaseRequest,
  state: &mut LockState,
  ttl_expiry_timer: &mut Pin<&mut Sleep>,
) {
  let res = state
    .release_lock(LockReleaseReason::ExplicitRelease, Some(ttl_expiry_timer))
    .await;
  let _ = req.res_chan.send(res);
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
