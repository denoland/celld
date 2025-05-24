use std::error::Error as _;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::{interval, sleep_until, Instant, Sleep};
use tracing::{debug, error, info, warn};

use crate::cell_manager::CellEntry;
use crate::cluster_membership::NodeId;

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
    protected_resource: CellEntry,
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
  ) -> anyhow::Result<bool> {
    let released = match self {
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

        true
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

        true
      }
      LockState::Released { .. } => {
        // Lock was already released. Do nothing.
        false
      }
    };

    Ok(released)
  }
}

enum LockStateRequest {
  SetResource(SetResourceRequest),
  GetSocketPath(AccessResourceRequest<PathBuf>),
  MutateResource(MutateResourceRequest),
  ReleaseIfIdle(ReleaseIfIdleRequest),
  Release(ReleaseRequest),
}

struct SetResourceRequest {
  resource: CellEntry,
  res_chan: oneshot::Sender<()>,
}

struct AccessResourceRequest<T> {
  accessor: Box<dyn FnOnce(&CellEntry) -> T + Send + Sync>,
  res_chan: oneshot::Sender<Option<T>>,
}

struct MutateResourceRequest {
  mutator: Box<dyn FnOnce(&mut CellEntry) + Send + Sync>,
  res_chan: oneshot::Sender<()>,
}

struct ReleaseIfIdleRequest {
  idle_timeout: Duration,
  res_chan: oneshot::Sender<anyhow::Result<bool>>,
}

struct ReleaseRequest {
  res_chan: oneshot::Sender<anyhow::Result<bool>>,
}

#[derive(Debug)]
pub struct LockHandle {
  descriptor: LockDescriptor,
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

    Self {
      tx,
      descriptor: lock_descriptor,
    }
  }

  pub fn descriptor(&self) -> &LockDescriptor {
    &self.descriptor
  }

  pub async fn set_resource(&self, resource: CellEntry) -> anyhow::Result<()> {
    let (tx, rx) = oneshot::channel();
    self
      .tx
      .send(LockStateRequest::SetResource(SetResourceRequest {
        resource,
        res_chan: tx,
      }))?;
    rx.await.context("Lock state loop exited unexpectedly")
  }

  pub async fn get_socket_path(&self) -> anyhow::Result<Option<PathBuf>> {
    let (tx, rx) = oneshot::channel();
    self
      .tx
      .send(LockStateRequest::GetSocketPath(AccessResourceRequest {
        accessor: Box::new(|process_entry| process_entry.socket_path.clone()),
        res_chan: tx,
      }))?;
    rx.await.context("Lock state loop exited unexpectedly")
  }

  pub async fn mutate_resource(
    &self,
    mutator: Box<dyn FnOnce(&mut CellEntry) + Send + Sync>,
  ) -> anyhow::Result<()> {
    let (tx, rx) = oneshot::channel();
    self
      .tx
      .send(LockStateRequest::MutateResource(MutateResourceRequest {
        mutator,
        res_chan: tx,
      }))?;
    rx.await.context("Lock state loop exited unexpectedly")
  }

  /// Releases the lock if the resource is idle for the given duration. Returns `true` if the resource was released.
  pub async fn release_if_idle(
    &self,
    idle_timeout: Duration,
  ) -> anyhow::Result<bool> {
    let (tx, rx) = oneshot::channel();
    self
      .tx
      .send(LockStateRequest::ReleaseIfIdle(ReleaseIfIdleRequest {
        idle_timeout,
        res_chan: tx,
      }))?;
    rx.await?
  }

  /// Terminates the resource and releases the lock. Returns `true` if the resource was released.
  pub async fn release(&self) -> anyhow::Result<bool> {
    let (tx, rx) = oneshot::channel();
    self
      .tx
      .send(LockStateRequest::Release(ReleaseRequest { res_chan: tx }))?;
    rx.await?
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
          Some(LockStateRequest::GetSocketPath(req)) => {
            handle_get_socket_path(req, &mut state);
          }
          Some(LockStateRequest::MutateResource(req)) => {
            handle_mutate_resource(req, &mut state);
          }
          Some(LockStateRequest::ReleaseIfIdle(req)) => {
            handle_release_if_idle(req, &mut state);
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

fn handle_get_socket_path(
  req: AccessResourceRequest<PathBuf>,
  state: &mut LockState,
) {
  match state {
    LockState::Init { .. } => {
      // Do nothing
      let _ = req.res_chan.send(None);
    }
    LockState::Active {
      protected_resource, ..
    } => {
      let socket_path = (req.accessor)(protected_resource);
      let _ = req.res_chan.send(Some(socket_path));
    }
    LockState::Released { .. } => {
      // Do nothing
      let _ = req.res_chan.send(None);
    }
  }
}

fn handle_mutate_resource(req: MutateResourceRequest, state: &mut LockState) {
  match state {
    LockState::Init { .. } => {
      // Do nothing
    }
    LockState::Active {
      protected_resource, ..
    } => {
      (req.mutator)(protected_resource);
    }
    LockState::Released { .. } => {
      // Do nothing
    }
  }

  let _ = req.res_chan.send(());
}

async fn handle_release_if_idle(
  req: ReleaseIfIdleRequest,
  state: &mut LockState,
) {
  match state {
    LockState::Active {
      protected_resource, ..
    } => {
      let now = std::time::Instant::now();
      if !protected_resource.has_active_connections()
        && now.duration_since(protected_resource.last_used) > req.idle_timeout
      {
        let res = state
          .release_lock(LockReleaseReason::ExplicitRelease, None)
          .await;
        let _ = req.res_chan.send(res);
      } else {
        let _ = req.res_chan.send(Ok(false));
      }
    }
    _ => {
      let _ = req.res_chan.send(Ok(false));
    }
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
        Ok(LockHandle::new(
          LockDescriptor {
            lock_key,
            node_id: node_id.clone(),
          },
          self,
          ttl,
        ))
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
                    Ok(LockHandle::new(
                      LockDescriptor {
                        lock_key,
                        node_id: node_id.clone(),
                      },
                      self,
                      ttl,
                    ))
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
                      self,
                      ttl,
                    ))
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
  ) -> anyhow::Result<()> {
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
          Err(del_err).with_context(|| {
            format!("Failed to release S3 lock: {}", lock_descriptor.lock_key)
          })
        }
      }
      Err(e) => {
        warn!(error = ?e, lock_key = %lock_descriptor.lock_key, node_id = ?lock_descriptor.node_id, "Failed to release S3 lock (SDK Error)");
        Err(e).with_context(|| {
          format!("SDK Error releasing S3 lock: {}", lock_descriptor.lock_key)
        })
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
    Ok(LockHandle::new(
      LockDescriptor {
        lock_key: lock_name.to_string(),
        node_id: node_id.clone(),
      },
      self,
      ttl,
    ))
  }

  async fn release(&self, _descriptor: &LockDescriptor) -> anyhow::Result<()> {
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
