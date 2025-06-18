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
//! configured as `locks/`, acquiring a lock for the database corresponding to
//! tenant `my-app.localhost` and cell `user-session-abc` might result in an
//! attempt to atomically create an S3 object like:
//!
//! ```text
//! s3://my-celld-state/locks/my-app.localhost/user-session-abc.lock
//! ```
//!
//! The content of this object would be a JSON representation of the `LockInfo`
//! struct.

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
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::{interval, sleep_until, Instant, Sleep};
use tracing::instrument;
use tracing::{debug, error, info, warn};

use crate::alarm_processor::Alarm;
use crate::cell_manager::CellEntry;
use crate::cluster_membership::NodeId;
use crate::node_state::NodeState;

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
    &self,
    lock_name: &str,
    node_id: &NodeId,
    global_ttl: Duration,
    local_ttl: Duration,
    // The lock manager instance to be set for use by LockHandle.
    lock_manager: Arc<dyn DistributedLock>,
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

#[derive(Debug, Clone, Copy)]
enum LockReleaseReason {
  /// The lock was released because the lock was explicitly released
  ExplicitRelease,
  /// The lock was released because the cell was idle
  Idle,
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
    protected_resource: Box<CellEntry>,
  },
  Releasing {
    descriptor: LockDescriptor,
    reason: LockReleaseReason,
  },
  Released {
    descriptor: LockDescriptor,
    _is_gracefully_shut_down: bool,
    _reason: LockReleaseReason,
  },
}

impl LockState {
  fn descriptor(&self) -> &LockDescriptor {
    match self {
      LockState::Init { descriptor, .. } => descriptor,
      LockState::Active { descriptor, .. } => descriptor,
      LockState::Releasing { descriptor, .. } => descriptor,
      LockState::Released { descriptor, .. } => descriptor,
    }
  }

  async fn renew_ttl(
    &self,
    global_ttl: Duration,
    local_ttl: Duration,
    global_ttl_expiry_timer: &mut Pin<&mut Sleep>,
    local_ttl_expiry_timer: &mut Pin<&mut Sleep>,
  ) -> anyhow::Result<()> {
    match self {
      LockState::Init {
        descriptor,
        lock_manager,
      } => {
        let now = Instant::now();
        lock_manager.renew(descriptor, global_ttl).await?;
        global_ttl_expiry_timer.as_mut().reset(now + global_ttl);
        local_ttl_expiry_timer.as_mut().reset(now + local_ttl);
      }
      LockState::Active {
        lock_manager,
        descriptor,
        ..
      } => {
        let now = Instant::now();
        lock_manager.renew(descriptor, global_ttl).await?;
        global_ttl_expiry_timer.as_mut().reset(now + global_ttl);
        local_ttl_expiry_timer.as_mut().reset(now + local_ttl);
      }
      LockState::Releasing { .. } => {
        // Do nothing
      }
      LockState::Released { .. } => {
        // Do nothing
      }
    }

    Ok(())
  }

  fn initiate_lock_release(
    &mut self,
    reason: LockReleaseReason,
    // The channel to send a message to the lock state loop.
    self_request_tx: mpsc::UnboundedSender<LockStateRequest>,
    // The channel to send to the requester of the lock release whether the lock
    // is released by this request (true), or it was already released by another
    // request (false).
    res_chan: oneshot::Sender<bool>,
    release_cancel_token: tokio_util::sync::CancellationToken,
  ) {
    match self {
      LockState::Init {
        descriptor,
        lock_manager,
      } => {
        let descriptor = descriptor.clone();
        let lock_manager = lock_manager.clone();

        *self = LockState::Releasing {
          descriptor: descriptor.clone(),
          reason,
        };

        // Spawn a task to release the lock.
        tokio::spawn({
          let descriptor = descriptor.clone();
          let lock_manager = lock_manager.clone();
          let self_request_tx = self_request_tx.clone();

          async move {
            tokio::select! {
              biased;

              _ = release_cancel_token.cancelled() => {
                warn!(?descriptor, "Lock release cancelled");
                let _ = self_request_tx.send(LockStateRequest::ReleaseCompleted(
                  ReleaseCompletedMessage {
                    is_gracefully_shut_down: false,
                    res_chan,
                  },
                ));
              }

              res = lock_manager.release(&descriptor) => {
                match res {
                  Ok(_) => {
                    debug!(?descriptor, "Lock release completed");
                    let _ = self_request_tx.send(LockStateRequest::ReleaseCompleted(
                      ReleaseCompletedMessage {
                        is_gracefully_shut_down: true,
                        res_chan,
                      },
                    ));
                  }
                  Err(e) => {
                    error!(?descriptor, error = ?e, "Failed to release lock");
                    let _ = self_request_tx.send(LockStateRequest::ReleaseCompleted(
                      ReleaseCompletedMessage {
                        is_gracefully_shut_down: false,
                        res_chan,
                      },
                    ));
                  }
                }
              }
            }
          }
        });
      }
      LockState::Active {
        descriptor,
        lock_manager,
        ..
      } => {
        let descriptor = descriptor.clone();
        let lock_manager = lock_manager.clone();

        let LockState::Active {
          mut protected_resource,
          ..
        } = std::mem::replace(
          self,
          LockState::Releasing {
            descriptor: descriptor.clone(),
            reason,
          },
        )
        else {
          unreachable!("We already checked that the state is Active in the match statement");
        };

        *self = LockState::Releasing {
          descriptor: descriptor.clone(),
          reason,
        };

        // Spawn a task to release the lock.
        tokio::spawn({
          // A future to shutdown the protected resource (i.e. Deno and then
          // Litestream) gracefully, then release the lock.
          // The release order matters. Deno process and Litestream replication
          // must be stopped *before* other nodes detect the lock as released.
          let release_fut = {
            let descriptor = descriptor.clone();
            let lock_manager = lock_manager.clone();

            async move {
              protected_resource.terminate().await;
              debug!(?descriptor, "protected resource gracefully terminated");
              lock_manager.release(&descriptor).await
            }
          };

          let self_request_tx = self_request_tx.clone();

          async move {
            tokio::select! {
              biased;

              _ = release_cancel_token.cancelled() => {
                error!(
                  ?descriptor,
                  "Timed out while gracefully terminating protected resource"
                );

                let _ = self_request_tx.send(LockStateRequest::ReleaseCompleted(
                  ReleaseCompletedMessage {
                    is_gracefully_shut_down: false,
                    res_chan,
                  },
                ));

                // When the release is cancelled, `release_fut` is dropped,
                // killing the protected resources forcibly. This ensures that
                // another node in the cluster can acquire the lock safely.
              }

              res = release_fut => {
                match res {
                  Ok(_) => {
                    debug!(?descriptor, "Lock release completed");
                    let _ = self_request_tx.send(LockStateRequest::ReleaseCompleted(
                      ReleaseCompletedMessage {
                        is_gracefully_shut_down: true,
                        res_chan,
                      },
                    ));
                  }
                  Err(e) => {
                    error!(?descriptor, error = ?e, "Failed to release lock");
                    let _ = self_request_tx.send(LockStateRequest::ReleaseCompleted(
                      ReleaseCompletedMessage {
                        is_gracefully_shut_down: false,
                        res_chan,
                      },
                    ));
                  }
                }
              }
            }
          }
        });
      }
      LockState::Releasing { .. } => {
        // Lock is already being released. Let the requester know about it.
        let _ = res_chan.send(false);
      }
      LockState::Released { .. } => {
        // Lock was already released. Let the requester know about it.
        let _ = res_chan.send(false);
      }
    }
  }
}

enum LockStateRequest {
  SetResource(SetResourceRequest),
  Ping(PingRequest),
  GetSocketPath(AccessOptionalResourceRequest<PathBuf>),
  MutateResource(MutateResourceRequest),
  GetAlarm(GetAlarmRequest),
  DeleteAlarm(DeleteAlarmRequest),
  SetAlarm(SetAlarmRequest),
  DispatchAlarms(DispatchAlarmsRequest),
  ReleaseIfIdle(ReleaseIfIdleRequest),
  Release(ReleaseRequest),
  ReleaseCompleted(ReleaseCompletedMessage),
}

impl LockStateRequest {
  fn kind(&self) -> &'static str {
    match self {
      LockStateRequest::SetResource(_) => "set_resource",
      LockStateRequest::Ping(_) => "ping",
      LockStateRequest::GetSocketPath(_) => "get_socket_path",
      LockStateRequest::MutateResource(_) => "mutate_resource",
      LockStateRequest::GetAlarm(_) => "get_alarm",
      LockStateRequest::DeleteAlarm(_) => "delete_alarm",
      LockStateRequest::SetAlarm(_) => "set_alarm",
      LockStateRequest::DispatchAlarms(_) => "dispatch_alarms",
      LockStateRequest::ReleaseIfIdle(_) => "release_if_idle",
      LockStateRequest::Release(_) => "release",
      LockStateRequest::ReleaseCompleted(_) => "release_completed",
    }
  }
}

struct SetResourceRequest {
  resource: Box<CellEntry>,
  res_chan: oneshot::Sender<()>,
}

pub enum LockStateKind {
  Init,
  Active,
  Releasing,
  Released,
}

struct PingRequest {
  res_chan: oneshot::Sender<LockStateKind>,
}

type OptionalResourceAccessor<T> =
  Box<dyn FnOnce(&CellEntry) -> Option<T> + Send + Sync>;

struct AccessOptionalResourceRequest<T> {
  accessor: OptionalResourceAccessor<T>,
  res_chan: oneshot::Sender<Option<T>>,
}

struct MutateResourceRequest {
  mutator: Box<dyn FnOnce(&mut CellEntry) + Send + Sync>,
  res_chan: oneshot::Sender<()>,
}

struct GetAlarmRequest {
  tenant: String,
  cell_id: String,
  res_chan: oneshot::Sender<Option<GetAlarmResponse>>,
}

struct GetAlarmResponse {
  scheduled_time_unix_ms: u64,
}

struct DeleteAlarmRequest {
  tenant: String,
  cell_id: String,
  res_chan: oneshot::Sender<anyhow::Result<()>>,
}

struct SetAlarmRequest {
  tenant: String,
  cell_id: String,
  scheduled_time_unix_ms: u64,
  res_chan: oneshot::Sender<anyhow::Result<()>>,
}

struct DispatchAlarmsRequest {
  node_state: Arc<NodeState>,
  now: DateTime<Utc>,
  limit: u32,
  res_chan: oneshot::Sender<anyhow::Result<()>>,
}

struct ReleaseIfIdleRequest {
  idle_timeout: Duration,
  res_chan: oneshot::Sender<bool>,
}

struct ReleaseRequest {
  res_chan: oneshot::Sender<bool>,
}

struct ReleaseCompletedMessage {
  res_chan: oneshot::Sender<bool>,
  is_gracefully_shut_down: bool,
}

#[derive(Debug, Clone)]
pub struct LockHandle {
  descriptor: LockDescriptor,
  tx: mpsc::UnboundedSender<LockStateRequest>,
}

impl LockHandle {
  pub fn new(
    lock_descriptor: LockDescriptor,
    lock_manager: Arc<dyn DistributedLock>,
    global_ttl: Duration,
    local_ttl: Duration,
  ) -> Self {
    let (tx, rx) = mpsc::unbounded_channel();

    tokio::spawn({
      let descriptor = lock_descriptor.clone();
      let lock_manager = lock_manager.clone();
      let tx = tx.clone();

      lock_state_loop(descriptor, lock_manager, global_ttl, local_ttl, tx, rx)
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
        resource: Box::new(resource),
        res_chan: tx,
      }))?;
    rx.await.context("Lock state loop exited unexpectedly")
  }

  pub async fn ping(&self) -> anyhow::Result<LockStateKind> {
    let (tx, rx) = oneshot::channel();
    self
      .tx
      .send(LockStateRequest::Ping(PingRequest { res_chan: tx }))?;
    rx.await.context("Lock state loop exited unexpectedly")
  }

  /// Get the UDS path through which the cell is listening for incoming HTTP
  /// requests. `None` is returned in either of the following cases:
  /// - the cell is not the system main cell
  /// - the cell is normal cell but not in `Active` state
  pub async fn get_socket_path(&self) -> anyhow::Result<Option<PathBuf>> {
    let (tx, rx) = oneshot::channel();
    self.tx.send(LockStateRequest::GetSocketPath(
      AccessOptionalResourceRequest {
        accessor: Box::new(|process_entry| {
          process_entry.get_socket_path().map(|p| p.to_path_buf())
        }),
        res_chan: tx,
      },
    ))?;
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

  pub async fn get_alarm(
    &self,
    tenant: &str,
    cell_id: &str,
  ) -> anyhow::Result<Option<Alarm>> {
    let (tx, rx) = oneshot::channel();
    self.tx.send(LockStateRequest::GetAlarm(GetAlarmRequest {
      tenant: tenant.to_string(),
      cell_id: cell_id.to_string(),
      res_chan: tx,
    }))?;

    match rx.await.context("Lock state loop exited unexpectedly")? {
      Some(alarm) => Ok(Some(Alarm {
        tenant: tenant.to_string(),
        cell_id: cell_id.to_string(),
        scheduled_time_unix_ms: alarm.scheduled_time_unix_ms,
      })),
      None => Ok(None),
    }
  }

  pub async fn delete_alarm(
    &self,
    tenant: &str,
    cell_id: &str,
  ) -> anyhow::Result<()> {
    let (tx, rx) = oneshot::channel();
    self
      .tx
      .send(LockStateRequest::DeleteAlarm(DeleteAlarmRequest {
        tenant: tenant.to_string(),
        cell_id: cell_id.to_string(),
        res_chan: tx,
      }))?;
    rx.await?
  }

  pub async fn set_alarm(
    &self,
    tenant: &str,
    cell_id: &str,
    scheduled_time_unix_ms: u64,
  ) -> anyhow::Result<()> {
    let (tx, rx) = oneshot::channel();
    self.tx.send(LockStateRequest::SetAlarm(SetAlarmRequest {
      tenant: tenant.to_string(),
      cell_id: cell_id.to_string(),
      scheduled_time_unix_ms,
      res_chan: tx,
    }))?;
    rx.await?
  }

  pub async fn dispatch_alarms(
    &self,
    node_state: Arc<NodeState>,
    now: DateTime<Utc>,
    limit: u32,
  ) -> anyhow::Result<()> {
    let (tx, rx) = oneshot::channel();
    self
      .tx
      .send(LockStateRequest::DispatchAlarms(DispatchAlarmsRequest {
        node_state,
        now,
        limit,
        res_chan: tx,
      }))?;
    rx.await?
  }

  /// Releases the lock if the resource is idle for the given duration. Returns
  /// `true` if the resource is released by this call.
  pub async fn release_if_idle(&self, idle_timeout: Duration) -> bool {
    let (tx, rx) = oneshot::channel();
    if self
      .tx
      .send(LockStateRequest::ReleaseIfIdle(ReleaseIfIdleRequest {
        idle_timeout,
        res_chan: tx,
      }))
      .is_err()
    {
      return false;
    }
    rx.await.unwrap_or(false)
  }

  /// Terminates the resource and releases the lock. If the resource was already
  /// released before this call, this method does nothing.
  pub async fn release(&self) {
    let (tx, rx) = oneshot::channel();
    let send_result = self
      .tx
      .send(LockStateRequest::Release(ReleaseRequest { res_chan: tx }));

    // send_result == Err means the lock state loop already exited, in which
    // case the release was already done.
    if send_result.is_err() {
      debug!(descriptor = ?self.descriptor, "The resource was already released");
      return;
    }

    match rx.await {
      Err(_) => {
        // In this error case, the sender was dropped and the lock state loop
        // already exited. So the release was already done.
        debug!(descriptor = ?self.descriptor, "The resource was already released");
      }
      Ok(released) => {
        if released {
          debug!(descriptor = ?self.descriptor, "The resource is released by this call");
        } else {
          debug!(descriptor = ?self.descriptor, "The resource was already released");
        }
      }
    }
  }
}

async fn lock_state_loop(
  descriptor: LockDescriptor,
  lock_manager: Arc<dyn DistributedLock>,
  global_ttl: Duration,
  local_ttl: Duration,
  request_tx: mpsc::UnboundedSender<LockStateRequest>,
  mut request_rx: mpsc::UnboundedReceiver<LockStateRequest>,
) {
  let mut state = LockState::Init {
    descriptor,
    lock_manager,
  };

  let mut global_ttl_renewal_interval = interval(local_ttl / 4);
  let now = Instant::now();
  let mut global_ttl_expiry_timer =
    std::pin::pin!(sleep_until(now + global_ttl));
  let mut local_ttl_expiry_timer = std::pin::pin!(sleep_until(now + local_ttl));

  // The token to cancel the release operation if it is not completed within the
  // the global TTL.
  let release_cancel_token = tokio_util::sync::CancellationToken::new();

  loop {
    tokio::select! {
      biased;

      _ = &mut global_ttl_expiry_timer, if !release_cancel_token.is_cancelled() => {
        // Global TTL expired; cancel the ongoing graceful release process to
        // forcibly release the lock.

        info!(descriptor = ?state.descriptor(), "Global TTL expired; cancelling ongoing graceful release process");

        release_cancel_token.cancel();
      }

      // Start the graceful release process if Local TTL expires and the lock is
      // still being held (i.e. `Init` or `Active` state).
      _ = &mut local_ttl_expiry_timer, if matches!(state, LockState::Init { .. } | LockState::Active { .. }) => {
        // Local TTL expired; starts the graceful release process.

        info!(descriptor = ?state.descriptor(), "TTL expired; starting graceful release process");

        // This is just to satisfy the signature of `initiate_lock_release`.
        let (oneshot_tx, _) = oneshot::channel();

        // Passing `global_ttl_expiry_timer` here so that this release operation
        // will finish (either gracefully or forcibly) before the global TTL
        // is reached.
        state.initiate_lock_release(
          LockReleaseReason::TTLExpired,
          request_tx.clone(),
          oneshot_tx,
          release_cancel_token.clone(),
        );
      }

      _ = global_ttl_renewal_interval.tick() => {
        debug!(descriptor = ?state.descriptor(), "Renewing TTL");

        // Renew the TTL using the lock manager
        match tokio::time::timeout_at(
          local_ttl_expiry_timer.deadline(),
          state.renew_ttl(global_ttl, local_ttl, &mut global_ttl_expiry_timer, &mut local_ttl_expiry_timer),
        ).await {
          Ok(Ok(_)) => {},
          Ok(Err(e)) => {
            error!(error = ?e, descriptor = ?state.descriptor(), "Failed to renew TTL");
          }
          Err(_elapsed) => {
            error!(descriptor = ?state.descriptor(), "Failed to renew TTL because of timeout");
          }
        }
      }

      req = request_rx.recv() => {
        let kind = req.as_ref().map(|r| r.kind()).unwrap_or("None");
        debug!(descriptor = ?state.descriptor(), ?kind, "Received request in lock state loop");

        // NOTE: it is important to keep message handlers non-async.
        //
        // If one is async, that may block the lock state loop from processing
        // other queued messages. And what's worse, it may cause deadlock if the
        // handler sends a message back to the `request_rx` channel and awaits
        // it - the handler waits for the response, but in order for the message
        // to be processed, the handler needs to finish and the loop needs to
        // continue to process the next message.

        match req {
          Some(LockStateRequest::SetResource(req)) => {
            handle_set_resource(req, &mut state);
          }
          Some(LockStateRequest::Ping(req)) => {
            handle_ping(req, &mut state);
          }
          Some(LockStateRequest::GetSocketPath(req)) => {
            handle_get_socket_path(req, &mut state);
          }
          Some(LockStateRequest::MutateResource(req)) => {
            handle_mutate_resource(req, &mut state);
          }
          Some(LockStateRequest::GetAlarm(req)) => {
            handle_get_alarm(req, &mut state);
          }
          Some(LockStateRequest::DeleteAlarm(req)) => {
            handle_delete_alarm(req, &mut state);
          }
          Some(LockStateRequest::SetAlarm(req)) => {
            handle_set_alarm(req, &mut state);
          }
          Some(LockStateRequest::DispatchAlarms(req)) => {
            handle_dispatch_alarms(req, &mut state);
          }
          Some(LockStateRequest::ReleaseIfIdle(req)) => {
            handle_release_if_idle(req, &mut state, request_tx.clone(), release_cancel_token.clone());
          }
          Some(LockStateRequest::Release(req)) => {
            handle_release(req, &mut state, request_tx.clone(), release_cancel_token.clone());
          }
          Some(LockStateRequest::ReleaseCompleted(req)) => {
            handle_release_completed(req, &mut state);
            return;
          }
          None => {
            // LockHandle was dropped; at this point, the state must be `Released`
            if !matches!(state, LockState::Released { .. }) {
              error!("LockHandle was dropped while in unexpected state");
            }
            return;
          }
        }

        debug!(descriptor = ?state.descriptor(), ?kind, "Handled request in lock state loop");
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

fn handle_ping(req: PingRequest, state: &mut LockState) {
  match state {
    LockState::Init { .. } => {
      let _ = req.res_chan.send(LockStateKind::Init);
    }
    LockState::Active { .. } => {
      let _ = req.res_chan.send(LockStateKind::Active);
    }
    LockState::Releasing { .. } => {
      let _ = req.res_chan.send(LockStateKind::Releasing);
    }
    LockState::Released { .. } => {
      let _ = req.res_chan.send(LockStateKind::Released);
    }
  }
}

fn handle_get_socket_path(
  req: AccessOptionalResourceRequest<PathBuf>,
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
      let _ = req.res_chan.send(socket_path);
    }
    LockState::Releasing { .. } => {
      // Do nothing
      let _ = req.res_chan.send(None);
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
    LockState::Releasing { .. } => {
      // Do nothing
    }
    LockState::Released { .. } => {
      // Do nothing
    }
  }

  let _ = req.res_chan.send(());
}

fn handle_get_alarm(req: GetAlarmRequest, state: &mut LockState) {
  match state {
    LockState::Init { .. } => {
      let _ = req.res_chan.send(None);
    }
    LockState::Active {
      protected_resource, ..
    } => {
      let Some(alarm_processor) = protected_resource.alarm_processor() else {
        let _ = req.res_chan.send(None);
        return;
      };

      tokio::spawn({
        let alarm_processor_handle = alarm_processor.handle();
        async move {
          let maybe_alarm = alarm_processor_handle
            .get(req.tenant, req.cell_id)
            .await
            .ok();
          let _ =
            req.res_chan.send(maybe_alarm.map(|alarm| GetAlarmResponse {
              scheduled_time_unix_ms: alarm.scheduled_time_unix_ms,
            }));
        }
      });
    }
    LockState::Releasing { .. } => {
      let _ = req.res_chan.send(None);
    }
    LockState::Released { .. } => {
      let _ = req.res_chan.send(None);
    }
  }
}

fn handle_delete_alarm(req: DeleteAlarmRequest, state: &mut LockState) {
  match state {
    LockState::Init { .. } => {
      let _ = req
        .res_chan
        .send(Err(anyhow::anyhow!("Cell is not initialized yet")));
    }
    LockState::Active {
      protected_resource, ..
    } => {
      let Some(alarm_processor) = protected_resource.alarm_processor() else {
        let _ = req
          .res_chan
          .send(Err(anyhow::anyhow!("Cell is not the system main cell")));
        return;
      };

      tokio::spawn({
        let alarm_processor_handle = alarm_processor.handle();
        async move {
          if let Err(e) =
            alarm_processor_handle.delete(req.tenant, req.cell_id).await
          {
            let _ = req.res_chan.send(Err(e.into()));
          } else {
            let _ = req.res_chan.send(Ok(()));
          }
        }
      });
    }
    LockState::Releasing { .. } => {
      let _ = req
        .res_chan
        .send(Err(anyhow::anyhow!("Cell is being released")));
    }
    LockState::Released { .. } => {
      let _ = req
        .res_chan
        .send(Err(anyhow::anyhow!("Cell is already released")));
    }
  }
}

fn handle_set_alarm(req: SetAlarmRequest, state: &mut LockState) {
  match state {
    LockState::Init { .. } => {
      let _ = req
        .res_chan
        .send(Err(anyhow::anyhow!("Cell is not initialized yet")));
    }
    LockState::Active {
      protected_resource, ..
    } => {
      let Some(alarm_processor) = protected_resource.alarm_processor() else {
        let _ = req
          .res_chan
          .send(Err(anyhow::anyhow!("Cell is not the system main cell")));
        return;
      };

      tokio::spawn({
        let alarm_processor_handle = alarm_processor.handle();
        async move {
          if let Err(e) = alarm_processor_handle
            .set(req.tenant, req.cell_id, req.scheduled_time_unix_ms)
            .await
          {
            let _ = req.res_chan.send(Err(e.into()));
          } else {
            let _ = req.res_chan.send(Ok(()));
          }
        }
      });
    }
    LockState::Releasing { .. } => {
      let _ = req
        .res_chan
        .send(Err(anyhow::anyhow!("Cell is being released")));
    }
    LockState::Released { .. } => {
      let _ = req
        .res_chan
        .send(Err(anyhow::anyhow!("Cell is already released")));
    }
  }
}

fn handle_dispatch_alarms(req: DispatchAlarmsRequest, state: &mut LockState) {
  match state {
    LockState::Init { .. } => {
      let _ = req
        .res_chan
        .send(Err(anyhow::anyhow!("Cell is not initialized yet")));
    }
    LockState::Active {
      protected_resource, ..
    } => {
      let Some(alarm_processor) = protected_resource.alarm_processor() else {
        let _ = req
          .res_chan
          .send(Err(anyhow::anyhow!("Cell is not the system main cell")));
        return;
      };

      tokio::spawn({
        let alarm_processor_handle = alarm_processor.handle();
        async move {
          match alarm_processor_handle
            .dispatch(req.node_state, req.now, req.limit)
            .await
          {
            Ok(_) => {
              let _ = req.res_chan.send(Ok(()));
            }
            Err(e) => {
              let _ = req.res_chan.send(Err(e.into()));
            }
          }
        }
      });
    }
    LockState::Releasing { .. } => {
      let _ = req
        .res_chan
        .send(Err(anyhow::anyhow!("Cell is being released")));
    }
    LockState::Released { .. } => {
      let _ = req
        .res_chan
        .send(Err(anyhow::anyhow!("Cell is already released")));
    }
  }
}

fn handle_release_if_idle(
  req: ReleaseIfIdleRequest,
  state: &mut LockState,
  self_request_tx: mpsc::UnboundedSender<LockStateRequest>,
  release_cancel_token: tokio_util::sync::CancellationToken,
) {
  match state {
    LockState::Active {
      protected_resource, ..
    } => {
      if protected_resource.is_idle(req.idle_timeout) {
        state.initiate_lock_release(
          LockReleaseReason::Idle,
          self_request_tx,
          req.res_chan,
          release_cancel_token,
        );
      }
    }
    _ => {
      let _ = req.res_chan.send(false);
    }
  }
}

fn handle_release(
  req: ReleaseRequest,
  state: &mut LockState,
  self_request_tx: mpsc::UnboundedSender<LockStateRequest>,
  release_cancel_token: tokio_util::sync::CancellationToken,
) {
  state.initiate_lock_release(
    LockReleaseReason::ExplicitRelease,
    self_request_tx,
    req.res_chan,
    release_cancel_token,
  );
}

fn handle_release_completed(
  req: ReleaseCompletedMessage,
  state: &mut LockState,
) {
  match state {
    LockState::Init { .. }
    | LockState::Active { .. }
    | LockState::Released { .. } => {
      unreachable!("Lock state should not be in `Releasing` state when receiving `ReleaseCompleted` message");
    }
    LockState::Releasing {
      descriptor, reason, ..
    } => {
      debug!(
        ?descriptor,
        ?reason,
        is_gracefully_shut_down = req.is_gracefully_shut_down,
        "Lock release completed",
      );
      *state = LockState::Released {
        descriptor: descriptor.clone(),
        _is_gracefully_shut_down: req.is_gracefully_shut_down,
        _reason: *reason,
      };
      // Send the result to the requester of the lock release.
      let _ = req.res_chan.send(true);
    }
  }
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
    Self {
      s3_client,
      bucket,
      prefix,
    }
  }

  fn get_lock_key(&self, lock_name: &str) -> String {
    assert!(self.prefix.ends_with('/'));
    assert!(!lock_name.is_empty(), "Lock name must not be empty");
    // Check that lock_name is valid string for S3 key
    fn is_safe_s3_key_char(c: char) -> bool {
      c.is_ascii_alphanumeric() || "-._~/".contains(c)
    }
    assert!(
      lock_name.chars().all(is_safe_s3_key_char),
      "Lock name '{lock_name}' contains invalid characters"
    );
    format!("{}{}.lock", self.prefix, lock_name)
  }
}

#[async_trait]
impl DistributedLock for S3DistributedLock {
  #[instrument(skip(self, lock_manager))]
  async fn try_acquire(
    &self,
    lock_name: &str,
    node_id: &NodeId,
    global_ttl: Duration,
    local_ttl: Duration,
    lock_manager: Arc<dyn DistributedLock>,
  ) -> Result<LockHandle, LockAcquireError> {
    let lock_key = self.get_lock_key(lock_name);
    debug!(
      lock_key,
      ?node_id,
      ?global_ttl,
      ?local_ttl,
      "Attempting to acquire S3 lock"
    );

    let lock_info = LockInfo {
      node_id: node_id.clone(),
      timestamp: Utc::now(),
      ttl_secs: global_ttl.as_secs(),
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
          lock_manager,
          global_ttl,
          local_ttl,
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
                      lock_manager,
                      global_ttl,
                      local_ttl,
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
                      lock_manager,
                      global_ttl,
                      local_ttl,
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
    &self,
    lock_name: &str,
    node_id: &NodeId,
    global_ttl: Duration,
    local_ttl: Duration,
    lock_manager: Arc<dyn DistributedLock>,
  ) -> Result<LockHandle, LockAcquireError> {
    Ok(LockHandle::new(
      LockDescriptor {
        lock_key: lock_name.to_string(),
        node_id: node_id.clone(),
      },
      lock_manager,
      global_ttl,
      local_ttl,
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
    let global_ttl = Duration::from_secs(60);
    let local_ttl = Duration::from_secs(50);

    let handle = lock_manager
      .clone()
      .try_acquire(
        lock_name,
        &node_id,
        global_ttl,
        local_ttl,
        lock_manager.clone(),
      )
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

    handle.release().await;

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
    let global_ttl = Duration::from_secs(60);
    let local_ttl = Duration::from_secs(50);

    let handle_a = lock_manager
      .clone()
      .try_acquire(
        lock_name,
        &node_a,
        global_ttl,
        local_ttl,
        lock_manager.clone(),
      )
      .await
      .expect("Node A failed to acquire lock");

    let handle_b = lock_manager
      .clone()
      .try_acquire(
        lock_name,
        &node_b,
        global_ttl,
        local_ttl,
        lock_manager.clone(),
      )
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

    handle_a.release().await;

    // Now node B should be able to acquire the lock
    let handle_b = lock_manager
      .try_acquire(
        lock_name,
        &node_b,
        global_ttl,
        local_ttl,
        lock_manager.clone(),
      )
      .await
      .expect("Node B failed to acquire lock after release");

    handle_b.release().await;
  }

  #[test_log::test(tokio::test)]
  async fn test_acquire_succeeds_if_expired() {
    let (lock_manager, _bucket, _minio) = setup_test_env().await;

    // LockManager which fails to renew the lock. Other operations are delegated
    // to the underlying S3DistributedLock.
    struct LockManagerWrapper(Arc<S3DistributedLock>);

    #[async_trait]
    impl DistributedLock for LockManagerWrapper {
      async fn try_acquire(
        &self,
        lock_name: &str,
        node_id: &NodeId,
        global_ttl: Duration,
        local_ttl: Duration,
        lock_manager: Arc<dyn DistributedLock>,
      ) -> Result<LockHandle, LockAcquireError> {
        self
          .0
          .try_acquire(lock_name, node_id, global_ttl, local_ttl, lock_manager)
          .await
      }

      async fn release(
        &self,
        lock_descriptor: &LockDescriptor,
      ) -> anyhow::Result<()> {
        self.0.release(lock_descriptor).await
      }

      async fn renew(
        &self,
        _lock_descriptor: &LockDescriptor,
        _new_ttl: Duration,
      ) -> Result<LockDescriptor, LockAcquireError> {
        Err(LockAcquireError::S3Error("fake error".to_string()))
      }
    }

    let lock_manager = Arc::new(LockManagerWrapper(lock_manager));

    let lock_name = "test_lock_3";
    let node_a = NodeId::new("node_a");
    let node_b = NodeId::new("node_b");
    let short_global_ttl = Duration::from_secs(2);
    let short_local_ttl = Duration::from_secs(1);
    let long_global_ttl = Duration::from_secs(60);
    let long_local_ttl = Duration::from_secs(50);

    let handle_a = lock_manager
      .clone()
      .try_acquire(
        lock_name,
        &node_a,
        short_global_ttl,
        short_local_ttl,
        lock_manager.clone(),
      )
      .await
      .expect("Node A failed to acquire lock with short TTL");

    // Wait until the lock expires
    sleep(short_global_ttl + Duration::from_secs(1)).await;

    let handle_b = lock_manager
      .try_acquire(
        lock_name,
        &node_b,
        long_global_ttl,
        long_local_ttl,
        lock_manager.clone(),
      )
      .await
      .expect("Node B failed to acquire expired lock");

    assert_eq!(handle_b.descriptor.node_id, node_b);

    // Call to `release` on the expired lock handle should just work
    handle_a.release().await;
    handle_b.release().await;
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
    let global_ttl = Duration::from_secs(10);
    let local_ttl = Duration::from_secs(5);

    let mut handles = vec![];
    for node_id in &node_ids {
      let lock_manager_ = lock_manager.clone();
      let node_id = node_id.clone();
      let handle = tokio::spawn(async move {
        lock_manager_
          .try_acquire(
            lock_name,
            &node_id,
            global_ttl,
            local_ttl,
            lock_manager_.clone(),
          )
          .await
      });
      handles.push(handle);
    }

    let results = futures::future::join_all(handles).await;

    let mut success_count = 0;
    let mut held_count = 0;
    let mut acquired_handle: Option<LockHandle> = None;

    for result in results {
      match result {
        Ok(Ok(handle)) => {
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
      handle.release().await;
    }
  }

  #[test_log::test(tokio::test)]
  async fn test_lock_renewal() {
    let (lock_manager, bucket, _minio) = setup_test_env().await;

    let lock_name = "test_lock_renewal";
    let node_id = NodeId::new("node_a");
    // Lock renewal happens every one fourth of the TTL i.e. 2 seconds.
    let global_ttl = Duration::from_secs(8);
    let local_ttl = Duration::from_secs(4);

    // First acquire the lock
    let handle = lock_manager
      .clone()
      .try_acquire(
        lock_name,
        &node_id,
        global_ttl,
        local_ttl,
        lock_manager.clone(),
      )
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

    match lock_manager.renew(&invalid_descriptor, global_ttl).await {
      Err(LockAcquireError::LockHeld(Some(info))) => {
        assert_eq!(
          info.node_id, node_id,
          "Error should indicate the real lock owner"
        );
      }
      other => panic!("Expected LockHeld error, got {:?}", other),
    }

    handle.release().await;
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
