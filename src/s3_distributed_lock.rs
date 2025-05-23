use anyhow::Context as _;
use async_trait::async_trait;
use aws_sdk_s3::error::ProvideErrorMetadata as _;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::cluster_membership::NodeId;
use crate::distributed_lock2::{
  DistributedLock, LockAcquireError, LockDescriptor, LockHandle, LockInfo,
};

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
