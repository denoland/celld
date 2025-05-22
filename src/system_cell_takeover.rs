use futures::future::BoxFuture;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use tracing::{debug, error, info};

use crate::cluster_membership::NodeId;
use crate::distributed_lock::{DistributedLock, LockDescriptor};
use crate::system_cell::{SystemCell, SYSTEM_CELL_ID, SYSTEM_TENANT};
use std::sync::Arc;
use std::time::Duration;

type SystemCellFactory = Box<
  dyn Fn(LockDescriptor) -> BoxFuture<'static, Result<Arc<SystemCell>, anyhow::Error>>
    + Send
    + Sync,
>;

/// SystemCellTakeover service periodically checks if there is a system cell in
/// the cluster. If not, it will attempt to take over the system cell.
/// Once it acquires the lock on the system cell, a [`SystemCell`] instance is created and broadcast via the channel.
pub struct SystemCellTakeover {
  /// The interval to check if there is a system cell in the cluster.
  pub interval: Duration,

  /// The channel to broadcast a created [`SystemCell`] instance.
  pub broadcast: tokio::sync::broadcast::Sender<Arc<SystemCell>>,

  pub lock_manager: Arc<dyn DistributedLock + Send + Sync>,

  pub node_id: NodeId,

  pub system_cell_factory: SystemCellFactory,
}

#[async_trait::async_trait]
impl BackgroundService for SystemCellTakeover {
  async fn start(&self, mut shutdown: ShutdownWatch) {
    info!(
      "Starting system cell takeover service with interval: {:?}",
      self.interval
    );

    const SYSTEM_CELL_LOCK_TTL: std::time::Duration =
      std::time::Duration::from_secs(10);

    let mut interval = tokio::time::interval(self.interval);

    loop {
      tokio::select! {
          _ = shutdown.changed() => {
              info!("System cell takeover service received shutdown signal");
              break;
          }
          _ = interval.tick() => {
              let lock_name = format!("{}/{}", SYSTEM_TENANT, SYSTEM_CELL_ID);
              let lock_guard = match self.lock_manager.clone().try_acquire(&lock_name, &self.node_id, SYSTEM_CELL_LOCK_TTL).await {
                Ok(lock_guard) => lock_guard,
                Err(e) => {
                    debug!(error = ?e, "Failed to acquire lock on system cell");
                    continue;
                }
              };

              let system_cell = match (self.system_cell_factory)(lock_guard).await {
                Ok(system_cell) => system_cell,
                Err(e) => {
                    debug!(error = ?e, "Failed to create system cell");
                    continue;
                }
              };

              if self.broadcast.send(system_cell).is_err() {
                error!("No active receivers for system cell broadcast");
              }
              break;
          }
      }
    }

    info!("System cell takeover service stopped");
  }
}
