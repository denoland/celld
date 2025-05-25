use crate::cell_manager::{SYSTEM_CELL_ID, SYSTEM_TENANT};
use crate::node_state::NodeState;
use pingora::{server::ShutdownWatch, services::background::BackgroundService};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// Service that periodically sends heartbeats to the cluster membership service
/// and updates the peer manager with active peers
pub struct HeartbeatService {
  /// The node state for this node
  pub node_state: Arc<NodeState>,
  /// Interval between heartbeat operations
  pub interval: Duration,
}

#[async_trait::async_trait]
impl BackgroundService for HeartbeatService {
  async fn start(&self, mut shutdown: ShutdownWatch) {
    let cluster_membership = self.node_state.cluster_membership.clone();
    let peer_manager = self.node_state.peer_manager.clone();
    let interval = self.interval;

    let mut interval_timer = tokio::time::interval(interval);

    loop {
      tokio::select! {
        _ = interval_timer.tick() => {
          // Send heartbeat to S3
          match cluster_membership.heartbeat().await {
            Ok(_) => debug!("Sent heartbeat to S3 successfully"),
            Err(e) => error!("Failed to send heartbeat to S3: {}", e),
          }

          // Get active peers from S3 and update peer manager
          match cluster_membership.get_active_nodes().await {
            Ok(active_peers) => {
              debug!("Found {} active peers in cluster", active_peers.len());
              // Update the peer manager with active peers
              peer_manager.update_peers(active_peers);
            },
            Err(e) => {
              error!("Failed to get active peers from S3: {}", e);
              continue;
            }
          }

          // Ensure system cell is spawned when this node is the owner of the
          // system cell as a result of the membership change.
          let system_cell_creation_fut = {
            let cell_manager = self.node_state.cell_manager.clone();
            let peer_manager = self.node_state.peer_manager.clone();
            async move {
              if peer_manager.is_local_owner(SYSTEM_TENANT, SYSTEM_CELL_ID) {
                if let Err(e) = cell_manager.ensure_system_cell_spawned(self.node_state.clone()).await {
                  error!(error = ?e, "Failed to ensure system cell is spawned");
                }
              }
            }
          };

          // Terminate cells that are no longer owned by this node so that new
          // owner nodes can take over without waiting for TTL expiration.
          let cell_termination_fut = self.node_state.cell_manager.terminate_unowned_cells(&peer_manager);

          futures::future::join(system_cell_creation_fut, cell_termination_fut).await;
        }

        _ = shutdown.changed() => {
          // Shutdown triggered, unregister node from S3
          info!("Shutting down heartbeat service, unregistering from cluster");
          if let Err(e) = cluster_membership.unregister().await {
            error!(error = ?e, "Failed to unregister from cluster during shutdown");
          }
          break;
        }
      }
    }
  }
}
