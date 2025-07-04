use crate::cell_manager::{SYSTEM_CELL_ID, SYSTEM_TENANT};
use crate::node_state::NodeState;
use crate::pingora::server::ShutdownWatch;
use crate::pingora::services::background::BackgroundService;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// Service that periodically performs cluster-related tasks:
/// 1. Sends heartbeats to the cluster membership service (e.g., S3).
/// 2. Fetches the list of active peers and updates the local PeerManager.
/// 3. Based on updated peer ownership, it may:
///    a. Ensure the system main cell is spawned if this node becomes its owner.
///    b. Terminate any normal cells that are no longer owned by this node.
pub struct HeartbeatService {
  /// The node state for this node
  pub node_state: Arc<NodeState>,
  /// Interval between heartbeat operations
  pub interval: Duration,
  /// Threshold for considering a node as stale
  pub staleness_threshold: Duration,
}

#[async_trait::async_trait]
impl BackgroundService for HeartbeatService {
  async fn start(&self, mut shutdown: ShutdownWatch) {
    let cluster_membership = self.node_state.cluster_membership.clone();
    let peer_manager = self.node_state.peer_manager.clone();
    let interval = self.interval;

    let mut interval_timer = tokio::time::interval(interval);
    info!(?interval, staleness_threshold = ?self.staleness_threshold, "Heartbeat service started");

    loop {
      tokio::select! {
        _ = interval_timer.tick() => {
          // Send heartbeat to S3
          match cluster_membership.heartbeat().await {
            Ok(_) => debug!("Sent heartbeat to S3 successfully"),
            Err(e) => error!(error = ?e, "Failed to send heartbeat to S3"),
          }

          // Get active peers from S3 and update peer manager
          match cluster_membership.get_active_nodes().await {
            Ok(active_peers) => {
              debug!("Found {} active peers in cluster", active_peers.len());
              // Update the peer manager with active peers
              peer_manager.update_peers(active_peers);
            },
            Err(e) => {
              error!(error = ?e, "Failed to get active peers from S3");
              continue;
            }
          }

          // Ensure system main cell is spawned when this node is the owner of
          // the system main cell as a result of the membership change.
          let system_main_cell_creation_fut = {
            let cell_manager = self.node_state.cell_manager.clone();
            let peer_manager = self.node_state.peer_manager.clone();
            async move {
              if peer_manager.is_local_owner(SYSTEM_TENANT, SYSTEM_CELL_ID) {
                if let Err(e) = cell_manager.ensure_system_main_cell_spawned(self.node_state.clone()).await {
                  error!(
                    error = ?e,
                    "Failed to ensure system main cell is spawned"
                  );
                }
              }
            }
          };

          // Terminate cells that are no longer owned by this node so that new
          // owner nodes can take over without waiting for TTL expiration.
          let cell_termination_fut = self.node_state.cell_manager.terminate_unowned_cells(&peer_manager);

          // System cell creation and/or cell termination may take a while,
          // blocking the next heartbeat. We need to make sure to give up these
          // tasks and proceed to the next heartbeat so that this node is not
          // considered as stale by other nodes.
          let timeout = std::cmp::max(
            self.staleness_threshold.saturating_sub(Duration::from_secs(5)),
            Duration::from_secs(3),
          );

          let (system_main_cell_creation_res, cell_termination_res) = futures::future::join(
            tokio::time::timeout(timeout, system_main_cell_creation_fut),
            tokio::time::timeout(timeout, cell_termination_fut),
          ).await;

          if let Err(e) = system_main_cell_creation_res {
            error!(error = ?e, ?timeout, "Failed to ensure system main cell is spawned because of timeout");
          }

          if let Err(e) = cell_termination_res {
            error!(error = ?e, ?timeout, "Failed to terminate unowned cells because of timeout");
          }
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

    info!("Heartbeat service stopped");
  }
}
