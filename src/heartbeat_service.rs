use crate::cluster_membership::ClusterMembership;
use crate::peer_manager::PeerManager;
use pingora::{server::ShutdownWatch, services::background::BackgroundService};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// Service that periodically sends heartbeats to the cluster membership service
/// and updates the peer manager with active peers
pub struct HeartbeatService {
  /// The cluster membership implementation
  pub cluster_membership: Arc<dyn ClusterMembership>,
  /// The peer manager for this node
  pub peer_manager: Arc<PeerManager>,
  /// Interval between heartbeat operations
  pub interval: Duration,
}

#[async_trait::async_trait]
impl BackgroundService for HeartbeatService {
  async fn start(&self, mut shutdown: ShutdownWatch) {
    let cm = self.cluster_membership.clone();
    let peer_manager = self.peer_manager.clone();
    let interval = self.interval;

    let mut interval_timer = tokio::time::interval(interval);

    loop {
      tokio::select! {
          _ = interval_timer.tick() => {
              // Send heartbeat to S3
              match cm.heartbeat().await {
                  Ok(_) => debug!("Sent heartbeat to S3 successfully"),
                  Err(e) => error!("Failed to send heartbeat to S3: {}", e),
              }

              // Get active peers from S3 and update peer manager
              match cm.get_active_nodes().await {
                  Ok(active_peers) => {
                      debug!("Found {} active peers in cluster", active_peers.len());
                      // Update the peer manager with active peers
                      peer_manager.update_peers(active_peers);
                  },
                  Err(e) => error!("Failed to get active peers from S3: {}", e),
              }
          }

          _ = shutdown.changed() => {
              // Shutdown triggered, unregister node from S3
              info!("Shutting down heartbeat service, unregistering from cluster");
              if let Err(e) = cm.unregister().await {
                  error!("Failed to unregister from cluster during shutdown: {}", e);
              }
              break;
          }
      }
    }
  }
}
