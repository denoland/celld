use crate::cluster_membership::NodeInfo;
use chrono::Utc;
use hashring::HashRing;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{error, warn};

// A separate struct to hold the state that will be updated via RwLock
struct PeerManagerState {
  ring: HashRing<String>,
  peers: Vec<NodeInfo>,
}

pub struct PeerManager {
  state: RwLock<PeerManagerState>,
  self_node_id: String,
  self_advertise_addr: String,
  staleness_threshold: Duration,
}

impl PeerManager {
  /// Create a new PeerManager with the given node_id and advertise_addr
  /// This creates a peer manager with just the local node initially
  pub fn new(
    self_advertise_addr: String,
    self_node_id: String,
    staleness_threshold: Duration,
  ) -> Self {
    // Initialize with just the local node
    let mut ring = HashRing::new();
    ring.add(self_advertise_addr.clone());

    let self_node = NodeInfo {
      node_id: self_node_id.clone(),
      advertise_addr: self_advertise_addr.clone(),
      heartbeat_timestamp: chrono::Utc::now(),
    };

    let initial_state = PeerManagerState {
      ring,
      peers: vec![self_node],
    };

    Self {
      state: RwLock::new(initial_state),
      self_node_id,
      self_advertise_addr,
      staleness_threshold,
    }
  }

  /// Update the peer list from a list of active NodeInfo peers from the cluster membership
  /// Note: argument active_peers should include self.
  pub fn update_peers(&self, active_peers: Vec<NodeInfo>) {
    let mut state = self.state.write().unwrap();
    state.peers = active_peers;
    let mut new_ring = HashRing::new();
    for peer in &state.peers {
      new_ring.add(peer.advertise_addr.clone());
    }
    state.ring = new_ring;
  }

  /// Get the peer responsible for a given cell ID
  pub fn get_owner_peer(&self, tenant: &str, cell_id: &str) -> String {
    let state = self.state.read().unwrap();
    let key = cell_hash_key(tenant, cell_id);
    state.ring.get(&key).unwrap().to_string()
  }

  /// Check if the local instance is responsible for handling this cell
  pub fn is_local_owner(&self, tenant: &str, cell_id: &str) -> bool {
    let owner = self.get_owner_peer(tenant, cell_id);
    owner == self.self_advertise_addr
  }

  /// Get an ordered list of active node addresses responsible for a given cell,
  /// based on the consistent hash of tenant:cell_id.
  ///
  /// This method returns up to MAX_OWNERS active nodes that could own the cell,
  /// in preference order according to the hash ring.
  pub fn get_cell_owners(&self, tenant: &str, cell_id: &str) -> Vec<String> {
    let state = self.state.read().unwrap();

    // Define a reasonable maximum number of owners/candidates we want to return
    // This limits how many nodes the proxy might try to contact in sequence.
    const MAX_OWNERS: usize = 3;

    let mut owners = Vec::new();

    // Use the combined key for hashing
    let key = cell_hash_key(tenant, cell_id);

    // Get the primary node and potential replicas/successors from the ring
    if let Some(potential_owners) =
      // Use num_peers() to get enough potential owners to filter down from.
      // get_with_replicas handles cases where replicas > ring size.
      state.ring.get_with_replicas(&key, self.num_peers())
    {
      for addr in potential_owners {
        // Filter out any nodes that are no longer considered active
        if self.is_peer_active(&addr) {
          owners.push(addr.clone()); // Clone the address string
                                     // Stop once we have enough active owners
          if owners.len() >= MAX_OWNERS {
            break;
          }
        }
      }
    }
    owners
  }

  /// Check if a node is considered active, meaning it was present in the
  /// latest list of active nodes received from the ClusterMembership service
  /// AND its heartbeat timestamp is not stale.
  pub fn is_peer_active(&self, node_addr: &str) -> bool {
    let state = self.state.read().unwrap();
    // Debug output for troubleshooting
    println!(">> is_peer_active state.peers {:?}", state.peers);

    // First check if the node exists in our peers list
    if let Some(node_info) = state
      .peers
      .iter()
      .find(|node| node.advertise_addr == node_addr)
    {
      // Now check if the node's heartbeat timestamp is stale
      let now = Utc::now();
      let node_time = node_info.heartbeat_timestamp;

      // Convert standard Duration to chrono::Duration for timestamp comparison
      let threshold_as_chrono = match chrono::Duration::from_std(
        self.staleness_threshold,
      ) {
        Ok(d) => d,
        Err(e) => {
          // This should not happen with normal Duration values
          error!(
            "Failed to convert std::time::Duration (staleness_threshold) to chrono::Duration: {}. Using default of 90s for this check.",
            e
          );
          chrono::Duration::seconds(90) // Default fallback
        }
      };

      let time_since_heartbeat = now.signed_duration_since(node_time);
      let is_stale = time_since_heartbeat > threshold_as_chrono;

      if is_stale {
        warn!(
          "Peer {} (Addr: {}) considered STALE. Last heartbeat: {}, Current time: {}, Threshold: {:?}",
          node_info.node_id, node_addr, node_time, now, self.staleness_threshold
        );
      }

      !is_stale // Return true if NOT stale
    } else {
      false // Node not found in peers list
    }
  }

  /// Get the number of peers in the mesh
  pub fn num_peers(&self) -> usize {
    let state = self.state.read().unwrap();
    state.peers.len()
  }

  /// Get a list of all peer NodeInfo objects
  pub fn get_all_peer_info(&self) -> Vec<NodeInfo> {
    let state = self.state.read().unwrap();
    state.peers.clone()
  }

  /// Get the local peer's advertise address
  pub fn get_local_peer(&self) -> &str {
    &self.self_advertise_addr
  }

  /// Get the local peer's node ID
  pub fn get_local_node_id(&self) -> &str {
    &self.self_node_id
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CellHashKey(String);

fn cell_hash_key(tenant: &str, cell_id: &str) -> CellHashKey {
  CellHashKey(format!("{}/{}", tenant, cell_id))
}

#[cfg(test)]
mod tests {
  use super::*;
  use chrono::Utc;

  #[test]
  fn test_peer_manager_filters_stale_peers() {
    // Define a staleness threshold for the test, 2 seconds
    let staleness_threshold = Duration::from_secs(2);

    // Define NodeInfo for self node, an active peer, and a stale peer
    let self_node_id = "self_node";
    let self_advertise_addr = "127.0.0.1:8000";
    let active_peer_node_id = "active_peer_node";
    let active_peer_advertise_addr = "127.0.0.1:8001";
    let stale_peer_node_id = "stale_peer_node";
    let stale_peer_advertise_addr = "127.0.0.1:8002";

    // Timestamps for NodeInfo
    let self_info_ts = Utc::now();
    let active_peer_ts = Utc::now() - chrono::Duration::seconds(1); // 1 second old, fresh
    let stale_peer_ts = Utc::now() - chrono::Duration::seconds(5); // 5 seconds old, stale compared to 2s threshold

    // Create NodeInfo instances
    let self_node_info = NodeInfo {
      node_id: self_node_id.to_string(),
      advertise_addr: self_advertise_addr.to_string(),
      heartbeat_timestamp: self_info_ts,
    };
    let active_peer_node_info = NodeInfo {
      node_id: active_peer_node_id.to_string(),
      advertise_addr: active_peer_advertise_addr.to_string(),
      heartbeat_timestamp: active_peer_ts,
    };
    let stale_peer_node_info = NodeInfo {
      node_id: stale_peer_node_id.to_string(),
      advertise_addr: stale_peer_advertise_addr.to_string(),
      heartbeat_timestamp: stale_peer_ts,
    };

    // Instantiate PeerManager with the staleness threshold
    let peer_manager = PeerManager::new(
      self_advertise_addr.to_string(),
      self_node_id.to_string(),
      staleness_threshold,
    );

    // Populate the PeerManager's list with all nodes
    peer_manager.update_peers(vec![
      self_node_info.clone(),
      active_peer_node_info.clone(),
      stale_peer_node_info.clone(),
    ]);

    // Test assertions
    // Check is_peer_active
    assert!(
      peer_manager.is_peer_active(active_peer_advertise_addr),
      "Active peer should be considered active."
    );
    assert!(
      !peer_manager.is_peer_active(stale_peer_advertise_addr),
      "Stale peer should NOT be considered active."
    );

    // Check that get_cell_owners doesn't include stale peers
    let owners = peer_manager.get_cell_owners("tenant", "test_cell");
    assert!(
      !owners.contains(&stale_peer_advertise_addr.to_string()),
      "Stale peer should not be returned by get_cell_owners."
    );
  }
}
