use crate::cluster_membership::NodeInfo;
use hashring::HashRing;
use std::sync::RwLock;

// A separate struct to hold the state that will be updated via RwLock
struct PeerManagerState {
  ring: HashRing<String>,
  peers: Vec<NodeInfo>,
}

pub struct PeerManager {
  state: RwLock<PeerManagerState>,
  self_node_id: String,
  self_advertise_addr: String,
}

impl PeerManager {
  /// Create a new PeerManager with the given node_id and advertise_addr
  /// This creates a peer manager with just the local node initially
  pub fn new(self_advertise_addr: String, self_node_id: String) -> Self {
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
    }
  }

  /// Update the peer list from a list of active NodeInfo peers from the cluster membership
  pub fn update_peers(&self, active_peers: Vec<NodeInfo>) {
    // Create new peer state
    let mut state = self.state.write().unwrap();

    // Store the updated peer list
    state.peers = active_peers;

    // Create a new hash ring with all the updated peers
    let mut new_ring = HashRing::new();

    // Always add ourselves
    new_ring.add(self.self_advertise_addr.clone());

    // Add each peer's advertise_addr to the ring
    for peer in &state.peers {
      // Skip ourselves (already added)
      if peer.node_id != self.self_node_id {
        new_ring.add(peer.advertise_addr.clone());
      }
    }

    // Replace the old ring with the new one
    state.ring = new_ring;
  }

  /// Get the peer responsible for a given room ID
  pub fn get_owner_peer(&self, room_id: &str) -> String {
    let state = self.state.read().unwrap();
    state.ring.get(&room_id).unwrap().to_string()
  }

  /// Check if the local instance is responsible for handling this room
  pub fn is_local_owner(&self, room_id: &str) -> bool {
    let owner = self.get_owner_peer(room_id);
    owner == self.self_advertise_addr
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
