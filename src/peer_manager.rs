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
  /// latest list of active nodes received from the ClusterMembership service.
  pub fn is_peer_active(&self, node_addr: &str) -> bool {
    let state = self.state.read().unwrap();
    // The `state.peers` list only contains nodes deemed active by ClusterMembership
    println!(">> is_peer_active state.peers {:?}", state.peers);
    state
      .peers
      .iter()
      .any(|node| node.advertise_addr == node_addr)
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

pub fn cell_hash_key(tenant: &str, cell_id: &str) -> String {
  format!("{}/{}", tenant, cell_id)
}
