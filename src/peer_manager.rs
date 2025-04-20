use hashring::HashRing;
use std::sync::Arc;

#[derive(Clone)]
pub struct PeerManager {
  ring: Arc<HashRing<String>>,
  peers: Vec<String>,
  self_id: String,
}

impl PeerManager {
  /// Create a new PeerManager with the given peer addresses
  pub fn new(mut peers: Vec<String>, self_id: String) -> Self {
    if !peers.contains(&self_id) {
      peers.push(self_id.clone());
    }
    let mut ring = HashRing::new();
    for peer in &peers {
      ring.add(peer.clone());
    }
    Self {
      ring: ring.into(),
      peers,
      self_id,
    }
  }

  /// Get the peer responsible for a given room ID
  pub fn get_owner_peer(&self, room_id: &str) -> &str {
    self.ring.get(&room_id).unwrap()
  }

  /// Check if the local instance is responsible for handling this room
  pub fn is_local_owner(&self, room_id: &str) -> bool {
    let owner = self.get_owner_peer(room_id);
    owner == self.self_id
  }

  /// Get the number of peers in the mesh
  pub fn num_peers(&self) -> usize {
    self.peers.len()
  }

  /// Get a list of all peers
  pub fn get_all_peers(&self) -> &Vec<String> {
    &self.peers
  }

  /// Get the local peer
  pub fn get_local_peer(&self) -> &str {
    &self.self_id
  }
}
