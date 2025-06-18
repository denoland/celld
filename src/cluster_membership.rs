use async_trait::async_trait;
use aws_sdk_s3::{primitives::ByteStream, Client};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{collections::HashSet, net::SocketAddr};
use tracing::{debug, info, warn};
use uuid::Uuid;

// Default heartbeat interval and staleness threshold
pub const DEFAULT_STALENESS_THRESHOLD: Duration = Duration::from_secs(90);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

impl NodeId {
  pub fn new_uuid_v4() -> Self {
    Self(Uuid::new_v4().to_string())
  }

  pub fn as_str(&self) -> &str {
    &self.0
  }

  pub fn new(id: impl ToString) -> Self {
    Self(id.to_string())
  }
}

/// Represents a node in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
  /// Unique ID for this node (UUID)
  pub node_id: NodeId,
  /// Network address other nodes should use to contact this node
  pub advertise_addr: SocketAddr,
  /// Timestamp of the most recent heartbeat
  #[serde(with = "chrono::serde::ts_seconds")]
  pub heartbeat_timestamp: DateTime<Utc>,
}

/// Defines the interface for cluster membership management
#[async_trait]
pub trait ClusterMembership: Send + Sync {
  /// Register this node with the cluster
  async fn register(&self) -> anyhow::Result<()>;

  /// Update this node's heartbeat timestamp
  async fn heartbeat(&self) -> anyhow::Result<()>;

  /// Get a list of all active peers (excluding stale nodes)
  async fn get_active_nodes(&self) -> anyhow::Result<Vec<NodeInfo>>;

  /// Unregister this node from the cluster
  async fn unregister(&self) -> anyhow::Result<()>;
}

/// S3-based implementation of ClusterMembership
pub struct S3ClusterMembership {
  /// S3 client
  s3_client: Client,
  /// S3 bucket name
  bucket: String,
  /// Prefix for node objects in S3 (e.g., "cluster_state/nodes/")
  prefix: String,
  /// This node's information
  node_info: NodeInfo,
  /// Duration after which a node is considered stale
  staleness_threshold: Duration,
}

impl S3ClusterMembership {
  /// Create a new S3ClusterMembership instance with a shared S3 client
  pub fn new(
    s3_client: Client,
    bucket: String,
    prefix: String,
    advertise_addr: SocketAddr,
    node_id: NodeId,
    staleness_threshold: Duration,
  ) -> Self {
    let node_info = NodeInfo {
      node_id,
      advertise_addr,
      heartbeat_timestamp: Utc::now(),
    };

    Self {
      s3_client,
      bucket,
      prefix,
      node_info,
      staleness_threshold,
    }
  }

  /// Get the full S3 key for this node
  fn get_node_key(&self) -> String {
    format!("{}{}.json", self.prefix, self.node_info.node_id.0)
  }

  /// Check if a node is stale based on its heartbeat timestamp
  fn is_node_stale(&self, node: &NodeInfo) -> bool {
    let now = Utc::now();
    let node_time = node.heartbeat_timestamp;
    let threshold =
      chrono::Duration::from_std(self.staleness_threshold).unwrap();

    now.signed_duration_since(node_time) > threshold
  }
}

#[async_trait]
impl ClusterMembership for S3ClusterMembership {
  async fn register(&self) -> anyhow::Result<()> {
    let node_json = serde_json::to_string(&self.node_info)?;
    let node_key = self.get_node_key();

    debug!(
      node_id = ?self.node_info.node_id,
      addr = %self.node_info.advertise_addr,
      node = %node_key,
      bucket = %self.bucket,
      region = ?self.s3_client.config().region(),
      "Registering node in S3"
    );

    // Upload node information to S3
    self
      .s3_client
      .put_object()
      .bucket(&self.bucket)
      .key(&node_key)
      .body(ByteStream::from(node_json.into_bytes()))
      .send()
      .await?;

    info!(
      node_id = ?self.node_info.node_id,
      addr = %self.node_info.advertise_addr,
      "Node registered successfully in S3"
    );

    Ok(())
  }

  async fn heartbeat(&self) -> anyhow::Result<()> {
    // Update the timestamp and serialize the node info
    let mut updated_info = self.node_info.clone();
    updated_info.heartbeat_timestamp = Utc::now();

    let node_json = serde_json::to_string(&updated_info)?;
    let node_key = self.get_node_key();

    debug!(
      node_id = ?self.node_info.node_id,
      addr = %self.node_info.advertise_addr,
      "Sending heartbeat to S3"
    );

    // Upload updated node information to S3
    self
      .s3_client
      .put_object()
      .bucket(&self.bucket)
      .key(&node_key)
      .body(ByteStream::from(node_json.into_bytes()))
      .send()
      .await?;

    Ok(())
  }

  async fn get_active_nodes(&self) -> anyhow::Result<Vec<NodeInfo>> {
    debug!(
      node_id = ?self.node_info.node_id,
      "Listing active peers from S3"
    );

    // List objects with the cluster nodes prefix
    let list_result = self
      .s3_client
      .list_objects_v2()
      .bucket(&self.bucket)
      .prefix(&self.prefix)
      .send()
      .await?;

    let mut peers = Vec::new();
    let mut found_nodes = HashSet::new();

    // Process each object (node)
    if let Some(objects) = list_result.contents {
      for obj in objects {
        if let Some(key) = &obj.key {
          // Skip if we've already processed this node (shouldn't happen, but just in case)
          if !found_nodes.insert(key.clone()) {
            continue;
          }

          // Get the object content
          match self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
          {
            Ok(response) => {
              // Read the object body
              let body = response.body.collect().await?;
              let bytes = body.into_bytes();

              // Parse the JSON into a NodeInfo object
              match serde_json::from_slice::<NodeInfo>(&bytes) {
                Ok(node) => {
                  // Skip stale nodes
                  if !self.is_node_stale(&node) {
                    peers.push(node);
                  }
                }
                Err(e) => {
                  warn!(key = %key, error = %e, "Failed to parse node JSON");
                }
              }
            }
            Err(e) => {
              warn!(key = %key, error = %e, "Failed to get node object");
            }
          }
        }
      }
    }

    debug!(
      node_id = ?self.node_info.node_id,
      peer_count = peers.len(),
      "Retrieved active peers"
    );

    Ok(peers)
  }

  async fn unregister(&self) -> anyhow::Result<()> {
    let node_key = self.get_node_key();

    debug!(
      node_id = ?self.node_info.node_id,
      addr = %self.node_info.advertise_addr,
      "Unregistering node from S3"
    );

    // Delete the node object from S3
    self
      .s3_client
      .delete_object()
      .bucket(&self.bucket)
      .key(&node_key)
      .send()
      .await?;

    info!(
      node_id = ?self.node_info.node_id,
      addr = %self.node_info.advertise_addr,
      "Node unregistered successfully from S3"
    );

    Ok(())
  }
}

pub struct StandaloneClusterMembership {
  node_info: NodeInfo,
}

impl StandaloneClusterMembership {
  pub fn new(node_id: NodeId, advertise_addr: SocketAddr) -> Self {
    let node_info = NodeInfo {
      node_id,
      advertise_addr,
      heartbeat_timestamp: Utc::now(),
    };
    Self { node_info }
  }
}

#[async_trait]
impl ClusterMembership for StandaloneClusterMembership {
  async fn register(&self) -> anyhow::Result<()> {
    Ok(())
  }

  async fn heartbeat(&self) -> anyhow::Result<()> {
    Ok(())
  }

  async fn get_active_nodes(&self) -> anyhow::Result<Vec<NodeInfo>> {
    Ok(vec![self.node_info.clone()])
  }

  async fn unregister(&self) -> anyhow::Result<()> {
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::test_utils::MinioTestServer;
  use tokio::time::sleep;

  async fn setup_test_membership(
    minio: &MinioTestServer,
    node_id: Option<NodeId>,
    advertise_addr: &str,
  ) -> S3ClusterMembership {
    let bucket = "cluster-test".to_string();
    let _ = minio.create_bucket(&bucket);

    let cfg = crate::config::S3Config {
      endpoint: Some(format!("http://127.0.0.1:{}", minio.port)),
      bucket: bucket.clone(),
      region: "us-east-1".to_string(),
      path: Some("cluster_state/nodes/".to_string()),
      access_key_id: Some(minio.access_key_id.clone()),
      secret_access_key: Some(minio.secret_access_key.clone()),
    };

    let node_id = node_id.unwrap_or_else(NodeId::new_uuid_v4);
    let prefix = cfg.subpath("cluster_state/nodes");
    let s3_client = crate::node_state::NodeState::create_s3_client(&cfg).await;

    S3ClusterMembership::new(
      s3_client,
      bucket,
      prefix,
      advertise_addr.parse().unwrap(),
      node_id,
      Duration::from_secs(2), // Short threshold for tests
    )
  }

  #[test_log::test(tokio::test)]
  async fn test_register_creates_correct_s3_object() {
    let minio = MinioTestServer::start();
    let node_id = NodeId::new_uuid_v4();
    let membership =
      setup_test_membership(&minio, Some(node_id.clone()), "127.0.0.1:8080")
        .await;

    // Register the node
    let register_result = membership.register().await;
    assert!(register_result.is_ok(), "Registration should succeed");

    // Verify the node was registered by retrieving all peers
    let s3_client = membership.s3_client.clone();

    // Basic check - get the direct object
    let get_result = s3_client
      .get_object()
      .bucket(&membership.bucket)
      .key(membership.get_node_key())
      .send()
      .await;

    assert!(
      get_result.is_ok(),
      "Object should exist at the expected path"
    );

    // Check that the list operation also shows the object
    let list_result = s3_client
      .list_objects_v2()
      .bucket(&membership.bucket)
      .prefix(&membership.prefix)
      .send()
      .await;

    assert!(list_result.is_ok(), "List operation should succeed");

    if let Ok(list_result) = list_result {
      let objects = list_result.contents.unwrap_or_default();
      assert!(!objects.is_empty(), "S3 objects list should not be empty");

      // Find our node ID in the keys
      let found = objects.iter().any(|obj| {
        obj
          .key()
          .map(|key| key.contains(node_id.as_str()))
          .unwrap_or(false)
      });

      assert!(found, "Node ID should be in S3 object key");
    }
  }

  #[test_log::test(tokio::test)]
  async fn test_heartbeat_updates_timestamp() {
    let minio = MinioTestServer::start();
    let node_id = NodeId::new_uuid_v4();
    let membership =
      setup_test_membership(&minio, Some(node_id.clone()), "127.0.0.1:8080")
        .await;

    // Register the node
    let register_result = membership.register().await;
    assert!(register_result.is_ok(), "Registration should succeed");

    // Get the original timestamp
    let s3_client = membership.s3_client.clone();
    let get_result = s3_client
      .get_object()
      .bucket(&membership.bucket)
      .key(membership.get_node_key())
      .send()
      .await;

    assert!(get_result.is_ok(), "Should be able to get the object");

    if let Ok(get_result) = get_result {
      let body = get_result.body.collect().await.unwrap().into_bytes();
      let original_node: NodeInfo = serde_json::from_slice(&body).unwrap();
      let original_timestamp = original_node.heartbeat_timestamp;

      // Wait a bit to ensure timestamp can be different
      sleep(Duration::from_secs(2)).await;

      // Send heartbeat
      let heartbeat_result = membership.heartbeat().await;
      assert!(heartbeat_result.is_ok(), "Heartbeat should succeed");

      // Get the updated timestamp
      let get_result = s3_client
        .get_object()
        .bucket(&membership.bucket)
        .key(membership.get_node_key())
        .send()
        .await;

      assert!(
        get_result.is_ok(),
        "Should be able to get the updated object"
      );

      if let Ok(get_result) = get_result {
        let body = get_result.body.collect().await.unwrap().into_bytes();
        let updated_node: NodeInfo = serde_json::from_slice(&body).unwrap();
        let updated_timestamp = updated_node.heartbeat_timestamp;

        // Verify the timestamp was updated
        assert!(
          updated_timestamp > original_timestamp,
          "Heartbeat should update timestamp"
        );
      }
    }
  }

  #[test_log::test(tokio::test)]
  async fn test_get_active_peers_filters_stale_nodes() {
    let minio = MinioTestServer::start();

    // Create first node (will go stale)
    let stale_node_id = NodeId::new_uuid_v4();
    let stale_membership = setup_test_membership(
      &minio,
      Some(stale_node_id.clone()),
      "127.0.0.1:8081",
    )
    .await;

    // Register the first node
    stale_membership.register().await.unwrap();

    // Wait long enough for the first node to become stale
    // (staleness threshold was set to 2 seconds in setup_test_membership)
    sleep(Duration::from_secs(3)).await;

    // Create a second, active node
    let active_node_id = NodeId::new_uuid_v4();
    let active_membership = setup_test_membership(
      &minio,
      Some(active_node_id.clone()),
      "127.0.0.1:8082",
    )
    .await;

    // Register the active node
    active_membership.register().await.unwrap();

    // Get peers from the active node - should not include the stale node
    let get_peers_result = active_membership.get_active_nodes().await;
    assert!(get_peers_result.is_ok());
    let peers = get_peers_result.unwrap();
    assert_eq!(peers.len(), 1);

    // Create a third node to verify active node detection works
    let third_node_id = NodeId::new_uuid_v4();
    let third_membership = setup_test_membership(
      &minio,
      Some(third_node_id.clone()),
      "127.0.0.1:8083",
    )
    .await;

    third_membership.register().await.unwrap();

    // Get peers from the active node - should include only the third node
    let peers = active_membership.get_active_nodes().await.unwrap();
    assert_eq!(peers.len(), 2);
  }

  #[test_log::test(tokio::test)]
  async fn test_unregister_deletes_s3_object() {
    let minio = MinioTestServer::start();
    let node_id = NodeId::new_uuid_v4();
    let membership =
      setup_test_membership(&minio, Some(node_id.clone()), "127.0.0.1:8080")
        .await;

    // Register the node
    let register_result = membership.register().await;
    assert!(register_result.is_ok(), "Registration should succeed");

    // Verify the node was registered
    let s3_client = membership.s3_client.clone();
    let list_result = s3_client
      .list_objects_v2()
      .bucket(&membership.bucket)
      .prefix(&membership.prefix)
      .send()
      .await;

    assert!(list_result.is_ok(), "List operation should succeed");

    if let Ok(list_result) = list_result {
      let original_count = list_result.contents.unwrap_or_default().len();
      assert!(original_count > 0, "Node should be registered");

      // Unregister the node
      let unregister_result = membership.unregister().await;
      assert!(unregister_result.is_ok(), "Unregister should succeed");

      // Verify the node was unregistered
      let list_result = s3_client
        .list_objects_v2()
        .bucket(&membership.bucket)
        .prefix(&membership.prefix)
        .send()
        .await;

      assert!(list_result.is_ok(), "List operation should succeed");

      if let Ok(list_result) = list_result {
        let objects = list_result.contents.unwrap_or_default();

        // Find our node ID in the keys - should not be found
        let found = objects.iter().any(|obj| {
          obj
            .key()
            .map(|key| key.contains(node_id.as_str()))
            .unwrap_or(false)
        });

        assert!(
          !found,
          "Node ID should not be in S3 object keys after unregister"
        );
      }
    }
  }
}
