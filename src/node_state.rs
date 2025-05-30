use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use crate::cell_manager::CellManager;
use crate::cluster_membership::{
  ClusterMembership, NodeId, S3ClusterMembership, StandaloneClusterMembership,
};
use crate::config::{self, S3Config};
use crate::control_socket_listener::ControlSocket;
use crate::distributed_lock::{
  DistributedLock, S3DistributedLock, StandaloneDistributedLock,
};
use crate::peer_manager::PeerManager;

/// Represents the global state shared across the application.
///
/// This struct serves as a container for various components and services
/// that need to be accessed from different parts of the application.
pub struct NodeState {
  /// Unique identifier for the node
  pub node_id: NodeId,

  /// Manager for cells running in the system
  pub cell_manager: Arc<CellManager>,

  /// Manager for peer node information and coordination
  pub peer_manager: Arc<PeerManager>,

  /// Cluster membership service
  pub cluster_membership: Arc<dyn ClusterMembership>,

  /// Distributed lock for coordinating operations
  pub distributed_lock: Arc<dyn DistributedLock>,

  /// Application configuration
  pub config: Arc<config::Config>,

  pub control_socket: ControlSocket,
}

impl NodeState {
  /// Creates a configured S3 client based on the provided S3Config
  pub async fn create_s3_client(s3_config: &S3Config) -> aws_sdk_s3::Client {
    let mut aws_config_builder =
      aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(s3_config.region.clone()));
    if let (Some(access_key), Some(secret_key)) =
      (&s3_config.access_key_id, &s3_config.secret_access_key)
    {
      debug!("Using explicit S3 credentials");
      aws_config_builder = aws_config_builder.credentials_provider(
        aws_sdk_s3::config::Credentials::new(
          access_key,
          secret_key,
          None,
          None,
          "static-s3-credentials",
        ),
      );
    }
    let aws_config = aws_config_builder.load().await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config)
      .force_path_style(true)
      .timeout_config(
        aws_smithy_types::timeout::TimeoutConfig::builder()
          .operation_timeout(Duration::from_secs(10))
          .build(),
      );
    if let Some(endpoint) = s3_config.endpoint.as_ref() {
      s3_config_builder = s3_config_builder.endpoint_url(endpoint);
    }
    aws_sdk_s3::Client::from_conf(s3_config_builder.build())
  }

  /// Creates a new NodeState with the given configuration
  pub fn new(config: config::Config) -> Result<Arc<Self>, anyhow::Error> {
    let control_socket = ControlSocket::new();

    // Create the process manager with the configured data directory
    let cell_manager =
      CellManager::new(config.data_dir.clone(), &control_socket);

    // Generate a unique node ID (UUID) for this instance
    let node_id = NodeId::new_uuid_v4();
    debug!("Generated node ID: {node_id:?}");

    // Initialize cluster membership and distributed lock
    let (cluster_membership, distributed_lock) = {
      // Create a small runtime just for initialization
      let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

      // Initialize the S3 cluster membership and distributed lock
      rt.block_on(async {
        // Try to get S3 membership config from configuration
        if config.has_s3_config() {
          info!("S3 cluster membership configured, initializing...");
          debug!("S3 cluster membership config: {:?}", config);
          let s3_config = config.to_s3_config().unwrap();

          // Create shared S3 client
          let s3_client = Self::create_s3_client(&s3_config).await;
          info!(
            "Created S3 client for bucket {} in region {}",
            s3_config.bucket, s3_config.region
          );

          // Create membership using the shared S3 client
          let membership = S3ClusterMembership::new(
            s3_client.clone(),
            s3_config.bucket.clone(),
            s3_config.subpath("cluster_state/nodes"),
            config.advertise_addr,
            node_id.clone(),
            config.staleness_threshold,
          );

          // Register the node with the cluster
          info!("Registering node {node_id:?} with S3 cluster");
          if let Err(e) = membership.register().await {
            error!("Failed to register node in S3 cluster: {}", e);
            std::process::exit(1);
          }

          // Use the shared S3 client for distributed lock
          let lock_prefix = s3_config.subpath("locks");

          // Create the distributed lock manager
          info!(
            "Initializing S3 distributed lock with bucket {} and prefix {}",
            s3_config.bucket, lock_prefix
          );
          let lock_manager = Arc::new(S3DistributedLock::new(
            s3_client,
            s3_config.bucket,
            lock_prefix,
          ));

          (
            Arc::new(membership) as Arc<dyn ClusterMembership>,
            lock_manager as Arc<dyn DistributedLock>,
          )
        } else {
          info!(
            "S3 cluster membership not configured, running in standalone mode"
          );
          (
            Arc::new(StandaloneClusterMembership::new(
              node_id.clone(),
              config.advertise_addr,
            )) as Arc<dyn ClusterMembership>,
            Arc::new(StandaloneDistributedLock) as Arc<dyn DistributedLock>,
          )
        }
      })
    };

    // Create peer manager with only local node
    let peer_manager = PeerManager::new(
      config.advertise_addr,
      node_id.clone(),
      config.hashring_seed,
    );
    debug!("Peer manager initialized in standalone mode");

    // Create config Arc for NodeState
    let config_arc = Arc::new(config);

    // Create NodeState container with cluster membership if available
    let node_state = Arc::new(NodeState {
      node_id,
      cell_manager: Arc::new(cell_manager),
      peer_manager: Arc::new(peer_manager),
      cluster_membership,
      distributed_lock,
      config: config_arc,
      control_socket,
    });

    Ok(node_state)
  }

  /// Creates a minimal NodeState for benchmarking purposes
  pub fn new_for_benchmark(config: config::Config) -> Arc<Self> {
    let data_dir = PathBuf::from("./data");
    let control_socket = ControlSocket::new();
    let cell_manager = CellManager::new(data_dir, &control_socket);

    let addr = "127.0.0.1:8000".parse().unwrap();
    let node_id = NodeId::new("benchmark-node");

    // Create minimal peer manager and node_state for benchmark
    let peer_manager =
      PeerManager::new(addr, node_id.clone(), config.hashring_seed);

    Arc::new(NodeState {
      node_id: node_id.clone(),
      cell_manager: Arc::new(cell_manager),
      peer_manager: Arc::new(peer_manager),
      cluster_membership: Arc::new(StandaloneClusterMembership::new(
        node_id, addr,
      )),
      distributed_lock: Arc::new(StandaloneDistributedLock),
      config: Arc::new(config),
      control_socket,
    })
  }
}
