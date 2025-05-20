use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use crate::cluster_membership::{
  ClusterMembership, NodeInfo, S3ClusterMembership, StandaloneClusterMembership,
};
use crate::config;
use crate::distributed_lock::{
  DistributedLock, S3DistributedLock, StandaloneDistributedLock,
};
use crate::peer_manager::PeerManager;
use crate::process_manager::ProcessManager;

/// Represents the global state shared across the application.
///
/// This struct serves as a container for various components and services
/// that need to be accessed from different parts of the application.
pub struct NodeState {
  /// Unique identifier for the node
  pub node_id: String,

  /// Manager for Deno processes running in the system
  pub process_manager: Arc<ProcessManager>,

  /// Manager for peer node information and coordination
  pub peer_manager: Arc<PeerManager>,

  /// Cluster membership service
  pub cluster_membership: Arc<dyn ClusterMembership>,

  /// Distributed lock for coordinating operations
  pub distributed_lock: Arc<dyn DistributedLock>,

  /// Shared S3 client for all S3 operations (None if S3 not configured)
  pub s3_client: Option<aws_sdk_s3::Client>,

  /// Application configuration
  pub config: Arc<config::Config>,
}

impl NodeState {
  /// Creates a new NodeState with the given configuration
  pub fn new(config: config::Config) -> Result<Arc<Self>, anyhow::Error> {
    // Create the process manager with the configured data directory
    let process_manager = ProcessManager::new(config.data_dir.clone());

    // Generate a unique node ID (UUID) for this instance
    let node_id = uuid::Uuid::new_v4().to_string();
    debug!("Generated node ID: {}", node_id);

    // Initialize cluster membership, distributed lock, and shared S3 client
    let (cluster_membership, distributed_lock, shared_s3_client) = {
      // Create a small runtime just for initialization
      let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

      // Initialize the S3 cluster membership and distributed lock
      rt.block_on(async {
        // Try to get S3 membership config from configuration
        if config.has_s3_config() {
          info!("S3 configured, initializing shared S3 client...");
          let s3_config = config.to_s3_config().unwrap();

          // Enhanced S3 configuration logging
          info!(
            target: "celld::config",
            s3_endpoint = s3_config.endpoint.as_deref().unwrap_or("AWS Default"),
            s3_bucket = %s3_config.bucket,
            s3_region = %s3_config.region,
            s3_path_prefix = s3_config.path.as_deref().unwrap_or("N/A"),
            s3_force_path_style = s3_config.endpoint.is_some(), // Path style is typically forced if endpoint is custom
            "Shared S3 Client Configuration"
          );

          // Configure AWS config builder with region and retry policy
          let mut aws_config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(s3_config.region.clone()))
            .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(5));

          // Set explicit credentials if available in S3Config
          if s3_config.access_key_id.is_some() && s3_config.secret_access_key.is_some() {
            debug!("Using explicit S3 credentials for shared client");
            let credentials = aws_sdk_s3::config::Credentials::new(
              s3_config.access_key_id.clone().unwrap(),
              s3_config.secret_access_key.clone().unwrap(),
              None,
              None,
              "celld-shared-static-credentials",
            );
            aws_config_loader = aws_config_loader.credentials_provider(credentials);
          } else {
            debug!("Using default credentials provider for shared S3 client");
            // Default provider is used if not overridden
          }

          // Load the AWS config
          let aws_conf = aws_config_loader.load().await;

          // Build the S3 client configuration
          let mut s3_client_conf_builder = aws_sdk_s3::config::Builder::from(&aws_conf)
            .force_path_style(s3_config.endpoint.is_some()); // Force path style if custom endpoint

          // Set S3 endpoint if configured
          if let Some(endpoint_url) = &s3_config.endpoint {
            s3_client_conf_builder = s3_client_conf_builder.endpoint_url(endpoint_url);
          }

          // Add timeout configuration
          s3_client_conf_builder = s3_client_conf_builder.timeout_config(
            aws_smithy_types::timeout::TimeoutConfig::builder()
              .operation_timeout(Duration::from_secs(10)) // Default operation timeout
              .build(),
          );

          // Create the shared S3 client that will be used across components
          let s3_client = aws_sdk_s3::Client::from_conf(s3_client_conf_builder.build());
          info!("Shared S3 client initialized.");

          // Create NodeInfo for the membership service
          let node_info = NodeInfo {
            node_id: node_id.clone(),
            advertise_addr: config.advertise_addr.clone(),
            heartbeat_timestamp: chrono::Utc::now(),
          };

          // Create the membership prefix path for S3
          let membership_prefix = s3_config.subpath("cluster_state/nodes");

          // Create membership service using the new constructor with shared client
          let membership = S3ClusterMembership::new(
            s3_client.clone(),
            s3_config.bucket.clone(),
            membership_prefix,
            node_info,
            config.staleness_threshold,
          );

          info!(
            "Initializing S3 cluster membership with bucket {}",
            membership.bucket()
          );

          // Register the node with the cluster
          info!("Registering node {} with S3 cluster", node_id);
          if let Err(e) = membership.register().await {
            error!("Failed to register node in S3 cluster: {}", e);
            std::process::exit(1);
          }

          // Create lock prefix (locks/restore/ by default)
          let lock_prefix = s3_config.subpath("locks/restore");

          // Create the distributed lock manager
          info!(
            "Initializing S3 distributed lock with bucket {} and prefix {}",
            s3_config.bucket, lock_prefix
          );

          // Create a new S3 client for the lock manager from the shared client
          let lock_manager = Arc::new(S3DistributedLock::new(
            s3_client.clone(),
            s3_config.bucket.clone(),
            lock_prefix,
          ));

          (
            Arc::new(membership) as Arc<dyn ClusterMembership>,
            lock_manager as Arc<dyn DistributedLock>,
            Some(s3_client),
          )
        } else {
          info!(
            "S3 cluster membership not configured, running in standalone mode"
          );
          (
            Arc::new(StandaloneClusterMembership::new(
              node_id.clone(),
              config.advertise_addr.clone(),
            )) as Arc<dyn ClusterMembership>,
            Arc::new(StandaloneDistributedLock) as Arc<dyn DistributedLock>,
            None,
          )
        }
      })
    };

    // Create peer manager with only local node
    let peer_manager =
      PeerManager::new(config.advertise_addr.clone(), node_id.clone());
    debug!("Peer manager initialized in standalone mode");

    // Create config Arc for NodeState
    let config_arc = Arc::new(config);

    // Create NodeState container with cluster membership if available
    let node_state = Arc::new(NodeState {
      node_id,
      process_manager: Arc::new(process_manager),
      peer_manager: Arc::new(peer_manager),
      cluster_membership,
      distributed_lock,
      s3_client: shared_s3_client,
      config: config_arc,
    });

    Ok(node_state)
  }

  /// Creates a minimal NodeState for benchmarking purposes
  pub fn new_for_benchmark(config: config::Config) -> Arc<Self> {
    let data_dir = PathBuf::from("./data");
    let process_manager = ProcessManager::new(data_dir);

    // Create minimal peer manager and node_state for benchmark
    let peer_manager = PeerManager::new(
      "127.0.0.1:8000".to_string(),
      "benchmark-node".to_string(),
    );

    Arc::new(NodeState {
      node_id: "benchmark-node".to_string(),
      process_manager: Arc::new(process_manager),
      peer_manager: Arc::new(peer_manager),
      cluster_membership: Arc::new(StandaloneClusterMembership::new(
        "benchmark-node".to_string(),
        "127.0.0.1:8000".to_string(),
      )),
      distributed_lock: Arc::new(StandaloneDistributedLock),
      s3_client: None, // No S3 client for benchmark mode
      config: Arc::new(config),
    })
  }
}
