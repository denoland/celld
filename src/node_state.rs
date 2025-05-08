use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

use crate::cluster_membership::{ClusterMembership, S3ClusterMembership};
use crate::config;
use crate::distributed_lock::{DistributedLock, S3DistributedLock};
use crate::peer_manager::PeerManager;
use crate::process_manager::ProcessManager;

/// Represents the global state shared across the application.
///
/// This struct serves as a container for various components and services
/// that need to be accessed from different parts of the application.
pub struct NodeState {
  /// Manager for Deno processes running in the system
  pub process_manager: Arc<ProcessManager>,

  /// Manager for peer node information and coordination
  pub peer_manager: Arc<PeerManager>,

  /// Cluster membership service (empty in standalone mode)
  pub cluster_membership: Option<Arc<dyn ClusterMembership>>,

  /// Distributed lock for coordinating operations (empty in standalone mode)
  pub distributed_lock: Option<Arc<dyn DistributedLock>>,

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

          // Create membership using from_config with configured staleness threshold
          let membership = match S3ClusterMembership::from_config(
            s3_config.clone(),
            config.advertise_addr.clone(),
            Some(node_id.clone()),
            Some(config.staleness_threshold),
          )
          .await
          {
            Ok(membership) => {
              info!(
                "Initializing S3 cluster membership with bucket {}",
                membership.bucket()
              );
              membership
            }
            Err(e) => {
              error!("Failed to create S3 cluster membership: {}", e);
              std::process::exit(1);
            }
          };

          // Register the node with the cluster
          info!("Registering node {} with S3 cluster", node_id);
          if let Err(e) = membership.register().await {
            error!("Failed to register node in S3 cluster: {}", e);
            std::process::exit(1);
          }

          // Initialize the S3 client for distributed lock
          let aws_config = aws_config::from_env()
            .region(aws_config::Region::new(s3_config.region.clone()))
            .load()
            .await;

          let mut s3_client_builder =
            aws_sdk_s3::config::Builder::from(&aws_config)
              .force_path_style(true);

          if let Some(endpoint) = s3_config.endpoint.as_ref() {
            s3_client_builder = s3_client_builder.endpoint_url(endpoint);
          }

          let cfg = s3_client_builder.build();

          let s3_client = aws_sdk_s3::Client::from_conf(cfg);

          // Create lock prefix (locks/restore/ by default)
          let lock_prefix = s3_config.subpath("locks/restore");

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
            Some(Arc::new(membership) as Arc<dyn ClusterMembership>),
            Some(lock_manager as Arc<dyn DistributedLock>),
          )
        } else {
          info!(
            "S3 cluster membership not configured, running in standalone mode"
          );
          (None, None)
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
      process_manager: Arc::new(process_manager),
      peer_manager: Arc::new(peer_manager),
      cluster_membership,
      distributed_lock,
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
      process_manager: Arc::new(process_manager),
      peer_manager: Arc::new(peer_manager),
      cluster_membership: None,
      distributed_lock: None,
      config: Arc::new(config),
    })
  }
}
