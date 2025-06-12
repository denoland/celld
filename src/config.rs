use serde::Deserialize;
use serde::Serialize;
use std::env::var;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr as _;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Clone, PartialEq, Default)]
pub enum StaticFallbackStrategy {
  #[default]
  Strict,
  Spa {
    root_file: PathBuf,
  },
  Custom404 {
    page_file: PathBuf,
  },
}

impl StaticFallbackStrategy {
  pub fn from_str(input: &str) -> Self {
    if input == "strict" {
      return StaticFallbackStrategy::Strict;
    }

    if let Some(file) = input.strip_prefix("spa:") {
      return StaticFallbackStrategy::Spa {
        root_file: PathBuf::from(file),
      };
    }

    if input == "spa" {
      return StaticFallbackStrategy::Spa {
        root_file: PathBuf::from("index.html"),
      };
    }

    if let Some(file) = input.strip_prefix("custom404:") {
      return StaticFallbackStrategy::Custom404 {
        page_file: PathBuf::from(file),
      };
    }

    if input == "custom404" {
      return StaticFallbackStrategy::Custom404 {
        page_file: PathBuf::from("404.html"),
      };
    }

    tracing::error!(
      "Invalid static fallback strategy: '{}'. Using default 'strict'.",
      input
    );
    StaticFallbackStrategy::Strict
  }
}

#[derive(Debug, Clone)]
pub struct Config {
  /// Directory to store data in
  pub data_dir: PathBuf,
  /// IP:port to listen on
  pub listen_addr: SocketAddr,
  /// IP:port to advertise to other nodes
  pub advertise_addr: SocketAddr,
  /// IP:port for internal control plane communication
  pub internal_listen_addr: SocketAddr,
  /// S3 endpoint for cluster membership and distributed locking
  pub s3_endpoint: Option<String>,
  /// S3 bucket for cluster membership and distributed locking
  pub s3_bucket: Option<String>,
  /// S3 region for cluster membership and distributed locking
  pub s3_region: Option<String>,
  /// S3 path prefix for cluster membership and distributed locking
  pub s3_path: Option<String>,
  /// S3 access key for cluster membership and distributed locking
  pub s3_access_key_id: Option<String>,
  /// S3 secret access key for cluster membership and distributed locking
  pub s3_secret_access_key: Option<String>,
  /// Heartbeat interval in seconds
  pub heartbeat_interval: Duration,
  /// Staleness threshold in seconds for detecting inactive nodes
  pub staleness_threshold: Duration,
  /// TTL for the lock guard, visible to all other nodes in the cluster.
  /// Once this TTL expires, other nodes consider the lock to be released, at
  /// which point they may try to acquire it. We must ensure that the protected
  /// resources have been terminated (either gracefully or forcibly) before
  /// this TTL expires; otherwise, the system would get into an undefined state.
  pub lock_guard_ttl_global: Duration,
  /// TTL for the lock guard for local operations.
  /// The lock owner is allowed to perform operations on the protected resource
  /// until this TTL expires. Once it expires, it must start graceful shutdown.
  ///
  /// This value is equal to `lock_guard_ttl - lock_graceful_shutdown_timeout`.
  ///
  /// As long as the protected resource is alive, the lock guard will be renewed
  /// at the interval of 1/4 of this value. For example, if the value is 20
  /// seconds, the lock guard will be renewed every 5 seconds to extend TTL.
  pub lock_guard_ttl_local: Duration,
  /// Interval for alarm scheduler
  pub alarm_scheduler_interval: Duration,
  /// Seed for deterministic hash ring (for testing)
  pub hashring_seed: Option<u64>,
  /// Number of retries for system main cell spawning
  pub system_main_cell_spawn_retries: u32,
  /// Delay between retries for system main cell spawning
  pub system_main_cell_retry_delay: Duration,
  /// Single tenant mode configuration
  pub single_tenant: Option<SingleTenantConfig>,
  /// Static file fallback strategy
  pub static_fallback: StaticFallbackStrategy,
}

#[derive(Debug, Clone)]
pub struct SingleTenantConfig {
  /// Source file for the default tenant
  pub src_file: PathBuf,
  /// Static files directory for the default tenant
  pub static_dir: Option<PathBuf>,
  /// Environment file to load for the single tenant
  pub env_file: Option<PathBuf>,
}

/// Configuration for a MinIO or S3 replica target
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
  /// S3 endpoint URL (e.g., http://localhost:9000)
  pub endpoint: Option<String>,
  /// S3 bucket name
  pub bucket: String,
  /// AWS access key
  pub access_key_id: Option<String>,
  /// AWS secret key
  pub secret_access_key: Option<String>,
  /// S3 path prefix within the bucket
  pub path: Option<String>,
  /// AWS region (often 'us-east-1' for MinIO)
  pub region: String,
}

impl S3Config {
  /// Constructs an S3 prefix for a specific sub-directory.
  ///
  /// Ensures the resulting prefix always ends with a '/'.
  /// If `self.path` is None or empty, the result is "subpath/".
  /// If `self.path` is "base", the result is "base/subpath/".
  /// If `self.path` is "base/", the result is "base/subpath/".
  ///
  /// # Panics
  /// Panics if `subpath` is empty or starts with '/'.
  pub fn subpath(&self, subpath: &str) -> String {
    assert!(!subpath.is_empty(), "subpath cannot be empty");
    assert!(!subpath.starts_with('/'), "subpath must be relative");

    match self.path.as_deref() {
      // No base prefix, or empty base prefix
      None | Some("") => format!("{}/", subpath),
      // Base prefix exists
      Some(base) => {
        // Use format! which is efficient for joining parts.
        // Check if base already ends with '/', format accordingly.
        if base.ends_with('/') {
          format!("{}{}/", base, subpath) // e.g., "base/subpath/"
        } else {
          format!("{}/{}/", base, subpath) // e.g., "base/subpath/"
        }
      }
    }
  }
}

impl Config {
  pub fn from_env() -> Result<Self, String> {
    let advertise_addr = var("ADVERTISE_ADDR")
      .ok()
      .and_then(|addr_str| SocketAddr::from_str(&addr_str).ok())
      .unwrap_or_else(|| {
        info!("ADVERTISE_ADDR not set, using 127.0.0.1:8000");
        "127.0.0.1:8000".parse().unwrap()
      });

    // Get listen_addr with fallback to advertise_addr port
    let listen_addr = var("LISTEN_ADDR")
      .ok()
      .and_then(|addr_str| SocketAddr::from_str(&addr_str).ok())
      .unwrap_or_else(|| {
        // If not set, use the port from ADVERTISE_ADDR
        SocketAddr::new(
          IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
          advertise_addr.port(),
        )
      });

    // Get internal_listen_addr with fallback to advertise_addr + 1
    let internal_listen_addr = var("INTERNAL_LISTEN_ADDR").ok().and_then(|addr_str| {
      SocketAddr::from_str(&addr_str).ok()
    }).unwrap_or_else(|| {
      // If not set, use advertise_addr with port + 1
      let mut addr = advertise_addr;
      addr.set_port(addr.port() + 1);
      // Log a warning for multi-node setups
      info!(
        "INTERNAL_LISTEN_ADDR not set, using {addr} (derived from ADVERTISE_ADDR). For production clusters, explicitly set INTERNAL_LISTEN_ADDR.",
      );
      addr
    });

    // Get data_dir with fallback to ./data
    let data_dir_str = var("DATA").unwrap_or_else(|_| "./data".to_string());
    let data_dir = PathBuf::from(&data_dir_str);

    // Verify data directory exists
    if !data_dir.is_dir() {
      return Err(format!(
        "DATA_DIR ('{}') is not an existing directory.",
        data_dir.display()
      ));
    }

    // Get heartbeat interval with fallback to 30 seconds
    let heartbeat_secs = var("CELL_HEARTBEAT_INTERVAL")
      .ok()
      .and_then(|s| s.parse::<u64>().ok())
      .unwrap_or(30);
    let heartbeat_interval = Duration::from_secs(heartbeat_secs);

    // Get staleness threshold with fallback to 90 seconds
    let staleness_threshold = var("CELL_STALENESS_THRESHOLD_SECS")
      .ok()
      .and_then(|s| s.parse::<u64>().ok())
      .map(Duration::from_secs)
      .unwrap_or(crate::cluster_membership::DEFAULT_STALENESS_THRESHOLD);

    let lock_guard_ttl_global_secs = var("CELL_LOCK_GUARD_TTL_SECS")
      .ok()
      .and_then(|s| s.parse::<u64>().ok())
      .unwrap_or(30);
    let lock_guard_ttl_global = Duration::from_secs(lock_guard_ttl_global_secs);

    let lock_graceful_shutdown_timeout_secs =
      var("CELL_LOCK_GRACEFUL_SHUTDOWN_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10);
    let lock_graceful_shutdown_timeout =
      Duration::from_secs(lock_graceful_shutdown_timeout_secs);

    let lock_guard_ttl_local = lock_guard_ttl_global
      .checked_sub(lock_graceful_shutdown_timeout)
      .expect("lock_guard_ttl_global must be greater than lock_graceful_shutdown_timeout");

    let alarm_scheduler_interval_secs =
      var("CELL_ALARM_SCHEDULER_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5);
    let alarm_scheduler_interval =
      Duration::from_secs(alarm_scheduler_interval_secs);

    // Get optional S3 configuration
    // Prioritize CELL_S3 specific variables over standard AWS variables
    let s3_endpoint = var("CELL_S3_ENDPOINT").ok();
    let s3_bucket = var("CELL_S3_BUCKET").ok();
    let s3_region = var("CELL_S3_REGION").or_else(|_| var("AWS_REGION")).ok();
    let s3_path = var("CELL_S3_PREFIX").ok();
    let s3_access_key_id = var("CELL_S3_ACCESS_KEY_ID")
      .or_else(|_| var("AWS_ACCESS_KEY_ID"))
      .ok();
    let s3_secret_access_key = var("CELL_S3_SECRET_ACCESS_KEY")
      .or_else(|_| var("AWS_SECRET_ACCESS_KEY"))
      .ok();

    // Get optional hashring seed for deterministic hashing (primarily for testing)
    let hashring_seed = var("CELL_HASHRING_SEED")
      .ok()
      .and_then(|s| s.parse::<u64>().ok());

    // Get system main cell spawn retry configuration
    let system_main_cell_spawn_retries =
      var("CELLD_SYSTEM_MAIN_CELL_SPAWN_RETRIES")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(10);

    let system_main_cell_retry_delay_ms =
      var("CELLD_SYSTEM_MAIN_CELL_RETRY_DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(100);
    let system_main_cell_retry_delay =
      Duration::from_millis(system_main_cell_retry_delay_ms);

    Ok(Config {
      listen_addr,
      advertise_addr,
      internal_listen_addr,
      data_dir,
      heartbeat_interval,
      staleness_threshold,
      lock_guard_ttl_global,
      lock_guard_ttl_local,
      alarm_scheduler_interval,
      s3_endpoint,
      s3_bucket,
      s3_region,
      s3_path,
      s3_access_key_id,
      s3_secret_access_key,
      hashring_seed,
      system_main_cell_spawn_retries,
      system_main_cell_retry_delay,
      single_tenant: None,
      static_fallback: StaticFallbackStrategy::default(),
    })
  }

  pub fn has_s3_config(&self) -> bool {
    self.s3_bucket.is_some()
  }

  pub fn to_s3_config(&self) -> Option<S3Config> {
    Some(S3Config {
      endpoint: self.s3_endpoint.clone(),
      bucket: self.s3_bucket.as_ref()?.clone(),
      access_key_id: self.s3_access_key_id.clone(),
      secret_access_key: self.s3_secret_access_key.clone(),
      path: self.s3_path.clone(),
      region: self
        .s3_region
        .as_ref()
        .cloned()
        .unwrap_or_else(|| "us-east-1".to_string()),
    })
  }
}
