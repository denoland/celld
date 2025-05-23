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
  /// TTL for the lock guard. As long as a Deno process is up and running,
  /// the lock guard will be renewed at the interval of one third of this value.
  /// For example, if the value is 30 seconds, the lock guard will be renewed
  /// every 10 seconds.
  pub lock_guard_ttl: Duration,
  /// Interval for system cell takeover
  pub system_cell_takeover_interval: Duration,
  /// Interval for alarm scheduler
  pub alarm_scheduler_interval: Duration,
  /// Single tenant mode configuration
  pub single_tenant: Option<SingleTenantConfig>,
}

#[derive(Debug, Clone)]
pub struct SingleTenantConfig {
  /// Source file for the default tenant
  pub src_file: PathBuf,
  /// Static files directory for the default tenant
  pub static_dir: Option<PathBuf>,
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
    let staleness_secs = var("CELL_STALENESS_THRESHOLD_SECS")
      .ok()
      .and_then(|s| s.parse::<u64>().ok())
      .unwrap_or(90);
    let staleness_threshold = Duration::from_secs(staleness_secs);

    let lock_guard_ttl_secs = var("CELL_LOCK_GUARD_TTL_SECS")
      .ok()
      .and_then(|s| s.parse::<u64>().ok())
      .unwrap_or(30);
    let lock_guard_ttl = Duration::from_secs(lock_guard_ttl_secs);

    let system_cell_takeover_interval_secs =
      var("CELL_SYSTEM_CELL_TAKEOVER_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10);
    let system_cell_takeover_interval =
      Duration::from_secs(system_cell_takeover_interval_secs);

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

    Ok(Config {
      listen_addr,
      advertise_addr,
      internal_listen_addr,
      data_dir,
      heartbeat_interval,
      staleness_threshold,
      lock_guard_ttl,
      system_cell_takeover_interval,
      alarm_scheduler_interval,
      s3_endpoint,
      s3_bucket,
      s3_region,
      s3_path,
      s3_access_key_id,
      s3_secret_access_key,
      single_tenant: None,
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
