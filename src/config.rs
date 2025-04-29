use serde::Deserialize;
use serde::Serialize;
use std::env::var;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug)]
pub struct Config {
  pub listen_addr: String,
  pub advertise_addr: String,
  pub data_dir: PathBuf,
  pub heartbeat_interval: Duration,
  pub s3_endpoint: Option<String>,
  pub s3_bucket: Option<String>,
  pub s3_region: Option<String>,
  pub s3_prefix: Option<String>,
  pub s3_access_key_id: Option<String>,
  pub s3_secret_access_key: Option<String>,
}

/// Configuration for a MinIO or S3 replica target
#[derive(Debug, Serialize, Deserialize)]
pub struct S3Config {
  /// S3 endpoint URL (e.g., http://localhost:9000)
  pub endpoint: String,
  /// S3 bucket name
  pub bucket: String,
  /// AWS access key
  pub access_key_id: String,
  /// AWS secret key
  pub secret_access_key: String,
  /// S3 path prefix within the bucket
  pub path: Option<String>,
  /// AWS region (often 'us-east-1' for MinIO)
  pub region: String,
}

impl Config {
  pub fn from_env() -> Result<Self, String> {
    // Get the required values
    let advertise_addr = match var("ADVERTISE_ADDR") {
      Ok(addr) if !addr.is_empty() => addr,
      _ => return Err(
        "ADVERTISE_ADDR environment variable must be set (e.g., 1.2.3.4:8080)"
          .into(),
      ),
    };

    // Get listen_addr with fallback to advertise_addr port
    let listen_addr = var("LISTEN_ADDR").unwrap_or_else(|_| {
      // If not set, use the port from ADVERTISE_ADDR or a default
      let port = advertise_addr.split(':').nth(1).unwrap_or("3000");
      format!("0.0.0.0:{}", port)
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
    let heartbeat_secs = var("ROOMD_HEARTBEAT_INTERVAL")
      .ok()
      .and_then(|s| s.parse::<u64>().ok())
      .unwrap_or(30);
    let heartbeat_interval = Duration::from_secs(heartbeat_secs);

    // Get optional S3 configuration
    let s3_endpoint = var("ROOMD_S3_ENDPOINT").ok();
    let s3_bucket = var("ROOMD_S3_BUCKET").ok();
    let s3_region = var("ROOMD_S3_REGION").ok();
    let s3_prefix = var("ROOMD_S3_PREFIX").ok();
    let s3_access_key_id = var("ROOMD_S3_ACCESS_KEY_ID").ok();
    let s3_secret_access_key = var("ROOMD_S3_SECRET_ACCESS_KEY").ok();

    Ok(Config {
      listen_addr,
      advertise_addr,
      data_dir,
      heartbeat_interval,
      s3_endpoint,
      s3_bucket,
      s3_region,
      s3_prefix,
      s3_access_key_id,
      s3_secret_access_key,
    })
  }

  pub fn has_s3_config(&self) -> bool {
    self.s3_endpoint.is_some()
      && self.s3_bucket.is_some()
      && self.s3_access_key_id.is_some()
      && self.s3_secret_access_key.is_some()
  }

  pub fn into_s3_config(&self) -> Option<S3Config> {
    Some(S3Config {
      endpoint: self.s3_endpoint.as_ref()?.clone(),
      bucket: self.s3_bucket.as_ref()?.clone(),
      access_key_id: self.s3_access_key_id.as_ref()?.clone(),
      secret_access_key: self.s3_secret_access_key.as_ref()?.clone(),
      path: self.s3_prefix.clone(),
      region: self
        .s3_region
        .as_ref()
        .cloned()
        .unwrap_or_else(|| "us-east-1".to_string()),
    })
  }
}
