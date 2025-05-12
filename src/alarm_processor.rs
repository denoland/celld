use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Debug, thiserror::Error)]
pub enum AlarmError {
  #[error("alarm not found")]
  AlarmNotFound,
}

#[derive(Debug, Clone)]
pub struct Alarm {
  pub tenant: String,
  pub cell_id: String,
  pub scheduled_time_unix_ms: u64,
}

#[async_trait]
pub trait AlarmProcessor {
  /// Get an alarm set for the given tenant and cell id.
  async fn get(&self, tenant: &str, cell_id: &str)
    -> Result<Alarm, AlarmError>;

  /// Delete an alarm set for the given tenant and cell id.
  async fn delete(
    &self,
    tenant: &str,
    cell_id: &str,
  ) -> Result<Alarm, AlarmError>;

  /// Set an alarm for the given tenant and cell id.
  /// If there is already an alarm for the cell, it will be overwritten.
  async fn set(
    &self,
    tenant: &str,
    cell_id: &str,
    scheduled_time_unix_ms: u64,
  ) -> Result<(), AlarmError>;

  /// Dispatch all alarms that are due at the given timestamp. Dispatched alarms
  /// are deleted from the internal datastore.
  async fn dispatch(
    &self,
    current_timestamp: DateTime<Utc>,
  ) -> Result<(), AlarmError>;
}
