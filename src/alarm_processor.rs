use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Debug, thiserror::Error)]
pub enum AlarmError {
  #[error("SQLite error: {0}")]
  SQLiteError(#[from] rusqlite::Error),
  #[error("alarm not found")]
  AlarmNotFound,
}

#[derive(Debug, Clone)]
pub struct Alarm {
  pub tenant: String,
  pub cell_id: String,
  pub scheduled_time_unix_ms: u64,
}

#[async_trait(?Send)]
pub trait AlarmProcessor {
  /// Get an alarm set for the given tenant and cell id.
  fn get(&self, tenant: &str, cell_id: &str) -> Result<Alarm, AlarmError>;

  /// Delete an alarm set for the given tenant and cell id.
  fn delete(&self, tenant: &str, cell_id: &str) -> Result<(), AlarmError>;

  /// Set an alarm for the given tenant and cell id.
  /// If there is already an alarm for the cell, it will be overwritten.
  fn set(
    &self,
    tenant: &str,
    cell_id: &str,
    scheduled_time_unix_ms: u64,
  ) -> Result<(), AlarmError>;

  /// Dispatch all alarms that are due at the given timestamp. Dispatched alarms
  /// are deleted from the internal datastore.
  async fn dispatch(
    &mut self,
    current_timestamp: DateTime<Utc>,
    limit: u32,
  ) -> Result<(), AlarmError>;
}
