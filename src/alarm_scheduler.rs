use crate::{system_cell::SystemCell, NodeState};
use chrono::Utc;
use futures::future::{BoxFuture, Shared};
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, info};

pub struct AlarmScheduler {
  pub node_state: Arc<NodeState>,
  pub system_cell_rx:
    Shared<BoxFuture<'static, Result<Arc<SystemCell>, RecvError>>>,
  pub interval: Duration,
}

#[async_trait::async_trait]
impl BackgroundService for AlarmScheduler {
  async fn start(&self, mut shutdown: ShutdownWatch) {
    info!(
      "Starting alarm scheduler service with interval: {:?}",
      self.interval
    );

    let system_cell = tokio::select! {
      _ = shutdown.changed() => {
        info!("Alarm scheduler received shutdown signal before receiving system cell");
        return;
      }
      system_cell = self.system_cell_rx.clone() => {
        system_cell.unwrap()
      }
    };

    let mut interval = tokio::time::interval(self.interval);

    loop {
      tokio::select! {
          _ = shutdown.changed() => {
              info!("Alarm scheduler received shutdown signal after receiving system cell");
              break;
          }
          _ = interval.tick() => {
            if let Err(e) = system_cell.alarm_processor().dispatch(self.node_state.clone(), Utc::now(), 100).await {
              error!(error = ?e, "Error dispatching due alarms");
            }
          }
      }
    }

    info!("Process reaper service stopped");
  }
}
