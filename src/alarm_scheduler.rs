use crate::node_state::NodeState;

use chrono::Utc;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

pub struct AlarmScheduler {
  pub node_state: Arc<NodeState>,
  pub interval: Duration,
}

#[async_trait::async_trait]
impl BackgroundService for AlarmScheduler {
  async fn start(&self, mut shutdown: ShutdownWatch) {
    info!(
      "Starting alarm scheduler service with interval: {:?}",
      self.interval
    );

    let mut interval = tokio::time::interval(self.interval);

    loop {
      tokio::select! {
        _ = shutdown.changed() => {
            info!("Alarm scheduler received shutdown signal after receiving system cell");
            break;
        }
        _ = interval.tick() => {
          let Some(system_cell_handle) = self.node_state.cell_manager.get_system_cell() else {
            continue;
          };

          if let Err(e) = system_cell_handle.dispatch_alarms(self.node_state.clone(), Utc::now(), 100).await {
            error!(error = ?e, "Error dispatching due alarms");
          }
        }
      }
    }

    info!("Alarm scheduler service stopped");
  }
}
