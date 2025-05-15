use crate::{system_cell::SystemCell, NodeState};
use chrono::Utc;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tracing::{error, info};

pub struct AlarmScheduler {
  pub node_state: Arc<NodeState>,
  pub system_cell_rx:
    StdMutex<Option<tokio::sync::broadcast::Receiver<Arc<SystemCell>>>>,
  pub interval: Duration,
}

#[async_trait::async_trait]
impl BackgroundService for AlarmScheduler {
  async fn start(&self, mut shutdown: ShutdownWatch) {
    info!(
      "Starting alarm scheduler service with interval: {:?}",
      self.interval
    );

    let Some(mut system_cell_rx) = self.system_cell_rx.lock().unwrap().take()
    else {
      return;
    };

    let system_cell = tokio::select! {
      _ = shutdown.changed() => {
        info!("Alarm scheduler received shutdown signal before receiving system cell");
        return;
      }
      system_cell = system_cell_rx.recv() => {
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
            if let Err(e) = system_cell.dispatch_due_alarms(Utc::now(), 100).await {
              error!(error = ?e, "Error dispatching due alarms");
            }
          }
      }
    }

    info!("Process reaper service stopped");
  }
}
