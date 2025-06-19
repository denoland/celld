#[cfg(feature = "hyper-compat")]
use crate::pingora_hyper::service::{BackgroundService, ShutdownWatch};
use crate::NodeState;
#[cfg(not(feature = "hyper-compat"))]
use pingora::server::ShutdownWatch;
#[cfg(not(feature = "hyper-compat"))]
use pingora::services::background::BackgroundService;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, trace};

/// ProcessReaper service cleans up idle processes
pub struct ProcessReaper {
  node_state: Arc<NodeState>,
  idle_timeout: Duration,
  interval: Duration,
}

impl ProcessReaper {
  pub fn new(
    node_state: Arc<NodeState>,
    idle_timeout: Duration,
    interval: Duration,
  ) -> Self {
    Self {
      node_state,
      idle_timeout,
      interval,
    }
  }

  async fn reap_processes(&self) {
    trace!("Reaper checking for idle processes...");
    let mut reaped_process_keys = HashSet::new();

    for entry in &self.node_state.cell_manager.cells {
      let process_key = entry.key();
      let entry = entry.value();
      let reaped = entry.release_if_idle(self.idle_timeout).await;
      if reaped {
        info!(?process_key, "Process was reaped due to inactivity");
        reaped_process_keys.insert(process_key.clone());
      }
    }

    self
      .node_state
      .cell_manager
      .cells
      .retain(|k, _| !reaped_process_keys.contains(k));

    trace!("Reaper check complete.");
  }
}

#[async_trait::async_trait]
impl BackgroundService for ProcessReaper {
  async fn start(&self, mut shutdown: ShutdownWatch) {
    info!(
      "Starting process reaper service with timeout: {:?}, interval: {:?}",
      self.idle_timeout, self.interval
    );

    let mut interval = tokio::time::interval(self.interval);

    loop {
      tokio::select! {
          _ = shutdown.changed() => {
              info!("Process reaper received shutdown signal - terminating all processes");
              // Terminate all processes when shutdown signal is received
              self.node_state.cell_manager.terminate_all().await;
              break;
          }
          _ = interval.tick() => {
              self.reap_processes().await;
          }
      }
    }

    info!("Process reaper service stopped");
  }
}
