use crate::NodeState;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, trace, warn};

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
    let now = std::time::Instant::now();

    // Identify processes to reap without holding the lock too long
    let hosts_to_reap = {
      let processes = self.node_state.process_manager.processes.lock().unwrap();
      let mut to_reap = Vec::new();

      for (host, entry) in processes.iter() {
        if now.duration_since(entry.last_used) > self.idle_timeout
          && entry.active_connections == 0
        {
          info!(
            host = %host,
            pid = entry.pid,
            idle_duration = ?now.duration_since(entry.last_used),
            "Process marked for reaping due to inactivity"
          );
          to_reap.push(host.clone());
        } else if entry.active_connections > 0 {
          trace!(
            host = %host,
            pid = entry.pid,
            active_connections = entry.active_connections,
            "Skipping reap for process with active connections"
          );
        }
      }
      to_reap
    };

    // Process each host to reap, acquiring the lock only for removal
    for host in hosts_to_reap {
      // Remove the entry from the map
      let entry = {
        let mut processes =
          self.node_state.process_manager.processes.lock().unwrap();
        processes.remove(&host)
      };

      // If we got an entry, reap it (without holding the lock)
      if let Some(entry) = entry {
        warn!(
          host = %host,
          pid = entry.pid,
          "Reaping idle process"
        );

        // Kill process using the parent_exit_guard
        entry.parent_exit_guard.kill();
        // The Drop implementation of ChildOnParentExit will finish the process cleanup
        info!(
          host = %host,
          pid = entry.pid,
          "Killed process using parent-exit guard"
        );

        // Attempt to clean up the socket file
        if let Err(e) = tokio::fs::remove_file(&entry.socket_path).await {
          // Log error but continue cleanup - file might already be gone
          if e.kind() != std::io::ErrorKind::NotFound {
            error!(
              host = %host,
              pid = entry.pid,
              socket = %entry.socket_path.display(),
              error = %e,
              "Failed to remove socket file during reap"
            );
          }
        }

        info!(
          host = %host,
          pid = entry.pid,
          "Process reaped successfully"
        );
      }
    }

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
              info!("Process reaper received shutdown signal");
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
