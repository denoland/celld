use crate::{
  process_manager::{ProcessEntry, ReusableProcessEntry},
  NodeState,
};
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
    let processes_to_reap = {
      let processes = self.node_state.process_manager.processes.lock().await;
      let mut to_reap = Vec::new();

      for (process_key, entry) in processes.iter() {
        let has_active_connections = entry.has_active_connections();
        if !has_active_connections
          && now.duration_since(entry.last_used()) > self.idle_timeout
        {
          info!(
            ?process_key,
            pid = entry.pid(),
            idle_duration = ?now.duration_since(entry.last_used()),
            "Process marked for reaping due to inactivity"
          );
          to_reap.push(process_key.clone());
        } else if has_active_connections {
          trace!(
            ?process_key,
            pid = entry.pid(),
            active_connections = has_active_connections,
            "Skipping reap for process with active connections"
          );
        }
      }
      to_reap
    };

    // Process each host to reap, acquiring the lock only for removal
    for process_key in processes_to_reap {
      // Remove the entry from the map - keep lock held minimal time
      let maybe_entry = {
        let mut processes =
          self.node_state.process_manager.processes.lock().await;
        processes.remove(&process_key)
      };

      // If we got an entry, reap it (without holding the lock)
      if let Some(entry) = maybe_entry {
        // TODO Move the following stanza to ProcessEntry::Drop ?

        let pid = entry.pid();
        warn!(?process_key, pid = pid, "Reaping idle process");

        // Kill the litestream replicate process.
        if let ProcessEntry::Reusable(ReusableProcessEntry {
          replica: Some(ref replica),
          ..
        }) = entry
        {
          if let Err(e) = replica.shutdown().await {
            warn!(
              ?process_key,
              error = %e,
              "Error shutting down litestream replicate"
            );
          }
        }

        // Store socket path to clean up after reaping
        let socket_path = entry.socket_path().to_path_buf();

        // Terminate the Deno process
        entry.terminate();

        // Log after the entry is dropped
        info!(
          ?process_key,
          pid = pid,
          "Killed process using parent-exit guard"
        );

        if let Err(e) = std::fs::remove_file(&socket_path) {
          // Log error but continue cleanup - file might already be gone
          if e.kind() != std::io::ErrorKind::NotFound {
            error!(
              ?process_key,
              pid = pid,
              socket = %socket_path.display(),
              error = %e,
              "Failed to remove socket file during reap"
            );
          }
        }

        info!(?process_key, pid = pid, "Process reaped successfully");
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
