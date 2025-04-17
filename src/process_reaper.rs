use crate::process_manager::ProcessManager;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use std::time::Duration;
use tracing::{error, info, trace, warn};

/// ProcessReaper service cleans up idle processes
pub struct ProcessReaper {
  process_manager: ProcessManager,
  idle_timeout: Duration,
  interval: Duration,
}

impl ProcessReaper {
  pub fn new(
    process_manager: ProcessManager,
    idle_timeout: Duration,
    interval: Duration,
  ) -> Self {
    Self {
      process_manager,
      idle_timeout,
      interval,
    }
  }

  async fn reap_processes(&self) {
    trace!("Reaper checking for idle processes...");
    let now = std::time::Instant::now();
    let mut hosts_to_reap = Vec::new();

    // Scope the lock to minimize time with the mutex locked
    {
      let processes = self.process_manager.processes.lock().await;

      for (host, entry) in processes.iter() {
        if now.duration_since(entry.last_used) > self.idle_timeout {
          info!(
            host = %host,
            pid = entry.pid,
            idle_duration = ?now.duration_since(entry.last_used),
            "Process marked for reaping due to inactivity"
          );
          hosts_to_reap.push(host.clone());
        }
      }
    }

    // Only lock again if we have something to reap
    if !hosts_to_reap.is_empty() {
      let mut processes = self.process_manager.processes.lock().await;

      for host in hosts_to_reap {
        if let Some(mut entry) = processes.remove(&host) {
          warn!(
            host = %host,
            pid = entry.pid,
            "Reaping idle process"
          );

          // Attempt to kill the process
          if let Err(e) = entry.process_handle.kill().await {
            error!(
              host = %host,
              pid = entry.pid,
              error = %e,
              "Failed to kill process during reap"
            );
          }

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
