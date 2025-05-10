use crate::process_manager::ProcessManager;
use pingora::server::ShutdownWatch;
use pingora::services::background::BackgroundService;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

pub struct LockGuardTTLUpdater {
  pub interval: Duration,
  pub process_manager: Arc<ProcessManager>,
  pub ttl: Duration,
}

#[async_trait::async_trait]
impl BackgroundService for LockGuardTTLUpdater {
  async fn start(&self, mut shutdown: ShutdownWatch) {
    info!(
      "Starting lock guard TTL updater with interval: {:?}",
      self.interval
    );

    let mut interval = tokio::time::interval(self.interval);

    loop {
      tokio::select! {
          _ = shutdown.changed() => {
              info!("Lock guard TTL updater received shutdown signal");
              if let Err(e) = self.process_manager.wait_until_process_cleanup_complete().await {
                tracing::error!(error = ?e, "Error waiting for process cleanup to complete");
              }
              break;
          }
          _ = interval.tick() => {
              if let Err(e) = self.process_manager.renew_all_lock_ttls(self.ttl).await {
                tracing::error!(error = ?e, "Error renewing lock guard TTLs");
              }
          }
      }
    }

    info!("Lock guard TTL updater service stopped");
  }
}
