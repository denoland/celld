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
              self.process_manager.wait_until_process_cleanup_complete().await;
              break;
          }
          _ = interval.tick() => {
              self.process_manager.request_all_lock_ttls_renewal(self.ttl);
          }
      }
    }

    info!("Lock guard TTL updater service stopped");
  }
}
