use async_trait::async_trait;
use std::sync::Arc;

use crate::server::{Service, ShutdownWatch};

#[async_trait]
pub trait BackgroundService: Send + Sync {
  async fn start(&self, mut shutdown: ShutdownWatch);
}

/// A generic type of background service
pub struct GenBackgroundService<A> {
  // Name of the service
  name: String,
  // Task the service will execute
  task: Arc<A>,
  /// The number of threads. Default is 1
  pub threads: Option<usize>,
}

impl<A> GenBackgroundService<A> {
  /// Generates a background service that can run in the pingora runtime
  pub fn new(name: String, task: Arc<A>) -> Self {
    Self {
      name,
      task,
      threads: Some(1),
    }
  }

  /// Return the task behind [Arc] to be shared other logic.
  pub fn task(&self) -> Arc<A> {
    self.task.clone()
  }
}

#[async_trait]
impl<A> Service for GenBackgroundService<A>
where
  A: BackgroundService + Send + Sync + 'static,
{
  async fn start_service(&mut self, shutdown: ShutdownWatch) {
    self.task.start(shutdown).await;
  }

  fn name(&self) -> &str {
    &self.name
  }

  fn threads(&self) -> Option<usize> {
    self.threads
  }
}

pub fn background_service<SV>(
  name: &str,
  task: SV,
) -> GenBackgroundService<SV> {
  GenBackgroundService::new(format!("BG {name}"), Arc::new(task))
}
