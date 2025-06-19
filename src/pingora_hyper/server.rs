use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::watch;

use crate::pingora_hyper::service::ShutdownWatch;

#[derive(Clone)]
pub struct ServerConf {
  pub grace_period_seconds: Option<u64>,
}

impl ServerConf {
  pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
    Ok(Self::default())
  }
}

impl Default for ServerConf {
  fn default() -> Self {
    Self {
      grace_period_seconds: Some(300),
    }
  }
}

/// The service interface
#[async_trait]
pub trait Service: Sync + Send {
  /// This function will be called when the server is ready to start the service.
  async fn start_service(&mut self, shutdown: ShutdownWatch);

  /// The name of the service, just for logging and naming the threads assigned to this service
  fn name(&self) -> &str;

  /// The preferred number of threads to run this service
  fn threads(&self) -> Option<usize> {
    None
  }
}

pub struct Server {
  services: Vec<Box<dyn Service>>,
  _conf: ServerConf,
}

impl Server {
  pub fn new_with_opt_and_conf(_opt: Option<()>, conf: ServerConf) -> Self {
    Self {
      services: Vec::new(),
      _conf: conf,
    }
  }

  pub fn add_service(&mut self, service: impl Service + 'static) {
    self.services.push(Box::new(service));
  }

  pub fn run_forever(self) -> ! {
    todo!("implement run_forever")
  }
}

/// The concrete type that holds the user defined HTTP proxy.
pub struct HttpProxy<SV> {
  inner: SV,
  name: String,
}

impl<SV> HttpProxy<SV> {
  pub fn new(inner: SV, name: String) -> Self {
    Self { inner, name }
  }
}

#[async_trait]
impl<SV> Service for HttpProxy<SV>
where
  SV: Send + Sync + 'static,
{
  async fn start_service(&mut self, _shutdown: ShutdownWatch) {
    todo!("implement HttpProxy service logic")
  }

  fn name(&self) -> &str {
    &self.name
  }
}

/// A listening service that can be configured with TCP addresses
pub struct ListeningService<T> {
  inner: T,
  tcp_addresses: Vec<String>,
}

impl<T> ListeningService<T> {
  pub fn new(inner: T) -> Self {
    Self {
      inner,
      tcp_addresses: Vec::new(),
    }
  }

  pub fn add_tcp(&mut self, addr: &str) {
    self.tcp_addresses.push(addr.to_string());
  }
}

#[async_trait]
impl<T> Service for ListeningService<T>
where
  T: Service,
{
  async fn start_service(&mut self, shutdown: ShutdownWatch) {
    // TODO: Start listening on TCP addresses
    self.inner.start_service(shutdown).await;
  }

  fn name(&self) -> &str {
    self.inner.name()
  }
}

pub fn http_proxy_service<SV>(
  conf: &Arc<ServerConf>,
  inner: SV,
) -> ListeningService<HttpProxy<SV>>
where
  SV: Send + Sync + 'static,
{
  let proxy = HttpProxy::new(inner, "HTTP Proxy Service".to_string());
  ListeningService::new(proxy)
}
