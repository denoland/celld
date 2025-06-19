use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
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
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async move {
      use tokio::signal;
      use tokio::sync::watch;

      let (shutdown_tx, shutdown_rx) = watch::channel(());

      // Start all services
      let mut service_handles = Vec::new();

      for mut service in self.services.into_iter() {
        let shutdown_watch = ShutdownWatch::new(shutdown_rx.clone());
        let handle = tokio::spawn(async move {
          service.start_service(shutdown_watch).await;
        });
        service_handles.push(handle);
      }

      // Wait for shutdown signal
      let mut sigterm =
        signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
      let mut sigint =
        signal::unix::signal(signal::unix::SignalKind::interrupt()).unwrap();

      tokio::select! {
        _ = sigterm.recv() => {
          println!("Received SIGTERM");
        }
        _ = sigint.recv() => {
          println!("Received SIGINT");
        }
      }

      // Send shutdown signal
      let _ = shutdown_tx.send(());

      // Wait for services to shutdown with timeout
      let grace_period =
        Duration::from_secs(self._conf.grace_period_seconds.unwrap_or(300));

      println!(
        "Waiting up to {}s for services to shutdown gracefully...",
        grace_period.as_secs()
      );

      let shutdown_future = async {
        for handle in service_handles {
          let _ = handle.await;
        }
      };

      if tokio::time::timeout(grace_period, shutdown_future)
        .await
        .is_err()
      {
        println!("Grace period expired, forcing shutdown");
      }

      println!("Shutdown complete");
    });

    std::process::exit(0);
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
  async fn start_service(&mut self, mut shutdown: ShutdownWatch) {
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;

    println!("Starting HTTP Proxy service: {}", self.name);

    // For now, just wait for shutdown signal
    // TODO: Implement actual HTTP serving logic
    if let Err(e) = shutdown.changed().await {
      println!("Shutdown signal received for {}: {}", self.name, e);
    }

    println!("HTTP Proxy service {} shutting down", self.name);
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
  async fn start_service(&mut self, mut shutdown: ShutdownWatch) {
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use tokio::net::TcpListener;

    if self.tcp_addresses.is_empty() {
      println!(
        "No TCP addresses configured for {}, just waiting for shutdown",
        self.name()
      );
      let _ = shutdown.changed().await;
      return;
    }

    let mut listeners = Vec::new();
    for addr in &self.tcp_addresses {
      match TcpListener::bind(addr).await {
        Ok(listener) => {
          println!("Listening on {}", addr);
          listeners.push(listener);
        }
        Err(e) => {
          println!("Failed to bind to {}: {}", addr, e);
        }
      }
    }

    if listeners.is_empty() {
      println!("No successful listeners for {}", self.name());
      return;
    }

    let service_fn = service_fn(|req| async move {
      use bytes::Bytes;
      use http_body_util::Full;

      // Simple health check response for now
      if req.uri().path() == "/_health" {
        let body = Full::new(Bytes::from("OK"));
        let response =
          hyper::Response::builder().status(200).body(body).unwrap();
        Ok::<_, hyper::Error>(response)
      } else {
        let body = Full::new(Bytes::from("Not Found"));
        let response =
          hyper::Response::builder().status(404).body(body).unwrap();
        Ok(response)
      }
    });

    // Start accepting connections on all listeners
    let mut handles = Vec::new();

    for listener in listeners {
      let service_fn = service_fn.clone();
      let handle = tokio::spawn(async move {
        loop {
          match listener.accept().await {
            Ok((stream, _)) => {
              let io = TokioIo::new(stream);
              let service_fn = service_fn.clone();

              tokio::spawn(async move {
                if let Err(err) =
                  http1::Builder::new().serve_connection(io, service_fn).await
                {
                  println!("Error serving connection: {:?}", err);
                }
              });
            }
            Err(e) => {
              println!("Accept error: {}", e);
              break;
            }
          }
        }
      });
      handles.push(handle);
    }

    // Wait for shutdown signal
    let _ = shutdown.changed().await;
    println!("Shutting down {} listeners", self.name());

    // Cancel all listener tasks
    for handle in handles {
      handle.abort();
    }

    // Also shutdown inner service
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
