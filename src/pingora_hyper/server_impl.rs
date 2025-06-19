use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use crate::pingora_hyper::proxy::{ProxyHttp, RequestHeader, Session};

pub struct ShutdownWatch {
  receiver: watch::Receiver<()>,
}

impl ShutdownWatch {
  pub fn new(receiver: watch::Receiver<()>) -> Self {
    Self { receiver }
  }

  pub async fn changed(&mut self) -> crate::pingora_hyper::error::Result<()> {
    self.receiver.changed().await.map_err(|_| {
      Box::new(crate::pingora_hyper::error::Error::InternalError(
        "shutdown watch closed".to_string(),
      ))
    })
  }
}

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
  inner: Arc<SV>,
  name: String,
}

impl<SV> HttpProxy<SV>
where
  SV: ProxyHttp + Send + Sync + 'static,
{
  pub fn new(inner: SV, name: String) -> Self {
    Self {
      inner: Arc::new(inner),
      name,
    }
  }
}

#[async_trait]
impl<SV> Service for HttpProxy<SV>
where
  SV: ProxyHttp + Send + Sync + 'static,
{
  async fn start_service(&mut self, mut shutdown: ShutdownWatch) {
    println!("Starting HTTP Proxy service: {}", self.name);

    // For now, just wait for shutdown signal
    // TODO: Implement actual HTTP serving logic with ProxyHttp
    if let Err(e) = shutdown.changed().await {
      println!("Shutdown signal received for {}: {}", self.name, e);
    }

    println!("HTTP Proxy service {} shutting down", self.name);
  }

  fn name(&self) -> &str {
    &self.name
  }
}

impl<SV> HttpProxy<SV>
where
  SV: ProxyHttp + Send + Sync + 'static,
{
  /// Start serving HTTP requests on the given listeners
  pub async fn serve_listeners(
    &self,
    listeners: Vec<tokio::net::TcpListener>,
    mut shutdown: ShutdownWatch,
  ) {
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;

    if listeners.is_empty() {
      println!("No listeners provided to HttpProxy");
      return;
    }

    let proxy_service = self.inner.clone();
    let service_fn = service_fn(move |req| {
      let proxy_service = proxy_service.clone();
      async move { proxy_http_bridge(proxy_service, req).await }
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
    println!("Shutting down HTTP proxy listeners");

    // Cancel all listener tasks
    for handle in handles {
      handle.abort();
    }
  }
}

/// A listening service that can be configured with TCP addresses
pub struct ListeningService<T> {
  inner: T,
  tcp_addresses: Vec<String>,
  listeners: Option<Vec<tokio::net::TcpListener>>,
}

impl<T> ListeningService<T> {
  pub fn new(inner: T) -> Self {
    Self {
      inner,
      tcp_addresses: Vec::new(),
      listeners: None,
    }
  }

  pub fn add_tcp(&mut self, addr: &str) {
    self.tcp_addresses.push(addr.to_string());
  }
}

#[async_trait]
impl<SV> Service for ListeningService<HttpProxy<SV>>
where
  SV: ProxyHttp + Send + Sync + 'static,
{
  async fn start_service(&mut self, shutdown: ShutdownWatch) {
    use tokio::net::TcpListener;

    if self.tcp_addresses.is_empty() {
      println!(
        "No TCP addresses configured for {}, just waiting for shutdown",
        self.name()
      );
      self.inner.start_service(shutdown).await;
      return;
    }

    // Create listeners
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
      self.inner.start_service(shutdown).await;
      return;
    }

    // Pass listeners to HttpProxy for serving
    self.inner.serve_listeners(listeners, shutdown).await;
  }

  fn name(&self) -> &str {
    self.inner.name()
  }
}

pub fn http_proxy_service<SV>(
  _conf: &Arc<ServerConf>,
  inner: SV,
) -> ListeningService<HttpProxy<SV>>
where
  SV: ProxyHttp + Send + Sync + 'static,
{
  let proxy = HttpProxy::new(inner, "HTTP Proxy Service".to_string());
  ListeningService::new(proxy)
}

/// Bridge function that converts hyper::Request to Pingora Session
/// and calls the ProxyHttp implementation
async fn proxy_http_bridge<SV>(
  proxy_service: Arc<SV>,
  hyper_req: hyper::Request<hyper::body::Incoming>,
) -> Result<hyper::Response<Full<Bytes>>, hyper::Error>
where
  SV: ProxyHttp + Send + Sync + 'static,
{
  // Convert hyper::Request to Pingora RequestHeader
  let (parts, body) = hyper_req.into_parts();
  let req_header = RequestHeader {
    method: parts.method,
    uri: parts.uri,
    version: parts.version,
    headers: parts.headers,
  };

  // Read the body
  let body_bytes = match body.collect().await {
    Ok(collected) => collected.to_bytes(),
    Err(_) => Bytes::new(),
  };

  // Create a Session with the request
  let mut session = Session::new(req_header, body_bytes);
  let mut ctx = proxy_service.new_ctx();

  // Execute ProxyHttp flow
  match proxy_service.request_filter(&mut session, &mut ctx).await {
    Ok(true) => {
      // request_filter handled everything, return the response
      return Ok(session.build_response());
    }
    Ok(false) => {
      // Continue to upstream logic
    }
    Err(e) => {
      eprintln!("request_filter error: {:?}", e);
      let body = Full::new(Bytes::from("Internal Server Error"));
      return Ok(hyper::Response::builder().status(500).body(body).unwrap());
    }
  }

  // Get upstream peer
  let _upstream_peer =
    match proxy_service.upstream_peer(&mut session, &mut ctx).await {
      Ok(peer) => peer,
      Err(e) => {
        eprintln!("upstream_peer error: {:?}", e);
        let body = Full::new(Bytes::from("Bad Gateway"));
        return Ok(hyper::Response::builder().status(502).body(body).unwrap());
      }
    };

  // For now, just return the response that was built
  // TODO: Implement actual upstream proxying
  proxy_service.logging(&mut session, None, &mut ctx).await;
  Ok(session.build_response())
}
