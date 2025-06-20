use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use crate::proxy::{ProxyHttp, RequestHeader, Session};
use crate::upstreams::peer::HttpPeer;

/// Helper function to create error responses with BoxBody
fn error_response(
  status: u16,
  message: &str,
) -> hyper::Response<BoxBody<Bytes, hyper::Error>> {
  let body = Full::new(Bytes::from(message.to_string()))
    .map_err(|never| match never {})
    .boxed();
  hyper::Response::builder()
    .status(status)
    .body(body)
    .unwrap()
}

pub struct ShutdownWatch {
  receiver: watch::Receiver<()>,
}

impl ShutdownWatch {
  pub fn new(receiver: watch::Receiver<()>) -> Self {
    Self { receiver }
  }

  pub async fn changed(&mut self) -> crate::error::Result<()> {
    self.receiver.changed().await.map_err(|_| {
      Box::new(crate::error::Error::InternalError(
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
  #[allow(dead_code)]
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
    // HTTP serving implemented via ListeningService and serve_listeners()
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
                if let Err(err) = http1::Builder::new()
                  .serve_connection(io, service_fn)
                  .with_upgrades()
                  .await
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
/// and calls the ProxyHttp implementation with streaming support
async fn proxy_http_bridge<SV>(
  proxy_service: Arc<SV>,
  hyper_req: hyper::Request<hyper::body::Incoming>,
) -> Result<hyper::Response<BoxBody<Bytes, hyper::Error>>, hyper::Error>
where
  SV: ProxyHttp + Send + Sync + 'static,
{
  // Check if this is a WebSocket upgrade request early
  let is_websocket = hyper_req.headers().get("sec-websocket-key").is_some();

  // For WebSocket upgrades, we need to handle differently to preserve the original request
  if is_websocket {
    return handle_websocket_bridge(proxy_service, hyper_req).await;
  }
  // Convert hyper::Request to Pingora RequestHeader
  let (parts, body) = hyper_req.into_parts();
  let req_header = RequestHeader {
    method: parts.method,
    uri: parts.uri,
    version: parts.version,
    headers: parts.headers,
  };

  // Create a Session with the streaming request body
  let mut session = Session::new(req_header, body);
  let mut ctx = proxy_service.new_ctx();

  // Store the original URI before request_filter potentially modifies it
  let original_uri = session.req_header().uri.clone();

  // Execute ProxyHttp flow
  match proxy_service.request_filter(&mut session, &mut ctx).await {
    Ok(true) => {
      // request_filter handled everything, return the response
      return Ok(session.build_response_boxed());
    }
    Ok(false) => {
      // Continue to upstream logic
    }
    Err(e) => {
      eprintln!("request_filter error: {:?}", e);
      return Ok(error_response(500, "Internal Server Error"));
    }
  }

  // Get upstream peer
  let upstream_peer =
    match proxy_service.upstream_peer(&mut session, &mut ctx).await {
      Ok(peer) => peer,
      Err(e) => {
        eprintln!("upstream_peer error: {:?}", e);
        return Ok(error_response(502, "Bad Gateway"));
      }
    };

  // Create upstream request based on the original request
  let mut upstream_request = session.req_header().clone();
  // If the URI was lost during request_filter, restore the original
  if upstream_request.uri.path().is_empty() {
    upstream_request.uri = original_uri.clone();
  }

  // Apply upstream request filter
  if let Err(e) = proxy_service
    .upstream_request_filter(&mut session, &mut upstream_request, &mut ctx)
    .await
  {
    eprintln!("upstream_request_filter error: {:?}", e);
    return Ok(error_response(502, "Bad Gateway"));
  }

  // Process request body through filters and collect for upstream
  let mut request_body_chunks = Vec::new();
  loop {
    match session.read_request_body().await {
      Ok(Some(chunk)) => {
        let end_of_stream = false; // We'll know it's the end when we get None
        if let Err(e) = proxy_service
          .request_body_filter(
            &mut session,
            &mut Some(chunk.clone()),
            end_of_stream,
            &mut ctx,
          )
          .await
        {
          eprintln!("request_body_filter error: {:?}", e);
          return Ok(error_response(500, "Internal Server Error"));
        }
        request_body_chunks.push(chunk);
      }
      Ok(None) => {
        // End of stream, call filter one last time
        if let Err(e) = proxy_service
          .request_body_filter(&mut session, &mut None, true, &mut ctx)
          .await
        {
          eprintln!("request_body_filter error on end of stream: {:?}", e);
          return Ok(error_response(500, "Internal Server Error"));
        }
        break;
      }
      Err(e) => {
        eprintln!("Error reading request body: {:?}", e);
        return Ok(error_response(400, "Bad Request"));
      }
    }
  }

  // Combine all chunks for upstream forwarding
  let body_bytes = if request_body_chunks.is_empty() {
    Bytes::new()
  } else {
    let total_len: usize = request_body_chunks.iter().map(|b| b.len()).sum();
    let mut combined = BytesMut::with_capacity(total_len);
    for chunk in request_body_chunks {
      combined.extend_from_slice(&chunk);
    }
    combined.freeze()
  };

  // Handle upstream connection with streaming - now returns BoxBody directly
  let upstream_response = if upstream_peer.is_uds {
    // Unix socket connection with streaming
    match handle_uds_upstream_streaming(
      &upstream_peer,
      upstream_request.clone(),
      &body_bytes,
    )
    .await
    {
      Ok(streaming_response) => {
        // Convert Incoming body to BoxBody for streaming
        let (parts, body) = streaming_response.into_parts();
        let boxed_body = body.boxed();

        let mut response_builder = hyper::Response::builder()
          .status(parts.status)
          .version(parts.version);

        for (name, value) in parts.headers.iter() {
          response_builder = response_builder.header(name, value);
        }

        Ok::<hyper::Response<BoxBody<Bytes, hyper::Error>>, hyper::Error>(
          response_builder.body(boxed_body).unwrap(),
        )
      }
      Err(e) => {
        eprintln!("Error in UDS streaming: {:?}", e);
        Ok(error_response(502, "Bad Gateway"))
      }
    }
  } else {
    // TCP connection with streaming
    match handle_tcp_upstream_streaming(
      &upstream_peer,
      upstream_request.clone(),
      &body_bytes,
    )
    .await
    {
      Ok(streaming_response) => {
        // Convert Incoming body to BoxBody for streaming
        let (parts, body) = streaming_response.into_parts();
        let boxed_body = body.boxed();

        let mut response_builder = hyper::Response::builder()
          .status(parts.status)
          .version(parts.version);

        for (name, value) in parts.headers.iter() {
          response_builder = response_builder.header(name, value);
        }

        Ok::<hyper::Response<BoxBody<Bytes, hyper::Error>>, hyper::Error>(
          response_builder.body(boxed_body).unwrap(),
        )
      }
      Err(e) => {
        eprintln!("Error in TCP streaming: {:?}", e);
        Ok(error_response(502, "Bad Gateway"))
      }
    }
  };

  match upstream_response {
    Ok(response) => {
      proxy_service.logging(&mut session, None, &mut ctx).await;
      Ok(response)
    }
    Err(e) => {
      eprintln!("upstream connection error: {:?}", e);
      // Convert to Pingora Error for logging
      use crate::error::{Error, ErrorType};
      let pingora_error = Error::explain(
        ErrorType::InternalError,
        &format!("Upstream error: {}", e),
      );
      proxy_service
        .logging(&mut session, Some(&pingora_error), &mut ctx)
        .await;
      Ok(error_response(502, "Bad Gateway"))
    }
  }
}

/// Handle WebSocket upgrade bridge - processes through ProxyHttp flow then upgrades
async fn handle_websocket_bridge<SV>(
  proxy_service: Arc<SV>,
  hyper_req: hyper::Request<hyper::body::Incoming>,
) -> Result<hyper::Response<BoxBody<Bytes, hyper::Error>>, hyper::Error>
where
  SV: ProxyHttp + Send + Sync + 'static,
{
  // Convert request for ProxyHttp processing
  let (parts, body) = hyper_req.into_parts();
  let req_header = RequestHeader {
    method: parts.method.clone(),
    uri: parts.uri.clone(),
    version: parts.version,
    headers: parts.headers.clone(),
  };

  // Create a Session with streaming body (should be empty for WebSocket upgrade)
  let mut session = Session::new(req_header, body);
  let mut ctx = proxy_service.new_ctx();
  let original_uri = session.req_header().uri.clone();

  // Process any request body (should be empty for WebSocket, but handle it anyway)
  let mut request_body_chunks = Vec::new();
  loop {
    match session.read_request_body().await {
      Ok(Some(chunk)) => {
        let end_of_stream = false;
        if let Err(e) = proxy_service
          .request_body_filter(
            &mut session,
            &mut Some(chunk.clone()),
            end_of_stream,
            &mut ctx,
          )
          .await
        {
          eprintln!("WebSocket request_body_filter error: {:?}", e);
          return Ok(error_response(500, "Internal Server Error"));
        }
        request_body_chunks.push(chunk);
      }
      Ok(None) => {
        // End of stream
        if let Err(e) = proxy_service
          .request_body_filter(&mut session, &mut None, true, &mut ctx)
          .await
        {
          eprintln!(
            "WebSocket request_body_filter error on end of stream: {:?}",
            e
          );
          return Ok(error_response(500, "Internal Server Error"));
        }
        break;
      }
      Err(e) => {
        eprintln!("WebSocket error reading request body: {:?}", e);
        return Ok(error_response(400, "Bad Request"));
      }
    }
  }

  // Execute ProxyHttp flow
  match proxy_service.request_filter(&mut session, &mut ctx).await {
    Ok(true) => {
      // request_filter handled everything, but this shouldn't happen for WebSocket
      return Ok(error_response(400, "Bad Request"));
    }
    Ok(false) => {
      // Continue to upstream logic
    }
    Err(e) => {
      eprintln!("WebSocket request_filter error: {:?}", e);
      return Ok(error_response(500, "Internal Server Error"));
    }
  }

  // Get upstream peer
  let upstream_peer =
    match proxy_service.upstream_peer(&mut session, &mut ctx).await {
      Ok(peer) => peer,
      Err(e) => {
        eprintln!("WebSocket upstream_peer error: {:?}", e);
        return Ok(error_response(502, "Bad Gateway"));
      }
    };

  // Create upstream request
  let mut upstream_request = session.req_header().clone();
  if upstream_request.uri.path().is_empty() {
    upstream_request.uri = original_uri.clone();
  }

  // Apply upstream request filter
  if let Err(e) = proxy_service
    .upstream_request_filter(&mut session, &mut upstream_request, &mut ctx)
    .await
  {
    eprintln!("WebSocket upstream_request_filter error: {:?}", e);
    return Ok(error_response(502, "Bad Gateway"));
  }

  // Handle WebSocket upgrade
  let websocket_result = if upstream_peer.is_uds {
    handle_websocket_uds_upgrade(&upstream_peer, upstream_request, parts).await
  } else {
    handle_websocket_tcp_upgrade(&upstream_peer, upstream_request, parts).await
  };

  match websocket_result {
    Ok(response) => {
      proxy_service.logging(&mut session, None, &mut ctx).await;
      Ok(response)
    }
    Err(e) => {
      eprintln!("WebSocket upgrade error: {:?}", e);
      // Convert to Pingora Error for logging
      use crate::error::{Error, ErrorType};
      let pingora_error = Error::explain(
        ErrorType::InternalError,
        &format!("WebSocket upgrade error: {}", e),
      );
      proxy_service
        .logging(&mut session, Some(&pingora_error), &mut ctx)
        .await;
      Ok(error_response(502, "Bad Gateway"))
    }
  }
}

/// Handle WebSocket upgrade over Unix domain socket
async fn handle_websocket_uds_upgrade(
  upstream_peer: &HttpPeer,
  upstream_request: RequestHeader,
  client_parts: http::request::Parts,
) -> Result<
  hyper::Response<BoxBody<Bytes, hyper::Error>>,
  Box<dyn std::error::Error + Send + Sync>,
> {
  use http_body_util::Empty;
  use hyper::header::{CONNECTION, SEC_WEBSOCKET_KEY, UPGRADE};
  use hyper_util::rt::TokioIo;
  use tokio::net::UnixStream;

  // Connect to the Unix domain socket
  let uds_stream = match UnixStream::connect(&upstream_peer.address).await {
    Ok(stream) => stream,
    Err(e) => {
      eprintln!("Failed to connect to UDS {}: {}", upstream_peer.address, e);
      return Err(e.into());
    }
  };

  // Build the upstream WebSocket upgrade request
  let mut req_builder = hyper::Request::builder()
    .method("GET")
    .uri(upstream_request.uri.clone())
    .version(hyper::Version::HTTP_11);

  // Copy over headers required for the WebSocket upgrade
  for (name, value) in upstream_request.headers.iter() {
    if name == UPGRADE
      || name == CONNECTION
      || name == SEC_WEBSOCKET_KEY
      || name.as_str().starts_with("sec-websocket-")
    {
      req_builder = req_builder.header(name, value);
    }
  }

  // Build the upgrade request for the UDS socket
  let uds_req = req_builder.body(Empty::<Bytes>::new()).unwrap();

  // Create a client for the UDS socket
  let io = TokioIo::new(uds_stream);
  let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

  // Start a task to handle the HTTP connection
  tokio::spawn(async move {
    if let Err(e) = conn.with_upgrades().await {
      eprintln!("UDS WebSocket client connection error: {}", e);
    }
  });

  // Send the WebSocket upgrade request to the UDS server
  let uds_res = sender.send_request(uds_req).await?;

  // Check if the upgrade was successful
  if uds_res.status() != hyper::StatusCode::SWITCHING_PROTOCOLS {
    return Err(
      format!(
        "UDS WebSocket upgrade failed with status: {}",
        uds_res.status()
      )
      .into(),
    );
  }

  // Build the upgrade response for the client
  let mut res_builder = hyper::Response::builder()
    .status(hyper::StatusCode::SWITCHING_PROTOCOLS)
    .header(UPGRADE, "websocket")
    .header(CONNECTION, "upgrade");

  // Copy relevant headers from the UDS server response
  for (name, value) in uds_res.headers() {
    if name.as_str().starts_with("sec-websocket-") {
      res_builder = res_builder.header(name, value);
    }
  }

  // Create the final response
  let res = res_builder
    .body(
      Full::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed(),
    )
    .unwrap();

  // Rebuild the client request for upgrade
  let client_req =
    hyper::Request::from_parts(client_parts, Empty::<Bytes>::new());

  // Handle the upgraded connection in a background task
  tokio::spawn(async move {
    match hyper::upgrade::on(client_req).await {
      Ok(client_upgraded) => {
        // Forward data between client and UDS
        match hyper::upgrade::on(uds_res).await {
          Ok(uds_upgraded) => {
            let mut client_io = TokioIo::new(client_upgraded);
            let mut uds_io = TokioIo::new(uds_upgraded);

            // Copy data between the two connections
            if let Err(e) =
              tokio::io::copy_bidirectional(&mut client_io, &mut uds_io).await
            {
              eprintln!("Error in WebSocket proxy: {}", e);
            }
          }
          Err(e) => eprintln!("Error upgrading UDS connection: {}", e),
        }
      }
      Err(e) => eprintln!("Error upgrading client connection: {}", e),
    }
  });

  Ok(res)
}

/// Handle WebSocket upgrade over TCP
async fn handle_websocket_tcp_upgrade(
  upstream_peer: &HttpPeer,
  upstream_request: RequestHeader,
  client_parts: http::request::Parts,
) -> Result<
  hyper::Response<BoxBody<Bytes, hyper::Error>>,
  Box<dyn std::error::Error + Send + Sync>,
> {
  use http_body_util::Empty;
  use hyper::header::{CONNECTION, SEC_WEBSOCKET_KEY, UPGRADE};
  use hyper_util::rt::TokioIo;
  use tokio::net::TcpStream;

  // Connect to the TCP address
  let tcp_stream = match TcpStream::connect(&upstream_peer.address).await {
    Ok(stream) => stream,
    Err(e) => {
      eprintln!("Failed to connect to TCP {}: {}", upstream_peer.address, e);
      return Err(e.into());
    }
  };

  // Build the upstream WebSocket upgrade request
  let mut req_builder = hyper::Request::builder()
    .method("GET")
    .uri(upstream_request.uri.clone())
    .version(hyper::Version::HTTP_11);

  // Copy over headers required for the WebSocket upgrade
  for (name, value) in upstream_request.headers.iter() {
    if name == UPGRADE
      || name == CONNECTION
      || name == SEC_WEBSOCKET_KEY
      || name.as_str().starts_with("sec-websocket-")
    {
      req_builder = req_builder.header(name, value);
    }
  }

  // Build the upgrade request for the TCP socket
  let tcp_req = req_builder.body(Empty::<Bytes>::new()).unwrap();

  // Create a client for the TCP socket
  let io = TokioIo::new(tcp_stream);
  let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

  // Start a task to handle the HTTP connection
  tokio::spawn(async move {
    if let Err(e) = conn.with_upgrades().await {
      eprintln!("TCP WebSocket client connection error: {}", e);
    }
  });

  // Send the WebSocket upgrade request to the TCP server
  let tcp_res = sender.send_request(tcp_req).await?;

  // Check if the upgrade was successful
  if tcp_res.status() != hyper::StatusCode::SWITCHING_PROTOCOLS {
    return Err(
      format!(
        "TCP WebSocket upgrade failed with status: {}",
        tcp_res.status()
      )
      .into(),
    );
  }

  // Build the upgrade response for the client
  let mut res_builder = hyper::Response::builder()
    .status(hyper::StatusCode::SWITCHING_PROTOCOLS)
    .header(UPGRADE, "websocket")
    .header(CONNECTION, "upgrade");

  // Copy relevant headers from the TCP server response
  for (name, value) in tcp_res.headers() {
    if name.as_str().starts_with("sec-websocket-") {
      res_builder = res_builder.header(name, value);
    }
  }

  // Create the final response
  let res = res_builder
    .body(
      Full::new(Bytes::new())
        .map_err(|never| match never {})
        .boxed(),
    )
    .unwrap();

  // Rebuild the client request for upgrade
  let client_req =
    hyper::Request::from_parts(client_parts, Empty::<Bytes>::new());

  // Handle the upgraded connection in a background task
  tokio::spawn(async move {
    match hyper::upgrade::on(client_req).await {
      Ok(client_upgraded) => {
        // Forward data between client and TCP upstream
        match hyper::upgrade::on(tcp_res).await {
          Ok(tcp_upgraded) => {
            let mut client_io = TokioIo::new(client_upgraded);
            let mut tcp_io = TokioIo::new(tcp_upgraded);

            // Copy data between the two connections
            if let Err(e) =
              tokio::io::copy_bidirectional(&mut client_io, &mut tcp_io).await
            {
              eprintln!("Error in TCP WebSocket proxy: {}", e);
            }
          }
          Err(e) => eprintln!("Error upgrading TCP connection: {}", e),
        }
      }
      Err(e) => eprintln!("Error upgrading client connection: {}", e),
    }
  });

  Ok(res)
}

/// Handle upstream connection via Unix domain socket with streaming
async fn handle_uds_upstream_streaming(
  upstream_peer: &HttpPeer,
  upstream_request: RequestHeader,
  body_bytes: &Bytes,
) -> Result<
  hyper::Response<hyper::body::Incoming>,
  Box<dyn std::error::Error + Send + Sync>,
> {
  use hyper_util::rt::TokioIo;
  use tokio::net::UnixStream;

  // Connect to the Unix domain socket
  let uds_stream = match UnixStream::connect(&upstream_peer.address).await {
    Ok(stream) => stream,
    Err(e) => {
      eprintln!("Failed to connect to UDS {}: {}", upstream_peer.address, e);
      return Err(e.into());
    }
  };

  // Build the upstream HTTP request
  let mut req_builder = hyper::Request::builder()
    .method(upstream_request.method)
    .uri(upstream_request.uri)
    .version(upstream_request.version);

  // Copy headers
  for (name, value) in upstream_request.headers.iter() {
    req_builder = req_builder.header(name, value);
  }

  // Create request with body
  let upstream_req = req_builder.body(Full::new(body_bytes.clone())).unwrap();

  // Create HTTP client for the UDS connection
  let io = TokioIo::new(uds_stream);
  let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

  // Start a task to handle the HTTP connection
  tokio::spawn(async move {
    if let Err(e) = conn.await {
      eprintln!("UDS client connection error: {}", e);
    }
  });

  // Send the request to the upstream server
  let upstream_response = sender.send_request(upstream_req).await?;

  // Return the streaming response directly without buffering
  let (parts, body) = upstream_response.into_parts();

  // Build final response with streaming body
  let mut response_builder = hyper::Response::builder()
    .status(parts.status)
    .version(parts.version);

  // Copy response headers
  for (name, value) in parts.headers.iter() {
    response_builder = response_builder.header(name, value);
  }

  Ok(response_builder.body(body).unwrap())
}

/// Handle upstream connection via TCP with streaming
async fn handle_tcp_upstream_streaming(
  upstream_peer: &HttpPeer,
  upstream_request: RequestHeader,
  body_bytes: &Bytes,
) -> Result<
  hyper::Response<hyper::body::Incoming>,
  Box<dyn std::error::Error + Send + Sync>,
> {
  use hyper_util::rt::TokioIo;
  use tokio::net::TcpStream;

  // Connect to the TCP address
  let tcp_stream = match TcpStream::connect(&upstream_peer.address).await {
    Ok(stream) => stream,
    Err(e) => {
      eprintln!("Failed to connect to TCP {}: {}", upstream_peer.address, e);
      return Err(e.into());
    }
  };

  // Build the upstream HTTP request
  let mut req_builder = hyper::Request::builder()
    .method(upstream_request.method)
    .uri(upstream_request.uri)
    .version(upstream_request.version);

  // Copy headers
  for (name, value) in upstream_request.headers.iter() {
    req_builder = req_builder.header(name, value);
  }

  // Create request with body
  let upstream_req = req_builder.body(Full::new(body_bytes.clone())).unwrap();

  // Create HTTP client for the TCP connection
  let io = TokioIo::new(tcp_stream);
  let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

  // Start a task to handle the HTTP connection
  tokio::spawn(async move {
    if let Err(e) = conn.await {
      eprintln!("TCP client connection error: {}", e);
    }
  });

  // Send the request to the upstream server
  let upstream_response = sender.send_request(upstream_req).await?;

  // Return the streaming response directly without buffering
  let (parts, body) = upstream_response.into_parts();

  // Build final response with streaming body
  let mut response_builder = hyper::Response::builder()
    .status(parts.status)
    .version(parts.version);

  // Copy response headers
  for (name, value) in parts.headers.iter() {
    response_builder = response_builder.header(name, value);
  }

  Ok(response_builder.body(body).unwrap())
}
