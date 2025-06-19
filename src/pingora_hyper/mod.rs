#[cfg(feature = "hyper-compat")]
pub mod error;
#[cfg(feature = "hyper-compat")]
pub mod http;
#[cfg(feature = "hyper-compat")]
pub mod proxy;
#[cfg(feature = "hyper-compat")]
pub mod server;
#[cfg(feature = "hyper-compat")]
mod server_impl;
#[cfg(feature = "hyper-compat")]
pub mod service;
#[cfg(feature = "hyper-compat")]
pub mod services;
#[cfg(feature = "hyper-compat")]
pub mod upstreams;

// Re-export server types at the top level
#[cfg(feature = "hyper-compat")]
pub use server_impl::{
  http_proxy_service, HttpProxy, ListeningService, Server, ServerConf, Service,
  ShutdownWatch,
};

#[cfg(feature = "hyper-compat")]
pub mod prelude {
  // Re-export types that would be in Pingora's prelude
  pub use crate::pingora_hyper::error::{Error, ErrorType, Result};
  pub use crate::pingora_hyper::proxy::{
    ProxyHttp, RequestHeader, ResponseHeader, Session,
  };
  pub use crate::pingora_hyper::server_impl::{
    http_proxy_service, Server, ServerConf,
  };
  pub use crate::pingora_hyper::upstreams::peer::HttpPeer;

  // Re-export other common types
  pub use crate::pingora_hyper::http::StatusCode;
}
