pub mod connection_pool;
pub mod error;
pub mod http;
pub mod proxy;
pub mod server;
mod server_impl;
pub mod service;
pub mod services;
pub mod upstreams;

// Re-export server types at the top level
pub use server_impl::{
  http_proxy_service, HttpProxy, ListeningService, Server, ServerConf, Service,
  ShutdownWatch,
};

pub mod prelude {
  // Re-export types that would be in Pingora's prelude
  pub use crate::error::{Error, ErrorType, Result};
  pub use crate::proxy::{
    ProxyHttp, RequestHeader, ResponseHeader, Session,
  };
  pub use crate::server_impl::{
    http_proxy_service, Server, ServerConf,
  };
  pub use crate::upstreams::peer::HttpPeer;

  // Re-export other common types
  pub use crate::http::StatusCode;
}
