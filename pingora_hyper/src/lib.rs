// Pingora-compatible HTTP server implementation using Hyper
//
// This crate provides a drop-in replacement for Pingora's HTTP server functionality
// using Hyper as the underlying HTTP implementation, while maintaining full API compatibility.

pub mod connection_pool;
pub mod error;
pub mod http;
pub mod proxy;
pub mod server;
mod server_impl;
pub mod service;
pub mod services;
pub mod upstreams;

// Re-export server types at the top level for compatibility
pub use server_impl::{
  http_proxy_service, HttpProxy, ListeningService, Server, ServerConf, Service,
  ShutdownWatch,
};

// Re-export Pingora-compatible types
pub use error::{Error, ErrorType, Result};

// Main prelude that provides the same API as Pingora's prelude
pub mod prelude {
  // Re-export all the main types that applications expect
  pub use crate::error::{Error, ErrorType, Result};
  pub use crate::http::StatusCode;
  pub use crate::proxy::{ProxyHttp, RequestHeader, ResponseHeader, Session};
  pub use crate::server::ShutdownWatch;
  pub use crate::server_impl::{http_proxy_service, Server, ServerConf};
  pub use crate::services::background::{
    background_service, BackgroundService,
  };
  pub use crate::upstreams::peer::HttpPeer;
}

// Compatibility module that can be used as a drop-in replacement for pingora
// when hyper-compat feature is enabled
pub mod pingora {
  pub use crate::prelude::*;

  pub mod prelude {
    pub use crate::prelude::*;
  }

  pub mod http {
    pub use crate::http::*;
    pub use crate::proxy::ResponseHeader;
  }

  pub mod upstreams {
    pub mod peer {
      pub use crate::upstreams::peer::*;
    }
  }

  pub mod server {
    pub use crate::server::*;
    pub use crate::server_impl::*;

    pub mod configuration {
      pub use crate::server::configuration::*;
    }
  }

  pub mod services {
    pub mod background {
      pub use crate::services::background::*;
    }
  }
}
