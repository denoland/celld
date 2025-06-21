// Unified Pingora compatibility layer

#[cfg(all(feature = "hyper-compat", feature = "pingora-only"))]
compile_error!("Cannot enable both hyper-compat and pingora-only features");

pub mod error;
pub mod http;
pub mod proxy;
pub mod server;
mod server_impl;
pub mod services;
pub mod upstreams;

// Re-export core types
pub use error::{Error, ErrorType, Result};
pub use server_impl::{
  http_proxy_service, Server, ServerConf, Service, ShutdownWatch,
};

// Prelude for common imports
pub mod prelude {
  pub use crate::http::StatusCode;
  pub use crate::proxy::{ProxyHttp, RequestHeader, ResponseHeader, Session};
  pub use crate::server_impl::{http_proxy_service, Server, ServerConf};
  pub use crate::services::background::{
    background_service, BackgroundService,
  };
  pub use crate::upstreams::peer::HttpPeer;
  pub use crate::{Error, ErrorType, Result};
}

// Pingora compatibility when needed
pub mod pingora {
  pub mod prelude {
    pub use crate::prelude::*;
  }

  pub mod http {
    pub use crate::http::*;
    pub use crate::proxy::ResponseHeader;
  }

  pub mod server {
    pub use crate::server::*;
    pub use crate::server_impl::*;

    pub mod configuration {
      pub use crate::server_impl::ServerConf;
    }
  }

  pub mod services {
    pub mod background {
      pub use crate::services::background::*;
    }
  }
}
