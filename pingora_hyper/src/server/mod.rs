// Re-export from parent server_impl
pub use crate::server_impl::{
  http_proxy_service, Server, ServerConf, Service, ShutdownWatch,
};

// Configuration compatibility
pub mod configuration {
  pub use crate::server_impl::ServerConf;
}
