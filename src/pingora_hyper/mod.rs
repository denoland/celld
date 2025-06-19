#[cfg(feature = "hyper-compat")]
pub mod error;
#[cfg(feature = "hyper-compat")]
pub mod proxy;
#[cfg(feature = "hyper-compat")]
pub mod server;
#[cfg(feature = "hyper-compat")]
pub mod service;

#[cfg(feature = "hyper-compat")]
pub mod prelude {
  pub use crate::pingora_hyper::error::*;
  pub use crate::pingora_hyper::proxy::*;
  pub use crate::pingora_hyper::server::*;
  pub use crate::pingora_hyper::service::*;
}
