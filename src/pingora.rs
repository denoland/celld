//! Unified Pingora compatibility layer
//!
//! This module provides a single import point that automatically uses
//! either the original Pingora or the hyper-compatible implementation
//! based on feature flags.

#[cfg(not(feature = "hyper-compat"))]
pub use pingora_real::*;

#[cfg(feature = "hyper-compat")]
pub use pingora_hyper_impl::pingora::*;

/// Re-export common types for easier imports
pub mod prelude {
  #[cfg(not(feature = "hyper-compat"))]
  pub use pingora_real::prelude::*;

  #[cfg(feature = "hyper-compat")]
  pub use pingora_hyper_impl::pingora::prelude::*;
}
