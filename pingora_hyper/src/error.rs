use std::fmt;

use http::StatusCode;
use thiserror::Error;

// Pingora uses Box<Error> for its Result type
pub type Result<T> = std::result::Result<T, Box<Error>>;

// Re-export ErrorType enum with the same values as Pingora
// Note: Currently unused but kept for Pingora compatibility
#[allow(unused_imports)]
pub use self::ErrorType::*;

#[derive(Debug, Error)]
pub enum Error {
  #[error("HTTP error: {0}")]
  HttpError(#[from] http::Error),

  #[error("Hyper error: {0}")]
  HyperError(#[from] hyper::Error),

  #[error("IO error: {0}")]
  IoError(#[from] std::io::Error),

  #[error("Internal error: {0}")]
  InternalError(String),

  #[error("Connection error: {0}")]
  ConnectionError(String),

  #[error("Timeout error")]
  TimeoutError,

  #[error("Bad request: {0}")]
  BadRequest(String),

  #[error("Service unavailable: {0}")]
  ServiceUnavailable(String),

  #[error("Invalid header")]
  InvalidHeader,
}

#[derive(Debug, Clone, Copy)]
pub enum ErrorType {
  InternalError,
  ConnectError,
  ReadError,
  WriteError,
  TimeoutError,
  BadRequest,
  ServiceUnavailable,
  HTTPStatus(u16),
}

impl Error {
  pub fn explain<S: AsRef<str>>(
    error_type: ErrorType,
    message: S,
  ) -> Box<Self> {
    let message = message.as_ref();
    let error = match error_type {
      ErrorType::InternalError => Error::InternalError(message.to_string()),
      ErrorType::ConnectError => Error::ConnectionError(message.to_string()),
      ErrorType::BadRequest => Error::BadRequest(message.to_string()),
      ErrorType::ServiceUnavailable => {
        Error::ServiceUnavailable(message.to_string())
      }
      ErrorType::TimeoutError => Error::TimeoutError,
      ErrorType::HTTPStatus(_) => Error::InternalError(message.to_string()),
      _ => Error::InternalError(format!("{}: {}", error_type, message)),
    };
    Box::new(error)
  }

  pub fn because<
    S: AsRef<str>,
    E: std::error::Error + Send + Sync + 'static,
  >(
    error_type: ErrorType,
    message: S,
    _cause: E,
  ) -> Box<Self> {
    // For now, we ignore the cause and just create an error with the message
    Self::explain(error_type, message)
  }

  pub fn error_type(&self) -> ErrorType {
    match self {
      Error::HttpError(_) | Error::InternalError(_) => ErrorType::InternalError,
      Error::HyperError(_) | Error::ConnectionError(_) => {
        ErrorType::ConnectError
      }
      Error::IoError(_) => ErrorType::ReadError,
      Error::TimeoutError => ErrorType::TimeoutError,
      Error::BadRequest(_) => ErrorType::BadRequest,
      Error::ServiceUnavailable(_) => ErrorType::ServiceUnavailable,
      Error::InvalidHeader => ErrorType::BadRequest,
    }
  }

  pub fn to_status_code(&self) -> StatusCode {
    match self {
      Error::BadRequest(_) => StatusCode::BAD_REQUEST,
      Error::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
      Error::TimeoutError => StatusCode::GATEWAY_TIMEOUT,
      _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
  }
}

impl fmt::Display for ErrorType {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      ErrorType::InternalError => write!(f, "InternalError"),
      ErrorType::ConnectError => write!(f, "ConnectError"),
      ErrorType::ReadError => write!(f, "ReadError"),
      ErrorType::WriteError => write!(f, "WriteError"),
      ErrorType::TimeoutError => write!(f, "TimeoutError"),
      ErrorType::BadRequest => write!(f, "BadRequest"),
      ErrorType::ServiceUnavailable => write!(f, "ServiceUnavailable"),
      ErrorType::HTTPStatus(code) => write!(f, "HTTPStatus({})", code),
    }
  }
}

// Note: CellManagerError conversion handled in router.rs where both types are available
