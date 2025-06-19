use std::fmt;

use http::StatusCode;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

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
}

impl Error {
  pub fn explain(&self) -> String {
    self.to_string()
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
    }
  }
}
