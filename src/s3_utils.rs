use tracing::error;

/// Enhanced S3 error logging function.
///
/// This function logs detailed information about S3 SDK errors,
/// including error type, operation details, and context.
#[allow(dead_code)]
pub fn log_s3_error(
  operation_description: &str,
  error: &impl std::fmt::Debug,
  bucket: Option<&str>,
  key: Option<&str>,
) {
  // Extract error details as a debug string
  let error_debug = format!("{:?}", error);

  // Log the error with structured fields
  if let (Some(b), Some(k)) = (bucket, key) {
    error!(
        target: "celld::s3_operation",
        operation = operation_description,
        error = %error_debug,
        bucket = %b,
        key = %k,
        "S3 operation failed"
    );
  } else if let Some(b) = bucket {
    error!(
        target: "celld::s3_operation",
        operation = operation_description,
        error = %error_debug,
        bucket = %b,
        "S3 operation failed"
    );
  } else if let Some(k) = key {
    error!(
        target: "celld::s3_operation",
        operation = operation_description,
        error = %error_debug,
        key = %k,
        "S3 operation failed"
    );
  } else {
    error!(
        target: "celld::s3_operation",
        operation = operation_description,
        error = %error_debug,
        "S3 operation failed"
    );
  }
}
