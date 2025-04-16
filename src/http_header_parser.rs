use tokio::io::AsyncReadExt;
use http::method::Method;
use http::uri::Uri;

/// Represents parsed HTTP headers with routing information
#[derive(Debug)]
pub struct HttpHeaderInfo {
  /// The host name from the Host header
  pub host: Option<String>,
  /// Whether the request included the x-single-use-isolate header
  pub single_use: bool,
  /// The HTTP method (GET, POST, etc.)
  pub method: Method,
  /// The request path
  pub path: Uri,
  /// The complete header buffer to forward to the upstream
  pub header_buffer: Vec<u8>,
}

impl Default for HttpHeaderInfo {
  fn default() -> Self {
    Self {
      host: None,
      single_use: false,
      method: Method::GET, // Default to GET method
      path: Uri::from_static("/"), // Default to root path
      header_buffer: Vec::new(),
    }
  }
}

/// Parse HTTP headers from an AsyncRead source
///
/// This function reads from the provided stream until it finds the end of HTTP headers,
/// extracts the relevant routing information, and returns the complete header buffer
/// for forwarding to the upstream server.
pub async fn parse_http_headers<R>(
  reader: &mut R,
) -> Result<HttpHeaderInfo, String>
where
  R: AsyncReadExt + Unpin,
{
  let mut buf = vec![0; 16 * 1024]; // 16 KB buffer
  let mut info = HttpHeaderInfo::default();
  let mut headers_complete = false;

  // Read until we find the end of headers or reach a limit
  while !headers_complete {
    // Read a chunk from the stream
    let n = match reader.read(&mut buf).await {
      Ok(0) => return Err("Connection closed during header parsing".into()),
      Ok(n) => n,
      Err(e) => return Err(format!("Error reading from socket: {}", e)),
    };

    // Add the new data to our buffer
    info.header_buffer.extend_from_slice(&buf[..n]);

    // Look for the end of headers marker (\r\n\r\n)
    if let Some(pos) = info
      .header_buffer
      .windows(4)
      .position(|window| window == b"\r\n\r\n")
    {
      headers_complete = true;

      // Parse the headers to extract routing information
      if let Ok(header_str) =
        std::str::from_utf8(&info.header_buffer[..pos + 4])
      {
        // First line should contain method and path
        let lines: Vec<&str> = header_str.lines().collect();
        if !lines.is_empty() {
          // Parse request line (e.g., "GET /path HTTP/1.1")
          let request_line = lines[0];
          let parts: Vec<&str> = request_line.split_whitespace().collect();
          if parts.len() >= 2 {
            // Parse method
            if let Ok(method) = Method::from_bytes(parts[0].as_bytes()) {
              info.method = method;
            }
            
            // Parse path
            if let Ok(uri) = Uri::try_from(parts[1]) {
              info.path = uri;
            }
          }
        }
        
        // Extract host header and other headers
        for line in lines.iter().skip(1) {
          if line.to_lowercase().starts_with("host:") {
            let parts: Vec<&str> = line.splitn(2, ':').collect();
            if parts.len() > 1 {
              let host_value = parts[1].trim();
              info.host = Some(
                host_value
                  .split(':')
                  .next()
                  .unwrap_or(host_value)
                  .to_string(),
              );
            }
          } else if line.to_lowercase().starts_with("x-single-use-isolate:") {
            info.single_use = true;
          }
        }
      }
    }

    // 32 KB max header size
    if info.header_buffer.len() > 32 * 1024 {
      return Err("HTTP headers too large".into());
    }
  }

  // Return both the host and the complete header buffer
  Ok(info)
}

#[cfg(test)]
mod tests {
  use super::*;
  use tokio_test::io::Builder;

  #[tokio::test]
  async fn test_parse_http_headers_simple() {
    let headers = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
    let mut reader = Builder::new().read(headers).build();
    let result = parse_http_headers(&mut reader).await.unwrap();
    assert_eq!(result.host.unwrap(), "example.com");
    assert!(!result.single_use);
    assert_eq!(result.method, Method::GET);
    assert_eq!(result.path, Uri::from_static("/"));
    assert_eq!(result.header_buffer, headers);
  }

  #[tokio::test]
  async fn test_parse_http_headers_with_single_use() {
    let headers = b"GET / HTTP/1.1\r\nHost: test.local\r\nX-Single-Use-Isolate: true\r\n\r\n";
    let mut reader = Builder::new().read(headers).build();
    let result = parse_http_headers(&mut reader).await.unwrap();
    assert_eq!(result.host.unwrap(), "test.local");
    assert!(result.single_use);
    assert_eq!(result.method, Method::GET);
    assert_eq!(result.path, Uri::from_static("/"));
    assert_eq!(result.header_buffer, headers);
  }

  #[tokio::test]
  async fn test_parse_http_headers_multiple_chunks() {
    let chunk1 = b"GET / HTTP/1.1\r\nH";
    let chunk2 = b"ost: multi.chunk\r\n";
    let chunk3 = b"X-Single-Use-Isolate: true\r\n\r\n";
    let mut reader = Builder::new()
      .read(chunk1)
      .read(chunk2)
      .read(chunk3)
      .build();
    let result = parse_http_headers(&mut reader).await.unwrap();
    assert_eq!(result.host.unwrap(), "multi.chunk");
    assert!(result.single_use);
    assert_eq!(result.method, Method::GET);
    assert_eq!(result.path, Uri::from_static("/"));
    let expected = b"GET / HTTP/1.1\r\nHost: multi.chunk\r\nX-Single-Use-Isolate: true\r\n\r\n";
    assert_eq!(result.header_buffer, expected);
  }

  #[tokio::test]
  async fn test_parse_http_headers_missing_host() {
    let headers = b"GET / HTTP/1.1\r\nContent-Type: text/plain\r\n\r\n";
    let mut reader = Builder::new().read(headers).build();
    let result = parse_http_headers(&mut reader).await.unwrap();
    assert!(result.host.is_none());
    assert!(!result.single_use);
    assert_eq!(result.method, Method::GET);
    assert_eq!(result.path, Uri::from_static("/"));
  }

  #[tokio::test]
  async fn test_parse_http_headers_with_port() {
    let headers = b"GET / HTTP/1.1\r\nHost: example.com:8080\r\n\r\n";
    let mut reader = Builder::new().read(headers).build();
    let result = parse_http_headers(&mut reader).await.unwrap();
    assert_eq!(result.host.unwrap(), "example.com");
    assert!(!result.single_use);
    assert_eq!(result.method, Method::GET);
    assert_eq!(result.path, Uri::from_static("/"));
  }

  #[tokio::test]
  async fn test_parse_http_headers_static_file_path() {
    let headers = b"GET /static/index.html HTTP/1.1\r\nHost: ry.local\r\n\r\n";
    let mut reader = Builder::new().read(headers).build();
    let result = parse_http_headers(&mut reader).await.unwrap();
    assert_eq!(result.host.unwrap(), "ry.local");
    assert!(!result.single_use);
    assert_eq!(result.method, Method::GET);
    assert_eq!(result.path, Uri::from_static("/static/index.html"));
  }

  #[tokio::test]
  async fn test_parse_http_headers_post_method() {
    let headers = b"POST /api/data HTTP/1.1\r\nHost: ry.local\r\nContent-Type: application/json\r\n\r\n";
    let mut reader = Builder::new().read(headers).build();
    let result = parse_http_headers(&mut reader).await.unwrap();
    assert_eq!(result.host.unwrap(), "ry.local");
    assert!(!result.single_use);
    assert_eq!(result.method, Method::POST);
    assert_eq!(result.path, Uri::from_static("/api/data"));
  }
}
