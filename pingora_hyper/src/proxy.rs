use async_trait::async_trait;
use http::{HeaderMap, Method, StatusCode, Uri, Version};
use http_body_util::BodyExt;
use hyper::body::Incoming;

use crate::error::{Error, Result};
use crate::upstreams::peer::HttpPeer;

#[derive(Clone)]
pub struct RequestHeader {
  pub method: Method,
  pub uri: Uri,
  pub version: Version,
  pub headers: HeaderMap,
}

impl RequestHeader {
  pub fn set_uri(&mut self, uri: Uri) {
    self.uri = uri;
  }
}

impl AsRef<RequestHeader> for RequestHeader {
  fn as_ref(&self) -> &RequestHeader {
    self
  }
}

impl From<RequestHeader> for http::request::Parts {
  fn from(header: RequestHeader) -> Self {
    let request = http::Request::builder()
      .method(header.method)
      .uri(header.uri)
      .version(header.version)
      .body(())
      .unwrap();
    let (mut parts, _) = request.into_parts();
    parts.headers = header.headers;
    parts
  }
}

impl From<&RequestHeader> for http::request::Parts {
  fn from(header: &RequestHeader) -> Self {
    let request = http::Request::builder()
      .method(header.method.clone())
      .uri(header.uri.clone())
      .version(header.version)
      .body(())
      .unwrap();
    let (mut parts, _) = request.into_parts();
    parts.headers = header.headers.clone();
    parts
  }
}

pub struct ResponseHeader {
  pub status: StatusCode,
  pub headers: HeaderMap,
  #[allow(dead_code)]
  pub version: Version,
}

impl ResponseHeader {
  pub fn build(status: StatusCode, _version: Option<u8>) -> Result<Self> {
    Ok(Self {
      status,
      headers: HeaderMap::new(),
      version: Version::HTTP_11,
    })
  }

  pub fn insert_header<K, V>(&mut self, name: K, value: V) -> Result<()>
  where
    K: TryInto<http::header::HeaderName>,
    V: TryInto<http::header::HeaderValue>,
  {
    match (name.try_into(), value.try_into()) {
      (Ok(name), Ok(value)) => {
        self.headers.insert(name, value);
        Ok(())
      }
      _ => Err(Box::new(Error::InvalidHeader)),
    }
  }
}

impl From<http::response::Parts> for ResponseHeader {
  fn from(parts: http::response::Parts) -> Self {
    Self {
      status: parts.status,
      headers: parts.headers,
      version: parts.version,
    }
  }
}

/// A concrete Session type for the hyper compatibility layer
/// Now supports streaming request bodies via hyper::body::Incoming
pub struct Session {
  req_header: RequestHeader,
  request_body: Option<Incoming>,
  response_status: Option<StatusCode>,
  response_headers: HeaderMap,
  response_body: Vec<bytes::Bytes>, // Used for local content - not critical for proxy streaming
  _keepalive: Option<u64>,
}

impl Session {
  pub fn new(req_header: RequestHeader, body: Incoming) -> Self {
    Self {
      req_header,
      request_body: Some(body),
      response_status: None,
      response_headers: HeaderMap::new(),
      response_body: Vec::new(),
      _keepalive: None,
    }
  }

  /// Create a new Session for WebSocket that doesn't consume the body
  pub fn new_websocket(req_header: RequestHeader) -> Self {
    Self {
      req_header,
      request_body: None, // WebSocket upgrades have no body
      response_status: None,
      response_headers: HeaderMap::new(),
      response_body: Vec::new(),
      _keepalive: None,
    }
  }

  /// Check if this session is for a WebSocket upgrade
  pub fn is_websocket(&self) -> bool {
    self.request_body.is_none()
      && self.req_header.headers.get("sec-websocket-key").is_some()
  }

  pub fn req_header(&self) -> &RequestHeader {
    &self.req_header
  }

  pub fn req_header_mut(&mut self) -> &mut RequestHeader {
    &mut self.req_header
  }

  pub async fn write_response_header(
    &mut self,
    resp: Box<ResponseHeader>,
    _end_of_stream: bool,
  ) -> Result<()> {
    self.response_status = Some(resp.status);
    self.response_headers = resp.headers.clone();
    Ok(())
  }

  /// Writes response body data - currently used for local content only
  /// Application calls this once with complete content (end_of_stream=true)
  /// Critical proxy streaming is handled at the upstream level via BoxBody
  pub async fn write_response_body(
    &mut self,
    data: Option<bytes::Bytes>,
    _end_of_stream: bool,
  ) -> Result<()> {
    if let Some(data) = data {
      self.response_body.push(data); // Used for local content generation (static files, errors)
    }
    Ok(())
  }

  pub fn set_keepalive(&mut self, ka: Option<u64>) {
    self._keepalive = ka;
  }

  pub async fn respond_error_with_body(
    &mut self,
    status: u16,
    body: bytes::Bytes,
  ) -> Result<()> {
    self.response_status = Some(
      StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
    );
    self.response_body = vec![body];
    Ok(())
  }

  /// Read next chunk of request body
  /// Returns Some(bytes) for each chunk, None when complete
  /// This matches Pingora's streaming interface
  pub async fn read_request_body(&mut self) -> Result<Option<bytes::Bytes>> {
    if self.is_websocket() {
      // WebSocket upgrades have no body
      return Ok(None);
    }

    loop {
      if let Some(body) = &mut self.request_body {
        match body.frame().await {
          Some(Ok(frame)) => {
            if let Ok(data) = frame.into_data() {
              return Ok(Some(data));
            }
            // Non-data frame (trailers, etc.), continue to next frame
          }
          Some(Err(_e)) => {
            // Error reading frame, mark body as consumed
            self.request_body = None;
            return Err(Box::new(Error::InternalError(
              "Error reading request body".to_string(),
            )));
          }
          None => {
            // End of stream
            self.request_body = None;
            return Ok(None);
          }
        }
      } else {
        // Body already consumed
        return Ok(None);
      }
    }
  }

  /// Build the final hyper response from accumulated state
  pub fn build_response(
    self,
  ) -> hyper::Response<http_body_util::Full<bytes::Bytes>> {
    use http_body_util::Full;

    let status = self.response_status.unwrap_or(StatusCode::OK);
    let body_bytes = if self.response_body.is_empty() {
      bytes::Bytes::new()
    } else {
      // Concatenate all body chunks
      let total_len: usize = self.response_body.iter().map(|b| b.len()).sum();
      let mut combined = bytes::BytesMut::with_capacity(total_len);
      for chunk in self.response_body {
        combined.extend_from_slice(&chunk);
      }
      combined.freeze()
    };

    let mut response = hyper::Response::builder().status(status);
    for (name, value) in self.response_headers.iter() {
      response = response.header(name.clone(), value.clone());
    }

    response.body(Full::new(body_bytes)).unwrap()
  }

  /// Build response with BoxBody for streaming compatibility
  pub fn build_response_boxed(
    self,
  ) -> hyper::Response<
    http_body_util::combinators::BoxBody<bytes::Bytes, hyper::Error>,
  > {
    use http_body_util::{BodyExt, Full};

    let status = self.response_status.unwrap_or(StatusCode::OK);
    let body_bytes = if self.response_body.is_empty() {
      bytes::Bytes::new()
    } else {
      // Concatenate all body chunks
      let total_len: usize = self.response_body.iter().map(|b| b.len()).sum();
      let mut combined = bytes::BytesMut::with_capacity(total_len);
      for chunk in self.response_body {
        combined.extend_from_slice(&chunk);
      }
      combined.freeze()
    };

    let mut response = hyper::Response::builder().status(status);
    for (name, value) in self.response_headers.iter() {
      response = response.header(name.clone(), value.clone());
    }

    response
      .body(
        Full::new(body_bytes)
          .map_err(|never| match never {})
          .boxed(),
      )
      .unwrap()
  }
}

#[async_trait]
pub trait ProxyHttp {
  type CTX: Send + Sync;

  fn new_ctx(&self) -> Self::CTX;

  async fn request_filter(
    &self,
    _session: &mut Session,
    _ctx: &mut Self::CTX,
  ) -> Result<bool>;

  async fn upstream_peer(
    &self,
    _session: &mut Session,
    _ctx: &mut Self::CTX,
  ) -> Result<Box<HttpPeer>>;

  async fn upstream_request_filter(
    &self,
    _session: &mut Session,
    _upstream_request: &mut RequestHeader,
    _ctx: &mut Self::CTX,
  ) -> Result<()> {
    Ok(())
  }

  /// Process request body chunks as they stream through
  /// Called for each chunk of the request body, matching Pingora's interface
  async fn request_body_filter(
    &self,
    _session: &mut Session,
    _body: &mut Option<bytes::Bytes>,
    _end_of_stream: bool,
    _ctx: &mut Self::CTX,
  ) -> Result<()> {
    // Default implementation does nothing
    Ok(())
  }

  async fn logging(
    &self,
    _session: &mut Session,
    _e: Option<&Error>,
    _ctx: &mut Self::CTX,
  ) {
    // Default implementation does nothing
  }
}
