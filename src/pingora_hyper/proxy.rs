use async_trait::async_trait;
use http::{HeaderMap, Method, StatusCode, Uri, Version};

use crate::pingora_hyper::error::{Error, Result};
use crate::pingora_hyper::upstreams::peer::HttpPeer;

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
pub struct Session {
  req_header: RequestHeader,
  _body: bytes::Bytes,
  response_status: Option<StatusCode>,
  response_headers: HeaderMap,
  response_body: Vec<bytes::Bytes>,
  _keepalive: Option<u64>,
}

impl Session {
  pub fn new(req_header: RequestHeader, body: bytes::Bytes) -> Self {
    Self {
      req_header,
      _body: body,
      response_status: None,
      response_headers: HeaderMap::new(),
      response_body: Vec::new(),
      _keepalive: None,
    }
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

  pub async fn write_response_body(
    &mut self,
    data: Option<bytes::Bytes>,
    _end_of_stream: bool,
  ) -> Result<()> {
    if let Some(data) = data {
      self.response_body.push(data);
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

  pub async fn read_request_body(&mut self) -> Result<Option<bytes::Bytes>> {
    // In hyper compatibility layer, the body is already read
    Ok(Some(self._body.clone()))
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

  async fn logging(
    &self,
    _session: &mut Session,
    _e: Option<&Error>,
    _ctx: &mut Self::CTX,
  ) {
    // Default implementation does nothing
  }
}
