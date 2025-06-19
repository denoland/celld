use async_trait::async_trait;
use http::{HeaderMap, Method, StatusCode, Uri, Version};

use crate::pingora_hyper::error::{Error, Result};

pub struct RequestHeader {
  pub method: Method,
  pub uri: Uri,
  pub version: Version,
  pub headers: HeaderMap,
}

pub struct ResponseHeader {
  pub status: StatusCode,
  pub headers: HeaderMap,
  pub version: Version,
}

pub struct HttpPeer {
  pub address: String,
  pub is_uds: bool,
}

impl HttpPeer {
  pub fn new(address: String, _tls: bool, _sni: String) -> Self {
    Self {
      address,
      is_uds: false,
    }
  }

  pub fn new_uds(path: String, host: String) -> Self {
    Self {
      address: format!("{}:{}", path, host),
      is_uds: true,
    }
  }
}

pub struct Session {
  req_header: RequestHeader,
  _keepalive: Option<u64>,
}

impl Session {
  pub fn req_header(&self) -> &RequestHeader {
    &self.req_header
  }

  pub fn req_header_mut(&mut self) -> &mut RequestHeader {
    &mut self.req_header
  }

  pub async fn write_response_header(
    &mut self,
    _resp: Box<ResponseHeader>,
    _end_of_stream: bool,
  ) -> Result<()> {
    todo!("implement write_response_header")
  }

  pub async fn write_response_body(
    &mut self,
    _data: Option<bytes::Bytes>,
    _end_of_stream: bool,
  ) -> Result<()> {
    todo!("implement write_response_body")
  }

  pub fn set_keepalive(&mut self, ka: Option<u64>) {
    self._keepalive = ka;
  }

  pub async fn respond_error_with_body(
    &mut self,
    _status: u16,
    _headers: Option<HeaderMap>,
    _body: Option<bytes::Bytes>,
  ) -> Result<()> {
    todo!("implement respond_error_with_body")
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
  ) -> Result<()>;

  async fn logging(
    &self,
    _session: &mut Session,
    _e: Option<&Error>,
    _ctx: &mut Self::CTX,
  );
}
