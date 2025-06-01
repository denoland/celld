use http_body_util::BodyExt;
use pingora::http::StatusCode;
use pingora::prelude::*;
use pingora::upstreams::peer::HttpPeer;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::alarm_processor::{dispatch_alarm_locally, Alarm};
use crate::cell_manager::{CellKey, CellManagerError};
use crate::control_socket_listener::locally_handle_internal_alarms;
use crate::node_state::NodeState;

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
  #[allow(dead_code)]
  #[error("Invalid hostname format")]
  InvalidHost,
  #[error("Application not found for host: {0}")]
  AppNotFound(String),
  #[error("Internal Server Error: {0}")]
  InternalError(#[from] anyhow::Error),
}

pub struct Proxy {
  pub node_state: Arc<NodeState>,
}

pub struct InternalAPI {
  pub node_state: Arc<NodeState>,
}

#[derive(Debug, Default)]
pub struct Ctx {
  pub tenant: String,
  pub cell_id: Option<String>,
  pub cell_key: Option<CellKey>,
}

#[derive(Debug)]
pub struct InternalCtx {}

#[async_trait::async_trait]
impl ProxyHttp for InternalAPI {
  type CTX = InternalCtx;

  fn new_ctx(&self) -> Self::CTX {
    InternalCtx {}
  }

  async fn request_filter(
    &self,
    session: &mut Session,
    _ctx: &mut Self::CTX,
  ) -> Result<bool> {
    let req_header = session.req_header();

    // Get the path
    let path = req_header.uri.path();

    info!(path, method = %req_header.method, "Internal API request received");

    // Handle internal endpoints
    if path == "/_internal/mesh/peers" {
      let local_peer = self.node_state.peer_manager.get_local_peer();

      // Get peer info with full details if it's available through the cluster membership
      let peer_infos = self.node_state.peer_manager.get_all_peer_info();

      // Build a JSON array of peers
      let mut peer_json = String::from("[");
      for (i, info) in peer_infos.iter().enumerate() {
        if i > 0 {
          peer_json.push(',');
        }
        let is_local =
          info.node_id == *self.node_state.peer_manager.get_local_node_id();
        peer_json.push_str(&format!(
            "{{\"node_id\":\"{}\",\"address\":\"{}\",\"is_local\":{},\"last_heartbeat\":\"{}\"}}",
            info.node_id.as_str(),
            info.advertise_addr,
            is_local,
            info.heartbeat_timestamp
          ));
      }
      peer_json.push(']');

      // Return a JSON response with all peers
      let response = format!(
        "{{\"peers\":{},\"count\":{},\"local\":\"{}\"}}",
        peer_json,
        peer_infos.len(),
        local_peer
      );

      let content_length = response.len();
      let mut resp =
        pingora::http::ResponseHeader::build(StatusCode::OK, Some(2)).unwrap();
      resp
        .insert_header(http::header::CONTENT_LENGTH, content_length.to_string())
        .unwrap();
      resp
        .insert_header(http::header::CONTENT_TYPE, "application/json")
        .unwrap();

      write_response_close_conn(session, resp, response.into()).await?;
      return Ok(true);
    }

    // Handle the owner endpoint
    if let Some(path_part) = path.strip_prefix("/_internal/mesh/owner/") {
      if path_part.is_empty() {
        error!(
          path,
          "/_internal/mesh/owner should be followed by `{{tenant}}/{{cell_id}}`"
        );
        let resp =
          pingora::http::ResponseHeader::build(StatusCode::BAD_REQUEST, None)
            .unwrap();

        write_response_close_conn(session, resp, "Bad Request".into()).await?;
        return Ok(true);
      }

      // Extract tenant and cell_id from the path
      // Expected format: /_internal/mesh/owner/{tenant}/{cell_id}
      let parts: Vec<&str> = path_part.split('/').collect();

      if parts.len() != 2 {
        error!(
          path,
          "/_internal/mesh/owner should be followed by `{{tenant}}/{{cell_id}}`"
        );
        let resp =
          pingora::http::ResponseHeader::build(StatusCode::BAD_REQUEST, None)
            .unwrap();

        write_response_close_conn(session, resp, "Bad Request".into()).await?;
        return Ok(true);
      }

      let tenant = parts[0];
      let cell_id = parts[1];

      let owner = self.node_state.peer_manager.get_owner_peer(tenant, cell_id);
      let is_local =
        self.node_state.peer_manager.is_local_owner(tenant, cell_id);

      // Return a simple JSON response with owner information
      let response = serde_json::to_string(&serde_json::json!({
        "tenant": tenant,
        "cell_id": cell_id,
        "owner": owner,
        "is_local": is_local
      }))
      .unwrap();

      let content_length = response.len();
      let mut resp =
        pingora::http::ResponseHeader::build(StatusCode::OK, Some(2)).unwrap();
      resp
        .insert_header(http::header::CONTENT_LENGTH, content_length.to_string())
        .unwrap();
      resp
        .insert_header(http::header::CONTENT_TYPE, "application/json")
        .unwrap();
      write_response_close_conn(session, resp, response.into()).await?;
      return Ok(true);
    }

    // Handle the alarms endpoint
    if path.starts_with("/_internal/alarms") {
      let Some(system_main_cell_handle) =
        self.node_state.cell_manager.get_system_main_cell().await
      else {
        error!("System main cell not found; most likely the system main cell is running on another node");
        let resp = pingora::http::ResponseHeader::build(
          StatusCode::INTERNAL_SERVER_ERROR,
          Some(0),
        )
        .unwrap();
        session.set_keepalive(None);
        session.write_response_header(Box::new(resp), true).await?;
        return Ok(true);
      };

      let parts = session.req_header().as_ref().clone();
      let req_body = session
        .read_request_body()
        .await?
        .map(|bytes| http_body_util::Full::new(bytes).boxed())
        .unwrap_or_else(|| http_body_util::Empty::new().boxed());
      let req = hyper::Request::from_parts(parts, req_body);

      match locally_handle_internal_alarms(req, system_main_cell_handle).await {
        Ok(res) => {
          let (parts, body) = res.into_parts();
          session
            .write_response_header(Box::new(parts.into()), false)
            .await?;
          session.set_keepalive(None);
          let body = body.collect().await.unwrap().to_bytes();
          session.write_response_body(Some(body), true).await?;
          return Ok(true);
        }
        Err(e) => {
          error!(error = ?e, "Error handling alarms");
          let resp = pingora::http::ResponseHeader::build(
            StatusCode::INTERNAL_SERVER_ERROR,
            Some(0),
          )
          .unwrap();
          session.set_keepalive(None);
          session.write_response_header(Box::new(resp), true).await?;
          return Ok(true);
        }
      }
    }

    // Handle the dispatch alarm endpoint
    if path.starts_with("/_internal/dispatch_alarm")
      && req_header.method == http::Method::POST
    {
      let Some(req_body) = session.read_request_body().await? else {
        error!("Error reading request body of dispatch_alarm endpoint");
        let resp = pingora::http::ResponseHeader::build(
          StatusCode::INTERNAL_SERVER_ERROR,
          Some(0),
        )
        .unwrap();
        session.set_keepalive(None);
        session.write_response_header(Box::new(resp), true).await?;
        return Ok(true);
      };
      let dispatched_alarm: Alarm = match serde_json::from_slice(&req_body) {
        Ok(dispatch_alarm) => dispatch_alarm,
        Err(e) => {
          error!(error = ?e, "Error deserializing dispatch alarm");
          let resp = pingora::http::ResponseHeader::build(
            StatusCode::INTERNAL_SERVER_ERROR,
            Some(0),
          )
          .unwrap();
          session.set_keepalive(None);
          session.write_response_header(Box::new(resp), true).await?;
          return Ok(true);
        }
      };
      let status =
        match dispatch_alarm_locally(dispatched_alarm, self.node_state.clone())
          .await
        {
          Ok(_) => StatusCode::OK,
          Err(e) => {
            error!(error = ?e, "Error dispatching alarm");
            StatusCode::INTERNAL_SERVER_ERROR
          }
        };
      let resp = pingora::http::ResponseHeader::build(status, Some(0)).unwrap();
      session.set_keepalive(None);
      session.write_response_header(Box::new(resp), true).await?;
      return Ok(true);
    }

    // If we didn't match any known internal endpoint, return a 404
    let response = "Not Found";
    let content_length = response.len();
    let mut resp =
      pingora::http::ResponseHeader::build(StatusCode::NOT_FOUND, Some(2))
        .unwrap();
    resp
      .insert_header(http::header::CONTENT_LENGTH, content_length.to_string())
      .unwrap();
    resp
      .insert_header(http::header::CONTENT_TYPE, "text/plain")
      .unwrap();

    write_response_close_conn(session, resp, response.into()).await?;

    Ok(true)
  }

  // This is a simple endpoint handler that doesn't need to proxy to an upstream
  async fn upstream_peer(
    &self,
    _session: &mut Session,
    _ctx: &mut Self::CTX,
  ) -> pingora::Result<Box<HttpPeer>> {
    // This should not be called because request_filter always returns true
    Err(pingora::Error::explain(
      ErrorType::HTTPStatus(StatusCode::INTERNAL_SERVER_ERROR.into()),
      "Internal control plane does not support proxying",
    ))
  }
}

/// Helper function to write a response and close the connection.
/// Note: we believe here and elsewhere that set_keepalive(None) should come before
/// write_response_header.
async fn write_response_close_conn(
  session: &mut Session,
  header: pingora::http::ResponseHeader,
  body: bytes::Bytes,
) -> pingora::Result<()> {
  session.set_keepalive(None);
  session
    .write_response_header(Box::new(header), false)
    .await?;
  session.write_response_body(Some(body), true).await?;
  Ok(())
}

#[async_trait::async_trait]
impl ProxyHttp for Proxy {
  type CTX = Ctx;

  // Required implementation of new_ctx
  fn new_ctx(&self) -> Self::CTX {
    Ctx::default()
  }

  // Called when the entire response is sent to the downstream, or when there is a fatal error
  async fn logging(
    &self,
    _session: &mut Session,
    _e: Option<&pingora::Error>,
    ctx: &mut Self::CTX,
  ) {
    if let Some(process_key) = &ctx.cell_key {
      let _ = self
        .node_state
        .cell_manager
        .decrement_connection_count(process_key)
        .await;
    }
  }

  async fn request_filter(
    &self,
    session: &mut Session,
    ctx: &mut Self::CTX,
  ) -> Result<bool> {
    let req_header = session.req_header();

    // Extract host header, fall back to "default" if missing
    let host =
      if let Some(header_value) = req_header.headers.get(http::header::HOST) {
        header_value.to_str().map_err(|_| {
          error!("Host header contains invalid characters");
          pingora::Error::explain(
            ErrorType::HTTPStatus(StatusCode::BAD_REQUEST.into()),
            "Invalid Host header encoding",
          )
        })?
      } else {
        "default"
      };

    // In single-tenant mode, always use "default" tenant
    let tenant = if self.node_state.config.single_tenant.is_some() {
      "default".to_string()
    } else {
      // Extract hostname without port
      let hostname = host.split(':').next().unwrap_or(host);
      let mut tenant = hostname.to_string();

      // Validate host format briefly (prevent directory traversal)
      if tenant.contains('/') || tenant.contains("..") {
        return Err(pingora::Error::explain(
          ErrorType::HTTPStatus(StatusCode::BAD_REQUEST.into()),
          "Invalid Host header",
        ));
      }

      // Check if tenant directory exists, fall back to "default" if not
      let tenant_dir = self.node_state.cell_manager.data_dir.join(&tenant);
      if !tenant_dir.exists() {
        tenant = "default".to_string();
      }

      tenant
    };

    ctx.tenant = tenant;

    // Get the path
    let path = req_header.uri.path();

    // Handle health check endpoint
    if path == "/_health" {
      let response = "OK\n";
      let content_length = response.len();
      let mut resp =
        pingora::http::ResponseHeader::build(StatusCode::OK, Some(2)).unwrap();
      resp
        .insert_header(http::header::CONTENT_LENGTH, content_length.to_string())
        .unwrap();
      resp
        .insert_header(http::header::CONTENT_TYPE, "text/plain")
        .unwrap();

      write_response_close_conn(session, resp, response.into()).await?;
      return Ok(true);
    }

    // Handle requests to the old mesh endpoints - just return 404 to indicate they've moved
    if path.starts_with("/_mesh/") {
      let response = "Mesh endpoints have moved to the internal API";
      let content_length = response.len();
      let mut resp =
        pingora::http::ResponseHeader::build(StatusCode::NOT_FOUND, Some(2))
          .unwrap();
      resp
        .insert_header(http::header::CONTENT_LENGTH, content_length.to_string())
        .unwrap();
      resp
        .insert_header(http::header::CONTENT_TYPE, "text/plain")
        .unwrap();

      write_response_close_conn(session, resp, response.into()).await?;
      return Ok(true);
    }

    // Check if this is a /cell/* path - if so, let the default proxy path handle it
    if let Some(cell_path) = path.strip_prefix("/cell/") {
      if !cell_path.is_empty() {
        // Store the cell ID as the first path segment
        let cell_id = cell_path.split('/').next().unwrap_or(cell_path);
        // Store the cell ID in the context for later use
        ctx.cell_id = Some(cell_id.to_string());

        return Ok(false); // Let it be handled by the upstream_peer method
      }
    }

    // Only handle GET and HEAD requests
    if req_header.method != http::Method::GET
      && req_header.method != http::Method::HEAD
    {
      return Ok(false);
    }

    // Process the path and handle static files for non-cell paths
    let rel_path = path.trim_start_matches('/');

    // Create a String to store our modified path
    let rel_path_ = if rel_path.is_empty() || rel_path.ends_with('/') {
      format!("{}index.html", rel_path)
    } else {
      rel_path.to_string()
    };

    // Construct the file path
    let static_dir = if let Some(ref single_tenant) =
      self.node_state.config.single_tenant
    {
      // In single-tenant mode, use the specified static directory or fall back to current dir
      single_tenant.static_dir.clone().unwrap_or_else(|| {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
      })
    } else {
      // In multi-tenant mode, use the standard path structure
      let tenant_dir = self.node_state.cell_manager.data_dir.join(&ctx.tenant);
      tenant_dir.join("static")
    };
    let file_path = static_dir.join(&rel_path_);

    // Try to read the file
    let file = match std::fs::read(&file_path) {
      Ok(file) => file,
      Err(_) => {
        debug!("File not found: {}", file_path.display());
        return Err(pingora::Error::explain(
          ErrorType::HTTPStatus(StatusCode::NOT_FOUND.into()),
          "Not found",
        ));
      }
    };

    // Determine content type based on file extension
    let content_type = match rel_path_.rsplit('.').next() {
      Some("html") | Some("htm") => "text/html",
      Some("css") => "text/css",
      Some("js") => "application/javascript",
      Some("json") => "application/json",
      Some("png") => "image/png",
      Some("jpg") | Some("jpeg") => "image/jpeg",
      Some("gif") => "image/gif",
      Some("svg") => "image/svg+xml",
      Some("webp") => "image/webp",
      Some("ico") => "image/x-icon",
      Some("woff") => "font/woff",
      Some("woff2") => "font/woff2",
      Some("ttf") => "font/ttf",
      Some("txt") => "text/plain",
      Some("pdf") => "application/pdf",
      Some("xml") => "application/xml",
      _ => "application/octet-stream",
    };

    let content_length = file.len();

    // Build and send response
    let mut resp =
      pingora::http::ResponseHeader::build(StatusCode::OK, Some(2)).unwrap();
    resp
      .insert_header(http::header::CONTENT_LENGTH, content_length.to_string())
      .unwrap();
    resp
      .insert_header(http::header::CONTENT_TYPE, content_type)
      .unwrap();

    let end_of_stream = req_header.method == http::Method::HEAD;
    session.set_keepalive(None);
    session
      .write_response_header(Box::new(resp), end_of_stream)
      .await?;

    if !end_of_stream {
      session.write_response_body(Some(file.into()), true).await?;
    }

    Ok(true)
  }

  // This method is called for each HTTP request to determine the upstream server
  async fn upstream_peer(
    &self,
    session: &mut Session,
    ctx: &mut Self::CTX,
  ) -> pingora::Result<Box<HttpPeer>> {
    // Start timing the request path
    let request_start = std::time::Instant::now();

    // Get the cell_id from the context, or use a default value
    let cell_id = match &ctx.cell_id {
      Some(id) => id.as_str(),
      None => "default-cell", // Default cell ID if none specified
    };

    debug!(
      host = %ctx.tenant,
      cell_id = %cell_id,
      request_init_time = ?request_start.elapsed(),
      "Processing request"
    );

    // Check if this instance is responsible for this cell
    if !self
      .node_state
      .peer_manager
      .is_local_owner(&ctx.tenant, cell_id)
    {
      let owners = self
        .node_state
        .peer_manager
        .get_cell_owners(&ctx.tenant, cell_id);
      if let Some(primary_owner_addr) = owners.first() {
        info!(
            my_node_id = ?self.node_state.node_id,
            host = %ctx.tenant,
            cell_id = %cell_id,
            responsible_peer = %primary_owner_addr,
            "Forwarding request to primary active owner"
        );
        let sni = ctx.tenant.clone();
        let peer = HttpPeer::new(primary_owner_addr, false, sni);
        return Ok(Box::new(peer));
      } else {
        // This case means no active owners were found according to PeerManager,
        // which might indicate cluster inconsistency or that the local node
        // should have been the owner but wasn't identified as such.
        error!(
            host = %ctx.tenant,
            cell_id = %cell_id,
            "No active owner found for cell, cannot forward request."
        );
        return Err(pingora::Error::explain(
          ErrorType::HTTPStatus(StatusCode::SERVICE_UNAVAILABLE.into()),
          "No available upstream node for the requested cell",
        ));
      }
    }

    // We are the responsible peer, so handle the request locally
    debug!(
      my_node_id = ?self.node_state.node_id,
      host = %ctx.tenant,
      cell_id = %cell_id,
      "This instance is responsible for handling the request"
    );

    const RETRY_COUNT: u32 = 10;
    const RETRY_INTERVAL: std::time::Duration =
      std::time::Duration::from_millis(500);

    let mut socket_path = None;

    for _ in 0..RETRY_COUNT {
      match self
        .node_state
        .cell_manager
        .get_or_spawn_normal_cell(&ctx.tenant, cell_id, self.node_state.clone())
        .await
      {
        Ok((path, _stream, process_key)) => {
          // We only need the path, Pingora will handle the connection
          // Increment active connection count
          self
            .node_state
            .cell_manager
            .increment_connection_count(&process_key)
            .await;
          ctx.cell_key = Some(process_key);
          socket_path = Some(path);
          break;
        }
        Err(CellManagerError::CellCreationInProgress) => {
          info!(
            node_id = ?self.node_state.node_id,
            "Lock is held by this node, meaning a cell creation is already in progress. Retry in {:?}",
            RETRY_INTERVAL
          );
          tokio::time::sleep(RETRY_INTERVAL).await;
          continue;
        }
        Err(error @ CellManagerError::LockContention(_)) => {
          debug!(
            ?error,
            "Lock is held by another node that is responsible for this cell"
          );
          // TODO: forward the request to the lock holder?
          return Err(error.into());
        }
        Err(error @ CellManagerError::S3(_)) => {
          debug!(?error, "S3 operation failed");
          return Err(error.into());
        }
        Err(error @ CellManagerError::Serde(_)) => {
          debug!(?error, "Failed to serialize or deserialize lock data");
          return Err(error.into());
        }
        Err(error @ CellManagerError::Internal(_)) => {
          error!(?error, "Error getting or spawning process");
          return Err(error.into());
        }
      }
    }

    let Some(socket_path) = socket_path else {
      return Err(pingora::Error::explain(
        ErrorType::HTTPStatus(StatusCode::INTERNAL_SERVER_ERROR.into()),
        "Failed to get or spawn process",
      ));
    };

    // Configure backend using the Unix Domain Socket
    debug!(
      process_manager_time = ?request_start.elapsed(),
      "Process manager get_or_spawn_process completed"
    );

    let socket_path_str = match socket_path.to_str() {
      Some(s) => s.to_string(),
      None => {
        error!("Invalid UTF-8 in socket path: {:?}", socket_path);
        return Err(pingora::Error::explain(
          ErrorType::HTTPStatus(StatusCode::INTERNAL_SERVER_ERROR.into()),
          "Invalid backend path encoding",
        ));
      }
    };

    // Create a Backend using the Unix Domain Socket address
    let peer_start = std::time::Instant::now();
    let sni = ctx.tenant.clone();
    match HttpPeer::new_uds(&socket_path_str, false, sni) {
      Ok(peer) => {
        debug!(
          host = %ctx.tenant,
          socket = %socket_path.display(),
          uds_peer_creation_time = ?peer_start.elapsed(),
          total_time_so_far = ?request_start.elapsed(),
          "Selected upstream UDS peer"
        );

        // Remove `/cell/{cell_id}` from the URI so that the user program won't
        // see this part
        let normalized_uri =
          remove_cell_id_from_uri(session.req_header().uri.clone(), cell_id);
        session.req_header_mut().set_uri(normalized_uri);

        // Assume anything after this point is handled by Pingora proxy machinery
        Ok(Box::new(peer))
      }
      Err(e) => {
        error!("Failed to create HTTP peer: {:?}", e);
        Err(pingora::Error::because(
          ErrorType::HTTPStatus(StatusCode::SERVICE_UNAVAILABLE.into()),
          "Failed to connect to upstream application",
          e,
        ))
      }
    }
  }
}

/// Remove the `/cell/{cell_id}` prefix from the URI
fn remove_cell_id_from_uri(uri: http::Uri, cell_id: &str) -> http::Uri {
  let mut parts = uri.into_parts();

  if let Some(path_and_query) = &mut parts.path_and_query {
    let path = path_and_query.path();
    let maybe_query = path_and_query.query();

    let modified_path = path
      .strip_prefix(&format!("/cell/{cell_id}"))
      .unwrap_or(path);

    let new_path_and_query = match maybe_query {
      Some(query) => format!("{modified_path}?{query}"),
      None => modified_path.to_string(),
    };

    *path_and_query = new_path_and_query.parse().unwrap();
  }

  http::Uri::from_parts(parts).unwrap()
}

#[cfg(test)]
mod tests {
  use super::*;
  use proptest::prelude::*;

  #[test]
  fn test_remove_cell_id_from_uri() {
    let uri = http::Uri::from_static("http://example.com");
    let new_uri = remove_cell_id_from_uri(uri, "123");
    assert_eq!(new_uri.to_string(), "http://example.com/");

    let uri = http::Uri::from_static("http://example.com/cell/123");
    let new_uri = remove_cell_id_from_uri(uri, "123");
    assert_eq!(new_uri.to_string(), "http://example.com/");

    let uri =
      http::Uri::from_static("https://example.com/cell/deadbeef1234/foo");
    let new_uri = remove_cell_id_from_uri(uri, "deadbeef1234");
    assert_eq!(new_uri.to_string(), "https://example.com/foo");

    let uri =
      http::Uri::from_static("https://example.com/cell/deadbeef1234/foo/bar");
    let new_uri = remove_cell_id_from_uri(uri, "deadbeef1234");
    assert_eq!(new_uri.to_string(), "https://example.com/foo/bar");

    let uri = http::Uri::from_static(
      "https://example.com/cell/deadbeef1234/foo/bar?hello=world",
    );
    let new_uri = remove_cell_id_from_uri(uri, "deadbeef1234");
    assert_eq!(
      new_uri.to_string(),
      "https://example.com/foo/bar?hello=world"
    );

    let uri = http::Uri::from_static(
      "https://example.com/cell/deadbeef1234/foo/bar?hello=world&food=sushi",
    );
    let new_uri = remove_cell_id_from_uri(uri, "deadbeef1234");
    assert_eq!(
      new_uri.to_string(),
      "https://example.com/foo/bar?hello=world&food=sushi"
    );
  }

  fn uri_strategy() -> impl Strategy<Value = http::Uri> {
    any::<(bool, String, String)>().prop_filter_map(
      "uri must be valid",
      |(https, path, query)| {
        let scheme = if https { "https" } else { "http" };
        use fake::Fake as _;
        let domain: String = fake::faker::internet::en::DomainSuffix().fake();
        let path: String = if path.is_empty() {
          String::new()
        } else {
          format!("/{}", path.replace("/", "").replace("?", ""))
        };
        let query_part = if query.is_empty() {
          String::new()
        } else {
          format!("?{}", query.replace("&", "").replace("=", "eq"))
        };

        format!("{}://example.{}{}{}", scheme, domain, path, query_part)
          .parse::<http::Uri>()
          .ok()
      },
    )
  }

  proptest! {
    #[test]
    fn prop_test_remove_cell_id_from_uri(uri in uri_strategy()) {
      let normalized_uri = remove_cell_id_from_uri(uri, "deadbeef1234");
      if let Some(path_and_query) = normalized_uri.path_and_query() {
        let path = path_and_query.path();
        prop_assert!(!path.starts_with("/cell/"));
      }
    }
  }
}
