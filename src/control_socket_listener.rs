use std::{
  net::SocketAddr,
  path::PathBuf,
  sync::{Arc, Mutex as StdMutex},
};

use bytes::{Buf as _, Bytes};
use futures::FutureExt as _;
use http_body_util::{combinators::BoxBody, BodyExt as _, Empty, Full};
use pingora::{server::ShutdownWatch, services::background::BackgroundService};
use tempfile::TempDir;
use tokio::net::UnixListener;
use tracing::{error, info, warn};

use crate::{
  system_cell::{SystemCell, SYSTEM_CELL_ID, SYSTEM_TENANT},
  NodeState,
};

pub struct ControlSocket {
  pub socket_path: PathBuf,
  _tempdir: TempDir,
}

impl ControlSocket {
  pub fn new() -> Self {
    let tempdir = tempfile::tempdir().unwrap();
    let socket_path = tempdir.path().join("control.sock");
    Self {
      socket_path,
      _tempdir: tempdir,
    }
  }
}

pub struct ControlSocketListener {
  pub node_state: Arc<NodeState>,
  pub system_cell_subscription:
    StdMutex<Option<tokio::sync::broadcast::Receiver<Arc<SystemCell>>>>,
}

#[async_trait::async_trait]
impl BackgroundService for ControlSocketListener {
  async fn start(&self, mut shutdown: ShutdownWatch) {
    info!("Starting control socket listener");

    let Some(mut system_cell_subscription) =
      self.system_cell_subscription.lock().unwrap().take()
    else {
      return;
    };

    let (system_cell_tx, system_cell_rx) = tokio::sync::oneshot::channel();
    let mut system_cell_tx = Some(system_cell_tx);

    let system_cell_slot = async move {
      let system_cell = system_cell_rx.await.unwrap();
      system_cell
    }
    .shared();

    let listener =
      UnixListener::bind(&self.node_state.control_socket.socket_path).unwrap();

    loop {
      tokio::select! {
          biased;

          _ = shutdown.changed() => {
              info!("Control socket listener shutting down");
              break;
          }

          Ok(system_cell) = system_cell_subscription.recv(), if system_cell_tx.is_some() => {
            let system_cell_tx = system_cell_tx.take().unwrap();
            let _ = system_cell_tx.send(system_cell);
          },

          stream = listener.accept() => {
              let stream = match stream {
                Ok((stream, _)) => stream,
                Err(e) => {
                    error!(error = ?e, "Control socket listener error");
                    continue;
                }
              };

              let io = hyper_util::rt::TokioIo::new(stream);
              let system_cell_slot = system_cell_slot.clone();
              let node_state = self.node_state.clone();

              let svc = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let system_cell_slot = system_cell_slot.clone();
                let node_state = node_state.clone();

                async move {
                  if req.uri().path() == "/_internal/alarms" {
                    tokio::select! {
                        biased;

                        system_cell = system_cell_slot => {
                          locally_handle_internal_alarms(req, system_cell).await
                        }

                        _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                          // Forward to the owner of the system cell
                          let system_cell_owner = node_state.peer_manager.get_owner_peer(SYSTEM_TENANT, SYSTEM_CELL_ID);
                          todo!("Forward req to the owner of the system cell")
                        }
                    }
                  } else {
                    // Paths other than /_internal/alarms are not supported now
                    hyper::Response::builder().status(404).body(empty())
                  }
                }
              });

              tokio::spawn(async move {
                  if let Err(err) = hyper::server::conn::http1::Builder::new().serve_connection(io, svc).await {
                    error!(error = ?err, "Control socket listener error");
                  }
              });
          }
      }
    }

    info!("Control socket listener stopped");
  }
}

async fn locally_handle_internal_alarms(
  req: hyper::Request<hyper::body::Incoming>,
  system_cell: Arc<SystemCell>,
) -> Result<hyper::Response<BoxBody<Bytes, hyper::Error>>, http::Error> {
  let (parts, body) = req.into_parts();
  let req_body = match body.collect().await {
    Ok(body) => body,
    Err(e) => {
      error!(error = ?e, "Failed to collect request body");
      return hyper::Response::builder().status(500).body(empty());
    }
  };

  #[derive(Debug, serde::Deserialize)]
  struct GetAlarmRequest {
    tenant: String,
    cell_id: String,
  }

  #[derive(Debug, serde::Serialize)]
  struct GetAlarmResponse {
    scheduled_time_unix_ms: u64,
  }

  #[derive(Debug, serde::Deserialize)]
  struct DeleteAlarmRequest {
    tenant: String,
    cell_id: String,
  }

  #[derive(Debug, serde::Deserialize)]
  struct SetAlarmRequest {
    tenant: String,
    cell_id: String,
    scheduled_time_unix_ms: u64,
  }

  match parts.method {
    hyper::Method::GET => {
      let Some(query) = parts.uri.query() else {
        return hyper::Response::builder()
          .status(400)
          .body(full("tenant and cell_id are required in the query params"));
      };

      let data: GetAlarmRequest = match serde_qs::from_str(query) {
        Ok(data) => data,
        Err(e) => {
          error!(%query, error = ?e, "Failed to parse query string");
          return hyper::Response::builder().status(400).body(empty());
        }
      };
      let alarm = match system_cell
        .alarm_processor()
        .get(data.tenant, data.cell_id)
        .await
      {
        Ok(alarm) => alarm,
        Err(e) => {
          error!(error = ?e, "Failed to get alarm");
          return hyper::Response::builder()
            .status(500)
            .body(full(e.to_string()));
        }
      };
      let response = GetAlarmResponse {
        scheduled_time_unix_ms: alarm.scheduled_time_unix_ms,
      };
      let body = serde_json::to_string(&response).unwrap();
      hyper::Response::builder().status(200).body(full(body))
    }
    hyper::Method::DELETE => {
      let data: DeleteAlarmRequest =
        match serde_json::from_reader(req_body.aggregate().reader()) {
          Ok(data) => data,
          Err(e) => {
            error!(error = ?e, "Failed to parse request body");
            return hyper::Response::builder().status(400).body(empty());
          }
        };
      if let Err(e) = system_cell
        .alarm_processor()
        .delete(data.tenant, data.cell_id)
        .await
      {
        error!(error = ?e, "Failed to delete alarm");
        return hyper::Response::builder().status(500).body(empty());
      }
      hyper::Response::builder().status(200).body(empty())
    }
    hyper::Method::POST => {
      let data: SetAlarmRequest =
        match serde_json::from_reader(req_body.aggregate().reader()) {
          Ok(data) => data,
          Err(e) => {
            error!(error = ?e, "Failed to parse request body");
            return hyper::Response::builder().status(400).body(empty());
          }
        };
      if let Err(e) = system_cell
        .alarm_processor()
        .set(data.tenant, data.cell_id, data.scheduled_time_unix_ms)
        .await
      {
        error!(error = ?e, "Failed to set alarm");
        return hyper::Response::builder().status(500).body(empty());
      }
      hyper::Response::builder().status(200).body(empty())
    }
    _ => hyper::Response::builder().status(405).body(empty()),
  }
}

fn empty() -> BoxBody<Bytes, hyper::Error> {
  Empty::<Bytes>::new()
    .map_err(|never| match never {})
    .boxed()
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
  Full::new(chunk.into())
    .map_err(|never| match never {})
    .boxed()
}
