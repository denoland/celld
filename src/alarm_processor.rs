use std::{path::Path, sync::Arc};

use chrono::{DateTime, Utc};
use tokio::net::TcpStream;
use tracing::{error, info};

use crate::node_state::NodeState;

#[derive(Debug, thiserror::Error)]
pub enum AlarmError {
  #[error("SQLite error: {0}")]
  SQLiteError(#[from] rusqlite::Error),
  #[error("alarm not found")]
  AlarmNotFound,
  #[error("failed to send request to alarm processor")]
  SendRequestError(
    #[from] tokio::sync::mpsc::error::SendError<AlarmProcessRequest>,
  ),
  #[error("failed to receive response from alarm processor")]
  RecvResponseError(#[from] tokio::sync::oneshot::error::RecvError),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Alarm {
  pub tenant: String,
  pub cell_id: String,
  pub scheduled_time_unix_ms: u64,
}

pub enum AlarmProcessRequest {
  Get {
    tenant: String,
    cell_id: String,
    response: tokio::sync::oneshot::Sender<Result<Alarm, AlarmError>>,
  },
  Delete {
    tenant: String,
    cell_id: String,
    response: tokio::sync::oneshot::Sender<Result<(), AlarmError>>,
  },
  Set {
    tenant: String,
    cell_id: String,
    scheduled_time_unix_ms: u64,
    response: tokio::sync::oneshot::Sender<Result<(), AlarmError>>,
  },
  Dispatch {
    node_state: Arc<NodeState>,
    current_timestamp: DateTime<Utc>,
    limit: u32,
    response: tokio::sync::oneshot::Sender<Result<(), AlarmError>>,
  },
  DispatchCompleted {
    response: tokio::sync::oneshot::Sender<Result<(), AlarmError>>,
    dispatched_alarms: Vec<Alarm>,
  },
  Shutdown {
    response: tokio::sync::oneshot::Sender<()>,
  },
}

impl AlarmProcessRequest {
  fn kind(&self) -> &'static str {
    match self {
      AlarmProcessRequest::Get { .. } => "Get",
      AlarmProcessRequest::Delete { .. } => "Delete",
      AlarmProcessRequest::Set { .. } => "Set",
      AlarmProcessRequest::Dispatch { .. } => "Dispatch",
      AlarmProcessRequest::DispatchCompleted { .. } => "DispatchCompleted",
      AlarmProcessRequest::Shutdown { .. } => "Shutdown",
    }
  }
}

#[derive(Debug)]
pub struct AlarmProcessor {
  _handle: std::thread::JoinHandle<()>,
  abort_handle: tokio::task::AbortHandle,
  request_tx: tokio::sync::mpsc::Sender<AlarmProcessRequest>,
}

impl AlarmProcessor {
  pub fn new(db_path: &Path) -> Result<Self, anyhow::Error> {
    // Ensure the directory exists
    let sqlite_dir = db_path.parent().unwrap();
    std::fs::create_dir_all(sqlite_dir)?;

    let mut conn = rusqlite::Connection::open(db_path)?;

    conn.execute(
      "CREATE TABLE IF NOT EXISTS global_alarms (
        tenant TEXT NOT NULL,
        cell_id TEXT NOT NULL,
        scheduled_time_unix_ms INTEGER NOT NULL,
        PRIMARY KEY (tenant, cell_id)
      )",
      (),
    )?;

    conn.execute(
      "CREATE INDEX IF NOT EXISTS idx_global_alarms_schedule_time ON global_alarms (scheduled_time_unix_ms)",
      (),
    )?;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<AlarmProcessRequest>(100);
    let self_message_tx = tx.clone();
    let (abort_handle_tx, abort_handle_rx) = std::sync::mpsc::channel();

    // Spawn a thread dedicated to process alarm requests.
    // This is necessary because SQLite client is not Send.
    let handle = std::thread::spawn(move || {
      let rt = tokio::runtime::Builder::new_current_thread()
        // Send the alarm to the remote node.
        .enable_all()
        .build()
        .unwrap();
      let local = tokio::task::LocalSet::new();

      let handle = local.spawn_local(async move {
        while let Some(request) = rx.recv().await {
          let kind = request.kind();
          tracing::debug!(?kind, "AlarmProcessor received request");

          // NOTE: it is important to keep message handlers non-async.
          //
          // If one is async, that may block the alarm processor loop from
          // processing other queued messages. And what's worse, it may cause
          // deadlock if the handler sends a message back to the channel and
          // awaits it - the handler waits for the response, but in order for
          // the message to be processed, the handler needs to finish and the
          // loop needs to continue to process the next message.
          match request {
            AlarmProcessRequest::Get {
              tenant,
              cell_id,
              response,
            } => {
              let res = get_alarm_handler(&conn, &tenant, &cell_id);
              let _ = response.send(res);
            }
            AlarmProcessRequest::Delete {
              tenant,
              cell_id,
              response,
            } => {
              let res = delete_alarm_handler(&conn, &tenant, &cell_id);
              let _ = response.send(res);
            }
            AlarmProcessRequest::Set {
              tenant,
              cell_id,
              scheduled_time_unix_ms,
              response,
            } => {
              let res = set_alarm_handler(
                &conn,
                &tenant,
                &cell_id,
                scheduled_time_unix_ms,
              );
              let _ = response.send(res);
            }
            AlarmProcessRequest::Dispatch {
              node_state,
              current_timestamp,
              limit,
              response,
            } => {
              dispatch_alarms_handler(
                &mut conn,
                node_state,
                current_timestamp,
                limit,
                self_message_tx.clone(),
                response,
              );
            }
            AlarmProcessRequest::DispatchCompleted {
              response,
              dispatched_alarms,
            } => {
              dispatch_completed_handler(&conn, dispatched_alarms, response);
            }
            AlarmProcessRequest::Shutdown { response } => {
              let _ = response.send(());
              break;
            }
          }

          tracing::debug!(?kind, "AlarmProcessor processed request");
        }
      });

      abort_handle_tx
        .send(handle.abort_handle())
        .expect("abort_handle_rx should be alive");

      rt.block_on(local);
    });

    let abort_handle = abort_handle_rx
      .recv()
      .expect("a tokio task that processes alarm requests should be spawned");

    Ok(Self {
      _handle: handle,
      abort_handle,
      request_tx: tx,
    })
  }

  pub fn handle(&self) -> AlarmProcessorHandle {
    AlarmProcessorHandle {
      request_tx: self.request_tx.clone(),
    }
  }

  /// Kill the alarm processor forcibly, without waiting for pending requests
  /// to finish.
  pub fn kill(&self) {
    if self.abort_handle.is_finished() {
      return;
    }

    self.abort_handle.abort();
  }
}

#[derive(Debug, Clone)]
pub struct AlarmProcessorHandle {
  request_tx: tokio::sync::mpsc::Sender<AlarmProcessRequest>,
}

impl AlarmProcessorHandle {
  /// Get an alarm set for the given tenant and cell id.
  pub async fn get(
    &self,
    tenant: String,
    cell_id: String,
  ) -> Result<Alarm, AlarmError> {
    let (res_tx, res_rx) = tokio::sync::oneshot::channel();
    self
      .request_tx
      .send(AlarmProcessRequest::Get {
        tenant,
        cell_id,
        response: res_tx,
      })
      .await?;
    res_rx.await?
  }

  /// Delete an alarm set for the given tenant and cell id.
  pub async fn delete(
    &self,
    tenant: String,
    cell_id: String,
  ) -> Result<(), AlarmError> {
    let (res_tx, res_rx) = tokio::sync::oneshot::channel();
    self
      .request_tx
      .send(AlarmProcessRequest::Delete {
        tenant,
        cell_id,
        response: res_tx,
      })
      .await?;
    res_rx.await?
  }

  /// Set an alarm for the given tenant and cell id.
  pub async fn set(
    &self,
    tenant: String,
    cell_id: String,
    scheduled_time_unix_ms: u64,
  ) -> Result<(), AlarmError> {
    let (res_tx, res_rx) = tokio::sync::oneshot::channel();
    self
      .request_tx
      .send(AlarmProcessRequest::Set {
        tenant: tenant.clone(),
        cell_id: cell_id.clone(),
        scheduled_time_unix_ms,
        response: res_tx,
      })
      .await?;
    res_rx.await?
  }

  /// Dispatch all alarms that are due at the given timestamp. Dispatched alarms
  /// are deleted from the internal datastore.
  pub async fn dispatch(
    &self,
    node_state: Arc<NodeState>,
    current_timestamp: DateTime<Utc>,
    limit: u32,
  ) -> Result<(), AlarmError> {
    let (res_tx, res_rx) = tokio::sync::oneshot::channel();
    self
      .request_tx
      .send(AlarmProcessRequest::Dispatch {
        node_state,
        current_timestamp,
        limit,
        response: res_tx,
      })
      .await?;
    res_rx.await?
  }

  /// Shutdown the alarm processor gracefully.
  /// This will resolve when the alarm processor has finished processing all
  /// requests.
  pub async fn shutdown(&mut self) -> Result<(), AlarmError> {
    let (res_tx, res_rx) = tokio::sync::oneshot::channel();
    self
      .request_tx
      .send(AlarmProcessRequest::Shutdown { response: res_tx })
      .await?;
    res_rx.await?;
    Ok(())
  }
}

fn get_alarm_handler(
  conn: &rusqlite::Connection,
  tenant: &str,
  cell_id: &str,
) -> Result<Alarm, AlarmError> {
  let mut stmt = conn.prepare(
    "SELECT tenant, cell_id, scheduled_time_unix_ms FROM global_alarms WHERE tenant = ? AND cell_id = ?",
  )?;
  let alarm = stmt
    .query_row((tenant, cell_id), |row| {
      Ok(Alarm {
        tenant: row.get("tenant")?,
        cell_id: row.get("cell_id")?,
        scheduled_time_unix_ms: row.get("scheduled_time_unix_ms")?,
      })
    })
    .map_err(|e| match e {
      rusqlite::Error::QueryReturnedNoRows => AlarmError::AlarmNotFound,
      _ => AlarmError::SQLiteError(e),
    })?;

  Ok(alarm)
}

fn delete_alarm_handler(
  conn: &rusqlite::Connection,
  tenant: &str,
  cell_id: &str,
) -> Result<(), AlarmError> {
  let mut stmt = conn
    .prepare("DELETE FROM global_alarms WHERE tenant = ? AND cell_id = ?")?;
  let affected_rows = stmt.execute((tenant, cell_id))?;

  if affected_rows == 0 {
    Err(AlarmError::AlarmNotFound)
  } else {
    Ok(())
  }
}

fn set_alarm_handler(
  conn: &rusqlite::Connection,
  tenant: &str,
  cell_id: &str,
  scheduled_time_unix_ms: u64,
) -> Result<(), AlarmError> {
  let mut stmt = conn
  .prepare(
    "INSERT OR REPLACE INTO global_alarms (tenant, cell_id, scheduled_time_unix_ms) VALUES (?, ?, ?)",
  )?;
  stmt.execute((tenant, cell_id, scheduled_time_unix_ms))?;
  Ok(())
}

pub async fn dispatch_alarm_locally(
  alarm: Alarm,
  node_state: Arc<NodeState>,
) -> anyhow::Result<Alarm> {
  let (_sock_path, stream, _process_key) = node_state
    .cell_manager
    .get_or_spawn_normal_cell(&alarm.tenant, &alarm.cell_id, node_state.clone())
    .await?;

  let io = hyper_util::rt::TokioIo::new(stream);
  let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
    .await
    .inspect_err(|e| {
      error!(?alarm, error = ?e, "Failed to handshake with Deno process");
    })?;

  tokio::spawn({
    let alarm = alarm.clone();
    async move {
      if let Err(e) = conn.await {
        error!(?alarm, error = ?e, "Connection error");
      }
    }
  });

  let req = hyper::Request::builder()
    .uri("/_internal/alarm")
    .method(hyper::Method::POST)
    .body(http_body_util::Full::new(bytes::Bytes::from(
      alarm.scheduled_time_unix_ms.to_string(),
    )))
    .unwrap();

  let res = sender.send_request(req).await?;

  if res.status() != http::StatusCode::OK {
    anyhow::bail!("Non-200 response from Deno process: {}", res.status());
  }

  Ok(alarm)
}

async fn dispatch_alarm_remotely(
  alarm: Alarm,
  node_state: Arc<NodeState>,
) -> anyhow::Result<Alarm> {
  let cell_owner = node_state
    .peer_manager
    .get_owner_peer(&alarm.tenant, &alarm.cell_id);
  // TODO(magurotuna): Can we have a better way to get internal address?
  let cell_owner_internal_addr = {
    let mut a = cell_owner;
    a.set_port(cell_owner.port() + 1);
    a
  };

  let tcp_stream = TcpStream::connect(cell_owner_internal_addr).await?;
  let io = hyper_util::rt::TokioIo::new(tcp_stream);
  let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

  tokio::spawn(async move {
    if let Err(e) = conn.await {
      error!(%cell_owner, error = ?e, "Failed to send alarm to system main cell owner");
    }
  });

  let body = serde_json::to_string(&alarm)?;

  let req = hyper::Request::builder()
    .uri("/_internal/dispatch_alarm")
    .method(hyper::Method::POST)
    .body(http_body_util::Full::new(bytes::Bytes::from(body)))
    .unwrap();

  let res = sender.send_request(req).await?;

  if res.status() != http::StatusCode::OK {
    anyhow::bail!("Non-200 response from Deno process: {}", res.status());
  }

  Ok(alarm)
}

fn get_due_alarms(
  conn: &rusqlite::Connection,
  current_timestamp: DateTime<Utc>,
  limit: u32,
) -> Result<Vec<Alarm>, AlarmError> {
  let mut stmt = conn.prepare(
    "SELECT tenant, cell_id, scheduled_time_unix_ms FROM global_alarms WHERE scheduled_time_unix_ms <= ? ORDER BY scheduled_time_unix_ms LIMIT ?",
  )?;
  let rows = stmt.query_map(
    [current_timestamp.timestamp_millis(), limit as i64],
    |row| {
      Ok(Alarm {
        tenant: row.get("tenant")?,
        cell_id: row.get("cell_id")?,
        scheduled_time_unix_ms: row.get("scheduled_time_unix_ms")?,
      })
    },
  )?;
  let alarms = rows.collect::<Result<Vec<_>, _>>()?;
  stmt.finalize()?;
  Ok(alarms)
}

fn dispatch_alarms_handler(
  conn: &mut rusqlite::Connection,
  node_state: Arc<NodeState>,
  current_timestamp: DateTime<Utc>,
  limit: u32,
  self_message_tx: tokio::sync::mpsc::Sender<AlarmProcessRequest>,
  response: tokio::sync::oneshot::Sender<Result<(), AlarmError>>,
) {
  let alarms = match get_due_alarms(conn, current_timestamp, limit) {
    Ok(alarms) => alarms,
    Err(e) => {
      error!(error = ?e, "Failed to get due alarms");
      let _ = response.send(Err(e));
      return;
    }
  };

  // Spawn a task to dispatch alarms in the background.
  tokio::task::spawn_local(async move {
    let futs = alarms.into_iter().map(|alarm| {
      use futures::{FutureExt as _, TryFutureExt as _};
      if node_state
        .peer_manager
        .is_local_owner(&alarm.tenant, &alarm.cell_id)
      {
        dispatch_alarm_locally(alarm.clone(), node_state.clone()).inspect_err(move |e| {
          error!(?alarm, error = ?e, "Failed to dispatch alarm to local Deno process");
        }).boxed()
      } else {
        dispatch_alarm_remotely(alarm.clone(), node_state.clone()).inspect_err(move |e| {
          error!(?alarm, error = ?e, "Failed to dispatch alarm to remote cell owner");
        }).boxed()
      }
    });

    // Awaits all futures and collect successfully dispatched alarms.
    let dispatched_alarms = futures::future::join_all(futs)
      .await
      .into_iter()
      .flatten()
      .collect();

    // Send a message to the message box to notify that the requested dispatch
    // is completed.
    let _ = self_message_tx
      .send(AlarmProcessRequest::DispatchCompleted {
        response,
        dispatched_alarms,
      })
      .await;
  });
}

fn dispatch_completed_handler(
  conn: &rusqlite::Connection,
  dispatched_alarms: Vec<Alarm>,
  response: tokio::sync::oneshot::Sender<Result<(), AlarmError>>,
) {
  if dispatched_alarms.is_empty() {
    let _ = response.send(Ok(()));
    return;
  }

  // Delete the dispatched alarms from the internal datastore.
  // Note that new alarms may have been set while dispatching alarms. In order
  // to avoid deleting such alarms, we delete alarms whose scheduled time
  // matches the dispatched one.
  let placeholders = dispatched_alarms
    .iter()
    .map(|_| "(?, ?, ?)")
    .collect::<Vec<_>>()
    .join(", ");
  let sql = format!(
    "DELETE FROM global_alarms WHERE (tenant, cell_id, scheduled_time_unix_ms) IN ({})",
    placeholders
  );

  let Ok(mut stmt) = conn.prepare(&sql).inspect_err(|e| {
    error!(error = ?e, "Failed to prepare batch delete statement");
  }) else {
    let _ = response.send(Ok(()));
    return;
  };

  let params = dispatched_alarms.iter().flat_map(|alarm| {
    [
      alarm.tenant.clone(),
      alarm.cell_id.clone(),
      alarm.scheduled_time_unix_ms.to_string(),
    ]
  });

  if let Err(e) = stmt.execute(rusqlite::params_from_iter(params)) {
    error!(error = ?e, "Failed to batch delete dispatched alarms");
  }

  let _ = response.send(Ok(()));
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test_log::test(tokio::test)]
  async fn test_alarm_processor() {
    let data_dir = tempfile::tempdir().unwrap();
    let db_path = data_dir.path().join("alarm.db");
    let alarm_processor = AlarmProcessor::new(&db_path).unwrap();
    let handle = alarm_processor.handle();

    let result = handle
      .get("mytenant".to_string(), "foo".to_string())
      .await
      .unwrap_err();
    assert!(matches!(result, AlarmError::AlarmNotFound), "{result:?}");

    let result = handle
      .delete("mytenant".to_string(), "foo".to_string())
      .await
      .unwrap_err();
    assert!(matches!(result, AlarmError::AlarmNotFound), "{result:?}");

    handle
      .set("mytenant".to_string(), "foo".to_string(), 1000)
      .await
      .unwrap();

    let result = handle
      .get("mytenant".to_string(), "foo".to_string())
      .await
      .unwrap();
    assert_eq!(result.scheduled_time_unix_ms, 1000);
    let result = handle
      .get("mytenant".to_string(), "different_cell".to_string())
      .await
      .unwrap_err();
    assert!(matches!(result, AlarmError::AlarmNotFound), "{result:?}");

    handle
      .delete("mytenant".to_string(), "foo".to_string())
      .await
      .unwrap();

    let result = handle
      .get("mytenant".to_string(), "foo".to_string())
      .await
      .unwrap_err();
    assert!(matches!(result, AlarmError::AlarmNotFound), "{result:?}");

    // TODO: test dispatch
  }
}
