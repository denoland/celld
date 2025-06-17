use std::{path::Path, sync::Arc};

use chrono::{DateTime, Utc};
use tokio::net::TcpStream;
use tracing::error;

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
  Shutdown {
    response: tokio::sync::oneshot::Sender<()>,
  },
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

    let (tx, mut rx) = tokio::sync::mpsc::channel(100);
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
          match request {
            AlarmProcessRequest::Get {
              tenant,
              cell_id,
              response,
            } => {
              let res = get_alarm(&conn, &tenant, &cell_id);
              let _ = response.send(res);
            }
            AlarmProcessRequest::Delete {
              tenant,
              cell_id,
              response,
            } => {
              let res = delete_alarm(&conn, &tenant, &cell_id);
              let _ = response.send(res);
            }
            AlarmProcessRequest::Set {
              tenant,
              cell_id,
              scheduled_time_unix_ms,
              response,
            } => {
              let res =
                set_alarm(&conn, &tenant, &cell_id, scheduled_time_unix_ms);
              let _ = response.send(res);
            }
            AlarmProcessRequest::Dispatch {
              node_state,
              current_timestamp,
              limit,
              response,
            } => {
              let res = dispatch_alarms(
                &mut conn,
                node_state,
                current_timestamp,
                limit,
              )
              .await;
              let _ = response.send(res);
            }
            AlarmProcessRequest::Shutdown { response } => {
              let _ = response.send(());
              break;
            }
          }
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
        tenant,
        cell_id,
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

fn get_alarm(
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

fn delete_alarm(
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

fn set_alarm(
  conn: &rusqlite::Connection,
  tenant: &str,
  cell_id: &str,
  scheduled_time_unix_ms: u64,
) -> Result<(), AlarmError> {
  let mut stmt = conn
  .prepare(
    "INSERT OR REPLACE INTO global_alarms (tenant, cell_id, scheduled_time_unix_ms) VALUES (?, ?, ?)",
  )?;
  stmt.insert((tenant, cell_id, scheduled_time_unix_ms))?;
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

async fn dispatch_alarms(
  conn: &mut rusqlite::Connection,
  node_state: Arc<NodeState>,
  current_timestamp: DateTime<Utc>,
  limit: u32,
) -> Result<(), AlarmError> {
  let tx = conn.transaction()?;

  let mut stmt = tx.prepare(
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

  let futs = alarms.into_iter().map(|alarm| {
    let node_state = node_state.clone();

    // Return `Some(alarm)` if the alarm was processed successfully, otherwise `None`.
    async move {
      if node_state.peer_manager.is_local_owner(&alarm.tenant, &alarm.cell_id) {
        // Dispatch the alarm to the local Deno process.
        dispatch_alarm_locally(alarm.clone(), node_state.clone()).await.inspect_err(|e| {
          error!(?alarm, error = ?e, "Failed to dispatch alarm to local Deno process");
        }).ok()
      } else {
        dispatch_alarm_remotely(alarm.clone(), node_state.clone()).await.inspect_err(|e| {
          error!(?alarm, error = ?e, "Failed to dispatch alarm to remote cell owner");
        }).ok()
      }
    }
  });

  let mut stmt =
    tx.prepare("DELETE FROM global_alarms WHERE tenant = ? AND cell_id = ?")?;
  for processed_alarm in
    futures::future::join_all(futs).await.into_iter().flatten()
  {
    stmt.execute([processed_alarm.tenant, processed_alarm.cell_id])?;
  }
  stmt.finalize()?;

  tx.commit()?;

  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
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
