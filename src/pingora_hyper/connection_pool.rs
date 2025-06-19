// Connection pooling implementation - planned for Phase 11
#![allow(dead_code, unused)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

/// A pooled TCP connection with metadata
#[derive(Debug)]
pub struct PooledConnection {
  pub stream: TcpStream,
  pub created_at: Instant,
  pub last_used: Instant,
}

impl PooledConnection {
  pub fn new(stream: TcpStream) -> Self {
    let now = Instant::now();
    Self {
      stream,
      created_at: now,
      last_used: now,
    }
  }

  pub fn touch(&mut self) {
    self.last_used = Instant::now();
  }

  pub fn is_expired(&self, max_age: Duration) -> bool {
    self.created_at.elapsed() > max_age
  }

  pub fn is_idle(&self, max_idle: Duration) -> bool {
    self.last_used.elapsed() > max_idle
  }
}

/// Simple TCP connection pool
pub struct TcpConnectionPool {
  pools: Arc<Mutex<HashMap<String, Vec<PooledConnection>>>>,
  max_connections_per_host: usize,
  max_connection_age: Duration,
  max_idle_time: Duration,
}

impl TcpConnectionPool {
  pub fn new() -> Self {
    Self {
      pools: Arc::new(Mutex::new(HashMap::new())),
      max_connections_per_host: 10,
      max_connection_age: Duration::from_secs(300), // 5 minutes
      max_idle_time: Duration::from_secs(60),       // 1 minute
    }
  }

  /// Get a connection from the pool or create a new one
  pub async fn get_connection(
    &self,
    address: &str,
  ) -> Result<TcpStream, Box<dyn std::error::Error + Send + Sync>> {
    // Try to get from pool first
    if let Some(stream) = self.try_get_from_pool(address).await {
      return Ok(stream);
    }

    // Create new connection
    let stream = TcpStream::connect(address).await?;
    Ok(stream)
  }

  /// Return a connection to the pool
  pub async fn return_connection(&self, address: String, stream: TcpStream) {
    let mut pools = self.pools.lock().await;
    let pool = pools.entry(address).or_insert_with(Vec::new);

    // Don't exceed max connections per host
    if pool.len() < self.max_connections_per_host {
      pool.push(PooledConnection::new(stream));
    }
    // If we're at capacity, just drop the connection
  }

  /// Try to get a usable connection from the pool
  async fn try_get_from_pool(&self, address: &str) -> Option<TcpStream> {
    let mut pools = self.pools.lock().await;
    let pool = pools.get_mut(address)?;

    // Find the first good connection and remove it
    let mut good_index = None;
    for (i, conn) in pool.iter().enumerate() {
      if !conn.is_expired(self.max_connection_age)
        && !conn.is_idle(self.max_idle_time)
      {
        good_index = Some(i);
        break;
      }
    }

    // Remove and return the good connection if found
    if let Some(index) = good_index {
      let conn = pool.remove(index);
      Some(conn.stream)
    } else {
      // Clean up expired/idle connections while we're here
      pool.retain(|conn| {
        !conn.is_expired(self.max_connection_age)
          && !conn.is_idle(self.max_idle_time)
      });
      None
    }
  }

  /// Clean up expired connections
  pub async fn cleanup(&self) {
    let mut pools = self.pools.lock().await;
    for pool in pools.values_mut() {
      pool.retain(|conn| {
        !conn.is_expired(self.max_connection_age)
          && !conn.is_idle(self.max_idle_time)
      });
    }
    // Remove empty pools
    pools.retain(|_, pool| !pool.is_empty());
  }
}

impl Default for TcpConnectionPool {
  fn default() -> Self {
    Self::new()
  }
}
