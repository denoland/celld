### Pingora → Hyper 1.x Port: step‑by‑step with **`cargo check` gates**

> Focus is raw cold‑start speed; logging/metrics will come later.

Update this list as you progress:
- [ ] Stage 1: Scaffold Hyper server
- [ ] Stage 2: Define `DenoProxyApp` service
- [ ] Stage 3: Static‑file handler
- [ ] Stage 4: Local room path
- [ ] Stage 5: Peer‑forward path
- [ ] Stage 6: WebSocket upgrade
- [ ] Stage 7: Debug endpoints
- [ ] Stage 8: Process reaper

---

## Stage 1 — Scaffold Hyper server

1. **Add crates** to `Cargo.toml`
   ```toml
   tokio       = { version = "1", features = ["full"] }
   hyper       = { version = "1", features = ["full"] }
   tower       = "0.4"
   hyper-util  = "0.1"
   hyperlocal  = "0.9"         # UDS connector
   hyper-tungstenite = "0.13"  # WS upgrade helper
   ```
2. **Create** `AppState` (`Arc<ProcessManager>, Arc<PeerManager>`).
3. **Replace `main`** with minimal Hyper server:

   ```rust
   #[tokio::main]
   async fn main() -> anyhow::Result<()> {
       tracing_subscriber::fmt::init();
       let state = Arc::new(AppState::new()?);
       let make  = tower::make::Shared::new(DenoProxyApp::new(state));
       hyper::Server::bind(&([0,0,0,0], DATA_PORT).into())
           .serve(make)
           .await?;
       Ok(())
   }
   ```

4. **`cargo check`** (compiles, nothing else).

---

## Stage 2 — Define `DenoProxyApp` Service

```rust
pub struct DenoProxyApp { state: Arc<AppState> }

impl DenoProxyApp {
    pub fn new(state: Arc<AppState>) -> Self { Self { state } }
}

impl tower::Service<hyper::Request<hyper::Body>> for DenoProxyApp {
    type Response = hyper::Response<hyper::Body>;
    type Error    = hyper::Error;
    type Future   = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>> + Send>>;

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let st = self.state.clone();
        Box::pin(async move { router(req, st).await.map_err(|e| e.into()) })
    }
}
```

_Tip for speed_: nothing allocates in `poll_ready`; Hyper calls it once per
task.

**`cargo check`**

---

## Stage 3 — Static‑file handler

- Implement `serve_static()` using `tokio::fs::read` into `Body::from(bytes)`.
- Add simple `router` match arm for paths **not** under `/room/` or `/_mesh/*`.

**`cargo check && cargo test static_file_serving`**

---

## Stage 4 — Local room path

- In `handle_room`:
  - call `ProcessManager::get_or_spawn_process`
  - proxy request to UDS via **hyperlocal**:

    ```rust
    let connector = hyperlocal::UnixConnector;
    static CLIENT: Lazy<Client<_, Body>> =
        Lazy::new(|| Client::builder(hyper_util::rt::TokioExecutor::new()).build(connector));
    ```

_Tips_

- Avoid `to_string()` on path; use `&Path` where possible.
- Leave `ProcessManager` async wait intact—focus is network layer now.

**`cargo check && cargo test basic_db`**

---

## Stage 5 — Peer‑forward path

- Global HTTP client:

  ```rust
  static REMOTE: Lazy<Client<_, Body>> =
      Lazy::new(|| Client::builder(hyper_util::rt::TokioExecutor::new()).build_http());
  ```

- `proxy_to_peer()` rewrites `URI`, sends via `REMOTE`.

- Keep timeout small (`connect_timeout = 30 ms`) to fail fast.

**`cargo check && cargo test proxy_with_ephemeral_port`**

---

## Stage 6 — WebSocket upgrade

- Use **hyper‑tungstenite**:

  ```rust
  if hyper_tungstenite::is_upgrade_request(&req) {
      let (resp, fut) = hyper_tungstenite::upgrade(req, None)?;
      tokio::spawn(async move { handle_ws(fut.await?, state).await });
      return Ok(resp);
  }
  ```

- For peer WS proxy reuse `tokio_tungstenite::connect_async`.

**`cargo check && cargo test websocket_echo websocket_broadcast separate_isolates_per_room`**

---

## Stage 7 — Debug endpoints

- Add JSON responses for `/_mesh/peers` and `/_mesh/owner/{id}` in `router`.

**`cargo check && cargo test mesh_*` (if you have such tests)**

---

## Stage 8 — Process reaper

- Move logic into `ProcessManager::reap_idle(timeout)`.
- Spawn with
  `tokio::spawn(async move { loop { sleep(interval); pm.reap_idle(...).await }})`.

**`cargo check && cargo test`** (full suite)

---

### Speed tips recap

1. **One global Hyper client** each for TCP & UDS; reuse connection pools.
2. Keep cold‑start code paths **async but minimal**—no expensive logging.
3. Set small `connect_timeout` when dialing peers; fail early.
4. Avoid body buffering; pass `Body` streams through untouched.
5. Use `tokio::fs::File::open` + `hyper::body::SizedStream` if you later want
   zero‑copy `sendfile()`.

Follow stages, run **`cargo check`** after every bullet, and you’ll have a
measurable latency win while keeping the familiar
`DenoProxyApp { process_manager, peer_manager }` structure.:
