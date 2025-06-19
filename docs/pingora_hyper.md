## Migration Plan: Pingora to Hyper via Compatibility Layer

### Goal

Create a `pingora_hyper` module that implements Pingora's API surface using
Hyper as the underlying HTTP server. This allows `celld` to migrate from Pingora
to Hyper with minimal code changes - just switching imports. The compatibility
layer will provide the exact same interfaces that `celld` currently uses from
Pingora.

### Why This Approach

After reviewing the code, `celld` uses a relatively small subset of Pingora's
functionality:

- HTTP request/response handling via the `ProxyHttp` trait
- Unix socket connections for Deno processes
- WebSocket upgrades
- Background service management
- Basic error types

By implementing just these interfaces, we can avoid rewriting the entire
application.

### Pingora API Surface Used by celld

From code analysis:

1. **Core HTTP Flow**:
   - `ProxyHttp` trait with methods: `new_ctx()`, `request_filter()`,
     `upstream_peer()`, `upstream_request_filter()`, `logging()`
   - `Session` for request/response handling
   - `HttpPeer` for upstream connections (both TCP and Unix sockets)

2. **Server Management**:
   - `Server::new_with_opt_and_conf()`
   - `Server::add_service()`
   - `Server::run_forever()`
   - `http_proxy_service()` helper function

3. **Background Services**:
   - `BackgroundService` trait with `start(ShutdownWatch)` method
   - `background_service()` helper to wrap services
   - `ShutdownWatch` for coordinated shutdown

4. **Types & Helpers**:
   - `Error`, `ErrorType`, `Result`
   - `RequestHeader`, `ResponseHeader`
   - Status code handling

### Implementation Plan

#### Phase 1: Project Setup ✅

**Goal**: Create the compatibility module structure with feature flag

- [x] Add feature flag to `Cargo.toml`:
  ```toml
  [features]
  default = []
  hyper-compat = ["hyper", "hyper-util", "tokio-tungstenite", "tower"]
  ```

- [x] Create module structure:
  - [x] Create `src/pingora_hyper/mod.rs`
  - [x] Create `src/pingora_hyper/server.rs`
  - [x] Create `src/pingora_hyper/proxy.rs`
  - [x] Create `src/pingora_hyper/service.rs`
  - [x] Create `src/pingora_hyper/error.rs`

- [x] In `src/main.rs`, add conditional imports:
  ```rust
  #[cfg(not(feature = "hyper-compat"))]
  use pingora::prelude::*;

  #[cfg(feature = "hyper-compat")]
  use crate::pingora_hyper::{self as pingora, prelude::*};
  ```

- [x] Create basic type stubs in each module to make it compile

**Test checkpoint**: ✅ `cargo check --features hyper-compat` compiles (even if
nothing works)

#### Phase 2: Core Types Implementation ✅

**Goal**: Implement basic types and server structure

- [x] In `src/pingora_hyper/error.rs`:
  - [x] Define `Error` enum with common cases
  - [x] Define `ErrorType` enum matching Pingora's
  - [x] Implement `Error::explain()` method
  - [x] Implement conversions to HTTP status codes

- [x] In `src/pingora_hyper/server.rs`:
  - [x] Define `Server` struct holding services and config
  - [x] Define `ServerConf` with `grace_period_seconds` field
  - [x] Implement `Server::new_with_opt_and_conf()`
  - [x] Implement `Server::add_service()` - just store in Vec
  - [x] Stub `Server::run_forever()` - panic with "not implemented"

- [x] In `src/pingora_hyper/service.rs`:
  - [x] Define `BackgroundService` trait matching Pingora's
  - [x] Define `ShutdownWatch` wrapping `tokio::sync::watch::Receiver<()>`
  - [x] Implement `background_service()` helper function
  - [x] Implement `ShutdownWatch::changed()` method

**Test checkpoint**: ✅ Types are defined, code compiles with feature flag

#### Phase 3: HTTP Server Foundation

**Goal**: Get basic HTTP server running with health checks

- [ ] In `src/pingora_hyper/server.rs`:
  - [ ] Implement `Server::run_forever()`:
    - [ ] Start Hyper servers on configured addresses
    - [ ] Route requests to appropriate services
    - [ ] Handle graceful shutdown
  - [ ] Implement `http_proxy_service()` function

- [ ] In `src/pingora_hyper/proxy.rs`:
  - [ ] Define `Session` struct with request/response state
  - [ ] Implement `Session::req_header()` returning `&RequestHeader`
  - [ ] Implement `Session::write_response_header()`
  - [ ] Implement `Session::write_response_body()`
  - [ ] Implement `Session::set_keepalive()`
  - [ ] Define `ProxyHttp` trait matching Pingora's
  - [ ] Create internal executor that calls trait methods in order

- [ ] In router.rs, test health check:
  - [ ] Ensure `/_health` endpoint works with hyper-compat

**Test checkpoint**:

- [ ] Server starts: `cargo run --features hyper-compat`
- [ ] Health check works: `curl http://localhost:8000/_health`

#### Phase 4: Request Routing & Static Files

**Goal**: Implement request filtering and static file serving

- [ ] In `src/pingora_hyper/proxy.rs`:
  - [ ] Implement `Session::respond_error_with_body()`
  - [ ] Add request body handling to Session
  - [ ] Implement proper error propagation

- [ ] Static file serving:
  - [ ] Ensure `request_filter` can return early with response
  - [ ] Test with existing static file logic

**Test checkpoints**:

- [ ] `test_static_file_serving` passes
- [ ] `test_default_tenant` passes

#### Phase 5: Upstream Connections (Unix Sockets)

**Goal**: Implement HttpPeer and upstream request proxying

- [ ] In `src/pingora_hyper/proxy.rs`:
  - [ ] Define `HttpPeer` struct with address and peer type
  - [ ] Implement `HttpPeer::new()` for TCP peers
  - [ ] Implement `HttpPeer::new_uds()` for Unix socket peers
  - [ ] Implement connection logic in proxy executor:
    - [ ] Create Hyper client with Unix socket connector
    - [ ] Forward request to upstream
    - [ ] Stream response back

- [ ] Session additions:
  - [ ] Implement `Session::req_header_mut()`
  - [ ] Implement `upstream_request_filter` call

**Test checkpoints**:

- [ ] `test_proxy_with_ephemeral_port` passes
- [ ] `basic_db` test passes
- [ ] `env_test` passes

#### Phase 6: WebSocket Support

**Goal**: Implement WebSocket upgrade handling

- [ ] In `src/pingora_hyper/proxy.rs`:
  - [ ] Add WebSocket detection to Session
  - [ ] Implement upgrade handling:
    - [ ] Follow pattern from `docs/hyper-example.rs`
    - [ ] Detect upgrade headers
    - [ ] Handle bidirectional streaming with `tokio::io::copy_bidirectional`
  - [ ] Ensure connection tracking works for WebSockets

- [ ] Key implementation details from example:
  - [ ] Check for `SEC_WEBSOCKET_KEY` header
  - [ ] Create upgrade response with proper headers
  - [ ] Use `hyper::upgrade::on()` for both client and upstream
  - [ ] Handle the upgraded connections properly

**Test checkpoints**:

- [ ] `test_websocket_echo` passes
- [ ] `test_websocket_broadcast` passes
- [ ] `test_separate_isolates_per_cell` passes

#### Phase 7: Background Services

**Goal**: Implement background service management

- [ ] In `src/pingora_hyper/service.rs`:
  - [ ] Implement service spawning in `Server::run_forever()`
  - [ ] Create shutdown coordination:
    - [ ] Spawn each service with shutdown watch
    - [ ] Wait for all services on shutdown
  - [ ] Handle graceful shutdown timeout

- [ ] Test each service starts:
  - [ ] ProcessReaper
  - [ ] HeartbeatService
  - [ ] AlarmScheduler
  - [ ] ControlSocketListener

**Test checkpoint**: All background services start and shutdown cleanly

#### Phase 8: Advanced Features

**Goal**: Complete remaining functionality

- [ ] Request body handling:
  - [ ] Implement `Session::read_request_body()`
  - [ ] Handle streaming for large bodies

- [ ] Internal API support:
  - [ ] Ensure internal server works on separate port
  - [ ] Test internal endpoints

- [ ] Connection pooling:
  - [ ] Add connection reuse for Unix sockets
  - [ ] Add connection pooling for remote HTTP

- [ ] Error handling refinement:
  - [ ] Ensure all Pingora error types are handled
  - [ ] Match status code behavior

**Test checkpoints** (from test_mesh.rs):

- [ ] `test_mesh_cell_connection` passes
- [ ] `test_mesh_message_broadcast` passes
- [ ] `test_mesh_dynamic_membership` passes
- [ ] `test_node_failure_takeover` passes
- [ ] `test_concurrent_takeover_locking` passes
- [ ] `test_restore_coordination` passes

#### Phase 9: Cleanup & Optimization

**Goal**: Remove Pingora dependency and optimize

- [ ] Performance testing:
  - [ ] Benchmark against Pingora version
  - [ ] Optimize hot paths
  - [ ] Reduce allocations

- [ ] Make hyper-compat the default:
  - [ ] Update Cargo.toml default features
  - [ ] Update CI to test both variants
  - [ ] Update documentation

- [ ] Consider removing original Pingora code:
  - [ ] After all tests pass
  - [ ] After performance validation

### Key Implementation Notes

1. **WebSocket Handling**: The `docs/hyper-example.rs` shows the pattern:
   - Upgrade both client and upstream connections
   - Use `tokio::io::copy_bidirectional` for proxying
   - Handle connection cleanup properly

2. **Session State**: The Session struct needs to maintain:
   - Original request headers
   - Mutable request for upstream
   - Response buffer
   - Connection state flags

3. **Error Mapping**: Create a comprehensive mapping from Hyper errors to
   Pingora error types

4. **Async Execution**: The ProxyHttp trait methods are called in sequence:
   - `request_filter` (can return early)
   - `upstream_peer` (get connection info)
   - `upstream_request_filter` (modify request)
   - Make upstream request
   - Stream response
   - `logging` (cleanup)

5. **Testing Strategy**:
   - Run with `cargo test --features hyper-compat`
   - Start with simple tests, work up to complex mesh tests
   - Use `RUST_LOG=debug` for troubleshooting

This approach minimizes changes to existing code while providing a clean
migration path from Pingora to Hyper.
