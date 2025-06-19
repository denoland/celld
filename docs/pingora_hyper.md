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

#### Phase 3: HTTP Server Foundation ✅

**Goal**: Get basic HTTP server running with health checks

- [x] In `src/pingora_hyper/server.rs`:
  - [x] Implement `Server::run_forever()`:
    - [x] Start Hyper servers on configured addresses
    - [x] Route requests to appropriate services
    - [x] Handle graceful shutdown
  - [x] Implement `http_proxy_service()` function

- [x] In `src/pingora_hyper/proxy.rs`:
  - [x] Define `Session` struct with request/response state
  - [x] Implement `Session::req_header()` returning `&RequestHeader`
  - [x] Implement `Session::write_response_header()`
  - [x] Implement `Session::write_response_body()`
  - [x] Implement `Session::set_keepalive()`
  - [x] Define `ProxyHttp` trait matching Pingora's
  - [x] Create internal executor that calls trait methods in order

- [x] In router.rs, test health check:
  - [x] Ensure `/_health` endpoint works with hyper-compat

**Test checkpoint**: ✅

- [x] ✅ Server starts: `cargo run --features hyper-compat`
- [x] ✅ Health check works: `curl http://localhost:8000/_health`

#### Phase 4: Request Routing & Static Files ✅

**Goal**: Implement request filtering and static file serving

- [x] In `src/pingora_hyper/proxy.rs`:
  - [x] Implement `Session::respond_error_with_body()`
  - [x] Add request body handling to Session
  - [x] Implement proper error propagation

- [x] ProxyHttp integration:
  - [x] ✅ Proper separation of concerns established
  - [x] ✅ Application logic remains in router.rs (Proxy & InternalAPI)
  - [x] ✅ pingora_hyper only implements Pingora API using Hyper
  - [x] ✅ Ready for ProxyHttp bridge implementation

**Test checkpoints**: ✅

- [x] ✅ Health check works: `curl http://localhost:8000/_health`
- [x] ✅ Server architecture supports ProxyHttp integration
- [x] ✅ No application logic duplicated in compatibility layer

**Note**: Static file serving and cell routing will work automatically once the
ProxyHttp bridge is implemented, since the existing `Proxy` and `InternalAPI`
from `router.rs` already handle all application logic.

#### Phase 5: ProxyHttp Bridge Implementation (Revised Approach) ✅

**Goal**: Create a clean bridge between hyper and ProxyHttp trait

**Architectural Changes**:

1. **Move HTTP serving into HttpProxy**: Instead of having `ListeningService`
   handle HTTP logic, move it into `HttpProxy::start_service()`. This allows
   proper integration with the ProxyHttp trait.

2. **Concrete Session type**: Create a `HyperSession` struct that implements
   Pingora's Session interface but internally works with hyper types. This
   avoids complex trait object issues.

3. **Separation of concerns**:
   - `ListeningService`: Manages TCP listeners and passes them to inner service
   - `HttpProxy`: Handles HTTP server logic and ProxyHttp bridging
   - `HyperSession`: Bridges between hyper requests/responses and Pingora
     Session API

**Implementation tasks**:

- [x] Refactor `HttpProxy::start_service()` to:
  - [x] Accept TCP listeners from `ListeningService`
  - [x] Create hyper HTTP server
  - [x] Handle incoming requests with ProxyHttp bridge

- [x] Create `Session` struct (not HyperSession):
  - [x] Store hyper request parts and body
  - [x] Build response incrementally
  - [x] Implement all Session methods

- [x] Implement bridge flow:
  - [x] Convert hyper::Request → Session
  - [x] Call ProxyHttp methods in sequence
  - [x] Convert Session → hyper::Response

- [x] Fix all type compatibility issues:
  - [x] Create proper module structure matching Pingora's
  - [x] Add conditional imports throughout codebase
  - [x] Implement error conversions
  - [x] Match method signatures exactly

**Test checkpoints**:

- [x] ✅ Code compiles with `cargo check --features hyper-compat`
- [x] ✅ Static file serving works - `test_static_file_serving` passes!
- [ ] Cell routing works - `basic_db` test currently fails (needs upstream
      proxying)

#### Phase 6: Upstream Connections (Unix Sockets) ✅

**Goal**: Implement HttpPeer and upstream request proxying

**Key implementation patterns from hyper-example.rs**:

- Use `UnixStream::connect()` for Unix socket connections
- Use `hyper::client::conn::http1::handshake()` for HTTP over Unix sockets
- Handle request/response forwarding with proper header transformation
- Use `tokio::io::copy_bidirectional()` for WebSocket upgrades

- [x] In `src/pingora_hyper/server_impl.rs`:
  - [x] Implement actual upstream connection in `proxy_http_bridge()`
  - [x] Create Unix socket client when `HttpPeer.is_uds` is true
  - [x] Forward request with modifications from `upstream_request_filter()`
  - [x] Stream response back through Session
  - [x] Handle error responses (502, 503, etc.)

- [x] **Critical bug fix in router.rs**: Fixed `remove_cell_id_from_uri()`
      function:
  - [x] Function was returning empty paths instead of "/" for root paths
  - [x] Added proper handling: when path becomes empty after prefix removal, use
        "/"
  - [x] This ensures `/cell/foo` correctly transforms to `/` when sent to Deno
        process

- [ ] Handle different response scenarios:
  - [x] Regular HTTP responses (body streaming)
  - [ ] WebSocket upgrades (bidirectional streaming) - **Phase 7**
  - [x] Error responses (502, 503, etc.)

**Test checkpoints**: ✅

- [x] ✅ `test_proxy_with_ephemeral_port` passes (Unix socket proxying works!)
- [x] ✅ Static file serving continues to work
- [ ] `basic_db` test passes - **Next: needs more comprehensive testing**
- [ ] `env_test` passes - **Next: needs testing**

**Implementation Details**:

The upstream connection implementation in `handle_uds_upstream()` follows these
steps:

1. **Connect**: Use `UnixStream::connect()` to connect to Deno process
2. **Build Request**: Convert `RequestHeader` to `hyper::Request` with proper
   method, URI, headers
3. **HTTP Client**: Use `hyper::client::conn::http1::handshake()` for HTTP over
   Unix socket
4. **Send Request**: Forward the HTTP request to upstream
5. **Stream Response**: Collect response body and headers, convert back to
   `hyper::Response`

The bridge properly handles:

- Request body forwarding (empty and with content)
- Header preservation
- Status code forwarding
- Error handling with appropriate HTTP status codes (502, 503)

**Architecture Notes**:

- ✅ **Separation of Concerns**: pingora_hyper contains no application logic
- ✅ **URI Transformation**: Handled entirely in router.rs via
  `upstream_request_filter()`
- ✅ **Bridge Pattern**: Clean conversion between hyper and Pingora types
- ✅ **Error Handling**: Proper Pingora Error types used for logging

#### Phase 7: WebSocket Support

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

#### Phase 8: Background Services

**Goal**: Implement background service management

- [ ] In `src/pingora_hyper/service.rs`:
  - [ ] Implement service spawning in `Server::run_forever()`
  - [ ] Create shutdown coordination:
    - [ ] Spawn each service with shutdown watch
    - [ ] Wait for all services on shutdown
  - [ ] Handle graceful shutdown timeout `pingora_config.grace_period_seconds`
        (default 300 seconds)

- [ ] Test each service starts:
  - [ ] ProcessReaper
  - [ ] HeartbeatService
  - [ ] AlarmScheduler
  - [ ] ControlSocketListener

**Test checkpoint**: All background services start and shutdown cleanly

#### Phase 9: Advanced Features

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

#### Phase 10: Cleanup & Optimization

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
