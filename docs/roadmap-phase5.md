# Roadmap: Phase 5 - Internal Control Plane

**Status:** Completed **Leads To:** Phase 6 (Alarms API)

## Goal

Establish a secure, dedicated network communication path for internal `roomd`
operations, separate from the public-facing data plane used for room traffic and
static files. This improves security posture and provides the necessary
infrastructure for future features requiring node-to-node RPCs (like Alarms).

## Non-Goals

- Implementing application-level authentication/authorization on the internal
  port (V1 focuses on network-level separation).
- Implementing any new features beyond moving existing debug endpoints.

## Key Tasks

1. **Configuration:**
   - **Define Env Var:** Introduce `INTERNAL_LISTEN_ADDR` environment variable
     (e.g., `127.0.0.1:6147`). This address is used _only_ for internal RPCs.
   - **Default Behavior (Optional):** Consider if a default derivation (e.g.,
     from `LISTEN_ADDR` or `ADVERTISE_ADDR` + offset, bound to loopback) is
     useful for single-node convenience, but log a warning recommending explicit
     setting in clustered environments. For multi-node tests, it _must_ be set
     explicitly per instance.
   - **Update `config.rs`:** Parse and store `internal_listen_addr`. Validate
     format.
   - **Documentation:** Clearly document `INTERNAL_LISTEN_ADDR`, its purpose
     (internal control plane), and recommend firewalling it from external
     access. Explain its relationship to `LISTEN_ADDR` (public data plane) and
     `ADVERTISE_ADDR` (how other nodes reach this node's public data plane).

2. **Pingora Server Setup (`main.rs`):**
   - In `start_server`:
     - Create a second Pingora `Service` specifically for internal handlers.
     - Bind this service _only_ to the `config.internal_listen_addr`. Handle
       binding errors robustly.
     - Pass necessary shared state (`Arc<NodeState>`) to the internal
       service/handlers.

3. **Implement Internal Handlers:**
   - Define a new struct/module for internal API handlers (e.g.,
     `internal_api.rs` implementing `ProxyHttp` or a simpler handler).
   - Ensure handlers have access to `NodeState`.

4. **Migrate `/_mesh` Endpoints:**
   - Remove handler logic for `/_mesh/peers` and `/_mesh/owner/*` from the
     public `Proxy` service (`main.rs`).
   - Implement handlers for `/_internal/mesh/peers` and
     `/_internal/mesh/owner/{tenant}/{room_id}` (or similar path structure)
     within the new internal API service. These handlers will use
     `NodeState::peer_manager`.

5. **Testing Strategy & Updates:**
   - **Refactor `test-mesh.rs` (`TestEnv`):**
     - Modify `TestEnv::new` and `spawn_roomd_instance` to manage _pairs_ of
       ports (public, internal) for each `roomd` instance. Ensure no port
       conflicts. Suggestion: use base port `N` for public (`ADVERTISE_ADDR`,
       `LISTEN_ADDR`) and `N+1` for internal (`INTERNAL_LISTEN_ADDR`).
     - `spawn_roomd_instance` must set both `ADVERTISE_ADDR` (for data plane
       routing/S3 registration) and `INTERNAL_LISTEN_ADDR` env vars.
     - `TestEnv` should provide helpers to get _both_ the public and internal
       address for a given test instance.
     - Modify tests like `test_mesh_dynamic_membership`,
       `test_node_failure_takeover` that query `/mesh/*` endpoints:
       - Construct the request URL using the **internal address** (host and
         internal port) obtained from `TestEnv`.
       - Target the new `/_internal/mesh/*` paths.
   - **Refactor `main.rs` tests (`TEST_SERVER`):**
     - The `Lazy` static setup needs to set _both_ `LISTEN_ADDR` (e.g.,
       `127.0.0.1:6146`) and `INTERNAL_LISTEN_ADDR` (e.g., `127.0.0.1:6147`).
     - Verify if any tests in `main.rs::tests` were implicitly relying on
       `/mesh` endpoints (unlikely, but double-check). Most tests there focus on
       public port functionality (proxying, websockets, static files) and should
       largely remain unchanged.
   - **New Tests:** Add simple tests specifically targeting the internal port to
     verify the `/internal/mesh` endpoints work there and are inaccessible on
     the public port.

## Success Criteria

- `roomd` starts with both public (`LISTEN_ADDR`) and internal
  (`INTERNAL_LISTEN_ADDR`) listeners active on distinct ports/addresses.
- Requests to the old public `/_mesh` endpoints result in 404s or are otherwise
  handled as non-internal requests.
- Requests to the new `/_internal/mesh/*` endpoints **on the internal port**
  return the expected peer/owner information.
- Requests to `/_internal/mesh/*` endpoints **on the public port** are rejected
  (e.g., 404 or connection refused if service not bound there).
- Public room traffic (`/room/*`, static files, WebSockets) on the public port
  remains unaffected.
- `test-mesh.rs` tests pass after being updated to use the internal port for
  mesh queries.
