```
src/
├── main.rs                # Hyper server + startup
├── service.rs             # tower::Service impl (DenoProxyApp)
├── router.rs              # routes reqs to handlers (room, static, mesh)
├── app_state.rs           # Arc<AppState> containing process_manager, peer_manager
│
├── room.rs                # /room/{id} logic: spawn + proxy + websocket upgrade
├── static_files.rs        # serve static assets from disk
├── mesh.rs                # peer discovery, consistent hash, ownership resolution
├── mesh_debug.rs          # /_mesh/ endpoints
├── process_manager.rs     # spawn/manage Deno processes
├── process_reaper.rs      # idle isolate cleanup task
├── bootstrap.rs           # generates bootstrap.ts or sets up isolate context
│
├── sqlite_replica.rs      # Litestream logic per room
├── db_adapter.rs          # `ctx.db` glue for room code
│
├── proxy_peer.rs          # forward request to TCP peer
├── proxy_uds.rs           # forward request to local Unix socket
├── uri_rewrite.rs         # rewrite URIs for proxying
│
├── websocket.rs           # WebSocket upgrade handler
├── ws_proxy.rs            # WS proxy to remote peer
│
├── tenant.rs              # resolve tenant from Host: header
├── sandbox.rs             # tenant directory layout
├── config.rs              # env parsing, constants
├── errors.rs              # shared error types
├── utils.rs               # small helpers
```
