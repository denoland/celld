# Phase 3: Room Resilience & Takeover - Updated To-Do List

Goal: Another node automatically takes over responsibility for a room if the
primary node fails, ensuring availability. S3 is used for node discovery
(Phase 1) and lock coordination (Phase 2).

- [ ] Refine Hashing Key & PeerManager:
  - Define/confirm a shared utility function
    `get_room_hash_key(tenant: &str, room_id: &str) -> String`.
  - Update `PeerManager::get_owner_peer` to accept `tenant` and `room_id`, using
    `get_room_hash_key` for the lookup.
  - Update `PeerManager::is_local_owner` to accept `tenant` and `room_id`, using
    `get_room_hash_key`.
  - Rename `get_candidate_owners` to `get_room_owners`.
  - Update `PeerManager::get_room_owners` signature to accept `tenant` and
    `room_id`, and use `get_room_hash_key` for the `get_with_replicas` lookup.
    Ensure it continues to filter for active peers using `is_peer_active`.

- [ ] Implement Takeover Coordination Logic (`process_manager.rs`):
  - Modify `ProcessManager::get_or_spawn_process`:
    - Input: Takes `host` (tenant), `room_id`.
    - Key: Calculate `process_key = get_room_hash_key(host, room_id)`.
    - Check Existing Process: Look for existing process using `process_key`. If
      found and connectable, return it (existing logic).
    - Determine Ownership: If no existing process, call
      `peer_manager.get_room_owners(host, room_id)` to get the ordered list of
      potential active owners.
    - Takeover Check:
      - If the current node (`self_advertise_addr`) is _not_ the first owner in
        the list:
        - Check if the first owner is still active
          (`peer_manager.is_peer_active`). _Initially, rely on `is_peer_active`.
          Future optimization: check `status.json` if implemented._
        - If the first owner is _inactive_: Proceed to attempt lock acquisition.
        - If the first owner is _active_: This node should _not_ take over.
          Return an error or state indicating it's not the owner/candidate
          responsible for startup.
      - If the current node _is_ the first owner (or if the first owner was
        inactive and this node is next): Proceed to lock acquisition.
    - Acquire Lock: Attempt
      `distributed_lock.try_acquire(process_key, node_id, ttl)`.
      - If successful: Continue to restore/spawn.
      - If failed (lock held): Return an error (e.g.,
        `ProxyError::InternalError` indicating lock contention/takeover in
        progress elsewhere).
    - Restore/Spawn: If lock acquired (or not needed because it's the undisputed
      primary), proceed with the _existing_ `SqliteReplica::ensure_restored`
      logic (which uses the lock internally again for safety) and
      `spawn_deno_process`.
    - Release Lock: Ensure the lock is released _after_ the Deno process is
      confirmed running and the socket is available (or if spawning fails
      definitively). _This might need careful placement, potentially associating
      lock lifetime with the `ProcessEntry` or handling release in
      `ensure_restored`._
  - (Optional) Update `status.json`: After a successful takeover (lock acquired,
    restore complete, process running), write the current `node_id` to
    `s3://<bucket>/<prefix>/room_status/<tenant>/<room_id>.json`.

- [ ] Update Proxy Forwarding Logic (`main.rs`):
  - Modify `Proxy::upstream_peer`:
    - Input: Gets `tenant` (host) and `room_id` from context.
    - Check Local Ownership: Call
      `peer_manager.is_local_owner(tenant, room_id)`. If true, proceed to call
      `process_manager.get_or_spawn_process` (local handling).
    - Forwarding: If `is_local_owner` is false:
      - Call `peer_manager.get_room_owners(tenant, room_id)` to get the ordered
        list of active owners.
      - Iterate through the returned `owners` list:
        - Attempt to create `HttpPeer` for the owner's address.
        - If successful, return `Ok(Box::new(peer))` immediately.
        - If connection fails (timeout, refused, etc.): Log the failure and
          continue to the next owner in the list.
      - If the list is exhausted or no owner could be connected to: Return an
        appropriate error (e.g.,
        `pingora::Error::explain(ErrorType::HTTPStatus(StatusCode::SERVICE_UNAVAILABLE.into()), ...)`).

- [ ] Configuration & Testability:
  - Add `ROOMD_STALENESS_THRESHOLD_SECS` environment variable parsing in
    `config.rs`.
  - Pass the configured staleness threshold value when creating
    `S3ClusterMembership`.

- [ ] Testing:
  - Implement and run "Durability Test 2 (Node Failure)" as described in
    `roadmap.md`.
  - Add tests for concurrent takeover attempts (verify only one node succeeds
    via locking).
  - Add tests for proxy forwarding correctly retrying down the owner list.

- [ ] Code Refinement (Optional but Recommended):
  - Review terminology usage ("Node" vs "Peer") and standardize where
    appropriate (favoring "Node").
  - Ensure consistent logging for takeover events (detection, lock attempts,
    restore, success/failure).
