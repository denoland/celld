# Battle Plan: **local dev**

### 1. **Vite plugin to spawn Deno process**

- Dev server runs Vite (frontend)
- Vite plugin spawns a Deno process:
  - Listens on `/rooms/:roomId`
  - When first request hits a `roomId`, spins up a subprocess (Deno isolate)
    with:
    - `--allow-net` (for WebSockets)
    - `--allow-read`/`--allow-write` (for SQLite database)
- Static assets served by Vite itself.

**Result:** frontend + backend fullstack dev in one local server.

### 2. **Room code uses `jsr:@deno/roomd`**

- Room code imports from `@deno/roomd`.
- Inside the module, it detects:
  - **Local dev:** spawn Deno subprocess or run inline.
  - **Prod (roomd mesh):** run inside bootstrap.ts + subprocess management.

**Result:** Same source code works everywhere.

### 3. **Type-checking + LSP support**

- Frontend gets normal Vite LSP (TypeScript types, etc).
- Backend (room code) gets Deno LSP and types from `@deno/roomd`.
- `createRoom()` + `addEventListener()` model ensures type inference for event
  handlers.

**Result:** Seamless dev experience — like writing React components but for
state machines.

### 4. **Directory Layout**

Vite **owns the project structure**, but you could _standardize_ a little:

Example layout:

```
demo-llm-chat/
  vite.config.ts
  rooms/       <-- all backend code
    main.ts
    support.ts (maybe extra room logic)
  static/      <-- frontend code
    index.html
    main.ts
```

# Next Steps?

Would you like me to now:

- Draft a **mock vite.config.ts** for this model
- Draft a **mock dev.ts** (Deno script) that launches frontend+backend cleanly
- Sketch a **future multiple-room-type dispatch** system
