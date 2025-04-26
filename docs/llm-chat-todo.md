# AI chat demo

* simple chatgpt clone
* uses vite, websockets, and openai API to demonstrate capabilities of roomd
* conversation persisted in sqlite
* later support for delayed tasks like in the cloudflare-agents-starter (using
  Alerts API, but that has to be developed first)

## Project Structure

**New layout**:

```
roomd/
├── demo-llm-chat/
│   ├── index.html
│   ├── vite.config.ts
│   └── ...
├── data/
│   └── llm-chat.localhost/
│       ├── static/            ← built output of Vite
│       │   ├── index.html
│       │   └── assets/...
│       └── code/
│           └── main.ts        ← backend (already written)
```

## AI Chat Demo Checklist

After each check stop to let me verify progress before continuing.

### Set up Vite demos

- [x] `mkdir demos/llm-chat/frontend && cd demos/llm-chat/frontend`
- [x] Run: `npm create vite@latest .` (use react and typescript)
- [x] Add WS logic to connect to room
- [x] Add basic styling: split messages by role (`user`, `assistant`). 
- [x] Add scroll-to-bottom after message
- [x] Add "Clear history" button in top right

### Wire Vite build to static dir

- [x] Set `build.outDir = '../../../data/llm-chat.localhost/static'` in `vite.config.ts`
- [x] Add `"build": "vite build"` to `package.json` scripts
- [ ] Run `npm run build` to compile into correct place
- [ ] Confirm visiting `http://llm-chat.localhost:3000` loads the app

### Build SQLite persistence + OpenAI backend

- [x] add `data/llm-chat.localhost/code/main.ts`, based on example from ws-echo.localhost
- [x] `main.ts` stores user & assistant messages in `messages` table
- [x] Messages replay on `onConnect`
- [x] OpenAI completions work
- [x] Improve error handling when OpenAI returns nothing or fails
- [x] Add "bot is thinking…" message
- [x] Filter messages sent over WS (e.g., no internal `system` rows)

### Room-specific env handling

- [x] Pass `OPENAI_API_KEY` to roomd via env file (`data/llm-chat.localhost/prod.env`)
- [x] Build rust code to read env vars and pass to deno subprocess in
      `src/process_manager.rs`
- [x] Room subprocess should read `Deno.env.get("OPENAI_API_KEY")`
- [x] Add tests for env var handling

### 4. UX polish

- [x] Distinguish bot vs. user visually (bot: gray left-align, user: blue right-align)
- [x] Disable input while waiting for reply
- [x] Auto-scroll to latest message
- [x] Show timestamps (read from message timestamp)

### 6. Test & verify

- [ ] Open in two browser tabs → messages should sync
- [ ] Restart peer → history is preserved
- [ ] Kill peer, restart → DB still works (Litestream)
- [ ] Confirm OpenAI API is hit, logs show call

### 7. Demo polish

- [x] Replace default title
- [x] Make mobile-friendly layout
- [x] "bot is typing…" indicator while waiting for response
- [x] Add agent prompt in system message
