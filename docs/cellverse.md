Okay, this sounds like a fantastic project to really explore and showcase Deno
Cells! Let's lay out a high-level plan of action for building "CellVerse."

**Project: CellVerse - A Multi-User, Channel-Based LLM Agent Platform**

**Core Vision:** CellVerse will be a web application providing a Discord-like
experience where users can:

1. Log in using their GitHub account.
2. View, create, and join different "channels."
3. Interact with a distinct LLM agent within each channel. Multiple users in the
   same channel will share the interaction with that channel's specific LLM.
4. The LLM's conversation history and any channel-specific context/personality
   will be persistent within that channel (cell).

This demo will highlight Deno Cells' strengths in managing isolated, stateful,
and interactive components, making it an ideal platform for hosting agent-like
applications.

**Tech Stack (as discussed):**

- **Frontend:** Vite (we'll assume Preact for now for speed and React-like
  experience, but you can adapt).
- **Backend:** Deno Cells (`celld` running in Docker, using the "default" tenant
  configuration for local development simplicity).
- **Authentication:** GitHub OAuth (OIDC).

**High-Level Plan of Action**

**Phase 1: Foundational Setup & GitHub OAuth Configuration**

- **Objective:** Get the basic development environment running and configure the
  GitHub side of authentication.
- **Tasks:**
  1. **Project Structure:**
     - Set up a monorepo or a project with distinct `frontend` (Vite) and
       `cells` (Deno cell code) directories.
     - Configure `npm scripts` (using `npm-run-all` or `concurrently`) to run
       both the Vite dev server and the `celld` Docker container simultaneously.
       - `dev:fe`: `vite`
       - `dev:be`:
         `docker run -e RUST_LOG=info -e CELL_DENO_OUTPUT=1 -p 8000:8000 -p 8001:8001 -v $PWD/cells:/app/src -v $PWD/celld_data:/data ghcr.io/denoland/cells:latest ./src/main.ts`
         (using `/app/src` for your Deno code and `/data` for persistent `celld`
         data).
     - Done: see in src/cells/demos/cellverse
  2. **Basic Vite Frontend:**
     - Initialize a Vite + Preact (or your choice) project.
     - Create a minimal landing page.
     - Set up Vite proxy to forward `/cell/*` requests to
       `http://localhost:8000`.
     - Done: see in src/cells/demos/cellverse
  3. **Basic `celld` Setup:**
     - Create a placeholder `cells/main.ts` for the default tenant's cells. This
       will be expanded later.
     - Ensure you can run `celld` via Docker and it serves a basic response from
       a test cell (e.g., `http://localhost:8000/cell/ping` returning "pong").
     - Done: see in src/cells/demos/cellverse/cells/main.ts
  4. **GitHub OAuth App Setup:**
     - Navigate to GitHub > Settings > Developer settings > OAuth Apps > "New
       OAuth App".
     - **Application name:** e.g., "CellVerse Demo"
     - **Homepage URL:** e.g., `http://localhost:5173` (your Vite dev server)
     - **Authorization callback URL:** This is crucial. It needs to point to an
       endpoint your "Auth Cell" will handle. For local dev, it will be
       something like `http://localhost:8000/cell/auth/github/callback`.
       - _Note:_ GitHub requires HTTPS for callback URLs for production apps,
         but `http://localhost` is usually allowed for development. If you use a
         tunneling service later (like ngrok) for testing on other devices,
         you'll update this to the HTTPS ngrok URL.
     - Once created, note down the **Client ID** and generate a **Client
       Secret**. _Keep the Client Secret confidential!_
- **Stopping Point for Sign-off (Conceptual):**
  - Can you successfully run the frontend and backend concurrently?
  - Is the GitHub OAuth App created, and do you have the Client ID and Secret?
  - Does the Vite proxy seem to be configured correctly (even if there are no
    real endpoints yet)?

phase 1 is complete. Github Oauth app Client ID Ov23liRJiAlpktDPnJx1 client
secret: ad8ddca4d41ccf13b205205d8e835e33c5a5b493

---

**Phase 2 Progress Update (June 1, 2025)**

Phase 2 is now complete! GitHub OAuth authentication is fully working.

**Implementation Notes:**

- Auth cell implemented in `demos/cellverse/cells/main.ts` with all required
  endpoints:
  - `/github/login` - Initiates OAuth flow
  - `/github/callback` - Handles OAuth callback and JWT generation
  - `/me` - Returns authenticated user info
- Frontend auth components implemented:
  - `AuthService` class in `auth.ts` manages tokens and user state
  - `AuthSuccess.tsx` handles OAuth callback redirect
  - Main app shows login/logout UI based on auth state
- SQLite database stores user info with proper node:sqlite API usage
- JWT tokens stored in localStorage for session persistence

**Key Fixes Applied:**

- Hardcoded redirect URIs for local development (localhost:5173 for callback)
- Fixed database API calls (changed from `cell.db.query()` to
  `cell.db.prepare()`)
- Removed environment variable access that caused permission errors in cells
  runtime
- GitHub OAuth app callback URL configured as
  `http://localhost:5173/cell/auth/github/callback`

**Current Status:**

- Users can successfully login with GitHub
- User info (username, avatar) displays after login
- Logout functionality works properly
- Ready to proceed to Phase 3: Basic Channel Management & UI

---

**Phase 2: Authentication Cell & Frontend Login/Logout**

- **Objective:** Implement the GitHub login flow and basic session management.
- **Tasks:**
  1. **Create "Auth Cell" (`cells/auth.ts`):**
     - This cell will handle all OIDC logic.
     - It will need access to the GitHub Client ID and Secret (pass these as
       environment variables to the Docker container and access them in Deno).
     - Implement the `/github/login` endpoint:
       - Constructs GitHub authorization URL.
       - Redirects user to GitHub.
     - Implement the `/github/callback` endpoint:
       - Handles the callback from GitHub.
       - Exchanges `code` for an `access_token`.
       - Fetches user profile from GitHub API using the `access_token`.
       - (Optional for V1 demo, but good practice) Store/update user info in its
         private SQLite DB.
       - Generates a JWT session token containing user info (e.g., GitHub ID,
         username).
       - Redirects user back to the frontend (e.g.,
         `http://localhost:5173/auth-success`) with the JWT (e.g., in a URL
         fragment).
     - Implement a `/me` endpoint (e.g., `/cell/auth/me`):
       - Accepts JWT in Authorization header.
       - Validates JWT.
       - Returns user info if valid.
  2. **Frontend Auth Logic (Vite/Preact):**
     - Create a simple UI with a "Login with GitHub" button.
     - Clicking the button navigates to `/cell/auth/github/login` (proxied).
     - Handle the redirect back from the Auth Cell (e.g., on an `/auth-success`
       route):
       - Extract JWT from URL fragment.
       - Store JWT (e.g., in `localStorage`).
       - Redirect to the main app view.
     - Create an "auth context" or service to manage login state and the JWT.
     - Implement a "Logout" button that clears the JWT and redirects to a
       logged-out state.
     - UI should conditionally show Login/Logout buttons and user info (e.g.,
       "Logged in as [username]") by calling the `/cell/auth/me` endpoint.
- **Stopping Point for Sign-off (UI & Functionality):**
  - Can a user click "Login with GitHub," go through the GitHub flow, and be
    redirected back to the app?
  - Does the frontend receive and store a JWT?
  - Can the frontend display basic user information (e.g., username) after
    login?
  - Does logout clear the session on the frontend?
  - **User Sign-off:** Review the login/logout flow and the basic display of
    user status. Is it intuitive?

---

**Phase 3: Basic Channel Management & UI**

- **Objective:** Allow users to see a list of channels and create new ones. The
  concept of a "channel" is still just metadata at this stage.
- **Tasks:**
  1. **Create "Channel Registry Cell" (`cells/channel_registry_cell.ts`):**
     - This cell manages the metadata about channels.
     - Its SQLite DB will have a `channels` table (e.g., `id TEXT PRIMARY KEY`,
       `name TEXT`, `creator_github_id TEXT`, `created_at TIMESTAMP`).
     - Implement `POST /cell/channel-registry/create`:
       - Requires JWT authentication.
       - Takes `{ name: "channel-name" }` as input.
       - Generates a unique ID for the channel (this ID will become the
         `cell_id` for the actual LLM channel later, e.g.,
         `llm-channel-${uuid()}`).
       - Stores channel metadata in its DB.
       - Returns the new channel info.
     - Implement `GET /cell/channel-registry/list`:
       - Requires JWT authentication.
       - Returns a list of all channels from its DB.
  2. **Frontend Channel UI (Vite/Preact):**
     - If logged in, display a section for channels.
     - Fetch and display the list of channels from
       `/cell/channel-registry/list`.
     - Provide a simple form/button to "Create New Channel."
       - On submit, POST to `/cell/channel-registry/create`.
       - Refresh channel list on success.
     - Each listed channel should be a clickable item (though it won't navigate
       to the chat yet).
- **Stopping Point for Sign-off (UI & Functionality):**
  - Can logged-in users see a (currently empty) list of channels?
  - Can logged-in users create a new channel, and does it appear in the list?
  - Is the basic UI for listing and creating channels clear?
  - **User Sign-off:** Review the channel listing and creation UI. Does it feel
    like the start of a Discord-like sidebar?

---

**Phase 4: LLM Channel Cell & Basic Chat Interface**

- **Objective:** Implement the core chat functionality. When a user clicks a
  channel, they enter a chat room powered by an LLM specific to that channel
  cell.
- **Tasks:**
  1. **Create "LLM Channel Cell" (`cells/llm_channel_cell.ts` or similar, this
     will be the `main.ts` for cells like `/cell/llm-channel-<id>`):**
     - This is the `main.ts` that will run when a cell like
       `/cell/llm-channel-xyz` is activated.
     - **Initialization:** When this cell type is first activated for a specific
       ID (e.g., `/cell/llm-channel-abc`), its `main.ts` should:
       - Create its SQLite schema if it doesn't exist (e.g., `messages` table:
         `id INTEGER PRIMARY KEY AUTOINCREMENT`, `github_id TEXT`,
         `username TEXT`, `timestamp TIMESTAMP`, `content TEXT`,
         `is_llm_response BOOLEAN`).
       - (Optional) Load/set an initial LLM personality/prompt for this channel
         (can be hardcoded for V1 or fetched based on channel ID if you stored
         it in the registry).
     - **WebSocket Handling (`cell.connect`, `cell.message`, `cell.broadcast`
       from `jsr:@ry/cells` [cite: README.md]):**
       - On `cell.connect`:
         - Authenticate the WebSocket connection (e.g., client sends JWT as an
           initial message or in query params).
         - On successful auth, retrieve and send recent message history from
           this cell's DB to the connecting client.
       - On `cell.message`:
         - Receive a message from a user. Store it in this cell's DB.
         - Broadcast the user's message to all connected clients in _this cell_.
         - Send the user's message (and conversation history) to an LLM API
           (you'll need to integrate with an LLM provider like OpenAI,
           Anthropic, or a local model).
         - Receive the LLM's response. Store it in this cell's DB.
         - Broadcast the LLM's response to all connected clients in _this cell_.
  2. **Frontend Chat UI (Vite/Preact):**
     - When a user clicks a channel from the list (Phase 3):
       - Navigate to a new view/route (e.g., `/channel/<channel_id>`).
       - Establish a WebSocket connection to `/cell/llm-channel-<channel_id>`
         (proxied). Send JWT for auth.
       - Display incoming message history.
       - Provide a message input field.
       - On send, send the message over WebSocket.
       - Render new messages (from self, other users in the same channel, and
         the LLM) as they arrive over WebSocket.
- **Stopping Point for Sign-off (UI & Functionality):**
  - Can a user click a channel and see a chat interface?
  - Can the user send a message and get a (basic) response from an LLM?
  - Are messages displayed in the chat window?
  - Is the chat history loaded when entering a channel?
  - **User Sign-off:** Review the basic chat UI. Does it feel like a functional
    chat room? Is the LLM interaction clear?

---

**Phase 5: Multi-User & Persistence Demonstration**

- **Objective:** Clearly demonstrate that multiple users can interact in the
  same channel and that conversation history is persistent per channel.
- **Tasks:**
  1. **Testing Multi-User:**
     - Open the CellVerse app in two separate browser windows/incognito tabs.
     - Log in as two different GitHub users (or use the same user if your GitHub
       app allows multiple sessions, though different users is better).
     - Both users join the _same_ channel.
     - Verify that messages sent by User A appear for User B, and vice-versa,
       along with LLM responses.
  2. **Testing Persistence:**
     - Have a conversation in a channel.
     - Close the browser tab for that channel.
     - Stop and restart your `celld` Docker container (ensure you mounted
       `/data` to a local volume so SQLite files persist).
     - Re-open the app, log in, and re-join the same channel.
     - Verify that the previous conversation history is loaded.
  3. **UI Polish (as needed):**
     - Ensure usernames are displayed next to messages.
     - Basic styling to make the chat readable.
- **Stopping Point for Sign-off (Core Demo Value):**
  - Does the multi-user chat in a single channel work as expected?
  - Is the conversation history correctly persisted and reloaded for a channel
    cell even after a `celld` restart?
  - **User Sign-off:** This is the core "Aha!" moment for Deno Cells. Does the
    demo effectively convey the power of isolated, stateful cells for this kind
    of application?
