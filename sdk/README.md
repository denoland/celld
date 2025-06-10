# Deno Cells

This is the TypeScript SDK for building stateful, distributed applications on
[Cells](https://ghcr.io/denoland/cells).

Cells provides a runtime for stateful, distributed JavaScript/TypeScript
applications with just three components: **Deno** isolates for compute,
**SQLite** for state, and **S3** for coordination. No other infrastructure
required.

Your application is composed of many independent "cells" - each addressable by
ID through URLs like `http://your-server/cell/agent-123` or
`http://your-server/cell/game-room-42`.

Each cell is:

- **Serverless** - cells activate when accessed, shut down when idle
- **Globally unique** - only one instance of `/cell/abc` runs across your entire
  cluster
- **Stateful** - private SQLite database with built-in replication to S3

The architecture scales horizontally: individual cells don't scale up (they're
single-threaded), but you can run thousands of cells across your cluster.
Perfect for AI agents that need persistent memory, multiplayer game rooms,
durable workflow execution, or any system that maps naturally to independent
state machines.

**S3 is the only cloud dependency** - it handles state replication, distributed
locking, and cluster coordination. Run anywhere S3 runs.

## Getting Started

To run Cells:

```bash
docker run ghcr.io/denoland/cells --help
```

## Basic example

Here's a simple cell that maintains a counter:

```typescript
import { cell } from "jsr:@ry/cells";

// Initialize database
cell.db.exec(`
  CREATE TABLE IF NOT EXISTS counter (
    id TEXT PRIMARY KEY, 
    value INTEGER
  )
`);
cell.db.exec(`INSERT OR IGNORE INTO counter VALUES ('hits', 0)`);

// Handle HTTP requests
cell.request((req) => {
  cell.db.exec(`UPDATE counter SET value = value + 1 WHERE id = 'hits'`);
  const result = cell.db.prepare(`SELECT value FROM counter WHERE id = 'hits'`)
    .get();
  return new Response(`Count: ${result.value} (Cell ID: ${cell.id})\n`);
});
```

Run it:

```bash
docker run -p 8000:8000 -v $PWD:/app ghcr.io/denoland/cells ./main.ts
```

Access your cell:

```bash
curl http://localhost:8000/cell/my-first-cell
```

For deployment and operational details, see:

```bash
docker run ghcr.io/denoland/cells --help
```

## Core Concepts

### Cell Identity

Each cell has a unique identifier accessible via `cell.id`. In multi-tenant
mode, `cell.tenant` provides the tenant ID.

```typescript
import { cell } from "jsr:@ry/cells";

cell.request(() => {
  return new Response(`Hello from ${cell.tenant}/${cell.id}`);
});
```

### SQLite Database

Every cell gets its own private SQLite database via `cell.db`, automatically
persisted to S3. Uses the
[Node.js SQLite API](https://nodejs.org/api/sqlite.html).

```typescript
import { cell } from "jsr:@ry/cells";

// Create tables
cell.db.exec(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  )
`);

// Insert data
const stmt = cell.db.prepare(`INSERT INTO messages (content) VALUES (?)`);
const result = stmt.run("Hello, Cell!");
console.log(`Inserted message with ID: ${result.lastInsertRowid}`);

// Query data
const messages = cell.db.prepare(
  `SELECT * FROM messages ORDER BY timestamp DESC LIMIT 10`,
).all();
```

### HTTP Request Handling

Register a handler for incoming HTTP requests:

```typescript
import { cell } from "jsr:@ry/cells";

cell.request(async (req) => {
  const url = new URL(req.url);

  if (url.pathname === "/status") {
    return new Response("OK");
  }

  if (req.method === "POST") {
    const body = await req.json();
    // Process and store in database
    return new Response(JSON.stringify({ received: body }), {
      headers: { "Content-Type": "application/json" },
    });
  }

  return new Response("Not Found", { status: 404 });
});
```

### WebSockets

Build real-time applications with WebSocket support:

```typescript
import { cell } from "jsr:@ry/cells";

// Handle new connections
cell.connect((socket, id) => {
  console.log(`Client ${id} connected`);
  socket.send(JSON.stringify({ type: "welcome", id }));
});

// Handle messages
cell.message((event, socket, id) => {
  const data = JSON.parse(event.data);

  // Echo to sender
  socket.send(JSON.stringify({ echo: data }));

  // Broadcast to all except sender
  cell.broadcast(
    JSON.stringify({
      from: id,
      message: data.message,
    }),
    [id],
  );
});

// Handle disconnections
cell.close((socket, id) => {
  console.log(`Client ${id} disconnected`);
  cell.broadcast(JSON.stringify({ type: "user_left", id }));
});

// Handle errors
cell.error((error) => {
  console.error("WebSocket error:", error);
});
```

Access connected sockets:

```typescript
// Get specific socket
const socket = cell.getWebSocket(id);

// Iterate all sockets
for (const socket of cell.getWebSockets()) {
  socket.send("Server announcement");
}
```

### Scheduled Tasks (Alarms)

Schedule tasks to run at specific times:

```typescript
import { cell } from "jsr:@ry/cells";

// Set an alarm
const alarmId = await cell.setAlarm(Date.now() + 60000); // 1 minute from now

// Handle when alarm triggers
cell.alarm(() => {
  console.log("Alarm triggered!");
  // Perform scheduled task
});

// Check alarm status
const scheduledTime = cell.getAlarm(alarmId);
if (scheduledTime) {
  console.log(`Alarm scheduled for: ${new Date(scheduledTime)}`);
}

// Cancel alarm
cell.deleteAlarm(alarmId);
```

### Durable Workflows

For complex, multi-step processes that need to survive failures, use the
Workflow API:

```typescript
import { cell } from "jsr:@ry/cells";

// Define a workflow
const processUserSignup = cell.workflow.define<
  { email: string; name: string },
  { userId: string; welcomeEmailId: string }
>({
  name: "user.signup",
  handler: async ({ input, step }) => {
    // Each step is memoized - if the workflow restarts, completed steps won't re-run
    const user = await step.run(
      "create-user",
      () => createUserAccount(input.email, input.name),
    );

    const emailId = await step.run(
      "send-welcome-email",
      () => sendEmail(input.email, "Welcome!", `Hello ${input.name}!`),
    );

    return { userId: user.id, welcomeEmailId: emailId };
  },
});

// Dispatch a workflow
const runId = cell.workflow.dispatch(processUserSignup, {
  email: "user@example.com",
  name: "Alice",
});

// Check progress
const progress = cell.workflow.getRunProgress(runId);
console.log(`Workflow status:`, progress);
```

Workflows automatically retry failed steps and resume from where they left off
after crashes.

#### Workflow Step Functions

**`step.run(name, fn)`** - Execute code as a durable step:

```typescript
const result = await step.run("fetch-user", async () => {
  const user = await fetch(`/api/users/${userId}`);
  return user.json();
});
```

**`step.invoke(workflow, input?)`** - Invoke another workflow as a step:

```typescript
const childWorkflow = cell.workflow.define<{ x: number }, number>({
  name: "add.five",
  handler: async ({ input }) => input.x + 5,
});

const parentWorkflow = cell.workflow.define<
  { value: number },
  { result: number }
>({
  name: "parent",
  handler: async ({ input, step }) => {
    const result = await step.invoke(childWorkflow, { x: input.value });
    return { result };
  },
});
```

**`step.sleep(name, durationMs)`** - Pause workflow execution for a specified
duration:

```typescript
const reminderWorkflow = cell.workflow.define<{ message: string }, null>({
  name: "send.reminder",
  handler: async ({ input, step }) => {
    await step.run("send-initial", () => sendEmail(input.message));

    // Wait 24 hours before sending reminder
    await step.sleep("wait-24h", 24 * 60 * 60 * 1000);

    await step.run(
      "send-reminder",
      () => sendEmail(`Reminder: ${input.message}`),
    );
    return null;
  },
});
```

Note: During sleep, the cell may shut down to save resources. The workflow will
resume when the sleep duration expires.

## Common Patterns

### AI Agent with Memory

```typescript
import { cell } from "jsr:@ry/cells";

// Agent's long-term memory
cell.db.exec(`
  CREATE TABLE IF NOT EXISTS memories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    context TEXT,
    content TEXT,
    importance INTEGER,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
  )
`);

cell.request(async (req) => {
  const { query } = await req.json();

  // Retrieve relevant memories
  const memories = cell.db.prepare(`
    SELECT * FROM memories 
    WHERE context LIKE ? 
    ORDER BY importance DESC, timestamp DESC 
    LIMIT 5
  `).all(`%${query}%`);

  // Generate response using memories as context
  const response = await generateAIResponse(query, memories);

  // Store new knowledge
  if (response.newMemory) {
    cell.db.prepare(`
      INSERT INTO memories (context, content, importance) 
      VALUES (?, ?, ?)
    `).run(query, response.newMemory, response.importance);
  }

  return new Response(JSON.stringify(response));
});
```

### Multiplayer Game Room

```typescript
import { cell } from "jsr:@ry/cells";

// Game state
cell.db.exec(`
  CREATE TABLE IF NOT EXISTS players (
    id TEXT PRIMARY KEY,
    x REAL, y REAL,
    score INTEGER DEFAULT 0
  )
`);

const players = new Map();

cell.connect((socket, id) => {
  // Add player to game
  players.set(id, socket);
  cell.db.prepare(`INSERT INTO players (id, x, y) VALUES (?, 0, 0)`).run(id);

  // Send current game state
  const allPlayers = cell.db.prepare(`SELECT * FROM players`).all();
  socket.send(JSON.stringify({ type: "init", players: allPlayers }));
});

cell.message((event, socket, id) => {
  const { x, y } = JSON.parse(event.data);

  // Update position
  cell.db.prepare(`UPDATE players SET x = ?, y = ? WHERE id = ?`).run(x, y, id);

  // Broadcast to all players
  cell.broadcast(JSON.stringify({ type: "move", id, x, y }));
});

cell.close((socket, id) => {
  players.delete(id);
  cell.db.prepare(`DELETE FROM players WHERE id = ?`).run(id);
  cell.broadcast(JSON.stringify({ type: "leave", id }));
});
```

### Background Task Processing

```typescript
import { cell } from "jsr:@ry/cells";

const processEmailQueue = cell.workflow.define<
  { emails: Array<{ to: string; subject: string }> }
>({
  name: "email.queue",
  handler: async ({ input, step }) => {
    for (const [index, email] of input.emails.entries()) {
      await step.run(
        `send-email-${index}`,
        () => sendEmail(email.to, email.subject),
      );
    }
  },
});

// Schedule periodic processing
cell.alarm(async () => {
  const pending = cell.db.prepare(`
    SELECT * FROM email_queue WHERE sent = 0
  `).all();

  if (pending.length > 0) {
    cell.workflow.dispatch(processEmailQueue, { emails: pending });
  }

  // Schedule next check
  await cell.setAlarm(Date.now() + 60000); // Check again in 1 minute
});
```

## Architecture

- **Single Isolate**: Each cell runs in exactly one Deno isolate across the
  cluster
- **Durable State**: SQLite databases are continuously replicated to S3
- **Automatic Scaling**: Cells activate on-demand and shut down when idle
- **Crash Recovery**: Workflows and state automatically recover from failures
- **WebSocket Support**: Cells with active connections stay alive

## Next Steps

- Run `docker run ghcr.io/denoland/cells --help` for deployment options
- Explore the [API documentation](https://jsr.io/@ry/cells/doc)
