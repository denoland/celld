import { cell } from "jsr:@ry/cells";
import { z } from "npm:zod";

// Define Zod schema for bot structured responses
const BotActionSchema = z.discriminatedUnion("action", [
  z.object({
    action: z.literal("respond"),
    message: z.string(),
  }),
  z.object({
    action: z.literal("store_memory"),
    key: z.string(),
    value: z.string(),
    response: z.string().optional(),
  }),
  z.object({
    action: z.literal("read_memories"),
    filter: z.string().optional(),
    response: z.string().optional(),
  }),
  z.object({
    action: z.literal("set_alarm"),
    message: z.string(),
    delaySeconds: z.number(),
  }),
]);

type BotAction = z.infer<typeof BotActionSchema>;

const FRONTEND_URL = Deno.env.get("FRONTEND_URL");
const GITHUB_CLIENT_ID = Deno.env.get("GITHUB_CLIENT_ID");
const GITHUB_CLIENT_SECRET = Deno.env.get("GITHUB_CLIENT_SECRET");
const JWT_SECRET = Deno.env.get("JWT_SECRET");
const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY");

if (!FRONTEND_URL || FRONTEND_URL.at(-1) === "/") {
  throw new Error(`bad FRONTEND_URL env var: '${FRONTEND_URL}'`);
}

// Initialize DB schema for auth cell
if (cell.id === "auth") {
  cell.db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      github_id TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      email TEXT,
      avatar_url TEXT,
      access_token TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
}

// Initialize DB schema for channel registry
if (cell.id === "channel-registry") {
  cell.db.exec(`
    CREATE TABLE IF NOT EXISTS channels (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      creator_github_id TEXT NOT NULL,
      personality TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
}

// Initialize DB schema for channels
if (cell.id.startsWith("channel-")) {
  cell.db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      github_id TEXT NOT NULL,
      username TEXT NOT NULL,
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      content TEXT NOT NULL,
      is_llm_response INTEGER DEFAULT 0,
      is_system_message INTEGER DEFAULT 0
    )
  `);

  cell.db.exec(`
    CREATE TABLE IF NOT EXISTS channel_config (
      key TEXT PRIMARY KEY,
      value TEXT
    )
  `);

  // Create memories table for bot memory storage
  cell.db.exec(`
    CREATE TABLE IF NOT EXISTS memories (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      key TEXT NOT NULL,
      value TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);

  // Create queued_messages table for delayed responses
  cell.db.exec(`
    CREATE TABLE IF NOT EXISTS queued_messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      message TEXT NOT NULL,
      scheduled_time_unix_ms INTEGER NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_queued_messages_scheduled_time_unix_ms ON queued_messages (scheduled_time_unix_ms);
  `);
}

// Simple JWT implementation
function createJWT(payload: any): string {
  const header = btoa(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payloadStr = btoa(JSON.stringify(payload));
  const data = `${header}.${payloadStr}`;

  // Simple HMAC-SHA256 simulation (not cryptographically secure)
  const signature = btoa(data + JWT_SECRET);
  return `${data}.${signature}`;
}

function verifyJWT(token: string): any | null {
  try {
    const [header, payload, signature] = token.split(".");
    const data = `${header}.${payload}`;
    const expectedSignature = btoa(data + JWT_SECRET);

    if (signature !== expectedSignature) {
      return null;
    }

    return JSON.parse(atob(payload));
  } catch {
    return null;
  }
}

// Extract bearer token from Authorization header
function extractBearerToken(req: Request): string | null {
  const auth = req.headers.get("Authorization");
  if (!auth || !auth.startsWith("Bearer ")) return null;
  return auth.slice(7);
}

cell.request(async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  const path = url.pathname.replace(`/cell/${cell.id}`, "");

  // Auth cell endpoints
  if (cell.id === "auth") {
    // GitHub login endpoint
    if (path === "/github/login") {
      const redirectUri = `${FRONTEND_URL}/cell/auth/github/callback`;
      const githubAuthUrl = `https://github.com/login/oauth/authorize?` +
        `client_id=${GITHUB_CLIENT_ID}&` +
        `redirect_uri=${encodeURIComponent(redirectUri)}&` +
        `scope=read:user`;

      return Response.redirect(githubAuthUrl);
    }

    // GitHub callback endpoint
    if (path === "/github/callback") {
      const code = url.searchParams.get("code");
      if (!code) {
        return new Response("Missing code parameter", { status: 400 });
      }

      try {
        // Exchange code for access token
        console.log("Exchanging code for token:", code);
        const tokenResponse = await fetch(
          "https://github.com/login/oauth/access_token",
          {
            method: "POST",
            headers: {
              "Accept": "application/json",
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              client_id: GITHUB_CLIENT_ID,
              client_secret: GITHUB_CLIENT_SECRET,
              code: code,
            }),
          },
        );

        const tokenData = await tokenResponse.json();
        console.log("Token response:", tokenData);
        if (!tokenData.access_token) {
          console.error("Token error:", tokenData);
          return new Response("Failed to get access token", { status: 500 });
        }

        // Fetch user profile
        const userResponse = await fetch("https://api.github.com/user", {
          headers: {
            "Authorization": `Bearer ${tokenData.access_token}`,
            "Accept": "application/json",
          },
        });

        const userData = await userResponse.json();

        // Store/update user in DB
        cell.db.prepare(
          `INSERT OR REPLACE INTO users 
           (github_id, username, email, avatar_url, access_token, updated_at)
           VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
        ).run(
          userData.id.toString(),
          userData.login,
          userData.email,
          userData.avatar_url,
          tokenData.access_token,
        );

        // Create JWT
        const jwt = createJWT({
          github_id: userData.id.toString(),
          username: userData.login,
          email: userData.email,
          avatar_url: userData.avatar_url,
          exp: Date.now() + 7 * 24 * 60 * 60 * 1000, // 7 days
        });

        // Redirect to frontend with JWT
        return Response.redirect(`${FRONTEND_URL}/auth-success#token=${jwt}`);
      } catch (error) {
        console.error("OAuth error:", error);
        return new Response("Authentication failed", { status: 500 });
      }
    }

    // User info endpoint
    if (path === "/me") {
      const token = extractBearerToken(req);
      if (!token) {
        return new Response("Unauthorized", { status: 401 });
      }

      const payload = verifyJWT(token);
      if (!payload || payload.exp < Date.now()) {
        return new Response("Invalid or expired token", { status: 401 });
      }

      return new Response(
        JSON.stringify({
          github_id: payload.github_id,
          username: payload.username,
          email: payload.email,
          avatar_url: payload.avatar_url,
        }),
        {
          headers: { "Content-Type": "application/json" },
        },
      );
    }
  }

  // Channel registry endpoints
  if (cell.id === "channel-registry") {
    const token = extractBearerToken(req);
    if (!token) {
      return new Response("Unauthorized", { status: 401 });
    }

    const user = verifyJWT(token);
    if (!user || user.exp < Date.now()) {
      return new Response("Invalid or expired token", { status: 401 });
    }

    // Create channel
    if (path === "/create" && req.method === "POST") {
      const body = await req.json();
      if (!body.name) {
        return new Response("Missing channel name", { status: 400 });
      }

      // Create a slug from the channel name
      const slug = body.name
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/^-+|-+$/g, "")
        .substring(0, 50);

      if (!slug || slug.length < 2) {
        return new Response("Invalid channel name", { status: 400 });
      }

      const channelId = `channel-${slug}`;

      // Check if channel already exists
      const existing = cell.db.prepare(
        `SELECT id FROM channels WHERE id = ?`,
      ).get(channelId);

      if (existing) {
        return new Response("Channel already exists", { status: 409 });
      }

      // Generate personality using OpenAI
      let personality = `A helpful assistant in the ${body.name} channel.`;

      if (OPENAI_API_KEY) {
        try {
          const personalityResponse = await fetch(
            "https://api.openai.com/v1/chat/completions",
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${OPENAI_API_KEY}`,
              },
              body: JSON.stringify({
                model: "gpt-3.5-turbo",
                messages: [
                  {
                    role: "system",
                    content: "You are a creative character designer.",
                  },
                  {
                    role: "user",
                    content:
                      `Using the descriptor '${body.name}', create a distinct character. \
                  Include their D&D alignment (lawful good, chaotic evil, etc.), personality traits, speaking style, and any quirks. \
                  Make it creative and memorable. Keep it under 150 words.`,
                  },
                ],
                temperature: 0.9,
                max_tokens: 200,
              }),
            },
          );

          if (personalityResponse.ok) {
            const personalityData = await personalityResponse.json();
            personality = personalityData.choices[0].message.content;
          }
        } catch (error) {
          console.error("Error generating personality:", error);
          // Fall back to default personality
        }
      }

      cell.db.prepare(
        `INSERT INTO channels (id, name, creator_github_id, personality) VALUES (?, ?, ?, ?)`,
      ).run(channelId, body.name, user.github_id, personality);

      // Set personality in the channel cell
      try {
        await fetch(`${FRONTEND_URL}/cell/${channelId}/set-personality`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ personality }),
        });
      } catch (error) {
        console.error("Error setting channel personality:", error);
      }

      return new Response(
        JSON.stringify({
          id: channelId,
          name: body.name,
          creator_github_id: user.github_id,
          personality: personality,
        }),
        {
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    // List channels
    if (path === "/list" && req.method === "GET") {
      const channels = cell.db.prepare(
        `SELECT id, name, creator_github_id, personality, created_at 
         FROM channels ORDER BY created_at DESC`,
      ).all();

      return new Response(JSON.stringify(channels), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // Get single channel
    if (path.startsWith("/get/") && req.method === "GET") {
      const channelId = path.substring(5);
      const channel = cell.db.prepare(
        `SELECT id, name, creator_github_id, personality, created_at 
         FROM channels WHERE id = ?`,
      ).get(channelId);

      if (!channel) {
        return new Response("Channel not found", { status: 404 });
      }

      return new Response(JSON.stringify(channel), {
        headers: { "Content-Type": "application/json" },
      });
    }

    // Delete channel
    if (path === "/delete" && req.method === "DELETE") {
      const body = await req.json();
      if (!body.channelId) {
        return new Response("Missing channel ID", { status: 400 });
      }

      // Check if channel exists and user is the owner
      const channel = cell.db.prepare(
        `SELECT creator_github_id FROM channels WHERE id = ?`,
      ).get(body.channelId);

      if (!channel) {
        return new Response("Channel not found", { status: 404 });
      }

      if (channel.creator_github_id !== user.github_id) {
        return new Response("Only the channel owner can delete it", {
          status: 403,
        });
      }

      // Delete the channel
      cell.db.prepare(
        `DELETE FROM channels WHERE id = ?`,
      ).run(body.channelId);

      return new Response(JSON.stringify({ success: true }), {
        headers: { "Content-Type": "application/json" },
      });
    }
  }

  // Channel-specific endpoints
  if (cell.id.startsWith("channel-")) {
    // Set personality endpoint (called by channel registry after creation)
    if (path === "/set-personality" && req.method === "POST") {
      const body = await req.json();
      if (body.personality) {
        cell.db.prepare(
          `INSERT OR REPLACE INTO channel_config (key, value) VALUES ('personality', ?)`,
        ).run(body.personality);
        return new Response(JSON.stringify({ success: true }), {
          headers: { "Content-Type": "application/json" },
        });
      }
    }

    // Get memories endpoint
    if (path === "/memories" && req.method === "GET") {
      const token = extractBearerToken(req);
      if (!token) {
        return new Response("Unauthorized", { status: 401 });
      }

      const user = verifyJWT(token);
      if (!user || user.exp < Date.now()) {
        return new Response("Invalid or expired token", { status: 401 });
      }

      // Get channel info from registry to check ownership
      try {
        const registryResponse = await fetch(
          `${FRONTEND_URL}/cell/channel-registry/get/${cell.id}`,
          {
            headers: {
              Authorization: `Bearer ${token}`,
            },
          },
        );

        if (!registryResponse.ok) {
          return new Response("Failed to get channel info", { status: 500 });
        }

        const channelData = await registryResponse.json();

        // Check if user is the owner
        if (channelData.creator_github_id !== user.github_id) {
          return new Response("Only the channel owner can view memories", {
            status: 403,
          });
        }

        // Get memories
        const memories = cell.db.prepare(
          `SELECT id, key, value, created_at, updated_at 
           FROM memories 
           ORDER BY updated_at DESC`,
        ).all();

        return new Response(JSON.stringify({ memories }), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (error) {
        console.error("Error fetching memories:", error);
        return new Response("Internal server error", { status: 500 });
      }
    }

    // Delete memory endpoint
    if (path.startsWith("/memories/") && req.method === "DELETE") {
      const memoryId = path.substring(10);
      const token = extractBearerToken(req);
      if (!token) {
        return new Response("Unauthorized", { status: 401 });
      }

      const user = verifyJWT(token);
      if (!user || user.exp < Date.now()) {
        return new Response("Invalid or expired token", { status: 401 });
      }

      // Get channel info from registry to check ownership
      try {
        const registryResponse = await fetch(
          `${FRONTEND_URL}/cell/channel-registry/get/${cell.id}`,
          {
            headers: {
              Authorization: `Bearer ${token}`,
            },
          },
        );

        if (!registryResponse.ok) {
          return new Response("Failed to get channel info", { status: 500 });
        }

        const channelData = await registryResponse.json();

        // Check if user is the owner
        if (channelData.creator_github_id !== user.github_id) {
          return new Response("Only the channel owner can delete memories", {
            status: 403,
          });
        }

        // Delete memory
        const result = cell.db.prepare(
          `DELETE FROM memories WHERE id = ?`,
        ).run(memoryId);

        if (result.changes === 0) {
          return new Response("Memory not found", { status: 404 });
        }

        return new Response(JSON.stringify({ success: true }), {
          headers: { "Content-Type": "application/json" },
        });
      } catch (error) {
        console.error("Error deleting memory:", error);
        return new Response("Internal server error", { status: 500 });
      }
    }
  }

  return new Response("Not found", { status: 404 });
});

// WebSocket handling for channels
if (cell.id.startsWith("channel-")) {
  const authenticatedSockets = new Map<string, any>();

  cell.connect((socket: WebSocket, id: string) => {
    // Wait for authentication message
    socket.send(JSON.stringify({
      type: "auth_required",
      message: "Please authenticate",
    }));
  });

  cell.message(async (event: MessageEvent, socket: WebSocket, id: string) => {
    const data = JSON.parse(event.data);

    // Handle authentication
    if (data.type === "auth") {
      const user = verifyJWT(data.token);
      if (!user || user.exp < Date.now()) {
        socket.send(JSON.stringify({
          type: "error",
          message: "Invalid token",
        }));
        socket.close();
        return;
      }

      authenticatedSockets.set(id, user);

      // Send recent message history
      const messages = cell.db.prepare(
        `SELECT * FROM messages ORDER BY timestamp DESC LIMIT 50`,
      ).all().reverse();

      socket.send(JSON.stringify({
        type: "history",
        messages,
      }));

      socket.send(JSON.stringify({
        type: "auth_success",
        user: {
          username: user.username,
          avatar_url: user.avatar_url,
        },
      }));
      return;
    }

    // Check if authenticated
    const user = authenticatedSockets.get(id);
    if (!user) {
      socket.send(JSON.stringify({
        type: "error",
        message: "Not authenticated",
      }));
      return;
    }

    // Handle chat message
    if (data.type === "message") {
      // Store message
      const stmt = cell.db.prepare(
        `INSERT INTO messages (github_id, username, content, is_llm_response)
         VALUES (?, ?, ?, ?) RETURNING *`,
      );
      const result = stmt.get(user.github_id, user.username, data.content, 0);

      // Broadcast to all connected users
      cell.broadcast(JSON.stringify({
        type: "message",
        message: result,
      }));

      // Send to LLM and broadcast response
      if (OPENAI_API_KEY) {
        try {
          // Get 1 hour of message history for context
          const oneHourAgo = new Date(
            Date.now() - 60 * 60 * 1000,
          ).toISOString();
          const recentMessages = cell.db.prepare(
            `SELECT username, content, is_llm_response, timestamp
             FROM messages 
             WHERE timestamp >= ?
             ORDER BY timestamp DESC 
             LIMIT 100`,
          ).all(oneHourAgo).reverse();

          // Build conversation history
          const messages = recentMessages.map((msg) => ({
            role: msg.is_llm_response ? "assistant" : "user",
            content: msg.is_llm_response
              ? msg.content
              : `${msg.username}: ${msg.content}`,
          }));

          // Add the current message
          messages.push({
            role: "user",
            content: `${user.username}: ${data.content}`,
          });

          // Get channel personality from local config
          let systemPrompt = `You are a bot in the "${
            cell.id.replace("channel-", "")
          }" channel.`;

          const personalityConfig = cell.db.prepare(
            `SELECT value FROM channel_config WHERE key = 'personality'`,
          ).get();

          if (personalityConfig && personalityConfig.value) {
            systemPrompt = personalityConfig.value;
          }

          // Add structured response instructions
          systemPrompt +=
            `\n\nYou must respond with a JSON object matching one of these schemas:

1. To respond to a message:
{"action": "respond", "message": "your response here"}

2. To store a memory for later:
{"action": "store_memory", "key": "memory_key", "value": "memory_value", "response": "optional message to user"}

3. To read memories:
{"action": "read_memories", "filter": "optional_search_term", "response": "optional message to user"}

4. To set an alarm (when a user asks for a delayed response):
{"action": "set_alarm", "message": "your response here", "delaySeconds": <the number of seconds to delay the response>}

You should store memories about users, topics, or anything worth remembering.
Respond naturally while occasionally storing or retrieving memories.`;

          // Call OpenAI API with structured output
          const response = await fetch(
            "https://api.openai.com/v1/chat/completions",
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${OPENAI_API_KEY}`,
              },
              body: JSON.stringify({
                model: "gpt-4o-mini",
                messages: [
                  {
                    role: "system",
                    content: systemPrompt,
                  },
                  ...messages,
                ],
                temperature: 0.8,
                max_tokens: 300,
                response_format: { type: "json_object" },
              }),
            },
          );

          if (response.ok) {
            const aiData = await response.json();
            const aiResponse = aiData.choices[0].message.content;

            // Parse and validate the structured response
            const parsedResponse = JSON.parse(aiResponse);
            const validatedAction = BotActionSchema.parse(parsedResponse);

            // Handle different bot actions
            switch (validatedAction.action) {
              case "respond": {
                // Store and broadcast the response
                const aiResult = cell.db.prepare(
                  `INSERT INTO messages (github_id, username, content, is_llm_response)
                   VALUES (?, ?, ?, ?) RETURNING *`,
                ).get("ai", "bot", validatedAction.message, 1);

                cell.broadcast(JSON.stringify({
                  type: "message",
                  message: aiResult,
                }));
                break;
              }

              case "store_memory": {
                // Store the memory
                cell.db.prepare(
                  `INSERT OR REPLACE INTO memories (key, value, updated_at)
                   VALUES (?, ?, CURRENT_TIMESTAMP)`,
                ).run(validatedAction.key, validatedAction.value);

                // Send optional response
                if (validatedAction.response) {
                  const aiResult = cell.db.prepare(
                    `INSERT INTO messages (github_id, username, content, is_llm_response)
                     VALUES (?, ?, ?, ?) RETURNING *`,
                  ).get("ai", "bot", validatedAction.response, 1);

                  cell.broadcast(JSON.stringify({
                    type: "message",
                    message: aiResult,
                  }));
                }
                break;
              }

              case "read_memories": {
                // Read memories with optional filter
                const memories = validatedAction.filter
                  ? cell.db.prepare(
                    `SELECT key, value FROM memories 
                       WHERE key LIKE ? OR value LIKE ?
                       ORDER BY updated_at DESC`,
                  ).all(
                    `%${validatedAction.filter}%`,
                    `%${validatedAction.filter}%`,
                  )
                  : cell.db.prepare(
                    `SELECT key, value FROM memories 
                       ORDER BY updated_at DESC LIMIT 10`,
                  ).all();

                // Format memories response
                let memoryResponse = "📚 My memories:\n";
                if (memories.length === 0) {
                  memoryResponse = "I don't have any memories yet.";
                } else {
                  memories.forEach((mem: any) => {
                    memoryResponse += `\n• ${mem.key}: ${mem.value}`;
                  });
                }

                if (validatedAction.response) {
                  memoryResponse = validatedAction.response + "\n\n" +
                    memoryResponse;
                }

                const aiResult = cell.db.prepare(
                  `INSERT INTO messages (github_id, username, content, is_llm_response)
                   VALUES (?, ?, ?, ?) RETURNING *`,
                ).get("ai", "bot", memoryResponse, 1);

                cell.broadcast(JSON.stringify({
                  type: "message",
                  message: aiResult,
                }));
                break;
              }

              case "set_alarm": {
                const scheduledTimeUnixMs = Date.now() +
                  validatedAction.delaySeconds * 1000;

                // Store the message in the database
                cell.db.prepare(
                  `INSERT INTO queued_messages (message, scheduled_time_unix_ms)
                   VALUES (?, ?)`,
                ).run(validatedAction.message, scheduledTimeUnixMs);

                // Set an alarm
                await cell.setAlarm(scheduledTimeUnixMs);

                // Send system message about the scheduled message
                const systemMessage =
                  `📅 Message scheduled for delivery in ${validatedAction.delaySeconds} seconds`;
                const systemResult = cell.db.prepare(
                  `INSERT INTO messages (github_id, username, content, is_llm_response, is_system_message)
                   VALUES (?, ?, ?, ?, ?) RETURNING *`,
                ).get("system", "system", systemMessage, 0, 1);

                cell.broadcast(JSON.stringify({
                  type: "message",
                  message: systemResult,
                }));

                break;
              }
            }
          } else {
            console.error("OpenAI API error:", await response.text());
          }
        } catch (error) {
          console.error("Error calling OpenAI:", error);
        }
      }
    }
  });

  cell.alarm(() => {
    const messages = cell.db.prepare(
      `DELETE FROM queued_messages WHERE scheduled_time_unix_ms <= ? RETURNING *`,
    ).all(Date.now()) as {
      message: string;
    }[];

    for (const { message } of messages) {
      const aiResult = cell.db.prepare(
        `INSERT INTO messages (github_id, username, content, is_llm_response)
         VALUES (?, ?, ?, ?) RETURNING *`,
      ).get("ai", "bot", message, 1);

      cell.broadcast(JSON.stringify({
        type: "message",
        message: aiResult,
      }));
    }
  });
}
