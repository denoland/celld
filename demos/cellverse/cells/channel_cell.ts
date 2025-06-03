import { cell } from "jsr:@ry/cells";
import { z } from "npm:zod";
import { extractBearerToken, verifyJWT } from "./utils.ts";

const FRONTEND_URL = Deno.env.get("FRONTEND_URL");
const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY");

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

// Only initialize if this is a channel cell
if (cell.id.startsWith("channel-")) {
  // Initialize DB schema for channels
  cell.db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      github_id TEXT NOT NULL,
      username TEXT NOT NULL,
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      content TEXT NOT NULL,
      is_llm_response INTEGER DEFAULT 0
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

  // Request handlers
  cell.request(async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const path = url.pathname.replace(`/cell/${cell.id}`, "");

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

    return new Response("Not found", { status: 404 });
  });

  // WebSocket handling for channels
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

  cell.close((_socket: WebSocket, id: string) => {
    authenticatedSockets.delete(id);
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
