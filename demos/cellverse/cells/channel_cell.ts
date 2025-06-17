import { cell } from "@ry/cells";
import { z } from "npm:zod";
import { extractBearerToken, verifyJWT } from "./utils.ts";

const FRONTEND_URL = Deno.env.get("FRONTEND_URL");
const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY");

// Helper functions for common operations
function broadcastMessage(cell: any, message: any) {
  cell.broadcast(JSON.stringify({
    type: "message",
    message,
  }));
}

// Multi-stage reasoning workflow with tool invocation
const ReasoningStepActionSchema = z.discriminatedUnion("action", [
  z.object({
    action: z.literal("store_memory"),
    key: z.string(),
    value: z.string(),
  }),
  z.object({
    action: z.literal("read_memories"),
    filter: z.string().optional(),
  }),
  z.object({
    action: z.literal("set_alarm"),
    message: z.string(),
    delaySeconds: z.number(),
  }),
  z.object({
    action: z.literal("think"),
    thought: z.string(),
  }),
  z.object({
    action: z.literal("respond"),
    message: z.string(),
  }),
]);

type ReasoningStepAction = z.infer<typeof ReasoningStepActionSchema>;

interface ReasoningContext {
  originalMessages: { role: "user" | "assistant"; content: string }[];
  stepHistory: Array<{
    step: number;
    thought?: string;
    toolsUsed: Array<{
      tool: string;
      input: any;
      output: any;
    }>;
  }>;
  memories: Array<{ key: string; value: string }>;
}

// Step execution logic
async function executeReasoningStep(
  stepResult: ReasoningStepAction,
  stepIndex: number,
  context: ReasoningContext,
  cell: any,
) {
  const currentStep = {
    step: stepIndex + 1,
    thought: undefined as string | undefined,
    toolsUsed: [] as Array<{ tool: string; input: any; output: any }>,
  };

  switch (stepResult.action) {
    case "store_memory": {
      storeMemory(cell.db, stepResult.key, stepResult.value);

      currentStep.toolsUsed.push({
        tool: "store_memory",
        input: { key: stepResult.key, value: stepResult.value },
        output: "stored",
      });

      const systemResult = insertMessage(
        cell.db,
        null,
        "system",
        `Stored memory: ${stepResult.key}`,
        "system",
        "store_memory",
      );

      broadcastMessage(cell, systemResult);

      context.stepHistory.push(currentStep);
      return { finished: false };
    }

    case "read_memories": {
      const memories = getMemories(cell.db, stepResult.filter);

      const memoryData = memories.map((mem: any) => ({
        key: mem.key,
        value: mem.value,
      }));

      context.memories = memoryData;

      currentStep.toolsUsed.push({
        tool: "read_memories",
        input: { filter: stepResult.filter },
        output: `Found ${memories.length} memories`,
      });

      const systemResult = insertMessage(
        cell.db,
        null,
        "system",
        `Retrieved ${memories.length} memories`,
        "system",
        "read_memories",
      );

      broadcastMessage(cell, systemResult);

      context.stepHistory.push(currentStep);
      return { finished: false };
    }

    case "set_alarm": {
      // Store the alarm details in context for the reasoning workflow to handle
      currentStep.toolsUsed.push({
        tool: "set_alarm",
        input: {
          message: stepResult.message,
          delaySeconds: stepResult.delaySeconds,
        },
        output: "alarm_scheduled",
      });

      const systemResult = insertMessage(
        cell.db,
        null,
        "system",
        `Message scheduled for ${stepResult.delaySeconds} seconds`,
        "system",
        "set_alarm",
      );

      broadcastMessage(cell, systemResult);

      context.stepHistory.push(currentStep);

      // Return the alarm details so the reasoning workflow can handle the sleep
      return {
        finished: false,
        alarm: {
          message: stepResult.message,
          delaySeconds: stepResult.delaySeconds,
        },
      };
    }

    case "think": {
      currentStep.thought = stepResult.thought;

      const thinkingResult = insertMessage(
        cell.db,
        null,
        "bot",
        stepResult.thought,
        "bot",
        "thinking",
      );

      broadcastMessage(cell, thinkingResult);

      context.stepHistory.push(currentStep);
      return { finished: false };
    }

    case "respond": {
      const aiResult = insertMessage(
        cell.db,
        null,
        "bot",
        stepResult.message,
        "bot",
        "respond",
      );

      broadcastMessage(cell, aiResult);

      return { finished: true };
    }

    default: {
      throw new Error(`Unknown action: ${stepResult satisfies never}`);
    }
  }
}

// Authentication helper functions
async function verifyTokenAndOwnership(token: string, channelId: string) {
  if (!token) {
    return { error: "Unauthorized", status: 401 };
  }

  const user = verifyJWT(token);
  if (!user || user.exp < Date.now()) {
    return { error: "Invalid or expired token", status: 401 };
  }

  try {
    const registryResponse = await fetch(
      `${FRONTEND_URL}/cell/registry/get/${channelId}`,
      {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      },
    );

    if (!registryResponse.ok) {
      return { error: "Failed to get channel info", status: 500 };
    }

    const channelData = await registryResponse.json();

    if (channelData.creator_github_id !== user.github_id) {
      return {
        error: "Only the channel owner can perform this action",
        status: 403,
      };
    }

    return { user, channelData };
  } catch (error) {
    console.error("Error verifying ownership:", error);
    return { error: "Internal server error", status: 500 };
  }
}

function authenticateWebSocketUser(token: string) {
  const user = verifyJWT(token);
  if (!user || user.exp < Date.now()) {
    return null;
  }
  return user;
}

// System prompt construction
function buildReasoningSystemPrompt(
  channelId: string,
  allowedSteps: number,
  db: any,
) {
  let p = `You are a bot in the "${
    channelId.replace("channel-", "")
  }" channel.`;

  const personalityConfig = getChannelConfig(db, "personality");

  if (personalityConfig && personalityConfig.value) {
    p = personalityConfig.value as string;
  }

  p += `\n\nYou are solving a complex problem through multi-stage reasoning.
At each step, you can use different tools to gather information or store insights.
You will have ${allowedSteps} steps to solve the problem.

Available tools at each step:
1. store_memory: Store important insights or facts for later use
2. read_memories: Retrieve previously stored memories
3. set_alarm: Schedule a message to be sent after a delay
4. think: Continue reasoning without using tools
5. respond: Provide your final answer (ends the reasoning)

You must respond with a JSON object matching one of these schemas:
{"action": "store_memory", "key": "memory_key", "value": "memory_value"}
{"action": "read_memories", "filter": "optional_search_term"}
{"action": "set_alarm", "message": "delayed message", "delaySeconds": 60}
{"action": "think", "thought": "your reasoning here"}
{"action": "respond", "message": "your final answer"}

Use tools strategically to gather context and build understanding before responding.
Store important information about users, topics, or insights that might be useful later.
Use alarms for reminders or delayed responses when appropriate.`;

  return p;
}

// Database helper functions
function insertMessage(
  db: any,
  githubId: string | null,
  username: string,
  content: string,
  messageType: "user" | "bot" | "system" = "user",
  messageCategory?: string,
) {
  try {
    console.log(
      `Inserting message from ${username} (${messageType}/${messageCategory}): ${
        content.substring(0, 50)
      }...`,
    );

    // First try with RETURNING (SQLite 3.35+)
    try {
      return db.prepare(
        `INSERT INTO messages (github_id, username, content, message_type, message_category)
         VALUES (?, ?, ?, ?, ?) RETURNING *`,
      ).get(githubId, username, content, messageType, messageCategory || null);
    } catch (returnError) {
      console.log(
        `RETURNING syntax failed, trying fallback approach:`,
        returnError,
      );

      // Fallback for older SQLite versions
      const insertStmt = db.prepare(
        `INSERT INTO messages (github_id, username, content, message_type, message_category)
         VALUES (?, ?, ?, ?, ?)`,
      );
      const result = insertStmt.run(
        githubId,
        username,
        content,
        messageType,
        messageCategory || null,
      );

      // Get the inserted row
      const selectStmt = db.prepare(`SELECT * FROM messages WHERE id = ?`);
      return selectStmt.get(result.lastInsertRowid);
    }
  } catch (error) {
    console.error(`Error inserting message from ${username}:`, error);
    console.error(
      `Database object:`,
      typeof db,
      db ? "exists" : "null/undefined",
    );
    throw error;
  }
}

function getRecentMessages(db: any, limit = 50) {
  return db.prepare(
    `SELECT * FROM messages ORDER BY id DESC LIMIT ?`,
  ).all(limit).reverse();
}

function getRecentMessagesInTimeframe(db: any, since: string, limit = 100) {
  return db.prepare(
    `SELECT username, content, message_type, timestamp, message_category
     FROM messages
     WHERE timestamp >= ?
     ORDER BY id DESC
     LIMIT ?`,
  ).all(since, limit).reverse() as {
    username: string;
    content: string;
    message_type: "user" | "bot" | "system";
    timestamp: string;
    message_category?: string;
  }[];
}

function storeMemory(db: any, key: string, value: string) {
  try {
    return db.prepare(
      `INSERT OR REPLACE INTO memories (key, value, updated_at)
       VALUES (?, ?, CURRENT_TIMESTAMP)`,
    ).run(key, value);
  } catch (error) {
    console.error(`Error storing memory "${key}":`, error);
    throw error;
  }
}

function getMemories(db: any, filter?: string, limit = 10) {
  try {
    if (filter) {
      return db.prepare(
        `SELECT key, value FROM memories
         WHERE key LIKE ? OR value LIKE ?
         ORDER BY updated_at DESC`,
      ).all(`%${filter}%`, `%${filter}%`);
    }
    return db.prepare(
      `SELECT key, value FROM memories
       ORDER BY updated_at DESC LIMIT ?`,
    ).all(limit);
  } catch (error) {
    console.error(`Error getting memories with filter "${filter}":`, error);
    return [];
  }
}

function getAllMemories(db: any) {
  return db.prepare(
    `SELECT id, key, value, created_at, updated_at
     FROM memories
     ORDER BY updated_at DESC`,
  ).all();
}

function deleteMemory(db: any, memoryId: string) {
  return db.prepare(`DELETE FROM memories WHERE id = ?`).run(memoryId);
}

function getChannelConfig(db: any, key: string) {
  try {
    console.log(`Getting channel config for key: ${key}`);
    const stmt = db.prepare(`SELECT value FROM channel_config WHERE key = ?`);
    const result = stmt.get(key);
    console.log(`Channel config result for ${key}:`, result);
    return result;
  } catch (error) {
    console.error(`Error getting channel config for key "${key}":`, error);
    console.error(`Error details:`, String(error));
    return null;
  }
}

function setChannelConfig(db: any, key: string, value: string) {
  return db.prepare(
    `INSERT OR REPLACE INTO channel_config (key, value) VALUES (?, ?)`,
  ).run(key, value);
}

// Multi-stage reasoning now handles all bot interactions

if (cell.id.startsWith("channel-")) {
  // Initialize DB schema for channels
  cell.db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      github_id TEXT,
      username TEXT NOT NULL,
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      content TEXT NOT NULL,
      message_type TEXT NOT NULL DEFAULT 'user' CHECK (message_type IN ('user', 'bot', 'system')),
      message_category TEXT
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

  // Delayed messages are now handled through workflows with step.sleep

  const reasoningWorkflow = cell.workflow.define<{
    messages: {
      role: "user" | "assistant";
      content: string;
    }[];
    allowedSteps: number;
  }, void>({
    name: "reasoning",
    handler: async ({ input, step }) => {
      const context: ReasoningContext = {
        originalMessages: input.messages,
        stepHistory: [],
        memories: [],
      };

      const systemPrompt = await step.run("construct-system-prompt", () => {
        console.log(`Building system prompt for channel ${cell.id}`);
        return buildReasoningSystemPrompt(cell.id, input.allowedSteps, cell.db);
      });

      for (let i = 0; i < input.allowedSteps; i++) {
        const stepResult = await step.run(`reasoning-step-${i}`, async () => {
          // Build context message with step history
          let contextMessage = "Previous reasoning steps:\n";
          context.stepHistory.forEach((stepInfo, idx) => {
            contextMessage += `\nStep ${idx + 1}:`;
            if (stepInfo.thought) {
              contextMessage += `\n  Thought: ${stepInfo.thought}`;
            }
            stepInfo.toolsUsed.forEach((tool) => {
              contextMessage += `\n  Used ${tool.tool}: ${
                JSON.stringify(tool.input)
              } → ${JSON.stringify(tool.output)}`;
            });
          });

          if (context.memories.length > 0) {
            contextMessage += "\n\nAvailable memories:";
            context.memories.forEach((mem) => {
              contextMessage += `\n  ${mem.key}: ${mem.value}`;
            });
          }

          const isLastStep = i === input.allowedSteps - 1;
          const stepPrompt = isLastStep
            ? "This is your last step. You must provide your final response."
            : `Step ${
              i + 1
            } of ${input.allowedSteps}. What tool would you like to use or what would you like to think about?`;

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
                  { role: "system", content: systemPrompt },
                  ...input.messages,
                  { role: "user", content: contextMessage },
                  { role: "user", content: stepPrompt },
                ],
                response_format: { type: "json_object" },
              }),
            },
          );

          if (!response.ok) {
            throw new Error(`OpenAI API error: ${await response.text()}`);
          }

          const aiData = await response.json();
          const aiResponse = aiData.choices[0].message.content;
          const parsedResponse = JSON.parse(aiResponse);
          const validatedAction = ReasoningStepActionSchema.parse(
            parsedResponse,
          );

          return validatedAction;
        });

        // Execute the step action and track results
        const executionResult = await step.run(
          `execute-step-${i}`,
          async () => {
            return await executeReasoningStep(
              stepResult,
              i,
              context,
              cell,
            );
          },
        );

        // Handle alarm case with step.sleep in this workflow
        if (executionResult && executionResult.alarm) {
          console.log(`[DEBUG] ReasoningWorkflow alarm detected:`, {
            alarm: executionResult.alarm,
            delaySeconds: executionResult.alarm.delaySeconds,
            delayMs: executionResult.alarm.delaySeconds * 1000,
            message: executionResult.alarm.message,
            timestamp: new Date().toISOString(),
          });

          console.log(
            `[ReasoningWorkflow] About to sleep for ${executionResult.alarm.delaySeconds} seconds`,
          );

          console.log(`[DEBUG] Calling step.sleep for alarm:`, {
            name: "alarm-delay",
            durationMs: executionResult.alarm.delaySeconds * 1000,
            delaySeconds: executionResult.alarm.delaySeconds,
            timestamp: new Date().toISOString(),
          });

          try {
            await step.sleep(
              "alarm-delay",
              executionResult.alarm.delaySeconds * 1000,
            );
            console.log(
              `[ReasoningWorkflow] Woke up from alarm sleep, sending delayed message`,
            );

            console.log(`[DEBUG] Successfully woke up from alarm sleep:`, {
              message: executionResult.alarm.message,
              timestamp: new Date().toISOString(),
            });

            // Send the delayed message
            const delayedResult = insertMessage(
              cell.db,
              null,
              "bot",
              executionResult.alarm.message,
              "bot",
              "respond",
            );

            broadcastMessage(cell, delayedResult);
            console.log(
              `[ReasoningWorkflow] Sent delayed message: "${executionResult.alarm.message}"`,
            );

            // Continue the reasoning workflow - don't break here
          } catch (error) {
            console.log(`[DEBUG] Error during step.sleep:`, {
              error: error.message,
              stack: error.stack,
              timestamp: new Date().toISOString(),
            });
            throw error;
          }
        }

        if (executionResult.finished) {
          break;
        }
      }
    },
  });

  // Request handlers
  cell.request(async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const path = url.pathname.replace(`/cell/${cell.id}`, "");

    // Set personality endpoint (called by channel registry after creation)
    if (path === "/set-personality" && req.method === "POST") {
      const body = await req.json();
      if (body.personality) {
        setChannelConfig(cell.db, "personality", body.personality);
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
          `${FRONTEND_URL}/cell/registry/get/${cell.id}`,
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
        const memories = getAllMemories(cell.db);

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
          `${FRONTEND_URL}/cell/registry/get/${cell.id}`,
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
        const result = deleteMemory(cell.db, memoryId);

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
      const user = authenticateWebSocketUser(data.token);
      if (!user) {
        socket.send(JSON.stringify({
          type: "error",
          message: "Invalid token",
        }));
        socket.close();
        return;
      }

      authenticatedSockets.set(id, user);

      // Send recent message history
      const messages = getRecentMessages(cell.db, 50);

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
      const result = insertMessage(
        cell.db,
        user.github_id,
        user.username,
        data.content,
        "user",
      );

      // Broadcast to all connected users
      broadcastMessage(cell, result);

      try {
        // Get 1 hour of message history for context
        const oneHourAgo = new Date(
          Date.now() - 60 * 60 * 1000,
        ).toISOString();
        const recentMessages = getRecentMessagesInTimeframe(
          cell.db,
          oneHourAgo,
          100,
        );

        console.log(
          `Found ${recentMessages.length} recent messages in timeframe`,
        );
        console.log(
          "Recent messages sample:",
          recentMessages.slice(-5).map((m) => ({
            type: m.message_type,
            category: m.message_category,
            content: m.content.substring(0, 30),
          })),
        );

        // Build conversation history - exclude thinking, tool usage, and system status messages
        const messages = recentMessages.filter((msg) => {
          // Include user messages and bot responses
          if (msg.message_type === "user") return true;
          if (
            msg.message_type === "bot" && msg.message_category === "respond"
          ) return true;
          // Exclude everything else (thinking, tool usage, system messages)
          return false;
        })
          .map((msg) => ({
            role: msg.message_type === "bot" ? "assistant" : "user",
            content: msg.message_type === "bot"
              ? msg.content
              : `${msg.username}: ${msg.content}`,
          } as const));

        console.log(`Filtered to ${messages.length} messages for AI context`);
        console.log(
          "Messages being sent to AI:",
          messages.map((m) => ({
            role: m.role,
            content: m.content.substring(0, 50),
          })),
        );

        // Add the current message
        messages.push({
          role: "user",
          content: `${user.username}: ${data.content}`,
        });

        // Always start with multi-stage reasoning workflow
        // Send system message to let users know the bot is processing
        const systemMessage = `Bot is thinking...`;
        const systemResult = insertMessage(
          cell.db,
          null,
          "system",
          systemMessage,
          "system",
          "system_status",
        );

        broadcastMessage(cell, systemResult);

        // Start reasoning workflow for every message
        cell.workflow.dispatch(reasoningWorkflow, {
          messages,
          allowedSteps: 3,
        });
      } catch (error) {
        console.error("Error calling OpenAI:", error);
      }
    }
  });

  cell.close((_socket: WebSocket, id: string) => {
    authenticatedSockets.delete(id);
  });

  // Alarm handling is now done through workflows with step.sleep
}
