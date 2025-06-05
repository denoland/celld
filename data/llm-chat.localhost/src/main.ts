import { cell } from "../../../sdk/mod.ts";

console.log(`[${cell.id}] Initializing LLM chat server...`);

interface Message {
  role: "user" | "assistant" | "system";
  content: string;
  timestamp: string;
}

// Helper function to create a message payload
function createMessage(
  type: string,
  data: Record<string, unknown>,
) {
  return JSON.stringify({
    type,
    ...data,
    timestamp: new Date().toISOString(),
  });
}

// Initialize database with messages table
function initializeDatabase() {
  cell.db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      role TEXT NOT NULL,
      content TEXT NOT NULL,
      timestamp TEXT NOT NULL
    )
  `);

  // Add a system message if the table is empty
  const count = cell.db.prepare("SELECT COUNT(*) as count FROM messages")
    .get() as { count: number };
  if (count.count === 0) {
    cell.db.prepare(
      "INSERT INTO messages (role, content, timestamp) VALUES (?, ?, ?)",
    ).run("system", "You are a helpful assistant.", new Date().toISOString());
  }
}

// Initialize the database
initializeDatabase();

// Get all messages from the database
function getMessages(): Message[] {
  const rows = cell.db.prepare(
    "SELECT role, content, timestamp FROM messages ORDER BY id ASC",
  ).all();

  return rows.map((row: any) => ({
    role: row.role as "user" | "assistant" | "system",
    content: row.content,
    timestamp: row.timestamp,
  }));
}

// Save a message to the database
function saveMessage(
  role: "user" | "assistant" | "system",
  content: string,
): void {
  cell.db.prepare(
    "INSERT INTO messages (role, content, timestamp) VALUES (?, ?, ?)",
  ).run(role, content, new Date().toISOString());
}

// Clear all messages from the database
function clearMessages(): void {
  cell.db.exec("DELETE FROM messages WHERE role != 'system'");
}

// Get assistant response from OpenAI API
async function getAssistantResponse(
  messages: Message[],
): Promise<string> {
  try {
    const apiKey = Deno.env.get("OPENAI_API_KEY");
    if (!apiKey) {
      console.error("OPENAI_API_KEY environment variable not set");
      return "I'm sorry, I can't process your request right now. The API key is missing.";
    }

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: "gpt-3.5-turbo",
        messages: messages.map((msg) => ({
          role: msg.role,
          content: msg.content,
        })),
        max_tokens: 500,
      }),
    });

    if (!response.ok) {
      const error = await response.text();
      console.error("OpenAI API error:", error);
      return "I'm sorry, I couldn't generate a response. Please try again later.";
    }

    const data = await response.json();
    return data.choices[0].message.content;
  } catch (error) {
    console.error("Error calling OpenAI API:", error);
    return "I'm sorry, an error occurred while processing your request. Please try again later.";
  }
}

// Handle HTTP requests
cell.request((request: Request): Response => {
  const url = new URL(request.url);

  if (url.pathname === "/stats") {
    // Return chat cell stats
    const connectionCount = Array.from(cell.getWebSockets()).length;
    const messageCount = getMessages().length;

    return new Response(
      JSON.stringify({
        cellId: cell.id,
        connections: connectionCount,
        messages: messageCount,
      }),
      {
        headers: {
          "Content-Type": "application/json",
        },
      },
    );
  }

  return new Response("Chat server is running");
});

// Handle new connections
cell.connect((socket: WebSocket, id: string) => {
  try {
    // Get all messages from the database
    const messages = getMessages();

    // Filter out system messages for the client
    const clientMessages = messages.filter((msg) => msg.role !== "system");

    // Send history to the new connection
    socket.send(
      createMessage("history", {
        messages: clientMessages,
      }),
    );
  } catch (error) {
    console.error("Error in connect:", error);
    socket.send(
      createMessage("error", {
        message: "Failed to load chat history. Please try refreshing the page.",
      }),
    );
  }
});

// Handle message reception
cell.message(async (event: MessageEvent, socket: WebSocket, id: string) => {
  try {
    const message = JSON.parse(event.data.toString());

    switch (message.type) {
      case "message":
        // Save user message to database
        saveMessage("user", message.content);

        // Get all messages for context
        const allMessages = getMessages();

        // Get response from OpenAI
        const responseContent = await getAssistantResponse(allMessages);

        // Save assistant response to database
        saveMessage("assistant", responseContent);

        // Send response to all clients
        cell.broadcast(
          createMessage("message", {
            role: "assistant",
            content: responseContent,
          }),
        );
        break;

      case "clear":
        // Clear messages from database
        clearMessages();
        break;

      default:
        socket.send(
          createMessage("error", {
            message: "Unknown message type",
          }),
        );
    }
  } catch (error) {
    console.error("Error processing message:", error);
    socket.send(
      createMessage("error", {
        message: "Error processing your message",
      }),
    );
  }
});

// Handle connection closures
cell.close((socket: WebSocket, id: string) => {
  console.log("Connection closed", { connectionId: id });
});

// Handle WebSocket errors
cell.error((error: Error | ErrorEvent | Event) => {
  console.error("WebSocket error:", error);
});
