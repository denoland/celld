// Import the Connection and Room types from our bootstrap.ts
import { Connection, Room } from "../../../src/bootstrap.ts";

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
async function initializeDatabase(roomId: string) {
  const db = await Deno.openKv("sqlite://sqlite/" + roomId + ".db");
  
  try {
    await db.execute(`
      CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role TEXT NOT NULL,
        content TEXT NOT NULL,
        timestamp TEXT NOT NULL
      )
    `);
    
    // Add a system message if the table is empty
    const count = await db.query("SELECT COUNT(*) as count FROM messages");
    if (count.rows[0].count === 0) {
      await db.execute(
        "INSERT INTO messages (role, content, timestamp) VALUES (?, ?, ?)",
        ["system", "You are a helpful assistant.", new Date().toISOString()]
      );
    }
  } finally {
    await db.close();
  }
}

// Get all messages from the database
async function getMessages(roomId: string): Promise<Message[]> {
  const db = await Deno.openKv("sqlite://sqlite/" + roomId + ".db");
  try {
    const result = await db.query(
      "SELECT role, content, timestamp FROM messages ORDER BY id ASC"
    );
    return result.rows.map((row) => ({
      role: row.role,
      content: row.content,
      timestamp: row.timestamp,
    }));
  } finally {
    await db.close();
  }
}

// Save a message to the database
async function saveMessage(
  roomId: string, 
  role: "user" | "assistant" | "system", 
  content: string
): Promise<void> {
  const db = await Deno.openKv("sqlite://sqlite/" + roomId + ".db");
  try {
    await db.execute(
      "INSERT INTO messages (role, content, timestamp) VALUES (?, ?, ?)",
      [role, content, new Date().toISOString()]
    );
  } finally {
    await db.close();
  }
}

// Clear all messages from the database
async function clearMessages(roomId: string): Promise<void> {
  const db = await Deno.openKv("sqlite://sqlite/" + roomId + ".db");
  try {
    await db.execute("DELETE FROM messages WHERE role != 'system'");
  } finally {
    await db.close();
  }
}

// Get assistant response from OpenAI API
async function getAssistantResponse(
  roomId: string,
  messages: Message[]
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
        messages: messages.map(msg => ({
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

export default {
  // Called when the server starts, before accepting connections
  async onStart(ctx: { roomId: string; room: Room }) {
    console.log("Chat server started for room", { roomId: ctx.roomId });
    await initializeDatabase(ctx.roomId);
  },

  // Called when a new WebSocket connection is established
  async onConnect(connection: Connection, ctx: { roomId: string; room: Room }) {
    try {
      // Get all messages from the database
      const messages = await getMessages(ctx.roomId);
      
      // Filter out system messages for the client
      const clientMessages = messages.filter(msg => msg.role !== "system");
      
      // Send history to the new connection
      connection.send(
        createMessage("history", {
          messages: clientMessages,
        })
      );
    } catch (error) {
      console.error("Error in onConnect:", error);
      connection.send(
        createMessage("error", {
          message: "Failed to load chat history. Please try refreshing the page.",
        })
      );
    }
  },

  // Called when a WebSocket message is received
  async onMessage(
    data: string,
    sender: Connection,
    ctx: { roomId: string; room: Room },
  ) {
    try {
      const message = JSON.parse(data);
      
      switch (message.type) {
        case "message":
          // Save user message to database
          await saveMessage(ctx.roomId, "user", message.content);
          
          // Get all messages for context
          const allMessages = await getMessages(ctx.roomId);
          
          // Get response from OpenAI
          const responseContent = await getAssistantResponse(ctx.roomId, allMessages);
          
          // Save assistant response to database
          await saveMessage(ctx.roomId, "assistant", responseContent);
          
          // Send response to all clients
          ctx.room.broadcast(
            createMessage("message", {
              role: "assistant",
              content: responseContent,
            })
          );
          break;
          
        case "clear":
          // Clear messages from database
          await clearMessages(ctx.roomId);
          break;
          
        default:
          sender.send(
            createMessage("error", {
              message: "Unknown message type",
            })
          );
      }
    } catch (error) {
      console.error("Error processing message:", error);
      sender.send(
        createMessage("error", {
          message: "Error processing your message",
        })
      );
    }
  },

  // Called when a WebSocket connection is closed
  async onClose(connection: Connection, ctx: { roomId: string; room: Room }) {
    console.log("Connection closed", { connectionId: connection.id });
  },

  // Called when a WebSocket error occurs
  async onError(
    connection: Connection,
    error: Event,
    ctx: { roomId: string; room: Room },
  ) {
    console.error("WebSocket error:", error);
  },

  // Called for HTTP requests
  async onRequest(request: Request, ctx: { roomId: string; room: Room }) {
    const url = new URL(request.url);
    
    if (url.pathname === "/stats") {
      // Return chat room stats
      const connectionCount = ctx.room.connections.size;
      const messageCount = (await getMessages(ctx.roomId)).length;
      
      return new Response(
        JSON.stringify({
          roomId: ctx.roomId,
          connections: connectionCount,
          messages: messageCount,
        }),
        {
          headers: {
            "Content-Type": "application/json",
          },
        }
      );
    }
    
    return new Response("Chat server is running");
  },
};