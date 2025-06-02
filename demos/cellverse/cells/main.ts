import { cell } from "jsr:@ry/cells";

const GITHUB_CLIENT_ID = Deno.env.get("GITHUB_CLIENT_ID") ||
  "Ov23liRJiAlpktDPnJx1";
const GITHUB_CLIENT_SECRET = Deno.env.get("GITHUB_CLIENT_SECRET") ||
  "ad8ddca4d41ccf13b205205d8e835e33c5a5b493";
const JWT_SECRET = Deno.env.get("JWT_SECRET") || "dev-secret-key";
const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY");

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
      is_llm_response INTEGER DEFAULT 0
    )
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
      const redirectUri = `http://localhost:5173/cell/auth/github/callback`;
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
        const frontendUrl = "http://localhost:5173";
        return Response.redirect(
          `${frontendUrl}/auth-success#token=${jwt}`,
        );
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

      cell.db.prepare(
        `INSERT INTO channels (id, name, creator_github_id) VALUES (?, ?, ?)`,
      ).run(channelId, body.name, user.github_id);

      return new Response(
        JSON.stringify({
          id: channelId,
          name: body.name,
          creator_github_id: user.github_id,
        }),
        {
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    // List channels
    if (path === "/list" && req.method === "GET") {
      const channels = cell.db.prepare(
        `SELECT id, name, creator_github_id, created_at 
         FROM channels ORDER BY created_at DESC`,
      ).all();

      return new Response(JSON.stringify(channels), {
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
        return new Response("Only the channel owner can delete it", { status: 403 });
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
          // Get recent message history for context
          const recentMessages = cell.db.prepare(
            `SELECT username, content, is_llm_response 
             FROM messages 
             ORDER BY timestamp DESC 
             LIMIT 10`,
          ).all().reverse();

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

          // Call OpenAI API
          const response = await fetch(
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
                    content: `You are a helpful assistant in the "${
                      cell.id.replace("channel-", "")
                    }" channel. Be conversational and friendly. Keep responses concise.`,
                  },
                  ...messages,
                ],
                temperature: 0.8,
                max_tokens: 150,
              }),
            },
          );

          if (response.ok) {
            const aiData = await response.json();
            const aiContent = aiData.choices[0].message.content;

            // Store AI response
            const aiResult = cell.db.prepare(
              `INSERT INTO messages (github_id, username, content, is_llm_response)
               VALUES (?, ?, ?, ?) RETURNING *`,
            ).get("ai", "AI Assistant", aiContent, 1);

            // Broadcast AI response
            cell.broadcast(JSON.stringify({
              type: "message",
              message: aiResult,
            }));
          } else {
            console.error("OpenAI API error:", await response.text());
          }
        } catch (error) {
          console.error("Error calling OpenAI:", error);
        }
      }
    }
  });
}
