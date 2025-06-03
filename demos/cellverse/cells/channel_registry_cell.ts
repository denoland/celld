import { cell } from "jsr:@ry/cells";
import { extractBearerToken, verifyJWT } from "./utils.ts";

const FRONTEND_URL = Deno.env.get("FRONTEND_URL");
const OPENAI_API_KEY = Deno.env.get("OPENAI_API_KEY");

if (!FRONTEND_URL || FRONTEND_URL.at(-1) === "/") {
  throw new Error(`bad FRONTEND_URL env var: '${FRONTEND_URL}'`);
}

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

  cell.request(async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const path = url.pathname.replace(`/cell/${cell.id}`, "");

    // All channel registry endpoints require authentication
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

    return new Response("Not found", { status: 404 });
  });
}
