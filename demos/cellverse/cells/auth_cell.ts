import { cell } from "jsr:@ry/cells";
import { createJWT, extractBearerToken, verifyJWT } from "./utils.ts";

const FRONTEND_URL = Deno.env.get("FRONTEND_URL");
const GITHUB_CLIENT_ID = Deno.env.get("GITHUB_CLIENT_ID");
const GITHUB_CLIENT_SECRET = Deno.env.get("GITHUB_CLIENT_SECRET");

if (!FRONTEND_URL || FRONTEND_URL.at(-1) === "/") {
  throw new Error(`bad FRONTEND_URL env var: '${FRONTEND_URL}'`);
}

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

  cell.request(async (req: Request): Promise<Response> => {
    const url = new URL(req.url);
    const path = url.pathname.replace(`/cell/${cell.id}`, "");

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

    return new Response("Not found", { status: 404 });
  });
}
