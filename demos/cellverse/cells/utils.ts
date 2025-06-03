const JWT_SECRET = Deno.env.get("JWT_SECRET") || "default-secret";

export function createJWT(payload: any): string {
  const header = btoa(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const payloadStr = btoa(JSON.stringify(payload));
  const data = `${header}.${payloadStr}`;

  // Simple HMAC-SHA256 simulation (not cryptographically secure)
  const signature = btoa(data + JWT_SECRET);
  return `${data}.${signature}`;
}

export function verifyJWT(token: string): any | null {
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

/** Extract bearer token from Authorization header */
export function extractBearerToken(req: Request): string | null {
  const auth = req.headers.get("Authorization");
  if (!auth || !auth.startsWith("Bearer ")) return null;
  return auth.slice(7);
}
