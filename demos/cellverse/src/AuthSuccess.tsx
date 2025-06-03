import { useEffect } from "preact/hooks";
import { authService } from "./auth";

export function AuthSuccess() {
  useEffect(() => {
    // Extract token from URL fragment
    const hash = window.location.hash;
    const tokenMatch = hash.match(/token=([^&]+)/);

    if (tokenMatch) {
      const token = tokenMatch[1];
      authService.handleAuthCallback(token);
      // Redirect to home page
      window.location.href = "/";
    } else {
      // No token found, redirect to home
      window.location.href = "/";
    }
  }, []);

  return (
    <div style={{ textAlign: "center", marginTop: "50px" }}>
      <h2>Authenticating...</h2>
      <p>Please wait while we complete the login process.</p>
    </div>
  );
}
