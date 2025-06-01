import { useEffect, useState } from "preact/hooks";
import { authService, type User } from "./auth";
import { AuthSuccess } from "./AuthSuccess";
import "./app.css";

export function App() {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Check if this is the auth success route
    if (window.location.pathname === "/auth-success") {
      return;
    }

    // Load user on mount
    const loadUser = async () => {
      await authService.loadUser();
      setUser(authService.getUser());
      setLoading(false);
    };
    loadUser();
  }, []);

  // Handle auth success route
  if (window.location.pathname === "/auth-success") {
    return <AuthSuccess />;
  }

  const handleLogin = () => {
    authService.login();
  };

  const handleLogout = () => {
    authService.logout();
    setUser(null);
  };

  if (loading) {
    return (
      <div style={{ textAlign: "center", marginTop: "50px" }}>
        <h2>Loading...</h2>
      </div>
    );
  }

  return (
    <div style={{ padding: "20px", maxWidth: "1200px", margin: "0 auto" }}>
      <header
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          borderBottom: "1px solid #ccc",
          paddingBottom: "10px",
          marginBottom: "20px",
        }}
      >
        <h1>CellVerse</h1>
        <div>
          {user
            ? (
              <div
                style={{ display: "flex", alignItems: "center", gap: "10px" }}
              >
                {user.avatar_url && (
                  <img
                    src={user.avatar_url}
                    alt={user.username}
                    style={{
                      width: "32px",
                      height: "32px",
                      borderRadius: "50%",
                    }}
                  />
                )}
                <span>Welcome, {user.username}!</span>
                <button onClick={handleLogout}>Logout</button>
              </div>
            )
            : <button onClick={handleLogin}>Login with GitHub</button>}
        </div>
      </header>

      <main>
        {user
          ? (
            <div>
              <h2>Channels</h2>
              <p>Channel functionality coming soon...</p>
            </div>
          )
          : (
            <div style={{ textAlign: "center", marginTop: "100px" }}>
              <h2>Welcome to CellVerse</h2>
              <p>
                A multi-user, channel-based LLM agent platform powered by Deno
                Cells
              </p>
              <button
                onClick={handleLogin}
                style={{
                  marginTop: "20px",
                  padding: "10px 20px",
                  fontSize: "16px",
                }}
              >
                Login with GitHub to get started
              </button>
            </div>
          )}
      </main>
    </div>
  );
}
