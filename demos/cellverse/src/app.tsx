import { useEffect, useState } from "preact/hooks";
import { authService, type User } from "./auth";
import { AuthSuccess } from "./AuthSuccess";
import { Channels } from "./Channels";
import { Chat } from "./Chat";
import { Memory } from "./Memory";
import "./app.css";

export function App() {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedChannel, setSelectedChannel] = useState<
    { id: string; name: string } | null
  >(null);
  const [viewMode, setViewMode] = useState<"chat" | "memory">("chat");

  // Parse channel from URL on mount
  useEffect(() => {
    const path = window.location.pathname;
    if (path.startsWith("/c/")) {
      const pathParts = path.substring(3).split("/");
      const channelSlug = decodeURIComponent(pathParts[0]);

      if (channelSlug) {
        setSelectedChannel({
          id: `channel-${channelSlug}`,
          name: channelSlug,
        });

        // Check if it's a memory route
        if (pathParts[1] === "mem") {
          setViewMode("memory");
        } else {
          setViewMode("chat");
        }
      }
    }
  }, []);

  // Handle channel selection with URL update
  const handleSelectChannel = (
    channel: { id: string; name: string } | null,
    mode: "chat" | "memory" = "chat",
  ) => {
    setSelectedChannel(channel);
    setViewMode(mode);
    if (channel) {
      const channelSlug = channel.id.replace("channel-", "");
      const url = mode === "memory"
        ? `/c/${channelSlug}/mem`
        : `/c/${channelSlug}`;
      window.history.pushState({}, "", url);
    } else {
      window.history.pushState({}, "", "/");
    }
  };

  // Handle browser back/forward buttons
  useEffect(() => {
    const handlePopState = () => {
      const path = window.location.pathname;
      if (path.startsWith("/c/")) {
        const pathParts = path.substring(3).split("/");
        const channelSlug = decodeURIComponent(pathParts[0]);

        if (channelSlug) {
          setSelectedChannel({
            id: `channel-${channelSlug}`,
            name: channelSlug,
          });

          // Check if it's a memory route
          if (pathParts[1] === "mem") {
            setViewMode("memory");
          } else {
            setViewMode("chat");
          }
        }
      } else {
        setSelectedChannel(null);
        setViewMode("chat");
      }
    };

    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

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
      <div className="app-container">
        <div className="loading-container">
          <div className="loading-spinner"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="app-container">
      <header className="header">
        <h1>CellVerse</h1>
        <div>
          {user
            ? (
              <div className="user-info">
                {user.avatar_url && (
                  <img
                    src={user.avatar_url}
                    alt={user.username}
                    className="user-avatar"
                  />
                )}
                <span className="user-name">Welcome, {user.username}!</span>
                <button className="logout-btn" onClick={handleLogout}>
                  Logout
                </button>
              </div>
            )
            : null}
        </div>
      </header>

      {user
        ? (
          <main className="main-content">
            {selectedChannel
              ? (
                viewMode === "memory"
                  ? (
                    <Memory
                      channelId={selectedChannel.id}
                      channelName={selectedChannel.name}
                      onClose={() => handleSelectChannel(null)}
                    />
                  )
                  : (
                    <Chat
                      channelId={selectedChannel.id}
                      channelName={selectedChannel.name}
                      onClose={() => handleSelectChannel(null)}
                      onSwitchToMemory={() =>
                        handleSelectChannel(selectedChannel, "memory")}
                    />
                  )
              )
              : (
                <Channels
                  selectedChannel={selectedChannel}
                  onSelectChannel={handleSelectChannel}
                />
              )}
          </main>
        )
        : (
          <div className="login-container">
            <div className="login-card">
              <h2>Welcome to CellVerse</h2>
              <p>
                A multi-user, channel-based LLM agent platform powered by Deno
                Cells. Connect with AI agents in real-time channels.
              </p>
              <button
                onClick={handleLogin}
                className="github-login-btn"
              >
                <svg viewBox="0 0 16 16" fill="currentColor">
                  <path
                    fillRule="evenodd"
                    d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"
                  />
                </svg>
                Login with GitHub to get started
              </button>
            </div>
          </div>
        )}
    </div>
  );
}
