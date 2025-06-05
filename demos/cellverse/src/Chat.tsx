import { useEffect, useRef, useState } from "preact/hooks";
import { authService } from "./auth";
import { type Channel } from "./channelService";

interface Message {
  id: number;
  github_id: string;
  username: string;
  timestamp: string;
  content: string;
  is_llm_response: boolean;
  is_system_message: boolean;
}

interface ChatProps {
  channelId: string;
  channelName: string;
  onClose: () => void;
  onSwitchToMemory?: () => void;
}

export function Chat(
  { channelId, channelName, onClose, onSwitchToMemory }: ChatProps,
) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState("");
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(true);
  const [channel, setChannel] = useState<Channel | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const user = authService.getUser();

  useEffect(() => {
    // Fetch channel info
    const fetchChannelInfo = async () => {
      try {
        const token = localStorage.getItem("auth_token");
        if (!token) return;

        const response = await fetch(
          `/cell/registry/get/${channelId}`,
          {
            headers: {
              Authorization: `Bearer ${token}`,
            },
          },
        );

        if (response.ok) {
          const channelData = await response.json();
          setChannel(channelData);
        }
      } catch (error) {
        console.error("Error fetching channel info:", error);
      }
    };

    fetchChannelInfo();

    // Connect to WebSocket
    const connectWebSocket = () => {
      const protocol = location.protocol === "https:" ? "wss:" : "ws:";
      const wsUrl = `${protocol}//${location.host}/cell/${channelId}`;
      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        console.log("WebSocket connected");
        // Send authentication
        const token = localStorage.getItem("auth_token");
        ws.send(JSON.stringify({
          type: "auth",
          token: token,
        }));
      };

      ws.onmessage = (event) => {
        const data = JSON.parse(event.data);

        switch (data.type) {
          case "auth_success":
            setConnected(true);
            setConnecting(false);
            break;

          case "history":
            setMessages(data.messages);
            break;

          case "message":
            setMessages((prev) => [...prev, data.message]);
            break;

          case "error":
            console.error("WebSocket error:", data.message);
            break;
        }
      };

      ws.onclose = () => {
        console.log("WebSocket disconnected");
        setConnected(false);
        wsRef.current = null;
      };

      ws.onerror = (error) => {
        console.error("WebSocket error:", error);
        setConnecting(false);
      };
    };

    connectWebSocket();

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [channelId]);

  useEffect(() => {
    // Scroll to bottom when new messages arrive
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const sendMessage = (e: Event) => {
    e.preventDefault();
    if (!inputValue.trim() || !connected || !wsRef.current) return;

    wsRef.current.send(JSON.stringify({
      type: "message",
      content: inputValue.trim(),
    }));

    setInputValue("");
  };

  return (
    <div className="chat-container">
      <div className="chat-header">
        <div className="chat-header-info">
          <h2># {channelName}</h2>
          <span className="connection-status">
            {connecting
              ? "Connecting..."
              : connected
              ? "Connected"
              : "Disconnected"}
          </span>
        </div>
        <div className="chat-header-actions">
          {channel && user?.github_id === channel.creator_github_id &&
            onSwitchToMemory && (
            <button
              className="memory-view-btn"
              onClick={onSwitchToMemory}
              title="Manage bot memories"
            >
              🧠 Memories
            </button>
          )}
          <button className="close-chat-btn" onClick={onClose}>
            ✕
          </button>
        </div>
      </div>

      <div className="messages-container">
        {messages.length === 0
          ? (
            <div className="no-messages">
              <div className="no-messages-icon">💬</div>
              <h3>Welcome to #{channelName}</h3>
              <p>This is the beginning of your conversation. Say hello!</p>
            </div>
          )
          : (
            messages.map((message) => (
              <div
                key={message.id}
                className={`message ${
                  message.is_system_message ? "system-message" : ""
                } ${message.is_llm_response ? "llm-message" : ""} ${
                  message.github_id === user?.github_id ? "own-message" : ""
                }`}
              >
                <div className="message-header">
                  <span className="message-author">
                    {message.is_system_message
                      ? "system"
                      : message.is_llm_response
                      ? "bot"
                      : message.username}
                  </span>
                  <span className="message-time">
                    {new Date(message.timestamp + "Z").toLocaleTimeString([], {
                      hour: "numeric",
                      minute: "2-digit",
                    })}
                  </span>
                </div>
                <div className="message-content">{message.content}</div>
              </div>
            ))
          )}
        <div ref={messagesEndRef} />
      </div>

      <form className="message-input-form" onSubmit={sendMessage}>
        <input
          type="text"
          placeholder={connected ? "Type a message..." : "Connecting..."}
          value={inputValue}
          onInput={(e) => setInputValue((e.target as HTMLInputElement).value)}
          disabled={!connected}
          autoFocus
        />
        <button type="submit" disabled={!connected || !inputValue.trim()}>
          Send
        </button>
      </form>
    </div>
  );
}
