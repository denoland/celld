import { useEffect, useRef, useState } from "preact/hooks";
import { authService } from "./auth.ts";
import { type Channel } from "./channelService.ts";

interface Message {
  id: number;
  github_id: string | null;
  username: string;
  timestamp: string;
  content: string;
  message_type: "user" | "bot" | "system";
  message_category?: string;
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

  // State to track which messages have thinking expanded
  const [expandedThinking, setExpandedThinking] = useState<Set<number>>(
    new Set(),
  );

  // Helper function to detect thinking messages
  const isThinkingMessage = (message: Message) => {
    return message.message_category === "thinking";
  };

  // Helper function to detect tool usage system messages
  const isToolMessage = (message: Message) => {
    return message.message_category && [
      "store_memory",
      "read_memories",
      "set_alarm",
      "system_status",
    ].includes(message.message_category);
  };

  // Helper function to get emoji for message category
  const getCategoryEmoji = (category?: string) => {
    switch (category) {
      case "thinking":
        return "💭";
      case "store_memory":
        return "💾";
      case "read_memories":
        return "🧠";
      case "set_alarm":
        return "📅";
      case "system_status":
        return "🤔";
      default:
        return "";
    }
  };

  // Helper function to format message content with emoji
  const formatMessageContent = (message: Message) => {
    const emoji = getCategoryEmoji(message.message_category);
    return emoji ? `${emoji} ${message.content}` : message.content;
  };

  // Find thinking and tool messages that immediately precede a bot message
  const findThinkingForMessage = (message: Message, messageIndex: number) => {
    if (message.message_type !== "bot") return null;

    const precedingMessages = [];

    // Look backwards from this message to find consecutive thinking/tool messages
    for (let i = messageIndex - 1; i >= 0; i--) {
      const prevMessage = messages[i];

      // Stop if we hit a regular bot message (not thinking) or user message
      if (
        (prevMessage.message_type === "bot" &&
          !isThinkingMessage(prevMessage)) ||
        (prevMessage.message_type !== "system" &&
          prevMessage.message_type !== "bot")
      ) {
        break;
      }

      // Collect thinking and tool messages
      if (isThinkingMessage(prevMessage) || isToolMessage(prevMessage)) {
        precedingMessages.unshift(prevMessage); // Add to beginning to maintain order
      }
    }
    return precedingMessages.length > 0 ? precedingMessages : null;
  };

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
            setMessages((prev) => {
              const newMessages = [...prev, data.message];
              return newMessages.sort((a, b) => a.id - b.id);
            });
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
            messages.map((message, index) => {
              // Skip thinking messages and tool messages - they will be shown as part of bot messages
              if (isThinkingMessage(message) || isToolMessage(message)) {
                return null;
              }

              // Skip system status messages (like "Bot is thinking...") from main conversation
              if (message.message_category === "system_status") {
                return null;
              }

              const relatedMessages = findThinkingForMessage(message, index);
              const hasThinking = !!relatedMessages;
              const isThinkingExpanded = expandedThinking.has(message.id);

              return (
                <div key={message.id}>
                  <div
                    className={`message ${
                      message.message_type === "system" ? "system-message" : ""
                    } ${message.message_type === "bot" ? "llm-message" : ""} ${
                      message.github_id === user?.github_id ? "own-message" : ""
                    }`}
                  >
                    {message.message_type !== "system" && (
                      <div className="message-header">
                        <span className="message-author">
                          {message.message_type === "bot"
                            ? "bot"
                            : message.username}
                        </span>
                        <span className="message-time">
                          {new Date(message.timestamp + "Z").toLocaleTimeString(
                            [],
                            {
                              hour: "numeric",
                              minute: "2-digit",
                            },
                          )}
                        </span>
                      </div>
                    )}
                    {hasThinking && isThinkingExpanded && (
                      <div className="thinking-content-expanded">
                        {relatedMessages.map((relMsg) => (
                          <div key={relMsg.id} className="thinking-item">
                            {formatMessageContent(relMsg)}
                          </div>
                        ))}
                      </div>
                    )}
                    <div className="message-content-wrapper">
                      <div className="message-content">
                        {message.message_type === "system"
                          ? formatMessageContent(message)
                          : message.content}
                      </div>
                      {hasThinking && (
                        <button
                          className="thinking-toggle-btn"
                          onClick={() => {
                            const newExpanded = new Set(expandedThinking);
                            if (isThinkingExpanded) {
                              newExpanded.delete(message.id);
                            } else {
                              newExpanded.add(message.id);
                            }
                            setExpandedThinking(newExpanded);
                          }}
                          title={isThinkingExpanded
                            ? "Hide thinking"
                            : "Show thinking"}
                        >
                          {isThinkingExpanded ? "⌃" : "⌄"}
                        </button>
                      )}
                    </div>
                  </div>
                </div>
              );
            }).filter(Boolean)
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
