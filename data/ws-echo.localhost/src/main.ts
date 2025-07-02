import { cell } from "../../../sdk/mod.ts";

console.log(`[${cell.id}] Initializing WebSocket echo server...`);

// Define user state
interface UserState {
  username: string;
  joinedAt: string;
  isTyping?: boolean;
}

// Map to store user states by connection ID
const users = new Map<string, UserState>();

// Helper function to create a message payload
function createMessage(
  type: string,
  data: Record<string, unknown>,
) {
  return JSON.stringify({
    type,
    ...data,
    timestamp: new Date().toISOString(),
    cellId: cell.id,
  });
}

// Helper to broadcast the current user list
function broadcastUserList() {
  const userList = Array.from(users.entries()).map(([id, state]) => {
    return {
      id,
      username: state.username,
    };
  });

  // Broadcast to everyone
  cell.broadcast(
    createMessage("userlist", { users: userList }),
  );
}

// Handle HTTP requests
cell.request((request: Request, ctx): Response => {
  const url = new URL(request.url);

  if (url.pathname === "/stats") {
    // Return chat cell stats
    return new Response(
      JSON.stringify({
        cellId: cell.id,
        connections: users.size,
        users: Array.from(users.values()).map((state) => state.username),
      }),
      {
        headers: {
          "Content-Type": "application/json",
        },
      },
    );
  }

  return new Response("Chat server is running");
});

// Handle new connections
cell.connect((socket: WebSocket, id: string, ctx) => {
  // Initialize user with a guest name
  const guestName = `Guest${Math.floor(Math.random() * 1000)}`;

  // Store the user state
  users.set(id, {
    username: guestName,
    joinedAt: new Date().toISOString(),
  });

  // Send a welcome message to the new user
  socket.send(
    createMessage("welcome", {
      message: `Welcome to the chat cell, ${guestName}!`,
      username: guestName,
    }),
  );

  // Announce the new user to the cell
  cell.broadcast(
    createMessage("system", {
      message: `${guestName} has joined the cell`,
    }),
    [id], // Don't send to the new user
  );

  // Send the current user list to everyone
  broadcastUserList();
});

// Handle message reception
cell.message((event: MessageEvent, socket: WebSocket, id: string, ctx) => {
  const senderState = users.get(id);
  if (!senderState) return;

  try {
    // Try to parse as JSON command
    let message: any;
    try {
      message = JSON.parse(event.data.toString());
    } catch (e) {
      // If not JSON, treat as a regular chat message
      message = { type: "chat", content: event.data.toString() };
    }

    // Handle different command types
    switch (message.type) {
      case "chat":
        // Broadcast the message to all users
        cell.broadcast(
          createMessage("chat", {
            username: senderState.username,
            message: message.content || event.data.toString(),
          }),
        );
        break;

      case "nickname":
        handleNicknameChange(message, socket, id, senderState);
        break;

      case "typing":
        handleTypingStatus(message, socket, id, senderState);
        break;

      case "private":
        handlePrivateMessage(message, socket, id, senderState);
        break;

      default:
        // Unknown command, echo it back
        socket.send(
          createMessage("echo", {
            originalMessage: event.data.toString(),
          }),
        );
    }
  } catch (error) {
    socket.send(
      createMessage("error", {
        message: "Error processing your message",
      }),
    );
  }
});

// Handle nickname changes
function handleNicknameChange(
  message: any,
  socket: WebSocket,
  id: string,
  senderState: UserState,
) {
  if (message.username && typeof message.username === "string") {
    const oldUsername = senderState.username;
    const newUsername = message.username.trim().substring(0, 20); // Limit length

    // Update the user's state
    users.set(id, {
      ...senderState,
      username: newUsername,
    });

    // Notify everyone of the name change
    cell.broadcast(
      createMessage("system", {
        message: `${oldUsername} is now known as ${newUsername}`,
      }),
    );

    // Update the user list
    broadcastUserList();
  }
}

// Handle typing status updates
function handleTypingStatus(
  message: any,
  socket: WebSocket,
  id: string,
  senderState: UserState,
) {
  // Update user's typing status
  users.set(id, {
    ...senderState,
    isTyping: message.isTyping === true,
  });

  // Broadcast typing status to others
  cell.broadcast(
    createMessage("typing", {
      username: senderState.username,
      isTyping: message.isTyping === true,
    }),
    [id], // Don't send back to the sender
  );
}

// Handle private messages
function handlePrivateMessage(
  message: any,
  socket: WebSocket,
  id: string,
  senderState: UserState,
) {
  if (message.to && message.content) {
    // Find the recipient by username
    let recipientId: string | undefined;
    let recipientSocket: WebSocket | undefined;

    for (const [userId, userState] of users.entries()) {
      if (userState.username === message.to) {
        recipientId = userId;
        recipientSocket = cell.getWebSocket(userId);
        break;
      }
    }

    if (recipientSocket && recipientId) {
      // Send the private message to the recipient
      recipientSocket.send(
        createMessage("private", {
          username: senderState.username,
          message: message.content,
        }),
      );

      // Also send confirmation to the sender
      socket.send(
        createMessage("private", {
          to: message.to,
          message: message.content,
        }),
      );
    } else {
      // User not found
      socket.send(
        createMessage("error", {
          message: `User '${message.to}' not found`,
        }),
      );
    }
  }
}

// Handle connection closures
cell.close((socket: WebSocket, id: string, ctx) => {
  const state = users.get(id);
  const username = state?.username || "A user";

  // Remove user from our map
  users.delete(id);

  // Announce that the user has left
  cell.broadcast(
    createMessage("system", {
      message: `${username} has left the cell`,
    }),
  );

  // Update the user list for everyone
  setTimeout(() => {
    broadcastUserList();
  }, 50);
});

// Handle WebSocket errors
cell.error((error: Error | ErrorEvent | Event) => {
  // Log errors, but don't take action
  console.error("WebSocket error:", error);
});
