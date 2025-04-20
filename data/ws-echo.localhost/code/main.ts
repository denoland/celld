// Import the Connection and Room types from our bootstrap.ts
import { Connection, Room } from "../../../src/bootstrap.ts";

// Define user state
interface UserState {
  username: string;
  joinedAt: string;
  isTyping?: boolean;
}

// Message types for our IRC-like chat
interface ChatMessage {
  type: string;
  username?: string;
  message?: string;
  timestamp: string;
  roomId?: string;
  users?: Array<{ id: string; username: string }>;
}

export default {
  // Called when the server starts, before accepting connections
  async onStart(ctx: { roomId: string; room: Room }) {
    console.log("Chat server started for room", { roomId: ctx.roomId });
  },

  // Called when a new WebSocket connection is established
  async onConnect(connection: Connection, ctx: { roomId: string; room: Room }) {
    console.log("New user connected", {
      id: connection.id,
      roomId: ctx.roomId,
    });

    // Initialize user with a guest name
    const guestName = `Guest${Math.floor(Math.random() * 1000)}`;
    connection.setState({
      username: guestName,
      joinedAt: new Date().toISOString(),
    });

    // Send a welcome message to the new user with their username
    connection.send(
      JSON.stringify({
        type: "welcome",
        message: `Welcome to the chat room, ${guestName}!`,
        username: guestName, // Include the assigned username
        timestamp: new Date().toISOString(),
        roomId: ctx.roomId,
      }),
    );

    // Announce the new user to the room
    ctx.room.broadcast(
      JSON.stringify({
        type: "system",
        message: `${guestName} has joined the room`,
        timestamp: new Date().toISOString(),
        roomId: ctx.roomId,
      }),
      [connection.id], // Don't send to the new user
    );

    // Wait a brief moment for any initialization to complete
    // This allows any set nickname command to be processed first
    setTimeout(() => {
      // Send the current user list to everyone
      const userList = Array.from(ctx.room.getConnections()).map((conn) => {
        const state = conn.state as UserState;
        return {
          id: conn.id,
          username: state.username,
        };
      });

      // Broadcast to everyone including the new user
      ctx.room.broadcast(
        JSON.stringify({
          type: "userlist",
          users: userList,
          timestamp: new Date().toISOString(),
          roomId: ctx.roomId,
        }),
      );
    }, 100); // Short delay
  },

  // Called when a WebSocket message is received
  async onMessage(
    data: string,
    sender: Connection,
    ctx: { roomId: string; room: Room },
  ) {
    if (!sender.state) return;

    const senderState = sender.state as UserState;
    console.log(`Message from ${senderState.username}:`, data, {
      roomId: ctx.roomId,
    });

    try {
      // Try to parse as JSON command
      let message: any;
      try {
        message = JSON.parse(data);
      } catch (e) {
        // If not JSON, treat as a regular chat message
        message = { type: "chat", content: data };
      }

      const timestamp = new Date().toISOString();

      // Handle different command types
      switch (message.type) {
        case "chat":
          // Broadcast the message to all users
          ctx.room.broadcast(
            JSON.stringify({
              type: "chat",
              username: senderState.username,
              message: message.content || data,
              timestamp,
              roomId: ctx.roomId,
            }),
          );
          break;

        case "nickname":
          // Change the user's nickname
          if (message.username && typeof message.username === "string") {
            const oldUsername = senderState.username;
            const newUsername = message.username.trim().substring(0, 20); // Limit username length

            // Update the user's state
            sender.setState({
              ...senderState,
              username: newUsername,
            });

            // Notify everyone of the name change
            ctx.room.broadcast(
              JSON.stringify({
                type: "system",
                message: `${oldUsername} is now known as ${newUsername}`,
                timestamp,
                roomId: ctx.roomId,
              }),
            );

            // Send updated user list to all clients
            const userList = Array.from(ctx.room.getConnections()).map(
              (conn) => {
                const state = conn.state as UserState;
                return {
                  id: conn.id,
                  username: state.username,
                };
              },
            );

            ctx.room.broadcast(
              JSON.stringify({
                type: "userlist",
                users: userList,
                timestamp: new Date().toISOString(),
                roomId: ctx.roomId,
              }),
            );
          }
          break;

        case "typing":
          // Update typing status
          sender.setState({
            ...senderState,
            isTyping: message.isTyping === true,
          });

          // Broadcast typing status to others
          ctx.room.broadcast(
            JSON.stringify({
              type: "typing",
              username: senderState.username,
              isTyping: message.isTyping === true,
              timestamp,
              roomId: ctx.roomId,
            }),
            [sender.id], // Don't send back to the sender
          );
          break;

        case "private":
          // Handle private messages
          if (message.to && message.content) {
            // Find the recipient by username
            const recipient = Array.from(ctx.room.getConnections()).find(
              (conn) => {
                const state = conn.state as UserState;
                return state.username === message.to;
              },
            );

            if (recipient) {
              // Send the private message to the recipient
              recipient.send(JSON.stringify({
                type: "private",
                username: senderState.username,
                message: message.content,
                timestamp,
                roomId: ctx.roomId,
              }));

              // Also send confirmation to the sender
              sender.send(JSON.stringify({
                type: "private",
                to: message.to,
                message: message.content,
                timestamp,
                roomId: ctx.roomId,
              }));
            } else {
              // User not found
              sender.send(JSON.stringify({
                type: "error",
                message: `User '${message.to}' not found`,
                timestamp,
                roomId: ctx.roomId,
              }));
            }
          }
          break;

        default:
          // Unknown command, echo it back
          sender.send(JSON.stringify({
            type: "echo",
            originalMessage: data,
            timestamp,
            roomId: ctx.roomId,
          }));
      }
    } catch (error) {
      console.error("Error processing message:", error);
      sender.send(JSON.stringify({
        type: "error",
        message: "Error processing your message",
        timestamp: new Date().toISOString(),
        roomId: ctx.roomId,
      }));
    }
  },

  // Called when a WebSocket connection is closed
  async onClose(connection: Connection, ctx: { roomId: string; room: Room }) {
    const state = connection.state as UserState;
    const username = state?.username || "A user";

    console.log(`${username} disconnected`, {
      id: connection.id,
      roomId: ctx.roomId,
    });

    // Announce that the user has left
    ctx.room.broadcast(
      JSON.stringify({
        type: "system",
        message: `${username} has left the room`,
        timestamp: new Date().toISOString(),
        roomId: ctx.roomId,
      }),
    );
  },

  // Called when a WebSocket error occurs
  async onError(
    connection: Connection,
    error: Event,
    ctx: { roomId: string; room: Room },
  ) {
    const state = connection.state as UserState;
    const username = state?.username || "A user";

    console.error(`Error for ${username}:`, error, {
      id: connection.id,
      roomId: ctx.roomId,
    });
  },

  // Called for HTTP requests
  async onRequest(request: Request, ctx: { roomId: string; room: Room }) {
    const url = new URL(request.url);
    console.log(`Request for path: ${url.pathname}`, { roomId: ctx.roomId });

    if (url.pathname === "/stats") {
      // Return chat room stats
      const connectionCount = ctx.room.connections.size;
      return new Response(
        JSON.stringify({
          roomId: ctx.roomId,
          connections: connectionCount,
          uptime: process.uptime(),
        }),
        {
          headers: {
            "Content-Type": "application/json",
          },
        },
      );
    }

    if (url.pathname === "/ping") {
      return new Response("pong");
    }

    return new Response("hello from ws-echo.local\n");
  },
};
