// Import the Connection and Room types from our bootstrap.ts
import { Connection, Room } from "../../../src/bootstrap.ts";

// Define user state
interface UserState {
  username: string;
  joinedAt: string;
  isTyping?: boolean;
}

// Helper function to create a message payload
function createMessage(
  type: string,
  data: Record<string, unknown>,
  roomId: string,
) {
  return JSON.stringify({
    type,
    ...data,
    timestamp: new Date().toISOString(),
    roomId,
  });
}

export default {
  // Called when the server starts, before accepting connections
  async onStart(ctx: { roomId: string; room: Room }) {
    console.log("Chat server started for room", { roomId: ctx.roomId });
  },

  // Called when a new WebSocket connection is established
  async onConnect(connection: Connection, ctx: { roomId: string; room: Room }) {
    // Initialize user with a guest name
    const guestName = `Guest${Math.floor(Math.random() * 1000)}`;
    connection.setState({
      username: guestName,
      joinedAt: new Date().toISOString(),
    });

    // Send a welcome message to the new user with their username
    connection.send(
      createMessage("welcome", {
        message: `Welcome to the chat room, ${guestName}!`,
        username: guestName,
      }, ctx.roomId),
    );

    // Announce the new user to the room
    ctx.room.broadcast(
      createMessage("system", {
        message: `${guestName} has joined the room`,
      }, ctx.roomId),
      [connection.id], // Don't send to the new user
    );

    // Send the current user list to everyone
    this.broadcastUserList(ctx.room, ctx.roomId);
  },

  // Helper to broadcast the current user list
  broadcastUserList(room: Room, roomId: string) {
    const userList = Array.from(room.getConnections()).map((conn) => {
      const state = conn.state as UserState;
      return {
        id: conn.id,
        username: state.username,
      };
    });

    // Broadcast to everyone
    room.broadcast(
      createMessage("userlist", { users: userList }, roomId),
    );
  },

  // Called when a WebSocket message is received
  async onMessage(
    data: string,
    sender: Connection,
    ctx: { roomId: string; room: Room },
  ) {
    if (!sender.state) return;

    const senderState = sender.state as UserState;

    try {
      // Try to parse as JSON command
      let message: any;
      try {
        message = JSON.parse(data);
      } catch (e) {
        // If not JSON, treat as a regular chat message
        message = { type: "chat", content: data };
      }

      // Handle different command types
      switch (message.type) {
        case "chat":
          // Broadcast the message to all users
          ctx.room.broadcast(
            createMessage("chat", {
              username: senderState.username,
              message: message.content || data,
            }, ctx.roomId),
          );
          break;

        case "nickname":
          this.handleNicknameChange(message, sender, senderState, ctx);
          break;

        case "typing":
          this.handleTypingStatus(message, sender, senderState, ctx);
          break;

        case "private":
          this.handlePrivateMessage(message, sender, senderState, ctx);
          break;

        default:
          // Unknown command, echo it back
          sender.send(
            createMessage("echo", {
              originalMessage: data,
            }, ctx.roomId),
          );
      }
    } catch (error) {
      sender.send(
        createMessage("error", {
          message: "Error processing your message",
        }, ctx.roomId),
      );
    }
  },

  // Handle nickname changes
  handleNicknameChange(
    message: any,
    sender: Connection,
    senderState: UserState,
    ctx: { roomId: string; room: Room },
  ) {
    if (message.username && typeof message.username === "string") {
      const oldUsername = senderState.username;
      const newUsername = message.username.trim().substring(0, 20); // Limit length

      // Update the user's state
      sender.setState({
        ...senderState,
        username: newUsername,
      });

      // Notify everyone of the name change
      ctx.room.broadcast(
        createMessage("system", {
          message: `${oldUsername} is now known as ${newUsername}`,
        }, ctx.roomId),
      );

      // Update the user list
      this.broadcastUserList(ctx.room, ctx.roomId);
    }
  },

  // Handle typing status updates
  handleTypingStatus(
    message: any,
    sender: Connection,
    senderState: UserState,
    ctx: { roomId: string; room: Room },
  ) {
    // Update user's typing status
    sender.setState({
      ...senderState,
      isTyping: message.isTyping === true,
    });

    // Broadcast typing status to others
    ctx.room.broadcast(
      createMessage("typing", {
        username: senderState.username,
        isTyping: message.isTyping === true,
      }, ctx.roomId),
      [sender.id], // Don't send back to the sender
    );
  },

  // Handle private messages
  handlePrivateMessage(
    message: any,
    sender: Connection,
    senderState: UserState,
    ctx: { roomId: string; room: Room },
  ) {
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
        recipient.send(
          createMessage("private", {
            username: senderState.username,
            message: message.content,
          }, ctx.roomId),
        );

        // Also send confirmation to the sender
        sender.send(
          createMessage("private", {
            to: message.to,
            message: message.content,
          }, ctx.roomId),
        );
      } else {
        // User not found
        sender.send(
          createMessage("error", {
            message: `User '${message.to}' not found`,
          }, ctx.roomId),
        );
      }
    }
  },

  // Called when a WebSocket connection is closed
  async onClose(connection: Connection, ctx: { roomId: string; room: Room }) {
    const state = connection.state as UserState;
    const username = state?.username || "A user";

    // Announce that the user has left
    ctx.room.broadcast(
      createMessage("system", {
        message: `${username} has left the room`,
      }, ctx.roomId),
    );

    // Update the user list for everyone
    setTimeout(() => {
      this.broadcastUserList(ctx.room, ctx.roomId);
    }, 50);
  },

  // Called when a WebSocket error occurs
  async onError(
    connection: Connection,
    error: Event,
    ctx: { roomId: string; room: Room },
  ) {
    // Log errors, but don't take action
    console.error("WebSocket error:", error);
  },

  // Called for HTTP requests
  async onRequest(request: Request, ctx: { roomId: string; room: Room }) {
    const url = new URL(request.url);

    if (url.pathname === "/stats") {
      // Return chat room stats
      const connectionCount = ctx.room.connections.size;
      return new Response(
        JSON.stringify({
          roomId: ctx.roomId,
          connections: connectionCount,
          users: Array.from(ctx.room.getConnections()).map((conn) => {
            const state = conn.state as UserState;
            return state.username;
          }),
        }),
        {
          headers: {
            "Content-Type": "application/json",
          },
        },
      );
    }

    return new Response("Chat server is running");
  },
};
