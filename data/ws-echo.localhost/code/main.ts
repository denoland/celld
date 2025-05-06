// Import the Connection and Cell types from our bootstrap.ts
import { Cell, Connection } from "../../../src/bootstrap.ts";

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
  cellId: string,
) {
  return JSON.stringify({
    type,
    ...data,
    timestamp: new Date().toISOString(),
    cellId,
  });
}

export default {
  // Called when the server starts, before accepting connections
  async onStart(ctx: { cellId: string; cell: Cell }) {
    console.log("Chat server started for cell", { cellId: ctx.cellId });
  },

  // Called when a new WebSocket connection is established
  async onConnect(connection: Connection, ctx: { cellId: string; cell: Cell }) {
    // Initialize user with a guest name
    const guestName = `Guest${Math.floor(Math.random() * 1000)}`;
    connection.setState({
      username: guestName,
      joinedAt: new Date().toISOString(),
    });

    // Send a welcome message to the new user with their username
    connection.send(
      createMessage("welcome", {
        message: `Welcome to the chat cell, ${guestName}!`,
        username: guestName,
      }, ctx.cellId),
    );

    // Announce the new user to the cell
    ctx.cell.broadcast(
      createMessage("system", {
        message: `${guestName} has joined the cell`,
      }, ctx.cellId),
      [connection.id], // Don't send to the new user
    );

    // Send the current user list to everyone
    this.broadcastUserList(ctx.cell, ctx.cellId);
  },

  // Helper to broadcast the current user list
  broadcastUserList(cell: Cell, cellId: string) {
    const userList = Array.from(cell.getConnections()).map((conn) => {
      const state = conn.state as UserState;
      return {
        id: conn.id,
        username: state.username,
      };
    });

    // Broadcast to everyone
    cell.broadcast(
      createMessage("userlist", { users: userList }, cellId),
    );
  },

  // Called when a WebSocket message is received
  async onMessage(
    data: string,
    sender: Connection,
    ctx: { cellId: string; cell: Cell },
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
          ctx.cell.broadcast(
            createMessage("chat", {
              username: senderState.username,
              message: message.content || data,
            }, ctx.cellId),
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
            }, ctx.cellId),
          );
      }
    } catch (error) {
      sender.send(
        createMessage("error", {
          message: "Error processing your message",
        }, ctx.cellId),
      );
    }
  },

  // Handle nickname changes
  handleNicknameChange(
    message: any,
    sender: Connection,
    senderState: UserState,
    ctx: { cellId: string; cell: Cell },
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
      ctx.cell.broadcast(
        createMessage("system", {
          message: `${oldUsername} is now known as ${newUsername}`,
        }, ctx.cellId),
      );

      // Update the user list
      this.broadcastUserList(ctx.cell, ctx.cellId);
    }
  },

  // Handle typing status updates
  handleTypingStatus(
    message: any,
    sender: Connection,
    senderState: UserState,
    ctx: { cellId: string; cell: Cell },
  ) {
    // Update user's typing status
    sender.setState({
      ...senderState,
      isTyping: message.isTyping === true,
    });

    // Broadcast typing status to others
    ctx.cell.broadcast(
      createMessage("typing", {
        username: senderState.username,
        isTyping: message.isTyping === true,
      }, ctx.cellId),
      [sender.id], // Don't send back to the sender
    );
  },

  // Handle private messages
  handlePrivateMessage(
    message: any,
    sender: Connection,
    senderState: UserState,
    ctx: { cellId: string; cell: Cell },
  ) {
    if (message.to && message.content) {
      // Find the recipient by username
      const recipient = Array.from(ctx.cell.getConnections()).find(
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
          }, ctx.cellId),
        );

        // Also send confirmation to the sender
        sender.send(
          createMessage("private", {
            to: message.to,
            message: message.content,
          }, ctx.cellId),
        );
      } else {
        // User not found
        sender.send(
          createMessage("error", {
            message: `User '${message.to}' not found`,
          }, ctx.cellId),
        );
      }
    }
  },

  // Called when a WebSocket connection is closed
  async onClose(connection: Connection, ctx: { cellId: string; cell: Cell }) {
    const state = connection.state as UserState;
    const username = state?.username || "A user";

    // Announce that the user has left
    ctx.cell.broadcast(
      createMessage("system", {
        message: `${username} has left the cell`,
      }, ctx.cellId),
    );

    // Update the user list for everyone
    setTimeout(() => {
      this.broadcastUserList(ctx.cell, ctx.cellId);
    }, 50);
  },

  // Called when a WebSocket error occurs
  async onError(
    connection: Connection,
    error: Event,
    ctx: { cellId: string; cell: Cell },
  ) {
    // Log errors, but don't take action
    console.error("WebSocket error:", error);
  },

  // Called for HTTP requests
  async onRequest(request: Request, ctx: { cellId: string; cell: Cell }) {
    const url = new URL(request.url);

    if (url.pathname === "/stats") {
      // Return chat cell stats
      const connectionCount = ctx.cell.connections.size;
      return new Response(
        JSON.stringify({
          cellId: ctx.cellId,
          connections: connectionCount,
          users: Array.from(ctx.cell.getConnections()).map((conn) => {
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
