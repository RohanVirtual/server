const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    credentials: true,
  },
});

const usersBySocketId = new Map();
const socketIdByUsername = new Map(); // username -> socketId
const typingSocketIds = new Set();
const MAX_FILE_BYTES = 2 * 1024 * 1024; // 2MB

// Track active conversations
const activeConversations = new Map(); // username -> { withUser, startTime }

function getUsername(socket, messageUsername) {
  const fromMessage =
    typeof messageUsername === "string" ? messageUsername.trim() : "";
  return fromMessage || usersBySocketId.get(socket.id) || "Anonymous";
}

function getSocketIdByUsername(username) {
  const name = typeof username === "string" ? username.trim() : "";
  if (!name) return null;
  return socketIdByUsername.get(name) || null;
}

// Get online users list
function getOnlineUsers() {
  const onlineUsers = [];
  for (const [username, socketId] of socketIdByUsername.entries()) {
    onlineUsers.push({
      username,
      socketId,
      isOnline: true,
    });
  }
  return onlineUsers;
}

// Handle WebSocket connections here
io.on("connection", (socket) => {
  console.log("A new user has connected", socket.id);
  let currentUsername = null;

  socket.on("join", ({ username } = {}) => {
    const name = typeof username === "string" ? username.trim() : "";
    if (!name) return;
    
    currentUsername = name;
    usersBySocketId.set(socket.id, name);
    socketIdByUsername.set(name, socket.id);
    
    // Send online users list to the newly joined user
    socket.emit("online-users", getOnlineUsers());
    
    // Broadcast to all other users that this user is online
    socket.broadcast.emit("user-online", { username: name, socketId: socket.id });
    
    console.log(`${name} joined the chat`);
  });

  // User wants to start a conversation
  socket.on("start-conversation", ({ withUser }) => {
    const fromUser = currentUsername || usersBySocketId.get(socket.id);
    if (!fromUser || !withUser) return;
    
    const targetSocketId = getSocketIdByUsername(withUser);
    if (targetSocketId) {
      // Notify the target user
      io.to(targetSocketId).emit("conversation-request", {
        from: fromUser,
        timestamp: new Date(),
      });
      
      // Store conversation start time
      activeConversations.set(fromUser, { withUser, startTime: Date.now() });
      activeConversations.set(withUser, { withUser: fromUser, startTime: Date.now() });
      
      // Send notification to both users
      io.to(targetSocketId).emit("notification", {
        type: "conversation_started",
        message: `${fromUser} wants to chat with you`,
        from: fromUser,
        timestamp: new Date(),
      });
      
      socket.emit("notification", {
        type: "conversation_started",
        message: `You started a conversation with ${withUser}`,
        to: withUser,
        timestamp: new Date(),
      });
    } else {
      socket.emit("notification", {
        type: "error",
        message: `${withUser} is not online`,
        timestamp: new Date(),
      });
    }
  });

  // Listen for incoming messages from clients
  socket.on("message", (message) => {
    const text = typeof message?.text === "string" ? message.text : "";
    if (!text.trim()) return;

    const username = getUsername(socket, message?.username);
    const toUsername = typeof message?.to === "string" ? message.to.trim() : "";

    if (typingSocketIds.has(socket.id)) {
      typingSocketIds.delete(socket.id);
      socket.broadcast.emit("typing:stop", { username, senderId: socket.id, to: toUsername });
    }

    const payload = {
      type: "text",
      text,
      username,
      senderId: socket.id,
      timestamp: message?.timestamp || new Date(),
      ...(toUsername ? { to: toUsername } : {}),
      ...(message?.clientMessageId ? { clientMessageId: message.clientMessageId } : {}),
    };

    // If "to" is provided, deliver privately (and echo to sender)
    if (toUsername) {
      const toSocketId = getSocketIdByUsername(toUsername);
      if (toSocketId) {
        io.to(toSocketId).emit("message", payload);
        
        // Send notification about new message
        io.to(toSocketId).emit("notification", {
          type: "new_message",
          message: `New message from ${username}`,
          from: username,
          timestamp: new Date(),
        });
      }
      socket.emit("message", payload);
      return;
    }

    // Otherwise broadcast globally
    io.emit("message", payload);
  });

  socket.on("file", (payload) => {
    const username = getUsername(socket, payload?.username);
    const toUsername = typeof payload?.to === "string" ? payload.to.trim() : "";
    const file = payload?.file;

    const name = typeof file?.name === "string" ? file.name : "file";
    const mime = typeof file?.mime === "string" ? file.mime : "application/octet-stream";
    const size = Number.isFinite(file?.size) ? file.size : null;
    const dataUrl = typeof file?.dataUrl === "string" ? file.dataUrl : "";

    if (!dataUrl.startsWith("data:")) return;
    if (size == null || size <= 0 || size > MAX_FILE_BYTES) return;

    // Basic sanity check: data URL should at least mention the mime
    if (!dataUrl.startsWith(`data:${mime}`)) return;

    if (typingSocketIds.has(socket.id)) {
      typingSocketIds.delete(socket.id);
      socket.broadcast.emit("typing:stop", { username, senderId: socket.id, to: toUsername });
    }

    const out = {
      type: "file",
      text: "",
      username,
      senderId: socket.id,
      timestamp: payload?.timestamp || new Date(),
      file: { name, mime, size, dataUrl },
      ...(toUsername ? { to: toUsername } : {}),
      ...(payload?.clientMessageId ? { clientMessageId: payload.clientMessageId } : {}),
    };

    if (toUsername) {
      const toSocketId = getSocketIdByUsername(toUsername);
      if (toSocketId) {
        io.to(toSocketId).emit("message", out);
        
        // Send notification about new file
        io.to(toSocketId).emit("notification", {
          type: "new_file",
          message: `${username} sent you a file: ${name}`,
          from: username,
          fileName: name,
          timestamp: new Date(),
        });
      }
      socket.emit("message", out);
      return;
    }

    io.emit("message", out);
  });

  socket.on("typing:start", ({ username, to } = {}) => {
    const name = getUsername(socket, username);
    if (typingSocketIds.has(socket.id)) return;

    typingSocketIds.add(socket.id);
    
    // If typing in DM, only send to the specific user
    if (to) {
      const toSocketId = getSocketIdByUsername(to);
      if (toSocketId) {
        io.to(toSocketId).emit("typing:start", { username: name, senderId: socket.id });
      }
    } else {
      socket.broadcast.emit("typing:start", { username: name, senderId: socket.id });
    }
  });

  socket.on("typing:stop", ({ username, to } = {}) => {
    const name = getUsername(socket, username);
    if (!typingSocketIds.has(socket.id)) return;

    typingSocketIds.delete(socket.id);
    
    // If typing in DM, only send to the specific user
    if (to) {
      const toSocketId = getSocketIdByUsername(to);
      if (toSocketId) {
        io.to(toSocketId).emit("typing:stop", { username: name, senderId: socket.id });
      }
    } else {
      socket.broadcast.emit("typing:stop", { username: name, senderId: socket.id });
    }
  });

  // Handle disconnections
  socket.on("disconnect", () => {
    console.log(socket.id, " disconnected");
    const username = usersBySocketId.get(socket.id);
    
    if (typingSocketIds.has(socket.id)) {
      typingSocketIds.delete(socket.id);
      socket.broadcast.emit("typing:stop", { username: username || "Anonymous", senderId: socket.id });
    }
    
    if (username) {
      // Broadcast to all users that this user is offline
      socket.broadcast.emit("user-offline", { username });
      
      if (socketIdByUsername.get(username) === socket.id) {
        socketIdByUsername.delete(username);
      }
      
      // Clean up active conversations
      if (activeConversations.has(username)) {
        activeConversations.delete(username);
      }
    }
    
    usersBySocketId.delete(socket.id);
  });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});