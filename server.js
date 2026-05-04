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
const typingSocketIds = new Set();
const MAX_FILE_BYTES = 2 * 1024 * 1024; // 2MB

function getUsername(socket, messageUsername) {
  const fromMessage =
    typeof messageUsername === "string" ? messageUsername.trim() : "";
  return fromMessage || usersBySocketId.get(socket.id) || "Anonymous";
}

// Handle WebSocket connections here
io.on("connection", (socket) => {
  console.log("A new user has connected", socket.id);

  socket.on("join", ({ username } = {}) => {
    const name = typeof username === "string" ? username.trim() : "";
    if (name) usersBySocketId.set(socket.id, name);
  });

  // Listen for incoming messages from clients
  socket.on("message", (message) => {
    const text = typeof message?.text === "string" ? message.text : "";
    if (!text.trim()) return;

    const username = getUsername(socket, message?.username);

    if (typingSocketIds.has(socket.id)) {
      typingSocketIds.delete(socket.id);
      socket.broadcast.emit("typing:stop", { username, senderId: socket.id });
    }

    // Broadcast the message to all connected clients
    io.emit("message", {
      type: "text",
      text,
      username,
      senderId: socket.id,
      timestamp: message?.timestamp || new Date(),
    });
  });

  socket.on("file", (payload) => {
    const username = getUsername(socket, payload?.username);
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
      socket.broadcast.emit("typing:stop", { username, senderId: socket.id });
    }

    io.emit("message", {
      type: "file",
      text: "",
      username,
      senderId: socket.id,
      timestamp: payload?.timestamp || new Date(),
      file: { name, mime, size, dataUrl },
    });
  });

  socket.on("typing:start", ({ username } = {}) => {
    const name = getUsername(socket, username);
    if (typingSocketIds.has(socket.id)) return;

    typingSocketIds.add(socket.id);
    socket.broadcast.emit("typing:start", { username: name, senderId: socket.id });
  });

  socket.on("typing:stop", ({ username } = {}) => {
    const name = getUsername(socket, username);
    if (!typingSocketIds.has(socket.id)) return;

    typingSocketIds.delete(socket.id);
    socket.broadcast.emit("typing:stop", { username: name, senderId: socket.id });
  });

  // Handle disconnections
  socket.on("disconnect", () => {
    console.log(socket.id, " disconnected");
    if (typingSocketIds.has(socket.id)) {
      typingSocketIds.delete(socket.id);
      const username = usersBySocketId.get(socket.id) || "Anonymous";
      socket.broadcast.emit("typing:stop", { username, senderId: socket.id });
    }
    usersBySocketId.delete(socket.id);
  });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
