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

    const username =
      (typeof message?.username === "string" && message.username.trim()) ||
      usersBySocketId.get(socket.id) ||
      "Anonymous";

    // Broadcast the message to all connected clients
    io.emit("message", {
      text,
      username,
      senderId: socket.id,
      timestamp: message?.timestamp || new Date(),
    });
  });

  // Handle disconnections
  socket.on("disconnect", () => {
    console.log(socket.id, " disconnected");
    usersBySocketId.delete(socket.id);
  });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
