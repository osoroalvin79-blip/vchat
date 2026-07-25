const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

// Keep the last 50 messages in memory so new joiners see recent history.
// This resets whenever the server restarts (fine for a casual chat room).
const HISTORY_LIMIT = 50;
const history = [];

// Track connected clients so we can show a live "people online" count
const clients = new Map(); // ws -> { id, nickname }

function broadcast(payload) {
  const raw = JSON.stringify(payload);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(raw);
  }
}

function broadcastPresence() {
  const nicknames = [...clients.values()].map((c) => c.nickname);
  broadcast({ type: 'presence', count: clients.size, nicknames });
}

wss.on('connection', (ws) => {
  const id = crypto.randomBytes(6).toString('hex');
  clients.set(ws, { id, nickname: null });

  ws.send(JSON.stringify({ type: 'history', messages: history }));

  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (data.type === 'join') {
      const nickname = String(data.nickname || 'Guest').slice(0, 24).trim() || 'Guest';
      clients.set(ws, { id, nickname });
      const sysMsg = {
        type: 'message',
        system: true,
        text: `${nickname} joined the chat`,
        timestamp: Date.now(),
      };
      history.push(sysMsg);
      if (history.length > HISTORY_LIMIT) history.shift();
      broadcast(sysMsg);
      broadcastPresence();
      return;
    }

    if (data.type === 'chat') {
      const client = clients.get(ws);
      if (!client || !client.nickname) return; // must have joined first
      const text = String(data.text || '').slice(0, 500).trim();
      if (!text) return;

      const msg = {
        type: 'message',
        id: crypto.randomBytes(6).toString('hex'),
        nickname: client.nickname,
        text,
        timestamp: Date.now(),
      };
      history.push(msg);
      if (history.length > HISTORY_LIMIT) history.shift();
      broadcast(msg);
    }
  });

  ws.on('close', () => {
    const client = clients.get(ws);
    clients.delete(ws);
    if (client && client.nickname) {
      const sysMsg = {
        type: 'message',
        system: true,
        text: `${client.nickname} left the chat`,
        timestamp: Date.now(),
      };
      history.push(sysMsg);
      if (history.length > HISTORY_LIMIT) history.shift();
      broadcast(sysMsg);
    }
    broadcastPresence();
  });
});

server.listen(PORT, () => {
  console.log(`Chat server listening on port ${PORT}`);
});
