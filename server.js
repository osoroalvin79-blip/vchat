const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DAILY_API_KEY = process.env.DAILY_API_KEY;

// Media (photos/files/voice notes) travel as base64 inside chat messages.
// This cap keeps the server and everyone's connection responsive.
const MAX_MEDIA_BYTES = 6 * 1024 * 1024; // ~6MB, generous enough for a compressed photo or short voice note

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

/**
 * Creates a new video meeting room via Daily.co's REST API.
 * The room expires automatically after 4 hours so unused rooms don't pile up.
 */
app.post('/api/create-room', async (req, res) => {
  if (!DAILY_API_KEY) {
    return res.status(500).json({ error: 'Video calling is not configured on this server yet.' });
  }
  try {
    const roomRes = await fetch('https://api.daily.co/v1/rooms', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${DAILY_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        privacy: 'private',
        properties: {
          enable_screenshare: true,
          enable_chat: false, // we already have our own chat
          exp: Math.floor(Date.now() / 1000) + 60 * 60 * 4, // auto-expire in 4 hours
        },
      }),
    });
    const room = await roomRes.json();
    if (!roomRes.ok) {
      console.error('[DAILY] create-room error:', room);
      return res.status(502).json({ error: room.error || 'Could not create room.' });
    }
    res.json({ url: room.url, name: room.name });
  } catch (err) {
    console.error('[DAILY] create-room exception:', err.message);
    res.status(500).json({ error: 'server_error' });
  }
});

/**
 * Issues a meeting token for a specific room, marking the requester as
 * either the host (is_owner: true - gets mute/moderation controls) or a
 * regular participant.
 */
app.post('/api/meeting-token', async (req, res) => {
  if (!DAILY_API_KEY) {
    return res.status(500).json({ error: 'Video calling is not configured on this server yet.' });
  }
  const { roomName, userName, isHost } = req.body || {};
  if (!roomName || !userName) {
    return res.status(400).json({ error: 'missing_fields' });
  }
  try {
    const tokenRes = await fetch('https://api.daily.co/v1/meeting-tokens', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${DAILY_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        properties: {
          room_name: roomName,
          user_name: String(userName).slice(0, 40),
          is_owner: Boolean(isHost),
        },
      }),
    });
    const tokenData = await tokenRes.json();
    if (!tokenRes.ok) {
      console.error('[DAILY] meeting-token error:', tokenData);
      return res.status(502).json({ error: tokenData.error || 'Could not create token.' });
    }

    // Also look up the room's real URL, so the client doesn't have to guess it
    const roomInfoRes = await fetch(`https://api.daily.co/v1/rooms/${encodeURIComponent(roomName)}`, {
      headers: { Authorization: `Bearer ${DAILY_API_KEY}` },
    });
    const roomInfo = await roomInfoRes.json();
    if (!roomInfoRes.ok) {
      return res.status(404).json({ error: 'Room not found. Check the room name/link and try again.' });
    }

    res.json({ token: tokenData.token, roomUrl: roomInfo.url });
  } catch (err) {
    console.error('[DAILY] meeting-token exception:', err.message);
    res.status(500).json({ error: 'server_error' });
  }
});

const server = http.createServer(app);
const wss = new WebSocketServer({
  server,
  path: '/ws',
  // Raise the default per-message size limit so photos/voice notes fit
  maxPayload: MAX_MEDIA_BYTES + 1024 * 100,
});

// Keep the last 50 messages in memory so new joiners see recent history.
// This resets whenever the server restarts (fine for a casual chat room).
const HISTORY_LIMIT = 50;
const history = [];

// Track connected clients so we can show a live "people online" count
const clients = new Map(); // ws -> { id, nickname }

// Track who's currently typing, so we can clear stale indicators server-side too
const typingUsers = new Map(); // nickname -> timeout handle

function broadcast(payload, exclude) {
  const raw = JSON.stringify(payload);
  for (const client of wss.clients) {
    if (client !== exclude && client.readyState === WebSocket.OPEN) client.send(raw);
  }
}

function broadcastPresence() {
  const nicknames = [...clients.values()].map((c) => c.nickname).filter(Boolean);
  broadcast({ type: 'presence', count: clients.size, nicknames });
}

function pushHistory(msg) {
  history.push(msg);
  if (history.length > HISTORY_LIMIT) history.shift();
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
      pushHistory(sysMsg);
      broadcast(sysMsg);
      broadcastPresence();
      return;
    }

    if (data.type === 'typing') {
      const client = clients.get(ws);
      if (!client || !client.nickname) return;
      broadcast({ type: 'typing', nickname: client.nickname }, ws);
      return;
    }

    if (data.type === 'stop_typing') {
      const client = clients.get(ws);
      if (!client || !client.nickname) return;
      broadcast({ type: 'stop_typing', nickname: client.nickname }, ws);
      return;
    }

    if (data.type === 'chat') {
      const client = clients.get(ws);
      if (!client || !client.nickname) return; // must have joined first

      const text = String(data.text || '').slice(0, 500).trim();

      // Optional media attachment: { mediaType: 'image'|'file'|'audio', mediaData: base64 string, mediaName, mediaSize }
      let media = null;
      if (data.mediaType && data.mediaData) {
        const approxBytes = Math.ceil((data.mediaData.length * 3) / 4);
        if (approxBytes > MAX_MEDIA_BYTES) {
          ws.send(JSON.stringify({ type: 'error', message: 'That file is too large (max ~6MB).' }));
          return;
        }
        if (!['image', 'file', 'audio'].includes(data.mediaType)) return;
        media = {
          mediaType: data.mediaType,
          mediaData: data.mediaData,
          mediaName: String(data.mediaName || 'file').slice(0, 120),
          mediaMime: String(data.mediaMime || '').slice(0, 100),
        };
      }

      if (!text && !media) return;

      const msg = {
        type: 'message',
        id: crypto.randomBytes(6).toString('hex'),
        nickname: client.nickname,
        text,
        timestamp: Date.now(),
        ...(media || {}),
      };
      pushHistory(msg);
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
      pushHistory(sysMsg);
      broadcast(sysMsg);
      broadcast({ type: 'stop_typing', nickname: client.nickname });
    }
    broadcastPresence();
  });
});

server.listen(PORT, () => {
  console.log(`Chat server listening on port ${PORT}`);
});
