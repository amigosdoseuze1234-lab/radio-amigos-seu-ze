const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// ================= CONFIG =================
const PORT = process.env.PORT || 10000;
const NODE_ENV = process.env.NODE_ENV || 'development';

const FRONTEND_DIR = path.resolve(__dirname, '../frontend');
const AUDIO_DIR = path.join(FRONTEND_DIR, 'audio');

// ================= MIDDLEWARE =================
app.use(cors({
  origin: NODE_ENV === 'production'
    ? ['https://radioamigosdoseuze.com.br']
    : '*',
  credentials: true
}));

app.use(express.json());

// ================= LOG SYSTEM =================
const LOG_DIR = path.join(__dirname, '../logs');
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

function log(level, message) {
  const line = `[${new Date().toISOString()}] [${level.toUpperCase()}] ${message}\n`;
  console.log(line.trim());
  fs.appendFileSync(path.join(LOG_DIR, 'server.log'), line);
}

// ================= STATIC FILES =================
app.use(express.static(FRONTEND_DIR));
app.use('/audio', express.static(AUDIO_DIR));

// ================= PLAYLIST =================
const PLAYLIST = [
  { file: "7 Espadas Ogum.mp3", title: "7 Espadas Ogum", artist: "Ponto de Umbanda" },
  { file: "Altar E Tronqueira.mp3", title: "Altar E Tronqueira", artist: "Ponto de Umbanda" },
  { file: "a vida e assim mesmo como ela e.mp3", title: "A Vida É Assim Mesmo", artist: "Ponto de Umbanda" },
  { file: "Batismo.mp3", title: "Batismo", artist: "Ponto de Umbanda" },
  { file: "Benzimento da Lua.mp3", title: "Benzimento da Lua", artist: "Ponto de Umbanda" },
  { file: "Caboclo Pedra Preta.mp3", title: "Caboclo Pedra Preta", artist: "Ponto de Umbanda" },
  { file: "ciganos.mp3", title: "Ciganos", artist: "Ponto de Umbanda" },
  { file: "Exu é lei.mp3", title: "Exu É Lei", artist: "Ponto de Umbanda" },
  { file: "jangadeiro.mp3", title: "Jangadeiro", artist: "Ponto de Umbanda" },
  { file: "João Batista.mp3", title: "João Batista", artist: "Ponto de Umbanda" },
  { file: "Jurema.mp3", title: "Jurema", artist: "Ponto de Umbanda" },
  { file: "Luz de Oxalá.mp3", title: "Luz de Oxalá", artist: "Ponto de Umbanda" }
];

// ================= STATE =================
let state = {
  listeners: 0,
  peakListeners: 0,
  currentTrack: PLAYLIST[0],
  playlist: PLAYLIST,
  history: [],
  uptime: 0,
  isLive: true,
  lastUpdate: Date.now()
};

// ================= WEBSOCKET =================
const clients = new Set();

function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  });
}

wss.on('connection', (ws, req) => {
  clients.add(ws);
  state.listeners = clients.size;

  log('info', `Cliente conectado. Total: ${state.listeners}`);

  ws.send(JSON.stringify({ type: 'state', data: state }));
  ws.send(JSON.stringify({ type: 'playlist', data: state.playlist }));

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong', time: Date.now() }));
      }

      if (data.type === 'update_metadata') {
        state.currentTrack = {
          title: data.title,
          artist: data.artist || 'Ponto de Umbanda'
        };

        state.lastUpdate = Date.now();

        broadcast({ type: 'metadata', data: state.currentTrack });
      }

    } catch (err) {
      log('error', `WS error: ${err.message}`);
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    state.listeners = clients.size;
    log('info', `Cliente desconectado. Total: ${state.listeners}`);
  });
});

// ================= API ROUTES =================
app.get('/api/status', (req, res) => {
  res.json(state);
});

app.get('/api/playlist', (req, res) => {
  res.json(state.playlist);
});

app.get('/api/history', (req, res) => {
  res.json(state.history.slice(0, 30));
});

// STREAM ATUAL CORRIGIDO
app.get('/api/stream', (req, res) => {
  const track = state.currentTrack;

  const fileUrl =
    `${req.protocol}://${req.get('host')}/audio/${encodeURIComponent(track.file)}`;

  res.json({
    stream: fileUrl,
    title: track.title,
    artist: track.artist
  });
});

// HEALTH CHECK
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    wsClients: clients.size,
    currentTrack: state.currentTrack
  });
});

// ================= LOOP =================
setInterval(() => {
  state.uptime = process.uptime();
  state.listeners = clients.size;

  broadcast({ type: 'state', data: state });
}, 10000);

// ================= START SERVER =================
server.listen(PORT, '0.0.0.0', () => {
  log('info', `Servidor rodando na porta ${PORT}`);
});

module.exports = { app, server, wss };