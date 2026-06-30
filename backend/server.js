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

const PORT = process.env.PORT || 10000;
const NODE_ENV = process.env.NODE_ENV || 'development';
const ICECAST_HOST = process.env.ICECAST_HOST || 'localhost';
const ICECAST_PORT = process.env.ICECAST_PORT || 8000;
const ICECAST_MOUNT = process.env.ICECAST_MOUNT || '/stream';
const STREAM_URL = `http://${ICECAST_HOST}:${ICECAST_PORT}${ICECAST_MOUNT}`;
const FRONTEND_URL = process.env.FRONTEND_URL || `http://localhost:${PORT}`;

// ===== MIDDLEWARE =====
app.use(cors({
  origin: NODE_ENV === 'production' ? [FRONTEND_URL, 'https://radioamigosdoseuze.com.br'] : '*',
  credentials: true
}));
app.use(express.json());

// ===== LOGGING =====
const LOG_DIR = path.join(__dirname, '../logs');
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

function log(level, message) {
  const timestamp = new Date().toISOString();
  const logEntry = `[${timestamp}] [${level.toUpperCase()}] ${message}\n`;
  console.log(logEntry.trim());
  fs.appendFileSync(path.join(LOG_DIR, 'server.log'), logEntry);
}

// ===== SERVIR ARQUIVOS ESTÁTICOS =====
app.use(express.static(path.join(__dirname, '../frontend')));

// ===== PLAYLIST FIXA =====
const PLAYLIST = [
  { file: "audio/7 Espadas Ogum.mp3", title: "7 Espadas Ogum", artist: "Ponto de Umbanda", duration: "3:42" },
  { file: "audio/Altar E Tronqueira.mp3", title: "Altar E Tronqueira", artist: "Ponto de Umbanda", duration: "4:15" },
  { file: "audio/a vida e assim mesmo como ela e.mp3", title: "A Vida É Assim Mesmo Como Ela É", artist: "Ponto de Umbanda", duration: "3:28" },
  { file: "audio/Batismo.mp3", title: "Batismo", artist: "Ponto de Umbanda", duration: "3:55" },
  { file: "audio/Benzimento da Lua.mp3", title: "Benzimento da Lua", artist: "Ponto de Umbanda", duration: "4:02" },
  { file: "audio/Caboclo Pedra Preta.mp3", title: "Caboclo Pedra Preta", artist: "Ponto de Umbanda", duration: "3:33" },
  { file: "audio/ciganos.mp3", title: "Ciganos", artist: "Ponto de Umbanda", duration: "3:47" },
  { file: "audio/Exu é lei.mp3", title: "Exu É Lei", artist: "Ponto de Umbanda", duration: "3:18" },
  { file: "audio/jangadeiro.mp3", title: "Jangadeiro", artist: "Ponto de Umbanda", duration: "4:05" },
  { file: "audio/João Batista .mp3", title: "João Batista", artist: "Ponto de Umbanda", duration: "3:51" },
  { file: "audio/Jurema .mp3", title: "Jurema", artist: "Ponto de Umbanda", duration: "3:39" },
  { file: "audio/Luz de Oxalá.mp3", title: "Luz de Oxalá", artist: "Ponto de Umbanda", duration: "4:12" },
  { file: "audio/marinheiros .mp3", title: "Marinheiros", artist: "Ponto de Umbanda", duration: "3:25" },
  { file: "audio/odociaba.mp3", title: "Odociaba", artist: "Ponto de Umbanda", duration: "3:58" },
  { file: "audio/Ogum Beira Mar.mp3", title: "Ogum Beira Mar", artist: "Ponto de Umbanda", duration: "4:20" },
  { file: "audio/ogum .mp3", title: "Ogum", artist: "Ponto de Umbanda", duration: "3:44" },
  { file: "audio/Oxossi é oke aro.mp3", title: "Oxossi É Oke Aro", artist: "Ponto de Umbanda", duration: "3:31" },
  { file: "audio/Passo da Malandragem.mp3", title: "Passo da Malandragem", artist: "Ponto de Umbanda", duration: "3:56" },
  { file: "audio/preto velho .mp3", title: "Preto Velho", artist: "Ponto de Umbanda", duration: "4:08" },
  { file: "audio/quatro da manhã.mp3", title: "Quatro da Manhã", artist: "Ponto de Umbanda", duration: "3:22" },
  { file: "audio/Rosa Caveira.mp3", title: "Rosa Caveira", artist: "Ponto de Umbanda", duration: "3:49" },
  { file: "audio/santo Antônio.mp3", title: "Santo Antônio", artist: "Ponto de Umbanda", duration: "4:01" },
  { file: "audio/Saudação às Sete Linhas.mp3", title: "Saudação às Sete Linhas", artist: "Ponto de Umbanda", duration: "3:37" },
  { file: "audio/Xangô é corisco.mp3", title: "Xangô É Corisco", artist: "Ponto de Umbanda", duration: "3:53" }
];

// ===== ESTADO DA RÁDIO =====
let state = {
  isLive: true,
  currentTrack: { title: PLAYLIST[0].title, artist: PLAYLIST[0].artist, timestamp: Date.now() },
  listeners: 0,
  peakListeners: 0,
  bitrate: 128,
  uptime: 0,
  sourceConnected: true,
  lastUpdate: Date.now(),
  djName: 'AutoDJ',
  playlist: PLAYLIST,
  history: [],
  isPlaying: true,
  serverStartTime: Date.now()
};

// ===== CLIENTES WEBSOCKET =====
let clients = new Set();

function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach(w => {
    if (w.readyState === WebSocket.OPEN) w.send(msg);
  });
}

function updateMetadata(title, artist = 'Ponto de Umbanda') {
  const track = { title, artist, timestamp: Date.now() };
  state.currentTrack = track;
  state.lastUpdate = Date.now();
  state.history.unshift(track);
  if (state.history.length > 50) state.history.pop();
  broadcast({ type: 'metadata', data: track });
  log('info', `Metadata: ${title} - ${artist}`);
}

// ===== WEBSOCKET HANDLERS =====
wss.on('connection', (ws, req) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  clients.add(ws);
  state.listeners = clients.size;
  if (state.listeners > state.peakListeners) state.peakListeners = state.listeners;

  log('info', `Cliente conectado: ${ip} | Total: ${state.listeners}`);

  ws.send(JSON.stringify({ type: 'state', data: state }));
  ws.send(JSON.stringify({ type: 'playlist', data: state.playlist }));

  ws.on('message', (msg) => {
    try {
      const d = JSON.parse(msg);
      if (d.type === 'update_metadata' && d.title) {
        updateMetadata(d.title, d.artist);
      }
      if (d.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
      }
    } catch (e) {
      log('error', `Erro WS: ${e.message}`);
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    state.listeners = clients.size;
    log('info', `Cliente desconectado | Total: ${state.listeners}`);
  });
});

// ===== ROTAS API =====
app.get('/api/status', (req, res) => {
  res.json({ success: true, data: state });
});

app.get('/api/playlist', (req, res) => {
  res.json({ success: true, data: state.playlist });
});

app.get('/api/history', (req, res) => {
  res.json({ success: true, data: state.history.slice(0, 20) });
});

app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    wsClients: clients.size,
    isLive: state.isLive,
    currentTrack: state.currentTrack,
    timestamp: Date.now(),
    env: NODE_ENV
  });
});

// ===== BROADCAST PERIÓDICO =====
setInterval(() => {
  state.uptime = process.uptime();
  state.listeners = clients.size;
  broadcast({ type: 'state', data: state });
}, 10000);

// ===== INICIAR SERVIDOR =====
server.listen(PORT, '0.0.0.0', () => {
  log('info', `
╔══════════════════════════════════════════════════════╗
║     🎧 RÁDIO AMIGOS DO SEU ZÉ - SERVIDOR             ║
╠══════════════════════════════════════════════════════╣
║  📡 Web:     http://0.0.0.0:${PORT}                    ║
║  🔌 WS:      ws://0.0.0.0:${PORT}                    ║
║  🌍 Ambiente: ${NODE_ENV.padEnd(36)}║
╚══════════════════════════════════════════════════════╝
  `);
});

module.exports = { app, server, wss };
