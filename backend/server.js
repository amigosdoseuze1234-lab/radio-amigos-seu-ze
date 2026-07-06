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

// 🔥 CORREÇÃO PRINCIPAL (Render / produção)
const ROOT_DIR = path.resolve(__dirname, '..');
const FRONTEND_DIR = path.join(ROOT_DIR, 'frontend');
const AUDIO_DIR = path.join(ROOT_DIR, 'audio');

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
if (fs.existsSync(AUDIO_DIR)) {
  app.use('/audio', express.static(AUDIO_DIR));
} else {
  console.error(`Pasta de áudio não encontrada: ${AUDIO_DIR}`);
}

// ================= PLAYLIST AUTOMÁTICA =================
let PLAYLIST = [];

if (fs.existsSync(AUDIO_DIR)) {
  PLAYLIST = fs.readdirSync(AUDIO_DIR)
    .filter(file => file.toLowerCase().endsWith('.mp3'))
    .sort((a, b) => a.localeCompare(b, 'pt-BR'))
    .map(file => ({
      file,
      title: path.parse(file).name,
      artist: 'Ponto de Umbanda'
    }));

  console.log(`✓ ${PLAYLIST.length} músicas carregadas.`);
} else {
  console.error(`Pasta de áudio não encontrada: ${AUDIO_DIR}`);
}

// ================= CHAT HISTORY =================
const MAX_CHAT_HISTORY = 100;
let chatHistory = [];
let chatUsers = new Map(); // ws -> { name, joinedAt }

function addChatMessage(name, message) {
  const msg = {
    type: 'chat',
    name: name,
    message: message,
    time: Date.now()
  };
  chatHistory.push(msg);
  if (chatHistory.length > MAX_CHAT_HISTORY) {
    chatHistory.shift();
  }
  return msg;
}

function broadcastChat(data, excludeWs = null) {
  const msg = JSON.stringify(data);
  clients.forEach(ws => {
    if (ws !== excludeWs && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  });
}

function getOnlineCount() {
  return chatUsers.size;
}

// ================= STATE =================
let state = {
  listeners: 0,
  peakListeners: 0,
  currentTrack: PLAYLIST[0] || {
    file: '',
    title: 'Nenhuma música',
    artist: 'Ponto de Umbanda'
  },
  playlist: PLAYLIST,
  history: [],
  uptime: 0,
  isLive: true,
  lastUpdate: Date.now()
};

// ================= HISTORY =================
function addToHistory(track) {
  state.history.unshift({
    ...track,
    playedAt: Date.now()
  });

  if (state.history.length > 30) {
    state.history.pop();
  }
}

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

wss.on('connection', (ws) => {
  clients.add(ws);
  state.listeners = clients.size;

  log('info', `Cliente conectado. Total: ${state.listeners}`);

  ws.send(JSON.stringify({ type: 'state', data: state }));
  ws.send(JSON.stringify({ type: 'playlist', data: state.playlist }));

  // Enviar histórico do chat para novo usuário
  if (chatHistory.length > 0) {
    ws.send(JSON.stringify({
      type: 'chat_history',
      data: chatHistory.slice(-20)
    }));
  }

  // Notificar todos sobre novo ouvinte online
  broadcastChat({
    type: 'online_count',
    count: getOnlineCount()
  });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong', time: Date.now() }));
      }

      if (data.type === 'update_metadata') {
        state.currentTrack = {
          title: data.title || 'Desconhecido',
          artist: data.artist || 'Ponto de Umbanda',
          file: data.file || state.currentTrack.file
        };

        state.lastUpdate = Date.now();

        addToHistory(state.currentTrack);

        broadcast({ type: 'metadata', data: state.currentTrack });
      }

      // ===== CHAT AO VIVO =====
      if (data.type === 'chat' && data.name && data.message) {
        const name = String(data.name).trim().substring(0, 20);
        const msgText = String(data.message).trim().substring(0, 200);

        if (name && msgText) {
          // Registrar usuário
          chatUsers.set(ws, { name, joinedAt: Date.now() });

          const chatMsg = addChatMessage(name, msgText);
          broadcastChat(chatMsg);
          log('info', `Chat: ${name}: ${msgText.substring(0, 50)}`);
        }
      }

      if (data.type === 'join_chat' && data.name) {
        const name = String(data.name).trim().substring(0, 20);
        if (name) {
          chatUsers.set(ws, { name, joinedAt: Date.now() });
          broadcastChat({
            type: 'system',
            message: `👋 ${name} entrou no chat`
          });
          broadcastChat({
            type: 'online_count',
            count: getOnlineCount()
          });
        }
      }

    } catch (err) {
      log('error', `WS error: ${err.message}`);
    }
  });

  ws.on('close', () => {
    const userInfo = chatUsers.get(ws);
    clients.delete(ws);
    chatUsers.delete(ws);
    state.listeners = clients.size;

    if (userInfo) {
      broadcastChat({
        type: 'system',
        message: `👋 ${userInfo.name} saiu do chat`
      });
    }

    broadcastChat({
      type: 'online_count',
      count: getOnlineCount()
    });

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

// ================= STREAM REAL (CORRIGIDO) =================
app.get('/stream', (req, res) => {

  if (!state.currentTrack.file) {
    return res.status(404).json({
      error: 'Nenhuma música disponível'
    });
  }

  const filePath = path.join(AUDIO_DIR, state.currentTrack.file);

  if (!fs.existsSync(filePath)) {
    return res.status(404).json({
      error: 'Arquivo não encontrado',
      file: state.currentTrack.file
    });
  }

  res.setHeader('Content-Type', 'audio/mpeg');
  res.setHeader('Accept-Ranges', 'bytes');

  const stream = fs.createReadStream(filePath);

  stream.on('error', (err) => {
    console.error(err);
    if (!res.headersSent) {
      res.status(500).end('Erro ao abrir o áudio');
    }
  });

  stream.pipe(res);
});

// ================= STREAM INFO =================
app.get('/api/stream', (req, res) => {
  const fileUrl = `${req.protocol}://${req.get('host')}/stream`;

  res.json({
    stream: fileUrl,
    title: state.currentTrack.title,
    artist: state.currentTrack.artist
  });
});

// ================= HEALTH CHECK =================
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

// ================= PING WS (ANTI DROP RENDER) =================
setInterval(() => {
  clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    }
  });
}, 25000);

// ================= START =================
console.log('ROOT_DIR:', ROOT_DIR);
console.log('FRONTEND_DIR:', FRONTEND_DIR);
console.log('AUDIO_DIR:', AUDIO_DIR);
console.log('Quantidade de músicas:', PLAYLIST.length);

server.listen(PORT, '0.0.0.0', () => {
  log('info', `Servidor rodando na porta ${PORT}`);
});

module.exports = { app, server, wss };