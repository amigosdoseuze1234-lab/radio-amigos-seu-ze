const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// ================= CONFIG =================
const PORT = process.env.PORT || 10000;
const NODE_ENV = process.env.NODE_ENV || 'development';

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

// ================= PLAYLIST =================
let PLAYLIST = [];

if (fs.existsSync(AUDIO_DIR)) {
  PLAYLIST = fs.readdirSync(AUDIO_DIR)
    .filter(file => file.toLowerCase().endsWith('.mp3'))
    .sort((a, b) => a.localeCompare(b, 'pt-BR'))
    .map(file => ({
      file,
      title: path.parse(file).name,
      artist: 'Ponto de Umbanda',
      path: path.join(AUDIO_DIR, file)
    }));

  console.log(`✓ ${PLAYLIST.length} músicas carregadas.`);
} else {
  console.error(`Pasta de áudio não encontrada: ${AUDIO_DIR}`);
}

// ================= STREAMING CONTÍNUO - TOCA TODAS AS MÚSICAS =================
let currentTrackIndex = 0;
let activeStreams = new Set(); // Conjunto de streams ativos (res.write)
let isPlaying = false;
let ffmpegProcess = null;
let streamClients = 0;

function getNextTrack() {
  if (PLAYLIST.length === 0) return null;
  const track = PLAYLIST[currentTrackIndex];
  currentTrackIndex = (currentTrackIndex + 1) % PLAYLIST.length;
  return track;
}

function getCurrentTrack() {
  if (PLAYLIST.length === 0) return null;
  // Retorna a música anterior (a que está tocando agora)
  const idx = (currentTrackIndex - 1 + PLAYLIST.length) % PLAYLIST.length;
  return PLAYLIST[idx];
}

function broadcastMetadata(track) {
  const msg = JSON.stringify({
    type: 'metadata',
    data: {
      title: track.title,
      artist: track.artist,
      file: track.file
    }
  });

  clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  });
}

function broadcastState() {
  const track = getCurrentTrack();
  const msg = JSON.stringify({
    type: 'state',
    data: {
      listeners: streamClients,
      currentTrack: track || { title: 'Nenhuma música', artist: 'Ponto de Umbanda', file: '' },
      isLive: true,
      uptime: process.uptime()
    }
  });

  clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  });
}

// ================= SISTEMA DE STREAMING COM FFMPEG/FFMPEG-STATIC =================
// Toca música por música, avançando automaticamente

function startRadioStream() {
  if (PLAYLIST.length === 0) {
    log('error', 'Nenhuma música na playlist');
    return;
  }

  if (isPlaying) {
    log('warn', 'Rádio já está tocando');
    return;
  }

  isPlaying = true;
  log('info', '🎵 Iniciando streaming da rádio...');
  playNextTrack();
}

function playNextTrack() {
  if (!isPlaying) return;

  const track = getNextTrack();
  if (!track) {
    log('error', 'Nenhuma música para tocar');
    isPlaying = false;
    return;
  }

  log('info', `▶ Tocando: ${track.title}`);
  broadcastMetadata(track);
  broadcastState();

  // Usar ffmpeg para transcodificar e enviar para todos os clientes conectados
  // Se não tiver ffmpeg, usa leitura direta do arquivo
  streamTrackToClients(track);
}

function streamTrackToClients(track) {
  if (!fs.existsSync(track.path)) {
    log('error', `Arquivo não encontrado: ${track.path}`);
    setTimeout(() => playNextTrack(), 1000);
    return;
  }

  // Obter duração do MP3 (aproximada pelo tamanho do arquivo)
  const stats = fs.statSync(track.path);
  // MP3 a ~128kbps = ~16KB/s. Duração ≈ tamanho / 16000
  const estimatedDuration = Math.ceil(stats.size / 16000) * 1000;
  const duration = Math.max(estimatedDuration, 30000); // Mínimo 30s

  log('info', `📀 ${track.title} - duração estimada: ${Math.floor(duration/1000)}s`);

  // Criar readable stream do arquivo
  const fileStream = fs.createReadStream(track.path);

  // Enviar para todos os clientes conectados
  activeStreams.forEach(res => {
    if (!res.destroyed) {
      fileStream.pipe(res, { end: false });
    }
  });

  fileStream.on('end', () => {
    log('info', `⏭ ${track.title} terminou. Próxima música...`);
    // Pequeno delay entre músicas (crossfade de 2 segundos)
    setTimeout(() => playNextTrack(), 2000);
  });

  fileStream.on('error', (err) => {
    log('error', `Erro no stream: ${err.message}`);
    setTimeout(() => playNextTrack(), 2000);
  });

  // Backup: se o stream travar, força próxima música após duração + 5s
  setTimeout(() => {
    if (isPlaying) {
      fileStream.destroy();
      playNextTrack();
    }
  }, duration + 5000);
}

// ================= CHAT =================
const MAX_CHAT_HISTORY = 100;
let chatHistory = [];
let chatUsers = new Map();

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

// ================= WEBSOCKET =================
const clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);

  log('info', `Cliente WebSocket conectado. Total: ${clients.size}`);

  // Enviar estado atual
  const track = getCurrentTrack();
  ws.send(JSON.stringify({
    type: 'metadata',
    data: track || { title: 'Iniciando...', artist: 'Ponto de Umbanda', file: '' }
  }));

  ws.send(JSON.stringify({
    type: 'state',
    data: {
      listeners: streamClients,
      currentTrack: track || { title: 'Iniciando...', artist: 'Ponto de Umbanda' },
      isLive: true,
      uptime: process.uptime()
    }
  }));

  // Enviar histórico do chat
  if (chatHistory.length > 0) {
    ws.send(JSON.stringify({
      type: 'chat_history',
      data: chatHistory.slice(-30)
    }));
  }

  broadcastChat({ type: 'online_count', count: getOnlineCount() });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong', time: Date.now() }));
      }

      // ===== CHAT =====
      if (data.type === 'chat' && data.name && data.message) {
        const name = String(data.name).trim().substring(0, 20);
        const msgText = String(data.message).trim().substring(0, 200);
        if (name && msgText) {
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
          broadcastChat({ type: 'system', message: `👋 ${name} entrou no chat` });
          broadcastChat({ type: 'online_count', count: getOnlineCount() });
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

    if (userInfo) {
      broadcastChat({ type: 'system', message: `👋 ${userInfo.name} saiu do chat` });
    }
    broadcastChat({ type: 'online_count', count: getOnlineCount() });

    log('info', `Cliente desconectado. Total: ${clients.size}`);
  });
});

// ================= API ROUTES =================
app.get('/api/playlist', (req, res) => {
  res.json(PLAYLIST.map(t => ({ file: t.file, title: t.title, artist: t.artist })));
});

app.get('/api/status', (req, res) => {
  const track = getCurrentTrack();
  res.json({
    listeners: streamClients,
    currentTrack: track || { title: 'Nenhuma música', artist: 'Ponto de Umbanda' },
    isLive: true,
    uptime: process.uptime(),
    playlistSize: PLAYLIST.length,
    currentIndex: currentTrackIndex
  });
});

// ================= STREAM (RÁDIO AO VIVO - TOCA TODAS) =================
// Endpoint que conecta o cliente ao stream contínuo da rádio
app.get('/stream', (req, res) => {
  if (PLAYLIST.length === 0) {
    return res.status(404).json({ error: 'Nenhuma música disponível' });
  }

  res.setHeader('Content-Type', 'audio/mpeg');
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('icy-name', 'Rádio Amigos do Seu Zé');
  res.setHeader('icy-genre', 'Ponto de Umbanda');

  // Adicionar este response ao conjunto de streams ativos
  activeStreams.add(res);
  streamClients = activeStreams.size;
  broadcastState();

  log('info', `🎧 Cliente conectado ao stream. Total ouvintes: ${streamClients}`);

  // Se a rádio ainda não está tocando, inicia
  if (!isPlaying) {
    startRadioStream();
  } else {
    // Se já está tocando, envia a música atual imediatamente
    const track = getCurrentTrack();
    if (track && fs.existsSync(track.path)) {
      const fileStream = fs.createReadStream(track.path);
      fileStream.pipe(res, { end: false });

      fileStream.on('error', () => {
        if (!res.destroyed) res.end();
      });
    }
  }

  // Quando o cliente desconectar
  req.on('close', () => {
    activeStreams.delete(res);
    streamClients = activeStreams.size;
    broadcastState();
    log('info', `🎧 Cliente desconectou do stream. Ouvintes: ${streamClients}`);

    // Se não houver mais ouvintes, para a rádio após 30s
    if (streamClients === 0) {
      setTimeout(() => {
        if (streamClients === 0 && isPlaying) {
          isPlaying = false;
          log('info', '⏹ Rádio parada - sem ouvintes');
        }
      }, 30000);
    }
  });

  req.on('error', () => {
    activeStreams.delete(res);
    streamClients = activeStreams.size;
  });
});

// ================= STREAM INFO =================
app.get('/api/stream', (req, res) => {
  const track = getCurrentTrack();
  res.json({
    stream: `${req.protocol}://${req.get('host')}/stream`,
    title: track ? track.title : 'Nenhuma música',
    artist: track ? track.artist : 'Ponto de Umbanda',
    listeners: streamClients
  });
});

// ================= HEALTH CHECK =================
app.get('/api/health', (req, res) => {
  const track = getCurrentTrack();
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    wsClients: clients.size,
    streamClients: streamClients,
    currentTrack: track || { title: 'Nenhuma música', artist: 'Ponto de Umbanda' },
    playlistSize: PLAYLIST.length,
    isPlaying: isPlaying,
    streaming: true
  });
});

// ================= LOOPS =================
// Envia estado periodicamente
setInterval(() => {
  broadcastState();
}, 10000);

// Ping WebSocket para manter conexão viva
setInterval(() => {
  clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    }
  });
}, 25000);

// ================= START =================
console.log('========================================');
console.log('🎵 RÁDIO AMIGOS DO SEU ZÉ');
console.log('========================================');
console.log('ROOT_DIR:', ROOT_DIR);
console.log('AUDIO_DIR:', AUDIO_DIR);
console.log(`Músicas: ${PLAYLIST.length}`);
console.log('');

if (PLAYLIST.length > 0) {
  console.log('📋 Playlist:');
  PLAYLIST.forEach((t, i) => {
    console.log(`  ${i + 1}. ${t.title}`);
  });
}

server.listen(PORT, '0.0.0.0', () => {
  log('info', `🎵 Rádio rodando na porta ${PORT}`);
  log('info', `📡 Stream: http://localhost:${PORT}/stream`);
  log('info', `💬 Chat ao vivo ativo`);
  log('info', `🎧 A rádio inicia automaticamente quando o primeiro ouvinte conecta`);
});

module.exports = { app, server, wss };