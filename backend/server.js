const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
const { PassThrough } = require('stream');
require('dotenv').config();

const app = express();
const server = http.createServer(app);

// ================= CONFIG =================
const PORT = process.env.PORT || 10000;
const NODE_ENV = process.env.NODE_ENV || 'development';

const ROOT_DIR = path.resolve(__dirname, '..');
const FRONTEND_DIR = path.join(ROOT_DIR, 'frontend');
const AUDIO_DIR = path.join(ROOT_DIR, 'audio');

// ================= WEBSOCKET =================
const wss = new WebSocket.Server({ server });

// ================= MIDDLEWARE =================
app.use(cors({
  origin: NODE_ENV === 'production'
    ? ['https://radioamigosdoseuze.com.br', 'https://www.radioamigosdoseuze.com.br']
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
  const line = `[${new Date().toISOString()}] [${level.toUpperCase()}] ${message}`;
  console.log(line);
  fs.appendFile(path.join(LOG_DIR, 'server.log'), line + '\n', () => {});
}

// ================= STATIC FILES =================
app.use(express.static(FRONTEND_DIR));
if (fs.existsSync(AUDIO_DIR)) {
  app.use('/audio', express.static(AUDIO_DIR));
} else {
  console.error(`Pasta de áudio não encontrada: ${AUDIO_DIR}`);
}

// ================= METADADOS ID3 =================
let parseFile;
try {
  const mm = require('music-metadata');
  parseFile = mm.parseFile;
} catch {
  parseFile = null;
}

async function getTrackMetadata(filePath) {
  const baseName = path.parse(path.basename(filePath)).name;
  if (parseFile) {
    try {
      const meta = await parseFile(filePath);
      return {
        title: meta.common.title || baseName,
        artist: meta.common.artist || 'Ponto de Umbanda',
        album: meta.common.album || '',
        duration: meta.format.duration || 0
      };
    } catch {
      // fallback
    }
  }
  return {
    title: baseName,
    artist: 'Ponto de Umbanda',
    album: '',
    duration: 0
  };
}

// ================= PLAYLIST =================
let PLAYLIST = [];

async function loadPlaylist() {
  PLAYLIST = [];
  if (!fs.existsSync(AUDIO_DIR)) {
    console.error(`Pasta de áudio não encontrada: ${AUDIO_DIR}`);
    return;
  }

  const files = fs.readdirSync(AUDIO_DIR)
    .filter(file => file.toLowerCase().endsWith('.mp3'))
    .sort((a, b) => a.localeCompare(b, 'pt-BR'));

  for (const file of files) {
    const filePath = path.join(AUDIO_DIR, file);
    const meta = await getTrackMetadata(filePath);
    const stats = fs.statSync(filePath);
    const estimatedDuration = meta.duration > 0
      ? meta.duration
      : Math.ceil(stats.size / 24000);

    PLAYLIST.push({
      file,
      title: meta.title,
      artist: meta.artist,
      album: meta.album,
      path: filePath,
      duration: estimatedDuration,
      size: stats.size
    });
  }

  console.log(`✓ ${PLAYLIST.length} músicas carregadas.`);
}

// ================= STREAMING COM FFMPEG =================
let currentTrackIndex = 0;
let isPlaying = false;
let ffmpegProcess = null;
let broadcastStream = null;
let streamClients = 0;
let activeResponses = new Set();
let trackStartTime = 0;
let currentTrackDuration = 0;
let trackTimeout = null;

function getNextTrack() {
  if (PLAYLIST.length === 0) return null;
  const track = PLAYLIST[currentTrackIndex];
  currentTrackIndex = (currentTrackIndex + 1) % PLAYLIST.length;
  return track;
}

function getCurrentTrack() {
  if (PLAYLIST.length === 0) return null;
  const idx = (currentTrackIndex - 1 + PLAYLIST.length) % PLAYLIST.length;
  return PLAYLIST[idx];
}

// ================= BROADCAST SEGURO =================
function safeWsSend(ws, data) {
  try {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(data));
    }
  } catch (err) {
    // Silencioso
  }
}

function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach(ws => {
    try {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(msg);
      }
    } catch (err) {
      // Silencioso
    }
  });
}

function broadcastMetadata(track) {
  broadcast({
    type: 'metadata',
    data: {
      title: track.title,
      artist: track.artist,
      file: track.file
    }
  });
}

function broadcastState() {
  const track = getCurrentTrack();
  broadcast({
    type: 'state',
    data: {
      listeners: streamClients,
      currentTrack: track || { title: 'Nenhuma música', artist: 'Ponto de Umbanda', file: '' },
      isLive: true,
      uptime: process.uptime()
    }
  });
}

// ================= SISTEMA DE STREAMING =================

// 🔧 NOVO: Limpa recursos do track anterior de forma segura
function cleanupPreviousTrack() {
  if (trackTimeout) {
    clearTimeout(trackTimeout);
    trackTimeout = null;
  }

  if (broadcastStream) {
    if (ffmpegProcess && ffmpegProcess.stdout) {
      try { ffmpegProcess.stdout.unpipe(broadcastStream); } catch (e) {}
    }
    broadcastStream.removeAllListeners('data');
    broadcastStream.removeAllListeners('error');
    try { broadcastStream.end(); } catch (e) {}
    broadcastStream = null;
  }

  if (ffmpegProcess) {
    const oldProcess = ffmpegProcess;
    ffmpegProcess = null;

    try {
      oldProcess.stdout.removeAllListeners('data');
      oldProcess.stdout.removeAllListeners('error');
      oldProcess.stderr.removeAllListeners('data');
      oldProcess.removeAllListeners('error');
      oldProcess.removeAllListeners('close');
      oldProcess.kill('SIGTERM');
    } catch (e) {}

    setTimeout(() => {
      try {
        if (!oldProcess.killed) {
          oldProcess.kill('SIGKILL');
        }
      } catch (e) {}
    }, 3000);
  }
}

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

function stopRadioStream() {
  isPlaying = false;
  cleanupPreviousTrack();
  log('info', '⏹ Rádio parada');
}

function playNextTrack() {
  if (!isPlaying) return;

  // 🔧 NOVO: Sempre limpa o track anterior antes de iniciar o próximo
  cleanupPreviousTrack();

  const track = getNextTrack();
  if (!track) {
    log('error', 'Nenhuma música para tocar');
    isPlaying = false;
    return;
  }

  if (!fs.existsSync(track.path)) {
    log('error', `Arquivo não encontrado: ${track.path}`);
    setTimeout(() => playNextTrack(), 1000);
    return;
  }

  log('info', `▶ Tocando: ${track.title} (${track.duration}s)`);
  broadcastMetadata(track);
  broadcastState();

  trackStartTime = Date.now();
  currentTrackDuration = track.duration * 1000;

  broadcastStream = new PassThrough();

  const ffmpegPath = process.env.FFMPEG_PATH || 'ffmpeg';

  ffmpegProcess = spawn(ffmpegPath, [
    '-re',
    '-i', track.path,
    '-map_metadata', '-1',
    '-acodec', 'libmp3lame',
    '-ab', '128k',
    '-ar', '44100',
    '-ac', '2',
    '-f', 'mp3',
    '-flush_packets', '1',
    'pipe:1'
  ], {
    stdio: ['ignore', 'pipe', 'pipe']
  });

  ffmpegProcess.stdout.pipe(broadcastStream, { end: false });

  ffmpegProcess.stdout.on('error', (err) => {
    log('error', `Erro no stdout do ffmpeg: ${err.message}`);
  });

  ffmpegProcess.stderr.on('data', () => {
    // FFmpeg loga no stderr; silenciar
  });

  ffmpegProcess.on('error', (err) => {
    log('error', `Erro ao iniciar ffmpeg: ${err.message}`);
    if (isPlaying && ffmpegProcess) {
      setTimeout(() => playNextTrack(), 2000);
    }
  });

  ffmpegProcess.on('close', (code, signal) => {
    log('info', `FFmpeg encerrou (código: ${code}, sinal: ${signal})`);

    if (!isPlaying) return;

    // 🔧 CORRIGIDO: Se foi encerrado por sinal (SIGTERM/SIGKILL), não avança
    if (signal) {
      log('info', `FFmpeg encerrou por sinal ${signal}, não avançando track`);
      return;
    }

    // 🔧 CORRIGIDO: Se encerrou com erro, tenta de novo
    if (code !== 0 && code !== null) {
      log('warn', `FFmpeg encerrou com erro ${code}, tentando próxima música em 2s`);
      setTimeout(() => playNextTrack(), 2000);
      return;
    }

    // 🔧 CORRIGIDO: Só avança se a música realmente terminou (tempo decorrido >= 70% da duração)
    const elapsed = Date.now() - trackStartTime;
    const minElapsed = Math.max(currentTrackDuration * 0.7, 5000);

    if (elapsed < minElapsed) {
      log('warn', `FFmpeg encerrou cedo (${elapsed}ms < ${minElapsed}ms), tentando de novo em 2s`);
      setTimeout(() => playNextTrack(), 2000);
      return;
    }

    // Tudo certo, avança para próxima música
    setTimeout(() => playNextTrack(), 1000);
  });

  // 🔧 CORRIGIDO: Timeout mais generoso (duração + 30s)
  trackTimeout = setTimeout(() => {
    if (!isPlaying) return;

    const elapsed = Date.now() - trackStartTime;

    if (elapsed > currentTrackDuration + 30000) {
      log('warn', `⏭ Timeout atingido para ${track.title} (${elapsed}ms > ${currentTrackDuration + 30000}ms)`);
      cleanupPreviousTrack();
      if (isPlaying) {
        setTimeout(() => playNextTrack(), 1000);
      }
    } else {
      log('info', `Timeout cancelado — música ainda dentro do tempo normal`);
    }
  }, currentTrackDuration + 35000);

  // Distribuir dados para clientes
  broadcastStream.on('data', (chunk) => {
    const deadResponses = [];
    activeResponses.forEach(res => {
      if (res.destroyed || res.writableEnded) {
        deadResponses.push(res);
        return;
      }
      try {
        res.write(chunk);
      } catch (err) {
        deadResponses.push(res);
      }
    });
    deadResponses.forEach(res => {
      activeResponses.delete(res);
      try { res.end(); } catch (e) {}
    });
    if (deadResponses.length > 0) {
      streamClients = activeResponses.size;
      broadcastState();
    }
  });

  broadcastStream.on('error', (err) => {
    log('error', `Erro no broadcast stream: ${err.message}`);
  });
}

// ================= CHAT =================
const MAX_CHAT_HISTORY = 100;
let chatHistory = [];
let chatUsers = new Map();
const CHAT_RATE_LIMIT = {
  windowMs: 10000,
  maxMessages: 5
};

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

function getOnlineCount() {
  return chatUsers.size;
}

function checkRateLimit(ws) {
  const user = chatUsers.get(ws);
  if (!user) return false;

  const now = Date.now();
  if (!user.lastMessageTime || (now - user.lastMessageTime) > CHAT_RATE_LIMIT.windowMs) {
    user.messageCount = 1;
    user.lastMessageTime = now;
    return true;
  }

  user.messageCount++;
  if (user.messageCount > CHAT_RATE_LIMIT.maxMessages) {
    return false;
  }
  return true;
}

// ================= WEBSOCKET =================
const clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);
  log('info', `Cliente WebSocket conectado. Total: ${clients.size}`);

  const track = getCurrentTrack();
  safeWsSend(ws, {
    type: 'metadata',
    data: track || { title: 'Iniciando...', artist: 'Ponto de Umbanda', file: '' }
  });

  safeWsSend(ws, {
    type: 'state',
    data: {
      listeners: streamClients,
      currentTrack: track || { title: 'Iniciando...', artist: 'Ponto de Umbanda' },
      isLive: true,
      uptime: process.uptime()
    }
  });

  if (chatHistory.length > 0) {
    safeWsSend(ws, {
      type: 'chat_history',
      data: chatHistory.slice(-30)
    });
  }

  broadcast({ type: 'online_count', count: getOnlineCount() });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.type === 'ping') {
        safeWsSend(ws, { type: 'pong', time: Date.now() });
        return;
      }

      if (data.type === 'chat' && data.name && data.message) {
        const name = String(data.name).trim().substring(0, 20);
        const msgText = String(data.message).trim().substring(0, 200);
        if (name && msgText) {
          chatUsers.set(ws, chatUsers.get(ws) || { name, joinedAt: Date.now() });

          if (!checkRateLimit(ws)) {
            safeWsSend(ws, {
              type: 'system',
              message: '⚠️ Muitas mensagens. Aguarde um pouco.'
            });
            return;
          }

          const chatMsg = addChatMessage(name, msgText);
          broadcast(chatMsg);
          log('info', `Chat: ${name}: ${msgText.substring(0, 50)}`);
        }
      }

      if (data.type === 'join_chat' && data.name) {
        const name = String(data.name).trim().substring(0, 20);
        if (name) {
          chatUsers.set(ws, { name, joinedAt: Date.now() });
          broadcast({ type: 'system', message: `👋 ${name} entrou no chat` });
          broadcast({ type: 'online_count', count: getOnlineCount() });
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
      broadcast({ type: 'system', message: `👋 ${userInfo.name} saiu do chat` });
    }
    broadcast({ type: 'online_count', count: getOnlineCount() });

    log('info', `Cliente desconectado. Total: ${clients.size}`);
  });

  ws.on('error', (err) => {
    log('error', `Erro no WebSocket: ${err.message}`);
    clients.delete(ws);
    chatUsers.delete(ws);
  });
});

// ================= PING WS =================
setInterval(() => {
  clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    }
  });
}, 25000);

// ================= API ROUTES =================
app.get('/api/playlist', (req, res) => {
  res.json(PLAYLIST.map(t => ({
    file: t.file,
    title: t.title,
    artist: t.artist,
    duration: t.duration
  })));
});

app.get('/api/status', (req, res) => {
  const track = getCurrentTrack();
  const elapsed = trackStartTime > 0 ? Math.floor((Date.now() - trackStartTime) / 1000) : 0;
  res.json({
    listeners: streamClients,
    currentTrack: track || { title: 'Nenhuma música', artist: 'Ponto de Umbanda' },
    isLive: true,
    uptime: process.uptime(),
    playlistSize: PLAYLIST.length,
    currentIndex: currentTrackIndex,
    elapsed: elapsed,
    duration: track ? track.duration : 0
  });
});

app.get('/api/history', (req, res) => {
  res.json(chatHistory.slice(0, 30));
});

// ================= STREAM (RÁDIO AO VIVO) =================
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

  activeResponses.add(res);
  streamClients = activeResponses.size;
  broadcastState();

  log('info', `🎧 Cliente conectado ao stream. Total ouvintes: ${streamClients}`);

  if (!isPlaying) {
    startRadioStream();
  }

  req.on('close', () => {
    activeResponses.delete(res);
    streamClients = activeResponses.size;
    broadcastState();
    log('info', `🎧 Cliente desconectou do stream. Ouvintes: ${streamClients}`);

    if (streamClients === 0) {
      setTimeout(() => {
        if (activeResponses.size === 0 && isPlaying) {
          stopRadioStream();
          log('info', '⏹ Rádio parada - sem ouvintes');
        }
      }, 30000);
    }
  });

  req.on('error', () => {
    activeResponses.delete(res);
    streamClients = activeResponses.size;
  });

  res.on('error', (err) => {
    log('error', `Erro no response do stream: ${err.message}`);
    activeResponses.delete(res);
    streamClients = activeResponses.size;
  });
});

app.get('/api/stream', (req, res) => {
  const track = getCurrentTrack();
  res.json({
    stream: `${req.protocol}://${req.get('host')}/stream`,
    title: track ? track.title : 'Nenhuma música',
    artist: track ? track.artist : 'Ponto de Umbanda',
    listeners: streamClients
  });
});

app.get('/api/health', (req, res) => {
  const track = getCurrentTrack();
  const elapsed = trackStartTime > 0 ? Math.floor((Date.now() - trackStartTime) / 1000) : 0;
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    wsClients: clients.size,
    streamClients: streamClients,
    currentTrack: track || { title: 'Nenhuma música', artist: 'Ponto de Umbanda' },
    playlistSize: PLAYLIST.length,
    isPlaying: isPlaying,
    streaming: isPlaying,
    elapsed: elapsed,
    duration: track ? track.duration : 0
  });
});

// ================= LOOPS =================
setInterval(() => {
  broadcastState();
}, 10000);

// ================= GRACEFUL SHUTDOWN =================
function gracefulShutdown(signal) {
  log('info', `Recebido ${signal}. Encerrando graciosamente...`);

  stopRadioStream();

  clients.forEach(ws => {
    try { ws.close(1001, 'Servidor reiniciando'); } catch (e) {}
  });

  server.close(() => {
    log('info', 'Servidor HTTP fechado');
    process.exit(0);
  });

  setTimeout(() => {
    console.error('Forçando encerramento após timeout');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// ================= START =================
(async () => {
  await loadPlaylist();

  console.log('========================================');
  console.log('🎵 RÁDIO AMIGOS DO SEU ZÉ');
  console.log('========================================');
  console.log('ROOT_DIR:', ROOT_DIR);
  console.log('FRONTEND_DIR:', FRONTEND_DIR);
  console.log('AUDIO_DIR:', AUDIO_DIR);
  console.log('Audio existe?', fs.existsSync(AUDIO_DIR));
  console.log(`Músicas: ${PLAYLIST.length}`);
  console.log('');

  if (PLAYLIST.length > 0) {
    console.log('📋 Playlist:');
    PLAYLIST.forEach((t, i) => {
      console.log(`  ${i + 1}. ${t.title} (${t.duration}s)`);
    });
  }

  if (fs.existsSync(AUDIO_DIR)) {
    console.log('Arquivos:', fs.readdirSync(AUDIO_DIR));
  }

  server.listen(PORT, '0.0.0.0', () => {
    log('info', `🎵 Rádio rodando na porta ${PORT}`);
    log('info', `📡 Stream: http://localhost:${PORT}/stream`);
    log('info', `💬 Chat ao vivo ativo`);
    log('info', `🎧 A rádio inicia automaticamente quando o primeiro ouvinte conecta`);
  });
})();

module.exports = { app, server, wss };