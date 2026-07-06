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

// ================= CONFIGURAÇÃO DO WEBSOCKET =================
// heartbeatInterval: tempo entre pings do servidor
// heartbeatTimeout: tempo máximo para cliente responder pong
const HEARTBEAT_INTERVAL = 30000;  // 30s
const HEARTBEAT_TIMEOUT = 60000;   // 60s (2x o intervalo)

const wss = new WebSocket.Server({
  server,
  // Permite conexões sem origin (navegadores mobile, extensões, etc.)
  verifyClient: (info, done) => {
    const origin = info.origin;
    // Em produção, aceita origin válido OU sem origin (alguns clientes não enviam)
    if (NODE_ENV === 'production') {
      const allowedOrigins = [
        'https://radioamigosdoseuze.com.br',
        'https://www.radioamigosdoseuze.com.br',
        'http://radioamigosdoseuze.com.br',
        'http://www.radioamigosdoseuze.com.br'
      ];
      // Aceita se origin estiver na lista OU for undefined (clientes locais/teste)
      if (!origin || allowedOrigins.includes(origin)) {
        return done(true);
      }
      console.warn(`WebSocket origin bloqueado: ${origin}`);
      return done(false, 403, 'Origin não autorizado');
    }
    // Desenvolvimento: aceita tudo
    done(true);
  }
});

// ================= CONFIG =================
const PORT = process.env.PORT || 10000;
const NODE_ENV = process.env.NODE_ENV || 'development';

const ROOT_DIR = path.resolve(__dirname, '..');
const FRONTEND_DIR = path.join(ROOT_DIR, 'frontend');
const AUDIO_DIR = path.join(ROOT_DIR, 'audio');

// ================= MIDDLEWARE =================
app.use(cors({
  origin: NODE_ENV === 'production'
    ? ['https://radioamigosdoseuze.com.br', 'https://www.radioamigosdoseuze.com.br']
    : '*',
  credentials: true
}));

app.use(express.json());

// ================= LOG SYSTEM (ASSÍNCRONO) =================
const LOG_DIR = path.join(__dirname, '../logs');
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

const logQueue = [];
let logWriting = false;

function flushLogs() {
  if (logWriting || logQueue.length === 0) return;
  logWriting = true;
  const batch = logQueue.splice(0, logQueue.length);
  const data = batch.join('');
  fs.appendFile(path.join(LOG_DIR, 'server.log'), data, (err) => {
    logWriting = false;
    if (err) console.error('Erro ao escrever log:', err);
    if (logQueue.length > 0) flushLogs();
  });
}

function log(level, message) {
  const line = `[${new Date().toISOString()}] [${level.toUpperCase()}] ${message}`;
  console.log(line);
  logQueue.push(line + '\n');
  flushLogs();
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

// ================= HELPERS DE BROADCAST SEGURO =================
// Envia mensagem WebSocket com tratamento de erro
function safeWsSend(ws, data) {
  try {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(data));
    }
  } catch (err) {
    log('warn', `Erro ao enviar WS: ${err.message}`);
    // Marca como inativo para cleanup
    ws.isAlive = false;
  }
}

function broadcastMetadata(track) {
  const msg = {
    type: 'metadata',
    data: {
      title: track.title,
      artist: track.artist,
      file: track.file
    }
  };
  clients.forEach(ws => safeWsSend(ws, msg));
}

function broadcastState() {
  const track = getCurrentTrack();
  const msg = {
    type: 'state',
    data: {
      listeners: streamClients,
      currentTrack: track || { title: 'Nenhuma música', artist: 'Ponto de Umbanda', file: '' },
      isLive: true,
      uptime: process.uptime()
    }
  };
  clients.forEach(ws => safeWsSend(ws, msg));
}

function broadcastChat(data, excludeWs = null) {
  const msg = { ...data };
  clients.forEach(ws => {
    if (ws !== excludeWs) {
      safeWsSend(ws, msg);
    }
  });
}

// ================= SISTEMA DE STREAMING =================
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
  if (ffmpegProcess) {
    try {
      ffmpegProcess.kill('SIGTERM');
    } catch (e) {
      // já morto
    }
    ffmpegProcess = null;
  }
  if (broadcastStream) {
    try {
      broadcastStream.end();
    } catch (e) {
      // já fechado
    }
    broadcastStream = null;
  }
  if (trackTimeout) {
    clearTimeout(trackTimeout);
    trackTimeout = null;
  }
  log('info', '⏹ Rádio parada');
}

function playNextTrack() {
  if (!isPlaying) return;

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

  // Criar o broadcast stream único
  if (broadcastStream) {
    try { broadcastStream.end(); } catch (e) {}
  }
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

  ffmpegProcess.stderr.on('data', (data) => {
    // FFmpeg loga no stderr; silenciar em produção
  });

  ffmpegProcess.on('error', (err) => {
    log('error', `Erro ao iniciar ffmpeg: ${err.message}`);
    setTimeout(() => playNextTrack(), 2000);
  });

  ffmpegProcess.on('close', (code) => {
    if (isPlaying) {
      if (code !== 0 && code !== null) {
        log('warn', `FFmpeg encerrou com código ${code}`);
      }
      setTimeout(() => playNextTrack(), 1000);
    }
  });

  // Backup timeout
  trackTimeout = setTimeout(() => {
    if (isPlaying && ffmpegProcess) {
      log('warn', `⏭ Timeout atingido para ${track.title}`);
      try { ffmpegProcess.kill('SIGTERM'); } catch (e) {}
    }
  }, currentTrackDuration + 5000);

  // Distribuir dados para clientes com tratamento de erro e backpressure
  broadcastStream.on('data', (chunk) => {
    const deadResponses = [];
    activeResponses.forEach(res => {
      if (res.destroyed || res.writableEnded) {
        deadResponses.push(res);
        return;
      }
      try {
        const ok = res.write(chunk);
        // Se res.write retornar false, buffer está cheio (backpressure)
        // Não fazemos pause aqui pois é stream ao vivo, mas marcamos
        if (!ok) {
          // Opcional: remover clientes lentos
          // deadResponses.push(res);
        }
      } catch (err) {
        log('warn', `Erro ao escrever no stream: ${err.message}`);
        deadResponses.push(res);
      }
    });
    // Limpar responses mortos
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

// ================= CHAT COM RATE LIMITING =================
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

// ================= WEBSOCKET COM HEARTBEAT E CLEANUP =================
const clients = new Set();

wss.on('connection', (ws, req) => {
  // Inicializar heartbeat
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  clients.add(ws);
  log('info', `Cliente WebSocket conectado. Total: ${clients.size}`);

  // Enviar estado atual
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

  broadcastChat({ type: 'online_count', count: getOnlineCount() });

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

  ws.on('close', (code, reason) => {
    const userInfo = chatUsers.get(ws);
    clients.delete(ws);
    chatUsers.delete(ws);

    if (userInfo) {
      broadcastChat({ type: 'system', message: `👋 ${userInfo.name} saiu do chat` });
    }
    broadcastChat({ type: 'online_count', count: getOnlineCount() });

    log('info', `Cliente desconectado (code: ${code}, reason: ${reason?.toString() || 'n/a'}). Total: ${clients.size}`);
  });

  ws.on('error', (err) => {
    log('error', `Erro no WebSocket: ${err.message}`);
    clients.delete(ws);
    chatUsers.delete(ws);
  });
});

// ================= HEARTBEAT: LIMPA CONEXÕES ZUMBIS =================
const heartbeatInterval = setInterval(() => {
  const deadClients = [];
  clients.forEach(ws => {
    if (ws.isAlive === false) {
      deadClients.push(ws);
      return;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch (err) {
      deadClients.push(ws);
    }
  });

  // Fechar e remover zumbis
  deadClients.forEach(ws => {
    log('warn', 'Removendo WebSocket zumbi (não respondeu pong)');
    clients.delete(ws);
    chatUsers.delete(ws);
    try {
      ws.terminate();
    } catch (e) {}
  });

  if (deadClients.length > 0) {
    broadcastChat({ type: 'online_count', count: getOnlineCount() });
  }
}, HEARTBEAT_INTERVAL);

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

  clearInterval(heartbeatInterval);
  stopRadioStream();

  clients.forEach(ws => {
    try {
      ws.close(1001, 'Servidor reiniciando');
    } catch (e) {}
  });

  server.close(() => {
    log('info', 'Servidor HTTP fechado');
    if (logQueue.length > 0) {
      fs.appendFileSync(path.join(LOG_DIR, 'server.log'), logQueue.join(''));
    }
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
  console.log('AUDIO_DIR:', AUDIO_DIR);
  console.log(`Músicas: ${PLAYLIST.length}`);
  console.log('');

  if (PLAYLIST.length > 0) {
    console.log('📋 Playlist:');
    PLAYLIST.forEach((t, i) => {
      console.log(`  ${i + 1}. ${t.title} (${t.duration}s)`);
    });
  }

  server.listen(PORT, '0.0.0.0', () => {
    log('info', `🎵 Rádio rodando na porta ${PORT}`);
    log('info', `📡 Stream: http://localhost:${PORT}/stream`);
    log('info', `💬 Chat ao vivo ativo`);
    log('info', `🎧 A rádio inicia automaticamente quando o primeiro ouvinte conecta`);
  });
})();

module.exports = { app, server, wss };