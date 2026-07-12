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
  console.error(`Pasta de audio nao encontrada: ${AUDIO_DIR}`);
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
    console.error(`Pasta de audio nao encontrada: ${AUDIO_DIR}`);
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
      : Math.ceil(stats.size / 16000);

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

  console.log(`✓ ${PLAYLIST.length} musicas carregadas.`);
}

// ================= SISTEMA DE OUVINTES =================
const listeners = new Map();
const wsToSession = new Map();
const resToSession = new Map();

let peakListeners = 0;
let dailyUniqueListeners = new Set();

const LISTENER_TIMEOUT_MS = 30000;
const CLEANUP_INTERVAL_MS = 15000;

function generateSessionId() {
  return `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
}

function getTodayKey() {
  return new Date().toISOString().split('T')[0];
}

function loadDailyStats() {
  const statsFile = path.join(LOG_DIR, 'daily_stats.json');
  try {
    if (fs.existsSync(statsFile)) {
      const data = JSON.parse(fs.readFileSync(statsFile, 'utf8'));
      const today = getTodayKey();
      if (data.date === today) {
        dailyUniqueListeners = new Set(data.uniqueListeners || []);
        peakListeners = data.peakListeners || 0;
        log('info', `Estatisticas do dia carregadas: ${dailyUniqueListeners.size} unicos, pico ${peakListeners}`);
      } else {
        dailyUniqueListeners.clear();
        peakListeners = 0;
        saveDailyStats();
      }
    }
  } catch (e) {
    log('warn', `Erro ao carregar estatisticas: ${e.message}`);
  }
}

function saveDailyStats() {
  const statsFile = path.join(LOG_DIR, 'daily_stats.json');
  try {
    fs.writeFileSync(statsFile, JSON.stringify({
      date: getTodayKey(),
      uniqueListeners: Array.from(dailyUniqueListeners),
      peakListeners: peakListeners,
      savedAt: new Date().toISOString()
    }, null, 2));
  } catch (e) {
    log('warn', `Erro ao salvar estatisticas: ${e.message}`);
  }
}

function addListener(sessionId, data) {
  if (listeners.has(sessionId)) {
    log('warn', `SessionId duplicado detectado: ${sessionId.substring(0, 8)}...`);
    removeListener(sessionId, 'duplicate_session');
  }

  const now = Date.now();
  listeners.set(sessionId, {
    sessionId,
    ws: data.ws || null,
    res: data.res || null,
    ip: data.ip || 'unknown',
    userAgent: data.userAgent || 'unknown',
    connectedAt: now,
    lastPing: now,
    isPlaying: true
  });

  dailyUniqueListeners.add(sessionId);

  const currentCount = listeners.size;
  if (currentCount > peakListeners) {
    peakListeners = currentCount;
    log('info', `🏆 Novo pico de ouvintes: ${peakListeners}`);
  }

  saveDailyStats();
  log('info', `👤 Ouvinte conectado: ${sessionId.substring(0, 8)}... (total: ${currentCount})`);
  broadcastState();
  return true;
}

function removeListener(sessionId, reason = 'unknown') {
  const listener = listeners.get(sessionId);
  if (!listener) return false;

  if (listener.res && !listener.res.destroyed) {
    try {
      listener.res.end();
    } catch (e) {}
  }
  if (listener.ws && listener.ws.readyState === WebSocket.OPEN) {
    try {
      listener.ws.close(1001, 'Removido: ' + reason);
    } catch (e) {}
  }

  if (listener.ws) wsToSession.delete(listener.ws);
  if (listener.res) resToSession.delete(listener.res);
  listeners.delete(sessionId);

  log('info', `👋 Ouvinte desconectado: ${sessionId.substring(0, 8)}... (razao: ${reason}, restantes: ${listeners.size})`);
  broadcastState();
  return true;
}

function updateListenerPing(sessionId) {
  const listener = listeners.get(sessionId);
  if (listener) {
    listener.lastPing = Date.now();
    return true;
  }
  return false;
}

function getActiveListenersCount() {
  return listeners.size;
}

function getListenerStats() {
  return {
    active: listeners.size,
    peak: peakListeners,
    dailyUnique: dailyUniqueListeners.size,
    today: getTodayKey()
  };
}

// ================= LIMPEZA AUTOMATICA =================
function cleanupDeadListeners() {
  const now = Date.now();
  let removed = 0;

  for (const [sessionId, listener] of listeners) {
    if (listener.res) {
      const resDead = listener.res.destroyed || listener.res.writableEnded || listener.res.writableFinished;
      if (resDead) {
        removeListener(sessionId, 'dead_http');
        removed++;
        continue;
      }
    }

    if (listener.ws) {
      if (listener.ws.readyState === WebSocket.CLOSED || listener.ws.readyState === WebSocket.CLOSING) {
        removeListener(sessionId, 'dead_ws');
        removed++;
        continue;
      }
    }

    if (now - listener.lastPing > LISTENER_TIMEOUT_MS) {
      removeListener(sessionId, 'timeout');
      removed++;
      continue;
    }
  }

  if (removed > 0) {
    log('info', `🧹 Limpeza: ${removed} ouvintes removidos. Ativos: ${listeners.size}`);
  }
}

setInterval(cleanupDeadListeners, CLEANUP_INTERVAL_MS);

// ============================================================
// STREAMING BROADCAST - RADIO AO VIVO REAL
// ============================================================
// Uma unica transmissao continua. Todos os ouvintes recebem
// exatamente o mesmo audio no mesmo ponto do tempo.
// ============================================================

let currentTrackIndex = 0;
let isPlaying = false;
let ffmpegProcess = null;
let masterStream = null;
let trackStartTime = 0;
let currentTrackDuration = 0;
let trackTimeout = null;
let isTransitioning = false;
let currentTrack = null;

function getNextTrack() {
  if (PLAYLIST.length === 0) return null;
  const track = PLAYLIST[currentTrackIndex];
  currentTrackIndex = (currentTrackIndex + 1) % PLAYLIST.length;
  return track;
}

function getCurrentTrack() {
  return currentTrack;
}

// ================= BROADCAST SEGURO =================
const clients = new Set();

function safeWsSend(ws, data) {
  try {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(data));
    }
  } catch (err) {}
}

function broadcast(data) {
  const msg = JSON.stringify(data);
  let sent = 0;
  clients.forEach(ws => {
    try {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(msg);
        sent++;
      }
    } catch (err) {}
  });
  return sent;
}

function broadcastMetadata(track) {
  const sent = broadcast({
    type: 'metadata',
    data: {
      title: track.title,
      artist: track.artist,
      file: track.file
    }
  });
  log('info', `📡 Metadata broadcast: "${track.title}" para ${sent} clientes WS`);
}

function broadcastState() {
  const track = getCurrentTrack();
  const stats = getListenerStats();
  const elapsed = trackStartTime > 0 ? Math.floor((Date.now() - trackStartTime) / 1000) : 0;
  const sent = broadcast({
    type: 'state',
    data: {
      listeners: stats.active,
      peakListeners: stats.peak,
      dailyUnique: stats.dailyUnique,
      currentTrack: track || { title: 'Nenhuma musica', artist: 'Ponto de Umbanda', file: '' },
      isLive: true,
      uptime: process.uptime(),
      elapsed: elapsed,
      duration: track ? track.duration : 0
    }
  });
}

// ================= MASTER STREAM =================

function initMasterStream() {
  if (masterStream) {
    log('info', 'MasterStream ja existe, reutilizando');
    return;
  }

  masterStream = new PassThrough();

  masterStream.on('data', (chunk) => {
    const deadSessions = [];
    for (const [sessionId, listener] of listeners) {
      if (listener.res) {
        if (listener.res.destroyed || listener.res.writableEnded || listener.res.writableFinished) {
          deadSessions.push(sessionId);
          continue;
        }
        try {
          const ok = listener.res.write(chunk);
          if (ok === false) {
            listener.res.once('drain', () => {});
          }
        } catch (err) {
          deadSessions.push(sessionId);
        }
      }
    }
    deadSessions.forEach(sid => removeListener(sid, 'stream_error'));
  });

  masterStream.on('error', (err) => {
    log('error', `Erro no masterStream: ${err.message}`);
    masterStream = null;
    setTimeout(() => initMasterStream(), 500);
  });

  masterStream.on('end', () => {
    log('info', 'MasterStream encerrou');
    masterStream = null;
  });

  log('info', 'MasterStream inicializado');
}

function destroyMasterStream() {
  if (!masterStream) return;
  try {
    masterStream.removeAllListeners('data');
    masterStream.removeAllListeners('error');
    masterStream.removeAllListeners('end');
    masterStream.destroy();
  } catch (e) {}
  masterStream = null;
  log('info', 'MasterStream destruido');
}

// ================= FFMPEG MANAGEMENT =================

function cleanupFfmpeg() {
  if (ffmpegProcess) {
    const oldProcess = ffmpegProcess;
    ffmpegProcess = null;

    try {
      if (masterStream && oldProcess.stdout) {
        oldProcess.stdout.unpipe(masterStream);
      }
    } catch (e) {}

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

  if (trackTimeout) {
    clearTimeout(trackTimeout);
    trackTimeout = null;
  }
}

// ================= RADIO ENGINE =================

function startRadioStream() {
  if (PLAYLIST.length === 0) {
    log('error', 'Nenhuma musica na playlist');
    return;
  }
  if (isPlaying) {
    log('warn', 'Radio ja esta tocando - ignorando startRadioStream()');
    return;
  }

  isPlaying = true;
  initMasterStream();
  log('info', '🎵 Iniciando streaming da radio ao vivo...');
  playNextTrack();
}

function stopRadioStream() {
  log('info', '⏹ Parando radio...');
  isPlaying = false;
  cleanupFfmpeg();
  destroyMasterStream();
  currentTrack = null;
  log('info', '⏹ Radio parada');
}

function playNextTrack() {
  if (!isPlaying) {
    log('warn', 'playNextTrack chamado mas isPlaying=false');
    return;
  }
  if (isTransitioning) {
    log('warn', 'Transicao ja em andamento, ignorando playNextTrack()');
    return;
  }

  log('info', '🔄 Iniciando transicao para proxima musica...');
  isTransitioning = true;
  cleanupFfmpeg();

  // Delay para garantir limpeza completa do ffmpeg anterior
  setTimeout(() => {
    _doPlayNextTrack();
  }, 300);
}

function _doPlayNextTrack() {
  if (!isPlaying) {
    log('warn', '_doPlayNextTrack: isPlaying=false, cancelando');
    isTransitioning = false;
    return;
  }

  const track = getNextTrack();
  if (!track) {
    log('error', 'Nenhuma musica para tocar');
    isPlaying = false;
    isTransitioning = false;
    return;
  }

  if (!fs.existsSync(track.path)) {
    log('error', `Arquivo nao encontrado: ${track.path}`);
    isTransitioning = false;
    setTimeout(() => playNextTrack(), 1000);
    return;
  }

  currentTrack = track;

  log('info', `▶ Tocando: ${track.title} (${track.duration}s)`);
  broadcastMetadata(track);
  broadcastState();

  trackStartTime = Date.now();
  currentTrackDuration = track.duration * 1000;

  if (!masterStream) {
    log('warn', 'MasterStream nulo, reinicializando...');
    initMasterStream();
  }

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
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: false
  });

  const thisTrack = track;
  const thisFfmpeg = ffmpegProcess;

  log('info', `🎬 FFmpeg iniciado PID=${ffmpegProcess.pid} para "${thisTrack.title}"`);

  ffmpegProcess.stdout.pipe(masterStream, { end: false });

  ffmpegProcess.stdout.on('error', (err) => {
    log('error', `Erro no stdout do ffmpeg: ${err.message}`);
  });

  ffmpegProcess.stderr.on('data', (data) => {
    // Silenciar logs do ffmpeg
  });

  ffmpegProcess.on('error', (err) => {
    log('error', `Erro ao iniciar ffmpeg: ${err.message}`);
    if (isPlaying && ffmpegProcess === thisFfmpeg) {
      isTransitioning = false;
      setTimeout(() => playNextTrack(), 2000);
    }
  });

  ffmpegProcess.on('close', (code, signal) => {
    log('info', `FFmpeg encerrou (codigo: ${code}, sinal: ${signal}) - musica: ${thisTrack.title}`);

    if (!isPlaying) {
      log('info', 'isPlaying=false, ignorando close do ffmpeg');
      isTransitioning = false;
      return;
    }

    // Ignora eventos de ffmpeg antigo
    if (ffmpegProcess !== thisFfmpeg && ffmpegProcess !== null) {
      log('info', `Ignorando close de ffmpeg antigo (${thisTrack.title})`);
      return;
    }

    // Se foi morto por sinal do cleanupFfmpeg
    if (signal) {
      const elapsed = Date.now() - trackStartTime;
      const minElapsed = Math.max(currentTrackDuration * 0.5, 3000);

      if (elapsed >= minElapsed) {
        log('info', `FFmpeg encerrou por sinal ${signal}, mas musica parece completa (${elapsed}ms). Avancando...`);
        isTransitioning = false;
        setTimeout(() => playNextTrack(), 500);
      } else {
        log('info', `FFmpeg encerrou por sinal ${signal}, nao avancando (${elapsed}ms < ${minElapsed}ms)`);
        isTransitioning = false;
      }
      return;
    }

    if (code !== 0 && code !== null) {
      log('warn', `FFmpeg erro ${code}, tentando proxima em 2s`);
      isTransitioning = false;
      setTimeout(() => playNextTrack(), 2000);
      return;
    }

    const elapsed = Date.now() - trackStartTime;
    const minElapsed = Math.max(currentTrackDuration * 0.5, 3000);

    if (elapsed < minElapsed) {
      log('warn', `FFmpeg encerrou cedo (${elapsed}ms < ${minElapsed}ms), retry em 2s`);
      isTransitioning = false;
      setTimeout(() => playNextTrack(), 2000);
      return;
    }

    log('info', `✅ ${thisTrack.title} terminou. Avancando...`);
    isTransitioning = false;
    setTimeout(() => playNextTrack(), 500);
  });

  // Timeout de seguranca
  const timeoutDelay = Math.min(currentTrackDuration + 30000, Math.max(currentTrackDuration + 10000, 300000));
  trackTimeout = setTimeout(() => {
    if (!isPlaying) return;
    const elapsed = Date.now() - trackStartTime;
    if (elapsed > currentTrackDuration + 30000) {
      log('warn', `⏭ Timeout: ${thisTrack.title} (${elapsed}ms)`);
      if (ffmpegProcess === thisFfmpeg) {
        isTransitioning = false;
        cleanupFfmpeg();
        if (isPlaying) {
          setTimeout(() => playNextTrack(), 500);
        }
      }
    }
  }, timeoutDelay);

  // Libera isTransitioning apos confirmar que ffmpeg esta rodando
  setTimeout(() => {
    if (ffmpegProcess === thisFfmpeg && isPlaying) {
      isTransitioning = false;
      log('info', `✅ Transicao completa: "${thisTrack.title}" tocando normalmente`);
    }
  }, 1500);
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
wss.on('connection', (ws, req) => {
  clients.add(ws);
  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const userAgent = req.headers['user-agent'] || 'unknown';

  const wsSessionId = generateSessionId();
  wsToSession.set(ws, wsSessionId);

  addListener(wsSessionId, {
    ws: ws,
    ip: clientIp,
    userAgent: userAgent
  });

  log('info', `🌐 WS Cliente conectado. Total WS: ${clients.size} | IP: ${clientIp}`);

  const track = getCurrentTrack();
  const elapsed = trackStartTime > 0 ? Math.floor((Date.now() - trackStartTime) / 1000) : 0;

  // Envia metadata atual imediatamente
  safeWsSend(ws, {
    type: 'metadata',
    data: track || { title: 'Iniciando...', artist: 'Ponto de Umbanda', file: '' }
  });

  // Envia estado atual imediatamente
  safeWsSend(ws, {
    type: 'state',
    data: {
      listeners: getActiveListenersCount(),
      peakListeners: peakListeners,
      dailyUnique: dailyUniqueListeners.size,
      currentTrack: track || { title: 'Iniciando...', artist: 'Ponto de Umbanda' },
      isLive: true,
      uptime: process.uptime(),
      elapsed: elapsed,
      duration: track ? track.duration : 0
    }
  });

  // Envia historico do chat
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

      // Heartbeat do ouvinte
      if (data.type === 'listener_ping' && data.sessionId) {
        updateListenerPing(data.sessionId);
        safeWsSend(ws, { type: 'listener_pong', time: Date.now() });
        return;
      }

      // Registro de ouvinte (compatibilidade)
      if (data.type === 'listener_join' && data.sessionId) {
        if (data.sessionId !== wsSessionId) {
          removeListener(wsSessionId, 'session_update');
          wsToSession.set(ws, data.sessionId);
          addListener(data.sessionId, {
            ws: ws,
            ip: clientIp,
            userAgent: userAgent
          });
        }
        safeWsSend(ws, {
          type: 'listener_confirmed',
          sessionId: data.sessionId || wsSessionId,
          stats: getListenerStats()
        });
        return;
      }

      if (data.type === 'ping') {
        safeWsSend(ws, { type: 'pong', time: Date.now() });
        return;
      }

      // ===== CHAT - COMPLETAMENTE ISOLADO DA RADIO =====
      if (data.type === 'join_chat' && data.name) {
        const name = String(data.name).trim().substring(0, 20);
        if (name) {
          chatUsers.set(ws, { name, joinedAt: Date.now(), messageCount: 0, lastMessageTime: 0 });
          broadcast({ type: 'system', message: `👋 ${name} entrou no chat` });
          broadcast({ type: 'online_count', count: getOnlineCount() });
          log('info', `💬 Chat: ${name} entrou`);
        }
        return;
      }

      if (data.type === 'chat' && data.name && data.message) {
        const name = String(data.name).trim().substring(0, 20);
        const msgText = String(data.message).trim().substring(0, 200);

        if (!name || !msgText) {
          safeWsSend(ws, { type: 'system', message: '⚠️ Nome ou mensagem invalidos.' });
          return;
        }

        let user = chatUsers.get(ws);
        if (!user) {
          user = { name, joinedAt: Date.now(), messageCount: 0, lastMessageTime: 0 };
          chatUsers.set(ws, user);
          broadcast({ type: 'system', message: `👋 ${name} entrou no chat` });
          broadcast({ type: 'online_count', count: getOnlineCount() });
        }

        if (!checkRateLimit(ws)) {
          safeWsSend(ws, {
            type: 'system',
            message: '⚠️ Muitas mensagens. Aguarde um pouco.'
          });
          return;
        }

        const chatMsg = addChatMessage(name, msgText);
        const sent = broadcast(chatMsg);
        log('info', `💬 Chat: ${name}: ${msgText.substring(0, 50)} (enviado para ${sent} clientes)`);
        return;
      }

      log('warn', `Mensagem WS desconhecida: ${data.type}`);

    } catch (err) {
      log('error', `WS error: ${err.message}`);
    }
  });

  ws.on('close', () => {
    const userInfo = chatUsers.get(ws);
    const sessionId = wsToSession.get(ws);

    clients.delete(ws);
    chatUsers.delete(ws);

    if (sessionId) {
      removeListener(sessionId, 'ws_close');
      wsToSession.delete(ws);
    }

    if (userInfo) {
      broadcast({ type: 'system', message: `👋 ${userInfo.name} saiu do chat` });
    }
    broadcast({ type: 'online_count', count: getOnlineCount() });
    log('info', `🌐 WS Cliente desconectado. Total WS: ${clients.size}`);
  });

  ws.on('error', (err) => {
    log('error', `Erro no WebSocket: ${err.message}`);
    const sessionId = wsToSession.get(ws);
    if (sessionId) {
      removeListener(sessionId, 'ws_error');
    }
    clients.delete(ws);
    chatUsers.delete(ws);
    wsToSession.delete(ws);
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
  const stats = getListenerStats();
  res.json({
    listeners: stats.active,
    peakListeners: stats.peak,
    dailyUnique: stats.dailyUnique,
    currentTrack: track || { title: 'Nenhuma musica', artist: 'Ponto de Umbanda' },
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

// ============================================================
// ROTA /stream - STREAMING BROADCAST AO VIVO
// ============================================================
// CRITICO: Todos os ouvintes recebem o MESMO audio do
// masterStream. Nunca cria stream por ouvinte.
// ============================================================

app.get('/stream', (req, res) => {
  if (PLAYLIST.length === 0) {
    return res.status(404).json({ error: 'Nenhuma musica disponivel' });
  }

  const sessionId = req.query.sid || generateSessionId();
  const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const userAgent = req.headers['user-agent'] || 'unknown';

  // Headers CRITICOS para streaming ao vivo sem cache
  res.setHeader('Content-Type', 'audio/mpeg');
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate, private');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '-1');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Accept-Ranges', 'none');
  res.setHeader('X-Content-Type-Options', 'nosniff');

  // Headers Icecast/Shoutcast
  res.setHeader('icy-name', 'Radio Amigos do Seu Ze');
  res.setHeader('icy-genre', 'Ponto de Umbanda');
  res.setHeader('icy-br', '128');
  res.setHeader('icy-sr', '44100');
  res.setHeader('icy-ch', '2');
  res.setHeader('icy-pub', '1');
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Envia headers imediatamente
  res.flushHeaders();

  // Registra ouvinte
  resToSession.set(res, sessionId);
  addListener(sessionId, {
    res: res,
    ip: clientIp,
    userAgent: userAgent
  });

  log('info', `🎧 Stream ouvinte conectado: ${sessionId.substring(0, 8)}... (total: ${getActiveListenersCount()})`);

  // Garante que o masterStream existe
  if (!masterStream) {
    log('info', 'MasterStream nulo, inicializando...');
    initMasterStream();
  }

  // Inicia a radio se nao estiver tocando
  if (!isPlaying) {
    log('info', 'Radio parada, iniciando...');
    startRadioStream();
  } else {
    log('info', `Radio ja tocando. Ouvinte recebera do ponto atual: "${currentTrack ? currentTrack.title : 'desconhecida'}"`);
  }

  // Handlers de desconexao
  const onReqClose = () => {
    removeListener(sessionId, 'client_disconnect');
    log('info', `🎧 Stream ouvinte desconectou: ${sessionId.substring(0, 8)}... (restam: ${getActiveListenersCount()})`);

    // Se nao houver mais ouvintes, para a radio apos 30s
    if (getActiveListenersCount() === 0) {
      log('info', 'Nenhum ouvinte restante. Parando radio em 30s...');
      setTimeout(() => {
        if (getActiveListenersCount() === 0 && isPlaying) {
          stopRadioStream();
          log('info', '⏹ Radio parada - sem ouvintes');
        }
      }, 30000);
    }
  };

  req.on('close', onReqClose);
  req.on('error', onReqClose);
  req.on('aborted', onReqClose);

  res.on('error', (err) => {
    log('error', `Erro na resposta do stream: ${err.message}`);
    removeListener(sessionId, 'stream_error');
  });
});

app.get('/api/stream', (req, res) => {
  const track = getCurrentTrack();
  const stats = getListenerStats();
  res.json({
    stream: `${req.protocol}://${req.get('host')}/stream`,
    title: track ? track.title : 'Nenhuma musica',
    artist: track ? track.artist : 'Ponto de Umbanda',
    listeners: stats.active,
    peakListeners: stats.peak,
    dailyUnique: stats.dailyUnique
  });
});

app.get('/api/health', (req, res) => {
  const track = getCurrentTrack();
  const elapsed = trackStartTime > 0 ? Math.floor((Date.now() - trackStartTime) / 1000) : 0;
  const stats = getListenerStats();
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    wsClients: clients.size,
    streamClients: stats.active,
    peakListeners: stats.peak,
    dailyUnique: stats.dailyUnique,
    currentTrack: track || { title: 'Nenhuma musica', artist: 'Ponto de Umbanda' },
    playlistSize: PLAYLIST.length,
    isPlaying: isPlaying,
    streaming: isPlaying,
    elapsed: elapsed,
    duration: track ? track.duration : 0
  });
});

// ================= LIMPEZA PERIODICA =================
setInterval(() => {
  cleanupDeadListeners();
}, CLEANUP_INTERVAL_MS);

setInterval(() => {
  broadcastState();
}, 10000);

setInterval(() => {
  saveDailyStats();
}, 60000);

// ================= GRACEFUL SHUTDOWN =================
function gracefulShutdown(signal) {
  log('info', `Recebido ${signal}. Encerrando...`);

  saveDailyStats();
  stopRadioStream();

  if (masterStream) {
    try { masterStream.end(); } catch (e) {}
    masterStream = null;
  }

  for (const [sessionId] of listeners) {
    removeListener(sessionId, 'shutdown');
  }

  clients.forEach(ws => {
    try { ws.close(1001, 'Servidor reiniciando'); } catch (e) {}
  });

  server.close(() => {
    log('info', 'Servidor HTTP fechado');
    process.exit(0);
  });

  setTimeout(() => {
    console.error('Forcando encerramento');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

process.on('uncaughtException', (err) => {
  log('error', `UNCAUGHT EXCEPTION: ${err.message}`);
  log('error', err.stack);
  saveDailyStats();
  gracefulShutdown('uncaughtException');
});

process.on('unhandledRejection', (reason, promise) => {
  log('error', `UNHANDLED REJECTION: ${reason}`);
});

// ================= START =================
(async () => {
  loadDailyStats();
  await loadPlaylist();

  console.log('========================================');
  console.log('🎵 RADIO AMIGOS DO SEU ZE');
  console.log('========================================');
  console.log('ROOT_DIR:', ROOT_DIR);
  console.log('FRONTEND_DIR:', FRONTEND_DIR);
  console.log('AUDIO_DIR:', AUDIO_DIR);
  console.log('Audio existe?', fs.existsSync(AUDIO_DIR));
  console.log(`Musicas: ${PLAYLIST.length}`);
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
    log('info', `🎵 Radio rodando na porta ${PORT}`);
    log('info', `📡 Stream: http://localhost:${PORT}/stream`);
    log('info', `💬 Chat ao vivo ativo`);
    log('info', `👥 Sistema de ouvintes: ativo`);
    log('info', `🎧 Todos os ouvintes recebem o MESMO audio em tempo real`);
  });
})();

module.exports = { app, server, wss };