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
  const line = '[' + new Date().toISOString() + '] [' + level.toUpperCase() + '] ' + message;
  console.log(line);
  fs.appendFile(path.join(LOG_DIR, 'server.log'), line + '\n', () => {});
}

// ================= STATIC FILES =================
app.use(express.static(FRONTEND_DIR));
if (fs.existsSync(AUDIO_DIR)) {
  app.use('/audio', express.static(AUDIO_DIR));
} else {
  console.error('Pasta de audio nao encontrada: ' + AUDIO_DIR);
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
    console.error('Pasta de audio nao encontrada: ' + AUDIO_DIR);
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

  console.log('\u2713 ' + PLAYLIST.length + ' musicas carregadas.');
}

// ================= STREAMING - ARQUITETURA GLOBAL STREAM =================
let currentTrackIndex = 0;
let isPlaying = false;
let ffmpegProcess = null;
let masterStream = null;
let streamClients = 0;
let activeResponses = new Set();
let trackStartTime = 0;
let currentTrackDuration = 0;
let trackTimeout = null;
let isTransitioning = false;

// Estado global compartilhado para sincronizacao perfeita
let globalState = {
  currentTrack: null,
  trackStartTime: 0,
  isPlaying: false,
  elapsed: 0
};

function getCurrentTrack() {
  if (PLAYLIST.length === 0) return null;
  const idx = (currentTrackIndex - 1 + PLAYLIST.length) % PLAYLIST.length;
  return PLAYLIST[idx];
}

function getNextTrack() {
  if (PLAYLIST.length === 0) return null;
  const track = PLAYLIST[currentTrackIndex];
  currentTrackIndex = (currentTrackIndex + 1) % PLAYLIST.length;
  return track;
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
  clients.forEach(ws => {
    try {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(msg);
      }
    } catch (err) {}
  });
}

function broadcastMetadata(track) {
  globalState.currentTrack = track;
  globalState.trackStartTime = Date.now();

  broadcast({
    type: 'metadata',
    data: {
      title: track.title,
      artist: track.artist,
      file: track.file,
      duration: track.duration,
      startTime: globalState.trackStartTime
    }
  });
}

function broadcastState() {
  const track = globalState.currentTrack || getCurrentTrack();
  const elapsed = globalState.trackStartTime > 0
    ? Math.floor((Date.now() - globalState.trackStartTime) / 1000)
    : 0;

  broadcast({
    type: 'state',
    data: {
      listeners: streamClients,
      currentTrack: track || { title: 'Nenhuma musica', artist: 'Ponto de Umbanda', file: '' },
      isLive: true,
      uptime: process.uptime(),
      elapsed: Math.min(elapsed, track ? track.duration : 0),
      duration: track ? track.duration : 0,
      trackStartTime: globalState.trackStartTime
    }
  });
}

// ================= SISTEMA DE STREAMING CORRIGIDO =================

function initMasterStream() {
  if (masterStream) {
    try {
      masterStream.removeAllListeners('data');
      masterStream.removeAllListeners('error');
      masterStream.end();
    } catch (e) {}
    masterStream = null;
  }

  masterStream = new PassThrough();

  masterStream.on('data', (chunk) => {
    const deadResponses = [];
    activeResponses.forEach(res => {
      if (res.destroyed || res.writableEnded || res.writableFinished) {
        deadResponses.push(res);
        return;
      }
      try {
        const ok = res.write(chunk);
        if (ok === false) {
          res.once('drain', () => {});
        }
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

  masterStream.on('error', (err) => {
    log('error', 'Erro no masterStream: ' + err.message);
    masterStream = null;
  });

  log('info', 'MasterStream inicializado');
}

function cleanupFfmpeg() {
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

  if (trackTimeout) {
    clearTimeout(trackTimeout);
    trackTimeout = null;
  }
}

function startRadioStream() {
  if (PLAYLIST.length === 0) {
    log('error', 'Nenhuma musica na playlist');
    return;
  }
  if (isPlaying) {
    log('warn', 'Radio ja esta tocando');
    return;
  }

  isPlaying = true;
  globalState.isPlaying = true;
  initMasterStream();
  log('info', '\uD83C\uDFB5 Iniciando streaming da radio...');
  playNextTrack();
}

function stopRadioStream() {
  isPlaying = false;
  globalState.isPlaying = false;
  cleanupFfmpeg();
  if (masterStream) {
    try { masterStream.end(); } catch (e) {}
    masterStream = null;
  }
  log('info', '\u23F9 Radio parada');
}

// ================= PLAYNEXTTRACK - CORRECAO CRITICA =================
// CORRECAO: Sincroniza metadata SOMENTE quando o ffmpeg realmente
// comeca a emitir dados. Usa buffer de sincronizacao.

function playNextTrack() {
  if (!isPlaying) return;
  if (isTransitioning) {
    log('warn', 'Transicao ja em andamento, ignorando');
    return;
  }

  isTransitioning = true;
  cleanupFfmpeg();

  const track = getNextTrack();
  if (!track) {
    log('error', 'Nenhuma musica para tocar');
    isPlaying = false;
    isTransitioning = false;
    return;
  }

  if (!fs.existsSync(track.path)) {
    log('error', 'Arquivo nao encontrado: ' + track.path);
    isTransitioning = false;
    setTimeout(() => playNextTrack(), 1000);
    return;
  }

  log('info', '\u25B6 Preparando: ' + track.title + ' (' + track.duration + 's)');

  // NAO seta trackStartTime ainda!
  currentTrackDuration = track.duration * 1000;

  if (!masterStream) {
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
    stdio: ['ignore', 'pipe', 'pipe']
  });

  // CORRECAO CRITICA: Buffer de sincronizacao
  let firstChunkReceived = false;
  let syncBuffer = [];
  const SYNC_BUFFER_SIZE = 5; // ~5 chunks para estabilizar

  ffmpegProcess.stdout.on('data', (chunk) => {
    if (!firstChunkReceived) {
      firstChunkReceived = true;
      // AGORA sim setamos o trackStartTime quando o audio REALMENTE comeca
      globalState.currentTrack = track;
      globalState.trackStartTime = Date.now();
      trackStartTime = Date.now();

      log('info', '\u25B6\u25B6 Tocando: ' + track.title + ' (iniciado em ' + new Date().toISOString() + ')');

      broadcastMetadata(track);
      broadcastState();
    }

    // Buffer de sincronizacao
    if (syncBuffer.length < SYNC_BUFFER_SIZE) {
      syncBuffer.push(chunk);
      return;
    }

    if (syncBuffer.length === SYNC_BUFFER_SIZE) {
      const bufferedData = Buffer.concat(syncBuffer);
      syncBuffer = null;
      masterStream.write(bufferedData);
    }

    masterStream.write(chunk);
  });

  ffmpegProcess.stdout.on('error', (err) => {
    log('error', 'Erro no stdout do ffmpeg: ' + err.message);
  });

  ffmpegProcess.stderr.on('data', (data) => {
    // Silenciar logs do ffmpeg
  });

  ffmpegProcess.on('error', (err) => {
    log('error', 'Erro ao iniciar ffmpeg: ' + err.message);
    if (isPlaying) {
      isTransitioning = false;
      setTimeout(() => playNextTrack(), 2000);
    }
  });

  ffmpegProcess.on('close', (code, signal) => {
    log('info', 'FFmpeg encerrou (codigo: ' + code + ', sinal: ' + signal + ')');

    if (!isPlaying) {
      isTransitioning = false;
      return;
    }

    if (signal) {
      log('info', 'FFmpeg encerrou por sinal ' + signal + ', nao avancando');
      isTransitioning = false;
      return;
    }

    if (code !== 0 && code !== null) {
      log('warn', 'FFmpeg erro ' + code + ', tentando proxima em 2s');
      isTransitioning = false;
      setTimeout(() => playNextTrack(), 2000);
      return;
    }

    const elapsed = Date.now() - trackStartTime;
    const minElapsed = Math.max(currentTrackDuration * 0.5, 3000);

    if (elapsed < minElapsed) {
      log('warn', 'FFmpeg encerrou cedo (' + elapsed + 'ms < ' + minElapsed + 'ms), retry em 2s');
      isTransitioning = false;
      setTimeout(() => playNextTrack(), 2000);
      return;
    }

    log('info', '\u2705 ' + track.title + ' terminou. Avancando...');
    isTransitioning = false;
    setTimeout(() => playNextTrack(), 500);
  });

  trackTimeout = setTimeout(() => {
    if (!isPlaying) return;
    const elapsed = Date.now() - trackStartTime;
    if (elapsed > currentTrackDuration + 60000) {
      log('warn', '\u23ED Timeout: ' + track.title + ' (' + elapsed + 'ms)');
      isTransitioning = false;
      cleanupFfmpeg();
      if (isPlaying) {
        setTimeout(() => playNextTrack(), 500);
      }
    }
  }, currentTrackDuration + 65000);

  setTimeout(() => {
    isTransitioning = false;
  }, 500);
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
wss.on('connection', (ws) => {
  clients.add(ws);
  log('info', 'Cliente WebSocket conectado. Total: ' + clients.size);

  // Envia estado atual com elapsed CORRETO
  const track = globalState.currentTrack;
  const elapsed = globalState.trackStartTime > 0
    ? Math.floor((Date.now() - globalState.trackStartTime) / 1000)
    : 0;

  if (track && isPlaying) {
    safeWsSend(ws, {
      type: 'metadata',
      data: {
        title: track.title,
        artist: track.artist,
        file: track.file,
        duration: track.duration,
        startTime: globalState.trackStartTime,
        elapsed: Math.min(elapsed, track.duration)
      }
    });
  } else {
    safeWsSend(ws, {
      type: 'metadata',
      data: {
        title: 'Iniciando...',
        artist: 'Ponto de Umbanda',
        file: '',
        duration: 0,
        startTime: 0,
        elapsed: 0
      }
    });
  }

  safeWsSend(ws, {
    type: 'state',
    data: {
      listeners: streamClients,
      currentTrack: track || { title: 'Iniciando...', artist: 'Ponto de Umbanda', file: '' },
      isLive: true,
      uptime: process.uptime(),
      elapsed: track ? Math.min(elapsed, track.duration) : 0,
      duration: track ? track.duration : 0,
      trackStartTime: globalState.trackStartTime
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

      if (data.type === 'listener_ping') {
        const track = globalState.currentTrack;
        const elapsed = globalState.trackStartTime > 0
          ? Math.floor((Date.now() - globalState.trackStartTime) / 1000)
          : 0;

        safeWsSend(ws, {
          type: 'listener_pong',
          time: Date.now(),
          listeners: streamClients,
          trackStartTime: globalState.trackStartTime,
          elapsed: track ? Math.min(elapsed, track.duration) : 0,
          duration: track ? track.duration : 0,
          isPlaying: isPlaying,
          currentTrack: track ? track.title : 'Iniciando...'
        });
        return;
      }

      if (data.type === 'join_chat' && data.name) {
        const name = String(data.name).trim().substring(0, 20);
        if (name) {
          chatUsers.set(ws, { name, joinedAt: Date.now(), messageCount: 0, lastMessageTime: 0 });
          broadcast({ type: 'system', message: '\uD83D\uDC4B ' + name + ' entrou no chat' });
          broadcast({ type: 'online_count', count: getOnlineCount() });
          log('info', 'Chat: ' + name + ' entrou');
        }
        return;
      }

      if (data.type === 'chat' && data.name && data.message) {
        const name = String(data.name).trim().substring(0, 20);
        const msgText = String(data.message).trim().substring(0, 200);

        if (!name || !msgText) {
          safeWsSend(ws, { type: 'system', message: '\u26A0\uFE0F Nome ou mensagem invalidos.' });
          return;
        }

        let user = chatUsers.get(ws);
        if (!user) {
          user = { name, joinedAt: Date.now(), messageCount: 0, lastMessageTime: 0 };
          chatUsers.set(ws, user);
          broadcast({ type: 'system', message: '\uD83D\uDC4B ' + name + ' entrou no chat' });
          broadcast({ type: 'online_count', count: getOnlineCount() });
        }

        if (!checkRateLimit(ws)) {
          safeWsSend(ws, {
            type: 'system',
            message: '\u26A0\uFE0F Muitas mensagens. Aguarde um pouco.'
          });
          return;
        }

        const chatMsg = addChatMessage(name, msgText);
        broadcast(chatMsg);
        log('info', 'Chat: ' + name + ': ' + msgText.substring(0, 50));
        return;
      }

      log('warn', 'Mensagem WS desconhecida: ' + data.type);

    } catch (err) {
      log('error', 'WS error: ' + err.message);
    }
  });

  ws.on('close', () => {
    const userInfo = chatUsers.get(ws);
    clients.delete(ws);
    chatUsers.delete(ws);

    if (userInfo) {
      broadcast({ type: 'system', message: '\uD83D\uDC4B ' + userInfo.name + ' saiu do chat' });
    }
    broadcast({ type: 'online_count', count: getOnlineCount() });
    log('info', 'Cliente desconectado. Total WS: ' + clients.size);
  });

  ws.on('error', (err) => {
    log('error', 'Erro no WebSocket: ' + err.message);
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
  const track = globalState.currentTrack;
  const elapsed = globalState.trackStartTime > 0
    ? Math.floor((Date.now() - globalState.trackStartTime) / 1000)
    : 0;
  res.json({
    listeners: streamClients,
    currentTrack: track || { title: 'Nenhuma musica', artist: 'Ponto de Umbanda' },
    isLive: true,
    uptime: process.uptime(),
    playlistSize: PLAYLIST.length,
    currentIndex: currentTrackIndex,
    elapsed: track ? Math.min(elapsed, track.duration) : 0,
    duration: track ? track.duration : 0,
    trackStartTime: globalState.trackStartTime,
    isPlaying: isPlaying
  });
});

app.get('/api/history', (req, res) => {
  res.json(chatHistory.slice(0, 30));
});

// ================= STREAM (RADIO AO VIVO) - CORRECAO CRITICA =================
// CORRECAO: Quando o ffmpeg reinicia (troca de musica), o stream cai.
// Novos clientes que conectam nesse gap ficam esperando dados que nunca chegam.
// SOLUCAO: Se nao ha ffmpeg ativo, iniciar imediatamente.

app.get('/stream', (req, res) => {
  if (PLAYLIST.length === 0) {
    return res.status(404).json({ error: 'Nenhuma musica disponivel' });
  }

  res.setHeader('Content-Type', 'audio/mpeg');
  res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('icy-name', 'Radio Amigos do Seu Ze');
  res.setHeader('icy-genre', 'Ponto de Umbanda');
  // CORRECAO: Headers para evitar que o browser trate como arquivo finito
  res.setHeader('Accept-Ranges', 'none');
  res.setHeader('Content-Transfer-Encoding', 'binary');

  const onResError = (err) => {
    log('error', 'Erro na resposta do stream: ' + err.message);
    activeResponses.delete(res);
    streamClients = activeResponses.size;
  };
  res.on('error', onResError);

  activeResponses.add(res);
  streamClients = activeResponses.size;
  broadcastState();

  log('info', '\uD83C\uDFA7 Cliente conectado ao stream. Total ouvintes: ' + streamClients);

  // CORRECAO CRITICA: Se nao ha masterStream ou ffmpeg ativo, reinicia
  if (!masterStream || !ffmpegProcess) {
    log('info', 'Stream inativo - reiniciando radio...');
    initMasterStream();
    if (!isPlaying) {
      startRadioStream();
    }
  }

  // Se o masterStream existe, conecta o cliente
  if (masterStream) {
    const dataHandler = (chunk) => {
      if (res.destroyed || res.writableEnded) {
        masterStream.removeListener('data', dataHandler);
        return;
      }
      try {
        res.write(chunk);
      } catch (e) {
        masterStream.removeListener('data', dataHandler);
      }
    };
    masterStream.on('data', dataHandler);
  }

  const onReqClose = () => {
    activeResponses.delete(res);
    streamClients = activeResponses.size;
    broadcastState();
    log('info', '\uD83C\uDFA7 Cliente desconectou. Ouvintes: ' + streamClients);

    if (streamClients === 0) {
      setTimeout(() => {
        if (activeResponses.size === 0 && isPlaying) {
          stopRadioStream();
          log('info', '\u23F9 Radio parada - sem ouvintes');
        }
      }, 30000);
    }
  };

  req.on('close', onReqClose);
  req.on('error', onReqClose);
});

app.get('/api/stream', (req, res) => {
  const track = globalState.currentTrack;
  const elapsed = globalState.trackStartTime > 0
    ? Math.floor((Date.now() - globalState.trackStartTime) / 1000)
    : 0;
  res.json({
    stream: req.protocol + '://' + req.get('host') + '/stream',
    title: track ? track.title : 'Nenhuma musica',
    artist: track ? track.artist : 'Ponto de Umbanda',
    listeners: streamClients,
    trackStartTime: globalState.trackStartTime,
    elapsed: track ? Math.min(elapsed, track.duration) : 0,
    duration: track ? track.duration : 0,
    isPlaying: isPlaying
  });
});

app.get('/api/health', (req, res) => {
  const track = globalState.currentTrack;
  const elapsed = globalState.trackStartTime > 0
    ? Math.floor((Date.now() - globalState.trackStartTime) / 1000)
    : 0;
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    wsClients: clients.size,
    streamClients: streamClients,
    currentTrack: track || { title: 'Nenhuma musica', artist: 'Ponto de Umbanda' },
    playlistSize: PLAYLIST.length,
    isPlaying: isPlaying,
    streaming: isPlaying,
    elapsed: track ? Math.min(elapsed, track.duration) : 0,
    duration: track ? track.duration : 0,
    trackStartTime: globalState.trackStartTime
  });
});

// ================= LIMPEZA PERIODICA =================
setInterval(() => {
  const before = activeResponses.size;
  const dead = [];
  activeResponses.forEach(res => {
    if (res.destroyed || res.writableEnded || res.writableFinished) {
      dead.push(res);
    }
  });
  dead.forEach(res => activeResponses.delete(res));
  if (dead.length > 0) {
    streamClients = activeResponses.size;
    broadcastState();
    log('info', '\uD83E\uDDF9 Limpados ' + dead.length + ' orfaos. Ouvintes: ' + streamClients + ' (antes: ' + before + ')');
  }
}, 30000);

setInterval(() => {
  broadcastState();
}, 3000);

// ================= GRACEFUL SHUTDOWN =================
function gracefulShutdown(signal) {
  log('info', 'Recebido ' + signal + '. Encerrando...');

  stopRadioStream();

  if (masterStream) {
    try { masterStream.end(); } catch (e) {}
    masterStream = null;
  }

  clients.forEach(ws => {
    try { ws.close(1001, 'Servidor reiniciando'); } catch (e) {}
  });

  activeResponses.forEach(res => {
    try { res.end(); } catch (e) {}
  });
  activeResponses.clear();

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
  log('error', 'UNCAUGHT EXCEPTION: ' + err.message);
  log('error', err.stack);
  gracefulShutdown('uncaughtException');
});

process.on('unhandledRejection', (reason, promise) => {
  log('error', 'UNHANDLED REJECTION: ' + reason);
});

// ================= START =================
(async () => {
  await loadPlaylist();

  console.log('========================================');
  console.log('\uD83C\uDFB5 RADIO AMIGOS DO SEU ZE');
  console.log('========================================');
  console.log('ROOT_DIR:', ROOT_DIR);
  console.log('FRONTEND_DIR:', FRONTEND_DIR);
  console.log('AUDIO_DIR:', AUDIO_DIR);
  console.log('Audio existe?', fs.existsSync(AUDIO_DIR));
  console.log('Musicas: ' + PLAYLIST.length);
  console.log('');

  if (PLAYLIST.length > 0) {
    console.log('\uD83D\uDCCB Playlist:');
    PLAYLIST.forEach((t, i) => {
      console.log('  ' + (i + 1) + '. ' + t.title + ' (' + t.duration + 's)');
    });
  }

  if (fs.existsSync(AUDIO_DIR)) {
    console.log('Arquivos:', fs.readdirSync(AUDIO_DIR));
  }

  server.listen(PORT, '0.0.0.0', () => {
    log('info', '\uD83C\uDFB5 Radio rodando na porta ' + PORT);
    log('info', '\uD83D\uDCE1 Stream: http://localhost:' + PORT + '/stream');
    log('info', '\uD83D\uDCAC Chat ao vivo ativo');
    log('info', '\uD83C\uDFA7 A radio inicia automaticamente quando o primeiro ouvinte conecta');
  });
})();

module.exports = { app, server, wss };