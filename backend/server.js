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

  console.log(`✓ ${PLAYLIST.length} músicas carregadas.`);
}

// ================= STREAMING - ARQUITETURA CORRIGIDA =================
// CORREÇÃO: Usar um sistema de track explícito em vez de derivar do índice.
// O masterStream é recriado a cada track para garantir MP3 válido.
// Cada cliente novo recebe o stream do INÍCIO da música atual (via buffer circular).

let currentTrackIndex = 0;       // Índice da PRÓXIMA música a tocar
let currentTrack = null;           // Música ATUALMENTE tocando (objeto explícito!)
let isPlaying = false;
let ffmpegProcess = null;
let masterStream = null;           // Stream da música atual
let streamClients = 0;
let activeResponses = new Set();   // Clientes HTTP conectados
let trackStartTime = 0;
let currentTrackDuration = 0;
let trackTimeout = null;
let isTransitioning = false;
let transitionLock = false;        // NOVO: Lock para evitar race conditions
let trackBuffer = [];              // NOVO: Buffer circular da música atual
let maxBufferSize = 500;           // NOVO: ~2MB de buffer (cada chunk ~4KB)
let trackBufferSize = 0;           // NOVO: Tamanho total do buffer em bytes

// ================= FUNÇÕES DE PLAYLIST CORRIGIDAS =================

function getNextTrack() {
  if (PLAYLIST.length === 0) return null;
  const track = PLAYLIST[currentTrackIndex];
  currentTrackIndex = (currentTrackIndex + 1) % PLAYLIST.length;
  return track;
}

function getCurrentTrack() {
  // CORREÇÃO: Retorna o currentTrack explícito, não deriva do índice
  // Isso elimina toda desincronização de metadata
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
  clients.forEach(ws => {
    try {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(msg);
      }
    } catch (err) {}
  });
}

function broadcastMetadata(track) {
  if (!track) return;
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

// ================= SISTEMA DE BUFFER CIRCULAR =================
// NOVO: Buffer para que clientes novos possam "alcançar" a música atual

function addToBuffer(chunk) {
  trackBuffer.push(chunk);
  trackBufferSize += chunk.length;

  while (trackBuffer.length > maxBufferSize && trackBuffer.length > 10) {
    const removed = trackBuffer.shift();
    trackBufferSize -= removed.length;
  }
}

function getBufferSnapshot() {
  return Buffer.concat(trackBuffer);
}

// ================= SISTEMA DE STREAMING CORRIGIDO =================

function initMasterStream() {
  if (masterStream) {
    try {
      masterStream.removeAllListeners('data');
      masterStream.removeAllListeners('error');
      masterStream.removeAllListeners('end');
      masterStream.destroy();
    } catch (e) {}
  }

  masterStream = new PassThrough();
  trackBuffer = [];
  trackBufferSize = 0;

  // Quando o masterStream recebe dados, distribui para todos os clientes HTTP
  masterStream.on('data', (chunk) => {
    addToBuffer(chunk);

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
    log('error', `Erro no masterStream: ${err.message}`);
  });

  masterStream.on('end', () => {
    log('info', 'MasterStream encerrou');
  });

  log('info', 'MasterStream inicializado');
}

function cleanupFfmpeg() {
  if (ffmpegProcess) {
    const oldProcess = ffmpegProcess;
    const oldPid = oldProcess.pid;
    ffmpegProcess = null;

    try {
      oldProcess.stdout.unpipe();
      oldProcess.stdout.removeAllListeners('data');
      oldProcess.stdout.removeAllListeners('error');
      oldProcess.stdout.removeAllListeners('end');
      oldProcess.stderr.removeAllListeners('data');
      oldProcess.removeAllListeners('error');
      oldProcess.removeAllListeners('close');
      oldProcess.removeAllListeners('exit');
      oldProcess.kill('SIGTERM');
      log('info', `FFmpeg PID ${oldPid} recebeu SIGTERM`);
    } catch (e) {
      log('warn', `Erro ao matar ffmpeg PID ${oldPid}: ${e.message}`);
    }

    setTimeout(() => {
      try {
        if (!oldProcess.killed) {
          oldProcess.kill('SIGKILL');
          log('warn', `FFmpeg PID ${oldPid} forçado com SIGKILL`);
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
  currentTrack = null;
  cleanupFfmpeg();

  if (masterStream) {
    try {
      masterStream.end();
      masterStream.destroy();
    } catch (e) {}
    masterStream = null;
  }

  trackBuffer = [];
  trackBufferSize = 0;
  log('info', '⏹ Rádio parada');
}

// ================= FUNÇÃO PRINCIPAL CORRIGIDA =================

function playNextTrack() {
  // CORREÇÃO CRÍTICA: Lock de transição para evitar race conditions
  if (!isPlaying) {
    log('warn', 'playNextTrack chamado mas isPlaying=false');
    return;
  }

  if (transitionLock) {
    log('warn', 'Transição já em andamento (lock ativo), ignorando');
    return;
  }

  transitionLock = true;
  isTransitioning = true;

  log('info', '=== INICIANDO TRANSIÇÃO DE MÚSICA ===');

  // Limpa o ffmpeg anterior
  cleanupFfmpeg();

  const track = getNextTrack();
  if (!track) {
    log('error', 'Nenhuma música para tocar');
    isPlaying = false;
    isTransitioning = false;
    transitionLock = false;
    return;
  }

  if (!fs.existsSync(track.path)) {
    log('error', `Arquivo não encontrado: ${track.path}`);
    isTransitioning = false;
    transitionLock = false;
    setTimeout(() => playNextTrack(), 1000);
    return;
  }

  // CORREÇÃO: Seta currentTrack ANTES de iniciar o ffmpeg
  // Isso garante que TODOS que consultarem getCurrentTrack() verão a música certa
  currentTrack = track;

  log('info', `▶ Tocando: ${track.title} (${track.duration}s)`);
  broadcastMetadata(track);
  broadcastState();

  trackStartTime = Date.now();
  currentTrackDuration = track.duration * 1000;

  // CORREÇÃO CRÍTICA: Recria o masterStream a cada música
  // Isso garante que cada música comece com MP3 válido (headers frescos)
  // e elimina o problema de frames quebrados na transição
  initMasterStream();

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

  const thisFfmpeg = ffmpegProcess;
  const thisTrack = track;
  const thisStartTime = Date.now();

  log('info', `FFmpeg PID ${thisFfmpeg.pid} iniciado para: ${track.title}`);

  // CORREÇÃO: Usar .pipe() normal, mas com handler de cleanup adequado
  thisFfmpeg.stdout.pipe(masterStream, { end: true });

  thisFfmpeg.stdout.on('error', (err) => {
    log('error', `Erro no stdout do ffmpeg: ${err.message}`);
  });

  thisFfmpeg.stderr.on('data', (data) => {
    // Silenciar logs do ffmpeg, mas podemos logar erros se necessário
    const stderr = data.toString();
    if (stderr.includes('Error') || stderr.includes('error')) {
      log('warn', `FFmpeg stderr: ${stderr.substring(0, 200)}`);
    }
  });

  thisFfmpeg.on('error', (err) => {
    log('error', `Erro ao iniciar ffmpeg: ${err.message}`);
    if (isPlaying && ffmpegProcess === thisFfmpeg) {
      isTransitioning = false;
      transitionLock = false;
      setTimeout(() => playNextTrack(), 2000);
    }
  });

  // ================= EVENTO CLOSE DO FFMPEG - CORREÇÃO CRÍTICA =================
  thisFfmpeg.on('close', (code, signal) => {
    log('info', `FFmpeg PID ${thisFfmpeg.pid} encerrou (código: ${code}, sinal: ${signal})`);

    // IMPORTANTE: Só processa se este ffmpeg ainda é o atual
    if (ffmpegProcess !== thisFfmpeg) {
      log('warn', `FFmpeg PID ${thisFfmpeg.pid} não é mais o processo atual, ignorando evento close`);
      return;
    }

    if (!isPlaying) {
      log('info', 'Rádio parada, não avançando');
      isTransitioning = false;
      transitionLock = false;
      return;
    }

    // Se foi encerrado por sinal (cleanup manual), não avança
    if (signal) {
      log('info', `FFmpeg encerrou por sinal ${signal}, não avançando`);
      isTransitioning = false;
      transitionLock = false;
      return;
    }

    // Se encerrou com erro
    if (code !== 0 && code !== null) {
      log('warn', `FFmpeg erro ${code}, tentando próxima em 2s`);
      isTransitioning = false;
      transitionLock = false;
      setTimeout(() => playNextTrack(), 2000);
      return;
    }

    // Verifica se a música realmente terminou
    const elapsed = Date.now() - thisStartTime;
    const minElapsed = Math.max(thisTrack.duration * 1000 * 0.5, 5000);

    if (elapsed < minElapsed) {
      log('warn', `FFmpeg encerrou cedo (${elapsed}ms < ${minElapsed}ms), retry em 2s`);
      isTransitioning = false;
      transitionLock = false;
      setTimeout(() => playNextTrack(), 2000);
      return;
    }

    // Música terminou normalmente → avança para próxima
    log('info', `✅ ${thisTrack.title} terminou (${elapsed}ms). Avançando...`);
    isTransitioning = false;
    transitionLock = false;

    // Delay curto para evitar glitches
    setTimeout(() => {
      if (isPlaying) {
        playNextTrack();
      }
    }, 500);
  });

  // Timeout de segurança - usa o trackStartTime da música atual
  trackTimeout = setTimeout(() => {
    if (!isPlaying) return;

    // Verifica se ainda estamos na mesma música
    if (ffmpegProcess !== thisFfmpeg) {
      log('info', 'Timeout: ffmpeg já foi substituído, ignorando');
      return;
    }

    const elapsed = Date.now() - trackStartTime;

    if (elapsed > currentTrackDuration + 30000) {
      log('warn', `⏭ Timeout: ${thisTrack.title} (${elapsed}ms > ${currentTrackDuration + 30000}ms)`);
      isTransitioning = false;
      transitionLock = false;
      cleanupFfmpeg();
      if (isPlaying) {
        setTimeout(() => playNextTrack(), 500);
      }
    }
  }, currentTrackDuration + 35000);

  // Libera a flag de transição após setup completo
  // Mas MANTÉM o transitionLock até o ffmpeg terminar ou ser substituído
  setTimeout(() => {
    isTransitioning = false;
    log('info', `Transição liberada para: ${thisTrack.title}`);
  }, 1000);
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

      if (data.type === 'join_chat' && data.name) {
        const name = String(data.name).trim().substring(0, 20);
        if (name) {
          chatUsers.set(ws, { name, joinedAt: Date.now(), messageCount: 0, lastMessageTime: 0 });
          broadcast({ type: 'system', message: `👋 ${name} entrou no chat` });
          broadcast({ type: 'online_count', count: getOnlineCount() });
          log('info', `Chat: ${name} entrou`);
        }
        return;
      }

      if (data.type === 'chat' && data.name && data.message) {
        const name = String(data.name).trim().substring(0, 20);
        const msgText = String(data.message).trim().substring(0, 200);

        if (!name || !msgText) {
          safeWsSend(ws, { type: 'system', message: '⚠️ Nome ou mensagem inválidos.' });
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
        broadcast(chatMsg);
        log('info', `Chat: ${name}: ${msgText.substring(0, 50)}`);
        return;
      }

      log('warn', `Mensagem WS desconhecida: ${data.type}`);

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
    log('info', `Cliente desconectado. Total WS: ${clients.size}`);
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

// ================= STREAM (RÁDIO AO VIVO) - CORREÇÃO CRÍTICA =================
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

  const onResError = (err) => {
    log('error', `Erro na resposta do stream: ${err.message}`);
    activeResponses.delete(res);
    streamClients = activeResponses.size;
  };
  res.on('error', onResError);

  activeResponses.add(res);
  streamClients = activeResponses.size;
  broadcastState();

  log('info', `🎧 Cliente conectado ao stream. Total ouvintes: ${streamClients}`);

  // CORREÇÃO CRÍTICA: Envia o buffer da música atual para sincronizar
  // Isso faz com que TODOS os clientes ouçam a MESMA música
  if (masterStream && trackBuffer.length > 0) {
    try {
      const snapshot = getBufferSnapshot();
      // Envia apenas os últimos 500KB para não sobrecarregar
      const recentData = snapshot.slice(-512000);
      res.write(recentData);
      log('info', `Enviados ${recentData.length} bytes de buffer para novo cliente`);
    } catch (e) {
      log('warn', `Erro ao enviar buffer: ${e.message}`);
    }
  }

  // Inicia a rádio se não estiver tocando
  if (!isPlaying) {
    startRadioStream();
  }

  const onReqClose = () => {
    activeResponses.delete(res);
    streamClients = activeResponses.size;
    broadcastState();
    log('info', `🎧 Cliente desconectou. Ouvintes: ${streamClients}`);

    if (streamClients === 0) {
      setTimeout(() => {
        if (activeResponses.size === 0 && isPlaying) {
          stopRadioStream();
          log('info', '⏹ Rádio parada - sem ouvintes');
        }
      }, 30000);
    }
  };

  req.on('close', onReqClose);
  req.on('error', onReqClose);
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
    duration: track ? track.duration : 0,
    transitioning: isTransitioning,
    transitionLocked: transitionLock
  });
});

// ================= LIMPEZA PERIÓDICA =================
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
    log('info', `🧹 Limpados ${dead.length} órfãos. Ouvintes: ${streamClients} (antes: ${before})`);
  }
}, 30000);

setInterval(() => {
  broadcastState();
}, 10000);

// ================= GRACEFUL SHUTDOWN =================
function gracefulShutdown(signal) {
  log('info', `Recebido ${signal}. Encerrando...`);

  stopRadioStream();

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
    console.error('Forçando encerramento');
    process.exit(1);
  }, 10000);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

process.on('uncaughtException', (err) => {
  log('error', `UNCAUGHT EXCEPTION: ${err.message}`);
  log('error', err.stack);
  gracefulShutdown('uncaughtException');
});

process.on('unhandledRejection', (reason, promise) => {
  log('error', `UNHANDLED REJECTION: ${reason}`);
});

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