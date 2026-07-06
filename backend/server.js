const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const { spawn } = require('child_process');
const { pipeline, PassThrough } = require('stream');
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

// ================= METADADOS ID3 (opcional, requer music-metadata) =================
// Se music-metadata estiver instalado, usa ele; senão, fallback para nome do arquivo
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
    // Duração estimada se não tiver ID3: bitrate médio ~192kbps = 24KB/s
    const estimatedDuration = meta.duration > 0
      ? meta.duration
      : Math.ceil(stats.size / 24000);

    PLAYLIST.push({
      file,
      title: meta.title,
      artist: meta.artist,
      album: meta.album,
      path: filePath,
      duration: estimatedDuration, // em segundos
      size: stats.size
    });
  }

  console.log(`✓ ${PLAYLIST.length} músicas carregadas.`);
}

// ================= STREAMING CONTÍNUO COM FFMPEG =================
let currentTrackIndex = 0;
let isPlaying = false;
let ffmpegProcess = null;
let broadcastStream = null; // PassThrough único que alimenta TODOS os clientes
let streamClients = 0;
let activeResponses = new Set(); // Set de res.write() ativos
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

// ================= SISTEMA DE STREAMING COM FFMPEG =================
// Usa UM único ffmpeg que transcodifica e envia para um PassThrough
// Todos os clientes leem desse mesmo PassThrough (rádio ao vivo real)

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
    ffmpegProcess.kill('SIGTERM');
    ffmpegProcess = null;
  }
  if (broadcastStream) {
    broadcastStream.end();
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
    broadcastStream.end();
  }
  broadcastStream = new PassThrough();

  // Detectar ffmpeg disponível
  const ffmpegPath = process.env.FFMPEG_PATH || 'ffmpeg';

  // FFmpeg: transcodifica para MP3 estéreo 128kbps, 44100Hz
  // Isso garante compatibilidade com todos os players
  ffmpegProcess = spawn(ffmpegPath, [
    '-re',                    // Leitura em tempo real (não acelerada)
    '-i', track.path,         // Input
    '-map_metadata', '-1',    // Remove metadados ID3 do stream
    '-acodec', 'libmp3lame',  // Codec MP3
    '-ab', '128k',            // Bitrate 128kbps
    '-ar', '44100',           // Sample rate 44100Hz
    '-ac', '2',               // Estéreo
    '-f', 'mp3',              // Formato de saída MP3
    '-flush_packets', '1',    // Flush imediato
    'pipe:1'                  // Saída para stdout
  ], {
    stdio: ['ignore', 'pipe', 'pipe']
  });

  // Pipe do ffmpeg stdout -> broadcastStream
  ffmpegProcess.stdout.pipe(broadcastStream, { end: false });

  ffmpegProcess.stdout.on('error', (err) => {
    log('error', `Erro no stdout do ffmpeg: ${err.message}`);
  });

  ffmpegProcess.stderr.on('data', (data) => {
    // FFmpeg loga no stderr; ignorar ou logar em debug
    // log('debug', `FFmpeg: ${data.toString().trim()}`);
  });

  ffmpegProcess.on('error', (err) => {
    log('error', `Erro ao iniciar ffmpeg: ${err.message}`);
    // Retry com próxima música
    setTimeout(() => playNextTrack(), 2000);
  });

  ffmpegProcess.on('close', (code) => {
    if (isPlaying) {
      if (code !== 0 && code !== null) {
        log('warn', `FFmpeg encerrou com código ${code}`);
      }
      // Avança para próxima música
      setTimeout(() => playNextTrack(), 1000);
    }
  });

  // Backup: força próxima música após duração + 5s (caso ffmpeg trave)
  trackTimeout = setTimeout(() => {
    if (isPlaying && ffmpegProcess) {
      log('warn', `⏭ Timeout atingido para ${track.title}`);
      ffmpegProcess.kill('SIGTERM');
    }
  }, currentTrackDuration + 5000);

  // Distribuir dados para todos os clientes conectados
  broadcastStream.on('data', (chunk) => {
    activeResponses.forEach(res => {
      if (!res.destroyed && res.writableEnded === false) {
        res.write(chunk);
      }
    });
  });

  broadcastStream.on('error', (err) => {
    log('error', `Erro no broadcast stream: ${err.message}`);
  });
}

// ================= CHAT COM RATE LIMITING =================
const MAX_CHAT_HISTORY = 100;
let chatHistory = [];
let chatUsers = new Map(); // ws -> { name, joinedAt, lastMessageTime, messageCount }
const CHAT_RATE_LIMIT = {
  windowMs: 10000,  // 10 segundos
  maxMessages: 5    // máximo 5 mensagens por janela
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

// ================= WEBSOCKET COM VALIDAÇÃO DE ORIGEM =================
const clients = new Set();

wss.on('connection', (ws, req) => {
  // Validação básica de origem
  const origin = req.headers.origin;
  if (NODE_ENV === 'production' && origin !== 'https://radioamigosdoseuze.com.br') {
    ws.close(1008, 'Origem não autorizada');
    return;
  }

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
        return;
      }

      // ===== CHAT =====
      if (data.type === 'chat' && data.name && data.message) {
        const name = String(data.name).trim().substring(0, 20);
        const msgText = String(data.message).trim().substring(0, 200);
        if (name && msgText) {
          chatUsers.set(ws, chatUsers.get(ws) || { name, joinedAt: Date.now() });

          if (!checkRateLimit(ws)) {
            ws.send(JSON.stringify({
              type: 'system',
              message: '⚠️ Muitas mensagens. Aguarde um pouco.'
            }));
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

  ws.on('error', (err) => {
    log('error', `Erro no WebSocket: ${err.message}`);
    clients.delete(ws);
    chatUsers.delete(ws);
  });
});

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

// ================= STREAM (RÁDIO AO VIVO - TODOS SINCRONIZADOS) =================
app.get('/stream', (req, res) => {
  // Verificar playlist ANTES de setar headers de áudio
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
  activeResponses.add(res);
  streamClients = activeResponses.size;
  broadcastState();

  log('info', `🎧 Cliente conectado ao stream. Total ouvintes: ${streamClients}`);

  // Se a rádio ainda não está tocando, inicia
  if (!isPlaying) {
    startRadioStream();
  }
  // NOTA: NÃO enviamos a música atual do início. O cliente começa a receber
  // o broadcastStream em tempo real, no ponto exato em que a música está.
  // Isso é o comportamento correto de uma rádio ao vivo.

  // Quando o cliente desconectar
  req.on('close', () => {
    activeResponses.delete(res);
    streamClients = activeResponses.size;
    broadcastState();
    log('info', `🎧 Cliente desconectou do stream. Ouvintes: ${streamClients}`);

    // Se não houver mais ouvintes, para a rádio após 30s
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

// ================= GRACEFUL SHUTDOWN =================
function gracefulShutdown(signal) {
  log('info', `Recebido ${signal}. Encerrando graciosamente...`);

  // Parar o stream
  stopRadioStream();

  // Fechar todas as conexões WebSocket
  clients.forEach(ws => {
    ws.close(1001, 'Servidor reiniciando');
  });

  // Fechar o servidor HTTP
  server.close(() => {
    log('info', 'Servidor HTTP fechado');
    // Flush logs restantes
    if (logQueue.length > 0) {
      fs.appendFileSync(path.join(LOG_DIR, 'server.log'), logQueue.join(''));
    }
    process.exit(0);
  });

  // Forçar saída após 10s se travar
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