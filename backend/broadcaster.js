const { spawn } = require('child_process');
const path = require('path');
const WebSocket = require('ws');
const fs = require('fs');
require('dotenv').config();

const ICECAST_HOST = process.env.ICECAST_HOST || 'localhost';
const ICECAST_PORT = process.env.ICECAST_PORT || 8000;
const ICECAST_MOUNT = process.env.ICECAST_MOUNT || '/stream';
const ICECAST_PASSWORD = process.env.ICECAST_PASSWORD || 'hackme';
const WS_URL = process.env.WS_URL || `ws://localhost:${process.env.PORT || 10000}`;
const AUDIO_DIR = path.join(__dirname, '../audio');
const LOG_DIR = path.join(__dirname, '../logs');

if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

function log(level, message) {
  const timestamp = new Date().toISOString();
  const logEntry = `[${timestamp}] [${level.toUpperCase()}] ${message}\n`;
  console.log(logEntry.trim());
  fs.appendFileSync(path.join(LOG_DIR, 'broadcaster.log'), logEntry);
}

const playlist = [
  "7 Espadas Ogum.mp3", "Altar E Tronqueira.mp3", "a vida e assim mesmo como ela e.mp3",
  "Batismo.mp3", "Benzimento da Lua.mp3", "Caboclo Pedra Preta.mp3", "ciganos.mp3",
  "Exu é lei.mp3", "jangadeiro.mp3", "João Batista .mp3", "Jurema .mp3",
  "Luz de Oxalá.mp3", "marinheiros .mp3", "odociaba.mp3", "Ogum Beira Mar.mp3",
  "ogum .mp3", "Oxossi é oke aro.mp3", "Passo da Malandragem.mp3", "preto velho .mp3",
  "quatro da manhã.mp3", "Rosa Caveira.mp3", "santo Antônio.mp3",
  "Saudação às Sete Linhas.mp3", "Xangô é corisco.mp3"
];

let currentIndex = 0;
let ffmpeg = null;
let ws = null;
let running = false;

function getNextTrack() {
  const track = playlist[currentIndex];
  currentIndex = (currentIndex + 1) % playlist.length;
  return track;
}

function connectWS() {
  ws = new WebSocket(WS_URL);
  ws.on('open', () => {
    log('info', '🔗 Conectado ao servidor WebSocket');
  });
  ws.on('error', () => {});
  ws.on('close', () => {
    setTimeout(connectWS, 5000);
  });
}

function broadcastTrack(file) {
  const filePath = path.join(AUDIO_DIR, file);
  if (!fs.existsSync(filePath)) {
    log('error', `❌ Arquivo não encontrado: ${filePath}`);
    if (running) setTimeout(playNext, 1000);
    return;
  }

  const name = file.replace(/\.mp3$/i, '').trim();
  log('info', `▶ Tocando: ${name}`);

  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'update_metadata', title: name, artist: 'Ponto de Umbanda' }));
  }

  const args = [
    '-re', '-i', filePath,
    '-c:a', 'libmp3lame', '-b:a', '128k', '-ar', '44100', '-ac', '2',
    '-content_type', 'audio/mpeg',
    '-ice_name', 'Rádio Amigos do Seu Zé',
    '-ice_description', 'Sua Rádio Comunitária de Pontos de Umbanda',
    '-ice_genre', 'Umbanda / Religioso',
    '-ice_public', '1',
    '-f', 'mp3',
    `icecast://source:${ICECAST_PASSWORD}@${ICECAST_HOST}:${ICECAST_PORT}${ICECAST_MOUNT}`
  ];

  ffmpeg = spawn('ffmpeg', args, { stdio: ['ignore', 'pipe', 'pipe'] });

  ffmpeg.on('close', () => {
    if (running) setTimeout(playNext, 500);
  });

  ffmpeg.on('error', (err) => {
    log('error', `❌ Erro FFmpeg: ${err.message}`);
    if (running) setTimeout(playNext, 2000);
  });
}

function playNext() {
  if (!running) return;
  broadcastTrack(getNextTrack());
}

function start() {
  log('info', '╔══════════════════════════════════════════════════════╗');
  log('info', '║     📡 BROADCASTER — Rádio Amigos do Seu Zé          ║');
  log('info', '╚══════════════════════════════════════════════════════╝');

  if (!fs.existsSync(AUDIO_DIR)) {
    log('error', `❌ ERRO: Diretório não encontrado: ${AUDIO_DIR}`);
    process.exit(1);
  }

  running = true;
  connectWS();
  playNext();
}

function stop() {
  running = false;
  if (ffmpeg) ffmpeg.kill('SIGTERM');
  if (ws) ws.close();
}

process.on('SIGINT', () => { stop(); setTimeout(() => process.exit(0), 500); });
process.on('SIGTERM', () => { stop(); setTimeout(() => process.exit(0), 500); });

start();
