// ================= RÁDIO AMIGOS DO SEU ZÉ - FRONTEND =================
// frontend/js/app.js

// ================= CONFIGURAÇÃO =================
const WS_URL = window.location.protocol === 'https:' 
  ? `wss://${window.location.host}` 
  : `ws://${window.location.host}`;

const API_BASE = `${window.location.protocol}//${window.location.host}`;

// ================= ELEMENTOS DO DOM =================
const player = document.getElementById('audio-player');
const playBtn = document.getElementById('play-btn');
const pauseBtn = document.getElementById('pause-btn');
const volumeSlider = document.getElementById('volume');
const currentTitle = document.getElementById('current-title');
const currentArtist = document.getElementById('current-artist');
const listenersCount = document.getElementById('listeners-count');
const playlistContainer = document.getElementById('playlist');
const historyContainer = document.getElementById('history');
const connectionStatus = document.getElementById('connection-status');

// ================= ESTADO =================
let ws = null;
let isPlaying = false;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_DELAY = 3000;

// ================= INICIALIZAÇÃO =================
function init() {
  console.log('🎵 Iniciando Rádio Amigos do Seu Zé...');
  console.log('WS_URL:', WS_URL);
  console.log('API_BASE:', API_BASE);

  connectWebSocket();
  setupPlayer();
  setupEventListeners();
  fetchPlaylist();
  fetchHistory();
}

// ================= WEBSOCKET =================
function connectWebSocket() {
  if (ws && (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN)) {
    return;
  }

  try {
    ws = new WebSocket(WS_URL);

    ws.onopen = () => {
      console.log('✅ WebSocket conectado');
      reconnectAttempts = 0;
      updateConnectionStatus('online');
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        handleWebSocketMessage(data);
      } catch (err) {
        console.error('Erro ao processar mensagem WS:', err);
      }
    };

    ws.onclose = () => {
      console.log('❌ WebSocket desconectado');
      updateConnectionStatus('offline');
      attemptReconnect();
    };

    ws.onerror = (err) => {
      console.error('Erro WebSocket:', err);
      updateConnectionStatus('error');
    };

  } catch (err) {
    console.error('Falha ao criar WebSocket:', err);
    attemptReconnect();
  }
}

function attemptReconnect() {
  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    console.error('Máximo de tentativas de reconexão atingido');
    return;
  }

  reconnectAttempts++;
  console.log(`Tentando reconectar... (${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);

  setTimeout(() => {
    connectWebSocket();
  }, RECONNECT_DELAY);
}

function handleWebSocketMessage(data) {
  switch (data.type) {
    case 'state':
      updateState(data.data);
      break;
    case 'metadata':
      updateMetadata(data.data);
      break;
    case 'playlist':
      renderPlaylist(data.data);
      break;
    case 'pong':
      // Heartbeat response
      break;
    default:
      console.log('Mensagem WS desconhecida:', data.type);
  }
}

function updateConnectionStatus(status) {
  if (!connectionStatus) return;

  const statusMap = {
    online: { text: 'Conectado', class: 'status-online' },
    offline: { text: 'Desconectado', class: 'status-offline' },
    error: { text: 'Erro de conexão', class: 'status-error' }
  };

  const info = statusMap[status] || statusMap.offline;
  connectionStatus.textContent = info.text;
  connectionStatus.className = info.class;
}

// ================= PLAYER DE ÁUDIO =================
function setupPlayer() {
  if (!player) {
    console.error('Elemento audio-player não encontrado');
    return;
  }

  // 🔥 CORREÇÃO PRINCIPAL: Usar a rota /stream do servidor
  // NUNCA usar caminho relativo como ./audio/ ou ../audio/
  const streamUrl = `${API_BASE}/stream`;

  console.log('Stream URL configurada:', streamUrl);

  player.src = streamUrl;
  player.crossOrigin = 'anonymous';
  player.preload = 'none';

  // Eventos do player
  player.addEventListener('error', (e) => {
    console.error('Erro no player:', e);
    console.error('Código do erro:', player.error?.code);
    console.error('Mensagem do erro:', player.error?.message);

    // Tentar reconectar após erro
    if (isPlaying) {
      setTimeout(() => {
        console.log('Tentando reconectar o stream...');
        player.src = streamUrl;
        player.play().catch(err => console.error('Falha ao reconectar:', err));
      }, 3000);
    }
  });

  player.addEventListener('stalled', () => {
    console.warn('Stream stalled - buffer vazio');
  });

  player.addEventListener('waiting', () => {
    console.warn('Aguardando dados do stream...');
  });

  player.addEventListener('playing', () => {
    console.log('✅ Stream começou a tocar');
    isPlaying = true;
    updatePlayButton();
  });

  player.addEventListener('pause', () => {
    console.log('⏸ Stream pausado');
    isPlaying = false;
    updatePlayButton();
  });

  player.addEventListener('ended', () => {
    console.log('Stream encerrado');
    isPlaying = false;
    updatePlayButton();
  });
}

function setupEventListeners() {
  // Botão Play
  if (playBtn) {
    playBtn.addEventListener('click', () => {
      console.log('▶️ Play clicado');
      playAudio();
    });
  }

  // Botão Pause
  if (pauseBtn) {
    pauseBtn.addEventListener('click', () => {
      console.log('⏸ Pause clicado');
      pauseAudio();
    });
  }

  // Volume
  if (volumeSlider) {
    volumeSlider.addEventListener('input', (e) => {
      const volume = e.target.value / 100;
      player.volume = volume;
      console.log('Volume:', volume);
    });
    // Volume inicial
    player.volume = (volumeSlider.value || 80) / 100;
  }
}

function playAudio() {
  if (!player) return;

  // Se não tiver src, configurar
  if (!player.src || player.src === window.location.href) {
    player.src = `${API_BASE}/stream`;
  }

  const playPromise = player.play();

  if (playPromise !== undefined) {
    playPromise
      .then(() => {
        console.log('✅ Reprodução iniciada');
        isPlaying = true;
        updatePlayButton();
      })
      .catch(err => {
        console.error('❌ Erro ao iniciar reprodução:', err);

        // Se for erro de rede, tentar novamente
        if (err.name === 'NotAllowedError') {
          console.warn('Autoplay bloqueado - aguardando interação do usuário');
        } else if (err.name === 'NotSupportedError') {
          console.error('Formato de áudio não suportado');
        } else {
          // Tentar reconectar
          setTimeout(() => {
            player.src = `${API_BASE}/stream`;
            player.play().catch(e => console.error('Segunda tentativa falhou:', e));
          }, 2000);
        }
      });
  }
}

function pauseAudio() {
  if (!player) return;
  player.pause();
  isPlaying = false;
  updatePlayButton();
}

function updatePlayButton() {
  if (playBtn && pauseBtn) {
    playBtn.style.display = isPlaying ? 'none' : 'inline-block';
    pauseBtn.style.display = isPlaying ? 'inline-block' : 'none';
  }
}

// ================= ATUALIZAÇÃO DE DADOS =================
function updateState(data) {
  if (!data) return;

  if (listenersCount && data.listeners !== undefined) {
    listenersCount.textContent = data.listeners;
  }

  if (data.currentTrack) {
    updateMetadata(data.currentTrack);
  }
}

function updateMetadata(track) {
  if (!track) return;

  console.log('🎵 Metadata atualizada:', track.title, '-', track.artist);

  if (currentTitle) {
    currentTitle.textContent = track.title || 'Desconhecido';
  }

  if (currentArtist) {
    currentArtist.textContent = track.artist || 'Ponto de Umbanda';
  }

  // Atualizar título da página
  document.title = `${track.title || 'Rádio Amigos do Seu Zé'} - ${track.artist || 'Ponto de Umbanda'}`;
}

// ================= PLAYLIST =================
async function fetchPlaylist() {
  try {
    const response = await fetch(`${API_BASE}/api/playlist`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    const playlist = await response.json();
    renderPlaylist(playlist);
  } catch (err) {
    console.error('Erro ao carregar playlist:', err);
  }
}

function renderPlaylist(playlist) {
  if (!playlistContainer || !playlist) return;

  playlistContainer.innerHTML = '';

  if (playlist.length === 0) {
    playlistContainer.innerHTML = '<p class="empty">Nenhuma música na playlist</p>';
    return;
  }

  playlist.forEach((track, index) => {
    const item = document.createElement('div');
    item.className = 'playlist-item';
    item.innerHTML = `
      <span class="track-number">${index + 1}</span>
      <span class="track-title">${escapeHtml(track.title || 'Desconhecido')}</span>
      <span class="track-artist">${escapeHtml(track.artist || 'Ponto de Umbanda')}</span>
    `;
    playlistContainer.appendChild(item);
  });

  console.log(`✓ ${playlist.length} músicas na playlist`);
}

// ================= HISTÓRICO =================
async function fetchHistory() {
  try {
    const response = await fetch(`${API_BASE}/api/history`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    const history = await response.json();
    renderHistory(history);
  } catch (err) {
    console.error('Erro ao carregar histórico:', err);
  }
}

function renderHistory(history) {
  if (!historyContainer || !history) return;

  historyContainer.innerHTML = '';

  if (history.length === 0) {
    historyContainer.innerHTML = '<p class="empty">Nenhuma música no histórico</p>';
    return;
  }

  history.forEach((track) => {
    const item = document.createElement('div');
    item.className = 'history-item';

    const playedAt = track.playedAt 
      ? new Date(track.playedAt).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })
      : '--:--';

    item.innerHTML = `
      <span class="history-time">${playedAt}</span>
      <span class="history-title">${escapeHtml(track.title || 'Desconhecido')}</span>
    `;
    historyContainer.appendChild(item);
  });
}

// ================= UTILITÁRIOS =================
function escapeHtml(text) {
  if (!text) return '';
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// Heartbeat para manter conexão viva
setInterval(() => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'ping', time: Date.now() }));
  }
}, 30000);

// Atualizar histórico periodicamente
setInterval(() => {
  fetchHistory();
}, 60000);

// ================= INICIAR =================
document.addEventListener('DOMContentLoaded', init);

// Exportar funções para uso global (se necessário)
window.RadioApp = {
  play: playAudio,
  pause: pauseAudio,
  getState: () => ({ isPlaying, wsConnected: ws?.readyState === WebSocket.OPEN })
};