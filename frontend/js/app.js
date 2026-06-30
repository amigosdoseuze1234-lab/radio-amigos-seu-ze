/* ============================================
   RÁDIO AMIGOS DO SEU ZÉ — app.js
   Player WebSocket + Controles
   ============================================ */

const WS_URL = window.location.protocol === 'https:' 
  ? `wss://${window.location.host}` 
  : `ws://${window.location.host}`;

const API_URL = `${window.location.protocol}//${window.location.host}`;

// ===== ELEMENTOS =====
const player = document.getElementById('radioPlayer');
const btnPlay = document.getElementById('btnPlay');
const btnPrev = document.getElementById('btnPrev');
const btnNext = document.getElementById('btnNext');
const btnVolume = document.getElementById('btnVolume');
const volumePopup = document.getElementById('volumePopup');
const volumeRange = document.getElementById('volumeRange');
const trackTitle = document.getElementById('trackTitle');
const trackArtist = document.getElementById('trackArtist');
const statusEl = document.getElementById('status');
const statusText = document.getElementById('statusText');
const visualizer = document.getElementById('visualizer');
const playlistEl = document.getElementById('playlist');
const playlistCount = document.getElementById('playlistCount');
const listenersEl = document.getElementById('listeners');
const uptimeEl = document.getElementById('uptime');
const splash = document.getElementById('splash');
const toast = document.getElementById('radio-toast');
const progressBar = document.getElementById('progressBar');
const currentTimeEl = document.getElementById('currentTime');
const totalTimeEl = document.getElementById('totalTime');

// ===== ESTADO =====
let isPlaying = false;
let ws = null;
let currentTrackIndex = 0;
let playlist = [];
let reconnectTimer = null;

// ===== SPLASH =====
setTimeout(() => {
  splash.classList.add('hidden');
}, 2500);

// ===== PARTICLES =====
function createParticles() {
  const container = document.getElementById('particles');
  for (let i = 0; i < 20; i++) {
    const span = document.createElement('span');
    span.style.left = Math.random() * 100 + '%';
    span.style.animationDelay = Math.random() * 8 + 's';
    span.style.animationDuration = (6 + Math.random() * 4) + 's';
    container.appendChild(span);
  }
}
createParticles();

// ===== TOAST =====
function showToast(msg) {
  toast.textContent = msg;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 3000);
}

// ===== WEBSOCKET =====
function connectWS() {
  ws = new WebSocket(WS_URL);

  ws.onopen = () => {
    console.log('🔌 WS conectado');
    showToast('Conectado à rádio!');
  };

  ws.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      if (data.type === 'state') updateState(data.data);
      if (data.type === 'metadata') updateTrack(data.data);
      if (data.type === 'playlist') loadPlaylist(data.data);
    } catch (err) {
      console.error('Erro WS:', err);
    }
  };

  ws.onclose = () => {
    console.log('🔌 WS desconectado');
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectWS, 5000);
  };

  ws.onerror = () => {
    console.log('⚠️ Erro WS');
  };
}

// ===== ATUALIZAR ESTADO =====
function updateState(state) {
  if (state.isLive) {
    statusEl.classList.add('online');
    statusText.textContent = 'AO VIVO';
  } else {
    statusEl.classList.remove('online');
    statusText.textContent = 'OFFLINE';
  }

  listenersEl.textContent = state.listeners || 0;
  uptimeEl.textContent = formatTime(state.uptime || 0);

  if (state.currentTrack) {
    updateTrack(state.currentTrack);
  }
}

// ===== ATUALIZAR MÚSICA =====
function updateTrack(track) {
  trackTitle.textContent = track.title || 'Carregando...';
  trackArtist.textContent = track.artist || 'Ponto de Umbanda';
}

// ===== FORMATAR TEMPO =====
function formatTime(seconds) {
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return `${m}:${s < 10 ? '0' : ''}${s}`;
}

// ===== PLAYLIST =====
function loadPlaylist(list) {
  playlist = list;
  playlistCount.textContent = `${list.length} músicas`;
  playlistEl.innerHTML = '';

  list.forEach((item, i) => {
    const div = document.createElement('div');
    div.className = 'playlist-item' + (i === currentTrackIndex ? ' active' : '');
    div.innerHTML = `
      <div class="num">${i + 1}</div>
      <div class="name">${item.title}</div>
      <div class="duration">${item.duration || '0:00'}</div>
    `;
    div.addEventListener('click', () => playTrack(i));
    playlistEl.appendChild(div);
  });
}

// ===== PLAY/PAUSE =====
function togglePlay() {
  if (isPlaying) {
    player.pause();
    btnPlay.textContent = '▶';
    btnPlay.classList.remove('playing');
    visualizer.classList.add('paused');
    isPlaying = false;
    showToast('Pausado');
  } else {
    player.play().catch(() => {
      showToast('Erro ao tocar. Tente novamente.');
    });
    btnPlay.textContent = '⏸';
    btnPlay.classList.add('playing');
    visualizer.classList.remove('paused');
    isPlaying = true;
    showToast('Tocando agora!');
  }
}

// ===== TOCAR MÚSICA ESPECÍFICA =====
function playTrack(index) {
  currentTrackIndex = index;
  const items = playlistEl.querySelectorAll('.playlist-item');
  items.forEach((item, i) => {
    item.classList.toggle('active', i === index);
  });

  // Atualiza o source do player
  if (playlist[index]) {
    player.src = `${API_URL}/api/play/${index}`;
    player.load();
    togglePlay();
  }
}

// ===== VOLUME =====
let volumeVisible = false;
btnVolume.addEventListener('click', () => {
  volumeVisible = !volumeVisible;
  volumePopup.classList.toggle('visible', volumeVisible);
});

volumeRange.addEventListener('input', (e) => {
  player.volume = e.target.value / 100;
});

document.addEventListener('click', (e) => {
  if (!e.target.closest('.volume-slider')) {
    volumeVisible = false;
    volumePopup.classList.remove('visible');
  }
});

// ===== PROGRESSO =====
player.addEventListener('timeupdate', () => {
  if (player.duration) {
    const pct = (player.currentTime / player.duration) * 100;
    progressBar.style.width = pct + '%';
    currentTimeEl.textContent = formatTime(player.currentTime);
    totalTimeEl.textContent = formatTime(player.duration);
  }
});

// ===== EVENTOS =====
btnPlay.addEventListener('click', togglePlay);

btnPrev.addEventListener('click', () => {
  const newIndex = (currentTrackIndex - 1 + playlist.length) % playlist.length;
  playTrack(newIndex);
});

btnNext.addEventListener('click', () => {
  const newIndex = (currentTrackIndex + 1) % playlist.length;
  playTrack(newIndex);
});

player.addEventListener('ended', () => {
  btnPlay.textContent = '▶';
  btnPlay.classList.remove('playing');
  visualizer.classList.add('paused');
  isPlaying = false;
});

player.addEventListener('error', () => {
  statusEl.classList.remove('online');
  statusText.textContent = 'STREAM OFFLINE';
  showToast('Stream offline. Verifique a conexão.');
});

// ===== INICIALIZAR =====
async function init() {
  try {
    const res = await fetch(`${API_URL}/api/status`);
    const data = await res.json();
    if (data.success) {
      updateState(data.data);
      loadPlaylist(data.data.playlist);
    }
  } catch (e) {
    console.error('Erro ao carregar status:', e);
  }

  connectWS();
}

init();
