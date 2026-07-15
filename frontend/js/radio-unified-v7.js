/**
 * Rádio Amigos do Seu Zé - Player Unificado v7
 * =============================================
 * Solução definitiva que unifica index.html + app.js + listener-client.js
 * 
 * Problemas resolvidos:
 * - Apenas 1 elemento de áudio existe por vez
 * - Apenas 1 conjunto de event listeners
 * - Reconexão inteligente sem conflitos
 * - Session ID único persistente
 * - Keepalive para detectar stalls
 * - Sincronização de metadata com servidor
 */

(function() {
  'use strict';

  // ============================================================
  // CONFIGURAÇÕES
  // ============================================================
  const CONFIG = {
    HEARTBEAT_INTERVAL: 5000,      // Ping WebSocket a cada 5s
    RECONNECT_DELAY: 1500,         // Delay base para reconexão
    MAX_RECONNECT_ATTEMPTS: 50,    // Máximo de tentativas
    SESSION_KEY: 'radio_session_id_v7',
    STALL_CHECK_INTERVAL: 4000,    // Verificar stall a cada 4s
    STALL_THRESHOLD: 2,            // Stalls consecutivos antes de reconectar
    KEEPALIVE_INTERVAL: 5000,      // Keepalive do áudio
    VOLUME_KEY: 'radio_volume_v7',
    CHAT_NAME_KEY: 'chat_name_v7',
    INTERACTED_KEY: 'radio_has_interacted_v7'
  };

  // ============================================================
  // ESTADO GLOBAL (único, protegido do escopo)
  // ============================================================
  const state = {
    sessionId: null,
    ws: null,
    audio: null,
    audioContainer: null,
    isPlaying: false,
    isReconnecting: false,
    reconnectAttempts: 0,
    reconnectCount: 0,
    stallCount: 0,
    lastCurrentTime: 0,
    firstPlay: true,
    interactionRequired: false,

    // Timers
    heartbeatTimer: null,
    syncTimer: null,
    stallTimer: null,
    keepaliveTimer: null,

    // Dados
    listeners: { active: 0, peak: 0, dailyUnique: 0 },
    currentTrack: { title: '', artist: '', duration: 0, startTime: 0, elapsed: 0 },

    // Chat
    chatName: '',
    chatInitialized: false
  };

  // ============================================================
  // UTILITÁRIOS
  // ============================================================
  function generateSessionId() {
    return 'sess_' + Date.now() + '_' + Math.random().toString(36).substring(2, 11);
  }

  function getSessionId() {
    let sid = localStorage.getItem(CONFIG.SESSION_KEY);
    if (!sid) {
      sid = generateSessionId();
      localStorage.setItem(CONFIG.SESSION_KEY, sid);
    }
    return sid;
  }

  function getWsUrl() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return protocol + '//' + window.location.host;
  }

  function getStreamUrl() {
    const ts = Date.now();
    return '/stream?sid=' + encodeURIComponent(state.sessionId) + '&_t=' + ts;
  }

  function formatTime(seconds) {
    if (!seconds || seconds < 0) return '0:00';
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return mins + ':' + secs.toString().padStart(2, '0');
  }

  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  // ============================================================
  // UI UPDATES
  // ============================================================
  function updateStatusBadge() {
    const badge = document.getElementById('statusBadge');
    const statusText = document.getElementById('statusText');
    if (!badge || !statusText) return;

    const { active } = state.listeners;

    if (state.isPlaying && !state.isReconnecting) {
      badge.className = 'status-badge online';
      statusText.textContent = 'AO VIVO • ' + active + ' ouvinte' + (active !== 1 ? 's' : '');
    } else if (state.isReconnecting) {
      badge.className = 'status-badge buffering';
      statusText.textContent = 'CONECTANDO...';
    } else {
      badge.className = 'status-badge offline';
      statusText.textContent = 'OFFLINE';
    }
  }

  function updatePlayButton(playing) {
    const btn = document.getElementById('playBtn');
    if (!btn) return;

    btn.innerHTML = playing ? '\u23F8' : '\u25B6';
    btn.setAttribute('aria-label', playing ? 'Pausar' : 'Tocar');
    btn.classList.toggle('playing', playing);
  }

  function updateTrackInfo(track) {
    const titleEl = document.getElementById('trackTitle');
    const artistEl = document.getElementById('trackArtist');

    if (titleEl) {
      titleEl.style.opacity = '0';
      setTimeout(() => {
        titleEl.textContent = track.title || 'Desconhecida';
        titleEl.style.opacity = '1';
      }, 200);
    }

    if (artistEl) {
      artistEl.style.opacity = '0';
      setTimeout(() => {
        artistEl.textContent = track.artist || 'Ponto de Umbanda';
        artistEl.style.opacity = '1';
      }, 200);
    }

    if (track.title) {
      document.title = track.title + ' \u2014 R\u00e1dio Amigos do Seu Z\u00e9';
    }
  }

  function updateProgressBar() {
    const progressFill = document.getElementById('progressFill');
    const currentTimeEl = document.getElementById('currentTime');
    const totalTimeEl = document.getElementById('totalTime');

    if (!state.currentTrack.duration) return;

    const serverElapsed = state.currentTrack.startTime > 0
      ? Math.floor((Date.now() - state.currentTrack.startTime) / 1000)
      : 0;

    const elapsed = Math.min(serverElapsed, state.currentTrack.duration);
    const progress = state.currentTrack.duration > 0
      ? (elapsed / state.currentTrack.duration) * 100
      : 0;

    if (progressFill) {
      progressFill.style.width = progress + '%';
      progressFill.setAttribute('aria-valuenow', Math.round(progress));
    }
    if (currentTimeEl) currentTimeEl.textContent = formatTime(elapsed);
    if (totalTimeEl) totalTimeEl.textContent = formatTime(state.currentTrack.duration);

    if (elapsed >= state.currentTrack.duration && state.currentTrack.duration > 0) {
      if (progressFill) progressFill.style.width = '100%';
    }
  }

  function animateAlbumArt() {
    const albumArt = document.getElementById('albumArt');
    if (!albumArt) return;
    albumArt.classList.remove('playing');
    void albumArt.offsetWidth; // trigger reflow
    albumArt.classList.add('playing');
  }

  function showToast(message) {
    const toast = document.getElementById('toast');
    if (toast) {
      toast.textContent = message;
      toast.classList.add('show');
      setTimeout(() => toast.classList.remove('show'), 4000);
    }
    console.log('[Toast]', message);
  }

  function showInteractionOverlay() {
    const overlay = document.getElementById('interactionOverlay');
    if (overlay) {
      overlay.style.display = 'flex';
      requestAnimationFrame(() => overlay.classList.add('show'));
    }
  }

  function hideInteractionOverlay() {
    const overlay = document.getElementById('interactionOverlay');
    if (overlay) {
      overlay.classList.remove('show');
      setTimeout(() => { if (!overlay.classList.contains('show')) overlay.style.display = 'none'; }, 300);
    }
  }

  // ============================================================
  // GERENCIAMENTO DE ÁUDIO - APENAS 1 ELEMENTO
  // ============================================================

  /**
   * Destrói TODOS os elementos de áudio existentes
   * Garante que nunca haja mais de 1 áudio ativo
   */
  function destroyAllAudio() {
    // Remove do container interno
    if (state.audioContainer) {
      while (state.audioContainer.firstChild) {
        const audio = state.audioContainer.firstChild;
        cleanupAudioElement(audio);
        state.audioContainer.removeChild(audio);
      }
    }

    // Remove quaisquer elementos audio órfãos no body
    const orphans = document.querySelectorAll('audio[data-radio-audio]');
    orphans.forEach(audio => {
      cleanupAudioElement(audio);
      if (audio.parentNode) audio.parentNode.removeChild(audio);
    });

    state.audio = null;
    console.log('[Radio] Todos os elementos de áudio destruídos');
  }

  /**
   * Limpa todos os event listeners e recursos de um elemento audio
   */
  function cleanupAudioElement(audio) {
    if (!audio) return;
    try {
      audio.pause();
      audio.src = '';
      audio.load();
    } catch (e) {}

    // Clona para remover TODOS os event listeners
    const clone = audio.cloneNode(false);
    if (audio.parentNode) {
      audio.parentNode.replaceChild(clone, audio);
    }
  }

  /**
   * Cria um NOVO elemento de áudio (sempre destrói o anterior primeiro)
   */
  function createAudioElement() {
    // SEMPRE destrói o anterior primeiro
    destroyAllAudio();

    // Cria container se não existir
    if (!state.audioContainer) {
      state.audioContainer = document.createElement('div');
      state.audioContainer.id = 'radio-audio-container';
      state.audioContainer.style.display = 'none';
      document.body.appendChild(state.audioContainer);
    }

    const audio = document.createElement('audio');
    audio.setAttribute('data-radio-audio', 'true');
    audio.crossOrigin = 'anonymous';
    audio.setAttribute('playsinline', '');
    audio.preload = 'auto';
    audio.muted = false;

    // Restaura volume salvo
    const savedVol = localStorage.getItem(CONFIG.VOLUME_KEY);
    audio.volume = savedVol ? parseInt(savedVol) / 100 : 0.8;

    // Adiciona event listeners (apenas uma vez)
    audio.addEventListener('play', onAudioPlay);
    audio.addEventListener('pause', onAudioPause);
    audio.addEventListener('error', onAudioError);
    audio.addEventListener('waiting', onAudioWaiting);
    audio.addEventListener('playing', onAudioPlaying);
    audio.addEventListener('stalled', onAudioStalled);
    audio.addEventListener('ended', onAudioEnded);
    audio.addEventListener('canplay', onAudioCanPlay);
    audio.addEventListener('abort', onAudioAbort);
    audio.addEventListener('emptied', onAudioEmptied);

    state.audioContainer.appendChild(audio);
    state.audio = audio;

    console.log('[Radio] Novo elemento de áudio criado');
    return audio;
  }

  // ============================================================
  // EVENT HANDLERS DO ÁUDIO
  // ============================================================
  function onAudioPlay() {
    console.log('[Radio] Event: play');
    state.isPlaying = true;
    state.isReconnecting = false;
    state.stallCount = 0;
    state.reconnectCount = 0;
    updateStatusBadge();

    const albumArt = document.getElementById('albumArt');
    if (albumArt) albumArt.classList.add('playing');
    updatePlayButton(true);
    state.interactionRequired = false;
    state.firstPlay = false;
  }

  function onAudioPause() {
    console.log('[Radio] Event: pause');
    state.isPlaying = false;
    updateStatusBadge();

    const albumArt = document.getElementById('albumArt');
    if (albumArt) albumArt.classList.remove('playing');
    updatePlayButton(false);
  }

  function onAudioError(e) {
    const err = state.audio ? state.audio.error : null;
    console.error('[Radio] Event: error', err ? err.code : 'unknown');

    // Não mostra erro ao usuário - reconecta silenciosamente
    if (!state.isReconnecting && state.isPlaying) {
      console.log('[Radio] Reconectando silenciosamente após erro...');
      setTimeout(() => reconnectAudio(), 800);
    }
  }

  function onAudioWaiting() {
    console.log('[Radio] Event: waiting');
  }

  function onAudioPlaying() {
    console.log('[Radio] Event: playing');
    state.stallCount = 0;
    state.isReconnecting = false;
  }

  function onAudioStalled() {
    console.log('[Radio] Event: stalled');
    if (!state.isReconnecting && state.isPlaying) {
      setTimeout(() => reconnectAudio(), 1000);
    }
  }

  function onAudioEnded() {
    console.log('[Radio] Event: ended');
    if (state.isPlaying && !state.isReconnecting) {
      setTimeout(() => reconnectAudio(), 500);
    }
  }

  function onAudioCanPlay() {
    console.log('[Radio] Event: canplay');
    state.isReconnecting = false;
  }

  function onAudioAbort() {
    console.log('[Radio] Event: abort');
  }

  function onAudioEmptied() {
    console.log('[Radio] Event: emptied');
  }

  // ============================================================
  // CONTROLES DE PLAY/PAUSE
  // ============================================================
  function playStream() {
    console.log('[Radio] playStream() chamado');

    // Se já está tocando, não faz nada
    if (state.audio && !state.audio.paused && state.audio.src) {
      console.log('[Radio] Já está tocando');
      return;
    }

    // Cria novo áudio se necessário
    if (!state.audio) {
      createAudioElement();
    }

    const streamUrl = getStreamUrl();
    state.audio.src = streamUrl;
    console.log('[Radio] Tocando URL:', streamUrl);

    const playPromise = state.audio.play();
    if (playPromise) {
      playPromise.catch(function(err) {
        console.error('[Radio] Erro ao tocar:', err.name, err.message);

        if (err.name === 'NotAllowedError') {
          state.interactionRequired = true;
          showInteractionOverlay();
          showToast('\uD83D\uDD0A Clique no botão de play para ouvir');
        } else if (err.name === 'NotSupportedError') {
          showToast('\u26A0\uFE0F Formato não suportado pelo navegador');
        } else {
          // Outro erro - tenta com novo elemento
          console.log('[Radio] Tentando com novo elemento...');
          setTimeout(() => {
            createAudioElement();
            playStream();
          }, 1000);
        }
      });
    }
  }

  function pauseStream() {
    if (state.audio) {
      state.audio.pause();
    }
  }

  function togglePlay() {
    if (state.isPlaying) {
      pauseStream();
    } else {
      playStream();
    }
  }

  // ============================================================
  // RECONEXÃO INTELIGENTE
  // ============================================================
  function reconnectAudio() {
    if (state.isReconnecting) {
      console.log('[Radio] Reconexão já em andamento, ignorando');
      return;
    }

    state.isReconnecting = true;
    state.reconnectCount++;
    console.log('[Radio] ========== RECONEXÃO #' + state.reconnectCount + ' ==========');

    // Destrói áudio antigo e cria novo
    createAudioElement();

    // Nova URL com timestamp
    const newUrl = getStreamUrl();
    state.audio.src = newUrl;
    console.log('[Radio] Nova URL:', newUrl);

    // Tenta tocar
    const playPromise = state.audio.play();
    if (playPromise) {
      playPromise.then(() => {
        console.log('[Radio] Reconectado com sucesso!');
        state.isReconnecting = false;
        state.reconnectCount = 0;
      }).catch((err) => {
        console.error('[Radio] Erro na reconexão:', err.name);
        state.isReconnecting = false;

        if (err.name === 'NotAllowedError') {
          showInteractionOverlay();
        } else {
          setTimeout(() => reconnectAudio(), 2000);
        }
      });
    } else {
      state.isReconnecting = false;
    }
  }

  // ============================================================
  // KEEPALIVE - DETECTA STALLS
  // ============================================================
  function startKeepalive() {
    stopKeepalive();
    state.lastCurrentTime = 0;
    state.stallCount = 0;

    state.keepaliveTimer = setInterval(() => {
      if (!state.audio || !state.isPlaying || state.audio.paused) return;

      const currentTime = state.audio.currentTime;

      // Se currentTime não avançou, conta como stall
      if (currentTime === state.lastCurrentTime || currentTime === 0) {
        state.stallCount++;
        console.log('[Radio] Keepalive: stall detectado (' + state.stallCount + '/' + CONFIG.STALL_THRESHOLD + ') currentTime=' + currentTime);

        if (state.stallCount >= CONFIG.STALL_THRESHOLD) {
          console.log('[Radio] Keepalive: STALL CONFIRMADO! Reconectando...');
          state.stallCount = 0;
          reconnectAudio();
        }
      } else {
        if (state.stallCount > 0) {
          console.log('[Radio] Keepalive: stream ok, currentTime=' + currentTime);
        }
        state.stallCount = 0;
      }

      state.lastCurrentTime = currentTime;
    }, CONFIG.STALL_CHECK_INTERVAL);
  }

  function stopKeepalive() {
    if (state.keepaliveTimer) {
      clearInterval(state.keepaliveTimer);
      state.keepaliveTimer = null;
    }
  }

  // ============================================================
  // WEBSOCKET
  // ============================================================
  function connectWebSocket() {
    if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    try {
      state.ws = new WebSocket(getWsUrl());

      state.ws.onopen = function() {
        console.log('[Radio] WebSocket conectado');
        state.reconnectAttempts = 0;

        state.ws.send(JSON.stringify({
          type: 'listener_ping',
          sessionId: state.sessionId,
          timestamp: Date.now()
        }));

        startHeartbeat();
        startSyncTimer();

        // Rejoin chat se já tinha nome
        if (state.chatName && state.chatInitialized) {
          state.ws.send(JSON.stringify({ type: 'join_chat', name: state.chatName }));
        }
      };

      state.ws.onmessage = function(event) {
        try {
          const data = JSON.parse(event.data);
          handleWsMessage(data);
        } catch (e) {
          console.warn('[Radio] Mensagem inválida:', e);
        }
      };

      state.ws.onclose = function(event) {
        console.log('[Radio] WebSocket fechado:', event.code);
        stopHeartbeat();
        stopSyncTimer();

        if (state.reconnectAttempts < CONFIG.MAX_RECONNECT_ATTEMPTS) {
          state.reconnectAttempts++;
          const delay = Math.min(CONFIG.RECONNECT_DELAY * state.reconnectAttempts, 30000);
          setTimeout(connectWebSocket, delay);
        }
      };

      state.ws.onerror = function(err) {
        console.error('[Radio] WebSocket erro:', err);
      };

    } catch (e) {
      console.error('[Radio] Erro ao conectar WebSocket:', e);
    }
  }

  function handleWsMessage(data) {
    switch (data.type) {
      case 'state':
        if (data.data) {
          state.listeners.active = data.data.listeners || 0;
          state.listeners.peak = data.data.peakListeners || state.listeners.peak;
          state.listeners.dailyUnique = data.data.dailyUnique || state.listeners.dailyUnique;
          updateStatusBadge();
        }
        break;

      case 'metadata':
        handleMetadata(data.data);
        break;

      case 'listener_pong':
        if (data.data) {
          state.listeners.active = data.data.listeners || 0;
          updateStatusBadge();
        }
        if (data.trackStartTime && data.duration) {
          handleMetadata({
            title: data.currentTrack || state.currentTrack.title,
            artist: state.currentTrack.artist,
            duration: data.duration,
            startTime: data.trackStartTime,
            elapsed: data.elapsed || 0
          });
        }
        break;

      case 'system':
        showToast(data.message);
        break;

      case 'chat':
        addChatMessage(data);
        break;

      case 'chat_history':
        loadChatHistory(data.data);
        break;
    }
  }

  function handleMetadata(track) {
    if (!track) return;

    const oldTitle = state.currentTrack.title;
    const newTitle = track.title || 'Desconhecida';
    const musicChanged = oldTitle !== newTitle && oldTitle !== '';

    state.currentTrack = {
      title: newTitle,
      artist: track.artist || 'Ponto de Umbanda',
      duration: track.duration || 0,
      startTime: track.startTime || 0,
      elapsed: track.elapsed || 0
    };

    updateTrackInfo(state.currentTrack);
    updateProgressBar();

    if (musicChanged) {
      showToast('\uD83C\uDFB5 Agora tocando: ' + newTitle);
      animateAlbumArt();
    }
  }

  function startHeartbeat() {
    stopHeartbeat();
    state.heartbeatTimer = setInterval(() => {
      if (state.ws && state.ws.readyState === WebSocket.OPEN) {
        state.ws.send(JSON.stringify({
          type: 'listener_ping',
          sessionId: state.sessionId,
          timestamp: Date.now()
        }));
      }
    }, CONFIG.HEARTBEAT_INTERVAL);
  }

  function stopHeartbeat() {
    if (state.heartbeatTimer) {
      clearInterval(state.heartbeatTimer);
      state.heartbeatTimer = null;
    }
  }

  function startSyncTimer() {
    stopSyncTimer();
    state.syncTimer = setInterval(updateProgressBar, 1000);
  }

  function stopSyncTimer() {
    if (state.syncTimer) {
      clearInterval(state.syncTimer);
      state.syncTimer = null;
    }
  }

  // ============================================================
  // CHAT
  // ============================================================
  function initChat() {
    if (state.chatInitialized) return;
    state.chatInitialized = true;

    state.chatName = localStorage.getItem(CONFIG.CHAT_NAME_KEY) || '';

    const chatInput = document.getElementById('chatInput');
    const chatSendBtn = document.getElementById('chatSendBtn');
    const chatNameOverlay = document.getElementById('chatNameOverlay');
    const chatNameInput = document.getElementById('chatNameInput');
    const chatNameBtn = document.getElementById('chatNameBtn');

    if (chatNameOverlay) {
      chatNameOverlay.style.display = !state.chatName ? 'flex' : 'none';
    }

    if (chatNameBtn && chatNameInput) {
      const saveName = () => {
        const name = chatNameInput.value.trim().substring(0, 20);
        if (name) {
          state.chatName = name;
          localStorage.setItem(CONFIG.CHAT_NAME_KEY, name);
          if (chatNameOverlay) chatNameOverlay.style.display = 'none';
          if (state.ws && state.ws.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify({ type: 'join_chat', name: name }));
          }
        }
      };

      chatNameBtn.addEventListener('click', saveName);
      chatNameInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') saveName();
      });
    }

    function sendChatMessage() {
      if (!chatInput || !state.chatName) return;
      const msg = chatInput.value.trim().substring(0, 200);
      if (!msg) return;

      if (state.ws && state.ws.readyState === WebSocket.OPEN) {
        state.ws.send(JSON.stringify({ type: 'chat', name: state.chatName, message: msg }));
        chatInput.value = '';
      }
    }

    if (chatSendBtn) {
      chatSendBtn.addEventListener('click', sendChatMessage);
    }

    if (chatInput) {
      chatInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') sendChatMessage();
      });
    }
  }

  function addChatMessage(data) {
    const container = document.getElementById('chatMessages');
    if (!container) return;

    const isOwn = data.name === state.chatName;
    const time = data.time
      ? new Date(data.time).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })
      : new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });

    const msgDiv = document.createElement('div');
    msgDiv.className = 'chat-message ' + (isOwn ? 'own' : 'other');
    msgDiv.innerHTML =
      '<div class="msg-sender">' + escapeHtml(data.name) + '</div>' +
      '<div class="chat-bubble">' +
        escapeHtml(data.message) +
        '<div class="chat-meta">' +
          time +
          (isOwn ? '<span class="msg-checks sent">\u2713\u2713</span>' : '') +
        '</div>' +
      '</div>';

    container.appendChild(msgDiv);
    container.scrollTop = container.scrollHeight;

    while (container.children.length > 100) {
      container.removeChild(container.firstChild);
    }
  }

  function loadChatHistory(messages) {
    const container = document.getElementById('chatMessages');
    if (!container) return;
    container.innerHTML = '<div class="chat-date-separator">Hoje</div>';
    if (Array.isArray(messages)) {
      messages.forEach(msg => addChatMessage(msg));
    }
  }

  // ============================================================
  // VISIBILITY API
  // ============================================================
  function handleVisibilityChange() {
    if (document.hidden) {
      console.log('[Radio] Página em background');
    } else {
      console.log('[Radio] Página visível');
      if (state.ws && state.ws.readyState !== WebSocket.OPEN) {
        connectWebSocket();
      }
      if (state.isPlaying && state.audio && state.audio.paused) {
        reconnectAudio();
      }
    }
  }

  // ============================================================
  // VISUALIZER (animação das barras)
  // ============================================================
  function initVisualizer() {
    const visualizer = document.getElementById('visualizer');
    if (!visualizer) return;

    const bars = visualizer.querySelectorAll('.bar');
    if (!bars.length) return;

    let visualizerInterval = null;

    function animate() {
      if (!state.isPlaying) {
        bars.forEach(bar => bar.style.height = '4px');
        return;
      }
      bars.forEach(bar => {
        const height = Math.random() * 24 + 4;
        bar.style.height = height + 'px';
      });
    }

    // Observa mudanças no estado de playing
    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        if (mutation.type === 'attributes' && mutation.attributeName === 'class') {
          const isPlayingNow = document.getElementById('albumArt')?.classList.contains('playing');
          if (isPlayingNow && !visualizerInterval) {
            visualizerInterval = setInterval(animate, 100);
          } else if (!isPlayingNow && visualizerInterval) {
            clearInterval(visualizerInterval);
            visualizerInterval = null;
            bars.forEach(bar => bar.style.height = '4px');
          }
        }
      });
    });

    const albumArt = document.getElementById('albumArt');
    if (albumArt) {
      observer.observe(albumArt, { attributes: true });
    }
  }

  // ============================================================
  // INICIALIZAÇÃO
  // ============================================================
  function init() {
    console.log('[Radio] Inicializando Player Unificado v7...');

    state.sessionId = getSessionId();
    console.log('[Radio] Session ID:', state.sessionId);

    // Conecta WebSocket
    connectWebSocket();

    // Configura botão play (apenas UM listener)
    const playBtn = document.getElementById('playBtn');
    if (playBtn) {
      playBtn.addEventListener('click', () => {
        hideInteractionOverlay();
        togglePlay();
      });
    }

    // Overlay de interação
    const interactionBtn = document.getElementById('interactionBtn');
    if (interactionBtn) {
      interactionBtn.addEventListener('click', () => {
        hideInteractionOverlay();
        playStream();
      });
    }

    // Volume
    const volumeSlider = document.getElementById('volumeSlider');
    const muteBtn = document.getElementById('muteBtn');

    if (volumeSlider) {
      const savedVol = localStorage.getItem(CONFIG.VOLUME_KEY);
      if (savedVol !== null) {
        volumeSlider.value = savedVol;
      }

      volumeSlider.addEventListener('input', function() {
        const vol = this.value / 100;
        if (state.audio) state.audio.volume = vol;
        localStorage.setItem(CONFIG.VOLUME_KEY, this.value);
      });
    }

    if (muteBtn) {
      muteBtn.addEventListener('click', function() {
        if (state.audio) {
          state.audio.muted = !state.audio.muted;
          this.textContent = state.audio.muted ? '\uD83D\uDD07' : '\uD83D\uDD0A';
          this.classList.toggle('muted', state.audio.muted);
        }
      });
    }

    // Inicia chat
    initChat();

    // Inicia visualizer
    initVisualizer();

    // Atualiza display inicial
    updateStatusBadge();
    startKeepalive();

    // Visibility API
    document.addEventListener('visibilitychange', handleVisibilityChange);

    // Tenta iniciar automaticamente se usuário já interagiu antes
    const hasInteracted = localStorage.getItem(CONFIG.INTERACTED_KEY);
    if (hasInteracted) {
      setTimeout(() => playStream(), 1000);
    }

    // Marca interação no primeiro click
    document.addEventListener('click', function markInteracted() {
      localStorage.setItem(CONFIG.INTERACTED_KEY, 'true');
      document.removeEventListener('click', markInteracted);
    }, { once: true });

    console.log('[Radio] Player Unificado v7 inicializado com sucesso!');
  }

  // Inicia quando DOM estiver pronto
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Cleanup ao sair
  window.addEventListener('beforeunload', () => {
    stopHeartbeat();
    stopSyncTimer();
    stopKeepalive();
    destroyAllAudio();
    if (state.ws) {
      try { state.ws.close(); } catch (e) {}
    }
  });

  // API Global
  window.RadioApp = {
    version: '7.0-unified',
    state: state,
    play: playStream,
    pause: pauseStream,
    toggle: togglePlay,
    reconnect: connectWebSocket,
    reconnectAudio: reconnectAudio,
    getStats: () => state.listeners,
    getTrack: () => state.currentTrack,
    config: CONFIG
  };

})();