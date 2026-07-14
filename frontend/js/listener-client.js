/**
 * Rádio Amigos do Seu Zé - Sistema de Ouvintes v6
 * SOLUCAO DEFINITIVA para "nao abre as musicas / fica carregando"
 *
 * Problema: Audio element entra em estado invalido quando o stream cai.
 * Solucao: SEMPRE criar audio element NOVO. Nunca reutilizar.
 *          Reconexao automatica invisivel ao usuario.
 */

(function() {
  'use strict';

  // ================= CONFIGURACOES =================
  const CONFIG = {
    HEARTBEAT_INTERVAL: 5000,
    RECONNECT_DELAY: 1500,
    MAX_RECONNECT_ATTEMPTS: 50,
    SESSION_KEY: 'radio_session_id_v6',
    STALL_CHECK_INTERVAL: 4000,
    STALL_THRESHOLD: 2,
    KEEPALIVE_INTERVAL: 5000
  };

  // ================= ESTADO =================
  const state = {
    sessionId: null,
    ws: null,
    audio: null,
    audioContainer: null,
    isPlaying: false,
    reconnectAttempts: 0,
    heartbeatTimer: null,
    syncTimer: null,
    stallTimer: null,
    keepaliveTimer: null,
    listeners: { active: 0, peak: 0, dailyUnique: 0 },
    currentTrack: { title: '', artist: '', duration: 0, startTime: 0, elapsed: 0 },
    isReconnecting: false,
    interactionRequired: false,
    stallCount: 0,
    lastCurrentTime: 0,
    reconnectCount: 0,
    firstPlay: true
  };

  // ================= UTILITARIOS =================
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

  // ================= UI UPDATES =================
  function updateListenerDisplay() {
    const badge = document.getElementById('statusBadge');
    const statusText = document.getElementById('statusText');
    if (!badge || !statusText) return;

    const { active } = state.listeners;
    if (state.isPlaying && !state.isReconnecting) {
      badge.className = 'status-badge online';
      statusText.textContent = 'AO VIVO \u2022 ' + active + ' ouvinte' + (active !== 1 ? 's' : '');
    } else if (state.isReconnecting) {
      badge.className = 'status-badge buffering';
      statusText.textContent = 'CONECTANDO...';
    } else {
      badge.className = 'status-badge offline';
      statusText.textContent = 'OFFLINE';
    }

    const infoEl = document.getElementById('listenerInfo');
    if (infoEl) {
      infoEl.innerHTML = '<span>\uD83D\uDC65 ' + active + ' online</span>' +
        '<span>\uD83C\uDFC6 Pico: ' + state.listeners.peak + '</span>' +
        '<span>\uD83D\uDCCA Hoje: ' + state.listeners.dailyUnique + '</span>';
    }
  }

  function updateProgressBar() {
    const progressBar = document.getElementById('progressBar');
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

    if (progressBar) {
      progressBar.style.width = progress + '%';
      progressBar.setAttribute('aria-valuenow', Math.round(progress));
    }
    if (currentTimeEl) currentTimeEl.textContent = formatTime(elapsed);
    if (totalTimeEl) totalTimeEl.textContent = formatTime(state.currentTrack.duration);

    if (elapsed >= state.currentTrack.duration && state.currentTrack.duration > 0) {
      if (progressBar) progressBar.style.width = '100%';
    }
  }

  // ================= WEBSOCKET =================
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
      };

      state.ws.onmessage = function(event) {
        try {
          const data = JSON.parse(event.data);
          handleWsMessage(data);
        } catch (e) {
          console.warn('[Radio] Mensagem invalida:', e);
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
          updateListenerDisplay();
        }
        break;

      case 'metadata':
        handleMetadata(data.data);
        break;

      case 'listener_pong':
        if (data.data) {
          state.listeners.active = data.data.listeners || 0;
          updateListenerDisplay();
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

      case 'online_count':
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

  // ================= PLAYER v6 - SOLUCAO DEFINITIVA =================
  // A causa raiz do "fica carregando":
  // 1. O Audio element reutilizado entra em estado invalido
  // 2. Event listeners acumulam e causam multiplas reconexoes
  // 3. O browser nao consegue decodificar MP3 quando o stream reinicia
  //
  // SOLUCAO: Criar container para audio elements e sempre usar um NOVO.

  function getAudioContainer() {
    if (!state.audioContainer) {
      state.audioContainer = document.createElement('div');
      state.audioContainer.id = 'radio-audio-container';
      state.audioContainer.style.display = 'none';
      document.body.appendChild(state.audioContainer);
    }
    return state.audioContainer;
  }

  function destroyAllAudioElements() {
    const container = getAudioContainer();
    while (container.firstChild) {
      const audio = container.firstChild;
      try {
        audio.pause();
        audio.src = '';
        audio.load();
      } catch (e) {}
      // Limpa todos os event handlers
      audio.onplay = null;
      audio.onpause = null;
      audio.onerror = null;
      audio.onwaiting = null;
      audio.onplaying = null;
      audio.onstalled = null;
      audio.onended = null;
      audio.onsuspend = null;
      audio.oncanplay = null;
      audio.onloadstart = null;
      audio.onprogress = null;
      audio.onabort = null;
      audio.onemptied = null;
      audio.onloadedmetadata = null;
      audio.onloadeddata = null;
      audio.oncanplaythrough = null;
      audio.ondurationchange = null;
      audio.ontimeupdate = null;
      container.removeChild(audio);
    }
    state.audio = null;
    console.log('[Radio] Todos os audio elements destruidos');
  }

  function createNewAudio() {
    // Destroi TODOS os audios anteriores
    destroyAllAudioElements();

    const container = getAudioContainer();
    const audio = document.createElement('audio');

    // Configuracoes otimizadas
    audio.crossOrigin = 'anonymous';
    audio.setAttribute('playsinline', '');
    audio.preload = 'auto';
    audio.muted = false;
    audio.volume = state.audio ? state.audio.volume : 1.0;

    // Event handlers inline (mais confiaveis)
    audio.onplay = function() {
      console.log('[Radio] === PLAY ===');
      state.isPlaying = true;
      state.isReconnecting = false;
      state.stallCount = 0;
      state.reconnectCount = 0;
      updateListenerDisplay();
      const albumArt = document.getElementById('albumArt');
      if (albumArt) albumArt.classList.add('playing');
      updatePlayButton(true);
      state.interactionRequired = false;
      state.firstPlay = false;
    };

    audio.onpause = function() {
      console.log('[Radio] === PAUSE ===');
      state.isPlaying = false;
      updateListenerDisplay();
      const albumArt = document.getElementById('albumArt');
      if (albumArt) albumArt.classList.remove('playing');
      updatePlayButton(false);
    };

    audio.onerror = function(e) {
      const err = audio.error;
      console.error('[Radio] === ERROR === code:', err ? err.code : 'unknown');

      // NAO mostra erro ao usuario - reconecta silenciosamente
      if (!state.isReconnecting && state.isPlaying) {
        console.log('[Radio] Reconectando silenciosamente apos erro...');
        setTimeout(() => reconnectAudio(), 800);
      }
    };

    audio.onwaiting = function() {
      console.log('[Radio] === WAITING ===');
    };

    audio.onplaying = function() {
      console.log('[Radio] === PLAYING ===');
      state.stallCount = 0;
      state.isReconnecting = false;
    };

    audio.onstalled = function() {
      console.log('[Radio] === STALLED ===');
      if (!state.isReconnecting && state.isPlaying) {
        setTimeout(() => reconnectAudio(), 1000);
      }
    };

    audio.onended = function() {
      console.log('[Radio] === ENDED ===');
      if (state.isPlaying && !state.isReconnecting) {
        setTimeout(() => reconnectAudio(), 500);
      }
    };

    audio.onsuspend = function() {
      console.log('[Radio] === SUSPEND ===');
    };

    audio.oncanplay = function() {
      console.log('[Radio] === CANPLAY ===');
      state.isReconnecting = false;
    };

    audio.onloadstart = function() {
      console.log('[Radio] === LOADSTART ===');
    };

    audio.onabort = function() {
      console.log('[Radio] === ABORT ===');
    };

    audio.onemptied = function() {
      console.log('[Radio] === EMPTIED ===');
    };

    audio.onloadedmetadata = function() {
      console.log('[Radio] === LOADEDMETADATA === duration:', audio.duration);
    };

    audio.onloadeddata = function() {
      console.log('[Radio] === LOADEDDATA ===');
    };

    audio.oncanplaythrough = function() {
      console.log('[Radio] === CANPLAYTHROUGH ===');
    };

    container.appendChild(audio);
    state.audio = audio;

    console.log('[Radio] Novo audio element criado');
    return audio;
  }

  // ================= PLAY / PAUSE =================
  function playStream() {
    console.log('[Radio] playStream() chamado');

    // Se ja tem audio tocando, nao faz nada
    if (state.audio && !state.audio.paused && state.audio.src) {
      console.log('[Radio] Ja esta tocando');
      return;
    }

    // Cria novo audio se necessario
    if (!state.audio) {
      createNewAudio();
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
          showToast('\uD83D\uDD0A Clique no botao de play para ouvir');
        } else if (err.name === 'NotSupportedError') {
          showToast('\u26A0\uFE0F Formato nao suportado pelo navegador');
        } else {
          // Outro erro - tenta com novo audio element
          console.log('[Radio] Tentando com novo audio element...');
          setTimeout(() => {
            createNewAudio();
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

  // ================= RECONEXAO =================
  function reconnectAudio() {
    if (state.isReconnecting) {
      console.log('[Radio] Reconexao ja em andamento, ignorando');
      return;
    }

    state.isReconnecting = true;
    state.reconnectCount++;
    console.log('[Radio] ========== RECONEXAO #' + state.reconnectCount + ' ==========');

    // Destroi audio antigo e cria novo
    createNewAudio();

    // Nova URL
    const newUrl = getStreamUrl();
    state.audio.src = newUrl;
    console.log('[Radio] Nova URL:', newUrl);

    // Tenta tocar
    const playPromise = state.audio.play();
    if (playPromise) {
      playPromise.then(function() {
        console.log('[Radio] Reconectado com sucesso!');
        state.isReconnecting = false;
        state.reconnectCount = 0;
      }).catch(function(err) {
        console.error('[Radio] Erro na reconexao:', err.name);
        state.isReconnecting = false;

        if (err.name === 'NotAllowedError') {
          showInteractionOverlay();
        } else {
          // Tenta novamente
          setTimeout(() => reconnectAudio(), 2000);
        }
      });
    } else {
      state.isReconnecting = false;
    }
  }

  // ================= KEEPALIVE =================
  function startKeepalive() {
    stopKeepalive();
    state.lastCurrentTime = 0;
    state.stallCount = 0;

    state.keepaliveTimer = setInterval(function() {
      if (!state.audio || !state.isPlaying || state.audio.paused) return;

      const currentTime = state.audio.currentTime;

      // Se currentTime nao avancou, conta como stall
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

  // ================= UI HELPERS =================
  function updatePlayButton(playing) {
    const btn = document.getElementById('playBtn');
    if (btn) {
      btn.innerHTML = playing ? '\u23F8' : '\u25B6';
      btn.setAttribute('aria-label', playing ? 'Pausar' : 'Tocar');
      btn.classList.toggle('playing', playing);
    }
  }

  function updateTrackInfo(track) {
    const titleEl = document.getElementById('trackTitle');
    const artistEl = document.getElementById('trackArtist');

    if (titleEl) {
      titleEl.style.opacity = '0';
      setTimeout(function() {
        titleEl.textContent = track.title || 'Desconhecida';
        titleEl.style.opacity = '1';
      }, 200);
    }

    if (artistEl) {
      artistEl.style.opacity = '0';
      setTimeout(function() {
        artistEl.textContent = track.artist || 'Ponto de Umbanda';
        artistEl.style.opacity = '1';
      }, 200);
    }

    if (track.title) {
      document.title = track.title + ' \u2014 Rádio Amigos do Seu Zé';
    }
  }

  function animateAlbumArt() {
    const albumArt = document.getElementById('albumArt');
    if (albumArt) {
      albumArt.classList.remove('playing');
      void albumArt.offsetWidth;
      albumArt.classList.add('playing');
    }
  }

  function showToast(message) {
    const toast = document.getElementById('toast');
    if (toast) {
      toast.textContent = message;
      toast.classList.add('show');
      setTimeout(() => toast.classList.remove('show'), 4000);
    } else {
      console.log('[Toast]', message);
    }
  }

  function showInteractionOverlay() {
    const overlay = document.getElementById('interactionOverlay');
    if (overlay) {
      overlay.classList.add('show');
      overlay.style.display = 'flex';
    }
  }

  function hideInteractionOverlay() {
    const overlay = document.getElementById('interactionOverlay');
    if (overlay) {
      overlay.classList.remove('show');
      setTimeout(() => { overlay.style.display = 'none'; }, 300);
    }
  }

  // ================= CHAT =================
  let chatName = localStorage.getItem('chat_name') || '';

  function initChat() {
    const chatInput = document.getElementById('chatInput');
    const chatSendBtn = document.getElementById('chatSendBtn');
    const chatNameOverlay = document.getElementById('chatNameOverlay');
    const chatNameInput = document.getElementById('chatNameInput');
    const chatNameBtn = document.getElementById('chatNameBtn');

    if (chatNameOverlay) {
      chatNameOverlay.style.display = !chatName ? 'flex' : 'none';
    }

    if (chatNameBtn && chatNameInput) {
      chatNameBtn.addEventListener('click', function() {
        const name = chatNameInput.value.trim().substring(0, 20);
        if (name) {
          chatName = name;
          localStorage.setItem('chat_name', name);
          if (chatNameOverlay) chatNameOverlay.style.display = 'none';
          if (state.ws && state.ws.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify({ type: 'join_chat', name: name }));
          }
        }
      });

      chatNameInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') chatNameBtn.click();
      });
    }

    function sendChatMessage() {
      if (!chatInput || !chatName) return;
      const msg = chatInput.value.trim().substring(0, 200);
      if (!msg) return;

      if (state.ws && state.ws.readyState === WebSocket.OPEN) {
        state.ws.send(JSON.stringify({ type: 'chat', name: chatName, message: msg }));
        chatInput.value = '';
      }
    }

    if (chatSendBtn) {
      chatSendBtn.addEventListener('click', sendChatMessage);
    }

    if (chatInput) {
      chatInput.addEventListener('keypress', function(e) {
        if (e.key === 'Enter') sendChatMessage();
      });
    }
  }

  function addChatMessage(data) {
    const container = document.getElementById('chatMessages');
    if (!container) return;

    const isOwn = data.name === chatName;
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

  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  // ================= VISIBILITY API =================
  function handleVisibilityChange() {
    if (document.hidden) {
      console.log('[Radio] Pagina em background');
    } else {
      console.log('[Radio] Pagina visivel');
      if (state.ws && state.ws.readyState !== WebSocket.OPEN) {
        connectWebSocket();
      }
      if (state.isPlaying && state.audio && state.audio.paused) {
        reconnectAudio();
      }
    }
  }

  // ================= INICIALIZACAO =================
  function init() {
    state.sessionId = getSessionId();
    console.log('[Radio] Session ID:', state.sessionId);

    connectWebSocket();

    const playBtn = document.getElementById('playBtn');
    if (playBtn) {
      playBtn.addEventListener('click', function() {
        hideInteractionOverlay();
        togglePlay();
      });
    }

    const interactionBtn = document.getElementById('interactionBtn');
    if (interactionBtn) {
      interactionBtn.addEventListener('click', function() {
        hideInteractionOverlay();
        playStream();
      });
    }

    const volumeSlider = document.getElementById('volumeSlider');
    const muteBtn = document.getElementById('muteBtn');

    if (volumeSlider) {
      const savedVol = localStorage.getItem('radio_volume');
      if (savedVol !== null) {
        volumeSlider.value = savedVol;
      }

      volumeSlider.addEventListener('input', function() {
        const vol = this.value / 100;
        if (state.audio) state.audio.volume = vol;
        localStorage.setItem('radio_volume', this.value);
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

    initChat();
    updateListenerDisplay();
    startKeepalive();

    document.addEventListener('visibilitychange', handleVisibilityChange);

    // NAO inicia automaticamente na primeira vez
    // Espera o usuario clicar em play
    const hasInteracted = localStorage.getItem('radio_has_interacted');
    if (hasInteracted) {
      setTimeout(() => {
        playStream();
      }, 1000);
    }

    document.addEventListener('click', function markInteracted() {
      localStorage.setItem('radio_has_interacted', 'true');
      document.removeEventListener('click', markInteracted);
    }, { once: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  window.RadioApp = {
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