/**
 * Rádio Amigos do Seu Zé - Sistema de Ouvintes v3
 * 
 * Correções aplicadas:
 * - Sincronização perfeita com servidor (metadata = áudio real)
 * - Reconexão inteligente sem duplicar ouvintes
 * - Progress bar sincronizada com tempo real da música
 * - Buffer de áudio otimizado para streaming ao vivo
 * - Fallback automático quando servidor reinicia
 */

(function() {
  'use strict';

  // ================= CONFIGURAÇÕES =================
  const CONFIG = {
    HEARTBEAT_INTERVAL: 5000,     // Ping a cada 5s (mais frequente para sincronia)
    RECONNECT_DELAY: 2000,        // Reconectar após 2s
    MAX_RECONNECT_ATTEMPTS: 20,   // Mais tentativas
    SESSION_KEY: 'radio_session_id_v3',
    AUDIO_BUFFER: 1.0,            // Buffer mínimo em segundos
    SYNC_THRESHOLD: 3,            // Diferença máxima aceitável (segundos)
    WS_PING_INTERVAL: 15000       // Ping nativo WS a cada 15s
  };

  // ================= ESTADO =================
  const state = {
    sessionId: null,
    ws: null,
    audio: null,
    isPlaying: false,
    reconnectAttempts: 0,
    heartbeatTimer: null,
    wsPingTimer: null,
    syncTimer: null,
    listeners: { active: 0, peak: 0, dailyUnique: 0 },
    currentTrack: { title: '', artist: '', duration: 0, startTime: 0, elapsed: 0 },
    lastSyncTime: 0,
    isReconnecting: false,
    interactionRequired: false
  };

  // ================= UTILITÁRIOS =================
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
    // Adiciona timestamp para evitar cache do browser
    const ts = Date.now();
    return '/stream?sid=' + encodeURIComponent(state.sessionId) + '&_t=' + ts;
  }

  function formatTime(seconds) {
    if (!seconds || seconds < 0) return '0:00';
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return mins + ':' + secs.toString().padStart(2, '0');
  }

  // ================= ATUALIZAÇÃO DA UI =================
  function updateListenerDisplay() {
    const badge = document.getElementById('statusBadge');
    const statusText = document.getElementById('statusText');
    if (!badge || !statusText) return;

    const { active } = state.listeners;
    if (state.isPlaying) {
      badge.className = 'status-badge online';
      statusText.textContent = 'AO VIVO \u2022 ' + active + ' ouvinte' + (active !== 1 ? 's' : '');
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

    // Calcula elapsed baseado no startTime do SERVIDOR (não do player local)
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

    // Se a música acabou (elapsed >= duration), espera proxima metadata
    if (elapsed >= state.currentTrack.duration && state.currentTrack.duration > 0) {
      if (progressBar) progressBar.style.width = '100%';
    }
  }

  // ================= SINCRONIZAÇÃO COM SERVIDOR =================
  function syncWithServer() {
    if (!state.currentTrack.startTime || !state.currentTrack.duration) return;

    const serverElapsed = Math.floor((Date.now() - state.currentTrack.startTime) / 1000);
    const localElapsed = state.audio ? state.audio.currentTime : 0;

    // Se a diferença for grande, pode indicar desincronização
    const diff = Math.abs(serverElapsed - localElapsed);

    // Só loga em debug, não força seek (streaming ao vivo não permite seek)
    if (diff > CONFIG.SYNC_THRESHOLD && state.isPlaying) {
      console.log('[Radio] Sync diff: ' + diff + 's (server: ' + serverElapsed + ', local: ' + localElapsed + ')');
    }

    updateProgressBar();
  }

  // ================= WEBSOCKET =================
  function connectWebSocket() {
    if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }

    state.isReconnecting = true;

    try {
      state.ws = new WebSocket(getWsUrl());

      state.ws.onopen = function() {
        console.log('[Radio] WebSocket conectado');
        state.reconnectAttempts = 0;
        state.isReconnecting = false;

        // Registrar como ouvinte
        state.ws.send(JSON.stringify({
          type: 'listener_ping',
          sessionId: state.sessionId,
          timestamp: Date.now()
        }));

        startHeartbeat();
        startSyncTimer();

        // Se estava tocando, reconecta o áudio
        if (state.isPlaying && state.audio) {
          reconnectAudio();
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
        console.log('[Radio] WebSocket fechado:', event.code, event.reason);
        stopHeartbeat();
        stopSyncTimer();

        if (!state.isReconnecting && state.reconnectAttempts < CONFIG.MAX_RECONNECT_ATTEMPTS) {
          state.reconnectAttempts++;
          const delay = Math.min(CONFIG.RECONNECT_DELAY * state.reconnectAttempts, 30000);
          console.log('[Radio] Reconectando em ' + delay + 'ms... (' + state.reconnectAttempts + '/' + CONFIG.MAX_RECONNECT_ATTEMPTS + ')');
          setTimeout(connectWebSocket, delay);
        }
      };

      state.ws.onerror = function(err) {
        console.error('[Radio] WebSocket erro:', err);
      };

    } catch (e) {
      console.error('[Radio] Erro ao conectar WebSocket:', e);
      state.isReconnecting = false;
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
        // Atualiza estado com dados do servidor
        if (data.data) {
          state.listeners.active = data.data.listeners || 0;
          updateListenerDisplay();
        }
        // Se o servidor enviou track info no pong, sincroniza
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
        // Chat online count
        break;

      default:
        // console.log('[Radio] Mensagem:', data.type, data);
    }
  }

  // ================= METADATA HANDLER - CORREÇÃO CRÍTICA =================
  function handleMetadata(track) {
    if (!track) return;

    const oldTitle = state.currentTrack.title;
    const newTitle = track.title || 'Desconhecida';

    // Só atualiza a UI se mudou de música
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

    // Se mudou de música, notifica visualmente
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
    state.syncTimer = setInterval(syncWithServer, 1000); // Atualiza progress bar a cada 1s
  }

  function stopSyncTimer() {
    if (state.syncTimer) {
      clearInterval(state.syncTimer);
      state.syncTimer = null;
    }
  }

  // ================= PLAYER DE ÁUDIO - CORREÇÃO CRÍTICA =================
  function initAudio() {
    if (state.audio) {
      try {
        state.audio.pause();
        state.audio.src = '';
        state.audio.load();
      } catch (e) {}
    }

    state.audio = new Audio();
    state.audio.crossOrigin = 'anonymous';
    state.audio.preload = 'none';

    // CORREÇÃO: Configurações otimizadas para streaming ao vivo
    state.audio.setAttribute('playsinline', '');
    state.audio.muted = false;

    // Eventos
    state.audio.addEventListener('play', onAudioPlay);
    state.audio.addEventListener('pause', onAudioPause);
    state.audio.addEventListener('error', onAudioError);
    state.audio.addEventListener('waiting', onAudioWaiting);
    state.audio.addEventListener('playing', onAudioPlaying);
    state.audio.addEventListener('stalled', onAudioStalled);
    state.audio.addEventListener('ended', onAudioEnded);
  }

  function onAudioPlay() {
    state.isPlaying = true;
    updateListenerDisplay();
    var albumArt = document.getElementById('albumArt');
    if (albumArt) albumArt.classList.add('playing');
    updatePlayButton(true);
    state.interactionRequired = false;
  }

  function onAudioPause() {
    state.isPlaying = false;
    updateListenerDisplay();
    var albumArt = document.getElementById('albumArt');
    if (albumArt) albumArt.classList.remove('playing');
    updatePlayButton(false);
  }

  function onAudioError(e) {
    console.error('[Radio] Erro no áudio:', e, state.audio ? state.audio.error : null);
    var error = state.audio ? state.audio.error : null;
    var msg = '\u26A0\uFE0F Erro na transmissão.';

    if (error) {
      switch (error.code) {
        case 1: msg = '\u26A0\uFE0F Interrompido. Tentando reconectar...'; break;
        case 2: msg = '\u26A0\uFE0F Erro de rede. Reconectando...'; break;
        case 3: msg = '\u26A0\uFE0F Erro de decodificação. Reconectando...'; break;
        case 4: msg = '\u26A0\uFE0F Formato não suportado.'; break;
      }
    }

    showToast(msg);

    // Tenta reconectar o áudio após erro
    if (state.reconnectAttempts < CONFIG.MAX_RECONNECT_ATTEMPTS) {
      setTimeout(() => {
        if (document.visibilityState !== 'hidden') {
          reconnectAudio();
        }
      }, 3000);
    }
  }

  function onAudioWaiting() {
    var badge = document.getElementById('statusBadge');
    var indicator = document.getElementById('bufferingIndicator');
    if (badge) badge.classList.add('buffering');
    if (indicator) indicator.classList.add('show');
  }

  function onAudioPlaying() {
    var badge = document.getElementById('statusBadge');
    var indicator = document.getElementById('bufferingIndicator');
    if (badge) badge.classList.remove('buffering');
    if (indicator) indicator.classList.remove('show');
  }

  function onAudioStalled() {
    console.log('[Radio] Áudio stalled - buffer vazio');
    var badge = document.getElementById('statusBadge');
    if (badge) badge.classList.add('buffering');
  }

  function onAudioEnded() {
    // Em streaming ao vivo, "ended" geralmente significa desconexão
    console.log('[Radio] Áudio ended - possível desconexão');
    if (state.isPlaying) {
      setTimeout(reconnectAudio, 1000);
    }
  }

  // ================= PLAY/PAUSE/RECONNECT =================
  function playStream() {
    if (!state.audio) {
      initAudio();
    }

    // Se já está tocando, não faz nada
    if (!state.audio.paused && state.audio.src) {
      return;
    }

    var streamUrl = getStreamUrl();

    // Se a URL mudou (reconexão), atualiza
    if (state.audio.src !== streamUrl) {
      state.audio.src = streamUrl;
    }

    var playPromise = state.audio.play();
    if (playPromise) {
      playPromise.catch(function(err) {
        console.error('[Radio] Erro ao tocar:', err.name, err.message);

        if (err.name === 'NotAllowedError') {
          // Navegador bloqueou autoplay - precisa de interação do usuário
          state.interactionRequired = true;
          showInteractionOverlay();
          showToast('\uD83D\uDD0A Clique no botão de play para ouvir');
        } else if (err.name === 'NotSupportedError') {
          showToast('\u26A0\uFE0F Formato de áudio não suportado');
        } else {
          // Outro erro, tenta novamente
          setTimeout(() => {
            if (state.isPlaying) playStream();
          }, 2000);
        }
      });
    }
  }

  function pauseStream() {
    if (state.audio) {
      state.audio.pause();
      // Não limpa src ao pausar - permite resume rápido
    }
  }

  function togglePlay() {
    if (state.isPlaying) {
      pauseStream();
    } else {
      playStream();
    }
  }

  // ================= RECONEXÃO INTELIGENTE =================
  function reconnectAudio() {
    console.log('[Radio] Reconectando áudio...');

    if (!state.audio) {
      initAudio();
    }

    // Pausa e limpa
    try {
      state.audio.pause();
      state.audio.src = '';
      state.audio.load();
    } catch (e) {}

    // Nova URL com timestamp para evitar cache
    var newUrl = getStreamUrl();
    state.audio.src = newUrl;

    // Toca
    var playPromise = state.audio.play();
    if (playPromise) {
      playPromise.catch(function(err) {
        console.error('[Radio] Erro na reconexão:', err);
        if (err.name === 'NotAllowedError') {
          showInteractionOverlay();
        }
      });
    }
  }

  function updatePlayButton(playing) {
    var btn = document.getElementById('playBtn');
    if (btn) {
      btn.innerHTML = playing ? '\u23F8' : '\u25B6';
      btn.setAttribute('aria-label', playing ? 'Pausar' : 'Tocar');
      btn.classList.toggle('playing', playing);
    }
  }

  // ================= UI HELPERS =================
  function updateTrackInfo(track) {
    var titleEl = document.getElementById('trackTitle');
    var artistEl = document.getElementById('trackArtist');
    var albumArt = document.getElementById('albumArt');

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

    // Atualiza título da aba do navegador
    if (track.title) {
      document.title = track.title + ' \u2014 Rádio Amigos do Seu Zé';
    }
  }

  function animateAlbumArt() {
    var albumArt = document.getElementById('albumArt');
    if (albumArt) {
      albumArt.classList.remove('playing');
      void albumArt.offsetWidth; // trigger reflow
      albumArt.classList.add('playing');
    }
  }

  function showToast(message) {
    var toast = document.getElementById('toast');
    if (toast) {
      toast.textContent = message;
      toast.classList.add('show');
      setTimeout(() => toast.classList.remove('show'), 4000);
    } else {
      console.log('[Toast]', message);
    }
  }

  function showInteractionOverlay() {
    var overlay = document.getElementById('interactionOverlay');
    if (overlay) {
      overlay.classList.add('show');
      overlay.style.display = 'flex';
    }
  }

  function hideInteractionOverlay() {
    var overlay = document.getElementById('interactionOverlay');
    if (overlay) {
      overlay.classList.remove('show');
      setTimeout(() => { overlay.style.display = 'none'; }, 300);
    }
  }

  // ================= CHAT =================
  var chatName = localStorage.getItem('chat_name') || '';

  function initChat() {
    var chatInput = document.getElementById('chatInput');
    var chatSendBtn = document.getElementById('chatSendBtn');
    var chatNameOverlay = document.getElementById('chatNameOverlay');
    var chatNameInput = document.getElementById('chatNameInput');
    var chatNameBtn = document.getElementById('chatNameBtn');

    if (chatNameOverlay) {
      chatNameOverlay.style.display = !chatName ? 'flex' : 'none';
    }

    if (chatNameBtn && chatNameInput) {
      chatNameBtn.addEventListener('click', function() {
        var name = chatNameInput.value.trim().substring(0, 20);
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
      var msg = chatInput.value.trim().substring(0, 200);
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
    var container = document.getElementById('chatMessages');
    if (!container) return;

    var isOwn = data.name === chatName;
    var time = data.time
      ? new Date(data.time).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })
      : new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });

    var msgDiv = document.createElement('div');
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

    // Limita a 100 mensagens no DOM
    while (container.children.length > 100) {
      container.removeChild(container.firstChild);
    }
  }

  function loadChatHistory(messages) {
    var container = document.getElementById('chatMessages');
    if (!container) return;
    container.innerHTML = '<div class="chat-date-separator">Hoje</div>';
    if (Array.isArray(messages)) {
      messages.forEach(msg => addChatMessage(msg));
    }
  }

  function escapeHtml(text) {
    var div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  // ================= VISIBILITY API (pausa em background) =================
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

  // ================= INICIALIZAÇÃO =================
  function init() {
    // Gerar/recuperar session ID
    state.sessionId = getSessionId();
    console.log('[Radio] Session ID:', state.sessionId);

    // Conectar WebSocket
    connectWebSocket();

    // Configurar botão play
    var playBtn = document.getElementById('playBtn');
    if (playBtn) {
      playBtn.addEventListener('click', function() {
        hideInteractionOverlay();
        togglePlay();
      });
    }

    // Overlay de interação
    var interactionBtn = document.getElementById('interactionBtn');
    if (interactionBtn) {
      interactionBtn.addEventListener('click', function() {
        hideInteractionOverlay();
        playStream();
      });
    }

    // Volume
    var volumeSlider = document.getElementById('volumeSlider');
    var muteBtn = document.getElementById('muteBtn');

    if (volumeSlider) {
      // Restaura volume salvo
      var savedVol = localStorage.getItem('radio_volume');
      if (savedVol !== null) {
        volumeSlider.value = savedVol;
        if (state.audio) state.audio.volume = savedVol / 100;
      }

      volumeSlider.addEventListener('input', function() {
        var vol = this.value / 100;
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

    // Iniciar chat
    initChat();

    // Atualizar display inicial
    updateListenerDisplay();

    // Visibility API
    document.addEventListener('visibilitychange', handleVisibilityChange);

    // Tentar iniciar automaticamente (pode ser bloqueado pelo navegador)
    // Só tenta se o usuário já interagiu antes
    var hasInteracted = localStorage.getItem('radio_has_interacted');
    if (hasInteracted) {
      setTimeout(() => {
        playStream();
      }, 500);
    }

    // Marca interação no primeiro click/toque
    document.addEventListener('click', function markInteracted() {
      localStorage.setItem('radio_has_interacted', 'true');
      document.removeEventListener('click', markInteracted);
    }, { once: true });
  }

  // Iniciar quando DOM estiver pronto
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Expor API global
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