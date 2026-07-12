/**
 * Rádio Amigos do Seu Zé - Sistema de Ouvintes
 * 
 * Este módulo gerencia:
 * - Identificação única de sessão (evita duplicatas)
 * - Heartbeat com o servidor (mantém conexão viva)
 * - Reconexão automática
 * - Contagem precisa de ouvintes
 */

(function() {
  'use strict';

  // ================= CONFIGURAÇÕES =================
  const CONFIG = {
    HEARTBEAT_INTERVAL: 10000,    // Enviar ping a cada 10s
    RECONNECT_DELAY: 3000,        // Tentar reconectar após 3s
    MAX_RECONNECT_ATTEMPTS: 10,   // Máximo de tentativas
    SESSION_KEY: 'radio_session_id',
    WS_PING_INTERVAL: 25000      // Ping do WebSocket nativo
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
    listeners: {
      active: 0,
      peak: 0,
      dailyUnique: 0
    }
  };

  // ================= UTILITÁRIOS =================
  function generateSessionId() {
    return 'sess_' + Date.now() + '_' + Math.random().toString(36).substring(2, 11);
  }

  function getSessionId() {
    // Tentar recuperar do localStorage
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
    const sid = state.sessionId || getSessionId();
    return '/stream?sid=' + encodeURIComponent(sid);
  }

  // ================= ATUALIZAÇÃO DA UI =================
  function updateListenerDisplay() {
    const badge = document.getElementById('statusBadge');
    const statusText = document.getElementById('statusText');

    if (!badge || !statusText) return;

    const { active, peak, dailyUnique } = state.listeners;

    if (state.isPlaying) {
      badge.className = 'status-badge online';
      statusText.textContent = `AO VIVO • ${active} ouvinte${active !== 1 ? 's' : ''}`;
    } else {
      badge.className = 'status-badge offline';
      statusText.textContent = 'OFFLINE';
    }

    // Atualizar info adicional se existir elemento
    const infoEl = document.getElementById('listenerInfo');
    if (infoEl) {
      infoEl.innerHTML = `
        <span>👥 ${active} online</span>
        <span>🏆 Pico: ${peak}</span>
        <span>📊 Hoje: ${dailyUnique}</span>
      `;
    }
  }

  // ================= WEBSOCKET =================
  function connectWebSocket() {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      return;
    }

    try {
      state.ws = new WebSocket(getWsUrl());

      state.ws.onopen = function() {
        console.log('[Radio] WebSocket conectado');
        state.reconnectAttempts = 0;

        // Registrar como ouvinte
        state.ws.send(JSON.stringify({
          type: 'listener_join',
          sessionId: state.sessionId
        }));

        startHeartbeat();
      };

      state.ws.onmessage = function(event) {
        try {
          const data = JSON.parse(event.data);
          handleWsMessage(data);
        } catch (e) {
          console.warn('[Radio] Mensagem inválida:', e);
        }
      };

      state.ws.onclose = function() {
        console.log('[Radio] WebSocket fechado');
        stopHeartbeat();

        if (state.reconnectAttempts < CONFIG.MAX_RECONNECT_ATTEMPTS) {
          state.reconnectAttempts++;
          console.log(`[Radio] Reconectando... (${state.reconnectAttempts}/${CONFIG.MAX_RECONNECT_ATTEMPTS})`);
          setTimeout(connectWebSocket, CONFIG.RECONNECT_DELAY);
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
          state.listeners.peak = data.data.peakListeners || 0;
          state.listeners.dailyUnique = data.data.dailyUnique || 0;
          updateListenerDisplay();
        }
        break;

      case 'metadata':
        updateTrackInfo(data.data);
        break;

      case 'listener_confirmed':
        console.log('[Radio] Sessão confirmada:', data.sessionId);
        break;

      case 'listener_pong':
        // Heartbeat confirmado
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
        // console.log('[Radio] Mensagem:', data.type);
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

  // ================= PLAYER DE ÁUDIO =================
  function initAudio() {
    if (state.audio) {
      try {
        state.audio.pause();
        state.audio.src = '';
      } catch (e) {}
    }

    state.audio = new Audio();
    state.audio.crossOrigin = 'anonymous';
    state.audio.preload = 'none';

    // Eventos do player
    state.audio.addEventListener('play', function() {
      state.isPlaying = true;
      updateListenerDisplay();
      document.getElementById('albumArt')?.classList.add('playing');
      updatePlayButton(true);
    });

    state.audio.addEventListener('pause', function() {
      state.isPlaying = false;
      updateListenerDisplay();
      document.getElementById('albumArt')?.classList.remove('playing');
      updatePlayButton(false);
    });

    state.audio.addEventListener('error', function(e) {
      console.error('[Radio] Erro no áudio:', e);
      showToast('⚠️ Erro na transmissão. Tentando reconectar...');
      setTimeout(() => {
        if (state.isPlaying) {
          playStream();
        }
      }, 2000);
    });

    state.audio.addEventListener('waiting', function() {
      document.getElementById('statusBadge')?.classList.add('buffering');
    });

    state.audio.addEventListener('playing', function() {
      document.getElementById('statusBadge')?.classList.remove('buffering');
    });
  }

  function playStream() {
    if (!state.audio) {
      initAudio();
    }

    const streamUrl = getStreamUrl();

    if (state.audio.src !== streamUrl) {
      state.audio.src = streamUrl;
    }

    const playPromise = state.audio.play();
    if (playPromise) {
      playPromise.catch(err => {
        console.error('[Radio] Erro ao tocar:', err);
        // Mostrar overlay de interação se necessário
        showInteractionOverlay();
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

  function updatePlayButton(playing) {
    const btn = document.getElementById('playBtn');
    if (btn) {
      btn.innerHTML = playing ? '⏸' : '▶';
      btn.setAttribute('aria-label', playing ? 'Pausar' : 'Tocar');
    }
  }

  // ================= UI HELPERS =================
  function updateTrackInfo(track) {
    const titleEl = document.getElementById('trackTitle');
    const artistEl = document.getElementById('trackArtist');

    if (titleEl) titleEl.textContent = track.title || 'Desconhecida';
    if (artistEl) artistEl.textContent = track.artist || 'Ponto de Umbanda';
  }

  function showToast(message) {
    const toast = document.getElementById('toast');
    if (toast) {
      toast.textContent = message;
      toast.classList.add('show');
      setTimeout(() => toast.classList.remove('show'), 3000);
    }
  }

  function showInteractionOverlay() {
    const overlay = document.getElementById('interactionOverlay');
    if (overlay) {
      overlay.classList.add('show');
    }
  }

  function hideInteractionOverlay() {
    const overlay = document.getElementById('interactionOverlay');
    if (overlay) {
      overlay.classList.remove('show');
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

    if (chatNameOverlay && !chatName) {
      chatNameOverlay.style.display = 'flex';
    } else if (chatNameOverlay) {
      chatNameOverlay.style.display = 'none';
    }

    if (chatNameBtn && chatNameInput) {
      chatNameBtn.addEventListener('click', function() {
        const name = chatNameInput.value.trim().substring(0, 20);
        if (name) {
          chatName = name;
          localStorage.setItem('chat_name', name);
          if (chatNameOverlay) chatNameOverlay.style.display = 'none';
          if (state.ws) {
            state.ws.send(JSON.stringify({
              type: 'join_chat',
              name: name
            }));
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
        state.ws.send(JSON.stringify({
          type: 'chat',
          name: chatName,
          message: msg
        }));
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
    const time = new Date(data.time).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });

    const msgDiv = document.createElement('div');
    msgDiv.className = `chat-message ${isOwn ? 'own' : 'other'}`;
    msgDiv.innerHTML = `
      <div class="msg-sender">${escapeHtml(data.name)}</div>
      <div class="chat-bubble">
        ${escapeHtml(data.message)}
        <div class="chat-meta">
          ${time}
          ${isOwn ? '<span class="msg-checks sent">✓✓</span>' : ''}
        </div>
      </div>
    `;

    container.appendChild(msgDiv);
    container.scrollTop = container.scrollHeight;
  }

  function loadChatHistory(messages) {
    const container = document.getElementById('chatMessages');
    if (!container) return;
    container.innerHTML = '<div class="chat-date-separator">Hoje</div>';
    messages.forEach(msg => addChatMessage(msg));
  }

  function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  // ================= INICIALIZAÇÃO =================
  function init() {
    // Gerar/recuperar session ID
    state.sessionId = getSessionId();
    console.log('[Radio] Session ID:', state.sessionId);

    // Conectar WebSocket
    connectWebSocket();

    // Configurar botões
    const playBtn = document.getElementById('playBtn');
    if (playBtn) {
      playBtn.addEventListener('click', togglePlay);
    }

    // Overlay de interação
    const interactionBtn = document.getElementById('interactionBtn');
    if (interactionBtn) {
      interactionBtn.addEventListener('click', function() {
        hideInteractionOverlay();
        playStream();
      });
    }

    // Volume
    const volumeSlider = document.getElementById('volumeSlider');
    const muteBtn = document.getElementById('muteBtn');

    if (volumeSlider) {
      volumeSlider.addEventListener('input', function() {
        if (state.audio) {
          state.audio.volume = this.value / 100;
        }
      });
    }

    if (muteBtn) {
      muteBtn.addEventListener('click', function() {
        if (state.audio) {
          state.audio.muted = !state.audio.muted;
          this.textContent = state.audio.muted ? '🔇' : '🔊';
        }
      });
    }

    // Iniciar chat
    initChat();

    // Atualizar display inicial
    updateListenerDisplay();

    // Tentar autoplay (pode falhar por políticas do navegador)
    // playStream();
  }

  // Iniciar quando DOM estiver pronto
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Expor API global para debug
  window.RadioApp = {
    state,
    play: playStream,
    pause: pauseStream,
    toggle: togglePlay,
    reconnect: connectWebSocket,
    getStats: () => state.listeners
  };

})();