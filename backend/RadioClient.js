/* ============================================================
   CLIENTE WEBSOCKET - RÁDIO AMIGOS DO SEU ZÉ
   Com reconexão exponencial, heartbeat e sincronização temporal
   ============================================================ */

class RadioClient {
  constructor(wsUrl) {
    this.wsUrl = wsUrl;
    this.ws = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
    this.baseReconnectDelay = 1000;
    this.maxReconnectDelay = 30000;
    this.reconnectTimer = null;
    this.heartbeatInterval = null;
    this.heartbeatTimeout = null;
    this.HEARTBEAT_INTERVAL = 25000;
    this.HEARTBEAT_TIMEOUT = 10000;
    this.listeners = {};
    this.isManualClose = false;

    // NOVO: Estado de sincronização
    this.syncState = {
      trackStartTime: 0,
      serverElapsed: 0,
      currentTrack: null,
      listeners: 0
    };
  }

  on(event, callback) {
    if (!this.listeners[event]) this.listeners[event] = [];
    this.listeners[event].push(callback);
  }

  emit(event, data) {
    if (this.listeners[event]) {
      this.listeners[event].forEach(cb => cb(data));
    }
  }

  connect() {
    if (this.ws?.readyState === WebSocket.OPEN) return;

    this.isManualClose = false;
    console.log(`[WS] Conectando em ${this.wsUrl}...`);

    try {
      this.ws = new WebSocket(this.wsUrl);
    } catch (err) {
      console.error('[WS] Erro ao criar WebSocket:', err);
      this.scheduleReconnect();
      return;
    }

    this.ws.onopen = () => {
      console.log('[WS] Conectado!');
      this.reconnectAttempts = 0;
      this.emit('connected');
      this.startHeartbeat();
    };

    this.ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        if (data.type === 'ping') {
          this.send({ type: 'pong', time: Date.now() });
          return;
        }

        if (data.type === 'pong') {
          this.resetHeartbeatTimeout();
          return;
        }

        // NOVO: Processa sincronização de estado
        if (data.type === 'state' && data.data) {
          this.syncState.listeners = data.data.listeners || 0;
          this.syncState.trackStartTime = data.data.trackStartTime || 0;
          this.syncState.serverElapsed = data.data.elapsed || 0;
          this.syncState.currentTrack = data.data.currentTrack || null;
        }

        // NOVO: Processa metadados com timestamp
        if (data.type === 'metadata' && data.data) {
          this.syncState.currentTrack = data.data;
          this.syncState.trackStartTime = data.data.startTime || Date.now();
        }

        // NOVO: Processa listener_pong com dados de ouvintes
        if (data.type === 'listener_pong') {
          if (data.listeners !== undefined) {
            this.syncState.listeners = data.listeners;
          }
          if (data.trackStartTime) {
            this.syncState.trackStartTime = data.trackStartTime;
          }
          if (data.elapsed !== undefined) {
            this.syncState.serverElapsed = data.elapsed;
          }
        }

        this.emit('message', data);
        this.emit(data.type, data);
      } catch (err) {
        console.warn('[WS] Mensagem inválida:', event.data);
      }
    };

    this.ws.onerror = (err) => {
      console.error('[WS] Erro:', err);
      this.emit('error', err);
    };

    this.ws.onclose = (event) => {
      console.log(`[WS] Desconectado (code: ${event.code}, reason: ${event.reason})`);
      this.stopHeartbeat();
      this.emit('disconnected', event);

      if (!this.isManualClose) {
        this.scheduleReconnect();
      }
    };
  }

  send(data) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      try {
        this.ws.send(JSON.stringify(data));
      } catch (err) {
        console.error('[WS] Erro ao enviar:', err);
      }
    }
  }

  // ===== HEARTBEAT =====
  startHeartbeat() {
    this.stopHeartbeat();

    this.heartbeatInterval = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.send({ type: 'ping', time: Date.now() });
        this.heartbeatTimeout = setTimeout(() => {
          console.warn('[WS] Heartbeat timeout - forçando reconexão');
          this.ws?.close();
        }, this.HEARTBEAT_TIMEOUT);
      }
    }, this.HEARTBEAT_INTERVAL);
  }

  resetHeartbeatTimeout() {
    if (this.heartbeatTimeout) {
      clearTimeout(this.heartbeatTimeout);
      this.heartbeatTimeout = null;
    }
  }

  stopHeartbeat() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
    this.resetHeartbeatTimeout();
  }

  // ===== RECONEXÃO EXPONENCIAL =====
  scheduleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error('[WS] Máximo de tentativas atingido. Desistindo.');
      this.emit('failed');
      return;
    }

    const delay = Math.min(
      this.baseReconnectDelay * Math.pow(2, this.reconnectAttempts),
      this.maxReconnectDelay
    );

    this.reconnectAttempts++;
    console.log(`[WS] Reconectando em ${delay}ms (tentativa ${this.reconnectAttempts}/${this.maxReconnectAttempts})...`);
    this.emit('reconnecting', { attempt: this.reconnectAttempts, delay });

    this.reconnectTimer = setTimeout(() => {
      this.connect();
    }, delay);
  }

  disconnect() {
    this.isManualClose = true;
    this.stopHeartbeat();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close(1000, 'Desconexão manual');
      this.ws = null;
    }
  }

  // NOVO: Métodos de sincronização
  getSyncState() {
    return { ...this.syncState };
  }

  getCurrentTrack() {
    return this.syncState.currentTrack;
  }

  getListeners() {
    return this.syncState.listeners;
  }

  getElapsedTime() {
    if (!this.syncState.trackStartTime) return 0;
    return Math.floor((Date.now() - this.syncState.trackStartTime) / 1000);
  }
}

// ===== USO NO FRONTEND =====
// const client = new RadioClient('wss://seu-render-url.onrender.com');
// client.on('connected', () => console.log('Conectado!'));
// client.on('metadata', (data) => console.log('Música:', data.data.title));
// client.on('state', (data) => console.log('Ouvintes:', data.data.listeners));
// client.on('chat', (data) => console.log(`${data.name}: ${data.message}`));
// client.connect();

// Exportar para uso em módulos ou global
if (typeof module !== 'undefined' && module.exports) {
  module.exports = RadioClient;
}