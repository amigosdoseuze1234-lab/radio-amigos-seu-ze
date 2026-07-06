/* ============================================================
   CLIENTE WEBSOCKET - RÁDIO AMIGOS DO SEU ZÉ
   Com reconexão exponencial e heartbeat
   ============================================================ */

class RadioClient {
  constructor(wsUrl) {
    this.wsUrl = wsUrl;
    this.ws = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 10;
    this.baseReconnectDelay = 1000;  // 1 segundo
    this.maxReconnectDelay = 30000;  // 30 segundos
    this.reconnectTimer = null;
    this.heartbeatInterval = null;
    this.heartbeatTimeout = null;
    this.HEARTBEAT_INTERVAL = 25000;  // 25s (menor que o servidor: 30s)
    this.HEARTBEAT_TIMEOUT = 10000;    // 10s para responder pong
    this.listeners = {};
    this.isManualClose = false;
  }

  // Event emitter simples
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

        // Responder ping do servidor
        if (data.type === 'ping') {
          this.send({ type: 'pong', time: Date.now() });
          return;
        }

        // Reset heartbeat no pong
        if (data.type === 'pong') {
          this.resetHeartbeatTimeout();
          return;
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

      // Não reconectar se foi fechamento manual
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

    // Enviar ping para o servidor a cada 25s
    this.heartbeatInterval = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.send({ type: 'ping', time: Date.now() });
        // Se não receber pong em 10s, forçar reconexão
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
}

// ===== USO NO FRONTEND =====
// const client = new RadioClient('wss://seu-render-url.onrender.com');
// client.on('connected', () => console.log('Conectado!'));
// client.on('metadata', (data) => console.log('Música:', data.data.title));
// client.on('chat', (data) => console.log(`${data.name}: ${data.message}`));
// client.connect();