const CACHE_VERSION = 'v3';
const STATIC_CACHE = `radio-amigos-seu-ze-static-${CACHE_VERSION}`;
const DYNAMIC_CACHE = `radio-amigos-seu-ze-dynamic-${CACHE_VERSION}`;
const IMAGE_CACHE = `radio-amigos-seu-ze-images-${CACHE_VERSION}`;

const STATIC_URLS = [
  '/',
  '/index.html',
  '/css/style.css',
  '/js/app.js',
  '/js/player.js',
  '/offline.html'
];

const NEVER_CACHE_PATTERNS = [
  '/api/',
  '/stream',
  '/socket.io/',
  'analytics',
  'track',
  '/ws',
  '/ws/',
  'wss://',
  'ws://'
];

const NEVER_CACHE_TYPES = [
  'audio/mpeg',
  'audio/mp3',
  'audio/ogg',
  'audio/aac',
  'video/mp4',
  'video/webm'
];

// ===== HELPER FUNCTIONS =====

function shouldNeverCache(request) {
  const url = request.url;
  const acceptHeader = request.headers.get('accept') || '';
  const rangeHeader = request.headers.get('range');

  // Nunca cachear requisições com Range header (streaming)
  if (rangeHeader) return true;

  // Nunca cachear conteúdo de áudio/vídeo
  if (NEVER_CACHE_TYPES.some(type => acceptHeader.includes(type))) return true;

  // Nunca cachear URLs de API, stream, WebSocket
  if (NEVER_CACHE_PATTERNS.some(pattern => url.includes(pattern))) return true;

  // Nunca cachear requisições POST, PUT, DELETE, PATCH
  if (['POST', 'PUT', 'DELETE', 'PATCH'].includes(request.method)) return true;

  return false;
}

function isStaticAsset(request) {
  const url = new URL(request.url);
  const staticExtensions = ['.css', '.js', '.woff', '.woff2', '.ttf', '.eot'];
  return staticExtensions.some(ext => url.pathname.endsWith(ext));
}

function isImageAsset(request) {
  const url = new URL(request.url);
  const imageExtensions = ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.webp', '.avif'];
  return imageExtensions.some(ext => url.pathname.endsWith(ext));
}

function isGoogleFont(request) {
  return request.url.includes('fonts.googleapis.com') || request.url.includes('fonts.gstatic.com');
}

// ===== INSTALL =====
self.addEventListener('install', event => {
  console.log('[SW] Installing...');

  event.waitUntil(
    caches.open(STATIC_CACHE)
      .then(cache => {
        console.log('[SW] Caching static assets...');
        return cache.addAll(STATIC_URLS);
      })
      .then(() => {
        console.log('[SW] Static assets cached successfully');
      })
      .catch(err => {
        console.error('[SW] Install failed:', err);
      })
  );

  self.skipWaiting();
});

// ===== ACTIVATE =====
self.addEventListener('activate', event => {
  console.log('[SW] Activating...');

  event.waitUntil(
    caches.keys()
      .then(cacheNames => {
        return Promise.all(
          cacheNames
            .filter(name => {
              // Remove todos os caches que não correspondem à versão atual
              return name !== STATIC_CACHE &&
                     name !== DYNAMIC_CACHE &&
                     name !== IMAGE_CACHE;
            })
            .map(name => {
              console.log('[SW] Deleting old cache:', name);
              return caches.delete(name);
            })
        );
      })
      .then(() => {
        console.log('[SW] Activated, old caches cleared');
        return self.clients.claim();
      })
      .then(() => {
        // Notifica todos os clients sobre a atualização
        return self.clients.matchAll({ type: 'window', includeUncontrolled: true });
      })
      .then(clients => {
        clients.forEach(client => {
          client.postMessage({
            type: 'SW_UPDATED',
            version: CACHE_VERSION,
            message: 'Nova versão disponível! Recarregue a página para atualizar.'
          });
        });
      })
      .catch(err => {
        console.error('[SW] Activation error:', err);
      })
  );
});

// ===== FETCH =====
self.addEventListener('fetch', event => {
  const { request } = event;

  // CORREÇÃO CRÍTICA: Nunca interceptar requisições de streaming ao vivo
  if (shouldNeverCache(request)) {
    // Para requisições de stream/áudio, apenas passa direto sem interceptar
    return;
  }

  // Google Fonts: Cache First com longo TTL
  if (isGoogleFont(request)) {
    event.respondWith(cacheFirstWithLongTTL(request));
    return;
  }

  // Assets estáticos (CSS, JS, fonts): Stale While Revalidate
  if (isStaticAsset(request)) {
    event.respondWith(staleWhileRevalidate(request, STATIC_CACHE));
    return;
  }

  // Imagens: Cache First com fallback para network
  if (isImageAsset(request)) {
    event.respondWith(cacheFirstWithNetworkFallback(request, IMAGE_CACHE));
    return;
  }

  // Navegação (HTML pages): Network First com fallback offline
  if (request.mode === 'navigate' || request.destination === 'document') {
    event.respondWith(networkFirstWithOfflineFallback(request));
    return;
  }

  // Default: Network First
  event.respondWith(networkFirst(request));
});

// ===== ESTRATÉGIAS DE CACHE =====

// Stale While Revalidate: retorna cache imediatamente, atualiza em background
async function staleWhileRevalidate(request, cacheName) {
  const cache = await caches.open(cacheName);
  const cachedResponse = await cache.match(request);

  const fetchPromise = fetch(request)
    .then(networkResponse => {
      if (networkResponse && networkResponse.status === 200) {
        cache.put(request, networkResponse.clone());
      }
      return networkResponse;
    })
    .catch(error => {
      console.log('[SW] Background fetch failed:', request.url);
      throw error;
    });

  if (cachedResponse) {
    fetchPromise.catch(() => {});
    return cachedResponse;
  }

  return fetchPromise;
}

// Cache First com fallback para network (para imagens)
async function cacheFirstWithNetworkFallback(request, cacheName) {
  const cache = await caches.open(cacheName);
  const cachedResponse = await cache.match(request);

  if (cachedResponse) {
    // Atualiza em background
    fetch(request)
      .then(networkResponse => {
        if (networkResponse && networkResponse.status === 200) {
          cache.put(request, networkResponse.clone());
        }
      })
      .catch(() => {});
    return cachedResponse;
  }

  const networkResponse = await fetch(request);
  if (networkResponse && networkResponse.status === 200) {
    cache.put(request, networkResponse.clone());
  }
  return networkResponse;
}

// Cache First com longo TTL (para Google Fonts)
async function cacheFirstWithLongTTL(request) {
  const cache = await caches.open(STATIC_CACHE);
  const cachedResponse = await cache.match(request);

  if (cachedResponse) {
    return cachedResponse;
  }

  const networkResponse = await fetch(request);
  if (networkResponse && networkResponse.status === 200) {
    cache.put(request, networkResponse.clone());
  }
  return networkResponse;
}

// Network First: tenta rede, se falhar usa cache
async function networkFirst(request) {
  try {
    const networkResponse = await fetch(request);
    if (networkResponse && networkResponse.status === 200) {
      const cache = await caches.open(DYNAMIC_CACHE);
      cache.put(request, networkResponse.clone());
    }
    return networkResponse;
  } catch (error) {
    console.log('[SW] Network failed, falling back to cache:', request.url);
    const cachedResponse = await caches.match(request);
    if (cachedResponse) return cachedResponse;
    throw error;
  }
}

// Network First com fallback offline (para páginas HTML)
async function networkFirstWithOfflineFallback(request) {
  try {
    const networkResponse = await fetch(request);
    if (networkResponse && networkResponse.status === 200) {
      const cache = await caches.open(DYNAMIC_CACHE);
      cache.put(request, networkResponse.clone());
    }
    return networkResponse;
  } catch (error) {
    console.log('[SW] Network failed, serving offline page');
    const cachedResponse = await caches.match(request);
    if (cachedResponse) return cachedResponse;

    // Fallback para página offline
    const offlineResponse = await caches.match('/offline.html');
    if (offlineResponse) return offlineResponse;

    // Último fallback: resposta HTML básica
    return new Response(
      `<!DOCTYPE html>
<html>
<head><title>Offline - Rádio Amigos do Seu Zé</title>
<style>
body{font-family:Inter,sans-serif;background:#1a1a2e;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;text-align:center}
.container{padding:2rem}
h1{color:#00d4aa;margin-bottom:1rem}
p{color:#aaa}
</style></head>
<body>
<div class="container">
<h1>📻 Você está offline</h1>
<p>Conecte-se à internet para ouvir a Rádio Amigos do Seu Zé.</p>
</div>
</body></html>`,
      {
        status: 200,
        headers: { 'Content-Type': 'text/html; charset=utf-8' }
      }
    );
  }
}

// ===== MESSAGE HANDLER =====
self.addEventListener('message', event => {
  if (event.data === 'SKIP_WAITING') {
    self.skipWaiting();
  }

  if (event.data === 'CHECK_UPDATE') {
    self.registration.update();
  }

  // Responde com informações do cache
  if (event.data === 'GET_CACHE_INFO') {
    caches.keys().then(cacheNames => {
      event.source.postMessage({
        type: 'CACHE_INFO',
        caches: cacheNames,
        version: CACHE_VERSION
      });
    });
  }
});

// ===== SYNC BACKGROUND (para notificações offline) =====
self.addEventListener('sync', event => {
  if (event.tag === 'sync-messages') {
    event.waitUntil(syncMessages());
  }
});

async function syncMessages() {
  // Implementar sincronização de mensagens de chat quando voltar online
  console.log('[SW] Background sync triggered');
}

// ===== PUSH NOTIFICATIONS =====
self.addEventListener('push', event => {
  const data = event.data ? event.data.json() : {};
  const options = {
    body: data.body || 'Rádio Amigos do Seu Zé',
    icon: '/img/icon-192x192.png',
    badge: '/img/badge-72x72.png',
    tag: data.tag || 'radio-notification',
    requireInteraction: false,
    data: data
  };

  event.waitUntil(
    self.registration.showNotification(data.title || 'Rádio Amigos do Seu Zé', options)
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(
    clients.openWindow('/')
  );
});