const CACHE_NAME = 'radio-amigos-seu-ze-v2';
const STATIC_CACHE = 'radio-amigos-seu-ze-static-v2';
const DYNAMIC_CACHE = 'radio-amigos-seu-ze-dynamic-v2';

const STATIC_URLS = [
  '/',
  '/index.html',
  '/css/style.css',
  '/js/app.js',
  'https://fonts.googleapis.com/css2?family=Righteous&family=Inter:wght@300;400;500;600;700&display=swap'
];

const NEVER_CACHE = [
  '/api/',
  '/socket.io/',
  'analytics',
  'track'
];

function shouldNeverCache(request) {
  const url = request.url;
  return NEVER_CACHE.some(pattern => url.includes(pattern));
}

function isStaticAsset(request) {
  const url = new URL(request.url);
  const staticExtensions = ['.css', '.js', '.woff', '.woff2', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.json'];
  return staticExtensions.some(ext => url.pathname.endsWith(ext));
}

// ===== INSTALL =====
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(STATIC_CACHE)
      .then(cache => {
        console.log('[SW] Caching static assets...');
        return cache.addAll(STATIC_URLS);
      })
      .catch(err => console.error('[SW] Install failed:', err))
  );
  self.skipWaiting();
});

// ===== ACTIVATE =====
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(cacheNames => {
        return Promise.all(
          cacheNames
            .filter(name => {
              // Remove todos os caches que não correspondem à versão atual
              return name !== STATIC_CACHE && name !== DYNAMIC_CACHE;
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
        return self.clients.matchAll({ type: 'window' });
      })
      .then(clients => {
        clients.forEach(client => {
          client.postMessage({
            type: 'SW_UPDATED',
            message: 'Nova versão disponível! Recarregue a página.'
          });
        });
      })
  );
});

// ===== FETCH =====
self.addEventListener('fetch', event => {
  const { request } = event;

  // Nunca cachear requisições de API/analytics
  if (shouldNeverCache(request)) {
    event.respondWith(fetch(request));
    return;
  }

  // Estratégia: Network First para arquivos que mudam com frequência (HTML, dados dinâmicos)
  // Stale While Revalidate para assets estáticos (CSS, JS, imagens, fonts)

  if (request.mode === 'navigate' || request.destination === 'document') {
    // HTML pages: Network First - sempre busca a versão mais recente
    event.respondWith(networkFirst(request));
  } else if (isStaticAsset(request)) {
    // CSS, JS, fonts, imagens: Stale While Revalidate - rápido + atualização em background
    event.respondWith(staleWhileRevalidate(request));
  } else {
    // Default: Cache First com fallback para network
    event.respondWith(cacheFirst(request));
  }
});

// ===== ESTRATÉGIAS =====

// Network First: tenta rede, se falhar usa cache (ideal para HTML)
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

// Stale While Revalidate: retorna cache imediatamente, atualiza em background (ideal para CSS/JS)
async function staleWhileRevalidate(request) {
  const cache = await caches.open(STATIC_CACHE);
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

  // Se tem no cache, retorna imediatamente e atualiza em background
  if (cachedResponse) {
    fetchPromise.catch(() => {}); // Silencia erro do background fetch
    return cachedResponse;
  }

  // Se não tem no cache, espera a rede
  return fetchPromise;
}

// Cache First: tenta cache primeiro, se não tiver vai na rede
async function cacheFirst(request) {
  const cachedResponse = await caches.match(request);
  if (cachedResponse) return cachedResponse;

  const networkResponse = await fetch(request);
  if (networkResponse && networkResponse.status === 200) {
    const cache = await caches.open(DYNAMIC_CACHE);
    cache.put(request, networkResponse.clone());
  }
  return networkResponse;
}

// ===== MESSAGE HANDLER =====
self.addEventListener('message', event => {
  if (event.data === 'SKIP_WAITING') {
    self.skipWaiting();
  }

  if (event.data === 'CHECK_UPDATE') {
    // Força verificação de atualização
    self.registration.update();
  }
});