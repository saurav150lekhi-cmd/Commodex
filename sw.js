// Commodex Service Worker
const CACHE = 'commodex-v1';
const OFFLINE_URL = '/offline';

// App shell assets to cache on install
const SHELL = [
  '/app',
  '/offline',
  'https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&family=Inter:wght@300;400;500;600;700;800;900&family=Cormorant+Garamond:wght@300;400&display=swap',
];

// API routes — always network-first (fresh data)
const API_ROUTES = ['/data', '/prices', '/calendar', '/macro', '/signals', '/auth/me', '/auth/alerts'];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE).then(cache => cache.addAll(SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // Skip non-GET and cross-origin (except Google Fonts)
  if (request.method !== 'GET') return;
  if (url.origin !== location.origin && !url.hostname.includes('fonts.g')) return;

  // API routes: network-first, no cache fallback (show stale is risky for financial data)
  const isApi = API_ROUTES.some(p => url.pathname.startsWith(p));
  if (isApi) {
    event.respondWith(
      fetch(request).catch(() => new Response(JSON.stringify({ error: 'Offline' }), {
        headers: { 'Content-Type': 'application/json' }
      }))
    );
    return;
  }

  // Navigation: network-first, offline fallback page
  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request)
        .then(res => {
          // Cache fresh copy of app shell
          if (res.ok) {
            const clone = res.clone();
            caches.open(CACHE).then(c => c.put(request, clone));
          }
          return res;
        })
        .catch(() => caches.match(OFFLINE_URL))
    );
    return;
  }

  // Static assets: stale-while-revalidate
  event.respondWith(
    caches.match(request).then(cached => {
      const network = fetch(request).then(res => {
        if (res.ok) {
          caches.open(CACHE).then(c => c.put(request, res.clone()));
        }
        return res;
      });
      return cached || network;
    })
  );
});
