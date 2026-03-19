// Codex Phone - Service Worker
// 功能：离线缓存 App Shell，Cache-First 策略

const CACHE_NAME = 'codex-v1';

// 预缓存的 App Shell 资源
const APP_SHELL = [
    '/',
    '/index.html',
    '/css/style.css',
    '/js/app.js',
    '/manifest.json',
    '/icons/icon-192.png',
    '/icons/icon-512.png'
];

// 需要缓存的外部资源域名（如 Google Fonts）
const CACHEABLE_ORIGINS = [
    'https://fonts.googleapis.com',
    'https://fonts.gstatic.com'
];

// 安装：预缓存 App Shell
self.addEventListener('install', (event) => {
    console.log('[SW] Installing, cache:', CACHE_NAME);
    event.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => cache.addAll(APP_SHELL))
            .then(() => self.skipWaiting()) // 立即激活新版本
    );
});

// 激活：清理旧缓存
self.addEventListener('activate', (event) => {
    console.log('[SW] Activating');
    event.waitUntil(
        caches.keys().then(keys =>
            Promise.all(
                keys.filter(k => k !== CACHE_NAME).map(k => {
                    console.log('[SW] Deleting old cache:', k);
                    return caches.delete(k);
                })
            )
        ).then(() => self.clients.claim()) // 接管所有页面
    );
});

// 请求拦截：Cache-First + Network Fallback
self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    // 跳过非 GET 请求（POST API 调用等）
    if (event.request.method !== 'GET') return;

    // 跳过 WebSocket 升级请求
    if (url.protocol === 'ws:' || url.protocol === 'wss:') return;

    // 跳过 API 请求（不缓存动态数据）
    if (url.pathname.startsWith('/v1/') || url.pathname.startsWith('/api/')) return;

    // 判断是否为可缓存的外部资源
    const isCacheableExternal = CACHEABLE_ORIGINS.some(o => url.href.startsWith(o));

    // 本站内资源 或 可缓存外部资源 → Cache-First
    if (url.origin === self.location.origin || isCacheableExternal) {
        event.respondWith(
            caches.match(event.request).then(cached => {
                if (cached) return cached;

                // 缓存未命中 → 网络请求并缓存
                return fetch(event.request).then(response => {
                    // 只缓存成功的响应
                    if (!response || response.status !== 200) return response;

                    const clone = response.clone();
                    caches.open(CACHE_NAME).then(cache => {
                        cache.put(event.request, clone);
                    });
                    return response;
                }).catch(() => {
                    // 网络失败 → 对导航请求返回离线页面
                    if (event.request.mode === 'navigate') {
                        return caches.match('/index.html');
                    }
                    return new Response('Offline', { status: 503 });
                });
            })
        );
    }
});

// 监听来自页面的消息
self.addEventListener('message', (event) => {
    if (event.data === 'skipWaiting') {
        self.skipWaiting();
    }
});
