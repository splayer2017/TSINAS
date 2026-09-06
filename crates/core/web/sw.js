// SW mínimo: cachea solo la shell (/, manifest). Nunca /api/* ni /stream/*.
// Bump de versión en cada cambio de UI para forzar actualización en clientes.
const SHELL = ["/", "/manifest.json"];
const CACHE = "baul-v5";
self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting()));
});
self.addEventListener("activate", (e) =>
  e.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  )
);
self.addEventListener("fetch", (e) => {
  const u = new URL(e.request.url);
  if (u.pathname.startsWith("/stream/") || u.pathname.startsWith("/api/")) return; // red, sin caché
  e.respondWith(caches.match(e.request).then((h) => h || fetch(e.request)));
});
