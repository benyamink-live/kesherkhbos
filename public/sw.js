/* Service Worker – התראות פוש */
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

self.addEventListener('push', (e) => {
  let d = {};
  try { d = e.data ? e.data.json() : {}; } catch { d = { title: 'קשר', body: e.data && e.data.text() }; }
  const title = (d.urgent ? '🚨 ' : '🔔 ') + (d.title || 'הודעה מהמנהל');
  e.waitUntil(self.registration.showNotification(title, {
    body: d.body || '',
    tag: d.id || 'kesher',
    renotify: true,
    requireInteraction: !!d.urgent,
    vibrate: d.urgent ? [500, 200, 500, 200, 500] : [200, 100, 200],
    icon: 'icon-192.png',
    badge: 'icon-192.png',
    dir: 'rtl',
    lang: 'he',
    data: { id: d.id },
    actions: [{ action: 'ack', title: 'קיבלתי ✓' }],
  }));
});

self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  const id = e.notification.data && e.notification.data.id;
  e.waitUntil((async () => {
    const all = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    if (e.action === 'ack' && id) for (const c of all) c.postMessage({ t: 'alert_ack', id });
    if (all.length) { await all[0].focus(); return; }
    await self.clients.openWindow('./');
  })());
});
