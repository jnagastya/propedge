// ============================================================
// Value Alert — Service Worker for Push Notifications
// ============================================================

self.addEventListener('push', event => {
  let data = { title: 'Value Alert', body: 'You have a new notification' };
  try { data = event.data.json(); } catch {}

  const options = {
    body: data.body || '',
    icon: data.icon || '/icon-192.png',
    badge: '/icon-192.png',
    tag: data.tag || 'va-' + Date.now(),
    data: {
      url: data.url || '/',
      messageId: data.messageId || null,
      conversationId: data.conversationId || null,
    },
    vibrate: [100, 50, 100],
    actions: data.actions || [],
  };

  event.waitUntil(self.registration.showNotification(data.title || 'Value Alert', options));
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  const notifData = event.notification.data || {};
  const action = event.action; // 'react_yes', 'react_maybe', 'react_no', or '' (body click)

  // Map action buttons to emoji reactions
  const reactionMap = {
    react_yes: '✅',
    react_maybe: '❓',
    react_no: '❌',
  };

  if (action && reactionMap[action] && notifData.messageId) {
    // Send reaction via API, then open the app
    event.waitUntil(
      sendReaction(notifData.messageId, reactionMap[action])
        .then(() => focusOrOpen(notifData.url || '/'))
        .catch(() => focusOrOpen(notifData.url || '/'))
    );
  } else {
    // Default click — open/focus the app at the conversation
    event.waitUntil(focusOrOpen(notifData.url || '/'));
  }
});

async function sendReaction(messageId, emoji) {
  // Try to send reaction from service worker — best effort
  try {
    await fetch(`/api/social/reactions/${messageId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ emoji }),
    });
  } catch (e) {
    // SW may not have auth token — reaction will be lost, but app opens as fallback
    console.warn('SW reaction failed (auth needed):', e.message);
  }
}

function focusOrOpen(url) {
  return clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
    // Focus existing tab if open and navigate it
    for (const client of list) {
      if (client.url.includes(self.location.origin) && 'focus' in client) {
        client.focus();
        client.postMessage({ type: 'navigate', url });
        return;
      }
    }
    // Otherwise open new tab
    return clients.openWindow(url);
  });
}

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', () => self.clients.claim());
