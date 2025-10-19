
self.addEventListener('install', (e)=>{
  e.waitUntil(caches.open('barc-v2').then(c=>c.addAll(['/','/manifest.json'])));
});
self.addEventListener('fetch', (e)=>{
  e.respondWith(caches.match(e.request).then(r=> r || fetch(e.request)));
});
