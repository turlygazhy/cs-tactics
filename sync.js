/* КС Тактики: shared board over a public MQTT broker, no accounts.
   Gives the page the same small db/room interface it uses inside Claude:
   - db: documents at slash paths, kept as retained messages, so whoever opens the link later gets the board;
   - room: live moments (a stroke being drawn) and presence (who is on the board right now).
   Anyone with the link can read and draw. The room name is in this file, or in ?room=name. */
(() => {
  'use strict';

  const DEFAULT_ROOM = 'team-5948b0260b';
  const BROKERS = [
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker.hivemq.com:8884/mqtt',
    'wss://test.mosquitto.org:8081/mqtt'
  ];

  const qs = new URLSearchParams(location.search);
  const roomName = /^[a-z0-9][a-z0-9-]{2,40}$/i.test(qs.get('room') || '') ? qs.get('room').toLowerCase() : DEFAULT_ROOM;
  const brokers = qs.get('broker') && /^wss?:\/\/[\w.:-]+(\/[\w/-]*)?$/.test(qs.get('broker')) ? [qs.get('broker')] : BROKERS;
  const ROOT = 'cstactics/v1/' + roomName + '/';

  const rnd = (n) => {
    const a = new Uint8Array(n);
    crypto.getRandomValues(a);
    return Array.from(a, (b) => 'abcdefghijklmnopqrstuvwxyz0123456789'[b % 36]).join('');
  };
  const peerId = rnd(12);
  const clone = (v) => JSON.parse(JSON.stringify(v));
  const isObj = (v) => v && typeof v === 'object' && !Array.isArray(v);
  function deepMerge(dst, patch) {
    for (const k of Object.keys(patch)) {
      const v = patch[k];
      if (isObj(v)) { if (!isObj(dst[k])) dst[k] = {}; deepMerge(dst[k], v); } else dst[k] = v;
    }
    return dst;
  }

  let client = null;
  let synced = false;
  const docs = new Map();         // path -> body
  const docSubs = new Map();      // path -> Set(fn)
  const colSubs = new Map();      // collection path -> Set(fn)
  const topicSubs = new Map();    // room topic -> Set(fn)
  const peerSubs = new Set();
  const connSubs = new Set();
  const peerMap = new Map();      // peer -> frozen Peer
  let peerList = Object.freeze([]);
  let myPresence = {};

  /* ---------- documents ---------- */

  const parentOf = (path) => path.split('/').slice(0, -1).join('/');
  const meta = Object.freeze({ fromCache: false, hasPendingWrites: false });
  function docSnap(path) {
    const has = docs.has(path);
    const body = has ? docs.get(path) : undefined;
    return { id: path.split('/').pop(), exists: has, data: () => (has ? clone(body) : undefined), metadata: meta };
  }
  function colSnap(col) {
    const list = [];
    for (const p of docs.keys()) if (parentOf(p) === col) list.push(docSnap(p));
    list.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    return { docs: list, size: list.length, empty: !list.length, metadata: meta, docChanges: () => [] };
  }

  const dirtyDocs = new Set(), dirtyCols = new Set();
  let flushQueued = false;
  function markDirty(path) {
    dirtyDocs.add(path); dirtyCols.add(parentOf(path));
    if (!synced || flushQueued) return;
    flushQueued = true;
    setTimeout(flush, 0);
  }
  function flush() {
    flushQueued = false;
    const ds = [...dirtyDocs], cs = [...dirtyCols];
    dirtyDocs.clear(); dirtyCols.clear();
    for (const p of ds) for (const fn of docSubs.get(p) || []) safe(fn, docSnap(p));
    for (const c of cs) for (const fn of colSubs.get(c) || []) safe(fn, colSnap(c));
  }
  function safe(fn, arg) { try { fn(arg); } catch (e) { setTimeout(() => { throw e; }); } }

  function publish(topic, payload, opts) {
    return new Promise((resolve, reject) => {
      if (!client) { reject({ code: 'unavailable', message: 'no connection' }); return; }
      client.publish(topic, payload, opts, (err) => (err ? reject({ code: 'unavailable', message: String(err) }) : resolve()));
    });
  }
  function write(path, body) {
    if (body === null) docs.delete(path); else docs.set(path, body);
    markDirty(path);
    return publish(ROOT + 'd/' + path, body === null ? '' : JSON.stringify({ d: body }), { qos: 1, retain: true });
  }

  function subscribe(map, key, fn, snap) {
    if (!map.has(key)) map.set(key, new Set());
    map.get(key).add(fn);
    if (synced) setTimeout(() => { if (map.get(key) && map.get(key).has(fn)) safe(fn, snap()); }, 0);
    return () => { const s = map.get(key); if (s) s.delete(fn); };
  }
  function docRef(path) {
    return {
      id: path.split('/').pop(),
      path,
      get: async () => docSnap(path),
      set: async (data) => write(path, clone(data)),
      update: async (patch) => {
        if (!docs.has(path)) throw { code: 'invalid_argument', message: 'document does not exist' };
        return write(path, deepMerge(clone(docs.get(path)), clone(patch)));
      },
      delete: async () => write(path, null),
      acquire: async () => ({ acquired: true }),
      onSnapshot: (next) => subscribe(docSubs, path, next, () => docSnap(path)),
      collection: (sub) => colRef(path + '/' + sub)
    };
  }
  function colRef(col) {
    const q = {
      path: col,
      doc: (id) => docRef(col + '/' + (id || rnd(16))),
      add: async (data) => { const r = docRef(col + '/' + rnd(16)); await r.set(data); return r; },
      get: async () => colSnap(col),
      onSnapshot: (next) => subscribe(colSubs, col, next, () => colSnap(col)),
      where: () => q, orderBy: () => q, limit: () => q
    };
    return q;
  }
  const db = Object.freeze({ doc: docRef, collection: colRef });

  /* ---------- room: moments and presence ---------- */

  function rebuildPeers() {
    peerList = Object.freeze([...peerMap.values()]);
    for (const fn of peerSubs) safe(fn, { peers: peerList, joined: [], left: [], updated: [] });
  }
  let presenceTimer = null;
  function sendPresence() {
    presenceTimer = null;
    if (client) publish(ROOT + 'p/' + peerId, JSON.stringify({ t: Date.now(), s: myPresence }), { qos: 0, retain: true }).catch(() => {});
  }
  const room = Object.freeze({
    emit: async (topic, data) => {
      if (!/^[a-z][a-z0-9_.-]{0,47}$/.test(topic)) throw { code: 'invalid_argument', message: 'bad topic' };
      return publish(ROOT + 'r/' + topic, JSON.stringify({ p: peerId, d: data === undefined ? null : data }), { qos: 0 });
    },
    on: (topic, fn) => {
      if (!topicSubs.has(topic)) topicSubs.set(topic, new Set());
      topicSubs.get(topic).add(fn);
      return () => topicSubs.get(topic).delete(fn);
    },
    presence: async (patch) => {
      for (const k of Object.keys(patch || {})) { if (patch[k] === null) delete myPresence[k]; else myPresence[k] = patch[k]; }
      if (!presenceTimer) presenceTimer = setTimeout(sendPresence, 120);
    },
    peers: () => peerList,
    onPeers: (fn) => { peerSubs.add(fn); setTimeout(() => safe(fn, { peers: peerList, joined: peerList, left: [], updated: [] }), 0); return () => peerSubs.delete(fn); },
    connected: () => !!(client && client.connected),
    onConnection: (fn) => { connSubs.add(fn); setTimeout(() => safe(fn, !!(client && client.connected)), 0); return () => connSubs.delete(fn); },
    join: async () => { throw { code: 'not_permitted', message: 'named rooms are not available here' }; },
    canSendToClaudeSession: async () => 'off'
  });

  /* ---------- images: split into retained parts, fetched only when someone opens them ---------- */

  const PART = 40000;                       // characters of base64 per message, well under public broker limits
  const IMG_ID = /^[a-z0-9]{8,32}$/;
  const imgUrls = new Map();                // id -> Promise<url>
  const imgMeta = new Map();                // id -> parts count
  const loaders = new Map();                // id -> { meta, parts: Map, done(url), fail(err) }

  function b64(blob) {
    return new Promise((resolve, reject) => {
      const r = new FileReader();
      r.onload = () => resolve(String(r.result).split(',')[1] || '');
      r.onerror = () => reject(r.error);
      r.readAsDataURL(blob);
    });
  }
  function tryFinish(id) {
    const L = loaders.get(id);
    if (!L || !L.meta || L.parts.size < L.meta.n) return;
    loaders.delete(id);
    if (client) client.unsubscribe(ROOT + 'i/' + id + '/#');
    try {
      let text = '';
      for (let i = 0; i < L.meta.n; i++) text += L.parts.get(i) || '';
      const bin = atob(text), bytes = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
      L.done(URL.createObjectURL(new Blob([bytes], { type: /^image\/(jpeg|png|webp)$/.test(L.meta.type) ? L.meta.type : 'image/jpeg' })));
    } catch (e) { L.fail({ code: 'invalid_argument', message: 'broken image' }); }
  }
  function onImage(key, text) {
    const slash = key.indexOf('/');
    const id = key.slice(0, slash), part = key.slice(slash + 1);
    const L = loaders.get(id);
    if (!L || !text) return;
    if (part === 'meta') {
      try {
        const m = JSON.parse(text);
        if (Number.isInteger(m.n) && m.n > 0 && m.n <= 200) { L.meta = { n: m.n, type: String(m.type || '') }; imgMeta.set(id, m.n); }
      } catch (e) { return; }
    } else if (/^\d{1,3}$/.test(part)) {
      if (/^[A-Za-z0-9+/=]*$/.test(text)) L.parts.set(+part, text);
    }
    tryFinish(id);
  }
  function loadImage(id) {
    if (!IMG_ID.test(id)) return Promise.reject({ code: 'invalid_argument', message: 'bad id' });
    if (imgUrls.has(id)) return imgUrls.get(id);
    const p = new Promise((resolve, reject) => {
      const L = { meta: null, parts: new Map(), done: resolve, fail: reject };
      loaders.set(id, L);
      setTimeout(() => {
        if (loaders.get(id) !== L) return;
        loaders.delete(id);
        if (client) client.unsubscribe(ROOT + 'i/' + id + '/#');
        reject({ code: 'unavailable', message: 'image not found' });
      }, 20000);
      if (client) client.subscribe(ROOT + 'i/' + id + '/#', { qos: 1 });
    });
    imgUrls.set(id, p);
    p.catch(() => imgUrls.delete(id));      // a later open tries again
    return p;
  }
  const assets = Object.freeze({
    upload: async (blob) => {
      const id = rnd(16);
      const text = await b64(blob);
      const n = Math.max(1, Math.ceil(text.length / PART));
      for (let i = 0; i < n; i++) await publish(ROOT + 'i/' + id + '/' + i, text.slice(i * PART, (i + 1) * PART), { qos: 1, retain: true });
      await publish(ROOT + 'i/' + id + '/meta', JSON.stringify({ n, type: blob.type || 'image/jpeg' }), { qos: 1, retain: true });
      imgMeta.set(id, n);
      const url = URL.createObjectURL(blob);
      imgUrls.set(id, Promise.resolve(url));
      return { id, url, sizeBytes: blob.size, contentType: blob.type || 'image/jpeg' };
    },
    url: loadImage,
    delete: async (id) => {
      if (!IMG_ID.test(id)) return;
      if (!imgMeta.has(id)) { try { await loadImage(id); } catch (e) { return; } }
      const n = imgMeta.get(id) || 0;
      await publish(ROOT + 'i/' + id + '/meta', '', { qos: 1, retain: true });
      for (let i = 0; i < n; i++) await publish(ROOT + 'i/' + id + '/' + i, '', { qos: 1, retain: true });
      imgUrls.delete(id); imgMeta.delete(id);
    },
    list: async () => ({ assets: [], usage: {} })
  });

  /* ---------- wire ---------- */

  function onMessage(topic, payload) {
    if (!topic.startsWith(ROOT)) return;
    const rest = topic.slice(ROOT.length);
    const kind = rest.slice(0, 2), key = rest.slice(2);
    const text = payload && payload.length ? new TextDecoder().decode(payload) : '';
    if (kind === 'i/') { onImage(key, text); return; }
    let obj = null;
    if (text) { try { obj = JSON.parse(text); } catch (e) { return; } }
    if (kind === 'd/') {
      if (!key) return;
      if (obj && isObj(obj.d)) docs.set(key, obj.d); else docs.delete(key);
      markDirty(key);
    } else if (kind === 'r/') {
      if (!obj || typeof obj.p !== 'string') return;
      const mine = obj.p === peerId;
      const msg = Object.freeze({ topic: key, data: obj.d, peer: obj.p, by: null, isMe: mine, sameTab: mine, kind: 'viewer', guest: false });
      for (const fn of topicSubs.get(key) || []) safe(fn, msg);
    } else if (kind === 'p/') {
      if (!key) return;
      if (!obj) { if (peerMap.delete(key)) rebuildPeers(); return; }
      const mine = key === peerId;
      peerMap.set(key, Object.freeze({ peer: key, by: null, isMe: mine, sameTab: mine, kind: 'viewer', guest: false,
        presence: Object.freeze(isObj(obj.s) ? obj.s : {}), updatedAt: Date.now() }));
      rebuildPeers();
    }
  }

  function onConnect() {
    for (const fn of connSubs) safe(fn, true);
    client.subscribe([ROOT + 'd/#', ROOT + 'r/#', ROOT + 'p/#'], { qos: 1 }, () => {
      sendPresence();
      // retained documents arrive right after the subscription; give them a moment, then hand the board over
      setTimeout(() => {
        if (!synced) {
          synced = true;
          for (const p of docs.keys()) markDirty(p);
          for (const p of docSubs.keys()) dirtyDocs.add(p);      // empty documents still answer, so the page stops waiting
          for (const c of colSubs.keys()) dirtyCols.add(c);
          flush();
        }
        else flush();
      }, 900);
    });
  }

  function connect() {
    return new Promise((resolve) => {
      if (typeof mqtt === 'undefined') { resolve(false); return; }
      let i = 0, done = false;
      const next = () => {
        if (done) return;
        if (i >= brokers.length) { done = true; resolve(false); return; }
        const url = brokers[i++];
        const c = mqtt.connect(url, {
          clientId: 'cst_' + peerId, clean: true, keepalive: 30, reconnectPeriod: 2500, connectTimeout: 8000,
          will: { topic: ROOT + 'p/' + peerId, payload: '', qos: 0, retain: true }
        });
        const timer = setTimeout(() => { if (!done) { c.end(true); next(); } }, 9000);
        c.on('error', () => {});
        c.once('connect', () => {
          if (done) { c.end(true); return; }
          done = true; clearTimeout(timer);
          client = c;
          c.on('message', onMessage);
          c.on('connect', onConnect);
          c.on('close', () => { for (const fn of connSubs) safe(fn, false); });
          onConnect();
          resolve(true);
        });
      };
      next();
    });
  }
  const ready = connect();

  window.addEventListener('pagehide', () => {
    if (client && client.connected) client.publish(ROOT + 'p/' + peerId, '', { qos: 0, retain: true });
  });

  window.__cst = { room: roomName, docs };
  window.claude = Object.freeze({
    use: (name) => ready.then((ok) => (!ok ? null : name === 'db' ? db : name === 'room' ? room : name === 'assets' ? assets : null))
  });
})();
