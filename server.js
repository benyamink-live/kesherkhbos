'use strict';
/*
 * קשר – שרת אפליקציית Push-To-Talk (בסגנון Zello)
 * - הצגת קבצי הלקוח (public/)
 * - WebSocket: שמע, נעילת ערוץ (דובר אחד בכל פעם), מיקום, הרשאות מנהל
 * - Web Push: התראות מהמנהל גם כשהאפליקציה סגורה
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const webpush = require('web-push');

const PORT = +process.env.PORT || 3000;
const ADMIN_USER = (process.env.ADMIN_USER || 'admin').trim();
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'admin1234';
const ALLOW_REGISTER = process.env.ALLOW_REGISTER === '1'; // ברירת מחדל: רק המנהל יוצר משתמשים
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const MAX_TALK_MS = (+process.env.MAX_TALK_SEC || 60) * 1000;
const PUSH_CONTACT = process.env.PUSH_CONTACT || 'mailto:admin@example.com';
const PUBLIC_DIR = path.join(__dirname, 'public');
const MAX_RECS = 10;                                   // הקלטות אחרונות לכל ערוץ
const MAX_REC_BYTES = Math.ceil(MAX_TALK_MS / 1000 + 2) * 32000;

if (!process.env.ADMIN_PASSWORD) {
  console.warn('⚠️  ADMIN_PASSWORD לא הוגדר – משתמשים בסיסמת ברירת מחדל "admin1234". מומלץ לשנות!');
}

// ---------------------------------------------------------------- נתונים
function newId(p) { return p + crypto.randomBytes(5).toString('hex'); }

// אחסון: אם הוגדר DATABASE_URL (Postgres, למשל Neon בחינם) – הנתונים נשמרים שם; אחרת בקובץ
const DATABASE_URL = process.env.DATABASE_URL || '';
let pgPool = null;
const recs = new Map();   // channelId -> [{ id, channel, name, start, end, lat, lng, acc, audio: Buffer }] (החדש ראשון)
let db = { users: {}, channels: {}, tokens: {}, vapid: null, adminLoc: null, adminPush: [], alerts: [] };

async function loadDb() {
  let loaded = null;
  if (DATABASE_URL) {
    const { Pool } = require('pg');
    pgPool = new Pool({ connectionString: DATABASE_URL, ssl: /localhost|127\.0\.0\.1/.test(DATABASE_URL) ? false : { rejectUnauthorized: false }, max: 2 });
    await pgPool.query('CREATE TABLE IF NOT EXISTS kesher_store (k TEXT PRIMARY KEY, v JSONB NOT NULL, updated TIMESTAMPTZ DEFAULT now())');
    const r = await pgPool.query("SELECT v FROM kesher_store WHERE k = 'db'");
    if (r.rows[0]) loaded = r.rows[0].v;
    await pgPool.query('CREATE TABLE IF NOT EXISTS kesher_rec (id TEXT PRIMARY KEY, channel TEXT NOT NULL, meta JSONB NOT NULL, audio BYTEA NOT NULL, ts BIGINT NOT NULL)');
    const rr = await pgPool.query('SELECT id, channel, meta, audio FROM kesher_rec ORDER BY ts DESC');
    for (const row of rr.rows) {
      const list = recs.get(row.channel) || [];
      if (list.length < MAX_RECS) list.push(Object.assign({}, row.meta, { id: row.id, channel: row.channel, audio: row.audio }));
      recs.set(row.channel, list);
    }
    console.log('💾 אחסון: Postgres');
  } else {
    try { loaded = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch { /* קובץ חדש */ }
    console.log('💾 אחסון: קובץ ' + DATA_FILE);
  }
  if (loaded) db = Object.assign(db, loaded);
  if (!Object.keys(db.channels).length) db.channels[newId('c')] = { name: 'ערוץ ראשי', created: Date.now() };
  if (!db.vapid) db.vapid = webpush.generateVAPIDKeys();
  if (!Array.isArray(db.adminPush)) db.adminPush = [];
  if (!Array.isArray(db.alerts)) db.alerts = [];
  webpush.setVapidDetails(PUSH_CONTACT, db.vapid.publicKey, db.vapid.privateKey);
  saveNow();
}

let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(saveNow, 300);
}
let pgWriting = false, pgAgain = false;
function saveNow() {
  if (pgPool) {
    if (pgWriting) { pgAgain = true; return Promise.resolve(); }
    pgWriting = true;
    return pgPool.query("INSERT INTO kesher_store (k, v, updated) VALUES ('db', $1, now()) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v, updated = now()", [JSON.stringify(db)])
      .catch((e) => console.error('db save failed', e.message))
      .finally(() => { pgWriting = false; if (pgAgain) { pgAgain = false; saveNow(); } });
  }
  try {
    fs.writeFileSync(DATA_FILE + '.tmp', JSON.stringify(db, null, 1));
    fs.renameSync(DATA_FILE + '.tmp', DATA_FILE);
  } catch (e) { console.error('save failed', e); }
  return Promise.resolve();
}
let locSavePending = false;
function saveLocSoon() {
  if (locSavePending) return;
  locSavePending = true;
  setTimeout(() => { locSavePending = false; save(); }, 10000);
}

function hashPw(pw, salt) { return crypto.scryptSync(pw, salt, 32).toString('hex'); }
function safeEq(a, b) {
  const A = Buffer.from(String(a)), B = Buffer.from(String(b));
  return A.length === B.length && crypto.timingSafeEqual(A, B);
}

// ---------------------------------------------------------------- HTTP
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml', '.png': 'image/png', '.webmanifest': 'application/manifest+json',
};
const server = http.createServer((req, res) => {
  let p;
  try { p = decodeURIComponent((req.url || '/').split('?')[0]); } catch { res.writeHead(400); return res.end(); }
  if (p === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (p === '/status') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ ok: true, storage: pgPool ? 'postgres' : 'file', online: [...sessions].filter(x => x.name).length, lastTx }));
  }
  const rm = p.match(/^\/rec\/([a-z0-9]+)$/);
  if (rm) return serveRec(req, res, rm[1]);
  if (p === '/') p = '/index.html';
  const f = path.normalize(path.join(PUBLIC_DIR, p));
  if (!f.startsWith(PUBLIC_DIR + path.sep)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404); return res.end('Not found'); }
    const headers = { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-cache' };
    if (p === '/sw.js') headers['Service-Worker-Allowed'] = '/';
    res.writeHead(200, headers);
    res.end(d);
  });
});

// ---------------------------------------------------------------- WebSocket
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });
const sessions = new Set();
const floors = new Map(); // channelId -> { s, entry, timer, chunks, bytes, peak, sent }
let lastTx = null;        // אבחון: נתוני השידור האחרון
const log = [];           // יומן שידורים (בזיכרון)

function send(s, obj) { if (s.ws.readyState === 1) s.ws.send(JSON.stringify(obj)); }
function toAdmins(obj) { for (const s of sessions) if (s.role === 'admin') send(s, obj); }
function toast(s, msg) { send(s, { t: 'toast', msg }); }

function allowedChannels(s) {
  if (s.role === 'admin') return Object.keys(db.channels);
  const u = db.users[s.name];
  return u ? u.channels.filter(c => db.channels[c]) : [];
}

wss.on('connection', (ws) => {
  const s = { ws, name: null, role: null, active: null, loc: null, speaking: null, alive: true, tries: 0, token: null };
  sessions.add(s);
  ws.on('pong', () => { s.alive = true; });
  ws.on('message', (data, isBinary) => {
    if (isBinary) return onAudio(s, data);
    let m;
    try { m = JSON.parse(data.toString()); } catch { return; }
    try { onMsg(s, m); } catch (e) { console.error(e); }
  });
  ws.on('close', () => {
    sessions.delete(s);
    if (s.speaking) releaseFloor(s.speaking);
    if (s.name) broadcastAll();
  });
  send(s, { t: 'hello', vapidPublicKey: db.vapid.publicKey, allowRegister: ALLOW_REGISTER });
});

setInterval(() => {
  for (const s of sessions) {
    if (!s.alive) { s.ws.terminate(); continue; }
    s.alive = false;
    try { s.ws.ping(); } catch { /* ignore */ }
  }
}, 25000);

function onAudio(s, data) {
  if (!s.speaking) return;
  const f = floors.get(s.speaking);
  if (!f || f.s !== s) return;
  if (f.bytes + data.length <= MAX_REC_BYTES) { f.chunks.push(Buffer.from(data)); f.bytes += data.length; }
  for (let i = 0; i + 1 < data.length; i += 8) { const v = Math.abs(data.readInt16LE(i)); if (v > f.peak) f.peak = v; }
  let n = 0;
  for (const o of sessions) {
    if (o !== s && o.name && o.active === s.speaking && o.ws.readyState === 1 && o.ws.bufferedAmount < 512 * 1024) {
      o.ws.send(data, { binary: true }); n++;
    }
  }
  f.sent += n; f.frames++; if (n > f.maxListeners) f.maxListeners = n;
}

// ---------------------------------------------------------------- הקלטות
function recMeta(r) {
  return { id: r.id, channel: r.channel, name: r.name, start: r.start, end: r.end, dur: Math.round(r.audio.length / 32) / 1000, lat: r.lat, lng: r.lng, acc: r.acc };
}
function saveRec(f) {
  const e = f.entry;
  const r = { id: e.id, channel: e.channel, name: e.name, start: e.start, end: e.end, lat: e.lat, lng: e.lng, acc: e.acc, audio: Buffer.concat(f.chunks) };
  const list = recs.get(e.channel) || [];
  list.unshift(r);
  const removed = list.splice(MAX_RECS);
  recs.set(e.channel, list);
  e.hasRec = true;
  if (pgPool) {
    const { audio, ...meta } = r;
    pgPool.query('INSERT INTO kesher_rec (id, channel, meta, audio, ts) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (id) DO NOTHING', [r.id, r.channel, JSON.stringify(meta), audio, r.start])
      .then(() => removed.length && pgPool.query('DELETE FROM kesher_rec WHERE id = ANY($1)', [removed.map(x => x.id)]))
      .catch(err => console.error('rec save failed', err.message));
  }
  for (const o of sessions) if (o.name && (o.active === e.channel || o.role === 'admin')) send(o, { t: 'recs_changed', channel: e.channel });
}
function findRec(id) {
  for (const list of recs.values()) { const r = list.find(x => x.id === id); if (r) return r; }
  return null;
}
function wavHeader(len) {
  const h = Buffer.alloc(44);
  h.write('RIFF', 0); h.writeUInt32LE(36 + len, 4); h.write('WAVE', 8);
  h.write('fmt ', 12); h.writeUInt32LE(16, 16); h.writeUInt16LE(1, 20); h.writeUInt16LE(1, 22);
  h.writeUInt32LE(16000, 24); h.writeUInt32LE(32000, 28); h.writeUInt16LE(2, 32); h.writeUInt16LE(16, 34);
  h.write('data', 36); h.writeUInt32LE(len, 40);
  return h;
}
function serveRec(req, res, id) {
  const auth = String(req.headers.authorization || '');
  const tk = db.tokens[auth.replace(/^Bearer\s+/i, '')];
  const r = findRec(id);
  const ok = tk && r && (tk.role === 'admin' || (db.users[tk.name] && !db.users[tk.name].blocked && db.users[tk.name].channels.includes(r.channel)));
  if (!tk) { res.writeHead(401); return res.end(); }
  if (!ok) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'Content-Type': 'audio/wav', 'Content-Length': 44 + r.audio.length, 'Cache-Control': 'private, max-age=3600' });
  res.end(Buffer.concat([wavHeader(r.audio.length), r.audio]));
}

function releaseFloor(ch) {
  const f = floors.get(ch);
  if (!f) return;
  clearTimeout(f.timer);
  floors.delete(ch);
  f.s.speaking = null;
  f.entry.end = Date.now();
  lastTx = { at: f.entry.end, ms: f.entry.end - f.entry.start, frames: f.frames, bytes: f.bytes, peak: f.peak, listeners: f.maxListeners, forwarded: f.sent };
  if (f.bytes >= 3200) saveRec(f); // לפחות 0.1 שניות
  for (const o of sessions) if (o !== f.s && o.name && o.active === ch) send(o, { t: 'speaking_end', channel: ch, name: f.s.name });
  toAdmins({ t: 'speak_end', entry: f.entry });
}

function revokeTokens(name) {
  for (const [k, v] of Object.entries(db.tokens)) if (v.name === name && v.role === 'user') delete db.tokens[k];
}
function kickUser(name, msg) {
  for (const o of sessions) if (o.name === name) { send(o, { t: 'kicked', msg, clearToken: true }); setTimeout(() => o.ws.close(), 100); }
}

function onMsg(s, m) {
  switch (m.t) {
    case 'auth': return onAuth(s, m);
    case 'logout': {
      if (s.token) { delete db.tokens[s.token]; save(); }
      if (s.name && s.role === 'user' && db.users[s.name] && m.endpoint) {
        db.users[s.name].push = (db.users[s.name].push || []).filter(p => p.endpoint !== m.endpoint);
      }
      return s.ws.close();
    }
  }
  if (!s.name) return;

  switch (m.t) {
    case 'join': {
      const ch = m.channel || null;
      if (ch && !allowedChannels(s).includes(ch)) return;
      if (s.speaking) releaseFloor(s.speaking);
      s.active = ch;
      if (s.role === 'user' && db.users[s.name]) { db.users[s.name].lastActive = ch; save(); }
      return broadcastAll();
    }
    case 'loc': {
      const lat = +m.lat, lng = +m.lng, acc = Math.round(+m.acc || 0);
      if (!isFinite(lat) || !isFinite(lng) || Math.abs(lat) > 90 || Math.abs(lng) > 180) return;
      s.loc = { lat, lng, acc, ts: Date.now() };
      if (s.role === 'user' && db.users[s.name]) db.users[s.name].lastLoc = s.loc; else db.adminLoc = s.loc;
      saveLocSoon();
      if (s.speaking) {
        const f = floors.get(s.speaking);
        if (f) Object.assign(f.entry, { lat, lng, acc });
      }
      return toAdmins({ t: 'loc', name: s.name, loc: s.loc, speaking: s.speaking });
    }
    case 'ptt_start': {
      const ch = s.active;
      if (!ch || !db.channels[ch] || !allowedChannels(s).includes(ch)) return send(s, { t: 'floor', ok: false, msg: 'אין ערוץ פעיל' });
      const f = floors.get(ch);
      if (f && f.s !== s) return send(s, { t: 'floor', ok: false, msg: `${f.s.name} מדבר כרגע` });
      if (f) return send(s, { t: 'floor', ok: true });
      const entry = {
        id: newId('l'), name: s.name, channel: ch, channelName: db.channels[ch].name,
        start: Date.now(), end: null,
        lat: s.loc ? s.loc.lat : null, lng: s.loc ? s.loc.lng : null, acc: s.loc ? s.loc.acc : null,
        locTs: s.loc ? s.loc.ts : null,
      };
      log.unshift(entry);
      if (log.length > 300) log.pop();
      floors.set(ch, {
        s, entry, chunks: [], bytes: 0, peak: 0, sent: 0, frames: 0, maxListeners: 0,
        timer: setTimeout(() => { send(s, { t: 'floor_timeout', msg: 'הגעת למגבלת זמן השידור' }); releaseFloor(ch); }, MAX_TALK_MS),
      });
      s.speaking = ch;
      send(s, { t: 'floor', ok: true });
      for (const o of sessions) if (o !== s && o.name && o.active === ch) send(o, { t: 'speaking', channel: ch, name: s.name });
      return toAdmins({ t: 'speak_start', entry });
    }
    case 'ptt_stop':
      if (s.speaking) releaseFloor(s.speaking);
      return;
    case 'push_sub': {
      const sub = m.sub;
      if (!sub || typeof sub.endpoint !== 'string' || !sub.keys) return;
      const list = s.role === 'admin' ? db.adminPush : (db.users[s.name].push = db.users[s.name].push || []);
      if (!list.some(p => p.endpoint === sub.endpoint)) list.push({ endpoint: sub.endpoint, keys: sub.keys });
      if (list.length > 5) list.splice(0, list.length - 5);
      save();
      return broadcastAll();
    }
    case 'recs': {
      const ch = m.channel;
      if (!ch || !allowedChannels(s).includes(ch)) return;
      return send(s, { t: 'recs', channel: ch, list: (recs.get(ch) || []).map(recMeta) });
    }
    case 'alert_ack': {
      const a = db.alerts.find(x => x.id === m.id);
      if (a && !a.acks[s.name]) { a.acks[s.name] = Date.now(); save(); broadcastAll(); }
      return;
    }
    case 'admin':
      if (s.role !== 'admin') return toast(s, 'אין לך הרשאת מנהל');
      return adminOp(s, m);
  }
}

function onAuth(s, m) {
  if (s.name) return;
  let name, role;
  if (m.token) {
    const tk = db.tokens[m.token];
    if (!tk) return send(s, { t: 'auth_err', msg: 'החיבור פג – התחבר מחדש', clearToken: true });
    ({ name, role } = tk);
    if (role === 'user' && (!db.users[name] || db.users[name].blocked)) {
      delete db.tokens[m.token]; save();
      return send(s, { t: 'auth_err', msg: db.users[name] ? 'המשתמש חסום על ידי המנהל' : 'המשתמש נמחק', clearToken: true });
    }
    if (role === 'admin' && name !== ADMIN_USER) return send(s, { t: 'auth_err', msg: 'החיבור פג – התחבר מחדש', clearToken: true });
    s.token = m.token;
  } else {
    if (++s.tries > 5) { send(s, { t: 'auth_err', msg: 'יותר מדי ניסיונות' }); return s.ws.close(); }
    name = String(m.name || '').trim().replace(/\s+/g, ' ');
    const pw = String(m.password || '');
    const err = (msg) => send(s, { t: 'auth_err', msg });
    if (name.toLowerCase() === ADMIN_USER.toLowerCase()) {
      if (!safeEq(pw, ADMIN_PASSWORD)) return err('סיסמת מנהל שגויה');
      name = ADMIN_USER; role = 'admin';
    } else {
      if (name.length < 2 || name.length > 24) return err('השם חייב להיות 2–24 תווים');
      if (pw.length < 4) return err('הסיסמה חייבת להיות לפחות 4 תווים');
      const u = db.users[name];
      if (u) {
        if (!safeEq(hashPw(pw, u.salt), u.hash)) return err('שם משתמש או סיסמה שגויים');
        if (u.blocked) return err('המשתמש חסום על ידי המנהל');
      } else {
        if (!ALLOW_REGISTER) return err('שם משתמש או סיסמה שגויים');
        const salt = crypto.randomBytes(8).toString('hex');
        db.users[name] = { salt, hash: hashPw(pw, salt), channels: [], blocked: false, created: Date.now(), lastLoc: null, push: [] };
      }
      role = 'user';
    }
    s.token = crypto.randomBytes(24).toString('hex');
    db.tokens[s.token] = { name, role, created: Date.now() };
    save();
  }
  for (const o of sessions) {
    if (o !== s && o.name === name) { send(o, { t: 'kicked', msg: 'התחברת ממכשיר אחר', clearToken: false }); o.ws.close(); o.name = null; }
  }
  s.name = name; s.role = role;
  const chans = allowedChannels(s);
  const prev = role === 'user' ? db.users[name].lastActive : db.adminActive;
  s.active = chans.includes(prev) ? prev : (chans[0] || null);
  send(s, { t: 'auth_ok', name, role, token: s.token, maxTalk: MAX_TALK_MS / 1000 });
  // התראות שעדיין לא אושרו
  for (const a of db.alerts.slice(0, 20).reverse()) {
    if (alertTargets(a).includes(name) && !a.acks[name]) send(s, { t: 'alert', alert: publicAlert(a) });
  }
  broadcastAll();
}

// ---------------------------------------------------------------- התראות (Push)
function alertTargets(a) {
  const names = Object.keys(db.users);
  if (a.target.type === 'all') return names;
  if (a.target.type === 'channel') return names.filter(n => db.users[n].channels.includes(a.target.id));
  if (a.target.type === 'user') return db.users[a.target.id] ? [a.target.id] : [];
  return [];
}
function publicAlert(a) {
  return { id: a.id, title: a.title, body: a.body, urgent: a.urgent, ts: a.ts, from: a.from, targetLabel: a.targetLabel };
}

async function sendAlert(s, m) {
  const title = String(m.title || '').trim().slice(0, 80) || 'הודעה מהמנהל';
  const body = String(m.body || '').trim().slice(0, 500);
  if (!body) return toast(s, 'כתוב תוכן להתראה');
  const tt = m.target && m.target.type;
  let target, targetLabel;
  if (tt === 'channel' && db.channels[m.target.id]) { target = { type: 'channel', id: m.target.id }; targetLabel = 'ערוץ ' + db.channels[m.target.id].name; }
  else if (tt === 'user' && db.users[m.target.id]) { target = { type: 'user', id: m.target.id }; targetLabel = m.target.id; }
  else { target = { type: 'all' }; targetLabel = 'כולם'; }

  const a = { id: newId('a'), title, body, urgent: !!m.urgent, ts: Date.now(), from: s.name, target, targetLabel, acks: {}, stats: { ws: 0, push: 0, pushFail: 0 } };
  db.alerts.unshift(a);
  if (db.alerts.length > 100) db.alerts.length = 100;

  const targets = alertTargets(a);
  const online = new Set();
  for (const o of sessions) {
    if (o.name && o.role === 'user' && targets.includes(o.name)) { send(o, { t: 'alert', alert: publicAlert(a) }); online.add(o.name); }
  }
  a.stats.ws = online.size;

  const payload = JSON.stringify({ title, body, id: a.id, urgent: a.urgent, from: s.name, ts: a.ts });
  const jobs = [];
  for (const name of targets) {
    const u = db.users[name];
    for (const sub of (u.push || [])) {
      jobs.push(webpush.sendNotification(sub, payload, { TTL: 3600, urgency: a.urgent ? 'high' : 'normal' })
        .then(() => { a.stats.push++; })
        .catch(err => {
          a.stats.pushFail++;
          if (err.statusCode === 404 || err.statusCode === 410) u.push = u.push.filter(p => p.endpoint !== sub.endpoint);
        }));
    }
  }
  toast(s, `ההתראה נשלחה ל־${targets.length} משתמשים (${online.size} מחוברים כעת)`);
  broadcastAll();
  await Promise.allSettled(jobs);
  save();
  broadcastAll();
}

// ---------------------------------------------------------------- פעולות מנהל
function adminOp(s, m) {
  const u = m.user ? db.users[m.user] : null;
  switch (m.op) {
    case 'create_user': {
      const name = String(m.name || '').trim().replace(/\s+/g, ' ');
      const pw = String(m.password || '');
      if (name.length < 2 || name.length > 24) return toast(s, 'השם חייב להיות 2–24 תווים');
      if (name.toLowerCase() === ADMIN_USER.toLowerCase()) return toast(s, 'השם הזה שמור למנהל');
      if (db.users[name]) return toast(s, 'כבר קיים משתמש בשם הזה');
      if (pw.length < 4) return toast(s, 'הסיסמה חייבת להיות לפחות 4 תווים');
      const salt = crypto.randomBytes(8).toString('hex');
      const chans = Array.isArray(m.channels) ? m.channels.filter(c => db.channels[c]) : [];
      db.users[name] = { salt, hash: hashPw(pw, salt), channels: chans, blocked: false, created: Date.now(), lastLoc: null, push: [] };
      toast(s, `המשתמש "${name}" נוצר`);
      break;
    }
    case 'set_password': {
      if (!u) return;
      const pw = String(m.password || '');
      if (pw.length < 4) return toast(s, 'הסיסמה חייבת להיות לפחות 4 תווים');
      u.salt = crypto.randomBytes(8).toString('hex');
      u.hash = hashPw(pw, u.salt);
      revokeTokens(m.user);
      kickUser(m.user, 'הסיסמה שלך שונתה על ידי המנהל – התחבר מחדש');
      toast(s, `הסיסמה של "${m.user}" עודכנה`);
      break;
    }
    case 'set_members': { // קביעת חברי קבוצה בבת אחת
      if (!db.channels[m.channel] || !Array.isArray(m.users)) return;
      const want = new Set(m.users);
      for (const [name, x] of Object.entries(db.users)) {
        const has = x.channels.includes(m.channel);
        if (want.has(name) && !has) x.channels.push(m.channel);
        if (!want.has(name) && has) x.channels = x.channels.filter(c => c !== m.channel);
      }
      for (const o of sessions) {
        if (o.role !== 'user') continue;
        const allowed = allowedChannels(o);
        if (o.active && !allowed.includes(o.active)) { if (o.speaking) releaseFloor(o.speaking); o.active = allowed[0] || null; }
        if (!o.active && allowed.length) o.active = allowed[0];
      }
      break;
    }
    case 'create_channel': {
      const name = String(m.name || '').trim().slice(0, 30);
      if (!name) return toast(s, 'הכנס שם ערוץ');
      if (Object.values(db.channels).some(c => c.name === name)) return toast(s, 'כבר קיים ערוץ בשם הזה');
      db.channels[newId('c')] = { name, created: Date.now() };
      break;
    }
    case 'rename_channel': {
      const name = String(m.name || '').trim().slice(0, 30);
      if (!db.channels[m.id] || !name) return;
      db.channels[m.id].name = name;
      break;
    }
    case 'delete_channel': {
      if (!db.channels[m.id]) return;
      releaseFloor(m.id);
      delete db.channels[m.id];
      recs.delete(m.id);
      if (pgPool) pgPool.query('DELETE FROM kesher_rec WHERE channel = $1', [m.id]).catch(() => {});
      for (const x of Object.values(db.users)) x.channels = x.channels.filter(c => c !== m.id);
      for (const o of sessions) if (o.active === m.id) o.active = allowedChannels(o)[0] || null;
      break;
    }
    case 'assign': {
      if (!u || !db.channels[m.channel]) return;
      const set = new Set(u.channels);
      if (m.on) set.add(m.channel); else set.delete(m.channel);
      u.channels = [...set];
      for (const o of sessions) {
        if (o.name !== m.user) continue;
        if (!m.on && o.active === m.channel) {
          if (o.speaking === m.channel) releaseFloor(m.channel);
          o.active = allowedChannels(o)[0] || null;
        }
        if (m.on && !o.active) o.active = m.channel;
      }
      break;
    }
    case 'move': {
      if (!u || !db.channels[m.channel]) return;
      if (!u.channels.includes(m.channel)) u.channels.push(m.channel);
      u.lastActive = m.channel;
      for (const o of sessions) {
        if (o.name !== m.user) continue;
        if (o.speaking && o.speaking !== m.channel) releaseFloor(o.speaking);
        o.active = m.channel;
        toast(o, `המנהל חיבר אותך לערוץ "${db.channels[m.channel].name}"`);
      }
      break;
    }
    case 'block': {
      if (!u) return;
      u.blocked = !!m.on;
      if (u.blocked) { revokeTokens(m.user); kickUser(m.user, 'המשתמש נחסם על ידי המנהל'); }
      break;
    }
    case 'kick': {
      if (!u) return;
      revokeTokens(m.user);
      kickUser(m.user, 'נותקת על ידי המנהל');
      break;
    }
    case 'delete_user': {
      if (!u) return;
      revokeTokens(m.user);
      kickUser(m.user, 'המשתמש נמחק על ידי המנהל');
      delete db.users[m.user];
      break;
    }
    case 'admin_channel': // המנהל בוחר ערוץ האזנה
      return;
    case 'send_alert':
      sendAlert(s, m);
      return;
    case 'delete_alert':
      db.alerts = db.alerts.filter(a => a.id !== m.id);
      break;
    case 'clear_log':
      log.length = 0;
      break;
    default:
      return;
  }
  save();
  broadcastAll();
}

// ---------------------------------------------------------------- עדכוני מצב
function sendMe(s) {
  const allowed = allowedChannels(s);
  if (s.active && !allowed.includes(s.active)) s.active = null;
  if (s.role === 'admin') db.adminActive = s.active;
  const channels = allowed.map(id => ({ id, name: db.channels[id].name, speaking: floors.has(id) ? floors.get(id).s.name : null }));
  const members = s.active ? [...sessions].filter(o => o.name && o.active === s.active).map(o => ({ name: o.name, role: o.role })) : [];
  const hasPush = s.role === 'admin' ? db.adminPush.length > 0 : !!(db.users[s.name] && (db.users[s.name].push || []).length);
  send(s, { t: 'me', channels, active: s.active, members, speaking: s.active && floors.has(s.active) ? floors.get(s.active).s.name : null, hasPush });
}

function buildState() {
  const online = new Map();
  for (const o of sessions) if (o.name) online.set(o.name, o);
  const users = [];
  const ad = online.get(ADMIN_USER);
  users.push({ name: ADMIN_USER, role: 'admin', online: !!ad, active: ad ? ad.active : null, channels: Object.keys(db.channels), blocked: false, loc: (ad && ad.loc) || db.adminLoc || null, speaking: ad ? ad.speaking : null, push: db.adminPush.length });
  for (const [name, u] of Object.entries(db.users)) {
    const o = online.get(name);
    users.push({ name, role: 'user', online: !!o, active: o ? o.active : null, channels: u.channels.filter(c => db.channels[c]), blocked: !!u.blocked, loc: (o && o.loc) || u.lastLoc || null, speaking: o ? o.speaking : null, created: u.created, push: (u.push || []).length });
  }
  users.sort((a, b) => (a.role === 'admin' ? -1 : b.role === 'admin' ? 1 : 0) || (b.online - a.online) || a.name.localeCompare(b.name, 'he'));
  const channels = Object.entries(db.channels).map(([id, c]) => ({
    id, name: c.name,
    speaking: floors.has(id) ? floors.get(id).s.name : null,
    online: [...online.values()].filter(o => o.active === id).length,
    assigned: Object.values(db.users).filter(x => x.channels.includes(id)).length,
  }));
  const alerts = db.alerts.slice(0, 50).map(a => ({ ...publicAlert(a), total: alertTargets(a).length, acks: Object.keys(a.acks).length, ackNames: Object.keys(a.acks), stats: a.stats }));
  return { t: 'state', users, channels, log: log.slice(0, 150), alerts };
}

function broadcastAll() {
  let state = null;
  for (const s of sessions) {
    if (!s.name) continue;
    sendMe(s);
    if (s.role === 'admin') send(s, state || (state = buildState()));
  }
}

const shutdown = () => { clearTimeout(saveTimer); Promise.resolve(saveNow()).finally(() => process.exit(0)); setTimeout(() => process.exit(0), 3000); };
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

loadDb().then(() => {
  server.listen(PORT, () => {
    console.log(`📻 שרת הקשר פועל: http://localhost:${PORT}`);
    console.log(`   מנהל: "${ADMIN_USER}"`);
  });
}).catch((e) => { console.error('טעינת הנתונים נכשלה:', e.message); process.exit(1); });
