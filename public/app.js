'use strict';
/* קשר – לקוח */
const $ = (s) => document.querySelector(s);
const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

const S = {
  ws: null, token: localStorage.getItem('ptt_token'), me: null, role: null, authed: false,
  channels: [], active: null, members: [], speaking: null, hasPush: false,
  wantTalk: false, talking: false, talkStart: 0, maxTalk: 60,
  loc: null, lastLocSent: 0, geoWatch: null,
  state: null, live: new Map(), vapid: null,
  pendingAuth: null, stopReconnect: false, reconnectDelay: 1000,
  alertQueue: [], currentAlert: null,
};

// ============================================================ שמע
const RATE = 16000;
const A = { ctx: null, out: null, stream: null, src: null, node: null, sink: null, workletOk: null, rsBuf: new Float32Array(0), rsPos: 0, pend: [], nextTime: 0, micReady: false, micPromise: null, micIdle: null, ctxIdle: null, alarm: null };
const IS_IOS = /iPad|iPhone|iPod/.test(navigator.userAgent) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
const MIC_IDLE_MS = 20000;   // סגירת המיקרופון אחרי 20 שניות בלי שידור
const CTX_IDLE_MS = 30000;   // השהיית מנוע השמע אחרי 30 שניות שקט

function setAudioSession(type) {
  try { if (navigator.audioSession) navigator.audioSession.type = type; } catch { /* ignore */ }
}
async function audioInit() {
  if (!A.ctx) {
    setAudioSession('playback');
    A.ctx = new (window.AudioContext || window.webkitAudioContext)();
    A.out = A.ctx.createGain();
    A.out.gain.value = 1.4;
    A.out.connect(A.ctx.destination);
  }
  if (A.ctx.state !== 'running') { try { await A.ctx.resume(); } catch { /* ignore */ } }
  return A.ctx.state === 'running';
}

function micInit() {
  clearTimeout(A.micIdle);
  if (A.micReady) return Promise.resolve(true);
  if (!A.micPromise) A.micPromise = openMic().finally(() => { A.micPromise = null; });
  return A.micPromise;
}
async function openMic() {
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) { toast('הדפדפן לא תומך במיקרופון (נדרש HTTPS)'); return false; }
  setAudioSession('play-and-record');
  try {
    A.stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true, channelCount: 1 } });
  } catch (e) { toast('אין גישה למיקרופון – אשר הרשאה בדפדפן'); return false; }
  await audioInit();
  A.src = A.ctx.createMediaStreamSource(A.stream);
  if (!A.sink) { A.sink = A.ctx.createGain(); A.sink.gain.value = 0; A.sink.connect(A.ctx.destination); }
  if (A.workletOk == null) {
    A.workletOk = false;
    if (A.ctx.audioWorklet && window.AudioWorkletNode) {
      try {
        const code = "class C extends AudioWorkletProcessor{process(i){const c=i[0]&&i[0][0];if(c)this.port.postMessage(c.slice(0));return true}}registerProcessor('cap',C)";
        await A.ctx.audioWorklet.addModule(URL.createObjectURL(new Blob([code], { type: 'application/javascript' })));
        A.workletOk = true;
      } catch { A.workletOk = false; }
    }
  }
  if (A.workletOk) {
    A.node = new AudioWorkletNode(A.ctx, 'cap');
    A.node.port.onmessage = (e) => onMic(e.data);
  } else {
    A.node = A.ctx.createScriptProcessor(2048, 1, 1);
    A.node.onaudioprocess = (e) => onMic(e.inputBuffer.getChannelData(0).slice(0));
  }
  A.src.connect(A.node); A.node.connect(A.sink);
  A.micReady = true;
  return true;
}
function micScheduleRelease() { clearTimeout(A.micIdle); A.micIdle = setTimeout(micRelease, MIC_IDLE_MS); }
function micRelease() {
  if (S.talking || S.wantTalk || !A.micReady) return;
  try { A.src.disconnect(); A.node.disconnect(); if (A.node.port) { A.node.port.onmessage = null; A.node.port.close(); } } catch { /* ignore */ }
  if (A.stream) A.stream.getTracks().forEach((t) => t.stop());
  A.stream = A.src = A.node = null; A.micReady = false;
  setAudioSession('playback');
  ctxScheduleIdle();
}
// השהיית מנוע השמע כשאין פעילות (חוסך סוללה). באייפון לא משהים – שם אי אפשר להפעיל מחדש בלי נגיעה
// מנוע השמע נשאר פעיל כדי שקול נכנס יישמע תמיד (המיקרופון עדיין נסגר כשלא מדברים)
function ctxScheduleIdle() { clearTimeout(A.ctxIdle); }
function ctxWake() {
  clearTimeout(A.ctxIdle);
  if (A.ctx && A.ctx.state === 'suspended') A.ctx.resume().catch(() => {});
}

function onMic(input) {
  if (!S.talking) { A.rsBuf = new Float32Array(0); A.rsPos = 0; A.pend = []; return; }
  let pk = 0; for (let i = 0; i < input.length; i += 4) { const v = Math.abs(input[i]); if (v > pk) pk = v; }
  S.txLevel = Math.max(pk, (S.txLevel || 0) * 0.85); if (pk > (S.txMax || 0)) S.txMax = pk;
  const ratio = A.ctx.sampleRate / RATE;
  const buf = new Float32Array(A.rsBuf.length + input.length);
  buf.set(A.rsBuf); buf.set(input, A.rsBuf.length);
  let pos = A.rsPos;
  while (pos + ratio <= buf.length) {
    const s = Math.floor(pos), e = Math.max(s + 1, Math.floor(pos + ratio));
    let sum = 0;
    for (let j = s; j < e; j++) sum += buf[j];
    let v = sum / (e - s);
    v = v < -1 ? -1 : v > 1 ? 1 : v;
    A.pend.push((v * 32767) | 0);
    pos += ratio;
  }
  const keep = Math.floor(pos);
  A.rsBuf = buf.slice(keep); A.rsPos = pos - keep;
  if (A.pend.length >= 640) flushMic(); // 40ms
}
function flushMic() {
  if (!A.pend.length) return;
  const a = Int16Array.from(A.pend); A.pend = [];
  if (S.ws && S.ws.readyState === 1) { S.ws.send(a.buffer); S.txBytes = (S.txBytes || 0) + a.byteLength; }
}

function playChunk(ab) {
  if (!A.ctx || A.ctx.state === 'closed' || ab.byteLength < 2) return;
  if (A.ctx.state !== 'running') ctxWake();
  const i16 = new Int16Array(ab, 0, ab.byteLength >> 1);
  S.rxBytes = (S.rxBytes || 0) + ab.byteLength;
  if (!S.rxShow || Date.now() - S.rxShow > 250) { S.rxShow = Date.now(); $('#diag').textContent = `📥 מקבל קול… ${Math.round(S.rxBytes / 1024)}KB${A.ctx.state !== 'running' ? ' · ⚠️ השמע מושהה – לחץ על המסך' : ''}`; }
  const b = A.ctx.createBuffer(1, i16.length, RATE);
  const d = b.getChannelData(0);
  for (let i = 0; i < i16.length; i++) d[i] = i16[i] / 32768;
  const src = A.ctx.createBufferSource();
  src.buffer = b; src.connect(A.out);
  const now = A.ctx.currentTime;
  if (A.nextTime < now + 0.02) A.nextTime = now + 0.22; // באפר נגד ריצוד
  src.start(A.nextTime);
  A.nextTime += b.duration;
}

function beep(freq = 880, dur = 0.08, vol = 0.12) {
  if (!A.ctx || A.ctx.state === 'closed') return;
  if (A.ctx.state !== 'running') ctxWake();
  const o = A.ctx.createOscillator(), g = A.ctx.createGain();
  o.frequency.value = freq; o.connect(g); g.connect(A.ctx.destination);
  const t = A.ctx.currentTime;
  g.gain.setValueAtTime(vol, t); g.gain.exponentialRampToValueAtTime(0.001, t + dur);
  o.start(t); o.stop(t + dur + 0.02);
}
function startAlarm(urgent) {
  stopAlarm(); ctxWake();
  const ring = () => { beep(urgent ? 1050 : 880, 0.18, 0.2); setTimeout(() => beep(urgent ? 780 : 660, 0.18, 0.2), 220); if (navigator.vibrate) navigator.vibrate(urgent ? [400, 150, 400] : [200]); };
  ring();
  if (urgent) A.alarm = setInterval(ring, 1400);
}
function stopAlarm() { if (A.alarm) clearInterval(A.alarm); A.alarm = null; if (navigator.vibrate) navigator.vibrate(0); ctxScheduleIdle(); }

// ============================================================ חיבור
function send(o) { if (S.ws && S.ws.readyState === 1) S.ws.send(JSON.stringify(o)); }

function connect() {
  if (S.ws && S.ws.readyState <= 1) return;
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.binaryType = 'arraybuffer';
  S.ws = ws;
  ws.onopen = () => {
    S.reconnectDelay = 1000; setConn(true);
    if (S.pendingAuth) send(S.pendingAuth);
    else if (S.token) send({ t: 'auth', token: S.token });
  };
  ws.onmessage = (e) => {
    if (typeof e.data !== 'string') return playChunk(e.data);
    let m; try { m = JSON.parse(e.data); } catch { return; }
    handle(m);
  };
  ws.onclose = () => {
    if (S.ws !== ws) return;
    setConn(false); S.authed = false;
    if (S.talking || S.wantTalk) { S.talking = false; S.wantTalk = false; renderPtt(); }
    if (S.stopReconnect) return;
    setTimeout(connect, S.reconnectDelay);
    S.reconnectDelay = Math.min(S.reconnectDelay * 2, 15000);
  };
}
function setConn(on) { $('#connDot').className = 'dot ' + (on ? 'on' : 'off'); }

function handle(m) {
  switch (m.t) {
    case 'hello': S.vapid = m.vapidPublicKey; $('#loginHint').textContent = m.allowRegister ? 'משתמש חדש? הכנס שם וסיסמה – החשבון ייפתח אוטומטית.' : 'את שם המשתמש והסיסמה מקבלים מהמנהל.'; break;
    case 'auth_ok':
      S.authed = true; S.pendingAuth = null; S.me = m.name; S.role = m.role; S.maxTalk = m.maxTalk || 60;
      S.token = m.token; localStorage.setItem('ptt_token', m.token);
      showMain();
      break;
    case 'auth_err':
      S.pendingAuth = null;
      if (m.clearToken) { S.token = null; localStorage.removeItem('ptt_token'); }
      showLogin(m.msg);
      break;
    case 'kicked':
      S.stopReconnect = true;
      if (m.clearToken) { S.token = null; localStorage.removeItem('ptt_token'); }
      showLogin(m.msg);
      break;
    case 'me':
      S.channels = m.channels; S.active = m.active; S.members = m.members; S.hasPush = m.hasPush;
      if (S.speaking !== m.speaking) A.nextTime = 0;
      S.speaking = m.speaking;
      renderTalk();
      if (S.recsCh !== S.active) { S.recsCh = S.active; S.recs = []; renderRecs(); requestRecs(); }
      renderPushBtn();
      break;
    case 'floor':
      if (!m.ok) { S.wantTalk = false; S.talking = false; toast(m.msg || 'הערוץ תפוס'); beep(300, 0.2); renderPtt(); break; }
      if (!S.wantTalk) { send({ t: 'ptt_stop' }); break; }
      S.talking = true; S.talkStart = Date.now();
      beep(1200, 0.06); renderPtt(); startTalkTimer();
      break;
    case 'floor_timeout':
      S.talking = false; S.wantTalk = false; toast(m.msg); stopTalkTimer(); micScheduleRelease(); renderPtt();
      break;
    case 'speaking':
      if (m.channel === S.active) {
        ctxWake(); S.speaking = m.name; A.nextTime = 0; S.rxBytes = 0; beep(700, 0.06); renderTalk();
        setTimeout(() => { if (A.ctx && A.ctx.state !== 'running') $('#unlock').classList.remove('hidden'); }, 700);
      }
      break;
    case 'speaking_end':
      if (m.channel === S.active) { S.speaking = null; setTimeout(() => beep(480, 0.08), 250); renderTalk(); if (S.rxBytes) $('#diag').textContent = `✓ התקבלו ${Math.round(S.rxBytes / 1024)}KB`; }
      break;
    case 'toast': toast(m.msg); break;
    case 'recs': if (m.channel === S.active) { S.recs = m.list; renderRecs(); } break;
    case 'recs_changed': if (m.channel === S.active) requestRecs(); break;
    case 'alert': queueAlert(m.alert); break;
    // ---- מנהל
    case 'state': S.state = m; renderAdmin(); break;
    case 'loc': onAdminLoc(m); break;
    case 'speak_start': onSpeakStart(m.entry); break;
    case 'speak_end': onSpeakEnd(m.entry); break;
  }
}

// ============================================================ מסכים
function showLogin(msg) {
  $('#main').classList.add('hidden');
  $('#login').classList.remove('hidden');
  $('#loginErr').textContent = msg || '';
  $('#loginBtn').disabled = false;
}

async function showMain() {
  $('#login').classList.add('hidden');
  $('#main').classList.remove('hidden');
  $('#meName').textContent = S.me;
  document.body.classList.toggle('is-admin', S.role === 'admin');
  $('#roleBadge').classList.toggle('hidden', S.role !== 'admin');
  if (S.role === 'admin') { initMap(); setTimeout(() => map && map.invalidateSize(), 50); }
  startGeo();
  keepAwake();
  registerSW();
  if (S.role === 'admin' && !S.agoTimer) S.agoTimer = setInterval(() => document.querySelectorAll('[data-ago]').forEach((el) => { el.textContent = ago(+el.dataset.ago); }), 10000);
  const ok = await audioInit();
  if (!ok) $('#unlock').classList.remove('hidden'); else ctxScheduleIdle();
}

$('#unlockBtn').onclick = async () => {
  $('#unlock').classList.add('hidden');
  await audioInit();
};

$('#loginForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const name = $('#loginName').value.trim(), password = $('#loginPass').value;
  if (!name || !password) return;
  $('#loginBtn').disabled = true; $('#loginErr').textContent = '';
  audioInit(); // נגיעת משתמש – פותח את השמע
  S.stopReconnect = false;
  S.pendingAuth = { t: 'auth', name, password };
  if (S.ws && S.ws.readyState === 1) send(S.pendingAuth); else connect();
  setTimeout(() => { $('#loginBtn').disabled = false; }, 2500);
});

$('#logoutBtn').onclick = async () => {
  let endpoint = null;
  try { const reg = await navigator.serviceWorker.getRegistration(); const sub = reg && await reg.pushManager.getSubscription(); if (sub) { endpoint = sub.endpoint; await sub.unsubscribe(); } } catch { /* ignore */ }
  send({ t: 'logout', endpoint });
  S.stopReconnect = true; S.token = null; localStorage.removeItem('ptt_token');
  showLogin('');
  setTimeout(() => { S.stopReconnect = false; connect(); }, 600);
};

// ============================================================ דיבור
function renderTalk() {
  const chips = $('#channelChips');
  chips.innerHTML = S.channels.map((c) =>
    `<button class="chip ${c.id === S.active ? 'active' : ''}" data-ch="${esc(c.id)}">${c.speaking ? '<span class="live"></span>' : ''}${esc(c.name)}</button>`).join('');
  $('#noChannels').classList.toggle('hidden', S.channels.length > 0);
  const st = $('#statusBox');
  if (S.talking) { st.className = 'status tx'; st.textContent = '🔴 אתה משדר…'; }
  else if (S.speaking && S.speaking !== S.me) { st.className = 'status rx'; st.textContent = `🔊 ${S.speaking} מדבר`; }
  else if (!S.active) { st.className = 'status'; st.textContent = 'לא נבחר ערוץ'; }
  else { st.className = 'status'; st.textContent = 'ערוץ: ' + ((S.channels.find((c) => c.id === S.active) || {}).name || ''); }
  $('#members').innerHTML = S.members.length
    ? S.members.map((u) => `<li class="${u.name === S.speaking ? 'speaking' : ''}"><span class="dot on"></span>${esc(u.name)}${u.role === 'admin' ? ' <span class="badge admin">מנהל</span>' : ''}${u.name === S.speaking ? ' 🔊' : ''}${u.name === S.me ? ' <span class="muted small">(אתה)</span>' : ''}</li>`).join('')
    : '<li class="muted">אין מחוברים</li>';
  renderPtt();
}
$('#channelChips').addEventListener('click', (e) => {
  const b = e.target.closest('[data-ch]'); if (!b) return;
  if (S.talking || S.wantTalk) stopTalk();
  send({ t: 'join', channel: b.dataset.ch });
});

function renderPtt() {
  const b = $('#pttBtn');
  b.className = 'ptt';
  let label = $('#toggleMode').checked ? 'לחץ לדיבור' : 'לחץ והחזק לדיבור';
  if (!S.active) { b.classList.add('disabled'); label = 'אין ערוץ'; }
  else if (S.talking) { b.classList.add('tx'); label = $('#toggleMode').checked ? 'לחץ לסיום' : 'משדר… שחרר לסיום'; }
  else if (S.wantTalk) { b.classList.add('wait'); label = 'מתחבר…'; }
  else if (S.speaking && S.speaking !== S.me) { b.classList.add('rx'); label = `${S.speaking} מדבר`; }
  $('#pttLabel').textContent = label;
  const st = $('#statusBox');
  if (S.talking && st) { st.className = 'status tx'; st.textContent = '🔴 אתה משדר…'; }
}

async function startTalk() {
  if (S.talking || S.wantTalk) return;
  if (!S.active) return toast('לא נבחר ערוץ');
  if (S.speaking && S.speaking !== S.me) { beep(300, 0.2); return toast(`${S.speaking} מדבר כרגע`); }
  S.wantTalk = true; renderPtt();
  ctxWake();
  const mic = micInit();          // פתיחת מיקרופון ובקשת רשות דיבור – במקביל, לזמן תגובה קצר
  sendLocNow(true);
  send({ t: 'ptt_start' });
  preciseFix();                   // GPS מדויק רק ברגע הדיבור
  await audioInit();
  if (!(await mic)) { if (S.talking || S.wantTalk) { S.wantTalk = false; S.talking = false; send({ t: 'ptt_stop' }); } renderPtt(); }
}
function stopTalk() {
  const was = S.talking;
  S.wantTalk = false;
  if (was) { flushMic(); S.talking = false; send({ t: 'ptt_stop' }); beep(600, 0.05); }
  stopTalkTimer();
  micScheduleRelease();
  renderPtt(); renderTalk();
}

// ---- הקלטות אחרונות
function requestRecs() { if (S.active) send({ t: 'recs', channel: S.active }); }
const recAudio = new Audio();
recAudio.preload = 'auto';
let recPlaying = null;
recAudio.addEventListener('ended', () => { recPlaying = null; renderRecs(); });
recAudio.addEventListener('pause', () => { if (recAudio.ended || recAudio.currentTime === 0) return; recPlaying = null; renderRecs(); });
function renderRecs() {
  const list = S.recs || [];
  $('#recsList').innerHTML = list.length ? list.map((r) => `
    <li class="${recPlaying === r.id ? 'playing' : ''}">
      <span class="meta"><b>${esc(r.name)}${r.name === S.me ? ' <span class="muted small">(אתה)</span>' : ''}</b><small>${fmtTime(r.start)} · ${Math.max(1, Math.round(r.dur))} שנ׳</small></span>
      <button class="play-btn" data-rec="${esc(r.id)}" aria-label="נגן">${recPlaying === r.id ? '⏹' : '▶'}</button>
    </li>`).join('') : '<li class="muted">אין עדיין הקלטות בערוץ הזה</li>';
}
async function playRec(id) {
  if (recPlaying === id) { recAudio.pause(); recPlaying = null; renderRecs(); return; }
  try {
    const res = await fetch('rec/' + encodeURIComponent(id), { headers: { Authorization: 'Bearer ' + S.token } });
    if (!res.ok) { toast(res.status === 404 ? 'ההקלטה כבר לא זמינה' : 'לא ניתן לנגן כרגע'); return; }
    const url = URL.createObjectURL(await res.blob());
    if (recAudio.src && recAudio.src.startsWith('blob:')) URL.revokeObjectURL(recAudio.src);
    recAudio.src = url;
    recPlaying = id; renderRecs();
    await recAudio.play();
  } catch (e) { recPlaying = null; renderRecs(); toast('לא ניתן לנגן: ' + e.message); }
}
$('#recsList').addEventListener('click', (e) => { const b = e.target.closest('[data-rec]'); if (b) playRec(b.dataset.rec); });
$('#testSoundBtn').addEventListener('click', async () => {
  await audioInit();
  $('#unlock').classList.add('hidden');
  beep(660, 0.25, 0.3); setTimeout(() => beep(880, 0.25, 0.3), 300); setTimeout(() => beep(1100, 0.35, 0.3), 600);
  toast(A.ctx && A.ctx.state === 'running' ? '🔊 אם שמעת 3 צפצופים – הרמקול תקין' : '⚠️ השמע חסום – נסה ללחוץ שוב');
});

const ptt = $('#pttBtn');
ptt.addEventListener('pointerdown', (e) => {
  e.preventDefault();
  if ($('#toggleMode').checked) { (S.talking || S.wantTalk) ? stopTalk() : startTalk(); return; }
  try { ptt.setPointerCapture(e.pointerId); } catch { /* ignore */ }
  startTalk();
});
const release = () => { if (!$('#toggleMode').checked) stopTalk(); };
ptt.addEventListener('pointerup', release);
ptt.addEventListener('pointercancel', release);
ptt.addEventListener('contextmenu', (e) => e.preventDefault());
$('#toggleMode').addEventListener('change', renderPtt);
let spaceDown = false;
document.addEventListener('keydown', (e) => {
  if (e.code !== 'Space' || e.repeat || /INPUT|TEXTAREA|SELECT/.test(document.activeElement.tagName) || !S.authed) return;
  e.preventDefault(); spaceDown = true; startTalk();
});
document.addEventListener('keyup', (e) => { if (e.code === 'Space' && spaceDown) { spaceDown = false; stopTalk(); } });

let talkTimer = null;
function startTalkTimer() {
  stopTalkTimer();
  S.txBytes = 0; S.txLevel = 0; S.txMax = 0;
  const tick = () => {
    $('#talkTimer').textContent = `${Math.floor((Date.now() - S.talkStart) / 1000)} / ${S.maxTalk} שניות`;
    const lv = Math.min(100, Math.round((S.txLevel || 0) * 140));
    $('#diag').innerHTML = `📤 נשלח ${Math.round((S.txBytes || 0) / 1024)}KB <span class="lvl"><i style="width:${lv}%"></i></span>${!A.micReady ? ' · פותח מיקרופון…' : (S.txBytes > 32000 && (S.txMax || 0) < 0.01 ? ' · ⚠️ המיקרופון לא קולט' : '')}`;
  };
  tick(); talkTimer = setInterval(tick, 300);
}
function stopTalkTimer() { clearInterval(talkTimer); talkTimer = null; $('#talkTimer').textContent = ''; if (S.txBytes) $('#diag').textContent = `✓ נשלחו ${Math.round(S.txBytes / 1024)}KB`; }

// ============================================================ מיקום
// חיסכון בסוללה: מעקב רגיל במצב חסכוני (רשת/WiFi), ו-GPS מדויק רק כשלוחצים לדבר
function onPos(p) {
  S.loc = { lat: p.coords.latitude, lng: p.coords.longitude, acc: Math.round(p.coords.accuracy) };
  setLocStatus(`📍 מיקום פעיל (דיוק ±${S.loc.acc} מ׳)`);
  if (Date.now() - S.lastLocSent > 10000 || S.talking || S.wantTalk) sendLocNow(true);
}
function onPosErr(err) {
  if (S.loc) return;
  setLocStatus(err.code === 1 ? '⚠️ אין הרשאת מיקום – אשר מיקום בהגדרות הדפדפן' : '⚠️ לא ניתן לקבל מיקום', true);
}
function startGeo() {
  if (S.geoWatch != null) return;
  if (!navigator.geolocation) return setLocStatus('⚠️ המכשיר לא תומך במיקום');
  S.geoWatch = navigator.geolocation.watchPosition(onPos, onPosErr, { enableHighAccuracy: false, maximumAge: 60000, timeout: 30000 });
}
function preciseFix() {
  if (!navigator.geolocation) return;
  navigator.geolocation.getCurrentPosition(onPos, onPosErr, { enableHighAccuracy: true, maximumAge: 10000, timeout: 8000 });
}
function sendLocNow(force) {
  if (!S.loc || !S.authed) return;
  if (!force && Date.now() - S.lastLocSent < 1000) return;
  send({ t: 'loc', lat: S.loc.lat, lng: S.loc.lng, acc: S.loc.acc });
  S.lastLocSent = Date.now();
}
setInterval(() => sendLocNow(), 60000);
function setLocStatus(t, bad) { const el = $('#locStatus'); el.textContent = t; el.style.color = bad ? '#fca5a5' : ''; }

let wakeLock = null;
$('#wakeChk').checked = localStorage.getItem('ptt_wake') === '1';
$('#wakeChk').addEventListener('change', () => {
  localStorage.setItem('ptt_wake', $('#wakeChk').checked ? '1' : '0');
  if ($('#wakeChk').checked) keepAwake(); else if (wakeLock) { wakeLock.release().catch(() => {}); wakeLock = null; }
});
async function keepAwake() {
  if (!$('#wakeChk').checked) return;
  try { if ('wakeLock' in navigator && !wakeLock) { wakeLock = await navigator.wakeLock.request('screen'); wakeLock.addEventListener('release', () => { wakeLock = null; }); } } catch { /* ignore */ }
}
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') {
    keepAwake();
    if (S.authed) sendLocNow(true);
    if (!S.ws || S.ws.readyState > 1) { if (!S.stopReconnect) connect(); }
  }
});

// ============================================================ Push / Service Worker
let swReg = null;
async function registerSW() {
  if (!('serviceWorker' in navigator)) return;
  try { swReg = await navigator.serviceWorker.register('sw.js'); } catch { swReg = null; }
  renderPushBtn();
  // אם כבר יש הרשאה – לרשום מחדש בשקט
  if (swReg && window.Notification && Notification.permission === 'granted' && S.role === 'user') subscribePush(true);
}
function pushSupported() { return 'serviceWorker' in navigator && 'PushManager' in window && window.Notification; }
function renderPushBtn() {
  const b = $('#pushBtn');
  const show = S.role === 'user' && S.authed && !S.hasPush;
  b.classList.toggle('hidden', !show);
}
function urlB64(base64) {
  const pad = '='.repeat((4 - (base64.length % 4)) % 4);
  const raw = atob((base64 + pad).replace(/-/g, '+').replace(/_/g, '/'));
  return Uint8Array.from([...raw].map((c) => c.charCodeAt(0)));
}
async function subscribePush(silent) {
  if (!pushSupported()) {
    if (!silent) toast(/iPhone|iPad/.test(navigator.userAgent) ? 'באייפון: שתף ← "הוסף למסך הבית", פתח משם ונסה שוב' : 'הדפדפן לא תומך בהתראות');
    return;
  }
  try {
    const perm = await Notification.requestPermission();
    if (perm !== 'granted') { if (!silent) toast('לא אושרו התראות'); return; }
    const reg = swReg || await navigator.serviceWorker.ready;
    let sub = await reg.pushManager.getSubscription();
    if (!sub) sub = await reg.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: urlB64(S.vapid) });
    send({ t: 'push_sub', sub: sub.toJSON() });
    if (!silent) toast('✅ התראות הופעלו');
  } catch (e) { if (!silent) toast('הפעלת ההתראות נכשלה: ' + e.message); }
}
$('#pushBtn').onclick = () => subscribePush(false);

if ('serviceWorker' in navigator) {
  navigator.serviceWorker.addEventListener('message', (e) => {
    if (e.data && e.data.t === 'alert_ack') send({ t: 'alert_ack', id: e.data.id });
  });
}

// ============================================================ התראות נכנסות
function queueAlert(a) {
  if (S.currentAlert && S.currentAlert.id === a.id) return;
  if (S.alertQueue.some((x) => x.id === a.id)) return;
  S.alertQueue.push(a);
  if (!S.currentAlert) nextAlert();
}
function nextAlert() {
  const a = S.alertQueue.shift();
  S.currentAlert = a || null;
  if (!a) { $('#alertOverlay').classList.add('hidden'); stopAlarm(); return; }
  $('#alertCard').classList.toggle('urgent', !!a.urgent);
  $('#alertIcon').textContent = a.urgent ? '🚨' : '🔔';
  $('#alertTitleView').textContent = a.title;
  $('#alertBodyView').textContent = a.body;
  $('#alertMeta').textContent = `מאת ${a.from} · ${fmtTime(a.ts)}`;
  $('#alertOverlay').classList.remove('hidden');
  startAlarm(a.urgent);
}
$('#alertAckBtn').onclick = async () => {
  if (S.currentAlert) {
    send({ t: 'alert_ack', id: S.currentAlert.id });
    try { const reg = await navigator.serviceWorker.getRegistration(); if (reg) (await reg.getNotifications({ tag: S.currentAlert.id })).forEach((n) => n.close()); } catch { /* ignore */ }
  }
  nextAlert();
};

// ============================================================ מנהל – מפה
let map = null; const markers = new Map(); let fitted = false;
function initMap() {
  if (map || !window.L) return;
  map = L.map('map', { zoomControl: true }).setView([31.6, 34.9], 8);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 19, attribution: '© OpenStreetMap' }).addTo(map);
  window.addEventListener('resize', () => map.invalidateSize());
}
function pinHtml(u) {
  const cls = ['pin'];
  if (!u.online) cls.push('off');
  if (u.role === 'admin') cls.push('admin');
  if (u.speaking || S.live.has(u.name)) cls.push('speaking');
  return `<div class="${cls.join(' ')}">${S.live.has(u.name) ? '🔊 ' : ''}${esc(u.name)}</div>`;
}
function popupHtml(u) {
  const l = u.loc;
  return `<b>${esc(u.name)}</b>${u.role === 'admin' ? ' (מנהל)' : ''}<br>${u.online ? '🟢 מחובר' : '⚪ לא מחובר'}${u.active ? ' · ערוץ ' + esc(chName(u.active)) : ''}<br>` +
    (l ? `עודכן ${ago(l.ts)} · דיוק ±${l.acc} מ׳<br>${navLinks(l.lat, l.lng)}` : 'אין מיקום');
}
function upsertMarker(u) {
  if (!map) return;
  let mk = markers.get(u.name);
  if (!u.loc) { if (mk) { map.removeLayer(mk); markers.delete(u.name); } return; }
  const icon = L.divIcon({ className: 'pin-wrap', html: pinHtml(u), iconSize: null });
  if (!mk) {
    mk = L.marker([u.loc.lat, u.loc.lng], { icon, riseOnHover: true }).addTo(map).bindPopup('');
    markers.set(u.name, mk);
  } else {
    mk.setLatLng([u.loc.lat, u.loc.lng]); mk.setIcon(icon);
  }
  mk.setZIndexOffset(S.live.has(u.name) ? 1000 : u.online ? 100 : 0);
  mk.setPopupContent(popupHtml(u));
}
function syncMarkers() {
  if (!S.state || !map) return;
  const names = new Set(S.state.users.map((u) => u.name));
  for (const [n, mk] of markers) if (!names.has(n)) { map.removeLayer(mk); markers.delete(n); }
  S.state.users.forEach(upsertMarker);
  if (!fitted && markers.size) { fitAll(); fitted = true; }
}
function fitAll() {
  if (!map || !markers.size) return;
  const b = L.latLngBounds([...markers.values()].map((m) => m.getLatLng()));
  map.fitBounds(b.pad(0.3), { maxZoom: 15 });
}
$('#fitBtn').onclick = fitAll;

function onAdminLoc(m) {
  if (!S.state) return;
  const u = S.state.users.find((x) => x.name === m.name);
  if (!u) return;
  u.loc = m.loc; u.online = true;
  upsertMarker(u);
  const live = S.live.get(m.name);
  if (live) {
    Object.assign(live, { lat: m.loc.lat, lng: m.loc.lng, acc: m.loc.acc });
    renderBanner();
    if ($('#followChk').checked && map) map.panTo([m.loc.lat, m.loc.lng]);
  }
}
function onSpeakStart(entry) {
  S.live.set(entry.name, entry);
  if (S.state) {
    S.state.log.unshift(entry);
    const u = S.state.users.find((x) => x.name === entry.name);
    if (u) { u.speaking = entry.channel; upsertMarker(u); }
    renderLog();
  }
  renderBanner();
  if ($('#followChk').checked && map && entry.lat != null) {
    map.flyTo([entry.lat, entry.lng], Math.max(map.getZoom(), 15), { duration: 0.8 });
  }
}
function onSpeakEnd(entry) {
  S.live.delete(entry.name);
  if (S.state) {
    const i = S.state.log.findIndex((x) => x.id === entry.id);
    if (i >= 0) S.state.log[i] = entry; else S.state.log.unshift(entry);
    const u = S.state.users.find((x) => x.name === entry.name);
    if (u) { u.speaking = null; upsertMarker(u); }
    renderLog();
  }
  renderBanner();
}
function renderBanner() {
  const el = $('#speakBanner');
  if (!S.live.size) { el.classList.add('hidden'); return; }
  el.innerHTML = [...S.live.values()].map((e) =>
    `<div class="b-row">🔴 <b>${esc(e.name)}</b> מדבר ב״${esc(e.channelName)}״ · ${e.lat != null ? `±${e.acc} מ׳ · ${navLinks(e.lat, e.lng)}` : '<span>מיקום לא ידוע</span>'}</div>`).join('');
  el.classList.remove('hidden');
}

// ============================================================ מנהל – פאנלים
$('#tabs').addEventListener('click', (e) => {
  const b = e.target.closest('[data-tab]'); if (!b) return;
  document.querySelectorAll('#tabs button').forEach((x) => x.classList.toggle('active', x === b));
  document.querySelectorAll('.tab').forEach((t) => t.classList.toggle('active', t.id === 'tab-' + b.dataset.tab));
});
function admin(op, extra) { send(Object.assign({ t: 'admin', op }, extra)); }
function chName(id) { const c = S.state && S.state.channels.find((x) => x.id === id); return c ? c.name : '?'; }

function renderAdmin() {
  if (S.role !== 'admin' || !S.state) return;
  // סינכרון דוברים חיים מתוך המצב
  for (const e of S.state.log) if (!e.end) S.live.set(e.name, e); else if (S.live.get(e.name) && S.live.get(e.name).id === e.id) S.live.delete(e.name);
  renderUsers(); renderNewUserChannels(); renderChannels(); renderAlerts(); renderLog(); syncMarkers(); renderBanner();
}

function renderUsers() {
  const st = S.state;
  const ae = document.activeElement;
  if (ae && ae.tagName === 'SELECT' && $('#usersList').contains(ae)) { setTimeout(renderUsers, 1500); return; } // לא לרנדר באמצע בחירה
  $('#usersList').innerHTML = st.users.map((u) => {
    const isAdm = u.role === 'admin';
    const boxes = isAdm ? '<div class="item-sub">גישה לכל הערוצים</div>' : `<div class="ch-boxes">${st.channels.map((c) =>
      `<label class="ch-box"><input type="checkbox" data-act="assign" data-user="${esc(u.name)}" data-ch="${esc(c.id)}" ${u.channels.includes(c.id) ? 'checked' : ''}>${esc(c.name)}</label>`).join('') || '<span class="muted small">אין ערוצים</span>'}</div>`;
    const actions = isAdm ? '' : `<div class="item-actions">
        <select data-act="move" data-user="${esc(u.name)}"><option value="">העבר לקבוצה…</option>${st.channels.map((c) => `<option value="${esc(c.id)}">${esc(c.name)}</option>`).join('')}</select>
        <button class="btn small" data-act="alert" data-user="${esc(u.name)}">🔔 התראה</button>
        <button class="btn small" data-act="pass" data-user="${esc(u.name)}">🔑 סיסמה</button>
        ${u.online ? `<button class="btn small" data-act="kick" data-user="${esc(u.name)}">נתק</button>` : ''}
        <button class="btn small ${u.blocked ? '' : 'danger'}" data-act="block" data-on="${u.blocked ? '' : '1'}" data-user="${esc(u.name)}">${u.blocked ? 'בטל חסימה' : 'חסום'}</button>
        <button class="btn small danger" data-act="delete" data-user="${esc(u.name)}">מחק</button>
      </div>`;
    return `<div class="item ${u.online ? '' : 'off'}">
      <div class="item-head"><span class="dot ${u.online ? 'on' : ''}"></span><b>${esc(u.name)}</b>
        ${isAdm ? '<span class="badge admin">מנהל</span>' : ''}
        ${u.blocked ? '<span class="tag red">חסום</span>' : ''}
        ${u.speaking ? '<span class="tag red">🔊 מדבר</span>' : ''}
        ${!isAdm ? (u.push ? '<span class="tag green">🔔 פוש</span>' : '<span class="tag">ללא פוש</span>') : ''}
        <span class="grow"></span>
        ${u.loc ? `<button class="btn small" data-act="locate" data-user="${esc(u.name)}">🗺️</button>` : ''}
      </div>
      <div class="item-sub">${u.online ? (u.active ? 'בערוץ: ' + esc(chName(u.active)) : 'לא בערוץ') : 'לא מחובר'} · ${u.loc ? `📍 <span data-ago="${u.loc.ts}">${ago(u.loc.ts)}</span>` : 'אין מיקום'}</div>
      ${boxes}${actions}
    </div>`;
  }).join('');
}
$('#usersList').addEventListener('change', (e) => {
  const t = e.target, user = t.dataset.user;
  if (t.dataset.act === 'assign') admin('assign', { user, channel: t.dataset.ch, on: t.checked });
  if (t.dataset.act === 'move' && t.value) { admin('move', { user, channel: t.value }); t.blur(); }
});
$('#usersList').addEventListener('click', (e) => {
  const b = e.target.closest('button[data-act]'); if (!b) return;
  const user = b.dataset.user;
  switch (b.dataset.act) {
    case 'kick': admin('kick', { user }); break;
    case 'block': admin('block', { user, on: !!b.dataset.on }); break;
    case 'delete': if (confirm(`למחוק את המשתמש "${user}"?`)) admin('delete_user', { user }); break;
    case 'locate': locateUser(user); break;
    case 'alert': openAlertTo('user:' + user); break;
    case 'pass': {
      const pw = prompt(`סיסמה חדשה ל"${user}" (לפחות 4 תווים):`);
      if (pw == null) break;
      if (pw.length < 4) { toast('הסיסמה חייבת להיות לפחות 4 תווים'); break; }
      admin('set_password', { user, password: pw });
      break;
    }
  }
});
function renderNewUserChannels() {
  const box = $('#newUserChannels');
  const checked = new Set([...box.querySelectorAll('input:checked')].map((i) => i.value));
  box.innerHTML = S.state.channels.map((c) => `<label class="ch-box"><input type="checkbox" value="${esc(c.id)}" ${checked.has(c.id) ? 'checked' : ''}>${esc(c.name)}</label>`).join('');
}
$('#newUserForm').addEventListener('submit', (e) => {
  e.preventDefault();
  const name = $('#newUserName').value.trim(), password = $('#newUserPass').value;
  if (password.length < 4) return toast('הסיסמה חייבת להיות לפחות 4 תווים');
  const channels = [...$('#newUserChannels').querySelectorAll('input:checked')].map((i) => i.value);
  admin('create_user', { name, password, channels });
  $('#newUserName').value = ''; $('#newUserPass').value = '';
});
function locateUser(name) {
  const mk = markers.get(name);
  if (mk && map) { map.flyTo(mk.getLatLng(), 16, { duration: 0.6 }); mk.openPopup(); }
  if (window.innerWidth <= 820) window.scrollTo({ top: 0, behavior: 'smooth' });
}

function renderChannels() {
  $('#channelsList').innerHTML = S.state.channels.map((c) => `
    <div class="item">
      <div class="item-head"><b>${esc(c.name)}</b>${c.speaking ? `<span class="tag red">🔊 ${esc(c.speaking)}</span>` : ''}<span class="grow"></span>
        <span class="tag green">${c.online} מחוברים</span><span class="tag">${c.assigned} מורשים</span></div>
      <div class="item-sub">חברים בקבוצה (סמן כדי לחבר):</div>
      <div class="mem-boxes">${S.state.users.filter((u) => u.role !== 'admin').map((u) =>
        `<label class="ch-box"><input type="checkbox" data-act="member" data-ch="${esc(c.id)}" data-user="${esc(u.name)}" ${u.channels.includes(c.id) ? 'checked' : ''}><span class="dot ${u.online ? 'on' : ''}"></span>${esc(u.name)}</label>`).join('') || '<span class="muted small">אין משתמשים עדיין</span>'}</div>
      <div class="item-actions">
        <button class="btn small" data-act="all" data-id="${esc(c.id)}">חבר את כולם</button>
        <button class="btn small" data-act="none" data-id="${esc(c.id)}">נקה</button>
        <button class="btn small" data-act="rename" data-id="${esc(c.id)}">שנה שם</button>
        <button class="btn small" data-act="alert" data-id="${esc(c.id)}">🔔 התראה לערוץ</button>
        <button class="btn small danger" data-act="del" data-id="${esc(c.id)}">מחק</button>
      </div>
    </div>`).join('') || '<div class="muted">אין ערוצים</div>';
}
$('#newChannelForm').addEventListener('submit', (e) => {
  e.preventDefault();
  const name = $('#newChannelName').value.trim();
  if (name) { admin('create_channel', { name }); $('#newChannelName').value = ''; }
});
$('#channelsList').addEventListener('click', (e) => {
  const b = e.target.closest('button[data-act]'); if (!b) return;
  const id = b.dataset.id;
  if (b.dataset.act === 'del' && confirm(`למחוק את הערוץ "${chName(id)}"?`)) admin('delete_channel', { id });
  if (b.dataset.act === 'rename') { const n = prompt('שם חדש לערוץ', chName(id)); if (n && n.trim()) admin('rename_channel', { id, name: n.trim() }); }
  if (b.dataset.act === 'alert') openAlertTo('channel:' + id);
  if (b.dataset.act === 'all') admin('set_members', { channel: id, users: S.state.users.filter((u) => u.role !== 'admin').map((u) => u.name) });
  if (b.dataset.act === 'none' && confirm(`להוציא את כולם מ"${chName(id)}"?`)) admin('set_members', { channel: id, users: [] });
});
$('#channelsList').addEventListener('change', (e) => {
  const t = e.target;
  if (t.dataset.act === 'member') admin('assign', { user: t.dataset.user, channel: t.dataset.ch, on: t.checked });
});

// ---- התראות (מנהל)
function renderAlerts() {
  const sel = $('#alertTarget'), prev = sel.value;
  sel.innerHTML = `<option value="all">📢 כל המשתמשים</option>` +
    S.state.channels.map((c) => `<option value="channel:${esc(c.id)}">📡 ערוץ: ${esc(c.name)}</option>`).join('') +
    S.state.users.filter((u) => u.role !== 'admin').map((u) => `<option value="user:${esc(u.name)}">👤 ${esc(u.name)}${u.push ? '' : ' (ללא פוש)'}</option>`).join('');
  if ([...sel.options].some((o) => o.value === prev)) sel.value = prev;
  $('#alertsList').innerHTML = S.state.alerts.map((a) => `
    <div class="item">
      <div class="item-head">${a.urgent ? '🚨' : '🔔'} <b>${esc(a.title)}</b><span class="grow"></span><span class="muted small">${fmtTime(a.ts)}</span></div>
      <div style="white-space:pre-wrap;margin-top:6px">${esc(a.body)}</div>
      <div class="item-sub">אל: ${esc(a.targetLabel)} · אישרו קבלה ${a.acks}/${a.total}${a.ackNames.length ? ' (' + a.ackNames.map(esc).join(', ') + ')' : ''}</div>
      <div class="item-sub">נמסר באפליקציה: ${a.stats.ws} · פוש: ${a.stats.push}${a.stats.pushFail ? ` · נכשלו ${a.stats.pushFail}` : ''}</div>
      <div class="item-actions"><button class="btn small ghost" data-del="${esc(a.id)}">מחק מהרשימה</button></div>
    </div>`).join('') || '<div class="muted">עדיין לא נשלחו התראות</div>';
}
function openAlertTo(value) {
  document.querySelector('#tabs [data-tab="alerts"]').click();
  $('#alertTarget').value = value;
  $('#alertBody').focus();
}
$('#alertForm').addEventListener('submit', (e) => {
  e.preventDefault();
  const v = $('#alertTarget').value;
  let target = { type: 'all' };
  if (v.startsWith('channel:')) target = { type: 'channel', id: v.slice(8) };
  else if (v.startsWith('user:')) target = { type: 'user', id: v.slice(5) };
  admin('send_alert', { target, title: $('#alertTitle').value, body: $('#alertBody').value, urgent: $('#alertUrgent').checked });
  $('#alertBody').value = ''; $('#alertUrgent').checked = false;
});
$('#alertsList').addEventListener('click', (e) => {
  const b = e.target.closest('[data-del]'); if (b) admin('delete_alert', { id: b.dataset.del });
});

// ---- יומן
function renderLog() {
  if (!S.state) return;
  $('#logList').innerHTML = S.state.log.map((e) => {
    const dur = e.end ? Math.max(1, Math.round((e.end - e.start) / 1000)) + ' שנ׳' : '<span class="tag red">משדר עכשיו</span>';
    return `<div class="item log-item">
      <div class="item-head"><b>${esc(e.name)}</b><span class="tag amber">${esc(e.channelName)}</span><span class="grow"></span><span class="muted small">${fmtTime(e.start)}</span></div>
      <div class="item-sub">משך: ${dur} · ${e.lat != null ? `📍 ${e.lat.toFixed(5)}, ${e.lng.toFixed(5)} (±${e.acc} מ׳)` : 'אין מיקום'}</div>
      <div class="item-actions">${e.hasRec ? `<button class="btn small" data-play="${esc(e.id)}">▶ השמע</button>` : ''}${e.lat != null ? `<button class="btn small" data-fly="${e.lat},${e.lng}">🗺️ הצג במפה</button>${navLinks(e.lat, e.lng)}` : ''}</div>
    </div>`;
  }).join('') || '<div class="muted">אין שידורים עדיין</div>';
}
$('#logList').addEventListener('click', (e) => {
  const pb = e.target.closest('[data-play]');
  if (pb) { playRec(pb.dataset.play); return; }
  const b = e.target.closest('[data-fly]'); if (!b || !map) return;
  const [lat, lng] = b.dataset.fly.split(',').map(Number);
  map.flyTo([lat, lng], 17, { duration: 0.6 });
  L.popup().setLatLng([lat, lng]).setContent('מיקום השידור').openOn(map);
  if (window.innerWidth <= 820) window.scrollTo({ top: 0, behavior: 'smooth' });
});
$('#clearLogBtn').onclick = () => { if (confirm('לנקות את יומן השידורים?')) admin('clear_log'); };

// ============================================================ עזרים
function navLinks(lat, lng) {
  return `<a href="https://www.google.com/maps?q=${lat},${lng}" target="_blank" rel="noopener">Google Maps</a> · <a href="https://waze.com/ul?ll=${lat},${lng}&navigate=yes" target="_blank" rel="noopener">Waze</a>`;
}
function ago(ts) {
  const s = Math.round((Date.now() - ts) / 1000);
  if (s < 10) return 'עכשיו';
  if (s < 60) return `לפני ${s} שנ׳`;
  if (s < 3600) return `לפני ${Math.round(s / 60)} דק׳`;
  if (s < 86400) return `לפני ${Math.round(s / 3600)} שע׳`;
  return new Date(ts).toLocaleDateString('he-IL');
}
function fmtTime(ts) {
  const d = new Date(ts), today = new Date().toDateString() === d.toDateString();
  return d.toLocaleTimeString('he-IL', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) + (today ? '' : ' ' + d.toLocaleDateString('he-IL'));
}
let toastT = null;
function toast(msg) {
  const t = $('#toast'); t.textContent = msg; t.classList.remove('hidden');
  clearTimeout(toastT); toastT = setTimeout(() => t.classList.add('hidden'), 3500);
}

// קישור להורדת האפליקציה – רק בדפדפן באנדרואיד (לא בתוך האפליקציה עצמה)
(function () {
  const inApp = document.referrer.startsWith('android-app://') || (window.matchMedia && matchMedia('(display-mode: standalone)').matches);
  if (/Android/i.test(navigator.userAgent) && !inApp) $('#apkLink').classList.remove('hidden');
})();

// ============================================================ התחלה
if (S.token) { $('#loginErr').textContent = 'מתחבר…'; }
connect();
// מסך פתיחה: לפחות 1.6 שניות, ונסגר כשהחיבור מוכן (או אחרי 4 שניות לכל היותר)
(function splash() {
  const el = $('#splash'), t0 = Date.now();
  const hide = () => { el.classList.add('out'); setTimeout(() => el.remove(), 500); };
  const check = () => {
    const ready = S.authed || !S.token || Date.now() - t0 > 4000;
    if (ready && Date.now() - t0 >= 1600) hide(); else setTimeout(check, 150);
  };
  check();
})();
