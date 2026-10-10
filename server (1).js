// ==========================================================
// Radio Voice backend — نسخة Cloudflare Realtime (SFU)
// المتغيرات المطلوبة (Environment في Render):
//   GAME_SECRET, CF_APP_ID, CF_APP_SECRET
// ==========================================================
const express = require('express');
const crypto = require('crypto');
const path = require('path');

const { GAME_SECRET, CF_APP_ID, CF_APP_SECRET, PORT = 3000 } = process.env;

const AWAY_MS = 30 * 60 * 1000;
const GONE_MS = 5 * 60 * 1000;
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const TRACK = 'mic';
const CF_BASE = `https://rtc.live.cloudflare.com/v1/apps/${CF_APP_ID}`;

const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: '200kb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---------- Cloudflare API ----------
async function cf(method, p, body) {
  const r = await fetch(CF_BASE + p, {
    method,
    headers: { Authorization: `Bearer ${CF_APP_SECRET}`, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.errorCode) {
    throw new Error(`CF ${method} ${p} -> ${r.status} ${j.errorDescription || j.errorCode || ''}`);
  }
  return j;
}

// ---------- التخزين ----------
const byUser = new Map();
const byCode = new Map();
const bySid = new Map();

function genCode() {
  let c;
  do {
    c = '';
    for (let i = 0; i < 6; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)];
  } while (byCode.has(c));
  return c;
}
const fmt = (c) => c.slice(0, 3) + '-' + c.slice(3);
const normalize = (v) => String(v || '').toUpperCase().replace(/[^A-Z0-9]/g, '');

function createSession(uid) {
  const s = {
    userId: uid, name: '', jobId: 'studio', freq: null, roomKey: null,
    code: genCode(), sid: null, linked: false, res: null,
    ptt: false, pttSeq: 0, awayAt: null, lastSeen: Date.now(), listenOnly: false,
    // صوت
    pubSession: null, published: false,
    recvSession: null, recvGen: 0, subscribed: new Set(), q: Promise.resolve(), waiter: null,
  };
  byUser.set(uid, s);
  byCode.set(s.code, s);
  return s;
}

function resetAudio(s) {
  s.pubSession = null; s.published = false;
  s.recvSession = null; s.subscribed = new Set(); s.recvGen++;
}

function destroy(s, reason) {
  if (s.res) {
    try { s.res.write(`data: ${JSON.stringify({ type: 'end', reason })}\n\n`); s.res.end(); } catch (e) {}
    s.res = null;
  }
  resetAudio(s);
  byUser.delete(s.userId);
  byCode.delete(s.code);
  if (s.sid) bySid.delete(s.sid);
}

// ---------- SSE ----------
function send(s, obj) {
  if (s.res) { try { s.res.write(`data: ${JSON.stringify(obj)}\n\n`); } catch (e) {} }
}
function statePayload(s) {
  if (s.freq == null) return { type: 'state', inRadio: false, awayUntil: s.awayAt + AWAY_MS };
  return { type: 'state', inRadio: true, room: s.roomKey, listenOnly: s.listenOnly, ptt: s.ptt };
}
function push(s) { if (s.res) send(s, statePayload(s)); }

// ---------- منطق الاشتراك (مين يسمع مين) ----------
function peersOf(s) {
  const out = [];
  for (const p of byUser.values()) {
    if (p !== s && p.freq != null && p.roomKey === s.roomKey && p.published && p.pubSession) out.push(p);
  }
  return out;
}

function enqueue(s, fn) {
  s.q = s.q.then(fn).catch((e) => console.error('queue:', e.message));
  return s.q;
}

function waitAnswer(s, ms) {
  return new Promise((resolve) => {
    const t = setTimeout(() => { s.waiter = null; resolve(null); }, ms);
    s.waiter = (sdp) => { clearTimeout(t); s.waiter = null; resolve(sdp); };
  });
}

// يشترك s في صوت peers. deliver(sdp) يوصل الـ offer للمتصفح
function subscribeTask(s, peers, deliver) {
  return enqueue(s, async () => {
    const gen = s.recvGen;
    const list = peers.filter((p) => p.pubSession && !s.subscribed.has(p.pubSession));
    if (!list.length || !s.recvSession) return;
    const j = await cf('POST', `/sessions/${s.recvSession}/tracks/new`, {
      tracks: list.map((p) => ({ location: 'remote', sessionId: p.pubSession, trackName: TRACK })),
    });
    if (gen !== s.recvGen) return;
    list.forEach((p, i) => {
      const t = (j.tracks || [])[i];
      if (!t || !t.errorCode) s.subscribed.add(p.pubSession);
      else console.warn('subscribe track error:', t.errorCode, t.errorDescription);
    });
    if (j.requiresImmediateRenegotiation && j.sessionDescription) {
      const pending = waitAnswer(s, 10000);
      deliver(j.sessionDescription.sdp);
      const ans = await pending;
      if (ans && gen === s.recvGen) {
        await cf('PUT', `/sessions/${s.recvSession}/renegotiate`, {
          sessionDescription: { type: 'answer', sdp: ans },
        });
      }
    }
  });
}

// بعد ما اللاعب p صار ينشر صوته، نخلي كل اللي بموجته يشتركون فيه
function notifyListeners(p) {
  if (!p.published) return;
  for (const l of byUser.values()) {
    if (l !== p && l.freq != null && l.roomKey === p.roomKey && l.recvSession && l.res) {
      subscribeTask(l, [p], (sdp) => send(l, { type: 'offer', sdp }));
    }
  }
}

// ---------- حماية مسارات اللعبة ----------
function requireGame(req, res, next) {
  const given = Buffer.from(String(req.get('x-secret') || ''));
  const real = Buffer.from(String(GAME_SECRET || ''));
  if (!GAME_SECRET || given.length !== real.length || !crypto.timingSafeEqual(given, real)) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

// ---------- مسارات اللعبة ----------
app.post('/api/state', requireGame, (req, res) => {
  const { userId, name, jobId, freq, listenOnly } = req.body || {};
  const uid = String(userId || '');
  if (!uid) return res.status(400).json({ error: 'userId' });

  let s = byUser.get(uid);
  if (s && s.freq == null && s.awayAt && Date.now() - s.awayAt > AWAY_MS) { destroy(s, 'expired'); s = null; }

  if (freq == null) {
    if (s) {
      s.freq = null; s.roomKey = null; s.ptt = false; s.awayAt = Date.now(); s.lastSeen = Date.now();
      resetAudio(s);
      push(s);
    }
    return res.json({ ok: true });
  }

  if (!s) s = createSession(uid);
  s.name = String(name || '').slice(0, 40);
  s.jobId = String(jobId || 'studio').slice(0, 60);
  const newKey = `${s.jobId}__${String(freq).slice(0, 12)}`;
  if (newKey !== s.roomKey) resetAudio(s);       // غيّر موجة → المتصفح يعيد الاتصال
  s.freq = String(freq).slice(0, 12);
  s.roomKey = newKey;
  if (typeof listenOnly === 'boolean') s.listenOnly = listenOnly;
  s.awayAt = null; s.ptt = false; s.lastSeen = Date.now();
  push(s);
  res.json({ linked: s.linked, code: s.linked ? null : fmt(s.code) });
});

app.post('/api/status', requireGame, (req, res) => {
  const out = {};
  for (const id of (req.body.userIds || []).slice(0, 300)) {
    const s = byUser.get(String(id));
    if (s) { s.lastSeen = Date.now(); out[id] = { linked: s.linked, code: s.linked ? null : fmt(s.code) }; }
  }
  res.json({ sessions: out });
});

app.post('/api/ptt', requireGame, (req, res) => {
  for (const e of (req.body.events || []).slice(0, 200)) {
    const s = byUser.get(String(e.userId));
    if (!s || !s.linked || s.listenOnly) continue;
    const seq = Number(e.seq) || 0;
    if (seq <= s.pttSeq) continue;
    s.pttSeq = seq; s.ptt = !!e.ptt;
    send(s, { type: 'ptt', ptt: s.ptt });
  }
  res.json({ ok: true });
});

app.post('/api/mode', requireGame, (req, res) => {
  const s = byUser.get(String(req.body.userId || ''));
  if (s) { s.listenOnly = !!req.body.listenOnly; s.ptt = false; s.lastSeen = Date.now(); resetAudio(s); push(s); }
  res.json({ ok: true });
});

app.post('/api/decline', requireGame, (req, res) => {
  const s = byUser.get(String(req.body.userId || ''));
  if (s) destroy(s, 'ended');
  res.json({ ok: true });
});

// ---------- مسارات الموقع ----------
const hits = new Map();
function limited(ip) {
  const now = Date.now();
  let h = hits.get(ip);
  if (!h || now > h.reset) { h = { n: 0, reset: now + 60000 }; hits.set(ip, h); }
  return ++h.n > 12;
}
const sessBySid = (req) => bySid.get(String((req.body && req.body.sid) || req.query.sid || ''));

app.post('/link', (req, res) => {
  if (limited(req.ip)) return res.status(429).json({ error: 'too_many' });
  const s = byCode.get(normalize(req.body && req.body.code));
  if (!s) return res.status(404).json({ error: 'bad_code' });
  if (s.sid) bySid.delete(s.sid);
  s.sid = crypto.randomBytes(24).toString('hex');
  bySid.set(s.sid, s);
  res.json({ sid: s.sid, listenOnly: s.listenOnly });
});

app.get('/events', (req, res) => {
  const s = bySid.get(String(req.query.sid || ''));
  if (!s) return res.status(404).end();
  res.set({
    'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive', 'X-Accel-Buffering': 'no',
  });
  res.flushHeaders();
  if (s.res && s.res !== res) {
    try { s.res.write(`data: ${JSON.stringify({ type: 'end', reason: 'replaced' })}\n\n`); s.res.end(); } catch (e) {}
  }
  s.res = res; s.linked = true; s.lastSeen = Date.now();
  resetAudio(s);
  push(s);

  const beat = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 20000);
  req.on('close', () => {
    clearInterval(beat);
    if (s.res === res) { s.res = null; s.linked = false; s.ptt = false; resetAudio(s); }
  });
});

// --- نشر صوت اللاعب ---
app.post('/rtc/publish', async (req, res) => {
  const s = sessBySid(req);
  if (!s || s.freq == null) return res.status(404).json({ error: 'no_session' });
  if (s.listenOnly) return res.status(403).json({ error: 'listen_only' });
  try {
    const { sdp, mid } = req.body;
    const sess = await cf('POST', '/sessions/new');
    s.pubSession = sess.sessionId; s.published = false;
    const j = await cf('POST', `/sessions/${s.pubSession}/tracks/new`, {
      sessionDescription: { type: 'offer', sdp },
      tracks: [{ location: 'local', mid, trackName: TRACK }],
    });
    res.json({ sdp: j.sessionDescription.sdp });
  } catch (e) { console.error(e.message); res.status(500).json({ error: 'publish_failed' }); }
});

// المتصفح يقول: اتصلت فعلًا، الحين الكل يقدر يشترك فيني
app.post('/rtc/ready', (req, res) => {
  const s = sessBySid(req);
  if (!s || !s.pubSession) return res.status(404).json({});
  s.published = true;
  notifyListeners(s);
  res.json({ ok: true });
});

// --- استقبال أصوات الباقين ---
app.post('/rtc/recv-init', async (req, res) => {
  const s = sessBySid(req);
  if (!s || s.freq == null) return res.status(404).json({ error: 'no_session' });
  try {
    s.recvGen++; s.subscribed = new Set(); s.q = Promise.resolve();
    const sess = await cf('POST', '/sessions/new');
    s.recvSession = sess.sessionId;
    let resolveOffer;
    const offerP = new Promise((r) => { resolveOffer = r; });
    const task = subscribeTask(s, peersOf(s), (sdp) => resolveOffer(sdp));
    const offer = await Promise.race([offerP, task.then(() => null)]);
    res.json({ offer });
  } catch (e) { console.error(e.message); res.status(500).json({ error: 'recv_failed' }); }
});

app.post('/rtc/answer', (req, res) => {
  const s = sessBySid(req);
  if (s && s.waiter) s.waiter(String(req.body.sdp || ''));
  res.json({ ok: true });
});

app.get('/health', (req, res) => res.send('ok'));

setInterval(() => {
  const now = Date.now();
  for (const s of [...byUser.values()]) {
    if (s.freq == null && s.awayAt && now - s.awayAt > AWAY_MS) destroy(s, 'expired');
    else if (now - s.lastSeen > GONE_MS) destroy(s, 'gone');
  }
}, 20000);

app.listen(PORT, () => console.log('Radio voice (Cloudflare) backend on', PORT));
