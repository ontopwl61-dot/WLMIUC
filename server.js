// ==========================================================
// Radio Voice backend v2 — Cloudflare Realtime (SFU)
// المتغيرات (Render > Environment): GAME_SECRET, CF_APP_ID, CF_APP_SECRET
// ==========================================================
const express = require('express');
const crypto = require('crypto');
const path = require('path');

const { GAME_SECRET, CF_APP_ID, CF_APP_SECRET, PORT = 3000 } = process.env;

const AWAY_MS = 30 * 60 * 1000;   // مهلة الكود بعد الطلوع من الراديو
const GONE_MS = 5 * 60 * 1000;    // اللعبة سكتت = اللاعب طلع
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const TRACK = 'mic';
const CF_BASE = `https://rtc.live.cloudflare.com/v1/apps/${CF_APP_ID}`;

process.on('uncaughtException', (e) => console.error('uncaught:', e));
process.on('unhandledRejection', (e) => console.error('unhandled:', e));

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
    signal: AbortSignal.timeout(10000),
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
    epoch: 1, report: { tracks: 0, at: 0 }, heal: 0, lowSince: 0,
    pubSession: null, published: false,
    recvSession: null, recvGen: 0, subscribed: new Set(), q: Promise.resolve(), waiter: null,
  };
  byUser.set(uid, s);
  byCode.set(s.code, s);
  return s;
}

// أي تغيير بالصوت (موجة جديدة، إعادة اتصال...) يرفع epoch والمتصفح يعيد بناء الاتصال
function resetAudio(s) {
  s.pubSession = null; s.published = false;
  s.recvSession = null; s.subscribed = new Set(); s.recvGen++; s.epoch++;
  s.q = Promise.resolve();
  s.report = { tracks: 0, at: 0 }; s.lowSince = 0;
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
  if (s.freq == null) return { type: 'state', inRadio: false, awayUntil: s.awayAt + AWAY_MS, epoch: s.epoch };
  return { type: 'state', inRadio: true, room: s.roomKey, listenOnly: s.listenOnly, ptt: s.ptt, epoch: s.epoch };
}
function push(s) { if (s.res) send(s, statePayload(s)); }

// ---------- مين يسمع مين ----------
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

function subscribeTask(s, peers, deliver) {
  return enqueue(s, async () => {
    const gen = s.recvGen;
    const list = peers.filter((p) => p.pubSession && !s.subscribed.has(p.pubSession));
    if (!list.length || !s.recvSession) return;
    const recv = s.recvSession;
    const j = await cf('POST', `/sessions/${recv}/tracks/new`, {
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
        await cf('PUT', `/sessions/${recv}/renegotiate`, { sessionDescription: { type: 'answer', sdp: ans } });
      }
    }
  });
}

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
const view = (s) => ({ linked: s.linked, code: s.linked ? null : fmt(s.code), listenOnly: s.listenOnly });

// ---------- مسارات اللعبة ----------
// يتأكد إن اللاعب عنده جلسة (ويطلع له كود لو ما ربط). تُستدعى لما يحاول يدخل الراديو بدون ربط
app.post('/api/ensure', requireGame, (req, res) => {
  const { userId, name, jobId } = req.body || {};
  const uid = String(userId || '');
  if (!uid) return res.status(400).json({ error: 'userId' });
  let s = byUser.get(uid);
  if (s && s.freq == null && s.awayAt && Date.now() - s.awayAt > AWAY_MS) { destroy(s, 'expired'); s = null; }
  if (!s) s = createSession(uid);
  s.name = String(name || s.name || '').slice(0, 40);
  s.jobId = String(jobId || s.jobId || 'studio').slice(0, 60);
  s.lastSeen = Date.now();
  res.json(view(s));
});

// دخل موجة (freq) أو طلع من الراديو (freq = null)
app.post('/api/state', requireGame, (req, res) => {
  const { userId, name, jobId, freq } = req.body || {};
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
  const changed = newKey !== s.roomKey;
  s.freq = String(freq).slice(0, 12);
  s.roomKey = newKey;
  s.awayAt = null; s.ptt = false; s.lastSeen = Date.now();
  if (changed) resetAudio(s);
  push(s);
  res.json(view(s));
});

app.post('/api/status', requireGame, (req, res) => {
  const out = {};
  for (const id of (req.body.userIds || []).slice(0, 300)) {
    const s = byUser.get(String(id));
    if (s) { s.lastSeen = Date.now(); out[id] = view(s); }
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

// خروج اللاعب من اللعبة نهائيًا
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
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (now > v.reset) hits.delete(k); }, 60000);

const sessBySid = (req) => bySid.get(String((req.body && req.body.sid) || req.query.sid || ''));
// يرفض الطلبات القديمة (من اتصال سابق)
function fresh(s, req, res) {
  if (!s) { res.status(404).json({ error: 'no_session' }); return false; }
  if (Number(req.body && req.body.epoch) !== s.epoch) { res.status(409).json({ error: 'stale' }); return false; }
  return true;
}

// ربط الموقع: mode = 'talk' (متحدث) أو 'listen' (مستمع)
app.post('/link', (req, res) => {
  if (limited(req.ip)) return res.status(429).json({ error: 'too_many' });
  const s = byCode.get(normalize(req.body && req.body.code));
  if (!s) return res.status(404).json({ error: 'bad_code' });
  if (s.sid) bySid.delete(s.sid);
  s.sid = crypto.randomBytes(24).toString('hex');
  bySid.set(s.sid, s);
  s.listenOnly = req.body.mode === 'listen';
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

// الموقع: "فصل المايك كامل" → نلغي الجلسة (يطلع له كود جديد ولازم يربط من جديد)
app.post('/unlink', (req, res) => {
  const s = sessBySid(req);
  if (s) destroy(s, 'ended');
  res.json({ ok: true });
});

// المتصفح يطلب إعادة بناء الاتصال (انقطع مثلًا)
app.post('/rtc/reset', (req, res) => {
  const s = sessBySid(req);
  if (!s) return res.status(404).json({});
  resetAudio(s); push(s);
  res.json({ ok: true });
});

// --- نشر صوت اللاعب ---
app.post('/rtc/publish', async (req, res) => {
  const s = sessBySid(req);
  if (!fresh(s, req, res)) return;
  if (s.freq == null) return res.status(409).json({ error: 'not_in_radio' });
  if (s.listenOnly) return res.status(403).json({ error: 'listen_only' });
  const epoch = s.epoch;
  try {
    const { sdp, mid } = req.body;
    const sess = await cf('POST', '/sessions/new');
    if (epoch !== s.epoch) return res.status(409).json({ error: 'stale' });
    s.pubSession = sess.sessionId; s.published = false;
    const j = await cf('POST', `/sessions/${s.pubSession}/tracks/new`, {
      sessionDescription: { type: 'offer', sdp },
      tracks: [{ location: 'local', mid, trackName: TRACK }],
    });
    const t = (j.tracks || [])[0];
    if (t && t.errorCode) throw new Error('publish track error ' + t.errorCode + ' ' + (t.errorDescription || ''));
    res.json({ sdp: j.sessionDescription.sdp });
  } catch (e) { console.error(e.message); res.status(500).json({ error: 'publish_failed' }); }
});

app.post('/rtc/ready', (req, res) => {
  const s = sessBySid(req);
  if (!fresh(s, req, res)) return;
  if (!s.pubSession) return res.status(404).json({});
  s.published = true;
  notifyListeners(s);
  res.json({ ok: true });
});

// --- استقبال أصوات الباقين ---
app.post('/rtc/recv-init', async (req, res) => {
  const s = sessBySid(req);
  if (!fresh(s, req, res)) return;
  if (s.freq == null) return res.status(409).json({ error: 'not_in_radio' });
  const epoch = s.epoch;
  try {
    s.recvGen++; s.subscribed = new Set(); s.q = Promise.resolve();
    const sess = await cf('POST', '/sessions/new');
    if (epoch !== s.epoch) return res.status(409).json({ error: 'stale' });
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
  if (s && s.waiter && Number(req.body.epoch) === s.epoch) s.waiter(String(req.body.sdp || ''));
  res.json({ ok: true });
});

// المتصفح يبلغ كم صوت مستلم فعلًا (للتشخيص والإصلاح التلقائي)
app.post('/rtc/report', (req, res) => {
  const s = sessBySid(req);
  if (s && Number(req.body.epoch) === s.epoch) {
    s.report = {
      tracks: Number(req.body.tracks) || 0,
      pub: String(req.body.pub || '').slice(0, 20),
      recv: String(req.body.recv || '').slice(0, 20),
      at: Date.now(),
    };
  }
  res.json({ ok: true });
});

// تشخيص: افتح /debug?key=GAME_SECRET (لا ترسل الرابط لأحد، أرسل الناتج بس)
app.get('/debug', (req, res) => {
  const given = Buffer.from(String(req.query.key || ''));
  const real = Buffer.from(String(GAME_SECRET || ''));
  if (!GAME_SECRET || given.length !== real.length || !crypto.timingSafeEqual(given, real)) return res.status(401).end();
  const now = Date.now();
  res.json([...byUser.values()].map((s) => ({
    name: s.name, room: s.roomKey, linked: s.linked, listenOnly: s.listenOnly,
    published: s.published, hasRecv: !!s.recvSession, subscribed: s.subscribed.size,
    expectedPeers: s.freq == null ? 0 : peersOf(s).length,
    reportedTracks: s.report.tracks, reportAgeSec: s.report.at ? Math.round((now - s.report.at) / 1000) : null,
    pub: s.report.pub, recv: s.report.recv, heals: s.heal, epoch: s.epoch,
  })));
});

app.get('/health', (req, res) => res.send('ok'));

// إصلاح تلقائي: لو لاعب المفروض يسمع ٣ ومستلم أقل (لأي سبب)، نعيد بناء اتصاله (٣ مرات كحد أقصى)
setInterval(() => {
  const now = Date.now();
  for (const l of byUser.values()) {
    if (l.freq == null || !l.res || !l.recvSession) { l.lowSince = 0; continue; }
    const expected = peersOf(l).length;
    const got = l.report.at && now - l.report.at < 25000 ? l.report.tracks : 0;
    if (expected > 0 && got < expected) {
      if (!l.lowSince) l.lowSince = now;
      else if (now - l.lowSince > 8000 && l.heal < 3) {
        l.heal++; l.lowSince = 0;
        console.warn('heal', l.name, 'expected', expected, 'got', got);
        resetAudio(l); push(l);
      }
    } else {
      l.lowSince = 0;
      if (expected > 0) l.heal = 0;
    }
  }
}, 5000);

setInterval(() => {
  const now = Date.now();
  for (const s of [...byUser.values()]) {
    if (s.freq == null && s.awayAt && now - s.awayAt > AWAY_MS) destroy(s, 'expired');
    else if (now - s.lastSeen > GONE_MS) destroy(s, 'gone');
  }
}, 20000);

app.listen(PORT, () => console.log('Radio voice (Cloudflare) v2 on', PORT));
