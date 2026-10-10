// ==========================================================
// Radio Voice backend v3 — Cloudflare Realtime SFU (بدون أي مكتبات خارجية)
// المتغيرات (Render > Environment): GAME_SECRET, CF_APP_ID, CF_APP_SECRET
//
// قواعد الكود:
//  - الكود يتجدد كل ما اللاعب يدخل الماب، وطول ما هو بالماب نفس الكود (حتى لو مو في موجة).
//  - لو طلع من الماب أو فصل الربط من الموقع: ينتهي ويطلع له كود جديد.
//
// قواعد Cloudflare اللي يلتزم فيها هذا الملف (من وثائقهم الرسمية):
//  1) جلسة الاستقبال تُنشأ فقط لما نبدأ فعلًا نشترك بمسار (الجلسة غير المتصلة تنتهي بسرعة وترجع 410).
//  2) المسار اللي ما يوصله أي صوت ٣٠ ثانية ينحذف، فالمتصفح يرسل نبضة صمت قصيرة كل ١٠ ثواني.
//  3) كل العمليات على جلسة واحدة تمشي بالدور (طابور) ولا نبدأ عملية قبل ما تخلص السابقة بالكامل.
//  4) نفحص نتيجة كل مسار على حدة، مو بس كود الاستجابة.
// ==========================================================
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const {
  GAME_SECRET, CF_APP_ID, CF_APP_SECRET, PORT = 3000,
  CF_API_BASE = 'https://rtc.live.cloudflare.com/v1',
} = process.env;

const CF_BASE = `${CF_API_BASE}/apps/${CF_APP_ID}`;
const GONE_MS = 5 * 60 * 1000;            // اللعبة سكتت = اللاعب طلع
const ANSWER_TIMEOUT_MS = +process.env.ANSWER_TIMEOUT_MS || 10000;
const MAX_SUBS = 40;
const MONITOR_MS = +process.env.MONITOR_MS || 5000;            // كل كم نفحص الاشتراكات
const HEAL_AFTER_MS = +process.env.HEAL_AFTER_MS || 10000;       // كم ننتظر قبل نعيد بناء استقبال ناقص
const NOT_PUBLISHED_MS = +process.env.NOT_PUBLISHED_MS || 20000; // كم ننتظر قبل نعيد نشر متحدث ما اكتمل نشره                       // لو تراكمت اشتراكات ميتة أكثر نبني الاستقبال من جديد
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const TRACK = 'mic';

process.on('uncaughtException', (e) => console.error('uncaught:', e));
process.on('unhandledRejection', (e) => console.error('unhandled:', e));

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
    const e = new Error(`CF ${method} ${p.replace(/[0-9a-f]{20,}/g, '…')} -> ${r.status} ${j.errorCode || ''} ${j.errorDescription || ''}`);
    e.status = r.status; e.code = j.errorCode;
    throw e;
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
    ptt: false, pttSeq: 0, lastSeen: Date.now(), listenOnly: false,
    epoch: 1, stateAt: Date.now(),
    // نشر
    pubSession: null, published: false, pubFixes: 0,
    // استقبال
    recvSession: null, recvGen: 1, subscribed: new Set(), q: Promise.resolve(), waiter: null, fails: 0,
    report: { tracks: 0, at: 0 }, heal: 0, lowSince: 0,
    rl: { n: 0, reset: 0 },
  };
  byUser.set(uid, s);
  byCode.set(s.code, s);
  return s;
}

function dropRecv(s, notify) {
  s.recvSession = null; s.subscribed = new Set(); s.recvGen++;
  s.report = { tracks: 0, at: 0 }; s.lowSince = 0;
  if (s.waiter) { const w = s.waiter; s.waiter = null; w(null); }
  if (notify) send(s, { type: 'recv-reset', gen: s.recvGen });
}
// أي تغيير كبير (موجة جديدة، إعادة اتصال...) يرفع epoch والمتصفح يعيد بناء الإرسال
function resetAudio(s) {
  s.pubSession = null; s.published = false;
  dropRecv(s, false);
  s.epoch++; s.stateAt = Date.now();
}

function destroy(s, reason) {
  if (s.res) {
    try { s.res.write(`data: ${JSON.stringify({ type: 'end', reason })}\n\n`); s.res.end(); } catch (e) {}
    s.res = null;
  }
  s.pubSession = null; s.published = false; s.recvSession = null; s.recvGen++;
  if (s.waiter) { const w = s.waiter; s.waiter = null; w(null); }
  byUser.delete(s.userId);
  byCode.delete(s.code);
  if (s.sid) bySid.delete(s.sid);
}

// ---------- SSE ----------
function send(s, obj) {
  if (s.res) { try { s.res.write(`data: ${JSON.stringify(obj)}\n\n`); } catch (e) {} }
}
function statePayload(s) {
  if (s.freq == null) return { type: 'state', inRadio: false, epoch: s.epoch };
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
  const run = s.q.then(fn);
  s.q = run.catch((e) => console.error('queue:', e.message));
  return s.q;
}
function waitAnswer(s, ms) {
  return new Promise((resolve) => {
    const t = setTimeout(() => { s.waiter = null; resolve(null); }, ms);
    s.waiter = (sdp) => { clearTimeout(t); s.waiter = null; resolve(sdp); };
  });
}

// منشور ما عاد يشتغل عند Cloudflare (انتهى بعد ٣٠ ثانية صمت مثلًا): نعيد نشره
function restorePublisher(p, why) {
  if (p.pubFixes >= 4) return;
  p.pubFixes++;
  console.warn('restore publisher', p.name, why);
  resetAudio(p); push(p);
  setTimeout(() => { if (byUser.get(p.userId) === p) p.pubFixes = Math.max(0, p.pubFixes - 1); }, 60000);
}

// يشترك s في صوت peers (بالدور، وبدون ما نبدأ عملية قبل ما تخلص السابقة)
function subscribeTask(s, peers) {
  const gen = s.recvGen;
  return enqueue(s, async () => {
    if (gen !== s.recvGen || !s.res || s.freq == null) return;
    const list = peers.filter((p) => p.pubSession && p.published && !s.subscribed.has(p.pubSession));
    if (!list.length) return;
    try {
      if (!s.recvSession) {                       // جلسة الاستقبال تنشأ هنا بس، لحظة الاشتراك
        const sess = await cf('POST', '/sessions/new');
        if (gen !== s.recvGen) return;
        s.recvSession = sess.sessionId;
      }
      const recv = s.recvSession;
      const j = await cf('POST', `/sessions/${recv}/tracks/new`, {
        tracks: list.map((p) => ({ location: 'remote', sessionId: p.pubSession, trackName: TRACK })),
      });
      if (gen !== s.recvGen) return;

      let good = 0;
      list.forEach((p, i) => {
        const t = (j.tracks || []).find((x) => x && x.sessionId === p.pubSession && x.trackName === TRACK) || (j.tracks || [])[i];
        if (t && t.errorCode) {
          console.warn('subscribe track error', t.errorCode, t.errorDescription || '');
          restorePublisher(p, 'subscribe:' + t.errorCode);
        } else { s.subscribed.add(p.pubSession); good++; }
      });

      if (j.requiresImmediateRenegotiation && j.sessionDescription && j.sessionDescription.sdp) {
        const pending = waitAnswer(s, ANSWER_TIMEOUT_MS);
        send(s, { type: 'offer', sdp: j.sessionDescription.sdp, gen });
        const ans = await pending;
        if (gen !== s.recvGen) return;
        if (!ans) throw Object.assign(new Error('no answer from browser'), { status: 0 });
        await cf('PUT', `/sessions/${recv}/renegotiate`, { sessionDescription: { type: 'answer', sdp: ans } });
      }
      if (good) s.fails = 0;
    } catch (e) {
      console.error('subscribe failed:', e.message);
      if (gen !== s.recvGen) return;
      // الجلسة انتهت أو ما رد المتصفح: نبني استقبال جديد من الصفر
      if (++s.fails <= 5) { dropRecv(s, true); setTimeout(() => reconcile(s), 1500 * s.fails); }
    }
  });
}

function reconcile(s) {
  if (byUser.get(s.userId) !== s || !s.res || s.freq == null || !s.linked) return;
  if (s.subscribed.size > MAX_SUBS) dropRecv(s, true);
  const missing = peersOf(s).filter((p) => !s.subscribed.has(p.pubSession));
  if (missing.length) subscribeTask(s, missing);
}
function reconcileRoom(roomKey) {
  for (const l of byUser.values()) if (l.roomKey === roomKey) reconcile(l);
}

// ---------- HTTP (بدون express) ----------
const PUBLIC = path.join(__dirname, 'public');
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.ico': 'image/x-icon', '.png': 'image/png' };
const routes = new Map();
const route = (method, p, fn) => routes.set(method + ' ' + p, fn);

function decorate(req, res) {
  res.status = (c) => { res.statusCode = c; return res; };
  res.json = (o) => {
    if (res.writableEnded) return;
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store');
    res.end(JSON.stringify(o));
  };
  const xf = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  req.ip = xf || req.socket.remoteAddress || '';
  req.get = (h) => req.headers[String(h).toLowerCase()];
}
function readBody(req) {
  return new Promise((resolve) => {
    if (req.method !== 'POST') return resolve({});
    let n = 0; const chunks = [];
    req.on('data', (c) => { n += c.length; if (n > 200 * 1024) { req.destroy(); } else chunks.push(c); });
    req.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}') || {}); } catch (e) { resolve({}); } });
    req.on('error', () => resolve({}));
  });
}

const server = http.createServer(async (req, res) => {
  try {
    decorate(req, res);
    const url = new URL(req.url, 'http://x');
    req.query = Object.fromEntries(url.searchParams);
    req.body = await readBody(req);
    const fn = routes.get(req.method + ' ' + url.pathname);
    if (fn) return await fn(req, res);
    if (req.method === 'GET') {                                    // ملفات الموقع
      let rel = url.pathname === '/' ? '/index.html' : url.pathname;
      const file = path.normalize(path.join(PUBLIC, rel));
      if (file.startsWith(PUBLIC) && fs.existsSync(file) && fs.statSync(file).isFile()) {
        res.setHeader('Content-Type', MIME[path.extname(file)] || 'application/octet-stream');
        return fs.createReadStream(file).pipe(res);
      }
    }
    res.status(404).json({ error: 'not_found' });
  } catch (e) {
    console.error('http error:', e.message);
    if (!res.headersSent) res.status(500).json({ error: 'server' });
  }
});

// ---------- حماية مسارات اللعبة ----------
const safeEq = (a, b) => {
  const x = Buffer.from(String(a || '')), y = Buffer.from(String(b || ''));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
};
const gameOk = (req) => GAME_SECRET && safeEq(req.get('x-secret'), GAME_SECRET);
const view = (s) => ({ linked: s.linked, code: s.linked ? null : fmt(s.code), listenOnly: s.listenOnly });

// ---------- مسارات اللعبة ----------
route('POST', '/api/ensure', (req, res) => {
  if (!gameOk(req)) return res.status(401).json({ error: 'unauthorized' });
  const { userId, name, jobId } = req.body;
  const uid = String(userId || '');
  if (!uid) return res.status(400).json({ error: 'userId' });
  let s = byUser.get(uid);
  if (!s) s = createSession(uid);
  s.name = String(name || s.name || '').slice(0, 40);
  s.jobId = String(jobId || s.jobId || 'studio').slice(0, 60);
  s.lastSeen = Date.now();
  res.json(view(s));
});

route('POST', '/api/state', (req, res) => {
  if (!gameOk(req)) return res.status(401).json({ error: 'unauthorized' });
  const { userId, name, jobId, freq } = req.body;
  const uid = String(userId || '');
  if (!uid) return res.status(400).json({ error: 'userId' });
  let s = byUser.get(uid);

  if (freq == null) {
    if (s) { s.freq = null; s.roomKey = null; s.ptt = false; s.lastSeen = Date.now(); resetAudio(s); push(s); }
    return res.json({ ok: true });
  }
  if (!s) s = createSession(uid);
  s.name = String(name || '').slice(0, 40);
  s.jobId = String(jobId || 'studio').slice(0, 60);
  const newKey = `${s.jobId}__${String(freq).slice(0, 12)}`;
  const changed = newKey !== s.roomKey || s.freq == null;
  s.freq = String(freq).slice(0, 12);
  s.roomKey = newKey;
  s.ptt = false; s.lastSeen = Date.now();
  if (changed) { resetAudio(s); s.pubFixes = 0; }
  push(s);
  if (changed) reconcile(s);
  res.json(view(s));
});

route('POST', '/api/status', (req, res) => {
  if (!gameOk(req)) return res.status(401).json({ error: 'unauthorized' });
  const out = {};
  for (const id of (Array.isArray(req.body.userIds) ? req.body.userIds : []).slice(0, 300)) {
    const s = byUser.get(String(id));
    if (s) { s.lastSeen = Date.now(); out[id] = view(s); }
  }
  res.json({ sessions: out });
});

route('POST', '/api/ptt', (req, res) => {
  if (!gameOk(req)) return res.status(401).json({ error: 'unauthorized' });
  for (const e of (Array.isArray(req.body.events) ? req.body.events : []).slice(0, 200)) {
    const s = byUser.get(String(e && e.userId));
    if (!s || !s.linked || s.listenOnly) continue;
    const seq = Number(e.seq) || 0;
    if (seq <= s.pttSeq) continue;
    s.pttSeq = seq; s.ptt = !!e.ptt;
    send(s, { type: 'ptt', ptt: s.ptt });
  }
  res.json({ ok: true });
});

// اللاعب دخل الماب (نبدأ من جديد) أو طلع منها (ينتهي الكود)
route('POST', '/api/decline', (req, res) => {
  if (!gameOk(req)) return res.status(401).json({ error: 'unauthorized' });
  const s = byUser.get(String(req.body.userId || ''));
  if (s) destroy(s, 'ended');
  res.json({ ok: true });
});

// ---------- مسارات الموقع ----------
const hits = new Map();
function limitedIp(ip) {
  const now = Date.now();
  let h = hits.get(ip);
  if (!h || now > h.reset) { h = { n: 0, reset: now + 60000 }; hits.set(ip, h); }
  return ++h.n > 12;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of hits) if (now > v.reset) hits.delete(k); }, 60000);

function limitedSid(s) {                    // حد لطلبات اللاعب الواحد
  const now = Date.now();
  if (now > s.rl.reset) s.rl = { n: 0, reset: now + 10000 };
  return ++s.rl.n > 80;
}
const sessBySid = (req) => bySid.get(String(req.body.sid || req.query.sid || ''));
function mine(req, res, needEpoch) {
  const s = sessBySid(req);
  if (!s) { res.status(404).json({ error: 'no_session' }); return null; }
  if (limitedSid(s)) { res.status(429).json({ error: 'too_many' }); return null; }
  if (needEpoch && Number(req.body.epoch) !== s.epoch) { res.status(409).json({ error: 'stale' }); return null; }
  return s;
}

route('POST', '/link', (req, res) => {
  if (limitedIp(req.ip)) return res.status(429).json({ error: 'too_many' });
  const s = byCode.get(normalize(req.body.code));
  if (!s) return res.status(404).json({ error: 'bad_code' });
  if (s.sid) bySid.delete(s.sid);
  s.sid = crypto.randomBytes(24).toString('hex');
  bySid.set(s.sid, s);
  s.listenOnly = req.body.mode === 'listen';
  res.json({ sid: s.sid, listenOnly: s.listenOnly });
});

route('GET', '/events', (req, res) => {
  const s = bySid.get(String(req.query.sid || ''));
  if (!s) return res.status(404).end();
  res.writeHead(200, {
    'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive', 'X-Accel-Buffering': 'no',
  });
  res.write(': open\n\n');
  if (s.res && s.res !== res) {
    try { s.res.write(`data: ${JSON.stringify({ type: 'end', reason: 'replaced' })}\n\n`); s.res.end(); } catch (e) {}
  }
  s.res = res; s.linked = true; s.lastSeen = Date.now();
  resetAudio(s);
  push(s);
  reconcile(s);

  const beat = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 20000);
  req.on('close', () => {
    clearInterval(beat);
    if (s.res === res) { s.res = null; s.linked = false; s.ptt = false; resetAudio(s); }
  });
});

route('POST', '/unlink', (req, res) => {
  const s = sessBySid(req);
  if (s) destroy(s, 'ended');
  res.json({ ok: true });
});

// المتصفح يطلب إعادة بناء الاتصال (انقطع مثلًا)
route('POST', '/rtc/reset', (req, res) => {
  const s = mine(req, res, false);
  if (!s) return;
  resetAudio(s); push(s); reconcile(s);
  res.json({ ok: true });
});

// --- نشر صوت اللاعب ---
route('POST', '/rtc/publish', async (req, res) => {
  const s = mine(req, res, true);
  if (!s) return;
  if (s.freq == null) return res.status(409).json({ error: 'not_in_radio' });
  if (s.listenOnly) return res.status(403).json({ error: 'listen_only' });
  const sdp = req.body.sdp, mid = String(req.body.mid == null ? '' : req.body.mid);
  if (typeof sdp !== 'string' || sdp.length < 50 || sdp.length > 40000 || !/^[\w.-]{1,16}$/.test(mid)) {
    return res.status(400).json({ error: 'bad_request' });
  }
  const epoch = s.epoch;
  try {
    const sess = await cf('POST', '/sessions/new');
    if (epoch !== s.epoch) return res.status(409).json({ error: 'stale' });
    s.pubSession = sess.sessionId; s.published = false;
    const j = await cf('POST', `/sessions/${s.pubSession}/tracks/new`, {
      sessionDescription: { type: 'offer', sdp },
      tracks: [{ location: 'local', mid, trackName: TRACK }],
    });
    const t = (j.tracks || [])[0];
    if (!j.sessionDescription || !j.sessionDescription.sdp || (t && t.errorCode)) {
      throw new Error('publish rejected ' + (t && t.errorCode));
    }
    if (epoch !== s.epoch) return res.status(409).json({ error: 'stale' });
    res.json({ sdp: j.sessionDescription.sdp });
  } catch (e) {
    console.error(e.message);
    if (epoch === s.epoch) s.pubSession = null;
    res.status(500).json({ error: 'publish_failed' });
  }
});

// المتصفح يقول: اتصلت فعلًا (ICE connected). الحين الكل يقدر يشترك فيني
route('POST', '/rtc/ready', (req, res) => {
  const s = mine(req, res, true);
  if (!s) return;
  if (!s.pubSession) return res.status(404).json({ error: 'no_publish' });
  s.published = true;
  reconcileRoom(s.roomKey);
  res.json({ ok: true });
});

route('POST', '/rtc/answer', (req, res) => {
  const s = mine(req, res, false);
  if (!s) return;
  const sdp = req.body.sdp;
  if (typeof sdp === 'string' && sdp.length < 40000 && Number(req.body.gen) === s.recvGen && s.waiter) s.waiter(sdp);
  res.json({ ok: true });
});

route('POST', '/rtc/report', (req, res) => {
  const s = mine(req, res, true);
  if (!s) return;
  s.report = {
    tracks: Math.max(0, Math.min(500, Number(req.body.tracks) || 0)),
    pub: String(req.body.pub || '').slice(0, 20), recv: String(req.body.recv || '').slice(0, 20),
    at: Date.now(),
  };
  res.json({ ok: true });
});

// تشخيص: /debug?key=GAME_SECRET (أرسل الناتج بس، لا ترسل الرابط)
route('GET', '/debug', (req, res) => {
  if (!GAME_SECRET || !safeEq(req.query.key, GAME_SECRET)) return res.status(401).end();
  const now = Date.now();
  res.json([...byUser.values()].map((s) => ({
    name: s.name, room: s.roomKey, linked: s.linked, listenOnly: s.listenOnly,
    published: s.published, hasRecv: !!s.recvSession, subscribed: s.subscribed.size,
    expectedPeers: s.freq == null ? 0 : peersOf(s).length,
    reportedTracks: s.report.tracks, reportAgeSec: s.report.at ? Math.round((now - s.report.at) / 1000) : null,
    pub: s.report.pub, recv: s.report.recv, heals: s.heal, fails: s.fails, epoch: s.epoch,
  })));
});

route('GET', '/health', (req, res) => { res.setHeader('Content-Type', 'text/plain'); res.end('ok'); });

// ---------- مراقبة وإصلاح تلقائي ----------
setInterval(() => {
  const now = Date.now();
  for (const s of [...byUser.values()]) {
    if (now - s.lastSeen > GONE_MS) { destroy(s, 'gone'); continue; }
    if (s.freq == null || !s.res || !s.linked) { s.lowSince = 0; continue; }

    // ١) لاعب متحدث ما اكتمل نشره: نعيد بناءه
    if (!s.listenOnly && !s.published && now - s.stateAt > NOT_PUBLISHED_MS) restorePublisher(s, 'not-published');

    // ٢) نكمل أي اشتراك ناقص
    reconcile(s);

    // ٣) لو المفروض يسمع أكثر مما يستلم فعلًا، نبني استقباله من جديد (٣ مرات كحد أقصى)
    const expected = peersOf(s).length;
    const fresh = s.report.at && now - s.report.at < 25000;
    if (expected > 0 && fresh && s.report.tracks < expected) {
      if (!s.lowSince) s.lowSince = now;
      else if (now - s.lowSince > HEAL_AFTER_MS && s.heal < 3) {
        s.heal++; s.lowSince = 0;
        console.warn('heal', s.name, 'expected', expected, 'got', s.report.tracks);
        dropRecv(s, true); reconcile(s);
      }
    } else if (expected > 0 && fresh) { s.lowSince = 0; s.heal = 0; }
    else s.lowSince = 0;
  }
}, MONITOR_MS);

server.listen(PORT, () => console.log('Radio voice (Cloudflare) v3 on', PORT));
module.exports = { server };
