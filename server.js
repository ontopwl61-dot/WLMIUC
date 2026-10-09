// ==========================================================
// Radio Voice backend  (Express + LiveKit)
// يشتغل على Render. المتغيرات المطلوبة (Environment):
//   GAME_SECRET, LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET
// ==========================================================
const express = require('express');
const crypto = require('crypto');
const path = require('path');
const { AccessToken } = require('livekit-server-sdk');

const {
  GAME_SECRET,
  LIVEKIT_URL,
  LIVEKIT_API_KEY,
  LIVEKIT_API_SECRET,
  PORT = 3000,
} = process.env;

const AWAY_MS = 30 * 60 * 1000;      // مهلة انتهاء الكود بعد الطلوع من الراديو
const GONE_MS = 5 * 60 * 1000;       // لو اللعبة سكتت هالمدة نعتبر اللاعب طلع
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';

const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: '200kb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---------- التخزين (بالذاكرة) ----------
const byUser = new Map();   // userId -> session
const byCode = new Map();   // CODE -> session
const bySid  = new Map();   // sid  -> session

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
    userId: uid, name: '', jobId: 'studio', freq: null,
    code: genCode(), sid: null, linked: false, res: null,
    ptt: false, pttSeq: 0, awayAt: null, lastSeen: Date.now(),
  };
  byUser.set(uid, s);
  byCode.set(s.code, s);
  return s;
}

function destroy(s, reason) {
  if (s.res) {
    try { s.res.write(`data: ${JSON.stringify({ type: 'end', reason })}\n\n`); s.res.end(); } catch (e) {}
    s.res = null;
  }
  byUser.delete(s.userId);
  byCode.delete(s.code);
  if (s.sid) bySid.delete(s.sid);
}

// ---------- بث الحالة للموقع ----------
async function statePayload(s) {
  if (s.freq == null) {
    return { type: 'state', inRadio: false, awayUntil: s.awayAt + AWAY_MS };
  }
  const room = `${s.jobId}__${s.freq}`.replace(/[^\w.-]/g, '_').slice(0, 100);
  const at = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, {
    identity: `u${s.userId}`,
    name: s.name || 'لاعب',
    ttl: '6h',
  });
  at.addGrant({ roomJoin: true, room, canPublish: true, canSubscribe: true });
  return { type: 'state', inRadio: true, room, url: LIVEKIT_URL, token: await at.toJwt(), ptt: s.ptt };
}
function send(s, obj) {
  if (s.res) { try { s.res.write(`data: ${JSON.stringify(obj)}\n\n`); } catch (e) {} }
}
async function push(s) {
  if (!s.res) return;
  try { send(s, await statePayload(s)); } catch (e) { console.error('push error', e.message); }
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
// اللاعب دخل موجة (freq) أو طلع من الراديو (freq = null)
app.post('/api/state', requireGame, async (req, res) => {
  const { userId, name, jobId, freq } = req.body || {};
  const uid = String(userId || '');
  if (!uid) return res.status(400).json({ error: 'userId' });

  let s = byUser.get(uid);
  if (s && s.freq == null && s.awayAt && Date.now() - s.awayAt > AWAY_MS) {
    destroy(s, 'expired');
    s = null;
  }

  if (freq == null) {
    if (s) {
      s.freq = null; s.ptt = false; s.awayAt = Date.now(); s.lastSeen = Date.now();
      await push(s);
    }
    return res.json({ ok: true });
  }

  if (!s) s = createSession(uid);
  s.name = String(name || '').slice(0, 40);
  s.jobId = String(jobId || 'studio').slice(0, 60);
  s.freq = String(freq).slice(0, 12);
  s.awayAt = null; s.ptt = false; s.lastSeen = Date.now();
  await push(s);
  res.json({ linked: s.linked, code: s.linked ? null : fmt(s.code) });
});

// اللعبة تسأل: مين تفعّل؟ (كل ٣ ثواني)
app.post('/api/status', requireGame, (req, res) => {
  const out = {};
  for (const id of (req.body.userIds || []).slice(0, 300)) {
    const s = byUser.get(String(id));
    if (s) {
      s.lastSeen = Date.now();
      out[id] = { linked: s.linked, code: s.linked ? null : fmt(s.code) };
    }
  }
  res.json({ sessions: out });
});

// ضغط/ترك زر التحدث (دفعات)
app.post('/api/ptt', requireGame, (req, res) => {
  for (const e of (req.body.events || []).slice(0, 200)) {
    const s = byUser.get(String(e.userId));
    if (!s || !s.linked) continue;
    const seq = Number(e.seq) || 0;
    if (seq <= s.pttSeq) continue;
    s.pttSeq = seq;
    s.ptt = !!e.ptt;
    send(s, { type: 'ptt', ptt: s.ptt });
  }
  res.json({ ok: true });
});

// "لا أريد" من اللعبة أو خروج اللاعب من اللعبة
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

app.post('/link', (req, res) => {
  if (limited(req.ip)) return res.status(429).json({ error: 'too_many' });
  const s = byCode.get(normalize(req.body && req.body.code));
  if (!s) return res.status(404).json({ error: 'bad_code' });
  if (s.sid) bySid.delete(s.sid);
  s.sid = crypto.randomBytes(24).toString('hex');
  bySid.set(s.sid, s);
  res.json({ sid: s.sid });
});

app.get('/events', async (req, res) => {
  const s = bySid.get(String(req.query.sid || ''));
  if (!s) return res.status(404).end();
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.flushHeaders();

  if (s.res && s.res !== res) {
    try { s.res.write(`data: ${JSON.stringify({ type: 'end', reason: 'replaced' })}\n\n`); s.res.end(); } catch (e) {}
  }
  s.res = res; s.linked = true; s.lastSeen = Date.now();
  await push(s);

  const beat = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 20000);
  req.on('close', () => {
    clearInterval(beat);
    if (s.res === res) { s.res = null; s.linked = false; s.ptt = false; }
  });
});

app.get('/health', (req, res) => res.send('ok'));

// ---------- تنظيف دوري ----------
setInterval(() => {
  const now = Date.now();
  for (const s of [...byUser.values()]) {
    if (s.freq == null && s.awayAt && now - s.awayAt > AWAY_MS) destroy(s, 'expired');
    else if (now - s.lastSeen > GONE_MS) destroy(s, 'gone');
  }
}, 20000);

app.listen(PORT, () => console.log('Radio voice backend on', PORT));
