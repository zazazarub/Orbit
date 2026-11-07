'use strict';
// Orbit messenger server. Node 18+.
// Only dependency: "pg" (loaded only when DATABASE_URL is set, otherwise data lives in local files).
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || __dirname;
const DB_URL = process.env.DATABASE_URL || '';

/* ================= storage adapters ================= */
function fileStore() {
  const file = path.join(DATA_DIR, 'data.json');
  const mdir = path.join(DATA_DIR, 'media');
  fs.mkdirSync(mdir, { recursive: true });
  return {
    kind: 'file',
    async load() { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch (e) { return null; } },
    async save(db) { const t = file + '.tmp'; fs.writeFileSync(t, JSON.stringify(db)); fs.renameSync(t, file); },
    async putMedia(id, buf) { fs.writeFileSync(path.join(mdir, id), buf); },
    async getMedia(id) { try { return fs.readFileSync(path.join(mdir, id)); } catch (e) { return null; } },
    async delMedia(id) { try { fs.unlinkSync(path.join(mdir, id)); } catch (e) {} }
  };
}

function pgStore() {
  const { Pool } = require('pg');
  const host = new URL(DB_URL).hostname;
  // external Render hostnames contain dots and need TLS; internal ones do not
  const pool = new Pool({ connectionString: DB_URL, ssl: host.includes('.') ? { rejectUnauthorized: false } : false, max: 5 });
  const ready = pool.query(
    'CREATE TABLE IF NOT EXISTS kv(k text PRIMARY KEY, v jsonb NOT NULL);' +
    'CREATE TABLE IF NOT EXISTS media(id text PRIMARY KEY, data bytea NOT NULL)');
  return {
    kind: 'postgres',
    async load() { await ready; const r = await pool.query("SELECT v FROM kv WHERE k='db'"); return r.rows[0] ? r.rows[0].v : null; },
    async save(db) { await ready; await pool.query("INSERT INTO kv(k,v) VALUES('db',$1) ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v", [JSON.stringify(db)]); },
    async putMedia(id, buf) { await ready; await pool.query('INSERT INTO media(id,data) VALUES($1,$2) ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data', [id, buf]); },
    async getMedia(id) { await ready; const r = await pool.query('SELECT data FROM media WHERE id=$1', [id]); return r.rows[0] ? r.rows[0].data : null; },
    async delMedia(id) { await ready; await pool.query('DELETE FROM media WHERE id=$1', [id]); }
  };
}

const store = DB_URL ? pgStore() : fileStore();

/* ================= state ================= */
const rnd = (n = 8) => crypto.randomBytes(n).toString('hex');
let db = { v: 2, users: {}, convs: {}, messages: [], media: {}, tokens: {}, pending: {}, vtokens: {}, seq: 0 };
const handles = new Map();   // handle -> {k:'u'|'c', id}
const byEmail = new Map();   // email -> user id
const invites = new Map();   // invite code -> conv id
const convMsgs = new Map();  // conv id -> [messages]
const msgById = new Map();   // message id -> message
const dmPeers = new Map();   // user id -> Set(user ids they have a DM with)
const online = new Set();    // user ids with an open connection
const offTimers = new Map();

function linkPeers(a, b) {
  for (const [x, y] of [[a, b], [b, a]]) { if (!dmPeers.has(x)) dmPeers.set(x, new Set()); dmPeers.get(x).add(y); }
}
function reindex() {
  handles.clear(); byEmail.clear(); invites.clear(); convMsgs.clear(); msgById.clear(); dmPeers.clear();
  for (const u of Object.values(db.users)) { handles.set(u.username, { k: 'u', id: u.id }); if (u.email) byEmail.set(u.email, u.id); }
  for (const c of Object.values(db.convs)) {
    if (c.username) handles.set(c.username, { k: 'c', id: c.id });
    if (c.invite) invites.set(c.invite, c.id);
    if (c.type === 'dm') linkPeers(c.members[0], c.members[1]);
  }
  for (const m of db.messages) addIndex(m);
}
function addIndex(m) {
  let a = convMsgs.get(m.conv);
  if (!a) { a = []; convMsgs.set(m.conv, a); }
  a.push(m); msgById.set(m.id, m);
}

let dirty = false, saving = false, timer = null;
function persist() {
  dirty = true;
  if (timer) return;
  timer = setTimeout(() => { timer = null; flush(); }, 400);
}
async function flush() {
  if (saving || !dirty) return;
  saving = true; dirty = false;
  try { await store.save(db); }
  catch (e) {
    console.error('save failed:', e.message);
    dirty = true;
    if (!timer) timer = setTimeout(() => { timer = null; flush(); }, 5000);
  }
  saving = false;
  if (dirty && !timer) persist();
}
async function shutdown() {
  clearTimeout(timer); timer = null;
  for (const id of online) if (db.users[id]) db.users[id].seen = Date.now();
  try { await store.save(db); } catch (e) { console.error('final save failed:', e.message); }
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

/* ================= helpers ================= */
class E extends Error { constructor(code, msg) { super(msg); this.code = code; } }
const json = (res, code, obj) => { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' }); res.end(JSON.stringify(obj)); };
const fail = (res, code, msg) => json(res, code, { error: msg });

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > 20000) { reject(new E(413, 'Слишком большой запрос')); req.destroy(); return; } chunks.push(c); });
    req.on('end', () => { try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}); } catch (e) { reject(new E(400, 'Некорректный запрос')); } });
    req.on('error', reject);
  });
}
function readRaw(req, limit) {
  return new Promise((resolve, reject) => {
    const len = parseInt(req.headers['content-length'] || '0', 10);
    if (len > limit) { req.resume(); return reject(new E(413, 'Файл слишком большой')); }
    const chunks = []; let size = 0;
    req.on('data', c => { size += c.length; if (size > limit) { reject(new E(413, 'Файл слишком большой')); req.destroy(); return; } chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function hashPw(pw, salt) { return crypto.scryptSync(pw, salt, 64).toString('hex'); }
function checkPw(pw, user) {
  const a = Buffer.from(hashPw(pw, user.salt), 'hex'), b = Buffer.from(user.hash, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

const hits = new Map();
function limited(req) {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const t = Date.now();
  const arr = (hits.get(ip) || []).filter(x => t - x < 600000);
  arr.push(t); hits.set(ip, arr);
  return arr.length > 40;
}
const wl = new Map();
function wlimit(u) {
  const t = Date.now();
  const a = (wl.get(u.id) || []).filter(x => t - x < 60000);
  a.push(t); wl.set(u.id, a);
  if (a.length > 150) throw new E(429, 'Слишком много действий, подождите минуту');
}

function authUser(req, url) {
  const h = req.headers.authorization || '';
  const t = h.startsWith('Bearer ') ? h.slice(7) : url.searchParams.get('token');
  const id = t && db.tokens[t];
  return (id && db.users[id]) || null;
}

const HANDLE = /^[a-z0-9_]{3,32}$/;
const cleanHandle = s => String(s || '').toLowerCase().replace(/^@/, '').trim();
const pubU = u => ({ id: u.id, username: u.username, name: u.name, bio: u.bio || '', avatar: u.avatar || null });
const selfU = u => Object.assign(pubU(u), { email: u.email || '' });

/* ---------- presence ---------- */
function seenBucket(ts) {
  if (!ts) return 'long';
  const d = Date.now() - ts;
  if (d < 3 * 864e5) return 'recently';
  if (d < 7 * 864e5) return 'week';
  if (d < 30 * 864e5) return 'month';
  return 'long';
}
function presenceOf(id) {
  const u = db.users[id];
  return { online: online.has(id), seen: online.has(id) ? null : seenBucket(u && u.seen) };
}
function announce(id) {
  const ps = dmPeers.get(id);
  if (!ps) return;
  const pr = Object.assign({ id }, presenceOf(id));
  for (const p of ps) push(p, 'presence', pr);
}
function maxRead(c, meId) {
  if (c.type === 'channel') return 0;
  let mx = 0;
  for (const id of c.members) {
    if (id === meId) continue;
    const u = db.users[id], p = u && u.read && u.read[c.id];
    if (p > mx) mx = p;
  }
  return mx;
}

/* ---------- realtime (SSE) ---------- */
const clients = new Map();
function push(userId, event, data) {
  const set = clients.get(userId);
  if (!set) return;
  for (const res of set) res.write('event: ' + event + '\ndata: ' + JSON.stringify(data) + '\n\n');
}
setInterval(() => { for (const set of clients.values()) for (const res of set) res.write(': ping\n\n'); }, 25000);

/* ---------- conversations ---------- */
function role(c, uid) {
  if (c.owner === uid) return 'owner';
  if (c.admins.includes(uid)) return 'admin';
  return c.members.includes(uid) ? 'member' : null;
}
const isAdmin = (c, uid) => { const r = role(c, uid); return r === 'owner' || r === 'admin'; };

function convView(c, meId) {
  if (c.type === 'dm') {
    const p = db.users[c.members.find(x => x !== meId)];
    return { id: c.id, type: 'dm', title: p.name, username: p.username, avatar: p.avatar || null, bio: p.bio || '', canPost: true, peerId: p.id, presence: presenceOf(p.id) };
  }
  const r = role(c, meId), adm = r === 'owner' || r === 'admin';
  return {
    id: c.id, type: c.type, title: c.title, bio: c.bio || '', username: c.username || null, avatar: c.avatar || null,
    members: c.members.length, role: r, canPost: c.type === 'channel' ? adm : !!r,
    invite: ((c.type === 'group' && r) || adm) ? c.invite : null
  };
}
function dmConv(a, b) {
  const id = 'dm_' + [a, b].sort().join('_');
  if (!db.convs[id]) { db.convs[id] = { id, type: 'dm', members: [a, b], admins: [], owner: null, created: Date.now() }; linkPeers(a, b); persist(); }
  return db.convs[id];
}
function getConv(id, me, mustBeMember = true) {
  const c = db.convs[String(id || '')];
  if (!c || (mustBeMember && !c.members.includes(me.id))) throw new E(404, 'Чат не найден');
  return c;
}
function pushConv(c) {
  const to = c.type === 'channel' ? [c.owner, ...c.admins] : c.members;
  for (const id of to) if (id) push(id, 'conv', { id: c.id });
}
function previewText(m) {
  if (m.deleted) return 'Сообщение удалено';
  switch (m.type) {
    case 'image': return '🖼 Фото';
    case 'file': return '📎 ' + ((m.media && m.media.name) || 'Файл');
    case 'voice': return '🎤 Голосовое сообщение';
    case 'circle': return '⏺ Видеосообщение';
    default: return m.text;
  }
}
function chatList(me) {
  const out = [];
  for (const c of Object.values(db.convs)) {
    if (!c.members.includes(me.id)) continue;
    const msgs = convMsgs.get(c.id) || [];
    if (c.type === 'dm' && !msgs.length) continue;
    const last = msgs[msgs.length - 1];
    const readId = (me.read || {})[c.id] || 0;
    let unread = 0;
    for (let i = msgs.length - 1; i >= 0 && msgs[i].id > readId; i--) if (msgs[i].from !== me.id && !msgs[i].deleted) unread++;
    const sender = last && db.users[last.from];
    const mr = maxRead(c, me.id);
    out.push(Object.assign(convView(c, me.id), {
      last: last ? { text: previewText(last), ts: last.ts, mine: last.from === me.id, from: sender ? sender.name : '', read: last.from === me.id && last.id <= mr } : null,
      unread, ts: last ? last.ts : c.created
    }));
  }
  return out.sort((a, b) => b.ts - a.ts);
}
function wire(m) {
  const u = db.users[m.from];
  return { id: m.id, conv: m.conv, from: m.from, u: u ? { username: u.username, name: u.name, avatar: u.avatar || null } : null,
    type: m.type, text: m.text, media: m.media, ts: m.ts, deleted: !!m.deleted };
}

async function dropMedia(id) {
  if (!id || !db.media[id]) return;
  delete db.media[id];
  try { await store.delMedia(id); } catch (e) {}
}
async function deleteConv(c) {
  for (const m of convMsgs.get(c.id) || []) { msgById.delete(m.id); if (m.media) await dropMedia(m.media.id); }
  convMsgs.delete(c.id);
  db.messages = db.messages.filter(m => m.conv !== c.id);
  if (c.username) handles.delete(c.username);
  if (c.invite) invites.delete(c.invite);
  await dropMedia(c.avatar);
  delete db.convs[c.id];
  persist();
}
function findPublic(q) {
  if (q.invite) { const id = invites.get(String(q.invite)); return id ? db.convs[id] : null; }
  if (q.handle) { const e = handles.get(cleanHandle(q.handle)); return e && e.k === 'c' ? db.convs[e.id] : null; }
  return null;
}
async function setAvatar(owner, holder, value) {
  if (value === null || value === '') { await dropMedia(holder.avatar); holder.avatar = null; return; }
  const mm = db.media[value];
  if (!mm || mm.owner !== owner.id || mm.kind !== 'avatar') throw new E(400, 'Некорректная аватарка');
  if (holder.avatar !== mm.id) { await dropMedia(holder.avatar); holder.avatar = mm.id; mm.used = true; }
}

/* ---------- uploads ---------- */
const LIMITS = { avatar: 2e6, image: 8e6, voice: 8e6, circle: 20e6, file: 20e6 };
const MIMES = {
  avatar: /^image\/(jpeg|png|webp)$/, image: /^image\/(jpeg|png|webp|gif)$/,
  voice: /^audio\/(webm|ogg|mp4|mpeg|wav|aac|x-m4a|m4a)$/, circle: /^video\/(webm|mp4|quicktime)$/
};
const TYPE_OF = { image: 'image', file: 'file', voice: 'voice', circle: 'circle' };

setInterval(async () => {
  const cut = Date.now() - 3600e3;
  for (const mm of Object.values(db.media)) {
    const ref = mm.kind === 'avatar' ? mm.used : !!mm.conv;
    if (!ref && mm.ts < cut) await dropMedia(mm.id);
  }
  persist();
}, 1800e3);

/* ================= email codes ================= */
const EMAIL_RE = /^[^\s@]{1,64}@[^\s@]{1,190}\.[^\s@]{2,}$/;
const cleanEmail = s => String(s || '').trim().toLowerCase().slice(0, 254);
const ipOf = req => (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
const rates = new Map();
function rate(key, max, windowMs) {
  const t = Date.now();
  const a = (rates.get(key) || []).filter(x => t - x < windowMs);
  a.push(t); rates.set(key, a);
  return a.length > max;
}
const mailConfigured = () => !!((process.env.BREVO_API_KEY || process.env.RESEND_API_KEY) && process.env.MAIL_FROM);

async function sendMail(to, subject, text, html) {
  const from = process.env.MAIL_FROM, name = process.env.MAIL_FROM_NAME || 'Orbita';
  if (process.env.BREVO_API_KEY && from) {
    const r = await fetch('https://api.brevo.com/v3/smtp/email', {
      method: 'POST',
      headers: { 'api-key': process.env.BREVO_API_KEY, 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify({ sender: { name, email: from }, to: [{ email: to }], subject, textContent: text, htmlContent: html })
    });
    if (!r.ok) throw new Error('brevo ' + r.status + ' ' + (await r.text()).slice(0, 200));
    return;
  }
  if (process.env.RESEND_API_KEY && from) {
    const r = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + process.env.RESEND_API_KEY, 'content-type': 'application/json' },
      body: JSON.stringify({ from: name + ' <' + from + '>', to: [to], subject, text, html })
    });
    if (!r.ok) throw new Error('resend ' + r.status + ' ' + (await r.text()).slice(0, 200));
    return;
  }
  throw new Error('mail not configured');
}
function codeMail(code, purpose) {
  const what = purpose === 'reset' ? 'восстановления пароля' : 'регистрации';
  const text = 'Ваш код для ' + what + ' в Orbita: ' + code + '\n\nКод действует 10 минут. Если вы его не запрашивали, просто проигнорируйте это письмо.';
  const html = '<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:420px;margin:auto;padding:24px;color:#1f2937">'
    + '<h2 style="margin:0 0 8px">Orbita</h2><p style="margin:0 0 16px;color:#64748b">Код для ' + what + '</p>'
    + '<div style="font-size:34px;letter-spacing:8px;font-weight:700;background:#eaf2ff;border-radius:16px;padding:16px;text-align:center;color:#2b6bea">' + code + '</div>'
    + '<p style="margin:16px 0 0;color:#64748b;font-size:13px">Код действует 10 минут. Если вы его не запрашивали, просто проигнорируйте это письмо.</p></div>';
  return { subject: 'Код Orbita: ' + code, text, html };
}
setInterval(() => {
  const t = Date.now();
  for (const [k, v] of Object.entries(db.pending)) if (v.exp < t) delete db.pending[k];
  for (const [k, v] of Object.entries(db.vtokens)) if (v.exp < t) delete db.vtokens[k];
  persist();
}, 3600e3);

/* ================= routes ================= */
async function api(req, res, url) {
  const route = req.method + ' ' + url.pathname;
  const q = url.searchParams;

  if (route === 'POST /api/auth/code') {
    const b = await readBody(req);
    const email = cleanEmail(b.email);
    const purpose = b.purpose === 'reset' ? 'reset' : 'register';
    if (!EMAIL_RE.test(email)) throw new E(400, 'Введите корректную почту');
    if (rate('ipc:' + ipOf(req), 20, 3600e3)) throw new E(429, 'Слишком много запросов, попробуйте позже');
    const exists = byEmail.has(email);
    if (purpose === 'register' && exists) throw new E(409, 'Эта почта уже зарегистрирована. Войдите в аккаунт.');
    if (purpose === 'reset' && !exists) return json(res, 200, { ok: true, resendIn: 60 });
    const old = db.pending[email];
    if (old && Date.now() - old.sentAt < 60000) throw new E(429, 'Код уже отправлен. Повторить можно через ' + Math.ceil((60000 - (Date.now() - old.sentAt)) / 1000) + ' с');
    if (rate('mail:' + email, 5, 3600e3)) throw new E(429, 'Слишком много писем на эту почту, попробуйте через час');
    const devLog = process.env.DEV_MAIL_LOG === '1';
    if (!mailConfigured() && !devLog) throw new E(503, 'Отправка писем ещё не настроена на сервере');
    const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
    if (mailConfigured()) {
      const m = codeMail(code, purpose);
      try { await sendMail(email, m.subject, m.text, m.html); }
      catch (e) { console.error('mail failed:', e.message); throw new E(502, 'Не удалось отправить письмо, попробуйте позже'); }
    } else console.log('[mail:dev] to=' + email + ' code=' + code);
    const salt = rnd(8);
    db.pending[email] = { salt, h: sha(salt + code), exp: Date.now() + 600e3, tries: 0, sentAt: Date.now(), purpose };
    persist();
    return json(res, 200, { ok: true, resendIn: 60 });
  }

  if (route === 'POST /api/auth/verify') {
    const b = await readBody(req);
    const email = cleanEmail(b.email), code = String(b.code || '').replace(/\D/g, '');
    if (rate('ipv:' + ipOf(req), 60, 3600e3)) throw new E(429, 'Слишком много попыток, попробуйте позже');
    const p = db.pending[email];
    if (!p || p.exp < Date.now()) throw new E(400, 'Код истёк или не запрашивался. Запросите новый.');
    p.tries++;
    const okCode = code.length === 6 && crypto.timingSafeEqual(Buffer.from(sha(p.salt + code), 'hex'), Buffer.from(p.h, 'hex'));
    if (!okCode) {
      if (p.tries >= 5) { delete db.pending[email]; persist(); throw new E(429, 'Слишком много неверных попыток. Запросите новый код.'); }
      persist();
      throw new E(400, 'Неверный код. Осталось попыток: ' + (5 - p.tries));
    }
    delete db.pending[email];
    const vtoken = rnd(24);
    db.vtokens[vtoken] = { email, purpose: p.purpose, exp: Date.now() + 1800e3 };
    persist();
    return json(res, 200, { vtoken });
  }

  if (route === 'POST /api/register') {
    if (limited(req)) throw new E(429, 'Слишком много попыток, подождите');
    const b = await readBody(req);
    const vt = db.vtokens[String(b.vtoken || '')];
    if (!vt || vt.purpose !== 'register' || vt.exp < Date.now()) throw new E(400, 'Подтверждение почты истекло. Начните регистрацию заново.');
    const username = cleanHandle(b.username);
    const name = String(b.name || '').trim().slice(0, 40);
    const password = String(b.password || '');
    if (!name) throw new E(400, 'Введите имя');
    if (!HANDLE.test(username)) throw new E(400, 'Юзернейм: 3-32 символа, латиница, цифры и _');
    if (password.length < 6) throw new E(400, 'Пароль минимум 6 символов');
    if (handles.has(username)) throw new E(409, 'Этот юзернейм уже занят');
    if (byEmail.has(vt.email)) throw new E(409, 'Эта почта уже зарегистрирована');
    const id = rnd(8), salt = rnd(16);
    db.users[id] = { id, username, email: vt.email, name, bio: '', avatar: null, salt, hash: hashPw(password, salt), created: Date.now(), seen: Date.now(), read: {} };
    handles.set(username, { k: 'u', id }); byEmail.set(vt.email, id);
    delete db.vtokens[b.vtoken];
    const token = rnd(32); db.tokens[token] = id; persist();
    return json(res, 200, { token, user: selfU(db.users[id]) });
  }

  if (route === 'POST /api/login') {
    if (limited(req)) throw new E(429, 'Слишком много попыток, подождите');
    const b = await readBody(req);
    const idf = String(b.login || b.username || '').trim().toLowerCase();
    let u = null;
    if (idf.includes('@') && !idf.startsWith('@')) { const id = byEmail.get(idf); u = id && db.users[id]; }
    else { const e = handles.get(idf.replace(/^@/, '')); u = e && e.k === 'u' && db.users[e.id]; }
    if (!u || !checkPw(String(b.password || ''), u)) throw new E(401, 'Неверная почта, юзернейм или пароль');
    const token = rnd(32); db.tokens[token] = u.id; persist();
    return json(res, 200, { token, user: selfU(u) });
  }

  if (route === 'POST /api/password/reset') {
    if (limited(req)) throw new E(429, 'Слишком много попыток, подождите');
    const b = await readBody(req);
    const vt = db.vtokens[String(b.vtoken || '')];
    if (!vt || vt.purpose !== 'reset' || vt.exp < Date.now()) throw new E(400, 'Подтверждение почты истекло. Начните заново.');
    const password = String(b.password || '');
    if (password.length < 6) throw new E(400, 'Пароль минимум 6 символов');
    const u = db.users[byEmail.get(vt.email)];
    if (!u) throw new E(404, 'Аккаунт не найден');
    u.salt = rnd(16); u.hash = hashPw(password, u.salt);
    for (const [t, id] of Object.entries(db.tokens)) if (id === u.id) delete db.tokens[t];
    delete db.vtokens[b.vtoken];
    const token = rnd(32); db.tokens[token] = u.id; persist();
    return json(res, 200, { token, user: selfU(u) });
  }

  // media GET (avatars are public, everything else needs access)
  if (req.method === 'GET' && url.pathname.startsWith('/api/media/')) {
    const id = url.pathname.slice('/api/media/'.length);
    if (!/^[a-f0-9]{24}$/.test(id)) throw new E(404, 'Не найдено');
    const mm = db.media[id];
    if (!mm) throw new E(404, 'Не найдено');
    if (mm.kind !== 'avatar') {
      const u = authUser(req, url);
      if (!u) throw new E(401, 'Нужно войти');
      const c = mm.conv && db.convs[mm.conv];
      if (mm.owner !== u.id && !(c && c.members.includes(u.id))) throw new E(403, 'Нет доступа');
    }
    const buf = await store.getMedia(id);
    if (!buf) throw new E(404, 'Не найдено');
    const inline = mm.kind !== 'file';
    const h = {
      'Content-Type': inline ? mm.mime : 'application/octet-stream',
      'Content-Length': buf.length,
      'Cache-Control': (mm.kind === 'avatar' ? 'public' : 'private') + ', max-age=31536000, immutable',
      'Content-Security-Policy': "default-src 'none'; sandbox",
      'X-Content-Type-Options': 'nosniff'
    };
    if (!inline) h['Content-Disposition'] = "attachment; filename*=UTF-8''" + encodeURIComponent(mm.name);
    res.writeHead(200, h);
    return res.end(buf);
  }

  const me = authUser(req, url);
  if (!me) throw new E(401, 'Нужно войти');
  if (req.method === 'POST') wlimit(me);

  if (route === 'GET /api/me') return json(res, 200, { user: selfU(me) });

  if (route === 'POST /api/logout') {
    const h = req.headers.authorization || '';
    delete db.tokens[h.slice(7)]; persist();
    return json(res, 200, { ok: true });
  }

  if (route === 'GET /api/stream') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
    res.write(': ok\n\n');
    if (!clients.has(me.id)) clients.set(me.id, new Set());
    clients.get(me.id).add(res);
    const grace = offTimers.get(me.id);
    if (grace) { clearTimeout(grace); offTimers.delete(me.id); }
    if (!online.has(me.id)) { online.add(me.id); announce(me.id); }
    req.on('close', () => {
      const s = clients.get(me.id);
      if (s) { s.delete(res); if (!s.size) clients.delete(me.id); }
      if (!clients.has(me.id) && !offTimers.has(me.id)) {
        // short grace period so page reloads and network blips do not flicker "offline"
        offTimers.set(me.id, setTimeout(() => {
          offTimers.delete(me.id);
          if (clients.has(me.id)) return;
          online.delete(me.id); me.seen = Date.now(); persist(); announce(me.id);
        }, 8000));
      }
    });
    return;
  }

  /* ----- profile ----- */
  if (route === 'POST /api/profile') {
    const b = await readBody(req);
    let name = me.name, bio = me.bio || '', username = me.username;
    if (b.name !== undefined) { name = String(b.name).trim().slice(0, 40); if (!name) throw new E(400, 'Имя не может быть пустым'); }
    if (b.bio !== undefined) bio = String(b.bio).trim().slice(0, 200);
    if (b.username !== undefined) {
      username = cleanHandle(b.username);
      if (username !== me.username) {
        if (!HANDLE.test(username)) throw new E(400, 'Юзернейм: 3-32 символа, латиница, цифры и _');
        if (handles.has(username)) throw new E(409, 'Этот юзернейм уже занят');
      }
    }
    if (b.avatar !== undefined) await setAvatar(me, me, b.avatar);
    if (username !== me.username) { handles.delete(me.username); handles.set(username, { k: 'u', id: me.id }); me.username = username; }
    me.name = name; me.bio = bio;
    persist();
    return json(res, 200, { user: selfU(me) });
  }

  /* ----- search ----- */
  if (route === 'GET /api/search') {
    const s = cleanHandle(q.get('q'));
    if (!s) return json(res, 200, { users: [], chats: [] });
    const users = Object.values(db.users)
      .filter(u => u.id !== me.id && (u.username.includes(s) || u.name.toLowerCase().includes(s)))
      .sort((a, b) => (b.username.startsWith(s) - a.username.startsWith(s)) || a.username.localeCompare(b.username))
      .slice(0, 15).map(pubU);
    const chats = Object.values(db.convs)
      .filter(c => c.type !== 'dm' && c.username && (c.username.includes(s) || c.title.toLowerCase().includes(s)))
      .slice(0, 15)
      .map(c => ({ id: c.id, type: c.type, title: c.title, username: c.username, avatar: c.avatar || null, members: c.members.length, joined: c.members.includes(me.id) }));
    return json(res, 200, { users, chats });
  }

  /* ----- chats ----- */
  if (route === 'GET /api/chats') return json(res, 200, { chats: chatList(me) });

  if (route === 'GET /api/chats/info') return json(res, 200, { chat: convView(getConv(q.get('conv'), me), me.id) });

  if (route === 'POST /api/dm') {
    const b = await readBody(req);
    const e = handles.get(cleanHandle(b.username));
    if (!e || e.k !== 'u') throw new E(404, 'Пользователь не найден');
    if (e.id === me.id) throw new E(400, 'Нельзя написать самому себе');
    return json(res, 200, { chat: convView(dmConv(me.id, e.id), me.id) });
  }

  if (route === 'POST /api/chats/create') {
    const b = await readBody(req);
    const type = b.type === 'channel' ? 'channel' : 'group';
    const title = String(b.title || '').trim().slice(0, 60);
    if (!title) throw new E(400, 'Введите название');
    const bio = String(b.bio || '').trim().slice(0, 200);
    let username = null;
    if (b.username) {
      username = cleanHandle(b.username);
      if (!HANDLE.test(username)) throw new E(400, 'Юзернейм: 3-32 символа, латиница, цифры и _');
      if (handles.has(username)) throw new E(409, 'Этот юзернейм уже занят');
    }
    const id = rnd(8), invite = rnd(9);
    const c = { id, type, title, bio, username, avatar: null, invite, owner: me.id, admins: [], members: [me.id], created: Date.now() };
    if (type === 'group' && Array.isArray(b.members)) {
      for (const n of b.members.slice(0, 50)) {
        const e = handles.get(cleanHandle(n));
        if (e && e.k === 'u' && !c.members.includes(e.id)) c.members.push(e.id);
      }
    }
    db.convs[id] = c; invites.set(invite, id);
    if (username) handles.set(username, { k: 'c', id });
    persist();
    for (const uid of c.members) if (uid !== me.id) push(uid, 'conv', { id });
    return json(res, 200, { chat: convView(c, me.id) });
  }

  if (route === 'GET /api/chats/members') {
    const c = getConv(q.get('conv'), me);
    if (c.type === 'dm') throw new E(400, 'Это личный чат');
    let ids = c.members;
    if (c.type === 'channel' && !isAdmin(c, me.id)) ids = ids.filter(id => role(c, id) !== 'member');
    return json(res, 200, { members: ids.map(id => Object.assign(pubU(db.users[id]), { role: role(c, id) })), total: c.members.length });
  }

  if (route === 'POST /api/chats/update') {
    const b = await readBody(req);
    const c = getConv(b.conv, me);
    if (c.type === 'dm' || !isAdmin(c, me.id)) throw new E(403, 'Недостаточно прав');
    let title = c.title, bio = c.bio || '', username = c.username || null;
    if (b.title !== undefined) { title = String(b.title).trim().slice(0, 60); if (!title) throw new E(400, 'Введите название'); }
    if (b.bio !== undefined) bio = String(b.bio).trim().slice(0, 200);
    if (b.username !== undefined) {
      const v = cleanHandle(b.username);
      if (!v) username = null;
      else {
        if (!HANDLE.test(v)) throw new E(400, 'Юзернейм: 3-32 символа, латиница, цифры и _');
        const e = handles.get(v);
        if (e && !(e.k === 'c' && e.id === c.id)) throw new E(409, 'Этот юзернейм уже занят');
        username = v;
      }
    }
    if (b.avatar !== undefined) await setAvatar(me, c, b.avatar);
    if (username !== (c.username || null)) {
      if (c.username) handles.delete(c.username);
      if (username) handles.set(username, { k: 'c', id: c.id });
      c.username = username;
    }
    c.title = title; c.bio = bio;
    persist(); pushConv(c);
    return json(res, 200, { chat: convView(c, me.id) });
  }

  if (route === 'POST /api/chats/invite/reset') {
    const b = await readBody(req);
    const c = getConv(b.conv, me);
    if (c.type === 'dm' || !isAdmin(c, me.id)) throw new E(403, 'Недостаточно прав');
    invites.delete(c.invite); c.invite = rnd(9); invites.set(c.invite, c.id); persist();
    return json(res, 200, { chat: convView(c, me.id) });
  }

  if (route === 'POST /api/chats/add') {
    const b = await readBody(req);
    const c = getConv(b.conv, me);
    if (c.type !== 'group') throw new E(400, 'Добавлять участников можно только в группу');
    if (!isAdmin(c, me.id)) throw new E(403, 'Недостаточно прав');
    const e = handles.get(cleanHandle(b.username));
    if (!e || e.k !== 'u') throw new E(404, 'Пользователь не найден');
    if (!c.members.includes(e.id)) { c.members.push(e.id); persist(); push(e.id, 'conv', { id: c.id }); pushConv(c); }
    return json(res, 200, { ok: true });
  }

  if (route === 'POST /api/chats/remove') {
    const b = await readBody(req);
    const c = getConv(b.conv, me);
    if (c.type === 'dm' || !isAdmin(c, me.id)) throw new E(403, 'Недостаточно прав');
    const e = handles.get(cleanHandle(b.username));
    if (!e || e.k !== 'u' || !c.members.includes(e.id)) throw new E(404, 'Участник не найден');
    const tr = role(c, e.id);
    if (tr === 'owner' || (tr === 'admin' && c.owner !== me.id)) throw new E(403, 'Недостаточно прав');
    c.members = c.members.filter(x => x !== e.id); c.admins = c.admins.filter(x => x !== e.id);
    persist(); push(e.id, 'gone', { conv: c.id }); pushConv(c);
    return json(res, 200, { ok: true });
  }

  if (route === 'POST /api/chats/admin') {
    const b = await readBody(req);
    const c = getConv(b.conv, me);
    if (c.type === 'dm' || c.owner !== me.id) throw new E(403, 'Только владелец может назначать администраторов');
    const e = handles.get(cleanHandle(b.username));
    if (!e || e.k !== 'u' || !c.members.includes(e.id) || e.id === c.owner) throw new E(404, 'Участник не найден');
    c.admins = c.admins.filter(x => x !== e.id);
    if (b.admin) c.admins.push(e.id);
    persist(); push(e.id, 'conv', { id: c.id }); pushConv(c);
    return json(res, 200, { ok: true });
  }

  if (route === 'POST /api/chats/leave') {
    const b = await readBody(req);
    const c = getConv(b.conv, me);
    if (c.type === 'dm') throw new E(400, 'Это личный чат');
    c.members = c.members.filter(x => x !== me.id); c.admins = c.admins.filter(x => x !== me.id);
    if (!c.members.length) await deleteConv(c);
    else {
      if (c.owner === me.id) { c.owner = c.admins[0] || c.members[0]; c.admins = c.admins.filter(x => x !== c.owner); }
      persist(); pushConv(c);
    }
    push(me.id, 'gone', { conv: c.id });
    return json(res, 200, { ok: true });
  }

  if (route === 'POST /api/chats/delete') {
    const b = await readBody(req);
    const c = getConv(b.conv, me);
    if (c.type === 'dm' || c.owner !== me.id) throw new E(403, 'Только владелец может удалить чат');
    const members = c.members.slice();
    await deleteConv(c);
    for (const id of members) push(id, 'gone', { conv: c.id });
    return json(res, 200, { ok: true });
  }

  if (route === 'GET /api/join/preview') {
    const c = findPublic({ invite: q.get('invite'), handle: q.get('handle') });
    if (!c) throw new E(404, 'Чат не найден или ссылка недействительна');
    return json(res, 200, {
      chat: { id: c.id, type: c.type, title: c.title, bio: c.bio || '', avatar: c.avatar || null, members: c.members.length, username: c.username || null },
      joined: c.members.includes(me.id)
    });
  }

  if (route === 'POST /api/join') {
    const b = await readBody(req);
    const c = findPublic(b);
    if (!c) throw new E(404, 'Чат не найден или ссылка недействительна');
    if (!c.members.includes(me.id)) {
      if (c.members.length >= 5000) throw new E(400, 'Чат заполнен');
      c.members.push(me.id); persist();
      for (const id of [c.owner, ...c.admins]) if (id && id !== me.id) push(id, 'conv', { id: c.id });
    }
    return json(res, 200, { chat: convView(c, me.id) });
  }

  /* ----- messages ----- */
  if (route === 'GET /api/messages') {
    const c = getConv(q.get('conv'), me);
    const list = (convMsgs.get(c.id) || []).slice(-200).map(wire);
    return json(res, 200, { chat: convView(c, me.id), messages: list, peerRead: maxRead(c, me.id) });
  }

  if (route === 'POST /api/read') {
    const b = await readBody(req);
    const c = getConv(b.conv, me);
    me.read = me.read || {};
    const prev = me.read[c.id] || 0;
    me.read[c.id] = db.seq; persist();
    if (db.seq > prev && c.type !== 'channel') {
      for (const id of c.members) if (id !== me.id) push(id, 'read', { conv: c.id, by: me.id, upTo: db.seq });
    }
    return json(res, 200, { ok: true });
  }

  if (route === 'POST /api/typing') {
    const b = await readBody(req);
    const c = getConv(b.conv, me);
    if (c.type === 'channel') return json(res, 200, { ok: true });
    const a = (b.a === 'voice' || b.a === 'circle') ? b.a : 'text';
    for (const id of c.members) if (id !== me.id) push(id, 'typing', { conv: c.id, from: me.id, name: me.name, a });
    return json(res, 200, { ok: true });
  }

  if (route === 'POST /api/send') {
    const b = await readBody(req);
    const c = getConv(b.conv, me);
    if (!(c.type === 'channel' ? isAdmin(c, me.id) : true)) throw new E(403, 'В этом канале публикуют только администраторы');
    const text = String(b.text || '').trim().slice(0, 4000);
    let type = 'text', media = null;
    if (b.media) {
      const mm = db.media[b.media];
      if (!mm || mm.owner !== me.id || mm.conv || mm.kind === 'avatar') throw new E(400, 'Некорректное вложение');
      mm.conv = c.id; type = TYPE_OF[mm.kind];
      media = { id: mm.id, mime: mm.mime, name: mm.name, size: mm.size, dur: mm.dur };
    } else if (!text) throw new E(400, 'Пустое сообщение');
    const m = { id: ++db.seq, conv: c.id, from: me.id, type, text, media, ts: Date.now() };
    db.messages.push(m); addIndex(m); persist();
    const w = wire(m);
    for (const uid of c.members) push(uid, 'msg', w);
    return json(res, 200, { message: w });
  }

  if (route === 'POST /api/messages/delete') {
    const b = await readBody(req);
    const m = msgById.get(Number(b.id));
    const c = m && db.convs[m.conv];
    if (!m || m.deleted || !c || !c.members.includes(me.id)) throw new E(404, 'Сообщение не найдено');
    if (m.from !== me.id && !isAdmin(c, me.id)) throw new E(403, 'Нельзя удалить чужое сообщение');
    m.deleted = true; m.text = '';
    if (m.media) { await dropMedia(m.media.id); m.media = null; }
    persist();
    for (const uid of c.members) push(uid, 'del', { conv: c.id, id: m.id });
    return json(res, 200, { ok: true });
  }

  /* ----- upload ----- */
  if (route === 'POST /api/upload') {
    const kind = q.get('kind');
    const lim = LIMITS[kind];
    if (!lim) throw new E(400, 'Неизвестный тип вложения');
    const mime = (req.headers['content-type'] || 'application/octet-stream').split(';')[0].trim().toLowerCase();
    if (MIMES[kind] && !MIMES[kind].test(mime)) throw new E(415, 'Неподдерживаемый формат файла');
    const buf = await readRaw(req, lim);
    if (!buf.length) throw new E(400, 'Пустой файл');
    const id = rnd(12);
    const name = String(q.get('name') || 'file').replace(/[\\/\0\r\n"]/g, '_').slice(0, 100) || 'file';
    const dur = Math.max(0, Math.min(3600, parseInt(q.get('dur'), 10) || 0));
    await store.putMedia(id, buf);
    db.media[id] = { id, owner: me.id, kind, mime: kind === 'file' ? 'application/octet-stream' : mime, name, size: buf.length, dur, conv: null, used: false, ts: Date.now() };
    persist();
    return json(res, 200, { media: { id, mime: db.media[id].mime, name, size: buf.length, dur } });
  }

  throw new E(404, 'Не найдено');
}

/* ================= http server ================= */
const CSP = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; media-src 'self' blob:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'";

const server = http.createServer(async (req, res) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  const url = new URL(req.url, 'http://x');
  try {
    if (url.pathname.startsWith('/api/')) return await api(req, res, url);
    if (url.pathname === '/healthz') { res.writeHead(200); return res.end('ok'); }
    if (url.pathname === '/' || url.pathname === '/index.html') {
      return fs.readFile(path.join(__dirname, 'index.html'), (err, buf) => {
        if (err) { res.writeHead(500); return res.end('index.html not found'); }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Content-Security-Policy': CSP, 'Cache-Control': 'no-cache' });
        res.end(buf);
      });
    }
    res.writeHead(404); res.end('Not found');
  } catch (e) {
    if (res.headersSent) { try { res.end(); } catch (x) {} return; }
    if (e instanceof E) return fail(res, e.code, e.message);
    console.error(e);
    fail(res, 500, 'Ошибка сервера');
  }
});

(async () => {
  const d = await store.load();
  if (d && d.v === 2) db = Object.assign(db, d);
  reindex();
  server.listen(PORT, () => console.log('Orbita running on port ' + PORT + ' (storage: ' + store.kind + ')'));
})().catch(e => { console.error('startup failed:', e); process.exit(1); });
