// Orbit messenger server. Zero dependencies: only Node.js built-ins (Node 18+).
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA = process.env.DATA_FILE || path.join(__dirname, 'data.json');

let db = { users: {}, messages: [], tokens: {}, seq: 0 };
try { db = JSON.parse(fs.readFileSync(DATA, 'utf8')); } catch (e) {}

let timer = null;
function flush() {
  const tmp = DATA + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(db));
  fs.renameSync(tmp, DATA);
}
function persist() { clearTimeout(timer); timer = setTimeout(flush, 300); }
process.on('SIGTERM', () => { flush(); process.exit(0); });
process.on('SIGINT', () => { flush(); process.exit(0); });

// ---------- helpers ----------
const json = (res, code, obj) => {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
};
const fail = (res, code, msg) => json(res, code, { error: msg });

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0, chunks = [];
    req.on('data', c => {
      size += c.length;
      if (size > 20000) { reject(new Error('too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {}); }
      catch (e) { reject(new Error('bad json')); }
    });
    req.on('error', reject);
  });
}

function hashPw(pw, salt) { return crypto.scryptSync(pw, salt, 64).toString('hex'); }
function checkPw(pw, user) {
  const a = Buffer.from(hashPw(pw, user.salt), 'hex');
  const b = Buffer.from(user.hash, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// naive per-IP rate limit for auth endpoints
const hits = new Map();
function limited(req) {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const now = Date.now();
  const arr = (hits.get(ip) || []).filter(t => now - t < 10 * 60 * 1000);
  arr.push(now);
  hits.set(ip, arr);
  return arr.length > 30;
}

function authUser(req, url) {
  const h = req.headers.authorization || '';
  const token = h.startsWith('Bearer ') ? h.slice(7) : url.searchParams.get('token');
  const name = token && db.tokens[token];
  return name && db.users[name] ? db.users[name] : null;
}

const publicUser = u => ({ username: u.username, name: u.name });

// ---------- realtime (SSE) ----------
const clients = new Map(); // username -> Set(res)
function push(username, event, data) {
  const set = clients.get(username);
  if (!set) return;
  for (const res of set) res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}
setInterval(() => {
  for (const set of clients.values()) for (const res of set) res.write(': ping\n\n');
}, 25000);

// ---------- chats ----------
function chatList(me) {
  const map = new Map();
  for (const m of db.messages) {
    if (m.from !== me.username && m.to !== me.username) continue;
    const peer = m.from === me.username ? m.to : m.from;
    let c = map.get(peer);
    if (!c) { c = { peer, last: m, unread: 0 }; map.set(peer, c); }
    c.last = m;
    if (m.from === peer && m.id > ((me.read || {})[peer] || 0)) c.unread++;
  }
  return [...map.values()]
    .map(c => ({
      username: c.peer,
      name: (db.users[c.peer] || { name: c.peer }).name,
      last: { text: c.last.text, ts: c.last.ts, mine: c.last.from === me.username },
      unread: c.unread
    }))
    .sort((a, b) => b.last.ts - a.last.ts);
}

// ---------- routes ----------
async function api(req, res, url) {
  const route = req.method + ' ' + url.pathname;

  if (route === 'POST /api/register') {
    if (limited(req)) return fail(res, 429, 'Слишком много попыток, подождите');
    const b = await readBody(req);
    const username = String(b.username || '').toLowerCase().replace(/^@/, '');
    const name = String(b.name || '').trim().slice(0, 40);
    const password = String(b.password || '');
    if (!/^[a-z0-9_]{3,20}$/.test(username)) return fail(res, 400, 'Юзернейм: 3-20 символов, латиница, цифры и _');
    if (!name) return fail(res, 400, 'Введите имя');
    if (password.length < 6) return fail(res, 400, 'Пароль минимум 6 символов');
    if (db.users[username]) return fail(res, 409, 'Этот юзернейм уже занят');
    const salt = crypto.randomBytes(16).toString('hex');
    db.users[username] = { username, name, salt, hash: hashPw(password, salt), created: Date.now(), read: {} };
    const token = crypto.randomBytes(32).toString('hex');
    db.tokens[token] = username;
    persist();
    return json(res, 200, { token, user: publicUser(db.users[username]) });
  }

  if (route === 'POST /api/login') {
    if (limited(req)) return fail(res, 429, 'Слишком много попыток, подождите');
    const b = await readBody(req);
    const username = String(b.username || '').toLowerCase().replace(/^@/, '');
    const u = db.users[username];
    if (!u || !checkPw(String(b.password || ''), u)) return fail(res, 401, 'Неверный юзернейм или пароль');
    const token = crypto.randomBytes(32).toString('hex');
    db.tokens[token] = username;
    persist();
    return json(res, 200, { token, user: publicUser(u) });
  }

  const me = authUser(req, url);
  if (!me) return fail(res, 401, 'Нужно войти');

  if (route === 'GET /api/me') return json(res, 200, { user: publicUser(me) });

  if (route === 'GET /api/stream') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache',
      'Connection': 'keep-alive', 'X-Accel-Buffering': 'no'
    });
    res.write(': ok\n\n');
    if (!clients.has(me.username)) clients.set(me.username, new Set());
    clients.get(me.username).add(res);
    req.on('close', () => {
      const s = clients.get(me.username);
      if (s) { s.delete(res); if (!s.size) clients.delete(me.username); }
    });
    return;
  }

  if (route === 'GET /api/users/search') {
    const q = (url.searchParams.get('q') || '').toLowerCase().replace(/^@/, '').trim();
    if (!q) return json(res, 200, { users: [] });
    const users = Object.values(db.users)
      .filter(u => u.username !== me.username && (u.username.includes(q) || u.name.toLowerCase().includes(q)))
      .sort((a, b) => (b.username.startsWith(q) - a.username.startsWith(q)) || a.username.localeCompare(b.username))
      .slice(0, 20).map(publicUser);
    return json(res, 200, { users });
  }

  if (route === 'GET /api/chats') return json(res, 200, { chats: chatList(me) });

  if (route === 'GET /api/messages') {
    const peer = (url.searchParams.get('with') || '').toLowerCase();
    const user = db.users[peer];
    if (!user) return fail(res, 404, 'Пользователь не найден');
    const list = db.messages
      .filter(m => (m.from === me.username && m.to === peer) || (m.from === peer && m.to === me.username))
      .slice(-300);
    return json(res, 200, { peer: publicUser(user), messages: list });
  }

  if (route === 'POST /api/send') {
    const b = await readBody(req);
    const to = String(b.to || '').toLowerCase();
    const text = String(b.text || '').trim().slice(0, 4000);
    if (!db.users[to] || to === me.username) return fail(res, 400, 'Получатель не найден');
    if (!text) return fail(res, 400, 'Пустое сообщение');
    const m = { id: ++db.seq, from: me.username, to, text, ts: Date.now() };
    db.messages.push(m);
    persist();
    push(to, 'msg', m);
    push(me.username, 'msg', m);
    return json(res, 200, { message: m });
  }

  if (route === 'POST /api/read') {
    const b = await readBody(req);
    const peer = String(b.with || '').toLowerCase();
    me.read = me.read || {};
    me.read[peer] = db.seq;
    persist();
    return json(res, 200, { ok: true });
  }

  if (route === 'POST /api/logout') {
    const h = req.headers.authorization || '';
    delete db.tokens[h.slice(7)];
    persist();
    return json(res, 200, { ok: true });
  }

  return fail(res, 404, 'Не найдено');
}

const server = http.createServer(async (req, res) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  const url = new URL(req.url, 'http://x');
  try {
    if (url.pathname.startsWith('/api/')) return await api(req, res, url);
    if (url.pathname === '/' || url.pathname === '/index.html') {
      return fs.readFile(path.join(__dirname, 'index.html'), (err, buf) => {
        if (err) { res.writeHead(500); return res.end('index.html not found'); }
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(buf);
      });
    }
    res.writeHead(404); res.end('Not found');
  } catch (e) {
    if (!res.headersSent) fail(res, 400, 'Некорректный запрос');
  }
});

server.listen(PORT, () => console.log('Orbit running on http://localhost:' + PORT));
