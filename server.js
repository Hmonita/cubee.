// CUBEE.IO — игровой сервер (без внешних зависимостей, нужен только Node.js 16+)
// Запуск:  node server.js        (порт: PORT=3000 node server.js)
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = +process.env.PORT || 3000;
const MAX_PLAYERS = 10;
const SERVER_NAME = 'Амстердам';
const TICK_MS = 50; // 20 раз в секунду
const PUBLIC = path.join(__dirname, 'public');
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.ico': 'image/x-icon', '.svg': 'image/svg+xml' };

const players = new Map(); // id -> player
let nextId = 1;

/* ---------- HTTP ---------- */
const httpServer = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/api/status') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ name: SERVER_NAME, players: players.size, max: MAX_PLAYERS }));
  }
  let file = path.normalize(path.join(PUBLIC, url === '/' ? 'index.html' : url));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
});

/* ---------- Минимальный WebSocket (RFC 6455) ---------- */
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

httpServer.on('upgrade', (req, socket) => {
  if (req.url.split('?')[0] !== '/ws' || !req.headers['sec-websocket-key']) return socket.destroy();
  const accept = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const conn = { socket, buf: Buffer.alloc(0), player: null, alive: true };
  socket.on('data', (chunk) => { conn.buf = Buffer.concat([conn.buf, chunk]); parseFrames(conn); });
  socket.on('close', () => drop(conn));
  socket.on('error', () => drop(conn));
});

function sendRaw(conn, opcode, payload) {
  if (!conn.alive || conn.socket.destroyed) return;
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  try { conn.socket.write(Buffer.concat([head, payload])); } catch (e) {}
}
const send = (conn, obj) => sendRaw(conn, 1, Buffer.from(JSON.stringify(obj)));

function parseFrames(conn) {
  for (;;) {
    const b = conn.buf;
    if (b.length < 2) return;
    const opcode = b[0] & 0x0f, masked = !!(b[1] & 0x80);
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    if (len > 16384) return conn.socket.destroy(); // защита от больших пакетов
    if (!masked) return conn.socket.destroy();
    if (b.length < off + 4 + len) return;
    const mask = b.slice(off, off + 4);
    const data = Buffer.from(b.slice(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
    conn.buf = b.slice(off + 4 + len);
    if (opcode === 8) { sendRaw(conn, 8, Buffer.alloc(0)); return conn.socket.end(); }
    if (opcode === 9) { sendRaw(conn, 10, data); continue; }
    if (opcode === 1) { try { onMessage(conn, JSON.parse(data.toString('utf8'))); } catch (e) {} }
  }
}

/* ---------- Игровая логика ---------- */
const clean = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, '').slice(0, n);
const num = (v, d) => (typeof v === 'number' && isFinite(v) ? v : d);

function onMessage(conn, m) {
  if (!m || typeof m !== 'object') return;
  if (m.t === 'join' && !conn.player) {
    if (players.size >= MAX_PLAYERS) { send(conn, { t: 'full' }); return conn.socket.end(); }
    const p = { id: nextId++, conn, n: clean(m.n, 14) || 'ИГРОК', x: 0, y: 0, ha: 0, hd: -1, hp: 100, g: 0, sw: 0, lastChat: 0 };
    conn.player = p;
    players.set(p.id, p);
    send(conn, { t: 'welcome', id: p.id, name: SERVER_NAME, max: MAX_PLAYERS });
    broadcast({ t: 'sys', m: p.n + ' зашёл на сервер' });
    console.log(`[+] ${p.n} (${players.size}/${MAX_PLAYERS})`);
  } else if (m.t === 'state' && conn.player) {
    const p = conn.player;
    p.x = num(m.x, p.x); p.y = num(m.y, p.y); p.ha = num(m.ha, p.ha);
    p.hd = num(m.hd, -1) | 0; p.hp = Math.max(0, Math.min(100, num(m.hp, 100)));
    p.g = Math.max(0, Math.min(1e9, num(m.g, 0) | 0)); p.sw = m.sw ? 1 : 0;
  } else if (m.t === 'chat' && conn.player) {
    const p = conn.player, now = Date.now();
    if (now - p.lastChat < 700) return; // анти-спам
    p.lastChat = now;
    const text = clean(m.m, 100).trim();
    if (text) broadcast({ t: 'chat', n: p.n, m: text }, p);
  }
}

function drop(conn) {
  if (!conn.alive) return;
  conn.alive = false;
  const p = conn.player;
  if (p && players.delete(p.id)) {
    broadcast({ t: 'sys', m: p.n + ' вышел' });
    console.log(`[-] ${p.n} (${players.size}/${MAX_PLAYERS})`);
  }
}

function broadcast(obj, except) {
  const data = Buffer.from(JSON.stringify(obj));
  for (const p of players.values()) if (p !== except) sendRaw(p.conn, 1, data);
}

setInterval(() => {
  if (!players.size) return;
  const list = [];
  for (const p of players.values())
    list.push({ i: p.id, n: p.n, x: +p.x.toFixed(3), y: +p.y.toFixed(3), ha: +p.ha.toFixed(2), hd: p.hd, hp: p.hp, g: p.g, sw: p.sw });
  broadcast({ t: 'snap', p: list });
}, TICK_MS);

httpServer.listen(PORT, () => console.log(`CUBEE.IO сервер «${SERVER_NAME}» запущен: http://localhost:${PORT}  (макс. игроков: ${MAX_PLAYERS})`));
