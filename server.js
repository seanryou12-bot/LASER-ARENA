// LASER ARENA 멀티플레이 서버 — 의존성 없음.  실행:  node server.js   (포트 변경: PORT=8080 node server.js)
// index.html 을 직접 서빙하고 /ws 로 WebSocket(방 관리 + 메시지 중계)을 제공한다.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), os = require('os');
const PORT = +process.env.PORT || 3000;
const MAPS = ['arena', 'factory', 'forest', 'mansion', 'city'];
const MAX_HUMANS = 12;
const PUB_TARGET = 8; // 공용방: 사람이 8명 이하이면 봇으로 총 8명까지 채움, 8명을 넘으면 봇 없음

// ---------------- 방 ----------------
const rooms = new Map(); // id -> room
let nextCid = 1;
function newRoom(id, map, mode, bots, priv) {
  const r = { id, map, mode, botsCfg: bots, priv, seed: (Math.random() * 2 ** 31) | 0, members: [] };
  rooms.set(id, r); return r;
}
for (const m of MAPS) newRoom('pub:' + m, m, m === 'city' ? 'coop' : 'ffa', -1, false); // 폐허 도시는 Co-op 전용
const botTarget = r => r.priv ? r.botsCfg : (r.mode === 'coop' ? 0 : r.members.length > PUB_TARGET ? 0 : Math.max(0, PUB_TARGET - r.members.length));
const roomInfo = r => ({ id: r.id, map: r.map, mode: r.mode, priv: r.priv, seed: r.seed, humans: r.members.length, bots: botTarget(r), host: r.members.length ? r.members[0].id : 0 });
function send(c, o) { if (c.sock && !c.sock.destroyed) c.sock.write(frame(JSON.stringify(o))); }
function broadcast(r, o, except) { const f = frame(JSON.stringify(o)); for (const m of r.members) if (m !== except && !m.sock.destroyed) m.sock.write(f); }
function leave(c) {
  const r = c.room; if (!r) return;
  c.room = null; r.members = r.members.filter(m => m !== c);
  if (!r.members.length) {
    if (r.priv) rooms.delete(r.id);
    else r.seed = (Math.random() * 2 ** 31) | 0; // 빈 공용방은 새 시드로 초기화
    return;
  }
  broadcast(r, { t: 'peer-', id: c.id });
  broadcast(r, { t: 'room', room: roomInfo(r) });
}
function join(c, r, name) {
  if (c.room) leave(c);
  if (r.members.length >= MAX_HUMANS) return send(c, { t: 'err', msg: '방이 가득 찼습니다 (최대 ' + MAX_HUMANS + '명)' });
  c.name = String(name || '').slice(0, 10) || '플레이어';
  r.members.push(c); c.room = r;
  send(c, { t: 'joined', id: c.id, room: roomInfo(r), members: r.members.map(m => ({ id: m.id, name: m.name })) });
  broadcast(r, { t: 'peer+', id: c.id, name: c.name }, c);
  broadcast(r, { t: 'room', room: roomInfo(r) });
}
function publicList() {
  return [...rooms.values()].filter(r => !r.priv).map(r => ({ id: r.id, map: r.map, mode: r.mode, humans: r.members.length, bots: botTarget(r) }));
}
function onMessage(c, m) {
  if (m.t === 'list') return send(c, { t: 'rooms', rooms: publicList() });
  if (m.t === 'join') {
    const r = rooms.get(String(m.room));
    if (!r) return send(c, { t: 'err', msg: '존재하지 않는 방입니다' });
    return join(c, r, m.name);
  }
  if (m.t === 'create') {
    let mode = m.mode === 'teams' ? 'teams' : (m.mode === 'coop' ? 'coop' : 'ffa'), map = m.map;
    if (mode === 'coop') map = 'city'; else if (map === 'city') return send(c, { t: 'err', msg: '폐허 도시는 Co-op 전용입니다' });
    if (!MAPS.includes(map)) return send(c, { t: 'err', msg: '잘못된 맵' });
    let code; do { code = String(100000 + Math.floor(Math.random() * 900000)); } while (rooms.has(code));
    const bots = Math.max(0, Math.min(7, m.bots | 0));
    const r = newRoom(code, map, mode, bots, true);
    return join(c, r, m.name);
  }
  if (m.t === 'leave') return leave(c);
  if (m.t === 'r' && c.room) {
    const out = frame(JSON.stringify({ t: 'r', from: c.id, d: m.d }));
    if (m.to) { const tg = c.room.members.find(x => x.id === m.to); if (tg && !tg.sock.destroyed) tg.sock.write(out); }
    else for (const x of c.room.members) if (x !== c && !x.sock.destroyed) x.sock.write(out);
  }
}

// ---------------- 최소 WebSocket (RFC 6455) ----------------
function frame(str, op = 1) {
  const p = Buffer.from(str), n = p.length;
  let h;
  if (n < 126) h = Buffer.from([0x80 | op, n]);
  else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, p]);
}
function attach(sock) {
  const c = { id: nextCid++, sock, room: null, name: '' };
  let buf = Buffer.alloc(0), frag = [];
  sock.on('data', d => {
    buf = Buffer.concat([buf, d]);
    for (;;) {
      if (buf.length < 2) return;
      const fin = !!(buf[0] & 0x80), op = buf[0] & 15, masked = !!(buf[1] & 0x80);
      let len = buf[1] & 127, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; len = Number(buf.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) return sock.destroy();
      const need = off + (masked ? 4 : 0) + len;
      if (buf.length < need) return;
      let payload = buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const k = buf.subarray(off, off + 4); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= k[i & 3]; }
      buf = buf.subarray(need);
      if (op === 8) { sock.end(frame('', 8)); return; }
      if (op === 9) { sock.write(frame(payload.toString(), 10)); continue; }
      if (op === 10) continue;
      frag.push(payload);
      if (fin) {
        const text = Buffer.concat(frag).toString('utf8'); frag = [];
        try { onMessage(c, JSON.parse(text)); } catch (e) { /* 잘못된 메시지 무시 */ }
      }
    }
  });
  const gone = () => leave(c);
  sock.on('close', gone); sock.on('error', gone);
  sock.setNoDelay(true);
}

// ---------------- HTTP ----------------
function gameFile() { // index.html 이 없으면 폴더의 첫 .html 파일 (예: LASER ARENA.html)
  const p = path.join(__dirname, 'index.html');
  if (fs.existsSync(p)) return p;
  const h = fs.readdirSync(__dirname).find(n => n.toLowerCase().endsWith('.html'));
  return path.join(__dirname, h || 'index.html');
}
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u.endsWith('.html')) {
    fs.readFile(gameFile(), (e, d) => {
      if (e) { res.writeHead(500); return res.end('게임 HTML 파일을 찾을 수 없습니다'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(d);
    });
  } else { res.writeHead(404); res.end('not found'); }
});
server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || req.url.split('?')[0] !== '/ws') return sock.destroy();
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  attach(sock);
});
server.listen(PORT, '0.0.0.0', () => {
  console.log('LASER ARENA 서버 시작 — 같은 네트워크의 다른 기기에서 아래 주소로 접속하세요');
  console.log('  이 컴퓨터:  http://localhost:' + PORT);
  for (const list of Object.values(os.networkInterfaces())) for (const a of list || []) if (a.family === 'IPv4' && !a.internal) console.log('  다른 기기:  http://' + a.address + ':' + PORT);
});
module.exports = { server, rooms };
