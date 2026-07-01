// ============================================================
//  큰마을 PC통신 - 통합 서버
//  회원가입/로그인 · 플라자(게시판+추천) · 자료실 · 쪽지함 · 대화방
//  저장: Upstash 있으면 클라우드 영구저장, 없으면 로컬 data.json
//  실행: npm install → node server.js   접속: http://localhost:8080
// ============================================================

const http = require("http");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const HTML_FILE = path.join(__dirname, "daehwabang.html");
const DB_FILE = "./data.json";
const DB_KEY = "pc-tongsin-db";
const MAX_HISTORY = 300, MAX_POSTS = 200, MAX_FILES = 200;

const useRedis = !!(process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN);
let redis = null;
if (useRedis) { const { Redis } = require("@upstash/redis"); redis = Redis.fromEnv(); }

let db = { users: {}, rooms: {}, board: [], notes: {}, files: [], nextNo: 1, fileNo: 1, noteNo: 1 };

async function loadDb() {
  try {
    if (useRedis) { const raw = await redis.get(DB_KEY); if (raw) db = (typeof raw === "string") ? JSON.parse(raw) : raw; }
    else if (fs.existsSync(DB_FILE)) db = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
  } catch (e) { console.error("불러오기 실패:", e); }
  db.users = db.users || {}; db.rooms = db.rooms || {}; db.board = db.board || [];
  db.notes = db.notes || {}; db.files = db.files || {};
  if (!Array.isArray(db.files)) db.files = [];
  db.nextNo = db.nextNo || (db.board.reduce((m, p) => Math.max(m, p.no), 0) + 1);
  db.fileNo = db.fileNo || (db.files.reduce((m, p) => Math.max(m, p.no), 0) + 1);
  db.noteNo = db.noteNo || 1;
}
async function save() {
  try {
    if (useRedis) await redis.set(DB_KEY, JSON.stringify(db));
    else fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
  } catch (e) { console.error("저장 실패:", e); }
}

function makeHash(pw) { const salt = crypto.randomBytes(8).toString("hex"); return { salt, hash: crypto.scryptSync(pw, salt, 32).toString("hex") }; }
function checkPw(pw, u) { const h = crypto.scryptSync(pw, u.salt, 32).toString("hex"); if (h.length !== u.hash.length) return false; return crypto.timingSafeEqual(Buffer.from(h), Buffer.from(u.hash)); }

const live = new Map(), roomSockets = new Map();
const server = http.createServer((req, res) => {
  fs.readFile(HTML_FILE, (err, data) => {
    if (err) { res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" }); return res.end("daehwabang.html not found"); }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" }); res.end(data);
  });
});
const wss = new WebSocketServer({ server });

function send(sock, o) { if (sock.readyState === sock.OPEN) sock.send(JSON.stringify(o)); }
function toRoom(code, o) { const s = roomSockets.get(code); if (s) for (const x of s) send(x, o); }
function rosterOf(code) { const s = roomSockets.get(code); return s ? [...s].map(x => live.get(x).username) : []; }
function pushRoster(code) { toRoom(code, { kind: "roster", list: rosterOf(code) }); }
function boardSummary() {
  return db.board.map(p => ({ no: p.no, nick: p.nick, title: p.title, time: p.time, hit: p.hit, rec: p.rec || 0, parent: p.parent || null })).sort((a, b) => b.no - a.no);
}
function fileSummary() {
  return db.files.map(f => ({ no: f.no, nick: f.nick, title: f.title, time: f.time, dl: f.dl || 0 })).sort((a, b) => b.no - a.no);
}
function noteSummary(user) {
  return (db.notes[user] || []).map(n => ({ id: n.id, from: n.from, time: n.time, read: n.read, preview: (n.text || "").slice(0, 20) })).sort((a, b) => b.time - a.time);
}
function leaveRoom(sock, st) {
  if (st.room && roomSockets.has(st.room)) { roomSockets.get(st.room).delete(sock); toRoom(st.room, { kind: "system", text: `'${st.username}' 님이 퇴장하셨습니다.` }); pushRoster(st.room); }
  st.room = null;
}

wss.on("connection", (sock) => {
  live.set(sock, { username: null, room: null });
  sock.on("message", (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const st = live.get(sock);

    // ── 인증 ──
    if (m.action === "signup") {
      const u = (m.username || "").trim();
      if (!u || !m.password) return send(sock, { kind: "auth-err", text: "아이디와 비밀번호를 입력하세요." });
      if (db.users[u]) return send(sock, { kind: "auth-err", text: "이미 있는 아이디입니다." });
      db.users[u] = makeHash(m.password); save();
      st.username = u; send(sock, { kind: "auth-ok", username: u });
    }
    else if (m.action === "login") {
      const u = (m.username || "").trim(); const user = db.users[u];
      if (!user || !checkPw(m.password || "", user)) return send(sock, { kind: "auth-err", text: "아이디 또는 비밀번호가 틀렸습니다." });
      st.username = u; send(sock, { kind: "auth-ok", username: u });
    }

    // ── 플라자(게시판) ──
    else if (m.action === "board-list") { send(sock, { kind: "board-list", list: boardSummary() }); }
    else if (m.action === "board-read") {
      const p = db.board.find(x => x.no === m.no);
      if (!p) return send(sock, { kind: "board-err", text: "글을 찾을 수 없습니다." });
      p.hit = (p.hit || 0) + 1; save();
      send(sock, { kind: "board-read", post: p, mine: p.nick === st.username });
    }
    else if (m.action === "board-write") {
      if (!st.username) return;
      const title = String(m.title || "").trim().slice(0, 60);
      if (!title) return send(sock, { kind: "board-err", text: "제목을 입력하세요." });
      const parent = (m.parent && db.board.some(x => x.no === m.parent)) ? m.parent : null;
      const post = { no: db.nextNo++, nick: st.username, title, body: String(m.body || "").slice(0, 4000), time: Date.now(), hit: 0, rec: 0, recBy: [], parent };
      db.board.push(post); if (db.board.length > MAX_POSTS) db.board.shift(); save();
      send(sock, { kind: "board-written", no: post.no });
    }
    else if (m.action === "board-rec") {
      if (!st.username) return;
      const p = db.board.find(x => x.no === m.no);
      if (!p) return send(sock, { kind: "board-err", text: "글을 찾을 수 없습니다." });
      p.recBy = p.recBy || [];
      if (p.recBy.includes(st.username)) return send(sock, { kind: "board-err", text: "이미 추천한 글입니다." });
      p.recBy.push(st.username); p.rec = (p.rec || 0) + 1; save();
      send(sock, { kind: "board-read", post: p, mine: p.nick === st.username });
    }

    else if (m.action === "board-edit") {
      if (!st.username) return;
      const p = db.board.find(x => x.no === m.no);
      if (!p) return send(sock, { kind: "board-err", text: "글을 찾을 수 없습니다." });
      if (p.nick !== st.username) return send(sock, { kind: "board-err", text: "내 글만 수정할 수 있습니다." });
      const title = String(m.title || "").trim().slice(0, 60);
      if (!title) return send(sock, { kind: "board-err", text: "제목을 입력하세요." });
      p.title = title; p.body = String(m.body || "").slice(0, 4000); save();
      send(sock, { kind: "board-read", post: p, mine: true });
    }
    else if (m.action === "board-delete") {
      if (!st.username) return;
      const p = db.board.find(x => x.no === m.no);
      if (!p) return send(sock, { kind: "board-err", text: "글을 찾을 수 없습니다." });
      if (p.nick !== st.username) return send(sock, { kind: "board-err", text: "내 글만 삭제할 수 있습니다." });
      db.board = db.board.filter(x => x.no !== m.no); save();
      send(sock, { kind: "board-list", list: boardSummary() });
    }

    // ── 자료실 ──
    else if (m.action === "file-list") { send(sock, { kind: "file-list", list: fileSummary() }); }
    else if (m.action === "file-read") {
      const f = db.files.find(x => x.no === m.no);
      if (!f) return send(sock, { kind: "file-err", text: "자료를 찾을 수 없습니다." });
      f.dl = (f.dl || 0) + 1; save();
      send(sock, { kind: "file-read", file: f, mine: f.nick === st.username });
    }
    else if (m.action === "file-write") {
      if (!st.username) return;
      const title = String(m.title || "").trim().slice(0, 60);
      if (!title) return send(sock, { kind: "file-err", text: "제목을 입력하세요." });
      const f = { no: db.fileNo++, nick: st.username, title, desc: String(m.desc || "").slice(0, 2000), url: String(m.url || "").slice(0, 500), time: Date.now(), dl: 0 };
      db.files.push(f); if (db.files.length > MAX_FILES) db.files.shift(); save();
      send(sock, { kind: "file-written", no: f.no });
    }
    else if (m.action === "file-delete") {
      if (!st.username) return;
      const f = db.files.find(x => x.no === m.no);
      if (!f || f.nick !== st.username) return send(sock, { kind: "file-err", text: "내 자료만 삭제할 수 있습니다." });
      db.files = db.files.filter(x => x.no !== m.no); save();
      send(sock, { kind: "file-list", list: fileSummary() });
    }

    // ── 쪽지함 ──
    else if (m.action === "note-list") { if (!st.username) return; send(sock, { kind: "note-list", list: noteSummary(st.username) }); }
    else if (m.action === "note-read") {
      if (!st.username) return;
      const box = db.notes[st.username] || []; const n = box.find(x => x.id === m.id);
      if (!n) return send(sock, { kind: "note-err", text: "쪽지를 찾을 수 없습니다." });
      n.read = true; save();
      send(sock, { kind: "note-read", note: n });
    }
    else if (m.action === "note-send") {
      if (!st.username) return;
      const to = (m.to || "").trim();
      if (!db.users[to]) return send(sock, { kind: "note-err", text: `'${to}' 님을 찾을 수 없습니다.` });
      db.notes[to] = db.notes[to] || [];
      db.notes[to].push({ id: "n" + (db.noteNo++), from: st.username, text: String(m.text || "").slice(0, 2000), time: Date.now(), read: false });
      save();
      // 상대가 접속 중이면 실시간 알림
      for (const [s, stt] of live) if (stt.username === to) send(s, { kind: "note-ping", from: st.username });
      send(sock, { kind: "note-sent", to });
    }
    else if (m.action === "note-delete") {
      if (!st.username) return;
      db.notes[st.username] = (db.notes[st.username] || []).filter(x => x.id !== m.id); save();
      send(sock, { kind: "note-list", list: noteSummary(st.username) });
    }

    // ── 대화방 ──
    else if (m.action === "join") {
      if (!st.username) return;
      const code = String(m.code || "").trim();
      if (!/^\d{4}$/.test(code)) return send(sock, { kind: "join-err", text: "코드는 숫자 4자리여야 합니다." });
      if (!db.rooms[code]) { db.rooms[code] = { messages: [] }; save(); }
      st.room = code;
      if (!roomSockets.has(code)) roomSockets.set(code, new Set());
      roomSockets.get(code).add(sock);
      send(sock, { kind: "joined", code });
      send(sock, { kind: "history", list: db.rooms[code].messages });
      toRoom(code, { kind: "system", text: `'${st.username}' 님이 입장하셨습니다.` });
      pushRoster(code);
    }
    else if (m.action === "leave-room") { leaveRoom(sock, st); }
    else if (m.action === "say") {
      if (!st.username || !st.room) return;
      const entry = { nick: st.username, text: String(m.text || "").slice(0, 500), time: Date.now() };
      const room = db.rooms[st.room]; room.messages.push(entry);
      if (room.messages.length > MAX_HISTORY) room.messages.shift();
      toRoom(st.room, { kind: "msg", ...entry }); save();
    }
    else if (m.action === "whisper") {
      if (!st.username || !st.room) return;
      const set = roomSockets.get(st.room);
      const targets = [...set].filter(s => live.get(s).username === m.to && s !== sock);
      if (targets.length === 0) return send(sock, { kind: "whisper-err", text: `'${m.to}' 님이 이 방에 없습니다.` });
      const text = String(m.text || "").slice(0, 500);
      for (const s of targets) send(s, { kind: "whisper", from: st.username, to: m.to, text });
      send(sock, { kind: "whisper", from: st.username, to: m.to, text, mine: true });
    }
  });
  sock.on("close", () => { const st = live.get(sock); if (st) leaveRoom(sock, st); live.delete(sock); });
});

loadDb().then(() => server.listen(PORT, () => console.log(`접속 주소: http://localhost:${PORT}  (저장: ${useRedis ? "Upstash 클라우드" : "로컬 파일"})`)));
