// ============================================================
//  큰마을 PC통신 - 통합 서버
//  기능: 회원가입/로그인 · 플라자(게시판) · 대화방(채팅/귓속말) · 기록저장
//
//  실행:  npm install ws  →  node server.js
//  접속:  http://localhost:8080
//  저장:  같은 폴더의 data.json (회원·게시글·대화기록)
// ============================================================

const http = require("http");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const DB_FILE = "./data.json";
const HTML_FILE = path.join(__dirname, "daehwabang.html");
const MAX_HISTORY = 300;   // 대화방 보관 줄 수
const MAX_POSTS = 200;     // 게시판 보관 글 수

// ── 저장소 ────────────────────────────────────────────────
let db = { users: {}, rooms: {}, board: [], nextNo: 1 };
if (fs.existsSync(DB_FILE)) {
  try {
    db = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
    db.board = db.board || [];
    db.nextNo = db.nextNo || (db.board.reduce((m, p) => Math.max(m, p.no), 0) + 1);
  } catch {}
}
function save() { fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2)); }

// ── 비밀번호 해시 ─────────────────────────────────────────
function makeHash(pw) {
  const salt = crypto.randomBytes(8).toString("hex");
  return { salt, hash: crypto.scryptSync(pw, salt, 32).toString("hex") };
}
function checkPw(pw, user) {
  const h = crypto.scryptSync(pw, user.salt, 32).toString("hex");
  if (h.length !== user.hash.length) return false;
  return crypto.timingSafeEqual(Buffer.from(h), Buffer.from(user.hash));
}

// ── 실시간 상태 ───────────────────────────────────────────
const live = new Map();         // sock -> { username, room }
const roomSockets = new Map();  // 방코드 -> Set(sock)

const server = http.createServer((req, res) => {
  fs.readFile(HTML_FILE, (err, data) => {
    if (err) { res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" }); return res.end("daehwabang.html not found"); }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(data);
  });
});
const wss = new WebSocketServer({ server });

function send(sock, obj) { if (sock.readyState === sock.OPEN) sock.send(JSON.stringify(obj)); }
function toRoom(code, obj) { const set = roomSockets.get(code); if (set) for (const s of set) send(s, obj); }
function rosterOf(code) { const set = roomSockets.get(code); return set ? [...set].map((s) => live.get(s).username) : []; }
function pushRoster(code) { toRoom(code, { kind: "roster", list: rosterOf(code) }); }

// 게시판 목록용 요약(본문 제외)
function boardSummary() {
  return db.board.map((p) => ({ no: p.no, nick: p.nick, title: p.title, time: p.time, hit: p.hit }))
                 .sort((a, b) => b.no - a.no);
}
function leaveRoom(sock, state) {
  if (state.room && roomSockets.has(state.room)) {
    roomSockets.get(state.room).delete(sock);
    toRoom(state.room, { kind: "system", text: `'${state.username}' 님이 퇴장하셨습니다.` });
    pushRoster(state.room);
  }
  state.room = null;
}

wss.on("connection", (sock) => {
  live.set(sock, { username: null, room: null });

  sock.on("message", (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const state = live.get(sock);

    // ── 인증 ──
    if (m.action === "signup") {
      const u = (m.username || "").trim();
      if (!u || !m.password) return send(sock, { kind: "auth-err", text: "아이디와 비밀번호를 입력하세요." });
      if (db.users[u])       return send(sock, { kind: "auth-err", text: "이미 있는 아이디입니다." });
      db.users[u] = makeHash(m.password); save();
      state.username = u; send(sock, { kind: "auth-ok", username: u });
    }
    else if (m.action === "login") {
      const u = (m.username || "").trim(); const user = db.users[u];
      if (!user || !checkPw(m.password || "", user)) return send(sock, { kind: "auth-err", text: "아이디 또는 비밀번호가 틀렸습니다." });
      state.username = u; send(sock, { kind: "auth-ok", username: u });
    }

    // ── 플라자(게시판) ──
    else if (m.action === "board-list") {
      send(sock, { kind: "board-list", list: boardSummary() });
    }
    else if (m.action === "board-read") {
      const p = db.board.find((x) => x.no === m.no);
      if (!p) return send(sock, { kind: "board-err", text: "글을 찾을 수 없습니다." });
      p.hit = (p.hit || 0) + 1; save();
      send(sock, { kind: "board-read", post: p });
    }
    else if (m.action === "board-write") {
      if (!state.username) return;
      const title = String(m.title || "").trim().slice(0, 60);
      if (!title) return send(sock, { kind: "board-err", text: "제목을 입력하세요." });
      const post = { no: db.nextNo++, nick: state.username, title, body: String(m.body || "").slice(0, 4000), time: Date.now(), hit: 0 };
      db.board.push(post);
      if (db.board.length > MAX_POSTS) db.board.shift();
      save();
      send(sock, { kind: "board-written", no: post.no });
    }

    // ── 대화방 ──
    else if (m.action === "join") {
      if (!state.username) return;
      const code = String(m.code || "").trim();
      if (!/^\d{4}$/.test(code)) return send(sock, { kind: "join-err", text: "코드는 숫자 4자리여야 합니다." });
      if (!db.rooms[code]) { db.rooms[code] = { messages: [] }; save(); }
      state.room = code;
      if (!roomSockets.has(code)) roomSockets.set(code, new Set());
      roomSockets.get(code).add(sock);
      send(sock, { kind: "joined", code });
      send(sock, { kind: "history", list: db.rooms[code].messages });
      toRoom(code, { kind: "system", text: `'${state.username}' 님이 입장하셨습니다.` });
      pushRoster(code);
    }
    else if (m.action === "leave-room") {
      leaveRoom(sock, state);
    }
    else if (m.action === "say") {
      if (!state.username || !state.room) return;
      const entry = { nick: state.username, text: String(m.text || "").slice(0, 500), time: Date.now() };
      const room = db.rooms[state.room];
      room.messages.push(entry);
      if (room.messages.length > MAX_HISTORY) room.messages.shift();
      save();
      toRoom(state.room, { kind: "msg", ...entry });
    }
    else if (m.action === "whisper") {
      if (!state.username || !state.room) return;
      const set = roomSockets.get(state.room);
      const targets = [...set].filter((s) => live.get(s).username === m.to && s !== sock);
      if (targets.length === 0) return send(sock, { kind: "whisper-err", text: `'${m.to}' 님이 이 방에 없습니다.` });
      const text = String(m.text || "").slice(0, 500);
      for (const s of targets) send(s, { kind: "whisper", from: state.username, to: m.to, text });
      send(sock, { kind: "whisper", from: state.username, to: m.to, text, mine: true });
    }
  });

  sock.on("close", () => {
    const state = live.get(sock);
    if (state) leaveRoom(sock, state);
    live.delete(sock);
  });
});

server.listen(PORT, () => console.log(`접속 주소: http://localhost:${PORT}`));
