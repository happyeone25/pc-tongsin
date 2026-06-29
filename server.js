// ============================================================
//  큰마을 수다방 - 통합 서버 (화면 + 채팅을 한 번에)
//
//  이 파일 하나가 (1) daehwabang.html 화면을 내보내고
//                 (2) 실시간 채팅(WebSocket)을 처리합니다.
//  → 주소 하나로 페이지도 뜨고 대화도 됩니다.
//
//  실행:
//    1) npm init -y
//    2) npm install ws
//    3) node server.js
//    4) 브라우저에서  http://localhost:8080  열기
//       (배포하면 이 한 줄 주소만 친구에게 공유하면 끝)
//
//  저장: 같은 폴더에 data.json (회원/대화기록) 이 자동 생성됩니다.
// ============================================================

const http = require("http");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;   // 배포 환경은 PORT를 자동 지정함
const DB_FILE = "./data.json";
const HTML_FILE = path.join(__dirname, "daehwabang.html");
const MAX_HISTORY = 300;

// ── 저장소 ────────────────────────────────────────────────
let db = { users: {}, rooms: {} };
if (fs.existsSync(DB_FILE)) { try { db = JSON.parse(fs.readFileSync(DB_FILE, "utf8")); } catch {} }
function save() { fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2)); }

// ── 비밀번호 해시 ─────────────────────────────────────────
function makeHash(pw) {
  const salt = crypto.randomBytes(8).toString("hex");
  const hash = crypto.scryptSync(pw, salt, 32).toString("hex");
  return { salt, hash };
}
function checkPw(pw, user) {
  const h = crypto.scryptSync(pw, user.salt, 32).toString("hex");
  if (h.length !== user.hash.length) return false;
  return crypto.timingSafeEqual(Buffer.from(h), Buffer.from(user.hash));
}

// ── (1) HTTP 서버: 화면(HTML) 내보내기 ────────────────────
const server = http.createServer((req, res) => {
  fs.readFile(HTML_FILE, (err, data) => {
    if (err) { res.writeHead(404); return res.end("daehwabang.html 을 같은 폴더에 두세요."); }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(data);
  });
});

// ── (2) 같은 서버에 WebSocket(채팅) 붙이기 ────────────────
const wss = new WebSocketServer({ server });

const live = new Map();         // sock -> { username, room }
const roomSockets = new Map();  // 방코드 -> Set(sock)

function send(sock, obj) { if (sock.readyState === sock.OPEN) sock.send(JSON.stringify(obj)); }
function toRoom(code, obj) { const set = roomSockets.get(code); if (set) for (const s of set) send(s, obj); }
function rosterOf(code) { const set = roomSockets.get(code); return set ? [...set].map((s) => live.get(s).username) : []; }
function pushRoster(code) { toRoom(code, { kind: "roster", list: rosterOf(code) }); }

wss.on("connection", (sock) => {
  live.set(sock, { username: null, room: null });

  sock.on("message", (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const state = live.get(sock);

    if (m.action === "signup") {
      const u = (m.username || "").trim();
      if (!u || !m.password) return send(sock, { kind: "auth-err", text: "아이디와 비밀번호를 입력하세요." });
      if (db.users[u])       return send(sock, { kind: "auth-err", text: "이미 있는 아이디입니다." });
      db.users[u] = makeHash(m.password); save();
      state.username = u; send(sock, { kind: "auth-ok", username: u });
    }
    else if (m.action === "login") {
      const u = (m.username || "").trim(); const user = db.users[u];
      if (!user || !checkPw(m.password || "", user))
        return send(sock, { kind: "auth-err", text: "아이디 또는 비밀번호가 틀렸습니다." });
      state.username = u; send(sock, { kind: "auth-ok", username: u });
    }
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
    if (state && state.room && roomSockets.has(state.room)) {
      roomSockets.get(state.room).delete(sock);
      toRoom(state.room, { kind: "system", text: `'${state.username}' 님이 퇴장하셨습니다.` });
      pushRoster(state.room);
    }
    live.delete(sock);
  });
});

server.listen(PORT, () => console.log(`접속 주소: http://localhost:${PORT}`));
