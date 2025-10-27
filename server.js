// server.js
import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import fs from "fs";
import path from "path";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import crypto from "crypto";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";

// If running Node < 18, uncomment one of these lines after installing node-fetch or undici
// import fetch from "node-fetch"; globalThis.fetch ||= fetch;
// import { fetch } from "undici"; globalThis.fetch ||= fetch;

dotenv.config();

const app = express();
const PORT = process.env.PORT || 10000;
const API_TOKEN = process.env.API_TOKEN || "changeme";

const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY || "";
const GOOGLE_BASE = process.env.GOOGLE_BASE || "https://generativelanguage.googleapis.com";
const GOOGLE_MODEL = process.env.GOOGLE_MODEL || "gemini-2.0-flash";

app.use(cors());
app.use(express.json({ limit: "5mb" }));

/* -------------------- Simple request logger (debug) -------------------- */
app.use((req, res, next) => {
  console.log(new Date().toISOString(), req.method, req.url, "origin:", req.headers.origin || "-", "host:", req.headers.host || "-", "x-api-token:", !!req.headers["x-api-token"]);
  next();
});

/* -------------------- DB connectors (sqlite) -------------------- */
const RAW_DB_URLS = Object.keys(process.env)
  .filter((k) => k.startsWith("DATABASE_URL"))
  .sort()
  .map((k) => process.env[k])
  .filter(Boolean);

if (RAW_DB_URLS.length === 0) {
  console.error("Pa gen DATABASE_URL nan env — mete youn.");
  process.exit(1);
}

const connectors = RAW_DB_URLS.map((url) => ({ url, conn: null }));

async function makeSqliteConnector(filePath) {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true }).catch(() => {});
  const db = await open({ filename: filePath, driver: sqlite3.Database });
  // Improve concurrency: set WAL and busy timeout
  try {
    await db.run("PRAGMA journal_mode = WAL;");
  } catch (e) {
    // ignore if not supported
  }
  try {
    await db.run("PRAGMA busy_timeout = 5000;");
  } catch (e) {}
  return {
    type: "sqlite",
    db,
    run: (sql, params = []) => db.run(sql, params),
    all: (sql, params = []) => db.all(sql, params),
    get: (sql, params = []) => db.get(sql, params),
    close: () => db.close(),
  };
}

async function getConnector(i) {
  if (!connectors[i]) return null;
  if (connectors[i].conn) return connectors[i].conn;
  const url = connectors[i].url;
  try {
    if (url.startsWith("sqlite://") || url.endsWith(".db")) {
      const filePath = url.startsWith("sqlite://") ? url.replace("sqlite://", "") : url;
      connectors[i].conn = await makeSqliteConnector(filePath);
      console.log("Connected sqlite:", filePath);
    } else {
      console.warn("Unsupported DB URL (only sqlite supported in this script):", url);
      connectors[i].conn = null;
    }
  } catch (err) {
    console.warn("DB init failed:", url, err?.message || err);
    connectors[i].conn = null;
  }
  return connectors[i].conn;
}

/* Ensure tables across all connectors */
async function ensureTables() {
  let okAny = false;
  for (let i = 0; i < connectors.length; i++) {
    const c = await getConnector(i);
    if (!c) continue;
    try {
      await c.run(`CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tfid TEXT NOT NULL,
        name TEXT NOT NULL,
        content TEXT NOT NULL,
        content_type TEXT,
        media_url TEXT,
        source_url TEXT,
        blocked INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`);
      await c.run(`CREATE TABLE IF NOT EXISTS frontends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        callback_url TEXT NOT NULL,
        secret TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`);
      await c.run(`CREATE TABLE IF NOT EXISTS blocks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        blocker_tfid TEXT NOT NULL,
        blocked_tfid TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(blocker_tfid, blocked_tfid)
      );`);
      await c.run(`CREATE TABLE IF NOT EXISTS groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        owner_name TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`);
      await c.run(`CREATE TABLE IF NOT EXISTS group_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER NOT NULL,
        tfid TEXT NOT NULL,
        UNIQUE(group_id, tfid)
      );`);
      await c.run(`CREATE TABLE IF NOT EXISTS outgoing_pushes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id INTEGER,
        frontend_name TEXT,
        callback_url TEXT,
        payload TEXT,
        status TEXT DEFAULT 'pending',
        attempts INTEGER DEFAULT 0,
        last_error TEXT,
        last_attempt_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`);
      okAny = true;
      console.log("Tables ensured on", connectors[i].url);
    } catch (e) {
      console.warn("Create tables failed on", connectors[i].url, e?.message || e);
    }
  }
  if (!okAny) throw new Error("Pa gen DB ki dispo pou kreye tablo.");
  return true;
}

/* Try run (INSERT/UPDATE/DELETE) on first working DB, fallback if fails */
async function runWithFallback(sql, params = []) {
  let lastErr = null;
  for (let i = 0; i < connectors.length; i++) {
    const c = await getConnector(i);
    if (!c) {
      lastErr = new Error("Connector null");
      continue;
    }
    try {
      const r = await c.run(sql, params);
      return { ok: true, db: connectors[i].url, lastID: r?.lastID ?? null, changes: r?.changes ?? null };
    } catch (err) {
      lastErr = err;
      console.warn("Run error on", connectors[i].url, err?.message || err);
      continue;
    }
  }
  return { ok: false, error: lastErr ? lastErr.message : "All DB failed" };
}

async function queryWithFallback(sql, params = []) {
  for (let i = 0; i < connectors.length; i++) {
    const c = await getConnector(i);
    if (!c) continue;
    try {
      const rows = await c.all(sql, params);
      return { ok: true, db: connectors[i].url, rows };
    } catch (err) {
      console.warn("Query failed on", connectors[i].url, err?.message || err);
      continue;
    }
  }
  return { ok: false, error: "No DB available to read" };
}

async function getOneWithFallback(sql, params = []) {
  for (let i = 0; i < connectors.length; i++) {
    const c = await getConnector(i);
    if (!c) continue;
    try {
      const row = await c.get(sql, params);
      return { ok: true, db: connectors[i].url, row };
    } catch (err) {
      console.warn("Query get failed on", connectors[i].url, err?.message || err);
      continue;
    }
  }
  return { ok: false, error: "No DB available to read" };
}

/* -------------------- Helpers -------------------- */
function isValidTfid(tfid) {
  return typeof tfid === "string" && /^\d{17}$/.test(tfid);
}

function sanitizeText(s) {
  if (typeof s !== "string") return null;
  let t = s
    .replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, "")
    .replace(/<\/?[^>]+(>|$)/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (t.length === 0) return null;
  const MAX = 20000;
  if (t.length > MAX) t = t.slice(0, MAX) + "...";
  return t;
}

function requireApiToken(req, res, next) {
  const token = req.headers["x-api-token"] || req.query.token;
  if (!token || token !== API_TOKEN) return res.status(401).json({ error: "Unauthorized" });
  next();
}

/* -------------------- WebSocket server -------------------- */
const httpServer = http.createServer(app);
const wss = new WebSocketServer({ server: httpServer, path: "/ws" });

// Map: tfid(string) => Set of ws connections
const subscribers = new Map();

function addSubscriber(tfid, ws) {
  if (!subscribers.has(tfid)) subscribers.set(tfid, new Set());
  subscribers.get(tfid).add(ws);
}

function removeSubscriber(tfid, ws) {
  const s = subscribers.get(tfid);
  if (!s) return;
  s.delete(ws);
  if (s.size === 0) subscribers.delete(tfid);
}

function broadcastToTfid(tfid, payload) {
  const s = subscribers.get(tfid);
  if (!s) return;
  const data = JSON.stringify(payload);
  for (const ws of s) {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(data); } catch (e) { /* ignore send errors */ }
    }
  }
}

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.subscriptions = new Set();

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (msg) => {
    let data = null;
    try { data = JSON.parse(msg.toString()); } catch (e) { return; }
    if (!data || typeof data !== "object") return;
    if (data.type === "subscribe" && data.tfid) {
      if (isValidTfid(data.tfid)) {
        addSubscriber(data.tfid, ws);
        ws.subscriptions.add(data.tfid);
        ws.send(JSON.stringify({ type: "subscribed", tfid: data.tfid }));
        console.log("WS subscribed", data.tfid);
        // send recent undelivered messages (last 200)
        (async () => {
          const r = await queryWithFallback("SELECT id, name, content, content_type, media_url, created_at FROM messages WHERE tfid = ? ORDER BY id DESC LIMIT 200", [data.tfid]);
          if (r.ok && r.rows && r.rows.length) {
            for (let i = r.rows.length - 1; i >= 0; i--) {
              try { ws.send(JSON.stringify({ type: "message", ...r.rows[i] })); } catch (e) {}
            }
          }
        })();
      } else {
        ws.send(JSON.stringify({ type: "error", error: "tfid invalid — must be 17 digits" }));
      }
    } else if (data.type === "unsubscribe" && data.tfid) {
      removeSubscriber(data.tfid, ws);
      ws.subscriptions.delete(data.tfid);
    } else {
      // ignore other messages
    }
  });

  ws.on("close", () => {
    for (const tfid of ws.subscriptions) removeSubscriber(tfid, ws);
    ws.subscriptions.clear();
  });

  ws.on("error", (err) => {
    console.warn("WS error", err?.message || err);
  });
});

// ping/pong to detect dead clients
const wsPingInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch (e) { /* ignore */ }
  });
}, 30000);

/* -------------------- Admin: register frontend -------------------- */
app.post("/admin/register-frontend", requireApiToken, async (req, res) => {
  const { name, callback_url } = req.body || {};
  if (!name || !/^[a-z0-9\-_]{3,64}$/i.test(name)) return res.status(400).json({ error: "Invalid name (3-64 alnum, -,_ allowed)" });
  if (!callback_url || typeof callback_url !== "string") return res.status(400).json({ error: "Missing callback_url" });
  try {
    const u = new URL(callback_url);
    if (!["http:", "https:"].includes(u.protocol)) throw new Error("invalid protocol");
  } catch (e) {
    return res.status(400).json({ error: "callback_url must be a valid http(s) URL" });
  }

  const secret = crypto.randomBytes(24).toString("hex");
  const r = await runWithFallback(
    "INSERT OR REPLACE INTO frontends (name, callback_url, secret) VALUES (?, ?, ?)",
    [name, callback_url, secret]
  );
  if (!r.ok) return res.status(500).json({ error: r.error });
  return res.json({ ok: true, name, callback_url, secret });
});

app.get("/admin/frontends", requireApiToken, async (req, res) => {
  const q = await queryWithFallback("SELECT id, name, callback_url, created_at FROM frontends ORDER BY id DESC", []);
  if (!q.ok) return res.status(500).json({ error: q.error });
  res.json({ ok: true, rows: q.rows });
});

/* -------------------- Group management -------------------- */
app.post("/admin/groups/create", requireApiToken, async (req, res) => {
  const { name, owner_name } = req.body || {};
  if (!name || typeof name !== "string") return res.status(400).json({ error: "Missing group name" });
  const r = await runWithFallback("INSERT OR IGNORE INTO groups (name, owner_name) VALUES (?, ?)", [name, owner_name || null]);
  if (!r.ok) return res.status(500).json({ error: r.error });
  res.json({ ok: true, group: name });
});

app.post("/admin/groups/add", requireApiToken, async (req, res) => {
  const { group_name, tfid } = req.body || {};
  if (!group_name || !tfid) return res.status(400).json({ error: "Missing group_name or tfid" });
  if (!isValidTfid(tfid)) return res.status(400).json({ error: "tfid invalid — must be 17 digits" });
  const g = await getOneWithFallback("SELECT id FROM groups WHERE name = ? LIMIT 1", [group_name]);
  if (!g.ok || !g.row) return res.status(400).json({ error: "Group not found" });
  const r = await runWithFallback("INSERT OR IGNORE INTO group_members (group_id, tfid) VALUES (?, ?)", [g.row.id, tfid]);
  if (!r.ok) return res.status(500).json({ error: r.error });
  res.json({ ok: true });
});

app.post("/admin/groups/remove", requireApiToken, async (req, res) => {
  const { group_name, tfid } = req.body || {};
  if (!group_name || !tfid) return res.status(400).json({ error: "Missing group_name or tfid" });
  const g = await getOneWithFallback("SELECT id FROM groups WHERE name = ? LIMIT 1", [group_name]);
  if (!g.ok || !g.row) return res.status(400).json({ error: "Group not found" });
  const q = await runWithFallback("DELETE FROM group_members WHERE group_id = ? AND tfid = ?", [g.row.id, tfid]);
  if (!q.ok) return res.status(500).json({ error: q.error });
  res.json({ ok: true });
});

app.get("/admin/groups/members", requireApiToken, async (req, res) => {
  const group_name = (req.query.group_name || "").toString();
  if (!group_name) return res.status(400).json({ error: "Missing group_name" });
  const g = await getOneWithFallback("SELECT id FROM groups WHERE name = ? LIMIT 1", [group_name]);
  if (!g.ok || !g.row) return res.status(400).json({ error: "Group not found" });
  const m = await queryWithFallback("SELECT tfid FROM group_members WHERE group_id = ?", [g.row.id]);
  if (!m.ok) return res.status(500).json({ error: m.error });
  res.json({ ok: true, members: m.rows.map((r) => r.tfid) });
});

/* -------------------- Blocks management -------------------- */
app.post("/admin/block", requireApiToken, async (req, res) => {
  const { blocker, blocked } = req.body || {};
  if (!isValidTfid(blocker) || !isValidTfid(blocked)) return res.status(400).json({ error: "Invalid tfid(s)" });
  const r = await runWithFallback("INSERT OR IGNORE INTO blocks (blocker_tfid, blocked_tfid) VALUES (?, ?)", [blocker, blocked]);
  if (!r.ok) return res.status(500).json({ error: r.error });
  res.json({ ok: true });
});

app.post("/admin/unblock", requireApiToken, async (req, res) => {
  const { blocker, blocked } = req.body || {};
  if (!isValidTfid(blocker) || !isValidTfid(blocked)) return res.status(400).json({ error: "Invalid tfid(s)" });
  const r = await runWithFallback("DELETE FROM blocks WHERE blocker_tfid = ? AND blocked_tfid = ?", [blocker, blocked]);
  if (!r.ok) return res.status(500).json({ error: r.error });
  res.json({ ok: true });
});

/* -------------------- Small helper: check if recipient blocked sender -------------------- */
async function isBlocked(recipientTfid, senderTfid) {
  const q = await queryWithFallback("SELECT 1 FROM blocks WHERE blocker_tfid = ? AND blocked_tfid = ? LIMIT 1", [recipientTfid, senderTfid]);
  return q.ok && q.rows && q.rows.length > 0;
}

/* -------------------- Outgoing pushes retry worker -------------------- */
async function processOutgoingPushes() {
  try {
    const q = await queryWithFallback("SELECT id, message_id, frontend_name, callback_url, payload, attempts FROM outgoing_pushes WHERE status IN ('pending','failed') ORDER BY created_at ASC LIMIT 50", []);
    if (!q.ok || !q.rows) return;
    for (const row of q.rows) {
      const fe = await queryWithFallback("SELECT * FROM frontends WHERE name = ? LIMIT 1", [row.frontend_name]);
      const secret = (fe.ok && fe.rows && fe.rows[0]) ? fe.rows[0].secret : null;
      try {
        const payloadObj = JSON.parse(row.payload);
        const sig = secret ? crypto.createHmac("sha256", secret).update(JSON.stringify(payloadObj)).digest("hex") : "";
        const resp = await fetch(row.callback_url, {
          method: "POST",
          headers: { "Content-Type": "application/json", "X-TF-Signature": sig },
          body: JSON.stringify(payloadObj),
        });
        if (resp.ok) {
          await runWithFallback("UPDATE outgoing_pushes SET status = 'sent', attempts = ?, last_attempt_at = CURRENT_TIMESTAMP WHERE id = ?", [row.attempts + 1, row.id]);
        } else {
          const txt = await resp.text().catch(() => "(no text)");
          await runWithFallback("UPDATE outgoing_pushes SET status = 'failed', attempts = ?, last_error = ?, last_attempt_at = CURRENT_TIMESTAMP WHERE id = ?", [row.attempts + 1, `status ${resp.status} ${txt}`.slice(0, 2000), row.id]);
        }
      } catch (err) {
        await runWithFallback("UPDATE outgoing_pushes SET status = 'failed', attempts = ?, last_error = ?, last_attempt_at = CURRENT_TIMESTAMP WHERE id = ?", [row.attempts + 1, (err?.message || String(err)).slice(0, 2000), row.id]);
      }
    }
  } catch (e) {
    console.warn("processOutgoingPushes err", e?.message || e);
  }
}

const pushWorkerInterval = setInterval(processOutgoingPushes, 20000); // attempt pending pushes every 20s

/* -------------------- Main API: /api/add supports single tfid, array of tfids, or group_name -------------------- */
app.post("/api/add", async (req, res) => {
  const body = req.body || {};
  // sender identification (who is sending) - optional
  const sender = (body.from || "").toString();
  const name = (body.name || "").toString();
  const rawContent = body.content;
  const contentType = (body.content_type || "").toString().toLowerCase(); // 'text', 'image', 'video'
  const mediaUrl = body.media_url || null;

  if (!name) return res.status(400).json({ error: "Missing name (frontend identifier required)" });

  // resolve recipients
  let targetTfids = [];
  if (body.group_name) {
    const g = await getOneWithFallback("SELECT id FROM groups WHERE name = ? LIMIT 1", [body.group_name]);
    if (!g.ok || !g.row) return res.status(400).json({ error: "Group not found" });
    const members = await queryWithFallback("SELECT tfid FROM group_members WHERE group_id = ?", [g.row.id]);
    if (!members.ok) return res.status(500).json({ error: members.error });
    targetTfids = members.rows.map((r) => r.tfid);
  } else if (Array.isArray(body.tfid)) {
    targetTfids = body.tfid.map((t) => t.toString()).filter((t) => isValidTfid(t));
  } else if (body.tfid) {
    const t = body.tfid.toString();
    if (!isValidTfid(t)) return res.status(400).json({ error: "tfid invalid — must be 17 digits" });
    targetTfids = [t];
  } else {
    return res.status(400).json({ error: "Missing tfid or group_name" });
  }

  if (targetTfids.length === 0) return res.status(400).json({ error: "No valid recipients" });

  // sanitize/prepare content
  let content = null;
  if (contentType === "text" || !contentType) {
    content = sanitizeText(typeof rawContent === "string" ? rawContent : JSON.stringify(rawContent));
    if (!content) return res.status(400).json({ error: "Content must be plain text (no empty, no binary). Max 20000 chars." });
  } else {
    // for image/video we expect media_url to be provided
    if (!mediaUrl || typeof mediaUrl !== "string") return res.status(400).json({ error: "media_url required for image/video content_type" });
    content = sanitizeText(typeof rawContent === "string" ? rawContent : (rawContent ? JSON.stringify(rawContent) : "")) || "";
  }

  // check frontend mapping
  const fe = await queryWithFallback("SELECT * FROM frontends WHERE name = ? LIMIT 1", [name]);
  if (!fe.ok) return res.status(500).json({ error: fe.error });
  if (!fe.rows || fe.rows.length === 0) return res.status(400).json({ error: "Frontend name not registered" });
  const frontend = fe.rows[0];

  const results = [];

  for (const tfid of targetTfids) {
    // check blocks: if recipient blocked sender, mark as blocked and still store but don't push
    let blockedFlag = 0;
    if (sender && isValidTfid(sender)) {
      if (await isBlocked(tfid, sender)) {
        blockedFlag = 1;
      }
    }

    // store in DB
    const ins = await runWithFallback(
      "INSERT INTO messages (tfid, name, content, content_type, media_url, source_url, blocked) VALUES (?, ?, ?, ?, ?, ?, ?)",
      [tfid, name, content, contentType || "text", mediaUrl, body.source_url || null, blockedFlag]
    );
    if (!ins.ok) {
      results.push({ tfid, ok: false, error: ins.error });
      continue;
    }

    const payload = {
      id: ins.lastID || null,
      tfid,
      name,
      content,
      content_type: contentType || "text",
      media_url: mediaUrl || null,
      from: sender || null,
      created_at: new Date().toISOString(),
    };

    // prepare signature
    const signature = crypto.createHmac("sha256", frontend.secret).update(JSON.stringify(payload)).digest("hex");

    // broadcast via WebSocket always (clients connected will get it)
    try {
      broadcastToTfid(tfid, { type: "message", ...payload });
    } catch (e) {
      console.warn("WS broadcast error", e?.message || e);
    }

    // if blocked by recipient we skip HTTP push to frontend callback
    if (blockedFlag) {
      results.push({ tfid, ok: true, id: ins.lastID, blocked: true });
      continue;
    }

    // attempt immediate push; if fails we queue into outgoing_pushes for retry
    try {
      const pushResp = await fetch(frontend.callback_url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-TF-Signature": signature,
        },
        body: JSON.stringify(payload),
      });
      if (!pushResp.ok) {
        const txt = await pushResp.text().catch(() => "(no text)");
        console.warn("Push failed to frontend", frontend.name, pushResp.status, txt);
        // queue for retry
        await runWithFallback(
          "INSERT INTO outgoing_pushes (message_id, frontend_name, callback_url, payload, status, attempts, last_error) VALUES (?, ?, ?, ?, 'failed', 1, ?)",
          [ins.lastID, frontend.name, frontend.callback_url, JSON.stringify(payload), `status ${pushResp.status} ${String(txt).slice(0, 1000)}`]
        );
        results.push({ tfid, ok: true, id: ins.lastID, pushed: false });
      } else {
        console.log("Pushed message to frontend", frontend.name);
        await runWithFallback(
          "INSERT INTO outgoing_pushes (message_id, frontend_name, callback_url, payload, status, attempts) VALUES (?, ?, ?, ?, 'sent', 1)",
          [ins.lastID, frontend.name, frontend.callback_url, JSON.stringify(payload)]
        );
        results.push({ tfid, ok: true, id: ins.lastID, pushed: true });
      }
    } catch (err) {
      console.warn("Push exception to frontend", frontend.name, (err?.message || err));
      // queue for retry
      await runWithFallback(
        "INSERT INTO outgoing_pushes (message_id, frontend_name, callback_url, payload, status, attempts, last_error) VALUES (?, ?, ?, ?, 'pending', 0, ?)",
        [ins.lastID, frontend.name, frontend.callback_url, JSON.stringify(payload), (err?.message || String(err)).slice(0, 1000)]
      );
      results.push({ tfid, ok: true, id: ins.lastID, pushed: false, queued: true });
    }
  }

  res.json({ ok: true, results });
});

/* GET /api/list?tfid=...&name=... */
app.get("/api/list", async (req, res) => {
  const tfid = (req.query.tfid || "").toString();
  const name = (req.query.name || "").toString();
  if (!isValidTfid(tfid)) return res.status(400).json({ error: "tfid invalid — must be 17 digits" });
  if (!name) return res.status(400).json({ error: "Missing name" });

  const r = await queryWithFallback(
    "SELECT id, tfid, name, content, content_type, media_url, source_url, blocked, created_at FROM messages WHERE tfid = ? AND name = ? ORDER BY id DESC LIMIT 500",
    [tfid, name]
  );
  if (!r.ok) return res.status(500).json({ error: r.error });
  res.json({ ok: true, db: r.db, rows: r.rows });
});

/* -------------------- AI endpoint (proxy) -------------------- */
app.post("/api/ai", async (req, res) => {
  const body = req.body || {};
  const tfid = (body.tfid || "").toString();
  const name = (body.name || "").toString();
  const prompt = (body.prompt || "").toString();
  const maxTokens = parseInt(body.max_output_tokens || "512", 10);

  if (!isValidTfid(tfid)) return res.status(400).json({ error: "tfid invalid — must be 17 digits" });
  if (!name) return res.status(400).json({ error: "Missing name" });
  if (!prompt || prompt.trim().length === 0) return res.status(400).json({ error: "Missing prompt" });
  if (!GOOGLE_API_KEY) return res.status(500).json({ error: "Server missing GOOGLE_API_KEY env" });

  const fe = await queryWithFallback("SELECT * FROM frontends WHERE name = ? LIMIT 1", [name]);
  if (!fe.ok) return res.status(500).json({ error: fe.error });
  if (!fe.rows || fe.rows.length === 0) return res.status(400).json({ error: "Frontend name not registered" });

  const url = `${GOOGLE_BASE}/v1/models/${encodeURIComponent(GOOGLE_MODEL)}:generate?key=${encodeURIComponent(GOOGLE_API_KEY)}`;

  const requestBody = {
    prompt: { text: prompt },
    max_output_tokens: Math.min(Math.max(64, maxTokens), 2048),
    temperature: 0.2,
  };

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(requestBody),
    });

    const json = await resp.json().catch(() => null);
    if (!resp.ok) {
      const eText = json ? JSON.stringify(json) : `status ${resp.status}`;
      return res.status(500).json({ error: "AI API error", detail: eText });
    }

    let aiText = null;
    if (json) {
      if (Array.isArray(json.candidates) && json.candidates.length > 0 && typeof json.candidates[0].content === "string") {
        aiText = json.candidates[0].content;
      } else if (typeof json.output_text === "string") {
        aiText = json.output_text;
      } else if (Array.isArray(json.output) && json.output.length > 0 && typeof json.output[0].content === "string") {
        aiText = json.output[0].content;
      } else if (typeof json.candidates?.[0]?.output === "string") {
        aiText = json.candidates[0].output;
      }
    }
    if (!aiText) aiText = json ? (JSON.stringify(json).slice(0, 20000)) : "";

    return res.json({ ok: true, ai: aiText });
  } catch (err) {
    console.error("AI call error:", err);
    return res.status(500).json({ error: "AI request failed", detail: err?.message || String(err) });
  }
});

/* -------------------- Small HTML viewer (optional) -------------------- */
app.get("/", async (req, res) => {
  const tfid = (req.query.tfid || "").toString();
  const name = (req.query.name || "").toString();

  if (!tfid || !name) {
    return res.send(`<html><body><h1>Error</h1><p>Missing tfid or name in query. Use ?tfid=123...&name=yourname</p></body></html>`);
  }
  if (!isValidTfid(tfid)) {
    return res.send(`<html><body><h1>Error</h1><p>tfid invalid — must be 17 digits</p></body></html>`);
  }

  const fe = await queryWithFallback("SELECT * FROM frontends WHERE name = ? LIMIT 1", [name]);
  if (!fe.ok) {
    return res.send(`<html><body><h1>Error</h1><p>Server DB error: ${escapeHtml(fe.error || "unknown")}</p></body></html>`);
  }
  if (!fe.rows || fe.rows.length === 0) {
    return res.send(`<html><body><h1>Error</h1><p>Frontend name not registered</p></body></html>`);
  }

  const r = await queryWithFallback("SELECT id, content, content_type, media_url, created_at FROM messages WHERE tfid = ? AND name = ? ORDER BY id DESC LIMIT 500", [tfid, name]);
  if (!r.ok) {
    return res.send(`<html><body><h1>Error</h1><p>Could not read messages: ${escapeHtml(r.error || "unknown")}</p></body></html>`);
  }

  const rows = r.rows || [];
  let listHtml = rows.map((row) => `<li><strong>#${row.id}</strong> — ${escapeHtml(row.content)} ${row.media_url ? `(media: ${escapeHtml(row.media_url)})` : ""} <em>(${escapeHtml(row.created_at)})</em></li>`).join("\n");
  if (!listHtml) listHtml = "<li>(pa gen mesaj pou tfid sa)</li>";

  const html = `<!doctype html>
  <html>
  <head><meta charset="utf-8"><title>TFID ${escapeHtml(tfid)}</title>
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <style>body{font-family:system-ui,Arial;max-width:900px;margin:20px auto;padding:10px}li{margin:8px 0;padding:8px;border:1px solid #eee;border-radius:6px}</style>
  </head>
  <body>
    <h1>Bien!</h1>
    <p><strong>tfid:</strong> ${escapeHtml(tfid)}<br/><strong>name:</strong> ${escapeHtml(name)}</p>
    <h2>Sak stocké yo</h2>
    <ul>${listHtml}</ul>
  </body>
  </html>`;

  res.send(html);
});

/* Health and admin init */
app.get("/health", (req, res) => res.json({ ok: true }));

app.post("/admin/init", requireApiToken, async (req, res) => {
  try {
    await ensureTables();
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
});

/* -------------------- Utilities -------------------- */
function escapeHtml(s) {
  if (s == null) return "";
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/* -------------------- Start server -------------------- */
httpServer.listen(PORT, async () => {
  try {
    await ensureTables();
  } catch (err) {
    console.warn("Warn: could not ensure tables on startup:", err?.message || String(err));
  }
  console.log(`WS+callback server listening on :${PORT}`);
});

/* Graceful shutdown */
async function gracefulShutdown(signal) {
  console.log("Shutting down (", signal, ")...");
  clearInterval(wsPingInterval);
  clearInterval(pushWorkerInterval);
  try { wss.clients.forEach((ws) => { try { ws.close(); } catch (e) {} }); wss.close(); } catch (e) {}
  try { await new Promise((resolve) => httpServer.close(resolve)); } catch (e) {}
  for (let i = 0; i < connectors.length; i++) {
    const c = connectors[i]?.conn;
    if (c && typeof c.close === "function") {
      try { await c.close(); console.log("Closed DB", connectors[i].url); } catch (e) { console.warn("Close DB err", e?.message || e); }
    }
  }
  process.exit(0);
}
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
