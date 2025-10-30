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
import multer from "multer";
import webpush from "web-push"; // NEW: web-push for sending notifications

dotenv.config();

const app = express();
const PORT = process.env.PORT || 10000;
const API_TOKEN = process.env.API_TOKEN || "changeme";

const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY || "";
const GOOGLE_BASE = process.env.GOOGLE_BASE || "https://generativelanguage.googleapis.com";
const GOOGLE_MODEL = process.env.GOOGLE_MODEL || "gemini-2.0-flash";

/* ========== VAPID / Web Push config ========== */
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY || "";
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY || "";
const VAPID_CONTACT = process.env.VAPID_CONTACT || "mailto:admin@example.com";

if (VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY) {
  try {
    webpush.setVapidDetails(VAPID_CONTACT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
    console.log("Web-push VAPID set.");
  } catch (e) {
    console.warn("Failed to set VAPID details:", e?.message || e);
  }
} else {
  console.warn("VAPID keys missing (VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY). Web Push will not work until set.");
}

/* -------------------- Express setup -------------------- */
app.use(cors());
app.use(express.json({ limit: "10mb" }));

/* -------------------- Simple request logger (debug) -------------------- */
app.use((req, res, next) => {
  console.log(new Date().toISOString(), req.method, req.url, "origin:", req.headers.origin || "-", "host:", req.headers.host || "-", "x-api-token:", !!req.headers["x-api-token"]);
  next();
});

/* -------------------- Uploads (multer) -------------------- */
const UPLOAD_DIR = path.join(process.cwd(), "uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname || "") || "";
    const name = Date.now().toString(36) + "-" + Math.floor(Math.random()*1e6).toString(36) + ext;
    cb(null, name);
  }
});
const upload = multer({ storage, limits: { fileSize: 50 * 1024 * 1024 } }); // 50MB limit
app.use('/uploads', express.static(UPLOAD_DIR));

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
  try { await db.run("PRAGMA journal_mode = WAL;"); } catch(e){ }
  try { await db.run("PRAGMA busy_timeout = 5000;"); } catch(e){ }
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
      // messages table
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
      // frontends
      await c.run(`CREATE TABLE IF NOT EXISTS frontends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        callback_url TEXT NOT NULL,
        secret TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`);
      // blocks
      await c.run(`CREATE TABLE IF NOT EXISTS blocks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        blocker_tfid TEXT NOT NULL,
        blocked_tfid TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(blocker_tfid, blocked_tfid)
      );`);
      // groups (added avatar_url, bio)
      await c.run(`CREATE TABLE IF NOT EXISTS groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        owner_name TEXT,
        avatar_url TEXT,
        bio TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`);
      await c.run(`CREATE TABLE IF NOT EXISTS group_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER NOT NULL,
        tfid TEXT NOT NULL,
        UNIQUE(group_id, tfid)
      );`);
      // outgoing pushes
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
      // reports
      await c.run(`CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reported_tfid TEXT NOT NULL,
        reporter_tfid TEXT,
        reason TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`);

      // NEW: push_subscriptions table
      await c.run(`CREATE TABLE IF NOT EXISTS push_subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tfid TEXT NOT NULL,
        endpoint TEXT NOT NULL UNIQUE,
        subscription TEXT NOT NULL,
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

/* -------------------- Push endpoints -------------------- */

/**
 * Save a subscription for a tfid.
 * Body: { tfid: '000...17digits', subscription: { endpoint:..., keys:{p256dh, auth}, ... } }
 */
app.post("/push/subscribe", async (req, res) => {
  const { tfid, subscription } = req.body || {};
  if (!tfid || !isValidTfid(tfid)) return res.status(400).json({ error: "tfid invalid — must be 17 digits" });
  if (!subscription || typeof subscription !== "object" || !subscription.endpoint) return res.status(400).json({ error: "subscription missing or invalid" });

  try {
    const subJson = JSON.stringify(subscription);
    // store/replace by endpoint unique
    const r = await runWithFallback(
      "INSERT OR REPLACE INTO push_subscriptions (tfid, endpoint, subscription) VALUES (?, ?, ?)",
      [tfid, subscription.endpoint, subJson]
    );
    if (!r.ok) return res.status(500).json({ error: r.error });
    return res.json({ ok: true });
  } catch (err) {
    console.warn("push subscribe error", err);
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

/**
 * Unsubscribe: body { endpoint } or { tfid, endpoint }
 */
app.post("/push/unsubscribe", async (req, res) => {
  const { endpoint, tfid } = req.body || {};
  if (!endpoint) return res.status(400).json({ error: "Missing endpoint" });
  try {
    if (tfid && isValidTfid(tfid)) {
      await runWithFallback("DELETE FROM push_subscriptions WHERE endpoint = ? AND tfid = ?", [endpoint, tfid]);
    } else {
      await runWithFallback("DELETE FROM push_subscriptions WHERE endpoint = ?", [endpoint]);
    }
    return res.json({ ok: true });
  } catch (err) {
    console.warn("push unsubscribe error", err);
    return res.status(500).json({ error: err?.message || String(err) });
  }
});

/**
 * Return VAPID public key to clients (convenience)
 */
app.get("/push/vapid", (req, res) => {
  if (!VAPID_PUBLIC_KEY) return res.status(500).json({ error: "VAPID_PUBLIC_KEY not configured on server" });
  res.json({ ok: true, publicKey: VAPID_PUBLIC_KEY });
});

/* -------------------- Groups management -------------------- */
/* (same as before — omitted here for brevity in this excerpt) */
/* ... (the rest of your group routes remain unchanged) ... */

/* -------------------- Groups listing for a user (API for frontend) -------------------- */
/* ... (unchanged) ... */

/* -------------------- Group messages listing (for UI) -------------------- */
/* ... (unchanged) ... */

/* -------------------- Blocks management -------------------- */
/* ... (unchanged) ... */

/* -------------------- Reports -------------------- */
/* ... (unchanged) ... */

/* -------------------- Small helper: check if recipient blocked sender -------------------- */
async function isBlocked(recipientTfid, senderTfid) {
  const q = await queryWithFallback("SELECT 1 FROM blocks WHERE blocker_tfid = ? AND blocked_tfid = ? LIMIT 1", [recipientTfid, senderTfid]);
  return q.ok && q.rows && q.rows.length > 0;
}

/* -------------------- Outgoing pushes retry worker (unchanged) -------------------- */
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

/* -------------------- Helper: send Web Push to tfid -------------------- */
async function sendWebPushToTfid(tfid, payloadObj) {
  if (!VAPID_PUBLIC_KEY || !VAPID_PRIVATE_KEY) {
    // not configured
    return { ok: false, error: "VAPID not configured" };
  }
  try {
    const q = await queryWithFallback("SELECT id, subscription FROM push_subscriptions WHERE tfid = ?", [tfid]);
    if (!q.ok) return { ok: false, error: q.error };
    const done = [];
    for (const row of q.rows || []) {
      try {
        const sub = JSON.parse(row.subscription);
        // payload string
        const str = JSON.stringify(payloadObj);
        await webpush.sendNotification(sub, str).catch(async (err) => {
          // analyze error, remove subscription if 410 Gone or 404
          const code = err && err.statusCode ? err.statusCode : (err && err.status ? err.status : null);
          if (code === 410 || code === 404) {
            try { await runWithFallback("DELETE FROM push_subscriptions WHERE id = ?", [row.id]); } catch(e) {}
          } else {
            console.warn("webpush send error", err?.message || err, code);
          }
          throw err;
        });
        done.push({ id: row.id, ok: true });
      } catch (err) {
        console.warn("sendWebPushToTfid send fail for subscription id", row.id, err?.message || err);
        // non-fatal
      }
    }
    return { ok: true, sent: done.length };
  } catch (err) {
    console.warn("sendWebPushToTfid error", err?.message || err);
    return { ok: false, error: err?.message || String(err) };
  }
}

/* -------------------- Upload endpoint -------------------- */
app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "No file" });
    const origin = req.protocol + "://" + req.get("host");
    const url = origin + "/uploads/" + encodeURIComponent(req.file.filename);
    return res.json({ ok: true, url });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err?.message || String(err) });
  }
});

/* -------------------- Main API: /api/add supports single tfid, array of tfids, or group_name -------------------- */
app.post("/api/add", async (req, res) => {
  const body = req.body || {};
  // sender identification (who is sending) - optional
  const sender = (body.from || "").toString();
  const name = (body.name || "").toString();
  const rawContent = body.content;
  const contentType = (body.content_type || "").toString().toLowerCase(); // 'text', 'image', 'video', 'group'
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
  if (contentType === "text" || !contentType || contentType === "group" || contentType === "card") {
    content = typeof rawContent === "string" ? sanitizeText(rawContent) : JSON.stringify(rawContent);
    if (!content) return res.status(400).json({ error: "Content must be plain text/JSON (no empty). Max 20000 chars." });
  } else {
    // for image/video we expect media_url to be provided
    if (!mediaUrl || typeof mediaUrl !== "string") return res.status(400).json({ error: "media_url required for image/video content_type" });
    content = typeof rawContent === "string" ? sanitizeText(rawContent) : (rawContent ? JSON.stringify(rawContent) : "");
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

    // if blocked by recipient we skip HTTP push to frontend callback and Web Push
    if (blockedFlag) {
      results.push({ tfid, ok: true, id: ins.lastID, blocked: true });
      continue;
    }

    // attempt immediate push to frontend callback; if fails we queue into outgoing_pushes for retry
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

    // NEW: attempt to send Web Push to stored subscriptions for that tfid
    try {
      // prepare a light notification payload that clients expect
      const notif = {
        title: (payload.name && payload.name !== "") ? payload.name : "TF-Chat",
        body: (typeof payload.content === "string" ? payload.content.slice(0, 160) : ""),
        data: {
          conversation: tfid,
          messageId: payload.id,
          tfid,
          frontend: name
        },
        icon: "/images/notification-128.png",
        badge: "/images/notification-badge.png",
        tag: `tfchat-${tfid}`
      };
      const pushResult = await sendWebPushToTfid(tfid, notif);
      if (pushResult && pushResult.ok) {
        // success info available
      } else {
        // no subscription or error
      }
    } catch (err) {
      console.warn("WebPush send failed (ignored):", err?.message || err);
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
/* ... (unchanged) ... */

/* -------------------- Small HTML viewer (optional) -------------------- */
/* ... (unchanged) ... */

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
