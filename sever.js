// server.js
import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import bodyParser from "body-parser";
import fs from "fs";
import path from "path";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import crypto from "crypto";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 10000;
const API_TOKEN = process.env.API_TOKEN || "changeme";

// Google Generative API envs (must be set)
const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY || "";
const GOOGLE_BASE = process.env.GOOGLE_BASE || "https://generativelanguage.googleapis.com";
const GOOGLE_MODEL = process.env.GOOGLE_MODEL || "gemini-2.0-flash";

app.use(cors());
app.use(bodyParser.json({ limit: "1mb" }));

/* -------------------- DB connectors (sqlite) -------------------- */
const RAW_DB_URLS = Object.keys(process.env)
  .filter(k => k.startsWith("DATABASE_URL"))
  .sort()
  .map(k => process.env[k])
  .filter(Boolean);

if (RAW_DB_URLS.length === 0) {
  console.error("Pa gen DATABASE_URL nan env — mete youn.");
  process.exit(1);
}

const connectors = RAW_DB_URLS.map(url => ({ url, conn: null }));

async function makeSqliteConnector(filePath) {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true }).catch(()=>{});
  const db = await open({ filename: filePath, driver: sqlite3.Database });
  return {
    type: "sqlite",
    db,
    run: (sql, params=[]) => db.run(sql, params),
    all: (sql, params=[]) => db.all(sql, params),
    get: (sql, params=[]) => db.get(sql, params),
    close: () => db.close()
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
    console.warn("DB init failed:", url, err.message);
    connectors[i].conn = null;
  }
  return connectors[i].conn;
}

async function ensureTables() {
  for (let i=0;i<connectors.length;i++){
    const c = await getConnector(i);
    if (!c) continue;
    try {
      await c.run(`CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tfid TEXT NOT NULL,
        name TEXT NOT NULL,
        content TEXT NOT NULL,
        source_url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`);
      await c.run(`CREATE TABLE IF NOT EXISTS frontends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        callback_url TEXT NOT NULL,
        secret TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );`);
      console.log("Tables ensured on", connectors[i].url);
      return true;
    } catch (e) {
      console.warn("Create tables failed on", connectors[i].url, e.message);
      continue;
    }
  }
  throw new Error("Pa gen DB ki dispo pou kreye tablo.");
}

/* Try insert into first working DB, fallback if fails */
async function insertWithFallback(sql, params=[]) {
  let lastErr = null;
  for (let i=0;i<connectors.length;i++){
    const c = await getConnector(i);
    if (!c) { lastErr = new Error("Connector null"); continue; }
    try {
      const r = await c.run(sql, params);
      return { ok: true, db: connectors[i].url, lastID: r.lastID || null };
    } catch (err) {
      lastErr = err;
      console.warn("Insert error on", connectors[i].url, err.message);
      continue;
    }
  }
  return { ok: false, error: lastErr ? lastErr.message : "All DB failed" };
}

async function queryWithFallback(sql, params=[]) {
  for (let i=0;i<connectors.length;i++){
    const c = await getConnector(i);
    if (!c) continue;
    try {
      const rows = await c.all(sql, params);
      return { ok: true, db: connectors[i].url, rows };
    } catch (err) {
      console.warn("Query failed on", connectors[i].url, err.message);
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
  // remove tags, control chars, trim
  let t = s.replace(/<script[\s\S]*?>[\s\S]*?<\/script>/gi, "")
           .replace(/<style[\s\S]*?>[\s\S]*?<\/style>/gi, "")
           .replace(/<\/?[^>]+(>|$)/g, " ")
           .replace(/\s+/g, " ")
           .trim();
  if (t.length === 0) return null;
  // limit length
  const MAX = 20000;
  if (t.length > MAX) t = t.slice(0, MAX) + "...";
  return t;
}

function requireApiToken(req, res, next) {
  const token = req.headers["x-api-token"] || req.query.token;
  if (!token || token !== API_TOKEN) return res.status(401).json({ error: "Unauthorized" });
  next();
}

/* -------------------- Admin: register frontend --------------------
   POST /admin/register-frontend
   body: { name, callback_url }  (protected by API_TOKEN)
   Generates a random secret (returned once) to sign push payloads.
------------------------------------------------------------------------*/
app.post("/admin/register-frontend", requireApiToken, async (req, res) => {
  const { name, callback_url } = req.body || {};
  if (!name || !/^[a-z0-9\-_]{3,64}$/i.test(name)) return res.status(400).json({ error: "Invalid name (3-64 alnum, -,_ allowed)" });
  if (!callback_url || typeof callback_url !== "string") return res.status(400).json({ error: "Missing callback_url" });

  const secret = crypto.randomBytes(24).toString("hex");
  const r = await insertWithFallback(
    "INSERT OR REPLACE INTO frontends (name, callback_url, secret) VALUES (?, ?, ?)",
    [name, callback_url, secret]
  );
  if (!r.ok) return res.status(500).json({ error: r.error });
  return res.json({ ok: true, name, callback_url, secret });
});

app.get("/admin/frontends", requireApiToken, async (req,res) => {
  const q = await queryWithFallback("SELECT id, name, callback_url, created_at FROM frontends ORDER BY id DESC", []);
  if (!q.ok) return res.status(500).json({ error: q.error });
  res.json({ ok: true, rows: q.rows });
});

/* -------------------- Main API -------------------- */

/* POST /api/add
   body: { tfid, name, content, source_url? }
   - require tfid 17 digits
   - require name (registered)
   - content must be text => sanitize & store
   - after storing, push to registered callback_url with signature
*/
app.post("/api/add", async (req, res) => {
  const body = req.body || {};
  const tfid = (body.tfid || "").toString();
  const name = (body.name || "").toString();
  const rawContent = body.content;

  if (!isValidTfid(tfid)) return res.status(400).json({ error: "tfid invalid — must be 17 digits" });
  if (!name) return res.status(400).json({ error: "Missing name (frontend identifier required)" });

  const content = sanitizeText(rawContent);
  if (!content) return res.status(400).json({ error: "Content must be plain text (no empty, no binary). Max 20000 chars." });

  // check frontend mapping
  const fe = await queryWithFallback("SELECT * FROM frontends WHERE name = ? LIMIT 1", [name]);
  if (!fe.ok) return res.status(500).json({ error: fe.error });
  if (!fe.rows || fe.rows.length === 0) return res.status(400).json({ error: "Frontend name not registered" });
  const frontend = fe.rows[0];

  // store in DB
  const ins = await insertWithFallback("INSERT INTO messages (tfid, name, content, source_url) VALUES (?, ?, ?, ?)", [tfid, name, content, body.source_url || null]);
  if (!ins.ok) return res.status(500).json({ error: ins.error });

  // prepare payload and signature to push to frontend callback
  const payload = {
    id: ins.lastID || null,
    tfid,
    name,
    content,
    created_at: new Date().toISOString()
  };

  // compute HMAC signature with frontend.secret
  const signature = crypto.createHmac("sha256", frontend.secret).update(JSON.stringify(payload)).digest("hex");

  // Try push (await so we actually try to deliver now)
  try {
    const pushResp = await fetch(frontend.callback_url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-TF-Signature": signature
      },
      body: JSON.stringify(payload),
      // no custom timeout: Render/Node default
    });
    if (!pushResp.ok) {
      const txt = await pushResp.text().catch(()=>"(no text)");
      console.warn("Push failed to frontend", frontend.name, pushResp.status, txt);
    } else {
      console.log("Pushed message to frontend", frontend.name);
    }
  } catch (err) {
    console.warn("Push exception to frontend", frontend.name, err.message);
  }

  res.json({ ok: true, id: ins.lastID, db: ins.db });
});

/* GET /api/list?tfid=...&name=... */
app.get("/api/list", async (req,res) => {
  const tfid = (req.query.tfid || "").toString();
  const name = (req.query.name || "").toString();
  if (!isValidTfid(tfid)) return res.status(400).json({ error: "tfid invalid — must be 17 digits" });
  if (!name) return res.status(400).json({ error: "Missing name" });

  const r = await queryWithFallback("SELECT id, tfid, name, content, source_url, created_at FROM messages WHERE tfid = ? AND name = ? ORDER BY id DESC LIMIT 500", [tfid, name]);
  if (!r.ok) return res.status(500).json({ error: r.error });
  res.json({ ok: true, db: r.db, rows: r.rows });
});

/* -------------------- AI endpoint (proxy to Google Generative API) --------------------
   POST /api/ai
   body: { tfid, name, prompt, max_output_tokens? }
   - verifies tfid & name and that name registered
   - calls Google Generative API with server-side API key (GOOGLE_API_KEY)
   - returns { ok: true, ai: <string> } or error
------------------------------------------------------------------------*/
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

  // verify frontend exists
  const fe = await queryWithFallback("SELECT * FROM frontends WHERE name = ? LIMIT 1", [name]);
  if (!fe.ok) return res.status(500).json({ error: fe.error });
  if (!fe.rows || fe.rows.length === 0) return res.status(400).json({ error: "Frontend name not registered" });

  // build request to Google Generative API
  const url = `${GOOGLE_BASE}/v1/models/${encodeURIComponent(GOOGLE_MODEL)}:generate?key=${encodeURIComponent(GOOGLE_API_KEY)}`;

  const requestBody = {
    prompt: {
      text: prompt
    },
    // safety and parameters you can tune
    max_output_tokens: Math.min(Math.max(64, maxTokens), 2048),
    temperature: 0.2
  };

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(requestBody)
    });

    const json = await resp.json().catch(()=>null);
    if (!resp.ok) {
      const eText = json ? JSON.stringify(json) : `status ${resp.status}`;
      return res.status(500).json({ error: "AI API error", detail: eText });
    }

    // try extract text from common response shapes
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
    if (!aiText) {
      // fallback to stringified JSON (short)
      aiText = json ? (JSON.stringify(json).slice(0, 20000)) : "";
    }

    return res.json({ ok: true, ai: aiText });
  } catch (err) {
    console.error("AI call error:", err);
    return res.status(500).json({ error: "AI request failed", detail: err.message });
  }
});

/* -------------------- Serve index page (rendered) --------------------
   GET /?tfid=...&name=...
   - validate tfid & name
   - fetch stored messages for that tfid+name and render a simple HTML page
   - show "Bien!" when OK, else show "Error"
------------------------------------------------------------------------*/
app.get("/", async (req, res) => {
  const tfid = (req.query.tfid || "").toString();
  const name = (req.query.name || "").toString();

  if (!tfid || !name) {
    return res.send(`<html><body><h1>Error</h1><p>Missing tfid or name in query. Use ?tfid=123...&name=yourname</p></body></html>`);
  }
  if (!isValidTfid(tfid)) {
    return res.send(`<html><body><h1>Error</h1><p>tfid invalid — must be 17 digits</p></body></html>`);
  }

  // verify frontend exists
  const fe = await queryWithFallback("SELECT * FROM frontends WHERE name = ? LIMIT 1", [name]);
  if (!fe.ok) {
    return res.send(`<html><body><h1>Error</h1><p>Server DB error: ${escapeHtml(fe.error || "unknown")}</p></body></html>`);
  }
  if (!fe.rows || fe.rows.length === 0) {
    return res.send(`<html><body><h1>Error</h1><p>Frontend name not registered</p></body></html>`);
  }

  // load messages
  const r = await queryWithFallback("SELECT id, content, created_at FROM messages WHERE tfid = ? AND name = ? ORDER BY id DESC LIMIT 500", [tfid, name]);
  if (!r.ok) {
    return res.send(`<html><body><h1>Error</h1><p>Could not read messages: ${escapeHtml(r.error || "unknown")}</p></body></html>`);
  }

  // render html
  const rows = r.rows || [];
  let listHtml = rows.map(row => `<li><strong>#${row.id}</strong> — ${escapeHtml(row.content)} <em>(${escapeHtml(row.created_at)})</em></li>`).join("\n");
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
app.get("/health", (req,res)=>res.json({ ok: true }));

app.post("/admin/init", requireApiToken, async (req,res) => {
  try {
    await ensureTables();
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
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
app.listen(PORT, async () => {
  try {
    await ensureTables();
  } catch (err) {
    console.warn("Warn: could not ensure tables on startup:", err.message);
  }
  console.log(`Server listening on port ${PORT}`);
});
