// ws-server.js
// Small WebSocket server + HTTP callback endpoint for tf-sove.onrender.com
// Usage:
//   FRONTEND_SECRET=<secret-from-tf-sove-registration> PORT=8080 node ws-server.js

import express from "express";
import http from "http";
import cors from "cors";
import bodyParser from "body-parser";
import crypto from "crypto";
import { WebSocketServer } from "ws";

const PORT = process.env.PORT ? parseInt(process.env.PORT,10) : 8080;
const FRONTEND_SECRET = process.env.FRONTEND_SECRET || ""; // secret returned by tf-sove.register-frontend for name "site1"
if (!FRONTEND_SECRET) {
  console.warn("WARNING: FRONTEND_SECRET not set. Signature verification will fail unless you set it.");
}

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: "1mb" }));

// HTTP server used by express and ws
const server = http.createServer(app);

// WebSocket server
const wss = new WebSocketServer({ server });

// Map serverTfid -> Set of ws clients
const subs = new Map(); // key: serverTfid (17-digit string), value: Set(ws)

// Recent delivered message ids (to avoid duplicates).
// We key by `${name}:${id}` where `name` is frontend name in callback (site1) and id is integer.
const delivered = new Map(); // key-> timestamp ms
const DELIVERED_TTL_MS = 1000 * 60 * 60; // keep 1 hour (tunable)

// Cleanup old delivered ids periodically
setInterval(() => {
  const now = Date.now();
  for (const [k, ts] of delivered.entries()) {
    if (now - ts > DELIVERED_TTL_MS) delivered.delete(k);
  }
}, 1000 * 60 * 5);

// Utility: verify HMAC sha256 signature (payload JSON string)
function verifySignature(secret, rawBodyStr, signatureHeader) {
  if (!secret) return false;
  try {
    const h = crypto.createHmac("sha256", secret).update(rawBodyStr).digest("hex");
    return h === signatureHeader;
  } catch (e) {
    return false;
  }
}

// WebSocket handling
wss.on("connection", (ws, req) => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  // temporary set of subscribed server IDs for this connection
  ws.subscriptions = new Set();

  ws.on("message", (msgRaw) => {
    let msg;
    try { msg = JSON.parse(msgRaw.toString()); } catch(e){ return; }
    if (msg && msg.type === "subscribe" && msg.server) {
      const serverTfid = String(msg.server);
      // add mapping
      if (!subs.has(serverTfid)) subs.set(serverTfid, new Set());
      subs.get(serverTfid).add(ws);
      ws.subscriptions.add(serverTfid);
      // optionally send ack
      ws.send(JSON.stringify({ type: "subscribed", server: serverTfid }));
    } else if (msg && msg.type === "unsubscribe" && msg.server) {
      const s = subs.get(msg.server);
      if (s) { s.delete(ws); }
      ws.subscriptions.delete(msg.server);
    } else if (msg && msg.type === "hello") {
      // friendly hello: {type:"hello", ui:"TF-7777777", server: "00000000000007777"}
      ws.send(JSON.stringify({ type:"hello:ack", now: new Date().toISOString() }));
    } else if (msg && msg.type === "ping") {
      ws.send(JSON.stringify({ type: "pong", now: new Date().toISOString() }));
    }
  });

  ws.on("close", () => {
    // remove from subs
    for (const s of ws.subscriptions) {
      const set = subs.get(s);
      if (set) {
        set.delete(ws);
        if (set.size === 0) subs.delete(s);
      }
    }
  });

  // On open we may want to ask client to subscribe
  ws.send(JSON.stringify({ type: "welcome", msg: "connected", serverTime: new Date().toISOString() }));
});

// Simple liveness ping
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping(() => {});
  });
}, 30_000);

/**
 * POST /callback
 * - expects JSON payload from tf-sove server:
 *   { id, tfid, name, content, created_at }
 * - header "X-TF-Signature" must be HMAC-SHA256(secret, JSON.stringify(payload))
 */
app.post("/callback", (req, res) => {
  const rawBody = req.rawBody || null; // not available by default with bodyParser.json; we will instead re-stringify
  const signature = req.headers["x-tf-signature"] || req.headers["x-tf-signature".toLowerCase()] || "";

  // Obtain the raw JSON string used to sign: safest is to re-stringify the parsed body
  const payloadStr = JSON.stringify(req.body || {});
  if (FRONTEND_SECRET) {
    const ok = verifySignature(FRONTEND_SECRET, payloadStr, signature);
    if (!ok) {
      console.warn("Callback signature invalid:", signature);
      return res.status(401).json({ error: "Invalid signature" });
    }
  } else {
    // If no secret configured, accept but log
    console.warn("No FRONTEND_SECRET configured; accepting callback without verification.");
  }

  const payload = req.body || {};
  const id = payload.id || null;
  const tfid = payload.tfid || null; // target server tfid (17-digit)
  const name = payload.name || "unknown";

  if (!tfid || !id) {
    console.warn("Callback missing tfid or id", payload);
    return res.status(400).json({ error: "Bad payload" });
  }

  const key = `${name}:${String(id)}`;
  if (delivered.has(key)) {
    // already delivered — ignore duplicate
    log(`Ignoring duplicate message ${key}`);
    return res.json({ ok: true, delivered: false, reason: "duplicate" });
  }

  // mark delivered (with timestamp)
  delivered.set(key, Date.now());

  // parse content (content is a stringified JSON from client)
  let contentParsed = null;
  try { contentParsed = JSON.parse(payload.content); } catch(e) { contentParsed = payload.content; }

  // Build broadcast message
  const broadcast = {
    type: "message",
    id,
    tfid,
    name,
    content: contentParsed,
    created_at: payload.created_at || new Date().toISOString()
  };

  log(`Broadcasting message id=${id} tfid=${tfid} to ${subs.has(tfid) ? subs.get(tfid).size : 0} client(s)`);

  // Broadcast to subscribers of this tfid (clients who have subscribed to this target)
  const set = subs.get(tfid);
  if (set) {
    for (const ws of Array.from(set)) {
      try {
        if (ws.readyState === ws.OPEN) {
          ws.send(JSON.stringify(broadcast));
        }
      } catch (e) {
        console.warn("Failed to send ws", e && e.message);
      }
    }
  }

  // Additionally you may want to notify any clients who represent the SENDER (payload.content.from) if present
  // If contentParsed.from exists and someone subscribed to that server (sender server), we might notify them.
  try {
    const fromServer = contentParsed && contentParsed.from ? String(contentParsed.from) : null;
    if (fromServer && subs.has(fromServer)) {
      const set2 = subs.get(fromServer);
      for (const ws of Array.from(set2)) {
        try{ if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ type: "message_copy", forwarded_for: tfid, ...broadcast })); }catch(e){}
      }
    }
  } catch(e){}

  return res.json({ ok: true, broadcasted: true });
});

// small index route
app.get("/", (req,res) => res.send("WS callback server is up. POST /callback from tf-sove to push messages to connected clients."));

// Start server
server.listen(PORT, () => {
  console.log(`WS+callback server listening on :${PORT}`);
});

/* tiny log wrapper */
function log(...args){ console.log("[ws-server]", ...args); }
