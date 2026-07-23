const WebSocket = require('ws');
const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const path = require('path');

require('dotenv').config();

const PORT = process.env.PORT || 8080;
const STATE_FILE = './data/server_state.json';
const DASHBOARD_FILE = path.join(__dirname, 'dashboard.html');

const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || null;

const LOG_FILE = './data/transactions.log';
const MAX_HISTORY = 2000;

const RETRY_INTERVAL_MS = 5000;

// ─────────────────────────────────────────────────────────────
// Persistance : disque local par défaut (pratique en local), mais un plan
// gratuit type Render a un système de fichiers ÉPHÉMÈRE — tout ce qui est
// écrit sur disque est perdu à chaque mise en veille/redéploiement. Si les
// identifiants Upstash Redis sont fournis (variables d'env
// UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN, gratuites, sans CB),
// on bascule automatiquement sur Redis pour que identities/links/pending
// survivent au cycle de veille de Render. Le disque local reste utilisé
// tel quel si ces variables sont absentes (usage local).
// ─────────────────────────────────────────────────────────────
const USE_REDIS = !!(process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN);
let redis = null;
if (USE_REDIS) {
  const { Redis } = require('@upstash/redis');
  redis = new Redis({
    url: process.env.UPSTASH_REDIS_REST_URL,
    token: process.env.UPSTASH_REDIS_REST_TOKEN,
  });
}
const REDIS_STATE_KEY = 'relais:state';
const REDIS_LOG_KEY = 'relais:log';

if (!USE_REDIS && !fs.existsSync('./data')) fs.mkdirSync('./data', { recursive: true });

// --- "Base de données" en mémoire (juste pour le test) ---
const users = new Map();       // id -> { pubkey, socket }         (connectés actuellement, jamais persisté)
const identities = new Map();  // id -> { signPubkey }             (épinglé au 1er contact — PERSISTÉ sur disque)
const challenges = new Map();  // socket -> { id, pubkey, nonce, expected }
const links = new Map();       // id -> Set<id des contacts liés>  (PERSISTÉ sur disque, cf. addLink/isLinked)
const pending = new Map();     // id -> [enveloppes en attente si hors ligne]  (PERSISTÉ sur disque)

// --- Observabilité (dashboard) ---
// Sockets admin abonnées au flux de transactions (pas persisté, juste pour observer en direct).
const adminSockets = new Set();

// Diffuse un évènement "transaction" à tous les tableaux de bord ouverts.
// `data` contient le message brut échangé (JSON complet), affiché au clic dans le dashboard.
function broadcastTransaction({ from = null, to = null, action, type = null, status = 'ok', data = {} }) {
  const event = { time: Date.now(), from, to, action, type, status, data };
  const raw = JSON.stringify(event);

  appendLog(event); // fire-and-forget : jamais attendu, comme l'écriture disque d'origine

  for (const s of adminSockets) {
    if (s.readyState === WebSocket.OPEN) s.send(raw);
  }
}

async function appendLog(event) {
  const raw = JSON.stringify(event);
  if (USE_REDIS) {
    try {
      await redis.rpush(REDIS_LOG_KEY, raw);
      await redis.ltrim(REDIS_LOG_KEY, -MAX_HISTORY, -1);
    } catch (err) {
      log(`ERREUR    écriture historique Redis : ${err.message}`);
    }
    return;
  }
  fs.appendFile(LOG_FILE, raw + '\n', (err) => {
    if (err) log(`ERREUR    écriture ${LOG_FILE} : ${err.message}`);
  });
}

// Renvoie une Promise dans les deux cas désormais (Redis est distant par
// nature) ; les appelants qui en ont besoin (admin_subscribe) l'attendent
// via .then(), les autres continuent de l'ignorer comme avant.
async function loadTransactionHistory() {
  if (USE_REDIS) {
    try {
      const raw = await redis.lrange(REDIS_LOG_KEY, 0, -1);
      return raw.map(r => (typeof r === 'string' ? JSON.parse(r) : r)).filter(Boolean);
    } catch (err) {
      log(`ERREUR    lecture historique Redis : ${err.message}`);
      return [];
    }
  }
  if (!fs.existsSync(LOG_FILE)) return [];
  const lines = fs.readFileSync(LOG_FILE, 'utf8').split('\n').filter(Boolean);
  const tail = lines.slice(-MAX_HISTORY);
  const events = [];
  for (const line of tail) {
    try { events.push(JSON.parse(line)); } catch { /* ligne corrompue, ignorée */ }
  }
  return events;
}

// identities, links ET pending doivent survivre à un redémarrage du serveur :
// sinon un client parfaitement authentifié et déjà lié se ferait quand même
// bloquer par isLinked(), un nouveau venu pourrait re-squatter un id en TOFU
// parce que le serveur aurait "oublié" qui le détenait déjà, et pire, des
// messages chiffrés en attente de livraison seraient perdus si le serveur
// s'éteint avant que le destinataire ne se reconnecte.
async function loadState() {
  let data = null;
  if (USE_REDIS) {
    try {
      const raw = await redis.get(REDIS_STATE_KEY);
      if (raw) data = typeof raw === 'string' ? JSON.parse(raw) : raw;
    } catch (err) {
      log(`ERREUR    lecture état Redis : ${err.message}`);
    }
  } else if (fs.existsSync(STATE_FILE)) {
    data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  }
  if (!data) {
    log(`ETAT      aucun état existant (${USE_REDIS ? 'Redis' : STATE_FILE} vide) — démarrage à vide`);
    return;
  }
  for (const [id, val] of Object.entries(data.identities || {})) identities.set(id, val);
  for (const [id, val] of Object.entries(data.links || {})) {
    // Migration : l'ancien format stockait UN SEUL id de partenaire (string).
    // Le nouveau format stocke un tableau, pour permettre plusieurs contacts
    // liés par id (panel "plusieurs contacts" côté client). Les deux formats
    // sont acceptés en lecture pour ne rien casser chez les clients existants.
    const peerIds = Array.isArray(val) ? val : [val];
    links.set(id, new Set(peerIds));
  }
  for (const [id, queue] of Object.entries(data.pending || {})) {
    const normalized = (queue || []).map((item) =>
      item && item.msg ? { msg: item.msg, sentAt: item.sentAt || 0 } : { msg: item, sentAt: 0 }
    );
    pending.set(id, normalized);
  }
  const pendingCount = [...pending.values()].reduce((sum, q) => sum + q.length, 0);
  const linkCount = [...links.values()].reduce((sum, set) => sum + set.size, 0) / 2;
  log(`ETAT      chargé depuis ${STATE_FILE} (${identities.size} identité(s), ${linkCount} lien(s), ${pendingCount} message(s) en attente)`);
}

async function saveState() {
  const payload = JSON.stringify({
    identities: Object.fromEntries(identities),
    links: Object.fromEntries([...links].map(([id, set]) => [id, [...set]])),
    pending: Object.fromEntries([...pending].map(([id, q]) => [id, q.map((e) => ({ msg: e.msg, sentAt: e.sentAt }))])),
  });
  if (USE_REDIS) {
    try {
      await redis.set(REDIS_STATE_KEY, payload);
    } catch (err) {
      log(`ERREUR    sauvegarde état Redis : ${err.message}`);
    }
    return;
  }
  fs.writeFileSync(STATE_FILE, payload);
}

function log(...args) {
  console.log(`[${new Date().toLocaleTimeString()}]`, ...args);
}

// --- Actions ---

function handleRegister(socket, connState, msg) {
  const { id, pubkey, signPubkey } = msg;
  broadcastTransaction({ from: id || '?', action: 'register', data: msg });

  if (!id || !pubkey || !signPubkey) {
    socket.send(JSON.stringify({ action: 'register_denied', reason: 'champs manquants' }));
    broadcastTransaction({ from: id || '?', action: 'register_denied', status: 'error', data: { reason: 'champs manquants' } });
    return;
  }

  const known = identities.get(id);
  if (!known) {
    identities.set(id, { signPubkey });
    saveState();
    completeRegistration(socket, connState, id, pubkey);
    return;
  }

  const nonce = crypto.randomBytes(32).toString('base64');
  challenges.set(socket, { id, pubkey, nonce, expected: known.signPubkey });
  socket.send(JSON.stringify({ action: 'challenge', nonce }));
  broadcastTransaction({ from: id, action: 'challenge', data: { nonce } });
}

function handleRegisterAuth(socket, connState, msg) {
  const ch = challenges.get(socket);
  challenges.delete(socket);
  broadcastTransaction({ from: msg.id || '?', action: 'register_auth', data: msg });

  if (!ch || ch.id !== msg.id) {
    socket.send(JSON.stringify({ action: 'register_denied', reason: 'aucun challenge en cours pour cet id' }));
    broadcastTransaction({ from: msg.id || '?', action: 'register_denied', status: 'error', data: { reason: 'aucun challenge en cours pour cet id' } });
    return;
  }
  if (!verifySignature(ch.expected, ch.nonce, msg.signature)) {
    log(`ALERTE    tentative d'usurpation de "${ch.id}" détectée (signature invalide) — connexion refusée`);
    socket.send(JSON.stringify({ action: 'register_denied', reason: 'signature invalide' }));
    broadcastTransaction({ from: ch.id, action: 'register_denied', status: 'error', data: { reason: 'signature invalide (tentative d\'usurpation)' } });
    return;
  }
  completeRegistration(socket, connState, ch.id, ch.pubkey);
}

function verifySignature(signPubkeyB64, nonceB64, signatureB64) {
  try {
    const keyObj = crypto.createPublicKey({ key: Buffer.from(signPubkeyB64, 'base64'), format: 'der', type: 'spki' });
    return crypto.verify(null, Buffer.from(nonceB64, 'base64'), keyObj, Buffer.from(signatureB64, 'base64'));
  } catch {
    return false;
  }
}

function completeRegistration(socket, connState, id, pubkey) {
  const existing = users.get(id);
  if (existing && existing.socket !== socket) {
    log(`SESSION   ancienne connexion de "${id}" remplacée (nouvelle identité vérifiée avec succès)`);
    existing.socket.terminate();
  }
  users.set(id, { pubkey, socket });
  connState.myId = id;
  log(`REGISTER  ${id} authentifié — clé publique reçue (${pubkey.slice(0, 16)}...)`);
  socket.send(JSON.stringify({ action: 'register_ok', id }));
  broadcastTransaction({ from: id, action: 'register_ok', data: { id, pubkey } });
  syncLinksOnConnect(id);
  retryPendingFor(id, { force: true });
}

function handlePairRequest(msg) {
  addLink(msg.from, msg.to);
  addLink(msg.to, msg.from);
  saveState();
  log(`LIEN      ${msg.from} <-> ${msg.to} (ajouté à la table "links")`);
  broadcastTransaction({ from: msg.from, to: msg.to, action: 'pair_request', data: msg });

  notifyPaired(msg.from, msg.to);
  notifyPaired(msg.to, msg.from);
}

function addLink(id, peerId) {
  if (!links.has(id)) links.set(id, new Set());
  links.get(id).add(peerId);
}

function notifyPaired(recipientId, peerId, opts = {}) {
  const recipient = users.get(recipientId);
  const peer = users.get(peerId);
  if (!recipient) return;
  recipient.socket.send(JSON.stringify({
    action: 'paired',
    peer: peerId,
    peerPubkey: peer ? peer.pubkey : null,
    sync: !!opts.sync,
  }));
  log(`LIEN pair - ${recipientId} -> ${peerId}`);
}

function syncLinksOnConnect(id) {
  const peers = links.get(id);
  if (!peers || !peers.size) return;
  for (const peerId of peers) {
    notifyPaired(id, peerId, { sync: true });     // fait découvrir/rafraîchir peerId à id
    notifyPaired(peerId, id, { sync: true });      // rafraîchit aussi l'autre côté si déjà connecté
  }
}

function isLinked(fromId, toId) {
  return links.has(fromId) && links.get(fromId).has(toId);
}


function handleEnvelope(socket, msg) {
  const effectiveType = msg.kind && msg.kind !== 'data' ? msg.kind : (msg.type || 'data');
  socket.send(JSON.stringify({ action: 'envelope_ack', from: msg.from, to: msg.to, seq: msg.seq }));

  if (!isLinked(msg.from, msg.to)) {
    log(`REFUS     ${msg.from} -> ${msg.to} (aucun lien valide en base, message rejeté)`);
    broadcastTransaction({ from: msg.from, to: msg.to, action: 'envelope_refused', type: effectiveType, status: 'error', data: msg });
    return;
  }

  const label = msg.kind && msg.kind !== 'data'
    ? `kind="${msg.kind}"${msg.refSeq != null ? ` refSeq=${msg.refSeq}` : ''}`
    : `type="${msg.type}"`;
  log(`ROUTAGE   ${msg.from} -> ${msg.to}  ${label} seq=${msg.seq}  ` +
      `payload chiffré (${msg.ciphertext.length} car. base64, illisible ici)`);

  queueAndDeliver(msg);
  broadcastTransaction({ from: msg.from, to: msg.to, action: 'envelope', type: effectiveType, status: 'queued', data: msg });
}

function queueAndDeliver(msg) {
  const q = pending.get(msg.to) || [];
  const dup = q.find((e) => e.msg.from === msg.from && e.msg.seq === msg.seq);
  const entry = dup || { msg, sentAt: 0 };
  if (!dup) {
    q.push(entry);
    pending.set(msg.to, q);
    saveState();
  }
  attemptSend(msg.to, entry);
}

function attemptSend(id, entry) {
  const target = users.get(id);
  if (!target || target.socket.readyState !== WebSocket.OPEN) return;
  entry.sentAt = Date.now();
  target.socket.send(JSON.stringify(entry.msg));
}

function retryPendingFor(id, { force = false } = {}) {
  const q = pending.get(id);
  if (!q || !q.length) return;
  const target = users.get(id);
  if (!target || target.socket.readyState !== WebSocket.OPEN) return;
  const now = Date.now();
  let sent = 0;
  for (const entry of q) {
    if (!force && now - entry.sentAt < RETRY_INTERVAL_MS) continue;
    entry.sentAt = now;
    target.socket.send(JSON.stringify(entry.msg));
    sent++;
  }
  if (sent) log(`RETENTE   ${sent} enveloppe(s) retentée(s) vers ${id}`);
}

function handleEnvelopeAck(connState, msg) {
  const id = connState.myId;
  if (!id) return;
  const q = pending.get(id);
  if (!q) return;
  const idx = q.findIndex((e) => e.msg.from === msg.from && e.msg.seq === msg.seq);
  if (idx === -1) return;
  q.splice(idx, 1);
  if (q.length) pending.set(id, q); else pending.delete(id);
  saveState();
  broadcastTransaction({ from: msg.from, to: id, action: 'envelope_ack', data: msg });
}

function handleDisconnect(id, socket) {
  if (!id) return;
  const current = users.get(id);
  if (current && current.socket !== socket) return;
  users.delete(id);
  log(`DECONNEX  ${id}`);
  broadcastTransaction({ from: id, action: 'disconnect', data: { id } });
}

// --- Dispatch ---

function dispatch(socket, connState, msg) {
  switch (msg.action) {
    case 'admin_subscribe': {
      if (!ADMIN_TOKEN || msg.token !== ADMIN_TOKEN) {
        socket.send(JSON.stringify({ action: 'admin_denied' }));
        log('ALERTE    tentative admin_subscribe refusée (token invalide ou absent)');
        return;
      }
      socket.send(JSON.stringify({ action: 'admin_subscribed' }));
      connState.isAdmin = true;
      loadTransactionHistory().then((history) => {
        if (socket.readyState !== WebSocket.OPEN) return;
        socket.send(JSON.stringify({ action: 'admin_history', events: history }));
        adminSockets.add(socket);
      });
      break;
    }
    case 'register':
      handleRegister(socket, connState, msg);
      break;
    case 'register_auth':
      handleRegisterAuth(socket, connState, msg);
      break;
    case 'pair_request':
      handlePairRequest(msg);
      break;
    case 'envelope':
      handleEnvelope(socket, msg);
      break;
    case 'envelope_ack':
      handleEnvelopeAck(connState, msg);
      break;
    default:
      log(`INCONNU   action non reconnue: ${msg.action}`);
  }
}

function safeEqual(a, b) {
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  const hashA = crypto.createHash('sha256').update(bufA).digest();
  const hashB = crypto.createHash('sha256').update(bufB).digest();
  return crypto.timingSafeEqual(hashA, hashB);
}

function isAuthorized(req) {
  if (!ADMIN_TOKEN) return false;
  const header = req.headers['authorization'] || '';
  if (!header.startsWith('Basic ')) return false;
  const decoded = Buffer.from(header.slice(6), 'base64').toString('utf8');
  const sepIndex = decoded.indexOf(':');
  const user = sepIndex === -1 ? decoded : decoded.slice(0, sepIndex);
  const password = sepIndex === -1 ? '' : decoded.slice(sepIndex + 1);
  return safeEqual(user, ADMIN_USER) && safeEqual(password, ADMIN_TOKEN);
}

function parseMessage(raw) {
  try {
    return JSON.parse(raw.toString());
  } catch {
    return null;
  }
}

// --- Serveur ---

function startServer() {
  const httpServer = http.createServer((req, res) => {
    if (req.method === 'GET' && (req.url === '/' || req.url === '/dashboard')) {
      if (!isAuthorized(req)) {
        res.writeHead(401, { 'WWW-Authenticate': 'Basic realm="Relais monitoring"' });
        res.end('Authentification requise.');
        return;
      }
      fs.readFile(DASHBOARD_FILE, 'utf8', (err, html) => {
        if (err) { res.writeHead(500); res.end('dashboard.html introuvable'); return; }
        const withToken = html.replace('__ADMIN_TOKEN__', JSON.stringify(ADMIN_TOKEN || ''));
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(withToken);
      });
      return;
    }
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  });

  const wss = new WebSocket.Server({ server: httpServer });

  wss.on('connection', (socket) => {
    const connState = { myId: null, isAdmin: false };

    socket.isAlive = true;
    socket.on('pong', () => { socket.isAlive = true; });

    socket.on('message', (raw) => {
      const msg = parseMessage(raw);
      if (msg) dispatch(socket, connState, msg);
    });

    socket.on('close', () => {
      challenges.delete(socket);
      adminSockets.delete(socket);
      handleDisconnect(connState.myId, socket);
    });
  });

  const heartbeat = setInterval(() => {
    for (const socket of wss.clients) {
      if (socket.isAlive === false) {
        socket.terminate();
        continue;
      }
      socket.isAlive = false;
      socket.ping();
    }
  }, 30000);
  wss.on('close', () => clearInterval(heartbeat));

  const retryTimer = setInterval(() => {
    for (const id of pending.keys()) retryPendingFor(id);
  }, RETRY_INTERVAL_MS);

  httpServer.listen(PORT, () => {
    log(`Serveur démarré (port ${PORT}, stockage: ${USE_REDIS ? 'Upstash Redis' : 'disque local'})`);
    log(ADMIN_TOKEN
      ? `DASHBOARD ADMIN_TOKEN détecté — dashboard accessible sur /dashboard (identifiant: ${ADMIN_USER})`
      : 'DASHBOARD ADMIN_TOKEN absent — dashboard désactivé (401). Ajoutez-le dans l\'onglet Environment de Render.');
  });
  loadState();
}

startServer();