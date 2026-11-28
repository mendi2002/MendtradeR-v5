'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 10000);
const DERIV_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';
const DEFAULT_SYMBOL = process.env.DEFAULT_SYMBOL || 'R_100';
const MAX_CLIENTS = Number(process.env.MAX_CLIENTS || 50);
const MAX_SYMBOLS = Number(process.env.MAX_SYMBOLS || 5);
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(x => x.trim()).filter(Boolean);

const FRONTEND_FILE = path.resolve(__dirname, '..', 'index.html');
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://rfgcsprgcbravydlnnuc.supabase.co';
const SUPABASE_PUBLISHABLE_KEY = process.env.SUPABASE_PUBLISHABLE_KEY || 'sb_publishable_kF7ycrRT90LwZCW532E3Jg_MHE03QKf';
const DERIV_CLIENT_ID = process.env.DERIV_CLIENT_ID || '34TEm2Gavyti0dcVDw73V';
const DERIV_REDIRECT_URI = process.env.DERIV_REDIRECT_URI || 'https://mendtrader-v6-integration-test.onrender.com/callback';
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || 'https://mendtrader-v6-integration-test.onrender.com';

const server = http.createServer((req, res) => {
  const requestUrl = new URL(req.url, 'http://localhost');
  const origin = req.headers.origin || '';
  const corsOrigins = new Set([FRONTEND_ORIGIN, ...ALLOWED_ORIGINS]);
  if (corsOrigins.has(origin)) {
    res.setHeader('access-control-allow-origin', origin);
    res.setHeader('vary', 'Origin');
    res.setHeader('access-control-allow-methods', 'GET, POST, OPTIONS');
    res.setHeader('access-control-allow-headers', 'authorization, content-type');
  }
  if ((requestUrl.pathname === '/oauth/deriv/token' || requestUrl.pathname === '/history') && req.method === 'OPTIONS') {
    if (!corsOrigins.has(origin)) { res.writeHead(403); return res.end(); }
    res.writeHead(204); return res.end();
  }
  if (requestUrl.pathname === '/callback' && req.method === 'GET') {
    fs.readFile(FRONTEND_FILE, (error, html) => {
      if (error) {
        res.writeHead(503, { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' });
        return res.end('A interface MendtradeR ainda não está disponível.');
      }
      res.writeHead(200, {
        'content-type': 'text/html; charset=utf-8',
        'cache-control': 'no-store',
        'referrer-policy': 'no-referrer',
        'x-content-type-options': 'nosniff'
      });
      return res.end(html);
    });
    return;
  }
  if (requestUrl.pathname === '/oauth/deriv/token' && req.method === 'POST') {
    const sendJson = (status, payload) => {
      if (res.headersSent) return;
      res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' });
      res.end(JSON.stringify(payload));
    };
    const authorization = req.headers.authorization || '';
    if (!/^Bearer\s+\S+$/i.test(authorization)) { sendJson(401, { error: 'Sessão MendtradeR necessária.' }); return; }
    let rawBody = '', bodyTooLarge = false;
    req.on('data', chunk => { rawBody += chunk.toString('utf8'); if (rawBody.length > 12000) bodyTooLarge = true; });
    req.on('end', async () => {
      if (bodyTooLarge) return sendJson(413, { error: 'Pedido demasiado grande.' });
      let payload;
      try { payload = JSON.parse(rawBody || '{}'); } catch { return sendJson(400, { error: 'Pedido inválido.' }); }
      const code = typeof payload.code === 'string' ? payload.code.trim() : '';
      const verifier = typeof payload.code_verifier === 'string' ? payload.code_verifier : '';
      if (!code || verifier.length < 43 || verifier.length > 128) return sendJson(400, { error: 'Código ou verificador PKCE inválido.' });
      if (payload.redirect_uri !== DERIV_REDIRECT_URI) return sendJson(400, { error: 'O endereço de retorno não corresponde ao registado na aplicação.' });
      try {
        const userCheck = await fetch(SUPABASE_URL + '/auth/v1/user', {
          method: 'GET', headers: { apikey: SUPABASE_PUBLISHABLE_KEY, Authorization: authorization }
        });
        if (!userCheck.ok) return sendJson(401, { error: 'A sessão MendtradeR expirou. Inicia sessão novamente.' });
        const form = new URLSearchParams({
          grant_type: 'authorization_code', client_id: DERIV_CLIENT_ID, code,
          code_verifier: verifier, redirect_uri: DERIV_REDIRECT_URI
        });
        const tokenResponse = await fetch('https://auth.deriv.com/oauth2/token', {
          method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' }, body: form.toString()
        });
        const tokenData = await tokenResponse.json().catch(() => ({}));
        if (!tokenResponse.ok || !tokenData.access_token) {
          return sendJson(400, { error: tokenData.error_description || tokenData.error || 'A Deriv não aceitou a autorização. Verifica App ID, permissões e URL de retorno.' });
        }
        return sendJson(200, { access_token: tokenData.access_token, expires_in: Number(tokenData.expires_in) || 0, token_type: tokenData.token_type || 'Bearer' });
      } catch {
        return sendJson(502, { error: 'Não foi possível contactar o serviço de autorização da Deriv. Tenta novamente.' });
      }
    });
    return;
  }
  if (requestUrl.pathname === '/history' && req.method === 'GET') {
    const sendHistory = (status, payload) => {
      if (res.headersSent) return;
      res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' });
      res.end(JSON.stringify(payload));
    };
    if (origin && !corsOrigins.has(origin)) return sendHistory(403, { error: 'Origem não autorizada.' });
    const symbol = requestUrl.searchParams.get('symbol') || DEFAULT_SYMBOL;
    const granularity = Number(requestUrl.searchParams.get('granularity') || 60);
    const count = Math.min(2000, Math.max(30, Number(requestUrl.searchParams.get('count') || 500)));
    const allowedGranularities = new Set([60, 300, 900, 1800, 3600, 14400, 86400]);
    if (!validSymbol(symbol) || !allowedGranularities.has(granularity) || !Number.isFinite(count)) {
      return sendHistory(400, { error: 'Parâmetros de histórico inválidos.' });
    }
    let upstream, settled = false, attempt = 0, lastError = null;
    const endpoints = [DERIV_URL, 'wss://ws.derivws.com/websockets/v3?app_id=' + encodeURIComponent(DERIV_CLIENT_ID)];
    const timer = setTimeout(() => finish(new Error('A Deriv não respondeu ao pedido de histórico dentro do prazo.')), 20000);
    const finish = (error, candles) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { upstream?.close(); } catch {}
      if (error) return sendHistory(502, { error: error.message || 'Falha ao consultar o histórico da Deriv.' });
      return sendHistory(200, { candles });
    };
    const connectNext = (error) => {
      if (settled) return;
      if (error) lastError = error;
      if (attempt >= endpoints.length) return finish(lastError || new Error('Não foi possível ligar ao histórico público da Deriv.'));
      const endpoint = endpoints[attempt++];
      let socket;
      try {
        socket = new WebSocket(endpoint);
        upstream = socket;
        socket.on('open', () => {
          if (settled || upstream !== socket) return;
          socket.send(JSON.stringify({
            ticks_history: symbol, adjust_start_time: 1, count, end: 'latest',
            style: 'candles', granularity, req_id: 78124
          }));
        });
        socket.on('message', raw => {
          if (settled || upstream !== socket) return;
          let data;
          try { data = JSON.parse(raw.toString()); } catch { return; }
          if (data.error) return connectNext(new Error(data.error.message || 'A Deriv recusou o histórico.'));
          if (Array.isArray(data.candles)) return finish(null, data.candles);
          if (Array.isArray(data.history?.candles)) return finish(null, data.history.candles);
        });
        socket.on('error', () => { if (!settled && upstream === socket) connectNext(new Error('Falha de ligação ao endpoint público da Deriv.')); });
        socket.on('close', () => { if (!settled && upstream === socket) connectNext(new Error('A Deriv encerrou a ligação antes de enviar os candles.')); });
      } catch (err) { connectNext(err); }
    };
    connectNext();
    return;
  }
  if (requestUrl.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    return res.end(JSON.stringify({ ok: true, service: 'MendtradeR market stream + frontend', uptime: Math.floor(process.uptime()) }));
  }
  if ((requestUrl.pathname === '/' || requestUrl.pathname === '/index.html') && (req.method === 'GET' || req.method === 'HEAD')) {
    fs.readFile(FRONTEND_FILE, (error, html) => {
      if (error) {
        res.writeHead(503, { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' });
        return res.end('A interface MendtradeR ainda não está disponível.');
      }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
      return res.end(req.method === 'HEAD' ? undefined : html);
    });
    return;
  }
  if (requestUrl.pathname === '/favicon.ico') {
    res.writeHead(204);
    return res.end();
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Não encontrado.');
});

const wss = new WebSocket.Server({ noServer: true, maxPayload: 1024 });
const streams = new Map();
const clients = new Set();

function validSymbol(value) {
  return typeof value === 'string' && /^[A-Za-z0-9_]{2,32}$/.test(value);
}

function send(client, payload) {
  if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(payload));
}

function startStream(symbol) {
  if (streams.has(symbol)) return streams.get(symbol);
  const state = { symbol, socket: null, subscriptionId: null, reconnectTimer: null, pingTimer: null, watchdogTimer: null, attempts: 0, lastTick: null, lastTickAt: 0, stopped: false };
  streams.set(symbol, state);

  const connect = () => {
    if (state.stopped) return;
    const upstream = new WebSocket(DERIV_URL);
    state.socket = upstream;
    upstream.on('open', () => {
      state.attempts = 0;
      state.lastTickAt = Date.now();
      upstream.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
      upstream.send(JSON.stringify({ ping: 1 }));
      clearInterval(state.pingTimer);
      clearInterval(state.watchdogTimer);
      state.pingTimer = setInterval(() => {
        if (upstream.readyState === WebSocket.OPEN) upstream.send(JSON.stringify({ ping: 1 }));
      }, 10000);
      // Alguns sockets podem permanecer abertos mesmo depois de pararem de entregar ticks.
      // Vigia a idade do último preço e força uma nova ligação se o fluxo ficar silencioso.
      state.watchdogTimer = setInterval(() => {
        if (state.stopped || state.socket !== upstream || upstream.readyState !== WebSocket.OPEN) return;
        const silentFor = Date.now() - (state.lastTickAt || Date.now());
        if (silentFor > 20000) {
          broadcast(symbol, { type: 'status', status: 'stale', symbol, silentFor });
          try { upstream.terminate(); } catch { try { upstream.close(); } catch {} }
        }
      }, 5000);
      broadcast(symbol, { type: 'status', status: 'connected', symbol, at: Date.now() });
    });
    upstream.on('message', raw => {
      let data;
      try { data = JSON.parse(raw.toString()); } catch { return; }
      if (data.error) {
        broadcast(symbol, { type: 'status', status: 'upstream_error', symbol, message: data.error.message || 'Erro da Deriv' });
        return;
      }
      if (data.msg_type === 'tick' && data.tick && Number.isFinite(Number(data.tick.quote))) {
        state.lastTick = data.tick;
        state.lastTickAt = Date.now();
        if (data.subscription && data.subscription.id) state.subscriptionId = data.subscription.id;
        broadcast(symbol, { type: 'tick', symbol, tick: data.tick, receivedAt: state.lastTickAt });
      }
    });
    upstream.on('close', () => {
      clearInterval(state.pingTimer);
      clearInterval(state.watchdogTimer);
      state.pingTimer = null;
      state.watchdogTimer = null;
      if (state.stopped) return;
      broadcast(symbol, { type: 'status', status: 'reconnecting', symbol });
      const delay = Math.min(30000, 1500 * Math.pow(2, Math.min(state.attempts++, 4)));
      clearTimeout(state.reconnectTimer);
      state.reconnectTimer = setTimeout(connect, delay);
    });
    upstream.on('error', () => {
      try { upstream.close(); } catch {}
    });
  };

  state.connect = connect;
  connect();
  return state;
}

function broadcast(symbol, payload) {
  for (const client of clients) {
    if (client.symbol === symbol) send(client, payload);
  }
}

server.on('upgrade', (req, socket, head) => {
  let url;
  try { url = new URL(req.url, 'http://localhost'); } catch { socket.destroy(); return; }
  if (url.pathname !== '/stream') { socket.destroy(); return; }
  const origin = req.headers.origin;
  if (ALLOWED_ORIGINS.length && origin && !ALLOWED_ORIGINS.includes(origin)) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n'); socket.destroy(); return;
  }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req, url));
});

wss.on('connection', (ws, req, url) => {
  if (clients.size >= MAX_CLIENTS) { ws.close(1013, 'Limite de ligações atingido'); return; }
  const symbol = url.searchParams.get('symbol') || DEFAULT_SYMBOL;
  if (!validSymbol(symbol)) { ws.close(1008, 'Símbolo inválido'); return; }
  if (!streams.has(symbol) && streams.size >= MAX_SYMBOLS) { ws.close(1013, 'Limite de símbolos atingido'); return; }
  ws.symbol = symbol;
  clients.add(ws);
  const stream = startStream(symbol);
  send(ws, { type: 'status', status: stream.socket && stream.socket.readyState === WebSocket.OPEN ? 'connected' : 'connecting', symbol });
  if (stream.lastTick) send(ws, { type: 'tick', symbol, tick: stream.lastTick, receivedAt: Date.now(), cached: true });
  // O navegador envia um heartbeat periódico enquanto a plataforma está aberta.
  // Isto mantém atividade de entrada no WebSocket do serviço gratuito.
  ws.on('message', raw => {
    try {
      const message = JSON.parse(raw.toString());
      if (message && message.type === 'ping') {
        send(ws, { type: 'pong', at: Date.now() });
      }
    } catch {}
  });
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

function shutdown() {
  for (const state of streams.values()) {
    state.stopped = true;
    clearTimeout(state.reconnectTimer);
    clearInterval(state.pingTimer);
    clearInterval(state.watchdogTimer);
    try { state.socket?.close(); } catch {}
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 8000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Inicia desde logo o fluxo padrão, mesmo sem navegador ligado.
startStream(DEFAULT_SYMBOL);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`MendtradeR market stream ativo na porta ${PORT}; símbolo inicial: ${DEFAULT_SYMBOL}`);
});
