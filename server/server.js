'use strict';

const http = require('http');
const { URL } = require('url');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 10000);
const DERIV_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';
const DEFAULT_SYMBOL = process.env.DEFAULT_SYMBOL || 'R_100';
const MAX_CLIENTS = Number(process.env.MAX_CLIENTS || 50);
const MAX_SYMBOLS = Number(process.env.MAX_SYMBOLS || 5);
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(x => x.trim()).filter(Boolean);

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    return res.end(JSON.stringify({ ok: true, service: 'MendtradeR market stream', uptime: Math.floor(process.uptime()) }));
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('MendtradeR market stream is running.\n');
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
  const state = { symbol, socket: null, subscriptionId: null, reconnectTimer: null, pingTimer: null, attempts: 0, lastTick: null, stopped: false };
  streams.set(symbol, state);

  const connect = () => {
    if (state.stopped) return;
    const upstream = new WebSocket(DERIV_URL);
    state.socket = upstream;
    upstream.on('open', () => {
      state.attempts = 0;
      upstream.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
      upstream.send(JSON.stringify({ ping: 1 }));
      clearInterval(state.pingTimer);
      state.pingTimer = setInterval(() => {
        if (upstream.readyState === WebSocket.OPEN) upstream.send(JSON.stringify({ ping: 1 }));
      }, 25000);
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
        if (data.subscription && data.subscription.id) state.subscriptionId = data.subscription.id;
        broadcast(symbol, { type: 'tick', symbol, tick: data.tick, receivedAt: Date.now() });
      }
    });
    upstream.on('close', () => {
      clearInterval(state.pingTimer);
      state.pingTimer = null;
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
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

function shutdown() {
  for (const state of streams.values()) {
    state.stopped = true;
    clearTimeout(state.reconnectTimer);
    clearInterval(state.pingTimer);
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
