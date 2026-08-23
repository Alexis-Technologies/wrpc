'use strict';

/**
 * Autobahn Testsuite echo target.
 *
 * Started by `scripts/autobahn/run.js` (or manually: `node scripts/autobahn/echo-server.js`)
 * as the server the crossbario/autobahn-testsuite fuzzing client connects to.
 * Every received message is echoed back: binary messages via sendBinary(),
 * text messages via sendText().
 *
 * Backpressure: send*() returns false when the socket buffered past its
 * high-water mark, but the bytes are still queued on the socket (Node's
 * socket.write() never drops data), so no app-level echo buffer is needed.
 * Instead of buffering, on false we pause() the connection (stop reading
 * frames from the peer) and resume() on 'drain' — pure TCP backpressure,
 * which is safe because the Autobahn fuzzer reads echoes promptly.
 *
 * Options: pingInterval 30s, maxBuffer 64 MiB (Autobahn sends cases up to
 * 16 MiB), perMessageDeflate enabled (cases 12/13).
 */

const http = require('node:http');
const { WebsocketServer } = require('../../src/websocket/ws.js');

const PORT = Number(process.env.PORT ?? 9001);
const PING_INTERVAL = 30000;
const MAX_BUFFER = 64 * 1024 * 1024;

const server = http.createServer((req, res) => {
  res.writeHead(426, { 'content-type': 'text/plain' });
  res.end('WebSocket upgrade required');
});

const wss = new WebsocketServer({
  server,
  pingInterval: PING_INTERVAL,
  maxBuffer: MAX_BUFFER,
  perMessageDeflate: true,
});

wss.on('connection', (ws) => {
  let paused = false;
  ws.on('drain', () => {
    if (!paused) return;
    paused = false;
    ws.resume();
  });
  ws.on('message', (data, isBinary) => {
    const ok = isBinary ? ws.sendBinary(data) : ws.sendText(data.toString('utf8'));
    if (!ok && !paused) {
      paused = true;
      ws.pause();
    }
  });
  // The fuzzer sends intentionally malformed frames; per-connection errors
  // are expected and already answered by the protocol layer (close frames).
  ws.on('error', () => {});
});

wss.on('error', (error) => {
  console.error(`WebsocketServer error: ${error.message}`);
});

server.on('error', (error) => {
  console.error(`HTTP server error: ${error.message}`);
  process.exitCode = 1;
});

server.listen(PORT, () => {
  console.log(`Autobahn echo target listening on ws://127.0.0.1:${PORT}`);
});
