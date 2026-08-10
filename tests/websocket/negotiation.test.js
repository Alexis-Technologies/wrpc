'use strict';

const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');

const { WebsocketServer } = require('#ws');
const { ProtocolClient } = require('./protocolClient.js');

const KEY = Buffer.from('0123456789abcdef').toString('base64');

const BASE_HEADERS = {
  Upgrade: 'websocket',
  Connection: 'Upgrade',
  'Sec-WebSocket-Version': '13',
  'Sec-WebSocket-Key': KEY,
};

const parseStatusCode = (statusLine) => parseInt(statusLine.split(' ')[1], 10);

const withServer = async (options, run) => {
  const httpServer = http.createServer();
  const wsServer = new WebsocketServer({ server: httpServer, pingInterval: 5000, ...options });
  await new Promise((resolve) => httpServer.listen(0, resolve));
  const port = httpServer.address().port;
  try {
    await run({ port, wsServer, httpServer });
  } finally {
    await new Promise((resolve) => httpServer.close(resolve));
  }
};

test('subprotocol: server picks the first client-offered protocol it supports', async () => {
  await withServer({ protocols: ['wrpc.v2', 'wrpc.v1'] }, async ({ port, wsServer }) => {
    const connectionProtocol = new Promise((resolve) => {
      wsServer.once('connection', (ws) => resolve(ws.protocol));
    });
    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/',
      headers: { ...BASE_HEADERS, 'Sec-WebSocket-Protocol': 'other, wrpc.v1, wrpc.v2' },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 101);
    assert.strictEqual(res.headers['sec-websocket-protocol'], 'wrpc.v1');
    assert.strictEqual(await connectionProtocol, 'wrpc.v1');
  });
});

test('subprotocol: no match connects without a protocol', async () => {
  await withServer({ protocols: ['wrpc.v1'] }, async ({ port }) => {
    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/',
      headers: { ...BASE_HEADERS, 'Sec-WebSocket-Protocol': 'other' },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 101);
    assert.strictEqual(res.headers['sec-websocket-protocol'], undefined);
  });
});

test('subprotocol: handleProtocols drives the selection', async () => {
  await withServer({ handleProtocols: (offered) => offered.at(-1) }, async ({ port }) => {
    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/',
      headers: { ...BASE_HEADERS, 'Sec-WebSocket-Protocol': 'a, b, c' },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 101);
    assert.strictEqual(res.headers['sec-websocket-protocol'], 'c');
  });
});

test('subprotocol: handleProtocols returning false rejects the handshake', async () => {
  await withServer({ handleProtocols: () => false }, async ({ port }) => {
    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/',
      headers: { ...BASE_HEADERS, 'Sec-WebSocket-Protocol': 'a' },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 400);
  });
});

test('deflate: server accepts a permessage-deflate offer with no-context-takeover response', async () => {
  await withServer({ perMessageDeflate: true }, async ({ port }) => {
    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/',
      headers: { ...BASE_HEADERS, 'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits' },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 101);
    assert.strictEqual(
      res.headers['sec-websocket-extensions'],
      'permessage-deflate; server_no_context_takeover; client_no_context_takeover',
    );
  });
});

test('deflate: grammar-violating extensions header fails the handshake with 400', async () => {
  await withServer({ perMessageDeflate: true }, async ({ port }) => {
    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/',
      headers: { ...BASE_HEADERS, 'Sec-WebSocket-Extensions': 'permessage-deflate; =' },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 400);
  });
});

test('deflate: unacceptable but well-formed offer is declined, not failed', async () => {
  await withServer({ perMessageDeflate: true }, async ({ port }) => {
    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/',
      headers: { ...BASE_HEADERS, 'Sec-WebSocket-Extensions': 'permessage-deflate; unknown_param' },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 101);
    assert.strictEqual(res.headers['sec-websocket-extensions'], undefined);
  });
});

test('deflate: disabled server ignores the offer', async () => {
  await withServer({}, async ({ port }) => {
    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/',
      headers: { ...BASE_HEADERS, 'Sec-WebSocket-Extensions': 'permessage-deflate' },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 101);
    assert.strictEqual(res.headers['sec-websocket-extensions'], undefined);
  });
});

test('WebsocketServer: close() notifies peers and rejects new upgrades', async () => {
  await withServer({}, async ({ port, wsServer }) => {
    const peer = new ProtocolClient(`ws://localhost:${port}`);
    await new Promise((resolve) => peer.on('open', resolve));
    assert.strictEqual(wsServer.connections.size, 1);

    const closed = new Promise((resolve) => peer.on('close', (code, reason) => resolve({ code, reason })));
    wsServer.close({ code: 1001, reason: 'maintenance' });
    const { code, reason } = await closed;
    assert.strictEqual(code, 1001);
    assert.strictEqual(reason.toString(), 'maintenance');
    assert.strictEqual(wsServer.connections.size, 0);

    const res = await ProtocolClient.attemptHandshake({
      host: 'localhost',
      port,
      path: '/',
      headers: { ...BASE_HEADERS },
      timeoutMs: 600,
    });
    assert.strictEqual(parseStatusCode(res.statusLine), 503);

    // idempotent
    assert.doesNotThrow(() => wsServer.close());
  });
});

test('WebsocketServer: connections getter returns a snapshot', async () => {
  await withServer({}, async ({ port, wsServer }) => {
    const peer = new ProtocolClient(`ws://localhost:${port}`);
    await new Promise((resolve) => peer.on('open', resolve));
    const snapshot = wsServer.connections;
    snapshot.clear(); // mutating the snapshot must not affect the server
    assert.strictEqual(wsServer.connections.size, 1);
    peer.close();
  });
});
