'use strict';

const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');

const { WebsocketServer } = require('#ws');
const { ProtocolClient } = require('./protocolClient.js');

test('WebsocketServer: accepts new connection after socket error', async () => {
  const httpServer = http.createServer();
  void new WebsocketServer({
    server: httpServer,
    pingInterval: 50,
  });

  await new Promise((resolve) => httpServer.listen(0, resolve));
  const port = httpServer.address().port;

  const first = new ProtocolClient(`ws://localhost:${port}`);
  await new Promise((resolve) => first.on('open', resolve));
  first.socket.destroy();

  await new Promise((resolve) => setTimeout(resolve, 150));

  const second = new ProtocolClient(`ws://localhost:${port}`);
  const opened = await new Promise((resolve) => {
    second.on('open', () => resolve(true));
    second.on('close', () => resolve(false));
  });

  assert.strictEqual(opened, true);
  second.close();
  await new Promise((resolve) => httpServer.close(resolve));
});

test('WebsocketServer: heartbeat terminates a dead peer and keeps pinging others', async () => {
  const httpServer = http.createServer();
  void new WebsocketServer({ server: httpServer, pingInterval: 50 });

  await new Promise((resolve) => httpServer.listen(0, resolve));
  const port = httpServer.address().port;

  // ProtocolClient never answers pings, so the server must terminate it
  // between one and two ping intervals.
  const dead = new ProtocolClient(`ws://localhost:${port}`);
  await new Promise((resolve) => dead.on('open', resolve));
  await new Promise((resolve) => dead.socket.once('close', resolve));

  // The heartbeat interval must survive the termination: a fresh peer
  // still receives pings on the next ticks.
  const live = new ProtocolClient(`ws://localhost:${port}`);
  await new Promise((resolve) => live.on('open', resolve));
  await new Promise((resolve) => live.on('ping', resolve));

  live.close();
  await new Promise((resolve) => httpServer.close(resolve));
});

test('WebsocketServer: heartbeat tick survives a peer with no close event', (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] });
  const { EventEmitter } = require('node:events');
  const { MockSocket } = require('./mockSocket.js');

  const fakeServer = new EventEmitter();
  void new WebsocketServer({ server: fakeServer, pingInterval: 20 });

  const socket = new MockSocket();
  const req = {
    httpVersion: '1.1',
    method: 'GET',
    url: '/',
    headers: {
      host: 'localhost',
      upgrade: 'websocket',
      connection: 'Upgrade',
      'sec-websocket-version': '13',
      'sec-websocket-key': Buffer.from('0123456789abcdef').toString('base64'),
    },
  };
  fakeServer.emit('upgrade', req, socket, Buffer.alloc(0));

  // Simulate a socket that died without ever emitting 'close': terminate()
  // becomes a no-op, so only the heartbeat tick itself can clean the maps.
  socket.destroyed = true;

  assert.doesNotThrow(() => t.mock.timers.tick(20)); // ping sent, awaiting = true
  assert.doesNotThrow(() => t.mock.timers.tick(20)); // dead-peer tick: terminate + full cleanup
  assert.doesNotThrow(() => t.mock.timers.tick(20)); // regression: used to TypeError on stale entry

  fakeServer.emit('close');
});

test('WebsocketServer: heartbeat spares paused connections and resumes checks after resume', (t) => {
  t.mock.timers.enable({ apis: ['setInterval'] });
  const { EventEmitter } = require('node:events');
  const { MockSocket } = require('./mockSocket.js');

  const fakeServer = new EventEmitter();
  const wsServer = new WebsocketServer({ server: fakeServer, pingInterval: 20 });
  let conn = null;
  wsServer.on('connection', (ws) => (conn = ws));

  const socket = new MockSocket();
  const req = {
    httpVersion: '1.1',
    method: 'GET',
    url: '/',
    headers: {
      host: 'localhost',
      upgrade: 'websocket',
      connection: 'Upgrade',
      'sec-websocket-version': '13',
      'sec-websocket-key': Buffer.from('0123456789abcdef').toString('base64'),
    },
  };
  fakeServer.emit('upgrade', req, socket, Buffer.alloc(0));

  t.mock.timers.tick(20); // ping sent, awaiting = true
  conn.pause(); // backpressure kicks in before the pong could be read
  t.mock.timers.tick(20); // would have terminated pre-fix: awaiting still true
  t.mock.timers.tick(20);
  assert.strictEqual(socket.destroyed, false);

  conn.resume(); // still no pong read: the next tick may terminate again
  t.mock.timers.tick(20);
  assert.strictEqual(socket.destroyed, true);

  fakeServer.emit('close');
});

test('WebsocketServer: an explicit server must be an http(s) server instance', () => {
  assert.throws(() => new WebsocketServer({ server: {} }), /options\.server must be an http\.Server/);
  assert.throws(() => new WebsocketServer({ server: null }), /options\.server must be an http\.Server/);
});

test('WebsocketServer: without a server, upgrades are driven by handleUpgrade', async () => {
  // Middleware adapters (express) own the 'upgrade' listener themselves.
  const wsServer = new WebsocketServer({ pingInterval: 50 });
  const httpServer = http.createServer();
  httpServer.on('upgrade', (req, socket, head) => {
    wsServer.handleUpgrade(req, socket, head);
  });
  await new Promise((resolve) => httpServer.listen(0, resolve));
  const { port } = httpServer.address();

  const connected = new Promise((resolve) => wsServer.once('connection', resolve));
  const peer = new ProtocolClient(`ws://127.0.0.1:${port}`);
  await new Promise((resolve) => peer.on('open', resolve));
  const conn = await connected;

  const echoed = new Promise((resolve) => peer.once('message', resolve));
  conn.sendText('manual upgrade');
  assert.strictEqual((await echoed).toString(), 'manual upgrade');

  peer.close();
  wsServer.close();
  await new Promise((resolve) => httpServer.close(resolve));
});

test('WebsocketServer: handleUpgrade answers 500 when the handshake throws', async () => {
  const wsServer = new WebsocketServer({ pingInterval: 50 });
  wsServer.on('error', () => {}); // otherwise EventEmitter rethrows
  const written = [];
  const socket = {
    on: () => {},
    // A getter that throws inside #handleUpgrade's header inspection
    write: (data) => void written.push(String(data)),
    destroy: () => {},
    cork: () => {},
    uncork: () => {},
  };
  const req = {
    get httpVersion() {
      throw new Error('boom');
    },
  };
  wsServer.handleUpgrade(req, socket, Buffer.alloc(0));
  assert.match(written.join(''), /^HTTP\/1\.1 500 Internal Server Error/);
  wsServer.close();
});

test('WebsocketServer: forwards http server errors when it has its own listeners', async () => {
  const httpServer = http.createServer();
  const wsServer = new WebsocketServer({ server: httpServer, pingInterval: 50 });
  await new Promise((resolve) => httpServer.listen(0, resolve));

  const forwarded = new Promise((resolve) => wsServer.once('error', resolve));
  httpServer.emit('error', new Error('boom'));
  const error = await forwarded;
  assert.strictEqual(error.message, 'boom');

  await new Promise((resolve) => httpServer.close(resolve));
});

test('WebsocketServer: does not forward http server errors already handled elsewhere', async () => {
  const httpServer = http.createServer();
  httpServer.on('error', () => {}); // an external listener: httpHasOtherListeners = true
  void new WebsocketServer({ server: httpServer, pingInterval: 50 });
  await new Promise((resolve) => httpServer.listen(0, resolve));

  // wsServer has no 'error' listeners of its own. Node's EventEmitter throws
  // synchronously on an 'error' emit with zero listeners, so if the guard in
  // #init forwarded here anyway, this call would throw.
  assert.doesNotThrow(() => httpServer.emit('error', new Error('already handled')));

  await new Promise((resolve) => httpServer.close(resolve));
});
