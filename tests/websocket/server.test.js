'use strict';

const test = require('node:test');
const assert = require('node:assert');
const crypto = require('node:crypto');
const http = require('node:http');
const net = require('node:net');

const { WebsocketServer, Connection } = require('#ws');
const { ProtocolClient } = require('./protocolClient.js');
const { MockSocket } = require('./mockSocket.js');
const { waitFor } = require('../helpers/wait.js');
const { recorder } = require('../helpers/recorder.js');

// A client frame built by hand (masked, under 126 bytes): `first` is the
// whole first byte, so a test can set what a client library never would.
const maskedFrame = (first, text) => {
  const payload = Buffer.from(text);
  const mask = crypto.randomBytes(4);
  const masked = Buffer.alloc(payload.length);
  for (let i = 0; i < payload.length; i++) masked[i] = payload[i] ^ mask[i % 4];
  return Buffer.concat([Buffer.from([first, 0x80 | payload.length]), mask, masked]);
};

// An upgrade request and `frames` in ONE write — what a client that does
// not wait for the 101 sends, and what node hands the 'upgrade' listener as
// `head`. `closed` resolves with every byte the server sent once it hung up;
// a Close from the server is answered, so nobody waits out a close timeout.
const rawUpgrade = (port, frames) => {
  const socket = net.connect(port, '127.0.0.1');
  const key = crypto.randomBytes(16).toString('base64');
  const request =
    `GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
    `Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ${key}\r\n\r\n`;
  const chunks = [];
  let answered = false;
  socket.on('data', (chunk) => {
    chunks.push(chunk);
    const bytes = Buffer.concat(chunks);
    // Server frames are unmasked, and these are short: one length byte.
    for (let at = bytes.indexOf('\r\n\r\n') + 4; at > 3 && at + 1 < bytes.length; at += 2 + (bytes[at + 1] & 0x7f)) {
      if ((bytes[at] & 0x0f) !== 0x08 || answered) continue;
      answered = true;
      socket.end(maskedFrame(0x88, ''));
    }
  });
  socket.on('error', () => {});
  const closed = new Promise((resolve) => socket.on('close', () => resolve(Buffer.concat(chunks))));
  socket.write(Buffer.concat([Buffer.from(request), ...frames]));
  return { socket, closed };
};

// What every shell does: an engine bound to an http server, and no 'error'
// listener on the WebsocketServer.
const bootBound = async (t, options = {}) => {
  const httpServer = http.createServer();
  const wsServer = new WebsocketServer({ server: httpServer, pingInterval: 5000, logger: false, ...options });
  await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    wsServer.close();
    httpServer.closeAllConnections();
    await new Promise((resolve) => httpServer.close(resolve));
  });
  return { wsServer, port: httpServer.address().port };
};

test('WebsocketServer: frames sent with the upgrade request are read once the connection is listened to', async (t) => {
  const { wsServer, port } = await bootBound(t);
  const received = [];
  wsServer.on('connection', (ws) => ws.on('message', (data) => received.push(String(data))));
  const raw = rawUpgrade(port, [maskedFrame(0x81, 'first'), maskedFrame(0x81, 'second')]);
  t.after(() => raw.socket.destroy());
  // They used to be parsed inside the Connection constructor, before the
  // 'connection' event: emitted to nobody, and lost.
  await waitFor(() => received.length === 2, 'the frames behind the handshake never arrived');
  assert.deepStrictEqual(received, ['first', 'second'], 'both, in order');
});

test('WebsocketServer: a frame that breaks the protocol, sent with the upgrade request, ends that connection only', async (t) => {
  const { port } = await bootBound(t);
  // RSV1 on a connection that negotiated no extension (RFC 6455 5.2): the
  // constructor's 'error' had no listener and threw, handleUpgrade re-emitted
  // it on a server with none, and the throw left the http server's 'upgrade'
  // listener — the process.
  const raw = rawUpgrade(port, [maskedFrame(0xc1, 'rsv1')]);
  t.after(() => raw.socket.destroy());
  const bytes = await raw.closed;
  assert.match(bytes.toString('latin1'), /^HTTP\/1\.1 101 /);
  const frames = bytes.subarray(bytes.indexOf('\r\n\r\n') + 4);
  assert.strictEqual(frames[0], 0x88, 'a Close frame');
  assert.strictEqual(frames.readUInt16BE(2), 1002, 'protocol error');
  const peer = new ProtocolClient(`ws://127.0.0.1:${port}`);
  await new Promise((resolve) => peer.on('open', resolve));
  peer.close();
});

test('Connection: a bad frame in the head is reported to whoever listens after construction', async () => {
  const socket = new MockSocket();
  // A test double has no `unshift`: the head is read on the next tick.
  const connection = new Connection(socket, maskedFrame(0xc1, 'rsv1'));
  const failed = new Promise((resolve) => connection.once('error', resolve));
  assert.match((await failed).message, /RSV/);
});

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

test('WebsocketServer: handleUpgrade answers 500 when the handshake throws — with no error listener too', async () => {
  const { writer, find } = recorder();
  const quiet = new WebsocketServer({ pingInterval: 50, logger: writer });
  const answered = [];
  const throwing = {
    get httpVersion() {
      throw new Error('boom');
    },
  };
  const bare = {
    on: () => {},
    write: (data) => void answered.push(String(data)),
    destroy: () => {},
    cork: () => {},
    uncork: () => {},
  };
  // No 'error' listener: the emit used to throw out of handleUpgrade.
  assert.doesNotThrow(() => quiet.handleUpgrade(throwing, bare, Buffer.alloc(0)));
  assert.match(answered.join(''), /^HTTP\/1\.1 500 Internal Server Error/);
  assert.strictEqual(find('ws.upgrade').level, 'error');
  assert.strictEqual(find('ws.upgrade').err.message, 'boom');
  quiet.close();

  const wsServer = new WebsocketServer({ pingInterval: 50 });
  wsServer.on('error', () => {}); // a listener still hears it
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
