'use strict';

// A connection that dies at the framing layer used to emit an 'error' and
// close, and nothing else. A server rarely listens for 'error' on an
// individual socket, so the operator's view was a connection that simply
// vanished — and the limits that closed it (maxPayload, maxBuffer,
// maxBackpressure) are theirs to raise, which they cannot do unasked.

const test = require('node:test');
const assert = require('node:assert');
const net = require('node:net');
const crypto = require('node:crypto');

const { Server, defineRouter, procedure } = require('../../index.js');

const recorder = () => {
  const entries = [];
  const push = (entry) => entries.push(entry);
  const writer = {
    level: 'debug',
    child() {
      return this;
    },
    log: push,
    info: push,
    debug: push,
    warn: push,
    error: push,
  };
  return { entries, writer, find: (event) => entries.find((entry) => entry.event === event) };
};

const router = defineRouter({ unit: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });

/** Opens a raw socket, completes the handshake, then hands it to `drive`. */
const rawUpgrade = async (t, serverOptions, drive) => {
  const { writer, find, entries } = recorder();
  const server = new Server({
    router,
    logger: writer,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    ...serverOptions,
  });
  await server.listen();
  t.after(() => server.close());
  const { port } = server.address();
  const socket = net.connect(port, '127.0.0.1');
  t.after(() => socket.destroy());
  socket.on('error', () => {});
  await new Promise((resolve) => {
    socket.once('connect', () => {
      const key = crypto.randomBytes(16).toString('base64');
      socket.write(
        `GET ${server.rpc.basePath} HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
          `Sec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`,
      );
      socket.once('data', resolve);
    });
  });
  drive(socket);
  // Long enough for the frame to be parsed and the connection failed.
  await new Promise((resolve) => setTimeout(resolve, 120));
  return { find, entries };
};

/** A client-masked frame, which is what a server requires. */
const maskedFrame = (opcode, payload) => {
  const mask = crypto.randomBytes(4);
  const masked = Buffer.from(payload.map((byte, i) => byte ^ mask[i % 4]));
  // Past 125 the length moves into its own 16-bit field.
  let header;
  if (payload.length < 126) {
    header = Buffer.from([0x80 | opcode, 0x80 | payload.length]);
  } else {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(payload.length, 2);
  }
  return Buffer.concat([header, mask, masked]);
};

test('websocket: invalid UTF-8 in a text frame reaches the server log', async (t) => {
  const { find } = await rawUpgrade(t, {}, (socket) => {
    socket.write(maskedFrame(0x1, Buffer.from([0xff, 0xfe, 0xfd])));
  });
  const entry = find('ws.frame') ?? find('ws.invalid-utf8');
  assert.ok(entry, 'the connection died with no line in the log');
  assert.match(entry.err.message, /UTF-8/);
});

test('websocket: a message past maxBuffer names the limit that closed it', async (t) => {
  // `maxBuffer` is the gate for an ordinary frame; `maxPayload` bounds a
  // COMPRESSED one after it inflates. The limit is the operator's to raise,
  // which is the whole reason the line has to say which one was hit.
  const { find, entries } = await rawUpgrade(t, { ws: { maxBuffer: 16 } }, (socket) => {
    socket.write(maskedFrame(0x1, Buffer.from('x'.repeat(600))));
  });
  // `maxBuffer` bounds both the receive queue and the assembled message,
  // so either guard may be the one that trips first; both must say so.
  const entry = find('ws.overflow') ?? find('ws.too-big') ?? find('ws.frame');
  assert.ok(
    entry,
    `a connection closed by a configured limit said nothing; saw ${JSON.stringify(entries.map((e) => e.event))}`,
  );
  assert.strictEqual(entry.max, 16, 'the entry names the limit, not just the failure');
});
