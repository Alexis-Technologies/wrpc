'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure, WrpcClient } = require('../../index.js');
const { createEncryption, generateKey } = require('../../encryption.js');
const { DEFAULT_REKEY_AFTER } = require('../../src/encryption/session.js');
const { normalizeServerEncryption } = require('../../src/encryption/server.js');
const { createUwsEngine } = require('../../uws.js');
const { FRAME_MARK, FRAME_HANDSHAKE, FRAME_SEALED } = require('../../src/wire.js');
const { MAX_QUEUED } = require('../../src/encryption/server.js');
const { aead: browserAead } = require('../../src/encryption/aead.browser.js');
const { ProtocolClient } = require('../websocket/protocolClient.js');
const { requireUws } = require('../adapters/boots.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const SECRET = 'a card number nobody on the path should read: 4111 1111 1111 1111';

const routerWith = (hooks = {}) =>
  defineRouter(
    {
      data: {
        echo: procedure({ access: 'public', handler: async (_ctx, args) => args }),
        session: procedure({
          access: 'public',
          handler: async (ctx) => {
            const info = ctx.client.encryption;
            if (info === null) return null;
            return {
              protocol: info.protocol,
              pattern: info.pattern,
              cipher: info.cipher,
              kid: info.kid,
              hash: Buffer.from(info.handshakeHash).toString('hex'),
              peer: info.remoteStatic ? Buffer.from(info.remoteStatic).toString('hex') : null,
            };
          },
        }),
        big: procedure({
          access: 'public',
          handler: async (_ctx, { rows }) => Array.from({ length: rows }, (_, i) => ({ i, name: `row-${i}` })),
        }),
        bytes: procedure({ access: 'public', handler: async (_ctx, { blob }) => ({ size: blob.length, blob }) }),
        upload: procedure({
          access: 'public',
          handler: async (ctx, { stream }) => {
            let bytes = 0;
            for await (const chunk of ctx.client.getStream(stream)) bytes += chunk.length;
            return bytes;
          },
        }),
        shout: procedure({
          access: 'public',
          handler: async (ctx, { text }) => {
            ctx.server.broadcast('data/shouted', { text });
            return true;
          },
        }),
        burst: procedure({
          access: 'public',
          handler: async (ctx, { count }) => {
            for (let i = 0; i < count; i++) ctx.client.sendEvent('data/burst', { i });
            return count;
          },
        }),
      },
    },
    { hooks },
  );

const router = routerWith();

// Every frame in both directions of every upgraded socket, as the wire saw it.
const spyWire = (server) => {
  const inbound = [];
  const outbound = [];
  const attachSocket = server.rpc.attachSocket.bind(server.rpc);
  server.rpc.attachSocket = (socket, meta) => {
    socket.on('message', (data, isBinary) => inbound.push({ isBinary, bytes: Buffer.from(data) }));
    const send = socket.send.bind(socket);
    socket.send = (data, options) => {
      outbound.push({ isBinary: typeof data !== 'string', bytes: Buffer.from(data), options });
      return send(data, options);
    };
    return attachSocket(socket, meta);
  };
  return { inbound, outbound };
};

const secure = async (t, options = {}, clientOptions = {}) => {
  const booted = await bootServer(t, { router, encryption: { keys: generateKey() }, ...options });
  const serverKey = await booted.server.rpc.encryptionKey();
  const connect = (extra = {}, encryption = {}) =>
    connectClient(t, booted.url, {
      encryption: createEncryption({ serverKey, ...encryption }),
      ...clientOptions,
      ...extra,
    });
  return { ...booted, serverKey, connect };
};

const hex = (bytes) => Buffer.from(bytes).toString('hex');

// A bare WebSocket peer: what it was closed with, after `act` had its say.
const rawClose = (url, act = () => {}) =>
  new Promise((resolve) => {
    const raw = new ProtocolClient(url);
    let close = { code: 1006, reason: '' };
    raw.on('frame', (opcode, payload) => {
      if (opcode !== 0x8) return;
      close = { code: payload.length >= 2 ? payload.readUInt16BE(0) : 1005, reason: payload.subarray(2).toString() };
    });
    raw.once('open', () => act(raw));
    raw.once('close', () => resolve(close));
  });

// A client the server hangs up on right after the upgrade: connect() may
// resolve first (an upgrade is not a session), so what is asserted is that
// the connection does not live.
const refused = async (url, options = {}) => {
  let client;
  try {
    client = await WrpcClient.connect(url, { heartbeat: false, reconnect: false, logger: false, ...options });
  } catch (error) {
    return error;
  }
  if (client.active) await new Promise((resolve) => client.once('close', resolve));
  const active = client.active;
  client.close();
  return active ? null : new Error('closed');
};

test('ws encryption: a whole session — calls, big results, bytes, a stream, an event — and nothing readable on the wire', async (t) => {
  const { server, connect } = await secure(t);
  const wire = spyWire(server);
  const client = await connect();
  await client.load('data');
  assert.deepStrictEqual(await client.api.data.echo({ note: SECRET }), { note: SECRET });
  assert.strictEqual((await client.api.data.big({ rows: 2000 })).length, 2000);
  const blob = Uint8Array.from({ length: 300 }, (_, i) => i % 251);
  const answered = await client.api.data.bytes({ blob });
  assert.strictEqual(answered.size, 300);
  assert.deepStrictEqual(Uint8Array.from(answered.blob), blob);
  const upload = client.createStream('data', 100_000);
  const uploaded = client.api.data.upload({ stream: upload.id });
  upload.write(new Uint8Array(60_000).fill(7));
  upload.write(new Uint8Array(40_000).fill(9));
  upload.end();
  assert.strictEqual(await uploaded, 100_000);
  const heard = new Promise((resolve) => client.api.data.on('shouted', resolve));
  await client.api.data.shout({ text: SECRET });
  assert.deepStrictEqual(await heard, { text: SECRET });

  const frames = [...wire.inbound, ...wire.outbound];
  assert.ok(frames.length > 10);
  for (const { isBinary, bytes } of frames) {
    assert.ok(isBinary, 'never a text frame');
    assert.strictEqual(bytes[0], FRAME_MARK);
    assert.ok(bytes[1] === FRAME_HANDSHAKE || bytes[1] === FRAME_SEALED);
    for (const needle of ['4111', 'echo', 'callback', 'shouted', 'row-1']) assert.ok(!bytes.includes(needle), needle);
  }
  const handshakes = (list) => list.filter((f) => f.bytes[1] === FRAME_HANDSHAKE).length;
  assert.deepStrictEqual([handshakes(wire.inbound), handshakes(wire.outbound)], [1, 1], 'NK is one message each way');
  assert.ok(
    wire.outbound.every((f) => f.options?.compress === false),
    'permessage-deflate is told to leave ciphertext alone',
  );
});

test('ws encryption: both ends hold the same session facts — the hash is what a credential binds to', async (t) => {
  const { connect, serverKey } = await secure(t);
  const client = await connect();
  await client.load('data');
  const seen = await client.api.data.session();
  const mine = client.encryption;
  assert.deepStrictEqual(seen, {
    protocol: 'Noise_NK_25519_AESGCM_SHA256',
    pattern: 'NK',
    cipher: 'AESGCM',
    kid: '0',
    hash: hex(mine.handshakeHash),
    peer: null,
  });
  assert.strictEqual(mine.protocol, seen.protocol);
  assert.strictEqual(Buffer.from(mine.remoteStatic).toString('base64url'), serverKey.split(':')[1]);
  assert.ok(Object.isFrozen(mine));
  // Another connection, another handshake: never the same hash twice
  const other = await connect();
  assert.notStrictEqual(hex(other.encryption.handshakeHash), hex(mine.handshakeHash));
});

test('ws encryption: optional by default — a plaintext client still connects, and is told apart', async (t) => {
  const { url, connect } = await secure(t);
  const plain = await connectClient(t, url);
  await plain.load('data');
  assert.strictEqual(await plain.api.data.session(), null);
  assert.strictEqual(plain.encryption, null);
  const sealed = await connect();
  await sealed.load('data');
  // One broadcast reaches both, each in its own clothes
  const heard = [plain, sealed].map((client) => new Promise((resolve) => client.api.data.on('shouted', resolve)));
  await sealed.api.data.shout({ text: 'to everyone' });
  assert.deepStrictEqual(await Promise.all(heard), [{ text: 'to everyone' }, { text: 'to everyone' }]);
});

test('ws encryption: required — plaintext is refused on the socket, over http, and on an unvouched transport', async (t) => {
  const { server, url, origin, connect } = await secure(t, { encryption: { keys: generateKey(), required: true } });
  assert.ok(await refused(url), 'a plaintext client does not stay connected');
  assert.deepStrictEqual(await rawClose(url), { code: 1008, reason: 'encryption' });
  const response = await fetch(`${origin}${server.rpc.basePath}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ type: 'call', id: '1', method: 'data/echo', args: { note: SECRET } }),
  });
  assert.strictEqual(response.status, 426);
  assert.ok(!(await response.text()).includes('4111'));
  // A wire the core cannot see into has to be vouched for by whoever attaches it
  const transport = { write() {}, send() {}, error() {}, close() {}, on() {}, once() {}, off() {} };
  assert.throws(() => server.rpc.attach(transport, { persistent: false }), /encryption is required/);
  server.rpc.attach(transport, { persistent: false, encrypted: true });
  const client = await connect();
  await client.load('data');
  assert.deepStrictEqual(await client.api.data.echo({ ok: 1 }), { ok: 1 });
});

test('ws encryption: a client that encrypts never settles for less', async (t) => {
  // A server that was never configured for it
  const bare = await bootServer(t, { router });
  const foreign = createEncryption({ serverKey: `0:${'A'.repeat(43)}:${'A'.repeat(43)}`, handshakeTimeout: 300 });
  assert.ok(await refused(bare.url, { encryption: foreign }));
  // The wrong pinned key: the handshake does not complete
  const { url } = await secure(t);
  const other = await secure(t);
  assert.ok(await refused(url, { encryption: createEncryption({ serverKey: other.serverKey }) }));
  // A transport that cannot carry it is refused before anything is opened — fallback candidates included
  const encryption = createEncryption({ serverKey: other.serverKey });
  class PlainTransport extends WrpcClient.transport.ws {
    static encrypts = false;
  }
  WrpcClient.transport.plain = PlainTransport;
  t.after(() => delete WrpcClient.transport.plain);
  await assert.rejects(
    WrpcClient.connect(url, { encryption, transport: 'plain' }),
    /'plain' cannot carry options\.encryption/,
  );
  await assert.rejects(
    WrpcClient.connect(url, { encryption, transport: ['ws', 'plain'] }),
    /'plain' cannot carry options\.encryption — and plaintext is not a fallback/,
  );
  await assert.rejects(WrpcClient.connect(url, { encryption: { keys: 'x' } }), /must come from createEncryption/);
  await assert.rejects(WrpcClient.connect(url, { encryption, worker: {} }), /belongs to the WrpcClientProxy/);
});

test('ws encryption: `static encrypts` is checked by deed — a transport that opens without sealing is closed before anything is said', async (t) => {
  // Not required on this server, so a plaintext socket is a session it accepts.
  const { url, server, serverKey } = await secure(t);
  const wire = spyWire(server);
  // The static is inherited; the open() is its own, and drops the option.
  class Lying extends WrpcClient.transport.ws {
    open(options) {
      return super.open({ ...options, encryption: undefined });
    }
  }
  WrpcClient.transport.lying = Lying;
  t.after(() => delete WrpcClient.transport.lying);
  const before = new Set(WrpcClient.connections);
  let presented = 0;
  const announced = [];
  const options = {
    transport: 'lying',
    encryption: createEncryption({ serverKey }),
    heartbeat: false,
    logger: false,
    authenticate: async () => void presented++,
  };
  await assert.rejects(WrpcClient.connect(url, options), (error) => {
    assert.ok(error instanceof TypeError);
    assert.match(error.message, /opened a session it did not encrypt/);
    return true;
  });
  assert.strictEqual(presented, 0, 'no credential was presented to a session nobody sealed');
  await timers.setTimeout(60);
  assert.deepStrictEqual(new Set(WrpcClient.connections), before, 'terminal: nothing is left reconnecting');
  assert.deepStrictEqual(
    wire.inbound.map((frame) => frame.bytes.toString()),
    [],
    'and not one packet left on it',
  );
  // In a fallback list the next candidate is tried — one that does seal.
  const client = await WrpcClient.connect(url, { ...options, transport: ['lying', 'ws'], reconnect: false });
  t.after(() => client.close());
  client.on('transport-fallback', (event) => announced.push(event));
  assert.strictEqual(presented, 1);
  assert.strictEqual(client.encryption?.pattern, 'NK');
});

test('ws encryption: a per-request candidate with nothing to seal a request to is refused up front, not on the day ws is down', async (t) => {
  const psk = generateKey();
  const { url } = await secure(t, { encryption: { keys: generateKey(), patterns: ['NK', 'NNpsk0'], psk } });
  // NNpsk0 has no server key: fine for a session, nothing for HPKE to seal to.
  const encryption = createEncryption({ pattern: 'NNpsk0', psk });
  await assert.rejects(
    WrpcClient.connect(url, { encryption, transport: ['ws', 'http'], logger: false }),
    (error) =>
      error instanceof TypeError &&
      /no serverKey to seal a request to — the http transport needs one/.test(error.message),
  );
  // By itself the session transport carries it.
  const client = await connectClient(t, url, { encryption, transport: ['ws'] });
  assert.strictEqual(client.encryption.pattern, 'NNpsk0');
  // And with a server key every candidate can.
  const pinned = await secure(t);
  const all = await connectClient(t, pinned.url, {
    encryption: createEncryption({ serverKey: pinned.serverKey }),
    transport: ['ws', 'http'],
  });
  assert.strictEqual(all.encryption.pattern, 'NK');
});

test('ws encryption: a server that takes the connection for a plaintext one is refused at once, not at the timeout', async (t) => {
  // The flag stripped from the URL on the way: the server answers the hello
  // with a plaintext error, and plaintext is where this client stops.
  const booted = await bootServer(t, { router, encryption: { keys: generateKey() } });
  const attachSocket = booted.server.rpc.attachSocket.bind(booted.server.rpc);
  booted.server.rpc.attachSocket = (socket, meta) => attachSocket(socket, { ...meta, url: '/' });
  const serverKey = await booted.server.rpc.encryptionKey();
  const started = Date.now();
  assert.ok(await refused(booted.url, { encryption: createEncryption({ serverKey, handshakeTimeout: 5_000 }) }));
  assert.ok(Date.now() - started < 2_000);
});

test('ws encryption: a server that never answers is given up on at the handshake timeout', async (t) => {
  const booted = await bootServer(t, { router });
  // Upgrades, then swallows everything: no answer of any kind
  booted.server.rpc.attachSocket = () => null;
  const encryption = createEncryption({ serverKey: `0:${'A'.repeat(43)}:${'A'.repeat(43)}`, handshakeTimeout: 100 });
  const started = Date.now();
  assert.ok(await refused(booted.url, { encryption }));
  assert.ok(Date.now() - started < 2_000);
});

test('ws encryption: XX — the server learns who, and authorize decides', async (t) => {
  const seen = [];
  const allowed = new Set();
  const { url, serverKey } = await secure(t, {
    encryption: {
      keys: generateKey(),
      authorize: async (peer) => {
        seen.push(peer);
        return allowed.has(hex(peer.remoteStatic));
      },
    },
  });
  const staticKey = generateKey();
  const xx = (options) => ({ encryption: createEncryption({ pattern: 'XX', staticKey, ...options }) });
  assert.ok(await refused(url, xx({ serverKey })), 'a key nobody allowed');
  assert.strictEqual(seen.length, 1);
  assert.strictEqual(seen[0].pattern, 'XX');
  allowed.add(hex(seen[0].remoteStatic));
  const client = await connectClient(t, url, xx({ serverKey }));
  await client.load('data');
  const session = await client.api.data.session();
  assert.strictEqual(session.protocol, 'Noise_XX_25519_AESGCM_SHA256');
  assert.strictEqual(session.peer, hex(seen[0].remoteStatic), 'the same static key, connection after connection');
  // Without a pin the client is asked — and a no ends it before its own key is sent
  const asked = [];
  const trusting = await connectClient(t, url, xx({ verifyServer: async (key) => asked.push(key) === 1 }));
  assert.strictEqual(Buffer.from(asked[0]).toString('base64url'), serverKey.split(':')[1]);
  assert.strictEqual(trusting.encryption.pattern, 'XX');
  await waitFor(() => seen.length === 3);
  const before = seen.length;
  assert.ok(await refused(url, xx({ verifyServer: async () => false })));
  assert.strictEqual(seen.length, before, 'the server never saw that client finish');
  // A pin that does not match the key that answered
  const elsewhere = await secure(t);
  assert.ok(await refused(url, xx({ serverKey: elsewhere.serverKey })));
});

test('ws encryption: NN and NNpsk0 are opted into by name, on both sides', async (t) => {
  const psk = generateKey();
  const { url } = await secure(t, { encryption: { keys: generateKey(), patterns: ['NK', 'NNpsk0'], psk } });
  const shared = await connectClient(t, url, { encryption: createEncryption({ pattern: 'NNpsk0', psk }) });
  await shared.load('data');
  assert.strictEqual((await shared.api.data.session()).protocol, 'Noise_NNpsk0_25519_AESGCM_SHA256');
  assert.strictEqual((await shared.api.data.session()).kid, '');
  assert.ok(
    await refused(url, { encryption: createEncryption({ pattern: 'NNpsk0', psk: generateKey() }) }),
    'another psk',
  );
  assert.ok(
    await refused(url, { encryption: createEncryption({ pattern: 'NN' }) }),
    'a pattern the server does not list',
  );
  const open = await secure(t, {
    encryption: { keys: generateKey(), patterns: ['NN'], ciphers: ['chacha20-poly1305'] },
  });
  const anonymous = await connectClient(t, open.url, {
    encryption: createEncryption({ pattern: 'NN', cipher: 'chacha20-poly1305' }),
  });
  assert.strictEqual(anonymous.encryption.protocol, 'Noise_NN_25519_ChaChaPoly_SHA256');
  assert.ok(
    await refused(open.url, { encryption: createEncryption({ pattern: 'NN' }) }),
    'a cipher the server does not list',
  );
});

test('ws encryption: an old pin keeps working while its key is on the ring, and stops when it is dropped', async (t) => {
  const [k1, k2] = [generateKey(), generateKey()];
  const before = await bootServer(t, { router, encryption: { keys: { current: 'k1', ring: { k1 } } } });
  const pinned = await before.server.rpc.encryptionKey();
  assert.ok(pinned.startsWith('k1:'));
  const rotated = await bootServer(t, { router, encryption: { keys: { current: 'k2', ring: { k1, k2 } } } });
  assert.ok((await rotated.server.rpc.encryptionKey()).startsWith('k2:'));
  const client = await connectClient(t, rotated.url, { encryption: createEncryption({ serverKey: pinned }) });
  await client.load('data');
  assert.strictEqual((await client.api.data.session()).kid, 'k1');
  const dropped = await bootServer(t, { router, encryption: { keys: { current: 'k2', ring: { k2 } } } });
  assert.ok(await refused(dropped.url, { encryption: createEncryption({ serverKey: pinned }) }));
});

test('ws encryption: a kid withdrawn from a live provider stops finishing handshakes at once — no restart', async (t) => {
  const ring = new Map([['k1', generateKey()]]);
  let current = 'k1';
  // What a KMS-backed keyring looks like: both answers read at the moment they are asked for.
  const keys = { current: () => current, get: (kid) => ring.get(kid) ?? null, kids: () => Array.from(ring.keys()) };
  const { server, url } = await bootServer(t, { router, encryption: { keys } });
  const pinned = await server.rpc.encryptionKey();
  assert.ok(pinned.startsWith('k1:'));
  const client = await connectClient(t, url, { encryption: createEncryption({ serverKey: pinned }) });
  await client.load('data');
  assert.strictEqual((await client.api.data.session()).kid, 'k1');
  // The key leaked: a new one is current and the old one is off the ring —
  // in the provider, in this same process.
  ring.set('k2', generateKey());
  current = 'k2';
  ring.delete('k1');
  assert.ok(await refused(url, { encryption: createEncryption({ serverKey: pinned }) }), 'the old pin is refused now');
  const next = await server.rpc.encryptionKey();
  assert.ok(next.startsWith('k2:'));
  const repinned = await connectClient(t, url, { encryption: createEncryption({ serverKey: next }) });
  await repinned.load('data');
  assert.strictEqual((await repinned.api.data.session()).kid, 'k2');
  // A session already established has its own keys and runs on: withdrawing
  // the static stops new handshakes, it does not reach into a live one.
  assert.deepStrictEqual(await client.api.data.echo({ still: 'here' }), { still: 'here' });
});

test('ws encryption: the server may speak first — what it sent before the handshake arrives after it, in order', async (t) => {
  const hooks = {
    onConnect: (client) => {
      for (let i = 0; i < 5; i++) client.sendEvent('data/greeting', { i });
    },
  };
  const booted = await bootServer(t, { router: routerWith(hooks), encryption: { keys: generateKey() } });
  const wire = spyWire(booted.server);
  const serverKey = await booted.server.rpc.encryptionKey();
  // Built by hand so the listener is there before the first frame is
  const Transport = WrpcClient.transport.ws;
  const options = { encryption: createEncryption({ serverKey }), heartbeat: false, reconnect: false, logger: false };
  const client = new WrpcClient(booted.url, new Transport(booted.url), options);
  t.after(() => void client.close());
  const greetings = [];
  client.on('unhandled-event', ({ name, data }) => greetings.push([name, data.i]));
  await client.open();
  await waitFor(() => greetings.length === 5);
  assert.deepStrictEqual(
    greetings,
    [0, 1, 2, 3, 4].map((i) => ['data/greeting', i]),
  );
  assert.strictEqual(wire.outbound[0].bytes[1], FRAME_HANDSHAKE, 'nothing left before the handshake answer');
  assert.ok(wire.outbound.slice(1).every((f) => f.bytes[1] === FRAME_SEALED));
});

test('ws encryption: a reconnect is a new handshake — new keys, the same pin', async (t) => {
  const { server, connect } = await secure(t);
  const client = await connect({ reconnect: { delay: 10, maxDelay: 20 } });
  await client.load('data');
  const first = hex(client.encryption.handshakeHash);
  const reconnected = new Promise((resolve) => client.once('reconnect', resolve));
  for (const peer of server.rpc.clients) peer.close();
  await reconnected;
  assert.notStrictEqual(hex(client.encryption.handshakeHash), first);
  assert.deepStrictEqual(await client.api.data.echo({ again: true }), { again: true });
});

test('ws encryption: compression happens inside the sealed frame', async (t) => {
  const { server, connect } = await secure(t, { compression: true });
  const wire = spyWire(server);
  const client = await connect({ compression: true });
  await client.load('data');
  const text = 'y'.repeat(50_000);
  assert.strictEqual((await client.api.data.echo({ text })).text.length, 50_000);
  const [peer] = [...server.rpc.clients];
  assert.strictEqual(peer.compression?.id, 'deflate-raw', 'the offer was answered — through the sealed channel');
  const largest = Math.max(...wire.inbound.map((f) => f.bytes.length));
  assert.ok(largest < 5_000, `a 50 KB call travelled as ${largest} sealed bytes`);
  assert.ok(wire.inbound.every((f) => f.bytes[1] === FRAME_HANDSHAKE || f.bytes[1] === FRAME_SEALED));
});

// What an attacker on the path can do to a sealed connection: all of it ends the connection.
test('ws encryption: a forged, plaintext or never-finished handshake is closed, with one reason on the wire', async (t) => {
  const warnings = [];
  const logger = {
    log() {},
    info() {},
    debug() {},
    error() {},
    warn: (entry) => warnings.push(entry),
    child: () => logger,
  };
  const { url } = await secure(t, { logger, encryption: { keys: generateKey(), handshakeTimeout: 150 } });
  const sealedUrl = `${url}?wrpc_e=1`;
  const unknown = Buffer.concat([Buffer.from([0, 5, 1, 4]), Buffer.from('nope'), Buffer.from([0]), Buffer.alloc(48)]);
  const closes = [
    await rawClose(sealedUrl, (raw) => raw.sendText('{"type":"ping"}')),
    await rawClose(sealedUrl, (raw) => raw.sendBinary(Buffer.from([0, 5, 1, 200]))),
    await rawClose(sealedUrl, (raw) => raw.sendBinary(Buffer.from([0, 6, 1, 2, 3]))),
    await rawClose(sealedUrl, (raw) => raw.sendBinary(unknown)),
    await rawClose(sealedUrl),
  ];
  assert.deepStrictEqual(
    closes.map((close) => [close.code, close.reason]),
    [
      [1002, 'encryption'],
      [1002, 'encryption'],
      [1002, 'encryption'],
      [1008, 'encryption'],
      [1008, 'encryption'],
    ],
  );
  assert.deepStrictEqual(
    warnings.filter((w) => w.event === 'encryption.refused').map((w) => w.reason),
    ['plaintext', 'handshake', 'handshake', 'protocol', 'timeout'],
  );
});

test("ws encryption: a refusal names the peer; a hook or a key provider that throws is this side's error, counted by outcome", async (t) => {
  const { recorder } = require('../helpers/recorder.js');
  const { createMetrics, point } = require('../helpers/metrics.js');
  const metrics = createMetrics();
  t.after(() => metrics.provider.shutdown());
  const log = recorder();
  let mode = 'ok';
  const keys = generateKey();
  const booted = await bootServer(t, {
    router,
    logger: log.writer,
    telemetry: { meter: metrics.meter },
    encryption: {
      keys,
      authorize: () => {
        if (mode === 'throw') throw new Error('directory down');
        return mode !== 'deny';
      },
    },
  });
  const serverKey = await booted.server.rpc.encryptionKey();
  const options = (encryption = {}) => ({ encryption: createEncryption({ serverKey, ...encryption }) });
  // A good one, then a denied one, then a hook that throws (an NK handshake
  // completes on the client before the server's authorize runs: refused()
  // waits for the close either way).
  const good = await connectClient(t, booted.url, options());
  assert.strictEqual(good.active, true);
  mode = 'deny';
  assert.ok(await refused(booted.url, options()), 'denied');
  mode = 'throw';
  assert.ok(await refused(booted.url, options()), 'the hook threw');
  mode = 'ok';
  // A plaintext client under a refusing server, and a key id nobody has.
  await rawClose(`${booted.url}?wrpc_e=1`, (raw) => raw.sendText('{"type":"ping"}'));
  const [, noise, hpke] = serverKey.split(':');
  assert.ok(
    await refused(booted.url, { encryption: createEncryption({ serverKey: `nope:${noise}:${hpke}` }) }),
    'unknown kid',
  );
  const lines = log.all('encryption.refused');
  assert.deepStrictEqual(
    lines.map((e) => [e.reason, e.level, e.kind, typeof e.peer]),
    [
      ['authorize', 'warn', 'ws', 'string'],
      ['hook', 'error', 'ws', 'string'],
      ['plaintext', 'warn', 'ws', 'string'],
      ['kid', 'warn', 'ws', 'string'],
    ],
  );
  assert.strictEqual(lines[1].err.message, 'directory down');
  assert.strictEqual(lines[3].kid, 'nope');
  assert.strictEqual(log.find('encryption.established').component, 'encryption');
  assert.strictEqual(typeof log.find('encryption.established').peer, 'string');
  const exported = await metrics.collect();
  const count = (outcome) =>
    point(exported, 'wrpc.server.encryption', (a) => a['wrpc.outcome'] === outcome && a['wrpc.kind'] === 'ws')?.value ??
    0;
  assert.deepStrictEqual(
    [count('established'), count('authorize'), count('hook'), count('plaintext'), count('kid')],
    [1, 1, 1, 1, 1],
  );
});

test('ws encryption: a sealed frame altered in flight, or sent twice, ends the session', async (t) => {
  const { server, connect } = await secure(t);
  const sockets = [];
  const attachSocket = server.rpc.attachSocket.bind(server.rpc);
  server.rpc.attachSocket = (socket, meta) => {
    sockets.push(socket);
    return attachSocket(socket, meta);
  };
  for (const tamper of ['flip', 'replay']) {
    const client = await connect();
    await client.load('data');
    const socket = sockets.at(-1);
    // The wrapper listens on the engine socket: hand it a frame as the wire would
    const listeners = socket.listeners('message');
    const captured = [];
    socket.prependListener('message', (data) => captured.push(Buffer.from(data)));
    await client.api.data.echo({ n: 1 });
    const closed = new Promise((resolve) => client.once('close', resolve));
    const frame = Buffer.from(captured.at(-1));
    if (tamper === 'flip') frame[frame.length - 1] ^= 1;
    for (const listener of listeners) listener(frame, true);
    await closed;
    assert.strictEqual(client.active, false, tamper);
  }
});

test('ws encryption: after a frame that does not open, nothing behind it reaches the application', async (t) => {
  const { server, connect } = await secure(t);
  // The wire, from the server's side: the first sealed frame after `arm()`
  // is altered, the frames right behind it are left alone.
  const armed = { flip: false };
  const attachSocket = server.rpc.attachSocket.bind(server.rpc);
  server.rpc.attachSocket = (socket, meta) => {
    const send = socket.send.bind(socket);
    socket.send = (data, options) => {
      if (armed.flip && typeof data !== 'string' && data[0] === FRAME_MARK && data[1] === FRAME_SEALED) {
        armed.flip = false;
        const frame = Buffer.from(data);
        frame[frame.length - 1] ^= 1;
        return send(frame, options);
      }
      return send(data, options);
    };
    return attachSocket(socket, meta);
  };
  // With a cipher over crypto.subtle the frames behind the altered one are
  // already queued, and resolve after it failed: the case the gate exists for.
  for (const [label, cipher] of [
    ['sync', undefined],
    ['async', browserAead()],
  ]) {
    const client = await connect({}, cipher === undefined ? {} : { cipher });
    await client.load('data');
    const events = [];
    client.api.data.on('burst', (data) => events.push(data.i));
    const closed = new Promise((resolve) => client.once('close', resolve));
    armed.flip = true;
    client.api.data.burst({ count: 3 }).catch(() => {});
    await closed;
    await waitFor(() => !client.active, `${label}: the client closed`);
    assert.deepStrictEqual(events, [], `${label}: nothing behind the altered frame was delivered`);
  }
});

test("ws encryption: a broadcast flood during a client's handshake does not cost it the connection", async (t) => {
  const entries = [];
  const writer = { level: 'debug', child: () => writer };
  for (const level of ['debug', 'info', 'warn', 'error']) {
    writer[level] = (entry) => entries.push({ level, ...entry });
  }
  const booted = await bootServer(t, { router, encryption: { keys: generateKey() }, logger: writer });
  const serverKey = await booted.server.rpc.encryptionKey();
  // The client's first handshake message leaves 300 ms late: the server's
  // side is attached and unsealed for that long, and everything the room
  // says meanwhile has to be held for it. What the channel delivers is
  // recorded here, below any unit: the held events land before load().
  const received = [];
  const encryption = createEncryption({ serverKey });
  const delayed = Object.freeze({
    ...encryption,
    secure: (link) =>
      encryption.secure({
        ...link,
        write: (bytes) => void setTimeout(() => link.write(bytes), 300),
        deliver: (message) => {
          if (typeof message === 'string') {
            const packet = JSON.parse(message);
            if (packet.type === 'event' && packet.name === 'data/flood') received.push(packet.data.i);
          }
          link.deliver(message);
        },
      }),
  });
  const connecting = connectClient(t, booted.url, { encryption: delayed });
  await waitFor(() => booted.server.rpc.clients.size === 1, 'the socket was attached');
  const flood = 2 * MAX_QUEUED;
  for (let i = 0; i < flood; i++) booted.server.rpc.broadcast('data/flood', { i });
  const client = await connecting;
  await client.load('data');
  // The connection lives, and works.
  assert.deepStrictEqual(await client.api.data.echo({ ok: true }), { ok: true });
  // The first MAX_QUEUED were held in order and delivered; the rest were
  // dropped for this client and said once.
  await waitFor(() => received.length >= MAX_QUEUED, 'the held broadcasts arrived');
  assert.deepStrictEqual(
    received,
    Array.from({ length: MAX_QUEUED }, (_, i) => i),
  );
  const dropped = entries.filter((e) => e.event === 'encryption.queue.dropped');
  assert.deepStrictEqual(dropped, [
    { level: 'warn', event: 'encryption.queue.dropped', count: flood - MAX_QUEUED, kind: 'ws' },
  ]);
});

test('ws encryption: a server that sends without bound before the handshake is cut off', async (t) => {
  const hooks = {
    onConnect: (client) => {
      for (let i = 0; i <= MAX_QUEUED; i++) client.sendEvent('data/flood', { i });
    },
  };
  const booted = await bootServer(t, { router: routerWith(hooks), encryption: { keys: generateKey() } });
  assert.strictEqual((await rawClose(`${booted.url}?wrpc_e=1`)).code, 1008);
});

test('ws encryption: the option is validated where the server is built', async (t) => {
  const keys = generateKey();
  const build = (encryption) => bootServer(t, { router, encryption });
  await assert.rejects(build(true), /encryption must be \{ keys/);
  await assert.rejects(build({ keys: 'short' }), /encryption\.keys must be 32 bytes/);
  await assert.rejects(build({ keys, required: 'yes' }), /required must be a boolean/);
  await assert.rejects(build({ keys, authorize: true }), /authorize must be a function/);
  await assert.rejects(build({ keys, handshakeTimeout: 0 }), /handshakeTimeout/);
  await assert.rejects(build({ keys, rekeyAfter: -1 }), /rekeyAfter/);
  await assert.rejects(build({ keys, patterns: ['IK'] }), /patterns must be a non-empty list of NN, NK, XX, NNpsk0/);
  await assert.rejects(build({ keys, patterns: [] }), /patterns must be a non-empty list/);
  await assert.rejects(build({ keys, ciphers: ['rot13'] }), /ciphers must be a non-empty list/);
  await assert.rejects(build({ keys, patterns: ['NNpsk0'] }), /NNpsk0, which needs encryption\.psk/);
  await assert.rejects(build({ keys, patterns: ['NNpsk0'], psk: 'short' }), /encryption\.psk must be 32 bytes/);
  const off = await bootServer(t, { router });
  assert.strictEqual(await off.server.rpc.encryptionKey(), null);
});

test('createEncryption: the options are validated where the object is built', () => {
  const serverKey = `k1:${'A'.repeat(43)}:${'B'.repeat(43)}`;
  assert.throws(() => createEncryption(), /a serverKey, or an explicit pattern — NN, NK, XX, NNpsk0/);
  assert.throws(() => createEncryption({ pattern: 'IK' }), /a serverKey, or an explicit pattern/);
  assert.throws(() => createEncryption({ pattern: 'NK' }), /NK needs the serverKey it pins/);
  assert.throws(() => createEncryption({ serverKey: 'k1:short:short' }), /serverKey must be a key bundle/);
  assert.throws(() => createEncryption({ serverKey: 'not a bundle' }), /must be a key bundle/);
  assert.throws(() => createEncryption({ serverKey: 7 }), /must be a key bundle/);
  assert.throws(() => createEncryption({ serverKey: { kid: 'k 1', noise: 'A', hpke: 'B' } }), /must be a key bundle/);
  assert.throws(() => createEncryption({ pattern: 'XX', serverKey }), /staticKey \(XX\) must be 32 bytes/);
  assert.throws(
    () => createEncryption({ pattern: 'XX', staticKey: generateKey() }),
    /XX needs a serverKey to pin, or verifyServer/,
  );
  assert.throws(() => createEncryption({ pattern: 'NNpsk0' }), /psk \(NNpsk0\) must be 32 bytes/);
  assert.throws(() => createEncryption({ serverKey, rekeyAfter: -1 }), /rekeyAfter/);
  // The rekey interval is not negotiated, so both ends must default to the
  // SAME constant, out of the one module — and a client can read its own
  // to compare with what a server was built with.
  assert.strictEqual(DEFAULT_REKEY_AFTER, 1 << 20);
  assert.strictEqual(createEncryption({ serverKey }).rekeyAfter, DEFAULT_REKEY_AFTER);
  assert.strictEqual(normalizeServerEncryption({ keys: generateKey() }, 'test').rekeyAfter, DEFAULT_REKEY_AFTER);
  assert.strictEqual(createEncryption({ serverKey, rekeyAfter: 8 }).rekeyAfter, 8);
  assert.throws(() => createEncryption({ serverKey, cipher: { id: 'x' } }), /cipher must be a cipher name or a Cipher/);
  assert.throws(() => createEncryption({ serverKey, dh: {} }), /dh must be a Dh/);
  const encryption = createEncryption({ serverKey: { kid: 'k1', noise: 'A'.repeat(43), hpke: new Uint8Array(32) } });
  assert.deepStrictEqual(
    [encryption.param, encryption.pattern, encryption.protocol],
    ['wrpc_e', 'NK', 'Noise_NK_25519_AESGCM_SHA256'],
  );
  assert.ok(Object.isFrozen(encryption));
});

const uws = requireUws();

test(
  'ws encryption: over the uWebSockets.js engine — the same wrapper, the same session',
  { skip: uws ? false : 'uWebSockets.js unavailable' },
  async (t) => {
    const { connect } = await secure(t, { engine: createUwsEngine({ uws }) });
    const client = await connect();
    await client.load('data');
    assert.deepStrictEqual(await client.api.data.echo({ note: SECRET }), { note: SECRET });
    assert.strictEqual((await client.api.data.big({ rows: 1500 })).length, 1500);
    assert.strictEqual((await client.api.data.session()).protocol, 'Noise_NK_25519_AESGCM_SHA256');
  },
);
