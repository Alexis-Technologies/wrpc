'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { RpcServer, defineRouter, procedure } = require('../../index.js');
const { MemoryBackplane, createRedisAdapter } = require('../../scaling.js');
const { generateKey } = require('../../encryption.js');
const { createEnvelope, isSealedEnvelope, SEALED_PREFIX } = require('../../src/rpc/envelope.js');
const {
  normalizeEnvelopeEncryption,
  createEnvelopeSealer,
  ReplayWindow,
  HEADER_LENGTH,
  MAX_SENDERS,
} = require('../../src/encryption/envelope.js');
const { OpenError } = require('../../src/encryption/contracts.js');
const { waitFor } = require('../helpers/server.js');
const { FakeRedis } = require('./fakeRedis.js');

const router = defineRouter({ test: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });

const spied = (backplane) => {
  const published = [];
  const publish = backplane.publish.bind(backplane);
  backplane.publish = (channel, message) => {
    published.push({ channel, message });
    return publish(channel, message);
  };
  return published;
};

class FakeSocket {
  constructor() {
    this.sent = [];
    this.listeners = new Map();
    this.remoteAddress = '127.0.0.1';
  }

  on(event, listener) {
    const list = this.listeners.get(event) ?? [];
    list.push(listener);
    this.listeners.set(event, list);
  }

  once(event, listener) {
    this.on(event, listener);
  }

  emit(event, ...args) {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }

  send(data) {
    this.sent.push(JSON.parse(data));
    return true;
  }

  close() {
    this.emit('close');
  }

  terminate() {
    this.emit('close');
  }

  get events() {
    return this.sent.filter((packet) => packet.type === 'event');
  }
}

const logs = () => {
  const warnings = [];
  const log = {
    log() {},
    info() {},
    debug() {},
    error() {},
    warn: (entry) => warnings.push(entry),
    child() {
      return log;
    },
  };
  return { log, warnings };
};

const instance = (t, backplane, options = {}, logger = false) => {
  const rpc = new RpcServer({ router, backplane, logger, sse: false, ...options });
  t.after(() => rpc.close());
  const socket = new FakeSocket();
  const client = rpc.attachSocket(socket, {});
  client.join('lobby');
  return { rpc, socket, client };
};

const SECRET = { card: '4111 1111 1111 1111', note: 'not for the operator of the Redis' };
const bigData = { rows: Array.from({ length: 200 }, (_, i) => ({ i, name: `row-${i}` })) };

// Two sealers under one keyring — two instances — and what they were built with.
const KEYRINGS = new WeakMap();
const keysOf = (sealer) => KEYRINGS.get(sealer);
const sealerPair = (options = {}, layer = 'rooms') => {
  const encryption = normalizeEnvelopeEncryption({ keys: generateKey(), ...options }, 'x');
  const pair = [createEnvelopeSealer({ encryption, layer }), createEnvelopeSealer({ encryption, layer })];
  for (const sealer of pair) KEYRINGS.set(sealer, encryption.keys);
  return [...pair, encryption];
};

test('rooms backplane: what the backplane carries is ciphertext, and the other instance still delivers it', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const rooms = { encryption: { keys: generateKey() } };
  const a = instance(t, backplane, { rooms });
  const b = instance(t, backplane, { rooms });
  await timers.setTimeout(10);
  a.rpc.to('lobby').emit('payment', SECRET);
  await waitFor(() => b.socket.events.length === 1);
  assert.deepStrictEqual(b.socket.events[0].data, SECRET);
  // Delivered once on the sender too: locally, never a second time off its own echo
  await timers.setTimeout(10);
  assert.strictEqual(a.socket.events.length, 1);
  const wire = published.filter((m) => m.channel.includes('lobby'));
  assert.strictEqual(wire.length, 1);
  assert.ok(wire[0].message.startsWith('wrpc-sealed:0:'));
  assert.strictEqual(isSealedEnvelope(wire[0].message), true);
  for (const needle of ['4111', 'payment', 'operator', 'lobby', 'instance']) {
    assert.ok(!wire[0].message.includes(needle), needle);
    assert.ok(!Buffer.from(wire[0].message.split(':')[2], 'base64').includes(needle), needle);
  }
});

test('rooms backplane: over the Redis adapter as well — a string carrier', async (t) => {
  const redis = new FakeRedis();
  const adapter = () => createRedisAdapter({ pub: redis.duplicate(), logger: false });
  const rooms = { encryption: { keys: generateKey(), cipher: 'chacha20-poly1305' } };
  const a = instance(t, adapter(), { rooms });
  const b = instance(t, adapter(), { rooms });
  await timers.setTimeout(20);
  a.rpc.to('lobby').emit('payment', SECRET);
  await waitFor(() => b.socket.events.length === 1);
  assert.deepStrictEqual(b.socket.events[0].data, SECRET);
});

test('rooms backplane: compress, then seal — one frame, one base64, the codec named inside', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const rooms = { compression: true, encryption: { keys: generateKey() } };
  const a = instance(t, backplane, { rooms });
  const b = instance(t, backplane, { rooms });
  await timers.setTimeout(10);
  a.rpc.to('lobby').emit('big', bigData);
  a.rpc.to('lobby').emit('small', { x: 1 });
  await waitFor(() => b.socket.events.length === 2);
  assert.deepStrictEqual(
    b.socket.events.map((e) => e.data),
    [bigData, { x: 1 }],
  );
  const wire = published.filter((m) => m.channel.includes('lobby')).map((m) => m.message);
  assert.ok(wire.every((message) => message.startsWith(SEALED_PREFIX)));
  assert.ok(!wire[0].includes('wrpc-enc:'), 'no marker inside a marker');
  assert.ok(wire[0].length < JSON.stringify(bigData).length / 3, 'the large envelope was compressed before sealing');
});

test('rooms backplane: an envelope does not open under another key, on another channel, or twice', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const keys = generateKey();
  const a = instance(t, backplane, { rooms: { encryption: { keys } } });
  const { log, warnings } = logs();
  const b = instance(t, backplane, { rooms: { encryption: { keys } } }, log);
  const stranger = logs();
  const c = instance(t, backplane, { rooms: { encryption: { keys: generateKey() } } }, stranger.log);
  b.client.join('vault');
  await timers.setTimeout(10);
  a.rpc.to('lobby').emit('payment', SECRET);
  await waitFor(() => b.socket.events.length === 1);
  await waitFor(() => stranger.warnings.some((w) => w.event === 'backplane.open'));
  assert.strictEqual(c.socket.events.length, 0);
  assert.deepStrictEqual(
    stranger.warnings.find((w) => w.event === 'backplane.open'),
    { event: 'backplane.open', channel: 'room:lobby', reason: 'open', kid: '0' },
  );

  // What an attacker with the Redis can do: replay it, move it, flip it
  const { channel, message } = published.find((m) => m.channel.includes('lobby'));
  const flipped = message.slice(0, -6) + (message.at(-6) === 'A' ? 'B' : 'A') + message.slice(-5);
  await backplane.publish(channel, message);
  await backplane.publish(channel.replace('lobby', 'vault'), message);
  await backplane.publish(channel, flipped);
  await backplane.publish(channel, message.replace('wrpc-sealed:0:', 'wrpc-sealed:nope:'));
  await backplane.publish(channel, 'wrpc-sealed:AAAA');
  await backplane.publish(channel, 'wrpc-sealed:0:AAAA');
  await waitFor(() => warnings.filter((w) => w.event === 'backplane.open').length === 6);
  assert.deepStrictEqual(
    warnings.filter((w) => w.event === 'backplane.open').map((w) => w.reason),
    ['replay', 'open', 'open', 'kid', 'format', 'format'],
  );
  assert.strictEqual(b.socket.events.length, 1, 'nothing but the original was delivered');
});

test('rooms backplane: plaintext is refused where encryption is on, and named where it is off', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const sealedLog = logs();
  const sealing = instance(t, backplane, { rooms: { encryption: { keys: generateKey() } } }, sealedLog.log);
  const plainLog = logs();
  const plain = instance(t, backplane, {}, plainLog.log);
  const packedLog = logs();
  const packed = instance(t, backplane, { rooms: { compression: true } }, packedLog.log);
  await timers.setTimeout(10);
  plain.rpc.to('lobby').emit('forged', { admin: true });
  await waitFor(() => sealedLog.warnings.some((w) => w.event === 'backplane.unsealed'));
  assert.strictEqual(sealing.socket.events.length, 0, 'a plaintext envelope is not delivered by a sealing instance');
  sealing.rpc.to('lobby').emit('payment', SECRET);
  await waitFor(() => plainLog.warnings.some((w) => w.event === 'backplane.sealed'));
  // A compression codec passes through what is not its own; the instance still names it
  await waitFor(() => packedLog.warnings.some((w) => w.event === 'backplane.sealed'));
  assert.strictEqual(plain.socket.events.length, 1, 'only its own emit');
  assert.strictEqual(packed.socket.events.length, 1, "only the plain instance's emit");
});

test("rooms backplane: a key provider that throws while opening is this side's error line, with the err and the kid", async (t) => {
  const { recorder } = require('../helpers/recorder.js');
  const backplane = new MemoryBackplane({ logger: false });
  const master = generateKey();
  const a = instance(t, backplane, { rooms: { encryption: { keys: { current: 'a', ring: { a: master } } } } });
  const log = recorder();
  let down = false;
  const flaky = {
    current: () => 'a',
    get: (kid) => {
      if (down) throw new Error('vault unreachable');
      return kid === 'a' ? master : null;
    },
  };
  const b = instance(t, backplane, { rooms: { encryption: { keys: flaky } } }, log.writer);
  await timers.setTimeout(10);
  // Down before the first envelope: a key a receiver already derived for a
  // sender's salt is cached, and the provider is not asked again for it.
  down = true;
  a.rpc.to('lobby').emit('payment', { n: 1 });
  await waitFor(() => log.all('backplane.keys').length === 1);
  const line = log.find('backplane.keys');
  assert.deepStrictEqual(
    [line.level, line.err.message, line.kid, line.channel],
    ['error', 'vault unreachable', 'a', 'room:lobby'],
  );
  assert.strictEqual(log.all('backplane.open').length, 0, 'not a refusal without a reason');
  assert.strictEqual(b.socket.events.length, 0);
  down = false;
  a.rpc.to('lobby').emit('payment', { n: 2 });
  await waitFor(() => b.socket.events.length === 1, 'the provider recovered');
});

test('rooms backplane: a sealer that cannot seal keeps the event local and names it — never plaintext', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  let current = 'a';
  const held = { a: generateKey() };
  const keys = { current: () => current, get: (kid) => held[kid] ?? null };
  const errors = [];
  const log = {
    log() {},
    info() {},
    debug() {},
    warn() {},
    error: (entry) => errors.push(entry),
    child() {
      return log;
    },
  };
  const sealing = instance(t, backplane, { rooms: { encryption: { keys } } }, log);
  const other = instance(t, backplane, { rooms: { encryption: { keys } } });
  await timers.setTimeout(10);
  sealing.rpc.to('lobby').emit('before', { n: 1 });
  await waitFor(() => other.socket.events.length === 1);
  // The provider rotated to a key it does not hold: emit() is not a throw
  // into the handler, the event reaches the local members, and nothing —
  // plaintext least of all — crosses the backplane.
  current = 'gone';
  assert.doesNotThrow(() => sealing.rpc.to('lobby').emit('payment', SECRET));
  await timers.setTimeout(20);
  assert.deepStrictEqual(
    errors.map((entry) => entry.event),
    ['backplane.seal'],
  );
  assert.match(errors[0].err.message, /current key "gone"/);
  assert.strictEqual(sealing.socket.events.length, 2, 'delivered locally');
  assert.strictEqual(other.socket.events.length, 1, 'and nowhere else');
  assert.ok(!JSON.stringify(published).includes('4111'), 'nothing plaintext crossed the backplane');
});

test('rooms backplane: the rollout is three deploys and loses nothing', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const keys = generateKey();
  const stage = (encryption) => ({ rooms: encryption ? { encryption: { keys, ...encryption } } : {} });
  const every = logs();
  // Deploy 1 next to the old fleet: everyone can OPEN, nobody seals yet
  const old = instance(t, backplane, stage(null), every.log);
  const one = instance(t, backplane, stage({ seal: false, acceptPlaintext: true }), every.log);
  // Deploy 2: sealing, still reading the plaintext of those not there yet
  const two = instance(t, backplane, stage({ acceptPlaintext: true }), every.log);
  await timers.setTimeout(10);
  old.rpc.to('lobby').emit('from-old', { n: 0 });
  one.rpc.to('lobby').emit('from-one', { n: 1 });
  await waitFor(() => [old, one, two].every((node) => node.socket.events.length === 2));
  two.rpc.to('lobby').emit('from-two', { n: 2 });
  await waitFor(() => one.socket.events.length === 3 && two.socket.events.length === 3);
  const wire = published.filter((m) => m.channel.includes('lobby')).map((m) => m.message[0]);
  assert.deepStrictEqual(wire, ['{', '{', 'w'], 'deploy 1 sends plaintext, deploy 2 seals');
  // The only loss is the one the order of the rollout rules out: a sealed
  // envelope reaching an instance that was never taught to open it.
  assert.deepStrictEqual(
    every.warnings.map((w) => w.event),
    ['backplane.sealed'],
  );
});

test('rooms backplane: a key rotation — add, make current, drop', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const [k1, k2] = [generateKey(), generateKey()];
  const rooms = (keys) => ({ rooms: { encryption: { keys } } });
  const old = instance(t, backplane, rooms({ current: 'k1', ring: { k1, k2 } }));
  const next = instance(t, backplane, rooms({ current: 'k2', ring: { k1, k2 } }));
  const { log, warnings } = logs();
  const dropped = instance(t, backplane, rooms({ current: 'k2', ring: { k2 } }), log);
  await timers.setTimeout(10);
  old.rpc.to('lobby').emit('under-k1', { n: 1 });
  next.rpc.to('lobby').emit('under-k2', { n: 2 });
  await waitFor(() => old.socket.events.length === 2 && next.socket.events.length === 2);
  await waitFor(() => warnings.length === 1);
  assert.deepStrictEqual(
    published.filter((m) => m.channel.includes('lobby')).map((m) => m.message.split(':')[1]),
    ['k1', 'k2'],
  );
  assert.deepStrictEqual(warnings[0], { event: 'backplane.open', channel: 'room:lobby', reason: 'kid', kid: 'k1' });
  assert.deepStrictEqual(
    dropped.socket.events.map((e) => e.name),
    ['under-k2'],
  );
});

test('cluster: signed, compressed, sealed — presence, sendTo and asks cross instances as ciphertext', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const published = spied(backplane);
  const cluster = {
    secret: 's3cret',
    compression: { threshold: 0 },
    encryption: { keys: generateKey() },
    presenceInterval: 50,
  };
  const a = instance(t, backplane, { cluster });
  const b = instance(t, backplane, { cluster });
  await waitFor(() => a.rpc.cluster.count('lobby') === 2 && b.rpc.cluster.count('lobby') === 2);
  a.rpc.sendTo(b.client.id, 'direct', SECRET);
  await waitFor(() => b.socket.events.some((e) => e.name === 'direct'));
  assert.deepStrictEqual(b.socket.events.find((e) => e.name === 'direct').data, SECRET);
  const wire = published.filter((m) => !m.channel.includes('room:'));
  assert.ok(wire.length > 2);
  assert.ok(wire.every((m) => m.message.startsWith('wrpc-sealed:0:')));
  assert.ok(wire.every((m) => !m.message.includes('4111') && !m.message.includes('s3cret')));

  // An envelope lifted from the shared channel onto a node's own does not open there
  const { log, warnings } = logs();
  const c = instance(t, backplane, { cluster }, log);
  await waitFor(() => c.rpc.cluster.count('lobby') === 3);
  const shared = published.find((m) => m.channel.endsWith('cluster'));
  const own = published.find((m) => m.channel !== shared.channel && !m.channel.includes('room:'));
  await backplane.publish(own.channel, shared.message);
  // A node without the keys says what it is looking at
  const outsider = logs();
  instance(t, backplane, { cluster: { secret: 's3cret', presenceInterval: 50 } }, outsider.log);
  await waitFor(() => outsider.warnings.some((w) => w.event === 'cluster.sealed'));
  assert.ok(warnings.every((w) => w.event !== 'cluster.badsig'));
});

test('cluster: a plaintext command is refused by a sealing node — the HMAC-less case the file warns about', async (t) => {
  const backplane = new MemoryBackplane({ logger: false });
  const { log, warnings } = logs();
  const node = instance(t, backplane, { cluster: { encryption: { keys: generateKey() } } }, log);
  await timers.setTimeout(10);
  const forged = { v: 1, from: 'evil', epoch: 'x', t: 'cmd', op: 'disconnect', sel: { all: true } };
  await backplane.publish('wrpc:cluster', JSON.stringify(forged));
  await backplane.publish('cluster', JSON.stringify(forged));
  await waitFor(() => warnings.some((w) => w.event === 'cluster.unsealed'), 'the refusal was logged');
  assert.strictEqual(node.rpc.clients.size, 1, 'nobody was disconnected');
});

test('sealer: maxSenders bounds the senders remembered — the oldest goes, and its replay window with it', () => {
  const encryption = normalizeEnvelopeEncryption({ keys: generateKey(), maxSenders: 2 }, 'x');
  assert.strictEqual(encryption.maxSenders, 2);
  assert.strictEqual(normalizeEnvelopeEncryption({ keys: generateKey() }, 'x').maxSenders, 1024);
  const [first, second, third, receiver] = [0, 1, 2, 3].map(() => createEnvelopeSealer({ encryption, layer: 'rooms' }));
  const open = (sealed) => receiver.open('0', sealed, 'ch').toString();
  const kept = first.seal(Buffer.from('one'), 'ch').sealed;
  assert.strictEqual(open(kept), 'one');
  assert.throws(() => open(kept), { reason: 'replay' }, 'remembered: its window refuses the copy');
  assert.strictEqual(open(second.seal(Buffer.from('two'), 'ch').sealed), 'two');
  assert.throws(() => open(kept), { reason: 'replay' }, 'two senders fit');
  // The third sender evicts the first, oldest in — and the copy opens again:
  // what a deployment with more live senders than `maxSenders` gives up.
  assert.strictEqual(open(third.seal(Buffer.from('three'), 'ch').sealed), 'three');
  assert.strictEqual(open(kept), 'one');
  for (const maxSenders of [0, -1, 1.5, '8', 1048577, false]) {
    assert.throws(
      () => normalizeEnvelopeEncryption({ keys: generateKey(), maxSenders }, 'x'),
      /x: encryption\.maxSenders must be an integer from 1 to 1048576/,
    );
  }
});

test('sealer: a kid withdrawn from a live provider is refused for a sender already known, not only for a new one', () => {
  const ring = new Map([['k1', generateKey()]]);
  const keys = { current: () => 'k1', get: (kid) => ring.get(kid) ?? null };
  const encryption = normalizeEnvelopeEncryption({ keys }, 'x');
  const [a, b] = [0, 1].map(() => createEnvelopeSealer({ encryption, layer: 'rooms' }));
  const first = a.seal(Buffer.from('one'), 'ch');
  assert.strictEqual(b.open(first.kid, first.sealed, 'ch').toString(), 'one', 'the sender is known now');
  // Sealed while the key was good, under a salt `b` remembers — which is on
  // the wire, so whoever holds the withdrawn key can seal under it too.
  const second = a.seal(Buffer.from('two'), 'ch');
  const kept = ring.get('k1');
  ring.delete('k1');
  assert.throws(() => b.open(second.kid, second.sealed, 'ch'), { reason: 'kid' });
  // Back on the ring (withdrawn by mistake): derived again, and it opens.
  ring.set('k1', kept);
  assert.strictEqual(b.open(second.kid, second.sealed, 'ch').toString(), 'two');
});

test('sealer: the frame, the echo, the sender cache and the reasons', () => {
  const [a, b] = sealerPair();
  const { kid, sealed } = a.seal(Buffer.from('hello'), 'ch');
  assert.strictEqual(kid, '0');
  assert.strictEqual(sealed.length, HEADER_LENGTH + 5 + 16);
  assert.deepStrictEqual([sealed[0], sealed[1]], [1, 1]);
  assert.strictEqual(b.open(kid, sealed, 'ch').toString(), 'hello');
  assert.strictEqual(a.open(kid, sealed, 'ch'), null, "a sealer's own envelope is its echo");
  // The counter counts; the salt stays
  const second = a.seal(Buffer.from('again'), 'ch').sealed;
  assert.deepStrictEqual(second.subarray(2, 18), sealed.subarray(2, 18));
  assert.strictEqual(second.readUInt32BE(22), 1);
  assert.strictEqual(b.open(kid, second, 'ch').toString(), 'again');
  const reason = (fn) => {
    try {
      fn();
    } catch (error) {
      assert.ok(error instanceof OpenError);
      assert.strictEqual(error.message, 'encryption: the message does not open');
      return error.reason;
    }
    return 'opened';
  };
  assert.strictEqual(
    reason(() => b.open(kid, sealed, 'ch')),
    'replay',
  );
  assert.strictEqual(
    reason(() => b.open(kid, sealed.subarray(0, 20), 'ch')),
    'format',
  );
  assert.strictEqual(
    reason(() => b.open(kid, Buffer.concat([Buffer.of(2), sealed.subarray(1)]), 'ch')),
    'format',
  );
  assert.strictEqual(
    reason(() => b.open(kid, Buffer.concat([Buffer.of(1, 9), sealed.subarray(2)]), 'ch')),
    'format',
  );
  assert.strictEqual(
    reason(() => b.open(kid, sealed.subarray(0, HEADER_LENGTH + 8), 'ch')),
    'format',
  );
  assert.strictEqual(
    reason(() => b.open('other', a.seal(Buffer.from('x'), 'ch').sealed, 'ch')),
    'kid',
  );
  // The suite byte is part of what a sender is known by: a copy of a KNOWN
  // sender's envelope with the byte changed takes the path of a new one —
  // another cipher id in the derivation, another key — and does not open.
  const third = a.seal(Buffer.from('third'), 'ch').sealed;
  const altered = Buffer.from(third);
  altered[1] = 2;
  assert.strictEqual(
    reason(() => b.open(kid, altered, 'ch')),
    'open',
  );
  assert.strictEqual(b.open(kid, third, 'ch').toString(), 'third', 'and the original still does');
  assert.strictEqual(
    reason(() => a.open(kid, altered, 'ch')),
    'open',
    'nor is it the echo of the sealer it was copied from',
  );
  // Another layer's sealer under the same keyring derives another key
  const [, , encryption] = sealerPair();
  const rooms = createEnvelopeSealer({ encryption, layer: 'rooms' });
  const cluster = createEnvelopeSealer({ encryption, layer: 'cluster' });
  assert.strictEqual(
    reason(() => cluster.open('0', rooms.seal(Buffer.from('x'), 'ch').sealed, 'ch')),
    'open',
  );
});

test('sealer: a made-up salt is never remembered; a real sender outlives a flood of them', () => {
  const [a, b, encryption] = sealerPair();
  const first = a.seal(Buffer.from('one'), 'ch');
  assert.strictEqual(b.open(first.kid, first.sealed, 'ch').toString(), 'one');
  for (let i = 0; i < MAX_SENDERS + 8; i++) {
    const forged = Buffer.from(first.sealed);
    forged.writeUInt32BE(i + 1, 2);
    assert.throws(() => b.open(first.kid, forged, 'ch'), OpenError);
  }
  // Still known: its replay window survived, so the first envelope is still a replay
  assert.throws(
    () => b.open(first.kid, first.sealed, 'ch'),
    (error) => error.reason === 'replay',
  );
  // Real senders do turn the cache over, oldest first
  for (let i = 0; i < MAX_SENDERS; i++) {
    const { kid, sealed } = createEnvelopeSealer({ encryption, layer: 'rooms' }).seal(Buffer.from('n'), 'ch');
    b.open(kid, sealed, 'ch');
  }
  assert.strictEqual(b.open(first.kid, first.sealed, 'ch').toString(), 'one', 'forgotten, so no longer a replay');
});

test('sealer: the counter reseeds its salt before the nonce space is spent, and follows a rotated kid', () => {
  let current = 'a';
  const held = { a: generateKey(), b: generateKey() };
  const encryption = normalizeEnvelopeEncryption(
    { keys: { current: () => current, get: (kid) => held[kid] ?? null } },
    'x',
  );
  const a = createEnvelopeSealer({ encryption, layer: 'rooms' });
  const b = createEnvelopeSealer({ encryption, layer: 'rooms' });
  const one = a.seal(Buffer.from('1'), 'ch');
  current = 'b';
  const two = a.seal(Buffer.from('2'), 'ch');
  assert.deepStrictEqual([one.kid, two.kid], ['a', 'b']);
  assert.notDeepStrictEqual(one.sealed.subarray(2, 18), two.sealed.subarray(2, 18), 'a new kid is a new salt');
  assert.strictEqual(two.sealed.readUInt32BE(22), 0);
  assert.strictEqual(b.open('b', two.sealed, 'ch').toString(), '2');
  assert.strictEqual(a.open('a', one.sealed, 'ch'), null, 'both salts are still its own echo');
  current = 'gone';
  assert.throws(() => a.seal(Buffer.from('3'), 'ch'), /does not hold its current key "gone"/);
});

test('ReplayWindow: gaps are free, repeats and the too-old are refused', () => {
  const window = new ReplayWindow(8);
  assert.strictEqual(window.accept(0), true);
  assert.strictEqual(window.accept(0), false);
  assert.strictEqual(window.accept(5), true);
  assert.strictEqual(window.accept(3), true, 'late, but inside the window');
  assert.strictEqual(window.accept(3), false);
  assert.strictEqual(window.accept(5), false);
  assert.strictEqual(window.accept(12), true);
  assert.strictEqual(window.accept(4), false, 'older than the window');
  assert.strictEqual(window.accept(6), true);
  assert.strictEqual(window.accept(7), true, 'a slot the advance cleared');
  assert.strictEqual(window.accept(1000), true, 'a jump past the whole window');
  assert.strictEqual(window.accept(999), true);
  assert.strictEqual(window.accept(992), false);
  assert.strictEqual(window.accept(2 ** 40), true);
  assert.strictEqual(window.accept(2 ** 40), false);
});

test('sealer: replayWindow: false delivers a repeat — for a carrier that redelivers on purpose', () => {
  const [a, b] = sealerPair({ replayWindow: false });
  const { kid, sealed } = a.seal(Buffer.from('again'), 'topic');
  assert.strictEqual(b.open(kid, sealed, 'topic').toString(), 'again');
  assert.strictEqual(b.open(kid, sealed, 'topic').toString(), 'again');
});

// A toy cipher in the guide's shape — the key kept by REFERENCE, read at
// seal time — whose tag depends on the key: the two properties that catch a
// sealer wiping the bytes it handed over (both ends then agree on zeros).
const xorCipher = (handed = []) => ({
  id: 'test-xor',
  keyLength: 32,
  nonceLength: 24,
  tagLength: 4,
  key: (raw) => {
    handed.push(raw);
    const tag = () => Buffer.from(raw.subarray(0, 4));
    return {
      seal: (nonce, plaintext) => Buffer.concat([plaintext.map((byte) => byte ^ raw[0] ^ nonce[23]), tag()]),
      open: (nonce, sealed) => {
        if (!sealed.subarray(-4).equals(tag())) throw new Error('bad tag');
        return Buffer.from(sealed.subarray(0, -4).map((byte) => byte ^ raw[0] ^ nonce[23]));
      },
    };
  },
});

test('envelope: an injected cipher rides under the injected suite, and must answer synchronously', () => {
  const handed = [];
  const xor = xorCipher(handed);
  const [a, b] = sealerPair({ cipher: xor });
  const { kid, sealed } = a.seal(Buffer.from('injected'), 'ch');
  assert.strictEqual(sealed[1], 0xff);
  assert.strictEqual(b.open(kid, sealed, 'ch').toString(), 'injected');
  // The key handed to the cipher is the derived one, intact — not wiped
  // under a cipher that kept the reference (the probe's random key first).
  assert.ok(handed.length >= 2);
  for (const raw of handed) {
    assert.ok(
      raw.some((byte) => byte !== 0),
      'a key that is not all zeros',
    );
  }
  // And a sealer over ANOTHER keyring does not open it: the tag depends on
  // the key, so agreeing on zeros would have passed this.
  const [c] = sealerPair({ cipher: xorCipher() });
  assert.throws(
    () => c.open(kid, sealed, 'ch'),
    (error) => error.name === 'OpenError' && error.reason === 'open',
  );
  // A built-in sender is still opened by an instance that injects its own
  const builtin = createEnvelopeSealer({ encryption: { ...sealerPair()[2], keys: keysOf(b) }, layer: 'rooms' });
  const aes = builtin.seal(Buffer.from('from aes'), 'ch');
  assert.strictEqual(aes.sealed[1], 1);
  assert.strictEqual(b.open(aes.kid, aes.sealed, 'ch').toString(), 'from aes');

  const keys = generateKey();
  const bad = (cipher, pattern) => assert.throws(() => normalizeEnvelopeEncryption({ keys, cipher }, 'x'), pattern);
  bad({ ...xor, key: async () => ({}) }, /synchronously/);
  bad({ ...xor, key: () => ({ seal: async () => Buffer.alloc(1), open() {} }) }, /synchronously/);
  bad({ ...xor, key: () => ({ seal: () => Buffer.alloc(1), open: async () => Buffer.alloc(1) }) }, /synchronously/);
  // The probe is a round trip: a cipher that cannot open its own seal, or
  // opens it to something else, is refused where it is configured.
  bad({ ...xor, key: () => ({ seal: () => Buffer.alloc(1), open: () => Buffer.from('other') }) }, /does not open/);
  bad(
    {
      ...xor,
      key: () => ({
        seal: () => Buffer.alloc(1),
        open: () => {
          throw new Error('never');
        },
      }),
    },
    /does not open/,
  );
  bad({ ...xor, nonceLength: 4 }, /nonce of 8 bytes or more/);
  bad({ id: 'x' }, /cipher must be a cipher name or a Cipher/);
  bad('aes-128-cbc', /unknown cipher/);
});

test('envelope: the option is validated where the server is built', () => {
  const backplane = new MemoryBackplane({ logger: false });
  const keys = generateKey();
  const build = (options) => new RpcServer({ router, backplane, logger: false, ...options });
  assert.throws(() => build({ rooms: { encryption: true } }), /rooms: encryption must be \{ keys/);
  assert.throws(() => build({ rooms: { encryption: { keys: 'short' } } }), /rooms: encryption\.keys must be 32 bytes/);
  assert.throws(() => build({ cluster: { encryption: {} } }), /cluster: encryption must be \{ keys/);
  assert.throws(() => build({ cluster: { encryption: { keys, seal: 'no' } } }), /seal must be a boolean/);
  assert.throws(
    () => build({ rooms: { encryption: { keys, acceptPlaintext: 1 } } }),
    /acceptPlaintext must be a boolean/,
  );
  assert.throws(() => build({ rooms: { encryption: { keys, replayWindow: 0 } } }), /replayWindow/);
  assert.throws(() => build({ rooms: { encryption: { keys, replayWindow: 1e6 } } }), /replayWindow/);
  assert.throws(() => build({ rooms: { encryption: { keys, seal: false } } }), /must accept plaintext/);
  for (const off of [undefined, null, false]) {
    assert.strictEqual(normalizeEnvelopeEncryption(off, 'x'), null);
    build({ rooms: { encryption: off }, cluster: { encryption: off } }).close();
  }
  backplane.close();
});

test('createEnvelope: with no option it only carries bytes; compression alone is the codec it always was', () => {
  const { log } = logs();
  const base = { maxMessage: 1 << 20, name: 'x', layer: 'rooms', event: 'backplane', log };
  const off = createEnvelope({ ...base });
  assert.strictEqual(off.sealed, undefined);
  assert.strictEqual(off.encode('{"v":1}', 'ch'), '{"v":1}', 'text is left exactly as it is');
  assert.strictEqual(off.decode('{"v":1}', 'ch'), '{"v":1}');
  assert.strictEqual(off.decode('wrpc-enc:deflate-raw:AAAA', 'ch'), null, 'compressed, and no codec here');
  assert.strictEqual(off.decode('wrpc-sealed:0:AAAA', 'ch'), 'wrpc-sealed:0:AAAA', 'named by the caller');
  const codec = createEnvelope({ ...base, compression: true });
  assert.strictEqual(codec.sealed, undefined);
  assert.strictEqual(codec.decode(codec.encode(JSON.stringify(bigData))), JSON.stringify(bigData));
});

test('createEnvelope: a sealed frame names a codec this instance does not hold, or is cut short', () => {
  const { log, warnings } = logs();
  const keys = generateKey();
  const base = { maxMessage: 1 << 20, name: 'x', layer: 'rooms', event: 'backplane', log, encryption: { keys } };
  const packing = createEnvelope({ ...base, compression: { codec: 'brotli', threshold: 0 } });
  const plain = createEnvelope({ ...base });
  const deflating = createEnvelope({ ...base, compression: true });
  const text = JSON.stringify(bigData);
  assert.strictEqual(plain.decode(packing.encode(text, 'ch'), 'ch'), undefined, 'compressed, and no codec here');
  assert.strictEqual(
    deflating.decode(packing.encode(text, 'ch'), 'ch'),
    undefined,
    'compressed with a codec not listed',
  );
  assert.deepStrictEqual(
    warnings.map((w) => [w.event, w.reason]),
    [
      ['backplane.open', 'codec'],
      ['backplane.open', 'codec'],
    ],
  );
  assert.strictEqual(deflating.decode(plain.encode(text, 'ch'), 'ch'), text, 'an uncompressed frame reads anywhere');
  // Past the inflate cap
  const capped = createEnvelope({ ...base, maxMessage: 64, compression: { codec: 'brotli', threshold: 0 } });
  assert.strictEqual(capped.decode(packing.encode(text, 'ch'), 'ch'), undefined);
});

test('createEnvelope: mid-rollout, compression still rides the plaintext both ways', () => {
  const { log, warnings } = logs();
  const keys = generateKey();
  const base = { maxMessage: 1 << 20, name: 'x', layer: 'rooms', event: 'backplane', log, compression: true };
  const text = JSON.stringify(bigData);
  // Deploy 1 publishes what it always did — the compressed marker included
  const opening = createEnvelope({ ...base, encryption: { keys, seal: false, acceptPlaintext: true } });
  const wire = opening.encode(text, 'ch');
  assert.ok(wire.startsWith('wrpc-enc:deflate-raw:'));
  // …and deploy 2 reads it, next to the sealed envelopes of its peers
  const sealing = createEnvelope({ ...base, encryption: { keys, acceptPlaintext: true } });
  assert.strictEqual(sealing.decode(wire, 'ch'), text);
  assert.strictEqual(sealing.decode(text, 'ch'), text);
  assert.strictEqual(opening.decode(sealing.encode(text, 'ch'), 'ch'), text);
  // Without a codec the plaintext passes as it is
  const bare = createEnvelope({
    ...base,
    compression: false,
    encryption: { keys, seal: false, acceptPlaintext: true },
  });
  assert.strictEqual(bare.encode(text, 'ch'), text);
  assert.strictEqual(bare.decode(text, 'ch'), text);
  assert.deepStrictEqual(warnings, []);
});

test('createEnvelope: a frame that opens but is malformed inside is refused like any other', () => {
  const { log, warnings } = logs();
  const keys = generateKey();
  const base = { maxMessage: 1 << 20, name: 'x', layer: 'rooms', event: 'backplane', log };
  const receiver = createEnvelope({ ...base, compression: true, encryption: { keys } });
  const forge = createEnvelopeSealer({ encryption: normalizeEnvelopeEncryption({ keys }, 'x'), layer: 'rooms' });
  const wire = (frame) => {
    const { kid, sealed } = forge.seal(Buffer.from(frame), 'ch');
    return `${SEALED_PREFIX}${kid}:${sealed.toString('base64')}`;
  };
  // Empty; compressed with no id; an id running past the frame; bytes that do not inflate
  for (const frame of [[], [1], [1, 200, 65], [1, 11, ...Buffer.from('deflate-raw'), 255, 255, 255]]) {
    assert.strictEqual(receiver.decode(wire(frame), 'ch'), undefined);
  }
  assert.deepStrictEqual(
    warnings.map((w) => w.reason),
    ['codec', 'codec', 'codec', 'codec'],
  );
  assert.strictEqual(receiver.decode(wire([0, ...Buffer.from('{"ok":1}')]), 'ch'), '{"ok":1}');
});
