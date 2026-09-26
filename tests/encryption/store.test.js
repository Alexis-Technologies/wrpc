'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const { setImmediate: settle } = require('node:timers/promises');

const { sealedStore, generateKey } = require('../../encryption.js');
const { createRedisSessionStore } = require('../../scaling.js');
const { MemorySessionStore } = require('../../index.js');
const { SessionManager } = require('../../src/rpc/sessions.js');
const { normalizeEnvelopeEncryption, createEnvelopeSealer } = require('../../src/encryption/envelope.js');

// ioredis-shaped, for the commands the Redis session store uses.
class FakeRedis {
  constructor() {
    this.entries = new Map();
    this.commands = [];
  }

  async get(key) {
    this.commands.push(['get', key]);
    return this.entries.get(key) ?? null;
  }

  async set(key, value) {
    this.commands.push(['set', key]);
    this.entries.set(key, value);
    return 'OK';
  }

  async del(key) {
    this.commands.push(['del', key]);
    return this.entries.delete(key) ? 1 : 0;
  }

  async pexpire(key) {
    this.commands.push(['pexpire', key]);
    return this.entries.has(key) ? 1 : 0;
  }

  dump() {
    return JSON.stringify([...this.entries]);
  }
}

// A store we can look into: rows as the objects they are.
const mapStore = () => {
  const rows = new Map();
  return {
    rows,
    get: async (key) => rows.get(key) ?? null,
    set: async (key, data) => void rows.set(key, data),
    delete: async (key) => void rows.delete(key),
  };
};

const logs = () => {
  const warnings = [];
  const logger = {
    log() {},
    info() {},
    debug() {},
    error() {},
    warn: (entry) => warnings.push(entry),
    child: () => logger,
  };
  return { logger, warnings };
};

const TOKEN = '0f1e2d3c-4b5a-4978-8695-a4b3c2d1e0f9';
const STATE = { userId: 42, email: 'ada@example.com', roles: ['admin'] };

const redisStore = (redis) => createRedisSessionStore({ client: redis, logger: false });

test('sealedStore: neither the token nor the state rests in the store', async () => {
  const redis = new FakeRedis();
  const store = sealedStore(redisStore(redis), { keys: generateKey(), logger: false });
  await store.set(TOKEN, STATE);
  assert.deepStrictEqual(await store.get(TOKEN), STATE);
  const dump = redis.dump();
  for (const needle of [TOKEN, TOKEN.slice(0, 8), 'ada@example.com', 'userId', 'admin']) {
    assert.ok(!dump.includes(needle), needle);
  }
  const [[key, value]] = [...redis.entries];
  assert.match(key, /^wrpc:session:[A-Za-z0-9_-]{43}$/);
  assert.deepStrictEqual(Object.keys(JSON.parse(value)), ['v', 'kid', 's']);
  assert.strictEqual(store.name, 'sealed(redis)');
  // Another token, another row; an unknown one, nothing
  assert.strictEqual(await store.get('another-token'), null);
  await store.delete(TOKEN);
  assert.strictEqual(redis.entries.size, 0);
  assert.strictEqual(await store.get(TOKEN), null);
});

test('sealedStore: a process reads back what it wrote, and what another process wrote', async () => {
  const redis = new FakeRedis();
  const keys = generateKey();
  const a = sealedStore(redisStore(redis), { keys, logger: false });
  const b = sealedStore(redisStore(redis), { keys, logger: false });
  await a.set(TOKEN, STATE);
  assert.deepStrictEqual(await a.get(TOKEN), STATE);
  assert.deepStrictEqual(await a.get(TOKEN), STATE, 'twice: a store has no replay to refuse');
  assert.deepStrictEqual(await b.get(TOKEN), STATE);
  await b.set(TOKEN, { ...STATE, roles: [] });
  assert.deepStrictEqual(await a.get(TOKEN), { ...STATE, roles: [] });
});

test('sealedStore: an injected cipher keeps the key it was handed', async () => {
  // The guide's shape — the key by reference, read at seal time — under
  // which a sealer wiping the bytes after key() sealed every row with zeros.
  const handed = [];
  const xor = {
    id: 'test-xor',
    keyLength: 32,
    nonceLength: 24,
    tagLength: 4,
    key: (raw) => {
      handed.push(raw);
      const tag = () => Buffer.from(raw.subarray(0, 4));
      return {
        seal: (nonce, plaintext) =>
          Buffer.concat([Buffer.from(plaintext).map((byte) => byte ^ raw[0] ^ nonce[23]), tag()]),
        open: (nonce, sealed) => {
          const bytes = Buffer.from(sealed);
          if (!bytes.subarray(-4).equals(tag())) throw new Error('bad tag');
          return Buffer.from(bytes.subarray(0, -4).map((byte) => byte ^ raw[0] ^ nonce[23]));
        },
      };
    },
  };
  const redis = new FakeRedis();
  const keys = generateKey();
  const a = sealedStore(redisStore(redis), { keys, cipher: xor, logger: false });
  const b = sealedStore(redisStore(redis), { keys, cipher: xor, logger: false });
  await a.set(TOKEN, STATE);
  assert.deepStrictEqual(await b.get(TOKEN), STATE);
  assert.ok(handed.length >= 2);
  for (const raw of handed) {
    assert.ok(
      raw.some((byte) => byte !== 0),
      'the cipher saw the derived key, not zeros',
    );
  }
  assert.ok(!redis.dump().includes('ada@example.com'), 'and the row is not readable as it rests');
});

test('sealedStore: a row moved into another session slot does not open there', async () => {
  const redis = new FakeRedis();
  const { logger, warnings } = logs();
  const store = sealedStore(redisStore(redis), { keys: generateKey(), logger });
  await store.set('victim', { userId: 1, role: 'admin' });
  await store.set('attacker', { userId: 2, role: 'guest' });
  const rows = [...redis.entries];
  // Whoever can write to the store copies the admin's row over their own
  redis.entries.set(rows[1][0], rows[0][1]);
  assert.strictEqual(await store.get('attacker'), null);
  assert.deepStrictEqual(warnings, [{ event: 'session.open', kid: '0', reason: 'open' }]);
  assert.deepStrictEqual(await store.get('victim'), { userId: 1, role: 'admin' });
});

test('sealedStore: a row that is not a sealed record is a missing session and one line', async () => {
  const inner = mapStore();
  const { logger, warnings } = logs();
  const store = sealedStore(inner, { keys: generateKey(), logger });
  await store.set(TOKEN, STATE);
  const [key] = [...inner.rows.keys()];
  const original = inner.rows.get(key);
  for (const row of [{ userId: 1 }, { v: 2, kid: '0', s: 'AAAA' }, { v: 1, kid: 'other', s: 'AAAA' }, 'text']) {
    inner.rows.set(key, row);
    assert.strictEqual(await store.get(TOKEN), null);
  }
  // A record in shape, with a body too short to be a sealed frame
  inner.rows.set(key, { v: 1, kid: '0', s: 'AAAA' });
  assert.strictEqual(await store.get(TOKEN), null);
  inner.rows.set(key, original);
  assert.deepStrictEqual(await store.get(TOKEN), STATE, 'the untouched row still opens');
  assert.deepStrictEqual(
    warnings.map((w) => [w.event, w.reason]),
    [
      ['session.unsealed', undefined],
      ['session.unsealed', undefined],
      ['session.unsealed', undefined],
      ['session.unsealed', undefined],
      ['session.open', 'format'],
    ],
  );
  assert.ok(
    warnings.every((w) => !JSON.stringify(w).includes(TOKEN)),
    'the token is a credential: never logged',
  );
});

test('sealedStore: what opens must be the state of a session — an object', async () => {
  const { logger, warnings } = logs();
  const store = sealedStore(mapStore(), { keys: generateKey(), logger });
  // The contract says objects; a caller that breaks it cannot poison a later read
  await store.set(TOKEN, ['not', 'a', 'state']);
  assert.strictEqual(await store.get(TOKEN), null);
  await store.set(TOKEN, 'text');
  assert.strictEqual(await store.get(TOKEN), null);
  assert.deepStrictEqual(
    warnings.map((w) => [w.event, w.reason]),
    [
      ['session.open', 'format'],
      ['session.open', 'format'],
    ],
  );
});

test('sealedStore: a row sealed under the right key and slot that is not JSON is still only a missing session', async () => {
  const inner = mapStore();
  const { logger, warnings } = logs();
  const keys = generateKey();
  const store = sealedStore(inner, { keys, logger });
  await store.set(TOKEN, STATE);
  const [slot] = [...inner.rows.keys()];
  const encryption = normalizeEnvelopeEncryption({ keys, replayWindow: false }, 'x');
  const { kid, sealed } = createEnvelopeSealer({ encryption, layer: 'store' }).seal(Buffer.from('{not json'), slot);
  inner.rows.set(slot, { v: 1, kid, s: sealed.toString('base64') });
  assert.strictEqual(await store.get(TOKEN), null);
  assert.deepStrictEqual(warnings, [{ event: 'session.open', kid: '0', reason: 'format' }]);
});

test('sealedStore: a rotation signs nobody out — the row moves to the current key on its next read', async () => {
  const redis = new FakeRedis();
  const [k1, k2] = [generateKey(), generateKey()];
  const before = sealedStore(redisStore(redis), { keys: { current: 'k1', ring: { k1 } }, logger: false });
  await before.set(TOKEN, STATE);
  const [oldKey] = [...redis.entries.keys()];
  const after = sealedStore(redisStore(redis), { keys: { current: 'k2', ring: { k1, k2 } }, logger: false });
  assert.deepStrictEqual(await after.get(TOKEN), STATE);
  const [newKey] = [...redis.entries.keys()];
  assert.strictEqual(redis.entries.size, 1, 'moved, not copied');
  assert.notStrictEqual(newKey, oldKey, 'the index key rotates too');
  assert.strictEqual(JSON.parse(redis.entries.get(newKey)).kid, 'k2');
  // Once k1 is dropped the moved row still reads; one never moved would be gone
  const dropped = sealedStore(redisStore(redis), { keys: { current: 'k2', ring: { k2 } }, logger: false });
  assert.deepStrictEqual(await dropped.get(TOKEN), STATE);
  // delete reaches a row wherever it still is
  await before.set('straggler', { n: 1 });
  await after.delete('straggler');
  assert.strictEqual(redis.entries.size, 1);
});

test('sealedStore: acceptPlaintext reads a row the unwrapped store wrote, once, and seals it', async () => {
  const redis = new FakeRedis();
  const plain = redisStore(redis);
  await plain.set(TOKEN, STATE);
  const strict = sealedStore(redisStore(redis), { keys: generateKey(), logger: false });
  assert.strictEqual(await strict.get(TOKEN), null, 'off by default');
  const store = sealedStore(redisStore(redis), { keys: generateKey(), acceptPlaintext: true, logger: false });
  assert.deepStrictEqual(await store.get(TOKEN), STATE);
  assert.strictEqual(redis.entries.size, 1);
  assert.ok(!redis.dump().includes(TOKEN), 'the plaintext row is gone');
  assert.deepStrictEqual(await store.get(TOKEN), STATE);
  await plain.set('legacy', { n: 1 });
  await store.delete('legacy');
  assert.strictEqual(await plain.get('legacy'), null);
  assert.strictEqual(await store.get('nobody'), null);
});

test('sealedStore: a failed migration costs a retry, not the session', async () => {
  const redis = new FakeRedis();
  const [k1, k2] = [generateKey(), generateKey()];
  await sealedStore(redisStore(redis), { keys: { current: 'k1', ring: { k1 } }, logger: false }).set(TOKEN, STATE);
  const { logger, warnings } = logs();
  const flaky = { ...redisStore(redis), set: async () => Promise.reject(new Error('READONLY')) };
  const store = sealedStore(flaky, { keys: { current: 'k2', ring: { k1, k2 } }, logger });
  assert.deepStrictEqual(await store.get(TOKEN), STATE);
  assert.strictEqual(warnings[0].event, 'session.migrate');
  assert.strictEqual(redis.entries.size, 1, 'the old row is still there');
});

test('sealedStore: touch is forwarded to the current slot, and only when the store has one', async () => {
  const redis = new FakeRedis();
  const store = sealedStore(redisStore(redis), { keys: generateKey(), logger: false });
  await store.set(TOKEN, STATE);
  redis.commands.length = 0;
  await store.touch(TOKEN);
  assert.deepStrictEqual(
    redis.commands.map(([command]) => command),
    ['pexpire'],
  );
  assert.ok(!redis.commands[0][1].includes(TOKEN));
  const bare = sealedStore(mapStore(), { keys: generateKey(), logger: false });
  assert.strictEqual(bare.touch, undefined);
  assert.strictEqual(bare.name, 'sealed(store)');
});

test('sealedStore: a provider that lost its current key fails the write loudly and the read quietly', async () => {
  let current = 'a';
  const held = { a: generateKey() };
  const keys = { current: () => current, get: (kid) => held[kid] ?? null, kids: () => ['a', 'gone'] };
  const touched = [];
  const inner = { ...mapStore(), touch: async (key) => void touched.push(key) };
  const store = sealedStore(inner, { keys, logger: false });
  await store.set(TOKEN, STATE);
  current = 'gone';
  await assert.rejects(async () => store.set(TOKEN, STATE), /does not hold its current key "gone"/);
  assert.deepStrictEqual(await store.get(TOKEN), STATE, 'still found under the kid that holds it');
  await store.touch(TOKEN);
  assert.deepStrictEqual(touched, [], 'nothing to touch under a key that is not held');
  await store.delete(TOKEN);
  current = 'a';
  assert.strictEqual(await store.get(TOKEN), null);
});

test('sealedStore: the injection is validated where it is built', () => {
  const inner = new MemorySessionStore();
  assert.throws(() => sealedStore(), /store must be a session store/);
  assert.throws(() => sealedStore({ get() {}, set() {} }, { keys: generateKey() }), /store must be a session store/);
  assert.throws(() => sealedStore(inner), /sealedStore: options: encryption must be \{ keys/);
  assert.throws(() => sealedStore(inner, { keys: 'short' }), /encryption\.keys must be 32 bytes/);
  assert.throws(() => sealedStore(inner, { keys: generateKey(), acceptPlaintext: 'yes' }), /acceptPlaintext/);
  assert.throws(() => sealedStore(inner, { keys: generateKey(), cipher: 'rot13' }), /unknown cipher/);
});

test('sealedStore: plugs into SessionManager — a session survives an "instance switch", sealed at rest', async () => {
  const redis = new FakeRedis();
  const keys = generateKey();
  const manager = () => new SessionManager({ store: sealedStore(redisStore(redis), { keys, logger: false }) }, false);
  const [first, second] = [manager(), manager()];
  const session = first.create(TOKEN, { userId: 42 });
  session.state.email = 'ada@example.com';
  await settle();
  assert.ok(!redis.dump().includes('ada@example.com') && !redis.dump().includes(TOKEN));
  const restored = await second.restore(TOKEN);
  assert.deepStrictEqual({ ...restored.state }, { userId: 42, email: 'ada@example.com' });
  await settle();
  assert.ok(
    redis.commands.some(([command]) => command === 'pexpire'),
    'restoring is active use: the sliding expiry ran',
  );
  await second.destroy(TOKEN);
  assert.strictEqual(await first.restore(TOKEN), null);
  assert.strictEqual(redis.entries.size, 0);
});
