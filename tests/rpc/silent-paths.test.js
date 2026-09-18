'use strict';

// The paths that used to fail without saying so.
//
// Each of these was a real report waiting to happen: a call refused with no
// line, a connection that "just disconnects", a user silently signed out, a
// resume token quietly rejected. The assertions are on the `event` name,
// because that is the field docs/guide/logging.md tells operators to build
// alerts on — a line with the wrong event is as good as no line.

const test = require('node:test');
const assert = require('node:assert');

const { defineRouter, procedure } = require('../../index.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

/** A structured writer: `debug` survives here, where a Console drops it. */
const recorder = () => {
  const entries = [];
  const writer = {
    level: 'debug',
    child() {
      return this;
    },
    log(entry) {
      entries.push({ level: 'log', ...entry });
    },
    info(entry) {
      entries.push({ level: 'info', ...entry });
    },
    debug(entry) {
      entries.push({ level: 'debug', ...entry });
    },
    warn(entry) {
      entries.push({ level: 'warn', ...entry });
    },
    error(entry) {
      entries.push({ level: 'error', ...entry });
    },
  };
  const find = (event) => entries.find((entry) => entry.event === event);
  return { entries, writer, find };
};

const router = defineRouter({
  unit: {
    ping: procedure({ access: 'public', handler: async () => 'pong' }),
  },
});

test('dispatcher: a call for a method that does not exist says so', async (t) => {
  const { writer, find } = recorder();
  const { url } = await bootServer(t, { router, logger: writer });
  const client = await connectClient(t, url);
  await assert.rejects(() => client.call('unit/nope', {}));
  await waitFor(() => find('call.unknown'), 'the 404 never reached the log');
  const entry = find('call.unknown');
  assert.strictEqual(entry.level, 'warn');
  assert.strictEqual(entry.code, 404);
  assert.strictEqual(entry.method, 'unit/nope', 'the method is in the log even though the metric says <unknown>');
});

test('dispatcher: the capacity refusal is debug, not warn', async (t) => {
  const slow = defineRouter({
    unit: {
      wait: procedure({
        access: 'public',
        handler: () => new Promise((resolve) => setTimeout(() => resolve(1), 200)),
      }),
    },
  });
  const { writer, find } = recorder();
  const { url } = await bootServer(t, { router: slow, logger: writer, maxCalls: 2 });
  const client = await connectClient(t, url);
  const calls = [client.call('unit/wait', {}), client.call('unit/wait', {}), client.call('unit/wait', {})];
  await Promise.allSettled(calls);
  const entry = find('call.capacity');
  assert.ok(entry, 'the 429 never reached the log');
  // Any peer can drive this in a loop without authenticating, so warning on
  // it would turn a refused flood into a log flood.
  assert.strictEqual(entry.level, 'debug');
  assert.strictEqual(entry.code, 429);
});

test('dispatcher: a packet of an unknown type is reported, like a malformed one', async (t) => {
  const { writer, find } = recorder();
  const { server, origin } = await bootServer(t, { router, logger: writer });
  // Structurally valid JSON that is not a packet this protocol has — a
  // version skew, or somebody else's client pointed at this port. Malformed
  // FRAMES already logged; this half of the same funnel did not.
  const res = await fetch(`${origin}${server.rpc.basePath}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ type: 'nonsense', id: 'a' }),
  });
  await res.json();
  const entry = find('packet.unknown');
  assert.ok(entry, 'an unknown packet type stayed silent');
  assert.strictEqual(entry.level, 'warn');
  assert.strictEqual(entry.type, 'nonsense');
});

test('dispatcher: an oversize batch is reported at debug', async (t) => {
  const { writer, find } = recorder();
  const { server, origin } = await bootServer(t, { router, logger: writer, maxBatch: 2 });
  const batch = [1, 2, 3].map((n) => ({ type: 'call', id: `c${n}`, method: 'unit/ping', args: {} }));
  await fetch(`${origin}${server.rpc.basePath}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(batch),
  });
  const entry = find('batch.refused');
  assert.ok(entry, 'the oversize batch stayed silent');
  assert.strictEqual(entry.level, 'debug');
  assert.strictEqual(entry.size, 3);
  assert.strictEqual(entry.max, 2);
});

test('sessions: a store that cannot delete does not take the process down', async () => {
  const { SessionManager } = require('../../src/rpc/sessions.js');
  const { writer, find } = recorder();
  const store = {
    async get() {
      return null;
    },
    async set() {},
    async delete() {
      throw new Error('redis is down');
    },
  };
  const sessions = new SessionManager({ store }, writer);
  // initializeSession() calls finalizeSession() through `void`, so a
  // rejection here used to become an unhandled rejection — a store blip
  // ending the server rather than one session.
  await assert.doesNotReject(() => sessions.destroy('token'));
  const entry = find('session.destroy');
  assert.ok(entry, 'the failed delete stayed silent');
  assert.strictEqual(entry.level, 'error');
  assert.strictEqual(entry.err.message, 'redis is down');
});

test('sessions: evicting live sessions for capacity logs once, with a count', () => {
  const { MemorySessionStore } = require('../../src/rpc/sessions.js');
  const { entries, writer, find } = recorder();
  const store = new MemorySessionStore({ maxSessions: 2, logger: writer });
  for (const token of ['a', 'b', 'c', 'd']) store.set(token, { user: token });
  const evictions = entries.filter((entry) => entry.event === 'session.evict');
  assert.ok(find('session.evict'), 'a live session was dropped silently');
  assert.strictEqual(find('session.evict').level, 'warn');
  // One line per sweep carrying a count — a store pinned at its ceiling
  // evicts on every create, and a line each would be the flood.
  assert.ok(
    evictions.every((entry) => typeof entry.evicted === 'number' && entry.evicted > 0),
    'every eviction line carries its count',
  );
});

test('logging: every entry the server writes carries an event', async (t) => {
  // The rule that keeps the catalogue in docs/guide/logging.md true. It was
  // broken exactly once, in the redis backplane, where entries went out as
  // `{ err, component }` and so could not be alerted on alongside the rest
  // — invisible until somebody went looking. An assertion is cheaper than
  // looking.
  const offenders = [];
  const assertEvent = (level) => (entry) => {
    if (typeof entry?.event !== 'string' || entry.event.length === 0) {
      offenders.push({ level, entry });
    }
  };
  const strict = {
    level: 'debug',
    child() {
      return this;
    },
    log: assertEvent('log'),
    info: assertEvent('info'),
    debug: assertEvent('debug'),
    warn: assertEvent('warn'),
    error: assertEvent('error'),
  };

  const wide = defineRouter({
    unit: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      guarded: procedure({ access: 'session', handler: async () => 'secret' }),
    },
  });
  const { server, url, origin } = await bootServer(t, { router: wide, logger: strict, sse: {}, maxBatch: 2 });
  const client = await connectClient(t, url);

  // Drive the paths that log: a good call, a 404, a 403, an oversize batch,
  // an unknown packet and a malformed frame.
  await client.load('unit');
  assert.strictEqual(await client.api.unit.ping(), 'pong');
  await assert.rejects(() => client.call('unit/nope', {}));
  await assert.rejects(() => client.call('unit/guarded', {}));
  const post = (body) =>
    fetch(`${origin}${server.rpc.basePath}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body,
    }).then((res) => res.text());
  await post(JSON.stringify([1, 2, 3].map((n) => ({ type: 'call', id: `c${n}`, method: 'unit/ping', args: {} }))));
  await post(JSON.stringify({ type: 'nonsense' }));
  await post('{not json');
  await fetch(`${origin}${server.rpc.basePath}/does-not-exist`);

  assert.deepStrictEqual(offenders, [], 'every log entry must carry a non-empty `event`');
});

test('scaling: a corrupt stored session is reported, and never logs the token', async () => {
  const { createRedisSessionStore } = require('../../scaling.js');
  const { writer, find } = recorder();
  const client = {
    async get() {
      return 'not json{{';
    },
    async set() {},
    async del() {},
  };
  const store = createRedisSessionStore({ client, logger: writer });
  assert.strictEqual(await store.get('a-real-session-token'), null);
  const entry = find('session.corrupt');
  assert.ok(entry, 'a silently signed-out user left no trace');
  assert.strictEqual(entry.level, 'warn');
  // The token is the credential. A log pipeline is not where it goes.
  assert.ok(!JSON.stringify(entry).includes('a-real-session-token'), 'the token must not reach the log');
});

test('broker: a resume token that fails its signature is warned about, by reason', async () => {
  const { MemoryBroker, brokerFeed } = require('../../broker.js');
  const broker = new MemoryBroker({ logger: false });
  const { writer, find } = recorder();
  const feed = brokerFeed(broker, 'topic', { secret: 'shhh' });
  const context = { log: writer };
  // A tampered token: not merely stale, but one whose HMAC does not check
  // out — someone reading a topic from an offset they invented. It used to
  // be indistinguishable from a garbled one, and silent.
  const iterator = feed(context, {}, { lastEventId: 'forged!deadbeef' });
  await assert.rejects(() => iterator.next());
  const entry = find('broker.feed.resume');
  assert.ok(entry, 'a forged resume token was refused silently');
  assert.strictEqual(entry.reason, 'signature');
  assert.strictEqual(entry.level, 'warn');
  // The id itself is peer-controlled text and stays out of the entry.
  assert.ok(!JSON.stringify(entry).includes('forged!deadbeef'));
  await broker.close();
});
