'use strict';

// A durable feed with many local subscribers of one topic: what does not
// depend on the subscriber — opening a sealed entry, signing its resume
// token — is done once per ENTRY (src/broker/feed.js; bench/feed-fanout.js).
// These check the sharing and its edges: who computes, what is still each
// subscriber's own, and that nothing is shared where it must not be.

const { test } = require('node:test');
const assert = require('node:assert');

const { brokerFeed } = require('../../src/broker/feed.js');
const { TopicTails } = require('../../src/broker/tail.js');
const { openId } = require('../../src/broker/ids.js');
const { createBrokerSealing } = require('../../src/broker/sealing.js');
const { generateKey } = require('../../encryption.js');
const { recorder } = require('../helpers/recorder.js');
const { waitFor } = require('./support.js');

// A log on TopicTails, as the four adapters are: the live tail hands every
// reader the SAME entry (MemoryBroker copies per reader, so it shares
// nothing). `headersOf` lets a test hand one constant bag to many entries.
const sharedLog = ({ headersOf = (headers) => headers } = {}) => {
  const topics = new Map(); // topic -> { entries, push }
  const stateOf = (topic) => {
    if (!topics.has(topic)) topics.set(topic, { entries: [], push: null });
    return topics.get(topic);
  };
  const tails = new TopicTails({
    live: async (topic, { signal, onEntry }) => {
      const state = stateOf(topic);
      state.push = onEntry;
      signal.addEventListener('abort', () => {
        state.push = null;
      });
      return state.entries.length === 0 ? null : String(state.entries.length);
    },
    range: async (topic, { after, limit }) => {
      const from = after === null ? 0 : Number(after);
      return stateOf(topic)
        .entries.slice(from, from + limit)
        .map((entry) => ({ ...entry, headers: { ...entry.headers } }));
    },
    covered: (cursor, entry) => Number(entry.id) <= Number(cursor),
    advance: (_cursor, entry) => entry.id,
  });
  return {
    name: 'shared',
    tails,
    append: async (topic, value, { headers = {} } = {}) => {
      const state = stateOf(topic);
      const entry = { id: String(state.entries.length + 1), value, headers: headersOf(headers) };
      state.entries.push(entry);
      state.push?.(entry);
      return entry.id;
    },
    read: (topic, options) => tails.read(topic, options),
    parseId: (text) => (/^\d{1,9}$/.test(text) ? text : null),
  };
};

// N subscribers of one feed, each a generator pumped into its own array.
const subscribers = (t, feed, count, { contexts = null, args = {}, options = {} } = {}) => {
  const controller = new AbortController();
  t.after(() => controller.abort());
  const out = [];
  for (let i = 0; i < count; i++) {
    const seen = [];
    const iterator = feed(contexts?.[i] ?? {}, args, { signal: controller.signal, ...options });
    void (async () => {
      for await (const value of iterator) seen.push(value);
    })().catch(() => {});
    out.push(seen);
  }
  return out;
};

const settled = () => new Promise((resolve) => setTimeout(resolve, 20));

test('feed fan-out: every subscriber of an entry is handed the same signed token, and resumes from it', async (t) => {
  const log = sharedLog();
  const feed = brokerFeed(log, 'orders', { secret: 's3cret' });
  const [a, b, c] = subscribers(t, feed, 3);
  await settled();
  for (let i = 1; i <= 3; i++) await log.append('orders', JSON.stringify({ n: i }));
  await waitFor(() => a.length === 3 && b.length === 3 && c.length === 3);
  for (let i = 0; i < 3; i++) {
    assert.strictEqual(a[i].id, b[i].id);
    assert.strictEqual(a[i].id, c[i].id);
    assert.strictEqual(openId('s3cret', a[i].id, 'orders'), String(i + 1), 'the token opens to the position');
    // decode is each subscriber's own: equal values, never one object.
    assert.deepStrictEqual(a[i].data, { n: i + 1 });
    assert.notStrictEqual(a[i].data, b[i].data);
  }
  // Both resume from the shared token — a catch-up page, then the tail.
  const [resumed] = subscribers(t, feed, 1, { options: { lastEventId: a[0].id } });
  await waitFor(() => resumed.length === 2);
  assert.deepStrictEqual(
    resumed.map((value) => [value.id, value.data.n]),
    [
      [a[1].id, 2],
      [a[2].id, 3],
    ],
    'a catch-up reader signs the same tokens the live ones were given',
  );
});

test('feed fan-out: more ids than the memo holds are still signed right; topics sharing an id share no token', async (t) => {
  const log = sharedLog();
  const feed = brokerFeed(log, (_context, args) => args.topic, { secret: 's3cret', decode: 'text' });
  const [a] = subscribers(t, feed, 1, { args: { topic: 'a' } });
  const [b] = subscribers(t, feed, 1, { args: { topic: 'b' } });
  await settled();
  // Past the memo's cap (256): the oldest tokens are dropped, never wrong.
  for (let i = 1; i <= 300; i++) {
    await log.append('a', `a-${i}`);
    await log.append('b', `b-${i}`);
  }
  await waitFor(() => a.length === 300 && b.length === 300);
  for (let i = 0; i < 300; i++) {
    assert.strictEqual(openId('s3cret', a[i].id, 'a'), String(i + 1));
    assert.strictEqual(openId('s3cret', b[i].id, 'b'), String(i + 1));
    assert.notStrictEqual(a[i].id, b[i].id, 'the same position on two topics is two tokens');
    assert.strictEqual(openId('s3cret', a[i].id, 'b'), null, 'and one topic’s token is not the other’s');
  }
  // An id the memo dropped long ago is signed again to the same token.
  const [again] = subscribers(t, feed, 1, { args: { topic: 'a' }, options: { lastEventId: a[0].id } });
  await waitFor(() => again.length >= 1);
  assert.strictEqual(again[0].id, a[1].id);
});

test('feed fan-out: a sealed entry is opened once for all its live subscribers', async (t) => {
  // A live key provider is asked on every open (a withdrawn kid must stop
  // working), which makes it the count of opens.
  const key = generateKey();
  let asked = 0;
  const keys = {
    current: () => 'k1',
    get: (kid) => {
      asked++;
      return kid === 'k1' ? key : null;
    },
  };
  const sealing = createBrokerSealing({ keys }, 'test', { layer: 'broker-log', replay: false, text: true });
  const log = sharedLog();
  const publish = (topic, value, headers = {}) => {
    const sealed = sealing.seal(topic, headers, JSON.stringify(value));
    return log.append(topic, sealed.body, { headers: sealed.headers });
  };
  const seenEntries = [];
  const feed = brokerFeed(log, 'orders', {
    encryption: { keys },
    map: (value, entry) => {
      seenEntries.push(entry);
      return { ...value, tenant: entry.headers['x-tenant'] };
    },
  });
  const readers = subscribers(t, feed, 5);
  await settled();
  await publish('orders', { id: 'o-1' }, { 'x-tenant': 'acme' });
  await publish('orders', { id: 'o-2' }, { 'x-tenant': 'umbrella' });
  await waitFor(() => readers.every((seen) => seen.length === 2));
  for (const seen of readers) {
    assert.deepStrictEqual(
      seen.map((value) => value.data),
      [
        { id: 'o-1', tenant: 'acme' },
        { id: 'o-2', tenant: 'umbrella' },
      ],
    );
  }
  // Every subscriber's `map` saw the entry under its own id and the opened
  // headers; its mapped value is its own object.
  assert.strictEqual(seenEntries.length, 10);
  assert.ok(seenEntries.every((entry) => entry.id === '1' || entry.id === '2'));
  assert.notStrictEqual(readers[0][0].data, readers[1][0].data);
  // The ring is asked once per open (a seal asks only for its first frame,
  // which is behind us): one more entry is one open, for five readers.
  asked = 0;
  await publish('orders', { id: 'o-3' }, { 'x-tenant': 'acme' });
  await waitFor(() => readers.every((seen) => seen.length === 3));
  assert.strictEqual(asked, 1, `one open for five subscribers, not five (asked ${asked})`);
  // A catch-up reader brings entries of its own and opens them itself — and
  // reads the same values.
  const [late] = subscribers(t, brokerFeed(log, 'orders', { from: 'earliest', encryption: { keys } }), 1);
  asked = 0;
  await waitFor(() => late.length === 3);
  assert.deepStrictEqual(
    late.map((value) => value.data.id),
    ['o-1', 'o-2', 'o-3'],
  );
  assert.strictEqual(asked, 3, 'a page of its own is opened entry by entry');
});

test('feed fan-out: an entry that does not open is refused once — one line, one count — and skipped by everyone', async (t) => {
  const keys = generateKey();
  const stranger = createBrokerSealing({ keys: generateKey() }, 'test', {
    layer: 'broker-log',
    replay: false,
    text: true,
  });
  const ours = createBrokerSealing({ keys }, 'test', { layer: 'broker-log', replay: false, text: true });
  const log = sharedLog();
  const feed = brokerFeed(log, 'orders', { encryption: { keys } });
  const logs = recorder();
  const counted = [];
  const otel = { recordBrokerRefusal: (system, reason) => counted.push([system, reason]) };
  const contexts = [0, 1, 2, 3].map((i) => ({ log: logs.writer.child({ subscriber: i }), otel }));
  const readers = subscribers(t, feed, 4, { contexts });
  await settled();
  const foreign = stranger.seal('orders', {}, JSON.stringify({ id: 'not-ours' }));
  await log.append('orders', foreign.body, { headers: foreign.headers });
  await log.append('orders', JSON.stringify({ id: 'plaintext' }), { headers: {} });
  const good = ours.seal('orders', {}, JSON.stringify({ id: 'o-1' }));
  await log.append('orders', good.body, { headers: good.headers });
  await waitFor(() => readers.every((seen) => seen.length === 1));
  for (const seen of readers) assert.deepStrictEqual(seen[0].data, { id: 'o-1' });
  const refused = logs.all('broker.feed.refused');
  assert.deepStrictEqual(
    refused.map((entry) => [entry.id, entry.reason]),
    [
      ['1', 'open'],
      ['2', 'unsealed'],
    ],
    'one line per entry, not one per subscriber',
  );
  assert.deepStrictEqual(counted, [
    ['shared', 'open'],
    ['shared', 'unsealed'],
  ]);
});

test('feed fan-out: one constant header bag on many entries is never taken for one entry', async (t) => {
  const keys = generateKey();
  const sealing = createBrokerSealing({ keys }, 'test', { layer: 'broker-log', replay: false, text: true });
  // An adapter that interns its header bags: every sealed entry under one
  // kid carries the very same object.
  const bags = new Map();
  const log = sharedLog({
    headersOf: (headers) => {
      const key = JSON.stringify(headers);
      if (!bags.has(key)) bags.set(key, headers);
      return bags.get(key);
    },
  });
  const feed = brokerFeed(log, (_context, args) => args.topic, { encryption: { keys, acceptPlaintext: true } });
  const [a, b] = subscribers(t, feed, 2, { args: { topic: 'orders' } });
  const [other] = subscribers(t, feed, 1, { args: { topic: 'invoices' } });
  await settled();
  for (const id of ['o-1', 'o-2', 'o-3']) {
    const sealed = sealing.seal('orders', {}, JSON.stringify({ id }));
    await log.append('orders', sealed.body, { headers: sealed.headers });
  }
  // Plaintext, accepted during a rollout: passed through, nothing remembered.
  await log.append('orders', JSON.stringify({ id: 'plain' }), { headers: {} });
  const invoice = sealing.seal('invoices', {}, JSON.stringify({ id: 'i-1' }));
  await log.append('invoices', invoice.body, { headers: invoice.headers });
  await waitFor(() => a.length === 4 && b.length === 4 && other.length === 1);
  assert.strictEqual(bags.size, 2, 'the sealed entries really did share one bag');
  for (const seen of [a, b]) {
    assert.deepStrictEqual(
      seen.map((value) => value.data.id),
      ['o-1', 'o-2', 'o-3', 'plain'],
    );
  }
  assert.deepStrictEqual(other[0].data, { id: 'i-1' });
});
