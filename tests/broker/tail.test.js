'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { TopicTails } = require('../../broker.js');
const { collect, waitFor } = require('./support.js');

// A toy single-sequence log: entries { seq, value, headers }, cursor = seq.
const createLog = () => {
  const topics = new Map();
  const listeners = new Map();
  const stats = { live: 0, range: 0 };
  const entriesOf = (topic) => {
    if (!topics.has(topic)) topics.set(topic, []);
    return topics.get(topic);
  };
  const append = (topic, value) => {
    const entries = entriesOf(topic);
    const entry = { seq: entries.length + 1, value, headers: {} };
    entries.push(entry);
    for (const listener of listeners.get(topic) ?? []) queueMicrotask(() => listener(entry));
    return entry.seq;
  };
  const tails = (options = {}) =>
    new TopicTails({
      live: async (topic, { signal, onEntry }) => {
        stats.live++;
        if (options.failLive) throw Object.assign(new Error('broker down'), { code: 503 });
        const set = listeners.get(topic) ?? new Set();
        listeners.set(topic, set);
        set.add(onEntry);
        signal.addEventListener('abort', () => set.delete(onEntry), { once: true });
        const entries = entriesOf(topic);
        return entries.length === 0 ? null : entries[entries.length - 1].seq;
      },
      range: async (topic, { after, limit }) => {
        stats.range++;
        await timers.setTimeout(1);
        if (after === 'bogus') throw Object.assign(new Error('gone'), { code: 410 });
        const start = after === null ? 0 : after;
        return entriesOf(topic).slice(start, start + limit);
      },
      covered: (cursor, entry) => entry.seq <= cursor,
      advance: (_cursor, entry) => entry.seq,
      ...options,
    });
  return { append, tails, stats, listeners };
};

test('TopicTails: construction is validated', () => {
  assert.throws(() => new TopicTails({}), /live must be a function/);
});

test('TopicTails: a resuming reader catches up, then follows the live tail without a gap or a duplicate', async () => {
  const log = createLog();
  const tails = log.tails({ page: 3 });
  for (let i = 1; i <= 10; i++) log.append('t', `v${i}`);
  const read = tails.read('t', { after: 4 });
  const pending = collect(read, 10);
  await read.ready;
  // Appends racing the catch-up: they reach the reader through BOTH paths.
  for (let i = 11; i <= 14; i++) log.append('t', `v${i}`);
  const entries = await pending;
  assert.deepStrictEqual(
    entries.map((entry) => entry.id),
    [5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
  );
  assert.deepStrictEqual(entries[0], { id: 5, value: 'v5', headers: {} });
});

test('TopicTails: one live tail per topic, shared, stopped with its last reader', async () => {
  const log = createLog();
  const tails = log.tails();
  const a = new AbortController();
  const b = new AbortController();
  const readA = tails.read('t', { signal: a.signal });
  const readB = tails.read('t', { signal: b.signal });
  const other = new AbortController();
  const readOther = tails.read('u', { signal: other.signal });
  await Promise.all([readA.ready, readB.ready, readOther.ready]);
  assert.strictEqual(log.stats.live, 2);
  assert.strictEqual(tails.size, 2);
  const pendingA = collect(readA, 2);
  const pendingB = collect(readB, 2);
  log.append('t', 'x');
  log.append('t', 'y');
  assert.deepStrictEqual(
    (await pendingA).map((entry) => entry.value),
    ['x', 'y'],
  );
  assert.deepStrictEqual(
    (await pendingB).map((entry) => entry.value),
    ['x', 'y'],
  );
  await waitFor(() => tails.size === 1);
  other.abort(); // never iterated: the signal lets go of it
  await waitFor(() => tails.size === 0);
  await waitFor(() => (log.listeners.get('t')?.size ?? 0) === 0);
});

test('TopicTails: a latest reader starts at the tip the tail tracks', async () => {
  const log = createLog();
  const tails = log.tails();
  log.append('t', 'before');
  const keep = tails.read('t');
  const keepIterator = keep[Symbol.asyncIterator]();
  const kept = keepIterator.next();
  await keep.ready;
  log.append('t', 'seen-by-first');
  assert.strictEqual((await kept).value.id, 2);
  // Joining a RUNNING tail: its cursor has moved past the start tip.
  const late = tails.read('t');
  const pending = collect(late, 1);
  await late.ready;
  log.append('t', 'new');
  const [entry] = await pending;
  assert.deepStrictEqual([entry.id, entry.value], [3, 'new']);
  await keepIterator.return();
});

test('TopicTails: an earliest reader over an empty topic follows from the start', async () => {
  const log = createLog();
  const tails = log.tails();
  const read = tails.read('empty', { from: 'earliest' });
  const pending = collect(read, 2);
  await read.ready;
  log.append('empty', 'a');
  log.append('empty', 'b');
  assert.deepStrictEqual(
    (await pending).map((entry) => entry.id),
    [1, 2],
  );
});

test('TopicTails: a slow reader past the high-water mark catches up through range()', async () => {
  const log = createLog();
  const tails = log.tails({ highWaterMark: 5, page: 4 });
  const read = tails.read('t');
  const iterator = read[Symbol.asyncIterator]();
  const first = iterator.next();
  await read.ready;
  log.append('t', 'v1');
  assert.strictEqual((await first).value.id, 1);
  // The reader is not pulling: 20 entries overflow its queue of 5.
  for (let i = 2; i <= 21; i++) log.append('t', `v${i}`);
  await timers.setTimeout(5);
  const rangesBefore = log.stats.range;
  const rest = [];
  for (let i = 0; i < 20; i++) rest.push((await iterator.next()).value.id);
  assert.deepStrictEqual(
    rest,
    Array.from({ length: 20 }, (_, i) => i + 2),
  );
  assert.ok(log.stats.range > rangesBefore, 'the lagging reader never re-ranged');
  await iterator.return();
});

test('TopicTails: range errors and live failures reach the reader', async (t) => {
  await t.test('an unusable cursor fails the read with the range error', async () => {
    const log = createLog();
    const tails = log.tails();
    const iterator = tails.read('t', { after: 'bogus' })[Symbol.asyncIterator]();
    await assert.rejects(iterator.next(), (error) => error.code === 410);
  });

  await t.test('a failed live tail fails its readers and is retried by the next read', async () => {
    const log = createLog();
    const failing = log.tails({ failLive: true });
    const read = failing.read('t');
    await assert.rejects(read.ready, (error) => error.code === 503);
    await assert.rejects(read[Symbol.asyncIterator]().next(), (error) => error.code === 503);
    assert.strictEqual(failing.size, 0);
    const again = failing.read('t');
    await assert.rejects(again.ready, (error) => error.code === 503);
    assert.strictEqual(log.stats.live, 2);
  });

  await t.test('a live tail failing after a reader joined reaches that reader', async () => {
    let fail;
    const tails = new TopicTails({
      live: () =>
        new Promise((_resolve, reject) => {
          fail = reject;
        }),
      range: async () => [],
      covered: () => false,
      advance: (_cursor, entry) => entry.seq,
    });
    const iterator = tails.read('t')[Symbol.asyncIterator]();
    const next = iterator.next();
    await timers.setTimeout(1);
    fail(Object.assign(new Error('lost'), { code: 503 }));
    await assert.rejects(next, (error) => error.code === 503);
  });
});

test('TopicTails: iteration rules', async (t) => {
  await t.test('a read is iterated once', async () => {
    const log = createLog();
    const tails = log.tails();
    const read = tails.read('t');
    const iterator = read[Symbol.asyncIterator]();
    assert.throws(() => read[Symbol.asyncIterator](), /iterated once/);
    await iterator.return();
  });

  await t.test('aborting ends a waiting read; close() stops every tail', async () => {
    const log = createLog();
    const tails = log.tails();
    const controller = new AbortController();
    const read = tails.read('t', { signal: controller.signal });
    const iterator = read[Symbol.asyncIterator]();
    const next = iterator.next();
    await read.ready;
    controller.abort();
    assert.deepStrictEqual(await next, { value: undefined, done: true });
    const other = tails.read('u');
    await other.ready;
    assert.strictEqual(tails.size, 1);
    tails.close();
    assert.strictEqual(tails.size, 0);
  });

  await t.test('an abort during catch-up ends the read', async () => {
    const log = createLog();
    const tails = log.tails({ page: 2 });
    for (let i = 1; i <= 6; i++) log.append('t', `v${i}`);
    const controller = new AbortController();
    const seen = [];
    for await (const entry of tails.read('t', { from: 'earliest', signal: controller.signal })) {
      seen.push(entry.id);
      if (seen.length === 1) controller.abort();
    }
    assert.deepStrictEqual(seen, [1]);
  });
});

test('TopicTails: a vector cursor (Kafka-shaped) is advanced, not replaced', async () => {
  // Entries carry { partition, offset }; the cursor maps partition -> offset.
  const entries = [];
  let listener = null;
  const tails = new TopicTails({
    live: async (_topic, { onEntry }) => {
      listener = onEntry;
      return { 0: -1, 1: -1 };
    },
    range: async (_topic, { after }) =>
      entries.filter((entry) => after === null || entry.offset > (after[entry.partition] ?? -1)),
    covered: (cursor, entry) => entry.offset <= (cursor[entry.partition] ?? -1),
    advance: (cursor, entry) => ({ ...(cursor ?? {}), [entry.partition]: entry.offset }),
  });
  const read = tails.read('t');
  const pending = collect(read, 3);
  await read.ready;
  for (const [partition, offset] of [
    [0, 0],
    [1, 0],
    [0, 1],
  ]) {
    const entry = { partition, offset, value: `${partition}:${offset}`, headers: {} };
    entries.push(entry);
    listener(entry);
  }
  const got = await pending;
  assert.deepStrictEqual(got[2].id, { 0: 1, 1: 0 });
});
