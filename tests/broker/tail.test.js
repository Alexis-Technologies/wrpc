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

test('TopicTails: onLag hears of readers that fell behind — once per fall, with the topic and how many', async () => {
  const log = createLog();
  const lags = [];
  const tails = log.tails({
    highWaterMark: 5,
    page: 4,
    onLag: (topic, readers) => {
      lags.push([topic, readers]);
      throw new Error("an observer that throws is not the tail's failure");
    },
  });
  // Two readers that do not pull, and one that keeps up.
  const slow = [tails.read('t'), tails.read('t')];
  const iterators = slow.map((read) => read[Symbol.asyncIterator]());
  const firsts = iterators.map((iterator) => iterator.next());
  const quick = tails.read('t');
  const pulled = [];
  const pump = (async () => {
    for await (const entry of quick) {
      pulled.push(entry.id);
      if (pulled.length === 21) break;
    }
  })();
  await Promise.all([...slow.map((read) => read.ready), quick.ready]);
  assert.deepStrictEqual(lags, [], 'nobody is behind, nothing is called');
  log.append('t', 'v1');
  await Promise.all(firsts);
  for (let i = 2; i <= 21; i++) {
    log.append('t', `v${i}`);
    await timers.setImmediate();
  }
  await pump;
  // Both slow readers passed the mark on the same entry: one call, for two.
  assert.deepStrictEqual(lags, [['t', 2]]);
  assert.strictEqual(pulled.length, 21, 'the reader that kept up was never dropped');
  // They catch up through range() as before, whatever the observer threw.
  const rest = [];
  for (let i = 0; i < 20; i++) rest.push((await iterators[0].next()).value.id);
  assert.deepStrictEqual(
    rest,
    Array.from({ length: 20 }, (_, i) => i + 2),
  );
  await Promise.all(iterators.map((iterator) => iterator.return()));
  assert.throws(() => log.tails({ onLag: 'log it' }), /onLag must be a function or null/);
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

  await t.test("'latest' keeps what is appended between ready and the first next()", async () => {
    const log = createLog();
    const tails = log.tails();
    log.append('t', 'old');
    const read = tails.read('t');
    await read.ready;
    log.append('t', 'during');
    await timers.setTimeout(5);
    const pending = collect(read, 2);
    log.append('t', 'after');
    assert.deepStrictEqual(
      (await pending).map((entry) => entry.value),
      ['during', 'after'],
    );
  });

  await t.test('a read returned or thrown into before its first next() releases the tail', async () => {
    const log = createLog();
    const tails = log.tails();
    const read = tails.read('t');
    await read.ready;
    assert.strictEqual(tails.size, 1);
    await read[Symbol.asyncIterator]().return();
    assert.strictEqual(tails.size, 0, 'return() before next() let go');
    const thrown = tails.read('t');
    await thrown.ready;
    await assert.rejects(thrown[Symbol.asyncIterator]().throw(new Error('gone')));
    assert.strictEqual(tails.size, 0, 'throw() before next() let go');
    // A read whose iterator was taken but never advanced still lets go
    // through its signal.
    const controller = new AbortController();
    const other = tails.read('t', { signal: controller.signal });
    await other.ready;
    const iterator = other[Symbol.asyncIterator]();
    controller.abort();
    assert.strictEqual(tails.size, 0, 'the abort let go');
    await iterator.return();
  });

  await t.test('a reader leaving a tail that is still positioning stops it once it is', async () => {
    const log = createLog();
    let release = null;
    const gate = new Promise((resolve) => (release = resolve));
    const aborted = [];
    const tails = log.tails({
      live: async (topic, { signal }) => {
        signal.addEventListener('abort', () => aborted.push(signal.aborted), { once: true });
        await gate;
        return null;
      },
    });
    const read = tails.read('t');
    await read[Symbol.asyncIterator]().return();
    assert.strictEqual(tails.size, 0, 'forgotten at once');
    assert.deepStrictEqual(aborted, [], 'but not aborted in the middle of its setup');
    release();
    await read.ready;
    await timers.setTimeout(1);
    assert.deepStrictEqual(aborted, [true], 'stopped once positioned');
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

test('TopicTails: a range that pages by time says whether it reached the tip', async (t) => {
  await t.test('`done: false` pages are followed by another range call from the advanced cursor', async () => {
    const { append, tails, stats } = createLog();
    for (let i = 1; i <= 9; i++) append('t', `v${i}`);
    // Three entries per call, and only the third call reaches the tip —
    // an array of three would have passed for a short page, the end.
    const paged = tails({
      range: async (_topic, { after }) => {
        stats.range++;
        const start = after === null ? 0 : after;
        const entries = [];
        for (let seq = start + 1; seq <= Math.min(start + 3, 9); seq++) {
          entries.push({ seq, value: `v${seq}`, headers: {} });
        }
        return { entries, done: start + 3 >= 9 };
      },
    });
    const values = await collect(paged.read('t', { from: 'earliest' }), 9);
    assert.deepStrictEqual(
      values.map((entry) => entry.value),
      Array.from({ length: 9 }, (_, i) => `v${i + 1}`),
    );
    assert.strictEqual(stats.range, 3, 'three pages, no fourth: the third said done');
  });

  await t.test('an incomplete page with nothing in it is asked again, a bounded number of times', async () => {
    const { tails, stats } = createLog();
    const stalled = tails({
      range: async () => {
        stats.range++;
        return { entries: [], done: false };
      },
    });
    const read = stalled.read('t', { from: 'earliest' });
    const started = Date.now();
    await assert.rejects(
      collect(read, 1, { timeout: 5000 }),
      (error) => error.code === 503 && /reach the tip/.test(error.message),
    );
    assert.ok(stats.range > 10, `asked ${stats.range} times`);
    assert.ok(Date.now() - started >= 400, 'with a pause between the asks');
  });

  await t.test('a page whose head skips past the cursor is not yielded — asked again, then 503', async () => {
    const { append, tails } = createLog();
    for (let i = 1; i <= 4; i++) append('t', `v${i}`);
    let skips = 2;
    const gappy = tails({
      contiguous: (cursor, entry) => entry.seq === cursor + 1,
      range: async (_topic, { after, limit }) => {
        const start = after === null ? 0 : after;
        // Twice the reader is handed a page that starts one entry too far.
        const from = skips-- > 0 && start > 0 ? start + 1 : start;
        return {
          entries: [1, 2, 3, 4]
            .filter((seq) => seq > from)
            .slice(0, limit)
            .map((seq) => ({ seq, value: `v${seq}`, headers: {} })),
          done: true,
        };
      },
    });
    const values = await collect(gappy.read('t', { after: 1 }), 3);
    assert.deepStrictEqual(
      values.map((entry) => entry.value),
      ['v2', 'v3', 'v4'],
      'nothing skipped: the page was asked again until it followed the cursor',
    );
    const broken = tails({
      contiguous: (cursor, entry) => entry.seq === cursor + 1,
      range: async () => ({ entries: [{ seq: 4, value: 'v4', headers: {} }], done: true }),
    });
    await assert.rejects(
      collect(broken.read('t', { after: 1 }), 1, { timeout: 5000 }),
      (error) => error.code === 503 && /gap/.test(error.message),
    );
    assert.throws(() => tails({ contiguous: 'yes' }), /contiguous must be a function or null/);
  });
});

test('TopicTails: a live tail that ends under its readers is replaced, and the readers miss nothing', async (t) => {
  // The toy log, with the tail's onEnd captured so the test can end it.
  const ending = (options = {}) => {
    const { append, tails, stats, listeners } = createLog();
    const ends = [];
    let lives = 0;
    const built = tails({
      live: async (topic, { signal, onEntry, onEnd }) => {
        stats.live++;
        if (options.failAfter !== undefined && ++lives > options.failAfter) {
          throw Object.assign(new Error('broker down'), { code: 503 });
        }
        const set = listeners.get(topic) ?? new Set();
        listeners.set(topic, set);
        set.add(onEntry);
        signal.addEventListener('abort', () => set.delete(onEntry), { once: true });
        ends.push(onEnd);
        return null;
      },
    });
    return { append, tails: built, stats, ends };
  };

  await t.test('the readers re-join a fresh tail and catch up from their cursors', async () => {
    const world = ending();
    const read = world.tails.read('t', { from: 'latest' });
    await read.ready;
    const iterator = read[Symbol.asyncIterator]();
    world.append('t', 'before');
    assert.strictEqual((await iterator.next()).value.value, 'before');
    // The tail dies: what is appended between its death and the fresh tail
    // is not seen live by anyone — the reader gets it through range().
    world.ends[0](new Error('lost'));
    world.append('t', 'during');
    await timers.setTimeout(5);
    world.append('t', 'after');
    assert.strictEqual((await iterator.next()).value.value, 'during');
    assert.strictEqual((await iterator.next()).value.value, 'after');
    assert.strictEqual(world.stats.live, 2, 'one fresh tail');
    assert.strictEqual(world.tails.size, 1);
    await iterator.return();
    assert.strictEqual(world.tails.size, 0);
  });

  await t.test('a fresh tail the broker cannot start fails the readers 503, after retries', async () => {
    const world = ending({ failAfter: 1 });
    const read = world.tails.read('t', { from: 'latest' });
    await read.ready;
    const iterator = read[Symbol.asyncIterator]();
    world.append('t', 'x');
    await iterator.next();
    const started = Date.now();
    world.ends[0](new Error('lost'));
    await assert.rejects(iterator.next(), (error) => error.code === 503 && /tail ended/.test(error.message));
    assert.ok(world.stats.live >= 3, `retried: ${world.stats.live} live() calls`);
    assert.ok(Date.now() - started >= 300, 'with a backoff between them');
  });

  await t.test('close() fails a reader parked on a tail with 503', async () => {
    const world = ending();
    const read = world.tails.read('t', { from: 'latest' });
    await read.ready;
    const iterator = read[Symbol.asyncIterator]();
    const next = iterator.next();
    await timers.setTimeout(1);
    world.tails.close();
    await assert.rejects(next, (error) => error.code === 503 && /closed/.test(error.message));
  });
});
