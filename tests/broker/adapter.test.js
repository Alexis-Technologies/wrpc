'use strict';

// The leaf the four broker adapters share (src/broker/adapter.js). The
// contract suites exercise it through every adapter; these pin the pieces
// themselves, so a change here is seen here and not in four suites at once.

const { test } = require('node:test');
const assert = require('node:assert');

const {
  ATTEMPT_HEADER,
  REDELIVERED_HEADER,
  DEAD_REASON_HEADER,
  TEXT_HEADER,
  DEFAULT_PREFETCH,
  idFactory,
  failedRead,
  guardedRead,
  checkConsume,
  checkListen,
  runDelivery,
} = require('../../src/broker/adapter.js');
const { codedError } = require('../../src/broker/ids.js');
const { waitFor } = require('./support.js');

// An inner read over a fixed list, counting how it was left.
const innerOf = (values, ready = Promise.resolve()) => {
  const state = { returned: 0, nexts: 0 };
  let index = 0;
  return {
    state,
    read: {
      ready,
      [Symbol.asyncIterator]: () => ({
        next: async () => {
          state.nexts++;
          return index < values.length ? { done: false, value: values[index++] } : { done: true, value: undefined };
        },
        return: async (value) => {
          state.returned++;
          return { done: true, value };
        },
      }),
    },
  };
};

test('adapter: the header names are the wire names', () => {
  assert.deepStrictEqual(
    [ATTEMPT_HEADER, REDELIVERED_HEADER, DEAD_REASON_HEADER, TEXT_HEADER, DEFAULT_PREFETCH],
    ['x-wrpc-attempt', 'x-wrpc-redelivered', 'x-wrpc-dead-reason', 'wrpc-text', 16],
  );
});

test('adapter: idFactory — the default is short where a broker repeats it; an injected generator is used whole', () => {
  const plain = idFactory(null, 'createTestBroker');
  assert.match(plain.nextId(), /^[0-9a-f-]{36}$/);
  assert.match(plain.shortName(), /^[0-9a-f]{8}$/);
  let n = 0;
  const injected = idFactory(() => `custom-id-number-${++n}`, 'createTestBroker');
  assert.match(injected.nextId(), /^custom-id-number-\d+$/);
  assert.match(injected.shortName(), /^custom-id-number-\d+$/, 'never truncated');
  assert.strictEqual(injected.shortName, injected.nextId);
  // Strict: refused where the broker is made, not at the first id.
  assert.throws(() => idFactory('nope', 'createTestBroker'), /createTestBroker/);
  assert.throws(() => idFactory(() => 42, 'createTestBroker'), TypeError);
});

test('adapter: failedRead rejects ready and every next() — and is nobody’s unhandled rejection', async () => {
  const unhandled = [];
  const onUnhandled = (error) => unhandled.push(error);
  process.on('unhandledRejection', onUnhandled);
  try {
    const error = codedError('Malformed event id', 400);
    const read = failedRead(error);
    // Nobody awaits `ready`: a turn of the loop must pass without a report.
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepStrictEqual(unhandled, []);
    await assert.rejects(read.ready, (thrown) => thrown === error);
    const iterator = read[Symbol.asyncIterator]();
    await assert.rejects(iterator.next(), (thrown) => thrown === error);
    await assert.rejects(iterator.next(), (thrown) => thrown === error);
    assert.deepStrictEqual(await iterator.return(), { value: undefined, done: true });
  } finally {
    process.off('unhandledRejection', onUnhandled);
  }
});

test('adapter: guardedRead — a guard that refuses returns the inner reader before it throws', async () => {
  const unhandled = [];
  const onUnhandled = (error) => unhandled.push(error);
  process.on('unhandledRejection', onUnhandled);
  try {
    const refusal = codedError('Event history was trimmed past this id', 410);
    const guard = Promise.reject(refusal);
    guard.catch(() => {});
    const { read: inner, state } = innerOf([{ id: 1, value: 'a' }]);
    const read = guardedRead(inner, { guard });
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepStrictEqual(unhandled, [], 'a guarded read nobody awaits is not an unhandled rejection');
    await assert.rejects(read.ready, (thrown) => thrown === refusal);
    const iterator = read[Symbol.asyncIterator]();
    await assert.rejects(iterator.next(), (thrown) => thrown === refusal);
    assert.deepStrictEqual(state, { returned: 1, nexts: 0 }, 'left the shared tail, read nothing');
  } finally {
    process.off('unhandledRejection', onUnhandled);
  }
});

test('adapter: guardedRead — the guard is awaited once, ids are mapped on every value, return() reaches the inner reader', async () => {
  let resolveGuard;
  const guard = new Promise((resolve) => {
    resolveGuard = resolve;
  });
  const { read: inner, state } = innerOf([
    { id: 1, value: 'a', headers: { h: '1' } },
    { id: 2, value: 'b', headers: {} },
  ]);
  const read = guardedRead(inner, { guard, mapId: (id) => `seq-${id}` });
  const iterator = read[Symbol.asyncIterator]();
  const first = iterator.next();
  await new Promise((resolve) => setImmediate(resolve));
  assert.strictEqual(state.nexts, 0, 'nothing is read before the position is checked');
  resolveGuard();
  assert.strictEqual(await read.ready, undefined);
  assert.deepStrictEqual(await first, { done: false, value: { id: 'seq-1', value: 'a', headers: { h: '1' } } });
  assert.deepStrictEqual((await iterator.next()).value, { id: 'seq-2', value: 'b', headers: {} });
  assert.deepStrictEqual(await iterator.next(), { done: true, value: undefined });
  assert.deepStrictEqual(await iterator.return('bye'), { done: true, value: 'bye' });
  assert.strictEqual(state.returned, 1);
});

test('adapter: guardedRead — no guard and no mapId is the inner read, values untouched', async () => {
  const entry = { id: 7, value: 'x' };
  const ready = Promise.resolve('positioned');
  const { read: inner } = innerOf([entry], ready);
  const read = guardedRead(inner);
  assert.strictEqual(read.ready, ready);
  const iterator = read[Symbol.asyncIterator]();
  assert.strictEqual((await iterator.next()).value, entry, 'the very object, not a copy');
  // An inner iterator with no return() of its own still answers one.
  const bare = guardedRead({
    ready,
    [Symbol.asyncIterator]: () => ({ next: async () => ({ done: true, value: undefined }) }),
  });
  assert.deepStrictEqual(await bare[Symbol.asyncIterator]().return('v'), { value: 'v', done: true });
});

test('adapter: checkConsume and checkListen say which broker and what was wrong', () => {
  const ok = () => {};
  assert.doesNotThrow(() => checkConsume('x queue.consume', ok, 16, null));
  assert.doesNotThrow(() => checkConsume('x queue.consume', ok, 1, 'dead'));
  assert.throws(() => checkConsume('x queue.consume', null, 16, null), {
    name: 'TypeError',
    message: 'x queue.consume: onDelivery must be a function',
  });
  for (const prefetch of [0, -1, 1.5, '16', Infinity]) {
    assert.throws(() => checkConsume('x queue.consume', ok, prefetch, null), {
      name: 'TypeError',
      message: 'x queue.consume: prefetch must be a positive integer',
    });
  }
  for (const deadLetter of ['', 7, undefined, {}]) {
    assert.throws(() => checkConsume('x queue.consume', ok, 16, deadLetter), {
      name: 'TypeError',
      message: 'x queue.consume: deadLetter must be a queue name or null',
    });
  }
  assert.doesNotThrow(() => checkListen('x direct.listen', 'svc', ok));
  assert.throws(() => checkListen('x direct.listen', 'svc', 'nope'), {
    name: 'TypeError',
    message: 'x direct.listen: onMessage must be a function',
  });
  for (const address of ['', null, 7]) {
    assert.throws(() => checkListen('x direct.listen', address, ok), {
      name: 'TypeError',
      message: 'x direct.listen: address must be a non-empty string',
    });
  }
  // The handler is named first, as every adapter did.
  assert.throws(() => checkListen('x direct.listen', '', null), /onMessage must be a function/);
});

test('adapter: runDelivery — a handler that throws or rejects is reported and retried after the crash backoff', async () => {
  const reports = [];
  const report = (event, error, extra) => reports.push([event, error.message, extra]);
  const retries = [];
  const delivery = (attempt) => ({ attempt, retry: (options) => retries.push([attempt, options.delay]) });
  let handled = 0;
  runDelivery(() => void handled++, delivery(1), report, 'broker.x.delivery', 'jobs');
  runDelivery(async () => void handled++, delivery(1), report, 'broker.x.delivery', 'jobs');
  runDelivery(
    () => {
      throw new Error('sync boom');
    },
    delivery(1),
    report,
    'broker.x.delivery',
    'jobs',
  );
  runDelivery(async () => Promise.reject(new Error('async boom')), delivery(3), report, 'broker.x.delivery', 'jobs');
  await waitFor(() => retries.length === 2);
  assert.strictEqual(handled, 2);
  assert.deepStrictEqual(reports, [
    ['broker.x.delivery', 'sync boom', { queue: 'jobs' }],
    ['broker.x.delivery', 'async boom', { queue: 'jobs' }],
  ]);
  // 50 ms, doubling per attempt: deterministic, so a contract can bound it.
  assert.deepStrictEqual(retries, [
    [1, 50],
    [3, 200],
  ]);
});
