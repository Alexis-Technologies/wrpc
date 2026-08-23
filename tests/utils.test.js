'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { Emitter, jsonParse, Semaphore, toKebab } = require('../src/utils.js');

test('Emitter', async (t) => {
  await t.test('emit resolves without listeners for non-error events', async () => {
    const emitter = new Emitter();
    await assert.doesNotReject(emitter.emit('idle'));
  });

  await t.test('emit throws Unhandled error when no error listeners', () => {
    const emitter = new Emitter();
    assert.throws(() => emitter.emit('error', new Error('boom')), /Unhandled error/);
  });

  await t.test('on/emit calls all listeners with the value', async () => {
    const emitter = new Emitter();
    const received = [];
    emitter.on('data', (value) => received.push(value));
    emitter.on('data', (value) => received.push(value * 2));
    await emitter.emit('data', 5);
    assert.deepStrictEqual(received, [5, 10]);
  });

  await t.test('once listener fires only a single time', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.once('tick', () => calls++);
    await emitter.emit('tick');
    await emitter.emit('tick');
    assert.strictEqual(calls, 1);
  });

  await t.test('once alongside a persistent listener keeps the persistent one', async () => {
    const emitter = new Emitter();
    let onceCalls = 0;
    let onCalls = 0;
    emitter.once('tick', () => onceCalls++);
    emitter.on('tick', () => onCalls++);
    await emitter.emit('tick');
    await emitter.emit('tick');
    assert.strictEqual(onceCalls, 1);
    assert.strictEqual(onCalls, 2);
  });

  await t.test('addListener throws on duplicate listener registration', () => {
    const emitter = new Emitter();
    const listener = () => {};
    emitter.on('data', listener);
    assert.throws(() => emitter.on('data', listener), /Duplicate listeners detected/);
  });

  await t.test('addListener warns past maxListeners instead of throwing', () => {
    const emitter = new Emitter({ maxListeners: 1 });
    emitter.on('data', () => {});
    const warnings = [];
    const originalWarn = globalThis.console.warn;
    globalThis.console.warn = (message) => warnings.push(message);
    try {
      assert.doesNotThrow(() => emitter.on('data', () => {}));
    } finally {
      globalThis.console.warn = originalWarn;
    }
    assert.strictEqual(emitter.listenerCount('data'), 2);
    assert.ok(warnings.some((message) => /MaxListenersExceededWarning/.test(message)));
  });

  await t.test('off removes a specific listener', async () => {
    const emitter = new Emitter();
    let calls = 0;
    const listener = () => calls++;
    emitter.on('data', listener);
    emitter.off('data', listener);
    await emitter.emit('data');
    assert.strictEqual(calls, 0);
  });

  await t.test('off without a listener removes the whole event', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.on('data', () => calls++);
    emitter.off('data');
    await emitter.emit('data');
    assert.strictEqual(calls, 0);
  });

  await t.test('off on an unknown event is a no-op', () => {
    const emitter = new Emitter();
    assert.doesNotThrow(() => emitter.off('unknown', () => {}));
  });

  await t.test('clear removes a single event', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.on('data', () => calls++);
    emitter.clear('data');
    await emitter.emit('data');
    assert.strictEqual(calls, 0);
  });

  await t.test('clear with no argument removes every event', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.on('data', () => calls++);
    emitter.on('other', () => calls++);
    emitter.clear();
    assert.deepStrictEqual(emitter.eventNames(), []);
  });

  await t.test('listeners returns the registered handlers', () => {
    const emitter = new Emitter();
    const listener = () => {};
    emitter.on('data', listener);
    assert.deepStrictEqual(emitter.listeners('data'), [listener]);
    assert.deepStrictEqual(emitter.listeners('unknown'), []);
  });

  await t.test('listeners throws without an eventName', () => {
    const emitter = new Emitter();
    assert.throws(() => emitter.listeners(), /Expected eventName/);
  });

  await t.test('listenerCount reports the number of handlers', () => {
    const emitter = new Emitter();
    emitter.on('data', () => {});
    emitter.on('data', () => {});
    assert.strictEqual(emitter.listenerCount('data'), 2);
    assert.strictEqual(emitter.listenerCount('unknown'), 0);
  });

  await t.test('listenerCount throws without an eventName', () => {
    const emitter = new Emitter();
    assert.throws(() => emitter.listenerCount(), /Expected eventName/);
  });

  await t.test('eventNames lists every registered event', () => {
    const emitter = new Emitter();
    emitter.on('data', () => {});
    emitter.on('close', () => {});
    assert.deepStrictEqual(emitter.eventNames().sort(), ['close', 'data']);
  });

  // The contract every emit() shape must preserve, characterized here because
  // `void emitter.emit('close')` is the house style: a listener's SYNCHRONOUS
  // throw has to surface as a rejection, never as a throw escaping into a
  // socket handler. Written against the pre-fast-path implementation.
  await t.test('a synchronously throwing listener rejects instead of throwing', async () => {
    const emitter = new Emitter();
    emitter.on('boom', () => {
      throw new Error('sync boom');
    });
    let escaped = null;
    let promise = null;
    try {
      promise = emitter.emit('boom', 1);
    } catch (error) {
      escaped = error;
    }
    assert.strictEqual(escaped, null, 'emit must not throw synchronously');
    await assert.rejects(promise, /sync boom/);
  });

  await t.test('a throwing listener does not stop the listeners after it', async () => {
    const emitter = new Emitter();
    const ran = [];
    emitter.on('boom', () => {
      ran.push('a');
      throw new Error('sync boom');
    });
    emitter.on('boom', () => ran.push('b'));
    await assert.rejects(emitter.emit('boom'), /sync boom/);
    assert.deepStrictEqual(ran, ['a', 'b']);
  });

  await t.test('an async listener rejection surfaces on the emit promise', async () => {
    const emitter = new Emitter();
    emitter.on('boom', async () => {
      throw new Error('async boom');
    });
    await assert.rejects(emitter.emit('boom'), /async boom/);
  });

  await t.test('emit resolves to undefined whatever a listener returns', async () => {
    const emitter = new Emitter();
    emitter.on('value', () => 42);
    assert.strictEqual(await emitter.emit('value'), undefined);
    const asyncEmitter = new Emitter();
    asyncEmitter.on('value', async () => 42);
    assert.strictEqual(await asyncEmitter.emit('value'), undefined);
  });

  await t.test('emit awaits an async listener before resolving', async () => {
    const emitter = new Emitter();
    let done = false;
    emitter.on('slow', async () => {
      await new Promise((resolve) => setTimeout(resolve, 5));
      done = true;
    });
    await emitter.emit('slow');
    assert.strictEqual(done, true);
  });

  // off() splices without deleting an emptied record, so an event with a live
  // entry and zero listeners is reachable and must still resolve.
  await t.test('emit on an event emptied by off resolves', async () => {
    const emitter = new Emitter();
    const listener = () => {};
    emitter.on('data', listener);
    emitter.off('data', listener);
    assert.strictEqual(emitter.listenerCount('data'), 0);
    await assert.doesNotReject(emitter.emit('data', 1));
  });

  await t.test('a lone once listener fires exactly once and clears the event', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.once('tick', () => calls++);
    await emitter.emit('tick');
    await emitter.emit('tick');
    assert.strictEqual(calls, 1);
    assert.deepStrictEqual(emitter.eventNames(), []);
  });

  await t.test('a once listener that throws still rejects and is still removed', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.once('boom', () => {
      calls++;
      throw new Error('once boom');
    });
    await assert.rejects(emitter.emit('boom'), /once boom/);
    await emitter.emit('boom');
    assert.strictEqual(calls, 1);
  });

  await t.test('an error event with a listener does not throw', async () => {
    const emitter = new Emitter();
    const seen = [];
    emitter.on('error', (error) => seen.push(error.message));
    await emitter.emit('error', new Error('handled'));
    assert.deepStrictEqual(seen, ['handled']);
  });
});

test('jsonParse', () => {
  assert.deepStrictEqual(jsonParse('{"a":1}'), { a: 1 });
  assert.strictEqual(jsonParse(null), null);
  assert.strictEqual(jsonParse('not json'), null);
});

test('Semaphore', async (t) => {
  await t.test('grants slots up to concurrency without waiting', async () => {
    const semaphore = new Semaphore({ concurrency: 2 });
    await semaphore.enter();
    await semaphore.enter();
    assert.strictEqual(semaphore.empty, false);
    semaphore.leave();
    semaphore.leave();
    assert.strictEqual(semaphore.empty, true);
  });

  await t.test('queues waiters and wakes them on leave in FIFO order', async () => {
    const semaphore = new Semaphore({ concurrency: 1, size: 2 });
    await semaphore.enter();
    const order = [];
    const second = semaphore.enter().then(() => order.push('second'));
    const third = semaphore.enter().then(() => order.push('third'));
    semaphore.leave();
    await second;
    semaphore.leave();
    await third;
    assert.deepStrictEqual(order, ['second', 'third']);
    semaphore.leave();
  });

  await t.test('rejects when the queue is full', async () => {
    const semaphore = new Semaphore({ concurrency: 1, size: 0 });
    await semaphore.enter();
    await assert.rejects(semaphore.enter(), /Semaphore queue is full/);
    semaphore.leave();
  });

  await t.test('rejects a queued waiter after the timeout', async () => {
    const semaphore = new Semaphore({ concurrency: 1, size: 1, timeout: 20 });
    await semaphore.enter();
    await assert.rejects(semaphore.enter(), /Semaphore timeout/);
    semaphore.leave();
    // the timed-out waiter must not have been left in the queue
    await semaphore.enter();
    semaphore.leave();
  });

  await t.test('leave without waiters restores capacity, never above concurrency', async () => {
    const semaphore = new Semaphore({ concurrency: 1 });
    semaphore.leave();
    semaphore.leave();
    await semaphore.enter();
    assert.strictEqual(semaphore.empty, false);
    semaphore.leave();
    assert.strictEqual(semaphore.empty, true);
  });
});

test('toKebab', async (t) => {
  // The contract table. Both ends of the wire run this exact function, so a
  // change here is a change to what `schema.headers` must be written against.
  const cases = [
    ['userId', 'user-id'],
    ['userID', 'user-id'], // collides with userId ON PURPOSE — documented, last write wins
    ['user-id', 'user-id'], // already kebab
    ['user_id', 'user_id'], // snake left alone, deliberately
    ['XMLHttpRequest', 'xml-http-request'], // the acronym pass earns its keep here
    ['HTTPServer', 'http-server'],
    ['ABC', 'abc'],
    ['a1B2', 'a1-b2'], // a digit is a boundary left-hand side
    ['x-app-version', 'x-app-version'],
    ['authorization', 'authorization'],
    ['', ''],
  ];
  await t.test('the contract table', () => {
    for (const [input, expected] of cases) assert.strictEqual(toKebab(input), expected, input);
  });

  await t.test('idempotent: the server may normalize what the client already did', () => {
    for (const [input] of cases) {
      const once = toKebab(input);
      assert.strictEqual(toKebab(once), once, input);
    }
  });

  await t.test('output is always lowercase, so a second pass can never split again', () => {
    for (const [input] of cases) assert.strictEqual(toKebab(input), toKebab(input).toLowerCase(), input);
  });
});
