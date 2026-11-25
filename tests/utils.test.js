'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { Emitter, jsonParse, Semaphore, toKebab } = require('../src/utils.js');

describe('Emitter', () => {
  it('should resolve emit without listeners for a non-error event', async () => {
    const emitter = new Emitter();
    await assert.doesNotReject(emitter.emit('idle'));
  });

  it('should throw Unhandled error from emit when an error event has no listener', () => {
    const emitter = new Emitter();
    assert.throws(() => emitter.emit('error', new Error('boom')), /Unhandled error/);
  });

  it('should call every listener with the value on emit', async () => {
    const emitter = new Emitter();
    const received = [];
    emitter.on('data', (value) => received.push(value));
    emitter.on('data', (value) => received.push(value * 2));
    await emitter.emit('data', 5);
    assert.deepStrictEqual(received, [5, 10]);
  });

  it('should fire a once listener only a single time', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.once('tick', () => calls++);
    await emitter.emit('tick');
    await emitter.emit('tick');
    assert.strictEqual(calls, 1);
  });

  it('should keep a persistent listener registered alongside a once one', async () => {
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

  it('should throw from addListener on a duplicate listener registration', () => {
    const emitter = new Emitter();
    const listener = () => {};
    emitter.on('data', listener);
    assert.throws(() => emitter.on('data', listener), /Duplicate listeners detected/);
  });

  it('should warn from addListener past maxListeners instead of throwing', () => {
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

  it('should send the warning where warn says, and nowhere for null', () => {
    const warnings = [];
    const routed = new Emitter({ maxListeners: 1, warn: (message) => warnings.push(message) });
    routed.on('data', () => {});
    routed.on('data', () => {});
    assert.strictEqual(warnings.length, 1);
    assert.match(warnings[0], /MaxListenersExceededWarning: Possible data memory leak/);
    const printed = [];
    const originalWarn = globalThis.console.warn;
    globalThis.console.warn = (message) => printed.push(message);
    try {
      const silent = new Emitter({ maxListeners: 1, warn: null });
      silent.on('data', () => {});
      silent.on('data', () => {});
    } finally {
      globalThis.console.warn = originalWarn;
    }
    assert.deepStrictEqual(printed, []);
  });

  it('should stay silent for an owner with logger: false, and warn through the logger of one that has one', () => {
    const { RpcServer, defineRouter } = require('../index.js');
    const printed = [];
    const originalWarn = globalThis.console.warn;
    globalThis.console.warn = (message) => printed.push(message);
    const lines = [];
    const logger = { warn: (...args) => lines.push(args), error() {}, info() {}, debug() {}, log() {} };
    try {
      const quiet = new RpcServer({ router: defineRouter({}), logger: false });
      for (let i = 0; i < 12; i++) quiet.on('data', () => {});
      const rpc = new RpcServer({ router: defineRouter({}), logger });
      for (let i = 0; i < 12; i++) rpc.on('data', () => {});
    } finally {
      globalThis.console.warn = originalWarn;
    }
    assert.deepStrictEqual(printed, []);
    assert.ok(lines.some((args) => args.some((part) => /MaxListenersExceededWarning/.test(String(part)))));
  });

  it('should remove a specific listener with off', async () => {
    const emitter = new Emitter();
    let calls = 0;
    const listener = () => calls++;
    emitter.on('data', listener);
    emitter.off('data', listener);
    await emitter.emit('data');
    assert.strictEqual(calls, 0);
  });

  it('should remove the whole event with off without a listener', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.on('data', () => calls++);
    emitter.off('data');
    await emitter.emit('data');
    assert.strictEqual(calls, 0);
  });

  it('should do nothing on off for an unknown event', () => {
    const emitter = new Emitter();
    assert.doesNotThrow(() => emitter.off('unknown', () => {}));
  });

  it('should remove a single event with clear', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.on('data', () => calls++);
    emitter.clear('data');
    await emitter.emit('data');
    assert.strictEqual(calls, 0);
  });

  it('should remove every event with clear and no argument', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.on('data', () => calls++);
    emitter.on('other', () => calls++);
    emitter.clear();
    assert.deepStrictEqual(emitter.eventNames(), []);
  });

  it('should return the registered handlers from listeners', () => {
    const emitter = new Emitter();
    const listener = () => {};
    emitter.on('data', listener);
    assert.deepStrictEqual(emitter.listeners('data'), [listener]);
    assert.deepStrictEqual(emitter.listeners('unknown'), []);
  });

  it('should throw from listeners without an eventName', () => {
    const emitter = new Emitter();
    assert.throws(() => emitter.listeners(), /Expected eventName/);
  });

  it('should report the number of handlers from listenerCount', () => {
    const emitter = new Emitter();
    emitter.on('data', () => {});
    emitter.on('data', () => {});
    assert.strictEqual(emitter.listenerCount('data'), 2);
    assert.strictEqual(emitter.listenerCount('unknown'), 0);
  });

  it('should throw from listenerCount without an eventName', () => {
    const emitter = new Emitter();
    assert.throws(() => emitter.listenerCount(), /Expected eventName/);
  });

  it('should list every registered event from eventNames', () => {
    const emitter = new Emitter();
    emitter.on('data', () => {});
    emitter.on('close', () => {});
    assert.deepStrictEqual(emitter.eventNames().sort(), ['close', 'data']);
  });

  // The contract every emit() shape must preserve, characterized here because
  // `void emitter.emit('close')` is the house style: a listener's SYNCHRONOUS
  // throw has to surface as a rejection, never as a throw escaping into a
  // socket handler. Written against the pre-fast-path implementation.
  it('should reject instead of throwing when a listener throws synchronously', async () => {
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

  it('should still call the listeners after one that throws', async () => {
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

  it('should surface an async listener rejection on the emit promise', async () => {
    const emitter = new Emitter();
    emitter.on('boom', async () => {
      throw new Error('async boom');
    });
    await assert.rejects(emitter.emit('boom'), /async boom/);
  });

  it('should resolve emit to undefined whatever a listener returns', async () => {
    const emitter = new Emitter();
    emitter.on('value', () => 42);
    assert.strictEqual(await emitter.emit('value'), undefined);
    const asyncEmitter = new Emitter();
    asyncEmitter.on('value', async () => 42);
    assert.strictEqual(await asyncEmitter.emit('value'), undefined);
  });

  it('should await an async listener before emit resolves', async () => {
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
  it('should resolve emit on an event emptied by off', async () => {
    const emitter = new Emitter();
    const listener = () => {};
    emitter.on('data', listener);
    emitter.off('data', listener);
    assert.strictEqual(emitter.listenerCount('data'), 0);
    await assert.doesNotReject(emitter.emit('data', 1));
  });

  it('should fire a lone once listener exactly once and clear the event', async () => {
    const emitter = new Emitter();
    let calls = 0;
    emitter.once('tick', () => calls++);
    await emitter.emit('tick');
    await emitter.emit('tick');
    assert.strictEqual(calls, 1);
    assert.deepStrictEqual(emitter.eventNames(), []);
  });

  it('should still reject and remove a once listener that throws', async () => {
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

  it('should not throw for an error event with a listener', async () => {
    const emitter = new Emitter();
    const seen = [];
    emitter.on('error', (error) => seen.push(error.message));
    await emitter.emit('error', new Error('handled'));
    assert.deepStrictEqual(seen, ['handled']);
  });
});

describe('jsonParse', () => {
  it('should parse JSON, and answer null for null and for what is not JSON', () => {
    assert.deepStrictEqual(jsonParse('{"a":1}'), { a: 1 });
    assert.strictEqual(jsonParse(null), null);
    assert.strictEqual(jsonParse('not json'), null);
  });
});

describe('Semaphore', () => {
  it('should grant slots up to concurrency without waiting', async () => {
    const semaphore = new Semaphore({ concurrency: 2 });
    await semaphore.enter();
    await semaphore.enter();
    assert.strictEqual(semaphore.empty, false);
    semaphore.leave();
    semaphore.leave();
    assert.strictEqual(semaphore.empty, true);
  });

  it('should queue waiters and wake them on leave in FIFO order', async () => {
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

  it('should reject when the queue is full', async () => {
    const semaphore = new Semaphore({ concurrency: 1, size: 0 });
    await semaphore.enter();
    await assert.rejects(semaphore.enter(), /Semaphore queue is full/);
    semaphore.leave();
  });

  it('should reject a queued waiter after the timeout', async () => {
    const semaphore = new Semaphore({ concurrency: 1, size: 1, timeout: 20 });
    await semaphore.enter();
    await assert.rejects(semaphore.enter(), /Semaphore timeout/);
    semaphore.leave();
    // the timed-out waiter must not have been left in the queue
    await semaphore.enter();
    semaphore.leave();
  });

  it('should restore capacity on leave without waiters, never above concurrency', async () => {
    const semaphore = new Semaphore({ concurrency: 1 });
    semaphore.leave();
    semaphore.leave();
    await semaphore.enter();
    assert.strictEqual(semaphore.empty, false);
    semaphore.leave();
    assert.strictEqual(semaphore.empty, true);
  });
});

describe('toKebab', () => {
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
  it('should hold the contract table', () => {
    for (const [input, expected] of cases) assert.strictEqual(toKebab(input), expected, input);
  });

  it('should be idempotent: the server may normalize what the client already did', () => {
    for (const [input] of cases) {
      const once = toKebab(input);
      assert.strictEqual(toKebab(once), once, input);
    }
  });

  it('should always output lowercase, so a second pass can never split again', () => {
    for (const [input] of cases) assert.strictEqual(toKebab(input), toKebab(input).toLowerCase(), input);
  });
});
