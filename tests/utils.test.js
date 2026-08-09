'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { Emitter, jsonParse } = require('../src/utils.js');

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

  await t.test('addListener throws past maxListeners', () => {
    const emitter = new Emitter({ maxListeners: 1 });
    emitter.on('data', () => {});
    assert.throws(() => emitter.on('data', () => {}), /MaxListenersExceededWarning/);
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
});

test('jsonParse', () => {
  assert.deepStrictEqual(jsonParse('{"a":1}'), { a: 1 });
  assert.strictEqual(jsonParse(null), null);
  assert.strictEqual(jsonParse('not json'), null);
});
