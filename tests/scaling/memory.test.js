'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { MemoryBackplane, createMemoryBackplane, isBackplane } = require('../../scaling.js');

const tick = () => new Promise((resolve) => queueMicrotask(resolve));

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

test('MemoryBackplane: publish and subscribe', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());

  await t.test('a subscriber receives what is published to its channel', async () => {
    const seen = [];
    backplane.subscribe('room:chat', (message) => seen.push(message));
    backplane.publish('room:chat', '{"n":1}');
    assert.deepStrictEqual(seen, [], 'delivery is never synchronous, like a real broker');
    await tick();
    assert.deepStrictEqual(seen, ['{"n":1}']);
  });

  await t.test('other channels are not disturbed', async () => {
    const seen = [];
    backplane.subscribe('room:lobby', (message) => seen.push(message));
    backplane.publish('room:other', 'x');
    await tick();
    assert.deepStrictEqual(seen, []);
  });

  await t.test('every subscriber of a channel gets the message', async () => {
    const first = [];
    const second = [];
    backplane.subscribe('fanout', (message) => first.push(message));
    backplane.subscribe('fanout', (message) => second.push(message));
    backplane.publish('fanout', 'both');
    await tick();
    assert.deepStrictEqual(first, ['both']);
    assert.deepStrictEqual(second, ['both']);
  });

  await t.test('publishing to a channel nobody holds is a no-op', () => {
    assert.doesNotThrow(() => backplane.publish('silent', 'x'));
  });

  await t.test('a non-function handler is rejected', () => {
    assert.throws(() => backplane.subscribe('bad', null), TypeError);
  });
});

test('MemoryBackplane: unsubscribe', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const seen = [];
  const off = backplane.subscribe('room:chat', (message) => seen.push(message));

  await t.test('the returned function removes only that handler', async () => {
    const other = [];
    backplane.subscribe('room:chat', (message) => other.push(message));
    off();
    backplane.publish('room:chat', 'after');
    await tick();
    assert.deepStrictEqual(seen, []);
    assert.deepStrictEqual(other, ['after']);
  });

  await t.test('unsubscribing twice is safe', () => {
    assert.doesNotThrow(off);
  });

  await t.test('a handler removed between publish and delivery is not called', async () => {
    const late = [];
    const remove = backplane.subscribe('late', (message) => late.push(message));
    backplane.publish('late', 'x');
    remove(); // still inside the same microtask turn as publish
    await tick();
    assert.deepStrictEqual(late, []);
  });

  await t.test('the channel is dropped once its last handler leaves', () => {
    const single = new MemoryBackplane();
    const stop = single.subscribe('solo', noop);
    assert.strictEqual(single.size, 1);
    stop();
    assert.strictEqual(single.size, 0);
  });
});

test('MemoryBackplane: isolation and lifecycle', async (t) => {
  await t.test('the prefix namespaces two adapters over one instance', async () => {
    const app = createMemoryBackplane({ prefix: 'app' });
    const other = createMemoryBackplane({ prefix: 'other' });
    t.after(() => {
      app.close();
      other.close();
    });
    const seen = [];
    app.subscribe('room:chat', (message) => seen.push(message));
    other.subscribe('room:chat', (message) => seen.push(message));
    app.publish('room:chat', 'x');
    await tick();
    assert.deepStrictEqual(seen, ['x'], 'prefixes keep the two channel spaces apart');
  });

  await t.test('a throwing handler is isolated and reported', async () => {
    const errors = [];
    const backplane = new MemoryBackplane({ console: { ...quiet, error: (e) => errors.push(e) } });
    t.after(() => backplane.close());
    const seen = [];
    backplane.subscribe('chan', () => {
      throw new Error('handler blew up');
    });
    backplane.subscribe('chan', (message) => seen.push(message));
    backplane.publish('chan', 'x');
    await tick();
    assert.deepStrictEqual(seen, ['x']);
    assert.strictEqual(errors.length, 1);
  });

  await t.test('close() drops every subscription and stops delivery', async () => {
    const backplane = new MemoryBackplane();
    const seen = [];
    backplane.subscribe('chan', (message) => seen.push(message));
    backplane.close();
    assert.strictEqual(backplane.size, 0);
    backplane.publish('chan', 'x');
    await tick();
    assert.deepStrictEqual(seen, []);
  });

  await t.test('it satisfies the structural contract', () => {
    assert.strictEqual(isBackplane(new MemoryBackplane()), true);
    assert.strictEqual(isBackplane({ publish: noop, subscribe: noop }), false);
    assert.strictEqual(isBackplane(null), false);
  });
});
