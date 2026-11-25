'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const { MemoryBackplane, createMemoryBackplane, isBackplane } = require('../../src/scaling/index.js');

const tick = () => new Promise((resolve) => queueMicrotask(resolve));

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

describe('MemoryBackplane', () => {
  describe('publish and subscribe', () => {
    let backplane = null;
    before(() => {
      backplane = new MemoryBackplane();
    });
    after(() => backplane.close());

    it('should deliver to a subscriber what is published to its channel', async () => {
      const seen = [];
      backplane.subscribe('room:chat', (message) => seen.push(message));
      backplane.publish('room:chat', '{"n":1}');
      assert.deepStrictEqual(seen, [], 'delivery is never synchronous, like a real broker');
      await tick();
      assert.deepStrictEqual(seen, ['{"n":1}']);
    });

    it('should not disturb other channels', async () => {
      const seen = [];
      backplane.subscribe('room:lobby', (message) => seen.push(message));
      backplane.publish('room:other', 'x');
      await tick();
      assert.deepStrictEqual(seen, []);
    });

    it('should give the message to every subscriber of a channel', async () => {
      const first = [];
      const second = [];
      backplane.subscribe('fanout', (message) => first.push(message));
      backplane.subscribe('fanout', (message) => second.push(message));
      backplane.publish('fanout', 'both');
      await tick();
      assert.deepStrictEqual(first, ['both']);
      assert.deepStrictEqual(second, ['both']);
    });

    it('should treat publishing to a channel nobody holds as a no-op', () => {
      assert.doesNotThrow(() => backplane.publish('silent', 'x'));
    });

    it('should reject a non-function handler', () => {
      assert.throws(() => backplane.subscribe('bad', null), TypeError);
    });
  });

  describe('unsubscribe', () => {
    const seen = [];
    let backplane = null;
    let off = null;
    before(() => {
      backplane = new MemoryBackplane();
      off = backplane.subscribe('room:chat', (message) => seen.push(message));
    });
    after(() => backplane.close());

    it('should remove only that handler through the returned function', async () => {
      const other = [];
      backplane.subscribe('room:chat', (message) => other.push(message));
      off();
      backplane.publish('room:chat', 'after');
      await tick();
      assert.deepStrictEqual(seen, []);
      assert.deepStrictEqual(other, ['after']);
    });

    it('should be safe to unsubscribe twice', () => {
      assert.doesNotThrow(off);
    });

    it('should not call a handler removed between publish and delivery', async () => {
      const late = [];
      const remove = backplane.subscribe('late', (message) => late.push(message));
      backplane.publish('late', 'x');
      remove(); // still inside the same microtask turn as publish
      await tick();
      assert.deepStrictEqual(late, []);
    });

    it('should drop the channel once its last handler leaves', () => {
      const single = new MemoryBackplane();
      const stop = single.subscribe('solo', noop);
      assert.strictEqual(single.size, 1);
      stop();
      assert.strictEqual(single.size, 0);
    });
  });

  describe('isolation and lifecycle', () => {
    it('should namespace two adapters over one instance by the prefix', async (t) => {
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

    it('should isolate and report a throwing handler', async (t) => {
      const errors = [];
      const backplane = new MemoryBackplane({ logger: { ...quiet, error: (e) => errors.push(e) } });
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

    it('should drop every subscription and stop delivery on close()', async () => {
      const backplane = new MemoryBackplane();
      const seen = [];
      backplane.subscribe('chan', (message) => seen.push(message));
      backplane.close();
      assert.strictEqual(backplane.size, 0);
      backplane.publish('chan', 'x');
      await tick();
      assert.deepStrictEqual(seen, []);
    });

    it('should satisfy the structural contract', () => {
      assert.strictEqual(isBackplane(new MemoryBackplane()), true);
      assert.strictEqual(isBackplane({ publish: noop, subscribe: noop }), false);
      assert.strictEqual(isBackplane(null), false);
    });
  });
});
