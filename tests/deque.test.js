'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { Deque } = require('../src/deque.js');

const drain = (deque) => {
  const out = [];
  for (let value = deque.shift(); value !== undefined; value = deque.shift()) out.push(value);
  return out;
};

describe('Deque', () => {
  it('should be first in, first out, across growth and wrap-around', () => {
    const deque = new Deque();
    for (let i = 0; i < 10; i++) deque.push(i);
    assert.deepStrictEqual([deque.shift(), deque.shift(), deque.shift()], [0, 1, 2]);
    // The head has moved: what is pushed now wraps around the end of the
    // slots, and growing has to put it back in order.
    for (let i = 10; i < 100; i++) deque.push(i);
    assert.strictEqual(deque.length, 97);
    assert.deepStrictEqual(
      drain(deque),
      Array.from({ length: 97 }, (_, i) => i + 3),
    );
    assert.strictEqual(deque.length, 0);
  });

  it('should answer undefined when empty, as an array does', () => {
    const deque = new Deque();
    assert.strictEqual(deque.shift(), undefined);
    assert.strictEqual(deque.peek(), undefined);
    deque.push('a');
    assert.strictEqual(deque.shift(), 'a');
    assert.strictEqual(deque.shift(), undefined);
    assert.strictEqual(deque.length, 0);
  });

  it('should peek at the head and leave it where it is', () => {
    const deque = new Deque();
    const first = { n: 1 };
    deque.push(first);
    deque.push({ n: 2 });
    assert.strictEqual(deque.peek(), first);
    assert.strictEqual(deque.length, 2);
    assert.strictEqual(deque.shift(), first);
    assert.strictEqual(deque.peek().n, 2);
  });

  it('should put a value back at the head with unshift', () => {
    const deque = new Deque();
    for (let i = 0; i < 20; i++) deque.push(i);
    const taken = deque.shift();
    deque.unshift(taken);
    deque.unshift(-1);
    assert.deepStrictEqual(drain(deque).slice(0, 3), [-1, 0, 1]);
  });

  it('should let go of everything on clear, and work again after it', () => {
    const deque = new Deque();
    for (let i = 0; i < 1000; i++) deque.push(i);
    deque.clear();
    assert.strictEqual(deque.length, 0);
    assert.strictEqual(deque.peek(), undefined);
    assert.strictEqual(deque.shift(), undefined);
    deque.push('again');
    assert.deepStrictEqual(drain(deque), ['again']);
  });

  it('should leave nothing for the next look when cleared from inside a drain', () => {
    // What a WebSocket connection does when a listener terminates it from
    // inside the loop that delivers its queue.
    const deque = new Deque();
    for (let i = 0; i < 50; i++) deque.push(i);
    const seen = [];
    while (deque.length > 0) {
      seen.push(deque.shift());
      if (seen.length === 3) deque.clear();
    }
    assert.deepStrictEqual(seen, [0, 1, 2]);
  });

  it('should take from the head in time linear in what was queued', () => {
    // The reason it exists: an array read with shift() moves the rest on every
    // take past a few thousand elements. A ratio, not an absolute — it holds on
    // any machine, loaded or not.
    const time = (n) => {
      const deque = new Deque();
      for (let i = 0; i < n; i++) deque.push(i);
      const started = process.hrtime.bigint();
      while (deque.length > 0) deque.shift();
      return Number(process.hrtime.bigint() - started);
    };
    time(8_000);
    const small = Math.max(time(8_000), 1);
    const large = time(256_000);
    // 32× the elements: linear is 32×, an array's shift() is several hundred.
    assert.ok(large / small < 200, `256k took ${(large / small).toFixed(0)}× the time of 8k`);
  });
});
