'use strict';

// The backplane contract (src/broker/port.js), executable. Run against the
// memory backplane, the Redis adapter over its in-repo fake, and — by hand
// or in the broker CI jobs — every adapter against a real server.
//
// harness: {
//   open(): Promise<[a, b]>    two backplane instances on ONE broker (may
//                              be the same object for an in-process one)
//   close(a, b): Promise        releases what open() created
//   settle?: number             ms to wait after subscribe on a real broker
//   timeout?: number            ms a delivery may take
// }

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const timers = require('node:timers/promises');

const { isBackplane } = require('../../src/scaling/index.js');
const { waitFor, unique } = require('./support.js');

// Registers one `describe(title)` of `it` cases. `options` goes to every case:
// an integration suite's `{ skip }` lists each of them as skipped (a skipped
// describe never runs its body, so its cases would not show at all).
const runBackplaneContract = (title, harness, options = {}) =>
  describe(title, () => {
    const timeout = harness.timeout ?? 2000;
    const settle = harness.settle ?? 0;

    const open = async (t) => {
      const [a, b] = await harness.open();
      t.after(() => harness.close(a, b));
      return [a, b];
    };
    const subscribe = async (backplane, channel, handler) => {
      const unsubscribe = await backplane.subscribe(channel, handler);
      if (settle > 0) await timers.setTimeout(settle);
      return unsubscribe;
    };

    it('should satisfy the structural contract', options, async (t) => {
      const [a, b] = await open(t);
      assert.strictEqual(isBackplane(a), true);
      assert.strictEqual(isBackplane(b), true);
    });

    it('should deliver a message to every subscriber, on both instances', options, async (t) => {
      const [a, b] = await open(t);
      const channel = unique('room:fanout');
      const onA = [];
      const onB = [];
      await subscribe(a, channel, (message) => onA.push(message));
      await subscribe(b, channel, (message) => onB.push(message));
      await a.publish(channel, 'hello');
      await waitFor(() => onA.length === 1 && onB.length === 1, { timeout, message: `a=${onA} b=${onB}` });
      assert.deepStrictEqual([onA[0], onB[0]], ['hello', 'hello']);
    });

    it('should carry the message string byte for byte', options, async (t) => {
      const [a, b] = await open(t);
      const channel = unique('room:bytes');
      const seen = [];
      await subscribe(b, channel, (message) => seen.push(message));
      const payloads = [
        JSON.stringify({ v: 1, text: 'привіт 👋', nested: { list: [1, 2, 3] } }),
        'x'.repeat(64 * 1024),
        '',
      ];
      for (const payload of payloads) await a.publish(channel, payload);
      await waitFor(() => seen.length === payloads.length, { timeout });
      assert.deepStrictEqual(seen, payloads);
    });

    it('should take channel names literally — metacharacters never widen a subscription', options, async (t) => {
      const [a, b] = await open(t);
      const base = unique('room');
      const names = [
        `${base}:*`,
        `${base}:>`,
        `${base}:a`,
        `${base}:a.b`,
        `${base}:a b`,
        `${base}:кімната`,
        `${base}:#`,
      ];
      const seen = new Map(names.map((channel) => [channel, []]));
      for (const channel of names) await subscribe(b, channel, (message) => seen.get(channel).push(message));
      for (const channel of names) await a.publish(channel, channel);
      await waitFor(() => names.every((channel) => seen.get(channel).length >= 1), { timeout });
      // Give a wrongly-widened subscription the time to show itself.
      await timers.setTimeout(Math.max(50, settle));
      for (const channel of names) assert.deepStrictEqual(seen.get(channel), [channel], `channel ${channel}`);
    });

    it('should hold several handlers on one channel, removed independently', options, async (t) => {
      const [a, b] = await open(t);
      const channel = unique('room:handlers');
      const first = [];
      const second = [];
      const stopFirst = await subscribe(b, channel, (message) => first.push(message));
      await subscribe(b, channel, (message) => second.push(message));
      await a.publish(channel, 'one');
      await waitFor(() => first.length === 1 && second.length === 1, { timeout });
      await stopFirst();
      await a.publish(channel, 'two');
      await waitFor(() => second.length === 2, { timeout });
      await timers.setTimeout(Math.max(50, settle));
      assert.deepStrictEqual(first, ['one']);
      assert.deepStrictEqual(second, ['one', 'two']);
    });

    it('should make unsubscribe idempotent and stop delivery', options, async (t) => {
      const [a, b] = await open(t);
      const channel = unique('room:unsub');
      const seen = [];
      const unsubscribe = await subscribe(b, channel, (message) => seen.push(message));
      await a.publish(channel, 'before');
      await waitFor(() => seen.length === 1, { timeout });
      await unsubscribe();
      await unsubscribe();
      if (settle > 0) await timers.setTimeout(settle);
      await a.publish(channel, 'after');
      await timers.setTimeout(Math.max(50, settle));
      assert.deepStrictEqual(seen, ['before']);
    });

    it('should never deliver synchronously inside publish()', options, async (t) => {
      const [a] = await open(t);
      const channel = unique('room:async');
      const seen = [];
      await subscribe(a, channel, (message) => seen.push(message));
      const result = a.publish(channel, 'x');
      assert.deepStrictEqual(seen, [], 'a handler ran inside publish()');
      await result;
      await waitFor(() => seen.length === 1, { timeout });
    });

    it('should not let a throwing handler stop the others', options, async (t) => {
      const [a, b] = await open(t);
      const channel = unique('room:throw');
      const seen = [];
      await subscribe(b, channel, () => {
        throw new Error('handler bug');
      });
      await subscribe(b, channel, (message) => seen.push(message));
      await a.publish(channel, 'still delivered');
      await waitFor(() => seen.length === 1, { timeout });
    });
  });

module.exports = { runBackplaneContract };
