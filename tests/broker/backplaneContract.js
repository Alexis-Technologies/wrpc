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

const assert = require('node:assert');
const timers = require('node:timers/promises');

const { isBackplane } = require('../../src/scaling/index.js');
const { waitFor, unique } = require('./support.js');

const runBackplaneContract = async (t, name, harness) => {
  const timeout = harness.timeout ?? 2000;
  const settle = harness.settle ?? 0;

  const open = async (sub) => {
    const [a, b] = await harness.open();
    sub.after(() => harness.close(a, b));
    return [a, b];
  };
  const subscribe = async (backplane, channel, handler) => {
    const unsubscribe = await backplane.subscribe(channel, handler);
    if (settle > 0) await timers.setTimeout(settle);
    return unsubscribe;
  };

  await t.test(`${name}: satisfies the structural contract`, async (sub) => {
    const [a, b] = await open(sub);
    assert.strictEqual(isBackplane(a), true);
    assert.strictEqual(isBackplane(b), true);
  });

  await t.test(`${name}: a message reaches every subscriber, on both instances`, async (sub) => {
    const [a, b] = await open(sub);
    const channel = unique('room:fanout');
    const onA = [];
    const onB = [];
    await subscribe(a, channel, (message) => onA.push(message));
    await subscribe(b, channel, (message) => onB.push(message));
    await a.publish(channel, 'hello');
    await waitFor(() => onA.length === 1 && onB.length === 1, { timeout, message: `a=${onA} b=${onB}` });
    assert.deepStrictEqual([onA[0], onB[0]], ['hello', 'hello']);
  });

  await t.test(`${name}: the message string survives byte for byte`, async (sub) => {
    const [a, b] = await open(sub);
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

  await t.test(`${name}: channel names are literal — metacharacters never widen a subscription`, async (sub) => {
    const [a, b] = await open(sub);
    const base = unique('room');
    const names = [`${base}:*`, `${base}:>`, `${base}:a`, `${base}:a.b`, `${base}:a b`, `${base}:кімната`, `${base}:#`];
    const seen = new Map(names.map((channel) => [channel, []]));
    for (const channel of names) await subscribe(b, channel, (message) => seen.get(channel).push(message));
    for (const channel of names) await a.publish(channel, channel);
    await waitFor(() => names.every((channel) => seen.get(channel).length >= 1), { timeout });
    // Give a wrongly-widened subscription the time to show itself.
    await timers.setTimeout(Math.max(50, settle));
    for (const channel of names) assert.deepStrictEqual(seen.get(channel), [channel], `channel ${channel}`);
  });

  await t.test(`${name}: several handlers on one channel, removed independently`, async (sub) => {
    const [a, b] = await open(sub);
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

  await t.test(`${name}: unsubscribe is idempotent and stops delivery`, async (sub) => {
    const [a, b] = await open(sub);
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

  await t.test(`${name}: delivery is never synchronous inside publish()`, async (sub) => {
    const [a] = await open(sub);
    const channel = unique('room:async');
    const seen = [];
    await subscribe(a, channel, (message) => seen.push(message));
    const result = a.publish(channel, 'x');
    assert.deepStrictEqual(seen, [], 'a handler ran inside publish()');
    await result;
    await waitFor(() => seen.length === 1, { timeout });
  });

  await t.test(`${name}: a throwing handler does not stop the others`, async (sub) => {
    const [a, b] = await open(sub);
    const channel = unique('room:throw');
    const seen = [];
    await subscribe(b, channel, () => {
      throw new Error('handler bug');
    });
    await subscribe(b, channel, (message) => seen.push(message));
    await a.publish(channel, 'still delivered');
    await waitFor(() => seen.length === 1, { timeout });
  });
};

module.exports = { runBackplaneContract };
