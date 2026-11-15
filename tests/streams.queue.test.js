'use strict';

// The queue of a readable: what a stream holds between the chunk that
// arrived and the reader that takes it. How long it gets is the sender's
// choice, so what reading it costs, and what holding it costs, are the
// receiver's to bound. Each test below failed before the fix it names.

const { test } = require('node:test');
const assert = require('node:assert');

const { WrpcReadable } = require('../src/streams.js');

const numbered = (value) => Uint8Array.of(value & 0xff, (value >> 8) & 0xff, (value >> 16) & 0xff);
const valueOf = (chunk) => chunk[0] | (chunk[1] << 8) | (chunk[2] << 16);

// Nanoseconds per chunk to read back `count` chunks that were queued whole.
const readBack = async (count) => {
  const chunk = new Uint8Array(1);
  const readable = new WrpcReadable('s', 'queued', count);
  for (let i = 0; i < count; i++) await readable.push(chunk);
  const ended = readable.end();
  const started = process.hrtime.bigint();
  let read = 0;
  for await (const piece of readable) read += piece.length;
  const elapsed = Number(process.hrtime.bigint() - started);
  await ended;
  assert.strictEqual(read, count);
  return elapsed / count;
};

// `queue.shift()` copied the rest of the array on every read: 1.6 µs a chunk
// at a thousand queued, 88 µs at 256,000 — 22 seconds, on microtasks, with
// the event loop standing still. A ratio, not a time: both sizes run on the
// same machine under the same load, and the quadratic read was 30 times
// slower per chunk at the larger one.
test('a long queue is read back in linear time', async () => {
  await readBack(2000);
  const short = await readBack(8000);
  const long = await readBack(256_000);
  assert.ok(long < short * 8, `${long.toFixed(0)} ns a chunk at 256,000 queued against ${short.toFixed(0)} at 8,000`);
});

test('the queue keeps its order across the moves that keep it short', async () => {
  // A handful of pushes in waiting: ten parked at once is where a readable
  // starts raising its high-water mark instead.
  const total = 6005;
  const readable = new WrpcReadable('s', 'ordered', total * 3);
  // Nobody reads yet, so nothing holds a push back.
  for (let i = 0; i < 6000; i++) await readable.push(numbered(i));
  const seen = [];
  // Past the head's run and past half of the array: what is left moves down.
  for (let i = 0; i < 3500; i++) seen.push(valueOf(await readable.read()));
  assert.strictEqual(readable.queued, 2500);
  // It has a reader now, and is over its high-water mark: these wait for
  // room, in order, behind what is queued.
  const parked = [];
  for (let i = 6000; i < total; i++) parked.push(readable.push(numbered(i)));
  const ended = readable.end();
  for await (const chunk of readable) seen.push(valueOf(chunk));
  await Promise.all(parked);
  await ended;
  assert.strictEqual(seen.length, total);
  for (let i = 0; i < seen.length; i++) assert.strictEqual(seen[i], i);
  assert.strictEqual(readable.queued, 0);
  assert.strictEqual(readable.bytesRead, total * 3);
});

test('readable.queue is a snapshot of what waits, readable.queued its count', async () => {
  const readable = new WrpcReadable('s', 'seen', 5);
  for (let i = 0; i < 5; i++) await readable.push(Uint8Array.of(i));
  await readable.read();
  await readable.read();
  assert.strictEqual(readable.queued, 3);
  const snapshot = readable.queue;
  assert.deepStrictEqual(
    snapshot.map((chunk) => chunk[0]),
    [2, 3, 4],
  );
  // What the stream holds is not reachable through it.
  snapshot.length = 0;
  assert.strictEqual(readable.queued, 3);
  assert.strictEqual((await readable.read())[0], 2);
  await readable.terminate();
  assert.strictEqual(readable.queued, 0);
  assert.deepStrictEqual(readable.queue, []);
});
