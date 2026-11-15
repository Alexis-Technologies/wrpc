'use strict';

// The queue of a readable: what a stream holds between the chunk that
// arrived and the reader that takes it. How long it gets is the sender's
// choice, so what reading it costs, and what holding it costs, are the
// receiver's to bound. Each test below failed before the fix it names.

const { test } = require('node:test');
const assert = require('node:assert');

const { WrpcReadable, heldBytes, CHUNK_COST } = require('../src/streams.js');
const { defineRouter, procedure } = require('../index.js');
const { bootServer, waitFor } = require('./helpers/server.js');
const { KIB, MIB, rawPeer } = require('./helpers/streams.js');

// What a connection holds of the peer's streams, as the server sees it.
const router = () =>
  defineRouter({
    files: {
      ping: procedure({ access: 'public', handler: async () => 'pong' }),
      held: procedure({
        access: 'public',
        handler: async (context) => {
          const streams = [];
          for (const stream of context.client.streams.values()) {
            streams.push({
              id: stream.id,
              queued: stream.queued,
              received: stream.bytesReceived,
              status: stream.status,
              code: stream.error === null ? null : stream.error.code,
            });
          }
          return streams;
        },
      }),
    },
  });

const held = async (peer, id) => {
  const call = `held-${peer.answers.length}-${Math.random()}`;
  peer.call(call, 'files/held');
  await waitFor(() => peer.answer(call), 'what the server holds');
  return peer.answer(call).result.find((stream) => stream.id === id);
};

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

// A chunk of no bytes counted for nothing — not against the announced size,
// not against `maxStreamBuffer` — and each one took a slot in the queue: a
// million and a half of them grew a server by 305 MiB under a 1 MiB cap.
test('a chunk of no bytes is not kept', async (t) => {
  const readable = new WrpcReadable('s', 'empty', 4);
  assert.strictEqual(await readable.push(new Uint8Array(0)), undefined);
  assert.strictEqual(readable.queued, 0);
  await readable.push(Uint8Array.of(1, 2, 3, 4));
  assert.strictEqual(readable.queued, 1);

  const { url } = await bootServer(t, { router: router(), maxStreamBuffer: 1 * MIB });
  const peer = await rawPeer(t, url);
  peer.announce('void', 1 * MIB);
  for (let i = 0; i < 20_000; i++) peer.chunk('void', new Uint8Array(0));
  await peer.settle();
  assert.deepStrictEqual(await held(peer, 'void'), {
    id: 'void',
    queued: 0,
    received: 0,
    status: 'active',
    code: null,
  });
  assert.strictEqual(peer.answer('void'), undefined, 'a chunk of no bytes was answered');
});

// `maxStreamBuffer` counted the bytes of a chunk and nothing of what holding
// one costs: 0.95 MiB of one-byte chunks passed under a 1 MiB cap and grew
// the server by 204 MiB. A chunk is counted as its bytes plus CHUNK_COST.
test('what a chunk costs to hold counts against maxStreamBuffer, not its bytes alone', async (t) => {
  const { url } = await bootServer(t, { router: router(), maxStreamBuffer: 1 * MIB });
  const peer = await rawPeer(t, url);
  peer.announce('drip', 1 * MIB);
  const one = new Uint8Array(1);
  for (let i = 0; i < 6000; i++) peer.chunk('drip', one);
  await waitFor(() => peer.answer('drip'), 'the stream that went past the cap was never refused');
  assert.strictEqual(peer.answer('drip').error.code, 413);
  const stream = await held(peer, 'drip');
  // It failed where the cap was reached — a megabyte in chunks that cost
  // 257 bytes each — and let go of what it held.
  assert.ok(stream.received <= Math.ceil((1 * MIB) / (1 + CHUNK_COST)), `${stream.received} one-byte chunks were held`);
  assert.deepStrictEqual([stream.queued, stream.status, stream.code], [0, 'terminated', 413]);
  // A stream of ordinary chunks still has the whole megabyte, less a chunk's cost each.
  peer.end('drip', 'terminate');
  peer.announce('plain', 1 * MIB);
  for (let i = 0; i < 15; i++) peer.chunk('plain', new Uint8Array(64 * KIB));
  await peer.settle();
  assert.strictEqual(peer.answer('plain'), undefined, 'fifteen 64 KiB chunks did not fit under a megabyte');
});

test('heldBytes sums the streams nobody reads, each chunk with its cost', async () => {
  const streams = new Map();
  const unread = new WrpcReadable('a', 'unread', 100);
  const read = new WrpcReadable('b', 'read', 100);
  const over = new WrpcReadable('c', 'over', 100);
  streams.set('a', unread).set('b', read).set('c', over);
  await unread.push(new Uint8Array(10));
  await unread.push(new Uint8Array(20));
  await read.push(new Uint8Array(40));
  await read.read();
  await read.push(new Uint8Array(40));
  await over.push(new Uint8Array(50));
  await over.terminate();
  // Two chunks of thirty bytes: the one being read is paced by its reader,
  // the one that is over holds nothing.
  assert.strictEqual(heldBytes(streams), 30 + 2 * CHUNK_COST);
});
