'use strict';

// The queue of a readable: what a stream holds between the chunk that
// arrived and the reader that takes it. How long it gets is the sender's
// choice, so what reading it costs, and what holding it costs, are the
// receiver's to bound. Each test below failed before the fix it names.

const { test } = require('node:test');
const assert = require('node:assert');

const { WrpcReadable, heldBytes, CHUNK_COST } = require('../src/streams.js');
const { defineRouter, procedure, RpcServer } = require('../index.js');
const { ServerTransport } = require('../src/rpc/serverTransport.js');
const { chunkEncode } = require('../src/chunks.js');
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

test('heldBytes sums what readers have not taken, each queued chunk with its cost', async () => {
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
  // Where the carrier paces a stream that has a reader, only the unread one
  // counts: two chunks of thirty bytes. The one that is over holds nothing.
  assert.strictEqual(heldBytes(streams, true), 30 + 2 * CHUNK_COST);
  // Where it cannot, what the reader is behind by counts as well.
  assert.strictEqual(heldBytes(streams, false), 30 + 2 * CHUNK_COST + 40 + CHUNK_COST);
});

// A carrier `RpcServer.attach` binds — a WebRTC channel, a broker session —
// has no socket to pause: what the peer sends arrives, whatever the reader does.
class Attached extends ServerTransport {
  sent = [];

  constructor() {
    super();
    this.connection = {};
  }

  write(data) {
    this.sent.push(data);
    return true;
  }

  close() {
    this.emit('close');
  }

  answer(id) {
    for (const data of this.sent) {
      if (typeof data !== 'string') continue;
      const packet = JSON.parse(data);
      if (packet.id === id) return packet;
    }
    return undefined;
  }
}

// A readable whose reader fell a thousand chunks behind threw a plain error
// for the chunk it could not take — a chunk push() had already counted. The
// stream went on, short of bytes it had "received": its `end` passed the size
// check and its reader waited for the dropped chunks for as long as the
// connection lived. On uWebSockets.js a 100 MiB upload into a sink that
// stalled once was read to 75.2 MiB and never answered.
test('a reader that falls too far behind fails the stream; nothing is dropped in silence', async (t) => {
  // Every other chunk raises the high-water mark by one and waits, the one
  // behind it is queued: some two thousand of them reach the limit.
  const chunks = 2600;
  const readable = new WrpcReadable('s', 'behind', chunks);
  const first = Uint8Array.of(1);
  await readable.push(first);
  assert.strictEqual(await readable.read(), first); // it has a reader, which now stalls
  const warned = [];
  t.mock.method(console, 'warn', (...args) => warned.push(args));
  const settle = (push) =>
    push.then(
      () => null,
      (error) => error,
    );
  const pushes = [];
  for (let i = 1; i < chunks; i++) pushes.push(settle(readable.push(Uint8Array.of(1))));
  const outcomes = await Promise.all(pushes);
  // A chunk waiting for room is no leak: a line on the console for each was
  // a thousand lines a peer could have a server print.
  assert.deepStrictEqual(warned, []);
  const rejected = outcomes.filter((error) => error !== null);
  assert.strictEqual(rejected.length, 1, 'the stream fails once, for the chunk that went too far');
  assert.strictEqual(rejected[0], readable.error);
  assert.strictEqual(readable.error.code, 413);
  assert.deepStrictEqual([readable.streaming, readable.status, readable.queued], [false, 'terminated', 0]);
  // Its sender's end is not short of anything — and does not hang.
  await readable.end();
  // Its reader is told why, where it used to wait for the missing chunks.
  await assert.rejects(
    readable
      .read()
      .then((chunk) => assert.strictEqual(chunk, null))
      .then(async () => {
        for await (const chunk of readable) assert.fail(`read ${chunk.length} bytes of a failed stream`);
      }),
    (error) => error === readable.error,
  );
});

test('a carrier with no socket to pause: a stalled handler is answered, not left waiting', async () => {
  let release;
  const stalled = new Promise((resolve) => {
    release = resolve;
  });
  const rpc = new RpcServer({
    logger: false,
    router: defineRouter({
      files: {
        store: procedure({
          access: 'public',
          handler: async (context, { id }) => {
            let stored = 0;
            for await (const chunk of context.client.getStream(id)) {
              stored += chunk.length;
              if (stored === chunk.length) await stalled; // the sink stalls once, then catches up
            }
            return stored;
          },
        }),
      },
    }),
  });
  const transport = new Attached();
  rpc.attach(transport, { session: {} });
  const chunks = 1600;
  transport.emit('packet', JSON.stringify({ type: 'stream', id: 'up', name: 'big', size: chunks * KIB }));
  transport.emit('packet', JSON.stringify({ type: 'call', id: 'c', method: 'files/store', args: { id: 'up' } }));
  for (let i = 0; i < chunks; i++) {
    transport.emit('chunk', chunkEncode('up', new Uint8Array(KIB)));
    if (i % 100 === 99) await new Promise((resolve) => setImmediate(resolve));
  }
  transport.emit('packet', JSON.stringify({ type: 'stream', id: 'up', status: 'end' }));
  release();
  await waitFor(() => transport.answer('c'), 'the handler of a stream that lost chunks was never answered');
  assert.strictEqual(transport.answer('c').error.code, 413);
  // The sender heard once, on the stream's id — not once per dropped chunk.
  const told = transport.sent.filter((data) => typeof data === 'string' && JSON.parse(data).id === 'up');
  assert.strictEqual(told.length, 1);
  assert.strictEqual(JSON.parse(told[0]).error.code, 413);
  transport.close();
  await rpc.close();
});

// `maxStreamBuffer` counted the streams nobody reads. One that HAS a reader
// was left to its high-water mark — which holds the sender back only where
// the carrier can be paused. Over `attach()` (and on uWebSockets.js) a
// handler that read one chunk and stalled held 116 MiB past a 64 MiB cap.
test('a carrier with no socket to pause: a reader that is behind is held to maxStreamBuffer', async () => {
  let reading;
  const started = new Promise((resolve) => {
    reading = resolve;
  });
  const rpc = new RpcServer({
    logger: false,
    maxStreamBuffer: 1 * MIB,
    router: defineRouter({
      files: {
        stall: procedure({
          access: 'public',
          handler: async (context, { id }) => {
            const stream = context.client.getStream(id);
            await stream.read();
            reading(stream);
            await new Promise(() => {}); // one chunk, and the sink never comes back
          },
        }),
      },
    }),
  });
  const transport = new Attached();
  const peer = rpc.attach(transport, { session: {} });
  assert.strictEqual(peer.paced, false);
  transport.emit('packet', JSON.stringify({ type: 'stream', id: 'up', name: 'big', size: 64 * MIB }));
  transport.emit('chunk', chunkEncode('up', new Uint8Array(64 * KIB)));
  transport.emit('packet', JSON.stringify({ type: 'call', id: 'c', method: 'files/stall', args: { id: 'up' } }));
  const stream = await started;
  let sent = 0;
  while (stream.streaming && sent < 200) {
    transport.emit('chunk', chunkEncode('up', new Uint8Array(64 * KIB)));
    sent++;
  }
  // Refused once what its reader is behind by passed a megabyte: some
  // sixteen chunks of 64 KiB, not the thousand its queue would have taken.
  assert.ok(sent <= 40, `${sent} chunks of 64 KiB were taken under a 1 MiB cap`);
  assert.deepStrictEqual([stream.error.code, stream.queued], [413, 0]);
  assert.match(stream.error.message, /too far ahead of its reader/);
  assert.strictEqual(transport.answer('up').error.code, 413);
  transport.close();
  await rpc.close();
});

// The terminate of a stream was awaited before the stream was forgotten, and
// the packet behind it is dispatched in the same turn: at `maxStreams` a
// sender that gave one stream up to open another was answered 429 for a
// slot it had just freed. A sender told to stop does exactly that.
test('a terminate frees its slot for the announcement right behind it', async (t) => {
  const { url } = await bootServer(t, { router: router(), maxStreams: 2 });
  const peer = await rawPeer(t, url);
  peer.announce('one', 10);
  peer.announce('two', 10);
  await peer.settle();
  // Two frames written back to back.
  peer.end('one', 'terminate');
  peer.announce('three', 10);
  await peer.settle();
  assert.strictEqual(peer.answer('three'), undefined, 'the slot a terminate freed was still counted');
  peer.call('h', 'files/held');
  await waitFor(() => peer.answer('h'), 'held');
  assert.deepStrictEqual(
    peer.answer('h').result.map((stream) => stream.id),
    ['two', 'three'],
  );
});
