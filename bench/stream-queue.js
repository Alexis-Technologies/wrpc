/**
 * The queue of a `WrpcReadable` — what a stream holds between the chunk that
 * arrived and the reader that takes it. It is read from its head, and how
 * long it gets is the SENDER's choice: a stream nobody reads yet keeps every
 * chunk, and a megabyte sent a byte at a time is a million of them.
 *
 * It used to be an array read with `shift()`. V8 trims a short array from the
 * left in place, but past a few thousand elements every `shift` copies the
 * rest: reading N queued chunks back costs N² / 2 moves, on microtasks, with
 * no turn of the event loop in between. A million one-byte chunks — 9 MiB on
 * the wire — stalled a server for 350 seconds.
 *
 * The rows below are the two spellings side by side, filled to N and read
 * back, in chunks per second: `shift()` falls off a cliff, the head cursor
 * (src/streams.js, `pull`) stays flat. The last rows are the readable itself.
 *
 * Run with `node bench/stream-queue.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { WrpcReadable } = require('../src/streams.js');

const CHUNK = new Uint8Array(1);
const COMPACT_AT = 1024;

let sink = 0;

const withShift = (n) => {
  const queue = [];
  for (let i = 0; i < n; i++) queue.push(CHUNK);
  while (queue.length > 0) sink += queue.shift().length;
};

// The cursor as the readable keeps it: the slot is let go of, the array is
// emptied when the head reaches its end and moved down once the head has
// passed half of it.
const withCursor = (n) => {
  const chunks = [];
  let head = 0;
  for (let i = 0; i < n; i++) chunks.push(CHUNK);
  while (head < chunks.length) {
    const chunk = chunks[head];
    chunks[head++] = undefined;
    if (head === chunks.length) {
      chunks.length = 0;
      head = 0;
    } else if (head >= COMPACT_AT && head * 2 >= chunks.length) {
      chunks.copyWithin(0, head);
      chunks.length -= head;
      head = 0;
    }
    sink += chunk.length;
  }
};

const readBack = async (n) => {
  const readable = new WrpcReadable('s', 'bench', n);
  for (let i = 0; i < n; i++) await readable.push(CHUNK);
  const ended = readable.end();
  for await (const chunk of readable) sink += chunk.length;
  await ended;
};

const main = async () => {
  console.log(`Stream queue benchmark — Node ${process.version}\n`);
  for (const n of [32, 1024, 16_384, 65_536]) {
    const options = { opsPerIteration: n, warmup: n > 1024 ? 2 : 200, measureMs: n > 1024 ? 2000 : 1000 };
    await bench(`shift()  — ${n} chunks queued, read back`, () => withShift(n), options);
    await bench(`cursor   — ${n} chunks queued, read back`, () => withCursor(n), options);
  }
  for (const n of [32, 16_384, 262_144]) {
    const options = { opsPerIteration: n, warmup: n > 1024 ? 2 : 200, measureMs: n > 1024 ? 2000 : 1000 };
    await bench(`WrpcReadable — ${n} chunks pushed, then read`, () => readBack(n), options);
  }
  if (sink < 0) console.log(sink);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
