/**
 * `SegmentQueue` — the WebSocket engine's receive buffer: the TCP segments of
 * a connection, read from the head as frames complete (src/websocket/segments.js).
 *
 * How many segments a frame arrives in is the PEER's choice: it may write its
 * payload a byte at a time. The queue used to drop consumed segments with
 * `shift()`, which past a few thousand elements copies the rest of the array
 * — consuming a frame of N segments cost N² / 2 moves in ONE synchronous
 * call. A 300,000-byte frame sent in one-byte writes stalled a server for
 * 12.8 seconds, before any authentication.
 *
 * The rows are one frame of N one-byte segments, pushed and consumed, in
 * segments per second: flat with the head cursor, where 256,000 took 5.9
 * seconds. The last rows are the ordinary shapes — a frame in one segment,
 * and a 1 MiB message in 64 KiB reads — which the cursor must not slow down.
 *
 * Run with `node bench/segment-queue.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { SegmentQueue } = require('../src/websocket/segments.js');

const BYTE = Buffer.alloc(1);
const SMALL = Buffer.alloc(125);
const READ = Buffer.alloc(64 * 1024);

let sink = 0;

const dribbled = (segments) => {
  const queue = new SegmentQueue();
  for (let i = 0; i < segments; i++) queue.push(BYTE);
  sink += queue.consume(segments).length;
};

const main = async () => {
  console.log(`Segment queue benchmark — Node ${process.version}\n`);
  for (const n of [1024, 16_384, 262_144]) {
    const options = { opsPerIteration: n, warmup: n > 1024 ? 2 : 200, measureMs: n > 1024 ? 2000 : 1000 };
    await bench(`a frame in ${n} one-byte segments, consumed`, () => dribbled(n), options);
  }
  {
    const queue = new SegmentQueue();
    await bench('a 125-byte frame in one segment (zero-copy)', () => {
      queue.push(SMALL);
      sink += queue.consume(125).length;
    });
  }
  {
    const queue = new SegmentQueue();
    await bench('a 1 MiB message in sixteen 64 KiB reads', () => {
      for (let i = 0; i < 16; i++) queue.push(READ);
      sink += queue.consume(16 * READ.length).length;
    });
  }
  if (sink < 0) console.log(sink);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
