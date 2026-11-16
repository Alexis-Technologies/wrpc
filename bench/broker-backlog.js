/**
 * The ready messages of a `MemoryBroker` queue — what was produced and no
 * consumer has taken yet (src/broker/memory.js). They leave from the head, a
 * released delivery and the unsettled work of a stopped consumer go back to
 * the head, and how many wait is up to the application's producers: a
 * consumer that starts late, or was stopped for a deploy, finds a backlog.
 *
 * They used to be an array, taken with `shift()` and put back with
 * `unshift()`. V8 trims a short array from the left in place, but past a few
 * thousand elements either one moves the rest: draining a backlog of N cost
 * N² / 2 moves — all of them on microtasks, with no turn of the event loop
 * in between, under a handler that settles at once.
 *
 * The first rows are that array, filled to N and taken from the head, in
 * messages per second: it falls off a cliff. The rest are the broker itself
 * over a ring (`Deque` in src/broker/memory.js) — a backlog produced with
 * nobody listening, then consumed by one consumer that acks each delivery,
 * and the same with every delivery released once first, which is the write
 * at the head. Over the array those rows read, in one run on the machine
 * that now prints 2.1 million for the largest of them (past the cliff a run
 * differs from the next by half, never by the order of magnitude):
 *
 *   backlog                  1,024     20,000     80,000    160,000
 *   consumed and acked   2,690,275    137,760     62,788     10,621
 *   released once, acked 1,362,817     42,767     13,072          —
 *
 * 160,000 messages took 15 seconds to produce and consume; 80,000 that were
 * each released once took 6.
 *
 * Run with `node bench/broker-backlog.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { MemoryBroker } = require('../src/broker/memory.js');

const QUEUE = 'jobs';
const MESSAGE = '{"n":1}';
const PREFETCH = 64;

const quiet = { log() {}, info() {}, warn() {}, error() {}, debug() {} };

let sink = 0;

const withShift = (n) => {
  const ready = [];
  for (let i = 0; i < n; i++) ready.push(MESSAGE);
  while (ready.length > 0) sink += ready.shift().length;
};

// A backlog of `n`, then the one consumer that drains it. With `release`,
// every message is handed back once before it is acked: it returns to the
// head of the queue and is the next one delivered.
const drain = async (n, release) => {
  const broker = new MemoryBroker({ logger: quiet });
  for (let i = 0; i < n; i++) await broker.queue.produce(QUEUE, MESSAGE);
  let left = n;
  await new Promise((resolve) => {
    const onDelivery = (delivery) => {
      if (release && !delivery.redelivered) return delivery.release();
      sink += delivery.body.length;
      if (--left === 0) resolve();
      return delivery.ack();
    };
    void broker.queue.consume(QUEUE, onDelivery, { prefetch: PREFETCH });
  });
  broker.close();
};

const optionsFor = (n) => ({ opsPerIteration: n, warmup: n > 1024 ? 2 : 200, measureMs: n > 1024 ? 2000 : 1000 });

const main = async () => {
  console.log(`Broker backlog benchmark — Node ${process.version}\n`);
  for (const n of [1024, 16_384, 65_536]) {
    await bench(`array shift() — ${n} ready, taken from the head`, () => withShift(n), optionsFor(n));
  }
  for (const n of [1024, 20_000, 80_000, 160_000]) {
    await bench(`MemoryBroker — a backlog of ${n}, consumed and acked`, () => drain(n, false), optionsFor(n));
  }
  for (const n of [1024, 20_000, 80_000]) {
    await bench(`MemoryBroker — a backlog of ${n}, released once, acked`, () => drain(n, true), optionsFor(n));
  }
  if (sink < 0) console.log(sink);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
