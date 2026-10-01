'use strict';

/**
 * Durable feed fan-out benchmark: one entry appended to a topic that N
 * local subscriptions of `brokerFeed` read.
 *
 * `TopicTails` already makes that one broker read per topic. What is left
 * per subscriber is the feed's own pump — and part of it does not depend on
 * the subscriber at all: opening a sealed entry (base64, AEAD, the header
 * JSON) and signing the resume token (one HMAC) answer the same bytes for
 * every reader of the entry. This measures the pump per reader-entry, with
 * and without `secret` and `encryption`, at two payload sizes, over a log
 * that stands on `TopicTails` the way the four adapters do (a MemoryBroker
 * read hands every reader a copy of its own, so nothing is shared there).
 *
 * When the two memos in src/broker/feed.js were written (one machine; the
 * ratios are what carries over), a reader-entry cost, at 1000 subscribers:
 *
 *                          0.2 KB               2 KB
 *   plain                  0.74 -> 0.65 µs      0.72 -> 0.65 µs   (unchanged)
 *   secret                 2.34 -> 0.84 µs      2.34 -> 0.83 µs   one HMAC per entry
 *   encryption             4.67 -> 0.71 µs      5.71 -> 0.74 µs   one open per entry
 *   secret + encryption    6.69 -> 0.91 µs      7.94 -> 0.88 µs
 *
 * The values are read as text here: `decode: 'json'` is a JSON.parse per
 * subscriber on top (each gets an object of its own, deliberately — one
 * shared object would cross every subscriber's `map`, output validator and
 * hooks), which the last rows show.
 *
 * Run: node bench/feed-fanout.js (or as part of `pnpm bench`)
 */

const { performance } = require('node:perf_hooks');

const { brokerFeed } = require('../src/broker/feed.js');
const { TopicTails } = require('../src/broker/tail.js');
const { createBrokerSealing } = require('../src/broker/sealing.js');
const { generateKey } = require('../src/encryption/index.js');

// A log on TopicTails with nothing behind it: live() hands every appended
// entry to the tail, which hands the SAME entry to every reader — what a
// Redis, NATS, AMQP or Kafka adapter's read is, minus the broker.
const createLog = () => {
  let onEntry = null;
  let last = 0;
  const tails = new TopicTails({
    live: async (_topic, { signal, onEntry: push }) => {
      onEntry = push;
      signal.addEventListener('abort', () => {
        onEntry = null;
      });
      return last === 0 ? null : String(last);
    },
    range: async () => [],
    covered: (cursor, entry) => Number(entry.id) <= Number(cursor),
    advance: (_cursor, entry) => entry.id,
  });
  return {
    name: 'bench',
    append: async (_topic, value, { headers = {} } = {}) => {
      const entry = { id: String(++last), value, headers };
      onEntry?.(entry);
      return entry.id;
    },
    read: (topic, options) => tails.read(topic, options),
    parseId: (text) => (/^\d{1,16}$/.test(text) ? text : null),
  };
};

const payloadOf = (size) => {
  const rows = [];
  let n = 0;
  while (JSON.stringify({ rows }).length < size) {
    rows.push({ id: n, name: `row-${n}`, at: '2026-10-01T10:00:00.000Z', ok: n % 2 === 0 });
    n++;
  }
  return JSON.stringify({ rows });
};

const TOPIC = 'orders.v1.created';

const fanout = async (subscribers, { secret, encryption, decode = 'text' }, label, size) => {
  const log = createLog();
  const feed = brokerFeed(log, TOPIC, { secret, encryption, decode });
  const sealing = createBrokerSealing(encryption, 'bench', { layer: 'broker-log', replay: false, text: true });
  const text = payloadOf(size);
  const append =
    sealing === null
      ? () => log.append(TOPIC, text)
      : () => {
          const sealed = sealing.seal(TOPIC, { 'content-type': 'application/json' }, text);
          return log.append(TOPIC, sealed.body, { headers: sealed.headers });
        };
  const controller = new AbortController();
  const iterators = new Array(subscribers);
  const pending = new Array(subscribers);
  for (let i = 0; i < subscribers; i++) {
    iterators[i] = feed({}, {}, { signal: controller.signal });
    pending[i] = iterators[i].next();
  }
  // Every reader is on the tail before the first append.
  await new Promise((resolve) => setTimeout(resolve, 20));
  const round = async () => {
    await append();
    await Promise.all(pending);
    for (let i = 0; i < subscribers; i++) pending[i] = iterators[i].next();
  };
  for (let i = 0; i < 30; i++) await round();
  let rounds = 0;
  const started = performance.now();
  while (performance.now() - started < 600) {
    await round();
    rounds++;
  }
  const elapsed = performance.now() - started;
  controller.abort();
  await Promise.all(pending);
  const mode = `${secret ? 'secret' : ''}${secret && encryption ? ' + ' : ''}${encryption ? 'encryption' : ''}`;
  console.log(
    `  ${`x${subscribers} ${mode || 'plain'}${decode === 'json' ? ' (json)' : ''}, ${label}`.padEnd(40)}` +
      `${((elapsed * 1000) / (rounds * subscribers)).toFixed(2).padStart(8)} µs/reader-entry`,
  );
};

const main = async () => {
  console.log(`Durable feed fan-out benchmark — Node ${process.version}\n`);
  const keys = generateKey();
  for (const subscribers of [100, 1000]) {
    for (const options of [
      { secret: null, encryption: null },
      { secret: 'bench-secret', encryption: null },
      { secret: null, encryption: { keys } },
      { secret: 'bench-secret', encryption: { keys } },
      { secret: null, encryption: null, decode: 'json' },
    ]) {
      for (const [label, size] of [
        ['0.2 KB', 200],
        ['2 KB', 2048],
      ]) {
        await fanout(subscribers, options, label, size);
      }
    }
    console.log();
  }
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
