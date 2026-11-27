/**
 * The token an adapter makes of a name — a channel, a topic, a queue, an
 * address — on every message it publishes or appends (`encodeToken` in
 * src/broker/ids.js: a walk over the name's characters, the token built by
 * concatenation, every byte outside the broker's alphabet escaped).
 *
 * The names are few and repeat: an adapter is asked about the same topic for
 * every event of it. `tokenEncoder` remembers them, in a memo that holds
 * 1,024 names and starts over when it is full. The first rows are the
 * encoding against the memo, for a name that needs no escape, two that do,
 * and one outside ASCII. One run printed:
 *
 *   name                                  encoded     remembered
 *   orders                             4,900,000     14,500,000 ops/s
 *   room:general                       1,700,000     14,500,000
 *   wrpc:rooms:tenant-42:chat/general    590,000     14,500,000
 *   користувач:42                        320,000     14,500,000
 *
 * The last rows are what that is of a whole operation: an append to a Redis
 * log through the adapter, over the in-process server the suites use (so the
 * rest of the number is the adapter's own work, not a network's). Before the
 * memo they printed 1,480,000 and 446,000 appends a second; with it,
 * 1,930,000 and 1,630,000.
 *
 * Run with `node bench/broker-token.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { encodeToken, tokenEncoder } = require('../src/broker/ids.js');
const { createRedisBroker } = require('../src/broker/redis/index.js');
const { createFakeRedis } = require('../tests/broker/fake-redis-server.js');

const NAMES = ['orders', 'room:general', 'wrpc:rooms:tenant-42:chat/general', 'користувач:42'];
const OPTIONS = { maxLength: 200 };
const quiet = { log() {}, info() {}, warn() {}, error() {}, debug() {} };

let sink = 0;

const main = async () => {
  console.log(`Broker token benchmark — Node ${process.version}\n`);
  const remembered = tokenEncoder(OPTIONS);
  for (const name of NAMES) {
    await bench(`encodeToken — ${name}`, () => {
      sink += encodeToken(name, OPTIONS).length;
    });
    await bench(`tokenEncoder — ${name}`, () => {
      sink += remembered(name).length;
    });
  }
  const broker = createRedisBroker({ client: createFakeRedis(), logger: quiet, blockMs: 20 });
  const body = JSON.stringify({ id: 'o-1', total: 42 });
  for (const topic of ['orders', 'wrpc:rooms:tenant-42:chat/general']) {
    await bench(`redis log.append — ${topic}`, () => broker.log.append(topic, { body, headers: {} }));
  }
  await broker.close();
  if (sink < 0) console.log(sink);
};

main().then(
  () => process.exit(0),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
