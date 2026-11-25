'use strict';

// What the broker adapters share, word for word: the header names a
// delivery carries, the two shapes of a `log.read` that is refused or
// guarded, the checks every `consume`/`listen` opens with, the id factory,
// and what happens to a delivery whose handler threw. Each of these was
// four copies (Redis, NATS, AMQP, Kafka) that had to be edited together.
//
// Deliberately NOT here: the delivery object itself. How a message is
// settled is where the brokers really differ — Redis releases its slot in a
// `finally`, NATS stops its keepalive before the work, Kafka awaits the
// handler to keep a partition in order, AMQP settles on the channel the
// delivery arrived on — and a shared `createDelivery` would be a table of
// exceptions.
//
// Node-only, like everything under src/broker/. Not in src/wire.js: that one
// is bundled for the browser, on a byte budget.

const { generateUUID } = require('../runtime/node.js');
const { resolveGenerateId } = require('../utils.js');
const { crashDelay, positiveInteger } = require('./retry.js');
const { lineLevels } = require('../rpc/errors.js');
const { ArgumentError } = require('../errors.js');

// Carried by a delivery an adapter re-published: the attempt it is on (the
// counter is the ADAPTER's — RabbitMQ 4 does not count a requeue, Kafka has
// no counter at all), whether it is a copy the adapter put back, and — on a
// dead-letter queue — why it was given up on.
const ATTEMPT_HEADER = 'x-wrpc-attempt';
const REDELIVERED_HEADER = 'x-wrpc-redelivered';
const DEAD_REASON_HEADER = 'x-wrpc-dead-reason';

// The attempt a delivery is on, read back from the header an adapter wrote
// when it re-published the message: a positive integer, or `fallback` for
// anything else. It used to be `Number(header ?? '1') || 1`, which believes
// whatever is there — and what is there on a first delivery is the
// PRODUCER's: `x-wrpc-attempt: -1000000` gave a counter that never reached
// the retry limit and a backoff of zero (a negative exponent), 999,994
// redeliveries in a second and a half; `99` dead-lettered a message on its
// first failure.
const attemptOf = (headers, fallback = 1) => {
  const attempt = Number(headers[ATTEMPT_HEADER]);
  return Number.isInteger(attempt) && attempt >= 1 ? attempt : fallback;
};

// A producer's headers without the three an adapter keeps for itself: what a
// public `produce` sends. The retry paths write them through their own
// sends, never through `produce`.
const producerHeaders = (headers) => {
  if (!headers || typeof headers !== 'object') return headers;
  if (!(ATTEMPT_HEADER in headers) && !(REDELIVERED_HEADER in headers) && !(DEAD_REASON_HEADER in headers)) {
    return headers;
  }
  const own = {};
  for (const name of Object.keys(headers)) {
    if (name === ATTEMPT_HEADER || name === REDELIVERED_HEADER || name === DEAD_REASON_HEADER) continue;
    own[name] = headers[name];
  }
  return own;
};

// The level of a refusal's line, by its reason: a warning the first time,
// debug for ten seconds after it (lineLevels in ../rpc/errors.js). A refusal is something a peer does —
// to an inbox anybody on the broker can write to — and each used to be a
// warning of its own, one per frame. The reasons are this package's (a
// closed set), so the table holds a handful of entries.
const refusalLevels = () => lineLevels();

// A direct message's body was a string ('1') or bytes ('0'), where the
// broker itself carries bytes only.
const TEXT_HEADER = 'wrpc-text';

const DEFAULT_PREFETCH = 16;

// A settlement that has to PUBLISH — a retry's copy, a dead letter — and was
// refused is tried again on this schedule by the adapters that settle that
// way (AMQP, Kafka), before the message is handed back to the broker as it
// was: at least once, attempt untouched.
const SETTLE_ATTEMPTS = 3;
const SETTLE_BACKOFF = Object.freeze({ minDelay: 100, maxDelay: 1000, factor: 2, jitter: false });

// A pause that does not keep the process alive.
const sleep = (ms) =>
  new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    if (typeof timer.unref === 'function') timer.unref();
  });

// An adapter's error line: the event it is, the error, what locates it.
const reporter =
  (log) =>
  (event, error, extra = {}) =>
    log.error({ err: error, event, ...extra });

/**
 * The id factory of an adapter. An injected `generateId` is used VERBATIM
 * for every id the adapter mints — never truncated. Trimming a user's id
 * would quietly weaken the uniqueness they chose it for, and all wrpc knows
 * about their generator is that it answers a string. The cost is that a
 * generator answering characters a broker refuses in a consumer name,
 * subject or queue name fails at the driver, not here.
 *
 * Strict: a new option, so a bad generator is refused at construction
 * rather than producing a name the broker rejects at connect time.
 * `shortName` is for the names a broker repeats in every log line and
 * metric label it emits: the DEFAULT stays short, an injected generator is
 * used whole.
 */
const idFactory = (generateId, label) => {
  if (generateId === null) return { nextId: generateUUID, shortName: () => generateUUID().slice(0, 8) };
  const nextId = resolveGenerateId(generateId, label).generate;
  return { nextId, shortName: nextId };
};

/**
 * A `log.read` refused before it started (a malformed id): `ready` rejects
 * — handled from birth, a read nobody awaits is not an unhandled rejection —
 * and so does every `next()`.
 */
const failedRead = (error) => {
  const rejected = Promise.reject(error);
  // Handled from birth (see above).
  rejected.catch(() => {});
  return {
    ready: rejected,
    [Symbol.asyncIterator]: () => ({
      next: () => Promise.reject(error),
      return: () => Promise.resolve({ value: undefined, done: true }),
    }),
  };
};

/**
 * A read behind a `guard` — the promise of a position check (is the id past
 * the tip? below the retention?) started alongside it — and, with `mapId`,
 * handing out its ids in the adapter's public spelling (a string for a
 * sequence, the encoded vector for Kafka).
 *
 * `ready` waits for both. The first `next()` awaits the guard, and a guard
 * that refuses RETURNS the inner iterator before it throws: a refused
 * position must not leave this reader on the shared tail, and an iterator
 * whose next() rejects is never closed by a for-await loop.
 */
const guardedRead = (inner, { guard = null, mapId = null } = {}) => {
  const ready = guard === null ? inner.ready : Promise.all([guard, inner.ready]).then(() => undefined);
  ready.catch(() => {}); // surfaced through the iteration; never unhandled
  return {
    ready,
    [Symbol.asyncIterator]: () => {
      const iterator = inner[Symbol.asyncIterator]();
      let verified = guard === null;
      return {
        next: async () => {
          if (!verified) {
            try {
              await guard;
            } catch (error) {
              await iterator.return?.();
              throw error;
            }
            verified = true;
          }
          const result = await iterator.next();
          if (mapId === null || result.done) return result;
          return { done: false, value: { ...result.value, id: mapId(result.value.id) } };
        },
        return: (value) => iterator.return?.(value) ?? Promise.resolve({ value, done: true }),
      };
    },
  };
};

/** What every `queue.consume` checks first; `label` is `'<broker> queue.consume'`. */
const checkConsume = (label, onDelivery, prefetch, deadLetter) => {
  if (typeof onDelivery !== 'function') throw new ArgumentError(`${label}: onDelivery must be a function`);
  if (!positiveInteger(prefetch)) throw new ArgumentError(`${label}: prefetch must be a positive integer`);
  if (deadLetter !== null && (typeof deadLetter !== 'string' || deadLetter.length === 0)) {
    throw new ArgumentError(`${label}: deadLetter must be a queue name or null`);
  }
};

/** What every `direct.listen` checks first; `label` is `'<broker> direct.listen'`. */
const checkListen = (label, address, onMessage) => {
  if (typeof onMessage !== 'function') throw new ArgumentError(`${label}: onMessage must be a function`);
  if (typeof address !== 'string' || address.length === 0) {
    throw new ArgumentError(`${label}: address must be a non-empty string`);
  }
};

/**
 * Hands a delivery to its handler. One that throws or rejects settled
 * nothing: reported (`report(event, error, { queue })`), and retried after a
 * backoff, attempt + 1 — the delivery contract (port.js), not a release to
 * the head, which a handler that always throws would spin hot.
 */
const runDelivery = (onDelivery, delivery, report, event, queue) => {
  Promise.resolve()
    .then(() => onDelivery(delivery))
    .catch((error) => {
      report(event, error, { queue });
      void delivery.retry({ delay: crashDelay(delivery.attempt) });
    });
};

module.exports = {
  ATTEMPT_HEADER,
  REDELIVERED_HEADER,
  DEAD_REASON_HEADER,
  attemptOf,
  producerHeaders,
  refusalLevels,
  SETTLE_ATTEMPTS,
  SETTLE_BACKOFF,
  sleep,
  reporter,
  TEXT_HEADER,
  DEFAULT_PREFETCH,
  idFactory,
  failedRead,
  guardedRead,
  checkConsume,
  checkListen,
  runDelivery,
};
