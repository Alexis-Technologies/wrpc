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

// Carried by a delivery an adapter re-published: the attempt it is on (the
// counter is the ADAPTER's — RabbitMQ 4 does not count a requeue, Kafka has
// no counter at all), whether it is a copy the adapter put back, and — on a
// dead-letter queue — why it was given up on.
const ATTEMPT_HEADER = 'x-wrpc-attempt';
const REDELIVERED_HEADER = 'x-wrpc-redelivered';
const DEAD_REASON_HEADER = 'x-wrpc-dead-reason';
// A direct message's body was a string ('1') or bytes ('0'), where the
// broker itself carries bytes only.
const TEXT_HEADER = 'wrpc-text';

const DEFAULT_PREFETCH = 16;

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
  if (typeof onDelivery !== 'function') throw new TypeError(`${label}: onDelivery must be a function`);
  if (!positiveInteger(prefetch)) throw new TypeError(`${label}: prefetch must be a positive integer`);
  if (deadLetter !== null && (typeof deadLetter !== 'string' || deadLetter.length === 0)) {
    throw new TypeError(`${label}: deadLetter must be a queue name or null`);
  }
};

/** What every `direct.listen` checks first; `label` is `'<broker> direct.listen'`. */
const checkListen = (label, address, onMessage) => {
  if (typeof onMessage !== 'function') throw new TypeError(`${label}: onMessage must be a function`);
  if (typeof address !== 'string' || address.length === 0) {
    throw new TypeError(`${label}: address must be a non-empty string`);
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
  TEXT_HEADER,
  DEFAULT_PREFETCH,
  idFactory,
  failedRead,
  guardedRead,
  checkConsume,
  checkListen,
  runDelivery,
};
