'use strict';

// What a consumed message's outcome becomes: ack, retry (with a delay),
// release, or the dead-letter queue. One table for every broker, so a
// procedure's failure means the same thing on Redis and on Kafka.

const { backoffDelay } = require('../utils.js');

// Transient by default — 500 included: a queue's 500 is usually a
// dependency that fell over (a database blip), and at-least-once already
// demands an idempotent handler. 408 is retried for the same reason.
const DEFAULT_RETRY_ON = Object.freeze([408, 429, 500, 503]);

const DEFAULT_RETRY = Object.freeze({
  attempts: 5,
  backoff: Object.freeze({ base: 1000, max: 60_000, factor: 2, jitter: true }),
  retryOn: DEFAULT_RETRY_ON,
});

const positiveInteger = (value) => Number.isInteger(value) && value > 0;
const nonNegative = (value) => typeof value === 'number' && Number.isFinite(value) && value >= 0;

const normalizeRetry = (options, label) => {
  if (options === undefined || options === null) return DEFAULT_RETRY;
  if (options === false) return Object.freeze({ ...DEFAULT_RETRY, attempts: 1 });
  if (typeof options !== 'object') throw new TypeError(`${label}: retry must be an object or false`);
  const { attempts = DEFAULT_RETRY.attempts, backoff = {}, retryOn = DEFAULT_RETRY_ON } = options;
  if (!positiveInteger(attempts)) throw new TypeError(`${label}: retry.attempts must be a positive integer`);
  if (typeof backoff !== 'object' || backoff === null) {
    throw new TypeError(`${label}: retry.backoff must be an object`);
  }
  const merged = { ...DEFAULT_RETRY.backoff, ...backoff };
  if (!nonNegative(merged.base) || !nonNegative(merged.max) || !(merged.factor > 0)) {
    throw new TypeError(`${label}: retry.backoff needs non-negative base/max and a positive factor`);
  }
  if (!Array.isArray(retryOn) || !retryOn.every((code) => Number.isInteger(code))) {
    throw new TypeError(`${label}: retry.retryOn must be an array of status codes`);
  }
  return Object.freeze({ attempts, backoff: Object.freeze(merged), retryOn: Object.freeze([...retryOn]) });
};

/**
 * `code` is null for a success. `attempt` is 1-based: attempt N failing
 * with a retryable code is retried while N < attempts.
 */
const decide = ({ code, attempt, retry = DEFAULT_RETRY, draining = false, random = Math.random }) => {
  if (code === null || code === undefined) return { action: 'ack', delay: 0 };
  // A node shutting down refuses new work with 503: that is not the
  // message's fault, and burning one of its attempts on it would be.
  if (draining && code === 503) return { action: 'release', delay: 0 };
  if (!retry.retryOn.includes(code)) return { action: 'dead', delay: 0 };
  if (attempt >= retry.attempts) return { action: 'dead', delay: 0 };
  const { base, max, factor, jitter } = retry.backoff;
  const delay = backoffDelay({ attempt: attempt - 1, minDelay: base, maxDelay: max, factor, jitter, random });
  return { action: 'retry', delay };
};

module.exports = { DEFAULT_RETRY, DEFAULT_RETRY_ON, normalizeRetry, decide };
