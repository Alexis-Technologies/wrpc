'use strict';

// The one `waitFor` of the test suites. It was seventeen copies with three
// signatures and three ways of giving up — a count of polls (which stretches
// under load and still fails a slow machine), a deadline, a deadline that
// throws a plain Error. Not a *.test.js: node --test must not run it.
//
// Require-free of the library on purpose: a suite that boots nothing (a
// unit test, a contract helper) waits the same way as one that does.

const assert = require('node:assert');
const timers = require('node:timers/promises');

// A slow machine raises every wait at once; a wait that passes never
// notices, since the deadline only bounds a FAILURE.
const fromEnv = Number(process.env.WRPC_TEST_TIMEOUT);
const DEFAULT_TIMEOUT = Number.isFinite(fromEnv) && fromEnv > 0 ? fromEnv : 5000;
const DEFAULT_INTERVAL = 5;

/**
 * Polls `predicate` (it may be async) until it answers truthy; fails the
 * test once `timeout` ms have passed. The second argument is the failure
 * message, or `{ message, timeout, interval }`.
 *
 * Prefer this to a fixed sleep before a POSITIVE assertion: a sleep is both
 * slower than the event it waits for and, on a loaded machine, shorter.
 * A sleep is still what a "nothing happens" check needs — there is no event
 * to wait for — and those say how long they watch.
 */
const waitFor = async (predicate, options) => {
  const {
    timeout = DEFAULT_TIMEOUT,
    message = 'condition never held',
    interval = DEFAULT_INTERVAL,
  } = typeof options === 'string' ? { message: options } : (options ?? {});
  const deadline = Date.now() + timeout;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() >= deadline) break;
    await timers.setTimeout(interval);
  }
  assert.fail(`${message} (waited ${timeout} ms)`);
};

/** A `waitFor` with another default timeout — what a suite with slower peers (a real broker) exports. */
const waitForWithin = (defaultTimeout) => (predicate, options) =>
  waitFor(
    predicate,
    typeof options === 'string'
      ? { message: options, timeout: defaultTimeout }
      : { timeout: defaultTimeout, ...options },
  );

module.exports = { waitFor, waitForWithin, DEFAULT_TIMEOUT };
