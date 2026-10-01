'use strict';

// Shared helpers for the broker contract suites. Not a *.test.js: node
// --test must not run it.

const assert = require('node:assert');
const timers = require('node:timers/promises');

const { waitForWithin } = require('../helpers/wait.js');

const quiet = { log() {}, info() {}, warn() {}, error() {}, debug() {} };

/** The shared waitFor (tests/helpers/wait.js), failing after 2 s unless told otherwise. */
const waitFor = waitForWithin(2000);

let counter = 0;
/** A name no other test (or earlier run against a real broker) used. */
const unique = (prefix) => `${prefix}-${process.pid}-${Date.now().toString(36)}-${++counter}`;

/** Collects up to `count` entries from an async iterable, or throws its error. */
const collect = async (iterable, count, { timeout = 2000 } = {}) => {
  const out = [];
  const iterator = iterable[Symbol.asyncIterator]();
  const deadline = timers.setTimeout(timeout, 'timeout');
  try {
    while (out.length < count) {
      const next = await Promise.race([iterator.next(), deadline]);
      if (next === 'timeout') assert.fail(`collected ${out.length} of ${count} before timing out`);
      if (next.done) break;
      out.push(next.value);
    }
  } finally {
    void iterator.return?.();
  }
  return out;
};

/** Resolves with the error an async iterable's first next() throws. */
const firstError = async (iterable, { timeout = 2000 } = {}) => {
  const iterator = iterable[Symbol.asyncIterator]();
  try {
    const result = await Promise.race([iterator.next(), timers.setTimeout(timeout, 'timeout')]);
    if (result === 'timeout') assert.fail('the read neither failed nor yielded');
    assert.fail(`expected the read to fail, it yielded ${JSON.stringify(result)}`);
  } catch (error) {
    if (error instanceof assert.AssertionError) throw error;
    return error;
  } finally {
    void iterator.return?.();
  }
};

// A client package an integration suite needs, loaded under the rule the
// suite's skip guard follows: not installed -> skip, so a machine without
// it still runs `pnpm test`. In CI the env var IS set and the package IS
// expected — a skip there is a broken environment passing green — so
// WRPC_INTEGRATION_STRICT=1 turns the missing package into a failure.
const loadClient = (load, missing) => {
  try {
    return load();
  } catch (error) {
    if (process.env.WRPC_INTEGRATION_STRICT) {
      throw new Error(`${missing} (WRPC_INTEGRATION_STRICT is set: a skip would hide a broken environment)`, {
        cause: error,
      });
    }
    return null;
  }
};

module.exports = { quiet, waitFor, unique, collect, firstError, loadClient };
