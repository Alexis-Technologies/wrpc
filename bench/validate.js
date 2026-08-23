'use strict';

// The synchronous fast path in runValidator (src/rpc/router.js): a compiled
// ajv validator is a plain synchronous function, and the old async wrapper
// (`await runValidator(...).catch(...)`) taxed it with two promise
// allocations and microtask hops per validated call and per yielded
// subscription value. This file is the measurement that comment cites: the
// validator below reproduces the compiled shape (check, throw-with-issues
// on failure, return the value), and the two harnesses reproduce the old
// and the new call-site shapes.

const { bench } = require('./support/harness.js');

const compiledShape = (args) => {
  const value = args && typeof args === 'object' ? args : {};
  if (typeof value.n !== 'number') {
    const error = new Error('/body must have number n');
    error.details = { issues: [{ message: 'must be number', path: '/body/n' }] };
    throw error;
  }
  return value;
};

// The OLD shape: async wrapper + .catch per invocation.
const runValidatorOld = async (validator, value, context = null) => {
  const result = await validator(value, context);
  return result === undefined ? value : result;
};

// The NEW shape: sync call, thenable check, try/catch at the site.
const runValidatorNew = (validator, value, context = null) => {
  const result = validator(value, context);
  if (result && typeof result.then === 'function') {
    return result.then((settled) => (settled === undefined ? value : settled));
  }
  return result === undefined ? value : result;
};

const args = { n: 7, tag: 'bench' };
const BATCH = 1000;

const run = async () => {
  await bench(
    'old: await runValidator(...).catch(...)',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) {
        out = await runValidatorOld(compiledShape, args).catch(() => null);
      }
      return out;
    },
    { opsPerIteration: BATCH },
  );

  await bench(
    'new: sync fast path + thenable check',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) {
        try {
          const checked = runValidatorNew(compiledShape, args);
          out = checked && typeof checked.then === 'function' ? await checked : checked;
        } catch {
          out = null;
        }
      }
      return out;
    },
    { opsPerIteration: BATCH },
  );
};

run();
