'use strict';

// Two per-request meta costs and the measurements their comments cite:
//
// 1. sanitizeMeta (src/rpc/dispatcher.js) — the size cap used to run
//    JSON.stringify on EVERY bag just to read .length; the upper-bound walk
//    accepts the common small bag without serializing.
// 2. declaredData's query gate (src/rpc/core.js) — the wrpc_meta
//    URLSearchParams parse is gated on a substring probe, because the
//    common REST/curl request carries no wrpc_meta at all.

const { bench } = require('./support/harness.js');

const bag = { idem: '9f3c-a114', tenant: 'acme', retries: 3 };
const LIMIT = 2048;

const stringifyMeasure = (value, limit) => JSON.stringify(value).length <= limit;

const upperBound = (value) => {
  let bound = 2;
  for (const key in value) {
    bound += key.length * 6 + 6;
    const entry = value[key];
    const type = typeof entry;
    if (type === 'string') bound += entry.length * 6 + 2;
    else if (type === 'number' || type === 'boolean') bound += 24;
    else if (entry === null) bound += 4;
    else return -1;
  }
  return bound;
};

const boundMeasure = (value, limit) => {
  const bound = upperBound(value);
  return !((bound < 0 || bound > limit) && JSON.stringify(value).length > limit);
};

const QUERY = 'page=2&sort=name&filter=active&locale=uk&theme=dark';

const BATCH = 1000;

const run = async () => {
  await bench(
    'meta cap: JSON.stringify every bag (old)',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) out = stringifyMeasure(bag, LIMIT);
      return out;
    },
    { opsPerIteration: BATCH },
  );

  await bench(
    'meta cap: upper-bound walk (new)',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) out = boundMeasure(bag, LIMIT);
      return out;
    },
    { opsPerIteration: BATCH },
  );

  await bench(
    'wrpc_meta lookup: URLSearchParams on every query (old)',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) out = new URLSearchParams(QUERY).get('wrpc_meta');
      return out;
    },
    { opsPerIteration: BATCH },
  );

  await bench(
    'wrpc_meta lookup: substring probe gate (new)',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) {
        out = QUERY.includes('wrpc_meta') ? new URLSearchParams(QUERY).get('wrpc_meta') : null;
      }
      return out;
    },
    { opsPerIteration: BATCH },
  );
};

run();
