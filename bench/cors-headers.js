'use strict';

// The Allow-Headers memo in src/transport.js (allowedHeaders): the string
// derives only from the immutable `cors` option, yet buildHeaders runs on
// every HTTP call — so before the WeakMap memo, six camelCase metaHeaders
// paid two global-regex toKebab passes per key per request. This file is
// the measurement that comment cites.
//
// The three shapes measured: no cors at all (the constant), a cors object
// with six camelCase metaHeaders rebuilt per call (the pre-memo cost,
// reproduced inline), and the same object through the memoized
// allowedHeaders via buildHeaders.

const { bench } = require('./support/harness.js');
const { buildHeaders } = require('../src/transport.js');
const { toKebab } = require('../src/utils.js');

const CORS = {
  origins: ['https://app.example'],
  credentials: true,
  metaHeaders: ['userId', 'tenantId', 'idempotencyKey', 'xAppVersion', 'requestId', 'deviceId'],
};

const DEFAULT_HEADERS = 'Content-Type, x-wrpc-channel, last-event-id, x-wrpc-meta';

// The pre-memo shape: what allowedHeaders did per request before the cache.
const rebuildEveryCall = (cors) => {
  const declared = cors.headers ?? DEFAULT_HEADERS;
  let allow = declared;
  for (let i = 0; i < cors.metaHeaders.length; i++) allow += `, x-wrpc-meta-${toKebab(cors.metaHeaders[i])}`;
  return allow;
};

// The harness awaits per call, which would drown a nanosecond op in promise
// overhead — each iteration runs a sync batch.
const BATCH = 1000;

const run = async () => {
  await bench(
    'buildHeaders, no cors option',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) out = buildHeaders(undefined, 'https://app.example');
      return out;
    },
    { opsPerIteration: BATCH },
  );

  await bench(
    'Allow-Headers rebuilt per call (pre-memo shape)',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) out = rebuildEveryCall(CORS);
      return out;
    },
    { opsPerIteration: BATCH },
  );

  await bench(
    'buildHeaders, six camelCase metaHeaders (memoized)',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) out = buildHeaders(CORS, 'https://app.example');
      return out;
    },
    { opsPerIteration: BATCH },
  );
};

run();
