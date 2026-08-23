'use strict';

// The compiled-serializer fast path in handleRpc (src/rpc/dispatcher.js):
// when a procedure's schema.response compiled a serializer, the callback
// envelope is assembled by string concatenation around the compiled result
// text instead of JSON.stringify walking the whole packet. This file is the
// measurement that comment cites.
//
// The "compiled serializer" here is a hand-written shape-specific function —
// exactly what fast-json-stringify generates for a known schema — so the
// numbers measure the SEAM (envelope surgery vs whole-packet stringify),
// not any particular serializer library.

const { bench } = require('./support/harness.js');

const result = {
  id: 'p_812',
  orgId: 'org_42',
  name: 'A project with a reasonably long name',
  archived: false,
  members: 12,
  tags: ['alpha', 'beta', 'gamma'],
};
const id = 'b1f0c9d2-4c1a-4a56-9d7e-2f3a4b5c6d7e';

// What fjs emits for the response schema: direct property access, no walk.
const compiledSerialize = (value) =>
  `{"id":${JSON.stringify(value.id)},"orgId":${JSON.stringify(value.orgId)},"name":${JSON.stringify(value.name)},` +
  `"archived":${value.archived},"members":${value.members},"tags":${JSON.stringify(value.tags)}}`;

// The harness awaits per call, which would drown a sub-microsecond op in
// promise overhead — so each iteration runs a sync batch.
const BATCH = 1000;

const run = async () => {
  await bench(
    'JSON.stringify(whole packet)',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) out = JSON.stringify({ type: 'callback', id, result });
      return out;
    },
    { opsPerIteration: BATCH },
  );

  await bench(
    'envelope surgery + compiled result',
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) {
        out = `{"type":"callback","id":${JSON.stringify(id)},"result":${compiledSerialize(result)}}`;
      }
      return out;
    },
    { opsPerIteration: BATCH },
  );
};

// Measured on Node 24 (2026-08): ~1.16x for the seam itself with this
// hand-written serializer; a real fast-json-stringify compilation widens
// the gap further on string-heavy payloads (its escape-scan beats
// JSON.stringify's). The seam is the point: without it a compiled
// serializer could only feed a SECOND stringify of the envelope.
run();
