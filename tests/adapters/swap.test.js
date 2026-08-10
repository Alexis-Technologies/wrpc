'use strict';

const test = require('node:test');

const { buildBoots } = require('./boots.js');
const { runAdapterSpec } = require('./spec.js');

// The swap test: one specification (tests/adapters/spec.js) replayed against
// every supported way of standing wrpc up. A boot whose optional peer package
// is missing reports as skipped rather than failing the run.
for (const entry of buildBoots()) {
  test(`adapter swap: ${entry.name}`, { skip: entry.skip ?? false }, async (t) => {
    await runAdapterSpec(entry, t);
  });
}
