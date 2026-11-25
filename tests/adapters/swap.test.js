'use strict';

const { describe } = require('node:test');

const { buildBoots } = require('./boots.js');
const { runAdapterSpec } = require('./spec.js');

// The swap test: one specification (tests/adapters/spec.js) replayed against
// every supported way of standing wrpc up. A boot whose optional peer package
// is missing reports as skipped rather than failing the run.
describe('adapter swap', () => {
  for (const entry of buildBoots()) {
    describe(entry.name, { skip: entry.skip ?? false }, () => {
      runAdapterSpec(entry);
    });
  }
});
