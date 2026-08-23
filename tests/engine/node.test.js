'use strict';

const test = require('node:test');

const { createNodeEngine, isEngine } = require('../../src/engine/index.js');
const { runEngineContract } = require('./engineContract.js');
const assert = require('node:assert');

test('isEngine: structural engine detection', () => {
  assert.strictEqual(isEngine(createNodeEngine()), true);
  assert.strictEqual(isEngine(null), false);
  assert.strictEqual(isEngine({}), false);
  assert.strictEqual(isEngine({ name: 'x', attach() {} }), false);
});

test('node engine satisfies the WrpcSocket engine contract', async (t) => {
  await runEngineContract(() => createNodeEngine(), t);
});
