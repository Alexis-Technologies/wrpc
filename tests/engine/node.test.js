'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { createNodeEngine, isEngine } = require('../../src/engine/index.js');
const { runEngineContract } = require('./engineContract.js');

describe('isEngine', () => {
  it('should detect an engine structurally', () => {
    assert.strictEqual(isEngine(createNodeEngine()), true);
    assert.strictEqual(isEngine(null), false);
    assert.strictEqual(isEngine({}), false);
    assert.strictEqual(isEngine({ name: 'x', attach() {} }), false);
  });
});

describe('node engine', () => {
  describe('the WrpcSocket engine contract', () => {
    runEngineContract(() => createNodeEngine());
  });
});
