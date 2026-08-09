'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');

const wrpc = require('../index.js');

test('package entry point loads and exports an object', () => {
  assert.equal(typeof wrpc, 'object');
  assert.notEqual(wrpc, null);
});
