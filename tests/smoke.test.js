'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');

const wrpc = require('../index.js');

describe('package entry point', () => {
  it('should load and export an object', () => {
    assert.equal(typeof wrpc, 'object');
    assert.notEqual(wrpc, null);
  });
});
