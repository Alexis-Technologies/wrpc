'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { STATUS_CODES } = require('../src/status.js');
const { publicErrorMessage } = require('../src/transport.js');

describe('STATUS_CODES', () => {
  // The table exists so the dispatcher can bundle without node:http; the
  // price is that it can drift from Node's own. This pins the two together.
  it('should mirror the node:http table exactly', () => {
    assert.deepStrictEqual({ ...STATUS_CODES }, { ...http.STATUS_CODES });
    assert.strictEqual(Object.isFrozen(STATUS_CODES), true);
  });

  it('should have a reason phrase for every code the core answers with', () => {
    for (const code of [400, 401, 403, 404, 408, 409, 413, 429, 500, 501, 503]) {
      assert.strictEqual(typeof STATUS_CODES[code], 'string', `missing ${code}`);
    }
    // 499 is wrpc's own (client closed request) and has no phrase in either
    // table — the error path falls back to the generic message.
    assert.strictEqual(STATUS_CODES[499], undefined);
    assert.strictEqual(publicErrorMessage(499), 'Unknown error');
    assert.strictEqual(publicErrorMessage(503), 'Service Unavailable');
  });
});
