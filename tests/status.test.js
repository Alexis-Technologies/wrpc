'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const http = require('node:http');

const { STATUS_CODES } = require('../src/status.js');
const { publicErrorMessage } = require('../src/transport.js');

// The table exists so the dispatcher can bundle without node:http; the
// price is that it can drift from Node's own. This pins the two together.
test('status: the table mirrors node:http exactly', () => {
  assert.deepStrictEqual({ ...STATUS_CODES }, { ...http.STATUS_CODES });
  assert.strictEqual(Object.isFrozen(STATUS_CODES), true);
});

test('status: every code the core answers with has a reason phrase', () => {
  for (const code of [400, 401, 403, 404, 408, 409, 413, 429, 500, 501, 503]) {
    assert.strictEqual(typeof STATUS_CODES[code], 'string', `missing ${code}`);
  }
  // 499 is wrpc's own (client closed request) and has no phrase in either
  // table — the error path falls back to the generic message.
  assert.strictEqual(STATUS_CODES[499], undefined);
  assert.strictEqual(publicErrorMessage(499), 'Unknown error');
  assert.strictEqual(publicErrorMessage(503), 'Service Unavailable');
});
