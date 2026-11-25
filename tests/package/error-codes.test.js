'use strict';

// The string codes wrpc throws are part of its API: every one the source
// writes is in the `ErrorCode` union of src/errors.d.ts and a row of the
// table in docs/reference/errors.md, and nothing is in either that the source
// does not write. A code that is renamed or removed fails here.

const path = require('node:path');
const { readFileSync, readdirSync } = require('node:fs');
const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { ConfigError, ArgumentError } = require('../../index.js');
const { FramingError } = require('../../src/errors.js');
const { CliError } = require('../../src/cli/types.js');

const ROOT = path.join(__dirname, '..', '..');
const CODE = /ALX_ERR_[A-Z_]+/g;

const read = (file) => readFileSync(path.join(ROOT, file), 'utf8');

const codesIn = (text) => new Set(text.match(CODE));

const sourceCodes = () => {
  const codes = new Set();
  for (const name of readdirSync(path.join(ROOT, 'src'), { recursive: true })) {
    if (!name.endsWith('.js')) continue;
    for (const code of codesIn(read(path.join('src', name)))) codes.add(code);
  }
  return codes;
};

const unionCodes = () => {
  const union = /export type ErrorCode =([^;]+);/.exec(read('src/errors.d.ts'));
  assert.ok(union, 'src/errors.d.ts declares the ErrorCode union');
  return codesIn(union[1]);
};

const tableCodes = () => {
  const text = read('docs/reference/errors.md');
  const start = text.indexOf('## Errors thrown to your code');
  assert.notEqual(start, -1, 'docs/reference/errors.md has the table of thrown codes');
  const section = text.slice(start, text.indexOf('\n## ', start + 1));
  const codes = new Set();
  for (const line of section.split('\n')) {
    const row = /^\| `(ALX_ERR_[A-Z_]+)` \|/.exec(line);
    if (row) codes.add(row[1]);
  }
  return codes;
};

// The TypeErrors that are neither a config nor an argument mistake: a peer's
// malformed frame, a server's answer that breaks the signaling protocol, and
// the circular structure JSON.stringify itself would refuse with a TypeError.
const BARE_TYPE_ERRORS = {
  'src/attachments.js': 2,
  'src/client/core.js': 1,
  'src/webrtc/peer.js': 1,
  'src/webrtc/signaler.js': 3,
};

describe('error codes', () => {
  it('should declare and document every code the source throws', () => {
    const thrown = sourceCodes();
    assert.deepEqual(unionCodes(), thrown, 'the ErrorCode union of src/errors.d.ts');
    assert.deepEqual(tableCodes(), thrown, 'the table in docs/reference/errors.md');
  });

  it('should give each class its code and keep instanceof TypeError', () => {
    const config = new ConfigError('x');
    const argument = new ArgumentError('y', { cause: config });
    assert.equal(config.code, 'ALX_ERR_CONFIG');
    assert.equal(config.name, 'ConfigError');
    assert.ok(config instanceof TypeError);
    assert.equal(argument.code, 'ALX_ERR_ARGUMENT');
    assert.equal(argument.name, 'ArgumentError');
    assert.equal(argument.cause, config);
    assert.ok(argument instanceof TypeError);
    assert.equal(new CliError('z').code, 'ALX_ERR_CLI');
  });

  it('should share one FramingError between WebTransport and WebRTC', () => {
    assert.equal(require('../../src/webtransport/framing.js').FramingError, FramingError);
    assert.equal(require('../../src/webrtc/framing.js').FramingError, FramingError);
  });

  it('should throw no bare TypeError in src/ but the ones that are not a config or an argument mistake', () => {
    const found = {};
    for (const name of readdirSync(path.join(ROOT, 'src'), { recursive: true })) {
      if (!name.endsWith('.js')) continue;
      const file = `src/${name.split(path.sep).join('/')}`;
      const count = read(file).split('new TypeError(').length - 1;
      if (count > 0) found[file] = count;
    }
    assert.deepEqual(found, BARE_TYPE_ERRORS);
  });
});
