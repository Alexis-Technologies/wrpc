'use strict';

// The string codes wrpc throws are part of its API: every one the source
// writes is in the `ErrorCode` union of src/errors.d.ts and a row of the
// table in docs/reference/errors.md, and nothing is in either that the source
// does not write. A code that is renamed or removed fails here.

const path = require('node:path');
const { readFileSync, readdirSync } = require('node:fs');
const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { ConfigError, ArgumentError, WrpcError } = require('../../index.js');
const { FramingError, TransportError } = require('../../src/errors.js');
const { CliError } = require('../../src/cli/types.js');
const { DeflateError } = require('../../src/deflate/index.js');
const { ParseError, PARSE_ERR_CODES } = require('../../src/websocket/ws.js');
const { AssertionError, outcomeOf } = require('../../src/webrtc/assertions.js');
const { OpenError, EncryptionRefusedError } = require('../../src/encryption/index.js');

const ROOT = path.join(__dirname, '..', '..');
// A code never ends with `_`: `ALX_ERR_FRAMING_*` in a comment is a family.
const CODE = /ALX_ERR_[A-Z0-9_]*[A-Z0-9](?![A-Z0-9_])/g;

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
    const row = /^\| `(ALX_ERR_[A-Z0-9_]+)` \|/.exec(line);
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

  it('should take (message, options) in every class, as Error does, with cause only when given', () => {
    const cause = new Error('under');
    const made = [
      [new ConfigError('a', { cause }), 'ConfigError', 'ALX_ERR_CONFIG'],
      [new ArgumentError('a', { cause }), 'ArgumentError', 'ALX_ERR_ARGUMENT'],
      [new CliError('a', { cause }), 'CliError', 'ALX_ERR_CLI'],
      [new FramingError('a', { code: 'ALX_ERR_FRAMING_UTF8', cause }), 'FramingError', 'ALX_ERR_FRAMING_UTF8'],
      [new TransportError('a', { code: 'ALX_ERR_BACKPRESSURE', cause }), 'TransportError', 'ALX_ERR_BACKPRESSURE'],
      [new DeflateError('a', { code: 'ALX_ERR_DEFLATE_BLOCK', cause }), 'DeflateError', 'ALX_ERR_DEFLATE_BLOCK'],
      [
        new ParseError('a', { code: PARSE_ERR_CODES.INVALID_PAYLOAD, cause }),
        'ParseError',
        'ALX_ERR_PARSE_INVALID_PAYLOAD',
      ],
      [new AssertionError('a', { code: 'ALX_ERR_ASSERTION_KID', cause }), 'AssertionError', 'ALX_ERR_ASSERTION_KID'],
      [new OpenError('a', { cause }), 'OpenError', 'ALX_ERR_OPEN'],
      [
        new EncryptionRefusedError('a', { statusCode: 426, cause }),
        'EncryptionRefusedError',
        'ALX_ERR_ENCRYPTION_REFUSED',
      ],
      [new WrpcError('a', { code: 404, cause }), 'WrpcError', 404],
    ];
    for (const [error, name, code] of made) {
      assert.deepEqual([error.message, error.name, error.code, error.cause], ['a', name, code, cause], name);
      assert.ok(error instanceof Error, name);
    }
    assert.equal(Object.hasOwn(new ConfigError('x'), 'cause'), false, 'no own cause when none is given');
    assert.equal(Object.hasOwn(new WrpcError('x', { code: 400 }), 'cause'), false);
  });

  it('should give WrpcError a 500 by default, and details and expose only when given', () => {
    const bare = new WrpcError('x');
    assert.equal(bare.code, 500);
    assert.equal(Object.hasOwn(bare, 'details'), false);
    assert.equal(Object.hasOwn(bare, 'expose'), false);
    const full = new WrpcError('y', { code: 400, details: { issues: [] }, expose: true });
    assert.deepEqual([full.code, full.details, full.expose], [400, { issues: [] }, true]);
    assert.equal(String(full), 'WrpcError: y');
  });

  it('should carry the class-specific fields from options', () => {
    assert.equal(new OpenError().message, 'encryption: the message does not open');
    assert.equal(new OpenError(undefined, { reason: 'replay' }).reason, 'replay');
    assert.equal(Object.hasOwn(new OpenError(), 'reason'), false);
    const refused = new EncryptionRefusedError('z', { statusCode: 409 });
    assert.deepEqual([refused.statusCode, refused.status], [409, 409], 'status is the 2.x name');
  });

  it('should count an assertion refusal under the metric label its code ends with', () => {
    assert.equal(outcomeOf(new AssertionError('x', { code: 'ALX_ERR_ASSERTION_SIGNATURE' })), 'signature');
    assert.equal(outcomeOf(new AssertionError('x', { code: 'ALX_ERR_ASSERTION_MISSING' })), 'missing');
    assert.equal(outcomeOf(new Error('x')), 'invalid');
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
