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
const { FramingError, TransportError, ClosedError, ProtocolError } = require('../../src/errors.js');
const { CliError } = require('../../src/cli/types.js');
const { DeflateError } = require('../../src/deflate/index.js');
const { ParseError, PARSE_ERR_CODES } = require('../../src/websocket/ws.js');
const { AssertionError, outcomeOf } = require('../../src/webrtc/assertions.js');
const { OpenError, EncryptionError, EncryptionRefusedError } = require('../../src/encryption/index.js');

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

// The bare Errors that stay, each for a reason: the code of the Node.js
// error it stands in for, set on it (`EADDRINUSE`, `ERR_STREAM_PREMATURE_CLOSE`,
// `ERR_HTTP_HEADERS_SENT`, `ERR_UNHANDLED_ERROR`).
const BARE_ERRORS = {
  'src/adapters/common.js': 1,
  'src/adapters/uws.js': 1,
  'src/transport.js': 1,
  'src/utils.js': 1,
};

// The RangeErrors that stay: a length an internal caller got wrong (hkdf, the
// segment queue's own bookkeeping), a peer's handshake message past the
// Noise maximum, and the inflate cap under zlib's own code on both halves.
const BARE_RANGE_ERRORS = {
  'src/compression/native.browser.js': 1,
  'src/encryption/hkdf.browser.js': 1,
  'src/encryption/hkdf.js': 1,
  'src/encryption/noise.js': 1,
  'src/websocket/deflateContext.js': 1,
  'src/websocket/segments.js': 1,
};

const countIn = (needle) => {
  const found = {};
  for (const name of readdirSync(path.join(ROOT, 'src'), { recursive: true })) {
    if (!name.endsWith('.js')) continue;
    const file = `src/${name.split(path.sep).join('/')}`;
    const count = read(file).split(needle).length - 1;
    if (count > 0) found[file] = count;
  }
  return found;
};

describe('error codes', () => {
  it('should declare and document every code the source throws', () => {
    const thrown = sourceCodes();
    assert.deepEqual(unionCodes(), thrown, 'the ErrorCode union of src/errors.d.ts');
    assert.deepEqual(tableCodes(), thrown, 'the table in docs/reference/errors.md');
  });

  // These threw a bare Error: no code to branch on, and a name that said
  // nothing of what was wrong — the setup, or an argument.
  it('should throw ConfigError or ArgumentError, not a bare Error, for a bad setup or argument', async () => {
    const { WrpcClient, WrpcClientProxy } = require('../../src/client.js');
    const { Emitter } = require('../../src/utils.js');
    const { chunkEncode } = require('../../src/chunks.js');
    const browserChunks = require('../../src/chunks.browser.js');
    const { createQueryUtils } = require('../../src/query/index.js');
    const config = (error) => error instanceof ConfigError && error.code === 'ALX_ERR_CONFIG';
    const argument = (error) => error instanceof ArgumentError && error.code === 'ALX_ERR_ARGUMENT';

    const connecting = (transport) => async () => WrpcClient.connect('ws://127.0.0.1:1', { transport });
    await assert.rejects(connecting([]), config);
    await assert.rejects(connecting(['ws', 'event']), config);
    await assert.rejects(connecting(['ws', 'smoke']), config);
    await assert.rejects(connecting('smoke'), config);
    const { ws: ClientWsTransport, event: ClientEventTransport } = WrpcClient.transport;
    const client = new WrpcClient('ws://127.0.0.1:1', new ClientWsTransport('ws://127.0.0.1:1'), {
      reconnect: false,
      heartbeat: false,
    });
    client.respond('twice', () => {});
    assert.throws(() => client.respond('twice', () => {}), argument);
    assert.throws(() => client.getStream('none'), argument);
    assert.throws(() => new WrpcClientProxy(), config);
    await assert.rejects(new ClientEventTransport('').open(), config);

    const emitter = new Emitter();
    const listener = () => {};
    emitter.on('x', listener);
    assert.throws(() => emitter.on('x', listener), argument);
    assert.throws(() => emitter.listeners(''), argument);
    assert.throws(() => emitter.listenerCount(''), argument);
    assert.throws(() => chunkEncode('x'.repeat(256), new Uint8Array(0)), argument);
    assert.throws(() => browserChunks.chunkEncode('x'.repeat(256), new Uint8Array(0)), argument);

    const query = createQueryUtils({ api: {} });
    await assert.rejects(() => query.queryOptions(['nope', 'send']).queryFn(), argument);
  });

  // What is gone, what a peer broke and what a carrier gave up on threw a
  // bare Error each: the message was all a caller had to branch on.
  it('should code what is gone, what was ended on purpose, and what a carrier gave up on', async () => {
    const { WrpcClient } = require('../../src/client.js');
    const { Semaphore } = require('../../src/utils.js');
    const { ws: ClientWsTransport } = WrpcClient.transport;
    assert.throws(
      () => new ClientWsTransport('ws://127.0.0.1:1').write('{}'),
      (error) => error instanceof ClosedError && error.code === 'ALX_ERR_CLOSED',
    );
    const full = new Semaphore({ concurrency: 1, size: 0 });
    await full.enter();
    await assert.rejects(
      full.enter(),
      (error) => error instanceof TransportError && error.code === 'ALX_ERR_QUEUE_FULL',
    );
    const waiting = new Semaphore({ concurrency: 0, size: 1 });
    const controller = new AbortController();
    const entered = waiting.enter(controller.signal);
    controller.abort();
    await assert.rejects(entered, (error) => error instanceof ClosedError && error.code === 'ALX_ERR_ABORTED');
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
      [new ClosedError('a', { cause }), 'ClosedError', 'ALX_ERR_CLOSED'],
      [new ClosedError('a', { code: 'ALX_ERR_ABORTED', cause }), 'ClosedError', 'ALX_ERR_ABORTED'],
      [new ProtocolError('a', { cause }), 'ProtocolError', 'ALX_ERR_PROTOCOL'],
      [new OpenError('a', { cause }), 'OpenError', 'ALX_ERR_OPEN'],
      [
        new EncryptionError('a', { code: 'ALX_ERR_ENCRYPTION_KEY', cause }),
        'EncryptionError',
        'ALX_ERR_ENCRYPTION_KEY',
      ],
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
    assert.equal(Object.hasOwn(bare, 'statusCode'), false);
    // The status under the name a host's error handler reads, given to the
    // constructor rather than set on the instance after it.
    assert.equal(new WrpcError('z', { code: 403, statusCode: 403 }).statusCode, 403);
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
    assert.deepEqual(countIn('new TypeError('), BARE_TYPE_ERRORS);
  });

  // 186 of them had no code until the review against alexiskills 1.2: a
  // caller could tell a closed connection from a peer's bad packet only by
  // the message. A new one takes a class of src/errors.js, or a row here.
  it('should throw no bare Error or RangeError in src/ but the ones listed with their reason', () => {
    assert.deepEqual(countIn('new Error('), BARE_ERRORS);
    assert.deepEqual(countIn('new RangeError('), BARE_RANGE_ERRORS);
  });
});
