'use strict';

// A sparse array an application passes in an option is accepted, as it was
// before 2.3: the methods the loops of src/ replaced (`every`, `some`, `map`,
// `filter`) skipped holes, and the loops skip them too (alexiskills 1.1.2).
// Where the method's result went on to crash on a hole (declaredHeaders,
// compression.encodings, assertion keys), the list now simply works.

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { RpcServer, defineRouter } = require('../index.js');
const { normalizeRetry, decide } = require('../src/broker/retry.js');
const { normalizeServerEncryption } = require('../src/encryption/server.js');
const { normalizeKeys, generateKey } = require('../src/encryption/keyring.js');
const { normalizeDeclaredHeaders } = require('../src/rpc/handshake.js');
const { normalizeCompression } = require('../src/content-encoding.js');
const { setupMirror } = require('../src/adapters/mirror.js');
const { createAssertionVerifier } = require('../src/webrtc/assertions.js');
const { createAssertionIssuer, generateAssertionKeys } = require('../src/webrtc/assertion-issuer.js');

// `sparse('a')` is `[, 'a']`: length 2, index 0 a hole.
const sparse = (...entries) => {
  const list = [];
  list.length = entries.length + 1;
  for (let i = 0; i < entries.length; i++) list[i + 1] = entries[i];
  return list;
};

const FP = 'sha-256 4A:AD:B9:B1:3F:82:18:3B:54:02:12:DF:3E:5D:49:6B:19:E5:7C:AB:3D:0A:DE:5F:13:4C:9E:7E:D2:A9:AA:DD';
const SDP = `v=0\r\na=fingerprint:${FP}\r\n`;

describe('a sparse array in an option', () => {
  it('should have a hole to skip', () => {
    const list = sparse('a');
    assert.strictEqual(list.length, 2);
    assert.strictEqual(0 in list, false);
  });

  it('should be accepted as a retryOn list, the codes it holds retried', () => {
    const retry = normalizeRetry({ retryOn: sparse(503) }, 'test');
    assert.strictEqual(decide({ code: 503, attempt: 1, retry }).action, 'retry');
    assert.strictEqual(decide({ code: 400, attempt: 1, retry }).action, 'dead');
  });

  it('should be accepted as encryption patterns and ciphers', () => {
    const encryption = normalizeServerEncryption(
      { keys: generateKey(), patterns: sparse('NN'), ciphers: sparse('aes-256-gcm') },
      'test',
    );
    assert.deepStrictEqual([...encryption.patterns], ['NN']);
    assert.deepStrictEqual([...encryption.ciphers], ['aes-256-gcm']);
  });

  it('should be accepted as cors.origins', () => {
    const rpc = new RpcServer({
      router: defineRouter({}),
      logger: false,
      cors: { origins: sparse('https://a.example') },
    });
    assert.ok(rpc);
    return rpc.close();
  });

  it('should be accepted as declaredHeaders, the hole naming nothing', () => {
    assert.deepStrictEqual(normalizeDeclaredHeaders(sparse('X-Tenant'), 'test'), new Set(['x-tenant']));
  });

  it('should be accepted as compression.encodings, the hole naming no encoding', () => {
    const compression = normalizeCompression({ encodings: sparse('gzip') }, 'test');
    assert.strictEqual(compression.encoders.length, 1);
    assert.strictEqual(compression.encoders[0].token, 'gzip');
  });

  it('should be accepted as a mirror allowHeaders list', () => {
    const hooks = [];
    const fastify = { addHook: (name) => void hooks.push(name) };
    assert.doesNotThrow(() => setupMirror(fastify, null, { mirror: { allowHeaders: sparse('X-Request-Id') } }));
    assert.deepStrictEqual(hooks, ['onRoute', 'onReady']);
  });

  it('should be read past by a key provider that answers kids() with holes', () => {
    const master = generateKey();
    const keys = normalizeKeys({
      current: () => 'a',
      get: (kid) => (kid === 'a' ? master : null),
      kids: () => sparse('a'),
    });
    assert.deepStrictEqual(keys.kids, ['a']);
  });

  it('should be accepted as assertion keys, and verify what the key signs', async () => {
    const pair = await generateAssertionKeys({ kid: 'k1' });
    const issuer = createAssertionIssuer({ key: pair.privateKey, ttl: 60 });
    const { assertion } = await issuer.sign({ sub: 'alice', fp: FP });
    const verifier = createAssertionVerifier({ keys: sparse(pair.publicKey) });
    const claims = await verifier.verify(assertion, { from: 'alice', sdp: SDP });
    assert.strictEqual(claims.sub, 'alice');
  });

  it('should still refuse assertion keys that are holes only', async () => {
    const pair = await generateAssertionKeys({ kid: 'k1' });
    const { assertion } = await createAssertionIssuer({ key: pair.privateKey, ttl: 60 }).sign({ sub: 'alice', fp: FP });
    const verifier = createAssertionVerifier({ keys: () => sparse() });
    await assert.rejects(verifier.verify(assertion, { from: 'alice', sdp: SDP }), /expected a non-empty array/);
  });
});
