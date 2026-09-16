'use strict';

// Trust assertions: the issuer signs, the verifier binds — over real ES256
// keys from WebCrypto, no WebRTC in sight. Every refusal code is reached.

const { test } = require('node:test');
const assert = require('node:assert');

const {
  AssertionError,
  base64urlEncode,
  base64urlDecode,
  isAssertion,
  parseJws,
  normalizeFingerprint,
  sdpFingerprint,
  createAssertionVerifier,
} = require('../../src/webrtc/assertions.js');
const { createAssertionIssuer, generateAssertionKeys } = require('../../src/webrtc/assertionIssuer.js');

const FP = 'sha-256 4A:AD:B9:B1:3F:82:18:3B:54:02:12:DF:3E:5D:49:6B:19:E5:7C:AB:3E:4B:65:2E:7D:46:3F:54:42:CD:54:F1';
const sdpWith = (fingerprint, extra = '') =>
  `v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\n${extra}a=fingerprint:${fingerprint}\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n`;

// A Chrome-shaped SDP: the fingerprint sits at the media level, in upper case.
const CHROME_SDP = [
  'v=0',
  'o=- 4611731400430051336 2 IN IP4 127.0.0.1',
  's=-',
  't=0 0',
  'a=group:BUNDLE 0',
  'a=msid-semantic: WMS',
  'm=application 9 UDP/DTLS/SCTP webrtc-datachannel',
  'c=IN IP4 0.0.0.0',
  'a=ice-ufrag:Yr8m',
  'a=ice-pwd:NsRXoAYbdjE0s6ehXcE4Jj0X',
  'a=ice-options:trickle',
  `a=fingerprint:${FP}`,
  'a=setup:actpass',
  'a=mid:0',
  'a=sctp-port:5000',
  'a=max-message-size:262144',
  '',
].join('\r\n');

const tamper = (token, part, mutate) => {
  const parts = token.split('.');
  const decoded = JSON.parse(new TextDecoder().decode(base64urlDecode(parts[part])));
  parts[part] = base64urlEncode(new TextEncoder().encode(JSON.stringify(mutate(decoded))));
  return parts.join('.');
};

const refused = (code) => (error) => error instanceof AssertionError && error.code === code;

test('assertions: base64url round-trips and refuses what is not base64url', () => {
  const bytes = new Uint8Array(300);
  for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 37) & 0xff;
  const text = base64urlEncode(bytes);
  assert.match(text, /^[A-Za-z0-9_-]+$/);
  assert.deepStrictEqual(base64urlDecode(text), bytes);
  assert.deepStrictEqual(base64urlDecode(''), new Uint8Array(0));
  assert.throws(() => base64urlDecode('a+b'), refused('malformed'));
  assert.throws(() => base64urlDecode(7), refused('malformed'));
});

test('assertions: fingerprints are normalized and read from an SDP at either level', () => {
  assert.strictEqual(normalizeFingerprint(FP.toLowerCase()), FP);
  assert.strictEqual(normalizeFingerprint(`  ${FP} `), FP);
  assert.strictEqual(normalizeFingerprint('sha-256 4A:AD'), 'sha-256 4A:AD');
  assert.strictEqual(normalizeFingerprint('sha-256 4AAD'), null);
  assert.strictEqual(normalizeFingerprint('sha-256'), null);
  assert.strictEqual(normalizeFingerprint(42), null);
  assert.strictEqual(normalizeFingerprint('x'.repeat(600)), null);
  assert.strictEqual(sdpFingerprint(CHROME_SDP), FP);
  assert.strictEqual(sdpFingerprint(sdpWith(FP.toLowerCase())), FP);
  // Several algorithms: the one asked for is picked, whatever the order.
  const multi = sdpWith(FP, 'a=fingerprint:sha-1 AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD\r\n');
  assert.strictEqual(sdpFingerprint(multi), FP);
  assert.strictEqual(
    sdpFingerprint(multi, 'SHA-1'),
    'sha-1 AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD',
  );
  assert.strictEqual(sdpFingerprint(multi, 'sha-512'), null);
  assert.strictEqual(sdpFingerprint('v=0\r\n'), null);
  assert.strictEqual(sdpFingerprint(null), null);
  assert.strictEqual(sdpFingerprint('a=fingerprint:sha-256 not:hex\r\n'), null);
});

test('assertions: an issuer signs what a verifier accepts, and binds it', async () => {
  const keys = await generateAssertionKeys({ kid: 'k1' });
  assert.strictEqual(keys.privateKey.kid, 'k1');
  assert.strictEqual(keys.publicKey.d, undefined);
  assert.deepStrictEqual([keys.publicKey.alg, keys.publicKey.use], ['ES256', 'sig']);
  const issuer = createAssertionIssuer({ key: keys.privateKey, ttl: 120, issuer: 'signaling.example' });
  assert.strictEqual(issuer.kid, 'k1');
  assert.strictEqual(issuer.ttl, 120);
  const before = Math.floor(Date.now() / 1000);
  const { assertion, iat, exp } = await issuer.sign({ sub: 'alice', fp: FP, role: 'host', exp: 1, iss: 'me' });
  assert.ok(iat >= before && iat <= before + 1);
  assert.strictEqual(exp, iat + 120);
  assert.ok(isAssertion(assertion));
  const { header, payload } = parseJws(assertion);
  assert.deepStrictEqual(header, { alg: 'ES256', typ: 'wrpc-rtc+jwt', kid: 'k1' });
  assert.deepStrictEqual(payload, { sub: 'alice', iat, exp, fp: FP, role: 'host', iss: 'signaling.example' });
  assert.deepStrictEqual(await issuer.publicKeys(), [keys.publicKey]);

  const verifier = createAssertionVerifier({ keys: keys.publicKey, issuer: 'signaling.example' });
  const claims = await verifier.verify(assertion, { from: 'alice', sdp: CHROME_SDP });
  assert.deepStrictEqual(claims, payload);
  assert.ok(Object.isFrozen(claims));
  // A lone key needs no kid to match; a lower-case sdp fingerprint still binds.
  const unkeyed = createAssertionVerifier({ keys: [{ ...keys.publicKey, kid: undefined }] });
  await unkeyed.verify(assertion, { from: 'alice', sdp: sdpWith(FP.toLowerCase()) });

  // Every binding, refused with its code.
  await assert.rejects(verifier.verify(assertion, { from: 'bob', sdp: CHROME_SDP }), refused('subject'));
  await assert.rejects(
    verifier.verify(assertion, { from: 'alice', sdp: sdpWith(FP.replace('4A', '4B')) }),
    refused('fingerprint'),
  );
  await assert.rejects(verifier.verify(assertion, { from: 'alice', sdp: 'v=0\r\n' }), refused('fingerprint'));
  await assert.rejects(
    verifier.verify(assertion, { from: 'alice', sdp: CHROME_SDP, now: (exp + 61) * 1000 }),
    refused('expired'),
  );
  await verifier.verify(assertion, { from: 'alice', sdp: CHROME_SDP, now: (exp + 59) * 1000 });
  const strict = createAssertionVerifier({ keys: keys.publicKey, issuer: 'someone.else' });
  await assert.rejects(strict.verify(assertion, { from: 'alice', sdp: CHROME_SDP }), refused('issuer'));
  // Another key: the signature does not hold.
  const other = await generateAssertionKeys({ kid: 'k1' });
  const stranger = createAssertionVerifier({ keys: other.publicKey });
  await assert.rejects(stranger.verify(assertion, { from: 'alice', sdp: CHROME_SDP }), refused('signature'));
  // A tampered payload: the signature no longer covers it.
  const forged = tamper(assertion, 1, (p) => ({ ...p, sub: 'bob' }));
  await assert.rejects(verifier.verify(forged, { from: 'bob', sdp: CHROME_SDP }), refused('signature'));
});

test('assertions: the header is checked before any crypto', async () => {
  const keys = await generateAssertionKeys();
  const issuer = createAssertionIssuer({ key: keys.privateKey });
  const verifier = createAssertionVerifier({ keys: keys.publicKey });
  const { assertion } = await issuer.sign({ sub: 'alice', fp: FP });
  const context = { from: 'alice', sdp: CHROME_SDP };
  await assert.rejects(
    verifier.verify(
      tamper(assertion, 0, (h) => ({ ...h, alg: 'none' })),
      context,
    ),
    refused('alg'),
  );
  await assert.rejects(
    verifier.verify(
      tamper(assertion, 0, (h) => ({ ...h, alg: 'HS256' })),
      context,
    ),
    refused('alg'),
  );
  await assert.rejects(
    verifier.verify(
      tamper(assertion, 0, (h) => ({ ...h, typ: 'JWT' })),
      context,
    ),
    refused('typ'),
  );
  await assert.rejects(
    verifier.verify(
      tamper(assertion, 0, (h) => ({ ...h, kid: 7 })),
      context,
    ),
    refused('kid'),
  );
  await assert.rejects(
    verifier.verify(
      tamper(assertion, 0, (h) => ({ ...h, kid: 'other' })),
      context,
    ),
    refused('kid'),
  );
  for (const bad of ['', 'a.b', 'a.b.c.d', 'x'.repeat(5000), 42, null, `${assertion}!`]) {
    await assert.rejects(verifier.verify(bad, context), refused('malformed'));
  }
  await assert.rejects(verifier.verify('e30.e30.e30', context), refused('typ'), 'empty objects parse, then fail typ');
  // Valid base64url, invalid JSON / non-object JSON.
  const enc = (text) => base64urlEncode(new TextEncoder().encode(text));
  await assert.rejects(verifier.verify(`${enc('{')}.${enc('{}')}.AA`, context), refused('malformed'));
  await assert.rejects(verifier.verify(`${enc('[]')}.${enc('{}')}.AA`, context), refused('malformed'));
  await assert.rejects(verifier.verify(`${enc('{}')}.${enc('"x"')}.AA`, context), refused('malformed'));
  assert.strictEqual(isAssertion('a.b.c'), true);
  assert.strictEqual(isAssertion('a.b'), false);
});

test('assertions: a keys function is asked once, and again for an unknown kid (rotation)', async () => {
  const first = await generateAssertionKeys({ kid: 'k1' });
  const second = await generateAssertionKeys({ kid: 'k2' });
  let published = [first.publicKey];
  let asked = 0;
  const verifier = createAssertionVerifier({
    keys: async () => {
      asked++;
      return published;
    },
  });
  const sign = (keys) => createAssertionIssuer({ key: keys.privateKey }).sign({ sub: 'alice', fp: FP });
  const context = { from: 'alice', sdp: CHROME_SDP };
  await verifier.verify((await sign(first)).assertion, context);
  await verifier.verify((await sign(first)).assertion, context);
  assert.strictEqual(asked, 1, 'imported once, cached');
  // The server rotated: the unknown kid triggers one refetch.
  published = [first.publicKey, second.publicKey];
  await verifier.verify((await sign(second)).assertion, context);
  assert.strictEqual(asked, 2);
  await verifier.verify((await sign(first)).assertion, context);
  assert.strictEqual(asked, 2, 'both keys known now');
  // A kid nobody publishes: one refetch, then refused.
  const third = await generateAssertionKeys({ kid: 'k3' });
  await assert.rejects(verifier.verify((await sign(third)).assertion, context), refused('kid'));
  assert.strictEqual(asked, 3);
  // A keys function that fails or answers garbage refuses with 'kid' too.
  const broken = createAssertionVerifier({ keys: async () => [] });
  await assert.rejects(broken.verify((await sign(first)).assertion, context), refused('kid'));
  const failing = createAssertionVerifier({
    keys: async () => {
      throw new Error('offline');
    },
  });
  await assert.rejects(
    failing.verify((await sign(first)).assertion, context),
    (error) => error.code === 'kid' && /offline/.test(error.message),
  );
});

test('assertions: option validation on both halves', async () => {
  const keys = await generateAssertionKeys();
  assert.throws(() => createAssertionVerifier({}), /keys must be/);
  assert.throws(() => createAssertionVerifier({ keys: { kty: 'RSA' } }), /keys must be/);
  assert.throws(() => createAssertionVerifier({ keys: keys.publicKey, issuer: '' }), /issuer must be/);
  assert.throws(() => createAssertionVerifier({ keys: keys.publicKey, subtle: null }), /WebCrypto/);
  assert.throws(() => createAssertionIssuer({}), /key must be/);
  assert.throws(() => createAssertionIssuer({ key: keys.publicKey }), /key must be/, 'a public JWK cannot sign');
  assert.throws(() => createAssertionIssuer({ key: keys.privateKey, ttl: 0 }), /ttl must be/);
  assert.throws(() => createAssertionIssuer({ key: keys.privateKey, issuer: 7 }), /issuer must be/);
  assert.throws(() => createAssertionIssuer({ key: keys.privateKey, kid: '' }), /kid must be/);
  assert.throws(() => createAssertionIssuer({ key: keys.privateKey, subtle: {} }), /WebCrypto/);
  await assert.rejects(generateAssertionKeys({ subtle: null }), /WebCrypto/);
  const issuer = createAssertionIssuer({ key: keys.privateKey });
  await assert.rejects(issuer.sign(null), /claims must be an object/);
  await assert.rejects(issuer.sign({ fp: FP }), /claims.sub is required/);
  await assert.rejects(issuer.sign({ sub: 'a' }), /claims.fp is required/);
  // A CryptoKey pair works as the key, with an explicit kid.
  const pair = await globalThis.crypto.subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, [
    'sign',
    'verify',
  ]);
  const fromPair = createAssertionIssuer({ key: pair, kid: 'pair' });
  const [jwk] = await fromPair.publicKeys();
  assert.strictEqual(jwk.kid, 'pair');
  assert.strictEqual(jwk.d, undefined);
  const { assertion } = await fromPair.sign({ sub: 'alice', fp: FP });
  assert.strictEqual(parseJws(assertion).header.kid, 'pair');
  await createAssertionVerifier({ keys: [jwk] }).verify(assertion, { from: 'alice', sdp: CHROME_SDP });
  // A private JWK without a kid: no kid in the header, a lone-key verifier accepts.
  const bare = createAssertionIssuer({ key: { ...keys.privateKey, kid: undefined } });
  assert.strictEqual(bare.kid, null);
  const plain = await bare.sign({ sub: 'alice', fp: FP });
  assert.strictEqual(parseJws(plain.assertion).header.kid, undefined);
  assert.strictEqual((await bare.publicKeys())[0].kid, undefined);
});
