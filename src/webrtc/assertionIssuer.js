'use strict';

// The issuing half of trust assertions: what the signaling unit signs with.
// Node-only by placement (required from signaling.js, never from the
// browser barrel) though it too speaks WebCrypto only — a page never issues.
// Format and binding: see assertions.js.

const {
  ASSERTION_TYP,
  ASSERTION_ALG,
  ES256,
  ES256_SIGN,
  base64urlEncode,
  isJwk,
  publicJwk,
} = require('./assertions.js');
const { generateUUID } = require('../runtime/node.js');

const DEFAULT_TTL = 300;
const RESERVED = ['iss', 'sub', 'iat', 'exp', 'fp'];

const encoder = new TextEncoder();

const encodeJson = (value) => base64urlEncode(encoder.encode(JSON.stringify(value)));

const isPrivateJwk = (value) => isJwk(value) && typeof value.d === 'string';

const isCryptoKey = (value) => typeof value === 'object' && value !== null && typeof value.type === 'string';

/**
 * A fresh ES256 key pair as JWKs, `kid` attached to both — for a server's
 * boot, a config script, a test. Keep the private one private.
 */
const generateAssertionKeys = async ({ kid = generateUUID(), subtle = globalThis.crypto?.subtle } = {}) => {
  if (!subtle) throw new TypeError('generateAssertionKeys: WebCrypto (crypto.subtle) is required');
  const pair = await subtle.generateKey(ES256, true, ['sign', 'verify']);
  // Without key_ops: the JWKs travel between an issuer and verifiers whose
  // usage is decided at import, and publicKeys() strips it the same way.
  const { key_ops: _sign, ...privateKey } = await subtle.exportKey('jwk', pair.privateKey);
  const { key_ops: _verify, ...publicKey } = await subtle.exportKey('jwk', pair.publicKey);
  return {
    kid,
    privateKey: { ...privateKey, kid, alg: ASSERTION_ALG, use: 'sig' },
    publicKey: { ...publicKey, kid, alg: ASSERTION_ALG, use: 'sig' },
  };
};

/**
 * Signs assertions. `key` is a private EC P-256 JWK (generateAssertionKeys'
 * `privateKey`) or a `{ privateKey, publicKey }` CryptoKey pair. Options:
 *   kid     the key id put in the header (default: the JWK's `kid`, if any)
 *   ttl     seconds an assertion is valid for (default 300)
 *   issuer  the `iss` claim, when the verifiers expect one
 *
 *   sign({ sub, fp, ...claims }) -> { assertion, iat, exp }
 *   publicKeys() -> [JWK]           what verifiers need, for a keys() call
 */
const createAssertionIssuer = ({
  key,
  kid,
  ttl = DEFAULT_TTL,
  issuer = null,
  subtle = globalThis.crypto?.subtle,
} = {}) => {
  if (!subtle || typeof subtle.sign !== 'function') {
    throw new TypeError('createAssertionIssuer: WebCrypto (crypto.subtle) is required');
  }
  if (!(Number.isInteger(ttl) && ttl > 0)) throw new TypeError('createAssertionIssuer: ttl must be a positive integer');
  if (issuer !== null && (typeof issuer !== 'string' || issuer.length === 0)) {
    throw new TypeError('createAssertionIssuer: issuer must be a non-empty string');
  }
  let privateKey;
  let publicKey;
  let keyId = kid ?? null;
  if (isPrivateJwk(key)) {
    keyId ??= typeof key.kid === 'string' ? key.kid : null;
    privateKey = subtle.importKey('jwk', key, ES256, false, ['sign']);
    publicKey = Promise.resolve({
      ...publicJwk(key),
      ...(keyId ? { kid: keyId } : {}),
      alg: ASSERTION_ALG,
      use: 'sig',
    });
  } else if (typeof key === 'object' && key !== null && isCryptoKey(key.privateKey) && isCryptoKey(key.publicKey)) {
    privateKey = Promise.resolve(key.privateKey);
    publicKey = subtle
      .exportKey('jwk', key.publicKey)
      .then((jwk) => ({ ...publicJwk(jwk), ...(keyId ? { kid: keyId } : {}), alg: ASSERTION_ALG, use: 'sig' }));
  } else {
    throw new TypeError(
      'createAssertionIssuer: key must be a private EC P-256 JWK or a { privateKey, publicKey } pair',
    );
  }
  if (keyId !== null && (typeof keyId !== 'string' || keyId.length === 0)) {
    throw new TypeError('createAssertionIssuer: kid must be a non-empty string');
  }
  // A key that never imports fails every sign() with the import's error,
  // never as an unhandled rejection.
  privateKey.catch(() => {});
  publicKey.catch(() => {});

  const header = { alg: ASSERTION_ALG, typ: ASSERTION_TYP };
  if (keyId !== null) header.kid = keyId;
  const encodedHeader = encodeJson(header);

  const sign = async (claims) => {
    if (typeof claims !== 'object' || claims === null) throw new TypeError('sign: claims must be an object');
    if (typeof claims.sub !== 'string' || claims.sub.length === 0) throw new TypeError('sign: claims.sub is required');
    if (typeof claims.fp !== 'string' || claims.fp.length === 0) throw new TypeError('sign: claims.fp is required');
    const iat = Math.floor(Date.now() / 1000);
    const exp = iat + ttl;
    // The reserved claims come last: nothing custom may override them.
    const payload = { ...claims, sub: claims.sub, iat, exp, fp: claims.fp };
    if (issuer !== null) payload.iss = issuer;
    else delete payload.iss;
    const signed = `${encodedHeader}.${encodeJson(payload)}`;
    const signature = await subtle.sign(ES256_SIGN, await privateKey, encoder.encode(signed));
    return { assertion: `${signed}.${base64urlEncode(new Uint8Array(signature))}`, iat, exp };
  };

  const publicKeys = async () => [await publicKey];

  return { sign, publicKeys, kid: keyId, ttl };
};

module.exports = { createAssertionIssuer, generateAssertionKeys, RESERVED_CLAIMS: Object.freeze(RESERVED) };
