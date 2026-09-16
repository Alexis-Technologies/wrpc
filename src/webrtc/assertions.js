'use strict';

// Trust assertions: the token a signaling server issues to a peer and the
// other peers verify — browser-safe, require-free, over WebCrypto only.
//
// An assertion is a JWS in compact serialization, `header.payload.sig` in
// base64url, alg ES256 (ECDSA P-256 / SHA-256 — the one algorithm every
// WebCrypto ships; WebCrypto's raw r||s signature is exactly what JWS ES256
// wants, so there is no DER to convert):
//
//   header  { alg: 'ES256', typ: 'wrpc-rtc+jwt', kid? }
//   payload { iss?, sub: <peer id>, iat, exp, fp: 'sha-256 AB:CD:…', ...claims }
//
// `fp` is the fingerprint of the DTLS certificate of the peer connection
// the token was issued for, as it appears in the SDP the peer sends
// (`a=fingerprint:`). The verifier compares it with the fingerprint in the
// description the assertion arrived with, and the DTLS handshake proves the
// remote end holds that certificate — so a token cannot be replayed from
// another connection, and the relay in between has nothing to lie about
// (the RFC 8827 identity binding, minus the IdP). Verification happens in
// WrpcPeer, before a description reaches the RtcLink.

const TYP = 'wrpc-rtc+jwt';
const ALG = 'ES256';
const ES256 = Object.freeze({ name: 'ECDSA', namedCurve: 'P-256' });
const ES256_SIGN = Object.freeze({ name: 'ECDSA', hash: 'SHA-256' });
const MAX_TOKEN_LENGTH = 4096;
// The request round trip and a little clock drift the server's clock
// offset (see WrpcPeer) does not absorb.
const SKEW_MS = 60_000;

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });

class AssertionError extends Error {
  constructor(message, code) {
    super(message);
    this.name = 'AssertionError';
    this.code = code;
  }
}

const refuse = (message, code) => {
  throw new AssertionError(message, code);
};

// base64url without Buffer: btoa/atob exist in every browser and in Node
// since 16. Tokens are a few hundred bytes, so a string built byte by byte
// is fine (and never `String.fromCharCode(...bytes)` — apply in disguise).
const base64urlEncode = (bytes) => {
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
};

const BASE64URL = /^[A-Za-z0-9_-]*$/;

const base64urlDecode = (text) => {
  if (typeof text !== 'string' || !BASE64URL.test(text)) refuse('assertion: not base64url', 'malformed');
  const padded = text + '='.repeat((4 - (text.length % 4)) % 4);
  let binary;
  try {
    binary = atob(padded.replace(/-/g, '+').replace(/_/g, '/'));
  } catch {
    refuse('assertion: not base64url', 'malformed');
  }
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
};

const isObject = (value) => typeof value === 'object' && value !== null && !Array.isArray(value);

const parseJson = (bytes, what) => {
  let value;
  try {
    value = JSON.parse(decoder.decode(bytes));
  } catch {
    refuse(`assertion: ${what} is not JSON`, 'malformed');
  }
  if (!isObject(value)) refuse(`assertion: ${what} is not an object`, 'malformed');
  return value;
};

/** True for anything shaped like a compact JWS of a sane size. */
const isAssertion = (value) =>
  typeof value === 'string' && value.length > 0 && value.length <= MAX_TOKEN_LENGTH && value.split('.').length === 3;

/** Splits a compact JWS into its parts; the header and payload parsed, the signed bytes kept. */
const parseJws = (token) => {
  if (!isAssertion(token)) refuse('assertion: not a compact JWS', 'malformed');
  const dot = token.indexOf('.');
  const second = token.indexOf('.', dot + 1);
  const header = parseJson(base64urlDecode(token.slice(0, dot)), 'header');
  const payload = parseJson(base64urlDecode(token.slice(dot + 1, second)), 'payload');
  const signature = base64urlDecode(token.slice(second + 1));
  return { header, payload, signed: encoder.encode(token.slice(0, second)), signature };
};

const FINGERPRINT = /^([A-Za-z0-9-]+) ((?:[0-9A-Fa-f]{2}:)+[0-9A-Fa-f]{2})$/;

/** `'sha-256 ab:cd'` -> `'sha-256 AB:CD'`, or null when it is not a fingerprint. */
const normalizeFingerprint = (value) => {
  if (typeof value !== 'string' || value.length > 512) return null;
  const match = FINGERPRINT.exec(value.trim());
  if (!match) return null;
  return `${match[1].toLowerCase()} ${match[2].toUpperCase()}`;
};

const FINGERPRINT_LINE = /^a=fingerprint:([A-Za-z0-9-]+)[ \t]+((?:[0-9A-Fa-f]{2}:)+[0-9A-Fa-f]{2})[ \t]*$/gm;

/**
 * The certificate fingerprint an SDP declares — session- or media-level,
 * the first line for `algorithm` (default sha-256, what every browser
 * emits), normalized like normalizeFingerprint — or null.
 */
const sdpFingerprint = (sdp, algorithm = 'sha-256') => {
  if (typeof sdp !== 'string') return null;
  const wanted = algorithm.toLowerCase();
  FINGERPRINT_LINE.lastIndex = 0;
  let match;
  while ((match = FINGERPRINT_LINE.exec(sdp)) !== null) {
    if (match[1].toLowerCase() === wanted) return `${wanted} ${match[2].toUpperCase()}`;
  }
  return null;
};

const isJwk = (value) => isObject(value) && value.kty === 'EC' && value.crv === 'P-256';

const publicJwk = (jwk) => {
  const { d: _d, key_ops: _ops, ...rest } = jwk;
  return rest;
};

/**
 * Verifies assertions against a set of public keys. `keys` is a JWK, an
 * array of JWKs, or a function answering them (a signaler's `keys()`, say)
 * — asked once, and once more when a `kid` is unknown, for rotation.
 *
 *   verify(token, { from, sdp, now? }) -> claims
 *
 * checks the header (`typ`, `alg`), finds the key by `kid` (a lone key
 * without one answers any), verifies the signature, then binds: `sub` must be `from`,
 * `fp` must be the fingerprint in `sdp`, `exp` must be after `now` (with a
 * minute of skew), `iss` must match when one is expected. Throws an
 * AssertionError with a `code` — the reason a link was refused.
 */
const createAssertionVerifier = ({ keys, issuer = null, subtle = globalThis.crypto?.subtle } = {}) => {
  if (!subtle || typeof subtle.verify !== 'function') {
    throw new TypeError('createAssertionVerifier: WebCrypto (crypto.subtle) is required');
  }
  if (typeof keys !== 'function' && !isJwk(keys) && !(Array.isArray(keys) && keys.every(isJwk))) {
    throw new TypeError('createAssertionVerifier: keys must be an EC P-256 JWK, an array of them, or a function');
  }
  if (issuer !== null && (typeof issuer !== 'string' || issuer.length === 0)) {
    throw new TypeError('createAssertionVerifier: issuer must be a non-empty string');
  }
  const imported = new Map(); // kid -> Promise<CryptoKey>
  // A lone key with no kid of its own is a wildcard: it answers any kid a
  // token names. A labelled key answers its own only — so a kid the set
  // does not know means a rotation to look up, not a key to try.
  let wildcard = null;
  let loaded = null;

  const load = async () => {
    const list = typeof keys === 'function' ? await keys() : Array.isArray(keys) ? keys : [keys];
    if (!Array.isArray(list) || list.length === 0 || !list.every(isJwk)) {
      throw new TypeError('assertion keys: expected a non-empty array of EC P-256 JWKs');
    }
    imported.clear();
    for (const jwk of list) {
      const kid = typeof jwk.kid === 'string' ? jwk.kid : '';
      imported.set(kid, subtle.importKey('jwk', publicJwk(jwk), ES256, false, ['verify']));
    }
    wildcard = list.length === 1 && typeof list[0].kid !== 'string' ? imported.get('') : null;
  };

  const lookup = (kid) => imported.get(kid) ?? wildcard;

  const keyFor = async (kid) => {
    if (loaded === null) loaded = load();
    await loaded;
    const found = lookup(kid);
    if (found) return found;
    if (typeof keys === 'function') {
      loaded = load();
      await loaded;
      const refreshed = lookup(kid);
      if (refreshed) return refreshed;
    }
    return refuse(`assertion: unknown key '${kid}'`, 'kid');
  };

  const verify = async (token, { from, sdp, now = Date.now() } = {}) => {
    const { header, payload, signed, signature } = parseJws(token);
    if (header.typ !== TYP) refuse(`assertion: typ must be ${TYP}`, 'typ');
    if (header.alg !== ALG) refuse(`assertion: alg must be ${ALG}`, 'alg');
    const kid = header.kid === undefined ? '' : header.kid;
    if (typeof kid !== 'string') refuse('assertion: kid must be a string', 'kid');
    let key;
    try {
      key = await keyFor(kid);
    } catch (error) {
      if (error instanceof AssertionError) throw error;
      refuse(`assertion: keys unavailable (${error.message})`, 'kid');
    }
    let valid = false;
    try {
      valid = await subtle.verify(ES256_SIGN, key, signature, signed);
    } catch {
      valid = false;
    }
    if (valid !== true) refuse('assertion: bad signature', 'signature');
    if (typeof payload.sub !== 'string' || payload.sub !== from) refuse('assertion: not about this peer', 'subject');
    const fingerprint = normalizeFingerprint(payload.fp);
    if (fingerprint === null) refuse('assertion: no fingerprint', 'fingerprint');
    const declared = sdpFingerprint(sdp, fingerprint.slice(0, fingerprint.indexOf(' ')));
    if (declared !== fingerprint) refuse('assertion: fingerprint does not match the description', 'fingerprint');
    if (typeof payload.exp !== 'number' || !(payload.exp * 1000 + SKEW_MS > now)) {
      refuse('assertion: expired', 'expired');
    }
    if (issuer !== null && payload.iss !== issuer) refuse('assertion: unexpected issuer', 'issuer');
    return Object.freeze({ ...payload });
  };

  return { verify };
};

module.exports = {
  AssertionError,
  ASSERTION_TYP: TYP,
  ASSERTION_ALG: ALG,
  ES256,
  ES256_SIGN,
  MAX_TOKEN_LENGTH,
  SKEW_MS,
  base64urlEncode,
  base64urlDecode,
  isAssertion,
  parseJws,
  normalizeFingerprint,
  sdpFingerprint,
  isJwk,
  publicJwk,
  createAssertionVerifier,
};
