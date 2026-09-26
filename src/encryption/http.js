'use strict';

// Encryption for a REQUEST — the HTTP transport, where there is no
// connection to hold a session: each request is sealed to the server's
// static key with HPKE (hpke.js), and the answer is sealed under a key both
// ends export from that same context. The construction is Oblivious HTTP's
// (RFC 9458 §4), without its relay.
//
//   POST <the transport's endpoint>          Content-Type: application/wrpc-sealed
//   request   u8 version (1) ‖ u8 aead id ‖ u8 kidLength ‖ kid ‖ enc ‖ ct
//   plaintext u32 headerLength ‖ JSON { m, u, h, t } ‖ body
//   response  nonce (32) ‖ AEAD( u32 headerLength ‖ JSON { s, h } ‖ body )
//
// The REAL request — method, URL, headers, body — is inside; what an
// observer sees is one endpoint being POSTed to. That is also what makes it
// uniform for the server: the core unwraps the call and carries on as if it
// had arrived in the clear, so packets, batches and REST all ride it.
//
// HPKE has no replay protection, and the party this exists for — whoever
// terminates the TLS — is exactly the one who could replay: so `t` (the
// sender's clock) must be fresh and an `enc` is accepted once.
//
// This file is the client half and what the two share — bundled for a page.
// The server half is httpServer.js, Node-only.

const { concat, utf8, counterNonce, fromBase64, toBase64Url } = require('./bytes.js');
const { OpenError } = require('./contracts.js');

const CONTENT_TYPE = 'application/wrpc-sealed';
const VERSION = 1;
const RESPONSE_NONCE = 32;
const INFO = utf8('wrpc http v1\0');
const RESPONSE_LABEL = utf8('wrpc http response');
const STREAM_LABEL = utf8('wrpc sse stream');
// A sealed event stream keeps the type every proxy knows how to treat (no
// buffering, no transformation) and says what it is in a parameter — the
// one response header a cross-origin page can always read.
const STREAM_TYPE = 'text/event-stream';
const SEALED_STREAM_TYPE = `${STREAM_TYPE}; wrpc-sealed=1`;
const KEY_LABEL = utf8('key');
const NONCE_LABEL = utf8('nonce');

const decoder = new TextDecoder('utf-8', { fatal: true });

const isSealedType = (value) => typeof value === 'string' && value.toLowerCase().startsWith(CONTENT_TYPE);

const isSealedStream = (value) =>
  typeof value === 'string' && value.toLowerCase().startsWith(STREAM_TYPE) && value.includes('wrpc-sealed=1');

// `u32 headerLength ‖ JSON ‖ body` — one JSON header, then bytes as they are.
const pack = (header, body) => {
  const json = utf8(JSON.stringify(header));
  const out = new Uint8Array(4 + json.length + body.length);
  new DataView(out.buffer).setUint32(0, json.length);
  out.set(json, 4);
  out.set(body, 4 + json.length);
  return out;
};

const unpack = (bytes) => {
  if (bytes.length < 4) throw new OpenError();
  const length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0);
  if (4 + length > bytes.length) throw new OpenError();
  let header;
  try {
    header = JSON.parse(decoder.decode(bytes.subarray(4, 4 + length)));
  } catch {
    throw new OpenError();
  }
  if (typeof header !== 'object' || header === null || Array.isArray(header)) throw new OpenError();
  return { header, body: bytes.subarray(4 + length) };
};

const bodyBytes = (body) => {
  if (body === undefined || body === null) return new Uint8Array(0);
  if (typeof body === 'string') return utf8(body);
  if (body instanceof Uint8Array) return body;
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  throw new TypeError('encryption: a sealed request body is a string or bytes');
};

// The response key: derived from a secret only the two ends of THIS request
// can export, salted with the request's enc and a fresh nonce — RFC 9458 §4.4.
const responseKey = async ({ kdf, cipher }, context, enc, nonce) => {
  const secret = await context.export(RESPONSE_LABEL, cipher.keyLength);
  const prk = await kdf.extract(concat(enc, nonce), secret);
  const [key, iv] = await Promise.all([
    kdf.expand(prk, KEY_LABEL, cipher.keyLength),
    kdf.expand(prk, NONCE_LABEL, cipher.nonceLength),
  ]);
  return { key: await cipher.key(key), iv };
};

// The key of a sealed event stream: the secret only the two ends of the
// request that opened THIS stream can export, salted with the request's enc
// and a nonce the SERVER draws per stream — carried as the `n` parameter of
// the stream's Content-Type — so every stream has its own key and counts
// its frames from zero. The salt is what makes a replayed open request (on
// another instance, whose replay memory is its own) a different key: the
// export alone is a function of the request, and two streams opened by the
// same bytes counted from zero under one key — a repeated (key, nonce).
const streamKey = async ({ kdf, cipher }, context, enc, nonce) => {
  const secret = await context.export(STREAM_LABEL, cipher.keyLength);
  const prk = await kdf.extract(concat(enc, nonce), secret);
  return cipher.key(await kdf.expand(prk, KEY_LABEL, cipher.keyLength));
};

const STREAM_NONCE_PARAM = /;\s*n=([A-Za-z0-9_-]+)/;

/** The Content-Type of a sealed stream under `nonce`. */
const sealedStreamType = (nonce) => `${SEALED_STREAM_TYPE}; n=${toBase64Url(nonce)}`;

/** The server nonce a sealed stream's Content-Type carries — exactly RESPONSE_NONCE bytes — or null. */
const streamNonce = (type) => {
  const match = typeof type === 'string' ? STREAM_NONCE_PARAM.exec(type) : null;
  const nonce = match === null ? null : fromBase64(match[1]);
  return nonce !== null && nonce.length === RESPONSE_NONCE ? nonce : null;
};

/**
 * A sealed event stream, opened: the outer body is `data: <base64>` events,
 * each the AEAD of one chunk of the REAL stream under a counter nonce; what
 * comes out is that real stream's bytes, for an SSE parser that never knew.
 * A frame that does not open — altered, dropped, reordered — errors the
 * stream, which the transport sees as a broken connection.
 */
const openSealedStream = (body, key) => {
  const reader = body.getReader();
  const text = new TextDecoder();
  let pending = '';
  let counter = 0;
  return new ReadableStream({
    async pull(controller) {
      for (;;) {
        const end = pending.indexOf('\n\n');
        if (end !== -1) {
          const event = pending.slice(0, end);
          pending = pending.slice(end + 2);
          // Anything but a data line is an intermediary's (a comment, a retry).
          if (!event.startsWith('data: ')) continue;
          const sealed = fromBase64(event.slice(6));
          if (sealed === null) throw new OpenError();
          return void controller.enqueue(await key.open(counterNonce(counter++), sealed, null));
        }
        const { value, done } = await reader.read();
        if (done) return void controller.close();
        pending += text.decode(value, { stream: true });
      }
    },
    cancel: (reason) => reader.cancel(reason),
  });
};

/**
 * The client half: `sealedFetch({ hpke, kdf, cipher, serverKey })`
 * answers `(fetch, endpoint) => fetch-shaped function`. The wrapper answers
 * a real `Response`, so a transport built on fetch needs no other change. A
 * response that is not a sealed one is an ERROR, whatever its status: a
 * client that encrypts does not read plaintext.
 */
const sealedFetch = ({ hpke, kdf, cipher, serverKey, now = Date.now }) => {
  const kid = utf8(serverKey.kid);
  const prefix = concat(Uint8Array.of(VERSION, hpke.aeadId, kid.length), kid);
  const info = concat(INFO, prefix);
  return (baseFetch, endpoint) => {
    const base = new URL(endpoint);
    return async (url, init = {}) => {
      const target = new URL(url, base);
      const header = {
        m: (init.method ?? 'GET').toUpperCase(),
        u: target.pathname + target.search,
        h: init.headers ?? {},
        t: now(),
      };
      const { enc, context } = await hpke.setupSender(serverKey.hpke, { info });
      const sealed = await context.seal(null, pack(header, bodyBytes(init.body)));
      const response = await baseFetch(base.href, {
        method: 'POST',
        headers: { 'Content-Type': CONTENT_TYPE },
        body: concat(prefix, enc, sealed),
        signal: init.signal,
        credentials: init.credentials,
      });
      const type = response.headers.get('content-type');
      if (isSealedStream(type)) {
        // A stream without its 32-byte nonce is not one this client opens.
        const nonce = streamNonce(type);
        if (nonce === null) throw new OpenError();
        const stream = openSealedStream(response.body, await streamKey({ kdf, cipher }, context, enc, nonce));
        return new Response(stream, { status: 200, headers: { 'Content-Type': STREAM_TYPE } });
      }
      if (!isSealedType(type)) {
        throw new Error(`encryption: the server answered in plaintext (${response.status})`);
      }
      const bytes = new Uint8Array(await response.arrayBuffer());
      if (bytes.length < RESPONSE_NONCE) throw new OpenError();
      const nonce = bytes.subarray(0, RESPONSE_NONCE);
      const { key, iv } = await responseKey({ kdf, cipher }, context, enc, nonce);
      const answer = unpack(await key.open(iv, bytes.subarray(RESPONSE_NONCE), null));
      const status = Number.isInteger(answer.header.s) ? answer.header.s : 0;
      if (status < 200 || status > 599) throw new OpenError();
      // A body is not allowed on these statuses by the Response constructor.
      const empty = status === 204 || status === 205 || status === 304;
      return new Response(empty ? null : answer.body, { status, headers: answer.header.h ?? {} });
    };
  };
};

module.exports = {
  sealedFetch,
  responseKey,
  streamKey,
  streamNonce,
  sealedStreamType,
  openSealedStream,
  isSealedStream,
  SEALED_STREAM_TYPE,
  STREAM_TYPE,
  pack,
  unpack,
  isSealedType,
  decoder,
  CONTENT_TYPE,
  VERSION,
  RESPONSE_NONCE,
  INFO,
};
