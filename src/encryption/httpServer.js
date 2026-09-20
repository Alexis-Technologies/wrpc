'use strict';

// The server half of per-request encryption (http.js has the format and the
// client half). Node-only, reached from the core: `unwrap` turns a sealed
// outer request into the real one — marked `encrypted`, its `respond`
// sealing the answer — and the core carries on as if it had arrived in the
// clear, so packets, batches and REST all ride it without knowing.
//
// HPKE has no replay protection, and the party this exists for — whoever
// terminates the TLS — is exactly the one who could replay: so `t` (the
// sender's clock) must be fresh, and an `enc` is accepted once.

const { concat, toBase64Url, counterNonce } = require('./bytes.js');
const {
  responseKey,
  streamKey,
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
} = require('./http.js');

const DEFAULT_MAX_SKEW = 5 * 60 * 1000;
const DEFAULT_REPLAY_ENTRIES = 100000;

/**
 * A bounded "seen once" memory: `seen(id, ttl)` answers true for an id it
 * was shown within `ttl` ms. In memory and per process — behind a balancer,
 * inject a shared one (`SET id 1 NX PX ttl` in Redis) with the same method.
 */
const createReplayCache = ({ max = DEFAULT_REPLAY_ENTRIES, now = Date.now } = {}) => {
  const entries = new Map();
  return {
    seen(id, ttl) {
      const time = now();
      // Insertion order is time order, so the expired are at the front.
      for (const [key, expires] of entries) {
        if (expires > time && entries.size < max) break;
        entries.delete(key);
      }
      if (entries.has(id)) return true;
      entries.set(id, time + ttl);
      return false;
    },
    get size() {
      return entries.size;
    },
  };
};

/**
 * The server half. `unwrap(call, outerHeaders)` answers the inner call — the
 * real request, marked `encrypted`, whose `respond` seals — or null when it
 * answered the outer one itself (an error: never sealed, never detailed).
 * `refuse(call, outerHeaders, status, reason)` is how: one log line with the
 * reason, a bare status on the wire.
 */
const createHttpSealing = ({
  suites,
  statics,
  kdf,
  maxSkew = DEFAULT_MAX_SKEW,
  replay,
  random,
  log,
  refuse,
  now = Date.now,
}) => {
  const cache = replay ?? createReplayCache({ now });

  const unwrap = async (call, outerHeaders) => {
    const raw = call.body;
    if (!(raw instanceof Uint8Array) || raw.length < 4 || raw[0] !== VERSION) {
      return void refuse(call, outerHeaders, 400, 'format');
    }
    const suite = suites.get(raw[1]);
    const kidEnd = 3 + raw[2];
    if (suite === undefined || kidEnd + suite.hpke.encLength >= raw.length) {
      return void refuse(call, outerHeaders, 400, 'format');
    }
    let kid;
    try {
      kid = decoder.decode(raw.subarray(3, kidEnd));
    } catch {
      return void refuse(call, outerHeaders, 400, 'format');
    }
    // The kid is public, so saying it is unknown tells nobody anything —
    // and tells a client with a stale pin what to do about it.
    const pending = statics(kid);
    if (pending === null) return void refuse(call, outerHeaders, 400, 'kid');
    const recipient = (await pending).hpke;
    const prefix = raw.subarray(0, kidEnd);
    const enc = Uint8Array.from(raw.subarray(kidEnd, kidEnd + suite.hpke.encLength));
    let context;
    let request;
    try {
      context = await suite.hpke.setupRecipient(enc, recipient, { info: concat(INFO, prefix) });
      request = unpack(await context.open(null, raw.subarray(kidEnd + suite.hpke.encLength)));
    } catch {
      return void refuse(call, outerHeaders, 400, 'open');
    }
    const { m: method, u: url, h: declared, t: sent } = request.header;
    if (typeof method !== 'string' || typeof url !== 'string' || !url.startsWith('/') || !Number.isFinite(sent)) {
      return void refuse(call, outerHeaders, 400, 'format');
    }
    // Fresh, and once: what HPKE itself does not promise.
    if (Math.abs(now() - sent) > maxSkew) return void refuse(call, outerHeaders, 409, 'stale');
    const replayed = await cache.seen(toBase64Url(enc), 2 * maxSkew);
    if (replayed === true) return void refuse(call, outerHeaders, 409, 'replay');

    // The outer request's own headers stay underneath — a cookie, the
    // origin, what a proxy added — and what the client declared inside wins.
    const headers = { ...call.headers };
    delete headers['content-type'];
    delete headers['content-length'];
    // What the outer request accepts is not what the inner answer may be:
    // the client reads the answer's bytes as they are, and compressing what
    // is about to be sealed is the outer layer's business, never the inner's.
    delete headers['accept-encoding'];
    if (typeof declared === 'object' && declared !== null) {
      for (const name of Object.keys(declared)) {
        if (typeof declared[name] === 'string') headers[name.toLowerCase()] = declared[name];
      }
    }

    const respond = ({ status, headers: inner = {}, body }) => {
      // Set-Cookie has to be the outer response's: script cannot set an
      // HttpOnly cookie, and that is the only kind a session should be.
      const outer = { ...outerHeaders, 'Content-Type': CONTENT_TYPE, 'Cache-Control': 'no-store' };
      const sealedHeaders = {};
      for (const name of Object.keys(inner)) {
        if (name.toLowerCase() === 'set-cookie') outer[name] = inner[name];
        else sealedHeaders[name] = Array.isArray(inner[name]) ? inner[name].join(', ') : String(inner[name]);
      }
      const nonce = random(RESPONSE_NONCE);
      responseKey({ kdf, cipher: suite.cipher }, context, enc, nonce)
        .then(async ({ key, iv }) => {
          const sealed = await key.seal(iv, pack({ s: status, h: sealedHeaders }, body ?? new Uint8Array(0)), null);
          const bytes = Buffer.concat([nonce, sealed]);
          call.respond({ status: 200, headers: { ...outer, 'Content-Length': bytes.length }, body: bytes });
        })
        .catch((error) => {
          log.error({ err: error, event: 'encryption.respond' });
          call.respond({ status: 500, headers: outerHeaders });
        });
    };

    const inner = {
      method: method.toUpperCase(),
      url,
      headers,
      body:
        request.body.length === 0
          ? null
          : Buffer.from(request.body.buffer, request.body.byteOffset, request.body.length),
      remoteAddress: call.remoteAddress,
      encrypted: true,
      respond,
    };
    if (typeof call.onAbort === 'function') inner.onAbort = (listener) => call.onAbort(listener);
    // An event stream (SSE): the key is exported now, because the core opens
    // the stream synchronously — and only when the request asked for one.
    if (typeof call.stream === 'function' && String(headers.accept ?? '').includes(STREAM_TYPE)) {
      const key = await streamKey(suite.cipher, context);
      inner.stream = ({ headers: streamHeaders = {} }) => {
        const outer = { ...outerHeaders };
        for (const name of Object.keys(streamHeaders)) {
          const lower = name.toLowerCase();
          if (lower !== 'content-type' && lower !== 'content-encoding') outer[name] = streamHeaders[name];
        }
        const writer = call.stream({ status: 200, headers: { ...outer, 'Content-Type': SEALED_STREAM_TYPE } });
        if (!writer) return writer;
        let counter = 0;
        // Every chunk of the real stream — an event, the `ready` frame that
        // hands out the channel id, a heartbeat comment — leaves as one
        // opaque `data:` event under the next counter.
        const sealedWriter = {
          write: (chunk) => {
            const sealed = key.seal(counterNonce(counter++), Buffer.from(chunk), null);
            return writer.write(`data: ${sealed.toString('base64')}\n\n`);
          },
          end: () => writer.end(),
        };
        if (typeof writer.onClose === 'function') sealedWriter.onClose = (listener) => writer.onClose(listener);
        if (typeof writer.onDrain === 'function') sealedWriter.onDrain = (listener) => writer.onDrain(listener);
        return sealedWriter;
      };
    }
    return inner;
  };

  return { unwrap, isSealed: (call) => isSealedType(call.headers?.['content-type']) };
};

module.exports = { createHttpSealing, createReplayCache, DEFAULT_MAX_SKEW };
