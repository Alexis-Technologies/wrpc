'use strict';

// A session store that holds nothing readable. `sessions: { store }` takes
// any `{ get, set, delete, touch? }`; this wraps one so that what rests in
// it — Redis, a table, a dump of either — is neither the session's state
// nor its token:
//
//   key   = base64url( HMAC-SHA256( index key, token ) )
//   value = { v: 1, kid, s: base64( sealed JSON ) }
//
// The token is a bearer credential and today it IS the Redis key, so a
// keyspace listing is a list of live sessions. An HMAC of it finds the row
// without resting the credential; the state is sealed with the same frame
// the backplane envelopes use (envelope.js — a key per writing process, a
// counter nonce), with the ROW'S KEY as additional data, so a row copied
// into another session's slot does not open there.
//
// Rotation must not sign everybody out, and the index key rotates with the
// rest: a read misses under the current kid, finds the row under an older
// one, and moves it — so a kid can be dropped once the longest session TTL
// has passed since it stopped being current. `acceptPlaintext` is the same
// idea for the first deploy: a row the unwrapped store wrote is read once,
// sealed, and its plaintext deleted.

const crypto = require('node:crypto');
const { normalizeEnvelopeEncryption, createEnvelopeSealer } = require('./envelope.js');
const { createLoggerWriter } = require('../logging.js');

const isFunction = (value) => typeof value === 'function';

const isRecord = (value) =>
  typeof value === 'object' &&
  value !== null &&
  value.v === 1 &&
  typeof value.kid === 'string' &&
  typeof value.s === 'string';

const sealedStore = (store, options = {}) => {
  if (!store || !isFunction(store.get) || !isFunction(store.set) || !isFunction(store.delete)) {
    throw new TypeError('sealedStore: store must be a session store with get(token), set(token, data), delete(token)');
  }
  const { acceptPlaintext = false, logger = globalThis.console } = options;
  if (typeof acceptPlaintext !== 'boolean') {
    throw new TypeError('sealedStore: options.acceptPlaintext must be a boolean');
  }
  const encryption = normalizeEnvelopeEncryption(
    { keys: options.keys, cipher: options.cipher, replayWindow: false },
    'sealedStore: options',
  );
  const { keys } = encryption;
  const sealer = createEnvelopeSealer({ encryption, layer: 'store', echo: true });
  const log = createLoggerWriter(logger).child({ component: 'sessions', store: 'sealed' });

  // kid → the key tokens are indexed under, derived once.
  const indexKeys = new Map();
  const slot = (kid, token) => {
    let indexKey = indexKeys.get(kid);
    if (indexKey === undefined) {
      const master = keys.get(kid);
      if (master === null) return null;
      indexKey = Buffer.from(crypto.hkdfSync('sha256', master, Buffer.alloc(0), `wrpc store-index v1\0${kid}`, 32));
      indexKeys.set(kid, indexKey);
    }
    return crypto.createHmac('sha256', indexKey).update(String(token)).digest('base64url');
  };

  const write = (token, data) => {
    const kid = keys.current;
    const key = slot(kid, token);
    if (key === null) throw new Error(`sealedStore: the keyring does not hold its current key ${JSON.stringify(kid)}`);
    const { sealed } = sealer.seal(Buffer.from(JSON.stringify(data)), key);
    return store.set(key, { v: 1, kid, s: sealed.toString('base64') });
  };

  // The state of one row, or null — a row that does not open is a missing
  // session and one log line, never a thrown request. The token is not
  // logged: it is the credential.
  const read = (kid, key, row) => {
    if (!isRecord(row) || row.kid !== kid) return void log.warn({ event: 'session.unsealed', kid });
    try {
      const data = JSON.parse(sealer.open(kid, Buffer.from(row.s, 'base64'), key).toString());
      if (typeof data === 'object' && data !== null && !Array.isArray(data)) return data;
    } catch (error) {
      return void log.warn({ event: 'session.open', kid, reason: error.reason ?? 'format' });
    }
    return void log.warn({ event: 'session.open', kid, reason: 'format' });
  };

  // Moved under the current key, best effort: the old row is still good for
  // as long as its kid is, so a failure here costs a retry at the next read.
  const migrate = async (token, data, stale) => {
    try {
      await write(token, data);
      await store.delete(stale);
    } catch (error) {
      log.warn({ err: error, event: 'session.migrate' });
    }
  };

  const sealedKeys = (token) => {
    const current = keys.current;
    const found = [[current, slot(current, token)]];
    const kids = keys.kids;
    for (let i = 0; i < kids.length; i++) {
      if (kids[i] !== current) found.push([kids[i], slot(kids[i], token)]);
    }
    return found;
  };

  const wrapped = {
    name: `sealed(${store.name ?? 'store'})`,
    async get(token) {
      const slots = sealedKeys(token);
      for (let i = 0; i < slots.length; i++) {
        const [kid, key] = slots[i];
        if (key === null) continue;
        const row = await store.get(key);
        if (row === null || row === undefined) continue;
        const data = read(kid, key, row);
        if (data === undefined) return null;
        if (i > 0) await migrate(token, data, key);
        return data;
      }
      if (!acceptPlaintext) return null;
      const plain = await store.get(token);
      if (plain === null || plain === undefined) return null;
      await migrate(token, plain, token);
      return plain;
    },
    set: (token, data) => write(token, data),
    async delete(token) {
      const slots = sealedKeys(token);
      for (let i = 0; i < slots.length; i++) if (slots[i][1] !== null) await store.delete(slots[i][1]);
      if (acceptPlaintext) await store.delete(token);
    },
  };
  if (isFunction(store.touch)) {
    wrapped.touch = async (token) => {
      const key = slot(keys.current, token);
      if (key !== null) await store.touch(key);
    };
  }
  return wrapped;
};

module.exports = { sealedStore };
