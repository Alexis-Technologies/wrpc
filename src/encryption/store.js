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
const { isKeyProvider } = require('./contracts.js');
const { createLoggerWriter } = require('../logging.js');

const isFunction = (value) => typeof value === 'function';

const isRecord = (value) =>
  typeof value === 'object' &&
  value !== null &&
  value.v === 1 &&
  typeof value.kid === 'string' &&
  typeof value.s === 'string';

// What a session's state is: an object, never an array or a scalar — and
// never a sealed record, which is a row of this store under the wrong
// name, not a session.
const isState = (value) => typeof value === 'object' && value !== null && !Array.isArray(value) && !isRecord(value);

const sealedStore = (store, options = {}) => {
  if (!store || !isFunction(store.get) || !isFunction(store.set) || !isFunction(store.delete)) {
    throw new TypeError('sealedStore: store must be a session store with get(token), set(token, data), delete(token)');
  }
  const { acceptPlaintext = false, seal = true, logger = globalThis.console } = options;
  if (typeof acceptPlaintext !== 'boolean') {
    throw new TypeError('sealedStore: options.acceptPlaintext must be a boolean');
  }
  if (typeof seal !== 'boolean') throw new TypeError('sealedStore: options.seal must be a boolean');
  // A rotation is walked through kids(): a provider without it would keep
  // every session under an older kid unreadable — a mass logout the
  // moment `current` moved. Refused here, not discovered at the rotation.
  if (isKeyProvider(options.keys) && typeof options.keys.kids !== 'function') {
    throw new TypeError('sealedStore: a key provider must implement kids() — the kids a rotation is walked through');
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

  const sealedKeys = (token) => {
    const current = keys.current;
    const found = [[current, slot(current, token)]];
    const kids = keys.kids;
    for (let i = 0; i < kids.length; i++) {
      if (kids[i] !== current) found.push([kids[i], slot(kids[i], token)]);
    }
    return found;
  };

  // Under the current kid; with `prune`, the token's slots under every
  // other kid go too — one row per token, so a fleet mid-rotation (one
  // instance writing under k1, another under k2) converges on the latest
  // write instead of each reading its own kid's stale row.
  const write = async (token, data, prune) => {
    // seal: false — the first of the three deploys (sessions guide): every
    // instance reads sealed rows already, none writes them yet, so a
    // rollback finds every session where it always was. The token's sealed
    // slots go, best effort: one row per token.
    if (!seal) {
      await store.set(token, data);
      const slots = sealedKeys(token);
      for (let i = 0; i < slots.length; i++) {
        if (slots[i][1] === null) continue;
        try {
          await store.delete(slots[i][1]);
        } catch (error) {
          log.warn({ err: error, event: 'session.migrate' });
        }
      }
      return;
    }
    const kid = keys.current;
    const key = slot(kid, token);
    if (key === null) throw new Error(`sealedStore: the keyring does not hold its current key ${JSON.stringify(kid)}`);
    const { sealed } = sealer.seal(Buffer.from(JSON.stringify(data)), key);
    await store.set(key, { v: 1, kid, s: sealed.toString('base64') });
    if (!prune) return;
    const kids = keys.kids;
    if (kids.length < 2) return;
    for (let i = 0; i < kids.length; i++) {
      if (kids[i] === kid) continue;
      const other = slot(kids[i], token);
      if (other === null) continue;
      try {
        await store.delete(other);
      } catch (error) {
        log.warn({ err: error, event: 'session.migrate' });
      }
    }
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
  // The stale row is deleted only if it is still the row that was read:
  // another instance may have written a newer state there meanwhile, in
  // which case THAT is the state to move — once; a second change is the
  // next read's.
  const migrate = async (token, data, stale, kid, seen) => {
    try {
      await write(token, data, false);
      const row = await store.get(stale);
      if (row === null || row === undefined) return;
      if (seen !== null && isRecord(row) && row.s !== seen) {
        // Written since it was read — an instance still on the old kid:
        // that state is the one to move. Once; a second change is the next
        // read's, and the stale row stays for it.
        const fresh = read(kid, stale, row);
        if (fresh === undefined) return;
        await write(token, fresh, false);
        const again = await store.get(stale);
        if (!isRecord(again) || again.s !== row.s) return;
      }
      await store.delete(stale);
    } catch (error) {
      log.warn({ err: error, event: 'session.migrate' });
    }
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
        // Not sealing yet: a sealed row is read where it is, never moved.
        if (i > 0 && seal) await migrate(token, data, key, kid, row.s);
        return data;
      }
      if (!acceptPlaintext && seal) return null;
      const plain = await store.get(token);
      if (plain === null || plain === undefined) return null;
      // A row under the raw token that is not a session: a sealed record
      // moved there — the row's name is a credential while acceptPlaintext
      // is on, and this is what someone holding one would try — or some
      // other value. Neither is adopted, migrated or deleted.
      if (!isState(plain)) {
        log.warn({ event: 'session.unsealed', kid: null });
        return null;
      }
      if (seal) await migrate(token, plain, token, null, null);
      return plain;
    },
    // Async on purpose: a keyring without its current key throws in write()
    // before the store is reached, and a SessionStore's set() answers a
    // rejection, never a throw — the session manager's flush counts on it.
    set: async (token, data) => write(token, data, true),
    async delete(token) {
      const slots = sealedKeys(token);
      for (let i = 0; i < slots.length; i++) if (slots[i][1] !== null) await store.delete(slots[i][1]);
      if (acceptPlaintext || !seal) await store.delete(token);
    },
  };
  if (isFunction(store.touch)) {
    wrapped.touch = async (token) => {
      const key = slot(keys.current, token);
      if (key !== null) await store.touch(key);
      // The row may still be plaintext, on either side of the rollout.
      if (acceptPlaintext || !seal) await store.touch(token);
    };
  }
  return wrapped;
};

module.exports = { sealedStore };
