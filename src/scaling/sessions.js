'use strict';

// A Redis session store, modelled on the ioredis API — `get(key)`,
// `set(key, value, 'PX', ttl)`, `del(key)` and `pexpire(key, ttl)` — for
// the `sessions: { store }` injection. The backplane carries room events
// between instances, not sessions: without a shared store a client that
// reconnects to another instance arrives anonymous, and the load balancer
// has to pin it (docs/guide/scaling.md). With this one, no instance owns a
// session and nothing needs pinning.
//
// Per the zero-dependency rule nothing is required here: the caller injects
// its own client and it is validated structurally, so anything with that
// shape plugs in.
//
//   const { createRedisSessionStore } = require('@alexify/wrpc/scaling');
//   const Redis = require('ioredis');
//   new Server({ router, sessions: { store: createRedisSessionStore({ client: new Redis(url) }) } });
//
// node-redis v4 spells the expiring set differently — `set(key, value,
// { PX: ttl })` — so it needs a two-line wrapper:
//   const client = { get: (k) => redis.get(k), del: (k) => redis.del(k),
//     set: (k, v, _px, ttl) => redis.set(k, v, { PX: ttl }), pexpire: (k, ttl) => redis.pExpire(k, ttl) };
//
// State is stored as JSON: a session's state is the plain data the
// application assigns, which is JSON by construction (it crosses the
// Session proxy as own enumerable properties).

const DEFAULT_PREFIX = 'wrpc:session:';
const DEFAULT_TTL = 24 * 60 * 60 * 1000; // 24h, like MemorySessionStore

const isFunction = (value) => typeof value === 'function';

const checkClient = (client) => {
  if (!client || !isFunction(client.get) || !isFunction(client.set) || !isFunction(client.del)) {
    throw new TypeError(
      'createRedisSessionStore: options.client must be a Redis client with get(key), set(key, value, ...), del(key)',
    );
  }
  return client;
};

const createRedisSessionStore = (options = {}) => {
  const { client, prefix = DEFAULT_PREFIX, ttl = DEFAULT_TTL } = options;
  checkClient(client);
  if (typeof prefix !== 'string') throw new TypeError('createRedisSessionStore: options.prefix must be a string');
  if (!(Number.isFinite(ttl) && ttl >= 0)) throw new TypeError('createRedisSessionStore: options.ttl must be >= 0 ms');
  const key = (token) => prefix + token;
  // Sliding expiry needs PEXPIRE; a client without it keeps absolute TTLs,
  // which SessionManager treats as a valid policy (no touch()).
  const sliding = ttl > 0 && isFunction(client.pexpire);

  const store = {
    name: 'redis',
    async get(token) {
      const raw = await client.get(key(token));
      if (raw === null || raw === undefined) return null;
      try {
        const data = JSON.parse(raw);
        return typeof data === 'object' && data !== null && !Array.isArray(data) ? data : null;
      } catch {
        // A corrupt entry is a missing session, not a thrown request.
        return null;
      }
    },
    async set(token, data) {
      const value = JSON.stringify(data);
      if (ttl > 0) await client.set(key(token), value, 'PX', ttl);
      else await client.set(key(token), value);
    },
    async delete(token) {
      await client.del(key(token));
    },
  };
  if (sliding) {
    store.touch = async (token) => {
      await client.pexpire(key(token), ttl);
    };
  }
  return store;
};

module.exports = { createRedisSessionStore, DEFAULT_SESSION_PREFIX: DEFAULT_PREFIX };
