'use strict';

const { generateUUID } = require('../runtime/node.js');
const { createLoggerWriter } = require('../logging.js');

const createProxy = (data, save) =>
  new Proxy(data, {
    get: (target, key) => {
      const value = Reflect.get(target, key);
      return value;
    },
    set: (target, key, value) => {
      // defineProperty rather than assignment: session state can carry keys
      // that came off the wire, and a plain `state['__proto__'] = x` would
      // swap the state object's prototype instead of storing a value — the
      // same rule router.js applies to unit and method names (assignKey).
      Object.defineProperty(target, key, { value, enumerable: true, writable: true, configurable: true });
      if (save) save(target);
      return true;
    },
  });

class Session {
  constructor(token, data, save) {
    this.token = token;
    this.state = createProxy(data, save);
  }
}

const DEFAULT_MAX_SESSIONS = 10000;
const DEFAULT_SESSION_TTL = 24 * 60 * 60 * 1000; // 24h

// Reference implementation of the structural SessionStore contract:
// { get(token) -> data | null, set(token, data), delete(token) } — all async.
// Anything matching the shape (a Redis wrapper, a DB table) plugs in via
// the `sessions.store` option, per the zero-dependency injection rule.
//
// Sessions outlive their connection (that is what makes reconnect work),
// so this store is bounded on both axes: entries expire after `ttl` and
// the least-recently-used one is evicted past `maxSessions`. Production
// deployments should inject a real store instead.
class MemorySessionStore {
  #sessions = new Map(); // token -> { data, expires }
  #maxSessions;
  #ttl;

  constructor({ maxSessions = DEFAULT_MAX_SESSIONS, ttl = DEFAULT_SESSION_TTL, now = Date.now } = {}) {
    this.#maxSessions = maxSessions;
    this.#ttl = ttl;
    this.now = now;
  }

  get size() {
    return this.#sessions.size;
  }

  #expired(entry) {
    return this.#ttl > 0 && entry.expires <= this.now();
  }

  async get(token) {
    const entry = this.#sessions.get(token);
    if (!entry) return null;
    if (this.#expired(entry)) {
      this.#sessions.delete(token);
      return null;
    }
    // Map keeps insertion order: re-insert to mark as recently used
    this.#sessions.delete(token);
    this.#sessions.set(token, entry);
    return entry.data;
  }

  async set(token, data) {
    this.#sessions.delete(token);
    this.#sessions.set(token, { data, expires: this.now() + this.#ttl });
    this.#evict();
  }

  async delete(token) {
    this.#sessions.delete(token);
  }

  /** Sliding expiry: a session in active use should not expire mid-use. */
  async touch(token) {
    const entry = this.#sessions.get(token);
    if (!entry || this.#expired(entry)) return;
    entry.expires = this.now() + this.#ttl;
  }

  #evict() {
    if (this.#ttl > 0) {
      for (const [token, entry] of this.#sessions) {
        if (!this.#expired(entry)) break; // oldest first: stop at the first live one
        this.#sessions.delete(token);
      }
    }
    while (this.#maxSessions > 0 && this.#sessions.size > this.#maxSessions) {
      const oldest = this.#sessions.keys().next().value;
      this.#sessions.delete(oldest);
    }
  }
}

const DEFAULT_COOKIE = {
  name: 'token',
  path: '/',
  httpOnly: true,
  secure: true,
  sameSite: 'Lax',
  maxAge: null, // session cookie by default
};

// RFC 6265 grammar, enforced because the token can come from an injected
// `generateToken` and the attributes from user config: a `;` or a control
// character in either would let a value smuggle extra cookie attributes (or
// a second cookie) into the Set-Cookie line.
const COOKIE_NAME = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/; // RFC 2616 token
// cookie-octet: %x21 / %x23-2B / %x2D-3A / %x3C-5B / %x5D-7E
const COOKIE_VALUE = /^[\x21\x23-\x2B\x2D-\x3A\x3C-\x5B\x5D-\x7E]*$/;
// path-value: printable ASCII minus ';' (0x3B) — spelled as positive ranges
// so no control character ever matches.
const COOKIE_PATH = /^[\x20-\x3A\x3C-\x7E]*$/;
const SAME_SITE = new Set(['Strict', 'Lax', 'None']);

const buildCookie = (name, value, options) => {
  if (!COOKIE_NAME.test(name)) {
    throw new TypeError(`buildCookie: invalid cookie name`);
  }
  if (typeof value !== 'string' || !COOKIE_VALUE.test(value)) {
    throw new TypeError('buildCookie: the value contains characters RFC 6265 forbids in a cookie');
  }
  if (typeof options.path !== 'string' || !COOKIE_PATH.test(options.path)) {
    throw new TypeError('buildCookie: invalid Path attribute');
  }
  const parts = [`${name}=${value}`, `Path=${options.path}`];
  if (options.maxAge !== null && options.maxAge !== undefined) {
    if (!Number.isSafeInteger(options.maxAge)) throw new TypeError('buildCookie: maxAge must be an integer');
    parts.push(`Max-Age=${options.maxAge}`);
  }
  if (options.httpOnly) parts.push('HttpOnly');
  if (options.secure) parts.push('Secure');
  if (options.sameSite) {
    if (!SAME_SITE.has(options.sameSite)) {
      throw new TypeError(`buildCookie: sameSite must be one of ${[...SAME_SITE].join(', ')}`);
    }
    parts.push(`SameSite=${options.sameSite}`);
  }
  return parts.join('; ');
};

// Per-server session facility (replaces the old module-global Map shared
// by every Server in the process).
class SessionManager {
  #log;

  constructor(options = {}, logger = globalThis.console) {
    const { store = new MemorySessionStore(), generateToken = generateUUID, cookie = {} } = options;
    this.store = store;
    this.generateToken = generateToken;
    this.cookie = { ...DEFAULT_COOKIE, ...cookie };
    this.#log = createLoggerWriter(logger);
  }

  #saver(token) {
    return (state) => {
      Promise.resolve(this.store.set(token, state)).catch((error) => {
        this.#log.error({ err: error, event: 'session.save' });
      });
    };
  }

  create(token = this.generateToken(), data = {}) {
    const save = this.#saver(token);
    save(data); // persist the initial state so restore works immediately
    return new Session(token, data, save);
  }

  async restore(token) {
    const data = await this.store.get(token);
    if (!data) return null;
    // Sliding expiry, when the store supports it: restoring IS active use,
    // and a shared-store session must not expire under a connected client.
    // Optional and fire-and-forget — a store without touch() keeps absolute
    // TTLs, which is a valid policy too.
    if (typeof this.store.touch === 'function') {
      Promise.resolve(this.store.touch(token)).catch((error) => {
        this.#log.error({ err: error, event: 'session.touch' });
      });
    }
    return new Session(token, data, this.#saver(token));
  }

  async destroy(token) {
    await this.store.delete(token);
  }

  cookieHeader(token) {
    return buildCookie(this.cookie.name, token, this.cookie);
  }

  cookieDeleteHeader() {
    return buildCookie(this.cookie.name, 'deleted', { ...this.cookie, maxAge: 0 });
  }

  readToken(cookies) {
    return cookies[this.cookie.name] ?? null;
  }
}

module.exports = { Session, MemorySessionStore, SessionManager, createProxy, buildCookie };
