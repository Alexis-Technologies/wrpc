'use strict';

const { generateUUID } = require('../runtime/node.js');

const createProxy = (data, save) =>
  new Proxy(data, {
    get: (target, key) => {
      const value = Reflect.get(target, key);
      return value;
    },
    set: (target, key, value) => {
      const success = Reflect.set(target, key, value);
      if (save) save(target);
      return success;
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

const buildCookie = (name, value, options) => {
  const parts = [`${name}=${value}`, `Path=${options.path}`];
  if (options.maxAge !== null && options.maxAge !== undefined) {
    parts.push(`Max-Age=${options.maxAge}`);
  }
  if (options.httpOnly) parts.push('HttpOnly');
  if (options.secure) parts.push('Secure');
  if (options.sameSite) parts.push(`SameSite=${options.sameSite}`);
  return parts.join('; ');
};

// Per-server session facility (replaces the old module-global Map shared
// by every Server in the process).
class SessionManager {
  #console;

  constructor(options = {}, console = globalThis.console) {
    const { store = new MemorySessionStore(), generateToken = generateUUID, cookie = {} } = options;
    this.store = store;
    this.generateToken = generateToken;
    this.cookie = { ...DEFAULT_COOKIE, ...cookie };
    this.#console = console;
  }

  #saver(token) {
    return (state) => {
      Promise.resolve(this.store.set(token, state)).catch((error) => {
        this.#console.error(error);
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
