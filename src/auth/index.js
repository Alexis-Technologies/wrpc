'use strict';

// The @alexify/wrpc/auth subpath: ready-made token STORES for the client,
// ready-made token TRANSPORTS for the server, and bearerAuth() — the
// composition of the client's authenticate/refresh/headers options around a
// store. Deliberately outside the base browser bundle: the seams these plug
// into (client hooks, sessions.transport) live in the core, the strategies
// do not — the same reasoning that keeps the rooms backplane in ./scaling.
//
// Browser-safe by construction: no node builtins, and the ONLY require is
// src/wire.js — the import-free wire-name constants (esbuild inlines them,
// so the subpath stays ~1 KB) — one file serves both sides, like ./query.

const { META_PARAM, META_HEADER, META_PREFIX } = require('../wire.js');

// ---------------------------------------------------------------------------
// Client-side token stores. The contract is three functions, sync or async
// (IndexedDB is only reachable through async, so every caller awaits):
//   { get(key), set(key, value), delete(key) }
// A Map already satisfies it — which is what memoryStore returns.

const isTokenStore = (value) =>
  Boolean(value) &&
  typeof value.get === 'function' &&
  typeof value.set === 'function' &&
  typeof value.delete === 'function';

/** In-memory, per-tab: a Map IS the contract. */
const memoryStore = () => new Map();

/**
 * localStorage / sessionStorage (or anything getItem/setItem/removeItem
 * shaped), values JSON-encoded so token PAIRS store as one entry.
 */
const webStorage = (storage, { prefix = 'wrpc:' } = {}) => ({
  get: (key) => {
    const raw = storage.getItem(prefix + key);
    if (typeof raw !== 'string') return undefined;
    try {
      return JSON.parse(raw);
    } catch {
      return undefined;
    }
  },
  set: (key, value) => void storage.setItem(prefix + key, JSON.stringify(value)),
  delete: (key) => void storage.removeItem(prefix + key),
});

/**
 * A JS-readable cookie as the store (document.cookie). NOT the session
 * cookie the server sets — that one is HttpOnly and belongs to the cookie
 * token transport; this is for apps that want tokens to survive a reload
 * without localStorage. Values are percent-encoded JSON.
 */
const cookieStorage = (doc, { prefix = 'wrpc:', maxAge = 31536000, path = '/', secure = true } = {}) => ({
  get: (key) => {
    const name = `${encodeURIComponent(prefix + key)}=`;
    for (const part of String(doc.cookie ?? '').split('; ')) {
      if (!part.startsWith(name)) continue;
      try {
        return JSON.parse(decodeURIComponent(part.slice(name.length)));
      } catch {
        return undefined;
      }
    }
    return undefined;
  },
  // `Secure` by default, matching the server cookie's own default: this
  // store is script-readable by design (that is the XSS tradeoff the doc
  // comment above owns), but a token pair must never ALSO travel in the
  // clear. `secure: false` is for localhost development only. The delete
  // stamp carries the same attributes — a cookie only clears on a match.
  set: (key, value) => {
    const body = `${encodeURIComponent(prefix + key)}=${encodeURIComponent(JSON.stringify(value))}`;
    doc.cookie = `${body}; Path=${path}; Max-Age=${maxAge}; SameSite=Lax${secure ? '; Secure' : ''}`;
  },
  delete: (key) => {
    doc.cookie = `${encodeURIComponent(prefix + key)}=; Path=${path}; Max-Age=0${secure ? '; Secure' : ''}`;
  },
});

// ---------------------------------------------------------------------------
// bearerAuth: the client-side composition this subpath exists for. Wires a
// store, the app's signIn and (optionally) its refresh into the THREE client
// options that make bearer work end to end:
//   headers      presents `Authorization: Bearer <access>` on every open, so
//                the server's bearer transport restores the session before
//                any packet is dispatched (over ws it rides the wrpc_h
//                connect-URL parameter — see docs/guide/metadata).
//   authenticate awaited before the reconnect restore; signs in when the
//                store holds nothing.
//   refresh      single-flight, one-shot retry; the handler receives the
//                stored tokens, stores what it returns, clears on failure.
//
//   const client = await connect(url, bearerAuth({
//     store: webStorage(localStorage),
//     signIn: (client) => client.call('auth/signIn', credentials()),
//     refresh: (client, tokens) => client.call('auth/refresh', { token: tokens.refresh }),
//     on: [401, 403],
//   }));
//
// signIn and refresh return the token pair to store ({ access, ... }) or a
// falsy value to store nothing. Mid-socket renewal is the server handler's
// half: an auth/refresh procedure that calls context.client.startSession
// re-binds the LIVE connection while the stored pair covers the next one.
const bearerAuth = ({ store, signIn, refresh, key = 'tokens', on }) => {
  if (!isTokenStore(store)) {
    throw new TypeError('bearerAuth: store must provide get/set/delete (a Map qualifies)');
  }
  if (typeof signIn !== 'function') throw new TypeError('bearerAuth: signIn must be a function');
  const options = {
    headers: async () => {
      const tokens = await store.get(key);
      return tokens?.access ? { authorization: `Bearer ${tokens.access}` } : {};
    },
    authenticate: async (client, info) => {
      const tokens = await store.get(key);
      // A stored token already rode the connection headers; the upgrade
      // restore did the work. Nothing stored means nobody: sign in.
      if (tokens?.access) return;
      const fresh = await signIn(client, info);
      if (fresh) await store.set(key, fresh);
    },
  };
  if (typeof refresh === 'function') {
    options.refresh = {
      // wrpc's own "no session" refusal is 403; app handlers usually 401.
      on: Array.isArray(on) && on.length > 0 ? on : [401, 403],
      handler: async (client, error) => {
        const tokens = await store.get(key);
        let next;
        try {
          next = await refresh(client, tokens, error);
        } catch (refreshError) {
          // A THROWING refresh clears the store too: the pair it holds just
          // proved dead, and leaving it in place would make the next
          // reconnect's authenticate short-circuit on a corpse forever.
          await store.delete(key);
          throw refreshError;
        }
        if (next) await store.set(key, next);
        else await store.delete(key);
      },
    };
  }
  return options;
};

// ---------------------------------------------------------------------------
// Server-side token transports (the sessions.transport injection). Both are
// non-ambient: the credential is script-attached, so the safe-method CSRF
// rule does not apply and write() has nothing to stamp.

const MAX_DECLARED = 2048;
// ^ Mirrors the server's metaMaxBytes DEFAULT, for the raw fallbacks below
// only: every core call site hands read() the already-parsed `declared` /
// `meta` bags, capped on the CONFIGURABLE limit — the fallbacks serve
// callers with no parsed bag (the SSE channel key, custom hosts).

// The ws leg cannot send real upgrade headers, so a declared bag arrives in
// a connect-URL parameter; real headers win, as always. Raw-fallback only —
// see MAX_DECLARED above.
const declaredFromUrl = (url, param) => {
  const query = typeof url === 'string' ? url.slice(url.indexOf('?') + 1) : '';
  if (!query || query === url || query.length > MAX_DECLARED) return null;
  let raw = null;
  try {
    raw = new URLSearchParams(query).get(param);
  } catch {
    return null;
  }
  if (!raw) return null;
  try {
    const value = JSON.parse(raw);
    return typeof value === 'object' && value !== null && !Array.isArray(value) ? value : null;
  } catch {
    return null;
  }
};

// The ws subprotocol carrier. The browser WebSocket constructor cannot set
// an Authorization header, and the connect URL lands in proxy access logs —
// but RFC 6455 lets the client OFFER subprotocols, which travel as a real
// upgrade header. The ws transport offers `wrpc.bearer.<token>` next to the
// wire revision; this is where the server reads it back.
const SUBPROTOCOL_PREFIX = 'wrpc.bearer.';

/**
 * Session token in `Authorization: <scheme> <token>` — the observed header
 * on http/sse, the `wrpc.bearer.<token>` subprotocol offer on ws (kept out
 * of the connect URL and its access logs), or the declared bag the core
 * parsed from wrpc_h.
 */
const bearerTransport = ({ scheme = 'Bearer' } = {}) => {
  const marker = `${scheme} `;
  const fromValue = (value) =>
    typeof value === 'string' && value.startsWith(marker) ? value.slice(marker.length) : null;
  return {
    ambient: false,
    read: ({ headers, declared }) => {
      const observed = fromValue(headers?.authorization);
      if (observed) return observed;
      const offered = headers?.['sec-websocket-protocol'];
      if (typeof offered === 'string' && offered.length > 0) {
        for (const part of offered.split(',')) {
          const name = part.trim();
          if (name.startsWith(SUBPROTOCOL_PREFIX)) return name.slice(SUBPROTOCOL_PREFIX.length);
        }
      }
      // The bag the core already parsed and capped (wrpc_h included) — this
      // file never re-derives it, so it cannot drift from the core parser.
      return fromValue(declared?.authorization);
    },
    write: () => null,
  };
};

// The core kebab-normalizes meta keys; the simple camel split below matches
// utils.toKebab for ordinary field names ('authToken' -> 'auth-token').
// Give `field` in kebab spelling outright if its casing is exotic.
const kebabField = (field) => field.replace(/([a-z0-9])([A-Z])/g, '$1-$2').toLowerCase();

/**
 * Session token as a field of the declared connection metadata (the client's
 * `meta` option / the x-wrpc-meta header, either spelling): `meta: { token }`.
 */
const payloadTransport = ({ field = 'token' } = {}) => {
  const name = kebabField(field);
  return {
    ambient: false,
    read: ({ headers, url, meta }) => {
      // The bag the core already parsed: both header spellings and the ws
      // query merged, kebab-keyed, capped on the configurable metaMaxBytes.
      const parsed = meta?.[name] ?? meta?.[field];
      if (typeof parsed === 'string' && parsed.length > 0) return parsed;
      // Raw fallbacks, for callers with no parsed bag: the per-key header,
      // the canonical JSON header, the ws connect-URL parameter.
      const prefixed = headers?.[META_PREFIX + name];
      if (typeof prefixed === 'string' && prefixed.length > 0) return prefixed;
      const header = headers?.[META_HEADER];
      let declared = null;
      if (typeof header === 'string' && header.length > 0 && header.length <= MAX_DECLARED) {
        try {
          declared = JSON.parse(decodeURIComponent(header));
        } catch {
          declared = null;
        }
        if (typeof declared !== 'object' || Array.isArray(declared)) declared = null;
      }
      declared ??= declaredFromUrl(url, META_PARAM);
      const token = declared?.[field] ?? declared?.[name];
      return typeof token === 'string' && token.length > 0 ? token : null;
    },
    write: () => null,
  };
};

module.exports = {
  isTokenStore,
  memoryStore,
  webStorage,
  cookieStorage,
  bearerAuth,
  bearerTransport,
  payloadTransport,
};
