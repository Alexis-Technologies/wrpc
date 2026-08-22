'use strict';

// The @alexify/wrpc/auth subpath: ready-made token STORES for the client,
// ready-made token TRANSPORTS for the server, and bearerAuth() — the
// composition of the client's authenticate/refresh/headers options around a
// store. Deliberately outside the base browser bundle: the seams these plug
// into (client hooks, sessions.transport) live in the core, the strategies
// do not — the same reasoning that keeps the rooms backplane in ./scaling.
//
// Browser-safe by construction: no node builtins, no requires — one file
// serves both sides, like ./query.

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
const cookieStorage = (doc, { prefix = 'wrpc:', maxAge = 31536000, path = '/' } = {}) => ({
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
  set: (key, value) => {
    const body = `${encodeURIComponent(prefix + key)}=${encodeURIComponent(JSON.stringify(value))}`;
    doc.cookie = `${body}; Path=${path}; Max-Age=${maxAge}; SameSite=Lax`;
  },
  delete: (key) => {
    doc.cookie = `${encodeURIComponent(prefix + key)}=; Path=${path}; Max-Age=0`;
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
        const next = await refresh(client, tokens, error);
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

// The ws leg cannot send real upgrade headers, so a declared authorization
// arrives in the wrpc_h connect-URL parameter; real headers win, as always.
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

/** Session token in `Authorization: <scheme> <token>` (header or ws query). */
const bearerTransport = ({ scheme = 'Bearer' } = {}) => {
  const marker = `${scheme} `;
  const fromValue = (value) =>
    typeof value === 'string' && value.startsWith(marker) ? value.slice(marker.length) : null;
  return {
    ambient: false,
    read: ({ headers, url }) => {
      const observed = fromValue(headers?.authorization);
      if (observed) return observed;
      const declared = declaredFromUrl(url, 'wrpc_h');
      for (const key in declared) {
        // The core's own wrpc_h parser normalizes names with toKebab, this one
        // only lowercases — and the two cannot disagree here, because the name
        // being matched has no lowercase-to-uppercase boundary for toKebab to
        // split. Kept spelled this way on purpose: this file requires NOTHING
        // (that is what keeps the subpath ~1 KB and browser-safe), so it must
        // not import a helper to reproduce a result it already computes.
        if (key.toLowerCase() === 'authorization') return fromValue(declared[key]);
      }
      return null;
    },
    write: () => null,
  };
};

/**
 * Session token as a field of the declared connection metadata (the client's
 * `meta` option / the x-wrpc-meta header): `meta: { token }`.
 */
const payloadTransport = ({ field = 'token' } = {}) => ({
  ambient: false,
  read: ({ headers, url }) => {
    const header = headers?.['x-wrpc-meta'];
    let declared = null;
    if (typeof header === 'string' && header.length > 0 && header.length <= MAX_DECLARED) {
      try {
        declared = JSON.parse(decodeURIComponent(header));
      } catch {
        declared = null;
      }
      if (typeof declared !== 'object' || Array.isArray(declared)) declared = null;
    }
    declared ??= declaredFromUrl(url, 'wrpc_meta');
    const token = declared?.[field];
    return typeof token === 'string' && token.length > 0 ? token : null;
  },
  write: () => null,
});

module.exports = {
  isTokenStore,
  memoryStore,
  webStorage,
  cookieStorage,
  bearerAuth,
  bearerTransport,
  payloadTransport,
};
