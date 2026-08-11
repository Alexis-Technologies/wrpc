# Sessions

A session is a **token** plus a **state object** held in a store. The token
travels in a cookie; the state is whatever your handlers put there.

```js
defineRouter({
  auth: {
    login: procedure({
      access: 'public',
      handler: async (context, { user, password }) => {
        await verify(user, password);
        context.client.startSession(undefined, { user });
        return { ok: true };
      },
    }),
    logout: procedure({
      access: 'session',
      handler: async (context) => context.client.finalizeSession(),
    }),
  },
  profile: {
    whoami: procedure({
      access: 'session',
      handler: async (context) => ({ user: context.session.state.user }),
    }),
  },
});
```

## Reading and writing state

`context.session.state` is a proxy: assigning to it persists to the store, with
no `save()` call to forget.

```js
handler: async (context) => {
  context.session.state.lastSeen = Date.now();   // written through to the store
},
```

The write is fire-and-forget — a store error is logged, not thrown at the
handler. Mutating a nested object (`state.prefs.theme = 'dark'`) does **not**
trigger a save, because only the top level is proxied; reassign the whole
branch instead.

## The client API

These live on `context.client`:

| Method | What it does |
| --- | --- |
| `startSession(token?, data?)` | Creates a session **and** sends the cookie. What a login calls. |
| `initializeSession(token?, data?)` | Creates it without touching cookies. |
| `restoreSession(token)` | Loads one from the store. `false` if it is gone. |
| `finalizeSession()` | Deletes it from the store and drops it. `false` if there was none. |

`token` defaults to a freshly generated one; pass your own to adopt an existing
identifier.

::: warning A login over WebSocket sets no cookie
A `Set-Cookie` header needs a response, and an open socket has none.
`startSession` therefore only emits the cookie on HTTP transports; over a
WebSocket it creates the session for that connection alone. If you want the
session to survive a reconnect, log in over HTTP (the browser stores the
cookie) and let the WebSocket upgrade restore it — which it does, from the same
cookie.
:::

A dropped connection never deletes the session from the store. That is exactly
what makes a reconnect cheap: sessions end through `finalizeSession()` or
store-side expiry, and nothing else.

## Cookies

```js
new Server({
  router,
  sessions: {
    cookie: { name: 'token', path: '/', httpOnly: true, secure: true, sameSite: 'Lax', maxAge: null },
  },
});
```

Those are the defaults. `maxAge: null` makes it a session cookie — gone when
the browser closes. The cookie is read back on **both** an HTTP request and the
WebSocket upgrade, which is what restores a session without a round trip.

::: tip `secure: true` and localhost
Browsers refuse a `Secure` cookie over plain `http://` — except on
`localhost`, which they treat as a secure context. So the default works in
development and stays correct in production. If you terminate TLS at a proxy
and genuinely serve plain HTTP on a real hostname, you will need
`secure: false`, and you should understand what you are giving up.
:::

### The cross-site GET gate

A `SameSite=Lax` cookie rides along on cross-site **navigation**. That is
convenient for ordinary web pages and dangerous for an RPC endpoint: an
attacker's page could point a browser at
`{basePath}/account/deleteEverything` and the cookie would come along.

So in [REST mode](./server#what-it-serves), a `GET`/`HEAD` request only gets its
cookie-restored session when the request proves same-origin intent through
`Sec-Fetch-Site`. A cross-site one runs anonymously — public procedures only,
`403` for the rest. Packet-mode `POST`s are unaffected, and non-browser peers
(curl, server-to-server), which send no Fetch metadata at all, keep their
session.

## Stores

The store contract is structural — three async methods, no base class to
extend:

```js
const store = {
  async get(token) { /* -> state | null */ },
  async set(token, state) {},
  async delete(token) {},
};

new Server({ router, sessions: { store } });
```

Which is how Redis, a database table, or a signed-cookie store plug in
**without wrpc depending on any of them**. The default is
`MemorySessionStore`, bounded on both axes:

```js
const { MemorySessionStore } = require('@alexify/wrpc');

new Server({
  router,
  sessions: { store: new MemorySessionStore({ maxSessions: 10000, ttl: 24 * 60 * 60 * 1000 }) },
});
```

LRU eviction past `maxSessions`, expiry after `ttl` (`0` disables either).
It is a real store, not a stub — but it lives in one process, so a second
instance shares nothing with it. Anything running more than one process wants
an injected store.

## Tokens

```js
new Server({ router, sessions: { generateToken: () => myUlid() } });
```

The default is a v4 UUID from `node:crypto` (or `globalThis.crypto` in a
browser build). Whatever you substitute must be unguessable: it is the whole
credential.
