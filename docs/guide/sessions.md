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
| `finalizeSession()` | Deletes it from the store and drops it — and ends it on this instance's other connections that restored the same token. `false` if there was none. |

`token` defaults to a freshly generated one; pass your own to adopt an existing
identifier.

::: warning A login over WebSocket sets no cookie
A `Set-Cookie` header needs a response, and an open socket has none.
`startSession` therefore only emits the cookie on HTTP transports; over a
WebSocket — or any persistent transport: WebTransport, a data channel, a
broker session — it creates the session for that connection alone. If you want the
session to survive a reconnect, log in over HTTP (the browser stores the
cookie) and let the WebSocket upgrade restore it — which it does, from the same
cookie.

The counterpart for everything the cookie cannot reach: the client's
[`authenticate` hook](./client#authenticating) re-presents the credential on
every reconnect, *before* the subscriptions are re-opened and the units
re-loaded. On a Node client — whose `WebSocket` sends no cookies at all — it
is the **only** way a session survives a reconnect.
:::

A dropped connection never deletes the session from the store. That is exactly
what makes a reconnect cheap: sessions end through `finalizeSession()` or
store-side expiry, and nothing else.

**A logout ends the session everywhere on this instance.** Every connection
that restored the same token — another tab, a second device holding the same
bearer token — holds its own copy of the session; `finalizeSession()` on one
marks the others ended, so their next call to a `session` procedure is a
`403`, as on the connection that logged out (a handler already running there
finishes with its `context.session`). **Another instance** has its copies in
its own memory and does not hear of it: a connection there keeps the session
until it reconnects — the restore then finds no row — or until your
application tells it, over the [cluster](./scaling) or the rooms backplane. A
store with short TTLs and `touch()` narrows that window.

**A store that fails is not a missing session.** When the store throws while a
connection's token is restored (`session.restore` in the log), the connection
goes on without a session, and a `session` procedure answers it `503` — which
a [broker consumer](./brokers/consumers#identity) retries — rather than the
`403` of a token that names no session.

## The lifecycle

```mermaid
sequenceDiagram
  autonumber
  participant B as client
  participant S as server
  participant T as SessionStore
  B->>S: call auth/login
  S->>T: set(token, state)
  S-->>B: callback + Set-Cookie
  B->>S: call profile/whoami
  Note over S: cookie → restoreSession(token)
  S->>T: get(token)
  T-->>S: state
  S-->>B: callback { user }
  Note over B,S: connection drops
  B->>S: reconnect, cookie replayed
  S->>T: get(token)
  Note over S: same session, new Client
```

A reconnect is a **new server-side client** carrying the **same session**: the
cookie is what survives the socket. That is why room membership has to be
re-applied by hand ([rooms](./rooms#rooms-and-reconnects)) while session state
simply reappears.

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
**without wRPC depending on any of them**. The default is
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
a shared store, and one ships for Redis:

```js
const { createRedisSessionStore } = require('@alexify/wrpc/scaling');
const Redis = require('ioredis');

new Server({
  router,
  sessions: { store: createRedisSessionStore({ client: new Redis(url), prefix: 'wrpc:session:', ttl: 24 * 3600_000 }) },
});
```

ioredis-shaped and injected, like the [backplane](./scaling#redis): `get`,
`set(key, value, 'PX', ttl)`, `del`, and `pexpire` for the sliding expiry
`restore()` performs (`touch`). State is stored as JSON under the prefix.
node-redis v4 spells the expiring set as `set(key, value, { PX })` — a
two-line wrapper adapts it. With a shared store, no instance owns a session
and a WebSocket client needs **no sticky routing** — see
[what stays per-instance](./scaling#what-stays-per-instance).

### Sealing what rests in the store {#sealed}

A shared store is a third party. The Redis store keeps each session's state
as JSON **under the token itself** — so a keyspace listing is a list of live
bearer credentials, and a dump is every user's state. `sealedStore` wraps
any store so that neither rests in it:

```js
const { sealedStore } = require('@alexify/wrpc/encryption');

new Server({
  router,
  sessions: {
    store: sealedStore(createRedisSessionStore({ client: new Redis(url) }), {
      keys: process.env.WRPC_SESSION_KEY, // 32 bytes: base64, hex or a Uint8Array
    }),
  },
});
```

A row is keyed by an HMAC of the token and holds the state sealed
(AES-256-GCM), with the row's own key as additional data — a row copied
into another session's slot does not open. A row that does not open is a
missing session and one `session.open` warning; the token is never logged.

- **Rotation signs nobody out.** With `keys: { current: 'k2', ring: { k1,
  k2 } }` a read that misses under `k2` finds the row under `k1` and moves
  it. Drop `k1` once your longest session TTL has passed since it stopped
  being current.
- **Adopting it over a store that already holds sessions:** `acceptPlaintext:
  true` reads a row the unwrapped store wrote once, seals it and deletes the
  plaintext. Turn it off after the same TTL — while it is on, the raw token
  is a name the store still answers to, and only a session-shaped object
  under it is taken for one (a sealed record put there is refused).
- **A fleet mid-rotation** — one instance writing under `k1`, another under
  `k2` — keeps one row per token: a write removes the token's rows under
  the other kids, and a migration deletes the old row only while it is
  still the row that was read (a newer state written there meanwhile is
  moved instead).
- It costs one extra `get` per *older* kid on a miss — an unknown token
  included — so keep the ring short.

Adopting it over a fleet that already holds sessions is **three deploys**,
because an instance on the previous deploy must still find every session
where it looks for it:

| Deploy | `sealedStore(store, …)` | Writes | Reads |
| --- | --- | --- | --- |
| 1 | `{ keys, seal: false, acceptPlaintext: true }` | plaintext | both — a sealed row is read where it is, never moved |
| 2 | `{ keys, acceptPlaintext: true }` | sealed | both — a plaintext row is sealed on its first read |
| 3 | `{ keys }` | sealed | sealed only |

Move on once every instance runs the deploy before, and leave deploy 2 on
for the longest session TTL. A key **provider** for the store must answer
`kids()` — a rotation is walked through it, and a provider without it would
keep every session under an older kid unreadable the moment `current`
moved (a `TypeError` at construction, not a mass logout later):

```js
sealedStore(store, {
  keys: { current: () => vault.current, get: (kid) => vault.get(kid), kids: () => vault.kids },
});
```

What it does not do: hide how many sessions exist or when they are touched,
or protect a session from someone holding the key — every instance does.

Writes to `session.state` are coalesced: the assignments of one turn become
**one** `store.set` on a microtask (the initial state of `create()` is
written immediately), and a session that was finalized in the meantime is
not written back. Every write after the first is **conditional** —
`set(token, state, { create: false })` — and a store answering `false` to it
is saying the row is gone: a logout on another connection or instance landed
first, and this write must not undo it. The session ends there
(`session.save` with `reason: 'gone'`) instead of coming back, token and
all. The memory store, the Redis store (`SET … XX`) and `sealedStore`
refuse such a write; a custom store may ignore the option and write as
before.

## Tokens

```js
new Server({ router, sessions: { generateToken: () => myUlid() } });
```

The default is a v4 UUID from `node:crypto` (or `globalThis.crypto` in a
browser build). Whatever you substitute must be unguessable: it is the whole
credential.

## Pluggable token carriers

*Where* the token lives on the wire is an injection like the store —
`sessions.transport`, structural (`isTokenTransport`), with the cookie
behaviour as the byte-identical default:

```js
// { read({ headers, url, declared, meta }) -> token | null,
//   write(token) -> Set-Cookie-style header value | null,
//   ambient?: boolean }
new Server({ router, sessions: { transport: myTransport } });
```

Besides the raw `headers`/`url`, `read()` receives what the core already
parsed: `declared` — the merged declared+observed header bag (whatever a
ws client declared, through the `wrpc.h.` subprotocol token or the `wrpc_h`
query, capped on the configurable `metaMaxBytes`) — and
`meta`, the sanitized connection-metadata bag with **both** `x-wrpc-meta`
spellings merged and keys kebab-normalized. Prefer them over re-parsing the
wire: a strategy with its own parser can silently drift from the core's.
(Both are absent on the SSE channel-key path, so keep a raw-header fallback
for the names you read.)

Two ready-made strategies ship in the **`@alexify/wrpc/auth`** subpath —
deliberately outside the base bundle, like the rooms backplane in
`./scaling`:

```js
const { bearerTransport, payloadTransport } = require('@alexify/wrpc/auth');
new Server({ router, sessions: { transport: bearerTransport() } });
```

- **`bearerTransport()`** reads `Authorization: Bearer <token>` — the real
  header where the transport can send one (http/sse, curl, ws from Node); on browser ws,
  where the WebSocket constructor cannot set headers, the client offers the
  token as a **`wrpc.bearer.<token>` subprotocol** next to the wire
  revision, so the credential travels as a real upgrade header and **never
  lands in the connect URL** (URLs end up in proxy access logs — see the
  [metadata caveat](./metadata#declared-headers-the-headers-client-option)). A token
  outside the RFC 7230 token charset (spaces, `/`, `=` padding) cannot ride
  bare and travels inside the declared-headers token instead — base64url, so
  still a header and still off the URL. Only `carrier: 'query'` or
  `protocols: []` can put it in the query, and the client warns when it does.
- **`payloadTransport({ field })`** reads a field of the client's declared
  `meta` — for apps that keep `authorization` semantics out of it. Both
  `x-wrpc-meta` spellings are read, the canonical JSON header and the
  per-key `x-wrpc-meta-<field>` form.

Two asymmetries every non-cookie strategy inherits, both by construction:

- **The server cannot *send* `Authorization`.** `write()` returns null; the
  `signIn` handler hands the token pair back **in its result**, the client
  stores it (see the client half below) and presents it on the next
  connection. Neither half works alone — the pair is the strategy.
- **The safe-method CSRF rule does not apply.** That rule guards *ambient*
  authority — a cookie the browser attaches without script. A bearer
  credential is script-attached, so a non-ambient transport
  (`ambient: false`) restores on safe methods too, cross-site fetch headers
  or not.

### The client half: stores and `bearerAuth()`

The same subpath carries the client side: token **stores**
(`get`/`set`/`delete`, sync or async — a `Map` already qualifies) and
`bearerAuth()`, which composes a store with your `signIn`/`refresh` calls
into the three [client options](./client#authenticating) that make the
strategy work end to end:

```js
const { bearerAuth, webStorage } = require('@alexify/wrpc/auth');

const client = await connect(url, {
  ...bearerAuth({
    store: webStorage(localStorage), // or memoryStore(), cookieStorage(document), your IndexedDB wrapper
    signIn: (c) => c.call('auth/signIn', credentials()),
    refresh: (c, tokens) => c.call('auth/refresh', { token: tokens.access }),
  }),
});
```

`headers` presents the stored token on **every** open (so the reconnect's
upgrade restores the session before the re-subscribe), `authenticate` signs
in only when the store is empty, and `refresh` is single-flight with a
one-shot retry. The server-side `auth/refresh` handler re-binds the **live**
connection with `context.client.startSession(...)` and returns the rotated
pair — that is what heals a token expiring mid-socket without a reconnect.
