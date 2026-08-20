# Declarative REST

One procedure, two audiences. A procedure that declares an `http` mapping is
addressable as a **real REST endpoint** — proper verb, proper path, proper
status — while staying an ordinary wrpc procedure on every other transport:

```js
const { defineRouter, procedure } = require('@alexify/wrpc');

const router = defineRouter({
  projects: {
    create: procedure({
      access: 'session',
      http: { method: 'POST', path: '/projects/:orgId', status: 201 },
      schema: {
        params: { type: 'object', properties: { orgId: { type: 'string' } }, required: ['orgId'] },
        body: { type: 'object', properties: { name: { type: 'string', minLength: 2 } }, required: ['name'] },
        response: { 201: { type: 'object', properties: { id: { type: 'string' } } } },
        tags: ['Projects'],
      },
      handler: async (context, { params, query, body }) => createProject(params.orgId, body),
    }),
  },
});
```

```bash
curl -X POST https://host/api/projects/42 -H 'content-type: application/json' -d '{"name":"Alpha"}'
```

The response is the **plain result** with status `201` — external REST
semantics, no callback envelope. The same procedure called over WebSocket is
`projects/create` with the same `{ params, query, body }` args object: the
mapping only defines how an HTTP request is unpacked.

## The `http` option

| Field | Meaning |
| --- | --- |
| `method` | `GET`, `HEAD`, `POST`, `PUT`, `PATCH` or `DELETE`. |
| `path` | Relative to the server's `basePath`. Segments are static or `:name`. |
| `status` | Success status; default `200`. `204` discards the result body by contract. |

Validated when the procedure is built: a typo'd verb, a wildcard segment or
a status outside 200–599 throws immediately. Subscriptions cannot carry a
mapping — they are already refused on plain HTTP.

Routing rules match find-my-way's: one tree per verb, a static segment beats
a parameter, and within one verb a duplicate path or two different parameter
names at the same position **throw when the router is built**, naming both
procedures. A known path hit with the wrong verb answers **405** with an
`Allow` header. Errors on a mapped route answer the wire error object —
`{ message, code, details? }` — with the code as the HTTP status.

The conventional `ANY {basePath}/:unit/:method` mode keeps working underneath
as a fallback, unchanged, callback envelopes and all.

## Versioning

By default a [versioned unit](./router#units-and-versions)'s declared path
registers verbatim — so two versions declaring the same path collide at build
time. The router-level `rest.version` strategy maps each version onto its own
URL instead:

```js
defineRouter(
  {
    'auth.v1': {
      signIn: procedure({ http: { method: 'POST', path: '/auth/signIn' }, handler }),
    },
  },
  { rest: { version: 'path' } },
);
// POST {basePath}/v1/auth/signIn  — the wire target stays auth.v1/signIn
```

The rules:

- `'path'` prefixes the declared path with `/vN` — the version token of the
  unit key, verbatim (`auth.v1` → `/v1/...`; `path: '/'` becomes `/v1`).
- The **default version stays unprefixed**: `auth` + `/auth/signIn` keeps
  `/auth/signIn`, so the same declared path in `auth` and `auth.v1` no longer
  conflicts and both dispatch.
- A function form takes full control: `version: (version, path) => string`
  receives the token (`'v1'`) and the declared path and returns the effective
  path (it must start with `/`).
- `proc.http` is the **declaration** and never mutates — the prefix is
  computed where routes surface (the dispatch trie, `restRoutes()`, and
  introspection), which is what keeps the shell, the host adapters and the
  client's REST leg version-consistent without any of them knowing about the
  strategy. The strategy also survives `merge()`.

## The `schema` option

The shape is `fastify.route.schema`, verbatim: `params`, `querystring` (or
its alias `query` — interchangeable), `body`, `headers`, `response` keyed by
status code, plus any passthrough keys (`tags`, `summary`, `security`,
`operationId`, …) that wrpc never interprets but forwards to hosts —
which is what makes swagger documentation free under the
[fastify adapter](./adapters/fastify#declarative-rest-routes).

`schema` and the programmatic `input`/`output` validators are **mutually
exclusive** on one procedure. How schemas become validation is the
[injected compiler's](./router#validators) job.

### Default error responses

wrpc documents its own lifecycle errors in the effective `response` — what a
host and swagger see — derived from the procedure's options:

| Status | When it is documented |
| --- | --- |
| `429`, `500`, `503` | Always — every call can hit maxCalls, a handler bug, a drain or a full queue. |
| `400` | The procedure validates (`schema` or `input`). |
| `403` | `access` is not `'public'`. |
| `408` | The procedure has a `timeout`. |

Each entry is the wire error shape. Your own `schema.response[code]`
**overrides** the default for that code; `response[code]: false` removes it.
`499` (cancel) is deliberately absent — a cancelled request gets no response
to document.

## The client side

A client on the **http transport** sends mapped calls as the same REST
requests external consumers do — URL built from `http.path` and
`args.params`, query string from `args.query`, JSON body from `args.body` —
and reads the plain result back. Every other transport speaks packets. The
mapping travels through `system/introspect` (`info.http`), so nothing is
configured on the client.

## Pluggable query strings

The default query parser is the prototype-safe `URLSearchParams` path —
flat string values, last one wins. To parse (and, client-side, serialize)
nested objects and arrays, inject your own — `qs`, for example:

```js
// Server
const qs = require('qs');
new Server({ router, querystring: { parse: (text) => qs.parse(text) } });

// Client — the mirror, so array encodings agree end to end
await WrpcClient.connect(url, { querystring: { stringify: (query) => qs.stringify(query) } });
```

An injected parser takes over prototype-pollution responsibility — `qs`
guards against `__proto__` keys itself; a hand-rolled parser must too.

## Hosts

Under the **fastify adapter** every mapped procedure becomes a native
fastify route — fastify owns routing, validation, serialization and swagger;
see [Fastify](./adapters/fastify#declarative-rest-routes). Under the
built-in `Server`, express and uws, the core serves the mapped routes
itself, schemas compiled by the [injected validator](./router#validators).
