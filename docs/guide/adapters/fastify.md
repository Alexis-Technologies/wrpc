# Fastify

```js
const fastify = require('fastify')();
const { wrpcFastify } = require('@alexify/wrpc/fastify');

await fastify.register(wrpcFastify, { router });
await fastify.listen({ port: 8000 });
```

That is the whole integration. The plugin registers the RPC HTTP routes under
`basePath`, attaches a WebSocket engine to whatever fastify is running on, and
decorates the instance with `fastify.wrpc` — the `RpcServer` — so handlers
outside wrpc can reach rooms and clients:

```js
fastify.get('/admin/announce', async (request, reply) => {
  fastify.wrpc.broadcast('system/announce', { text: 'maintenance in 5m' });
  return { ok: true };
});
```

::: info fastify is injected, never depended on
`fastify` is a devDependency here, used only by the adapter tests. The plugin
receives the instance from `register()` and duck-types it, so nothing under
`src/` ever `require`s a framework.
:::

## Two backends, detected

One plugin covers both ways of running fastify:

| What fastify runs on | Engine |
| --- | --- |
| a real `http.Server` (plain `fastify()`) | the built-in [node engine](../../reference/engine), attached to its `upgrade` event |
| a `fastify-uws` server factory | the [uWebSockets.js engine](./uws) over the same uws app |

```js
const { serverFactory } = require('fastify-uws');
const app = require('fastify')({ serverFactory });

await app.register(wrpcFastify, { router });
```

Detection is by feature, not by configuration. Pass `engine` explicitly to skip
it.

## Options

Everything [`RpcServer`](../server#rpc-options) takes, plus:

| Option | Meaning |
| --- | --- |
| `rpc` | Reuse an existing `RpcServer` instead of building one from `router`. |
| `engine` | Skip backend detection. |
| `ws` | Forwarded to the engine's `attach()` — `path`, `protocols`, `verifyClient`, … |
| `maxBodySize` | A per-route `bodyLimit` for the RPC routes. |

`logger` defaults to `fastify.log`. That is a pino, which wrpc detects as a
[structured logger](../logging) and calls natively — so your RPC entries land
in fastify's own stream, with its bindings, and no adapter in between.

::: tip `maxBodySize` only narrows
Unlike the express and uws adapters, this plugin never reads the request
stream — fastify parses the body, so its own `bodyLimit` (1 MiB by default)
already guards these routes and answers `413 FST_ERR_CTP_BODY_TOO_LARGE`. Set
`maxBodySize` only to make that stricter; leaving it unset keeps the app's own
limit, because a plugin silently *raising* the host's body limit would be a
security regression the app never asked for.
:::

## Hooks run first

HTTP calls arrive through fastify's own routes, so the app's `onRequest`,
`preHandler`, authentication and error handling all run **before** wrpc sees
the call. That is the point of registering as a plugin rather than mounting a
second server:

```js
fastify.addHook('onRequest', async (request) => {
  request.tenant = await resolveTenant(request);
});

await fastify.register(wrpcFastify, { router });
```

The routes registered are `POST {basePath}`, `{basePath}/:unit/:method` and
`{basePath}/events` — the SSE endpoint is a static segment on purpose, so
find-my-way prefers it over the parametric route it would otherwise fall into.

A path outside `basePath` gets **fastify's** 404, not wrpc's error packet. That
difference is the whole reason to compose as a plugin.

## Declarative REST routes {#declarative-rest-routes}

A procedure with an [`http` mapping](../rest) becomes a **native fastify
route** under this plugin — not a proxy into the wrpc dispatcher. Fastify
owns routing, schema validation, serialization and (through
@fastify/swagger) documentation; wrpc supplies the per-request `Context`
(session, rooms, client lifecycle) and runs the bare handler under the
procedure's own queue/timeout semantics. The internal API over HTTP is
therefore the **same endpoint external consumers hit**.

```js
const router = defineRouter({
  projects: {
    create: procedure({
      http: { method: 'POST', path: '/projects/:orgId', status: 201 },
      schema: { body: { /* JSON Schema */ }, response: { 201: { /* … */ } }, tags: ['Projects'] },
      handler: async (context, { params, query, body }) => { /* ... */ },
    }),
  },
});
```

`schema` is forwarded to `fastify.route.schema` verbatim (plus wrpc's
[default error responses](../rest#default-error-responses)), so swagger
sees everything. wrpc's lifecycle hooks map onto fastify's phases by name —
the naming was fastify's to begin with — with the payload differences
documented in the adapter source.

Two things to know:

- **Validation runs once, in fastify.** The wrpc-side compiled validators
  are for the other transports; on this path the fastify pipeline already
  ran.
- **Errors answer the wire shape** `{ message, code, details? }` with the
  code as the status. Pass `restErrors: 'app'` to keep your app's own error
  format instead (the default error responses are then not documented, so
  your format survives serialization).

A collision between a mapped path and an app route under `basePath` makes
find-my-way throw at registration — the correct failure.

## Mirroring existing routes {#mirroring-existing-routes}

The reverse direction: your **existing fastify routes** become wrpc
procedures, without rewriting any of them.

```js
await fastify.register(wrpcFastify, {
  router,
  mirror: {
    access: 'session',
    headers: (context) => ({ authorization: `Bearer ${context.session?.state.token ?? ''}` }),
  },
});

// later, from a browser:
await client.load('projects');
await client.api.projects.create({ params: { orgId: '42' }, body: { name: 'Alpha' } });
```

The generated procedure dispatches through `fastify.inject()`
(light-my-request, no network), so the route's **whole pipeline** — your
onRequest hooks, auth, schema validation, serialization — runs exactly as
for a real request. Route errors flow back with their status, message and
`details`.

Rules of the collection:

- Register the plugin **before** the routes it should mirror — collection
  happens via an `onRoute` hook.
- **Naming is reverse REST semantics**: `POST /projects` → `create`,
  `GET /projects/:id` → `findById`, `GET /projects/slug/:slug` →
  `findBySlug`, `GET /projects/archive` → `findAllArchive`,
  `POST /projects/:orgId/archive/:id` → `createArchive`; `PATCH` → update,
  `PUT` → replace, `DELETE` → delete. The unit is the last static segment
  before the first param. Anything the semantics cannot express is what
  `config.wrpc` is for:

```js
fastify.route({
  method: 'DELETE',
  url: '/projects/:orgId/archive/:id',
  config: { wrpc: { name: 'unarchive' } },   // instead of deleteArchive
  handler,
});

fastify.get('/internal/health', { config: { wrpc: false } }, handler);  // opt out
```

- A naming collision throws at `onReady`, naming both routes.
- Mirrored procedures carry a `signature` distilled from the route's JSON
  Schemas, so [`wrpc types`](../cli) types them; `meta.mirrored` records
  the origin.
- Mirrored calls are calls only — no subscriptions or binary streams — and
  `fastify.inject` makes this a colder path than a native procedure.

## Shutdown

`fastify.close()` is enough: the plugin closes the core and the engine on
fastify's `preClose` hook.

## Types

```ts
import fastify from 'fastify';
import { wrpcFastify } from '@alexify/wrpc/fastify';
import type { RpcServer } from '@alexify/wrpc';

declare module 'fastify' {
  interface FastifyInstance { wrpc: RpcServer }
}
```

`findUwsApp(server)` is exported too — it digs the uWebSockets.js app out of a
fastify-uws server, and answers `null` for anything else.
