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

`console` defaults to fastify's own logger, adapted: pino has `info`/`warn`/
`error` but no `log`, which the core calls on every successful invocation.

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
