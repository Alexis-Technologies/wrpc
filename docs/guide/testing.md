# Testing

wrpc's own suite runs on `node --test` with no framework, and the patterns
below are the ones it uses. They work the same in Vitest or Jest — the only
wrpc-specific parts are how you boot a server on a free port and how you make
sure it is closed.

## The boot pattern

```js
const { test } = require('node:test');
const assert = require('node:assert');
const { Server, WrpcClient, defineRouter, procedure } = require('@alexify/wrpc');

const bootServer = async (t, options = {}) => {
  const server = new Server({
    host: '127.0.0.1',
    port: 0,            // let the OS pick — parallel test files never collide
    protocol: 'http',
    logger: false,      // quiet: a passing test should print nothing
    ...options,
  });
  await server.listen();
  t.after(() => server.close());        // registered the moment it resolves
  const { port } = server.address();
  return { server, url: `ws://127.0.0.1:${port}${server.rpc.basePath}` };
};

const connectClient = async (t, url, options = {}) => {
  const client = await WrpcClient.connect(url, { heartbeat: false, reconnect: false, ...options });
  t.after(() => void client.close());
  return client;
};
```

::: danger Register teardown with `t.after`, never a trailing `await close()`
If an assertion fails before the closing line runs, the server is never closed.
With the default engine that leaks a socket; with a
[standalone uws engine](./adapters/uws) it leaks a **native listen socket**,
and `node --test` hangs forever instead of reporting the failure. `t.after`
runs whether the test passed or not.
:::

Three details earn their place:

- **`port: 0`** — the OS picks a free port, so test files run in parallel
  without a port registry. Read it back with `server.address()`, which also
  works with a standalone engine where `server.httpServer` is `null`.
- **`logger: false`** — silences the default `console` writer. Pass a
  collecting logger instead when the test is *about*
  [logging](./logging).
- **`heartbeat: false, reconnect: false`** on the client — a test that asserts
  on a disconnect should not race a reconnect, and a 30-second heartbeat timer
  keeps the process alive.

## A first test

```js
test('greeting/hello answers', async (t) => {
  const router = defineRouter({
    greeting: {
      hello: procedure({ access: 'public', handler: async (context, { name }) => `Hello, ${name}` }),
    },
  });

  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('greeting');

  assert.equal(await client.api.greeting.hello({ name: 'World' }), 'Hello, World');
});
```

## Waiting for something asynchronous

Events, room broadcasts and subscription values arrive on their own schedule.
Polling with a deadline beats a fixed `setTimeout`, which is either flaky or
slow:

```js
const timers = require('node:timers/promises');

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 300; i++) {          // ~1.5 s
    if (await predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

test('a room broadcast reaches the client', async (t) => {
  const { server, url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  await client.load('chat');

  const seen = [];
  client.api.chat.on('message', (data) => seen.push(data));

  await waitFor(() => server.rpc.clients.size === 1);
  server.to('lobby').emit('chat/message', { text: 'hi' });   // after a join
  await waitFor(() => seen.length === 1, 'no event arrived');
});
```

## Testing multi-instance behaviour without infrastructure

`MemoryBackplane` is the reference implementation of the
[backplane contract](./scaling#the-contract), and two `RpcServer`s sharing one
behave like two processes sharing a Redis — including
[cluster](./cluster) presence and `fetchClients`:

```js
const { RpcServer } = require('@alexify/wrpc');
const { MemoryBackplane } = require('@alexify/wrpc/scaling');

const backplane = new MemoryBackplane();
const a = new RpcServer({ router, backplane, instanceId: 'a' });
const b = new RpcServer({ router, backplane, instanceId: 'b' });
```

Delivery is deferred to a microtask exactly like a real broker's, so a test
that asserts immediately after an `emit()` will see nothing — `await` a tick,
or use `waitFor`.

Reach for a real Redis only when you are testing the **adapter**; wrpc's own
Redis suite is skip-guarded on `REDIS_URL` and is not part of `pnpm test` for
that reason.

## Testing without a network at all

`rpc.attachPort(port)` attaches a `MessagePort` as a transport — the same seam
the [Service Worker](./client#service-workers) client uses. A
`node:worker_threads` `MessageChannel` gives you a full client/server exchange
with no listener, no port and no sockets:

```js
const { MessageChannel } = require('node:worker_threads');

const { port1, port2 } = new MessageChannel();
rpc.attachPort(port1);
```

## Testing your handlers directly

A procedure is a plain object with an `invoke(context, args, hooks)` method, so
unit-testing the handler needs no server. But do it deliberately: calling the
handler function directly skips access checks, validators, hooks, the queue and
the timeout — everything the router adds. For anything where that policy is
part of the behaviour, boot a server; it costs a millisecond.

## Adapters and optional dependencies

If your suite covers a [host adapter](./adapters/fastify), make the framework
optional so a machine without it **skips** rather than fails — the pattern
wrpc's own adapter tests use for `uWebSockets.js`, whose native binary does not
build everywhere:

```js
const optional = (name) => {
  try {
    return require(name);
  } catch {
    return null;
  }
};

const uws = optional('uWebSockets.js');
test('uws engine', { skip: uws ? false : 'uWebSockets.js not installed' }, async (t) => { /* … */ });
```

## Running wrpc's own tests

```bash
pnpm test                              # everything, recursive
node --test tests/smoke.test.js        # one file — node --test takes files, not dirs
node --test --test-name-pattern="rooms"
pnpm test:coverage                     # c8 over src/
pnpm test:types                        # tsd against the .d.ts files
```

::: tip When a run hangs instead of failing
Redirect to a file and read it back with a kill guard — piping through
`tail`/`head` buffers the output and shows you nothing. A hang almost always
means a leaked listen socket; look for a test that closes outside `t.after`.
:::
