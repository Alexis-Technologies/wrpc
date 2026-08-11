# Logging

Every server component writes through one injected logger. Pass yours as
`logger`:

```js
const pino = require('pino');
const { Server } = require('@alexify/wrpc');

const server = new Server({ router, logger: pino(), port: 8000, protocol: 'http' });
```

That is the whole configuration. There are no levels to set, no destination to
choose and no format to pick — those are your logger's job, and wrpc does not
duplicate them.

::: info The logger is injected, never depended on
`@alexify/wrpc` has no dependencies and never will. Your logger is duck-typed:
wrpc looks at which methods it has, not at what it is. `pino` is a
devDependency here purely so the tests have something real to write into.
:::

## What you can pass

| Value | Behaviour |
| ----- | --------- |
| a **structured** logger | called as `(entry, message)` — pino, bunyan, winston, roarr |
| a **Console** | called as `(message)`; the entry is dropped |
| `true` | the global console |
| `false` / `null` | silent |
| omitted | the global console |

A logger that matches neither shape disables logging rather than throwing.
Observability is never the reason a server fails to boot.

### How the two shapes are told apart

By `child()` or a `level` property. Every structured logger worth injecting
has one or both; a `Console` has neither.

The asymmetry is deliberate. Guessing "structured" wrong prints an object
where a string belongs — loud and obvious. Guessing "Console" wrong drops
some fields — quiet. So a sink giving no signal is treated as a Console.

If you have a `(entry, message)` logger without `child` or `level`, give it a
`level` property and it takes the structured path.

## Entries

Every line carries a machine-readable entry and a human message. Structured
loggers get both, in pino's `(mergingObject, msg)` convention; a Console gets
the message alone.

```json
{ "level": 30, "component": "rooms", "peer": "10.0.0.4",
  "event": "call.ok", "method": "chat/send", "msg": "10.0.0.4\tCALL\tchat/send\tOK" }
```

`event` is on every entry and is the field to filter on. An `err` key, when
present, holds the Error itself.

### Bindings

wrpc builds child loggers so you do not have to correlate by hand:

| Binding | Scope |
| ------- | ----- |
| `component` | `rooms`, `sse` or `sessions` — built once per server |
| `peer` | one connection, bound when it opens |
| `callId` | one call, subscription or inbound event |

`callId` is bound lazily, because a `Context` is allocated for every packet
and most handlers never log. It costs nothing until something asks for it.

## Logging from a handler

`context.log` is a child bound to that call:

```js
const send = procedure({
  access: 'session',
  handler: async (context, { text }) => {
    context.log.info({ event: 'message.accepted', length: text.length });
    return { ok: true };
  },
});
```

The entry it writes already carries `callId` and `peer`, so a single call is
greppable end to end without threading an id through your own code.

## Turning it off

```js
new Server({ router, logger: false });
```

Silent — not "writes to a sink that discards", but never called at all. The
disabled writer is a frozen no-op singleton whose `child()` returns itself, so
the per-connection and per-call bindings cost nothing when logging is off.

## Fastify

The [fastify plugin](./adapters/fastify) uses `fastify.log` unless you pass
`logger` explicitly. Since that is a pino, it goes in as a structured logger
and your wrpc entries land in the same stream as fastify's own, with the same
request ids.

## Failures

A logger that throws cannot break a request. Every call is guarded inside the
writer, so a broken transport, a full disk or a serialization error costs you
the line, not the call.

## Errors are observed, not swallowed

On the client the `logger` is **off by default** — a browser console filling
up with reconnect noise is not a default anyone asked for:

```js
const client = await WrpcClient.connect(url, { logger: pino() });
```

When it is on, an error is both logged and emitted as `'error'`. A logger
observes; a listener handles. You get both, and adding one does not silence
the other.
