# Queue consumers

A queue carries work that must not be lost: an order to charge, an email to
send, an event another service emitted. `attachConsumers` delivers a broker
queue's messages into router procedures, **at least once**, through the same
pipeline a WebSocket call takes — hooks, validators, access, the procedure's
`queue` and `timeout`, telemetry — and turns the outcome into an ack, a retry,
or a dead letter.

```js
const { defineRouter, procedure } = require('@alexify/wrpc');
const { attachConsumers } = require('@alexify/wrpc/broker');

const router = defineRouter({
  'billing.v1': {
    consumes: {
      'orders.created': procedure({
        access: 'public',
        input: OrderCreated,                     // a Standard Schema or a function
        consume: { prefetch: 16 },
        handler: async (ctx, order) => {
          await charge(order, { idempotencyKey: ctx.callMeta.messageId });
        },
      }),
    },
  },
});

const server = new Server({ router, port: 8000 });
await server.listen();
const consumers = await attachConsumers(server, broker);
```

## Declaring consumers

`consumes` is a **reserved unit key**, next to [`on`](../router#units-and-versions) and
`hooks`. Each entry is a full procedure, keyed by the queue it drains, with an
optional `consume` policy. Two things set consumers apart from methods:

- **No peer can call one.** They live apart from the unit's methods: a `call`
  packet for `billing.v1/consumes.orders.created` — or `orders.created` —
  answers `404`.
- **Introspection does not list them.** Which queues a server drains is
  deployment topology, not a client contract; `router.consumers()` lists them
  on the server side.

`consume` is refused on a method or an event handler, where it could only be
silently ignored, and a subscription cannot consume.

## Binding at attach time

`attachConsumers(server, broker, table, options)` binds every declared consumer
(`auto: true`, the default) and takes a table for what varies per deployment:

```js
await attachConsumers(server, broker, {
  // Override a declared consumer: 'unit.vN/source'
  'billing.v1/orders.created': { queue: 'prod.orders.created', prefetch: 64 },
  // Bind an ORDINARY procedure to a queue: any other key, with a target
  'audit.events': { target: 'audit.v1/record', args: (body, headers) => ({ line: body }) },
});
```

The whole table is resolved before any consumer starts: an unknown key, a
missing target, a subscription target or a `prefetch` above the server's
`maxCalls` fails the attach instead of leaving half the consumers running.

| Policy field | Default | |
| --- | --- | --- |
| `queue` | the `consumes` key (or table key) | The broker-side queue or topic |
| `group` | the queue | The consumer group, where the broker needs one |
| `prefetch` | `16` | Messages held at once, processed concurrently; at most `maxCalls` |
| `retry` | 5 attempts, 1 s → 60 s full-jitter backoff | `{ attempts, backoff: { base, max, factor, jitter }, retryOn }`, or `false` |
| `deadLetter` | `'<queue>.dlq'` | Where exhausted and refused messages go; `false` drops them |
| `identity` | `{ trust: 'none' }` | Who the procedure runs as — see below |
| `meta` | `[]` | Message headers copied into `ctx.callMeta` |
| `args` | `JSON.parse(body)` | `(body, headers, delivery) => args` |

## What a message becomes

Every delivery is an ordinary call packet dispatched by the core:

- `ctx.callMeta` carries `messageId`, `attempt` (1-based) and `queue`, plus the
  headers `meta` allows.
- A `tp`/`ts` header continues the producer's trace: the call span is a
  `CONSUMER` span with `messaging.*` attributes (see [Publishing](#publishing)).
- The consumer's client reports `ctx.client.transportKind === 'broker'`. It is
  a request/response carrier: it runs calls, is not counted among connected
  clients, and receives no broadcasts.

The outcome decides the settlement:

| Outcome | Settlement |
| --- | --- |
| success | **ack** |
| `408`, `429`, `500`, `503` (`retryOn`) | **retry** after backoff, until `attempts` — then **dead letter** |
| `400`, `403`, `404`, `422`, `501`, anything else | **dead letter** at once |
| `503` while the server drains | **release**, attempt not counted |
| body `args` cannot parse | **dead letter** (`400`) |

`500` is retried here although [Errors](../../reference/errors#which-codes-are-worth-retrying)
says not to retry one from a client: a queue's `500` is usually a dependency
that fell over, and at-least-once delivery already demands an idempotent
handler. Narrow `retryOn` where that is not true.

A dead letter lands on the `deadLetter` queue with `x-wrpc-dead-reason`
(`"<code> <message>"`) and `x-wrpc-attempt` headers, and `onDeadLetter`
(an `attachConsumers` option) runs first — the place for an alert.

::: warning At least once means duplicates
A consumer can see a message twice: a crash between the handler's side effect
and the ack, a redelivery after `release`, a broker failover. Make handlers
idempotent — `ctx.callMeta.messageId` is a stable key to deduplicate on.
:::

## Identity

| `identity.trust` | The procedure runs with |
| --- | --- |
| `'none'` (default) | no session — a consumer whose procedure is `access: 'session'` refuses to bind |
| `'service'` | one pseudo-session for the binding: `{ token, state: { consumer: queue } }`, or `identity.session` |
| `'token'` | the session a bearer token in `identity.header` (default `authorization`) restores through the configured [token carrier](../auth) |

Token mode keeps one client per distinct token (`tokenClients`, LRU, default
128). A message without a token runs anonymous, so a session procedure refuses
it with `403` — which dead-letters.

## Draining and shutdown

A server that starts [draining](../production#graceful-shutdown) emits
`'draining'`, and every binding **pauses**: no new messages are taken, the ones
already held finish and are acked. `server.close()` emits `'close'`, and every
binding stops; a message whose call was cut off by the close is released back
to the queue for another instance. The handle drives the same by hand:

```js
await consumers.pause();
await consumers.resume();
await consumers.stop();
consumers.healthy;          // false once a consumer stopped or failed
consumers.bindings;         // [{ key, queue, group, method, healthy }]
```

Wire `healthy` into readiness next to `server.rpc.healthy`.

## Publishing

`emits` stays what it is — a declaration of a unit's outbound events for
[`wrpc types`](../cli). `createPublisher` publishes those events to a broker,
by name, so a typo cannot quietly create a topic nobody consumes:

```js
const { createPublisher } = require('@alexify/wrpc/broker');

const router = defineRouter({
  'orders.v1': {
    emits: { created: { data: { id: 'string', total: 'number' } } },
    place: procedure({
      handler: async (ctx, order) => {
        const saved = await save(order);
        await publisher.publish('orders.v1/created', saved);
        return saved;
      },
    }),
  },
});

const publisher = createPublisher(server, broker, {
  'orders.v1/created': { to: 'queue', topic: 'orders.created', key: (order) => order.id, validate: OrderCreated },
});
```

| Field | Default | |
| --- | --- | --- |
| `topic` | `'<unitKey>.<event>'` | Queue or log topic |
| `to` | `'log'` if the broker has one, else `'queue'` | A log feeds [durable feeds](./feeds); a queue feeds consumers |
| `key` | — | A string or `(data) => string`: the partition/ordering key |
| `validate` | — | A function or Standard Schema; a refusal rejects with `400` and nothing is published |

An event not declared in the unit's `emits` is refused when the publisher is
created (`strict: false` lifts that). `publish` resolves with the log id for a
log target.

Each publish is a `PRODUCER` span, and the active trace context rides in the
message headers — so a trace starts at the WebSocket call that placed the
order, continues through the publish, and ends in the consumer that charged it.
`wrpc.broker.published` counts publishes by outcome, `wrpc.broker.deliveries`
consumed messages by settlement.

::: tip Publishing is not transactional
`publish` after a database write can fail after the write succeeded. When the
two must agree, write the event to an outbox table in the same transaction and
publish from the outbox.
:::
