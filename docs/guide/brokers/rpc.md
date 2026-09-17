# RPC over a broker

Services that already share a broker can call each other through it, with no
HTTP or WebSocket between them and no service discovery: every instance of a
service listens on one address, the broker load-balances, and a caller only
needs the broker and the service's name.

```js
// the billing service
const { attachBrokerRpc } = require('@alexify/wrpc/broker');
await attachBrokerRpc(server, broker, { service: 'billing' });

// a caller
const { connect } = require('@alexify/wrpc');
require('@alexify/wrpc/broker');           // registers the 'broker' transport

const billing = await connect('broker://billing', { transport: 'broker', broker });
await billing.load('invoices');
await billing.api.invoices.create({ orderId });
```

The packets on the wire are ordinary wrpc packets — the same router, hooks,
sessions, validators and telemetry serve a WebSocket client and a broker one
side by side. The carrier is specified in
[the protocol reference](../../reference/protocol#broker-binding).

::: warning Experimental
Part of the [`@alexify/wrpc/broker`](../brokers) family, which may change in a
minor release until every adapter has shipped. Needs a broker with the
`direct` capability: the in-process, Redis, NATS and RabbitMQ adapters have
one; Kafka does not.
:::

## Two modes

| `mode` | Like | Carries | Instances |
| --- | --- | --- | --- |
| `'stateless'` (default) | the `http` transport | calls and batches | any instance answers any call |
| `'session'` | the `ws` transport | calls, events, subscriptions, cancellation, streams | one instance per session, found at connect |

**Stateless** is the one to reach for between services: nothing to keep alive,
nothing to lose when an instance goes away, and a request is handled on the
server exactly like a packet-mode HTTP POST — sessions come from its headers,
batches work, and a subscription is refused with `400`.

**Session** is the whole protocol. The client's `hello` reaches one instance,
which answers with its own inbox; every later frame goes there, numbered in
both directions. It is what you want when a service subscribes to another's
feed, or streams a file to it.

```js
const reports = await connect('broker://reports', {
  transport: 'broker',
  broker,
  mode: 'session',
  headers: { authorization: `Bearer ${serviceToken}` },
});
for await (const row of reports.api.export.rows.iterate({ since })) handle(row);
```

## What happens when an instance goes away

A session lives on one instance, so losing it is a lost connection, and the
client's ordinary [reconnect](../client#reconnecting) takes it from there:

- An instance that **closes** (`server.close()`) says `bye` to every session it
  holds; the client reconnects, its `hello` is taken by another instance, and
  subscriptions resume from `lastEventId`.
- An instance that **dies** says nothing. The client's heartbeat stops getting
  `pong`s and the client reconnects the same way.
- A **frame the broker dropped** shows up as a gap in the frame numbers, on
  whichever side receives the next one. The session is treated as lost rather
  than continuing with a call nobody will ever answer.
- A client that dies holds a server-side session until `idleTimeout`
  (default 90 s, three client heartbeats) ends it.

Stateless requests have none of this: a request nobody took fails its calls
with `503` when the broker can tell (NATS, the in-process broker), or with
the call timeout otherwise.

## Draining

A server that starts [draining](../production#graceful-shutdown) stops
consuming the service address — new requests and new sessions go to the
other instances — while the sessions already here keep running until the
close.

## Server options

| Option | Default | |
| --- | --- | --- |
| `service` | — | Served at `wrpc.<service>` |
| `address` | — | The service address outright, instead of `service` |
| `idleTimeout` | `90000` | A session silent this long is ended |
| `highWaterMark` | `1024` | Unconfirmed frames per session before `write()` reports backpressure |
| `sessions` | `true` | `false` serves stateless requests only |

`attachBrokerRpc` resolves with `{ address, inbox, sessions, healthy, stop() }`
and stops by itself when the server closes.

## Client options

`connect('broker://<service>', { transport: 'broker', ... })` takes the usual
[client options](../client) plus:

| Option | Default | |
| --- | --- | --- |
| `broker` | — | A broker with `direct`, or the capability |
| `mode` | `'stateless'` | `'session'` for the full protocol |
| `address` | `wrpc.<service>` | Where the service listens |
| `requestTimeout` | `30000` | How long the broker may hold a request nobody took |

`headers` and `meta` travel on every stateless request and on a session's
`hello`, where the server reads them as a WebSocket upgrade's.
