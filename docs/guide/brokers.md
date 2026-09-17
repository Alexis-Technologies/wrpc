# Message brokers

[Scaling](./scaling) uses a broker as a **backplane**: a fan-out that makes a
room span every instance, at most once. That is the right contract for
presence and typing indicators, and the wrong one for everything a broker is
usually deployed *for* — work that must not be lost, history a client resumes
from, services talking to each other without HTTP between them.

`@alexify/wrpc/broker` is the broker-agnostic core for those. It describes a
broker by what it can do, and every wrpc feature built on brokers asks only for
the capability it needs:

| Capability | Guarantee | What wrpc builds on it |
| --- | --- | --- |
| `backplane` | at-most-once fan-out | [rooms](./rooms) and the [cluster](./cluster) across instances |
| `log` | ordered, replayable | [durable subscription feeds](./brokers/feeds) that resume on any instance |
| `queue` | at-least-once, competing consumers | [procedures invoked from a queue](./brokers/consumers), with retry and dead-lettering |
| `direct` | addressable inboxes | [RPC between services](./brokers/rpc) over the broker itself |

::: warning Experimental
The whole `@alexify/wrpc/broker*` family is `@experimental`: it may change in a
minor release until every adapter has shipped. See
[Stability](../reference/stability#experimental-carve-outs).
:::

## No broker is a dependency

Like the [Redis backplane](./scaling#redis), every adapter takes **your**
client and checks its shape; `@alexify/wrpc` installs no broker package and
never will. Each broker gets its own subpath, so an application that uses
Kafka does not load the NATS adapter:

| Broker | Subpath | backplane | log | queue | direct |
| --- | --- | :---: | :---: | :---: | :---: |
| in-process | `@alexify/wrpc/broker` | ✓ | ✓ | ✓ | ✓ |
| [Redis](./brokers/redis) (Streams + pub/sub) | `@alexify/wrpc/broker/redis` | ✓ | ✓ | ✓ | ✓ |
| [NATS](./brokers/nats) + JetStream | `@alexify/wrpc/broker/nats` | ✓ | ✓ | ✓ | ✓ |
| [RabbitMQ](./brokers/amqp) (AMQP 0-9-1) | `@alexify/wrpc/broker/amqp` | ✓ | ✓ | ✓ | ✓ |
| [Kafka](./brokers/kafka) | `@alexify/wrpc/broker/kafka` | caveats | ✓ | ✓ | — |

Kafka has no `direct`: consumer-group rebalances and a topic per instance make
it a poor carrier for request/response. Its backplane works, with costs the
Kafka page spells out. Brokers that speak one of these protocols need no
adapter of their own — Valkey, KeyDB and Dragonfly use the Redis one;
Redpanda and Azure Event Hubs' Kafka endpoint use the Kafka one.

## The in-process broker

```js
const { MemoryBroker } = require('@alexify/wrpc/broker');

const broker = new MemoryBroker();
const a = new RpcServer({ router, backplane: broker.backplane });
const b = new RpcServer({ router, backplane: broker.backplane });
```

`MemoryBroker` implements every capability in one process. It is the
reference implementation the contract tests are written against, and what
makes a multi-instance setup testable without infrastructure: two servers
sharing one `MemoryBroker` behave like two processes sharing a real broker.
Delivery is always deferred to a microtask, as a network broker's would be,
so code that accidentally relied on synchronous delivery fails in the test
suite and not after a deploy.

| Option | Default | |
| --- | --- | --- |
| `retention.maxEntries` | `10000` | Entries kept per log topic; older ones are trimmed |
| `epoch` | random | Stamped into every log id, so an id from another broker is recognized as foreign |
| `prefix` | `'wrpc'` | Backplane channel namespace |
| `logger` | `console` | `false` silences it |

## The contracts

A broker is a plain object: `{ name, backplane?, log?, queue?, direct?, close() }`.
`isBroker(value)` and `isBrokerLog`/`isBrokerQueue`/`isBrokerDirect` are the
structural checks every entry point runs.

### log

```js
const id = await log.append('orders', JSON.stringify(order), { headers, key });

const read = log.read('orders', { after: lastEventId, signal });
await read.ready;                       // the position is fixed from here on
for await (const { id, value, headers } of read) { /* … */ }
```

- `after` reads strictly after an id this log minted. An id older than the
  retained history — or minted by another log — fails the read with **410**;
  a malformed id, or one past the tip, with **400**.
- Without `after`, `from: 'latest'` (the default) reads only new entries and
  `'earliest'` everything retained.
- `parseId(text)` checks an id's syntax. A `lastEventId` comes from the peer,
  so it is refused before it reaches the broker.
- The `id` a read yields is the **resume token** after that entry. For a
  single-sequence log it is the entry's own id; for Kafka it is a vector of
  partition offsets.

### queue

```js
await queue.produce('invoices', body, { headers });

const consumer = await queue.consume('invoices', async (delivery) => {
  try {
    await handle(delivery.body);
    await delivery.ack();
  } catch {
    await delivery.retry({ delay: 1000 });
  }
}, { prefetch: 16, deadLetter: 'invoices.dlq' });
```

- Consumers of one queue **compete**: each message reaches one of them, at
  least once. `prefetch` bounds the unsettled deliveries a consumer holds, and
  they run concurrently.
- A delivery settles once — the first of `ack()`, `retry({ delay })`,
  `release()` or `deadLetter(reason)` wins:

| Settlement | Effect | `attempt` |
| --- | --- | --- |
| `ack()` | done | — |
| `retry({ delay })` | redelivered after `delay` ms | + 1 |
| `release()` | back to the queue now (a draining node handing work over) | unchanged |
| `deadLetter(reason)` | moved to the `deadLetter` queue with `x-wrpc-dead-reason` and `x-wrpc-attempt` headers | — |

- The attempt counter belongs to the adapter, not the broker: RabbitMQ 4 does
  not count a requeue, so the adapters carry it in an `x-wrpc-attempt` header
  where the broker has none.
- `stop()` takes no new deliveries; whatever was unsettled is redelivered
  later.

### direct

```js
const stop = await direct.listen('svc.billing', onMessage, { group: 'svc.billing' });
await direct.send('svc.billing', packet, { correlationId, replyTo: direct.inbox(), timeout: 5000 });
```

- `inbox()` answers a unique, routable address.
- Listeners sharing a `group` compete; ungrouped listeners each receive every
  message. Messages from one sender to one address arrive in order.
- Delivery is **at-most-once**. The RPC binding built on it numbers its frames
  and treats a gap as a lost connection. `send` may reject with **503** when the
  broker knows nobody listens, and may discard a message nobody took within
  `timeout`.

### backplane

The same three methods [Scaling](./scaling#the-contract) documents. The
broker family adds one requirement the contract tests enforce: channel names
are **literal**. A room called `room:*` must never become a wildcard
subscription, and an adapter encodes names into whatever its broker allows.

## Writing an adapter

The contracts are executable. `tests/broker/{backplane,log,queue,direct}Contract.js`
in the repository run against the memory broker, every adapter's in-repo fake,
and — in the broker CI jobs — a real server. A new adapter that passes them is
a working adapter.

Two building blocks carry the parts every adapter would otherwise get wrong:

- **`TopicTails`** keeps one live reader per topic per instance, shared by
  every local read. A feed is read by many subscriptions at once, and a broker
  consumer per subscription does not survive Kafka (a group join each) or Redis
  (a blocked connection each). The adapter supplies `live` (a positioned tail)
  and `range` (a catch-up page); `TopicTails` joins them without a gap or a
  duplicate, and sends a reader that falls behind back to `range` so memory per
  slow subscriber stays bounded.
- **`encodeToken(name, { safe, escape, maxLength })`** maps an arbitrary name
  into one token of a broker's alphabet, injectively, shortening past
  `maxLength` with a digest (AMQP routing keys stop at 255 bytes, Kafka topic
  names at 249 characters).
