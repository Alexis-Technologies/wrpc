# RabbitMQ

RabbitMQ covers all four [capabilities](../brokers) — with the richest queue
semantics of the four adapters (per-message TTL, dead-letter exchanges,
quorum queues) and the most opinions about how a queue should be declared.

```js
const amqp = require('amqplib');
const { createAmqpBroker } = require('@alexify/wrpc/broker/amqp');

const connection = await amqp.connect(process.env.AMQP_URL);
const broker = createAmqpBroker({ connection });

const server = new Server({ router, backplane: broker.backplane, port: 8000 });
```

The connection is **injected**: `amqplib` is a devDependency of this
repository and never a runtime one, and `broker.close()` releases the
channels it opened but never your connection.

| Option | Default | |
| --- | --- | --- |
| `connection` | — | An amqplib connection |
| `prefix` | `'wrpc'` | Exchange, queue and routing-key namespace |
| `queueType` | `'quorum'` | `x-queue-type` of work queues |
| `inboxTtl` | `60000` | `x-expires` of a service group's shared queue |
| `streamMaxBytes` | `0` | `x-max-length-bytes` on a log's stream queue |

## What maps to what

| Capability | RabbitMQ |
| --- | --- |
| `backplane` | a direct exchange; every instance has **one** exclusive auto-delete queue and one consumer channel for all of its rooms, bound and unbound per room by routing key — a room event fans out without a shared queue, and the channel count does not grow with the rooms |
| `log` | a **stream queue** (`x-queue-type: stream`); the delivery's `x-stream-offset` is the feed's resume token |
| `queue` | a quorum queue, prefetch per consumer channel, a TTL retry queue that dead-letters back into the main one, and a dead-letter queue for what is exhausted |
| `direct` | **one** direct exchange (`<prefix>.direct`), the address as the routing key: an inbox binds an exclusive queue, a service group binds one durable queue its members compete over |

The adapter declares exactly two exchanges, `<prefix>.bp` and
`<prefix>.direct`, however many rooms, addresses and inboxes it serves — the
permissions a RabbitMQ user needs are those two names plus the queues under
`<prefix>.`. An address is whatever a peer puts in `replyTo`, one per client
inbox, so anything declared *per address* would grow with every client for as
long as the broker lived.

## What RabbitMQ 4 changed, and how the adapter answers

The [phase-0 spike](../brokers#writing-an-adapter) measured both of these
against a real 4.x server:

- **`nack(requeue)` no longer counts a delivery.** `x-delivery-count` only
  grows when a consumer *fails* (its channel or connection closes), so a
  retry built on requeue would loop a poison message forever. The adapter
  therefore carries the attempt in an `x-wrpc-attempt` header and a
  `retry()` is a **republish** — through the TTL retry queue when it has to
  wait. `release()` is the plain requeue, which is exactly right: it must
  *not* count an attempt.
- **A transient non-exclusive queue is refused** with a CONNECTION-level
  `541` that takes every channel on it down. Every shared queue the adapter
  declares is durable, with `x-expires` where it should not outlive its
  consumers.

## Sharp edges

- **A channel-level error closes that channel, not the broker.** A
  declaration that does not match what the server holds (`406`, a changed
  `streamMaxBytes`), a queue deleted under a reader (`404`), a node going
  away: the adapter forgets the closed channel and opens another on the next
  call, and a queue consumer re-consumes with a backoff (`healthy` is `false`
  meanwhile, logged `broker.amqp.consumer.closed`). A closed **connection**
  is different — nothing can be re-opened on it: every binding turns
  unhealthy for good (`broker.amqp.connection`, once), and the fix is a new
  connection with a new broker on it.
- **`log.append` costs an extra round trip.** AMQP 0-9-1 never reports the
  offset a publish landed on (only the stream protocol does), so the append
  reads the tip back to answer with a real id. A high-rate producer should
  either ignore the receipt — readers yield the authoritative ids — or use a
  broker whose publish answers with the position (Redis, NATS, Kafka).
- **A catch-up page ends at the tip, not at a pause.** A stream reader has
  no "end", so a page reads the stream's last offset first and is complete
  when it reaches it — a delivery that pauses (flow control, a slow link)
  hands the page back incomplete, and the reader asks again from where it
  stopped. An empty stream is given half a second to answer "last" before
  it is taken for empty.
- **A trimmed offset is detected, not reported by the broker.** RabbitMQ
  silently starts a stream reader at the oldest retained message; the
  adapter notices that the first entry is past what was asked for and fails
  the read with `410`, which [`onGap`](./feeds#gaps-and-snapshots) turns
  into a snapshot.
- **One group per address.** Members of a group share one queue; two
  different groups on one address would compete rather than each receive.
  The RPC binding uses one group per service, which is the supported shape.
- **Retry delays share one TTL queue, and RabbitMQ expires only its head.**
  Every `retry()` republishes to `<queue>.retry` with a per-message
  `expiration`, and a message behind one with a longer delay waits for that
  one to expire first. The **default** policy is exactly that mix — an
  exponential backoff with full jitter, 1 s → 60 s — so on RabbitMQ a retry
  due in 300 ms can sit behind one due in 8 s (the largest delay five
  attempts produce), and behind one of up to `max` with more attempts. The
  delays are a lower bound here, not a schedule. Where the timing matters,
  make them equal, so that nothing can be behind anything later than
  itself:

  ```js
  await attachConsumers(server, broker, {
    orders: { target: 'orders.v1/process', retry: { backoff: { base: 5000, max: 5000, factor: 1, jitter: false } } },
  });
  ```

  The retry queue is a **classic** queue: durable, but not replicated — a
  retry waiting there is lost with the node that holds it, where the work
  queue and the dead-letter queue, both quorum, survive. The other adapters
  delay a retry per message and have neither edge.

## Running the tests

`pnpm test` runs the AMQP suites against an in-repo fake RabbitMQ. Against a
real server:

```bash
docker compose up -d rabbitmq
AMQP_URL=amqp://127.0.0.1:5672 node --test tests/broker/amqp.integration.test.js
```
