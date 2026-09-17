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
| `backplane` | a direct exchange; every instance binds its **own** exclusive auto-delete queue per channel, so a room event fans out without a shared queue |
| `log` | a **stream queue** (`x-queue-type: stream`); the delivery's `x-stream-offset` is the feed's resume token |
| `queue` | a quorum queue, prefetch per consumer channel, a TTL retry queue that dead-letters back into the main one, and a dead-letter queue for what is exhausted |
| `direct` | a fanout exchange per address: an inbox binds an exclusive queue, a service group binds one durable queue its members compete over |

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

- **`log.append` costs an extra round trip.** AMQP 0-9-1 never reports the
  offset a publish landed on (only the stream protocol does), so the append
  reads the tip back to answer with a real id. A high-rate producer should
  either ignore the receipt — readers yield the authoritative ids — or use a
  broker whose publish answers with the position (Redis, NATS, Kafka).
- **A trimmed offset is detected, not reported by the broker.** RabbitMQ
  silently starts a stream reader at the oldest retained message; the
  adapter notices that the first entry is past what was asked for and fails
  the read with `410`, which [`onGap`](./feeds#gaps-and-snapshots) turns
  into a snapshot.
- **One group per address.** Members of a group share one queue; two
  different groups on one address would compete rather than each receive.
  The RPC binding uses one group per service, which is the supported shape.
- **Mixed retry delays share one TTL queue.** A message with a long delay at
  the head blocks shorter ones behind it (RabbitMQ expires from the head).
  Keep the backoff schedule uniform, or give slow retries their own queue.

## Running the tests

`pnpm test` runs the AMQP suites against an in-repo fake RabbitMQ. Against a
real server:

```bash
docker compose up -d rabbitmq
AMQP_URL=amqp://127.0.0.1:5672 node --test tests/broker/amqp.integration.test.js
```
