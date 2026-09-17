# Kafka

Kafka is the durable-log broker: feeds and work queues that keep history for
days, replayable by any consumer. wrpc uses it for `log` and `queue`, offers
a `backplane` with real costs, and refuses `direct` outright.

```js
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
const { createKafkaBroker } = require('@alexify/wrpc/broker/kafka');

const kafka = new Kafka({ kafkaJS: { brokers: process.env.KAFKA_BROKERS.split(',') } });
const broker = createKafkaBroker({ kafka });
```

`kafkajs` works the same way — `new Kafka({ brokers })` — and the adapter
detects which client it was handed. Both are **injected**: neither is a
runtime dependency of wrpc.

::: tip Which client
`@confluentinc/kafka-javascript` (librdkafka, actively released) is the one
to reach for. `kafkajs` is pure JavaScript and still widely deployed, but its
last release was 2.2.4 in February 2023 — it is not deprecated, just
inactive, and it emits timer warnings on current Node versions. The adapter
supports both and CI exercises both.
:::

| Option | Default | |
| --- | --- | --- |
| `kafka` | — | The client |
| `flavor` | detected | `'kafkajs'` or `'confluent'` when detection is wrong |
| `prefix` | `'wrpc'` | Topic namespace |
| `partitions` | `3` | Partitions for **queue** topics — the concurrency ceiling |
| `logPartitions` | `1` | Partitions for **log** topics; one keeps a feed globally ordered |
| `backplane.topic` / `.partitions` | `<prefix>.backplane` / `1` | The backplane topic |
| `maxRetryDelay` | `60000` | Cap on how long a retry's delay waits in-process |

## What maps to what

| Capability | Kafka |
| --- | --- |
| `log` | a topic per feed; the resume token is a **vector** of partition offsets (`k1:<p>=<o>,…`) |
| `queue` | a topic per queue, one consumer group, manual commits, `partitionsConsumedConcurrently` as the prefetch |
| `backplane` | one topic, the channel in a header, a unique consumer group per instance — **with caveats** |
| `direct` | — |

## No RPC over Kafka

`attachBrokerRpc` refuses a Kafka broker: a request/response carrier needs an
addressable inbox per instance and low, predictable latency, and Kafka gives
neither — a topic per instance plus a rebalance pause on every join is the
opposite of what an RPC caller wants. Use Redis, NATS or RabbitMQ for
[RPC over a broker](./rpc), alongside Kafka for feeds and queues.

## The backplane's caveats

It works, and the contract tests run against it, but know what you are
buying:

- **An instance is deaf until its group joins.** A fresh consumer group
  reading `latest` never sees what was published before the join. Rooms are
  at-most-once anyway, but the silent window is seconds, not milliseconds —
  and the broker's own `group.initial.rebalance.delay.ms` (3 s by default)
  is most of it. Set it to `0` for wrpc's groups.
- **Every instance reads every envelope.** Filtering is local (the channel is
  a header), so a big cluster pays fan-out bandwidth a Redis or NATS
  backplane does not.
- **Groups pile up.** One per instance, per boot; `close()` deletes the ones
  it created, but a crashed instance leaves one behind until
  `offsets.retention.minutes`.
- **1 MiB payload cap** by default, which a large `fetchClients` answer can
  exceed.

If Redis or NATS is available, put the backplane there and keep Kafka for
what it is good at.

## Feeds

A feed topic is **single-partition by default**: a durable feed that
reordered itself under load would be a subtle, permanent bug. Set
`logPartitions` higher when throughput matters more than global order — the
resume token is a vector precisely so a multi-partition feed still resumes
exactly, and per-key order still holds.

Readers pin their position by **seeking to a watermark captured before the
join**: a fresh group resolves `latest` at its first fetch, which can land
after the next append. Without the seek, the first entries of a live feed
would vanish now and then — a race the phase-0 spike caught.

## Queues

- **No nack.** A `retry()` is a republish carrying `x-wrpc-attempt`, and the
  delay waits in-process (capped by `maxRetryDelay`). The original message
  stays **uncommitted** until the copy is written, so a crash mid-wait
  redelivers rather than loses — at-least-once holds.
- **Ordering is lost on retry.** The republished copy goes to the end of a
  partition. Where per-key order matters, produce with a key and accept that
  a retried message trails its siblings.
- **Concurrency comes from partitions.** Within one partition Kafka is
  strictly sequential; `prefetch` becomes `partitionsConsumedConcurrently`,
  so a queue that wants 16 concurrent handlers needs at least 16 partitions.
- **`redelivered` is best-effort.** A rebalance simply rewinds an uncommitted
  offset, and Kafka carries no delivery counter — the flag is true only for
  messages the adapter itself re-published.

## Running the tests

`pnpm test` runs the Kafka suites against an in-repo fake, in **both** client
shapes. Against a real broker:

```bash
docker compose up -d kafka
KAFKA_BROKERS=127.0.0.1:9092 node --test tests/broker/kafka.integration.test.js
```
