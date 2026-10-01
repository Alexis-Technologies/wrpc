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
| `replicationFactor` | `-1` | Replication factor of the topics the adapter creates; `-1` is the broker's `default.replication.factor` (Kafka 2.4+) |
| `backplane.topic` / `.partitions` | `<prefix>.backplane` / `1` | The backplane topic |
| `maxRetryDelay` | `60000` | Cap on how long a retry's delay waits in-process |
| `maxCatchUp` | `4` | Catch-up pages of a feed read at once — each is a consumer group of its own; the rest wait their turn |

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

## Topics

The adapter creates the topics it needs on first use — one per feed, one per
queue, one for the backplane — and logs each creation once
(`broker.kafka.topic`, with the partition count and replication factor). It
never alters a topic that exists: the partition count and the replication
factor stay what the topic was created with, whoever created it.

Replication is the cluster's to decide. The default `replicationFactor` of
`-1` asks for the broker's `default.replication.factor`, so a production
cluster configured for three replicas and `min.insync.replicas=2` gets
exactly that, and a single-node development broker gets one. Pass a number
only when the topics wrpc creates should differ from the cluster's default,
or create them ahead of time (IaC, `kafka-topics.sh`) with the settings
you want — the adapter will find and use them.

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

A catch-up page is complete only once every partition it wanted reached the
high watermark read before the page started; its window opens after the
group joined and the seek landed, so a slow rebalance cannot cut a page short
and pass it off as the tip. What retention already took is not waited for.

Every reader is its own consumer group — a live tail's for as long as it
runs, a catch-up page's for the page — deleted the moment the reader is
done (a page's group used to wait for `close()`; a failed read used to leave
its consumer joined). The price of a resume is therefore a **group join per
page** of catch-up, which on a broker with the default
`group.initial.rebalance.delay.ms` (3 s) is three seconds per 256 entries:
set it to `0` on a broker that serves feeds, or size the page. A queue's
group is durable and is never deleted by wrpc: its committed offsets are the
queue's progress.

A resume **storm** — a deploy, and every client of the instance that went
resumes at once — is bounded two ways. Readers asking for the same page
(the same topic, the same resume token, the same page size: a room that
lost one instance) share **one** read. And no more than `maxCatchUp` pages
(4) are read at the same time; the rest wait, first come first served,
so the coordinator sees four group joins at a time rather than a thousand.
The price is latency under the storm — a queue instead of a stampede —
which is the trade to make: raise `maxCatchUp` on a cluster that takes
more.

## Queues

- **No nack.** A `retry()` is a republish carrying `x-wrpc-attempt`, and the
  delay waits in-process (capped by `maxRetryDelay`), in steps of three
  seconds that each end in a heartbeat, so a long wait does not look like a
  dead member to the group coordinator. The original message stays
  **uncommitted** until the copy is written, so a crash mid-wait redelivers
  rather than loses — at-least-once holds. A heartbeat that fails mid-wait is
  a rebalance: the message is left to the partition's new owner
  (`broker.kafka.rebalanced`), neither republished nor committed here.
- **A refused settlement seeks back.** A copy the producer cannot write or a
  commit the group will not take is tried three times; then the consumer
  seeks the partition back to that offset and reports `broker.kafka.settle`
  — `healthy` is `false` until a settlement lands again, and nothing is ever
  committed past the message.
- **Ordering is lost on retry.** The republished copy goes to the end of a
  partition. Where per-key order matters, produce with a key and accept that
  a retried message trails its siblings.
- **Concurrency comes from partitions.** Within one partition Kafka is
  strictly sequential; `prefetch` becomes `partitionsConsumedConcurrently`,
  so a queue that wants 16 concurrent handlers needs at least 16 partitions.
  The confluent client spins its workers up once the group has assigned it
  partitions and fetches one message at a time until it has measured the
  handler, so the first messages after a slow join (the broker's default
  3 s rebalance delay) run with less concurrency than asked for; it settles
  within a second.
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
