# NATS

NATS is the most natural fit of the four: core subjects are a backplane and a
request/reply fabric by design, and JetStream adds the durable half — feeds
and work queues — without a second system.

```js
const { connect, headers, createInbox } = require('@nats-io/transport-node');
const { jetstream, jetstreamManager } = require('@nats-io/jetstream');
const { createNatsBroker } = require('@alexify/wrpc/broker/nats');

const nc = await connect({ servers: process.env.NATS_URL });
const broker = createNatsBroker({ nc, headers, createInbox, jetstream, jetstreamManager });

const server = new Server({ router, backplane: broker.backplane, port: 8000 });
```

`headers` is a **package export**, not a method on the connection — that is
why it is injected separately. Leave `jetstream`/`jetstreamManager` out and
you get a backplane + RPC broker with no `log` and no `queue`; everything
else works unchanged.

| Option | Default | |
| --- | --- | --- |
| `nc` | — | The connection; `close()` never drains it — its lifetime is yours |
| `headers` | — | The `headers()` factory |
| `jetstream`, `jetstreamManager` | — | Both, or neither |
| `createInbox` | a uuid subject | The `createInbox()` export |
| `prefix` | `'wrpc'` | Subject and stream-name namespace |
| `ackWait` | `30000` | JetStream ack window for queue consumers (ms) |
| `stream` | `{}` | Extra stream config, e.g. `{ queue: { storage: 'memory' } }` |

## What maps to what

| Capability | NATS |
| --- | --- |
| `backplane` | core subjects: `PUB`/`SUB`, at-most-once, no persistence |
| `log` | one JetStream stream per topic; the message **sequence** is the feed's resume token |
| `queue` | a work-queue stream with one durable pull consumer per group; `max_ack_pending` is the prefetch |
| `direct` | core subjects: plain subscriptions for inboxes, **queue groups** for a service address, native reply subjects |

## Names become one subject token

A room, a topic, a queue and an address are application strings; a NATS
subject is a dot-separated hierarchy where `*` and `>` are wildcards. So every
name is encoded into exactly **one** token: a room called `room:*` subscribes
to that room and to nothing else, and `orders.eu` cannot collide with a
subject someone else meant. Stream names get the same treatment (`.` is
illegal in one).

## Sharp edges

- **A JetStream sequence carries no epoch.** An id from another stream looks
  like a future position in this one, so a feed answers `400` (past the tip)
  where a log with an epoch would say `410`. Sign the ids
  ([`brokerFeed({ secret })`](./feeds#signed-ids)) when clients may send ids
  from elsewhere.
- **Retention is yours to choose.** The adapter creates a log stream with the
  server's defaults; pass `stream.log` (`max_msgs`, `max_age`, `storage`) for
  something else. A feed resuming past a purge answers `410`, which `onGap`
  turns into a snapshot.
- **`release()` costs a republish.** JetStream's `nak` always counts a
  delivery, and a release must not, so the adapter re-publishes the message
  with its attempt carried in a header and terminates the original. `retry()`
  is the cheap path — a plain `nak(delay)`.
- **A slow handler keeps its lease.** While a delivery is in flight the
  adapter calls `working()` every `ackWait / 2`, and it stops the moment the
  consumer does — so a consumer that stops hands its messages back after
  `ackWait` rather than holding them forever.
- **Core NATS never queues.** A `direct.send` to an address nobody listens on
  is dropped by the server, as core NATS always does: the caller learns from
  its own timeout rather than a `503`.
- **Payloads are capped** (1 MiB by default). A wrpc call or a cluster
  `fetchClients` answer above it is refused by the server — raise
  `max_payload` or keep the answers small.

## Running the tests

`pnpm test` runs the NATS suites against an in-repo fake NATS + JetStream.
Against a real server:

```bash
docker compose up -d nats
NATS_URL=nats://127.0.0.1:4222 node --test tests/broker/nats.integration.test.js
```
