# Redis

Redis covers all four [capabilities](../brokers): pub/sub for the backplane,
Streams for durable feeds and queues, and pub/sub plus a list for RPC. It is
the least new infrastructure of any option here — most deployments already
have one.

```js
const Redis = require('ioredis');
const { createRedisBroker } = require('@alexify/wrpc/broker/redis');

const broker = createRedisBroker({ client: new Redis(process.env.REDIS_URL) });

const server = new Server({ router, backplane: broker.backplane, port: 8000 });
```

The client is **injected**, as everywhere in wrpc: `ioredis` is a
devDependency of this repository and never a runtime one. Anything with the
same command surface works — Valkey, KeyDB and Dragonfly need no adapter of
their own.

| Option | Default | |
| --- | --- | --- |
| `client` | — | Runs every command; **never** quit by `close()` |
| `subscriber` | `client.duplicate()` | The connection subscriptions use |
| `connect` | `client.duplicate()` | Opens the extra connections blocking reads need |
| `prefix` | `'wrpc'` | Key and channel namespace |
| `blockMs` | `1000` | How long a blocking read parks before looping |
| `claimIdleMs` | `60000` | Idle time after which a stopped consumer's messages are claimed |
| `maxLen` | `0` | `XADD MAXLEN ~ n` on every log append; `0` never trims |
| `inboxTtl` | `60000` | TTL of an RPC group's delivery list and presence key |

## What maps to what

| Capability | Redis |
| --- | --- |
| `backplane` | `PUBLISH` / `SUBSCRIBE` (the same adapter [`@alexify/wrpc/scaling`](../scaling#redis) ships) |
| `log` | Streams: `XADD`, `XRANGE` for catch-up, one blocking `XREAD` per instance for the tail |
| `queue` | Streams + consumer groups: `XREADGROUP`, `XACK`+`XDEL`, `XAUTOCLAIM` for what a stopped consumer held, a sorted set for delayed retries |
| `direct` | `PUBLISH` for inboxes; a list (`RPUSH`/`BLPOP`) for a competing group |

Stream ids (`<ms>-<seq>`) are the feed resume tokens, so a subscriber resumes
exactly where it stopped, on any instance.

## Connections

Redis blocks a whole connection while it waits, so the adapter opens more than
the one you inject:

- one for the shared log tail (all topics multiplexed into a single
  `XREAD BLOCK`);
- one per queue consumer (`XREADGROUP BLOCK`);
- one per RPC service group (`BLPOP`);
- one for subscriptions, shared by the backplane and by RPC inboxes.

It opens them with `client.duplicate()` (or your `connect`) and quits only the
ones it opened. Budget accordingly on a managed Redis with a connection cap.

## Trimming and retention

A log topic is a stream that grows until something trims it. Set `maxLen` for
an approximate cap per append, or run `XTRIM` on your own schedule — the
[feed](./feeds) answers `410` (and calls `onGap`) when a client resumes from
an id the trim has passed.

A queue's stream, by contrast, trims itself: an acked, retried or
dead-lettered message is `XDEL`ed, so a queue that keeps up stays small. A
delayed retry rides a sorted set until it is due, which any instance may
promote.

## Sharp edges

- **A consumer group starts at the beginning.** The first consumer of a queue
  creates its group at `0`, so work produced before it existed is delivered.
  The group's position then lives in Redis and survives restarts.
- **One RPC group per address.** Competing listeners share one list, so two
  different groups on one address would compete rather than each receive. The
  binding uses one group per service, which is exactly the supported shape.
- **A message for a group nobody serves waits on the list** until its TTL
  (`inboxTtl`, shortened by an RPC request's own timeout) expires. With no
  listener at all, `send` refuses with `503` instead — the presence key is how
  the sender can tell.
- **Pub/sub is at-most-once**, which is what the backplane's
  [loss detection](../scaling#loss-detection) exists for. Feeds and queues are
  Streams and do not share that property.

## Running the tests

`pnpm test` runs the Redis suites against an in-repo fake. Against a real
server:

```bash
pnpm redis:up
REDIS_URL=redis://127.0.0.1:6379 node --test tests/broker/redis.integration.test.js
```
