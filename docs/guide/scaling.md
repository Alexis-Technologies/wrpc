# Scaling

[Rooms](./rooms) work inside one process with no configuration. Run a second
process and each one only reaches the clients it holds — a **backplane** is
what makes a room span them.

```js
const Redis = require('ioredis');
const { Server } = require('@alexify/wrpc');
const { createRedisAdapter } = require('@alexify/wrpc/scaling');

const backplane = createRedisAdapter({ pub: new Redis(url) });

const server = new Server({ router, backplane, port: 8000, protocol: 'http' });
```

That is the whole change. `server.to('lobby').emit(...)` now reaches every
member of `lobby` on every instance; handlers, clients and the wire protocol
are untouched.

::: info Redis is injected, never depended on
`@alexify/wrpc` has no dependencies and never will. You pass your own client;
the adapter duck-types it. `ioredis` is a devDependency here purely so the
integration tests have something real to talk to.
:::

## The contract

A backplane is three methods. Anything with this shape plugs in:

```js
const backplane = {
  publish(channel, message) {},                 // message is always a string
  subscribe(channel, handler) { return () => {}; }, // returns an unsubscribe
  close() {},
};
```

Each may return a promise. `isBackplane(value)` is the structural check
`RpcServer` runs, exported so you can run it yourself.

## Adapters

### Memory

```js
const { MemoryBackplane } = require('@alexify/wrpc/scaling');

const backplane = new MemoryBackplane();
const a = new RpcServer({ router, backplane });
const b = new RpcServer({ router, backplane });
```

In-process, delivery deferred to a microtask like a real broker's. It is the
reference implementation, and it is what makes multi-instance fan-out testable
without standing up infrastructure: two `RpcServer`s sharing one
`MemoryBackplane` behave like two processes sharing a Redis.

### Redis

```js
const { createRedisAdapter } = require('@alexify/wrpc/scaling');

const backplane = createRedisAdapter({
  pub: redis,               // required
  sub: redis.duplicate(),   // optional; defaults to pub.duplicate()
  prefix: 'wrpc',           // channel namespace
});
```

Modelled on the ioredis API — `publish(channel, message)`, `subscribe(channel)`
plus an `'message'(channel, message)` event, and `duplicate()`. A subscribed
Redis connection cannot publish, which is why there are two.

`close()` releases this adapter's subscriptions and its listener. A client
**you injected** is never quit — its lifetime is yours — but a subscriber the
adapter created for itself through `duplicate()` is, since nothing else holds
a reference to close it.

### Something else

NATS, MQTT, Postgres `LISTEN/NOTIFY`, a cloud pub/sub — all three methods, all
strings. The rooms layer serializes its own envelope, so an adapter never needs
to know the payload shape:

```js
const backplane = {
  publish: (channel, message) => nats.publish(channel, message),
  subscribe: (channel, handler) => {
    const sub = nats.subscribe(channel, (message) => handler(message));
    return () => sub.unsubscribe();
  },
  close: () => nats.close(),
};
```

## How it works

Every non-local emit is published as an envelope:

```json
{ "v": 1, "instance": "c0ffee…", "rooms": ["lobby"], "name": "chat/message", "data": { "text": "hi" } }
```

- `instance` identifies the publisher, and every instance **drops its own
  envelopes** — that is the echo suppression.
- `rooms` is `null` for a broadcast to everyone.
- An emit targeting **exactly one** room is published on that room's channel
  (`room:<name>`), which only instances holding members subscribe to. Anything
  else goes to the single `broadcast` channel every instance holds. So an
  envelope reaches a given instance through exactly one channel, and no
  receiver-side deduplication is needed.

Channel subscriptions follow membership: the first client to join a room here
subscribes this instance to its channel, the last one to leave unsubscribes.
The `broadcast` channel is held for the life of the process — an instance with
no rooms at all still has to hear `server.broadcast()`.

A failing backplane is isolated: a publish that throws is logged, and local
delivery happens either way. A broken Redis degrades a cluster to a set of
independent instances rather than taking the room mechanism down.

## At-most-once, and what to do about it

Delivery is **at-most-once**, deliberately. A message published while an
instance is between subscriptions, or dropped by the broker, is gone. Rooms are
a fan-out mechanism, not a queue.

If a client must not miss values, do not build that guarantee out of rooms.
Build it out of [subscriptions](./subscriptions#resuming): `tracked()` values,
an event log the handler can replay from, and the `lastEventId` the client
sends back on reconnect. Rooms tell connected clients what is happening now;
subscriptions are what let one catch up.

## Sessions

The backplane carries room events, not sessions. Two instances behind a load
balancer need a shared [session store](./sessions#stores) as well — the default
`MemorySessionStore` lives in one process, so a client that reconnects to a
different instance would arrive anonymous.

## The cluster layer

On top of room fan-out, the same backplane powers `server.cluster` —
presence, introspection and node-to-node messaging. It is always there:
without a backplane every operation degrades to its local half, so
application code never branches on the deployment.

Two more channels join `broadcast`/`room:<name>`, both held for the life of
the process: `cluster` (every instance — presence, wide requests and
commands) and `inst:<instanceId>` (one instance — its answers and addressed
commands).

### Presence: replicated, read locally

```js
server.cluster.count('chat');     // cluster-wide membership — no network
server.cluster.presence('chat');  // { total, instances: { 'node-1': 2, … } }
server.cluster.instances();       // live instance ids, this one first
```

`count()` and `presence()` are **local sums**: every join/leave publishes a
±1 delta, and a periodic snapshot (`cluster.presenceInterval`, default 5 s)
corrects whatever the at-most-once broker dropped — a lost delta heals
within one interval. A node that goes silent for `presenceTimeout` (default
3× the interval) is evicted; a graceful `close()` says goodbye and is
evicted immediately; a restart carries a fresh epoch, so its counters are
replaced, never doubled. `rooms.count(room)` and `members(room)` remain the
LOCAL numbers — zero-cost reads for code that wants exactly this instance.

### Introspection and commands

```js
const clients = await server.cluster.fetchClients({ room: 'chat' });
// [{ id, instance, rooms, data, transport, session }, …] from every node

server.cluster.join(clientId, 'ops');          // addressed: ONE instance hears it
server.cluster.leave({ room: 'chat' }, 'x');   // filter: applied on every instance
server.cluster.disconnect({ room: 'banned' });
```

`client.id` is instance-prefixed (`<instanceId>.<generateId()>`), so an
id-addressed command travels as **one message to one node** — no
cluster-wide filtering. `fetchClients` knows its respondent set from
presence and resolves the moment every live node answered; the timeout
(`cluster.requestTimeout`, default 2 s) is a backstop that resolves the
partial array with a non-enumerable `incomplete: true`, never silently.
`client.data` is the application's bag and rides along in descriptors.

### Node-to-node messaging

```js
server.cluster.sendEvent('cache/invalidate', { key });   // fire-and-forget
server.cluster.on('cache/invalidate', ({ key }) => { … }); // on OTHER nodes

server.cluster.respond('stats', async () => ({ load: cpu() }));
const { answers, errors, incomplete } = await server.cluster.ask('stats');
```

As everywhere in wrpc, `emit` is the local Emitter emit; the wire send is
`sendEvent`. `ask()` collects one answer per node — a node without a
responder contributes an error entry, not silence.

## What stays per-instance

The backplane carries events, presence and requests between instances —
not connections themselves. Two things deliberately stay local, and a
deployment has to account for them:

- **SSE channels.** A channel lives on the instance that created it, so SSE
  needs sticky routing (cookie affinity). A misrouted request answers
  `409` and the client starts over — see the [SSE guide](./sse).
- **Event logs.** `createEventLog()` is per-process memory, and its ids are
  epoch-stamped so this is *visible*: a client resuming against another
  instance (or a restarted one) presents a foreign epoch, `since()` answers
  `null`, and the handler falls back to a snapshot instead of silently
  missing events. A shared/persisted log passes its own stable `epoch`.

## Multi-room emits use the broadcast channel

An emit targeting **one** room travels on that room's own backplane channel
(only instances holding members are subscribed). An emit targeting several
rooms at once — `to('a', 'b').emit(...)` — degrades to the single broadcast
channel every instance holds, because delivering through per-room channels
would need receiver-side deduplication the protocol deliberately does not
have. Prefer single-room emits in fan-out-heavy paths.
