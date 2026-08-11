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
