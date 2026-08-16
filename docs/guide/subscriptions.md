# Subscriptions

A subscription is a procedure that answers with **many** values instead of one.
Write it as an async generator:

```js
const { defineRouter, procedure, tracked } = require('@alexify/wrpc');

const router = defineRouter({
  chat: {
    onMessage: procedure.subscription({
      access: 'session',
      handler: async function* (context, { room }, { lastEventId, signal }) {
        for await (const message of feed(room, { signal })) {
          yield tracked(message.id, message);
        }
      },
    }),
  },
});
```

`procedure.subscription()` is the explicit spelling; an async generator handler
is detected on its own, so plain `procedure({ handler: async function* ... })`
works too. The explicit form is required only when the handler is a normal
function returning an async iterable.

On the wire that is `subscribe` → `data`* → `end`. Subscriptions need a
connection that stays open, so they are refused on plain HTTP with code `400`.

## Consuming one

Two shapes, same feed. A callback handle:

```js
const sub = client.api.chat.onMessage.subscribe(
  { room: 'lobby' },
  {
    onData: (message) => render(message),
    onError: (error) => console.error(error.code, error.message),
    onEnd: () => console.log('the feed ended'),
  },
);

sub.unsubscribe();
```

…or a `for await` loop:

```js
for await (const message of client.api.chat.onMessage.iterate({ room: 'lobby' })) {
  render(message);
  if (message.final) break;   // breaking out unsubscribes, server-side included
}
```

`iterate()` accepts an `AbortSignal` as well; aborting it unsubscribes and ends
the iterator. The `Subscription` handle it returns is on the iterator as
`.subscription`.

The handle carries what you need to reason about a live feed:

| Member | Meaning |
| --- | --- |
| `id` | The subscription id, as it appears on the wire. |
| `lastEventId` | The last **tracked** id seen — what a reconnect resumes from. |
| `closed` | Whether it is over. |
| `unsubscribe()` | Ends it. |

### When callbacks fire

Exactly one of `onEnd`/`onError` fires, and **only for an ending the caller did
not ask for**:

- The handler finished, or refused → `onEnd`, or `onError` with the code.
- `client.close()` took the whole client down → `onEnd`.
- You called `unsubscribe()` → **nothing**. You already know.
- The connection dropped → **nothing**. A reconnect is not an ending; the
  client re-opens its subscriptions from the last eventId it saw.

## Backpressure

The pump waits for the transport to drain before pulling the next value from
the generator. A slow consumer therefore stops the producer, rather than
filling server memory with values nobody is reading — which is the whole point
of expressing a feed as a generator instead of a callback firehose.

Honour `signal`. It is aborted on unsubscribe, on disconnect, and on server
shutdown, and it is what lets the generator's `finally` run and release
whatever it opened:

```js
handler: async function* (context, args, { signal }) {
  const source = openUpstream({ signal });
  try {
    yield* source;
  } finally {
    source.close();
  }
},
```

A handler that ignores `signal` keeps running after its peer is gone, and its
values are dropped rather than delivered.

## Bridging a callback API

Most real sources push at you (`emitter.on(...)`, a Redis subscriber, a
database change feed). `createEventStream()` is the push → pull adapter:

```js
const { createEventStream } = require('@alexify/wrpc');

handler: async function* (context, { room }, { signal }) {
  const stream = createEventStream({ signal, highWaterMark: 1000 });
  const listener = (message) => stream.push(message);
  bus.on(room, listener);
  try {
    yield* stream;
  } finally {
    bus.off(room, listener);
  }
},
```

Its queue is **bounded**: a producer that outruns the consumer drops the
**oldest** value and counts it in `stream.dropped`. That is a deliberate
choice — an unbounded queue in front of a slow client is just a slower memory
leak. Read `dropped` if your feed needs to tell the client it fell behind.

## Resuming

Three separate pieces make resume work, and they are separate on purpose:

- **`tracked(id, data)`** labels one value with an id the client remembers.
  Untracked values have no resume point — the right default for a live-only
  feed.
- **`lastEventId`** is what the client sends back on re-subscribe, and it
  arrives as the handler's third argument.
- **`createEventLog({ size })`** is a bounded replay buffer for the values you
  might have to send again.

```js
const { createEventLog, tracked } = require('@alexify/wrpc');

const log = createEventLog({ size: 500 });
bus.on('message', (message) => log.push(message));   // ids are assigned here

procedure.subscription({
  handler: async function* (context, args, { lastEventId, signal }) {
    const missed = log.since(lastEventId);
    if (missed === null) {
      // The gap is older than the buffer: send a snapshot instead of
      // pretending nothing was lost.
      yield { type: 'snapshot', items: await loadAll() };
    } else {
      yield* missed;
    }
    yield* live(signal);
  },
});
```

`since()` answers with what was missed, `[]` when nothing was, and **`null`
when the id has fallen out of the buffer**. That third case is why it is not
just an array: silently skipping a gap is the one outcome a resumable feed must
never produce.

The client re-subscribes automatically after a reconnect, sending the last
tracked id it saw. Nothing extra to write.

## Limits

`maxSubscriptions` (256 per client by default) caps concurrent subscriptions;
past it, a `subscribe` is refused with code `429` on the `end` packet. Raise it
on the server:

```js
new Server({ router, maxSubscriptions: 1000 });
```

## Across instances

A subscription is per-connection, so it needs nothing special to scale — but a
feed that fans out from a room does. See [Scaling](./scaling) for the backplane
that carries room events between instances, and note that its delivery is
**at-most-once**: `lastEventId` and an event log are what turn that into a
feed a client can trust.

## Event log ids carry an epoch

`createEventLog()` mints ids as `<epoch>.<n>` — a monotonic counter stamped
with which log (which process incarnation) produced it. The epoch is random
per instance by default: after a restart, or against another instance, a
client's `lastEventId` belongs to a foreign epoch and `since()` answers
`null` — the honest "cannot resume, take a snapshot" — instead of a numeric
coincidence silently pretending nothing was missed. A log persisted or
shared between processes passes its own stable `epoch`:

```js
const log = createEventLog({ size: 1000, epoch: 'feed-v1' });
```
