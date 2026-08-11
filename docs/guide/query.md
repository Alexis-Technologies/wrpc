# TanStack Query

`@alexify/wrpc/query` binds a wrpc client to [TanStack
Query](https://tanstack.com/query). Not hooks — **option factories**, in the
shape tRPC v11 settled on:

```js
const { createQueryUtils } = require('@alexify/wrpc/query');

const wq = createQueryUtils(client, { queryClient });

useQuery(wq.queryOptions(['chat', 'list'], { room: 'a' }));
```

`queryOptions()` hands back the plain `{ queryKey, queryFn }` object you spread
into your own `useQuery`. So one file serves React Query, Solid Query, Svelte
Query, Vue Query and query-core alike — and imports **none** of them. Together
with the wrpc client being injected too, that is what keeps this subpath at
~1 KB min+gzip and free of runtime dependencies.

## Setup

```js
const { WrpcClient } = require('@alexify/wrpc');
const { createQueryUtils } = require('@alexify/wrpc/query');
const { QueryClient } = require('@tanstack/query-core');

const client = await WrpcClient.connect('wss://host/api');
await client.load('chat');

const queryClient = new QueryClient();
const wq = createQueryUtils(client, { queryClient, prefix: ['wrpc'] });
```

| Option | Meaning |
| --- | --- |
| `queryClient` | The default for `subscriptionHandler()`. Structural — anything with `setQueryData`. |
| `prefix` | Prepended to every key, so several clients can share one cache. |

## Queries

```js
useQuery(wq.queryOptions(['chat', 'list'], { room: 'a' }, { staleTime: 30_000 }));
```

The third argument is merged in first, so anything TanStack accepts passes
through. It is deliberately **not** checked against TanStack's option types —
that would mean depending on them. Assign the result to your framework's option
type if you want a typo in `staleTime` caught.

Two things happen for you:

- **TanStack's `AbortSignal` is forwarded.** A cancelled query or an unmounted
  component reaches the server as `{ type: 'cancel' }`, and the call rejects
  with code 499.
- **The path is resolved lazily**, inside `queryFn`. `client.api` only exists
  after `load()` and is rebuilt on every reconnect, so options captured before
  either would otherwise point at a dead method. The path is *validated*
  eagerly, though: a typo is a programmer error and should surface where it was
  written.

### Keys

```js
wq.queryKey(['chat', 'list'], { room: 'a' });   // ['wrpc', 'chat', 'list', { room: 'a' }]
```

Prefix-shaped on purpose: TanStack matches keys by prefix, so

```js
queryClient.invalidateQueries({ queryKey: wq.queryKey(['chat']) });
```

reaches every query of the unit, while different arguments stay in different
cache entries.

## Mutations

```js
const mutation = useMutation(wq.mutationOptions(['chat', 'send']));
mutation.mutate({ text: 'hi' });
```

The variables **are** the arguments — there is no second mapping step.

## Subscriptions

`subscriptionHandler()` opens a subscription and writes every value into the
cache, so the component reading a query reads live data with no extra wiring:

```js
useEffect(() => {
  const sub = wq.subscriptionHandler(['chat', 'onMessage'], { room: 'a' }, {
    queryKey: wq.queryKey(['chat', 'list'], { room: 'a' }),
    update: (previous, message) => [...(previous ?? []), message],
  });
  return () => sub.unsubscribe();
}, []);
```

| Option | Default | Meaning |
| --- | --- | --- |
| `queryClient` | the one from `createQueryUtils` | Required if neither was given. |
| `queryKey` | `queryKey(path, args)` | Which entry to write. |
| `update` | replace | `(previous, data) => next`. |
| `lastEventId` | — | Where to resume from. |
| `onData` / `onError` / `onEnd` | — | The usual [subscription callbacks](./subscriptions#when-callbacks-fire). |

It returns the wrpc `Subscription` handle: call `unsubscribe()` on teardown.
One call opens one subscription, and nothing here de-duplicates two callers of
the same feed.

### Four things to know

**The default `update` replaces.** Appending by default would look friendlier
and grow without bound for the life of a feed that never ends.

**Pass `queryKey` when the feed feeds a query.** The default key is the
subscription's own path, where no `queryFn` can exist — `queryOptions()`
refuses subscription paths. Either read it with `enabled: false` / `skipToken`,
or point the handler at the query that already holds the data, as above.

**A `setQueryData`-only entry has no observers**, so TanStack garbage-collects
it after `gcTime` (5 minutes in a browser; never on a server). An accumulating
`update` therefore loses its history unless something is observing the key.
That is TanStack's behaviour, not this bridge's.

**Reconnects keep delivering, but resume is not free of duplicates.** The
client re-opens its subscriptions from the last eventId it saw, on the same
record, so nothing here re-subscribes and nothing leaks. But an *untracked*
feed has no resume point and replays from the start — an accumulating `update`
would then append the replay to what it already had. Label values with
[`tracked()`](./subscriptions#resuming) server-side, or make `update`
idempotent.

## With a contract

Pass the contract type and every path, argument and result is checked:

```ts
const wq = createQueryUtils<Api>(client, { queryClient });

useQuery(wq.queryOptions(['chat', 'list'], { room: 'a' }));
//                        ^ only call paths of Api
//                                          ^ typed args, and required when the procedure needs them

wq.subscriptionHandler(['chat', 'onMessage'], { room: 'a' });
//                      ^ only subscription paths
```

`queryOptions` accepts only `CallPath<Api>` and `subscriptionHandler` only
`SubscriptionPath<Api>`, so the two can never be crossed at compile time —
and at runtime, mixing them up throws with a message that names the fix.
