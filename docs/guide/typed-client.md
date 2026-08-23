# Typed client

wrpc has **no TypeScript at runtime**. The typed client is types only: you
declare the api once as an ordinary interface and thread it through
`connect<Api>()`. Nothing is generated, nothing is checked at runtime — what
you buy is autocompletion and a compile error on a typo.

```ts
import { connect, type SubscriptionContract } from '@alexify/wrpc';

interface Api {
  chat: {
    send(args: { text: string }): Promise<{ id: string }>;
    onMessage: SubscriptionContract<{ room: string }, { text: string }>;
  };
  'auth.v1': { signIn(args: { login: string }): Promise<{ token: string }> };
}

const client = await connect<Api>('wss://host/api');
await client.load('chat');

const { id } = await client.api.chat.send({ text: 'hi' });
```

`connect` is a one-line alias of `WrpcClient.connect`, added because a
*function* is where a type argument reads naturally. Both accept the type
argument; use whichever you prefer.

## Writing a contract

A unit is an object of members. A **call** is a method taking one args object
and returning a promise; a **subscription** is declared with
`SubscriptionContract<Args, Data>`:

```ts
interface Api {
  math: {
    double(args: { n: number }): Promise<number>;
    ping(): Promise<void>;                                    // no arguments
  };
  feed: {
    ticks: SubscriptionContract<{ room: string }, { n: number }>;
  };
  'auth.v1': { signIn(args: { login: string }): Promise<{ token: string }> };
}
```

Versioned units are just keys with a dot, exactly as they are on the wire and
in `load()`.

What the mapping gives you:

- A call keeps its declared arguments and **gains** the trailing
  `CallOptions` — which is what carries `{ signal }`.
- Its result is awaited whether the contract promised one or not.
- A `SubscriptionContract` member becomes `{ subscribe, iterate }` rather than
  something callable, so calling a subscription is a compile error.
- `load()` only accepts unit keys the contract declares.

```ts
const value = await client.api.math.double({ n: 2 });                    // number
const sub = client.api.feed.ticks.subscribe({ room: 'a' }, { onData });  // Subscription
for await (const tick of client.api.feed.ticks.iterate({ room: 'a' })) {} // { n: number }
```

## Three sharp edges

**A zero-argument member keeps its args slot.** Slot 0 on the wire is always
the procedure's arguments, so collapsing the tuple would compile a call that
ships `{"signal":{}}` as the args and silently drops the cancellation:

```ts
await client.api.math.ping(undefined, { signal });   // ✅
await client.api.math.ping({ signal });              // ❌ compile error
```

**A member with two parameters is rejected.** A wrpc procedure receives exactly
one args object, so anything else maps to `InvalidContractMember`, whose text
the compiler quotes back at the call site:

```ts
interface Bad { unit: { method(a: string, b: number): Promise<void> } }
//                     ^ 'wrpc: a contract member is `(args) => Promise<T>` or a SubscriptionContract'
```

**A contract key named `on` is not mapped.** A unit is an `Emitter` at runtime,
so `api.chat.on` has to stay the listener registration:

```ts
client.api.chat.on('message', (data) => {});   // always the event listener
```

## Typed events

Two reserved contract keys type the realtime surface — declarations only,
zero runtime bytes:

```ts
interface Api {
  chat: {
    send(args: { text: string }): Promise<{ id: string }>;
    // server -> client: what `api.chat.on(...)` delivers, and what the
    // server's ask (`client.respond`) carries. The function form encodes
    // the ask's answer type; a bare payload type fits fire-and-forget.
    events: {
      message: (data: { text: string; from: string }) => void;
      confirm: (data: { id: string }) => boolean;
    };
    // client -> server: what `client.sendEvent('chat/typing', ...)` carries
    // (the router's inbound `on` handlers receive it).
    sends: { typing: { on: boolean } };
  };
}
```

With `events` declared, the unit emitter narrows: `api.chat.on('message',
(data) => ...)` types `data`, and an undeclared name is a compile error.
`client.respond('chat/confirm', handler)` types the payload **and** the
answer; `client.sendEvent('chat/typing', data)` types the payload.
Undeclared names on `sendEvent`/`respond` stay allowed (they degrade to
`unknown`), so partial contracts keep working.

The server side declares both halves in the router — inbound handlers under
the reserved `on` key as always, outbound payloads under the
declaration-only `emits` key (`signature`-language descriptors) — and both
travel through introspection, so [`wrpc types`](./cli) generates these
blocks for you.

## Without a contract

`connect(url)` with no type argument behaves exactly as it always did:
`client.api` is a loose record and nothing tightens under you. That is what the
`IsAny` guard in the types is for, and `tests/index.test-d.ts` asserts it.

## Utility types

Exported for building your own helpers on top:

| Type | What it gives |
| --- | --- |
| `TypedApi<Api>` | The whole mapped api — what `client.api` is. |
| `TypedUnit<Unit>` / `TypedMethod<T>` | One unit / one member, mapped. |
| `TypedSubscriptionMethod<Args, Data>` | What a `SubscriptionContract` becomes. |
| `InferArgs<T>` / `InferResult<T>` | A member's argument and result types. |
| `FirstArg<Params>` | The first element of a parameter tuple, `void` when empty. |
| `ContractArgs` / `TypedParams` | The parameter-tuple plumbing behind the mapping. |
| `UntypedApi` / `IsAny<T>` / `InvalidContractMember` | The escape hatch, the `any` guard, the error message. |
| `UnitEvents<Unit>` / `TypedUnitEmitter<Events>` | The declared `events` map and the narrowed unit emitter. |
| `ServerEventName/Data/Answer<Api, …>` / `ClientSendName/Data<Api, …>` | The `unit/event` name and payload plumbing behind typed `respond`/`sendEvent`. |

## Generating the contract

Writing the interface by hand gives you exact types. When you would rather not
write it at all, generate it from a running server — the CLI emits the same
shape of interface:

```bash
npx wrpc types http://localhost:8000/api --out api.d.ts
```

See [Codegen CLI](./cli). The two halves meet in the middle: generate, then
hand-edit if you want something more precise than the server's `signature`
descriptors can express.

## Static introspection

`load()` asks the running server what a unit looks like — one
`system/introspect` round-trip, repeated on every reconnect. When the contract
is generated anyway, the *runtime* shape can be generated too: the same CLI
run emits the raw introspection as an importable module, and `client.use()`
scaffolds from it with **no wire traffic at all**:

```bash
wrpc types http://localhost:8000/api --out api.d.ts --schema api.static.js
```

```js
import { connect } from '@alexify/wrpc';
import type { Api } from './api.js';
import schema from './api.static.js';

const client = await connect<Api>('wss://example.com/api');
client.use(schema); // synchronous — client.api is complete right here
```

The rules, in the order they matter:

- **`use()` is synchronous and offline.** It works before `open()` and
  without a reachable server — the artifact is the contract, exactly as the
  generated `.d.ts` is.
- **Dynamic wins.** `load()`ing a unit supersedes its static scaffold and the
  unit reloads on reconnect from then on; `use()` on an already-`load()`ed
  unit is a no-op. The two coexist: static for the stable core, `load()` for
  anything you want introspected live.
- **Static units never re-introspect.** A reconnect re-loads `load()`ed units
  only, and the `'reconnect'` event's `units` payload lists only those.
- **Pre-validation compiles from the artifact's schemas** (when you inject a
  client-side ajv) and does not update until you regenerate — contract drift
  is the codegen's responsibility, the same trade a generated `.d.ts` makes.
- A server can keep schemas out of the artifact with
  `introspection: { schemas: false }`, which shrinks it to names and access.

## Server-side types

The router side is typed too — `procedure()`, `defineRouter()`, `Context`,
`Client`, `Session` and the packet shapes all come from the hand-maintained
`index.d.ts`. A handler's `context` and `args` are contextually typed, and
`args` is deliberately loose: what actually constrains it is the `input`
validator, which is a runtime thing.

```ts
import { defineRouter, procedure, type Context } from '@alexify/wrpc';

const router = defineRouter({
  chat: {
    send: procedure({
      input: (args: unknown) => schema.parse(args),
      handler: async (context: Context, args: { text: string }) => ({ id: '1' }),
    }),
  },
});
```
