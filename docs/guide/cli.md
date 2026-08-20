# Codegen CLI

A running server already describes itself. `wrpc types` turns that description
into the same shape of contract interface you would otherwise hand-write for
[`connect<Api>()`](./typed-client):

```bash
npx wrpc types http://localhost:8000/api --out api.d.ts
```

```ts
import type { Api } from './api';
import { connect } from '@alexify/wrpc';

const client = await connect<Api>('wss://localhost:8000/api');
```

The command talks plain HTTP — one `POST` carrying a `system/introspect` packet
— so it needs no socket, no client library and no running build.

## Usage

```
wrpc types <url> [options]

Arguments:
  <url>                 The server's wrpc endpoint, e.g. http://localhost:8000/api

Options:
  --out <path>          Write the declaration file here; '-' writes to stdout
  --units <a,b>         Only these units (repeatable, or comma-separated)
  --interface <name>    Name of the generated interface (default: Api)
  --package <specifier> Where to import SubscriptionContract from (default: @alexify/wrpc)
  --schema <path>       Also write the raw introspection as a module for
                        client.use(); '-' writes to stdout
  --format <cjs|esm>    Module format for --schema (default: cjs)
  -h, --help            Show this help
  -v, --version         Show the wrpc version
```

```bash
wrpc types http://localhost:8000/api --units chat,auth.v1 --out src/api.d.ts
wrpc types http://localhost:8000/api --out api.d.ts --schema api.static.js
wrpc types http://localhost:8000/api --out -          # stdout, for piping
```

`--package` matters in a monorepo that re-exports wrpc under its own name: the
generated file imports `SubscriptionContract` from wherever you say.

`--schema` writes a second artifact from the same fetch: the raw
`system/introspect` result as an importable module (`module.exports = {...}`,
or `export default {...}` with `--format esm`). It is what
[`client.use()`](./typed-client#static-introspection) consumes to scaffold the
api with no introspection round-trip — the runtime counterpart of the
generated types. One stdout per run: `--schema -` and `--out -` cannot be
combined.

## Describing procedures

Generated types are only as precise as the `signature` descriptors your router
carries. A procedure with none is emitted honestly as
`(args?: unknown) => Promise<unknown>` — which still buys you the method names,
the units and the call/subscription split.

```js
procedure({
  meta: { description: 'List the messages in a room' },
  signature: {
    args: { room: 'string', 'limit?': 'number' },
    returns: [{ id: 'string', text: 'string' }],
  },
  handler: async (context, { room }) => load(room),
});
```

becomes

```ts
export interface Api {
  chat: {
    /** List the messages in a room */
    list(args: { room: string; limit?: number }): Promise<Array<{ id: string; text: string }>>;
  };
}
```

A shape is one of three things — a type name (`'string'`, `'number[]'`,
`'string|null'`), a field map (keys ending in `?` are optional, and they nest),
or a one-element array meaning "an array of that". The type names are exactly
`string`, `number`, `boolean`, `object`, `null`, `unknown`, `any` and `never`.
There is no `Date`, because there is no `Date` in JSON. The full grammar is in
[the protocol reference](../reference/protocol#the-signature-descriptor).

`meta.description` is the **one** home for prose: the CLI turns it into a doc
comment, so it does not also live in the signature.

Subscriptions use `data` rather than `returns` and come out as
`SubscriptionContract<Args, Data>`:

```js
procedure.subscription({
  signature: { args: { room: 'string' }, data: { text: 'string' } },
  handler: async function* (context, { room }) { /* ... */ },
});
```

```ts
onMessage: SubscriptionContract<{ room: string }, { text: string }>;
```

Getting it the wrong way round (`returns` on a subscription, `data` on a call)
is a warning, not a silent misgeneration.

## Signatures are not validation

A signature says what a procedure **looks like**; `input`/`output` are what
actually enforce anything. The two are deliberately separate — a schema library
is your choice, while this has to survive a JSON round trip and end up inside a
file someone compiles.

Which is also why the descriptor format is a small **closed** set rather than an
open schema language. Every byte of it arrives over the network, so nothing is
interpolated verbatim: names are JSON-quoted, type names are matched against an
allowlist, nesting is depth-capped, and anything unrecognised degrades to
`unknown` with a warning. A hostile server can make the output useless; it
cannot make it dangerous.

## Determinism

Units and methods are emitted sorted, so re-running the command produces a
byte-identical file. A diff means the server changed — which makes the
generated file worth committing and checking in CI:

```bash
wrpc types "$WRPC_URL" --out api.d.ts && git diff --exit-code api.d.ts
```

## How it is built

`bin/wrpc.js` is a shim; the logic lives in `src/cli/types.js`, where the
coverage thresholds reach it. Everything beyond `argv` is injected — stdout,
stderr, `fetch`, the file writer, the version string — so the command can be
driven in-process with no network and no filesystem:

```js
await main(['types', 'http://host/api', '--out', '-'], { log, error, fetch, write });
```

That module is internal (there is no `@alexify/wrpc/cli` subpath), but the
shape is worth knowing if you want to read the tests: `tests/cli/types.test.js`
drives `main` directly, and `tests/cli/e2e.test.js` stands up a real server,
runs the actual binary, and type-checks the declaration file it wrote with
`tsc --noEmit`.
