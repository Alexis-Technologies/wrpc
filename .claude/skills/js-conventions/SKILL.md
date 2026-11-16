---
name: js-conventions
description: Apply the Alexis JavaScript style (Metarhia-derived, linted by oxlint and formatted by oxfmt). Use when writing or editing .js, .mjs, .ts files (with certain corrections for typescript), formatting code, or when the user asks about code style or linting.
---

<!-- Adapted from metaskills v1.0.5 (skills/js-conventions) for the Alexis toolchain and for wRPC:
     oxlint + oxfmt instead of ESLint + Prettier, pnpm instead of npm, 120 columns instead of 80,
     'use strict' in every src file, and comments that explain why.
     Where upstream (and the sibling projects) ban array methods from src/ outright, wRPC draws the
     line between HOT and COLD paths — and on a hot path it preallocates where they build packed.
     The wRPC-specific rules (the protocol revision, no runtime dependencies, byte budgets, the
     benchmark a hand-optimization cites) live in CLAUDE.md, which wins if the two ever disagree. -->

# JavaScript Code Style

Use following conventions for JavaScript and TypeScript code.

## Preparations

- Linting is oxlint (`.oxlintrc.json`) and formatting is oxfmt (`.oxfmtrc.json`, `.editorconfig`);
  never add ESLint or Prettier
- The package manager is pnpm; the scripts are already in package.json:
  - `pnpm lint`: oxlint over `src`, `tests`, `scripts`, `bench`, `bin` and `examples`
  - `pnpm format`: oxfmt rewrites those files in place
  - `pnpm format:check`: oxfmt check only, as CI runs it
- `docs/` is covered by neither; `pnpm docs:build` is its gate (a dead link fails it)
- Before and after code analysis with AI run `pnpm lint` and `pnpm test`
- Use `pnpm format` to fix formatting; fix lint errors in the code, and suppress a rule only for an
  intentional construct with a targeted `// oxlint-disable-next-line <rule>`

## Formatting

- Use single quotes and semicolons
- Keep max line length 120 (ignore URLs)
- Use trailing commas in multiline arrays, objects, and params
- Keep one empty line between semantic blocks
- Follow oxfmt and `.editorconfig` for spacing, braces, and indentation (2 spaces, LF)

## Modules

- Use CommonJS: `require` and `module.exports`, no `import`/`export` in `.js` sources; there is no
  build step, `src/` is what gets published
- Start every `src/` file with `'use strict'`
- Use the `node:` prefix for Node.js built-in modules (`require('node:crypto')`)
- Always include the `.js` extension in relative requires (`require('./streams.js')`)
- No runtime dependencies: the only bare `require` allowed in `src/` is a `node:` built-in, and none
  at all in a file that reaches a browser bundle. A framework, an engine or a broker client is
  **injected** and checked structurally (duck typing, never `instanceof` against an imported class);
  anything reused from another package is copied in, not installed
- A platform split is a matched pair — `name.js` and `name.browser.js` (dot, not dash) — swapped
  through `package.json#browser`; a new subpath is a root shim, a hand-written root `.d.ts`, an
  `exports` entry, `files` additions and a `tests/<name>.test-d.ts`, all in one change
- Layers do not reach up: `src/rpc/` never requires `src/websocket/`, `src/webrtc/` or `src/broker/`

## Naming

- Use `camelCase` for variables and parameters
- Use `UpperCamelCase` for classes and types
- Use `UPPER_SNAKE_CASE` for constants
- Use `new` with capitalized constructor names
- Use `error` in catch blocks, not `err`
- A `catch` that does nothing says why, in a comment inside it (`no-empty` with `allowEmptyCatch: false` enforces it); so does a `.catch(() => {})`, on the line above
- Use short names (`e`, `i`) only in short one-line callbacks
- Use descriptive names in multiline callbacks (`event`, `index`, `item`)
- Use boolean prefixes (`is`, `has`, `can`, `should`) for new locals and functions; a public
  property keeps the name its API has (`closed`, `streaming`, `persistent`, `binary`)
- Use singular names for entities and plural names for collections
- Use verb names for functions and handlers (`parse`, `create`, `handle`, `on`)
- Include units in numeric names (`timeoutMs`, `sizeBytes`) except options, which are milliseconds
  and bytes by convention (`callTimeout`, `maxStreamBuffer`)

## Best practices

- Prefer `const`, minimize `let`, never use `var`
- Prefer arrow functions (except prototype/class methods)
- Do not mutate input parameters; avoid `arguments`
- Keep functions small and single-purpose
- Decompose long expressions into intermediate variables
- **Hot paths are hand-written; cold paths stay idiomatic.** A path is hot when it runs per
  WebSocket frame, per packet, per batch element, per stream chunk, per broadcast recipient or per
  SSE line. There: a manual indexed loop, `new Array(len)` filled by index and truncated with
  `result.length = index`, `for (const key in obj)` over an object this package built, no closure
  or `RegExp` created per call. Everywhere else — startup, router definition, introspection,
  reconnect, handshake, CLI — `.map`/`.filter`/spread/`Object.entries` are the preferred spelling
- Never spread a data array into arguments — `out.push(...items)`, `Math.max(...values)`,
  `fn(...array)`: past the engine's argument limit it throws `RangeError`
- A queue that is read from its head is never an `Array` with `shift()`: past a few thousand
  elements every `shift` is a copy of the rest, and a peer decides how long the queue is — keep an
  index cursor or a ring
- Both rules are executable: every `.shift()` / `.unshift(` and every spread into a call in `src/`
  is listed with its bound in `HEAD_READS` / `CALL_SPREADS` (`tests/package/consistency.test.js`).
  A new one fails the suite — use the `Deque` of `src/deque.js` or an indexed loop, or add the
  line with the reason it is bounded
- A collection a peer can grow has a cap (a count, bytes, a TTL), and the cap counts what holding
  an element costs, not only its payload
- Fire-and-forget is `return void fn()` (the Metarhia spelling; `no-void` is deliberately off)
- No labelled `break`/`continue` (`no-labels`, `allowLoop: false`): restructure into a helper with
  an early `return`
- A `.map`/`.filter` callback returns on every path (`array-callback-return`)
- Use `===` and `!==`; the one loose comparison is `value == null` for "null or undefined"
  (`eqeqeq` ignores `null`)
- Do not turn a `switch` or an `if`/`else if` chain into a table of handler functions: with a real
  call in the loop the table loses ~25% (`bench/dispatch.js`). A table is for late registration
  (`WrpcClient.transport`), and one keyed on anything peer-controlled needs `__proto__: null` or
  `Object.hasOwn`
- A defensive copy is not an allocation to eliminate; where one is genuinely unnecessary, say so
  in a comment
- Keep return types consistent
- Avoid nested ternaries and deep callback nesting
- Self-documented and self-descriptive code; a comment explains why (an invariant, a trap, a
  decision), never what the next line does
- A reference in a comment or in CLAUDE.md names a symbol in a file, never a line number
- Throw a `TypeError` for a bad argument or option, prefixed with the thing being configured
  (`RpcServer.attach: options.session must be an object`); give an error a peer may be told a
  numeric HTTP-like `code` (and `expose = true` when a 5xx message is written for the peer); never
  swallow an error without saying why in a comment
- A string a peer chose — a method, an id, a stream name — is bounded (`clip`) and escaped before
  it becomes a log line
- In the shared files of `src/encryption/`, `await` every cipher, DH and KDF call: they are
  synchronous on Node and promise-returning in a browser

## Optimizations

- Keep object shapes stable: same keys, types, and key order
- Change shape only in constructors and creational patterns; avoid mix-ins — a field a later
  function needs is declared in the class, not attached to the instance afterwards
- Use shape mutations only for metaprogramming
- Use `null` for empty reference types; use `undefined` for empty primitives
- Avoid array destructuring in assignments and loops; object destructuring is ok
- Keep hot functions monomorphic: stable arg count, types, and return types
- Avoid reading the same property from many unrelated object shapes
- Prefer string or number literals for status/codes; avoid mixing primitives with objects in one variable
- Keep objects that share a property to the same shape; avoid polymorphic hotspots
- Prefer fixed property access (`obj.x`) over dynamic keys (`obj[key]`)
- Keep arrays dense and of one element kind
- Initialize object fields in constructor/factory once and in one order
- Set field to `null` or `undefined`; avoid `delete`
- Move reusable callbacks outside loops
- Keep `try/catch`, spread/rest away from hot loops
- Use `Map` for dynamic key dictionaries, plain objects for fixed schemas
- Use `Set` for big and dynamic collections, prefer arrays for short and stable
- Use `Object.create(null)` for pure dictionaries
- Use typed arrays for numeric and binary workloads
- Reduce GC pressure: reuse arrays, objects, and buffers when safe
- A hot-path hand-optimization cites its benchmark in a comment (`bench/unmask.js` over
  `applyMask`, `bench/emitter.js` over `Emitter`); a new measurement is a new top-level file under
  `bench/`, which `bench/run-all.js` discovers. Unmeasured, the code is only harder to read
- Browser-reachable files are on a min+gzip byte budget that `pnpm size` enforces: a manual loop
  costs source bytes, so optimize there only with a measurement, and raise a budget only in
  `scripts/size.js`, in the change that earns the bytes, with a comment saying why
