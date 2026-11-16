# `src/cli/` — the `wrpc types` codegen

Cross-cutting rules — the protocol revision, no runtime dependencies, commands, the package layout — are in the root `CLAUDE.md`; the layering of `src/` and the hot-path conventions are in `src/CLAUDE.md`. A rule lives in ONE file: add to the file of the folder it is about.

`src/cli/types.js` — the `wrpc types <url> --out api.d.ts` codegen, behind the `bin/wrpc.js` shim. Consumes `system/introspect` and writes the same shape of contract interface a user would hand-write for `connect<Api>()`; the `signature` descriptor format it reads is specified in `docs/reference/protocol.md`. Everything beyond argv is injected (`main(argv, { log, error, fetch, write, version })`), so the whole command is testable in-process. **Nothing from the wire is interpolated verbatim**: names go through `quoteKey` (JSON escaping, downgraded to single quotes only when nothing needed escaping) and type names are matched against a closed allowlist, with a depth cap on nesting.

What goes to the TERMINAL is the server's text too: `printable` in `src/cli/types.js` escapes control characters and bounds a line, in every sink. The fetch has a deadline over headers and body (`--timeout`, `DEFAULT_TIMEOUT`) and the answer a cap (`MAX_ANSWER`); lists whose length the server chose are appended in a loop, never spread into a call.
