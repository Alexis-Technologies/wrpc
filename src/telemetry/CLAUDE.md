# `src/telemetry/` — the injected-OpenTelemetry writers

Cross-cutting rules — the protocol revision, no runtime dependencies, commands, the package layout — are in the root `CLAUDE.md`; the layering of `src/` and the hot-path conventions are in `src/CLAUDE.md`. A rule lives in ONE file: add to the file of the folder it is about.

`src/telemetry/` — the injected-OpenTelemetry writers (`shared.js` with the `tp`/`ts` W3C propagation fields, `server.js`, `client.js`); everything duck-typed, `@opentelemetry/*` only ever a devDependency.
