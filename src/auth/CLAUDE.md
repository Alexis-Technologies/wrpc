# `src/auth/` — token strategies

Cross-cutting rules — the protocol revision, no runtime dependencies, commands, the package layout — are in the root `CLAUDE.md`; the layering of `src/` and the hot-path conventions are in `src/CLAUDE.md`. A rule lives in ONE file: add to the file of the folder it is about.

`src/auth/index.js` — token strategies behind the `@alexify/wrpc/auth` subpath, browser-safe and require-free like `src/query/`: client token stores (`memoryStore`/`webStorage`/`cookieStorage`; a `Map` already satisfies the `get`/`set`/`delete` contract), `bearerAuth()` (composes the client's `headers`+`authenticate`+`refresh` options around a store), and the server halves `bearerTransport()`/`payloadTransport()` for the `sessions: { transport }` injection (`isTokenTransport` in `src/rpc/sessions.js`; the cookie default is byte-identical). Deliberately outside the base browser bundle — the seams live in the core, the strategies do not.
