# `src/scaling/` — the rooms backplane

Cross-cutting rules — the protocol revision, no runtime dependencies, commands, the package layout — are in the root `CLAUDE.md`; the layering of `src/` and the hot-path conventions are in `src/CLAUDE.md`. A rule lives in ONE file: add to the file of the folder it is about.

`src/scaling/` — the rooms backplane behind the `@alexify/wrpc/scaling` subpath: the structural contract (`publish(channel, message)` / `subscribe(channel, handler) -> unsubscribe` / `close()`, checked by `isBackplane`), `memory.js` (`MemoryBackplane`, in-process, microtask delivery) and `redis.js` (`createRedisAdapter({ pub, sub, prefix })`, ioredis-shaped, clients **injected** — see "No runtime dependencies" in the root `CLAUDE.md`). Delivery is at-most-once by design.
