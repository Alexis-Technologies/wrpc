# Stability & deprecation

What "1.x" promises, in one place — the consumer-facing copy of the policy
[CONTRIBUTING.md](https://github.com/Alexis-Technologies/wrpc/blob/main/CONTRIBUTING.md#stability-and-deprecation)
holds for contributors.

## The public surface

**The `exports` subpaths are the public API.** Everything reachable through
`@alexify/wrpc` and its subpaths (`/ws`, `/engine`, `/uws`, `/fastify`,
`/express`, `/scaling`, `/sse`, `/query`, `/auth`), as typed by the
hand-maintained root `.d.ts` files, is stable under semver. Deep imports
into `src/` are **not** addressable and not supported: the module layout
may change in any release (and has — `rpc/core.js` split twice already).

## Semver, spelled out

- **Patch** — fixes; no observable API change.
- **Minor** — additive API, additive wire fields, new options with safe
  defaults. Deprecations are announced here: a deprecated API keeps working
  for at least one minor, with the note in the CHANGELOG and the docs.
- **Major** — the only place a stable API may break or a deprecated one may
  be removed.

## `@experimental` carve-outs

Two areas are marked `@experimental` in the `.d.ts` files and may change in
a **minor** (described in the CHANGELOG):

- the **telemetry** writer shapes and metric set (the `telemetry` option,
  and the `RpcServer#otel` getter the fastify adapter brackets delegated
  routes with) — the signals will keep improving;
- the **engine port** internals beyond the documented `WrpcSocket` contract
  (`capabilities` in particular).

## The wire protocol's own, stronger promise

The [wire protocol](./protocol#stability) is **frozen at 1.0**: packet
shapes never break inside a major, new fields are additive and ignorable,
and the revision is named on the wire — the `wrpc.v1` subprotocol on
WebSocket, the reserved `wrpc-version` response header on HTTP. An
independent implementation written against the reference keeps working for
the life of the major version.
