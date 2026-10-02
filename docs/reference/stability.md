# Stability & deprecation

What 2.x promises, in one place — the consumer-facing copy of the policy
[CONTRIBUTING.md](https://github.com/Alexis-Technologies/wrpc/blob/main/CONTRIBUTING.md#stability-and-deprecation)
holds for contributors.

## The public surface

**The `exports` subpaths are the public API.** Everything reachable through
`@alexify/wrpc` and its subpaths (`/ws`, `/engine`, `/uws`, `/fastify`,
`/express`, `/scaling`, `/sse`, `/query`, `/auth`, `/deflate`, `/webrtc`,
`/wt`, `/encryption`, `/broker`, `/broker/redis`, `/broker/nats`,
`/broker/amqp`, `/broker/kafka`), as typed by the hand-maintained root
`.d.ts` files, is stable under semver — except where a subpath is listed
under the carve-outs below. Deep imports
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

Five areas are marked `@experimental` in the `.d.ts` files and may change in
a **minor** (described in the CHANGELOG):

- the **telemetry** writer shapes and metric set (the `telemetry` option,
  and the `RpcServer#otel` getter the fastify adapter brackets delegated
  routes with) — the signals will keep improving;
- the **engine port** internals beyond the documented `WrpcSocket` contract
  (`capabilities` in particular);
- **WebTransport**, whole: the `wt` client transport and its `wt` connect
  option, the `@alexify/wrpc/wt` subpath, and the
  [control-stream framing](./protocol#webtransport) it speaks — Node has no
  WebTransport of its own yet, and the carrier will follow what lands.
- the **message-broker family**, whole: `@alexify/wrpc/broker` and every
  `@alexify/wrpc/broker/*` adapter subpath, their capability contracts, the
  broker metrics, the `broker` client transport and its
  [broker binding](./protocol#broker-binding) — until every adapter has shipped
  ([Message brokers](../guide/brokers)).
- **application-level encryption**, whole: the `@alexify/wrpc/encryption`
  subpath and every `encryption` option it feeds — the server's and the
  client's session encryption, `rooms.encryption`/`cluster.encryption`, the
  broker bindings', `sealedStore` — with `Client.encryption`,
  `RpcServer.encryptionKey()`/`encryptionRequired` and `attach({ encrypted })`,
  and the [session encryption](./protocol#session-encryption),
  [sealed requests](./protocol#sealed-requests) and
  [broker sealing](./protocol#broker-sealing) wire formats — until the
  formats have been reviewed against real deployments
  ([Encryption](../guide/encryption)). TLS is not in this list: it is the
  platform's.

## The wire protocol's own, stronger promise

The [wire protocol](./protocol#stability) is documented at **revision 2**,
in three tiers: the packet core, which never breaks inside a major — 2.0
added one thing to it, the framed messages that carry bytes as bytes, and
that is what the revision named on the wire negotiates: `wrpc.v2` on a
WebSocket and `wrpc-version: 2` on HTTP between two 2.x ends, `wrpc.v1`
with a 1.0 peer, [with nothing to configure](./protocol#versioning); the
carrier conventions, additive under the same rule, of which the ones a 1.0
peer never saw are badged `since 2.0` and listed with what happens when the
versions meet under [Changes since 1.0](./protocol#changes-since-1-0); and
the experimental sections, which follow the carve-outs above. An independent
implementation written against the reference keeps working for the life of
the major version.
