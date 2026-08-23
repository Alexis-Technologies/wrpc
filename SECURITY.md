# Security Policy

## Reporting a vulnerability

Report suspected vulnerabilities **privately** via
[GitHub Security Advisories](https://github.com/Alexis-Technologies/wrpc/security/advisories/new)
or by emailing the author (see `package.json`). Do not open public issues or
pull requests for security reports — a fix should ship before the details do.

What to include: the affected surface (see below), a minimal reproduction,
and the impact as you understand it. Reports are acknowledged within
**7 days**; a confirmed vulnerability gets a fix or a mitigation plan within
**30 days**, and coordinated disclosure (an advisory + a patched release)
once the fix is out. Credit is given unless you ask otherwise.

## Scope

wrpc implements its own protocol surfaces from scratch, and they are the
areas where a report matters most:

- **The WebSocket engine** (`src/websocket/`): RFC 6455 handshake and
  framing, RFC 7692 permessage-deflate — parsing hostile bytes off the wire,
  inflation limits (`maxPayload`), buffering limits (`maxBuffer`,
  `maxBackpressure`).
- **The RPC dispatch layer** (`src/rpc/`): packet parsing, access control
  (`access`, hooks), session handling and cookies (token generation,
  RFC 6265 encoding, the `Set-Cookie` path), prototype-pollution guards on
  wire-supplied keys.
- **The HTTP surface** (`src/transport.js`, `src/adapters/`, `src/sse/`):
  CORS policy, the sec-fetch-site CSRF gate, SSE channel identity binding
  and caps, body-size limits.
- **The auth surfaces** (`src/auth/`, the `sessions.transport` seam in
  `src/rpc/sessions.js`): where the session token travels on the wire —
  bearer/payload carriers, the ws subprotocol carrier, client token stores
  — and the `authenticate`/`refresh` client lifecycle around it. The most
  security-sensitive recent code in the package.
- **The cluster surface** (`src/rpc/cluster.js`): the backplane is a trust
  peer of every node; the opt-in `cluster.secret` HMAC and the command
  shape validation exist to be unbreakable too.
- **The codegen CLI** (`src/cli/`): everything read from a remote server's
  introspection is untrusted input to a file generator.

Denial-of-service through resource exhaustion on any of these surfaces is
in scope; the limits exist to be unbreakable.

## Supported versions

| Version | Supported |
| --- | --- |
| Unreleased (`main` at HEAD, pre-first-publish) | ✔ fixes land on `main` |
| Latest published minor (once 1.0.0 ships) | ✔ security fixes |
| Older releases | ✖ upgrade to the latest |

The wire protocol carries its own compatibility promise
([protocol.md](./docs/reference/protocol.md#stability)) — a security fix
will never need a wire-breaking change inside a major version.
