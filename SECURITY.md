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
- **Encryption** (`src/encryption/`, and the sealed layers built on it in
  `src/rpc/envelope.js`, `src/broker/sealing.js`, `src/webtransport/`):
  a counter nonce that repeats (including across a reseed), a message that
  opens under another layer, channel, key id or AAD than it was sealed for,
  an `OpenError` that is distinguishable by cause, a downgrade to plaintext
  where a peer or a mode forbids it, a replay the window or the request
  memory should have refused, a static key derived for one protocol usable
  under another.
- **Parsers of hostile bytes** beyond the WebSocket engine:
  `src/webtransport/` (the control-stream framing and the stream mux),
  `src/deflate/inflate.js` (a complete DEFLATE decoder — a block that
  costs more than its header says is in scope), `src/attachments.js`
  (the binary attachments frame), `src/webrtc/framing.js` and
  `src/webrtc/assertions.js` (data-channel framing, the JWS trust
  assertions and the SDP fingerprint they bind to), `src/broker/ids.js`
  and `src/broker/sealing.js` (resume tokens, sealed broker messages).

Denial-of-service through resource exhaustion on any of these surfaces is
in scope; the limits exist to be unbreakable. Subpaths marked
`@experimental` (`./wt`, `./encryption`, `./broker/*`) are in scope too:
experimental describes the API's stability, not how seriously a hole in it
is taken.

### Documented limits — not vulnerabilities

Some things the encryption does not protect are stated up front, in
[what it does not protect](./docs/guide/encryption.md#what-it-does-not-protect):
metadata (sizes, timing, channel and room names, key ids), a compromised
end, replay on a log that is meant to be re-read, and a browser against the
origin that ships its script. A report that one of these holds is
confirming the documentation; a report that the documentation is wrong
about where a limit lies is very much wanted.

## Supported versions

| Version | Supported |
| --- | --- |
| `2.x` (the latest published minor) | ✔ security fixes |
| `1.x` | ✖ not supported — upgrade to `2.x` (the [migration notes](./CHANGELOG.md) list every breaking change) |
| Older releases | ✖ upgrade to the latest |

There is no maintenance branch for `1.x`: 1.0.0 shipped five weeks before
2.0, and the 2.0 line is where every fix lands.

The wire protocol carries its own compatibility promise
([protocol.md](./docs/reference/protocol.md#stability)) — a security fix
will never need a wire-breaking change inside a major version.
