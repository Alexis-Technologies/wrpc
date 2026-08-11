# Wire format

[The protocol reference](./protocol) describes the **packets**. This page
describes what carries them: the binary chunk framing wrpc defines, and the
from-scratch RFC 6455 WebSocket implementation the default engine is built on.

Most applications never need this page. Read it if you are implementing an
[engine](./engine), speaking the protocol from another language, or debugging
something at the frame level.

## Binary chunks

JSON packets travel as WebSocket **text** frames. Stream payloads travel as
**binary** frames, and each one is exactly one chunk of one stream:

```
┌────────┬──────────────────┬──────────────────────────┐
│ 1 byte │  id (idLength)   │        payload           │
│ idLen  │  utf-8 stream id │        bytes             │
└────────┴──────────────────┴──────────────────────────┘
```

- Byte 0 is the id length, so an id is at most 255 bytes.
- The id is the same string the opening `{ type: 'stream', id, name, size }`
  packet announced.
- Everything after it is payload, verbatim.

Prefixing the id rather than opening a frame per stream is what lets several
streams interleave over one connection without head-of-line blocking: a large
upload cannot starve a small one, and neither blocks the JSON packets sharing
the socket.

```js
const { chunkEncode, chunkDecode } = require('@alexify/wrpc');

const frame = chunkEncode('9a2c…', payload);
const { id, payload: bytes } = chunkDecode(frame);
```

The Node build uses `Buffer`, the browser build `TextEncoder`/`TextDecoder` —
swapped through the package's `browser` field, so the same code works in both.

## The WebSocket engine

`@alexify/wrpc/ws` publishes the implementation itself. It is **not** in the
main barrel: an application needs the RPC layer, not the frame parser.

```js
const { WebsocketServer, Connection, Frame, FrameParser, OPCODES, CLOSE_CODES } = require('@alexify/wrpc/ws');
```

`WebsocketServer` is usable on its own, as a plain WebSocket server:

```js
const wss = new WebsocketServer({ server: httpServer, path: '/ws' });
wss.on('connection', (connection, req) => {
  connection.on('message', (data, isBinary) => connection.send(data));
});
```

| Option | Default | Meaning |
| --- | --- | --- |
| `server` | — | Binds to its `'upgrade'` event. **Omit it** to drive handshakes by hand through `handleUpgrade(req, socket, head)`. |
| `path` | — | Restrict upgrades to this pathname. |
| `verifyClient({ req, socket, head })` | — | Gate the handshake. |
| `protocols` / `handleProtocols(offered, req)` | — | Subprotocol negotiation; `false` rejects the handshake. |
| `perMessageDeflate` | off | `true` or `{ threshold }`. |
| `pingInterval` | `10000` | Protocol-ping interval; a peer that misses one is terminated. |
| `maxBuffer` | 100 MiB | Largest inbound message. |
| `maxBackpressure` | `0` (unbounded) | Outbound cap; exceeding it closes with `1009`. |
| `fragmentThreshold` | `0` (off) | Fragment outbound messages above this size. |
| `closeTimeout` | `1000` | How long to wait for the peer's close frame. |

`wss.connections` is a snapshot of the live connections, and
`wss.close({ code, reason })` closes them all.

## `Connection`

```js
connection.send(data);          // text for a string, binary for a Buffer
connection.sendText(message);
connection.sendBinary(buffer);
connection.sendPing(payload);
connection.sendPong(payload);
connection.sendClose(code, reason);
connection.terminate();
connection.pause();  connection.resume();
```

Events: `'message'(data, isBinary)`, `'ping'(payload)`, `'pong'(payload)`,
`'drain'`, `'close'(code, reason)`, `'error'(error)`.

### Backpressure

`send()` and its siblings return a boolean: `false` means the socket buffer is
above its high-water mark, and a `'drain'` event will follow. `bufferedAmount`
is what is queued but not yet flushed.

This is accounted honestly rather than assumed — the return value of the
underlying `socket.write()` is tracked everywhere, fast ping/pong paths
included. With `maxBackpressure` set, a peer that lets the buffer grow past it
is closed with `1009` (message too big) instead of being allowed to exhaust
memory.

`pause()`/`resume()` are the receive side of the same idea: while binary chunks
are being consumed, the RPC layer stops reading from the socket, so the
pressure reaches the sender through TCP.

### Payload ownership

A received payload may be a view into the receive buffer. **Copy it if you
retain it past the listener call** — otherwise a small retained slice pins a
whole segment, which is retention amplification rather than a leak, and just as
fatal at volume.

## Frames

`Frame` and `FrameParser` are the layer below `Connection`, exported for tests
and for anyone parsing frames themselves:

```js
Frame.text('hi');
Frame.binary(buffer);
Frame.ping(payload);  Frame.pong(payload);
Frame.close(1000, 'bye');

const { value, error } = FrameParser.parse(buffer, { allowedRsv });
```

Parsing answers `{ value, error }` rather than throwing: a protocol violation
from a peer is data, not an exception. `ParseError.code` is one of
`PARSE_ERR_CODES` — `MESSAGE_TOO_BIG`, `PROTOCOL_ERROR-COMMON`,
`PROTOCOL_ERROR-RSV`, `PROTOCOL_ERROR-CTRL_TOO_LONG`, `INVALID_PAYLOAD`.

`OPCODES` and `CLOSE_CODES` are exported as named constants
(`CLOSE_CODES.NORMAL_CLOSE`, `OPCODES.BINARY`, …).

### Receive-path performance

Two things that matter at volume, both measured by `bench/`:

- **Segment queue, not `Buffer.concat`.** Incoming TCP segments go into a list
  with a cursor (`SegmentQueue`), so receiving an *n*-segment message is O(n)
  rather than the O(n²) that re-concatenating on every segment costs.
- **Word-wise unmasking.** The XOR unmask runs 32 bits at a time with a byte
  tail, rather than byte by byte.

## permessage-deflate

RFC 7692 compression, over `node:zlib`, **off by default**:

```js
new WebsocketServer({ server, perMessageDeflate: { threshold: 1024 } });
```

Messages below `threshold` (1 KiB by default) are sent uncompressed — below it,
compression costs more than it saves. The negotiated response always asks for
`server_no_context_takeover` and `client_no_context_takeover`, which trades
some ratio for bounded per-connection memory: context takeover keeps a zlib
window alive per peer, and thousands of idle connections each holding one is a
worse problem than a slightly larger frame.

`server_max_window_bits` is honoured when the client asks for it (8–15).

When deflate is negotiated, `RSV1` becomes a legal bit on data frames and the
parser is told so through `allowedRsv` — an unnegotiated RSV bit is still a
protocol error.

## Conformance

The receive path is covered by a large RFC 6455 conformance suite in
`tests/websocket/` — fragmentation, control-frame rules, UTF-8 validation
(including split sequences), close-code handling, masking, oversized frames.
`scripts/autobahn/` runs the
[Autobahn Testsuite](https://github.com/crossbario/autobahn-testsuite) against
an echo server built on `WebsocketServer` for the full external check.
