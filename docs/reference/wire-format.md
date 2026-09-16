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

## Data-channel frames

On a WebRTC data channel (the `@alexify/wrpc/webrtc` transport) both packets
and chunks travel as **binary** messages, because a channel message has a
size limit and a packet or a chunk may not fit in one. Each message is one
fragment, prefixed by a single header byte:

```
┌────────────┬────────────────────────────────────────┐
│  1 byte    │              fragment payload           │
│  KIND|FIN  │  UTF-8 JSON (KIND 0) or chunk (KIND 1)  │
└────────────┴────────────────────────────────────────┘
bit 0 KIND, bit 1 FIN (last fragment), bits 2–7 reserved (0)
```

A packet under the limit is one message: header `0b11`, then the JSON. A
64 KiB stream chunk at a 16 KiB limit is five messages: four with header
`0b01` and a final `0b11`, the payloads concatenated being exactly the chunk
frame above (id length, id, payload). The receiver concatenates until FIN —
the channel is ordered and reliable, so no message id is needed — and then
hands a KIND 0 message to the packet parser and a KIND 1 message to
`chunkDecode`. The rules and the error cases are in
[the protocol reference](./protocol#webrtc-framing).

```js
const { FrameEncoder, FrameDecoder, KIND_TEXT } = require('@alexify/wrpc/webrtc');

const encoder = new FrameEncoder(16 * 1024); // the negotiated maxMessageSize
encoder.encodeText('{"type":"ping"}', (frame) => channel.send(frame));
const decoder = new FrameDecoder();
const message = decoder.push(event.data); // null until FIN; then { kind, data }
```

The frame handed to the sink is a view over a buffer the encoder reuses for
the next fragment — hand it to `send()`, which copies, and never keep it.

## WebTransport stream frames

On a WebTransport session (`transport: 'wt'`, `@alexify/wrpc/wt`) every packet
and chunk travels on one bidirectional stream, the control stream. A QUIC
stream carries bytes with no message boundary, so each message is
length-prefixed:

```
┌──────────────────┬────────┬────────────────────────────────────────┐
│     4 bytes      │ 1 byte │              payload                    │
│ LENGTH (BE u32)  │  KIND  │  UTF-8 JSON (KIND 0) or chunk (KIND 1)  │
└──────────────────┴────────┴────────────────────────────────────────┘
```

A `{"type":"ping"}` packet is 20 bytes: `00 00 00 0F 00` and then the 15
bytes of JSON. A 64 KiB stream chunk is one message of 65 541 bytes, however
many reads it takes to arrive; the receiver hands a KIND 0 message to the
packet parser and a KIND 1 message to `chunkDecode`. The rules and the error
cases are in [the protocol reference](./protocol#webtransport-framing).

```js
const { frame, frameText, StreamParser, KIND_BINARY } = require('@alexify/wrpc/wt');

writer.write(frameText('{"type":"ping"}')); // a fresh frame — safe to queue
writer.write(frame(KIND_BINARY, chunkEncode(id, bytes)));
const parser = new StreamParser({ onMessage: (kind, data) => {} });
for await (const read of readable) parser.push(read); // onMessage once per message
```

Unlike the data-channel encoder, the frames here are fresh buffers: a WHATWG
writer takes its chunk by reference and may process it after `write()`
returns, so a reused scratch buffer would corrupt what is still queued.

Two more shapes ride a session. A **datagram** (an unreliable event) is one
whole packet under the KIND byte alone, `00` then the JSON — see
[the protocol reference](./protocol#webtransport-datagrams). A **binary
stream on its own unidirectional stream** opens with the chunk header —
`idLen`, then the id — and then carries raw payload bytes to its FIN; the
receiver rebuilds `chunkEncode` frames from them, one per read
([the protocol reference](./protocol#webtransport-streams)).

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
| `perMessageDeflate` | off | `true` or `{ threshold, filter }`. |
| `coalesce` | `true` | Cork every write of one event-loop turn and flush on the next tick, one `writev` per burst. |
| `pingInterval` | `10000` | Protocol-ping interval; a peer that misses one is terminated. |
| `maxBuffer` | 100 MiB | Largest inbound message. |
| `maxBackpressure` | `maxBuffer` (100 MiB) | Outbound cap; a connection past it is **terminated** (the peer observes `1006`). |
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
included. A peer that lets the buffer grow past `maxBackpressure` (default:
`maxBuffer`, 100 MiB) is **terminated** — a hard socket destroy, so the peer
observes an abnormal `1006` close, not a graceful `1009`: a connection that
far behind cannot be trusted to complete a close handshake. In practice the
protocol heartbeat bounds the accumulation first — a peer whose receive side
stalled misses its pong and is terminated within ~2 ping intervals. Note the
uws engine's default outbound ceiling is its 16 MiB `maxPayload`; set
`maxBackpressure` explicitly if the two engines must match.

`pause()`/`resume()` are the receive side of the same idea: while binary chunks
are being consumed, the RPC layer stops reading from the socket, so the
pressure reaches the sender through TCP.

Server connections **coalesce** writes (`coalesce: true`): the first write of
an event-loop turn corks the socket and the next tick uncorks it, so the N
answers to a batch or a burst of events leave in one `writev` instead of N
syscalls — the idiom `node:http` uses. The boolean stays honest: a corked
write still reports the buffered length against the high-water mark, and
`'drain'` is unchanged. A bare `Connection` defaults to `coalesce: false`,
so a write is on the socket the moment `send()` returns.

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
new WebsocketServer({
  server,
  perMessageDeflate: {
    threshold: 1024,
    // Per connection: compress for a browser on a slow link, not for a
    // service in the same datacenter. Declines the offer, so the peer
    // learns it from the handshake.
    filter: (req) => req.headers['x-forwarded-proto'] !== undefined,
  },
});
```

Messages below `threshold` (1 KiB by default) are sent uncompressed — below it,
compression costs more than it saves. `filter(req)` decides per upgrade
request whether the peer's offer is accepted at all. The negotiated response
always asks for `server_no_context_takeover` and
`client_no_context_takeover`, which trades some ratio for bounded
per-connection memory: context takeover keeps a zlib window alive per peer,
and thousands of idle connections each holding one is a worse problem than a
slightly larger frame.

`server_max_window_bits` is honoured when the client asks for it (8–15).

Because no context spans messages, the compressed bytes of a message depend
only on the payload and the window size. That is what lets a room broadcast
share one deflated frame per distinct window across all its recipients
(`Connection.sendPrepared`, see [performance](../guide/performance#fan-out))
instead of deflating once per member.

Per message, `send(data, { compress: false })` — and `emit(name, data,
{ compress: false })` on a room — sends uncompressed past the threshold: for
a payload that is already compressed, or one where latency matters more than
bytes.

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
