# Errors and close codes

wRPC reuses **HTTP status numbers** as its error vocabulary, on every transport.
A code that reaches a WebSocket client means the same thing it would over
HTTP — and in [REST mode](../guide/server#what-it-serves) it literally *is* the
HTTP status.

## The error shape

An error travels inside a `callback` packet:

```json
{ "type": "callback", "id": "7", "error": { "message": "Not found", "code": 404 } }
```

An error MAY additionally carry structured `details` — validation failures
put their issue list there:

```json
{ "error": { "message": "Invalid arguments: text is required", "code": 400,
             "details": { "issues": [{ "message": "text is required", "path": ["text"] }] } } }
```

On the client it arrives as a rejected promise carrying a `WrpcError`:

```js
try {
  await client.api.chat.send({ text });
} catch (error) {
  if (error.code === 429) backOff();
  else if (error.code >= 500) report(error);
}
```

`WrpcError` is `Error` plus a numeric `code` — and `details`, when the
server attached any; its `name` is `'WrpcError'`. There is no error *class*
per code on purpose: the number crosses the wire, subclasses do not.

```js
catch (error) {
  if (error.code === 400) showIssues(error.details?.issues);
}
```

## Throwing from a handler

Throw a `WrpcError` with the status as its `code`, and that is what the peer
is answered — the same class the client rejects with, constructed as every
wRPC error is, `(message, options)`:

```js
const { WrpcError } = require('@alexify/wrpc');

handler: async (context, { id }) => {
  const row = await db.find(id);
  if (!row) throw new WrpcError('No such document', { code: 404 });
  return row;
};
```

`options` takes `code` (500 when left out), `details` — the same exposure
rule as the message applies, a 4xx's details travel and a 5xx's stay in the
log — `expose` and `cause`, which stays in the log too. Any other error with
a numeric `code` is answered the same way, so code that sets `error.code`
by hand keeps working.

::: warning 5xx messages do not travel
A 4xx is part of the protocol conversation and its message reaches the peer. A
**5xx is a server internal**: the message is replaced with the generic status
text on the wire and the real one goes to the log. Opt out per error with
`expose: true` (or `error.expose = true` on an error of your own). Stack
traces and causes never travel.
:::

A code above `599` is reported as `500`.

## Codes the library itself sends

| Code | Meaning | Where it comes from |
| --- | --- | --- |
| `400` | Malformed packet, unparseable frame, duplicate in-flight call id, invalid arguments, `{type:'call'}` against a subscription; a [binary stream](../guide/streams#size) that went past its announced size or ended short of it — answered on the stream's id, and thrown to its reader | dispatcher, [validators](../guide/router#validators), streams |
| `403` | No [session](../guide/sessions) for `access: 'session'`; an SSE channel presented without its cookie identity; a cross-site `GET` | core, [SSE](../guide/sse) |
| `404` | No such unit, version or procedure | router lookup |
| `408` | Procedure `timeout` elapsed, or an [ask](../guide/rooms#asking-a-room) went unanswered; a [binary stream nobody read](../guide/streams#limits) within `streamIdleTimeout` — said to its sender, and thrown to a reader that comes late | router, `expectAnswer`, dispatcher |
| `409` | Unknown SSE channel — the server no longer holds that id | SSE |
| `413` | More bytes in [streams nobody reads yet](../guide/streams#limits) than `maxStreamBuffer` allows one connection, or — where the carrier cannot pause — a stream too far ahead of its reader; answered on the id of the stream that went past it, and thrown to its reader | dispatcher, client |
| `429` | `maxCalls` in flight on this connection, `maxStreams` open on it, or `maxChannelsPerAddress` for SSE | dispatcher, SSE |
| `499` | Cancelled by the caller — `{type:'cancel'}`, or an aborted `signal`; a [binary stream its reader let go of](../guide/streams#ended-by-the-receiver) — said to the stream's sender, on the stream's id, and found on `writable.error`; a stream its **sender** terminated — thrown to its reader, which was promised more | dispatcher, client, streams |
| `500` | Handler threw without a code; a subscription handler returned a non-iterable; an `output` validator rejected the result | router |
| `501` | No responder registered for an [ask](../guide/rooms#asking-a-room); a binary stream attempted over [SSE](../guide/sse#what-it-cannot-do) | client, SSE |
| `503` | Queue full, server [draining](../guide/production#graceful-shutdown), transport closed under an in-flight call — or under a [binary stream](../guide/streams#size) being read, whose reader is thrown it — `maxChannels` reached | router, core, SSE, streams |
| `400`, `409`, `426` (sealed) | A [sealed request](../guide/encryption#session) refused before it was read: not accepted (the pinned key may be retired), stale or replayed (a clock), or plaintext where encryption is `required`. Bare statuses, no body — the calls the request carried fail with the same code on the client, and the client emits `EncryptionRefusedError` (`code: 'ALX_ERR_ENCRYPTION_REFUSED'`, `statusCode`); any other plaintext answer to a sealed request is a `503` | encryption |

::: tip 499 is not a failure
It acknowledges a cancel the caller asked for. The client resolves the
already-rejected call silently rather than surfacing it twice — you see the
`AbortError` you caused, not a second error from the server.
:::

## Which codes are worth retrying

| Code | Retry? | Broker consumer | Why |
| --- | --- | --- | --- |
| `400`, `403`, `404`, `501` | **No** | dead letter | The same request will fail identically. |
| `408` | Careful | retry | Only if the operation is idempotent — it may have completed. |
| `409` | Automatic | — | The SSE transport starts a fresh channel itself. |
| `429`, `503` | **Yes, with backoff** | retry | Load or a rolling deploy; both are temporary by construction. |
| `499` | No | — | You caused it. |
| `500` | No, alert | retry, then dead letter | A bug, not a condition — but on a queue usually a dependency that fell over. |

The **Broker consumer** column is the default `retryOn` policy of
[queue consumers](../guide/brokers/consumers#what-a-message-becomes): at-least-once
delivery already requires an idempotent handler, so a transient `408`/`500` is
retried with backoff up to the attempt limit before the message is dead-lettered.

The client's own [reconnect](../guide/client#reconnecting) already applies
truncated exponential backoff with full jitter to the *connection*. Per-call
retries are yours: wRPC never replays a call automatically, because it cannot
know whether yours is idempotent.

## Errors thrown to your code {#thrown}

Not every error crosses the wire. A mistake of the program that calls wRPC —
an option it cannot use, a router it cannot build, an argument a method
refuses — and a peer's bytes that break a framing are thrown (or emitted as
`'error'`) where they happened, with a **string** `code`. Every one starts
with `ALX_ERR_`, the prefix every Alexis library codes its errors with, as
Fastify's start with `FST_ERR_`; a **numeric** `code` is never one of these —
it is a wire status, of a `WrpcError` or of an error a handler throws.

| Code | Class | Thrown when |
| --- | --- | --- |
| `ALX_ERR_CONFIG` | `ConfigError` | An option, a definition (a router, a procedure, a broker binding or event table) or an injected host is missing or invalid — at construction, `defineRouter`, `procedure()`, `attach…` |
| `ALX_ERR_ARGUMENT` | `ArgumentError` | An argument of a call made while running is invalid — `createStream(name, size)`, `setHeader`, `sendTo`, `respond`, the arguments a peer gave a signaling procedure |
| `ALX_ERR_CLI` | `CliError` | The [`wrpc` command](../guide/cli) refuses an argument, or an answer it cannot use; it prints the message and exits 1 |
| `ALX_ERR_FRAMING_EMPTY` | `FramingError` | A WebRTC frame with no header byte, or a fragment with no payload before the last |
| `ALX_ERR_FRAMING_RESERVED` | `FramingError` | A WebRTC frame with a reserved header bit set — the deflate bit included, until the decoder is told |
| `ALX_ERR_FRAMING_KIND` | `FramingError` | A WebTransport message of an unknown kind, or a WebRTC continuation whose kind is not its message's |
| `ALX_ERR_FRAMING_TOO_LARGE` | `FramingError` | A message past `maxMessage` (WebTransport) or `maxReassembly` (WebRTC) |
| `ALX_ERR_FRAMING_FRAGMENTS` | `FramingError` | A WebRTC message in more fragments than `maxFragments` |
| `ALX_ERR_FRAMING_UTF8` | `FramingError` | A text packet that is not valid UTF-8, on either carrier |
| `ALX_ERR_FRAMING_MESSAGE_SIZE` | `FramingError` | A WebRTC peer whose `max-message-size` is under 1 KiB: the link fails, never connects |
| `ALX_ERR_FRAMING_INFLATE` | `FramingError` | A compressed WebRTC frame that does not inflate, or inflates past `maxReassembly`; the `cause` is the codec's error |
| `ALX_ERR_BACKPRESSURE` | `TransportError` | A WebTransport session or a WebRTC channel past `maxBackpressure`: it is closed |
| `ALX_ERR_CHANNEL_SEND` | `TransportError` | A WebRTC data channel's `send()` threw before the first fragment: only that message is lost |
| `ALX_ERR_CHANNEL_DESYNC` | `TransportError` | …or threw mid-message: the peer holds a message with no end, and the channel is closed |
| `ALX_ERR_DEFLATE_TRUNCATED` | `DeflateError` | [`./deflate`](../guide/compression): the input ends before its last block does |
| `ALX_ERR_DEFLATE_HUFFMAN` | `DeflateError` | …a Huffman code that is over-subscribed, empty or invalid, or a length or distance code out of range |
| `ALX_ERR_DEFLATE_STORED` | `DeflateError` | …a stored block whose `NLEN` is not `LEN`'s complement |
| `ALX_ERR_DEFLATE_DISTANCE` | `DeflateError` | …a distance that reaches before the start of the output (a dictionary that is missing, most often) |
| `ALX_ERR_DEFLATE_BLOCK` | `DeflateError` | …the reserved block type 3 |
| `ALX_ERR_DEFLATE_TOO_LARGE` | `DeflateError` | …an output past `maxOutput` |
| `ALX_ERR_PARSE_MESSAGE_TOO_BIG` | `ParseError` | `./ws`, the frame parser: a payload length past `Number.MAX_SAFE_INTEGER` — closed 1009 |
| `ALX_ERR_PARSE_PROTOCOL` | `ParseError` | …a frame that breaks RFC 6455 otherwise (an opcode, a continuation, a close code) — closed 1002 |
| `ALX_ERR_PARSE_RSV` | `ParseError` | …an RSV bit no extension negotiated — closed 1002 |
| `ALX_ERR_PARSE_CONTROL_TOO_LONG` | `ParseError` | …a control frame over 125 bytes — closed 1002 |
| `ALX_ERR_PARSE_INVALID_PAYLOAD` | `ParseError` | …a text frame or a close reason that is not UTF-8 — closed 1007 |
| `ALX_ERR_ASSERTION_MALFORMED` | `AssertionError` | A WebRTC [trust assertion](../guide/webrtc-trust) that is not a compact JWS of JSON objects |
| `ALX_ERR_ASSERTION_TYP` | `AssertionError` | …whose `typ` is not `wrpc-rtc+jwt` |
| `ALX_ERR_ASSERTION_ALG` | `AssertionError` | …whose `alg` is not `ES256` |
| `ALX_ERR_ASSERTION_KID` | `AssertionError` | …signed by a key the verifier does not hold, or whose keys could not be loaded |
| `ALX_ERR_ASSERTION_SIGNATURE` | `AssertionError` | …whose signature does not verify |
| `ALX_ERR_ASSERTION_SUBJECT` | `AssertionError` | …about another peer |
| `ALX_ERR_ASSERTION_FINGERPRINT` | `AssertionError` | …bound to another DTLS fingerprint than the description's, or to none |
| `ALX_ERR_ASSERTION_EXPIRED` | `AssertionError` | …past its `exp`, beyond a minute of skew |
| `ALX_ERR_ASSERTION_ISSUER` | `AssertionError` | …from another issuer than the one expected |
| `ALX_ERR_ASSERTION_MISSING` | `AssertionError` | A description that carries no assertion where one is required |
| `ALX_ERR_OPEN` | `OpenError` | [`./encryption`](../guide/encryption): a sealed message that does not open — a wrong key, nonce, bit or length, on purpose one error |
| `ALX_ERR_ENCRYPTION_REFUSED` | `EncryptionRefusedError` | A sealed request answered in plaintext; `statusCode` is the outer status |

Every class takes `(message, options)`, as `Error` does: `options` holds
`cause` — set only when it is given — and the class's own fields, `code`
among them where one class has several. `ConfigError` and `ArgumentError`
extend `TypeError` — what 2.2 and older threw for both — so `instanceof
TypeError` still holds; they are exported from `@alexify/wrpc`, the others
from the subpath that throws them (`FramingError` and `TransportError` from
`./wt` and `./webrtc` alike). An assertion refusal counts in
`wrpc.rtc.assertions` under the last word of its code, lowercased —
`signature` for `ALX_ERR_ASSERTION_SIGNATURE` — the labels the
[metric](../guide/telemetry) had before the codes.

```js
const { ConfigError } = require('@alexify/wrpc');

try {
  new RpcServer({ router, maxStreamBuffer: '1mb' });
} catch (error) {
  if (error.code === 'ALX_ERR_CONFIG') console.error(`bad wRPC option: ${error.message}`);
}
```

Three errors keep the code of the Node.js error they stand in for, so code
that handles Node's handles them: a uWebSockets.js engine that cannot bind
(`EADDRINUSE`), a body cut short behind an adapter
(`ERR_STREAM_PREMATURE_CLOSE`) and a permessage-deflate message that
inflates past `maxPayload` (`ERR_BUFFER_TOO_LARGE`, what `zlib` itself throws
for it).

## WebSocket close codes

Close codes are a different vocabulary — RFC 6455's, not HTTP's — and they end
the *connection*, not one call. Exported as `CLOSE_CODES` from
[`@alexify/wrpc/ws`](./wire-format). A [WebTransport](../guide/wt) session
ends with the same codes — the server's 1001 on shutdown, 1002 on a framing
violation — as its `closeCode`.

| Code | Name | When wRPC uses it |
| --- | --- | --- |
| `1000` | Normal closure | `client.close()`, a clean goodbye. |
| `1001` | Going away | The server is [shutting down](../guide/production#graceful-shutdown). Reconnect elsewhere. |
| `1002` | Protocol error | A frame violated RFC 6455 — bad opcode, bad continuation, reserved bit set. With reason `encryption`: a [session handshake](../guide/encryption#session) frame that did not verify or parse, or a sealed frame that did not open. |
| `1006` | Abnormal closure | No close frame arrived. Never sent — it is what a local socket reports. |
| `1007` | Invalid payload | A text frame that was not valid UTF-8. |
| `1008` | Policy violation | With reason `encryption`: the session handshake was refused by policy — plaintext where encryption is required, an unlisted protocol or key id, `authorize` said no, the handshake timed out, the server had too much to say before it finished, or the peer sent more than the handshake holds (4,096 frames, 4 MiB) while it was still running. |
| `1009` | Message too big | An inbound message over `maxBuffer` — or, under permessage-deflate, one that inflates past `maxPayload`. |
| `1011` | Internal error | The engine could not continue. |

Codes `3000`–`4999` are yours to use; the parser accepts them and rejects
everything else outside the table above.

A client that sees `1001` reconnects with backoff, reloads its units and
re-opens its subscriptions. A client that sees `1009` will do the same, and
fail the same way — that one is a signal to send less, not to retry.
