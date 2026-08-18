# Errors and close codes

wrpc reuses **HTTP status numbers** as its error vocabulary, on every transport.
A code that reaches a WebSocket client means the same thing it would over
HTTP — and in [REST mode](../guide/server#what-it-serves) it literally *is* the
HTTP status.

## The error shape

An error travels inside a `callback` packet:

```json
{ "type": "callback", "id": "7", "error": { "message": "Not found", "code": 404 } }
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

`WrpcError` is `Error` plus a numeric `code`. There is no error *class* per
code on purpose: the number crosses the wire, subclasses do not.

## Throwing from a handler

Attach `code` to any error and it becomes the wire code:

```js
handler: async (context, { id }) => {
  const row = await db.find(id);
  if (!row) {
    const error = new Error('No such document');
    error.code = 404;
    throw error;
  }
  return row;
};
```

::: warning 5xx messages do not travel
A 4xx is part of the protocol conversation and its message reaches the peer. A
**5xx is a server internal**: the message is replaced with the generic status
text on the wire and the real one goes to the log. Opt out per error with
`error.expose = true`. Stack traces never travel.
:::

A code above `599` is reported as `500`.

## Codes the library itself sends

| Code | Meaning | Where it comes from |
| --- | --- | --- |
| `400` | Malformed packet, unparseable frame, duplicate in-flight call id, invalid arguments, `{type:'call'}` against a subscription | dispatcher, [validators](../guide/router#validators) |
| `403` | No [session](../guide/sessions) for `access: 'session'`; an SSE channel presented without its cookie identity; a cross-site `GET` | core, [SSE](../guide/sse) |
| `404` | No such unit, version or procedure | router lookup |
| `408` | Procedure `timeout` elapsed, or an [ask](../guide/rooms#asking-a-room) went unanswered | router, `expectAnswer` |
| `409` | Unknown SSE channel — the server no longer holds that id | SSE |
| `429` | `maxCalls` in flight on this connection, or `maxChannelsPerAddress` for SSE | dispatcher, SSE |
| `499` | Cancelled by the caller — `{type:'cancel'}`, or an aborted `signal` | dispatcher, client |
| `500` | Handler threw without a code; a subscription handler returned a non-iterable; an `output` validator rejected the result | router |
| `501` | No responder registered for an [ask](../guide/rooms#asking-a-room); a binary stream attempted over [SSE](../guide/sse#what-it-cannot-do) | client, SSE |
| `503` | Queue full, server [draining](../guide/production#graceful-shutdown), transport closed under an in-flight call, `maxChannels` reached | router, core, SSE |

::: tip 499 is not a failure
It acknowledges a cancel the caller asked for. The client resolves the
already-rejected call silently rather than surfacing it twice — you see the
`AbortError` you caused, not a second error from the server.
:::

## Which codes are worth retrying

| Code | Retry? | Why |
| --- | --- | --- |
| `400`, `403`, `404`, `501` | **No** | The same request will fail identically. |
| `408` | Careful | Only if the operation is idempotent — it may have completed. |
| `409` | Automatic | The SSE transport starts a fresh channel itself. |
| `429`, `503` | **Yes, with backoff** | Load or a rolling deploy; both are temporary by construction. |
| `499` | No | You caused it. |
| `500` | No, alert | A bug, not a condition. |

The client's own [reconnect](../guide/client#reconnecting) already applies
truncated exponential backoff with full jitter to the *connection*. Per-call
retries are yours: wrpc never replays a call automatically, because it cannot
know whether yours is idempotent.

## WebSocket close codes

Close codes are a different vocabulary — RFC 6455's, not HTTP's — and they end
the *connection*, not one call. Exported as `CLOSE_CODES` from
[`@alexify/wrpc/ws`](./wire-format).

| Code | Name | When wrpc uses it |
| --- | --- | --- |
| `1000` | Normal closure | `client.close()`, a clean goodbye. |
| `1001` | Going away | The server is [shutting down](../guide/production#graceful-shutdown). Reconnect elsewhere. |
| `1002` | Protocol error | A frame violated RFC 6455 — bad opcode, bad continuation, reserved bit set. |
| `1006` | Abnormal closure | No close frame arrived. Never sent — it is what a local socket reports. |
| `1007` | Invalid payload | A text frame that was not valid UTF-8. |
| `1009` | Message too big | Over `maxPayload`. |
| `1011` | Internal error | The engine could not continue. |

Codes `3000`–`4999` are yours to use; the parser accepts them and rejects
everything else outside the table above.

A client that sees `1001` reconnects with backoff, reloads its units and
re-opens its subscriptions. A client that sees `1009` will do the same, and
fail the same way — that one is a signal to send less, not to retry.
