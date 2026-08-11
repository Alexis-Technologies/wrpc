# Binary streams

Calls carry JSON. For bytes — a file upload, a video download, anything big
enough that buffering it would be a mistake — wrpc has a second framing:
**binary chunks**, interleaved on the same connection.

Two objects, one on each end:

- **`WrpcWritable`** — the sending side. `write(chunk)` reports whether the
  transport accepted it; `end()` closes the stream.
- **`WrpcReadable`** — the receiving side. Async-iterable, or `pipe()` into a
  node `Writable`, or `toBlob()` in a browser.

The stream id prefixes every chunk, which is what lets several streams
interleave over one connection without head-of-line blocking. Streams need a
persistent, binary-capable transport: not HTTP, and not [SSE](./sse).

## Uploading

Announce the stream, call the procedure with its id, then push the bytes:

```js
// client
const blob = new Blob([bytes]);
blob.name = 'video.mp4';                  // optional; defaults to 'blob'
const uploader = client.createBlobUploader(blob);

const receiving = client.api.media.receive({ id: uploader.id });
await uploader.upload();
console.log(await receiving);
```

```js
// server
receive: procedure({
  handler: async (context, { id }) => {
    const stream = context.client.getStream(id);
    for await (const chunk of stream) {
      await sink.write(chunk);
    }
    return { name: stream.name, size: stream.size };
  },
}),
```

Note the order: the call is issued **before** the upload finishes. The handler
gets the stream as soon as its opening packet has arrived and consumes chunks
as they land — the call's promise resolves when the handler returns.

Without a `Blob`, drive a `WrpcWritable` yourself:

```js
const drained = (stream) => new Promise((resolve) => stream.once('drain', resolve));

const stream = client.createStream('report.csv', bytes.length);
const receiving = client.api.media.receive({ id: stream.id });
for (const chunk of chunks) {
  if (!stream.write(chunk)) await drained(stream);
}
stream.end();
```

## Downloading

The same machinery, mirrored:

```js
// server
download: procedure({
  handler: async (context, { name }) => {
    const payload = await load(name);
    const stream = context.client.createStream(name, payload.length);
    queueMicrotask(() => {
      stream.write(payload);
      stream.end();
    });
    return { id: stream.id };
  },
}),
```

```js
// client
const { id } = await client.api.media.download({ name: 'report.csv' });
const readable = client.getStream(id);

const blob = await readable.toBlob('text/csv');   // browser
readable.pipe(fs.createWriteStream('report.csv')); // node
for await (const chunk of readable) { /* ... */ }  // either
```

`createStream()` sends the opening packet immediately, so writing from inside
the same tick as the `return` would race the caller's chance to register a
consumer — hence the `queueMicrotask`.

## Backpressure

This is the part that makes streams usable for real payloads, and it works end
to end:

```js
if (!stream.write(chunk)) {
  await new Promise((resolve) => stream.once('drain', resolve));
}
```

`write()` returns the **transport's real answer**: `false` means the socket
buffer is above its high-water mark. Wait for `'drain'` before writing more.
A transport with no flow-control reporting (a browser `WebSocket`, HTTP)
always reports accepted.

`false` can also mean the transport closed, and no `'drain'` will ever follow —
check `stream.closed`, or listen for `'close'`.

On the receiving side, `WrpcReadable` applies its own high-water mark
(`highWaterMark`, 32 chunks by default) once a consumer has attached, and the
server pauses socket reads while chunks are being consumed. So a slow consumer
pushes back through the queue, through the socket, and into TCP — the producer
on the other end sees `write()` return `false`. Nothing between them buffers
without bound.

::: tip Verified, not asserted
`tests/perf/stream-memory.perf.js` (`pnpm test:perf`) streams 1 GiB through and
fails if RSS grows with it.
:::

## Lifecycle

| Call | Effect |
| --- | --- |
| `writable.end()` | Normal end. The reader's iteration finishes. |
| `writable.terminate()` | Abnormal end — the reader stops where it is. |
| `readable.close()` | Waits for the announced `size`, then closes. |
| `readable.terminate()` | Stops now, discarding what is queued. |

A disconnect terminates every stream the client held. `readable.status` is
`'active'`, `'closed'` or `'terminated'`; `bytesRead` is what has been
consumed so far, against the announced `size`.

## Chunk framing

Each binary frame is one chunk of one stream:

```
┌────────┬──────────────────┬──────────────────────────┐
│ 1 byte │  id (idLength)   │        payload           │
│ idLen  │  utf-8 stream id │        bytes             │
└────────┴──────────────────┴──────────────────────────┘
```

`chunkEncode(id, payload)` / `chunkDecode(chunk)` are exported if you need to
speak it yourself. The Node build uses `Buffer`, the browser build
`TextEncoder`/`TextDecoder` — swapped through the package's `browser` field, so
you never choose. Full details in
[the wire format reference](../reference/wire-format#binary-chunks).
