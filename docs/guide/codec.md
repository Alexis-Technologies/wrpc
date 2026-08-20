# Wire codec

By default every wrpc packet travels as JSON. The `codec` option replaces
that framing with your own — superjson, devalue, an encrypting wrapper —
injected on **both** sides:

```js
const codec = {
  encode: (packet) => superjson.stringify(packet),
  decode: (text) => superjson.parse(text),
  contentType: 'application/x-superjson',   // optional
};

// Server
new Server({ router, codec });

// Client
const client = await WrpcClient.connect(url, { codec });
```

The shape is structural — `isCodec(value)` is the check the options run —
and wrpc imports nothing: the codec is yours.

## Scope

The codec frames **wrpc packets**: WebSocket frames, packet-mode HTTP
(`POST {basePath}`, batches included), SSE `data:` payloads, worker ports.

It deliberately does **not** touch:

- **REST mode** — both the conventional `/:unit/:method` mode and
  [declaratively mapped routes](./rest) stay JSON. curl, browsers and
  swagger are that surface's audience; the codec is packet framing, not a
  body format.
- **SSE control frames** (`ready`, `gap`) — connection plumbing the client
  must parse before any codec could be negotiated.
- **Binary stream chunks** — already a binary protocol of their own.

## Text-only, single-line

`encode` must return a **single line of text** (no raw newlines): SSE
frames by line, and a batch is one frame. Binary codecs (msgpack, cbor) are
out of scope in v1 — a binary packet would be indistinguishable from a
stream chunk on a WebSocket, and could not ride SSE at all.

## The rules that follow

- **Both sides or neither.** A codec server answers a JSON client with
  malformed-packet errors, and vice versa — there is no negotiation.
- **One codec per server**, which is what keeps a room broadcast
  single-encode for the whole fan-out. The [ask](./rooms#asking-a-room)
  fan-out and client batching take a per-recipient / whole-array encode
  instead of the JSON fast paths — deliberate slow paths, only the codec
  knows its framing.
- **Mutually exclusive with compiled response serializers**
  ([`validation.serializer`](./router#validators)): a compiled serializer
  emits JSON the codec would have to re-frame. The server refuses the
  combination at construction.
- **Content-Type**: packet-mode HTTP requests and responses carry
  `codec.contentType` when set. Under the [fastify adapter](./adapters/fastify)
  a non-JSON content type needs an app-side `addContentTypeParser` (as raw
  text), or fastify rejects the body before wrpc sees it.
- The frozen [1.0 protocol](../reference/protocol) is JSON: a codec is an
  opt-in framing **outside** that interop promise. Two peers you control,
  one codec — that is the contract.
