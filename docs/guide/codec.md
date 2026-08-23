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
  [declaratively mapped routes](./rest) stay JSON *unless the codec carries
  a [`rest` section](#rest-bodies-codec-rest)*. curl, browsers and swagger
  are that surface's default audience; the packet codec is packet framing,
  not a body format.
- **SSE control frames** (`ready`, `gap`) — connection plumbing the client
  must parse before any codec could be negotiated.
- **Binary stream chunks** — already a binary protocol of their own.

## Text-only, single-line

`encode` must return a **single line of text** (no raw newlines): SSE
frames by line, and a batch is one frame. Binary codecs (msgpack, cbor) are
out of scope for the *packet* half — a binary packet would be
indistinguishable from a stream chunk on a WebSocket, and could not ride
SSE at all. REST bodies have no such collision, which is exactly what the
`rest` section below exists for.

## REST bodies: codec.rest {#rest-bodies-codec-rest}

The optional `rest` section of the same codec object re-frames **REST
bodies** — and here binary is fine: an HTTP body is a whole value with no
framing to collide with. msgpack fits naturally:

```js
const codec = {
  rest: {
    encode: (value) => msgpack.encode(value),          // Buffer/Uint8Array ok
    decode: (body) => msgpack.decode(body),            // body: Buffer on the server, Uint8Array on the client
    contentType: 'application/msgpack',
  },
};

new Server({ router, codec });                          // rest-only codecs are valid
const client = await WrpcClient.connect(url, { codec }); // the REST leg speaks it too
```

What it encodes are **values, not packets**: the plain result of a
[declaratively mapped route](./rest), the wire error object
(`{ message, code, details? }`), the request body — and, in the
conventional `/:unit/:method` mode, the callback envelope that IS that
mode's body. One opt-in governs **both** REST modes, requests and
responses, errors included; `contentType` rides on all of them in both
directions. A request body the codec cannot decode answers `400`.

The two halves are independent: a codec may carry only `rest` (packets stay
JSON), only the packet half (REST stays JSON), or both. The rules that
follow:

- **Both sides or neither**, exactly like the packet half — there is no
  negotiation.
- **The core hosts serve it natively** — the node shell, express and uws
  pass bodies through as Buffers untouched. Express 4 with a global
  `express.json()` would consume JSON-typed bodies before wrpc reads them;
  a binary `contentType` is unaffected.
- **The fastify adapter refuses `codec.rest` next to delegated REST
  routes** (procedures with `http` mappings): delegation exists *for*
  fastify's serialization, schemas and swagger, and a codec-framed body
  would silently bypass them. Serve binary REST from a core host, or drop
  the mappings under the plugin. `codec.rest` without mappings registers
  normally.
- Like the packet codec, `codec.rest` is an opt-in framing **outside** the
  frozen 1.0 interop promise.

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
