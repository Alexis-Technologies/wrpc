# `src/webtransport/` — WebTransport (HTTP/3)

Cross-cutting rules — the protocol revision, no runtime dependencies, commands, the package layout — are in the root `CLAUDE.md`; the layering of `src/` and the hot-path conventions are in `src/CLAUDE.md`. A rule lives in ONE file: add to the file of the folder it is about.

`src/webtransport/` — WebTransport (HTTP/3), behind the `@alexify/wrpc/wt` subpath (experimental) for the SERVER half; the client transport (`transport: 'wt'`, `src/client/webtransport.js`) is in the base entry so a `['wt', 'ws']` fallback list needs no import. The server is DI-only: Node speaks no HTTP/3 (`node:quic` was spiked and rejected), so the session comes from an injected implementation and is validated structurally.

- `port.js` — wrpc's OWN W3C-shaped session contract (`isWtSession`/`isWtStream`/`isWtDatagrams`); binds to no library.
- `channel.js` — `WtChannel`, the ONE core both ends run on: the control stream's framing, the outbound queue and its water marks, `maxBackpressure`, the per-message codec behind a `Sequencer`, datagrams (64 in flight, then dropped), the side-stream mux, graceful close (stream first, then the session, bounded by `closeTimeout`). Its callbacks are POSITIONAL on purpose — option keys do not minify, and the main browser entry had no bytes for them. `closeQuietly` lives here, not in `port.js`.
- `framing.js` (length-prefixed frames, kinds 0–4) and `streams.js` (`StreamMux`: one uni stream per binary wrpc stream, refused past `maxHeldStreams`/`holdTimeout`, never read before its open packet) — browser-budgeted with `channel.js`.
- `socket.js` (`WtSocket`, the `WrpcSocket` shim an `RpcServer.attachSocket` takes) and `src/client/webtransport.js` (`ClientWtTransport`) are thin adapters over `WtChannel`; `tests/wt/channelContract.js` is the behavioural suite both satisfy.
- `transport.js` (`ServerWtTransport`, registered as `ServerTransport.transport.wt`), `index.js` (`attachSession`, `acceptSessions` — concurrent, `maxPending`, one `acceptTimeout` per handshake, `onError` contained — and `fromFails`/`failsRequestCallback`), `quico.js` (`fromQuico`: the pure-JS stack's node-stream shape bridged to the port).

`StreamMux` remembers at most `MAX_ANNOUNCED` in `src/webtransport/streams.js` stream announcements nobody has claimed yet.
