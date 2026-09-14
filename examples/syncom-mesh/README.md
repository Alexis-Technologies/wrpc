# wrpc mesh example

Every tab is a wrpc peer: it serves a router and calls the routers of every
other tab, over WebRTC data channels — the server only signals. The wrpc
equivalent of a "who is in the room" demo, with the traffic itself peer to
peer.

```bash
node build.js    # bundles the browser client + webrtc peer into public/wrpc.browser.js
node server.js   # starts the signaling server on http://127.0.0.1:8000
```

Then open `public/index.html` directly in two or more tabs (no static file
server needed — it connects out to the signaling server over WebSocket and
to the other tabs over WebRTC). Each tab joins the `lobby` mesh; the list
shows every link and its state, **Broadcast** sends an event to every peer,
**Ask everyone** collects an answer from each, and **Send 1 MiB** streams a
blob to each peer over its data channel, with backpressure. Close a tab and
the others see it leave.

All tabs run on one machine, so `iceServers` is empty; across machines give
`WrpcPeer` a STUN (and, behind symmetric NATs, a TURN) server.

This example is not published with the package (`package.json#files` is an
explicit allowlist that omits `examples/`) — it requires the package's own
`node_modules` (for `esbuild`) and loads the library by relative path. In
your own app the two imports are `@alexify/wrpc` and `@alexify/wrpc/webrtc`,
bundled by your own tooling per [Browser & bundling](../../docs/guide/browser.md);
the full story is in the [WebRTC guide](../../docs/guide/webrtc.md).
