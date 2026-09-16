# wrpc over WebTransport

One `Server`, two doors: the WebSocket on TCP and an HTTP/3 host on UDP, with
`transport: ['wt', 'ws']` in the page picking WebTransport where the browser
has it and falling back to the WebSocket otherwise. A tab on each ends up in
the same room.

```bash
node scripts/wt-cert.js certs   # a 13-day ECDSA P-256 certificate + its hash (browsers require both)
node examples/wt/build.js       # bundles the browser client into public/wrpc.browser.js
node examples/wt/server.js      # http://127.0.0.1:8001 for the page, tcp/8000 for the wrpc Server, udp/4433 for HTTP/3
```

Open `http://127.0.0.1:8001/` in Chrome (or any browser with WebTransport —
Baseline since Safari 26.4); the status line names the transport that won.
Use `127.0.0.1`, not `localhost`. The hash the page hands to
`serverCertificateHashes` comes from `/cert-hash.json`, so a regenerated
certificate needs only a reload; after 14 days a browser refuses it — run
the cert script again.

The HTTP/3 host is [`@fails-components/webtransport`](https://github.com/fails-components/webtransport),
a devDependency of this repository (its native binary needs the install
script pnpm runs for it — see `package.json#pnpm.onlyBuiltDependencies`).
The package itself depends on nothing: see [the guide](../../docs/guide/wt.md)
for the session contract and the quico alternative.
