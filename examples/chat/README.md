# wrpc chat example

The [getting-started](../../docs/guide/getting-started.md) snippets, wired
into one room and one page — the wrpc equivalent of socket.io's chat demo.

```bash
node build.js    # bundles the browser client into public/wrpc.browser.js
node server.js   # starts the wrpc server on http://127.0.0.1:8000
```

Then open `public/index.html` directly in a browser (no static file server
needed — it connects out to the wrpc server over WebSocket). Open it in two
tabs to see messages arrive on both.

This example is not published with the package (`package.json#files` is an
explicit allowlist that omits `examples/`) — it requires the package's own
`node_modules` (for `esbuild`) and loads the library by relative path since
it isn't published to npm yet. Once it is, swap `require('../../index.js')`
for `require('@alexify/wrpc')` and drop `build.js` in favor of your own
bundler config, per [Browser & bundling](../../docs/guide/browser.md).
