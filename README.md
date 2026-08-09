# wrpc

Fast and low overhead, zero-dependency, WebSocket-based RPC protocol for Node.js and browsers.

## Installation

```bash
pnpm add @alexify/wrpc
```

### Browser usage

The package ships a browser entry (`browser.js`), resolved automatically by bundlers that honor the `package.json` `browser` field (webpack, Vite, esbuild `platform: browser`, Rollup, Parcel, Bun). It exposes `WrpcClient`/`WrpcClientProxy` and the stream/chunk helpers, and excludes the Node-only server code.

## Quick Start

```js
const { Server, WrpcClient } = require('@alexify/wrpc');

class Procedure {
  constructor({ access = 'public', handler }) {
    this.access = access;
    this.handler = handler;
  }
  async enter() {}
  leave() {}
  invoke(context, args) {
    return this.handler(args, context);
  }
}

const api = {
  system: {
    introspect: { handler: async () => api },
  },
  greeting: {
    hello: { handler: async ({ name }) => `Hello, ${name}` },
  },
};

const application = {
  console,
  auth: { saveSession: async () => {} },
  getMethod: (unit, _version, method) => {
    const def = api[unit]?.[method];
    return def ? new Procedure(def) : null;
  },
};

const main = async () => {
  const server = new Server(application, { host: '127.0.0.1', port: 8000, protocol: 'http' });
  await server.listen();

  const client = await WrpcClient.connect('ws://127.0.0.1:8000/');
  await client.load('greeting');
  console.log(await client.api.greeting.hello({ name: 'World' }));
};

main();
```

## API Reference

See [index.d.ts](./index.d.ts) for the full public surface: `WrpcClient`/`WrpcClientProxy` (client), `Server`/`Client`/`Context`/`Session` (server), `WrpcReadable`/`WrpcWritable` (streams), and `WebsocketServer`/`Connection`/`Frame`/`FrameParser` (the underlying WebSocket implementation).

## Benchmarks

_Coming soon — see `bench/bench.js`._

## Changelog

See [CHANGELOG.md](./CHANGELOG.md).

## License

MIT — see [LICENSE](./LICENSE).
