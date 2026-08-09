# Getting Started

## Installation

```bash
pnpm add @alexify/wrpc
```

## Quick Start

Start a server, connect a client over WebSocket, and call a remote method:

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

The same call also works over plain HTTP — connect with an `http://` URL
(`WrpcClient.connect('http://127.0.0.1:8000/api')`) and every call becomes a
`POST` request carrying the same JSON packet.

> The server-side `application` contract (`getMethod` + procedure objects) is
> transitional: it is being replaced by a router/procedure API in an upcoming
> release. The client API (`connect`, `load`, `api.unit.method()`) is stable.
