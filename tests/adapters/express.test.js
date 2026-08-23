'use strict';

const test = require('node:test');
const assert = require('node:assert');
const http = require('node:http');
const { EventEmitter } = require('node:events');

const { WrpcClient, defineRouter, procedure } = require('../../index.js');
const { createWrpc } = require('../../express.js');
const { createUwsEngine } = require('../../uws.js');
const { optional, requireUws } = require('./boots.js');

const express = optional('express');
const uws = requireUws();

const noExpress = express ? false : 'express unavailable';
const noUws = uws ? false : 'uWebSockets.js unavailable';

const createRouter = () =>
  defineRouter({
    probe: {
      echo: procedure({ access: 'public', handler: async (_context, args) => args }),
    },
  });

const postPacket = (url, method, args) =>
  fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'call', id: '1', method, args }),
  });

// The composition the adapter is written for: wrpc is middleware, express
// keeps its own routes, and the listener belongs to the app.
const boot = async (t, options = {}) => {
  const { parseJson = false, ...wrpcOptions } = options;
  const app = express();
  const wrpc = createWrpc({ router: createRouter(), logger: false, ...wrpcOptions });
  const bodies = [];
  if (parseJson) app.use(express.json());
  // Records what the wrpc handler is about to see: `undefined` on the raw
  // path, an already-parsed object behind express.json().
  app.use((req, _res, next) => {
    bodies.push(req.body);
    next();
  });
  app.use(wrpc.handler);
  app.get('/later', (_req, res) => void res.send('later route'));

  const httpServer = http.createServer(app);
  httpServer.on('upgrade', wrpc.upgrade);
  // Registered before listen(): a failing assertion must not leave a live
  // server (and its engine) behind.
  t.after(async () => {
    await wrpc.close().catch(() => {});
    httpServer.closeAllConnections();
    await new Promise((resolve) => httpServer.close(() => resolve()));
  });
  await new Promise((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
  const { port } = httpServer.address();
  return { app, wrpc, httpServer, bodies, port, origin: `http://127.0.0.1:${port}` };
};

test('a path outside basePath falls through to the rest of the app', { skip: noExpress }, async (t) => {
  const { origin } = await boot(t);
  const res = await fetch(`${origin}/later`);
  assert.strictEqual(res.status, 200, 'wrpc called next() instead of answering 404');
  assert.strictEqual(await res.text(), 'later route');
});

test('raw body: a packet POST works with no body parser installed', { skip: noExpress }, async (t) => {
  const { origin, bodies } = await boot(t);
  const res = await postPacket(`${origin}/api`, 'probe/echo', { a: 1 });
  assert.strictEqual(res.status, 200);
  assert.deepStrictEqual((await res.json()).result, { a: 1 });
  assert.strictEqual(bodies[0], undefined, 'nothing parsed the body: the adapter read the stream');
});

test('parsed body: express.json() upstream is re-serialized for the core', { skip: noExpress }, async (t) => {
  const { origin, bodies } = await boot(t, { parseJson: true });
  const res = await postPacket(`${origin}/api`, 'probe/echo', { a: 1 });
  assert.strictEqual(res.status, 200);
  assert.deepStrictEqual((await res.json()).result, { a: 1 });
  assert.deepStrictEqual(bodies[0], { type: 'call', id: '1', method: 'probe/echo', args: { a: 1 } });
});

test('a body over maxBodySize gets the 400 error packet', { skip: noExpress }, async (t) => {
  const { origin } = await boot(t, { maxBodySize: 256 });
  const res = await fetch(`${origin}/api`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'call', id: '1', method: 'probe/echo', args: { pad: 'x'.repeat(4096) } }),
  });
  assert.strictEqual(res.status, 400);
  const packet = await res.json();
  assert.strictEqual(packet.type, 'callback');
  assert.strictEqual(packet.error.code, 400);
  assert.match(packet.error.message, /Body size limit exceeded/);
});

test('a standalone engine cannot be driven as middleware', { skip: noUws }, () => {
  const engine = createUwsEngine({ uws });
  try {
    assert.throws(() => createWrpc({ router: createRouter(), logger: false, engine }), /owns its own network stack/);
  } finally {
    engine.close();
  }
});

test('a non-Engine options.engine is refused at the boundary', () => {
  assert.throws(
    () => createWrpc({ router: createRouter(), logger: false, engine: {} }),
    /does not implement the Engine contract/,
  );
});

test('an engine that cannot be upgraded by hand is refused', () => {
  // Hosted, valid, but its source exposes no handleUpgrade — there is no way
  // to drive the handshake from the app's own 'upgrade' listener.
  const source = new EventEmitter();
  const engine = { name: 'fake', attach: () => source, close: () => {} };
  assert.throws(
    () => createWrpc({ router: createRouter(), logger: false, engine }),
    /does not support manual upgrades/,
  );
});

test('the upgrade listener carries WebSocket RPC', { skip: noExpress }, async (t) => {
  const { port } = await boot(t);
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
  t.after(() => void client.close());
  await client.load('probe');
  assert.deepStrictEqual(await client.api.probe.echo({ b: 2 }), { b: 2 });
});

test('close() tears the core down', { skip: noExpress }, async (t) => {
  const { wrpc, port } = await boot(t);
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
  t.after(() => void client.close());
  await client.load('probe');
  assert.strictEqual(wrpc.rpc.clients.size, 1);

  const dropped = new Promise((resolve) => client.on('close', resolve));
  await wrpc.close();
  assert.strictEqual(wrpc.rpc.clients.size, 0);
  await dropped;
});

test('codec.rest: a binary REST body round-trips through the middleware', { skip: noExpress }, async (t) => {
  const codec = {
    rest: {
      contentType: 'application/x-wrpc-bin',
      encode: (value) => Buffer.concat([Buffer.from([0xab]), Buffer.from(JSON.stringify(value ?? null))]),
      decode: (body) => {
        const buffer = Buffer.isBuffer(body) ? body : Buffer.from(body);
        if (buffer[0] !== 0xab) throw new Error('bad frame');
        return JSON.parse(buffer.subarray(1).toString());
      },
    },
  };
  const router = defineRouter({
    projects: {
      create: procedure({
        access: 'public',
        http: { method: 'POST', path: '/projects/:orgId', status: 201 },
        handler: async (_ctx, { params, body }) => ({ orgId: params.orgId, name: body?.name }),
      }),
    },
  });
  const { origin } = await boot(t, { router, codec });
  const res = await fetch(`${origin}/api/projects/9`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-wrpc-bin' },
    body: codec.rest.encode({ name: 'Bin' }),
  });
  assert.strictEqual(res.status, 201);
  assert.strictEqual(res.headers.get('content-type'), 'application/x-wrpc-bin');
  assert.deepStrictEqual(codec.rest.decode(Buffer.from(await res.arrayBuffer())), { orgId: '9', name: 'Bin' });
});
