'use strict';

const timers = require('node:timers/promises');
const { Blob } = require('node:buffer');
const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');

const { Server, WrpcClient, connect, defineRouter, procedure } = require('../index.js');

const { emitWarning } = process;
process.emitWarning = (warning, type, ...args) => {
  if (type === 'ExperimentalWarning') return;
  emitWarning(warning, type, ...args);
};
// Put back for whatever runs after this file in the same process.
after(() => {
  process.emitWarning = emitWarning;
});

const createServer = async (router) => {
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    logger: false,
    timeouts: { bind: 100 },
  });
  await server.listen();
  const { port } = server.address();
  return { server, port };
};

describe('Integration / WrpcClient with Server', () => {
  const router = defineRouter({
    test: {
      hello: procedure({
        access: 'public',
        handler: async (_context, { name }) => {
          await timers.setTimeout(10);
          return `Hello, ${name}`;
        },
      }),
      fail: procedure({
        access: 'public',
        handler: async () => {
          const error = new Error('Boom');
          error.code = 400;
          throw error;
        },
      }),
      secret: procedure({
        access: 'session',
        handler: async () => 'secret',
      }),
      notify: procedure({
        access: 'public',
        handler: async (context) => {
          context.client.sendEvent('test/ping', { ping: true });
          return { ok: true };
        },
      }),
      readUpload: procedure({
        access: 'public',
        handler: async (context, { id }) => {
          const stream = context.client.getStream(id);
          const chunks = [];
          for await (const chunk of stream) chunks.push(Buffer.from(chunk));
          return {
            name: stream.name,
            size: stream.size,
            data: Buffer.concat(chunks).toString('utf8'),
          };
        },
      }),
      download: procedure({
        access: 'public',
        handler: async (context, { name }) => {
          const payload = Buffer.from('hello from server');
          const stream = context.client.createStream(name, payload.length);
          queueMicrotask(() => {
            stream.write(payload);
            stream.end();
          });
          return { id: stream.id };
        },
      }),
    },
  });

  let server = null;
  let port = 0;

  before(async () => {
    ({ server, port } = await createServer(router));
  });

  after(async () => {
    await server?.close();
  });

  it('should load and call a public method over WS RPC', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    const result = await client.api.test.hello({ name: 'Max' });
    assert.strictEqual(result, 'Hello, Max');
  });

  it('should load and call a public method over HTTP RPC', async (sub) => {
    const client = await WrpcClient.connect(`http://127.0.0.1:${port}/api`);
    sub.after(() => void client.close());
    await client.load('test');
    const result = await client.api.test.hello({ name: 'Ada' });
    assert.strictEqual(result, 'Hello, Ada');
  });

  it('should propagate method errors over WS RPC', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    await assert.rejects(client.api.test.fail(), (error) => error.message === 'Boom' && error.code === 400);
  });

  it('should reject a session-only method without a session over WS RPC', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    await assert.rejects(client.api.test.secret(), (error) => error.code === 403);
  });

  it('should bring a server emit to the client unit over WS events', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    const ping = new Promise((resolve) => client.api.test.on('ping', resolve));
    const result = await client.api.test.notify();
    assert.deepStrictEqual(result, { ok: true });
    assert.deepStrictEqual(await ping, { ping: true });
  });

  it('should make a client upload readable on the server over WS streams', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    const data = 'Some random data for upload to the server';
    const blob = new Blob([data]);
    blob.name = 'upload-stream';
    const uploader = client.createBlobUploader(blob);
    const resultPromise = client.api.test.readUpload({ id: uploader.id });
    await uploader.upload();
    const uploaded = await resultPromise;
    assert.strictEqual(uploaded.name, 'upload-stream');
    assert.strictEqual(uploaded.size, blob.size);
    assert.strictEqual(uploaded.data, data);
  });

  it('should not deadlock on an upload sent before the reading call over WS streams', async (sub) => {
    // Regression: all chunks (past the 32-chunk high-water mark) arrive
    // before the call that starts the consumer. The receive-side pause
    // used to deadlock here: pushes blocked on the high-water mark, the
    // socket stayed paused, and the consumer-starting call was never read.
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    const chunk = 'x'.repeat(1024);
    const chunkCount = 48;
    const consumer = client.createStream('bulk-upload', chunk.length * chunkCount);
    for (let i = 0; i < chunkCount; i++) consumer.write(Buffer.from(chunk));
    consumer.end();
    const uploaded = await client.api.test.readUpload({ id: consumer.id });
    assert.strictEqual(uploaded.name, 'bulk-upload');
    assert.strictEqual(uploaded.size, chunk.length * chunkCount);
    assert.strictEqual(uploaded.data, chunk.repeat(chunkCount));
  });

  it('should make a server download readable on the client over WS streams', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    const { id } = await client.api.test.download({ name: 'download-stream' });
    const readable = client.getStream(id);
    const blob = await readable.toBlob();
    assert.strictEqual(await blob.text(), 'hello from server');
  });

  // The typed-client entry point. `connect` exists so a contract type has a
  // natural place to go (`connect<Api>(url)`); at runtime it must be the very
  // same call, options and all.
  it('should make connect() WrpcClient.connect', async (sub) => {
    const client = await connect(`ws://127.0.0.1:${port}/`, { callTimeout: 3000 });
    sub.after(() => void client.close());
    assert.ok(client instanceof WrpcClient);
    await client.load('test');
    assert.strictEqual(await client.api.test.hello({ name: 'World' }), 'Hello, World');
  });
});
