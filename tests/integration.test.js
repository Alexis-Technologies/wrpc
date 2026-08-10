'use strict';

const timers = require('node:timers/promises');
const { Blob } = require('node:buffer');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, WrpcClient, defineRouter, procedure } = require('../index.js');

const { emitWarning } = process;
process.emitWarning = (warning, type, ...args) => {
  if (type === 'ExperimentalWarning') return;
  emitWarning(warning, type, ...args);
};

const noop = () => {};
const quietConsole = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const createServer = async (router) => {
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    console: quietConsole,
    timeouts: { bind: 100 },
  });
  await server.listen();
  const { port } = server.httpServer.address();
  return { server, port };
};

test('Integration / WrpcClient with Server', async (t) => {
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
          await context.client.emit('test/ping', { ping: true });
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

  const { server, port } = await createServer(router);
  t.after(async () => {
    await server.close();
  });

  await t.test('WS RPC: load and call public method', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    const result = await client.api.test.hello({ name: 'Max' });
    assert.strictEqual(result, 'Hello, Max');
  });

  await t.test('HTTP RPC: load and call public method', async (sub) => {
    const client = await WrpcClient.connect(`http://127.0.0.1:${port}/api`);
    sub.after(() => void client.close());
    await client.load('test');
    const result = await client.api.test.hello({ name: 'Ada' });
    assert.strictEqual(result, 'Hello, Ada');
  });

  await t.test('WS RPC: propagates method errors', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    await assert.rejects(client.api.test.fail(), (error) => error.message === 'Boom' && error.code === 400);
  });

  await t.test('WS RPC: rejects session-only method without session', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    await assert.rejects(client.api.test.secret(), (error) => error.code === 403);
  });

  await t.test('WS events: server emit reaches client unit', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    const ping = new Promise((resolve) => client.api.test.on('ping', resolve));
    const result = await client.api.test.notify();
    assert.deepStrictEqual(result, { ok: true });
    assert.deepStrictEqual(await ping, { ping: true });
  });

  await t.test('WS streams: client upload is readable on server', async (sub) => {
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

  await t.test('WS streams: upload before the reading call does not deadlock', async (sub) => {
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

  await t.test('WS streams: server download readable on client', async (sub) => {
    const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
    sub.after(() => void client.close());
    await client.load('test');
    const { id } = await client.api.test.download({ name: 'download-stream' });
    const readable = client.getStream(id);
    const blob = await readable.toBlob();
    assert.strictEqual(await blob.text(), 'hello from server');
  });
});
