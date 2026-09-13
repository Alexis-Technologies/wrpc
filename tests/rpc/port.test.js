'use strict';

// attachPort end to end over a real node:worker_threads MessageChannel — the
// seam the Service Worker client and the testing guide rely on. Two things
// this pins that nothing used to: a port-attached client is PERSISTENT
// (subscriptions, events and streams are admitted, not refused with 400),
// and a binary chunk arriving as a Buffer is routed to the stream, not to
// the JSON parser.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');
const { MessageChannel } = require('node:worker_threads');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure } = require('../../src/rpc/router.js');
const { chunkEncode } = require('../../src/chunks.js');

const quiet = { log() {}, info() {}, warn() {}, error() {}, debug() {} };

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 300; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

const boot = (t) => {
  const received = { events: [], uploads: [] };
  const router = defineRouter({
    feed: {
      count: procedure({
        access: 'public',
        handler: async function* (_context, { to }) {
          for (let n = 1; n <= to; n++) yield { n };
        },
      }),
      readUpload: procedure({
        access: 'public',
        handler: async (context, { id }) => {
          const stream = context.client.getStream(id);
          const chunks = [];
          for await (const chunk of stream) chunks.push(Buffer.from(chunk));
          const data = Buffer.concat(chunks).toString('utf8');
          received.uploads.push(data);
          return { data };
        },
      }),
      on: {
        ping: procedure({ access: 'public', handler: async (_context, data) => void received.events.push(data) }),
      },
    },
  });
  const rpc = new RpcServer({ router, logger: quiet });
  const { port1, port2 } = new MessageChannel();
  const client = rpc.attachPort(port1);
  const packets = [];
  port2.on('message', (data) => void packets.push(typeof data === 'string' ? JSON.parse(data) : data));
  t.after(() => {
    port1.close();
    port2.close();
    return rpc.close();
  });
  return { rpc, client, port: port2, packets, received };
};

test('attachPort: the client is persistent', async (t) => {
  const { client } = boot(t);
  assert.strictEqual(client.persistent, true);
  assert.strictEqual(client.binary, true);
});

test('attachPort: subscriptions are admitted and delivered', async (t) => {
  const { port, packets } = boot(t);
  port.postMessage(JSON.stringify({ type: 'subscribe', id: 's1', method: 'feed/count', args: { to: 2 } }));
  await waitFor(() => packets.some((p) => p.type === 'end'), 'the subscription to end');
  assert.deepStrictEqual(
    packets.map((p) => [p.type, p.data ?? p.error?.code ?? null]),
    [
      ['data', { n: 1 }],
      ['data', { n: 2 }],
      ['end', null],
    ],
  );
});

test('attachPort: inbound events reach the unit handler', async (t) => {
  const { port, received } = boot(t);
  port.postMessage(JSON.stringify({ type: 'event', name: 'feed/ping', data: { hello: true } }));
  await waitFor(() => received.events.length === 1, 'the event handler to run');
  assert.deepStrictEqual(received.events, [{ hello: true }]);
});

test('attachPort: a binary chunk posted as a Buffer lands in the stream', async (t) => {
  const { port, packets } = boot(t);
  const id = 'up-1';
  const payload = 'bytes over a port';
  port.postMessage(JSON.stringify({ type: 'stream', id, name: 'upload', size: payload.length }));
  port.postMessage(JSON.stringify({ type: 'call', id: 'c1', method: 'feed/readUpload', args: { id } }));
  // A Buffer is a Uint8Array; it used to be checked first and sent to the
  // JSON parser, which answered "Packet structure error" instead.
  port.postMessage(Buffer.from(chunkEncode(id, Buffer.from(payload))));
  port.postMessage(JSON.stringify({ type: 'stream', id, status: 'end' }));
  await waitFor(() => packets.some((p) => p.type === 'callback' && p.id === 'c1'), 'the upload call to answer');
  const answer = packets.find((p) => p.type === 'callback' && p.id === 'c1');
  assert.deepStrictEqual(answer.result, { data: payload });
  assert.ok(!packets.some((p) => p.error), `no error packets expected: ${JSON.stringify(packets)}`);
});

test('attachPort: a plain Uint8Array chunk still works and other clones are ignored', async (t) => {
  const { port, packets } = boot(t);
  const id = 'up-2';
  port.postMessage(JSON.stringify({ type: 'stream', id, name: 'upload', size: 2 }));
  port.postMessage(JSON.stringify({ type: 'call', id: 'c2', method: 'feed/readUpload', args: { id } }));
  port.postMessage({ not: 'on the wire' });
  port.postMessage(new Uint8Array(chunkEncode(id, new TextEncoder().encode('ok'))));
  port.postMessage(JSON.stringify({ type: 'stream', id, status: 'end' }));
  await waitFor(() => packets.some((p) => p.type === 'callback' && p.id === 'c2'), 'the upload call to answer');
  assert.deepStrictEqual(packets.find((p) => p.id === 'c2').result, { data: 'ok' });
  assert.ok(!packets.some((p) => p.error));
});
