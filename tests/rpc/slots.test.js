'use strict';

// A call or a subscription holds a slot (maxCalls, maxSubscriptions) from the
// moment it is registered. Whatever goes wrong between that and the code
// that answers it has to give the slot back and answer.

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const { defineRouter, procedure } = require('../../index.js');
const { bootServer, waitFor } = require('../helpers/server.js');

const deep = (depth) => '['.repeat(depth) + ']'.repeat(depth);
// 20,000 levels: JSON.parse reads them, JSON.stringify overflows the stack.
const DEEP_META = `{"x":${deep(20_000)}}`;

const router = defineRouter({
  u: {
    echo: procedure({ access: 'public', handler: async (context) => ({ meta: context.meta?.call ?? null }) }),
    feed: procedure({
      access: 'public',
      subscription: true,
      async *handler() {
        yield 1;
      },
    }),
  },
});

const rawSocket = async (t, url) => {
  const socket = new WebSocket(url);
  t.after(() => socket.close());
  const received = [];
  socket.addEventListener('message', (event) => received.push(JSON.parse(String(event.data))));
  await new Promise((resolve, reject) => {
    socket.addEventListener('open', resolve);
    socket.addEventListener('error', reject);
  });
  return { socket, received };
};

describe('slots (maxCalls, maxSubscriptions)', () => {
  it('should answer over HTTP a call whose meta is nested too deep to measure, the label dropped', async (t) => {
    const { origin, server } = await bootServer(t, { router });
    const response = await fetch(origin + server.rpc.basePath, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: `{"type":"call","id":"h1","method":"u/echo","args":{},"meta":${DEEP_META}}`,
      // It used to hang: the measure threw after the call took its slot and
      // before anything that answers it.
      signal: AbortSignal.timeout(3000),
    });
    assert.strictEqual(response.status, 200);
    const packet = await response.json();
    assert.strictEqual(packet.id, 'h1');
    assert.strictEqual(packet.error, undefined, 'the call runs, as it does for a bag over the size limit');
  });

  it('should not use up maxCalls on a socket with calls carrying such a meta', async (t) => {
    const { url, server } = await bootServer(t, { router, maxCalls: 3 });
    const { socket, received } = await rawSocket(t, url);
    for (let i = 0; i < 6; i++) {
      socket.send(`{"type":"call","id":"d${i}","method":"u/echo","args":{},"meta":${DEEP_META}}`);
    }
    await waitFor(() => received.length === 6, 'every call is answered');
    assert.ok(
      received.every((packet) => packet.type === 'callback' && packet.error === undefined),
      'none was refused',
    );
    const [client] = server.rpc.clients;
    assert.strictEqual(client.calls.size, 0, 'no slot is left taken');
    socket.send('{"type":"call","id":"after","method":"u/echo","args":{}}');
    await waitFor(() => received.some((packet) => packet.id === 'after'));
    assert.strictEqual(received.find((packet) => packet.id === 'after').error, undefined);
  });

  it('should not use up maxSubscriptions with subscriptions carrying such a meta', async (t) => {
    const { url, server } = await bootServer(t, { router, maxSubscriptions: 2 });
    const { socket, received } = await rawSocket(t, url);
    for (let i = 0; i < 5; i++) {
      socket.send(`{"type":"subscribe","id":"s${i}","method":"u/feed","args":{},"meta":${DEEP_META}}`);
    }
    await waitFor(() => received.filter((packet) => packet.type === 'end').length === 5, 'every subscription ends');
    assert.ok(
      received.filter((packet) => packet.type === 'end').every((packet) => packet.error?.code !== 429),
      'none was refused for capacity',
    );
    const [client] = server.rpc.clients;
    assert.strictEqual(client.subscriptions.size, 0);
  });

  it('should end the subscription and free its slot when a connection gate rejects', async (t) => {
    const { url, server } = await bootServer(t, { router });
    const { socket, received } = await rawSocket(t, url);
    const [client] = server.rpc.clients;
    // What a session restore that failed leaves behind.
    Object.defineProperty(client, 'isReady', { value: false, configurable: true });
    const failure = Promise.reject(Object.assign(new Error('store down'), { code: 503 }));
    failure.catch(() => {});
    Object.defineProperty(client, 'ready', { value: failure, configurable: true });
    socket.send('{"type":"subscribe","id":"s1","method":"u/feed","args":{}}');
    await waitFor(() => received.some((packet) => packet.type === 'end' && packet.id === 's1'));
    assert.strictEqual(received.find((packet) => packet.id === 's1').error.code, 503);
    assert.strictEqual(client.subscriptions.size, 0, 'the slot is given back');
  });

  it('should treat a caller timeout no timer can hold as no deadline at all', async (t) => {
    const { url } = await bootServer(t, { router });
    const { socket, received } = await rawSocket(t, url);
    const warnings = [];
    const onWarning = (warning) => warnings.push(warning.name);
    process.on('warning', onWarning);
    t.after(() => process.off('warning', onWarning));
    // 1e400 is Infinity once parsed; it used to arm a one-millisecond timer
    // (with a TimeoutOverflowWarning) and answer 408.
    socket.send('{"type":"call","id":"inf","method":"u/echo","args":{},"timeout":1e400}');
    socket.send('{"type":"call","id":"big","method":"u/echo","args":{},"timeout":4294967296}');
    socket.send('{"type":"call","id":"neg","method":"u/echo","args":{},"timeout":-5}');
    await waitFor(() => received.length === 3);
    assert.ok(
      received.every((packet) => packet.error === undefined),
      'every call ran to its answer',
    );
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepStrictEqual(warnings, []);
  });
});

describe('procedure timeout', () => {
  it('should refuse at definition a timeout that is not a delay a timer can hold', () => {
    for (const timeout of [Infinity, NaN, -1, 2 ** 31, '300']) {
      assert.throws(() => procedure({ timeout, handler: async () => 1 }), /timeout must be a number of milliseconds/);
    }
    assert.doesNotThrow(() => procedure({ timeout: 0, handler: async () => 1 }));
    assert.doesNotThrow(() => procedure({ timeout: 2 ** 31 - 1, handler: async () => 1 }));
  });
});
