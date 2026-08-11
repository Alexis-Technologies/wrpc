'use strict';

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, WrpcClient, Emitter, defineRouter, procedure } = require('../../index.js');
const { jsonParse } = require('../../src/utils.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

const settle = async () => {
  for (let i = 0; i < 8; i++) await Promise.resolve();
};

// A transport with no socket under it: batching is about which frames leave
// the client, and that is exactly what this records.
class FakeTransport extends Emitter {
  active = false;
  heartbeat = false;

  constructor() {
    super({ maxListeners: Number.MAX_SAFE_INTEGER });
    this.frames = [];
  }

  async open() {
    this.active = true;
    await this.emit('open');
  }

  close() {
    this.active = false;
    this.emit('close');
  }

  send(obj) {
    this.frames.push(obj);
    return true;
  }

  write(data) {
    this.frames.push(jsonParse(data));
    return true;
  }

  /** Feeds a packet back as if the peer had sent it. */
  deliver(packet) {
    return this.emit('message', JSON.stringify(packet));
  }

  /** Answers every call in the frames recorded so far. */
  answer(handler) {
    const frames = this.frames.splice(0, this.frames.length);
    for (const frame of frames) {
      const packets = Array.isArray(frame) ? frame : [frame];
      for (const packet of packets) {
        if (packet.type !== 'call') continue;
        this.deliver(handler(packet));
      }
    }
    return frames;
  }
}

const createFake = async (options = {}) => {
  const transport = new FakeTransport();
  const client = new WrpcClient('ws://fake', transport, { heartbeat: false, ...options });
  await client.open();
  return { client, transport };
};

test('batching: which frames leave the client', async (t) => {
  const call = (client, n) => client.send({ type: 'call', id: `n${n}`, method: 'math/double', args: { n } });

  await t.test('without batching each call is its own frame', async () => {
    const { client, transport } = await createFake();
    t.after(() => client.close());
    call(client, 1);
    call(client, 2);
    await settle();
    assert.strictEqual(transport.frames.length, 2);
    assert.strictEqual(Array.isArray(transport.frames[0]), false);
  });

  await t.test('with batching a tick of calls coalesces into one array', async () => {
    const { client, transport } = await createFake({ batch: true });
    t.after(() => client.close());
    call(client, 1);
    call(client, 2);
    call(client, 3);
    assert.strictEqual(transport.frames.length, 0, 'nothing leaves synchronously');
    await settle();
    assert.strictEqual(transport.frames.length, 1, 'three calls, one frame');
    assert.deepStrictEqual(
      transport.frames[0].map((packet) => packet.id),
      ['n1', 'n2', 'n3'],
    );
  });

  await t.test('a batch of one is sent bare, not wrapped in an array', async () => {
    const { client, transport } = await createFake({ batch: true });
    t.after(() => client.close());
    call(client, 1);
    await settle();
    assert.strictEqual(Array.isArray(transport.frames[0]), false);
    assert.strictEqual(transport.frames[0].id, 'n1');
  });

  await t.test('only calls batch: control packets leave immediately', async () => {
    const { client, transport } = await createFake({ batch: true });
    t.after(() => client.close());
    call(client, 1);
    client.send({ type: 'ping' });
    client.sendEvent('chat/typing', {});
    client.send({ type: 'cancel', id: 'n1' });
    assert.deepStrictEqual(
      transport.frames.map((packet) => packet.type),
      ['ping', 'event', 'cancel'],
      'a control packet whose point is to arrive now does',
    );
    await settle();
    assert.strictEqual(transport.frames.at(-1).type, 'call');
  });

  await t.test('maxSize flushes early', async () => {
    const { client, transport } = await createFake({ batch: { maxSize: 2 } });
    t.after(() => client.close());
    call(client, 1);
    call(client, 2);
    assert.strictEqual(transport.frames.length, 1, 'the cap flushed without waiting for the tick');
    assert.strictEqual(transport.frames[0].length, 2);
    call(client, 3);
    await settle();
    assert.strictEqual(transport.frames.length, 2);
  });

  await t.test('maxBytes flushes early too', async () => {
    const { client, transport } = await createFake({ batch: { maxSize: 100, maxBytes: 120 } });
    t.after(() => client.close());
    for (let i = 0; i < 6; i++) call(client, i);
    assert.ok(transport.frames.length >= 1, 'the byte cap flushed before the tick ended');
  });

  await t.test('a timed flush waits the interval out', async () => {
    const { client, transport } = await createFake({ batch: { flush: 40 } });
    t.after(() => client.close());
    call(client, 1);
    await settle();
    assert.strictEqual(transport.frames.length, 0, 'a microtask is not enough');
    await timers.setTimeout(70);
    assert.strictEqual(transport.frames.length, 1);
  });

  await t.test('flush() sends what is queued right away', async () => {
    const { client, transport } = await createFake({ batch: { flush: 5000 } });
    t.after(() => client.close());
    call(client, 1);
    client.flush();
    assert.strictEqual(transport.frames.length, 1);
  });

  await t.test('close() flushes rather than stranding a queued call', async () => {
    const { client, transport } = await createFake({ batch: { flush: 5000 } });
    call(client, 1);
    client.close();
    assert.strictEqual(transport.frames.length, 1);
  });

  await t.test('a batched answer array is demultiplexed back to each caller', async () => {
    const { client, transport } = await createFake({ batch: true });
    t.after(() => client.close());
    const introspection = { math: { double: { access: 'public' } } };
    const loading = client.load('math');
    await settle();
    transport.answer((packet) => ({ type: 'callback', id: packet.id, result: introspection }));
    await loading;

    const pending = [client.api.math.double({ n: 1 }), client.api.math.double({ n: 2 })];
    await settle();
    assert.strictEqual(transport.frames.length, 1);
    const batch = transport.frames.splice(0, 1)[0];
    // One frame back carrying both answers, out of order on purpose.
    await transport.deliver([
      { type: 'callback', id: batch[1].id, result: 4 },
      { type: 'callback', id: batch[0].id, result: 2 },
    ]);
    assert.deepStrictEqual(await Promise.all(pending), [2, 4]);
  });
});

// ---------------------------------------------------------------------------
// The server side, over a real listener

const router = defineRouter({
  math: {
    double: procedure({ access: 'public', handler: async (_context, { n }) => n * 2 }),
    fail: procedure({
      access: 'public',
      handler: async () => {
        const error = new Error('nope');
        error.code = 418;
        throw error;
      },
    }),
    // Answers only when the connection outlives it, which it will not.
    hang: procedure({ access: 'public', handler: () => new Promise(() => {}) }),
  },
});

const createServer = async (options = {}) => {
  const server = new Server({
    router,
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    console: quiet,
    timeouts: { bind: 50 },
    ...options,
  });
  await server.listen();
  return { server, port: server.address().port };
};

test('batching: over HTTP the answers come back as one array', async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());
  const base = `http://127.0.0.1:${port}/api`;

  const post = async (body) => {
    const res = await fetch(base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return { status: res.status, body: await res.json() };
  };

  await t.test('one response, one entry per request, in request order', async () => {
    const { status, body } = await post([
      { type: 'call', id: 'a', method: 'math/double', args: { n: 1 } },
      { type: 'call', id: 'b', method: 'math/fail', args: {} },
      { type: 'call', id: 'c', method: 'math/double', args: { n: 3 } },
    ]);
    assert.strictEqual(status, 200, 'the frame succeeded even though one call inside it failed');
    assert.strictEqual(Array.isArray(body), true);
    assert.deepStrictEqual(
      body.map((packet) => packet.id),
      ['a', 'b', 'c'],
    );
    assert.strictEqual(body[0].result, 2);
    assert.strictEqual(body[1].error.code, 418);
    assert.strictEqual(body[2].result, 6);
  });

  await t.test('a malformed element is reported without losing the others', async () => {
    const { body } = await post([{ type: 'call', id: 'ok', method: 'math/double', args: { n: 2 } }, { type: 'junk' }]);
    assert.strictEqual(body.length, 2);
    assert.strictEqual(body[0].result, 4);
    assert.strictEqual(body[1].error.code, 500);
  });

  await t.test('a malformed element with an id keeps its slot', async () => {
    // Positional zipping is the guarantee the protocol makes about a batch,
    // so a structure error has to answer ON the id it was sent with —
    // otherwise it lands at the end and shifts every answer after it.
    const { body } = await post([
      { type: 'call', id: 'a', method: 'math/double', args: { n: 1 } },
      { type: 'junk', id: 'b' },
      { type: 'call', id: 'c', method: 'math/double', args: { n: 3 } },
    ]);
    assert.deepStrictEqual(
      body.map((packet) => packet.id),
      ['a', 'b', 'c'],
    );
    assert.strictEqual(body[0].result, 2);
    assert.strictEqual(body[1].error.code, 500);
    assert.strictEqual(body[2].result, 6);
  });

  await t.test('an empty batch is refused', async () => {
    const { status, body } = await post([]);
    assert.strictEqual(status, 400);
    assert.match(body.error.message, /Batch size/);
  });

  await t.test('an oversized batch is refused as a whole', async () => {
    const small = await createServer({ maxBatch: 3 });
    t.after(() => small.server.close());
    const packets = Array.from({ length: 5 }, (_value, i) => ({
      type: 'call',
      id: `x${i}`,
      method: 'math/double',
      args: { n: i },
    }));
    const res = await fetch(`http://127.0.0.1:${small.port}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(packets),
    });
    const body = await res.json();
    assert.strictEqual(res.status, 400);
    assert.match(body.error.message, /Batch size/);
  });

  await t.test('every answered batch releases its client', async () => {
    await timers.setTimeout(20);
    assert.strictEqual(server.rpc.clients.size, 0);
  });
});

// Timed out on purpose: the failure this guards against is a request that
// never answers, and without a bound that wedges the whole run.
test('batching: a batch closed before it finished still answers', { timeout: 10000 }, async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());

  const pending = fetch(`http://127.0.0.1:${port}/api`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify([
      { type: 'call', id: 'a', method: 'math/double', args: { n: 1 } },
      { type: 'call', id: 'b', method: 'math/hang', args: {} },
      { type: 'call', id: 'c', method: 'math/hang', args: {} },
    ]),
  });

  // Let 'a' answer and be collected, then evict the client out from under
  // the two that never will — a shutdown, or any other close.
  await timers.setTimeout(50);
  assert.strictEqual(server.rpc.clients.size, 1, 'the batch is still in flight');
  for (const client of server.rpc.clients) client.close();

  const res = await pending;
  assert.strictEqual(res.status, 503);
  const body = await res.json();
  assert.deepStrictEqual(
    body.map((packet) => packet.id),
    ['a', 'b', 'c'],
    'every id is answered, in request order — one id-less packet is unroutable',
  );
  assert.strictEqual(body[0].result, 2, 'an answer already collected is not thrown away');
  assert.strictEqual(body[1].error.code, 503);
  assert.strictEqual(body[2].error.code, 503);
  await timers.setTimeout(20);
  assert.strictEqual(server.rpc.clients.size, 0, 'responding is what evicts the client');
});

test('batching: a real client over a real websocket', async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());
  const client = await WrpcClient.connect(`ws://127.0.0.1:${port}/api`, { heartbeat: false, batch: true });
  t.after(() => void client.close());
  await client.load('math');

  const results = await Promise.all([
    client.api.math.double({ n: 1 }),
    client.api.math.double({ n: 2 }),
    client.api.math.double({ n: 3 }),
  ]);
  assert.deepStrictEqual(results, [2, 4, 6]);

  // Errors inside a batch stay attached to their own call.
  const mixed = await Promise.allSettled([client.api.math.double({ n: 4 }), client.api.math.fail()]);
  assert.strictEqual(mixed[0].value, 8);
  assert.strictEqual(mixed[1].reason.code, 418);
});
