'use strict';

const timers = require('node:timers/promises');
const { test } = require('node:test');
const assert = require('node:assert');

const { Server, WrpcClient, Emitter, defineRouter, procedure } = require('../../index.js');
const { jsonParse } = require('../../src/utils.js');

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
    logger: false,
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

// Above ServerHttpTransport's INDEX_THRESHOLD, #ordered switches from the
// linear scan to an id-indexed pass. Both must produce identical output, so
// the large-batch cases below exist to reach the second path at all — the
// suites above never exceed a handful of answers.
test('batching: a large batch is reordered identically to a small one', async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());

  const post = async (body) => {
    const res = await fetch(`http://127.0.0.1:${port}/api`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return { status: res.status, body: await res.json() };
  };

  await t.test('40 answers come back in request order, not completion order', async () => {
    const size = 40;
    const packets = [];
    for (let i = 0; i < size; i++) {
      packets.push({ type: 'call', id: `id-${i}`, method: 'math/double', args: { n: i } });
    }
    const { status, body } = await post(packets);
    assert.strictEqual(status, 200);
    assert.strictEqual(body.length, size);
    for (let i = 0; i < size; i++) {
      assert.strictEqual(body[i].id, `id-${i}`, `slot ${i} holds its own id`);
      assert.strictEqual(body[i].result, i * 2);
    }
  });

  await t.test('a repeated id consumes one answer per slot', async () => {
    const size = 14;
    const packets = [];
    for (let i = 0; i < size; i++) {
      // Every slot asks under the SAME id. The dispatcher answers the first
      // and refuses the rest as already in flight, so all 14 answers share one
      // id — which is precisely the case the id index must not collapse: it
      // has to hand out one collected answer per slot, not the first one 14
      // times.
      packets.push({ type: 'call', id: 'dup', method: 'math/double', args: { n: i } });
    }
    const { status, body } = await post(packets);
    assert.strictEqual(status, 200);
    assert.strictEqual(body.length, size, 'every slot is answered');
    for (const packet of body) assert.strictEqual(packet.id, 'dup');
    const answered = body.filter((packet) => packet.result !== undefined);
    const refused = body.filter((packet) => packet.error?.code === 400);
    assert.strictEqual(answered.length, 1, 'exactly one call ran');
    assert.strictEqual(refused.length, size - 1, 'the rest were refused as in-flight');
    assert.match(refused[0].error.message, /already in flight/);
  });

  await t.test('a failing call keeps its slot among many successes', async () => {
    const packets = [];
    for (let i = 0; i < 20; i++) {
      const failing = i === 7;
      packets.push({
        type: 'call',
        id: `id-${i}`,
        method: failing ? 'math/fail' : 'math/double',
        args: { n: i },
      });
    }
    const { body } = await post(packets);
    assert.strictEqual(body.length, 20);
    for (let i = 0; i < 20; i++) assert.strictEqual(body[i].id, `id-${i}`);
    assert.strictEqual(body[7].error.code, 418);
    assert.strictEqual(body[8].result, 16);
  });
});

// The close() path pre-fills every unanswered slot and then runs #ordered, so
// it reaches the indexed branch too once the batch is large enough.
test('batching: a large batch closed mid-flight answers every slot', { timeout: 10000 }, async (t) => {
  const { server, port } = await createServer();
  t.after(() => server.close());

  const packets = [{ type: 'call', id: 'first', method: 'math/double', args: { n: 21 } }];
  for (let i = 0; i < 19; i++) {
    packets.push({ type: 'call', id: `hang-${i}`, method: 'math/hang', args: {} });
  }

  const pending = fetch(`http://127.0.0.1:${port}/api`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(packets),
  });

  await timers.setTimeout(50);
  for (const client of server.rpc.clients) client.close();

  const res = await pending;
  assert.strictEqual(res.status, 503);
  const body = await res.json();
  assert.deepStrictEqual(
    body.map((packet) => packet.id),
    packets.map((packet) => packet.id),
    'every id is answered, in request order',
  );
  assert.strictEqual(body[0].result, 42, 'the one collected answer is not thrown away');
  assert.strictEqual(body[1].error.code, 503);
});

test('batching: per-call meta aggregates into the request headers, last write wins', async (t) => {
  const seen = [];
  const router = defineRouter({
    unit: {
      run: procedure({
        access: 'public',
        handler: async (context) => {
          seen.push({ call: { ...context.callMeta }, connection: { ...context.meta.data } });
          return true;
        },
      }),
    },
  });
  const server = new Server({ host: '127.0.0.1', port: 0, protocol: 'http', logger: false, router });
  await server.listen();
  t.after(() => server.close());
  const { port } = server.address();

  const client = await WrpcClient.connect(`http://127.0.0.1:${port}/api`, {
    transport: ['http'],
    heartbeat: false,
    reconnect: false,
    logger: false,
    batch: true,
    meta: { tenant: 'acme' },
  });
  t.after(() => void client.close());
  await client.load('unit');
  seen.length = 0;

  // Three calls in one tick -> one POST, one header block.
  const [a, b, c] = await Promise.all([
    client.api.unit.run.withMeta({ traceId: 't1', shared: 'same' })(),
    client.api.unit.run.withMeta({ traceId: 't2', shared: 'same' })(),
    client.api.unit.run.withMeta({ traceId: 't3' })(),
  ]);
  assert.deepStrictEqual([a, b, c], [true, true, true]);
  assert.strictEqual(seen.length, 3);

  // The packet field is untouched by batching: every call keeps its EXACT
  // meta, which is what context.callMeta reports. This is the source of truth.
  assert.deepStrictEqual(
    seen.map((entry) => entry.call['trace-id']),
    ['t1', 't2', 't3'],
  );

  // The headers carry the aggregate — a lossy summary for infrastructure.
  // A key every call agreed on survives; a key they disagreed on shows the
  // LAST value; the connection's own bag is still underneath.
  for (const entry of seen) {
    assert.strictEqual(entry.connection.shared, 'same');
    assert.strictEqual(entry.connection['trace-id'], 't3', 'the aggregate must be last-write-wins');
    assert.strictEqual(entry.connection.tenant, 'acme', 'the connection bag must survive the aggregate');
  }
});

test('batching: a batch carrying no meta leaves the connection header alone', async (t) => {
  const seen = [];
  const router = defineRouter({
    unit: {
      run: procedure({ access: 'public', handler: async (ctx) => void seen.push({ ...ctx.meta.data }) ?? true }),
    },
  });
  const server = new Server({ host: '127.0.0.1', port: 0, protocol: 'http', logger: false, router });
  await server.listen();
  t.after(() => server.close());
  const { port } = server.address();
  const client = await WrpcClient.connect(`http://127.0.0.1:${port}/api`, {
    transport: ['http'],
    heartbeat: false,
    reconnect: false,
    logger: false,
    batch: true,
    meta: { tenant: 'acme' },
  });
  t.after(() => void client.close());
  await client.load('unit');
  seen.length = 0;
  await Promise.all([client.api.unit.run(), client.api.unit.run()]);
  for (const entry of seen) assert.deepStrictEqual(entry, { tenant: 'acme' });
});

test('batching: an oversize aggregate is refused loudly, keeping the connection label', async (t) => {
  const seen = [];
  const warnings = [];
  const router = defineRouter({
    unit: {
      run: procedure({ access: 'public', handler: async (ctx) => void seen.push({ ...ctx.meta.data }) ?? true }),
    },
  });
  const server = new Server({ host: '127.0.0.1', port: 0, protocol: 'http', logger: false, router });
  await server.listen();
  t.after(() => server.close());
  const { port } = server.address();
  const client = await WrpcClient.connect(`http://127.0.0.1:${port}/api`, {
    transport: ['http'],
    heartbeat: false,
    reconnect: false,
    batch: true,
    meta: { tenant: 'acme' },
    logger: { warn: (e) => warnings.push(e), info: () => {}, error: () => {}, debug: () => {}, log: () => {} },
  });
  t.after(() => void client.close());
  await client.load('unit');
  seen.length = 0;

  await client.api.unit.run.withMeta({ pad: 'x'.repeat(4096) })();

  // The server drops the WHOLE bag past its cap, so refusing client-side is
  // the difference between a visible warning and a label that silently
  // stopped arriving — and the connection's own bag survives either way.
  assert.deepStrictEqual(seen[0], { tenant: 'acme' });
  assert.ok(
    warnings.some((entry) => entry.event === 'meta.oversize'),
    'the refusal must be visible on the side that can fix it',
  );
});
