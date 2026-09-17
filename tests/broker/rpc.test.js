'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure, WrpcClient, tracked, createEventLog, createEventStream } = require('../../index.js');
const { MemoryBroker, attachBrokerRpc, ClientBrokerTransport } = require('../../broker.js');
const { bearerTransport } = require('../../auth.js');
const { runTransportContract } = require('../client/transportContract.js');
const { quiet, waitFor } = require('./support.js');

const onceEvent = (emitter, name) => new Promise((resolve) => emitter.once(name, resolve));

// One shared feed across every instance, as a broker-backed app would have.
const createApp = () => {
  const log = createEventLog({ size: 100, epoch: 'shared' });
  const streams = new Set();
  const push = (value) => {
    const item = tracked(log.push(value), value);
    for (const stream of streams) stream.push(item);
  };
  const router = defineRouter({
    calc: {
      add: procedure({ access: 'public', handler: async (_ctx, { a, b }) => a + b }),
      who: procedure({
        access: 'public',
        handler: async (ctx) => ({
          instance: ctx.server.instanceId,
          session: ctx.session?.state ? { ...ctx.session.state } : null,
          kind: ctx.client.transportKind,
          tenant: ctx.meta.headers['x-tenant'] ?? null,
        }),
      }),
      notify: procedure({
        access: 'public',
        handler: async (ctx, { text }) => void ctx.client.sendEvent('calc/note', { text }),
      }),
      slow: procedure({
        access: 'public',
        handler: async (ctx) => {
          await timers.setTimeout(200, undefined, { signal: ctx.signal }).catch(() => {});
          return 'late';
        },
      }),
      feed: procedure.subscription({
        access: 'public',
        handler: async function* (_ctx, _args, { lastEventId, signal }) {
          for (const item of log.since(lastEventId) ?? []) yield item;
          const stream = createEventStream({ signal });
          streams.add(stream);
          try {
            yield* stream;
          } finally {
            streams.delete(stream);
          }
        },
      }),
      readUpload: procedure({
        access: 'public',
        handler: async (ctx, { id }) => {
          let total = 0;
          for await (const chunk of ctx.client.getStream(id)) total += chunk.length;
          return total;
        },
      }),
      download: procedure({
        access: 'public',
        handler: async (ctx, { size }) => {
          const stream = ctx.client.createStream('blob', size);
          queueMicrotask(() => {
            stream.write(new Uint8Array(size).fill(3));
            stream.end();
          });
          return { id: stream.id };
        },
      }),
      mine: procedure({ handler: async (ctx) => ({ ...ctx.session.state }) }),
    },
  });
  return { router, push, streams };
};

const instance = async (t, broker, app, options = {}) => {
  const rpc = new RpcServer({ router: app.router, logger: quiet, sse: false, ...options.rpc });
  const handle = await attachBrokerRpc(rpc, broker, { service: 'calc', logger: quiet, ...options.attach });
  t.after(async () => {
    await handle.stop();
    await rpc.close();
  });
  return { rpc, handle };
};

const connect = async (t, broker, options = {}) => {
  const client = await WrpcClient.connect('broker://calc', {
    transport: 'broker',
    broker,
    heartbeat: false,
    reconnect: false,
    callTimeout: 2000,
    connectTimeout: 1000,
    ...options,
  });
  t.after(() => client.close());
  await client.load('calc');
  return client;
};

test('broker transport: satisfies the shared client-transport contract', async (t) => {
  await runTransportContract(t, 'broker', ClientBrokerTransport);
  assert.strictEqual(WrpcClient.transport.broker, ClientBrokerTransport);
});

test('stateless: any instance answers any call; headers restore a session; no affinity', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const app = createApp();
  const sessions = { transport: bearerTransport() };
  const a = await instance(t, broker, app, { rpc: { sessions } });
  const b = await instance(t, broker, app, { rpc: { sessions } });
  assert.strictEqual(a.handle.address, 'wrpc.calc');
  const token = a.rpc.sessions.create(undefined, { user: 'ada' }).token;
  b.rpc.sessions.store.set(token, { user: 'ada' });
  await timers.setTimeout(5);
  const client = await connect(t, broker, {
    headers: { authorization: `Bearer ${token}`, 'x-tenant': 't1', 'wrpc-kind': 'forged' },
  });
  assert.strictEqual(await client.api.calc.add({ a: 2, b: 3 }), 5);
  const answers = [];
  for (let i = 0; i < 8; i++) answers.push(await client.api.calc.who({}));
  assert.deepStrictEqual(
    new Set(answers.map((answer) => answer.instance)),
    new Set([a.rpc.instanceId, b.rpc.instanceId]),
  );
  assert.deepStrictEqual(answers[0].session, { user: 'ada' });
  assert.strictEqual(answers[0].tenant, 't1');
  assert.strictEqual(answers[0].kind, 'http');
  assert.deepStrictEqual(await client.api.calc.mine({}), { user: 'ada' });
  // Request/response: a subscription is refused like on plain HTTP.
  const errors = [];
  client.api.calc.feed.subscribe({}, { onError: (error) => errors.push(error) });
  await waitFor(() => errors.length === 1);
  assert.strictEqual(errors[0].code, 400);
  assert.strictEqual(a.handle.sessions + b.handle.sessions, 0);
});

test('stateless: nobody serving the address fails the call with 503 at once', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const app = createApp();
  const { handle } = await instance(t, broker, app);
  const client = await connect(t, broker);
  await handle.stop();
  const started = Date.now();
  await assert.rejects(client.api.calc.add({ a: 1, b: 1 }), (error) => error.code === 503);
  assert.ok(Date.now() - started < 500);
});

test('session: calls, server events, subscriptions and streams over one instance', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const app = createApp();
  const a = await instance(t, broker, app);
  const client = await connect(t, broker, { mode: 'session', headers: { 'x-tenant': 't9' } });
  assert.strictEqual(a.handle.sessions, 1);
  assert.strictEqual(await client.api.calc.add({ a: 20, b: 22 }), 42);
  const who = await client.api.calc.who({});
  assert.deepStrictEqual([who.instance, who.kind, who.tenant], [a.rpc.instanceId, 'broker', 't9']);
  // A server-initiated event.
  const notes = [];
  client.api.calc.on('note', (data) => notes.push(data));
  await client.api.calc.notify({ text: 'hi' });
  await waitFor(() => notes.length === 1);
  assert.deepStrictEqual(notes, [{ text: 'hi' }]);
  // A subscription.
  const values = [];
  client.api.calc.feed.subscribe({}, { onData: (value) => values.push(value) });
  await waitFor(() => app.streams.size === 1);
  app.push('one');
  app.push('two');
  await waitFor(() => values.length === 2);
  // Binary streams both ways.
  const size = 256 * 1024;
  const upload = client.createStream('blob', size);
  const total = client.api.calc.readUpload({ id: upload.id });
  for (let offset = 0; offset < size; offset += 32 * 1024) upload.write(new Uint8Array(32 * 1024).fill(1));
  upload.end();
  assert.strictEqual(await total, size);
  const { id } = await client.api.calc.download({ size: 64 * 1024 });
  let received = 0;
  for await (const chunk of client.getStream(id)) received += chunk.length;
  assert.strictEqual(received, 64 * 1024);
  // Cancellation reaches the server.
  const controller = new AbortController();
  const slow =
    client.api.calc.slow.withOptions?.({ signal: controller.signal })?.({}) ??
    client.call('calc/slow', {}, { signal: controller.signal });
  controller.abort();
  await assert.rejects(slow);
});

test('session: losing the instance reconnects to another and the subscription resumes', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const app = createApp();
  const first = await instance(t, broker, app);
  const client = await connect(t, broker, {
    mode: 'session',
    reconnect: { minDelay: 5, maxDelay: 20, jitter: false },
  });
  const values = [];
  const handle = client.api.calc.feed.subscribe({}, { onData: (value) => values.push(value) });
  await waitFor(() => app.streams.size === 1);
  app.push('before');
  await waitFor(() => values.length === 1);
  const second = await instance(t, broker, app);
  const reconnected = onceEvent(client, 'reconnect');
  // The first instance goes away: its sessions are told goodbye.
  await first.handle.stop();
  await first.rpc.close();
  app.push('while away');
  await reconnected;
  await waitFor(() => second.handle.sessions === 1);
  app.push('after');
  await waitFor(() => values.length === 3);
  assert.deepStrictEqual(values, ['before', 'while away', 'after']);
  assert.match(handle.lastEventId, /^shared\./);
});

test('session: a frame gap on either side ends the session', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const app = createApp();
  let dropServerFrame = false;
  let dropClientFrame = false;
  // A lossy broker: drops the next data frame in one direction.
  const lossy = {
    name: 'lossy',
    close() {},
    direct: {
      inbox: () => broker.direct.inbox(),
      listen: (...args) => broker.direct.listen(...args),
      send: (address, body, options) => {
        const kind = options?.headers?.['wrpc-kind'];
        const fromServer = options?.replyTo === undefined;
        if (kind === 'packet' && fromServer && dropServerFrame) {
          dropServerFrame = false;
          return Promise.resolve();
        }
        if (kind === 'packet' && !fromServer && dropClientFrame) {
          dropClientFrame = false;
          return Promise.resolve();
        }
        return broker.direct.send(address, body, options);
      },
    },
  };
  const a = await instance(t, lossy, app);
  const client = await connect(t, lossy, { mode: 'session' });
  // Server -> client: the dropped answer is noticed at the next frame.
  dropServerFrame = true;
  const lost = client.api.calc.add({ a: 1, b: 1 }).catch((error) => error);
  const closed = onceEvent(client, 'close');
  await client.call('calc/add', { a: 2, b: 2 }).catch(() => {});
  await closed;
  assert.strictEqual((await lost).code, 503);
  await waitFor(() => a.handle.sessions === 0);

  // Client -> server: the server notices, ends the session and says bye.
  const again = await connect(t, lossy, { mode: 'session' });
  await waitFor(() => a.handle.sessions === 1);
  dropClientFrame = true;
  const byeClose = onceEvent(again, 'close');
  void again.call('calc/add', { a: 1, b: 1 }).catch(() => {});
  void again.call('calc/add', { a: 1, b: 1 }).catch(() => {});
  await byeClose;
  await waitFor(() => a.handle.sessions === 0);
});

test('session: a silent client is ended after idleTimeout; a frame for a lost session gets a bye', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const app = createApp();
  const a = await instance(t, broker, app, { attach: { idleTimeout: 150 } });
  const client = await connect(t, broker, { mode: 'session' });
  assert.strictEqual(a.handle.sessions, 1);
  const closed = onceEvent(client, 'close');
  await closed;
  assert.strictEqual(a.handle.sessions, 0);

  // A frame for a session the instance does not hold is answered with a bye.
  const inbox = broker.direct.inbox();
  const replies = [];
  await broker.direct.listen(inbox, (message) => replies.push(message));
  const frame = { headers: { 'wrpc-kind': 'packet', 'wrpc-seq': '1' }, correlationId: 'ghost', replyTo: inbox };
  await broker.direct.send(a.handle.inbox, '{"type":"ping"}', frame);
  // ...but a bye for an unknown session, or one without replyTo, is not.
  await broker.direct.send(a.handle.inbox, '', {
    headers: { 'wrpc-kind': 'bye' },
    correlationId: 'ghost',
    replyTo: inbox,
  });
  await broker.direct.send(a.handle.inbox, '{}', { headers: { 'wrpc-kind': 'packet' }, correlationId: 'ghost' });
  await broker.direct.send(a.handle.inbox, '{}', { headers: { 'wrpc-kind': 'packet' } });
  // Malformed service messages are ignored: no replyTo, no correlation, an unknown kind.
  await broker.direct.send(a.handle.address, '{}', { headers: { 'wrpc-kind': 'request' } });
  await broker.direct.send(a.handle.address, '', { headers: { 'wrpc-kind': 'hello' }, replyTo: inbox });
  await broker.direct.send(a.handle.address, '', { headers: { 'wrpc-kind': 'mystery' }, replyTo: inbox });
  await waitFor(() => replies.length === 1);
  await timers.setTimeout(20);
  assert.strictEqual(replies.length, 1);
  assert.deepStrictEqual(
    [replies[0].headers['wrpc-kind'], replies[0].headers['wrpc-reason'], replies[0].correlationId],
    ['bye', 'unknown session', 'ghost'],
  );
});

test('session: a repeated hello replaces the session; the server stopping says goodbye', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const app = createApp();
  const a = await instance(t, broker, app);
  const inbox = broker.direct.inbox();
  const replies = [];
  await broker.direct.listen(inbox, (message) => replies.push(message));
  const hello = { headers: { 'wrpc-kind': 'hello' }, correlationId: 'same', replyTo: inbox };
  await broker.direct.send(a.handle.address, '', hello);
  await broker.direct.send(a.handle.address, '', hello);
  await waitFor(() => replies.length === 2);
  assert.strictEqual(a.handle.sessions, 1);
  const client = await connect(t, broker, { mode: 'session' });
  const closed = onceEvent(client, 'close');
  await a.handle.stop();
  await closed;
  assert.strictEqual(a.handle.sessions, 0);
});

test('session: draining stops taking new sessions; close says goodbye to the ones it holds', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const app = createApp();
  const a = await instance(t, broker, app);
  const held = await connect(t, broker, { mode: 'session' });
  const draining = a.rpc.drain(50);
  const b = await instance(t, broker, app);
  const fresh = await connect(t, broker, { mode: 'session' });
  assert.strictEqual(b.handle.sessions, 1);
  assert.strictEqual(a.handle.sessions, 1);
  // The held session still answers (calls get 503 while draining, but it is alive).
  await assert.rejects(held.api.calc.add({ a: 1, b: 1 }), (error) => error.code === 503);
  await draining;
  const closed = onceEvent(held, 'close');
  await a.rpc.close();
  await closed;
  assert.strictEqual(a.handle.healthy, false);
  assert.strictEqual(await fresh.api.calc.add({ a: 1, b: 2 }), 3);
});

test('session: refused handshakes', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const app = createApp();
  // Nobody listening at all.
  await assert.rejects(
    WrpcClient.connect('broker://calc', {
      transport: 'broker',
      broker,
      mode: 'session',
      reconnect: false,
      heartbeat: false,
    }),
    (error) => error.code === 503,
  );
  // An instance that serves stateless calls only.
  await instance(t, broker, app, { attach: { sessions: false } });
  await assert.rejects(
    WrpcClient.connect('broker://calc', {
      transport: 'broker',
      broker,
      mode: 'session',
      reconnect: false,
      heartbeat: false,
      connectTimeout: 500,
    }),
    /Session ended: sessions disabled/,
  );
  const stateless = await connect(t, broker);
  assert.strictEqual(await stateless.api.calc.add({ a: 1, b: 1 }), 2);
});

test('broker rpc: options are validated', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const rpc = new RpcServer({ router: createApp().router, logger: quiet, sse: false });
  t.after(() => rpc.close());
  await assert.rejects(attachBrokerRpc(rpc, broker, {}), /service must be a non-empty string/);
  await assert.rejects(attachBrokerRpc(rpc, broker, { address: '' }), /address must be a non-empty string/);
  await assert.rejects(attachBrokerRpc(rpc, broker, { service: 's', idleTimeout: 0 }), /idleTimeout/);
  await assert.rejects(attachBrokerRpc(rpc, broker, { service: 's', highWaterMark: 0 }), /highWaterMark/);
  await assert.rejects(
    attachBrokerRpc(rpc, { name: 'k', close() {}, queue: broker.queue }, { service: 's' }),
    /no 'direct'/,
  );
  await assert.rejects(attachBrokerRpc({}, broker, { service: 's' }), /Server or an RpcServer/);
  const custom = await attachBrokerRpc(rpc, broker.direct, { address: 'custom.address' });
  assert.strictEqual(custom.address, 'custom.address');
  await custom.stop();
  await custom.stop();
  await assert.rejects(
    WrpcClient.connect('broker://calc', { transport: 'broker', broker, mode: 'duplex', reconnect: false }),
    /mode must be 'stateless' or 'session'/,
  );
  await assert.rejects(
    WrpcClient.connect('not-a-broker-url', { transport: 'broker', broker, reconnect: false }),
    /service must be a non-empty string/,
  );
});

test('broker rpc: backpressure on a slow broker, both halves', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  let release = null;
  let gate = Promise.resolve();
  const slow = {
    name: 'slow',
    close() {},
    direct: {
      inbox: () => broker.direct.inbox(),
      listen: (...args) => broker.direct.listen(...args),
      send: async (...args) => {
        await gate;
        return broker.direct.send(...args);
      },
    },
  };
  const app = createApp();
  const a = await instance(t, slow, app, { attach: { highWaterMark: 2 } });
  const client = await connect(t, slow, { mode: 'session' });
  // Server half, driven directly against a gated broker.
  gate = new Promise((resolve) => (release = resolve));
  const { BrokerSessionTransport } = require('../../src/broker/rpc/server.js');
  const session = new BrokerSessionTransport({
    direct: slow.direct,
    peer: '_nowhere',
    session: 's',
    highWaterMark: 2,
    onFailure: () => {},
  });
  assert.strictEqual(session.write('a'), true);
  assert.strictEqual(session.write('b'), false);
  const drained = onceEvent(session, 'drain');
  release();
  await drained;
  session.close();
  assert.strictEqual(session.write('c'), false);
  assert.strictEqual(session.closed, true);
  assert.ok(a.handle.healthy);

  // Client half: the same gate, the transport's write reports the pressure.
  const clientTransport = new ClientBrokerTransport('broker://calc');
  await clientTransport.open({ broker: slow, mode: 'session' });
  t.after(() => clientTransport.close());
  gate = new Promise((resolve) => (release = resolve));
  const frames = [clientTransport.write('{"type":"ping"}')];
  for (let i = 0; i < 1024; i++) frames.push(clientTransport.write('{"type":"ping"}'));
  assert.strictEqual(frames[0], true);
  assert.strictEqual(frames.at(-1), false);
  const clientDrained = onceEvent(clientTransport, 'drain');
  release();
  await clientDrained;
  assert.ok(client.active);
});
