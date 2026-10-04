'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure, WrpcClient } = require('../../index.js');
const { MemoryBroker, attachBrokerRpc } = require('../../broker.js');
const { bearerTransport } = require('../../auth.js');
const { generateKey } = require('../../encryption.js');
const { createBrokerSealing, HEADER_SEALED } = require('../../src/broker/sealing.js');
const { KIND, HEADER_KIND } = require('../../src/broker/rpc/frames.js');
const { quiet, waitFor } = require('./support.js');

const SECRET = 'not for whoever can read the topic: 4111 1111 1111 1111';

const router = defineRouter({
  calc: {
    echo: procedure({ access: 'public', handler: async (_ctx, args) => args }),
    who: procedure({
      access: 'public',
      handler: async (ctx) => ({
        user: ctx.session?.state?.user ?? null,
        tenant: ctx.meta.headers['x-tenant'] ?? null,
        kind: ctx.client.transportKind,
      }),
    }),
    big: procedure({
      access: 'public',
      handler: async (_ctx, { rows }) => Array.from({ length: rows }, (_, i) => ({ i })),
    }),
    upload: procedure({
      access: 'public',
      handler: async (ctx, { id }) => {
        let total = 0;
        for await (const chunk of ctx.client.getStream(id)) total += chunk.length;
        return total;
      },
    }),
    nudge: procedure({
      access: 'public',
      handler: async (ctx) => void ctx.client.sendEvent('calc/nudged', { note: SECRET }),
    }),
  },
});

// A broker whose `direct.send` is recorded — everything it was handed, as
// whoever operates it would read it. The capability object is frozen, so
// the spy is a broker of its own around the same one.
const spied = (broker) => {
  const carried = [];
  const direct = {
    name: broker.direct.name,
    inbox: () => broker.direct.inbox(),
    listen: (...args) => broker.direct.listen(...args),
    send: (address, body, message) => {
      const text = body instanceof Uint8Array ? Buffer.from(body).toString('latin1') : String(body);
      carried.push({
        address,
        text,
        headers: { ...message?.headers },
        correlationId: message?.correlationId,
        replyTo: message?.replyTo,
      });
      return broker.direct.send(address, body, message);
    },
  };
  return { carried, broker: { name: broker.name, direct, close: () => broker.close() } };
};

const logs = () => {
  const warnings = [];
  const logger = {
    log() {},
    info() {},
    debug() {},
    error() {},
    warn: (entry) => warnings.push(entry),
    child: () => logger,
  };
  return { logger, warnings };
};

const boot = async (t, { rpc = {}, attach = {}, broker = new MemoryBroker({ logger: quiet }) } = {}) => {
  t.after(() => broker.close());
  const sessions = { transport: bearerTransport() };
  const server = new RpcServer({ router, logger: quiet, sse: false, sessions, ...rpc });
  const handle = await attachBrokerRpc(server, broker, { service: 'calc', logger: quiet, ...attach });
  t.after(async () => {
    await handle.stop();
    await server.close();
  });
  const token = server.sessions.create(undefined, { user: 'ada' }).token;
  const connect = async (options = {}) => {
    const client = await WrpcClient.connect('broker://calc', {
      transport: 'broker',
      broker,
      heartbeat: false,
      reconnect: false,
      callTimeout: 1000,
      connectTimeout: 1000,
      logger: false,
      ...options,
    });
    t.after(() => client.close());
    return client;
  };
  return { broker, server, handle, token, connect };
};

for (const mode of ['stateless', 'session']) {
  test('broker sealing: a key provider that throws while opening answers { refused: "keys", error }', () => {
    const master = generateKey();
    let down = false;
    const flaky = {
      current: () => 'a',
      get: (kid) => {
        if (down) throw new Error('vault unreachable');
        return kid === 'a' ? master : null;
      },
    };
    const sealing = createBrokerSealing({ keys: flaky }, 'x', { layer: 'broker-log', replay: false, text: true });
    const sealed = sealing.seal('orders', { n: '1' }, 'body');
    down = true;
    const refused = sealing.open('orders', sealed);
    assert.strictEqual(refused.refused, 'keys');
    assert.strictEqual(refused.error.message, 'vault unreachable');
    down = false;
    assert.strictEqual(sealing.open('orders', sealed).sealed, true);
  });

  test(`broker encryption (${mode}): the broker carries neither the packets nor the bearer token`, async (t) => {
    const keys = generateKey();
    const { carried, broker } = spied(new MemoryBroker({ logger: quiet }));
    const { token, connect } = await boot(t, { broker, attach: { encryption: { keys } } });
    const client = await connect({
      mode,
      encryption: { keys },
      headers: { authorization: `Bearer ${token}`, 'x-tenant': 't1' },
    });
    await client.load('calc');
    assert.deepStrictEqual(await client.api.calc.echo({ note: SECRET }), { note: SECRET });
    assert.deepStrictEqual(await client.api.calc.who({}), {
      user: 'ada',
      tenant: 't1',
      kind: mode === 'session' ? 'broker' : 'http',
    });
    assert.strictEqual((await client.api.calc.big({ rows: 500 })).length, 500);
    if (mode === 'session') {
      const upload = client.createStream('data', 50_000);
      const uploaded = client.api.calc.upload({ id: upload.id });
      upload.write(new Uint8Array(50_000).fill(5));
      upload.end();
      assert.strictEqual(await uploaded, 50_000);
      const nudged = new Promise((resolve) => client.api.calc.on('nudged', resolve));
      await client.api.calc.nudge({});
      assert.deepStrictEqual(await nudged, { note: SECRET });
    }
    assert.ok(carried.length > 5);
    for (const { text, headers } of carried) {
      assert.strictEqual(typeof headers[HEADER_SEALED], 'string');
      // What stays readable is what the binding routes by — and only that
      for (const name of Object.keys(headers)) assert.ok(['wrpc-sealed', 'wrpc-kind', 'wrpc-seq'].includes(name), name);
      for (const needle of ['4111', token, 'Bearer', 'x-tenant', 'echo', 'callback', 'wrpc.inbox']) {
        assert.ok(!text.includes(needle), needle);
      }
    }
  });
}

test('broker encryption: the goodbye is sealed like every other frame — a sealed client hears the close', async (t) => {
  const keys = generateKey();
  const { carried, broker } = spied(new MemoryBroker({ logger: quiet }));
  const { connect, handle } = await boot(t, { broker, attach: { encryption: { keys } } });
  const client = await connect({ mode: 'session', encryption: { keys } });
  await client.load('calc');
  assert.deepStrictEqual(await client.api.calc.echo({ n: 1 }), { n: 1 });
  // The binding stops: its goodbye used to go past the sealer, and a sealed
  // client dropped it as `unsealed` — waiting on a session that was over.
  const closed = new Promise((resolve) => client.once('close', resolve));
  await handle.stop();
  await closed;
  assert.strictEqual(client.active, false);
  const byes = carried.filter(({ headers }) => headers[HEADER_KIND] === KIND.BYE);
  assert.ok(byes.length >= 1, 'a goodbye was carried');
  for (const { headers, text } of carried) {
    assert.strictEqual(typeof headers[HEADER_SEALED], 'string', `sealed: ${headers[HEADER_KIND]}`);
    assert.ok(!text.includes('closing') && !text.includes('closed'), 'the reason rides inside');
  }
});

test('broker encryption: compression still applies — compress, then seal, the codec id inside', async (t) => {
  const keys = generateKey();
  const compression = { threshold: 0 };
  const { carried, broker } = spied(new MemoryBroker({ logger: quiet }));
  const { connect } = await boot(t, { broker, attach: { encryption: { keys }, compression } });
  const client = await connect({ mode: 'session', encryption: { keys }, compression });
  await client.load('calc');
  const text = 'w'.repeat(30_000);
  assert.strictEqual((await client.api.calc.echo({ text })).text.length, 30_000);
  assert.deepStrictEqual(client.transport?.compression ?? null, null);
  assert.ok(
    carried.every((frame) => frame.text.length < 3_000),
    'a 30 KB call crossed as a few hundred sealed bytes',
  );
  assert.ok(
    carried.every((frame) => frame.headers['wrpc-enc'] === undefined),
    'the codec id is not a readable header',
  );
});

test('broker encryption: a plaintext, foreign-key, moved or replayed frame is dropped and named, never answered', async (t) => {
  const keys = generateKey();
  const { logger, warnings } = logs();
  const { broker, handle, connect } = await boot(t, { attach: { encryption: { keys }, logger } });
  const replies = [];
  const inbox = broker.direct.inbox();
  await broker.direct.listen(inbox, (message) => replies.push(message));
  const packet = JSON.stringify({ type: 'call', id: '1', method: 'calc/echo', args: {} });
  const send = (body, headers, correlationId = 'c1') =>
    broker.direct.send(handle.address, body, { headers, correlationId, replyTo: inbox });

  await send(packet, { 'wrpc-kind': 'request' });
  // The sender's clock rides inside every sealed request (a clockless one is stale)
  const stamped = () => ({ 'wrpc-t': String(Date.now()) });
  const stranger = createBrokerSealing({ keys: generateKey() }, 'x', { layer: 'broker-rpc', replay: true });
  const forged = stranger.seal(`${handle.address}\0request\0c1\0`, stamped(), packet, { 'wrpc-kind': 'request' });
  await send(forged.body, forged.headers);
  // The right key, sealed for ANOTHER conversation or another kind: the binding is part of the seal
  const insider = createBrokerSealing({ keys }, 'x', { layer: 'broker-rpc', replay: true });
  const moved = insider.seal(`${handle.address}\0request\0c1\0`, stamped(), packet, { 'wrpc-kind': 'request' });
  await send(moved.body, moved.headers, 'c2');
  await send(moved.body, { ...moved.headers, 'wrpc-kind': 'hello' });
  await send(moved.body, moved.headers);
  await waitFor(() => replies.length === 1, 'the one honest frame was not answered');
  await send(moved.body, moved.headers);
  await send(Buffer.from('short'), moved.headers);
  await waitFor(() => warnings.filter((w) => w.event === 'broker.rpc.refused').length === 6);
  assert.deepStrictEqual(
    warnings.filter((w) => w.event === 'broker.rpc.refused').map((w) => w.reason),
    ['unsealed', 'open', 'open', 'open', 'replay', 'format'],
  );
  assert.strictEqual(replies.length, 1);
  // A client without the key cannot use the service, and one with it can
  const blind = await connect({ mode: 'stateless' });
  await assert.rejects(blind.load('calc'), /./);
  const sighted = await connect({ encryption: { keys } });
  await sighted.load('calc');
  assert.deepStrictEqual(await sighted.api.calc.echo({ ok: 1 }), { ok: 1 });
});

test('broker encryption: the rollout — open first, seal second, refuse last', async (t) => {
  const keys = generateKey();
  const { carried, broker } = spied(new MemoryBroker({ logger: quiet }));
  const one = await boot(t, { broker, attach: { encryption: { keys, seal: false, acceptPlaintext: true } } });
  const old = await one.connect();
  await old.load('calc');
  assert.deepStrictEqual(await old.api.calc.echo({ from: 'old' }), { from: 'old' });
  const next = await one.connect({ encryption: { keys, acceptPlaintext: true } });
  await next.load('calc');
  assert.deepStrictEqual(await next.api.calc.echo({ from: 'next' }), { from: 'next' });
  const sealedRequests = carried.filter((f) => f.headers['wrpc-kind'] === 'request' && f.headers[HEADER_SEALED]);
  const plainResponses = carried.filter((f) => f.headers['wrpc-kind'] === 'response' && !f.headers[HEADER_SEALED]);
  assert.ok(sealedRequests.length >= 2, 'the upgraded client seals');
  assert.ok(plainResponses.length >= 4, 'the service in deploy 1 still answers in the clear');
});

test('broker encryption: under encryption.required on the server, only a sealed binding is served', async (t) => {
  const keys = generateKey();
  const required = { encryption: { keys: generateKey(), required: true } };
  const sealed = await boot(t, { rpc: required, attach: { encryption: { keys } } });
  for (const mode of ['stateless', 'session']) {
    const client = await sealed.connect({ mode, encryption: { keys } });
    await client.load('calc');
    assert.deepStrictEqual(await client.api.calc.echo({ mode }), { mode });
  }
  const open = await boot(t, { rpc: required });
  const stateless = await open.connect();
  await assert.rejects(stateless.load('calc'), /./, 'a plaintext request is answered with a refusal, never served');
  await assert.rejects(open.connect({ mode: 'session' }), /encryption required/);
});

test('broker encryption: the options are validated, and a keyring is not a session object', async (t) => {
  const broker = new MemoryBroker({ logger: quiet });
  t.after(() => broker.close());
  const server = new RpcServer({ router, logger: quiet, sse: false });
  t.after(() => server.close());
  await assert.rejects(
    attachBrokerRpc(server, broker, { service: 'calc', logger: quiet, encryption: true }),
    /attachBrokerRpc: options: encryption must be \{ keys/,
  );
  const connect = (encryption) =>
    WrpcClient.connect('broker://calc', { transport: 'broker', broker, encryption, reconnect: false, logger: false });
  await assert.rejects(connect({ keys: 'short' }), /encryption\.keys must be 32 bytes/);
  await assert.rejects(connect({ secure() {}, param: 'wrpc_e' }), /'broker' cannot carry options\.encryption/);
  await timers.setTimeout(1);
});

test('broker sealing: an injected cipher keeps the key it was handed — another keyring does not open', () => {
  // The guide's shape: the key kept by reference and read at seal time,
  // with a tag that depends on it. A sealer wiping the bytes after key()
  // left such a cipher sealing under zeros — on both ends, unnoticed.
  const handed = [];
  const xor = {
    id: 'test-xor',
    keyLength: 32,
    nonceLength: 24,
    tagLength: 4,
    key: (raw) => {
      handed.push(raw);
      const tag = () => Buffer.from(raw.subarray(0, 4));
      return {
        seal: (nonce, plaintext) =>
          Buffer.concat([Buffer.from(plaintext).map((byte) => byte ^ raw[0] ^ nonce[23]), tag()]),
        open: (nonce, sealed) => {
          const bytes = Buffer.from(sealed);
          if (!bytes.subarray(-4).equals(tag())) throw new Error('bad tag');
          return Buffer.from(bytes.subarray(0, -4).map((byte) => byte ^ raw[0] ^ nonce[23]));
        },
      };
    },
  };
  const keys = generateKey();
  const options = { layer: 'broker-log', replay: false };
  const a = createBrokerSealing({ keys, cipher: xor }, 'x', options);
  const b = createBrokerSealing({ keys, cipher: xor }, 'x', options);
  const other = createBrokerSealing({ keys: generateKey(), cipher: xor }, 'x', options);
  const sealed = a.seal('orders', { n: 1 }, 'body', {});
  assert.strictEqual(Buffer.from(b.open('orders', sealed).body).toString(), 'body');
  assert.deepStrictEqual(other.open('orders', sealed), { refused: 'open' });
  for (const raw of handed) {
    assert.ok(
      raw.some((byte) => byte !== 0),
      'the cipher saw the derived key, not zeros',
    );
  }
});

test('broker sealing: headers go inside as a null-prototype string map; off is null', () => {
  const keys = generateKey();
  assert.strictEqual(createBrokerSealing(null, 'x', { layer: 'broker-log', replay: false }), null);
  const a = createBrokerSealing({ keys }, 'x', { layer: 'broker-log', replay: false });
  const b = createBrokerSealing({ keys }, 'x', { layer: 'broker-log', replay: false });
  const sealed = a.seal('orders', { tp: '00-abc', n: 7, __proto__: { polluted: 'yes' } }, 'body', { key: 'k' });
  assert.deepStrictEqual(Object.keys(sealed.headers).sort(), ['key', 'wrpc-sealed']);
  const opened = b.open('orders', sealed);
  assert.strictEqual(Object.getPrototypeOf(opened.headers), null);
  // Strings only, the way every broker hands headers back — a number goes
  // in as its text, the same map a plaintext message would carry.
  assert.deepStrictEqual({ ...opened.headers }, { tp: '00-abc', n: '7' });
  assert.strictEqual(Buffer.from(opened.body).toString(), 'body');
  assert.strictEqual(opened.sealed, true);
  // A log is read again and again: no replay window on that layer
  assert.strictEqual(b.open('orders', sealed).sealed, true);
  assert.deepStrictEqual(b.open('invoices', sealed), { refused: 'open' });
  assert.deepStrictEqual(b.open('orders', { headers: {}, body: 'plain' }), { refused: 'unsealed' });
});

test('broker encryption: a sealed session takes no plaintext frame — acceptPlaintext does not reopen it', async (t) => {
  const keys = generateKey();
  const { carried, broker } = spied(new MemoryBroker({ logger: quiet }));
  const { logger, warnings } = logs();
  const { handle, connect } = await boot(t, {
    broker,
    attach: { encryption: { keys, acceptPlaintext: true }, logger },
  });
  const client = await connect({ mode: 'session', encryption: { keys, acceptPlaintext: true }, logger });
  await client.load('calc');
  assert.deepStrictEqual(await client.api.calc.echo({ n: 1 }), { n: 1 });
  const hello = carried.find((f) => f.address === handle.address && f.headers[HEADER_KIND] === KIND.HELLO);
  const session = hello.correlationId;
  const clientInbox = hello.replyTo;
  const downgrades = () => warnings.filter((w) => w.event === 'broker.rpc.refused' && w.reason === 'downgrade');
  // The client's numbered frames so far, so the plaintext packet carries the
  // very next sequence number — the one that used to walk in.
  const sent = carried.filter((f) => f.address === handle.inbox && f.correlationId === session).length;
  const call = JSON.stringify({ type: 'call', id: 'x1', method: 'calc/echo', args: { plain: true } });
  await broker.direct.send(handle.inbox, call, {
    headers: { [HEADER_KIND]: KIND.PACKET, 'wrpc-seq': String(sent + 1) },
    correlationId: session,
    replyTo: clientInbox,
  });
  await broker.direct.send(handle.inbox, '', { headers: { [HEADER_KIND]: KIND.BYE }, correlationId: session });
  await waitFor(() => downgrades().length === 2);
  assert.strictEqual(handle.sessions, 1, 'a plaintext bye does not end a sealed session');
  // The sequence was not consumed: the real next frame is served.
  assert.deepStrictEqual(await client.api.calc.echo({ n: 2 }), { n: 2 });
  // The client's side of the same rule.
  await broker.direct.send(clientInbox, '', {
    headers: { [HEADER_KIND]: KIND.BYE, 'wrpc-reason': 'forged' },
    correlationId: session,
  });
  await broker.direct.send(clientInbox, JSON.stringify({ type: 'event', name: 'calc/nudged', data: {} }), {
    headers: { [HEADER_KIND]: KIND.PACKET, 'wrpc-seq': '99' },
    correlationId: session,
  });
  await waitFor(() => downgrades().length === 4);
  assert.strictEqual(client.active, true, 'a plaintext bye does not close a sealed client');
  assert.deepStrictEqual(await client.api.calc.echo({ n: 3 }), { n: 3 });
  assert.deepStrictEqual(
    downgrades().map((w) => w.kind),
    [KIND.PACKET, KIND.BYE, KIND.BYE, KIND.PACKET],
  );
});

test('broker encryption: a sealed request older than maxSkew, or without a clock, is refused as stale', async (t) => {
  const keys = generateKey();
  const { logger, warnings } = logs();
  const { broker, handle, connect } = await boot(t, { attach: { encryption: { keys, maxSkew: 200 }, logger } });
  const replies = [];
  const inbox = broker.direct.inbox();
  await broker.direct.listen(inbox, (message) => replies.push(message));
  const packet = JSON.stringify({ type: 'call', id: '1', method: 'calc/echo', args: {} });
  const sealer = createBrokerSealing({ keys }, 'x', { layer: 'broker-rpc', replay: true });
  const send = (inner, correlationId) => {
    const frame = sealer.seal(`${handle.address}\0request\0${correlationId}\0`, inner, packet, {
      [HEADER_KIND]: KIND.REQUEST,
    });
    return broker.direct.send(handle.address, frame.body, { headers: frame.headers, correlationId, replyTo: inbox });
  };
  await send({ 'wrpc-t': String(Date.now() - 1000) }, 'old');
  await send({ 'wrpc-t': String(Date.now() + 1000) }, 'future');
  await send({}, 'clockless'); // a 1.x client
  await send({ 'wrpc-t': String(Date.now()) }, 'fresh');
  await waitFor(() => replies.length === 1);
  await timers.setTimeout(30);
  assert.deepStrictEqual(
    replies.map((reply) => reply.correlationId),
    ['fresh'],
  );
  assert.deepStrictEqual(
    warnings.filter((w) => w.event === 'broker.rpc.refused').map((w) => w.reason),
    ['stale', 'stale', 'stale'],
  );
  // The real client stamps every request and hello.
  const client = await connect({ encryption: { keys } });
  await client.load('calc');
  assert.deepStrictEqual(await client.api.calc.echo({ ok: 1 }), { ok: 1 });
  const session = await connect({ mode: 'session', encryption: { keys } });
  await session.load('calc');
  assert.deepStrictEqual(await session.api.calc.echo({ ok: 2 }), { ok: 2 });
  await assert.rejects(
    attachBrokerRpc(new RpcServer({ router, logger: quiet, sse: false }), broker, {
      service: 'x',
      encryption: { keys, maxSkew: 0 },
    }),
    /maxSkew/,
  );
  await assert.rejects(
    attachBrokerRpc(new RpcServer({ router, logger: quiet, sse: false }), broker, {
      service: 'x',
      encryption: { keys, replay: {} },
    }),
    /replay must be/,
  );
});

test('broker encryption: a shared replay memory refuses a request replayed to another instance', async (t) => {
  const { createReplayCache } = require('../../encryption.js');
  const keys = generateKey();
  const broker = new MemoryBroker({ logger: quiet });
  const replay = createReplayCache();
  const { logger, warnings } = logs();
  // Three instances of one service, each with its own envelope window, one
  // shared memory between them.
  const instances = [];
  for (let i = 0; i < 3; i++) {
    instances.push(await boot(t, { broker, attach: { encryption: { keys, replay }, logger } }));
  }
  const { handle } = instances[0];
  const replies = [];
  const inbox = broker.direct.inbox();
  await broker.direct.listen(inbox, (message) => replies.push(message));
  const packet = JSON.stringify({ type: 'call', id: '1', method: 'calc/echo', args: {} });
  const sealer = createBrokerSealing({ keys }, 'x', { layer: 'broker-rpc', replay: true });
  const frame = sealer.seal(`${handle.address}\0request\0c1\0`, { 'wrpc-t': String(Date.now()) }, packet, {
    [HEADER_KIND]: KIND.REQUEST,
  });
  // Six copies of one captured frame spread over the group: without the
  // shared memory each instance would answer the first it saw.
  for (let i = 0; i < 6; i++) {
    await broker.direct.send(handle.address, frame.body, {
      headers: frame.headers,
      correlationId: 'c1',
      replyTo: inbox,
    });
  }
  await waitFor(() => warnings.filter((w) => w.event === 'broker.rpc.refused').length === 5);
  await timers.setTimeout(30);
  assert.strictEqual(replies.length, 1, 'answered once across the fleet');
  assert.ok(
    warnings.filter((w) => w.event === 'broker.rpc.refused').every((w) => w.reason === 'replay'),
    'every copy after the first is a replay',
  );
  // A memory that cannot be asked serves nothing.
  const errors = [];
  // Its own child(): the spread would hand back the parent, whose error() is a no-op.
  const failing = {
    ...logger,
    error: (entry) => errors.push(entry),
    child: () => failing,
  };
  const { handle: closed } = await boot(t, {
    broker,
    attach: {
      service: 'other',
      encryption: {
        keys,
        replay: {
          seen: () => Promise.reject(new Error('memory down')),
        },
      },
      logger: failing,
    },
  });
  const other = sealer.seal(`${closed.address}\0request\0c2\0`, { 'wrpc-t': String(Date.now()) }, packet, {
    [HEADER_KIND]: KIND.REQUEST,
  });
  await broker.direct.send(closed.address, other.body, { headers: other.headers, correlationId: 'c2', replyTo: inbox });
  await waitFor(() => errors.some((entry) => entry.event === 'broker.rpc.replay'));
  await timers.setTimeout(30);
  assert.strictEqual(replies.length, 1, 'not served on a memory that failed');
});
