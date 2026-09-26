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
      carried.push({ address, text, headers: { ...message?.headers } });
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
  const stranger = createBrokerSealing({ keys: generateKey() }, 'x', { layer: 'broker-rpc', replay: true });
  const forged = stranger.seal(`${handle.address}\0request\0c1\0`, {}, packet, { 'wrpc-kind': 'request' });
  await send(forged.body, forged.headers);
  // The right key, sealed for ANOTHER conversation or another kind: the binding is part of the seal
  const insider = createBrokerSealing({ keys }, 'x', { layer: 'broker-rpc', replay: true });
  const moved = insider.seal(`${handle.address}\0request\0c1\0`, {}, packet, { 'wrpc-kind': 'request' });
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
  for (const raw of handed)
    assert.ok(
      raw.some((byte) => byte !== 0),
      'the cipher saw the derived key, not zeros',
    );
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
  assert.deepStrictEqual({ ...opened.headers }, { tp: '00-abc' }, 'strings only');
  assert.strictEqual(Buffer.from(opened.body).toString(), 'body');
  assert.strictEqual(opened.sealed, true);
  // A log is read again and again: no replay window on that layer
  assert.strictEqual(b.open('orders', sealed).sealed, true);
  assert.deepStrictEqual(b.open('invoices', sealed), { refused: 'open' });
  assert.deepStrictEqual(b.open('orders', { headers: {}, body: 'plain' }), { refused: 'unsealed' });
});
