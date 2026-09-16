'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { Emitter } = require('../../src/utils.js');
const { defineRouter, procedure } = require('../../src/rpc/router.js');
const { PeerHost, isInboundTransport } = require('../../src/webrtc/host.js');
const { chunkEncode } = require('../../src/chunks.js');

const quiet = {
  log() {},
  info() {},
  warn() {},
  error() {},
  debug() {},
  child() {
    return this;
  },
};

// A persistent inbound transport in memory: what the host writes lands in
// `sent` (parsed), what the test feeds through packet()/chunk() is inbound.
class FakePeerTransport extends Emitter {
  kind = 'fake';
  sent = [];
  raw = [];
  closed = 0;
  accept = true;
  constructor(peer) {
    super({ maxListeners: Number.MAX_SAFE_INTEGER });
    this.source = peer;
    this.connection = this;
  }
  send(obj, _code, text) {
    if (this.codec) return this.write(this.codec.encode(obj));
    return this.write(text ?? JSON.stringify(obj));
  }
  error(code, { id = '', error = null } = {}) {
    return this.send({ type: 'callback', id, error: { message: error?.message ?? String(code), code } });
  }
  write(data) {
    if (typeof data === 'string') this.sent.push(JSON.parse(data));
    else this.raw.push(data);
    return this.accept;
  }
  close() {
    this.closed++;
    void this.emit('close');
  }
  packet(obj) {
    void this.emit('packet', JSON.stringify(obj));
  }
  chunk(bytes) {
    void this.emit('chunk', bytes);
  }
}

const waitFor = async (predicate, message = 'condition never held') => {
  for (let i = 0; i < 300; i++) {
    if (predicate()) return;
    await timers.setTimeout(5);
  }
  assert.fail(message);
};

const routerOf = (seen) =>
  defineRouter({
    math: {
      add: procedure({ handler: async (_ctx, { a, b }) => a + b }), // default access: session
      whoami: procedure({
        access: 'public',
        handler: async (ctx) => ({
          peer: ctx.meta.data.peer,
          room: ctx.meta.data.room,
          session: ctx.session ? ctx.session.data.peer : null,
          id: ctx.client.id,
          server: ctx.client.server === ctx.server,
        }),
      }),
      count: procedure({
        access: 'public',
        handler: async function* (_ctx, { to }) {
          for (let n = 1; n <= to; n++) yield { n };
        },
      }),
      readUpload: procedure({
        access: 'public',
        handler: async (ctx, { id }) => {
          const stream = ctx.client.getStream(id);
          const chunks = [];
          for await (const chunk of stream) chunks.push(Buffer.from(chunk));
          return { data: Buffer.concat(chunks).toString('utf8') };
        },
      }),
      session: procedure({
        access: 'public',
        handler: async (ctx) => {
          try {
            ctx.client.startSession('t', {});
            return 'started';
          } catch (error) {
            return { code: error.code, message: error.message };
          }
        },
      }),
      on: {
        ping: procedure({ access: 'public', handler: async (ctx, data) => void seen.push([ctx.client.source, data]) }),
      },
    },
  });

test('peer host: constructor validation and shape', () => {
  assert.throws(() => new PeerHost({}), /router/);
  assert.throws(() => new PeerHost({ router: routerOf([]), codec: {} }), /codec/);
  assert.throws(() => new PeerHost({ router: routerOf([]), trust: 'maybe' }), /trust/);
  assert.throws(() => new PeerHost({ router: routerOf([]), instanceId: 'a.b' }), /instanceId/);
  assert.throws(() => new PeerHost({ router: routerOf([]), instanceId: '' }), /instanceId/);
  const host = new PeerHost({ router: routerOf([]), logger: quiet, instanceId: 'peer-1' });
  assert.strictEqual(host.instanceId, 'peer-1');
  assert.strictEqual(host.trust, 'link');
  assert.strictEqual(host.clients.size, 0);
  assert.strictEqual(typeof host.router.getProcedure('system', '*', 'introspect'), 'object', 'introspection mounted');
  const bare = new PeerHost({ router: routerOf([]), logger: quiet, introspection: false });
  assert.ok(!bare.router.getProcedure('system', '*', 'introspect'));
  assert.ok(!new PeerHost({ router: routerOf([]) }).instanceId.includes('.'));
  assert.strictEqual(isInboundTransport(new FakePeerTransport('x')), true);
  assert.strictEqual(isInboundTransport({ write() {}, close() {}, on() {}, once() {} }), false, 'not persistent');
  assert.strictEqual(isInboundTransport(null), false);
});

test('peer host: attach validation', () => {
  const host = new PeerHost({ router: routerOf([]), logger: quiet });
  assert.throws(() => host.attach({}, { peer: 'a' }), /persistent transport/);
  assert.throws(() => host.attach(new FakePeerTransport('a'), {}), /peer must be/);
  assert.throws(() => host.attach(new FakePeerTransport('a'), { peer: '' }), /peer must be/);
});

test('peer host: a call, introspection, meta and the link pseudo-session', async () => {
  const seen = [];
  const host = new PeerHost({ router: routerOf(seen), logger: quiet, instanceId: 'me' });
  const transport = new FakePeerTransport('them');
  const attached = [];
  host.on('attach', (client) => attached.push(client));
  const client = host.attach(transport, { peer: 'them', room: 'lobby', data: { name: 'Grace' } });
  assert.deepStrictEqual(attached, [client]);
  assert.strictEqual(client.persistent, true);
  assert.strictEqual(client.binary, true);
  assert.strictEqual(client.source, 'them');
  assert.ok(client.id.startsWith('me.'));
  assert.strictEqual(host.getClient(client.id), client);
  assert.strictEqual(client.session.token, 'them');
  assert.deepStrictEqual({ ...client.session.data }, { peer: 'them', room: 'lobby', name: 'Grace' });
  assert.strictEqual(Object.isFrozen(client.session), true);
  transport.packet({ type: 'call', id: '1', method: 'math/add', args: { a: 2, b: 3 } });
  transport.packet({ type: 'call', id: '2', method: 'math/whoami', args: {} });
  transport.packet({ type: 'call', id: '3', method: 'system/introspect', args: ['math'] });
  await waitFor(() => transport.sent.length === 3);
  const byId = Object.fromEntries(transport.sent.map((p) => [p.id, p]));
  assert.strictEqual(byId['1'].result, 5, 'access session is satisfied by the link');
  assert.deepStrictEqual(byId['2'].result, {
    peer: 'them',
    room: 'lobby',
    session: 'them',
    id: client.id,
    server: true,
  });
  assert.ok(byId['3'].result.math.add, 'introspection answers');
  assert.strictEqual(byId['3'].result.math.add.access, 'session');
});

test("peer host: trust 'assertion' requires verified claims and exposes them in the session", async () => {
  const seen = [];
  const host = new PeerHost({ router: routerOf(seen), logger: quiet, trust: 'assertion' });
  assert.strictEqual(host.trust, 'assertion');
  const transport = new FakePeerTransport('alice');
  assert.throws(() => host.attach(transport, { peer: 'alice' }), /requires the peer's verified claims/);
  assert.throws(() => host.attach(transport, { peer: 'alice', claims: 'yes' }), /claims must be an object/);
  assert.throws(() => host.attach(transport, { peer: 'alice', claims: [1] }), /claims must be an object/);
  const claims = { sub: 'alice', role: 'host', fp: 'sha-256 AA:BB' };
  // Roster data cannot shadow the claims: they are placed last.
  const client = host.attach(transport, { peer: 'alice', data: { claims: 'forged', name: 'ada' }, claims });
  assert.deepStrictEqual(client.session.token, 'alice');
  assert.deepStrictEqual(client.session.data.claims, claims);
  assert.ok(Object.isFrozen(client.session.data.claims));
  assert.strictEqual(client.session.data.name, 'ada');
  assert.strictEqual(client.meta.data.claims.role, 'host');
  // Under trust 'link' the claims are optional, and kept when given.
  const linked = new PeerHost({ router: routerOf(seen), logger: quiet });
  const bare = linked.attach(new FakePeerTransport('bob'), { peer: 'bob' });
  assert.strictEqual(bare.session.data.claims, undefined);
  const withClaims = linked.attach(new FakePeerTransport('carol'), { peer: 'carol', claims: { sub: 'carol' } });
  assert.deepStrictEqual(withClaims.session.data.claims, { sub: 'carol' });
  // Under trust 'none' they ride the meta only.
  const none = new PeerHost({ router: routerOf(seen), logger: quiet, trust: 'none' });
  const open = none.attach(new FakePeerTransport('dan'), { peer: 'dan', claims: { sub: 'dan' } });
  assert.strictEqual(open.session, null);
  assert.deepStrictEqual(open.meta.data.claims, { sub: 'dan' });
});

test("peer host: trust 'none' leaves the session null — session procedures answer 403", async () => {
  const host = new PeerHost({ router: routerOf([]), logger: quiet, trust: 'none' });
  const transport = new FakePeerTransport('them');
  const client = host.attach(transport, { peer: 'them' });
  assert.strictEqual(client.session, null);
  transport.packet({ type: 'call', id: '1', method: 'math/add', args: { a: 2, b: 3 } });
  transport.packet({ type: 'call', id: '2', method: 'math/whoami', args: {} });
  await waitFor(() => transport.sent.length === 2);
  const byId = Object.fromEntries(transport.sent.map((p) => [p.id, p]));
  assert.strictEqual(byId['1'].error.code, 403);
  assert.strictEqual(byId['2'].result.session, null);
});

test('peer host: sessions are refused with a coded error, not a TypeError', async () => {
  const host = new PeerHost({ router: routerOf([]), logger: quiet });
  const transport = new FakePeerTransport('them');
  const client = host.attach(transport, { peer: 'them' });
  transport.packet({ type: 'call', id: '1', method: 'math/session', args: {} });
  await waitFor(() => transport.sent.length === 1);
  assert.deepStrictEqual(transport.sent[0].result, {
    code: 400,
    message: 'startSession: sessions are not available on this host',
  });
  await assert.rejects(client.restoreSession('t'), /restoreSession: sessions are not available/);
  // The link session can be dropped by the app without a store behind it.
  assert.strictEqual(await client.finalizeSession(), true);
  assert.strictEqual(client.session, null);
  assert.strictEqual(await client.finalizeSession(), false);
});

test('peer host: subscriptions, inbound events, uploads and server-initiated traffic', async () => {
  const seen = [];
  const host = new PeerHost({ router: routerOf(seen), logger: quiet });
  const transport = new FakePeerTransport('them');
  const client = host.attach(transport, { peer: 'them' });
  transport.packet({ type: 'subscribe', id: 's1', method: 'math/count', args: { to: 2 } });
  await waitFor(() => transport.sent.some((p) => p.type === 'end' && p.id === 's1'));
  assert.deepStrictEqual(
    transport.sent.filter((p) => p.id === 's1').map((p) => p.data ?? p.type),
    [{ n: 1 }, { n: 2 }, 'end'],
  );
  transport.packet({ type: 'event', name: 'math/ping', data: { hello: true } });
  await waitFor(() => seen.length === 1);
  assert.deepStrictEqual(seen, [['them', { hello: true }]]);
  // An upload: stream packet, chunk bytes, end — then the call that reads it.
  transport.packet({ type: 'stream', id: 'u1', name: 'file', size: 5 });
  transport.packet({ type: 'call', id: 'c1', method: 'math/readUpload', args: { id: 'u1' } });
  transport.chunk(chunkEncode('u1', new TextEncoder().encode('hello')));
  transport.packet({ type: 'stream', id: 'u1', status: 'end' });
  await waitFor(() => transport.sent.some((p) => p.id === 'c1'));
  assert.deepStrictEqual(transport.sent.find((p) => p.id === 'c1').result, { data: 'hello' });
  // Host → peer: an event, an ask answered by the peer, a download stream.
  client.sendEvent('math/tick', { t: 1 });
  assert.deepStrictEqual(transport.sent.at(-1), { type: 'event', name: 'math/tick', data: { t: 1 } });
  const answer = client.ask('math/question', { q: 1 }, { timeout: 1000 });
  const asked = transport.sent.at(-1);
  assert.strictEqual(asked.type, 'event');
  assert.ok(asked.id);
  transport.packet({ type: 'callback', id: asked.id, result: 42 });
  assert.strictEqual(await answer, 42);
  const stream = client.createStream('down', 3);
  stream.write(new Uint8Array([1, 2, 3]));
  stream.end();
  assert.ok(transport.raw.length >= 1, 'chunk bytes went out raw');
});

test('peer host: rooms, to/except/broadcast, backpressure counting and close()', async () => {
  const host = new PeerHost({ router: routerOf([]), logger: quiet });
  const t1 = new FakePeerTransport('one');
  const t2 = new FakePeerTransport('two');
  const t3 = new FakePeerTransport('three');
  const c1 = host.attach(t1, { peer: 'one' });
  const c2 = host.attach(t2, { peer: 'two' });
  const c3 = host.attach(t3, { peer: 'three' });
  assert.strictEqual(host.clients.size, 3);
  c1.join('mesh:lobby');
  c2.join('mesh:lobby');
  assert.strictEqual(host.rooms.count('mesh:lobby'), 2);
  assert.strictEqual(host.to('mesh:lobby').emit('x/y', { n: 1 }), 2);
  assert.strictEqual(host.to('mesh:lobby').except(c1).emit('x/y', { n: 2 }), 1);
  assert.strictEqual(host.broadcast('x/all', null), 3);
  assert.deepStrictEqual(
    t1.sent.map((p) => p.data),
    [{ n: 1 }, null],
  );
  assert.deepStrictEqual(
    t2.sent.map((p) => p.data),
    [{ n: 1 }, { n: 2 }, null],
  );
  assert.deepStrictEqual(
    t3.sent.map((p) => p.data),
    [null],
  );
  t2.accept = false;
  assert.strictEqual(host.to('mesh:lobby').emit('x/y', 3), 2, 'a backpressured peer still counts as a recipient');
  const detached = [];
  host.on('detach', (client) => detached.push(client));
  host.close();
  assert.strictEqual(t1.closed + t2.closed + t3.closed, 3);
  await timers.setTimeout(5);
  assert.strictEqual(host.clients.size, 0);
  assert.strictEqual(host.rooms.count('mesh:lobby'), 0);
  assert.deepStrictEqual(new Set(detached), new Set([c1, c2, c3]));
  assert.strictEqual(host.getClient(c1.id), undefined);
});

test('peer host: router connection hooks run in order and see the peer', async () => {
  const events = [];
  const router = defineRouter(
    {
      echo: { hi: procedure({ access: 'public', handler: async () => 'hi' }) },
    },
    {
      hooks: {
        onConnect: async (client) => {
          await timers.setTimeout(5);
          events.push(['connect', client.source]);
          client.join('joined-by-hook');
        },
        onDisconnect: async (client, { rooms }) => void events.push(['disconnect', client.source, [...rooms]]),
      },
    },
  );
  const host = new PeerHost({ router, logger: quiet });
  const transport = new FakePeerTransport('them');
  host.attach(transport, { peer: 'them' });
  // Dispatch is gated on client.ready, so the room joined by the hook is
  // there by the time the first call runs.
  transport.packet({ type: 'call', id: '1', method: 'echo/hi', args: {} });
  await waitFor(() => transport.sent.length === 1);
  assert.deepStrictEqual(events, [['connect', 'them']]);
  transport.close();
  await waitFor(() => events.length === 2);
  assert.deepStrictEqual(events[1], ['disconnect', 'them', ['joined-by-hook']]);
});

test('peer host: a packet codec re-frames the wire; codec + serializers is refused', async () => {
  const codec = {
    encode: (packet) => `C:${JSON.stringify(packet)}`,
    decode: (text) => JSON.parse(text.slice(2)),
  };
  const host = new PeerHost({ router: routerOf([]), logger: quiet, codec });
  const transport = new FakePeerTransport('them');
  const raw = [];
  transport.write = (data) => {
    raw.push(data);
    return true;
  };
  host.attach(transport, { peer: 'them' });
  assert.strictEqual(transport.codec, codec);
  void transport.emit('packet', 'C:{"type":"call","id":"1","method":"math/whoami","args":{}}');
  await waitFor(() => raw.length === 1);
  assert.ok(raw[0].startsWith('C:{"type":"callback"'));
  const serializing = defineRouter(
    {
      s: {
        get: procedure({
          access: 'public',
          schema: { response: { 200: { type: 'object' } } },
          handler: async () => ({}),
        }),
      },
    },
    {
      validation: {
        ajv: { compile: () => () => true },
        serializer: { compile: () => (value) => JSON.stringify(value) },
      },
    },
  );
  assert.strictEqual(serializing.hasSerializers, true);
  assert.throws(() => new PeerHost({ router: serializing, codec, logger: quiet }), /mutually exclusive/);
});
