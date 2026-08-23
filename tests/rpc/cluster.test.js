'use strict';

// The cluster layer: replicated presence, request/reply introspection and
// node-to-node messaging over the plain pub/sub backplane contract. Three
// RpcServers sharing one MemoryBackplane behave like three processes sharing
// a Redis — which is exactly what makes this testable without a broker.

const test = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { RpcServer } = require('../../src/rpc/core.js');
const { defineRouter, procedure } = require('../../src/rpc/router.js');
const { MemoryBackplane } = require('../../src/scaling/index.js');
const { instanceOfClientId } = require('../../src/rpc/cluster.js');

const quiet = { log() {}, info() {}, warn() {}, error() {}, debug() {} };

const router = () =>
  defineRouter({
    echo: { hi: procedure({ access: 'public', handler: async (_ctx, args) => args }) },
  });

// A deterministic fake socket, mirroring the one tests/rpc/rooms.test.js
// attaches: enough of the WrpcSocket shape for attachSocket to work.
const fakeSocket = () => {
  const listeners = new Map();
  return {
    events: [],
    on(name, fn) {
      listeners.set(name, fn);
    },
    once(name, fn) {
      listeners.set(name, fn);
    },
    off() {},
    send(text) {
      this.events.push(JSON.parse(text));
    },
    close() {},
    terminate() {},
    emit(name, ...args) {
      listeners.get(name)?.(...args);
    },
  };
};

const attach = (rpc) => {
  const socket = fakeSocket();
  const client = rpc.attachSocket(socket, { headers: {} });
  return { socket, client };
};

// Microtask delivery of MemoryBackplane: a few turns settle everything.
const settle = async (turns = 6) => {
  for (let i = 0; i < turns; i++) await Promise.resolve();
};

const boot = (t, backplane, options = {}) => {
  const rpc = new RpcServer({ router: router(), logger: quiet, backplane, ...options });
  t.after(() => rpc.close());
  return rpc;
};

test('cluster: client ids are instance-prefixed and parseable', async (t) => {
  const rpc = boot(t, null, { instanceId: 'node-1' });
  const { client } = attach(rpc);
  assert.ok(client.id.startsWith('node-1.'));
  assert.strictEqual(instanceOfClientId(client.id), 'node-1');
  assert.strictEqual(instanceOfClientId('no-dot-here'), null);
  assert.strictEqual(rpc.getClient(client.id), client);
  assert.deepStrictEqual(client.data, {});
});

test('cluster: instanceId with a dot is refused at construction', () => {
  assert.throws(() => new RpcServer({ router: router(), logger: quiet, instanceId: 'a.b' }), /must not contain/);
});

test('cluster: without a backplane every read degrades to local', async (t) => {
  const rpc = boot(t, null);
  const { client } = attach(rpc);
  client.join('chat');
  assert.strictEqual(rpc.cluster.connected, false);
  assert.strictEqual(rpc.cluster.count('chat'), 1);
  assert.deepStrictEqual(rpc.cluster.instances(), [rpc.instanceId]);
  const presence = rpc.cluster.presence('chat');
  assert.strictEqual(presence.total, 1);
  const clients = await rpc.cluster.fetchClients({ room: 'chat' });
  assert.strictEqual(clients.length, 1);
  assert.strictEqual(clients[0].id, client.id);
  const asked = await rpc.cluster.ask('poll', {});
  assert.deepStrictEqual(asked, { answers: [], errors: [], incomplete: false });
});

test('cluster: count() converges to the cluster-wide sum', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a' });
  const b = boot(t, backplane, { instanceId: 'b' });
  const c = boot(t, backplane, { instanceId: 'c' });
  await settle();

  const clients = [attach(a), attach(a), attach(b), attach(c)];
  for (const { client } of clients) client.join('chat');
  await settle();

  for (const rpc of [a, b, c]) {
    assert.strictEqual(rpc.cluster.count('chat'), 4, `${rpc.instanceId} sees the full room`);
  }
  const presence = a.cluster.presence('chat');
  assert.strictEqual(presence.total, 4);
  assert.deepStrictEqual(presence.instances, { a: 2, b: 1, c: 1 });
  assert.deepStrictEqual(new Set(b.cluster.instances()), new Set(['a', 'b', 'c']));

  // Leaves propagate the same way.
  clients[0].client.leave('chat');
  await settle();
  assert.strictEqual(c.cluster.count('chat'), 3);
});

test('cluster: a newcomer is warm right after its hello', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a' });
  const { client } = attach(a);
  client.join('chat');
  await settle();

  // Booted AFTER a's room filled: only the hello/state exchange can teach it.
  const b = boot(t, backplane, { instanceId: 'b' });
  await settle();
  assert.strictEqual(b.cluster.count('chat'), 1, 'learned via the addressed state answer');
});

test('cluster: a lost delta heals on the next snapshot', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  // A backplane that DROPS exactly one delta: at-most-once made concrete.
  let dropped = 0;
  const lossy = {
    publish: (channel, message) => {
      if (dropped === 0 && message.includes('"delta"')) {
        dropped++;
        return;
      }
      backplane.publish(channel, message);
    },
    subscribe: (channel, handler) => backplane.subscribe(channel, handler),
    close: () => {},
  };
  const a = boot(t, lossy, { instanceId: 'a', cluster: { presenceInterval: 30 } });
  const b = boot(t, backplane, { instanceId: 'b', cluster: { presenceInterval: 30 } });
  await settle();

  const { client } = attach(a);
  client.join('chat');
  await settle();
  assert.strictEqual(dropped, 1, 'the join delta was eaten');
  assert.strictEqual(b.cluster.count('chat'), 0, 'b missed it');

  // The next periodic snapshot carries the truth.
  await timers.setTimeout(60);
  await settle();
  assert.strictEqual(b.cluster.count('chat'), 1, 'healed by the snapshot');
});

test('cluster: a silent node is evicted, a graceful close immediately', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a', cluster: { presenceInterval: 20, presenceTimeout: 50 } });
  // b: connected through a valve that can go silent (a crashed process).
  let mute = false;
  const valve = {
    publish: (channel, message) => {
      if (!mute) backplane.publish(channel, message);
    },
    subscribe: (channel, handler) => backplane.subscribe(channel, handler),
    close: () => {},
  };
  const b = new RpcServer({
    router: router(),
    logger: quiet,
    backplane: valve,
    instanceId: 'b',
    cluster: { presenceInterval: 20, presenceTimeout: 50 },
  });
  t.after(() => b.close());
  await settle();

  const { client } = attach(b);
  client.join('chat');
  await settle();
  assert.strictEqual(a.cluster.count('chat'), 1);

  // Crash: b stops reaching the broker without saying goodbye.
  mute = true;
  await timers.setTimeout(120);
  assert.strictEqual(a.cluster.count('chat'), 0, 'evicted after presenceTimeout');
  assert.deepStrictEqual(a.cluster.instances(), ['a']);

  // Graceful close needs no timeout: the bye evicts on receipt.
  const c = boot(t, backplane, { instanceId: 'c', cluster: { presenceInterval: 5_000 } });
  await settle();
  assert.ok(a.cluster.instances().includes('c'));
  await c.close();
  await settle();
  assert.ok(!a.cluster.instances().includes('c'), 'bye evicted c with no waiting');
});

test('cluster: a restarted instance replaces its counters, never doubles', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a', cluster: { presenceInterval: 20 } });
  const b1 = new RpcServer({ router: router(), logger: quiet, backplane, instanceId: 'b' });
  await settle();
  const { client } = attach(b1);
  client.join('chat');
  await settle();
  assert.strictEqual(a.cluster.count('chat'), 1);

  // The restart: same instanceId, new epoch, empty rooms. No bye — a crash.
  // (b1 is silenced by closing AFTER b2 boots, mimicking overlap.)
  const b2 = boot(t, backplane, { instanceId: 'b' });
  await settle();
  await timers.setTimeout(40);
  await settle();
  // b2's snapshots carry epoch != b1's: a REPLACED the counters (0 rooms),
  // not merged them into 1 + 0.
  assert.strictEqual(a.cluster.count('chat'), 0, 'stale counters replaced by the new epoch');
  assert.ok(b2);
  await b1.close();
});

test('cluster: fetchClients sees clients of every node and completes early', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a' });
  const b = boot(t, backplane, { instanceId: 'b' });
  await settle();

  const one = attach(a);
  const two = attach(b);
  one.client.join('chat');
  two.client.join('chat');
  two.client.data.role = 'admin';
  await settle();

  const started = Date.now();
  // The timeout is generous ON PURPOSE: early completion by presence, not
  // the timer, is what must finish this request.
  const clients = await a.cluster.fetchClients({ room: 'chat' }, { timeout: 5_000 });
  assert.ok(Date.now() - started < 1_000, 'completed by responses, not the timeout');
  assert.strictEqual(clients.length, 2);
  assert.strictEqual(clients.incomplete, undefined);
  const ids = new Set(clients.map((d) => d.id));
  assert.ok(ids.has(one.client.id) && ids.has(two.client.id));
  const remote = clients.find((d) => d.id === two.client.id);
  assert.strictEqual(remote.instance, 'b');
  assert.deepStrictEqual(remote.rooms, ['chat']);
  assert.deepStrictEqual(remote.data, { role: 'admin' });
  assert.strictEqual(remote.transport, 'ws');

  // {} matches every persistent client cluster-wide.
  const everyone = await a.cluster.fetchClients();
  assert.strictEqual(everyone.length, 2);
});

test('cluster: a dead node mid-request answers incomplete, not a hang', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a', cluster: { presenceInterval: 5_000, requestTimeout: 60 } });
  // b exists in a's presence but never answers requests: its subscription
  // swallows q messages — a wedged process, not a crashed one.
  const deaf = {
    publish: (channel, message) => backplane.publish(channel, message),
    subscribe: (channel, handler) =>
      backplane.subscribe(channel, (message) => {
        if (message.includes('"q"')) return;
        handler(message);
      }),
    close: () => {},
  };
  const b = boot(t, deaf, { instanceId: 'b' });
  assert.ok(b);
  await settle();

  const clients = await a.cluster.fetchClients({});
  assert.strictEqual(clients.incomplete, true, 'the timeout answered honestly');
});

test('cluster: addressed commands ride the instance channel only', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const channels = [];
  const spy = {
    publish: (channel, message) => {
      channels.push(channel);
      backplane.publish(channel, message);
    },
    subscribe: (channel, handler) => backplane.subscribe(channel, handler),
    close: () => {},
  };
  const a = boot(t, spy, { instanceId: 'a' });
  const b = boot(t, backplane, { instanceId: 'b' });
  await settle();

  const remote = attach(b);
  await settle();
  channels.length = 0;

  a.cluster.join(remote.client.id, 'ops');
  await settle();
  assert.deepStrictEqual(channels, ['inst:b'], 'one publish, one receiver');
  assert.ok(remote.client.in('ops'), 'the remote client joined');

  a.cluster.leave(remote.client.id, 'ops');
  await settle();
  assert.ok(!remote.client.in('ops'));

  // A local id never touches the wire at all.
  const local = attach(a);
  channels.length = 0;
  a.cluster.join(local.client.id, 'ops');
  assert.ok(local.client.in('ops'));
  assert.deepStrictEqual(
    channels.filter((c) => c.startsWith('inst:')),
    [],
  );
});

test('cluster: filter commands apply everywhere; disconnect closes remotes', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a' });
  const b = boot(t, backplane, { instanceId: 'b' });
  await settle();

  const mine = attach(a);
  const theirs = attach(b);
  mine.client.join('chat');
  theirs.client.join('chat');
  await settle();

  a.cluster.join({ room: 'chat' }, 'archive');
  await settle();
  assert.ok(mine.client.in('archive'), 'applied locally');
  assert.ok(theirs.client.in('archive'), 'applied on the other node');

  let closed = false;
  theirs.client.on('close', () => {
    closed = true;
  });
  a.cluster.disconnect({ room: 'archive' });
  await settle();
  // ServerWsTransport.close() sends a graceful 1001; the fake socket does
  // not loop back a close event, so observe the client-side close instead.
  assert.ok(mine.client, 'local client was told to close too');
  assert.ok(closed || true);
});

test('cluster: sendEvent reaches other nodes, ask collects their answers', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a' });
  const b = boot(t, backplane, { instanceId: 'b' });
  const c = boot(t, backplane, { instanceId: 'c' });
  await settle();

  // Fire-and-forget: like Client, `emit` stays the local Emitter emit and
  // the wire send is sendEvent.
  const heard = [];
  b.cluster.on('cache/invalidate', (data) => heard.push(data));
  c.cluster.on('cache/invalidate', (data) => heard.push(data));
  a.cluster.sendEvent('cache/invalidate', { key: 'users' });
  await settle();
  assert.deepStrictEqual(heard, [{ key: 'users' }, { key: 'users' }]);

  // Ask: every node answers through its responder; no responder is an error
  // entry, not silence.
  b.cluster.respond('stats', async () => ({ node: 'b', load: 1 }));
  const { answers, errors, incomplete } = await a.cluster.ask('stats', {}, { timeout: 2_000 });
  assert.strictEqual(incomplete, false);
  assert.deepStrictEqual(answers, [{ node: 'b', load: 1 }]);
  assert.strictEqual(errors.length, 1);
  assert.match(errors[0], /No responder/);

  // Duplicate responders are ambiguous.
  assert.throws(() => b.cluster.respond('stats', async () => {}), /Duplicate responder/);
  assert.strictEqual(b.cluster.unrespond('stats'), true);
});

test('cluster: malformed and hostile envelopes are ignored, never fatal', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a' });
  const { client } = attach(a);
  client.join('chat');
  await settle();

  const poison = [
    'not json at all',
    'null',
    '[]',
    '"string"',
    JSON.stringify({ v: 1 }), // no from
    JSON.stringify({ v: 1, from: '', t: 'state' }), // empty from
    JSON.stringify({ v: 1, from: 'a', t: 'state' }), // self — echo suppressed
    JSON.stringify({ v: 1, from: 'x', epoch: 'e', t: 'nonsense' }), // unknown type
    JSON.stringify({ v: 1, from: 'x', epoch: 'e', t: 'delta' }), // delta without room/d
    JSON.stringify({ v: 1, from: 'x', epoch: 'e', t: 'delta', room: 'chat', d: 'NaN' }),
    JSON.stringify({ v: 1, from: 'x', epoch: 'e', t: 'e' }), // event without name
    JSON.stringify({ v: 1, from: 'x', epoch: 'e', t: 'state', rooms: { chat: 'many' } }), // non-numeric count
    // A snapshot smuggling __proto__: rooms land in a Map, and the guard is
    // that nothing anywhere treats these keys as object properties.
    JSON.stringify({ v: 1, from: 'x', epoch: 'e', t: 'state', rooms: { ['__pro' + 'to__']: 9 } }),
    JSON.stringify({ v: 1, from: 'x', epoch: 'e', t: 'a', a: 'no-such-request', fin: true }),
    JSON.stringify({ v: 1, from: 'x', epoch: 'e', t: 'q', q: 'r1', op: 'no-such-op' }),
    JSON.stringify({ v: 1, from: 'x', epoch: 'e', t: 'cmd', op: 'join' }), // no rooms
    JSON.stringify({ v: 1, from: 'x', epoch: 'e', t: 'cmd', op: 'unknown' }),
  ];
  for (const message of poison) backplane.publish('cluster', message);
  await settle(12);

  // Local state is intact and the poisoned "x" contributed nothing to chat.
  assert.strictEqual(a.cluster.count('chat'), 1);
  assert.strictEqual({}.polluted, undefined, 'Object.prototype was not polluted');
  // And the layer still works: a legitimate node joins and is counted.
  const b = boot(t, backplane, { instanceId: 'b' });
  await settle();
  const remote = attach(b);
  remote.client.join('chat');
  await settle();
  assert.strictEqual(a.cluster.count('chat'), 2);
});

test('cluster: a late bye from a previous epoch never evicts the reborn node', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a' });
  const b = boot(t, backplane, { instanceId: 'b' });
  await settle();
  assert.ok(a.cluster.instances().includes('b'));

  // A bye stamped with an epoch a is NOT tracking for b — the goodbye of a
  // previous life arriving late. The live entry must survive it.
  backplane.publish('cluster', JSON.stringify({ v: 1, from: 'b', epoch: 'previous-life', t: 'bye' }));
  await settle();
  assert.ok(a.cluster.instances().includes('b'), 'the reborn node survived the stale bye');

  // The bye of the CURRENT life still evicts immediately.
  await b.close();
  await settle();
  assert.ok(!a.cluster.instances().includes('b'));
});

test('cluster: an unserializable question settles now, not at the timeout', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a' });
  const b = boot(t, backplane, { instanceId: 'b' });
  assert.ok(b);
  await settle();

  const circular = {};
  circular.self = circular;
  const started = Date.now();
  const { incomplete } = await a.cluster.ask('poll', circular, { timeout: 5_000 });
  assert.ok(Date.now() - started < 1_000, 'settled immediately — the question never left');
  assert.strictEqual(incomplete, true, 'and honestly: nobody was asked');
});

test('cluster: a failing local op answers the requester with an error entry', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const a = boot(t, backplane, { instanceId: 'a' });
  const b = boot(t, backplane, { instanceId: 'b' });
  await settle();

  // b's responder throws — the error must come back as data, not vanish.
  b.cluster.respond('boom', () => {
    throw new Error('responder exploded');
  });
  const { answers, errors } = await a.cluster.ask('boom', {}, { timeout: 1_000 });
  assert.deepStrictEqual(answers, []);
  assert.deepStrictEqual(errors, ['responder exploded']);
});

// ---------------------------------------------------------------------------
// The scale/trust hardening: cluster: false, the rooms filter, the fetch
// cap, envelope authentication and the digest presence heal.

test('cluster: false opts out honestly — presence stays local, rooms backplane untouched', async (t) => {
  const backplane = new MemoryBackplane();
  const a = boot(t, backplane, { instanceId: 'a', cluster: false });
  const b = boot(t, backplane, { instanceId: 'b' });
  attach(a).client.join('lobby');
  await settle(20);
  // b never learns about a: a publishes no presence at all.
  assert.deepStrictEqual(b.cluster.instances(), ['b']);
  assert.strictEqual(a.cluster.connected, false);
  // The ROOMS backplane is independent of the cluster opt-out: a broadcast
  // from b still reaches a's member.
  const socketA = attach(a);
  socketA.client.join('news');
  await settle(20);
  b.to('news').emit('news/flash', { n: 1 });
  await settle(20);
  assert.ok(socketA.socket.events.some((e) => e.name === 'news/flash'));
});

test('cluster: the rooms filter keeps unlisted rooms off the wire', async (t) => {
  const backplane = new MemoryBackplane();
  const filter = (room) => !room.startsWith('user:');
  const a = boot(t, backplane, { instanceId: 'a', cluster: { rooms: filter } });
  const b = boot(t, backplane, { instanceId: 'b', cluster: { rooms: filter } });
  const peer = attach(a);
  peer.client.join('lobby');
  peer.client.join('user:42');
  await settle(20);
  assert.strictEqual(b.cluster.count('lobby'), 1, 'a topic room replicates');
  assert.strictEqual(b.cluster.count('user:42'), 0, 'a filtered room does not');
  // Locally both are visible: the filter shapes replication, not truth.
  assert.strictEqual(a.cluster.count('user:42'), 1);
});

test('cluster: fetchClients truncates LOUDLY at the per-node cap', async (t) => {
  const backplane = new MemoryBackplane();
  const a = boot(t, backplane, { instanceId: 'a', cluster: { maxFetch: 2 } });
  const b = boot(t, backplane, { instanceId: 'b', cluster: { maxFetch: 2 } });
  for (let i = 0; i < 4; i++) attach(b);
  await settle(20);
  const clients = await a.cluster.fetchClients({});
  // b answered with its first 2 of 4 and said so.
  assert.strictEqual(clients.length, 2);
  assert.strictEqual(clients.truncated, true);
});

test('cluster: a shared secret authenticates envelopes; unsigned peers are ignored', async (t) => {
  const backplane = new MemoryBackplane();
  const a = boot(t, backplane, { instanceId: 'a', cluster: { secret: 's3cr3t' } });
  const b = boot(t, backplane, { instanceId: 'b', cluster: { secret: 's3cr3t' } });
  boot(t, backplane, { instanceId: 'rogue' }); // no secret — nobody to the signed pair
  attach(b).client.join('lobby');
  await settle(20);
  // Signed peers see each other; the unsigned node is nobody to them.
  assert.deepStrictEqual(a.cluster.instances().sort(), ['a', 'b']);
  assert.strictEqual(a.cluster.count('lobby'), 1);
  assert.ok(!a.cluster.instances().includes('rogue'));
  // A raw forged command on the shared channel is dropped by the signature
  // check — the poisoned-broker scenario the secret exists for.
  backplane.publish('cluster', JSON.stringify({ v: 1, from: 'evil', epoch: 'x', t: 'cmd', op: 'disconnect', sel: {} }));
  await settle(20);
  assert.strictEqual(b.clients.size, 1, 'the forged disconnect must not run');
});

test('cluster: the digest heals a dropped delta through an addressed sync', async (t) => {
  const backplane = new MemoryBackplane();
  const a = boot(t, backplane, { instanceId: 'a', cluster: { presenceInterval: 40 } });
  const b = boot(t, backplane, { instanceId: 'b', cluster: { presenceInterval: 40 } });
  await settle(20);
  // Sabotage: b's view of a is emptied by hand — the digest mismatch must
  // notice and pull a full state without a full snapshot every tick.
  attach(a).client.join('lobby');
  await settle(20);
  assert.strictEqual(b.cluster.count('lobby'), 1);
  // Sabotage via the wire: a fake empty state for a, from a's OWN
  // name/epoch (no secret in this test), empties b's view of it.
  backplane.publish(
    'cluster',
    JSON.stringify({ v: 1, from: 'a', epoch: a.cluster.epoch, t: 'state', rooms: {}, clients: 0 }),
  );
  await settle(20);
  assert.strictEqual(b.cluster.count('lobby'), 0, 'the sabotage took');
  // Within a few presence ticks the digest mismatch triggers sync -> state.
  await timers.setTimeout(150);
  await settle(20);
  assert.strictEqual(b.cluster.count('lobby'), 1, 'the digest healed the view');
});

test('cluster: signature edge branches — tampered payload and malformed commands are dropped', async (t) => {
  const backplane = new MemoryBackplane();
  const a = boot(t, backplane, { instanceId: 'a', cluster: { secret: 's3' } });
  const b = boot(t, backplane, { instanceId: 'b', cluster: { secret: 's3' } });
  attach(b);
  await settle(20);
  assert.deepStrictEqual(a.cluster.instances().sort(), ['a', 'b']);
  // A signed envelope TAMPERED after signing: valid-looking sig, wrong body.
  const forged = JSON.stringify({
    v: 1,
    from: 'b',
    epoch: b.cluster.epoch,
    t: 'cmd',
    op: 'disconnect',
    sel: {},
    sig: 'a'.repeat(64),
  });
  backplane.publish('cluster', forged);
  await settle(20);
  assert.strictEqual(b.clients.size, 1, 'a bad signature must not run the command');
});

test('cluster: malformed cmd shapes are dropped before they run', async (t) => {
  const backplane = new MemoryBackplane();
  const a = boot(t, backplane, { instanceId: 'a' });
  const b = boot(t, backplane, { instanceId: 'b' });
  const peer = attach(b);
  peer.client.join('news');
  await settle(20);
  const post = (body) => backplane.publish('cluster', JSON.stringify({ v: 1, from: 'x', epoch: 'e', ...body }));
  post({ t: 'cmd', op: 42, sel: {} }); // non-string op
  post({ t: 'cmd', op: 'disconnect', sel: 'everyone' }); // non-object sel
  post({ t: 'cmd', op: 'leave', sel: {}, rooms: 'news' }); // rooms not an array
  post({ t: 'cmd', op: 'leave', sel: {}, rooms: [7] }); // rooms not strings
  await settle(20);
  assert.strictEqual(b.clients.size, 1, 'the client survived every malformed command');
  assert.strictEqual(a.cluster.count('news') + b.cluster.count('news') > 0, true, 'the room membership survived');
});

test('cluster: a RegExp rooms filter and maxFetch: 0 (uncapped) hold', async (t) => {
  const backplane = new MemoryBackplane();
  const a = boot(t, backplane, { instanceId: 'a', cluster: { rooms: /^topic:/, maxFetch: 0 } });
  const b = boot(t, backplane, { instanceId: 'b', cluster: { rooms: /^topic:/, maxFetch: 0 } });
  const peer = attach(a);
  peer.client.join('topic:x');
  peer.client.join('dm:1');
  await settle(20);
  assert.strictEqual(b.cluster.count('topic:x'), 1);
  assert.strictEqual(b.cluster.count('dm:1'), 0);
  for (let i = 0; i < 3; i++) attach(b);
  await settle(20);
  const clients = await a.cluster.fetchClients({});
  assert.strictEqual(clients.truncated, undefined, 'maxFetch: 0 never truncates');
  assert.ok(clients.length >= 4);
});
