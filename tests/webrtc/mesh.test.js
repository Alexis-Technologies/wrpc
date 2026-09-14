'use strict';

// Mesh over the fake RTC and the in-memory hub, then end to end: the real
// signaling unit on a wrpc server, wrpcSignaler over WebSocket, peers over
// the fake RTC — the whole path a browser tab takes, minus the browser.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure } = require('../../index.js');
const {
  WrpcPeer,
  Mesh,
  createSignalingUnit,
  createSignalingHooks,
  wrpcSignaler,
} = require('../../src/webrtc/index.js');
const { createFakeRtc } = require('./fakeRtc.js');
const { FakeSignalHub } = require('./fakeSignalHub.js');
const { within, waitFor } = require('./portContract.js');
const { bootServer, connectClient } = require('../helpers/server.js');

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

const onceEvent = (emitter, name) => new Promise((resolve) => emitter.once(name, resolve));

const routerOf = (name) =>
  defineRouter({
    chat: {
      hello: procedure({ handler: async (ctx) => `${name} greets ${ctx.session.data.peer}` }),
    },
  });

const world = (t, options = {}) => {
  const fake = createFakeRtc(options.fake);
  const hub = new FakeSignalHub();
  const peers = [];
  t.after(() => {
    for (const peer of peers) peer.close();
    fake.world.close();
  });
  const peer = (id, { router = true, signaler = hub.signaler(id), ...rest } = {}) => {
    const instance = new WrpcPeer({
      router: router ? routerOf(id) : null,
      signaler,
      rtc: fake.adapter,
      logger: quiet,
      client: { heartbeat: false, reconnect: { minDelay: 5, maxDelay: 20, jitter: false } },
      connectTimeout: 500,
      restartTimeout: 30,
      redial: { minDelay: 5, maxDelay: 20, jitter: false, retries: 3 },
      ...rest,
    });
    instance.on('error', (error) => void (instance.errors ??= []).push(error));
    peers.push(instance);
    return instance;
  };
  return { fake, hub, peer };
};

// Every member of `mesh` has an open link with the mesh.
const settled = (mesh, count) => waitFor(() => mesh.peers.size === count, `${mesh.room}: ${count} peers`);

test('mesh: everyone in the room links with everyone, from either side', async (t) => {
  const { peer } = world(t);
  const a = peer('a');
  const b = peer('b');
  const c = peer('c');
  const joins = { a: [], b: [], c: [] };
  const meshes = {};
  for (const [id, p] of Object.entries({ a, b, c })) {
    meshes[id] = p.join('lobby', { data: { name: id.toUpperCase() } });
    assert.ok(meshes[id] instanceof Mesh);
    assert.strictEqual(p.join('lobby'), meshes[id], 'join() is idempotent per room');
    assert.strictEqual(p.mesh('lobby'), meshes[id]);
    meshes[id].on('join', (event) => joins[id].push(event));
    await meshes[id].ready();
  }
  for (const mesh of Object.values(meshes)) await within(settled(mesh, 2), 'linked');
  assert.deepStrictEqual(meshes.a.peers, new Set(['b', 'c']));
  assert.deepStrictEqual(meshes.c.peers, new Set(['a', 'b']));
  assert.strictEqual(a.links.size, 2, 'one link per pair');
  assert.strictEqual(meshes.a.link('b'), a.link('b'));
  assert.strictEqual(meshes.a.links.size, 2);
  assert.strictEqual(meshes.a.room, 'lobby');
  assert.strictEqual(meshes.a.hostRoom, 'mesh:lobby');
  await waitFor(() => joins.a.length === 2 && joins.b.length === 2 && joins.c.length === 2, 'joins announced');
  const byId = (list) => [...list].sort((x, y) => x.id.localeCompare(y.id));
  assert.deepStrictEqual(byId(joins.a), [
    { id: 'b', data: { name: 'B' } },
    { id: 'c', data: { name: 'C' } },
  ]);
  assert.deepStrictEqual(byId(joins.c), [
    { id: 'a', data: { name: 'A' } },
    { id: 'b', data: { name: 'B' } },
  ]);
  // Every link's host Client sits in the mesh room on both ends.
  for (const link of a.links.values()) assert.ok(link.client.in('mesh:lobby'));
  assert.strictEqual(a.host.rooms.count('mesh:lobby'), 2);
  // The links are ordinary PeerLinks: calls work across them.
  const ab = meshes.a.link('b');
  await ab.load('chat');
  assert.strictEqual(await ab.api.chat.hello(), 'b greets a');
});

test('mesh: broadcast, ask and respond reach every member, present and future', async (t) => {
  const { peer } = world(t);
  const a = peer('a');
  const b = peer('b');
  const c = peer('c');
  const ma = a.join('room');
  const mb = b.join('room');
  const mc = c.join('room');
  for (const mesh of [ma, mb, mc]) await within(settled(mesh, 2), 'linked');

  // broadcast: one event to each member's remote client; delivered on the
  // unit emitter of the link, the way server events are.
  const heard = [];
  for (const [id, mesh] of Object.entries({ b: mb, c: mc })) {
    const link = mesh.link('a');
    await link.load('chat');
    link.api.chat.on('note', (data) => heard.push([id, data]));
  }
  assert.strictEqual(ma.broadcast('chat/note', { n: 1 }), 2);
  await waitFor(() => heard.length === 2, 'both heard');
  assert.deepStrictEqual(heard.sort(), [
    ['b', { n: 1 }],
    ['c', { n: 1 }],
  ]);

  // ask/respond: each member answers through its link to the asker.
  mb.respond('poll', async (data) => `b:${data.q}`);
  mc.respond('poll', async () => {
    throw Object.assign(new Error('c abstains'), { code: 409 });
  });
  const result = await ma.ask('poll', { q: 1 }, { timeout: 1000 });
  assert.deepStrictEqual(result.answers, ['b:1']);
  assert.deepStrictEqual(result.errors, [{ message: 'c abstains', code: 409 }]);
  assert.strictEqual(result.expected, 2);
  assert.strictEqual(result.incomplete, false);

  // A member arriving later gets the responders and the room.
  const d = peer('d');
  const md = d.join('room');
  const joined = onceEvent(ma, 'join');
  await within(settled(md, 3), 'd linked to all');
  assert.deepStrictEqual(await within(joined, 'a saw d'), { id: 'd', data: null });
  await within(settled(ma, 3), 'a sees d');
  md.respond('poll', async () => 'd!');
  mb.respond('poll', async () => 'b again');
  assert.strictEqual(mb.unrespond('missing'), false);
  const second = await ma.ask('poll', { q: 2 }, { timeout: 1000 });
  assert.deepStrictEqual(second.answers.sort(), ['b again', 'd!']);
  assert.strictEqual(second.expected, 3);
  assert.strictEqual(mc.unrespond('poll'), true);
  assert.throws(() => mc.respond('x', 'not a function'), /handler must be a function/);
  const third = await ma.ask('poll', {}, { timeout: 200 });
  assert.strictEqual(third.errors.find((e) => e.code === 501)?.message, "No responder for 'poll'");
});

test('mesh: leave announces to the others and closes the links nobody else holds', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a');
  const b = peer('b');
  const c = peer('c');
  const ma = a.join('room');
  const mb = b.join('room');
  const mc = c.join('room');
  for (const mesh of [ma, mb, mc]) await within(settled(mesh, 2), 'linked');
  // a and b are also together in another room: that link survives a's leave.
  const xa = a.join('side');
  const xb = b.join('side');
  await within(settled(xa, 1), 'side linked');
  await within(settled(xb, 1), 'side linked');
  assert.strictEqual(a.links.size, 2, 'the a↔b link serves both meshes');

  const leaves = [];
  mb.on('leave', (event) => leaves.push(['b', event.id]));
  mc.on('leave', (event) => leaves.push(['c', event.id]));
  const left = onceEvent(ma, 'left');
  await ma.leave();
  await within(left, 'left');
  await ma.leave(); // idempotent
  assert.strictEqual(a.mesh('room'), undefined);
  assert.deepStrictEqual(hub.members('room'), new Set(['b', 'c']));
  await waitFor(() => leaves.length === 2, 'both told');
  assert.deepStrictEqual(leaves.sort(), [
    ['b', 'a'],
    ['c', 'a'],
  ]);
  await waitFor(() => c.links.size === 1 && a.links.size === 1, 'a↔c closed, a↔b kept');
  assert.strictEqual(a.link('b').state, 'open');
  assert.ok(!a.link('b').client.in('mesh:room'), 'the kept link left the mesh room');
  assert.ok(a.link('b').client.in('mesh:side'));
  assert.deepStrictEqual(mb.peers, new Set(['c']));
  assert.strictEqual(ma.broadcast('chat/note', 1), 0, 'a left mesh fans out to nobody');
  // Leaving the last room drops the link too; a signaler that fails the
  // leave call is an error on the peer, not a throw.
  const original = a.signaler.leave;
  a.signaler.leave = () => Promise.reject(new Error('leave refused'));
  await xa.leave();
  a.signaler.leave = original;
  assert.strictEqual(a.errors.at(-1).message, 'leave refused');
  await waitFor(() => a.links.size === 0 && b.links.size === 1, 'all a links gone');
});

test('mesh: a signaling reset rebuilds the room under the new id', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a');
  const b = peer('b');
  const ma = a.join('room', { data: 'A' });
  const mb = b.join('room', { data: 'B' });
  await within(settled(ma, 1), 'linked');
  await within(settled(mb, 1), 'linked');
  const events = [];
  mb.on('leave', (event) => events.push(['leave', event.id]));
  mb.on('join', (event) => events.push(['join', event.id, event.data]));
  const rejoined = onceEvent(ma, 'join');
  await hub.reset('a', 'a2');
  assert.strictEqual(a.id, 'a2');
  assert.deepStrictEqual(await within(rejoined, 'a relinked'), { id: 'b', data: 'B' });
  await within(settled(mb, 1), 'b relinked');
  await waitFor(() => events.length === 2, 'b saw the swap');
  assert.deepStrictEqual(events, [
    ['leave', 'a'],
    ['join', 'a2', 'A'],
  ]);
  assert.strictEqual(ma.link('b').link.localId, 'a2');
  assert.strictEqual(mb.link('a2').state, 'open');
  assert.strictEqual(mb.link('a'), undefined);
});

test('mesh: a peer without a router cannot fan out, and a signaler without a roster cannot mesh', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a', { router: false });
  const b = peer('b');
  const ma = a.join('room');
  const mb = b.join('room');
  await within(settled(ma, 1), 'linked');
  await within(settled(mb, 1), 'linked');
  assert.throws(() => ma.broadcast('x/y', 1), /no router/);
  assert.throws(() => ma.ask('x/y', 1), /no router/);
  assert.strictEqual(mb.broadcast('chat/note', 1), 1);
  const bare = hub.signaler('bare');
  const original = { join: bare.join, leave: bare.leave };
  delete bare.join;
  Object.defineProperty(bare, 'join', { value: undefined, configurable: true });
  Object.defineProperty(bare, 'leave', { value: undefined, configurable: true });
  const lone = peer('lone', { signaler: bare });
  assert.throws(() => lone.join('room'), /roster/);
  assert.throws(() => lone.join(''), /room must be/);
  Object.defineProperty(bare, 'join', { value: original.join, configurable: true });
  Object.defineProperty(bare, 'leave', { value: original.leave, configurable: true });
  // A link this peer made outside the mesh's room is not a member.
  peer('c');
  await within(b.connect('c'), 'direct link');
  assert.strictEqual(mb.has('c'), false);
  assert.strictEqual(b.links.size, 2);
  // Closing the peer detaches its meshes without a leave on the wire.
  b.close();
  assert.strictEqual(b.mesh('room'), undefined);
  assert.deepStrictEqual(hub.members('room'), new Set(['a', 'b']), "the roster is the signaler owner's to clean");
});

// ---------------------------------------------------------------------------
// End to end over a real wrpc server.

test('mesh: over createSignalingUnit + wrpcSignaler on a real server', async (t) => {
  const router = defineRouter({ ...createSignalingUnit({ access: 'public' }) }, { hooks: createSignalingHooks() });
  const { server, url } = await bootServer(t, { router });
  const fake = createFakeRtc();
  t.after(() => fake.world.close());
  const peers = [];
  t.after(() => {
    for (const peer of peers) peer.close();
  });
  const peer = async (name) => {
    const client = await connectClient(t, url, { reconnect: { minDelay: 10, maxDelay: 20, jitter: false } });
    client.on('error', () => {});
    const signaler = wrpcSignaler(client);
    const instance = new WrpcPeer({
      router: routerOf(name),
      signaler,
      rtc: fake.adapter,
      logger: quiet,
      client: { heartbeat: false, reconnect: { minDelay: 5, maxDelay: 20, jitter: false } },
      connectTimeout: 1000,
      restartTimeout: 50,
      redial: { minDelay: 5, maxDelay: 20, jitter: false, retries: 3 },
    });
    instance.on('error', () => {});
    peers.push(instance);
    const id = await instance.start();
    return { instance, signaler, client, id, name };
  };
  const a = await peer('a');
  const b = await peer('b');
  const c = await peer('c');
  const ma = a.instance.join('lobby', { data: { name: 'a' } });
  const mb = b.instance.join('lobby', { data: { name: 'b' } });
  const mc = c.instance.join('lobby', { data: { name: 'c' } });
  for (const mesh of [ma, mb, mc]) await within(settled(mesh, 2), 'linked over the server');
  assert.deepStrictEqual(ma.peers, new Set([b.id, c.id]));
  const link = ma.link(b.id);
  await link.load('chat');
  assert.strictEqual(await link.api.chat.hello(), `b greets ${a.id}`);
  assert.strictEqual(ma.broadcast('chat/note', 1), 2);
  mb.respond('poll', async () => 'b');
  mc.respond('poll', async () => 'c');
  const asked = await ma.ask('poll', {}, { timeout: 1000 });
  assert.deepStrictEqual(asked.answers.sort(), ['b', 'c']);

  // The server drops a's signaling connection: b and c see a leave (the
  // hook), a comes back under a new id, re-joins and re-links.
  const oldId = a.id;
  const events = [];
  mb.on('leave', (event) => events.push(['leave', event.id]));
  mb.on('join', (event) => events.push(['join', event.id]));
  const reset = onceEvent(a.signaler, 'reset');
  server.rpc.getClient(oldId).close();
  await within(reset, 'a reset');
  const newId = a.signaler.id;
  assert.notStrictEqual(newId, oldId);
  await within(settled(ma, 2), 'a relinked');
  await within(settled(mb, 2), 'b relinked');
  await waitFor(() => events.length === 2, 'b saw the swap');
  assert.deepStrictEqual(events, [
    ['leave', oldId],
    ['join', newId],
  ]);
  assert.strictEqual(ma.link(b.id).link.localId, newId);
  const relinked = mb.link(newId);
  await relinked.load('chat');
  assert.strictEqual(await relinked.api.chat.hello(), `a greets ${b.id}`);

  await mc.leave();
  await waitFor(() => ma.peers.size === 1 && mb.peers.size === 1, 'c gone');
  await timers.setTimeout(10);
  assert.strictEqual(c.instance.links.size, 0);
});
