'use strict';

// The built-in signaling unit, driven the way a browser would drive it:
// real WS clients over bootServer with wrpcSignaler as the client half, plus
// two RpcServers over one MemoryBackplane for the cross-instance relay.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure, RpcServer } = require('../../index.js');
const { MemoryBackplane } = require('../../src/scaling/index.js');
const {
  createSignalingUnit,
  createSignalingHooks,
  wrpcSignaler,
  isSignaler,
  hasRoster,
} = require('../../src/webrtc/index.js');
const { bootServer, connectClient, waitFor } = require('../helpers/server.js');

const quiet = { log() {}, info() {}, warn() {}, error() {}, debug() {} };

const signalingRouter = (unit = {}, options = {}) =>
  defineRouter(
    { ...createSignalingUnit({ access: 'public', ...unit }) },
    { hooks: createSignalingHooks(unit), ...options },
  );

// A peer: one WS client and the signaler over it, with every inbound
// event recorded so a test can assert on what reached it.
const peer = async (t, url, options = {}) => {
  const client = await connectClient(t, url, options);
  const signaler = wrpcSignaler(client, options.unit ? { unit: options.unit } : undefined);
  const heard = [];
  for (const name of ['signal', 'join', 'leave']) signaler.on(name, (payload) => heard.push({ name, ...payload }));
  const id = await signaler.ready();
  return { client, signaler, heard, id };
};

const description = { type: 'description', description: { type: 'offer', sdp: 'v=0' } };

test('signaling: option validation', () => {
  assert.throws(() => createSignalingUnit({ name: 'a/b' }), /name must be a unit name/);
  assert.throws(() => createSignalingUnit({ name: '' }), /name must be a unit name/);
  assert.throws(() => createSignalingUnit({ access: 'admin' }), /access must be/);
  assert.throws(() => createSignalingUnit({ authorize: 'yes' }), /authorize must be a function/);
  assert.throws(() => createSignalingUnit({ relay: 'everyone' }), /relay must be/);
  assert.throws(() => createSignalingUnit({ prefix: 1 }), /prefix must be a string/);
  assert.throws(() => createSignalingHooks({ name: '' }), /name must be a string/);
  assert.throws(() => createSignalingHooks({ prefix: null }), /prefix must be a string/);
  const unit = createSignalingUnit({ name: 'rtc' });
  assert.deepStrictEqual(Object.keys(unit), ['rtc']);
  assert.deepStrictEqual(Object.keys(unit.rtc).sort(), ['emits', 'join', 'leave', 'members', 'on', 'whoami']);
  assert.deepStrictEqual(Object.keys(createSignalingHooks()), ['onDisconnect']);
});

test('signaling: the client half satisfies the Signaler and RosterSignaler contracts', async (t) => {
  const { url } = await bootServer(t, { router: signalingRouter() });
  const { signaler, id, client } = await peer(t, url);
  assert.ok(isSignaler(signaler));
  assert.ok(hasRoster(signaler));
  assert.strictEqual(signaler.id, id);
  assert.strictEqual(signaler.client, client);
  assert.strictEqual(signaler.unit, 'signaling');
  assert.ok(id.includes('.'), 'the id is the instance-prefixed server client id');
  // ready() is single-flight and idempotent.
  assert.strictEqual(await signaler.ready(), id);
});

test('signaling: whoami answers the server-side client id', async (t) => {
  const { server, url } = await bootServer(t, { router: signalingRouter() });
  const { id } = await peer(t, url);
  assert.ok(server.rpc.getClient(id), 'the id names a live server client');
});

test('signaling: join returns the roster and announces to the members already there', async (t) => {
  const { server, url } = await bootServer(t, { router: signalingRouter() });
  const a = await peer(t, url);
  const b = await peer(t, url);

  assert.deepStrictEqual(await a.signaler.join('lobby', { name: 'ada' }), []);
  assert.deepStrictEqual(a.signaler.rooms, new Set(['lobby']));
  assert.ok(server.rpc.getClient(a.id).in('rtc:lobby'), 'rooms are namespaced under the prefix');

  const members = await b.signaler.join('lobby', { name: 'bob' });
  assert.deepStrictEqual(members, [{ id: a.id, data: { name: 'ada' } }]);
  await waitFor(() => a.heard.length === 1);
  assert.deepStrictEqual(a.heard, [{ name: 'join', room: 'lobby', id: b.id, data: { name: 'bob' } }]);
  assert.deepStrictEqual(b.heard, [], 'the joiner does not hear its own join');

  // members() is the same roster on demand; a re-join updates the data
  // without a second announcement.
  assert.deepStrictEqual(await a.signaler.members('lobby'), [{ id: b.id, data: { name: 'bob' } }]);
  await b.signaler.join('lobby', { name: 'bobby' });
  assert.deepStrictEqual(await a.signaler.members('lobby'), [{ id: b.id, data: { name: 'bobby' } }]);
  await timers.setTimeout(20);
  assert.strictEqual(a.heard.length, 1);
  // Data is per room: joining a second room with none leaves the first as is.
  assert.deepStrictEqual(await b.signaler.join('other'), []);
  assert.deepStrictEqual(await a.signaler.members('lobby'), [{ id: b.id, data: { name: 'bobby' } }]);
  assert.deepStrictEqual(await a.signaler.members('other'), [{ id: b.id, data: null }]);
  // A member the application put into the room itself, without join, has
  // no data of its own and is still listed.
  const c = await peer(t, url);
  server.rpc.getClient(c.id).join('rtc:lobby');
  assert.deepStrictEqual(await a.signaler.members('lobby'), [
    { id: b.id, data: { name: 'bobby' } },
    { id: c.id, data: null },
  ]);
});

test('signaling: signals relay between members of a room, in both directions', async (t) => {
  const { url } = await bootServer(t, { router: signalingRouter() });
  const a = await peer(t, url);
  const b = await peer(t, url);
  await a.signaler.join('room');
  await b.signaler.join('room');
  await waitFor(() => a.heard.length === 1);
  a.heard.length = 0;

  a.signaler.send(b.id, description, { room: 'room' });
  await waitFor(() => b.heard.length === 1);
  assert.deepStrictEqual(b.heard, [{ name: 'signal', from: a.id, room: 'room', message: description }]);

  const candidate = { type: 'candidate', candidate: { candidate: 'a=1', sdpMid: '0' } };
  b.signaler.send(a.id, candidate, { room: 'room' });
  b.signaler.send(a.id, { type: 'close' }, { room: 'room' });
  await waitFor(() => a.heard.length === 2);
  assert.deepStrictEqual(a.heard[0], { name: 'signal', from: b.id, room: 'room', message: candidate });
  assert.deepStrictEqual(a.heard[1], { name: 'signal', from: b.id, room: 'room', message: { type: 'close' } });
});

test("signaling: relay 'room' bounds delivery to a shared membership", async (t) => {
  const { url } = await bootServer(t, { router: signalingRouter() });
  const a = await peer(t, url);
  const b = await peer(t, url);
  const c = await peer(t, url);
  await a.signaler.join('room');
  await b.signaler.join('room');

  // Not a member (c) sending into the room; a member sending to a non-member;
  // a member sending with no room; and a member sending with a bad payload
  // straight through the client: none of these arrive.
  c.signaler.send(a.id, description, { room: 'room' });
  a.signaler.send(c.id, description, { room: 'room' });
  a.client.sendEvent('signaling/signal', { to: b.id, message: description });
  a.client.sendEvent('signaling/signal', { to: b.id, room: 'room', message: { type: 'nope' } });
  a.client.sendEvent('signaling/signal', 'garbage');
  a.client.sendEvent('signaling/signal', null);
  // Then one that does, to prove the channel was open all along.
  a.signaler.send(b.id, { type: 'close' }, { room: 'room' });
  await waitFor(() => b.heard.some((entry) => entry.name === 'signal'));
  await timers.setTimeout(20);
  assert.deepStrictEqual(
    b.heard.filter((entry) => entry.name === 'signal'),
    [{ name: 'signal', from: a.id, room: 'room', message: { type: 'close' } }],
  );
  await timers.setTimeout(20);
  assert.deepStrictEqual(c.heard, []);
  assert.deepStrictEqual(
    a.heard.filter((entry) => entry.name === 'signal'),
    [],
  );
});

test("signaling: relay 'any' reaches any connected client by id", async (t) => {
  const { url } = await bootServer(t, { router: signalingRouter({ relay: 'any' }) });
  const a = await peer(t, url);
  const b = await peer(t, url);
  a.signaler.send(b.id, description);
  a.client.sendEvent('signaling/signal', { to: b.id, room: null, message: { type: 'close' } });
  await waitFor(() => b.heard.length === 2);
  assert.deepStrictEqual(b.heard, [
    { name: 'signal', from: a.id, room: null, message: description },
    { name: 'signal', from: a.id, room: null, message: { type: 'close' } },
  ]);
  // An unknown id is not deliverable; nothing blows up.
  a.signaler.send('nobody.here', description);
  await timers.setTimeout(20);
  assert.strictEqual(b.heard.length, 2);
});

test('signaling: leave and disconnect both announce to the remaining members', async (t) => {
  const { url } = await bootServer(t, { router: signalingRouter() });
  const a = await peer(t, url);
  const b = await peer(t, url);
  const c = await peer(t, url);
  await a.signaler.join('room');
  await b.signaler.join('room');
  await c.signaler.join('room');
  await c.signaler.join('other');
  await waitFor(() => a.heard.length === 2 && b.heard.length === 1);
  a.heard.length = 0;
  b.heard.length = 0;

  await b.signaler.leave('room');
  assert.deepStrictEqual(b.signaler.rooms, new Set());
  await waitFor(() => a.heard.length === 1);
  assert.deepStrictEqual(a.heard, [{ name: 'leave', room: 'room', id: b.id }]);
  // Leaving a room one is not in is honest, and announces nothing.
  await b.signaler.leave('room');
  assert.deepStrictEqual(await b.client.call('signaling/leave', { room: 'room' }), { room: 'room', left: false });

  // A dropped connection leaves every rtc room through the hooks — one
  // announcement per room, none for 'other' where a is not a member.
  a.heard.length = 0;
  c.client.close();
  await waitFor(() => a.heard.length === 1);
  assert.deepStrictEqual(a.heard, [{ name: 'leave', room: 'room', id: c.id }]);
  await timers.setTimeout(20);
  assert.deepStrictEqual(b.heard, [], 'b left before c dropped');
});

test('signaling: input validation answers 400', async (t) => {
  const { url } = await bootServer(t, { router: signalingRouter() });
  const { client, signaler } = await peer(t, url);
  for (const args of [{}, { room: '' }, { room: 'x'.repeat(257) }, null, 'lobby', { room: 7 }]) {
    await assert.rejects(client.call('signaling/join', args), (error) => error.code === 400);
    await assert.rejects(client.call('signaling/members', args), (error) => error.code === 400);
  }
  // The client half refuses locally, before any packet.
  assert.throws(() => signaler.send('', description), /to must be a peer id/);
  assert.throws(() => signaler.send('x', { type: 'nope' }), /message.type/);
  assert.throws(() => signaler.send('x', description, { room: '' }), /room must be/);
  await assert.rejects(signaler.join(''), /room must be/);
  await assert.rejects(signaler.leave(''), /room must be/);
  await assert.rejects(signaler.members(3), /room must be/);
});

test('signaling: authorize gates join and signal', async (t) => {
  const seen = [];
  const authorize = async (context, info) => {
    seen.push({ id: context.client.id, ...info });
    if (info.action === 'join' && info.room === 'vip') return false;
    if (info.action === 'join' && info.room === 'boom') throw Object.assign(new Error('nope'), { code: 418 });
    if (info.action === 'signal' && info.message.type === 'close') return false;
    return true;
  };
  const { url } = await bootServer(t, { router: signalingRouter({ authorize }) });
  const a = await peer(t, url);
  const b = await peer(t, url);
  await assert.rejects(a.signaler.join('vip'), (error) => error.code === 403 && /join refused/.test(error.message));
  await assert.rejects(a.signaler.join('boom'), (error) => error.code === 418 && error.message === 'nope');
  assert.deepStrictEqual(a.signaler.rooms, new Set(), 'a refused join is not remembered');
  await a.signaler.join('room', { role: 'host' });
  await b.signaler.join('room');
  a.signaler.send(b.id, { type: 'close' }, { room: 'room' });
  a.signaler.send(b.id, description, { room: 'room' });
  await waitFor(() => b.heard.some((entry) => entry.name === 'signal'));
  assert.deepStrictEqual(
    b.heard.filter((entry) => entry.name === 'signal'),
    [{ name: 'signal', from: a.id, room: 'room', message: description }],
  );
  assert.deepStrictEqual(seen[0], { id: a.id, action: 'join', room: 'vip', data: null });
  assert.deepStrictEqual(seen[2], { id: a.id, action: 'join', room: 'room', data: { role: 'host' } });
  assert.deepStrictEqual(seen[4], { id: a.id, action: 'signal', room: 'room', to: b.id, message: { type: 'close' } });
});

test("signaling: the default access is 'session'", async (t) => {
  const router = defineRouter(
    {
      ...createSignalingUnit(),
      auth: {
        signin: procedure({
          access: 'public',
          handler: async (context) => {
            context.client.startSession(undefined, { user: 'noa' });
            return { ok: true };
          },
        }),
      },
    },
    { hooks: createSignalingHooks() },
  );
  const { url } = await bootServer(t, { router });
  const client = await connectClient(t, url);
  const signaler = wrpcSignaler(client);
  await assert.rejects(signaler.ready(), (error) => error.code === 403);
  assert.strictEqual(signaler.id, null);
  await client.call('auth/signin');
  const id = await signaler.ready();
  assert.strictEqual(typeof id, 'string');
  assert.deepStrictEqual(await signaler.join('room'), []);
});

test('signaling: a custom unit name and prefix', async (t) => {
  const unit = { name: 'rtc', prefix: 'p2p/' };
  const { server, url } = await bootServer(t, { router: signalingRouter(unit) });
  const a = await peer(t, url, { unit: 'rtc' });
  const b = await peer(t, url, { unit: 'rtc' });
  await a.signaler.join('room');
  assert.ok(server.rpc.getClient(a.id).in('p2p/room'));
  await b.signaler.join('room');
  await waitFor(() => a.heard.length === 1);
  b.signaler.send(a.id, description, { room: 'room' });
  await waitFor(() => a.heard.length === 2);
  assert.strictEqual(a.heard[1].message.type, 'description');
  b.client.close();
  await waitFor(() => a.heard.length === 3);
  assert.deepStrictEqual(a.heard[2], { name: 'leave', room: 'room', id: b.id });
});

test('signaling: a reconnected signaling client gets a new id, re-joins and announces reset', async (t) => {
  const { server, url } = await bootServer(t, { router: signalingRouter() });
  const a = await peer(t, url, { reconnect: { minDelay: 10, maxDelay: 20, jitter: false } });
  a.client.on('error', () => {});
  const b = await peer(t, url);
  await a.signaler.join('room', { name: 'ada' });
  await b.signaler.join('room', { name: 'bob' });
  await waitFor(() => a.heard.length === 1);
  const resets = [];
  a.signaler.on('reset', (payload) => resets.push(payload));

  // The server drops a's connection: b hears the leave, a reconnects with a
  // fresh server-side client, re-joins and b hears the join again.
  server.rpc.getClient(a.id).close();
  await waitFor(() => resets.length === 1);
  const [reset] = resets;
  assert.strictEqual(reset.previous, a.id);
  assert.notStrictEqual(reset.id, a.id);
  assert.strictEqual(a.signaler.id, reset.id);
  assert.deepStrictEqual(reset.rooms, [{ room: 'room', members: [{ id: b.id, data: { name: 'bob' } }] }]);
  await waitFor(() => b.heard.length === 2);
  assert.deepStrictEqual(b.heard, [
    { name: 'leave', room: 'room', id: a.id },
    { name: 'join', room: 'room', id: reset.id, data: { name: 'ada' } },
  ]);
  // And the relay follows the new id.
  b.signaler.send(reset.id, description, { room: 'room' });
  await waitFor(() => a.heard.length === 2);
  assert.strictEqual(a.heard[1].from, b.id);
});

test('signaling: close() detaches the signaler and leaves the client alone', async (t) => {
  const { url } = await bootServer(t, { router: signalingRouter() });
  const a = await peer(t, url);
  const b = await peer(t, url);
  await a.signaler.join('room');
  await b.signaler.join('room');
  a.signaler.close();
  assert.strictEqual(a.signaler.id, null);
  assert.deepStrictEqual(a.signaler.rooms, new Set());
  await assert.rejects(a.signaler.ready(), /closed/);
  a.signaler.close(); // idempotent
  b.signaler.send(a.id, description, { room: 'room' });
  await timers.setTimeout(30);
  assert.deepStrictEqual(a.heard.slice(1), [], 'nothing reaches a detached signaler');
  assert.deepStrictEqual(await a.client.call('signaling/whoami'), { id: a.id }, 'the client is still open');
});

// ---------------------------------------------------------------------------
// Cross-instance: two RpcServers, one MemoryBackplane, fake sockets. The
// relay of a signal to a peer on another instance is RpcServer.sendTo's
// cluster leg; the roster is cluster.fetchClients; join/leave notifications
// ride Broadcast over the backplane.

let loopHold = null;
test.before(() => void (loopHold = setInterval(() => {}, 1000)));
test.after(() => clearInterval(loopHold));

const fakeSocket = () => {
  const listeners = new Map();
  return {
    packets: [],
    on(name, fn) {
      listeners.set(name, fn);
    },
    once(name, fn) {
      listeners.set(name, fn);
    },
    off() {},
    send(text) {
      this.packets.push(JSON.parse(text));
    },
    close() {},
    terminate() {},
    emit(name, ...args) {
      listeners.get(name)?.(...args);
    },
  };
};

const settle = async (turns = 8) => {
  for (let i = 0; i < turns; i++) await Promise.resolve();
};

test('signaling: relays across instances through the cluster', async (t) => {
  const backplane = new MemoryBackplane();
  t.after(() => backplane.close());
  const boot = (instanceId) => {
    const rpc = new RpcServer({ router: signalingRouter(), logger: quiet, backplane, instanceId });
    t.after(() => rpc.close());
    return rpc;
  };
  const a = boot('a');
  const b = boot('b');
  await settle();
  const attach = (rpc) => {
    const socket = fakeSocket();
    const client = rpc.attachSocket(socket, { headers: {} });
    let seq = 0;
    const call = async (method, args) => {
      const id = `${client.id}#${++seq}`;
      socket.emit('message', JSON.stringify({ type: 'call', id, method, args }), false);
      await waitFor(() => socket.packets.some((packet) => packet.id === id));
      const packet = socket.packets.find((entry) => entry.id === id);
      if (packet.error) throw Object.assign(new Error(packet.error.message), { code: packet.error.code });
      return packet.result;
    };
    const events = () => socket.packets.filter((packet) => packet.type === 'event');
    const send = (to, message) =>
      socket.emit(
        'message',
        JSON.stringify({ type: 'event', name: 'signaling/signal', data: { to, room: 'room', message } }),
        false,
      );
    return { socket, client, call, events, send };
  };
  const pa = attach(a);
  const pb = attach(b);

  assert.deepStrictEqual(await pa.call('signaling/join', { room: 'room', data: 'A' }), {
    id: pa.client.id,
    room: 'room',
    members: [],
  });
  await settle();
  const joined = await pb.call('signaling/join', { room: 'room', data: 'B' });
  assert.deepStrictEqual(joined.members, [{ id: pa.client.id, data: 'A' }], 'the roster spans instances');
  await waitFor(() => pa.events().length === 1);
  assert.deepStrictEqual(pa.events()[0], {
    type: 'event',
    name: 'signaling/join',
    data: { room: 'room', id: pb.client.id, data: 'B' },
  });

  pb.send(pa.client.id, description);
  await waitFor(() => pa.events().length === 2);
  assert.deepStrictEqual(pa.events()[1], {
    type: 'event',
    name: 'signaling/signal',
    data: { from: pb.client.id, room: 'room', message: description },
  });
  // The membership bound holds across instances too: after a leaves the
  // room, a signal addressed to it is not delivered.
  await pa.call('signaling/leave', { room: 'room' });
  await waitFor(() => pb.events().length === 1);
  assert.strictEqual(pb.events()[0].name, 'signaling/leave');
  pb.send(pa.client.id, { type: 'close' });
  await settle(20);
  assert.strictEqual(pa.events().length, 2);
});

test('signaling: the disconnect hook ignores foreign rooms and missing payloads', () => {
  const emitted = [];
  const client = {
    id: 'a.1',
    server: { to: (room) => ({ emit: (name, data) => emitted.push({ room, name, data }) }) },
  };
  const { onDisconnect } = createSignalingHooks();
  onDisconnect(client, null);
  onDisconnect(client, {});
  onDisconnect(client, { rooms: ['chat', 42, 'rtc:lobby'] });
  assert.deepStrictEqual(emitted, [{ room: 'rtc:lobby', name: 'signaling/leave', data: { room: 'lobby', id: 'a.1' } }]);
});
