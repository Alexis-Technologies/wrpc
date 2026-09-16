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
const { generateAssertionKeys } = require('../../src/webrtc/assertionIssuer.js');
const { createAssertionVerifier, parseJws } = require('../../src/webrtc/assertions.js');

const quiet = { log() {}, info() {}, warn() {}, error() {}, debug() {} };

const signalingRouter = (unit = {}, options = {}) =>
  defineRouter(
    { ...createSignalingUnit({ access: 'public', ...unit }) },
    { hooks: createSignalingHooks(unit), ...options },
  );

// A peer: one WS client and the signaler over it, with every inbound
// event recorded so a test can assert on what reached it.
const peer = async (t, { server, url }, options = {}) => {
  const { unit, identity, generateId, ...clientOptions } = options;
  const client = await connectClient(t, url, clientOptions);
  const signaler = wrpcSignaler(client, { unit, identity, generateId });
  const heard = [];
  for (const name of ['signal', 'join', 'leave', 'replaced']) {
    signaler.on(name, (payload) => heard.push({ name, ...payload }));
  }
  const id = await signaler.ready();
  return {
    client,
    signaler,
    heard,
    id,
    instance: signaler.instance,
    // The server-side connection owning this peer id (under the default
    // identity its client id IS the peer id), and its routable address.
    get connection() {
      return [...server.rpc.clients].find((entry) => entry.data.rtc?.id === id && !entry.data.rtc.revoked) ?? null;
    },
    get address() {
      return this.connection.id;
    },
  };
};

const description = { type: 'description', description: { type: 'offer', sdp: 'v=0' } };

// What a peer looks like in a roster / join / leave / signal payload.
const asMember = (p, data = null) => ({ id: p.id, instance: p.instance, address: p.address, data });
const signalFrom = (p, room, message) => ({
  name: 'signal',
  from: p.id,
  instance: p.instance,
  address: p.address,
  room,
  message,
});
const leaveOf = (p, room, reason, address = p.address) => ({
  name: 'leave',
  room,
  id: p.id,
  instance: p.instance,
  address,
  reason,
});

test('signaling: option validation', () => {
  assert.throws(() => createSignalingUnit({ name: 'a/b' }), /name must be a unit name/);
  assert.throws(() => createSignalingUnit({ name: '' }), /name must be a unit name/);
  assert.throws(() => createSignalingUnit({ access: 'admin' }), /access must be/);
  assert.throws(() => createSignalingUnit({ authorize: 'yes' }), /authorize must be a function/);
  assert.throws(() => createSignalingUnit({ relay: 'everyone' }), /relay must be/);
  assert.throws(() => createSignalingUnit({ prefix: 1 }), /prefix must be a string/);
  assert.throws(() => createSignalingUnit({ identity: 'alice' }), /identity must be a function/);
  assert.throws(() => createSignalingUnit({ duplicate: 'merge' }), /duplicate must be one of/);
  assert.throws(() => createSignalingHooks({ name: '' }), /name must be a string/);
  assert.throws(() => createSignalingHooks({ prefix: null }), /prefix must be a string/);
  assert.throws(() => createSignalingUnit({ assertions: 'yes' }), /assertions must be an object/);
  assert.throws(() => createSignalingUnit({ assertions: { key: {} } }), /key must be/);
  assert.throws(() => createSignalingUnit({ assertions: { key: {}, claims: 1 } }), /claims must be a function/);
  const unit = createSignalingUnit({ name: 'rtc' });
  assert.deepStrictEqual(Object.keys(unit), ['rtc']);
  assert.deepStrictEqual(Object.keys(unit.rtc).sort(), ['emits', 'join', 'leave', 'members', 'on', 'whoami']);
  assert.deepStrictEqual(Object.keys(createSignalingHooks()), ['onDisconnect']);
});

test('signaling: the client half satisfies the Signaler and RosterSignaler contracts', async (t) => {
  const boot = await bootServer(t, { router: signalingRouter() });
  const { signaler, id, client } = await peer(t, boot);
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
  const boot = await bootServer(t, { router: signalingRouter() });
  const { server } = boot;
  const { id } = await peer(t, boot);
  assert.ok(server.rpc.getClient(id), 'the id names a live server client');
});

test('signaling: join returns the roster and announces to the members already there', async (t) => {
  const boot = await bootServer(t, { router: signalingRouter() });
  const { server } = boot;
  const a = await peer(t, boot);
  const b = await peer(t, boot);

  assert.deepStrictEqual(await a.signaler.join('lobby', { name: 'ada' }), []);
  assert.deepStrictEqual(a.signaler.rooms, new Set(['lobby']));
  assert.ok(server.rpc.getClient(a.id).in('rtc:lobby'), 'rooms are namespaced under the prefix');

  const members = await b.signaler.join('lobby', { name: 'bob' });
  assert.deepStrictEqual(members, [asMember(a, { name: 'ada' })]);
  await waitFor(() => a.heard.length === 1);
  assert.deepStrictEqual(a.heard, [{ name: 'join', room: 'lobby', ...asMember(b, { name: 'bob' }) }]);
  assert.deepStrictEqual(b.heard, [], 'the joiner does not hear its own join');
  assert.strictEqual(b.signaler.addressOf(a.id), a.id, 'the roster taught b where a lives');

  // members() is the same roster on demand; a re-join updates the data
  // without a second announcement.
  assert.deepStrictEqual(await a.signaler.members('lobby'), [asMember(b, { name: 'bob' })]);
  await b.signaler.join('lobby', { name: 'bobby' });
  assert.deepStrictEqual(await a.signaler.members('lobby'), [asMember(b, { name: 'bobby' })]);
  await timers.setTimeout(20);
  assert.strictEqual(a.heard.length, 1);
  // Data is per room: joining a second room with none leaves the first as is.
  assert.deepStrictEqual(await b.signaler.join('other'), []);
  assert.deepStrictEqual(await a.signaler.members('lobby'), [asMember(b, { name: 'bobby' })]);
  assert.deepStrictEqual(await a.signaler.members('other'), [asMember(b)]);
  // A member the application put into the room itself, without join, has
  // no data of its own and is still listed — under its client id, since it
  // never identified.
  const c = await connectClient(t, boot.url);
  const raw = [...server.rpc.clients].find((entry) => entry.id !== a.id && entry.id !== b.id);
  raw.join('rtc:lobby');
  assert.deepStrictEqual(await a.signaler.members('lobby'), [
    asMember(b, { name: 'bobby' }),
    { id: raw.id, instance: null, address: raw.id, data: null },
  ]);
  assert.ok(c.active);
});

test('signaling: signals relay between members of a room, in both directions', async (t) => {
  const boot = await bootServer(t, { router: signalingRouter() });
  const a = await peer(t, boot);
  const b = await peer(t, boot);
  await a.signaler.join('room');
  await b.signaler.join('room');
  await waitFor(() => a.heard.length === 1);
  a.heard.length = 0;

  a.signaler.send(b.id, description, { room: 'room' });
  await waitFor(() => b.heard.length === 1);
  assert.deepStrictEqual(b.heard, [signalFrom(a, 'room', description)]);

  const candidate = { type: 'candidate', candidate: { candidate: 'a=1', sdpMid: '0' } };
  b.signaler.send(a.id, candidate, { room: 'room' });
  b.signaler.send(a.id, { type: 'close' }, { room: 'room' });
  await waitFor(() => a.heard.length === 2);
  assert.deepStrictEqual(a.heard[0], signalFrom(b, 'room', candidate));
  assert.deepStrictEqual(a.heard[1], signalFrom(b, 'room', { type: 'close' }));
});

test("signaling: relay 'room' bounds delivery to a shared membership", async (t) => {
  const boot = await bootServer(t, { router: signalingRouter() });
  const a = await peer(t, boot);
  const b = await peer(t, boot);
  const c = await peer(t, boot);
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
    [signalFrom(a, 'room', { type: 'close' })],
  );
  await timers.setTimeout(20);
  assert.deepStrictEqual(c.heard, []);
  assert.deepStrictEqual(
    a.heard.filter((entry) => entry.name === 'signal'),
    [],
  );
});

test("signaling: relay 'any' reaches any connected client by id", async (t) => {
  const boot = await bootServer(t, { router: signalingRouter({ relay: 'any' }) });
  const a = await peer(t, boot);
  const b = await peer(t, boot);
  a.signaler.send(b.id, description);
  a.client.sendEvent('signaling/signal', { to: b.id, room: null, message: { type: 'close' } });
  await waitFor(() => b.heard.length === 2);
  assert.deepStrictEqual(b.heard, [signalFrom(a, null, description), signalFrom(a, null, { type: 'close' })]);
  // An unknown id is not deliverable; nothing blows up. Nor does an address
  // hint pointing at a connection that is not the peer named.
  a.signaler.send('nobody.here', description);
  a.signaler.send('nobody.here', description, { address: b.id });
  await timers.setTimeout(20);
  assert.strictEqual(b.heard.length, 2);
});

test('signaling: leave and disconnect both announce to the remaining members', async (t) => {
  const boot = await bootServer(t, { router: signalingRouter() });
  const a = await peer(t, boot);
  const b = await peer(t, boot);
  const c = await peer(t, boot);
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
  assert.deepStrictEqual(a.heard, [leaveOf(b, 'room', 'left')]);
  // Leaving a room one is not in is honest, and announces nothing.
  await b.signaler.leave('room');
  assert.deepStrictEqual(await b.client.call('signaling/leave', { room: 'room' }), { room: 'room', left: false });

  // A dropped connection leaves every rtc room through the hooks — one
  // announcement per room, none for 'other' where a is not a member.
  a.heard.length = 0;
  c.client.close();
  await waitFor(() => a.heard.length === 1);
  assert.deepStrictEqual(a.heard, [leaveOf(c, 'room', 'disconnect', c.id)]);
  await timers.setTimeout(20);
  assert.deepStrictEqual(b.heard, [], 'b left before c dropped');
});

test('signaling: input validation answers 400', async (t) => {
  const boot = await bootServer(t, { router: signalingRouter() });
  const { client, signaler } = await peer(t, boot);
  for (const args of [{}, { room: '' }, { room: 'x'.repeat(257) }, null, 'lobby', { room: 7 }]) {
    await assert.rejects(client.call('signaling/join', args), (error) => error.code === 400);
    await assert.rejects(client.call('signaling/members', args), (error) => error.code === 400);
  }
  for (const args of [{ id: '' }, { id: 'x'.repeat(257) }, { instance: 7 }, 'me']) {
    await assert.rejects(client.call('signaling/whoami', args), (error) => error.code === 400);
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
  const boot = await bootServer(t, { router: signalingRouter({ authorize }) });
  const a = await peer(t, boot);
  const b = await peer(t, boot);
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
    [signalFrom(a, 'room', description)],
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
  const boot = await bootServer(t, { router });
  const client = await connectClient(t, boot.url);
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
  const boot = await bootServer(t, { router: signalingRouter(unit) });
  const { server } = boot;
  const a = await peer(t, boot, { unit: 'rtc' });
  const b = await peer(t, boot, { unit: 'rtc' });
  await a.signaler.join('room');
  assert.ok(server.rpc.getClient(a.id).in('p2p/room'));
  await b.signaler.join('room');
  await waitFor(() => a.heard.length === 1);
  b.signaler.send(a.id, description, { room: 'room' });
  await waitFor(() => a.heard.length === 2);
  assert.strictEqual(a.heard[1].message.type, 'description');
  b.client.close();
  await waitFor(() => a.heard.length === 3);
  assert.deepStrictEqual(a.heard[2], leaveOf(b, 'room', 'disconnect', b.id));
});

test('signaling: a reconnected signaling client gets a new id, re-joins and announces reset', async (t) => {
  const boot = await bootServer(t, { router: signalingRouter() });
  const { server } = boot;
  const a = await peer(t, boot, { reconnect: { minDelay: 10, maxDelay: 20, jitter: false } });
  a.client.on('error', () => {});
  const b = await peer(t, boot);
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
  assert.deepStrictEqual(reset.rooms, [{ room: 'room', members: [asMember(b, { name: 'bob' })] }]);
  await waitFor(() => b.heard.length === 2);
  assert.deepStrictEqual(b.heard, [
    leaveOf(a, 'room', 'disconnect', a.id),
    { name: 'join', room: 'room', id: reset.id, instance: a.instance, address: reset.id, data: { name: 'ada' } },
  ]);
  // And the relay follows the new id.
  b.signaler.send(reset.id, description, { room: 'room' });
  await waitFor(() => a.heard.length === 2);
  assert.strictEqual(a.heard[1].from, b.id);
});

test('signaling: close() detaches the signaler and leaves the client alone', async (t) => {
  const boot = await bootServer(t, { router: signalingRouter() });
  const a = await peer(t, boot);
  const b = await peer(t, boot);
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

test('signaling: the default identity is the connection, and a proposal is ignored', async (t) => {
  const boot = await bootServer(t, { router: signalingRouter() });
  const { server } = boot;
  const a = await peer(t, boot, { identity: 'alice' });
  assert.ok(server.rpc.getClient(a.id), 'the id is still the server client id');
  assert.deepStrictEqual(Object.keys(a.connection.data.rtc.rooms), []);
  assert.strictEqual(a.connection.data.rtc.instance, a.instance);
  // whoami is idempotent for the same connection; a second call with a
  // different proposal is a conflict, not a re-identification.
  assert.deepStrictEqual(await a.client.call('signaling/whoami'), { id: a.id });
  assert.deepStrictEqual(await a.client.call('signaling/whoami', { id: a.id }), { id: a.id });
  await assert.rejects(a.client.call('signaling/whoami', { id: 'someone' }), (error) => error.code === 409);
});

test('signaling: an identity strategy gives a stable id, kept across a reconnect, with the links addressed by it', async (t) => {
  const seen = [];
  const identity = (context, { proposed }) => {
    seen.push({ proposed, client: context.client.id });
    return proposed === null ? `anon:${context.client.id}` : `user:${proposed}`;
  };
  const boot = await bootServer(t, { router: signalingRouter({ identity }) });
  const { server } = boot;
  const a = await peer(t, boot, { identity: 'ada', reconnect: { minDelay: 10, maxDelay: 20, jitter: false } });
  a.client.on('error', () => {});
  const b = await peer(t, boot, { identity: () => 'bob' });
  const anon = await peer(t, boot);
  assert.strictEqual(a.id, 'user:ada');
  assert.strictEqual(b.id, 'user:bob');
  assert.ok(anon.id.startsWith('anon:'));
  assert.deepStrictEqual(seen[0], { proposed: 'ada', client: a.address });
  assert.strictEqual(server.rpc.getClient(a.id), undefined, 'a peer id is not a client id');

  await a.signaler.join('room', { name: 'ada' });
  assert.deepStrictEqual(await b.signaler.join('room'), [asMember(a, { name: 'ada' })]);
  await waitFor(() => a.heard.length === 1);
  // Relay by peer id, with the address the roster taught each side...
  b.signaler.send(a.id, description, { room: 'room' });
  await waitFor(() => a.heard.length === 2);
  assert.deepStrictEqual(a.heard[1], signalFrom(b, 'room', description));
  // ...and by name alone, when nothing was learned yet (a connect() by id).
  anon.signaler.send(a.id, { type: 'connect' }, { room: 'room' });
  await timers.setTimeout(20);
  assert.strictEqual(a.heard.length, 2, 'not a member: bounded');
  await anon.signaler.join('room');
  anon.signaler.send(a.id, { type: 'connect' }, { room: 'room' });
  await waitFor(() => a.heard.length === 4);
  assert.deepStrictEqual(a.heard[3], signalFrom(anon, 'room', { type: 'connect' }));

  // The server drops a's connection: b hears a 'disconnect' leave, a comes
  // back as user:ada with the same instance, re-joins, and b hears the join.
  b.heard.length = 0;
  const resets = [];
  a.signaler.on('reset', (payload) => resets.push(payload));
  const before = a.address;
  a.connection.close();
  await waitFor(() => resets.length === 1);
  assert.deepStrictEqual([resets[0].id, resets[0].previous], ['user:ada', 'user:ada']);
  assert.strictEqual(a.signaler.id, 'user:ada');
  assert.notStrictEqual(a.address, before, 'a new connection');
  await waitFor(() => b.heard.length === 2);
  assert.deepStrictEqual(b.heard, [
    leaveOf(a, 'room', 'disconnect', before),
    { name: 'join', room: 'room', id: a.id, instance: a.instance, address: a.address, data: { name: 'ada' } },
  ]);
  // b still held a's OLD address: the relay falls back to the registry and
  // the signal lands on the new connection.
  b.signaler.send(a.id, description, { room: 'room', address: before });
  await waitFor(() => a.heard.length === 5);
  assert.deepStrictEqual(a.heard[4], signalFrom(b, 'room', description));
});

test('signaling: a strategy that answers garbage is a 500, and a bounded id', async (t) => {
  const boot = await bootServer(t, { router: signalingRouter({ identity: () => 42 }) });
  const client = await connectClient(t, boot.url);
  const signaler = wrpcSignaler(client);
  await assert.rejects(signaler.ready(), (error) => error.code === 500);
  const long = await bootServer(t, { router: signalingRouter({ identity: () => 'x'.repeat(300) }) });
  const other = wrpcSignaler(await connectClient(t, long.url));
  await assert.rejects(other.ready(), (error) => error.code === 500);
});

test("signaling: duplicate 'refuse' answers the second connection 409", async (t) => {
  const identity = (_context, { proposed }) => proposed ?? 'anon';
  const boot = await bootServer(t, { router: signalingRouter({ identity, duplicate: 'refuse' }) });
  const first = await peer(t, boot, { identity: 'alice' });
  const client = await connectClient(t, boot.url);
  const second = wrpcSignaler(client, { identity: 'alice' });
  await assert.rejects(second.ready(), (error) => error.code === 409 && /already connected/.test(error.message));
  assert.strictEqual(second.id, null);
  assert.deepStrictEqual(first.heard, [], 'the first is untouched');
  // Once the first is gone the id is free again.
  first.client.close();
  await waitFor(async () => {
    try {
      return (await second.ready()) === 'alice';
    } catch {
      return false;
    }
  });
});

test("signaling: duplicate 'replace' hands the id to the newer connection", async (t) => {
  const identity = (_context, { proposed }) => proposed;
  const boot = await bootServer(t, { router: signalingRouter({ identity }) });
  const { server } = boot;
  const old = await peer(t, boot, { identity: 'alice' });
  const b = await peer(t, boot, { identity: 'bob' });
  await old.signaler.join('room', { tab: 1 });
  await old.signaler.join('other');
  await b.signaler.join('room');
  await waitFor(() => old.heard.length === 1);
  b.heard.length = 0;

  // A second tab: the first is told, its rooms hear a 'replaced' leave,
  // and its later calls are refused — it acts under nobody's id now.
  const oldAddress = old.address;
  const fresh = await peer(t, boot, { identity: 'alice' });
  await waitFor(() => old.heard.length === 2);
  assert.deepStrictEqual(old.heard[1], { name: 'replaced', id: 'alice' });
  assert.strictEqual(old.signaler.replaced, true);
  assert.strictEqual(old.signaler.id, null);
  await waitFor(() => b.heard.length === 1);
  assert.deepStrictEqual(b.heard, [leaveOf({ ...old, id: 'alice' }, 'room', 'replaced', oldAddress)]);
  assert.strictEqual(server.rpc.getClient(oldAddress).data.rtc.revoked, true, 'the old connection owns no peer id');
  assert.strictEqual(fresh.address !== oldAddress, true);
  await assert.rejects(old.client.call('signaling/join', { room: 'room' }), (error) => error.code === 409);
  await assert.rejects(old.client.call('signaling/whoami'), (error) => error.code === 409);
  old.client.sendEvent('signaling/signal', { to: 'bob', room: 'room', message: description });
  await timers.setTimeout(20);
  assert.deepStrictEqual(b.heard.length, 1, 'nothing relayed from the replaced connection');

  // The newcomer joins and is addressed as alice; the old connection's
  // close announces nothing — alice is still there.
  assert.deepStrictEqual(await fresh.signaler.join('room', { tab: 2 }), [asMember(b)]);
  await waitFor(() => b.heard.length === 2);
  assert.deepStrictEqual(b.heard[1], { name: 'join', room: 'room', ...asMember(fresh, { tab: 2 }) });
  b.signaler.send('alice', description, { room: 'room' });
  await waitFor(() => fresh.heard.length === 1);
  assert.deepStrictEqual(fresh.heard[0], signalFrom(b, 'room', description));
  old.client.close();
  await timers.setTimeout(30);
  assert.strictEqual(b.heard.length, 2, 'no leave for a revoked connection');
});

test('signaling: a takeover by the same incarnation is silent', async (t) => {
  const identity = (_context, { proposed }) => proposed;
  const boot = await bootServer(t, { router: signalingRouter({ identity }) });
  const generateId = () => 'tab-1';
  const old = await peer(t, boot, { identity: 'alice', generateId });
  const b = await peer(t, boot, { identity: 'bob' });
  await old.signaler.join('room');
  await b.signaler.join('room');
  await waitFor(() => old.heard.length === 1);
  b.heard.length = 0;
  // The same tab reconnecting before the server noticed the old socket die:
  // no 'replaced' leave — the id never left; the re-join is a fresh join
  // on the new connection, which is announced as usual.
  const fresh = await peer(t, boot, { identity: 'alice', generateId });
  await waitFor(() => old.heard.length === 2);
  assert.strictEqual(old.heard[1].name, 'replaced');
  await fresh.signaler.join('room');
  await waitFor(() => b.heard.length === 1);
  assert.deepStrictEqual(b.heard, [{ name: 'join', room: 'room', ...asMember(fresh) }]);
});

test('signaling: ownership is re-checked after every await', async (t) => {
  const identity = (_context, { proposed }) => proposed;
  let release = null;
  const authorize = (context) =>
    context.client.data.rtc?.id === 'alice' && release === null
      ? new Promise((resolve) => void (release = () => resolve(true)))
      : true;
  const boot = await bootServer(t, { router: signalingRouter({ identity, authorize }) });
  const old = await peer(t, boot, { identity: 'alice' });
  const b = await peer(t, boot, { identity: 'bob' });
  await b.signaler.join('room');
  // The old connection's join is held in authorize while a newer
  // connection takes the id: when it resumes it is refused, and b never
  // hears a join from it.
  const held = old.client.call('signaling/join', { room: 'room' });
  await waitFor(() => release !== null);
  const fresh = await peer(t, boot, { identity: 'alice' });
  release();
  await assert.rejects(held, (error) => error.code === 409 && /replaced/.test(error.message));
  await timers.setTimeout(20);
  assert.deepStrictEqual(b.heard, []);
  assert.strictEqual(fresh.id, 'alice');
});

test('signaling: a stale address hint is not followed onto a stranger', async (t) => {
  const identity = (_context, { proposed }) => proposed;
  const boot = await bootServer(t, { router: signalingRouter({ identity, relay: 'any' }) });
  const a = await peer(t, boot, { identity: 'alice' });
  const b = await peer(t, boot, { identity: 'bob' });
  const c = await peer(t, boot, { identity: 'carol' });
  // a addresses bob with carol's connection as the hint: the registry says
  // bob lives elsewhere, so the hint is ignored and bob gets it.
  a.signaler.send('bob', description, { address: c.address });
  await waitFor(() => b.heard.length === 1);
  assert.deepStrictEqual(c.heard, []);
});

test('signaling: assertions bind the stable id to a certificate, with the claims the server adds', async (t) => {
  const keys = await generateAssertionKeys({ kid: 'k1' });
  const identity = (_context, { proposed }) => proposed;
  const seen = [];
  const claims = (context) => {
    seen.push(context.client.data.rtc.id);
    return { role: 'member', sub: 'forged', exp: 1 };
  };
  const unit = { identity, assertions: { key: keys.privateKey, ttl: 90, issuer: 'sig.test', claims } };
  const boot = await bootServer(t, { router: signalingRouter(unit) });
  const a = await peer(t, boot, { identity: 'alice' });
  const fp = 'sha-256 ' + 'AB:'.repeat(31) + 'CD';
  const answer = await a.signaler.assert({ fingerprint: fp.toLowerCase() });
  assert.strictEqual(typeof answer.assertion, 'string');
  assert.strictEqual(answer.exp, answer.iat + 90);
  const { header, payload } = parseJws(answer.assertion);
  assert.deepStrictEqual(header, { alg: 'ES256', typ: 'wrpc-rtc+jwt', kid: 'k1' });
  assert.deepStrictEqual(payload, {
    role: 'member',
    sub: 'alice',
    iat: answer.iat,
    exp: answer.exp,
    fp,
    iss: 'sig.test',
  });
  assert.deepStrictEqual(seen, ['alice'], 'claims() ran with the identified context');
  // keys() is public: a fresh, unidentified connection reads them.
  const stranger = await connectClient(t, boot.url);
  const published = await wrpcSignaler(stranger).keys();
  assert.deepStrictEqual(published, [keys.publicKey]);
  const verifier = createAssertionVerifier({ keys: published, issuer: 'sig.test' });
  const sdp = `v=0\r\na=fingerprint:${fp}\r\n`;
  assert.deepStrictEqual(await verifier.verify(answer.assertion, { from: 'alice', sdp }), payload);
  // Input is validated; a replaced connection is refused.
  for (const args of [{}, { fingerprint: 'nope' }, { fingerprint: 7 }, null]) {
    await assert.rejects(a.client.call('signaling/assert', args), (error) => error.code === 400);
  }
  await assert.rejects(a.signaler.assert({}), /fingerprint must be/);
  await peer(t, boot, { identity: 'alice' });
  await waitFor(() => a.signaler.replaced);
  await assert.rejects(a.client.call('signaling/assert', { fingerprint: fp }), (error) => error.code === 409);
});

test('signaling: a claims hook that answers garbage is a 500; no assertions means no assert/keys', async (t) => {
  const keys = await generateAssertionKeys();
  const broken = await bootServer(t, {
    router: signalingRouter({ assertions: { key: keys.privateKey, claims: () => 'nope' } }),
  });
  const a = await peer(t, broken);
  await assert.rejects(a.signaler.assert({ fingerprint: 'sha-256 AA:BB' }), (error) => error.code === 500);
  const plain = await bootServer(t, { router: signalingRouter() });
  const b = await peer(t, plain);
  await assert.rejects(b.signaler.assert({ fingerprint: 'sha-256 AA:BB' }), (error) => error.code === 404);
  await assert.rejects(b.signaler.keys(), (error) => error.code === 404);
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
  const member = (p, data) => ({ id: p.client.id, instance: null, address: p.client.id, data });
  assert.deepStrictEqual(joined.members, [member(pa, 'A')], 'the roster spans instances');
  await waitFor(() => pa.events().length === 1);
  assert.deepStrictEqual(pa.events()[0], {
    type: 'event',
    name: 'signaling/join',
    data: { room: 'room', ...member(pb, 'B') },
  });

  // No address hint from this bare socket: the relay resolves the peer id
  // through the room's descriptors across the cluster.
  pb.send(pa.client.id, description);
  await waitFor(() => pa.events().length === 2);
  assert.deepStrictEqual(pa.events()[1], {
    type: 'event',
    name: 'signaling/signal',
    data: {
      from: pb.client.id,
      instance: null,
      address: pb.client.id,
      to: pa.client.id,
      toInstance: null,
      room: 'room',
      message: description,
    },
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
  assert.deepStrictEqual(emitted, [
    {
      room: 'rtc:lobby',
      name: 'signaling/leave',
      data: { room: 'lobby', id: 'a.1', instance: null, address: 'a.1', reason: 'disconnect' },
    },
  ]);
  // An identified client announces its peer id; a revoked one nothing.
  emitted.length = 0;
  client.data = { rtc: { id: 'alice', instance: 'i1', rooms: {} } };
  onDisconnect(client, { rooms: ['rtc:lobby'] });
  assert.deepStrictEqual(emitted[0].data, {
    room: 'lobby',
    id: 'alice',
    instance: 'i1',
    address: 'a.1',
    reason: 'disconnect',
  });
  emitted.length = 0;
  client.data.rtc.revoked = true;
  onDisconnect(client, { rooms: ['rtc:lobby'] });
  assert.deepStrictEqual(emitted, []);
});
