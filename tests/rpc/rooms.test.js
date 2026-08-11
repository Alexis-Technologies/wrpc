'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { RoomRegistry, Broadcast } = require('../../src/rpc/rooms.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

// Rooms only ever touch `persistent` and `sendEvent` on a client, which is
// what makes the registry testable without a socket underneath.
const fakeClient = (name, { persistent = true, broken = false } = {}) => ({
  name,
  persistent,
  received: [],
  sendEvent(event, data) {
    if (broken) throw new Error(`socket ${name} is gone`);
    this.received.push([event, data]);
  },
});

const target = (registry, clients, options = {}) =>
  new Broadcast({ registry, clients: () => clients, log: quiet, ...options });

test('RoomRegistry: join, leave and the reverse index', async (t) => {
  const registry = new RoomRegistry();
  const ada = fakeClient('ada');
  const grace = fakeClient('grace');

  await t.test('join reports whether the membership is new', () => {
    assert.strictEqual(registry.join(ada, 'chat'), true);
    assert.strictEqual(registry.join(ada, 'chat'), false, 'joining twice is a no-op');
    assert.strictEqual(registry.count('chat'), 1);
  });

  await t.test('both directions stay in sync', () => {
    registry.join(ada, 'lobby');
    registry.join(grace, 'chat');
    assert.deepStrictEqual(registry.list().sort(), ['chat', 'lobby']);
    assert.deepStrictEqual([...registry.roomsOf(ada)].sort(), ['chat', 'lobby']);
    assert.strictEqual(registry.members('chat').has(grace), true);
    assert.strictEqual(registry.size, 2);
  });

  await t.test('leave removes the room once it empties', () => {
    assert.strictEqual(registry.leave(ada, 'lobby'), true);
    assert.strictEqual(registry.has('lobby'), false, 'an empty room is dropped');
    assert.strictEqual(registry.leave(ada, 'lobby'), false, 'leaving twice is a no-op');
    assert.strictEqual(registry.leave(ada, 'never-joined'), false);
  });

  await t.test('leaveAll clears every membership of one client', () => {
    registry.join(ada, 'a');
    registry.join(ada, 'b');
    registry.leaveAll(ada);
    assert.strictEqual(registry.roomsOf(ada).size, 0);
    assert.strictEqual(registry.members('chat').has(ada), false);
    assert.strictEqual(registry.members('chat').has(grace), true, 'other members survive');
  });

  await t.test('a room name must be a non-empty string', () => {
    assert.throws(() => registry.join(ada, ''), TypeError);
    assert.throws(() => registry.join(ada, 42), TypeError);
  });
});

test('RoomRegistry: subscribe callbacks fire on the first and last member', () => {
  const subscribed = [];
  const unsubscribed = [];
  const registry = new RoomRegistry({
    onSubscribe: (room) => subscribed.push(room),
    onUnsubscribe: (room) => unsubscribed.push(room),
  });
  const ada = fakeClient('ada');
  const grace = fakeClient('grace');

  registry.join(ada, 'chat');
  registry.join(grace, 'chat');
  assert.deepStrictEqual(subscribed, ['chat'], 'only the first member subscribes');

  registry.leave(ada, 'chat');
  assert.deepStrictEqual(unsubscribed, [], 'a room with members stays subscribed');
  registry.leave(grace, 'chat');
  assert.deepStrictEqual(unsubscribed, ['chat']);

  registry.join(ada, 'chat');
  assert.deepStrictEqual(subscribed, ['chat', 'chat'], 'a re-created room subscribes again');
  registry.clear();
  assert.deepStrictEqual(unsubscribed, ['chat', 'chat']);
  assert.strictEqual(registry.size, 0);
});

test('Broadcast: targeting', async (t) => {
  const registry = new RoomRegistry();
  const ada = fakeClient('ada');
  const grace = fakeClient('grace');
  const linus = fakeClient('linus');
  const clients = new Set([ada, grace, linus]);
  registry.join(ada, 'chat');
  registry.join(grace, 'chat');
  registry.join(grace, 'lobby');
  registry.join(linus, 'lobby');

  await t.test('to(room) reaches exactly its members', () => {
    const sent = target(registry, clients).to('chat').emit('msg', { n: 1 });
    assert.strictEqual(sent, 2);
    assert.deepStrictEqual(ada.received, [['msg', { n: 1 }]]);
    assert.deepStrictEqual(linus.received, []);
  });

  await t.test('to() over several rooms is a union, each client once', () => {
    for (const client of clients) client.received.length = 0;
    const sent = target(registry, clients).to('chat', 'lobby').emit('msg', 2);
    assert.strictEqual(sent, 3);
    assert.strictEqual(grace.received.length, 1, 'a member of both rooms gets one copy');
  });

  await t.test('except() drops a client from the fan-out', () => {
    for (const client of clients) client.received.length = 0;
    const sent = target(registry, clients).to('chat').except(ada).emit('msg', 3);
    assert.strictEqual(sent, 1);
    assert.deepStrictEqual(ada.received, []);
    assert.deepStrictEqual(grace.received, [['msg', 3]]);
  });

  await t.test('with no room the target is every client', () => {
    for (const client of clients) client.received.length = 0;
    assert.strictEqual(target(registry, clients).emit('all', 4), 3);
    assert.strictEqual(target(registry, clients).except(linus).emit('all', 5), 2);
  });

  await t.test('chaining is immutable: a stored target is not narrowed later', () => {
    const chat = target(registry, clients).to('chat');
    const narrowed = chat.except(ada);
    assert.notStrictEqual(chat, narrowed);
    assert.deepStrictEqual(chat.rooms, ['chat']);
    assert.strictEqual(target(registry, clients).rooms, null);
    for (const client of clients) client.received.length = 0;
    assert.strictEqual(chat.emit('msg', 6), 2, 'the original still reaches both');
  });

  await t.test('an empty room delivers to nobody', () => {
    assert.strictEqual(target(registry, clients).to('nowhere').emit('msg', 7), 0);
  });

  await t.test('narrowing to no rooms reaches nobody, not everybody', () => {
    const published = [];
    const rooms = []; // e.g. a computed list that came back empty
    const empty = target(registry, clients, { publish: (e) => published.push(e) }).to(...rooms);
    assert.deepStrictEqual(empty.rooms, []);
    assert.strictEqual(empty.emit('msg', 8), 0, 'an empty target must not fall back to everyone');
    assert.deepStrictEqual(published, [], 'and there is nothing to publish either');
  });

  await t.test('the event name must be a non-empty string', () => {
    assert.throws(() => target(registry, clients).emit(''), TypeError);
    assert.throws(() => target(registry, clients).emit(null), TypeError);
  });
});

test('Broadcast: non-persistent and broken clients', () => {
  const registry = new RoomRegistry();
  const http = fakeClient('http', { persistent: false });
  const dead = fakeClient('dead', { broken: true });
  const live = fakeClient('live');
  const clients = new Set([http, dead, live]);
  for (const client of clients) registry.join(client, 'chat');

  const errors = [];
  const log = { ...quiet, error: (error) => errors.push(error) };
  const sent = new Broadcast({ registry, clients: () => clients, log }).to('chat').emit('msg', 1);

  assert.strictEqual(sent, 1, 'only the live persistent client counts');
  assert.deepStrictEqual(http.received, [], 'an http client cannot carry events');
  assert.deepStrictEqual(live.received, [['msg', 1]], 'a throwing peer does not truncate the fan-out');
  assert.strictEqual(errors.length, 1);
});

test('Broadcast: publishing to the backplane', async (t) => {
  const registry = new RoomRegistry();
  const ada = fakeClient('ada');
  const clients = new Set([ada]);
  registry.join(ada, 'chat');

  const published = [];
  const publish = (envelope) => published.push(envelope);

  await t.test('a normal emit publishes alongside local delivery', () => {
    target(registry, clients, { publish }).to('chat').emit('msg', { n: 1 });
    assert.deepStrictEqual(published, [{ rooms: ['chat'], name: 'msg', data: { n: 1 } }]);
  });

  await t.test('a broadcast publishes with a null room list', () => {
    published.length = 0;
    target(registry, clients, { publish }).emit('all', 2);
    assert.deepStrictEqual(published, [{ rooms: null, name: 'all', data: 2 }]);
  });

  await t.test('local() delivers without publishing', () => {
    published.length = 0;
    ada.received.length = 0;
    const sent = target(registry, clients, { publish }).to('chat').local().emit('msg', 3);
    assert.strictEqual(sent, 1);
    assert.deepStrictEqual(published, [], 'a replayed event must not be republished');
  });
});
