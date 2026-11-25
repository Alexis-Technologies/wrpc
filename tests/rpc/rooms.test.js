'use strict';

const { describe, it, before } = require('node:test');
const assert = require('node:assert/strict');

const { RoomRegistry, Broadcast } = require('../../src/rpc/rooms.js');

const noop = () => {};
const quiet = { log: noop, info: noop, warn: noop, error: noop, debug: noop };

// Rooms only ever touch `persistent`, `sendShared` (emit: one shared
// `{ text, frames, compress }` message for the whole fan-out) and `sendRaw`
// (ask: a per-recipient text) on a client, which is what makes the registry
// testable without a socket underneath.
const fakeClient = (name, { persistent = true, broken = false } = {}) => ({
  name,
  persistent,
  received: [],
  sendRaw(text) {
    if (broken) throw new Error(`socket ${name} is gone`);
    const packet = JSON.parse(text);
    this.received.push([packet.name, packet.data]);
    return true;
  },
  sendShared(message) {
    this.shared = message;
    return this.sendRaw(message.text);
  },
});

const target = (registry, clients, options = {}) =>
  new Broadcast({ registry, clients: () => clients, log: quiet, ...options });

describe('RoomRegistry', () => {
  describe('join, leave and the reverse index', () => {
    const registry = new RoomRegistry();
    const ada = fakeClient('ada');
    const grace = fakeClient('grace');

    it('should report from join whether the membership is new', () => {
      assert.strictEqual(registry.join(ada, 'chat'), true);
      assert.strictEqual(registry.join(ada, 'chat'), false, 'joining twice is a no-op');
      assert.strictEqual(registry.count('chat'), 1);
    });

    it('should keep both directions in sync', () => {
      registry.join(ada, 'lobby');
      registry.join(grace, 'chat');
      assert.deepStrictEqual(registry.list().sort(), ['chat', 'lobby']);
      assert.deepStrictEqual([...registry.roomsOf(ada)].sort(), ['chat', 'lobby']);
      assert.strictEqual(registry.members('chat').has(grace), true);
      assert.strictEqual(registry.size, 2);
    });

    it('should remove the room on leave once it empties', () => {
      assert.strictEqual(registry.leave(ada, 'lobby'), true);
      assert.strictEqual(registry.has('lobby'), false, 'an empty room is dropped');
      assert.strictEqual(registry.leave(ada, 'lobby'), false, 'leaving twice is a no-op');
      assert.strictEqual(registry.leave(ada, 'never-joined'), false);
    });

    it('should clear every membership of one client on leaveAll', () => {
      registry.join(ada, 'a');
      registry.join(ada, 'b');
      registry.leaveAll(ada);
      assert.strictEqual(registry.roomsOf(ada).size, 0);
      assert.strictEqual(registry.members('chat').has(ada), false);
      assert.strictEqual(registry.members('chat').has(grace), true, 'other members survive');
    });

    it('should require a room name to be a non-empty string', () => {
      assert.throws(() => registry.join(ada, ''), TypeError);
      assert.throws(() => registry.join(ada, 42), TypeError);
    });
  });

  it('should fire the subscribe callbacks on the first and last member', () => {
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
});

describe('Broadcast', () => {
  describe('targeting', () => {
    const registry = new RoomRegistry();
    const ada = fakeClient('ada');
    const grace = fakeClient('grace');
    const linus = fakeClient('linus');
    const clients = new Set([ada, grace, linus]);
    before(() => {
      registry.join(ada, 'chat');
      registry.join(grace, 'chat');
      registry.join(grace, 'lobby');
      registry.join(linus, 'lobby');
    });

    it('should reach exactly the members of to(room)', () => {
      const sent = target(registry, clients).to('chat').emit('msg', { n: 1 });
      assert.strictEqual(sent, 2);
      assert.deepStrictEqual(ada.received, [['msg', { n: 1 }]]);
      assert.deepStrictEqual(linus.received, []);
    });

    it('should make to() over several rooms a union, each client once', () => {
      for (const client of clients) client.received.length = 0;
      const sent = target(registry, clients).to('chat', 'lobby').emit('msg', 2);
      assert.strictEqual(sent, 3);
      assert.strictEqual(grace.received.length, 1, 'a member of both rooms gets one copy');
    });

    it('should drop a client from the fan-out with except()', () => {
      for (const client of clients) client.received.length = 0;
      const sent = target(registry, clients).to('chat').except(ada).emit('msg', 3);
      assert.strictEqual(sent, 1);
      assert.deepStrictEqual(ada.received, []);
      assert.deepStrictEqual(grace.received, [['msg', 3]]);
    });

    it('should target every client with no room', () => {
      for (const client of clients) client.received.length = 0;
      assert.strictEqual(target(registry, clients).emit('all', 4), 3);
      assert.strictEqual(target(registry, clients).except(linus).emit('all', 5), 2);
    });

    it('should chain immutably: a stored target is not narrowed later', () => {
      const chat = target(registry, clients).to('chat');
      const narrowed = chat.except(ada);
      assert.notStrictEqual(chat, narrowed);
      assert.deepStrictEqual(chat.rooms, ['chat']);
      assert.strictEqual(target(registry, clients).rooms, null);
      for (const client of clients) client.received.length = 0;
      assert.strictEqual(chat.emit('msg', 6), 2, 'the original still reaches both');
    });

    it('should deliver to nobody for an empty room', () => {
      assert.strictEqual(target(registry, clients).to('nowhere').emit('msg', 7), 0);
    });

    it('should reach nobody when narrowed to no rooms, not everybody', () => {
      const published = [];
      const rooms = []; // e.g. a computed list that came back empty
      const empty = target(registry, clients, { publish: (e) => published.push(e) }).to(...rooms);
      assert.deepStrictEqual(empty.rooms, []);
      assert.strictEqual(empty.emit('msg', 8), 0, 'an empty target must not fall back to everyone');
      assert.deepStrictEqual(published, [], 'and there is nothing to publish either');
    });

    it('should require the event name to be a non-empty string', () => {
      assert.throws(() => target(registry, clients).emit(''), TypeError);
      assert.throws(() => target(registry, clients).emit(null), TypeError);
    });
  });

  it('should count only the live persistent client among non-persistent and broken clients', () => {
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

  describe('publishing to the backplane', () => {
    const registry = new RoomRegistry();
    const ada = fakeClient('ada');
    const clients = new Set([ada]);
    before(() => registry.join(ada, 'chat'));

    const published = [];
    const publish = (envelope) => published.push(envelope);

    it('should publish a normal emit alongside local delivery', () => {
      target(registry, clients, { publish }).to('chat').emit('msg', { n: 1 });
      assert.deepStrictEqual(published, [{ rooms: ['chat'], name: 'msg', data: { n: 1 } }]);
    });

    it('should publish a broadcast with a null room list', () => {
      published.length = 0;
      target(registry, clients, { publish }).emit('all', 2);
      assert.deepStrictEqual(published, [{ rooms: null, name: 'all', data: 2 }]);
    });

    it('should deliver without publishing with local()', () => {
      published.length = 0;
      ada.received.length = 0;
      const sent = target(registry, clients, { publish }).to('chat').local().emit('msg', 3);
      assert.strictEqual(sent, 1);
      assert.deepStrictEqual(published, [], 'a replayed event must not be republished');
    });
  });
});

describe('Broadcast.emit', () => {
  describe('one shared message object reaches every recipient', () => {
    const registry = new RoomRegistry();
    const ada = fakeClient('ada');
    const grace = fakeClient('grace');
    before(() => {
      registry.join(ada, 'chat');
      registry.join(grace, 'chat');
    });

    it('should share the same object, serialized once, with the compress flag on by default', () => {
      target(registry, [ada, grace]).to('chat').emit('hello', { n: 1 });
      assert.strictEqual(ada.shared, grace.shared, 'recipients share ONE message');
      assert.strictEqual(ada.shared.frames, null, "the slot is the engine's to claim");
      assert.strictEqual(ada.shared.compress, true);
      assert.deepStrictEqual(JSON.parse(ada.shared.text), { type: 'event', name: 'hello', data: { n: 1 } });
    });

    it('should carry compress:false on the shared message', () => {
      target(registry, [ada, grace]).to('chat').emit('bulk', 'x'.repeat(4096), { compress: false });
      assert.strictEqual(ada.shared.compress, false);
      assert.strictEqual(grace.shared.compress, false);
    });

    it('should show the second recipient the slot the first one claimed', () => {
      const claimer = fakeClient('claimer');
      claimer.sendShared = function (message) {
        message.frames = 'claimed';
        this.shared = message;
        return this.sendRaw(message.text);
      };
      registry.join(claimer, 'shared');
      registry.join(grace, 'shared');
      target(registry, [claimer, grace]).to('shared').emit('once', null);
      assert.strictEqual(grace.shared.frames, 'claimed');
    });
  });
});
