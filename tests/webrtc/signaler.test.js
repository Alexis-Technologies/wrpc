'use strict';

// wrpcSignaler over a fake WrpcClient: the contract, the packets it sends,
// what it accepts inbound, and the reconnect → reset cycle — all without a
// server, so each branch is reached directly.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');
const { waitFor } = require('../helpers/server.js');

const { Emitter } = require('../../src/utils.js');
const {
  wrpcSignaler,
  WrpcSignaler,
  isSignaler,
  hasRoster,
  isSignalMessage,
  SIGNAL_MESSAGE_TYPES,
} = require('../../src/webrtc/browser.js');

// The slice of WrpcClient a signaler touches: use() scaffolds api[unit] as
// an Emitter, call() answers from a programmable table, sendEvent() records.
class FakeClient extends Emitter {
  api = {};
  calls = [];
  events = [];
  answers = new Map();
  use(introspection) {
    for (const unit of Object.keys(introspection)) this.api[unit] ??= new Emitter();
    return this;
  }
  async call(method, args) {
    this.calls.push({ method, args });
    const answer = this.answers.get(method);
    if (typeof answer === 'function') return answer(args);
    if (answer instanceof Error) throw answer;
    return answer;
  }
  sendEvent(name, data) {
    this.events.push({ name, data });
  }
}

const description = { type: 'description', description: { type: 'answer', sdp: 'v=0' } };

test('signaler: the structural checks', () => {
  assert.strictEqual(isSignaler(null), false);
  assert.strictEqual(isSignaler({ id: null, ready() {}, send() {}, on() {} }), false);
  const bare = { id: null, ready() {}, send() {}, on() {}, off() {} };
  assert.strictEqual(isSignaler(bare), true);
  assert.strictEqual(hasRoster(bare), false);
  assert.strictEqual(hasRoster({ ...bare, join() {}, leave() {} }), true);
  assert.deepStrictEqual([...SIGNAL_MESSAGE_TYPES], ['description', 'candidate', 'close', 'connect']);
  assert.ok(Object.isFrozen(SIGNAL_MESSAGE_TYPES));
  assert.strictEqual(isSignalMessage({ type: 'candidate' }), true);
  assert.strictEqual(isSignalMessage({ type: 'offer' }), false);
  assert.strictEqual(isSignalMessage('close'), false);
});

test('signaler: construction validates the client and the unit name', () => {
  assert.throws(() => wrpcSignaler({}), /must be a WrpcClient/);
  assert.throws(() => wrpcSignaler(null), /must be a WrpcClient/);
  assert.throws(() => wrpcSignaler(new FakeClient(), { unit: '' }), /unit must be a unit name/);
  const client = new FakeClient();
  const signaler = wrpcSignaler(client, { unit: 'rtc' });
  assert.ok(signaler instanceof WrpcSignaler);
  assert.ok(client.api.rtc instanceof Emitter, 'the unit is scaffolded statically');
  assert.strictEqual(signaler.unit, 'rtc');
  assert.strictEqual(signaler.id, null);
  assert.strictEqual(client.calls.length, 0, 'no wire traffic at construction');
});

test('signaler: ready() is single-flight, validates the answer and retries after a failure', async () => {
  const client = new FakeClient();
  let asked = 0;
  client.answers.set('signaling/whoami', () => {
    asked++;
    return asked === 1 ? { nope: true } : { id: 'i.1' };
  });
  const signaler = wrpcSignaler(client);
  await assert.rejects(signaler.ready(), /answered without an id/);
  assert.strictEqual(signaler.id, null);
  const [first, second] = await Promise.all([signaler.ready(), signaler.ready()]);
  assert.strictEqual(first, 'i.1');
  assert.strictEqual(second, 'i.1');
  assert.strictEqual(asked, 2, 'concurrent ready() calls share one whoami');
  assert.strictEqual(signaler.id, 'i.1');
});

test('signaler: send() writes the signal event; join/leave/members are calls', async () => {
  const client = new FakeClient();
  client.answers.set('signaling/whoami', { id: 'i.1' });
  client.answers.set('signaling/join', { id: 'i.1', room: 'r', members: [{ id: 'i.2', data: null }] });
  client.answers.set('signaling/leave', { room: 'r', left: true });
  client.answers.set('signaling/members', [{ id: 'i.2', data: 'x' }]);
  const signaler = wrpcSignaler(client);
  signaler.send('i.2', description, { room: 'r' });
  signaler.send('i.2', { type: 'close' });
  assert.deepStrictEqual(client.events, [
    { name: 'signaling/signal', data: { to: 'i.2', room: 'r', message: description } },
    { name: 'signaling/signal', data: { to: 'i.2', room: null, message: { type: 'close' } } },
  ]);
  assert.deepStrictEqual(await signaler.join('r', { name: 'me' }), [{ id: 'i.2', data: null }]);
  assert.deepStrictEqual(signaler.rooms, new Set(['r']));
  assert.deepStrictEqual(await signaler.members('r'), [{ id: 'i.2', data: 'x' }]);
  await signaler.leave('r');
  assert.deepStrictEqual(signaler.rooms, new Set());
  assert.deepStrictEqual(
    client.calls.map((entry) => entry.method),
    ['signaling/whoami', 'signaling/join', 'signaling/members', 'signaling/leave'],
  );
  assert.deepStrictEqual(client.calls[1].args, { room: 'r', data: { name: 'me' } });
  // A malformed answer degrades to an empty roster rather than a crash.
  client.answers.set('signaling/join', { id: 'i.1' });
  client.answers.set('signaling/members', null);
  assert.deepStrictEqual(await signaler.join('q'), []);
  assert.deepStrictEqual(await signaler.members('q'), []);
});

test('signaler: inbound events are shape-checked before they are re-emitted', async () => {
  const client = new FakeClient();
  const signaler = wrpcSignaler(client);
  const heard = [];
  for (const name of ['signal', 'join', 'leave']) signaler.on(name, (payload) => heard.push({ name, ...payload }));
  const api = client.api.signaling;
  await api.emit('signal', { from: 'i.2', room: 'r', message: description });
  await api.emit('signal', { from: 'i.2', message: { type: 'close' } });
  await api.emit('signal', { from: 7, room: 'r', message: description });
  await api.emit('signal', { from: 'i.2', room: 'r', message: { type: 'hello' } });
  await api.emit('signal', 'text');
  await api.emit('join', { room: 'r', id: 'i.3', data: 1 });
  await api.emit('join', null);
  await api.emit('leave', { room: 'r', id: 'i.3' });
  await api.emit('leave', 'i.3');
  assert.deepStrictEqual(heard, [
    { name: 'signal', from: 'i.2', room: 'r', message: description },
    { name: 'signal', from: 'i.2', room: null, message: { type: 'close' } },
    { name: 'join', room: 'r', id: 'i.3', data: 1 },
    { name: 'leave', room: 'r', id: 'i.3' },
  ]);
});

test('signaler: reconnect re-identifies, re-joins every room and emits reset', async () => {
  const client = new FakeClient();
  let ids = 0;
  client.answers.set('signaling/whoami', () => ({ id: `i.${++ids}` }));
  client.answers.set('signaling/join', ({ room }) => ({ id: 'x', room, members: [{ id: 'other', data: room }] }));
  const signaler = wrpcSignaler(client);
  assert.strictEqual(await signaler.ready(), 'i.1');
  await signaler.join('a', 'A');
  await signaler.join('b');
  const resets = [];
  const reset = new Promise((resolve) => signaler.on('reset', (payload) => resolve(resets.push(payload))));
  // The client's own 'reconnect' emit does not wait for the re-join: the
  // signaler must never hold the client's reconnect cycle hostage.
  await client.emit('reconnect', { units: [], attempts: 1, subscriptions: 0 });
  await reset;
  assert.strictEqual(signaler.id, 'i.2');
  assert.deepStrictEqual(resets, [
    {
      id: 'i.2',
      previous: 'i.1',
      rooms: [
        { room: 'a', members: [{ id: 'other', data: 'a' }] },
        { room: 'b', members: [{ id: 'other', data: 'b' }] },
      ],
    },
  ]);
  const joins = client.calls.filter((entry) => entry.method === 'signaling/join').map((entry) => entry.args);
  assert.deepStrictEqual(joins.slice(2), [
    { room: 'a', data: 'A' },
    { room: 'b', data: null },
  ]);
});

test('signaler: a failed reset surfaces as error only when someone listens', async () => {
  const client = new FakeClient();
  client.answers.set('signaling/whoami', { id: 'i.1' });
  const signaler = wrpcSignaler(client);
  await signaler.ready();
  client.answers.set('signaling/whoami', Object.assign(new Error('gone'), { code: 403 }));
  const resets = [];
  signaler.on('reset', (payload) => resets.push(payload));
  // No 'error' listener: swallowed, not thrown out of the client's emit.
  await client.emit('reconnect', {});
  await timers.setTimeout(5);
  assert.strictEqual(signaler.id, null);
  assert.deepStrictEqual(resets, []);
  const errors = [];
  const failed = new Promise((resolve) => signaler.on('error', (error) => resolve(errors.push(error.message))));
  await client.emit('reconnect', {});
  await failed;
  assert.deepStrictEqual(errors, ['gone']);
  // The next ready() tries again rather than caching the failure.
  client.answers.set('signaling/whoami', { id: 'i.9' });
  assert.strictEqual(await signaler.ready(), 'i.9');
});

test('signaler: close() during a reset swallows the reset', async () => {
  const client = new FakeClient();
  let release = null;
  client.answers.set('signaling/whoami', () => new Promise((resolve) => void (release = () => resolve({ id: 'i.2' }))));
  const signaler = wrpcSignaler(client);
  const heard = [];
  signaler.on('reset', (payload) => heard.push(payload));
  signaler.on('error', (error) => heard.push(error));
  await client.emit('reconnect', {});
  signaler.close();
  release();
  await timers.setTimeout(5);
  assert.deepStrictEqual(heard, []);
  assert.strictEqual(signaler.id, null);

  // The same when the close lands during the re-join rather than whoami.
  const late = new FakeClient();
  late.answers.set('signaling/whoami', { id: 'i.3' });
  let releaseJoin = null;
  late.answers.set(
    'signaling/join',
    () => new Promise((resolve) => void (releaseJoin = () => resolve({ members: [] }))),
  );
  const other = wrpcSignaler(late);
  const joined = other.join('r');
  await waitFor(() => releaseJoin !== null);
  releaseJoin();
  await joined;
  releaseJoin = null;
  other.on('reset', (payload) => heard.push(payload));
  await late.emit('reconnect', {});
  await waitFor(() => releaseJoin !== null);
  other.close();
  releaseJoin();
  await timers.setTimeout(5);
  assert.deepStrictEqual(heard, []);
});

test('signaler: close() detaches every listener and forgets the rooms', async () => {
  const client = new FakeClient();
  client.answers.set('signaling/whoami', { id: 'i.1' });
  client.answers.set('signaling/join', { members: [] });
  const signaler = wrpcSignaler(client);
  await signaler.join('r');
  const heard = [];
  signaler.on('signal', (payload) => heard.push(payload));
  signaler.on('reset', (payload) => heard.push(payload));
  signaler.close();
  assert.strictEqual(signaler.id, null);
  assert.deepStrictEqual(signaler.rooms, new Set());
  assert.strictEqual(client.api.signaling.listenerCount('signal'), 0);
  assert.strictEqual(client.listenerCount('reconnect'), 0);
  await client.api.signaling.emit('signal', { from: 'i.2', room: 'r', message: description });
  await client.emit('reconnect', {});
  assert.deepStrictEqual(heard, []);
  await assert.rejects(signaler.ready(), /Signaler is closed/);
  const before = client.calls.length;
  signaler.close();
  assert.strictEqual(client.calls.length, before, 'a second close is a no-op');
});
