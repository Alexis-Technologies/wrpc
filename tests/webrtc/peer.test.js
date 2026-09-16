'use strict';

// WrpcPeer + PeerLink over the fake RTC and the in-memory signaling hub:
// both directions of a link, roles by id, connect() from either side,
// accept, failure → redial → reconnect with subscription resume, uploads
// across fragmentation, and every way a link ends.

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { defineRouter, procedure, tracked, createEventLog, createEventStream } = require('../../index.js');
const { WrpcPeer, PeerLink, normalizeRedial, REDIAL } = require('../../src/webrtc/peer.js');
const { createFakeRtc } = require('./fakeRtc.js');
const { FakeSignalHub } = require('./fakeSignalHub.js');
const { within, waitFor } = require('./portContract.js');
const { createAssertionIssuer, generateAssertionKeys } = require('../../src/webrtc/assertionIssuer.js');
const { sdpFingerprint } = require('../../src/webrtc/assertions.js');

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

// The router every peer serves: calls, an inbound event, a resumable
// subscription over an event log, an upload reader and a download.
const routerOf = (name, seen = []) => {
  const feed = { log: createEventLog({ size: 16, epoch: name }), streams: new Set() };
  const push = (data) => {
    const value = tracked(feed.log.push(data), data);
    for (const stream of feed.streams) stream.push(value);
  };
  const router = defineRouter({
    calc: {
      add: procedure({ handler: async (_ctx, { a, b }) => a + b }),
      who: procedure({
        handler: async (ctx) => ({
          me: name,
          peer: ctx.session.data.peer,
          room: ctx.meta.data.room,
          token: ctx.session.token,
        }),
      }),
      feed: procedure.subscription({
        handler: async function* (_ctx, _args, { lastEventId, signal }) {
          for (const value of feed.log.since(lastEventId) ?? []) yield value;
          const stream = createEventStream({ signal });
          feed.streams.add(stream);
          try {
            for await (const value of stream) yield value;
          } finally {
            feed.streams.delete(stream);
          }
        },
      }),
      readUpload: procedure({
        handler: async (ctx, { id }) => {
          const stream = ctx.client.getStream(id);
          let total = 0;
          let sum = 0;
          for await (const chunk of stream) {
            total += chunk.length;
            for (let i = 0; i < chunk.length; i++) sum = (sum + chunk[i]) % 251;
          }
          return { total, sum };
        },
      }),
      download: procedure({
        handler: async (ctx, { size }) => {
          const stream = ctx.client.createStream('blob', size);
          queueMicrotask(() => {
            stream.write(new Uint8Array(size).fill(7));
            stream.end();
          });
          return { id: stream.id };
        },
      }),
      on: {
        ping: procedure({ handler: async (ctx, data) => void seen.push([ctx.session.data.peer, data]) }),
      },
    },
  });
  return { router, push, feed };
};

// A world: one fake RTC, one hub, peers by id. Every peer is torn down.
const world = (t, options = {}) => {
  const fake = createFakeRtc(options.fake);
  const hub = new FakeSignalHub(options.hub);
  const peers = [];
  t.after(() => {
    for (const peer of peers) peer.close();
    fake.world.close();
  });
  const peer = (id, { router = true, seen, signaler = hub.signaler(id), ...rest } = {}) => {
    const served = router ? routerOf(id, seen) : null;
    const instance = new WrpcPeer({
      router: served ? served.router : null,
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
    instance.served = served;
    peers.push(instance);
    return instance;
  };
  return { fake, hub, peer };
};

test('peer: option validation and redial normalization', () => {
  const hub = new FakeSignalHub();
  const fake = createFakeRtc();
  const base = { signaler: hub.signaler('a'), rtc: fake.adapter };
  assert.throws(() => new WrpcPeer({}), /signaler must satisfy/);
  assert.throws(() => new WrpcPeer({ ...base, rtc: {} }), /rtc must satisfy/);
  assert.throws(() => new WrpcPeer({ ...base, accept: 'yes' }), /accept must be a function/);
  assert.throws(() => new WrpcPeer({ ...base, client: null }), /client must be an object/);
  assert.throws(() => new WrpcPeer({ ...base, host: 3 }), /host must be an object/);
  assert.throws(() => new WrpcPeer({ ...base, channels: { initiator: 1, responder: 1 } }), /differ|distinct/);
  // No adapter and no RTCPeerConnection in this runtime: a plain TypeError.
  assert.throws(() => new WrpcPeer({ signaler: hub.signaler('b') }), TypeError);
  const peer = new WrpcPeer({ ...base, iceServers: [{ urls: 'stun:x' }], channels: { initiator: 5, responder: 6 } });
  assert.deepStrictEqual(peer.channels, { initiator: 5, responder: 6, label: 'wrpc' });
  assert.strictEqual(peer.host, null);
  assert.strictEqual(peer.router, null);
  assert.strictEqual(peer.id, 'a');
  assert.deepStrictEqual(peer.links, new Map());
  assert.deepStrictEqual(normalizeRedial(false), { ...REDIAL, retries: 0 });
  assert.deepStrictEqual(normalizeRedial({ retries: -1, minDelay: 0, maxDelay: 1 }), {
    ...REDIAL,
    minDelay: REDIAL.minDelay,
    maxDelay: REDIAL.minDelay,
  });
  fake.world.close();
});

test('peer: a link is both directions, with roles by id order', async (t) => {
  const { peer } = world(t);
  const seenA = [];
  const seenB = [];
  const a = peer('a', { seen: seenA });
  const b = peer('b', { seen: seenB });
  const linksSeen = [];
  a.on('link', (link) => linksSeen.push(['a', link.id]));
  b.on('link', (link) => linksSeen.push(['b', link.id]));

  const ab = await within(a.connect('b', { room: 'r', data: { name: 'bee' } }), 'a→b open');
  assert.ok(ab instanceof PeerLink);
  assert.strictEqual(ab.id, 'b');
  assert.strictEqual(ab.room, 'r');
  assert.deepStrictEqual(ab.data, { name: 'bee' });
  assert.strictEqual(ab.initiator, true);
  assert.strictEqual(ab.state, 'open');
  assert.ok(ab.open);
  assert.strictEqual(ab.link.localId, 'a');
  const ba = b.link('a');
  assert.ok(ba, 'b accepted the incoming link');
  await within(ba.ready(), 'b→a open');
  assert.strictEqual(ba.initiator, false);
  assert.strictEqual(ba.room, 'r', 'the room travels with the signal');
  assert.strictEqual(ba.data, null);
  assert.deepStrictEqual(linksSeen, [
    ['a', 'b'],
    ['b', 'a'],
  ]);
  assert.strictEqual(a.links.size, 1);
  assert.strictEqual(await a.connect('b'), ab, 'connect() is idempotent');

  // Calls in both directions, with the link's pseudo-session on each host.
  await ab.load('calc');
  await ba.load('calc');
  assert.strictEqual(await ab.api.calc.add({ a: 2, b: 3 }), 5);
  assert.strictEqual(await ba.api.calc.add({ a: 40, b: 2 }), 42);
  assert.deepStrictEqual(await ab.api.calc.who(), { me: 'b', peer: 'a', room: 'r', token: 'a' });
  assert.deepStrictEqual(await ba.api.calc.who(), { me: 'a', peer: 'b', room: 'r', token: 'b' });
  assert.deepStrictEqual(await ab.call('calc/add', { a: 1, b: 1 }), 2);
  // Host-side clients exist on both ends and are the same objects the hosts hold.
  assert.strictEqual(a.host.getClient(ab.client.id), ab.client);
  ab.join('team');
  assert.deepStrictEqual(ab.rooms, new Set(['team']));
  assert.ok(ab.client.in('team'));
  ab.leave('team');
  assert.deepStrictEqual(ab.rooms, new Set());
  assert.strictEqual(ab.client.meta.data.peer, 'b');
  assert.strictEqual(a.host.clients.size, 1);

  // Events remote → host land in the router's `on`; events host → remote
  // land on the remote's unit emitter, the way a server's events do.
  ba.remote.sendEvent('calc/ping', { from: 'b' });
  ab.remote.sendEvent('calc/ping', { from: 'a' });
  await waitFor(() => seenA.length === 1 && seenB.length === 1, 'both pings heard');
  assert.deepStrictEqual(seenA, [['b', { from: 'b' }]]);
  assert.deepStrictEqual(seenB, [['a', { from: 'a' }]]);
  const pushed = [];
  ba.api.calc.on('ping', (data) => pushed.push(data));
  ab.send('calc/ping', { from: 'a-host' });
  await waitFor(() => pushed.length === 1, 'b received the host event');
  assert.deepStrictEqual(pushed, [{ from: 'a-host' }]);

  // Ask ↔ respond in both directions.
  ba.respond('quiz', async (data) => data.n * 2);
  assert.strictEqual(await ab.ask('quiz', { n: 21 }, { timeout: 1000 }), 42);
  ab.respond('quiz', async () => {
    throw Object.assign(new Error('no'), { code: 418 });
  });
  await assert.rejects(ba.ask('quiz', {}, { timeout: 1000 }), (error) => error.code === 418);
  ab.respond('quiz', async () => 'replaced');
  assert.strictEqual(await ba.ask('quiz', {}, { timeout: 1000 }), 'replaced');
  assert.strictEqual(ab.unrespond('quiz'), true);
  assert.strictEqual(ab.unrespond('quiz'), false);
});

test('peer: connect() from the higher id knocks, and simultaneous connects make one link', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a');
  const b = peer('b');
  const ba = await within(b.connect('a'), 'b→a open');
  assert.strictEqual(ba.initiator, false);
  assert.ok(hub.sent.some((entry) => entry.from === 'b' && entry.to === 'a' && entry.type === 'connect'));
  const ab = a.link('b');
  assert.strictEqual(ab.initiator, true);
  await within(ab.ready(), 'a→b open');
  await ab.load('calc');
  assert.strictEqual(await ab.api.calc.add({ a: 1, b: 2 }), 3);

  const c = peer('c');
  const d = peer('d');
  const [cd, dc] = await within(Promise.all([c.connect('d'), d.connect('c')]), 'both open');
  assert.strictEqual(c.links.size, 1);
  assert.strictEqual(d.links.size, 1);
  assert.strictEqual(cd, c.link('d'));
  assert.strictEqual(dc, d.link('c'));
  // A knock for a link that already exists and is fine is ignored.
  hub.relay('d', 'c', null, { type: 'connect' });
  await hub.tick();
  await hub.tick();
  assert.strictEqual(cd.state, 'open');
  await assert.rejects(c.connect(''), /remoteId must be/);
  await assert.rejects(c.connect('c'), /cannot connect to self/);
});

test('peer: accept() gates incoming links; a refusal closes the caller', async (t) => {
  const { peer, hub } = world(t);
  const asked = [];
  const a = peer('a', {
    accept: async (from, room) => {
      asked.push([from, room]);
      if (from === 'x') throw new Error('accept broke');
      return from !== 'z';
    },
  });
  const z = peer('z');
  await assert.rejects(within(z.connect('a', { room: 'lobby' }), 'z refused'), /closed/);
  assert.strictEqual(a.link('z'), undefined);
  assert.strictEqual(z.link('a'), undefined, 'the refused side dropped its link too');
  assert.deepStrictEqual(asked, [['z', 'lobby']]);
  assert.ok(hub.sent.some((entry) => entry.from === 'a' && entry.to === 'z' && entry.type === 'close'));

  const x = peer('x');
  await assert.rejects(within(x.connect('a'), 'x refused'), /closed/);
  assert.strictEqual(a.errors.length, 1);
  assert.strictEqual(a.errors[0].message, 'accept broke');

  const b = peer('b');
  const ba = await within(b.connect('a'), 'b accepted');
  assert.strictEqual(ba.state, 'open');
  assert.deepStrictEqual(asked.slice(1), [
    ['x', null],
    ['b', null],
  ]);
  // Signals from nobody in particular, or for a link we do not have, are noise.
  hub.relay('ghost', 'a', null, { type: 'candidate', candidate: null });
  hub.relay('ghost', 'a', null, { type: 'close' });
  hub.relay('a', 'a', null, { type: 'connect' });
  await hub.tick();
  await hub.tick();
  assert.strictEqual(a.links.size, 1);
});

test('peer: uploads and downloads cross the fragmenting channel', async (t) => {
  const { peer } = world(t, { fake: { maxMessageSize: 16 * 1024 } });
  const a = peer('a');
  peer('b');
  const ab = await within(a.connect('b'), 'open');
  await ab.load('calc');
  const size = 1024 * 1024;
  const payload = new Uint8Array(size);
  let sum = 0;
  for (let i = 0; i < size; i++) {
    payload[i] = i % 256;
    sum = (sum + payload[i]) % 251;
  }
  const stream = ab.remote.createStream('blob', size);
  const receiving = ab.api.calc.readUpload({ id: stream.id });
  const drained = () => new Promise((resolve) => stream.once('drain', resolve));
  for (let offset = 0; offset < size; offset += 64 * 1024) {
    if (!stream.write(payload.subarray(offset, offset + 64 * 1024))) await drained();
  }
  stream.end();
  assert.deepStrictEqual(await within(receiving, 'upload read'), { total: size, sum });

  const { id } = await ab.api.calc.download({ size: 100 * 1024 });
  const readable = ab.remote.getStream(id);
  let got = 0;
  for await (const chunk of readable) got += chunk.length;
  assert.strictEqual(got, 100 * 1024);
});

test('peer: ICE failure → redial → the client reconnects and a subscription resumes', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a');
  const b = peer('b');
  const ab = await within(a.connect('b'), 'open');
  const ba = await within(b.link('a').ready(), 'open');
  await ab.load('calc');
  const got = [];
  const handle = ab.api.calc.feed.subscribe({}, { onData: (value) => got.push(value) });
  await waitFor(() => b.served.feed.streams.size === 1, 'the handler is live');
  b.served.push('one');
  b.served.push('two');
  await waitFor(() => got.length === 2, 'two values');
  assert.strictEqual(handle.lastEventId, 'b.1');

  const states = [];
  ab.on('state', (state) => states.push(state));
  const reconnected = onceEvent(ab, 'reconnect');
  const firstClient = ab.client;
  const hostDetached = onceEvent(a.host, 'detach');
  // Signaling is deaf while ICE fails, so the restart cannot heal the
  // connection and the link fails outright; then it comes back for the
  // redial.
  hub.mute('a');
  hub.mute('b');
  ab.link.pc.failIce();
  await within(hostDetached, 'host client destroyed');
  assert.strictEqual(ab.client, null, 'no host client while down');
  assert.strictEqual(ab.link.state, 'failed');
  hub.unmute('a');
  hub.unmute('b');
  await within(reconnected, 'link reconnected');
  assert.deepStrictEqual(states, ['reconnecting', 'open']);
  assert.notStrictEqual(ab.client, firstClient, 'a fresh host client after the redial');
  await waitFor(() => ba.state === 'open', 'the responder followed');

  // Pushed while the link was down: replayed from the log, no duplicates.
  b.served.push('three');
  await waitFor(() => got.length === 3, 'resumed value');
  assert.deepStrictEqual(got, ['one', 'two', 'three']);
  assert.strictEqual(await ab.api.calc.add({ a: 1, b: 1 }), 2);
  const client = b.link('a').remote;
  assert.ok(client.active, 'the responder side client is up again');
});

test('peer: a heartbeat timeout on a silently dead path restarts ICE, then the redial cycle takes over', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a', {
    client: { heartbeat: { interval: 15, timeout: 15 }, reconnect: { minDelay: 5, maxDelay: 20, jitter: false } },
    restartTimeout: 40,
  });
  const b = peer('b', { restartTimeout: 40 });
  const ab = await within(a.connect('b'), 'open');
  const ba = await within(b.link('a').ready(), 'open');
  let timeouts = 0;
  ab.remote.on('heartbeat-timeout', () => void timeouts++);
  // The path dies but ICE has not noticed yet (no state change, bytes
  // eaten); signaling is deaf so the restart cannot heal it either.
  hub.mute('a');
  hub.mute('b');
  const pc = ab.link.pc;
  pc.blackhole();
  await waitFor(() => timeouts >= 1, 'the heartbeat noticed');
  assert.ok(pc.restarts >= 1, 'the transport asked the link for an ICE restart');
  // Until the restart fails the client keeps re-opening on the dead link
  // (it still reads 'connected') and timing out again — bounded by
  // restartTimeout, after which the redial cycle owns the recovery.
  await waitFor(() => ab.link.state === 'failed', 'the restart failed');
  assert.ok(timeouts >= 2, 'the heartbeat kept timing out on the dead path');
  assert.strictEqual(ab.state, 'reconnecting');
  const reconnected = onceEvent(ab, 'reconnect');
  hub.unmute('a');
  hub.unmute('b');
  await within(reconnected, 'redialled and reconnected');
  await waitFor(() => ba.state === 'open', 'the responder followed');
  await ab.load('calc');
  assert.strictEqual(await ab.api.calc.add({ a: 2, b: 2 }), 4);
});

test('peer: a responder whose initiator vanished knocks, then gives up', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a');
  const b = peer('b', { redial: { retries: 2, minDelay: 5, maxDelay: 5, jitter: false }, connectTimeout: 40 });
  const ab = await within(a.connect('b'), 'open');
  const ba = b.link('a');
  await within(ba.ready(), 'open');
  // a vanishes without a goodbye (its signaling is deaf both ways), then
  // b's link fails: b knocks twice, nobody dials, b closes.
  hub.mute('a');
  hub.mute('b');
  a.close();
  const closed = onceEvent(ba, 'close');
  ba.link.pc.failIce();
  await within(closed, 'b gave up');
  assert.strictEqual(ba.state, 'closed');
  assert.strictEqual(hub.sent.filter((e) => e.from === 'b' && e.type === 'connect').length, 2);
  assert.strictEqual(b.links.size, 0);
  assert.strictEqual(ab.state, 'closed');
  assert.strictEqual(await ba.ready(), ba, 'ready() stays settled: the link did open once');
  assert.throws(() => ba.send('x', 1), /not open/);
  assert.throws(() => ba.ask('x', 1), /not open/);
  assert.throws(() => ba.createStream('x', 1), /not open/);
});

test('peer: an initiator that cannot reach its peer redials with backoff and gives up', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a', { redial: { retries: 2, minDelay: 5, maxDelay: 5, jitter: false }, connectTimeout: 30 });
  const b = peer('b');
  const ab = await within(a.connect('b'), 'open');
  const ba = b.link('a');
  const bClosed = onceEvent(ba, 'close');
  hub.mute('b');
  hub.mute('a');
  const closed = onceEvent(ab, 'close');
  const states = [];
  ab.on('state', (state) => states.push(state));
  const redials = [];
  ab.link.on('state', (state) => void (state === 'reconnecting' && redials.push(state)));
  ab.link.pc.failIce();
  await within(closed, 'a gave up');
  assert.strictEqual(ab.state, 'closed');
  assert.deepStrictEqual(states, ['reconnecting', 'closed']);
  assert.strictEqual(redials.length, 2, 'two redials, then it gave up');
  assert.strictEqual(a.links.size, 0);
  assert.ok(!ab.remote.active);
  // b's side meanwhile knocked into the void and closed on its own.
  await within(bClosed, 'b gave up');
  assert.strictEqual(b.links.size, 0);
});

test("peer: close() says goodbye on every link; the signaler's reset closes them all", async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a');
  const b = peer('b');
  const c = peer('c');
  await within(Promise.all([a.connect('b'), a.connect('c')]), 'open');
  const closes = [];
  b.link('a').on('close', () => closes.push('b'));
  c.link('a').on('close', () => closes.push('c'));
  const closedA = onceEvent(a, 'close');
  a.close();
  await within(closedA, 'a closed');
  a.close(); // idempotent
  await waitFor(() => closes.length === 2, 'both peers told');
  assert.strictEqual(a.links.size, 0);
  assert.strictEqual(b.links.size, 0);
  assert.strictEqual(c.links.size, 0);
  await assert.rejects(a.start(), /closed/);
  await assert.rejects(a.connect('b'), /closed/);

  const bc = await within(b.connect('c'), 'open');
  const resetSeen = onceEvent(b, 'reset');
  await hub.reset('b', 'b2');
  await within(resetSeen, 'reset announced');
  assert.strictEqual(b.id, 'b2');
  assert.strictEqual(bc.state, 'closed');
  await waitFor(() => c.links.size === 0, 'c dropped the old link');
  const b2c = await within(b.connect('c'), 'open under the new id');
  assert.strictEqual(b2c.link.localId, 'b2');
});

test('peer: a signaling reconnect under the same id keeps the links; a new id closes them', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a');
  const b = peer('b');
  const ab = await within(a.connect('b'), 'open');
  const closes = [];
  ab.on('close', () => closes.push('ab'));
  b.link('a').on('close', () => closes.push('ba'));
  const reset = onceEvent(a, 'reset');
  await hub.reconnect('a');
  const event = await within(reset, 'reset announced');
  assert.deepStrictEqual([event.id, event.previous], ['a', 'a']);
  assert.strictEqual(a.link('b'), ab, 'the same link object');
  assert.strictEqual(ab.state, 'open');
  await ab.load('calc');
  assert.strictEqual(await ab.api.calc.add({ a: 2, b: 3 }), 5, 'and it still answers');
  assert.deepStrictEqual(closes, []);
  // The id changes: the links were made under the old one, so they go.
  await hub.reset('a', 'a1');
  await waitFor(() => closes.length === 2, 'both halves closed');
  assert.strictEqual(a.links.size, 0);
});

test('peer: another incarnation of a linked id replaces the stale link, without a goodbye to the newcomer', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a');
  const b = peer('b');
  const stale = await within(a.connect('b'), 'open');
  assert.strictEqual(stale.instance, b.signaler.instance, 'the instance is learned from the first signal');
  assert.strictEqual(b.link('a').instance, a.signaler.instance);
  const staleClosed = onceEvent(stale, 'close');
  // b comes back as a new tab under the same id: a knock (or offer) from the
  // new instance abandons the old link on a and opens a new one.
  const fresh = peer('b', { signaler: await hub.replace('b') });
  const opened = onceEvent(a, 'link');
  const link = await within(fresh.connect('a'), 'new link open');
  await within(staleClosed, 'stale closed');
  const replacement = await within(opened, 'a made a new link');
  assert.strictEqual(a.link('b'), replacement);
  assert.strictEqual(replacement.instance, fresh.signaler.instance);
  assert.strictEqual(link.state, 'open');
  await link.load('calc');
  assert.strictEqual(await link.api.calc.add({ a: 1, b: 1 }), 2);
  // No 'close' was sent to b's new incarnation while the old link went.
  assert.ok(!hub.sent.some((entry) => entry.from === 'a' && entry.type === 'close'), 'no goodbye sent');
  // A hand-rolled signaler that carries no instance never triggers this.
  assert.strictEqual(a.link('b'), replacement);
  hub.relay('b', 'a', null, { type: 'connect' });
  await hub.tick();
  assert.strictEqual(a.link('b'), replacement, 'an instance-less signal is not another incarnation');
});

test('peer: a replaced peer abandons its links and closes', async (t) => {
  const { peer, hub } = world(t);
  const a = peer('a');
  const b = peer('b');
  const c = peer('c');
  await within(Promise.all([a.connect('b'), a.connect('c')]), 'open');
  const replaced = onceEvent(a, 'replaced');
  const closed = onceEvent(a, 'close');
  await hub.replace('a');
  assert.deepStrictEqual(await within(replaced, 'told'), { id: 'a' });
  await within(closed, 'closed');
  assert.strictEqual(a.links.size, 0);
  assert.ok(!hub.sent.some((entry) => entry.from === 'a' && entry.type === 'close'), 'no goodbye from a stranger');
  await assert.rejects(a.connect('b'), /closed/);
  // b and c lose the link through ICE, not through signaling.
  await waitFor(() => b.links.size === 0 && c.links.size === 0, 'the others gave up');
});

test('peer: a peer without a router is client-only', async (t) => {
  const { peer } = world(t);
  const a = peer('a', { router: false });
  const b = peer('b');
  const ab = await within(a.connect('b'), 'open');
  assert.strictEqual(ab.client, null);
  await ab.load('calc');
  assert.strictEqual(await ab.api.calc.add({ a: 1, b: 2 }), 3);
  assert.throws(() => ab.send('x', 1), /not open/);
  // The other side's calls have nobody to answer them: a timeout, not a hang.
  const ba = b.link('a');
  await assert.rejects(ba.call('calc/add', {}, { timeout: 30 }), (error) => /timeout/i.test(error.message));
});

test('peer: signaler send failures and link errors reach the peer error listener', async (t) => {
  const { peer } = world(t);
  const a = peer('a');
  peer('b');
  const ab = await within(a.connect('b'), 'open');
  const original = a.signaler.send;
  a.signaler.send = () => {
    throw new Error('signaling down');
  };
  t.after(() => void (a.signaler.send = original));
  a.signal('b', { type: 'close' }, null);
  assert.strictEqual(a.errors.at(-1).message, 'signaling down');
  a.signaler.send = () => Promise.reject(new Error('async down'));
  a.signal('b', { type: 'close' }, null);
  await timers.setTimeout(5);
  assert.strictEqual(a.errors.at(-1).message, 'async down');
  // A link error goes to the peer's listener; a link with its own error
  // listener keeps errors to itself.
  ab.link.emit('error', new Error('link first'));
  await timers.setTimeout(5);
  assert.strictEqual(a.errors.at(-1).message, 'link first');
  const mine = [];
  ab.on('error', (error) => mine.push(error.message));
  const count = a.errors.length;
  ab.link.emit('error', new Error('link says'));
  await timers.setTimeout(5);
  assert.deepStrictEqual(mine, ['link says']);
  assert.strictEqual(a.errors.length, count);
});

test('peer: a signaler that cannot identify fails start() and connect(), and start() retries', async (t) => {
  const { fake, hub } = world(t);
  const signaler = hub.signaler('s');
  let attempts = 0;
  signaler.ready = () => {
    attempts++;
    return attempts < 4 ? Promise.reject(new Error('no whoami yet')) : Promise.resolve('s');
  };
  // Without a peer 'error' listener an escalation is logged, not thrown.
  const peer = new WrpcPeer({ signaler, rtc: fake.adapter, logger: quiet });
  t.after(() => peer.close());
  await assert.rejects(peer.start(), /no whoami yet/);
  await assert.rejects(peer.connect('x'), /no whoami yet/);
  // A signal arriving while the peer cannot identify is dropped, not fatal
  // (the third attempt, made on its behalf, fails too).
  hub.relay('x', 's', null, { type: 'connect' });
  await hub.tick();
  await hub.tick();
  assert.strictEqual(attempts, 3);
  assert.strictEqual(await peer.start(), 's');
  assert.strictEqual(attempts, 4);
  assert.strictEqual(peer.links.size, 0);
  peer.escalate(new Error('nobody listens'));
});

// ---------------------------------------------------------------------------
// Trust assertions: the hub issues them the way the real unit does, the
// peers verify them against the hub's public keys.

const trusted = async (t, { claims = null, peerOptions = {} } = {}) => {
  const keys = await generateAssertionKeys({ kid: 'hub' });
  const issuer = createAssertionIssuer({ key: keys.privateKey, ttl: 60, issuer: 'hub.test' });
  const { peer, hub, fake } = world(t, { hub: { issuer, claims } });
  const assertions = { issuer: 'hub.test', ...peerOptions.assertions };
  const trustedPeer = (id, options = {}) =>
    peer(id, { assertions, host: { trust: 'assertion' }, ...peerOptions, ...options });
  return { peer: trustedPeer, plain: peer, hub, fake, keys, issuer };
};

test('peer: assertions are issued per dial, verified both ways and land in the session', async (t) => {
  const seen = [];
  const { peer, hub, keys } = await trusted(t, {
    claims: (id) => ({ role: id === 'a' ? 'host' : 'guest' }),
    peerOptions: {
      accept: (from, room, about) => {
        seen.push([from, room, about.instance, about.claims?.sub, about.claims?.role]);
        return true;
      },
    },
  });
  const a = peer('a');
  const b = peer('b');
  assert.strictEqual(a.assertions, true);
  const ab = await within(a.connect('b', { room: 'r' }), 'open');
  const ba = b.link('a');
  // Both descriptions carried a token bound to the pc that sent them.
  const stamped = hub.sent.filter((entry) => entry.type === 'description');
  assert.strictEqual(stamped.length, 2, 'offer and answer');
  assert.deepStrictEqual([ab.claims.sub, ab.claims.role, ab.claims.iss], ['b', 'guest', 'hub.test']);
  assert.deepStrictEqual([ba.claims.sub, ba.claims.role], ['a', 'host']);
  assert.strictEqual(ab.claims.fp, ba.link.pc.fingerprint, "b's token names b's certificate");
  assert.strictEqual(ba.claims.fp, ab.link.pc.fingerprint);
  // b's accept saw a's verified claims before it let the link in; a's
  // connect() dialled, so b's claims came with the answer.
  assert.deepStrictEqual(seen, [['a', 'r', a.signaler.instance, 'a', 'host']]);
  // The host half sees them as the session: trust 'assertion'.
  await ab.load('calc');
  const who = await ab.api.calc.who();
  assert.strictEqual(who.peer, 'a');
  assert.strictEqual(who.token, 'a');
  assert.strictEqual(ba.client.session.data.claims.role, 'host');
  assert.strictEqual(ab.client.session.data.claims.role, 'guest');
  assert.ok(Object.isFrozen(ab.client.session.data.claims));
  assert.strictEqual(keys.publicKey.kid, 'hub');
});

test('peer: a description without a valid assertion is refused before it reaches the link', async (t) => {
  const { peer, plain, hub, issuer } = await trusted(t);
  const a = peer('a');
  // c has no assertions configured: its offer carries no token.
  const c = plain('c');
  await assert.rejects(within(c.connect('a'), 'c refused'), /closed/);
  assert.strictEqual(a.link('c'), undefined);
  assert.ok(hub.sent.some((entry) => entry.from === 'a' && entry.to === 'c' && entry.type === 'close'));

  // d signs with a key of its own: the signature does not verify.
  const rogue = await generateAssertionKeys({ kid: 'hub' });
  const d = peer('d', { signaler: hub.signaler('d', { issuer: createAssertionIssuer({ key: rogue.privateKey }) }) });
  await assert.rejects(within(d.connect('a'), 'd refused'), /closed/);
  assert.strictEqual(a.link('d'), undefined);

  // e replays a token for another certificate: the fingerprint does not
  // match the description it arrives with.
  const replay = {
    sign: (claims) => issuer.sign({ ...claims, fp: 'sha-256 ' + 'AA:'.repeat(31) + 'AA' }),
    publicKeys: () => issuer.publicKeys(),
  };
  const e = peer('e', { signaler: hub.signaler('e', { issuer: replay }) });
  await assert.rejects(within(e.connect('a'), 'e refused'), /closed/);
  assert.strictEqual(a.link('e'), undefined);

  // The dialling side refuses a bad ANSWER the same way: a dials f, whose
  // answer is signed by the rogue key; a's link closes with a goodbye.
  const f = peer('f', { signaler: hub.signaler('f', { issuer: createAssertionIssuer({ key: rogue.privateKey }) }) });
  await assert.rejects(within(a.connect('f'), 'f refused'), /closed/);
  assert.strictEqual(a.link('f'), undefined);
  await waitFor(() => f.links.size === 0, 'f dropped too');

  // And a good peer still gets in.
  peer('b');
  const ab = await within(a.connect('b'), 'b accepted');
  assert.strictEqual(ab.claims.sub, 'b');
});

test('peer: a redial re-verifies the new certificate; an ICE restart on the same pc does not', async (t) => {
  const { peer, hub, fake } = await trusted(t);
  const a = peer('a');
  const b = peer('b');
  const ab = await within(a.connect('b'), 'open');
  const ba = b.link('a');
  const dials = () => hub.sent.filter((entry) => entry.type === 'description').length;
  const first = { a: ab.claims.fp, b: ba.claims.fp };
  assert.strictEqual(dials(), 2);

  // ICE restart: same pc, same certificate — offer/answer flow again, with
  // tokens (the initiator stamps every description) but no new claims.
  const restarted = onceEvent(ab.link, 'restart');
  ab.link.restart();
  await within(restarted, 'restarted');
  await waitFor(() => dials() === 4, 'restart offer and answer');
  assert.deepStrictEqual({ a: ab.claims.fp, b: ba.claims.fp }, first, 'the pins did not move');

  // A failed link redials onto a fresh pc: new certificates, new
  // fingerprints, verified anew — and the session claims follow.
  const reopened = onceEvent(ab, 'reconnect');
  ab.link.pc.failIce();
  await within(reopened, 'redialled');
  await waitFor(() => ba.open, 'b side back');
  assert.notStrictEqual(ab.claims.fp, first.a, 'a new certificate on b');
  assert.notStrictEqual(ba.claims.fp, first.b);
  assert.strictEqual(ab.claims.fp, ba.link.pc.fingerprint);
  assert.strictEqual(ba.client.session.data.claims.fp, ba.claims.fp, 'the re-attached host half has the new claims');
  void fake;
  await ab.load('calc');
  assert.strictEqual(await ab.api.calc.add({ a: 1, b: 2 }), 3);
});

test('peer: candidates never overtake a description that is waiting for its assertion', async (t) => {
  const { peer, hub, issuer } = await trusted(t);
  // A slow issuer: the offer waits ~30ms for its token while ICE gathers.
  const slow = {
    sign: async (claims) => {
      await timers.setTimeout(30);
      return issuer.sign(claims);
    },
    publicKeys: () => issuer.publicKeys(),
  };
  const a = peer('a', { signaler: hub.signaler('a', { issuer: slow }) });
  peer('b', { signaler: hub.signaler('b', { issuer: slow }) });
  const ab = await within(a.connect('b'), 'open');
  assert.strictEqual(ab.state, 'open');
  const order = hub.sent.filter((entry) => entry.from === 'a').map((entry) => entry.type);
  assert.strictEqual(order.indexOf('description'), 0, 'the offer went first');
  assert.ok(order.includes('candidate'));
});

test("peer: host trust 'assertion' requires assertions; assertions need a signaler that issues", async (t) => {
  const { fake, hub } = world(t);
  const { router } = routerOf('x');
  assert.throws(
    () => new WrpcPeer({ router, signaler: hub.signaler('x'), rtc: fake.adapter, host: { trust: 'assertion' } }),
    /needs options.assertions/,
  );
  const mute = { id: null, ready: async () => 'y', send() {}, on() {}, off() {} };
  assert.throws(
    () => new WrpcPeer({ router, signaler: mute, rtc: fake.adapter, assertions: {} }),
    /signaler with assert/,
  );
  assert.throws(
    () => new WrpcPeer({ router, signaler: hub.signaler('z'), rtc: fake.adapter, assertions: 'yes' }),
    /must be an object/,
  );
  // A signaler that issues but publishes no keys needs them by hand.
  const issuing = { ...mute, assert: async () => ({ assertion: 'x.y.z' }) };
  assert.throws(
    () => new WrpcPeer({ router, signaler: issuing, rtc: fake.adapter, assertions: {} }),
    /keys is required/,
  );
  const keys = await generateAssertionKeys();
  const peer = new WrpcPeer({ router, signaler: issuing, rtc: fake.adapter, assertions: { keys: keys.publicKey } });
  assert.strictEqual(peer.assertions, true);
  peer.close();
  assert.strictEqual(sdpFingerprint('v=0'), null);
});
