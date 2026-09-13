'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const { RtcLink, normalizeChannels, DEFAULT_CHANNELS, MAX_CHANNEL_ID } = require('../../src/webrtc/link.js');
const { MIN_MESSAGE_SIZE } = require('../../src/webrtc/framing.js');
const { createFakeRtc } = require('./fakeRtc.js');
const { waitFor, within, once } = require('./portContract.js');

// Structured (child() marks it so): entries arrive as objects, not strings.
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

// Two links wired to each other through an in-memory signaler: `a.signal`
// delivers to `b.receive` and back, asynchronously like a real relay, with
// a switch to cut the relay and a log of everything that crossed.
const wire = (t, { world, adapter }, options = {}) => {
  const relay = { up: true, log: [] };
  const links = {};
  const make = (localId, remoteId) =>
    new RtcLink({
      localId,
      remoteId,
      adapter,
      log: quiet,
      ...options,
      signal: async (message) => {
        relay.log.push([localId, message.type]);
        if (!relay.up) throw new Error('relay is down');
        await timers.setImmediate();
        const target = links[remoteId];
        if (target) await target.receive(message);
      },
    });
  links.a = make('a', 'b');
  links.b = make('b', 'a');
  t.after(() => {
    links.a.close();
    links.b.close();
    world.close();
  });
  return { a: links.a, b: links.b, relay };
};

const bothOpen = (a, b) => within(Promise.all([a.waitOpen(), b.waitOpen()]), 'both links open');

test('rtc link: roles come from the ids, channels are validated', () => {
  const { adapter, world } = createFakeRtc();
  const link = new RtcLink({ localId: 'a', remoteId: 'b', adapter, signal() {}, log: quiet });
  assert.strictEqual(link.initiator, true);
  assert.strictEqual(link.polite, false);
  assert.strictEqual(link.state, 'new');
  assert.strictEqual(link.pc, null);
  assert.strictEqual(link.maxMessageSize, MIN_MESSAGE_SIZE);
  assert.deepStrictEqual(link.channels, DEFAULT_CHANNELS);
  const other = new RtcLink({ localId: 'b', remoteId: 'a', adapter, signal() {}, log: quiet });
  assert.strictEqual(other.initiator, false);
  assert.strictEqual(other.polite, true);
  world.close();

  assert.throws(() => new RtcLink({ localId: '', remoteId: 'b', adapter, signal() {} }), TypeError);
  assert.throws(() => new RtcLink({ localId: 'a', remoteId: 'a', adapter, signal() {} }), TypeError);
  assert.throws(() => new RtcLink({ localId: 'a', remoteId: 'b', adapter: {}, signal() {} }), TypeError);
  assert.throws(() => new RtcLink({ localId: 'a', remoteId: 'b', adapter, signal: 1 }), TypeError);
  assert.throws(
    () => new RtcLink({ localId: 'a', remoteId: 'b', adapter, signal() {}, channels: { initiator: 3, responder: 3 } }),
    TypeError,
  );

  assert.deepStrictEqual(normalizeChannels(), DEFAULT_CHANNELS);
  assert.deepStrictEqual(normalizeChannels({ initiator: 10 }), { initiator: 10, responder: 1, label: 'wrpc' });
  assert.throws(() => normalizeChannels(null), TypeError);
  assert.throws(() => normalizeChannels({ initiator: -1 }), TypeError);
  assert.throws(() => normalizeChannels({ responder: MAX_CHANNEL_ID + 1 }), TypeError);
  assert.throws(() => normalizeChannels({ initiator: 1.5 }), TypeError);
  assert.throws(() => normalizeChannels({ label: '' }), TypeError);
  assert.throws(() => normalizeChannels({ label: 7 }), TypeError);
});

test('rtc link: the initiator offers, both channels open on both ends, the offer count is one', async (t) => {
  const fake = createFakeRtc({ maxMessageSize: 65536 });
  const { a, b, relay } = wire(t, fake);
  const states = { a: [], b: [] };
  a.on('state', (s) => states.a.push(s));
  b.on('state', (s) => states.b.push(s));
  const opens = [onceEvent(a, 'open'), onceEvent(b, 'open')];
  b.start();
  a.start();
  await bothOpen(a, b);
  await within(Promise.all(opens), 'open events');
  assert.strictEqual(a.state, 'connected');
  assert.strictEqual(b.state, 'connected');
  assert.deepStrictEqual(states.a, ['connecting', 'connected']);
  assert.deepStrictEqual(states.b, ['connecting', 'connected']);
  assert.strictEqual(a.open, true);
  assert.strictEqual(a.maxMessageSize, 65536);
  assert.strictEqual(b.maxMessageSize, 65536);
  // Roles on the wire: a offered (impolite), b answered.
  const descriptions = relay.log.filter(([, type]) => type === 'description');
  assert.deepStrictEqual(descriptions, [
    ['a', 'description'],
    ['b', 'description'],
  ]);
  // Channel geometry: a's client channel is b's host channel and vice versa.
  assert.strictEqual(a.clientChannel.id, DEFAULT_CHANNELS.initiator);
  assert.strictEqual(b.hostChannel.id, DEFAULT_CHANNELS.initiator);
  assert.strictEqual(b.clientChannel.id, DEFAULT_CHANNELS.responder);
  assert.strictEqual(a.hostChannel.id, DEFAULT_CHANNELS.responder);
  assert.strictEqual(a.clientChannel.binaryType, 'arraybuffer');
  assert.strictEqual(a.clientChannel.readyState, 'open');
  // Traffic crosses: a's client writes, b's host reads.
  const got = once(b.hostChannel, 'message');
  a.clientChannel.send('hi');
  assert.strictEqual((await within(got, 'message')).data, 'hi');
  // Trickle ICE went both ways, end-of-candidates included (the null
  // candidate may trail the open).
  await within(
    waitFor(() => a.pc.endOfCandidates && b.pc.endOfCandidates, 'end of candidates'),
    'end of candidates',
  );
  assert.ok(a.pc.remoteCandidates.length >= 1 && b.pc.remoteCandidates.length >= 1);
  // waitOpen() after the fact resolves immediately; start() twice throws.
  await a.waitOpen();
  assert.throws(() => a.start(), /already connected/);
});

test('rtc link: custom channel ids and label are used on both ends', async (t) => {
  const fake = createFakeRtc();
  const { a, b } = wire(t, fake, { channels: { initiator: 40, responder: 41, label: 'app' } });
  a.start();
  b.start();
  await bothOpen(a, b);
  assert.strictEqual(a.clientChannel.id, 40);
  assert.strictEqual(a.clientChannel.label, 'app');
  assert.strictEqual(b.clientChannel.id, 41);
  assert.strictEqual(b.hostChannel.id, 40);
});

test('rtc link: candidates arriving before the description are buffered, not dropped', async (t) => {
  const fake = createFakeRtc();
  // A relay that delivers candidates FIRST: descriptions are held until the
  // candidates for the same side went through.
  const links = {};
  const held = [];
  const make = (localId, remoteId) =>
    new RtcLink({
      localId,
      remoteId,
      adapter: fake.adapter,
      log: quiet,
      signal: async (message) => {
        const target = () => links[remoteId];
        if (message.type === 'description' && localId === 'a') {
          held.push(message);
          return;
        }
        await timers.setImmediate();
        await target().receive(message);
        // Once a's candidates are out, release the held offer.
        if (message.type === 'candidate' && message.candidate === null && localId === 'a') {
          for (const description of held.splice(0)) await target().receive(description);
        }
      },
    });
  links.a = make('a', 'b');
  links.b = make('b', 'a');
  t.after(() => {
    links.a.close();
    links.b.close();
    fake.world.close();
  });
  links.a.start();
  links.b.start();
  await bothOpen(links.a, links.b);
  assert.ok(links.b.pc.remoteCandidates.length >= 1, 'buffered candidates were applied after the offer');
});

test('rtc link: an offer collision is resolved by politeness — the responder rolls back', async (t) => {
  const fake = createFakeRtc();
  const { a, b, relay } = wire(t, fake);
  b.start();
  // Force the responder into an offer of its own before a's arrives: the
  // fake (like libdatachannel) lets a pc offer on its own.
  await b.pc.setLocalDescription();
  assert.strictEqual(b.pc.signalingState, 'have-local-offer');
  a.start();
  await bothOpen(a, b);
  assert.strictEqual(a.state, 'connected');
  assert.strictEqual(b.state, 'connected');
  assert.ok(relay.log.some(([from, type]) => from === 'a' && type === 'description'));
});

test('rtc link: the impolite side ignores a colliding offer and its candidates', async (t) => {
  const fake = createFakeRtc();
  const { a, b } = wire(t, fake);
  const errors = [];
  a.on('error', (e) => errors.push(e));
  a.start();
  // While a is offering, a stray offer from b arrives: ignored, no error,
  // and a stray candidate for it is swallowed too.
  const stray = await fake.adapter.createPeerConnection().createOffer();
  await a.receive({ type: 'description', description: { ...stray, sdp: `v=0 fake ${b.pc?.id ?? 'pc2'} o=9` } });
  await a.receive({ type: 'candidate', candidate: { candidate: 'candidate:x', sdpMid: '0' } });
  b.start();
  await bothOpen(a, b);
  assert.deepStrictEqual(errors, []);
});

test('rtc link: ICE failure — the initiator restarts and the link recovers with its channels intact', async (t) => {
  const fake = createFakeRtc();
  const { a, b, relay } = wire(t, fake, { restartTimeout: 2000 });
  a.start();
  b.start();
  await bothOpen(a, b);
  const clientChannel = a.clientChannel;
  const before = relay.log.length;
  a.pc.failIce();
  await within(
    waitFor(() => a.pc.restarts === 1, 'a restart'),
    'restart',
  );
  await within(
    waitFor(() => a.pc.connectionState === 'connected' && b.pc.connectionState === 'connected', 'reconnected'),
    'reconnected',
  );
  assert.strictEqual(a.state, 'connected', 'the link never left connected');
  assert.strictEqual(b.state, 'connected');
  assert.strictEqual(a.clientChannel, clientChannel, 'the same channel survives an ICE restart');
  assert.strictEqual(b.pc.restarts, 0, 'the responder did not race a second restart');
  const renegotiated = relay.log.slice(before).filter(([, type]) => type === 'description');
  assert.deepStrictEqual(renegotiated, [
    ['a', 'description'],
    ['b', 'description'],
  ]);
  const got = once(b.hostChannel, 'message');
  clientChannel.send('after restart');
  assert.strictEqual((await within(got, 'message after restart')).data, 'after restart');
});

test('rtc link: a restart that never reconnects fails both sides after restartTimeout', async (t) => {
  const fake = createFakeRtc();
  const { a, b, relay } = wire(t, fake, { restartTimeout: 40 });
  a.start();
  b.start();
  await bothOpen(a, b);
  relay.up = false; // the renegotiation cannot reach the peer
  const errors = [];
  a.on('error', (e) => errors.push(e));
  a.pc.failIce();
  await within(
    waitFor(() => a.state === 'failed' && b.state === 'failed', 'both failed'),
    'failed',
  );
  assert.ok(
    errors.some((e) => /relay is down/.test(e.message)),
    'the failed signal surfaced as an error event',
  );
  await assert.rejects(a.waitOpen(), /failed/);
  assert.strictEqual(a.pc, null, 'the failed pc is released');
});

test('rtc link: redial after failure — the responder follows the initiator onto a new pc', async (t) => {
  const fake = createFakeRtc();
  const { a, b } = wire(t, fake, { restartTimeout: 20 });
  a.start();
  b.start();
  await bothOpen(a, b);
  const firstPc = a.pc;
  const reopened = { a: 0, b: 0 };
  a.on('open', () => reopened.a++);
  b.on('open', () => reopened.b++);
  // Kill the link for good: fail ICE and swallow the restart on b's side.
  const originalReceive = b.receive.bind(b);
  let swallow = true;
  b.receive = (message) => (swallow ? Promise.resolve() : originalReceive(message));
  a.pc.failIce();
  await within(
    waitFor(() => a.state === 'failed', 'a failed'),
    'a failed',
  );
  await within(
    waitFor(() => b.state === 'failed', 'b failed'),
    'b failed',
  );
  swallow = false;
  assert.strictEqual(b.redial(), true, 'from failed, the owner may redial either side');
  assert.strictEqual(b.state, 'reconnecting');
  assert.strictEqual(a.redial(), true);
  assert.strictEqual(a.state, 'reconnecting');
  assert.notStrictEqual(a.pc, firstPc);
  await bothOpen(a, b);
  assert.strictEqual(reopened.a, 1);
  assert.strictEqual(reopened.b, 1);
  assert.strictEqual(a.redial(), false, 'redial is a no-op unless failed');
  const got = once(b.hostChannel, 'message');
  a.clientChannel.send('redialled');
  assert.strictEqual((await within(got, 'message')).data, 'redialled');
});

test("rtc link: an initiator's redial offer reaches a responder that has not redialled yet", async (t) => {
  const fake = createFakeRtc();
  const { a, b } = wire(t, fake, { restartTimeout: 20 });
  a.start();
  b.start();
  await bothOpen(a, b);
  const originalReceive = b.receive.bind(b);
  let swallow = true;
  b.receive = (message) => (swallow ? Promise.resolve() : originalReceive(message));
  a.pc.failIce();
  await within(
    waitFor(() => a.state === 'failed' && b.state === 'failed', 'failed'),
    'failed',
  );
  swallow = false;
  // Only the initiator's owner redials; the responder auto-follows the offer
  // (its waitOpen() rejects while it is still 'failed' — by design, the
  // owner retries — so wait for it to leave that state first).
  a.redial();
  await within(
    waitFor(() => b.state === 'reconnecting' || b.state === 'connected', 'b follows'),
    'b follows',
  );
  await bothOpen(a, b);
  assert.strictEqual(b.state, 'connected');
  assert.strictEqual(b.redial(), false);
});

test('rtc link: the connect timeout fails a dial nobody answers', async (t) => {
  const fake = createFakeRtc();
  const adapter = fake.adapter;
  t.after(() => fake.world.close());
  const a = new RtcLink({ localId: 'a', remoteId: 'b', adapter, signal() {}, log: quiet, connectTimeout: 30 });
  a.start();
  const waiting = a.waitOpen();
  await assert.rejects(waiting, /connect timeout/);
  assert.strictEqual(a.state, 'failed');
  // No timer: the dial waits forever (the owner's connectTimeout decides).
  const b = new RtcLink({ localId: 'a', remoteId: 'b', adapter, signal() {}, log: quiet, connectTimeout: 0 });
  b.start();
  await timers.setTimeout(40);
  assert.strictEqual(b.state, 'connecting');
  b.close();
});

test('rtc link: close() tells the peer, closes the pc, emits close once; the peer closes without replying', async (t) => {
  const fake = createFakeRtc();
  const { a, b, relay } = wire(t, fake);
  a.start();
  b.start();
  await bothOpen(a, b);
  const closes = { a: 0, b: 0 };
  a.on('close', () => closes.a++);
  b.on('close', () => closes.b++);
  const pc = a.pc;
  a.close();
  assert.strictEqual(a.state, 'closed');
  assert.strictEqual(a.pc, null);
  assert.strictEqual(pc.connectionState, 'closed');
  await within(
    waitFor(() => b.state === 'closed', 'b closed by the goodbye'),
    'b closed',
  );
  await timers.setTimeout(5);
  assert.strictEqual(closes.a, 1);
  assert.strictEqual(closes.b, 1);
  const goodbyes = relay.log.filter(([, type]) => type === 'close');
  assert.deepStrictEqual(goodbyes, [['a', 'close']], 'the peer does not answer a goodbye with a goodbye');
  a.close(); // idempotent
  assert.strictEqual(closes.a, 1);
  await assert.rejects(a.waitOpen(), /closed/);
  assert.strictEqual(a.redial(), false);
  await a.receive({ type: 'description', description: { type: 'offer', sdp: 'x' } }); // ignored after close
});

test('rtc link: a channel closed by the peer without a goodbye fails the link', async (t) => {
  const fake = createFakeRtc();
  const { a, b } = wire(t, fake);
  a.start();
  b.start();
  await bothOpen(a, b);
  const which = [];
  a.on('channel-close', (e) => which.push(e.which));
  // b's pc dies abruptly (tab crash): no signaling, channels just close.
  b.pc.close();
  await within(
    waitFor(() => a.state === 'failed', 'a failed'),
    'a failed',
  );
  assert.ok(which.length >= 1);
  assert.ok(['client', 'host'].includes(which[0]));
});

test('rtc link: waitOpen before start rejects; unknown and malformed signals are ignored', async (t) => {
  const fake = createFakeRtc();
  t.after(() => fake.world.close());
  const warnings = [];
  const log = { ...quiet, warn: (entry) => warnings.push(entry.event) };
  const a = new RtcLink({ localId: 'a', remoteId: 'b', adapter: fake.adapter, signal() {}, log });
  await assert.rejects(a.waitOpen(), /not started/);
  await a.receive(null);
  await a.receive({ type: 'nonsense' });
  await a.receive({ type: 'description', description: 7 });
  await a.receive({ type: 'candidate', candidate: null }); // no pc yet: dropped
  assert.deepStrictEqual(warnings, ['rtc.signal.malformed', 'rtc.signal.unknown', 'rtc.signal.malformed']);
  // An offer before start() on a link that never dialled is ignored.
  await a.receive({ type: 'description', description: { type: 'offer', sdp: 'v=0 fake pc1' } });
  assert.strictEqual(a.state, 'new');
});

test('rtc link: a signaler that throws synchronously or rejects is an error event, not a crash', async (t) => {
  const fake = createFakeRtc();
  t.after(() => fake.world.close());
  const errors = [];
  const thrower = new RtcLink({
    localId: 'a',
    remoteId: 'b',
    adapter: fake.adapter,
    log: quiet,
    signal() {
      throw new Error('sync boom');
    },
  });
  thrower.on('error', (e) => errors.push(e.message));
  thrower.start();
  await within(
    waitFor(() => errors.length >= 1, 'error'),
    'error',
  );
  assert.strictEqual(errors[0], 'sync boom');
  thrower.close();
  // Without an error listener the failure is logged, never thrown.
  const logged = [];
  const silent = new RtcLink({
    localId: 'a',
    remoteId: 'b',
    adapter: fake.adapter,
    log: { ...quiet, error: (entry) => logged.push(entry.origin) },
    signal: () => Promise.reject(new Error('async boom')),
  });
  silent.start();
  await within(
    waitFor(() => logged.length >= 1, 'logged'),
    'logged',
  );
  assert.strictEqual(logged[0], 'signal');
  silent.close();
});

test('rtc link: restart() without restartIce falls back to an iceRestart offer; a responder may restart too', async (t) => {
  const fake = createFakeRtc();
  const { a, b } = wire(t, fake);
  a.start();
  b.start();
  await bothOpen(a, b);
  // Responder-initiated restart: it offers itself (only for a restart it asked for).
  b.pc.restartIce = undefined;
  b.restart();
  await within(
    waitFor(() => b.pc.localDescription?.sdp.includes('ice-restart'), 'iceRestart offer'),
    'iceRestart offer',
  );
  await within(
    waitFor(() => a.pc.connectionState === 'connected' && b.pc.connectionState === 'connected', 'still connected'),
    'connected',
  );
  assert.strictEqual(b.state, 'connected');
  // restart() on a closed link is a no-op.
  b.close();
  b.restart();
  assert.strictEqual(b.state, 'closed');
});

// ---- the defensive branches: what a real implementation may throw or
// report, and what an owner may do at an awkward moment.

test('rtc link: ids are exposed, a bad adapter product and a bad remoteId are refused', () => {
  const fake = createFakeRtc();
  const link = new RtcLink({ localId: 'a', remoteId: 'b', adapter: fake.adapter, signal() {}, log: quiet });
  assert.strictEqual(link.localId, 'a');
  assert.strictEqual(link.remoteId, 'b');
  assert.throws(() => new RtcLink({ localId: 'a', remoteId: '', adapter: fake.adapter, signal() {} }), TypeError);
  const broken = new RtcLink({
    localId: 'a',
    remoteId: 'b',
    adapter: { createPeerConnection: () => ({}) },
    signal() {},
    log: quiet,
  });
  assert.throws(() => broken.start(), /did not return a peer connection/);
  fake.world.close();
});

test('rtc link: a candidate without toJSON is serialized by hand, usernameFragment included', async (t) => {
  const fake = createFakeRtc();
  const { a, b, relay } = wire(t, fake);
  a.start();
  b.start();
  await bothOpen(a, b);
  // Synthetic icecandidate events straight from the pc, plain objects.
  a.pc.dispatchEvent(
    Object.assign(new Event('icecandidate'), {
      candidate: { candidate: 'candidate:plain 1 udp 1 192.0.2.2 2 typ host', sdpMid: '0', usernameFragment: 'ufrag' },
    }),
  );
  a.pc.dispatchEvent(
    Object.assign(new Event('icecandidate'), { candidate: { candidate: 'candidate:plain2', sdpMid: '0' } }),
  );
  const find = (text) => b.pc.remoteCandidates.find((c) => c.candidate.startsWith(text));
  await within(
    waitFor(() => find('candidate:plain ') && find('candidate:plain2'), 'both candidates delivered'),
    'delivered',
  );
  const withUfrag = find('candidate:plain ');
  const without = find('candidate:plain2');
  assert.deepStrictEqual(withUfrag, {
    candidate: 'candidate:plain 1 udp 1 192.0.2.2 2 typ host',
    sdpMid: '0',
    sdpMLineIndex: null,
    usernameFragment: 'ufrag',
  });
  assert.deepStrictEqual(without, { candidate: 'candidate:plain2', sdpMid: '0', sdpMLineIndex: null });
  assert.ok(relay.log.some(([from, type]) => from === 'a' && type === 'candidate'));
});

test('rtc link: implementation errors surface as error events with their origin', async (t) => {
  const fake = createFakeRtc();
  const { a, b } = wire(t, fake);
  const errors = [];
  a.on('error', (e) => errors.push(e.message));
  a.start();
  b.start();
  await bothOpen(a, b);
  // An answer while stable: setRemoteDescription refuses it.
  await a.receive({ type: 'description', description: { type: 'answer', sdp: `v=0 fake ${b.pc.id} a=99` } });
  assert.ok(
    errors.some((m) => /setRemoteDescription\(answer\)/.test(m)),
    errors.join('|'),
  );
  // A candidate the implementation rejects, for an offer that was NOT ignored.
  a.pc.addIceCandidate = async () => {
    throw new Error('bad candidate');
  };
  await a.receive({ type: 'candidate', candidate: { candidate: 'candidate:x', sdpMid: '0' } });
  assert.ok(errors.includes('bad candidate'));
  // End-of-candidates refused by an implementation is not an error.
  errors.length = 0;
  await a.receive({ type: 'candidate', candidate: null });
  assert.deepStrictEqual(errors, []);
  // A failing setLocalDescription during a restart offer.
  a.pc.setLocalDescription = async () => {
    throw new Error('sld failed');
  };
  a.restart();
  await within(
    waitFor(() => errors.includes('sld failed'), 'offer error'),
    'offer error',
  );
  // A channel error event is relayed with the channel's error.
  a.hostChannel.dispatchEvent(Object.assign(new Event('error'), { error: new Error('channel broke') }));
  a.hostChannel.dispatchEvent(new Event('error'));
  assert.ok(errors.includes('channel broke'));
  assert.ok(errors.includes('data channel error'));
});

test('rtc link: an oversize send kills the channel and the link fails', async (t) => {
  const fake = createFakeRtc({ maxMessageSize: 64 });
  const { a, b } = wire(t, fake);
  const errors = [];
  a.on('error', (e) => errors.push(e.message));
  a.start();
  b.start();
  await bothOpen(a, b);
  assert.strictEqual(a.maxMessageSize, 64);
  a.clientChannel.send(new Uint8Array(65));
  await within(
    waitFor(() => a.state === 'failed', 'failed'),
    'failed',
  );
  assert.ok(errors.some((m) => /too large/.test(m)));
});

test('rtc link: close() while an offer or an answer is in flight is safe', async (t) => {
  const fake = createFakeRtc();
  t.after(() => fake.world.close());
  const sent = [];
  const a = new RtcLink({
    localId: 'a',
    remoteId: 'b',
    adapter: fake.adapter,
    log: quiet,
    signal: (m) => void sent.push(m),
  });
  const errors = [];
  a.on('error', (e) => errors.push(e));
  a.start();
  a.close(); // setLocalDescription() is still pending inside #offer
  await timers.setTimeout(5);
  assert.deepStrictEqual(
    sent.map((m) => m.type),
    ['close'],
    'the in-flight offer was never sent after close()',
  );
  // The answering side: close during setRemoteDescription/setLocalDescription.
  const b = new RtcLink({
    localId: 'b',
    remoteId: 'a',
    adapter: fake.adapter,
    log: quiet,
    signal: (m) => void sent.push(m),
  });
  b.start();
  const offerer = fake.adapter.createPeerConnection();
  offerer.createDataChannel('x', { negotiated: true, id: 0 });
  const offer = await offerer.createOffer();
  const receiving = b.receive({ type: 'description', description: offer });
  b.close();
  await receiving;
  assert.deepStrictEqual(errors, []);
  assert.strictEqual(b.state, 'closed');
  offerer.close();
});

test('rtc link: a pc whose close() throws, a connectionState of closed, and restartTimeout 0', async (t) => {
  const fake = createFakeRtc();
  const { a, b } = wire(t, fake, { restartTimeout: 0 });
  a.start();
  b.start();
  await bothOpen(a, b);
  // restartTimeout 0: an ICE failure starts a restart with no deadline —
  // and the relay is up, so it reconnects on its own.
  a.pc.failIce();
  await timers.setTimeout(10);
  assert.notStrictEqual(a.state, 'failed', 'no restart timer was armed');
  await within(
    waitFor(() => a.pc.connectionState === 'connected', 'reconnected'),
    'reconnected',
  );
  // The implementation reports 'closed' on its own (no channel close first).
  Object.defineProperty(b.pc, 'connectionState', { value: 'closed', configurable: true });
  b.pc.dispatchEvent(new Event('connectionstatechange'));
  assert.strictEqual(b.state, 'failed');
  // teardown survives an implementation whose close() throws.
  await within(
    waitFor(() => a.state === 'failed', "a failed after b's teardown closed the channels"),
    'a failed',
  );
  a.redial();
  const pc = a.pc;
  const realClose = pc.close.bind(pc);
  pc.close = () => {
    throw new Error('already closed');
  };
  a.close();
  assert.strictEqual(a.state, 'closed');
  pc.close = realClose; // the fake world's teardown closes it for real
});

test('rtc link: a responder that never asked for a restart ignores negotiationneeded', async (t) => {
  const fake = createFakeRtc();
  const { a, b, relay } = wire(t, fake);
  a.start();
  b.start();
  await bothOpen(a, b);
  relay.log.length = 0;
  b.pc.dispatchEvent(new Event('negotiationneeded'));
  await timers.setTimeout(5);
  const offers = () => relay.log.filter(([, type]) => type === 'description');
  assert.deepStrictEqual(offers(), [], 'no offer from the responder');
  a.pc.dispatchEvent(new Event('negotiationneeded'));
  await within(
    waitFor(() => relay.log.some(([from, type]) => from === 'a' && type === 'description'), 'initiator re-offers'),
    'initiator re-offers',
  );
});
