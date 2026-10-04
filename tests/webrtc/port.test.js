'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const { isRtcAdapter, isRtcPeerConnection, isRtcDataChannel, createW3cAdapter } = require('../../src/webrtc/port.js');
const { createFakeRtc } = require('./fakeRtc.js');
const { runRtcPortContract, connectPair, opened, waitFor, within, once } = require('./portContract.js');

test('webrtc port: the fake satisfies the shared contract', async (t) => {
  await runRtcPortContract(() => createFakeRtc().adapter, t);
});

test('webrtc port: validators are structural', () => {
  assert.strictEqual(isRtcAdapter({ createPeerConnection() {} }), true);
  assert.strictEqual(isRtcAdapter({}), false);
  assert.strictEqual(isRtcAdapter(null), false);
  assert.strictEqual(
    isRtcAdapter(() => {}),
    false,
  );
  const { adapter } = createFakeRtc();
  const pc = adapter.createPeerConnection({});
  assert.strictEqual(isRtcPeerConnection(pc), true);
  assert.strictEqual(isRtcPeerConnection({ ...pc, signalingState: 7 }), false);
  assert.strictEqual(isRtcPeerConnection({}), false);
  assert.strictEqual(isRtcPeerConnection(null), false);
  const channel = pc.createDataChannel('x', { negotiated: true, id: 0 });
  assert.strictEqual(isRtcDataChannel(channel), true);
  assert.strictEqual(isRtcDataChannel({ send() {}, close() {} }), false);
  assert.strictEqual(isRtcDataChannel(null), false);
  pc.close();
});

test('webrtc port: createW3cAdapter wraps any W3C-shaped constructor and refuses the rest', () => {
  assert.throws(() => createW3cAdapter({}), /RTCPeerConnection constructor is required/);
  assert.throws(() => createW3cAdapter(), TypeError); // globalThis has none in node
  const { world } = createFakeRtc();
  const { FakePeerConnection } = require('./fakeRtc.js');
  class RTCPeerConnection extends FakePeerConnection {
    constructor(configuration) {
      super(world, configuration);
    }
  }
  const adapter = createW3cAdapter({ RTCPeerConnection });
  assert.strictEqual(adapter.name, 'w3c');
  assert.strictEqual(isRtcAdapter(adapter), true);
  const pc = adapter.createPeerConnection({ iceServers: [] });
  assert.strictEqual(isRtcPeerConnection(pc), true);
  assert.deepStrictEqual(pc.getConfiguration(), { iceServers: [] });
  pc.close();
  // A constructor producing something else is refused at creation time.
  const broken = createW3cAdapter({ RTCPeerConnection: class {} });
  assert.throws(() => broken.createPeerConnection(), /RTCPeerConnection-shaped/);
});

// Fake-specific behaviours the link and transport tests will lean on.
test('webrtc fake: perfect-negotiation collision rolls the polite side back', async (t) => {
  const { adapter, world } = createFakeRtc();
  t.after(() => world.close());
  const a = adapter.createPeerConnection();
  const b = adapter.createPeerConnection();
  a.createDataChannel('wrpc', { negotiated: true, id: 0 });
  b.createDataChannel('wrpc', { negotiated: true, id: 0 });
  // Both offer at once.
  await a.setLocalDescription();
  await b.setLocalDescription();
  assert.strictEqual(a.signalingState, 'have-local-offer');
  assert.strictEqual(b.signalingState, 'have-local-offer');
  // Impolite a would ignore b's offer; polite b takes a's — implicit rollback.
  await b.setRemoteDescription(a.localDescription);
  assert.strictEqual(b.signalingState, 'have-remote-offer');
  await b.setLocalDescription();
  await a.setRemoteDescription(b.localDescription);
  assert.strictEqual(a.signalingState, 'stable');
  assert.strictEqual(b.signalingState, 'stable');
  await within(
    waitFor(() => a.connectionState === 'connected' && b.connectionState === 'connected', 'connected'),
    'connected',
  );
  // An answer with no local offer is refused, like a browser does.
  await assert.rejects(a.setRemoteDescription(b.localDescription), /InvalidStateError|have-local-offer/);
});

test('webrtc fake: a message over maxMessageSize kills the channel; ICE failure and restart', async (t) => {
  const { adapter, world } = createFakeRtc({ maxMessageSize: 16 });
  t.after(() => world.close());
  const a = adapter.createPeerConnection();
  const b = adapter.createPeerConnection();
  const ca = a.createDataChannel('wrpc', { negotiated: true, id: 0 });
  const cb = b.createDataChannel('wrpc', { negotiated: true, id: 0 });
  await connectPair(a, b);
  await within(Promise.all([opened(ca), opened(cb)]), 'open');
  assert.strictEqual(a.sctp.maxMessageSize, 16);

  // Severed link: sends are eaten, states report failure on both ends.
  const failed = [once(a, 'iceconnectionstatechange'), once(b, 'connectionstatechange')];
  a.failIce();
  await within(Promise.all(failed), 'failure events');
  assert.strictEqual(a.iceConnectionState, 'failed');
  assert.strictEqual(b.connectionState, 'failed');
  const got = [];
  cb.addEventListener('message', ({ data }) => got.push(data));
  ca.send('lost');
  await within(
    waitFor(() => ca.bufferedAmount === 0, 'drain'),
    'drain',
  );
  assert.deepStrictEqual(got, []);

  // An ICE restart is a renegotiation; the fake reconnects on the new answer.
  const needed = once(a, 'negotiationneeded');
  a.restartIce();
  await within(needed, 'negotiationneeded');
  assert.strictEqual(a.restarts, 1);
  await connectPair(a, b);
  await within(
    waitFor(() => a.connectionState === 'connected' && b.connectionState === 'connected', 'reconnected'),
    'reconnected',
  );
  ca.send('back');
  await within(
    waitFor(() => got.length === 1, 'delivery after restart'),
    'delivery',
  );
  assert.deepStrictEqual(got, ['back']);

  // Oversize: error + close on the sender, close on the pair.
  const errored = once(ca, 'error');
  const closed = once(cb, 'close');
  ca.send(new Uint8Array(17));
  await within(Promise.all([errored, closed]), 'oversize close');
  assert.strictEqual(ca.readyState, 'closed');
  assert.strictEqual(cb.readyState, 'closed');
});

test('webrtc fake: bufferedamountlow fires when the buffer drops under the threshold; blob is the default', async (t) => {
  const { adapter, world } = createFakeRtc({ latency: 2 });
  t.after(() => world.close());
  const a = adapter.createPeerConnection();
  const b = adapter.createPeerConnection();
  const ca = a.createDataChannel('wrpc', { negotiated: true, id: 0 });
  const cb = b.createDataChannel('wrpc', { negotiated: true, id: 0 });
  await connectPair(a, b);
  await within(Promise.all([opened(ca), opened(cb)]), 'open');
  assert.strictEqual(cb.binaryType, 'blob');
  const blobs = [];
  cb.addEventListener('message', ({ data }) => blobs.push(data));
  ca.bufferedAmountLowThreshold = 100;
  let low = 0;
  ca.addEventListener('bufferedamountlow', () => low++);
  ca.send(new Uint8Array(150));
  assert.strictEqual(ca.bufferedAmount, 150);
  await within(
    waitFor(() => low === 1, 'bufferedamountlow'),
    'bufferedamountlow',
  );
  assert.strictEqual(ca.bufferedAmount, 0);
  ca.send(new Uint8Array(50)); // never above the threshold: no event
  await within(
    waitFor(() => blobs.length === 2, 'both'),
    'both',
  );
  assert.strictEqual(low, 1);
  assert.ok(blobs[0] instanceof Blob, 'the browser default hands over a Blob');
  assert.strictEqual(blobs[0].size, 150);
  assert.strictEqual(world.pending, 0);
});

test('webrtc fake: closing one side shows up on the other as disconnected', async (t) => {
  const { adapter, world } = createFakeRtc();
  t.after(() => world.close());
  const a = adapter.createPeerConnection();
  const b = adapter.createPeerConnection();
  a.createDataChannel('wrpc', { negotiated: true, id: 0 });
  b.createDataChannel('wrpc', { negotiated: true, id: 0 });
  await connectPair(a, b);
  await within(
    waitFor(() => b.connectionState === 'connected', 'connected'),
    'connected',
  );
  const changed = once(b, 'connectionstatechange');
  a.close();
  await within(changed, 'disconnected');
  assert.strictEqual(b.connectionState, 'disconnected');
  assert.strictEqual(b.iceConnectionState, 'disconnected');
  assert.strictEqual(world.peers.has(a.id), false);
  a.close(); // idempotent
  // A channel created on a live but unlinked pc opens once the link is up.
  const c = adapter.createPeerConnection();
  const d = adapter.createPeerConnection();
  const late = c.createDataChannel('late', { negotiated: true, id: 5 });
  await connectPair(c, d);
  await within(
    waitFor(() => c.connectionState === 'connected', 'c connected'),
    'c connected',
  );
  assert.strictEqual(late.readyState, 'connecting', 'no pair on d yet');
  const dLate = d.createDataChannel('late', { negotiated: true, id: 5 });
  await within(Promise.all([opened(late), opened(dLate)]), 'late open');
  assert.throws(() => d.createDataChannel('dup', { negotiated: true, id: 5 }));
  assert.throws(() => d.createDataChannel('bad', { negotiated: true }), TypeError);
});
