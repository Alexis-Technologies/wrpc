'use strict';

// Shared contract suite for the WebRTC port (src/webrtc/port.js): what the
// transport assumes of any RtcAdapter. Runs against the in-repo fake on
// every `pnpm test` and, skip-guarded, against a real Node implementation
// (tests/webrtc/node-datachannel.integration.test.js). Not a *.test.js.
//
//   harness.createAdapter()  -> an RtcAdapter (fresh world per call)
//   harness.configuration?   -> the RTCConfiguration to pass (default {})
//
// The suite does its own loopback signaling — offer/answer through
// setLocalDescription() and trickle candidates both ways — so an
// implementation is exercised through the exact calls the link makes.

const assert = require('node:assert');
const timers = require('node:timers/promises');

const { isRtcAdapter, isRtcPeerConnection, isRtcDataChannel } = require('../../src/webrtc/port.js');

const CLIENT_ID = 0;
const HOST_ID = 1;

const within = (promise, label, ms = 5000) =>
  Promise.race([
    promise,
    timers.setTimeout(ms).then(() => {
      throw new Error(`timed out waiting for ${label}`);
    }),
  ]);

const once = (target, type, predicate = () => true) =>
  new Promise((resolve) => {
    const listener = (event) => {
      if (!predicate(event)) return;
      target.removeEventListener(type, listener);
      resolve(event);
    };
    target.addEventListener(type, listener);
  });

const waitFor = async (predicate, label, ms = 5000) => {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await timers.setTimeout(5);
  }
};

const opened = (channel) =>
  channel.readyState === 'open' ? Promise.resolve() : once(channel, 'open').then(() => undefined);

// Trickle candidates from `from` to `to`, holding them until `to` has a
// remote description (addIceCandidate before one is an error per spec).
const trickle = (from, to) => {
  const queue = [];
  let flushing = null;
  const flush = async () => {
    while (queue.length > 0 && to.remoteDescription) {
      const candidate = queue.shift();
      await to.addIceCandidate(candidate);
    }
  };
  from.addEventListener('icecandidate', ({ candidate }) => {
    queue.push(candidate === null ? null : (candidate.toJSON?.() ?? candidate));
    flushing = (flushing ?? Promise.resolve()).then(flush);
  });
  return () => (flushing ?? Promise.resolve()).then(flush);
};

const connectPair = async (a, b) => {
  const flushA = trickle(a, b);
  const flushB = trickle(b, a);
  await a.setLocalDescription();
  await b.setRemoteDescription(a.localDescription);
  await b.setLocalDescription();
  await a.setRemoteDescription(b.localDescription);
  await flushA();
  await flushB();
};

const runRtcPortContract = async (harness, t) => {
  const { createAdapter, configuration = {} } = typeof harness === 'function' ? { createAdapter: harness } : harness;

  // Two peers with both negotiated channels created BEFORE the offer (an
  // m=application section needs at least one channel to exist), connected
  // through loopback signaling. Teardown is a sub.after hook so a failed
  // assertion never leaves a native peer connection behind.
  const pair = async (sub) => {
    const adapter = createAdapter();
    const a = adapter.createPeerConnection(configuration);
    const b = adapter.createPeerConnection(configuration);
    sub.after(() => {
      a.close();
      b.close();
      adapter.world?.close?.();
    });
    const channels = {
      a: {
        client: a.createDataChannel('wrpc', { negotiated: true, id: CLIENT_ID }),
        host: a.createDataChannel('wrpc', { negotiated: true, id: HOST_ID }),
      },
      b: {
        client: b.createDataChannel('wrpc', { negotiated: true, id: CLIENT_ID }),
        host: b.createDataChannel('wrpc', { negotiated: true, id: HOST_ID }),
      },
    };
    for (const side of [channels.a, channels.b]) {
      side.client.binaryType = 'arraybuffer';
      side.host.binaryType = 'arraybuffer';
    }
    return { adapter, a, b, channels };
  };

  const connected = async (sub) => {
    const booted = await pair(sub);
    await connectPair(booted.a, booted.b);
    const { a: ca, b: cb } = booted.channels;
    await within(Promise.all([opened(ca.client), opened(ca.host), opened(cb.client), opened(cb.host)]), 'channels');
    return booted;
  };

  await t.test('adapter and peer connection satisfy the port shape', async (sub) => {
    const adapter = createAdapter();
    assert.strictEqual(isRtcAdapter(adapter), true);
    const fresh = adapter.createPeerConnection(configuration);
    sub.after(() => {
      fresh.close();
      adapter.world?.close?.();
    });
    assert.strictEqual(isRtcPeerConnection(fresh), true);
    assert.strictEqual(fresh.signalingState, 'stable');
    assert.strictEqual(typeof fresh.connectionState, 'string');
    assert.strictEqual(typeof fresh.iceConnectionState, 'string');
    // Creating a channel MAY start negotiation on its own (libdatachannel
    // does; a browser waits for negotiationneeded) — the link copes with
    // both, so the contract only pins that the state stays a known one.
    fresh.createDataChannel('wrpc', { negotiated: true, id: CLIENT_ID });
    assert.ok(['stable', 'have-local-offer'].includes(fresh.signalingState), fresh.signalingState);
  });

  await t.test('negotiated channels open on both ends after one offer/answer', async (sub) => {
    const { a, b, channels } = await connected(sub);
    for (const channel of [channels.a.client, channels.a.host, channels.b.client, channels.b.host]) {
      assert.strictEqual(isRtcDataChannel(channel), true);
      assert.strictEqual(channel.readyState, 'open');
    }
    assert.strictEqual(channels.a.client.id, CLIENT_ID);
    assert.strictEqual(channels.b.host.id, HOST_ID);
    await within(
      waitFor(() => a.connectionState === 'connected' && b.connectionState === 'connected', 'connected'),
      'connectionState',
    );
    assert.strictEqual(a.signalingState, 'stable');
    assert.strictEqual(b.signalingState, 'stable');
  });

  await t.test('sctp reports a positive maxMessageSize once connected, or is null', async (sub) => {
    const { a } = await connected(sub);
    if (a.sctp === null) return;
    assert.strictEqual(typeof a.sctp.maxMessageSize, 'number');
    assert.ok(a.sctp.maxMessageSize > 0, 'maxMessageSize must be positive');
  });

  await t.test('text, ArrayBuffer and Uint8Array messages arrive in order on the paired channel', async (sub) => {
    const { channels } = await connected(sub);
    const received = [];
    channels.b.client.addEventListener('message', ({ data }) => received.push(data));
    channels.a.client.send('one');
    channels.a.client.send(new Uint8Array([2, 2]).buffer);
    channels.a.client.send(new Uint8Array([3, 3, 3]));
    await within(
      waitFor(() => received.length === 3, 'three messages'),
      'messages',
    );
    assert.strictEqual(received[0], 'one');
    assert.ok(received[1] instanceof ArrayBuffer, 'binaryType arraybuffer delivers an ArrayBuffer');
    assert.deepStrictEqual(new Uint8Array(received[1]), new Uint8Array([2, 2]));
    assert.deepStrictEqual(new Uint8Array(received[2]), new Uint8Array([3, 3, 3]));
  });

  await t.test('the other channel is independent: host traffic never crosses into client', async (sub) => {
    const { channels } = await connected(sub);
    const onClient = [];
    const onHost = [];
    channels.b.client.addEventListener('message', ({ data }) => onClient.push(data));
    channels.b.host.addEventListener('message', ({ data }) => onHost.push(data));
    channels.a.host.send('h');
    channels.a.client.send('c');
    await within(
      waitFor(() => onClient.length + onHost.length === 2, 'two messages'),
      'messages',
    );
    assert.deepStrictEqual(onClient, ['c']);
    assert.deepStrictEqual(onHost, ['h']);
  });

  await t.test('bufferedAmount counts what is in flight and drains to zero', async (sub) => {
    const { channels } = await connected(sub);
    const payload = new Uint8Array(1024);
    channels.a.client.send(payload);
    assert.ok(channels.a.client.bufferedAmount >= 0);
    await within(
      waitFor(() => channels.a.client.bufferedAmount === 0, 'drain'),
      'bufferedAmount to drain',
    );
    assert.strictEqual(typeof channels.a.client.bufferedAmountLowThreshold, 'number');
  });

  await t.test('send() on a channel that is not open throws', async (sub) => {
    const { channels } = await pair(sub);
    assert.strictEqual(channels.a.client.readyState, 'connecting');
    assert.throws(() => channels.a.client.send('early'));
  });

  await t.test('closing a channel closes its pair; closing the connection closes everything', async (sub) => {
    const { a, b, channels } = await connected(sub);
    const bClientClosed = once(channels.b.client, 'close');
    channels.a.client.close();
    await within(bClientClosed, "the pair's close");
    assert.strictEqual(channels.b.client.readyState, 'closed');
    assert.strictEqual(channels.b.host.readyState, 'open', 'the other channel is untouched');

    const bHostClosed = once(channels.b.host, 'close');
    a.close();
    await within(bHostClosed, "the peer's channel close after pc.close()");
    // signalingState after close() is 'closed' per spec but not on every
    // implementation (libdatachannel keeps 'stable'); connectionState is
    // what the link reads, and every channel must be gone.
    assert.strictEqual(a.connectionState, 'closed');
    await within(
      waitFor(() => channels.a.host.readyState === 'closed', 'own channels closed'),
      'own channels closed',
    );
    assert.throws(() => a.createDataChannel('late', { negotiated: true, id: 7 }));
    b.close();
  });

  await t.test('addIceCandidate before a remote description rejects', async (sub) => {
    const { a } = await pair(sub);
    await assert.rejects(a.addIceCandidate({ candidate: 'candidate:x 1 udp 1 192.0.2.9 1 typ host', sdpMid: '0' }));
  });
};

module.exports = { runRtcPortContract, connectPair, trickle, once, opened, waitFor, within, CLIENT_ID, HOST_ID };
