'use strict';

// A connected pair of raw data channels on two fake peer connections — the
// level UNDER RtcLink: the offer/answer exchange and the candidate relay
// are done by hand here, which is exactly the signaling an application on
// that level owns. What the raw-channel tests of the transports and
// attachChannel (src/webrtc/index.js) run over.
//
// Not a *.test.js — a helper for tests/webrtc/*.test.js and tests/rpc/.

const { FakeWorld, FakePeerConnection } = require('./fakeRtc.js');
const { waitFor, within } = require('./portContract.js');

// Two peer connections in one world, a negotiated channel with the same id
// on each, described to each other: resolves once both channels are open.
// `world` may be shared across calls (a factory-driven reconnect builds
// pair after pair in one world) — pass the one from a previous result.
// With `deferred: true` the channels come back still 'connecting' and the
// exchange runs when the returned `connect()` is called.
const rawChannelPair = async (
  t,
  { id = 0, label = 'wrpc', world: shared = null, deferred = false, fake: fakeOptions } = {},
) => {
  const world = shared ?? new FakeWorld(fakeOptions);
  if (!shared) t.after(() => world.close());
  const a = new FakePeerConnection(world, {});
  const b = new FakePeerConnection(world, {});
  // Trickle ICE by hand. A candidate may arrive before the other side has
  // its remote description; a real application buffers, this one drops
  // (the fake connects without them).
  a.addEventListener('icecandidate', ({ candidate }) => void b.addIceCandidate(candidate).catch(() => {}));
  b.addEventListener('icecandidate', ({ candidate }) => void a.addIceCandidate(candidate).catch(() => {}));
  const channels = {
    a: a.createDataChannel(label, { negotiated: true, id, ordered: true }),
    b: b.createDataChannel(label, { negotiated: true, id, ordered: true }),
  };
  const connect = async () => {
    const offer = await a.createOffer();
    await a.setLocalDescription(offer);
    await b.setRemoteDescription(offer);
    const answer = await b.createAnswer();
    await b.setLocalDescription(answer);
    await a.setRemoteDescription(answer);
    await within(
      waitFor(() => channels.a.readyState === 'open' && channels.b.readyState === 'open', 'raw channels open'),
      'raw channels',
    );
  };
  if (!deferred) await connect();
  return { a: channels.a, b: channels.b, pcs: { a, b }, world, connect };
};

module.exports = { rawChannelPair };
