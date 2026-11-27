'use strict';

// The port contract against a REAL Node WebRTC implementation. Guarded on
// WRPC_RTC=node-datachannel (or WRPC_RTC_MODULE=<path to a W3C-shaped
// module>) so `pnpm test` stays self-contained: node-datachannel is a
// native devDependency used only here, the adapter itself requires nothing
// and duck-types whatever it is handed — the same rule as ioredis in
// tests/scaling/redis.integration.test.js. Run by hand:
//
//   WRPC_RTC=node-datachannel node --test tests/webrtc/node-datachannel.integration.test.js

const { describe, it, after } = require('node:test');
const assert = require('node:assert/strict');

const { createW3cAdapter } = require('../../src/webrtc/port.js');
const { runRtcPortContract } = require('./port-contract.js');

const MODULE =
  process.env.WRPC_RTC_MODULE ?? (process.env.WRPC_RTC === 'node-datachannel' ? 'node-datachannel/polyfill' : null);

let w3c = null;
if (MODULE) {
  try {
    w3c = require(MODULE);
  } catch {
    w3c = null;
  }
}
const skip = !w3c && 'set WRPC_RTC=node-datachannel';

describe('webrtc port', () => {
  describe('the shared contract over a real implementation', () => {
    // Loopback only: no STUN, so the pair connects over host candidates.
    runRtcPortContract({ createAdapter: () => createW3cAdapter(w3c), configuration: { iceServers: [] } }, { skip });
    // node-datachannel keeps a worker thread alive; release it so the runner exits.
    after(() => {
      if (!w3c) return;
      try {
        require('node-datachannel').cleanup();
      } catch {
        // not node-datachannel, nothing to release
      }
    });
  });
});

describe('webrtc assertions', () => {
  it('should have a real implementation declare a certificate fingerprint in its SDP', { skip }, async (t) => {
    const { sdpFingerprint, normalizeFingerprint } = require('../../src/webrtc/assertions.js');
    const adapter = createW3cAdapter(w3c);
    const pc = adapter.createPeerConnection({ iceServers: [] });
    t.after(() => pc.close());
    pc.createDataChannel('probe', { negotiated: true, id: 0 });
    const offer = await pc.createOffer();
    // What the peer binds its assertion to: the DTLS fingerprint, present in
    // every local description libdatachannel produces, sha-256 by default.
    const fingerprint = sdpFingerprint(offer.sdp);
    assert.strictEqual(typeof fingerprint, 'string');
    assert.match(fingerprint, /^sha-256 (?:[0-9A-F]{2}:){31}[0-9A-F]{2}$/);
    assert.strictEqual(normalizeFingerprint(fingerprint), fingerprint);
  });
});

describe('webrtc raw channel', () => {
  it('should carry attachChannel over a real implementation', { skip }, async (t) => {
    const { RpcServer } = require('../../src/rpc/core.js');
    const { defineRouter, procedure } = require('../../src/rpc/router.js');
    const { WrpcClient } = require('../../src/client/core.js');
    const { attachChannel } = require('../../src/webrtc/index.js');
    const { connectPair, opened, within } = require('./port-contract.js');
    const adapter = createW3cAdapter(w3c);
    const a = adapter.createPeerConnection({ iceServers: [] });
    const b = adapter.createPeerConnection({ iceServers: [] });
    t.after(() => {
      a.close();
      b.close();
    });
    const channels = {
      a: a.createDataChannel('wrpc', { negotiated: true, id: 0 }),
      b: b.createDataChannel('wrpc', { negotiated: true, id: 0 }),
    };
    await connectPair(a, b);
    await within(Promise.all([opened(channels.a), opened(channels.b)]), 'channels');
    const router = defineRouter({
      calc: { add: procedure({ access: 'public', handler: async (_ctx, { x, y }) => x + y }) },
    });
    const rpc = new RpcServer({ router, logger: false });
    t.after(() => rpc.close());
    const attached = attachChannel(rpc, channels.b, { peer: 'real' });
    const client = await WrpcClient.connect('webrtc:server', {
      transport: 'webrtc',
      channel: channels.a,
      heartbeat: false,
      reconnect: false,
    });
    t.after(() => client.close());
    await client.load('calc');
    assert.strictEqual(await client.api.calc.add({ x: 2, y: 2 }), 4);
    assert.strictEqual(attached.transportKind, 'webrtc');
  });
});
