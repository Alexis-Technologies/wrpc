'use strict';

// The port contract against a REAL Node WebRTC implementation. Guarded on
// WRPC_RTC=node-datachannel (or WRPC_RTC_MODULE=<path to a W3C-shaped
// module>) so `pnpm test` stays self-contained: node-datachannel is a
// native devDependency used only here, the adapter itself requires nothing
// and duck-types whatever it is handed — the same rule as ioredis in
// tests/scaling/redis.integration.test.js. Run by hand:
//
//   WRPC_RTC=node-datachannel node --test tests/webrtc/node-datachannel.integration.test.js

const { test } = require('node:test');
const assert = require('node:assert');

const { createW3cAdapter } = require('../../src/webrtc/port.js');
const { runRtcPortContract } = require('./portContract.js');

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

test(
  'webrtc port: a real implementation satisfies the shared contract',
  { skip: !w3c && 'set WRPC_RTC=node-datachannel' },
  async (t) => {
    // Loopback only: no STUN, so the pair connects over host candidates.
    await runRtcPortContract({ createAdapter: () => createW3cAdapter(w3c), configuration: { iceServers: [] } }, t);
    // node-datachannel keeps a worker thread alive; release it so the runner exits.
    t.after(() => {
      try {
        require('node-datachannel').cleanup();
      } catch {
        // not node-datachannel, nothing to release
      }
    });
  },
);

test(
  'webrtc raw channel: attachChannel over a real implementation',
  { skip: !w3c && 'set WRPC_RTC=node-datachannel' },
  async (t) => {
    const { RpcServer } = require('../../src/rpc/core.js');
    const { defineRouter, procedure } = require('../../src/rpc/router.js');
    const { WrpcClient } = require('../../src/client/core.js');
    const { attachChannel } = require('../../src/webrtc/index.js');
    const { connectPair, opened, within } = require('./portContract.js');
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
  },
);
