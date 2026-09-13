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
