/**
 * Honest bundle-size measurement for the README "Bundle size" table.
 *
 * Every entry from package.json#exports is bundled the way a consumer's
 * bundler would (browser condition for the main entry, plain Node
 * resolution for the server-only subpaths), then reports:
 *   - raw:      bundled, un-minified
 *   - min:      fully minified (whitespace + identifier mangling + syntax,
 *               comments stripped)
 *   - min+gzip: the number that matters for shipping to browsers
 *
 * The Node-only subpaths (ws/engine/uws/fastify/express) never ship to a
 * browser — they are reported for visibility into how much each one pulls
 * in, not because their gzip size is a real shipping cost. Only the entries
 * that DO reach a browser carry a `budget` (min+gzip KB), and exceeding one
 * fails the run, which is what keeps CI honest about bundle growth.
 *
 * Run with `pnpm size`.
 */

const { gzipSync } = require('node:zlib');
const path = require('node:path');
const esbuild = require('esbuild');

const ROOT = path.join(__dirname, '..');

// One row per package.json#exports entry. `platform: 'browser'` is the only
// one that actually ships to a browser bundle (via the `browser` condition);
// everything else is Node-only and bundled with `platform: 'node'` so
// `require('node:...')` stays a real require instead of failing to resolve.
const ENTRIES = [
  // Raised 10 -> 11 deliberately (the ratchet's documented escape hatch):
  // the v1 hardening added client-side substance a browser genuinely ships —
  // in-flight rejection on disconnect, restore decoupling, the wrpc.v1
  // subprotocol offer, pluggable generateId, and synthesized answers for
  // failed HTTP batches.
  // Raised 11 -> 12 for the REST bridge: the client's REST leg for mapped
  // procedures (URL building, plain-result/wire-error handling, pluggable
  // querystring), error.details on WrpcError, plus the transport-fallback
  // and codec seams of the same release. 12 -> 13 for static introspection
  // (client.use() + the #scaffoldUnit extraction it shares with load());
  // the same raise absorbs the codec.rest client bytes of this release.
  // 13 -> 14 for the auth + metadata batch: the authenticate hook (the
  // pre-restore seam), single-flight refresh with its one-shot retry,
  // public client.call(), the connection-phase headers/meta options across
  // all four transports, and per-call meta (withMeta) — measured together
  // at ~+750 B min+gzip.
  // 14 -> 15 for the resilience batch: connectTimeout (the handshake race),
  // the reconnect stability window (stableAfter), refresh-aware subscribe
  // restore, the joined-run refresh guard, coded 408/503 rejections and the
  // shared failPackets settlement — measured together at ~+450 B min+gzip.
  // 15 -> 16: the worker proxy grew a SharedWorker leg (the `connect`
  // listener, the `url` option, port cleanup on `close`) with 16 bytes of
  // headroom left; the marketing line moved to "~15 KB" in the same change.
  // 16 -> 17 for the WebTransport client transport (`transport: 'wt'`, in
  // the base entry so the `['wt', 'ws']` fallback list needs no import) and
  // its stream framing — measured together at +1.8 KB min+gzip (16.9); the
  // connect-URL builder it shares with ws moved into the core in the same
  // change, so the ws leg paid nothing twice.
  // 17 -> 19 for the rest of WebTransport: unreliable events over datagrams
  // and binary streams on their own WebTransport streams (the stream mux,
  // capabilities negotiation) — measured together at +1.5 KB (18.4).
  // 19 -> 20 for the call-path work behind the paper's 26 % figure: the
  // bucketed deadline scheduler (one timer for every call in flight instead
  // of a setTimeout + three closures per call), the synchronous `callback`
  // fast path and the per-message `compress` option — +0.6 KB (measured
  // 19.0), earned by bench/browser/calls.js and bench/bench.js.
  { label: 'main entry — browser (@alexify/wrpc)', entry: 'browser.js', platform: 'browser', budget: 20 },
  { label: 'main entry — node (@alexify/wrpc)', entry: 'index.js', platform: 'node' },
  { label: 'websocket engine (@alexify/wrpc/ws)', entry: 'ws.js', platform: 'node' },
  { label: 'engine port (@alexify/wrpc/engine)', entry: 'engine.js', platform: 'node' },
  { label: 'uWebSockets.js adapter (@alexify/wrpc/uws)', entry: 'uws.js', platform: 'node' },
  { label: 'fastify adapter (@alexify/wrpc/fastify)', entry: 'fastify.js', platform: 'node' },
  { label: 'express adapter (@alexify/wrpc/express)', entry: 'express.js', platform: 'node' },
  { label: 'rooms backplane (@alexify/wrpc/scaling)', entry: 'scaling.js', platform: 'node' },
  // The broker family is Node-only: brokers are reached from servers, never
  // from a browser bundle.
  { label: 'broker core (@alexify/wrpc/broker)', entry: 'broker.js', platform: 'node' },
  { label: 'redis broker (@alexify/wrpc/broker/redis)', entry: 'broker/redis.js', platform: 'node' },
  { label: 'nats broker (@alexify/wrpc/broker/nats)', entry: 'broker/nats.js', platform: 'node' },
  { label: 'amqp broker (@alexify/wrpc/broker/amqp)', entry: 'broker/amqp.js', platform: 'node' },
  { label: 'kafka broker (@alexify/wrpc/broker/kafka)', entry: 'broker/kafka.js', platform: 'node' },
  // 12 -> 13 alongside the main-entry raise: the sse entry bundles the same
  // client core, so the REST-bridge bytes land here too. 13 -> 14 with the
  // main entry's static-introspection raise, for the same reason; 14 -> 15
  // with its auth + metadata raise, again for the same shared core (plus
  // the sse transport's own declared-headers/meta legs); 15 -> 16 with its
  // resilience raise (same shared core, plus the sse POST settling its own
  // refused calls through failPackets).
  // 16 -> 17: bundles the main browser entry, so it inherits its SharedWorker bytes.
  // 17 -> 18 with the main entry's WebTransport raise, for the same reason (measured 17.8);
  // 18 -> 20 with its datagram + stream-mux raise (measured 19.2).
  // 20 -> 21 with the main entry's call-path raise (measured 19.9).
  { label: 'sse — browser (@alexify/wrpc/sse)', entry: 'sse.browser.js', platform: 'browser', budget: 21 },
  { label: 'sse — node (@alexify/wrpc/sse)', entry: 'sse.js', platform: 'node' },
  { label: 'query bindings (@alexify/wrpc/query)', entry: 'query.js', platform: 'browser', budget: 2 },
  // Browser-reachable like query (stores + bearerAuth ship to pages), and
  // deliberately OUTSIDE the main entry so only apps that opt into the
  // strategies pay for them.
  { label: 'auth strategies (@alexify/wrpc/auth)', entry: 'auth.js', platform: 'browser', budget: 2 },
  // A peer is a client AND a server: the webrtc browser entry bundles the
  // client core plus the router, dispatcher, per-peer Client, rooms and
  // Broadcast (what makes a mesh broadcast/ask a single-encode fan-out),
  // plus the link, framing, peer, mesh and signaler halves — measured at
  // 38.3 KB when the row landed. 40 -> 41 for the server telemetry writer:
  // a peer answers calls, so it emits the server spans and gauges a server
  // does (+1.9 KB, the whole of src/telemetry/server.js). 41 -> 42 for stable
  // identity: the signaler's instance/address bookkeeping, the peer's
  // incarnation check and the mesh's away set (+0.7 KB, measured 41.4).
  // 42 -> 45 for trust assertions: the JWS verifier over crypto.subtle
  // (base64url, the SDP fingerprint parser, key lookup and rotation) plus
  // the peer's per-link verify/stamp chains and the host's trust
  // 'assertion' (+2.6 KB, measured 44.0) — the cryptographic layer the
  // peer-to-peer trust model rests on, a deliberate spend.
  // 45 -> 46 for the shared fan-out message (`Client.sendShared`, the
  // `compress` option, `isReady`, the lazy Context uuid) — the rpc leaves
  // this entry bundles for PeerHost (+0.5 KB, measured 45.0).
  // 46 -> 47 for the message-broker telemetry seams (the CONSUMER span kind
  // a host-built client picks, withMessagingSpan, the wrpc.broker.*
  // instruments): they live in telemetry/server.js, which PeerHost bundles
  // (+0.3 KB, measured 46.2 against 45.9).
  { label: 'webrtc — browser (@alexify/wrpc/webrtc)', entry: 'webrtc.browser.js', platform: 'browser', budget: 47 },
  { label: 'webrtc — node (@alexify/wrpc/webrtc)', entry: 'webrtc.js', platform: 'node' },
  // The server half of WebTransport (session contract, socket shim, host
  // adapters); the client transport is in the main entry, so this never
  // reaches a browser.
  { label: 'webtransport — node (@alexify/wrpc/wt)', entry: 'wt.js', platform: 'node' },
];

// A browser entry has to be self-contained: no node builtins, and no packages
// either — esbuild resolves devDependencies happily, so a stray
// `require('@tanstack/query-core')` would be inlined into the bundle and only
// show up as a few extra KB. Failing at RESOLVE time names the offending
// import and its importer, which a substring search over the output cannot.
const selfContained = {
  name: 'self-contained',
  setup(build) {
    build.onResolve({ filter: /.*/ }, (args) => {
      if (args.kind === 'entry-point') return null;
      if (args.path.startsWith('.') || path.isAbsolute(args.path)) return null;
      const text = `browser bundle must not import '${args.path}' (from ${path.relative(ROOT, args.importer)})`;
      return { errors: [{ text }] };
    });
  },
};

async function bundle(entry, platform, minify) {
  const result = await esbuild.build({
    entryPoints: [path.join(ROOT, entry)],
    bundle: true,
    minify,
    platform,
    conditions: platform === 'browser' ? ['browser'] : [],
    plugins: platform === 'browser' ? [selfContained] : [],
    write: false,
    logLevel: 'silent',
  });
  return Buffer.from(result.outputFiles[0].contents);
}

function kb(bytes) {
  return `${(bytes / 1024).toFixed(1)} KB`;
}

async function main() {
  const rows = [];
  const over = [];
  for (const { label, entry, platform, budget } of ENTRIES) {
    // The browser entries are bundled through the self-contained plugin, so an
    // import that does not belong fails the build here rather than inflating a
    // number nobody reads.
    const raw = await bundle(entry, platform, false);
    const min = await bundle(entry, platform, true);
    const gzip = gzipSync(min, { level: 9 }).length;
    // A budget only makes sense where the number is a real shipping cost, so
    // only the browser-reachable entries carry one. It is a ratchet against
    // accidental bloat, not a target: raise it deliberately, in the same
    // change that earns the bytes.
    if (budget !== undefined && gzip > budget * 1024) {
      over.push(`${entry}: ${kb(gzip)} min+gzip is over its ${budget} KB budget`);
    }
    rows.push({
      label,
      raw: kb(raw.length),
      min: kb(min.length),
      gzip: kb(gzip),
      budget: budget === undefined ? '—' : `${budget}.0 KB`,
    });
  }

  console.log('| Entry | raw | min | min+gzip | budget |');
  console.log('| ----- | ---:| ---:| --------:| ------:|');
  for (const row of rows) {
    console.log(`| ${row.label} | ${row.raw} | ${row.min} | ${row.gzip} | ${row.budget} |`);
  }

  // Reported after the table: seeing every number is what makes a budget
  // failure actionable.
  if (over.length > 0) throw new Error(`bundle size budget exceeded\n  ${over.join('\n  ')}`);
}

main().catch((error) => {
  // esbuild rejects with an object whose useful part is `errors[].text`; the
  // object itself prints as a wall of getters, which is how a clear "you
  // imported a package into the browser bundle" turns into noise.
  const reported = Array.isArray(error?.errors) ? error.errors : null;
  if (reported) for (const { text } of reported) console.error(text);
  else console.error(error);
  process.exitCode = 1;
});
