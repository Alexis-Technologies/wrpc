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
  { label: 'main entry — browser (@alexify/wrpc)', entry: 'browser.js', platform: 'browser', budget: 13 },
  { label: 'main entry — node (@alexify/wrpc)', entry: 'index.js', platform: 'node' },
  { label: 'websocket engine (@alexify/wrpc/ws)', entry: 'ws.js', platform: 'node' },
  { label: 'engine port (@alexify/wrpc/engine)', entry: 'engine.js', platform: 'node' },
  { label: 'uWebSockets.js adapter (@alexify/wrpc/uws)', entry: 'uws.js', platform: 'node' },
  { label: 'fastify adapter (@alexify/wrpc/fastify)', entry: 'fastify.js', platform: 'node' },
  { label: 'express adapter (@alexify/wrpc/express)', entry: 'express.js', platform: 'node' },
  { label: 'rooms backplane (@alexify/wrpc/scaling)', entry: 'scaling.js', platform: 'node' },
  // 12 -> 13 alongside the main-entry raise: the sse entry bundles the same
  // client core, so the REST-bridge bytes land here too. 13 -> 14 with the
  // main entry's static-introspection raise, for the same reason.
  { label: 'sse — browser (@alexify/wrpc/sse)', entry: 'sse.browser.js', platform: 'browser', budget: 14 },
  { label: 'sse — node (@alexify/wrpc/sse)', entry: 'sse.js', platform: 'node' },
  { label: 'query bindings (@alexify/wrpc/query)', entry: 'query.js', platform: 'browser', budget: 2 },
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
