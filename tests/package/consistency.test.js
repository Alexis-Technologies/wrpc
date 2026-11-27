'use strict';

// The machine-checked version of the release checklist's "check what would
// ship" step. `files` is an explicit allowlist, so a root file that was not
// added to it silently disappears from the tarball; an exports entry whose
// file does not exist fails only at a consumer's first import.
// This suite makes every one of those a red test instead.

const path = require('node:path');
const { existsSync } = require('node:fs');
const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const ROOT = path.join(__dirname, '..', '..');
const pkg = require('../../package.json');

// Every file an exports condition can resolve to, flattened.
const exportTargets = (value, out = []) => {
  if (typeof value === 'string') out.push(value);
  else if (value && typeof value === 'object') {
    for (const nested of Object.values(value)) exportTargets(nested, out);
  }
  return out;
};

const shipped = (relative) => {
  // The allowlist ships whole directories (src/, bin/) and named root files.
  const clean = relative.replace(/^\.\//, '');
  return pkg.files.some((entry) => (entry.endsWith('/') ? clean.startsWith(entry) : clean === entry));
};

// A subpath resolves straight to its entry under src/, the d.ts beside it
// (the layout of the sibling mailer): the root holds the main entry and
// nothing else of the package — no shim of one line per subpath, no root
// d.ts per subpath. bin/ is the CLI, not an entry.
const ROOT_FILES = ['index.js', 'index.d.ts'];

// ---------------------------------------------------------------------------
// The hand-synced pairs CLAUDE.md names (and two the review found unnamed):
// every one of these used to be a release-checklist memory item, and two had
// already drifted by the time the guard below was written.

const read = (relative) => require('node:fs').readFileSync(path.join(ROOT, relative), 'utf8');

// The value declarations of a d.ts — class, function, const, let —
// with its re-exports followed (`export * from './x.js'` brings x's values;
// `export { a, b } from './x.js'` the named ones x declares as values).
// Interfaces and `export type` are not values and are not counted.
const declaredValues = (file, seen = new Set()) => {
  const names = new Set();
  if (seen.has(file) || !existsSync(path.join(ROOT, file))) return names;
  seen.add(file);
  const text = read(file);
  for (const match of text.matchAll(/^export (?:declare )?(?:abstract )?(?:class|function|const|let)\s+(\w+)/gm)) {
    names.add(match[1]);
  }
  // A reference is relative to the d.ts that makes it.
  const near = (ref) => path.posix.join(path.posix.dirname(file), `${ref}.d.ts`);
  for (const match of text.matchAll(/^export \* from '(\.{1,2}\/[\w./-]+)\.js';/gm)) {
    for (const name of declaredValues(near(match[1]), seen)) names.add(name);
  }
  for (const match of text.matchAll(/^export \{([^}]*)\} from '(\.{1,2}\/[\w./-]+)\.js';/gm)) {
    const target = declaredValues(near(match[2]), seen);
    for (const entry of match[1].split(',')) {
      const name = entry
        .trim()
        .split(/\s+as\s+/)
        .at(-1);
      if (name && target.has(name)) names.add(name);
    }
  }
  return names;
};

// Members, not just names: a member the runtime class has and its d.ts
// class never declares is an untyped API too — `ServerTransport#revision`,
// `#setRevision` and the port transport's `negotiate` were, with every name
// above in order. The classes an application reaches: their prototype chain
// up to the Emitter, plus the instance fields of the ones a test can build.
// The d.ts side is read with the TypeScript tsd itself type-checks with.
// What the core calls on its own objects is named here, with the caller.
const INTERNAL_MEMBERS = {
  // httpReply (`context.http`) and the fastify adapter's delegated routes.
  ServerHttpTransport: ['setHeader', 'setStatus', 'pendingCookies'],
  // The broadcast fan-out's shared frames.
  ServerWsTransport: ['writeWith', 'writeShared'],
  // The dispatcher's.
  Client: ['decodePacket', 'negotiateRevision'],
};

// The `[Unreleased]` section of the CHANGELOG, split by its `### ` headings:
// a map of heading -> the lines under it. The release checklist reads the
// breaking section to decide the bump, so an entry marked **Breaking that
// sits under `### Changed` or `### Added` is a semver lie waiting to ship.
const unreleasedSections = () => {
  const changelog = read('CHANGELOG.md');
  const start = changelog.indexOf('\n## [Unreleased]');
  assert.ok(start >= 0, 'CHANGELOG.md must keep an [Unreleased] section');
  const next = changelog.indexOf('\n## [', start + 1);
  const lines = changelog.slice(start, next < 0 ? undefined : next).split('\n');
  const sections = new Map();
  let heading = '';
  for (const line of lines) {
    if (line.startsWith('### ')) heading = line.slice(4).trim();
    else if (heading) sections.get(heading)?.push(line) ?? sections.set(heading, [line]);
  }
  return sections;
};

// Keep a Changelog, as every Alexis library writes it: `## [X.Y.Z] - date`
// newest first, the six headings plus the four of alexiskills, and a link
// for every version in the block at the bottom.
const HEADINGS = new Set([
  'Added',
  'Changed',
  'Deprecated',
  'Removed',
  'Fixed',
  'Security',
  'Changed (breaking)',
  'Documentation',
  'Tooling',
]);

const NUMBER_WORDS = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine'];
const subpaths = Object.keys(pkg.exports)
  .filter((key) => key !== '.' && key !== './package.json')
  .map((key) => key.slice(1));

// Two rules of the project skills that a review keeps finding broken, made
// executable. "A queue read from its head is never an Array with shift()":
// past a few thousand elements every take moves the rest (found three times
// — a stream's queue, the ws receive buffer, the ws inbox). "Never spread a
// data array into a call's arguments": it is Function.prototype.apply, and a
// RangeError once the array passes the engine's argument limit.
//
// Every `.shift()` / `.unshift(` and every spread into a call in src/ is on
// one of these lists with the reason it is safe — what it is called on, or
// what bounds the array. A new one fails here: use a Deque (src/deque.js) or
// a head cursor, an indexed loop instead of the spread, or add the line with
// its bound. A line that is gone fails too, so the lists stay the code's.
const HEAD_READS = {
  'src/broker/kafka/index.js': {
    'const next = waiting.shift();': 'a Deque',
    'for (let next = waiting.shift(); next !== undefined; next = waiting.shift()) next();': 'a Deque',
  },
  'src/broker/memory.js': {
    'state.ready.unshift(delivery.message);': 'a Deque',
    'this.#dispatch(state, consumer, state.ready.shift());': 'a Deque',
    'state.ready.unshift(message);': 'a Deque',
  },
  'src/broker/redis/index.js': { 'const entry = buffered.shift();': 'a Deque' },
  'src/broker/tail.js': { 'const entry = reader.queue.shift();': 'a Deque' },
  'src/transport.js': {
    'const index = bucket.shift();': 'the answers of ONE batch that share an id: at most maxBatch (128)',
  },
  'src/utils.js': {
    'this.#queue.shift();': 'EventStream, at its highWaterMark (1,024 by default); utils.js is on the browser budget',
    'return Promise.resolve({ value: this.#queue.shift(), done: false });': 'the same queue',
  },
  'src/websocket/connection.js': {
    "if (typeof this.#socket.unshift === 'function') this.#socket.unshift(head);": 'a stream method, not an array',
    'const item = inbox.shift();': 'the inflate inbox, held at INBOX_HIGH + 1 messages',
    'outbox.shift();': 'a Deque',
    'this.#outbox.shift();': 'a Deque',
  },
  'src/websocket/deflateContext.js': { 'ops.shift();': 'at most 33 operations wait on one context' },
};

const CALL_SPREADS = {
  'src/broker/redis/index.js': {
    'rows = await tail.connection.xread(...args);': 'a tail reads at most TAIL_TOPICS (8,192) topics',
    [`if (maxLen > 0) return client.xadd(streamKey(topic), 'MAXLEN', '~', \`\${maxLen}\`, '*', ...fields);`]:
      'two fields per header of one message this process produced',
    "return client.xadd(streamKey(topic), '*', ...fields);": 'the same fields',
    "await client.xadd(queueKey(name), '*', ...encodeFields(body, producerHeaders(headers)));": 'the same fields',
    "await client.xadd(stream, '*', ...encodeFields(message.body, headers));": 'the same fields',
    "await client.xadd(queueKey(deadLetter), '*', ...encodeFields(message.body, headers));": 'the same fields',
    'await client.eval(PROMOTE, 2, delayedKey(name), stream, item, ...fields);': 'the same fields',
    "await client.xadd(stream, '*', ...fields);": 'the same fields',
  },
  'src/client/core.js': { 'if (units.length > 0) await this.load(...units);': 'the units the application loaded' },
  'src/client/wsHandshake.browser.js': {
    'btoa(String.fromCharCode(...new TextEncoder().encode(text)))': 'the caller refuses a text past META_MAX first',
  },
  'src/encryption/noise.js': {
    'const message = concat(...parts);': 'the parts of one handshake message: five at most',
  },
  'src/encryption/server.js': { "this.emit('close', ...args);": "the engine's own (code, reason)" },
  'src/encryption/session.js': {
    'const frame = (kind, ...parts) => concat(Uint8Array.of(FRAME_MARK, kind), ...parts);': 'its own rest parameter',
  },
  'src/rpc/core.js': {
    'const target = rooms ? this.#target().to(...rooms) : this.#target();':
      "an envelope's rooms: another instance's to(...rooms), and the caller catches what this throws",
    'const target = Array.isArray(rooms) ? this.#target().to(...rooms) : this.#target();': 'the same rooms',
    'return this.#target().to(...rooms);': 'its own rest parameter',
    'return this.#target().except(...clients);': 'its own rest parameter',
  },
  'src/scaling/sessions.js': {
    'const result = await client.set(key(token), value, ...args);': 'three options at most, built two lines above',
  },
  'src/server.js': {
    'return this.rpc.to(...rooms);': 'its own rest parameter',
    'return this.rpc.except(...clients);': 'its own rest parameter',
  },
  'src/webrtc/host.js': {
    'return this.#target().to(...rooms);': 'its own rest parameter',
    'return this.#target().except(...clients);': 'its own rest parameter',
  },
  'src/webrtc/peerLink.js': { 'return this.#remote.load(...units);': 'its own rest parameter' },
};

// The CLAUDE.md files are binding instructions for an agent — the root one in
// every session, a folder's when a file of that folder is read — so a
// reference in one that points nowhere is an instruction to look at the wrong
// code. Three things are held in every one of them: no line numbers (they
// drift within days — eleven of twenty-two were wrong when they were
// removed), every path named exists, and every "`symbol` in `file`" is still
// true. Paths are written from the repository root in all of them.
const guidanceFiles = () => {
  const { readdirSync } = require('node:fs');
  const SKIP = new Set(['node_modules', '.git', 'coverage', '.vitepress', 'dist']);
  const walk = (dir, out = []) => {
    for (const entry of readdirSync(path.join(ROOT, dir), { withFileTypes: true })) {
      const relative = dir === '.' ? entry.name : `${dir}/${entry.name}`;
      if (entry.isDirectory()) {
        if (!SKIP.has(entry.name)) walk(relative, out);
      } else if (entry.name === 'CLAUDE.md') out.push(relative);
    }
    return out;
  };
  return walk('.').sort();
};

describe('package.json', () => {
  it('should have every exports target exist and sit in the files allowlist', () => {
    for (const [key, value] of Object.entries(pkg.exports)) {
      for (const target of exportTargets(value)) {
        assert.ok(existsSync(path.join(ROOT, target)), `${key} -> ${target} does not exist`);
        assert.ok(shipped(target), `${key} -> ${target} is not in the files allowlist`);
      }
    }
  });

  it('should give every subpath a types condition wherever it has a runtime one', () => {
    for (const [key, value] of Object.entries(pkg.exports)) {
      if (key === './package.json') continue;
      const check = (conditions, label) => {
        assert.ok(typeof conditions === 'object', `${label} must be a conditions object`);
        assert.ok(conditions.types, `${label} has no types condition`);
        assert.ok(conditions.default, `${label} has no default condition`);
        // exports conditions match top-down: a `types` AFTER `default` is dead.
        const keys = Object.keys(conditions);
        assert.ok(keys.indexOf('types') < keys.indexOf('default'), `${label}: types must come before default`);
        if (conditions.browser && typeof conditions.browser === 'object') check(conditions.browser, `${label}.browser`);
      };
      check(value, key);
    }
  });

  it('should resolve every subpath into src/, the root holding only the main entry', () => {
    for (const [key, value] of Object.entries(pkg.exports)) {
      if (key === '.' || key === './package.json') continue;
      for (const target of exportTargets(value)) {
        assert.ok(target.startsWith('./src/'), `${key} -> ${target} is not an entry under src/`);
      }
    }
    const named = pkg.files.filter((entry) => !entry.startsWith('!') && !entry.endsWith('/'));
    const code = named.filter((entry) => /\.(?:js|ts)$/.test(entry));
    assert.deepStrictEqual(code, ROOT_FILES, 'the files allowlist names no root code but the main entry');
    const stray = require('node:fs')
      .readdirSync(ROOT)
      .filter((name) => /\.(?:c?js|d\.ts)$/.test(name) && !ROOT_FILES.includes(name));
    assert.deepStrictEqual(stray, [], 'a root shim or d.ts is back; its entry belongs under src/');
    assert.ok(!existsSync(path.join(ROOT, 'broker')), 'the broker/ shims are back; they live in src/broker/<name>/');
  });

  // TypeScript under `moduleResolution: node10` (the default for
  // `module: commonjs`) never reads `exports`; `typesVersions` is what tells it
  // where a subpath's types are, now that no root d.ts carries the subpath's
  // name. It must name exactly the d.ts each subpath's `types` condition
  // names — the two are one fact written twice.
  it('should mirror the types condition of every subpath in typesVersions', () => {
    const map = pkg.typesVersions?.['*'];
    assert.ok(map, 'package.json has typesVersions["*"]');
    const expected = {};
    for (const [key, value] of Object.entries(pkg.exports)) {
      if (key === '.' || key === './package.json') continue;
      expected[key.slice(2)] = [value.types];
    }
    assert.deepStrictEqual(map, expected);
  });

  it('should check that every published subpath has a tsd file', () => {
    for (const key of Object.keys(pkg.exports)) {
      if (key === './package.json') continue;
      const name = key === '.' ? 'index' : key.slice(2);
      const candidates = [
        path.join(ROOT, 'tests', `${name}.test-d.ts`),
        path.join(ROOT, 'tests', name, 'index.test-d.ts'),
      ];
      assert.ok(
        candidates.some((candidate) => existsSync(candidate)),
        `${key} has no tests/${name}.test-d.ts`,
      );
    }
  });

  it('should point the browser field map at files that ship', () => {
    for (const [from, to] of Object.entries(pkg.browser)) {
      for (const file of [from, to]) {
        assert.ok(existsSync(path.join(ROOT, file)), `browser map: ${file} does not exist`);
        assert.ok(shipped(file), `browser map: ${file} is not in the files allowlist`);
      }
    }
  });

  it('should keep the bin shim existing, shipped, and requiring a shipped file', () => {
    for (const [name, target] of Object.entries(pkg.bin)) {
      assert.ok(existsSync(path.join(ROOT, target)), `bin ${name} -> ${target} does not exist`);
      assert.ok(shipped(target), `bin ${name} -> ${target} is not in the files allowlist`);
    }
  });

  it('should have every file the allowlist names exist', () => {
    for (const entry of pkg.files) {
      // A negation takes files OUT of the list (the CLAUDE.md guard below).
      if (entry.startsWith('!')) continue;
      assert.ok(existsSync(path.join(ROOT, entry.replace(/\/$/, ''))), `files entry ${entry} does not exist`);
    }
  });

  it('should give every exports subpath a bundle-size row in scripts/size.js', () => {
    const size = read('scripts/size.js');
    const entries = [...size.matchAll(/entry: '([^']+)'/g)].map((m) => m[1]);
    for (const [key, value] of Object.entries(pkg.exports)) {
      if (key === './package.json') continue;
      const targets = exportTargets(value).map((target) => target.replace(/^\.\//, ''));
      // .d.ts targets are types conditions, not bundles.
      const runtime = targets.filter((target) => target.endsWith('.js'));
      assert.ok(
        runtime.some((target) => entries.includes(target)),
        `${key} has no ENTRIES row in scripts/size.js (targets: ${runtime.join(', ')})`,
      );
    }
  });
});

describe('the hand-written types', () => {
  // Every barrel of every subpath, under both conditions, against the d.ts its
  // `types` condition names — in BOTH directions: a runtime export the types
  // never declare is an untyped API, and a value the types declare that the
  // barrel never exports is a lie a user only discovers at runtime. One-way,
  // over six barrels, this test missed both for a release.
  it('should agree with the runtime barrel exports, both ways, on every subpath', () => {
    const cases = [];
    for (const [key, value] of Object.entries(pkg.exports)) {
      if (key === './package.json') continue;
      const conditions = [value];
      if (value.browser) conditions.push(value.browser);
      for (const condition of conditions) {
        if (typeof condition !== 'object' || !condition.default || !condition.types) continue;
        cases.push([condition.default.replace(/^\.\//, ''), condition.types.replace(/^\.\//, '')]);
      }
    }
    assert.ok(cases.length >= 22, `every subpath, both conditions: ${cases.length}`);
    for (const [barrel, dts] of cases) {
      let runtime;
      try {
        runtime = Object.keys(require(path.join(ROOT, barrel)));
      } catch (error) {
        // An adapter barrel whose framework is not installed here: nothing to
        // compare, and not this machine's failure (optional(), as the
        // adapter tests do).
        if (error.code === 'MODULE_NOT_FOUND') continue;
        throw error;
      }
      const declared = declaredValues(dts);
      for (const name of runtime) {
        assert.ok(declared.has(name), `${barrel} exports '${name}' but ${dts} never declares it`);
      }
      for (const name of declared) {
        assert.ok(runtime.includes(name), `${dts} declares '${name}' as a value but ${barrel} does not export it`);
      }
    }
  });

  it('should declare every public member of a runtime class on its d.ts class', () => {
    const { EventEmitter } = require('node:events');
    const ts = require(require.resolve('@tsd/typescript', { paths: [path.dirname(require.resolve('tsd'))] }));
    const declared = new Map();
    for (const file of ['index.d.ts', 'src/rpc.d.ts', 'src/client.d.ts']) {
      const source = ts.createSourceFile(file, read(file), ts.ScriptTarget.Latest, true);
      const visit = (node) => {
        if (ts.isClassDeclaration(node) && node.name) {
          const members = declared.get(node.name.text) ?? new Set();
          for (const member of node.members) {
            if (member.name && ts.isIdentifier(member.name)) members.add(member.name.text);
          }
          declared.set(node.name.text, members);
        }
        ts.forEachChild(node, visit);
      };
      visit(source);
    }
    const wrpc = require(path.join(ROOT, 'index.js'));
    const { Client } = require(path.join(ROOT, 'src/rpc/client.js'));
    const { Emitter } = require(path.join(ROOT, 'src/utils.js'));
    const { ServerTransport } = wrpc;
    const { http: HttpTransport, ws: WsTransport, event: EventTransport } = ServerTransport.transport;
    const port = () => Object.assign(new EventEmitter(), { postMessage() {}, close() {} });
    const cases = [
      ['ServerTransport', ServerTransport, () => new ServerTransport('x')],
      ['ServerHttpTransport', HttpTransport, () => new HttpTransport({ headers: {}, respond() {} })],
      ['ServerWsTransport', WsTransport, () => new WsTransport(Object.assign(new EventEmitter(), { send() {} }))],
      ['ServerEventTransport', EventTransport, () => new EventTransport(port())],
      ['Client', Client, null],
      ['RpcServer', wrpc.RpcServer, null],
      ['WrpcClient', wrpc.WrpcClient, null],
    ];
    for (const [name, Class, make] of cases) {
      const runtime = new Set();
      for (let proto = Class.prototype; proto !== Emitter.prototype && proto !== Object.prototype;) {
        for (const member of Object.getOwnPropertyNames(proto)) if (member !== 'constructor') runtime.add(member);
        proto = Object.getPrototypeOf(proto);
      }
      if (make) for (const field of Object.keys(make())) runtime.add(field);
      const types = new Set(declared.get(name));
      // The transports' d.ts classes extend ServerTransport's.
      if (name !== 'ServerTransport' && name.startsWith('Server')) {
        for (const member of declared.get('ServerTransport')) types.add(member);
      }
      const internal = INTERNAL_MEMBERS[name] ?? [];
      for (const member of runtime) {
        if (member.startsWith('_') || internal.includes(member)) continue;
        assert.ok(types.has(member), `${name}#${member} exists at runtime but its d.ts class never declares it`);
      }
    }
  });

  it('should resolve every relative x.js reference inside a shipped d.ts to a shipped x.d.ts', () => {
    const { readdirSync } = require('node:fs');
    const typings = readdirSync(path.join(ROOT, 'src'), { recursive: true })
      .filter((name) => name.endsWith('.d.ts'))
      .map((name) => `src/${name.split(path.sep).join('/')}`);
    assert.ok(typings.length >= 24, `only ${typings.length} d.ts under src/`);
    for (const file of [...ROOT_FILES.filter((name) => name.endsWith('.d.ts')), ...typings]) {
      const text = read(file);
      const refs = [
        ...[...text.matchAll(/from '(\.{1,2}\/[\w./-]+)\.js'/g)].map((m) => m[1]),
        ...[...text.matchAll(/import\('(\.{1,2}\/[\w./-]+)\.js'\)/g)].map((m) => m[1]),
      ];
      for (const ref of refs) {
        const dts = path.posix.join(path.posix.dirname(file), `${ref}.d.ts`);
        assert.ok(existsSync(path.join(ROOT, dts)), `${file} references ${ref}.js but ${dts} does not exist`);
        assert.ok(shipped(dts), `${file} references ${ref}.js but ${dts} is not in the files allowlist`);
      }
    }
  });

  // What a bundler hands a browser — every row scripts/size.js bundles with
  // platform 'browser' — must type-check with no Node types at all: a page
  // whose project has no @types/node gets the client surface, not 41 errors
  // from `node:http` and `Buffer`. `query.d.ts` imported the Node barrel for
  // seven names `client.d.ts` declares, and did since 1.0.
  it('should check the types of every browser-reachable entry without node types', () => {
    const ts = require(require.resolve('@tsd/typescript', { paths: [path.dirname(require.resolve('tsd'))] }));
    const size = read('scripts/size.js');
    const entries = [...size.matchAll(/entry: '([^']+)',\s*platform: 'browser'/g)].map((m) => m[1]);
    assert.ok(entries.length >= 7, `browser rows found: ${entries.join(', ')}`);
    const program = ts.createProgram(
      entries.map((entry) => path.join(ROOT, entry.replace(/\.js$/, '.d.ts'))),
      {
        noEmit: true,
        strict: true,
        types: [],
        lib: ['lib.es2022.d.ts', 'lib.dom.d.ts'],
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.ESNext,
        moduleResolution: ts.ModuleResolutionKind.Bundler,
      },
    );
    const errors = ts.getPreEmitDiagnostics(program).map((diagnostic) => {
      const where = diagnostic.file ? path.relative(ROOT, diagnostic.file.fileName) : '';
      return `${where}: ${ts.flattenDiagnosticMessageText(diagnostic.messageText, ' ')}`;
    });
    assert.deepStrictEqual(errors, []);
  });
});

describe('the docs', () => {
  it('should mirror package.json keywords and version in docs/.vitepress/config.mts', () => {
    const config = read('docs/.vitepress/config.mts');
    const start = config.indexOf('const keywords = [');
    assert.ok(start >= 0, 'config.mts must keep its keywords array');
    const block = config.slice(start, config.indexOf(']', start));
    const mirrored = [...block.matchAll(/'([^']+)'/g)].map((match) => match[1]);
    assert.deepStrictEqual(mirrored, pkg.keywords, 'config.mts keywords must mirror package.json exactly');
    assert.ok(config.includes(`'v${pkg.version}'`), `the nav version label must read v${pkg.version}`);
  });

  // The size tables in README.md and docs/guide/browser.md are hand-pasted
  // from `pnpm size`; the numbers go stale by design (the release checklist
  // refreshes them), but a BUDGET that differs from scripts/size.js is a lie
  // about what CI enforces — so every budget the tables show must be the
  // script's, and every budgeted entry of the script must be in README's.
  it('should match the budget column of the size tables in README and the browser guide to scripts/size.js', () => {
    // scripts/size.js: every budgeted entry, as (subpath, half) -> budget,
    // the subpath read from the row's label (`… (@alexify/wrpc/sse)`).
    const script = read('scripts/size.js');
    const budgets = new Map();
    const row =
      /label: '[^']*\(@alexify\/wrpc(\/[\w/]+)?\)',\s*entry: '[^']+',\s*platform: 'browser',\s*budget: (\d+)/g;
    for (const match of script.matchAll(row)) {
      // A budgeted entry IS a browser half (only browser-reachable entries carry
      // a budget), whatever the file is called: query, auth and deflate are one
      // file for both platforms.
      budgets.set(`${match[1] ?? ''}|browser`, Number(match[2]));
    }
    assert.ok(budgets.size >= 7, `budgeted entries in scripts/size.js: ${budgets.size}`);
    // A table row that shows a budget: its subpath is the first backticked
    // token, its half is "browser" unless the row says "node".
    const shown = (text) => {
      const rows = new Map();
      const line = /^\| (\S[^|]*?) \| [\d.]+ KB \| \*\*[\d.]+ KB\*\* \| ([\d.]+) KB \|$/gm;
      for (const match of text.matchAll(line)) {
        const token = match[1].match(/@alexify\/wrpc(\/[\w/]+)?/);
        if (!token) continue;
        const half = / — node/.test(match[1]) ? 'node' : 'browser';
        rows.set(`${token[1] ?? ''}|${half}`, Number(match[2]));
      }
      return rows;
    };
    for (const file of ['README.md', 'docs/guide/browser.md']) {
      const rows = shown(read(file));
      assert.ok(rows.size >= 7, `${file}: budgeted rows found: ${rows.size}`);
      for (const [key, budget] of rows) {
        assert.ok(budgets.has(key), `${file}: ${key} shows a budget but scripts/size.js has none for it`);
        assert.strictEqual(
          budget,
          budgets.get(key),
          `${file}: ${key} shows ${budget} KB; scripts/size.js says ${budgets.get(key)}`,
        );
      }
      if (file === 'README.md') {
        for (const key of budgets.keys()) {
          assert.ok(rows.has(key), `README.md has no row for the budgeted entry ${key}`);
        }
      }
    }
  });

  // The home page's "One router, every transport" table is the short copy of
  // the matrix in the client guide. Two hand-written tables drift: a transport
  // added to one, or a cell changed in one, must reach the other — so every
  // row of either is in both, and every shared column agrees on its verdict
  // (the first token of a cell: "❌ (code 400)" says the same as "❌").
  it('should mirror the client guide matrix in the home transport table', () => {
    const table = (file) => {
      const lines = read(file).split('\n');
      const start = lines.findIndex((line) => line.startsWith('| Transport |'));
      assert.ok(start >= 0, `${file} has a "| Transport |" table`);
      const cells = (line) =>
        line
          .slice(1, -1)
          .split('|')
          .map((cell) => cell.trim());
      const headers = cells(lines[start]);
      const rows = new Map();
      for (let i = start + 2; i < lines.length && lines[i].startsWith('|'); i++) {
        const row = cells(lines[i]);
        rows.set(row[0], row);
      }
      return { headers, rows };
    };
    const ROWS = {
      WebSocket: '`ws`',
      'HTTP & REST': '`http`',
      'Server-Sent Events': '`sse`',
      WebTransport: '`wt`',
      WebRTC: '`webrtc`',
      'Worker port': '`event`',
      'Broker, stateless': '`broker`, stateless',
      'Broker, session': '`broker`, session',
    };
    const COLUMNS = {
      Calls: 'Calls',
      'Events & subscriptions': 'Events, subscriptions, cancel',
      'Binary streams': 'Binary streams',
      Compression: 'Compression',
      Encryption: 'Encryption',
    };
    const home = table('docs/index.md');
    const guide = table('docs/guide/client.md');
    const verdict = (cell) => cell.split(/\s+/)[0];
    const mapped = new Set();
    for (const [label, row] of home.rows) {
      const name = label.match(/^\[([^\]]+)\]/)?.[1];
      assert.ok(name && Object.hasOwn(ROWS, name), `docs/index.md: unknown transport row ${label}`);
      const twin = guide.rows.get(ROWS[name]);
      assert.ok(twin, `docs/guide/client.md has no row ${ROWS[name]} for the home row ${name}`);
      mapped.add(ROWS[name]);
      for (const [homeColumn, guideColumn] of Object.entries(COLUMNS)) {
        const a = row[home.headers.indexOf(homeColumn)];
        const b = twin[guide.headers.indexOf(guideColumn)];
        assert.ok(a !== undefined && b !== undefined, `${name}: column ${homeColumn} is missing on one side`);
        assert.strictEqual(
          verdict(a),
          verdict(b),
          `${name} / ${homeColumn}: home says "${a}", the client guide "${b}"`,
        );
      }
    }
    for (const key of guide.rows.keys()) {
      assert.ok(mapped.has(key), `docs/guide/client.md row ${key} is missing from the home transport table`);
    }
  });

  // The benchmark charts (docs/.vitepress/theme/benchmarks.json, drawn by
  // BenchChart.vue) and the tables under them are two copies of one run: a
  // re-run pasted into one and not the other draws numbers the page does not
  // say. Every value a chart draws must be a number its page prints, and every
  // <BenchChart> in the docs must name a dataset (and rows) that exist.
  it('should print every number a benchmark chart draws in its page, and name real data in every chart', () => {
    const sets = JSON.parse(read('docs/.vitepress/theme/benchmarks.json'));
    const numbers = (text) =>
      new Set([...text.matchAll(/\d[\d,]*(?:\.\d+)?/g)].map((m) => Number(m[0].replaceAll(',', ''))));
    for (const [name, set] of Object.entries(sets)) {
      const page = read(set.page);
      assert.ok(page.includes(`<BenchChart set="${name}"`), `${set.page} draws no chart of "${name}"`);
      const printed = numbers(page);
      for (const row of set.rows) {
        assert.ok(row.kind === 'wrpc' || row.kind === 'other', `${name}/${row.id}: kind is wrpc or other`);
        for (const metric of set.metrics) {
          const value = row[metric.key];
          if (value === undefined) continue;
          assert.ok(printed.has(value), `${name}/${row.id}/${metric.key}: ${value} is not printed in ${set.page}`);
        }
      }
    }
    const docs = require('node:fs')
      .readdirSync(path.join(ROOT, 'docs'), { recursive: true })
      // Not CLAUDE.md: it is the agents' file about the site, never a page of it.
      .filter((file) => file.endsWith('.md') && !file.includes('.vitepress') && !file.endsWith('CLAUDE.md'));
    for (const file of docs) {
      for (const [tag] of read(`docs/${file}`).matchAll(/<BenchChart\b[^>]*>/g)) {
        const name = tag.match(/\bset="([^"]+)"/)?.[1];
        assert.ok(name && Object.hasOwn(sets, name), `docs/${file}: ${tag} names no dataset`);
        const pick = tag.match(/\bpick="([^"]+)"/)?.[1];
        for (const id of pick ? pick.split(',') : []) {
          assert.ok(
            sets[name].rows.some((row) => row.id === id.trim()),
            `docs/${file}: ${tag} picks "${id}", not a row of "${name}"`,
          );
        }
      }
    }
  });

  // SECURITY.md is a release-checklist item: its version table must name the
  // current major line (never a "pre-first-publish" that shipped), and its
  // scope must name every directory that parses hostile bytes or holds keys.
  it('should name the supported major in SECURITY.md, no pre-publish placeholder, and every hostile-input surface', () => {
    const text = read('SECURITY.md');
    const major = Math.max(2, Number(pkg.version.split('.')[0]));
    assert.ok(text.includes(`| \`${major}.x\``), `the supported row names ${major}.x`);
    assert.ok(!text.includes('pre-first-publish'), 'the pre-publish placeholder shipped once; never again');
    for (const surface of [
      'src/websocket/',
      'src/rpc/',
      'src/transport.js',
      'src/adapters/',
      'src/sse/',
      'src/auth/',
      'src/rpc/cluster.js',
      'src/cli/',
      'src/encryption/',
      'src/webtransport/',
      'src/deflate/inflate.js',
      'src/attachments.js',
      'src/webrtc/assertions.js',
      'src/broker/sealing.js',
    ]) {
      assert.ok(text.includes(surface), `SECURITY.md scope names ${surface}`);
    }
    assert.ok(text.includes('what-it-does-not-protect'), 'the documented limits are linked');
  });

  // The 1.x deprecation notes promised that "2.0 makes it a TypeError"; 2.0
  // kept the promise, so the sentence has nothing left to announce. A copy
  // of it surviving in the source, the types or the docs would describe a
  // fallback that no longer exists.
  it('should hold no text that still promises what 2.0 already did', () => {
    const { readdirSync, readFileSync } = require('node:fs');
    const STALE = /2\.0 (?:will make|makes) (?:it|all of them)/;
    const walk = (dir, out) => {
      for (const entry of readdirSync(path.join(ROOT, dir), { withFileTypes: true }) ?? []) {
        const relative = path.join(dir, entry.name);
        if (entry.isDirectory()) walk(relative, out);
        else if (/\.(?:js|md|ts)$/.test(entry.name)) out.push(relative);
      }
      return out;
    };
    const files = [...walk('src', []), ...walk('docs/guide', []), ...walk('docs/reference', [])];
    for (const name of readdirSync(ROOT)) if (name.endsWith('.d.ts')) files.push(name);
    const stale = files.filter((file) => STALE.test(readFileSync(path.join(ROOT, file), 'utf8')));
    assert.deepStrictEqual(stale, [], 'these files still promise what 2.0 already did');
  });

  // The stability page is where a consumer learns what the semver promise
  // covers, llms.txt where a model does; a subpath missing from either is a
  // public API nobody was told about.
  it('should name every exports subpath in docs/reference/stability.md and llms.txt', () => {
    const stability = read('docs/reference/stability.md');
    const llms = read('docs/public/llms.txt');
    for (const subpath of subpaths) {
      assert.ok(stability.includes(`\`${subpath}\``), `stability.md must list ${subpath}`);
      assert.ok(llms.includes(`\`${subpath}\``), `llms.txt must list ${subpath}`);
    }
  });

  // "Four areas are marked @experimental" went stale twice — the brokers and
  // then encryption were added under it. The count is derived from the list.
  it('should match the @experimental carve-out counts to their lists', () => {
    const bullets = (text, from, to, label) => {
      const start = text.indexOf(from);
      assert.ok(start >= 0, `${label} must keep its "${from}" section`);
      const end = text.indexOf(to, start + from.length);
      const block = text.slice(start, end < 0 ? undefined : end);
      return block.split('\n').filter((line) => line.startsWith('- ')).length;
    };
    const stability = read('docs/reference/stability.md');
    const count = bullets(stability, '## `@experimental` carve-outs', '\n## ', 'stability.md');
    assert.ok(count > 0 && count < NUMBER_WORDS.length, `an unexpected carve-out count: ${count}`);
    const word = NUMBER_WORDS[count];
    const capitalized = word[0].toUpperCase() + word.slice(1);
    assert.ok(stability.includes(`${capitalized} areas are marked`), `stability.md must say "${capitalized} areas"`);
    const contributing = read('CONTRIBUTING.md');
    const mirrored = bullets(
      contributing,
      '## Stability and deprecation',
      '\nAn `@experimental` API',
      'CONTRIBUTING.md',
    );
    assert.strictEqual(mirrored, count, 'CONTRIBUTING.md must list the same carve-outs as stability.md');
    assert.ok(contributing.includes(`with ${word} carve-outs`), `CONTRIBUTING.md must say "${word} carve-outs"`);
  });

  // protocol.md is the wire reference: every `## ` section 1.0 did not have
  // carries a "since" badge, so a reader knows what an older peer never saw,
  // and the experimental carriers say so in their first paragraph. The list of
  // 1.0 sections is closed — a section added without a badge fails here, and
  // so does a badge on one 1.0 had.
  it('should badge in protocol.md every section that is new since 1.0, and mark the experimental ones', () => {
    const text = read('docs/reference/protocol.md');
    const ORIGINAL = new Set([
      'Stability',
      'Versioning',
      'Changes since 1.0',
      'Framing',
      'Packets',
      'Introspection',
      'Sessions',
      'Rooms',
      'Server-Sent Events',
      'Reconnect',
    ]);
    const EXPERIMENTAL = new Set(['WebTransport', 'Broker binding', 'Session encryption']);
    const headings = [...text.matchAll(/^## (.+)$/gm)].map((match) => match[1]);
    assert.ok(headings.length >= ORIGINAL.size + EXPERIMENTAL.size, 'protocol.md lost sections');
    for (const heading of headings) {
      const title = heading
        .replace(/\s*<Badge[^>]*\/>/, '')
        .replace(/\s*\{#[^}]+\}\s*$/, '')
        .trim();
      const badge = /<Badge type="info" text="since \d+\.\d+" \/>/.test(heading);
      if (ORIGINAL.has(title)) assert.ok(!badge, `${title} was in 1.0: no since-badge`);
      else assert.ok(badge, `${title} is new since 1.0: its heading needs <Badge type="info" text="since N.0" />`);
      if (EXPERIMENTAL.has(title)) {
        const start = text.indexOf(`## ${heading}`);
        const next = text.indexOf('\n## ', start + 1);
        const section = text.slice(start, next < 0 ? undefined : next);
        assert.ok(
          section.replace(/\s+/g, ' ').includes('**This section is experimental**'),
          `${title} must say it is experimental at its top`,
        );
      }
    }
    assert.ok(text.includes('## Changes since 1.0 {#changes-since-1-0}'), 'the migration table keeps its anchor');
    assert.ok(
      !/frozen 1\.0|frozen at 1\.0|what 1\.x speaks/i.test(text),
      'the page describes revision 2.0, not a frozen 1.0',
    );
  });

  // docs/guide/logging.md is the catalogue of `event` names — the field
  // alerts are built on. Every name the source can write has a row there, and
  // every row names something the source still writes: a log line added
  // without a row, or a row left behind by a rename, fails here. It is the
  // only way a catalogue of two hundred names stays one.
  it('should catalogue in the logging guide every log event the source writes, and nothing it does not', () => {
    const { readdirSync, readFileSync } = require('node:fs');
    const walk = (dir, out = []) => {
      for (const entry of readdirSync(path.join(ROOT, dir), { withFileTypes: true })) {
        const relative = `${dir}/${entry.name}`;
        if (entry.isDirectory()) walk(relative, out);
        else if (entry.name.endsWith('.js')) out.push(relative);
      }
      return out;
    };
    // How an event name reaches a logger in src/ — keep these SIMPLE, and add
    // a sink here when a new helper takes the name as an argument:
    const SINKS = [
      /\bevent:\s*'([^'\n]+)'/g, // an entry's own field
      /\breport\(\s*'([^'\n]+)'/g, // the broker adapters' report(event, error)
      /\bfault\(\s*'([^'\n]+)'/g, // the ws / wt engines' #fault(event, error)
      /\bescalate\(\s*\w+,\s*'([^'\n]+)'/g, // the client's #escalate(error, event)
      /\bevent = '([^'\n]+)'/g, // …and its default
      /\brunDelivery\([^)]*?'([^'\n]+)'/g, // the adapters' handler-crash tail
      /\brefuseCall\([^)\n]*?'([^'\n]+)'\)/g, // the dispatcher's refusals
      /\bevent: [^,'\n]+ \? '([^'\n]+)' : '[^'\n]+'/g, // a name chosen by a condition…
      /\bevent: [^,'\n]+ \? '[^'\n]+' : '([^'\n]+)'/g, // …either branch
    ];
    // Names built at run time, spelled out: `broker.${action}` for the three
    // outcomes that are not `dead` (which has a line of its own), and the
    // envelope's `${event}.unsealed|keys|open` under its two prefixes.
    const COMPUTED = [
      'broker.ack',
      'broker.retry',
      'broker.release',
      ...['backplane', 'cluster'].flatMap((prefix) => ['unsealed', 'keys', 'open'].map((kind) => `${prefix}.${kind}`)),
    ];
    // Matched by a sink, yet not a line: createEnvelope's `event` OPTION is
    // the prefix of the three computed names above.
    const NOT_EVENTS = new Set(['backplane']);

    const written = new Map();
    for (const file of walk('src')) {
      const text = readFileSync(path.join(ROOT, file), 'utf8');
      for (const sink of SINKS) {
        for (const match of text.matchAll(sink)) {
          if (!NOT_EVENTS.has(match[1]) && !written.has(match[1])) written.set(match[1], file);
        }
      }
    }
    for (const name of COMPUTED) written.set(name, '(computed)');
    assert.ok(written.size > 200, `the sinks found only ${written.size} events — a pattern stopped matching`);

    const guide = read('docs/guide/logging.md');
    const mentioned = new Set(Array.from(guide.matchAll(/`([a-z][A-Za-z0-9.-]*)`/g), (match) => match[1]));
    const undocumented = Array.from(written)
      .filter(([name]) => !mentioned.has(name))
      .map(([name, file]) => `${name} (${file})`);
    assert.deepStrictEqual(undocumented, [], 'log events with no row in docs/guide/logging.md');

    // The other direction: the first cell of every catalogue row.
    const catalogue = guide.slice(guide.indexOf('## Event catalogue'), guide.indexOf('### Why some refusals are'));
    const stale = [];
    for (const row of catalogue.matchAll(/^\| (`[^|]+) \|/gm)) {
      if (row[1] === '`event`') continue; // a table's header
      for (const cell of row[1].matchAll(/`([^`]+)`/g)) if (!written.has(cell[1])) stale.push(cell[1]);
    }
    assert.deepStrictEqual(stale, [], 'catalogue rows for events the source no longer writes');
  });

  // docs/guide/telemetry.md is the catalogue of what the telemetry writers emit
  // — the instrument names dashboards are built on, and the attribute keys
  // their series are split by. Ten keys were in the source and nowhere in the
  // docs; this keeps the two lists one, in both directions.
  it('should name in the telemetry guide every instrument and attribute the source writes, and nothing it does not', () => {
    const { readdirSync, readFileSync } = require('node:fs');
    const walk = (dir, out = []) => {
      for (const entry of readdirSync(path.join(ROOT, dir), { withFileTypes: true })) {
        const relative = `${dir}/${entry.name}`;
        if (entry.isDirectory()) walk(relative, out);
        else if (entry.name.endsWith('.js')) out.push(relative);
      }
      return out;
    };
    const NAME = "'((?:wrpc|rpc|messaging|network|error)\\.[a-z0-9_.]+)'";
    // How a name becomes telemetry in src/: an instrument is created by name,
    // an attribute is a key of an object literal, a key assigned by index, or
    // the first argument of setAttribute.
    const INSTRUMENT = new RegExp(`\\bcreate(?:Counter|Histogram|UpDownCounter)\\(\\s*${NAME}`, 'g');
    const ATTRIBUTES = [
      new RegExp(`${NAME}\\s*:`, 'g'),
      new RegExp(`\\[${NAME}\\]\\s*=`, 'g'),
      new RegExp(`setAttribute\\??\\.?\\(\\s*${NAME}`, 'g'),
    ];
    const instruments = new Map();
    const attributes = new Map();
    for (const file of walk('src')) {
      const text = readFileSync(path.join(ROOT, file), 'utf8');
      for (const match of text.matchAll(INSTRUMENT)) instruments.set(match[1], file);
      for (const pattern of ATTRIBUTES) {
        for (const match of text.matchAll(pattern)) if (!attributes.has(match[1])) attributes.set(match[1], file);
      }
    }
    assert.ok(instruments.size >= 39, `found only ${instruments.size} instruments — a pattern stopped matching`);
    assert.ok(attributes.size >= 40, `found only ${attributes.size} attribute keys — a pattern stopped matching`);

    const guide = read('docs/guide/telemetry.md');
    const mentioned = new Set(Array.from(guide.matchAll(/`([a-z][a-z0-9_.]*)`/g), (match) => match[1]));
    const missing = (names) =>
      Array.from(names)
        .filter(([name]) => !mentioned.has(name))
        .map(([name, file]) => `${name} (${file})`);
    assert.deepStrictEqual(missing(attributes), [], 'attribute keys with no mention in docs/guide/telemetry.md');

    // Instruments: the first cell of every row of the Metrics table — the
    // block right under the heading — both ways.
    const table = guide.slice(guide.indexOf('## Metrics')).split('\n\n')[1];
    const rows = new Set(Array.from(table.matchAll(/^\| `([^`]+)` \|/gm), (match) => match[1]));
    assert.deepStrictEqual(
      Array.from(instruments.keys()).filter((name) => !rows.has(name)),
      [],
      'instruments with no row in the Metrics table',
    );
    assert.deepStrictEqual(
      Array.from(rows).filter((name) => !instruments.has(name)),
      [],
      'Metrics rows for instruments the source no longer creates',
    );

    // And the other direction for everything else the guide names in these
    // namespaces: an attribute that was renamed or removed leaves its mention
    // behind. `wrpc.rtc.*` and `wrpc.broker.*` are how the prose names a family.
    const stale = Array.from(mentioned).filter(
      (name) =>
        /^(?:wrpc|rpc|messaging|network)\.[a-z0-9_.]+$/.test(name) &&
        !name.endsWith('.') &&
        !instruments.has(name) &&
        !attributes.has(name),
    );
    assert.deepStrictEqual(stale, [], 'names in the telemetry guide that the source no longer writes');
  });
});

describe('the CHANGELOG', () => {
  it('should keep every **Breaking entry of [Unreleased] under ### Changed (breaking)', () => {
    const sections = unreleasedSections();
    const breaking = sections.get('Changed (breaking)');
    assert.ok(breaking, '[Unreleased] must keep a "### Changed (breaking)" section (empty is fine)');
    for (const [heading, lines] of sections) {
      if (heading === 'Changed (breaking)') continue;
      const marked = lines.filter((line) => line.includes('**Breaking'));
      assert.deepStrictEqual(
        marked,
        [],
        `"### ${heading}" holds a **Breaking entry; move it to "### Changed (breaking)"`,
      );
    }
    // The migration guide belongs to a breaking section that HAS entries: a
    // reader who lands on "what broke" finds "what to do" in the section after
    // it. The fresh, empty section a release opens needs none.
    if (breaking.some((line) => line.startsWith('- '))) {
      assert.ok(
        Array.from(sections.keys()).some((heading) => heading.startsWith('Upgrading from ')),
        'a breaking section with entries must come with an "### Upgrading from <version>" section',
      );
    }
  });

  it('should use the headings of Keep a Changelog and link every version', () => {
    const changelog = read('CHANGELOG.md');
    const versions = [];
    for (const line of changelog.split('\n')) {
      const version = /^## \[([^\]]+)\]/.exec(line);
      if (version) versions.push(version[1]);
      else if (line.startsWith('## ')) assert.fail(`a version heading is "## [X.Y.Z] - YYYY-MM-DD": ${line}`);
      if (!line.startsWith('### ')) continue;
      const heading = line.slice(4).trim();
      assert.ok(
        HEADINGS.has(heading) || /^Upgrading from \d+\.\d+$/.test(heading),
        `a heading Keep a Changelog has not: ${line}`,
      );
    }
    assert.equal(versions[0], 'Unreleased');
    for (const version of versions) {
      assert.match(
        changelog,
        new RegExp(`^\\[${version.replaceAll('.', '\\.')}\\]: https://github\\.com/`, 'm'),
        `no link for [${version}]`,
      );
    }
    assert.doesNotMatch(changelog, /^#### /m, 'a level-4 heading: write the section at level 3');
  });
});

describe('the source', () => {
  it('should match RPC_OPTION_KEYS to the RpcServer constructor destructure', () => {
    const core = read('src/rpc/core.js');
    const keysStart = core.indexOf('const RPC_OPTION_KEYS = [');
    const keys = [...core.slice(keysStart, core.indexOf(']', keysStart)).matchAll(/'([^']+)'/g)].map((m) => m[1]);
    assert.ok(keys.length > 0);
    const ctorStart = core.indexOf('constructor(options = {})');
    const destructure = core.slice(core.indexOf('const {', ctorStart), core.indexOf('} = options;', ctorStart));
    const names = [...destructure.matchAll(/^\s{6}(\w+)/gm)].map((m) => m[1]);
    // Set equality both ways: a key the shells would drop (in the constructor
    // but not the list) and a stale key (listed but no longer destructured)
    // are both silent bugs.
    assert.deepStrictEqual([...names].sort(), [...keys].sort());
  });

  it('should read no queue in src/ with shift() and spread no array into a call, except where a bound is written down', () => {
    const { readdirSync, readFileSync } = require('node:fs');
    const { codeOf, callSpreads } = require('./scan.js');
    const walk = (dir, out = []) => {
      for (const entry of readdirSync(path.join(ROOT, dir), { withFileTypes: true })) {
        const relative = `${dir}/${entry.name}`;
        if (entry.isDirectory()) walk(relative, out);
        else if (entry.name.endsWith('.js')) out.push(relative);
      }
      return out;
    };
    const flatten = (lists) => {
      const keys = [];
      for (const file of Object.keys(lists)) {
        for (const line of Object.keys(lists[file])) keys.push(`${file} :: ${line}`);
      }
      return keys.sort();
    };
    const heads = new Set();
    const spreads = new Set();
    for (const file of walk('src')) {
      const text = readFileSync(path.join(ROOT, file), 'utf8');
      const code = codeOf(text);
      const lines = text.split('\n');
      const lineAt = (at) => lines[code.slice(0, at).split('\n').length - 1].trim();
      for (const match of code.matchAll(/\.(?:shift|unshift)\(/g)) heads.add(`${file} :: ${lineAt(match.index)}`);
      for (const at of callSpreads(code)) spreads.add(`${file} :: ${lineAt(at)}`);
    }
    assert.deepStrictEqual(
      Array.from(heads).sort(),
      flatten(HEAD_READS),
      'shift() / unshift() sites against HEAD_READS',
    );
    assert.deepStrictEqual(
      Array.from(spreads).sort(),
      flatten(CALL_SPREADS),
      'spreads into a call against CALL_SPREADS',
    );
  });

  it('should have the scanner behind that guard tell a call from a parameter list, an object and a string', () => {
    const { codeOf, callSpreads } = require('./scan.js');
    const found = (source) => {
      const code = codeOf(source);
      return callSpreads(code).map((at) =>
        source.slice(at, source.indexOf('\n', at) === -1 ? undefined : source.indexOf('\n', at)),
      );
    };
    assert.deepStrictEqual(found('f(...a);'), ['...a);']);
    assert.deepStrictEqual(found('new F(a, ...b);'), ['...b);']);
    assert.deepStrictEqual(found('f(\n  a,\n  ...b,\n);').length, 1);
    assert.deepStrictEqual(found('const f = (...args) => g(1);'), []);
    assert.deepStrictEqual(found('class A { to(...rooms) { return 1; } }'), []);
    assert.deepStrictEqual(found('function f(...args) { return args; }'), []);
    assert.deepStrictEqual(found('f({ ...a }); f([...a]); const b = { ...a }; const c = [...a];'), []);
    // A template's placeholder, spelled so that this file's own strings hold none.
    const OPEN = String.fromCharCode(0x24, 0x7b); // a dollar sign and an opening brace
    assert.deepStrictEqual(
      found(`f('(...a)'); f(\`(...a) ${OPEN}x} (...b)\`); // g(...c)\n/* h(...d) */ const r = /\\(\\.\\.\\.e\\)/;`),
      [],
    );
    assert.deepStrictEqual(found(`f(\`${OPEN}g(...a)}\`);`), ['...a)}`);']);
    assert.deepStrictEqual(found('const f = (a, ...rest) => concat(a, ...rest);'), ['...rest);']);
  });
});

describe('CLAUDE.md guidance', () => {
  it('should name symbols, not line numbers, in every CLAUDE.md, with paths that exist and symbols where it says', () => {
    const { readFileSync } = require('node:fs');
    const files = guidanceFiles();
    assert.ok(files.includes('CLAUDE.md') && files.includes('src/CLAUDE.md'), 'the root and src/ files are there');
    assert.ok(files.length >= 15, `only ${files.length} CLAUDE.md files were found`);

    const numbered = [];
    const missing = [];
    const adrift = [];
    let pairs = 0;
    for (const file of files) {
      const guide = read(file);
      for (const match of guide.matchAll(/`((?:src|bench|tests|scripts)\/[\w/.-]+:\d+)`/g)) {
        numbered.push(`${file}: ${match[1]}`);
      }
      // A backticked path: starts in a tracked directory, has an extension or a
      // trailing slash, and is not a pattern ({a,b}, *, <placeholder>, …).
      for (const match of guide.matchAll(/`((?:src|bench|tests|scripts|docs|bin|examples)\/[^`\s]*)`/g)) {
        const mentioned = match[1];
        if (/[{}*<>$|]|\.\.\./.test(mentioned)) continue;
        const clean = mentioned.replace(/[),.:;]+$/, '');
        if (!existsSync(path.join(ROOT, clean))) missing.push(`${file}: ${mentioned}`);
      }
      // "`symbol` in `path`": the symbol's text occurs in that file.
      for (const match of guide.matchAll(/`([^`\n]+)` in `((?:src|bench|tests|scripts)\/[\w/.-]+\.js)`/g)) {
        const [, symbol, target] = match;
        if (!existsSync(path.join(ROOT, target))) continue; // reported above
        pairs++;
        // A function may be written as a call — `nodeStream()` — in prose.
        const name = symbol.replace(/\(\)$/, '');
        if (!readFileSync(path.join(ROOT, target), 'utf8').includes(name)) {
          adrift.push(`${file}: ${symbol} in ${target}`);
        }
      }
    }
    assert.deepStrictEqual(numbered, [], 'file:line references in a CLAUDE.md — name the symbol instead');
    assert.deepStrictEqual(missing, [], 'paths a CLAUDE.md names that do not exist');
    assert.deepStrictEqual(adrift, [], 'symbols a CLAUDE.md places in a file that no longer has them');
    assert.ok(pairs >= 15, `only ${pairs} symbol-in-file references were found — the pattern stopped matching`);
  });

  // The split exists to keep what EVERY task reads small: the root file is in
  // every session, src/CLAUDE.md in every one that touches the source. One
  // file held 85 KB — a single bullet of it 15 — before it was split by
  // folder. New detail goes into the folder's own file; these two only shrink
  // or stay.
  it('should keep the two CLAUDE.md files every task reads small, and give every source folder that needs one its own', () => {
    const { statSync } = require('node:fs');
    const size = (file) => statSync(path.join(ROOT, file)).size;
    assert.ok(
      size('CLAUDE.md') <= 17 * 1024,
      `CLAUDE.md is ${size('CLAUDE.md')} bytes — move detail into a folder's file`,
    );
    assert.ok(size('src/CLAUDE.md') <= 10 * 1024, `src/CLAUDE.md is ${size('src/CLAUDE.md')} bytes`);
    const root = read('CLAUDE.md');
    for (const file of guidanceFiles()) {
      if (file === 'CLAUDE.md') continue;
      const folder = `${path.dirname(file)}/`;
      assert.ok(root.includes(`\`${folder}\``), `${file} is not in the root CLAUDE.md's table`);
    }
  });

  // `files` ships src/ and bin/ whole, and the guidance lives next to the code
  // it is about: what keeps it out of the tarball is the `!**/CLAUDE.md` entry.
  // Asked of pnpm itself — the packing list of what `pnpm publish` ships — not of
  // the pattern.
  it('should ship no CLAUDE.md in the tarball', () => {
    const { spawnSync } = require('node:child_process');
    assert.ok(pkg.files.includes('!**/CLAUDE.md'), 'the files allowlist ends with the CLAUDE.md exclusion');
    assert.ok(
      guidanceFiles().some((file) => file.startsWith('src/')),
      'there are CLAUDE.md files under src/ to keep out',
    );
    const args = ['--config.ignore-scripts=true', 'pack', '--dry-run', '--json'];
    const packed = spawnSync('pnpm', args, { cwd: ROOT, encoding: 'utf8' });
    assert.strictEqual(packed.status, 0, packed.stderr);
    const shippedFiles = JSON.parse(packed.stdout).files.map((entry) => entry.path);
    assert.ok(shippedFiles.length > 150, `pnpm lists only ${shippedFiles.length} files`);
    assert.deepStrictEqual(
      shippedFiles.filter((file) => /(^|\/)CLAUDE\.md$/i.test(file) || file.startsWith('.claude/')),
      [],
    );
  });
});
