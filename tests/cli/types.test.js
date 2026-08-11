'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const os = require('node:os');
const { mkdtemp, readFile, rm } = require('node:fs/promises');

const {
  CliError,
  MAX_DEPTH,
  USAGE,
  fetchIntrospection,
  main,
  parseArgs,
  quoteKey,
  renderType,
  renderTypes,
} = require('../../src/cli/types.js');

const collect = () => {
  const lines = [];
  const io = (text) => void lines.push(String(text));
  io.lines = lines;
  io.text = () => lines.join('\n');
  return io;
};

const warner = () => {
  const warnings = [];
  const warn = (message) => void warnings.push(message);
  warn.warnings = warnings;
  warn.matching = (needle) => warnings.filter((message) => message.includes(needle));
  return warn;
};

const answer = (packet, overrides = {}) => ({
  ok: true,
  status: 200,
  statusText: 'OK',
  text: async () => JSON.stringify(packet),
  ...overrides,
});

// ---------------------------------------------------------------------------
// argv

test('CLI parseArgs: the happy paths', () => {
  const parsed = parseArgs(['types', 'http://h/api', '--out', 'api.d.ts']);
  assert.strictEqual(parsed.url, 'http://h/api');
  assert.strictEqual(parsed.out, 'api.d.ts');
  assert.strictEqual(parsed.interfaceName, 'Api');
  assert.strictEqual(parsed.packageName, '@alexify/wrpc');
  assert.deepStrictEqual(parsed.units, []);

  const full = parseArgs([
    'types',
    'http://h/api',
    '--out',
    '-',
    '--units',
    'chat, auth.1',
    '--units',
    'presence',
    '--interface',
    'Contract',
    '--package',
    '../wrpc',
  ]);
  assert.strictEqual(full.out, '-');
  assert.deepStrictEqual(full.units, ['chat', 'auth.1', 'presence']);
  assert.strictEqual(full.interfaceName, 'Contract');
  assert.strictEqual(full.packageName, '../wrpc');

  assert.deepStrictEqual(parseArgs(['--help']), { help: true });
  assert.deepStrictEqual(parseArgs(['-h']), { help: true });
  assert.deepStrictEqual(parseArgs(['--version']), { version: true });
  assert.deepStrictEqual(parseArgs(['-v']), { version: true });
  // --help wins wherever it appears, even after a broken argument list
  assert.deepStrictEqual(parseArgs(['types', '--help']), { help: true });
});

test("CLI parseArgs: '--' ends option parsing", () => {
  const parsed = parseArgs(['--out', 'api.d.ts', '--', 'types', 'http://h/api']);
  assert.strictEqual(parsed.url, 'http://h/api');
  // ...so a URL that looks like a flag is still a URL
  const dashed = parseArgs(['--out', '-', '--', 'types', '--weird-host/api']);
  assert.strictEqual(dashed.url, '--weird-host/api');
});

test('CLI parseArgs: rejects every malformed invocation', () => {
  const rejects = (argv, needle) => {
    assert.throws(
      () => parseArgs(argv),
      (error) => {
        assert.ok(error instanceof CliError, `${needle}: expected a CliError, got ${error}`);
        assert.match(error.message, needle);
        return true;
      },
      `expected ${JSON.stringify(argv)} to be rejected`,
    );
  };

  rejects([], /Missing command/);
  rejects(['serve', 'http://h', '--out', 'x'], /Unknown command 'serve'/);
  rejects(['types'], /Missing <url>/);
  rejects(['types', 'http://h'], /Missing --out/);
  rejects(['types', 'http://h', 'extra', '--out', 'x'], /Unexpected argument 'extra'/);
  rejects(['types', 'http://h', '--nope'], /Unknown option --nope/);
  // A flag with no value must not swallow the next flag
  rejects(['types', 'http://h', '--out'], /--out requires a value/);
  rejects(['types', 'http://h', '--out', '--units', 'chat'], /--out requires a value/);
  rejects(['types', 'http://h', '--units'], /--units requires a value/);
  rejects(['types', 'http://h', '--out', 'x', '--units', 'chat,,auth'], /must not contain an empty unit/);
  rejects(['types', 'http://h', '--out', 'x', '--interface', 'not an identifier'], /valid TypeScript identifier/);
  rejects(['types', 'http://h', '--out', 'x', '--interface', '1Api'], /valid TypeScript identifier/);
});

// ---------------------------------------------------------------------------
// rendering

test('CLI quoteKey: bare identifiers, safely quoted everything else', () => {
  assert.strictEqual(quoteKey('chat'), 'chat');
  assert.strictEqual(quoteKey('_$x0'), '_$x0');
  assert.strictEqual(quoteKey('auth.1'), "'auth.1'");
  assert.strictEqual(quoteKey('weird-name'), "'weird-name'");
  assert.strictEqual(quoteKey('1st'), "'1st'");
  // Anything that needed escaping keeps JSON's quoting, which is what makes it safe
  assert.strictEqual(quoteKey('a\nb'), '"a\\nb"');
  assert.strictEqual(quoteKey('x"y'), '"x\\"y"');
  assert.strictEqual(quoteKey('back\\slash'), '"back\\\\slash"');
  assert.strictEqual(quoteKey("it's"), '"it\'s"');
});

test('CLI renderType: type names, arrays and unions', () => {
  const warn = warner();
  assert.strictEqual(renderType('string', warn), 'string');
  assert.strictEqual(renderType('number[]', warn), 'number[]');
  assert.strictEqual(renderType('boolean[][]', warn), 'boolean[][]');
  assert.strictEqual(renderType('string|number', warn), 'string | number');
  assert.strictEqual(renderType(' string | null ', warn), 'string | null');
  assert.strictEqual(renderType(['string'], warn), 'Array<string>');
  assert.strictEqual(renderType([{ id: 'string' }], warn), 'Array<{ id: string }>');
  assert.deepStrictEqual(warn.warnings, []);
});

test('CLI renderType: an unknown type name degrades to unknown and says so', () => {
  const warn = warner();
  // The allowlist is the whole defence: a descriptor is data, and this is
  // where a hostile one would otherwise become code.
  assert.strictEqual(renderType('string; export const boom = 1; type Z = ', warn), 'unknown');
  assert.strictEqual(renderType('Date', warn), 'unknown');
  assert.strictEqual(renderType('string|', warn), 'unknown');
  assert.strictEqual(renderType('str ing', warn), 'unknown');
  assert.strictEqual(renderType('string[', warn), 'unknown');
  assert.strictEqual(warn.matching('unknown type').length, 5);
});

test('CLI renderType: field maps, optional keys, empty cases', () => {
  const warn = warner();
  assert.strictEqual(renderType({ a: 'string', 'b?': 'number' }, warn), '{ a: string; b?: number }');
  assert.strictEqual(renderType({ 'weird-key': 'string' }, warn), "{ 'weird-key': string }");
  // No fields declared means exactly that; 'object' is how you say "unspecified"
  assert.strictEqual(renderType({}, warn), 'Record<string, never>');
  assert.strictEqual(renderType('object', warn), 'object');
  assert.deepStrictEqual(warn.warnings, []);

  assert.strictEqual(renderType({ '?': 'string' }, warn), 'Record<string, never>');
  assert.strictEqual(warn.matching('empty name').length, 1);
});

test('CLI renderType: refuses shapes it cannot mean', () => {
  const warn = warner();
  // An array describes an array OF one shape, so a tuple is not a descriptor
  assert.strictEqual(renderType(['string', 'number'], warn), 'unknown');
  assert.strictEqual(renderType([], warn), 'unknown');
  assert.strictEqual(warn.matching('exactly one entry').length, 2);
  assert.strictEqual(renderType(42, warn), 'unknown');
  assert.strictEqual(renderType(null, warn), 'unknown');
  assert.strictEqual(renderType(true, warn), 'unknown');
  assert.strictEqual(warn.matching('must be a type name').length, 3);
});

test('CLI renderType: nesting is capped', () => {
  const warn = warner();
  const deep = (depth) => (depth === 0 ? 'string' : { nested: deep(depth - 1) });
  const rendered = renderType(deep(MAX_DEPTH + 4), warn);
  assert.strictEqual(warn.matching(`deeper than ${MAX_DEPTH}`).length, 1);
  // Capped, not truncated silently: the innermost level says 'unknown'
  assert.match(rendered, /unknown/);
  assert.strictEqual(warner().warnings.length, 0);
  const shallow = warner();
  assert.strictEqual(renderType(deep(1), shallow), '{ nested: string }');
  assert.deepStrictEqual(shallow.warnings, []);
});

test('CLI renderTypes: a whole introspection becomes a contract interface', () => {
  const warn = warner();
  const text = renderTypes(
    {
      chat: {
        send: {
          access: 'public',
          meta: { description: 'Post a message' },
          signature: { args: { text: 'string', 'replyTo?': 'string' }, returns: { id: 'string' } },
        },
        ping: { access: 'public' },
        onMessage: {
          access: 'public',
          kind: 'subscription',
          signature: { args: { room: 'string' }, data: { text: 'string' } },
        },
      },
      'auth.1': { signIn: { access: 'public', signature: { args: { login: 'string' } } } },
    },
    { url: 'http://127.0.0.1:8000/api', warn },
  );

  assert.deepStrictEqual(warn.warnings, []);
  assert.strictEqual(
    text,
    [
      '// Generated by `wrpc types`. Do not edit — re-run the command instead.',
      '// Source: http://127.0.0.1:8000/api',
      '',
      "import type { SubscriptionContract } from '@alexify/wrpc';",
      '',
      'export interface Api {',
      "  'auth.1': {",
      '    signIn(args: { login: string }): Promise<unknown>;',
      '  };',
      '  chat: {',
      '    onMessage: SubscriptionContract<{ room: string }, { text: string }>;',
      '    ping(args?: unknown): Promise<unknown>;',
      '    /** Post a message */',
      '    send(args: { text: string; replyTo?: string }): Promise<{ id: string }>;',
      '  };',
      '}',
      '',
    ].join('\n'),
  );
});

test('CLI renderTypes: no subscriptions means no import, and the name is configurable', () => {
  const text = renderTypes(
    { chat: { ping: { access: 'public' } } },
    {
      url: 'http://h/api',
      interfaceName: 'Contract',
      packageName: '../../index.js',
    },
  );
  assert.ok(!text.includes('import type'), 'an unused import is noise in a generated file');
  assert.match(text, /export interface Contract \{/);

  const withSub = renderTypes({ feed: { onTick: { kind: 'subscription' } } }, { packageName: '../../index.js' });
  assert.match(withSub, /import type \{ SubscriptionContract \} from '\.\.\/\.\.\/index\.js';/);
  // Undeclared subscription args stay callable both ways: `any`, not `unknown`
  assert.match(withSub, /onTick: SubscriptionContract<any, unknown>;/);
});

test('CLI renderTypes: hostile names and values cannot escape into code', () => {
  const warn = warner();
  // The real path is JSON.parse, where '__proto__' is an OWN property rather
  // than the prototype setter an object literal would hit.
  const introspection = JSON.parse(
    JSON.stringify({
      '*/ export const pwned = 1; /*': { m: { access: 'public' } },
      'line\nbreak': { m: { access: 'public' } },
      evil: {
        m: {
          access: 'public',
          meta: { description: 'closes */ the comment' },
          // Backticks and template syntax in a field name must stay inert text.
          // eslint-disable-next-line no-template-curly-in-string
          signature: { args: { '`tpl`': 'string', '${p}': 'number' } },
        },
      },
    }),
  );
  introspection.__proto__ = { m: { access: 'public' } }; // eslint-disable-line no-proto
  const text = renderTypes(introspection, { url: 'http://h/api', warn });

  // The hostile name survives as DATA — a quoted key — and never as code:
  // no generated line begins with it, so nothing it contains is a statement.
  assert.match(text, /^ {2}'\*\/ export const pwned = 1; \/\*': \{$/m);
  assert.ok(
    !text.split('\n').some((line) => line.trim().startsWith('export const pwned')),
    'a unit name must never become a statement',
  );
  assert.match(text, /"line\\nbreak": \{/);
  assert.ok(!text.split('\n').some((line) => line.trim() === 'break": {'), 'a newline must not reach the output');
  assert.match(text, /\/\*\* closes \* \/ the comment \*\//);
  assert.match(text, /'`tpl`': string; '\$\{p\}': number/);
  assert.strictEqual(Object.prototype.m, undefined, 'nothing may be written through __proto__');

  // ...and the whole thing still parses as TypeScript-shaped text: every
  // generated line is a declaration, a brace, a comment or an import.
  for (const line of text.split('\n')) {
    if (line.trim() === '') continue;
    assert.match(
      line,
      /^(\/\/|\s*\/\*\*|import type |export interface |\s*[$\w'"[]|\s*\};?$|\})/,
      `stray line: ${line}`,
    );
  }
});

test('CLI renderTypes: skips what it cannot describe, loudly', () => {
  const warn = warner();
  const text = renderTypes(
    {
      broken: 'not an object',
      unit: { notAMethod: 7, ok: { access: 'public' } },
      // A unit whose only declarations are inbound events has no methods at all
      eventsOnly: {},
      mixed: {
        call: { access: 'public', signature: { data: 'string' } },
        feed: { kind: 'subscription', signature: { returns: 'string', data: 'string' } },
        bad: { access: 'public', signature: 'nope' },
      },
    },
    { warn },
  );

  assert.strictEqual(warn.matching("unit 'broken' is not an object").length, 1);
  assert.strictEqual(warn.matching('unit/notAMethod is not a method descriptor').length, 1);
  assert.strictEqual(warn.matching("mixed/call: 'data' means nothing for a call").length, 1);
  assert.strictEqual(warn.matching("mixed/feed: 'returns' means nothing for a subscription").length, 1);
  assert.strictEqual(warn.matching('mixed/bad: signature must be an object').length, 1);
  assert.ok(!text.includes('broken'));
  assert.match(text, /eventsOnly: Record<string, never>;/);
  assert.match(text, /ok\(args\?: unknown\): Promise<unknown>;/);
});

test('CLI renderTypes: an introspection that is not an object is a failure, not an empty file', () => {
  assert.throws(() => renderTypes(null), CliError);
  assert.throws(() => renderTypes('nope'), /did not answer introspection with an object/);
});

test('CLI renderTypes: diagnostics are optional — no warn means no crash', () => {
  // Anything worth warning about must still render, whether anyone is
  // listening or not: renderTypes is used by the CLI, which passes a warn,
  // and directly by tests and tooling, which need not.
  const text = renderTypes({ broken: 'not an object', ok: { m: { access: 'public', signature: { args: 'Date' } } } });
  assert.match(text, /m\(args: unknown\): Promise<unknown>;/);
  assert.ok(!text.includes('broken'));
});

test('CLI renderTypes: the source comment drops credentials, query strings and junk', () => {
  assert.match(
    renderTypes({}, { url: 'http://user:pw@h:8000/api?token=secret' }),
    /\/\/ Source: http:\/\/h:8000\/api$/m,
  );
  // Not a URL at all: kept verbatim, but still sanitised
  assert.match(renderTypes({}, { url: 'not a url' }), /\/\/ Source: not a url$/m);
  assert.match(renderTypes({}, { url: 'a\nb' }), /\/\/ Source: a b$/m);
  assert.match(renderTypes({}), /\/\/ Source: $/m);
});

// ---------------------------------------------------------------------------
// talking to a server

test('CLI fetchIntrospection: sends a call packet and returns the result', async () => {
  const calls = [];
  const doFetch = async (url, options) => {
    calls.push({ url, options });
    return answer({ type: 'callback', id: '1', result: { chat: {} } });
  };
  assert.deepStrictEqual(await fetchIntrospection('http://h/api', [], doFetch), { chat: {} });
  assert.strictEqual(calls[0].url, 'http://h/api');
  assert.strictEqual(calls[0].options.method, 'POST');
  assert.strictEqual(calls[0].options.headers['Content-Type'], 'application/json');
  const sent = JSON.parse(calls[0].options.body);
  assert.strictEqual(sent.method, 'system/introspect');
  // No filter must NOT be an empty array: introspect() would filter every unit
  // away and answer with nothing.
  assert.deepStrictEqual(sent.args, {});

  await fetchIntrospection('http://h/api', ['chat'], doFetch);
  assert.deepStrictEqual(JSON.parse(calls[1].options.body).args, ['chat']);
});

test('CLI fetchIntrospection: every way the server can disappoint', async () => {
  const rejects = async (doFetch, needle) => {
    await assert.rejects(
      () => fetchIntrospection('http://h/api', [], doFetch),
      (error) => {
        assert.ok(error instanceof CliError, `${needle}: expected a CliError, got ${error}`);
        assert.match(error.message, needle);
        return true;
      },
    );
  };

  await rejects(async () => {
    throw new Error('ECONNREFUSED');
  }, /Cannot reach http:\/\/h\/api: ECONNREFUSED/);
  await rejects(async () => ({ ok: false, status: 404, statusText: 'Not Found' }), /answered 404 Not Found/);
  await rejects(async () => ({ ok: false, status: 500, statusText: '' }), /answered 500$/);
  await rejects(async () => answer(null, { text: async () => 'not json' }), /answered something that is not JSON/);
  await rejects(
    async () => answer({ type: 'callback', id: '1', error: { message: 'forbidden', code: 403 } }),
    /refused with 403: forbidden/,
  );
  await rejects(async () => answer({ type: 'callback', id: '1', error: {} }), /refused with 500: unknown error/);
  await rejects(async () => answer({ type: 'end', id: '1' }), /answered 'end' instead of a callback/);
});

// ---------------------------------------------------------------------------
// the command

test('CLI main: --help and --version answer on stdout with code 0', async () => {
  const log = collect();
  assert.strictEqual(await main(['--help'], { log }), 0);
  assert.strictEqual(log.text(), USAGE);

  const versionLog = collect();
  assert.strictEqual(await main(['--version'], { log: versionLog, version: () => '9.9.9' }), 0);
  assert.strictEqual(versionLog.text(), '9.9.9');
});

test('CLI main: --version with no injected reader reports the package version', async () => {
  // Covers the default `version` and `log`: both are resolved at call time,
  // so swapping console.log is enough to keep the run quiet.
  const { version } = require('../../package.json');
  const lines = [];
  const original = console.log;
  console.log = (text) => void lines.push(String(text));
  try {
    assert.strictEqual(await main(['--version']), 0);
  } finally {
    console.log = original;
  }
  assert.deepStrictEqual(lines, [version]);
});

test('CLI main: writes the declaration file', async () => {
  const written = [];
  const doFetch = async () => answer({ type: 'callback', id: '1', result: { chat: { ping: { access: 'public' } } } });
  const code = await main(['types', 'http://h/api', '--out', 'api.d.ts'], {
    log: collect(),
    error: collect(),
    fetch: doFetch,
    write: async (file, text) => void written.push({ file, text }),
  });
  assert.strictEqual(code, 0);
  assert.strictEqual(written.length, 1);
  assert.strictEqual(written[0].file, 'api.d.ts');
  assert.match(written[0].text, /export interface Api \{/);
  assert.ok(written[0].text.endsWith('\n'), 'a generated file ends with a newline');
});

test("CLI main: '--out -' writes to stdout instead of a file", async () => {
  const log = collect();
  const doFetch = async () => answer({ type: 'callback', id: '1', result: { chat: { ping: { access: 'public' } } } });
  const code = await main(['types', 'http://h/api', '--out', '-'], {
    log,
    error: collect(),
    fetch: doFetch,
    write: async () => assert.fail('nothing may be written when --out is -'),
  });
  assert.strictEqual(code, 0);
  assert.match(log.text(), /export interface Api \{/);
  assert.ok(!log.text().endsWith('\n'), 'console.log adds the trailing newline itself');
});

test('CLI main: the real fs writer', async () => {
  // Covers the default `write`, which is node:fs/promises.writeFile.
  const dir = await mkdtemp(path.join(os.tmpdir(), 'wrpc-cli-'));
  const file = path.join(dir, 'api.d.ts');
  try {
    const doFetch = async () => answer({ type: 'callback', id: '1', result: { chat: { ping: { access: 'public' } } } });
    const code = await main(['types', 'http://h/api', '--out', file], {
      log: collect(),
      error: collect(),
      fetch: doFetch,
    });
    assert.strictEqual(code, 0);
    assert.match(await readFile(file, 'utf8'), /ping\(args\?: unknown\): Promise<unknown>;/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('CLI main: warnings go to stderr and do not fail the run', async () => {
  const error = collect();
  const doFetch = async () =>
    answer({
      type: 'callback',
      id: '1',
      result: { chat: { ping: { access: 'public', signature: { args: 'Date' } } } },
    });
  const code = await main(['types', 'http://h/api', '--out', '-'], {
    log: collect(),
    error,
    fetch: doFetch,
    write: async () => {},
  });
  assert.strictEqual(code, 0);
  assert.match(error.text(), /wrpc types: unknown type 'Date'/);
});

test('CLI main: failures answer 1 with a one-line message', async () => {
  const error = collect();
  assert.strictEqual(await main(['types', 'http://h/api'], { log: collect(), error }), 1);
  assert.deepStrictEqual(error.lines, ["wrpc types: Missing --out <path>. Use '--out -' to write to stdout."]);

  const unreachable = collect();
  const code = await main(['types', 'http://h/api', '--out', 'x'], {
    log: collect(),
    error: unreachable,
    fetch: async () => {
      throw new Error('ECONNREFUSED');
    },
  });
  assert.strictEqual(code, 1);
  assert.match(unreachable.text(), /Cannot reach http:\/\/h\/api/);
});

test('CLI main: a path it cannot write to is a message, not a stack', async () => {
  // The commonest real failure — a mistyped --out — must not answer with ENOENT
  // and a stack trace.
  const error = collect();
  const code = await main(['types', 'http://h/api', '--out', 'no/such/dir/api.d.ts'], {
    log: collect(),
    error,
    fetch: async () => answer({ type: 'callback', id: '1', result: { chat: { ping: { access: 'public' } } } }),
  });
  assert.strictEqual(code, 1);
  assert.match(error.text(), /^wrpc types: Cannot write no\/such\/dir\/api\.d\.ts: /);
  assert.ok(!error.text().includes('    at '), 'an expected failure must not print a stack');
});

test('CLI main: an unexpected failure reports its stack rather than pretending to succeed', async () => {
  const error = collect();
  const code = await main(['types', 'http://h/api', '--out', '-'], {
    error,
    fetch: async () => answer({ type: 'callback', id: '1', result: { chat: { ping: { access: 'public' } } } }),
    // Writing to a closed stdout is the realistic version of this: nothing
    // downstream expects it, so the last-resort handler has to say what broke.
    log: () => {
      throw new TypeError('EPIPE');
    },
  });
  assert.strictEqual(code, 1);
  assert.match(error.text(), /TypeError: EPIPE/);
  assert.match(error.text(), /at /, 'an unexpected failure keeps its stack');
});

test('CLI main: a thrown non-Error still reports on the default stderr', async () => {
  // Covers the default `error` writer and the stackless branch of the last
  // resort handler — something thrown that is not an Error has no `.stack`.
  const lines = [];
  const original = console.error;
  console.error = (text) => void lines.push(String(text));
  try {
    const code = await main(['types', 'http://h/api', '--out', '-'], {
      fetch: async () => answer({ type: 'callback', id: '1', result: { chat: { ping: { access: 'public' } } } }),
      log: () => {
        throw 'just a string'; // eslint-disable-line no-throw-literal
      },
    });
    assert.strictEqual(code, 1);
  } finally {
    console.error = original;
  }
  assert.deepStrictEqual(lines, ['wrpc types: just a string']);
});

test('CLI main: a write that fails with a non-Error still says what it was doing', async () => {
  const error = collect();
  const code = await main(['types', 'http://h/api', '--out', 'x'], {
    log: collect(),
    error,
    fetch: async () => answer({ type: 'callback', id: '1', result: { chat: { ping: { access: 'public' } } } }),
    write: async () => {
      throw 'disk on fire'; // eslint-disable-line no-throw-literal
    },
  });
  assert.strictEqual(code, 1);
  assert.deepStrictEqual(error.lines, ['wrpc types: Cannot write x: disk on fire']);
});
