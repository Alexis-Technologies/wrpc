'use strict';

// `wrpc types <url> --out api.d.ts` — the codegen half of F6.
//
// A running server already describes itself: `system/introspect` answers with
// { unitKey: { method: { access, kind?, meta?, signature? } } }. This turns
// that into the same shape of interface a user would otherwise hand-write for
// `connect<Api>()`, so the two halves of the typed client meet in the middle:
// hand-write the contract when you want exact types, generate it when you do
// not want to write it at all.
//
// The generated types are only as precise as the `signature` descriptors the
// router carries. That is deliberate — the descriptor is a coarse, closed
// format (see TYPE_NAMES) rather than an open schema language, because every
// byte of it arrives over the network and ends up inside a file the user
// compiles. Nothing from the wire is ever interpolated verbatim: names are
// quoted through quoteKey() and types are matched against an allowlist.

const { writeFile } = require('node:fs/promises');

const DEFAULT_INTERFACE = 'Api';
const DEFAULT_PACKAGE = '@alexify/wrpc';
const MARKER = 'SubscriptionContract';

// A descriptor is data, not code: only these names may reach the output, and
// anything else degrades to `unknown` with a warning. JSON has no Date, so
// neither does this list.
const TYPE_NAMES = new Set(['string', 'number', 'boolean', 'unknown', 'any', 'object', 'null', 'never']);

// A hostile (or generated-from-a-cycle) descriptor must not be able to blow
// the stack or the output size.
const MAX_DEPTH = 8;

const IDENTIFIER = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
// 'string', 'number[]', 'boolean[][]' — a base name plus array suffixes.
const TYPE_REF = /^([A-Za-z]+)((?:\[\])*)$/;

const USAGE = `wrpc types <url> [options]

Generates a TypeScript contract interface from a running wrpc server, for use
with connect<Api>(url).

Arguments:
  <url>                 The server's wrpc endpoint, e.g. http://localhost:8000/api

Options:
  --out <path>          Write the declaration file here; '-' writes to stdout
  --units <a,b>         Only these units (repeatable, or comma-separated)
  --interface <name>    Name of the generated interface (default: ${DEFAULT_INTERFACE})
  --package <specifier> Where to import ${MARKER} from (default: ${DEFAULT_PACKAGE})
  -h, --help            Show this help
  -v, --version         Show the wrpc version

Examples:
  wrpc types http://localhost:8000/api --out api.d.ts
  wrpc types http://localhost:8000/api --units chat,auth.1 --out src/api.d.ts
  wrpc types http://localhost:8000/api --out -`;

class CliError extends Error {}

const fail = (message) => {
  throw new CliError(message);
};

// ---------------------------------------------------------------------------
// argv

const FLAGS_WITH_VALUE = new Set(['--out', '--units', '--interface', '--package']);

const parseArgs = (argv) => {
  const options = { units: [], interfaceName: DEFAULT_INTERFACE, packageName: DEFAULT_PACKAGE };
  const positional = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--help' || arg === '-h') return { help: true };
    if (arg === '--version' || arg === '-v') return { version: true };
    // Everything after '--' is positional, even if it looks like a flag.
    if (arg === '--') {
      positional.push(...argv.slice(i + 1));
      break;
    }
    if (FLAGS_WITH_VALUE.has(arg)) {
      const value = argv[i + 1];
      // A missing value must not silently swallow the next flag: `--out --units x`
      // would otherwise write a file called '--units'. A bare '-' is the one
      // dash-leading value that IS a value — it means stdout.
      if (value === undefined || (value.startsWith('-') && value !== '-')) fail(`${arg} requires a value`);
      i++;
      if (arg === '--out') options.out = value;
      else if (arg === '--units') options.units.push(...value.split(',').map((unit) => unit.trim()));
      else if (arg === '--interface') options.interfaceName = value;
      else options.packageName = value;
      continue;
    }
    if (arg.startsWith('-') && arg !== '-') fail(`Unknown option ${arg}`);
    positional.push(arg);
  }
  const [command, url, ...extra] = positional;
  if (!command) fail('Missing command. Try `wrpc types <url> --out api.d.ts`.');
  if (command !== 'types') fail(`Unknown command '${command}'. The only command is 'types'.`);
  if (!url) fail('Missing <url>. Try `wrpc types http://localhost:8000/api --out api.d.ts`.');
  if (extra.length > 0) fail(`Unexpected argument '${extra[0]}'`);
  if (!options.out) fail("Missing --out <path>. Use '--out -' to write to stdout.");
  // An empty --units would filter every unit away and generate an empty
  // interface, which looks like a working run and is not one.
  if (options.units.some((unit) => unit === '')) fail('--units must not contain an empty unit name');
  if (!IDENTIFIER.test(options.interfaceName)) {
    fail(`--interface must be a valid TypeScript identifier, got '${options.interfaceName}'`);
  }
  return { ...options, url };
};

// ---------------------------------------------------------------------------
// rendering

/**
 * Bare when it is an identifier, a quoted string literal otherwise. The
 * escaping is JSON.stringify's — that is what makes a name arriving from the
 * wire safe to put in a file — and the quotes are then downgraded to single
 * ones to match the rest of the codebase whenever nothing needed escaping.
 * (Nothing needed escaping exactly when the body holds no backslash: a `"`
 * would have become `\"`, a newline `\n`.)
 */
const quoteKey = (name) => {
  if (IDENTIFIER.test(name)) return name;
  const quoted = JSON.stringify(name);
  const body = quoted.slice(1, -1);
  return body.includes('\\') || body.includes("'") ? quoted : `'${body}'`;
};

// A comment is the one place free-form text (meta.description, the source URL)
// reaches the output, so it must not be able to close the comment or start a
// new line of code.
const commentText = (text) =>
  String(text)
    .replace(/\*\//g, '* /')
    // Matching control characters is the entire point: a newline (or a line
    // separator, which JSON does not escape) would end the comment and turn
    // whatever follows into code.
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u001f\u007f\u2028\u2029]+/g, ' ')
    .trim();

/** The endpoint, with any credentials and query string left out of the file. */
const sourceLabel = (url) => {
  try {
    const parsed = new URL(url);
    return commentText(`${parsed.origin}${parsed.pathname}`);
  } catch {
    return commentText(url);
  }
};

const renderTypeName = (text, warn) => {
  const parts = text.split('|').map((part) => part.trim());
  const rendered = [];
  for (const part of parts) {
    const match = TYPE_REF.exec(part);
    if (!match || !TYPE_NAMES.has(match[1])) {
      warn(`unknown type '${text}' in a signature; using 'unknown'`);
      return 'unknown';
    }
    rendered.push(match[1] + match[2]);
  }
  return rendered.join(' | ');
};

const isFieldMap = (value) => typeof value === 'object' && value !== null && !Array.isArray(value);

/**
 * A descriptor shape is a type name ('string', 'number[]', 'string|number'), a
 * one-element array (an array of that shape), or a field map whose keys may end
 * in '?' to mark the field optional.
 */
const renderType = (shape, warn, depth = 0) => {
  if (depth > MAX_DEPTH) {
    warn(`signature nested deeper than ${MAX_DEPTH}; using 'unknown'`);
    return 'unknown';
  }
  if (typeof shape === 'string') return renderTypeName(shape, warn);
  if (Array.isArray(shape)) {
    if (shape.length !== 1) {
      warn("an array in a signature describes an array OF one shape, so it holds exactly one entry; using 'unknown'");
      return 'unknown';
    }
    return `Array<${renderType(shape[0], warn, depth + 1)}>`;
  }
  if (isFieldMap(shape)) {
    const fields = [];
    // Object.entries, never a lookup by name: a descriptor arriving as JSON
    // can carry a '__proto__' key, and it must be read as data.
    for (const [rawKey, value] of Object.entries(shape)) {
      const optional = rawKey.endsWith('?');
      const name = optional ? rawKey.slice(0, -1) : rawKey;
      if (name === '') {
        warn('a signature field with an empty name was skipped');
        continue;
      }
      fields.push(`${quoteKey(name)}${optional ? '?' : ''}: ${renderType(value, warn, depth + 1)}`);
    }
    // No fields declared means exactly that. Write 'object' for "an object,
    // contents unspecified" — `{}` in TypeScript means "anything but null".
    if (fields.length === 0) return 'Record<string, never>';
    return `{ ${fields.join('; ')} }`;
  }
  warn(`a signature shape must be a type name, an array, or an object, got ${typeof shape}; using 'unknown'`);
  return 'unknown';
};

const signatureOf = (info, target, warn) => {
  const { signature } = info;
  if (signature === undefined || signature === null) return null;
  if (!isFieldMap(signature)) {
    warn(`${target}: signature must be an object; ignoring it`);
    return null;
  }
  return signature;
};

const renderMethod = (unitKey, method, info, warn) => {
  const target = `${unitKey}/${method}`;
  const signature = signatureOf(info, target, warn);
  const lines = [];
  const description = info.meta?.description;
  if (typeof description === 'string' && description.trim() !== '') {
    lines.push(`  /** ${commentText(description)} */`);
  }
  if (info.kind === 'subscription') {
    if (signature?.returns !== undefined) {
      warn(`${target}: 'returns' means nothing for a subscription (it yields values); use 'data'`);
    }
    // `any` rather than `unknown` for undeclared args: it is what lets both
    // `subscribe()` and `subscribe(args)` compile, which is the honest surface
    // for a subscription whose arguments nobody described.
    const args = signature?.args === undefined ? 'any' : renderType(signature.args, warn);
    const data = signature?.data === undefined ? 'unknown' : renderType(signature.data, warn);
    lines.push(`  ${quoteKey(method)}: ${MARKER}<${args}, ${data}>;`);
    return lines;
  }
  if (signature?.data !== undefined) {
    warn(`${target}: 'data' means nothing for a call (it answers once); use 'returns'`);
  }
  const args = signature?.args === undefined ? 'args?: unknown' : `args: ${renderType(signature.args, warn)}`;
  const returns = signature?.returns === undefined ? 'unknown' : renderType(signature.returns, warn);
  lines.push(`  ${quoteKey(method)}(${args}): Promise<${returns}>;`);
  return lines;
};

/**
 * Introspection -> the text of a .d.ts. Pure: everything it needs is an
 * argument, and every diagnostic goes to `warn`, so it is straightforward to
 * test and impossible for it to write a file by accident.
 */
const renderTypes = (introspection, options = {}) => {
  const { url = '', interfaceName = DEFAULT_INTERFACE, packageName = DEFAULT_PACKAGE, warn = () => {} } = options;
  if (!isFieldMap(introspection)) fail('The server did not answer introspection with an object');
  const units = [];
  let subscriptions = 0;
  // Sorted, so re-running the command produces a byte-identical file and a
  // diff means the server changed.
  for (const unitKey of Object.keys(introspection).sort()) {
    const methods = introspection[unitKey];
    if (!isFieldMap(methods)) {
      warn(`unit '${unitKey}' is not an object of methods; skipped`);
      continue;
    }
    const body = [];
    for (const method of Object.keys(methods).sort()) {
      const info = methods[method];
      if (!isFieldMap(info)) {
        warn(`${unitKey}/${method} is not a method descriptor; skipped`);
        continue;
      }
      if (info.kind === 'subscription') subscriptions++;
      body.push(...renderMethod(unitKey, method, info, warn));
    }
    // A unit CAN have no callable methods — one that only declares inbound
    // event handlers (`on: {...}`) introspects as an empty method map. It is
    // still worth emitting: the unit object is an Emitter, which is where its
    // server -> client events arrive.
    if (body.length === 0) {
      units.push(`  ${quoteKey(unitKey)}: Record<string, never>;`);
      continue;
    }
    units.push(`  ${quoteKey(unitKey)}: {\n${body.map((line) => `  ${line}`).join('\n')}\n  };`);
  }
  const lines = [
    '// Generated by `wrpc types`. Do not edit — re-run the command instead.',
    `// Source: ${sourceLabel(url)}`,
    '',
  ];
  // Only imported when something needs it: an unused import is noise in a
  // file the user is told not to edit.
  if (subscriptions > 0) lines.push(`import type { ${MARKER} } from '${packageName}';`, '');
  lines.push(`export interface ${interfaceName} {`, ...units, '}', '');
  return lines.join('\n');
};

// ---------------------------------------------------------------------------
// the server

const INTROSPECT = 'system/introspect';

const fetchIntrospection = async (url, units, doFetch) => {
  // `introspect` treats anything but an array as "no filter", which is what an
  // empty object says here — an empty ARRAY would filter every unit away.
  const args = units.length > 0 ? units : {};
  const body = JSON.stringify({ type: 'call', id: '1', method: INTROSPECT, args });
  let response = null;
  try {
    response = await doFetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
  } catch (error) {
    fail(`Cannot reach ${url}: ${error.message}`);
  }
  if (!response.ok) fail(`${url} answered ${response.status}${response.statusText ? ` ${response.statusText}` : ''}`);
  const text = await response.text();
  let packet = null;
  try {
    packet = JSON.parse(text);
  } catch {
    fail(`${url} answered something that is not JSON`);
  }
  if (packet?.error) {
    const { message = 'unknown error', code = 500 } = packet.error;
    fail(`${INTROSPECT} was refused with ${code}: ${message}`);
  }
  if (packet?.type !== 'callback') fail(`${url} answered '${packet?.type}' instead of a callback`);
  return packet.result;
};

// ---------------------------------------------------------------------------
// entry point

/**
 * Everything the CLI touches beyond argv is injected, so the whole command is
 * testable in-process: no stdout, no network, no filesystem.
 */
const main = async (argv, io = {}) => {
  const {
    log = (text) => void console.log(text),
    error = (text) => void console.error(text),
    fetch: doFetch = globalThis.fetch,
    write = writeFile,
    version = () => require('../../package.json').version,
  } = io;
  try {
    const options = parseArgs(argv);
    if (options.help) {
      log(USAGE);
      return 0;
    }
    if (options.version) {
      log(version());
      return 0;
    }
    const introspection = await fetchIntrospection(options.url, options.units, doFetch);
    const warn = (message) => void error(`wrpc types: ${message}`);
    const text = renderTypes(introspection, { ...options, warn });
    if (options.out === '-') {
      log(text.replace(/\n$/, ''));
      return 0;
    }
    try {
      await write(options.out, text);
    } catch (cause) {
      // A mistyped --out is the commonest way this fails; ENOENT with a stack
      // is not the answer to "that directory does not exist".
      fail(`Cannot write ${options.out}: ${cause?.message ?? cause}`);
    }
    return 0;
  } catch (cause) {
    if (cause instanceof CliError) {
      error(`wrpc types: ${cause.message}`);
      return 1;
    }
    error(`wrpc types: ${cause?.stack ?? cause}`);
    return 1;
  }
};

module.exports = {
  CliError,
  MAX_DEPTH,
  TYPE_NAMES,
  USAGE,
  fetchIntrospection,
  main,
  parseArgs,
  quoteKey,
  renderType,
  renderTypes,
};
