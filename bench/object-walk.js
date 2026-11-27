/**
 * Walking the keys of an object this package built: `for (const key in obj)`
 * against `for (const [key, value] of Object.entries(obj))` and against
 * `Object.keys` with an index loop — the spelling alexiskills 1.1 asks of a
 * library's `src/`.
 *
 * Two places do it per message rather than per start-up:
 *
 *   - `eachHeader` in src/adapters/common.js — the header block of every
 *     HTTP answer a framework adapter writes (seven headers, one of them an
 *     array). It stays `for...in`, on the strength of the first rows: on a
 *     small object V8 walks it from the enum cache, and neither other
 *     spelling comes close;
 *   - `#applySnapshot` in src/rpc/cluster.js — the rooms of a presence
 *     snapshot, once per peer per presence interval (a thousand rooms here).
 *     It is `Object.keys` with an index loop since 2.3: JSON that large is a
 *     dictionary-mode object, which `for...in` walks slower than the array of
 *     its keys.
 *
 * `Object.entries` builds the array and a [key, value] pair for every
 * property before the loop sees one. One run (Node 24.14) printed:
 *
 *   headers, 7 keys      for...in  9.9 M ops/s   Object.entries  6.8 M   Object.keys  6.8 M
 *   snapshot, 1,000      for...in  22,000        Object.entries  9,400   Object.keys  29,000
 *
 * Both objects are literals or JSON this package made — own, enumerable,
 * string keys, no prototype chain to walk — which is what makes `for...in`
 * the same walk. On an object of unknown origin it is not, and `Object.keys`
 * (or `Object.hasOwn` per key) stays the spelling.
 *
 * Run with `node bench/object-walk.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');
const { isolate, isLeader } = require('./support/isolate.js');

const HEADERS = {
  'Content-Type': 'application/json',
  'Content-Length': 128,
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Credentials': 'true',
  'X-Content-Type-Options': 'nosniff',
  'wrpc-version': '2',
  'Set-Cookie': ['token=abc; HttpOnly', 'theme=dark'],
};

const ROOMS = {};
for (let i = 0; i < 1000; i++) ROOMS[`room-${i}`] = (i % 7) + 1;

let sink = 0;
const visit = (name, value) => {
  sink += name.length + value.length;
};

const headersForIn = (headers) => {
  for (const name in headers) {
    const value = headers[name];
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) visit(name, String(value[i]));
      continue;
    }
    visit(name, String(value));
  }
};

const headersEntries = (headers) => {
  for (const [name, value] of Object.entries(headers)) {
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) visit(name, String(value[i]));
      continue;
    }
    visit(name, String(value));
  }
};

const headersKeys = (headers) => {
  const names = Object.keys(headers);
  for (let k = 0; k < names.length; k++) {
    const name = names[k];
    const value = headers[name];
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) visit(name, String(value[i]));
      continue;
    }
    visit(name, String(value));
  }
};

const snapshotForIn = (rooms) => {
  const map = new Map();
  for (const room in rooms) {
    const count = rooms[room];
    if (typeof count === 'number' && count > 0) map.set(room, count);
  }
  return map;
};

const snapshotEntries = (rooms) => {
  const map = new Map();
  for (const [room, count] of Object.entries(rooms)) {
    if (typeof count === 'number' && count > 0) map.set(room, count);
  }
  return map;
};

const snapshotKeys = (rooms) => {
  const map = new Map();
  const names = Object.keys(rooms);
  for (let i = 0; i < names.length; i++) {
    const count = rooms[names[i]];
    if (typeof count === 'number' && count > 0) map.set(names[i], count);
  }
  return map;
};

const main = async () => {
  if (isLeader) console.log(`Object walk benchmark — Node ${process.version}\n`);
  await bench('headers (7 keys) — for...in', () => headersForIn(HEADERS));
  await bench('headers (7 keys) — Object.entries', () => headersEntries(HEADERS));
  await bench('headers (7 keys) — Object.keys + index', () => headersKeys(HEADERS));
  // A snapshot arrives as JSON: parsed once here, as the cluster has it.
  const rooms = JSON.parse(JSON.stringify(ROOMS));
  await bench('presence snapshot (1,000 rooms) — for...in', () => {
    sink += snapshotForIn(rooms).size;
  });
  await bench('presence snapshot (1,000 rooms) — Object.entries', () => {
    sink += snapshotEntries(rooms).size;
  });
  await bench('presence snapshot (1,000 rooms) — Object.keys + index', () => {
    sink += snapshotKeys(rooms).size;
  });
  if (sink < 0) console.log(sink);
};

// One case per process (bench/support/isolate.js).
if (!isolate(__filename)) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
