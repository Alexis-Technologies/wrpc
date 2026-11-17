/**
 * Walking the keys of an object this package built: `for (const key in obj)`
 * against `for (const [key, value] of Object.entries(obj))`.
 *
 * Two places do it per message rather than per start-up, and both are
 * written with `for...in` on the strength of these rows:
 *
 *   - `eachHeader` in src/adapters/common.js — the header block of every
 *     HTTP answer a framework adapter writes (seven headers, one of them an
 *     array);
 *   - `#applySnapshot` in src/rpc/cluster.js — the rooms of a presence
 *     snapshot, once per peer per presence interval (a thousand rooms here).
 *
 * `Object.entries` builds the array and a [key, value] pair for every
 * property before the loop sees one. One run printed:
 *
 *   headers, 7 keys      for...in  7.9 M ops/s    Object.entries  5.5 M
 *   snapshot, 1,000      for...in  17,000         Object.entries  7,100
 *
 * Both objects are literals or JSON this package made — own, enumerable,
 * string keys, no prototype chain to walk — which is what makes `for...in`
 * the same walk. On an object of unknown origin it is not, and
 * `Object.entries` (or `Object.hasOwn` per key) stays the spelling.
 *
 * Run with `node bench/object-walk.js` or `pnpm bench`.
 */

'use strict';

const { bench } = require('./support/harness.js');

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

const main = async () => {
  console.log(`Object walk benchmark — Node ${process.version}\n`);
  await bench('headers (7 keys) — for...in', () => headersForIn(HEADERS));
  await bench('headers (7 keys) — Object.entries', () => headersEntries(HEADERS));
  // A snapshot arrives as JSON: parsed once here, as the cluster has it.
  const rooms = JSON.parse(JSON.stringify(ROOMS));
  await bench('presence snapshot (1,000 rooms) — for...in', () => {
    sink += snapshotForIn(rooms).size;
  });
  await bench('presence snapshot (1,000 rooms) — Object.entries', () => {
    sink += snapshotEntries(rooms).size;
  });
  if (sink < 0) console.log(sink);
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
