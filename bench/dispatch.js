/**
 * Branch dispatch: `switch` vs `if`/`else if` vs a table of handler functions.
 *
 * This bench exists to be cited, not to be won. The intuition that a lookup
 * table is the O(1) answer to a long `if`/`else if` chain is half right: the
 * chain really is linear in branch position, but in V8 the construct that
 * fixes it is `switch`, not a table. A `switch` over string literals compiles
 * to direct comparisons and the call target stays inlinable; a table yields a
 * function VALUE, which V8 cannot inline through, and a peer-keyed one needs a
 * null prototype on top — so it loses on both counts.
 *
 * The shapes below are the real ones: the six cluster envelope types from
 * src/rpc/cluster.js #receive, and the nine packet types from
 * src/rpc/dispatcher.js handlePacket, measured at the first branch, the last
 * branch, and a uniform mix so the linearity is visible rather than asserted.
 *
 * Run with `node bench/dispatch.js` or `pnpm bench`.
 */

'use strict';

const MEASURE_MS = 400;

let sink = 0;
const handler = (n) => (value) => {
  sink += value + n;
};

const measure = (name, fn, pick) => {
  for (let i = 0; i < 200_000; i++) fn(pick(i), i);
  let iterations = 0;
  const start = process.hrtime.bigint();
  const deadline = start + BigInt(MEASURE_MS) * 1_000_000n;
  do {
    for (let i = 0; i < 10_000; i++) fn(pick(i), i);
    iterations += 10_000;
  } while (process.hrtime.bigint() < deadline);
  const ns = Number(process.hrtime.bigint() - start);
  const perSec = (iterations / (ns / 1e9) / 1e6).toFixed(1);
  console.log(`  ${name.padEnd(44)}${perSec.padStart(8)} M ops/sec`);
};

// --- six cluster envelope types, src/rpc/cluster.js #receive ---------------

const CLUSTER = ['hello', 'state', 'delta', 'e', 'q', 'cmd'];
const C = CLUSTER.map((_, i) => handler(i));

const clusterSwitch = (t, v) => {
  switch (t) {
    case 'hello':
      return C[0](v);
    case 'state':
      return C[1](v);
    case 'delta':
      return C[2](v);
    case 'e':
      return C[3](v);
    case 'q':
      return C[4](v);
    case 'cmd':
      return C[5](v);
    default:
      return undefined;
  }
};

const CLUSTER_TABLE = { __proto__: null };
for (let i = 0; i < CLUSTER.length; i++) CLUSTER_TABLE[CLUSTER[i]] = C[i];
const clusterTable = (t, v) => {
  const fn = CLUSTER_TABLE[t];
  if (typeof fn === 'function') return fn(v);
  return undefined;
};

const CLUSTER_FROZEN = Object.freeze(Object.fromEntries(CLUSTER.map((name, i) => [name, C[i]])));
const clusterFrozen = (t, v) => {
  const fn = CLUSTER_FROZEN[t];
  if (typeof fn === 'function') return fn(v);
  return undefined;
};

const CLUSTER_MAP = new Map(CLUSTER.map((name, i) => [name, C[i]]));
const clusterMap = (t, v) => {
  const fn = CLUSTER_MAP.get(t);
  if (fn !== undefined) return fn(v);
  return undefined;
};

// --- nine packet types, src/rpc/dispatcher.js handlePacket ----------------

const PACKETS = ['call', 'subscribe', 'unsubscribe', 'cancel', 'stream', 'event', 'callback', 'ping', 'pong'];
const P = PACKETS.map((_, i) => handler(i));

const packetChain = (t, v) => {
  if (t === 'call') return P[0](v);
  else if (t === 'subscribe') return P[1](v);
  else if (t === 'unsubscribe') return P[2](v);
  else if (t === 'cancel') return P[3](v);
  else if (t === 'stream') return P[4](v);
  else if (t === 'event') return P[5](v);
  else if (t === 'callback') return P[6](v);
  else if (t === 'ping') return P[7](v);
  else if (t === 'pong') return P[8](v);
  return undefined;
};

const packetSwitch = (t, v) => {
  switch (t) {
    case 'call':
      return P[0](v);
    case 'subscribe':
      return P[1](v);
    case 'unsubscribe':
      return P[2](v);
    case 'cancel':
      return P[3](v);
    case 'stream':
      return P[4](v);
    case 'event':
      return P[5](v);
    case 'callback':
      return P[6](v);
    case 'ping':
      return P[7](v);
    case 'pong':
      return P[8](v);
    default:
      return undefined;
  }
};

const PACKET_TABLE = { __proto__: null };
for (let i = 0; i < PACKETS.length; i++) PACKET_TABLE[PACKETS[i]] = P[i];
const packetTable = (t, v) => {
  const fn = PACKET_TABLE[t];
  if (typeof fn === 'function') return fn(v);
  return undefined;
};

const first = () => PACKETS[0];
const last = () => PACKETS[PACKETS.length - 1];
const mixed = (i) => PACKETS[i % PACKETS.length];
const clusterMix = (i) => CLUSTER[i % CLUSTER.length];

console.log(`Dispatch benchmark — Node ${process.version}\n`);

console.log('6 cluster envelope types, uniform mix (src/rpc/cluster.js #receive)');
measure('switch', clusterSwitch, clusterMix);
measure('object table (__proto__: null)', clusterTable, clusterMix);
measure('frozen object table', clusterFrozen, clusterMix);
measure('Map table', clusterMap, clusterMix);

console.log('\n9 packet types, ALWAYS FIRST branch (chain best case)');
measure('if/else if chain', packetChain, first);
measure('switch', packetSwitch, first);
measure('object table', packetTable, first);

console.log('\n9 packet types, ALWAYS LAST branch (chain worst case)');
measure('if/else if chain', packetChain, last);
measure('switch', packetSwitch, last);
measure('object table', packetTable, last);

console.log('\n9 packet types, uniform mix (src/rpc/dispatcher.js handlePacket)');
measure('if/else if chain', packetChain, mixed);
measure('switch', packetSwitch, mixed);
measure('object table', packetTable, mixed);

// The rows above all pay for a real handler call, which is what production
// does — and that call is expensive enough to swamp the branch cost, which is
// why switch and the chain come out level there. Isolating the dispatch (no
// call, just a returned number) shows the effect that is otherwise hidden: the
// chain IS linear in branch position, the switch is flatter, and the table is
// flat but starts from a worse constant. Both facts matter, so both are here.

const chainOnly = (t) => {
  if (t === 'call') return 0;
  else if (t === 'subscribe') return 1;
  else if (t === 'unsubscribe') return 2;
  else if (t === 'cancel') return 3;
  else if (t === 'stream') return 4;
  else if (t === 'event') return 5;
  else if (t === 'callback') return 6;
  else if (t === 'ping') return 7;
  else if (t === 'pong') return 8;
  return -1;
};

const switchOnly = (t) => {
  switch (t) {
    case 'call':
      return 0;
    case 'subscribe':
      return 1;
    case 'unsubscribe':
      return 2;
    case 'cancel':
      return 3;
    case 'stream':
      return 4;
    case 'event':
      return 5;
    case 'callback':
      return 6;
    case 'ping':
      return 7;
    case 'pong':
      return 8;
    default:
      return -1;
  }
};

const INDEX_TABLE = { __proto__: null };
for (let i = 0; i < PACKETS.length; i++) INDEX_TABLE[PACKETS[i]] = i;
const tableOnly = (t) => {
  const value = INDEX_TABLE[t];
  return value === undefined ? -1 : value;
};

// `pick` reads through an array so the discriminant is not a foldable literal —
// with a constant, V8 elides the whole chain and reports a fantasy number.
const measureOnly = (name, fn, pick) => {
  for (let i = 0; i < 2_000_000; i++) sink += fn(pick(i));
  const iterations = 20_000_000;
  const start = process.hrtime.bigint();
  for (let i = 0; i < iterations; i++) sink += fn(pick(i));
  const ns = Number(process.hrtime.bigint() - start);
  console.log(`  ${name.padEnd(44)}${(iterations / (ns / 1e9) / 1e6).toFixed(0).padStart(8)} M ops/sec`);
};

const firstOnly = (i) => PACKETS[i & 0];
const lastOnly = (i) => PACKETS[8 - (i & 0)];

console.log('\ndispatch only, no handler call — FIRST branch');
measureOnly('if/else if chain', chainOnly, firstOnly);
measureOnly('switch', switchOnly, firstOnly);
measureOnly('object table', tableOnly, firstOnly);

console.log('\ndispatch only, no handler call — LAST branch');
measureOnly('if/else if chain', chainOnly, lastOnly);
measureOnly('switch', switchOnly, lastOnly);
measureOnly('object table', tableOnly, lastOnly);

console.log('\ndispatch only, no handler call — uniform mix');
measureOnly('if/else if chain', chainOnly, mixed);
measureOnly('switch', switchOnly, mixed);
measureOnly('object table', tableOnly, mixed);

// A table keyed on a peer-controlled string answers inherited names unless the
// prototype is severed — the reason PACKET_TABLE above is `__proto__: null`.
const NAIVE = { call: P[0] };
console.log(
  `\nnaive table: typeof TABLE['toString'] === '${typeof NAIVE.toString}' ` +
    `| null-proto table: '${typeof PACKET_TABLE.toString}'`,
);

if (sink === Number.MIN_SAFE_INTEGER) console.log('unreachable', sink);
