'use strict';

// What wrpc itself spends on one HTTP request, the network left out:
// RpcServer.handleHttpCall driven with the abstract call a host hands it
// (src/server.js, the adapters), answered into a stub `respond`. The same
// two paths bench/http-comparison.js loads over a socket — a declared REST
// route and packet mode — so a change that moves these numbers is a change
// to wrpc's share of that table, and one that does not is noise there.
//
// The body half is measured apart: receiveBody (src/adapters/common.js) is
// the shell's, not the core's, and runs before handleHttpCall. Then the
// per-request shapes the request path was rewritten around, each next to
// the one it replaced (reproduced inline, as bench/cors-headers.js does):
// the numbers the comments in src/ cite.
//
//   node bench/http-call.js

const { Readable } = require('node:stream');

const { bench } = require('./support/harness.js');
const { RpcServer } = require('../src/rpc/core.js');
const { defineRouter, procedure } = require('../src/rpc/router.js');
const { parseParams } = require('../src/rpc/dispatcher.js');
const { receiveBody } = require('../src/adapters/common.js');
const { Emitter, jsonParse } = require('../src/utils.js');

const router = defineRouter({
  bench: {
    echo: procedure({
      access: 'public',
      http: { method: 'POST', path: '/echo' },
      handler: async (_context, { body }) => body,
    }),
    args: procedure({ access: 'public', handler: async (_context, args) => args }),
  },
});

const rpc = new RpcServer({ router, logger: false });

// What autocannon sends in bench/http-comparison.js, as node:http hands it
// over: lower-cased names, string values.
const HEADERS = { host: '127.0.0.1:3000', 'content-type': 'application/json', 'content-length': '14' };
// …and what a browser's fetch sends, for the per-header costs below.
const BROWSER_HEADERS = {
  host: 'api.example',
  connection: 'keep-alive',
  'content-type': 'application/json',
  'content-length': '14',
  accept: '*/*',
  'accept-language': 'en-US,en;q=0.9',
  'accept-encoding': 'gzip, deflate, br, zstd',
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
  origin: 'https://app.example',
  referer: 'https://app.example/',
  'sec-fetch-site': 'same-site',
  'sec-fetch-mode': 'cors',
};
const REST_BODY = Buffer.from(JSON.stringify({ name: 'Ada' }));
const PACKET_BODY = Buffer.from(JSON.stringify({ type: 'call', id: '1', method: 'bench/args', args: { name: 'Ada' } }));

// One request: resolves with the answer's status once wrpc has written it.
const request = (url, body) =>
  new Promise((resolve) => {
    void rpc.handleHttpCall({
      method: 'POST',
      url,
      headers: HEADERS,
      body,
      remoteAddress: '127.0.0.1',
      respond: ({ status }) => resolve(status),
      onAbort: () => {},
    });
  });

// In flight together, as autocannon's pipelined connections keep them: the
// harness awaits per iteration, which would otherwise be most of what is
// measured.
const CONCURRENT = 64;

const burst = async (url, body) => {
  const pending = new Array(CONCURRENT);
  for (let i = 0; i < CONCURRENT; i++) pending[i] = request(url, body);
  const statuses = await Promise.all(pending);
  if (statuses[0] !== 200) throw new Error(`${url} answered ${statuses[0]}`);
};

// A request body as node:http delivers a small one: one chunk, then the end.
const bodyStream = () => {
  const stream = new Readable({ read() {} });
  stream.push(REST_BODY);
  stream.push(null);
  return stream;
};

// The loop receiveBody was before it listened on the stream.
const receiveByIterator = async (stream) => {
  const chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  return chunks.length === 0 ? null : chunks.length === 1 ? chunks[0] : Buffer.concat(chunks);
};

const BODIES = 256;

const bodies = (receive) => async () => {
  const pending = new Array(BODIES);
  for (let i = 0; i < BODIES; i++) pending[i] = receive(bodyStream());
  await Promise.all(pending);
};

// A declared route's path, split and decoded segment by segment — what
// #matchDeclaredRoute in src/rpc/core.js did for every path.
const decodeEach = (rest) => {
  const raw = rest.split('/');
  const segments = new Array(raw.length);
  for (let i = 0; i < raw.length; i++) {
    try {
      segments[i] = decodeURIComponent(raw[i]);
    } catch {
      return null;
    }
  }
  return segments;
};

// …and what it does now when nothing in the path is escaped.
const decodeEscaped = (rest) => (rest.includes('%') ? decodeEach(rest) : rest.split('/'));

let closes = 0;
const noteClose = () => {
  closes++;
};

// The harness awaits per call, which would drown a nanosecond op in promise
// overhead — each iteration below runs a synchronous batch.
const BATCH = 1000;

const sync = (name, fn) =>
  bench(
    name,
    async () => {
      let out;
      for (let i = 0; i < BATCH; i++) out = fn();
      return out;
    },
    { opsPerIteration: BATCH },
  );

const run = async () => {
  await bench('handleHttpCall, declared REST route', () => burst('/api/echo', REST_BODY), {
    opsPerIteration: CONCURRENT,
  });
  await bench('handleHttpCall, packet mode', () => burst('/api', PACKET_BODY), { opsPerIteration: CONCURRENT });

  await bench('receiveBody, one-chunk body (for await, before)', bodies(receiveByIterator), {
    opsPerIteration: BODIES,
  });
  await bench('receiveBody, one-chunk body (listeners)', bodies(receiveBody), { opsPerIteration: BODIES });

  await sync('JSON body, JSON.parse(buffer) (before)', () => jsonParse(PACKET_BODY));
  await sync('JSON body, JSON.parse(buffer.toString())', () => jsonParse(PACKET_BODY.toString()));

  await sync('no query string, URLSearchParams (before)', () => Object.fromEntries(new URLSearchParams('')));
  await sync('no query string, parseParams', () => parseParams(''));

  await sync('route /users/42/posts, decode each segment (before)', () => decodeEach('users/42/posts'));
  await sync('route /users/42/posts, nothing escaped', () => decodeEscaped('users/42/posts'));

  await sync('transport close, once + two emits (before)', () => {
    const transport = new Emitter();
    transport.once('close', noteClose);
    transport.emit('close');
    transport.emit('close');
  });
  await sync('transport close, on + flag + two emits', () => {
    const transport = new Emitter();
    let closed = false;
    transport.on('close', () => {
      if (closed) return;
      closed = true;
      noteClose();
    });
    transport.emit('close');
    transport.emit('close');
  });

  // Measured alone, the two copies are close; it is the dictionary-mode
  // object living through a whole request that the handleHttpCall rows
  // above feel (bench/http-call.js before/after, cited in rpc/client.js).
  await sync('meta.headers, 12 headers, __proto__ literal (before)', () =>
    Object.freeze({ __proto__: null, ...BROWSER_HEADERS }),
  );
  await sync('meta.headers, 12 headers, spread + setPrototypeOf', () =>
    Object.freeze(Object.setPrototypeOf({ ...BROWSER_HEADERS }, null)),
  );
  if (closes === 0) throw new Error('the close listeners never ran');
};

run();
