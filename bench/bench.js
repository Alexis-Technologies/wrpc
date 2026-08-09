/**
 * Zero-dependency ops/sec benchmark harness for wrpc hot paths.
 *
 * Run: pnpm bench (or: node bench/bench.js)
 *
 * Spins up a real Server and connects real WrpcClient instances (WS + HTTP)
 * against it, then measures round-trip call/event throughput end to end —
 * the same path exercised by tests/integration.test.js.
 */
const { performance } = require('node:perf_hooks');
const { randomUUID } = require('node:crypto');

const { Server } = require('../src/server.js');
const { WrpcClient } = require('../src/client.js');

const WARMUP_ITERATIONS = 200;
const MEASURE_MS = 1_000;

async function bench(name, fn) {
  for (let i = 0; i < WARMUP_ITERATIONS; i++) await fn();

  let iterations = 0;
  const start = performance.now();
  while (performance.now() - start < MEASURE_MS) {
    await fn();
    iterations += 1;
  }
  const elapsed = performance.now() - start;
  const opsPerSec = Math.round((iterations / elapsed) * 1000);
  console.log(`${name.padEnd(52)} ${opsPerSec.toLocaleString('en-US').padStart(12)} ops/sec`);
  return { name, opsPerSec };
}

const noop = () => {};

class ProcedureMock {
  constructor({ access = 'public', handler }) {
    this.access = access;
    this.handler = handler;
  }

  // eslint-disable-next-line class-methods-use-this
  async enter() {}
  // eslint-disable-next-line class-methods-use-this
  leave() {}
  invoke(context, args) {
    return this.handler(args, context);
  }
}

const createApplication = (api) => {
  const introspect = (units = []) => {
    const result = {};
    for (const unit of units) {
      if (!api[unit]) continue;
      const methods = {};
      for (const name of Object.keys(api[unit])) methods[name] = {};
      result[unit] = methods;
    }
    return result;
  };

  return {
    console: { log: noop, info: noop, warn: noop, error: noop, debug: noop },
    static: { constructor: { name: 'Static' } },
    auth: { saveSession: async () => {} },
    getMethod: (unit, _ver, method) => {
      if (unit === 'system' && method === 'introspect') {
        return new ProcedureMock({ handler: async (units) => introspect(units) });
      }
      const def = api[unit]?.[method];
      if (!def) return null;
      return new ProcedureMock(def);
    },
  };
};

const createServer = async (api) => {
  const options = {
    host: '127.0.0.1',
    port: 0,
    protocol: 'http',
    timeouts: { bind: 100 },
    queue: { concurrency: 100, size: 100, timeout: 5_000 },
    generateId: randomUUID,
  };
  const server = new Server(createApplication(api), options);
  await server.listen();
  const { port } = server.httpServer.address();
  return { server, port };
};

const smallPayload = { name: 'Ada' };
const largePayload = { text: 'x'.repeat(10_000) };

async function main() {
  console.log(`Node ${process.version} | ${new Date().toISOString()}\n`);
  const results = [];

  const api = {
    bench: {
      echo: { handler: async (args) => args },
      notify: {
        handler: async (_args, context) => {
          context.client.emit('bench/ping', { ping: true });
          return { ok: true };
        },
      },
    },
  };

  const { server, port } = await createServer(api);

  const wsClient = await WrpcClient.connect(`ws://127.0.0.1:${port}/`);
  await wsClient.load('bench');

  const httpClient = await WrpcClient.connect(`http://127.0.0.1:${port}/api`);
  await httpClient.load('bench');

  results.push(await bench('WS call round-trip — small payload', () => wsClient.api.bench.echo(smallPayload)));

  results.push(await bench('WS call round-trip — 10KB payload', () => wsClient.api.bench.echo(largePayload)));

  results.push(await bench('HTTP call round-trip — small payload', () => httpClient.api.bench.echo(smallPayload)));

  results.push(
    await bench('WS call + server event round-trip', async () => {
      const ping = new Promise((resolve) => wsClient.api.bench.once('ping', resolve));
      await wsClient.api.bench.notify();
      await ping;
    }),
  );

  wsClient.close();
  httpClient.close();
  await server.close();

  return results;
}

main();
