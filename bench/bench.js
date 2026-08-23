/**
 * Zero-dependency ops/sec benchmark harness for wrpc hot paths.
 *
 * Run: pnpm bench (or: node bench/bench.js)
 *
 * Spins up a real Server and connects real WrpcClient instances (WS + HTTP)
 * against it, then measures round-trip call/event throughput end to end —
 * the same path exercised by tests/integration.test.js.
 */
const { WrpcClient } = require('../src/client.js');
const { bench } = require('./support/harness.js');
const { createWrpcServer } = require('./support/wrpc-echo.js');

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
          context.client.sendEvent('bench/ping', { ping: true });
          return { ok: true };
        },
      },
    },
  };

  const { server, port } = await createWrpcServer(api);

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
