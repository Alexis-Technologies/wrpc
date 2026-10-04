'use strict';
/**
 * Runs ONE server of bench/support/http-servers.js in its own process for
 * bench/http-comparison.js: prints a `READY_JSON:` line with its port, path
 * and request body, then serves until its stdin closes. A process per stack
 * keeps one framework's warm-up, GC pressure and leftover handles out of the
 * next one's numbers, and the load generator out of every server's.
 */
const servers = require('./http-servers.js');

async function main() {
  const key = process.argv[2];
  if (!Object.hasOwn(servers, key)) throw new Error(`unknown http stack ${key}`);
  const { port, path, body, stop } = await servers[key].start();
  console.log(`READY_JSON:${JSON.stringify({ port, path, body })}`);
  process.stdin.resume();
  process.stdin.on('end', async () => {
    await stop();
    process.exit(0);
  });
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
