/**
 * Runs every benchmark script in this directory, one child process at a
 * time, so `pnpm bench` always covers the full `bench/` folder instead of
 * a single hardcoded entry point.
 *
 * Run with `pnpm bench`.
 */

const { readdirSync } = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const SELF = path.basename(__filename);

const files = readdirSync(__dirname)
  .filter((file) => file.endsWith('.js') && file !== SELF)
  .sort();

for (const file of files) {
  console.log(`\n=== ${file} ===\n`);
  const result = spawnSync(process.execPath, [path.join(__dirname, file)], { stdio: 'inherit' });
  if (result.status !== 0) process.exitCode = result.status ?? 1;
}
