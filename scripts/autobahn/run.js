'use strict';

/**
 * Autobahn Testsuite orchestrator.
 *
 * Run locally: `node scripts/autobahn/run.js` (requires docker; the full
 * suite of 500+ cases takes several minutes). It starts echo-server.js as a
 * child, waits for it to listen, runs the crossbario/autobahn-testsuite
 * fuzzing client against it in a container, then grades reports/index.json:
 * 'OK', 'NON-STRICT', 'INFORMATIONAL', and 'UNIMPLEMENTED' behaviors pass;
 * 'FAILED' and 'WRONG CODE' fail the run (exit code 1). The 6.4.* fail-fast
 * timing cases are deliberately not excluded — non-strict results there are
 * acceptable.
 *
 * Path scheme (one consistent scheme, container-side paths in the config):
 *   scripts/autobahn         -> /config   (base fuzzingclient.json, for reference/manual runs)
 *   scripts/autobahn/reports -> /reports  (outdir AND the generated effective config)
 * The checked-in fuzzingclient.json is the base; every run generates
 * reports/fuzzingclient.generated.json from it so the servers url can be
 * adjusted per platform, and wstest is pointed at the generated file.
 *
 * macOS: docker cannot use --network host (the flag targets the Linux VM,
 * not the Mac), so on process.platform === 'darwin' the generated config
 * rewrites the servers url to ws://host.docker.internal:<port> and the
 * --network host flag is dropped; on Linux it keeps ws://127.0.0.1:<port>
 * with --network host. No manual edit of fuzzingclient.json is needed.
 *
 * Note: reports/ is generated output — do not commit it.
 */

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const AUTOBAHN_DIR = __dirname;
const REPORTS_DIR = path.join(AUTOBAHN_DIR, 'reports');
const BASE_CONFIG = path.join(AUTOBAHN_DIR, 'fuzzingclient.json');
const GENERATED_CONFIG = 'fuzzingclient.generated.json';
const IMAGE = 'crossbario/autobahn-testsuite';
const PORT = Number(process.env.PORT ?? 9001);
const FAIL_BEHAVIORS = new Set(['FAILED', 'WRONG CODE']);

const isDarwin = process.platform === 'darwin';

const writeEffectiveConfig = () => {
  const base = JSON.parse(fs.readFileSync(BASE_CONFIG, 'utf8'));
  const host = isDarwin ? 'host.docker.internal' : '127.0.0.1';
  const config = {
    ...base,
    servers: base.servers.map((server) => ({ ...server, url: `ws://${host}:${PORT}` })),
  };
  const generatedPath = path.join(REPORTS_DIR, GENERATED_CONFIG);
  fs.writeFileSync(generatedPath, `${JSON.stringify(config, null, 2)}\n`);
};

const startEchoServer = () =>
  new Promise((resolve, reject) => {
    const script = path.join(AUTOBAHN_DIR, 'echo-server.js');
    const child = spawn(process.execPath, [script], {
      env: { ...process.env, PORT: String(PORT) },
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    let ready = false;
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => {
      process.stdout.write(chunk);
      if (!ready && chunk.includes('listening')) {
        ready = true;
        resolve(child);
      }
    });
    child.once('error', (error) => {
      if (!ready) reject(error);
    });
    child.once('exit', (code) => {
      if (!ready) reject(new Error(`echo-server exited before listening (code ${code})`));
    });
  });

const runDocker = () =>
  new Promise((resolve, reject) => {
    const args = ['run', '--rm'];
    if (!isDarwin) args.push('--network', 'host');
    args.push('-v', `${AUTOBAHN_DIR}:/config`);
    args.push('-v', `${REPORTS_DIR}:/reports`);
    args.push(IMAGE, 'wstest', '-m', 'fuzzingclient', '-s', `/reports/${GENERATED_CONFIG}`);
    console.log(`> docker ${args.join(' ')}`);
    const child = spawn('docker', args, { stdio: 'inherit' });
    child.once('error', (error) => {
      reject(new Error(`failed to start docker (is it installed and running?): ${error.message}`));
    });
    child.once('exit', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`docker exited with code ${code}`));
    });
  });

const collectFailures = () => {
  const indexPath = path.join(REPORTS_DIR, 'index.json');
  if (!fs.existsSync(indexPath)) {
    throw new Error(`missing ${indexPath} — wstest produced no report`);
  }
  const index = JSON.parse(fs.readFileSync(indexPath, 'utf8'));
  const failures = [];
  for (const agent of Object.values(index)) {
    for (const [caseId, result] of Object.entries(agent)) {
      if (FAIL_BEHAVIORS.has(result.behavior) || FAIL_BEHAVIORS.has(result.behaviorClose)) {
        failures.push(`${caseId} (behavior: ${result.behavior}, close: ${result.behaviorClose})`);
      }
    }
  }
  return failures;
};

const main = async () => {
  fs.rmSync(REPORTS_DIR, { recursive: true, force: true });
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  writeEffectiveConfig();
  const echo = await startEchoServer();
  try {
    await runDocker();
    const failures = collectFailures();
    if (failures.length > 0) {
      console.error(`Autobahn: ${failures.length} failing case(s):`);
      for (const failure of failures) console.error(`  ${failure}`);
      process.exitCode = 1;
    } else {
      console.log(`Autobahn: all cases passed (report: ${path.join(REPORTS_DIR, 'index.html')})`);
    }
  } finally {
    echo.kill('SIGTERM');
  }
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
