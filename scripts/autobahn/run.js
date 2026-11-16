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
 * A run that STOPPED EARLY fails too. The fuzzing client gives up on the
 * whole suite when one connect to the echo server times out (an emulated
 * amd64 image on a busy Docker VM does that), and it still writes a report
 * — of the cases it got to. Grading that report alone called 262 of 517
 * cases "all passed". So the client's output is read as it goes: a lost
 * connection, or a report holding fewer cases than were started, is a
 * failed run, and the verdict says how many cases it stands on.
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
 * `--slices` runs the suite as its case groups, one container each
 * (`1.*` … `13.7.*`, a report directory per group under reports/), and a
 * group whose client lost the server is run again, up to SLICE_ATTEMPTS
 * times. One lost connect costs that group's minutes instead of the whole
 * run: on an emulated image the suite in one go did not finish in four
 * tries, and in slices it does. The verdict is the sum, and it fails when
 * the groups add up to fewer cases than the whole suite has.
 *
 * `AUTOBAHN_DEFLATE=takeover` (read by echo-server.js) is the second mode a
 * release runs: context takeover both ways and asynchronous deflate.
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
// The suite's case groups (there is no 8 and no 11), the big ones — limits,
// and the two compression sections — a subgroup at a time.
const SLICES = [
  '1.*',
  '2.*',
  '3.*',
  '4.*',
  '5.*',
  '6.*',
  '7.*',
  ...[1, 2, 3, 4, 5, 6, 7, 8].map((n) => `9.${n}.*`),
  '10.*',
  ...[1, 2, 3, 4, 5].map((n) => `12.${n}.*`),
  ...[1, 2, 3, 4, 5, 6, 7].map((n) => `13.${n}.*`),
];
const SLICE_ATTEMPTS = 3;
// What `cases: ['*']` runs: the floor the slices have to add up to.
const SUITE_CASES = 517;

const isDarwin = process.platform === 'darwin';

// `slice` is a case pattern with a report directory of its own, or null for
// the whole suite into reports/.
const sliceDir = (slice) => (slice === null ? '' : `slice-${slice.replace('.*', '')}`);

const writeEffectiveConfig = (slice = null) => {
  const base = JSON.parse(fs.readFileSync(BASE_CONFIG, 'utf8'));
  const host = isDarwin ? 'host.docker.internal' : '127.0.0.1';
  const config = {
    ...base,
    servers: base.servers.map((server) => ({ ...server, url: `ws://${host}:${PORT}` })),
  };
  if (slice !== null) {
    config.cases = [slice];
    config.outdir = `/reports/${sliceDir(slice)}`;
    fs.mkdirSync(path.join(REPORTS_DIR, sliceDir(slice)), { recursive: true });
  }
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
    const child = spawn('docker', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    // Passed through as it arrives, and read line by line for the two things
    // the report cannot say: how many cases were started, and whether the
    // client lost the server on the way.
    let started = 0;
    let lost = null;
    const scan = (stream, out) => {
      let rest = '';
      stream.setEncoding('utf8');
      stream.on('data', (chunk) => {
        out.write(chunk);
        const lines = (rest + chunk).split('\n');
        rest = lines.pop();
        for (const line of lines) {
          if (line.startsWith('Running test case ID')) started++;
          else if (lost === null && /^Connection to \S+ failed/.test(line)) lost = line.trim();
        }
      });
    };
    scan(child.stdout, process.stdout);
    scan(child.stderr, process.stderr);
    child.once('error', (error) => {
      reject(new Error(`failed to start docker (is it installed and running?): ${error.message}`));
    });
    child.once('exit', (code) => {
      if (code !== 0) return void reject(new Error(`docker exited with code ${code}`));
      if (lost !== null) {
        const error = new Error(
          `the fuzzing client stopped after ${started} cases — ${lost} The report is PARTIAL; run it again.`,
        );
        error.partial = true;
        return void reject(error);
      }
      resolve(started);
    });
  });

const collectFailures = (slice = null) => {
  const indexPath = path.join(REPORTS_DIR, sliceDir(slice), 'index.json');
  if (!fs.existsSync(indexPath)) {
    throw new Error(`missing ${indexPath} — wstest produced no report`);
  }
  const index = JSON.parse(fs.readFileSync(indexPath, 'utf8'));
  const failures = [];
  let graded = 0;
  for (const agent of Object.values(index)) {
    for (const [caseId, result] of Object.entries(agent)) {
      graded++;
      if (FAIL_BEHAVIORS.has(result.behavior) || FAIL_BEHAVIORS.has(result.behaviorClose)) {
        failures.push(`${caseId} (behavior: ${result.behavior}, close: ${result.behaviorClose})`);
      }
    }
  }
  return { failures, graded };
};

// One run of the client — the whole suite, or one slice — graded.
const runOnce = async (slice = null) => {
  writeEffectiveConfig(slice);
  const started = await runDocker();
  const { failures, graded } = collectFailures(slice);
  if (graded === 0 || graded < started) {
    throw new Error(`the report holds ${graded} cases of the ${started} that were started`);
  }
  return { failures, graded };
};

// A slice, again when the client lost the server on the way: a partial
// report is thrown away with its directory, never added up.
const runSlice = async (slice) => {
  for (let attempt = 1; ; attempt++) {
    console.log(`\n=== cases ${slice} (attempt ${attempt} of ${SLICE_ATTEMPTS}) ===`);
    try {
      return await runOnce(slice);
    } catch (error) {
      if (error.partial !== true || attempt === SLICE_ATTEMPTS) throw error;
      console.error(`${error.message}\nRunning ${slice} again.`);
      fs.rmSync(path.join(REPORTS_DIR, sliceDir(slice)), { recursive: true, force: true });
    }
  }
};

const runSlices = async () => {
  const failures = [];
  let graded = 0;
  for (const slice of SLICES) {
    const result = await runSlice(slice);
    graded += result.graded;
    for (const failure of result.failures) failures.push(failure);
    console.log(`=== ${slice}: ${result.graded} cases, ${result.failures.length} failing (${graded} so far) ===`);
  }
  if (graded < SUITE_CASES) {
    throw new Error(
      `the slices add up to ${graded} cases; the suite has ${SUITE_CASES} — a group is missing from SLICES`,
    );
  }
  return { failures, graded };
};

const main = async () => {
  const slices = process.argv.includes('--slices');
  fs.rmSync(REPORTS_DIR, { recursive: true, force: true });
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const echo = await startEchoServer();
  try {
    const { failures, graded } = slices ? await runSlices() : await runOnce();
    const where = slices ? `${REPORTS_DIR}/slice-*/index.html` : path.join(REPORTS_DIR, 'index.html');
    if (failures.length > 0) {
      console.error(`Autobahn: ${failures.length} of ${graded} cases failing:`);
      for (const failure of failures) console.error(`  ${failure}`);
      process.exitCode = 1;
    } else {
      console.log(`Autobahn: all ${graded} cases passed (report: ${where})`);
    }
  } finally {
    echo.kill('SIGTERM');
  }
};

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
