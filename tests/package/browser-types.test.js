'use strict';

// The browser types condition has one job: a browser TS project WITHOUT
// @types/node must compile against browser.d.ts / sse.browser.d.ts, and a
// server name imported there must be a compile error instead of an
// `undefined` at runtime. tsd cannot check the first half (its run always
// has node types available), so this test drives the repo's own tsc with
// `types: []` over a fixture that uses the browser surface — and expects
// exactly the server-name errors the fixture marks.

const path = require('node:path');
const { existsSync } = require('node:fs');
const { mkdtemp, writeFile } = require('node:fs/promises');
const { tmpdir } = require('node:os');
const { execFile } = require('node:child_process');
const { test } = require('node:test');
const assert = require('node:assert');

const ROOT = path.join(__dirname, '..', '..');
const TSC = path.join(ROOT, 'node_modules', '.bin', 'tsc');

const run = (command, args, options) =>
  new Promise((resolve) => {
    execFile(command, args, options, (error, stdout, stderr) => {
      resolve({ code: error?.code ?? 0, stdout: String(stdout), stderr: String(stderr) });
    });
  });

const FIXTURE = `
import {
  WrpcClient,
  WrpcClientProxy,
  WrpcError,
  WrpcReadable,
  WrpcWritable,
  Emitter,
  EventStream,
  connect,
  createEventStream,
  chunkEncode,
  chunkDecode,
} from '${ROOT}/browser.js';
import { ClientSseTransport, SseParser, CHANNEL_HEADER } from '${ROOT}/sse.browser.js';

interface Api {
  chat: { send: (args: { text: string }) => Promise<{ ok: boolean }> };
}

async function main() {
  const client = await connect<Api>('wss://example.test/api', {
    generateId: () => 'id-1',
    protocols: ['wrpc.v1'],
  });
  const answer = await client.api.chat.send({ text: 'hi' });
  answer.ok satisfies boolean;
  void WrpcClient;
  void WrpcClientProxy;
  void WrpcError;
  void WrpcReadable;
  void WrpcWritable;
  void Emitter;
  void EventStream;
  void createEventStream;
  void chunkEncode;
  void chunkDecode;
  void ClientSseTransport;
  void SseParser;
  void CHANNEL_HEADER;
}
void main;

// The server surface must NOT exist here: each of these is the compile
// error that replaces an \`undefined\` at runtime in a browser bundle.
// @ts-expect-error Server is not part of the browser types
import { Server } from '${ROOT}/browser.js';
// @ts-expect-error the channel registry is server machinery
import { SseChannels } from '${ROOT}/sse.browser.js';
void Server;
void SseChannels;
`;

const TSCONFIG = {
  compilerOptions: {
    strict: true,
    noEmit: true,
    target: 'es2022',
    module: 'node18',
    moduleResolution: 'node16',
    // The whole point: NO ambient node types. Only the DOM and the
    // language itself may satisfy the browser type graph.
    types: [],
    lib: ['es2022', 'dom', 'dom.iterable'],
    skipLibCheck: false,
  },
  include: ['./browser-types.fixture.ts'],
};

test('browser.d.ts compiles without @types/node and hides the server surface', async (t) => {
  if (!existsSync(TSC)) return void t.skip('typescript is not installed');
  const dir = await mkdtemp(path.join(tmpdir(), 'wrpc-browser-types-'));
  await writeFile(path.join(dir, 'browser-types.fixture.ts'), FIXTURE);
  await writeFile(path.join(dir, 'tsconfig.json'), JSON.stringify(TSCONFIG, null, 2));
  const checked = await run(TSC, ['-p', path.join(dir, 'tsconfig.json')], { cwd: dir });
  assert.strictEqual(checked.code, 0, `tsc rejected the browser type surface:\n${checked.stdout}${checked.stderr}`);
});
