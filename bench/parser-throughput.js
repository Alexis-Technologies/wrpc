'use strict';

/**
 * Receive-path throughput benchmark for the WebSocket engine.
 *
 * Feeds pre-built masked frames into a server-side Connection through a
 * minimal fake socket — the same entry point real TCP data takes — and
 * measures how fast the SegmentQueue + FrameParser pipeline turns wire
 * bytes into 'message' events:
 *
 *   (a) one 16 MiB masked BINARY message delivered in 16 KiB segments —
 *       exercises incremental header parsing and spanning-segment consume;
 *   (b) 50K masked 200 B TEXT frames, many frames per 64 KiB segment —
 *       exercises per-frame overhead, including UTF-8 validation.
 *
 * Received 'message' events are counted and spot-checked; any mismatch
 * fails the run (process.exitCode = 1).
 *
 * Run: node bench/parser-throughput.js (or as part of `pnpm bench`)
 */

const { EventEmitter } = require('node:events');
const { performance } = require('node:perf_hooks');
const { Connection, Frame } = require('../src/websocket/ws.js');

const KIB = 1024;
const MIB = 1024 * 1024;

let failures = 0;

const fail = (message) => {
  failures++;
  if (failures <= 10) console.error(`FAIL: ${message}`);
};

// Minimal socket stand-in: Connection only needs events plus these members.
class FakeSocket extends EventEmitter {
  destroyed = false;
  writableLength = 0;
  cork() {}
  uncork() {}
  write() {
    return true;
  }
  pause() {}
  resume() {}
  end() {}
  destroy() {
    this.destroyed = true;
  }
}

const createConnection = (onMessage) => {
  const socket = new FakeSocket();
  const connection = new Connection(socket, null);
  connection.on('message', onMessage);
  connection.on('error', (error) => fail(`connection error: ${error.message}`));
  return socket;
};

const sliceSegments = (wire, segmentSize) => {
  const segments = [];
  for (let offset = 0; offset < wire.length; offset += segmentSize) {
    segments.push(wire.subarray(offset, offset + segmentSize));
  }
  return segments;
};

const benchLargeBinary = () => {
  const SIZE = 16 * MIB;
  const SEGMENT = 16 * KIB;
  const WARMUP = 3;
  const ITERATIONS = 20;

  const payload = Buffer.alloc(SIZE, 0x5a);
  payload[0] = 0x01;
  payload[SIZE >> 1] = 0x02;
  payload[SIZE - 1] = 0x03;

  const frame = Frame.binary(Buffer.from(payload));
  frame.maskPayload();
  const wire = frame.toBuffer();
  // The 16 MiB payload always spans segments, so consume() copies before
  // the in-place unmask — the shared wire buffer stays intact across runs.
  const segments = sliceSegments(wire, SEGMENT);

  let received = 0;
  const socket = createConnection((message, isBinary) => {
    received++;
    if (!isBinary) fail('large binary: expected a binary message');
    if (message.length !== SIZE) {
      fail(`large binary: length ${message.length} !== ${SIZE}`);
    } else if (message[0] !== 0x01 || message[SIZE >> 1] !== 0x02 || message[SIZE - 1] !== 0x03) {
      fail('large binary: payload spot-check mismatch');
    }
  });

  let measured = 0;
  for (let iter = 0; iter < WARMUP + ITERATIONS; iter++) {
    const start = performance.now();
    for (const segment of segments) socket.emit('data', segment);
    const duration = performance.now() - start;
    if (iter >= WARMUP) measured += duration;
  }

  const expected = WARMUP + ITERATIONS;
  if (received !== expected) fail(`large binary: received ${received} messages, expected ${expected}`);

  const seconds = measured / 1000;
  return {
    scenario: `16 MiB binary message, ${SEGMENT / KIB} KiB segments`,
    mbPerSec: (SIZE * ITERATIONS) / MIB / seconds,
    msgsPerSec: ITERATIONS / seconds,
    messages: received,
  };
};

const benchSmallText = () => {
  const FRAME_COUNT = 50_000;
  const TEXT = 'x'.repeat(200);
  const SEGMENT = 64 * KIB;
  const WARMUP = 2;
  const ITERATIONS = 10;

  const buffers = new Array(FRAME_COUNT);
  for (let i = 0; i < FRAME_COUNT; i++) {
    const frame = Frame.text(TEXT);
    frame.maskPayload();
    buffers[i] = frame.toBuffer();
  }
  const wire = Buffer.concat(buffers);

  let received = 0;
  let firstOfRun = null;
  const socket = createConnection((message, isBinary) => {
    received++;
    if (isBinary) fail('small text: expected a text message');
    if (message.length !== TEXT.length) fail(`small text: length ${message.length} !== ${TEXT.length}`);
    if (firstOfRun === null) firstOfRun = message.toString('utf8');
  });

  let measured = 0;
  for (let iter = 0; iter < WARMUP + ITERATIONS; iter++) {
    // Small payloads are unmasked in place inside their segment, so every
    // run gets a fresh copy of the wire bytes (built outside the timing).
    const wireCopy = Buffer.from(wire);
    const segments = sliceSegments(wireCopy, SEGMENT);
    firstOfRun = null;
    const start = performance.now();
    for (const segment of segments) socket.emit('data', segment);
    const duration = performance.now() - start;
    if (iter >= WARMUP) measured += duration;
    if (firstOfRun !== TEXT) fail(`small text: run ${iter} first message corrupted`);
  }

  const expected = (WARMUP + ITERATIONS) * FRAME_COUNT;
  if (received !== expected) fail(`small text: received ${received} messages, expected ${expected}`);

  const seconds = measured / 1000;
  return {
    scenario: `200 B text frames x ${FRAME_COUNT / 1000}K, ${SEGMENT / KIB} KiB segments`,
    mbPerSec: (TEXT.length * FRAME_COUNT * ITERATIONS) / MIB / seconds,
    msgsPerSec: (FRAME_COUNT * ITERATIONS) / seconds,
    messages: received,
  };
};

const formatNumber = (num) => num.toLocaleString('en-US', { maximumFractionDigits: 2 });

const main = () => {
  console.log(`Parser throughput benchmark — Node ${process.version}\n`);
  const results = [benchLargeBinary(), benchSmallText()];
  console.log(`  ${'scenario'.padEnd(46)}${'MB/s'.padStart(12)}${'msgs/sec'.padStart(14)}`);
  for (const { scenario, mbPerSec, msgsPerSec } of results) {
    const row = `${scenario.padEnd(46)}${formatNumber(mbPerSec).padStart(12)}${formatNumber(msgsPerSec).padStart(14)}`;
    console.log(`  ${row}`);
  }
  console.log();
  if (failures > 0) {
    console.error(`Integrity: FAILED (${failures} mismatches)`);
    process.exitCode = 1;
  } else {
    const totalMessages = results.reduce((sum, result) => sum + result.messages, 0);
    console.log(`Integrity: OK (${formatNumber(totalMessages)} messages verified)`);
  }
};

main();
