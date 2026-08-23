'use strict';

// Perf/memory guard for the F1 claim: "1 GiB stream without memory growth".
//
// Streams 1 GiB in 64 KiB chunks through the real backpressure chain:
//   WrpcReadable.finalize -> WrpcWritable -> ServerWsTransport -> Connection -> socket
// The socket is a MockSocket subclass modelling kernel pressure: every write is
// counted and then DISCARDED (so the measurement covers OUR buffering, not the
// sink's), writes report false once the simulated buffer exceeds 1 MiB, and the
// buffer drains asynchronously via setImmediate, like a kernel flush.
//
// Deliberately NOT named *.test.js so bare `node --test` discovery skips it.
// Run directly: node tests/perf/stream-memory.perf.js — prints a one-line
// summary and sets exit code 1 when throughput or memory guarantees break.

const { Connection } = require('../../src/websocket/ws.js');
const { ServerTransport } = require('../../src/transport.js');
const { WrpcReadable, WrpcWritable } = require('../../src/streams.js');
const { MockSocket } = require('../websocket/mockSocket.js');

const ServerWsTransport = ServerTransport.transport.ws;

const KIB = 1024;
const MIB = 1024 * KIB;
const CHUNK_SIZE = 64 * KIB;
const TOTAL_BYTES = 1024 * MIB; // 1 GiB
const CHUNK_COUNT = TOTAL_BYTES / CHUNK_SIZE;
const PRESSURE_LIMIT = MIB; // simulated socket buffer high-water mark
const RSS_LIMIT = 256 * MIB; // peak RSS delta budget over the baseline
const SAMPLE_EVERY = 64; // sample RSS every N produced chunks
const STREAM_ID = 'S1';
const STREAM_NAME = 'stream-memory.bin';
// chunkEncode frames each data chunk as [idLength][id][payload] in one buffer;
// only those writes are this long, so the size identifies data chunks at the sink.
const ENCODED_CHUNK_SIZE = 1 + STREAM_ID.length + CHUNK_SIZE;

class PressureSocket extends MockSocket {
  bytesReceived = 0;
  payloadBytes = 0;
  chunkWrites = 0;
  buffered = 0;
  #drainScheduled = false;

  write(data) {
    const size = Buffer.byteLength(data);
    this.bytesReceived += size;
    if (size === ENCODED_CHUNK_SIZE) {
      this.chunkWrites++;
      this.payloadBytes += CHUNK_SIZE;
    }
    // The data itself is discarded: MockSocket.writtenData would retain all
    // 1 GiB and the RSS measurement would be about the sink, not our buffering.
    this.buffered += size;
    this.writableLength = this.buffered;
    if (this.buffered <= PRESSURE_LIMIT) return true;
    if (!this.#drainScheduled) {
      this.#drainScheduled = true;
      setImmediate(() => {
        this.#drainScheduled = false;
        this.buffered = 0;
        this.writableLength = 0;
        this.emit('drain');
      });
    }
    return false;
  }
}

const main = async () => {
  const socket = new PressureSocket();
  const connection = new Connection(socket, Buffer.alloc(0));
  const transport = new ServerWsTransport(connection, { remoteAddress: '127.0.0.1' });
  const writable = new WrpcWritable(STREAM_ID, STREAM_NAME, TOTAL_BYTES, transport);

  // Harness adapters for WrpcReadable.finalize's writable contract: the real
  // WrpcWritable (a utils Emitter) has no waitEvent/removeListener, and no
  // remote peer exists to acknowledge the final 'end' packet, so end() also
  // simulates the peer ack by emitting 'close' on the next microtask.
  writable.waitEvent = (event) => new Promise((resolve) => writable.once(event, resolve));
  writable.removeListener = (event, listener) => writable.off(event, listener);
  const sendEnd = writable.end.bind(writable);
  writable.end = () => {
    sendEnd();
    queueMicrotask(() => void writable.emit('close'));
  };

  let drainWaits = 0;
  writable.on('drain', () => drainWaits++);

  const readable = new WrpcReadable(STREAM_ID, STREAM_NAME, TOTAL_BYTES);

  const baselineRss = process.memoryUsage.rss();
  let peakRss = baselineRss;
  const sampleRss = () => {
    const rss = process.memoryUsage.rss();
    if (rss > peakRss) peakRss = rss;
  };

  const startedAt = performance.now();
  const flow = readable.finalize(writable); // the exact code path pipe() wraps

  for (let i = 0; i < CHUNK_COUNT; i++) {
    await readable.push(Buffer.allocUnsafe(CHUNK_SIZE));
    if (i % SAMPLE_EVERY === 0) sampleRss();
  }
  await readable.close();
  await flow;
  sampleRss();

  const seconds = (performance.now() - startedAt) / 1000;
  connection.terminate();

  const peakDelta = peakRss - baselineRss;
  const rate = (TOTAL_BYTES / MIB / seconds).toFixed(0);
  const deltaMib = (peakDelta / MIB).toFixed(1);
  console.log(
    `stream-memory: ${socket.payloadBytes} bytes in ${seconds.toFixed(2)} s ` +
      `(${rate} MB/s), peak RSS delta ${deltaMib} MiB, drain waits: ${drainWaits}`,
  );

  const failures = [];
  if (socket.chunkWrites !== CHUNK_COUNT || socket.payloadBytes !== TOTAL_BYTES) {
    failures.push(
      `sink received ${socket.payloadBytes} payload bytes in ${socket.chunkWrites} chunks, ` +
        `expected ${TOTAL_BYTES} bytes in ${CHUNK_COUNT} chunks`,
    );
  }
  if (readable.bytesRead !== TOTAL_BYTES) {
    failures.push(`readable consumed ${readable.bytesRead} bytes, expected ${TOTAL_BYTES}`);
  }
  if (drainWaits === 0) {
    failures.push('backpressure never engaged: finalize hit zero drain waits');
  }
  if (peakDelta > RSS_LIMIT) {
    failures.push(`peak RSS delta ${peakDelta} bytes exceeds the ${RSS_LIMIT} bytes budget`);
  }
  for (const failure of failures) console.error(`stream-memory FAIL: ${failure}`);
  if (failures.length > 0) process.exitCode = 1;
};

main().catch((error) => {
  console.error('stream-memory FAIL:', error);
  process.exitCode = 1;
});
