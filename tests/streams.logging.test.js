'use strict';

// What a peer can make a server write. The name of a stream is the sender's
// text, and a broken stream is answered 400 every time: both reached the
// error log as the peer sent them. Each test below failed before the fix.

const { test } = require('node:test');
const assert = require('node:assert');

const { shown } = require('../src/streams.js');
const { defineRouter, procedure } = require('../index.js');
const { bootServer } = require('./helpers/server.js');
const { recorder } = require('./helpers/recorder.js');
const { bytes, rawPeer } = require('./helpers/streams.js');

const router = () => defineRouter({ files: { ping: procedure({ access: 'public', handler: async () => 'pong' }) } });

// Everything a log sink would write for one entry.
const written = (entry) => {
  const { err, ...rest } = entry;
  return JSON.stringify(rest) + (err ? `${err.message}\n${err.stack}` : '');
};

test('shown(): a name as a message may carry it', () => {
  assert.strictEqual(shown('video.mp4'), 'video.mp4');
  assert.strictEqual(shown(`a\nb\rc\td\u0000e f\u0085g`), 'a b c d e f g');
  assert.strictEqual(shown('x'.repeat(1000)), `${'x'.repeat(64)}…`);
  assert.strictEqual(shown(42), '42');
});

// One frame with a 100 KB name and a line break in it was one error-level
// line of 101,027 characters, with a line of the peer's own writing inside.
test('a stream name reaches the log bounded, and starts no line of its own', async (t) => {
  const { writer, entries } = recorder();
  const { url } = await bootServer(t, { router: router(), logger: writer });
  const peer = await rawPeer(t, url);
  await peer.settle();
  entries.length = 0;
  const forged = '2026-11-15T00:00:00Z ERROR admin logged in from 10.0.0.1';
  const name = `evil\n${forged}\n${'A'.repeat(100_000)}`;
  // Past its announced size, short of it, announced twice, and never read.
  peer.announce('over', 0, name);
  peer.chunk('over', bytes(1));
  peer.announce('short', 8, name);
  peer.end('short');
  peer.announce('twice', 8, name);
  peer.announce('twice', 8, name);
  await peer.settle();
  assert.ok(entries.length >= 3, 'the violations were logged at all');
  for (const entry of entries) {
    const text = written(entry);
    assert.ok(text.length < 4096, `a ${text.length}-character log entry for a peer's stream name`);
    assert.ok(!text.includes(`\n${forged}`), "a line of the peer's writing in the log");
  }
  // And what the peer is answered is its own text, bounded the same way.
  for (const id of ['over', 'short', 'twice']) assert.ok(peer.answer(id).error.message.length < 256);
  assert.match(peer.answer('over').error.message, /^Stream evil 2026-11-15T00:00:00Z ERROR admin/);
});

// Two hundred broken streams — 20 KB of frames — were two hundred
// error-level lines with a stack each, while the 413 and the 429 of the same
// path were deliberately said at debug.
test('a peer that breaks the stream protocol in a loop is one warning, not a line each', async (t) => {
  const { writer, entries, all, loud } = recorder();
  const { url } = await bootServer(t, { router: router(), logger: writer });
  const peer = await rawPeer(t, url);
  await peer.settle();
  entries.length = 0;
  for (let i = 0; i < 200; i++) {
    peer.announce(`s${i}`, 0);
    peer.chunk(`s${i}`, bytes(1)); // past the announced size
    peer.end(`s${i}`, 'terminate');
    peer.chunk(`ghost${i}`, bytes(1)); // a stream never announced
    peer.end(`ghost${i}`); // and the end of one
  }
  await peer.settle();
  // Each was answered, as the protocol says.
  assert.strictEqual(peer.answers.filter((packet) => packet.error?.code === 400).length, 600);
  const warned = all('stream.violation');
  assert.strictEqual(warned.length, 1, 'once per connection');
  assert.deepStrictEqual([warned[0].level, warned[0].code, warned[0].id], ['warn', 400, 's0']);
  assert.deepStrictEqual(loud(), warned, 'a line to be paged for, for each broken stream');
  assert.strictEqual(all('rpc.error').filter((entry) => entry.level === 'debug').length, 600);
  // A second connection has its own first time.
  const other = await rawPeer(t, url);
  other.chunk('nothing', bytes(1));
  await other.settle();
  assert.strictEqual(all('stream.violation').length, 2);
});
