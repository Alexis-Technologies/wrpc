'use strict';

const { test } = require('node:test');
const assert = require('node:assert');

const wrpcBrowser = require('../src/index.browser.js');
const { generateUUID } = require('../src/runtime/browser.js');

test('browser barrel exposes only browser-safe exports', () => {
  assert.deepStrictEqual(Object.keys(wrpcBrowser).sort(), [
    'Emitter',
    'EventStream',
    'WrpcClient',
    'WrpcClientProxy',
    'WrpcError',
    'WrpcReadable',
    'WrpcWritable',
    'chunkDecode',
    'chunkEncode',
    'connect',
    'createEventStream',
    'isCodec',
  ]);
  assert.strictEqual(typeof wrpcBrowser.Emitter, 'function');
  assert.strictEqual(typeof wrpcBrowser.connect, 'function');
  assert.strictEqual(typeof wrpcBrowser.EventStream, 'function');
  assert.strictEqual(typeof wrpcBrowser.createEventStream, 'function');
  assert.strictEqual(typeof wrpcBrowser.WrpcClient, 'function');
  assert.strictEqual(typeof wrpcBrowser.WrpcClientProxy, 'function');
  assert.strictEqual(typeof wrpcBrowser.WrpcError, 'function');
  assert.strictEqual(typeof wrpcBrowser.WrpcReadable, 'function');
  assert.strictEqual(typeof wrpcBrowser.WrpcWritable, 'function');

  const payload = new Uint8Array([1, 2, 3]);
  const chunk = wrpcBrowser.chunkEncode('id', payload);
  const decoded = wrpcBrowser.chunkDecode(chunk);
  assert.strictEqual(decoded.id, 'id');
  assert.deepStrictEqual(decoded.payload, payload);
});

test('runtime/browser generateUUID returns a valid UUID via globalThis.crypto', () => {
  const uuid = generateUUID();
  assert.match(uuid, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  assert.notStrictEqual(uuid, generateUUID());
});
