'use strict';

// A preset dictionary for the per-message codecs, built from what a router
// declares. One-shot deflate has no history to lean on: a 90 B event
// compresses to 79 B because every field name and every method target is
// seen for the first time, in every message. A preset dictionary IS that
// history — the strings a wrpc packet is made of, sitting in the window
// before the first byte — and a 169 B event becomes 76 B instead of 133
// (bench/dictionary.js), with no state per connection and the frames of a
// fan-out still shareable.
//
// zlib matches against the LAST 32 KiB of a dictionary and finds recent
// bytes cheapest, so the content is ordered least to most frequent: the
// field names a router's signatures and schemas declare, then every
// `unit/method` target and `unit/event` name, then the packet skeletons
// every message starts with. Past the cap the FRONT is dropped, never the
// tail. Deterministic — sorted within each group — so two instances of
// the same router build the same bytes, and the codec id derived from them
// (dictionaryId) agrees across a fleet.
//
// Node-free and browser-safe (TextEncoder only): a browser peer host can
// build one for the pure-JS codec of @alexify/wrpc/deflate.

const MAX_DICTIONARY = 32 * 1024;
const MAX_DEPTH = 8;

// The JSON every packet type starts with, most frequent last. The event,
// callback and data skeletons are what a busy connection mostly carries;
// ping/pong close the list because a heartbeat is the most repeated
// message of all.
const SKELETONS = [
  '{"type":"stream","id":"',
  '"name":"',
  '"size":',
  '{"type":"subscribe","id":"',
  '"lastEventId":"',
  '{"type":"unsubscribe","id":"',
  '{"type":"cancel","id":"',
  '{"type":"end","id":"',
  '"error":{"message":"',
  '"code":',
  '"details":',
  '"meta":{',
  '"tp":"00-',
  '"ts":"',
  ',"unreliable":true',
  '"eventId":"',
  '{"type":"data","id":"',
  '"data":',
  '{"type":"event","name":"',
  '"args":',
  '"method":"',
  '{"type":"call","id":"',
  '"result":',
  '{"type":"callback","id":"',
  'true',
  'false',
  'null',
  '{"type":"ping"}',
  '{"type":"pong"}',
];

// Field names a `signature` shape declares (protocol.md#the-signature-descriptor):
// a type name, a field map, or a one-element array — maps nest.
const shapeKeys = (shape, out, depth = 0) => {
  if (depth > MAX_DEPTH || shape === null || typeof shape !== 'object') return;
  if (Array.isArray(shape)) {
    for (let i = 0; i < shape.length; i++) shapeKeys(shape[i], out, depth + 1);
    return;
  }
  for (const key of Object.keys(shape)) {
    out.add(key.endsWith('?') ? key.slice(0, -1) : key);
    shapeKeys(shape[key], out, depth + 1);
  }
};

// Field names a JSON-Schema-shaped part declares: `properties`, through
// `items` and the combinators.
const schemaKeys = (schema, out, depth = 0) => {
  if (depth > MAX_DEPTH || schema === null || typeof schema !== 'object') return;
  if (Array.isArray(schema)) {
    for (let i = 0; i < schema.length; i++) schemaKeys(schema[i], out, depth + 1);
    return;
  }
  const { properties } = schema;
  if (properties !== null && typeof properties === 'object') {
    for (const key of Object.keys(properties)) {
      out.add(key);
      schemaKeys(properties[key], out, depth + 1);
    }
  }
  if (schema.items !== undefined) schemaKeys(schema.items, out, depth + 1);
  if (schema.anyOf !== undefined) schemaKeys(schema.anyOf, out, depth + 1);
  if (schema.oneOf !== undefined) schemaKeys(schema.oneOf, out, depth + 1);
  if (schema.allOf !== undefined) schemaKeys(schema.allOf, out, depth + 1);
};

/**
 * The dictionary bytes for `router` (anything with `introspect()`), at
 * most `limit` (32 KiB by default — what zlib looks at). Deterministic for
 * a given router definition.
 */
const buildDictionary = (router, { limit = MAX_DICTIONARY } = {}) => {
  if (!router || typeof router.introspect !== 'function') {
    throw new TypeError('buildDictionary: a Router (anything with introspect()) is required');
  }
  if (!Number.isInteger(limit) || limit <= 0) throw new TypeError('buildDictionary: limit must be a positive integer');
  const info = router.introspect(null, { schemas: true });
  const keys = new Set();
  const targets = [];
  const events = [];
  for (const unit of Object.keys(info)) {
    const methods = info[unit];
    for (const name of Object.keys(methods)) {
      const entry = methods[name];
      if (name === 'on' || name === 'emits') {
        // The unit's inbound handlers and declared outbound events, both
        // named on the wire as `unit/event`.
        for (const event of Object.keys(entry)) {
          events.push(`${unit}/${event}`);
          const declared = entry[event];
          if (declared && typeof declared === 'object') {
            shapeKeys(declared.signature?.args, keys);
            shapeKeys(declared.data, keys);
            shapeKeys(declared.returns, keys);
          }
        }
        continue;
      }
      targets.push(`${unit}/${name}`);
      if (entry.signature) {
        shapeKeys(entry.signature.args, keys);
        shapeKeys(entry.signature.returns, keys);
        shapeKeys(entry.signature.data, keys);
      }
      if (entry.schema) {
        schemaKeys(entry.schema.params, keys);
        schemaKeys(entry.schema.querystring, keys);
        schemaKeys(entry.schema.body, keys);
      }
    }
  }
  const parts = [];
  const sortedKeys = Array.from(keys).sort();
  for (let i = 0; i < sortedKeys.length; i++) parts.push(`"${sortedKeys[i]}":`);
  targets.sort();
  for (let i = 0; i < targets.length; i++) parts.push(`"method":"${targets[i]}"`);
  events.sort();
  for (let i = 0; i < events.length; i++) parts.push(`"name":"${events[i]}"`);
  for (let i = 0; i < SKELETONS.length; i++) parts.push(SKELETONS[i]);
  const bytes = new TextEncoder().encode(parts.join(''));
  // The tail is the frequent end: cut the front. A cut inside a multi-byte
  // character is harmless — a dictionary is bytes, not text.
  return bytes.length > limit ? bytes.subarray(bytes.length - limit) : bytes;
};

module.exports = { buildDictionary, MAX_DICTIONARY };
