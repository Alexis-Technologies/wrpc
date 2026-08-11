'use strict';

const { MemoryBackplane, createMemoryBackplane, DEFAULT_PREFIX } = require('./memory.js');
const { createRedisAdapter } = require('./redis.js');

// The structural backplane contract — anything with this shape plugs into
// `new Server({ backplane })` / `new RpcServer({ backplane })`:
//
//   publish(channel, message)            -> void | Promise<unknown>
//   subscribe(channel, handler)          -> unsubscribe | Promise<unsubscribe>
//   close()                              -> void | Promise<unknown>
//
// `message` is always a string (the rooms layer serializes its envelope), so
// an adapter needs no knowledge of the payload shape.
//
// Guarantees: at-most-once. A message published while an instance is between
// subscriptions, or dropped by the broker, is gone — rooms are a fan-out
// mechanism, not a queue. Replay of missed events lands with subscriptions
// and lastEventId.
const isBackplane = (value) =>
  typeof value === 'object' &&
  value !== null &&
  typeof value.publish === 'function' &&
  typeof value.subscribe === 'function' &&
  typeof value.close === 'function';

module.exports = {
  MemoryBackplane,
  createMemoryBackplane,
  createRedisAdapter,
  isBackplane,
  DEFAULT_PREFIX,
};
