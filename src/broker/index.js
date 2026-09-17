'use strict';

// @alexify/wrpc/broker — the broker-agnostic core: the capability contracts
// (port.js), the in-process reference broker, and the building blocks an
// adapter for another broker reuses. Every concrete broker lives behind its
// own subpath (@alexify/wrpc/broker/redis, /nats, /amqp, /kafka) and takes
// its client by injection; nothing here requires a broker package.
//
// Experimental: the API may change in a minor release until every adapter
// has shipped (see docs/reference/stability.md).

const { isBroker, isBrokerLog, isBrokerQueue, isBrokerDirect, isBackplane } = require('./port.js');
const { MemoryBroker, createMemoryBroker } = require('./memory.js');
const { TopicTails } = require('./tail.js');
const { encodeToken, toText, toBytes, toHeaders } = require('./ids.js');
const { DEFAULT_RETRY } = require('./retry.js');
const { brokerFeed } = require('./feed.js');
const { attachConsumers } = require('./consumers.js');
const { createPublisher } = require('./publisher.js');

module.exports = {
  brokerFeed,
  attachConsumers,
  createPublisher,
  MemoryBroker,
  createMemoryBroker,
  isBroker,
  isBrokerLog,
  isBrokerQueue,
  isBrokerDirect,
  isBackplane,
  TopicTails,
  encodeToken,
  toText,
  toBytes,
  toHeaders,
  DEFAULT_RETRY,
};
