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
const { attachBrokerRpc } = require('./rpc/server.js');
const { createBrokerSealing } = require('./sealing.js');
// Registers WrpcClient.transport.broker as a side effect.
const { ClientBrokerTransport } = require('./rpc/client.js');

/**
 * Opens one sealed log entry or delivery by hand — a dead letter, a row
 * read straight off a topic — under the keyring that sealed it: the same
 * `encryption` option the bindings take, `topic` the queue or topic the
 * message was sealed for. Answers `{ headers, body, sealed }` with the body
 * as text, or `{ refused }` with the reason.
 */
const openSealedMessage = (encryption, { topic, headers, body } = {}) => {
  const sealing = createBrokerSealing(encryption, 'openSealedMessage', {
    layer: 'broker-log',
    replay: false,
    text: true,
  });
  if (sealing === null) throw new TypeError('openSealedMessage: encryption is required');
  if (typeof topic !== 'string' || topic.length === 0) {
    throw new TypeError('openSealedMessage: topic must be the queue or topic the message was sealed for');
  }
  const opened = sealing.open(topic, { headers: toHeaders(headers), body });
  if (opened.refused !== undefined) return opened;
  return { headers: opened.headers, body: toText(opened.body), sealed: opened.sealed };
};

module.exports = {
  brokerFeed,
  attachConsumers,
  createPublisher,
  attachBrokerRpc,
  openSealedMessage,
  ClientBrokerTransport,
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
