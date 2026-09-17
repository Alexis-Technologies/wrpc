'use strict';

// The broker port: what wrpc asks of a message broker, split into four
// CAPABILITIES so every broker implements the subset natural to it. An
// adapter is a plain object — nothing here is a base class:
//
//   { name, backplane?, log?, queue?, direct?, close() }
//
// The contracts, precisely (tests/broker/*Contract.js is their executable
// form, run against the memory reference and every adapter):
//
// backplane — the rooms/cluster fan-out, unchanged from @alexify/wrpc/scaling:
//   publish(channel, message)          -> void | Promise
//   subscribe(channel, handler)        -> unsubscribe | Promise<unsubscribe>
//   close()                            -> void | Promise
//   Every subscriber of a channel receives each message, at most once.
//   Channel names are arbitrary strings: an adapter encodes them into
//   whatever its broker allows, and `room:*` must never mean a wildcard.
//
// log — an ordered, replayable journal (what a durable subscription feed
// reads from):
//   append(topic, value, { headers, key })          -> Promise<id>
//   read(topic, { after, from, signal })            -> ReadIterable
//     ReadIterable: AsyncIterable<{ id, value, headers }> & { ready: Promise }
//     - `ready` resolves once the read position is fixed; entries appended
//       after that are guaranteed to be read.
//     - `after` (an id this log minted): entries strictly after it. An id
//       older than retained history, or minted by another log, makes the
//       iteration throw a coded 410; an id beyond the tip, a coded 400.
//     - without `after`: `from: 'latest'` (default) reads only new entries,
//       `'earliest'` everything retained.
//     - `signal` aborting, or breaking out of the loop, ends it cleanly.
//   parseId(text)                                   -> id | null
//     Syntax only — `lastEventId` is peer-controlled, and garbage must be
//     refused before it reaches the broker.
//   Ids are opaque strings; values and header values are strings.
//
// queue — at-least-once work distribution:
//   produce(queue, body, { headers, key })          -> Promise<void>
//   consume(queue, onDelivery, { group, prefetch, deadLetter, signal })
//                                                    -> Promise<Consumer>
//     Consumer: { stop(): Promise<void>, readonly healthy: boolean }
//     - consumers of one queue compete: a message reaches one of them.
//       `group` names the broker-side consumer group where one is needed
//       (Kafka, JetStream, Redis) and defaults to the queue name; one queue
//       is consumed by one group.
//     - at most `prefetch` unsettled deliveries per consumer, dispatched
//       CONCURRENTLY (the adapter drives it, not a sequential loop).
//     - messages produced before any consumer exists are retained.
//   delivery: { id, body, headers, attempt, redelivered,
//               ack(), retry({ delay }), release(), deadLetter(reason) }
//     - the first settlement wins; later ones are no-ops.
//     - retry: redelivered no sooner than `delay` ms, attempt + 1. The
//       attempt counter is the ADAPTER's (RabbitMQ 4 does not count a
//       requeue), carried in `x-wrpc-attempt` where the broker has none.
//     - release: back to the queue, attempt unchanged (a draining node
//       handing work to a healthy one).
//     - deadLetter: to the `deadLetter` queue with `x-wrpc-dead-reason` and
//       `x-wrpc-attempt` headers, or dropped when none is configured.
//     - stop(): no new deliveries; the unsettled ones are redelivered later.
//
// direct — addressable inboxes, the substrate of RPC over a broker:
//   inbox()                                          -> address (sync)
//   listen(address, onMessage, { group })           -> Promise<stop>
//     - resolves once live: a message sent after it is received.
//     - listeners without a group each receive every message; listeners
//       sharing a group compete, one of them per message.
//     - messages from one sender to one address arrive in send order.
//   send(address, body, { headers, correlationId, replyTo, timeout })
//                                                    -> Promise<void>
//     - at-most-once: the RPC binding numbers its frames and treats a gap
//       as a closed connection. MAY reject with a coded 503 when the broker
//       knows nobody listens; `timeout` lets it discard a message nobody
//       took in time.
//   message: { body, headers, correlationId, replyTo } — body is a string
//   or a Uint8Array, as sent.

const { isBackplane } = require('../scaling/index.js');

const isObject = (value) => typeof value === 'object' && value !== null;
const hasFunctions = (value, names) => isObject(value) && names.every((name) => typeof value[name] === 'function');

const isBrokerLog = (value) => hasFunctions(value, ['append', 'read', 'parseId']);
const isBrokerQueue = (value) => hasFunctions(value, ['produce', 'consume']);
const isBrokerDirect = (value) => hasFunctions(value, ['inbox', 'listen', 'send']);

const CAPABILITIES = Object.freeze({
  backplane: isBackplane,
  log: isBrokerLog,
  queue: isBrokerQueue,
  direct: isBrokerDirect,
});

// A broker exposes at least one capability, each one well-shaped when
// present, and a close(). A present-but-malformed capability is a broken
// adapter, not a missing feature — refused rather than silently skipped.
const isBroker = (value) => {
  if (!hasFunctions(value, ['close'])) return false;
  let present = 0;
  for (const name in CAPABILITIES) {
    if (value[name] === undefined || value[name] === null) continue;
    if (!CAPABILITIES[name](value[name])) return false;
    present++;
  }
  return present > 0;
};

// What every scenario entry point accepts: the whole broker, or the bare
// capability object (a hand-written log, a test double).
const capabilityOf = (value, name, label) => {
  const check = CAPABILITIES[name];
  if (check(value)) return value;
  if (isObject(value) && check(value[name])) return value[name];
  const what = isObject(value) && typeof value.name === 'string' ? `the '${value.name}' broker` : 'the given broker';
  throw new TypeError(`${label}: ${what} has no '${name}' capability`);
};

// The broker's name for metric labels and log fields; 'custom' for a bare
// capability nobody named.
const brokerName = (value) =>
  isObject(value) && typeof value.name === 'string' && value.name.length > 0 ? value.name : 'custom';

module.exports = { isBroker, isBrokerLog, isBrokerQueue, isBrokerDirect, isBackplane, capabilityOf, brokerName };
