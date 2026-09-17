'use strict';

// Publishing a unit's declared outbound events (`emits`) to a broker.
//
// `emits` stays a declaration — signature descriptors for `wrpc types`,
// no runtime behaviour — so a publisher is bound to it by NAME: every event
// it may publish must be declared, which is what keeps a typo from quietly
// creating a topic nobody consumes. The runtime checks (a validator per
// event) live in the publisher's own table, never inside `emits`, whose
// content has to stay JSON for introspection.
//
// Each publish is a PRODUCER span, and the active trace context rides in
// the message headers (`tp`/`ts`), so the consumer on the other side parents
// its CONSUMER span on it.

const { runValidator } = require('../rpc/router.js');
const { SPAN_KIND_PRODUCER } = require('../telemetry/shared.js');
const { capabilityOf, brokerName, isBrokerLog, isBrokerQueue } = require('./port.js');
const { codedError } = require('./ids.js');
const { rpcOf } = require('./host.js');

const isValidatorShape = (value) =>
  typeof value === 'function' || (typeof value === 'object' && value !== null && '~standard' in value);

const createPublisher = (server, broker, table = {}, options = {}) => {
  const rpc = rpcOf(server, 'createPublisher');
  if (typeof table !== 'object' || table === null || Array.isArray(table)) {
    throw new TypeError('createPublisher: the event table must be an object');
  }
  const { strict = true } = options;
  const hasLog = isBrokerLog(broker) || isBrokerLog(broker?.log);
  const hasQueue = isBrokerQueue(broker) || isBrokerQueue(broker?.queue);
  if (!hasLog && !hasQueue) {
    throw new TypeError('createPublisher: the broker has neither a log nor a queue capability');
  }
  const system = brokerName(broker);
  const declared = rpc.router.introspect(null, { schemas: false });

  const events = new Map();
  for (const name of Object.keys(table)) {
    const label = `createPublisher: '${name}'`;
    const slash = name.indexOf('/');
    if (slash <= 0 || slash === name.length - 1) throw new TypeError(`${label}: an event name is 'unit/event'`);
    const unitKey = name.slice(0, slash);
    const event = name.slice(slash + 1);
    if (strict && !Object.hasOwn(declared[unitKey]?.emits ?? {}, event)) {
      throw new TypeError(`${label}: the router declares no such event in ${unitKey}.emits`);
    }
    const entry = table[name] ?? {};
    const { topic = `${unitKey}.${event}`, to = hasLog ? 'log' : 'queue', key = null, validate = null } = entry;
    if (typeof topic !== 'string' || topic.length === 0) {
      throw new TypeError(`${label}: topic must be a non-empty string`);
    }
    if (to !== 'log' && to !== 'queue') throw new TypeError(`${label}: to must be 'log' or 'queue'`);
    const target = capabilityOf(broker, to, label);
    if (key !== null && typeof key !== 'string' && typeof key !== 'function') {
      throw new TypeError(`${label}: key must be a string or a (data) => string function`);
    }
    if (validate !== null && !isValidatorShape(validate)) {
      throw new TypeError(`${label}: validate must be a function or a Standard Schema`);
    }
    events.set(name, { topic, to, target, key, validate });
  }

  const publish = async (name, data, { headers = null, key: keyOverride = null } = {}) => {
    const event = events.get(name);
    if (!event) throw new TypeError(`createPublisher: '${name}' is not in this publisher's table`);
    const { topic, to, target, validate } = event;
    const otel = rpc.otel;
    const attributes = {
      'messaging.system': system,
      'messaging.destination.name': topic,
      'messaging.operation.type': 'send',
      'wrpc.event.name': name,
    };
    return otel.withMessagingSpan(
      { name: `${topic} publish`, kind: SPAN_KIND_PRODUCER, attributes },
      async (handle) => {
        try {
          let value = data;
          if (validate) {
            try {
              const checked = runValidator(validate, data);
              value = checked && typeof checked.then === 'function' ? await checked : checked;
            } catch (error) {
              const refused = codedError(`Invalid ${name} payload: ${error.message}`, 400);
              if (error.details !== undefined) refused.details = error.details;
              throw refused;
            }
          }
          const carried = headers ? { ...headers } : {};
          otel.inject(carried);
          const key = keyOverride ?? (typeof event.key === 'function' ? event.key(value) : event.key);
          const body = JSON.stringify(value);
          const message =
            key === null || key === undefined ? { headers: carried } : { headers: carried, key: String(key) };
          const result =
            to === 'log' ? await target.append(topic, body, message) : await target.produce(topic, body, message);
          otel.recordBrokerPublish(system, 'ok');
          return to === 'log' ? result : undefined;
        } catch (error) {
          otel.recordBrokerPublish(system, 'error');
          otel.recordError(handle, error, typeof error.code === 'number' ? error.code : 500);
          throw error;
        } finally {
          otel.endSpan(handle);
        }
      },
    );
  };

  return {
    publish,
    get events() {
      return Array.from(events.keys());
    },
  };
};

module.exports = { createPublisher };
