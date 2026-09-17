'use strict';

// Queue consumers: broker messages delivered into router procedures,
// at least once.
//
// Each binding owns one long-lived client attached to the RpcServer as a
// request/response carrier (`attach({ persistent: false })`), and every
// delivery becomes an ordinary call packet dispatched through handleRpc —
// so the consumer runs the SAME pipeline a WebSocket call does: onRequest,
// the ready gate, access, preValidation, validators, preHandler, the
// procedure's queue and timeout, onSend/onError/onTimeout, telemetry. The
// outcome decides the settlement through retry.js: ack, retry with
// backoff, release (a draining node), or the dead-letter queue.
//
// Consumer procedures come from two places:
// - a unit's reserved `consumes` block (the static declaration; unreachable
//   by any call packet), bound automatically;
// - the binding table, which overrides a declared consumer's policy
//   ('unit.v1/source') or binds an ordinary procedure to a queue
//   (`{ target: 'unit.v1/method' }`) — deployment topology, not code.
//
// Nothing about the dispatcher changed for this: handleRpc takes the router
// as a parameter, and each binding hands it a one-procedure VIEW whose hooks
// and compiled validators are the real router's.

const { Emitter } = require('../utils.js');
const { handleRpc, parseTarget } = require('../rpc/dispatcher.js');
const { TRACEPARENT, TRACESTATE, SPAN_KIND_CONSUMER } = require('../telemetry/shared.js');
const { createLoggerWriter } = require('../logging.js');
const { capabilityOf, brokerName } = require('./port.js');
const { normalizeRetry, decide } = require('./retry.js');
const { rpcOf } = require('./host.js');

const DEFAULT_PREFETCH = 16;
const DEFAULT_TOKEN_CLIENTS = 128;
const DEFAULT_VERSION = '*';
const TRUST = new Set(['none', 'service', 'token']);

// The outbound half of a consumer client: it never writes to a wire, it
// settles the delivery waiting on the call id.
class ConsumerTransport extends Emitter {
  kind = 'broker';
  connection = true; // cleared by attach({ persistent: false })
  #pending = new Map(); // call id -> resolve
  #closed = false;

  constructor(source) {
    super({ maxListeners: Number.MAX_SAFE_INTEGER });
    this.source = source;
  }

  expect(id) {
    return new Promise((resolve) => {
      if (this.#closed) return void resolve({ code: 503, error: null, closed: true });
      this.#pending.set(id, resolve);
    });
  }

  settle(id, outcome) {
    const resolve = this.#pending.get(id);
    if (!resolve) return;
    this.#pending.delete(id);
    resolve(outcome);
  }

  send(packet) {
    if (packet?.type === 'callback' && !packet.error) this.settle(packet.id, { code: null, error: null });
    return true;
  }

  // The original error, before wireError masks it: what a dead-letter
  // reason and the log want.
  error(code, { id = '', error = null } = {}) {
    this.settle(id, { code, error });
    return true;
  }

  write() {
    return true;
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    // A call the close aborted never answers: its delivery is released
    // rather than left waiting forever.
    for (const resolve of this.#pending.values()) resolve({ code: 503, error: null, closed: true });
    this.#pending.clear();
    this.emit('close');
  }
}

const labelOf = (unitKey, name) => `${unitKey}/${name}`;

const parseJson = (body) => JSON.parse(body);

const normalizePolicy = (policy, { label, queue: defaultQueue, maxCalls }) => {
  if (policy !== undefined && policy !== null && (typeof policy !== 'object' || Array.isArray(policy))) {
    throw new TypeError(`${label}: the binding must be an object`);
  }
  const {
    queue = defaultQueue,
    group = null,
    prefetch = DEFAULT_PREFETCH,
    retry,
    deadLetter,
    identity = { trust: 'none' },
    meta = [],
    args = parseJson,
  } = policy ?? {};
  if (typeof queue !== 'string' || queue.length === 0) {
    throw new TypeError(`${label}: queue must be a non-empty string`);
  }
  if (group !== null && (typeof group !== 'string' || group.length === 0)) {
    throw new TypeError(`${label}: group must be a non-empty string`);
  }
  if (!Number.isInteger(prefetch) || prefetch <= 0) {
    throw new TypeError(`${label}: prefetch must be a positive integer`);
  }
  // One client runs every delivery of the binding: past maxCalls the
  // dispatcher would answer 429 to messages the broker handed us.
  if (prefetch > maxCalls) {
    throw new TypeError(`${label}: prefetch ${prefetch} exceeds the server's maxCalls ${maxCalls}`);
  }
  const dead = deadLetter === undefined ? `${queue}.dlq` : deadLetter;
  if (dead !== false && dead !== null && (typeof dead !== 'string' || dead.length === 0)) {
    throw new TypeError(`${label}: deadLetter must be a queue name or false`);
  }
  if (!identity || typeof identity !== 'object' || !TRUST.has(identity.trust)) {
    throw new TypeError(`${label}: identity.trust must be 'none', 'service' or 'token'`);
  }
  if (!Array.isArray(meta) || !meta.every((name) => typeof name === 'string')) {
    throw new TypeError(`${label}: meta must be an array of header names`);
  }
  if (typeof args !== 'function') throw new TypeError(`${label}: args must be a function`);
  return {
    queue,
    group: group ?? queue,
    prefetch,
    retry: normalizeRetry(retry, label),
    deadLetter: dead || null,
    identity: { trust: identity.trust, session: identity.session ?? null, header: identity.header ?? 'authorization' },
    meta: meta.map((name) => name.toLowerCase()),
    args,
  };
};

/**
 * Binds broker queues to procedures. Resolves once every consumer is live.
 *
 *   const consumers = await attachConsumers(server, broker, {
 *     'billing.v1/orders.created': { prefetch: 32 },            // override a declared consumer
 *     'audit.events': { target: 'audit.v1/record' },             // bind an ordinary procedure
 *   });
 */
const attachConsumers = async (server, broker, table = {}, options = {}) => {
  const rpc = rpcOf(server, 'attachConsumers');
  const queue = capabilityOf(broker, 'queue', 'attachConsumers');
  const system = brokerName(queue) === 'custom' ? brokerName(broker) : brokerName(queue);
  if (typeof table !== 'object' || table === null || Array.isArray(table)) {
    throw new TypeError('attachConsumers: the binding table must be an object');
  }
  const { auto = true, onDeadLetter = null, logger = null, tokenClients = DEFAULT_TOKEN_CLIENTS } = options;
  if (onDeadLetter !== null && typeof onDeadLetter !== 'function') {
    throw new TypeError('attachConsumers: onDeadLetter must be a function');
  }
  const router = rpc.router;
  const log = createLoggerWriter(logger ?? globalThis.console).child({ component: 'broker', broker: system });
  const { maxCalls } = rpc.limits;

  // Resolve every binding BEFORE starting any: a typo in the table must
  // fail the attach, not leave half the consumers running.
  const plans = [];
  const declared = new Map();
  for (const entry of router.consumers()) declared.set(labelOf(entry.unitKey, entry.name), entry);
  for (const key of Object.keys(table)) {
    const override = table[key] ?? {};
    const label = `attachConsumers: '${key}'`;
    if (typeof override !== 'object' || Array.isArray(override)) {
      throw new TypeError(`${label}: the binding must be an object`);
    }
    const entry = declared.get(key);
    if (entry) {
      if (override.target !== undefined) throw new TypeError(`${label}: a declared consumer cannot be retargeted`);
      plans.push({ key, entry, policy: { ...entry.procedure.consume, ...override } });
      continue;
    }
    if (typeof override.target !== 'string') {
      throw new TypeError(`${label}: neither a declared consumer (unit/source) nor a binding with a target`);
    }
    const { unit, version, name } = parseTarget(override.target);
    const proc = router.getProcedure(unit, version, name);
    if (!proc) throw new TypeError(`${label}: target '${override.target}' is not a procedure`);
    if (proc.subscription) throw new TypeError(`${label}: target '${override.target}' is a subscription`);
    const unitKey = version === DEFAULT_VERSION ? unit : `${unit}.${version}`;
    const { target, ...policy } = override;
    plans.push({ key, entry: { unitKey, name, procedure: proc }, policy, queue: key, method: target });
  }
  if (auto) {
    for (const [key, entry] of declared) {
      if (!Object.hasOwn(table, key)) plans.push({ key, entry, policy: entry.procedure.consume ?? {} });
    }
  }

  const bindings = [];
  for (const plan of plans) {
    const { key, entry } = plan;
    const label = `attachConsumers: '${key}'`;
    const policy = normalizePolicy(plan.policy, { label, queue: plan.queue ?? entry.name, maxCalls });
    if (entry.procedure.access !== 'public' && policy.identity.trust === 'none') {
      throw new TypeError(
        `${label}: the procedure requires a session, so the binding needs identity.trust 'service' or 'token'`,
      );
    }
    bindings.push({
      key,
      method: plan.method ?? labelOf(entry.unitKey, `consumes.${entry.name}`),
      procedure: entry.procedure,
      policy,
    });
  }

  const handles = bindings.map((binding) =>
    bindConsumer({ rpc, queue, system, binding, log, onDeadLetter, tokenClients }),
  );
  try {
    await Promise.all(handles.map((handle) => handle.start()));
  } catch (error) {
    await Promise.all(handles.map((handle) => handle.stop()));
    throw error;
  }

  const onDraining = () => {
    for (const handle of handles) void handle.pause();
  };
  let stopped = false;
  const stop = async () => {
    if (stopped) return;
    stopped = true;
    rpc.off('draining', onDraining);
    rpc.off('close', onClose);
    await Promise.all(handles.map((handle) => handle.stop()));
  };
  const onClose = () => void stop();
  rpc.on('draining', onDraining);
  rpc.on('close', onClose);
  return {
    get bindings() {
      return handles.map((handle) => handle.describe());
    },
    get healthy() {
      return handles.every((handle) => handle.healthy);
    },
    pause: () => Promise.all(handles.map((handle) => handle.pause())).then(() => undefined),
    resume: () => Promise.all(handles.map((handle) => handle.resume())).then(() => undefined),
    stop,
  };
};

// One binding: its clients, its broker consumer, the per-delivery dispatch.
const bindConsumer = ({ rpc, queue, system, binding, log, onDeadLetter, tokenClients }) => {
  const { policy, procedure, method } = binding;
  const router = rpc.router;
  // The one-procedure router view handleRpc dispatches against.
  const view = {
    getProcedure: () => procedure,
    hooksFor: (proc) => router.hooksFor(proc),
    compiledFor: (proc) => router.compiledFor(proc),
  };
  const spanAttributes = {
    'messaging.system': system,
    'messaging.destination.name': policy.queue,
    'messaging.consumer.group.name': policy.group,
    'messaging.operation.type': 'process',
  };
  let consumer = null;
  let stopped = false;

  const attachClient = (identity) => {
    const transport = new ConsumerTransport(`broker:${policy.queue}`);
    const client = rpc.attach(transport, { persistent: false, ...identity });
    client.spanKind = SPAN_KIND_CONSUMER;
    client.spanAttributes = spanAttributes;
    return { client, transport };
  };

  let shared = null; // the 'none' / 'service' client
  const tokens = new Map(); // token -> { client, transport }, LRU by insertion
  const clientFor = (headers) => {
    if (policy.identity.trust !== 'token') {
      if (!shared) {
        const session =
          policy.identity.trust === 'service'
            ? (policy.identity.session ?? { token: `consumer:${policy.queue}`, state: { consumer: policy.queue } })
            : null;
        shared = attachClient(session ? { session } : {});
      }
      return shared;
    }
    const token = headers[policy.identity.header] ?? '';
    const cached = tokens.get(token);
    if (cached) {
      tokens.delete(token);
      tokens.set(token, cached);
      return cached;
    }
    const attached = attachClient({ request: { headers: { authorization: token } } });
    tokens.set(token, attached);
    if (tokens.size > tokenClients) {
      const [oldest, evicted] = tokens.entries().next().value;
      tokens.delete(oldest);
      // Its in-flight calls settle as released: the broker redelivers them.
      evicted.transport.close();
    }
    return attached;
  };

  const settle = async (delivery, decision, code, error) => {
    const { action, delay } = decision;
    rpc.otel.recordBrokerDelivery(system, action);
    if (action === 'ack') return delivery.ack();
    if (action === 'retry') return delivery.retry({ delay });
    if (action === 'release') return delivery.release();
    const reason = `${code}${error?.message ? ` ${error.message}` : ''}`;
    log.warn({ event: 'broker.dead', queue: policy.queue, method, code, attempt: delivery.attempt, id: delivery.id });
    if (onDeadLetter) {
      try {
        await onDeadLetter({ queue: policy.queue, method, code, error, delivery });
      } catch (hookError) {
        log.error({ event: 'broker.onDeadLetter', err: hookError });
      }
    }
    return delivery.deadLetter(reason);
  };

  const onDelivery = async (delivery) => {
    if (stopped || rpc.draining) return void (await settle(delivery, { action: 'release', delay: 0 }));
    let args;
    try {
      args = policy.args(delivery.body, delivery.headers, delivery);
    } catch (error) {
      return void (await settle(delivery, { action: 'dead', delay: 0 }, 400, error));
    }
    const { client, transport } = clientFor(delivery.headers);
    const id = client.generateId();
    const meta = { messageId: delivery.id, attempt: delivery.attempt, queue: policy.queue };
    for (const name of policy.meta) {
      const value = delivery.headers[name];
      if (value !== undefined) meta[name] = value;
    }
    const packet = { type: 'call', id, method, args, meta };
    if (typeof delivery.headers.tp === 'string') packet[TRACEPARENT] = delivery.headers.tp;
    if (typeof delivery.headers.ts === 'string') packet[TRACESTATE] = delivery.headers.ts;
    const outcome = transport.expect(id);
    handleRpc(client, packet, view).catch((error) => transport.settle(id, { code: 500, error }));
    const { code, error, closed } = await outcome;
    if (closed) return void (await settle(delivery, { action: 'release', delay: 0 }));
    const decision = decide({ code, attempt: delivery.attempt, retry: policy.retry, draining: rpc.draining });
    await settle(delivery, decision, code, error);
  };

  const start = async () => {
    consumer = await queue.consume(
      policy.queue,
      (delivery) =>
        onDelivery(delivery).catch((error) => {
          // A settlement the broker refused: the message comes back on its
          // own (redelivery), so this is a log line, not a loss.
          log.error({ event: 'broker.settle', queue: policy.queue, err: error });
        }),
      { group: policy.group, prefetch: policy.prefetch, deadLetter: policy.deadLetter },
    );
    if (stopped) await consumer.stop();
  };

  const stop = async () => {
    if (stopped) return;
    stopped = true;
    if (consumer) await consumer.stop();
    if (shared) shared.transport.close();
    for (const attached of tokens.values()) attached.transport.close();
    tokens.clear();
  };

  return {
    start,
    stop,
    pause: async () => {
      if (consumer && !stopped) await consumer.pause();
    },
    resume: async () => {
      if (consumer && !stopped) await consumer.resume();
    },
    get healthy() {
      return !stopped && consumer !== null && consumer.healthy;
    },
    describe: () => ({
      key: binding.key,
      queue: policy.queue,
      group: policy.group,
      method,
      healthy: !stopped && consumer !== null && consumer.healthy,
    }),
  };
};

module.exports = { attachConsumers, ConsumerTransport };
