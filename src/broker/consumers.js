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
const { toText, reasonText } = require('./ids.js');
const { createBrokerSealing } = require('./sealing.js');

const DEFAULT_PREFETCH = 16;
const DEFAULT_TOKEN_CLIENTS = 128;
const DEFAULT_TOKEN_TTL = 60_000;
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

  /** Deliveries whose call has not answered yet. */
  get pending() {
    return this.#pending.size;
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
  const {
    auto = true,
    onDeadLetter = null,
    logger = null,
    tokenClients = DEFAULT_TOKEN_CLIENTS,
    tokenTtl = DEFAULT_TOKEN_TTL,
    encryption = null,
  } = options;
  // The consuming half of a publisher's `encryption`: a delivery is opened
  // before anything reads it — its headers carry the credential a binding
  // may restore a session from — bound to the queue it was produced to. No
  // replay window: a redelivery is the same message, on purpose.
  const sealing = createBrokerSealing(encryption, 'attachConsumers: options', {
    layer: 'broker-log',
    replay: false,
    text: true,
  });
  if (onDeadLetter !== null && typeof onDeadLetter !== 'function') {
    throw new TypeError('attachConsumers: onDeadLetter must be a function');
  }
  // `tokenClients: 0` used to evict every client the moment a second token
  // arrived, and a string compared as a number never evicted at all.
  if (!Number.isInteger(tokenClients) || tokenClients <= 0) {
    throw new TypeError('attachConsumers: tokenClients must be a positive integer');
  }
  // A cached token client is presented to the session store again once it
  // is this old, so a logout or a rotation takes effect within one ttl; 0
  // keeps a client for the life of the binding (`forget()` stays immediate).
  if (!Number.isInteger(tokenTtl) || tokenTtl < 0) {
    throw new TypeError('attachConsumers: tokenTtl must be a non-negative integer of milliseconds (0 disables it)');
  }
  // A server that serves sealed transports only: a binding that does not
  // seal would attach clients it refuses every delivery on — and used to
  // attach them anyway, with every delivery settling nowhere.
  if (rpc.encryptionRequired === true && sealing === null) {
    throw new TypeError(
      'attachConsumers: the server requires encryption — bind with { encryption: { keys } } (the sealed log)',
    );
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
    bindConsumer({ rpc, queue, system, binding, log, onDeadLetter, tokenClients, tokenTtl, sealing }),
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
    // The logout hook: the next delivery under this header value attaches
    // afresh, whatever `tokenTtl` says. The value as messages carry it.
    forget: (token) => {
      if (typeof token !== 'string') throw new TypeError('ConsumersHandle.forget: token must be a string');
      let found = false;
      for (const handle of handles) found = handle.forget(token) || found;
      return found;
    },
    stop,
  };
};

// One binding: its clients, its broker consumer, the per-delivery dispatch.
const bindConsumer = ({ rpc, queue, system, binding, log, onDeadLetter, tokenClients, tokenTtl, sealing }) => {
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
    // Vouched for at the binding, not per delivery: a sealing binding
    // opens every delivery before it dispatches (a plaintext one under
    // `required` is dead-lettered below), so its clients are encrypted ones.
    const client = rpc.attach(transport, { persistent: false, encrypted: sealing !== null, ...identity });
    client.spanKind = SPAN_KIND_CONSUMER;
    client.spanAttributes = spanAttributes;
    return { client, transport };
  };

  let shared = null; // the 'none' / 'service' client
  const tokens = new Map(); // token -> { client, transport, retired }, LRU by insertion
  // A client dropped from the cache outlives its last delivery, not longer.
  // Closing it at once settled every call it still held as released — the
  // broker redelivered them, so a cache merely wrapping around under more
  // distinct tokens than `tokenClients` turned into duplicated work, and
  // under prefetch > tokenClients into a livelock where no delivery ever
  // finished. Retired instead: the delivery that finds it so closes it after
  // its own settlement (see onDelivery).
  const retire = (attached) => {
    if (attached.transport.pending === 0) attached.transport.close();
    else attached.retired = true;
  };
  const forget = (token) => {
    const cached = tokens.get(token);
    if (!cached) return false;
    tokens.delete(token);
    retire(cached);
    return true;
  };
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
      // '' is the anonymous client: there is nothing to re-validate.
      if (token === '' || tokenTtl === 0 || Date.now() - cached.attachedAt < tokenTtl) {
        tokens.delete(token);
        tokens.set(token, cached);
        return cached;
      }
      // Past its ttl: the token goes to the session store again, so a
      // logout or a rotation since the first delivery is what this one sees.
      tokens.delete(token);
      retire(cached);
    }
    const attached = {
      ...attachClient({ request: { headers: { authorization: token } } }),
      retired: false,
      attachedAt: Date.now(),
    };
    tokens.set(token, attached);
    // A restore that did not happen — the store was down, the token names
    // no session — used to be cached as "this token is anonymous", and every
    // later delivery under it was refused 403 until an eviction. Dropped
    // instead: the next delivery presents the token to the store again.
    if (token !== '') {
      void attached.client.sessionReady.then((restored) => {
        if (restored === true || tokens.get(token) !== attached) return;
        tokens.delete(token);
        retire(attached);
      });
    }
    if (tokens.size > tokenClients) {
      const [oldest, evicted] = tokens.entries().next().value;
      tokens.delete(oldest);
      // Not free even so — the next message with that token attaches and
      // restores its session again — and a cache thrashing at its ceiling
      // did it silently. The token is NOT logged: it is a credential.
      log.warn({ event: 'broker.evict', queue: policy.queue, cached: tokens.size, max: tokenClients });
      retire(evicted);
    }
    return attached;
  };

  const settle = async (delivery, decision, code, error) => {
    const { action, delay } = decision;
    rpc.otel.recordBrokerDelivery(system, action);
    // Only on a settlement that ENDS the message: recording on a retry too
    // would count the same message once per attempt and turn the histogram
    // into a triangle rather than a distribution.
    if (action === 'ack' || action === 'dead') rpc.otel.recordBrokerAttempts(system, delivery.attempt ?? 1);
    // Only the terminal outcome used to log, so a queue that was retrying
    // itself in a circle looked identical to one that was healthy. Debug,
    // because this is one line per message: a Console writer drops it and a
    // structured logger's own level decides.
    if (log.debugEnabled && action !== 'dead') {
      log.debug({ event: `broker.${action}`, queue: policy.queue, method, code, attempt: delivery.attempt, delay });
    }
    if (action === 'ack') return delivery.ack();
    if (action === 'retry') return delivery.retry({ delay });
    if (action === 'release') return delivery.release();
    // One line, bounded: a header value on every broker (a validator's
    // multi-line message used to make NATS refuse the dead-letter publish,
    // and the message came back forever).
    const reason = reasonText(`${code}${error?.message ? ` ${error.message}` : ''}`);
    // `err` alongside the code: the code says a message was dead-lettered,
    // the error says why, and only the code was ever recorded.
    log.warn({
      event: 'broker.dead',
      queue: policy.queue,
      method,
      code,
      attempt: delivery.attempt,
      id: delivery.id,
      err: error,
    });
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
    let { body, headers } = delivery;
    if (sealing !== null) {
      const opened = sealing.open(policy.queue, delivery);
      if (opened.refused !== undefined) {
        rpc.otel.recordBrokerRefusal(system, opened.refused);
        log.warn({
          event: 'broker.refused',
          queue: policy.queue,
          reason: opened.refused,
          id: delivery.id,
          attempt: delivery.attempt,
        });
        // A key id this service does not hold is what a rotation in progress
        // looks like — a fleet where this instance has not received the new
        // key yet, or dropped the old one a moment early — so it is retried
        // like a 503, to the binding's attempts, before it dead-letters.
        // Everything else (plaintext where none is accepted, a body that
        // does not open, a malformed one) will not change on a retry: dead
        // at once, with the reason in the log only.
        if (opened.refused === 'kid') {
          const decision = decide({
            code: 503,
            attempt: delivery.attempt,
            retry: policy.retry,
            draining: rpc.draining,
          });
          return void (await settle(delivery, decision, 503, new Error('Sealed delivery refused: unknown key id')));
        }
        return void (await settle(delivery, { action: 'dead', delay: 0 }, 400, new Error('Sealed delivery refused')));
      }
      if (opened.sealed) {
        body = toText(opened.body);
        headers = opened.headers;
      } else if (rpc.encryptionRequired === true) {
        // acceptPlaintext let it through the opener; the server's rule is
        // stricter than the rollout's, and a plaintext delivery is not
        // served — parity with `unsealed`.
        log.warn({ event: 'broker.refused', queue: policy.queue, reason: 'plaintext' });
        return void (await settle(
          delivery,
          { action: 'dead', delay: 0 },
          400,
          new Error('Plaintext delivery refused'),
        ));
      }
    }
    let args;
    try {
      args = policy.args(body, headers, delivery);
    } catch (error) {
      return void (await settle(delivery, { action: 'dead', delay: 0 }, 400, error));
    }
    // attach() may refuse (a transport the server will not serve, a bad
    // identity): a bounded retry, then dead — never a delivery that settles
    // nowhere.
    let attached;
    try {
      attached = clientFor(headers);
    } catch (error) {
      log.error({ err: error, event: 'broker.attach', queue: policy.queue });
      const decision = decide({ code: 500, attempt: delivery.attempt, retry: policy.retry, draining: rpc.draining });
      return void (await settle(delivery, decision, 500, error));
    }
    const { client, transport } = attached;
    const id = client.generateId();
    const meta = { messageId: delivery.id, attempt: delivery.attempt, queue: policy.queue };
    for (const name of policy.meta) {
      const value = headers[name];
      if (value !== undefined) meta[name] = value;
    }
    const packet = { type: 'call', id, method, args, meta };
    if (typeof headers.tp === 'string') packet[TRACEPARENT] = headers.tp;
    if (typeof headers.ts === 'string') packet[TRACESTATE] = headers.ts;
    const outcome = transport.expect(id);
    handleRpc(client, packet, view).catch((error) => {
      // The dispatcher itself threw, not the handler: settled as a 500 so
      // the delivery is not stuck, but the throw was going nowhere. The
      // settlement below only sees a code.
      log.error({ err: error, event: 'broker.dispatch', queue: policy.queue, method, id: delivery.id });
      transport.settle(id, { code: 500, error });
    });
    const { code, error, closed } = await outcome;
    try {
      const decision = closed
        ? { action: 'release', delay: 0 }
        : decide({ code, attempt: delivery.attempt, retry: policy.retry, draining: rpc.draining });
      await settle(delivery, decision, code, error);
    } finally {
      // Retired while this delivery was in flight (evicted from the cache):
      // closed once its last call has settled — here, after settle(), never
      // inside send()/settle(), where close() would destroy the Client
      // re-entrantly under the dispatcher that is still answering.
      if (attached.retired && transport.pending === 0) transport.close();
    }
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
    forget,
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
