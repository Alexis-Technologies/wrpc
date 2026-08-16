'use strict';

const { Semaphore } = require('../utils.js');
const { isTracked, tracked } = require('./subscriptions.js');

const DEFAULT_VERSION = '*';

const CALL = 'call';
const SUBSCRIPTION = 'subscription';

const AsyncGeneratorFunction = Object.getPrototypeOf(async function* () {}).constructor;

// An async generator handler IS the subscription declaration: writing
// `procedure.subscription({...})` around one is allowed but never required.
const isGeneratorHandler = (handler) => handler instanceof AsyncGeneratorFunction;

// Dispatcher maps `error.code` (number) onto the wire error code:
// 400 invalid input, 408 timeout, 500 invalid output, 503 queue overflow.
// `expose` marks the message as protocol surface: these strings are written
// for the caller, so the transport sends them verbatim even on a 5xx, where
// an ordinary error's message is replaced by the status line.
const codedError = (message, code) => {
  const error = new Error(message);
  error.code = code;
  error.expose = true;
  return error;
};

// A validator is either a plain function `(value) => value | throws`
// (returning undefined keeps the original value) or a Standard Schema
// (https://standardschema.dev) object carrying `~standard`.
const runValidator = async (validator, value) => {
  if (typeof validator === 'function') {
    const result = await validator(value);
    return result === undefined ? value : result;
  }
  const standard = validator['~standard'];
  const result = await standard.validate(value);
  if (result.issues) {
    const message = result.issues.map((issue) => issue.message).join('; ');
    throw new Error(message);
  }
  return result.value;
};

const isValidator = (value) =>
  typeof value === 'function' || (typeof value === 'object' && value !== null && '~standard' in value);

// ---------------------------------------------------------------------------
// Hooks: named lifecycle phases, fastify-style. No `next` — a hook runs, and
// either returns (letting the pipeline continue) or throws a coded error
// (ending the call with that code). "After" is a later phase, not code after
// a next() call, which is what keeps every hook a plain awaited function and
// the empty case a skipped `if`.
//
// Registration is three-level — defineRouter(units, { hooks }), a unit's
// reserved `hooks` key, and procedure({ preHandler }) — and the levels are
// FLATTENED ONCE per procedure when the router is built: dispatch walks a
// frozen array, never a chain of closures.

// Phases that run around one invocation (call, subscribe, inbound event).
const INVOCATION_PHASES = [
  'onRequest', // packet accepted, before session restore and access
  'preValidation', // after access, before input validation
  'preHandler', // after input validation, before the handler
  'preSerialization', // after the handler, before output validation
  'onSend', // before the callback packet is written; may mutate it
  'onResponse', // after the write; observational
  'onError', // any failure; observational
  'onTimeout', // a 408 specifically; observational, fires before onError
  'onSubscribe', // after access, before a subscription starts
  'onUnsubscribe', // after a subscription ended, whatever ended it
];
// Phases that run around a connection's lifetime (router-level only).
const CONNECTION_PHASES = ['onConnect', 'onDisconnect'];
const ROUTER_PHASES = [...INVOCATION_PHASES, ...CONNECTION_PHASES];
// The subset a single procedure can carry in its own options.
const PROCEDURE_PHASES = ['preValidation', 'preHandler', 'preSerialization', 'onError'];

const toHookList = (value, phase, label) => {
  if (value === undefined || value === null) return [];
  const list = Array.isArray(value) ? value : [value];
  for (const fn of list) {
    if (typeof fn !== 'function') {
      throw new TypeError(`${label}: hooks.${phase} must be a function or an array of functions`);
    }
  }
  return list;
};

// Unknown phase names throw: a typo'd hook that silently never runs is an
// authorization check that silently never runs.
const normalizeHooks = (hooks, allowed, label) => {
  if (hooks === undefined || hooks === null) hooks = {};
  if (typeof hooks !== 'object') throw new TypeError(`${label}: hooks must be an object of phase handlers`);
  for (const key of Object.keys(hooks)) {
    if (!allowed.includes(key)) throw new TypeError(`${label}: unknown hook phase '${key}'`);
  }
  const result = {};
  for (const phase of allowed) result[phase] = toHookList(hooks[phase], phase, label);
  return result;
};

const EMPTY_LIST = Object.freeze([]);
const EMPTY_HOOKS = Object.freeze(Object.fromEntries(INVOCATION_PHASES.map((phase) => [phase, EMPTY_LIST])));

/** Runs one phase in order; a throw ends the call with the error's code. */
const runHooks = async (hooks, context, payload) => {
  for (const hook of hooks) await hook(context, payload);
};

// The observational phases must never break what they observe: a throwing
// onResponse/onError/onDisconnect is reported to the log and contained.
const runHooksSafe = async (hooks, context, payload, log, phase) => {
  for (const hook of hooks) {
    try {
      await hook(context, payload);
    } catch (error) {
      log?.warn?.({ event: 'hook.error', phase, err: error }, `HOOK\t${phase}\t${error?.stack ?? error}`);
    }
  }
};

// Unit and method names come from user definitions: a '__proto__' key must
// become an own property instead of mutating the result's prototype, which
// a plain assignment would do.
const assignKey = (target, key, value) => {
  Object.defineProperty(target, key, { value, enumerable: true, writable: true, configurable: true });
};

const timeoutRace = (promise, ms) => {
  let timer = null;
  const failure = new Promise((resolve, reject) => {
    timer = setTimeout(() => {
      reject(codedError('Procedure timeout', 408));
    }, ms);
  });
  return Promise.race([promise, failure]).finally(() => clearTimeout(timer));
};

class Procedure {
  constructor(options = {}) {
    const {
      handler,
      access = 'session',
      input = null,
      output = null,
      timeout = 0,
      queue = null,
      meta = {},
      signature = null,
      kind = null,
      preValidation = null,
      preHandler = null,
      preSerialization = null,
      onError = null,
    } = options;
    if (typeof handler !== 'function') {
      throw new TypeError('procedure() requires a handler function');
    }
    // Anything but the two known levels used to silently mean 'session' —
    // an access model that LOOKS custom but is not is an auth bug waiting.
    // Custom policies are hooks' job (preValidation reads proc.meta).
    if (access !== 'public' && access !== 'session') {
      throw new TypeError(`procedure() access must be 'public' or 'session', got '${access}'`);
    }
    if (input !== null && !isValidator(input)) {
      throw new TypeError('procedure() input must be a function or a Standard Schema');
    }
    if (output !== null && !isValidator(output)) {
      throw new TypeError('procedure() output must be a function or a Standard Schema');
    }
    this.kind = kind ?? (isGeneratorHandler(handler) ? SUBSCRIPTION : CALL);
    if (this.kind !== CALL && this.kind !== SUBSCRIPTION) {
      throw new TypeError(`procedure() kind must be '${CALL}' or '${SUBSCRIPTION}'`);
    }
    // The procedure's own slice of the pipeline; router and unit levels are
    // merged in front of these when the router is built.
    this.hooks = normalizeHooks(
      { preValidation, preHandler, preSerialization, onError },
      PROCEDURE_PHASES,
      'procedure()',
    );
    this.handler = handler;
    this.access = access;
    this.input = input;
    this.output = output;
    this.timeout = timeout;
    this.meta = meta;
    this.signature = signature;
    if (queue && !(Number.isInteger(queue.concurrency) && queue.concurrency > 0)) {
      throw new TypeError('procedure() queue.concurrency must be a positive integer');
    }
    // A subscription lives until it is cancelled, so both of these would
    // mean something different from what they mean for a call — and quietly
    // meaning something else is worse than refusing.
    if (this.kind === SUBSCRIPTION && (queue || timeout)) {
      throw new TypeError('procedure.subscription() does not support queue or timeout');
    }
    this.semaphore = queue ? new Semaphore(queue) : null;
  }

  get subscription() {
    return this.kind === SUBSCRIPTION;
  }

  /**
   * The value stream behind `{type:'subscribe'}`. Input validation and the
   * per-value output validation happen here, so a subscription gets the same
   * guarantees a call does; `lastEventId` and `signal` reach the handler as
   * its third argument.
   */
  async *subscribe(context, args, options = {}, hooks = EMPTY_HOOKS) {
    if (this.kind !== SUBSCRIPTION) {
      throw codedError('Not a subscription', 400);
    }
    if (hooks.preValidation.length > 0) await runHooks(hooks.preValidation, context, args);
    let input = args;
    if (this.input) {
      input = await runValidator(this.input, args).catch((error) => {
        throw codedError(`Invalid arguments: ${error.message}`, 400);
      });
    }
    if (hooks.preHandler.length > 0) await runHooks(hooks.preHandler, context, input);
    const source = this.handler(context, input, options);
    if (!source || typeof source[Symbol.asyncIterator] !== 'function') {
      throw codedError('Subscription handler must return an async iterable', 500);
    }
    for await (const value of source) {
      if (!this.output) {
        yield value;
        continue;
      }
      // Validate the payload, not the tracking wrapper: an output schema
      // describes what the client receives, not how it is labelled.
      const payload = isTracked(value) ? value.data : value;
      const checked = await runValidator(this.output, payload).catch((error) => {
        throw codedError(`Invalid subscription value: ${error.message}`, 500);
      });
      yield isTracked(value) ? tracked(value.id, checked) : checked;
    }
  }

  async invoke(context, args, hooks = EMPTY_HOOKS) {
    if (this.kind === SUBSCRIPTION) {
      throw codedError('This procedure is a subscription: use {type:"subscribe"}', 400);
    }
    // The procedure's deadline starts NOW, queue wait included: a caller
    // that waited 900 ms of a 1000 ms timeout in the queue has 100 ms of
    // handler budget left, not a fresh 1000 — overload must not do work for
    // callers that already gave up.
    const deadline = this.timeout > 0 ? Date.now() + this.timeout : 0;
    if (this.semaphore) {
      try {
        // Abort-aware: a queued waiter whose caller cancelled or
        // disconnected leaves the queue instead of taking a slot later.
        await this.semaphore.enter(context.signal ?? null);
      } catch (error) {
        throw codedError(error.message, 503);
      }
    }
    if (deadline > 0 && Date.now() >= deadline) {
      throw codedError('Procedure timeout', 408);
    }
    let handlerStarted = false;
    try {
      if (hooks.preValidation.length > 0) await runHooks(hooks.preValidation, context, args);
      if (this.input) {
        args = await runValidator(this.input, args).catch((error) => {
          throw codedError(`Invalid arguments: ${error.message}`, 400);
        });
      }
      if (hooks.preHandler.length > 0) await runHooks(hooks.preHandler, context, args);
      handlerStarted = true;
      const invocation = Promise.resolve().then(() => this.handler(context, args));
      if (this.semaphore) {
        // The queue slot is held until the HANDLER settles: a timeout
        // rejects the caller but cannot cancel the handler, and freeing
        // the slot early would break the concurrency guarantee.
        const release = () => this.semaphore.leave();
        invocation.then(release, release);
      }
      let result = deadline > 0 ? await timeoutRace(invocation, Math.max(1, deadline - Date.now())) : await invocation;
      // A preSerialization hook that returns something replaces the result;
      // one that returns undefined leaves it alone. Runs BEFORE the output
      // validator, so what the hook shaped is what the schema checks.
      for (const hook of hooks.preSerialization) {
        const replaced = await hook(context, result);
        if (replaced !== undefined) result = replaced;
      }
      if (this.output) {
        result = await runValidator(this.output, result).catch((error) => {
          throw codedError(`Invalid procedure result: ${error.message}`, 500);
        });
      }
      return result;
    } finally {
      // The handler never ran (validation failed) — free the slot here
      if (this.semaphore && !handlerStarted) this.semaphore.leave();
    }
  }
}

const procedure = (options) => {
  if (typeof options === 'function') return new Procedure({ handler: options });
  return new Procedure(options);
};

// Explicit spelling for a subscription. Redundant when the handler is an
// async generator (which is detected), required when it is a plain function
// returning an async iterable.
procedure.subscription = (options) => {
  if (typeof options === 'function') return new Procedure({ handler: options, kind: SUBSCRIPTION });
  return new Procedure({ ...options, kind: SUBSCRIPTION });
};

const toProcedure = (value, unitKey, methodName) => {
  if (value instanceof Procedure) return value;
  if (typeof value === 'function') return new Procedure({ handler: value });
  if (typeof value === 'object' && value !== null && typeof value.handler === 'function') {
    return new Procedure(value);
  }
  throw new TypeError(
    `Router definition ${unitKey}/${methodName} must be a procedure, a handler function, or an options object`,
  );
};

// `on` is reserved inside a unit definition: it declares the unit's inbound
// (client -> server) event handlers rather than a method named 'on'.
// `hooks` is reserved too: the unit's slice of the lifecycle pipeline.
const EVENTS_KEY = 'on';
const HOOKS_KEY = 'hooks';

// unit-level hooks may not carry the connection phases: a connection is not
// scoped to a unit, so an onConnect there could never mean anything.
const concatHooks = (base, extra, phases) => {
  const result = {};
  for (const phase of phases) result[phase] = [...(base?.[phase] ?? []), ...(extra?.[phase] ?? [])];
  return result;
};

class Router {
  // unit -> Map(version -> { methods: Map(name -> Procedure),
  //                          events:  Map(name -> Procedure),
  //                          hooks:   { phase: [fns] } })
  #units = new Map();
  #hooks;
  // Procedure -> frozen { phase: frozen [fns] }, the flattened pipeline the
  // dispatcher walks. Kept on the ROUTER, not the procedure: the same
  // Procedure instance may be registered in several routers (merge reuses
  // them), each with different router-level hooks.
  #chains = new Map();

  constructor(definition = {}, options = {}) {
    this.#hooks = normalizeHooks(options.hooks, ROUTER_PHASES, 'defineRouter');
    for (const [unitKey, methods] of Object.entries(definition)) {
      this.#addUnit(unitKey, methods);
    }
    this.#rebuildChains();
  }

  /** Adds a router-level hook after construction. Returns the router. */
  addHook(name, fn) {
    if (!ROUTER_PHASES.includes(name)) {
      throw new TypeError(`addHook: unknown hook phase '${name}'`);
    }
    if (typeof fn !== 'function') throw new TypeError('addHook: the hook must be a function');
    this.#hooks[name] = [...this.#hooks[name], fn];
    this.#rebuildChains();
    return this;
  }

  /** The flattened pipeline for one procedure (router + unit + procedure). */
  hooksFor(proc) {
    return this.#chains.get(proc) ?? EMPTY_HOOKS;
  }

  /** Router-level connection lifecycle hooks, consumed by RpcServer. */
  get connectionHooks() {
    return { onConnect: this.#hooks.onConnect, onDisconnect: this.#hooks.onDisconnect };
  }

  #rebuildChains() {
    this.#chains = new Map();
    for (const versions of this.#units.values()) {
      for (const entry of versions.values()) {
        for (const proc of entry.methods.values()) this.#chains.set(proc, this.#chainFor(entry, proc));
        for (const proc of entry.events.values()) this.#chains.set(proc, this.#chainFor(entry, proc));
      }
    }
  }

  #chainFor(entry, proc) {
    const chain = {};
    for (const phase of INVOCATION_PHASES) {
      const merged = [...this.#hooks[phase], ...(entry.hooks?.[phase] ?? []), ...(proc.hooks[phase] ?? [])];
      chain[phase] = merged.length > 0 ? Object.freeze(merged) : EMPTY_LIST;
    }
    return Object.freeze(chain);
  }

  #addUnit(unitKey, definition) {
    const [unit, version = DEFAULT_VERSION, ...extra] = unitKey.split('.');
    // A silent split would truncate 'unit.1.2' into unit.1 and merge
    // colliding registrations — reject anything but 'unit' / 'unit.ver'
    if (!unit || version === '' || extra.length > 0 || typeof definition !== 'object' || definition === null) {
      throw new TypeError(`Invalid router unit definition: ${unitKey}`);
    }
    let versions = this.#units.get(unit);
    if (!versions) {
      versions = new Map();
      this.#units.set(unit, versions);
    }
    let entry = versions.get(version);
    if (!entry) {
      entry = { methods: new Map(), events: new Map(), hooks: null };
      versions.set(version, entry);
    }
    for (const [name, value] of Object.entries(definition)) {
      if (name === EVENTS_KEY) {
        this.#addEvents(unitKey, entry.events, value);
        continue;
      }
      if (name === HOOKS_KEY) {
        const unitHooks = normalizeHooks(value, INVOCATION_PHASES, `${unitKey}.hooks`);
        entry.hooks = entry.hooks ? concatHooks(entry.hooks, unitHooks, INVOCATION_PHASES) : unitHooks;
        continue;
      }
      entry.methods.set(name, toProcedure(value, unitKey, name));
    }
  }

  // Event handlers reuse Procedure: an inbound event is a call that never
  // answers, so access control, input validation and queueing come for free.
  #addEvents(unitKey, events, definition) {
    if (typeof definition !== 'object' || definition === null) {
      throw new TypeError(`Router definition ${unitKey}.on must be an object of event handlers`);
    }
    for (const [name, value] of Object.entries(definition)) {
      events.set(name, toProcedure(value, unitKey, `on.${name}`));
    }
  }

  getProcedure(unit, version = DEFAULT_VERSION, method) {
    const entry = this.#units.get(unit)?.get(version);
    return entry?.methods.get(method) ?? null;
  }

  getEventHandler(unit, version = DEFAULT_VERSION, name) {
    const entry = this.#units.get(unit)?.get(version);
    return entry?.events.get(name) ?? null;
  }

  // Introspection v2: { unitKey: { method: { access, meta?, signature? } } }
  // where unitKey is 'unit' for the default version and 'unit.ver' otherwise.
  // Anything but an array of unit keys means "no filter" — REST calls
  // deliver plain-object args here.
  introspect(units = null) {
    const filter = Array.isArray(units) ? units : null;
    const result = {};
    for (const [unit, versions] of this.#units) {
      for (const [version, entry] of versions) {
        const unitKey = version === DEFAULT_VERSION ? unit : `${unit}.${version}`;
        if (filter && !filter.includes(unitKey)) continue;
        const methodsInfo = {};
        for (const [methodName, proc] of entry.methods) {
          const info = { access: proc.access };
          // Only subscriptions carry `kind`: a client scaffolds a call
          // unless told otherwise, so the common case stays one key.
          if (proc.subscription) info.kind = proc.kind;
          if (Object.keys(proc.meta).length > 0) info.meta = proc.meta;
          if (proc.signature) info.signature = proc.signature;
          assignKey(methodsInfo, methodName, info);
        }
        assignKey(result, unitKey, methodsInfo);
      }
    }
    return result;
  }

  // Returns a NEW router; on collision the other router's procedure wins.
  // Hooks travel too: router-level hooks concatenate (this first), unit
  // hooks ride with their unit, and the chains are rebuilt for the merged
  // set — the shared Procedure instances themselves are never mutated.
  merge(other) {
    const hooks = concatHooks(this.#hooks, other.#hooks, ROUTER_PHASES);
    const merged = new Router({}, { hooks });
    for (const source of [this, other]) {
      for (const [unit, versions] of source.#units) {
        for (const [version, entry] of versions) {
          const unitKey = version === DEFAULT_VERSION ? unit : `${unit}.${version}`;
          const definition = Object.fromEntries(entry.methods);
          if (entry.events.size > 0) definition[EVENTS_KEY] = Object.fromEntries(entry.events);
          if (entry.hooks) definition[HOOKS_KEY] = entry.hooks;
          merged.#addUnit(unitKey, definition);
        }
      }
    }
    merged.#rebuildChains();
    return merged;
  }
}

const defineRouter = (definition, options) => new Router(definition, options);

module.exports = {
  Procedure,
  Router,
  procedure,
  defineRouter,
  runHooks,
  runHooksSafe,
  EMPTY_HOOKS,
  INVOCATION_PHASES,
  CONNECTION_PHASES,
};
