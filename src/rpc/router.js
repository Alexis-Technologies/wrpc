'use strict';

const { Semaphore } = require('../utils.js');
const { isTracked, tracked } = require('./subscriptions.js');
const {
  unitVersions,
  normalizeHttp,
  normalizeRestOptions,
  effectiveHttp,
  buildRestTrees,
  matchRestTrees,
  collectRestRoutes,
  publicHttp,
} = require('./rest.js');
const { ConfigError, WrpcError } = require('../errors.js');

const DEFAULT_VERSION = '*';
// The version token of a unit key: 'auth.v1' -> 'v1'. Closed on purpose —
// the token rides in wire targets and (with rest.version) URL paths, so it
// must stay a safe path segment.
const VERSION_TOKEN = /^v\d+$/;

const CALL = 'call';
const SUBSCRIPTION = 'subscription';

const AsyncGeneratorFunction = Object.getPrototypeOf(async function* () {}).constructor;

// An async generator handler IS the subscription declaration: writing
// `procedure.subscription({...})` around one is allowed but never required.
const isGeneratorHandler = (handler) => handler instanceof AsyncGeneratorFunction;

// The router's own refusals are WrpcErrors the dispatcher maps by `code`:
// 400 invalid input, 408 timeout, 500 invalid output, 503 queue overflow.
// `expose` marks the message as protocol surface: these strings are written
// for the caller, so the transport sends them verbatim even on a 5xx, where
// an ordinary error's message is replaced by the status line.

// The issues a validator found, as the error the router wraps — in a 400
// for arguments, a 500 for a result: the joined messages its text, the
// issues, with their paths, its details.
const issuesError = (messages, issues) => new WrpcError(messages.join('; '), { code: 400, details: { issues } });

// A validator is either a plain function `(value) => value | throws`
// (returning undefined keeps the original value) or a Standard Schema
// (https://standardschema.dev) object carrying `~standard`. `context` is
// forwarded so a compiled input validator can reach what is NOT in args —
// the connection headers a schema.headers part checks.
// Synchronous when the validator is — which the injected-ajv compiled fast
// path always is: forcing it through an async wrapper cost two promise
// allocations and microtask hops per validated call AND per yielded
// subscription value (bench/validate.js). A function validator returning a
// thenable (an async custom validator) still settles through it; the
// Standard Schema branch stays async by its own contract. Callers therefore
// use try/catch (a sync validator THROWS synchronously now) and only await
// a result that is actually a thenable.
const runValidator = (validator, value, context = null) => {
  if (typeof validator === 'function') {
    const result = validator(value, context);
    if (result && typeof result.then === 'function') {
      return result.then((settled) => (settled === undefined ? value : settled));
    }
    return result === undefined ? value : result;
  }
  return runStandardSchema(validator, value);
};

const runStandardSchema = async (validator, value) => {
  const standard = validator['~standard'];
  const result = await standard.validate(value);
  if (result.issues) {
    // The joined message stays the human line; the issues keep their paths
    // as structured `details`, which the transport forwards on 4xx (or on
    // expose) as the wire error's optional `details` field.
    const issues = [];
    const messages = [];
    for (let i = 0; i < result.issues.length; i++) {
      const issue = result.issues[i];
      issues.push({ message: issue.message, path: issue.path });
      messages.push(issue.message);
    }
    throw issuesError(messages, issues);
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
  for (let i = 0; i < list.length; i++) {
    if (typeof list[i] !== 'function') {
      throw new ConfigError(`${label}: hooks.${phase} must be a function or an array of functions`);
    }
  }
  return list;
};

// Unknown phase names throw: a typo'd hook that silently never runs is an
// authorization check that silently never runs.
const normalizeHooks = (hooks, allowed, label) => {
  if (hooks === undefined || hooks === null) hooks = {};
  if (typeof hooks !== 'object') throw new ConfigError(`${label}: hooks must be an object of phase handlers`);
  const keys = Object.keys(hooks);
  for (let i = 0; i < keys.length; i++) {
    if (!allowed.includes(keys[i])) throw new ConfigError(`${label}: unknown hook phase '${keys[i]}'`);
  }
  const result = {};
  for (let i = 0; i < allowed.length; i++) result[allowed[i]] = toHookList(hooks[allowed[i]], allowed[i], label);
  return result;
};

const EMPTY_LIST = Object.freeze([]);
const EMPTY_HOOKS = {};
for (let i = 0; i < INVOCATION_PHASES.length; i++) EMPTY_HOOKS[INVOCATION_PHASES[i]] = EMPTY_LIST;
Object.freeze(EMPTY_HOOKS);

/** Runs one phase in order; a throw ends the call with the error's code. */
const runHooks = async (hooks, context, payload) => {
  for (let i = 0; i < hooks.length; i++) {
    const hook = hooks[i];
    await hook(context, payload);
  }
};

// The observational phases must never break what they observe: a throwing
// onResponse/onError/onDisconnect is reported to the log and contained.
const runHooksSafe = async (hooks, context, payload, log, phase) => {
  for (let i = 0; i < hooks.length; i++) {
    const hook = hooks[i];
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

// ---------------------------------------------------------------------------
// Declarative REST mapping and the fastify-shaped schema object.
//
// `http: { method, path, status? }` maps a procedure onto a real HTTP
// endpoint under the server's basePath; args then arrive structured as
// { params, query, body }, over EVERY transport — the mapping only defines
// how an HTTP request is unpacked, a ws caller passes the same shape itself.
//
// `schema` follows fastify.route.schema: `params`, `querystring` (or its
// alias `query` — interchangeable, normalized here), `body`, `headers`,
// `response` keyed by status code, plus any passthrough keys (tags,
// summary, security, ...) that wrpc never interprets but forwards to hosts
// (fastify/swagger) verbatim.

// The parts wrpc itself understands; everything else on `schema` passes
// through untouched. `query` and `querystring` are interchangeable spellings
// of the same part — normalized to `querystring` (fastify's canon).
const SCHEMA_PARTS = ['params', 'querystring', 'body', 'headers', 'response'];

const normalizeSchema = (schema, label) => {
  if (schema === null || schema === undefined) return null;
  if (typeof schema !== 'object' || Array.isArray(schema)) {
    throw new ConfigError(`${label} schema must be an object (fastify route schema shape)`);
  }
  if (schema.query !== undefined && schema.querystring !== undefined && schema.query !== schema.querystring) {
    throw new ConfigError(`${label} schema has both 'query' and 'querystring' and they differ — use one`);
  }
  const normalized = {};
  const keys = Object.keys(schema);
  for (let i = 0; i < keys.length; i++) {
    if (keys[i] === 'query') continue; // folded into querystring below
    assignKey(normalized, keys[i], schema[keys[i]]);
  }
  if (schema.query !== undefined && schema.querystring === undefined) normalized.querystring = schema.query;
  for (let i = 0; i < SCHEMA_PARTS.length; i++) {
    const value = normalized[SCHEMA_PARTS[i]];
    if (value !== undefined && (typeof value !== 'object' || value === null)) {
      throw new ConfigError(`${label} schema.${SCHEMA_PARTS[i]} must be an object`);
    }
  }
  return normalized;
};

const timeoutRace = (promise, ms) => {
  let timer = null;
  const failure = new Promise((resolve, reject) => {
    timer = setTimeout(() => {
      reject(new WrpcError('Procedure timeout', { code: 408, expose: true }));
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
      http = null,
      schema = null,
      preValidation = null,
      preHandler = null,
      preSerialization = null,
      onError = null,
      consume = null,
    } = options;
    if (typeof handler !== 'function') {
      throw new ConfigError('procedure() requires a handler function');
    }
    // Anything but the two known levels used to silently mean 'session' —
    // an access model that LOOKS custom but is not is an auth bug waiting.
    // Custom policies are hooks' job (preValidation reads proc.meta).
    if (access !== 'public' && access !== 'session') {
      throw new ConfigError(`procedure() access must be 'public' or 'session', got '${access}'`);
    }
    if (input !== null && !isValidator(input)) {
      throw new ConfigError('procedure() input must be a function or a Standard Schema');
    }
    if (output !== null && !isValidator(output)) {
      throw new ConfigError('procedure() output must be a function or a Standard Schema');
    }
    this.kind = kind ?? (isGeneratorHandler(handler) ? SUBSCRIPTION : CALL);
    if (this.kind !== CALL && this.kind !== SUBSCRIPTION) {
      throw new ConfigError(`procedure() kind must be '${CALL}' or '${SUBSCRIPTION}'`);
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
    // Milliseconds a timer can hold, or 0 for none: anything else became a
    // deadline of NaN (never) or of one millisecond with a warning per call.
    if (!(typeof timeout === 'number' && timeout >= 0 && timeout < 2 ** 31)) {
      throw new ConfigError('procedure: timeout must be a number of milliseconds from 0 to 2^31 - 1');
    }
    this.timeout = timeout;
    this.meta = meta;
    this.signature = signature;
    this.http = normalizeHttp(http);
    this.schema = normalizeSchema(schema, 'procedure()');
    // One declarative source of truth per direction: a schema beside a
    // programmatic validator would leave "which one ran?" ambiguous.
    if (this.schema && (input || output)) {
      throw new ConfigError('procedure() schema and input/output are mutually exclusive');
    }
    if (queue && !(Number.isInteger(queue.concurrency) && queue.concurrency > 0)) {
      throw new ConfigError('procedure() queue.concurrency must be a positive integer');
    }
    // A subscription lives until it is cancelled, so both of these would
    // mean something different from what they mean for a call — and quietly
    // meaning something else is worse than refusing.
    if (this.kind === SUBSCRIPTION && (queue || timeout)) {
      throw new ConfigError('procedure.subscription() does not support queue or timeout');
    }
    // A subscription is already refused on plain HTTP (400), so an HTTP
    // route mapping could never mean anything for one.
    if (this.kind === SUBSCRIPTION && this.http) {
      throw new ConfigError('procedure.subscription() does not support http');
    }
    this.semaphore = queue ? new Semaphore(queue) : null;
    // A queue-consumer procedure's delivery policy (see the unit's reserved
    // `consumes` key). Plain data, shape-checked here; the broker binding
    // owns what the values mean.
    if (consume !== null && (typeof consume !== 'object' || Array.isArray(consume))) {
      throw new ConfigError('procedure() consume must be an object');
    }
    if (consume && (this.kind === SUBSCRIPTION || this.http)) {
      throw new ConfigError('procedure() consume cannot be combined with a subscription or an http mapping');
    }
    this.consume = consume ? Object.freeze({ ...consume }) : null;
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
  async *subscribe(context, args, options = {}, hooks = EMPTY_HOOKS, compiled = null) {
    if (this.kind !== SUBSCRIPTION) {
      throw new WrpcError('Not a subscription', { code: 400, expose: true });
    }
    const inputValidator = compiled?.input ?? this.input;
    const outputValidator = compiled?.output ?? this.output;
    if (hooks.preValidation.length > 0) await runHooks(hooks.preValidation, context, args);
    let input = args;
    if (inputValidator) {
      try {
        const checked = runValidator(inputValidator, args, context);
        input = checked && typeof checked.then === 'function' ? await checked : checked;
      } catch (error) {
        throw new WrpcError(`Invalid arguments: ${error.message}`, {
          code: 400,
          details: error.details,
          expose: true,
          cause: error,
        });
      }
    }
    if (hooks.preHandler.length > 0) await runHooks(hooks.preHandler, context, input);
    const source = this.handler(context, input, options);
    if (!source || typeof source[Symbol.asyncIterator] !== 'function') {
      throw new WrpcError('Subscription handler must return an async iterable', { code: 500, expose: true });
    }
    for await (const value of source) {
      if (!outputValidator) {
        yield value;
        continue;
      }
      // Validate the payload, not the tracking wrapper: an output schema
      // describes what the client receives, not how it is labelled.
      const payload = isTracked(value) ? value.data : value;
      let checked = null;
      try {
        checked = runValidator(outputValidator, payload);
        if (checked && typeof checked.then === 'function') checked = await checked;
      } catch (error) {
        throw new WrpcError(`Invalid subscription value: ${error.message}`, {
          code: 500,
          details: error.details,
          expose: true,
          cause: error,
        });
      }
      yield isTracked(value) ? tracked(value.id, checked) : checked;
    }
  }

  // The host-delegated entry (the fastify adapter's native routes): the
  // semaphore and the deadline behave EXACTLY as in invoke() — the slot is
  // held until the handler settles — but hooks, access and validators are
  // the host's job, already run by the time this is called.
  // Queue wait, measured once and in one place. Without it the time a call
  // spends waiting for a slot is folded invisibly into `rpc.server.duration`,
  // so a saturated queue and a slow handler look identical — and they are
  // opposite problems with opposite fixes (raise the queue vs. fix the
  // handler). Both `invoke` and `invokeBare` take this path, so the two
  // cannot drift apart.
  async #acquire(context, deadline) {
    const otel = context.client?.otel;
    // Nothing listening: no clock reads, no counter traffic.
    const started = otel?.enabled ? Date.now() : 0;
    if (started > 0) otel.recordQueue(1);
    // The deadline bounds the wait too: a call queued behind a hung handler
    // used to wait for the slot whatever its timeout said, and only then
    // learn it was late.
    const waitMs = deadline > 0 ? Math.max(1, deadline - Date.now()) : 0;
    let entered = false;
    try {
      // Abort-aware: a queued waiter whose caller cancelled or disconnected
      // leaves the queue instead of taking a slot later.
      entered = await this.semaphore.enter(context.signal ?? null, waitMs);
    } catch (error) {
      if (started > 0) otel.recordQueue(-1);
      throw new WrpcError(error.message, { code: 503, expose: true, cause: error });
    }
    if (started > 0) otel.recordQueue(-1, Date.now() - started);
    if (!entered) throw new WrpcError('Procedure timeout', { code: 408, expose: true });
  }

  async invokeBare(context, args) {
    if (this.kind === SUBSCRIPTION) {
      throw new WrpcError('This procedure is a subscription: use {type:"subscribe"}', { code: 400, expose: true });
    }
    const deadline = this.timeout > 0 ? Date.now() + this.timeout : 0;
    if (this.semaphore) await this.#acquire(context, deadline);
    let handlerStarted = false;
    try {
      if (deadline > 0 && Date.now() >= deadline) {
        throw new WrpcError('Procedure timeout', { code: 408, expose: true });
      }
      handlerStarted = true;
      // Called directly rather than through Promise.resolve().then(): the
      // handler starts now instead of a microtask later, and a synchronous
      // throw becomes the rejection it would have been (bench/bench.js).
      let invocation = null;
      try {
        invocation = Promise.resolve(this.handler(context, args));
      } catch (error) {
        invocation = Promise.reject(error);
      }
      if (this.semaphore) {
        const release = () => this.semaphore.leave();
        invocation.then(release, release);
      }
      return deadline > 0 ? await timeoutRace(invocation, Math.max(1, deadline - Date.now())) : await invocation;
    } finally {
      if (this.semaphore && !handlerStarted) this.semaphore.leave();
    }
  }

  async invoke(context, args, hooks = EMPTY_HOOKS, compiled = null, budget = 0) {
    if (this.kind === SUBSCRIPTION) {
      throw new WrpcError('This procedure is a subscription: use {type:"subscribe"}', { code: 400, expose: true });
    }
    // The procedure's deadline starts NOW, queue wait included: a caller
    // that waited 900 ms of a 1000 ms timeout in the queue has 100 ms of
    // handler budget left, not a fresh 1000 — overload must not do work for
    // callers that already gave up.
    // `budget` is the CALLER's per-call deadline (the packet's optional
    // `timeout` field — CallOptions.timeout, the gRPC-deadline shape). It
    // can only SHORTEN the procedure's own limit, never widen it: the
    // procedure's timeout is the server's protection, the caller's budget
    // is a courtesy — no point doing work whose answer nobody awaits.
    const cap = this.timeout > 0 ? this.timeout : 0;
    const limit = budget > 0 && (cap === 0 || budget < cap) ? budget : cap;
    const deadline = limit > 0 ? Date.now() + limit : 0;
    if (this.semaphore) await this.#acquire(context, deadline);
    let handlerStarted = false;
    try {
      // Inside the try: a slot taken just as the deadline passed is given
      // back by the finally below — outside it, that slot was never freed.
      if (deadline > 0 && Date.now() >= deadline) {
        throw new WrpcError('Procedure timeout', { code: 408, expose: true });
      }
      const inputValidator = compiled?.input ?? this.input;
      const outputValidator = compiled?.output ?? this.output;
      if (hooks.preValidation.length > 0) await runHooks(hooks.preValidation, context, args);
      if (inputValidator) {
        try {
          const checked = runValidator(inputValidator, args, context);
          args = checked && typeof checked.then === 'function' ? await checked : checked;
        } catch (error) {
          throw new WrpcError(`Invalid arguments: ${error.message}`, {
            code: 400,
            details: error.details,
            expose: true,
            cause: error,
          });
        }
      }
      if (hooks.preHandler.length > 0) await runHooks(hooks.preHandler, context, args);
      handlerStarted = true;
      // Called directly rather than through Promise.resolve().then(): the
      // handler starts now instead of a microtask later, and a synchronous
      // throw becomes the rejection it would have been (bench/bench.js).
      let invocation = null;
      try {
        invocation = Promise.resolve(this.handler(context, args));
      } catch (error) {
        invocation = Promise.reject(error);
      }
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
      const preSerialization = hooks.preSerialization;
      for (let i = 0; i < preSerialization.length; i++) {
        const hook = preSerialization[i];
        const replaced = await hook(context, result);
        if (replaced !== undefined) result = replaced;
      }
      if (outputValidator) {
        try {
          const checked = runValidator(outputValidator, result);
          result = checked && typeof checked.then === 'function' ? await checked : checked;
        } catch (error) {
          throw new WrpcError(`Invalid procedure result: ${error.message}`, {
            code: 500,
            details: error.details,
            expose: true,
            cause: error,
          });
        }
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

// `consume` is a consumer's policy: on a callable method or an event handler
// it would silently do nothing, so it is refused where it cannot apply.
const rejectConsume = (proc, label) => {
  if (proc.consume) throw new ConfigError(`${label}: consume is only valid inside a unit's consumes block`);
  return proc;
};

const toProcedure = (value, unitKey, methodName) => {
  if (value instanceof Procedure) return value;
  if (typeof value === 'function') return new Procedure({ handler: value });
  if (typeof value === 'object' && value !== null && typeof value.handler === 'function') {
    return new Procedure(value);
  }
  throw new ConfigError(
    `Router definition ${unitKey}/${methodName} must be a procedure, a handler function, or an options object`,
  );
};

// `on` is reserved inside a unit definition: it declares the unit's inbound
// (client -> server) event handlers rather than a method named 'on'.
// `hooks` is reserved too: the unit's slice of the lifecycle pipeline.
const EVENTS_KEY = 'on';
const HOOKS_KEY = 'hooks';
// Declaration-only: the unit's OUTBOUND (server -> client) events, as
// `signature`-style descriptors ({ data, returns? }). No runtime behavior —
// the map travels through introspection so `wrpc types` can generate the
// contract's `events` key, the same way `signature` types calls.
const EMITS_KEY = 'emits';
// The unit's QUEUE CONSUMERS: `consumes: { 'orders.created': procedure(...) }`.
// Each is a full Procedure (hooks, validators, access, queue, timeout) that
// a broker binding invokes per delivered message — and that no peer can
// call: they live apart from `methods`, so a `call` packet cannot reach one.
const CONSUMES_KEY = 'consumes';

// unit-level hooks may not carry the connection phases: a connection is not
// scoped to a unit, so an onConnect there could never mean anything.
const concatHooks = (base, extra, phases) => {
  const result = {};
  for (let i = 0; i < phases.length; i++) {
    const phase = phases[i];
    result[phase] = [...(base?.[phase] ?? []), ...(extra?.[phase] ?? [])];
  }
  return result;
};

// ---------------------------------------------------------------------------
// Injected JSON Schema compilers. Structural, per the zero-dependency rule:
// `ajv` is anything with compile(schema) -> validateFn (ajv-shaped: the fn
// answers a boolean and exposes `.errors`), `serializer` anything with
// compile(schema) -> (value) -> string (fast-json-stringify-shaped). wrpc
// never imports either — the application passes its own.

const normalizeValidation = (validation) => {
  if (validation === null || validation === undefined) return null;
  if (typeof validation !== 'object') {
    throw new ConfigError('defineRouter: options.validation must be an object');
  }
  const { ajv = null, serializer = null } = validation;
  if (ajv !== null && typeof ajv.compile !== 'function') {
    throw new ConfigError('defineRouter: validation.ajv must provide compile(schema)');
  }
  if (serializer !== null && typeof serializer.compile !== 'function') {
    throw new ConfigError('defineRouter: validation.serializer must provide compile(schema)');
  }
  if (!ajv && !serializer) return null;
  return { ajv, serializer };
};

// The schema parts and the args keys they validate.
const INPUT_PARTS = [
  ['params', 'params'],
  ['querystring', 'query'],
  ['body', 'body'],
];

// An injected ajv's errors, as issues under one part's path.
const addIssues = (issues, errors, part) => {
  const list = errors ?? EMPTY_LIST;
  for (let i = 0; i < list.length; i++) {
    issues.push({ message: list[i].message, path: `/${part}${list[i].instancePath ?? ''}` });
  }
  return issues;
};

// Compiles the input parts into ONE function validator, fastify-style: each
// part validates its slice of `{ params, query, body }`, issues carry
// part-prefixed paths, and ajv's own coercions land in place. The wrapper is
// an ordinary function validator, so `runValidator` stays the single
// execution seam.
const compileInput = (ajv, schema, label) => {
  const parts = [];
  for (let i = 0; i < INPUT_PARTS.length; i++) {
    const part = INPUT_PARTS[i][0];
    const argsKey = INPUT_PARTS[i][1];
    if (schema[part] === undefined) continue;
    let validate = null;
    try {
      validate = ajv.compile(schema[part]);
    } catch (error) {
      throw new ConfigError(`${label}: schema.${part} failed to compile: ${error.message}`, { cause: error });
    }
    parts.push({ part, argsKey, validate });
  }
  // The asymmetric part: headers are not an args slice — they live on
  // context.meta.headers — so they get their own check instead of a row in
  // INPUT_PARTS, and the closure takes the context runValidator forwards.
  let headersValidate = null;
  if (schema.headers !== undefined) {
    try {
      headersValidate = ajv.compile(schema.headers);
    } catch (error) {
      throw new ConfigError(`${label}: schema.headers failed to compile: ${error.message}`, { cause: error });
    }
  }
  if (parts.length === 0 && !headersValidate) return null;
  return (args, context) => {
    const value = args && typeof args === 'object' ? args : {};
    let issues = null;
    for (let i = 0; i < parts.length; i++) {
      const { part, argsKey, validate } = parts[i];
      if (validate(value[argsKey] ?? {})) continue;
      issues = addIssues(issues ?? [], validate.errors, part);
    }
    if (headersValidate) {
      // A COPY on purpose: client.meta.headers is frozen, and an injected
      // ajv compiled with coerceTypes would otherwise throw writing the
      // coerced value back (compiled validators are strict-mode code). The
      // copy also keeps the snapshot honest — validation observes headers,
      // it never rewrites them.
      if (!headersValidate({ ...context?.meta?.headers })) {
        issues = addIssues(issues ?? [], headersValidate.errors, 'headers');
      }
    }
    if (issues) {
      const messages = [];
      for (let i = 0; i < issues.length; i++) messages.push(`${issues[i].path} ${issues[i].message}`);
      throw issuesError(messages, issues);
    }
    return value;
  };
};

// The success-response schema: the mapped status first, then any 2xx entry.
const successResponse = (proc) => {
  const response = proc.schema?.response;
  if (!response || typeof response !== 'object') return null;
  const preferred = proc.http?.status ?? 200;
  if (response[preferred] && response[preferred] !== false) return response[preferred];
  const keys = Object.keys(response);
  for (let i = 0; i < keys.length; i++) {
    const code = Number(keys[i]);
    if (code >= 200 && code < 300 && response[keys[i]] !== false) return response[keys[i]];
  }
  return null;
};

const compileOutput = (ajv, schema, label) => {
  let validate = null;
  try {
    validate = ajv.compile(schema);
  } catch (error) {
    throw new ConfigError(`${label}: schema.response failed to compile: ${error.message}`, { cause: error });
  }
  return (result) => {
    if (validate(result)) return result;
    const issues = [];
    const messages = [];
    const errors = validate.errors ?? EMPTY_LIST;
    for (let i = 0; i < errors.length; i++) {
      issues.push({ message: errors[i].message, path: errors[i].instancePath });
      messages.push(`${errors[i].instancePath} ${errors[i].message}`);
    }
    throw issuesError(messages, issues);
  };
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
  // The declarative REST table: a segment trie over every procedure's
  // `http` mapping. Per-router like #chains (merge reuses Procedures), and
  // rebuilt whenever the unit set changes. `null` until a mapping exists,
  // so routers without REST pay one null check.
  #rest = null;
  // Injected compilers (ajv-shaped, fast-json-stringify-shaped) and the
  // per-router artifacts they produce: Procedure -> { input?, output?,
  // serialize? }. Per-router like #chains — merge() shares Procedure
  // instances, and two routers may carry different compilers.
  #validation = null;
  #compiled = new Map();
  // The rest.version strategy; `#rest` is taken by the trie above.
  #restOptions = null;

  constructor(definition = {}, options = {}) {
    this.#hooks = normalizeHooks(options.hooks, ROUTER_PHASES, 'defineRouter');
    this.#validation = normalizeValidation(options.validation);
    this.#restOptions = normalizeRestOptions(options.rest);
    const unitKeys = Object.keys(definition);
    for (let i = 0; i < unitKeys.length; i++) this.#addUnit(unitKeys[i], definition[unitKeys[i]]);
    this.#rebuildChains();
  }

  /**
   * Adds a unit after construction — how the fastify adapter's mirror
   * feature lands units discovered at onReady, when the router already
   * exists. Refuses a unit key that is already registered (merge() is the
   * tool for combining routers). Returns the router.
   */
  addUnit(unitKey, definition) {
    const parts = String(unitKey).split('.');
    const version = parts.length > 1 ? parts[1] : DEFAULT_VERSION;
    if (this.#units.get(parts[0])?.has(version)) {
      throw new ConfigError(`addUnit: unit '${unitKey}' is already registered`);
    }
    this.#addUnit(unitKey, definition);
    this.#rebuildChains();
    return this;
  }

  /** Adds a router-level hook after construction. Returns the router. */
  addHook(name, fn) {
    if (!ROUTER_PHASES.includes(name)) {
      throw new ConfigError(`addHook: unknown hook phase '${name}'`);
    }
    if (typeof fn !== 'function') throw new ConfigError('addHook: the hook must be a function');
    this.#hooks[name] = [...this.#hooks[name], fn];
    this.#rebuildChains();
    return this;
  }

  /** The flattened pipeline for one procedure (router + unit + procedure). */
  hooksFor(proc) {
    return this.#chains.get(proc) ?? EMPTY_HOOKS;
  }

  /** The compiled { input?, output?, serialize? } for one procedure. */
  compiledFor(proc) {
    return this.#compiled.get(proc) ?? null;
  }

  /** True when any procedure compiled a response serializer. */
  get hasSerializers() {
    for (const artifacts of this.#compiled.values()) {
      if (artifacts.serialize) return true;
    }
    return false;
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
        for (const proc of entry.consumes.values()) this.#chains.set(proc, this.#chainFor(entry, proc));
      }
    }
    this.#rebuildRest();
    this.#rebuildCompiled();
  }

  // Schemas compile ONCE, when the router is built. A procedure whose
  // schema declares validation parts in a router with no injected ajv is a
  // build error: the declaration would otherwise be silently unenforced,
  // and an unenforced schema is an authorization bug waiting to be found.
  #rebuildCompiled() {
    this.#compiled = new Map();
    for (const { unitKey, entry } of unitVersions(this.#units)) {
      for (const method of entry.methods) this.#compileInto(method[1], `${unitKey}/${method[0]}`);
      for (const event of entry.events) this.#compileInto(event[1], `${unitKey}/on.${event[0]}`);
      for (const consumer of entry.consumes) this.#compileInto(consumer[1], `${unitKey}/consumes.${consumer[0]}`);
    }
  }

  #compileInto(proc, label) {
    const artifacts = this.#compileFor(proc, label);
    if (artifacts) this.#compiled.set(proc, artifacts);
  }

  #compileFor(proc, label) {
    const schema = proc.schema;
    if (!schema) return null;
    const hasInput =
      schema.params !== undefined ||
      schema.querystring !== undefined ||
      schema.body !== undefined ||
      schema.headers !== undefined;
    const success = successResponse(proc);
    if (!hasInput && !success) return null; // passthrough-only schema (tags, ...)
    const ajv = this.#validation?.ajv ?? null;
    if (!ajv) {
      throw new ConfigError(
        `Router: ${label} declares schema validation but no validation.ajv compiler was injected — ` +
          'pass defineRouter(units, { validation: { ajv } })',
      );
    }
    const artifacts = {};
    const input = hasInput ? compileInput(ajv, schema, label) : null;
    if (input) artifacts.input = input;
    if (success) {
      artifacts.output = compileOutput(ajv, success, label);
      const serializer = this.#validation?.serializer;
      if (serializer) {
        try {
          artifacts.serialize = serializer.compile(success);
        } catch (error) {
          throw new ConfigError(`Router: ${label} schema.response failed to compile a serializer: ${error.message}`, {
            cause: error,
          });
        }
      }
    }
    return artifacts;
  }

  // The trie — one tree per HTTP verb, the find-my-way shape, so
  // `POST /projects/:orgId` and `GET /projects/:id` coexist the way they
  // do in fastify. A node is { static: Map(segment -> node),
  // param: { name, node } | null, terminal: route | null }. Static beats
  // param on match; within one verb a duplicate path or two different
  // param names at one position are build-time conflicts, reported with
  // both procedures' addresses.
  #rebuildRest() {
    this.#rest = buildRestTrees(this.#units, this.#restOptions);
  }

  #chainFor(entry, proc) {
    const chain = {};
    for (let i = 0; i < INVOCATION_PHASES.length; i++) {
      const phase = INVOCATION_PHASES[i];
      const merged = [...this.#hooks[phase], ...(entry.hooks?.[phase] ?? []), ...(proc.hooks[phase] ?? [])];
      chain[phase] = merged.length > 0 ? Object.freeze(merged) : EMPTY_LIST;
    }
    return Object.freeze(chain);
  }

  #addUnit(unitKey, definition) {
    const parts = unitKey.split('.');
    const unit = parts[0];
    const rawVersion = parts[1];
    // A silent split would truncate 'unit.v1.2' into unit.v1 and merge
    // colliding registrations — reject anything but 'unit' / 'unit.vN'.
    // The vN pattern is the whole version syntax: 'auth.1' is invalid.
    if (
      !unit ||
      parts.length > 2 ||
      (rawVersion !== undefined && !VERSION_TOKEN.test(rawVersion)) ||
      typeof definition !== 'object' ||
      definition === null
    ) {
      throw new ConfigError(`Invalid router unit definition: ${unitKey} (a unit key is 'unit' or 'unit.vN')`);
    }
    const version = rawVersion ?? DEFAULT_VERSION;
    let versions = this.#units.get(unit);
    if (!versions) {
      versions = new Map();
      this.#units.set(unit, versions);
    }
    let entry = versions.get(version);
    if (!entry) {
      entry = { methods: new Map(), events: new Map(), consumes: new Map(), emits: null, hooks: null };
      versions.set(version, entry);
    }
    const names = Object.keys(definition);
    for (let i = 0; i < names.length; i++) {
      const name = names[i];
      const value = definition[name];
      if (name === EVENTS_KEY) {
        this.#addEvents(unitKey, entry.events, value);
        continue;
      }
      if (name === CONSUMES_KEY) {
        this.#addConsumers(unitKey, entry.consumes, value);
        continue;
      }
      if (name === EMITS_KEY) {
        if (typeof value !== 'object' || value === null || Array.isArray(value)) {
          throw new ConfigError(`Router definition ${unitKey}.emits must be an object of event descriptors`);
        }
        entry.emits = entry.emits ? { ...entry.emits, ...value } : { ...value };
        continue;
      }
      if (name === HOOKS_KEY) {
        const unitHooks = normalizeHooks(value, INVOCATION_PHASES, `${unitKey}.hooks`);
        entry.hooks = entry.hooks ? concatHooks(entry.hooks, unitHooks, INVOCATION_PHASES) : unitHooks;
        continue;
      }
      entry.methods.set(name, rejectConsume(toProcedure(value, unitKey, name), `${unitKey}/${name}`));
    }
  }

  #addConsumers(unitKey, consumes, definition) {
    if (typeof definition !== 'object' || definition === null || Array.isArray(definition)) {
      throw new ConfigError(`Router definition ${unitKey}.consumes must be an object of consumer procedures`);
    }
    const names = Object.keys(definition);
    for (let i = 0; i < names.length; i++) {
      const name = names[i];
      const proc = toProcedure(definition[name], unitKey, `consumes.${name}`);
      if (proc.subscription) {
        throw new ConfigError(`Router definition ${unitKey}.consumes.${name} cannot be a subscription`);
      }
      consumes.set(name, proc);
    }
  }

  // Event handlers reuse Procedure: an inbound event is a call that never
  // answers, so access control, input validation and queueing come for free.
  #addEvents(unitKey, events, definition) {
    if (typeof definition !== 'object' || definition === null) {
      throw new ConfigError(`Router definition ${unitKey}.on must be an object of event handlers`);
    }
    const names = Object.keys(definition);
    for (let i = 0; i < names.length; i++) {
      const name = names[i];
      events.set(name, rejectConsume(toProcedure(definition[name], unitKey, `on.${name}`), `${unitKey}/on.${name}`));
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

  /** A unit's queue-consumer procedure, or null. Never reachable by a call packet. */
  getConsumer(unit, version = DEFAULT_VERSION, name) {
    const entry = this.#units.get(unit)?.get(version);
    return entry?.consumes.get(name) ?? null;
  }

  /**
   * Every declared queue consumer — what a broker binding subscribes to.
   * Cold path. Deliberately NOT part of introspect(): which queues a server
   * drains is deployment topology, not a client contract.
   */
  consumers() {
    const list = [];
    for (const { unit, version, unitKey, entry } of unitVersions(this.#units)) {
      for (const consumer of entry.consumes) {
        list.push({ unitKey, unit, version, name: consumer[0], procedure: consumer[1] });
      }
    }
    return list;
  }

  /** True when at least one procedure declares an `http` mapping. */
  get hasRestRoutes() {
    return this.#rest !== null;
  }

  /** See matchRestTrees in rest.js — the trie set is per-router state. */
  matchRest(method, segments) {
    return matchRestTrees(this.#rest, method, segments);
  }

  /** Every declared REST route — what a host adapter registers natively. */
  restRoutes() {
    return collectRestRoutes(this.#units, this.#restOptions);
  }

  // Introspection v2: { unitKey: { method: { access, meta?, signature? } } }
  // where unitKey is 'unit' for the default version and 'unit.vN' otherwise.
  // Anything but an array of unit keys means "no filter" — REST calls
  // deliver plain-object args here.
  introspect(units = null, options = {}) {
    // A Set once, not includes() per unit: the list is the caller's, as
    // long as the caller makes it.
    const filter = Array.isArray(units) ? new Set(units) : null;
    // The input schema parts travel by default: they are what a client's
    // injected ajv pre-validates against, saving the round trip a doomed
    // call would make. `schemas: false` strips them (they can be sizable).
    const { schemas = true } = options;
    const result = {};
    for (const { version, unitKey, entry } of unitVersions(this.#units)) {
      if (filter && !filter.has(unitKey)) continue;
      const methodsInfo = {};
      for (const method of entry.methods) {
        const proc = method[1];
        const info = { access: proc.access };
        // Only subscriptions carry `kind`: a client scaffolds a call
        // unless told otherwise, so the common case stays one key.
        if (proc.subscription) info.kind = proc.kind;
        if (Object.keys(proc.meta).length > 0) info.meta = proc.meta;
        if (proc.signature) info.signature = proc.signature;
        // The REST mapping travels so clients (and codegen/OpenAPI
        // tooling) can address the same procedure as a plain endpoint —
        // effective, so a client's REST leg calls the versioned URL.
        if (proc.http) info.http = publicHttp(effectiveHttp(proc.http, version, this.#restOptions));
        if (schemas && proc.schema) {
          const parts = {};
          if (proc.schema.params !== undefined) parts.params = proc.schema.params;
          if (proc.schema.querystring !== undefined) parts.querystring = proc.schema.querystring;
          if (proc.schema.body !== undefined) parts.body = proc.schema.body;
          if (Object.keys(parts).length > 0) info.schema = parts;
        }
        assignKey(methodsInfo, method[0], info);
      }
      // The unit's INBOUND event handlers, under the same reserved 'on'
      // key the definition used — additive, and impossible to collide
      // with a method ('on' routes to #addEvents at definition time).
      // What `wrpc types` turns into the contract's `sends` key.
      if (entry.events.size > 0) {
        const inbound = {};
        for (const event of entry.events) {
          const eventInfo = { access: event[1].access };
          if (event[1].signature) eventInfo.signature = event[1].signature;
          assignKey(inbound, event[0], eventInfo);
        }
        assignKey(methodsInfo, EVENTS_KEY, inbound);
      }
      // The declared OUTBOUND events, verbatim — the contract's `events`.
      if (entry.emits) assignKey(methodsInfo, EMITS_KEY, entry.emits);
      assignKey(result, unitKey, methodsInfo);
    }
    return result;
  }

  // Returns a NEW router; on collision the other router's procedure wins.
  // Hooks travel too: router-level hooks concatenate (this first), unit
  // hooks ride with their unit, and the chains are rebuilt for the merged
  // set — the shared Procedure instances themselves are never mutated.
  merge(other) {
    const hooks = concatHooks(this.#hooks, other.#hooks, ROUTER_PHASES);
    // This router's compilers win, the other's fill in — mirroring the
    // hook order (this first). Procedures recompile against the winner.
    const validation =
      this.#validation || other.#validation
        ? {
            ajv: this.#validation?.ajv ?? other.#validation?.ajv,
            serializer: this.#validation?.serializer ?? other.#validation?.serializer,
          }
        : undefined;
    // The rest strategy must survive a merge (receiver wins, like
    // validation): #withIntrospection merges a system router into EVERY
    // server's router by default, and dropping it there would silently
    // unprefix every versioned route.
    const merged = new Router({}, { hooks, validation, rest: this.#restOptions ?? other.#restOptions ?? undefined });
    this.#addUnitsTo(merged);
    other.#addUnitsTo(merged);
    merged.#rebuildChains();
    return merged;
  }

  #addUnitsTo(target) {
    for (const { unitKey, entry } of unitVersions(this.#units)) {
      const definition = Object.fromEntries(entry.methods);
      if (entry.events.size > 0) definition[EVENTS_KEY] = Object.fromEntries(entry.events);
      if (entry.consumes.size > 0) definition[CONSUMES_KEY] = Object.fromEntries(entry.consumes);
      if (entry.emits) definition[EMITS_KEY] = entry.emits;
      if (entry.hooks) definition[HOOKS_KEY] = entry.hooks;
      target.#addUnit(unitKey, definition);
    }
  }
}

// ---------------------------------------------------------------------------
// The effective fastify-shaped schema a host (fastify routes, swagger,
// introspection consumers) receives: the user's declaration with wrpc's own
// lifecycle error statuses documented underneath. Which statuses apply is
// derived from the procedure's options; docs/reference/errors.md is the
// dictionary. 499 (cancel) is deliberately absent — a cancelled HTTP
// request gets no response to document. A user's schema.response entry for
// the same code always overrides the default, and `false` removes it.

const WIRE_ERROR_SCHEMA = Object.freeze({
  type: 'object',
  properties: {
    message: { type: 'string' },
    code: { type: 'number' },
    details: {},
  },
  required: ['message', 'code'],
});

const defaultErrorResponses = (proc) => {
  const codes = [429, 500, 503];
  if (proc.schema || proc.input) codes.push(400);
  if (proc.access !== 'public') codes.push(403);
  if (proc.timeout > 0) codes.push(408);
  const responses = {};
  for (let i = 0; i < codes.length; i++) responses[codes[i]] = WIRE_ERROR_SCHEMA;
  return responses;
};

const effectiveSchema = (proc) => {
  const response = defaultErrorResponses(proc);
  const declared = proc.schema?.response ?? {};
  const keys = Object.keys(declared);
  for (let i = 0; i < keys.length; i++) {
    if (declared[keys[i]] === false) delete response[keys[i]];
    else assignKey(response, keys[i], declared[keys[i]]);
  }
  const schema = { ...proc.schema };
  schema.response = response;
  return schema;
};

const defineRouter = (definition, options) => new Router(definition, options);

module.exports = {
  runValidator,
  Procedure,
  Router,
  procedure,
  defineRouter,
  effectiveSchema,
  runHooks,
  runHooksSafe,
  EMPTY_HOOKS,
  INVOCATION_PHASES,
  CONNECTION_PHASES,
};
