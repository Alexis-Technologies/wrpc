'use strict';

const { Semaphore } = require('../utils.js');
const { isTracked, tracked } = require('./subscriptions.js');

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

// Dispatcher maps `error.code` (number) onto the wire error code:
// 400 invalid input, 408 timeout, 500 invalid output, 503 queue overflow.
// `expose` marks the message as protocol surface: these strings are written
// for the caller, so the transport sends them verbatim even on a 5xx, where
// an ordinary error's message is replaced by the status line.
const codedError = (message, code, details) => {
  const error = new Error(message);
  error.code = code;
  error.expose = true;
  if (details !== undefined) error.details = details;
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
    // The joined message stays the human line; the issues keep their paths
    // as structured `details`, which the transport forwards on 4xx (or on
    // expose) as the wire error's optional `details` field.
    const issues = [];
    for (const issue of result.issues) {
      issues.push({ message: issue.message, path: issue.path });
    }
    const error = new Error(issues.map((issue) => issue.message).join('; '));
    error.details = { issues };
    throw error;
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

const HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE'];
const PARAM_SEGMENT = /^:[A-Za-z_$][\w$]*$/;
const STATIC_SEGMENT = /^[^/:*]+$/;

const normalizeHttp = (http) => {
  if (http === null || http === undefined) return null;
  if (typeof http !== 'object') throw new TypeError('procedure() http must be an object');
  const { method, path, status = null } = http;
  if (!HTTP_METHODS.includes(method)) {
    throw new TypeError(`procedure() http.method must be one of ${HTTP_METHODS.join(', ')}`);
  }
  if (typeof path !== 'string' || !path.startsWith('/')) {
    throw new TypeError("procedure() http.path must be a string starting with '/'");
  }
  const segments = path === '/' ? [] : path.slice(1).split('/');
  for (const segment of segments) {
    if (!PARAM_SEGMENT.test(segment) && !STATIC_SEGMENT.test(segment)) {
      throw new TypeError(`procedure() http.path has an invalid segment '${segment}' in '${path}'`);
    }
  }
  if (status !== null && !(Number.isInteger(status) && status >= 200 && status <= 599)) {
    throw new TypeError('procedure() http.status must be an integer HTTP status (200-599)');
  }
  return { method, path, status };
};

// The parts wrpc itself understands; everything else on `schema` passes
// through untouched. `query` and `querystring` are interchangeable spellings
// of the same part — normalized to `querystring` (fastify's canon).
const SCHEMA_PARTS = ['params', 'querystring', 'body', 'headers', 'response'];

const normalizeSchema = (schema, label) => {
  if (schema === null || schema === undefined) return null;
  if (typeof schema !== 'object' || Array.isArray(schema)) {
    throw new TypeError(`${label} schema must be an object (fastify route schema shape)`);
  }
  if (schema.query !== undefined && schema.querystring !== undefined && schema.query !== schema.querystring) {
    throw new TypeError(`${label} schema has both 'query' and 'querystring' and they differ — use one`);
  }
  const normalized = {};
  for (const key of Object.keys(schema)) {
    if (key === 'query') continue; // folded into querystring below
    assignKey(normalized, key, schema[key]);
  }
  if (schema.query !== undefined && schema.querystring === undefined) normalized.querystring = schema.query;
  for (const part of SCHEMA_PARTS) {
    const value = normalized[part];
    if (value !== undefined && (typeof value !== 'object' || value === null)) {
      throw new TypeError(`${label} schema.${part} must be an object`);
    }
  }
  return normalized;
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
      http = null,
      schema = null,
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
    this.http = normalizeHttp(http);
    this.schema = normalizeSchema(schema, 'procedure()');
    // One declarative source of truth per direction: a schema beside a
    // programmatic validator would leave "which one ran?" ambiguous.
    if (this.schema && (input || output)) {
      throw new TypeError('procedure() schema and input/output are mutually exclusive');
    }
    if (queue && !(Number.isInteger(queue.concurrency) && queue.concurrency > 0)) {
      throw new TypeError('procedure() queue.concurrency must be a positive integer');
    }
    // A subscription lives until it is cancelled, so both of these would
    // mean something different from what they mean for a call — and quietly
    // meaning something else is worse than refusing.
    if (this.kind === SUBSCRIPTION && (queue || timeout)) {
      throw new TypeError('procedure.subscription() does not support queue or timeout');
    }
    // A subscription is already refused on plain HTTP (400), so an HTTP
    // route mapping could never mean anything for one.
    if (this.kind === SUBSCRIPTION && this.http) {
      throw new TypeError('procedure.subscription() does not support http');
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
  async *subscribe(context, args, options = {}, hooks = EMPTY_HOOKS, compiled = null) {
    if (this.kind !== SUBSCRIPTION) {
      throw codedError('Not a subscription', 400);
    }
    const inputValidator = compiled?.input ?? this.input;
    const outputValidator = compiled?.output ?? this.output;
    if (hooks.preValidation.length > 0) await runHooks(hooks.preValidation, context, args);
    let input = args;
    if (inputValidator) {
      input = await runValidator(inputValidator, args).catch((error) => {
        throw codedError(`Invalid arguments: ${error.message}`, 400, error.details);
      });
    }
    if (hooks.preHandler.length > 0) await runHooks(hooks.preHandler, context, input);
    const source = this.handler(context, input, options);
    if (!source || typeof source[Symbol.asyncIterator] !== 'function') {
      throw codedError('Subscription handler must return an async iterable', 500);
    }
    for await (const value of source) {
      if (!outputValidator) {
        yield value;
        continue;
      }
      // Validate the payload, not the tracking wrapper: an output schema
      // describes what the client receives, not how it is labelled.
      const payload = isTracked(value) ? value.data : value;
      const checked = await runValidator(outputValidator, payload).catch((error) => {
        throw codedError(`Invalid subscription value: ${error.message}`, 500, error.details);
      });
      yield isTracked(value) ? tracked(value.id, checked) : checked;
    }
  }

  // The host-delegated entry (the fastify adapter's native routes): the
  // semaphore and the deadline behave EXACTLY as in invoke() — the slot is
  // held until the handler settles — but hooks, access and validators are
  // the host's job, already run by the time this is called.
  async invokeBare(context, args) {
    if (this.kind === SUBSCRIPTION) {
      throw codedError('This procedure is a subscription: use {type:"subscribe"}', 400);
    }
    const deadline = this.timeout > 0 ? Date.now() + this.timeout : 0;
    if (this.semaphore) {
      try {
        await this.semaphore.enter(context.signal ?? null);
      } catch (error) {
        throw codedError(error.message, 503);
      }
    }
    let handlerStarted = false;
    try {
      if (deadline > 0 && Date.now() >= deadline) {
        throw codedError('Procedure timeout', 408);
      }
      handlerStarted = true;
      const invocation = Promise.resolve().then(() => this.handler(context, args));
      if (this.semaphore) {
        const release = () => this.semaphore.leave();
        invocation.then(release, release);
      }
      return deadline > 0 ? await timeoutRace(invocation, Math.max(1, deadline - Date.now())) : await invocation;
    } finally {
      if (this.semaphore && !handlerStarted) this.semaphore.leave();
    }
  }

  async invoke(context, args, hooks = EMPTY_HOOKS, compiled = null) {
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
      const inputValidator = compiled?.input ?? this.input;
      const outputValidator = compiled?.output ?? this.output;
      if (hooks.preValidation.length > 0) await runHooks(hooks.preValidation, context, args);
      if (inputValidator) {
        args = await runValidator(inputValidator, args).catch((error) => {
          throw codedError(`Invalid arguments: ${error.message}`, 400, error.details);
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
      if (outputValidator) {
        result = await runValidator(outputValidator, result).catch((error) => {
          throw codedError(`Invalid procedure result: ${error.message}`, 500, error.details);
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

// ---------------------------------------------------------------------------
// Injected JSON Schema compilers. Structural, per the zero-dependency rule:
// `ajv` is anything with compile(schema) -> validateFn (ajv-shaped: the fn
// answers a boolean and exposes `.errors`), `serializer` anything with
// compile(schema) -> (value) -> string (fast-json-stringify-shaped). wrpc
// never imports either — the application passes its own.

const normalizeValidation = (validation) => {
  if (validation === null || validation === undefined) return null;
  if (typeof validation !== 'object') {
    throw new TypeError('defineRouter: options.validation must be an object');
  }
  const { ajv = null, serializer = null } = validation;
  if (ajv !== null && typeof ajv.compile !== 'function') {
    throw new TypeError('defineRouter: validation.ajv must provide compile(schema)');
  }
  if (serializer !== null && typeof serializer.compile !== 'function') {
    throw new TypeError('defineRouter: validation.serializer must provide compile(schema)');
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

// Compiles the input parts into ONE function validator, fastify-style: each
// part validates its slice of `{ params, query, body }`, issues carry
// part-prefixed paths, and ajv's own coercions land in place. The wrapper is
// an ordinary function validator, so `runValidator` stays the single
// execution seam.
const compileInput = (ajv, schema, label) => {
  const parts = [];
  for (const [part, argsKey] of INPUT_PARTS) {
    if (schema[part] === undefined) continue;
    let validate;
    try {
      validate = ajv.compile(schema[part]);
    } catch (error) {
      throw new TypeError(`${label}: schema.${part} failed to compile: ${error.message}`);
    }
    parts.push({ part, argsKey, validate });
  }
  if (parts.length === 0) return null;
  return (args) => {
    const value = args && typeof args === 'object' ? args : {};
    let issues = null;
    for (const { part, argsKey, validate } of parts) {
      if (validate(value[argsKey] ?? {})) continue;
      issues ??= [];
      for (const item of validate.errors ?? []) {
        issues.push({ message: item.message, path: `/${part}${item.instancePath ?? ''}` });
      }
    }
    if (issues) {
      const messages = [];
      for (const issue of issues) messages.push(`${issue.path} ${issue.message}`);
      const error = new Error(messages.join('; '));
      error.details = { issues };
      throw error;
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
  for (const key of Object.keys(response)) {
    const code = Number(key);
    if (code >= 200 && code < 300 && response[key] !== false) return response[key];
  }
  return null;
};

const compileOutput = (ajv, schema, label) => {
  let validate;
  try {
    validate = ajv.compile(schema);
  } catch (error) {
    throw new TypeError(`${label}: schema.response failed to compile: ${error.message}`);
  }
  return (result) => {
    if (validate(result)) return result;
    const issues = [];
    for (const item of validate.errors ?? []) {
      issues.push({ message: item.message, path: item.instancePath });
    }
    const error = new Error(issues.map((issue) => `${issue.path} ${issue.message}`).join('; '));
    error.details = { issues };
    throw error;
  };
};

// Router-level REST options; today one strategy: how a versioned unit's
// declared paths surface. `null` means "declared paths verbatim".
const normalizeRestOptions = (rest) => {
  if (rest === undefined || rest === null) return null;
  if (typeof rest !== 'object' || Array.isArray(rest)) {
    throw new TypeError('defineRouter: rest must be an options object');
  }
  const { version } = rest;
  if (version === undefined) return null;
  if (version !== 'path' && typeof version !== 'function') {
    throw new TypeError("defineRouter: rest.version must be 'path' or a function (version, path) => path");
  }
  return { version };
};

// The version-aware view of a procedure's http mapping: with a rest.version
// strategy, a versioned unit's declared path gains its '/vN' prefix here —
// computed at build/introspection time, never by mutating proc.http
// (Procedure instances are shared across routers by merge()). The version
// token already carries the 'v' ('auth.v1' -> 'v1'), so 'path' is a plain
// prefix. The one seam serves the trie, restRoutes() and introspect(), which
// is what keeps dispatch, host adapters and clients version-consistent.
const effectiveHttp = (http, version, restOptions) => {
  if (!http || version === DEFAULT_VERSION || !restOptions?.version) return http;
  const spec = restOptions.version;
  const path =
    typeof spec === 'function' ? spec(version, http.path) : `/${version}${http.path === '/' ? '' : http.path}`;
  if (typeof path !== 'string' || !path.startsWith('/')) {
    throw new TypeError(`rest.version must produce a path starting with '/', got ${JSON.stringify(path)}`);
  }
  return { ...http, path };
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
    for (const [unitKey, methods] of Object.entries(definition)) {
      this.#addUnit(unitKey, methods);
    }
    this.#rebuildChains();
  }

  /**
   * Adds a unit after construction — how the fastify adapter's mirror
   * feature lands units discovered at onReady, when the router already
   * exists. Refuses a unit key that is already registered (merge() is the
   * tool for combining routers). Returns the router.
   */
  addUnit(unitKey, definition) {
    const [unit, version = DEFAULT_VERSION] = String(unitKey).split('.');
    if (this.#units.get(unit)?.has(version)) {
      throw new TypeError(`addUnit: unit '${unitKey}' is already registered`);
    }
    this.#addUnit(unitKey, definition);
    this.#rebuildChains();
    return this;
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
    for (const [unit, versions] of this.#units) {
      for (const [version, entry] of versions) {
        const unitKey = version === DEFAULT_VERSION ? unit : `${unit}.${version}`;
        for (const [methodName, proc] of entry.methods) {
          const artifacts = this.#compileFor(proc, `${unitKey}/${methodName}`);
          if (artifacts) this.#compiled.set(proc, artifacts);
        }
        for (const [eventName, proc] of entry.events) {
          const artifacts = this.#compileFor(proc, `${unitKey}/on.${eventName}`);
          if (artifacts) this.#compiled.set(proc, artifacts);
        }
      }
    }
  }

  #compileFor(proc, label) {
    const schema = proc.schema;
    if (!schema) return null;
    const hasInput = schema.params !== undefined || schema.querystring !== undefined || schema.body !== undefined;
    const success = successResponse(proc);
    if (!hasInput && !success) return null; // passthrough-only schema (tags, ...)
    const ajv = this.#validation?.ajv ?? null;
    if (!ajv) {
      throw new TypeError(
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
          throw new TypeError(`Router: ${label} schema.response failed to compile a serializer: ${error.message}`);
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
    this.#rest = null;
    const makeNode = () => ({ static: new Map(), param: null, terminal: null });
    let trees = null;
    for (const [unit, versions] of this.#units) {
      for (const [version, entry] of versions) {
        const unitKey = version === DEFAULT_VERSION ? unit : `${unit}.${version}`;
        for (const [methodName, proc] of entry.methods) {
          if (!proc.http) continue;
          trees ??= new Map();
          // Effective, not declared: with rest.version, two versions of one
          // declared path diverge by their '/vN' prefix BEFORE the conflict
          // check below ever sees them.
          const http = effectiveHttp(proc.http, version, this.#restOptions);
          const { method, path } = http;
          let node = trees.get(method);
          if (!node) {
            node = makeNode();
            trees.set(method, node);
          }
          const segments = path === '/' ? [] : path.slice(1).split('/');
          const paramNames = [];
          for (const segment of segments) {
            if (segment.startsWith(':')) {
              const name = segment.slice(1);
              if (node.param && node.param.name !== name) {
                throw new TypeError(
                  `REST route conflict: ${method} ':${node.param.name}' and ':${name}' at the same position ` +
                    `(${unitKey}/${methodName} vs an earlier route)`,
                );
              }
              node.param ??= { name, node: makeNode() };
              paramNames.push(name);
              node = node.param.node;
              continue;
            }
            let next = node.static.get(segment);
            if (!next) {
              next = makeNode();
              node.static.set(segment, next);
            }
            node = next;
          }
          if (node.terminal) {
            const existing = node.terminal;
            throw new TypeError(
              `REST route conflict: ${method} ${path} is declared by both ` +
                `${existing.unitKey}/${existing.methodName} and ${unitKey}/${methodName}`,
            );
          }
          node.terminal = { proc, unitKey, methodName, paramNames, http };
        }
      }
    }
    this.#rest = trees;
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
    const [unit, rawVersion, ...extra] = unitKey.split('.');
    // A silent split would truncate 'unit.v1.2' into unit.v1 and merge
    // colliding registrations — reject anything but 'unit' / 'unit.vN'.
    // The vN pattern is the whole version syntax: 'auth.1' is invalid.
    if (
      !unit ||
      extra.length > 0 ||
      (rawVersion !== undefined && !VERSION_TOKEN.test(rawVersion)) ||
      typeof definition !== 'object' ||
      definition === null
    ) {
      throw new TypeError(`Invalid router unit definition: ${unitKey} (a unit key is 'unit' or 'unit.vN')`);
    }
    const version = rawVersion ?? DEFAULT_VERSION;
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

  /** True when at least one procedure declares an `http` mapping. */
  get hasRestRoutes() {
    return this.#rest !== null;
  }

  /**
   * Matches an HTTP verb and decoded path segments against the declarative
   * REST table. Returns null (no path match — the caller falls back to the
   * conventional /:unit/:method mode), `{ allowed }` (some OTHER verb
   * matches this path — a 405 with an Allow list), or the full route
   * `{ proc, unitKey, methodName, params, http }`.
   */
  matchRest(method, segments) {
    if (!this.#rest) return null;
    const hit = this.#walk(this.#rest.get(method), segments);
    if (hit) {
      const params = {};
      for (let i = 0; i < hit.route.paramNames.length; i++) {
        assignKey(params, hit.route.paramNames[i], hit.values[i]);
      }
      const { proc, unitKey, methodName, http } = hit.route;
      return { proc, unitKey, methodName, params, http };
    }
    // No route under this verb — probe the other verbs' trees so the
    // answer distinguishes "unknown path" (fall through, maybe the
    // conventional mode knows it) from "known path, wrong verb" (405).
    const allowed = [];
    for (const [verb, tree] of this.#rest) {
      if (verb !== method && this.#walk(tree, segments)) allowed.push(verb);
    }
    return allowed.length > 0 ? { allowed } : null;
  }

  #walk(node, segments) {
    if (!node) return null;
    const values = [];
    for (const segment of segments) {
      const next = node.static.get(segment);
      if (next) {
        node = next;
        continue;
      }
      if (node.param) {
        values.push(segment);
        node = node.param.node;
        continue;
      }
      return null;
    }
    return node.terminal ? { route: node.terminal, values } : null;
  }

  /** Every declared REST route — what a host adapter registers natively. */
  restRoutes() {
    const routes = [];
    for (const [unit, versions] of this.#units) {
      for (const [version, entry] of versions) {
        const unitKey = version === DEFAULT_VERSION ? unit : `${unit}.${version}`;
        for (const [methodName, proc] of entry.methods) {
          if (!proc.http) continue;
          routes.push({ unitKey, methodName, proc, http: effectiveHttp(proc.http, version, this.#restOptions) });
        }
      }
    }
    return routes;
  }

  // Introspection v2: { unitKey: { method: { access, meta?, signature? } } }
  // where unitKey is 'unit' for the default version and 'unit.vN' otherwise.
  // Anything but an array of unit keys means "no filter" — REST calls
  // deliver plain-object args here.
  introspect(units = null, options = {}) {
    const filter = Array.isArray(units) ? units : null;
    // The input schema parts travel by default: they are what a client's
    // injected ajv pre-validates against, saving the round trip a doomed
    // call would make. `schemas: false` strips them (they can be sizable).
    const { schemas = true } = options;
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
          // The REST mapping travels so clients (and codegen/OpenAPI
          // tooling) can address the same procedure as a plain endpoint —
          // effective, so a client's REST leg calls the versioned URL.
          if (proc.http) info.http = effectiveHttp(proc.http, version, this.#restOptions);
          if (schemas && proc.schema) {
            const parts = {};
            if (proc.schema.params !== undefined) parts.params = proc.schema.params;
            if (proc.schema.querystring !== undefined) parts.querystring = proc.schema.querystring;
            if (proc.schema.body !== undefined) parts.body = proc.schema.body;
            if (Object.keys(parts).length > 0) info.schema = parts;
          }
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
  for (const code of codes) responses[code] = WIRE_ERROR_SCHEMA;
  return responses;
};

const effectiveSchema = (proc) => {
  const response = defaultErrorResponses(proc);
  const declared = proc.schema?.response ?? {};
  for (const key of Object.keys(declared)) {
    if (declared[key] === false) delete response[key];
    else assignKey(response, key, declared[key]);
  }
  const schema = { ...(proc.schema ?? {}) };
  schema.response = response;
  return schema;
};

const defineRouter = (definition, options) => new Router(definition, options);

module.exports = {
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
