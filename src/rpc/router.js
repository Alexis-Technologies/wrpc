'use strict';

const { Semaphore } = require('../utils.js');

const DEFAULT_VERSION = '*';

// Dispatcher maps `error.code` (number) onto the wire error code:
// 400 invalid input, 408 timeout, 500 invalid output, 503 queue overflow.
const codedError = (message, code) => {
  const error = new Error(message);
  error.code = code;
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
    } = options;
    if (typeof handler !== 'function') {
      throw new TypeError('procedure() requires a handler function');
    }
    if (input !== null && !isValidator(input)) {
      throw new TypeError('procedure() input must be a function or a Standard Schema');
    }
    if (output !== null && !isValidator(output)) {
      throw new TypeError('procedure() output must be a function or a Standard Schema');
    }
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
    this.semaphore = queue ? new Semaphore(queue) : null;
  }

  async invoke(context, args) {
    if (this.semaphore) {
      try {
        await this.semaphore.enter();
      } catch (error) {
        throw codedError(error.message, 503);
      }
    }
    let handlerStarted = false;
    try {
      if (this.input) {
        args = await runValidator(this.input, args).catch((error) => {
          throw codedError(`Invalid arguments: ${error.message}`, 400);
        });
      }
      handlerStarted = true;
      const invocation = Promise.resolve().then(() => this.handler(context, args));
      if (this.semaphore) {
        // The queue slot is held until the HANDLER settles: a timeout
        // rejects the caller but cannot cancel the handler, and freeing
        // the slot early would break the concurrency guarantee.
        const release = () => this.semaphore.leave();
        invocation.then(release, release);
      }
      let result = this.timeout > 0 ? await timeoutRace(invocation, this.timeout) : await invocation;
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
const EVENTS_KEY = 'on';

class Router {
  // unit -> Map(version -> { methods: Map(name -> Procedure),
  //                          events:  Map(name -> Procedure) })
  #units = new Map();

  constructor(definition = {}) {
    for (const [unitKey, methods] of Object.entries(definition)) {
      this.#addUnit(unitKey, methods);
    }
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
      entry = { methods: new Map(), events: new Map() };
      versions.set(version, entry);
    }
    for (const [name, value] of Object.entries(definition)) {
      if (name === EVENTS_KEY) {
        this.#addEvents(unitKey, entry.events, value);
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
          if (Object.keys(proc.meta).length > 0) info.meta = proc.meta;
          if (proc.signature) info.signature = proc.signature;
          assignKey(methodsInfo, methodName, info);
        }
        assignKey(result, unitKey, methodsInfo);
      }
    }
    return result;
  }

  // Returns a NEW router; on collision the other router's procedure wins
  merge(other) {
    const merged = new Router();
    for (const source of [this, other]) {
      for (const [unit, versions] of source.#units) {
        for (const [version, entry] of versions) {
          const unitKey = version === DEFAULT_VERSION ? unit : `${unit}.${version}`;
          const definition = Object.fromEntries(entry.methods);
          if (entry.events.size > 0) definition[EVENTS_KEY] = Object.fromEntries(entry.events);
          merged.#addUnit(unitKey, definition);
        }
      }
    }
    return merged;
  }
}

const defineRouter = (definition) => new Router(definition);

module.exports = { Procedure, Router, procedure, defineRouter };
