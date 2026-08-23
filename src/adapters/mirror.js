'use strict';

// Reverse engineering for the fastify adapter: existing fastify routes
// become wrpc procedures, without rewriting a single route. The generated
// procedure dispatches through `fastify.inject()` (light-my-request), so the
// route's WHOLE pipeline — onRequest, auth, schema validation,
// serialization — runs exactly as it does for a network request; wrpc adds
// addressability (`client.api.projects.create(...)` over ws/sse/worker) on
// top.
//
// Routes are collected with an `onRoute` hook while the app registers them
// (so the wrpc plugin must be registered BEFORE the routes it should
// mirror), and the units land on the router at `onReady` through the public
// `Router.addUnit`.
//
// Mirrored procedures deliberately carry NO `http` mapping and NO `schema`:
// the real endpoint already exists at the route's own path (outside
// basePath), and validation is the route's — wrpc validating again would be
// the double-validation this design exists to avoid. The route's origin is
// recorded in `meta` instead, and its JSON Schemas are distilled into a
// `signature` so `wrpc types` still types the mirrored methods.

const { procedure } = require('../rpc/router.js');

const HTTP_METHODS = new Set(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE']);

const capitalize = (text) => (text.length === 0 ? text : text[0].toUpperCase() + text.slice(1));

// 'org-users' / 'org_users' -> 'orgUsers'
const toCamelCase = (text) => text.replace(/[-_]+(\w)/g, (_, letter) => letter.toUpperCase());

const isParam = (segment) => segment.startsWith(':');

// The default unit, two-pass. A parametric URL names its own unit: the
// LAST static segment before the first param — '/workspace/projects/:id'
// -> 'projects' — with one refinement: when the URL ends in
// '<static>/:<sameName>' the static is a qualifier of the param
// ('/projects/slug/:slug'), so the unit is the segment BEFORE it and the
// qualifier stays in the tail for naming. A parameterless URL is ambiguous
// ('/workspace/projects/archive': unit 'projects' + tail, or unit
// 'archive'?), so it consults the units the parametric routes established:
// the RIGHTMOST segment naming a known unit wins, and only without one
// does the last segment fall back to being the unit. Returns null for a
// URL with no static segment to name a unit after.
const defaultUnit = (segments, knownUnits = null) => {
  let end = segments.length;
  for (let i = 0; i < segments.length; i++) {
    if (isParam(segments[i])) {
      end = i;
      break;
    }
  }
  if (end === 0) return null;
  if (end === segments.length && knownUnits) {
    for (let i = segments.length - 1; i >= 0; i--) {
      if (knownUnits.has(toCamelCase(segments[i]))) {
        return { unit: toCamelCase(segments[i]), tailIndex: i + 1 };
      }
    }
  }
  const qualifier =
    end >= 2 && end === segments.length - 1 && capitalize(segments[end - 1]) === capitalize(segments[end].slice(1));
  const unitIndex = qualifier ? end - 2 : end - 1;
  if (unitIndex < 0) return null;
  return { unit: toCamelCase(segments[unitIndex]), tailIndex: unitIndex + 1 };
};

// Reverse REST semantics: the verb implies the action, the tail refines it.
//   POST /projects            -> create        GET /projects        -> findAll
//   GET  /projects/:id        -> findById      GET /projects/archive -> findAllArchive
//   GET  /projects/slug/:slug -> findBySlug    POST /:orgId/archive/:id -> createArchive
//   PATCH -> update..., PUT -> replace..., DELETE -> delete...
// A name the semantics cannot express is what `config.wrpc.name` is for.
const defaultName = (method, tail) => {
  const statics = [];
  let lastParam = null;
  for (const segment of tail) {
    if (isParam(segment)) lastParam = segment.slice(1);
    else statics.push(toCamelCase(segment));
  }
  const endsWithParam = tail.length > 0 && isParam(tail[tail.length - 1]);
  // '/slug/:slug' collapses: the static names the param, not a sub-resource.
  if (endsWithParam && statics.length > 0 && capitalize(statics[statics.length - 1]) === capitalize(lastParam)) {
    statics.pop();
  }
  const suffix = statics.map(capitalize).join('');
  if (method === 'GET' || method === 'HEAD') {
    return endsWithParam ? `find${suffix}By${capitalize(lastParam)}` : `findAll${suffix}`;
  }
  const verbs = { POST: 'create', PUT: 'replace', PATCH: 'update', DELETE: 'delete' };
  return `${verbs[method]}${suffix}`;
};

// ---------------------------------------------------------------------------
// JSON Schema -> the closed `signature` format (protocol.md), best-effort:
// anything the format cannot express becomes 'unknown', never a guess.

const SIGNATURE_DEPTH = 4;

const signatureType = (schema, depth = 0) => {
  if (!schema || typeof schema !== 'object' || depth >= SIGNATURE_DEPTH) return 'unknown';
  const { type } = schema;
  if (type === 'string' || type === 'number' || type === 'boolean' || type === 'null') return type;
  if (type === 'integer') return 'number';
  if (type === 'array') return [signatureType(schema.items, depth + 1)];
  if (type === 'object') {
    if (!schema.properties) return 'object';
    const required = new Set(Array.isArray(schema.required) ? schema.required : []);
    const fields = {};
    for (const key of Object.keys(schema.properties)) {
      const name = required.has(key) ? key : `${key}?`;
      Object.defineProperty(fields, name, {
        value: signatureType(schema.properties[key], depth + 1),
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
    return fields;
  }
  return 'unknown';
};

const routeSignature = (schema) => {
  if (!schema || typeof schema !== 'object') return null;
  const args = {};
  if (schema.params) args.params = signatureType(schema.params);
  if (schema.querystring ?? schema.query) args['query?'] = signatureType(schema.querystring ?? schema.query);
  if (schema.body) args.body = signatureType(schema.body);
  const success = schema.response?.[200] ?? schema.response?.[201] ?? schema.response?.['2xx'];
  const signature = {};
  if (Object.keys(args).length > 0) signature.args = args;
  if (success) signature.returns = signatureType(success);
  return Object.keys(signature).length > 0 ? signature : null;
};

// ---------------------------------------------------------------------------

const buildUrl = (url, params, query, stringify) => {
  const segments = url.split('/');
  const parts = new Array(segments.length);
  for (let i = 0; i < segments.length; i++) {
    const segment = segments[i];
    if (!isParam(segment)) {
      parts[i] = segment;
      continue;
    }
    const name = segment.slice(1);
    const value = params?.[name];
    if (value === undefined) {
      const error = new Error(`Missing path param '${name}' for ${url}`);
      error.code = 400;
      error.expose = true;
      throw error;
    }
    parts[i] = encodeURIComponent(String(value));
  }
  let built = parts.join('/');
  if (query && Object.keys(query).length > 0) {
    const text = stringify ? stringify(query) : String(new URLSearchParams(query));
    if (text) built += `?${text}`;
  }
  return built;
};

const mirrorHandler = (fastify, route, mirror, stringify) => {
  const { method, url } = route;
  return async (context, { params = {}, query = {}, body, headers } = {}) => {
    const response = await fastify.inject({
      method,
      url: buildUrl(url, params, query, stringify),
      ...(body === undefined ? {} : { payload: body }),
      headers: { ...(mirror.headers ? await mirror.headers(context) : {}), ...headers },
    });
    const json = (response.headers['content-type'] ?? '').includes('json');
    const parsed = json ? response.json() : response.body;
    if (response.statusCode >= 400) {
      const error = new Error((json && parsed?.message) || `HTTP ${response.statusCode}`);
      error.code = response.statusCode;
      error.expose = true;
      if (json && parsed?.details !== undefined) error.details = parsed.details;
      throw error;
    }
    if (response.statusCode === 204) return undefined;
    return parsed;
  };
};

// Wired by wrpcFastify when `options.mirror` is set. `mirror: true` takes
// every default; an object refines them.
const setupMirror = (fastify, rpc, options) => {
  const mirror = options.mirror === true ? {} : options.mirror;
  const stringify = options.querystring?.stringify ?? null;
  const collected = [];

  fastify.addHook('onRoute', (route) => {
    // The adapter's own routes carry config.wrpc === true; an OBJECT there
    // is the app's per-route mirror config ({ unit, name }), and `false`
    // opts a route out. HEAD twins and OPTIONS are transport furniture.
    if (route.config?.wrpc === true) return;
    const methods = Array.isArray(route.method) ? route.method : [route.method];
    for (const method of methods) {
      if (method === 'HEAD' || method === 'OPTIONS' || !HTTP_METHODS.has(method)) continue;
      if (route.url.includes('*')) continue; // a wildcard names no action
      const entry = { method, url: route.url, schema: route.schema ?? null, config: route.config ?? {} };
      if (mirror.include && !mirror.include(entry)) continue;
      if (entry.config.wrpc === false) continue;
      collected.push(entry);
    }
  });

  fastify.addHook('onReady', async () => {
    const units = new Map();
    // Pass 1: parametric routes establish the unit vocabulary the
    // parameterless ones disambiguate against.
    const knownUnits = new Set();
    for (const route of collected) {
      const segments = route.url.split('/').filter((segment) => segment.length > 0);
      if (!segments.some(isParam)) continue;
      const named = defaultUnit(segments);
      if (named) knownUnits.add(named.unit);
    }
    for (const route of collected) {
      const segments = route.url.split('/').filter((segment) => segment.length > 0);
      const named = defaultUnit(segments, knownUnits);
      const unitName = route.config.wrpc?.unit ?? mirror.unit?.(route) ?? named?.unit;
      if (!unitName) continue; // '/', '/:id' — nothing to name a unit after
      const methodName =
        route.config.wrpc?.name ?? mirror.name?.(route) ?? defaultName(route.method, segments.slice(named.tailIndex));
      let unit = units.get(unitName);
      if (!unit) {
        unit = {};
        units.set(unitName, unit);
      }
      if (Object.hasOwn(unit, methodName)) {
        throw new TypeError(
          `wrpcFastify mirror: '${unitName}/${methodName}' is claimed by both ` +
            `${route.method} ${route.url} and an earlier route — set config.wrpc.name on one`,
        );
      }
      const signature = routeSignature(route.schema);
      Object.defineProperty(unit, methodName, {
        value: procedure({
          access: mirror.access ?? 'session',
          meta: { mirrored: { method: route.method, path: route.url } },
          ...(signature ? { signature } : {}),
          handler: mirrorHandler(fastify, route, mirror, stringify),
        }),
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
    for (const [unitName, definition] of units) {
      rpc.router.addUnit(unitName, definition);
    }
  });
};

module.exports = { setupMirror, defaultName, defaultUnit, routeSignature };
