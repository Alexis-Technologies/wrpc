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
const { sanitizeDeclared } = require('../rpc/handshake.js');
const { ConfigError, WrpcError } = require('../errors.js');

const HTTP_METHODS = new Set(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE']);

const capitalize = (text) => (text.length === 0 ? text : text[0].toUpperCase() + text.slice(1));

// 'org-users' / 'org_users' -> 'orgUsers'
const toCamelCase = (text) => text.replace(/[-_]+(\w)/g, (_, letter) => letter.toUpperCase());

const isParam = (segment) => segment.startsWith(':');

const hasParam = (segments) => {
  for (let i = 0; i < segments.length; i++) if (isParam(segments[i])) return true;
  return false;
};

// The non-empty segments of a route URL.
const segmentsOf = (url) => {
  const parts = url.split('/');
  const segments = [];
  for (let i = 0; i < parts.length; i++) if (parts[i].length > 0) segments.push(parts[i]);
  return segments;
};

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
  for (let i = 0; i < tail.length; i++) {
    const segment = tail[i];
    if (isParam(segment)) lastParam = segment.slice(1);
    else statics.push(toCamelCase(segment));
  }
  const endsWithParam = tail.length > 0 && isParam(tail[tail.length - 1]);
  // '/slug/:slug' collapses: the static names the param, not a sub-resource.
  if (endsWithParam && statics.length > 0 && capitalize(statics[statics.length - 1]) === capitalize(lastParam)) {
    statics.pop();
  }
  let suffix = '';
  for (let i = 0; i < statics.length; i++) suffix += capitalize(statics[i]);
  if (method === 'GET' || method === 'HEAD') {
    return endsWithParam ? `find${suffix}By${capitalize(lastParam)}` : `findAll${suffix}`;
  }
  const verbs = { POST: 'create', PUT: 'replace', PATCH: 'update', DELETE: 'delete' };
  return `${verbs[method]}${suffix}`;
};

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
    const keys = Object.keys(schema.properties);
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
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

const buildUrl = (url, params, query, stringify) => {
  const segments = url.split('/');
  const parts = [];
  for (let i = 0; i < segments.length; i++) {
    const segment = segments[i];
    if (!isParam(segment)) {
      parts.push(segment);
      continue;
    }
    const name = segment.slice(1);
    const value = params?.[name];
    if (value === undefined) {
      throw new WrpcError(`Missing path param '${name}' for ${url}`, { code: 400, expose: true });
    }
    const text = `${value}`;
    // A value that is a path of its own. `encodeURIComponent` leaves dots
    // alone, and the URL the injected request is parsed as resolves them:
    // `id: '..'` turned `/admin/users/:id` into `/admin/` — ANOTHER route,
    // reached through a procedure named after this one. An empty value does
    // the same one level down (`/users/`). Percent-encoding the dots would
    // not help: a URL parser normalizes `%2e%2e` as well.
    if (text === '' || text === '.' || text === '..') {
      throw new WrpcError(`Invalid path param '${name}' for ${url}`, { code: 400, expose: true });
    }
    parts.push(encodeURIComponent(text));
  }
  let built = parts.join('/');
  if (query && Object.keys(query).length > 0) {
    const text = stringify ? stringify(query) : `${new URLSearchParams(query)}`;
    if (text) built += `?${text}`;
  }
  return built;
};

// The headers of the injected request. What the application derives from the
// context (`mirror.headers` — the session's token, a user id) always wins,
// and of what the CALLER sent in `args.headers` only the names the
// application listed pass (`mirror.allowHeaders`), through the sanitizer the
// handshake's declared headers go through — none by default.
//
// It used to be `{ ...mirror.headers(context), ...headers }`: the caller's
// spread last, so any procedure argument overrode the header the route
// trusts for identity, and any header at all — `x-forwarded-for`, a proxy's
// own `x-user-id` — reached a request that arrives from inside the process,
// past whatever strips those at the edge. Names are compared lowercased, as
// the injected request lowercases them anyway.
const injectedHeaders = async (mirror, allow, context, headers) => {
  const merged = {};
  const passed = allow === null ? null : sanitizeDeclared(headers, allow);
  if (passed !== null) for (const name in passed) merged[name] = passed[name];
  if (mirror.headers) {
    const own = await mirror.headers(context);
    if (own) {
      const names = Object.keys(own);
      for (let i = 0; i < names.length; i++) merged[names[i].toLowerCase()] = own[names[i]];
    }
  }
  return merged;
};

// The lowercase names of a list, or null when an entry is not a name.
const lowerNames = (list) => {
  const names = new Set();
  for (let i = 0; i < list.length; i++) {
    // A hole of a sparse list is skipped, as `every` and `map` skipped it.
    if (!(i in list)) continue;
    const name = list[i];
    if (typeof name !== 'string' || name.length === 0) return null;
    names.add(name.toLowerCase());
  }
  return names;
};

// `mirror.allowHeaders` → a Set of lowercase names, or null for none.
const allowedNames = (value) => {
  if (value === undefined || value === null) return null;
  const names = Array.isArray(value) ? lowerNames(value) : null;
  if (names === null) throw new ConfigError('wrpcFastify mirror: allowHeaders must be an array of header names');
  return names;
};

const mirrorHandler = (fastify, route, mirror, stringify, allow) => {
  const { method, url } = route;
  return async (context, { params = {}, query = {}, body, headers } = {}) => {
    const response = await fastify.inject({
      method,
      url: buildUrl(url, params, query, stringify),
      ...(body === undefined ? {} : { payload: body }),
      headers: await injectedHeaders(mirror, allow, context, headers),
    });
    const json = (response.headers['content-type'] ?? '').includes('json');
    const parsed = json ? response.json() : response.body;
    if (response.statusCode >= 400) {
      throw new WrpcError((json && parsed?.message) || `HTTP ${response.statusCode}`, {
        code: response.statusCode,
        details: json ? parsed?.details : undefined,
        expose: true,
      });
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
  // The caller-supplied header names a mirrored call may carry, or null for
  // none (the default).
  const allow = allowedNames(mirror.allowHeaders);
  const collected = [];

  fastify.addHook('onRoute', (route) => {
    // The adapter's own routes carry config.wrpc === true; an OBJECT there
    // is the app's per-route mirror config ({ unit, name }), and `false`
    // opts a route out. HEAD twins and OPTIONS are transport furniture.
    if (route.config?.wrpc === true) return;
    const methods = Array.isArray(route.method) ? route.method : [route.method];
    for (let i = 0; i < methods.length; i++) {
      const method = methods[i];
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
    for (let i = 0; i < collected.length; i++) {
      const segments = segmentsOf(collected[i].url);
      if (!hasParam(segments)) continue;
      const named = defaultUnit(segments);
      if (named) knownUnits.add(named.unit);
    }
    for (let i = 0; i < collected.length; i++) {
      const route = collected[i];
      const segments = segmentsOf(route.url);
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
        throw new ConfigError(
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
          handler: mirrorHandler(fastify, route, mirror, stringify, allow),
        }),
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
    for (const entry of units) rpc.router.addUnit(entry[0], entry[1]);
  });
};

module.exports = { setupMirror, defaultName, defaultUnit, routeSignature };
