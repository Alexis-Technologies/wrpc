'use strict';

// The declarative REST bridge, split out of router.js: the http-mapping
// normalizer, the rest.version strategy, and the per-verb segment trie the
// dispatcher matches requests against. Everything here is COLD by the
// performance conventions — the trees are built when the unit set changes,
// and matchRestTrees runs once per HTTP request, not per packet.

const DEFAULT_VERSION = '*';

// User-defined names ('__proto__' params included) must land as own
// properties — the same guard router.js applies (see assignKey there); a
// local copy keeps this module dependency-free of the router.
const assignKey = (target, key, value) => {
  Object.defineProperty(target, key, { value, enumerable: true, writable: true, configurable: true });
};

const HTTP_METHODS = ['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE'];
// Response headers the transport owns — a route may not redeclare them.
const RESERVED_HEADERS = new Set(['content-length', 'set-cookie', 'wrpc-version', 'transfer-encoding', 'connection']);

// The static response headers of a declared route: string -> string, with
// the transport-owned names refused up front.
const normalizeHeaders = (headers) => {
  if (headers === undefined || headers === null) return null;
  if (typeof headers !== 'object' || Array.isArray(headers)) {
    throw new TypeError('procedure() http.headers must be an object of header name -> value');
  }
  const result = {};
  for (const name of Object.keys(headers)) {
    const value = headers[name];
    if (typeof value !== 'string') throw new TypeError(`procedure() http.headers['${name}'] must be a string`);
    if (RESERVED_HEADERS.has(name.toLowerCase())) {
      throw new TypeError(`procedure() http.headers may not set '${name}': the transport owns it`);
    }
    result[name] = value;
  }
  return Object.keys(result).length > 0 ? result : null;
};

// `http.cache` — how a shared cache (a CDN, a browser) may treat the
// route's successful responses. Only a safe method can be cacheable.
const normalizeCache = (cache, method) => {
  if (cache === undefined || cache === null) return null;
  if (typeof cache !== 'object' || Array.isArray(cache)) {
    throw new TypeError('procedure() http.cache must be an object');
  }
  if (method !== 'GET' && method !== 'HEAD') {
    throw new TypeError('procedure() http.cache is only valid on GET and HEAD');
  }
  const { maxAge, public: isPublic = false, staleWhileRevalidate = null, etag = true } = cache;
  if (!(Number.isInteger(maxAge) && maxAge >= 0)) {
    throw new TypeError('procedure() http.cache.maxAge must be an integer >= 0 (seconds)');
  }
  if (staleWhileRevalidate !== null && !(Number.isInteger(staleWhileRevalidate) && staleWhileRevalidate >= 0)) {
    throw new TypeError('procedure() http.cache.staleWhileRevalidate must be an integer >= 0 (seconds)');
  }
  return { maxAge, public: isPublic === true, staleWhileRevalidate, etag: etag !== false };
};

// The cache policy for ONE response, decided where the session is known.
// `public` is emitted only for a public procedure on a request that
// restored no session and set no cookie; anything session-bearing is
// `private, no-store` whatever the route asked for — one shared cache
// serving one user's answer to another is the failure this exists to stop.
// Returns { control, etag } or null for "no cache headers".
const cacheHeadersFor = (cache, { access, session, cookies }) => {
  if (!cache) return null;
  const bearing = access !== 'public' || session === true || cookies === true;
  if (bearing) return { control: 'private, no-store', etag: false };
  let control = `${cache.public ? 'public' : 'private'}, max-age=${cache.maxAge}`;
  if (cache.staleWhileRevalidate !== null) control += `, stale-while-revalidate=${cache.staleWhileRevalidate}`;
  return { control, etag: cache.etag };
};
const PARAM_SEGMENT = /^:[A-Za-z_$][\w$]*$/;
const STATIC_SEGMENT = /^[^/:*]+$/;

const normalizeHttp = (http) => {
  if (http === null || http === undefined) return null;
  if (typeof http !== 'object') throw new TypeError('procedure() http must be an object');
  const { method, path, status = null, headers, cache } = http;
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
  return { method, path, status, headers: normalizeHeaders(headers), cache: normalizeCache(cache, method) };
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

// The introspection view of an http mapping: the optional keys only when
// set, so a client or `wrpc types` sees `{ method, path, status? }` as
// before and `headers`/`cache` only on routes that declare them.
const publicHttp = (http) => {
  const view = { method: http.method, path: http.path };
  if (http.status !== null && http.status !== undefined) view.status = http.status;
  if (http.headers) view.headers = http.headers;
  if (http.cache) view.cache = http.cache;
  return view;
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

// The per-verb segment trie over every procedure's effective http mapping,
// or null when no procedure declares one. Throws on conflicting routes —
// a definition error, loud at build time.
const buildRestTrees = (units, restOptions) => {
  const makeNode = () => ({ static: new Map(), param: null, terminal: null });
  let trees = null;
  for (const [unit, versions] of units) {
    for (const [version, entry] of versions) {
      const unitKey = version === DEFAULT_VERSION ? unit : `${unit}.${version}`;
      for (const [methodName, proc] of entry.methods) {
        if (!proc.http) continue;
        trees ??= new Map();
        // Effective, not declared: with rest.version, two versions of one
        // declared path diverge by their '/vN' prefix BEFORE the conflict
        // check below ever sees them.
        const http = effectiveHttp(proc.http, version, restOptions);
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
  return trees;
};

const walk = (node, segments) => {
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
};

/**
 * Matches an HTTP verb and decoded path segments against the trie set.
 * Returns null (no path match — the caller falls back to the conventional
 * /:unit/:method mode), `{ allowed }` (some OTHER verb matches this path —
 * a 405 with an Allow list), or the full route
 * `{ proc, unitKey, methodName, params, http }`.
 */
const matchRestTrees = (trees, method, segments) => {
  if (!trees) return null;
  // HEAD is answered by the GET route (RFC 9110 9.3.2): the host strips the
  // body, the headers — the cache policy's included — are the GET's.
  const hit = walk(trees.get(method), segments) ?? (method === 'HEAD' ? walk(trees.get('GET'), segments) : null);
  if (hit) {
    const params = {};
    for (let i = 0; i < hit.route.paramNames.length; i++) {
      assignKey(params, hit.route.paramNames[i], hit.values[i]);
    }
    const { proc, unitKey, methodName, http } = hit.route;
    return { proc, unitKey, methodName, params, http };
  }
  // No route under this verb — probe the other verbs' trees so the answer
  // distinguishes "unknown path" (fall through, maybe the conventional mode
  // knows it) from "known path, wrong verb" (405).
  const allowed = [];
  for (const [verb, tree] of trees) {
    if (verb !== method && walk(tree, segments)) allowed.push(verb);
  }
  return allowed.length > 0 ? { allowed } : null;
};

/** Every declared REST route — what a host adapter registers natively. */
const collectRestRoutes = (units, restOptions) => {
  const routes = [];
  for (const [unit, versions] of units) {
    for (const [version, entry] of versions) {
      const unitKey = version === DEFAULT_VERSION ? unit : `${unit}.${version}`;
      for (const [methodName, proc] of entry.methods) {
        if (!proc.http) continue;
        routes.push({ unitKey, methodName, proc, http: effectiveHttp(proc.http, version, restOptions) });
      }
    }
  }
  return routes;
};

module.exports = {
  normalizeHttp,
  publicHttp,
  cacheHeadersFor,
  RESERVED_HEADERS,
  normalizeRestOptions,
  effectiveHttp,
  buildRestTrees,
  matchRestTrees,
  collectRestRoutes,
};
