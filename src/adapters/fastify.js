'use strict';

const http = require('node:http');

const { RpcServer } = require('../rpc/core.js');
const { createNodeEngine, isEngine } = require('../engine/index.js');
const { createUwsEngine } = require('./uws.js');
const { isOriginAllowed } = require('../transport.js');
const { normalizeBody, eachHeader } = require('./common.js');

// Fastify plugin. One plugin, two backends, picked by looking at what
// fastify is actually running on:
//
//   fastify()                     -> a real node http.Server -> node engine,
//                                    attached to its 'upgrade' event
//   fastify({ serverFactory })    -> a uws-backed fake server (fastify-uws)
//                                    -> uws engine over the same uws app
//
// HTTP calls go through fastify's own routes, so the app's hooks, auth and
// error handling run BEFORE wrpc sees the call ("upgrade-through-router").

const getPathname = (url) => (url ? url.split('?')[0] : '/');

// fastify-uws keeps its uWebSockets.js app on a private symbol. Reaching for
// it by description is deliberate: the package exports no accessor, and the
// alternative is refusing to support the only uws-backed fastify there is.
const findUwsApp = (server) => {
  if (!server || typeof server !== 'object') return null;
  const key = Object.getOwnPropertySymbols(server).find((symbol) => symbol.description === 'uws.app');
  const app = key ? server[key] : null;
  return app && typeof app.ws === 'function' ? app : null;
};

const resolveEngine = (fastify, options) => {
  const { engine } = options;
  if (engine) {
    if (!isEngine(engine)) {
      throw new TypeError('wrpcFastify: options.engine does not implement the Engine contract');
    }
    return engine;
  }
  const { server } = fastify;
  if (server instanceof http.Server) return createNodeEngine(options.ws);
  const app = findUwsApp(server);
  if (app) return createUwsEngine({ ...options.ws, app });
  throw new TypeError(
    'wrpcFastify: could not detect a WebSocket backend for this fastify instance — ' +
      'pass one explicitly via options.engine',
  );
};

// fastify's logger is pino: it has info/warn/error/debug but no `log`, which
// the RPC core calls on every successful invocation. Adapt rather than hand
// it over raw (that crashed on the first successful call).
const toConsole = (logger) => {
  if (!logger) return globalThis.console;
  if (typeof logger.log === 'function') return logger;
  if (typeof logger.info !== 'function') return globalThis.console;
  return {
    log: (...args) => logger.info(...args),
    info: (...args) => logger.info(...args),
    warn: (...args) => logger.warn(...args),
    error: (...args) => logger.error(...args),
    debug: (...args) => logger.debug?.(...args),
  };
};

const wrpcFastify = async (fastify, options = {}) => {
  const { router, sessions, cors = null, basePath, ws = {}, maxBodySize, backplane = null, instanceId } = options;
  const console = options.console ?? toConsole(fastify.log);
  const rpc = options.rpc ?? new RpcServer({ router, sessions, cors, basePath, console, backplane, instanceId });
  const base = rpc.basePath;
  const engine = resolveEngine(fastify, options);

  // ---- HTTP: fastify routes feeding the engine-agnostic core -------------

  const handle = async (request, reply) => {
    // Fastify already parsed the body; the core re-parses the JSON packet,
    // so hand it back the text rather than an object.
    const body = normalizeBody(request.body);
    let settled = false;
    await new Promise((resolve) => {
      const call = {
        method: request.method,
        url: request.url,
        headers: request.headers,
        body,
        remoteAddress: request.ip,
        respond({ status, headers, body: payload }) {
          if (settled) return;
          settled = true;
          reply.code(status);
          eachHeader(headers ?? {}, (name, value) => {
            // fastify derives Content-Length from the payload (and a
            // compression plugin would invalidate a fixed one).
            if (name.toLowerCase() === 'content-length') return;
            reply.header(name, value);
          });
          reply.send(payload ?? '');
          resolve();
        },
        // reply.raw, NOT request.raw: fastify closes the request stream as
        // soon as the body is consumed, which is before the handler runs —
        // wiring eviction to it destroyed every HTTP client at call start
        // (rpc.clients was empty mid-call and handlers saw a false abort).
        // The response closes when it is written or the peer disconnects,
        // which is the signal the node shell uses too.
        onAbort(listener) {
          reply.raw?.on?.('close', listener);
        },
      };
      // A call that never answers (a stream packet over HTTP is rejected,
      // but a handler can still hang) must not park the route forever —
      // the core evicts the client through onAbort either way.
      reply.raw?.on?.('close', resolve);
      void rpc.handleHttpCall(call);
    });
    return reply;
  };

  // Unlike the express and uws adapters, nothing here reads the request
  // stream — fastify parses the body and hands over `request.body`, so its
  // own `bodyLimit` (1 MiB by default) already guards these routes and
  // rejects an oversized call with 413 FST_ERR_CTP_BODY_TOO_LARGE before the
  // handler runs. `maxBodySize` only narrows that per route; left unset, the
  // app's limit stands, because a plugin silently RAISING the host's body
  // limit would be a security regression the app never asked for.
  const routes = base === '' ? ['/', '/:unit/:method'] : [base, `${base}/:unit/:method`];
  for (const url of routes) {
    fastify.route({
      method: ['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
      url,
      ...(maxBodySize === undefined ? {} : { bodyLimit: maxBodySize }),
      handler: handle,
    });
  }

  // ---- WebSocket: engine attach ------------------------------------------

  const verifyUpgrade = ({ req }) => {
    if (ws.path === undefined) {
      const pathname = getPathname(req.url);
      if (pathname !== '/' && rpc.matchPath(pathname) === null) return false;
    }
    return isOriginAllowed(cors, req.headers.origin);
  };

  const source = engine.attach({
    ...(engine.standalone ? {} : { server: fastify.server }),
    ...ws,
    verifyClient: ws.verifyClient ?? verifyUpgrade,
  });
  source.on('connection', (socket, req) => {
    rpc.attachSocket(socket, {
      headers: req.headers,
      remoteAddress: req.socket?.remoteAddress ?? socket.remoteAddress,
    });
  });

  fastify.decorate('wrpc', rpc);
  fastify.addHook('preClose', async () => {
    await rpc.close();
    engine.close();
  });
};

// Escape fastify's encapsulation the way fastify-plugin does, without taking
// fastify-plugin as a dependency: the decorator and the routes belong to the
// app that registered the plugin, not to a child scope.
wrpcFastify[Symbol.for('skip-override')] = true;
wrpcFastify[Symbol.for('fastify.display-name')] = '@alexify/wrpc';

module.exports = { wrpcFastify, findUwsApp };
