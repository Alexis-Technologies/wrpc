'use strict';

const http = require('node:http');

const { RpcServer, rpcOptions } = require('../rpc/core.js');
const { effectiveSchema } = require('../rpc/router.js');
const { publicErrorMessage, publicErrorDetails } = require('../transport.js');
const { createNodeEngine, isEngine } = require('../engine/index.js');
const { createUwsEngine } = require('./uws.js');
const { normalizeBody, eachHeader, nodeStream, createUpgradeGate } = require('./common.js');
const { setupMirror } = require('./mirror.js');

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

// ---- Delegated REST routes ------------------------------------------------
//
// A procedure with an `http` mapping becomes a REAL fastify route: fastify
// owns routing, schema validation, serialization and swagger; wrpc supplies
// the per-request Context (session, rooms, client lifecycle) and runs the
// bare handler under the procedure's own queue/timeout semantics
// (Procedure.invokeBare). The internal API over HTTP is therefore the SAME
// endpoint external consumers hit — one route, two audiences.
//
// wrpc's lifecycle hooks map onto fastify's phases by name (the naming was
// fastify's to begin with). Payload shapes differ where fastify's do:
//   onRequest        -> fastify onRequest,        payload = the request
//   preValidation    -> fastify preValidation,    payload = { params, query, body } (raw)
//   preHandler       -> fastify preHandler,       payload = the same args, validated
//   preSerialization -> fastify preSerialization, payload = the result (return replaces)
//   onSend           -> fastify onSend,           payload = the serialized body
//   onError          -> fastify onError,          payload = the error (observational)
//   onResponse       -> fastify onResponse,       payload = null
// onTimeout has no fastify phase; a 408 still reaches onError.

const argsOf = (request) => ({ params: request.params, query: request.query, body: request.body });

const forbidden = () => {
  const error = new Error('Forbidden');
  error.code = 403;
  // fastify's own error handler reads statusCode; wrpc's reads code. Both
  // are set so `restErrors: 'app'` keeps the status too.
  error.statusCode = 403;
  error.expose = true;
  return error;
};

// The wrpc wire error shape, as a fastify route errorHandler: the code is
// the status, the body is { message, code, details? } under the same
// redaction rule every other transport applies. Fastify's own schema
// validation failures land here too — their issue list becomes `details`.
const wireErrorHandler = (error, request, reply) => {
  let code =
    typeof error.code === 'number' ? error.code : typeof error.statusCode === 'number' ? error.statusCode : 500;
  if (!Number.isInteger(code) || code < 200 || code > 599) code = 500;
  const body = { message: publicErrorMessage(code, error), code };
  if (Array.isArray(error.validation)) {
    const issues = [];
    for (const issue of error.validation) {
      issues.push({ message: issue.message, path: issue.instancePath });
    }
    body.details = { issues };
  } else {
    const details = publicErrorDetails(code, error);
    if (details !== undefined) body.details = details;
  }
  reply.code(code).send(body);
};

const registerRestRoutes = (fastify, rpc, options) => {
  const routes = rpc.router.restRoutes();
  if (routes.length === 0) return;
  const contexts = new WeakMap();
  // One request runs exactly one route, so the first caller's target wins —
  // and every caller for a given request passes that route's own target.
  const contextOf = (request, target) => {
    let pending = contexts.get(request);
    if (!pending) {
      pending = rpc.delegatedContext(
        {
          method: request.method,
          headers: request.headers,
          remoteAddress: request.ip,
          url: request.url,
        },
        target,
      );
      contexts.set(request, pending);
    }
    return pending;
  };
  const base = rpc.basePath;
  for (const route of routes) {
    const { proc, http } = route;
    const hooks = rpc.router.hooksFor(proc);
    // The same call identity a packet-mode Context carries, so cross-cutting
    // hooks read context.method/context.procedure on the delegated path too.
    const rpcTarget = { method: `${route.unitKey}/${route.methodName}`, procedure: proc };
    // Arity matters to fastify: an async hook with a third parameter is
    // read as callback-style and refused — so the payload-less phases get
    // two-parameter wrappers, and only the payload phases take three.
    const wrap = (list, payloadOf) => {
      const wrapped = [];
      for (const hook of list) {
        wrapped.push(async (request, reply) => {
          const { context } = await contextOf(request, rpcTarget);
          return void (await hook(context, payloadOf(request)));
        });
      }
      return wrapped;
    };
    const wrapPayload = (list) => {
      const wrapped = [];
      for (const hook of list) {
        wrapped.push(async (request, reply, payload) => {
          const { context } = await contextOf(request, rpcTarget);
          return void (await hook(context, payload));
        });
      }
      return wrapped;
    };
    // Replacement semantics for the two payload-shaping phases: a wrpc hook
    // returning undefined keeps the payload, anything else replaces it —
    // translated to fastify's "the returned value IS the payload".
    const wrapShaping = (list) => {
      const wrapped = [];
      for (const hook of list) {
        wrapped.push(async (request, reply, payload) => {
          const { context } = await contextOf(request, rpcTarget);
          const replaced = await hook(context, payload);
          return replaced === undefined ? payload : replaced;
        });
      }
      return wrapped;
    };
    const init = async (request, reply) => {
      const { release } = await contextOf(request, rpcTarget);
      // The response's close is the eviction signal, whether the reply was
      // sent, hijacked or the peer vanished.
      reply.raw?.on?.('close', release);
    };
    // wrpc order: onRequest -> session restore -> ACCESS -> preValidation.
    // The session is restored inside delegatedContext (init), so the gate
    // closes the onRequest phase.
    const accessGuard = async (request) => {
      const { client } = await contextOf(request, rpcTarget);
      if (proc.access !== 'public' && !client.session) throw forbidden();
    };
    const status = http.status ?? 200;
    // With `restErrors: 'app'` the app owns the error format, so the
    // wire-shaped default error responses must not be documented — they
    // would re-serialize the app's error bodies into wrpc's shape.
    const schema = options.restErrors === 'app' ? (proc.schema ?? undefined) : effectiveSchema(proc);
    fastify.route({
      method: http.method,
      url: `${base}${http.path === '/' ? '' : http.path}` || '/',
      config: { wrpc: true },
      schema,
      onRequest: [init, ...wrap(hooks.onRequest, (request) => request), accessGuard],
      preValidation: wrap(hooks.preValidation, argsOf),
      preHandler: wrap(hooks.preHandler, argsOf),
      preSerialization: wrapShaping(hooks.preSerialization),
      onSend: wrapShaping(hooks.onSend),
      onError: wrapPayload(hooks.onError),
      onResponse: wrap(hooks.onResponse, () => null),
      ...(options.restErrors === 'app' ? {} : { errorHandler: wireErrorHandler }),
      handler: async (request, reply) => {
        const { context, transport } = await contextOf(request, rpcTarget);
        let result;
        try {
          result = await proc.invokeBare(context, argsOf(request));
        } catch (error) {
          // Mirror the numeric wrpc code onto fastify's statusCode so an
          // app-owned error handler (restErrors: 'app') keeps the status.
          if (error && typeof error.code === 'number' && error.statusCode === undefined) error.statusCode = error.code;
          throw error;
        }
        // A login that called startSession queued its cookie on the
        // transport nothing will flush — copy it onto the real reply.
        if (transport.pendingCookies.length > 0) reply.header('set-cookie', transport.pendingCookies);
        reply.code(status);
        // 204 promises "no content": the result is discarded by contract.
        if (status === 204) return reply.send();
        return result === undefined ? null : result;
      },
    });
  }
};

const wrpcFastify = async (fastify, options = {}) => {
  const { cors = null, ws = {}, maxBodySize } = options;
  // fastify's logger IS a pino, so it goes straight in: the core detects the
  // structured shape and calls child()/info(entry, message) natively.
  const logger = options.logger ?? fastify.log;
  const rpc = options.rpc ?? new RpcServer(rpcOptions({ ...options, cors, logger }));
  // Delegated REST routes exist FOR fastify's own serialization, schema and
  // swagger; a codec.rest body (possibly a raw Buffer) would silently bypass
  // fjs and preSerialization. Refusing loudly beats a route that documents
  // JSON and ships msgpack — the core hosts serve binary REST natively.
  // Checked on the resolved rpc so the options.rpc path is covered too.
  if (rpc.codec?.rest && rpc.router.hasRestRoutes) {
    throw new TypeError(
      'wrpcFastify: codec.rest and delegated REST routes (procedures with http mappings) are mutually exclusive — ' +
        'serve binary REST from a core host, or drop the http mappings under this plugin',
    );
  }
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
        // Streaming bypasses fastify's reply machinery and writes straight
        // to the raw response: an SSE body has no end for `reply.send` to
        // wait for. `hijack()` tells fastify not to answer it as well.
        stream(options) {
          if (typeof reply.hijack === 'function') reply.hijack();
          settled = true;
          resolve();
          return nodeStream(reply.raw)(options);
        },
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
  // The SSE endpoint is a static segment, so find-my-way prefers it over
  // the parametric '/:unit/:method' it would otherwise fall into — where the
  // core never sees it as an events request.
  const routes =
    base === '' ? ['/', rpc.eventsPath, '/:unit/:method'] : [base, rpc.eventsPath, `${base}/:unit/:method`];
  for (const url of routes) {
    fastify.route({
      method: ['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
      url,
      // The marker the mirror feature skips: wrpc must not mirror itself.
      config: { wrpc: true },
      ...(maxBodySize === undefined ? {} : { bodyLimit: maxBodySize }),
      handler: handle,
    });
  }

  // Procedures with an `http` mapping become native fastify routes — the
  // full delegation described above. Registered AFTER the conventional
  // routes; find-my-way prefers the more specific static/parametric shape
  // per segment, so `${base}/projects/:orgId` wins over the generic
  // `${base}/:unit/:method` where both could match.
  registerRestRoutes(fastify, rpc, options);

  // Reverse engineering: the app's own routes become wrpc procedures
  // (collected from here on — register this plugin before the routes it
  // should mirror), dispatched through fastify.inject so the route's whole
  // pipeline keeps running.
  if (options.mirror) setupMirror(fastify, rpc, options);

  // ---- WebSocket: engine attach ------------------------------------------

  const source = engine.attach({
    ...(engine.standalone ? {} : { server: fastify.server }),
    ...ws,
    verifyClient: createUpgradeGate({ rpc, cors, ws }),
  });
  source.on('connection', (socket, req) => {
    rpc.attachSocket(socket, {
      headers: req.headers,
      url: req.url ?? '',
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
