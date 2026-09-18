'use strict';

// The Node barrel of @alexify/wrpc/wt: the server half of WebTransport.
// The client transport (`transport: 'wt'`) lives in the base entry; what a
// server needs is here — the session contract (port.js), the WrpcSocket shim
// over a session (socket.js), attachSession/acceptSessions to hand sessions
// to an RpcServer, and the adapters that read a host implementation's
// CONNECT request (fromFails for @fails-components/webtransport, fromQuico
// for quico). No implementation is required from here: Node has no
// WebTransport of its own, and the one an application runs is injected.

const { WtSocket, DEFAULT_HIGH_WATER_MARK, DEFAULT_LOW_WATER_MARK } = require('./socket.js');
const { createLoggerWriter } = require('../logging.js');
const { isWtSession, isWtStream, isWtDatagrams } = require('./port.js');
const { fromQuico } = require('./quico.js');
// Requiring it registers ServerTransport.transport.wt — what attachSocket
// picks for `kind: 'wt'`.
const { ServerWtTransport } = require('./transport.js');
const framing = require('./framing.js');

// How long a session may sit without opening its control stream before it
// is refused: a client that connected and never spoke.
const DEFAULT_ACCEPT_TIMEOUT = 10_000;

const closeQuietly = (session, info) => {
  try {
    session.close(info);
  } catch {
    // Already closed.
  }
};

// The RpcServer behind whatever was handed over: a Server (its `rpc`), an
// RpcServer, or anything else with attachSocket — a PeerHost does not
// qualify, it has no socket path.
const rpcOf = (server) => {
  if (typeof server?.attachSocket === 'function') return server;
  if (typeof server?.rpc?.attachSocket === 'function') return server.rpc;
  throw new TypeError('attachSession: a Server or RpcServer (something with attachSocket) is required');
};

// The first bidirectional stream the client opens is the control stream.
// Read with a reader released afterwards, so the incoming-streams stream
// stays usable for what comes next.
const firstStream = async (session, timeout) => {
  const reader = session.incomingBidirectionalStreams.getReader();
  let timer = null;
  const expired = new Promise((resolve) => {
    timer = setTimeout(() => resolve({ done: true, timeout: true }), timeout);
  });
  try {
    const result = await Promise.race([reader.read(), expired]);
    return result.done ? null : result.value;
  } finally {
    clearTimeout(timer);
    reader.releaseLock();
  }
};

/**
 * Hands one WebTransport session to a server as a client: waits for its
 * control stream, wraps the session as a WrpcSocket and attaches it the way
 * a WebSocket is attached — sessions, declared headers, flow control and the
 * ServerWsTransport all apply. `headers`, `url` and `remoteAddress` are the
 * CONNECT request as the host implementation saw it (an adapter reads them
 * off the session: fromFails, fromQuico); `url` carries the `wrpc_h` /
 * `wrpc_meta` query a browser client declares its bags in.
 *
 * Resolves with the Client, or null when the session was refused — by
 * `verify` (closed 403) or for never opening a control stream within
 * `acceptTimeout` (closed 408). `idleTimeout` (off by default) terminates a
 * session that sends nothing for that long — see WtSocket.
 */
const attachSession = async (server, session, options = {}) => {
  const rpc = rpcOf(server);
  if (!isWtSession(session)) {
    throw new TypeError(
      'attachSession: a WebTransport session (incomingBidirectionalStreams, close, closed) is required',
    );
  }
  const {
    headers = {},
    url = '/',
    remoteAddress = '',
    verify = null,
    acceptTimeout = DEFAULT_ACCEPT_TIMEOUT,
    kind = 'wt',
    highWaterMark,
    lowWaterMark,
    maxMessage,
    idleTimeout,
  } = options;
  if (verify && (await verify({ headers, url, remoteAddress, session })) === false) {
    closeQuietly(session, { closeCode: 403, reason: 'Forbidden' });
    return null;
  }
  if (session.ready) await session.ready;
  const stream = await firstStream(session, acceptTimeout);
  if (!stream) {
    closeQuietly(session, { closeCode: 408, reason: 'No control stream' });
    return null;
  }
  const socket = new WtSocket(session, stream, { remoteAddress, highWaterMark, lowWaterMark, maxMessage, idleTimeout });
  return rpc.attachSocket(socket, { headers, url, remoteAddress, kind });
};

// A ReadableStream or any (async) iterable of sessions, as one iterator
// that stop() can end from outside.
const iterate = (sessions) => {
  if (typeof sessions?.getReader === 'function') {
    const reader = sessions.getReader();
    return {
      next: () => reader.read(),
      stop: () => reader.cancel().catch(() => {}),
    };
  }
  const iterator = sessions?.[Symbol.asyncIterator]?.() ?? sessions?.[Symbol.iterator]?.();
  if (!iterator) throw new TypeError('acceptSessions: a ReadableStream or an iterable of sessions is required');
  return {
    next: () => iterator.next(),
    // Not awaited: an async generator suspended on an await it owns queues
    // the return() until that await settles, which may be never — the
    // source is the application's to end; stop() only stops attaching.
    stop: () => {
      Promise.resolve()
        .then(() => iterator.return?.())
        .catch(() => {});
      return Promise.resolve();
    },
  };
};

/**
 * Attaches every session a host hands out — a `sessionStream(path)` from
 * @fails-components/webtransport, or any async iterable. `meta(session)`
 * reads the CONNECT request off each (fromFails by default); the rest of
 * the options go to attachSession. Returns `{ stop, done }`: stop() ends
 * the loop — cancelling a ReadableStream, asking an iterator to return —
 * and `done` settles once the source has yielded its last session (at once
 * for a stream; an iterator blocked on its own await, when that settles).
 */
const acceptSessions = (server, sessions, options = {}) => {
  const rpc = rpcOf(server);
  const { meta = fromFails, onError = null, onClient = null, logger = null, ...rest } = options;
  // `onError` defaulted to null, so a session that failed to attach — a
  // verify that threw, a source that died — was dropped without a trace:
  // the one failure mode of a WebTransport server that nothing above could
  // observe. The default is now the server's own writer (borrowed through
  // `rpc.log`, which exists for exactly this), and an explicit `onError`
  // still wins, since a caller that handles the error wants to decide.
  const log = createLoggerWriter(logger ?? rpc.log).child({ component: 'wt' });
  const report =
    onError ??
    ((error, session) => {
      log.error({ err: error, event: session === null ? 'wt.source' : 'wt.attach' });
    });
  const source = iterate(sessions);
  let stopped = false;
  // `done` always resolves: a source that throws ends the loop through
  // onError, never as a rejection nobody awaited.
  const done = (async () => {
    for (;;) {
      let next;
      try {
        next = await source.next();
      } catch (error) {
        if (!stopped) report(error, null);
        return;
      }
      const { value: session, done: finished } = next;
      if (finished || stopped) return;
      try {
        const observed = await meta(session);
        const client = await attachSession(rpc, session, { ...rest, ...observed });
        if (client && onClient) onClient(client, session);
      } catch (error) {
        closeQuietly(session, { closeCode: 500, reason: 'Internal error' });
        report(error, session);
      }
    }
  })();
  return {
    stop() {
      stopped = true;
      return source.stop();
    },
    done,
  };
};

/**
 * The CONNECT request off a @fails-components/webtransport session: its
 * `header` carries the HTTP/3 pseudo-headers and the real ones; `:path`
 * keeps the query. Pseudo-headers are dropped from the bag the server sees
 * (`origin` and friends stay), the way an upgrade request's are.
 */
const fromFails = (session) => {
  const header = session?.header ?? {};
  const headers = {};
  for (const name in header) {
    if (name.charCodeAt(0) === 58) continue; // ':'
    const value = header[name];
    if (value !== undefined && value !== null) headers[name.toLowerCase()] = String(value);
  }
  const address = session?.peerAddress;
  const remoteAddress =
    typeof address === 'string'
      ? address.replace(/:\d+$/, '')
      : typeof address?.address === 'string'
        ? address.address
        : '';
  return { headers, url: typeof header[':path'] === 'string' ? header[':path'] : '/', remoteAddress };
};

/**
 * The request callback an @fails-components/webtransport server needs
 * (`h3.setRequestCallback(failsRequestCallback)`): its `sessionStream(path)`
 * matches the request path literally, query included, and a wrpc client
 * declares its headers in the query — so the session is routed by pathname,
 * while `header[':path']` keeps the query for fromFails() to read.
 */
const failsRequestCallback = async (args) => {
  const target = args.header?.[':path'] ?? '/';
  const at = target.indexOf('?');
  // `status` is what accepts the request; the header is left as received,
  // so the session's `header[':path']` keeps the query.
  return { ...args, path: at === -1 ? target : target.slice(0, at), status: 200 };
};

module.exports = {
  attachSession,
  acceptSessions,
  fromFails,
  failsRequestCallback,
  fromQuico,
  WtSocket,
  ServerWtTransport,
  isWtSession,
  isWtStream,
  isWtDatagrams,
  DEFAULT_ACCEPT_TIMEOUT,
  DEFAULT_HIGH_WATER_MARK,
  DEFAULT_LOW_WATER_MARK,
  ...framing,
};
